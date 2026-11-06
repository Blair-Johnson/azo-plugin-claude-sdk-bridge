import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { createBridgeServer } from '../src/server.mjs';
import { validateRequest, validateSessionId } from '../src/request.mjs';

const key = 'test-local-key-not-a-credential-123456789';
const body = { model: 'sonnet', max_tokens: 64, messages: [{ role: 'user', content: 'hello' }] };
const message = { id: 'msg_test', type: 'message', role: 'assistant', model: 'sonnet',
  content: [{ type: 'text', text: 'OK' }], stop_reason: 'end_turn', stop_sequence: null,
  usage: { input_tokens: 2, output_tokens: 1 } };
async function fixture(t, overrides = {}, options = {}) {
  const calls = [];
  const manager = { async handle(body, context) { calls.push({ body, context }); return message; },
    async cancel(id) { calls.push({ canceled: id }); }, async close() {}, ...overrides };
  const endpoint = createBridgeServer({ manager, key, ...options });
  const address = await endpoint.listen(0);
  t.after(() => endpoint.close());
  const url = `http://127.0.0.1:${address.port}`;
  const send = (value = body, headers = {}, path = '/v1/messages', method = 'POST') => fetch(url + path, {
    method, headers: { 'x-api-key': key, 'x-azo-session-id': 'session-1', 'content-type': 'application/json', ...headers },
    body: method === 'POST' ? JSON.stringify(value) : undefined,
  });
  return { send, calls, endpoint, url };
}

test('normal JSON response and session header reach manager unchanged', async t => {
  const { send, calls } = await fixture(t);
  const response = await send();
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), message);
  assert.equal(calls[0].context.sessionId, 'session-1');
  assert.deepEqual(calls[0].body.tools, []);
});

test('authentication and browser/host checks precede manager and health', async t => {
  const { send, calls, url } = await fixture(t);
  for (const headers of [{ 'x-api-key': '' }, { 'x-api-key': 'wrong' }])
    assert.equal((await send(body, headers)).status, 401);
  for (const headers of [{ origin: 'https://evil.test' }, { host: 'evil.test' }, { host: '127.0.0.1.evil.test' }]) {
    const status = await new Promise((resolve, reject) => {
      const request = http.request(url + '/health', { headers: { 'x-api-key': key, ...headers } }, response => {
        response.resume();
        resolve(response.statusCode);
      });
      request.on('error', reject);
      request.end();
    });
    assert.equal(status, 403);
  }
  assert.equal((await send(null, { 'x-api-key': '' }, '/health', 'GET')).status, 401);
  assert.equal((await send(null, {}, '/health', 'GET')).status, 200);
  assert.equal(calls.length, 0);
});

test('missing session, malformed content, unsupported controls, oversize body', async t => {
  const { send, calls } = await fixture(t, {}, { maxBodyBytes: 1000 });
  assert.equal((await send(body, { 'x-azo-session-id': '' })).status, 400);
  assert.equal((await send(body, { 'content-type': 'text/plain' })).status, 415);
  assert.equal((await send({ ...body, temperature: 0.5 })).status, 400);
  assert.equal((await send({ ...body, messages: [{ role: 'user', content: 'x'.repeat(2000) }] })).status, 413);
  assert.equal(calls.length, 0);
});

test('DELETE session is explicit and authenticated', async t => {
  const { send, calls } = await fixture(t);
  assert.equal((await send(null, {}, '/v1/session', 'DELETE')).status, 200);
  assert.deepEqual(calls, [{ canceled: 'session-1' }]);
});

test('unsafe SDK error detail is not exposed', async t => {
  const { send } = await fixture(t, { async handle() { throw new Error('secret-token and private prompt'); } });
  const response = await send();
  assert.equal(response.status, 502);
  assert.doesNotMatch(await response.text(), /secret-token|private prompt/);
});

test('known public errors keep HTTP status', async t => {
  const { send } = await fixture(t, { async handle() {
    throw Object.assign(new Error('internal'), { statusCode: 409, publicMessage: 'Session busy.' });
  } });
  const response = await send();
  assert.equal(response.status, 409);
  assert.match(await response.text(), /Session busy/);
});

test('SSE uses native events and successful completion does not abort parked query', async t => {
  let signal;
  const { send } = await fixture(t, { async handle(body, context) {
    signal = context.signal;
    context.onEvent({ type: 'message_start', message: { ...message, content: [], stop_reason: null } });
    context.onEvent({ type: 'content_block_start', index: 0, content_block: message.content[0] });
    context.onEvent({ type: 'content_block_stop', index: 0 });
    context.onEvent({ type: 'message_delta', delta: { stop_reason: 'end_turn', stop_sequence: null }, usage: { output_tokens: 1 } });
    context.onEvent({ type: 'message_stop' });
    return message;
  } });
  const response = await send({ ...body, stream: true });
  assert.match(response.headers.get('content-type'), /text\/event-stream/);
  const text = await response.text();
  assert.equal(text.match(/event: message_stop/g).length, 1);
  assert.match(text, /event: content_block_start/);
  assert.equal(signal.aborted, false);
});

test('stream failure produces a safe terminal error', async t => {
  const { send } = await fixture(t, { async handle() { throw new Error('PRIVATE'); } });
  const text = await (await send({ ...body, stream: true })).text();
  assert.match(text, /event: error/);
  assert.doesNotMatch(text, /PRIVATE/);
});

test('timeout aborts manager and terminates response even if manager ignores signal', async t => {
  let signal;
  const { send } = await fixture(t, { handle(_, context) {
    signal = context.signal;
    return new Promise(() => {});
  } }, { requestTimeoutMs: 40, pingMs: 10 });
  const text = await (await send({ ...body, stream: true })).text();
  assert.match(text, /event: ping/);
  assert.match(text, /canceled or timed out/);
  assert.equal(signal.aborted, true);
});

test('client disconnect signals cancellation', async t => {
  let started;
  const ready = new Promise(resolve => { started = resolve; });
  let aborted;
  const stopped = new Promise(resolve => { aborted = resolve; });
  const { url } = await fixture(t, { handle(_, { signal }) {
    signal.addEventListener('abort', aborted, { once: true });
    started();
    return new Promise(() => {});
  } });
  const req = http.request(url + '/v1/messages', { method: 'POST', headers: {
    'x-api-key': key, 'x-azo-session-id': 'disconnect', 'content-type': 'application/json',
  } });
  req.on('error', () => {});
  req.end(JSON.stringify({ ...body, stream: true }));
  await ready;
  req.destroy();
  await stopped;
});

test('validation accepts Agent Zoo null tools and rejects unsupported semantics', () => {
  assert.deepEqual(validateRequest({ ...body, tools: null }).tools, []);
  for (const extra of [{ temperature: 1 }, { tool_choice: { type: 'any' } },
    { tool_choice: { type: 'auto', disable_parallel_tool_use: true } },
    { output_config: { format: { type: 'json_schema' } } }, { stream: 'true' },
    { max_tokens: 0 }, { thinking: { type: 'enabled', budget_tokens: 10 } }])
    assert.throws(() => validateRequest({ ...body, ...extra }));
  assert.throws(() => validateRequest({ ...body, messages: [{ role: 'assistant', content: 'prefix' }] }));
  assert.throws(() => validateRequest({ ...body, system: [{ type: 'image' }] }));
  assert.throws(() => validateSessionId('../path'));
  assert.equal(validateSessionId('abc-123'), 'abc-123');
});

test('model progress reaches HTTP client before generation completes', { timeout: 2000 }, async t => {
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  let completed = false;
  const { send } = await fixture(t, { async handle(_, { onEvent }) {
    onEvent({ type: 'message_start', message: { ...message, content: [], stop_reason: null } });
    onEvent({ type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } });
    onEvent({ type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'live' } });
    await gate;
    completed = true;
    onEvent({ type: 'content_block_stop', index: 0 });
    onEvent({ type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: 1 } });
    onEvent({ type: 'message_stop' });
    return message;
  } });
  const response = await send({ ...body, stream: true });
  const reader = response.body.getReader();
  let received = '';
  while (!received.includes('text_delta')) received += new TextDecoder().decode((await reader.read()).value);
  assert.equal(completed, false);
  release();
  while (!(await reader.read()).done) { /* drain terminal events */ }
  assert.equal(completed, true);
});

test('request validation explains unsupported root tool schemas', () => {
  for (const key of ['anyOf', 'oneOf', 'allOf']) {
    assert.throws(() => validateRequest({ ...body, tools: [{ name: 'root_schema',
      input_schema: { type: 'object', [key]: [{ required: ['value'] }] },
    }] }), error => error.statusCode === 400 && /Top-level anyOf\/oneOf\/allOf/.test(error.publicMessage));
  }
});
