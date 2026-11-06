import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as turn } from 'node:timers/promises';
import { BridgeManager } from '../src/engine.mjs';
import { createBridgeServer } from '../src/server.mjs';

const key = 'offline-cancellation-key-12345678901234567890';
const body = { model: 'claude-test', max_tokens: 64, tools: [], messages: [{ role: 'user', content: 'hello' }] };
const sessionId = 'handoff';
const deferred = () => {
  let resolve;
  const promise = new Promise(yes => { resolve = yes; });
  return { promise, resolve };
};

async function fixture(t, { holdReplacement = false } = {}) {
  const root = await mkdtemp(join(tmpdir(), 'azo-cancellation-test-'));
  const queries = [];
  const manager = new BridgeManager({ stateRoot: root, requestTimeoutMs: 2000,
    queryFactory({ options }) {
      const gate = deferred(), stopped = deferred();
      const index = queries.length + 1;
      const query = {
        closed: false,
        complete() { gate.resolve(); },
        close() { this.closed = true; gate.resolve(); stopped.resolve(); },
        async *[Symbol.asyncIterator]() {
          const emit = event => ({ type: 'stream_event', parent_tool_use_id: null, event });
          yield emit({ type: 'message_start', message: { id: `msg_${index}`, type: 'message', role: 'assistant',
            model: options.model, content: [], stop_reason: null, usage: { input_tokens: 1, output_tokens: 0 } } });
          yield emit({ type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } });
          yield emit({ type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: `text_${index}` } });
          if (index === 1 || holdReplacement) await gate.promise;
          if (this.closed) return;
          yield emit({ type: 'content_block_stop', index: 0 });
          yield emit({ type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: 1 } });
          yield emit({ type: 'message_stop' });
          yield { type: 'result', subtype: 'success', is_error: false };
          await stopped.promise;
        },
      };
      queries.push(query);
      return query;
    },
  });
  const endpoint = createBridgeServer({ manager, key });
  const address = await endpoint.listen(0);
  t.after(async () => { await endpoint.close(); await rm(root, { recursive: true, force: true }); });
  const send = (stream = false, signal) => fetch(`http://127.0.0.1:${address.port}/v1/messages`, {
    method: 'POST', signal,
    headers: { 'x-api-key': key, 'x-azo-session-id': sessionId, 'content-type': 'application/json' },
    body: JSON.stringify({ ...body, stream }),
  });
  const start = async () => {
    const progress = deferred(), controller = new AbortController();
    const pending = manager.handle(body, { sessionId, signal: controller.signal,
      onEvent(event) { if (event.type === 'content_block_delta') progress.resolve(); } });
    pending.catch(() => {});
    await progress.promise;
    return { pending, controller };
  };
  return { manager, endpoint, queries, send, start };
}

async function firstText(response) {
  const reader = response.body.getReader();
  let text = '';
  while (!text.includes('text_delta')) {
    const chunk = await reader.read();
    assert.equal(chunk.done, false);
    text += new TextDecoder().decode(chunk.value);
  }
  return reader;
}

// Hold exactly the first response's close notification until the replacement
// reaches handle(). This deterministically reproduces late TCP-close delivery,
// rather than hoping the OS happens to deliver POST before close on this run.
test('HTTP first text -> abort -> reader.cancel -> immediate resubmit accepts late disconnect', { timeout: 3000 }, async t => {
  const f = await fixture(t);
  let releaseClose, replacementStarted = false, intercepted = false, overlapped = false;
  f.endpoint.server.prependOnceListener('request', (_, res) => {
    const emit = res.emit;
    res.emit = function (event, ...args) {
      if (event !== 'close') return emit.call(this, event, ...args);
      intercepted = true;
      releaseClose = () => emit.call(this, event, ...args);
      if (replacementStarted) setImmediate(releaseClose);
      return true;
    };
  });
  const handle = f.manager.handle.bind(f.manager);
  let calls = 0;
  f.manager.handle = (input, context) => {
    if (++calls !== 2) return handle(input, context);
    const prior = f.manager.sessions.get(sessionId);
    assert.equal(prior.busy, true);
    assert.equal(prior.current.signal.aborted, false);
    overlapped = true;
    replacementStarted = true;
    const pending = handle(input, context);
    if (releaseClose) setImmediate(releaseClose);
    return pending;
  };
  const controller = new AbortController();
  const reader = await firstText(await f.send(true, controller.signal));
  controller.abort();
  await reader.cancel().catch(error => assert.equal(error.name, 'AbortError'));
  const response = await f.send(true);
  const text = await response.text();
  assert.equal(response.status, 200);
  assert.doesNotMatch(text, /event: error|invalid_request_error/);
  assert.match(text, /text_2/);
  assert.match(text, /event: message_stop/);
  assert.equal(intercepted, true);
  assert.equal(overlapped, true);
  assert.equal(f.queries.length, 2);
  assert.equal(f.queries[0].closed, true);
});

test('healthy HTTP overlap returns 409 without aborting or replacing the active query', { timeout: 3000 }, async t => {
  const f = await fixture(t);
  const controller = new AbortController();
  const reader = await firstText(await f.send(true, controller.signal));
  const previous = f.manager.sessions.get(sessionId).current;
  const response = await f.send();
  assert.equal(response.status, 409);
  assert.match(await response.text(), /already active/);
  assert.equal(previous.signal.aborted, false);
  assert.equal(f.manager.sessions.get(sessionId).current, previous);
  assert.equal(f.queries.length, 1);
  assert.equal(f.queries[0].closed, false);
  f.queries[0].complete();
  while (!(await reader.read()).done) { /* drain healthy completion */ }
});

test('overlap grace ends at 40ms rather than waiting behind inference', { timeout: 3000 }, async t => {
  const f = await fixture(t);
  const first = await f.start();
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let settled = false;
  const second = f.manager.handle(body, { sessionId, allowTransportHandoff: true });
  const rejected = assert.rejects(second, { statusCode: 409 }).then(() => { settled = true; });
  await turn();
  t.mock.timers.tick(39);
  await turn();
  assert.equal(settled, false);
  t.mock.timers.tick(1);
  await rejected;
  assert.equal(first.controller.signal.aborted, false);
  assert.equal(f.queries.length, 1);
  t.mock.timers.reset();
  f.queries[0].complete();
  await first.pending;
});

test('healthy completion during grace still rejects the overlapping request', { timeout: 3000 }, async t => {
  const f = await fixture(t);
  const first = await f.start();
  const rejected = assert.rejects(f.manager.handle(body, { sessionId, allowTransportHandoff: true }), { statusCode: 409 });
  await turn();
  f.queries[0].complete();
  await Promise.all([first.pending, rejected]);
  assert.equal(first.controller.signal.aborted, false);
  assert.equal(f.queries.length, 1);
});

test('two handoff contenders recheck ownership: exactly one starts a replacement', { timeout: 3000 }, async t => {
  const f = await fixture(t, { holdReplacement: true });
  const first = await f.start();
  const progress = deferred();
  const second = f.manager.handle(body, { sessionId, allowTransportHandoff: true,
    onEvent(event) { if (event.type === 'content_block_delta') progress.resolve(); } });
  const rejected = assert.rejects(f.manager.handle(body, { sessionId, allowTransportHandoff: true }), { statusCode: 409 });
  await turn();
  first.controller.abort();
  await assert.rejects(first.pending, { statusCode: 499 });
  await Promise.all([progress.promise, rejected]);
  assert.equal(f.manager.sessions.get(sessionId).busy, true);
  assert.equal(f.queries.length, 2);
  f.queries[1].complete();
  assert.equal((await second).content[0].text, 'text_2');
});

test('canceling a handoff contender never cancels the healthy owner', { timeout: 3000 }, async t => {
  const f = await fixture(t);
  const first = await f.start();
  const controller = new AbortController();
  const rejected = assert.rejects(f.manager.handle(body, { sessionId, signal: controller.signal,
    allowTransportHandoff: true }), { statusCode: 499 });
  await turn();
  controller.abort();
  await rejected;
  assert.equal(first.controller.signal.aborted, false);
  assert.equal(f.queries.length, 1);
  f.queries[0].complete();
  await first.pending;
});
