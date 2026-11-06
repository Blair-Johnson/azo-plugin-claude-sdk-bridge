import test from 'node:test';
import assert from 'node:assert/strict';
import { ServerResponse } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import { canonicalMessages } from '../src/history.mjs';
import { createBridgeServer } from '../src/server.mjs';
import { offlineSdk, SYNTHETIC_KEY, until } from './fixtures/local-api.mjs';

const SESSION = 'offline-fast-steering';
const FIRST = 'SYNTHETIC_FIRST_USER';
const PARTIAL = 'SYNTHETIC_ABORTED_PARTIAL';
const NEXT = 'SYNTHETIC_STEERING_USER';
const ANSWER = 'SYNTHETIC_REPLACEMENT_ANSWER';
const KEY = 'offline-fast-steering-key-12345678901234567890';
const BASE = { model: 'claude-sonnet-4-6', max_tokens: 512, tools: [],
  system: 'Synthetic offline fast-steering acceptance test.', thinking: { type: 'disabled' } };
const user = content => ({ role: 'user', content });
const asMessage = ({ role, content }) => ({ role, content });
const blocks = body => body.messages.flatMap(message => Array.isArray(message.content) ? message.content : []);
const texts = body => blocks(body).filter(block => block.type === 'text').map(block => block.text).join('\n');
const deferred = () => {
  let resolve;
  const promise = new Promise(yes => { resolve = yes; });
  return { promise, resolve };
};

// Keep selected *upstream* responses open before content_block_stop. Match the
// synthetic API key, so bridge HTTP streams are never intercepted. All fixture
// auth, loopback routing, isolated home/config, and child-spawn guards stay intact.
function holdResponses(t, indexes = [1]) {
  const write = ServerResponse.prototype.write, end = ServerResponse.prototype.end;
  const held = new Set();
  ServerResponse.prototype.write = function(chunk, ...args) {
    const text = String(chunk);
    if (this.req?.headers['x-api-key'] === SYNTHETIC_KEY && text.startsWith('event: message_start')) {
      const event = JSON.parse(text.split('\ndata: ')[1]);
      if (indexes.includes(Number(event.message.id.replace('msg_offline_', '')))) held.add(this);
    }
    if (held.has(this) && /^event: (content_block_stop|message_(delta|stop))/.test(text)) return true;
    return write.call(this, chunk, ...args);
  };
  ServerResponse.prototype.end = function(...args) {
    if (held.has(this)) return this;
    return end.apply(this, args);
  };
  t.after(() => {
    ServerResponse.prototype.write = write;
    ServerResponse.prototype.end = end;
  });
}

// The shared fixture deliberately exposes only iteration/close. Restore the real
// SDK interrupt method locally and observe its actual messages/receipts. Gates
// delay delivery only; they neither fabricate SDK events nor alter SDK history.
async function setup(t, { reply = (_body, index) => [{ type: 'text', text: index ? ANSWER : PARTIAL }],
  hold = [1], managerOptions = {}, interrupt = 'real', receiptGate, resultGate } = {}) {
  holdResponses(t, hold);
  const runtime = await offlineSdk({ after(close) {
    t.after(async () => {
      receiptGate?.resolve(); resultGate?.resolve();
      await close();
    });
  } }, reply, { managerOptions });
  const manager = runtime.makeManager();
  const factory = manager.queryFactory;
  const calls = [], events = [];
  manager.queryFactory = async args => {
    const wrapped = await factory(args);
    const real = runtime.queries.at(-1);
    return {
      close: () => wrapped.close(),
      ...(interrupt === 'unsupported' ? {} : { async interrupt() {
        const call = { query: real, receiptReturned: false, resultObserved: false };
        calls.push(call);
        const receipt = await real.interrupt();
        call.receipt = receipt;
        call.receiptReturned = true;
        if (interrupt === 'hung') await new Promise(() => {});
        if (receiptGate) await receiptGate.promise;
        return receipt;
      } }),
      async *[Symbol.asyncIterator]() {
        for await (const event of wrapped) {
          events.push(event);
          const call = calls.findLast(item => item.query === real && !item.resultObserved);
          if (event.type === 'result' && call) {
            call.resultObserved = true;
            if (resultGate) await resultGate.promise;
          }
          yield event;
        }
      },
    };
  };
  return { ...runtime, manager, calls, events };
}

async function start(f, messages = [user(FIRST)], base = BASE) {
  const controller = new AbortController(), events = [];
  const pending = f.manager.handle({ ...base, messages }, { sessionId: SESSION, signal: controller.signal,
    onEvent: event => events.push(event) });
  pending.catch(() => {});
  await until(() => events.some(event => event.type === 'content_block_delta'), { signal: f.controller.signal });
  return { controller, events, pending };
}

function assertRetained(body, partial = PARTIAL, next = NEXT) {
  assert.ok(texts(body).includes(partial), 'Actual next API request retains the partial assistant');
  assert.match(texts(body), /\[Request interrupted by user\]/);
  assert.ok(texts(body).includes(next), 'Actual next API request contains the new host user message');
  assert.ok(!blocks(body).some(block => block.type === 'tool_result'), 'No synthetic host-tool outcome enters inference');
}

async function firstText(response, text) {
  const reader = response.body.getReader();
  let received = '';
  while (!received.includes(text)) {
    const chunk = await reader.read();
    assert.equal(chunk.done, false);
    received += new TextDecoder().decode(chunk.value);
  }
  return reader;
}

test('offline real SDK: canceled HTTP + new user reuses PID/query and isolates old events', { timeout: 45_000 }, async t => {
  const f = await setup(t);
  const endpoint = createBridgeServer({ manager: f.manager, key: KEY });
  const address = await endpoint.listen(0);
  t.after(() => endpoint.close());
  const outcomes = [];
  const handle = f.manager.handle.bind(f.manager);
  f.manager.handle = async (...args) => {
    try { return await handle(...args); }
    catch (error) { outcomes.push(error.statusCode); throw error; }
  };
  const send = (messages, signal) => fetch(`http://127.0.0.1:${address.port}/v1/messages`, {
    method: 'POST', signal, headers: { 'x-api-key': KEY, 'x-azo-session-id': SESSION, 'content-type': 'application/json' },
    body: JSON.stringify({ ...BASE, stream: true, messages }),
  });
  const controller = new AbortController();
  const reader = await firstText(await send([user(FIRST)], controller.signal), PARTIAL);
  const original = f.queries[0], pid = f.processes[0].child.pid;
  controller.abort();
  await reader.cancel().catch(error => assert.equal(error.name, 'AbortError'));
  const response = await send([user(FIRST), user(NEXT)]);
  const stream = await response.text();
  assert.equal(response.status, 200);
  assert.match(stream, new RegExp(ANSWER));
  assert.doesNotMatch(stream, /SYNTHETIC_ABORTED_PARTIAL|event: error|error_during_execution|interrupted/);
  assert.equal((stream.match(/event: message_start/g) ?? []).length, 1);
  assert.equal((stream.match(/event: message_stop/g) ?? []).length, 1);
  assert.deepEqual(outcomes, [499], 'Canceled handle rejects with 499 behind the disconnected HTTP transport');
  assert.equal(f.calls.length, 1);
  assert.deepEqual(f.calls[0].receipt.still_queued, []);
  assert.equal(f.queries.length, 1);
  assert.equal(f.queries[0], original);
  assert.equal(f.processes[0].child.pid, pid);
  assert.equal(f.processes[0].didExit, false);
  assert.equal(f.api.requests.length, 2);
  assertRetained(f.api.requests[1]);
  assert.deepEqual(f.callbacks, []);
  f.api.assertHealthy();
});

test('offline real SDK: repeated feedback and normal continuation retain only SDK-side partials', { timeout: 45_000 }, async t => {
  const f = await setup(t, { hold: [1, 3], reply: (_body, index) => [{ type: 'text', text:
    [PARTIAL, ANSWER, 'SYNTHETIC_SECOND_PARTIAL', 'SYNTHETIC_SECOND_ANSWER', 'SYNTHETIC_FINAL_ANSWER'][index] }] });
  const history = [user(FIRST)];
  const first = await start(f, history);
  const pid = f.processes[0].child.pid;
  first.controller.abort();
  await assert.rejects(first.pending, { statusCode: 499 });
  history.push(user(NEXT));
  const events = [];
  const answer = await f.manager.handle({ ...BASE, messages: history }, { sessionId: SESSION, onEvent: event => events.push(event) });
  assert.equal(answer.content[0].text, ANSWER);
  assertRetained(f.api.requests[1]);
  history.push(asMessage(answer), user('SYNTHETIC_NORMAL_USER'));
  const second = await start(f, history);
  assert.equal(f.queries.length, 1, 'A normal turn after steering must reuse despite divergent SDK/host histories');
  assertRetained(f.api.requests[2]);
  second.controller.abort();
  await assert.rejects(second.pending, { statusCode: 499 });
  history.push(user('SYNTHETIC_SECOND_STEERING'));
  const secondAnswer = await f.manager.handle({ ...BASE, messages: history }, { sessionId: SESSION, onEvent: event => events.push(event) });
  assert.equal(secondAnswer.content[0].text, 'SYNTHETIC_SECOND_ANSWER');
  assertRetained(f.api.requests[3], 'SYNTHETIC_SECOND_PARTIAL', 'SYNTHETIC_SECOND_STEERING');
  history.push(asMessage(secondAnswer), user('SYNTHETIC_FINAL_USER'));
  const final = await f.manager.handle({ ...BASE, messages: history }, { sessionId: SESSION, onEvent: event => events.push(event) });
  assert.equal(final.content[0].text, 'SYNTHETIC_FINAL_ANSWER');
  assertRetained(f.api.requests[4], 'SYNTHETIC_SECOND_PARTIAL', 'SYNTHETIC_FINAL_USER');
  assert.deepEqual(f.manager.sessions.get(SESSION).session.expected, canonicalMessages([...history, asMessage(final)]));
  assert.doesNotMatch(JSON.stringify(history), /PARTIAL|interrupted/);
  assert.doesNotMatch(JSON.stringify(events), /PARTIAL|interrupted|error_during_execution/);
  assert.equal(f.calls.length, 2);
  assert.ok(f.calls.every(call => call.receiptReturned && call.resultObserved));
  assert.equal(f.queries.length, 1);
  assert.equal(f.processes[0].child.pid, pid);
  assert.equal(f.processes[0].didExit, false);
  assert.equal(f.api.requests.length, 5);
  assert.deepEqual(f.callbacks, []);
  f.api.assertHealthy();
});

test('offline real SDK: 499 is immediate, but replacement waits for both interrupt receipt and old result', { timeout: 45_000 }, async t => {
  const receiptGate = deferred(), resultGate = deferred();
  const f = await setup(t, { receiptGate, resultGate, managerOptions: { interruptTimeoutMs: 2000 } });
  const first = await start(f);
  first.controller.abort();
  await assert.rejects(first.pending, { statusCode: 499 });
  const replacement = f.manager.handle({ ...BASE, messages: [user(FIRST), user(NEXT)] }, { sessionId: SESSION });
  replacement.catch(() => {});
  await until(() => f.calls[0]?.receiptReturned && f.calls[0]?.resultObserved, { signal: f.controller.signal });
  assert.equal(f.api.requests.length, 1, 'Neither SDK completion observation is sufficient while both are gated');
  receiptGate.resolve();
  await delay(30);
  assert.equal(f.api.requests.length, 1, 'Receipt alone must not open the replacement sink/prompt');
  resultGate.resolve();
  assert.equal((await replacement).content[0].text, ANSWER);
  assert.equal(f.calls.length, 1);
  assert.equal(f.queries.length, 1);
  assertRetained(f.api.requests[1]);
  f.api.assertHealthy();
});

for (const scenario of ['strict rebuild', 'same prompt retry', 'changed history', 'changed settings', 'unsupported interrupt', 'hung interrupt', 'observed tool block']) {
  test(`offline real SDK: ${scenario} conservatively rebuilds`, { timeout: 45_000 }, async t => {
    const toolCase = scenario === 'observed tool block';
    const tools = toolCase ? [{ name: 'offline_probe', description: 'Synthetic probe; no host action executes',
      input_schema: { type: 'object', properties: {}, additionalProperties: false } }] : [];
    const base = { ...BASE, tools };
    const f = await setup(t, {
      managerOptions: scenario === 'strict rebuild' ? { interruptMode: 'rebuild' } :
        scenario === 'hung interrupt' ? { interruptTimeoutMs: 100 } : {},
      interrupt: scenario === 'unsupported interrupt' ? 'unsupported' : scenario === 'hung interrupt' ? 'hung' : 'real',
      reply: (body, index) => index === 0 && toolCase
        ? [{ type: 'tool_use', id: 'synthetic_unfinished_tool', name: body.tools[0].name, input: {} }]
        : [{ type: 'text', text: index ? ANSWER : PARTIAL }],
    });
    const first = await start(f, [user(FIRST)], base);
    first.controller.abort();
    await assert.rejects(first.pending, { statusCode: 499 });
    const messages = scenario === 'same prompt retry' ? [user(FIRST)] :
      scenario === 'changed history' ? [user('SYNTHETIC_REPLACED_HISTORY'), user(NEXT)] : [user(FIRST), user(NEXT)];
    const request = { ...base, messages,
      ...(scenario === 'changed settings' ? { system: 'Synthetic changed system setting.' } : {}) };
    const events = [];
    const answer = await f.manager.handle(request, { sessionId: SESSION, onEvent: event => events.push(event) });
    assert.equal(answer.content[0].text, ANSWER);
    assert.equal(f.queries.length, 2);
    assert.equal(f.processes[0].didExit, true, 'Rebuild must terminate the old SDK process before continuing');
    assert.equal(f.api.requests.length, 2, 'No hidden cancellation/tool-error inference may run');
    assert.doesNotMatch(texts(f.api.requests[1]), /SYNTHETIC_ABORTED_PARTIAL|\[Request interrupted by user/);
    assert.ok(!blocks(f.api.requests[1]).some(block => ['tool_result', 'tool_use'].includes(block.type)));
    assert.doesNotMatch(JSON.stringify(events), /SYNTHETIC_ABORTED_PARTIAL|synthetic_unfinished_tool|interrupted/);
    assert.deepEqual(f.callbacks, [], 'An unfinished streamed tool must never become a host action');
    if (['strict rebuild', 'unsupported interrupt', 'observed tool block'].includes(scenario)) assert.equal(f.calls.length, 0);
    else assert.equal(f.calls.length, 1);
    f.api.assertHealthy();
  });
}
