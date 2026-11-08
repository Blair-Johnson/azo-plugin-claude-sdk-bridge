import test from 'node:test';
import assert from 'node:assert/strict';
import { readdir } from 'node:fs/promises';
import { ServerResponse } from 'node:http';
import { join } from 'node:path';
import { canonicalMessages } from '../src/history.mjs';
import { offlineSdk, SYNTHETIC_KEY, until } from './fixtures/local-api.mjs';

// Default --interrupt-mode rollback, against the real SDK and a synthetic API:
// after an interrupt the replacement resumes the SDK's own transcript just
// before the undelivered answer, instead of reusing or re-importing history.
const SESSION = 'offline-interrupt-rollback';
const FIRST = 'SYNTHETIC_FIRST_USER';
const PARTIAL = 'SYNTHETIC_ABORTED_PARTIAL';
const NEXT = 'SYNTHETIC_STEERING_USER';
const ANSWER = 'SYNTHETIC_REPLACEMENT_ANSWER';
const BASE = { model: 'claude-sonnet-4-6', max_tokens: 512, tools: [],
  system: 'Synthetic offline interrupt rollback test.', thinking: { type: 'disabled' } };
const PROBE = { name: 'offline_probe', description: 'Synthetic probe; no host action executes',
  input_schema: { type: 'object', properties: {}, additionalProperties: false } };
const user = content => ({ role: 'user', content });
const asMessage = ({ role, content }) => ({ role, content });
const blocks = body => body.messages.flatMap(message => typeof message.content === 'string'
  ? [{ type: 'text', text: message.content }] : message.content);
const texts = body => blocks(body).filter(block => block.type === 'text').map(block => block.text).join('\n');
// Block-level identity of the request prefix, which is what the prompt cache keys on.
const flat = body => body.messages.flatMap((message, index) => (typeof message.content === 'string'
  ? [{ type: 'text', text: message.content }] : message.content)
  .map(({ cache_control, ...block }) => `${index}:${message.role}:${JSON.stringify(block)}`));

function holdResponses(t, indexes) {
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
  ServerResponse.prototype.end = function(...args) { return held.has(this) ? this : end.apply(this, args); };
  t.after(() => { ServerResponse.prototype.write = write; ServerResponse.prototype.end = end; });
}

async function setup(t, { reply, hold }) {
  holdResponses(t, hold);
  const runtime = await offlineSdk({ after(close) { t.after(close); } }, reply);
  const manager = runtime.makeManager();
  const factory = manager.queryFactory, options = [], imports = [];
  // The shared fixture exposes only iteration/close; restore the real interrupt.
  manager.queryFactory = async args => {
    options.push(args.options);
    const wrapped = await factory(args);
    const real = runtime.queries.at(-1);
    return { close: () => wrapped.close(), interrupt: () => real.interrupt(), [Symbol.asyncIterator]: () => wrapped[Symbol.asyncIterator]() };
  };
  const importer = manager.historyImporter;
  manager.historyImporter = value => { imports.push(value.messages.length); return importer(value); };
  return { ...runtime, manager, options, imports };
}

async function interrupted(f, messages, base = BASE) {
  const controller = new AbortController(), events = [];
  const pending = f.manager.handle({ ...base, messages }, { sessionId: SESSION, signal: controller.signal,
    onEvent: event => events.push(event) });
  pending.catch(() => {});
  await until(() => events.some(event => event.type === 'content_block_delta' && event.delta?.text?.includes('PARTIAL')),
    { signal: f.controller.signal });
  controller.abort();
  await assert.rejects(pending, { statusCode: 499 });
}

const send = (f, messages, base = BASE) => f.manager.handle({ ...base, messages }, { sessionId: SESSION });

function assertRolledBack(f, before, after, partial = PARTIAL) {
  const old = f.api.requests[before], next = f.api.requests[after];
  assert.doesNotMatch(texts(next), new RegExp(`${partial}|\\[Request interrupted`), 'Interrupted answer is dropped');
  assert.equal(JSON.stringify(next.system), JSON.stringify(old.system));
  assert.equal(JSON.stringify(next.tools), JSON.stringify(old.tools));
  const a = flat(old), b = flat(next);
  let shared = 0;
  while (shared < a.length && a[shared] === b[shared]) shared++;
  // The SDK appends a newline to the kept prompt when it merges the feedback; all
  // earlier blocks, including the startup reminders, must be byte-identical.
  assert.ok(shared >= a.length - 1, `Cached prefix preserved: ${shared}/${a.length} blocks`);
}

async function transcripts(f) {
  const root = join(f.api.config, 'projects'), found = [];
  for (const dir of await readdir(root).catch(() => [])) {
    for (const name of await readdir(join(root, dir))) if (name.endsWith('.jsonl')) found.push(name);
  }
  return found;
}

test('offline real SDK: interrupt rolls back to the SDK transcript without re-import', { timeout: 45_000 }, async t => {
  const f = await setup(t, { hold: [1], reply: (_body, index) => [{ type: 'text', text: index ? ANSWER : PARTIAL }] });
  await interrupted(f, [user(FIRST)]);
  const history = [user(FIRST), user(NEXT)];
  const answer = await send(f, history);
  assert.equal(answer.content[0].text, ANSWER);
  assert.equal(f.queries.length, 2, 'Rollback starts a replacement process');
  assert.ok(f.processes[0].didExit);
  assert.equal(f.options[1].resume, f.options[0].sessionId, 'Replacement resumes the same SDK session');
  assert.equal(typeof f.options[1].resumeSessionAt, 'string');
  assert.deepEqual(f.imports, [], 'Host history is never re-imported');
  assert.equal(f.api.requests.length, 2);
  assertRolledBack(f, 0, 1);
  assert.match(texts(f.api.requests[1]), new RegExp(`${FIRST}[\\s\\S]*${NEXT}`));
  assert.deepEqual(f.manager.sessions.get(SESSION).session.expected, canonicalMessages([...history, asMessage(answer)]));
  assert.ok((await transcripts(f)).length > 0, 'Rollback mode persists the SDK transcript');
  await f.manager.cancel(SESSION);
  assert.deepEqual(await transcripts(f), [], 'Session cleanup removes every SDK transcript');
  f.api.assertHealthy();
});

test('offline real SDK: repeated interrupts roll back from the resumed transcript', { timeout: 45_000 }, async t => {
  const replies = [PARTIAL, ANSWER, 'SYNTHETIC_SECOND_PARTIAL', 'SYNTHETIC_SECOND_ANSWER', 'SYNTHETIC_FINAL_ANSWER'];
  const f = await setup(t, { hold: [1, 3], reply: (_body, index) => [{ type: 'text', text: replies[index] }] });
  const history = [user(FIRST)];
  await interrupted(f, history);
  history.push(user(NEXT));
  const answer = await send(f, history);
  history.push(asMessage(answer), user('SYNTHETIC_NORMAL_USER'));
  await interrupted(f, history);
  history.push(user('SYNTHETIC_SECOND_STEERING'));
  const second = await send(f, history);
  assert.equal(second.content[0].text, 'SYNTHETIC_SECOND_ANSWER');
  assertRolledBack(f, 2, 3, 'SYNTHETIC_SECOND_PARTIAL');
  assert.equal(f.options[2].resume, f.options[0].sessionId);
  history.push(asMessage(second), user('SYNTHETIC_FINAL_USER'));
  const final = await send(f, history);
  assert.equal(final.content[0].text, 'SYNTHETIC_FINAL_ANSWER');
  assert.equal(f.queries.length, 3, 'A normal turn after a rollback reuses the replacement');
  assert.doesNotMatch(texts(f.api.requests[4]), /PARTIAL|\[Request interrupted/);
  assert.deepEqual(f.imports, []);
  assert.deepEqual(f.manager.sessions.get(SESSION).session.expected, canonicalMessages([...history, asMessage(final)]));
  f.api.assertHealthy();
});

test('offline real SDK: interrupt after a tool result keeps the completed tool exchange', { timeout: 45_000 }, async t => {
  const base = { ...BASE, tools: [PROBE] };
  const f = await setup(t, { hold: [2], reply: (body, index) => index === 0
    ? [{ type: 'tool_use', id: 'toolu_offline_1', name: body.tools.find(tool => tool.name.includes('offline_probe')).name, input: {} }]
    : [{ type: 'text', text: index === 1 ? PARTIAL : ANSWER }] });
  const call = await send(f, [user(FIRST)], base);
  assert.equal(call.stop_reason, 'tool_use');
  const history = [user(FIRST), asMessage(call),
    user([{ type: 'tool_result', tool_use_id: call.content.find(b => b.type === 'tool_use').id, content: 'SYNTHETIC_RESULT' }])];
  await interrupted(f, history, base);
  history.push(user([{ type: 'text', text: NEXT }]));
  const answer = await send(f, history, base);
  assert.equal(answer.content[0].text, ANSWER);
  assert.equal(f.queries.length, 2);
  assert.deepEqual(f.imports, []);
  assertRolledBack(f, 1, 2);
  const kept = blocks(f.api.requests[2]);
  assert.ok(kept.some(block => block.type === 'tool_use'), 'Completed tool call is kept');
  assert.ok(kept.some(block => block.type === 'tool_result' && JSON.stringify(block).includes('SYNTHETIC_RESULT')));
  assert.deepEqual(f.callbacks.length, 1, 'The tool is not re-executed');
  f.api.assertHealthy();
});

test('offline real SDK: without a valid rollback point the interrupt rebuilds from host history', { timeout: 45_000 }, async t => {
  const f = await setup(t, { hold: [1], reply: (_body, index) => [{ type: 'text', text: index ? ANSWER : PARTIAL }] });
  f.manager.rollbackTarget = async () => null;
  await interrupted(f, [user(FIRST)]);
  const answer = await send(f, [user(FIRST), user(NEXT)]);
  assert.equal(answer.content[0].text, ANSWER);
  assert.equal(f.queries.length, 2);
  assert.deepEqual(f.imports, [1], 'Fallback re-imports the host prefix');
  assert.notEqual(f.options[1].resume, f.options[0].sessionId, 'Fallback resumes the import, not the old transcript');
  assert.doesNotMatch(texts(f.api.requests[1]), /PARTIAL|\[Request interrupted/);
  await f.manager.cancel(SESSION);
  assert.deepEqual(await transcripts(f), [], 'Fallback also removes the abandoned transcript');
  f.api.assertHealthy();
});
