import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { access, lstat, mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { getSessionPath, parseJsonl, readSession } from 'cc-session-io';
import { canonicalMessages, digest, importSession, requestFingerprint, validateHistory } from '../src/history.mjs';

const text = (value) => ({ type: 'text', text: value });
const image = { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'aW1hZ2U=' } };
const urlImage = { type: 'image', source: { type: 'url', url: 'https://example.invalid/image.png' } };
const call = (id = 'toolu_a', name = 'read') => ({ type: 'tool_use', id, name, input: { path: 'file.txt' } });
const result = (id = 'toolu_a', content = 'done', is_error = false) => ({ type: 'tool_result', tool_use_id: id, content, is_error });
const user = (content) => ({ role: 'user', content });
const assistant = (content) => ({ role: 'assistant', content });
const mapTool = (name) => ({ read: 'mcp__azo__read', write: 'mcp__azo__write' })[name];
const badRequest = (error) => error.statusCode === 400 && typeof error.publicMessage === 'string' && error.publicMessage.length > 0;

// Every filesystem test isolates CLAUDE_CONFIG_DIR before invoking the importer.
// Tests are intentionally sequential because process.env is process-global.
async function sandbox(run) {
  const root = await mkdtemp(join(tmpdir(), 'azo-sdk-history-'));
  const previous = process.env.CLAUDE_CONFIG_DIR;
  const config = join(root, 'claude');
  const cwd = join(root, 'project');
  await mkdir(cwd);
  process.env.CLAUDE_CONFIG_DIR = config;
  try { await run({ root, config, cwd }); }
  finally {
    if (previous === undefined) delete process.env.CLAUDE_CONFIG_DIR;
    else process.env.CLAUDE_CONFIG_DIR = previous;
    await rm(root, { recursive: true, force: true });
  }
}

function history() {
  return [
    user([text('Inspect these'), image, urlImage]),
    assistant([
      { type: 'thinking', thinking: 'Use the tools.', signature: 'signed-by-origin' },
      { type: 'redacted_thinking', data: 'opaque-encrypted-data' },
      text('Reading and writing.'), call(), call('toolu_b', 'write'),
    ]),
    user([result('toolu_a', [text('read output'), image]), result('toolu_b', [text('permission denied'), urlImage], true), text('Continue'), image]),
    assistant('The write failed.'),
  ];
}

test('digest is stable JSON SHA-256, preserves array order, and rejects non-JSON values', () => {
  assert.equal(digest({ z: [1, { b: 2, a: 1 }], a: true }), digest({ a: true, z: [1, { a: 1, b: 2 }] }));
  assert.equal(digest({}), '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a');
  assert.notEqual(digest([1, 2]), digest([2, 1]));
  for (const value of [undefined, NaN, Infinity, { x: undefined }, new Date(), [Symbol('x')]]) {
    assert.throws(() => digest(value), badRequest);
  }
  const cycle = {}; cycle.self = cycle;
  assert.throws(() => digest(cycle), badRequest);
});

test('canonical messages deep-copy, ignore protocol cache markers, and normalize text', () => {
  const original = [user('hello'), assistant([text('answer')]), user([result('toolu_a', 'output')])];
  const marked = [
    { role: 'user', content: [{ text: 'hello', cache_control: { type: 'ephemeral' }, type: 'text' }] },
    { content: [{ cache_control: { type: 'ephemeral', ttl: '1h' }, type: 'text', text: 'answer' }], role: 'assistant' },
    user([{ ...result('toolu_a', [{ ...text('output'), cache_control: { type: 'ephemeral' } }]), cache_control: { type: 'ephemeral' } }]),
  ];
  assert.equal(digest(canonicalMessages(original)), digest(canonicalMessages(marked)));
  const copy = canonicalMessages(marked);
  copy[0].content[0].text = 'changed';
  assert.equal(marked[0].content[0].text, 'hello');
  assert.ok(marked[0].content[0].cache_control);
  assert.deepEqual(canonicalMessages(copy), copy);
});

test('history comparison detects content, IDs, errors, images, thinking, truncation and branches', () => {
  const baseline = history();
  const fingerprint = digest(canonicalMessages(baseline));
  for (const mutate of [
    (messages) => { messages[0].content[0].text += '!'; },
    (messages) => { messages[0].content[1].source.data = 'ZGlmZmVyZW50'; },
    (messages) => { messages[1].content[0].signature = 'different'; },
    (messages) => { messages[1].content[0].thinking = 'different'; },
    (messages) => { messages[1].content[1].data = 'different'; },
    (messages) => { messages[1].content[3].id = 'toolu_other'; },
    (messages) => { messages[1].content[3].name = 'write'; },
    (messages) => { messages[1].content[3].input.path = 'other.txt'; },
    (messages) => { messages[2].content[0].tool_use_id = 'toolu_other'; },
    (messages) => { messages[2].content[1].is_error = false; },
    (messages) => { messages[2].content.reverse(); },
    (messages) => { messages.pop(); },
    (messages) => { messages.splice(0, 2, user('Compacted summary')); },
  ]) {
    const changed = structuredClone(baseline);
    mutate(changed);
    assert.notEqual(digest(canonicalMessages(changed)), fingerprint);
  }
});

test('cache_control in tool input/schema is actual data, not a cache marker', () => {
  const first = [assistant([{ ...call(), input: { cache_control: 'actual argument' } }])];
  const second = [assistant([{ ...call(), input: { cache_control: 'different argument' } }])];
  assert.notEqual(digest(canonicalMessages(first)), digest(canonicalMessages(second)));
  assert.notEqual(requestFingerprint({ tools: [{ input_schema: { properties: { cache_control: { type: 'string' } } } }] }),
    requestFingerprint({ tools: [{ input_schema: { properties: {} } }] }));
});

test('request fingerprint ignores history, transport metadata, streaming and cache markers', () => {
  const base = {
    model: 'claude-test', system: 'Be precise.', max_tokens: 1024,
    tools: [{ name: 'read', input_schema: { type: 'object', properties: {} } }],
    thinking: { type: 'enabled', budget_tokens: 512 }, output_config: { effort: 'high' },
  };
  const variant = {
    ...structuredClone(base), stream: true, metadata: { user_id: 'transport-correlation' }, messages: [user('different turn')],
    system: [{ ...text(base.system), cache_control: { type: 'ephemeral' } }],
  };
  variant.tools[0].cache_control = { type: 'ephemeral', ttl: '1h' };
  assert.equal(requestFingerprint(base), requestFingerprint(variant));
  for (const [key, value] of Object.entries({
    model: 'other', system: 'Different system', tools: [], max_tokens: 2048,
    thinking: { type: 'disabled' }, output_config: { effort: 'low' },
    temperature: 0.3, top_p: 0.8, top_k: 4, stop_sequences: ['stop'],
    tool_choice: { type: 'any' }, context_management: { edits: [] }, future_semantic_option: true,
  })) assert.notEqual(requestFingerprint(base), requestFingerprint({ ...base, [key]: value }), key);
});

test('imports native text/images/tool errors/signed thinking with full readback and private permissions', async () => {
  await sandbox(async ({ config, cwd }) => {
    const messages = history();
    const original = structuredClone(messages);
    const imported = await importSession({ messages, cwd, toolNameToSdk: mapTool, model: 'host-requested-model' });
    assert.match(imported.sessionId, /^[0-9a-f-]{36}$/);
    assert.equal(imported.path, getSessionPath(imported.sessionId, cwd, config));
    assert.equal((await lstat(imported.path)).mode & 0o777, 0o600);
    const session = readSession(imported.path, cwd);
    assert.equal(session.sessionId, imported.sessionId);
    assert.equal(session.messages.length, messages.length);
    assert.deepEqual(session.messages.filter(record => record.type === 'assistant').map(record => record.message.model),
      ['host-requested-model', 'host-requested-model']);
    const expected = canonicalMessages(messages);
    expected[1].content.filter((block) => block.type === 'tool_use').forEach((block) => { block.name = mapTool(block.name); });
    assert.deepEqual(session.messages.map(({ message }) => ({ role: message.role, content: message.content })), expected);
    let parentUuid = null;
    for (const record of session.records) {
      assert.equal(record.cwd, await realpath(cwd));
      assert.equal(record.sessionId, imported.sessionId);
      assert.equal(record.parentUuid, parentUuid);
      parentUuid = record.uuid;
    }
    const raw = await readFile(imported.path, 'utf8');
    assert.ok(raw.endsWith('\n'));
    assert.equal(parseJsonl(raw).length, messages.length);
    assert.deepEqual(messages, original);
  });
});

test('parallel result messages are grouped without blank user records or fabricated outcomes', async () => {
  await sandbox(async ({ cwd }) => {
    const imported = await importSession({ cwd, toolNameToSdk: mapTool, messages: [
      user('Go'), assistant([call(), call('toolu_b', 'write')]),
      user([result('toolu_b', '', true)]), user([result('toolu_a', [text('done'), image])]),
      assistant('Finished'),
    ] });
    const records = readSession(imported.path, cwd).messages;
    assert.equal(records.length, 4);
    const results = records[2].message.content;
    assert.deepEqual(results.map((block) => block.tool_use_id), ['toolu_b', 'toolu_a']);
    assert.equal(results[0].is_error, true);
    assert.deepEqual(results[0].content, [text('')]);
    assert.deepEqual(results[1].content, [text('done'), image]);
    assert.ok(records.every((record) => record.message.content.length));
  });
});

test('an unresolved import prefix is retained without fabricating tool outcomes', async () => {
  await sandbox(async ({ cwd }) => {
    for (const messages of [
      [user('Go'), assistant([call(), call('toolu_b', 'write')])],
      [user('Go'), assistant([call(), call('toolu_b', 'write')]), user([result()])],
    ]) {
      const imported = await importSession({ cwd, messages, toolNameToSdk: mapTool });
      const records = readSession(imported.path, cwd).messages;
      assert.equal(records.length, messages.length);
      assert.equal(records.filter((record) => record.type === 'user').length, messages.filter((message) => message.role === 'user').length);
    }
  });
});

test('empty prefix creates an empty fresh file, never an artificial user message', async () => {
  await sandbox(async ({ cwd }) => {
    const imported = await importSession({ cwd, messages: [] });
    assert.equal(await readFile(imported.path, 'utf8'), '');
    assert.equal((await lstat(imported.path)).mode & 0o777, 0o600);
  });
});

test('full replay after compaction or branching always creates independent sessions', async () => {
  await sandbox(async ({ cwd }) => {
    const original = await importSession({ cwd, messages: [user('Earlier'), assistant('Old answer')] });
    const bytes = await readFile(original.path, 'utf8');
    const compacted = await importSession({ cwd, messages: [user('Summary'), assistant('New answer')] });
    const branch = await importSession({ cwd, messages: [user('Earlier'), assistant('Different answer')] });
    assert.equal(new Set([original.sessionId, compacted.sessionId, branch.sessionId]).size, 3);
    assert.equal(await readFile(original.path, 'utf8'), bytes);
    assert.deepEqual(readSession(compacted.path, cwd).messages[0].message.content, [text('Summary')]);
  });
});

test('caller UUID is exclusive; existing files and symlinks are never overwritten', async () => {
  await sandbox(async ({ root, config, cwd }) => {
    const sessionId = randomUUID();
    const imported = await importSession({ cwd, sessionId, messages: [user('Original')] });
    const original = await readFile(imported.path, 'utf8');
    await assert.rejects(importSession({ cwd, sessionId, messages: [user('Replacement')] }), (error) => error.statusCode === 409);
    assert.equal(await readFile(imported.path, 'utf8'), original);
    const target = join(root, 'do-not-touch');
    await writeFile(target, 'untouched');
    const linkId = randomUUID();
    await symlink(target, getSessionPath(linkId, cwd, config));
    await assert.rejects(importSession({ cwd, sessionId: linkId, messages: [user('Replacement')] }), (error) => error.statusCode === 409);
    assert.equal(await readFile(target, 'utf8'), 'untouched');
    assert.ok((await lstat(getSessionPath(linkId, cwd, config))).isSymbolicLink());
  });
});

test('symlink cwd uses the real project path for both records and session discovery', async () => {
  await sandbox(async ({ root, cwd, config }) => {
    const linked = join(root, 'linked-project');
    await symlink(cwd, linked);
    const imported = await importSession({ cwd: linked, messages: [user('Hello')] });
    assert.equal(imported.path, getSessionPath(imported.sessionId, await realpath(cwd), config));
    assert.equal(readSession(imported.path).messages[0].cwd, await realpath(cwd));
  });
});

test('unsafe IDs, bad roles, unsupported blocks and unknown tool names fail safely before writes', async () => {
  await sandbox(async ({ config, cwd }) => {
    const cases = [
      { sessionId: '../escape', messages: [] },
      { sessionId: '', messages: [] },
      { sessionId: 'not-a-uuid', messages: [] },
      { cwd: 'relative/path', messages: [] },
      { messages: [{ role: 'system', content: 'secret fixture contents' }] },
      { messages: [user([{ type: 'document', source: { type: 'text', data: 'secret fixture contents' } }])] },
      { messages: [assistant([{ type: 'server_tool_use', id: 'toolu_x', name: 'read', input: {} }])] },
      { messages: [assistant([call('../escape')])] },
      { messages: [assistant([call('toolu_a', 'unknown')])] },
      { messages: [assistant([call()])], toolNameToSdk: () => '../invalid-name' },
      { messages: [assistant([call()])], toolNameToSdk: () => { throw new Error('secret fixture contents'); } },
      { messages: [assistant([call()])], toolNameToSdk: undefined },
      { messages: [assistant([{ type: 'thinking', thinking: 'unsigned' }])] },
      { messages: [user([{ type: 'thinking', thinking: 'not user content', signature: 'signed' }])] },
      { messages: [assistant([image])] },
      { messages: [user([{ type: 'image', source: { type: 'file', path: '/private/secret' } }])] },
      { messages: [user('')] }, { messages: [user([])] },
      { messages: [{ role: 'user', content: 'hi', extra: 'would be silently lost' }] },
    ];
    for (const options of cases) {
      await assert.rejects(importSession({ cwd, toolNameToSdk: mapTool, ...options }), (error) => {
        assert.ok(!error.publicMessage?.includes('secret fixture contents'));
        return badRequest(error);
      });
    }
    await assert.rejects(access(config), { code: 'ENOENT' });
  });
});

test('orphan, duplicate, misplaced and prematurely interrupted tool batches reject rather than repair', async () => {
  await sandbox(async ({ config, cwd }) => {
    for (const messages of [
      [user([result()])],
      [assistant([call(), call()])],
      [assistant([call()]), user([result(), result()])],
      [assistant([call()]), user([result('toolu_other')])],
      [assistant([call()]), assistant('Missing outcome')],
      [assistant([call()]), user('Missing outcome')],
      [assistant([call()]), user([text('Out of order'), result()])],
      [assistant([call(), call('toolu_b')]), user([result(), text('Missing second outcome')])],
      [assistant([call()]), user([result('toolu_a', [{ type: 'tool_use', id: 'nested', name: 'read', input: {} }])])],
      [assistant([call()]), user([result('toolu_a', 'bad flag', 'yes')])],
    ]) await assert.rejects(importSession({ cwd, messages, toolNameToSdk: mapTool }), badRequest);
    await assert.rejects(access(config), { code: 'ENOENT' });
  });
});


test('full-history validation requires exact final results without changing the caller transcript', () => {
  const prefix = [user('Go'), assistant([call(), call('toolu_b', 'removed/tool')])];
  for (const ending of [[], [result()], [result(), result()], [result(), result('unknown')]]) {
    assert.throws(() => validateHistory([...prefix, user(ending)]), badRequest);
  }
  assert.throws(() => validateHistory(prefix), badRequest);
  assert.throws(() => validateHistory([user([result()])]), badRequest);
  const complete = [...prefix, user([result('toolu_b')]), user([result(), text('Continue')])];
  const original = structuredClone(complete);
  assert.doesNotThrow(() => validateHistory(complete));
  assert.deepEqual(complete, original);
});
