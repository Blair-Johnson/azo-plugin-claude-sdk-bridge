import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { setImmediate } from 'node:timers/promises';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { ToolBridge } from '../src/tools.mjs';

const schema = { type: 'object', properties: { value: { type: 'string' } }, required: ['value'], additionalProperties: false };
const tool = (name = 'read') => ({ name, description: 'Host-owned tool', input_schema: structuredClone(schema) });
const use = (id, value = id, name = 'read') => ({ type: 'tool_use', id, name, input: { value } });
const result = (id, content = id, is_error = false) => ({ type: 'tool_result', tool_use_id: id, content, is_error });
const bad = (fn, pattern) => assert.throws(fn, (error) => {
  assert.equal(error.statusCode, 400);
  assert.equal(error.publicMessage, error.message);
  if (pattern) assert.match(error.publicMessage, pattern);
  return true;
});

// Only the MCP protocol runs: no Agent SDK query, credential access, sockets,
// subprocesses, timers-as-ordering-assumptions, or actual host tool effects.
async function fixture(t, tools = [tool()]) {
  const bridge = new ToolBridge(tools);
  const client = new Client({ name: 'deterministic-test', version: '1' });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await bridge.sdkServer.instance.connect(serverTransport);
  await client.connect(clientTransport);
  t.after(async () => {
    bridge.close();
    await client.close();
    await bridge.sdkServer.instance.close();
  });
  const call = (id, value = id, name = 'read', extra = {}) => client.callTool({
    name: bridge.toSdkName(name).slice('mcp__azo__'.length),
    arguments: { value },
    _meta: { 'claudecode/toolUseId': id },
    ...extra,
  });
  return { bridge, client, call };
}

test('raw input schemas survive tools/list without weakening any constraints', async (t) => {
  const input_schema = {
    type: 'object',
    properties: { nested: { $ref: '#/$defs/nested' } },
    $defs: { nested: { type: 'object', properties: { tag: { const: 'x' } }, required: ['tag'], additionalProperties: false,
      anyOf: [{ required: ['tag'] }, { properties: { other: { enum: [1, 2] } }, required: ['other'] }],
      allOf: [{ not: { required: ['forbidden'] } }],
    } },
    additionalProperties: false,
  };
  const original = structuredClone(input_schema);
  const { bridge, client } = await fixture(t, [{ name: 'strict', description: 'exact schema', input_schema }]);
  input_schema.$defs.nested.anyOf.length = 0;
  const listed = await client.listTools();
  assert.deepEqual(listed.tools, [{ name: bridge.toSdkName('strict').slice(10), description: 'exact schema', inputSchema: original }]);
  assert.equal(bridge.sdkServer.type, 'sdk');
  assert.equal(bridge.sdkServer.name, 'azo');
  assert.equal(typeof bridge.sdkServer.instance.connect, 'function');
});

test('names are bounded, reversible, and distinct across punctuation, Unicode, and truncation', () => {
  const names = ['a-b', 'a_b', 'a.b', 'a/b', 'mcp__azo__read', '__proto__', 'é', 'é', 'a'.repeat(100), 'a'.repeat(99) + 'b'];
  const bridge = new ToolBridge(names.map(tool));
  const sdkNames = names.map((name) => bridge.toSdkName(name));
  assert.equal(new Set(sdkNames).size, names.length);
  sdkNames.forEach((sdk, index) => {
    assert.match(sdk, /^mcp__azo__azo_[A-Za-z0-9_-]+$/);
    assert.ok(sdk.length <= 64);
    assert.equal(bridge.fromSdkName(sdk), names[index]);
  });
  bad(() => bridge.fromSdkName('read'), /Unknown/);
  bad(() => bridge.fromSdkName('mcp__other__read'), /Unknown/);
  bad(() => bridge.toSdkName('no-such-tool'), /Unknown/);
  bad(() => new ToolBridge([tool('same'), tool('same')]), /Duplicate/);
  bad(() => new ToolBridge([tool('bad\nname')]), /Invalid/);
  bad(() => new ToolBridge([tool('\ud800')]), /Invalid/);
  bridge.close();
});

test('ordinary names stay readable up to the fully qualified length limit', async (t) => {
  const names = ['read', 'read_file', 'a-b', 'ABC_012', 'a'.repeat(50)];
  const definitions = names.map(tool);
  delete definitions[1].description;
  definitions[2].description = '';
  const { bridge, client } = await fixture(t, definitions);
  const listed = await client.listTools();
  for (const [index, name] of names.entries()) {
    assert.equal(bridge.toSdkName(name), `mcp__azo__azo_${name}`);
    assert.equal(bridge.fromSdkName(`mcp__azo__azo_${name}`), name);
    assert.deepEqual(listed.tools[index], {
      name: `azo_${name}`,
      ...(definitions[index].description === undefined ? {} : { description: definitions[index].description }),
      inputSchema: schema,
    });
  }
  assert.equal(bridge.toSdkName(names.at(-1)).length, 64);
  // Reverse lookup is an explicit registry, never a prefix-stripping guess.
  bad(() => bridge.fromSdkName('mcp__azo__azo_unregistered'), /Unknown/);
});

test('unsafe and oversized aliases are deterministic hashes with original-name clarification', async (t) => {
  const names = ['a'.repeat(51), 'a/b', 'a.b', 'é', 'é', 'quote"name'];
  const definitions = names.map(tool);
  delete definitions[1].description;
  definitions[2].description = '';
  const { bridge, client } = await fixture(t, definitions);
  const reversed = new ToolBridge([...definitions].reverse());
  t.after(() => reversed.close());
  const listed = await client.listTools();
  for (const [index, name] of names.entries()) {
    const localName = `azo_h_${createHash('sha256').update(name).digest('base64url')}`;
    assert.equal(bridge.toSdkName(name), `mcp__azo__${localName}`);
    assert.equal(reversed.toSdkName(name), bridge.toSdkName(name));
    assert.ok(bridge.toSdkName(name).length <= 64);
    assert.equal(bridge.fromSdkName(bridge.toSdkName(name)), name);
    const clarification = `Host tool name: ${JSON.stringify(name)}.`;
    assert.deepEqual(listed.tools[index], {
      name: localName,
      description: definitions[index].description === undefined
        ? clarification : `${definitions[index].description}\n\n${clarification}`,
      inputSchema: schema,
    });
  }
  assert.equal(definitions[0].description, 'Host-owned tool');
  assert.equal(Object.hasOwn(definitions[1], 'description'), false);
});

test('literal names colliding with a hash alias are rejected in either definition order', () => {
  const unsafeName = 'a/b';
  const literalName = `h_${createHash('sha256').update(unsafeName).digest('base64url')}`;
  for (const names of [[unsafeName, literalName], [literalName, unsafeName]]) {
    bad(() => new ToolBridge(names.map(tool)), /collision/);
  }
});

test('unsupported tool configuration fails closed; custom type and cache hints are permitted', async (t) => {
  for (const [key, value] of [
    ['strict', true], ['strict', false], ['defer_loading', true],
    ['input_examples', [{ value: 'example' }]], ['unknown_option', null], ['unknown_option', undefined],
  ]) bad(() => new ToolBridge([{ ...tool(), [key]: value }]), /Unsupported tool configuration/);
  const { client } = await fixture(t, [{ ...tool(), type: 'custom', cache_control: { type: 'ephemeral' } }]);
  assert.deepEqual((await client.listTools()).tools, [{ name: 'azo_read', description: 'Host-owned tool', inputSchema: schema }]);
});

test('same-name parallel calls pair by exact ID despite reverse callback and result orders', async (t) => {
  const { bridge, call } = await fixture(t);
  bridge.expect([use('a'), use('b')]);
  assert.deepEqual(bridge.pendingIds, new Set(['a', 'b']));
  const second = call('b');
  const first = call('a');
  await setImmediate();
  bridge.deliver([result('b', 'second'), result('a', 'first')]);
  assert.equal((await first).content[0].text, 'first');
  assert.equal((await second).content[0].text, 'second');
  assert.equal(bridge.pendingIds.size, 0);
});

test('results before callbacks queue by registered ID and duplicate delivery is idempotent', async (t) => {
  const { bridge, call } = await fixture(t);
  bridge.expect([use('a'), use('b')]);
  bridge.deliver([result('a', 'A'), result('b', 'B')]);
  bridge.deliver([result('a', 'A')]);
  assert.equal((await call('b')).content[0].text, 'B');
  assert.equal((await call('a')).content[0].text, 'A');
  bridge.deliver([result('a', 'A')]);
  bad(() => bridge.deliver([result('a', 'changed')]), /Conflicting/);
});

test('callbacks may precede expect, but results may not bypass model registration', async (t) => {
  const { bridge, call } = await fixture(t);
  let settled = false;
  const waiting = call('early').then((value) => { settled = true; return value; });
  await setImmediate();
  assert.equal(settled, false);
  assert.equal(bridge.pendingIds.size, 0);
  bad(() => bridge.deliver([result('early')]), /unregistered/);
  bridge.expect([use('early')]);
  assert.equal(settled, false);
  bridge.deliver([result('early', 'registered')]);
  assert.equal((await waiting).content[0].text, 'registered');
});

test('an early callback with mismatching arguments or name rejects the full model batch', async (t) => {
  const { bridge, call } = await fixture(t, [tool(), tool('write')]);
  const early = call('early', 'actual');
  await setImmediate();
  bad(() => bridge.expect([use('other'), use('early', 'different')]), /does not match/);
  assert.equal(bridge.pendingIds.size, 0);
  bad(() => bridge.expect([use('early', 'actual', 'write')]), /does not match/);
  bridge.expect([use('early', 'actual')]);
  bridge.deliver([result('early')]);
  assert.equal((await early).isError, false);
});

test('expect is atomic, rejects unknown/SDK names, duplicate IDs, and non-object input', async (t) => {
  const { bridge } = await fixture(t);
  for (const malformed of [use('b', 'x', 'unknown'), { ...use('b'), input: [] }, { ...use('b'), input: null }, { ...use('b'), input: { x: undefined } }, use('a')]) {
    bad(() => bridge.expect([use('a'), malformed]));
    assert.equal(bridge.pendingIds.size, 0);
  }
  bad(() => bridge.expect([use('a', 'x', bridge.toSdkName('read'))]), /Unknown/);
  bridge.expect([{ type: 'text', text: 'thinking' }, use('a')]);
  bad(() => bridge.expect([use('b'), use('a')]), /already registered/);
  assert.deepEqual(bridge.pendingIds, new Set(['a']));
  const externallyChanged = bridge.pendingIds;
  externallyChanged.clear();
  assert.equal(bridge.pendingIds.size, 1);
});

test('invalid result batches never partially settle callbacks or queue results', async (t) => {
  const { bridge, call } = await fixture(t);
  bridge.expect([use('a'), use('b')]);
  let settled = false;
  const waiting = call('a').then((value) => { settled = true; return value; });
  await setImmediate();
  const malformed = [result('unknown'), result('b', [{ type: 'document' }]), result('b', 'x', 'yes'), result('a')];
  for (const block of malformed) {
    bad(() => bridge.deliver([result('a', 'must not leak'), block]));
    await setImmediate();
    assert.equal(settled, false);
    assert.deepEqual(bridge.pendingIds, new Set(['a', 'b']));
  }
  bridge.deliver([result('a', 'valid'), result('b', 'valid B')]);
  assert.equal((await waiting).content[0].text, 'valid');
  assert.equal((await call('b')).content[0].text, 'valid B');
});

test('conflicting retry rejects the whole batch, exact retry ignores JSON key order', async (t) => {
  const { bridge, call } = await fixture(t);
  bridge.expect([use('a'), use('b')]);
  bridge.deliver([result('a', 'first')]);
  bad(() => bridge.deliver([result('b', 'B'), result('a', 'changed')]), /Conflicting/);
  assert.deepEqual(bridge.pendingIds, new Set(['b']));
  bridge.deliver([{ content: 'first', is_error: false, tool_use_id: 'a', type: 'tool_result' }, result('b', 'B')]);
  assert.equal((await call('a')).content[0].text, 'first');
  assert.equal((await call('b')).content[0].text, 'B');
});

test('missing or incorrect metadata and unknown names fail closed without consuming a result', async (t) => {
  const { bridge, call } = await fixture(t);
  bridge.expect([use('a')]);
  bridge.deliver([result('a', 'correct')]);
  for (const meta of [undefined, {}, { toolUseId: 'a' }, { 'claudecode/toolUseId': '' }, { 'claudecode/toolUseId': 5 }]) {
    await assert.rejects(call('a', 'a', 'read', { _meta: meta }), /tool-use ID/);
  }
  await assert.rejects(call('a', 'a', 'read', { name: 'missing' }), /Unknown SDK tool name/);
  await assert.rejects(call('a', 'wrong'), /does not match/);
  assert.equal((await call('a')).content[0].text, 'correct');
});

test('duplicate callbacks never overwrite parked resolvers, including completed calls', async (t) => {
  const { bridge, call } = await fixture(t);
  bridge.expect([use('a')]);
  const first = call('a');
  await setImmediate();
  await assert.rejects(call('a'), /Duplicate SDK tool callback/);
  bridge.deliver([result('a', 'original waiter')]);
  assert.equal((await first).content[0].text, 'original waiter');
  await assert.rejects(call('a'), /Duplicate SDK tool callback/);
});

test('local object checks and JSON argument comparison run even though full validation is host-owned', async (t) => {
  const { bridge, call } = await fixture(t);
  const input = { nested: { b: 2, a: 1 }, outsideSchema: true };
  bridge.expect([{ ...use('a'), input }]);
  input.nested.a = 99;
  await assert.rejects(call('a', 'unused', 'read', { arguments: { nested: { a: 99, b: 2 }, outsideSchema: true } }), /does not match/);
  bridge.deliver([result('a')]);
  const reply = await call('a', 'unused', 'read', { arguments: { outsideSchema: true, nested: { a: 1, b: 2 } } });
  assert.equal(reply.isError, false);
  bridge.expect([use('b')]);
  await assert.rejects(call('b', 'b', 'read', { arguments: undefined }), /JSON object/);
});

test('text, empty results, base64 images, and is_error convert without content loss', async (t) => {
  const { bridge, call } = await fixture(t);
  bridge.expect([use('a'), use('b'), use('c')]);
  const content = [{ type: 'text', text: 'before\n\u0000after' }, { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'AQIDBA==' } }, { type: 'text', text: '' }];
  bridge.deliver([result('a', content, true), { type: 'tool_result', tool_use_id: 'b' }, result('c', '')]);
  content[0].text = 'mutated';
  assert.deepEqual(await call('a'), { content: [{ type: 'text', text: 'before\n\u0000after' }, { type: 'image', mimeType: 'image/png', data: 'AQIDBA==' }, { type: 'text', text: '' }], isError: true });
  assert.deepEqual((await call('b')).content, []);
  assert.deepEqual((await call('c')).content, [{ type: 'text', text: '' }]);
});

test('unsupported content, lossy fields, and malformed images are rejected rather than dropped', async (t) => {
  const { bridge } = await fixture(t);
  bridge.expect([use('a')]);
  for (const content of [
    [{ type: 'audio', data: 'AQID' }],
    [{ type: 'text', text: 'x', citations: [] }],
    [{ type: 'image', source: { type: 'url', url: 'https://example.invalid/image' } }],
    [{ type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'not base64' } }],
    [{ type: 'image', source: { type: 'base64', media_type: 'text/plain', data: 'AQID' } }],
  ]) bad(() => bridge.deliver([result('a', content)]));
  assert.deepEqual(bridge.pendingIds, new Set(['a']));
});

test('close settles registered and unregistered parked callbacks and rejects later mutations', async (t) => {
  const { bridge, call } = await fixture(t);
  bridge.expect([use('registered'), use('queued')]);
  bridge.deliver([result('queued', 'should be discarded')]);
  const a = call('registered');
  const b = call('unregistered');
  await setImmediate();
  bridge.close(new Error('secret credential'));
  bridge.close('idempotent');
  for (const reply of await Promise.all([a, b, call('queued'), call('after-close')])) {
    assert.equal(reply.isError, true);
    assert.doesNotMatch(JSON.stringify(reply), /secret credential|should be discarded/);
  }
  assert.equal(bridge.pendingIds.size, 0);
  bad(() => bridge.expect([use('after-close')]), /closed/);
  bad(() => bridge.deliver([result('registered')]), /closed/);
});

test('deliver-before-close wins for an already parked handler; close-before-deliver wins otherwise', async (t) => {
  const { bridge, call } = await fixture(t);
  bridge.expect([use('a')]);
  const waiting = call('a');
  await setImmediate();
  bridge.deliver([result('a', 'delivered')]);
  bridge.close();
  assert.equal((await waiting).content[0].text, 'delivered');
});

test('query instances do not share IDs, pending results, or closure state', async (t) => {
  const one = await fixture(t);
  const two = await fixture(t);
  one.bridge.expect([use('same')]);
  two.bridge.expect([use('same')]);
  one.bridge.deliver([result('same', 'one')]);
  two.bridge.deliver([result('same', 'two')]);
  assert.equal((await one.call('same')).content[0].text, 'one');
  one.bridge.close();
  assert.equal((await two.call('same')).content[0].text, 'two');
});

test('invalid definitions, cyclic JSON, and oversized arguments fail with safe HTTP errors', () => {
  for (const tools of [null, [{ name: 'builtin', type: 'computer_20250124', input_schema: schema }], [{ name: 'scalar', input_schema: { type: 'string' } }]]) {
    bad(() => new ToolBridge(tools));
  }
  const bridge = new ToolBridge([tool()]);
  const cyclic = {}; cyclic.self = cyclic;
  bad(() => bridge.expect([{ ...use('a'), input: cyclic }]), /cycles/);
  bad(() => bridge.expect([{ ...use('a'), input: { secret: 'x'.repeat(8 * 1024 * 1024) } }]), /size limits/);
  assert.equal(bridge.pendingIds.size, 0);
  bridge.close();
});

test('early duplicate callbacks cannot replace the original waiter', async (t) => {
  const { bridge, call } = await fixture(t);
  const first = call('early');
  await setImmediate();
  await assert.rejects(call('early'), /Duplicate SDK tool callback/);
  bridge.expect([use('early')]);
  bridge.deliver([result('early', 'first waiter survives')]);
  assert.equal((await first).content[0].text, 'first waiter survives');
});

test('registered name mismatches cannot claim queued results', async (t) => {
  const { bridge, call } = await fixture(t, [tool(), tool('write')]);
  bridge.expect([use('a')]);
  bridge.deliver([result('a', 'right tool')]);
  await assert.rejects(call('a', 'a', 'write'), /does not match/);
  assert.equal((await call('a')).content[0].text, 'right tool');
});

test('per-query call ledger is bounded and over-limit batches register nothing', () => {
  const bridge = new ToolBridge([tool()]);
  const calls = Array.from({ length: 4096 }, (_, index) => use(`id-${index}`));
  bad(() => bridge.expect([...calls, use('over-limit')]), /call limits/);
  assert.equal(bridge.pendingIds.size, 0);
  bridge.expect(calls);
  bad(() => bridge.expect([use('over-limit')]), /call limits/);
  assert.equal(bridge.pendingIds.size, 4096);
  bridge.close();
  assert.equal(bridge.pendingIds.size, 0);
});

test('root schema combinators fail explicitly rather than disappearing in Claude Code', () => {
  for (const key of ['anyOf', 'oneOf', 'allOf']) {
    bad(() => new ToolBridge([{ ...tool(), input_schema: { ...schema, [key]: [{ required: ['value'] }] } }]), /Top-level/);
  }
});
