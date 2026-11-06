import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile, access } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { BridgeManager, CONTINUE_AFTER_RESUME_PROMPT } from '../src/engine.mjs';
import { canonicalMessages } from '../src/history.mjs';
import { QueryProcess } from '../src/child.mjs';
import { EventEmitter } from 'node:events';

const tool = { name: 'host_tool', description: 'host executes this', input_schema: { type: 'object', properties: { n: { type: 'integer' } } } };
const body = (messages = [{ role: 'user', content: 'hello' }], extras = {}) => ({ model: 'claude-test', max_tokens: 128, tools: [tool], messages, ...extras });
const use = (id, input = {}) => ({ type: 'tool_use', id, name: 'host_tool', input });
const result = (id, text = id) => ({ type: 'tool_result', tool_use_id: id, content: text });
const next = (request, answer, content) => body([...request.messages, { role: 'assistant', content: answer.content }, { role: 'user', content }]);
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(predicate) {
  for (let n = 0; n < 200; n++) { if (predicate()) return; await delay(2); }
  throw new Error('stub condition did not arrive');
}
function channel() {
  const queue = [];
  let waiting, ended = false;
  return {
    push(value) { if (waiting) { const resolve = waiting; waiting = null; resolve({ value, done: false }); } else queue.push(value); },
    end() { ended = true; waiting?.({ done: true }); waiting = null; },
    [Symbol.asyncIterator]() { return this; },
    next() { if (queue.length) return Promise.resolve({ value: queue.shift(), done: false });
      if (ended) return Promise.resolve({ done: true }); return new Promise(resolve => { waiting = resolve; }); },
    return() { this.end(); return Promise.resolve({ done: true }); },
  };
}
function sdkFactory(onPrompt = () => {}) {
  const queries = [];
  const factory = ({ prompt, options }) => {
    const events = channel();
    const query = { options, prompts: [], closed: 0, clients: [],
      ...events,
      close() { this.closed++; events.end(); },
      emit(event) { events.push({ type: 'stream_event', parent_tool_use_id: null, event }); },
      result() { events.push({ type: 'result', subtype: 'success', is_error: false }); },
      raw(message) { events.push(message); },
      respond(content, { id = randomUUID(), stop = 'end_turn', result: complete = stop !== 'tool_use', echo = true } = {}) {
        const sdkContent = content.map(b => b.type === 'tool_use' ? { ...b, name: options.allowedTools[0] } : b);
        this.emit({ type: 'message_start', message: { id, type: 'message', role: 'assistant', model: options.model,
          content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 12, output_tokens: 0 } } });
        sdkContent.forEach((block, index) => {
          if (block.type === 'tool_use') {
            this.emit({ type: 'content_block_start', index, content_block: { ...block, input: {} } });
            this.emit({ type: 'content_block_delta', index, delta: { type: 'input_json_delta', partial_json: JSON.stringify(block.input) } });
          } else if (block.type === 'text') {
            this.emit({ type: 'content_block_start', index, content_block: { ...block, text: '' } });
            this.emit({ type: 'content_block_delta', index, delta: { type: 'text_delta', text: block.text } });
          } else if (block.type === 'thinking') {
            this.emit({ type: 'content_block_start', index, content_block: { type: 'thinking', thinking: '', signature: '' } });
            this.emit({ type: 'content_block_delta', index, delta: { type: 'thinking_delta', thinking: block.thinking } });
            this.emit({ type: 'content_block_delta', index, delta: { type: 'signature_delta', signature: block.signature } });
          } else this.emit({ type: 'content_block_start', index, content_block: block });
          this.emit({ type: 'content_block_stop', index });
        });
        this.emit({ type: 'message_delta', delta: { stop_reason: stop, stop_sequence: null }, usage: { output_tokens: 9 } });
        this.emit({ type: 'message_stop' });
        if (echo) for (const block of sdkContent) events.push({ type: 'assistant', uuid: randomUUID(), parent_tool_use_id: null,
          message: { id, model: options.model, content: [block], stop_reason: null } });
        if (complete) this.result();
        return id;
      },
      async connect() {
        const client = new Client({ name: 'engine-test', version: '1' });
        const [a, b] = InMemoryTransport.createLinkedPair();
        await options.mcpServers.azo.instance.connect(b);
        await client.connect(a);
        this.clients.push(client);
        this.client = client;
        return client;
      },
      async call(id, input = {}) {
        const client = this.client ?? await this.connect();
        return client.callTool({ name: options.allowedTools[0].replace(/^mcp__azo__/, ''), arguments: input,
          _meta: { 'claudecode/toolUseId': id } });
      },
    };
    queries.push(query);
    query.inputTask = (async () => {
      for await (const message of prompt) {
        query.prompts.push(message);
        await onPrompt(query, message, queries.length);
      }
    })().catch(error => { query.raw({ type: 'result', subtype: 'error_during_execution', errors: [String(error)] }); });
    return query;
  };
  return { factory, queries };
}
async function fixture(t, onPrompt, options = {}) {
  const root = await mkdtemp(join(tmpdir(), 'azo-engine-test-'));
  const stub = sdkFactory(onPrompt);
  const imports = [];
  const manager = new BridgeManager({ queryFactory: stub.factory, stateRoot: root, requestTimeoutMs: 2000,
    env: { HOME: '/stub-home', PATH: process.env.PATH },
    historyImporter: async value => {
      imports.push(structuredClone({ ...value, toolNameToSdk: undefined }));
      const path = join(value.cwd, 'imported.jsonl');
      await writeFile(path, JSON.stringify({ type: 'user', uuid: randomUUID(), message: value.messages.at(-1) }));
      return { path, sessionId: randomUUID() };
    }, ...options });
  t.after(async () => {
    await manager.close();
    await Promise.all(stub.queries.flatMap(q => q.clients).map(client => client.close()));
    await rm(root, { recursive: true, force: true });
  });
  return { manager, imports, root, ...stub };
}

test('locked-down SDK options preserve prompt blocks, thinking, and raw stream content', async t => {
  const responseContent = [{ type: 'thinking', thinking: 'reason', signature: 'signed' },
    { type: 'redacted_thinking', data: 'opaque' }, { type: 'text', text: 'answer' }];
  const f = await fixture(t, q => q.respond(responseContent), { env: {
    ANTHROPIC_API_KEY: 'ambient-key', ANTHROPIC_BASE_URL: 'https://invalid.example',
    AZO_CLAUDE_BRIDGE_KEY: 'bridge-secret', CLAUDE_CODE_ENTRYPOINT: 'ambient-attribution' } });
  const input = body([{ role: 'user', content: [{ type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'eA==' } },
    { type: 'text', text: 'inspect' }] }], { system: [{ type: 'text', text: 'a' }, { type: 'text', text: 'b' }],
    thinking: { type: 'enabled', budget_tokens: 64 }, output_config: { effort: 'high' } });
  const events = [];
  const response = await f.manager.handle(input, { sessionId: 's', onEvent: event => events.push(event) });
  assert.deepEqual(response.content, responseContent);
  assert.equal(response.usage.output_tokens, 9);
  assert.deepEqual(f.queries[0].prompts[0].message.content, input.messages[0].content);
  const o = f.queries[0].options;
  assert.deepEqual(o.tools, []); assert.deepEqual(o.settingSources, []); assert.deepEqual(o.plugins, []);
  assert.deepEqual(o.systemPrompt, { type: 'preset', preset: 'claude_code', append: 'a\n\nb' });
  assert.deepEqual(o.skills, []); assert.equal(o.strictMcpConfig, true);
  assert.equal(o.settings.autoMemoryEnabled, false); assert.equal(o.settings.includeGitInstructions, false);
  assert.equal(o.env.ANTHROPIC_API_KEY, undefined); assert.equal(o.env.ANTHROPIC_BASE_URL, undefined);
  assert.equal(o.env.AZO_CLAUDE_BRIDGE_KEY, undefined); assert.equal(o.env.CLAUDE_CODE_ENTRYPOINT, undefined);
  assert.equal(o.persistSession, false); assert.equal(o.permissionMode, 'default');
  assert.deepEqual(o.thinking, { type: 'enabled', budgetTokens: 64 }); assert.equal(o.effort, 'high');
  assert.equal(o.env.CLAUDE_CODE_MAX_OUTPUT_TOKENS, '128');
  assert.equal((await o.canUseTool('Bash', {})).behavior, 'deny');
  assert.equal((await o.canUseTool(o.allowedTools[0], {})).behavior, 'allow');
  assert.equal(events.filter(e => e.type === 'message_stop').length, 1);
  assert.equal(events.filter(e => e.type === 'content_block_start').length, 3);
});

test('full parallel tool batch is registered once; out-of-order results precede MCP callbacks safely', async t => {
  const f = await fixture(t, q => q.respond([{ type: 'text', text: 'work' }, use('call_a', { n: 1 }), use('call_b', { n: 2 })], { stop: 'tool_use' }));
  const input = body(); const events = [];
  const first = await f.manager.handle(input, { sessionId: 's', onEvent: event => {
    if (event.type === 'message_stop') {
      assert.deepEqual([...f.manager.sessions.get('s').session.tools.pendingIds], ['call_a', 'call_b']);
    }
    if (event.type === 'content_block_start' && event.content_block.type === 'tool_use') {
      assert.equal(event.content_block.name, 'host_tool');
    }
    events.push(event);
  } });
  assert.equal(first.content.filter(b => b.type === 'tool_use').length, 2);
  assert.equal(events.filter(e => e.type === 'message_stop').length, 1);
  const secondInput = next(input, first, [result('call_b', 'B'), result('call_a', 'A')]);
  const secondEvents = [];
  const pending = f.manager.handle(secondInput, { sessionId: 's', onEvent: event => secondEvents.push(event) });
  await until(() => f.manager.sessions.get('s').session.tools.pendingIds.size === 0);
  const q = f.queries[0]; await q.connect();
  const b = await q.call('call_b', { n: 2 }); const a = await q.call('call_a', { n: 1 });
  assert.equal(b.content[0].text, 'B'); assert.equal(a.content[0].text, 'A');
  q.respond([{ type: 'text', text: 'done' }]);
  assert.equal((await pending).content[0].text, 'done');
  assert.equal(secondEvents.filter(e => e.type === 'message_stop').length, 1);
  assert.equal(f.queries.length, 1); assert.equal(q.prompts.length, 1);
});

test('partial, duplicate, or unrelated tool IDs never release a parked batch', async t => {
  const f = await fixture(t, q => q.respond([use('a'), use('b')], { stop: 'tool_use' }));
  const input = body(); const first = await f.manager.handle(input, { sessionId: 's' });
  for (const results of [[result('a')], [result('a'), result('a')], [result('a'), result('wrong')]]) {
    await assert.rejects(f.manager.handle(next(input, first, results), { sessionId: 's' }), { statusCode: 400 });
    assert.deepEqual([...f.manager.sessions.get('s').session.tools.pendingIds], ['a', 'b']);
  }
  assert.equal(f.queries[0].closed, 0);
});

test('normal next user turn reuses persistent prompt stream after result boundary', async t => {
  const f = await fixture(t, q => q.respond([{ type: 'text', text: `reply ${q.prompts.length}` }]));
  const input = body(); const first = await f.manager.handle(input, { sessionId: 's' });
  const second = await f.manager.handle(next(input, first, 'next'), { sessionId: 's' });
  assert.equal(second.content[0].text, 'reply 2');
  assert.equal(f.queries.length, 1); assert.equal(f.queries[0].prompts.length, 2);
});

test('changed canonical history rebuilds even when tool IDs and message counts match', async t => {
  const f = await fixture(t, (q, message, n) => n === 1 ? q.respond([use('a', { n: 1 })], { stop: 'tool_use' })
    : q.respond([{ type: 'text', text: 'rebuilt' }]));
  const input = body(); const first = await f.manager.handle(input, { sessionId: 's' });
  const changed = next(input, first, [result('a')]);
  changed.messages[0] = { role: 'user', content: 'edited' };
  assert.equal((await f.manager.handle(changed, { sessionId: 's' })).content[0].text, 'rebuilt');
  assert.equal(f.queries.length, 2); assert.equal(f.queries[0].closed, 1);
  assert.equal(f.imports[0].messages[0].content[0].text, 'edited');
  assert.deepEqual(f.imports[0].messages, canonicalMessages(changed.messages));
  assert.equal(f.imports[0].model, changed.model);
  assert.deepEqual(f.queries[1].prompts[0].message.content, [{ type: 'text', text: CONTINUE_AFTER_RESUME_PROMPT }]);
  assert.ok(f.queries[1].options.resume);
});

test('same-request steering with tool results rebuilds instead of queueing across the tool boundary', async t => {
  const f = await fixture(t, (q, message, n) => n === 1 ? q.respond([use('a')], { stop: 'tool_use' })
    : q.respond([{ type: 'text', text: 'steered' }]));
  const input = body(); const first = await f.manager.handle(input, { sessionId: 's' });
  const second = next(input, first, [result('a'), { type: 'text', text: 'now change direction' }]);
  await f.manager.handle(second, { sessionId: 's' });
  assert.equal(f.queries.length, 2);
  assert.deepEqual(f.queries[1].prompts[0].message.content, [{ type: 'text', text: 'now change direction' }]);
  assert.deepEqual(f.imports[0].messages, canonicalMessages([...second.messages.slice(0, -1),
    { role: 'user', content: [result('a')] }]));
});

test('only protocol cache hints are ignored; tool input/schema cache_control keys are real content', async t => {
  const f = await fixture(t, q => q.respond([{ type: 'text', text: 'ok' }]));
  const input = body([{ role: 'user', content: [{ type: 'text', text: 'hello', cache_control: { type: 'ephemeral' } }] }]);
  const first = await f.manager.handle(input, { sessionId: 's' });
  const continuation = next(input, first, 'next');
  delete continuation.messages[0].content[0].cache_control;
  const second = await f.manager.handle(continuation, { sessionId: 's' });
  assert.equal(f.queries.length, 1);
  const changed = next(continuation, second, 'third');
  changed.tools = [{ ...tool, input_schema: { type: 'object', properties: { cache_control: { const: 'changed' } } } }];
  await f.manager.handle(changed, { sessionId: 's' });
  assert.equal(f.queries.length, 2);
});

test('SDK exhaustion triggers a clean full-history rebuild on the next turn', async t => {
  const f = await fixture(t, q => { q.respond([{ type: 'text', text: 'done' }]); q.end(); });
  const input = body(); const first = await f.manager.handle(input, { sessionId: 's' });
  await until(() => f.manager.sessions.get('s').session.dead);
  await f.manager.handle(next(input, first, 'another'), { sessionId: 's' });
  assert.equal(f.queries.length, 2); assert.equal(f.imports.length, 1);
});

test('per-session lock, capacity, and idle expiry are enforced', async t => {
  const f = await fixture(t, () => {}, { maxSessions: 1 });
  const pending = f.manager.handle(body(), { sessionId: 'a' });
  await until(() => f.queries.length === 1);
  await assert.rejects(f.manager.handle(body(), { sessionId: 'a' }), { statusCode: 409 });
  await assert.rejects(f.manager.handle(body(), { sessionId: 'b' }), { statusCode: 503 });
  f.queries[0].respond([{ type: 'text', text: 'done' }]); await pending;
  f.manager.sessions.get('a').touched = Date.now() - f.manager.idleMs - 1;
  await f.manager.sweep(); assert.equal(f.manager.sessions.size, 0); assert.equal(f.queries[0].closed, 1);
});

test('anonymous requests have independent ephemeral scopes', async t => {
  const f = await fixture(t, q => q.respond([{ type: 'text', text: 'done' }]));
  await Promise.all([f.manager.handle(body()), f.manager.handle(body())]);
  assert.equal(f.queries.length, 2);
  assert.equal(f.manager.sessions.size, 0);
  assert.notEqual(f.queries[0].options.cwd, f.queries[1].options.cwd);
  assert.equal(f.queries[0].closed, 1); assert.equal(f.queries[1].closed, 1);
});

test('cancel and timeouts sanitize errors, close query, and remove private cwd', async t => {
  const f = await fixture(t, () => {}, { requestTimeoutMs: 100 });
  const first = f.manager.handle(body(), { sessionId: 's' });
  await until(() => f.queries.length === 1);
  const rejected = assert.rejects(first, { statusCode: 499 });
  await f.manager.cancel('s'); await rejected;
  assert.equal(f.queries[0].closed, 1);
  await assert.rejects(access(f.queries[0].options.cwd));
  await assert.rejects(f.manager.handle(body(), { sessionId: 'timeout' }), { statusCode: 504 });
  await until(() => f.queries[1].closed === 1);
});

test('aborted setup never starts a late SDK query and cleans its imported file', async t => {
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  let imported;
  const f = await fixture(t, () => {}, { historyImporter: async ({ cwd }) => {
    await gate; imported = join(cwd, 'late.jsonl'); await writeFile(imported, 'late');
    return { path: imported, sessionId: randomUUID() };
  } });
  const controller = new AbortController();
  const request = f.manager.handle(body([{ role: 'user', content: 'past' }, { role: 'assistant', content: 'answer' },
    { role: 'user', content: 'next' }]), { sessionId: 's', signal: controller.signal });
  await until(() => f.manager.sessions.get('s')?.session?.cwd);
  controller.abort();
  await assert.rejects(request, { statusCode: 499 });
  release();
  await delay(20);
  assert.equal(f.queries.length, 0);
  if (imported) await assert.rejects(access(imported));
});

test('SDK and sink failures never expose raw messages and always close the query', async t => {
  const f = await fixture(t, q => q.raw({ type: 'result', subtype: 'error_during_execution', errors: ['secret token'] }));
  await assert.rejects(f.manager.handle(body(), { sessionId: 's' }), error => {
    assert.equal(error.statusCode, 502); assert.doesNotMatch(error.publicMessage, /secret/); return true;
  });
  assert.equal(f.queries[0].closed, 1);
  const g = await fixture(t, q => q.respond([{ type: 'text', text: 'answer' }]));
  await assert.rejects(g.manager.handle(body(), { sessionId: 's', onEvent() { throw new Error('secret sink error'); } }), { statusCode: 502 });
  assert.equal(g.queries[0].closed, 1);
});

test('unsupported live input is rejected before SDK creation', async t => {
  const f = await fixture(t);
  await assert.rejects(f.manager.handle(body([{ role: 'user', content: [{ type: 'document', source: { type: 'text', data: 'no' } }] }]),
    { sessionId: 's' }), { statusCode: 400 });
  assert.equal(f.queries.length, 0);
});


test('waiting MCP callbacks can immediately emit into the newly attached continuation sink', async t => {
  const f = await fixture(t, q => q.respond([use('a'), use('b')], { stop: 'tool_use' }));
  const input = body(); const first = await f.manager.handle(input, { sessionId: 's' });
  const q = f.queries[0]; await q.connect();
  const callbacks = Promise.all([q.call('a'), q.call('b')]).then(() => {
    q.respond([{ type: 'text', text: 'immediate continuation' }]);
  });
  const continuation = body([...input.messages, { role: 'assistant', content: first.content },
    { role: 'user', content: [result('b')] }, { role: 'user', content: [result('a')] }]);
  const events = [];
  const response = await f.manager.handle(continuation, { sessionId: 's', onEvent: event => events.push(event) });
  await callbacks;
  assert.equal(response.content[0].text, 'immediate continuation');
  assert.equal(events.filter(event => event.type === 'message_start').length, 1);
  assert.equal(events.filter(event => event.type === 'message_stop').length, 1);
  assert.equal(f.queries.length, 1);
});

test('system prompt mode preserves the entire host prompt and validates configuration', async t => {
  assert.throws(() => new BridgeManager({ systemPromptMode: 'invalid' }), /system prompt mode/);
  for (const system of ['Full host system: use host tools.', [{ type: 'text', text: 'first' }, { type: 'text', text: 'second' }]]) {
    const f = await fixture(t, q => q.respond([{ type: 'text', text: 'ok' }]), { systemPromptMode: 'custom' });
    await f.manager.handle(body(undefined, { system }), { sessionId: 's' });
    assert.deepEqual(f.queries[0].options.systemPrompt, typeof system === 'string' ? system : ['first', 'second']);
  }
  const f = await fixture(t, q => q.respond([{ type: 'text', text: 'ok' }]));
  await f.manager.handle(body(undefined, { system: 'Do not filter this full host instruction.' }), { sessionId: 's' });
  assert.deepEqual(f.queries[0].options.systemPrompt, {
    type: 'preset', preset: 'claude_code', append: 'Do not filter this full host instruction.',
  });
});

test('late child close releases an expired slot without poisoning unrelated requests', async t => {
  const children = [];
  const f = await fixture(t, q => {
    q.options.spawnClaudeCodeProcess({ command: 'stub', args: [], env: {} });
    q.respond([{ type: 'text', text: 'ok' }]);
  }, { maxSessions: 2, processFactory: () => new QueryProcess({ graceMs: 2, killMs: 2,
    spawnFactory: () => {
      const child = new EventEmitter(); child.stderr = new EventEmitter(); child.signals = [];
      child.kill = signal => { child.signals.push(signal); return true; };
      children.push(child); return child;
    } }) });
  await f.manager.handle(body(), { sessionId: 'slow' });
  const slow = f.manager.sessions.get('slow');
  slow.touched = Date.now() - f.manager.idleMs - 1;
  await f.manager.sweep();
  assert.deepEqual(children[0].signals, ['SIGTERM', 'SIGKILL']);
  assert.equal(f.manager.sessions.get('slow'), slow);
  assert.equal(slow.closing, true); assert.equal(slow.session.process.didExit, false);
  await access(slow.session.cwd);
  await f.manager.sweep(); // cached bounded removal is not a cached rejection
  await f.manager.handle(body(), { sessionId: 'other' });
  assert.equal(f.manager.sessions.size, 2);
  children[0].emit('close', 0, 'SIGKILL');
  await until(() => !f.manager.sessions.has('slow'));
  await until(() => slow.session.process.didExit);
  await f.manager.handle(body(), { sessionId: 'after-close' });
  assert.equal(f.queries.length, 3);
  for (const child of children) child.emit('close', 0, null);
});

test('restart and changed-settings rebuilds reject incomplete final batches before imports or SDK work', async t => {
  const f = await fixture(t, q => q.respond([use('a'), use('b')], { stop: 'tool_use' }));
  const input = body(); const answer = await f.manager.handle(input, { sessionId: 'parked' });
  for (const sessionId of ['restart', 'parked']) {
    for (const results of [[result('a')], [result('a'), result('a')], [result('a'), result('orphan')]]) {
      const invalid = next(input, answer, results);
      invalid.model = 'changed-settings';
      invalid.messages[0] = { role: 'user', content: 'Compacted authoritative prefix' };
      await assert.rejects(f.manager.handle(invalid, { sessionId }), { statusCode: 400 });
    }
  }
  await assert.rejects(f.manager.handle(body([{ role: 'user', content: [result('orphan')] }]), { sessionId: 'orphan' }), { statusCode: 400 });
  assert.equal(f.queries.length, 1); assert.equal(f.imports.length, 0); assert.equal(f.queries[0].closed, 0);
  assert.deepEqual([...f.manager.sessions.get('parked').session.tools.pendingIds], ['a', 'b']);
});

test('removed historical tools map deterministically but are never advertised or permitted', async t => {
  let historicalName;
  const f = await fixture(t, q => q.respond([{ type: 'text', text: 'resumed' }]), {
    historyImporter: async ({ messages, toolNameToSdk }) => {
      assert.deepEqual(messages.at(-1).content, [{ ...result('old'), content: [{ type: 'text', text: 'actual host result' }] }]);
      historicalName = toolNameToSdk(messages[1].content[0].name);
      assert.equal(toolNameToSdk('host_tool'), 'mcp__azo__azo_host_tool');
      return { sessionId: randomUUID() };
    },
  });
  const old = { ...use('old'), name: 'removed/tool' };
  await f.manager.handle(body([{ role: 'user', content: 'earlier' }, { role: 'assistant', content: [old] },
    { role: 'user', content: [result('old', 'actual host result')] }]), { sessionId: 's' });
  assert.match(historicalName, /^mcp__azo__azo_h_/);
  const q = f.queries[0]; const client = await q.connect();
  const listed = await client.listTools();
  assert.deepEqual(listed.tools.map(tool => tool.name), ['azo_host_tool']);
  assert.deepEqual(q.options.allowedTools, ['mcp__azo__azo_host_tool']);
  assert.equal((await q.options.canUseTool(historicalName, {})).behavior, 'deny');
  assert.deepEqual(q.prompts[0].message.content, [{ type: 'text', text: CONTINUE_AFTER_RESUME_PROMPT }]);
});

test('historical and live alias collisions reject before query startup', async t => {
  const f = await fixture(t);
  const name = 'removed/tool';
  const colliding = `h_${createHash('sha256').update(name).digest('base64url')}`;
  const input = body([{ role: 'user', content: 'earlier' }, { role: 'assistant', content: [{ ...use('old'), name }] },
    { role: 'user', content: [result('old')] }], { tools: [{ ...tool, name: colliding }] });
  await assert.rejects(f.manager.handle(input, { sessionId: 's' }), { statusCode: 400 });
  assert.equal(f.queries.length, 0); assert.equal(f.imports.length, 0);
});

function startMessage(q, id) {
  q.emit({ type: 'message_start', message: { id, type: 'message', role: 'assistant', model: 'claude-test',
    content: [], stop_reason: null, usage: { input_tokens: 1, output_tokens: 0 } } });
}
function fallback(q, id, content, stop_reason = null) {
  q.raw({ type: 'assistant', uuid: randomUUID(), parent_tool_use_id: null,
    message: { id, role: 'assistant', model: 'claude-test', content, stop_reason } });
}

test('live deltas keep a host progress watchdog alive beyond its deadline', async t => {
  const progressMs = 100;
  let timer, deltas = 0;
  const controller = new AbortController();
  t.after(() => clearTimeout(timer));
  const f = await fixture(t, async q => {
    startMessage(q, 'live');
    q.emit({ type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } });
    for (let i = 0; i < 8; i++) {
      q.emit({ type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: '.' } });
      await delay(20);
    }
    q.emit({ type: 'content_block_stop', index: 0 });
    q.emit({ type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: 8 } });
    q.emit({ type: 'message_stop' }); q.result();
  });
  const started = Date.now();
  timer = setTimeout(() => controller.abort(), progressMs);
  const response = await f.manager.handle(body(), { sessionId: 's', signal: controller.signal, onEvent(event) {
    if (event.type === 'content_block_delta') {
      deltas++; clearTimeout(timer); timer = setTimeout(() => controller.abort(), progressMs);
    }
  } });
  clearTimeout(timer);
  assert.ok(Date.now() - started > progressMs);
  assert.equal(deltas, 8); assert.equal(controller.signal.aborted, false);
  assert.equal(response.content[0].text, '........');
});

test('unpublished abandoned stream can become a complete fallback parallel batch', async t => {
  const f = await fixture(t, q => {
    startMessage(q, 'abandoned');
    fallback(q, 'replacement', [use('a'), use('b')].map(block => ({ ...block, name: q.options.allowedTools[0] })), 'tool_use');
  });
  const events = [];
  const answer = await f.manager.handle(body(), { sessionId: 's', onEvent(event) {
    if (event.type === 'message_stop') assert.deepEqual([...f.manager.sessions.get('s').session.tools.pendingIds], ['a', 'b']);
    events.push(event);
  } });
  assert.equal(answer.id, 'replacement'); assert.equal(answer.content.length, 2);
  assert.deepEqual(events.filter(event => event.type === 'message_start').map(event => event.message.id), ['replacement']);
  assert.equal(events.filter(event => event.type === 'message_stop').length, 1);
});

test('per-block fallback tools without a terminal boundary fail immediately, not as a partial batch or timeout', async t => {
  const f = await fixture(t, q => {
    startMessage(q, 'abandoned');
    for (const id of ['a', 'b']) fallback(q, 'replacement', [{ ...use(id), name: q.options.allowedTools[0] }]);
  });
  const events = [];
  await assert.rejects(f.manager.handle(body(), { sessionId: 's', onEvent(event) { events.push(event); } }), error => {
    assert.equal(error.statusCode, 502); assert.match(error.publicMessage, /completion boundary/); return true;
  });
  assert.deepEqual(events, []); assert.equal(f.queries[0].closed, 1);
  assert.equal(f.manager.sessions.get('s').session.tools.pendingIds.size, 0);
});

test('replacement streams discard unpublished starts, but never mix already-published IDs', async t => {
  const f = await fixture(t, q => { startMessage(q, 'abandoned'); q.respond([{ type: 'text', text: 'replacement' }], { id: 'new' }); });
  const starts = [];
  assert.equal((await f.manager.handle(body(), { sessionId: 's', onEvent(event) {
    if (event.type === 'message_start') starts.push(event.message.id);
  } })).id, 'new');
  assert.deepEqual(starts, ['new']);
  for (const retry of ['stream', 'fallback']) {
    const g = await fixture(t, q => {
      startMessage(q, 'visible');
      q.emit({ type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } });
      q.emit({ type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'visible partial' } });
      if (retry === 'stream') q.respond([{ type: 'text', text: 'replacement' }], { id: 'new' });
      else fallback(q, 'new', [{ type: 'text', text: 'replacement' }], 'end_turn');
    });
    const events = [];
    await assert.rejects(g.manager.handle(body(), { sessionId: 's', onEvent(event) { events.push(event); } }), { statusCode: 502 });
    assert.deepEqual(events.filter(event => event.type === 'message_start').map(event => event.message.id), ['visible']);
    assert.equal(events.some(event => event.type === 'message_stop'), false);
    assert.equal(g.queries[0].closed, 1);
  }
});

test('nonstream clients may replace unpublished partial content without leaking abandoned blocks', async t => {
  const f = await fixture(t, q => {
    startMessage(q, 'abandoned');
    q.emit({ type: 'content_block_start', index: 0, content_block: { type: 'text', text: 'discard me' } });
    fallback(q, 'new', [{ type: 'text', text: 'fresh' }], 'end_turn'); q.result();
  });
  const answer = await f.manager.handle(body(), { sessionId: 's' });
  assert.equal(answer.id, 'new'); assert.deepEqual(answer.content, [{ type: 'text', text: 'fresh' }]);
});

test('results-only rebuild opener is the exact reference continuation', () => {
  assert.equal(CONTINUE_AFTER_RESUME_PROMPT,
    '[Your session was interrupted and resumed. What precedes this ends with the tool result '
    + 'you were waiting for. Continue the task from there.]');
});

for (const steering of ['none', 'same-record', 'separate-records']) {
  test(`rebuild imports adjacent result records before startup: ${steering}`, async t => {
    const image = { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'aGVsbG8=' } };
    const realPrompt = [{ type: 'text', text: 'use these real results' }, image, { type: 'text', text: 'then stop' }];
    let f;
    f = await fixture(t, q => {
      assert.equal(f.imports.length, 1, 'All results must be imported before SDK startup');
      q.respond([{ type: 'text', text: 'done' }]);
    });
    const history = [{ role: 'user', content: 'earlier' }, { role: 'assistant', content: [use('a'), use('b')] },
      { role: 'user', content: [{ ...result('b'), is_error: true, content: [{ type: 'text', text: 'real error' }, image] }] },
      { role: 'user', content: [result('a', 'actual result')] }];
    const imported = canonicalMessages(history);
    if (steering === 'same-record') history.at(-1).content.push(...realPrompt);
    if (steering === 'separate-records') history.push(
      { role: 'user', content: realPrompt.slice(0, 2) }, { role: 'user', content: realPrompt.slice(2) });
    const original = structuredClone(history);
    const input = body(history);
    const answer = await f.manager.handle(input, { sessionId: 's' });
    assert.deepEqual(history, original, 'No input history mutation');
    assert.deepEqual(f.imports[0].messages, imported);
    assert.equal(f.imports[0].model, input.model);
    assert.match(f.queries[0].options.resumeSessionAt, /^[0-9a-f-]{36}$/);
    assert.equal(f.queries[0].prompts[0].client_composed, true);
    assert.deepEqual(f.queries[0].prompts[0].message.content, steering === 'none'
      ? [{ type: 'text', text: CONTINUE_AFTER_RESUME_PROMPT }] : realPrompt);
    const session = f.manager.sessions.get('s').session;
    assert.deepEqual(session.expected, canonicalMessages([...history, { role: 'assistant', content: answer.content }]));
    await until(() => session.ready);
    await f.manager.handle(next(input, answer, 'next real user'), { sessionId: 's' });
    assert.equal(f.queries.length, 1, 'SDK-only continuation must not break canonical reuse');
  });
}

test('parked disposal kills before SDK close and settles callbacks only after actual child exit', async t => {
  const child = new EventEmitter(); child.stderr = new EventEmitter();
  const order = []; child.kill = signal => { order.push(signal); return true; };
  const f = await fixture(t, q => {
    q.options.spawnClaudeCodeProcess({ command: 'stub', args: [], env: {} });
    const close = q.close.bind(q);
    q.close = () => { order.push('sdk-close'); close(); };
    q.respond([use('a')], { stop: 'tool_use' });
  }, { processFactory: () => new QueryProcess({ graceMs: 2, killMs: 2, spawnFactory: () => child }) });
  await f.manager.handle(body(), { sessionId: 's' });
  const session = f.manager.sessions.get('s').session;
  // Invoke the real in-memory MCP handler and observe resolution independently.
  let settled = false;
  const callback = f.queries[0].call('a').then(value => { settled = true; return value; });
  await delay(10);
  assert.equal(settled, false);
  await f.manager.dispose(session);
  assert.equal(order[0], 'SIGKILL');
  assert.ok(order.indexOf('sdk-close') > 0);
  assert.equal(session.controller.signal.aborted, true);
  assert.equal(session.process.didExit, false);
  assert.equal(settled, false, 'A shutdown timeout must not release live-child callbacks');
  await access(session.cwd);
  child.emit('close', 0, 'SIGKILL');
  await callback;
  assert.equal(settled, true);
  await until(() => session.process.didExit);
});

function pendingProgress(q, type = 'text') {
  q.emit({ type: 'message_start', message: { id: randomUUID(), type: 'message', role: 'assistant',
    model: 'claude-test', content: [], usage: { input_tokens: 1, output_tokens: 0 } } });
  q.emit({ type: 'content_block_start', index: 0, content_block: type === 'thinking'
    ? { type, thinking: '', signature: '' } : { type, text: '' } });
  q.emit({ type: 'content_block_delta', index: 0, delta: type === 'thinking'
    ? { type: 'thinking_delta', thinking: 'unfinished thought' } : { type: 'text_delta', text: 'unfinished' } });
}

test('thinking interruption coalesces superseded user feedback without duplicate SDK prompts', async t => {
  let receipt, interrupts = 0;
  const f = await fixture(t, q => {
    if (q.prompts.length > 1) { q.respond([{ type: 'text', text: 'steered' }]); return; }
    q.interrupt = () => { interrupts++; return new Promise(resolve => { receipt = resolve; }); };
    pendingProgress(q, 'thinking');
  });
  const first = body(), stop = new AbortController();
  const old = f.manager.handle(first, { sessionId: 's', signal: stop.signal });
  await until(() => f.manager.sessions.get('s')?.current?.stream?.blocks.size);
  stop.abort(); await assert.rejects(old, e => e.statusCode === 499);
  const feedback = body([{ role: 'user', content: [{ type: 'text', text: 'hello' }, { type: 'text', text: 'feedback one' }] }]);
  const secondStop = new AbortController();
  const second = f.manager.handle(feedback, { sessionId: 's', signal: secondStop.signal });
  await until(() => f.manager.sessions.get('s')?.current?.reusing);
  secondStop.abort(); await assert.rejects(second, e => e.statusCode === 499);
  const latest = body([{ role: 'user', content: [...feedback.messages[0].content, { type: 'text', text: 'feedback two' }] }]);
  const third = f.manager.handle(latest, { sessionId: 's' });
  f.queries[0].raw({ type: 'result', subtype: 'error_during_execution', is_error: true });
  receipt({ still_queued: [] });
  assert.equal((await third).content[0].text, 'steered');
  assert.equal(interrupts, 1); assert.equal(f.queries.length, 1);
  assert.deepEqual(f.queries[0].prompts[1].message.content, feedback.messages[0].content.slice(1)
    .concat([{ type: 'text', text: 'feedback two' }]));
});

test('a nonempty interrupt queue forces reconstruction instead of running stale SDK work', async t => {
  const f = await fixture(t, (q, _message, n) => {
    if (n > 1) { q.respond([{ type: 'text', text: 'fresh' }]); return; }
    q.interrupt = async () => {
      q.raw({ type: 'result', subtype: 'error_during_execution', is_error: true });
      return { still_queued: ['stale-message'] };
    };
    pendingProgress(q);
  });
  const first = body(), stop = new AbortController();
  const old = f.manager.handle(first, { sessionId: 's', signal: stop.signal });
  await until(() => f.manager.sessions.get('s')?.current?.stream?.blocks.size);
  stop.abort(); await assert.rejects(old, e => e.statusCode === 499);
  const answer = await f.manager.handle(body([...first.messages, { role: 'user', content: 'new feedback' }]), { sessionId: 's' });
  assert.equal(answer.content[0].text, 'fresh'); assert.equal(f.queries.length, 2);
  assert.ok(f.queries[0].closed);
});

test('an early waiting MCP callback prevents same-process interruption before its stream block arrives', async t => {
  let interrupts = 0;
  const f = await fixture(t, (q, _message, n) => {
    if (n > 1) { q.respond([{ type: 'text', text: 'fresh' }]); return; }
    q.interrupt = async () => { interrupts++; return { still_queued: [] }; };
    q.earlyCall = q.call('early_call');
    q.earlyCall.catch(() => {});
  });
  const first = body(), stop = new AbortController();
  const old = f.manager.handle(first, { sessionId: 's', signal: stop.signal });
  await until(() => f.manager.sessions.get('s')?.session?.tools.hasWaitingCalls);
  stop.abort(); await assert.rejects(old, e => e.statusCode === 499);
  const answer = await f.manager.handle(body([...first.messages, { role: 'user', content: 'new feedback' }]), { sessionId: 's' });
  assert.equal(answer.content[0].text, 'fresh'); assert.equal(f.queries.length, 2);
  assert.equal(interrupts, 0); assert.equal((await f.queries[0].earlyCall).isError, true);
});

for (const late of ['block', 'callback']) {
  test(`late ${late} after completed interrupt forces rebuild before feedback`, async t => {
    const f = await fixture(t, (q, _message, n) => {
      if (n > 1) { q.respond([{ type: 'text', text: 'fresh' }]); return; }
      q.interrupt = async () => {
        q.raw({ type: 'result', subtype: 'error_during_execution', is_error: true });
        return { still_queued: [] };
      };
      pendingProgress(q);
    });
    const first = body(), stop = new AbortController();
    const old = f.manager.handle(first, { sessionId: 's', signal: stop.signal });
    await until(() => f.manager.sessions.get('s')?.current?.stream?.blocks.size);
    stop.abort(); await assert.rejects(old, e => e.statusCode === 499);
    const session = f.manager.sessions.get('s').session;
    assert.equal(await session.interruption.promise, true);
    let callback;
    if (late === 'callback') {
      callback = f.queries[0].call('late_call');
      await until(() => session.tools.hasWaitingCalls);
    } else {
      f.queries[0].emit({ type: 'content_block_start', index: 1,
        content_block: { type: 'tool_use', id: 'late_call', name: 'mcp__azo__azo_probe', input: {} } });
      await until(() => session.unsettledTool);
    }
    const answer = await f.manager.handle(body([...first.messages, { role: 'user', content: 'new feedback' }]), { sessionId: 's' });
    assert.equal(answer.content[0].text, 'fresh'); assert.equal(f.queries.length, 2);
    assert.equal(f.queries[0].prompts.length, 1); assert.ok(f.queries[0].closed);
    if (callback) assert.equal((await callback).isError, true);
  });
}
