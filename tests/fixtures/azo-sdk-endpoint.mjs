import assert from 'node:assert/strict';
import { createBridgeServer } from '../../src/server.mjs';
import { canonicalMessages } from '../../src/history.mjs';
import { offlineSdk } from './local-api.mjs';

// Optional endpoint for test_agent_zoo_provider.py. Every response traverses the
// real BridgeManager, SDK CLI, MCP transport, and synthetic loopback Messages API.
// This fixture never computes a tool result: only the Python host computes 2 + 3.
const key = process.env.AZO_CLAUDE_BRIDGE_KEY;
assert.ok(typeof key === 'string' && key.length >= 32, 'Python must supply a synthetic bridge key');
const cleanup = [];
const SCHEMA = {
  type: 'object', properties: { a: { type: 'integer' }, b: { type: 'integer' } }, required: ['a', 'b'],
};
const NAME = 'mcp__azo__azo_probe_add';
const textOf = content => typeof content === 'string' ? content
  : (content ?? []).filter(block => block.type === 'text').map(block => block.text).join('\n');
let hostCalls = 0;
const runtime = await offlineSdk({ after(fn) { cleanup.push(fn); } }, (body, index) => {
  assert.ok(textOf(body.system).includes('Host-owned instructions'));
  assert.equal(body.tools.length, 1, 'No built-in, removed, or external MCP tool is allowed');
  assert.equal(body.tools[0].name, NAME);
  assert.deepEqual(body.tools[0].input_schema, SCHEMA);
  const blocks = body.messages.flatMap(message => Array.isArray(message.content) ? message.content : []);
  const uses = blocks.filter(block => block.type === 'tool_use');
  const results = blocks.filter(block => block.type === 'tool_result');
  if (index === 0) {
    assert.equal(hostCalls, 1);
    assert.deepEqual(uses, []);
    assert.deepEqual(results, []);
    assert.ok(JSON.stringify(body.messages).includes('Use probe_add for 2 + 3.'));
    return [
      { type: 'text', text: 'Before the tool.' },
      { type: 'tool_use', id: 'tool_fixture', name: NAME, input: { a: 2, b: 3 } },
      { type: 'text', text: 'After the tool.' },
    ];
  }
  assert.equal(index, 1, 'Only two synthetic model calls are expected');
  assert.equal(hostCalls, 2);
  // The pinned SDK groups text before tool calls in its own provider replay.
  // Exact host ordering is checked separately before realManager.handle below.
  assert.deepEqual(body.messages.find(message => message.role === 'assistant').content.map(({ cache_control, ...block }) => block), [
    { type: 'text', text: 'Before the tool.' },
    { type: 'text', text: 'After the tool.' },
    { type: 'tool_use', id: 'tool_fixture', name: NAME, input: { a: 2, b: 3 } },
  ], 'SDK normalization must preserve text boundaries and the complete tool call');
  assert.deepEqual(uses.map(({ id, name, input }) => ({ id, name, input })), [
    { id: 'tool_fixture', name: NAME, input: { a: 2, b: 3 } },
  ]);
  assert.equal(results.length, 1, 'No fabricated interruption or duplicate result is allowed');
  assert.equal(results[0].tool_use_id, 'tool_fixture');
  assert.equal(textOf(results[0].content), '5', 'Only the actual Python host result may satisfy MCP');
  assert.equal(results[0].is_error ?? false, false);
  return [{ type: 'text', text: 'HOST_RESULT_5' }];
}, { usage: { input_tokens: 7, output_tokens: 3, cache_read_input_tokens: 11, cache_creation_input_tokens: 0 } });
const realManager = runtime.makeManager();
const manager = {
  async handle(body, context) {
    assert.equal(context.sessionId, 'native-azo-fixture');
    assert.equal(body.model, 'sonnet');
    assert.ok(textOf(body.system).includes('Host-owned instructions'));
    assert.equal(body.tools.length, 1);
    assert.equal(body.tools[0].name, 'probe_add');
    assert.deepEqual(body.tools[0].input_schema, SCHEMA);
    const turn = ++hostCalls;
    assert.ok(turn <= 2);
    try {
      if (turn === 2) {
        assert.deepEqual(canonicalMessages(body.messages.slice(0, -1)),
          realManager.sessions.get(context.sessionId)?.session.expected,
          'Python replay must exactly match the parked SDK history');
      }
      const message = await realManager.handle(body, context);
      if (turn === 1) {
        assert.deepEqual(runtime.settled, []);
      } else {
        assert.equal(runtime.queries.length, 1, 'Exact replay must reuse the existing SDK query');
        assert.equal(runtime.callbacks.length, 1, 'Historical tools must not execute again');
        assert.equal(runtime.callbacks[0]._meta?.['claudecode/toolUseId'], 'tool_fixture');
        assert.equal(runtime.callbacks[0].name, 'azo_probe_add');
        assert.deepEqual(runtime.callbacks[0].arguments, { a: 2, b: 3 });
        assert.equal(textOf(message.content), 'HOST_RESULT_5');
      }
      runtime.api.assertHealthy();
      return message;
    } catch (error) {
      // Synthetic-only fixture: surface the upstream assertion, not its SDK wrapper.
      const failure = runtime.api.failures[0] ?? error;
      process.stderr.write(`${failure.stack}\n`);
      throw failure;
    }
  },
  cancel: sessionId => realManager.cancel(sessionId),
  close: () => realManager.close(),
};
const endpoint = createBridgeServer({ manager, key, requestTimeoutMs: 25_000 });
let closing;
function stop() {
  return closing ??= (async () => {
    // Parent allows five seconds. Keep child termination ahead of config removal.
    const deadline = setTimeout(() => process.exit(2), 4_500);
    deadline.unref();
    try {
      try { await endpoint.close(); }
      finally { for (const close of cleanup) await close(); }
    } catch {
      // Never print SDK messages, raw streams, environment, or credentials.
      process.stderr.write('Offline SDK endpoint cleanup or validation failed.\n');
      process.exitCode = 1;
    } finally {
      if (runtime.processes.every(child => child.didExit)) clearTimeout(deadline);
    }
  })();
}
process.once('SIGTERM', stop);
process.once('SIGINT', stop);
try {
  const address = await endpoint.listen(0);
  // This is the sole stdout record consumed by the Python fixture launcher.
  process.stdout.write(`${JSON.stringify({ port: address.port })}\n`);
} catch {
  await stop();
  process.exitCode = 1;
}
