import assert from 'node:assert/strict';
import { createBridgeServer } from '../../src/server.mjs';
import { canonicalMessages } from '../../src/history.mjs';
import { offlineSdk } from './local-api.mjs';

// Optional endpoint for test_agent_zoo_provider.py. Every response traverses the
// real BridgeManager, SDK CLI, MCP transport, and synthetic loopback Messages API.
// This fixture never computes a tool result: only the Python host computes 2 + 3.
const key = process.env.AZO_CLAUDE_BRIDGE_KEY;
assert.ok(typeof key === 'string' && key.length >= 32, 'Python must supply a synthetic bridge key');
const continuation = process.env.AZO_SDK_FIXTURE_CONTINUATION === '1';
const PREFIX = 'OFFLINE_CONTINUATION_PREFIX: unfinished response; ';
const USAGE = { input_tokens: 7, output_tokens: 3, cache_read_input_tokens: 11, cache_creation_input_tokens: 0 };
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
  // The example config enables server web search: only built-in WebSearch joins the host tool.
  assert.deepEqual(body.tools.map(tool => tool.name).sort(), [NAME, 'WebSearch'].sort(),
    'No other built-in, removed, or external MCP tool is allowed');
  assert.deepEqual(body.tools.find(tool => tool.name === NAME).input_schema, SCHEMA);
  const blocks = body.messages.flatMap(message => Array.isArray(message.content) ? message.content : []);
  const uses = blocks.filter(block => block.type === 'tool_use');
  const results = blocks.filter(block => block.type === 'tool_result');
  if (index <= Number(continuation)) {
    assert.equal(hostCalls, 1);
    assert.deepEqual(uses, []);
    assert.deepEqual(results, []);
    assert.ok(JSON.stringify(body.messages).includes('Use probe_add for 2 + 3.'));
    if (continuation && index === 0) {
      return { content: [{ type: 'text', text: PREFIX }], stop_reason: 'max_tokens', usage: USAGE };
    }
    if (continuation) {
      const previous = body.messages.findIndex(message => message.role === 'assistant' && textOf(message.content) === PREFIX);
      assert.ok(previous >= 0, 'SDK continuation must retain the truncated assistant response');
      assert.ok(body.messages.slice(previous + 1).some(message => message.role === 'user' && textOf(message.content).trim()),
        'SDK must append a nonempty continuation prompt');
    }
    return [
      { type: 'text', text: 'Before the tool.' },
      { type: 'tool_use', id: 'tool_fixture', name: NAME, input: { a: 2, b: 3 } },
      { type: 'text', text: 'After the tool.' },
    ];
  }
  assert.equal(index, 1 + Number(continuation), 'Only the initial segment(s) and tool-result model call are expected');
  assert.equal(hostCalls, 2);
  // The pinned SDK groups text before tool calls in its own provider replay.
  // Exact host ordering is checked separately before realManager.handle below.
  const toolAssistant = body.messages.find(message => message.role === 'assistant'
    && Array.isArray(message.content) && message.content.some(block => block.type === 'tool_use'));
  assert.ok(toolAssistant, 'SDK replay must contain the assistant tool batch');
  assert.deepEqual(toolAssistant.content.map(({ cache_control, ...block }) => block), [
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
}, { usage: USAGE });
const realManager = runtime.makeManager();
const manager = {
  async handle(body, context) {
    assert.equal(context.sessionId, 'native-azo-fixture');
    assert.equal(body.model, 'sonnet');
    assert.ok(textOf(body.system).includes('Host-owned instructions'));
    assert.deepEqual(body.tools.map(tool => tool.type ?? tool.name), ['web_search_20250305', 'probe_add']);
    assert.deepEqual(body.tools[1].input_schema, SCHEMA);
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
        assert.equal(message.usage.output_tokens, continuation ? 6 : 3);
        assert.equal(message.usage.input_tokens, 7);
        assert.equal(message.usage.cache_read_input_tokens, 11);
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
