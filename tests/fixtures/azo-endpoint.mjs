import assert from 'node:assert/strict';
import { createBridgeServer } from '../../src/server.mjs';

// Deterministic endpoint for the real Python make_llm_fn compatibility test.
// No SDK, login, remote network, or tool execution is involved in this fixture.
let calls = 0;
const manager = {
  async handle(body, { sessionId, onEvent }) {
    assert.equal(sessionId, 'native-azo-fixture');
    assert.equal(body.model, 'sonnet');
    assert.match(JSON.stringify(body.system), /Host-owned instructions/);
    assert.equal(body.tools[0].name, 'probe_add');
    const turn = ++calls;
    if (turn === 2) {
      const history = body.messages.flatMap(m => Array.isArray(m.content) ? m.content : []);
      assert.ok(history.some(b => b.type === 'tool_use' && b.id === 'tool_fixture' && b.name === 'probe_add'));
      assert.ok(history.some(b => b.type === 'tool_result' && b.tool_use_id === 'tool_fixture' && JSON.stringify(b.content).includes('5')));
    }
    const content = turn === 1 ? [{ type: 'tool_use', id: 'tool_fixture', name: 'probe_add', input: { a: 2, b: 3 } }]
      : [{ type: 'text', text: 'HOST_RESULT_5' }];
    const message = { type: 'message', id: `msg_fixture_${turn}`, role: 'assistant', model: body.model,
      content, stop_reason: turn === 1 ? 'tool_use' : 'end_turn', stop_sequence: null,
      usage: { input_tokens: 7, output_tokens: 3, cache_read_input_tokens: 11, cache_creation_input_tokens: 0 } };
    if (onEvent) {
      onEvent({ type: 'message_start', message: { ...message, content: [], stop_reason: null } });
      content.forEach((content_block, index) => {
        onEvent({ type: 'content_block_start', index, content_block });
        onEvent({ type: 'content_block_stop', index });
      });
      onEvent({ type: 'message_delta', delta: { stop_reason: message.stop_reason, stop_sequence: null }, usage: { output_tokens: 3 } });
      onEvent({ type: 'message_stop' });
    }
    return message;
  },
  async cancel() {}, async close() {},
};
const endpoint = createBridgeServer({ manager, key: process.env.AZO_CLAUDE_BRIDGE_KEY });
const address = await endpoint.listen(0);
console.log(JSON.stringify({ port: address.port }));
process.once('SIGTERM', () => endpoint.close());
process.once('SIGINT', () => endpoint.close());
