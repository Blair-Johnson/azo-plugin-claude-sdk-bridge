import test from 'node:test';
import assert from 'node:assert/strict';
import { offlineSdk, until } from './fixtures/local-api.mjs';

const FIRST = 'OFFLINE_CONTINUATION_USER: complete the synthetic response.';
const PART_ONE = 'OFFLINE_SEGMENT_ONE: unfinished response; ';
const PART_TWO = 'OFFLINE_SEGMENT_TWO: continued response.';
const FOLLOWUP = 'OFFLINE_CONTINUATION_FOLLOWUP: acknowledge the completed response.';
const IDS = ['toolu_continuation_first', 'toolu_continuation_second'];
const INPUT = { label: 'identical-offline-probe' };
const TOOLS = [{
  name: 'offline_probe', description: 'OFFLINE_CONTINUATION_PROBE',
  input_schema: { type: 'object', properties: { label: { type: 'string' } }, required: ['label'], additionalProperties: false },
}];
const BASE = {
  model: 'claude-sonnet-4-6', system: 'OFFLINE_ONLY: use only synthetic fixture responses.',
  tools: TOOLS, max_tokens: 512, stream: true,
};
const asMessage = ({ role, content }) => ({ role, content });
const textOf = content => typeof content === 'string' ? content
  : (content ?? []).filter(block => block.type === 'text').map(block => block.text).join('');
const blocks = body => body.messages.flatMap(message => Array.isArray(message.content) ? message.content : []);
const truncated = () => ({
  content: [{ type: 'text', text: PART_ONE }], stop_reason: 'max_tokens',
  usage: { input_tokens: 32, output_tokens: 9 },
});
const continued = content => ({ content, usage: { input_tokens: 47, output_tokens: 12 } });

function assertContinuationRequest(body) {
  assert.ok(body.messages.some(message => message.role === 'user' && textOf(message.content).includes(FIRST)));
  const previous = body.messages.findIndex(message => message.role === 'assistant' && textOf(message.content).includes(PART_ONE));
  assert.ok(previous >= 0, 'Actual second SDK request must retain the truncated assistant response');
  const prompts = body.messages.slice(previous + 1).filter(message => message.role === 'user');
  assert.ok(prompts.some(message => textOf(message.content).trim().length > 0),
    'SDK must append a nonempty continuation prompt after the truncated response');
  assert.ok(prompts.every(message => !textOf(message.content).includes(FIRST)),
    'Continuation must not resubmit the original host user turn');
  assert.equal(blocks(body).filter(block => block.type === 'tool_result').length, 0);
}

function assertJoinedResponse(message, events, content, stopReason) {
  assert.equal(message.stop_reason, stopReason);
  assert.deepEqual(message.content, content, 'Both segments must survive in order, with no SDK echo duplication');
  assert.equal(message.usage.output_tokens, 21, 'Output usage must sum both API segments (9 + 12)');
  assert.equal(message.usage.input_tokens, 47, 'Input usage must describe the latest segment, not sum repeated context');
  assert.equal(events.filter(event => event.type === 'message_start').length, 1);
  assert.equal(events.filter(event => event.type === 'message_stop').length, 1);
  assert.equal(events[0].type, 'message_start');
  assert.equal(events.at(-1).type, 'message_stop');
  const indexes = content.map((_, index) => index);
  assert.deepEqual(events.filter(event => event.type === 'content_block_start').map(event => event.index), indexes);
  assert.deepEqual(events.filter(event => event.type === 'content_block_stop').map(event => event.index), indexes);
  const open = new Set();
  const streamed = [];
  for (const event of events) {
    if (event.type === 'content_block_start') {
      open.add(event.index);
      streamed[event.index] = structuredClone(event.content_block);
    } else if (event.type === 'content_block_delta') {
      assert.ok(open.has(event.index), 'Delta indexes must refer to an open, rebased block');
      if (event.delta.type === 'text_delta') streamed[event.index].text += event.delta.text;
    } else if (event.type === 'content_block_stop') {
      assert.ok(open.delete(event.index), 'Each rebased block must close exactly once');
    }
  }
  assert.equal(open.size, 0);
  assert.equal(textOf(streamed), PART_ONE + PART_TWO);
  const deltas = events.filter(event => event.type === 'message_delta');
  assert.deepEqual(deltas.filter(event => event.delta.stop_reason).map(event => event.delta.stop_reason), [stopReason],
    'No intermediate max_tokens terminal delta may escape to the host');
  assert.equal(deltas.at(-1).usage.output_tokens, 21);
}

// Real SDK child and MCP transport; every inference is served by loopback.
test('real SDK offline: max_tokens text continues as one native assistant message', async t => {
  const { api, controller, makeManager, queries, callbacks } = await offlineSdk(t, (body, index) => {
    if (index === 0) return truncated();
    if (index === 1) {
      assertContinuationRequest(body);
      return { ...continued([{ type: 'text', text: PART_TWO }]), stop_reason: 'end_turn' };
    }
    assert.equal(index, 2, 'Only one ordinary follow-up inference is expected');
    assert.ok(JSON.stringify(body.messages).includes(FOLLOWUP));
    assert.ok(JSON.stringify(body.messages).includes(PART_ONE));
    assert.ok(JSON.stringify(body.messages).includes(PART_TWO));
    return [{ type: 'text', text: 'OFFLINE_ORDINARY_REPLY' }];
  });
  const manager = makeManager();
  const history = [{ role: 'user', content: FIRST }];
  const events = [];
  const context = { sessionId: 'offline-text-output-continuation', signal: controller.signal };
  const first = await manager.handle({ ...BASE, messages: history }, { ...context, onEvent: event => events.push(event) });
  api.assertHealthy();
  assertJoinedResponse(first, events, [
    { type: 'text', text: PART_ONE }, { type: 'text', text: PART_TWO },
  ], 'end_turn');
  assert.equal(api.requests.length, 2);
  assert.equal(queries.length, 1, 'Output continuation must remain inside the original SDK query');
  assert.equal(callbacks.length, 0);

  history.push(asMessage(first), { role: 'user', content: FOLLOWUP });
  const second = await manager.handle({ ...BASE, messages: history }, context);
  assert.equal(second.stop_reason, 'end_turn');
  assert.equal(textOf(second.content), 'OFFLINE_ORDINARY_REPLY');
  assert.equal(queries.length, 1, 'An ordinary turn after continuation must reuse the SDK query');
  assert.equal(api.requests.length, 3);
  assert.equal(callbacks.length, 0);
  await manager.close();
  api.assertHealthy();
});

test('real SDK offline: max_tokens continuation parks the full parallel batch and resumes once', async t => {
  let sdkToolName;
  const { api, controller, makeManager, queries, callbacks, callbackQueries, settled } = await offlineSdk(t, (body, index) => {
    if (index === 0) {
      sdkToolName = body.tools.find(tool => tool.description === TOOLS[0].description).name;
      return truncated();
    }
    if (index === 1) {
      assertContinuationRequest(body);
      return { ...continued([
        { type: 'text', text: PART_TWO },
        ...IDS.map(id => ({ type: 'tool_use', id, name: sdkToolName, input: INPUT })),
      ]), stop_reason: 'tool_use' };
    }
    assert.ok(index === 2 || index === 3, 'Unexpected extra SDK inference');
    const results = blocks(body).filter(block => block.type === 'tool_result');
    assert.deepEqual(results.map(block => block.tool_use_id).sort(), [...IDS].sort());
    for (const id of IDS) assert.ok(textOf(results.find(block => block.tool_use_id === id).content).includes(`HOST_RESULT_${id}`));
    const uses = blocks(body).filter(block => block.type === 'tool_use');
    assert.deepEqual(uses.map(({ id, name, input }) => ({ id, name, input })),
      IDS.map(id => ({ id, name: sdkToolName, input: INPUT })));
    if (index === 3) assert.ok(JSON.stringify(body.messages).includes(FOLLOWUP));
    return [{ type: 'text', text: index === 2 ? 'OFFLINE_RESULTS_RECEIVED' : 'OFFLINE_ORDINARY_REPLY' }];
  });
  const manager = makeManager();
  const history = [{ role: 'user', content: FIRST }];
  const events = [];
  const context = { sessionId: 'offline-tool-output-continuation', signal: controller.signal };
  const first = await manager.handle({ ...BASE, messages: history }, { ...context, onEvent: event => events.push(event) });
  api.assertHealthy();
  assertJoinedResponse(first, events, [
    { type: 'text', text: PART_ONE }, { type: 'text', text: PART_TWO },
    ...IDS.map(id => ({ type: 'tool_use', id, name: 'offline_probe', input: INPUT })),
  ], 'tool_use');
  assert.equal(api.requests.length, 2, 'SDK must park before any inference using tool results');
  assert.equal(queries.length, 1);
  await until(() => callbacks.length > 0, { signal: controller.signal });
  assert.deepEqual(settled, [], 'Real MCP callbacks stay parked until the host supplies results');
  assert.ok(callbacks.every(call => IDS.includes(call._meta?.['claudecode/toolUseId'])));
  const streamedUses = events.filter(event => event.type === 'content_block_start' && event.content_block.type === 'tool_use');
  assert.deepEqual(streamedUses.map(event => event.content_block.id), IDS);
  assert.ok(streamedUses.every(event => event.content_block.name === 'offline_probe'));

  history.push(asMessage(first), { role: 'user', content: [...IDS].reverse().map(id => ({
    type: 'tool_result', tool_use_id: id, content: `HOST_RESULT_${id}`,
  })) });
  const second = await manager.handle({ ...BASE, messages: history }, context);
  api.assertHealthy();
  assert.equal(second.stop_reason, 'end_turn');
  assert.equal(textOf(second.content), 'OFFLINE_RESULTS_RECEIVED');
  assert.equal(queries.length, 1, 'Host results must resume the same query that performed output continuation');
  assert.deepEqual([...settled].sort(), [...IDS].sort());
  assert.deepEqual(callbacks.map(call => call._meta?.['claudecode/toolUseId']).sort(), [...IDS].sort());
  assert.deepEqual(callbackQueries, [0, 0]);
  assert.ok(callbacks.every(call => JSON.stringify(call.arguments) === JSON.stringify(INPUT)));

  history.push(asMessage(second), { role: 'user', content: FOLLOWUP });
  const third = await manager.handle({ ...BASE, messages: history }, context);
  assert.equal(third.stop_reason, 'end_turn');
  assert.equal(textOf(third.content), 'OFFLINE_ORDINARY_REPLY');
  assert.equal(queries.length, 1, 'The subsequent ordinary turn must still reuse the query');
  assert.equal(callbacks.length, 2, 'Neither identical tool call may execute twice');
  assert.equal(api.requests.length, 4);
  await manager.close();
  api.assertHealthy();
});
