import assert from 'node:assert/strict';
import { measuredManager } from './measured-manager.mjs';

if (!process.argv.includes('--live')) throw new Error('Pass --live to authorize three model turns');
const started = performance.now();
const emit = record => console.log(JSON.stringify({ elapsed_ms: Math.round(performance.now() - started), ...record }));
const { manager, marks, processes } = measuredManager();
let stage = 'initial', maxStops = 0;
const consume = manager.consume.bind(manager);
manager.consume = (slot, session, message) => {
  const event = message.event;
  if (event?.type === 'message_start' || event?.type === 'message_delta') {
    if (event.delta?.stop_reason === 'max_tokens') maxStops++;
    emit({ kind: 'sdk_boundary', stage, event: event.type, id: event.message?.id,
      stop_reason: event.delta?.stop_reason, usage: event.usage });
  }
  return consume(slot, session, message);
};
const requireThinking = process.argv.includes('--require-thinking');
const count = requireThinking ? 600 : 300;
const base = { model: 'claude-opus-5-5', max_tokens: requireThinking ? 1024 : 512, stream: true, tools: [],
  thinking: { type: 'adaptive' }, output_config: { effort: 'high' },
  system: 'This is an isolated streaming integrity test. Follow the user output format exactly.' };
const reasoning = requireThinking ? 'First reason privately about how many binary strings of length 20 have exactly 8 ones and no adjacent ones; check your combinatorial derivation. Do not include that reasoning or its result in the answer. Then ' : '';
const history = [{ role: 'user', content: `${reasoning}Write the integers 1 through ${count}, comma-separated, in order, with no omissions, ranges, shorthand, code fences, or commentary. Start with 1 and finish with ${count}.` }];
const controller = new AbortController();
const watchdog = setTimeout(() => controller.abort(new Error('Live diagnostic watchdog')), 240_000);
const handle = async (body) => {
  const events = [];
  const answer = await manager.handle(body, { sessionId: 'live-continuation-probe', signal: controller.signal,
    onEvent: event => events.push(event) });
  assert.equal(events.filter(e => e.type === 'message_start').length, 1);
  assert.equal(events.filter(e => e.type === 'message_stop').length, 1);
  assert.equal(events.some(e => e.delta?.stop_reason === 'max_tokens'), false);
  const starts = events.filter(e => e.type === 'content_block_start');
  assert.deepEqual(starts.map(e => e.index), starts.map((_, i) => i));
  emit({ kind: 'host_response', stage, stop_reason: answer.stop_reason, usage: answer.usage,
    block_types: answer.content.map(b => b.type), queries: marks.filter(m => m.event === 'query_construct').length });
  return answer;
};
const text = answer => answer.content.filter(b => b.type === 'text').map(b => b.text).join('');
try {
  emit({ kind: 'config', model: base.model, max_tokens: base.max_tokens, node: process.version });
  const first = await handle({ ...base, messages: history });
  assert.ok(maxStops > 0, 'Probe must actually exercise an SDK max_tokens continuation');
  assert.deepEqual(text(first).match(/\d+/g)?.map(Number), Array.from({ length: count }, (_, i) => i + 1));
  const signedBlocks = first.content.filter(b => b.type === 'thinking' && b.signature).length;
  if (requireThinking) assert.ok(signedBlocks > 0, 'Probe must retain a real signed thinking block');
  history.push({ role: 'assistant', content: first.content }, { role: 'user', content: 'Reply exactly OK.' });
  stage = 'warm';
  const warm = await handle({ ...base, messages: history });
  assert.equal(text(warm).trim(), 'OK');
  assert.equal(marks.filter(m => m.event === 'query_construct').length, 1);
  history.push({ role: 'assistant', content: warm.content }, { role: 'user', content: 'Reply exactly OK again.' });
  stage = 'cold';
  const cold = await handle({ ...base, max_tokens: base.max_tokens * 2, messages: history });
  assert.equal(text(cold).trim(), 'OK');
  assert.equal(marks.filter(m => m.event === 'query_construct').length, 2);
  emit({ kind: 'final', passed: true, max_tokens_segments: maxStops, signed_blocks_replayed: signedBlocks, warm_reused: true, cold_replay_accepted: true });
} catch (error) {
  emit({ kind: 'final', passed: false, stage, error: error.message });
  process.exitCode = 1;
} finally {
  clearTimeout(watchdog);
  await manager.close();
  emit({ kind: 'cleanup', children_exited: processes.every(p => p.didExit) });
}
