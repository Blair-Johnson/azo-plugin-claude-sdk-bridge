import test from 'node:test';
import assert from 'node:assert/strict';
import { rollbackPoint } from '../src/rollback.mjs';

// Minimal transcript records: only the fields rollbackPoint reads.
let n = 0;
const chain = [];
function add(type, extra = {}) {
  const record = { type, uuid: `u${++n}`, parentUuid: chain.at(-1)?.uuid ?? null, ...extra };
  chain.push(record);
  return record;
}
const text = value => ({ message: { content: [{ type: 'text', text: value }] } });
function transcript(build) {
  n = 0; chain.length = 0;
  build();
  return [{ type: 'queue-operation' }, ...chain, { type: 'last-prompt' }];
}
const ids = new Set(['msg_partial']);

test('cuts exactly before the interrupted answer and its marker', () => {
  let prompt, kept;
  const records = transcript(() => {
    add('user', text('first'));
    add('attachment', { attachment: { type: 'date' } });
    add('assistant', { message: { id: 'msg_done', content: [] } });
    prompt = add('user', text('count'));
    kept = add('attachment', { attachment: { type: 'prompt_snapshot' } });
    add('assistant', { message: { id: 'msg_partial', content: [] } });
    add('assistant', { message: { id: 'msg_partial', content: [] } });
    add('user', text('[Request interrupted by user]'));
    add('system', { subtype: 'turn_duration' });
  });
  assert.deepEqual(rollbackPoint(records, { messageIds: ids, turnPrompt: prompt.uuid }),
    { resumeSessionAt: kept.uuid, dropped: 4 });
});

test('keeps a completed tool exchange inside the interrupted turn', () => {
  let prompt, result;
  const records = transcript(() => {
    prompt = add('user', text('use tool'));
    add('assistant', { message: { id: 'msg_tool', content: [{ type: 'tool_use' }] } });
    result = add('user', { message: { content: [{ type: 'tool_result', tool_use_id: 't' }] } });
    add('assistant', { message: { id: 'msg_partial', content: [] } });
    add('user', text('[Request interrupted by user]'));
  });
  assert.equal(rollbackPoint(records, { messageIds: ids, turnPrompt: prompt.uuid }).resumeSessionAt, result.uuid);
});

test('a marker alone is dropped when no answer was persisted', () => {
  let prompt;
  const records = transcript(() => {
    prompt = add('user', text('count'));
    add('user', text('[Request interrupted by user]'));
  });
  assert.equal(rollbackPoint(records, { messageIds: ids, turnPrompt: prompt.uuid }).resumeSessionAt, prompt.uuid);
});

test('refuses to drop anything the host did not see', () => {
  for (const extra of [
    () => add('user', text('queued user message')),
    () => add('user', { message: { content: [{ type: 'tool_result', tool_use_id: 't' }] } }),
    () => add('assistant', { message: { id: 'msg_other', content: [] } }),
  ]) {
    let prompt;
    const records = transcript(() => {
      prompt = add('user', text('count'));
      add('assistant', { message: { id: 'msg_partial', content: [] } });
      extra();
    });
    assert.equal(rollbackPoint(records, { messageIds: ids, turnPrompt: prompt.uuid }), null);
  }
});

test('refuses an unknown turn prompt or a broken parent chain', () => {
  const records = transcript(() => {
    add('user', text('count'));
    add('assistant', { message: { id: 'msg_partial', content: [] } });
  });
  assert.equal(rollbackPoint(records, { messageIds: ids, turnPrompt: 'missing' }), null);
  const broken = records.map(record => record.uuid === 'u2' ? { ...record, parentUuid: 'gone' } : record);
  assert.equal(rollbackPoint(broken, { messageIds: ids, turnPrompt: 'u1' }), null);
  assert.equal(rollbackPoint([], { messageIds: ids, turnPrompt: 'u1' }), null);
});

test('follows the newest branch after an earlier rollback', () => {
  const records = transcript(() => {
    add('user', text('first'));
    add('assistant', { message: { id: 'msg_old_partial', content: [] } });
    add('user', text('[Request interrupted by user]'));
  });
  // A resumed process appends a new branch whose parent is the kept prompt.
  records.push({ type: 'user', uuid: 'b1', parentUuid: 'u1', ...text('steer') },
    { type: 'assistant', uuid: 'b2', parentUuid: 'b1', message: { id: 'msg_partial', content: [] } });
  assert.deepEqual(rollbackPoint(records, { messageIds: ids, turnPrompt: 'b1' }), { resumeSessionAt: 'b1', dropped: 1 });
});
