// Native SDK rollback after an interrupt. The SDK's own transcript keeps its
// startup reminders where the live process first attached them, so resuming it
// (unlike re-importing host history) leaves the cached request prefix intact.

const CHAIN = new Set(['user', 'assistant', 'attachment', 'system']);
const MARKER = /^\[Request interrupted by user/;

function text(record) {
  const content = record.message?.content;
  if (typeof content === 'string') return content;
  return Array.isArray(content) ? content.filter(b => b?.type === 'text').map(b => b.text).join('') : '';
}

/** Find where to resume a persisted SDK transcript so that exactly the
 * interrupted answer is discarded. `messageIds` are the provider message IDs of
 * the interrupted response; `turnPrompt` is the UUID of the prompt that started
 * the SDK turn. Returns null unless everything past the resume point is that
 * undelivered answer, its interruption marker, or SDK bookkeeping. */
export function rollbackPoint(records, { messageIds, turnPrompt }) {
  const byId = new Map();
  let leaf;
  for (const record of records) {
    if (typeof record?.uuid !== 'string' || !CHAIN.has(record.type)) continue;
    byId.set(record.uuid, record);
    leaf = record;
  }
  const chain = [];
  for (let record = leaf; record;) {
    chain.unshift(record);
    if (chain.length > byId.size) return null;
    if (record.parentUuid == null) break;
    record = byId.get(record.parentUuid);
    if (!record) return null;
  }
  const start = chain.findIndex(record => record.uuid === turnPrompt);
  if (start < 0) return null;
  const dropped = record => record.type === 'assistant' && messageIds.has(record.message?.id)
    || record.type === 'user' && MARKER.test(text(record));
  let cut = chain.findIndex((record, index) => index > start && dropped(record));
  if (cut < 0) cut = chain.length;
  // A queued user message or a real tool result past the cut is not ours to drop.
  if (chain.slice(cut).some(record => !dropped(record) && !['attachment', 'system'].includes(record.type))) return null;
  return { resumeSessionAt: chain[cut - 1].uuid, dropped: chain.length - cut };
}
