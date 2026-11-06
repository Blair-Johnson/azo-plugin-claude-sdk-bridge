import { createHash, randomUUID } from 'node:crypto';
import { mkdir, open, unlink } from 'node:fs/promises';
import { dirname, isAbsolute } from 'node:path';
import { Session, getSessionPath, normalizeProjectPath, serializeJsonl } from 'cc-session-io';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const TOOL_ID = /^[A-Za-z0-9_-]+$/;
const TOOL_NAME = /^[A-Za-z0-9_-]{1,128}$/;

function invalid(message) {
  return Object.assign(new Error(message), { statusCode: 400, publicMessage: message });
}

function object(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

// No JSON round-trip: it silently drops undefined/nonfinite values. Sorting here
// also makes the canonical representation independent of object insertion order.
function copyJson(value, ancestors = new Set()) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value !== 'object' || ancestors.has(value)) throw invalid('Expected finite, acyclic JSON data.');
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) {
    throw invalid('Expected plain JSON data.');
  }
  ancestors.add(value);
  const result = Array.isArray(value)
    ? Array.from(value, (item) => copyJson(item, ancestors))
    : Object.fromEntries(Object.keys(value).sort().map((key) => [key, copyJson(value[key], ancestors)]));
  ancestors.delete(value);
  return result;
}

function canonicalContent(content) {
  if (typeof content === 'string') return [{ type: 'text', text: content }];
  if (!Array.isArray(content)) throw invalid('Message content must be text or content blocks.');
  return content.map((block) => {
    if (!object(block)) throw invalid('Content blocks must be objects.');
    const result = copyJson(block);
    delete result.cache_control;
    // Only protocol cache markers are ignored. A cache_control key inside tool
    // input, image data, citations, or a JSON schema is actual content.
    if (result.type === 'tool_result' && Object.hasOwn(result, 'content')) {
      result.content = canonicalContent(result.content);
    }
    return result;
  });
}

/** Deep canonical native Messages copy. Nothing but cache markers is omitted;
 * string content and its one-text-block form have the same representation. */
export function canonicalMessages(messages) {
  if (!Array.isArray(messages)) throw invalid('Messages must be an array.');
  return messages.map((message) => {
    if (!object(message)) throw invalid('Messages must be objects.');
    const result = copyJson(message);
    delete result.cache_control;
    result.content = canonicalContent(result.content);
    return result;
  });
}

/** SHA-256 of stable JSON; arrays (including block/tool order) remain ordered. */
export function digest(value) {
  return createHash('sha256').update(JSON.stringify(copyJson(value))).digest('hex');
}

/** History is checked separately. Conservatively include unknown request options
 * so future semantic fields cannot accidentally reuse an incompatible session. */
export function requestFingerprint(body) {
  if (!object(body)) throw invalid('Request must be an object.');
  const options = Object.fromEntries(Object.entries(body).filter(([key]) =>
    !['messages', 'stream', 'metadata', 'cache_control'].includes(key)));
  const canonical = copyJson(options);
  if (Object.hasOwn(canonical, 'system')) canonical.system = canonicalContent(canonical.system);
  if (Array.isArray(canonical.tools)) {
    canonical.tools = canonical.tools.map((tool) => {
      if (!object(tool)) throw invalid('Tool definitions must be objects.');
      delete tool.cache_control;
      return tool;
    });
  }
  return digest(canonical);
}

function validateBlock(block, role, nested = false) {
  switch (block.type) {
    case 'text':
      if (typeof block.text !== 'string') throw invalid('Text blocks require text.');
      break;
    case 'image': {
      if (role !== 'user') throw invalid('Images are only supported in user content.');
      const source = block.source;
      if (!object(source) || !(
        (source.type === 'base64' && typeof source.media_type === 'string' && source.media_type.length &&
          typeof source.data === 'string' && source.data.length) ||
        (source.type === 'url' && typeof source.url === 'string' && source.url.length)
      )) throw invalid('Invalid image source.');
      break;
    }
    case 'thinking':
      if (role !== 'assistant' || nested || typeof block.thinking !== 'string' ||
          typeof block.signature !== 'string' || !block.signature.length) {
        throw invalid('Thinking blocks require an assistant role and a signature.');
      }
      break;
    case 'redacted_thinking':
      if (role !== 'assistant' || nested || typeof block.data !== 'string' || !block.data.length) {
        throw invalid('Invalid redacted thinking block.');
      }
      break;
    case 'tool_use':
      if (role !== 'assistant' || nested || !TOOL_ID.test(block.id ?? '') ||
          typeof block.id !== 'string' || typeof block.name !== 'string' || !block.name.length || Buffer.byteLength(block.name) > 1024 ||
          !block.name.isWellFormed() || /[\x00-\x20\x7f]/u.test(block.name) ||
          !object(block.input)) throw invalid('Invalid tool use block.');
      break;
    case 'tool_result':
      if (role !== 'user' || nested || typeof block.tool_use_id !== 'string' ||
          !TOOL_ID.test(block.tool_use_id) ||
          (Object.hasOwn(block, 'is_error') && typeof block.is_error !== 'boolean')) {
        throw invalid('Invalid tool result block.');
      }
      // An absent/empty result is a valid real tool outcome, not a synthetic turn.
      for (const child of block.content ?? []) validateBlock(child, 'user', true);
      break;
    default:
      throw invalid('Unsupported history content block type.');
  }
}

function prepareMessages(messages, toolNameToSdk, { allowPending = true, mapTools = true } = {}) {
  const canonical = canonicalMessages(messages);
  const pending = new Set();
  const seen = new Set();
  const prepared = [];
  for (const message of canonical) {
    if (!['user', 'assistant'].includes(message.role)) throw invalid('Unsupported history message role.');
    if (Object.keys(message).some((key) => !['role', 'content'].includes(key))) {
      throw invalid('Unsupported history message field.');
    }
    const blocks = message.content;
    if (!blocks.length || !blocks.some((block) => block.type !== 'text' || block.text?.length)) {
      throw invalid('Empty history messages are not supported.');
    }
    for (const block of blocks) validateBlock(block, message.role);
    if (message.role === 'assistant') {
      if (pending.size) throw invalid('Tool results are missing before an assistant turn.');
      for (const block of blocks) {
        if (block.type !== 'tool_use') continue;
        if (seen.has(block.id)) throw invalid('Duplicate history tool use ID.');
        seen.add(block.id);
        pending.add(block.id);
        if (!mapTools) continue;
        let name;
        try { name = typeof toolNameToSdk === 'function' ? toolNameToSdk(block.name) : undefined; }
        catch { throw invalid('Unknown history tool name.'); }
        if (typeof name !== 'string' || !TOOL_NAME.test(name)) throw invalid('Unknown history tool name.');
        block.name = name;
      }
    } else {
      let rest = false;
      const hadPending = pending.size > 0;
      let results = 0;
      for (const block of blocks) {
        if (block.type !== 'tool_result') { rest = true; continue; }
        if (rest) throw invalid('Tool results must precede other user content.');
        if (!pending.delete(block.tool_use_id)) throw invalid('Unmatched or duplicate history tool result.');
        results++;
      }
      if (hadPending && (!results || (pending.size && rest))) {
        throw invalid('Tool results are missing before user content.');
      }
      // Native requests may split a parallel batch over adjacent user messages.
      // Emit one record for those results, retaining their order and mixed text.
      const previous = prepared.at(-1);
      if (results && previous?.role === 'user' && previous.content.every((block) => block.type === 'tool_result')) {
        previous.content.push(...blocks);
        continue;
      }
    }
    prepared.push(message);
  }
  if (!allowPending && pending.size) throw invalid('The complete history is missing tool results.');
  // Import prefixes may end pending; full incoming histories may not.
  return prepared;
}

/** Validate every turn, including final-user results, without mapping or rewriting. */
export function validateHistory(messages) {
  prepareMessages(messages, undefined, { allowPending: false, mapTools: false });
}

/** Import a complete native historical prefix into a fresh private transcript.
 * Signed/redacted thinking is preserved verbatim. The caller must explicitly
 * remove incompatible provider signatures BEFORE canonical comparison/import;
 * this module cannot establish which provider signed an opaque signature.
 * No repair helpers are used: they fabricate results and discard orphan calls.
 * The caller owns the returned file (and any later SDK artifacts) and cleanup.
 */
export async function importSession({ messages, toolNameToSdk, cwd, model = 'unknown', sessionId = randomUUID() }) {
  if (typeof sessionId !== 'string' || !UUID.test(sessionId)) throw invalid('Session ID must be a UUID.');
  if (typeof cwd !== 'string' || !isAbsolute(cwd) || cwd.includes('\0')) throw invalid('Session cwd must be an absolute path.');
  const prepared = prepareMessages(messages, toolNameToSdk);
  const projectPath = normalizeProjectPath(cwd);
  const path = getSessionPath(sessionId, projectPath, process.env.CLAUDE_CONFIG_DIR);
  const session = new Session({ sessionId, projectPath, cwd: projectPath, jsonlPath: path, model });
  for (const message of prepared) {
    if (message.role === 'assistant') session.addAssistantMessage(message.content);
    else if (message.content.every((block) => block.type === 'tool_result')) session.addToolResultBlocks(message.content);
    else session.addUserMessage(message.content);
  }
  const data = session.records.length ? serializeJsonl([...session.records]) : '';
  let file;
  try {
    await mkdir(dirname(path), { recursive: true, mode: 0o700 });
    // Session.save() can append/overwrite. Use its public JSONL writer with an
    // exclusive open instead; even an existing symlink must never be followed.
    file = await open(path, 'wx', 0o600);
    await file.chmod(0o600);
    await file.writeFile(data, 'utf8');
    await file.close();
  } catch (error) {
    if (file) {
      await file.close().catch(() => {});
      await unlink(path).catch(() => {});
    }
    if (error.code === 'EEXIST') {
      throw Object.assign(new Error('Session already exists.'), { statusCode: 409, publicMessage: 'Session already exists.' });
    }
    throw Object.assign(new Error('Unable to write imported session.', { cause: error }), {
      statusCode: 500, publicMessage: 'Unable to write imported session.',
    });
  }
  return { sessionId, path };
}
