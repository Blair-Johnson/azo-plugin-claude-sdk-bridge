// Anthropic server-side web search through Claude Code's built-in WebSearch.
//
// Host requests opt in with a `web_search_20250305` server tool. The SDK runs
// each search itself; the bridge reports it to the host as the standard
// `server_tool_use` / `web_search_tool_result` blocks of one native response.
// Each result block also keeps the SDK's exact tool_result as `sdk_tool_result`,
// so a later rebuild can restore what the model actually read.

export const SDK_WEB_SEARCH = 'WebSearch';
export const HOST_WEB_SEARCH = 'web_search';
const SERVER_TOOL = /^web_search_\d{8}$/;
const ERROR_CODES = new Set(['too_many_requests', 'invalid_input', 'max_uses_exceeded', 'query_too_long', 'unavailable']);
const MAX_USES_DENIAL = 'max_uses_exceeded';

const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
function invalid(message) {
  return Object.assign(new Error(message), { statusCode: 400, publicMessage: message });
}
function domains(value, key) {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || !value.length || value.length > 100
      || value.some(item => typeof item !== 'string' || !item || item.length > 253 || /[\s/]/.test(item)))
    throw invalid(`web_search ${key} must be a nonempty list of domain names.`);
  return [...value];
}

export const isServerTool = tool => object(tool) && typeof tool.type === 'string' && tool.type !== 'custom';

/** Split host function tools from the optional server web search tool. */
export function serverTools(tools = []) {
  const host = [], server = tools.filter(isServerTool);
  for (const tool of tools) if (!isServerTool(tool)) host.push(tool);
  if (!server.length) return { host, webSearch: null };
  if (server.length > 1) throw invalid('Only one server tool is supported by this bridge.');
  const [tool] = server;
  if (!SERVER_TOOL.test(tool.type)) throw invalid(`Unsupported server tool type: ${tool.type.slice(0, 64)}.`);
  const keys = ['type', 'name', 'max_uses', 'allowed_domains', 'blocked_domains', 'cache_control'];
  const extra = Object.keys(tool).find(key => !keys.includes(key));
  if (extra === 'user_location') throw invalid('web_search user_location is not supported by the Claude SDK.');
  if (extra) throw invalid(`Unsupported web_search option: ${extra.slice(0, 64)}.`);
  if (tool.name !== undefined && tool.name !== HOST_WEB_SEARCH) throw invalid('The web_search server tool must be named web_search.');
  if (tool.max_uses !== undefined && (!Number.isSafeInteger(tool.max_uses) || tool.max_uses < 1))
    throw invalid('web_search max_uses must be a positive integer.');
  const allowed = domains(tool.allowed_domains, 'allowed_domains');
  const blocked = domains(tool.blocked_domains, 'blocked_domains');
  if (allowed && blocked) throw invalid('web_search accepts allowed_domains or blocked_domains, not both.');
  if (host.some(item => item?.name === SDK_WEB_SEARCH || item?.name === HOST_WEB_SEARCH))
    throw invalid('A host tool name conflicts with server web search.');
  return { host, webSearch: { maxUses: tool.max_uses, allowed, blocked } };
}

/** Permission decision for one SDK WebSearch call. Host domain filters replace
 * the model's own; `used` counts searches already allowed in this response. */
export function searchPermission(config, input, used) {
  if (!config) return { behavior: 'deny', message: 'Web search is not enabled for this request.' };
  if (config.maxUses !== undefined && used >= config.maxUses) return { behavior: 'deny', message: MAX_USES_DENIAL };
  const updated = { ...input };
  if (config.allowed) { updated.allowed_domains = [...config.allowed]; delete updated.blocked_domains; }
  else if (config.blocked) { updated.blocked_domains = [...config.blocked]; delete updated.allowed_domains; }
  return { behavior: 'allow', updatedInput: updated };
}

const safeUrl = value => {
  if (typeof value !== 'string' || value.length > 4096) return null;
  try { return ['http:', 'https:'].includes(new URL(value).protocol) ? value : null; } catch { return null; }
};

/** Convert the SDK's WebSearch outcome into a native web_search_tool_result. */
export function searchResultBlock(toolResult, output) {
  const id = toolResult.tool_use_id;
  const results = [], errors = [];
  for (const item of Array.isArray(output?.results) ? output.results : []) {
    if (typeof item === 'string') {
      const match = /^Web search error: (\S+)/.exec(item);
      if (match) errors.push(match[1]);
    } else if (object(item) && Array.isArray(item.content)) {
      for (const hit of item.content) {
        const url = safeUrl(hit?.url);
        if (url && !results.some(result => result.url === url)) {
          results.push({ type: 'web_search_result', url, title: typeof hit.title === 'string' ? hit.title : url, page_age: null, encrypted_content: '' });
        }
      }
    }
  }
  const text = typeof toolResult.content === 'string' ? toolResult.content
    : Array.isArray(toolResult.content) ? toolResult.content.filter(b => b?.type === 'text').map(b => b.text).join('\n') : '';
  if (!output && !toolResult.is_error) {
    // No structured output for this record: read the SDK's own "Links:" lines.
    for (const match of text.matchAll(/^Links: (\[.*\])$/gm)) {
      try { for (const hit of JSON.parse(match[1])) {
        const url = safeUrl(hit?.url);
        if (url && !results.some(result => result.url === url)) {
          results.push({ type: 'web_search_result', url, title: typeof hit.title === 'string' ? hit.title : url, page_age: null, encrypted_content: '' });
        }
      } } catch { /* not the SDK's JSON link list */ }
    }
    const match = /^Web search error: (\S+)/m.exec(text);
    if (match) errors.push(match[1]);
  }
  let error = null;
  if (toolResult.is_error) error = text.trim() === MAX_USES_DENIAL ? MAX_USES_DENIAL : 'unavailable';
  else if (!results.length && errors.length) error = ERROR_CODES.has(errors[0]) ? errors[0] : 'unavailable';
  return {
    type: 'web_search_tool_result', tool_use_id: id,
    content: error ? { type: 'web_search_tool_result_error', error_code: error } : results,
    sdk_tool_result: { content: structuredClone(toolResult.content ?? ''), is_error: Boolean(toolResult.is_error) },
  };
}

// What the SDK model would have read, for results the SDK did not produce here.
function sdkToolResult(block) {
  const saved = block?.sdk_tool_result;
  if (object(saved) && (typeof saved.content === 'string' || Array.isArray(saved.content))) {
    return { type: 'tool_result', tool_use_id: block.tool_use_id, content: structuredClone(saved.content),
      ...(saved.is_error ? { is_error: true } : {}) };
  }
  if (!block) return { type: 'tool_result', tool_use_id: undefined, content: 'Web search result unavailable.', is_error: true };
  if (Array.isArray(block.content)) {
    const links = block.content.filter(hit => object(hit) && typeof hit.url === 'string')
      .map(hit => ({ title: typeof hit.title === 'string' ? hit.title : hit.url, url: hit.url }));
    return { type: 'tool_result', tool_use_id: block.tool_use_id, content: `Web search results.\n\nLinks: ${JSON.stringify(links)}` };
  }
  const code = object(block.content) && typeof block.content.error_code === 'string' ? block.content.error_code : 'unavailable';
  return { type: 'tool_result', tool_use_id: block.tool_use_id, content: `Web search error: ${code}`, is_error: true };
}

/** Rewrite host history into the SDK's own form: each server_tool_use becomes a
 * WebSearch tool_use, answered by a tool_result in the user turn that follows
 * it. A host message is split where a search result appears mid-response, and
 * results delivered in a later response are moved back to their call. Returns
 * the rewritten messages and the IDs of the converted calls. */
export function lowerServerTools(messages) {
  const results = new Map();
  for (const message of messages) {
    if (message.role !== 'assistant') continue;
    for (const block of message.content) if (block.type === 'web_search_tool_result') results.set(block.tool_use_id, block);
  }
  const searchIds = new Set();
  if (!messages.some(message => message.role === 'assistant'
      && message.content.some(block => block.type === 'server_tool_use' || block.type.endsWith('_tool_result')
        || block.type === 'text' && Array.isArray(block.citations)))) {
    return { messages, searchIds };
  }
  const out = [];
  let open = [];
  const answer = (ids) => ids.map(id => ({ ...sdkToolResult(results.get(id)), tool_use_id: id }));
  for (const message of messages) {
    if (message.role === 'user') {
      if (open.length) {
        const own = message.content.filter(block => block.type === 'tool_result');
        out.push({ role: 'user', content: [...answer(open), ...own, ...message.content.filter(block => block.type !== 'tool_result')] });
        open = [];
      } else out.push(message);
      continue;
    }
    let segment = [];
    const flush = () => {
      if (segment.length) out.push({ role: 'assistant', content: segment });
      segment = [];
    };
    for (const block of message.content) {
      if (block.type === 'server_tool_use' && block.name !== HOST_WEB_SEARCH) continue; // other server tools: no SDK form
      if (block.type.endsWith('_tool_result') && !['tool_result', 'web_search_tool_result'].includes(block.type)) continue;
      if (block.type === 'server_tool_use') {
        searchIds.add(block.id);
        segment.push({ type: 'tool_use', id: block.id, name: SDK_WEB_SEARCH, input: structuredClone(block.input ?? {}) });
        open.push(block.id);
      } else if (block.type === 'web_search_tool_result') {
        // A result for a call in this segment closes it; a late one was already answered.
        if (!open.length || !segment.some(item => item.type === 'tool_use' && item.id === block.tool_use_id)) continue;
        if (segment.some(item => item.type === 'tool_use' && !searchIds.has(item.id))) continue; // mixed batch: next user turn
        flush();
        out.push({ role: 'user', content: answer(open) });
        open = [];
      } else if (block.type === 'text' && Array.isArray(block.citations)) {
        const { citations, ...text } = block;
        segment.push(text);
      } else segment.push(block);
    }
    flush();
  }
  return { messages: out, searchIds };
}
