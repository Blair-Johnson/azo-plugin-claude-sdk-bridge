import { createHash } from 'node:crypto';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js';
import { validateSchemaCompatibility } from './request.mjs';

const SDK_PREFIX = 'mcp__azo__';
const ID_META = 'claudecode/toolUseId';
const MAX_CALLS = 4096;
const MAX_JSON_BYTES = 8 * 1024 * 1024;
const MAX_QUEUED_BYTES = 32 * 1024 * 1024;

function invalid(message) {
  return Object.assign(new Error(message), { statusCode: 400, publicMessage: message });
}
function assert(condition, message) {
  if (!condition) throw invalid(message);
}
function record(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    && [Object.prototype, null].includes(Object.getPrototypeOf(value));
}
function hash(text) {
  return createHash('sha256').update(text).digest('base64url');
}

// Snapshot JSON, reject values JSON.stringify would silently discard/change, and
// canonicalize object-key order for argument matching and exact result retries.
function snapshot(value) {
  const ancestors = new Set();
  let nodes = 0;
  function encode(item, depth) {
    assert(++nodes <= 100000 && depth <= 64, 'Tool JSON exceeds complexity limits.');
    if (item === null || typeof item === 'boolean' || typeof item === 'string') return JSON.stringify(item);
    if (typeof item === 'number') {
      assert(Number.isFinite(item), 'Tool JSON contains an invalid number.');
      return JSON.stringify(item);
    }
    assert(Array.isArray(item) || record(item), 'Tool payload must contain only JSON values.');
    assert(!ancestors.has(item), 'Tool JSON must not contain cycles.');
    ancestors.add(item);
    let encoded;
    if (Array.isArray(item)) {
      encoded = `[${Array.from(item, (entry) => encode(entry, depth + 1)).join(',')}]`;
    } else {
      encoded = `{${Object.keys(item).sort().map((key) => `${JSON.stringify(key)}:${encode(item[key], depth + 1)}`).join(',')}}`;
    }
    ancestors.delete(item);
    return encoded;
  }
  const encoded = encode(value, 0);
  const bytes = Buffer.byteLength(encoded);
  assert(bytes <= MAX_JSON_BYTES, 'Tool JSON exceeds size limits.');
  return { value: JSON.parse(encoded), key: hash(encoded), bytes };
}
function id(value) {
  assert(typeof value === 'string' && value.length > 0 && value.length <= 1024
    && !/[\x00-\x20\x7f]/u.test(value), 'Invalid tool-use ID.');
  return value;
}
function argumentsKey(value) {
  assert(record(value), 'Tool arguments must be a JSON object.');
  return snapshot(value).key;
}
function onlyKeys(value, allowed) {
  assert(Object.keys(value).every((key) => allowed.includes(key)), 'Unsupported tool-result fields.');
}
function resultContent(content) {
  if (content === undefined) return [];
  if (typeof content === 'string') return [{ type: 'text', text: content }];
  assert(Array.isArray(content), 'Tool-result content must be text or a content array.');
  return content.map((block) => {
    assert(record(block), 'Invalid tool-result content block.');
    if (block.type === 'text') {
      onlyKeys(block, ['type', 'text']);
      assert(typeof block.text === 'string', 'Tool-result text must be a string.');
      return { type: 'text', text: block.text };
    }
    if (block.type === 'image') {
      onlyKeys(block, ['type', 'source']);
      const source = block.source;
      assert(record(source), 'Invalid tool-result image source.');
      onlyKeys(source, ['type', 'media_type', 'data']);
      assert(source.type === 'base64' && typeof source.media_type === 'string'
        && /^image\/[A-Za-z0-9.+-]+$/u.test(source.media_type), 'Only base64 image results are supported.');
      assert(typeof source.data === 'string' && source.data.length > 0
        && /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/u.test(source.data)
        && Buffer.from(source.data, 'base64').toString('base64') === source.data, 'Invalid base64 image data.');
      return { type: 'image', mimeType: source.media_type, data: source.data };
    }
    throw invalid('Unsupported tool-result content type.');
  });
}

/**
 * One instance per SDK query. The engine must call expect() on the entire
 * host-named model batch BEFORE publishing the terminal message_stop to Agent Zoo.
 * Nothing here validates the full tool schema or executes host effects: those
 * belong to Agent Zoo. Raw JSON schemas are advertised without normalization.
 */
export class ToolBridge {
  #hostNames = new Map();
  #sdkNames = new Map();
  #calls = new Map();
  #pending = new Set();
  #queuedBytes = 0;
  #closed = false;
  #server;

  constructor(tools) {
    assert(Array.isArray(tools) && tools.length <= MAX_CALLS, 'Tools must be a bounded array.');
    const definitions = tools.map((tool) => {
      assert(record(tool), 'Invalid tool definition.');
      // cache_control is a transport hint, not tool behavior; all other unknown
      // configuration must fail rather than silently lose host semantics.
      assert(Object.keys(tool).every((key) => ['name', 'description', 'input_schema', 'type', 'cache_control'].includes(key)),
        'Unsupported tool configuration fields.');
      const name = tool.name;
      assert(typeof name === 'string' && name.length > 0 && Buffer.byteLength(name) <= 1024
        && name.isWellFormed() && !/[\x00-\x20\x7f]/u.test(name), 'Invalid host tool name.');
      assert(!this.#hostNames.has(name), 'Duplicate host tool name.');
      assert(tool.type === undefined || tool.type === 'custom', 'Only custom host tools are supported.');
      assert(record(tool.input_schema) && tool.input_schema.type === 'object', 'Tool input_schema must be an object schema.');
      validateSchemaCompatibility(tool.input_schema);
      assert(tool.description === undefined || typeof tool.description === 'string', 'Invalid tool description.');
      const schema = snapshot(tool.input_schema).value;
      const readable = `azo_${name}`;
      // Keep the fully qualified SDK name at most 64 characters. Literal host
      // names may overlap the hash namespace: reject collisions in either order.
      const hashed = !/^[A-Za-z0-9_-]+$/u.test(name) || SDK_PREFIX.length + readable.length > 64;
      const localName = hashed ? `azo_h_${hash(name)}` : readable;
      const sdkName = SDK_PREFIX + localName;
      assert(!this.#sdkNames.has(sdkName), 'Tool-name alias collision.');
      this.#hostNames.set(name, sdkName);
      this.#sdkNames.set(sdkName, name);
      const clarification = `Host tool name: ${JSON.stringify(name)}.`;
      const description = hashed
        ? (tool.description === undefined ? clarification : `${tool.description}\n\n${clarification}`)
        : tool.description;
      return { name: localName, ...(description === undefined ? {} : { description }), inputSchema: schema };
    });
    this.#server = new Server({ name: 'azo', version: '0.1.0' }, { capabilities: { tools: {} } });
    this.#server.setRequestHandler(ListToolsRequestSchema, () => ({ tools: structuredClone(definitions) }));
    this.#server.setRequestHandler(CallToolRequestSchema, (request) => this.#handle(request.params));
  }

  get sdkServer() { return { type: 'sdk', name: 'azo', instance: this.#server }; }
  // A snapshot prevents callers from corrupting the internal pending registry.
  // An ID stops being pending when its host result is accepted, even if MCP has
  // not yet asked for it; callback-before-expect IDs are not host-pending IDs.
  get pendingIds() { return new Set(this.#pending); }
  get hasWaitingCalls() { return [...this.#calls.values()].some(entry => typeof entry.resolve === 'function'); }

  toSdkName(hostName) {
    assert(this.#hostNames.has(hostName), 'Unknown host tool name.');
    return this.#hostNames.get(hostName);
  }
  fromSdkName(sdkName) {
    assert(this.#sdkNames.has(sdkName), 'Unknown SDK tool name.');
    return this.#sdkNames.get(sdkName);
  }
  #assertOpen() { assert(!this.#closed, 'Tool bridge is closed.'); }
  #matches(entry, name, key) {
    assert(entry.name === name && entry.argumentsKey === key, 'Tool callback does not match the model tool use.');
  }

  expect(contentBlocks) {
    this.#assertOpen();
    assert(Array.isArray(contentBlocks), 'Model content must be an array.');
    const staged = new Map();
    for (const block of contentBlocks) {
      assert(record(block) && typeof block.type === 'string', 'Invalid model content block.');
      if (block.type !== 'tool_use') continue;
      const callId = id(block.id);
      this.toSdkName(block.name); // HOST names only, never accept SDK aliases here.
      const key = argumentsKey(block.input);
      assert(!staged.has(callId), 'Duplicate tool-use ID in model batch.');
      const previous = this.#calls.get(callId);
      assert(!previous?.expected, 'Model tool-use ID was already registered.');
      if (previous) this.#matches(previous, block.name, key);
      staged.set(callId, previous ?? { name: block.name, argumentsKey: key });
    }
    const additions = [...staged.keys()].filter((key) => !this.#calls.has(key)).length;
    assert(this.#calls.size + additions <= MAX_CALLS, 'Tool query exceeds call limits.');
    // No state was changed above: malformed batches cannot publish a partial set.
    for (const [callId, entry] of staged) {
      entry.expected = true;
      this.#calls.set(callId, entry);
      this.#pending.add(callId);
    }
  }

  deliver(toolResultBlocks) {
    this.#assertOpen();
    assert(Array.isArray(toolResultBlocks), 'Tool results must be an array.');
    const staged = new Map();
    let additionalBytes = 0;
    for (const block of toolResultBlocks) {
      assert(record(block) && block.type === 'tool_result', 'Expected a tool_result block.');
      onlyKeys(block, ['type', 'tool_use_id', 'content', 'is_error']);
      const callId = id(block.tool_use_id);
      assert(!staged.has(callId), 'Duplicate tool-use ID in result batch.');
      const entry = this.#calls.get(callId);
      assert(entry?.expected, 'Result refers to an unregistered model tool-use ID.');
      assert(block.is_error === undefined || typeof block.is_error === 'boolean', 'Tool-result is_error must be boolean.');
      const copy = snapshot(block);
      const result = { content: resultContent(copy.value.content), isError: copy.value.is_error ?? false };
      assert(entry.resultKey === undefined || entry.resultKey === copy.key, 'Conflicting duplicate tool result.');
      const fresh = entry.resultKey === undefined;
      if (fresh && !entry.resolve) additionalBytes += copy.bytes;
      staged.set(callId, { entry, result, key: copy.key, bytes: copy.bytes, fresh });
    }
    assert(this.#queuedBytes + additionalBytes <= MAX_QUEUED_BYTES, 'Queued tool results exceed size limits.');
    // Validate the entire result batch before resolving ANY parked SDK handler.
    for (const [callId, { entry, result, key, bytes, fresh }] of staged) {
      if (!fresh) continue;
      entry.resultKey = key;
      this.#pending.delete(callId);
      if (entry.resolve) {
        const resolve = entry.resolve;
        delete entry.resolve;
        resolve(result);
      } else {
        entry.result = result;
        entry.bytes = bytes;
        this.#queuedBytes += bytes;
      }
    }
  }

  #handle(params) {
    if (this.#closed) return this.#closedResult();
    const name = this.fromSdkName(SDK_PREFIX + params.name);
    // Deliberately NO fallback by name, arrival order, or request JSON-RPC ID.
    const callId = id(params._meta?.[ID_META]);
    const key = argumentsKey(params.arguments);
    let entry = this.#calls.get(callId);
    if (entry) {
      this.#matches(entry, name, key);
      assert(!entry.callbackSeen, 'Duplicate SDK tool callback.');
    } else {
      assert(this.#calls.size < MAX_CALLS, 'Tool query exceeds call limits.');
      entry = { name, argumentsKey: key };
      this.#calls.set(callId, entry);
    }
    entry.callbackSeen = true;
    if (entry.expected && entry.result !== undefined) {
      const result = entry.result;
      this.#queuedBytes -= entry.bytes;
      delete entry.result;
      delete entry.bytes;
      return result;
    }
    // An early SDK callback cannot receive a host result until expect() has
    // registered and checked its ID, host name, and exact JSON arguments.
    return new Promise((resolve) => { entry.resolve = resolve; });
  }

  #closedResult() {
    return { content: [{ type: 'text', text: 'Tool bridge closed before the tool result was available.' }], isError: true };
  }

  close(_reason) {
    if (this.#closed) return;
    this.#closed = true;
    // Do not echo arbitrary exception text (paths, prompts, credentials) to SDK.
    for (const entry of this.#calls.values()) entry.resolve?.(this.#closedResult());
    this.#calls.clear();
    this.#pending.clear();
    this.#queuedBytes = 0;
    // The engine owns transport shutdown. Closing it here can race the error
    // replies above and strand the SDK's outstanding tools/call requests.
  }
}
