import { randomUUID } from 'node:crypto';
import { readFile, rm } from 'node:fs/promises';
import { createWorkspace } from './workspace.mjs';
import { ToolBridge, toolLabel } from './tools.mjs';
import { canonicalMessages, digest, importSession, isSessionId, requestFingerprint, transcriptPath, validateHistory } from './history.mjs';
import { rollbackPoint } from './rollback.mjs';
import { HOST_WEB_SEARCH, SDK_WEB_SEARCH, lowerServerTools, searchPermission, searchResultBlock, serverTools } from './websearch.mjs';
import { childEnvironment, isolatedSettings, QueryProcess } from './child.mjs';
import { errorText } from './errors.mjs';

export class BridgeError extends Error {
  constructor(statusCode, publicMessage, options) {
    super(publicMessage, options);
    this.name = 'BridgeError';
    this.statusCode = statusCode;
    this.publicMessage = publicMessage;
  }
}
const bad = message => new BridgeError(400, message);
const upstream = (reason = 'internal_error', detail) => new BridgeError(502,
  `The Claude SDK session failed. [reason=${reason}]${detail === undefined ? '' : `\n${errorText(detail)}`}`,
  detail === undefined ? undefined : { cause: detail });
// SDK failure categories as native Messages errors, so hosts can tell a usage
// limit or login problem from a transient server failure.
const SDK_ERROR_STATUS = {
  rate_limit: [429, 'rate_limit_error'], billing_error: [402, 'billing_error'],
  authentication_failed: [401, 'authentication_error'], cloud_credential_error: [401, 'authentication_error'],
  oauth_org_not_allowed: [403, 'permission_error'], account_on_hold: [403, 'permission_error'],
  verification_required: [403, 'permission_error'], model_not_found: [404, 'not_found_error'],
  invalid_request: [400, 'invalid_request_error'], max_output_tokens: [400, 'invalid_request_error'],
  overloaded: [529, 'overloaded_error'], server_error: [500, 'api_error'],
};
const HTTP_ERROR_TYPES = { 400: 'invalid_request_error', 401: 'authentication_error', 402: 'billing_error',
  403: 'permission_error', 404: 'not_found_error', 413: 'request_too_large', 429: 'rate_limit_error',
  529: 'overloaded_error' };
const sdkText = content => typeof content === 'string' ? content : Array.isArray(content)
  ? content.filter(block => block?.type === 'text' && typeof block.text === 'string').map(block => block.text).join('\n') : '';
const resetTime = seconds => Number.isFinite(seconds) && seconds > 0 ? new Date(seconds * 1000).toISOString() : undefined;

/** The SDK's own user-facing failure text, first and unabridged, plus the plan
 * limit and HTTP status it reported. Full envelopes remain in the diagnostics. */
function providerFailure(reason, sdk, session) {
  const failure = upstream(reason, sdk);
  const kind = typeof sdk?.error === 'string' ? sdk.error : undefined;
  const text = (sdk?.type === 'assistant' ? sdkText(sdk.message?.content)
    : sdk?.is_error && typeof sdk.result === 'string' ? sdk.result
    : (Array.isArray(sdk?.errors) ? sdk.errors.filter(item => typeof item === 'string').join('\n') : '')).trim();
  const limit = session?.rateLimit?.status === 'rejected' ? session.rateLimit : undefined;
  const httpStatus = Number.isInteger(sdk?.api_error_status) ? sdk.api_error_status : session?.lastRetry?.error_status;
  const details = [
    kind && `sdk_error=${kind}`,
    Number.isInteger(httpStatus) && `api_status=${httpStatus}`,
    limit && `limit=${limit.rateLimitType ?? 'unknown'}`,
    resetTime(limit?.resetsAt) && `resets_at=${resetTime(limit.resetsAt)}`,
    limit?.overageDisabledReason && `overage=${limit.overageDisabledReason}`,
    limit?.errorCode && `limit_error=${limit.errorCode}`,
  ].filter(Boolean);
  if (!text && !details.length) return failure;
  failure.providerMessage = (text || 'The Claude SDK reported a provider failure.')
    + (details.length ? ` [${details.join(' ')}]` : '');
  const [status, type] = SDK_ERROR_STATUS[kind] ?? (limit ? [429, 'rate_limit_error']
    : Number.isInteger(httpStatus) && httpStatus >= 400 && httpStatus < 600
      ? [httpStatus, HTTP_ERROR_TYPES[httpStatus] ?? (httpStatus >= 500 ? 'api_error' : 'invalid_request_error')]
      : [502, 'api_error']);
  failure.statusCode = status;
  failure.errorType = type;
  return failure;
}
const clone = value => structuredClone(value);
// The CLI answers a call to a tool it does not have ("No such tool available")
// and the model retries. Bound how often that may repeat within one host response.
const MAX_UNAVAILABLE_CALLS = 8;

function streamFailure(reason, request, nextId, session) {
  const stream = request.stream;
  return upstream(reason, {
    previous_message_id: stream?.message.id, next_message_id: nextId,
    previous_stopped: stream?.stopped, previous_stop_reason: stream?.message.stop_reason,
    previous_usage: stream?.message.usage,
    previous_block_types: [...(stream?.blocks.values() ?? [])].map(entry => entry.block.type),
    api_retry: session.lastRetry,
  });
}

// One native response can span multiple SDK max_tokens segments. Prompt/cache
// counters describe the LAST request (context size), not aggregate billing;
// output counters include all retained segments. Never sum cumulative deltas.
function continuationMessage(request, source) {
  const prefix = request.continuation;
  if (!prefix) return source;
  const usage = { ...source.usage,
    cache_creation_input_tokens: source.usage?.cache_creation_input_tokens ?? 0,
    cache_read_input_tokens: source.usage?.cache_read_input_tokens ?? 0,
    output_tokens: prefix.outputTokens + (source.usage?.output_tokens ?? 0) };
  const details = { ...source.usage?.output_tokens_details };
  for (const [key, value] of Object.entries(prefix.outputDetails)) details[key] = value + (details[key] ?? 0);
  if (Object.keys(details).length) usage.output_tokens_details = details;
  return { ...source, id: prefix.id, content: [...prefix.content, ...source.content], usage };
}

function retainContinuation(request) {
  const stream = request.stream;
  const prefix = request.continuation ??= {
    id: stream.message.id, content: [], outputTokens: 0, outputDetails: {}, ids: new Set(), published: false,
  };
  prefix.content.push(...stream.message.content);
  prefix.outputTokens += stream.message.usage?.output_tokens ?? 0;
  for (const [key, value] of Object.entries(stream.message.usage?.output_tokens_details ?? {})) {
    if (typeof value === 'number') prefix.outputDetails[key] = (prefix.outputDetails[key] ?? 0) + value;
  }
  prefix.ids.add(stream.message.id);
  prefix.published ||= stream.published;
}
function bridgeError(error, reason = 'internal_error') {
  if (error instanceof BridgeError) return error;
  const status = error?.statusCode;
  const failure = new BridgeError(Number.isInteger(status) && status >= 400 && status < 500 ? status : 502,
    `${errorText(error)} [reason=${reason}]`, { cause: error });
  failure.reason = reason;
  return failure;
}
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  // A setup failure may occur before handle() starts awaiting this promise.
  promise.catch(() => {});
  return { promise, resolve, reject };
}
// An HTTP abort can arrive just after its replacement request. Only observe the
// prior request: never cancel it, and never queue behind inference past 40 ms.
async function waitForTransportHandoff(request, signal) {
  let timer, onAbort;
  try {
    await Promise.race([request.promise.catch(() => {}), new Promise(resolve => {
      timer = setTimeout(resolve, 40);
      onAbort = resolve;
      signal?.addEventListener('abort', onAbort, { once: true });
      if (signal?.aborted) resolve();
    })]);
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener('abort', onAbort);
  }
}

function promptStream() {
  const queue = [];
  let ended = false, wake;
  return {
    push(content, uuid = randomUUID()) {
      if (ended) throw upstream('prompt_stream_closed');
      // Host messages are already composed: never expand mentions/commands or
      // attach CLI-local reminders to them. The UUID names the transcript record.
      queue.push({ type: 'user', message: { role: 'user', content: clone(content) }, parent_tool_use_id: null,
        client_composed: true, uuid });
      wake?.();
      wake = undefined;
    },
    end() { ended = true; queue.length = 0; wake?.(); wake = undefined; },
    async *[Symbol.asyncIterator]() {
      while (!ended) {
        if (queue.length) yield queue.shift();
        else await new Promise(resolve => { wake = resolve; });
      }
    },
  };
}
function settingsKey(body) {
  return requestFingerprint({ ...body, tools: body.tools ?? [] });
}

function same(a, b) { return digest(a) === digest(b); }
function systemPrompt(system) {
  if (system === undefined) return '';
  if (typeof system === 'string') return system;
  if (!Array.isArray(system) || system.some(b => b?.type !== 'text' || typeof b.text !== 'string'
      || Object.keys(b).some(k => !['type', 'text', 'cache_control'].includes(k)))) throw bad('Unsupported system content.');
  return system.map(b => b.text);
}
function thinkingOption(thinking) {
  if (thinking === undefined) return undefined;
  if (thinking.type === 'enabled') return { type: 'enabled', budgetTokens: thinking.budget_tokens };
  if (thinking.type === 'adaptive' || thinking.type === 'disabled') return { type: thinking.type };
  throw bad('Unsupported thinking configuration.');
}

function rebuildInput(messages) {
  // Claude Code repairs unresolved imported calls before it reads stdin. Import
  // every actual result first, including batches split over adjacent user records.
  // Keep the real trailing steering (text/images, in order) as the live prompt.
  for (let i = messages.length - 1; i >= 0 && messages[i].role === 'user'; i--) {
    const content = messages[i].content;
    const lastResult = content.findLastIndex(block => block.type === 'tool_result');
    if (lastResult < 0) continue;
    const prompt = [...content.slice(lastResult + 1), ...messages.slice(i + 1).flatMap(message => message.content)];
    return {
      prefix: [...messages.slice(0, i), { role: 'user', content: content.slice(0, lastResult + 1) }],
      prompt,
    };
  }
  return { prefix: messages.slice(0, -1), prompt: messages.at(-1).content };
}

// Native Agent Zoo coalesces adjacent user records. Accept only an unchanged
// prefix plus genuinely new user blocks; never replay the old prompt on reuse.
function appendedFeedback(previous, messages) {
  if (!previous?.length || previous.at(-1).role !== 'user' || messages.length < previous.length) return null;
  const index = previous.length - 1, before = previous[index], after = messages[index];
  if (!same(previous.slice(0, index), messages.slice(0, index)) || after.role !== 'user') return null;
  if (!same({ ...before, content: [] }, { ...after, content: [] })
      || !same(before.content, after.content.slice(0, before.content.length))) return null;
  const trailing = messages.slice(previous.length);
  if (trailing.some(message => message.role !== 'user')) return null;
  const content = [...after.content.slice(before.content.length), ...trailing.flatMap(message => message.content)];
  return content.length && content.every(block => ['text', 'image'].includes(block.type)) ? content : null;
}
// Agent Zoo appends wake/interrupt notices and steering text after the tool
// results that close a batch. Rebuilding for that relocates the SDK's startup
// reminders into older turns and invalidates the upstream prompt cache. Instead,
// append the trailing text/images verbatim to the last result of the parked batch.
// Host history is unchanged; only the live SDK mirror carries the folded form.
function foldTrailingContent(content, resultCount) {
  if (content.slice(0, resultCount).some(block => block.type !== 'tool_result')) return null;
  const trailing = content.slice(resultCount);
  const supported = trailing.every(block => block.type === 'text'
    ? Object.keys(block).every(key => ['type', 'text'].includes(key))
    : block.type === 'image' && Object.keys(block).every(key => ['type', 'source'].includes(key))
      && block.source?.type === 'base64'
      && Object.keys(block.source).every(key => ['type', 'media_type', 'data'].includes(key)));
  if (!trailing.length || !supported) return null;
  const results = clone(content.slice(0, resultCount));
  const last = results.at(-1);
  last.content = [...(last.content ?? []), ...clone(trailing)];
  return results;
}
// rollback: resume the SDK's own transcript just before the interrupted answer,
// so SDK and host histories agree and the cached request prefix is unchanged.
// reuse: keep the live process, which retains the SDK-private partial answer.
// rebuild: re-import host history, which relocates startup reminders (cache miss).
export const INTERRUPT_MODES = ['rollback', 'reuse', 'rebuild'];
/** Agent Zoo owns history and tools. SDK sessions are disposable mirrors only. */
export class BridgeManager {
  constructor({ queryFactory, stateRoot, idleMs = 300000,
    requestTimeoutMs = 0, historyImporter = importSession,
    processFactory = () => new QueryProcess(), env = process.env, systemPromptMode = 'append',
    interruptMode = 'rollback', interruptTimeoutMs = 500 } = {}) {
    if (!INTERRUPT_MODES.includes(interruptMode)) throw new TypeError('Invalid interrupt mode');
    if (!Number.isFinite(interruptTimeoutMs) || interruptTimeoutMs <= 0 || interruptTimeoutMs > 5000)
      throw new TypeError('Invalid interrupt timeout');
    this.interruptMode = interruptMode;
    this.interruptTimeoutMs = interruptTimeoutMs;
    if (!['append', 'custom'].includes(systemPromptMode)) throw new TypeError('Invalid system prompt mode');
    this.systemPromptMode = systemPromptMode;
    if (!Number.isFinite(idleMs) || idleMs <= 0
        || !Number.isFinite(requestTimeoutMs) || requestTimeoutMs < 0)
      throw new TypeError('Invalid bridge resource limits');
    this.queryFactory = queryFactory ?? (async options => (await import('@anthropic-ai/claude-agent-sdk')).query(options));
    this.historyImporter = historyImporter;
    this.processFactory = processFactory;
    this.env = env;
    this.stateRoot = stateRoot;
    this.idleMs = idleMs;
    this.requestTimeoutMs = requestTimeoutMs;
    this.sessions = new Map();
    this.closing = false;
    this.sweepTimer = setInterval(() => { this.sweep().catch(() => {}); }, Math.max(10, Math.min(idleMs, 10000)));
    this.sweepTimer.unref?.();
  }

  async handle(body, { sessionId, signal, onEvent, allowTransportHandoff = false } = {}) {
    if (this.closing) throw new BridgeError(503, 'The bridge is shutting down.');
    if (sessionId !== undefined && (typeof sessionId !== 'string' || !sessionId)) throw bad('Invalid session ID.');
    if (signal?.aborted) throw new BridgeError(499, 'The request was cancelled.');
    let messages, fingerprint;
    try {
      messages = canonicalMessages(body.messages);
      validateHistory(messages);
      if (!messages.length || messages.at(-1).role !== 'user') throw bad('The final message must be a user turn.');
      fingerprint = settingsKey(body);
      serverTools(body.tools ?? []);
      validateUserMessage(messages.at(-1));
      systemPrompt(body.system);
      thinkingOption(body.thinking);
    } catch (error) { throw bridgeError(error); }
    await this.sweep();
    if (this.closing) throw new BridgeError(503, 'The bridge is shutting down.');
    // Anonymous calls never share a scope, including with other anonymous calls.
    const key = sessionId ?? Symbol('anonymous');
    let slot = this.sessions.get(key);
    if (slot?.busy && allowTransportHandoff && slot.current?.signal) {
      const previous = slot.current;
      await waitForTransportHandoff(previous, signal);
      if (this.closing) throw new BridgeError(503, 'The bridge is shutting down.');
      if (signal?.aborted) throw new BridgeError(499, 'The request was cancelled.');
      // Ordinary completion is not a disconnect: concurrent healthy clients
      // still conflict, even if inference happens to finish during the grace.
      if (!previous.signal.aborted) throw new BridgeError(409, 'A request is already active for this session.');
      // Another contender may already own this slot, or removal may replace it.
      // No await between this recheck and claiming busy below.
      slot = this.sessions.get(key);
    }
    if (signal?.aborted) throw new BridgeError(499, 'The request was cancelled.');
    if (slot?.busy) throw new BridgeError(409, 'A request is already active for this session.');
    if (!slot) {
      slot = { key, busy: false, session: null, cwd: null, current: null, touched: Date.now(), closing: false };
      this.sessions.set(key, slot);
    }
    if (slot.closing) throw new BridgeError(409, 'This session is closing.');
    // Validate continuation before installing a sink or releasing any MCP waiter.
    let plan;
    try { plan = this.plan(slot.session, messages, fingerprint); }
    catch (error) { throw bridgeError(error); }
    slot.busy = true;
    const request = { ...deferred(), messages, onEvent, signal, settled: false, stream: null, complete: null, fallback: null,
      sdkUuids: new Set(), phase: 'setup' };
    slot.current = request;
    const abort = () => this.cancelRequest(slot, request);
    signal?.addEventListener('abort', abort, { once: true });
    // Off by default: a total deadline kills long but actively streaming answers.
    // The host's stream idle/progress timeouts own liveness; 0 disables this cap.
    const timer = this.requestTimeoutMs > 0 ? setTimeout(() => this.fail(slot, request,
      new BridgeError(504, 'The Claude SDK request timed out.')), this.requestTimeoutMs) : undefined;
    if (signal?.aborted) abort();
    // Sink installation above MUST precede deliver(): it can synchronously wake
    // MCP handlers and produce output for the new HTTP response.
    this.drive(slot, request, body, fingerprint, plan).catch(error => this.fail(slot, request,
      bridgeError(error, request.phase === 'query_factory' ? 'query_factory_failed'
        : request.phase === 'continuation' ? 'continuation_failed' : 'setup_failed')));
    try {
      return await request.promise;
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener('abort', abort);
      if (slot.current === request) slot.current = null;
      slot.busy = false;
      slot.touched = Date.now();
      if (sessionId === undefined) await this.remove(slot);
    }
  }

  plan(session, messages, fingerprint) {
    if (!session) return { kind: 'rebuild', reason: 'initial' };
    if (session.dead) return { kind: 'rebuild', reason: 'dead' };
    if (session.fingerprint !== fingerprint) return { kind: 'rebuild', reason: 'settings_changed' };
    if (!session.expected) return { kind: 'rebuild', reason: 'missing_history' };
    if (session.interruption) {
      const content = appendedFeedback(session.expected, messages);
      return content ? { kind: 'steer', content } : { kind: 'rebuild', reason: 'interrupted_history_changed' };
    }
    const prefix = messages.slice(0, session.expected.length);
    if (!same(prefix, session.expected)) return { kind: 'rebuild', reason: 'history_changed' };
    const suffix = messages.slice(session.expected.length);
    const pending = session.tools.pendingIds;
    if (pending.size) {
      if (!suffix.length || suffix.some(message => message.role !== 'user' || !Array.isArray(message.content))) {
        throw bad('A complete tool-result batch is required.');
      }
      const content = suffix.flatMap(message => message.content);
      const results = content.filter(b => b.type === 'tool_result');
      const ids = new Set(results.map(b => b.tool_use_id));
      if (ids.size !== results.length || ids.size !== pending.size || [...ids].some(id => !pending.has(id))) {
        throw bad('Tool results must match the complete pending model batch.');
      }
      // Never queue steering behind or ahead of a parked MCP batch: deliver it
      // inside the batch, or rebuild when its content cannot ride in a result.
      if (results.length !== content.length) {
        const folded = foldTrailingContent(content, results.length);
        return folded ? { kind: 'results', results: folded } : { kind: 'rebuild', reason: 'mixed_results' };
      }
      return { kind: 'results', results };
    }
    if (session.ready && suffix.length === 1 && suffix[0].role === 'user'
        && !(Array.isArray(suffix[0].content) && suffix[0].content.some(b => b.type === 'tool_result'))) {
      return { kind: 'user', content: suffix[0].content };
    }
    return { kind: 'rebuild', reason: 'continuation_shape' };
  }

  async drive(slot, request, body, fingerprint, plan) {
    if (request.settled || this.closing || slot.closing) return;
    request.action = plan.kind;
    request.rebuildReason = plan.kind === 'rebuild' ? plan.reason : 'none';
    if (plan.kind === 'steer') {
      const session = slot.session;
      request.reusing = true;
      request.phase = 'interrupt';
      const reusable = await session.interruption.promise;
      if (request.settled || this.closing || slot.closing) return;
      if (!reusable || session.dead || session.unsettledTool
          || session.tools.pendingIds.size || session.tools.hasWaitingCalls) {
        request.reusing = false;
        return this.drive(slot, request, body, fingerprint, { kind: 'rebuild', reason: 'interrupt_not_reusable' });
      }
      const { messageIds, turnPrompt } = session.interruption;
      session.interruption = null;
      if (this.interruptMode === 'rollback') {
        request.reusing = false;
        return this.drive(slot, request, body, fingerprint, { kind: 'rebuild', reason: 'interrupt_rollback',
          rollback: { content: plan.content, messageIds, turnPrompt } });
      }
      session.ready = false;
      session.searchUses = 0;
      request.sent = true;
      request.phase = 'continuation';
      session.prompts.push(plan.content, session.turnPrompt = randomUUID());
      request.phase = 'iterator';
      return;
    }
    if (plan.kind === 'rebuild') {
      request.phase = 'dispose';
      const previous = slot.session;
      // A rollback resumes the previous mirror's own transcript: keep its files
      // out of that process's cleanup, then hand them to the replacement.
      const inherited = plan.rollback && previous ? previous.files : null;
      if (inherited) previous.files = new Set();
      try {
        await this.dispose(previous);
        if (previous && (!previous.process.didExit || !previous.setupFinished))
          throw new BridgeError(503, 'The previous SDK process is still closing.');
        if (previous) await this.cleanupFiles(previous);
      } catch (error) {
        // Still owned by a live process: its eventual exit cleanup removes them.
        for (const path of inherited ?? []) previous.files.add(path);
        throw error;
      }
      // Read only after exit, so the interrupted turn is fully flushed and no
      // queued input can be appended behind the validated resume point.
      const resume = inherited ? await this.rollbackTarget(previous, plan.rollback) : null;
      if (plan.rollback) request.rebuildReason = resume ? 'interrupt_rollback' : 'rollback_unavailable';
      if (inherited && (!resume || request.settled || this.closing || slot.closing)) await removeFiles(inherited);
      if (request.settled || this.closing || slot.closing) return;
      request.phase = 'setup';
      const { host: hostTools, webSearch } = serverTools(body.tools ?? []);
      const session = { tools: new ToolBridge(hostTools), prompts: promptStream(), process: this.processFactory(),
        controller: new AbortController(), query: null, cwd: null, imported: null, expected: null, fingerprint,
        files: new Set(resume ? inherited : []), sdkSessionId: null, turnPrompt: null,
        // SDK-run searches: committed once their call reached a finished host response.
        webSearch, searches: new Map(), searchResults: [], searchUses: 0,
        ready: false, dead: false, emitted: new Set(), cleanupPromise: null, pump: null, settingUp: true, setupFinished: false, setup: deferred() };
      slot.session = session;
      try {
        request.phase = 'workspace';
        // SDK-generated environment context includes cwd: keep it stable for this host
        // slot, but private from every other named or anonymous slot.
        slot.cwd ??= await createWorkspace(this.stateRoot);
        session.cwd = slot.cwd;
        if (session.dead || request.settled) return;
        // A resumed SDK transcript already holds the history; send only the feedback.
        // Otherwise host search records are lowered to the SDK's WebSearch form.
        const lowered = resume ? { messages: [], searchIds: new Set() } : lowerServerTools(request.messages);
        const { prefix, prompt } = resume ? { prefix: [], prompt: plan.rollback.content } : rebuildInput(lowered.messages);
        // Historical tools are names only: never advertise or execute them.
        const definitions = [...hostTools];
        const names = new Set(definitions.map(tool => tool.name));
        for (const message of prefix) for (const block of message.content) {
          if (block.type === 'tool_use' && !lowered.searchIds.has(block.id) && !names.has(block.name)) {
            if (block.name === SDK_WEB_SEARCH) throw bad('A historical host tool name conflicts with server web search.');
            names.add(block.name);
            definitions.push({ name: block.name, input_schema: { type: 'object' } });
          }
        }
        // Use the same deterministic aliases and collision checks as live tools.
        const historyTools = new ToolBridge(definitions);
        const historyNames = new Map([...names].map(name => [name, historyTools.toSdkName(name)]));
        historyTools.close();
        if (prefix.length) {
          request.phase = 'history_import';
          session.imported = await this.historyImporter({ messages: prefix, cwd: session.cwd, model: body.model,
            toolNameToSdk: name => name === SDK_WEB_SEARCH ? SDK_WEB_SEARCH : historyNames.get(name) });
          if (session.imported?.path) session.files.add(session.imported.path);
        }
        if (session.dead || request.settled) return;
        // The CLI appends a synthetic "No response requested." assistant when
        // loading a user-ended transcript. Anchor at the actual final result so
        // that loader-only placeholder never reaches inference. No host record
        // is removed or rewritten; the SDK prompt starts immediately after it.
        request.phase = 'resume_setup';
        let resumeSessionAt;
        if (prefix.at(-1)?.role === 'user' && session.imported?.path) {
          const records = (await readFile(session.imported.path, 'utf8')).trim().split('\n').map(line => JSON.parse(line));
          resumeSessionAt = records.findLast(record => record.type === 'user')?.uuid;
          if (!resumeSessionAt) throw upstream('resume_anchor_missing');
        }
        if (session.dead || request.settled) return;
        const options = {
          cwd: session.cwd,
          model: body.model,
          systemPrompt: this.systemPromptMode === 'custom' ? systemPrompt(body.system)
            : { type: 'preset', preset: 'claude_code', append: [systemPrompt(body.system)].flat().join('\n\n') },
          // WebSearch is the only built-in tool, and only when the host asked for it.
          tools: session.webSearch ? [SDK_WEB_SEARCH] : [],
          allowedTools: hostTools.map(tool => session.tools.toSdkName(tool.name)),
          mcpServers: { azo: session.tools.sdkServer },
          strictMcpConfig: true,
          settingSources: [],
          settings: clone(isolatedSettings),
          plugins: [],
          skills: [],
          hooks: {},
          permissionMode: 'default',
          canUseTool: async (name, input) => {
            // Never in allowedTools, so every search passes the host's limits here.
            if (name === SDK_WEB_SEARCH && !session.dead) {
              const decision = searchPermission(session.webSearch, input, session.searchUses);
              if (decision.behavior === 'allow') session.searchUses++;
              return decision;
            }
            try {
              const host = session.tools.fromSdkName(name);
              if (!host || session.dead) throw new Error('denied');
              return { behavior: 'allow', updatedInput: input };
            } catch { return { behavior: 'deny', message: 'Only Agent Zoo bridge tools are permitted.' }; }
          },
          includePartialMessages: true,
          // Rollback needs the SDK's own transcript; the slot removes it on cleanup.
          persistSession: this.interruptMode === 'rollback',
          abortController: session.controller,
          spawnClaudeCodeProcess: session.process.spawn,
          env: childEnvironment(this.env, { CLAUDE_CODE_MAX_OUTPUT_TOKENS: String(body.max_tokens), ENABLE_TOOL_SEARCH: 'false' }),
          ...(resume ? { resume: resume.sessionId, resumeSessionAt: resume.resumeSessionAt }
            : session.imported ? { resume: session.imported.sessionId } : { sessionId: randomUUID() }),
          ...(resumeSessionAt ? { resumeSessionAt } : {}),
          ...(body.thinking ? { thinking: thinkingOption(body.thinking) } : {}),
          ...(body.output_config?.effort !== undefined ? { effort: body.output_config.effort } : {}),
        };
        this.adoptTranscript(session, options.resume ?? options.sessionId);
        request.sent = true;
        session.prompts.push(prompt, session.turnPrompt = randomUUID());
        request.phase = 'query_factory';
        session.query = await this.queryFactory({ prompt: session.prompts, options });
        if (session.dead || request.settled) {
          try { Promise.resolve(session.query?.close?.()).catch(() => {}); } catch { /* already cancelled; closing is best effort */ }
          return;
        }
        request.phase = 'iterator';
        session.pump = this.pump(slot, session).catch(() => {});
      } finally {
        // A cancelled setup can still create an import or return a late query.
        // Do not reuse/remove its slot workspace until all setup work has ended.
        session.settingUp = false;
        try {
          if (session.dead || request.settled && !session.query) await this.cleanupFiles(session);
        } finally { session.setupFinished = true; session.setup.resolve(); }
      }
    } else {
      const session = slot.session;
      request.phase = 'continuation';
      if (session.dead) throw upstream('session_dead');
      session.ready = false;
      session.searchUses = 0; // max_uses applies to each host response
      request.sent = true;
      if (plan.kind === 'results') {
        session.tools.deliver(plan.results);
        session.unsettledTool = false;
      } else session.prompts.push(plan.content, session.turnPrompt = randomUUID());
      request.phase = 'iterator';
    }
  }

  async pump(slot, session) {
    const failed = (error, phase) => {
      if (session.interruption) session.interruption.failure.resolve(false);
      else if (!session.dead && slot.current && !slot.current.settled) this.fail(slot, slot.current,
        bridgeError(error, phase === 'iterator' ? 'iterator_failed' : 'sdk_message_invalid'), phase);
    };
    try {
      for await (const message of session.query) {
        if (session.dead) break;
        // Fail and start disposal BEFORE leaving the loop: exiting a for-await
        // awaits the SDK iterator's return(), during which the CLI keeps running.
        try { this.consume(slot, session, message); }
        catch (error) { failed(error, 'consume'); break; }
      }
      if (!session.dead) {
        // Exhaustion is NOT an ordinary SDK result boundary. A later request
        // rebuilds rather than writing into an already exhausted prompt stream.
        session.dead = true;
        if (session.interruption) session.interruption.failure.resolve(false);
        else if (slot.current && !slot.current.settled) this.fail(slot, slot.current, upstream('iterator_exhausted'), 'iterator');
      }
    } catch (error) {
      failed(error, 'iterator');
    } finally {
      await this.dispose(session);
    }
  }

  consume(slot, session, sdk) {
    // Status only. The CLI's 30-second tool_heartbeat names the still-running call
    // as parent_tool_use_id: that is an ordinary host call parked while the rest of
    // its batch streams, not a subagent.
    if (sdk.type === 'tool_progress') return;
    if (sdk.parent_tool_use_id != null) throw upstream('unexpected_subagent', // subagents are never enabled
      `type=${sdk.type}${sdk.subtype ? ` subtype=${sdk.subtype}` : ''} parent_tool_use_id=${sdk.parent_tool_use_id}`);
    if (sdk.session_id !== session.sdkSessionId && isSessionId(sdk.session_id)) this.adoptTranscript(session, sdk.session_id);
    if (sdk.type === 'system' && sdk.subtype === 'compact_boundary') throw upstream('unexpected_compaction');
    // Context for a later failure: which plan limit rejected us, and the
    // provider status behind SDK-internal retries.
    if (sdk.type === 'rate_limit_event') { session.rateLimit = sdk.rate_limit_info; return; }
    if (sdk.type === 'system' && sdk.subtype === 'api_retry') { session.lastRetry = sdk; return; }
    // Only host tools park the SDK; its own searches and its answers to
    // unavailable tools settle inside the turn. After an interrupt, any late call
    // means the old turn is still running, so its process cannot be reused.
    const hostCall = block => block?.type === 'tool_use' && block.name !== SDK_WEB_SEARCH
      && (session.interruption || session.tools.hasSdkName(block.name));
    const hasTool = sdk.type === 'assistant' && !session.emitted.has(sdk.message?.id)
      && sdk.message?.content?.some(hostCall)
      || sdk.type === 'stream_event' && sdk.event?.type === 'content_block_start' && hostCall(sdk.event.content_block);
    if (hasTool) session.unsettledTool = true;
    if (sdk.type === 'user') { this.collectSearchResults(slot, session, sdk); return; }
    if (session.interruption) {
      if (hasTool || session.tools.hasWaitingCalls) session.interruption.failure.resolve(false);
      if (sdk.type === 'result') session.interruption.result.resolve(sdk);
      return; // Old partials/results must never enter the replacement sink.
    }
    if (sdk.type === 'assistant' && sdk.aborted) throw upstream('assistant_aborted', sdk);
    if (sdk.type === 'result') {
      if (sdk.subtype !== 'success' || sdk.is_error) throw providerFailure('result_error', sdk, session);
      session.ready = true;
      session.lastRetry = undefined;
      const request = slot.current;
      if (request && !request.settled) {
        if (request.stream?.stopped && request.stream.message.stop_reason === 'max_tokens') {
          throw streamFailure('output_limit_without_continuation', request, undefined, session);
        }
        if (request.stream?.stopped && sdkOnly(request.stream.message)) {
          const search = request.stream.message.content.some(block => block.type === 'server_tool_use');
          throw streamFailure(search ? 'web_search_without_continuation' : 'unavailable_tool_without_continuation',
            request, undefined, session);
        }
        const message = request.complete ?? request.fallback;
        if (!message) throw upstream('result_without_message');
        if (!message.stop_reason) message.stop_reason = sdk.stop_reason ?? 'end_turn';
        this.finish(slot, session, request, message, request.stream?.events);
      }
      return;
    }
    const request = slot.current;
    if (!request || request.settled) return;
    if (sdk.type === 'assistant') {
      if (sdk.error) throw providerFailure('assistant_error', sdk, session);
      if (sdk.message?.model === '<synthetic>') throw providerFailure('synthetic_assistant', sdk, session);
      if (session.emitted.has(sdk.message?.id)) return; // late per-block SDK echo
      if (sdk.uuid && request.sdkUuids.has(sdk.uuid)) return;
      if (sdk.uuid) request.sdkUuids.add(sdk.uuid);
      // Same-ID envelopes echo streamed blocks. Same-ID echoes from every retained
      // segment may arrive late; none is a replacement of the logical response.
      if (request.stream?.message.id === sdk.message?.id || request.continuation?.ids.has(sdk.message?.id)) return;
      if (request.stream) {
        if (request.stream.published || request.continuation) {
          throw streamFailure('published_message_replaced', request, sdk.message?.id, session);
        }
        request.stream = null;
        request.complete = null;
      }
      const message = clone(sdk.message);
      message.content = message.content.map(block => this.mapBlock(session, request, block)).filter(Boolean);
      if (request.fallback && request.fallback.id !== message.id) request.fallback = null;
      if (request.fallback) {
        request.fallback.content.push(...message.content);
        request.fallback.usage = message.usage ?? request.fallback.usage;
        request.fallback.stop_reason = message.stop_reason ?? request.fallback.stop_reason;
      } else request.fallback = message;
      if (request.fallback.content.some(block => block.type === 'tool_use')) {
        // Per-block fallbacks with no terminal boundary cannot prove a parallel
        // batch complete. Fail now: waiting for result deadlocks parked MCP calls.
        if (!request.fallback.stop_reason) throw new BridgeError(502,
          'The Claude SDK returned a tool batch without a completion boundary. [reason=tool_batch_boundary_missing]');
        this.finish(slot, session, request, request.fallback);
      }
      return;
    }
    if (sdk.type !== 'stream_event') return;
    const event = clone(sdk.event);
    if (event.type === 'ping') return;
    if (request.ignoredStream) {
      if (event.type === 'message_stop') request.ignoredStream = false;
      return;
    }
    if (event.type === 'message_start') {
      const previous = request.stream;
      if (session.emitted.has(event.message.id) || request.continuation?.ids.has(event.message.id)
          || previous?.stopped && previous.message.id === event.message.id) {
        request.ignoredStream = true;
        return;
      }
      const message = clone(event.message);
      if (message.content?.length) throw upstream('nonempty_message_start');
      // A segment whose calls the SDK settled itself (searches, unavailable tools)
      // continues in the SDK's next message, like max_tokens.
      if (previous?.stopped && (previous.message.stop_reason === 'max_tokens' || sdkOnly(previous.message))) {
        if (message.model !== previous.message.model) {
          throw streamFailure('continuation_model_changed', request, message.id, session);
        }
        retainContinuation(request);
      } else if (previous?.published || request.continuation) {
        throw streamFailure('published_stream_replaced', request, message.id, session);
      }
      request.complete = null;
      message.content = [];
      event.message = clone(message);
      request.fallback = null;
      request.stream = { message, events: request.continuation ? previous.events : [event], blocks: new Map(), hidden: new Set(), stopped: false,
        published: request.continuation?.published ?? false, offset: request.continuation?.content.length ?? 0, prefix: [] };
      // Results the SDK produced after the previous host response (a mixed batch)
      // lead this response, right before the model's first new block.
      for (const item of session.searchResults.filter(item => !item.shownIn)) {
        const index = request.stream.offset + request.stream.prefix.length;
        request.stream.prefix.push(item.block);
        request.stream.events.push(...blockEvents(item.block, index));
        item.shownIn = request;
      }
      return;
    }
    const stream = request.stream;
    if (!stream || stream.stopped) throw upstream('stream_boundary_invalid');
    if (event.type === 'content_block_start') {
      if (!Number.isInteger(event.index) || event.index < 0 || stream.blocks.has(event.index)) throw upstream('block_index_invalid');
      const block = this.mapBlock(session, request, event.content_block);
      stream.blocks.set(event.index, { block, json: '', closed: false });
      if (!block) {
        // A hidden block has no host index; later blocks close the gap. Blocks
        // that already started above it were published and cannot be renumbered.
        if ([...stream.blocks.keys()].some(index => index > event.index)) throw upstream('block_index_invalid');
        stream.hidden.add(event.index);
        return;
      }
      event.content_block = clone(block);
    } else if (event.type === 'content_block_delta') {
      const entry = stream.blocks.get(event.index);
      if (!entry || entry.closed) throw upstream('block_boundary_invalid');
      if (!entry.block) return;
      const block = entry.block, delta = event.delta;
      if (delta.type === 'text_delta' && block.type === 'text') block.text += delta.text;
      else if (delta.type === 'thinking_delta' && block.type === 'thinking') block.thinking += delta.thinking;
      else if (delta.type === 'signature_delta' && block.type === 'thinking') block.signature = (block.signature ?? '') + delta.signature;
      else if (delta.type === 'input_json_delta' && ['tool_use', 'server_tool_use'].includes(block.type)) entry.json += delta.partial_json;
      else if (delta.type === 'citations_delta' && block.type === 'text') (block.citations ??= []).push(delta.citation);
      else throw upstream('unsupported_delta');
    } else if (event.type === 'content_block_stop') {
      const entry = stream.blocks.get(event.index);
      if (!entry || entry.closed) throw upstream('block_boundary_invalid');
      entry.closed = true;
      if (!entry.block) return;
      if (entry.json) entry.block.input = JSON.parse(entry.json);
    } else if (event.type === 'message_delta') {
      Object.assign(stream.message, event.delta);
      stream.message.usage = { ...stream.message.usage, ...event.usage };
      // This may be an intermediate max_tokens boundary. Only publish terminal
      // metadata after the segment is validated and known to be the final one.
      stream.delta = { ...event, delta: { ...stream.delta?.delta, ...event.delta } };
      return;
    } else if (event.type === 'message_stop') {
      const entries = [...stream.blocks.entries()].sort(([a], [b]) => a - b);
      if (entries.some(([index, entry], i) => index !== i || !entry.closed)) throw upstream('incomplete_stream_blocks');
      stream.message.content = [...stream.prefix, ...entries.map(([, entry]) => entry.block).filter(Boolean)];
      stream.stopped = true;
      if (stream.message.stop_reason === 'max_tokens') {
        // The SDK may already be waiting on these calls: never dispatch a
        // truncated batch, or wait indefinitely for a continuation it cannot send.
        if (stream.message.content.some(block => block.type === 'tool_use')) {
          throw streamFailure('max_tokens_tool_batch_incomplete', request, undefined, session);
        }
        if (stream.message.content.some(block => block.type === 'thinking' && !block.signature)) {
          throw streamFailure('max_tokens_thinking_incomplete', request, undefined, session);
        }
        request.complete = null;
        return;
      }
      if (sdkOnly(stream.message)) {
        // Withhold the boundary: results, then the SDK's next message, follow.
        request.complete = null;
        this.publish(slot, request, this.appendSearchResults(session, request));
        return;
      }
      // Results that arrived while this message streamed belong at its end.
      stream.events.push(...this.appendSearchResults(session, request));
      request.complete = continuationMessage(request, stream.message);
      if (stream.delta) stream.events.push({ ...stream.delta, usage: clone(request.complete.usage) });
    } else throw upstream('unsupported_event');
    // Validate local SDK indexes above; only transport indexes are offset.
    if ('index' in event) {
      const hidden = [...stream.hidden].filter(index => index < event.index).length;
      event.index += stream.offset + stream.prefix.length - hidden;
    }
    stream.events.push(event);
    if (stream.stopped && stream.message.stop_reason === 'tool_use') {
      this.finish(slot, session, request, request.complete, stream.events);
    } else if (!stream.stopped) {
      this.publish(slot, request, stream.events.splice(0));
      stream.published ||= Boolean(request.onEvent);
    }
  }

  // The SDK names its transcript by session ID; a resume may continue or fork it.
  adoptTranscript(session, sessionId) {
    session.sdkSessionId = sessionId;
    if (this.interruptMode === 'rollback' && isSessionId(sessionId) && session.cwd) {
      session.files.add(transcriptPath(sessionId, session.cwd));
    }
  }

  async rollbackTarget(session, { messageIds, turnPrompt }) {
    if (!isSessionId(session?.sdkSessionId) || !turnPrompt || !session.cwd) return null;
    try {
      const data = await readFile(transcriptPath(session.sdkSessionId, session.cwd), 'utf8');
      const records = data.trim().split('\n').map(line => JSON.parse(line));
      const point = rollbackPoint(records, { messageIds: new Set(messageIds), turnPrompt });
      return point && { sessionId: session.sdkSessionId, resumeSessionAt: point.resumeSessionAt };
    } catch { return null; } // Missing or unreadable transcript: rebuild from host history instead.
  }

  /** The host form of one SDK block, or null for a block the host never sees. */
  mapBlock(session, request, source) {
    const block = clone(source);
    if (!['text', 'thinking', 'redacted_thinking', 'tool_use'].includes(block?.type)) throw upstream('unsupported_block');
    if (block.type === 'tool_use' && block.name === SDK_WEB_SEARCH && session.webSearch) {
      // The SDK runs this search itself: report it as the native server tool.
      if (!session.searches.has(block.id)) session.searches.set(block.id, { committed: false, result: false });
      return { type: 'server_tool_use', id: block.id, name: HOST_WEB_SEARCH, input: block.input ?? {} };
    }
    if (block.type === 'tool_use' && !session.tools.hasSdkName(block.name)) {
      // A tool the model invented or a disabled built-in. The CLI answers it with
      // "No such tool available" and the model continues; the host has nothing to run.
      const calls = request.unavailableCalls ??= new Map();
      calls.set(block.id, block.name);
      if (calls.size > MAX_UNAVAILABLE_CALLS) {
        const names = [...new Set(calls.values())].map(toolLabel).join(', ');
        throw upstream('unavailable_tool_loop', `The model called unavailable tools ${calls.size} times in one response: ${names}.`);
      }
      return null;
    }
    if (block.type === 'tool_use') block.name = session.tools.fromSdkName(block.name);
    return block;
  }

  // The SDK reports each finished search as a user tool_result record.
  collectSearchResults(slot, session, sdk) {
    const content = Array.isArray(sdk.message?.content) ? sdk.message.content : [];
    const found = content.filter(block => block?.type === 'tool_result' && session.searches.has(block.tool_use_id));
    for (const result of found) {
      const record = session.searches.get(result.tool_use_id);
      if (record.result) continue; // replayed record
      record.result = true;
      session.searchResults.push({ block: searchResultBlock(result, found.length === 1 ? sdk.tool_use_result : undefined), shownIn: null });
    }
    if (!found.length || session.interruption) return;
    const request = slot.current;
    if (request && !request.settled && request.stream?.stopped && sdkOnly(request.stream.message)) {
      this.publish(slot, request, this.appendSearchResults(session, request));
    }
  }

  // Append pending results to the current (stopped) segment; returns their events.
  appendSearchResults(session, request) {
    const stream = request.stream, events = [];
    for (const item of session.searchResults.filter(item => !item.shownIn)) {
      events.push(...blockEvents(item.block, stream.offset + stream.message.content.length));
      stream.message.content.push(item.block);
      item.shownIn = request;
    }
    return events;
  }

  // A canceled response loses its own searches; results for calls the host
  // already recorded stay pending for the next response.
  abandonSearchResults(session, request) {
    if (!session?.searchResults) return;
    session.searchResults = session.searchResults.filter(item => session.searches.get(item.block.tool_use_id)?.committed);
    for (const item of session.searchResults) if (item.shownIn === request) item.shownIn = null;
  }

  finish(slot, session, request, source, rawEvents) {
    if (request.settled || session.emitted.has(source.id)) return;
    const message = clone(source);
    if (typeof message.id !== 'string' || !Array.isArray(message.content)) throw upstream('invalid_message');
    message.type = 'message';
    message.role = 'assistant';
    message.stop_sequence ??= null;
    message.usage ??= { input_tokens: 0, output_tokens: 0 };
    const calls = message.content.filter(block => block.type === 'tool_use');
    if (calls.length && message.stop_reason !== 'tool_use' || !calls.length && message.stop_reason === 'tool_use') throw upstream('tool_stop_mismatch');
    if (calls.length) {
      session.tools.expect(message.content);
      session.ready = false;
    }
    session.expected = canonicalMessages([...request.messages, { role: 'assistant', content: message.content }]);
    // Searches in this response are now host history; their shown results are done.
    for (const block of message.content) {
      if (block.type === 'server_tool_use' && session.searches?.has(block.id)) session.searches.get(block.id).committed = true;
    }
    if (session.searchResults) session.searchResults = session.searchResults.filter(item => item.shownIn !== request);
    for (const id of [message.id, ...(request.continuation?.ids ?? []), request.stream?.message.id].filter(Boolean)) {
      session.emitted.add(id);
      if (session.emitted.size > 512) session.emitted.delete(session.emitted.values().next().value);
    }
    // Progress already flowed; expect(fullBatch) MUST precede message_stop,
    // the boundary at which the host collector may execute the tools.
    const events = rawEvents ?? messageEvents(message);
    this.publish(slot, request, events);
    if (request.settled) return;
    request.settled = true;
    request.resolve(message);
  }

  publish(slot, request, events) {
    for (const event of events) {
      if (request.settled) return;
      let callback;
      try { callback = request.onEvent?.(clone(event)); }
      catch (error) { throw bridgeError(error, 'sink_failed'); }
      if (callback && typeof callback.then === 'function') callback.catch(error => this.fail(slot, request, bridgeError(error, 'sink_failed'), 'publish'));
    }
  }

  cancelRequest(slot, request) {
    if (request.settled) return;
    const session = slot.session;
    const error = new BridgeError(499, 'The request was cancelled.');
    // A second feedback update can cancel a replacement still waiting for the
    // same interrupt boundary. Nothing new has been sent to Claude in that case.
    if (request.reusing && !request.sent && session?.interruption && !session.dead) {
      request.settled = true;
      request.reject(error);
      return;
    }
    if (this.interruptMode === 'rebuild' || !request.sent || !session || session.dead
        || session.interruption || session.ready || session.unsettledTool
        || session.tools.pendingIds.size || session.tools.hasWaitingCalls
        || typeof session.query?.interrupt !== 'function') {
      this.fail(slot, request, error);
      return;
    }
    request.settled = true;
    request.reject(error);
    this.abandonSearchResults(session, request);
    // This is the HOST prefix, not the SDK's private retained partial answer.
    session.expected = request.messages;
    session.ready = false;
    // Rollback discards exactly this undelivered answer from the SDK transcript.
    const messageIds = [request.stream?.message.id, ...(request.continuation?.ids ?? []), request.fallback?.id].filter(Boolean);
    const interruption = { result: deferred(), failure: deferred(), messageIds, turnPrompt: session.turnPrompt };
    session.interruption = interruption;
    interruption.promise = (async () => {
      let timer, reusable = false;
      try {
        reusable = await Promise.race([
          Promise.all([session.query.interrupt(), interruption.result.promise]).then(([receipt, result]) =>
            Array.isArray(receipt?.still_queued) && receipt.still_queued.length === 0
            && ['success', 'error_during_execution'].includes(result.subtype)),
          interruption.failure.promise,
          new Promise(resolve => { timer = setTimeout(() => resolve(false), this.interruptTimeoutMs); }),
        ]);
      } catch { /* The cancelled request is settled; fall back to rebuilding. */ }
      finally { clearTimeout(timer); }
      reusable = Boolean(reusable && !session.dead && !session.unsettledTool
        && !session.tools.pendingIds.size && !session.tools.hasWaitingCalls);
      if (!reusable) await this.dispose(session);
      else session.ready = true;
      return reusable;
    })();
  }
  fail(slot, request, error, phase = request.phase) {
    if (request.settled) return;
    request.settled = true;
    const failure = bridgeError(error);
    if (failure.statusCode >= 500 || failure.providerMessage !== undefined) {
      // Snapshot before disposal; neither an old closing process nor sink failure
      // should acquire unrelated process/shutdown diagnostics.
      const process = !slot.session?.cleanupPromise && !['dispose', 'publish'].includes(phase)
        && failure.reason !== 'sink_failed' ? slot.session?.process : undefined;
      const exitCode = ['query_factory', 'iterator', 'consume'].includes(phase) ? process?.child?.exitCode : undefined;
      const exit = Number.isInteger(exitCode) && exitCode >= 0 && exitCode <= 255 ? ` exit=${exitCode}` : '';
      const context = ` [phase=${phase} action=${request.action} rebuild=${request.rebuildReason}${exit}]`
        + (process?.stderr ? `\nProcess stderr:\n${process.stderr}` : '')
        + (process?.spawnError ? `\nProcess spawn error:\n${errorText(process.spawnError)}` : '');
      failure.message += context;
      failure.publicMessage += context;
    }
    request.reject(failure);
    this.dispose(slot.session).catch(() => {});
  }

  async cleanupFiles(session) {
    // A stubborn process must not lose the transcript underneath its live fd.
    if (!session.process.didExit || session.settingUp) return;
    // Each owned file is deleted once: late cleanup must not unlink a newer import
    // that happens to use the same path in the slot-owned workspace.
    const paths = [...session.files];
    session.files.clear();
    session.filesCleanupPromise = Promise.all([session.filesCleanupPromise, removeFiles(paths)]);
    await session.filesCleanupPromise;
  }

  dispose(session) {
    if (!session) return Promise.resolve();
    if (session.cleanupPromise) return session.cleanupPromise;
    session.dead = true;
    session.interruption?.failure.resolve(false);
    // Graceful CLI cancellation can synthesize interrupted MCP results and run
    // inference before exiting. A parked mirror has nothing to flush: terminate
    // it before invoking SDK close/abort or releasing any host callback.
    if ((session.unsettledTool || session.tools.pendingIds.size || session.tools.hasWaitingCalls) && !session.process.didExit) {
      try { session.process.child?.kill('SIGKILL'); } catch { /* bounded shutdown below still owns exit */ }
    }
    session.prompts.end();
    session.cleanupPromise = (async () => {
      const exited = await session.process.shutdown(session.query, session.controller);
      // Releasing MCP waiters while the child is alive can trigger another model
      // request with fabricated shutdown errors. Only settle after actual exit,
      // including a late exit after bounded shutdown has timed out.
      const finish = async () => {
        session.tools.close('Bridge session closed.');
        await this.cleanupFiles(session);
      };
      if (exited) await finish();
      else session.process.exited.then(finish).catch(() => {});
    })();
    return session.cleanupPromise;
  }

  async remove(slot) {
    if (slot.removing) return slot.removing;
    slot.closing = true;
    slot.removing = (async () => {
      if (slot.current && !slot.current.settled) this.fail(slot, slot.current, new BridgeError(499, 'The session was cancelled.'));
      const session = slot.session;
      const forget = async () => {
        if (session) await this.cleanupFiles(session);
        if (slot.cwd) await rm(slot.cwd, { recursive: true, force: true }).catch(() => {});
        if (this.sessions.get(slot.key) === slot) this.sessions.delete(slot.key);
      };
      await this.dispose(session);
      if (!session || session.process.didExit && session.setupFinished) {
        if (session) await session.setup.promise;
        await forget();
      } else {
        // Bounded shutdown must not wait for a stuck child or setup. Keep its
        // closing slot reserved, then release it only after both have settled.
        Promise.all([session.process.exited, session.setup.promise]).then(forget).catch(() => {});
      }
    })();
    return slot.removing;
  }

  async sweep() {
    const now = Date.now();
    // A dead mirror is replaceable, not an eviction: retain its host slot's cwd
    // until idle expiry, explicit cancellation, anonymous completion, or shutdown.
    await Promise.allSettled([...this.sessions.values()].filter(slot => !slot.busy && now - slot.touched >= this.idleMs)
      .map(slot => this.remove(slot)));
  }

  async cancel(sessionId) {
    const slot = this.sessions.get(sessionId);
    if (slot) await this.remove(slot);
  }

  async close() {
    this.closing = true;
    clearInterval(this.sweepTimer);
    await Promise.all([...this.sessions.values()].map(slot => this.remove(slot)));
  }
}

function removeFiles(paths) {
  return Promise.all([...paths].map(path => rm(path, { force: true }).catch(() => {})));
}

// The SDK settled every call in this message itself (searches it ran, tools it
// does not have); the host has nothing to execute.
function sdkOnly(message) {
  return message.stop_reason === 'tool_use' && !message.content.some(block => block.type === 'tool_use');
}

function blockEvents(block, index) {
  return [{ type: 'content_block_start', index, content_block: clone(block) }, { type: 'content_block_stop', index }];
}

function messageEvents(message) {
  const events = [{ type: 'message_start', message: { ...clone(message), content: [], stop_reason: null, stop_sequence: null, usage: { ...message.usage, output_tokens: 0 } } }];
  message.content.forEach((block, index) => events.push(...blockEvents(block, index)));
  events.push({ type: 'message_delta', delta: { stop_reason: message.stop_reason, stop_sequence: message.stop_sequence },
    usage: { output_tokens: message.usage.output_tokens } }, { type: 'message_stop' });
  return events;
}

function validateUserMessage(message) {
  if (Object.keys(message).some(k => !['role', 'content'].includes(k))) throw bad('Unsupported user message fields.');
  const check = (block, nested = false) => {
    if (block.type === 'text' && typeof block.text === 'string') return;
    if (block.type === 'image' && block.source && (
      block.source.type === 'base64' && typeof block.source.data === 'string' && typeof block.source.media_type === 'string'
      || block.source.type === 'url' && typeof block.source.url === 'string')) return;
    if (block.type === 'tool_result' && !nested && typeof block.tool_use_id === 'string'
        && (block.is_error === undefined || typeof block.is_error === 'boolean')) {
      for (const child of block.content ?? []) check(child, true);
      return;
    }
    throw bad('Unsupported user content block.');
  };
  for (const block of message.content) check(block);
}
