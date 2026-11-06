import { randomUUID } from 'node:crypto';
import { readFile, rm } from 'node:fs/promises';
import { createWorkspace } from './workspace.mjs';
import { ToolBridge } from './tools.mjs';
import { canonicalMessages, digest, importSession, requestFingerprint, validateHistory } from './history.mjs';
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
    push(content) {
      if (ended) throw upstream('prompt_stream_closed');
      // Host messages are already composed: never expand mentions/commands or
      // attach CLI-local reminders to them.
      queue.push({ type: 'user', message: { role: 'user', content: clone(content) }, parent_tool_use_id: null, client_composed: true });
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
/** Agent Zoo owns history and tools. SDK sessions are disposable mirrors only. */
export class BridgeManager {
  constructor({ queryFactory, stateRoot, idleMs = 300000,
    requestTimeoutMs = 0, maxSessions = 16, historyImporter = importSession,
    processFactory = () => new QueryProcess(), env = process.env, systemPromptMode = 'append',
    interruptMode = 'reuse', interruptTimeoutMs = 500 } = {}) {
    if (!['reuse', 'rebuild'].includes(interruptMode)) throw new TypeError('Invalid interrupt mode');
    if (!Number.isFinite(interruptTimeoutMs) || interruptTimeoutMs <= 0 || interruptTimeoutMs > 5000)
      throw new TypeError('Invalid interrupt timeout');
    this.interruptMode = interruptMode;
    this.interruptTimeoutMs = interruptTimeoutMs;
    if (!['append', 'custom'].includes(systemPromptMode)) throw new TypeError('Invalid system prompt mode');
    this.systemPromptMode = systemPromptMode;
    if (![idleMs, maxSessions].every(n => Number.isFinite(n) && n > 0)
        || !Number.isFinite(requestTimeoutMs) || requestTimeoutMs < 0
        || !Number.isInteger(maxSessions)) throw new TypeError('Invalid bridge resource limits');
    this.queryFactory = queryFactory ?? (async options => (await import('@anthropic-ai/claude-agent-sdk')).query(options));
    this.historyImporter = historyImporter;
    this.processFactory = processFactory;
    this.env = env;
    this.stateRoot = stateRoot;
    this.idleMs = idleMs;
    this.requestTimeoutMs = requestTimeoutMs;
    this.maxSessions = maxSessions;
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
      if (this.sessions.size >= this.maxSessions) throw new BridgeError(503, 'The bridge session capacity is full.');
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
      session.interruption = null;
      session.ready = false;
      request.sent = true;
      request.phase = 'continuation';
      session.prompts.push(plan.content);
      request.phase = 'iterator';
      return;
    }
    if (plan.kind === 'rebuild') {
      request.phase = 'dispose';
      await this.dispose(slot.session);
      if (slot.session && (!slot.session.process.didExit || !slot.session.setupFinished))
        throw new BridgeError(503, 'The previous SDK process is still closing.');
      if (slot.session) await this.cleanupFiles(slot.session);
      if (request.settled || this.closing || slot.closing) return;
      request.phase = 'setup';
      const session = { tools: new ToolBridge(body.tools ?? []), prompts: promptStream(), process: this.processFactory(),
        controller: new AbortController(), query: null, cwd: null, imported: null, expected: null, fingerprint,
        ready: false, dead: false, emitted: new Set(), cleanupPromise: null, pump: null, settingUp: true, setupFinished: false, setup: deferred() };
      slot.session = session;
      try {
        request.phase = 'workspace';
        // SDK-generated environment context includes cwd: keep it stable for this host
        // slot, but private from every other named or anonymous slot.
        slot.cwd ??= await createWorkspace(this.stateRoot);
        session.cwd = slot.cwd;
        if (session.dead || request.settled) return;
        const { prefix, prompt } = rebuildInput(request.messages);
        // Historical tools are names only: never advertise or execute them.
        const definitions = [...(body.tools ?? [])];
        const names = new Set(definitions.map(tool => tool.name));
        for (const message of prefix) for (const block of message.content) {
          if (block.type === 'tool_use' && !names.has(block.name)) {
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
            toolNameToSdk: name => historyNames.get(name) });
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
          tools: [],
          allowedTools: (body.tools ?? []).map(tool => session.tools.toSdkName(tool.name)),
          mcpServers: { azo: session.tools.sdkServer },
          strictMcpConfig: true,
          settingSources: [],
          settings: clone(isolatedSettings),
          plugins: [],
          skills: [],
          hooks: {},
          permissionMode: 'default',
          canUseTool: async (name, input) => {
            try {
              const host = session.tools.fromSdkName(name);
              if (!host || session.dead) throw new Error('denied');
              return { behavior: 'allow', updatedInput: input };
            } catch { return { behavior: 'deny', message: 'Only Agent Zoo bridge tools are permitted.' }; }
          },
          includePartialMessages: true,
          persistSession: false,
          abortController: session.controller,
          spawnClaudeCodeProcess: session.process.spawn,
          env: childEnvironment(this.env, { CLAUDE_CODE_MAX_OUTPUT_TOKENS: String(body.max_tokens), ENABLE_TOOL_SEARCH: 'false' }),
          ...(session.imported ? { resume: session.imported.sessionId } : { sessionId: randomUUID() }),
          ...(resumeSessionAt ? { resumeSessionAt } : {}),
          ...(body.thinking ? { thinking: thinkingOption(body.thinking) } : {}),
          ...(body.output_config?.effort !== undefined ? { effort: body.output_config.effort } : {}),
        };
        request.sent = true;
        session.prompts.push(prompt);
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
      request.sent = true;
      if (plan.kind === 'results') {
        session.tools.deliver(plan.results);
        session.unsettledTool = false;
      } else session.prompts.push(plan.content);
      request.phase = 'iterator';
    }
  }

  async pump(slot, session) {
    let phase = 'iterator';
    try {
      for await (const message of session.query) {
        if (session.dead) break;
        phase = 'consume';
        this.consume(slot, session, message);
        phase = 'iterator';
      }
      if (!session.dead) {
        // Exhaustion is NOT an ordinary SDK result boundary. A later request
        // rebuilds rather than writing into an already exhausted prompt stream.
        session.dead = true;
        if (session.interruption) session.interruption.failure.resolve(false);
        else if (slot.current && !slot.current.settled) this.fail(slot, slot.current, upstream('iterator_exhausted'), phase);
      }
    } catch (error) {
      if (session.interruption) session.interruption.failure.resolve(false);
      else if (!session.dead && slot.current && !slot.current.settled) this.fail(slot, slot.current,
        bridgeError(error, phase === 'iterator' ? 'iterator_failed' : 'sdk_message_invalid'), phase);
    } finally {
      await this.dispose(session);
    }
  }

  consume(slot, session, sdk) {
    if (sdk.parent_tool_use_id != null) throw upstream('unexpected_subagent'); // subagents are never enabled
    if (sdk.type === 'system' && sdk.subtype === 'compact_boundary') throw upstream('unexpected_compaction');
    // Context for a later failure: which plan limit rejected us, and the
    // provider status behind SDK-internal retries.
    if (sdk.type === 'rate_limit_event') { session.rateLimit = sdk.rate_limit_info; return; }
    if (sdk.type === 'system' && sdk.subtype === 'api_retry') { session.lastRetry = sdk; return; }
    const hasTool = sdk.type === 'assistant' && !session.emitted.has(sdk.message?.id)
      && sdk.message?.content?.some(block => block.type === 'tool_use')
      || sdk.type === 'stream_event' && sdk.event?.type === 'content_block_start' && sdk.event.content_block?.type === 'tool_use';
    if (hasTool) session.unsettledTool = true;
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
      // Same-ID envelopes echo streamed blocks. A different ID is a retry:
      // discard only unpublished state, never splice two visible responses.
      if (request.stream?.message.id === sdk.message?.id) return;
      if (request.stream) {
        if (request.stream.published) throw upstream('published_message_replaced');
        request.stream = null;
        request.complete = null;
      }
      const message = clone(sdk.message);
      message.content = message.content.map(block => this.mapBlock(session, block));
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
      if (session.emitted.has(event.message.id)) { request.ignoredStream = true; return; }
      if (request.stream?.published) throw upstream('published_stream_replaced');
      request.complete = null;
      const message = clone(event.message);
      if (message.content?.length) throw upstream('nonempty_message_start');
      message.content = [];
      event.message = clone(message);
      request.fallback = null;
      request.stream = { message, events: [event], blocks: new Map(), stopped: false, published: false };
      return;
    }
    const stream = request.stream;
    if (!stream || stream.stopped) throw upstream('stream_boundary_invalid');
    if (event.type === 'content_block_start') {
      if (!Number.isInteger(event.index) || event.index < 0 || stream.blocks.has(event.index)) throw upstream('block_index_invalid');
      const block = this.mapBlock(session, event.content_block);
      event.content_block = clone(block);
      stream.blocks.set(event.index, { block, json: '', closed: false });
    } else if (event.type === 'content_block_delta') {
      const entry = stream.blocks.get(event.index);
      if (!entry || entry.closed) throw upstream('block_boundary_invalid');
      const block = entry.block, delta = event.delta;
      if (delta.type === 'text_delta' && block.type === 'text') block.text += delta.text;
      else if (delta.type === 'thinking_delta' && block.type === 'thinking') block.thinking += delta.thinking;
      else if (delta.type === 'signature_delta' && block.type === 'thinking') block.signature = (block.signature ?? '') + delta.signature;
      else if (delta.type === 'input_json_delta' && block.type === 'tool_use') entry.json += delta.partial_json;
      else if (delta.type === 'citations_delta' && block.type === 'text') (block.citations ??= []).push(delta.citation);
      else throw upstream('unsupported_delta');
    } else if (event.type === 'content_block_stop') {
      const entry = stream.blocks.get(event.index);
      if (!entry || entry.closed) throw upstream('block_boundary_invalid');
      if (entry.json) entry.block.input = JSON.parse(entry.json);
      entry.closed = true;
    } else if (event.type === 'message_delta') {
      Object.assign(stream.message, event.delta);
      stream.message.usage = { ...stream.message.usage, ...event.usage };
    } else if (event.type === 'message_stop') {
      const entries = [...stream.blocks.entries()].sort(([a], [b]) => a - b);
      if (entries.some(([index, entry], i) => index !== i || !entry.closed)) throw upstream('incomplete_stream_blocks');
      stream.message.content = entries.map(([, entry]) => entry.block);
      stream.stopped = true;
      request.complete = stream.message;
    } else throw upstream('unsupported_event');
    stream.events.push(event);
    if (stream.stopped && stream.message.stop_reason === 'tool_use') {
      this.finish(slot, session, request, stream.message, stream.events);
    } else if (!stream.stopped) {
      this.publish(slot, request, stream.events.splice(0));
      stream.published ||= Boolean(request.onEvent);
    }
  }

  mapBlock(session, source) {
    const block = clone(source);
    if (!['text', 'thinking', 'redacted_thinking', 'tool_use'].includes(block?.type)) throw upstream('unsupported_block');
    if (block.type === 'tool_use') block.name = session.tools.fromSdkName(block.name);
    return block;
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
    session.emitted.add(message.id);
    if (session.emitted.size > 512) session.emitted.delete(session.emitted.values().next().value);
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
    if (this.interruptMode !== 'reuse' || !request.sent || !session || session.dead
        || session.interruption || session.ready || session.unsettledTool
        || session.tools.pendingIds.size || session.tools.hasWaitingCalls
        || typeof session.query?.interrupt !== 'function') {
      this.fail(slot, request, error);
      return;
    }
    request.settled = true;
    request.reject(error);
    // This is the HOST prefix, not the SDK's private retained partial answer.
    session.expected = request.messages;
    session.ready = false;
    const interruption = { result: deferred(), failure: deferred() };
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
    // One deletion per generation: late cleanup must not unlink a newer import
    // that happens to use the same path in the slot-owned workspace.
    session.filesCleanupPromise ??= session.imported?.path
      ? rm(session.imported.path, { force: true }).catch(() => {}) : Promise.resolve();
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

function messageEvents(message) {
  const events = [{ type: 'message_start', message: { ...clone(message), content: [], stop_reason: null, stop_sequence: null, usage: { ...message.usage, output_tokens: 0 } } }];
  message.content.forEach((block, index) => {
    events.push({ type: 'content_block_start', index, content_block: clone(block) }, { type: 'content_block_stop', index });
  });
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
