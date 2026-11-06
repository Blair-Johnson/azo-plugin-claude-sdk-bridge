import { randomUUID } from 'node:crypto';
import { readFile, rm } from 'node:fs/promises';
import { createWorkspace } from './workspace.mjs';
import { ToolBridge } from './tools.mjs';
import { canonicalMessages, digest, importSession, requestFingerprint, validateHistory } from './history.mjs';
import { childEnvironment, isolatedSettings, QueryProcess } from './child.mjs';

export class BridgeError extends Error {
  constructor(statusCode, publicMessage) {
    super(publicMessage);
    this.name = 'BridgeError';
    this.statusCode = statusCode;
    this.publicMessage = publicMessage;
  }
}
const bad = message => new BridgeError(400, message);
const upstream = () => new BridgeError(502, 'The Claude SDK session failed.');
const clone = value => structuredClone(value);
function safeError(error) {
  if (error instanceof BridgeError) return error;
  return error?.statusCode === 400 ? bad('Unsupported or invalid message/tool content.') : upstream();
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
      if (ended) throw upstream();
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

// Reference-compatible SDK-only opener for a rebuilt turn ending in real tool
// results. Never add this to request.messages or the host canonical ledger.
export const CONTINUE_AFTER_RESUME_PROMPT =
  '[Your session was interrupted and resumed. What precedes this ends with the tool result '
  + 'you were waiting for. Continue the task from there.]';

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
      prompt: prompt.length ? prompt : [{ type: 'text', text: CONTINUE_AFTER_RESUME_PROMPT }],
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
/** Agent Zoo owns history and tools. SDK sessions are disposable mirrors only. */
export class BridgeManager {
  constructor({ queryFactory, stateRoot, idleMs = 300000,
    requestTimeoutMs = 120000, maxSessions = 16, historyImporter = importSession,
    processFactory = () => new QueryProcess(), env = process.env, systemPromptMode = 'append',
    interruptMode = 'reuse', interruptTimeoutMs = 500 } = {}) {
    if (!['reuse', 'rebuild'].includes(interruptMode)) throw new TypeError('Invalid interrupt mode');
    if (!Number.isFinite(interruptTimeoutMs) || interruptTimeoutMs <= 0 || interruptTimeoutMs > 5000)
      throw new TypeError('Invalid interrupt timeout');
    this.interruptMode = interruptMode;
    this.interruptTimeoutMs = interruptTimeoutMs;
    if (!['append', 'custom'].includes(systemPromptMode)) throw new TypeError('Invalid system prompt mode');
    this.systemPromptMode = systemPromptMode;
    if (![idleMs, requestTimeoutMs, maxSessions].every(n => Number.isFinite(n) && n > 0)
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
    } catch (error) { throw safeError(error); }
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
      slot = { key, busy: false, session: null, current: null, touched: Date.now(), closing: false };
      this.sessions.set(key, slot);
    }
    if (slot.closing) throw new BridgeError(409, 'This session is closing.');
    // Validate continuation before installing a sink or releasing any MCP waiter.
    let plan;
    try { plan = this.plan(slot.session, messages, fingerprint); }
    catch (error) { throw safeError(error); }
    slot.busy = true;
    const request = { ...deferred(), messages, onEvent, signal, settled: false, stream: null, complete: null, fallback: null,
      sdkUuids: new Set() };
    slot.current = request;
    const abort = () => this.cancelRequest(slot, request);
    signal?.addEventListener('abort', abort, { once: true });
    const timer = setTimeout(() => this.fail(slot, request, new BridgeError(504, 'The Claude SDK request timed out.')), this.requestTimeoutMs);
    if (signal?.aborted) abort();
    // Sink installation above MUST precede deliver(): it can synchronously wake
    // MCP handlers and produce output for the new HTTP response.
    this.drive(slot, request, body, fingerprint, plan).catch(error => this.fail(slot, request, safeError(error)));
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
    if (!session || session.dead || session.fingerprint !== fingerprint || !session.expected) return { kind: 'rebuild' };
    if (session.interruption) {
      const content = appendedFeedback(session.expected, messages);
      return content ? { kind: 'steer', content } : { kind: 'rebuild' };
    }
    const prefix = messages.slice(0, session.expected.length);
    if (!same(prefix, session.expected)) return { kind: 'rebuild' };
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
      // Never queue steering behind or ahead of a parked MCP batch accidentally.
      if (results.length !== content.length) return { kind: 'rebuild' };
      return { kind: 'results', results };
    }
    if (session.ready && suffix.length === 1 && suffix[0].role === 'user'
        && !(Array.isArray(suffix[0].content) && suffix[0].content.some(b => b.type === 'tool_result'))) {
      return { kind: 'user', content: suffix[0].content };
    }
    return { kind: 'rebuild' };
  }

  async drive(slot, request, body, fingerprint, plan) {
    if (request.settled || this.closing || slot.closing) return;
    if (plan.kind === 'steer') {
      const session = slot.session;
      request.reusing = true;
      const reusable = await session.interruption.promise;
      if (request.settled || this.closing || slot.closing) return;
      if (!reusable || session.dead || session.unsettledTool
          || session.tools.pendingIds.size || session.tools.hasWaitingCalls) {
        request.reusing = false;
        return this.drive(slot, request, body, fingerprint, { kind: 'rebuild' });
      }
      session.interruption = null;
      session.ready = false;
      request.sent = true;
      session.prompts.push(plan.content);
      return;
    }
    if (plan.kind === 'rebuild') {
      await this.dispose(slot.session);
      if (slot.session && !slot.session.process.didExit) throw new BridgeError(503, 'The previous SDK process is still closing.');
      if (request.settled || this.closing || slot.closing) return;
      const session = { tools: new ToolBridge(body.tools ?? []), prompts: promptStream(), process: this.processFactory(),
        controller: new AbortController(), query: null, cwd: null, imported: null, expected: null, fingerprint,
        ready: false, dead: false, emitted: new Set(), cleanupPromise: null, pump: null };
      slot.session = session;
      try {
        session.cwd = await createWorkspace(this.stateRoot);
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
          session.imported = await this.historyImporter({ messages: prefix, cwd: session.cwd, model: body.model,
            toolNameToSdk: name => historyNames.get(name) });
        }
        if (session.dead || request.settled) return;
        // The CLI appends a synthetic "No response requested." assistant when
        // loading a user-ended transcript. Anchor at the actual final result so
        // that loader-only placeholder never reaches inference. No host record
        // is removed or rewritten; the SDK prompt starts immediately after it.
        let resumeSessionAt;
        if (prefix.at(-1)?.role === 'user' && session.imported?.path) {
          const records = (await readFile(session.imported.path, 'utf8')).trim().split('\n').map(line => JSON.parse(line));
          resumeSessionAt = records.findLast(record => record.type === 'user')?.uuid;
          if (!resumeSessionAt) throw upstream();
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
          env: childEnvironment(this.env, { CLAUDE_CODE_MAX_OUTPUT_TOKENS: String(body.max_tokens), ENABLE_TOOL_SEARCH: 'false',
            // A custom system prompt means host-owned context, not hidden CLI
            // environment/model/date attachments embedded in real tool results.
            CLAUDE_CODE_SIMPLE: this.systemPromptMode === 'custom' ? '1' : undefined,
          }),
          ...(session.imported ? { resume: session.imported.sessionId } : { sessionId: randomUUID() }),
          ...(resumeSessionAt ? { resumeSessionAt } : {}),
          ...(body.thinking ? { thinking: thinkingOption(body.thinking) } : {}),
          ...(body.output_config?.effort !== undefined ? { effort: body.output_config.effort } : {}),
        };
        request.sent = true;
        session.prompts.push(prompt);
        session.query = await this.queryFactory({ prompt: session.prompts, options });
        if (session.dead || request.settled) {
          try { Promise.resolve(session.query?.close?.()).catch(() => {}); } catch { /* sanitized */ }
          return;
        }
        session.pump = this.pump(slot, session).catch(() => {});
      } finally {
        // Cancellation can race mkdtemp/import/query construction. A second
        // cleanup pass removes files that appeared after the first pass.
        if (session.dead || request.settled && !session.query) await this.cleanupFiles(session);
      }
    } else {
      const session = slot.session;
      if (session.dead) throw upstream();
      session.ready = false;
      request.sent = true;
      if (plan.kind === 'results') {
        session.tools.deliver(plan.results);
        session.unsettledTool = false;
      } else session.prompts.push(plan.content);
    }
  }

  async pump(slot, session) {
    try {
      for await (const message of session.query) {
        if (session.dead) break;
        this.consume(slot, session, message);
      }
      if (!session.dead) {
        // Exhaustion is NOT an ordinary SDK result boundary. A later request
        // rebuilds rather than writing into an already exhausted prompt stream.
        session.dead = true;
        if (session.interruption) session.interruption.failure.resolve(false);
        else if (slot.current && !slot.current.settled) this.fail(slot, slot.current, upstream());
      }
    } catch (error) {
      if (session.interruption) session.interruption.failure.resolve(false);
      else if (!session.dead && slot.current && !slot.current.settled) this.fail(slot, slot.current, safeError(error));
    } finally {
      await this.dispose(session);
    }
  }

  consume(slot, session, sdk) {
    if (sdk.parent_tool_use_id != null) throw upstream(); // subagents are never enabled
    if (sdk.type === 'system' && sdk.subtype === 'compact_boundary') throw upstream();
    const hasTool = sdk.type === 'assistant' && !session.emitted.has(sdk.message?.id)
      && sdk.message?.content?.some(block => block.type === 'tool_use')
      || sdk.type === 'stream_event' && sdk.event?.type === 'content_block_start' && sdk.event.content_block?.type === 'tool_use';
    if (hasTool) session.unsettledTool = true;
    if (session.interruption) {
      if (hasTool || session.tools.hasWaitingCalls) session.interruption.failure.resolve(false);
      if (sdk.type === 'result') session.interruption.result.resolve(sdk);
      return; // Old partials/results must never enter the replacement sink.
    }
    if (sdk.type === 'assistant' && sdk.aborted) throw upstream();
    if (sdk.type === 'result') {
      if (sdk.subtype !== 'success' || sdk.is_error) throw upstream();
      session.ready = true;
      const request = slot.current;
      if (request && !request.settled) {
        const message = request.complete ?? request.fallback;
        if (!message) throw upstream();
        if (!message.stop_reason) message.stop_reason = sdk.stop_reason ?? 'end_turn';
        this.finish(slot, session, request, message, request.stream?.events);
      }
      return;
    }
    const request = slot.current;
    if (!request || request.settled) return;
    if (sdk.type === 'assistant') {
      if (sdk.error || sdk.message?.model === '<synthetic>') throw upstream();
      if (session.emitted.has(sdk.message?.id)) return; // late per-block SDK echo
      if (sdk.uuid && request.sdkUuids.has(sdk.uuid)) return;
      if (sdk.uuid) request.sdkUuids.add(sdk.uuid);
      // Same-ID envelopes echo streamed blocks. A different ID is a retry:
      // discard only unpublished state, never splice two visible responses.
      if (request.stream?.message.id === sdk.message?.id) return;
      if (request.stream) {
        if (request.stream.published) throw upstream();
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
          'The Claude SDK returned a tool batch without a completion boundary.');
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
      if (request.stream?.published) throw upstream();
      request.complete = null;
      const message = clone(event.message);
      if (message.content?.length) throw upstream();
      message.content = [];
      event.message = clone(message);
      request.fallback = null;
      request.stream = { message, events: [event], blocks: new Map(), stopped: false, published: false };
      return;
    }
    const stream = request.stream;
    if (!stream || stream.stopped) throw upstream();
    if (event.type === 'content_block_start') {
      if (!Number.isInteger(event.index) || event.index < 0 || stream.blocks.has(event.index)) throw upstream();
      const block = this.mapBlock(session, event.content_block);
      event.content_block = clone(block);
      stream.blocks.set(event.index, { block, json: '', closed: false });
    } else if (event.type === 'content_block_delta') {
      const entry = stream.blocks.get(event.index);
      if (!entry || entry.closed) throw upstream();
      const block = entry.block, delta = event.delta;
      if (delta.type === 'text_delta' && block.type === 'text') block.text += delta.text;
      else if (delta.type === 'thinking_delta' && block.type === 'thinking') block.thinking += delta.thinking;
      else if (delta.type === 'signature_delta' && block.type === 'thinking') block.signature = (block.signature ?? '') + delta.signature;
      else if (delta.type === 'input_json_delta' && block.type === 'tool_use') entry.json += delta.partial_json;
      else if (delta.type === 'citations_delta' && block.type === 'text') (block.citations ??= []).push(delta.citation);
      else throw upstream();
    } else if (event.type === 'content_block_stop') {
      const entry = stream.blocks.get(event.index);
      if (!entry || entry.closed) throw upstream();
      if (entry.json) entry.block.input = JSON.parse(entry.json);
      entry.closed = true;
    } else if (event.type === 'message_delta') {
      Object.assign(stream.message, event.delta);
      stream.message.usage = { ...stream.message.usage, ...event.usage };
    } else if (event.type === 'message_stop') {
      const entries = [...stream.blocks.entries()].sort(([a], [b]) => a - b);
      if (entries.some(([index, entry], i) => index !== i || !entry.closed)) throw upstream();
      stream.message.content = entries.map(([, entry]) => entry.block);
      stream.stopped = true;
      request.complete = stream.message;
    } else throw upstream();
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
    if (!['text', 'thinking', 'redacted_thinking', 'tool_use'].includes(block?.type)) throw upstream();
    if (block.type === 'tool_use') block.name = session.tools.fromSdkName(block.name);
    return block;
  }

  finish(slot, session, request, source, rawEvents) {
    if (request.settled || session.emitted.has(source.id)) return;
    const message = clone(source);
    if (typeof message.id !== 'string' || !Array.isArray(message.content)) throw upstream();
    message.type = 'message';
    message.role = 'assistant';
    message.stop_sequence ??= null;
    message.usage ??= { input_tokens: 0, output_tokens: 0 };
    const calls = message.content.filter(block => block.type === 'tool_use');
    if (calls.length && message.stop_reason !== 'tool_use' || !calls.length && message.stop_reason === 'tool_use') throw upstream();
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
      const callback = request.onEvent?.(clone(event));
      if (callback && typeof callback.then === 'function') callback.catch(() => this.fail(slot, request, upstream()));
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
      } catch { /* Fall back without exposing SDK errors or queue contents. */ }
      finally { clearTimeout(timer); }
      reusable = Boolean(reusable && !session.dead && !session.unsettledTool
        && !session.tools.pendingIds.size && !session.tools.hasWaitingCalls);
      if (!reusable) await this.dispose(session);
      else session.ready = true;
      return reusable;
    })();
  }
  fail(slot, request, error) {
    if (request.settled) return;
    request.settled = true;
    request.reject(safeError(error));
    this.dispose(slot.session).catch(() => {});
  }

  async cleanupFiles(session) {
    // A stubborn process must not lose the transcript underneath its live fd.
    if (!session.process.didExit) return;
    if (session.imported?.path) await rm(session.imported.path, { force: true }).catch(() => {});
    if (session.cwd) await rm(session.cwd, { recursive: true, force: true }).catch(() => {});
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
      const forget = () => { if (this.sessions.get(slot.key) === slot) this.sessions.delete(slot.key); };
      // Install before bounded shutdown: a late close must always free capacity.
      slot.session?.process.exited.then(forget).catch(() => {});
      await this.dispose(slot.session);
      if (!slot.session || slot.session.process.didExit) forget();
      // A live child keeps its reserved closing slot, not a rejected sweep promise.
    })();
    return slot.removing;
  }

  async sweep() {
    const now = Date.now();
    await Promise.allSettled([...this.sessions.values()].filter(slot => !slot.busy && (!slot.session || slot.session.dead || now - slot.touched >= this.idleMs))
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
