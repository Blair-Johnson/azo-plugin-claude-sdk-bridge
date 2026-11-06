import { spawn } from 'node:child_process';
import { mkdtemp, appendFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes } from 'node:crypto';
import { parseArgs } from 'node:util';
import { createBridgeServer } from '../src/server.mjs';
import { measuredManager } from './measured-manager.mjs';

const requiredChecks = ['provider_signal_received', 'normal_user_text_sent',
  'exactly_two_native_submissions', 'replacement_saw_user_instruction', 'first_future_cancelled',
  'normal_user_preemption', 'first_worker_exited', 'worker_replaced', 'only_steered_answer_applied',
  'stale_answer_absent', 'no_manual_interrupt', 'no_provider_error', 'watchdog_not_used',
  'cancellation_bounded', 'all_native_workers_exited', 'watchdog_thread_exited'];

export function validateRuntimeResult(stdout) {
  const lines = stdout.trim().split('\n').filter(Boolean);
  if (lines.length !== 1) throw new Error('InvalidPythonResult');
  const result = JSON.parse(lines[0]);
  if (result.kind !== 'runtime_steering' || result.ok !== true || result.mode !== 'live'
      || requiredChecks.some(key => result.checks?.[key] !== true)
      || Object.values(result.checks).some(value => value !== true)
      || result.counts?.submissions !== 2 || result.counts?.applied_answers !== 1
      || result.counts?.cancellations !== 1 || !Array.isArray(result.error_types)
      || result.error_types.length || !result.metrics
      || Object.values(result.metrics).some(value => !Number.isFinite(value))) {
    throw new Error('InvalidPythonResult');
  }
  return result;
}

// Only this replicate's marks are supplied. Request numbers below are owned by
// this launcher, not inferred from whichever request happened to abort first.
export function validateRuntimeMarks(marks, session, budgetMs = 5000, interruptMode = 'reuse') {
  const requests = marks.filter(m => m.event === 'runtime_request' && m.session === session);
  const queries = marks.filter(m => m.event === 'query_construct');
  const spawns = marks.filter(m => m.event === 'spawn');
  const aborts = marks.filter(m => m.event === 'runtime_transport_aborted'
    && m.session === session && m.request === 1);
  const firstText = marks.find(m => m.event === 'first_text' && m.session === session && m.request === 1);
  const text = marks.find(m => m.event === 'first_text' && m.session === session && m.request === 2);
  const first = queries[0], replacement = interruptMode === 'reuse' ? first : queries[1], aborted = aborts[0];
  const oldSpawn = spawns.find(m => m.query === first?.query);
  const newSpawn = spawns.find(m => m.query === replacement?.query);
  const exit = marks.find(m => m.event === 'exit' && m.query === first?.query);
  const interrupts = marks.filter(m => m.event === 'interrupt_requested' && m.query === first?.query);
  const acks = marks.filter(m => m.event === 'interrupt_ack' && m.query === first?.query);
  const interrupt = interrupts[0], ack = acks[0];
  const oldResult = marks.find(m => m.event === 'sdk_result' && m.query === first?.query);
  const checks = {
    valid_interrupt_mode: ['reuse', 'rebuild'].includes(interruptMode),
    exactly_two_requests: requests.length === 2 && requests[0]?.request === 1 && requests[1]?.request === 2,
    first_query_correlated: Boolean(first && oldSpawn && requests[0] && firstText
      && requests[0].at_ms <= first.at_ms && first.at_ms <= oldSpawn.at_ms && oldSpawn.at_ms <= firstText.at_ms),
    transport_abort_observed: aborts.length === 1 && Boolean(firstText && aborted.at_ms >= firstText.at_ms),
    first_sdk_unfinished_at_abort: Boolean(first && aborted)
      && !marks.some(m => m.event === 'sdk_result' && m.query === first.query && m.at_ms <= aborted.at_ms),
    replacement_text_after_request: Boolean(requests[1] && text && aborted
      && requests[1].at_ms >= aborted.at_ms && text.at_ms >= requests[1].at_ms),
  };
  if (interruptMode === 'reuse') Object.assign(checks, {
    exactly_one_query_and_spawn: queries.length === 1 && spawns.length === 1,
    interrupt_after_transport_abort: interrupts.length === 1 && Boolean(aborted && interrupt.at_ms >= aborted.at_ms),
    interrupt_ack_before_replacement_text: acks.length === 1 && Boolean(interrupt && text
      && ack.at_ms >= interrupt.at_ms && ack.at_ms <= text.at_ms && ack.queued_count === 0),
    old_sdk_result_before_replacement_text: Boolean(interrupt && oldResult && text
      && oldResult.at_ms >= interrupt.at_ms && oldResult.at_ms < text.at_ms),
    old_child_alive_until_cleanup: !exit && marks.some(m => m.event === 'child_alive'
      && m.query === first?.query && text && m.at_ms >= text.at_ms),
  });
  else Object.assign(checks, {
    exactly_two_requests_and_queries: requests.length === 2 && queries.length === 2 && spawns.length === 2,
    distinct_queries: first?.query !== undefined && replacement?.query !== undefined
      && first.query !== replacement.query,
    old_sdk_exit_bounded: Boolean(aborted && exit)
      && exit.at_ms >= aborted.at_ms && exit.at_ms - aborted.at_ms <= budgetMs,
    replacement_spawn_after_old_exit: Boolean(newSpawn && exit && replacement && requests[1] && text
      && requests[1].at_ms <= replacement.at_ms && replacement.at_ms <= newSpawn.at_ms
      && exit.at_ms <= newSpawn.at_ms && newSpawn.at_ms <= text.at_ms),
  });
  return { passed: Object.values(checks).every(Boolean), checks, interrupt_mode: interruptMode,
    first_query: first?.query ?? null, replacement_query: replacement?.query ?? null,
    process_restarts: Math.max(0, spawns.length - 1),
    abort_to_sdk_exit_ms: aborted && exit ? exit.at_ms - aborted.at_ms : null,
    abort_to_interrupt_ack_ms: aborted && ack ? ack.at_ms - aborted.at_ms : null,
    abort_to_replacement_first_text_ms: aborted && text ? text.at_ms - aborted.at_ms : null,
    replacement_arrival_to_first_text_ms: requests[1] && text ? text.at_ms - requests[1].at_ms : null };
}

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
async function waitUntil(predicate, milliseconds) {
  const deadline = performance.now() + milliseconds;
  while (!predicate() && performance.now() < deadline) await delay(20);
  return predicate();
}

// A fresh POSIX session owns Python and its multiprocessing descendants. Never
// wait indefinitely on inherited stdout pipes or trust TERM alone to stop them.
export async function runOwnedPython(command, args, { env = process.env, timeoutMs = 100000,
  graceMs = 1000, killMs = 1000 } = {}) {
  if (process.platform === 'win32') throw new Error('PosixProcessGroupsRequired');
  const child = spawn(command, args, { env, detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '', stderr = false, overflow = false, closed = false, timedOut = false;
  let code = null, spawnError = false, timer, cleanupOk = false;
  const finished = new Promise(resolve => {
    child.once('error', () => { spawnError = true; resolve(); });
    child.once('close', exitCode => { code = exitCode; closed = true; resolve(); });
  });
  child.stdout.on('data', data => {
    if (stdout.length + data.length > 1024 * 1024) overflow = true;
    else if (!overflow) stdout += data;
  });
  child.stderr.on('data', () => { stderr = true; });
  const groupExists = () => {
    if (!child.pid) return false;
    try { process.kill(-child.pid, 0); return true; }
    catch (error) {
      if (error.code === 'ESRCH') return false;
      if (error.code === 'EPERM') return true; // Not proof the group is gone; keep the bounded wait.
      throw error;
    }
  };
  const signalGroup = signal => {
    if (!child.pid) return;
    try { process.kill(-child.pid, signal); }
    catch (error) { if (error.code !== 'ESRCH') throw error; }
  };
  try {
    await Promise.race([finished, new Promise(resolve => {
      timer = setTimeout(() => { timedOut = true; resolve(); }, timeoutMs);
    })]);
  } finally {
    clearTimeout(timer);
    try {
      if (groupExists()) signalGroup('SIGTERM');
      cleanupOk = await waitUntil(() => !groupExists() && closed, graceMs);
      if (!cleanupOk) {
        signalGroup('SIGKILL');
        cleanupOk = await waitUntil(() => !groupExists() && closed, killMs);
      }
    } finally {
      child.stdout.destroy(); child.stderr.destroy(); child.unref();
    }
  }
  return { stdout, stderr_present: stderr, exit_code: code, timed_out: timedOut,
    spawn_error: spawnError, output_overflow: overflow, all_native_children_exited: cleanupOk };
}

export async function main(argv = process.argv.slice(2)) {
  const emit = data => console.log(JSON.stringify(data));
  let workspace, observation, endpoint, passed = false, completed = 0, nativeChildrenExited = true;
  const errors = [];
  try {
    const { values } = parseArgs({ args: argv, options: { live: { type: 'boolean', default: false },
      'interrupt-mode': { type: 'string', default: 'reuse' },
      python: { type: 'string', default: 'python' }, replicates: { type: 'string', default: '3' } } });
    if (!values.live) throw new Error('LiveInferenceNotAuthorized');
    const interruptMode = values['interrupt-mode'];
    if (!['reuse', 'rebuild'].includes(interruptMode)) throw new Error('InvalidInterruptMode');
    const replicates = Number(values.replicates);
    if (!Number.isInteger(replicates) || replicates < 1 || replicates > 10) throw new Error('InvalidReplicates');
    workspace = await mkdtemp(join(tmpdir(), 'azo-runtime-latency-'));
    const key = randomBytes(32).toString('hex');
    observation = measuredManager({ requestTimeoutMs: 90000, systemPromptMode: 'append', interruptMode });
    const handle = observation.manager.handle.bind(observation.manager);
    let eventsFile, currentSession, requestIndex = 0;
    observation.manager.handle = async (body, context) => {
      const index = ++requestIndex, session = context.sessionId;
      const path = eventsFile;
      observation.mark('runtime_request', { session, request: index });
      const onAbort = () => observation.mark('runtime_transport_aborted', { session, request: index });
      context.signal?.addEventListener('abort', onAbort, { once: true });
      let seen = false;
      try {
        return await handle(body, { ...context, onEvent(event) {
          context.onEvent?.(event);
          if (!seen && event.type === 'content_block_delta' && event.delta?.type === 'text_delta' && event.delta.text) {
            seen = true;
            observation.mark('first_text', { session, request: index });
            if (index === 1) appendFile(path, JSON.stringify({ event: 'provider_started',
              session_id: session, request_index: 1 }) + '\n', { mode: 0o600 }).catch(() => {});
          }
        } });
      } finally { context.signal?.removeEventListener('abort', onAbort); }
    };
    endpoint = createBridgeServer({ manager: observation.manager, key, requestTimeoutMs: 90000 });
    const address = await endpoint.listen(0);
    const sdkPackage = JSON.parse(await readFile(new URL('../node_modules/@anthropic-ai/claude-agent-sdk/package.json', import.meta.url), 'utf8'));
    emit({ kind: 'config', live: true, scope: 'native_runtime_session_io_not_frontend', replicates,
      interrupt_mode: interruptMode, sdk: sdkPackage.version, node: process.version, model: 'sonnet', system_prompt_mode: 'append',
      max_tokens: 2048, thinking: 'sdk_default', credential_values_logged: false });
    const script = fileURLToPath(new URL('./runtime_steering.py', import.meta.url));
    for (let i = 0; i < replicates; i++) {
      currentSession = `runtime-live-${i}-${randomBytes(5).toString('hex')}`;
      eventsFile = join(workspace, `${i}.jsonl`); requestIndex = 0;
      const markIndex = observation.marks.length;
      let result, bridgeResult;
      try {
        nativeChildrenExited = false; // Unknown until bounded group cleanup confirms it.
        const processResult = await runOwnedPython(values.python, [script, '--live', '--base-url',
          `http://127.0.0.1:${address.port}`, '--events-file', eventsFile, '--session-id', currentSession],
        { env: { ...process.env, AZO_CLAUDE_BRIDGE_KEY: key } });
        nativeChildrenExited = processResult.all_native_children_exited;
        const { stdout, ...processChecks } = processResult;
        if (processResult.exit_code !== 0 || processResult.timed_out || processResult.spawn_error
            || processResult.output_overflow || !processResult.all_native_children_exited) {
          emit({ kind: 'runtime_bridge', replicate: i, passed: false, ...processChecks });
          throw new Error('PythonProcessFailed');
        }
        result = validateRuntimeResult(stdout);
        for (const [query, child] of observation.processes.entries()) {
          if (!child.didExit) observation.mark('child_alive', { query });
        }
        const marks = observation.marks.slice(markIndex);
        const evidence = validateRuntimeMarks(marks, currentSession, 5000, interruptMode);
        bridgeResult = { kind: 'runtime_bridge', replicate: i, ...processChecks, ...evidence,
          requests: requestIndex, processes_started: marks.filter(m => m.event === 'spawn').length,
          resolved_models: [...new Set(marks.filter(m => m.event === 'sdk_init').map(m => m.model))] };
        if (!evidence.passed) {
          emit(bridgeResult);
          throw new Error('InvalidCancellationEvidence');
        }
      } finally { await observation.manager.cancel(currentSession); }
      if (!observation.processes.every(p => p.didExit)) throw new Error('SdkChildCleanupFailed');
      emit(bridgeResult);
      emit({ ...result, replicate: i });
      completed++;
    }
    passed = completed === replicates;
  } catch (error) {
    // Do not print exception messages, argv, Python stderr, or malformed stdout.
    errors.push(error instanceof SyntaxError ? 'SyntaxError' : error instanceof TypeError ? 'TypeError' : 'Error');
  } finally {
    let cleanupOk = true;
    try {
      if (endpoint) await endpoint.close();
      else if (observation) await observation.manager.close();
    } catch { cleanupOk = false; errors.push('BridgeCleanupError'); }
    try { if (workspace) await rm(workspace, { recursive: true, force: true }); }
    catch { cleanupOk = false; errors.push('WorkspaceCleanupError'); }
    const sdkChildrenExited = observation?.processes.every(p => p.didExit) ?? true;
    const allChildrenExited = nativeChildrenExited && sdkChildrenExited;
    passed = passed && cleanupOk && allChildrenExited && errors.length === 0;
    emit({ kind: 'final', passed, completed_replicates: completed,
      all_children_exited: allChildrenExited, error_types: errors });
  }
  return passed ? 0 : 1;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = await main();
}
