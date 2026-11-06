import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { parseArgs } from 'node:util';
import { createBridgeServer } from '../src/server.mjs';
import { measuredManager } from './measured-manager.mjs';

const { values } = parseArgs({ options: {
  live: { type: 'boolean', default: false },
  model: { type: 'string', default: 'sonnet' },
  replicates: { type: 'string', default: '3' },
  'history-kib': { type: 'string', default: '0' },
  'prompt-mode': { type: 'string', default: 'append' },
  'interrupt-mode': { type: 'string', default: 'reuse' },
} });
if (!values.live) throw new Error('Live inference requires explicit --live.');
const interruptMode = values['interrupt-mode'];
assert.ok(['reuse', 'rebuild'].includes(interruptMode), 'Invalid interrupt mode');
const replicates = Number(values.replicates), historyKiB = Number(values['history-kib']);
assert.ok(Number.isInteger(replicates) && replicates >= 1 && replicates <= 10);
assert.ok(Number.isInteger(historyKiB) && historyKiB >= 0 && historyKiB <= 128);
const emit = record => console.log(JSON.stringify(record));
const observation = measuredManager({ systemPromptMode: values['prompt-mode'], requestTimeoutMs: 90000, interruptMode });
const key = randomBytes(32).toString('hex');
const endpoint = createBridgeServer({ manager: observation.manager, key, requestTimeoutMs: 90000 });
const address = await endpoint.listen(0);
const url = `http://127.0.0.1:${address.port}`;
const base = { model: values.model, max_tokens: 512, stream: true, tools: [], thinking: { type: 'disabled' },
  system: 'Controlled latency test. Follow the latest user instruction. When asked for an exact marker, output only that marker. Do not use tools.' };
const headers = session => ({ 'x-api-key': key, 'x-azo-session-id': session, 'content-type': 'application/json' });
const asAssistant = text => ({ role: 'assistant', content: [{ type: 'text', text }] });
const user = text => ({ role: 'user', content: [{ type: 'text', text }] });
const rows = [];
emit({ kind: 'config', live: true, requested_model: values.model, replicates, history_kib: historyKiB,
  prompt_mode: values['prompt-mode'], interrupt_mode: interruptMode, thinking: 'disabled', max_tokens: 512, node: process.version,
  platform: process.platform, architecture: process.arch, sdk: '0.3.308', credentials_logged: false });

async function call(label, session, messages, expected, abortAfterText = false) {
  const started = performance.now(), markIndex = observation.marks.length;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 90000);
  let text = '', firstText = null, firstEvent = null, stopped = false, canceledAt = null;
  let usage = {}, model = null, status = null;
  try {
    const response = await fetch(`${url}/v1/messages`, { method: 'POST', headers: headers(session),
      body: JSON.stringify({ ...base, messages }), signal: controller.signal });
    status = response.status;
    assert.equal(status, 200, `HTTP ${status} in ${label}`);
    const reader = response.body.getReader(), decoder = new TextDecoder();
    let pending = '';
    read: while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      pending += decoder.decode(value, { stream: true });
      let boundary;
      while ((boundary = pending.indexOf('\n\n')) >= 0) {
        const frame = pending.slice(0, boundary); pending = pending.slice(boundary + 2);
        const data = frame.split('\n').filter(line => line.startsWith('data:')).map(line => line.slice(5).trim()).join('\n');
        if (!data) continue;
        const event = JSON.parse(data);
        if (event.type === 'error') throw new Error(`Stream error: ${event.error?.type ?? 'unknown'}: ${event.error?.message ?? ''}`);
        if (event.type !== 'ping') firstEvent ??= performance.now();
        if (event.type === 'message_start') { model = event.message.model; usage = { ...event.message.usage }; }
        if (event.type === 'message_delta') usage = { ...usage, ...event.usage };
        if (event.type === 'content_block_delta' && event.delta?.type === 'text_delta' && event.delta.text) {
          firstText ??= performance.now(); text += event.delta.text;
          if (abortAfterText) { canceledAt = performance.now(); controller.abort(); await reader.cancel().catch(() => {}); break read; }
        }
        if (event.type === 'message_stop') stopped = true;
      }
    }
  } finally { clearTimeout(timeout); }
  const ended = performance.now(), marks = observation.marks.slice(markIndex);
  const spawn = marks.find(mark => mark.event === 'spawn'), init = marks.find(mark => mark.event === 'sdk_init');
  const row = { kind: 'measurement', case: label, session, interrupt_mode: interruptMode, history_kib: historyKiB,
    ttft_ms: firstText === null ? null : firstText - started, total_ms: ended - started,
    first_event_ms: firstEvent === null ? null : firstEvent - started,
    spawn_count: marks.filter(mark => mark.event === 'spawn').length,
    spawn_after_request_ms: spawn ? spawn.at_ms - started : null,
    sdk_init_ms: init ? init.at_ms - started : null,
    import_ms: marks.filter(mark => mark.event === 'history_import').reduce((sum, mark) => sum + mark.duration_ms, 0),
    model, usage, status, matched: expected == null ? null : text.trim() === expected, aborted: canceledAt !== null };
  rows.push(row); emit(row);
  if (expected != null) assert.equal(text.trim(), expected, `Unexpected answer in ${label}`);
  assert.ok(abortAfterText ? canceledAt !== null : stopped, `Missing stream boundary in ${label}`);
  return { text, started, ended, firstText, canceledAt, row };
}
async function clear(session) {
  const start = performance.now();
  const response = await fetch(`${url}/v1/session`, { method: 'DELETE', headers: headers(session) });
  assert.equal(response.status, 200);
  emit({ kind: 'teardown', session, duration_ms: performance.now() - start });
}

let passed = false;
try {
  for (let i = 0; i < replicates; i++) {
    const session = `latency-${i}-${randomBytes(5).toString('hex')}`;
    const padding = historyKiB ? [user(`Synthetic context; no action required.\n${'alpha beta gamma delta. '.repeat(Math.ceil(historyKiB * 1024 / 24)).slice(0, historyKiB * 1024)}`), asAssistant('Acknowledged.')] : [];
    const seed = [...padding, user(`Reply exactly BASE_${i}.`)];
    const cold = await call('cold', session, seed, `BASE_${i}`);
    const followHistory = [...seed, asAssistant(cold.text), user(`Reply exactly FOLLOW_${i}.`)];
    const warm = await call('reused', session, followHistory, `FOLLOW_${i}`);
    await clear(session);
    const rebuilt = await call('rebuilt', session, followHistory, `FOLLOW_${i}`);
    assert.equal(warm.row.spawn_count, 0, 'Warm comparison unexpectedly rebuilt');
    assert.equal(rebuilt.row.spawn_count, 1, 'Rebuild comparison did not start a new process');
    assert.equal(warm.row.model, rebuilt.row.model);
    emit({ kind: 'paired', replicate: i, interrupt_mode: interruptMode, history_kib: historyKiB,
      extra_ttft_ms: rebuilt.row.ttft_ms - warm.row.ttft_ms,
      extra_total_ms: rebuilt.row.total_ms - warm.row.total_ms });
    const interruptedHistory = [...followHistory, asAssistant(rebuilt.text),
      user('Count from 1 to 500, one integer per line. Output all integers without commentary.')];
    const oldProcess = observation.processes.length - 1;
    const interruptMarks = observation.marks.length;
    const interrupted = await call('interrupted', session, interruptedHistory, null, true);
    const steered = await call('after_interrupt', session,
      [...interruptedHistory, user(`Cancel the counting request. Reply exactly STEER_${i}.`)], `STEER_${i}`);
    const marks = observation.marks.slice(interruptMarks);
    const exit = marks.find(mark => mark.event === 'exit' && mark.query === oldProcess);
    const abort = marks.find(mark => mark.event === 'request_aborted' && mark.session === session);
    assert.ok(abort && abort.at_ms >= interrupted.canceledAt, 'Transport cancellation not observed');
    // Check the server-observed abort, not just the earlier client abort: a result
    // in that gap is a completed request, not evidence of interrupting inference.
    assert.ok(!marks.some(mark => mark.event === 'sdk_result' && mark.query === oldProcess
      && mark.at_ms <= abort.at_ms), 'Old SDK request completed before cancellation; invalid sample');
    const interrupts = marks.filter(mark => mark.event === 'interrupt_requested' && mark.query === oldProcess);
    const acks = marks.filter(mark => mark.event === 'interrupt_ack' && mark.query === oldProcess);
    const interrupt = interrupts[0], ack = acks[0];
    if (interruptMode === 'reuse') {
      assert.equal(steered.row.spawn_count, 0, 'Reuse unexpectedly restarted the SDK child');
      assert.equal(marks.filter(mark => mark.event === 'query_construct' || mark.event === 'spawn').length, 0);
      assert.equal(observation.processes.length - 1, oldProcess, 'Reuse changed the owned child');
      assert.ok(!exit && !observation.processes[oldProcess].didExit, 'Reused child exited before cleanup');
      assert.equal(interrupts.length, 1, 'Missing or repeated SDK interrupt');
      assert.equal(acks.length, 1, 'Missing or repeated interrupt acknowledgment');
      assert.ok(interrupt.at_ms >= abort.at_ms && ack.at_ms >= interrupt.at_ms
        && ack.at_ms <= steered.firstText && ack.queued_count === 0, 'Invalid interrupt acknowledgment');
      const oldResult = marks.find(mark => mark.event === 'sdk_result' && mark.query === oldProcess);
      assert.ok(oldResult && oldResult.at_ms >= interrupt.at_ms && oldResult.at_ms < steered.firstText,
        'Old SDK result boundary missing before replacement text');
    } else {
      assert.ok(exit && exit.at_ms >= abort.at_ms, 'Old process exit not observed');
      assert.equal(steered.row.spawn_count, 1, 'Replacement did not restart the SDK child');
      const newSpawn = marks.find(mark => mark.event === 'spawn');
      assert.ok(newSpawn && newSpawn.at_ms >= exit.at_ms, 'Replacement spawned before old child exited');
    }
    emit({ kind: 'steering', replicate: i, history_kib: historyKiB, interrupt_mode: interruptMode,
      process_restarts: steered.row.spawn_count,
      cancel_to_resubmit_ms: steered.started - interrupted.canceledAt,
      cancel_to_replacement_first_text_ms: steered.firstText - interrupted.canceledAt,
      cancel_to_interrupt_ack_ms: ack ? ack.at_ms - interrupted.canceledAt : null,
      cancel_to_old_child_exit_ms: exit ? exit.at_ms - interrupted.canceledAt : null, matched: steered.row.matched });
    await clear(session);
  }
  passed = true;
} catch (error) {
  emit({ kind: 'failure', error_type: error.name, message: String(error.message).slice(0, 240) });
  process.exitCode = 1;
} finally {
  await endpoint.close();
  if (!observation.processes.every(child => child.didExit)) { passed = false; process.exitCode = 1; }
  for (const label of ['cold', 'reused', 'rebuilt', 'after_interrupt']) {
    const data = rows.filter(row => row.case === label && row.ttft_ms != null).map(row => row.ttft_ms).sort((a, b) => a - b);
    if (data.length) emit({ kind: 'summary', case: label, interrupt_mode: interruptMode, n: data.length, history_kib: historyKiB,
      median_ttft_ms: data[Math.floor(data.length / 2)], min_ttft_ms: data[0], max_ttft_ms: data.at(-1) });
  }
  emit({ kind: 'final', passed, interrupt_mode: interruptMode, processes_started: observation.processes.length,
    all_children_exited: observation.processes.every(child => child.didExit), credential_values_logged: false });
}
