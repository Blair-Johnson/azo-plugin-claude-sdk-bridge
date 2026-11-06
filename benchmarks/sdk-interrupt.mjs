import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import { query } from '@anthropic-ai/claude-agent-sdk';
import { parseArgs } from 'node:util';
import { rm } from 'node:fs/promises';
import { createWorkspace } from '../src/workspace.mjs';
import { childEnvironment, isolatedSettings, QueryProcess } from '../src/child.mjs';

const { values } = parseArgs({ options: { live: { type: 'boolean', default: false },
  replicates: { type: 'string', default: '3' } } });
if (!values.live) throw new Error('Live inference requires --live.');
const replicates = Number(values.replicates);
assert.ok(Number.isInteger(replicates) && replicates > 0 && replicates <= 10);
const emit = value => console.log(JSON.stringify(value));
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const queue = []; let wake, ended = false, turn;
const push = text => { queue.push({ type: 'user', message: { role: 'user', content: text },
  parent_tool_use_id: null, client_composed: true }); wake?.(); wake = null; };
async function* input() { while (!ended) { if (queue.length) yield queue.shift(); else await new Promise(resolve => { wake = resolve; }); } }
const cwd = await createWorkspace(), child = new QueryProcess(), controller = new AbortController();
const sdk = query({ prompt: input(), options: { cwd, model: 'sonnet', tools: [], allowedTools: [],
  mcpServers: {}, strictMcpConfig: true, settingSources: [], settings: { ...isolatedSettings },
  plugins: [], skills: [], hooks: {}, canUseTool: async () => ({ behavior: 'deny', message: 'No tools in benchmark.' }),
  systemPrompt: { type: 'preset', preset: 'claude_code', append: 'Synthetic latency test. Follow the latest instruction exactly.' },
  env: childEnvironment(process.env, { CLAUDE_CODE_MAX_OUTPUT_TOKENS: '512', ENABLE_TOOL_SEARCH: 'false', CLAUDE_CODE_SIMPLE: undefined }),
  persistSession: false, includePartialMessages: true, thinking: { type: 'disabled' },
  spawnClaudeCodeProcess: child.spawn, abortController: controller } });
const pump = (async () => {
  for await (const message of sdk) {
    if (message.type === 'system' && message.subtype === 'init') emit({ kind: 'sdk_init', model: message.model });
    if (!turn) continue;
    if (message.type === 'assistant' && message.aborted) turn.aborted_assistants++;
    const event = message.type === 'stream_event' ? message.event : null;
    if (event?.type === 'content_block_delta' && event.delta?.type === 'text_delta' && event.delta.text) {
      turn.first ??= performance.now(); turn.text += event.delta.text;
      if (turn.interrupt && !turn.interruptAt) {
        const target = turn; target.interruptAt = performance.now();
        target.receipt = sdk.interrupt().then(receipt => { target.ack = performance.now(); target.queued = receipt?.still_queued?.length ?? null; });
        target.receipt.catch(error => target.done.reject(error));
      }
    }
    if (message.type === 'result') { turn.resultAt = performance.now(); turn.result = message.subtype; turn.done.resolve(); }
  }
})().catch(error => turn?.done.reject(error));
const timeout = setTimeout(() => { turn?.done.reject(new Error('Benchmark timeout')); child.shutdown(sdk, controller); }, 90000);
let passed = false;
try {
  emit({ kind: 'config', live: true, sdk: '0.3.308', replicates, thinking: 'disabled', tools: 0, max_tokens: 512 });
  for (let i = 0; i < replicates; i++) {
    turn = { done: deferred(), text: '', interrupt: true, aborted_assistants: 0 };
    push('Count from 1 to 500, one integer per line, without commentary.');
    await turn.done.promise;
    assert.ok(turn.interruptAt, 'Old response ended before interruption');
    await turn.receipt;
    const interrupted = turn;
    turn = { done: deferred(), text: '', interrupt: false, aborted_assistants: 0 };
    const submitted = performance.now();
    push(`Cancel the counting request. Reply exactly SDK_STEER_${i} and nothing else.`);
    await turn.done.promise;
    const matched = turn.text.trim() === `SDK_STEER_${i}`;
    emit({ kind: 'sdk_interrupt', replicate: i, matched, same_process_alive: !child.didExit,
      interrupt_ack_ms: interrupted.ack - interrupted.interruptAt,
      interrupt_to_old_result_ms: interrupted.resultAt - interrupted.interruptAt,
      interrupt_to_resubmit_ms: submitted - interrupted.interruptAt,
      interrupt_to_first_text_ms: turn.first - interrupted.interruptAt,
      replacement_ttft_ms: turn.first - submitted, interrupt_to_final_ms: turn.resultAt - interrupted.interruptAt,
      still_queued: interrupted.queued, aborted_assistant_events: interrupted.aborted_assistants,
      old_partial_chars: interrupted.text.length, old_result: interrupted.result, new_result: turn.result });
    assert.ok(matched && !child.didExit);
  }
  passed = true;
} catch (error) { emit({ kind: 'failure', error_type: error.name }); process.exitCode = 1; }
finally {
  clearTimeout(timeout); ended = true; wake?.();
  await child.shutdown(sdk, controller); await pump;
  if (child.didExit) await rm(cwd, { recursive: true, force: true });
  if (!child.didExit) { passed = false; process.exitCode = 1; }
  emit({ kind: 'final', passed, child_exited: child.didExit, credential_values_logged: false });
}
