import test from 'node:test';
import assert from 'node:assert/strict';
import { offlineSdk } from './fixtures/local-api.mjs';
import { ToolBridge } from '../src/tools.mjs';

// Models sometimes call a tool they were never offered: a Claude Code built-in
// such as Bash, or a host tool under the wrong name. The real CLI answers such a
// call itself; the bridge must let the model continue instead of failing the turn.
const PROBE = { name: 'offline_probe', description: 'Synthetic probe; no host action executes',
  input_schema: { type: 'object', properties: {}, additionalProperties: false } };
const BASE = { model: 'claude-sonnet-4-6', max_tokens: 512, system: 'Synthetic offline unavailable-tool test.',
  thinking: { type: 'disabled' }, tools: [PROBE] };
const SESSION = 'offline-unavailable-tool';
const user = content => ({ role: 'user', content });
const asMessage = ({ role, content }) => ({ role, content });
const blocks = message => typeof message.content === 'string' ? [{ type: 'text', text: message.content }] : message.content;
const bash = id => ({ type: 'tool_use', id, name: 'Bash', input: { command: 'git clone https://example.com/repo' } });
const probe = body => ({ type: 'tool_use', id: 'toolu_probe', name: body.tools.find(t => t.name.includes('offline_probe')).name, input: {} });
const resultsIn = body => body.messages.flatMap(message => message.role === 'user' ? blocks(message) : [])
  .filter(block => block.type === 'tool_result');
const resultText = block => typeof block.content === 'string' ? block.content : JSON.stringify(block.content);

async function setup(t, reply) {
  const runtime = await offlineSdk({ after(close) { t.after(close); } }, reply);
  return { ...runtime, manager: runtime.makeManager() };
}

async function send(f, messages) {
  const events = [];
  const answer = await f.manager.handle({ ...BASE, messages }, { sessionId: SESSION, onEvent: event => events.push(event) });
  return { answer, events };
}

function assertStream(events, answer) {
  assert.equal(events.filter(e => e.type === 'message_start').length, 1, 'One host response');
  assert.equal(events.filter(e => e.type === 'message_stop').length, 1);
  const starts = events.filter(e => e.type === 'content_block_start').map(e => e.index);
  assert.deepEqual(starts, answer.content.map((_, i) => i), 'Contiguous transport indexes for every block');
  assert.ok(!JSON.stringify(events).includes('Bash'), 'No event reveals the unavailable call');
}

test('an unavailable tool is answered by the SDK and the model continues in one host response', { timeout: 45_000 }, async t => {
  const f = await setup(t, (body, index) => index === 0
    ? [{ type: 'text', text: 'Let me clone it.' }, bash('toolu_bash_1')]
    : [{ type: 'text', text: 'FINAL_ANSWER' }]);
  const { answer, events } = await send(f, [user('Clone the repository.')]);
  assert.deepEqual(answer.content, [{ type: 'text', text: 'Let me clone it.' }, { type: 'text', text: 'FINAL_ANSWER' }]);
  assert.equal(answer.stop_reason, 'end_turn');
  assertStream(events, answer);
  assert.deepEqual(f.callbacks, [], 'No host tool ran');

  // The CLI itself told the model that the tool does not exist.
  const [result] = resultsIn(f.api.requests[1]);
  assert.equal(result.tool_use_id, 'toolu_bash_1');
  assert.equal(result.is_error, true);
  assert.match(resultText(result), /No such tool available: Bash/);

  // The host history omits the call, and the same SDK process continues.
  await send(f, [user('Clone the repository.'), asMessage(answer), user('Thanks.')]);
  assert.equal(f.queries.length, 1);
  f.api.assertHealthy();
});

test('an unavailable call beside a host call: the host runs only its own tool', { timeout: 45_000 }, async t => {
  const f = await setup(t, (body, index) => index === 0
    ? [bash('toolu_bash_1'), probe(body)]
    : [{ type: 'text', text: 'FINAL_ANSWER' }]);
  const first = await send(f, [user('Clone, then probe.')]);
  assert.equal(first.answer.stop_reason, 'tool_use');
  assert.deepEqual(first.answer.content.map(b => [b.type, b.id]), [['tool_use', 'toolu_probe']]);
  assert.equal(first.answer.content[0].name, 'offline_probe');
  assertStream(first.events, first.answer);

  const history = [user('Clone, then probe.'), asMessage(first.answer),
    user([{ type: 'tool_result', tool_use_id: 'toolu_probe', content: 'PROBE_RESULT' }])];
  const second = await send(f, history);
  assert.deepEqual(second.answer.content, [{ type: 'text', text: 'FINAL_ANSWER' }]);
  assert.equal(f.queries.length, 1);
  const results = resultsIn(f.api.requests[1]);
  assert.deepEqual(results.map(b => b.tool_use_id).sort(), ['toolu_bash_1', 'toolu_probe']);
  assert.match(resultText(results.find(b => b.tool_use_id === 'toolu_bash_1')), /No such tool available: Bash/);
  f.api.assertHealthy();
});

test('repeated unavailable calls fail with the tool names instead of looping', { timeout: 45_000 }, async t => {
  const f = await setup(t, (body, index) => [bash(`toolu_bash_${index}`)]);
  let requests;
  await assert.rejects(send(f, [user('Clone the repository.')]).finally(() => { requests = f.api.requests.length; }),
    error => /unavailable_tool_loop/.test(error.publicMessage) && /9 times in one response: "Bash"/.test(error.publicMessage));
  // The instant synthetic API can serve the CLI's next request before shutdown.
  assert.ok(requests >= 9 && requests <= 10, `Failed after the ninth call, not later (${requests} requests)`);
  assert.deepEqual(f.callbacks, []);
});

test('unknown SDK tool names are reported, bounded and quoted', () => {
  const tools = new ToolBridge([PROBE]);
  assert.equal(tools.hasSdkName('mcp__azo__azo_offline_probe'), true);
  assert.equal(tools.hasSdkName('Bash'), false);
  assert.throws(() => tools.fromSdkName('mcp__azo__terminal_run'), /Unknown SDK tool name: "mcp__azo__terminal_run"\./);
  assert.throws(() => tools.fromSdkName(`x${'y'.repeat(200)}`),
    error => error.publicMessage === `Unknown SDK tool name: "x${'y'.repeat(79)}…".`);
});
