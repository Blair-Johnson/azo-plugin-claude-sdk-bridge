import test from 'node:test';
import assert from 'node:assert/strict';
import { type as osType, release as osRelease } from 'node:os';
import { canonicalMessages } from '../src/history.mjs';
import { offlineSdk, until } from './fixtures/local-api.mjs';

const SYSTEM = 'OFFLINE_ONLY_SYSTEM: preserve the host instruction verbatim.';
const FIRST = 'OFFLINE_ONLY_USER: request two identical probes; the host supplies their results.';
const FOLLOWUP = 'OFFLINE_ONLY_FOLLOWUP: summarize the previous synthetic results.';
const PNG = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=';
const IDS = ['toolu_offline_first', 'toolu_offline_second'];
const INPUT = { label: 'synthetic-identical-input' };
const TOOLS = [
  {
    name: 'offline_probe', description: 'OFFLINE_ONLY_PROBE',
    input_schema: { type: 'object', properties: { label: { type: 'string', minLength: 1 } }, required: ['label'], additionalProperties: false },
  },
  {
    name: 'offline_unused', description: 'OFFLINE_ONLY_UNUSED',
    input_schema: { type: 'object', properties: {}, additionalProperties: false },
  },
];
const asMessage = ({ role, content }) => ({ role, content });
const blocks = body => body.messages.flatMap(message => Array.isArray(message.content) ? message.content : []);
const textOf = value => typeof value === 'string' ? value : (value ?? []).filter(block => block.type === 'text').map(block => block.text).join('\n');

// No opt-in and no account required: every model response comes from loopback.
// Run: node --test tests/sdk-local.test.mjs
// The CLI is a real SDK-owned child, never a local logged-in `claude` command.
test('real SDK: parallel parked MCP metadata, host results, and history reconstruction stay offline', { timeout: 60_000 }, async t => {
  let sdkToolName;
  const { api, controller, makeManager, processes, queries, initTools, callbacks, settled, factoryOptions } =
    await offlineSdk(t, (body, index) => {
    assert.equal(body.model, 'claude-sonnet-4-6');
    assert.ok(textOf(body.system).includes(SYSTEM), 'Host system prompt must survive SDK translation');
    assert.equal(body.tools.length, TOOLS.length, 'No built-in or external MCP tools may be advertised');
    assert.ok(body.tools.every(tool => tool.name.startsWith('mcp__azo__azo_')));
    assert.deepEqual(body.tools.map(tool => tool.description).sort(), TOOLS.map(tool => tool.description).sort());
    const probe = body.tools.find(tool => tool.description === TOOLS[0].description);
    assert.deepEqual(probe.input_schema, TOOLS[0].input_schema, 'Raw host JSON schema must survive MCP');
    sdkToolName ??= probe.name;
    assert.equal(probe.name, sdkToolName, 'Tool encoding must be stable across reconstructed sessions');
    assert.ok(JSON.stringify(body.messages).includes(FIRST), 'Authoritative first user turn must remain in history');
    if (index === 0) {
      assert.equal(blocks(body).filter(block => block.type === 'tool_result').length, 0);
      return [
        { type: 'text', text: 'Two synthetic probes are needed.' },
        ...IDS.map(id => ({ type: 'tool_use', id, name: sdkToolName, input: INPUT })),
      ];
    }
    assert.ok(index <= 2, 'Unexpected extra model inference');
    const results = blocks(body).filter(block => block.type === 'tool_result');
    assert.deepEqual(results.map(result => result.tool_use_id).sort(), [...IDS].sort());
    const first = results.find(result => result.tool_use_id === IDS[0]);
    const second = results.find(result => result.tool_use_id === IDS[1]);
    assert.ok(textOf(first.content).includes('FIRST_HOST_RESULT'));
    assert.ok(textOf(second.content).includes('SECOND_HOST_RESULT'));
    assert.equal(first.is_error ?? false, false);
    const image = (Array.isArray(second.content) ? second.content : []).find(block => block.type === 'image');
    assert.ok(image, 'Host image result must reach the API under its own tool-use ID');
    assert.equal(image.source.type, 'base64');
    assert.equal(image.source.media_type, 'image/png');
    assert.equal(image.source.data, PNG);
    const uses = blocks(body).filter(block => block.type === 'tool_use');
    assert.deepEqual(uses.map(({ id, name, input }) => ({ id, name, input })), IDS.map(id => ({ id, name: sdkToolName, input: INPUT })));
    if (index === 2) {
      assert.ok(JSON.stringify(body.messages).includes('OFFLINE_FINAL_REPLY'));
      assert.ok(JSON.stringify(body.messages).includes(FOLLOWUP));
    }
    return [{ type: 'text', text: index === 1 ? 'OFFLINE_FINAL_REPLY' : 'OFFLINE_REBUILT_REPLY' }];
  });

  let manager = makeManager();
  const base = { model: 'claude-sonnet-4-6', system: SYSTEM, tools: TOOLS, max_tokens: 512, stream: true };
  const history = [{ role: 'user', content: FIRST }];
  const events = [];
  const context = { sessionId: 'offline-host-session', signal: controller.signal, onEvent: event => events.push(event) };
  const handle = body => manager.handle(body, context).catch(error => {
    api.assertHealthy();
    throw error;
  });
  const first = await handle({ ...base, messages: history });
  api.assertHealthy();
  assert.equal(first.stop_reason, 'tool_use', 'Host must receive the batch before supplying any MCP results');
  assert.deepEqual(first.content.filter(block => block.type === 'tool_use'), IDS.map(id => ({ type: 'tool_use', id, name: 'offline_probe', input: INPUT })));
  // The model emits parallel uses, but the CLI may serialize MCP handlers
  // without readOnlyHint. The bridge must support both parked and queued results.
  await until(() => callbacks.length > 0, { signal: controller.signal, message: 'A real MCP handler must enter before results are supplied' });
  assert.ok(callbacks.every(call => IDS.includes(call._meta?.['claudecode/toolUseId'])));
  assert.deepEqual(settled, [], 'The real MCP handlers must remain parked');
  assert.equal(api.requests.length, 1, 'SDK must not continue inference without host results');
  assert.equal(initTools.length, 1);
  assert.deepEqual([...initTools[0]].sort(), api.requests[0].tools.map(tool => tool.name).sort());
  const streamedUses = events.filter(event => event.type === 'content_block_start' && event.content_block.type === 'tool_use');
  assert.deepEqual(streamedUses.map(event => event.content_block.name), ['offline_probe', 'offline_probe']);

  history.push(asMessage(first), { role: 'user', content: [
    // Deliberately reverse result order; IDs, not callback/array ordering, bind.
    { type: 'tool_result', tool_use_id: IDS[1], content: [
      { type: 'text', text: 'SECOND_HOST_RESULT' },
      { type: 'image', source: { type: 'base64', media_type: 'image/png', data: PNG } },
    ] },
    { type: 'tool_result', tool_use_id: IDS[0], content: 'FIRST_HOST_RESULT' },
  ] });
  const second = await handle({ ...base, messages: history });
  api.assertHealthy();
  assert.equal(second.stop_reason, 'end_turn');
  assert.equal(textOf(second.content), 'OFFLINE_FINAL_REPLY');
  assert.deepEqual([...settled].sort(), [...IDS].sort());
  assert.deepEqual(callbacks.map(call => call._meta?.['claudecode/toolUseId']).sort(), [...IDS].sort());
  assert.ok(callbacks.every(call => `mcp__azo__${call.name}` === sdkToolName));
  assert.ok(callbacks.every(call => JSON.stringify(call.arguments) === JSON.stringify(INPUT)));
  assert.equal(queries.length, 1, 'Host tool results must resume the parked query, not start a new query');
  assert.equal(callbacks.length, 2, 'No host tool may be re-executed');

  history.push(asMessage(second), { role: 'user', content: FOLLOWUP });
  await manager.close();
  manager = makeManager();
  const third = await handle({ ...base, messages: history });
  api.assertHealthy();
  assert.equal(third.stop_reason, 'end_turn');
  assert.equal(textOf(third.content), 'OFFLINE_REBUILT_REPLY');
  assert.equal(queries.length, 2);
  assert.equal(typeof factoryOptions[1].resume, 'string', 'Full history must be imported and resumed');
  assert.notEqual(factoryOptions[1].resume, factoryOptions[0].sessionId);
  assert.equal(callbacks.length, 2, 'Reconstructed completed tools must not execute again');
  assert.equal(api.requests.length, 3);
  assert.equal(initTools.length, 2);
  assert.deepEqual([...initTools[1]].sort(), [...initTools[0]].sort());
  await manager.close();
  assert.ok(processes.every(process => process.didExit));
  api.assertHealthy();
});

const STEERING = 'OFFLINE_STEERING: use the real host results and do not run either probe again.';
const COMPACTED = 'OFFLINE_COMPACTED_SUMMARY: the host retained the pending synthetic probe batch.';
const CHANGED_TOOLS = [{
  name: 'offline_unused', description: 'OFFLINE_ONLY_CURRENT_SCHEMA',
  input_schema: { type: 'object', properties: { current: { type: 'boolean' } }, required: ['current'], additionalProperties: false },
}];

// Agent Zoo appends interrupt/wake notices after a closing result batch. Those
// must continue the parked query: a rebuild relocates the SDK startup context
// into older turns and invalidates the upstream prompt cache.
test('real SDK: parked results plus trailing text continue the query with a stable upstream prefix', { timeout: 60_000 }, async t => {
  let sdkToolName;
  const runtime = await offlineSdk(t, (body, index) => {
    if (index === 0) {
      sdkToolName = body.tools.find(tool => tool.description === TOOLS[0].description).name;
      return IDS.map(id => ({ type: 'tool_use', id, name: sdkToolName, input: INPUT }));
    }
    return [{ type: 'text', text: index === 1 ? 'OFFLINE_FOLDED_REPLY' : 'OFFLINE_ORDINARY_REPLY' }];
  });
  const { api, controller, makeManager, queries, callbacks, settled } = runtime;
  const manager = makeManager();
  const base = { model: 'claude-sonnet-4-6', system: SYSTEM, tools: TOOLS, max_tokens: 512, stream: true };
  const context = { sessionId: 'offline-folded-steering', signal: controller.signal };
  const history = [{ role: 'user', content: FIRST }];
  const first = await manager.handle({ ...base, messages: history }, context);
  assert.equal(first.stop_reason, 'tool_use');
  await until(() => callbacks.length > 0, { signal: controller.signal });

  history.push(asMessage(first), { role: 'user', content: [
    { type: 'tool_result', tool_use_id: IDS[1], content: 'SECOND_HOST_RESULT' },
    { type: 'tool_result', tool_use_id: IDS[0], content: 'FIRST_HOST_RESULT' },
    { type: 'text', text: STEERING },
  ] });
  const original = structuredClone(history);
  const second = await manager.handle({ ...base, messages: history }, context);
  assert.deepEqual(history, original, 'Folding must not alter host input history');
  assert.equal(textOf(second.content), 'OFFLINE_FOLDED_REPLY');
  assert.equal(queries.length, 1, 'Results plus trailing text must continue the parked query, not rebuild');
  assert.deepEqual([...settled].sort(), [...IDS].sort());
  assert.equal(callbacks.length, 2, 'No host tool may be re-executed');
  assert.deepEqual(manager.sessions.get(context.sessionId).session.expected,
    canonicalMessages([...history, asMessage(second)]), 'Host ledger keeps the unfolded host form');

  const [cold, folded] = api.requests.map(body => canonicalMessages(body.messages));
  assert.deepEqual(folded.slice(0, cold.length), cold, 'Upstream prefix (including SDK startup context) must be unchanged');
  const results = folded.at(-1).content.filter(block => block.type === 'tool_result');
  assert.deepEqual(results.map(block => block.tool_use_id).sort(), [...IDS].sort());
  const carrier = results.find(block => textOf(block.content).includes(STEERING));
  assert.ok(carrier, 'Trailing host text must reach the model inside the batch');
  assert.equal(carrier.tool_use_id, IDS[0], 'Trailing text rides on the last host result');
  assert.ok(textOf(carrier.content).includes('FIRST_HOST_RESULT'));
  assert.ok(!folded.at(-1).content.some(block => block.type === 'text' && block.text.includes('<system-reminder>')),
    'Continuation must not re-attach SDK startup context');

  history.push(asMessage(second), { role: 'user', content: FOLLOWUP });
  const third = await manager.handle({ ...base, messages: history }, context);
  assert.equal(textOf(third.content), 'OFFLINE_ORDINARY_REPLY');
  assert.equal(queries.length, 1, 'Ordinary follow-up after folding must keep reusing the query');
  const ordinary = canonicalMessages(api.requests[2].messages);
  assert.deepEqual(ordinary.slice(0, folded.length), folded, 'Folded turn must stay stable for later cache reads');
  await manager.close();
  api.assertHealthy();
});

function apiHistory(history) {
  // Normalize transport hints, text-block equivalence, MCP aliases, and the
  // API's adjacent-user grouping only. No content/assistant turns disappear.
  const grouped = [];
  for (const message of canonicalMessages(history)) {
    const mapped = { ...message, content: message.content.map(block => block.type === 'tool_use'
      ? { ...block, name: `mcp__azo__azo_${block.name}` } : block) };
    if (mapped.role === 'user' && grouped.at(-1)?.role === 'user') grouped.at(-1).content.push(...mapped.content);
    else grouped.push(mapped);
  }
  return grouped;
}

for (const scenario of [
  { name: 'restart with an unresolved parallel batch', restart: true },
  { name: 'restart with actual results and steering text', restart: true, steering: true },
  { name: 'restart with results split over adjacent user records', restart: true, split: true },
  { name: 'restart with split results and same-record steering', restart: true, split: true, steering: true },
  { name: 'restart with split results and separate steering records', restart: true, split: true, steering: true, separate: true },
  { name: 'restart after host compaction with pending results', restart: true, compacted: true },
  { name: 'restart with a removed historical tool and changed current schema', restart: true, changed: true },
]) {
  test(`real SDK: ${scenario.name}`, { timeout: 60_000 }, async t => {
    let currentTools = TOOLS;
    let expectedHistory;
    let sdkToolName;
    let coldCwd;
    const runtime = await offlineSdk(t, (body, index) => {
      assert.equal(body.model, 'claude-sonnet-4-6');
      assert.ok(textOf(body.system).includes(SYSTEM));
      assert.deepEqual(body.tools.map(tool => ({
        name: tool.name, description: tool.description, input_schema: tool.input_schema,
      })).sort((a, b) => a.name.localeCompare(b.name)), currentTools.map(tool => ({
        ...tool, name: `mcp__azo__azo_${tool.name}`,
      })).sort((a, b) => a.name.localeCompare(b.name)), 'Only the current tools and exact current schemas may be advertised');
      if (index === 0) {
        coldCwd = manager.sessions.get(context.sessionId).session.cwd;
        sdkToolName = body.tools.find(tool => tool.description === TOOLS[0].description).name;
        return [
          { type: 'text', text: 'The host must supply both pending results.' },
          ...IDS.map(id => ({ type: 'tool_use', id, name: sdkToolName, input: INPUT })),
        ];
      }
      assert.equal(index, 1, 'A restart must not perform extra inference or repeat a historical action');
      assert.ok(expectedHistory, 'Host history must be fixed before the resumed request');
      const expectedApi = apiHistory(expectedHistory);
      // SDK 0.3.308 normal mode attaches startup context inside the last
      // imported tool result. Model that exact addition, not a reminder filter:
      // every host block (including its whitespace) is still compared below.
      const startup = canonicalMessages([runtime.api.requests[0].messages[0]])[0].content.slice(0, 3);
      assert.deepEqual(startup.map(block => block.type), ['text', 'text', 'text']);
      const environment = startup[0].text.split('\n');
      assert.match(environment[4], /^ - Is a git repository: (true|false)$/);
      assert.deepEqual(environment, [
        '<system-reminder>', '# Environment', 'You have been invoked in the following environment: ',
        ' - Primary working directory: ' + coldCwd, environment[4],
        ' - Platform: ' + process.platform, ' - Shell: unknown', ' - OS Version: ' + osType() + ' ' + osRelease(),
        " - Downloaded files and extracted archives are untrusted data: put each in its own new, empty directory, keep scripts you write in a different directory, and pass paths as arguments instead of running an interpreter or build tool from inside it. Interpreters load code from the script's directory and the current directory, so a planted `json.py` runs on `import json`. Run any Python that reads them with `-I`. This does not apply to code the user asked you to build or run.",
        '</system-reminder>',
      ]);
      assert.equal(startup[1].text, '<system-reminder>\nYou are powered by the model named Sonnet 4.6. The exact model ID is claude-sonnet-4-6. Assistant knowledge cutoff is August 2025.\n</system-reminder>');
      assert.match(startup[2].text, /^<system-reminder>\nToday's date is \d{4}-\d{2}-\d{2}\.\n<\/system-reminder>\n$/);
      assert.deepEqual(canonicalMessages([runtime.api.requests[0].messages[0]])[0].content.slice(3),
        [{ type: 'text', text: FIRST }], 'Cold context must not hide or change host content');
      const attached = startup.map((block, i) => ({ ...block, text: i === 0
        ? block.text.replace(coldCwd, manager.sessions.get(context.sessionId).session.cwd) + '\n'
        : i === 1 ? block.text + '\n' : block.text.slice(0, -1) }));
      const lastResult = expectedApi.at(-1).content.findLast(block => block.type === 'tool_result');
      assert.deepEqual(lastResult.content, [{ type: 'text', text: 'FIRST_ACTUAL_HOST_RESULT' }]);
      lastResult.content[0].text += '\n'; // SDK separator before its context, not host text trimming.
      lastResult.content.push(...attached);
      assert.deepEqual(canonicalMessages(body.messages), expectedApi,
        'Resumed API history must preserve every host block plus only the exact SDK startup context');
      const uses = blocks(body).filter(block => block.type === 'tool_use');
      assert.deepEqual(uses.map(({ id, name, input }) => ({ id, name, input })),
        IDS.map(id => ({ id, name: sdkToolName, input: INPUT })));
      if (scenario.changed) {
        assert.ok(!body.tools.some(tool => tool.name === sdkToolName), 'Historical aliases must not register removed tools');
        assert.ok(uses.every(tool => tool.name === 'mcp__azo__azo_offline_probe'), 'Unavailable historical tool names must remain readable');
      }
      return [{ type: 'text', text: 'OFFLINE_PENDING_RESTART_COMPLETE' }];
    }, { managerOptions: { systemPromptMode: 'custom' } });
    const { api, controller, makeManager, callbacks, callbackQueries, queries, processes, initTools, factoryOptions } = runtime;
    let manager = makeManager();
    const base = { model: 'claude-sonnet-4-6', system: SYSTEM, tools: TOOLS, max_tokens: 512, stream: true };
    const context = { sessionId: 'offline-pending-restart', signal: controller.signal };
    const handle = body => manager.handle(body, context).catch(error => {
      api.assertHealthy();
      throw error;
    });
    const first = await handle({ ...base, messages: [{ role: 'user', content: FIRST }] });
    assert.equal(first.stop_reason, 'tool_use');
    assert.deepEqual(first.content.filter(block => block.type === 'tool_use'),
      IDS.map(id => ({ type: 'tool_use', id, name: 'offline_probe', input: INPUT })));
    await until(() => callbacks.length > 0, { signal: controller.signal, message: 'Real MCP callback must park before restart' });
    assert.deepEqual(runtime.settled, [], 'No host result exists before the restart boundary');
    assert.equal(api.requests.length, 1);
    assert.equal(queries.length, 1);
    if (scenario.restart) {
      await manager.close();
      assert.ok(processes.every(process => process.didExit), 'Old SDK child must exit before fresh manager creation');
      assert.equal(api.requests.length, 1, 'Closing parked callbacks must not trigger inference');
      api.assertHealthy();
      manager = makeManager();
    }
    if (scenario.changed) currentTools = CHANGED_TOOLS;
    // These are actual host outcomes, not results fabricated to repair history.
    // Reverse IDs and include an image to catch positional or lossy matching.
    expectedHistory = [
      { role: 'user', content: scenario.compacted ? COMPACTED : FIRST },
      asMessage(first),
      { role: 'user', content: [
        { type: 'tool_result', tool_use_id: IDS[1], is_error: false, content: [
          { type: 'text', text: 'SECOND_ACTUAL_HOST_RESULT' },
          { type: 'image', source: { type: 'base64', media_type: 'image/png', data: PNG } },
        ] },
        { type: 'tool_result', tool_use_id: IDS[0], is_error: false, content: 'FIRST_ACTUAL_HOST_RESULT' },
        ...(scenario.steering ? [{ type: 'text', text: STEERING }] : []),
      ] },
    ];
    if (scenario.split) {
      const content = expectedHistory.pop().content;
      expectedHistory.push({ role: 'user', content: content.slice(0, 1) },
        { role: 'user', content: content.slice(1, scenario.separate ? 2 : undefined) });
      if (scenario.separate) expectedHistory.push({ role: 'user', content: content.slice(2) });
    }
    const original = structuredClone(expectedHistory);
    const final = await handle({ ...base, tools: currentTools, messages: expectedHistory });
    assert.deepEqual(expectedHistory, original, 'SDK-only opener must never alter host input history');
    assert.deepEqual(manager.sessions.get(context.sessionId).session.expected,
      canonicalMessages([...expectedHistory, asMessage(final)]), 'Host ledger must contain only real messages');
    api.assertHealthy();
    assert.equal(final.stop_reason, 'end_turn');
    assert.equal(textOf(final.content), 'OFFLINE_PENDING_RESTART_COMPLETE');
    assert.equal(queries.length, 2, 'Pending history must be reconstructed into exactly one new SDK query');
    assert.equal(typeof factoryOptions[1].resume, 'string');
    assert.notEqual(factoryOptions[1].resume, factoryOptions[0].sessionId);
    assert.ok(callbackQueries.every(index => index === 0), 'Imported historical calls must never enter the new MCP server');
    assert.equal(api.requests.length, 2);
    assert.equal(initTools.length, 2);
    assert.deepEqual([...initTools[1]].sort(), currentTools.map(tool => `mcp__azo__azo_${tool.name}`).sort(),
      'Real SDK init metadata must advertise only currently registered tools');
    await manager.close();
    assert.ok(processes.every(process => process.didExit));
    api.assertHealthy();
  });
}
