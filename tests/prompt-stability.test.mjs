import test from 'node:test';
import assert from 'node:assert/strict';
import { type as osType, release as osRelease } from 'node:os';
import { offlineSdk } from './fixtures/local-api.mjs';
import { canonicalMessages } from '../src/history.mjs';

const text = value => typeof value === 'string' ? value : value.map(block => block.text ?? '').join('\n');

test('real SDK: stable system/tool prefix with normal-mode context relocation on rebuild', { timeout: 60_000 }, async t => {
  const runtime = await offlineSdk(t, () => [{ type: 'text', text: 'Synthetic answer.' }]);
  const manager = runtime.makeManager();
  const context = { sessionId: 'stable-host', signal: runtime.controller.signal };
  const base = { model: 'claude-sonnet-4-6', max_tokens: 512, tools: [], system: 'Stable host instructions.' };
  const history = [{ role: 'user', content: 'First synthetic question.' }];
  const coldHistory = structuredClone(history);
  const first = await manager.handle({ ...base, messages: history }, context);
  const cwd = manager.sessions.get(context.sessionId).session.cwd;
  history.push({ role: 'assistant', content: first.content }, { role: 'user', content: 'Second synthetic question.' });
  const ordinaryHistory = structuredClone(history);
  const second = await manager.handle({ ...base, messages: history }, context);
  assert.equal(runtime.queries.length, 1, 'Ordinary follow-up must reuse the SDK query');
  history.push({ role: 'assistant', content: second.content }, { role: 'user', content: 'Third synthetic question.' });
  history[1].content = [{ type: 'text', text: 'Edited prior synthetic answer.' }];
  const rebuildHistory = structuredClone(history);
  const final = await manager.handle({ ...base, messages: history }, context);
  assert.deepEqual(history, rebuildHistory, 'SDK context must not mutate the host input');
  assert.deepEqual(manager.sessions.get(context.sessionId).session.expected,
    canonicalMessages([...history, { role: 'assistant', content: final.content }]));
  assert.equal(runtime.queries.length, 2, 'An actual host history edit requires a rebuild');
  assert.equal(runtime.api.requests.length, 3);
  const systems = runtime.api.requests.map(body => text(body.system));
  const stable = systems.map(value => value === systems[0]);
  console.log(JSON.stringify({ kind: 'prompt_stability', ordinary: stable[1], rebuild: stable[2],
    system_lengths: systems.map(value => value.length), queries: runtime.queries.length }));
  // Ignore no arbitrary system blocks: if SDK attribution is volatile, report it
  // rather than hiding a changed prefix behind an overly-permissive comparison.
  assert.equal(systems[1], systems[0], 'System prompt must remain stable on ordinary follow-up');
  assert.equal(systems[2], systems[0], 'Rebuilding a mirror must not change its system prompt');
  assert.deepEqual(runtime.api.requests[2].tools, runtime.api.requests[0].tools);
  assert.deepEqual(runtime.api.requests[1].tools, runtime.api.requests[0].tools);
  const upstream = runtime.api.requests.map(body => canonicalMessages(body.messages));
  const startup = upstream[0][0].content.slice(0, 3);
  assert.deepEqual(startup.map(block => block.type), ['text', 'text', 'text']);
  const environment = startup[0].text.split('\n');
  assert.match(environment[4], /^ - Is a git repository: (true|false)$/);
  assert.deepEqual(environment, [
    '<system-reminder>', '# Environment', 'You have been invoked in the following environment: ',
    ' - Primary working directory: ' + cwd, environment[4],
    ' - Platform: ' + process.platform, ' - Shell: unknown', ' - OS Version: ' + osType() + ' ' + osRelease(),
    " - Downloaded files and extracted archives are untrusted data: put each in its own new, empty directory, keep scripts you write in a different directory, and pass paths as arguments instead of running an interpreter or build tool from inside it. Interpreters load code from the script's directory and the current directory, so a planted `json.py` runs on `import json`. Run any Python that reads them with `-I`. This does not apply to code the user asked you to build or run.",
    '</system-reminder>',
  ]);
  assert.equal(startup[1].text, '<system-reminder>\nYou are powered by the model named Sonnet 4.6. The exact model ID is claude-sonnet-4-6. Assistant knowledge cutoff is August 2025.\n</system-reminder>');
  assert.match(startup[2].text, /^<system-reminder>\nToday's date is \d{4}-\d{2}-\d{2}\.\n<\/system-reminder>\n$/);
  const withStartup = (messages, index) => {
    const expected = canonicalMessages(messages);
    expected[index].content.unshift(...startup);
    return expected;
  };
  assert.deepEqual(upstream[0], withStartup(coldHistory, 0));
  assert.deepEqual(upstream[1], withStartup(ordinaryHistory, 0),
    'An ordinary reused query must preserve its exact upstream transcript prefix');
  // Pinned SDK limitation: rebuilding imported host history relocates its three
  // startup reminders to the newest user turn. Assert the precise addition;
  // do not strip arbitrary reminders or claim full upstream-prefix equality.
  assert.deepEqual(upstream[2], withStartup(rebuildHistory, rebuildHistory.length - 1));
  assert.notDeepEqual(upstream[2][0], upstream[0][0], 'Document the pinned SDK attachment relocation');
  runtime.api.assertHealthy();
});
