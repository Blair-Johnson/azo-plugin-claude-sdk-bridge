import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { childEnvironment, isolatedSettings, QueryProcess } from '../src/child.mjs';

function childStub({ exitOn } = {}) {
  const child = new EventEmitter();
  child.stderr = new EventEmitter();
  child.signals = [];
  child.kill = signal => {
    child.signals.push(signal);
    if (signal === exitOn) queueMicrotask(() => child.emit('close', 0, signal));
    return true;
  };
  return child;
}

test('child environment removes metered routing but leaves SDK-owned login alone', () => {
  const env = childEnvironment({ HOME: '/home/test', PATH: '/bin', CLAUDE_CONFIG_DIR: '/private/claude',
    CLAUDE_CODE_OAUTH_TOKEN: 'sdk-owned', ANTHROPIC_API_KEY: 'must-not-route', ANTHROPIC_AUTH_TOKEN: 'api-token',
    ANTHROPIC_BASE_URL: 'https://invalid.example', ANTHROPIC_CUSTOM_HEADERS: 'bad', CLAUDE_CODE_USE_BEDROCK: '1',
    AZO_CLAUDE_BRIDGE_KEY: 'local-key', AWS_ACCESS_KEY_ID: 'key', CLAUDECODE: '1', ENABLE_CLAUDEAI_MCP_SERVERS: '1', DISABLE_AUTO_COMPACT: '0' });
  assert.equal(env.ANTHROPIC_API_KEY, undefined);
  assert.equal(env.AZO_CLAUDE_BRIDGE_KEY, undefined);
  assert.equal(env.DISABLE_AUTOUPDATER, '1');
  assert.equal(env.CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC, '1');
  assert.equal(env.CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL, '1');
  assert.equal(isolatedSettings.autoMemoryEnabled, false);
  assert.equal(isolatedSettings.includeGitInstructions, false);
  assert.equal(env.ANTHROPIC_AUTH_TOKEN, undefined);
  assert.equal(env.ANTHROPIC_BASE_URL, undefined);
  assert.equal(env.ANTHROPIC_CUSTOM_HEADERS, undefined);
  assert.equal(env.CLAUDE_CODE_USE_BEDROCK, undefined);
  assert.equal(env.AWS_ACCESS_KEY_ID, undefined);
  assert.equal(env.CLAUDECODE, undefined);
  assert.equal(env.CLAUDE_CODE_OAUTH_TOKEN, 'sdk-owned');
  assert.equal(env.CLAUDE_CONFIG_DIR, '/private/claude');
  assert.equal(env.ENABLE_CLAUDEAI_MCP_SERVERS, '0');
  assert.equal(env.DISABLE_AUTO_COMPACT, '1');
  assert.equal(isolatedSettings.disableAllHooks, true);
  assert.deepEqual(isolatedSettings.enabledPlugins, {});
});

test('process drains stderr, observes actual close, and cannot spawn twice', async () => {
  const child = childStub({ exitOn: 'SIGTERM' });
  let spawnOptions;
  const process = new QueryProcess({ spawnFactory: (command, args, options) => {
    assert.equal(command, 'stub'); assert.deepEqual(args, ['a']); spawnOptions = options; return child;
  }, graceMs: 10, killMs: 10 });
  const signal = new AbortController().signal;
  const sdkEnv = { ANTHROPIC_API_KEY: 'synthetic-sdk-key', ANTHROPIC_BASE_URL: 'http://127.0.0.1:1',
    CLAUDE_CODE_ENTRYPOINT: 'sdk-generated', CLAUDE_CODE_SESSION_ID: 'sdk-session', CLAUDECODE: '1' };
  assert.equal(process.spawn({ command: 'stub', args: ['a'], cwd: '/private/test', env: sdkEnv, signal }), child);
  assert.equal(spawnOptions.signal, signal);
  assert.equal(spawnOptions.env, sdkEnv); // pass through unchanged, including SDK identity/routing
  assert.deepEqual(spawnOptions.stdio, ['pipe', 'pipe', 'pipe']);
  assert.equal(child.listenerCount('data'), 0);
  assert.equal(child.stderr.listenerCount('data'), 1);
  child.stderr.emit('data', Buffer.from('SECRET MUST NOT BE LOGGED'));
  child.emit('error', new Error('SECRET'));
  assert.throws(() => process.spawn({}), /already/);
  let closed = 0;
  assert.equal(await process.shutdown({ close() { closed++; } }, new AbortController()), true);
  assert.equal(process.didExit, true);
  assert.equal(closed, 1);
  assert.deepEqual(child.signals, ['SIGTERM']);
  await process.shutdown();
  assert.equal(closed, 1);
});

test('shutdown escalates to SIGKILL even when SDK close never resolves', async () => {
  const child = childStub({ exitOn: 'SIGKILL' });
  const process = new QueryProcess({ spawnFactory: () => child, graceMs: 5, killMs: 10 });
  process.spawn({ command: 'stub', args: [], env: {} });
  const controller = new AbortController();
  assert.equal(await process.shutdown({ close: () => new Promise(() => {}) }, controller), true);
  assert.equal(controller.signal.aborted, true);
  assert.deepEqual(child.signals, ['SIGTERM', 'SIGKILL']);
});

test('bounded shutdown never claims that a still-running child has exited', async () => {
  const child = childStub();
  const process = new QueryProcess({ spawnFactory: () => child, graceMs: 5, killMs: 5 });
  process.spawn({ command: 'stub', args: [], env: {} });
  assert.equal(await process.shutdown({ close() { throw new Error('secret'); } }), false);
  assert.equal(process.didExit, false);
  child.emit('close', 0, 'SIGKILL');
  await process.exited;
  assert.equal(process.didExit, true);
});

test('closing before deferred spawn seals the process hook', async () => {
  let spawned = false;
  const process = new QueryProcess({ spawnFactory: () => { spawned = true; return childStub(); } });
  assert.equal(await process.shutdown(), true);
  assert.throws(() => process.spawn({}), /closed/);
  assert.equal(spawned, false);
});
