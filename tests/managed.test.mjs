import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { copyFile, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

const entry = fileURLToPath(new URL('../src/managed.mjs', import.meta.url));
const key = randomBytes(32).toString('hex'); // Synthetic local bearer, never a credential.
const frame = overrides => JSON.stringify({ version: 1, key, port: 0, ...overrides }) + '\n';

function launch(t, script = entry, { detached = process.platform !== 'win32' } = {}) {
  const child = spawn(process.execPath, [script], {
    detached, stdio: ['pipe', 'pipe', 'pipe'], env: { PATH: process.env.PATH },
  });
  let stdout = '', stderr = '';
  let resolveReady, rejectReady;
  const ready = new Promise((resolve, reject) => { resolveReady = resolve; rejectReady = reject; });
  ready.catch(() => {});
  child.stdout.setEncoding('utf8').on('data', chunk => {
    stdout += chunk;
    if (stdout.includes('\n')) {
      try { resolveReady(JSON.parse(stdout.split('\n')[0])); }
      catch (error) { rejectReady(error); }
    }
  });
  child.stderr.setEncoding('utf8').on('data', chunk => { stderr += chunk; });
  child.stdin.on('error', () => {}); // Rejected frames may close before a pending write.
  const kill = () => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    // Test-created private groups only, while the owned leader is still live.
    if (detached && child.pid) {
      try { process.kill(-child.pid, 'SIGKILL'); } catch { /* already exited */ }
    } else child.kill('SIGKILL');
  };
  const watchdog = setTimeout(kill, 12000);
  const exited = new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => {
      clearTimeout(watchdog);
      rejectReady(new Error('Exited before readiness'));
      resolve({ code, signal, stdout, stderr });
    });
  });
  t.after(async () => { kill(); await exited; });
  return { child, ready, exited };
}

async function stopped(run, code = 0, ready = false) {
  const result = await run.exited;
  assert.equal(result.code, code);
  assert.equal(result.signal, null);
  assert.equal(result.stderr, '');
  assert.equal(result.stdout.includes(key), false);
  if (!ready) assert.equal(result.stdout, '');
  else assert.equal(result.stdout.trim().split('\n').length, 1);
  return result;
}

async function fixture(t, { engine = '', server = '' } = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'azo-managed-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  await copyFile(entry, join(dir, 'managed.mjs'));
  await writeFile(join(dir, 'engine.mjs'), engine || `
    import { writeFile } from 'node:fs/promises';
    export class BridgeManager {
      sessions = new Map();
      async close() { await writeFile(new URL('./closed', import.meta.url), 'closed'); }
    }
  `);
  await writeFile(join(dir, 'server.mjs'), server);
  return { dir, script: join(dir, 'managed.mjs') };
}

test('dynamic readiness, authenticated health, and owner EOF', { timeout: 15000 }, async t => {
  const run = launch(t);
  const control = frame({ ownedProcessGroup: process.platform !== 'win32' });
  run.child.stdin.write(control.slice(0, 25));
  run.child.stdin.write(control.slice(25));
  const ready = await run.ready;
  assert.deepEqual(Object.keys(ready).sort(), ['kind', 'pid', 'port', 'version']);
  assert.equal(ready.kind, 'ready');
  assert.equal(ready.version, 1);
  assert.equal(ready.pid, run.child.pid);
  assert.ok(Number.isInteger(ready.port) && ready.port > 0 && ready.port <= 65535);
  const url = `http://127.0.0.1:${ready.port}/health`;
  for (const [headers, status] of [[{}, 401], [{ 'x-api-key': 'wrong' }, 401], [{ 'x-api-key': key }, 200]]) {
    const response = await fetch(url, { headers, signal: AbortSignal.timeout(2000) });
    assert.equal(response.status, status);
    const body = await response.json();
    if (status === 200) assert.equal(body.status, 'ok');
  }
  assert.equal(run.child.exitCode, null); // stdin is still the live ownership lease.
  run.child.stdin.end();
  await stopped(run, 0, true);
  await assert.rejects(fetch(url, { signal: AbortSignal.timeout(2000) }));
});

for (const signal of ['SIGTERM', 'SIGINT']) {
  test(`${signal} closes a manually launched bridge without signaling its parent group`, async t => {
    const run = launch(t, entry, { detached: false });
    run.child.stdin.write(frame({ systemPromptMode: 'custom', interruptMode: 'rebuild' }));
    await run.ready;
    run.child.kill(signal);
    await stopped(run, 0, true);
  });
}

test('EOF before any control frame exits quietly', async t => {
  const run = launch(t);
  run.child.stdin.end();
  await stopped(run);
});

const invalidFrames = [
  ['invalid JSON', '{private malformed payload}\n'],
  ['null', 'null\n'], ['array', '[]\n'],
  ['version', frame({ version: 2 })], ['missing key', '{"version":1,"port":0}\n'],
  ['short key', frame({ key: 'a'.repeat(63) })], ['nonhex key', frame({ key: 'z'.repeat(64) })],
  ['port', frame({ port: 1234 })], ['string port', frame({ port: '0' })],
  ['prompt mode', frame({ systemPromptMode: 'invalid' })],
  ['interrupt mode', frame({ interruptMode: null })],
  ['unknown field', frame({ host: '0.0.0.0' })],
  ['private-group flag type', frame({ ownedProcessGroup: 'true' })],
  ['multiple frames', frame() + frame()],
  ['oversized terminated frame', ' '.repeat(4097) + '\n'],
  ['oversized unterminated frame', ' '.repeat(4097)],
];
for (const [name, input] of invalidFrames) {
  test(`rejects ${name} without logging control data`, async t => {
    const run = launch(t);
    run.child.stdin.write(input); // Rejection must not depend on EOF.
    await stopped(run, 1);
  });
}

test('rejects a later second frame and closes the live endpoint', async t => {
  const run = launch(t);
  run.child.stdin.write(frame());
  await run.ready;
  run.child.stdin.write(frame());
  await stopped(run, 1, true);
});

async function waitForFile(path) {
  for (let attempt = 0; attempt < 400; attempt++) {
    try { return await readFile(path, 'utf8'); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    await delay(10);
  }
  throw new Error('Fixture did not reach startup checkpoint');
}

for (const phase of ['construction', 'listen']) {
  test(`startup ${phase} failure closes the manager without leaking the raw error`, async t => {
    const setup = await fixture(t, { server: `
      import { EventEmitter } from 'node:events';
      export function createBridgeServer({ manager }) {
        if (${JSON.stringify(phase)} === 'construction') throw new Error('PRIVATE SDK ERROR');
        return { server: new EventEmitter(),
          async listen() { throw new Error('PRIVATE SDK ERROR'); },
          async close() { await manager.close(); }
        };
      }
    ` });
    const run = launch(t, setup.script);
    run.child.stdin.write(frame());
    await stopped(run, 1);
    assert.equal(await readFile(join(setup.dir, 'closed'), 'utf8'), 'closed');
  });
}

test('EOF during an async import never constructs a manager or publishes readiness', async t => {
  const setup = await fixture(t, { engine: `
    import { writeFile } from 'node:fs/promises';
    await writeFile(new URL('./started', import.meta.url), 'started');
    await new Promise(resolve => setTimeout(resolve, 250));
    export class BridgeManager { constructor() { throw new Error('Must not construct'); } }
  `, server: 'export function createBridgeServer() { throw new Error("Must not start"); }' });
  const run = launch(t, setup.script);
  run.child.stdin.write(frame());
  await waitForFile(join(setup.dir, 'started'));
  run.child.stdin.end();
  await stopped(run);
});

test('EOF during listen waits for startup and closes the endpoint and manager', async t => {
  const setup = await fixture(t, { server: `
    import { EventEmitter } from 'node:events';
    import { writeFile } from 'node:fs/promises';
    export function createBridgeServer({ manager }) {
      return { server: new EventEmitter(),
        async listen() {
          await writeFile(new URL('./started', import.meta.url), 'started');
          await new Promise(resolve => setTimeout(resolve, 250));
          return { port: 12345 };
        },
        async close() {
          await writeFile(new URL('./endpoint-closed', import.meta.url), 'closed');
          await manager.close();
        }
      };
    }
  ` });
  const run = launch(t, setup.script);
  run.child.stdin.write(frame());
  await waitForFile(join(setup.dir, 'started'));
  run.child.stdin.end();
  await stopped(run);
  for (const file of ['closed', 'endpoint-closed']) {
    assert.equal(await readFile(join(setup.dir, file), 'utf8'), 'closed');
  }
});

test('EOF during a stalled import has a bounded nonzero exit', { timeout: 15000 }, async t => {
  const setup = await fixture(t, { engine: `
    import { writeFile } from 'node:fs/promises';
    await writeFile(new URL('./started', import.meta.url), 'started');
    await new Promise(() => {});
    export class BridgeManager {}
  `, server: 'export function createBridgeServer() {}' });
  const run = launch(t, setup.script);
  run.child.stdin.write(frame());
  await waitForFile(join(setup.dir, 'started'));
  const start = Date.now();
  run.child.stdin.end();
  await stopped(run, 1);
  assert.ok(Date.now() - start < 8000);
});

for (const failure of ['stalled close', 'retained child']) {
  test(`${failure} reports unsuccessful cleanup to the owner`, { timeout: 15000 }, async t => {
    const setup = await fixture(t, { engine: `
      export class BridgeManager {
        sessions = new Map(${JSON.stringify(failure === 'retained child' ? [['live', {}]] : [])});
        close() { return ${failure === 'stalled close' ? 'new Promise(() => {})' : 'Promise.resolve()'}; }
      }
    `, server: `
      import { EventEmitter } from 'node:events';
      export function createBridgeServer({ manager }) {
        return { server: new EventEmitter(), async listen() { return { port: 12345 }; },
          async close() { await manager.close(); } };
      }
    ` });
    const run = launch(t, setup.script, { detached: false });
    run.child.stdin.write(frame());
    await run.ready;
    const start = Date.now();
    run.child.stdin.end();
    await stopped(run, 1, true);
    assert.ok(Date.now() - start < 8000);
  });
}

test('EOF with an incomplete control frame rejects it', async t => {
  const run = launch(t);
  run.child.stdin.end(frame().trimEnd());
  await stopped(run, 1);
});

test('accepts the exact 4096-byte frame boundary', async t => {
  const run = launch(t);
  const control = frame().trimEnd();
  run.child.stdin.write(control + ' '.repeat(4096 - Buffer.byteLength(control)) + '\n');
  await run.ready;
  run.child.stdin.end();
  await stopped(run, 0, true);
});

test('dependency stdout cannot corrupt readiness; optional modes reach the manager', async t => {
  const setup = await fixture(t, { engine: `
    console.log('PRIVATE SDK CONFIG');
    await new Promise(resolve => process.stdout.write('PRIVATE SDK ERROR', resolve));
    export class BridgeManager {
      sessions = new Map();
      constructor(options) {
        if (options.systemPromptMode !== 'custom' || options.interruptMode !== 'rebuild')
          throw new Error('Incorrect modes');
      }
      async close() {}
    }
  `, server: `
    import { EventEmitter } from 'node:events';
    export function createBridgeServer({ manager }) {
      return { server: new EventEmitter(), async listen() { return { port: 12345 }; },
        async close() { await manager.close(); } };
    }
  ` });
  const run = launch(t, setup.script);
  run.child.stdin.write(frame({ systemPromptMode: 'custom', interruptMode: 'rebuild' }));
  assert.equal((await run.ready).kind, 'ready');
  run.child.stdin.end();
  await stopped(run, 0, true);
});

test('private-group opt-in fails closed when the process is not its group leader',
  { skip: process.platform === 'win32' }, async t => {
    const run = launch(t, entry, { detached: false });
    run.child.stdin.write(frame({ ownedProcessGroup: true }));
    await stopped(run, 1);
  });

function stillRunning(pid) {
  try {
    const state = execFileSync('/bin/ps', ['-o', 'stat=', '-p', String(pid)], {
      encoding: 'utf8', timeout: 1000, stdio: ['ignore', 'pipe', 'ignore'],
    }).trim();
    return state !== '' && !state.startsWith('Z'); // Reparented zombies cannot execute.
  } catch (error) {
    if (error.status === 1) return false; // No such process.
    throw error;
  }
}

for (const failure of ['stalled close', 'retained child']) {
  test(`private-group ${failure}: owner EOF kills child and grandchild before leader disappears`,
    { timeout: 15000, skip: process.platform === 'win32' }, async t => {
      const childProgram = `
        const { spawn } = require('node:child_process');
        const { writeFileSync } = require('node:fs');
        const grandchild = spawn(process.execPath,
          ['-e', 'process.on("SIGTERM", () => {}); setInterval(() => {}, 1000)'],
          { stdio: 'ignore' });
        writeFileSync(process.argv[1], JSON.stringify([process.pid, grandchild.pid]));
        process.on('SIGTERM', () => {});
        setInterval(() => {}, 1000);
      `;
      const setup = await fixture(t, { engine: `
        import { spawn } from 'node:child_process';
        import { fileURLToPath } from 'node:url';
        export class BridgeManager {
          sessions = new Map([['live', {}]]);
          constructor() {
            spawn(process.execPath, ['-e', ${JSON.stringify(childProgram)},
              fileURLToPath(new URL('./descendants', import.meta.url))], { stdio: 'ignore' });
          }
          close() { return ${failure === 'stalled close' ? 'new Promise(() => {})' : 'Promise.resolve()'}; }
        }
      `, server: `
        import { EventEmitter } from 'node:events';
        export function createBridgeServer({ manager }) {
          return { server: new EventEmitter(), async listen() { return { port: 12345 }; },
            async close() { await manager.close(); } };
        }
      ` });
      const run = launch(t, setup.script);
      run.child.stdin.write(frame({ ownedProcessGroup: true }));
      await run.ready;
      const descendants = JSON.parse(await waitForFile(join(setup.dir, 'descendants')));
      assert.ok(descendants.every(stillRunning));
      const start = Date.now();
      run.child.stdin.end(); // Same ownership-lease event as an abruptly dead backend.
      const result = await run.exited;
      assert.equal(result.code, null);
      assert.equal(result.signal, 'SIGKILL');
      assert.equal(result.stderr, '');
      assert.equal(result.stdout.includes(key), false);
      assert.equal(result.stdout.trim().split('\n').length, 1);
      assert.ok(Date.now() - start < 8000);
      for (let attempt = 0; attempt < 100 && descendants.some(stillRunning); attempt++) await delay(10);
      assert.ok(descendants.every(pid => !stillRunning(pid)));
    });
}
