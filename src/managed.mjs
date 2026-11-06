import { execFileSync } from 'node:child_process';

// Private stdin control channel; stdout is reserved for the single readiness frame.
// ownedProcessGroup is an explicit owner assertion: this process was started in a
// private POSIX session/group. Never infer that authorization from a PID alone.
const MAX_FRAME_BYTES = 4096;
const SHUTDOWN_MS = 5000;
const writeReady = process.stdout.write.bind(process.stdout);
// Dependencies must not put diagnostics on the private protocol channel.
process.stdout.write = (_chunk, encoding, callback) => {
  const done = typeof encoding === 'function' ? encoding : callback;
  if (typeof done === 'function') queueMicrotask(done);
  return true;
};
let pending = Buffer.alloc(0);
let framed = false;
let stopping = false;
let exitCode = 0;
let manager;
let endpoint;
let startup = Promise.resolve();
let ownedProcessGroup = false;

function verifyPrivateGroup() {
  if (process.platform === 'win32') throw new Error('Private POSIX group required');
  // Node has no public getpgid(). Use a fixed executable, no shell/PATH lookup,
  // bounded output/runtime, and suppress diagnostics. No SDK exists at this point.
  const group = execFileSync('/bin/ps', ['-o', 'pgid=', '-p', String(process.pid)], {
    encoding: 'utf8', timeout: 1000, killSignal: 'SIGKILL', maxBuffer: 1024,
    stdio: ['ignore', 'pipe', 'ignore'],
  }).trim();
  if (!/^[0-9]+$/.test(group) || Number(group) !== process.pid)
    throw new Error('Private POSIX group required');
}

function forceExit() {
  // A live, verified group leader pins its group identity: no post-reap PID race.
  if (ownedProcessGroup) {
    try { process.kill(-process.pid, 'SIGKILL'); return; } catch { /* self-exit below */ }
  }
  process.exit(1);
}

function stop(code = 0) {
  exitCode = Math.max(exitCode, code);
  if (stopping) return;
  stopping = true;
  process.stdin.pause();
  // Keep this timer referenced, including when an async import/listen never settles.
  setTimeout(forceExit, SHUTDOWN_MS);
  void (async () => {
    try {
      await startup;
      if (endpoint) await endpoint.close();
      else if (manager) await manager.close();
      // BridgeManager retains slots whose real SDK child has not exited.
      if (manager?.sessions?.size) exitCode = 1;
    } catch { exitCode = 1; }
    if (exitCode) forceExit();
    else process.exit(0);
  })();
}

process.on('SIGTERM', () => stop());
process.on('SIGINT', () => stop());
process.on('uncaughtException', () => stop(1));
process.on('unhandledRejection', () => stop(1));
process.stdin.on('end', () => stop(!framed && pending.length ? 1 : 0));
process.stdin.on('error', () => stop(1));
process.stdout.on('error', () => stop(1));

function validate(frame) {
  const allowed = ['version', 'key', 'port', 'systemPromptMode', 'interruptMode', 'ownedProcessGroup'];
  if (!frame || typeof frame !== 'object' || Array.isArray(frame)
      || Object.keys(frame).some(key => !allowed.includes(key))
      || frame.version !== 1 || frame.port !== 0
      || typeof frame.key !== 'string' || !/^[a-f0-9]{64}$/i.test(frame.key)
      || ('ownedProcessGroup' in frame && typeof frame.ownedProcessGroup !== 'boolean')
      || ('systemPromptMode' in frame && !['append', 'custom'].includes(frame.systemPromptMode))
      || ('interruptMode' in frame && !['reuse', 'rebuild'].includes(frame.interruptMode))) {
    throw new Error('Invalid control frame');
  }
  return frame;
}

async function start(frame) {
  if (frame.ownedProcessGroup) {
    verifyPrivateGroup();
    ownedProcessGroup = true;
  }
  const [{ BridgeManager }, { createBridgeServer }] = await Promise.all([
    import('./engine.mjs'), import('./server.mjs'),
  ]);
  if (stopping) return;
  manager = new BridgeManager({ systemPromptMode: frame.systemPromptMode, interruptMode: frame.interruptMode });
  endpoint = createBridgeServer({ manager, key: frame.key });
  endpoint.server.on('error', () => stop(1));
  const address = await endpoint.listen(0);
  if (stopping) return;
  writeReady(JSON.stringify({ kind: 'ready', version: 1, port: address.port, pid: process.pid }) + '\n');
}

process.stdin.on('data', chunk => {
  if (stopping) return;
  if (framed) return stop(1);
  const newline = chunk.indexOf(10);
  const length = newline < 0 ? chunk.length : newline;
  if (pending.length + length > MAX_FRAME_BYTES) return stop(1);
  pending = Buffer.concat([pending, chunk.subarray(0, length)]);
  if (newline < 0) return;
  // No second command, even when it shares the first read. Keep draining stdin
  // afterward so EOF always tracks the owning backend's lifetime.
  if (newline !== chunk.length - 1) return stop(1);
  framed = true;
  try {
    const frame = validate(JSON.parse(pending.toString('utf8')));
    pending = Buffer.alloc(0);
    startup = start(frame).catch(() => { stop(1); });
  } catch { stop(1); }
});
