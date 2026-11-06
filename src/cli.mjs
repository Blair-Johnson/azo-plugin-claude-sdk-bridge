import { readFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { createBridgeServer } from './server.mjs';

const { values } = parseArgs({ options: {
  help: { type: 'boolean', short: 'h' },
  port: { type: 'string', default: '8766' },
  'key-file': { type: 'string' },
  'idle-seconds': { type: 'string', default: '300' },
  'timeout-seconds': { type: 'string', default: '120' },
  'max-sessions': { type: 'string', default: '16' },
  'system-prompt-mode': { type: 'string', default: 'append' },
  'interrupt-mode': { type: 'string', default: 'reuse' },
} });
if (values.help) {
  console.log('Agent Zoo Claude SDK bridge (opt-in, loopback only)\n' +
    'Usage: pixi run start [--port 8766] [--key-file PATH]\n' +
    'Local key: AZO_CLAUDE_BRIDGE_KEY, minimum 32 characters; never a Claude credential.\n' +
    'Controls: --idle-seconds 300 --timeout-seconds 120 --max-sessions 16\n' +
    'Prompt: --system-prompt-mode append (reference default) or custom\n' +
    'Interruption: --interrupt-mode reuse (default) or rebuild\n' +
    'Uses the Claude Agent SDK login. Does not enable or change extra-usage settings.');
} else {
  let endpoint;
  try {
    const integer = (name, low, high) => {
      const value = Number(values[name]);
      if (!Number.isSafeInteger(value) || value < low || value > high) throw new Error('Invalid option');
      return value;
    };
    const port = integer('port', 1, 65535);
    const idleMs = integer('idle-seconds', 1, 86400) * 1000;
    const requestTimeoutMs = integer('timeout-seconds', 1, 1800) * 1000;
    const maxSessions = integer('max-sessions', 1, 128);
    const key = values['key-file'] ? (await readFile(values['key-file'], 'utf8')).trim()
      : process.env.AZO_CLAUDE_BRIDGE_KEY;
    if (typeof key !== 'string' || key.length < 32) throw new Error('Missing key');
    const { BridgeManager } = await import('./engine.mjs');
    const systemPromptMode = values['system-prompt-mode'];
    if (!['append', 'custom'].includes(systemPromptMode)) throw new Error('Invalid prompt mode');
    const interruptMode = values['interrupt-mode'];
    const manager = new BridgeManager({ idleMs, requestTimeoutMs, maxSessions, systemPromptMode, interruptMode });
    endpoint = createBridgeServer({ manager, key, requestTimeoutMs });
    const address = await endpoint.listen(port);
    console.log(JSON.stringify({ kind: 'listening', host: address.address, port: address.port,
      backend: 'claude-agent-sdk', tools_executed_by: 'agent-zoo' }));
    let stopping = false;
    const stop = async () => {
      if (stopping) return;
      stopping = true;
      const deadline = setTimeout(() => process.exit(1), 15000);
      deadline.unref();
      try {
        await endpoint.close();
        clearTimeout(deadline);
      } catch {
        console.error('Bridge shutdown could not confirm all SDK children exited.');
        process.exitCode = 1;
      }
    };
    process.once('SIGINT', stop);
    process.once('SIGTERM', stop);
  } catch {
    if (endpoint) await endpoint.close().catch(() => {});
    console.error('Bridge startup failed. Check local key, options, dependencies, and whether the port is available.');
    process.exitCode = 1;
  }
}
