import { spawn as nodeSpawn } from 'node:child_process';

// Undefined entries deliberately shadow SDK merges with process.env. Authentication
// remains the SDK's responsibility; do not inspect credential files or tokens here.
export function childEnvironment(parent = process.env, overrides = {}) {
  const env = { ...parent, ...overrides };
  for (const key of Object.keys(env)) {
    if (/^(ANTHROPIC_|CLAUDE_CODE_USE_|AWS_|BEDROCK_|VERTEX_|GOOGLE_|CLOUD_ML_|AZURE_|OPENAI_)/i.test(key)
        || /^(CLAUDE_CODE_API_KEY_HELPER|CLAUDE_CODE_ELEGANT_MEADOW|CLAUDE_CODE_PLUGIN_SEED_DIR|AZO_CLAUDE_BRIDGE_KEY)$/i.test(key)
        || /^(CLAUDECODE|CLAUDE_CODE_ENTRYPOINT|CLAUDE_CODE_SESSION_ID|CLAUDE_CODE_PARENT_SESSION_ID)$/i.test(key)) {
      env[key] = undefined;
    }
  }
  return {
    ...env,
    ENABLE_CLAUDEAI_MCP_SERVERS: '0',
    DISABLE_AUTO_COMPACT: '1',
    DISABLE_AUTOUPDATER: '1',
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1',
    CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL: '1',
    DISABLE_NON_ESSENTIAL_MODEL_CALLS: '1',
    CLAUDE_CODE_DISABLE_AUTO_MEMORY: '1',
    CLAUDE_CODE_ELEGANT_MEADOW: '0',
    CLAUDE_CODE_RESUME_INTERRUPTED_TURN_MAX_AGE_MS: '1',
  };
}

export const isolatedSettings = Object.freeze({
  disableAllHooks: true,
  autoMemoryEnabled: false,
  includeGitInstructions: false,
  enableAllProjectMcpServers: false,
  enabledPlugins: {},
  claudeMdExcludes: ['/**'],
});

async function waitBounded(promise, milliseconds) {
  let timer;
  try {
    return await Promise.race([
      promise.then(() => true),
      new Promise(resolve => { timer = setTimeout(() => resolve(false), milliseconds); }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

/** Track the real process, not merely the SDK iterator's completion. */
export class QueryProcess {
  constructor({ spawnFactory = nodeSpawn, graceMs = 1000, killMs = 1000 } = {}) {
    this.spawnFactory = spawnFactory;
    this.graceMs = graceMs;
    this.killMs = killMs;
    this.child = null;
    this.sealed = false;
    this.didExit = false;
    this.shutdownPromise = null;
    this.exited = new Promise(resolve => { this.resolveExit = resolve; });
  }

  spawn = (options) => {
    if (this.sealed || this.child) throw new Error('Bridge child is already closed or started');
    const child = this.spawnFactory(options.command, options.args, {
      cwd: options.cwd,
      // Sanitized before query construction; preserve SDK-added identity/auth.
      env: options.env,
      signal: options.signal,
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: true,
    });
    this.child = child;
    // Drain without logging: CLI stderr can contain credentials or user content.
    child.stderr?.on('data', () => {});
    child.on('error', () => {});
    child.once('close', () => this.markExited());
    return child;
  };

  markExited() {
    this.didExit = true;
    this.resolveExit();
  }

  shutdown(query, controller) {
    if (this.shutdownPromise) return this.shutdownPromise;
    this.sealed = true;
    this.shutdownPromise = this.stop(query, controller);
    return this.shutdownPromise;
  }

  async stop(query, controller) {
    // Neither an SDK close promise nor interrupt() is trusted to settle.
    try { Promise.resolve(query?.close?.()).catch(() => {}); } catch { /* sanitized */ }
    try { controller?.abort(); } catch { /* sanitized */ }
    if (!this.child) this.markExited();
    if (this.didExit) return true;
    try { this.child.kill('SIGTERM'); } catch { /* still wait for actual close */ }
    if (await waitBounded(this.exited, this.graceMs)) return true;
    try { this.child.kill('SIGKILL'); } catch { /* don't claim exit */ }
    return waitBounded(this.exited, this.killMs);
  }
}
