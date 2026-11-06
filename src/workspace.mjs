import { chmod, lstat, mkdir, mkdtemp, realpath, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { isAbsolute, join } from 'node:path';

/** A fresh private directory, never a predictable cross-user temporary parent. */
export async function createWorkspace(parent) {
  if (parent !== undefined) {
    if (typeof parent !== 'string' || !isAbsolute(parent)) throw new Error('Invalid bridge state directory.');
    await mkdir(parent, { recursive: true, mode: 0o700 });
    const stat = await lstat(parent);
    if (!stat.isDirectory() || stat.isSymbolicLink()
        || typeof process.getuid === 'function' && stat.uid !== process.getuid()
        || (stat.mode & 0o077) !== 0) throw new Error('Bridge state directory must be private and owned by the current user.');
  }
  const path = await mkdtemp(join(parent ?? tmpdir(), 'azo-claude-bridge-'));
  try {
    await chmod(path, 0o700);
    // The SDK reports the canonical cwd (macOS: /var -> /private/var); match it.
    return await realpath(path);
  } catch (error) {
    await rm(path, { recursive: true, force: true }).catch(() => {});
    throw error;
  }
}
