import test from 'node:test';
import assert from 'node:assert/strict';
import { chmod, lstat, mkdir, mkdtemp, readdir, realpath, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { createWorkspace } from '../src/workspace.mjs';

async function root(t) {
  const path = await mkdtemp(join(tmpdir(), 'azo-workspace-test-'));
  t.after(() => rm(path, { recursive: true, force: true }));
  return path;
}

test('default workspaces are distinct private children of system temp, not a shared predictable parent', async t => {
  const paths = [];
  t.after(() => Promise.all(paths.map(path => rm(path, { recursive: true, force: true }))));
  paths.push(await createWorkspace(), await createWorkspace());
  assert.notEqual(paths[0], paths[1]);
  for (const path of paths) {
    assert.equal(await realpath(dirname(path)), await realpath(tmpdir()));
    assert.equal((await lstat(path)).mode & 0o777, 0o700);
  }
});

test('custom state parents are created privately and contain only unique workspaces', async t => {
  const parent = join(await root(t), 'private');
  const path = await createWorkspace(parent);
  assert.equal(dirname(path), parent);
  assert.equal((await lstat(parent)).mode & 0o777, 0o700);
  assert.equal((await readdir(parent)).length, 1);
});

test('preexisting shared or symlinked parents are rejected without changing their permissions', async t => {
  const base = await root(t);
  const shared = join(base, 'shared');
  await mkdir(shared);
  await chmod(shared, 0o777);
  await assert.rejects(createWorkspace(shared), /private/);
  assert.equal((await lstat(shared)).mode & 0o777, 0o777);
  assert.deepEqual(await readdir(shared), []);
  const target = join(base, 'target');
  await mkdir(target, { mode: 0o700 });
  const linked = join(base, 'linked');
  await symlink(target, linked);
  await assert.rejects(createWorkspace(linked), /private/);
  assert.deepEqual(await readdir(target), []);
  await assert.rejects(createWorkspace('relative'), /Invalid/);
});
