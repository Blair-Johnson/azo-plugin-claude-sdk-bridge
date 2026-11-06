import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const script = fileURLToPath(new URL('../benchmarks/replay-offline.mjs', import.meta.url));

for (const input of ['PRIVATE_SYNTHETIC_MARKER', '{"secret":"PRIVATE_SYNTHETIC_MARKER"', '']) {
  test(`offline replay reports the actual malformed-input error (${input.length} bytes)`, () => {
    const result = spawnSync(process.execPath, [script], { input, encoding: 'utf8', timeout: 10_000 });
    assert.ifError(result.error);
    assert.equal(result.status, 1);
    assert.equal(result.stderr, '');
    const failure = JSON.parse(result.stdout);
    assert.equal(failure.kind, 'replay_failed');
    assert.equal(failure.calls, 0);
    assert.match(failure.reason, /SyntaxError/);
    assert.match(failure.reason, /JSON/);
    assert.ok(!failure.reason.includes('Local diagnostic failed'));
  });
}
