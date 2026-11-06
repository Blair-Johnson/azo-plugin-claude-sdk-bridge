import test from 'node:test';
import assert from 'node:assert/strict';
import { validateRequest } from '../src/request.mjs';

const body = { max_tokens: 64, messages: [{ role: 'user', content: 'hello' }] };
test('SDK long-context model suffix is preserved without accepting arbitrary syntax', () => {
  for (const model of ['sonnet', 'claude-opus-5-5', 'claude-fable-5-1[1m]', 'x'.repeat(128)])
    assert.equal(validateRequest({ ...body, model }).model, model);
  for (const model of ['', 'x'.repeat(129), 'x'.repeat(125) + '[1m]', 'sonnet[other]',
    'sonnet[1m][1m]', 'sonnet --flag', '../model', 'sonnet\n', null])
    assert.throws(() => validateRequest({ ...body, model }), error => error.statusCode === 400);
});
