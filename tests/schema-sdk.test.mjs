import test from 'node:test';
import assert from 'node:assert/strict';
import { offlineSdk } from './fixtures/local-api.mjs';

const schema = {
  type: 'object',
  properties: {
    value: { anyOf: [{ type: 'string', minLength: 1 }, { type: 'integer', minimum: 0 }] },
  },
  required: ['value'],
  additionalProperties: false,
};
const tool = { name: 'schema_probe', description: 'Host-owned schema probe', input_schema: schema };

test('real SDK advertises nested schema constraints unchanged and rejects root combinators before launch', { timeout: 60_000 }, async t => {
  const runtime = await offlineSdk(t, body => {
    assert.deepEqual(body.tools, [{ ...tool, name: 'mcp__azo__azo_schema_probe' }]);
    return [{ type: 'text', text: 'SCHEMA_VISIBLE' }];
  });
  const manager = runtime.makeManager();
  const body = { model: 'claude-sonnet-4-6', max_tokens: 512, tools: [tool],
    messages: [{ role: 'user', content: 'Reply without executing tools.' }] };
  for (const key of ['anyOf', 'oneOf', 'allOf']) {
    await assert.rejects(manager.handle({ ...body, tools: [{ ...tool,
      input_schema: { ...schema, [key]: [{ required: ['value'] }] },
    }] }, { sessionId: `bad-${key}` }), error => error.statusCode === 400);
  }
  assert.equal(runtime.queries.length, 0, 'Unsupported schemas must never start Claude');
  assert.equal(runtime.api.requests.length, 0);
  const reply = await manager.handle(body, { sessionId: 'valid-schema' });
  assert.equal(reply.content[0].text, 'SCHEMA_VISIBLE');
  runtime.api.assertHealthy();
  assert.equal(runtime.queries.length, 1);
  assert.equal(runtime.api.requests.length, 1);
  assert.equal(runtime.callbacks.length, 0, 'The SDK must not execute a host action');
  await manager.close();
});
