import test from 'node:test';
import assert from 'node:assert/strict';
import { offlineSdk } from './fixtures/local-api.mjs';
import { lowerServerTools, searchPermission, searchResultBlock, serverTools } from '../src/websearch.mjs';

// The SDK's WebSearch runs one inner Messages request carrying the native
// web_search server tool. The synthetic API answers it with real block shapes.
const SEARCH = { type: 'web_search_20250305', name: 'web_search' };
const PROBE = { name: 'offline_probe', description: 'Synthetic probe; no host action executes',
  input_schema: { type: 'object', properties: {}, additionalProperties: false } };
const BASE = { model: 'claude-sonnet-4-6', max_tokens: 512, system: 'Synthetic offline web search test.',
  thinking: { type: 'disabled' } };
const SESSION = 'offline-web-search';
const user = content => ({ role: 'user', content });
const asMessage = ({ role, content }) => ({ role, content });
const blocks = message => typeof message.content === 'string' ? [{ type: 'text', text: message.content }] : message.content;
const lastText = body => blocks(body.messages.at(-1)).filter(b => b.type === 'text').map(b => b.text).join(' ');
const isInner = body => (body.tools ?? []).some(tool => tool.type === SEARCH.type);
const searchCall = (body, id, query) => ({ type: 'tool_use', id, name: body.tools.find(t => t.name === 'WebSearch').name, input: { query } });
const innerAnswer = body => {
  const query = lastText(body).replace(/^.*query: /, '');
  return [
    { type: 'server_tool_use', id: `srvtoolu_${query.replace(/\W/g, '')}`, name: 'web_search', input: { query } },
    { type: 'web_search_tool_result', tool_use_id: `srvtoolu_${query.replace(/\W/g, '')}`, content: [
      { type: 'web_search_result', url: `https://example.com/${query.replace(/\W/g, '')}`, title: `Result for ${query}`,
        encrypted_content: 'SYNTHETIC_ENCRYPTED', page_age: null },
    ] },
    { type: 'text', text: `SUMMARY_${query.replace(/\W/g, '')}` },
  ];
};

async function setup(t, mainReply) {
  const inner = [];
  const runtime = await offlineSdk({ after(close) { t.after(close); } }, (body, index) => {
    if (isInner(body)) { inner.push(body); return innerAnswer(body); }
    return mainReply(body, index);
  });
  return { ...runtime, manager: runtime.makeManager(), inner };
}

async function send(f, messages, { tools = [SEARCH], events = [], ...extra } = {}) {
  const answer = await f.manager.handle({ ...BASE, tools, messages, ...extra },
    { sessionId: SESSION, onEvent: event => events.push(event) });
  return { answer, events };
}

function assertStream(events, answer) {
  assert.equal(events.filter(e => e.type === 'message_start').length, 1, 'One host response');
  assert.equal(events.filter(e => e.type === 'message_stop').length, 1);
  const starts = events.filter(e => e.type === 'content_block_start').map(e => e.index);
  assert.deepEqual(starts, answer.content.map((_, i) => i), 'Contiguous transport indexes for every block');
}

test('search-only turn streams one native response with server tool blocks', { timeout: 45_000 }, async t => {
  const f = await setup(t, (body, index) => index === 0
    ? [{ type: 'text', text: 'Let me search.' }, searchCall(body, 'toolu_ws_1', 'alpha')]
    : [{ type: 'text', text: 'FINAL_ANSWER per [Result](https://example.com/alpha).' }]);
  const { answer, events } = await send(f, [user('Search for alpha.')]);
  assert.deepEqual(answer.content.map(b => b.type), ['text', 'server_tool_use', 'web_search_tool_result', 'text']);
  assert.equal(answer.stop_reason, 'end_turn');
  const [, call, result] = answer.content;
  assert.deepEqual(call, { type: 'server_tool_use', id: 'toolu_ws_1', name: 'web_search', input: { query: 'alpha' } });
  assert.equal(result.tool_use_id, 'toolu_ws_1');
  assert.deepEqual(result.content, [{ type: 'web_search_result', url: 'https://example.com/alpha', title: 'Result for alpha', page_age: null, encrypted_content: '' }]);
  assert.match(result.sdk_tool_result.content, /SUMMARY_alpha/, 'The SDK result text the model read is kept');
  assertStream(events, answer);
  assert.equal(f.inner.length, 1);
  assert.deepEqual(f.callbacks, [], 'No host tool ran');

  // The next host turn continues the same SDK process: no rebuild for history
  // that contains server search blocks.
  const history = [user('Search for alpha.'), asMessage(answer), user('Thanks.')];
  await send(f, history);
  assert.equal(f.queries.length, 1);
  f.api.assertHealthy();
});

test('rebuilt history restores each search as the SDK WebSearch exchange', { timeout: 45_000 }, async t => {
  const f = await setup(t, (body, index) => index === 0
    ? [{ type: 'text', text: 'Let me search.' }, searchCall(body, 'toolu_ws_1', 'alpha')]
    : [{ type: 'text', text: index === 2 ? 'FINAL_ANSWER' : 'SECOND_ANSWER' }]);
  const { answer } = await send(f, [user('Search for alpha.')]);
  await f.manager.cancel(SESSION);
  await send(f, [user('Search for alpha.'), asMessage(answer), user('Next question.')]);
  const replay = f.api.requests.at(-1);
  assert.deepEqual(replay.messages.map(m => m.role), ['user', 'assistant', 'user', 'assistant', 'user']);
  const [call] = blocks(replay.messages[1]).filter(b => b.type === 'tool_use');
  assert.equal(call.name, 'WebSearch', 'Search calls replay as the built-in tool, not an Agent Zoo tool');
  assert.deepEqual(call.input, { query: 'alpha' });
  const [resultBlock] = blocks(replay.messages[2]).filter(b => b.type === 'tool_result');
  assert.equal(resultBlock.tool_use_id, 'toolu_ws_1');
  assert.match(JSON.stringify(resultBlock.content), /SUMMARY_alpha/, 'The exact search text is restored');
  assert.ok(!JSON.stringify(replay.messages).includes('server_tool_use'));
  f.api.assertHealthy();
});

test('a host tool and a later search in one batch: the result leads the next response', { timeout: 45_000 }, async t => {
  const f = await setup(t, (body, index) => index === 0
    ? [{ type: 'tool_use', id: 'toolu_probe', name: body.tools.find(t => t.name.includes('offline_probe')).name, input: {} },
      searchCall(body, 'toolu_ws_1', 'beta')]
    : [{ type: 'text', text: 'FINAL_ANSWER' }]);
  const first = await send(f, [user('Probe, then search beta.')], { tools: [SEARCH, PROBE] });
  assert.equal(first.answer.stop_reason, 'tool_use');
  assert.deepEqual(first.answer.content.map(b => b.type), ['tool_use', 'server_tool_use']);
  const history = [user('Probe, then search beta.'), asMessage(first.answer),
    user([{ type: 'tool_result', tool_use_id: 'toolu_probe', content: 'PROBE_RESULT' }])];
  const second = await send(f, history, { tools: [SEARCH, PROBE] });
  assert.deepEqual(second.answer.content.map(b => b.type), ['web_search_tool_result', 'text']);
  assert.equal(second.answer.content[0].tool_use_id, 'toolu_ws_1');
  assertStream(second.events, second.answer);
  assert.equal(f.queries.length, 1);

  // Rebuild: the late result moves back beside its call, with the host result.
  await f.manager.cancel(SESSION);
  await send(f, [...history, asMessage(second.answer), user('Next.')], { tools: [SEARCH, PROBE] });
  const replay = f.api.requests.at(-1);
  const results = blocks(replay.messages[2]).filter(b => b.type === 'tool_result').map(b => b.tool_use_id);
  assert.deepEqual(results.sort(), ['toolu_probe', 'toolu_ws_1']);
  f.api.assertHealthy();
});

test('max_uses and domain filters are enforced on SDK searches', { timeout: 45_000 }, async t => {
  const f = await setup(t, (body, index) => index === 0
    ? [searchCall(body, 'toolu_ws_1', 'one'), searchCall(body, 'toolu_ws_2', 'two')]
    : [{ type: 'text', text: 'FINAL_ANSWER' }]);
  const tool = { ...SEARCH, max_uses: 1, allowed_domains: ['example.com'] };
  const { answer } = await send(f, [user('Two searches.')], { tools: [tool] });
  const results = Object.fromEntries(answer.content.filter(b => b.type === 'web_search_tool_result')
    .map(b => [b.tool_use_id, b.content]));
  assert.ok(Array.isArray(results.toolu_ws_1), 'The first search ran');
  assert.deepEqual(results.toolu_ws_2, { type: 'web_search_tool_result_error', error_code: 'max_uses_exceeded' });
  assert.equal(f.inner.length, 1, 'The denied search never ran');
  assert.deepEqual(f.inner[0].tools.find(t => t.type === SEARCH.type).allowed_domains, ['example.com']);
  f.api.assertHealthy();
});

test('server tool configuration is validated', () => {
  assert.deepEqual(serverTools([PROBE]), { host: [PROBE], webSearch: null });
  assert.deepEqual(serverTools([SEARCH, PROBE]).webSearch, { maxUses: undefined, allowed: undefined, blocked: undefined });
  for (const bad of [
    [{ ...SEARCH, user_location: { type: 'approximate' } }], [{ ...SEARCH, name: 'search' }],
    [{ ...SEARCH, max_uses: 0 }], [{ ...SEARCH, allowed_domains: ['a.com'], blocked_domains: ['b.com'] }],
    [{ type: 'web_fetch_20250910', name: 'web_fetch' }], [SEARCH, { ...SEARCH }],
    [SEARCH, { ...PROBE, name: 'WebSearch' }],
  ]) assert.throws(() => serverTools(bad), error => error.statusCode === 400);
  assert.equal(searchPermission(null, { query: 'q' }, 0).behavior, 'deny');
  assert.deepEqual(searchPermission({ blocked: ['x.com'] }, { query: 'q', allowed_domains: ['y.com'] }, 0),
    { behavior: 'allow', updatedInput: { query: 'q', blocked_domains: ['x.com'] } });
});

test('search results convert errors, denials and unsafe URLs', () => {
  const ok = searchResultBlock({ tool_use_id: 't1', content: 'text' }, { results: ['note',
    { tool_use_id: 's', content: [{ title: 'A', url: 'https://a.example' }, { title: 'X', url: 'javascript:alert(1)' }] }] });
  assert.deepEqual(ok.content, [{ type: 'web_search_result', url: 'https://a.example', title: 'A', page_age: null, encrypted_content: '' }]);
  assert.deepEqual(searchResultBlock({ tool_use_id: 't2', content: 'Web search error: too_many_requests' },
    { results: ['Web search error: too_many_requests'] }).content,
    { type: 'web_search_tool_result_error', error_code: 'too_many_requests' });
  assert.deepEqual(searchResultBlock({ tool_use_id: 't3', content: 'max_uses_exceeded', is_error: true }, 'Error').content,
    { type: 'web_search_tool_result_error', error_code: 'max_uses_exceeded' });
  const parsed = searchResultBlock({ tool_use_id: 't4', content: 'Web search results\n\nLinks: [{"title":"B","url":"https://b.example"}]\n' });
  assert.equal(parsed.content[0].url, 'https://b.example');
});

test('lowering handles split, repeated and foreign server search history', () => {
  const result = (id, text) => ({ type: 'web_search_tool_result', tool_use_id: id, content: [],
    sdk_tool_result: { content: text, is_error: false } });
  const stu = id => ({ type: 'server_tool_use', id, name: 'web_search', input: { query: id } });
  const { messages, searchIds } = lowerServerTools([
    user('q'),
    { role: 'assistant', content: [{ type: 'text', text: 'a' }, stu('s1'), stu('s2'), result('s1', 'R1'), result('s2', 'R2'),
      { type: 'text', text: 'b', citations: [{ type: 'web_search_result_location', url: 'https://x' }] },
      { type: 'server_tool_use', id: 'f1', name: 'web_fetch', input: {} }, { type: 'web_fetch_tool_result', tool_use_id: 'f1', content: {} }] },
    user('next'),
  ]);
  assert.deepEqual([...searchIds], ['s1', 's2']);
  assert.deepEqual(messages.map(m => m.role), ['user', 'assistant', 'user', 'assistant', 'user']);
  assert.deepEqual(messages[1].content.map(b => b.type), ['text', 'tool_use', 'tool_use']);
  assert.deepEqual(messages[2].content.map(b => [b.tool_use_id, b.content]), [['s1', 'R1'], ['s2', 'R2']]);
  assert.deepEqual(messages[3].content, [{ type: 'text', text: 'b' }], 'Citations and foreign server tools are dropped');
});
