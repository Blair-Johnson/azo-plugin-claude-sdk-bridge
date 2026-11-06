import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { query as realQuery } from '@anthropic-ai/claude-agent-sdk';
import { BridgeManager } from '../../src/engine.mjs';
import { QueryProcess } from '../../src/child.mjs';

export const SYNTHETIC_KEY = 'synthetic-offline-key';

function streamMessage(response, body, content, index, usage) {
  response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
  const emit = (type, data) => response.write(`event: ${type}\ndata: ${JSON.stringify({ type, ...data })}\n\n`);
  emit('message_start', { message: {
    id: `msg_offline_${index}`, type: 'message', role: 'assistant', model: body.model,
    content: [], stop_reason: null, stop_sequence: null,
    usage: { ...usage, output_tokens: 0 },
  } });
  content.forEach((block, blockIndex) => {
    if (block.type === 'text') {
      emit('content_block_start', { index: blockIndex, content_block: { type: 'text', text: '' } });
      emit('content_block_delta', { index: blockIndex, delta: { type: 'text_delta', text: block.text } });
    } else {
      assert.equal(block.type, 'tool_use');
      emit('content_block_start', { index: blockIndex, content_block: { ...block, input: {} } });
      // Split JSON to exercise the actual SDK's streaming tool-argument assembly.
      const json = JSON.stringify(block.input);
      const middle = Math.max(1, Math.floor(json.length / 2));
      for (const partial_json of [json.slice(0, middle), json.slice(middle)]) {
        emit('content_block_delta', { index: blockIndex, delta: { type: 'input_json_delta', partial_json } });
      }
    }
    emit('content_block_stop', { index: blockIndex });
  });
  emit('message_delta', {
    delta: { stop_reason: content.some(block => block.type === 'tool_use') ? 'tool_use' : 'end_turn', stop_sequence: null },
    usage: { output_tokens: usage.output_tokens },
  });
  emit('message_stop', {});
  response.end();
}

/** Entirely synthetic API. No forwarding, DNS lookup, authentication, or live SDK login. */
export async function localApi(t, reply, { usage = {
  input_tokens: 32, output_tokens: 12, cache_creation_input_tokens: 0, cache_read_input_tokens: 0,
} } = {}) {
  const root = await mkdtemp(join(tmpdir(), 'azo-sdk-offline-'));
  const home = join(root, 'home');
  const config = join(root, 'config');
  const cwd = join(root, 'project');
  await Promise.all([home, config, cwd].map(path => mkdir(path)));
  const requests = [];
  const failures = [];
  const sockets = new Set();
  const server = createServer(async (request, response) => {
    try {
      // Also acts as a dead-end proxy: absolute URLs are never forwarded.
      assert.ok(request.url.startsWith('/v1/messages'), 'Unexpected outbound HTTP endpoint');
      assert.ok(request.headers['x-api-key'] === SYNTHETIC_KEY, 'SDK must use only the synthetic key');
      assert.ok(request.headers.authorization === undefined, 'OAuth credentials must never be sent');
      assert.equal(request.method, 'POST');
      let payload = '';
      for await (const chunk of request) {
        payload += chunk;
        assert.ok(payload.length < 4 * 1024 * 1024, 'Unexpectedly large synthetic request');
      }
      const body = JSON.parse(payload);
      const pathname = new URL(request.url, 'http://127.0.0.1').pathname;
      if (pathname === '/v1/messages/count_tokens') {
        response.writeHead(200, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ input_tokens: 32 }));
        return;
      }
      assert.equal(pathname, '/v1/messages');
      assert.equal(body.stream, true, 'Fixture expects the real SDK streaming protocol');
      requests.push(body);
      const content = await reply(body, requests.length - 1);
      streamMessage(response, body, content, requests.length, usage);
    } catch (error) {
      failures.push(error);
      if (!response.headersSent) response.writeHead(400, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'Offline fixture rejected request' } }));
    }
  });
  server.on('connection', socket => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
  });
  server.on('connect', (_request, socket) => {
    failures.push(new Error('Unexpected outbound HTTPS CONNECT; offline fixture never forwards'));
    socket.end('HTTP/1.1 403 Forbidden\r\n\r\n');
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const url = `http://127.0.0.1:${server.address().port}`;
  // Undefined values shadow all inherited SDK env defaults, including tokens,
  // provider routing, proxy credentials, NODE_OPTIONS, and API-key helpers.
  const env = Object.fromEntries(Object.keys(process.env).map(key => [key, undefined]));
  Object.assign(env, {
    PATH: process.env.PATH,
    SystemRoot: process.env.SystemRoot,
    TMPDIR: root, TMP: root, TEMP: root,
    HOME: home, USERPROFILE: home, XDG_CONFIG_HOME: config,
    CLAUDE_CONFIG_DIR: config,
    ANTHROPIC_BASE_URL: url, ANTHROPIC_API_KEY: SYNTHETIC_KEY,
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1',
    DISABLE_TELEMETRY: '1', DISABLE_ERROR_REPORTING: '1', DISABLE_AUTOUPDATER: '1',
    DISABLE_NON_ESSENTIAL_MODEL_CALLS: '1', DISABLE_AUTO_COMPACT: '1',
    CLAUDE_CODE_DISABLE_AUTO_MEMORY: '1', ENABLE_CLAUDEAI_MCP_SERVERS: '0',
    CLAUDE_CODE_ELEGANT_MEADOW: '0',
    HTTP_PROXY: url, HTTPS_PROXY: url, ALL_PROXY: url,
    http_proxy: url, https_proxy: url, all_proxy: url,
    NO_PROXY: '127.0.0.1,localhost', no_proxy: '127.0.0.1,localhost',
  });
  const previous = new Map(['HOME', 'USERPROFILE', 'CLAUDE_CONFIG_DIR'].map(key => [key, process.env[key]]));
  // cc-session-io uses the parent process environment, not Query.options.env.
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  process.env.CLAUDE_CONFIG_DIR = config;
  const close = async () => {
    for (const [key, value] of previous) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => server.close(resolve));
    await rm(root, { recursive: true, force: true });
  };
  // Tests close owned SDK children before invoking this cleanup.
  t.after(close);
  return { root, cwd, home, config, url, env, requests, failures,
    assertHealthy() { if (failures.length) throw failures[0]; },
  };
}

export async function until(predicate, { signal, message = 'Offline SDK condition timed out' } = {}) {
  const deadline = Date.now() + 10_000;
  while (!predicate()) {
    signal?.throwIfAborted();
    assert.ok(Date.now() < deadline, message);
    await new Promise(resolve => setTimeout(resolve, 20));
  }
}

/** Shared real-SDK harness. Synthetic routing is restored only at queryFactory;
 * the final spawn guard must fail if production strips it a second time. */
export async function offlineSdk(t, reply, { usage, managerOptions = {} } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('Offline SDK deadline')), 45_000);
  const managers = [], processes = [], queries = [], initTools = [];
  const callbacks = [], callbackQueries = [], settled = [], factoryOptions = [];
  let api, apiClose, closing;
  const close = () => closing ??= (async () => {
    clearTimeout(timer);
    controller.abort();
    for (const query of queries) { try { query.close(); } catch { /* already closed */ } }
    // Do not let a manager-close error skip terminating another owned child.
    const outcomes = await Promise.allSettled(managers.map(manager => manager.close()));
    outcomes.push(...await Promise.allSettled(processes.map(process => process.shutdown(null, null))));
    assert.ok(processes.every(process => process.didExit), 'All owned SDK children must exit before config cleanup');
    await apiClose?.();
    const failed = outcomes.find(outcome => outcome.status === 'rejected');
    if (failed) throw failed.reason;
    api?.assertHealthy();
  })();
  t.after(close);
  api = await localApi({ after(fn) { apiClose = fn; } }, reply, { usage });
  const makeManager = (options = {}) => {
    const manager = new BridgeManager({
      ...managerOptions, ...options,
      stateRoot: join(api.root, 'bridge-state'), requestTimeoutMs: 20_000, env: api.env,
      processFactory: () => {
        const process = new QueryProcess({
          spawnFactory(command, args, options) {
            // Last boundary before any process starts: never bypass this guard.
            try {
              assert.ok(options.env.ANTHROPIC_BASE_URL === api.url, 'Child launcher stripped offline API routing');
              assert.ok(options.env.ANTHROPIC_API_KEY === SYNTHETIC_KEY, 'Synthetic API key must survive');
              assert.ok(options.env.CLAUDE_CONFIG_DIR === api.config, 'Child config must be isolated');
              assert.ok(options.env.HOME === api.home, 'Child HOME must be isolated');
              assert.ok(options.env.CLAUDE_CODE_OAUTH_TOKEN === undefined, 'Inherited OAuth must be absent');
              assert.ok(options.env.ANTHROPIC_AUTH_TOKEN === undefined, 'Inherited auth token must be absent');
              assert.equal(options.env.ENABLE_CLAUDEAI_MCP_SERVERS, '0');
              assert.equal(options.env.CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC, '1');
              assert.ok(options.env.HTTPS_PROXY === api.url, 'Outbound HTTPS must use the dead-end proxy');
              assert.ok(options.env.HTTP_PROXY === api.url, 'Outbound HTTP must use the dead-end proxy');
            } catch (error) {
              api.failures.push(error);
              throw error;
            }
            return spawn(command, args, options);
          },
        });
        processes.push(process);
        return process;
      },
      queryFactory(args) {
        const queryIndex = queries.length;
        factoryOptions.push({ resume: args.options.resume, sessionId: args.options.sessionId });
        assert.deepEqual(Object.keys(args.options.mcpServers), ['azo']);
        assert.deepEqual(args.options.tools, []);
        assert.deepEqual(args.options.skills, []);
        assert.deepEqual(args.options.settingSources, []);
        assert.equal(args.options.strictMcpConfig, true);
        // Observe real MCP traffic; never invoke handlers or fabricate SDK events.
        const handlers = args.options.mcpServers.azo.instance._requestHandlers;
        const call = handlers.get('tools/call');
        assert.equal(typeof call, 'function');
        handlers.set('tools/call', async (request, extra) => {
          callbacks.push(structuredClone(request.params));
          callbackQueries.push(queryIndex);
          const result = await call(request, extra);
          settled.push(request.params._meta?.['claudecode/toolUseId']);
          return result;
        });
        const query = realQuery({ ...args, options: { ...args.options, env: {
          ...args.options.env, ...api.env, ENABLE_TOOL_SEARCH: 'false',
          CLAUDE_CODE_MAX_OUTPUT_TOKENS: String(512),
        } } });
        queries.push(query);
        return {
          close: () => query.close(),
          async *[Symbol.asyncIterator]() {
            for await (const message of query) {
              if (message.type === 'system' && message.subtype === 'init') initTools.push(message.tools);
              yield message;
            }
          },
        };
      },
    });
    managers.push(manager);
    return manager;
  };
  return { api, controller, makeManager, close, managers, processes, queries,
    initTools, callbacks, callbackQueries, settled, factoryOptions };
}
