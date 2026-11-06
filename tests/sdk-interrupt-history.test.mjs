import test from 'node:test';
import assert from 'node:assert/strict';
import { ServerResponse } from 'node:http';
import { spawn } from 'node:child_process';
import { query, createSdkMcpServer, tool } from '@anthropic-ai/claude-agent-sdk';
import { localApi, SYNTHETIC_KEY, until } from './fixtures/local-api.mjs';
import { QueryProcess, isolatedSettings } from '../src/child.mjs';

// Synthetic-only research regression: interrupted SDK history is not host history.
for (const mode of ['text', 'parked-mcp']) {
  test(`offline SDK interrupt history: ${mode}`, { timeout: 35_000 }, async t => {
    let apiClose;
    let parked = false, release;
    let ended = false, wake;
    const queue = [], events = [];
    const controller = new AbortController();
    const pendingTool = new Promise(resolve => { release = resolve; });
    const api = await localApi({ after(fn) { apiClose = fn; } }, (body, index) => {
      if (index > 0) return [{ type: 'text', text: 'SYNTHETIC_NEXT_REPLY' }];
      if (mode === 'text') return [{ type: 'text', text: 'SYNTHETIC_ABORTED_PARTIAL' }];
      const name = body.tools.find(item => item.name.includes('park')).name;
      return [{ type: 'tool_use', id: 'synthetic_tool_1', name, input: {} }];
    });
    // localApi already guards credentials, destination and HTTP shape. Hold only
    // its first SSE response after the text block, before message completion.
    const write = ServerResponse.prototype.write, end = ServerResponse.prototype.end;
    let held;
    if (mode === 'text') {
      ServerResponse.prototype.write = function(chunk, ...args) {
        if (!held && String(chunk).startsWith('event: message_start')) held = this;
        if (this === held && /^event: (message_(delta|stop)|content_block_stop)/.test(String(chunk))) return true;
        return write.call(this, chunk, ...args);
      };
      ServerResponse.prototype.end = function(...args) {
        if (this === held) return this;
        return end.apply(this, args);
      };
    }
    const child = new QueryProcess({ spawnFactory(command, args, options) {
      assert.equal(options.env.ANTHROPIC_BASE_URL, api.url);
      assert.equal(options.env.ANTHROPIC_API_KEY, SYNTHETIC_KEY);
      assert.equal(options.env.HOME, api.home);
      assert.equal(options.env.CLAUDE_CONFIG_DIR, api.config);
      assert.equal(options.env.CLAUDE_CODE_OAUTH_TOKEN, undefined);
      assert.equal(options.env.ANTHROPIC_AUTH_TOKEN, undefined);
      assert.equal(options.env.HTTPS_PROXY, api.url);
      return spawn(command, args, options);
    } });
    const push = text => {
      queue.push({ type: 'user', message: { role: 'user', content: text }, parent_tool_use_id: null, client_composed: true });
      wake?.(); wake = null;
    };
    async function* input() {
      while (!ended) {
        if (queue.length) yield queue.shift();
        else await new Promise(resolve => { wake = resolve; });
      }
    }
    const sdk = query({ prompt: input(), options: {
      cwd: api.cwd, model: 'claude-sonnet-4-6', env: api.env,
      tools: [], allowedTools: [],
      mcpServers: mode === 'text' ? {} : { synthetic: createSdkMcpServer({ name: 'synthetic', tools: [
        tool('park', 'Synthetic parked callback; performs no host action', {}, async () => {
          parked = true;
          return pendingTool;
        }),
      ] }) },
      strictMcpConfig: true, settingSources: [], settings: { ...isolatedSettings },
      plugins: [], skills: [], hooks: {}, canUseTool: async () => ({ behavior: 'allow', updatedInput: {} }),
      systemPrompt: 'Synthetic offline history probe.', persistSession: false,
      includePartialMessages: true, thinking: { type: 'disabled' },
      spawnClaudeCodeProcess: child.spawn, abortController: controller,
    } });
    let pumpError;
    const pump = (async () => { for await (const event of sdk) events.push(event); })().catch(error => { pumpError = error; });
    t.after(async () => {
      ServerResponse.prototype.write = write; ServerResponse.prototype.end = end;
      ended = true; wake?.(); release({ content: [{ type: 'text', text: 'SYNTHETIC_CLEANUP_ONLY' }] });
      await child.shutdown(sdk, controller); await pump;
      assert.ok(child.didExit); await apiClose(); api.assertHealthy();
    });
    push('SYNTHETIC_FIRST_USER');
    await until(() => mode === 'text'
      ? events.some(event => event.type === 'stream_event' && event.event?.delta?.text === 'SYNTHETIC_ABORTED_PARTIAL')
      : parked);
    const receipt = await sdk.interrupt();
    await until(() => events.some(event => event.type === 'result'));
    const interrupted = events.find(event => event.type === 'result');
    push('SYNTHETIC_NEXT_USER');
    await until(() => events.filter(event => event.type === 'result').length === 2);
    assert.equal(pumpError, undefined);
    assert.equal(api.requests.length, 2);
    assert.equal(child.didExit, false);
    const messages = api.requests[1].messages.map(message => ({ ...message, content: message.content.filter(block => !(block.type === 'text' && block.text.startsWith('<system-reminder>'))) }));
    // Only synthetic content and API message structure are emitted; never env,
    // request headers, system prompt, credentials, or real conversations.
    console.log(JSON.stringify({ kind: 'sdk_interrupt_history', mode, sdk: '0.3.308',
      old_result: interrupted.subtype, aborted_assistants: events.filter(event => event.type === 'assistant' && event.aborted).length,
      still_queued: receipt?.still_queued?.length ?? null, parked_callback_entered: parked,
      host_side_effects: 0, messages }));
    assert.deepEqual(messages.map(message => message.role), ['user', 'assistant', 'user']);
    assert.equal(messages[0].content[0].text, 'SYNTHETIC_FIRST_USER');
    assert.equal(messages[2].content.at(-1).text, 'SYNTHETIC_NEXT_USER');
    assert.ok(JSON.stringify(messages).includes('interrupted'), 'SDK synthetic interruption marker persists');
    if (mode === 'text') assert.ok(JSON.stringify(messages).includes('SYNTHETIC_ABORTED_PARTIAL'));
    else {
      assert.equal(messages[1].content[0].type, 'tool_use');
      assert.equal(messages[2].content[0].type, 'tool_result');
      assert.equal(messages[2].content[0].is_error, true);
      assert.equal(messages[2].content[0].tool_use_id, 'synthetic_tool_1');
      assert.ok(!JSON.stringify(messages).includes('SYNTHETIC_CLEANUP_ONLY'));
    }
    api.assertHealthy();
  });
}
