import { performance } from 'node:perf_hooks';
import { query } from '@anthropic-ai/claude-agent-sdk';
import { BridgeManager } from '../src/engine.mjs';
import { QueryProcess } from '../src/child.mjs';
import { importSession } from '../src/history.mjs';

// Observation only: production request, credential, and tool behavior is unchanged.
export function measuredManager(options = {}, observe = () => {}) {
  const marks = [], processes = [];
  const mark = (event, details = {}) => {
    const record = { event, at_ms: performance.now(), ...details };
    marks.push(record);
    observe(record);
  };
  let queryCount = 0;
  const manager = new BridgeManager({ ...options,
    processFactory() {
      const process = new QueryProcess();
      const index = processes.length;
      processes.push(process);
      const spawn = process.spawn;
      process.spawn = options => { mark('spawn', { query: index }); return spawn(options); };
      process.exited.then(() => mark('exit', { query: index }));
      return process;
    },
    async historyImporter(options) {
      const start = performance.now();
      const result = await importSession(options);
      mark('history_import', { duration_ms: performance.now() - start, messages: options.messages.length });
      return result;
    },
    queryFactory(options) {
      const index = queryCount++;
      mark('query_construct', { query: index, resumed: Boolean(options.options.resume) });
      const sdk = query(options);
      return {
        close: () => sdk.close(),
        async interrupt() {
          mark('interrupt_requested', { query: index });
          const receipt = await sdk.interrupt();
          mark('interrupt_ack', { query: index, queued_count: receipt?.still_queued?.length ?? 0 });
          return receipt;
        },
        async *[Symbol.asyncIterator]() {
          for await (const message of sdk) {
            if (message.type === 'system' && message.subtype === 'init')
              mark('sdk_init', { query: index, model: message.model, tools: message.tools?.length ?? null });
            if (message.type === 'result') mark('sdk_result', { query: index, is_error: Boolean(message.is_error),
              subtype: message.subtype, duration_api_ms: message.duration_api_ms ?? null });
            yield message;
          }
        },
      };
    },
  });
  const handle = manager.handle.bind(manager);
  manager.handle = async (body, context = {}) => {
    mark('request_received', { session: context.sessionId });
    const aborted = () => mark('request_aborted', { session: context.sessionId });
    context.signal?.addEventListener('abort', aborted, { once: true });
    try { return await handle(body, context); }
    finally {
      context.signal?.removeEventListener('abort', aborted);
      mark('request_settled', { session: context.sessionId });
    }
  };
  return { manager, marks, processes, mark };
}
