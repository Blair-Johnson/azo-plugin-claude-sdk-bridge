// Pipe session-replay.py --request here. No live API or host tools execute.
import { offlineSdk } from '../tests/fixtures/local-api.mjs';
import { errorText } from '../src/errors.mjs';

const cleanups = [];
let calls = 0;
try {
  let input = '';
  for await (const chunk of process.stdin) input += chunk;
  const body = JSON.parse(input);
  const runtime = await offlineSdk({ after: fn => cleanups.push(fn) }, request => {
    calls++;
    console.log(JSON.stringify({ kind: 'upstream_received', messages: request.messages.length,
      tools: request.tools.length, content_blocks: request.messages.reduce((sum, m) => sum + m.content.length, 0) }));
    return [{ type: 'text', text: 'Offline replay accepted.' }];
  });
  const manager = runtime.makeManager();
  const result = await manager.handle(body, { sessionId: 'incident-offline-replay', signal: runtime.controller.signal });
  console.log(JSON.stringify({ kind: 'replay_result', stop_reason: result.stop_reason, calls }));
  runtime.api.assertHealthy();
} catch (error) {
  console.log(JSON.stringify({ kind: 'replay_failed', status: error.statusCode,
    reason: errorText(error), calls }));
  process.exitCode = 1;
} finally {
  for (const close of cleanups.reverse()) await close();
}
