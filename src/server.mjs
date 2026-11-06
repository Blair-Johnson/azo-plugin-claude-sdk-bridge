import http from 'node:http';
import { createHash, timingSafeEqual } from 'node:crypto';
import { invalid, validateRequest, validateSessionId } from './request.mjs';
import { errorText } from './errors.mjs';

const hash = s => createHash('sha256').update(s).digest();
const loopback = address => ['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(address);

function errorBody(error) {
  const status = Number.isInteger(error?.statusCode) && error.statusCode >= 400 && error.statusCode < 600
    ? error.statusCode : 502;
  const type = typeof error?.errorType === 'string' ? error.errorType
    : status === 429 ? 'rate_limit_error' : status === 401 ? 'authentication_error' :
      status >= 500 ? 'api_error' : 'invalid_request_error';
  // Hosts usually show only the start of a message: lead with the provider's
  // actionable text (e.g. a usage-limit reset), then the full diagnostics.
  const details = errorText(error);
  const message = typeof error?.providerMessage === 'string' && error.providerMessage
    ? `${error.providerMessage}\n\n${details}` : details;
  return { status, body: { type: 'error', error: { type, message } } };
}

function json(res, status, body) {
  if (res.destroyed || res.writableEnded) return;
  res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  res.end(JSON.stringify(body));
}

async function readBody(req, limit) {
  const declared = Number(req.headers['content-length']);
  if (Number.isFinite(declared) && declared > limit) throw invalid('Request body too large.', 413);
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit) throw invalid('Request body too large.', 413);
    chunks.push(chunk);
  }
  try { return JSON.parse(Buffer.concat(chunks).toString('utf8')); }
  catch { throw invalid('Invalid JSON body.'); }
}

// HTTP transport only. The SDK manager owns queries; Agent Zoo owns tool effects.
export function createBridgeServer({ manager, key, maxBodyBytes = 32 * 1024 * 1024,
  requestTimeoutMs = 0, pingMs = 10000 } = {}) {
  if (!manager || typeof key !== 'string' || key.length < 32)
    throw new Error('A manager and local bridge key of at least 32 characters are required.');
  const secret = hash(key);
  const active = new Set();
  let closing = false;
  const server = http.createServer(async (req, res) => {
    try {
      if (closing) throw invalid('Bridge is shutting down.', 503);
      if (!loopback(req.socket.remoteAddress)) throw invalid('Loopback access only.', 403);
      const host = req.headers.host || '';
      if (!/^(127\.0\.0\.1|localhost|\[::1\])(?::[0-9]{1,5})?$/.test(host) || req.headers.origin)
        throw invalid('Browser and non-loopback host requests are not accepted.', 403);
      const supplied = req.headers['x-api-key'];
      if (typeof supplied !== 'string' || !timingSafeEqual(hash(supplied), secret))
        throw invalid('Invalid local bridge key.', 401);
      const url = new URL(req.url, 'http://127.0.0.1');
      if (req.method === 'GET' && url.pathname === '/health') {
        json(res, 200, { status: 'ok', backend: 'claude-agent-sdk', version: '0.1.0' });
        return;
      }
      if (req.method === 'DELETE' && url.pathname === '/v1/session') {
        await manager.cancel(validateSessionId(req.headers['x-azo-session-id']));
        json(res, 200, { canceled: true });
        return;
      }
      if (req.method !== 'POST' || url.pathname !== '/v1/messages') throw invalid('Unknown endpoint.', 404);
      const sessionId = validateSessionId(req.headers['x-azo-session-id']);
      if (!(req.headers['content-type'] || '').toLowerCase().startsWith('application/json'))
        throw invalid('Content-Type must be application/json.', 415);
      const body = validateRequest(await readBody(req, maxBodyBytes));
      await respond(manager, body, { req, res, sessionId, active, requestTimeoutMs, pingMs });
    } catch (error) {
      const { status, body } = errorBody(error);
      json(res, status, body);
    }
  });
  server.headersTimeout = 15000;
  server.maxRequestsPerSocket = 100;
  return {
    server,
    async listen(port = 8766) {
      await new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(port, '127.0.0.1', () => { server.off('error', reject); resolve(); });
      });
      return server.address();
    },
    async close() {
      closing = true;
      for (const controller of active) controller.abort();
      server.closeAllConnections();
      await Promise.all([new Promise(resolve => server.close(resolve)), manager.close()]);
    },
  };
}

async function respond(manager, body, { req, res, sessionId, active, requestTimeoutMs, pingMs }) {
  const controller = new AbortController();
  active.add(controller);
  let complete = false;
  let ping;
  const disconnected = () => { if (!complete) controller.abort(); };
  req.once('aborted', disconnected);
  res.once('close', disconnected);
  // Optional total deadline (0 = none). Disconnects still cancel inference.
  const timer = requestTimeoutMs > 0 ? setTimeout(() => controller.abort(), requestTimeoutMs) : undefined;
  timer?.unref();
  const send = event => {
    if (controller.signal.aborted || res.destroyed || res.writableEnded) return;
    if (res.writableLength > 16 * 1024 * 1024) {
      controller.abort();
      return;
    }
    res.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
  };
  let abortListener;
  try {
    if (body.stream) {
      res.writeHead(200, { 'content-type': 'text/event-stream; charset=utf-8',
        'cache-control': 'no-store', connection: 'keep-alive', 'x-accel-buffering': 'no' });
      res.flushHeaders();
      ping = setInterval(() => send({ type: 'ping' }), pingMs);
      ping.unref();
    }
    const aborted = new Promise((_, reject) => {
      abortListener = () => reject(invalid('Bridge request canceled or timed out.', 504));
      controller.signal.addEventListener('abort', abortListener, { once: true });
      if (controller.signal.aborted) abortListener();
    });
    const message = await Promise.race([aborted,
      manager.handle(body, { sessionId, signal: controller.signal, onEvent: body.stream ? send : undefined,
        allowTransportHandoff: true }),
    ]);
    complete = true;
    if (body.stream) res.end();
    else json(res, 200, message);
  } catch (error) {
    complete = true;
    const failure = errorBody(error);
    if (body.stream && !res.destroyed && !res.writableEnded) {
      // Abort disables normal writes, but the client still needs a terminal API error.
      res.write(`event: error\ndata: ${JSON.stringify(failure.body)}\n\n`);
      res.end();
    } else json(res, failure.status, failure.body);
  } finally {
    clearTimeout(timer);
    clearInterval(ping);
    controller.signal.removeEventListener('abort', abortListener);
    req.off('aborted', disconnected);
    res.off('close', disconnected);
    active.delete(controller);
  }
}
