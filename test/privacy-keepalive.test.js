'use strict';

// Loopback-only tests. The self-ping timer is accelerated by a test preload;
// these tests do not simulate a real 30–60 minute hosting-provider soak.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const crypto = require('node:crypto');
const fs = require('node:fs');
const http = require('node:http');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');

const ROOT = path.join(__dirname, '..');
const UUID = '11111111111111111111111111111111';
const REMARK = 'sensitive-fixture-remark';
const CLIENT_IP = '8.8.4.4';
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function timeout(promise, ms, label) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`timeout: ${label}`)), ms); })
  ]).finally(() => clearTimeout(timer));
}

async function eventually(fn, label, maxMs = 4000) {
  const until = Date.now() + maxMs;
  while (Date.now() < until) {
    const value = await fn();
    if (value) return value;
    await delay(30);
  }
  throw new Error(`timeout: ${label}`);
}

async function freePort() {
  const server = net.createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

function request(port, method, route, { json, body, headers = {} } = {}) {
  if (json !== undefined) {
    body = Buffer.from(JSON.stringify(json));
    headers = { ...headers, 'content-type': 'application/json', 'content-length': body.length };
  } else if (body) {
    headers = { ...headers, 'content-type': 'application/octet-stream', 'content-length': body.length };
  }
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, path: route, method, headers }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.setTimeout(4000, () => req.destroy(new Error(`request timed out: ${route}`)));
    req.on('error', reject);
    req.end(body || undefined);
  });
}

async function stop(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, 'exit').catch(() => {});
  child.kill('SIGTERM');
  await Promise.race([exited, delay(1200)]);
  if (child.exitCode === null && child.signalCode === null) {
    child.kill('SIGKILL');
    await exited;
  }
}

async function launchPanel(t, overrides = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'panel-privacy-test-'));
  const dbPath = path.join(dir, 'db.json');
  const passwordHash = crypto.createHash('sha256').update('fixtureMAHAN_PANEL_NODE_WS_SALT_v2').digest('hex');
  fs.writeFileSync(dbPath, JSON.stringify({ admin: { username: 'fixture', password_hash: passwordHash }, users: [
    { id: 'fixture-user', remark: REMARK, uuid: UUID, sub_id: 'fixture-sub', is_active: 1,
      traffic_limit: 0, connection_limit: 0 }
  ] }));
  const children = [];
  t.after(async () => {
    await Promise.all(children.map(stop));
    fs.rmSync(dir, { recursive: true, force: true });
  });
  for (let attempt = 0; attempt < 4; attempt++) {
    const port = await freePort();
    const child = spawn(process.execPath, [
      '--require', path.join(__dirname, 'helpers/loopback.js'),
      '--require', path.join(__dirname, 'helpers/short-keepalive.js'),
      path.join(ROOT, 'server.js')
    ], { cwd: ROOT, env: {
      ...process.env, PORT: String(port), DB_PATH: dbPath, NODE_OPTIONS: '', MEMORY_GUARD: '0',
      REALITY_ENABLED: '0', KEEPALIVE_ENABLED: '0', KEEPALIVE_URL: '', KEEPALIVE_AUTO_DETECT: '1',
      KEEPALIVE_USER_AGENT: '', KEEPALIVE_INTERVAL_MS: '', PANEL_TEST_KEEPALIVE_TICK_MS: '150',
      LOG_SENSITIVE_DETAILS: '0', PUBLIC_HEALTH_MINIMAL: '0', DEBUG_ACTIVE_CLIENTS: '0', ...overrides
    }, stdio: ['ignore', 'pipe', 'pipe'] });
    children.push(child);
    let logs = '';
    child.stdout.on('data', (chunk) => { logs = (logs + chunk).slice(-200000); });
    child.stderr.on('data', (chunk) => { logs = (logs + chunk).slice(-200000); });
    try {
      await eventually(async () => {
        if (child.exitCode !== null || child.signalCode !== null) throw new Error(`panel exited: ${logs}`);
        if (!logs.includes('[INFO] Boot:')) return false;
        try { return (await request(port, 'GET', '/health')).status === 200; } catch (_) { return false; }
      }, 'panel startup');
      return { port, child, logs: () => logs, stop: () => stop(child) };
    } catch (err) {
      await stop(child);
      await delay(20);
      if (attempt < 3 && /EADDRINUSE/.test(logs)) continue;
      throw new Error(`${err.message}\n${logs}`);
    }
  }
  throw new Error('could not reserve a panel port');
}

async function login(port) {
  const res = await request(port, 'POST', '/api/login', { json: { username: 'fixture', password: 'fixture' } });
  assert.equal(res.status, 200);
  return res.headers['set-cookie'][0].split(';')[0];
}

async function snapshot(port, cookie) {
  const res = await request(port, 'GET', '/api/keepalive', { headers: { cookie } });
  assert.equal(res.status, 200);
  return JSON.parse(res.body);
}

function buffered(stream, event = 'data') {
  let bytes = Buffer.alloc(0), ended = null;
  const waiters = [];
  function settle() {
    for (let i = waiters.length - 1; i >= 0; i--) {
      const w = waiters[i];
      if (bytes.length >= w.size) {
        waiters.splice(i, 1);
        const value = bytes.subarray(0, w.size);
        bytes = bytes.subarray(w.size);
        w.resolve(value);
      } else if (ended) {
        waiters.splice(i, 1);
        w.reject(ended);
      }
    }
  }
  stream.on(event, (chunk) => { bytes = Buffer.concat([bytes, Buffer.from(chunk)]); settle(); });
  stream.on('error', (err) => { ended = err; settle(); });
  stream.on('close', () => { ended = ended || new Error('stream closed'); settle(); });
  stream.on('end', () => { ended = ended || new Error('stream ended'); settle(); });
  return (size) => timeout(new Promise((resolve, reject) => { waiters.push({ size, resolve, reject }); settle(); }), 4000, 'stream bytes');
}

function vlessHeader(port, host = 'target.test') {
  const name = Buffer.from(host), p = Buffer.alloc(2);
  p.writeUInt16BE(port);
  return Buffer.concat([Buffer.from([0]), Buffer.from(UUID, 'hex'), Buffer.from([0, 1]), p,
    Buffer.from([2, name.length]), name]);
}

async function echoTarget(t) {
  const sockets = new Set();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('close', () => sockets.delete(socket));
    socket.pipe(socket);
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => { for (const s of sockets) s.destroy(); server.close(); });
  return server.address().port;
}

async function wsSession(t, port, target) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`, { headers: { 'x-forwarded-for': CLIENT_IP } });
  const read = buffered(ws, 'message');
  t.after(() => ws.terminate());
  await timeout(once(ws, 'open'), 4000, 'WebSocket open');
  const first = Buffer.from('ws-before');
  ws.send(Buffer.concat([vlessHeader(target), first]));
  assert.deepEqual(await read(first.length + 2), Buffer.concat([Buffer.from([0, 0]), first]));
  return { ws, read };
}

async function xhttpSession(t, port, target, sid = 'sensitive-session-id') {
  const get = http.get({ host: '127.0.0.1', port, path: `/xhttp/${sid}` });
  get.on('error', () => {});
  t.after(() => get.destroy());
  const response = once(get, 'response');
  response.catch(() => {});
  const initial = Buffer.from('xhttp-before');
  const post = await request(port, 'POST', `/xhttp/${sid}/0`, { body: Buffer.concat([vlessHeader(target), initial]) });
  assert.equal(post.status, 200);
  const [res] = await timeout(response, 4000, 'XHTTP downlink');
  const read = buffered(res);
  assert.deepEqual(await read(initial.length + 2), Buffer.concat([Buffer.from([0, 0]), initial]));
  return { res, sid, read };
}

async function pingSink(t, { hang = false, status = 200 } = {}) {
  const hits = [], sockets = new Set();
  const server = http.createServer((req, res) => {
    hits.push({ path: req.url, userAgent: req.headers['user-agent'], closed: false });
    const hit = hits[hits.length - 1];
    res.on('close', () => { hit.closed = true; });
    if (!hang) { res.writeHead(status); res.end('ok'); }
  });
  server.on('connection', (socket) => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => { for (const s of sockets) s.destroy(); server.close(); });
  return { hits, port: server.address().port };
}

test('minimal public health hides details from anonymous users but preserves the authenticated dashboard', async (t) => {
  const panel = await launchPanel(t, { PUBLIC_HEALTH_MINIMAL: '1' });
  for (const route of ['/health', '/healthz']) {
    const res = await request(panel.port, 'GET', route);
    assert.equal(res.status, 200);
    assert.equal(res.body.toString(), 'ok\n');
    assert.equal(res.headers['x-powered-by'], undefined);
  }
  for (const headers of [{}, { cookie: 'admin_session=not-a-valid-session' }]) {
    const res = await request(panel.port, 'GET', '/api/health', { headers });
    assert.equal(res.status, 200);
    assert.deepEqual(JSON.parse(res.body), { ok: true });
    assert.match(res.headers['cache-control'], /no-store/);
    assert.match(res.headers.vary, /Cookie/i);
  }
  const cookie = await login(panel.port);
  const res = await request(panel.port, 'GET', '/api/health', { headers: { cookie } });
  const health = JSON.parse(res.body);
  assert.equal(health.build, 'optional-reality-r1');
  assert.ok(health.transports.websocket && health.transports.xhttp && health.memory_guard);
  assert.match(res.headers['cache-control'], /no-store/);
});

test('public health remains detailed by default for existing monitoring', async (t) => {
  const panel = await launchPanel(t);
  const res = await request(panel.port, 'GET', '/api/health');
  assert.equal(JSON.parse(res.body).build, 'optional-reality-r1');
  assert.equal(res.headers['x-powered-by'], undefined);
  assert.match((await request(panel.port, 'GET', '/health')).body.toString(), /optional-reality-r1/);
});

test('self-ping pause is authenticated, aborts pending pings, and leaves active WS/XHTTP usable', async (t) => {
  const sink = await pingSink(t, { hang: true });
  const panel = await launchPanel(t, {
    KEEPALIVE_ENABLED: '1', KEEPALIVE_URL: `http://127.0.0.1:${sink.port}`,
    KEEPALIVE_USER_AGENT: 'health-check/1.0', PUBLIC_HEALTH_MINIMAL: '1'
  });
  const cookie = await login(panel.port);
  assert.equal((await request(panel.port, 'POST', '/api/keepalive', { json: { enabled: false } })).status, 401);
  assert.equal((await request(panel.port, 'POST', '/api/keepalive', { json: { enabled: 'false' }, headers: { cookie } })).status, 400);
  const target = await echoTarget(t);
  const ws = await wsSession(t, panel.port, target);
  const xhttp = await xhttpSession(t, panel.port, target);
  await eventually(async () => (await snapshot(panel.port, cookie)).in_flight > 0 && sink.hits.length > 0, 'pending self-ping');
  assert.equal(sink.hits[0].path, '/health');
  assert.equal(sink.hits[0].userAgent, 'health-check/1.0');
  const paused = await request(panel.port, 'POST', '/api/keepalive', { json: { enabled: false }, headers: { cookie } });
  assert.equal(paused.status, 200);
  assert.deepEqual({ enabled: JSON.parse(paused.body).enabled, temporary: JSON.parse(paused.body).temporary,
    configured_enabled: JSON.parse(paused.body).configured_enabled },
  { enabled: false, temporary: true, configured_enabled: true });
  await eventually(async () => (await snapshot(panel.port, cookie)).in_flight === 0 && sink.hits.every((h) => h.closed), 'aborted self-ping');
  await delay(250);
  const stopped = sink.hits.length;
  await delay(450); // three accelerated ticks; no real-time hosting claim
  assert.equal(sink.hits.length, stopped);

  const afterWs = Buffer.from('ws-after');
  ws.ws.send(afterWs);
  assert.deepEqual(await ws.read(afterWs.length), afterWs);
  assert.equal(ws.ws.readyState, WebSocket.OPEN);
  const afterXhttp = Buffer.from('xhttp-after');
  assert.equal((await request(panel.port, 'POST', `/xhttp/${xhttp.sid}/1`, { body: afterXhttp })).status, 200);
  assert.deepEqual(await xhttp.read(afterXhttp.length), afterXhttp);
  assert.equal(xhttp.res.destroyed || xhttp.res.readableEnded, false);

  const resumed = await request(panel.port, 'POST', '/api/keepalive', { json: { enabled: true }, headers: { cookie } });
  assert.equal(JSON.parse(resumed.body).enabled, true);
  await eventually(() => sink.hits.length > stopped, 'self-ping resumes');
  await panel.stop();
  const restarted = await launchPanel(t, {
    KEEPALIVE_ENABLED: '1', KEEPALIVE_URL: `http://127.0.0.1:${sink.port}`
  });
  const nextCookie = await login(restarted.port);
  assert.equal((await snapshot(restarted.port, nextCookie)).enabled, true);
});

test('auto-discovery can be disabled and cannot be directed by a Host header', async (t) => {
  const panel = await launchPanel(t, { KEEPALIVE_ENABLED: '1', KEEPALIVE_AUTO_DETECT: '0' });
  const cookie = await login(panel.port);
  await request(panel.port, 'GET', '/health', { headers: { host: 'untrusted.example:8888' } });
  await delay(550);
  const state = await snapshot(panel.port, cookie);
  assert.equal(state.enabled, true);
  assert.equal(state.auto_detect, false);
  assert.equal(state.source, 'none');
  assert.equal(state.total_pings, 0);
  assert.equal(state.url, '(not set)');
});

test('invalid and overflowing keepalive intervals cannot become 1ms timers', async (t) => {
  for (const [setting, expected] of [['not-a-number', 240000], ['Infinity', 240000], ['1e100', 2147483647], ['0', 60000]]) {
    await t.test(setting, async (sub) => {
      const panel = await launchPanel(sub, { KEEPALIVE_ENABLED: '1', KEEPALIVE_AUTO_DETECT: '0', KEEPALIVE_INTERVAL_MS: setting });
      const cookie = await login(panel.port);
      assert.equal((await snapshot(panel.port, cookie)).interval_ms, expected);
    });
  }
});

test('invalid user agents fall back to the legacy header and HTTP errors are not counted as successes', async (t) => {
  const sink = await pingSink(t, { status: 503 });
  const panel = await launchPanel(t, { KEEPALIVE_ENABLED: '1', KEEPALIVE_URL: `http://127.0.0.1:${sink.port}`,
    KEEPALIVE_USER_AGENT: 'bad\nheader' });
  const cookie = await login(panel.port);
  await eventually(() => sink.hits.length >= 1, 'self-ping with invalid agent');
  assert.equal(sink.hits[0].userAgent, 'Mahan-Panel-Keepalive/2.0');
  const state = await eventually(async () => {
    const value = await snapshot(panel.port, cookie);
    return value.last_error === 'HTTP 503' ? value : false;
  }, 'HTTP error recorded');
  assert.equal(state.last_success_at, 'never');
});

test('default logs omit per-connection details and new user credentials', async (t) => {
  const panel = await launchPanel(t);
  const cookie = await login(panel.port);
  const created = await request(panel.port, 'POST', '/api/users', { json: { remark: 'sensitive-new-remark' }, headers: { cookie } });
  assert.equal(created.status, 200);
  const newUser = JSON.parse(created.body).user;
  const edited = await request(panel.port, 'PUT', `/api/users/${newUser.id}`,
    { json: { remark: 'sensitive-new-remark-edited' }, headers: { cookie } });
  assert.equal(edited.status, 200);
  const target = await echoTarget(t);
  await wsSession(t, panel.port, target);
  await xhttpSession(t, panel.port, target);
  const refusedPort = await freePort();
  const failed = new WebSocket(`ws://127.0.0.1:${panel.port}/ws`, { headers: { 'x-forwarded-for': CLIENT_IP } });
  failed.on('error', () => {});
  t.after(() => failed.terminate());
  await timeout(once(failed, 'open'), 4000, 'failed-target WebSocket open');
  failed.send(vlessHeader(refusedPort));
  await eventually(() => panel.logs().includes('[Remote] Error:'), 'redacted error log');
  failed.terminate();
  for (const secret of [UUID, newUser.uuid, REMARK, 'sensitive-new-remark', CLIENT_IP, 'target.test', 'sensitive-session-id']) {
    assert.ok(!panel.logs().includes(secret), `log unexpectedly included ${secret}\n${panel.logs()}`);
  }
  assert.match(panel.logs(), /\[Remote\] Error: E[A-Z0-9_]+/);
});

test('verbose logging is explicitly opt-in and still does not log created or edited UUIDs', async (t) => {
  const panel = await launchPanel(t, { LOG_SENSITIVE_DETAILS: '1' });
  const cookie = await login(panel.port);
  const created = await request(panel.port, 'POST', '/api/users', { json: { remark: 'verbose-client' }, headers: { cookie } });
  const user = JSON.parse(created.body).user;
  await request(panel.port, 'PUT', `/api/users/${user.id}`, { json: { remark: 'verbose-client-edited' }, headers: { cookie } });
  const target = await echoTarget(t);
  await wsSession(t, panel.port, target);
  await eventually(() => panel.logs().includes('target.test'), 'verbose connection log');
  assert.ok(panel.logs().includes(REMARK));
  assert.ok(!panel.logs().includes(user.uuid), 'new user UUID must not be logged');
});

test('legacy self-ping auto-detection stays enabled by default', async (t) => {
  const panel = await launchPanel(t, { KEEPALIVE_ENABLED: '1' });
  const cookie = await login(panel.port);
  const state = await snapshot(panel.port, cookie);
  assert.equal(state.auto_detect, true);
  assert.equal(state.source, 'auto-detected');
  assert.equal(state.url, `http://127.0.0.1:${panel.port}/health`);
  await eventually(async () => (await snapshot(panel.port, cookie)).last_success_at !== 'never', 'legacy auto self-ping');
});

test('resuming an env-disabled self-ping is temporary and a restart restores the disabled default', async (t) => {
  const sink = await pingSink(t);
  const env = { KEEPALIVE_ENABLED: '0', KEEPALIVE_URL: `http://127.0.0.1:${sink.port}` };
  const panel = await launchPanel(t, env);
  const cookie = await login(panel.port);
  await delay(400);
  assert.equal(sink.hits.length, 0);
  const enabled = await request(panel.port, 'POST', '/api/keepalive', { json: { enabled: true }, headers: { cookie } });
  assert.equal(JSON.parse(enabled.body).configured_enabled, false);
  await eventually(() => sink.hits.length > 0, 'resumed self-ping');
  await panel.stop();
  const count = sink.hits.length;
  const restarted = await launchPanel(t, env);
  const nextCookie = await login(restarted.port);
  const state = await snapshot(restarted.port, nextCookie);
  assert.equal(state.enabled, false);
  assert.equal(state.configured_enabled, false);
  await delay(400);
  assert.equal(sink.hits.length, count);
});
