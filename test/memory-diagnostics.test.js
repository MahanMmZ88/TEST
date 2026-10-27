'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const fs = require('node:fs');
const http = require('node:http');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');

const ROOT = path.join(__dirname, '..');
const MB = 1024 * 1024;
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function freePort() {
  const server = net.createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

function request(port, method, pathname, body = null, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, method, path: pathname, headers }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('error', reject);
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
    });
    req.setTimeout(3000, () => req.destroy(new Error(`request timed out: ${pathname}`)));
    req.on('error', reject);
    req.end(body || undefined);
  });
}

function websocketUpgradeStatus(port) {
  return new Promise((resolve, reject) => {
    const req = http.request({
      host: '127.0.0.1', port, path: '/ws',
      headers: {
        Connection: 'Upgrade', Upgrade: 'websocket',
        'Sec-WebSocket-Key': crypto.randomBytes(16).toString('base64'),
        'Sec-WebSocket-Version': '13'
      }
    }, (res) => {
      res.resume();
      res.on('end', () => resolve(res.statusCode));
      res.on('error', reject);
    });
    req.on('upgrade', (res, socket) => {
      socket.destroy();
      reject(new Error(`expected a refused upgrade but received ${res.statusCode}`));
    });
    req.setTimeout(3000, () => req.destroy(new Error('WebSocket upgrade timed out')));
    req.on('error', reject);
    req.end();
  });
}

async function launchPanel(t, { usedMb = 300, limitMb = 1024, guard = true, users = [] } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'panel-memory-diagnostics-'));
  const dbPath = path.join(dir, 'db.json');
  const snapshot = path.join(dir, 'memory.json');
  const passwordHash = crypto.createHash('sha256').update('fixtureMAHAN_PANEL_NODE_WS_SALT_v2').digest('hex');
  fs.writeFileSync(dbPath, JSON.stringify({ admin: { username: 'fixture', password_hash: passwordHash }, users }));
  function setUsage(nextUsedMb) {
    const temp = `${snapshot}.tmp`;
    fs.writeFileSync(temp, JSON.stringify({ usedMb: nextUsedMb, limitMb }));
    fs.renameSync(temp, snapshot);
  }
  setUsage(usedMb);
  const port = await freePort();
  const child = spawn(process.execPath, [
    '--require', path.join(__dirname, 'helpers/mock-memory-guard.js'),
    '--require', path.join(__dirname, 'helpers/loopback.js'), path.join(ROOT, 'server.js')
  ], {
    cwd: ROOT,
    env: {
      ...process.env,
      PORT: String(port), DB_PATH: dbPath, PANEL_TEST_MEMORY_SNAPSHOT: snapshot,
      MEMORY_LIMIT_MB: '', MEMORY_GUARD: guard ? '1' : '0',
      MEMORY_GUARD_SOFT: '', MEMORY_GUARD_HARD: '', MEMORY_GUARD_RECOVER: '',
      MEMORY_GUARD_WAIT_MS: '', NODE_OPTIONS: '', KEEPALIVE_ENABLED: '0'
    },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let logs = '';
  child.stdout.on('data', (chunk) => { logs = (logs + chunk).slice(-30000); });
  child.stderr.on('data', (chunk) => { logs = (logs + chunk).slice(-30000); });
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = once(child, 'exit').catch(() => {});
      child.kill('SIGTERM');
      await Promise.race([exited, delay(1200)]);
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    }
    fs.rmSync(dir, { recursive: true, force: true });
  });
  for (let i = 0; i < 100; i++) {
    if (child.exitCode !== null) throw new Error(`panel exited before startup: ${logs}`);
    try {
      if ((await request(port, 'GET', '/health')).status === 200) return { port, setUsage };
    } catch (_) {}
    await delay(50);
  }
  throw new Error(`panel did not start: ${logs}`);
}

async function login(port) {
  const body = Buffer.from(JSON.stringify({ username: 'fixture', password: 'fixture' }));
  const result = await request(port, 'POST', '/api/login', body, {
    'content-type': 'application/json', 'content-length': body.length
  });
  assert.equal(result.status, 200);
  return result.headers['set-cookie'][0].split(';')[0];
}

async function health(port) {
  const result = await request(port, 'GET', '/api/health');
  assert.equal(result.status, 200);
  return JSON.parse(result.body);
}

async function stats(port, cookie) {
  const result = await request(port, 'GET', '/api/stats', null, { cookie });
  assert.equal(result.status, 200);
  return JSON.parse(result.body);
}

function captureBytes(stream, event) {
  let data = Buffer.alloc(0);
  let ended = null;
  const waiters = [];
  const settle = () => {
    for (let i = waiters.length - 1; i >= 0; i--) {
      const waiter = waiters[i];
      if (data.length >= waiter.length) {
        waiters.splice(i, 1);
        waiter.resolve(data.subarray(0, waiter.length));
      } else if (ended) {
        waiters.splice(i, 1);
        waiter.reject(ended);
      }
    }
  };
  stream.on(event, (chunk) => { data = Buffer.concat([data, Buffer.from(chunk)]); settle(); });
  stream.on('close', () => { ended = new Error('stream closed before expected echo'); settle(); });
  stream.on('error', (err) => { ended = err; settle(); });
  return (length) => {
    if (data.length >= length) return Promise.resolve(data.subarray(0, length));
    if (ended) return Promise.reject(ended);
    return new Promise((resolve, reject) => waiters.push({ length, resolve, reject }));
  };
}

async function echoTarget(t) {
  const sockets = new Set();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('data', (data) => socket.write(data));
    socket.on('close', () => sockets.delete(socket));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => { for (const socket of sockets) socket.destroy(); server.close(); });
  return server.address().port;
}

function vlessHeader(port) {
  const uuid = Buffer.from('11111111111111111111111111111111', 'hex');
  const name = Buffer.from('target.test');
  const p = Buffer.alloc(2);
  p.writeUInt16BE(port);
  return Buffer.concat([Buffer.from([0]), uuid, Buffer.from([0, 1]), p, Buffer.from([2, name.length]), name]);
}

function downlink(port, sid) {
  let req;
  const response = new Promise((resolve, reject) => {
    req = http.get({ host: '127.0.0.1', port, path: `/xhttp/${sid}` }, resolve);
    req.on('error', reject);
  });
  response.catch(() => {});
  return { response, destroy: () => req.destroy() };
}

function timeout(promise, ms, label) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`timeout: ${label}`)), ms); })
  ]).finally(() => clearTimeout(timer));
}

test('memory diagnostics', async (suite) => {
  await suite.test('dashboard reports the constrained cgroup rather than host RAM on a 1 GiB plan', async (t) => {
    const panel = await launchPanel(t);
    const snapshot = await health(panel.port);
    const dashboard = await stats(panel.port, await login(panel.port));
    assert.equal(snapshot.build, 'stability-2026-10-27-a');
    assert.equal(snapshot.low_memory_mode, false);
    assert.equal(snapshot.memory_guard.used_mb, 300);
    assert.equal(snapshot.memory_guard.limit_mb, 1024);
    assert.equal(dashboard.ram_source, 'cgroup-v2');
    assert.equal(dashboard.ram_used, 300 * MB);
    assert.equal(dashboard.ram_total, 1024 * MB);
    assert.equal(dashboard.ram_usage, 29);
  });

  await suite.test('dashboard retains cgroup-based reporting for 128 MiB plans', async (t) => {
    const panel = await launchPanel(t, { usedMb: 64, limitMb: 128 });
    const snapshot = await health(panel.port);
    const dashboard = await stats(panel.port, await login(panel.port));
    assert.equal(snapshot.low_memory_mode, true);
    assert.equal(dashboard.ram_source, 'cgroup-v2');
    assert.equal(dashboard.ram_used, 64 * MB);
    assert.equal(dashboard.ram_total, 128 * MB);
    assert.equal(dashboard.ram_usage, 50);
  });

  await suite.test('dashboard refreshes cgroup usage when the memory guard is disabled', async (t) => {
    const panel = await launchPanel(t, { guard: false });
    const cookie = await login(panel.port);
    panel.setUsage(400);
    const dashboard = await stats(panel.port, cookie);
    assert.equal(dashboard.ram_source, 'cgroup-v2');
    assert.equal(dashboard.ram_used, 400 * MB);
    assert.equal(dashboard.ram_total, 1024 * MB);
    assert.equal(dashboard.ram_usage, 39);
  });

  await suite.test('RSS fallback is identified when a limit is known but cgroup usage is unavailable', async (t) => {
    const panel = await launchPanel(t, { usedMb: 0 });
    const snapshot = await health(panel.port);
    const dashboard = await stats(panel.port, await login(panel.port));
    assert.equal(snapshot.memory_guard.usage_source, 'rss');
    assert.equal(dashboard.ram_source, 'rss');
    assert.equal(dashboard.ram_total, 1024 * MB);
    assert.ok(dashboard.ram_used > 0);
  });

  await suite.test('dashboard falls back to system stats when no memory limit is known', async (t) => {
    const panel = await launchPanel(t, { limitMb: 0 });
    const snapshot = await health(panel.port);
    const dashboard = await stats(panel.port, await login(panel.port));
    assert.equal(snapshot.memory_guard.active, false);
    assert.ok(['proc-meminfo', 'cgroup-v2-raw', 'cgroup-v1-raw', 'os'].includes(dashboard.ram_source));
    assert.ok(dashboard.ram_total > 0);
  });

  await suite.test('hard pressure preserves established WS and XHTTP traffic while refusing new sessions', async (t) => {
    const users = [{ id: 'pressure-user', remark: 'test', uuid: '11111111111111111111111111111111',
      is_active: 1, traffic_limit: 0, connection_limit: 0 }];
    const panel = await launchPanel(t, { users });
    const port = await echoTarget(t);
    const before = Buffer.from('before');
    const after = Buffer.from('after');
    const expectedBefore = Buffer.concat([Buffer.from([0, 0]), before]);
    const expectedAll = Buffer.concat([expectedBefore, after]);

    const ws = new WebSocket(`ws://127.0.0.1:${panel.port}/ws`);
    t.after(() => ws.terminate());
    const wsBytes = captureBytes(ws, 'message');
    await timeout(once(ws, 'open'), 3000, 'WS before pressure');
    ws.send(Buffer.concat([vlessHeader(port), before]));
    assert.deepEqual(await timeout(wsBytes(expectedBefore.length), 3000, 'WS first echo'), expectedBefore);

    const sid = 'pressure-existing-session';
    const get = downlink(panel.port, sid);
    t.after(get.destroy);
    const post = await request(panel.port, 'POST', `/xhttp/${sid}/0`, Buffer.concat([vlessHeader(port), before]));
    assert.equal(post.status, 200);
    const res = await timeout(get.response, 3000, 'XHTTP before pressure');
    t.after(() => res.destroy());
    const xhttpBytes = captureBytes(res, 'data');
    assert.deepEqual(await timeout(xhttpBytes(expectedBefore.length), 3000, 'XHTTP first echo'), expectedBefore);

    panel.setUsage(860);
    let current;
    for (let i = 0; i < 50; i++) {
      current = await health(panel.port);
      if (current.memory_guard.state === 'hard') break;
      await delay(20);
    }
    assert.equal(current.memory_guard.state, 'hard');
    assert.equal(await websocketUpgradeStatus(panel.port), 503);
    assert.equal((await request(panel.port, 'GET', '/xhttp/new-pressure-session')).status, 503);
    ws.send(after);
    assert.equal((await request(panel.port, 'POST', `/xhttp/${sid}/1`, after)).status, 200);
    assert.deepEqual(await timeout(wsBytes(expectedAll.length), 3000, 'WS echo during pressure'), expectedAll);
    assert.deepEqual(await timeout(xhttpBytes(expectedAll.length), 3000, 'XHTTP echo during pressure'), expectedAll);
    assert.equal(ws.readyState, WebSocket.OPEN);
  });

  await suite.test('a simulated 84% cgroup reading enters hard pressure and rejects new WS/XHTTP attempts', async (t) => {
    const panel = await launchPanel(t);
    const ws = new WebSocket(`ws://127.0.0.1:${panel.port}/ws`);
    ws.on('error', () => {});
    t.after(() => ws.terminate());
    await Promise.race([once(ws, 'open'), delay(3000).then(() => { throw new Error('WebSocket open timed out'); })]);
    panel.setUsage(860);
    let current;
    for (let i = 0; i < 50; i++) {
      current = await health(panel.port);
      if (current.memory_guard.state === 'hard') break;
      await delay(20);
    }
    assert.equal(current.low_memory_mode, false);
    assert.equal(current.memory_guard.state, 'hard');
    assert.equal(current.memory_guard.used_mb, 860);
    assert.equal(current.memory_guard.hard_ratio, 0.78);
    const rejectedBefore = current.memory_guard.rejected_new_connections;
    assert.equal(await websocketUpgradeStatus(panel.port), 503);
    assert.equal((await request(panel.port, 'GET', '/xhttp/new-session')).status, 503);
    const rejectedAfter = (await health(panel.port)).memory_guard.rejected_new_connections;
    assert.equal(rejectedAfter, rejectedBefore + 2);
    assert.equal(ws.readyState, WebSocket.OPEN, 'existing WebSocket remains connected');
  });
});
