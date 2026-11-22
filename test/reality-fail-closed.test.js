'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const crypto = require('node:crypto');
const { once } = require('node:events');
const fs = require('node:fs');
const http = require('node:http');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');

const ROOT = path.join(__dirname, '..');
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const UUID = '11111111111111111111111111111111';
const privateKey = crypto.generateKeyPairSync('x25519').privateKey.export({ format: 'jwk' }).d;

async function freePort() {
  const server = net.createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}
function request(port, route, { method = 'GET', headers = {}, body = null } = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, path: route, method, headers }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
    });
    req.on('error', reject);
    req.end(body || undefined);
  });
}
async function wsEcho(port, targetPort) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  ws.on('error', () => {});
  await once(ws, 'open');
  const host = Buffer.from('target.test');
  const p = Buffer.alloc(2); p.writeUInt16BE(targetPort);
  const payload = Buffer.from('unchanged');
  const packet = Buffer.concat([Buffer.from([0]), Buffer.from(UUID, 'hex'), Buffer.from([0, 1]), p,
    Buffer.from([2, host.length]), host, payload]);
  let data = Buffer.alloc(0);
  let timer;
  try {
    const result = new Promise((resolve, reject) => {
      timer = setTimeout(() => reject(new Error('WS echo timeout')), 5000);
      ws.on('message', (chunk) => {
        data = Buffer.concat([data, Buffer.from(chunk)]);
        if (data.length >= payload.length + 2) resolve(data);
      });
      ws.on('close', () => reject(new Error('WS closed before echo')));
    });
    ws.send(packet);
    assert.deepEqual(await result, Buffer.concat([Buffer.from([0, 0]), payload]));
  } finally { clearTimeout(timer); ws.terminate(); }
}
async function stop(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, 'exit').catch(() => {});
  child.kill('SIGTERM');
  await Promise.race([exited, delay(1200)]);
  if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
}

async function launchPanel(db, snapshot, limit, item) {
  // Other test files also choose ephemeral ports. Retry only a confirmed bind
  // collision between releasing the probe socket and the child listening.
  for (let attempt = 0; attempt < 4; attempt++) {
    const port = await freePort();
    let rawPort = await freePort();
    while (rawPort === port) rawPort = await freePort();
    const child = spawn(process.execPath, ['--require', path.join(__dirname, 'helpers/mock-memory-guard.js'),
      '--require', path.join(__dirname, 'helpers/loopback.js'), path.join(ROOT, 'server.js')], {
      cwd: ROOT, env: { ...process.env, PORT: String(port), DB_PATH: db, KEEPALIVE_ENABLED: '0', NODE_OPTIONS: '',
        MEMORY_LIMIT_MB: String(limit), MEMORY_GUARD: '1', PANEL_TEST_MEMORY_SNAPSHOT: snapshot,
        REALITY_ENABLED: '1', REALITY_PORT: String(rawPort), REALITY_PUBLIC_HOST: 'example.com',
        REALITY_PUBLIC_PORT: '443', REALITY_SNI: 'example.com', REALITY_PRIVATE_KEY: privateKey,
        REALITY_SHORT_ID: '0123456789abcdef', REALITY_ACK_DIRECT_SOURCE_IP: '1', ...item.overrides },
      stdio: ['ignore', 'pipe', 'pipe']
    });
    let logs = '';
    child.stdout.on('data', (chunk) => { logs += chunk; });
    child.stderr.on('data', (chunk) => { logs += chunk; });
    try {
      for (let i = 0; i < 100; i++) {
        if (child.exitCode !== null || child.signalCode !== null) {
          await delay(30); // collect the child's final stderr before classifying it
          throw new Error(`panel exited: ${logs}`);
        }
        // Don't accidentally inspect another test's process if it won the port.
        if (logs.includes('[INFO] Boot:')) {
          try {
            const state = JSON.parse((await request(port, '/api/health')).body);
            if (state.transports.reality.status === 'failed') return { child, port, state, logs: () => logs };
          } catch (_) {}
        }
        await delay(50);
      }
      throw new Error(`no failure state: ${logs}`);
    } catch (error) {
      await stop(child);
      if (/EADDRINUSE/.test(logs) && attempt < 3) continue;
      throw error;
    }
  }
  throw new Error('unable to allocate an HTTP test port');
}

test('optional REALITY configuration fails closed while original WS and subscription remain available', async (t) => {
  const target = net.createServer((socket) => { socket.on('error', () => {}); socket.pipe(socket); });
  target.listen(0, '127.0.0.1');
  await once(target, 'listening');
  t.after(() => target.close());
  const cases = [
    { name: 'source IP acknowledgement missing', overrides: { REALITY_ACK_DIRECT_SOURCE_IP: '' }, reason: 'source_ip_not_acknowledged' },
    { name: 'sidecar missing', overrides: { XRAY_BIN: '/definitely-not-present-xray' }, reason: 'xray_binary_unavailable' },
    { name: 'unverified sidecar', overrides: { XRAY_BIN: '/bin/true' }, reason: 'xray_binary_mismatch' },
    { name: 'guard disabled', overrides: { MEMORY_GUARD: '0' }, reason: 'cgroup_memory_guard_required' },
    { name: 'small memory plan', limitMb: 128, overrides: {}, reason: 'memory_limit_below_256mib' }
  ];
  for (const item of cases) await t.test(item.name, async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'panel-reality-fail-'));
    const db = path.join(dir, 'db.json');
    const snapshot = path.join(dir, 'memory.json');
    fs.writeFileSync(db, JSON.stringify({ users: [{ id: 'u', remark: 'u', uuid: UUID, sub_id: 'sub',
      is_active: 1, traffic_limit: 0, connection_limit: 0 }] }));
    const limit = item.limitMb || 1024;
    fs.writeFileSync(snapshot, JSON.stringify({ usedMb: limit === 128 ? 50 : 300, limitMb: limit }));
    let panel;
    try {
      panel = await launchPanel(db, snapshot, limit, item);
      const { port, state } = panel;
      assert.equal(state.ok, true);
      assert.equal(state.transports.reality.reason, item.reason);
      assert.ok(!JSON.stringify(state).includes(privateKey));
      assert.equal((await request(port, '/api/users/u/config/reality')).status, 401);
      const body = Buffer.from(JSON.stringify({ username: 'admin', password: 'admin' }));
      const login = await request(port, '/api/login', { method: 'POST', body,
        headers: { 'content-type': 'application/json', 'content-length': body.length } });
      assert.equal(login.status, 200);
      const cookie = login.headers['set-cookie'][0].split(';')[0];
      const config = await request(port, '/api/users/u/config/reality', { headers: { cookie } });
      assert.equal(config.status, 503);
      assert.match(config.headers['cache-control'], /no-store/);
      assert.ok(!config.body.toString().includes(privateKey));
      const raw = await request(port, '/sub/sub?raw=1');
      const links = Buffer.from(raw.body.toString(), 'base64').toString().split('\n');
      assert.deepEqual(links.map((link) => new URL(link).searchParams.get('type')), ['ws', 'xhttp']);
      await wsEcho(port, target.address().port);
      assert.ok(!panel.logs().includes(privateKey));
    } finally {
      if (panel) await stop(panel.child);
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
