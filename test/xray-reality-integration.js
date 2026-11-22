'use strict';

// Optional, local end-to-end tests with the pinned real Xray executable and a
// local TLS cover site. The main run uses a test-only cgroup snapshot fixture
// for reproducible memory pressure; it is not a production capacity test.
// Requires openssl and XRAY_BIN=/absolute/path/to/xray.
const assert = require('node:assert/strict');
const { spawn, execFileSync } = require('node:child_process');
const crypto = require('node:crypto');
const dgram = require('node:dgram');
const { once } = require('node:events');
const fs = require('node:fs');
const http = require('node:http');
const http2 = require('node:http2');
const https = require('node:https');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { PassThrough } = require('node:stream');
const WebSocket = require('ws');
const { XRAY_SHA256 } = require('../scripts/reality-bridge');

const ROOT = path.join(__dirname, '..');
const XRAY = process.env.XRAY_BIN || '/usr/local/bin/xray';
// Set only by the optional privileged stress wrapper. The traffic generator,
// local cover site and clients stay outside this group.
const CGROUP = process.env.REALITY_TEST_CGROUP || '';
const PANEL_NODE = process.env.REALITY_TEST_NODE || process.execPath;
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const UUID = {
  normal: '11111111-1111-1111-1111-111111111111',
  limited: '22222222-2222-2222-2222-222222222222',
  quota: '33333333-3333-3333-3333-333333333333',
  inactive: '44444444-4444-4444-4444-444444444444',
  expired: '55555555-5555-5555-5555-555555555555',
  toggle: '66666666-6666-6666-6666-666666666666',
  muxquota: '77777777-7777-7777-7777-777777777777',
  wrong: '99999999-9999-9999-9999-999999999999'
};
let panelLogs = '';
let xrayLogs = '';

function timeout(promise, ms, label) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`timeout: ${label}`)), ms); })
  ]).finally(() => clearTimeout(timer));
}
async function freePort() {
  const server = net.createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}
function request(port, method, route, body = null, headers = {}) {
  return new Promise((resolve, reject) => {
    const data = body === null ? null : Buffer.isBuffer(body) ? body : Buffer.from(JSON.stringify(body));
    const req = http.request({ host: '127.0.0.1', port, method, path: route,
      headers: { ...(data ? { 'content-length': data.length } : {}), ...headers } }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.end(data || undefined);
  });
}
class Reader {
  constructor(stream) {
    this.buffer = Buffer.alloc(0);
    this.waiting = [];
    this.error = null;
    stream.on('data', (data) => { this.buffer = Buffer.concat([this.buffer, data]); this.flush(); });
    stream.on('close', () => { this.error = new Error('stream closed before expected bytes'); this.flush(); });
    stream.on('error', (err) => { this.error = err; this.flush(); });
  }
  flush() {
    while (this.waiting.length && this.buffer.length >= this.waiting[0].length) {
      const waiter = this.waiting.shift();
      const part = this.buffer.subarray(0, waiter.length);
      this.buffer = this.buffer.subarray(waiter.length);
      waiter.resolve(part);
    }
    if (this.error) while (this.waiting.length) this.waiting.shift().reject(this.error);
  }
  read(length) {
    return new Promise((resolve, reject) => { this.waiting.push({ length, resolve, reject }); this.flush(); });
  }
}
async function socksConnect(port, host, targetPort) {
  const socket = net.connect(port, '127.0.0.1');
  const reader = new Reader(socket);
  await timeout(once(socket, 'connect'), 4000, 'SOCKS connect');
  socket.write(Buffer.from([5, 1, 0]));
  assert.deepEqual(await timeout(reader.read(2), 4000, 'SOCKS greeting'), Buffer.from([5, 0]));
  const name = Buffer.from(host);
  const p = Buffer.alloc(2); p.writeUInt16BE(targetPort);
  socket.write(Buffer.concat([Buffer.from([5, 1, 0, 3, name.length]), name, p]));
  const result = await timeout(reader.read(4), 4000, 'SOCKS request');
  assert.equal(result[1], 0, `SOCKS error ${result[1]}`);
  if (result[3] === 1) await reader.read(4);
  else if (result[3] === 4) await reader.read(16);
  else if (result[3] === 3) await reader.read((await reader.read(1))[0]);
  else throw new Error(`unknown SOCKS address type ${result[3]}`);
  await reader.read(2);
  return { socket, reader };
}
async function echo(port, targetPort, length = 1024, host = 'target.test') {
  const { socket, reader } = await socksConnect(port, host, targetPort);
  try {
    const payload = crypto.randomBytes(length);
    socket.write(payload);
    assert.deepEqual(await timeout(reader.read(length), 12000, `TCP echo ${length}`), payload);
  } finally { socket.destroy(); }
}
async function expectRejected(port, targetPort, host = 'target.test') {
  const { socket, reader } = await socksConnect(port, host, targetPort);
  try {
    socket.write(Buffer.from('rejected-probe'));
    let rejected = false;
    try { await timeout(reader.read(1), 6000, 'rejected flow close'); }
    catch (err) {
      if (String(err.message).startsWith('timeout:')) throw err;
      rejected = true;
    }
    assert.ok(rejected, 'unexpected data on a rejected flow');
  } finally { socket.destroy(); }
}
async function udpBurst(socksPort, targetPort, count = 20) {
  const control = net.connect(socksPort, '127.0.0.1');
  const reader = new Reader(control);
  const udp = dgram.createSocket('udp4');
  udp.on('error', () => {});
  try {
    await timeout(once(control, 'connect'), 4000, 'SOCKS UDP control');
    control.write(Buffer.from([5, 1, 0]));
    assert.deepEqual(await timeout(reader.read(2), 4000, 'SOCKS UDP greeting'), Buffer.from([5, 0]));
    control.write(Buffer.from([5, 3, 0, 1, 0, 0, 0, 0, 0, 0]));
    const reply = await timeout(reader.read(4), 4000, 'SOCKS UDP associate');
    assert.equal(reply[1], 0);
    let relay;
    if (reply[3] === 1) relay = [...(await reader.read(4))].join('.');
    else if (reply[3] === 3) relay = (await reader.read((await reader.read(1))[0])).toString();
    else throw new Error(`unexpected SOCKS UDP address type ${reply[3]}`);
    const relayPort = (await reader.read(2)).readUInt16BE();
    if (relay === '0.0.0.0') relay = '127.0.0.1';
    udp.bind(0, '127.0.0.1');
    await once(udp, 'listening');
    const received = new Set();
    udp.on('message', (message) => {
      const text = message.subarray(-7).toString();
      if (/^udp-\d\d\d$/.test(text)) received.add(text);
    });
    const host = Buffer.from('dual-ws.test');
    const p = Buffer.alloc(2); p.writeUInt16BE(targetPort);
    for (let i = 0; i < count; i++) {
      const payload = Buffer.from(`udp-${String(i).padStart(3, '0')}`);
      const packet = Buffer.concat([Buffer.from([0, 0, 0, 3, host.length]), host, p, payload]);
      await new Promise((resolve, reject) => udp.send(packet, relayPort, relay, (err) => err ? reject(err) : resolve()));
      await delay(10);
    }
    const deadline = Date.now() + 5000;
    while (received.size < count && Date.now() < deadline) await delay(20);
    assert.equal(received.size, count, `UDP replies ${received.size}/${count}`);
    return received.size;
  } finally { control.destroy(); try { udp.close(); } catch (_) {} }
}
function vlessHeader(uuid, port, host = 'target.test') {
  const name = Buffer.from(host);
  const p = Buffer.alloc(2); p.writeUInt16BE(port);
  return Buffer.concat([Buffer.from([0]), Buffer.from(uuid.replace(/-/g, ''), 'hex'), Buffer.from([0, 1]),
    p, Buffer.from([2, name.length]), name]);
}
async function wsEcho(panelPort, uuid, targetPort, ip = '127.0.0.1') {
  const ws = new WebSocket(`ws://127.0.0.1:${panelPort}/ws`, { headers: { 'x-forwarded-for': ip } });
  ws.on('error', () => {});
  await timeout(once(ws, 'open'), 4000, 'WS open');
  const payload = Buffer.from('ws-still-works');
  let data = Buffer.alloc(0);
  const result = new Promise((resolve, reject) => {
    ws.on('message', (chunk) => {
      data = Buffer.concat([data, Buffer.from(chunk)]);
      if (data.length >= payload.length + 2) resolve(data);
    });
    ws.once('close', () => reject(new Error('WS closed before echo')));
  });
  try {
    ws.send(Buffer.concat([vlessHeader(uuid, targetPort), payload]));
    assert.deepEqual(await timeout(result, 5000, 'WS echo'), Buffer.concat([Buffer.from([0, 0]), payload]));
  } finally { ws.terminate(); }
}
async function holdWs(panelPort, uuid, targetPort) {
  const ws = new WebSocket(`ws://127.0.0.1:${panelPort}/ws`);
  ws.on('error', () => {});
  const stream = new PassThrough();
  const reader = new Reader(stream);
  ws.on('message', (data) => stream.write(data));
  ws.once('close', () => stream.destroy());
  await timeout(once(ws, 'open'), 4000, 'held WS open');
  ws.send(Buffer.concat([vlessHeader(uuid, targetPort), Buffer.from('before')]));
  assert.deepEqual(await timeout(reader.read(8), 5000, 'held WS initial echo'), Buffer.concat([Buffer.from([0, 0]), Buffer.from('before')]));
  return { ws, reader };
}
async function holdXhttp(panelPort, uuid, targetPort) {
  const session = crypto.randomBytes(6).toString('hex');
  const response = new Promise((resolve, reject) => {
    const req = http.get({ host: '127.0.0.1', port: panelPort, path: `/xhttp/${session}` }, resolve);
    req.on('error', reject);
  });
  const post = request(panelPort, 'POST', `/xhttp/${session}/0`, Buffer.concat([vlessHeader(uuid, targetPort), Buffer.from('before')]));
  const res = await timeout(response, 5000, 'held XHTTP downlink');
  const reader = new Reader(res);
  assert.equal((await post).status, 200);
  assert.deepEqual(await timeout(reader.read(8), 5000, 'held XHTTP initial echo'), Buffer.concat([Buffer.from([0, 0]), Buffer.from('before')]));
  return { session, res, reader };
}
async function xhttpEcho(panelPort, uuid, targetPort) {
  const session = crypto.randomBytes(6).toString('hex');
  const response = new Promise((resolve, reject) => {
    const req = http.get({ host: '127.0.0.1', port: panelPort, path: `/xhttp/${session}` }, resolve);
    req.on('error', reject);
  });
  const payload = Buffer.from('xhttp-still-works');
  // Headers may not flush until the session has data; start the POST before
  // waiting for the GET response rather than deadlocking the test itself.
  const postPromise = request(panelPort, 'POST', `/xhttp/${session}/0`, Buffer.concat([vlessHeader(uuid, targetPort), payload]));
  const res = await timeout(response, 4000, 'XHTTP downlink');
  const reader = new Reader(res);
  try {
    const post = await postPromise;
    assert.equal(post.status, 200);
    assert.deepEqual(await timeout(reader.read(payload.length + 2), 5000, 'XHTTP echo'), Buffer.concat([Buffer.from([0, 0]), payload]));
  } finally { res.destroy(); }
}
function spawnInGroup(group, command, args, env = {}) {
  const shell = 'echo $$ > "$1/cgroup.procs"; shift; user="$1"; shift; exec runuser -u "$user" -- "$@"';
  return spawn('sudo', ['-n', 'sh', '-c', shell, 'reality-cgroup-test', group, os.userInfo().username,
    'env', ...Object.entries(env).map(([name, value]) => `${name}=${value}`), command, ...args],
  { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'] });
}
async function stopChild(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, 'exit').catch(() => {});
  child.kill('SIGTERM');
  await Promise.race([exited, delay(1200)]);
  if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
}
async function waitFor(fn, label, ms = 8000) {
  const deadline = Date.now() + ms;
  let last;
  while (Date.now() < deadline) {
    try { last = await fn(); if (last) return last; } catch (_) {}
    await delay(50);
  }
  throw new Error(`timeout: ${label}; last=${JSON.stringify(last)}`);
}

async function run() {
  const binaryHash = crypto.createHash('sha256').update(fs.readFileSync(XRAY)).digest('hex');
  assert.equal(binaryHash, XRAY_SHA256, 'test requires the pinned official Xray executable');
  const version = execFileSync(XRAY, ['version'], { encoding: 'utf8' }).split('\n')[0];
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'panel-reality-test-'));
  const clients = new Set();
  const auxiliaries = new Set();
  const sockets = new Set();
  let panel, cover, tcpTarget, udpTarget, hog, hogPid;
  try {
    const output = execFileSync(XRAY, ['x25519'], { encoding: 'utf8' });
    const privateKey = output.match(/PrivateKey: (\S+)/)[1];
    const publicKey = output.match(/Password \(PublicKey\): (\S+)/)[1];
    const shortId = crypto.randomBytes(8).toString('hex');
    const keyFile = path.join(temp, 'key.pem');
    const certFile = path.join(temp, 'cert.pem');
    execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', keyFile,
      '-out', certFile, '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'], { stdio: 'ignore' });
    cover = http2.createSecureServer({ key: fs.readFileSync(keyFile), cert: fs.readFileSync(certFile), allowHTTP1: true });
    cover.on('error', () => {});
    cover.on('sessionError', () => {});
    cover.on('request', (req, res) => { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('local cover site'); });
    cover.listen(0, '::');
    await once(cover, 'listening');

    tcpTarget = net.createServer((socket) => {
      sockets.add(socket);
      socket.on('error', () => {});
      socket.on('close', () => sockets.delete(socket));
      socket.pipe(socket);
    });
    tcpTarget.listen(0, '127.0.0.1');
    await once(tcpTarget, 'listening');
    const targetPort = tcpTarget.address().port;
    udpTarget = dgram.createSocket('udp4');
    udpTarget.on('message', (data, address) => udpTarget.send(data, address.port, address.address));
    udpTarget.bind(0, '127.0.0.1');
    await once(udpTarget, 'listening');
    const udpPort = udpTarget.address().port;

    const passwordHash = crypto.createHash('sha256').update('fixtureMAHAN_PANEL_NODE_WS_SALT_v2').digest('hex');
    const users = Object.entries(UUID).filter(([key]) => key !== 'wrong').map(([id, uuid]) => ({
      id, remark: id, sub_id: `${id}-sub`, uuid, is_active: id === 'inactive' ? 0 : 1,
      expiry_time: id === 'expired' ? Date.now() - 10000 : 0,
      traffic_limit: ['quota', 'muxquota'].includes(id) ? 8192 : 0, connection_limit: id === 'limited' ? 1 : 0
    }));
    const db = path.join(temp, 'db.json');
    fs.writeFileSync(db, JSON.stringify({ admin: { username: 'fixture', password_hash: passwordHash }, users }));
    const memoryFile = path.join(temp, 'memory.json');
    const setMemory = (usedMb) => {
      const next = `${memoryFile}.tmp`;
      fs.writeFileSync(next, JSON.stringify({ usedMb, limitMb: 1024 }));
      fs.renameSync(next, memoryFile);
    };
    setMemory(300);
    const panelPort = await freePort();
    const realityPort = await freePort();
    const panelEnv = { PORT: String(panelPort), DB_PATH: db, KEEPALIVE_ENABLED: '0', NODE_OPTIONS: '',
      MEMORY_LIMIT_MB: CGROUP ? '' : '1024', MEMORY_GUARD: '1', PANEL_TEST_MEMORY_SNAPSHOT: memoryFile,
      REALITY_ENABLED: '1', REALITY_PORT: String(realityPort), REALITY_PUBLIC_HOST: 'localhost',
      REALITY_PUBLIC_PORT: String(realityPort), REALITY_SNI: 'localhost', REALITY_TARGET_PORT: String(cover.address().port),
      REALITY_PRIVATE_KEY: privateKey, REALITY_SHORT_ID: shortId, REALITY_ACK_DIRECT_SOURCE_IP: '1',
      REALITY_ALLOW_PRIVATE_SOURCE: '1', REALITY_ALLOW_LOCAL_TARGET: '1', XRAY_BIN: XRAY,
      XRAY_BUF_SPLICE: 'enable', 'xray.buf.splice': 'enable' };
    const panelArgs = [...(CGROUP ? [] : ['--require', path.join(__dirname, 'helpers/mock-memory-guard.js')]),
      '--require', path.join(__dirname, 'helpers/loopback.js'), path.join(ROOT, 'server.js')];
    if (CGROUP) {
      // The pinned binary was read for verification above. Evict its copied
      // pages, and the copied Node executable, before launching the cold group.
      execFileSync('python3', ['-c', 'import os,sys\nfor p in sys.argv[1:]:\n f=os.open(p,os.O_RDONLY); os.fsync(f); os.posix_fadvise(f,0,0,os.POSIX_FADV_DONTNEED); os.close(f)', XRAY, PANEL_NODE]);
      panel = spawnInGroup(CGROUP, PANEL_NODE, panelArgs, panelEnv);
    } else {
      panel = spawn(PANEL_NODE, panelArgs, { cwd: ROOT, env: { ...process.env, ...panelEnv }, stdio: ['ignore', 'pipe', 'pipe'] });
    }
    panel.stdout.on('data', (data) => { panelLogs = (panelLogs + data).slice(-50000); });
    panel.stderr.on('data', (data) => { panelLogs = (panelLogs + data).slice(-50000); });
    const health = async () => JSON.parse((await request(panelPort, 'GET', '/api/health')).body);
    await waitFor(async () => {
      if (panel.exitCode !== null) throw new Error('panel exited');
      const status = (await health()).transports.reality;
      if (status.status === 'failed') throw new Error(`sidecar failed: ${status.reason}`);
      return status.status === 'ready';
    }, 'REALITY startup');
    const initialHealth = await health();
    if (CGROUP) {
      assert.equal(initialHealth.memory_guard.usage_source, 'cgroup-v2');
      assert.ok(initialHealth.memory_guard.active);
    }
    console.log(JSON.stringify({ scenario: 'startup', xray: version,
      memory_guard: CGROUP ? 'real cgroup-v2' : 'synthetic cgroup snapshot', memory_limit_mb: initialHealth.memory_limit_mb }));

    const login = await request(panelPort, 'POST', '/api/login', { username: 'fixture', password: 'fixture' },
      { 'content-type': 'application/json' });
    assert.equal(login.status, 200);
    const cookie = login.headers['set-cookie'][0].split(';')[0];
    const getUsers = async () => JSON.parse((await request(panelPort, 'GET', '/api/users', null, { cookie })).body).users;
    const subscription = await request(panelPort, 'GET', '/sub/normal-sub?raw=1');
    const links = Buffer.from(subscription.body.toString(), 'base64').toString().split('\n');
    assert.deepEqual(links.map((link) => new URL(link).searchParams.get('type')), ['ws', 'xhttp', 'tcp']);
    const rawLink = new URL(links[2]);
    assert.equal(rawLink.searchParams.get('security'), 'reality');
    assert.equal(rawLink.searchParams.get('pbk'), publicKey);
    assert.equal(rawLink.searchParams.get('sid'), shortId);
    assert.equal(rawLink.searchParams.has('flow'), false);
    assert.ok(!links.join('\n').includes(privateKey));
    assert.equal((await request(panelPort, 'GET', '/api/users/normal/config/reality')).status, 401);
    const adminConfig = await request(panelPort, 'GET', '/api/users/normal/config/reality', null, { cookie });
    assert.equal(adminConfig.status, 200);
    assert.match(adminConfig.headers['cache-control'], /no-store/);
    assert.equal(JSON.parse(adminConfig.body).link, links[2]);
    assert.ok(!adminConfig.body.toString().includes(privateKey));
    assert.equal((await request(panelPort, 'GET', '/api/users/missing/config/reality', null, { cookie })).status, 404);
    const html = await request(panelPort, 'GET', '/sub/normal-sub?html=1');
    assert.match(html.body.toString(), /id="tab-reality"/);
    assert.equal((await request(panelPort, 'GET', '/qr/normal-sub/reality')).status, 200);
    assert.ok(!JSON.stringify(await health()).includes(privateKey));
    console.log(JSON.stringify({ scenario: 'subscriptions and QR', transports: ['ws', 'xhttp', 'reality'], private_key_exposed: false }));

    async function startClient({ id = UUID.normal, mux = false, source = '127.0.0.1', flow = '', destinationPort = realityPort } = {}) {
      const socksPort = await freePort();
      const clientConfig = {
        log: { loglevel: 'warning', access: 'none' },
        inbounds: [{ listen: '127.0.0.1', port: socksPort, protocol: 'socks', settings: { auth: 'noauth', udp: true } }],
        outbounds: [{ sendThrough: source, protocol: 'vless', settings: { vnext: [{ address: '127.0.0.1', port: destinationPort,
          users: [{ id, encryption: 'none', ...(flow ? { flow } : {}) }] }] },
          streamSettings: { network: 'raw', security: 'reality', realitySettings: {
            serverName: 'localhost', fingerprint: 'chrome', publicKey, shortId, spiderX: '/'
          } }, mux: { enabled: mux, concurrency: 8 } }]
      };
      const file = path.join(temp, `client-${socksPort}.json`);
      fs.writeFileSync(file, JSON.stringify(clientConfig), { mode: 0o600 });
      const child = spawn(XRAY, ['run', '-config', file], { stdio: ['ignore', 'pipe', 'pipe'] });
      clients.add(child);
      child.stdout.on('data', (data) => { xrayLogs = (xrayLogs + data).slice(-40000); });
      child.stderr.on('data', (data) => { xrayLogs = (xrayLogs + data).slice(-40000); });
      await waitFor(async () => {
        if (child.exitCode !== null) throw new Error('client exited');
        const socket = net.connect(socksPort, '127.0.0.1');
        socket.on('error', () => {});
        try { await once(socket, 'connect'); socket.destroy(); return true; }
        catch (_) { socket.destroy(); return false; }
      }, 'client startup');
      return { port: socksPort, child };
    }
    async function stopClient(client) { await stopChild(client.child); clients.delete(client.child); }

    // REALITY fallback is a real TLS connection to a local cover site; it is
    // not routed to the VLESS handler unless the REALITY handshake succeeds.
    const fallback = await timeout(new Promise((resolve, reject) => {
      const req = https.get({ host: '127.0.0.1', port: realityPort, servername: 'localhost', rejectUnauthorized: false, agent: false }, (res) => {
        const parts = []; res.on('data', (d) => parts.push(d)); res.on('end', () => resolve(Buffer.concat(parts).toString()));
      }); req.on('error', reject);
    }), 5000, 'TLS fallback');
    assert.equal(fallback, 'local cover site');
    console.log(JSON.stringify({ scenario: 'ordinary TLS fallback', body: fallback }));

    const before = (await getUsers()).find((u) => u.id === 'normal');
    const basic = await startClient();
    await echo(basic.port, targetPort, 4096);
    const after = await waitFor(async () => {
      const u = (await getUsers()).find((item) => item.id === 'normal');
      return u.uplink_bytes >= (before.uplink_bytes || 0) + 4096 && u.downlink_bytes >= (before.downlink_bytes || 0) + 4096 ? u : null;
    }, 'traffic accounting');
    assert.equal(after.uplink_bytes - (before.uplink_bytes || 0), 4096);
    assert.equal(after.downlink_bytes - (before.downlink_bytes || 0), 4096);
    assert.ok(!panelLogs.includes('target.test'), 'normal REALITY traffic leaked its target into the new data-plane log');
    assert.ok(!panelLogs.includes(privateKey), 'REALITY private key appeared in panel logs');
    for (const mux of [false, true]) {
      const client = mux ? await startClient({ mux }) : basic;
      await Promise.all(Array.from({ length: 16 }, (_, i) => echo(client.port, targetPort, i === 0 ? 512 * 1024 : 1024)));
      assert.equal(await udpBurst(client.port, udpPort), 20);
      if (mux) await stopClient(client);
      console.log(JSON.stringify({ scenario: 'real Xray TCP/UDP', mux, tcp_flows: 16, udp_datagrams: 20 }));
    }
    // Exercise both error paths, not just successful connections, before any
    // WS/XHTTP traffic can legitimately log this fixture destination.
    const closedTargetPort = await freePort();
    await expectRejected(basic.port, closedTargetPort);
    const quietMux = await startClient({ mux: true });
    await expectRejected(quietMux.port, closedTargetPort);
    await stopClient(quietMux);
    assert.ok(!panelLogs.includes('target.test'), 'REALITY normal/MUX errors leaked a destination into data-plane logs');
    const slow = await socksConnect(basic.port, 'target.test', targetPort);
    try {
      const payload = crypto.randomBytes(4 * 1024 * 1024);
      slow.socket.pause();
      slow.socket.write(payload);
      await delay(900);
      slow.socket.resume();
      assert.deepEqual(await timeout(slow.reader.read(payload.length), 20000, 'slow receiver recovery'), payload);
    } finally { slow.socket.destroy(); }
    console.log(JSON.stringify({ scenario: 'paused receiver recovery', bytes: 4 * 1024 * 1024, paused_ms: 900 }));

    for (const id of [UUID.wrong, UUID.inactive, UUID.expired]) {
      const client = await startClient({ id });
      await expectRejected(client.port, targetPort);
      await stopClient(client);
    }
    await expectRejected(basic.port, targetPort, '127.0.0.1');
    const vision = await startClient({ flow: 'xtls-rprx-vision' });
    await expectRejected(vision.port, targetPort);
    await stopClient(vision);
    console.log(JSON.stringify({ scenario: 'authentication and forbidden destinations', invalid: true, inactive: true,
      expired: true, private_target_blocked: true, unsupported_vision_rejected: true }));

    const limitedA = await startClient({ id: UUID.limited });
    const held = await socksConnect(limitedA.port, 'target.test', targetPort);
    held.socket.write('held');
    assert.equal((await timeout(held.reader.read(4), 5000, 'limited first echo')).toString(), 'held');
    await echo(limitedA.port, targetPort, 32); // parallel streams from the same cohort remain permitted
    await wsEcho(panelPort, UUID.limited, targetPort, '127.0.0.1'); // same cohort across transports
    const limitedB = await startClient({ id: UUID.limited, source: '127.2.0.1' });
    await expectRejected(limitedB.port, targetPort);
    const debug = JSON.parse((await request(panelPort, 'GET', '/api/debug/active', null, { cookie })).body);
    const connections = debug.users.find((u) => u.id === 'limited').connections;
    assert.ok(connections.some((c) => c.kind === 'reality' && c.ip === '127.0.0.1'), JSON.stringify(connections));
    assert.ok(!connections.some((c) => c.ip === '127.2.0.1'), JSON.stringify(connections));
    held.socket.destroy();
    await stopClient(limitedA); await stopClient(limitedB);
    console.log(JSON.stringify({ scenario: 'per-user cohort limit', same_source_allowed: true,
      ws_and_reality_share_limit: true, different_source_rejected: true }));

    const quotaWs = await holdWs(panelPort, UUID.quota, targetPort);
    const quotaWsClosed = once(quotaWs.ws, 'close');
    const quota = await startClient({ id: UUID.quota });
    const quotaFlow = await socksConnect(quota.port, 'target.test', targetPort);
    quotaFlow.socket.write(Buffer.alloc(12000, 0x61));
    await waitFor(async () => {
      const u = (await getUsers()).find((item) => item.id === 'quota');
      return u.is_active === 0 && u.used_bytes >= 8192 ? u : null;
    }, 'quota enforcement');
    await timeout(quotaWsClosed, 5000, 'shared quota closes WS');
    await expectRejected(quota.port, targetPort);
    quotaFlow.socket.destroy(); await stopClient(quota);

    const muxQuota = await startClient({ id: UUID.muxquota, mux: true });
    const muxQuotaFlow = await socksConnect(muxQuota.port, 'target.test', targetPort);
    muxQuotaFlow.socket.write(Buffer.alloc(12000, 0x62));
    await waitFor(async () => {
      const u = (await getUsers()).find((item) => item.id === 'muxquota');
      return u.is_active === 0 && u.used_bytes >= 8192 ? u : null;
    }, 'MUX quota enforcement');
    muxQuotaFlow.socket.destroy();
    await stopClient(muxQuota);
    console.log(JSON.stringify({ scenario: 'traffic quota', normal_and_mux_enforced: true,
      other_transport_closed: true, subsequent_flow_rejected: true }));

    const toggled = await startClient({ id: UUID.toggle });
    const live = await socksConnect(toggled.port, 'target.test', targetPort);
    live.socket.write('live');
    assert.equal((await timeout(live.reader.read(4), 5000, 'pre-disable echo')).toString(), 'live');
    const liveClosed = once(live.socket, 'close');
    assert.equal((await request(panelPort, 'POST', '/api/users/toggle/toggle', null, { cookie })).status, 200);
    await timeout(liveClosed, 5000, 'disabled user connection close');
    await expectRejected(toggled.port, targetPort);
    await stopClient(toggled);
    console.log(JSON.stringify({ scenario: 'administrative disable', live_flow_closed: true, subsequent_flow_rejected: true }));

    // The normal run injects a snapshot. The privileged variant instead adds
    // a bounded test-only allocation to the panel's real cgroup until it is
    // above the hard threshold, without intentionally reaching memory.max.
    const retained = await socksConnect(basic.port, 'target.test', targetPort);
    retained.socket.write('before');
    assert.equal((await timeout(retained.reader.read(6), 5000, 'pre-pressure echo')).toString(), 'before');
    if (CGROUP) {
      const script = [
        'import os,sys,time', 'group=sys.argv[1]',
        'limit=int(open(group+"/memory.max").read())', 'target=int(limit*0.83)',
        'blocks=[]', 'print("PID="+str(os.getpid()), flush=True)',
        'while int(open(group+"/memory.current").read()) < target:',
        ' blocks.append(bytearray(1024*1024)); time.sleep(0.02)',
        'print("READY", flush=True)', 'while True: time.sleep(1)'
      ].join('\n');
      hog = spawnInGroup(CGROUP, '/usr/bin/python3', ['-c', script, CGROUP]);
      let hogOutput = '';
      hog.stdout.on('data', (data) => { hogOutput += data; });
      hog.stderr.on('data', (data) => { hogOutput += data; });
      await waitFor(() => {
        const match = hogOutput.match(/PID=(\d+)/);
        if (match) hogPid = Number(match[1]);
        return hogPid && hogOutput.includes('READY');
      }, 'bounded cgroup pressure generator');
    } else setMemory(850);
    await waitFor(async () => (await health()).memory_guard.state === 'hard', 'hard guard state');
    retained.socket.write('after');
    assert.equal((await timeout(retained.reader.read(5), 5000, 'existing flow under pressure')).toString(), 'after');
    await expectRejected(basic.port, targetPort);
    assert.ok((await health()).transports.reality.rejected_memory > 0);
    if (CGROUP) {
      process.kill(hogPid, 'SIGTERM');
      await stopChild(hog);
      hog = null; hogPid = null;
    } else setMemory(300);
    await waitFor(async () => (await health()).memory_guard.state === 'normal', 'guard recovery');
    await echo(basic.port, targetPort, 64);
    retained.socket.destroy();
    console.log(JSON.stringify({ scenario: CGROUP ? 'real cgroup hard memory pressure' : 'synthetic hard memory pressure',
      existing_flow_survived: true, new_flow_rejected: true, recovered: true }));

    // Kill only the pinned Xray child of this test panel, then verify that the
    // optional port fails closed and the original transports continue working.
    const heldWs = await holdWs(panelPort, UUID.normal, targetPort);
    const heldXhttp = await holdXhttp(panelPort, UUID.normal, targetPort);
    const dying = await socksConnect(basic.port, 'target.test', targetPort);
    dying.socket.write('before');
    assert.equal((await timeout(dying.reader.read(6), 5000, 'pre-sidecar-kill echo')).toString(), 'before');
    const dyingClosed = once(dying.socket, 'close');
    const pidFile = CGROUP ? path.join(CGROUP, 'cgroup.procs') : `/proc/${panel.pid}/task/${panel.pid}/children`;
    const children = fs.readFileSync(pidFile, 'utf8').trim().split(/\s+/).filter(Boolean).map(Number);
    const xrayPid = children.find((pid) => {
      try { return fs.realpathSync(`/proc/${pid}/exe`) === fs.realpathSync(XRAY); } catch (_) { return false; }
    });
    assert.ok(xrayPid, `cannot find panel's Xray child: ${children}`);
    process.kill(xrayPid, 'SIGKILL');
    await timeout(dyingClosed, 6000, 'flow closed after sidecar failure');
    await waitFor(async () => (await health()).transports.reality.status === 'failed', 'sidecar failure state');
    const failedLinks = Buffer.from((await request(panelPort, 'GET', '/sub/normal-sub?raw=1')).body.toString(), 'base64').toString().split('\n');
    assert.equal(failedLinks.length, 2);
    assert.equal((await request(panelPort, 'GET', '/qr/normal-sub/reality')).status, 400);
    const failedConfig = await request(panelPort, 'GET', '/api/users/normal/config/reality', null, { cookie });
    assert.equal(failedConfig.status, 503);
    assert.ok(!failedConfig.body.toString().includes(privateKey));
    heldWs.ws.send('after');
    assert.equal((await timeout(heldWs.reader.read(5), 5000, 'existing WS after crash')).toString(), 'after');
    assert.equal((await request(panelPort, 'POST', `/xhttp/${heldXhttp.session}/1`, Buffer.from('after'))).status, 200);
    assert.equal((await timeout(heldXhttp.reader.read(5), 5000, 'existing XHTTP after crash')).toString(), 'after');
    heldWs.ws.terminate();
    heldXhttp.res.destroy();
    await wsEcho(panelPort, UUID.normal, targetPort);
    await xhttpEcho(panelPort, UUID.normal, targetPort);
    console.log(JSON.stringify({ scenario: 'sidecar crash', reality_removed_from_subscription: true,
      existing_reality_flow_closed: true, existing_and_new_ws_xhttp_survived: true }));
    await stopClient(basic);

    if (!CGROUP) {
      // Extra processes stay outside the cold cgroup run. First occupy the
      // requested raw port, then verify an actual Xray startup cannot disturb
      // the original HTTP/WS service when the new listener fails to bind.
      const auxDb = path.join(temp, 'aux-db.json');
      fs.writeFileSync(auxDb, JSON.stringify({ users: [{ id: 'normal', remark: 'normal', sub_id: 'normal-sub',
        uuid: UUID.normal, is_active: 1, traffic_limit: 0, connection_limit: 0 }] }));
      async function startAux(rawPort, extra = {}, args = panelArgs) {
        const auxPort = await freePort();
        const child = spawn(PANEL_NODE, args, { cwd: ROOT,
          env: { ...process.env, ...panelEnv, PORT: String(auxPort), DB_PATH: auxDb,
            REALITY_PORT: String(rawPort), REALITY_PUBLIC_PORT: String(rawPort), ...extra },
          stdio: ['ignore', 'pipe', 'pipe'] });
        auxiliaries.add(child);
        let logs = '';
        child.stdout.on('data', (data) => { logs = (logs + data).slice(-10000); });
        child.stderr.on('data', (data) => { logs = (logs + data).slice(-10000); });
        const get = async () => JSON.parse((await request(auxPort, 'GET', '/api/health')).body).transports.reality;
        await waitFor(async () => { const status = (await get()).status; return status === 'failed' || status === 'ready'; }, 'auxiliary startup');
        return { child, port: auxPort, get, logs: () => logs };
      }
      const conflict = await startAux(targetPort);
      assert.equal((await conflict.get()).reason, 'listen_port_unavailable', conflict.logs());
      await wsEcho(conflict.port, UUID.normal, targetPort);
      const linksAfterBindFailure = Buffer.from((await request(conflict.port, 'GET', '/sub/normal-sub?raw=1')).body.toString(), 'base64').toString().split('\n');
      assert.equal(linksAfterBindFailure.length, 2);
      await stopChild(conflict.child); auxiliaries.delete(conflict.child);
      console.log(JSON.stringify({ scenario: 'raw port already occupied', optional_listener_failed_closed: true, ws_survived: true }));

      // The raw connection cap must reject new sockets without evicting an
      // established flow, and admit again when capacity is released.
      const capPort = await freePort();
      const capped = await startAux(capPort, { REALITY_MAX_CONNECTIONS: '1' });
      assert.equal((await capped.get()).status, 'ready', capped.logs());
      const capClient = await startClient({ destinationPort: capPort });
      const first = await socksConnect(capClient.port, 'target.test', targetPort);
      first.socket.write('before');
      assert.equal((await timeout(first.reader.read(6), 5000, 'capped flow initial echo')).toString(), 'before');
      await expectRejected(capClient.port, targetPort);
      await waitFor(async () => (await capped.get()).rejected_capacity > 0, 'capacity rejection metric');
      first.socket.write('after');
      assert.equal((await timeout(first.reader.read(5), 5000, 'established flow at cap')).toString(), 'after');
      first.socket.destroy();
      await waitFor(async () => (await capped.get()).ingress_active === 0, 'capacity release');
      await echo(capClient.port, targetPort, 64);
      await stopClient(capClient);
      await stopChild(capped.child); auxiliaries.delete(capped.child);
      console.log(JSON.stringify({ scenario: 'raw connection capacity', rejected_new: true,
        existing_flow_survived: true, capacity_recovered: true }));

      // Advance only the panel's wall clock and shorten its session/remote
      // idle timers. A live quiet REALITY tunnel must neither be pruned nor
      // lose its cohort limit merely because it stopped moving data.
      fs.writeFileSync(auxDb, JSON.stringify({ users: [{ id: 'normal', remark: 'normal', sub_id: 'normal-sub',
        uuid: UUID.normal, is_active: 1, traffic_limit: 0, connection_limit: 1 }] }));
      const clock = path.join(temp, 'aux-clock');
      fs.writeFileSync(clock, '0');
      const idlePort = await freePort();
      const idlePanel = await startAux(idlePort, { PANEL_TEST_CLOCK_FILE: clock,
        PANEL_TEST_XHTTP_IDLE_MS: '250', PANEL_TEST_TCP_IDLE_MS: '250' }, [
        '--require', path.join(__dirname, 'helpers/controlled-clock.js'),
        '--require', path.join(__dirname, 'helpers/short-timeouts.js'), ...panelArgs
      ]);
      assert.equal((await idlePanel.get()).status, 'ready', idlePanel.logs());
      const idleClient = await startClient({ destinationPort: idlePort });
      const quiet = await socksConnect(idleClient.port, 'target.test', targetPort);
      quiet.socket.write('before');
      assert.equal((await timeout(quiet.reader.read(6), 5000, 'quiet flow initial echo')).toString(), 'before');
      fs.writeFileSync(clock, '61000');
      await delay(1000);
      const otherClient = await startClient({ source: '127.0.2.1', destinationPort: idlePort });
      await expectRejected(otherClient.port, targetPort);
      quiet.socket.write('after');
      assert.equal((await timeout(quiet.reader.read(5), 5000, 'quiet flow after simulated idle')).toString(), 'after');
      quiet.socket.destroy();
      await stopClient(otherClient);
      await stopClient(idleClient);
      await stopChild(idlePanel.child); auxiliaries.delete(idlePanel.child);
      console.log(JSON.stringify({ scenario: 'quiet REALITY flow', accelerated_wall_clock_ms: 61000,
        shortened_idle_timers_ms: 250, flow_survived: true, cohort_limit_retained: true }));

      // A private/loopback peer is rejected unless the operator explicitly
      // allows it; this cannot identify a proxy with a public address.
      const protectedPort = await freePort();
      const protectedPanel = await startAux(protectedPort, { REALITY_ALLOW_PRIVATE_SOURCE: '0', REALITY_PUBLIC_HOST: '127.0.0.1' });
      assert.equal((await protectedPanel.get()).status, 'ready', protectedPanel.logs());
      const privateClient = await startClient({ destinationPort: protectedPort });
      await expectRejected(privateClient.port, targetPort);
      assert.ok((await protectedPanel.get()).rejected_source > 0);
      await stopClient(privateClient);
      await stopChild(protectedPanel.child); auxiliaries.delete(protectedPanel.child);
      console.log(JSON.stringify({ scenario: 'non-public source', rejected_by_default: true }));
    }
  } catch (err) {
    console.error('REALITY INTEGRATION FAILED:', err.stack || err);
    console.error('Last panel logs:', panelLogs);
    console.error('Last Xray client logs:', xrayLogs);
    process.exitCode = 1;
  } finally {
    if (hogPid) { try { process.kill(hogPid, 'SIGTERM'); } catch (_) {} }
    if (hog) await stopChild(hog);
    for (const client of clients) await stopChild(client);
    for (const auxiliary of auxiliaries) await stopChild(auxiliary);
    if (panel && CGROUP) {
      try {
        const pids = fs.readFileSync(path.join(CGROUP, 'cgroup.procs'), 'utf8').trim().split(/\s+/).map(Number);
        for (const pid of pids) {
          try { if (fs.realpathSync(`/proc/${pid}/exe`) === fs.realpathSync(PANEL_NODE)) process.kill(pid, 'SIGTERM'); } catch (_) {}
        }
      } catch (_) {}
    }
    if (panel) await stopChild(panel);
    for (const socket of sockets) socket.destroy();
    try { tcpTarget?.close(); } catch (_) {}
    try { udpTarget?.close(); } catch (_) {}
    try { cover?.close(); } catch (_) {}
    fs.rmSync(temp, { recursive: true, force: true });
  }
}

run().catch((err) => { console.error('Cannot start REALITY integration:', err.stack || err); process.exitCode = 1; });
