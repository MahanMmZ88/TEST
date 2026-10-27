'use strict';

// Loopback integration tests for the real server. The test-only preload maps
// synthetic target names to a local TCP server; production SSRF checks remain
// unchanged. Each test verifies byte-for-byte delivery across backpressure.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const { spawn } = require('node:child_process');
const crypto = require('node:crypto');
const dgram = require('node:dgram');
const fs = require('node:fs');
const http = require('node:http');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');

const ROOT = path.join(__dirname, '..');
const UUID = '11111111111111111111111111111111';
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
let panel;

function timeout(promise, ms, what) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`timeout: ${what}`)), ms); })
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

function request(method, port, pathname, body = null) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, path: pathname, method,
      headers: body ? { 'content-type': 'application/octet-stream', 'content-length': body.length } : {} }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.end(body || undefined);
  });
}

async function launchPanel() {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'panel-transport-test-'));
  const db = path.join(temp, 'db.json');
  fs.writeFileSync(db, JSON.stringify({ admin: { username: 'test', password_hash: 'test' }, users: [
    { id: 'test-user', remark: 'test', uuid: UUID, is_active: 1, traffic_limit: 0, connection_limit: 0 }
  ] }));
  const port = await freePort();
  const child = spawn(process.execPath, ['--require', path.join(__dirname, 'helpers/loopback.js'), path.join(ROOT, 'server.js')], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(port), DB_PATH: db, MEMORY_LIMIT_MB: '128', MEMORY_GUARD: '0',
      NODE_OPTIONS: '--max-old-space-size=48 --max-semi-space-size=1', KEEPALIVE_ENABLED: '0' },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let logs = '';
  child.stdout.on('data', (chunk) => { logs = (logs + chunk.toString()).slice(-30000); });
  child.stderr.on('data', (chunk) => { logs = (logs + chunk.toString()).slice(-30000); });
  const result = { child, port, temp, logs: () => logs };
  try {
    for (let i = 0; i < 100; i++) {
      if (child.exitCode !== null) throw new Error(`panel exited: ${logs}`);
      try {
        const health = await request('GET', port, '/api/health');
        if (health.status === 200) return result;
      } catch (_) {}
      await delay(50);
    }
    throw new Error(`panel did not start: ${logs}`);
  } catch (err) {
    child.kill('SIGKILL');
    fs.rmSync(temp, { recursive: true, force: true });
    throw err;
  }
}

async function closePanel(instance) {
  if (!instance) return;
  if (instance.child.exitCode === null && instance.child.signalCode === null) {
    instance.child.kill('SIGTERM');
    await Promise.race([once(instance.child, 'exit').catch(() => {}), delay(1200)]);
    if (instance.child.exitCode === null && instance.child.signalCode === null) instance.child.kill('SIGKILL');
  }
  fs.rmSync(instance.temp, { recursive: true, force: true });
}

async function target(t, handler) {
  const sockets = new Set();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('close', () => sockets.delete(socket));
    handler(socket);
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => {
    for (const socket of sockets) socket.destroy();
    server.close();
  });
  return server.address().port;
}

async function udpTarget(t, family = 4) {
  const socket = dgram.createSocket(family === 6 ? 'udp6' : 'udp4');
  socket.on('message', (message, from) => socket.send(message, from.port, from.address));
  socket.bind(0, family === 6 ? '::1' : '127.0.0.1');
  try {
    await once(socket, 'listening');
  } catch (err) {
    if (family === 6 && ['EAFNOSUPPORT', 'EADDRNOTAVAIL', 'EPROTONOSUPPORT'].includes(err.code)) {
      t.skip(`IPv6 loopback unavailable: ${err.code}`);
      return null;
    }
    throw err;
  }
  t.after(() => socket.close());
  return socket.address().port;
}

function udpFrame(payload) {
  const length = Buffer.alloc(2);
  length.writeUInt16BE(payload.length);
  return Buffer.concat([length, payload]);
}

function bytes(length, seed = 0) {
  const buffer = Buffer.allocUnsafe(length);
  for (let i = 0; i < length; i++) buffer[i] = (i * 31 + (i >>> 9) + seed) & 255;
  return buffer;
}

function sha(buffer) { return crypto.createHash('sha256').update(buffer).digest('hex'); }

function pumpSource(socket, payload, finish = false) {
  let offset = 0;
  function pump() {
    while (!socket.destroyed && offset < payload.length) {
      const end = Math.min(payload.length, offset + 16384);
      const writable = socket.write(payload.subarray(offset, end));
      offset = end;
      if (!writable) { socket.once('drain', pump); return; }
    }
    if (finish && !socket.destroyed && !socket.writableEnded) socket.end();
  }
  pump();
}

function vlessHeader(port, host = 'target.test', command = 1) {
  const prefix = [Buffer.from([0]), Buffer.from(UUID, 'hex'), Buffer.from([0, command])];
  if (command === 3) return Buffer.concat(prefix);
  const address = Buffer.from(host);
  const p = Buffer.alloc(2);
  p.writeUInt16BE(port);
  return Buffer.concat([...prefix, p, Buffer.from([2, address.length]), address]);
}

function openWs() {
  const ws = new WebSocket(`ws://127.0.0.1:${panel.port}/ws`, { perMessageDeflate: false });
  ws.on('error', () => {});
  return ws;
}

function collectWs(ws, expectedBytes, pauseMs = 0) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    let total = 0;
    let header = Buffer.alloc(0);
    let ready = false;
    ws.on('message', (data) => {
      let chunk = Buffer.from(data);
      if (!ready) {
        header = Buffer.concat([header, chunk]);
        if (header.length < 2) return;
        if (header[0] !== 0 || header[1] !== 0) return reject(new Error('bad VLESS response header'));
        ready = true;
        chunk = header.subarray(2);
        if (pauseMs) {
          ws.pause();
          setTimeout(() => { if (ws.readyState === WebSocket.OPEN) ws.resume(); }, pauseMs);
        }
      }
      hash.update(chunk);
      total += chunk.length;
      if (process.env.TEST_DEBUG && (total % (1024 * 1024) < chunk.length)) console.error('WS bytes:', total);
      if (total >= expectedBytes) resolve({ total, hash: hash.digest('hex') });
    });
    ws.once('close', () => { if (total < expectedBytes) reject(new Error(`WS closed after ${total}/${expectedBytes} bytes`)); });
    ws.once('error', reject);
  });
}

function collectHttp(res, expectedBytes, pauseMs = 0) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    let total = 0;
    let header = Buffer.alloc(0);
    let ready = false;
    if (pauseMs) { res.pause(); setTimeout(() => res.resume(), pauseMs); }
    res.on('data', (data) => {
      let chunk = data;
      if (!ready) {
        header = Buffer.concat([header, chunk]);
        if (header.length < 2) return;
        if (header[0] !== 0 || header[1] !== 0) return reject(new Error('bad VLESS response header'));
        ready = true;
        chunk = header.subarray(2);
      }
      total += chunk.length;
      hash.update(chunk);
      if (total >= expectedBytes) resolve({ total, hash: hash.digest('hex') });
    });
    res.once('end', () => { if (total < expectedBytes) reject(new Error(`HTTP ended after ${total}/${expectedBytes} bytes`)); });
    res.once('error', reject);
  });
}

function downlink(sessionId) {
  let req;
  const response = new Promise((resolve, reject) => {
    req = http.get({ host: '127.0.0.1', port: panel.port, path: `/xhttp/${sessionId}` }, resolve);
    req.on('error', reject);
  });
  // Some assertions can fail before the response is awaited; avoid a late
  // rejected request when test cleanup destroys the pending downlink.
  response.catch(() => {});
  return { response, destroy: () => req.destroy() };
}

function collectTarget(expectedBytes, pauseMs = 0) {
  let resolve, reject;
  const completed = new Promise((ok, fail) => { resolve = ok; reject = fail; });
  const handler = (socket) => {
    const hash = crypto.createHash('sha256');
    let total = 0;
    if (pauseMs) { socket.pause(); setTimeout(() => { if (!socket.destroyed) socket.resume(); }, pauseMs); }
    socket.on('data', (data) => {
      total += data.length;
      hash.update(data);
      if (total >= expectedBytes) resolve({ total, hash: hash.digest('hex') });
    });
    socket.once('error', reject);
    socket.once('close', () => { if (total < expectedBytes) reject(new Error(`target closed after ${total}/${expectedBytes}`)); });
  };
  return { handler, completed };
}

test('transport integration', async (suite) => {
  panel = await launchPanel();
  suite.after(async () => { await closePanel(panel); });

  await suite.test('low-memory profile and health remain available', async () => {
    const result = await request('GET', panel.port, '/api/health');
    assert.equal(result.status, 200);
    const health = JSON.parse(result.body);
    assert.equal(health.low_memory_mode, true);
    assert.equal(health.memory_limit_mb, 128);
    assert.equal(health.memory_guard.enabled, false);
    assert.equal((await request('GET', panel.port, '/health')).status, 200);
  });

  await suite.test('WebSocket download pauses for a slow client and resumes without corrupting bytes', async (t) => {
    const data = bytes(8 * 1024 * 1024, 1);
    const port = await target(t, (socket) => pumpSource(socket, data));
    const ws = openWs();
    t.after(() => ws.terminate());
    const received = collectWs(ws, data.length, 500);
    await once(ws, 'open');
    ws.send(vlessHeader(port, 'cdn.test'));
    let actual;
    try { actual = await timeout(received, 15000, 'WS download'); }
    catch (err) { console.error('panel logs on WS failure:\n' + panel.logs()); throw err; }
    assert.deepEqual(actual, { total: data.length, hash: sha(data) });
    assert.equal(ws.readyState, WebSocket.OPEN);
  });

  await suite.test('a finished upstream does not truncate a backpressured WebSocket download', async (t) => {
    const data = bytes(8 * 1024 * 1024, 10);
    const port = await target(t, (socket) => pumpSource(socket, data, true));
    const ws = openWs();
    t.after(() => ws.terminate());
    const received = collectWs(ws, data.length, 500);
    await once(ws, 'open');
    ws.send(vlessHeader(port, 'cdn.test'));
    assert.deepEqual(await timeout(received, 15000, 'completed WS download'), { total: data.length, hash: sha(data) });
  });

  await suite.test('WebSocket upload waits for a slow upstream socket and preserves ordering', async (t) => {
    const data = bytes(4 * 1024 * 1024, 2);
    const sink = collectTarget(data.length, 600);
    const port = await target(t, sink.handler);
    const ws = openWs();
    t.after(() => ws.terminate());
    await once(ws, 'open');
    ws.send(vlessHeader(port));
    for (let i = 0; i < data.length; i += 16384) ws.send(data.subarray(i, i + 16384));
    const actual = await timeout(sink.completed, 15000, 'WS upload');
    assert.deepEqual(actual, { total: data.length, hash: sha(data) });
    assert.equal(ws.readyState, WebSocket.OPEN);
  });

  await suite.test('XHTTP download applies backpressure without dropping TCP game-category data', async (t) => {
    const data = bytes(8 * 1024 * 1024, 3);
    const port = await target(t, (socket) => pumpSource(socket, data));
    const sid = 'game-download-test';
    const get = downlink(sid);
    t.after(get.destroy);
    const post = await timeout(request('POST', panel.port, `/xhttp/${sid}/0`, vlessHeader(port, 'game.test')), 10000, 'XHTTP init');
    assert.equal(post.status, 200);
    const res = await timeout(get.response, 10000, 'XHTTP response');
    assert.equal(res.statusCode, 200);
    t.after(() => res.destroy());
    const actual = await timeout(collectHttp(res, data.length, 500), 15000, 'XHTTP download');
    assert.deepEqual(actual, { total: data.length, hash: sha(data) });
  });

  await suite.test('a finished upstream does not truncate a backpressured XHTTP download', async (t) => {
    const data = bytes(8 * 1024 * 1024, 11);
    const port = await target(t, (socket) => pumpSource(socket, data, true));
    const sid = 'finished-download-test';
    const get = downlink(sid);
    t.after(get.destroy);
    assert.equal((await timeout(request('POST', panel.port, `/xhttp/${sid}/0`, vlessHeader(port, 'cdn.test')), 10000, 'XHTTP init')).status, 200);
    const res = await timeout(get.response, 10000, 'XHTTP response');
    t.after(() => res.destroy());
    assert.deepEqual(await timeout(collectHttp(res, data.length, 500), 15000, 'completed XHTTP download'),
      { total: data.length, hash: sha(data) });
  });

  await suite.test('WebSocket UDP datagrams still round-trip', async (t) => {
    const port = await udpTarget(t);
    const payload = Buffer.from('udp websocket echo');
    const ws = openWs();
    t.after(() => ws.terminate());
    const response = new Promise((resolve, reject) => {
      let headerSeen = false;
      ws.on('message', (raw) => {
        const data = Buffer.from(raw);
        if (!headerSeen) {
          headerSeen = true;
          if (data.length !== 2 || data[0] !== 0 || data[1] !== 0) reject(new Error('bad VLESS UDP header'));
          return;
        }
        if (data.length !== payload.length + 2 || data.readUInt16BE(0) !== payload.length) return reject(new Error('bad UDP framing'));
        resolve(data.subarray(2));
      });
      ws.once('error', reject);
    });
    await once(ws, 'open');
    ws.send(Buffer.concat([vlessHeader(port, '198.18.0.9', 2), udpFrame(payload)]));
    assert.deepEqual(await timeout(response, 10000, 'WS UDP echo'), payload);
  });

  await suite.test('XHTTP UDP datagrams still round-trip', async (t) => {
    const port = await udpTarget(t);
    const payload = Buffer.from('udp xhttp echo');
    const sid = 'udp-echo-test';
    const get = downlink(sid);
    t.after(get.destroy);
    const post = await timeout(request('POST', panel.port, `/xhttp/${sid}/0`,
      Buffer.concat([vlessHeader(port, '198.18.0.9', 2), udpFrame(payload)])), 10000, 'XHTTP UDP init');
    assert.equal(post.status, 200);
    const res = await timeout(get.response, 10000, 'XHTTP UDP response');
    t.after(() => res.destroy());
    const response = new Promise((resolve, reject) => {
      let all = Buffer.alloc(0);
      res.on('data', (data) => {
        all = Buffer.concat([all, data]);
        if (all.length < payload.length + 4) return;
        if (all[0] !== 0 || all[1] !== 0 || all.readUInt16BE(2) !== payload.length) return reject(new Error('bad XHTTP UDP framing'));
        resolve(all.subarray(4));
      });
      res.once('error', reject);
    });
    assert.deepEqual(await timeout(response, 10000, 'XHTTP UDP echo'), payload);
  });

  await suite.test('oversized XHTTP packet-up receives 413 without stopping the server', async () => {
    const tooLarge = Buffer.alloc(1024 * 1024 + 1, 0x61);
    const response = await timeout(request('POST', panel.port, '/xhttp/oversized/0', tooLarge), 10000, 'oversized POST');
    assert.equal(response.status, 413);
    assert.equal((await request('GET', panel.port, '/health')).status, 200);
  });

  await suite.test('XHTTP packet-up preserves out-of-order posts and waits for slow uploads', async (t) => {
    const a = bytes(16000, 4), b = bytes(130000, 5), c = bytes(130000, 6);
    const expected = Buffer.concat([a, b, c]);
    const sink = collectTarget(expected.length, 500);
    const port = await target(t, sink.handler);
    const sid = 'ordered-packets-test';
    const get = downlink(sid);
    t.after(get.destroy);
    assert.equal((await timeout(request('POST', panel.port, `/xhttp/${sid}/1`, b), 10000, 'post seq1')).status, 200);
    const p0 = request('POST', panel.port, `/xhttp/${sid}/0`, Buffer.concat([vlessHeader(port), a]));
    const p2 = request('POST', panel.port, `/xhttp/${sid}/2`, c);
    const [r0, r2, actual] = await timeout(Promise.all([p0, p2, sink.completed]), 15000, 'ordered XHTTP packets');
    assert.equal(r0.status, 200);
    assert.equal(r2.status, 200);
    assert.deepEqual(actual, { total: expected.length, hash: sha(expected) });
    const res = await timeout(get.response, 10000, 'packet downlink');
    res.resume();
    t.after(() => res.destroy());
  });

  await suite.test('XHTTP stream-up backpressures a slow upstream without losing bytes', async (t) => {
    const data = bytes(4 * 1024 * 1024, 8);
    const sink = collectTarget(data.length, 600);
    const port = await target(t, sink.handler);
    const sid = 'stream-upload-test';
    const get = downlink(sid);
    t.after(get.destroy);
    const posted = new Promise((resolve, reject) => {
      const req = http.request({ host: '127.0.0.1', port: panel.port, method: 'POST', path: `/xhttp/${sid}`,
        headers: { 'content-type': 'application/octet-stream' } }, (res) => {
        res.resume();
        res.on('end', () => resolve(res.statusCode));
        res.on('error', reject);
      });
      req.on('error', reject);
      req.write(vlessHeader(port));
      for (let i = 0; i < data.length; i += 16384) req.write(data.subarray(i, i + 16384));
      req.end();
    });
    const [status, actual] = await timeout(Promise.all([posted, sink.completed]), 15000, 'XHTTP stream-up');
    assert.equal(status, 200);
    assert.deepEqual(actual, { total: data.length, hash: sha(data) });
    const res = await timeout(get.response, 10000, 'stream downlink');
    res.resume();
    t.after(() => res.destroy());
  });

  function muxFrame(id, status, targetInfo, payload) {
    const base = Buffer.from([id >> 8, id & 255, status, payload?.length ? 1 : 0]);
    let metadata = base;
    if (status === 1) {
      const hostname = Buffer.from(targetInfo.host);
      const p = Buffer.alloc(2);
      p.writeUInt16BE(targetInfo.port);
      metadata = Buffer.concat([base, Buffer.from([targetInfo.network === 'udp' ? 2 : 1]), p, Buffer.from([2, hostname.length]), hostname]);
    }
    const metaLen = Buffer.alloc(2);
    metaLen.writeUInt16BE(metadata.length);
    if (!payload?.length) return Buffer.concat([metaLen, metadata]);
    const len = Buffer.alloc(2);
    len.writeUInt16BE(payload.length);
    return Buffer.concat([metaLen, metadata, len, payload]);
  }

  await suite.test('MUX download pauses substreams for a slow client without corrupting frames', async (t) => {
    const data = bytes(4 * 1024 * 1024, 9);
    const port = await target(t, (socket) => pumpSource(socket, data));
    const ws = openWs();
    t.after(() => ws.terminate());
    const received = new Promise((resolve, reject) => {
      const hash = crypto.createHash('sha256');
      let total = 0;
      let responseHeader = false;
      ws.on('message', (raw) => {
        const frame = Buffer.from(raw);
        if (!responseHeader) {
          responseHeader = true;
          if (frame.length !== 2 || frame[0] !== 0 || frame[1] !== 0) return reject(new Error('bad mux VLESS header'));
          ws.pause();
          setTimeout(() => { if (ws.readyState === WebSocket.OPEN) ws.resume(); }, 500);
          return;
        }
        const metaLength = frame.readUInt16BE(0);
        const id = frame.readUInt16BE(2);
        const status = frame[4], option = frame[5];
        if (metaLength !== 4 || id !== 1 || status !== 2 || !(option & 1)) return reject(new Error('unexpected mux frame'));
        const dataLength = frame.readUInt16BE(6);
        const payload = frame.subarray(8);
        if (payload.length !== dataLength) return reject(new Error('bad mux payload length'));
        total += payload.length;
        hash.update(payload);
        if (total >= data.length) resolve({ total, hash: hash.digest('hex') });
      });
      ws.once('error', reject);
      ws.once('close', () => { if (total < data.length) reject(new Error('MUX closed before download completed')); });
    });
    await once(ws, 'open');
    ws.send(Buffer.concat([vlessHeader(0, '', 3), muxFrame(1, 1, { host: 'cdn.test', port })]));
    assert.deepEqual(await timeout(received, 15000, 'MUX download'), { total: data.length, hash: sha(data) });
    assert.equal(ws.readyState, WebSocket.OPEN);
  });

  await suite.test('MUX upload pauses input when a substream is slow and resumes in order', async (t) => {
    const data = bytes(2 * 1024 * 1024, 7);
    const sink = collectTarget(data.length, 600);
    const port = await target(t, sink.handler);
    const ws = openWs();
    t.after(() => ws.terminate());
    await once(ws, 'open');
    const first = muxFrame(1, 1, { host: 'target.test', port }, data.subarray(0, 16384));
    ws.send(Buffer.concat([vlessHeader(0, '', 3), first]));
    for (let i = 16384; i < data.length; i += 16384) ws.send(muxFrame(1, 2, null, data.subarray(i, i + 16384)));
    const actual = await timeout(sink.completed, 15000, 'MUX upload');
    assert.deepEqual(actual, { total: data.length, hash: sha(data) });
    assert.equal(ws.readyState, WebSocket.OPEN);
  });

  await suite.test('WebSocket accepts a VLESS header split across messages', async (t) => {
    const payload = Buffer.from('fragmented websocket header');
    const port = await target(t, (socket) => socket.on('data', (chunk) => socket.write(chunk)));
    const ws = openWs();
    t.after(() => ws.terminate());
    const result = collectWs(ws, payload.length);
    await once(ws, 'open');
    const header = vlessHeader(port);
    for (const byte of header) ws.send(Buffer.from([byte]));
    ws.send(payload);
    assert.deepEqual(await timeout(result, 3000, 'fragmented WS header'), { total: payload.length, hash: sha(payload) });
    assert.equal(ws.readyState, WebSocket.OPEN);
  });

  await suite.test('WebSocket early data and the next frame form one VLESS header in order', async (t) => {
    const payload = Buffer.from('early data split header');
    const port = await target(t, (socket) => socket.on('data', (chunk) => socket.write(chunk)));
    const header = vlessHeader(port);
    const early = header.subarray(0, 18).toString('base64url');
    const ws = new WebSocket(`ws://127.0.0.1:${panel.port}/ws`, early, { perMessageDeflate: false });
    ws.on('error', () => {});
    t.after(() => ws.terminate());
    const result = collectWs(ws, payload.length);
    await once(ws, 'open');
    ws.send(Buffer.concat([header.subarray(18), payload]));
    assert.deepEqual(await timeout(result, 3000, 'early WS data'), { total: payload.length, hash: sha(payload) });
  });

  await suite.test('a standalone MUX VLESS header receives a response before any MUX frame', async (t) => {
    const payload = Buffer.from('mux standalone header');
    const port = await target(t, (socket) => socket.on('data', (chunk) => socket.write(chunk)));
    const ws = openWs();
    t.after(() => ws.terminate());
    const headerReceived = once(ws, 'message');
    await once(ws, 'open');
    ws.send(vlessHeader(0, '', 3));
    const [header] = await timeout(headerReceived, 3000, 'standalone WS MUX header');
    assert.deepEqual(Buffer.from(header), Buffer.from([0, 0]));
    const dataReceived = new Promise((resolve, reject) => {
      ws.once('message', (raw) => {
        const frame = Buffer.from(raw);
        if (frame.length < 8 || frame[4] !== 2 || frame.readUInt16BE(6) !== payload.length) return reject(new Error('bad MUX echo frame'));
        resolve(frame.subarray(8));
      });
      ws.once('error', reject);
      ws.once('close', () => reject(new Error('MUX closed before echo')));
    });
    ws.send(muxFrame(1, 1, { host: 'target.test', port }, payload));
    assert.deepEqual(await timeout(dataReceived, 3000, 'standalone WS MUX echo'), payload);
  });

  await suite.test('XHTTP packet-up reassembles a VLESS header split across posts', async (t) => {
    const payload = Buffer.from('fragmented packet-up header');
    const port = await target(t, (socket) => socket.on('data', (chunk) => socket.write(chunk)));
    const sid = 'split-vless-header-test';
    const get = downlink(sid);
    t.after(get.destroy);
    const header = vlessHeader(port);
    const first = await timeout(request('POST', panel.port, `/xhttp/${sid}/0`, header.subarray(0, 10)), 3000, 'split XHTTP seq0');
    assert.equal(first.status, 200);
    const second = await timeout(request('POST', panel.port, `/xhttp/${sid}/1`, Buffer.concat([header.subarray(10), payload])), 3000, 'split XHTTP seq1');
    assert.equal(second.status, 200);
    const res = await timeout(get.response, 3000, 'split XHTTP response');
    t.after(() => res.destroy());
    assert.deepEqual(await timeout(collectHttp(res, payload.length), 3000, 'split XHTTP echo'), { total: payload.length, hash: sha(payload) });
  });

  await suite.test('XHTTP stream-up reassembles a VLESS header split across upload chunks', async (t) => {
    const payload = Buffer.from('fragmented stream-up header');
    const port = await target(t, (socket) => socket.on('data', (chunk) => socket.write(chunk)));
    const sid = 'split-stream-header-test';
    const get = downlink(sid);
    t.after(get.destroy);
    const upload = http.request({ host: '127.0.0.1', port: panel.port, method: 'POST', path: `/xhttp/${sid}`,
      headers: { 'content-type': 'application/octet-stream' } });
    upload.on('error', () => {});
    t.after(() => upload.destroy());
    const header = vlessHeader(port);
    upload.write(header.subarray(0, 18));
    await delay(50);
    upload.write(header.subarray(18, 22));
    await delay(50);
    upload.write(Buffer.concat([header.subarray(22), payload]));
    const res = await timeout(get.response, 3000, 'split stream-up response');
    t.after(() => res.destroy());
    assert.deepEqual(await timeout(collectHttp(res, payload.length), 3000, 'split stream-up echo'),
      { total: payload.length, hash: sha(payload) });
    upload.end();
  });

  await suite.test('XHTTP packet-up accepts a split standalone MUX header before MUX frames', async (t) => {
    const payload = Buffer.from('packet MUX echo');
    const port = await target(t, (socket) => socket.on('data', (chunk) => socket.write(chunk)));
    const sid = 'split-packet-mux-test';
    const get = downlink(sid);
    t.after(get.destroy);
    const header = vlessHeader(0, '', 3);
    assert.equal((await request('POST', panel.port, `/xhttp/${sid}/0`, header.subarray(0, 10))).status, 200);
    assert.equal((await request('POST', panel.port, `/xhttp/${sid}/1`, header.subarray(10))).status, 200);
    const res = await timeout(get.response, 3000, 'packet MUX response');
    t.after(() => res.destroy());
    const [responseHeader] = await timeout(once(res, 'data'), 3000, 'packet MUX VLESS header');
    assert.deepEqual(Buffer.from(responseHeader), Buffer.from([0, 0]));
    const echo = once(res, 'data');
    assert.equal((await request('POST', panel.port, `/xhttp/${sid}/2`,
      muxFrame(1, 1, { host: 'target.test', port }, payload))).status, 200);
    const [raw] = await timeout(echo, 3000, 'packet MUX echo');
    const frame = Buffer.from(raw);
    assert.equal(frame[4], 2);
    assert.equal(frame.readUInt16BE(6), payload.length);
    assert.deepEqual(frame.subarray(8), payload);
  });

  await suite.test('XHTTP stream-up accepts a standalone 19-byte MUX VLESS header', async (t) => {
    const sid = 'standalone-mux-stream-test';
    const get = downlink(sid);
    t.after(get.destroy);
    const upload = http.request({ host: '127.0.0.1', port: panel.port, method: 'POST', path: `/xhttp/${sid}`,
      headers: { 'content-type': 'application/octet-stream' } });
    upload.on('error', () => {});
    t.after(() => upload.destroy());
    const requestHeader = vlessHeader(0, '', 3);
    upload.write(requestHeader.subarray(0, 18));
    await delay(50);
    upload.write(requestHeader.subarray(18));
    const res = await timeout(get.response, 3000, 'standalone XHTTP MUX response');
    t.after(() => res.destroy());
    const [header] = await timeout(once(res, 'data'), 3000, 'standalone XHTTP MUX header');
    assert.deepEqual(Buffer.from(header), Buffer.from([0, 0]));
    upload.end();
  });

  await suite.test('WebSocket UDP repeatedly resolves a dual-stack hostname using the socket family', async (t) => {
    const port = await udpTarget(t);
    const payloads = [Buffer.from('ws-one'), Buffer.from('ws-two'), Buffer.from('ws-three')];
    const ws = openWs();
    t.after(() => ws.terminate());
    const echoes = new Promise((resolve, reject) => {
      let headerSeen = false;
      const received = [];
      ws.on('message', (raw) => {
        const data = Buffer.from(raw);
        if (!headerSeen) { headerSeen = true; return; }
        if (data.length < 2 || data.readUInt16BE(0) !== data.length - 2) return reject(new Error('invalid WS UDP datagram'));
        received.push(data.subarray(2));
        if (received.length === payloads.length) return resolve(received);
        ws.send(udpFrame(payloads[received.length]));
      });
      ws.once('close', () => reject(new Error('dual-stack WS UDP closed')));
      ws.once('error', reject);
    });
    await once(ws, 'open');
    ws.send(Buffer.concat([vlessHeader(port, 'dual-ws.test', 2), udpFrame(payloads[0])]));
    assert.deepEqual(await timeout(echoes, 5000, 'dual-stack WS UDP'), payloads);
  });

  await suite.test('XHTTP UDP repeatedly resolves a dual-stack hostname using the socket family', async (t) => {
    const port = await udpTarget(t);
    const payloads = [Buffer.from('xhttp-one'), Buffer.from('xhttp-two'), Buffer.from('xhttp-three')];
    const sid = 'dual-stack-udp-test';
    const get = downlink(sid);
    t.after(get.destroy);
    const post = await timeout(request('POST', panel.port, `/xhttp/${sid}/0`,
      Buffer.concat([vlessHeader(port, 'dual-xhttp.test', 2), udpFrame(payloads[0])])), 3000, 'dual XHTTP init');
    assert.equal(post.status, 200);
    const res = await timeout(get.response, 3000, 'dual XHTTP response');
    t.after(() => res.destroy());
    const echoes = new Promise((resolve, reject) => {
      let buffer = Buffer.alloc(0), headerSeen = false;
      const received = [];
      res.on('data', (chunk) => {
        buffer = Buffer.concat([buffer, chunk]);
        if (!headerSeen) {
          if (buffer.length < 2) return;
          if (buffer[0] !== 0 || buffer[1] !== 0) return reject(new Error('bad XHTTP VLESS header'));
          buffer = buffer.subarray(2);
          headerSeen = true;
        }
        while (buffer.length >= 2 && buffer.length >= 2 + buffer.readUInt16BE(0)) {
          const length = buffer.readUInt16BE(0);
          received.push(buffer.subarray(2, 2 + length));
          buffer = buffer.subarray(2 + length);
          if (received.length === payloads.length) return resolve(received);
          const seq = received.length;
          request('POST', panel.port, `/xhttp/${sid}/${seq}`, udpFrame(payloads[seq])).then((r) => {
            if (r.status !== 200) reject(new Error(`XHTTP UDP POST returned ${r.status}`));
          }, reject);
        }
      });
      res.once('end', () => reject(new Error('dual-stack XHTTP UDP closed')));
      res.once('error', reject);
    });
    assert.deepEqual(await timeout(echoes, 5000, 'dual-stack XHTTP UDP'), payloads);
  });

  await suite.test('MUX UDP repeatedly resolves a dual-stack hostname using the socket family', async (t) => {
    const port = await udpTarget(t);
    const payloads = [Buffer.from('mux-one'), Buffer.from('mux-two'), Buffer.from('mux-three')];
    const ws = openWs();
    t.after(() => ws.terminate());
    const echoes = new Promise((resolve, reject) => {
      let headerSeen = false;
      const received = [];
      ws.on('message', (raw) => {
        const frame = Buffer.from(raw);
        if (!headerSeen) { headerSeen = true; return; }
        if (frame.length < 8 || frame[4] !== 2 || frame.readUInt16BE(6) !== frame.length - 8) return reject(new Error('invalid MUX UDP frame'));
        received.push(frame.subarray(8));
        if (received.length === payloads.length) return resolve(received);
        ws.send(muxFrame(1, 2, null, payloads[received.length]));
      });
      ws.once('close', () => reject(new Error('dual-stack MUX closed')));
      ws.once('error', reject);
    });
    await once(ws, 'open');
    ws.send(Buffer.concat([vlessHeader(0, '', 3), muxFrame(1, 1, { host: 'dual-mux.test', port, network: 'udp' }, payloads[0])]));
    assert.deepEqual(await timeout(echoes, 5000, 'dual-stack MUX UDP'), payloads);
  });

  await suite.test('WebSocket reassembles the maximum VLESS options and hostname header', async (t) => {
    const payload = Buffer.from('maximum options');
    const port = await target(t, (socket) => socket.on('data', (chunk) => socket.write(chunk)));
    const base = vlessHeader(port, 'a'.repeat(255));
    const header = Buffer.concat([base.subarray(0, 17), Buffer.from([255]), Buffer.alloc(255, 7), base.subarray(18)]);
    const ws = openWs();
    t.after(() => ws.terminate());
    const result = collectWs(ws, payload.length);
    await once(ws, 'open');
    for (let i = 0; i < header.length; i += 11) ws.send(header.subarray(i, i + 11));
    ws.send(payload);
    assert.deepEqual(await timeout(result, 3000, 'header with options'), { total: payload.length, hash: sha(payload) });
  });

  await suite.test('valid first WebSocket and XHTTP messages may include payload larger than the header limit', async (t) => {
    const payload = bytes(256 * 1024, 13);
    const port = await target(t, (socket) => socket.on('data', (chunk) => socket.write(chunk)));
    const ws = openWs();
    t.after(() => ws.terminate());
    const wsResult = collectWs(ws, payload.length);
    await once(ws, 'open');
    ws.send(Buffer.concat([vlessHeader(port), payload]));
    assert.deepEqual(await timeout(wsResult, 5000, 'large first WS payload'), { total: payload.length, hash: sha(payload) });

    const sid = 'large-initial-payload-test';
    const get = downlink(sid);
    t.after(get.destroy);
    const post = await timeout(request('POST', panel.port, `/xhttp/${sid}/0`, Buffer.concat([vlessHeader(port), payload])), 5000, 'large first XHTTP payload');
    assert.equal(post.status, 200);
    const res = await timeout(get.response, 3000, 'large first XHTTP downlink');
    t.after(() => res.destroy());
    assert.deepEqual(await timeout(collectHttp(res, payload.length), 5000, 'large first XHTTP echo'), { total: payload.length, hash: sha(payload) });
  });

  await suite.test('the low-memory WebSocket message-size cap remains enforced', async (t) => {
    const ws = openWs();
    t.after(() => ws.terminate());
    const closed = once(ws, 'close');
    await once(ws, 'open');
    ws.send(Buffer.alloc(1024 * 1024 + 1));
    const [code] = await timeout(closed, 3000, 'oversized WS frame');
    assert.equal(code, 1009);
    assert.equal((await request('GET', panel.port, '/api/health')).status, 200);
  });

  await suite.test('fragmented malformed requests and private targets are rejected', async (t) => {
    for (const header of [
      Buffer.concat([vlessHeader(443).subarray(0, 22), Buffer.from([0])]), // zero-length domain
      vlessHeader(443, '127.0.0.1') // forbidden target
    ]) {
      const ws = openWs();
      t.after(() => ws.terminate());
      const closed = once(ws, 'close');
      await once(ws, 'open');
      ws.send(header.subarray(0, 18));
      ws.send(header.subarray(18));
      const [code] = await timeout(closed, 3000, 'invalid fragmented VLESS request');
      assert.equal(code, 1008);
    }
    assert.equal((await request('GET', panel.port, '/api/health')).status, 200);
  });

  await suite.test('UDP rejects hostnames resolving only to private addresses', async (t) => {
    const port = await udpTarget(t);
    const ws = openWs();
    t.after(() => ws.terminate());
    const closed = once(ws, 'close');
    await once(ws, 'open');
    ws.send(Buffer.concat([vlessHeader(port, 'private-dns.test', 2), udpFrame(Buffer.from('private'))]));
    const [code] = await timeout(closed, 3000, 'private DNS rejection');
    assert.equal(code, 1011);
    assert.equal((await request('GET', panel.port, '/api/health')).status, 200);
  });

  await suite.test('UDP uses the actual family of an IPv4-mapped DNS answer', async (t) => {
    const port = await udpTarget(t);
    const payload = Buffer.from('mapped IPv4');
    const framed = udpFrame(payload);
    const ws = openWs();
    t.after(() => ws.terminate());
    const result = collectWs(ws, framed.length);
    await once(ws, 'open');
    ws.send(Buffer.concat([vlessHeader(port, 'mapped-ws.test', 2), framed]));
    assert.deepEqual(await timeout(result, 3000, 'mapped IPv4 UDP'), { total: framed.length, hash: sha(framed) });
  });

  await suite.test('closing UDP during a slow lookup leaves the server healthy', async (t) => {
    const port = await udpTarget(t);
    const ws = openWs();
    t.after(() => ws.terminate());
    await once(ws, 'open');
    ws.send(Buffer.concat([vlessHeader(port, 'slow-udp.test', 2), udpFrame(Buffer.from('before-close'))]));
    await delay(40);
    const closed = once(ws, 'close');
    ws.close();
    await timeout(closed, 3000, 'close while resolving UDP');
    await delay(500);
    assert.equal((await request('GET', panel.port, '/api/health')).status, 200);
  });

  await suite.test('WebSocket UDP supports a public IPv6-only hostname', async (t) => {
    const port = await udpTarget(t, 6);
    if (port === null) return;
    const payload = Buffer.from('ws ipv6 only');
    const ws = openWs();
    t.after(() => ws.terminate());
    const result = new Promise((resolve, reject) => {
      let headerSeen = false;
      ws.on('message', (raw) => {
        const data = Buffer.from(raw);
        if (!headerSeen) { headerSeen = true; return; }
        resolve(data);
      });
      ws.once('close', () => reject(new Error('IPv6 WS UDP closed')));
      ws.once('error', reject);
    });
    await once(ws, 'open');
    ws.send(Buffer.concat([vlessHeader(port, 'v6-only-ws.test', 2), udpFrame(payload)]));
    assert.deepEqual(await timeout(result, 5000, 'IPv6 WS UDP'), udpFrame(payload));
  });

  await suite.test('XHTTP UDP supports a public IPv6-only hostname', async (t) => {
    const port = await udpTarget(t, 6);
    if (port === null) return;
    const payload = Buffer.from('xhttp ipv6 only');
    const sid = 'v6-only-udp-test';
    const get = downlink(sid);
    t.after(get.destroy);
    const post = await timeout(request('POST', panel.port, `/xhttp/${sid}/0`,
      Buffer.concat([vlessHeader(port, 'v6-only-xhttp.test', 2), udpFrame(payload)])), 3000, 'IPv6 XHTTP init');
    assert.equal(post.status, 200);
    const res = await timeout(get.response, 3000, 'IPv6 XHTTP response');
    t.after(() => res.destroy());
    const framed = udpFrame(payload);
    assert.deepEqual(await timeout(collectHttp(res, framed.length), 5000, 'IPv6 XHTTP UDP'),
      { total: framed.length, hash: sha(framed) });
  });

  await suite.test('MUX UDP supports a public IPv6-only hostname', async (t) => {
    const port = await udpTarget(t, 6);
    if (port === null) return;
    const payload = Buffer.from('mux ipv6 only');
    const ws = openWs();
    t.after(() => ws.terminate());
    const result = new Promise((resolve, reject) => {
      let headerSeen = false;
      ws.on('message', (raw) => {
        const frame = Buffer.from(raw);
        if (!headerSeen) { headerSeen = true; return; }
        if (frame.length < 8 || frame[4] !== 2 || frame.readUInt16BE(6) !== payload.length) return reject(new Error('invalid IPv6 MUX UDP frame'));
        resolve(frame.subarray(8));
      });
      ws.once('close', () => reject(new Error('IPv6 MUX closed')));
      ws.once('error', reject);
    });
    await once(ws, 'open');
    ws.send(Buffer.concat([vlessHeader(0, '', 3), muxFrame(1, 1, { host: 'v6-only-mux.test', port, network: 'udp' }, payload)]));
    assert.deepEqual(await timeout(result, 5000, 'IPv6 MUX UDP'), payload);
  });

  await suite.test('traffic callbacks produce no uncaught exceptions or unhandled rejections', () => {
    assert.doesNotMatch(panel.logs(), /\[ERROR\] (Uncaught|Unhandled)/);
  });

});
