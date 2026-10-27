'use strict';

// Exercise real TCP/WS/XHTTP sockets while advancing only the test server's
// wall clock, so a minute of inactivity need not slow the regression suite.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const { spawn } = require('node:child_process');
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

function request(method, port, pathname, body = null) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, path: pathname, method,
      headers: body ? { 'content-type': 'application/octet-stream', 'content-length': body.length } : {} }, (res) => {
      res.resume();
      res.on('end', () => resolve(res.statusCode));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.end(body || undefined);
  });
}

async function launchPanel(t, connectionLimit = 0, extraEnv = {}) {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'panel-lifecycle-test-'));
  const dbPath = path.join(temp, 'db.json');
  const clockFile = path.join(temp, 'clock');
  fs.writeFileSync(dbPath, JSON.stringify({ admin: { username: 'test', password_hash: 'test' }, users: [
    { id: 'test-user', remark: 'test', uuid: UUID, is_active: 1, traffic_limit: 0, connection_limit: connectionLimit }
  ] }));
  fs.writeFileSync(clockFile, '0');
  const port = await freePort();
  const child = spawn(process.execPath, [
    '--require', path.join(__dirname, 'helpers/controlled-clock.js'),
    '--require', path.join(__dirname, 'helpers/short-timeouts.js'),
    '--require', path.join(__dirname, 'helpers/loopback.js'),
    path.join(ROOT, 'server.js')
  ], { cwd: ROOT, env: {
    ...process.env, PORT: String(port), DB_PATH: dbPath, PANEL_TEST_CLOCK_FILE: clockFile,
    MEMORY_LIMIT_MB: '128', MEMORY_GUARD: '0', NODE_OPTIONS: '--max-old-space-size=48 --max-semi-space-size=1',
    KEEPALIVE_ENABLED: '0', DEBUG_ACTIVE_CLIENTS: '1', ...extraEnv
  }, stdio: ['ignore', 'pipe', 'pipe'] });
  let logs = '';
  child.stdout.on('data', (chunk) => { logs = (logs + chunk.toString()).slice(-50000); });
  child.stderr.on('data', (chunk) => { logs = (logs + chunk.toString()).slice(-50000); });
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = once(child, 'exit').catch(() => {});
      child.kill('SIGTERM');
      await Promise.race([exited, delay(1200)]);
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    }
    fs.rmSync(temp, { recursive: true, force: true });
  });
  for (let i = 0; i < 100; i++) {
    if (child.exitCode !== null) throw new Error(`panel exited: ${logs}`);
    try {
      if (await request('GET', port, '/health') === 200) {
        return {
          port, logs: () => logs,
          advance(ms) {
            const tmp = `${clockFile}.tmp`;
            fs.writeFileSync(tmp, String(ms));
            fs.renameSync(tmp, clockFile);
          }
        };
      }
    } catch (_) {}
    await delay(50);
  }
  throw new Error(`panel startup failed: ${logs}`);
}

async function echoTarget(t) {
  const sockets = new Set();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('data', (chunk) => socket.write(chunk));
    socket.on('close', () => sockets.delete(socket));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => {
    for (const socket of sockets) socket.destroy();
    server.close();
  });
  return server.address().port;
}

function vlessHeader(port, host = 'target.test', command = 1) {
  const name = Buffer.from(host);
  const p = Buffer.alloc(2);
  p.writeUInt16BE(port);
  return Buffer.concat([Buffer.from([0]), Buffer.from(UUID, 'hex'), Buffer.from([0, command]), p,
    Buffer.from([2, name.length]), name]);
}

function udpFrame(payload) {
  const length = Buffer.alloc(2);
  length.writeUInt16BE(payload.length);
  return Buffer.concat([length, payload]);
}

function muxNew(port, host = 'target.test') {
  const name = Buffer.from(host);
  const p = Buffer.alloc(2);
  p.writeUInt16BE(port);
  const meta = Buffer.concat([Buffer.from([0, 1, 1, 0, 1]), p, Buffer.from([2, name.length]), name]);
  const size = Buffer.alloc(2);
  size.writeUInt16BE(meta.length);
  return Buffer.concat([size, meta]);
}

function muxHeaderAndOpen(port, host = 'target.test') {
  return Buffer.concat([Buffer.from([0]), Buffer.from(UUID, 'hex'), Buffer.from([0, 3]), muxNew(port, host)]);
}

function muxKeep(payload) {
  const length = Buffer.alloc(2);
  length.writeUInt16BE(payload.length);
  return Buffer.concat([Buffer.from([0, 4, 0, 1, 2, 1]), length, payload]);
}

function messageQueue(ws) {
  const values = [];
  const waiters = [];
  let ended = null;
  ws.on('message', (data) => {
    const value = Buffer.from(data);
    if (waiters.length) waiters.shift().resolve(value);
    else values.push(value);
  });
  ws.on('close', (code, reason) => {
    ended = new Error(`WebSocket closed: ${code} ${reason}`);
    while (waiters.length) waiters.shift().reject(ended);
  });
  ws.on('error', () => {});
  return () => {
    if (values.length) return Promise.resolve(values.shift());
    if (ended) return Promise.reject(ended);
    return new Promise((resolve, reject) => waiters.push({ resolve, reject }));
  };
}

async function connectedWs(t, panel, targetPort, ip = '') {
  const options = ip ? { headers: { 'x-forwarded-for': ip } } : {};
  const ws = new WebSocket(`ws://127.0.0.1:${panel.port}/ws`, options);
  t.after(() => ws.terminate());
  const next = messageQueue(ws);
  await timeout(once(ws, 'open'), 3000, 'WS upgrade');
  ws.send(Buffer.concat([vlessHeader(targetPort), Buffer.from('start')]));
  assert.deepEqual(await timeout(next(), 3000, 'VLESS response'), Buffer.from([0, 0]));
  assert.deepEqual(await timeout(next(), 3000, 'first echo'), Buffer.from('start'));
  return { ws, next };
}

function openDownlink(port, sid) {
  let req;
  const response = new Promise((resolve, reject) => {
    req = http.get({ host: '127.0.0.1', port, path: `/xhttp/${sid}` }, resolve);
    req.on('error', reject);
  });
  response.catch(() => {});
  return { response, destroy: () => req.destroy() };
}

async function receivingOnlyUdpTarget(t) {
  const socket = dgram.createSocket('udp4');
  let timer = null;
  socket.on('message', (_data, from) => {
    if (timer) return;
    let sent = 0;
    const send = () => {
      if (++sent > 12) { clearInterval(timer); return; }
      socket.send(Buffer.from('data'), from.port, from.address);
    };
    send();
    timer = setInterval(send, 60);
  });
  socket.bind(0, '127.0.0.1');
  await once(socket, 'listening');
  t.after(() => { clearInterval(timer); socket.close(); });
  return socket.address().port;
}

async function receivingOnlyTcpTarget(t) {
  const sockets = new Set();
  const timers = new Set();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('data', (data) => socket.write(data));
    let sent = 0;
    const timer = setInterval(() => {
      if (socket.destroyed || ++sent > 12) {
        clearInterval(timer);
        timers.delete(timer);
      } else socket.write(Buffer.from('data'));
    }, 60);
    timers.add(timer);
    socket.on('close', () => sockets.delete(socket));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => {
    for (const timer of timers) clearInterval(timer);
    for (const socket of sockets) socket.destroy();
    server.close();
  });
  return server.address().port;
}

test('transport lifecycle stability', async (suite) => {
  await suite.test('an open quiet WebSocket is not closed as stale after 60 seconds', async (t) => {
    const panel = await launchPanel(t);
    const port = await echoTarget(t);
    const { ws, next } = await connectedWs(t, panel, port);
    panel.advance(61000);
    await delay(800);
    assert.equal(ws.readyState, WebSocket.OPEN, panel.logs());
    ws.send(Buffer.from('still-open'));
    assert.deepEqual(await timeout(next(), 3000, 'echo after idle'), Buffer.from('still-open'));
  });

  await suite.test('parallel quiet WebSockets from one client stay usable', async (t) => {
    const panel = await launchPanel(t);
    const port = await echoTarget(t);
    const connections = await Promise.all(Array.from({ length: 12 }, () => connectedWs(t, panel, port)));
    panel.advance(61000);
    await delay(800);
    assert.equal(connections.filter(({ ws }) => ws.readyState !== WebSocket.OPEN).length, 0, panel.logs());
    for (const { ws, next } of connections) {
      ws.send(Buffer.from('still-open'));
      assert.deepEqual(await timeout(next(), 3000, 'parallel WS echo after idle'), Buffer.from('still-open'));
    }
  });

  await suite.test('more than eight live XHTTP sessions are not killed after a quiet interval', async (t) => {
    const panel = await launchPanel(t);
    const port = await echoTarget(t);
    const streams = [];
    for (let i = 0; i < 12; i++) {
      const sid = `concurrent-${i}`;
      const get = openDownlink(panel.port, sid);
      t.after(get.destroy);
      const status = await timeout(request('POST', panel.port, `/xhttp/${sid}/0`, vlessHeader(port)), 3000, 'XHTTP init');
      assert.equal(status, 200);
      const res = await timeout(get.response, 3000, 'XHTTP downlink');
      res.on('error', () => {});
      t.after(() => res.destroy());
      const [header] = await timeout(once(res, 'data'), 3000, 'XHTTP VLESS header');
      assert.deepEqual(Buffer.from(header), Buffer.from([0, 0]));
      streams.push({ sid, res });
    }
    panel.advance(16000);
    await delay(800);
    const closed = streams.filter(({ res }) => res.destroyed || res.readableEnded);
    assert.equal(closed.length, 0, `closed ${closed.length} live streams\n${panel.logs()}`);
    for (const { sid, res } of streams) {
      const received = once(res, 'data');
      assert.equal(await timeout(request('POST', panel.port, `/xhttp/${sid}/1`, Buffer.from('echo')), 3000, 'XHTTP packet'), 200);
      const [data] = await timeout(received, 3000, 'XHTTP echo after idle');
      assert.deepEqual(Buffer.from(data), Buffer.from('echo'));
    }
  });

  await suite.test('a live quiet endpoint still counts against an explicit client limit', async (t) => {
    const panel = await launchPanel(t, 1);
    const port = await echoTarget(t);
    const first = await connectedWs(t, panel, port, '8.8.8.8');
    panel.advance(61000);
    const second = new WebSocket(`ws://127.0.0.1:${panel.port}/ws`, { headers: { 'x-forwarded-for': '1.1.1.1' } });
    second.on('error', () => {});
    t.after(() => second.terminate());
    const closed = once(second, 'close');
    await timeout(once(second, 'open'), 3000, 'second WS upgrade');
    second.send(vlessHeader(port));
    const [code, reason] = await timeout(closed, 3000, 'explicit client limit');
    assert.equal(code, 1008);
    assert.equal(String(reason), 'client limit exceeded');
    assert.equal(first.ws.readyState, WebSocket.OPEN, panel.logs());
    first.ws.send(Buffer.from('limit-preserved'));
    assert.deepEqual(await timeout(first.next(), 3000, 'first endpoint remains usable'), Buffer.from('limit-preserved'));

    const firstClosed = once(first.ws, 'close');
    first.ws.close();
    await timeout(firstClosed, 3000, 'first endpoint close');
    await delay(50);
    const replacement = await connectedWs(t, panel, port, '1.1.1.1');
    assert.equal(replacement.ws.readyState, WebSocket.OPEN, 'closed sockets release the client slot');
  });

  for (const [label, env] of [
    ['transport', { PANEL_TEST_WS_IDLE_MS: '250' }],
    ['remote TCP', { PANEL_TEST_TCP_IDLE_MS: '250' }]
  ]) {
    await suite.test(`an established quiet WebSocket survives its ${label} idle interval`, async (t) => {
      const panel = await launchPanel(t, 0, env);
      const port = await echoTarget(t);
      const { ws, next } = await connectedWs(t, panel, port);
      await delay(750);
      assert.equal(ws.readyState, WebSocket.OPEN, panel.logs());
      ws.send(Buffer.from('after-long-idle'));
      assert.deepEqual(await timeout(next(), 3000, 'WS echo after transport idle'), Buffer.from('after-long-idle'));
    });
  }

  for (const [label, env] of [
    ['remote TCP', { PANEL_TEST_TCP_IDLE_MS: '250' }],
    ['session', { PANEL_TEST_XHTTP_IDLE_MS: '250' }]
  ]) {
    await suite.test(`an established quiet XHTTP session survives its ${label} idle interval`, async (t) => {
      const panel = await launchPanel(t, 0, env);
      const port = await echoTarget(t);
      const sid = 'quiet-established-xhttp';
      const get = openDownlink(panel.port, sid);
      t.after(get.destroy);
      assert.equal(await timeout(request('POST', panel.port, `/xhttp/${sid}/0`, vlessHeader(port)), 3000, 'quiet XHTTP init'), 200);
      const res = await timeout(get.response, 3000, 'quiet XHTTP downlink');
      res.on('error', () => {});
      t.after(() => res.destroy());
      const [header] = await timeout(once(res, 'data'), 3000, 'quiet XHTTP header');
      assert.deepEqual(Buffer.from(header), Buffer.from([0, 0]));
      await delay(750);
      assert.equal(res.destroyed || res.readableEnded, false, panel.logs());
      const received = once(res, 'data');
      assert.equal(await timeout(request('POST', panel.port, `/xhttp/${sid}/1`, Buffer.from('still-open')), 3000, 'quiet XHTTP packet'), 200);
      const [data] = await timeout(received, 3000, 'quiet XHTTP echo');
      assert.deepEqual(Buffer.from(data), Buffer.from('still-open'));
    });
  }

  await suite.test('an established quiet MUX TCP substream survives its idle interval', async (t) => {
    const panel = await launchPanel(t, 0, { PANEL_TEST_TCP_IDLE_MS: '250' });
    const port = await echoTarget(t);
    const ws = new WebSocket(`ws://127.0.0.1:${panel.port}/ws`);
    t.after(() => ws.terminate());
    const next = messageQueue(ws);
    await timeout(once(ws, 'open'), 3000, 'quiet MUX upgrade');
    ws.send(Buffer.concat([muxHeaderAndOpen(port), muxKeep(Buffer.from('start'))]));
    assert.deepEqual(await timeout(next(), 3000, 'quiet MUX VLESS header'), Buffer.from([0, 0]));
    let frame = await timeout(next(), 3000, 'quiet MUX first echo');
    assert.equal(frame[4], 2);
    assert.deepEqual(frame.subarray(8), Buffer.from('start'));
    await delay(750);
    assert.equal(ws.readyState, WebSocket.OPEN, panel.logs());
    ws.send(muxKeep(Buffer.from('still-open')));
    frame = await timeout(next(), 3000, 'quiet MUX second echo');
    assert.equal(frame[4], 2);
    assert.deepEqual(frame.subarray(8), Buffer.from('still-open'));
  });

  await suite.test('an incomplete WebSocket handshake still times out', async (t) => {
    const panel = await launchPanel(t, 0, { PANEL_TEST_CONNECT_MS: '250' });
    const ws = new WebSocket(`ws://127.0.0.1:${panel.port}/ws`);
    ws.on('error', () => {});
    t.after(() => ws.terminate());
    const closed = once(ws, 'close');
    await timeout(once(ws, 'open'), 3000, 'incomplete WS upgrade');
    ws.send(Buffer.from([0]));
    const [code, reason] = await timeout(closed, 3000, 'incomplete WS timeout');
    assert.equal(code, 1013);
    assert.equal(String(reason), 'connect timeout');
  });

  await suite.test('an uninitialized XHTTP session still times out and can be recreated', async (t) => {
    const panel = await launchPanel(t, 0, { PANEL_TEST_XHTTP_IDLE_MS: '250' });
    const sid = 'unfinished-xhttp';
    const get = openDownlink(panel.port, sid);
    t.after(get.destroy);
    const res = await timeout(get.response, 3000, 'unfinished XHTTP timeout');
    t.after(() => res.destroy());
    const ended = once(res, 'end');
    res.resume();
    await timeout(ended, 3000, 'unfinished XHTTP end');

    const port = await echoTarget(t);
    const replacement = openDownlink(panel.port, sid);
    t.after(replacement.destroy);
    assert.equal(await timeout(request('POST', panel.port, `/xhttp/${sid}/0`, vlessHeader(port)), 3000, 'recreated XHTTP init'), 200);
    const replacementRes = await timeout(replacement.response, 3000, 'recreated XHTTP downlink');
    t.after(() => replacementRes.destroy());
    const [header] = await timeout(once(replacementRes, 'data'), 3000, 'recreated XHTTP header');
    assert.deepEqual(Buffer.from(header), Buffer.from([0, 0]));
  });

  await suite.test('a stalled MUX TCP connect times out without closing the outer tunnel', async (t) => {
    const panel = await launchPanel(t, 0, { PANEL_TEST_TCP_CONNECT_MS: '250' });
    const port = await echoTarget(t);
    const ws = new WebSocket(`ws://127.0.0.1:${panel.port}/ws`);
    t.after(() => ws.terminate());
    const next = messageQueue(ws);
    await timeout(once(ws, 'open'), 3000, 'stalled MUX upgrade');
    ws.send(muxHeaderAndOpen(443, 'never-dns.test'));
    assert.deepEqual(await timeout(next(), 3000, 'stalled MUX VLESS header'), Buffer.from([0, 0]));
    const ended = await timeout(next(), 3000, 'stalled MUX connect timeout');
    assert.equal(ended[4], 3);
    assert.equal(ended[5] & 2, 2);
    assert.equal(ws.readyState, WebSocket.OPEN);

    ws.send(Buffer.concat([muxNew(port), muxKeep(Buffer.from('retry'))]));
    const echoed = await timeout(next(), 3000, 'MUX retry after timeout').catch((err) => {
      throw new Error(`${err.message}\n${panel.logs()}`);
    });
    assert.equal(echoed[4], 2);
    assert.deepEqual(echoed.subarray(8), Buffer.from('retry'));
  });

  await suite.test('receive-only WebSocket UDP traffic refreshes its idle timer', async (t) => {
    const panel = await launchPanel(t, 0, { PANEL_TEST_UDP_IDLE_MS: '250' });
    const port = await receivingOnlyUdpTarget(t);
    const ws = new WebSocket(`ws://127.0.0.1:${panel.port}/ws`);
    ws.on('error', () => {});
    t.after(() => ws.terminate());
    const closed = once(ws, 'close');
    let messages = 0;
    ws.on('message', (data) => { if (data.length > 2) messages++; });
    await timeout(once(ws, 'open'), 3000, 'UDP WS upgrade');
    ws.send(Buffer.concat([vlessHeader(port, '198.18.0.9', 2), udpFrame(Buffer.from('start'))]));
    await delay(750);
    assert.equal(ws.readyState, WebSocket.OPEN, panel.logs());
    assert.ok(messages >= 8, `received only ${messages} datagrams`);
    const [code, reason] = await timeout(closed, 1500, 'idle UDP WS close');
    assert.equal(code, 1000);
    assert.equal(String(reason), 'udp idle timeout');
  });

  await suite.test('receive-only XHTTP UDP traffic refreshes its idle timer', async (t) => {
    const panel = await launchPanel(t, 0, { PANEL_TEST_UDP_IDLE_MS: '250' });
    const port = await receivingOnlyUdpTarget(t);
    const sid = 'receive-only-udp';
    const get = openDownlink(panel.port, sid);
    t.after(get.destroy);
    assert.equal(await timeout(request('POST', panel.port, `/xhttp/${sid}/0`,
      Buffer.concat([vlessHeader(port, '198.18.0.9', 2), udpFrame(Buffer.from('start'))])), 3000, 'UDP XHTTP init'), 200);
    const res = await timeout(get.response, 3000, 'UDP XHTTP downlink');
    res.on('error', () => {});
    t.after(() => res.destroy());
    const ended = once(res, 'end');
    let bytes = 0;
    res.on('data', (data) => { bytes += data.length; });
    await delay(750);
    assert.equal(res.destroyed || res.readableEnded, false, panel.logs());
    assert.ok(bytes >= 2 + 8 * 6, `received only ${bytes} bytes`);
    await timeout(ended, 1500, 'idle UDP XHTTP close');
  });

  await suite.test('receive-only MUX traffic refreshes the outer WebSocket idle timer', async (t) => {
    const panel = await launchPanel(t, 0, { PANEL_TEST_WS_IDLE_MS: '250' });
    const port = await receivingOnlyTcpTarget(t);
    const ws = new WebSocket(`ws://127.0.0.1:${panel.port}/ws`);
    ws.on('error', () => {});
    t.after(() => ws.terminate());
    let messages = 0;
    ws.on('message', (data) => { if (data.length >= 8 && data[4] === 2) messages++; });
    await timeout(once(ws, 'open'), 3000, 'MUX WS upgrade');
    ws.send(muxHeaderAndOpen(port));
    await delay(750);
    assert.equal(ws.readyState, WebSocket.OPEN, panel.logs());
    assert.ok(messages >= 8, `received only ${messages} MUX frames`);
    await delay(550); // no upstream bytes for longer than the shortened idle interval
    assert.equal(ws.readyState, WebSocket.OPEN, panel.logs());
    const received = once(ws, 'message');
    ws.send(muxKeep(Buffer.from('after-idle')));
    const [frame] = await timeout(received, 3000, 'MUX echo after quiet period');
    assert.equal(frame[4], 2);
    assert.deepEqual(Buffer.from(frame).subarray(8), Buffer.from('after-idle'));
  });
});
