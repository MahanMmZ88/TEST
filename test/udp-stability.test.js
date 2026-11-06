'use strict';

// Real loopback UDP sockets through the production WS, XHTTP and MUX paths.
// A test-only preload redirects synthetic public DNS answers to loopback; the
// production address validation and bounded queues are not disabled.
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
const BURST_SIZE = 120;
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function timeout(promise, ms, description) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`timeout: ${description}`)), ms); })
  ]).finally(() => clearTimeout(timer));
}

function request(port, method, pathname, body) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, method, path: pathname,
      headers: body ? { 'content-length': body.length, 'content-type': 'application/octet-stream' } : {} }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.once('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
      res.once('error', reject);
    });
    req.once('error', reject);
    req.end(body);
  });
}

async function launchPanel(t, memoryMb) {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'panel-udp-test-'));
  const db = path.join(temp, 'db.json');
  fs.writeFileSync(db, JSON.stringify({ admin: { username: 'test', password_hash: 'test' }, users: [
    { id: 'test-user', remark: 'test', uuid: UUID, is_active: 1, traffic_limit: 0, connection_limit: 0 }
  ] }));
  const reservation = net.createServer();
  reservation.listen(0, '127.0.0.1');
  await once(reservation, 'listening');
  const port = reservation.address().port;
  await new Promise((resolve) => reservation.close(resolve));
  const child = spawn(process.execPath, ['--require', path.join(__dirname, 'helpers/loopback.js'), path.join(ROOT, 'server.js')], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(port), DB_PATH: db, MEMORY_LIMIT_MB: String(memoryMb), MEMORY_GUARD: '0',
      SMART_BOOSTER: '1', NODE_OPTIONS: memoryMb === 128 ? '--max-old-space-size=48 --max-semi-space-size=1' : '',
      KEEPALIVE_ENABLED: '0' },
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
    fs.rmSync(temp, { recursive: true, force: true });
  });
  for (let i = 0; i < 100; i++) {
    if (child.exitCode !== null) throw new Error(`panel exited: ${logs}`);
    try {
      const health = await request(port, 'GET', '/api/health');
      if (health.status === 200) return { port, logs: () => logs };
    } catch (_) {}
    await delay(50);
  }
  throw new Error(`panel did not start: ${logs}`);
}

function vlessHeader(port, host, command) {
  const prefix = [Buffer.from([0]), Buffer.from(UUID, 'hex'), Buffer.from([0, command])];
  if (command === 3) return Buffer.concat(prefix);
  const address = Buffer.from(host);
  const portBytes = Buffer.alloc(2);
  portBytes.writeUInt16BE(port);
  return Buffer.concat([...prefix, portBytes, Buffer.from([2, address.length]), address]);
}

function udpFrame(payload) {
  const length = Buffer.alloc(2);
  length.writeUInt16BE(payload.length);
  return Buffer.concat([length, payload]);
}

function muxFrame(status, target, payload) {
  const base = Buffer.from([0, 1, status, payload?.length ? 1 : 0]);
  let meta = base;
  if (status === 1) {
    const address = Buffer.from(target.host);
    const port = Buffer.alloc(2);
    port.writeUInt16BE(target.port);
    meta = Buffer.concat([base, Buffer.from([2]), port, Buffer.from([2, address.length]), address]);
  }
  const metaLength = Buffer.alloc(2);
  metaLength.writeUInt16BE(meta.length);
  return payload?.length ? Buffer.concat([metaLength, meta, udpFrame(payload)]) : Buffer.concat([metaLength, meta]);
}

function collectDatagrams(mux) {
  let buffer = Buffer.alloc(0);
  let hasHeader = false;
  let failure = null;
  const packets = [];
  const waiters = new Set();
  let resolveHeader, rejectHeader;
  const header = new Promise((resolve, reject) => { resolveHeader = resolve; rejectHeader = reject; });
  header.catch(() => {});

  function fail(err) {
    if (failure) return;
    failure = err;
    if (!hasHeader) rejectHeader(err);
    for (const waiter of waiters) waiter.reject(err);
    waiters.clear();
  }
  function receive(chunk) {
    try {
      buffer = Buffer.concat([buffer, chunk]);
      if (!hasHeader) {
        if (buffer.length < 2) return;
        assert.deepEqual(buffer.subarray(0, 2), Buffer.from([0, 0]));
        buffer = buffer.subarray(2);
        hasHeader = true;
        resolveHeader();
      }
      while (buffer.length >= 2) {
        if (mux) {
          const metaLength = buffer.readUInt16BE(0);
          if (buffer.length < 2 + metaLength) return;
          assert.equal(metaLength, 4);
          assert.equal(buffer.readUInt16BE(2), 1);
          assert.equal(buffer[4], 2, 'unexpected MUX status');
          assert.equal(buffer[5] & 1, 1, 'MUX frame has no datagram');
          if (buffer.length < 2 + metaLength + 2) return;
          const length = buffer.readUInt16BE(2 + metaLength);
          if (buffer.length < 2 + metaLength + 2 + length) return;
          packets.push(Buffer.from(buffer.subarray(2 + metaLength + 2, 2 + metaLength + 2 + length)));
          buffer = buffer.subarray(2 + metaLength + 2 + length);
        } else {
          const length = buffer.readUInt16BE(0);
          if (buffer.length < length + 2) return;
          packets.push(Buffer.from(buffer.subarray(2, length + 2)));
          buffer = buffer.subarray(length + 2);
        }
        for (const waiter of waiters) {
          if (packets.length >= waiter.count) { waiters.delete(waiter); waiter.resolve(packets.slice()); }
        }
      }
    } catch (err) { fail(err); }
  }
  function wait(count, ms = 2000) {
    if (failure) return Promise.reject(failure);
    if (packets.length >= count) return Promise.resolve(packets.slice());
    return new Promise((resolve, reject) => {
      const waiter = {
        count,
        resolve: (value) => { clearTimeout(timer); resolve(value); },
        reject: (err) => { clearTimeout(timer); reject(err); }
      };
      const timer = setTimeout(() => {
        waiters.delete(waiter);
        reject(new Error(`timeout: expected ${count} UDP datagrams, received ${packets.length}`));
      }, ms);
      waiters.add(waiter);
    });
  }
  return { receive, wait, header, fail, packets };
}

async function openFlow(t, panel, kind, targetPort, host) {
  const mux = kind === 'mux' || kind === 'mux-xhttp';
  const collector = collectDatagrams(mux);
  let send, pauseDownlink, resumeDownlink;
  if (kind === 'xhttp' || kind === 'mux-xhttp') {
    const sid = `udp-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    let response;
    const get = http.get({ host: '127.0.0.1', port: panel.port, path: `/xhttp/${sid}` }, (res) => {
      response = res;
      t.after(() => res.destroy());
      res.on('data', collector.receive);
      res.once('end', () => collector.fail(new Error('XHTTP downlink ended')));
      res.once('error', (err) => collector.fail(err));
    });
    get.once('error', (err) => collector.fail(err));
    t.after(() => get.destroy());
    pauseDownlink = () => response.pause();
    resumeDownlink = () => response.resume();
    const initialBody = mux
      ? Buffer.concat([vlessHeader(0, '', 3), muxFrame(1, { host, port: targetPort })])
      : vlessHeader(targetPort, host, 2);
    const initial = await request(panel.port, 'POST', `/xhttp/${sid}/0`, initialBody);
    assert.equal(initial.status, 200);
    let seq = 1;
    send = async (packets) => {
      const frames = packets.map((packet) => mux ? muxFrame(2, null, packet) : udpFrame(packet));
      const result = await request(panel.port, 'POST', `/xhttp/${sid}/${seq++}`, Buffer.concat(frames));
      assert.equal(result.status, 200);
    };
  } else {
    const ws = new WebSocket(`ws://127.0.0.1:${panel.port}/ws`, { perMessageDeflate: false });
    ws.on('message', (chunk) => collector.receive(Buffer.from(chunk)));
    ws.once('error', (err) => collector.fail(err));
    ws.once('close', () => collector.fail(new Error('WebSocket closed')));
    t.after(() => ws.terminate());
    pauseDownlink = () => ws.pause();
    resumeDownlink = () => ws.resume();
    await timeout(once(ws, 'open'), 3000, 'WS upgrade');
    const initial = mux
      ? Buffer.concat([vlessHeader(0, '', 3), muxFrame(1, { host, port: targetPort })])
      : vlessHeader(targetPort, host, 2);
    ws.send(initial);
    send = async (packets) => ws.send(Buffer.concat(packets.map((packet) => mux ? muxFrame(2, null, packet) : udpFrame(packet))));
  }
  await timeout(collector.header, 3000, `${kind} VLESS response`);
  return { send, collector, pauseDownlink, resumeDownlink };
}

async function udpTarget(t, handler) {
  const socket = dgram.createSocket('udp4');
  socket.on('message', (message, from) => handler(socket, message, from));
  socket.bind(0, '127.0.0.1');
  await once(socket, 'listening');
  t.after(() => socket.close());
  return socket.address().port;
}

async function affinityTargets(t) {
  const a = dgram.createSocket('udp4');
  const b = dgram.createSocket('udp4');
  a.bind(0, '127.0.0.1');
  await once(a, 'listening');
  b.bind(a.address().port, '127.0.0.2');
  try { await once(b, 'listening'); }
  catch (err) {
    a.close();
    if (['EADDRNOTAVAIL', 'EAFNOSUPPORT'].includes(err.code)) {
      t.skip(`second loopback address unavailable: ${err.code}`);
      return null;
    }
    throw err;
  }
  for (const [socket, label] of [[a, 'A'], [b, 'B']]) {
    socket.on('message', (message, from) => socket.send(Buffer.concat([Buffer.from(label), message]), from.port, from.address));
    t.after(() => socket.close());
  }
  return a.address().port;
}

function burstPayloads() {
  return Array.from({ length: BURST_SIZE }, (_, i) => {
    const value = Buffer.alloc(4);
    value.writeUInt32BE(i);
    return value;
  });
}

function assertBurst(packets) {
  assert.equal(packets.length, BURST_SIZE);
  assert.deepEqual(packets.map((packet) => packet.readUInt32BE(0)).sort((a, b) => a - b),
    Array.from({ length: BURST_SIZE }, (_, i) => i));
}

test('UDP flow stability with normal network pressure', async (root) => {
  for (const memoryMb of [128, 1024]) {
    await root.test(`${memoryMb} MiB configuration`, async (suite) => {
      const panel = await launchPanel(suite, memoryMb);
      for (const kind of ['ws', 'xhttp', 'mux', 'mux-xhttp']) {
        await suite.test(`${kind}: uplink burst has no artificial packet-count loss`, async (t) => {
          const port = await udpTarget(t, (socket, message, from) => socket.send(message, from.port, from.address));
          const flow = await openFlow(t, panel, kind, port, '198.18.0.9');
          await flow.send(burstPayloads());
          assertBurst(await flow.collector.wait(BURST_SIZE));
        });

        await suite.test(`${kind}: downlink burst has no artificial packet-count loss`, async (t) => {
          const port = await udpTarget(t, (socket, _message, from) => {
            for (const packet of burstPayloads()) socket.send(packet, from.port, from.address);
          });
          const flow = await openFlow(t, panel, kind, port, '198.18.0.9');
          await flow.send([Buffer.from('start')]);
          assertBurst(await flow.collector.wait(BURST_SIZE));
        });

        await suite.test(`${kind}: a UDP flow stays on one resolved endpoint`, async (t) => {
          const port = await affinityTargets(t);
          if (port === null) return;
          const flow = await openFlow(t, panel, kind, port, `affinity-${kind}.test`);
          const endpoints = new Set();
          for (let i = 0; i < 6; i++) {
            const payload = Buffer.from(`datagram-${i}`);
            await flow.send([payload]);
            const packets = await flow.collector.wait(i + 1);
            const reply = packets[i];
            assert.deepEqual(reply.subarray(1), payload);
            endpoints.add(reply.subarray(0, 1).toString());
          }
          assert.equal(endpoints.size, 1, `one UDP flow reached multiple DNS endpoints: ${[...endpoints]}`);
        });

        await suite.test(`${kind}: new UDP flows can use different healthy DNS endpoints`, async (t) => {
          const port = await affinityTargets(t);
          if (port === null) return;
          const endpoints = new Set();
          for (let i = 0; i < 4; i++) {
            const flow = await openFlow(t, panel, kind, port, `affinity-${kind}.test`);
            const payload = Buffer.from(`new-flow-${i}`);
            await flow.send([payload]);
            const [reply] = await flow.collector.wait(1);
            assert.deepEqual(reply.subarray(1), payload);
            endpoints.add(reply.subarray(0, 1).toString());
          }
          assert.equal(endpoints.size, 2, 'round-robin selection happens between flows, not between their packets');
        });
      }
      for (const kind of ['ws', 'xhttp']) {
        await suite.test(`${kind}: slow receiver causes bounded, reported UDP drops and later recovers`, async (t) => {
          let floodTimer;
          const bulk = Buffer.alloc(12 * 1024, 0x5a);
          const trigger = Buffer.from('start-downlink-flood');
          const probe = Buffer.from('after-downlink-flood');
          const port = await udpTarget(t, (socket, message, from) => {
            if (message.equals(trigger) && !floodTimer) {
              floodTimer = setInterval(() => {
                for (let i = 0; i < 32; i++) socket.send(bulk, from.port, from.address);
              }, 5);
            } else if (message.equals(probe)) socket.send(probe, from.port, from.address);
          });
          t.after(() => clearInterval(floodTimer));
          const flow = await openFlow(t, panel, kind, port, '198.18.0.9');
          const before = JSON.parse((await request(panel.port, 'GET', '/api/health')).body).udp_drops.downlink_pressure;
          flow.pauseDownlink();
          await flow.send([trigger]);
          let drops = before;
          for (let i = 0; i < 120 && drops === before; i++) {
            await delay(25);
            drops = JSON.parse((await request(panel.port, 'GET', '/api/health')).body).udp_drops.downlink_pressure;
          }
          clearInterval(floodTimer);
          assert.ok(drops > before, 'slow receiver must eventually trigger the bounded downlink policy');
          flow.resumeDownlink();
          // UDP is lossy: the first probe can itself arrive while buffered
          // output is still draining, so retry until the path has recovered.
          for (let i = 0; i < 80 && !flow.collector.packets.some((packet) => packet.equals(probe)); i++) {
            await flow.send([probe]);
            await delay(50);
          }
          assert.ok(flow.collector.packets.some((packet) => packet.equals(probe)), 'UDP flow recovers when receiver resumes');
        });
      }
      await suite.test('slow UDP DNS keeps a bounded upload queue and reports overload', async (t) => {
        const port = await udpTarget(t, (socket, message, from) => socket.send(message, from.port, from.address));
        const flow = await openFlow(t, panel, 'ws', port, 'slow-udp.test');
        const healthBefore = JSON.parse((await request(panel.port, 'GET', '/api/health')).body);
        const before = healthBefore.udp_drops.uplink_queue_full;
        const packets = Array.from({ length: 600 }, (_, i) => {
          const value = Buffer.alloc(4);
          value.writeUInt32BE(i);
          return value;
        });
        await flow.send(packets);
        // The test resolver waits 400 ms; far more packets than the configured
        // 256/512 queue slots arrive before it can create a socket.
        await delay(100);
        const health = JSON.parse((await request(panel.port, 'GET', '/api/health')).body);
        assert.ok(health.udp_drops.uplink_queue_full - before >= 600 - 512);
        await delay(600);
        const probe = Buffer.from('probe-after-overload');
        await flow.send([probe]);
        for (let i = 0; i < 40 && !flow.collector.packets.some((packet) => packet.equals(probe)); i++) await delay(50);
        assert.ok(flow.collector.packets.some((packet) => packet.equals(probe)), 'UDP flow recovers after DNS resolves');
      });
      await suite.test('UDP upload queue is bounded by bytes as well as packet count', async (t) => {
        const probe = Buffer.from('after-byte-limit');
        const port = await udpTarget(t, (socket, message, from) => {
          if (message.equals(probe)) socket.send(probe, from.port, from.address);
        });
        const flow = await openFlow(t, panel, 'ws', port, 'slow-udp-bytes.test');
        const before = JSON.parse((await request(panel.port, 'GET', '/api/health')).body).udp_drops.uplink_queue_full;
        // 64 packets is well below the packet-count cap; the total of 2 MiB
        // exceeds both the low-memory (128 KiB) and normal (1 MiB) byte caps.
        const packet = Buffer.alloc(32 * 1024, 0x63);
        for (let i = 0; i < 4; i++) await flow.send(Array(16).fill(packet));
        await delay(100);
        const health = JSON.parse((await request(panel.port, 'GET', '/api/health')).body);
        assert.ok(health.udp_drops.uplink_queue_full - before >= 32, 'byte budget limits outstanding sends');
        await delay(600);
        for (let i = 0; i < 40 && !flow.collector.packets.some((value) => value.equals(probe)); i++) {
          await flow.send([probe]);
          await delay(50);
        }
        assert.ok(flow.collector.packets.some((value) => value.equals(probe)), 'flow recovers after byte overload');
      });
      await suite.test('no uncaught exceptions occurred', () => {
        assert.doesNotMatch(panel.logs(), /\[ERROR\] (Uncaught|Unhandled)/);
      });
    });
  }
});
