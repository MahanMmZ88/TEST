'use strict';

// Real HTTP sockets with one upstream whose DNS never resolves. A stalled
// packet-up flow must not consume all process-wide body readers while it waits
// for that destination: an unrelated session should still be able to connect.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const http = require('node:http');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');

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

function request(port, method, pathname, body) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, method, path: pathname,
      headers: body ? { 'content-length': body.length, 'content-type': 'application/octet-stream' } : {} }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.end(body);
  });
}

function openDownlink(port, id) {
  let req;
  const response = new Promise((resolve, reject) => {
    req = http.get({ host: '127.0.0.1', port, path: `/xhttp/${id}` }, resolve);
    req.on('error', reject);
  });
  response.catch(() => {});
  return { request: req, response };
}

function captureBytes(stream, length) {
  return new Promise((resolve, reject) => {
    let data = Buffer.alloc(0);
    stream.on('data', (chunk) => {
      data = Buffer.concat([data, chunk]);
      if (data.length >= length) resolve(data);
    });
    stream.on('error', reject);
  });
}

function vlessHeader(host, port) {
  const address = Buffer.from(host);
  const portBytes = Buffer.alloc(2);
  portBytes.writeUInt16BE(port);
  return Buffer.concat([Buffer.from([0]), Buffer.from(UUID, 'hex'), Buffer.from([0, 1]),
    portBytes, Buffer.from([2, address.length]), address]);
}

async function launchPanel(t, memoryMb) {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'panel-xhttp-fairness-'));
  const db = path.join(temp, 'db.json');
  fs.writeFileSync(db, JSON.stringify({ users: [{
    id: 'test-user', remark: 'test', uuid: UUID, sub_id: 'test-sub', is_active: 1, traffic_limit: 0, connection_limit: 0
  }] }));
  const port = await freePort();
  const child = spawn(process.execPath, ['--require', path.join(__dirname, 'helpers/loopback.js'), path.join(ROOT, 'server.js')], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(port), DB_PATH: db, MEMORY_LIMIT_MB: String(memoryMb),
      MEMORY_GUARD: '0', KEEPALIVE_ENABLED: '0' },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let logs = '';
  child.stdout.on('data', (chunk) => { logs = (logs + chunk.toString()).slice(-30000); });
  child.stderr.on('data', (chunk) => { logs = (logs + chunk.toString()).slice(-30000); });
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGTERM');
      await Promise.race([once(child, 'exit').catch(() => {}), delay(1000)]);
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    }
    fs.rmSync(temp, { recursive: true, force: true });
  });
  for (let i = 0; i < 100; i++) {
    if (child.exitCode !== null) throw new Error(`panel exited: ${logs}`);
    try { if ((await request(port, 'GET', '/api/health')).status === 200) return { port, logs: () => logs }; } catch (_) {}
    await delay(50);
  }
  throw new Error(`panel did not start: ${logs}`);
}

async function waitForQueuedBytes(port, min) {
  for (let i = 0; i < 100; i++) {
    const res = await request(port, 'GET', '/api/health');
    if (JSON.parse(res.body).memory_guard.xhttp_packet_buffer_bytes >= min) return;
    await delay(20);
  }
  throw new Error(`stalled upload never queued ${min} bytes`);
}

test('subscription uses a non-aggressive XHTTP HTTP/2 keepalive', async (t) => {
  const panel = await launchPanel(t, 1024);
  const response = await request(panel.port, 'GET', '/sub/test-sub?raw=1');
  assert.equal(response.status, 200);
  const links = Buffer.from(response.body.toString(), 'base64').toString().split('\n');
  assert.equal(links.length, 2);
  const ws = new URL(links.find((link) => new URL(link).searchParams.get('type') === 'ws'));
  const xhttp = new URL(links.find((link) => new URL(link).searchParams.get('type') === 'xhttp'));
  assert.equal(ws.searchParams.get('path'), '/ws');
  assert.equal(xhttp.searchParams.get('path'), '/xhttp');
  assert.equal(xhttp.searchParams.get('mode'), 'packet-up');
  assert.deepEqual(JSON.parse(xhttp.searchParams.get('extra')), { xmux: { hKeepAlivePeriod: 45 } });
});

test('split routing of one XHTTP session to different replicas cannot share a live socket', async (t) => {
  const first = await launchPanel(t, 1024);
  const second = await launchPanel(t, 1024);
  const sockets = new Set();
  const target = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    socket.on('error', () => {});
    socket.on('data', (data) => socket.write(data));
  });
  target.listen(0, '127.0.0.1');
  await once(target, 'listening');
  t.after(() => { for (const socket of sockets) socket.destroy(); target.close(); });
  const downlinkOnFirst = openDownlink(first.port, 'split-replica');
  t.after(() => downlinkOnFirst.request.destroy());
  const marker = Buffer.from('only-the-owning-replica-can-echo');
  const post = await request(second.port, 'POST', '/xhttp/split-replica/0',
    Buffer.concat([vlessHeader('target.test', target.address().port), marker]));
  assert.equal(post.status, 200);
  await assert.rejects(timeout(downlinkOnFirst.response, 350, 'split replicas'), /timeout/);
  const downlinkOnSecond = openDownlink(second.port, 'split-replica');
  t.after(() => downlinkOnSecond.request.destroy());
  const res = await timeout(downlinkOnSecond.response, 3000, 'owning replica');
  assert.deepEqual(await timeout(captureBytes(res, marker.length + 2), 3000, 'owning replica echo'),
    Buffer.concat([Buffer.from([0, 0]), marker]));
});

test('a short delayed TCP connect does not reject small ordered packet-up bursts', async (t) => {
  const panel = await launchPanel(t, 128);
  const sockets = new Set();
  const target = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    socket.on('error', () => {});
    socket.on('data', (chunk) => socket.write(chunk));
  });
  target.listen(0, '127.0.0.1');
  await once(target, 'listening');
  t.after(() => { for (const socket of sockets) socket.destroy(); target.close(); });
  const response = new Promise((resolve, reject) => {
    const get = http.get({ host: '127.0.0.1', port: panel.port, path: '/xhttp/delayed' }, resolve);
    get.on('error', reject);
    t.after(() => get.destroy());
  });
  response.catch(() => {});
  const first = vlessHeader('slow-connect.test', target.address().port);
  const posts = [request(panel.port, 'POST', '/xhttp/delayed/0', first)];
  posts[0].catch(() => {});
  await waitForQueuedBytes(panel.port, first.length);
  const chunks = Array.from({ length: 12 }, (_, i) => Buffer.from(`packet-${String(i + 1).padStart(2, '0')}|`));
  for (let i = 0; i < chunks.length; i++) {
    const post = request(panel.port, 'POST', `/xhttp/delayed/${i + 1}`, chunks[i]);
    post.catch(() => {});
    posts.push(post);
  }
  const statuses = (await timeout(Promise.all(posts), 4000, 'delayed packet posts')).map((r) => r.status);
  assert.deepEqual(statuses, Array(13).fill(200));
  const res = await timeout(response, 3000, 'delayed downlink');
  t.after(() => res.destroy());
  const expected = Buffer.concat([Buffer.from([0, 0]), ...chunks]);
  const data = await timeout(new Promise((resolve, reject) => {
    let buffered = Buffer.alloc(0);
    res.on('data', (chunk) => {
      buffered = Buffer.concat([buffered, chunk]);
      if (buffered.length >= expected.length) resolve(buffered);
    });
    res.on('error', reject);
  }), 3000, 'delayed ordered echo');
  assert.deepEqual(data, expected);
});

test('out-of-order packet count is bounded while the missing sequence can still drain it', async (t) => {
  const panel = await launchPanel(t, 128);
  const sockets = new Set();
  const target = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    socket.on('error', () => {});
    socket.on('data', (chunk) => socket.write(chunk));
  });
  target.listen(0, '127.0.0.1');
  await once(target, 'listening');
  t.after(() => { for (const socket of sockets) socket.destroy(); target.close(); });
  const response = new Promise((resolve, reject) => {
    const get = http.get({ host: '127.0.0.1', port: panel.port, path: '/xhttp/reorder' }, resolve);
    get.on('error', reject);
    t.after(() => get.destroy());
  });
  response.catch(() => {});
  const header = vlessHeader('target.test', target.address().port);
  assert.equal((await request(panel.port, 'POST', '/xhttp/reorder/0', header.subarray(0, 10))).status, 200);
  const packets = Array.from({ length: 32 }, (_, i) => Buffer.from(`seq-${String(i + 2).padStart(2, '0')}|`));
  for (let i = 0; i < packets.length; i++) {
    assert.equal((await request(panel.port, 'POST', `/xhttp/reorder/${i + 2}`, packets[i])).status, 200);
  }
  assert.equal((await request(panel.port, 'POST', '/xhttp/reorder/34', Buffer.from('overflow'))).status, 503);
  assert.equal((await timeout(request(panel.port, 'POST', '/xhttp/reorder/1', header.subarray(10)), 3000, 'missing sequence')).status, 200);
  const res = await timeout(response, 3000, 'reordered downlink');
  t.after(() => res.destroy());
  const expected = Buffer.concat([Buffer.from([0, 0]), ...packets]);
  const data = await timeout(new Promise((resolve, reject) => {
    let buffered = Buffer.alloc(0);
    res.on('data', (chunk) => {
      buffered = Buffer.concat([buffered, chunk]);
      if (buffered.length >= expected.length) resolve(buffered);
    });
    res.on('error', reject);
  }), 3000, 'reordered echo');
  assert.deepEqual(data, expected);
  const snapshot = JSON.parse((await request(panel.port, 'GET', '/api/health')).body);
  assert.equal(snapshot.memory_guard.xhttp_packet_buffer_bytes, 0);
  assert.equal(snapshot.transports.xhttp.packet_posts_503, 1);
});

test('global packet count bounds tiny requests and reserves the missing packet', async (t) => {
  const panel = await launchPanel(t, 128);
  const sockets = new Set();
  const target = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    socket.on('error', () => {});
    socket.resume();
  });
  target.listen(0, '127.0.0.1');
  await once(target, 'listening');
  t.after(() => { for (const socket of sockets) socket.destroy(); target.close(); });
  const tiny = Buffer.from('x');
  for (let session = 0; session < 8; session++) {
    for (let seq = 1; seq <= 32; seq++) {
      assert.equal((await request(panel.port, 'POST', `/xhttp/count-${session}/${seq}`, tiny)).status, 200);
    }
  }
  let snapshot = JSON.parse((await request(panel.port, 'GET', '/api/health')).body);
  assert.equal(snapshot.memory_guard.xhttp_packet_buffer_count, 256);
  assert.equal(snapshot.memory_guard.xhttp_packet_buffer_bytes, 256);
  assert.equal((await request(panel.port, 'POST', '/xhttp/count-overflow/1', tiny)).status, 503);

  // Even at the global cap, sequence zero must enter and free the 32 queued
  // packets; otherwise an out-of-order burst can deadlock the entire budget.
  assert.equal((await timeout(request(panel.port, 'POST', '/xhttp/count-0/0',
    vlessHeader('target.test', target.address().port)), 3000, 'global reserved packet')).status, 200);
  assert.equal((await request(panel.port, 'POST', '/xhttp/count-overflow/1', tiny)).status, 200);
  snapshot = JSON.parse((await request(panel.port, 'GET', '/api/health')).body);
  assert.equal(snapshot.memory_guard.xhttp_packet_buffer_count, 225);
  assert.equal(snapshot.memory_guard.xhttp_packet_buffer_bytes, 225);
  assert.equal(snapshot.transports.xhttp.packet_posts_503, 1);
});

test('stalled packet bodies remain globally bounded and release their budget on close', async (t) => {
  const panel = await launchPanel(t, 128);
  const first = vlessHeader('never-dns.test', 443);
  const get = http.get({ host: '127.0.0.1', port: panel.port, path: '/xhttp/bounded' });
  get.on('error', () => {});
  t.after(() => get.destroy());
  const pending = [request(panel.port, 'POST', '/xhttp/bounded/0', first)];
  pending[0].catch(() => {});
  await waitForQueuedBytes(panel.port, first.length);
  const packet = Buffer.alloc(1024 * 1024, 0x71);
  for (let seq = 1; seq <= 3; seq++) {
    const post = request(panel.port, 'POST', `/xhttp/bounded/${seq}`, packet);
    post.catch(() => {});
    pending.push(post);
    await waitForQueuedBytes(panel.port, first.length + seq * packet.length);
  }
  const overloaded = await timeout(request(panel.port, 'POST', '/xhttp/bounded/4', packet), 3000, 'bounded queue rejection');
  assert.equal(overloaded.status, 503);
  const full = JSON.parse((await request(panel.port, 'GET', '/api/health')).body);
  assert.equal(full.transports.xhttp.packet_readers_active, 0);
  assert.equal(full.transports.xhttp.packet_readers_waiting, 0);
  assert.equal(full.transports.xhttp.packet_posts_503, 1);
  assert.ok(full.memory_guard.xhttp_packet_buffer_bytes <= 4 * 1024 * 1024 + packet.length);

  get.destroy();
  for (let i = 0; i < 100; i++) {
    const snapshot = JSON.parse((await request(panel.port, 'GET', '/api/health')).body);
    if (snapshot.memory_guard.xhttp_packet_buffer_bytes === 0) return;
    await delay(20);
  }
  assert.fail('closed session retained packet budget');
});

test('the bounded body waiter queue does not block an eligible session', async (t) => {
  const panel = await launchPanel(t, 128);
  const sockets = new Set();
  const target = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('close', () => sockets.delete(socket));
    socket.on('data', (chunk) => socket.write(chunk));
  });
  target.listen(0, '127.0.0.1');
  await once(target, 'listening');
  t.after(() => { for (const socket of sockets) socket.destroy(); target.close(); });
  const slow = [];
  function addSlow(seq) {
    const req = http.request({ host: '127.0.0.1', port: panel.port, method: 'POST',
      path: `/xhttp/slow-waiters/${seq}`, headers: { 'content-length': 1024 } });
    req.on('error', () => {});
    req.write('x');
    slow.push(req);
  }
  t.after(() => { for (const req of slow) req.destroy(); });
  async function waitForReaders(active, waiting) {
    for (let i = 0; i < 100; i++) {
      const xhttp = JSON.parse((await request(panel.port, 'GET', '/api/health')).body).transports.xhttp;
      if (xhttp.packet_readers_active === active && xhttp.packet_readers_waiting === waiting) return xhttp;
      await delay(20);
    }
    throw new Error(`readers did not reach ${active}/${waiting}`);
  }
  addSlow(0); addSlow(1);
  await waitForReaders(2, 0);
  for (let seq = 2; seq < 66; seq++) addSlow(seq);
  const full = await waitForReaders(2, 64);
  assert.equal(full.packet_reader_waiters_limit, 64);
  assert.equal((await timeout(request(panel.port, 'POST', '/xhttp/slow-waiters/66', Buffer.alloc(0)),
    1500, 'global waiter cap')).status, 503);
  const downlink = openDownlink(panel.port, 'healthy-after-waiters');
  t.after(() => downlink.request.destroy());
  const payload = Buffer.from('healthy despite full global waiter queue');
  assert.equal((await timeout(request(panel.port, 'POST', '/xhttp/healthy-after-waiters/0',
    Buffer.concat([vlessHeader('target.test', target.address().port), payload])), 1500, 'other session')).status, 200);
  const response = await timeout(downlink.response, 1500, 'other session downlink');
  assert.deepEqual(await timeout(captureBytes(response, payload.length + 2), 1500, 'other session echo'),
    Buffer.concat([Buffer.from([0, 0]), payload]));
  slow[0].destroy();
  await waitForReaders(2, 63); // the first waiting request receives the freed slot
  for (const req of slow) req.destroy();
  await waitForReaders(0, 0);
});

test('body reader queue skips capped sessions without head-of-line blocking', async (t) => {
  const panel = await launchPanel(t, 128);
  const sockets = new Set();
  const target = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('close', () => sockets.delete(socket));
    socket.on('data', (chunk) => socket.write(chunk));
  });
  target.listen(0, '127.0.0.1');
  await once(target, 'listening');
  t.after(() => { for (const socket of sockets) socket.destroy(); target.close(); });
  const partial = [];
  function add(session, seq) {
    const req = http.request({ host: '127.0.0.1', port: panel.port, method: 'POST',
      path: `/xhttp/${session}/${seq}`, headers: { 'content-length': 1024 } });
    req.on('error', () => {});
    req.write('x');
    partial.push(req);
    return req;
  }
  t.after(() => { for (const req of partial) req.destroy(); });
  async function waitForReaders(active, waiting) {
    for (let i = 0; i < 100; i++) {
      const xhttp = JSON.parse((await request(panel.port, 'GET', '/api/health')).body).transports.xhttp;
      if (xhttp.packet_readers_active === active && xhttp.packet_readers_waiting === waiting) return;
      await delay(20);
    }
    throw new Error(`readers did not reach ${active}/${waiting}`);
  }
  add('capped-a', 0); add('capped-a', 1);
  await waitForReaders(2, 0);
  add('capped-a', 2); add('capped-a', 3);
  await waitForReaders(2, 2);
  const release = add('capped-b', 0);
  add('capped-b', 1);
  await waitForReaders(4, 2);
  const downlink = openDownlink(panel.port, 'healthy-queued');
  t.after(() => downlink.request.destroy());
  const payload = Buffer.from('skip ineligible waiters');
  const healthy = request(panel.port, 'POST', '/xhttp/healthy-queued/0',
    Buffer.concat([vlessHeader('target.test', target.address().port), payload]));
  healthy.catch(() => {});
  await waitForReaders(4, 3);
  release.destroy();
  assert.equal((await timeout(healthy, 1500, 'eligible queued reader')).status, 200);
  const res = await timeout(downlink.response, 1500, 'eligible downlink');
  assert.deepEqual(await timeout(captureBytes(res, payload.length + 2), 1500, 'eligible echo'),
    Buffer.concat([Buffer.from([0, 0]), payload]));
  for (const req of partial) req.destroy();
  await waitForReaders(0, 0);
});

for (const [memoryMb, readers, perSession] of [[128, 4, 2], [1024, 32, 8]]) {
  test(`slow POST bodies from one XHTTP session do not occupy every reader (${memoryMb} MiB)`, async (t) => {
    const panel = await launchPanel(t, memoryMb);
    const sockets = new Set();
    const target = net.createServer((socket) => {
      sockets.add(socket);
      socket.on('error', () => {});
      socket.on('close', () => sockets.delete(socket));
      socket.on('data', (chunk) => socket.write(chunk));
    });
    target.listen(0, '127.0.0.1');
    await once(target, 'listening');
    t.after(() => { for (const socket of sockets) socket.destroy(); target.close(); });
    const slow = [];
    for (let seq = 0; seq < readers; seq++) {
      const req = http.request({ host: '127.0.0.1', port: panel.port, method: 'POST',
        path: `/xhttp/slow-bodies/${seq}`, headers: { 'content-length': 1024 } });
      req.on('error', () => {});
      req.write('x'); // Do not complete the body until cleanup.
      slow.push(req);
    }
    t.after(() => { for (const req of slow) req.destroy(); });
    let snapshot;
    for (let i = 0; i < 100; i++) {
      snapshot = JSON.parse((await request(panel.port, 'GET', '/api/health')).body);
      const xhttp = snapshot.transports.xhttp;
      if (xhttp.packet_readers_active + xhttp.packet_readers_waiting === readers) break;
      await delay(20);
    }
    const response = openDownlink(panel.port, 'healthy-body');
    t.after(() => response.request.destroy());
    const marker = Buffer.from('a different upload is not stuck behind incomplete bodies');
    const post = request(panel.port, 'POST', '/xhttp/healthy-body/0',
      Buffer.concat([vlessHeader('target.test', target.address().port), marker]));
    post.catch(() => {});
    assert.equal((await timeout(post, 1500, 'healthy packet body')).status, 200);
    const res = await timeout(response.response, 1500, 'healthy body downlink');
    assert.deepEqual(await timeout(captureBytes(res, marker.length + 2), 1500, 'healthy body echo'),
      Buffer.concat([Buffer.from([0, 0]), marker]));
    snapshot = JSON.parse((await request(panel.port, 'GET', '/api/health')).body);
    assert.equal(snapshot.transports.xhttp.packet_readers_active, perSession);
    assert.equal(snapshot.transports.xhttp.packet_readers_waiting, readers - perSession);
    assert.equal(snapshot.transports.xhttp.packet_readers_per_session_limit, perSession);
    for (const req of slow) req.destroy();
    for (let i = 0; i < 100; i++) {
      snapshot = JSON.parse((await request(panel.port, 'GET', '/api/health')).body);
      if (!snapshot.transports.xhttp.packet_readers_active && !snapshot.transports.xhttp.packet_readers_waiting) return;
      await delay(20);
    }
    assert.fail('aborted body readers were not released');
  });

  test(`stalled XHTTP packet-up does not block another session (${memoryMb} MiB)`, async (t) => {
    const panel = await launchPanel(t, memoryMb);
    const sockets = new Set();
    const target = net.createServer((socket) => {
      sockets.add(socket);
      socket.on('error', () => {});
      socket.on('close', () => sockets.delete(socket));
      socket.on('data', (chunk) => socket.write(chunk));
    });
    target.listen(0, '127.0.0.1');
    await once(target, 'listening');
    t.after(() => { for (const socket of sockets) socket.destroy(); target.close(); });

    const first = vlessHeader('never-dns.test', 443);
    const small = Buffer.alloc(128, 0x6a);
    const slowPosts = [];
    slowPosts.push(request(panel.port, 'POST', '/xhttp/stalled/0', first));
    slowPosts[0].catch(() => {});
    await waitForQueuedBytes(panel.port, first.length);
    for (let seq = 1; seq < readers; seq++) {
      const promise = request(panel.port, 'POST', `/xhttp/stalled/${seq}`, small);
      promise.catch(() => {});
      slowPosts.push(promise);
    }
    await waitForQueuedBytes(panel.port, first.length + (readers - 1) * small.length);

    const response = new Promise((resolve, reject) => {
      const get = http.get({ host: '127.0.0.1', port: panel.port, path: '/xhttp/healthy' }, resolve);
      get.on('error', reject);
      t.after(() => get.destroy());
    });
    response.catch(() => {});
    const payload = Buffer.from('an unrelated session stays responsive');
    const post = request(panel.port, 'POST', '/xhttp/healthy/0',
      Buffer.concat([vlessHeader('target.test', target.address().port), payload]));
    post.catch(() => {});
    assert.equal((await timeout(post, 1500, `healthy POST, server logs: ${panel.logs()}`)).status, 200);
    const res = await timeout(response, 1500, 'healthy downlink headers');
    t.after(() => res.destroy());
    const data = await timeout(new Promise((resolve, reject) => {
      let chunks = Buffer.alloc(0);
      res.on('data', (chunk) => {
        chunks = Buffer.concat([chunks, chunk]);
        if (chunks.length >= payload.length + 2) resolve(chunks);
      });
      res.on('error', reject);
    }), 1500, 'healthy echo');
    assert.deepEqual(data, Buffer.concat([Buffer.from([0, 0]), payload]));
  });
}
