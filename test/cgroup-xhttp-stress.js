'use strict';

// Optional privileged Linux-only stress test. Only the panel is placed in a
// cold 128 MiB / zero-swap / 0.5 CPU cgroup; the traffic generator and target
// stay outside it. Uses test-only DNS/loopback mocks. Never run in production.
// Run: node test/cgroup-xhttp-stress.js (requires passwordless sudo).
const assert = require('node:assert/strict');
const { spawn, execFileSync } = require('node:child_process');
const { once } = require('node:events');
const fs = require('node:fs');
const http = require('node:http');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');

const root = path.join(__dirname, '..');
const group = `/sys/fs/cgroup/arena-xhttp-fair-${process.pid}`;
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'panel-xhttp-cgroup-'));
const uuid = '11111111111111111111111111111111';
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const sockets = new Set();
let panel, target, created = false, logs = '';

function sudo(...args) { return execFileSync('sudo', ['-n', ...args], { encoding: 'utf8' }); }
function groupNumber(file) { return Number(fs.readFileSync(path.join(group, file), 'utf8').trim()); }
function groupStats(file) {
  return Object.fromEntries(fs.readFileSync(path.join(group, file), 'utf8').trim().split('\n')
    .map((line) => line.split(/\s+/)).map(([key, value]) => [key, Number(value)]));
}
function timeout(promise, ms, label) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`timeout: ${label}`)), ms); })
  ]).finally(() => clearTimeout(timer));
}
function request(port, method, url, body = null) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, method, path: url,
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
async function health(port) { return JSON.parse((await request(port, 'GET', '/api/health')).body); }
async function freePort() {
  const server = net.createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}
function header(host, port) {
  const name = Buffer.from(host);
  const portBytes = Buffer.alloc(2); portBytes.writeUInt16BE(port);
  return Buffer.concat([Buffer.from([0]), Buffer.from(uuid, 'hex'), Buffer.from([0, 1]), portBytes,
    Buffer.from([2, name.length]), name]);
}
function openDownlink(port, id) {
  let req;
  const response = new Promise((resolve, reject) => {
    req = http.get({ host: '127.0.0.1', port, path: `/xhttp/${id}` }, resolve);
    req.on('error', reject);
  });
  response.catch(() => {});
  return { response, close: () => req.destroy() };
}
function readBytes(stream) {
  let data = Buffer.alloc(0), error = null;
  const waiters = [];
  function settle() {
    for (let i = waiters.length - 1; i >= 0; i--) {
      const entry = waiters[i];
      if (data.length >= entry.length) { waiters.splice(i, 1); entry.resolve(data.subarray(0, entry.length)); }
      else if (error) { waiters.splice(i, 1); entry.reject(error); }
    }
  }
  stream.on('data', (chunk) => { data = Buffer.concat([data, chunk]); settle(); });
  stream.on('close', () => { error = new Error('downlink closed'); settle(); });
  stream.on('error', (err) => { error = err; settle(); });
  return (length) => {
    if (data.length >= length) return Promise.resolve(data.subarray(0, length));
    if (error) return Promise.reject(error);
    return new Promise((resolve, reject) => waiters.push({ length, resolve, reject }));
  };
}
async function waitForCount(port, expected) {
  for (let i = 0; i < 150; i++) {
    const snapshot = await health(port);
    if (snapshot.memory_guard.xhttp_packet_buffer_count === expected) return snapshot;
    await sleep(30);
  }
  throw new Error(`packet count did not reach ${expected}; ${logs}`);
}
async function waitForReaders(port, active, waiting) {
  for (let i = 0; i < 150; i++) {
    const xhttp = (await health(port)).transports.xhttp;
    if (xhttp.packet_readers_active === active && xhttp.packet_readers_waiting === waiting) return;
    await sleep(30);
  }
  throw new Error(`readers did not reach ${active}/${waiting}; ${logs}`);
}

async function run() {
  sudo('mkdir', group); created = true;
  sudo('sh', '-c', 'printf 134217728 > "$1/memory.max"; printf 0 > "$1/memory.swap.max"; printf "50000 100000" > "$1/cpu.max"', 'sh', group);
  fs.writeFileSync(path.join(temp, 'db.json'), JSON.stringify({ users: [
    { id: 'test', uuid, is_active: 1, traffic_limit: 0, connection_limit: 0 }
  ] }));
  target = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    socket.on('error', () => {});
    socket.on('data', (data) => socket.write(data));
  });
  target.listen(0, '127.0.0.1');
  await once(target, 'listening');
  const targetPort = target.address().port;
  const port = await freePort();

  // Charge a uniquely copied, cache-evicted Node executable to the cgroup.
  const nodeExecutable = path.join(temp, 'node-cold');
  fs.copyFileSync(process.execPath, nodeExecutable);
  fs.chmodSync(nodeExecutable, 0o755);
  execFileSync('python3', ['-c',
    'import os,sys; f=os.open(sys.argv[1],os.O_RDONLY); os.fsync(f); os.posix_fadvise(f,0,0,os.POSIX_FADV_DONTNEED); os.close(f)', nodeExecutable]);
  const shell = 'echo $$ > "$1/cgroup.procs"; shift; user="$1"; shift; exec runuser -u "$user" -- "$@"';
  panel = spawn('sudo', ['-n', 'sh', '-c', shell, 'cgroup-start', group, os.userInfo().username,
    'env', `PORT=${port}`, `DB_PATH=${path.join(temp, 'db.json')}`, 'KEEPALIVE_ENABLED=0',
    'NODE_OPTIONS=--max-old-space-size=48 --max-semi-space-size=1', 'MALLOC_ARENA_MAX=1',
    nodeExecutable, '--require', path.join(__dirname, 'helpers/loopback.js'), path.join(root, 'server.js')],
  { cwd: root, stdio: ['ignore', 'pipe', 'pipe'] });
  panel.stdout.on('data', (data) => { logs = (logs + data).slice(-30000); });
  panel.stderr.on('data', (data) => { logs = (logs + data).slice(-30000); });
  let initial;
  for (let i = 0; i < 100; i++) {
    if (panel.exitCode !== null || panel.signalCode !== null) throw new Error(`panel exited: ${logs}`);
    try { initial = await health(port); break; } catch (_) { await sleep(100); }
  }
  assert.ok(initial, `panel did not start: ${logs}`);
  assert.equal(initial.low_memory_mode, true);
  assert.equal(initial.memory_limit_mb, 128);
  assert.equal(initial.memory_guard.active, true);
  assert.equal(initial.memory_guard.usage_source, 'cgroup-v2');
  const baseline = groupNumber('memory.current');
  const pending = [];
  const slow = openDownlink(port, 'stalled');
  const first = request(port, 'POST', '/xhttp/stalled/0', header('never-dns.test', 443));
  first.catch(() => {}); pending.push(first);
  await waitForCount(port, 1);
  const small = Buffer.alloc(8192, 0x61);
  for (let seq = 1; seq <= 24; seq++) {
    const post = request(port, 'POST', `/xhttp/stalled/${seq}`, small);
    post.catch(() => {}); pending.push(post);
  }
  await waitForCount(port, 25);
  // The global body-reader cap must also stay fair when one session sends
  // multiple incomplete bodies (not merely when DNS or TCP connect stalls).
  await sleep(400);
  const slowBodies = [];
  for (let seq = 0; seq < 66; seq++) {
    const req = http.request({ host: '127.0.0.1', port, method: 'POST',
      path: `/xhttp/slow-body/${seq}`, headers: { 'content-length': 1024 } });
    req.on('error', () => {});
    req.write('x');
    slowBodies.push(req);
  }
  await waitForReaders(port, 2, 64);
  const waiterOverflow = await timeout(request(port, 'POST', '/xhttp/slow-body/66', Buffer.alloc(0)),
    4000, 'body-waiter cap rejection');
  assert.equal(waiterOverflow.status, 503);
  // Let the admission projection settle; the stalled flows remain open.
  await sleep(400);
  const healthy = openDownlink(port, 'healthy');
  const started = Date.now();
  const before = Buffer.from('healthy-before-pressure');
  const post = await timeout(request(port, 'POST', '/xhttp/healthy/0',
    Buffer.concat([header('target.test', targetPort), before])), 4000, 'healthy admission');
  assert.equal(post.status, 200);
  const res = await timeout(healthy.response, 4000, 'healthy downlink');
  const read = readBytes(res);
  assert.deepEqual(await timeout(read(before.length + 2), 3000, 'healthy echo'),
    Buffer.concat([Buffer.from([0, 0]), before]));
  const healthyMs = Date.now() - started;
  for (const req of slowBodies) req.destroy();
  await waitForReaders(port, 0, 0);

  // Exercise the independent byte cap with real socket-backed 1 MiB POSTs.
  const large = Buffer.alloc(1024 * 1024, 0x62);
  for (let seq = 25; seq <= 27; seq++) {
    const p = request(port, 'POST', `/xhttp/stalled/${seq}`, large);
    p.catch(() => {}); pending.push(p);
    await waitForCount(port, seq + 1);
  }
  const refused = await timeout(request(port, 'POST', '/xhttp/stalled/28', large), 4000, 'byte-cap rejection');
  assert.equal(refused.status, 503);
  const pressured = await health(port);
  assert.ok(pressured.memory_guard.xhttp_packet_buffer_bytes <= 5 * 1024 * 1024);
  assert.ok(pressured.memory_guard.xhttp_packet_buffer_count <= 257);
  assert.equal(pressured.transports.xhttp.packet_readers_active, 0);
  const during = Buffer.from('healthy-during-pressure');
  assert.equal((await timeout(request(port, 'POST', '/xhttp/healthy/1', during), 4000, 'healthy under load')).status, 200);
  assert.deepEqual(await timeout(read(before.length + during.length + 2), 4000, 'echo under load'),
    Buffer.concat([Buffer.from([0, 0]), before, during]));
  slow.close();
  await waitForCount(port, 0);
  const after = Buffer.from('healthy-after-cleanup');
  assert.equal((await timeout(request(port, 'POST', '/xhttp/healthy/2', after), 4000, 'healthy recovery')).status, 200);
  assert.deepEqual(await timeout(read(before.length + during.length + after.length + 2), 4000, 'recovery echo'),
    Buffer.concat([Buffer.from([0, 0]), before, during, after]));
  healthy.close();

  // Fill the global packet-count budget with tiny out-of-order packets. Byte
  // accounting alone cannot bound the JS/request overhead of this pattern.
  const tiny = Buffer.from('x');
  for (let session = 0; session < 8; session++) {
    for (let seq = 1; seq <= 32; seq++) {
      assert.equal((await request(port, 'POST', `/xhttp/count-${session}/${seq}`, tiny)).status, 200);
    }
    await sleep(350); // do not turn this into an admission-projection test
  }
  let countSnapshot = await waitForCount(port, 256);
  assert.equal(countSnapshot.memory_guard.xhttp_packet_buffer_bytes, 256);
  assert.equal((await request(port, 'POST', '/xhttp/count-overflow/1', tiny)).status, 503);
  // The missing sequence gets the single global reserve even when full.
  const missing = request(port, 'POST', '/xhttp/count-0/0', header('never-dns.test', 443));
  missing.catch(() => {}); pending.push(missing);
  await waitForCount(port, 257);
  assert.equal((await request(port, 'POST', '/xhttp/count-overflow/2', tiny)).status, 503);
  const release = openDownlink(port, 'count-0');
  await sleep(100);
  release.close();
  countSnapshot = await waitForCount(port, 224);
  assert.equal(countSnapshot.memory_guard.xhttp_packet_buffer_bytes, 224);
  assert.equal((await request(port, 'POST', '/xhttp/count-overflow/1', tiny)).status, 200);
  await waitForCount(port, 225);
  const final = await health(port);
  const memoryEvents = groupStats('memory.events');
  const peak = groupNumber('memory.peak');
  assert.equal(memoryEvents.oom_kill, 0, `OOM during XHTTP test: ${logs}`);
  assert.ok(peak <= 128 * 1024 * 1024);
  console.log(JSON.stringify({
    cold_node: true, limit_mb: 128, swap_mb: 0, cpu_limit: 0.5,
    baseline_mb: Number((baseline / 1048576).toFixed(1)), peak_mb: Number((peak / 1048576).toFixed(1)),
    pending_small_posts: 24, slow_body_readers: 2, slow_body_waiters: 64,
    body_waiter_overflow_status: waiterOverflow.status, queued_large_posts: 3, overflow_status: refused.status,
    tiny_packets_at_cap: 256, expected_packet_reserved: true, tiny_packet_recovery: true,
    healthy_echo_ms: healthyMs, healthy_recovered: true,
    memory_events: memoryEvents, cpu_stat: groupStats('cpu.stat'), guard: final.memory_guard,
    event_loop_delay: final.event_loop_delay
  }, null, 2));
}

async function cleanup() {
  for (const socket of sockets) socket.destroy();
  if (target) target.close();
  if (created) {
    try { sudo('sh', '-c', 'printf 1 > "$1/cgroup.kill"', 'sh', group); } catch (_) {}
    if (panel && panel.exitCode === null && panel.signalCode === null) {
      await Promise.race([once(panel, 'exit').catch(() => {}), sleep(1500)]);
    }
    try { sudo('rmdir', group); } catch (err) { console.error('Could not remove test cgroup:', err.message); }
  }
  fs.rmSync(temp, { recursive: true, force: true });
}

run().catch((err) => { console.error('XHTTP CGROUP TEST FAILED:', err.stack || err); console.error(logs); process.exitCode = 1; })
  .finally(cleanup);
