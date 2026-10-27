'use strict';

// Optional Linux-only, privileged test. It creates a temporary cgroup-v2 child
// with a real 128 MiB limit and places ONLY the test server process in it. The
// traffic generator stays outside that cgroup. No production code is patched;
// a test-only preload routes synthetic target names to a loopback TCP server.
// Run: node test/cgroup-stress.js [clients]  (needs sudo without a password).
const assert = require('node:assert/strict');
const { spawn, execFileSync } = require('node:child_process');
const { once } = require('node:events');
const fs = require('node:fs');
const http = require('node:http');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');

const root = path.join(__dirname, '..');
const group = `/sys/fs/cgroup/arena-panel-test-${process.pid}`;
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'panel-real-cgroup-'));
const count = Math.max(1, Math.min(32, Number(process.argv[2] || 6)));
const burst = process.argv.includes('burst');
const cold = process.argv.includes('cold');
const transport = process.argv.includes('mux') ? 'mux' : process.argv.includes('xhttp') ? 'xhttp' : 'ws';
const light = process.argv.includes('light') || transport === 'mux';
const largeInitial = process.argv.includes('large-initial');
const initialPayload = largeInitial ? Buffer.alloc(768 * 1024, 0x63) : null;
const uuid = '11111111111111111111111111111111';
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const sourceChunk = Buffer.alloc(16 * 1024, 0x5a);
let panel, upstream, groupCreated = false;
let clients = [];
let rejected = 0;
let serverSockets = new Set();
const muxPayloads = new Set();
let logs = '';
let receivedInitialBytes = 0;
let invalidInitialBytes = false;

function sudo(...args) { return execFileSync('sudo', ['-n', ...args], { encoding: 'utf8' }); }
function number(file) { return Number(fs.readFileSync(path.join(group, file), 'utf8').trim()); }
function events() {
  return Object.fromEntries(fs.readFileSync(path.join(group, 'memory.events'), 'utf8').trim().split('\n')
    .map((line) => line.split(/\s+/)).map(([k, v]) => [k, Number(v)]));
}
function health(port) {
  return new Promise((resolve) => {
    const req = http.get({ host: '127.0.0.1', port, path: '/api/health', timeout: 1500 }, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch (_) { resolve(null); } });
    });
    req.on('timeout', () => req.destroy());
    req.on('error', () => resolve(null));
  });
}
function header(port) {
  const host = Buffer.from('cdn.test');
  const p = Buffer.alloc(2); p.writeUInt16BE(port);
  return Buffer.concat([Buffer.from([0]), Buffer.from(uuid, 'hex'), Buffer.from([0, 1]), p,
    Buffer.from([2, host.length]), host]);
}
function firstPacket(port) {
  const request = header(port);
  return initialPayload ? Buffer.concat([request, initialPayload]) : request;
}

function openClient(port, upstreamPort) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`, { perMessageDeflate: false });
    const entry = { ws, bytes: 0, closed: false, resume: () => ws.resume(), close: () => ws.terminate() };
    ws.once('open', () => ws.send(firstPacket(upstreamPort)));
    ws.once('error', reject);
    ws.once('close', () => { entry.closed = true; });
    ws.once('unexpected-response', (req, res) => {
      res.resume();
      if (res.statusCode !== 503) return reject(new Error(`unexpected status ${res.statusCode}`));
      rejected += 1;
      resolve(null);
    });
    ws.once('message', () => {
      clients.push(entry);
      ws.on('message', (data) => { entry.bytes += data.length; });
      ws.pause();
      resolve(entry);
    });
  });
}

function muxNewFrame(id, port) {
  const host = Buffer.from('cdn.test');
  const payload = Buffer.from(`stream-${id}`);
  const portBytes = Buffer.alloc(2); portBytes.writeUInt16BE(port);
  const metadata = Buffer.concat([Buffer.from([id >> 8, id & 255, 1, 1, 1]), portBytes,
    Buffer.from([2, host.length]), host]);
  const metaLength = Buffer.alloc(2); metaLength.writeUInt16BE(metadata.length);
  const dataLength = Buffer.alloc(2); dataLength.writeUInt16BE(payload.length);
  return Buffer.concat([metaLength, metadata, dataLength, payload]);
}

function exerciseMux(port, upstreamPort) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`, { perMessageDeflate: false });
    const entry = { ws, bytes: 0, closed: false, resume: () => ws.resume(), close: () => ws.terminate() };
    clients.push(entry);
    const received = new Map();
    let responseHeader = false;
    ws.once('error', reject);
    ws.once('close', () => { entry.closed = true; if (received.size < count) reject(new Error('MUX closed early')); });
    ws.on('message', (raw) => {
      const frame = Buffer.from(raw);
      if (!responseHeader) {
        responseHeader = true;
        if (frame.length !== 2 || frame[0] !== 0 || frame[1] !== 0) reject(new Error('bad MUX VLESS header'));
        return;
      }
      if (frame.length < 6) return reject(new Error('short MUX frame'));
      const metaLength = frame.readUInt16BE(0);
      const id = frame.readUInt16BE(2);
      const status = frame[4], option = frame[5];
      if (status === 3) return reject(new Error(`MUX substream ${id} was rejected`));
      if (metaLength !== 4 || status !== 2 || !(option & 1)) return reject(new Error('unexpected MUX frame'));
      const n = frame.readUInt16BE(6);
      if (frame.length !== n + 8) return reject(new Error('bad MUX frame length'));
      received.set(id, (received.get(id) || 0) + n);
      if (received.size === count && [...received.values()].every((size) => size === 256 * 1024)) resolve(received);
    });
    ws.once('open', () => {
      const muxHeader = Buffer.concat([Buffer.from([0]), Buffer.from(uuid, 'hex'), Buffer.from([0, 3])]);
      const frames = Array.from({ length: count }, (_, i) => muxNewFrame(i + 1, upstreamPort));
      ws.send(Buffer.concat([muxHeader, ...frames]));
    });
  });
}

function openXhttpClient(port, upstreamPort, index) {
  return new Promise((resolve, reject) => {
    const sid = `cgroup-xhttp-${index}`;
    let settled = false;
    let getReq, postReq;
    const entry = { bytes: 0, closed: false, resume: () => {}, close: () => { getReq?.destroy(); postReq?.destroy(); } };
    const fail = (err) => { if (!settled) { settled = true; reject(err); } };
    const rejectNew = () => {
      if (settled) return;
      settled = true;
      rejected += 1;
      entry.close();
      resolve(null);
    };
    getReq = http.get({ host: '127.0.0.1', port, path: `/xhttp/${sid}` }, (res) => {
      if (res.statusCode === 503) { res.resume(); rejectNew(); return; }
      if (res.statusCode !== 200) return fail(new Error(`XHTTP status ${res.statusCode}`));
      entry.resume = () => res.resume();
      res.once('close', () => { entry.closed = true; });
      res.once('data', (data) => {
        if (data.length < 2 || data[0] !== 0 || data[1] !== 0) return fail(new Error('bad XHTTP VLESS header'));
        if (settled) return;
        settled = true;
        clients.push(entry);
        res.on('data', (chunk) => { entry.bytes += chunk.length; });
        res.pause();
        resolve(entry);
      });
    });
    getReq.on('error', fail);
    const body = firstPacket(upstreamPort);
    postReq = http.request({ host: '127.0.0.1', port, path: `/xhttp/${sid}/0`, method: 'POST',
      headers: { 'content-type': 'application/octet-stream', 'content-length': body.length } }, (res) => {
      res.resume();
      if (res.statusCode === 503) rejectNew();
      else if (res.statusCode !== 200) fail(new Error(`XHTTP POST status ${res.statusCode}`));
    });
    postReq.on('error', fail);
    postReq.end(body);
  });
}

async function run() {
  sudo('mkdir', group);
  groupCreated = true;
  sudo('sh', '-c', 'printf 134217728 > "$1/memory.max"; printf 0 > "$1/memory.swap.max"', 'sh', group);
  fs.writeFileSync(path.join(temp, 'db.json'), JSON.stringify({ admin: { username: 'test', password_hash: 'x' }, users: [
    { id: 'stress', remark: 'stress', uuid, is_active: 1, traffic_limit: 0, connection_limit: 0 }
  ] }));

  upstream = net.createServer((socket) => {
    serverSockets.add(socket);
    socket.on('error', () => {});
    socket.on('close', () => serverSockets.delete(socket));
    if (transport === 'mux') socket.on('data', (data) => muxPayloads.add(data.toString()));
    else if (largeInitial) socket.on('data', (data) => {
      receivedInitialBytes += data.length;
      if (!data.every((byte) => byte === 0x63)) invalidInitialBytes = true;
    });
    let sent = 0;
    function pump() {
      while (!socket.destroyed && (!light || sent < 256 * 1024)) {
        sent += sourceChunk.length;
        if (!socket.write(sourceChunk)) return;
      }
    }
    socket.on('drain', pump);
    pump();
  });
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const upstreamPort = upstream.address().port;
  const tempServer = net.createServer();
  tempServer.listen(0, '127.0.0.1');
  await once(tempServer, 'listening');
  const port = tempServer.address().port;
  await new Promise((resolve) => tempServer.close(resolve));

  let nodeExecutable = process.execPath;
  if (cold) {
    // A unique, cache-evicted Node binary makes its file-backed pages charge to
    // the test cgroup, rather than to a previous warm process outside it.
    nodeExecutable = path.join(temp, 'node-cold');
    fs.copyFileSync(process.execPath, nodeExecutable);
    fs.chmodSync(nodeExecutable, 0o755);
    const fd = fs.openSync(nodeExecutable, 'r');
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    execFileSync('python3', ['-c',
      'import os,sys; f=os.open(sys.argv[1],os.O_RDONLY); os.posix_fadvise(f,0,0,os.POSIX_FADV_DONTNEED); os.close(f)', nodeExecutable]);
  }
  const user = os.userInfo().username;
  // Move the shell into the group before exec so the initial Node allocations
  // are charged to this cgroup too. runuser drops privileges for the server.
  const shell = 'echo $$ > "$1/cgroup.procs"; shift; user="$1"; shift; exec runuser -u "$user" -- "$@"';
  const envArgs = [
    `PORT=${port}`, `DB_PATH=${path.join(temp, 'db.json')}`, 'KEEPALIVE_ENABLED=0',
    'NODE_OPTIONS=--max-old-space-size=48 --max-semi-space-size=1', 'MALLOC_ARENA_MAX=1'
  ];
  panel = spawn('sudo', ['-n', 'sh', '-c', shell, 'cgroup-start', group, user,
    'env', ...envArgs, nodeExecutable, '--require', path.join(__dirname, 'helpers/loopback.js'), path.join(root, 'server.js')],
  { cwd: root, stdio: ['ignore', 'pipe', 'pipe'] });
  panel.stdout.on('data', (data) => { logs = (logs + data.toString()).slice(-30000); });
  panel.stderr.on('data', (data) => { logs = (logs + data.toString()).slice(-30000); });
  let initial = null;
  for (let i = 0; i < 100; i++) {
    if (panel.exitCode !== null || panel.signalCode !== null) throw new Error(`panel exited early: ${logs}`);
    initial = await health(port);
    if (initial) break;
    await sleep(100);
  }
  assert.ok(initial, `panel did not start: ${logs}`);
  assert.equal(initial.low_memory_mode, true);
  assert.equal(initial.memory_limit_mb, 128);
  assert.equal(initial.memory_guard.active, true);
  assert.equal(initial.memory_guard.usage_source, 'cgroup-v2');
  const baseline = number('memory.current');

  if (transport === 'mux') {
    const received = await Promise.race([
      exerciseMux(port, upstreamPort),
      sleep(12000).then(() => { throw new Error('MUX admission test timed out'); })
    ]);
    await sleep(100);
    assert.equal(events().oom_kill, 0, `OOM-killed during MUX burst; logs: ${logs}`);
    assert.equal(clients[0].closed, false);
    for (let i = 1; i <= count; i++) {
      assert.equal(received.get(i), 256 * 1024, `MUX substream ${i} lost bytes`);
      assert.ok(muxPayloads.has(`stream-${i}`), `MUX initial payload ${i} was lost`);
    }
    const h = await health(port);
    assert.equal(h.memory_guard.rejected_new_connections, 0);
    console.log(JSON.stringify({ transport, substreams: count, cold_node_binary: cold,
      baseline_cgroup_mb: Number((baseline / 1048576).toFixed(1)),
      peak_cgroup_mb: Number((number('memory.peak') / 1048576).toFixed(1)),
      received_bytes_per_substream: 256 * 1024, events: events(), final_guard: h.memory_guard }, null, 2));
    return;
  }

  const connectOne = (i) => Promise.race([
    transport === 'ws' ? openClient(port, upstreamPort) : openXhttpClient(port, upstreamPort, i),
    sleep(10000).then(() => { throw new Error(`client ${i + 1} could not connect`); })
  ]);
  if (burst) await Promise.all(Array.from({ length: count }, (_, i) => connectOne(i)));
  else for (let i = 0; i < count; i++) await connectOne(i);
  let maxCurrent = number('memory.current');
  let states = new Set();
  for (let i = 0; i < 15; i++) {
    await sleep(200);
    maxCurrent = Math.max(maxCurrent, number('memory.current'));
    const h = await health(port);
    if (h) states.add(h.memory_guard.state);
  }
  assert.equal(events().oom_kill, 0, `OOM-killed while clients were stalled; logs: ${logs}`);
  assert.ok(clients.length > 0, 'no client was admitted');
  assert.equal(clients.length + rejected, count);
  if (light && !largeInitial) assert.equal(rejected, 0, 'a transient burst should be delayed, not rejected');
  assert.ok(clients.every((c) => !c.closed), 'an admitted client was closed during throttling');
  for (const client of clients) client.resume();
  await sleep(1500);
  assert.equal(events().oom_kill, 0, `OOM-killed after resuming; logs: ${logs}`);
  assert.ok(clients.every((c) => !c.closed && c.bytes > 65536), 'a client did not resume transferring');
  if (largeInitial) {
    assert.equal(invalidInitialBytes, false, 'large initial payload was corrupted');
    assert.equal(receivedInitialBytes, clients.length * initialPayload.length, 'large initial payload lost bytes');
  }
  const finalHealth = await health(port);
  assert.ok(finalHealth?.ok, 'health endpoint failed after load');
  const peak = number('memory.peak');
  console.log(JSON.stringify({
    transport,
    attempted_clients: count,
    burst,
    light,
    cold_node_binary: cold,
    initial_payload_bytes_per_client: initialPayload?.length || 0,
    received_initial_bytes: receivedInitialBytes,
    admitted_clients: clients.length,
    rejected_new_clients: rejected,
    limit_mb: 128,
    baseline_cgroup_mb: Number((baseline / 1048576).toFixed(1)),
    peak_cgroup_mb: Number((peak / 1048576).toFixed(1)),
    sampled_peak_mb: Number((maxCurrent / 1048576).toFixed(1)),
    observed_states: [...states],
    events: events(),
    resumed_bytes_per_client: clients.map((c) => c.bytes),
    final_guard: finalHealth.memory_guard
  }, null, 2));
  assert.ok(peak <= 128 * 1048576, 'peak must not exceed cgroup limit');
}

async function cleanup() {
  for (const client of clients) { try { client.close(); } catch (_) {} }
  for (const socket of serverSockets) socket.destroy();
  if (upstream) upstream.close();
  if (groupCreated) {
    try { sudo('sh', '-c', 'printf 1 > "$1/cgroup.kill"', 'sh', group); } catch (_) {}
    if (panel && panel.exitCode === null && panel.signalCode === null) {
      await Promise.race([once(panel, 'exit').catch(() => {}), sleep(1500)]);
    }
    try { sudo('rmdir', group); } catch (err) { console.error('Could not remove test cgroup:', err.message); }
  }
  fs.rmSync(temp, { recursive: true, force: true });
}

run().catch((err) => { console.error('CGROUP TEST FAILED:', err.stack || err); console.error(logs); process.exitCode = 1; })
  .finally(cleanup);
