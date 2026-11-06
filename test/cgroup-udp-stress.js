'use strict';

// Optional privileged Linux test: exercise real, bounded UDP bursts against a
// cold server in a 128 MiB / zero-swap cgroup. The traffic generator and UDP
// target remain outside the cgroup. Never run on a production machine.
// Run: node test/cgroup-udp-stress.js [clients] (needs sudo without a password).
const assert = require('node:assert/strict');
const { spawn, execFileSync } = require('node:child_process');
const { once } = require('node:events');
const dgram = require('node:dgram');
const fs = require('node:fs');
const http = require('node:http');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');

const root = path.join(__dirname, '..');
const group = `/sys/fs/cgroup/arena-panel-udp-${process.pid}`;
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'panel-udp-cgroup-'));
const count = Math.max(1, Math.min(32, Number(process.argv[2] || 12)));
const uuid = '11111111111111111111111111111111';
const probe = Buffer.from('probe-after-udp-burst');
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const clients = [];
let panel, target, groupCreated = false, logs = '', rejected = 0;

function sudo(...args) { return execFileSync('sudo', ['-n', ...args], { encoding: 'utf8' }); }
function number(file) { return Number(fs.readFileSync(path.join(group, file), 'utf8').trim()); }
function events() {
  return Object.fromEntries(fs.readFileSync(path.join(group, 'memory.events'), 'utf8').trim().split('\n')
    .map((line) => line.split(/\s+/)).map(([key, value]) => [key, Number(value)]));
}
function timeout(promise, ms, label) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`timeout: ${label}`)), ms); })
  ]).finally(() => clearTimeout(timer));
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
function vlessHeader(port) {
  const hostname = Buffer.from('slow-udp.test');
  const portBytes = Buffer.alloc(2);
  portBytes.writeUInt16BE(port);
  return Buffer.concat([Buffer.from([0]), Buffer.from(uuid, 'hex'), Buffer.from([0, 2]), portBytes,
    Buffer.from([2, hostname.length]), hostname]);
}
function udpFrame(packet) {
  const size = Buffer.alloc(2);
  size.writeUInt16BE(packet.length);
  return Buffer.concat([size, packet]);
}

function openClient(port, targetPort, batch) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`, { perMessageDeflate: false });
    const client = { ws, closed: false, probeSeen: false };
    let settled = false, headerSeen = false;
    ws.on('error', (err) => { if (!settled) { settled = true; reject(err); } });
    ws.once('unexpected-response', (_req, res) => {
      res.resume();
      if (settled) return;
      settled = true;
      if (res.statusCode !== 503) return reject(new Error(`unexpected HTTP ${res.statusCode}`));
      rejected += 1;
      resolve(null);
    });
    ws.once('close', () => { client.closed = true; if (!settled) { settled = true; reject(new Error('UDP WS closed early')); } });
    ws.once('open', () => ws.send(vlessHeader(targetPort)));
    ws.on('message', (raw) => {
      const data = Buffer.from(raw);
      if (!headerSeen) {
        headerSeen = true;
        if (!data.equals(Buffer.from([0, 0]))) {
          if (!settled) { settled = true; reject(new Error('bad VLESS response')); }
          return;
        }
        clients.push(client);
        // 64 * 16 KiB is far above the 128 KiB pending-byte cap. Each WS
        // message remains below the low-memory maximum message size.
        ws.send(batch);
        ws.send(batch);
        if (!settled) { settled = true; resolve(client); }
        return;
      }
      if (data.length >= 2 && data.readUInt16BE(0) === data.length - 2 && data.subarray(2).equals(probe)) {
        client.probeSeen = true;
      }
    });
  });
}

async function run() {
  sudo('mkdir', group);
  groupCreated = true;
  sudo('sh', '-c', 'printf 134217728 > "$1/memory.max"; printf 0 > "$1/memory.swap.max"', 'sh', group);
  fs.writeFileSync(path.join(temp, 'db.json'), JSON.stringify({ admin: { username: 'test', password_hash: 'x' }, users: [
    { id: 'udp-stress', remark: 'test', uuid, is_active: 1, traffic_limit: 0, connection_limit: 0 }
  ] }));

  target = dgram.createSocket('udp4');
  target.on('message', (message, from) => { if (message.equals(probe)) target.send(message, from.port, from.address); });
  target.bind(0, '127.0.0.1');
  await once(target, 'listening');
  const targetPort = target.address().port;
  const reservation = net.createServer();
  reservation.listen(0, '127.0.0.1');
  await once(reservation, 'listening');
  const port = reservation.address().port;
  await new Promise((resolve) => reservation.close(resolve));

  // Evict the unique executable's page cache so cold Node is charged here.
  const nodeExecutable = path.join(temp, 'node-cold');
  fs.copyFileSync(process.execPath, nodeExecutable);
  fs.chmodSync(nodeExecutable, 0o755);
  const fd = fs.openSync(nodeExecutable, 'r');
  fs.fsyncSync(fd);
  fs.closeSync(fd);
  execFileSync('python3', ['-c',
    'import os,sys; f=os.open(sys.argv[1],os.O_RDONLY); os.posix_fadvise(f,0,0,os.POSIX_FADV_DONTNEED); os.close(f)', nodeExecutable]);
  const shell = 'echo $$ > "$1/cgroup.procs"; shift; user="$1"; shift; exec runuser -u "$user" -- "$@"';
  const env = [`PORT=${port}`, `DB_PATH=${path.join(temp, 'db.json')}`, 'KEEPALIVE_ENABLED=0',
    'NODE_OPTIONS=--max-old-space-size=48 --max-semi-space-size=1', 'MALLOC_ARENA_MAX=1'];
  panel = spawn('sudo', ['-n', 'sh', '-c', shell, 'cgroup-start', group, os.userInfo().username,
    'env', ...env, nodeExecutable, '--require', path.join(__dirname, 'helpers/loopback.js'), path.join(root, 'server.js')],
  { cwd: root, stdio: ['ignore', 'pipe', 'pipe'] });
  panel.stdout.on('data', (data) => { logs = (logs + data).slice(-30000); });
  panel.stderr.on('data', (data) => { logs = (logs + data).slice(-30000); });
  let initial = null;
  for (let i = 0; i < 100; i++) {
    if (panel.exitCode !== null || panel.signalCode !== null) throw new Error(`panel exited: ${logs}`);
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
  const batch = Buffer.concat(Array(32).fill(udpFrame(Buffer.alloc(16 * 1024, 0x63))));
  await timeout(Promise.all(Array.from({ length: count }, () => openClient(port, targetPort, batch))), 12000, 'UDP clients');
  assert.ok(clients.length > 0, 'at least one client must be admitted');
  assert.equal(clients.length + rejected, count);
  await sleep(700);
  const afterBurst = await health(port);
  assert.ok(afterBurst?.udp_drops.uplink_queue_full > 0, 'slow DNS burst must hit the bounded queue');
  for (let i = 0; i < 50 && clients.some((client) => !client.probeSeen); i++) {
    for (const client of clients) if (!client.closed && !client.probeSeen) client.ws.send(udpFrame(probe));
    await sleep(100);
  }
  assert.ok(clients.every((client) => !client.closed && client.probeSeen), 'every admitted UDP flow must recover');
  assert.equal(events().oom_kill, 0, `OOM while processing UDP burst: ${logs}`);
  const peak = number('memory.peak');
  assert.ok(peak <= 128 * 1048576, 'peak must not exceed the cgroup limit');
  console.log(JSON.stringify({
    transport: 'ws-udp', attempted_clients: count, admitted_clients: clients.length,
    rejected_new_clients: rejected, offered_datagrams_per_client: 64, offered_bytes_per_client: 64 * 16 * 1024,
    recovered_clients: clients.filter((client) => client.probeSeen).length,
    baseline_cgroup_mb: Number((baseline / 1048576).toFixed(1)), peak_cgroup_mb: Number((peak / 1048576).toFixed(1)),
    udp_drops: afterBurst.udp_drops, events: events(), final_guard: afterBurst.memory_guard
  }, null, 2));
}

async function cleanup() {
  for (const client of clients) { try { client.ws.terminate(); } catch (_) {} }
  if (target) { try { target.close(); } catch (_) {} }
  if (groupCreated) {
    try { sudo('sh', '-c', 'printf 1 > "$1/cgroup.kill"', 'sh', group); } catch (_) {}
    if (panel && panel.exitCode === null && panel.signalCode === null) {
      await Promise.race([once(panel, 'exit').catch(() => {}), sleep(1500)]);
    }
    try { sudo('rmdir', group); } catch (err) { console.error('Could not remove test cgroup:', err.message); }
  }
  fs.rmSync(temp, { recursive: true, force: true });
}

run().catch((err) => { console.error('UDP CGROUP TEST FAILED:', err.stack || err); console.error(logs); process.exitCode = 1; })
  .finally(cleanup);
