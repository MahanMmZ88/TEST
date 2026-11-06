'use strict';

// Optional end-to-end test with an independently installed Xray-core binary.
// Requires openssl and XRAY_BIN=/absolute/path/to/xray (or xray on PATH).
// This is a local simulation: TLS terminates at a loopback H2/H1+WS edge which
// forwards to the real Node origin. DNS loopback overrides are test-only.
// MEMORY_GUARD is disabled here; use cgroup-xhttp-stress.js for a real guard.
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
const { Transform } = require('node:stream');

const ROOT = path.join(__dirname, '..');
const XRAY = process.env.XRAY_BIN || 'xray';
const MEMORY_MB = Number(process.env.XRAY_TEST_MEMORY_MB || 1024);
const rawSoakSeconds = Number(process.env.XRAY_TEST_SOAK_SECONDS || 0);
const SOAK_SECONDS = Number.isFinite(rawSoakSeconds) ? Math.max(0, Math.min(rawSoakSeconds, 600)) : 0;
const UUID = '11111111-1111-1111-1111-111111111111';
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
function httpGet(port, pathname) {
  return new Promise((resolve, reject) => {
    const req = http.get({ host: '127.0.0.1', port, path: pathname }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.on('error', reject);
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
    return new Promise((resolve, reject) => {
      this.waiting.push({ length, resolve, reject });
      this.flush();
    });
  }
}

async function socksConnect(port, host, targetPort) {
  const socket = net.connect(port, '127.0.0.1');
  const reader = new Reader(socket);
  await timeout(once(socket, 'connect'), 4000, 'SOCKS connect');
  socket.write(Buffer.from([5, 1, 0]));
  assert.deepEqual(await timeout(reader.read(2), 4000, 'SOCKS greeting'), Buffer.from([5, 0]));
  const name = Buffer.from(host);
  const portBytes = Buffer.alloc(2); portBytes.writeUInt16BE(targetPort);
  socket.write(Buffer.concat([Buffer.from([5, 1, 0, 3, name.length]), name, portBytes]));
  const result = await timeout(reader.read(4), 4000, 'SOCKS request');
  assert.equal(result[1], 0, `SOCKS error ${result[1]}`);
  if (result[3] === 1) await reader.read(4);
  else if (result[3] === 4) await reader.read(16);
  else if (result[3] === 3) await reader.read((await reader.read(1))[0]);
  else throw new Error(`unknown SOCKS response address type ${result[3]}`);
  await reader.read(2);
  return { socket, reader };
}
async function tcpEcho(socksPort, targetPort, size) {
  const { socket, reader } = await socksConnect(socksPort, 'target.test', targetPort);
  try {
    const payload = crypto.randomBytes(size);
    socket.write(payload);
    assert.deepEqual(await timeout(reader.read(payload.length), 10000, `TCP echo ${size}`), payload);
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
    const portBytes = Buffer.alloc(2); portBytes.writeUInt16BE(targetPort);
    for (let i = 0; i < count; i++) {
      const payload = Buffer.from(`udp-${String(i).padStart(3, '0')}`);
      const packet = Buffer.concat([Buffer.from([0, 0, 0, 3, host.length]), host, portBytes, payload]);
      await new Promise((resolve, reject) => udp.send(packet, relayPort, relay, (err) => err ? reject(err) : resolve()));
      await delay(10);
    }
    const deadline = Date.now() + 3000;
    while (received.size < count && Date.now() < deadline) await delay(20);
    assert.equal(received.size, count, `local UDP replies: ${received.size}/${count}`);
    return received.size;
  } finally { control.destroy(); try { udp.close(); } catch (_) {} }
}

function makeEdge(originPort, key, cert, http1Only = false) {
  const stats = { h2: 0, h1: 0, ws: 0, pings: 0, rejectedPings: 0, packetPosts: 0 };
  const sessions = new Set();
  const sockets = new Set();
  const server = http1Only
    ? https.createServer({ key, cert, ALPNProtocols: ['http/1.1'] })
    : http2.createSecureServer({ key, cert, allowHTTP1: true });
  server.on('session', (session) => {
    sessions.add(session);
    let lastPing = 0;
    session.on('ping', () => {
      stats.pings += 1;
      const now = Date.now();
      if (server.minimumPingMs && lastPing && now - lastPing < server.minimumPingMs) {
        stats.rejectedPings += 1;
        try { session.goaway(http2.constants.NGHTTP2_ENHANCE_YOUR_CALM, 0, Buffer.from('too_many_pings')); } catch (_) {}
        setTimeout(() => session.destroy(), 20);
      }
      lastPing = now;
    });
    session.on('error', () => {});
    session.on('close', () => sessions.delete(session));
  });
  server.on('connection', (socket) => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('close', () => sockets.delete(socket));
  });
  function pipeWithDelay(source, destination) {
    if (!server.delayMs && !server.delayPattern) { source.pipe(destination); return; }
    let index = 0;
    const transform = new Transform({
      transform(chunk, encoding, callback) {
        const pattern = server.delayPattern;
        const lag = pattern ? pattern[index++ % pattern.length] : (server.delayMs || 0);
        setTimeout(() => callback(null, chunk), lag);
      }
    });
    transform.on('error', () => {});
    source.pipe(transform).pipe(destination);
  }
  server.on('request', (req, res) => {
    const h2 = req.httpVersionMajor === 2;
    stats[h2 ? 'h2' : 'h1'] += 1;
    if (req.method === 'POST' && /\/\d+$/.test(req.url)) stats.packetPosts += 1;
    const headers = {};
    for (const [key, value] of Object.entries(req.headers)) {
      if (!key.startsWith(':') && !['connection', 'upgrade', 'transfer-encoding'].includes(key.toLowerCase())) headers[key] = value;
    }
    const upstream = http.request({ host: '127.0.0.1', port: originPort, path: req.url,
      method: req.method, headers, agent: false }, (response) => {
      const cleanHeaders = {};
      for (const [key, value] of Object.entries(response.headers)) {
        if (!['connection', 'transfer-encoding', 'keep-alive'].includes(key.toLowerCase())) cleanHeaders[key] = value;
      }
      try { res.writeHead(response.statusCode, cleanHeaders); pipeWithDelay(response, res); }
      catch (_) { response.destroy(); }
    });
    req.on('error', () => {});
    res.on('error', () => {});
    res.on('close', () => upstream.destroy());
    upstream.on('error', () => {
      try { if (!res.headersSent) res.writeHead(502); res.end(); } catch (_) {}
    });
    pipeWithDelay(req, upstream);
  });
  server.on('upgrade', (req, client, head) => {
    stats.ws += 1;
    const upstream = net.connect(originPort, '127.0.0.1');
    upstream.on('error', () => client.destroy());
    client.on('error', () => upstream.destroy());
    client.on('close', () => upstream.destroy());
    upstream.on('close', () => client.destroy());
    upstream.on('connect', () => {
      upstream.write(`${req.method} ${req.url} HTTP/1.1\r\n${req.rawHeaders.reduce((text, value, i) =>
        text + (i % 2 ? `${value}\r\n` : `${value}: `), '')}\r\n`);
      if (head.length) upstream.write(head);
      pipeWithDelay(client, upstream);
      pipeWithDelay(upstream, client);
    });
  });
  return {
    server, stats,
    dropConnections() {
      for (const session of sessions) session.destroy();
      for (const socket of sockets) socket.destroy();
    }
  };
}

async function run() {
  const version = execFileSync(XRAY, ['version'], { encoding: 'utf8' }).split('\n')[0];
  console.log(JSON.stringify({ xray: version, simulated_memory_mb: MEMORY_MB, guard_enabled: false }));
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'panel-xray-integration-'));
  let panel, edge, h1Edge, tcpTarget, udpTarget;
  const targetSockets = new Set();
  let panelLogs = '';
  let xrayLogs = '';
  try {
    const db = path.join(temp, 'db.json');
    fs.writeFileSync(db, JSON.stringify({ users: [{
      id: 'integration', remark: 'integration', uuid: UUID.replace(/-/g, ''), sub_id: 'integration-sub',
      is_active: 1, traffic_limit: 0, connection_limit: 0
    }] }));
    const panelPort = await freePort();
    panel = spawn(process.execPath, ['--require', path.join(__dirname, 'helpers/loopback.js'), path.join(ROOT, 'server.js')], {
      cwd: ROOT,
      env: { ...process.env, PORT: String(panelPort), DB_PATH: db, MEMORY_LIMIT_MB: String(MEMORY_MB),
        MEMORY_GUARD: '0', KEEPALIVE_ENABLED: '0', PANEL_TEST_SLOW_CONNECT_MS: '2200' },
      stdio: ['ignore', 'pipe', 'pipe']
    });
    panel.stdout.on('data', (data) => { panelLogs = (panelLogs + data).slice(-30000); });
    panel.stderr.on('data', (data) => { panelLogs = (panelLogs + data).slice(-30000); });
    let ready = false;
    for (let i = 0; i < 100; i++) {
      if (panel.exitCode !== null) throw new Error(`panel exited: ${panelLogs}`);
      try { if ((await httpGet(panelPort, '/health')).status === 200) { ready = true; break; } } catch (_) {}
      await delay(50);
    }
    assert.ok(ready, `panel did not start: ${panelLogs}`);
    const subscription = await httpGet(panelPort, '/sub/integration-sub?raw=1');
    const links = Buffer.from(subscription.body.toString(), 'base64').toString().split('\n');
    const xhttp = new URL(links.find((link) => new URL(link).searchParams.get('type') === 'xhttp'));
    const generatedKeepalive = JSON.parse(xhttp.searchParams.get('extra')).xmux.hKeepAlivePeriod;
    assert.equal(generatedKeepalive, 45);

    tcpTarget = net.createServer((socket) => {
      targetSockets.add(socket);
      socket.on('error', () => {});
      socket.on('close', () => targetSockets.delete(socket));
      socket.on('data', (data) => socket.write(data));
    });
    tcpTarget.listen(0, '127.0.0.1');
    await once(tcpTarget, 'listening');
    const tcpPort = tcpTarget.address().port;
    udpTarget = dgram.createSocket('udp4');
    udpTarget.on('message', (data, address) => udpTarget.send(data, address.port, address.address));
    udpTarget.bind(0, '127.0.0.1');
    await once(udpTarget, 'listening');
    const udpPort = udpTarget.address().port;

    const keyFile = path.join(temp, 'key.pem');
    const certFile = path.join(temp, 'cert.pem');
    execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', keyFile,
      '-out', certFile, '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'], { stdio: 'ignore' });
    const cert = fs.readFileSync(certFile);
    const pin = crypto.createHash('sha256').update(new crypto.X509Certificate(cert).raw).digest('hex');
    edge = makeEdge(panelPort, fs.readFileSync(keyFile), cert);
    edge.server.listen(0, '127.0.0.1');
    await once(edge.server, 'listening');
    h1Edge = makeEdge(panelPort, fs.readFileSync(keyFile), cert, true);
    h1Edge.server.listen(0, '127.0.0.1');
    await once(h1Edge.server, 'listening');

    async function withXray({ mode, mux = false, http1 = false, keepalive = generatedKeepalive }, fn) {
      const activeEdge = http1 ? h1Edge : edge;
      const edgePort = activeEdge.server.address().port;
      const socksPort = await freePort();
      const streamSettings = mode === 'ws'
        ? { network: 'ws', security: 'tls', wsSettings: { path: '/ws', host: 'localhost' } }
        : { network: 'xhttp', security: 'tls', xhttpSettings: {
          path: '/xhttp', host: 'localhost', mode, xmux: { hKeepAlivePeriod: keepalive }
        } };
      streamSettings.tlsSettings = { serverName: 'localhost',
        alpn: mode === 'ws' || http1 ? ['http/1.1'] : ['h2', 'http/1.1'], pinnedPeerCertSha256: pin };
      const config = {
        log: { loglevel: 'warning' },
        inbounds: [{ listen: '127.0.0.1', port: socksPort, protocol: 'socks', settings: { auth: 'noauth', udp: true } }],
        outbounds: [{ protocol: 'vless', settings: { vnext: [{ address: '127.0.0.1', port: edgePort,
          users: [{ id: UUID, encryption: 'none' }] }] }, streamSettings, mux: { enabled: mux, concurrency: 8 } }]
      };
      const configPath = path.join(temp, `xray-${mode}-${http1 ? 'h1' : 'h2'}-${mux}-${keepalive}.json`);
      fs.writeFileSync(configPath, JSON.stringify(config));
      const client = spawn(XRAY, ['run', '-c', configPath], { stdio: ['ignore', 'pipe', 'pipe'] });
      xrayLogs = '';
      client.stdout.on('data', (data) => { xrayLogs = (xrayLogs + data).slice(-30000); });
      client.stderr.on('data', (data) => { xrayLogs = (xrayLogs + data).slice(-30000); });
      try {
        let listening = false;
        for (let i = 0; i < 100; i++) {
          if (client.exitCode !== null) throw new Error(`Xray exited: ${xrayLogs}`);
          try {
            const check = net.connect(socksPort, '127.0.0.1');
            await once(check, 'connect');
            check.destroy();
            listening = true;
            break;
          } catch (_) { await delay(30); }
        }
        assert.ok(listening, `Xray did not start: ${xrayLogs}`);
        return await fn(socksPort);
      } finally {
        if (client.exitCode === null && client.signalCode === null) {
          const exited = once(client, 'exit').catch(() => {});
          client.kill('SIGTERM');
          await Promise.race([exited, delay(1000)]);
          if (client.exitCode === null && client.signalCode === null) client.kill('SIGKILL');
        }
        activeEdge.dropConnections();
      }
    }

    const cases = [
      { mode: 'ws', mux: false }, { mode: 'ws', mux: true },
      { mode: 'packet-up', mux: false }, { mode: 'packet-up', mux: true },
      { mode: 'stream-up', mux: false }, { mode: 'stream-up', mux: true },
      { mode: 'packet-up', mux: false, http1: true }, { mode: 'stream-up', mux: false, http1: true }
    ];
    for (const config of cases) {
      const stats = (config.http1 ? h1Edge : edge).stats;
      const before = { ...stats };
      const started = Date.now();
      await withXray(config, async (port) => {
        await Promise.all(Array.from({ length: 24 }, (_, i) => tcpEcho(port, tcpPort, i === 0 ? 512 * 1024 : 1024)));
        assert.equal(await udpBurst(port, udpPort), 20);
      });
      const used = { h2: stats.h2 - before.h2, h1: stats.h1 - before.h1, ws: stats.ws - before.ws };
      if (config.mode === 'ws') assert.ok(used.ws > 0);
      else if (config.http1) assert.ok(used.h1 > 0 && used.h2 === 0, JSON.stringify(used));
      else assert.ok(used.h2 > 0, JSON.stringify(used));
      console.log(JSON.stringify({ ...config, tcp_flows: 24, udp_datagrams: 20, edge_requests: used, elapsed_ms: Date.now() - started }));
    }

    // Pause a real SOCKS receiver while a large response accumulates, then
    // check byte-exact delivery after it resumes. This is a local pressure
    // scenario, not a throughput or production capacity measurement.
    for (const mode of ['ws', 'packet-up', 'stream-up']) {
      await withXray({ mode }, async (port) => {
        const flow = await socksConnect(port, 'target.test', tcpPort);
        try {
          const data = Buffer.alloc(4 * 1024 * 1024, 0x5d);
          flow.socket.pause();
          flow.socket.write(data);
          await delay(900);
          flow.socket.resume();
          assert.deepEqual(await timeout(flow.reader.read(data.length), 15000, 'slow receiver recovery'), data);
          console.log(JSON.stringify({ scenario: 'paused real-Xray receiver', mode, bytes: data.length, paused_ms: 900 }));
        } finally { flow.socket.destroy(); }
      });
    }

    // One slow target generates many packet POSTs; a second, unrelated tunnel
    // must still work while the first is connecting.
    await withXray({ mode: 'packet-up' }, async (port) => {
      const before = edge.stats.packetPosts;
      const slow = await socksConnect(port, 'slow-connect.test', tcpPort);
      const pieces = Array.from({ length: 12 }, (_, i) => Buffer.from(`chunk-${String(i).padStart(2, '0')}|`));
      try {
        const writer = (async () => { for (const piece of pieces) { slow.socket.write(piece); await delay(65); } })();
        await delay(420);
        const start = Date.now();
        await timeout(tcpEcho(port, tcpPort, 1024), 1500, 'unrelated Xray tunnel during delayed connect');
        const healthyMs = Date.now() - start;
        await writer;
        assert.deepEqual(await timeout(slow.reader.read(Buffer.concat(pieces).length), 6000, 'delayed Xray echo'), Buffer.concat(pieces));
        console.log(JSON.stringify({ scenario: 'real Xray delayed connect', slow_connect_ms: 2200,
          posted_chunks: pieces.length, packet_posts: edge.stats.packetPosts - before, unrelated_echo_ms: healthyMs }));
      } finally { slow.socket.destroy(); }
    });

    // A broken transport cannot keep an existing TCP byte stream alive; check
    // that a subsequent new flow can reconnect rather than making that claim.
    for (const mode of ['ws', 'packet-up']) {
      await withXray({ mode }, async (port) => {
        const flow = await socksConnect(port, 'target.test', tcpPort);
        flow.socket.write(Buffer.from('before'));
        assert.equal((await timeout(flow.reader.read(6), 4000, 'pre-interruption echo')).toString(), 'before');
        const closed = once(flow.socket, 'close');
        edge.dropConnections();
        await timeout(closed, 5000, 'interrupted flow close');
        await delay(100);
        await tcpEcho(port, tcpPort, 1024);
        console.log(JSON.stringify({ scenario: 'edge interruption', mode, existing_flow_closed: true, new_flow_recovered: true }));
      });
    }

    // Artificially add 50 ms in each direction, independently of network ping.
    edge.server.delayMs = 50;
    await withXray({ mode: 'ws' }, async (port) => {
      const flow = await socksConnect(port, 'target.test', tcpPort);
      try {
        const samples = [];
        for (let i = 0; i < 8; i++) {
          const payload = Buffer.from(`ping-${i}`);
          const start = Date.now();
          flow.socket.write(payload);
          assert.deepEqual(await timeout(flow.reader.read(payload.length), 3000, 'delayed echo'), payload);
          samples.push(Date.now() - start);
        }
        assert.ok(Math.min(...samples) >= 80, JSON.stringify(samples));
        console.log(JSON.stringify({ scenario: 'simulated 50ms per direction', rtt_ms: samples }));
      } finally { flow.socket.destroy(); }
    });
    edge.server.delayMs = 0;

    // This deliberately restrictive policy emulates an H2 peer which rejects
    // PING intervals shorter than 10 seconds; it is not a claim about a host.
    edge.server.minimumPingMs = 10000;
    const rejectedBefore = edge.stats.rejectedPings;
    await withXray({ mode: 'packet-up', keepalive: 2 }, async (port) => {
      const flow = await socksConnect(port, 'target.test', tcpPort);
      flow.socket.write(Buffer.from('old'));
      assert.equal((await timeout(flow.reader.read(3), 4000, 'old keepalive first echo')).toString(), 'old');
      await timeout(once(flow.socket, 'close'), 9000, 'strict edge PING rejection');
    });
    assert.ok(edge.stats.rejectedPings > rejectedBefore, 'strict edge did not reject the old PING rate');
    const rejectedAfterOld = edge.stats.rejectedPings;
    await withXray({ mode: 'packet-up', keepalive: generatedKeepalive }, async (port) => {
      const flow = await socksConnect(port, 'target.test', tcpPort);
      try {
        flow.socket.write(Buffer.from('before'));
        assert.equal((await timeout(flow.reader.read(6), 4000, 'new keepalive first echo')).toString(), 'before');
        await delay(6500);
        flow.socket.write(Buffer.from('after'));
        assert.equal((await timeout(flow.reader.read(5), 4000, 'new keepalive later echo')).toString(), 'after');
      } finally { flow.socket.destroy(); }
    });
    assert.equal(edge.stats.rejectedPings, rejectedAfterOld);
    console.log(JSON.stringify({ scenario: 'simulated strict H2 PING policy', old_seconds: 2,
      old_rejected: true, generated_seconds: generatedKeepalive, survived_idle_ms: 6500 }));
    edge.server.minimumPingMs = 0;

    if (SOAK_SECONDS > 0) {
      // Optional sustained flow with ordered per-chunk latency variation and
      // periodic concurrent transfers. Still only a loopback simulation.
      edge.server.delayPattern = [0, 10, 25, 40];
      for (const mode of ['ws', 'packet-up']) {
        await withXray({ mode }, async (port) => {
          const flow = await socksConnect(port, 'target.test', tcpPort);
          const samples = [];
          let burstCount = 0;
          const deadline = Date.now() + SOAK_SECONDS * 1000;
          try {
            while (Date.now() < deadline) {
              const payload = crypto.randomBytes(32);
              const start = Date.now();
              flow.socket.write(payload);
              assert.deepEqual(await timeout(flow.reader.read(payload.length), 4000, 'soak echo'), payload);
              samples.push(Date.now() - start);
              if (samples.length % 10 === 0) { await tcpEcho(port, tcpPort, 128 * 1024); burstCount += 1; }
              await delay(400);
            }
            samples.sort((a, b) => a - b);
            console.log(JSON.stringify({ scenario: 'long-lived flow with simulated jitter', mode,
              seconds: SOAK_SECONDS, probes: samples.length, concurrent_bursts: burstCount,
              p50_ms: samples[Math.floor(samples.length * 0.5)],
              p95_ms: samples[Math.min(samples.length - 1, Math.floor(samples.length * 0.95))],
              max_ms: samples[samples.length - 1] }));
          } finally { flow.socket.destroy(); }
        });
      }
      edge.server.delayPattern = null;
    }
  } catch (err) {
    console.error('XRAY INTEGRATION FAILED:', err.stack || err);
    console.error('Last Xray logs:', xrayLogs);
    console.error('Last panel logs:', panelLogs);
    process.exitCode = 1;
  } finally {
    if (edge) { edge.dropConnections(); edge.server.close(); }
    if (h1Edge) { h1Edge.dropConnections(); h1Edge.server.close(); }
    for (const socket of targetSockets) socket.destroy();
    if (tcpTarget) tcpTarget.close();
    if (udpTarget) udpTarget.close();
    if (panel && panel.exitCode === null && panel.signalCode === null) {
      const exited = once(panel, 'exit').catch(() => {});
      panel.kill('SIGTERM');
      await Promise.race([exited, delay(1000)]);
      if (panel.exitCode === null && panel.signalCode === null) panel.kill('SIGKILL');
    }
    fs.rmSync(temp, { recursive: true, force: true });
  }
}

run().catch((err) => { console.error('Cannot start Xray integration:', err.stack || err); process.exitCode = 1; });
