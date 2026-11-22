'use strict';

// Optional, separately exposed REALITY ingress. This is not a way to hide a
// proxy from the machine hosting it. Only the pinned Xray build tested with
// this bridge is accepted. Existing HTTP/WS/XHTTP listeners remain independent.
const crypto = require('node:crypto');
const fs = require('node:fs');
const net = require('node:net');
const { spawn } = require('node:child_process');

const XRAY_VERSION = '26.3.27';
// Extracted executable from the official Xray-linux-64.zip release whose
// archive SHA-256 is 23cd9af937744d97776ee35ecad4972cf4b2109d1e0fe6be9930467608f7c8ae.
const XRAY_SHA256 = '8255dd939c34cf966cc91517b6324dd3c8d0bcf49ffac8beca049a38c46845ed';
const MIB = 1024 * 1024;
const PROXY_HEADER_MAX = 107;
const HANDSHAKE_TIMEOUT_MS = 30000;
const STARTUP_TIMEOUT_MS = 10000;

function fail(code) {
  const error = new Error(code);
  error.code = code;
  throw error;
}

function portValue(value, code) {
  const raw = String(value || '');
  if (!/^\d{1,5}$/.test(raw)) fail(code);
  const n = Number(raw);
  if (n < 1 || n > 65535) fail(code);
  return n;
}

function hostname(value, localAllowed = false) {
  const name = String(value || '').trim().toLowerCase();
  if (localAllowed && name === 'localhost') return name;
  if (name.length > 253 || name.includes(':') || net.isIP(name)) return '';
  const parts = name.split('.');
  if (parts.length < 2 || parts.some((part) => !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(part))) return '';
  if (parts[parts.length - 1] === 'localhost' || parts[parts.length - 1] === 'local') return '';
  return name;
}

function publicHost(value, localAllowed = false) {
  let host = String(value || '').trim().toLowerCase();
  if (host.startsWith('[') && host.endsWith(']')) host = host.slice(1, -1);
  return net.isIP(host) ? host : hostname(host, localAllowed);
}

function derivePublicKey(privateKey) {
  const text = String(privateKey || '');
  if (!/^[A-Za-z0-9_-]{43}$/.test(text)) fail('invalid_private_key');
  const raw = Buffer.from(text, 'base64url');
  if (raw.length !== 32 || raw.toString('base64url') !== text) fail('invalid_private_key');
  try {
    const prefix = Buffer.from('302e020100300506032b656e04220420', 'hex');
    const key = crypto.createPrivateKey({ key: Buffer.concat([prefix, raw]), type: 'pkcs8', format: 'der' });
    return crypto.createPublicKey(key).export({ format: 'jwk' }).x;
  } catch (_) {
    fail('invalid_private_key');
  }
}

function readConfig(env, httpPort) {
  // This acknowledgement is required because TCP proxies can replace the
  // client address. PROXY protocol inside this bridge cannot restore it.
  if (env.REALITY_ACK_DIRECT_SOURCE_IP !== '1') fail('source_ip_not_acknowledged');
  const port = portValue(env.REALITY_PORT, 'invalid_listen_port');
  if (port === httpPort) fail('port_conflicts_with_http');
  const externalPort = portValue(env.REALITY_PUBLIC_PORT, 'invalid_public_port');
  const allowLocalTarget = env.REALITY_ALLOW_LOCAL_TARGET === '1';
  const serverName = hostname(env.REALITY_SNI, allowLocalTarget);
  const externalHost = publicHost(env.REALITY_PUBLIC_HOST, env.REALITY_ALLOW_PRIVATE_SOURCE === '1');
  if (!serverName) fail('invalid_server_name');
  if (!externalHost) fail('invalid_public_host');
  const targetPort = env.REALITY_TARGET_PORT ? portValue(env.REALITY_TARGET_PORT, 'invalid_target_port') : 443;
  const shortId = String(env.REALITY_SHORT_ID || '').toLowerCase();
  if (!/^[0-9a-f]{16}$/.test(shortId)) fail('invalid_short_id');
  const privateKey = String(env.REALITY_PRIVATE_KEY || '');
  const publicKey = derivePublicKey(privateKey);
  const binary = String(env.XRAY_BIN || '/usr/local/bin/xray');
  if (!binary.startsWith('/')) fail('xray_path_not_absolute');
  const maxRaw = String(env.REALITY_MAX_CONNECTIONS || '256');
  if (!/^\d{1,4}$/.test(maxRaw) || Number(maxRaw) < 1 || Number(maxRaw) > 2048) fail('invalid_connection_cap');
  return {
    port, externalPort, externalHost, serverName, targetPort, privateKey, publicKey, shortId, binary,
    allowPrivateSource: env.REALITY_ALLOW_PRIVATE_SOURCE === '1', maxConnections: Number(maxRaw)
  };
}

function formatUuid(uuid) {
  const hex = String(uuid || '').replace(/-/g, '').toLowerCase();
  if (!/^[0-9a-f]{32}$/.test(hex)) return '';
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function makeLink(config, user) {
  const uuid = formatUuid(user && user.uuid);
  if (!uuid) return '';
  const host = net.isIP(config.externalHost) === 6 ? `[${config.externalHost}]` : config.externalHost;
  const params = new URLSearchParams({
    encryption: 'none', security: 'reality', type: 'tcp', sni: config.serverName,
    fp: 'chrome', pbk: config.publicKey, sid: config.shortId, spx: '/'
  });
  return `vless://${uuid}@${host}:${config.externalPort}?${params}#${encodeURIComponent((user.remark || 'Mahan') + '-REALITY')}`;
}

async function verifyBinary(binary) {
  if (process.platform !== 'linux' || process.arch !== 'x64') fail('unsupported_platform');
  try { await fs.promises.access(binary, fs.constants.R_OK | fs.constants.X_OK); }
  catch (_) { fail('xray_binary_unavailable'); }
  const hash = crypto.createHash('sha256');
  try {
    const input = fs.createReadStream(binary);
    for await (const chunk of input) hash.update(chunk);
  } catch (_) { fail('xray_binary_unavailable'); }
  if (hash.digest('hex') !== XRAY_SHA256) fail('xray_binary_mismatch');
}

function listen(server, port, host) {
  return new Promise((resolve, reject) => {
    const onError = (err) => { server.removeListener('listening', onListen); reject(err); };
    const onListen = () => { server.removeListener('error', onError); resolve(); };
    server.once('error', onError);
    server.once('listening', onListen);
    server.listen(port, host);
  });
}

async function availableLoopbackPort() {
  const server = net.createServer();
  await listen(server, 0, '127.0.0.1');
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

function normalizedIp(value) {
  return String(value || '').replace(/^::ffff:/i, '').split('%')[0];
}

// Reject common non-public source ranges by default, including alternate
// IPv6 spellings and IPv4-mapped IPv6. This cannot prove that a public source
// is not a proxy; the operator must verify it.
const nonPublicSources = new net.BlockList();
for (const [address, prefix] of [
  ['0.0.0.0', 8], ['10.0.0.0', 8], ['100.64.0.0', 10], ['127.0.0.0', 8],
  ['169.254.0.0', 16], ['172.16.0.0', 12], ['192.0.0.0', 24], ['192.0.2.0', 24],
  ['192.168.0.0', 16], ['198.18.0.0', 15], ['198.51.100.0', 24], ['203.0.113.0', 24],
  ['224.0.0.0', 4], ['240.0.0.0', 4]
]) nonPublicSources.addSubnet(address, prefix, 'ipv4');
for (const [address, prefix] of [
  ['::', 128], ['::1', 128], ['fc00::', 7], ['fe80::', 10], ['ff00::', 8], ['2001:db8::', 32]
]) nonPublicSources.addSubnet(address, prefix, 'ipv6');

function privateSource(value) {
  const ip = normalizedIp(value).toLowerCase();
  const family = net.isIP(ip);
  return !family || nonPublicSources.check(ip, family === 4 ? 'ipv4' : 'ipv6');
}

class RealityBridge {
  constructor({ env = process.env, httpPort, guard, memoryLimitMb, onTunnel, log }) {
    this.env = env;
    this.httpPort = httpPort;
    this.guard = guard;
    this.memoryLimitMb = memoryLimitMb;
    this.onTunnel = onTunnel;
    this.log = log;
    this.requested = env.REALITY_ENABLED === '1';
    this.status = this.requested ? 'starting' : 'disabled';
    this.reason = null;
    this.config = null;
    this.child = null;
    this.front = null;
    this.backend = null;
    this.stopping = false;
    this.entries = new Map();
    this.backendSockets = new Set();
    this.metrics = { accepted: 0, rejected_memory: 0, rejected_source: 0, rejected_capacity: 0, rejected_bridge: 0 };
  }

  health() {
    return { enabled: this.requested, status: this.status, reason: this.reason,
      ingress_active: this.entries.size, ...this.metrics };
  }

  link(user) {
    return this.status === 'ready' && this.config ? makeLink(this.config, user) : '';
  }

  async start() {
    if (!this.requested || this.stopping) return;
    try {
      const config = readConfig(this.env, this.httpPort);
      if (!this.guard.enabled || !this.guard.hasLimit() || !/^cgroup-v[12]$/.test(this.guard.usageSource)) {
        fail('cgroup_memory_guard_required');
      }
      if (this.memoryLimitMb && this.memoryLimitMb < 256) fail('memory_limit_below_256mib');
      if (!this.guard.admit(32 * MIB, false)) fail('insufficient_memory_headroom');
      await verifyBinary(config.binary);
      if (this.stopping) return;
      this.config = config;

      // Only Xray's fixed loopback outbound reaches this listener. The random,
      // single-use token below additionally ties each accepted backend to an
      // actual ingress connection; loopback alone is not an authentication.
      this.backend = net.createServer({ pauseOnConnect: true }, (socket) => this.acceptBackend(socket));
      this.backend.maxConnections = config.maxConnections + 32;
      await listen(this.backend, 0, '127.0.0.1');
      this.backend.on('error', () => this.unavailable('backend_listener_failed'));
      if (this.stopping) return;
      const bridgePort = this.backend.address().port;
      const xrayPort = await availableLoopbackPort();
      if (this.stopping) return;
      const xrayConfig = {
        log: { loglevel: 'warning', access: 'none' },
        // Xray's default is a five-minute idle timeout. Keep established quiet
        // flows for much longer, and bound its per-connection pipe buffer.
        policy: { levels: { 0: { connIdle: 31536000, bufferSize: 256 } } },
        inbounds: [{
          listen: '127.0.0.1', port: xrayPort, protocol: 'dokodemo-door',
          settings: { address: '127.0.0.1', port: bridgePort, network: 'tcp' },
          streamSettings: {
            network: 'raw', security: 'reality',
            realitySettings: { target: `${config.serverName}:${config.targetPort}`,
              serverNames: [config.serverName], privateKey: config.privateKey, shortIds: [config.shortId] },
            sockopt: { acceptProxyProtocol: true }
          }
        }],
        outbounds: [{ protocol: 'freedom', settings: { proxyProtocol: 1 } }]
      };
      await this.startXray(config.binary, xrayConfig);
      if (this.stopping || this.status === 'failed') return;
      // Xray's pages and sockets are included in cgroup accounting. Do not
      // open the new public port if starting it already pushed us into hard pressure.
      this.guard.tick(true);
      if (this.guard.isHard()) fail('insufficient_memory_headroom');
      this.front = net.createServer({ pauseOnConnect: true }, (socket) => this.acceptFront(socket, xrayPort));
      this.front.maxConnections = config.maxConnections;
      // Node drops sockets at maxConnections before acceptFront is called.
      this.front.on('drop', () => { this.metrics.rejected_capacity += 1; });
      try { await listen(this.front, config.port, '0.0.0.0'); }
      catch (_) { fail('listen_port_unavailable'); }
      this.front.on('error', () => this.unavailable('frontend_listener_failed'));
      if (this.stopping || this.status === 'failed') return;
      this.status = 'ready';
      this.reason = null;
      this.log('[REALITY] Optional raw TCP ingress ready. Verify client source IPs; the host can still observe the service.');
    } catch (err) {
      if (!this.stopping) this.unavailable(err && err.code ? err.code : 'startup_failed');
    }
  }

  startXray(binary, config) {
    return new Promise((resolve, reject) => {
      let settled = false;
      let output = '';
      let timer;
      const finish = (err) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (err) reject(err); else resolve();
      };
      // Do not pass unrelated application credentials to the child. Both
      // names are set because Xray checks the dotted name before the normalized
      // one. Freedom's Linux splice otherwise bypasses REALITY on the return
      // path for this dokodemo configuration in the pinned Xray release.
      const child = spawn(binary, ['run', '-config', 'stdin:'], {
        env: { PATH: this.env.PATH || '/usr/bin:/bin', XRAY_BUF_SPLICE: 'disable', 'xray.buf.splice': 'disable' },
        stdio: ['pipe', 'pipe', 'pipe']
      });
      this.child = child;
      const scan = (chunk) => {
        output = (output + chunk.toString('utf8')).slice(-4096);
        if (output.includes(`core: Xray ${XRAY_VERSION} started`)) finish();
      };
      child.stdout.on('data', scan);
      child.stderr.on('data', scan);
      child.stdin.on('error', () => {});
      child.once('error', () => {
        finish(Object.assign(new Error('xray_start_failed'), { code: 'xray_start_failed' }));
        if (!this.stopping) this.unavailable('xray_start_failed');
      });
      child.once('exit', () => {
        finish(Object.assign(new Error('xray_exited'), { code: 'xray_exited' }));
        if (!this.stopping) this.unavailable('xray_exited');
      });
      timer = setTimeout(() => finish(Object.assign(new Error('xray_start_timeout'), { code: 'xray_start_timeout' })), STARTUP_TIMEOUT_MS);
      child.stdin.end(JSON.stringify(config));
    });
  }

  acceptFront(socket, xrayPort) {
    socket.on('error', () => {});
    const config = this.config;
    if (this.status !== 'ready' || !config) return socket.destroy();
    const actualIp = normalizedIp(socket.remoteAddress);
    if (!net.isIP(actualIp) || (!config.allowPrivateSource && privateSource(actualIp))) {
      this.metrics.rejected_source += 1;
      return socket.destroy();
    }
    if (this.entries.size >= config.maxConnections) {
      this.metrics.rejected_capacity += 1;
      return socket.destroy();
    }
    if (!this.guard.admit(2 * MIB, false)) {
      this.metrics.rejected_memory += 1;
      this.guard.recordAdmissionRejection();
      return socket.destroy();
    }

    // 48-bit, per-connection single-use cookie encoded in a PROXY IPv4/port
    // pair. It is not the user's IP. On return, only a matching live ingress
    // receives a VLESS handler, which uses the saved actual socket peer IP.
    let token, ip, port;
    do {
      const random = crypto.randomBytes(6);
      ip = Array.from(random.subarray(0, 4)).join('.');
      port = random.readUInt16BE(4);
      token = `${ip}:${port}`;
    } while (!port || this.entries.has(token));
    const entry = { socket, actualIp, token, claimed: false, backend: null, inner: null, timer: null };
    try { socket.setKeepAlive(true, 30000); socket.setNoDelay(true); } catch (_) {}
    this.entries.set(token, entry);
    this.metrics.accepted += 1;
    entry.timer = setTimeout(() => { if (!entry.claimed) socket.destroy(); }, HANDSHAKE_TIMEOUT_MS);
    entry.timer.unref?.();
    const inner = net.createConnection({ host: '127.0.0.1', port: xrayPort });
    entry.inner = inner;
    const cleanup = () => {
      clearTimeout(entry.timer);
      if (this.entries.get(token) === entry) this.entries.delete(token);
      if (entry.backend && !entry.backend.destroyed) entry.backend.destroy();
      if (!inner.destroyed) inner.destroy();
    };
    socket.once('close', cleanup);
    inner.on('error', () => socket.destroy());
    inner.once('close', () => socket.destroy());
    inner.once('connect', () => {
      if (socket.destroyed || this.status !== 'ready') return socket.destroy();
      inner.write(`PROXY TCP4 ${ip} 127.0.0.1 ${port} ${xrayPort}\r\n`);
      socket.pipe(inner);
      inner.pipe(socket);
      socket.resume();
    });
  }

  acceptBackend(socket) {
    this.backendSockets.add(socket);
    socket.on('error', () => {});
    socket.once('close', () => this.backendSockets.delete(socket));
    if (normalizedIp(socket.remoteAddress) !== '127.0.0.1' || this.status !== 'ready') return socket.destroy();
    socket.setTimeout(5000, () => socket.destroy());
    let header = Buffer.alloc(0);
    const reject = () => { this.metrics.rejected_bridge += 1; socket.destroy(); };
    const onData = (chunk) => {
      const data = header.length ? Buffer.concat([header, chunk]) : chunk;
      const end = data.indexOf('\r\n');
      if (end < 0) {
        if (data.length > PROXY_HEADER_MAX) return reject();
        header = Buffer.from(data);
        return;
      }
      if (end + 2 > PROXY_HEADER_MAX) return reject();
      const line = data.subarray(0, end).toString('latin1');
      const match = /^PROXY TCP4 (\d{1,3}(?:\.\d{1,3}){3}) 127\.0\.0\.1 (\d{1,5}) (\d{1,5})$/.exec(line);
      const backendAddress = this.backend && this.backend.address();
      if (!match || !backendAddress || net.isIP(match[1]) !== 4 || Number(match[3]) !== backendAddress.port) return reject();
      const entry = this.entries.get(`${match[1]}:${Number(match[2])}`);
      if (!entry || entry.claimed || entry.socket.destroyed) return reject();
      entry.claimed = true;
      entry.backend = socket;
      clearTimeout(entry.timer);
      socket.pause();
      socket.setTimeout(0);
      socket.removeListener('data', onData);
      try { this.onTunnel(socket, entry.actualIp, data.subarray(end + 2)); }
      catch (_) { socket.destroy(); entry.socket.destroy(); }
    };
    socket.on('data', onData);
    socket.resume();
  }

  unavailable(reason) {
    if (this.stopping || this.status === 'failed') return;
    this.status = 'failed';
    this.reason = String(reason || 'startup_failed');
    this.log(`[REALITY] Unavailable (${this.reason}); existing WS/XHTTP remain independent.`);
    this.closeResources();
  }

  closeResources() {
    try { this.front?.close(); } catch (_) {}
    try { this.backend?.close(); } catch (_) {}
    for (const entry of this.entries.values()) {
      clearTimeout(entry.timer);
      entry.socket.destroy();
      entry.inner?.destroy();
      entry.backend?.destroy();
    }
    this.entries.clear();
    for (const socket of this.backendSockets) socket.destroy();
    this.backendSockets.clear();
    if (this.child && this.child.exitCode === null && this.child.signalCode === null) {
      try { this.child.kill('SIGTERM'); } catch (_) {}
      const child = this.child;
      setTimeout(() => {
        if (child.exitCode === null && child.signalCode === null) { try { child.kill('SIGKILL'); } catch (_) {} }
      }, 1000).unref();
    }
  }

  stop() {
    this.stopping = true;
    if (this.requested) this.status = 'stopped';
    this.closeResources();
  }
}

module.exports = { RealityBridge, readConfig, makeLink, privateSource, XRAY_VERSION, XRAY_SHA256 };
