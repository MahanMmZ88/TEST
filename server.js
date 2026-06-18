'use strict';

const http = require('http');
const net = require('net');
const dgram = require('dgram');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const express = require('express');
const cookieParser = require('cookie-parser');
const QRCode = require('qrcode');
const { WebSocketServer } = require('ws');

const app = express();
app.set('trust proxy', true);

const PORT = Number(process.env.PORT || 3000); // Railway/Koyeb/Render provide PORT automatically.
const WS_PATH = '/ws';
const XHTTP_PATH = '/xhttp';
const DB_PATH = path.join(__dirname, 'db.json');

const PASSWORD_SALT = 'MAHAN_PANEL_NODE_WS_SALT_v2';
const CONNECT_TIMEOUT_MS = 15000;
const IDLE_TIMEOUT_MS = 120000;
const MAX_FIRST_PACKET = 128 * 1024;
const MAX_WS_PAYLOAD = 16 * 1024 * 1024;
const WS_BACKPRESSURE_LIMIT = 8 * 1024 * 1024;
const WS_BACKPRESSURE_RESUME = 2 * 1024 * 1024;
const MAX_UDP_PACKET_SIZE = 64 * 1024;
const UDP_IDLE_TIMEOUT_MS = 60000;
const SELF_PING_INTERVAL_MS = 5 * 60 * 1000;
const MAX_XHTTP_POST_SIZE = 2 * 1024 * 1024;
const MAX_XHTTP_BUFFERED_POSTS = 64;
const MAX_XHTTP_DOWNLINK_BUFFER = 8 * 1024 * 1024;
const XHTTP_SESSION_TIMEOUT_MS = 300000;
const XHTTP_RESPONSE_HEADERS = {
  'content-type': 'application/octet-stream',
  'cache-control': 'no-store',
  'x-accel-buffering': 'no',
  'access-control-allow-origin': '*',
  'access-control-allow-methods': 'GET, POST, OPTIONS'
};
const DB_SAVE_DEBOUNCE_MS = 1500;
const LOGIN_MAX_ATTEMPTS = 5;
const LOGIN_WINDOW_MS = 10 * 60 * 1000;
const LOGIN_LOCK_MS = 15 * 60 * 1000;

let DB = { admin: null, users: [] };
let dbSaveTimer = null;
let lastCpuUsage = process.cpuUsage();
let lastCpuCheck = process.hrtime.bigint();
let trafficCurrentMinuteBytes = 0;
let trafficHistory = [];
let selfPingHealthUrl = '';
let lastSelfPingUrlLog = '';
const activeSessions = new Set();
const loginAttempts = new Map(); // ip -> { count, firstAt, lockedUntil }
const activeConnections = new Map(); // user.id -> Set(closeFn)

app.use(express.json({ limit: '256kb' }));
app.use(cookieParser());

function rememberPublicSelfUrl(req) {
  const host = String(req.headers.host || '').trim();
  if (!host) return;

  const forwardedProto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
  const proto = forwardedProto || (req.secure ? 'https' : req.protocol || 'http');
  if (proto !== 'http' && proto !== 'https') return;

  const nextUrl = `${proto}://${host}/health`;
  if (selfPingHealthUrl !== nextUrl) {
    selfPingHealthUrl = nextUrl;
  }
}

app.use((req, res, next) => {
  rememberPublicSelfUrl(req);
  next();
});

app.use(express.static(path.join(__dirname, 'public')));

function log(message, extra = '') {
  const suffix = extra ? ` ${extra}` : '';
  console.log(`[${new Date().toISOString()}] ${message}${suffix}`);
}

function generateHash(password) {
  return crypto.createHash('sha256').update(String(password) + PASSWORD_SALT).digest('hex');
}

function getTrafficChart() {
  const now = Date.now();
  const points = trafficHistory.slice(-59).concat([{ ts: now, bytes: trafficCurrentMinuteBytes }]);
  const padded = [];
  const missing = Math.max(0, 60 - points.length);

  for (let i = missing; i > 0; i--) {
    padded.push({ ts: now - i * 60 * 1000, bytes: 0 });
  }
  padded.push(...points);

  return {
    labels: padded.map((p) => new Intl.DateTimeFormat('en-US', { timeZone: 'Asia/Tehran', hour: '2-digit', minute: '2-digit', hour12: false }).format(new Date(p.ts))),
    values: padded.map((p) => Number((p.bytes / 1024 / 1024).toFixed(3)))
  };
}

function getCpuUsagePercent() {
  const currentUsage = process.cpuUsage();
  const currentTime = process.hrtime.bigint();
  const elapsedMicros = Number(currentTime - lastCpuCheck) / 1000;
  const usedMicros = (currentUsage.user - lastCpuUsage.user) + (currentUsage.system - lastCpuUsage.system);

  lastCpuUsage = currentUsage;
  lastCpuCheck = currentTime;

  if (!elapsedMicros || elapsedMicros <= 0) return 0;
  const percent = Math.max(0, Math.min(100, (usedMicros / elapsedMicros) * 100));
  return Number(percent.toFixed(1));
}

function formatBytes(bytes) {
  if (!bytes || bytes <= 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.min(sizes.length - 1, Math.floor(Math.log(bytes) / Math.log(k)));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

function calcPercent(used, limit) {
  if (!limit || limit <= 0) return 0;
  return Math.min(100, (used / limit) * 100);
}

function formatTehranDate(timestamp) {
  if (!timestamp || timestamp <= 0) return 'Never';
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Tehran',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  }).formatToParts(new Date(timestamp));

  const get = (type) => (parts.find((p) => p.type === type) || {}).value || '00';
  return `${get('year')}-${get('month')}-${get('day')} ${get('hour')}:${get('minute')}`;
}

function safeName(value) {
  return String(value || '').replace(/[<>"'&]/g, (c) => ({ '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;', '&': '&amp;' }[c]));
}

function normalizeUser(u) {
  if (!u || typeof u !== 'object') return null;
  return {
    id: String(u.id || crypto.randomUUID()),
    remark: String(u.remark || 'user'),
    uuid: String(u.uuid || crypto.randomUUID()).toLowerCase(),
    sub_id: String(u.sub_id || crypto.randomBytes(8).toString('hex')),
    traffic_limit: Number(u.traffic_limit || 0),
    used_bytes: Number(u.used_bytes || 0),
    expiry_time: Number(u.expiry_time || 0),
    last_seen: Number(u.last_seen || 0),
    is_active: Number(u.is_active === 0 ? 0 : 1),
    protocol: 'ws+xhttp',
    created_on: String(u.created_on || 'other'),
    created_at: String(u.created_at || new Date().toISOString())
  };
}

function loadDB() {
  try {
    if (fs.existsSync(DB_PATH)) {
      const parsed = JSON.parse(fs.readFileSync(DB_PATH, 'utf8'));
      DB.admin = parsed.admin || null;
      DB.users = Array.isArray(parsed.users) ? parsed.users.map(normalizeUser).filter(Boolean) : [];
    }
  } catch (err) {
    log('[DB] Load failed:', err.message);
  }

  if (!DB.admin) {
    DB.admin = { username: 'admin', password_hash: generateHash('admin') };
  }
  if (!Array.isArray(DB.users)) DB.users = [];

  scheduleSaveDB(true);
  log('[DB] Loaded users:', String(DB.users.length));
}

function saveDBNow() {
  if (dbSaveTimer) clearTimeout(dbSaveTimer);
  dbSaveTimer = null;
  const tmp = DB_PATH + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(DB, null, 2));
  fs.renameSync(tmp, DB_PATH);
}

function scheduleSaveDB(immediate = false) {
  if (immediate) {
    try { saveDBNow(); } catch (err) { log('[DB] Save failed:', err.message); }
    return;
  }
  if (dbSaveTimer) return;
  dbSaveTimer = setTimeout(() => {
    try { saveDBNow(); } catch (err) { log('[DB] Save failed:', err.message); }
  }, DB_SAVE_DEBOUNCE_MS);
}

function isUserExpired(user) {
  if (!user) return true;
  const now = Date.now();
  const trafficExceeded = user.traffic_limit > 0 && user.used_bytes >= user.traffic_limit;
  const timeExceeded = user.expiry_time > 0 && now > user.expiry_time;
  return trafficExceeded || timeExceeded;
}

function getActiveUserByUuid(uuidBytes) {
  const uuidHex = uuidBytes.toString('hex');
  const user = DB.users.find((u) => String(u.uuid).replace(/-/g, '').toLowerCase() === uuidHex);
  if (!user) return null;
  if (user.is_active !== 1 || isUserExpired(user)) return null;
  return user;
}

function registerConnection(userId, closeFn) {
  if (!activeConnections.has(userId)) activeConnections.set(userId, new Set());
  activeConnections.get(userId).add(closeFn);
  return () => {
    const set = activeConnections.get(userId);
    if (!set) return;
    set.delete(closeFn);
    if (set.size === 0) activeConnections.delete(userId);
  };
}

function closeUserConnections(userId, reason = 'user disabled') {
  const set = activeConnections.get(userId);
  if (!set) return;
  for (const closeFn of Array.from(set)) {
    try { closeFn(reason); } catch (_) {}
  }
}

function updateTraffic(user, bytes) {
  if (!user || !bytes || bytes <= 0) return;
  trafficCurrentMinuteBytes += bytes;
  user.used_bytes = Number(user.used_bytes || 0) + bytes;
  user.last_seen = Date.now();
  scheduleSaveDB(false);

  if (user.is_active === 1 && isUserExpired(user)) {
    user.is_active = 0;
    scheduleSaveDB(false);
    log('[QUOTA] Limit reached:', `${user.remark} total=${formatBytes(user.used_bytes)}`);
    closeUserConnections(user.id, 'quota exceeded');
  }
}

function uuidToBytes(uuid) {
  const hex = String(uuid || '').replace(/-/g, '').toLowerCase();
  if (!/^[0-9a-f]{32}$/.test(hex)) return null;
  return Buffer.from(hex, 'hex');
}

function safeCloseWebSocket(ws, code = 1000, reason = '') {
  if (ws.readyState === ws.OPEN || ws.readyState === ws.CONNECTING) {
    try { ws.close(code, reason); } catch (_) {}
  }
}

function destroySocket(socket) {
  if (socket && !socket.destroyed) {
    try { socket.destroy(); } catch (_) {}
  }
}

function closeUdpSocket(socket) {
  if (socket) {
    try { socket.close(); } catch (_) {}
  }
}

function isForbiddenTarget(host, port) {
  const h = String(host || '').toLowerCase();
  if (!h || port <= 0 || port > 65535) return true;
  if (h === 'localhost' || h.endsWith('.localhost')) return true;
  if (h === '0.0.0.0' || h === '::' || h === '::1') return true;
  if (/^127\./.test(h)) return true;
  if (/^10\./.test(h)) return true;
  if (/^192\.168\./.test(h)) return true;
  if (/^172\.(1[6-9]|2\d|3[0-1])\./.test(h)) return true;
  if (/^169\.254\./.test(h)) return true;
  return false;
}

function parseVlessRequest(buffer, options = {}) {
  const allowUdp = options.allowUdp === true;
  if (!Buffer.isBuffer(buffer)) buffer = Buffer.from(buffer);
  if (buffer.length < 24) throw new Error(`request too short: ${buffer.length}`);
  if (buffer.length > MAX_FIRST_PACKET) throw new Error(`first packet too large: ${buffer.length}`);

  let offset = 0;
  const version = buffer[offset++];
  const uuidBytes = buffer.subarray(offset, offset + 16);
  const user = getActiveUserByUuid(uuidBytes);
  if (!user) throw new Error('invalid, inactive, or expired uuid');
  offset += 16;

  const optLen = buffer[offset++];
  if (buffer.length < offset + optLen + 4) throw new Error('invalid options length');
  offset += optLen;

  const command = buffer[offset++];
  if (command !== 0x01 && command !== 0x02) {
    if (command === 0x03) throw new Error('MUX is not supported; disable MUX in client');
    throw new Error(`unsupported command: ${command}`);
  }
  if (command === 0x02 && !allowUdp) {
    throw new Error('UDP is not supported on this transport yet; use WebSocket UDP or disable UDP/xudp in client');
  }

  const port = buffer.readUInt16BE(offset);
  offset += 2;
  const addressType = buffer[offset++];
  let host;

  if (addressType === 0x01) {
    if (buffer.length < offset + 4) throw new Error('invalid ipv4 address');
    host = `${buffer[offset]}.${buffer[offset + 1]}.${buffer[offset + 2]}.${buffer[offset + 3]}`;
    offset += 4;
  } else if (addressType === 0x02) {
    if (buffer.length < offset + 1) throw new Error('invalid domain length');
    const len = buffer[offset++];
    if (len === 0 || buffer.length < offset + len) throw new Error('invalid domain');
    host = buffer.subarray(offset, offset + len).toString('utf8');
    offset += len;
  } else if (addressType === 0x03) {
    if (buffer.length < offset + 16) throw new Error('invalid ipv6 address');
    const parts = [];
    for (let i = 0; i < 8; i++) parts.push(buffer.readUInt16BE(offset + i * 2).toString(16));
    host = parts.join(':');
    offset += 16;
  } else {
    throw new Error(`unsupported address type: ${addressType}`);
  }

  if (isForbiddenTarget(host, port)) throw new Error(`forbidden target: ${host}:${port}`);

  return { version, user, command, host, port, payload: buffer.subarray(offset) };
}

function encodeUdpPacket(packet) {
  if (!Buffer.isBuffer(packet)) packet = Buffer.from(packet);
  if (packet.length > 0xffff) throw new Error('udp packet too large');
  const len = Buffer.allocUnsafe(2);
  len.writeUInt16BE(packet.length, 0);
  return Buffer.concat([len, packet]);
}

function drainUdpPackets(state, chunk, onPacket) {
  if (!chunk || chunk.length === 0) return;
  state.buffer = state.buffer && state.buffer.length ? Buffer.concat([state.buffer, chunk]) : Buffer.from(chunk);

  while (state.buffer.length >= 2) {
    const packetLen = state.buffer.readUInt16BE(0);
    if (packetLen <= 0 || packetLen > MAX_UDP_PACKET_SIZE) throw new Error('invalid udp packet size');
    if (state.buffer.length < 2 + packetLen) break;
    const packet = state.buffer.subarray(2, 2 + packetLen);
    state.buffer = state.buffer.subarray(2 + packetLen);
    onPacket(packet);
  }
}

function decodeEarlyDataFromProtocolHeader(header) {
  if (!header || typeof header !== 'string') return null;
  const token = header.split(',').map((x) => x.trim()).find(Boolean);
  if (!token) return null;
  if (!/^[A-Za-z0-9_-]+={0,2}$/.test(token) || token.length < 16) return null;
  try {
    const normalized = token.replace(/-/g, '+').replace(/_/g, '/');
    const padded = normalized + '='.repeat((4 - (normalized.length % 4)) % 4);
    const buf = Buffer.from(padded, 'base64');
    return buf.length ? buf : null;
  } catch (_) {
    return null;
  }
}

function getPublicEndpoint(req) {
  const rawHost = String(req.get('host') || '').trim();
  const forwardedProto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
  const proto = forwardedProto || req.protocol || 'https';
  const isTls = proto === 'https';

  // Railway/custom domains are usually domain.com. Local tests may be 127.0.0.1:3000.
  const host = rawHost.includes(':') && !rawHost.startsWith('[')
    ? rawHost.split(':')[0]
    : rawHost.replace(/^\[/, '').replace(/\](:\d+)?$/, '');
  const explicitPort = rawHost.includes(':') && !rawHost.startsWith('[') ? rawHost.split(':').pop() : '';
  const port = isTls ? '443' : (explicitPort || '80');
  const security = isTls ? 'tls' : 'none';

  return { host, port, security, isTls };
}

function buildWsVlessLink(req, user) {
  const { host, port, security, isTls } = getPublicEndpoint(req);
  const tag = encodeURIComponent((user.remark || 'Mahan') + '-WS');
  let link = `vless://${user.uuid}@${host}:${port}?encryption=none&security=${security}&type=ws&host=${encodeURIComponent(host)}&path=%2Fws`;
  if (isTls) link += `&sni=${encodeURIComponent(host)}`;
  link += `#${tag}`;
  return link;
}

function buildXhttpVlessLink(req, user) {
  const { host, port, security, isTls } = getPublicEndpoint(req);
  const tag = encodeURIComponent((user.remark || 'Mahan') + '-XHTTP');
  let link = `vless://${user.uuid}@${host}:${port}?encryption=none&security=${security}&type=xhttp&host=${encodeURIComponent(host)}&path=%2Fxhttp&mode=packet-up`;
  if (isTls) link += `&sni=${encodeURIComponent(host)}&alpn=h2%2Chttp%2F1.1&fp=chrome`;
  link += `#${tag}`;
  return link;
}

function buildVlessLink(req, user) {
  return buildWsVlessLink(req, user);
}

function buildSubscriptionLinks(req, user) {
  return [buildWsVlessLink(req, user), buildXhttpVlessLink(req, user)];
}

function getPublicBaseUrl(req) {
  const rawHost = String(req.get('host') || '').trim();
  const forwardedProto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
  const proto = forwardedProto || req.protocol || 'https';
  return `${proto}://${rawHost}`;
}

function buildSubUrl(req, user) {
  return `${getPublicBaseUrl(req)}/sub/${user.sub_id}`;
}

function getQrPayload(req, user, type) {
  if (type === 'ws') return buildWsVlessLink(req, user);
  if (type === 'xhttp') return buildXhttpVlessLink(req, user);
  if (type === 'sub') return buildSubUrl(req, user);
  return '';
}

async function sendQrSvg(res, payload) {
  if (!payload) {
    res.status(400).type('text/plain').send('bad qr type');
    return;
  }
  const svg = await QRCode.toString(payload, {
    type: 'svg',
    errorCorrectionLevel: 'M',
    margin: 1,
    color: { dark: '#111111', light: '#ffffff' },
    width: 320
  });
  res.setHeader('Content-Type', 'image/svg+xml; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.send(svg);
}

function readRequestBody(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > limit) {
        reject(new Error('request body too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
    req.on('aborted', () => reject(new Error('request aborted')));
  });
}

const xhttpSessions = new Map();

class XHttpSession {
  constructor(sessionId) {
    this.sessionId = sessionId;
    this.user = null;
    this.remote = null;
    this.udpSocket = null;
    this.unregisterConnection = null;
    this.closed = false;
    this.established = false;
    this.responseHeaderSent = false;
    this.udpMode = false;
    this.udpTargetHost = '';
    this.udpTargetPort = 0;
    this.udpState = { buffer: Buffer.alloc(0) };
    this.udpIdleTimer = null;
    this.version = 0;
    this.downlinkRes = null;
    this.nextSeq = 0;
    this.pendingPackets = new Map();
    this.pendingRemoteWrites = [];
    this.pendingDownlinkChunks = [];
    this.pendingDownlinkBytes = 0;
    this.lastActivity = Date.now();
    this.connectTimer = null;
    this.cleanupTimer = setTimeout(() => this.close('session timeout'), XHTTP_SESSION_TIMEOUT_MS);
  }

  refresh() {
    this.lastActivity = Date.now();
    if (this.cleanupTimer && this.cleanupTimer.refresh) this.cleanupTimer.refresh();
  }

  attachDownlink(req, res) {
    if (this.closed) {
      if (!res.headersSent) res.writeHead(410, XHTTP_RESPONSE_HEADERS);
      res.end();
      return;
    }

    // Only one active downlink response per XHTTP session.
    if (this.downlinkRes && !this.downlinkRes.writableEnded) {
      try { this.downlinkRes.end(); } catch (_) {}
    }

    this.refresh();
    this.downlinkRes = res;
    req.setTimeout(0);
    res.setTimeout(0);
    try { req.socket.setKeepAlive(true, 30000); req.socket.setNoDelay(true); } catch (_) {}

    if (!res.headersSent) {
      res.writeHead(200, XHTTP_RESPONSE_HEADERS);
    }

    res.on('close', () => {
      // In packet-up mode this is the only download pipe; closing it ends the session.
      if (!this.closed) this.close('downlink closed');
    });
    res.on('error', () => this.close('downlink error'));

    this.flushDownlink();
  }

  async processPacket(seq, data) {
    if (this.closed) throw new Error('session closed');
    if (!Number.isSafeInteger(seq) || seq < 0) throw new Error('invalid sequence');
    if (this.pendingPackets.size > MAX_XHTTP_BUFFERED_POSTS) throw new Error('too many buffered posts');

    this.refresh();
    this.pendingPackets.set(seq, Buffer.isBuffer(data) ? data : Buffer.from(data));
    await this.drainPackets();
  }

  async drainPackets() {
    while (!this.closed && this.pendingPackets.has(this.nextSeq)) {
      const seq = this.nextSeq;
      const packet = this.pendingPackets.get(seq);
      this.pendingPackets.delete(seq);
      this.nextSeq += 1;

      if (seq === 0) {
        await this.initialize(packet);
      } else {
        this.writeToRemote(packet);
      }
    }
  }

  initialize(firstPacket) {
    if (this.remote || this.udpMode) return Promise.resolve();

    let request;
    try {
      request = parseVlessRequest(firstPacket, { allowUdp: true });
    } catch (err) {
      this.close('bad vless request');
      throw err;
    }

    this.user = request.user;
    this.version = request.version;
    this.unregisterConnection = registerConnection(this.user.id, (reason) => this.close(reason));
    log('[XHTTP] Connect:', `${this.user.remark} -> ${request.host}:${request.port} sid=${this.sessionId}`);

    if (request.command === 0x02) {
      this.udpMode = true;
      this.udpTargetHost = request.host;
      this.udpTargetPort = request.port;
      this.established = true;
      this.flushDownlink();

      const udpType = request.host.includes(':') ? 'udp6' : 'udp4';
      this.udpSocket = dgram.createSocket(udpType);
      this.udpSocket.on('message', (msg) => {
        this.refresh();
        updateTraffic(this.user, msg.length);
        try {
          this.sendDownlink(encodeUdpPacket(msg));
        } catch (err) {
          log('[XHTTP UDP] Encode failed:', err.message);
          this.close('udp encode failed');
        }
      });
      this.udpSocket.on('error', (err) => {
        log('[XHTTP UDP] Error:', err && err.message ? err.message : 'unknown');
        this.close('udp error');
      });
      this.udpSocket.on('close', () => this.close('udp closed'));
      this.udpIdleTimer = setTimeout(() => this.close('udp idle timeout'), UDP_IDLE_TIMEOUT_MS);

      if (request.payload.length) this.writeToRemote(request.payload);
      this.flushRemoteWrites();
      return Promise.resolve();
    }

    return new Promise((resolve, reject) => {
      let settled = false;
      this.connectTimer = setTimeout(() => {
        if (settled) return;
        settled = true;
        this.close('connect timeout');
        reject(new Error('connect timeout'));
      }, CONNECT_TIMEOUT_MS);

      this.remote = net.createConnection({ host: request.host, port: request.port, allowHalfOpen: false, autoSelectFamily: true });
      this.remote.setNoDelay(true);
      this.remote.setKeepAlive(true, 30000);
      this.remote.setTimeout(IDLE_TIMEOUT_MS);

      this.remote.once('connect', () => {
        if (this.closed) return;
        if (settled) return;
        settled = true;
        clearTimeout(this.connectTimer);
        this.established = true;
        this.flushDownlink();
        if (request.payload.length) this.writeToRemote(request.payload);
        this.flushRemoteWrites();
        resolve();
      });

      this.remote.once('error', (err) => {
        log('[XHTTP Remote] Error:', `${request.host}:${request.port} ${err && err.message ? err.message : ''}`);
        if (!settled) {
          settled = true;
          clearTimeout(this.connectTimer);
          reject(err);
        }
        this.close('remote error');
      });

      this.remote.on('data', (chunk) => {
        this.refresh();
        updateTraffic(this.user, chunk.length);
        this.sendDownlink(chunk);
      });
      this.remote.on('drain', () => this.flushRemoteWrites());
      this.remote.on('timeout', () => this.close('remote timeout'));
      this.remote.on('close', () => this.close('remote closed'));
    });
  }

  writeToRemote(chunk) {
    if (this.closed) return;
    if (!chunk || chunk.length === 0) return;

    if (this.udpMode) {
      try {
        drainUdpPackets(this.udpState, chunk, (packet) => this.sendUdpPacket(packet));
      } catch (err) {
        log('[XHTTP UDP] Bad packet:', err.message);
        this.close('bad udp packet');
      }
      return;
    }

    if (!this.remote || !this.established || this.remote.destroyed) {
      this.pendingRemoteWrites.push(chunk);
      return;
    }

    updateTraffic(this.user, chunk.length);
    if (!this.remote.write(chunk)) {
      // Let Node socket backpressure work; later packets remain queued if needed.
    }
  }

  sendUdpPacket(packet) {
    if (this.closed || !this.udpSocket || !this.udpTargetHost || !this.udpTargetPort) return;
    if (this.udpIdleTimer && this.udpIdleTimer.refresh) this.udpIdleTimer.refresh();
    updateTraffic(this.user, packet.length);
    this.udpSocket.send(packet, this.udpTargetPort, this.udpTargetHost, (err) => {
      if (err) {
        log('[XHTTP UDP] Send failed:', err.message);
        this.close('udp send failed');
      }
    });
  }

  flushRemoteWrites() {
    if (this.udpMode) {
      while (this.pendingRemoteWrites.length) this.writeToRemote(this.pendingRemoteWrites.shift());
      return;
    }
    if (!this.remote || !this.established || this.remote.destroyed) return;
    while (this.pendingRemoteWrites.length) {
      const chunk = this.pendingRemoteWrites.shift();
      updateTraffic(this.user, chunk.length);
      if (!this.remote.write(chunk)) break;
    }
  }

  sendDownlink(chunk) {
    if (this.closed) return;
    if (!this.downlinkRes || this.downlinkRes.writableEnded || this.downlinkRes.destroyed) {
      this.pendingDownlinkChunks.push(chunk);
      this.pendingDownlinkBytes += chunk.length;
      if (this.pendingDownlinkBytes > MAX_XHTTP_DOWNLINK_BUFFER) {
        this.close('downlink buffer overflow');
      }
      return;
    }
    this.flushDownlink();
    if (!this.downlinkRes.writableEnded) this.downlinkRes.write(chunk);
  }

  sendVlessResponseHeader() {
    if (this.responseHeaderSent || !this.downlinkRes || this.downlinkRes.writableEnded) return;
    this.responseHeaderSent = true;
    this.downlinkRes.write(Buffer.from([this.version, 0x00]));
  }

  flushDownlink() {
    if (this.closed || !this.downlinkRes || this.downlinkRes.writableEnded) return;
    if (this.established) this.sendVlessResponseHeader();
    while (this.pendingDownlinkChunks.length && !this.downlinkRes.writableEnded) {
      const chunk = this.pendingDownlinkChunks.shift();
      this.pendingDownlinkBytes -= chunk.length;
      this.downlinkRes.write(chunk);
    }
  }

  close(reason = 'closed') {
    if (this.closed) return;
    this.closed = true;
    clearTimeout(this.cleanupTimer);
    clearTimeout(this.connectTimer);
    clearTimeout(this.udpIdleTimer);
    if (this.unregisterConnection) this.unregisterConnection();
    destroySocket(this.remote);
    closeUdpSocket(this.udpSocket);
    if (this.downlinkRes && !this.downlinkRes.writableEnded) {
      try { this.downlinkRes.end(); } catch (_) {}
    }
    this.pendingPackets.clear();
    this.pendingRemoteWrites = [];
    this.pendingDownlinkChunks = [];
    this.pendingDownlinkBytes = 0;
    xhttpSessions.delete(this.sessionId);
    if (reason && reason !== 'remote closed' && reason !== 'downlink closed') {
      log('[XHTTP] Session closed:', `${this.sessionId} ${reason}`);
    }
  }
}

function getXHttpSession(sessionId) {
  let session = xhttpSessions.get(sessionId);
  if (!session || session.closed) {
    session = new XHttpSession(sessionId);
    xhttpSessions.set(sessionId, session);
  }
  return session;
}

function getClientIp(req) {
  return String(req.headers['cf-connecting-ip'] || req.ip || req.socket.remoteAddress || 'unknown');
}

function getLoginState(ip) {
  const now = Date.now();
  let state = loginAttempts.get(ip);
  if (!state || now - state.firstAt > LOGIN_WINDOW_MS) {
    state = { count: 0, firstAt: now, lockedUntil: 0 };
    loginAttempts.set(ip, state);
  }
  return state;
}

function isLoginLocked(ip) {
  const state = getLoginState(ip);
  if (state.lockedUntil && Date.now() < state.lockedUntil) return state.lockedUntil;
  if (state.lockedUntil && Date.now() >= state.lockedUntil) {
    loginAttempts.delete(ip);
  }
  return 0;
}

function recordLoginFailure(ip) {
  const state = getLoginState(ip);
  state.count += 1;
  if (state.count >= LOGIN_MAX_ATTEMPTS) {
    state.lockedUntil = Date.now() + LOGIN_LOCK_MS;
  }
  loginAttempts.set(ip, state);
  return state;
}

function recordLoginSuccess(ip) {
  loginAttempts.delete(ip);
}

function requireAuth(req, res, next) {
  const token = req.cookies.admin_session;
  if (token && activeSessions.has(token)) return next();
  return res.status(401).json({ error: 'Unauthorized' });
}

app.get('/health', (req, res) => res.type('text/plain').send('ok\n'));

app.options(`${XHTTP_PATH}/*`, (req, res) => {
  res.writeHead(204, XHTTP_RESPONSE_HEADERS);
  res.end();
});

app.get([`${XHTTP_PATH}/:sessionId`, `${XHTTP_PATH}/:sessionId/`], (req, res) => {
  const sessionId = String(req.params.sessionId || '').trim();
  if (sessionId.length < 1 || sessionId.length > 256) {
    res.writeHead(400, XHTTP_RESPONSE_HEADERS);
    res.end();
    return;
  }
  const session = getXHttpSession(sessionId);
  session.attachDownlink(req, res);
});

app.post([`${XHTTP_PATH}/:sessionId/:seq`, `${XHTTP_PATH}/:sessionId/:seq/`], async (req, res) => {
  const sessionId = String(req.params.sessionId || '').trim();
  const seq = Number(req.params.seq);
  if (sessionId.length < 1 || sessionId.length > 256 || !Number.isSafeInteger(seq) || seq < 0) {
    res.writeHead(400, XHTTP_RESPONSE_HEADERS);
    res.end();
    return;
  }

  try {
    const body = await readRequestBody(req, MAX_XHTTP_POST_SIZE);
    const session = getXHttpSession(sessionId);
    await session.processPacket(seq, body);
    res.writeHead(200, XHTTP_RESPONSE_HEADERS);
    res.end();
  } catch (err) {
    log('[XHTTP] POST failed:', err && err.message ? err.message : 'unknown');
    if (!res.headersSent) res.writeHead(err && err.message === 'request body too large' ? 413 : 500, XHTTP_RESPONSE_HEADERS);
    try { res.end(); } catch (_) {}
  }
});

app.post('/api/login', (req, res) => {
  const ip = getClientIp(req);
  const lockedUntil = isLoginLocked(ip);
  if (lockedUntil) {
    const retryAfter = Math.ceil((lockedUntil - Date.now()) / 1000);
    res.setHeader('Retry-After', String(retryAfter));
    return res.status(429).json({ success: false, error: 'Too many login attempts. Try again later.', retry_after: retryAfter });
  }

  const { username, password } = req.body || {};
  const hash = generateHash(password || '');
  if (DB.admin && DB.admin.username === username && DB.admin.password_hash === hash) {
    recordLoginSuccess(ip);
    const token = crypto.randomBytes(32).toString('hex');
    activeSessions.add(token);
    res.cookie('admin_session', token, {
      httpOnly: true,
      sameSite: 'lax',
      secure: req.secure || req.headers['x-forwarded-proto'] === 'https',
      maxAge: 24 * 60 * 60 * 1000
    });
    return res.json({ success: true });
  }

  const state = recordLoginFailure(ip);
  return res.status(401).json({ success: false, error: 'Invalid credentials', attempts_left: Math.max(0, LOGIN_MAX_ATTEMPTS - state.count) });
});

app.post('/api/logout', requireAuth, (req, res) => {
  activeSessions.delete(req.cookies.admin_session);
  res.clearCookie('admin_session');
  res.json({ success: true });
});

app.post('/api/admin/change-password', requireAuth, (req, res) => {
  const { current_password, new_password } = req.body || {};
  const currentHash = generateHash(current_password || '');
  const nextPassword = String(new_password || '');

  if (!DB.admin || DB.admin.password_hash !== currentHash) {
    return res.status(400).json({ error: 'Current password is incorrect' });
  }
  if (nextPassword.length < 8) {
    return res.status(400).json({ error: 'New password must be at least 8 characters' });
  }

  DB.admin.password_hash = generateHash(nextPassword);
  scheduleSaveDB(false);
  activeSessions.clear();
  res.clearCookie('admin_session');
  log('[Admin] Password changed');
  res.json({ success: true, relogin: true });
});

app.get('/api/stats', requireAuth, (req, res) => {
  const memory = process.memoryUsage();
  const totalTraffic = DB.users.reduce((sum, u) => sum + Number(u.used_bytes || 0), 0);
  const connectionCounts = Array.from(activeConnections.values()).map((set) => set.size);
  const activeConnectionCount = connectionCounts.reduce((sum, count) => sum + count, 0);
  res.json({
    ram_usage: Math.round((memory.heapUsed / Math.max(1, memory.heapTotal)) * 100),
    cpu_usage: getCpuUsagePercent(),
    uptime: Math.round(process.uptime()),
    total_traffic: totalTraffic,
    users_count: DB.users.length,
    active_count: DB.users.filter((u) => u.is_active === 1 && !isUserExpired(u)).length,
    online_users: activeConnections.size,
    ws_connections: activeConnectionCount,
    xhttp_sessions: xhttpSessions.size,
    traffic_chart: getTrafficChart()
  });
});

app.get('/api/users', requireAuth, (req, res) => {
  const users = DB.users.map((user) => {
    const connections = activeConnections.get(user.id)?.size || 0;
    return { ...user, online: connections > 0, connections };
  });
  res.json({ users });
});

app.post('/api/users', requireAuth, (req, res) => {
  const { remark, traffic_limit_gb, expiry_days } = req.body || {};
  const cleanRemark = String(remark || '').trim();
  if (!cleanRemark) return res.status(400).json({ error: 'Remark is required' });
  if (DB.users.some((u) => u.remark === cleanRemark)) return res.status(400).json({ error: 'Username must be unique' });

  const limitGB = Number(traffic_limit_gb || 0);
  const expiryDays = Number(expiry_days || 0);
  const host = req.get('host') || '';
  let createdOn = 'other';
  if (host.includes('railway.app')) createdOn = 'railway';
  else if (host.includes('koyeb.app')) createdOn = 'koyeb';

  const user = {
    id: crypto.randomUUID(),
    remark: cleanRemark,
    uuid: crypto.randomUUID(),
    sub_id: crypto.randomBytes(8).toString('hex'),
    traffic_limit: limitGB > 0 ? Math.floor(limitGB * 1024 * 1024 * 1024) : 0,
    used_bytes: 0,
    expiry_time: expiryDays > 0 ? Date.now() + Math.floor(expiryDays * 24 * 60 * 60 * 1000) : 0,
    last_seen: 0,
    is_active: 1,
    protocol: 'ws+xhttp',
    created_on: createdOn,
    created_at: new Date().toISOString()
  };

  DB.users.unshift(user);
  scheduleSaveDB(false);
  log('[User] Created:', `${user.remark} ${user.uuid}`);
  res.json({ success: true, user });
});

app.delete('/api/users/:id', requireAuth, (req, res) => {
  const before = DB.users.length;
  const user = DB.users.find((u) => u.id === req.params.id);
  DB.users = DB.users.filter((u) => u.id !== req.params.id);
  if (DB.users.length < before) {
    closeUserConnections(req.params.id, 'user deleted');
    scheduleSaveDB(false);
    log('[User] Deleted:', user ? user.remark : req.params.id);
    return res.json({ success: true });
  }
  return res.status(404).json({ error: 'User not found' });
});

app.post('/api/users/:id/toggle', requireAuth, (req, res) => {
  const user = DB.users.find((u) => u.id === req.params.id);
  if (!user) return res.status(404).json({ error: 'User not found' });
  user.is_active = user.is_active === 1 ? 0 : 1;
  if (user.is_active === 0) closeUserConnections(user.id, 'user disabled');
  scheduleSaveDB(false);
  res.json({ success: true, is_active: user.is_active });
});

app.post('/api/users/:id/reset', requireAuth, (req, res) => {
  const user = DB.users.find((u) => u.id === req.params.id);
  if (!user) return res.status(404).json({ error: 'User not found' });
  user.used_bytes = 0;
  user.is_active = 1;
  user.last_seen = 0;
  scheduleSaveDB(false);
  res.json({ success: true });
});

app.get('/api/users/:id/qr/:type', requireAuth, async (req, res) => {
  try {
    const user = DB.users.find((u) => u.id === req.params.id);
    if (!user) return res.status(404).type('text/plain').send('User not found');
    await sendQrSvg(res, getQrPayload(req, user, req.params.type));
  } catch (err) {
    log('[QR] Generate failed:', err && err.message ? err.message : 'unknown');
    res.status(500).type('text/plain').send('QR generation failed');
  }
});

app.put('/api/users/:id', requireAuth, (req, res) => {
  const user = DB.users.find((u) => u.id === req.params.id);
  if (!user) return res.status(404).json({ error: 'User not found' });

  const { remark, traffic_limit_gb, expiry_days } = req.body || {};
  const cleanRemark = String(remark || '').trim();
  if (!cleanRemark) return res.status(400).json({ error: 'Remark is required' });
  if (DB.users.some((u) => u.id !== user.id && u.remark === cleanRemark)) {
    return res.status(400).json({ error: 'Username must be unique' });
  }

  const limitGB = Number(traffic_limit_gb || 0);
  const expiryDays = Number(expiry_days || 0);

  user.remark = cleanRemark;
  user.traffic_limit = limitGB > 0 ? Math.floor(limitGB * 1024 * 1024 * 1024) : 0;
  user.expiry_time = expiryDays > 0 ? Date.now() + Math.floor(expiryDays * 24 * 60 * 60 * 1000) : 0;

  if (isUserExpired(user)) {
    user.is_active = 0;
    closeUserConnections(user.id, 'user edited and expired');
  } else {
    user.is_active = 1;
  }

  scheduleSaveDB(false);
  log('[User] Edited:', `${user.remark} ${user.uuid}`);
  res.json({ success: true, user });
});

app.get('/qr/:sub_id/:type', async (req, res) => {
  try {
    const user = DB.users.find((u) => u.sub_id === req.params.sub_id);
    if (!user) return res.status(404).type('text/plain').send('Subscription not found');
    await sendQrSvg(res, getQrPayload(req, user, req.params.type));
  } catch (err) {
    log('[QR] Public generate failed:', err && err.message ? err.message : 'unknown');
    res.status(500).type('text/plain').send('QR generation failed');
  }
});

app.get('/sub/:sub_id', (req, res) => {
  const user = DB.users.find((u) => u.sub_id === req.params.sub_id);
  if (!user) return res.status(404).send('Subscription Not Found');

  const subscriptionLinks = buildSubscriptionLinks(req, user);
  const wsLink = subscriptionLinks[0];
  const xhttpLink = subscriptionLinks[1];
  const ua = String(req.headers['user-agent'] || '').toLowerCase();
  const accept = String(req.headers.accept || '').toLowerCase();

  // Smart subscription routing:
  // - Human browsers get the visual subscription page with separate copy buttons.
  // - VPN clients/subscription updaters get base64 text containing both WS and XHTTP configs.
  // - ?raw=1 always returns base64 subscription.
  // - ?html=1 always returns the visual page.
  const clientKeywords = [
    'v2ray', 'v2rayn', 'v2rayng', 'nekobox', 'nekoray', 'hiddify', 'sing-box', 'singbox',
    'shadowrocket', 'streisand', 'foxray', 'clash', 'mihomo', 'surge', 'quantumult',
    'loon', 'sager', 'sagernet', 'surfboard', 'sub-store', 'subconverter', 'go-http-client',
    'curl', 'wget', 'python-requests', 'okhttp'
  ];
  const browserKeywords = ['mozilla', 'chrome', 'chromium', 'safari', 'firefox', 'edg/', 'opr/', 'brave'];
  const isClient = clientKeywords.some((k) => ua.includes(k));
  const isBrowser = browserKeywords.some((k) => ua.includes(k)) && accept.includes('text/html') && !isClient;
  const wantsHtml = req.query.html === '1' || (isBrowser && req.query.raw !== '1');

  if (!wantsHtml) {
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.setHeader('Profile-Update-Interval', '24');
    res.setHeader('Subscription-Userinfo', `upload=${user.used_bytes || 0}; download=0; total=${user.traffic_limit || 0}; expire=${user.expiry_time ? Math.floor(user.expiry_time / 1000) : 0}`);
    return res.send(Buffer.from(subscriptionLinks.join('\n')).toString('base64'));
  }

  const usedFormatted = formatBytes(user.used_bytes);
  const remainingFormatted = user.traffic_limit === 0 ? 'Unlimited' : formatBytes(Math.max(0, user.traffic_limit - user.used_bytes));
  const percent = calcPercent(user.used_bytes, user.traffic_limit).toFixed(1);
  const progressWidth = calcPercent(user.used_bytes, user.traffic_limit).toFixed(2);
  const isDanger = user.traffic_limit > 0 && (user.used_bytes / user.traffic_limit) > 0.9;
  const expired = isUserExpired(user);
  const expiryStr = user.expiry_time === 0 ? 'Never' : formatTehranDate(user.expiry_time);
  const lastSeenStr = user.last_seen === 0 ? 'Never' : formatTehranDate(user.last_seen);
  const remarkSafe = safeName(user.remark);
  const wsLinkJSON = JSON.stringify(wsLink);
  const xhttpLinkJSON = JSON.stringify(xhttpLink);
  const allLinksJSON = JSON.stringify(subscriptionLinks.join('\n'));

  const subUrlJSON = JSON.stringify(buildSubUrl(req, user));
  const qrBase = `/qr/${user.sub_id}`;

  res.send(`<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>${remarkSafe} - Subscription</title>
<style>
*{box-sizing:border-box;margin:0;padding:0;font-family:Arial,sans-serif}body{background:radial-gradient(circle at top right,#1a1a2e,#08080d 70%);color:#fff;min-height:100vh;display:flex;justify-content:center;align-items:center;padding:20px}.shell{width:100%;max-width:980px}.card{background:rgba(255,255,255,.045);border:1px solid rgba(255,255,255,.08);border-radius:28px;padding:30px;box-shadow:0 25px 70px rgba(0,0,0,.55);backdrop-filter:blur(18px)}.hero{text-align:center;margin-bottom:26px}h1{color:#d4af37;margin-bottom:8px;font-size:32px}.sub{color:#888;letter-spacing:2px;font-size:12px;text-transform:uppercase}.grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}.stat{background:rgba(0,0,0,.25);border:1px solid rgba(255,255,255,.06);border-radius:18px;padding:18px;text-align:center}.label{color:#888;font-size:11px;text-transform:uppercase;letter-spacing:1px}.value{font-size:22px;font-weight:800;margin-top:6px}.progress{height:9px;background:rgba(255,255,255,.08);border-radius:10px;overflow:hidden;margin:12px 0 22px}.fill{height:100%;background:linear-gradient(90deg,#d4af37,#eab308);width:${progressWidth}%}.danger{background:linear-gradient(90deg,#ef4444,#f87171)}.info{background:rgba(0,0,0,.25);border:1px solid rgba(255,255,255,.06);border-radius:18px;padding:18px;margin:18px 0}.row{display:flex;justify-content:space-between;gap:14px;margin:12px 0;color:#ddd}.row span:first-child{color:#888}.badge{white-space:nowrap;padding:5px 12px;border-radius:20px;font-size:11px;font-weight:800}.active{background:rgba(34,197,94,.13);color:#4ade80}.expired{background:rgba(239,68,68,.13);color:#f87171}.configs{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-top:18px}.cfg{background:rgba(0,0,0,.25);border:1px solid rgba(255,255,255,.07);border-radius:20px;padding:18px}.cfg h3{font-size:16px;margin-bottom:12px;color:#d4af37}.qr{background:#fff;border-radius:14px;padding:10px;width:148px;height:148px;margin:0 auto 14px;display:flex;align-items:center;justify-content:center}.qr img{width:128px;height:128px}.btn{width:100%;background:linear-gradient(45deg,#d4af37,#b2891d);color:#000;border:0;padding:14px;border-radius:12px;font-weight:900;cursor:pointer;margin-top:9px}.btn.secondary{background:rgba(255,255,255,.06);color:#d4af37;border:1px solid rgba(212,175,55,.35)}.btn.dark{background:rgba(0,0,0,.28);color:#ddd;border:1px solid rgba(255,255,255,.1)}.guide{text-align:center;margin-top:18px;color:#777;font-size:12px;line-height:1.6}@media(max-width:760px){.card{padding:22px;border-radius:22px}.grid,.configs{grid-template-columns:1fr}h1{font-size:26px}.row{font-size:13px}.qr{width:136px;height:136px}.qr img{width:116px;height:116px}}
</style></head><body><main class="shell"><div class="card"><div class="hero"><h1>${remarkSafe}</h1><div class="sub">Smart VLESS Subscription</div></div>
<div class="grid"><div class="stat"><div class="label">Used Traffic</div><div class="value">${usedFormatted}</div></div><div class="stat"><div class="label">Remaining</div><div class="value">${remainingFormatted}</div></div></div>
<div style="display:flex;justify-content:space-between;color:#aaa;font-size:12px;margin-top:18px"><span>Traffic Usage</span><span>${percent}%</span></div><div class="progress"><div class="fill ${isDanger ? 'danger' : ''}"></div></div>
<div class="info"><div class="row"><span>Status</span><b class="badge ${expired ? 'expired' : 'active'}">${expired ? 'Expired' : 'Active'}</b></div><div class="row"><span>Protocol</span><b>VLESS over XHTTP + WS</b></div><div class="row"><span>Expiration</span><b>${expiryStr}</b></div><div class="row"><span>Last Seen</span><b>${lastSeenStr}</b></div></div>
<div class="configs"><section class="cfg"><h3>XHTTP Config</h3><div class="qr"><img alt="XHTTP QR" src="${qrBase}/xhttp"></div><button class="btn" onclick="copyXhttp()">Copy XHTTP Config</button></section><section class="cfg"><h3>WebSocket Config</h3><div class="qr"><img alt="WebSocket QR" src="${qrBase}/ws"></div><button class="btn secondary" onclick="copyWs()">Copy WebSocket Config</button></section></div>
<button class="btn dark" onclick="copySub()">Copy Subscription URL</button><button class="btn dark" onclick="copyAll()">Copy Both Raw Configs</button><div class="guide">VPN clients get both configs automatically. Browsers see this smart page with separate QR codes and copy buttons.</div></div></main>
<script>var WS_LINK=${wsLinkJSON};var XHTTP_LINK=${xhttpLinkJSON};var ALL_LINKS=${allLinksJSON};var SUB_URL=${subUrlJSON};function copyText(t){navigator.clipboard.writeText(t).then(function(){alert('Copied!')}).catch(function(){prompt('Copy:',t)})}function copyXhttp(){copyText(XHTTP_LINK)}function copyWs(){copyText(WS_LINK)}function copyAll(){copyText(ALL_LINKS)}function copySub(){copyText(SUB_URL)}</script></body></html>`);});

app.get('/login', (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/panel', (req, res) => {
  if (!activeSessions.has(req.cookies.admin_session)) return res.redirect('/login');
  res.sendFile(path.join(__dirname, 'public', 'dashboard.html'));
});
app.get('/', (req, res) => res.redirect('/login'));

const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true, perMessageDeflate: false, maxPayload: MAX_WS_PAYLOAD });

server.on('upgrade', (req, socket, head) => {
  let url;
  try { url = new URL(req.url || '/', 'http://127.0.0.1'); } catch (_) { socket.destroy(); return; }
  if (url.pathname !== WS_PATH) {
    socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});

wss.on('connection', (ws, req) => {
  ws.binaryType = 'nodebuffer';

  let remote = null;
  let udpSocket = null;
  let user = null;
  let unregisterConnection = null;
  let established = false;
  let closed = false;
  let firstPacketHandled = false;
  let responseHeaderSent = false;
  let udpMode = false;
  let udpTargetHost = '';
  let udpTargetPort = 0;
  let udpIdleTimer = null;
  const udpState = { buffer: Buffer.alloc(0) };
  const pendingClientChunks = [];

  const idleTimer = setTimeout(() => closeAll(1000, 'idle timeout'), IDLE_TIMEOUT_MS);
  const connectTimer = setTimeout(() => closeAll(1013, 'connect timeout'), CONNECT_TIMEOUT_MS);

  function refreshIdleTimer() {
    if (idleTimer.refresh) idleTimer.refresh();
  }

  function closeAll(code = 1000, reason = '') {
    if (closed) return;
    closed = true;
    clearTimeout(idleTimer);
    clearTimeout(connectTimer);
    clearTimeout(udpIdleTimer);
    if (unregisterConnection) unregisterConnection();
    destroySocket(remote);
    closeUdpSocket(udpSocket);
    safeCloseWebSocket(ws, code, reason);
  }

  function sendToWs(chunk) {
    if (closed || ws.readyState !== ws.OPEN) return false;
    if (ws.bufferedAmount > WS_BACKPRESSURE_LIMIT) {
      if (remote) remote.pause();
      const interval = setInterval(() => {
        if (closed || ws.readyState !== ws.OPEN) { clearInterval(interval); return; }
        if (ws.bufferedAmount < WS_BACKPRESSURE_RESUME) {
          clearInterval(interval);
          if (remote) remote.resume();
        }
      }, 25);
    }
    ws.send(chunk, { binary: true }, (err) => {
      if (err) {
        log('[WS] Send failed:', err.message);
        closeAll(1011, 'websocket send failed');
      }
    });
    return true;
  }

  function sendVlessResponseHeader(version) {
    if (responseHeaderSent) return;
    responseHeaderSent = true;
    sendToWs(Buffer.from([version, 0x00]));
  }

  function flushPendingClientChunks() {
    if (!remote || remote.destroyed || !established) return;
    while (pendingClientChunks.length) {
      const chunk = pendingClientChunks.shift();
      updateTraffic(user, chunk.length);
      if (!remote.write(chunk)) break;
    }
  }

  function sendUdpPacket(packet) {
    if (closed || !udpSocket || !udpTargetHost || !udpTargetPort) return;
    if (udpIdleTimer && udpIdleTimer.refresh) udpIdleTimer.refresh();
    updateTraffic(user, packet.length);
    udpSocket.send(packet, udpTargetPort, udpTargetHost, (err) => {
      if (err) {
        log('[UDP] Send failed:', err.message);
        closeAll(1011, 'udp send failed');
      }
    });
  }

  function handleClientData(data) {
    refreshIdleTimer();
    const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);

    if (!firstPacketHandled) {
      firstPacketHandled = true;
      let request;
      try {
        request = parseVlessRequest(chunk, { allowUdp: true });
      } catch (err) {
        log('[VLESS] Bad request:', err.message);
        closeAll(1008, 'bad vless request');
        return;
      }

      user = request.user;
      unregisterConnection = registerConnection(user.id, (reason) => closeAll(1000, reason));
      log('[VLESS] Connect:', `${user.remark} -> ${request.host}:${request.port}`);

      if (request.command === 0x02) {
        udpMode = true;
        clearTimeout(connectTimer);
        established = true;
        sendVlessResponseHeader(request.version);

        udpTargetHost = request.host;
        udpTargetPort = request.port;
        const udpType = request.host.includes(':') ? 'udp6' : 'udp4';
        udpSocket = dgram.createSocket(udpType);
        udpSocket.on('message', (msg) => {
          refreshIdleTimer();
          updateTraffic(user, msg.length);
          try {
            sendToWs(encodeUdpPacket(msg));
          } catch (err) {
            log('[UDP] Encode failed:', err.message);
            closeAll(1011, 'udp encode failed');
          }
        });
        udpSocket.on('error', (err) => {
          log('[UDP] Error:', err && err.message ? err.message : 'unknown');
          closeAll(1011, 'udp error');
        });
        udpSocket.on('close', () => closeAll(1000, 'udp closed'));

        udpIdleTimer = setTimeout(() => closeAll(1000, 'udp idle timeout'), UDP_IDLE_TIMEOUT_MS);

        try {
          drainUdpPackets(udpState, request.payload, sendUdpPacket);
          while (pendingClientChunks.length) drainUdpPackets(udpState, pendingClientChunks.shift(), sendUdpPacket);
        } catch (err) {
          log('[UDP] Bad packet:', err.message);
          closeAll(1008, 'bad udp packet');
        }
        return;
      }

      remote = net.createConnection({ host: request.host, port: request.port, allowHalfOpen: false, autoSelectFamily: true });
      remote.setNoDelay(true);
      remote.setKeepAlive(true, 30000);
      remote.setTimeout(IDLE_TIMEOUT_MS);

      remote.once('connect', () => {
        if (closed) return;
        clearTimeout(connectTimer);
        established = true;
        sendVlessResponseHeader(request.version);
        if (request.payload.length) {
          updateTraffic(user, request.payload.length);
          if (!remote.write(request.payload)) {
            try { req.socket.pause(); } catch (_) {}
          }
        }
        flushPendingClientChunks();
      });

      remote.once('error', (err) => {
        log('[Remote] Error:', `${request.host}:${request.port} ${err && err.message ? err.message : ''}`);
        closeAll(1011, 'remote error');
      });

      remote.on('data', (remoteChunk) => {
        refreshIdleTimer();
        updateTraffic(user, remoteChunk.length);
        sendToWs(remoteChunk);
      });

      remote.on('drain', () => {
        try { req.socket.resume(); } catch (_) {}
        flushPendingClientChunks();
      });

      remote.on('timeout', () => closeAll(1000, 'remote timeout'));
      remote.on('close', () => closeAll(1000, 'remote closed'));
      return;
    }

    if (udpMode) {
      try {
        drainUdpPackets(udpState, chunk, sendUdpPacket);
      } catch (err) {
        log('[UDP] Bad packet:', err.message);
        closeAll(1008, 'bad udp packet');
      }
      return;
    }

    if (!remote || remote.destroyed) {
      closeAll(1011, 'remote not available');
      return;
    }

    if (!established) {
      pendingClientChunks.push(chunk);
      return;
    }

    updateTraffic(user, chunk.length);
    if (!remote.write(chunk)) {
      try { req.socket.pause(); } catch (_) {}
    }
  }

  ws.on('message', (data, isBinary) => {
    if (!isBinary && typeof data === 'string') {
      closeAll(1003, 'binary frames required');
      return;
    }
    handleClientData(data);
  });
  ws.on('close', () => closeAll(1000, 'websocket closed'));
  ws.on('error', (err) => {
    log('[WS] Error:', err && err.message ? err.message : 'unknown');
    closeAll(1011, 'websocket error');
  });

  const earlyData = decodeEarlyDataFromProtocolHeader(req.headers['sec-websocket-protocol']);
  if (earlyData) {
    log('[WS] Early data:', `${earlyData.length} bytes`);
    setImmediate(() => handleClientData(earlyData));
  }
});

setInterval(() => {
  trafficHistory.push({ ts: Date.now(), bytes: trafficCurrentMinuteBytes });
  trafficCurrentMinuteBytes = 0;
  if (trafficHistory.length > 59) trafficHistory = trafficHistory.slice(-59);
}, 60 * 1000);

setInterval(() => {
  for (const user of DB.users) {
    if (user.is_active === 1 && isUserExpired(user)) {
      user.is_active = 0;
      closeUserConnections(user.id, 'expired');
      scheduleSaveDB(false);
      log('[User] Expired:', user.remark);
    }
  }
}, 30000);

setInterval(() => {
  try {
    const url = selfPingHealthUrl;
    if (!url) return;

    if (lastSelfPingUrlLog !== url) {
      lastSelfPingUrlLog = url;
      log('[KEEPALIVE] Self-ping target:', url);
    }

    fetch(url, {
      method: 'HEAD',
      cache: 'no-store',
      headers: { 'user-agent': 'Mahan-Panel-Self-Keepalive/1.0' }
    }).catch((err) => {
      log('[KEEPALIVE] Self-ping failed:', err && err.message ? err.message : 'unknown');
    });
  } catch (_) {}
}, SELF_PING_INTERVAL_MS);

process.on('SIGTERM', () => {
  try { saveDBNow(); } catch (_) {}
  process.exit(0);
});
process.on('SIGINT', () => {
  try { saveDBNow(); } catch (_) {}
  process.exit(0);
});
process.on('uncaughtException', (err) => log('[ERROR] Uncaught:', err && err.stack ? err.stack : String(err)));
process.on('unhandledRejection', (err) => log('[ERROR] Unhandled:', err && err.stack ? err.stack : String(err)));

loadDB();
server.listen(PORT, '0.0.0.0', () => {
  log('[INFO] Mahan Panel VLESS-WS/XHTTP active on port', String(PORT));
  log('[INFO] WS path:', WS_PATH);
  log('[INFO] XHTTP path:', XHTTP_PATH);
  log('[INFO] Default admin:', 'admin / admin');
});
