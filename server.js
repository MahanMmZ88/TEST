'use strict';

const http = require('http');
const net = require('net');
const dgram = require('dgram');
const dns = require('dns').promises;
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
const BUILD_ID = 'server-only-subscription-layout-2026-06-20-a';
const WS_PATH = '/ws';
const XHTTP_PATH = '/xhttp';
const DB_PATH = path.join(__dirname, 'db.json');

const PASSWORD_SALT = 'MAHAN_PANEL_NODE_WS_SALT_v2';
const CONNECT_TIMEOUT_MS = 15000;
const IDLE_TIMEOUT_MS = 120000;
const MAX_FIRST_PACKET = 128 * 1024;
const MAX_WS_PAYLOAD = 8 * 1024 * 1024;
const WS_BACKPRESSURE_LIMIT = 4 * 1024 * 1024;
const WS_BACKPRESSURE_RESUME = 1 * 1024 * 1024;
const MAX_UDP_PACKET_SIZE = 64 * 1024;
const UDP_IDLE_TIMEOUT_MS = 60000;
const SELF_PING_INTERVAL_MS = 5 * 60 * 1000;
const MAX_XHTTP_POST_SIZE = 4 * 1024 * 1024;
const MAX_XHTTP_BUFFERED_POSTS = 32;
const MAX_XHTTP_DOWNLINK_BUFFER = 4 * 1024 * 1024;
const XHTTP_SESSION_TIMEOUT_MS = 300000;
const XHTTP_RESPONSE_HEADERS = {
  'content-type': 'application/octet-stream',
  'cache-control': 'no-store',
  'x-accel-buffering': 'no',
  'access-control-allow-origin': '*',
  'access-control-allow-methods': 'GET, POST, OPTIONS'
};
const DB_SAVE_DEBOUNCE_MS = 5000;
const LOGIN_MAX_ATTEMPTS = 5;
const LOGIN_WINDOW_MS = 10 * 60 * 1000;
const LOGIN_LOCK_MS = 15 * 60 * 1000;
const ACTIVE_CONNECTION_STALE_MS = 60 * 1000;
const CONNECTION_CLEANUP_INTERVAL_MS = 500;
// Online client accounting: keep a client slot alive from socket/session heartbeat,
// not only from real traffic bytes. This prevents connected-but-idle VPN clients
// from disappearing in the panel while still removing truly stale slots.
const ACTIVE_CLIENT_SLOT_TTL_MS = Math.max(30000, Number(process.env.ACTIVE_CLIENT_SLOT_TTL_MS || 75000));
const DEVICE_DRAINING_MS = Math.max(500, Number(process.env.DEVICE_DRAINING_MS || 1200));
const DEVICE_PASSIVE_DRAIN_MS = Math.max(800, Number(process.env.DEVICE_PASSIVE_DRAIN_MS || 1800));
const DEVICE_CLUSTER_WINDOW_MS = Math.max(3000, Number(process.env.DEVICE_CLUSTER_WINDOW_MS || 30000));
const DEVICE_RECENT_WINDOW_MS = Math.max(3000, Number(process.env.DEVICE_RECENT_WINDOW_MS || 12000));
const DEVICE_STALE_MS = Math.max(30000, Number(process.env.DEVICE_STALE_MS || 90000));
// MDIE v3: Active Client is recent client-side activity/heartbeat, not raw open sockets.
const DEVICE_ACTIVITY_WINDOW_MS = Math.max(3000, Number(process.env.DEVICE_ACTIVITY_WINDOW_MS || 7000));
// Mahan Active Client Tracker v3: fast traffic/heartbeat based online counter.
const ACTIVE_CLIENT_TTL_MS = Math.max(1500, Number(process.env.ACTIVE_CLIENT_TTL_MS || 2500));
const WS_PING_INTERVAL_MS = 2000;

// Mahan Smart Booster: adaptive low-latency/throughput tuning on the same /ws and /xhttp paths.
// It does not change user configs; each connection is tuned after the VLESS target is parsed.
const SMART_BOOSTER_ENABLED = process.env.SMART_BOOSTER === '0' ? false : true;
const BOOSTER_DNS_TTL_MS = Math.max(5000, Number(process.env.BOOSTER_DNS_TTL_MS || 60000));
const BOOSTER_DNS_CACHE_MAX = Math.max(64, Number(process.env.BOOSTER_DNS_CACHE_MAX || 512));
const BOOSTER_DEST_MEMORY_MAX = Math.max(128, Number(process.env.BOOSTER_DEST_MEMORY_MAX || 2048));
const BOOSTER_BAD_ADDRESS_TTL_MS = Math.max(10000, Number(process.env.BOOSTER_BAD_ADDRESS_TTL_MS || 120000));
const BOOSTER_UDP_BURST_WINDOW_MS = Math.max(50, Number(process.env.BOOSTER_UDP_BURST_WINDOW_MS || 250));
const BOOSTER_UDP_BURST_PACKETS = Math.max(8, Number(process.env.BOOSTER_UDP_BURST_PACKETS || 80));
const BOOSTER_PROFILE_BALANCED = Object.freeze({
  name: 'balanced',
  wsBackpressureLimit: 4 * 1024 * 1024,
  wsBackpressureResume: 1 * 1024 * 1024,
  xhttpDownlinkBufferLimit: MAX_XHTTP_DOWNLINK_BUFFER,
  idleTimeoutMs: 180000,
  udpIdleTimeoutMs: 90000,
  connectTimeoutMs: CONNECT_TIMEOUT_MS,
  wsPingIntervalMs: 1500,
  autoSelectFamilyAttemptTimeout: 220,
  socketHighWaterMark: 64 * 1024,
  allowMux: true
});
const BOOSTER_PROFILE_LATENCY = Object.freeze({
  name: 'latency',
  wsBackpressureLimit: 512 * 1024,
  wsBackpressureResume: 128 * 1024,
  xhttpDownlinkBufferLimit: 1 * 1024 * 1024,
  idleTimeoutMs: 300000,
  udpIdleTimeoutMs: 180000,
  connectTimeoutMs: 10000,
  wsPingIntervalMs: 1500,
  autoSelectFamilyAttemptTimeout: 120,
  socketHighWaterMark: 16 * 1024,
  allowMux: false
});
const BOOSTER_PROFILE_SPEED = Object.freeze({
  name: 'speed',
  wsBackpressureLimit: 16 * 1024 * 1024,
  wsBackpressureResume: 4 * 1024 * 1024,
  xhttpDownlinkBufferLimit: 8 * 1024 * 1024,
  idleTimeoutMs: 300000,
  udpIdleTimeoutMs: 120000,
  connectTimeoutMs: CONNECT_TIMEOUT_MS,
  wsPingIntervalMs: 1500,
  autoSelectFamilyAttemptTimeout: 250,
  socketHighWaterMark: 256 * 1024,
  allowMux: true
});
const BOOSTER_PROFILE_STABLE = Object.freeze({
  name: 'stable',
  wsBackpressureLimit: 8 * 1024 * 1024,
  wsBackpressureResume: 2 * 1024 * 1024,
  xhttpDownlinkBufferLimit: 6 * 1024 * 1024,
  idleTimeoutMs: 600000,
  udpIdleTimeoutMs: 300000,
  connectTimeoutMs: 20000,
  wsPingIntervalMs: 1500,
  autoSelectFamilyAttemptTimeout: 300,
  socketHighWaterMark: 128 * 1024,
  allowMux: true
});

let DB = { admin: null, users: [] };
let dbSaveTimer = null;
let lastCpuUsage = process.cpuUsage();
let lastCpuCheck = process.hrtime.bigint();
let cachedCpuUsage = 0;
let trafficCurrentMinuteBytes = 0;
let trafficHistory = [];
const pendingTrafficBytes = new Map(); // user.id -> total bytes waiting for batched commit
const pendingUplinkBytes = new Map(); // user.id -> upload/uplink bytes waiting for commit
const pendingDownlinkBytes = new Map(); // user.id -> download/downlink bytes waiting for commit
const pendingLastSeen = new Map(); // user.id -> latest timestamp waiting for batched commit
let selfPingHealthUrl = '';
let lastSelfPingUrlLog = '';
const activeSessions = new Set();
const loginAttempts = new Map(); // ip -> { count, firstAt, lockedUntil }
const activeConnections = new Map(); // user.id -> Map(connectionId, { ip, clientKey, kind, closeFn, lastSeen })
const clientSlots = new Map(); // legacy placeholder; active client accounting is handled by Mahan Device Intelligence Engine.
const activeDevices = new Map(); // kept for endpoint correlation; active count comes from activeClientActivity.
const activeClientActivity = new Map(); // user.id -> Map(endpointKey, { lastActivity, ip, uaFamily, transports })
const adminEventClients = new Set();
const subEventClients = new Map(); // sub_id -> Set(res)
const boosterDnsCache = new Map(); // hostname -> { expiresAt, addresses, next }
const boosterDestMemory = new Map(); // host:port -> adaptive performance memory
let realtimeBroadcastTimer = null;

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

function sampleCpuUsage() {
  const currentUsage = process.cpuUsage();
  const currentTime = process.hrtime.bigint();
  const elapsedMicros = Number(currentTime - lastCpuCheck) / 1000;
  const usedMicros = (currentUsage.user - lastCpuUsage.user) + (currentUsage.system - lastCpuUsage.system);

  lastCpuUsage = currentUsage;
  lastCpuCheck = currentTime;

  if (!elapsedMicros || elapsedMicros <= 0) return cachedCpuUsage;

  const rawPercent = Math.max(0, Math.min(100, (usedMicros / elapsedMicros) * 100));
  // Smooth short page-refresh/build spikes so the dashboard shows stable process load.
  cachedCpuUsage = Number(((cachedCpuUsage * 0.65) + (rawPercent * 0.35)).toFixed(1));
  return cachedCpuUsage;
}

function getCpuUsagePercent() {
  return cachedCpuUsage;
}

function parseUserNumber(value, fallback = 0) {
  if (value === null || value === undefined) return fallback;
  const fa = '۰۱۲۳۴۵۶۷۸۹';
  const ar = '٠١٢٣٤٥٦٧٨٩';
  let s = String(value).trim();
  s = s.replace(/[۰-۹]/g, (d) => String(fa.indexOf(d)));
  s = s.replace(/[٠-٩]/g, (d) => String(ar.indexOf(d)));
  s = s.replace(/,/g, '.');
  s = s.replace(/[^0-9.+-]/g, '');
  const n = Number(s);
  return Number.isFinite(n) ? n : fallback;
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
    connection_limit: Number(u.connection_limit || u.ip_limit || 0),
    used_bytes: Number(u.used_bytes || 0),
    uplink_bytes: Number(u.uplink_bytes || 0),
    downlink_bytes: Number(u.downlink_bytes !== undefined ? u.downlink_bytes : (u.used_bytes || 0)),
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

function getPendingTraffic(userId) {
  return Number(pendingTrafficBytes.get(userId) || 0);
}

function getPendingUplink(userId) {
  return Number(pendingUplinkBytes.get(userId) || 0);
}

function getPendingDownlink(userId) {
  return Number(pendingDownlinkBytes.get(userId) || 0);
}

function getEffectiveUsedBytes(user) {
  if (!user) return 0;
  return Number(user.used_bytes || 0) + getPendingTraffic(user.id);
}

function getEffectiveUplinkBytes(user) {
  if (!user) return 0;
  return Number(user.uplink_bytes || 0) + getPendingUplink(user.id);
}

function getEffectiveDownlinkBytes(user) {
  if (!user) return 0;
  return Number(user.downlink_bytes !== undefined ? user.downlink_bytes : (user.used_bytes || 0)) + getPendingDownlink(user.id);
}

function getEffectiveLastSeen(user) {
  if (!user) return 0;
  return Math.max(Number(user.last_seen || 0), Number(pendingLastSeen.get(user.id) || 0));
}

function isUserExpired(user) {
  if (!user) return true;
  const now = Date.now();
  const trafficExceeded = user.traffic_limit > 0 && user.used_bytes >= user.traffic_limit;
  const timeExceeded = user.expiry_time > 0 && now > user.expiry_time;
  return trafficExceeded || timeExceeded;
}

function isUserExpiredEffective(user) {
  if (!user) return true;
  const now = Date.now();
  const trafficExceeded = user.traffic_limit > 0 && getEffectiveUsedBytes(user) >= user.traffic_limit;
  const timeExceeded = user.expiry_time > 0 && now > user.expiry_time;
  return trafficExceeded || timeExceeded;
}

function getUserStatus(user) {
  if (!user || user.is_active !== 1) return 'Inactive';
  if (isUserExpiredEffective(user)) return 'Expired';
  return 'Active';
}

function flushTrafficBuffers() {
  if (pendingTrafficBytes.size === 0 && pendingUplinkBytes.size === 0 && pendingDownlinkBytes.size === 0 && pendingLastSeen.size === 0) return false;

  let changed = false;
  const ids = new Set([...pendingTrafficBytes.keys(), ...pendingUplinkBytes.keys(), ...pendingDownlinkBytes.keys(), ...pendingLastSeen.keys()]);

  for (const userId of ids) {
    const user = DB.users.find((u) => u.id === userId);
    const bytes = Number(pendingTrafficBytes.get(userId) || 0);
    const uplink = Number(pendingUplinkBytes.get(userId) || 0);
    const downlink = Number(pendingDownlinkBytes.get(userId) || 0);
    const seen = Number(pendingLastSeen.get(userId) || 0);

    pendingTrafficBytes.delete(userId);
    pendingUplinkBytes.delete(userId);
    pendingDownlinkBytes.delete(userId);
    pendingLastSeen.delete(userId);

    if (!user) continue;
    if (bytes > 0) {
      user.used_bytes = Number(user.used_bytes || 0) + bytes;
      user.uplink_bytes = Number(user.uplink_bytes || 0) + uplink;
      user.downlink_bytes = Number(user.downlink_bytes !== undefined ? user.downlink_bytes : 0) + downlink;
      changed = true;
    }
    if (seen > Number(user.last_seen || 0)) {
      user.last_seen = seen;
      changed = true;
    }

    if (user.is_active === 1 && isUserExpired(user)) {
      user.is_active = 0;
      log('[QUOTA] Limit reached:', `${user.remark} total=${formatBytes(user.used_bytes)}`);
      closeUserConnections(user.id, 'quota exceeded');
      changed = true;
    }
  }

  if (changed) {
    scheduleRealtimeBroadcast();
    scheduleSaveDB(false);
  }
  return changed;
}

function getActiveUserByUuid(uuidBytes) {
  const uuidHex = uuidBytes.toString('hex');
  const user = DB.users.find((u) => String(u.uuid).replace(/-/g, '').toLowerCase() === uuidHex);
  if (!user) return null;
  if (user.is_active !== 1 || isUserExpiredEffective(user)) return null;
  return user;
}

function removeConnection(userId, connectionId, reason = 'removed') {
  const map = activeConnections.get(userId);
  if (!map) return false;
  const conn = map.get(connectionId);
  const existed = map.delete(connectionId);
  if (map.size === 0) activeConnections.delete(userId);
  if (existed) {
    releaseClientSlotIfUnused(userId, conn?.clientKey, connectionId);
    scheduleRealtimeBroadcast();
  }
  return existed;
}

function pruneStaleConnections(userId, maxAge = ACTIVE_CONNECTION_STALE_MS) {
  const map = activeConnections.get(userId);
  if (!map) return 0;

  const now = Date.now();
  let removed = 0;
  for (const conn of Array.from(map.values())) {
    if (now - conn.lastSeen > maxAge) {
      try { conn.closeFn('connection stale'); } catch (_) {}
      if (removeConnection(userId, conn.id, 'stale')) removed += 1;
    }
  }
  return removed;
}

function getActiveIpSet(userId) {
  pruneStaleConnections(userId, ACTIVE_CONNECTION_STALE_MS);
  const map = activeConnections.get(userId);
  if (!map) return new Set();
  return new Set(Array.from(map.values()).map((conn) => conn.ip).filter(Boolean));
}

function getActiveSessionCount(userId) {
  pruneStaleConnections(userId, ACTIVE_CONNECTION_STALE_MS);
  return activeConnections.get(userId)?.size || 0;
}

function shortHash(value, len = 16) {
  return crypto.createHash('sha256').update(String(value || '')).digest('hex').slice(0, len);
}

function normalizeUserAgent(value) {
  return String(value || '').toLowerCase().replace(/\s+/g, ' ').slice(0, 240);
}

function getUserAgentFamily(ua) {
  const v = normalizeUserAgent(ua);
  if (!v) return 'unknown';
  const families = [
    'hiddify', 'v2rayng', 'v2rayn', 'nekobox', 'nekoray', 'sing-box', 'singbox', 'shadowrocket',
    'streisand', 'foxray', 'sagernet', 'sager', 'clash', 'mihomo', 'surge', 'quantumult', 'loon',
    'curl', 'go-http-client', 'okhttp', 'python-requests'
  ];
  return families.find((x) => v.includes(x)) || v.split(/[\/\s(;]+/).filter(Boolean)[0] || 'unknown';
}

function getIpGroup(ip) {
  const h = normalizeIpAddress(ip);
  if (!h || h === 'unknown') return 'unknown';
  if (net.isIP(h) === 4) {
    const parts = h.split('.');
    return parts.length === 4 ? `${parts[0]}.${parts[1]}.${parts[2]}.0/24` : h;
  }
  if (net.isIP(h) === 6) {
    const clean = h.toLowerCase();
    const expanded = clean.includes('::') ? clean : clean;
    return expanded.split(':').slice(0, 4).join(':') + '::/64';
  }
  return h;
}

function getConnectionDeviceMeta(req, ip, transport = 'unknown', target = {}) {
  const ua = String(req?.headers?.['user-agent'] || req?.headers?.['sec-ch-ua'] || '').slice(0, 300);
  const normalizedIp = normalizeIpAddress(ip || getClientIp(req) || 'unknown') || 'unknown';
  const ipGroup = getIpGroup(normalizedIp);
  const uaNorm = normalizeUserAgent(ua);
  const uaFamily = getUserAgentFamily(uaNorm);
  const uaHash = shortHash(uaNorm || uaFamily || 'unknown', 12);
  const command = Number(target.command || 0);
  const host = String(target.host || '').toLowerCase();
  const port = Number(target.port || 0);
  const category = detectDestinationCategory({ command, host, port });
  const role = String(target.role || 'active');
  const identityBase = uaNorm
    ? `${normalizedIp}|${uaHash}`
    : `${normalizedIp}|${uaFamily || 'unknown'}`;
  return {
    ip: normalizedIp,
    ipGroup,
    ua: uaNorm,
    uaHash,
    uaFamily,
    transport: String(transport || 'unknown'),
    command,
    host,
    port,
    category,
    role,
    endpointKey: `ep_${shortHash(identityBase, 18)}`,
    createdAt: Date.now()
  };
}

function getDeviceMap(userId, create = true) {
  let map = activeDevices.get(userId);
  if (!map && create) {
    map = new Map();
    activeDevices.set(userId, map);
  }
  return map || null;
}

function getActivityMap(userId, create = true) {
  let map = activeClientActivity.get(userId);
  if (!map && create) {
    map = new Map();
    activeClientActivity.set(userId, map);
  }
  return map || null;
}

function touchActiveClient(userId, endpointKey, meta = {}) {
  if (!userId || !endpointKey) return;
  const map = getActivityMap(userId, true);
  const now = Date.now();
  const existing = map.get(endpointKey) || {
    firstActivity: now,
    lastActivity: 0,
    ip: meta.ip || 'unknown',
    ipGroup: meta.ipGroup || 'unknown',
    uaFamily: meta.uaFamily || 'unknown',
    transports: new Set()
  };
  existing.lastActivity = now;
  if (meta.ip) existing.ip = meta.ip;
  if (meta.ipGroup) existing.ipGroup = meta.ipGroup;
  if (meta.uaFamily) existing.uaFamily = meta.uaFamily;
  if (meta.transport) existing.transports.add(meta.transport);
  map.set(endpointKey, existing);
}

function pruneActiveClientActivity(userId) {
  const map = getActivityMap(userId, false);
  if (!map) return 0;
  const now = Date.now();
  let removed = 0;
  for (const [key, item] of Array.from(map.entries())) {
    if (now - Number(item.lastActivity || 0) > ACTIVE_CLIENT_TTL_MS) {
      map.delete(key);
      removed += 1;
    }
  }
  if (map.size === 0) activeClientActivity.delete(userId);
  if (removed) scheduleRealtimeBroadcast();
  return removed;
}

function getActiveClientActivityCount(userId) {
  pruneActiveClientActivity(userId);
  return getActivityMap(userId, false)?.size || 0;
}

function scoreDeviceCandidate(device, meta, now = Date.now()) {
  if (!device || !meta) return -999;
  let score = 0;
  const age = now - Number(device.lastSeen || device.firstSeen || 0);
  const sinceFirst = now - Number(device.firstSeen || now);
  const exactIp = device.ipSet?.has(meta.ip);
  const groupIp = device.ipGroupSet?.has(meta.ipGroup);
  const sameUa = device.uaHash && meta.uaHash && device.uaHash === meta.uaHash;
  const sameUaFamily = device.uaFamily && meta.uaFamily && device.uaFamily === meta.uaFamily;
  const unknownUaPair = (!device.uaFamily || device.uaFamily === 'unknown' || !meta.uaFamily || meta.uaFamily === 'unknown');
  const inClusterWindow = age < DEVICE_CLUSTER_WINDOW_MS;

  // Endpoint Cluster Lock: multiple WS/XHTTP pipes/probes from the same endpoint burst
  // are almost always one physical client, even when one request has missing/slightly
  // different UA. This prevents one desktop/mobile app from becoming 2-4 clients.
  if (inClusterWindow && (exactIp || groupIp) && (sameUa || sameUaFamily || unknownUaPair)) score += 42;

  if (exactIp) score += 38;
  else if (groupIp) score += 20;
  else score -= 14;

  if (sameUa) score += 38;
  else if (sameUaFamily && meta.uaFamily !== 'unknown') score += 18;
  else if (device.uaFamily !== 'unknown' && meta.uaFamily !== 'unknown') score -= 34;

  if (device.transportSet?.has(meta.transport)) score += 8;
  if (device.categorySet?.has(meta.category)) score += 8;
  if (device.state === 'draining') score += 28;
  if (age < 3000) score += 20;
  else if (age < DEVICE_RECENT_WINDOW_MS) score += 12;
  else if (age > DEVICE_STALE_MS) score -= 50;

  // A burst of multiple streams during the first seconds is usually one app opening WS/XHTTP pipes.
  if (sinceFirst < 8000 && (exactIp || groupIp) && (sameUa || sameUaFamily)) score += 14;

  // If exact same UA/IP has active connections, strongly prefer grouping.
  if ((device.connections?.size || 0) > 0 && exactIp && sameUa) score += 20;
  return score;
}

function findBestDeviceCandidate(userId, meta) {
  const map = getDeviceMap(userId, false);
  if (!map || !meta) return null;
  const now = Date.now();
  let best = null;
  for (const device of map.values()) {
    if (device.state === 'offline') continue;
    const score = scoreDeviceCandidate(device, meta, now);
    if (!best || score > best.score) best = { device, score };
  }
  if (!best) return null;
  const threshold = best.device.state === 'draining' ? 58 : 72;
  return best.score >= threshold ? best : null;
}

function createDeviceState(userId, meta) {
  const now = Date.now();
  const id = meta?.endpointKey || `dev_${crypto.randomBytes(8).toString('hex')}`;
  return {
    id,
    userId,
    state: 'online',
    connections: new Set(),
    anchorConnections: new Set(),
    passiveConnections: new Set(),
    connectionRoles: new Map(),
    ipSet: new Set(meta?.ip ? [meta.ip] : []),
    ipGroupSet: new Set(meta?.ipGroup ? [meta.ipGroup] : []),
    uaHash: meta?.uaHash || 'unknown',
    uaFamily: meta?.uaFamily || 'unknown',
    transportSet: new Set(meta?.transport ? [meta.transport] : []),
    categorySet: new Set(meta?.category ? [meta.category] : []),
    recentDestinations: meta?.host ? [{ host: meta.host, port: meta.port, ts: now }] : [],
    firstSeen: now,
    lastSeen: now,
    lastAnchorSeen: meta?.role === 'passive' ? 0 : now,
    passiveOnlySince: 0,
    lastTrafficAt: 0,
    confidence: 1,
    closeTimer: null
  };
}

function updateDeviceState(device, meta, connectionId = '') {
  if (!device || !meta) return;
  const now = Date.now();
  clearTimeout(device.closeTimer);
  device.closeTimer = null;
  device.state = 'online';
  device.lastSeen = now;
  const role = meta.role === 'passive' ? 'passive' : 'active';
  if (connectionId) {
    device.connections.add(connectionId);
    device.connectionRoles.set(connectionId, role);
    if (role === 'passive') device.passiveConnections.add(connectionId);
    else {
      device.anchorConnections.add(connectionId);
      device.lastAnchorSeen = now;
      device.passiveOnlySince = 0;
    }
  }
  if (meta.ip) device.ipSet.add(meta.ip);
  if (meta.ipGroup) device.ipGroupSet.add(meta.ipGroup);
  if (meta.uaHash && device.uaHash === 'unknown') device.uaHash = meta.uaHash;
  if (meta.uaFamily && device.uaFamily === 'unknown') device.uaFamily = meta.uaFamily;
  if (meta.transport) device.transportSet.add(meta.transport);
  if (meta.category) device.categorySet.add(meta.category);
  if (meta.host) {
    device.recentDestinations.unshift({ host: meta.host, port: meta.port, ts: now });
    device.recentDestinations = device.recentDestinations.slice(0, 10);
  }
}

function attachDeviceConnection(userId, connectionId, meta = {}, preferredDeviceId = '') {
  const map = getDeviceMap(userId, true);
  const stableKey = preferredDeviceId || meta?.endpointKey || '';
  let device = stableKey ? map.get(stableKey) : null;
  if (!device && stableKey && meta?.endpointKey) {
    device = createDeviceState(userId, meta);
    map.set(device.id, device);
  }
  if (!device) device = findBestDeviceCandidate(userId, meta)?.device || null;
  if (!device) {
    device = createDeviceState(userId, meta);
    map.set(device.id, device);
  }
  updateDeviceState(device, meta, connectionId);
  scheduleRealtimeBroadcast();
  return device.id;
}

function refreshClientSlot(userId, clientKey, traffic = false) {
  if (!clientKey) return;
  const device = getDeviceMap(userId, false)?.get(clientKey);
  if (!device) return;
  const now = Date.now();
  device.lastSeen = now;
  if (traffic) {
    device.lastTrafficAt = now;
    if ((device.anchorConnections?.size || 0) > 0) device.lastAnchorSeen = now;
  }
  if (device.state === 'draining' && ((device.anchorConnections?.size || 0) > 0)) {
    clearTimeout(device.closeTimer);
    device.closeTimer = null;
    device.state = 'online';
    device.passiveOnlySince = 0;
    scheduleRealtimeBroadcast();
  }
}

function pruneClientSlots(userId) {
  const map = getDeviceMap(userId, false);
  if (!map) return 0;
  const now = Date.now();
  let removed = 0;
  for (const [id, device] of Array.from(map.entries())) {
    const anchors = device.anchorConnections?.size || 0;
    const passives = device.passiveConnections?.size || 0;
    if (anchors === 0 && passives > 0 && device.state !== 'draining') {
      if (!device.passiveOnlySince) device.passiveOnlySince = now;
      if (now - Number(device.passiveOnlySince || now) > DEVICE_PASSIVE_DRAIN_MS) {
        scheduleDeviceOffline(userId, id, 1);
      }
      continue;
    }
    if ((device.connections?.size || 0) === 0 && device.state !== 'draining') {
      if (now - Number(device.lastSeen || 0) > DEVICE_DRAINING_MS) {
        clearTimeout(device.closeTimer);
        map.delete(id);
        removed += 1;
      }
      continue;
    }
    if (now - Number(device.lastSeen || 0) > DEVICE_STALE_MS) {
      clearTimeout(device.closeTimer);
      map.delete(id);
      removed += 1;
    }
  }
  if (map.size === 0) activeDevices.delete(userId);
  if (removed) scheduleRealtimeBroadcast();
  return removed;
}

function getClientSlotCount(userId) {
  // Active Client v3: count only endpoints with very recent real activity or WS pong.
  // This intentionally ignores long-open/stuck sockets so dashboard changes within ~2-3s.
  return getActiveClientActivityCount(userId);
}

function makeClientKey(ip) {
  return `ip:${normalizeIpAddress(ip) || 'unknown'}`;
}

function canAcceptUserSession(user, clientKey) {
  const limit = Number(user.connection_limit || user.ip_limit || 0);
  if (!limit || limit <= 0) return true;
  pruneActiveClientActivity(user.id);
  if (clientKey && getActivityMap(user.id, false)?.has(clientKey)) return true;
  return getActiveClientActivityCount(user.id) < limit;
}

function scheduleDeviceOffline(userId, clientKey, delayMs) {
  const device = getDeviceMap(userId, false)?.get(clientKey);
  if (!device) return;
  clearTimeout(device.closeTimer);
  device.state = 'draining';
  device.closeTimer = setTimeout(() => {
    const current = getDeviceMap(userId, false)?.get(clientKey);
    if (!current) return;
    if ((current.anchorConnections?.size || 0) > 0) {
      current.state = 'online';
      current.closeTimer = null;
      return;
    }
    const currentMap = getDeviceMap(userId, false);
    currentMap?.delete(clientKey);
    if (currentMap && currentMap.size === 0) activeDevices.delete(userId);
    scheduleRealtimeBroadcast();
  }, delayMs);
  scheduleRealtimeBroadcast();
}

function releaseClientSlotIfUnused(userId, clientKey, connectionId) {
  if (!clientKey) return;
  const map = getDeviceMap(userId, false);
  const device = map?.get(clientKey);
  if (!device) return;
  if (connectionId) {
    device.connections.delete(connectionId);
    const role = device.connectionRoles?.get(connectionId) || 'active';
    device.connectionRoles?.delete(connectionId);
    if (role === 'passive') device.passiveConnections?.delete(connectionId);
    else device.anchorConnections?.delete(connectionId);
  }
  const now = Date.now();
  device.lastSeen = now;
  const anchors = device.anchorConnections?.size || 0;
  const passives = device.passiveConnections?.size || 0;
  if (anchors === 0) {
    if (!device.passiveOnlySince) device.passiveOnlySince = now;
    scheduleDeviceOffline(userId, clientKey, passives > 0 ? DEVICE_PASSIVE_DRAIN_MS : DEVICE_DRAINING_MS);
    return;
  }
  device.state = 'online';
  device.passiveOnlySince = 0;
  scheduleRealtimeBroadcast();
}

function registerConnection(userId, closeFn, meta = {}) {
  if (!activeConnections.has(userId)) activeConnections.set(userId, new Map());
  const map = activeConnections.get(userId);
  const id = crypto.randomBytes(8).toString('hex');
  const deviceMeta = meta.deviceMeta || {
    ip: normalizeIpAddress(meta.ip || 'unknown') || 'unknown',
    ipGroup: getIpGroup(meta.ip || 'unknown'),
    ua: '',
    uaHash: 'unknown',
    uaFamily: 'unknown',
    transport: String(meta.kind || 'unknown'),
    command: 0,
    host: '',
    port: 0,
    category: 'general',
    createdAt: Date.now()
  };
  const deviceKey = attachDeviceConnection(userId, id, deviceMeta, meta.clientKey || deviceMeta.endpointKey || '');
  touchActiveClient(userId, deviceKey, deviceMeta);
  const conn = {
    id,
    ip: String(deviceMeta.ip || meta.ip || 'unknown'),
    clientKey: deviceKey,
    kind: String(meta.kind || deviceMeta.transport || 'unknown'),
    closeFn,
    createdAt: Date.now(),
    lastSeen: Date.now(),
    lastDataAt: 0
  };
  map.set(id, conn);
  scheduleRealtimeBroadcast();

  const unregister = () => {
    removeConnection(userId, id, 'unregister');
  };
  unregister.touch = () => {
    const current = activeConnections.get(userId);
    const item = current?.get(id);
    if (item) {
      const now = Date.now();
      // Heartbeat/session liveness renews the online client slot. This makes
      // Active Client / Client Limit reflect real connection lifecycle, even
      // when the VPN is connected but no user traffic is currently flowing.
      item.lastSeen = now;
      refreshClientSlot(userId, item.clientKey);
      touchActiveClient(userId, item.clientKey, { ip: item.ip, transport: item.kind });
    }
  };
  unregister.touchData = () => {
    const current = activeConnections.get(userId);
    const item = current?.get(id);
    if (item) {
      const now = Date.now();
      item.lastSeen = now;
      item.lastDataAt = now;
      refreshClientSlot(userId, item.clientKey, true);
      touchActiveClient(userId, item.clientKey, { ip: item.ip, transport: item.kind });
    }
  };
  unregister.id = id;
  unregister.clientKey = conn.clientKey;
  return unregister;
}

function closeUserConnections(userId, reason = 'user disabled') {
  const map = activeConnections.get(userId);
  if (!map) return;
  for (const conn of Array.from(map.values())) {
    try { conn.closeFn(reason); } catch (_) {}
    removeConnection(userId, conn.id, reason);
  }
}

function forceClearUserConnections(userId, reason = 'connections cleared') {
  const map = activeConnections.get(userId);
  if (map) {
    for (const conn of Array.from(map.values())) {
      try { conn.closeFn(reason); } catch (_) {}
    }
  }
  activeConnections.delete(userId);
  const deviceMap = activeDevices.get(userId);
  if (deviceMap) {
    for (const device of deviceMap.values()) clearTimeout(device.closeTimer);
  }
  activeDevices.delete(userId);
  activeClientActivity.delete(userId);
  scheduleRealtimeBroadcast();
}

function markUserDataActivity(userId) {
  const map = activeConnections.get(userId);
  if (!map) return;
  const now = Date.now();
  for (const conn of map.values()) {
    conn.lastSeen = now;
    conn.lastDataAt = now;
    refreshClientSlot(userId, conn.clientKey, true);
    touchActiveClient(userId, conn.clientKey, { ip: conn.ip, transport: conn.kind });
  }
}

function updateTraffic(user, bytes, direction = 'total', touchActivity = null) {
  if (!user || !bytes || bytes <= 0) return;

  // Touch only the connection/session that actually moved bytes. Previously we
  // refreshed every connection for the user, which could keep disconnected
  // client slots alive while another device was still passing traffic.
  if (typeof touchActivity === 'function') touchActivity();
  else markUserDataActivity(user.id);

  // Fast data-plane path: only increment in-memory counters here.
  // Expensive UI/DB/quota work is committed in a 1s batch by flushTrafficBuffers().
  trafficCurrentMinuteBytes += bytes;
  pendingTrafficBytes.set(user.id, getPendingTraffic(user.id) + bytes);
  if (direction === 'uplink') pendingUplinkBytes.set(user.id, getPendingUplink(user.id) + bytes);
  else if (direction === 'downlink') pendingDownlinkBytes.set(user.id, getPendingDownlink(user.id) + bytes);
  pendingLastSeen.set(user.id, Date.now());

  // Enforce quota promptly when the buffered total crosses the limit.
  if (user.traffic_limit > 0 && getEffectiveUsedBytes(user) >= user.traffic_limit) {
    flushTrafficBuffers();
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

function getBoosterProfile(profileName) {
  if (!SMART_BOOSTER_ENABLED) return BOOSTER_PROFILE_BALANCED;
  if (profileName === 'latency') return BOOSTER_PROFILE_LATENCY;
  if (profileName === 'speed') return BOOSTER_PROFILE_SPEED;
  if (profileName === 'stable') return BOOSTER_PROFILE_STABLE;
  return BOOSTER_PROFILE_BALANCED;
}

function getDestinationKey(host, port) {
  const h = String(host || '').trim().toLowerCase();
  return `${h}:${Number(port || 0)}`;
}

function getDestinationMemory(host, port, create = true) {
  const key = getDestinationKey(host, port);
  let mem = boosterDestMemory.get(key);
  if (!mem && create) {
    mem = {
      key,
      host: String(host || '').trim().toLowerCase(),
      port: Number(port || 0),
      success: 0,
      errors: 0,
      connectAvgMs: 0,
      preferredFamily: 0,
      category: 'general',
      lastProfile: 'balanced',
      badAddresses: new Map(),
      lastSeen: Date.now()
    };
    boosterDestMemory.set(key, mem);
    trimDestinationMemory();
  }
  if (mem) mem.lastSeen = Date.now();
  return mem || null;
}

function trimDestinationMemory() {
  const now = Date.now();
  for (const [key, mem] of Array.from(boosterDestMemory.entries())) {
    if (!mem || now - Number(mem.lastSeen || 0) > 6 * 60 * 60 * 1000) boosterDestMemory.delete(key);
  }
  while (boosterDestMemory.size > BOOSTER_DEST_MEMORY_MAX) {
    let oldestKey = '';
    let oldestSeen = Infinity;
    for (const [key, mem] of boosterDestMemory.entries()) {
      const seen = Number(mem.lastSeen || 0);
      if (seen < oldestSeen) { oldestSeen = seen; oldestKey = key; }
    }
    if (!oldestKey) break;
    boosterDestMemory.delete(oldestKey);
  }
}

function cleanBadAddresses(mem) {
  if (!mem || !mem.badAddresses) return;
  const now = Date.now();
  for (const [address, until] of Array.from(mem.badAddresses.entries())) {
    if (Number(until || 0) <= now) mem.badAddresses.delete(address);
  }
}

function isAddressTemporarilyBad(mem, address) {
  if (!mem || !address) return false;
  cleanBadAddresses(mem);
  return Number(mem.badAddresses?.get(address) || 0) > Date.now();
}

function markBoosterConnectSuccess(host, port, address, elapsedMs) {
  if (!SMART_BOOSTER_ENABLED) return;
  const mem = getDestinationMemory(host, port, true);
  if (!mem) return;
  mem.success += 1;
  mem.lastSeen = Date.now();
  mem.errors = Math.max(0, Number(mem.errors || 0) - 1);
  const elapsed = Math.max(1, Number(elapsedMs || 0));
  mem.connectAvgMs = mem.connectAvgMs ? Math.round((mem.connectAvgMs * 0.75) + (elapsed * 0.25)) : elapsed;
  const family = net.isIP(address);
  if (family === 4 || family === 6) mem.preferredFamily = family;
  if (address && mem.badAddresses) mem.badAddresses.delete(address);
}

function markBoosterConnectFailure(host, port, address = '', err = null) {
  if (!SMART_BOOSTER_ENABLED) return;
  const mem = getDestinationMemory(host, port, true);
  if (!mem) return;
  mem.errors += 1;
  mem.lastSeen = Date.now();
  if (address && mem.badAddresses) mem.badAddresses.set(address, Date.now() + BOOSTER_BAD_ADDRESS_TTL_MS);
  if (mem.errors >= 3) {
    // Smart failure recovery: force a fresh DNS answer after repeated failures.
    boosterDnsCache.delete(String(host || '').trim().toLowerCase());
  }
}

function detectDestinationCategory(requestOrTarget) {
  const command = Number(requestOrTarget?.command || 0);
  const port = Number(requestOrTarget?.port || 0);
  const host = String(requestOrTarget?.host || '').toLowerCase();

  if (command === 0x02) return 'game';
  if (command === 0x03) return 'mux';
  if ([53, 123, 500, 4500, 3478, 3479, 3480, 3074, 27015, 27031, 27036, 3659, 5222, 5223].includes(port)) return 'game';
  if (/(game|steam|xbox|playstation|psn|epicgames|riot|valorant|pubg|callofduty|activision|ea\.com|easports|fortnite|garena|roblox|ubisoft|battle\.net)/i.test(host)) return 'game';
  if (/(googlevideo|youtube|ytimg|netflix|nflxvideo|cloudfront|akamai|fastly|cdn|video|media|download|telegram|whatsapp|instagram|tiktok)/i.test(host)) return 'download';
  if (port === 80 || port === 443 || port === 8080 || port === 8443) return 'web';
  return 'general';
}

function chooseBoosterProfile(request, transport = 'ws') {
  if (!SMART_BOOSTER_ENABLED || !request) return BOOSTER_PROFILE_BALANCED;
  const category = detectDestinationCategory(request);
  const mem = getDestinationMemory(request.host, request.port, true);
  if (mem) {
    mem.category = category;
  }

  let profile = BOOSTER_PROFILE_BALANCED;
  if (category === 'game') profile = BOOSTER_PROFILE_LATENCY;
  else if (category === 'download' || category === 'web') profile = BOOSTER_PROFILE_SPEED;
  else if (category === 'mux') profile = transport === 'xhttp' ? BOOSTER_PROFILE_STABLE : BOOSTER_PROFILE_BALANCED;

  // Per-destination memory: repeated failures prefer stability over raw speed.
  if (mem && Number(mem.errors || 0) >= 2 && profile.name === 'speed') profile = BOOSTER_PROFILE_STABLE;
  if (mem && Number(mem.errors || 0) >= 4) profile = BOOSTER_PROFILE_STABLE;
  if (mem) mem.lastProfile = profile.name;
  return profile;
}

function describeBoosterProfile(profile) {
  return SMART_BOOSTER_ENABLED ? `booster=${profile?.name || 'balanced'}` : 'booster=off';
}

function trimBoosterDnsCache() {
  if (boosterDnsCache.size <= BOOSTER_DNS_CACHE_MAX) return;
  const now = Date.now();
  for (const [key, entry] of Array.from(boosterDnsCache.entries())) {
    if (!entry || entry.expiresAt <= now) boosterDnsCache.delete(key);
  }
  while (boosterDnsCache.size > BOOSTER_DNS_CACHE_MAX) {
    const first = boosterDnsCache.keys().next().value;
    if (!first) break;
    boosterDnsCache.delete(first);
  }
}

function sortBoosterAddresses(addresses, profile, family = 0, host = '', port = 0) {
  const mem = getDestinationMemory(host, port, false);
  cleanBadAddresses(mem);
  const source = (Array.isArray(addresses) ? addresses.slice() : []).filter((a) => a && a.address);
  let list = source.filter((a) => !isAddressTemporarilyBad(mem, a.address));
  if (!list.length) list = source;
  const wantedFamily = Number(family || 0);
  const preferredFamily = wantedFamily || Number(mem?.preferredFamily || 0);

  if (preferredFamily === 4 || preferredFamily === 6) {
    list.sort((a, b) => (a.family === preferredFamily ? 0 : 1) - (b.family === preferredFamily ? 0 : 1));
  } else if (profile?.name === 'latency') {
    // Latency mode prefers IPv4 first on most PaaS networks; speed/stable keep resolver order.
    list.sort((a, b) => (a.family === 4 ? 0 : 1) - (b.family === 4 ? 0 : 1));
  }

  // Warm memory: if a family has a good average connect time, keep it first.
  if (mem?.connectAvgMs && mem.preferredFamily) {
    list.sort((a, b) => (a.family === mem.preferredFamily ? 0 : 1) - (b.family === mem.preferredFamily ? 0 : 1));
  }
  return list;
}

function getCachedBoosterAddresses(hostname) {
  const key = String(hostname || '').toLowerCase();
  const entry = boosterDnsCache.get(key);
  if (!entry || entry.expiresAt <= Date.now() || !Array.isArray(entry.addresses) || !entry.addresses.length) {
    boosterDnsCache.delete(key);
    return null;
  }
  return entry;
}

function setCachedBoosterAddresses(hostname, addresses) {
  const key = String(hostname || '').toLowerCase();
  boosterDnsCache.set(key, { hostname: key, expiresAt: Date.now() + BOOSTER_DNS_TTL_MS, addresses, next: 0 });
  trimBoosterDnsCache();
  return boosterDnsCache.get(key);
}

function selectBoosterAddress(entry, profile, family = 0, port = 0) {
  const sorted = sortBoosterAddresses(entry.addresses, profile, family, entry.hostname, port);
  if (!sorted.length) return null;
  const idx = Math.abs(Number(entry.next || 0)) % sorted.length;
  entry.next = idx + 1;
  return sorted[idx];
}

function makeSmartDnsLookup(port, profile) {
  return (hostname, options, callback) => {
    const cb = typeof callback === 'function' ? callback : options;
    const opts = typeof options === 'object' && options ? options : {};
    const host = String(hostname || '').trim();
    const family = Number(opts.family || 0);
    const directFamily = net.isIP(host);

    if (directFamily) {
      if (isForbiddenTarget(host, port) || isPrivateOrProxyIp(host)) return cb(new Error(`forbidden resolved target: ${host}:${port}`));
      if (opts.all) return cb(null, [{ address: host, family: directFamily }]);
      return cb(null, host, directFamily);
    }

    const cached = getCachedBoosterAddresses(host);
    if (cached) {
      const addresses = sortBoosterAddresses(cached.addresses, profile, family, host, port);
      if (opts.all) return cb(null, addresses);
      const selected = selectBoosterAddress(cached, profile, family, port);
      if (selected) return cb(null, selected.address, selected.family);
    }

    dns.lookup(host, { all: true, verbatim: false }).then((records) => {
      const safe = records
        .map((r) => ({ address: normalizeIpAddress(r.address), family: Number(r.family || net.isIP(r.address)) }))
        .filter((r) => r.address && r.family && !isForbiddenTarget(r.address, port) && !isPrivateOrProxyIp(r.address));

      if (!safe.length) throw new Error(`no public address for ${host}`);
      const entry = setCachedBoosterAddresses(host, safe);
      const addresses = sortBoosterAddresses(entry.addresses, profile, family, host, port);
      if (opts.all) return cb(null, addresses);
      const selected = selectBoosterAddress(entry, profile, family, port) || addresses[0];
      return cb(null, selected.address, selected.family);
    }).catch((err) => cb(err));
  };
}

function resolveBoostedAddress(hostname, port, profile = BOOSTER_PROFILE_BALANCED, family = 0) {
  const host = String(hostname || '').trim();
  const directFamily = net.isIP(host);
  if (directFamily) {
    if (isForbiddenTarget(host, port) || isPrivateOrProxyIp(host)) {
      return Promise.reject(new Error(`forbidden resolved target: ${host}:${port}`));
    }
    if ((family === 4 || family === 6) && family !== directFamily) {
      return Promise.reject(new Error(`address family mismatch for ${host}`));
    }
    return Promise.resolve(host);
  }

  const cached = getCachedBoosterAddresses(host);
  if (cached) {
    const selected = selectBoosterAddress(cached, profile, family, port);
    if (selected) return Promise.resolve(selected.address);
  }

  return dns.lookup(host, { all: true, verbatim: false }).then((records) => {
    const safe = records
      .map((r) => ({ address: normalizeIpAddress(r.address), family: Number(r.family || net.isIP(r.address)) }))
      .filter((r) => r.address && r.family && !isForbiddenTarget(r.address, port) && !isPrivateOrProxyIp(r.address));
    if (!safe.length) throw new Error(`no public address for ${host}`);
    const entry = setCachedBoosterAddresses(host, safe);
    const selected = selectBoosterAddress(entry, profile, family, port);
    if (!selected) throw new Error(`no address for ${host}`);
    return selected.address;
  });
}

function getUdpSocketFamily(socket, host = '') {
  try {
    if (socket && typeof socket.type === 'function') return socket.type() === 'udp6' ? 6 : 4;
  } catch (_) {}
  const ipFamily = net.isIP(host);
  return ipFamily === 6 ? 6 : 4;
}

function createUdpBurstState() {
  return { windowStart: Date.now(), count: 0, dropped: 0, lastLog: 0 };
}

function shouldDropUdpForLatency(profile, state, bufferedBytes = 0, bufferLimit = 0) {
  if (!SMART_BOOSTER_ENABLED || profile?.name !== 'latency') return false;
  const now = Date.now();
  const limit = Number(bufferLimit || profile.wsBackpressureLimit || 512 * 1024);
  if (bufferedBytes > Math.max(64 * 1024, limit * 0.75)) return true;

  if (!state) return false;
  if (now - Number(state.windowStart || 0) > BOOSTER_UDP_BURST_WINDOW_MS) {
    state.windowStart = now;
    state.count = 0;
  }
  state.count += 1;
  const allowed = BOOSTER_UDP_BURST_PACKETS;
  if (state.count > allowed) {
    state.dropped += 1;
    return true;
  }
  return false;
}

function adaptXhttpDownlinkProfile(session, pressureBytes = 0) {
  if (!SMART_BOOSTER_ENABLED || !session || !session.boosterProfile) return;
  const profile = session.boosterProfile;
  const limit = profile.xhttpDownlinkBufferLimit || MAX_XHTTP_DOWNLINK_BUFFER;
  if (pressureBytes > limit * 0.7 && profile.name === 'speed') {
    session.boosterProfile = BOOSTER_PROFILE_STABLE;
    log('[BOOSTER] XHTTP adapted:', `sid=${session.sessionId} speed->stable pressure=${formatBytes(pressureBytes)}`);
  } else if (pressureBytes > limit * 0.9 && profile.name === 'stable') {
    session.boosterProfile = BOOSTER_PROFILE_BALANCED;
    log('[BOOSTER] XHTTP adapted:', `sid=${session.sessionId} stable->balanced pressure=${formatBytes(pressureBytes)}`);
  }
}

function createBoostedTcpConnection(host, port, profile = BOOSTER_PROFILE_BALANCED) {
  const p = profile || BOOSTER_PROFILE_BALANCED;
  const mem = getDestinationMemory(host, port, true);
  const attemptTimeout = mem?.connectAvgMs
    ? Math.max(75, Math.min(p.autoSelectFamilyAttemptTimeout || 250, Math.round(mem.connectAvgMs * 0.65)))
    : (p.autoSelectFamilyAttemptTimeout || 250);
  const options = {
    host,
    port,
    allowHalfOpen: false,
    autoSelectFamily: true,
    autoSelectFamilyAttemptTimeout: attemptTimeout,
    highWaterMark: p.socketHighWaterMark || 64 * 1024,
    lookup: makeSmartDnsLookup(port, p)
  };
  const startedAt = Date.now();
  const socket = net.createConnection(options);
  let connected = false;
  socket.once('connect', () => {
    connected = true;
    markBoosterConnectSuccess(host, port, socket.remoteAddress || '', Date.now() - startedAt);
  });
  socket.once('error', (err) => {
    if (!connected) markBoosterConnectFailure(host, port, socket.remoteAddress || '', err);
  });
  return socket;
}

function parseVlessRequest(buffer, options = {}) {
  const allowUdp = options.allowUdp === true;
  const allowMux = options.allowMux === true;
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
  if (command !== 0x01 && command !== 0x02 && command !== 0x03) {
    throw new Error(`unsupported command: ${command}`);
  }
  if (command === 0x02 && !allowUdp) {
    throw new Error('UDP is not supported on this transport yet; use WebSocket UDP or disable UDP/xudp in client');
  }
  if (command === 0x03) {
    if (!allowMux) throw new Error('MUX is not supported on this transport yet; disable MUX in client');
    return { version, user, command, host: 'v1.mux.cool', port: 0, payload: buffer.subarray(offset) };
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

const MUX_STATUS_NEW = 0x01;
const MUX_STATUS_KEEP = 0x02;
const MUX_STATUS_END = 0x03;
const MUX_STATUS_KEEPALIVE = 0x04;
const MUX_OPTION_DATA = 0x01;
const MUX_OPTION_ERROR = 0x02;
const MUX_NETWORK_TCP = 0x01;
const MUX_NETWORK_UDP = 0x02;
const MUX_CHUNK_SIZE = 16 * 1024;

function encodeMuxAddress(host) {
  const h = String(host || '');
  if (/^\d+\.\d+\.\d+\.\d+$/.test(h)) {
    return Buffer.from([0x01, ...h.split('.').map((x) => Number(x) & 0xff)]);
  }
  if (h.includes(':')) {
    const net = require('net');
    if (net.isIP(h) === 6) {
      // Basic IPv6 expansion.
      const sections = h.split('::');
      const left = sections[0] ? sections[0].split(':').filter(Boolean) : [];
      const right = sections[1] ? sections[1].split(':').filter(Boolean) : [];
      const missing = 8 - left.length - right.length;
      const parts = left.concat(Array(Math.max(0, missing)).fill('0')).concat(right).map((x) => parseInt(x || '0', 16));
      const out = Buffer.alloc(17);
      out[0] = 0x03;
      for (let i = 0; i < 8; i++) out.writeUInt16BE(parts[i] || 0, 1 + i * 2);
      return out;
    }
  }
  const domain = Buffer.from(h, 'utf8');
  if (domain.length > 255) throw new Error('mux domain too long');
  return Buffer.concat([Buffer.from([0x02, domain.length]), domain]);
}

function parseMuxAddressPort(buffer, offset) {
  if (buffer.length < offset + 3) throw new Error('mux address too short');
  const port = buffer.readUInt16BE(offset);
  offset += 2;
  const type = buffer[offset++];
  let host;
  if (type === 0x01) {
    if (buffer.length < offset + 4) throw new Error('mux ipv4 too short');
    host = `${buffer[offset]}.${buffer[offset + 1]}.${buffer[offset + 2]}.${buffer[offset + 3]}`;
    offset += 4;
  } else if (type === 0x02) {
    if (buffer.length < offset + 1) throw new Error('mux domain length missing');
    const len = buffer[offset++];
    if (!len || buffer.length < offset + len) throw new Error('mux domain invalid');
    host = buffer.subarray(offset, offset + len).toString('utf8');
    offset += len;
  } else if (type === 0x03) {
    if (buffer.length < offset + 16) throw new Error('mux ipv6 too short');
    const parts = [];
    for (let i = 0; i < 8; i++) parts.push(buffer.readUInt16BE(offset + i * 2).toString(16));
    host = parts.join(':');
    offset += 16;
  } else {
    throw new Error('mux unsupported address type');
  }
  return { host, port, offset };
}

function encodeMuxFrame(sessionId, status, option = 0, target = null, data = null) {
  const metaParts = [];
  const base = Buffer.alloc(4);
  base.writeUInt16BE(sessionId & 0xffff, 0);
  base[2] = status;
  base[3] = option;
  metaParts.push(base);
  if (status === MUX_STATUS_NEW && target) {
    const netByte = target.network === 'udp' ? MUX_NETWORK_UDP : MUX_NETWORK_TCP;
    const port = Buffer.alloc(2);
    port.writeUInt16BE(target.port & 0xffff, 0);
    metaParts.push(Buffer.from([netByte]), port, encodeMuxAddress(target.host));
  }
  const meta = Buffer.concat(metaParts);
  const metaLen = Buffer.alloc(2);
  metaLen.writeUInt16BE(meta.length, 0);
  if (option & MUX_OPTION_DATA) {
    const payload = Buffer.isBuffer(data) ? data : Buffer.from(data || []);
    const dataLen = Buffer.alloc(2);
    dataLen.writeUInt16BE(payload.length, 0);
    return Buffer.concat([metaLen, meta, dataLen, payload]);
  }
  return Buffer.concat([metaLen, meta]);
}

function splitMuxData(data) {
  const chunks = [];
  const buf = Buffer.isBuffer(data) ? data : Buffer.from(data || []);
  for (let i = 0; i < buf.length; i += MUX_CHUNK_SIZE) chunks.push(buf.subarray(i, i + MUX_CHUNK_SIZE));
  return chunks;
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
    this.muxMode = false;
    this.muxHandler = null;
    this.udpTargetHost = '';
    this.udpTargetPort = 0;
    this.udpState = { buffer: Buffer.alloc(0) };
    this.udpBurstState = createUdpBurstState();
    this.udpDownlinkBurstState = createUdpBurstState();
    this.udpIdleTimer = null;
    this.clientIp = 'unknown';
    this.clientKey = '';
    this.userAgent = '';
    this.version = 0;
    this.boosterProfile = BOOSTER_PROFILE_BALANCED;
    this.downlinkRes = null;
    this.nextSeq = 0;
    this.pendingPackets = new Map();
    this.pendingRemoteWrites = [];
    this.pendingDownlinkChunks = [];
    this.pendingDownlinkBytes = 0;
    this.streamInitBuffer = Buffer.alloc(0);
    this.streamQueue = Promise.resolve();
    this.aliveTimer = null;
    this.lastActivity = Date.now();
    this.connectTimer = null;
    this.cleanupTimer = setTimeout(() => this.close('session timeout'), XHTTP_SESSION_TIMEOUT_MS);
  }

  refresh() {
    this.lastActivity = Date.now();
    this.unregisterConnection?.touch?.();
    if (this.cleanupTimer && this.cleanupTimer.refresh) this.cleanupTimer.refresh();
  }

  touchData() {
    this.lastActivity = Date.now();
    this.unregisterConnection?.touchData?.();
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
    this.clientIp = getClientIp(req);
    this.userAgent = String(req.headers['user-agent'] || this.userAgent || '');
    this.downlinkRes = res;
    req.setTimeout(0);
    res.setTimeout(0);
    try { req.socket.setKeepAlive(true, 30000); req.socket.setNoDelay(true); } catch (_) {}

    if (!res.headersSent) {
      res.writeHead(200, XHTTP_RESPONSE_HEADERS);
    }

    clearInterval(this.aliveTimer);
    this.aliveTimer = setInterval(() => {
      if (this.closed || !this.downlinkRes || this.downlinkRes.writableEnded || this.downlinkRes.destroyed) {
        clearInterval(this.aliveTimer);
        this.aliveTimer = null;
        return;
      }
      // MDIE v3: do not renew active-client state from server-only downlink pings.
      // Only client-originated traffic/requests or WS pong should keep a device active.
    }, 1000);

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

  processStreamChunk(chunk) {
    this.streamQueue = this.streamQueue.then(() => this._processStreamChunk(chunk));
    return this.streamQueue;
  }

  async _processStreamChunk(chunk) {
    if (this.closed || !chunk || chunk.length === 0) return;
    this.refresh();

    if (this.established || this.remote || this.udpMode || this.muxMode) {
      this.writeToRemote(chunk);
      return;
    }

    this.streamInitBuffer = this.streamInitBuffer.length ? Buffer.concat([this.streamInitBuffer, chunk]) : Buffer.from(chunk);
    if (this.streamInitBuffer.length > MAX_FIRST_PACKET) throw new Error('stream-up first packet too large');

    try {
      await this.initialize(this.streamInitBuffer, { allowIncomplete: true });
      this.streamInitBuffer = Buffer.alloc(0);
    } catch (err) {
      const msg = err && err.message ? err.message : '';
      const incomplete = msg.includes('too short') || msg.includes('invalid options length') ||
        msg.includes('invalid ipv4') || msg.includes('invalid domain length') ||
        msg.includes('invalid domain') || msg.includes('invalid ipv6');
      if (incomplete && !this.closed) return;
      throw err;
    }
  }

  initialize(firstPacket, options = {}) {
    if (this.remote || this.udpMode || this.muxMode) return Promise.resolve();

    let request;
    try {
      request = parseVlessRequest(firstPacket, { allowUdp: true, allowMux: true });
    } catch (err) {
      const msg = err && err.message ? err.message : '';
      const incomplete = msg.includes('too short') || msg.includes('invalid options length') ||
        msg.includes('invalid ipv4') || msg.includes('invalid domain length') ||
        msg.includes('invalid domain') || msg.includes('invalid ipv6');
      if (!options.allowIncomplete || !incomplete) this.close('bad vless request');
      throw err;
    }

    this.user = request.user;
    this.version = request.version;
    this.boosterProfile = chooseBoosterProfile(request, 'xhttp');
    const deviceMeta = getConnectionDeviceMeta({ headers: { 'user-agent': this.userAgent } }, this.clientIp, 'xhttp', request);
    const candidate = findBestDeviceCandidate(this.user.id, deviceMeta);
    this.clientKey = deviceMeta.endpointKey || candidate?.device?.id || '';
    if (!canAcceptUserSession(this.user, this.clientKey)) {
      this.close('client limit exceeded');
      throw new Error('client limit exceeded');
    }
    this.unregisterConnection = registerConnection(this.user.id, (reason) => this.close(reason), { ip: this.clientIp, clientKey: this.clientKey, kind: 'xhttp', deviceMeta });
    this.clientKey = this.unregisterConnection.clientKey || this.clientKey;
    log('[XHTTP] Connect:', `${this.user.remark} -> ${request.host}:${request.port} sid=${this.sessionId} ${describeBoosterProfile(this.boosterProfile)} device=${this.clientKey}`);

    if (request.command === 0x03) {
      this.muxMode = true;
      this.established = true;
      this.flushDownlink();
      this.muxHandler = new MuxHandler({
        user: this.user,
        label: 'XHTTP MUX',
        profile: this.boosterProfile,
        touch: () => this.touchData(),
        send: (chunk) => this.sendDownlink(chunk),
        close: (reason) => this.close(reason || 'mux closed')
      });
      if (request.payload.length) this.writeToRemote(request.payload);
      this.flushRemoteWrites();
      return Promise.resolve();
    }

    if (request.command === 0x02) {
      this.udpMode = true;
      this.udpTargetHost = request.host;
      this.udpTargetPort = request.port;
      this.established = true;
      this.flushDownlink();

      const udpType = request.host.includes(':') ? 'udp6' : 'udp4';
      this.udpSocket = dgram.createSocket(udpType);
      this.udpSocket.on('message', (msg) => {
        this.touchData();
        const buffered = this.pendingDownlinkBytes || 0;
        const limit = this.boosterProfile.xhttpDownlinkBufferLimit || MAX_XHTTP_DOWNLINK_BUFFER;
        if (shouldDropUdpForLatency(this.boosterProfile, this.udpDownlinkBurstState, buffered, limit)) return;
        updateTraffic(this.user, msg.length, 'downlink', () => this.touchData());
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
      this.udpIdleTimer = setTimeout(() => this.close('udp idle timeout'), this.boosterProfile.udpIdleTimeoutMs || UDP_IDLE_TIMEOUT_MS);

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
      }, this.boosterProfile.connectTimeoutMs || CONNECT_TIMEOUT_MS);

      this.remote = createBoostedTcpConnection(request.host, request.port, this.boosterProfile);
      this.remote.setNoDelay(true);
      this.remote.setKeepAlive(true, 30000);
      this.remote.setTimeout(this.boosterProfile.idleTimeoutMs || IDLE_TIMEOUT_MS);

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
        this.touchData();
        updateTraffic(this.user, chunk.length, 'downlink', () => this.touchData());
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

    if (this.muxMode) {
      try {
        this.muxHandler?.feed(chunk);
      } catch (err) {
        log('[XHTTP MUX] Bad frame:', err.message);
        this.close('bad mux frame');
      }
      return;
    }

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

    updateTraffic(this.user, chunk.length, 'uplink', () => this.touchData());
    if (!this.remote.write(chunk)) {
      // Let Node socket backpressure work; later packets remain queued if needed.
    }
  }

  sendUdpPacket(packet) {
    if (this.closed || !this.udpSocket || !this.udpTargetHost || !this.udpTargetPort) return;
    if (shouldDropUdpForLatency(this.boosterProfile, this.udpBurstState, this.pendingDownlinkBytes || 0, this.boosterProfile.xhttpDownlinkBufferLimit || MAX_XHTTP_DOWNLINK_BUFFER)) return;
    if (this.udpIdleTimer && this.udpIdleTimer.refresh) this.udpIdleTimer.refresh();
    updateTraffic(this.user, packet.length, 'uplink', () => this.touchData());
    const family = getUdpSocketFamily(this.udpSocket, this.udpTargetHost);
    resolveBoostedAddress(this.udpTargetHost, this.udpTargetPort, this.boosterProfile, family).then((address) => {
      if (this.closed || !this.udpSocket) return;
      this.udpSocket.send(packet, this.udpTargetPort, address, (err) => {
        if (err) {
          log('[XHTTP UDP] Send failed:', err.message);
          markBoosterConnectFailure(this.udpTargetHost, this.udpTargetPort, address, err);
          this.close('udp send failed');
        }
      });
    }).catch((err) => {
      log('[XHTTP UDP] Resolve failed:', err && err.message ? err.message : 'unknown');
      markBoosterConnectFailure(this.udpTargetHost, this.udpTargetPort, '', err);
      this.close('udp resolve failed');
    });
  }

  flushRemoteWrites() {
    if (this.muxMode || this.udpMode) {
      while (this.pendingRemoteWrites.length) this.writeToRemote(this.pendingRemoteWrites.shift());
      return;
    }
    if (!this.remote || !this.established || this.remote.destroyed) return;
    while (this.pendingRemoteWrites.length) {
      const chunk = this.pendingRemoteWrites.shift();
      updateTraffic(this.user, chunk.length, 'uplink', () => this.touchData());
      if (!this.remote.write(chunk)) break;
    }
  }

  sendDownlink(chunk) {
    if (this.closed) return;
    if (!this.downlinkRes || this.downlinkRes.writableEnded || this.downlinkRes.destroyed) {
      this.pendingDownlinkChunks.push(chunk);
      this.pendingDownlinkBytes += chunk.length;
      adaptXhttpDownlinkProfile(this, this.pendingDownlinkBytes);
      const downlinkLimit = this.boosterProfile.xhttpDownlinkBufferLimit || MAX_XHTTP_DOWNLINK_BUFFER;
      if (this.pendingDownlinkBytes > downlinkLimit) {
        this.close('downlink buffer overflow');
      }
      return;
    }
    this.flushDownlink();
    if (!this.downlinkRes.writableEnded) {
      const ok = this.downlinkRes.write(chunk);
      if (!ok) adaptXhttpDownlinkProfile(this, Math.max(this.pendingDownlinkBytes, chunk.length));
    }
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
    clearInterval(this.aliveTimer);
    if (this.unregisterConnection) this.unregisterConnection();
    destroySocket(this.remote);
    closeUdpSocket(this.udpSocket);
    this.muxHandler?.close();
    if (this.downlinkRes && !this.downlinkRes.writableEnded) {
      try { this.downlinkRes.end(); } catch (_) {}
    }
    this.pendingPackets.clear();
    this.streamInitBuffer = Buffer.alloc(0);
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

class MuxHandler {
  constructor({ user, send, close, touch = null, label = 'MUX', profile = BOOSTER_PROFILE_BALANCED }) {
    this.user = user;
    this.profile = profile || BOOSTER_PROFILE_BALANCED;
    this.send = send;
    this.closeTransport = close;
    this.touch = touch;
    this.label = label;
    this.buffer = Buffer.alloc(0);
    this.sessions = new Map();
    this.udpBurstStates = new Map();
    this.closed = false;
  }

  feed(chunk) {
    if (this.closed || !chunk || chunk.length === 0) return;
    this.buffer = this.buffer.length ? Buffer.concat([this.buffer, chunk]) : Buffer.from(chunk);

    while (this.buffer.length >= 2) {
      const metaLen = this.buffer.readUInt16BE(0);
      if (metaLen > 512) throw new Error('mux invalid metadata length');
      if (this.buffer.length < 2 + metaLen) break;

      const meta = this.buffer.subarray(2, 2 + metaLen);
      if (meta.length < 4) throw new Error('mux metadata too short');
      const sessionId = meta.readUInt16BE(0);
      const status = meta[2];
      const option = meta[3];
      let offset = 4;
      let target = null;

      if (status === MUX_STATUS_NEW) {
        if (meta.length < 5) throw new Error('mux new missing network');
        const networkByte = meta[offset++];
        const parsed = parseMuxAddressPort(meta, offset);
        offset = parsed.offset;
        target = { network: networkByte === MUX_NETWORK_UDP ? 'udp' : 'tcp', host: parsed.host, port: parsed.port };
        if (isForbiddenTarget(target.host, target.port)) throw new Error(`mux forbidden target: ${target.host}:${target.port}`);
      }

      let frameLen = 2 + metaLen;
      let data = Buffer.alloc(0);
      if (option & MUX_OPTION_DATA) {
        if (this.buffer.length < frameLen + 2) break;
        const dataLen = this.buffer.readUInt16BE(frameLen);
        if (dataLen > MAX_WS_PAYLOAD) throw new Error('mux data too large');
        if (this.buffer.length < frameLen + 2 + dataLen) break;
        data = this.buffer.subarray(frameLen + 2, frameLen + 2 + dataLen);
        frameLen += 2 + dataLen;
      }

      this.buffer = this.buffer.subarray(frameLen);
      this.handleFrame({ sessionId, status, option, target, data });
    }
  }

  handleFrame(frame) {
    if (frame.status === MUX_STATUS_KEEPALIVE) return;
    if (frame.status === MUX_STATUS_NEW) {
      this.openSession(frame.sessionId, frame.target);
      if (frame.option & MUX_OPTION_DATA) this.writeSession(frame.sessionId, frame.data);
      return;
    }
    if (frame.status === MUX_STATUS_KEEP) {
      if (frame.option & MUX_OPTION_DATA) this.writeSession(frame.sessionId, frame.data);
      return;
    }
    if (frame.status === MUX_STATUS_END) {
      this.closeSession(frame.sessionId, Boolean(frame.option & MUX_OPTION_ERROR));
    }
  }

  openSession(sessionId, target) {
    this.closeSession(sessionId, false, true);
    const category = detectDestinationCategory({ host: target.host, port: target.port, command: target.network === 'udp' ? 0x02 : 0x01 });
    const session = { id: sessionId, target, category, remote: null, udp: null, connected: false, pending: [], pendingBytes: 0, closed: false };
    this.sessions.set(sessionId, session);
    if (target.network === 'udp') this.udpBurstStates.set(sessionId, createUdpBurstState());

    if (target.network === 'udp') {
      const udpType = target.host.includes(':') ? 'udp6' : 'udp4';
      session.udp = dgram.createSocket(udpType);
      session.connected = true;
      session.udp.on('message', (msg) => {
        updateTraffic(this.user, msg.length, 'downlink', this.touch);
        this.sendData(sessionId, msg);
      });
      session.udp.on('error', (err) => {
        log(`[${this.label}] UDP error:`, err && err.message ? err.message : 'unknown');
        this.sendEnd(sessionId, true);
        this.closeSession(sessionId, true);
      });
      return;
    }

    session.remote = createBoostedTcpConnection(target.host, target.port, this.profile);
    session.remote.setNoDelay(true);
    session.remote.setKeepAlive(true, 30000);
    session.remote.setTimeout(this.profile.idleTimeoutMs || IDLE_TIMEOUT_MS);
    session.remote.once('connect', () => {
      session.connected = true;
      while (session.pending.length && session.remote && !session.remote.destroyed) {
        const p = session.pending.shift();
        session.pendingBytes = Math.max(0, Number(session.pendingBytes || 0) - p.length);
        updateTraffic(this.user, p.length, 'uplink', this.touch);
        if (!session.remote.write(p)) break;
      }
    });
    session.remote.on('data', (data) => {
      updateTraffic(this.user, data.length, 'downlink', this.touch);
      this.sendData(sessionId, data);
    });
    session.remote.once('error', (err) => {
      log(`[${this.label}] TCP error:`, `${target.host}:${target.port} ${err && err.message ? err.message : ''}`);
      this.sendEnd(sessionId, true);
      this.closeSession(sessionId, true);
    });
    session.remote.on('timeout', () => {
      this.sendEnd(sessionId, false);
      this.closeSession(sessionId, false);
    });
    session.remote.on('close', () => {
      if (!session.closed) this.sendEnd(sessionId, false);
      this.closeSession(sessionId, false, true);
    });
  }

  writeSession(sessionId, data) {
    if (!data || data.length === 0) return;
    const session = this.sessions.get(sessionId);
    if (!session || session.closed) return;

    if (session.target.network === 'udp') {
      const burstState = this.udpBurstStates.get(sessionId) || createUdpBurstState();
      this.udpBurstStates.set(sessionId, burstState);
      const effectiveProfile = session.category === 'game' ? BOOSTER_PROFILE_LATENCY : this.profile;
      if (shouldDropUdpForLatency(effectiveProfile, burstState, 0, effectiveProfile.wsBackpressureLimit || WS_BACKPRESSURE_LIMIT)) return;
      updateTraffic(this.user, data.length, 'uplink', this.touch);
      const family = getUdpSocketFamily(session.udp, session.target.host);
      resolveBoostedAddress(session.target.host, session.target.port, this.profile, family).then((address) => {
        if (!session.udp || session.closed) return;
        session.udp.send(data, session.target.port, address, (err) => {
          if (err) {
            log(`[${this.label}] UDP send failed:`, err.message);
            markBoosterConnectFailure(session.target.host, session.target.port, address, err);
            this.sendEnd(sessionId, true);
            this.closeSession(sessionId, true);
          }
        });
      }).catch((err) => {
        log(`[${this.label}] UDP resolve failed:`, err && err.message ? err.message : 'unknown');
        markBoosterConnectFailure(session.target.host, session.target.port, '', err);
        this.sendEnd(sessionId, true);
        this.closeSession(sessionId, true);
      });
      return;
    }

    if (!session.remote || session.remote.destroyed || !session.connected) {
      const pendingLimit = this.profile.name === 'speed' ? 2 * 1024 * 1024 : 512 * 1024;
      session.pendingBytes = Number(session.pendingBytes || 0) + data.length;
      if (session.pendingBytes > pendingLimit) {
        log(`[${this.label}] Pending overflow:`, `${sessionId} ${formatBytes(session.pendingBytes)}`);
        this.sendEnd(sessionId, true);
        this.closeSession(sessionId, true);
        return;
      }
      session.pending.push(Buffer.from(data));
      return;
    }
    updateTraffic(this.user, data.length, 'uplink', this.touch);
    session.remote.write(data);
  }

  sendData(sessionId, data) {
    for (const chunk of splitMuxData(data)) {
      this.send(encodeMuxFrame(sessionId, MUX_STATUS_KEEP, MUX_OPTION_DATA, null, chunk));
    }
  }

  sendEnd(sessionId, hasError = false) {
    this.send(encodeMuxFrame(sessionId, MUX_STATUS_END, hasError ? MUX_OPTION_ERROR : 0));
  }

  closeSession(sessionId, hasError = false, silent = false) {
    const session = this.sessions.get(sessionId);
    if (!session) return;
    session.closed = true;
    destroySocket(session.remote);
    closeUdpSocket(session.udp);
    session.pending = [];
    this.sessions.delete(sessionId);
    this.udpBurstStates.delete(sessionId);
    if (hasError && !silent) log(`[${this.label}] Session closed with error:`, String(sessionId));
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    for (const id of Array.from(this.sessions.keys())) this.closeSession(id, false, true);
    this.buffer = Buffer.alloc(0);
  }
}

function normalizeIpAddress(ip) {
  let value = String(ip || '').trim();
  if (!value) return '';
  value = value.replace(/^::ffff:/i, '');
  value = value.replace(/^\[/, '').replace(/\]$/, '');
  const zoneIndex = value.indexOf('%');
  if (zoneIndex >= 0) value = value.slice(0, zoneIndex);
  return value;
}

function isPrivateOrProxyIp(ip) {
  const h = normalizeIpAddress(ip).toLowerCase();
  if (!h) return true;
  if (h === 'unknown' || h === 'localhost' || h === '::1') return true;
  if (/^127\./.test(h) || /^10\./.test(h) || /^192\.168\./.test(h) || /^169\.254\./.test(h)) return true;
  if (/^172\.(1[6-9]|2\d|3[0-1])\./.test(h)) return true;
  if (h === '0.0.0.0' || h === '::') return true;
  if (/^(fc|fd)[0-9a-f]{2}:/i.test(h) || /^fe80:/i.test(h)) return true;
  return false;
}

function getClientIp(req) {
  const cfIp = normalizeIpAddress(req.headers['cf-connecting-ip']);
  if (cfIp && !isPrivateOrProxyIp(cfIp)) return cfIp;

  const realIp = normalizeIpAddress(req.headers['x-real-ip']);
  if (realIp && !isPrivateOrProxyIp(realIp)) return realIp;

  const xff = String(req.headers['x-forwarded-for'] || '')
    .split(',')
    .map((x) => normalizeIpAddress(x))
    .filter(Boolean);
  const publicForwarded = xff.find((ip) => !isPrivateOrProxyIp(ip));
  if (publicForwarded) return publicForwarded;
  if (xff[0]) return xff[0];

  return normalizeIpAddress(req.ip || req.socket.remoteAddress || 'unknown') || 'unknown';
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

function buildUsersSnapshot() {
  return DB.users.map((user) => {
    pruneStaleConnections(user.id, ACTIVE_CONNECTION_STALE_MS);
    pruneClientSlots(user.id);
    return {
      ...user,
      used_bytes: getEffectiveUsedBytes(user),
      uplink_bytes: getEffectiveUplinkBytes(user),
      downlink_bytes: getEffectiveDownlinkBytes(user),
      last_seen: getEffectiveLastSeen(user),
      connections: activeConnections.get(user.id)?.size || 0,
      active_clients: getClientSlotCount(user.id)
    };
  });
}

function buildStatsSnapshot() {
  const memory = process.memoryUsage();
  const totalTraffic = DB.users.reduce((sum, u) => sum + getEffectiveUsedBytes(u), 0);
  let activeClientCount = 0;
  for (const user of DB.users) activeClientCount += getClientSlotCount(user.id);
  return {
    ram_usage: Math.round((memory.heapUsed / Math.max(1, memory.heapTotal)) * 100),
    cpu_usage: getCpuUsagePercent(),
    uptime: Math.round(process.uptime()),
    total_traffic: totalTraffic,
    users_count: DB.users.length,
    active_count: DB.users.filter((u) => u.is_active === 1 && !isUserExpiredEffective(u)).length,
    active_clients: activeClientCount,
    online_users: activeClientCount,
    ws_connections: Array.from(activeConnections.values()).reduce((sum, map) => sum + map.size, 0),
    xhttp_sessions: xhttpSessions.size,
    traffic_chart: getTrafficChart()
  };
}

function buildSubSnapshot(user) {
  const used = getEffectiveUsedBytes(user);
  const unlimited = Number(user.traffic_limit || 0) === 0;
  const remaining = unlimited ? null : Math.max(0, user.traffic_limit - used);
  const percent = unlimited ? null : Number(calcPercent(used, user.traffic_limit).toFixed(1));
  const progressWidth = unlimited ? 100 : Number(calcPercent(used, user.traffic_limit).toFixed(2));
  const rawStatus = getUserStatus(user);
  const unavailable = rawStatus !== 'Active';
  const status = unavailable ? 'INACTIVE' : 'ACTIVE';
  return {
    used_bytes: used,
    used_formatted: formatBytes(used),
    uplink_bytes: getEffectiveUplinkBytes(user),
    uplink_formatted: formatBytes(getEffectiveUplinkBytes(user)),
    downlink_bytes: getEffectiveDownlinkBytes(user),
    downlink_formatted: formatBytes(getEffectiveDownlinkBytes(user)),
    remaining_bytes: remaining,
    remaining_formatted: remaining === null ? 'Unlimited' : formatBytes(remaining),
    unlimited,
    percent,
    percent_label: unlimited ? '∞' : `${percent}%`,
    progress_width: progressWidth,
    is_danger: unavailable || (!unlimited && user.traffic_limit > 0 && (used / user.traffic_limit) > 0.9),
    expired: rawStatus === 'Expired',
    inactive: rawStatus === 'Inactive',
    status,
    status_class: unavailable ? 'expired' : 'active',
    ring_theme: unavailable ? 'red' : (unlimited ? 'green' : 'gold'),
    expiry: user.expiry_time === 0 ? 'Never' : formatTehranDate(user.expiry_time),
    last_seen: getEffectiveLastSeen(user) === 0 ? 'Never' : formatTehranDate(getEffectiveLastSeen(user))
  };
}

function writeSse(res, event, data) {
  try {
    res.write(`event: ${event}\n`);
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  } catch (_) {}
}

function setupSse(req, res) {
  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-store, no-transform',
    'connection': 'keep-alive',
    'x-accel-buffering': 'no'
  });
  try { res.flushHeaders?.(); } catch (_) {}
}

function broadcastRealtimeNow() {
  if (adminEventClients.size) {
    const snapshot = { stats: buildStatsSnapshot(), users: buildUsersSnapshot() };
    for (const res of Array.from(adminEventClients)) writeSse(res, 'snapshot', snapshot);
  }

  for (const [subId, clients] of Array.from(subEventClients.entries())) {
    const user = DB.users.find((u) => u.sub_id === subId);
    if (!user) {
      for (const res of Array.from(clients)) writeSse(res, 'deleted', { deleted: true });
      continue;
    }
    const snapshot = buildSubSnapshot(user);
    for (const res of Array.from(clients)) writeSse(res, 'snapshot', snapshot);
  }
}

function scheduleRealtimeBroadcast() {
  if (realtimeBroadcastTimer) return;
  realtimeBroadcastTimer = setTimeout(() => {
    realtimeBroadcastTimer = null;
    broadcastRealtimeNow();
  }, 100);
}

app.get('/health', (req, res) => res.type('text/plain').send(`ok - ${BUILD_ID}\n`));

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

// XHTTP stream-up upload channel: one long-lived POST carrying ordered VLESS bytes.
app.post([`${XHTTP_PATH}/:sessionId`, `${XHTTP_PATH}/:sessionId/`], (req, res) => {
  const sessionId = String(req.params.sessionId || '').trim();
  if (sessionId.length < 1 || sessionId.length > 256) {
    res.writeHead(400, XHTTP_RESPONSE_HEADERS);
    res.end();
    return;
  }

  const session = getXHttpSession(sessionId);
  if (!session.clientIp || session.clientIp === 'unknown') {
    session.clientIp = getClientIp(req);
    session.userAgent = String(req.headers['user-agent'] || session.userAgent || '');
  }
  let failed = false;
  req.setTimeout(0);
  res.setTimeout(0);
  res.writeHead(200, XHTTP_RESPONSE_HEADERS);

  req.on('data', (chunk) => {
    if (failed) return;
    req.pause();
    session.processStreamChunk(chunk).then(() => {
      if (!failed) req.resume();
    }).catch((err) => {
      failed = true;
      log('[XHTTP stream-up] failed:', err && err.message ? err.message : 'unknown');
      session.close('stream-up failed');
      try { res.end(); } catch (_) {}
    });
  });

  req.on('end', () => {
    session.streamQueue.finally(() => {
      if (!res.writableEnded) res.end();
    });
  });
  req.on('error', () => {
    failed = true;
    session.close('stream-up request error');
    try { res.end(); } catch (_) {}
  });
  req.on('aborted', () => {
    failed = true;
    session.close('stream-up aborted');
  });
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
    if (!session.clientIp || session.clientIp === 'unknown') {
      session.clientIp = getClientIp(req);
      session.userAgent = String(req.headers['user-agent'] || session.userAgent || '');
    }
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

app.post('/api/backup/restore', requireAuth, (req, res) => {
  const incomingUsers = Array.isArray(req.body?.users) ? req.body.users : null;
  const overwrite = req.body?.overwrite === true;
  if (!incomingUsers) return res.status(400).json({ error: 'Invalid backup: users array is required' });
  if (incomingUsers.length > 10000) return res.status(400).json({ error: 'Invalid backup: too many users' });

  const restoredUsers = incomingUsers.map(normalizeUser).filter(Boolean);
  if (restoredUsers.length !== incomingUsers.length) {
    return res.status(400).json({ error: 'Invalid backup: one or more users are malformed' });
  }

  const duplicateKeys = (a, b) => {
    return (a.id && b.id && a.id === b.id) ||
      (a.uuid && b.uuid && String(a.uuid).toLowerCase() === String(b.uuid).toLowerCase()) ||
      (a.sub_id && b.sub_id && a.sub_id === b.sub_id) ||
      (a.remark && b.remark && a.remark === b.remark);
  };

  let added = 0;
  let overwritten = 0;
  let skipped = 0;

  for (const restored of restoredUsers) {
    const idx = DB.users.findIndex((existing) => duplicateKeys(existing, restored));
    if (idx >= 0) {
      if (overwrite) {
        forceClearUserConnections(DB.users[idx].id, 'backup overwrite');
        DB.users[idx] = restored;
        overwritten += 1;
      } else {
        skipped += 1;
      }
    } else {
      DB.users.unshift(restored);
      added += 1;
    }
  }

  pendingTrafficBytes.clear();
  pendingUplinkBytes.clear();
  pendingDownlinkBytes.clear();
  pendingLastSeen.clear();
  for (const deviceMap of activeDevices.values()) {
    for (const device of deviceMap.values()) clearTimeout(device.closeTimer);
  }
  activeDevices.clear();
  activeClientActivity.clear();

  scheduleRealtimeBroadcast();
  scheduleSaveDB(false);
  log('[Backup] Merge restored:', `added=${added} overwritten=${overwritten} skipped=${skipped}`);
  res.json({ success: true, added, overwritten, skipped, users_count: DB.users.length });
});

app.get('/api/stats', requireAuth, (req, res) => {
  res.json(buildStatsSnapshot());
});

app.get('/api/users', requireAuth, (req, res) => {
  res.json({ users: buildUsersSnapshot() });
});

app.get('/api/events', requireAuth, (req, res) => {
  setupSse(req, res);
  adminEventClients.add(res);
  writeSse(res, 'snapshot', { stats: buildStatsSnapshot(), users: buildUsersSnapshot() });
  const heartbeat = setInterval(() => writeSse(res, 'ping', { now: Date.now() }), 25000);
  req.on('close', () => {
    clearInterval(heartbeat);
    adminEventClients.delete(res);
  });
});

app.post('/api/users', requireAuth, (req, res) => {
  const { remark, traffic_limit_gb, expiry_days, ip_limit, connection_limit } = req.body || {};
  const cleanRemark = String(remark || '').trim();
  if (!cleanRemark) return res.status(400).json({ error: 'Remark is required' });
  if (DB.users.some((u) => u.remark === cleanRemark)) return res.status(400).json({ error: 'Username must be unique' });

  const limitGB = parseUserNumber(traffic_limit_gb, 0);
  const expiryDays = parseUserNumber(expiry_days, 0);
  const connectionLimit = Math.max(0, Math.floor(parseUserNumber(connection_limit ?? ip_limit, 0)));
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
    connection_limit: connectionLimit,
    used_bytes: 0,
    uplink_bytes: 0,
    downlink_bytes: 0,
    expiry_time: expiryDays > 0 ? Date.now() + Math.floor(expiryDays * 24 * 60 * 60 * 1000) : 0,
    last_seen: 0,
    is_active: 1,
    protocol: 'ws+xhttp',
    created_on: createdOn,
    created_at: new Date().toISOString()
  };

  DB.users.unshift(user);
  scheduleRealtimeBroadcast();
  scheduleSaveDB(false);
  log('[User] Created:', `${user.remark} ${user.uuid}`);
  res.json({ success: true, user });
});

app.delete('/api/users/:id', requireAuth, (req, res) => {
  const before = DB.users.length;
  const user = DB.users.find((u) => u.id === req.params.id);
  pendingTrafficBytes.delete(req.params.id);
  pendingUplinkBytes.delete(req.params.id);
  pendingDownlinkBytes.delete(req.params.id);
  pendingLastSeen.delete(req.params.id);
  DB.users = DB.users.filter((u) => u.id !== req.params.id);
  if (DB.users.length < before) {
    closeUserConnections(req.params.id, 'user deleted');
    scheduleRealtimeBroadcast();
    scheduleSaveDB(false);
    log('[User] Deleted:', user ? user.remark : req.params.id);
    return res.json({ success: true });
  }
  return res.status(404).json({ error: 'User not found' });
});

app.post('/api/users/:id/toggle', requireAuth, (req, res) => {
  flushTrafficBuffers();
  const user = DB.users.find((u) => u.id === req.params.id);
  if (!user) return res.status(404).json({ error: 'User not found' });
  user.is_active = user.is_active === 1 ? 0 : 1;
  if (user.is_active === 0) closeUserConnections(user.id, 'user disabled');
  scheduleRealtimeBroadcast();
  scheduleSaveDB(false);
  res.json({ success: true, is_active: user.is_active });
});

app.post('/api/users/:id/reset', requireAuth, (req, res) => {
  const user = DB.users.find((u) => u.id === req.params.id);
  if (!user) return res.status(404).json({ error: 'User not found' });
  pendingTrafficBytes.delete(user.id);
  pendingUplinkBytes.delete(user.id);
  pendingDownlinkBytes.delete(user.id);
  pendingLastSeen.delete(user.id);
  user.used_bytes = 0;
  user.uplink_bytes = 0;
  user.downlink_bytes = 0;
  user.is_active = 1;
  user.last_seen = 0;
  scheduleRealtimeBroadcast();
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
  flushTrafficBuffers();
  const user = DB.users.find((u) => u.id === req.params.id);
  if (!user) return res.status(404).json({ error: 'User not found' });

  const { remark, traffic_limit_gb, expiry_days, ip_limit, connection_limit } = req.body || {};
  const cleanRemark = String(remark || '').trim();
  if (!cleanRemark) return res.status(400).json({ error: 'Remark is required' });
  if (DB.users.some((u) => u.id !== user.id && u.remark === cleanRemark)) {
    return res.status(400).json({ error: 'Username must be unique' });
  }

  const limitGB = parseUserNumber(traffic_limit_gb, 0);
  const expiryDays = parseUserNumber(expiry_days, 0);
  const connectionLimit = Math.max(0, Math.floor(parseUserNumber(connection_limit ?? ip_limit, 0)));
  const oldConnectionLimit = Number(user.connection_limit || user.ip_limit || 0);

  user.remark = cleanRemark;
  user.traffic_limit = limitGB > 0 ? Math.floor(limitGB * 1024 * 1024 * 1024) : 0;
  user.connection_limit = connectionLimit;
  user.expiry_time = expiryDays > 0 ? Date.now() + Math.floor(expiryDays * 24 * 60 * 60 * 1000) : 0;

  if (oldConnectionLimit !== connectionLimit) {
    // Changing the connection limit invalidates the old session accounting.
    // Force-clear existing sessions so a previously unlimited connection cannot
    // reserve capacity and block the user's next connection.
    forceClearUserConnections(user.id, 'connection limit changed');
  }

  if (isUserExpired(user)) {
    user.is_active = 0;
    forceClearUserConnections(user.id, 'user edited and expired');
  } else {
    user.is_active = 1;
  }

  scheduleRealtimeBroadcast();
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

app.get('/sub/:sub_id/events', (req, res) => {
  const user = DB.users.find((u) => u.sub_id === req.params.sub_id);
  if (!user) return res.status(404).end();
  setupSse(req, res);
  if (!subEventClients.has(req.params.sub_id)) subEventClients.set(req.params.sub_id, new Set());
  const clients = subEventClients.get(req.params.sub_id);
  clients.add(res);
  writeSse(res, 'snapshot', buildSubSnapshot(user));
  const heartbeat = setInterval(() => writeSse(res, 'ping', { now: Date.now() }), 25000);
  req.on('close', () => {
    clearInterval(heartbeat);
    clients.delete(res);
    if (clients.size === 0) subEventClients.delete(req.params.sub_id);
  });
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

  const effectiveUsed = getEffectiveUsedBytes(user);
  const effectiveLastSeen = getEffectiveLastSeen(user);
  const usedFormatted = formatBytes(effectiveUsed);
  const uplinkFormatted = formatBytes(getEffectiveUplinkBytes(user));
  const downlinkFormatted = formatBytes(getEffectiveDownlinkBytes(user));
  const unlimitedTraffic = Number(user.traffic_limit || 0) === 0;
  const remainingFormatted = unlimitedTraffic ? 'Unlimited' : formatBytes(Math.max(0, user.traffic_limit - effectiveUsed));
  const numericPercent = unlimitedTraffic ? null : Number(calcPercent(effectiveUsed, user.traffic_limit).toFixed(1));
  const percent = unlimitedTraffic ? '∞' : numericPercent.toFixed(1);
  const progressWidth = unlimitedTraffic ? '100.00' : calcPercent(effectiveUsed, user.traffic_limit).toFixed(2);
  const rawStatusText = getUserStatus(user);
  const expired = rawStatusText !== 'Active';
  const statusText = expired ? 'INACTIVE' : 'ACTIVE';
  const isDanger = expired || (!unlimitedTraffic && user.traffic_limit > 0 && (effectiveUsed / user.traffic_limit) > 0.9);
  const expiryStr = user.expiry_time === 0 ? 'Never' : formatTehranDate(user.expiry_time);
  const lastSeenStr = effectiveLastSeen === 0 ? 'Never' : formatTehranDate(effectiveLastSeen);
  const remarkSafe = safeName(user.remark);
  const wsLinkJSON = JSON.stringify(wsLink);
  const xhttpLinkJSON = JSON.stringify(xhttpLink);
  const allLinksJSON = JSON.stringify(subscriptionLinks.join('\n'));

  const subUrlJSON = JSON.stringify(buildSubUrl(req, user));
  const qrBase = `/qr/${user.sub_id}`;

  const ringColor = expired ? '#ef4444' : (unlimitedTraffic ? '#22c55e' : 'var(--gold)');
  const ringDeg = expired ? 360 : (unlimitedTraffic ? 360 : Math.max(0, Math.min(100, Number(numericPercent))) * 3.6);

  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${remarkSafe} - Subscription</title>
  <!-- BUILD_MARKER: ${BUILD_ID} -->
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
    :root { --gold:#d4af37; --gold2:#f3e5ab; --bg:#07070a; --line:rgba(255,255,255,.085); --muted:#8a8a94; --success:#22c55e; --danger:#ef4444; }
    body { min-height:100vh; color:#fff; background: radial-gradient(circle at 12% 8%, rgba(212,175,55,.13), transparent 28%), radial-gradient(circle at 88% 12%, rgba(99,102,241,.13), transparent 24%), radial-gradient(circle at 50% 110%, rgba(34,197,94,.08), transparent 28%), #07070a; display:flex; align-items:center; justify-content:center; padding:22px; overflow-x:hidden; }
    body:before { content:''; position:fixed; inset:0; background-image: linear-gradient(rgba(255,255,255,.025) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,.02) 1px, transparent 1px); background-size:42px 42px; mask-image:radial-gradient(circle at center, #000, transparent 75%); pointer-events:none; z-index:-2; }
    .orb { position:fixed; filter:blur(80px); opacity:.45; z-index:-1; animation:float 9s ease-in-out infinite alternate; }
    .orb.one { width:320px; height:320px; background:#d4af37; top:-120px; right:-90px; }
    .orb.two { width:260px; height:260px; background:#6366f1; bottom:-110px; left:-80px; animation-delay:-4s; }
    @keyframes float { from { transform:translate3d(0,0,0) scale(1); } to { transform:translate3d(24px,28px,0) scale(1.08); } }
    .shell { width:100%; max-width:1280px; }
    .card { position:relative; overflow:hidden; background:linear-gradient(135deg, rgba(255,255,255,.06), rgba(255,255,255,.025)); border:1px solid var(--line); border-radius:32px; padding:30px; box-shadow:0 30px 90px rgba(0,0,0,.58); backdrop-filter:blur(18px); }
    .card:after { content:''; position:absolute; width:320px; height:320px; border-radius:50%; background:radial-gradient(circle, rgba(212,175,55,.16), transparent 68%); top:-150px; right:-120px; pointer-events:none; }
    .hero { position:relative; z-index:1; display:flex; flex-direction:column; justify-content:center; align-items:center; text-align:center; gap:14px; margin-bottom:26px; }
    .brand-title { color:var(--gold); font-size:14px; font-weight:1000; letter-spacing:6px; text-transform:uppercase; margin-bottom:12px; opacity:.95; text-shadow:0 0 18px rgba(212,175,55,.22),0 0 34px rgba(212,175,55,.12); animation:brandPulse 4.2s ease-in-out infinite; }
    h1 { font-size:38px; line-height:1.1; letter-spacing:.6px; text-align:center; background:linear-gradient(90deg,#fff,var(--gold2),#fff); background-size:220% auto; -webkit-background-clip:text; -webkit-text-fill-color:transparent; animation:titleShine 4.5s ease-in-out infinite alternate; filter:drop-shadow(0 0 18px rgba(212,175,55,.10)); }@keyframes titleShine{from{background-position:0% center;filter:drop-shadow(0 0 12px rgba(212,175,55,.08))}to{background-position:100% center;filter:drop-shadow(0 0 24px rgba(212,175,55,.18))}}@keyframes brandPulse{0%,100%{opacity:.72;text-shadow:0 0 14px rgba(212,175,55,.16),0 0 26px rgba(212,175,55,.08)}50%{opacity:1;text-shadow:0 0 24px rgba(212,175,55,.42),0 0 48px rgba(212,175,55,.20)}}
    .desc { display:none; }
    .chips { display:flex; flex-wrap:nowrap; justify-content:center; max-width:100%; gap:8px; margin-top:18px; overflow-x:auto; padding-bottom:4px; scrollbar-width:none; }
    .chip { display:inline-flex; flex:0 0 auto; align-items:center; gap:7px; padding:8px 12px; border-radius:999px; background:rgba(0,0,0,.25); border:1px solid rgba(255,255,255,.08); color:#bbb; font-size:11px; font-weight:900; letter-spacing:.7px; text-transform:uppercase; white-space:nowrap; }
    .chips::-webkit-scrollbar{display:none}.chip.ok { color:#4ade80; background:rgba(34,197,94,.09); border-color:rgba(34,197,94,.26); }
    .chip.gold { color:var(--gold); background:rgba(212,175,55,.08); border-color:rgba(212,175,55,.28); }
    .status-box { min-width:160px; text-align:center; color:#aaa; font-size:12px; }
    .status-pill { display:inline-flex; align-items:center; gap:7px; padding:9px 13px; border-radius:999px; border:1px solid rgba(34,197,94,.28); background:rgba(34,197,94,.09); color:#4ade80; font-weight:900; letter-spacing:.8px; text-transform:uppercase; }
    .status-pill.expired { border-color:rgba(239,68,68,.35); background:rgba(239,68,68,.10); color:#f87171; }
    .status-pill:before { content:''; width:8px; height:8px; border-radius:50%; background:#22c55e; box-shadow:0 0 14px #22c55e; }
    .status-pill.expired:before { background:#ef4444; box-shadow:0 0 14px #ef4444; }
    .main-grid { display:grid; grid-template-columns:minmax(0,1.12fr) minmax(360px,.88fr); gap:18px; align-items:stretch; position:relative; z-index:1; }
    .panel { background:rgba(0,0,0,.24); border:1px solid rgba(255,255,255,.075); border-radius:24px; padding:20px; }
    .usage-head { display:flex; justify-content:space-between; align-items:center; gap:14px; margin-bottom:18px; }
    .ring { width:132px; height:132px; border-radius:50%; display:grid; place-items:center; background: conic-gradient(${ringColor} 0 ${ringDeg}deg, rgba(255,255,255,.09) ${ringDeg}deg 360deg); box-shadow:0 0 34px rgba(212,175,55,.10); position:relative; flex:0 0 auto; }
    .ring:before { content:''; position:absolute; inset:11px; border-radius:50%; background:#111119; border:1px solid rgba(255,255,255,.06); }
    .ring span { position:relative; z-index:1; font-size:28px; font-weight:900; color:#fff; }
    .usage-title h2 { font-size:22px; margin-bottom:6px; }
    .usage-title p { color:#888; font-size:13px; line-height:1.6; }
    .progress { height:10px; background:rgba(255,255,255,.08); border-radius:999px; overflow:hidden; margin:14px 0 18px; }
    .fill { height:100%; width:${progressWidth}%; background:linear-gradient(90deg,var(--gold),#eab308); border-radius:999px; box-shadow:0 0 18px rgba(212,175,55,.25); }
    .danger { background:linear-gradient(90deg,#ef4444,#f87171)!important; }.greenfill{background:linear-gradient(90deg,#22c55e,#86efac)!important;box-shadow:0 0 18px rgba(34,197,94,.24)!important;}
    .stat-grid { display:grid; grid-template-columns:repeat(2,1fr); gap:12px; }
    .stat { background:rgba(255,255,255,.035); border:1px solid rgba(255,255,255,.065); border-radius:18px; padding:15px; }
    .label { color:#888; font-size:10px; text-transform:uppercase; letter-spacing:1.2px; font-weight:900; }
    .value { margin-top:6px; font-size:20px; font-weight:900; color:#fff; }
    .value.gold { color:var(--gold); }
    .info { display:grid; gap:10px; margin-top:14px; }
    .row { display:flex; justify-content:space-between; gap:14px; color:#ddd; padding:10px 0; border-bottom:1px solid rgba(255,255,255,.055); }
    .row:last-child { border-bottom:0; }
    .row span:first-child { color:#888; }
    .badge { white-space:nowrap; padding:5px 12px; border-radius:999px; font-size:11px; font-weight:900; }
    .active { background:rgba(34,197,94,.13); color:#4ade80; border:1px solid rgba(34,197,94,.22); }
    .expired { background:rgba(239,68,68,.13); color:#f87171; border:1px solid rgba(239,68,68,.22); }
    .qr-tabs { display:flex; gap:8px; padding:6px; border-radius:17px; background:rgba(0,0,0,.28); border:1px solid rgba(255,255,255,.075); margin-bottom:16px; }
    .qr-tab { flex:1; border:0; border-radius:12px; padding:12px 8px; cursor:pointer; color:#aaa; background:transparent; font-weight:900; letter-spacing:.7px; transition:.22s; }
    .qr-tab.active { background:linear-gradient(135deg,var(--gold),var(--gold2)); color:#000; }
    .qr-stage { text-align:center; padding:20px; border-radius:22px; background:rgba(255,255,255,.03); border:1px solid rgba(255,255,255,.06); }
    .qr { width:224px; height:224px; background:#fff; border-radius:20px; display:inline-flex; align-items:center; justify-content:center; padding:12px; margin-bottom:14px; }
    .qr img { width:100%; height:100%; display:block; }
    .transport-info { color:#aaa; font-size:12px; line-height:1.7; margin-bottom:12px; }
    .btn { width:100%; border:0; cursor:pointer; border-radius:14px; padding:14px 16px; font-weight:900; letter-spacing:.3px; margin-top:9px; transition:.22s; }
    .btn:hover { transform:translateY(-1px); filter:brightness(1.05); }
    .btn.primary { background:linear-gradient(135deg,var(--gold),#b2891d); color:#000; box-shadow:0 12px 26px rgba(212,175,55,.16); }
    .btn.dark { background:rgba(0,0,0,.32); color:#ddd; border:1px solid rgba(255,255,255,.095); }
    .guide { color:#777; text-align:center; font-size:12px; line-height:1.7; margin-top:16px; }
    .show-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin:12px 0}.show-btn{border:1px solid rgba(212,175,55,.22);background:rgba(212,175,55,.07);color:#d4af37;border-radius:12px;padding:10px;font-weight:900;cursor:pointer}.qr-full{display:none;position:fixed;inset:0;background:rgba(0,0,0,.78);backdrop-filter:blur(8px);z-index:900;align-items:center;justify-content:center;padding:20px}.qr-full-card{background:#111119;border:1px solid rgba(255,255,255,.1);border-radius:26px;padding:24px;width:min(420px,100%);text-align:center;box-shadow:0 30px 80px rgba(0,0,0,.6)}.qr-full-card .qr{width:300px;height:300px}.qr-full-card .qr img{width:276px;height:276px}.toast { position:fixed; right:20px; bottom:20px; background:#15151d; color:#fff; border:1px solid rgba(212,175,55,.35); padding:12px 16px; border-radius:14px; z-index:999; box-shadow:0 15px 35px rgba(0,0,0,.42); animation:toastIn .22s ease; }
    @keyframes toastIn { from { opacity:0; transform:translateY(10px); } to { opacity:1; transform:translateY(0); } }
    @media(max-width:850px){ body{padding:14px}.card{padding:20px;border-radius:24px}.hero{flex-direction:column}.status-box{text-align:center}.main-grid{grid-template-columns:1fr}.usage-head{flex-direction:column;text-align:center}.stat-grid{grid-template-columns:1fr}.qr{width:190px;height:190px}.qr-tabs{flex-direction:column}h1{font-size:30px}.brand-title{font-size:12px;letter-spacing:4px} }
  </style>
</head>
<body>
  <div class="orb one"></div><div class="orb two"></div>
  <main class="shell">
    <section class="card">
      <div class="hero">
        <div>
          <div class="brand-title">MAHAN PANEL</div>
          <h1>${remarkSafe}</h1>
          <div class="chips"><span class="chip gold">XHTTP</span><span class="chip gold">WebSocket</span><span class="chip">UDP</span><span class="chip">MUX</span></div>
        </div>
        <div class="status-box"><div id="statusPill" class="status-pill ${expired ? 'expired' : ''}">${statusText}</div></div>
      </div>
      <div class="main-grid">
        <section class="panel">
          <div class="usage-head"><div class="usage-title"><h2>Traffic Usage</h2></div><div class="ring" id="usageRing"><span id="ringValue">${percent}%</span></div></div>
          <div class="progress"><div id="trafficFill" class="fill ${isDanger ? 'danger' : (unlimitedTraffic ? 'greenfill' : '')}"></div></div>
          <div class="stat-grid">
            <div class="stat"><div class="label">Used</div><div class="value" id="usedValue">${usedFormatted}</div></div>
            <div class="stat"><div class="label">Remaining</div><div class="value gold" id="remainingValue">${remainingFormatted}</div></div>
            <div class="stat"><div class="label">Upload</div><div class="value" id="uploadValue">${uplinkFormatted}</div></div>
            <div class="stat"><div class="label">Download</div><div class="value" id="downloadValue">${downlinkFormatted}</div></div>
          </div>
          <div class="info"><div class="row"><span>Status</span><b id="statusBadge" class="badge ${expired ? 'expired' : 'active'}">${expired ? 'Expired' : 'Active'}</b></div><div class="row"><span>Protocol</span><b>VLESS over XHTTP + WS</b></div><div class="row"><span>Expiration</span><b id="expiryValue">${expiryStr}</b></div><div class="row"><span>Last Seen</span><b id="lastSeenValue">${lastSeenStr}</b></div></div>
        </section>
        <section class="panel">
          <div class="qr-tabs"><button class="qr-tab active" id="tab-xhttp" onclick="setQrTab('xhttp')">XHTTP</button><button class="qr-tab" id="tab-ws" onclick="setQrTab('ws')">WebSocket</button><button class="qr-tab" id="tab-sub" onclick="setQrTab('sub')">Subscription</button></div>
          <div class="qr-stage"><div class="transport-info" id="transportInfo">Transport: XHTTP · Path: /xhttp · UDP/MUX supported</div><button class="btn primary" id="copyActiveBtn" onclick="copyActiveConfig()">Copy XHTTP Config</button><button class="btn dark" id="showQrBtn" onclick="showFullQr(ACTIVE_QR)">Show QR</button></div>
          <button class="btn primary" onclick="copyAll()">Copy All Configs</button>
          <p class="guide">VPN clients receive both configs automatically. Browser users see this page.</p>
        </section>
      </div>
    </section>
  </main><div class="qr-full" id="qrFull" onclick="hideFullQr(event)"><div class="qr-full-card"><h2 id="qrFullTitle" style="margin-bottom:16px;color:#d4af37">QR Code</h2><div class="qr"><img id="qrFullImg" alt="Full QR"></div><button class="btn primary" onclick="downloadQr()">Download QR</button><button class="btn dark" onclick="document.getElementById('qrFull').style.display='none'">Close</button></div></div>
  <script>var WS_LINK=${wsLinkJSON};var XHTTP_LINK=${xhttpLinkJSON};var ALL_LINKS=${allLinksJSON};var SUB_URL=${subUrlJSON};var QR_BASE='${qrBase}';var ACTIVE_QR='xhttp';function copyText(t){navigator.clipboard.writeText(t).then(function(){showToast('Copied')}).catch(function(){prompt('Copy:',t)})}function showToast(t){var e=document.createElement('div');e.className='toast';e.textContent=t;document.body.appendChild(e);setTimeout(function(){e.remove()},2200)}function activePayload(){return ACTIVE_QR==='xhttp'?XHTTP_LINK:(ACTIVE_QR==='ws'?WS_LINK:SUB_URL)}function copyActiveConfig(){copyText(activePayload())}function copyAll(){copyText(ALL_LINKS)}function setQrTab(t){ACTIVE_QR=t;['xhttp','ws','sub'].forEach(function(x){document.getElementById('tab-'+x).classList.toggle('active',x===t)});var label=t==='xhttp'?'XHTTP Config':(t==='ws'?'WebSocket Config':'Subscription URL');document.getElementById('copyActiveBtn').textContent='Copy '+label;document.getElementById('transportInfo').textContent=t==='xhttp'?'Transport: XHTTP · Path: /xhttp · UDP/MUX supported':(t==='ws'?'Transport: WebSocket · Path: /ws · UDP/MUX supported':'Smart subscription URL · Includes XHTTP and WebSocket')}function showFullQr(t){setQrTab(t);document.getElementById('qrFullTitle').textContent=(t==='xhttp'?'XHTTP':(t==='ws'?'WebSocket':'Subscription'))+' QR';document.getElementById('qrFullImg').src=QR_BASE+'/'+t+'?ts='+Date.now();document.getElementById('qrFull').style.display='flex'}function hideFullQr(e){if(e.target.id==='qrFull')e.currentTarget.style.display='none'}function downloadQr(){var src=(document.getElementById('qrFull').style.display==='flex'?document.getElementById('qrFullImg').src:(QR_BASE+'/'+ACTIVE_QR));var c=document.createElement('canvas');c.width=900;c.height=900;var x=c.getContext('2d');x.fillStyle='#fff';x.fillRect(0,0,c.width,c.height);var i=new Image();i.onload=function(){x.drawImage(i,40,40,820,820);var a=document.createElement('a');a.href=c.toDataURL('image/jpeg',0.92);a.download='qr-'+ACTIVE_QR+'.jpg';a.click()};i.src=src}function applySubSnapshot(d){if(!d)return;document.getElementById('usedValue').textContent=d.used_formatted;document.getElementById('remainingValue').textContent=d.remaining_formatted;document.getElementById('uploadValue').textContent=d.uplink_formatted;document.getElementById('downloadValue').textContent=d.downlink_formatted;document.getElementById('ringValue').textContent=d.percent_label||((d.percent||0)+'%');var deg=d.unlimited&&!d.is_danger?360:(Number(d.percent)||0)*3.6;if(d.is_danger)deg=360;var color=d.is_danger?'#ef4444':(d.unlimited?'#22c55e':'var(--gold)');document.getElementById('usageRing').style.background='conic-gradient('+color+' 0 '+deg+'deg, rgba(255,255,255,.09) '+deg+'deg 360deg)';var f=document.getElementById('trafficFill');f.style.width=d.progress_width+'%';f.className='fill '+(d.is_danger?'danger':(d.unlimited?'greenfill':''));var b=document.getElementById('statusBadge');b.textContent=d.status;b.className='badge '+d.status_class;var p=document.getElementById('statusPill');p.textContent=d.status;p.className='status-pill '+(d.status==='ACTIVE'?'':'expired');document.getElementById('expiryValue').textContent=d.expiry;document.getElementById('lastSeenValue').textContent=d.last_seen}try{var es=new EventSource(location.pathname.replace(/\\/$/,'')+'/events');es.addEventListener('snapshot',function(e){applySubSnapshot(JSON.parse(e.data))});es.addEventListener('deleted',function(){document.body.innerHTML='<div style="color:white;text-align:center;padding:40px">Subscription deleted</div>'})}catch(e){}</script></body></html>`);
});

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
  let muxMode = false;
  let muxHandler = null;
  let udpTargetHost = '';
  let udpTargetPort = 0;
  let udpIdleTimer = null;
  const udpState = { buffer: Buffer.alloc(0) };
  const udpBurstState = createUdpBurstState();
  const udpDownlinkBurstState = createUdpBurstState();
  const pendingClientChunks = [];
  let wsAlive = true;
  let boosterProfile = BOOSTER_PROFILE_BALANCED;

  let wsPingTimer = null;
  function scheduleWsPing() {
    clearTimeout(wsPingTimer);
    wsPingTimer = setTimeout(() => {
      if (closed) return;
      if (!wsAlive) {
        closeAll(1000, 'websocket ping timeout');
        try { ws.terminate(); } catch (_) {}
        return;
      }
      wsAlive = false;
      try { ws.ping(); } catch (_) { closeAll(1011, 'websocket ping failed'); return; }
      scheduleWsPing();
    }, boosterProfile.wsPingIntervalMs || WS_PING_INTERVAL_MS);
  }
  scheduleWsPing();

  let idleTimer = setTimeout(() => closeAll(1000, 'idle timeout'), boosterProfile.idleTimeoutMs || IDLE_TIMEOUT_MS);
  let connectTimer = setTimeout(() => closeAll(1013, 'connect timeout'), boosterProfile.connectTimeoutMs || CONNECT_TIMEOUT_MS);

  function resetIdleTimer() {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(() => closeAll(1000, 'idle timeout'), boosterProfile.idleTimeoutMs || IDLE_TIMEOUT_MS);
  }

  function resetConnectTimer() {
    clearTimeout(connectTimer);
    connectTimer = setTimeout(() => closeAll(1013, 'connect timeout'), boosterProfile.connectTimeoutMs || CONNECT_TIMEOUT_MS);
  }

  function refreshIdleTimer() {
    unregisterConnection?.touch?.();
    if (idleTimer.refresh) idleTimer.refresh();
    else resetIdleTimer();
  }

  function closeAll(code = 1000, reason = '') {
    if (closed) return;
    closed = true;
    clearTimeout(idleTimer);
    clearTimeout(connectTimer);
    clearTimeout(wsPingTimer);
    clearTimeout(udpIdleTimer);
    if (unregisterConnection) unregisterConnection();
    destroySocket(remote);
    closeUdpSocket(udpSocket);
    muxHandler?.close();
    safeCloseWebSocket(ws, code, reason);
  }

  function sendToWs(chunk, meta = {}) {
    if (closed || ws.readyState !== ws.OPEN) return false;
    const backpressureLimit = boosterProfile.wsBackpressureLimit || WS_BACKPRESSURE_LIMIT;
    const backpressureResume = boosterProfile.wsBackpressureResume || WS_BACKPRESSURE_RESUME;
    if (meta.udp && shouldDropUdpForLatency(boosterProfile, udpDownlinkBurstState, ws.bufferedAmount, backpressureLimit)) return false;
    if (ws.bufferedAmount > backpressureLimit) {
      if (remote) remote.pause();
      const interval = setInterval(() => {
        if (closed || ws.readyState !== ws.OPEN) { clearInterval(interval); return; }
        if (ws.bufferedAmount < backpressureResume) {
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
      updateTraffic(user, chunk.length, 'uplink', () => unregisterConnection?.touchData?.());
      if (!remote.write(chunk)) break;
    }
  }

  function sendUdpPacket(packet) {
    if (closed || !udpSocket || !udpTargetHost || !udpTargetPort) return;
    if (shouldDropUdpForLatency(boosterProfile, udpBurstState, ws.bufferedAmount, boosterProfile.wsBackpressureLimit || WS_BACKPRESSURE_LIMIT)) return;
    if (udpIdleTimer && udpIdleTimer.refresh) udpIdleTimer.refresh();
    updateTraffic(user, packet.length, 'uplink', () => unregisterConnection?.touchData?.());
    const family = getUdpSocketFamily(udpSocket, udpTargetHost);
    resolveBoostedAddress(udpTargetHost, udpTargetPort, boosterProfile, family).then((address) => {
      if (closed || !udpSocket) return;
      udpSocket.send(packet, udpTargetPort, address, (err) => {
        if (err) {
          log('[UDP] Send failed:', err.message);
          markBoosterConnectFailure(udpTargetHost, udpTargetPort, address, err);
          closeAll(1011, 'udp send failed');
        }
      });
    }).catch((err) => {
      log('[UDP] Resolve failed:', err && err.message ? err.message : 'unknown');
      markBoosterConnectFailure(udpTargetHost, udpTargetPort, '', err);
      closeAll(1011, 'udp resolve failed');
    });
  }

  function handleClientData(data) {
    refreshIdleTimer();
    const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);

    if (!firstPacketHandled) {
      firstPacketHandled = true;
      let request;
      try {
        request = parseVlessRequest(chunk, { allowUdp: true, allowMux: true });
      } catch (err) {
        log('[VLESS] Bad request:', err.message);
        closeAll(1008, 'bad vless request');
        return;
      }

      user = request.user;
      boosterProfile = chooseBoosterProfile(request, 'ws');
      resetIdleTimer();
      resetConnectTimer();
      scheduleWsPing();
      const clientIp = getClientIp(req);
      const deviceMeta = getConnectionDeviceMeta(req, clientIp, 'ws', request);
      const candidate = findBestDeviceCandidate(user.id, deviceMeta);
      const clientKey = deviceMeta.endpointKey || candidate?.device?.id || '';
      if (!canAcceptUserSession(user, clientKey)) {
        log('[LIMIT] Client limit exceeded:', `${user.remark} ip=${clientIp}`);
        closeAll(1008, 'client limit exceeded');
        return;
      }
      unregisterConnection = registerConnection(user.id, (reason) => closeAll(1000, reason), { ip: clientIp, clientKey, kind: 'ws', deviceMeta });
      log('[VLESS] Connect:', `${user.remark} -> ${request.host}:${request.port} ${describeBoosterProfile(boosterProfile)} device=${unregisterConnection.clientKey || clientKey}`);

      if (request.command === 0x03) {
        muxMode = true;
        clearTimeout(connectTimer);
        established = true;
        sendVlessResponseHeader(request.version);
        muxHandler = new MuxHandler({
          user,
          label: 'WS MUX',
          profile: boosterProfile,
          touch: () => unregisterConnection?.touchData?.(),
          send: (chunk) => sendToWs(chunk),
          close: (reason) => closeAll(1000, reason || 'mux closed')
        });
        try {
          if (request.payload.length) muxHandler.feed(request.payload);
          while (pendingClientChunks.length) muxHandler.feed(pendingClientChunks.shift());
        } catch (err) {
          log('[MUX] Bad frame:', err.message);
          closeAll(1008, 'bad mux frame');
        }
        return;
      }

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
          updateTraffic(user, msg.length, 'downlink', () => unregisterConnection?.touchData?.());
          try {
            sendToWs(encodeUdpPacket(msg), { udp: true });
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

        udpIdleTimer = setTimeout(() => closeAll(1000, 'udp idle timeout'), boosterProfile.udpIdleTimeoutMs || UDP_IDLE_TIMEOUT_MS);

        try {
          drainUdpPackets(udpState, request.payload, sendUdpPacket);
          while (pendingClientChunks.length) drainUdpPackets(udpState, pendingClientChunks.shift(), sendUdpPacket);
        } catch (err) {
          log('[UDP] Bad packet:', err.message);
          closeAll(1008, 'bad udp packet');
        }
        return;
      }

      remote = createBoostedTcpConnection(request.host, request.port, boosterProfile);
      remote.setNoDelay(true);
      remote.setKeepAlive(true, 30000);
      remote.setTimeout(boosterProfile.idleTimeoutMs || IDLE_TIMEOUT_MS);

      remote.once('connect', () => {
        if (closed) return;
        clearTimeout(connectTimer);
        established = true;
        sendVlessResponseHeader(request.version);
        if (request.payload.length) {
          updateTraffic(user, request.payload.length, 'uplink', () => unregisterConnection?.touchData?.());
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
        updateTraffic(user, remoteChunk.length, 'downlink', () => unregisterConnection?.touchData?.());
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

    if (muxMode) {
      try {
        muxHandler?.feed(chunk);
      } catch (err) {
        log('[MUX] Bad frame:', err.message);
        closeAll(1008, 'bad mux frame');
      }
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

    updateTraffic(user, chunk.length, 'uplink', () => unregisterConnection?.touchData?.());
    if (!remote.write(chunk)) {
      try { req.socket.pause(); } catch (_) {}
    }
  }

  ws.on('pong', () => {
    wsAlive = true;
    refreshIdleTimer();
  });

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
  const now = Date.now();
  for (const [userId, map] of Array.from(activeConnections.entries())) {
    for (const conn of Array.from(map.values())) {
      // Do NOT close a connection just because it has no traffic bytes. A VPN can
      // be connected and idle for seconds/minutes. Only heartbeat/socket staleness
      // should remove it; explicit client close is handled immediately by close events.
      if (now - conn.lastSeen > ACTIVE_CONNECTION_STALE_MS) {
        try { conn.closeFn('connection stale'); } catch (_) {}
        removeConnection(userId, conn.id, 'stale cleaner');
      }
    }
    pruneClientSlots(userId);
  }
  for (const userId of Array.from(activeDevices.keys())) pruneClientSlots(userId);
  for (const userId of Array.from(activeClientActivity.keys())) pruneActiveClientActivity(userId);
}, CONNECTION_CLEANUP_INTERVAL_MS);

setInterval(() => {
  flushTrafficBuffers();
}, 1000);

setInterval(() => {
  sampleCpuUsage();
}, 1000);

setInterval(() => {
  if (adminEventClients.size || subEventClients.size) broadcastRealtimeNow();
}, 2000);

setInterval(() => {
  trafficHistory.push({ ts: Date.now(), bytes: trafficCurrentMinuteBytes });
  trafficCurrentMinuteBytes = 0;
  if (trafficHistory.length > 59) trafficHistory = trafficHistory.slice(-59);
  scheduleRealtimeBroadcast();
}, 60 * 1000);

setInterval(() => {
  for (const user of DB.users) {
    if (user.is_active === 1 && isUserExpired(user)) {
      user.is_active = 0;
      closeUserConnections(user.id, 'expired');
      scheduleRealtimeBroadcast();
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
  try { flushTrafficBuffers(); saveDBNow(); } catch (_) {}
  process.exit(0);
});
process.on('SIGINT', () => {
  try { flushTrafficBuffers(); saveDBNow(); } catch (_) {}
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
