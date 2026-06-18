'use strict';

const http = require('http');
const net = require('net');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const express = require('express');
const cookieParser = require('cookie-parser');
const { WebSocketServer } = require('ws');

const app = express();
const PORT = Number(process.env.PORT || 3000); // Railway provides PORT automatically.
const WS_PATH = '/ws';
const DB_PATH = path.join(__dirname, 'db.json');

const PASSWORD_SALT = 'MAHAN_PANEL_NODE_WS_SALT_v2';
const CONNECT_TIMEOUT_MS = 15000;
const IDLE_TIMEOUT_MS = 120000;
const MAX_FIRST_PACKET = 128 * 1024;
const MAX_WS_PAYLOAD = 16 * 1024 * 1024;
const WS_BACKPRESSURE_LIMIT = 8 * 1024 * 1024;
const WS_BACKPRESSURE_RESUME = 2 * 1024 * 1024;
const DB_SAVE_DEBOUNCE_MS = 1500;

let DB = { admin: null, users: [] };
let dbSaveTimer = null;
const activeSessions = new Set();
const activeConnections = new Map(); // user.id -> Set(closeFn)

app.use(express.json({ limit: '256kb' }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

function log(message, extra = '') {
  const suffix = extra ? ` ${extra}` : '';
  console.log(`[${new Date().toISOString()}] ${message}${suffix}`);
}

function generateHash(password) {
  return crypto.createHash('sha256').update(String(password) + PASSWORD_SALT).digest('hex');
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
    protocol: 'ws',
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

function parseVlessRequest(buffer) {
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
  if (command !== 0x01) {
    if (command === 0x02) throw new Error('UDP is not supported; disable UDP/xudp in client');
    if (command === 0x03) throw new Error('MUX is not supported; disable MUX in client');
    throw new Error(`unsupported command: ${command}`);
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

  return { version, user, host, port, payload: buffer.subarray(offset) };
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

function buildVlessLink(req, user) {
  const rawHost = String(req.get('host') || '').trim();
  const forwardedProto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
  const proto = forwardedProto || req.protocol || 'https';
  const isTls = proto === 'https';

  // For Railway/custom domains rawHost is usually just domain.com.
  // For local tests it may be 127.0.0.1:3000, so split the port safely enough for common hostnames.
  const host = rawHost.includes(':') && !rawHost.startsWith('[') ? rawHost.split(':')[0] : rawHost.replace(/^\[/, '').replace(/\](:\d+)?$/, '');
  const explicitPort = rawHost.includes(':') && !rawHost.startsWith('[') ? rawHost.split(':').pop() : '';
  const port = isTls ? '443' : (explicitPort || '80');
  const security = isTls ? 'tls' : 'none';
  const tag = encodeURIComponent(user.remark || 'Mahan-WS');

  let link = `vless://${user.uuid}@${host}:${port}?encryption=none&security=${security}&type=ws&host=${encodeURIComponent(host)}&path=%2Fws`;
  if (isTls) link += `&sni=${encodeURIComponent(host)}`;
  link += `#${tag}`;
  return link;
}

function requireAuth(req, res, next) {
  const token = req.cookies.admin_session;
  if (token && activeSessions.has(token)) return next();
  return res.status(401).json({ error: 'Unauthorized' });
}

app.get('/health', (req, res) => res.type('text/plain').send('ok\n'));

app.post('/api/login', (req, res) => {
  const { username, password } = req.body || {};
  const hash = generateHash(password || '');
  if (DB.admin && DB.admin.username === username && DB.admin.password_hash === hash) {
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
  return res.status(401).json({ success: false });
});

app.post('/api/logout', requireAuth, (req, res) => {
  activeSessions.delete(req.cookies.admin_session);
  res.clearCookie('admin_session');
  res.json({ success: true });
});

app.get('/api/stats', requireAuth, (req, res) => {
  const memory = process.memoryUsage();
  const totalTraffic = DB.users.reduce((sum, u) => sum + Number(u.used_bytes || 0), 0);
  res.json({
    ram_usage: Math.round((memory.heapUsed / Math.max(1, memory.heapTotal)) * 100),
    uptime: Math.round(process.uptime()),
    total_traffic: totalTraffic,
    users_count: DB.users.length,
    active_count: DB.users.filter((u) => u.is_active === 1 && !isUserExpired(u)).length,
    ws_connections: Array.from(activeConnections.values()).reduce((sum, set) => sum + set.size, 0)
  });
});

app.get('/api/users', requireAuth, (req, res) => {
  res.json({ users: DB.users });
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
    protocol: 'ws',
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

app.get('/sub/:sub_id', (req, res) => {
  const user = DB.users.find((u) => u.sub_id === req.params.sub_id);
  if (!user) return res.status(404).send('Subscription Not Found');

  const vlessLink = buildVlessLink(req, user);
  const ua = String(req.headers['user-agent'] || '').toLowerCase();
  const isClient = ua.includes('v2ray') || ua.includes('nekobox') || ua.includes('shadowrocket') ||
    ua.includes('clash') || ua.includes('surge') || ua.includes('quantumult') ||
    ua.includes('v2rayng') || ua.includes('v2rayn') || ua.includes('sing-box') || ua.includes('hiddify');

  if (isClient || req.query.raw === '1') {
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    return res.send(Buffer.from(vlessLink).toString('base64'));
  }

  const usedFormatted = formatBytes(user.used_bytes);
  const remainingFormatted = user.traffic_limit === 0 ? 'Unlimited' : formatBytes(Math.max(0, user.traffic_limit - user.used_bytes));
  const percent = calcPercent(user.used_bytes, user.traffic_limit).toFixed(1);
  const progressWidth = calcPercent(user.used_bytes, user.traffic_limit).toFixed(2);
  const isDanger = user.traffic_limit > 0 && (user.used_bytes / user.traffic_limit) > 0.9;
  const expired = isUserExpired(user);
  const expiryStr = user.expiry_time === 0 ? 'Never' : new Date(user.expiry_time).toLocaleDateString();
  const lastSeenStr = user.last_seen === 0 ? 'Never' : new Date(user.last_seen).toLocaleString();
  const remarkSafe = safeName(user.remark);
  const vlessLinkJSON = JSON.stringify(vlessLink);

  res.send(`<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>${remarkSafe} - Subscription</title>
<style>
*{box-sizing:border-box;margin:0;padding:0;font-family:Arial,sans-serif}body{background:radial-gradient(circle at top right,#1a1a2e,#0f0f16);color:#fff;min-height:100vh;display:flex;justify-content:center;align-items:center;padding:20px}.card{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:24px;padding:36px;width:100%;max-width:450px;box-shadow:0 25px 50px rgba(0,0,0,.5)}h1{text-align:center;color:#d4af37;margin-bottom:6px}.sub{text-align:center;color:#888;margin-bottom:28px;letter-spacing:2px}.stats{display:flex;gap:12px;margin-bottom:22px}.box{flex:1;background:rgba(0,0,0,.25);border-radius:16px;padding:15px;text-align:center}.label{color:#888;font-size:11px}.value{font-size:20px;font-weight:700;margin-top:5px}.progress{height:8px;background:rgba(255,255,255,.08);border-radius:8px;overflow:hidden;margin:10px 0 24px}.fill{height:100%;background:linear-gradient(90deg,#d4af37,#eab308);width:${progressWidth}%}.danger{background:linear-gradient(90deg,#ef4444,#f87171)}.info{background:rgba(0,0,0,.25);border-radius:16px;padding:18px;margin-bottom:22px}.row{display:flex;justify-content:space-between;margin:10px 0;color:#ddd}.row span:first-child{color:#888}.badge{padding:4px 10px;border-radius:20px;font-size:11px;font-weight:700}.active{background:rgba(34,197,94,.12);color:#4ade80}.expired{background:rgba(239,68,68,.12);color:#f87171}.btn{width:100%;background:linear-gradient(45deg,#d4af37,#b2891d);color:#000;border:0;padding:16px;border-radius:12px;font-weight:800;cursor:pointer}.guide{text-align:center;margin-top:16px;color:#777;font-size:12px}
</style></head><body><div class="card"><h1>${remarkSafe}</h1><div class="sub">VLESS WS SUBSCRIPTION</div>
<div class="stats"><div class="box"><div class="label">Used</div><div class="value">${usedFormatted}</div></div><div class="box"><div class="label">Remaining</div><div class="value">${remainingFormatted}</div></div></div>
<div style="display:flex;justify-content:space-between;color:#aaa;font-size:12px"><span>Traffic</span><span>${percent}%</span></div><div class="progress"><div class="fill ${isDanger ? 'danger' : ''}"></div></div>
<div class="info"><div class="row"><span>Status</span><b class="badge ${expired ? 'expired' : 'active'}">${expired ? 'Expired' : 'Active'}</b></div><div class="row"><span>Protocol</span><b>VLESS over WS</b></div><div class="row"><span>Expiration</span><b>${expiryStr}</b></div><div class="row"><span>Last Seen</span><b>${lastSeenStr}</b></div></div>
<button class="btn" onclick="copyConfig()">Copy Config</button><div class="guide">You can import this page URL directly in many clients.</div></div>
<script>var VLESS_LINK=${vlessLinkJSON};function copyConfig(){navigator.clipboard.writeText(VLESS_LINK).then(function(){alert('Copied!')}).catch(function(){prompt('Copy:',VLESS_LINK)})}</script></body></html>`);
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
  let user = null;
  let unregisterConnection = null;
  let established = false;
  let closed = false;
  let firstPacketHandled = false;
  let responseHeaderSent = false;
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
    if (unregisterConnection) unregisterConnection();
    destroySocket(remote);
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

  function handleClientData(data) {
    refreshIdleTimer();
    const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);

    if (!firstPacketHandled) {
      firstPacketHandled = true;
      let request;
      try {
        request = parseVlessRequest(chunk);
      } catch (err) {
        log('[VLESS] Bad request:', err.message);
        closeAll(1008, 'bad vless request');
        return;
      }

      user = request.user;
      unregisterConnection = registerConnection(user.id, (reason) => closeAll(1000, reason));
      log('[VLESS] Connect:', `${user.remark} -> ${request.host}:${request.port}`);

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
    const url = 'https://www.google.com';
    fetch(url, { method: 'HEAD' }).catch(() => {});
  } catch (_) {}
}, 4 * 60 * 1000);

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
  log('[INFO] Mahan Panel VLESS-WS active on port', String(PORT));
  log('[INFO] WS path:', WS_PATH);
  log('[INFO] Default admin:', 'admin / admin');
});
