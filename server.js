'use strict';

const http = require('http');
const net = require('net');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT    = parseInt(process.env.PORT) || 8080;
const WS_PATH = process.env.WS_PATH || '/ws';
const RAW_IDS = process.env.USER_IDS || process.env.UUID || '';

function loadUUIDs() {
  if (!RAW_IDS.trim()) {
    const id = crypto.randomUUID();
    console.log('[BOOT] UUID موقت:', id);
    return new Set([id]);
  }
  const list = RAW_IDS.split(',').map(s => s.trim()).filter(Boolean);
  console.log('[BOOT] UUIDs:', [...list]);
  return new Set(list);
}

const VALID_UUIDS = loadUUIDs();

function uuidToBytes(uuid) {
  return Buffer.from(uuid.replace(/-/g, ''), 'hex');
}

function checkUUID(buf16) {
  for (const uuid of VALID_UUIDS) {
    try {
      if (crypto.timingSafeEqual(buf16, uuidToBytes(uuid))) return true;
    } catch (_) {}
  }
  return false;
}

function parseTarget(buf, off) {
  const type = buf[off++];
  let host;

  if (type === 0x01) {
    host = `${buf[off]}.${buf[off+1]}.${buf[off+2]}.${buf[off+3]}`;
    off += 4;
  } else if (type === 0x02) {
    const len = buf[off++];
    host = buf.slice(off, off + len).toString('utf8');
    off += len;
  } else if (type === 0x03) {
    const p = [];
    for (let i = 0; i < 8; i++) p.push(buf.readUInt16BE(off + i * 2).toString(16));
    host = '[' + p.join(':') + ']';
    off += 16;
  } else {
    throw new Error('addr type نامعتبر: 0x' + type.toString(16));
  }

  const port = buf.readUInt16BE(off);
  off += 2;
  return { host, port, off };
}

function handleVLESS(ws, clientIP) {
  const sid   = crypto.randomBytes(3).toString('hex').toUpperCase();
  let ready   = false;
  let tcp     = null;
  let queue   = [];
  let accum   = Buffer.alloc(0);

  console.log(`[${sid}] اتصال جدید از ${clientIP}`);

  function sendWS(data) {
    if (ws.readyState === 1) {
      ws.send(data, { binary: true });
    }
  }

  function die(reason) {
    console.log(`[${sid}] بسته شد: ${reason}`);
    if (tcp && !tcp.destroyed) tcp.destroy();
    try { if (ws.readyState < 2) ws.close(1000); } catch (_) {}
  }

  function tryParseHeader(buf) {
    // حداقل: 1+16+1+1+2+1 = 22 بایت
    if (buf.length < 22) return false;

    let off = 0;

    // Version
    if (buf[off++] !== 0x00) { die('version اشتباه'); return false; }

    // UUID
    const rxUUID = buf.slice(off, off + 16);
    off += 16;
    if (!checkUUID(rxUUID)) { die('UUID نامعتبر'); return false; }

    // Addon Length
    const addonLen = buf[off++];
    if (buf.length < off + addonLen + 4) return false;
    off += addonLen;

    // Command
    const cmd = buf[off++];
    if (cmd !== 0x01) { die('فقط TCP پشتیبانی میشه، cmd=' + cmd); return false; }

    // Port
    const port = buf.readUInt16BE(off);
    off += 2;

    // Address
    let target;
    try {
      target = parseTarget(buf, off);
    } catch (e) {
      die('آدرس نامعتبر: ' + e.message);
      return false;
    }

    const { host, off: nextOff } = target;
    off = nextOff;

    const payload = buf.slice(off);

    console.log(`[${sid}] → ${host}:${port} | payload=${payload.length}B`);
    connect(host, port, payload);
    return true;
  }

  function connect(host, port, initData) {
    tcp = net.createConnection({ host, port });
    tcp.setTimeout(20000);

    tcp.once('connect', () => {
      console.log(`[${sid}] TCP متصل → ${host}:${port}`);

      // ── پاسخ VLESS ──
      // دقیقاً 2 بایت
      sendWS(Buffer.from([0x00, 0x00]));

      if (initData.length > 0) tcp.write(initData);
      for (const c of queue) tcp.write(c);
      queue = [];
      ready = true;
    });

    tcp.on('data', (chunk) => sendWS(chunk));

    tcp.on('timeout', () => { console.log(`[${sid}] timeout`); tcp.destroy(); });

    tcp.on('error', (err) => {
      console.log(`[${sid}] TCP error: ${err.code} ${err.message}`);
      die(err.message);
    });

    tcp.on('close', () => {
      console.log(`[${sid}] TCP بسته شد`);
      try { if (ws.readyState < 2) ws.close(1000); } catch (_) {}
    });
  }

  ws.on('message', (data, isBinary) => {
    if (!Buffer.isBuffer(data)) data = Buffer.from(data);

    if (!ready) {
      accum = Buffer.concat([accum, data]);
      tryParseHeader(accum);
    } else {
      if (tcp && !tcp.destroyed) tcp.write(data);
      else queue.push(data);
    }
  });

  ws.on('close', (code) => {
    console.log(`[${sid}] WS بسته شد، code=${code}`);
    if (tcp && !tcp.destroyed) tcp.destroy();
  });

  ws.on('error', (err) => {
    console.log(`[${sid}] WS error: ${err.message}`);
    if (tcp && !tcp.destroyed) tcp.destroy();
  });
}

// ── HTTP ──────────────────────────────────────────────
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('ok');
});

// ── WebSocket ─────────────────────────────────────────
const wss = new WebSocketServer({ server, path: WS_PATH });

wss.on('connection', (ws, req) => {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '?')
               .split(',')[0].trim();
  handleVLESS(ws, ip);
});

// ── Listen ────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  console.log(`پورت: ${PORT} | مسیر: ${WS_PATH}`);
});
