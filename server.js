'use strict';

const http = require('http');
const net = require('net');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

// ─────────────────────────────────────────
//  تنظیمات
// ─────────────────────────────────────────
const PORT     = parseInt(process.env.PORT)    || 3000;
const WS_PATH  = process.env.WS_PATH           || '/tunnel';
const RAW_IDS  = process.env.USER_IDS          || process.env.UUID || '';

// ─────────────────────────────────────────
//  UUID ها
// ─────────────────────────────────────────
function loadUUIDs() {
  if (!RAW_IDS.trim()) {
    const id = crypto.randomUUID();
    console.log('[BOOT] UUID تنظیم نشده - موقت:', id);
    return new Set([id]);
  }
  const list = RAW_IDS.split(',').map(s => s.trim()).filter(Boolean);
  console.log('[BOOT] تعداد UUID بارگذاری شده:', list.length);
  return new Set(list);
}

const VALID_UUIDS = loadUUIDs();

// ─────────────────────────────────────────
//  تبدیل UUID به 16 بایت
// ─────────────────────────────────────────
function uuidToBytes(uuid) {
  const hex = uuid.replace(/-/g, '');
  if (hex.length !== 32) throw new Error('UUID نامعتبر');
  return Buffer.from(hex, 'hex');
}

// ─────────────────────────────────────────
//  مقایسه امن
// ─────────────────────────────────────────
function checkUUID(received16Bytes) {
  for (const uuid of VALID_UUIDS) {
    try {
      const valid = uuidToBytes(uuid);
      if (crypto.timingSafeEqual(received16Bytes, valid)) return true;
    } catch (_) {}
  }
  return false;
}

// ─────────────────────────────────────────
//  پارس کردن آدرس مقصد از هدر VLESS
// ─────────────────────────────────────────
function parseTarget(buf, offset) {
  const type = buf[offset++];
  let host;

  if (type === 0x01) {
    // IPv4
    host = `${buf[offset]}.${buf[offset+1]}.${buf[offset+2]}.${buf[offset+3]}`;
    offset += 4;
  } else if (type === 0x02) {
    // Domain
    const len = buf[offset++];
    host = buf.slice(offset, offset + len).toString();
    offset += len;
  } else if (type === 0x03) {
    // IPv6
    const parts = [];
    for (let i = 0; i < 8; i++) {
      parts.push(buf.readUInt16BE(offset + i * 2).toString(16));
    }
    host = '[' + parts.join(':') + ']';
    offset += 16;
  } else {
    throw new Error(`نوع آدرس ناشناخته: ${type}`);
  }

  const port = buf.readUInt16BE(offset);
  offset += 2;

  return { host, port, offset };
}

// ─────────────────────────────────────────
//  مدیریت هر اتصال VLESS
// ─────────────────────────────────────────
function handleVLESS(ws) {
  const sid   = crypto.randomBytes(3).toString('hex');
  let   ready = false;       // هدر پارس شد؟
  let   tcp   = null;        // سوکت مقصد
  let   queue = [];          // بافر قبل از اتصال
  let   accumulator = Buffer.alloc(0);  // جمع‌آوری هدر

  function send(data) {
    if (ws.readyState === 1) {
      ws.send(data, { binary: true });
    }
  }

  function die(reason) {
    console.log(`[${sid}] بسته شد:`, reason);
    if (tcp && !tcp.destroyed) tcp.destroy();
    if (ws.readyState < 2) ws.close(1000);
  }

  // ── پارس هدر VLESS ──────────────────────
  function parseHeader(buf) {
    // حداقل: 1(ver) + 16(uuid) + 1(addon_len) + 1(cmd) + 2(port) + 1(addr_type)
    if (buf.length < 22) return false;

    let off = 0;

    // Version
    if (buf[off++] !== 0) { die('نسخه اشتباه'); return false; }

    // UUID - 16 بایت
    const rxUUID = buf.slice(off, off + 16);
    off += 16;
    if (!checkUUID(rxUUID)) { die('UUID نامعتبر'); return false; }

    // Addon length (رد میکنیم)
    const addonLen = buf[off++];
    off += addonLen;

    if (buf.length < off + 4) return false;

    // Command
    const cmd = buf[off++];
    if (cmd !== 0x01) { die(`دستور ${cmd} پشتیبانی نمیشه`); return false; }

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

    const { host, offset: nextOff } = target;
    const payload = buf.slice(nextOff); // داده بعد از هدر

    console.log(`[${sid}] → ${host}:${port} | payload: ${payload.length} bytes`);

    // اتصال به مقصد
    connectTarget(host, port, payload);
    return true;
  }

  // ── اتصال TCP به مقصد ───────────────────
  function connectTarget(host, port, initialPayload) {
    tcp = net.createConnection({ host, port, timeout: 10000 });

    tcp.once('connect', () => {
      console.log(`[${sid}] اتصال برقرار شد → ${host}:${port}`);

      // پاسخ VLESS: [version=0, addon_len=0]
      send(Buffer.from([0x00, 0x00]));

      // ارسال payload اولیه
      if (initialPayload.length > 0) tcp.write(initialPayload);

      // ارسال صف
      for (const chunk of queue) {
        if (!tcp.destroyed) tcp.write(chunk);
      }
      queue = [];
      ready = true;
    });

    tcp.on('data', (chunk) => {
      send(chunk);
    });

    tcp.on('error', (err) => {
      console.log(`[${sid}] خطای TCP:`, err.message);
      die('TCP error');
    });

    tcp.on('close', () => {
      console.log(`[${sid}] TCP بسته شد`);
      if (ws.readyState < 2) ws.close(1000);
    });

    tcp.on('timeout', () => {
      console.log(`[${sid}] timeout`);
      tcp.destroy();
    });
  }

  // ── پیام‌های WebSocket ───────────────────
  ws.on('message', (data, isBinary) => {
    if (!isBinary) {
      data = Buffer.from(data.toString(), 'binary');
    }
    if (!Buffer.isBuffer(data)) {
      data = Buffer.from(data);
    }

    if (!ready) {
      // جمع‌آوری هدر
      accumulator = Buffer.concat([accumulator, data]);
      const parsed = parseHeader(accumulator);
      if (!parsed && accumulator.length > 8192) {
        die('هدر خیلی بزرگ');
      }
    } else {
      // ارسال مستقیم
      if (tcp && !tcp.destroyed) {
        tcp.write(data);
      } else {
        queue.push(data);
      }
    }
  });

  ws.on('close', () => die('WebSocket بسته شد'));
  ws.on('error', (e) => die('WS error: ' + e.message));
}

// ─────────────────────────────────────────
//  HTTP Server
// ─────────────────────────────────────────
const server = http.createServer((req, res) => {
  // همه مسیرها → صفحه ساده
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('OK');
});

// ─────────────────────────────────────────
//  WebSocket Server
// ─────────────────────────────────────────
const wss = new WebSocketServer({
  server,
  path: WS_PATH,
  maxPayload: 1024 * 1024 * 4
});

wss.on('connection', (ws, req) => {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '?')
               .split(',')[0].trim();
  console.log(`[WS] اتصال جدید از ${ip}`);
  handleVLESS(ws);
});

wss.on('error', (err) => {
  console.error('[WSS] خطا:', err.message);
});

// ─────────────────────────────────────────
//  شروع
// ─────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  console.log('═══════════════════════════════════');
  console.log(`  پورت   : ${PORT}`);
  console.log(`  مسیر WS: ${WS_PATH}`);
  console.log('═══════════════════════════════════');
});

server.on('error', (err) => {
  console.error('[SERVER] خطا:', err.message);
  process.exit(1);
});

process.on('uncaughtException', (err) => {
  console.error('[CRASH]', err.message, err.stack);
});
