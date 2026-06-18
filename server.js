'use strict';

const http = require('http');
const net = require('net');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT    = parseInt(process.env.PORT) || 3000;
const WS_PATH = process.env.WS_PATH        || '/tunnel';
const RAW_IDS = process.env.USER_IDS       || process.env.UUID || '';

function loadUUIDs() {
  if (!RAW_IDS.trim()) {
    const id = crypto.randomUUID();
    console.log('[BOOT] UUID موقت:', id);
    return new Set([id]);
  }
  const list = RAW_IDS.split(',').map(s => s.trim()).filter(Boolean);
  console.log('[BOOT] UUID بارگذاری شد:', list.length, 'عدد');
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

// ─── پارس آدرس ───────────────────────────────────────
function parseTarget(buf, off) {
  const type = buf[off++];
  let host;

  if (type === 0x01) {
    // IPv4
    host = `${buf[off]}.${buf[off+1]}.${buf[off+2]}.${buf[off+3]}`;
    off += 4;
  } else if (type === 0x02) {
    // Domain
    const len = buf[off++];
    host = buf.slice(off, off + len).toString('utf8');
    off += len;
  } else if (type === 0x03) {
    // IPv6
    const p = [];
    for (let i = 0; i < 8; i++) p.push(buf.readUInt16BE(off + i * 2).toString(16));
    host = '[' + p.join(':') + ']';
    off += 16;
  } else {
    throw new Error('نوع آدرس ناشناخته: 0x' + type.toString(16));
  }

  const port = buf.readUInt16BE(off);
  off += 2;
  return { host, port, off };
}

// ─── هندل هر اتصال ───────────────────────────────────
function handleVLESS(ws, clientIP) {
  const sid  = crypto.randomBytes(3).toString('hex').toUpperCase();
  let ready  = false;
  let tcp    = null;
  let queue  = [];
  let accum  = Buffer.alloc(0);

  // ──────────────────────────────────────────
  //  ارسال به کلاینت
  // ──────────────────────────────────────────
  function sendWS(data) {
    if (ws.readyState === 1) {
      ws.send(data, { binary: true }, (err) => {
        if (err) console.log(`[${sid}] sendWS error:`, err.message);
      });
    }
  }

  // ──────────────────────────────────────────
  //  بستن همه چیز
  // ──────────────────────────────────────────
  function die(reason) {
    console.log(`[${sid}] ✗ ${reason}`);
    if (tcp && !tcp.destroyed) tcp.destroy();
    try { if (ws.readyState < 2) ws.close(1000); } catch (_) {}
  }

  // ──────────────────────────────────────────
  //  پارس هدر VLESS
  // ──────────────────────────────────────────
  function tryParseHeader(buf) {
    console.log(`[${sid}] هدر دریافت شد - اندازه: ${buf.length} bytes`);
    console.log(`[${sid}] هدر hex: ${buf.slice(0, Math.min(buf.length, 64)).toString('hex')}`);

    // حداقل اندازه معتبر
    // 1(ver) + 16(uuid) + 1(addonLen) + [addons] + 1(cmd) + 2(port) + 1(addrType) + ...
    if (buf.length < 22) {
      console.log(`[${sid}] هدر هنوز کامل نیست (${buf.length} < 22)`);
      return false; // صبر کن بیشتر بیاد
    }

    let off = 0;

    // ── Version ──
    const ver = buf[off++];
    console.log(`[${sid}] version: ${ver}`);
    if (ver !== 0) {
      die(`نسخه اشتباه: ${ver}`);
      return false;
    }

    // ── UUID ──
    const rxUUID = buf.slice(off, off + 16);
    off += 16;
    console.log(`[${sid}] UUID hex: ${rxUUID.toString('hex')}`);

    if (!checkUUID(rxUUID)) {
      console.log(`[${sid}] UUID های معتبر:`);
      for (const u of VALID_UUIDS) {
        console.log(`  - ${u} → ${uuidToBytes(u).toString('hex')}`);
      }
      die('UUID نامعتبر');
      return false;
    }
    console.log(`[${sid}] ✓ UUID معتبر`);

    // ── Addon Length ──
    const addonLen = buf[off++];
    console.log(`[${sid}] addonLen: ${addonLen}`);

    // بررسی کامل بودن بافر برای addons
    if (buf.length < off + addonLen + 1 + 2 + 1) {
      console.log(`[${sid}] هدر هنوز کامل نیست (بعد از addon)`);
      return false;
    }

    // addons رو رد کن
    if (addonLen > 0) {
      console.log(`[${sid}] addons: ${buf.slice(off, off + addonLen).toString('hex')}`);
    }
    off += addonLen;

    // ── Command ──
    const cmd = buf[off++];
    console.log(`[${sid}] cmd: ${cmd} (${cmd === 1 ? 'TCP' : cmd === 2 ? 'UDP' : 'unknown'})`);
    if (cmd !== 0x01 && cmd !== 0x02) {
      die(`cmd پشتیبانی نمیشه: ${cmd}`);
      return false;
    }

    // ── Port ──
    const port = buf.readUInt16BE(off);
    off += 2;
    console.log(`[${sid}] port: ${port}`);

    // ── Address ──
    let target;
    try {
      target = parseTarget(buf, off);
    } catch (e) {
      die('آدرس نامعتبر: ' + e.message);
      return false;
    }

    const { host, port: addrPort, off: nextOff } = target;
    console.log(`[${sid}] host: ${host} | addrPort: ${addrPort}`);
    off = nextOff;

    // payload باقیمانده
    const payload = buf.slice(off);
    console.log(`[${sid}] payload باقیمانده: ${payload.length} bytes`);

    // اتصال به مقصد
    connect(host, port, payload);
    return true;
  }

  // ──────────────────────────────────────────
  //  اتصال TCP به مقصد
  // ──────────────────────────────────────────
  function connect(host, port, initPayload) {
    console.log(`[${sid}] در حال اتصال به ${host}:${port}...`);

    tcp = net.createConnection({ host, port });

    // timeout
    tcp.setTimeout(15000);

    tcp.once('connect', () => {
      console.log(`[${sid}] ✓ اتصال TCP برقرار → ${host}:${port}`);

      // ─── پاسخ VLESS Response Header ───
      // دقیقاً 2 بایت: [version=0x00, addonLen=0x00]
      const resp = Buffer.allocUnsafe(2);
      resp[0] = 0x00; // version
      resp[1] = 0x00; // addon length = 0
      sendWS(resp);
      console.log(`[${sid}] ✓ VLESS response فرستاده شد: ${resp.toString('hex')}`);

      // payload اولیه
      if (initPayload && initPayload.length > 0) {
        tcp.write(initPayload);
        console.log(`[${sid}] payload اولیه فرستاده شد: ${initPayload.length} bytes`);
      }

      // صف
      for (const chunk of queue) {
        if (!tcp.destroyed) tcp.write(chunk);
      }
      queue = [];
      ready = true;
    });

    tcp.on('data', (chunk) => {
      console.log(`[${sid}] ← TCP data: ${chunk.length} bytes`);
      sendWS(chunk);
    });

    tcp.on('timeout', () => {
      console.log(`[${sid}] TCP timeout`);
      tcp.destroy();
    });

    tcp.on('error', (err) => {
      console.log(`[${sid}] TCP error: ${err.code} - ${err.message}`);
      die('TCP error: ' + err.message);
    });

    tcp.on('close', (hadError) => {
      console.log(`[${sid}] TCP closed (hadError: ${hadError})`);
      try { if (ws.readyState < 2) ws.close(1000); } catch (_) {}
    });
  }

  // ──────────────────────────────────────────
  //  پیام‌های WebSocket
  // ──────────────────────────────────────────
  ws.on('message', (data, isBinary) => {
    // تبدیل به Buffer
    if (!Buffer.isBuffer(data)) {
      data = Buffer.isBuffer(data) ? data : Buffer.from(data);
    }

    console.log(`[${sid}] WS message: ${data.length} bytes | isBinary: ${isBinary} | ready: ${ready}`);

    if (!ready) {
      accum = Buffer.concat([accum, data]);
      const ok = tryParseHeader(accum);
      if (!ok && accum.length > 16 * 1024) {
        die('هدر خیلی بزرگ');
      }
    } else {
      if (tcp && !tcp.destroyed) {
        tcp.write(data);
        console.log(`[${sid}] → TCP: ${data.length} bytes`);
      } else {
        queue.push(data);
      }
    }
  });

  ws.on('close', (code, reason) => {
    console.log(`[${sid}] WS closed | code: ${code} | reason: ${reason.toString()}`);
    if (tcp && !tcp.destroyed) tcp.destroy();
  });

  ws.on('error', (err) => {
    console.log(`[${sid}] WS error: ${err.message}`);
    if (tcp && !tcp.destroyed) tcp.destroy();
  });

  // ping/pong
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
}

// ─── HTTP Server ──────────────────────────────────────
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('OK');
});

// ─── WebSocket Server ─────────────────────────────────
const wss = new WebSocketServer({
  server,
  path: WS_PATH,
  maxPayload: 4 * 1024 * 1024
});

// ping interval
const pingTimer = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

wss.on('connection', (ws, req) => {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '?')
               .split(',')[0].trim();
  console.log(`\n[WS] ← اتصال جدید از ${ip} | path: ${req.url}`);
  handleVLESS(ws, ip);
});

wss.on('close', () => clearInterval(pingTimer));

wss.on('error', (err) => console.error('[WSS] error:', err.message));

// ─── شروع ─────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  console.log('══════════════════════════════════');
  console.log(`  پورت    : ${PORT}`);
  console.log(`  مسیر WS : ${WS_PATH}`);
  console.log('══════════════════════════════════');
});

server.on('error', (err) => {
  console.error('[SERVER] error:', err.message);
  process.exit(1);
});

process.on('uncaughtException', (err) => {
  console.error('[CRASH]', err.message);
  console.error(err.stack);
});

process.on('unhandledRejection', (r) => {
  console.error('[REJECT]', r);
});
