'use strict';

const http = require('http');
const net = require('net');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

// Railway automatically provides PORT. No custom environment variable is required.
const PORT = Number(process.env.PORT || 3000);

// Hardcoded configuration
const WS_PATH = '/ws';
const UUID_TEXT = '11100c13-d74b-425a-b689-f0b0737c62b2';
const REQUIRE_HOST_HEADER = '';
const CONNECT_TIMEOUT_MS = 15000;
const IDLE_TIMEOUT_MS = 120000;
const MAX_FIRST_PACKET = 64 * 1024;

const USER_ID = uuidToBytes(UUID_TEXT);
if (!USER_ID) {
  console.error('Invalid hardcoded UUID.');
  process.exit(1);
}

function normalizePath(path) {
  if (!path || path === '/') return '/ws';
  return path.startsWith('/') ? path : '/' + path;
}

function uuidToBytes(uuid) {
  const hex = String(uuid).replace(/-/g, '').toLowerCase();
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
  // Safety guard: avoid obvious local/private abuse from the hosted app.
  // Remove only if you fully understand the risk.
  const h = String(host).toLowerCase();
  if (port <= 0 || port > 65535) return true;
  if (h === 'localhost' || h.endsWith('.localhost')) return true;
  if (h === '0.0.0.0' || h === '::' || h === '::1') return true;
  if (/^127\./.test(h)) return true;
  if (/^10\./.test(h)) return true;
  if (/^192\.168\./.test(h)) return true;
  if (/^172\.(1[6-9]|2\d|3[0-1])\./.test(h)) return true;
  if (/^169\.254\./.test(h)) return true;
  if (h === '100.64.0.0') return true;
  return false;
}

function parseVlessRequest(buffer) {
  if (!Buffer.isBuffer(buffer)) buffer = Buffer.from(buffer);
  if (buffer.length < 24) throw new Error('request too short');
  if (buffer.length > MAX_FIRST_PACKET) throw new Error('first packet too large');

  let offset = 0;
  const version = buffer[offset++];
  if (!buffer.subarray(offset, offset + 16).equals(USER_ID)) throw new Error('invalid uuid');
  offset += 16;

  const optLen = buffer[offset++];
  if (buffer.length < offset + optLen + 4) throw new Error('invalid options length');
  offset += optLen;

  const command = buffer[offset++];
  if (command !== 0x01) throw new Error('only TCP command is supported');

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
    throw new Error('unsupported address type');
  }

  if (isForbiddenTarget(host, port)) throw new Error('forbidden target');

  return {
    version,
    host,
    port,
    payload: buffer.subarray(offset)
  };
}

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' });
    res.end('ok\n');
    return;
  }

  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('not found\n');
});

const wss = new WebSocketServer({ noServer: true, perMessageDeflate: false, maxPayload: 16 * 1024 * 1024 });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url || '/', 'http://127.0.0.1');

  if (url.pathname !== WS_PATH) {
    socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }

  if (REQUIRE_HOST_HEADER && req.headers.host !== REQUIRE_HOST_HEADER) {
    socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req);
  });
});

wss.on('connection', (ws, req) => {
  ws.binaryType = 'nodebuffer';

  let remote = null;
  let established = false;
  let closed = false;
  let firstPacketHandled = false;
  const pendingClientChunks = [];

  const idleTimer = setTimeout(() => closeAll(1000, 'idle timeout'), IDLE_TIMEOUT_MS);
  const connectTimer = setTimeout(() => closeAll(1013, 'connect timeout'), CONNECT_TIMEOUT_MS);

  function refreshIdleTimer() {
    idleTimer.refresh?.();
  }

  function closeAll(code = 1000, reason = '') {
    if (closed) return;
    closed = true;
    clearTimeout(idleTimer);
    clearTimeout(connectTimer);
    destroySocket(remote);
    safeCloseWebSocket(ws, code, reason);
  }

  function sendToWs(chunk) {
    if (closed || ws.readyState !== ws.OPEN) return false;

    // Simple backpressure guard for WebSocket buffer.
    if (ws.bufferedAmount > 8 * 1024 * 1024) {
      remote?.pause();
      const resumeInterval = setInterval(() => {
        if (closed || ws.readyState !== ws.OPEN) {
          clearInterval(resumeInterval);
          return;
        }
        if (ws.bufferedAmount < 2 * 1024 * 1024) {
          clearInterval(resumeInterval);
          remote?.resume();
        }
      }, 25);
    }

    ws.send(chunk, { binary: true }, (err) => {
      if (err) closeAll(1011, 'websocket send failed');
    });
    return true;
  }

  ws.on('message', (data, isBinary) => {
    refreshIdleTimer();

    if (!isBinary && typeof data === 'string') {
      closeAll(1003, 'binary frames required');
      return;
    }

    const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);

    if (!firstPacketHandled) {
      firstPacketHandled = true;
      let request;
      try {
        request = parseVlessRequest(chunk);
      } catch (err) {
        closeAll(1008, 'bad request');
        return;
      }

      remote = net.createConnection({ host: request.host, port: request.port, allowHalfOpen: false });
      remote.setNoDelay(true);
      remote.setKeepAlive(true, 30000);

      remote.once('connect', () => {
        if (closed) return;
        clearTimeout(connectTimer);
        established = true;

        // VLESS response header: version + zero-length addon.
        sendToWs(Buffer.from([request.version, 0x00]));

        if (request.payload.length) remote.write(request.payload);
        while (pendingClientChunks.length) {
          const queued = pendingClientChunks.shift();
          if (!remote.write(queued)) break;
        }
      });

      remote.on('data', (remoteChunk) => {
        refreshIdleTimer();
        sendToWs(remoteChunk);
      });

      remote.on('drain', () => {
        // Resume reading from the WebSocket TCP socket after remote backpressure clears.
        try { req.socket.resume(); } catch (_) {}

        while (pendingClientChunks.length && remote && !remote.destroyed) {
          if (!remote.write(pendingClientChunks.shift())) break;
        }
      });

      remote.on('error', () => closeAll(1011, 'remote error'));
      remote.on('timeout', () => closeAll(1000, 'remote timeout'));
      remote.on('close', () => closeAll(1000, 'remote closed'));
      remote.setTimeout(IDLE_TIMEOUT_MS);
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

    if (!remote.write(chunk)) {
      // Pause WebSocket TCP socket when remote backpressure occurs.
      req.socket.pause();
    }
  });

  ws.on('close', () => closeAll(1000, 'websocket closed'));
  ws.on('error', () => closeAll(1011, 'websocket error'));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`VLESS over WebSocket server listening on 0.0.0.0:${PORT}`);
  console.log(`WebSocket path: ${WS_PATH}`);
});
