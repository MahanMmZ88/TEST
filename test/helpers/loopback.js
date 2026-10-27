'use strict';

// Test-only upstream routing: production target validation stays unchanged.
// The integration harness advertises these synthetic public-looking names in
// VLESS packets, while this preload redirects just those names to loopback.
const net = require('node:net');
const dgram = require('node:dgram');
const original = net.createConnection;
const originalCreateSocket = dgram.createSocket;
const names = new Set(['target.test', 'cdn.test', 'game.test']);
net.createConnection = function testConnection(options, ...rest) {
  if (options && typeof options === 'object' && names.has(options.host)) {
    options = { ...options, host: '127.0.0.1', family: 4, lookup: undefined };
  }
  return original.call(this, options, ...rest);
};
dgram.createSocket = function testDatagramSocket(...args) {
  const socket = originalCreateSocket.apply(this, args);
  const send = socket.send;
  socket.send = function testSend(...params) {
    if (params[2] === '198.18.0.9') params[2] = '127.0.0.1';
    return send.apply(this, params);
  };
  return socket;
};
