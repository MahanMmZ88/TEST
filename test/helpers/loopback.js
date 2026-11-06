'use strict';

// Test-only upstream routing: production target validation stays unchanged.
// The integration harness advertises these synthetic public-looking names in
// VLESS packets, while this preload redirects just those names to loopback.
const net = require('node:net');
const dgram = require('node:dgram');
const dns = require('node:dns').promises;
const original = net.createConnection;
const originalCreateSocket = dgram.createSocket;
const originalLookup = dns.lookup;
// Simulate dual-stack public DNS without relying on an external resolver.
dns.lookup = function testLookup(host, ...options) {
  if (/^dual-(ws|xhttp|mux)\.test$/i.test(String(host))) {
    return Promise.resolve([
      { address: '198.18.0.9', family: 4 },
      { address: '2001:4860:4860::8888', family: 6 }
    ]);
  }
  if (/^v6-only-(ws|xhttp|mux)\.test$/i.test(String(host))) {
    return Promise.resolve([{ address: '2001:4860:4860::8888', family: 6 }]);
  }
  // Distinct public-looking answers, redirected to separate UDP loopbacks
  // below, exercise address affinity within a single stateful datagram flow.
  if (/^affinity-(ws|xhttp|mux|mux-xhttp)\.test$/i.test(String(host))) {
    return Promise.resolve([
      { address: '198.18.0.9', family: 4 },
      { address: '198.18.0.10', family: 4 }
    ]);
  }
  if (String(host).toLowerCase() === 'private-dns.test') {
    return Promise.resolve([{ address: '127.0.0.1', family: 4 }, { address: '::1', family: 6 }]);
  }
  if (String(host).toLowerCase() === 'mapped-ws.test') {
    return Promise.resolve([{ address: '::ffff:198.18.0.9', family: 6 }]);
  }
  if (/^slow-udp(?:-bytes)?\.test$/i.test(String(host))) {
    return new Promise((resolve) => setTimeout(() => resolve([{ address: '198.18.0.9', family: 4 }]), 400));
  }
  if (String(host).toLowerCase() === 'never-dns.test') return new Promise(() => {});
  return originalLookup.call(this, host, ...options);
};
const names = new Set(['target.test', 'cdn.test', 'game.test']);
net.createConnection = function testConnection(options, ...rest) {
  if (options && typeof options === 'object' && options.host === 'slow-connect.test') {
    // Model a destination whose DNS/TCP setup is delayed, without ever touching
    // production target validation or depending on an external network.
    const socket = new net.Socket({ highWaterMark: options.highWaterMark });
    setTimeout(() => {
      if (!socket.destroyed) socket.connect({ host: '127.0.0.1', port: options.port, family: 4 });
    }, Number(process.env.PANEL_TEST_SLOW_CONNECT_MS || 800)).unref();
    return socket;
  }
  if (options && typeof options === 'object' && (names.has(options.host) || options.host === 'a'.repeat(255))) {
    options = { ...options, host: '127.0.0.1', family: 4, lookup: undefined };
  }
  return original.call(this, options, ...rest);
};
dgram.createSocket = function testDatagramSocket(...args) {
  const socket = originalCreateSocket.apply(this, args);
  const send = socket.send;
  socket.send = function testSend(...params) {
    if (params[2] === '198.18.0.9') params[2] = '127.0.0.1';
    if (params[2] === '198.18.0.10') params[2] = '127.0.0.2';
    if (params[2] === '2001:4860:4860::8888') params[2] = '::1';
    return send.apply(this, params);
  };
  return socket;
};
