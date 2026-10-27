'use strict';

// Test-only preload: shorten particular transport idle timers, without changing
// other timers or any production configuration.
const setTimeoutOriginal = global.setTimeout;
global.setTimeout = function testTimeout(callback, delay, ...args) {
  const source = typeof callback === 'function' ? Function.prototype.toString.call(callback) : '';
  if (process.env.PANEL_TEST_UDP_IDLE_MS && source.includes('udp idle timeout')) {
    delay = Number(process.env.PANEL_TEST_UDP_IDLE_MS);
  }
  if (process.env.PANEL_TEST_WS_IDLE_MS && callback?.name === 'handleIdleTimeout') {
    delay = Number(process.env.PANEL_TEST_WS_IDLE_MS);
  }
  if (process.env.PANEL_TEST_XHTTP_IDLE_MS && source.includes('session timeout')) {
    delay = Number(process.env.PANEL_TEST_XHTTP_IDLE_MS);
  }
  if (process.env.PANEL_TEST_CONNECT_MS && source.includes('connect timeout')) {
    delay = Number(process.env.PANEL_TEST_CONNECT_MS);
  }
  return setTimeoutOriginal(callback, delay, ...args);
};

if (process.env.PANEL_TEST_TCP_IDLE_MS || process.env.PANEL_TEST_TCP_CONNECT_MS) {
  const net = require('node:net');
  const socketTimeout = net.Socket.prototype.setTimeout;
  net.Socket.prototype.setTimeout = function testSocketTimeout(ms, ...args) {
    let timeout = ms;
    if (process.env.PANEL_TEST_TCP_IDLE_MS && [180000, 300000, 600000].includes(ms)) timeout = Number(process.env.PANEL_TEST_TCP_IDLE_MS);
    if (process.env.PANEL_TEST_TCP_CONNECT_MS && [10000, 15000, 20000].includes(ms)) timeout = Number(process.env.PANEL_TEST_TCP_CONNECT_MS);
    return socketTimeout.call(this, timeout, ...args);
  };
}
