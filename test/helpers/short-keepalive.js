'use strict';

// Test-only preload. Shorten just the server's self-ping interval while leaving
// transport timers and all production defaults untouched.
const originalSetInterval = global.setInterval;
global.setInterval = function testInterval(callback, ms, ...args) {
  const source = typeof callback === 'function' ? Function.prototype.toString.call(callback) : '';
  if (source.includes('keepaliveEnabled') && source.includes('KEEPALIVE_USER_AGENT')) {
    ms = Number(process.env.PANEL_TEST_KEEPALIVE_TICK_MS || 150);
  }
  return originalSetInterval(callback, ms, ...args);
};
