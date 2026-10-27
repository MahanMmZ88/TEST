'use strict';

// Test-only preload: advance the server's wall clock while timers and sockets
// remain real. It is never loaded by production startup.
const fs = require('node:fs');
const offsetFile = process.env.PANEL_TEST_CLOCK_FILE;
if (!offsetFile) throw new Error('missing test clock file');
const now = Date.now;
Date.now = function testNow() {
  try { return now() + (Number(fs.readFileSync(offsetFile, 'utf8')) || 0); }
  catch (_) { return now(); }
};
