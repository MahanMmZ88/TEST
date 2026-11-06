'use strict';

// Test-only: emulate a scheduler/CPU/GC pause on demand. The production server
// never installs this signal handler.
process.on('SIGUSR2', () => {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 250);
});
