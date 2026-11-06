'use strict';

// Test-only preload: inject a mutable cgroup snapshot into the real guard. No
// production endpoint or environment variable can enable this fixture.
const fs = require('node:fs');
const memory = require('../../scripts/memory-guard');
const OriginalMemoryGuard = memory.MemoryGuard;
const snapshotFile = process.env.PANEL_TEST_MEMORY_SNAPSHOT;
if (!snapshotFile) throw new Error('missing test memory snapshot');

memory.MemoryGuard = class TestMemoryGuard extends OriginalMemoryGuard {
  constructor(options = {}) {
    super({
      ...options,
      readUsage: () => {
        const { usedMb, limitMb } = JSON.parse(fs.readFileSync(snapshotFile, 'utf8'));
        return {
          used: usedMb * memory.MB,
          leafUsed: usedMb * memory.MB,
          limit: limitMb * memory.MB,
          source: 'cgroup-v2'
        };
      }
    });
    this.testPumpCalls = 0;
  }

  register(fn) {
    if (process.env.PANEL_TEST_COUNT_GUARD_PUMPS !== '1' || typeof fn !== 'function') return super.register(fn);
    return super.register((guard) => { this.testPumpCalls += 1; fn(guard); });
  }

  stats() {
    const result = super.stats();
    if (process.env.PANEL_TEST_COUNT_GUARD_PUMPS === '1') result.test_pump_calls = this.testPumpCalls || 0;
    return result;
  }
};
