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
  }
};
