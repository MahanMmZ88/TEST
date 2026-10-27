'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { MemoryGuard, parseCgroupLocation, readCgroupMemory, MB } = require('../scripts/memory-guard');

function guardAt(mb, options = {}) {
  let used = mb * MB;
  const guard = new MemoryGuard({
    limitMb: 128,
    readUsage: () => ({ used, limit: 128 * MB, source: 'test' }),
    ...options
  });
  return { guard, set: (value) => { used = value * MB; } };
}

test('soft/hard pressure shrink buffers before the configured ceiling', () => {
  const { guard, set } = guardAt(60);
  guard.tick();
  assert.equal(guard.level, 0);
  assert.equal(guard.limitFor(256 * 1024), 256 * 1024);
  set(85);
  guard.tick();
  assert.equal(guard.level, 1);
  assert.equal(guard.limitFor(256 * 1024), 128 * 1024);
  set(105);
  guard.tick();
  assert.equal(guard.level, 2);
  assert.equal(guard.limitFor(256 * 1024), 64 * 1024);
  assert.ok(guard.isHard());
  assert.equal(guard.stats().usage_source, 'test');
});

test('pressure uses hysteresis and recovers from hard to soft to normal', () => {
  const { guard, set } = guardAt(105);
  guard.tick();
  set(96); // 75% is below hard, but still above the hard recovery threshold
  guard.tick();
  assert.equal(guard.level, 2);
  set(85); // 66%: recover from hard to soft
  guard.tick();
  assert.equal(guard.level, 1);
  set(73); // below soft but above recovery: stay soft
  guard.tick();
  assert.equal(guard.level, 1);
  set(65);
  guard.tick();
  assert.equal(guard.level, 0);
});

test('smaller detected cgroup limit wins over a configured limit', () => {
  const guard = new MemoryGuard({
    limitMb: 128,
    readUsage: () => ({ used: 75 * MB, leafUsed: 55 * MB, limit: 100 * MB, source: 'cgroup-v2' })
  });
  guard.tick();
  assert.equal(guard.limitResolvedBytes, 100 * MB);
  assert.equal(guard.usedBytes, 75 * MB);
  assert.equal(guard.limitSource, 'cgroup-v2');
  assert.equal(guard.level, 1);
});

test('more conservative configured ceiling uses the process cgroup usage', () => {
  const guard = new MemoryGuard({
    limitMb: 128,
    readUsage: () => ({ used: 1000 * MB, leafUsed: 70 * MB, limit: 4096 * MB, source: 'cgroup-v2' })
  });
  guard.tick();
  assert.equal(guard.limitResolvedBytes, 128 * MB);
  assert.equal(guard.usedBytes, 70 * MB);
  assert.equal(guard.limitSource, 'configured');
});

test('admission samples immediately and rejects only new work under hard pressure', () => {
  const { guard, set } = guardAt(60);
  let liveCalls = 0;
  guard.register(() => { liveCalls += 1; });
  assert.equal(guard.admit(), true);
  set(105);
  assert.equal(guard.admit(), false);
  assert.equal(guard.stats().rejected_new_connections, 1);
  assert.equal(guard.stats().targets, 1);
  assert.ok(liveCalls >= 2, 'existing transports stay registered');
  set(85);
  assert.equal(guard.admit(), true);
});

test('burst projection reserves headroom until fresh usage can be sampled', () => {
  let now = 1000;
  const { guard, set } = guardAt(40, { now: () => now, admissionWindowMs: 300 });
  for (let i = 0; i < 5; i++) assert.equal(guard.admit(10 * MB), true);
  assert.equal(guard.admit(10 * MB), false, 'sixth simultaneous tunnel would exceed projected hard threshold');
  assert.equal(guard.stats().rejected_new_connections, 1);
  now += 301;
  set(60);
  assert.equal(guard.admit(10 * MB), true, 'projection resets to measured use in the next window');
});

test('unknown limit never pretends the guard is active', () => {
  const guard = new MemoryGuard({ readUsage: () => ({ used: 60 * MB, limit: 0 }) });
  guard.tick();
  assert.equal(guard.stats().active, false);
  assert.equal(guard.stats().limit_mb, null);
  assert.equal(guard.level, 0);
});

test('invalid thresholds and interval fall back safely', () => {
  const { guard } = guardAt(60, { softRatio: 0.9, hardRatio: 0.4, recoverRatio: 0.8, intervalMs: NaN });
  assert.equal(guard.softRatio, 0.62);
  assert.equal(guard.hardRatio, 0.78);
  assert.equal(guard.recoverRatio, 0.55);
  assert.equal(guard.intervalMs, 200);
  const low = guardAt(60, {
    defaultRatios: { soft: 0.6, hard: 0.72, recover: 0.53 },
    softRatio: 0.9, hardRatio: 0.4
  }).guard;
  assert.equal(low.softRatio, 0.6);
  assert.equal(low.hardRatio, 0.72);
  assert.equal(low.recoverRatio, 0.53);
});

test('callbacks are removed on disconnect, and a disabled guard does not throttle', () => {
  const { guard } = guardAt(105);
  let calls = 0;
  let sheds = 0;
  const off = guard.register(() => { calls += 1; });
  guard.onHard(() => { sheds += 1; });
  guard.tick();
  assert.equal(calls, 1);
  assert.equal(sheds, 1);
  off();
  guard.tick();
  assert.equal(calls, 1);
  assert.equal(guard.stats().targets, 0);
  guard.enabled = false;
  guard.tick();
  assert.equal(calls, 1);
  assert.equal(guard.isHard(), false);
});

test('cgroup v2 membership is resolved beneath the actual mount', () => {
  const info = '30 23 0:25 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n';
  assert.deepEqual(parseCgroupLocation('0::/user/my.service\n', info), {
    version: 2, mount: '/sys/fs/cgroup', directory: '/sys/fs/cgroup/user/my.service'
  });
  const subtree = '30 23 0:25 /user /sys/fs/cgroup rw - cgroup2 cgroup rw\n';
  assert.deepEqual(parseCgroupLocation('0::/user/my.service\n', subtree), {
    version: 2, mount: '/sys/fs/cgroup', directory: '/sys/fs/cgroup/my.service'
  });
});

test('cgroup v1 memory controller and escaped mount paths are resolved', () => {
  const info = '31 23 0:26 / /sys/fs/cgroup/my\\040memory rw - cgroup cgroup rw,memory\n';
  assert.deepEqual(parseCgroupLocation('5:cpu:/x\n4:memory:/apps/panel\n', info), {
    version: 1, mount: '/sys/fs/cgroup/my memory', directory: '/sys/fs/cgroup/my memory/apps/panel'
  });
});

test('a more restrictive parent cgroup supplies both limit and usage', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'panel-cgroup-test-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const leaf = path.join(root, 'parent', 'panel');
  fs.mkdirSync(leaf, { recursive: true });
  fs.writeFileSync(path.join(root, 'memory.max'), 'max');
  fs.writeFileSync(path.join(root, 'parent', 'memory.max'), String(128 * MB));
  fs.writeFileSync(path.join(root, 'parent', 'memory.current'), String(90 * MB));
  fs.writeFileSync(path.join(leaf, 'memory.max'), 'max');
  fs.writeFileSync(path.join(leaf, 'memory.current'), String(60 * MB));
  const snap = readCgroupMemory({ version: 2, mount: root, directory: leaf });
  assert.deepEqual(snap, { used: 90 * MB, leafUsed: 60 * MB, limit: 128 * MB, source: 'cgroup-v2' });
});
