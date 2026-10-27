'use strict';

// Samples cgroup usage when it is available and falls back to process RSS.
// RSS is only an estimate: it excludes other processes and kernel memory.
const fs = require('fs');
const path = require('path');
const MB = 1024 * 1024;

function readNumber(file) {
  try {
    const value = fs.readFileSync(file, 'utf8').trim();
    if (!value || value === 'max') return 0;
    const n = Number(value);
    // cgroup v1 uses an enormous sentinel for an unlimited group.
    return Number.isFinite(n) && n > 0 && n < Number.MAX_SAFE_INTEGER ? n : 0;
  } catch (_) {
    return 0;
  }
}

function unescapeMountPath(value) {
  return value.replace(/\\([0-7]{3})/g, (_, octal) => String.fromCharCode(parseInt(octal, 8)));
}

// The process need not be in the root cgroup. Resolve its membership relative
// to the mounted hierarchy (works both inside a cgroup namespace and on hosts
// where /proc/self/cgroup is e.g. 0::/user/my-service).
function parseCgroupLocation(membership, mountinfo) {
  const groups = String(membership).trim().split('\n').map((line) => {
    const match = line.match(/^\d+:([^:]*):(.*)$/);
    return match ? { controllers: match[1].split(',').filter(Boolean), group: match[2] || '/' } : null;
  }).filter(Boolean);
  const mounts = String(mountinfo).trim().split('\n').map((line) => {
    const parts = line.split(' - ');
    if (parts.length !== 2) return null;
    const left = parts[0].split(' ');
    const right = parts[1].split(' ');
    if (left.length < 6 || right.length < 3) return null;
    return {
      root: unescapeMountPath(left[3]), mount: unescapeMountPath(left[4]),
      type: right[0], options: `${left[5]},${right.slice(2).join(',')}`
    };
  }).filter(Boolean);

  for (const version of [2, 1]) {
    for (const mount of mounts) {
      if (version === 2 && mount.type !== 'cgroup2') continue;
      if (version === 1 && (mount.type !== 'cgroup' || !mount.options.split(',').includes('memory'))) continue;
      const group = groups.find((g) => version === 2 ? g.controllers.length === 0 : g.controllers.includes('memory'));
      if (!group) continue;
      const root = path.posix.resolve('/', mount.root);
      const membershipPath = path.posix.resolve('/', group.group);
      // A mount may start at a subtree, or the namespace may already make the
      // membership path relative to that subtree.
      const relative = root !== '/' && (membershipPath === root || membershipPath.startsWith(`${root}/`))
        ? membershipPath.slice(root.length) : membershipPath;
      const mountPath = path.resolve(mount.mount);
      const directory = path.resolve(mountPath, `.${relative || '/'}`);
      if (directory !== mountPath && !directory.startsWith(`${mountPath}${path.sep}`)) continue;
      return { version, mount: mountPath, directory };
    }
  }
  return null;
}

function detectCgroupMemory() {
  try {
    const membership = fs.readFileSync('/proc/self/cgroup', 'utf8');
    const mounts = fs.readFileSync('/proc/self/mountinfo', 'utf8');
    const location = parseCgroupLocation(membership, mounts);
    if (location) {
      const usage = location.version === 2 ? 'memory.current' : 'memory.usage_in_bytes';
      if (fs.existsSync(path.join(location.directory, usage))) return location;
    }
    // Hybrid hosts can expose cgroup v2 without the memory controller, while
    // memory itself is still mounted on v1.
    const v1Mounts = mounts.split('\n').filter((line) => line.includes(' - cgroup ')).join('\n');
    const v1 = parseCgroupLocation(membership, v1Mounts);
    if (v1 && fs.existsSync(path.join(v1.directory, 'memory.usage_in_bytes'))) return v1;
  } catch (_) {}
  return null;
}

function readCgroupMemory(location) {
  if (!location) return { used: 0, limit: 0, source: 'unknown' };
  const currentFile = location.version === 2 ? 'memory.current' : 'memory.usage_in_bytes';
  const limitFile = location.version === 2 ? 'memory.max' : 'memory.limit_in_bytes';
  const source = `cgroup-v${location.version}`;
  let directory = location.directory;
  let leafUsed = 0;
  let limitingUsed = 0;
  let limit = 0;
  while (directory === location.mount || directory.startsWith(`${location.mount}${path.sep}`)) {
    const used = readNumber(path.join(directory, currentFile));
    const currentLimit = readNumber(path.join(directory, limitFile));
    if (!leafUsed && used) leafUsed = used;
    if (currentLimit && (!limit || currentLimit < limit)) {
      limit = currentLimit;
      limitingUsed = used;
    }
    if (directory === location.mount) break;
    directory = path.dirname(directory);
  }
  return { used: limitingUsed || leafUsed, leafUsed, limit, source };
}

function ratioOption(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 && n < 1 ? n : fallback;
}

class MemoryGuard {
  constructor(options = {}) {
    this.enabled = options.enabled !== false;
    const configuredMb = Number(options.limitMb);
    this.limitBytes = Number.isFinite(configuredMb) && configuredMb > 0 ? configuredMb * MB : 0;
    const defaults = options.defaultRatios || {};
    let fallbackSoft = ratioOption(defaults.soft, 0.62);
    let fallbackHard = ratioOption(defaults.hard, 0.78);
    let fallbackRecover = ratioOption(defaults.recover, 0.55);
    if (!(fallbackRecover < fallbackSoft && fallbackSoft < fallbackHard)) {
      fallbackSoft = 0.62; fallbackHard = 0.78; fallbackRecover = 0.55;
    }
    this.softRatio = ratioOption(options.softRatio, fallbackSoft);
    this.hardRatio = ratioOption(options.hardRatio, fallbackHard);
    this.recoverRatio = ratioOption(options.recoverRatio, fallbackRecover);
    if (!(this.recoverRatio < this.softRatio && this.softRatio < this.hardRatio)) {
      this.softRatio = fallbackSoft;
      this.hardRatio = fallbackHard;
      this.recoverRatio = fallbackRecover;
    }
    const interval = Number(options.intervalMs);
    this.intervalMs = Number.isFinite(interval) && interval > 0 ? Math.max(50, Math.min(5000, interval)) : 200;
    this.cgroup = options.cgroup === undefined ? detectCgroupMemory() : options.cgroup;
    this.readUsage = typeof options.readUsage === 'function' ? options.readUsage : null;
    this.onLevelChange = typeof options.onLevelChange === 'function' ? options.onLevelChange : null;
    this.now = typeof options.now === 'function' ? options.now : Date.now;
    const burstWindow = Number(options.admissionWindowMs);
    this.admissionWindowMs = Number.isFinite(burstWindow) && burstWindow > 0 ? Math.max(50, Math.min(5000, burstWindow)) : 300;
    this.projectedBytes = 0;
    this.projectionExpiresAt = 0;
    this.level = 0;
    this.ratio = 0;
    this.usedBytes = 0;
    this.peakUsedBytes = 0;
    this.limitResolvedBytes = 0;
    this.detectedLimitBytes = 0;
    this.usageSource = 'unknown';
    this.limitSource = 'unknown';
    this.ticks = 0;
    this.throttleEvents = 0;
    this.admissionRejections = 0;
    this.targets = new Set();
    this.shedHooks = new Set();
    this.timer = null;
  }

  sample() {
    const snap = this.readUsage ? (this.readUsage() || {}) : readCgroupMemory(this.cgroup);
    let detectedLimit = Number(snap.limit) || 0;
    let detectedSource = detectedLimit ? (snap.source || 'injected') : 'unknown';
    if (!this.readUsage && typeof process.constrainedMemory === 'function') {
      try {
        const value = process.constrainedMemory();
        if (Number.isFinite(value) && value > 0 && value < Number.MAX_SAFE_INTEGER &&
            (!detectedLimit || value < detectedLimit)) {
          detectedLimit = value;
          detectedSource = 'node';
        }
      } catch (_) {}
    }
    this.detectedLimitBytes = detectedLimit;
    // A configured ceiling may be more conservative, but must never override a
    // smaller actual cgroup limit.
    const useConfigured = this.limitBytes && (!detectedLimit || this.limitBytes < detectedLimit);
    const limit = useConfigured ? this.limitBytes : detectedLimit;
    this.limitSource = useConfigured ? 'configured' : detectedSource;
    let used = Number(useConfigured && snap.leafUsed ? snap.leafUsed : snap.used) || 0;
    this.usageSource = used ? (snap.source || 'injected') : 'rss';
    if (!used) used = typeof process.memoryUsage.rss === 'function' ? process.memoryUsage.rss() : process.memoryUsage().rss;
    this.usedBytes = used;
    this.peakUsedBytes = Math.max(this.peakUsedBytes, used);
    this.limitResolvedBytes = limit || 0;
    this.ratio = limit ? used / limit : 0;
    return this.ratio;
  }

  hasLimit() { return this.limitResolvedBytes > 0; }
  isHard() { return this.enabled && this.level >= 2; }
  scale() { return !this.enabled ? 1 : this.level === 2 ? 0.25 : this.level === 1 ? 0.5 : 1; }

  limitFor(base, minBytes = 32 * 1024) {
    const b = Number(base) > 0 ? Number(base) : minBytes;
    return Math.max(Number(minBytes) || 0, Math.floor(b * this.scale()));
  }

  register(fn) {
    if (typeof fn !== 'function') return () => {};
    this.targets.add(fn);
    return () => this.targets.delete(fn);
  }

  onHard(fn) {
    if (typeof fn !== 'function') return () => {};
    this.shedHooks.add(fn);
    return () => this.shedHooks.delete(fn);
  }

  // No finite memory limit can sustain an unbounded number of new streams.
  // Sample on each admission and keep a short-lived, conservative projection
  // for connection bursts whose kernel buffers have not yet been charged to the
  // cgroup. Existing connections are never closed by this admission check.
  admit(estimatedBytes = 0, countRejection = true) {
    if (!this.enabled) return true;
    this.tick();
    if (!this.hasLimit()) return true;
    const now = this.now();
    if (now >= this.projectionExpiresAt) {
      this.projectedBytes = this.usedBytes;
      this.projectionExpiresAt = now + this.admissionWindowMs;
    }
    const projection = Math.max(this.projectedBytes, this.usedBytes) + Math.max(0, Number(estimatedBytes) || 0);
    if (this.isHard() || projection > this.hardRatio * this.limitResolvedBytes) {
      if (countRejection) this.admissionRejections += 1;
      return false;
    }
    this.projectedBytes = projection;
    return true;
  }

  recordAdmissionRejection() { this.admissionRejections += 1; }

  updateLevel() {
    const previous = this.level;
    if (!this.hasLimit()) this.level = 0;
    else if (this.ratio >= this.hardRatio) this.level = 2;
    else if (previous === 2) {
      // Separate recovery thresholds keep hard mode from oscillating, without
      // waiting for RSS to return all the way to its cold-start level.
      const hardRecover = Math.max(this.softRatio, this.hardRatio - 0.06);
      if (this.ratio <= hardRecover) this.level = 1;
    } else if (this.ratio >= this.softRatio) this.level = 1;
    else if (this.ratio <= this.recoverRatio) this.level = 0;
    if (this.level !== previous) {
      if (this.level > previous) this.throttleEvents += 1;
      if (this.onLevelChange) this.onLevelChange(this.level, previous, this.stats());
    }
    return this.level;
  }

  tick() {
    if (!this.enabled) return;
    this.ticks += 1;
    this.sample();
    this.updateLevel();
    if (this.isHard()) {
      for (const fn of this.shedHooks) {
        try { fn(this); } catch (_) {}
      }
    }
    for (const fn of this.targets) {
      try { fn(this); } catch (_) {}
    }
  }

  start() {
    if (!this.enabled || this.timer) return;
    this.tick();
    this.timer = setInterval(() => this.tick(), this.intervalMs);
    this.timer.unref?.();
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  stats() {
    return {
      enabled: this.enabled,
      active: this.enabled && this.hasLimit(),
      state: this.level === 2 ? 'hard' : this.level === 1 ? 'soft' : 'normal',
      ratio: Number(this.ratio.toFixed(3)),
      used_mb: Math.round(this.usedBytes / MB),
      peak_used_mb: Math.round(this.peakUsedBytes / MB),
      limit_mb: this.limitResolvedBytes ? Math.round(this.limitResolvedBytes / MB) : null,
      usage_source: this.usageSource,
      limit_source: this.limitSource,
      soft_ratio: this.softRatio,
      hard_ratio: this.hardRatio,
      recover_ratio: this.recoverRatio,
      interval_ms: this.intervalMs,
      targets: this.targets.size,
      throttle_events: this.throttleEvents,
      rejected_new_connections: this.admissionRejections
    };
  }
}

module.exports = { MemoryGuard, detectCgroupMemory, parseCgroupLocation, readCgroupMemory, MB };
