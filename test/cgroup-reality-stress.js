'use strict';

// Optional privileged, local-only end-to-end stress. The panel AND its Xray
// child run in a cold, zero-swap cgroup; clients, target and cover stay outside.
// Requires Linux cgroup v2, passwordless sudo, openssl and the pinned XRAY_BIN.
// Never run against a production cgroup. REALITY_TEST_CGROUP_MIB defaults to 512.
const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const ROOT = path.join(__dirname, '..');
const sourceXray = process.env.XRAY_BIN || '/usr/local/bin/xray';
const requested = Number(process.env.REALITY_TEST_CGROUP_MIB || 512);
if (!Number.isInteger(requested) || requested < 256 || requested > 2048) throw new Error('REALITY_TEST_CGROUP_MIB must be 256..2048');
const group = `/sys/fs/cgroup/arena-reality-${process.pid}`;
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'panel-reality-cgroup-'));
let created = false;
let failure = null;

function sudo(...args) { return execFileSync('sudo', ['-n', ...args], { encoding: 'utf8' }); }
function readStats(file) {
  return Object.fromEntries(fs.readFileSync(path.join(group, file), 'utf8').trim().split('\n')
    .map((line) => line.split(/\s+/)).map(([key, value]) => [key, Number(value)]));
}

try {
  sudo('mkdir', group);
  created = true;
  sudo('sh', '-c', 'printf "%s" "$2" > "$1/memory.max"; printf 0 > "$1/memory.swap.max"; printf "50000 100000" > "$1/cpu.max"',
    'sh', group, String(requested * 1024 * 1024));
  const node = path.join(temp, 'node-cold');
  const xray = path.join(temp, 'xray-cold');
  fs.copyFileSync(process.execPath, node);
  fs.copyFileSync(sourceXray, xray);
  fs.chmodSync(node, 0o755);
  fs.chmodSync(xray, 0o755);
  try {
    const output = execFileSync(process.execPath, [path.join(__dirname, 'xray-reality-integration.js')], {
      cwd: ROOT, env: { ...process.env, XRAY_BIN: xray, REALITY_TEST_CGROUP: group, REALITY_TEST_NODE: node },
      encoding: 'utf8', timeout: 180000, maxBuffer: 4 * 1024 * 1024
    });
    process.stdout.write(output);
  } catch (err) {
    if (err.stdout) process.stdout.write(err.stdout);
    if (err.stderr) process.stderr.write(err.stderr);
    failure = err;
  }
  const events = readStats('memory.events');
  const peak = Number(fs.readFileSync(path.join(group, 'memory.peak'), 'utf8').trim());
  console.log(JSON.stringify({ scenario: 'cold cgroup result', limit_mib: requested, swap_mib: 0, cpu: 0.5,
    peak_mib: Math.round(peak / 1024 / 1024 * 10) / 10, oom: events.oom, oom_kill: events.oom_kill }));
  try { assert.equal(events.oom_kill, 0, 'cgroup killed a process for OOM'); }
  catch (err) { failure ||= err; }
} catch (err) {
  failure ||= err;
} finally {
  if (created) {
    try { sudo('sh', '-c', 'if test -w "$1/cgroup.kill"; then echo 1 > "$1/cgroup.kill"; else for p in $(cat "$1/cgroup.procs"); do kill -9 "$p" 2>/dev/null || true; done; fi', 'sh', group); } catch (_) {}
    try { sudo('rmdir', group); } catch (_) {}
  }
  fs.rmSync(temp, { recursive: true, force: true });
}
if (failure) {
  console.error('REALITY CGROUP TEST FAILED:', failure.message || failure);
  process.exitCode = 1;
}
