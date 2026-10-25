#!/usr/bin/env node
'use strict';

/**
 * Cross-platform dependency bootstrap (Abasthan + Deplexo + local).
 *
 * Abasthan starts every build in a clean environment and REQUIRES the build
 * command to install dependencies (https://abasthan.com/docs/deploy-nodejs).
 * Some dashboards only run `npm start` with no install step at all, which used
 * to crash with `Cannot find module 'express'` before any port was opened ->
 * the platform reported `no_live_port`.
 *
 * Deplexo is the opposite: its runtime root filesystem is READ-ONLY and the
 * image already contains node_modules. Running `npm install` there caused
 * `EROFS: read-only file system, open '/app/package-lock.json'`.
 *
 * So: install ONLY when a dependency is actually missing. On any platform that
 * already built the dependencies, this script is a strict no-op and never
 * invokes npm install.
 */

const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

const ROOT = path.resolve(__dirname, '..');
const REQUIRED = ['express', 'cookie-parser', 'qrcode', 'ws'];
const NPM = process.platform === 'win32' ? 'npm.cmd' : 'npm';

function missingDeps() {
  return REQUIRED.filter((name) => {
    try {
      require.resolve(name, { paths: [ROOT] });
      return false;
    } catch (_) {
      return true;
    }
  });
}

function run(args) {
  execFileSync(NPM, args, {
    cwd: ROOT,
    stdio: 'inherit',
    env: {
      ...process.env,
      NODE_ENV: 'production',
      npm_config_audit: 'false',
      npm_config_fund: 'false'
    }
  });
}

const missing = missingDeps();
if (missing.length === 0) {
  console.log('[deps] OK - all runtime dependencies present; skipping npm install');
  process.exit(0);
}

console.log('[deps] Missing:', missing.join(', '));
const hasLock = fs.existsSync(path.join(ROOT, 'package-lock.json'));
const flags = ['--omit=dev', '--no-audit', '--no-fund'];
let installed = false;

if (hasLock) {
  try {
    console.log('[deps] Installing with npm ci (lockfile present)...');
    run(['ci', ...flags]);
    installed = true;
  } catch (err) {
    console.error('[deps] npm ci failed; falling back to npm install:',
      err && err.message ? err.message : String(err));
  }
}

if (!installed) {
  try {
    console.log('[deps] Installing with npm install...');
    run(['install', ...flags]);
  } catch (err) {
    console.error('[deps] FATAL: could not install dependencies:',
      err && err.message ? err.message : String(err));
    process.exit(1);
  }
}

const stillMissing = missingDeps();
if (stillMissing.length) {
  console.error('[deps] FATAL: still missing after install:', stillMissing.join(', '));
  process.exit(1);
}
console.log('[deps] Dependencies ready');
