#!/usr/bin/env node
'use strict';

// Runtime launcher for an optional locally encrypted release. This is source
// confidentiality at rest only: the host receives the key and can inspect the
// process, decrypted temporary files, logs and network activity.
const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const zlib = require('node:zlib');

const MAGIC = Buffer.from('APPBUNDLE1');
const IV_BYTES = 12;
const TAG_BYTES = 16;
const MAX_ARCHIVE_BYTES = 8 * 1024 * 1024;
const MAX_UNPACKED_BYTES = 8 * 1024 * 1024;

function fatal(message) {
  console.error(`[release] ${message}`);
  process.exitCode = 1;
}

function decodeArchive() {
  const value = process.env.RELEASE_PACKAGE_KEY || '';
  // Don't pass the key to child processes. This is not protection from the host.
  delete process.env.RELEASE_PACKAGE_KEY;
  if (!/^[a-fA-F0-9]{64}$/.test(value)) {
    throw new Error('RELEASE_PACKAGE_KEY must be a 64-character hexadecimal key');
  }
  const key = Buffer.from(value, 'hex');
  try {
    const bundlePath = path.join(__dirname, 'release.bundle');
    const stat = fs.statSync(bundlePath);
    if (!stat.isFile() || stat.size < MAGIC.length + IV_BYTES + TAG_BYTES + 1 || stat.size > MAX_ARCHIVE_BYTES) {
      throw new Error('invalid release bundle size');
    }
    const bundle = fs.readFileSync(bundlePath);
    if (!bundle.subarray(0, MAGIC.length).equals(MAGIC)) throw new Error('invalid release bundle format');
    const ivStart = MAGIC.length;
    const tagStart = ivStart + IV_BYTES;
    const dataStart = tagStart + TAG_BYTES;
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, bundle.subarray(ivStart, tagStart));
    decipher.setAAD(MAGIC);
    decipher.setAuthTag(bundle.subarray(tagStart, dataStart));
    let compressed;
    try {
      // final() must succeed before any contents are unpacked or executed.
      compressed = Buffer.concat([decipher.update(bundle.subarray(dataStart)), decipher.final()]);
    } catch (_) {
      throw new Error('release bundle authentication failed (wrong key or damaged bundle)');
    }
    try {
      const decoded = zlib.gunzipSync(compressed, { maxOutputLength: MAX_UNPACKED_BYTES });
      try {
        return JSON.parse(decoded.toString('utf8'));
      } finally {
        decoded.fill(0);
      }
    } finally {
      compressed.fill(0);
    }
  } finally {
    key.fill(0);
  }
}

function safeParts(name) {
  if (typeof name !== 'string' || name.length === 0 || name.length > 240) throw new Error('invalid bundle path');
  const parts = name.split('/');
  if (parts.some((part) => !/^[A-Za-z0-9._-]+$/.test(part) || part === '.' || part === '..')) {
    throw new Error('invalid bundle path');
  }
  if (parts[0] === 'node_modules') throw new Error('invalid bundle path');
  return parts;
}

function unpackAndStart(archive) {
  if (!archive || archive.version !== 1 || !Array.isArray(archive.files) || archive.files.length === 0 || archive.files.length > 32) {
    throw new Error('invalid bundle manifest');
  }
  const entryParts = safeParts(archive.entry);
  const deps = path.join(__dirname, 'node_modules');
  if (!fs.existsSync(deps)) throw new Error('dependencies are missing; run npm ci before starting');

  // mkdtemp makes an owner-only directory. Decrypted code/assets are visible
  // to the hosting provider and can remain after SIGKILL or a machine crash.
  const runtimeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'app-release-'));
  const cleanup = () => {
    try { fs.rmSync(runtimeDir, { recursive: true, force: true }); } catch (_) {}
  };
  try {
    const names = new Set();
    for (const file of archive.files) {
      if (!file || typeof file.data !== 'string' || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(file.data)) {
        throw new Error('invalid bundle file');
      }
      const parts = safeParts(file.path);
      const name = parts.join('/');
      if (names.has(name)) throw new Error('duplicate bundle file');
      names.add(name);
      const target = path.join(runtimeDir, ...parts);
      fs.mkdirSync(path.dirname(target), { recursive: true, mode: 0o700 });
      fs.writeFileSync(target, Buffer.from(file.data, 'base64'), { flag: 'wx', mode: 0o600 });
    }
    if (!names.has(entryParts.join('/'))) throw new Error('bundle entry point is missing');
    fs.symlinkSync(deps, path.join(runtimeDir, 'node_modules'), process.platform === 'win32' ? 'junction' : 'dir');
    // Keep the original database search/seed behavior relative to the release
    // directory rather than the temporary source directory.
    process.env.RELEASE_APP_ROOT = __dirname;
    process.once('exit', cleanup);
    require(path.join(runtimeDir, ...entryParts));
  } catch (err) {
    cleanup();
    throw err;
  }
}

try {
  unpackAndStart(decodeArchive());
} catch (err) {
  fatal(err && err.message ? err.message : 'cannot start the release');
}
