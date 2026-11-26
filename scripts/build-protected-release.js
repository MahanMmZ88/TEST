#!/usr/bin/env node
'use strict';

// Build an optional encrypted-at-rest release locally, before a host clones it.
// The host still gets the key and decrypted application at runtime. This is not
// a way to guarantee that the application's purpose cannot be identified.
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const { spawnSync } = require('node:child_process');

const ROOT = path.resolve(__dirname, '..');
const MAGIC = Buffer.from('APPBUNDLE1');
const MAX_ARCHIVE_BYTES = 8 * 1024 * 1024;
const MAX_UNPACKED_BYTES = 8 * 1024 * 1024;
const INPUT_FILES = [
  'server.js',
  'scripts/memory-guard.js',
  'scripts/reality-bridge.js',
  'public/login.html',
  'public/dashboard.html'
];
const ORIGINAL_SEED_LINE = "const SEED_DB_PATH = path.join(__dirname, DB_FILE_NAME);";
const PACKAGED_SEED_LINE = "const SEED_DB_PATH = path.join(process.env.RELEASE_APP_ROOT || __dirname, DB_FILE_NAME);";

function isWithin(parent, child) {
  const relative = path.relative(parent, child);
  return relative === '' || (!relative.startsWith(`..${path.sep}`) && relative !== '..' && !path.isAbsolute(relative));
}

function readSourceFiles() {
  const files = [];
  for (const name of INPUT_FILES) {
    const filename = path.join(ROOT, name);
    let contents = fs.readFileSync(filename);
    if (name.endsWith('.js')) {
      const check = spawnSync(process.execPath, ['--check', filename], { encoding: 'utf8' });
      if (check.error || check.status !== 0) {
        throw new Error(`syntax check failed for ${name}: ${check.error ? check.error.message : (check.stderr || '').trim()}`);
      }
    }
    if (name === 'server.js') {
      const source = contents.toString('utf8');
      const segments = source.split(ORIGINAL_SEED_LINE);
      if (segments.length !== 2) throw new Error('database path anchor changed; review release packaging before building');
      contents = Buffer.from(segments.join(PACKAGED_SEED_LINE), 'utf8');
    }
    files.push({ path: name, data: contents.toString('base64') });
  }
  return files;
}

function loadOrCreateKey(keyFile) {
  fs.mkdirSync(path.dirname(keyFile), { recursive: true, mode: 0o700 });
  try {
    fs.writeFileSync(keyFile, `${crypto.randomBytes(32).toString('hex')}\n`, { flag: 'wx', mode: 0o600 });
  } catch (err) {
    if (err.code !== 'EEXIST') throw err;
  }
  const stat = fs.lstatSync(keyFile);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error('key path must be a regular file, not a symlink');
  if (process.platform !== 'win32' && (stat.mode & 0o077) !== 0) {
    throw new Error('key file is accessible to other users; restrict its permissions to 0600');
  }
  const value = fs.readFileSync(keyFile, 'utf8').trim();
  if (!/^[a-fA-F0-9]{64}$/.test(value)) throw new Error('key file must contain exactly 64 hexadecimal characters');
  return Buffer.from(value, 'hex');
}

function makeManifest() {
  const original = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));
  const lock = JSON.parse(fs.readFileSync(path.join(ROOT, 'package-lock.json'), 'utf8'));
  if (!original.version || !original.dependencies || !lock.packages || !lock.packages['']) {
    throw new Error('unsupported package manifest or lockfile');
  }
  for (const [name, version] of Object.entries(original.dependencies)) {
    if (!lock.packages[''].dependencies || lock.packages[''].dependencies[name] !== version) {
      throw new Error('package.json and package-lock.json do not match');
    }
  }
  const manifest = {
    name: 'service-runtime',
    version: original.version,
    private: true,
    scripts: { build: 'node --check app.cjs', start: 'node app.cjs' },
    engines: original.engines,
    dependencies: original.dependencies
  };
  lock.name = manifest.name;
  lock.packages[''].name = manifest.name;
  delete lock.packages[''].devDependencies;
  return { manifest, lock };
}

const DOCKERFILE = `FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund && npm cache clean --force
COPY app.cjs release.bundle ./
RUN mkdir -p /data && chown node:node /data
ENV NODE_ENV=production PORT=3000 DB_PATH=/data/db.json
USER node
EXPOSE 3000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \\
  CMD wget -q -O /dev/null "http://127.0.0.1:\${PORT:-3000}/health" || exit 1
STOPSIGNAL SIGTERM
CMD ["node", "app.cjs"]
`;

function buildRelease() {
  const files = readSourceFiles();
  const { manifest, lock } = makeManifest();
  const outputBase = path.resolve(process.env.RELEASE_OUTPUT_DIR || path.join(ROOT, 'release-output'));
  const keyFile = path.resolve(process.env.RELEASE_KEY_FILE || path.join(ROOT, '.release-secrets', 'package.key'));
  fs.mkdirSync(outputBase, { recursive: true, mode: 0o700 });
  fs.mkdirSync(path.dirname(keyFile), { recursive: true, mode: 0o700 });
  const realOutputBase = fs.realpathSync(outputBase);
  const realKeyPath = path.join(fs.realpathSync(path.dirname(keyFile)), path.basename(keyFile));
  if (isWithin(realOutputBase, realKeyPath)) throw new Error('key file must be outside the release output directory');

  const key = loadOrCreateKey(keyFile);
  let dir;
  try {
    const serialized = Buffer.from(JSON.stringify({ version: 1, entry: 'server.js', files }), 'utf8');
    if (serialized.length > MAX_UNPACKED_BYTES) throw new Error('source bundle is too large for the runtime launcher');
    const compressed = zlib.gzipSync(serialized, { level: 9 });
    serialized.fill(0);
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
    cipher.setAAD(MAGIC);
    const encrypted = Buffer.concat([cipher.update(compressed), cipher.final()]);
    compressed.fill(0);
    const bundle = Buffer.concat([MAGIC, iv, cipher.getAuthTag(), encrypted]);
    if (bundle.length > MAX_ARCHIVE_BYTES) throw new Error('encrypted bundle is too large for the runtime launcher');

    dir = fs.mkdtempSync(path.join(realOutputBase, 'build-'));
    fs.copyFileSync(path.join(ROOT, 'scripts/release-bootstrap.cjs'), path.join(dir, 'app.cjs'));
    fs.writeFileSync(path.join(dir, 'release.bundle'), bundle);
    fs.writeFileSync(path.join(dir, 'package.json'), `${JSON.stringify(manifest, null, 2)}\n`);
    fs.writeFileSync(path.join(dir, 'package-lock.json'), `${JSON.stringify(lock, null, 2)}\n`);
    fs.writeFileSync(path.join(dir, '.node-version'), fs.readFileSync(path.join(ROOT, '.node-version')));
    fs.writeFileSync(path.join(dir, 'Dockerfile'), DOCKERFILE);
    fs.writeFileSync(path.join(dir, '.gitignore'), 'node_modules/\ndb.json\n*.tmp\n*.log\n.env*\n');
    fs.writeFileSync(path.join(dir, '.dockerignore'), '.git\nnode_modules\ndb.json\n*.tmp\n*.log\n.env*\n');
    console.log(`[release] Output: ${dir}`);
    console.log(`[release] Key file (keep separate and private): ${keyFile}`);
    console.log('[release] The current database, environment files, tests, docs and Git history were not included.');
    console.log('[release] The host has the key and can inspect the running application; detection is not prevented.');
  } catch (err) {
    if (dir) fs.rmSync(dir, { recursive: true, force: true });
    throw err;
  } finally {
    key.fill(0);
  }
}

try {
  buildRelease();
} catch (err) {
  console.error(`[release] ${err && err.message ? err.message : 'failed to build release'}`);
  process.exitCode = 1;
}
