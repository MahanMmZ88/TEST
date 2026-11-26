'use strict';

// The release protects an uploaded bundle without its key; it cannot protect
// source or runtime behavior from a host that runs the application.
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const { spawn, spawnSync } = require('node:child_process');
const { once } = require('node:events');
const fs = require('node:fs');
const http = require('node:http');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const zlib = require('node:zlib');

const ROOT = path.resolve(__dirname, '..');
const BUILDER = path.join(ROOT, 'scripts/build-protected-release.js');
const MAGIC = Buffer.from('APPBUNDLE1');
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
let temp, outputBase, keyFile, key, output;

function build(extra = {}) {
  return spawnSync(process.execPath, [BUILDER], {
    cwd: ROOT, encoding: 'utf8', timeout: 20000,
    env: { ...process.env, RELEASE_OUTPUT_DIR: outputBase, RELEASE_KEY_FILE: keyFile, ...extra }
  });
}

function attachDependencies(dir) {
  fs.symlinkSync(path.join(ROOT, 'node_modules'), path.join(dir, 'node_modules'), process.platform === 'win32' ? 'junction' : 'dir');
}

function decode(bundle) {
  assert.ok(bundle.subarray(0, MAGIC.length).equals(MAGIC));
  const start = MAGIC.length;
  const decipher = crypto.createDecipheriv('aes-256-gcm', Buffer.from(key, 'hex'), bundle.subarray(start, start + 12));
  decipher.setAAD(MAGIC);
  decipher.setAuthTag(bundle.subarray(start + 12, start + 28));
  const compressed = Buffer.concat([decipher.update(bundle.subarray(start + 28)), decipher.final()]);
  return JSON.parse(zlib.gunzipSync(compressed).toString('utf8'));
}

function makeBundle(archive) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', Buffer.from(key, 'hex'), iv);
  cipher.setAAD(MAGIC);
  const content = zlib.gzipSync(Buffer.from(JSON.stringify(archive)));
  const encrypted = Buffer.concat([cipher.update(content), cipher.final()]);
  return Buffer.concat([MAGIC, iv, cipher.getAuthTag(), encrypted]);
}

function launchFailure(dir, packageKey) {
  const env = { ...process.env, RELEASE_PACKAGE_KEY: packageKey, NODE_OPTIONS: '' };
  if (packageKey === undefined) delete env.RELEASE_PACKAGE_KEY;
  return spawnSync(process.execPath, [path.join(dir, 'app.cjs')], { cwd: dir, env, encoding: 'utf8', timeout: 5000 });
}

async function freePort() {
  const server = net.createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

function request(port, method, url, json, headers = {}) {
  const body = json === undefined ? undefined : Buffer.from(JSON.stringify(json));
  if (body) headers = { ...headers, 'content-type': 'application/json', 'content-length': body.length };
  return new Promise((resolve, reject) => {
    const req = http.request({ hostname: '127.0.0.1', port, method, path: url, headers }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.setTimeout(4000, () => req.destroy(new Error(`timeout requesting ${url}`)));
    req.on('error', reject);
    req.end(body);
  });
}

async function launch(dbPath, tempDir, releaseDir = output) {
  const port = await freePort();
  const child = spawn(process.execPath, [path.join(releaseDir, 'app.cjs')], {
    cwd: releaseDir,
    env: { ...process.env, RELEASE_PACKAGE_KEY: key, PORT: String(port), DB_PATH: dbPath,
      TMPDIR: tempDir, TEMP: tempDir, TMP: tempDir, NODE_OPTIONS: '', REALITY_ENABLED: '0',
      MEMORY_GUARD: '0', KEEPALIVE_ENABLED: '0', PUBLIC_HEALTH_MINIMAL: '0' },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let logs = '';
  child.stdout.on('data', (chunk) => { logs = (logs + chunk).slice(-20000); });
  child.stderr.on('data', (chunk) => { logs = (logs + chunk).slice(-20000); });
  try {
    for (let i = 0; i < 150; i++) {
      if (child.exitCode !== null || child.signalCode !== null) throw new Error(`server exited: ${logs}`);
      try {
        const health = await request(port, 'GET', '/api/health');
        if (health.status === 200) return { child, port, logs: () => logs };
      } catch (_) {}
      await wait(40);
    }
    throw new Error(`server did not start: ${logs}`);
  } catch (err) {
    await stop(child);
    throw err;
  }
}

async function stop(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, 'exit').catch(() => {});
  child.kill('SIGTERM');
  await Promise.race([exited, wait(4500)]);
  if (child.exitCode === null && child.signalCode === null) {
    child.kill('SIGKILL');
    await exited;
  }
}

async function login(port) {
  const res = await request(port, 'POST', '/api/login', { username: 'admin', password: 'admin' });
  assert.equal(res.status, 200);
  return res.headers['set-cookie'][0].split(';')[0];
}

before(() => {
  temp = fs.mkdtempSync(path.join(os.tmpdir(), 'package-test-'));
  outputBase = path.join(temp, 'output');
  keyFile = path.join(temp, 'secrets', 'package.key');
  const result = build();
  assert.equal(result.status, 0, result.stderr);
  const dirs = fs.readdirSync(outputBase);
  assert.equal(dirs.length, 1);
  output = path.join(outputBase, dirs[0]);
  key = fs.readFileSync(keyFile, 'utf8').trim();
  assert.match(key, /^[a-f0-9]{64}$/);
  attachDependencies(output);
});

after(() => { if (temp) fs.rmSync(temp, { recursive: true, force: true }); });

test('export contains an authenticated encrypted payload, not source, database, env files, or the key', () => {
  const names = fs.readdirSync(output).filter((name) => name !== 'node_modules').sort();
  assert.deepEqual(names, ['.dockerignore', '.gitignore', '.node-version', 'Dockerfile', 'app.cjs', 'package-lock.json', 'package.json', 'release.bundle'].sort());
  assert.ok(path.relative(outputBase, keyFile).startsWith(`..${path.sep}`));
  if (process.platform !== 'win32') assert.equal(fs.statSync(keyFile).mode & 0o777, 0o600);
  const bundle = fs.readFileSync(path.join(output, 'release.bundle'));
  for (const marker of ['Mahan Panel', 'VLESS', 'XHTTP', 'REALITY', key]) assert.equal(bundle.includes(Buffer.from(marker)), false);
  for (const name of names) assert.equal(fs.readFileSync(path.join(output, name)).includes(Buffer.from(key)), false);
  const archive = decode(bundle);
  assert.deepEqual(archive.files.map((file) => file.path), [
    'server.js', 'scripts/memory-guard.js', 'scripts/reality-bridge.js', 'public/login.html', 'public/dashboard.html'
  ]);
  for (const file of archive.files) {
    let expected = fs.readFileSync(path.join(ROOT, file.path), 'utf8');
    if (file.path === 'server.js') {
      expected = expected.replace('const SEED_DB_PATH = path.join(__dirname, DB_FILE_NAME);',
        'const SEED_DB_PATH = path.join(process.env.RELEASE_APP_ROOT || __dirname, DB_FILE_NAME);');
    }
    assert.equal(Buffer.from(file.data, 'base64').toString('utf8'), expected);
  }
  const manifest = JSON.parse(fs.readFileSync(path.join(output, 'package.json'), 'utf8'));
  const lock = JSON.parse(fs.readFileSync(path.join(output, 'package-lock.json'), 'utf8'));
  assert.equal(manifest.name, lock.name);
  assert.equal(manifest.name, lock.packages[''].name);
  assert.deepEqual(manifest.dependencies, lock.packages[''].dependencies);
});

test('subsequent exports reuse the key but use a fresh random GCM nonce', () => {
  const first = fs.readFileSync(path.join(output, 'release.bundle'));
  const result = build();
  assert.equal(result.status, 0, result.stderr);
  assert.equal(fs.readFileSync(keyFile, 'utf8').trim(), key);
  const dirs = fs.readdirSync(outputBase);
  assert.equal(dirs.length, 2);
  const other = dirs.map((name) => path.join(outputBase, name)).find((dir) => dir !== output);
  const second = fs.readFileSync(path.join(other, 'release.bundle'));
  assert.notDeepEqual(first.subarray(MAGIC.length, MAGIC.length + 12), second.subarray(MAGIC.length, MAGIC.length + 12));
  assert.deepEqual(decode(first), decode(second));
});

test('missing/wrong key and tampered ciphertext fail before starting, without printing the key', () => {
  const missing = launchFailure(output, undefined);
  assert.equal(missing.status, 1);
  assert.match(missing.stderr, /RELEASE_PACKAGE_KEY/);
  const wrongKey = '0'.repeat(64) === key ? '1'.repeat(64) : '0'.repeat(64);
  const wrong = launchFailure(output, wrongKey);
  assert.equal(wrong.status, 1);
  assert.match(wrong.stderr, /authentication failed/);
  assert.equal((wrong.stderr + wrong.stdout).includes(wrongKey), false);
  const tampered = path.join(temp, 'tampered');
  fs.mkdirSync(tampered);
  fs.copyFileSync(path.join(output, 'app.cjs'), path.join(tampered, 'app.cjs'));
  const bundle = fs.readFileSync(path.join(output, 'release.bundle'));
  bundle[bundle.length - 1] ^= 1;
  fs.writeFileSync(path.join(tampered, 'release.bundle'), bundle);
  const changed = launchFailure(tampered, key);
  assert.equal(changed.status, 1);
  assert.match(changed.stderr, /authentication failed/);
  assert.equal((changed.stderr + changed.stdout).includes(key), false);
});

test('rejects paths outside the temporary directory even with an authenticated bundle', () => {
  const bad = path.join(temp, 'bad-path');
  fs.mkdirSync(bad);
  fs.copyFileSync(path.join(output, 'app.cjs'), path.join(bad, 'app.cjs'));
  attachDependencies(bad);
  fs.writeFileSync(path.join(bad, 'release.bundle'), makeBundle({ version: 1, entry: 'main.js', files: [
    { path: '../escape.js', data: Buffer.from('bad').toString('base64') },
    { path: 'main.js', data: Buffer.from('bad').toString('base64') }
  ] }));
  const result = launchFailure(bad, key);
  assert.equal(result.status, 1);
  assert.match(result.stderr, /invalid bundle path/);
  assert.equal(fs.existsSync(path.join(temp, 'escape.js')), false);
});

test('will not place a key within the upload directory', () => {
  const badOut = path.join(temp, 'unsafe-output');
  const unsafeKey = path.join(badOut, 'secret', 'key');
  const result = build({ RELEASE_OUTPUT_DIR: badOut, RELEASE_KEY_FILE: unsafeKey });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /outside the release output directory/);
  assert.equal(fs.existsSync(unsafeKey), false);
});

test('rejects a symlinked key or an existing key readable by other users', { skip: process.platform === 'win32' }, () => {
  const secretDir = path.join(temp, 'insecure-key');
  fs.mkdirSync(secretDir);
  const looseKey = path.join(secretDir, 'loose');
  fs.writeFileSync(looseKey, `${key}\n`, { mode: 0o644 });
  fs.chmodSync(looseKey, 0o644);
  const loose = build({ RELEASE_OUTPUT_DIR: path.join(temp, 'loose-output'), RELEASE_KEY_FILE: looseKey });
  assert.equal(loose.status, 1);
  assert.match(loose.stderr, /permissions to 0600/);
  const link = path.join(secretDir, 'link');
  fs.symlinkSync(keyFile, link);
  const symlinked = build({ RELEASE_OUTPUT_DIR: path.join(temp, 'link-output'), RELEASE_KEY_FILE: link });
  assert.equal(symlinked.status, 1);
  assert.match(symlinked.stderr, /not a symlink/);
});

test('packaged app serves assets, saves data on shutdown, restarts, and cleans temporary files', { timeout: 30000 }, async (t) => {
  const runtimeTemp = path.join(temp, 'runtime-tmp');
  const stateDir = path.join(temp, 'state');
  fs.mkdirSync(runtimeTemp);
  fs.mkdirSync(stateDir);
  const dbPath = path.join(stateDir, 'db.json');
  const children = [];
  t.after(async () => { for (const child of children) await stop(child); });

  const first = await launch(dbPath, runtimeTemp);
  children.push(first.child);
  const runtimeDirs = fs.readdirSync(runtimeTemp).filter((name) => name.startsWith('app-release-'));
  assert.equal(runtimeDirs.length, 1);
  if (process.platform !== 'win32') assert.equal(fs.statSync(path.join(runtimeTemp, runtimeDirs[0])).mode & 0o777, 0o700);
  const health = JSON.parse((await request(first.port, 'GET', '/api/health')).body);
  assert.equal(health.db_path, dbPath);
  const loginPage = await request(first.port, 'GET', '/');
  assert.equal(loginPage.status, 200);
  assert.deepEqual(loginPage.body, fs.readFileSync(path.join(ROOT, 'public/login.html')));
  const cookie = await login(first.port);
  const dashboard = await request(first.port, 'GET', '/panel', undefined, { cookie });
  assert.equal(dashboard.status, 200);
  assert.deepEqual(dashboard.body, fs.readFileSync(path.join(ROOT, 'public/dashboard.html')));
  const add = await request(first.port, 'POST', '/api/users', { remark: 'saved-fixture' }, { cookie });
  assert.equal(add.status, 200);
  assert.equal(first.logs().includes(key), false);
  await stop(first.child);
  assert.deepEqual(fs.readdirSync(runtimeTemp), []);
  assert.equal(JSON.parse(fs.readFileSync(dbPath, 'utf8')).users[0].remark, 'saved-fixture');

  const second = await launch(dbPath, runtimeTemp);
  children.push(second.child);
  const cookie2 = await login(second.port);
  const users = await request(second.port, 'GET', '/api/users', undefined, { cookie: cookie2 });
  assert.equal(users.status, 200);
  assert.match(users.body.toString(), /saved-fixture/);
  await stop(second.child);
  assert.deepEqual(fs.readdirSync(runtimeTemp), []);

  // With no explicit DB_PATH, the normal writable release directory is used,
  // not the disposable extraction directory.
  const third = await launch('', runtimeTemp);
  children.push(third.child);
  const defaultHealth = JSON.parse((await request(third.port, 'GET', '/api/health')).body);
  assert.equal(defaultHealth.db_path, path.join(output, 'db.json'));
  await stop(third.child);
  assert.deepEqual(fs.readdirSync(runtimeTemp), []);
});

test('packaged app starts with a read-only release directory and a separate writable data path', {
  timeout: 15000,
  skip: process.platform === 'win32' || (typeof process.getuid === 'function' && process.getuid() === 0)
}, async (t) => {
  const readonly = path.join(temp, 'readonly');
  const runtimeTemp = path.join(temp, 'readonly-tmp');
  const stateDir = path.join(temp, 'readonly-state');
  fs.mkdirSync(readonly);
  fs.mkdirSync(runtimeTemp);
  fs.mkdirSync(stateDir);
  for (const name of ['app.cjs', 'release.bundle']) fs.copyFileSync(path.join(output, name), path.join(readonly, name));
  attachDependencies(readonly);
  fs.chmodSync(readonly, 0o555);
  t.after(() => fs.chmodSync(readonly, 0o700));
  const dbPath = path.join(stateDir, 'db.json');
  const app = await launch(dbPath, runtimeTemp, readonly);
  t.after(() => stop(app.child));
  const health = JSON.parse((await request(app.port, 'GET', '/api/health')).body);
  assert.equal(health.db_path, dbPath);
  assert.equal((await request(app.port, 'GET', '/login')).status, 200);
  await stop(app.child);
  assert.equal(fs.existsSync(dbPath), true);
  assert.deepEqual(fs.readdirSync(runtimeTemp), []);
});
