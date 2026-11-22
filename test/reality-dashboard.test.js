'use strict';

// Exercise the dashboard's actual inline script with a minimal DOM and mocked
// fetch/clipboard. These tests need no browser and do not replace a visual test.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'dashboard.html'), 'utf8');
const script = html.match(/<script>\s*([\s\S]*?)<\/script>/)?.[1];
if (!script) throw new Error('dashboard script not found');
const flush = () => new Promise((resolve) => setImmediate(resolve));
const deferred = () => {
  let resolve;
  const promise = new Promise((done) => { resolve = done; });
  return { promise, resolve };
};
const response = (data, ok = true) => ({ ok, json: async () => data });

function dashboard(fetchImpl) {
  const elements = new Map();
  function element(id) {
    if (!elements.has(id)) {
      const classes = new Set();
      elements.set(id, {
        id, style: {}, hidden: id === 'qrTabReality' || id === 'qrRealityInfo',
        textContent: '', value: '', src: '',
        classList: {
          add: (name) => classes.add(name), remove: (name) => classes.delete(name),
          contains: (name) => classes.has(name),
          toggle: (name, force) => { if (force === undefined ? !classes.has(name) : force) classes.add(name); else classes.delete(name); }
        },
        getContext: () => null, appendChild: () => {}
      });
    }
    return elements.get(id);
  }
  const copied = [];
  const document = {
    getElementById: element, addEventListener: () => {}, querySelector: () => null,
    createElement: () => ({ style: {}, classList: { add() {}, remove() {} } }),
    body: { appendChild() {}, removeChild() {} }
  };
  const window = { location: { hostname: 'panel.example', host: 'panel.example', protocol: 'https:', port: '' },
    addEventListener: () => {}, innerWidth: 1200 };
  const navigator = { clipboard: { writeText: async (value) => { copied.push(value); } } };
  const context = vm.createContext({ document, window, navigator, console, fetch: fetchImpl,
    setTimeout: () => 0, clearTimeout: () => {}, URL, Blob });
  vm.runInContext(script, context, { filename: 'dashboard-inline.js' });
  return { context, element, copied };
}

test('REALITY QR uses the server-provided link and preserves the existing tabs', async () => {
  assert.match(html, /id="qrTabReality"[^>]*hidden/);
  const realityLink = 'vless://11111111-1111-1111-1111-111111111111@tcp.example:443?security=reality&type=tcp';
  const calls = [];
  const ui = dashboard(async (url, options) => {
    calls.push({ url, options });
    if (url === '/api/users/alice/config/reality') return response({ link: realityLink });
    return response({}, false); // initial stats/users are irrelevant to this test
  });
  ui.context.currentUsers = [{ id: 'alice', remark: 'Alice', uuid: 'uuid-alice', sub_id: 'sub-alice' }];
  ui.context.openQrModal('alice');
  assert.equal(ui.element('qrTabReality').hidden, true);
  await flush();
  assert.equal(ui.element('qrTabReality').hidden, false);
  assert.equal(calls.find((call) => call.url.endsWith('/config/reality')).options.cache, 'no-store');
  ui.context.setQrTab('reality');
  assert.equal(ui.element('qrCopyBtn').textContent, 'Copy REALITY');
  assert.equal(ui.element('qrRealityInfo').hidden, false);
  assert.match(ui.element('qrMainImg').src, /^\/api\/users\/alice\/qr\/reality\?ts=/);
  ui.context.copyQrLink('reality');
  await flush();
  assert.deepEqual(ui.copied, [realityLink]);
  ui.context.setQrTab('ws');
  assert.equal(ui.element('qrCopyBtn').textContent, 'Copy WebSocket');
  assert.equal(ui.element('qrRealityInfo').hidden, true);
  ui.context.closeQrModal();
  assert.equal(ui.element('qrTabReality').hidden, true);
});

test('disabled or failed REALITY does not add a tab or disrupt WS/XHTTP', async () => {
  const ui = dashboard(async (url) => {
    if (url === '/api/health') return response({ transports: { reality: { status: 'failed', reason: 'xray_exited' } } });
    return response({}, false);
  });
  ui.context.currentUsers = [{ id: 'alice', remark: 'Alice', uuid: 'uuid-alice', sub_id: 'sub-alice' }];
  ui.context.openQrModal('alice');
  await flush();
  assert.equal(ui.element('qrTabReality').hidden, true);
  ui.context.setQrTab('ws');
  assert.equal(ui.element('qrCopyBtn').textContent, 'Copy WebSocket');
  ui.context.setQrTab('xhttp');
  assert.equal(ui.element('qrCopyBtn').textContent, 'Copy XHTTP');
  await ui.context.refreshRealityStatus();
  assert.equal(ui.element('realityStatus').textContent, 'failed (xray_exited)');
});

test('late REALITY config for another user or a closed modal is discarded', async () => {
  const alice = deferred(), bob = deferred(), secondBob = deferred();
  let bobRequests = 0;
  const ui = dashboard((url) => {
    if (url === '/api/users/alice/config/reality') return alice.promise;
    if (url === '/api/users/bob/config/reality') return ++bobRequests === 1 ? bob.promise : secondBob.promise;
    return Promise.resolve(response({}, false));
  });
  ui.context.currentUsers = [{ id: 'alice', remark: 'Alice' }, { id: 'bob', remark: 'Bob' }];
  ui.context.openQrModal('alice');
  ui.context.openQrModal('bob');
  alice.resolve(response({ link: 'vless://alice' }));
  await flush();
  assert.equal(ui.element('qrTabReality').hidden, true);
  bob.resolve(response({ link: 'vless://bob' }));
  await flush();
  assert.equal(ui.element('qrTabReality').hidden, false);
  ui.context.copyQrLink('reality');
  await flush();
  assert.deepEqual(ui.copied, ['vless://bob']);
  ui.context.openQrModal('bob');
  ui.context.closeQrModal();
  secondBob.resolve(response({ link: 'vless://stale' }));
  await flush();
  assert.equal(ui.element('qrTabReality').hidden, true);
});
