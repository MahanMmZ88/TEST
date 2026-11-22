'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const net = require('node:net');
const { once } = require('node:events');
const { RealityBridge, readConfig, makeLink, privateSource } = require('../scripts/reality-bridge');

const pair = crypto.generateKeyPairSync('x25519');
const privateKey = pair.privateKey.export({ format: 'jwk' }).d;
const publicKey = pair.publicKey.export({ format: 'jwk' }).x;
function env() {
  return {
    REALITY_ENABLED: '1', REALITY_PORT: '8443', REALITY_PUBLIC_PORT: '443',
    REALITY_PUBLIC_HOST: 'edge.example.com', REALITY_SNI: 'cover.example.com',
    REALITY_PRIVATE_KEY: privateKey, REALITY_SHORT_ID: '0123456789abcdef',
    REALITY_ACK_DIRECT_SOURCE_IP: '1', XRAY_BIN: '/opt/xray'
  };
}

test('REALITY config derives its public key and leaves Vision disabled', () => {
  const config = readConfig(env(), 3000);
  assert.equal(config.publicKey, publicKey);
  assert.equal(config.targetPort, 443);
  const link = makeLink(config, { uuid: '11111111111111111111111111111111', remark: 'A B' });
  const url = new URL(link);
  assert.equal(url.username, '11111111-1111-1111-1111-111111111111');
  assert.equal(url.hostname, 'edge.example.com');
  assert.equal(url.searchParams.get('pbk'), publicKey);
  assert.equal(url.searchParams.get('sid'), '0123456789abcdef');
  assert.equal(url.searchParams.get('security'), 'reality');
  assert.equal(url.searchParams.get('type'), 'tcp');
  assert.equal(url.searchParams.has('flow'), false);
  assert.equal(link.includes(privateKey), false);
  assert.equal(makeLink(config, { uuid: 'not-a-uuid' }), '');
});

test('REALITY requires explicit, valid and distinct configuration', () => {
  const checks = [
    ['REALITY_ACK_DIRECT_SOURCE_IP', '', 'source_ip_not_acknowledged'],
    ['REALITY_PORT', '3000', 'port_conflicts_with_http'],
    ['REALITY_PORT', '65536', 'invalid_listen_port'],
    ['REALITY_PUBLIC_PORT', '', 'invalid_public_port'],
    ['REALITY_PUBLIC_HOST', 'bad host:443', 'invalid_public_host'],
    ['REALITY_SNI', 'localhost', 'invalid_server_name'],
    ['REALITY_SNI', '127.0.0.1', 'invalid_server_name'],
    ['REALITY_SNI', 'bad.local', 'invalid_server_name'],
    ['REALITY_PRIVATE_KEY', 'bad', 'invalid_private_key'],
    ['REALITY_SHORT_ID', 'short', 'invalid_short_id'],
    ['XRAY_BIN', 'xray', 'xray_path_not_absolute'],
    ['REALITY_MAX_CONNECTIONS', '0', 'invalid_connection_cap'],
    ['REALITY_MAX_CONNECTIONS', '2049', 'invalid_connection_cap']
  ];
  for (const [name, value, code] of checks) {
    assert.throws(() => readConfig({ ...env(), [name]: value }, 3000), (error) => error.code === code, name);
  }
  const local = readConfig({ ...env(), REALITY_SNI: 'localhost', REALITY_ALLOW_LOCAL_TARGET: '1' }, 3000);
  assert.equal(local.serverName, 'localhost');
  const v6 = readConfig({ ...env(), REALITY_PUBLIC_HOST: '[2001:4860:4860::8888]' }, 3000);
  assert.match(makeLink(v6, { uuid: '11111111111111111111111111111111' }), /@\[2001:4860:4860::8888\]:443\?/);
});

test('non-public socket peers fail closed by default, including IPv4-mapped IPv6', () => {
  for (const ip of ['127.0.0.1', '10.0.0.2', '100.64.1.1', '172.31.0.2', '192.168.0.1',
    '198.18.0.1', '::1', '0:0:0:0:0:0:0:1', 'fe90::1', 'fd00::1', '::ffff:127.0.0.1', '::ffff:7f00:1']) {
    assert.equal(privateSource(ip), true, ip);
  }
  for (const ip of ['8.8.8.8', '1.1.1.1', '2001:4860:4860::8888']) {
    assert.equal(privateSource(ip), false, ip);
  }
  assert.equal(privateSource('not-an-ip'), true);
});

test('disabled REALITY mode does not start a process or change subscription links', async () => {
  const bridge = new RealityBridge({ env: {}, httpPort: 3000, guard: {}, memoryLimitMb: 128,
    onTunnel: () => { throw new Error('not called'); }, log: () => { throw new Error('not called'); } });
  await bridge.start();
  assert.equal(bridge.health().status, 'disabled');
  assert.equal(bridge.link({ uuid: '11111111111111111111111111111111' }), '');
  assert.equal(bridge.child, null);
  bridge.stop();
});

test('loopback bridge requires a live single-use ingress cookie and bounds its header', async (t) => {
  const accepted = [];
  const bridge = new RealityBridge({ env: {}, httpPort: 3000, guard: {}, memoryLimitMb: 1024,
    onTunnel: (socket, ip, initial) => { accepted.push({ ip, data: initial.toString() }); socket.destroy(); }, log: () => {} });
  bridge.status = 'ready';
  bridge.backend = net.createServer({ pauseOnConnect: true }, (socket) => bridge.acceptBackend(socket));
  bridge.backend.listen(0, '127.0.0.1');
  await once(bridge.backend, 'listening');
  t.after(() => { bridge.stop(); });
  const port = bridge.backend.address().port;
  const fakeFront = { destroyed: false, destroy() { this.destroyed = true; } };
  const token = '12.34.56.78:4567';
  bridge.entries.set(token, { socket: fakeFront, actualIp: '8.8.8.8', claimed: false, timer: null });

  async function send(parts) {
    const client = net.connect(port, '127.0.0.1');
    client.on('error', () => {});
    await once(client, 'connect');
    const closed = once(client, 'close');
    for (const part of parts) {
      client.write(part);
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    await Promise.race([closed, new Promise((_, reject) => setTimeout(() => reject(new Error('bridge did not close')), 2000))]);
  }
  await send([`PROXY TCP4 12.34.56.77 127.0.0.1 4567 ${port}\r\nforged`]);
  assert.equal(accepted.length, 0);
  await send(['x'.repeat(108)]);
  assert.equal(accepted.length, 0);
  await send(['PROXY TCP4 12.34.', `56.78 127.0.0.1 4567 ${port}\r\nVLESS`]);
  assert.deepEqual(accepted, [{ ip: '8.8.8.8', data: 'VLESS' }]);
  await send([`PROXY TCP4 12.34.56.78 127.0.0.1 4567 ${port}\r\nreplay`]);
  assert.equal(accepted.length, 1);
  assert.equal(bridge.metrics.rejected_bridge, 3);
});
