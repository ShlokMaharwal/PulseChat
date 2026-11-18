import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { TestClient } from './helpers/test-client.js';

const exec = promisify(execFile);
const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:16379');
const base = process.env.TEST_HTTP_URL ?? 'http://localhost:8080';
const clients: TestClient[] = [];
const service = 'gateway-3';
let killed = false;
try {
  const inspected = await exec('docker', ['compose', 'ps', '-q', service], {
    cwd: '..',
  });
  const container = inspected.stdout.trim();
  if (!container) throw new Error('Gateway container is missing');
  const result = await exec('docker', [
    'inspect',
    '--format',
    '{{.Config.Hostname}}',
    container,
  ]);
  const target = result.stdout.trim();
  let userId = '';
  for (let attempt = 0; attempt < 12; attempt++) {
    const response = await fetch(`${base}/api/auth/register`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        username: `ttl_${randomUUID().replaceAll('-', '').slice(0, 14)}`,
        password: 'Presence-test-password!',
      }),
    });
    if (!response.ok) throw new Error(`Register failed: ${response.status}`);
    const account = (await response.json()) as {
      token: string;
      user: { id: string };
    };
    const client = new TestClient({ reconnect: false });
    clients.push(client);
    const welcome = await client.connect(
      base.replace(/^http/, 'ws') + '/ws',
      account.token,
    );
    if (welcome.type === 'welcome' && welcome.gatewayId === target) {
      userId = account.user.id;
      break;
    }
  }
  if (!userId) throw new Error('Could not connect a test user to gateway-3');
  const until = Date.now() + 10000;
  while (!(await redis.exists(`presence:${userId}`)) && Date.now() < until)
    await delay(100);
  if (!(await redis.exists(`presence:${userId}`)))
    throw new Error('Presence did not become online');
  const started = Date.now();
  killed = true;
  await exec('docker', ['compose', 'kill', '-s', 'SIGKILL', service], {
    cwd: '..',
  });
  while (
    Date.now() - started < 40000 &&
    (await redis.exists(`presence:${userId}`))
  )
    await delay(500);
  const elapsedMs = Date.now() - started;
  if (elapsedMs > 40000)
    throw new Error(
      `Presence expiry observation exceeded 40 seconds: ${elapsedMs}ms`,
    );
  if (await redis.exists(`presence:${userId}`))
    throw new Error('Ghost online user survived the expiry deadline');
  if (await redis.exists(`user:${userId}:gateway:${target}`))
    throw new Error('Crashed gateway route survived expiry');
  if (await redis.exists(`user:${userId}:gateway`))
    throw new Error('Gateway routing hint survived expiry');
  console.log(
    JSON.stringify({
      test: 'gateway crash presence expiry',
      result: 'PASS',
      elapsedMs,
      ttlSeconds: 30,
      userId,
      gatewayId: target,
    }),
  );
} finally {
  await Promise.all(clients.map((client) => client.close()));
  if (killed)
    await exec('docker', ['compose', 'up', '-d', '--no-deps', service], {
      cwd: '..',
    });
  redis.disconnect();
}
