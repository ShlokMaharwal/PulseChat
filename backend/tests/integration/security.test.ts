import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import WebSocket from 'ws';
import { afterAll, beforeAll, expect, it } from 'vitest';
import {
  createDependencies,
  createLogger,
  parseConfig,
} from '@pulsechat/shared';
import { AuthThrottle } from '../../packages/gateway/src/security.js';
const config = parseConfig(process.env);
const deps = createDependencies(
  config,
  'security-integration',
  createLogger(config, 'security-integration'),
);
const base = process.env.TEST_GATEWAY_A ?? 'http://gateway-1:8080';
const post = (path: string, body: unknown) =>
  fetch(base + path, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(60000),
  });
beforeAll(() => deps.connect());
afterAll(() => deps.close());
it('shares IP and canonical username limits, advances failure backoff, and enforces lockout in Redis', async () => {
  const throttle = new AuthThrottle(deps.redis, {
    ...config,
    AUTH_IP_LIMIT: 2,
    AUTH_USERNAME_LIMIT: 2,
  });
  const id = randomUUID();
  await throttle.ip(id);
  await throttle.ip(id);
  await expect(throttle.ip(id)).rejects.toMatchObject({ status: 429 });
  await throttle.username(id);
  await throttle.username(id.toUpperCase());
  await expect(throttle.username(id)).rejects.toMatchObject({ status: 429 });
  const failureKey = await throttle.username(randomUUID());
  for (let i = 0; i < 5; i++) await throttle.failure(failureKey);
  expect(
    Number(await deps.redis.hget(failureKey + ':fail', 'until')) - Date.now(),
  ).toBeGreaterThan(890000);
  await throttle.success(failureKey);
  expect(await deps.redis.exists(failureKey + ':fail')).toBe(0);
});
it('throttles failed HTTP authentication and rotates, detects replay, and revokes refresh families', async () => {
  const username = 'secure_' + randomUUID().replaceAll('-', '').slice(0, 12),
    password = 'Secure-password-123!';
  const register = await post('/auth/register', { username, password });
  expect(register.status).toBe(201);
  const account = (await register.json()) as {
    token: string;
    refreshToken: string;
    expiresIn: number;
  };
  expect(account.expiresIn).toBeLessThanOrEqual(900);
  const failed = await post('/auth/login', {
    username,
    password: 'wrong-password',
  });
  expect(failed.status).toBe(401);
  const limited = await post('/auth/login', {
    username: username.toUpperCase(),
    password,
  });
  expect(limited.status).toBe(429);
  expect(limited.headers.get('retry-after')).toBeTruthy();
  await delay(1100);
  expect((await post('/auth/login', { username, password })).status).toBe(200);
  const first = await post('/auth/refresh', {
    refreshToken: account.refreshToken,
  });
  expect(first.status).toBe(200);
  const rotated = (await first.json()) as { refreshToken: string };
  expect(rotated.refreshToken).not.toBe(account.refreshToken);
  expect(
    (await post('/auth/refresh', { refreshToken: account.refreshToken }))
      .status,
  ).toBe(401);
  expect(
    (await post('/auth/refresh', { refreshToken: rotated.refreshToken }))
      .status,
  ).toBe(401);
  const login = (await (
    await post('/auth/login', { username, password })
  ).json()) as { refreshToken: string };
  expect(
    (await post('/auth/logout', { refreshToken: login.refreshToken })).status,
  ).toBe(200);
  expect(
    (await post('/auth/refresh', { refreshToken: login.refreshToken })).status,
  ).toBe(401);
});
it('rejects a valid authenticated browser upgrade from an unapproved or missing origin', async () => {
  const response = await post('/auth/register', {
    username: 'origin_' + randomUUID().replaceAll('-', '').slice(0, 12),
    password: 'Secure-password-123!',
  });
  expect(response.status).toBe(201);
  const account = (await response.json()) as { token: string };
  for (const origin of ['https://evil.example', undefined]) {
    const socket = new WebSocket(
      base.replace(/^http/, 'ws') + '/ws',
      ['pulsechat', 'jwt.' + account.token],
      origin ? { origin } : {},
    );
    const status = await new Promise<number>((resolve, reject) => {
      socket.on('unexpected-response', (_, r) => {
        r.resume();
        resolve(r.statusCode ?? 0);
        socket.terminate();
      });
      socket.on('error', () => undefined);
      socket.on('open', () => reject(new Error('Disallowed origin upgraded')));
      setTimeout(() => {
        socket.terminate();
        reject(new Error('Origin rejection timed out'));
      }, 10000).unref();
    });
    expect(status).toBe(403);
  }
});
