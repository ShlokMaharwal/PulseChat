import { randomBytes } from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';
import { parseConfig } from '../../packages/shared/src/config.js';
import {
  AuthThrottle,
  clientIp,
  originAllowed,
} from '../../packages/gateway/src/security.js';
import type { createDependencies } from '../../packages/shared/src/dependencies.js';
import type { IncomingMessage } from 'node:http';
import { clientFrameSchema } from '../../packages/shared/src/schemas.js';
const config = parseConfig({ JWT_SECRET: randomBytes(32).toString('hex') });
describe('security boundaries', () => {
  it('rejects invalid Unicode, NUL, and byte-heavy bodies while retaining ordinary text', () => {
    const frame = {
      type: 'message',
      chatId: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      clientMsgId: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
    };
    for (const body of ['\ud800', 'bad\u0000text', '😀'.repeat(5000)])
      expect(clientFrameSchema.safeParse({ ...frame, body }).success).toBe(
        false,
      );
    expect(
      clientFrameSchema.safeParse({
        ...frame,
        body: '<img src=x onerror=alert(1)> hello 🌍',
      }).success,
    ).toBe(true);
  });
  it('rejects missing, null, suffix-matched, and non-allowlisted origins', () => {
    for (const value of [
      undefined,
      'null',
      'https://evil.example',
      'http://localhost:8080.evil.example',
      'http://localhost:8080/',
    ])
      expect(originAllowed(value, config)).toBe(false);
    expect(originAllowed('http://localhost:8080', config)).toBe(true);
  });
  it('rejects default and repetitive production secrets and long access lifetimes', () => {
    for (const secret of [
      'pulsechat-local-development-secret-change-before-deploying',
      'a'.repeat(64),
    ])
      expect(() =>
        parseConfig({ NODE_ENV: 'production', JWT_SECRET: secret }),
      ).toThrow();
    expect(
      parseConfig({
        NODE_ENV: 'production',
        JWT_SECRET: randomBytes(32).toString('hex'),
      }).JWT_EXPIRES_IN_SECONDS,
    ).toBe(900);
    expect(() =>
      parseConfig({
        JWT_SECRET: config.JWT_SECRET,
        JWT_EXPIRES_IN_SECONDS: '86400',
      }),
    ).toThrow();
  });
  it('ignores forwarded IPs from peers outside the explicit proxy list', async () => {
    const request = {
      headers: { 'x-real-ip': '1.2.3.4' },
      socket: { remoteAddress: '::ffff:127.0.0.1' },
    } as unknown as IncomingMessage;
    expect(await clientIp(request, config)).toBe('127.0.0.1');
  });
  it('normalizes username keys and returns a bounded Retry-After error', async () => {
    const evalMock = vi.fn().mockResolvedValue(0);
    const redis = { eval: evalMock, del: vi.fn() } as unknown as ReturnType<
      typeof createDependencies
    >['redis'];
    const throttle = new AuthThrottle(redis, config);
    expect(await throttle.username(' Alice ')).toBe(
      await throttle.username('alice'),
    );
    evalMock.mockResolvedValue(1234);
    await expect(throttle.ip('127.0.0.1')).rejects.toMatchObject({
      status: 429,
      retryAfterMs: 1234,
    });
  });
});
