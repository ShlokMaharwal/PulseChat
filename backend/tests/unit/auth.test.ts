import { randomUUID } from 'node:crypto';
import jwt from 'jsonwebtoken';
import { describe, expect, it } from 'vitest';
import { parseConfig } from '../../packages/shared/src/config.js';
import {
  bearerToken,
  verifyToken,
  websocketToken,
} from '../../packages/gateway/src/auth.js';

const config = parseConfig({
  JWT_SECRET: 'test-auth-secret-with-at-least-32-characters',
});
const userId = randomUUID();

describe('JWT authentication boundary', () => {
  it('accepts only the intended issuer, audience, subject, and signature', () => {
    const token = jwt.sign({}, config.JWT_SECRET, {
      algorithm: 'HS256',
      issuer: 'pulsechat',
      audience: 'pulsechat',
      subject: userId,
      expiresIn: 60,
    });
    expect(verifyToken(token, config)).toBe(userId);
  });

  it.each([
    {
      algorithm: 'HS384' as const,
      issuer: 'pulsechat',
      audience: 'pulsechat',
      subject: userId,
      expiresIn: 60,
    },
    {
      issuer: 'another-app',
      audience: 'pulsechat',
      subject: userId,
      expiresIn: 60,
    },
    {
      issuer: 'pulsechat',
      audience: 'another-app',
      subject: userId,
      expiresIn: 60,
    },
    {
      issuer: 'pulsechat',
      audience: 'pulsechat',
      subject: 'not-a-user-id',
      expiresIn: 60,
    },
    {
      issuer: 'pulsechat',
      audience: 'pulsechat',
      subject: userId,
      expiresIn: -1,
    },
  ])('rejects invalid token claims %j', (options) => {
    const token = jwt.sign({}, config.JWT_SECRET, options);
    expect(() => verifyToken(token, config)).toThrow('valid, unexpired token');
  });

  it('rejects forged signatures and malformed tokens', () => {
    const token = jwt.sign(
      {},
      'a-different-secret-with-more-than-32-characters',
      { issuer: 'pulsechat', audience: 'pulsechat', subject: userId },
    );
    expect(() => verifyToken(token, config)).toThrow();
    expect(() => verifyToken('not-a-jwt', config)).toThrow();
  });

  it('extracts bearer and WebSocket protocol tokens without query parameters', () => {
    expect(bearerToken('Bearer abc.def.ghi')).toBe('abc.def.ghi');
    expect(websocketToken('pulsechat, jwt.abc.def.ghi')).toBe('abc.def.ghi');
    expect(websocketToken(['pulsechat', 'jwt.abc.def.ghi'])).toBe(
      'abc.def.ghi',
    );
  });

  it.each([
    undefined,
    '',
    'jwt.token',
    'pulsechat',
    'pulsechat,jwt.',
    'pulsechat,jwt.one,jwt.two',
  ])('rejects missing or ambiguous WebSocket credentials %s', (protocols) => {
    expect(() => websocketToken(protocols)).toThrow();
  });

  it.each([undefined, '', 'Basic abc', 'Bearer abc def'])(
    'rejects invalid bearer headers %s',
    (header) => {
      expect(() => bearerToken(header)).toThrow();
    },
  );
});
