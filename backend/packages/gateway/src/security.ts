import { createHash } from 'node:crypto';
import { lookup } from 'node:dns/promises';
import { isIP } from 'node:net';
import type { IncomingMessage } from 'node:http';
import type { Config, createDependencies } from '@pulsechat/shared';
import { HttpError } from './errors.js';

type Redis = ReturnType<typeof createDependencies>['redis'];
const digest = (value: string) =>
  createHash('sha256').update(value).digest('hex');
const LIMIT = `
local count = redis.call('INCR', KEYS[1])
if count == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[2]) end
if count > tonumber(ARGV[1]) then return redis.call('PTTL', KEYS[1]) end
return 0`;
const FAILURE = `
local failures = redis.call('HINCRBY', KEYS[1], 'failures', 1)
local clock = redis.call('TIME')
local now = tonumber(clock[1])*1000 + math.floor(tonumber(clock[2])/1000)
local delay = math.min(60000, 1000 * 2 ^ math.min(failures-1, 6))
if failures >= 5 then delay = 900000 end
redis.call('HSET', KEYS[1], 'until', now + delay)
redis.call('PEXPIRE', KEYS[1], 900000)
return delay`;
const LOCK = `
local clock = redis.call('TIME')
local now = tonumber(clock[1])*1000 + math.floor(tonumber(clock[2])/1000)
return math.max(0, (tonumber(redis.call('HGET', KEYS[1], 'until')) or 0) - now)`;

export function originAllowed(origin: string | undefined, config: Config) {
  return origin !== undefined && config.WS_ALLOWED_ORIGINS.includes(origin);
}
const proxyCache = new Map<string, { until: number; addresses: string[] }>();
export async function clientIp(request: IncomingMessage, config: Config) {
  const peer = (request.socket.remoteAddress ?? 'unknown').replace(
    /^::ffff:/,
    '',
  );
  const forwarded = request.headers['x-real-ip'];
  if (typeof forwarded !== 'string' || !isIP(forwarded)) return peer;
  for (const host of config.TRUSTED_PROXY_HOSTS) {
    let cached = proxyCache.get(host);
    if (!cached || cached.until < Date.now()) {
      try {
        cached = {
          until: Date.now() + 60000,
          addresses: (await lookup(host, { all: true })).map((v) => v.address),
        };
        proxyCache.set(host, cached);
      } catch {
        continue;
      }
    }
    if (cached.addresses.includes(peer)) return forwarded;
  }
  return peer;
}
export class AuthThrottle {
  constructor(
    private readonly redis: Redis,
    private readonly config: Config,
  ) {}
  private reject(delay: number): never {
    throw new HttpError(
      429,
      'RATE_LIMITED',
      'Authentication is temporarily limited. Try again later.',
      Math.max(1, delay),
    );
  }
  async ip(ip: string) {
    const retry = Number(
      await this.redis.eval(
        LIMIT,
        1,
        'auth:ip:' + digest(ip),
        this.config.AUTH_IP_LIMIT,
        this.config.AUTH_WINDOW_MS,
      ),
    );
    if (retry > 0) this.reject(retry);
  }
  async username(username: unknown) {
    const key =
      'auth:user:' +
      digest(
        typeof username === 'string'
          ? username.trim().toLowerCase().slice(0, 32)
          : 'invalid',
      );
    const locked = Number(await this.redis.eval(LOCK, 1, key + ':fail'));
    if (locked > 0) this.reject(locked);
    const retry = Number(
      await this.redis.eval(
        LIMIT,
        1,
        key,
        this.config.AUTH_USERNAME_LIMIT,
        this.config.AUTH_WINDOW_MS,
      ),
    );
    if (retry > 0) this.reject(retry);
    return key;
  }
  async failure(key: string) {
    await this.redis.eval(FAILURE, 1, key + ':fail');
  }
  async success(key: string) {
    await this.redis.del(key + ':fail');
  }
}
