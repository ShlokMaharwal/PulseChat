import type { createDependencies } from './dependencies.js';
import type { ServerFrame } from './schemas.js';
import { timestamp, type PipelineStage } from './tracing.js';

type RedisClient = ReturnType<typeof createDependencies>['redis'];

export const RATE_LIMIT_LUA = `
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local cost = math.min(tonumber(ARGV[3]), capacity)
local previous = redis.call('HMGET', KEYS[1], 'tokens', 'updated')
local tokens = tonumber(previous[1]) or capacity
local updated = tonumber(previous[2]) or now
tokens = math.min(capacity, tokens + math.max(0, now - updated) * refill / 1000)
local accepted = 0
local retry = 0
if tokens >= cost then tokens = tokens - cost; accepted = 1
else retry = math.ceil((cost - tokens) * 1000 / refill) end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity * 2000 / refill))
return { accepted, retry }
`;

export async function consumeToken(
  redis: RedisClient,
  userId: string,
  capacity: number,
  refill: number,
  cost = 1,
) {
  const result = (await redis.eval(
    RATE_LIMIT_LUA,
    1,
    `rate:${userId}`,
    capacity,
    refill,
    cost,
  )) as [number, number];
  return { allowed: result[0] === 1, retryAfterMs: Math.max(1, result[1]) };
}

const REFRESH_ROUTE_LUA = `
local wasOnline = redis.call('EXISTS', KEYS[1])
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[3])
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
redis.call('SET', KEYS[3], ARGV[2], 'EX', ARGV[3])
redis.call('SADD', KEYS[4], ARGV[2])
redis.call('EXPIRE', KEYS[4], tonumber(ARGV[3]) * 2)
return wasOnline
`;

const REMOVE_ROUTE_LUA = `
redis.call('DEL', KEYS[3])
redis.call('SREM', KEYS[4], ARGV[1])
if redis.call('GET', KEYS[2]) == ARGV[1] then redis.call('DEL', KEYS[2]) end
local gateways = redis.call('SMEMBERS', KEYS[4])
for _, gateway in ipairs(gateways) do
  if redis.call('EXISTS', ARGV[2] .. gateway) == 1 then return 0 end
  redis.call('SREM', KEYS[4], gateway)
end
redis.call('DEL', KEYS[1], KEYS[4])
return 1
`;

const TARGET_ROUTES_LUA = `
local candidates = redis.call('SMEMBERS', KEYS[2])
local hint = redis.call('GET', KEYS[1])
if hint then table.insert(candidates, hint) end
local seen = {}
local result = {}
for _, gateway in ipairs(candidates) do
  if not seen[gateway] then
    seen[gateway] = true
    if redis.call('EXISTS', ARGV[1] .. gateway) == 1 then
      table.insert(result, gateway)
    else
      redis.call('SREM', KEYS[2], gateway)
    end
  end
end
return result
`;

export class UserRouter {
  constructor(
    private readonly redis: RedisClient,
    private readonly gatewayId: string,
    private readonly ttl: number,
    private readonly observe?: (
      stage: PipelineStage,
      start: number,
      end?: number,
    ) => void,
  ) {}

  async refresh(userId: string): Promise<boolean> {
    const wasOnline = await this.redis.eval(
      REFRESH_ROUTE_LUA,
      4,
      `presence:${userId}`,
      `user:${userId}:gateway`,
      `user:${userId}:gateway:${this.gatewayId}`,
      `user:${userId}:gateways`,
      new Date().toISOString(),
      this.gatewayId,
      this.ttl,
    );
    return Number(wasOnline) === 0;
  }

  async remove(userId: string): Promise<boolean> {
    const offline = await this.redis.eval(
      REMOVE_ROUTE_LUA,
      4,
      `presence:${userId}`,
      `user:${userId}:gateway`,
      `user:${userId}:gateway:${this.gatewayId}`,
      `user:${userId}:gateways`,
      this.gatewayId,
      `user:${userId}:gateway:`,
    );
    return Number(offline) === 1;
  }

  async targets(userId: string): Promise<string[]> {
    return (await this.redis.eval(
      TARGET_ROUTES_LUA,
      2,
      `user:${userId}:gateway`,
      `user:${userId}:gateways`,
      `user:${userId}:gateway:`,
    )) as string[];
  }

  async publish(userId: string, payload: ServerFrame) {
    const start = timestamp();
    const gateways = await this.targets(userId);
    if (payload.type === 'message') this.observe?.('route_lookup', start);
    const publishing = timestamp();
    if (payload.type === 'message' && payload.trace)
      payload = {
        ...payload,
        trace: { ...payload.trace, redisPublishStartedAt: publishing },
      };
    const envelope = JSON.stringify({ userId, payload });
    await Promise.all(
      gateways.map((id) => this.redis.publish(`gateway:${id}`, envelope)),
    );
    if (payload.type === 'message' && gateways.length)
      this.observe?.('redis_publish', publishing);
  }

  async online(userIds: string[]): Promise<Map<string, boolean>> {
    if (!userIds.length) return new Map();
    const values = await this.redis.mget(
      ...userIds.map((id) => `presence:${id}`),
    );
    return new Map(userIds.map((id, index) => [id, Boolean(values[index])]));
  }
}
