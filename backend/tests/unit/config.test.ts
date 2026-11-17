import { describe, expect, it } from 'vitest';
import { parseConfig } from '../../packages/shared/src/config.js';

const secret = 'a-test-secret-with-at-least-32-characters';

describe('startup configuration', () => {
  it('parses defaults and a comma-separated broker list', () => {
    const config = parseConfig({
      JWT_SECRET: secret,
      KAFKA_BROKERS: 'kafka:9092, other:9092',
      PORT: '9000',
    });
    expect(config.KAFKA_BROKERS).toEqual(['kafka:9092', 'other:9092']);
    expect(config.PORT).toBe(9000);
    expect(config.PRESENCE_TTL_SECONDS).toBe(30);
    expect(config.REDIS_URL).toBe('redis://localhost:16379');
    expect(config.MONGO_URL).toBe('mongodb://localhost:27018');
  });

  it.each([
    {},
    { JWT_SECRET: 'short' },
    { JWT_SECRET: secret, PORT: '0' },
    { JWT_SECRET: secret, PORT: '65536' },
    { JWT_SECRET: secret, PORT: 'abc' },
    { JWT_SECRET: secret, KAFKA_BROKERS: 'kafka:9092,' },
    { JWT_SECRET: secret, REDIS_URL: 'http://redis:6379' },
    { JWT_SECRET: secret, MONGO_URL: 'http://mongo:27017' },
    { JWT_SECRET: secret, KAFKA_PARTITIONS: '1' },
    {
      JWT_SECRET: secret,
      HEARTBEAT_INTERVAL_MS: '30000',
      PRESENCE_TTL_SECONDS: '30',
    },
  ])('fails fast on unsafe configuration %j', (env) => {
    expect(() => parseConfig(env)).toThrow();
  });
});
