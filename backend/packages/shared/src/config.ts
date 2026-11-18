import { hostname } from 'node:os';
import { z } from 'zod';

const positiveInteger = z.coerce.number().int().positive();
const envSchema = z.object({
  NODE_ENV: z
    .enum(['development', 'test', 'production'])
    .default('development'),
  PORT: positiveInteger.max(65535).default(8080),
  INSTANCE_ID: z.string().min(1).default(hostname()),
  LOG_LEVEL: z
    .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'])
    .default('info'),
  KAFKA_BROKERS: z
    .string()
    .min(1)
    .default('localhost:19092')
    .transform((value) => value.split(',').map((broker) => broker.trim()))
    .pipe(
      z.array(z.string().regex(/^[^\s:,]+:\d+$/, 'Expected host:port')).min(1),
    ),
  KAFKA_GROUP_ID: z.string().min(1).default('pulsechat-processors'),
  KAFKA_PARTITIONS: z.coerce.number().int().min(6).default(6),
  KAFKA_REPLICATION_FACTOR: positiveInteger.default(1),
  KAFKA_REQUEST_TIMEOUT_MS: positiveInteger.default(30000),
  REDIS_URL: z
    .string()
    .url()
    .refine(
      (value) => /^rediss?:/.test(value),
      'Expected redis:// or rediss://',
    )
    .default('redis://localhost:16379'),
  MONGO_URL: z
    .string()
    .regex(/^mongodb(?:\+srv)?:\/\//)
    .default('mongodb://localhost:27018'),
  MONGO_DB: z
    .string()
    .regex(/^[a-zA-Z0-9_-]+$/)
    .default('pulsechat'),
  JWT_SECRET: z.string().min(32),
  JWT_EXPIRES_IN_SECONDS: positiveInteger.default(86400),
  MAX_MESSAGE_BYTES: positiveInteger.max(65536).default(16384),
  MAX_SOCKET_BUFFER_BYTES: positiveInteger.default(1048576),
  HEARTBEAT_INTERVAL_MS: positiveInteger.default(15000),
  PRESENCE_TTL_SECONDS: positiveInteger.default(30),
  SHUTDOWN_TIMEOUT_MS: positiveInteger.default(15000),
  DEPENDENCY_TIMEOUT_MS: positiveInteger.default(3000),
  RATE_LIMIT_CAPACITY: positiveInteger.max(100000).default(100),
  RATE_LIMIT_REFILL_PER_SECOND: z.coerce
    .number()
    .finite()
    .min(0.1)
    .max(100000)
    .default(50),
  PROCESSING_MAX_RETRIES: z.coerce.number().int().min(1).max(20).default(5),
});

export function parseConfig(env: NodeJS.ProcessEnv) {
  return envSchema
    .superRefine((value, context) => {
      if (value.PRESENCE_TTL_SECONDS * 1000 <= value.HEARTBEAT_INTERVAL_MS) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ['PRESENCE_TTL_SECONDS'],
          message: 'Presence TTL must exceed heartbeat interval',
        });
      }
      if (value.MAX_SOCKET_BUFFER_BYTES < value.MAX_MESSAGE_BYTES * 2) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ['MAX_SOCKET_BUFFER_BYTES'],
          message:
            'Socket buffer must hold at least two maximum-sized client frames',
        });
      }
    })
    .parse(env);
}

export type Config = ReturnType<typeof parseConfig>;
