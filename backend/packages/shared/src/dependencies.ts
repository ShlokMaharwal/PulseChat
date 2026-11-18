import { Redis } from 'ioredis';
import { MongoClient } from 'mongodb';
import type { Logger } from 'pino';
import type { Config } from './config.js';
import { createKafka, ensureTopics, verifyTopics } from './kafka.js';

export interface DependencyHealth {
  redis: boolean;
  mongo: boolean;
  kafka: boolean;
}

export function createDependencies(
  config: Config,
  service: string,
  logger: Logger,
) {
  const redis = new Redis(config.REDIS_URL, {
    lazyConnect: true,
    enableOfflineQueue: false,
    maxRetriesPerRequest: 1,
    connectTimeout: config.DEPENDENCY_TIMEOUT_MS,
    commandTimeout: config.DEPENDENCY_TIMEOUT_MS,
    retryStrategy: (attempt) => Math.min(attempt * 200, 2000),
  });
  redis.on('error', (err) => logger.warn({ err }, 'Redis connection error'));
  const mongo = new MongoClient(config.MONGO_URL, {
    serverSelectionTimeoutMS: config.DEPENDENCY_TIMEOUT_MS,
    connectTimeoutMS: config.DEPENDENCY_TIMEOUT_MS,
    socketTimeoutMS: config.DEPENDENCY_TIMEOUT_MS,
  });
  const db = mongo.db(config.MONGO_DB);
  const kafka = createKafka(config, service);
  const admin = kafka.admin();

  async function close() {
    const results = await Promise.allSettled([
      admin.disconnect(),
      mongo.close(),
      Promise.resolve().then(() => redis.disconnect()),
    ]);
    results.forEach((result) => {
      if (result.status === 'rejected')
        logger.error({ err: result.reason }, 'Dependency cleanup failed');
    });
  }

  async function connect() {
    // Await every attempt before cleanup so a late successful connection cannot leak.
    const results = await Promise.allSettled([
      redis.connect(),
      mongo.connect(),
      admin.connect(),
    ]);
    const failed = results.find((result) => result.status === 'rejected');
    if (failed?.status === 'rejected') throw failed.reason;
    await ensureTopics(admin, config);
    await db.collection('messages').createIndexes([
      { key: { messageId: 1 }, unique: true, name: 'messageId_unique' },
      { key: { chatId: 1, seq: 1 }, unique: true, name: 'chatId_seq_unique' },
    ]);
    await db
      .collection('chat_receipts')
      .createIndex(
        { chatId: 1, userId: 1 },
        { unique: true, name: 'chatUser_receipt_unique' },
      );
  }

  async function check(): Promise<DependencyHealth> {
    const results = await Promise.allSettled([
      redis.ping(),
      db.command({ ping: 1 }),
      verifyTopics(admin, config),
    ]);
    return {
      redis: results[0]?.status === 'fulfilled',
      mongo: results[1]?.status === 'fulfilled',
      kafka: results[2]?.status === 'fulfilled',
    };
  }

  return { redis, mongo, db, kafka, admin, connect, check, close };
}
