import { Kafka, logLevel, type Admin } from 'kafkajs';
import type { Config } from './config.js';

export const TOPICS = ['messages', 'receipts', 'messages.dlq'] as const;

export function createKafka(config: Config, service: string) {
  return new Kafka({
    clientId: `${service}-${config.INSTANCE_ID}`,
    brokers: config.KAFKA_BROKERS,
    logLevel: logLevel.NOTHING,
    connectionTimeout: config.DEPENDENCY_TIMEOUT_MS,
    requestTimeout: config.KAFKA_REQUEST_TIMEOUT_MS,
    retry: { initialRetryTime: 300, retries: 5 },
  });
}

export async function ensureTopics(admin: Admin, config: Config) {
  // createTopics is safe when another starting replica created the topics first.
  await admin.createTopics({
    waitForLeaders: true,
    topics: TOPICS.map((topic) => ({
      topic,
      numPartitions: config.KAFKA_PARTITIONS,
      replicationFactor: config.KAFKA_REPLICATION_FACTOR,
    })),
  });
  await verifyTopics(admin, config);
}

export async function verifyTopics(admin: Admin, config: Config) {
  const metadata = await admin.fetchTopicMetadata({ topics: [...TOPICS] });
  for (const name of TOPICS) {
    const topic = metadata.topics.find((entry) => entry.name === name);
    if (
      !topic ||
      topic.partitions.length !== config.KAFKA_PARTITIONS ||
      topic.partitions.some(
        (partition) =>
          partition.leader < 0 ||
          partition.replicas.length !== config.KAFKA_REPLICATION_FACTOR,
      )
    ) {
      throw new Error(
        `Topic ${name} has missing leaders or incorrect partition/replication configuration`,
      );
    }
  }
}
