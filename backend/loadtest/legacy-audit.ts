import { readFile, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { Kafka, logLevel } from 'kafkajs';
import { MongoClient } from 'mongodb';

const directory = process.argv[2];
if (!directory) throw new Error('Supply the historical load-run directory');
const fixture = JSON.parse(await readFile(directory + '/fixture.json', 'utf8'));
const chats = new Set<string>(
  fixture.accounts.map((a: { chatId: string }) => a.chatId),
);
const kafka = new Kafka({
  clientId: 'legacy-evidence',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:19092').split(','),
  logLevel: logLevel.NOTHING,
  requestTimeout: 30000,
});
const admin = kafka.admin();
const consumer = kafka.consumer({
  groupId: 'legacy-evidence-' + randomUUID(),
  sessionTimeout: 60000,
});
const mongo = new MongoClient(
  process.env.MONGO_URL ?? 'mongodb://localhost:27018',
  { serverSelectionTimeoutMS: 30000 },
);
const positions = new Map<string, bigint>();
const source = new Map<string, unknown>();
const dlq: unknown[] = [];
const heads: {
  topic: string;
  partition: number;
  offset: string;
  low: string;
}[] = [];
try {
  await Promise.all([admin.connect(), consumer.connect(), mongo.connect()]);
  for (const topic of ['messages', 'messages.dlq'])
    for (const p of await admin.fetchTopicOffsets(topic)) {
      heads.push({ topic, ...p });
      positions.set(topic + ':' + p.partition, BigInt(p.low));
    }
  consumer.on(consumer.events.GROUP_JOIN, () => {
    for (const p of heads)
      consumer.seek({ topic: p.topic, partition: p.partition, offset: p.low });
  });
  await consumer.subscribe({
    topics: ['messages', 'messages.dlq'],
    fromBeginning: true,
  });
  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async ({
      batch,
      heartbeat,
      resolveOffset,
      isRunning,
      isStale,
    }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;
        const key = batch.topic + ':' + batch.partition;
        positions.set(key, BigInt(message.offset) + 1n);
        try {
          const value = JSON.parse(message.value?.toString() ?? 'null');
          const record =
            batch.topic === 'messages.dlq'
              ? JSON.parse(Buffer.from(value.value ?? '', 'base64').toString())
              : value;
          if (
            record &&
            chats.has(record.chatId) &&
            record.body?.startsWith('k6-socket-')
          ) {
            const evidence = {
              messageId: record.messageId,
              clientMsgId: record.clientMsgId,
              chatId: record.chatId,
              body: record.body,
              partition: batch.partition,
              offset: message.offset,
              ...(batch.topic === 'messages.dlq'
                ? {
                    failure: value.error,
                    failedAt: value.failedAt,
                    originalPartition: value.partition,
                    originalOffset: value.offset,
                  }
                : {}),
            };
            if (batch.topic === 'messages')
              source.set(record.messageId, evidence);
            else dlq.push(evidence);
          }
        } catch {
          // Malformed records are unrelated to the historical JSON message set.
        }
        resolveOffset(message.offset);
        await heartbeat();
      }
    },
  });
  const deadline = Date.now() + 300000;
  while (
    heads.some(
      (p) =>
        (positions.get(p.topic + ':' + p.partition) ?? 0n) < BigInt(p.offset),
    )
  ) {
    if (Date.now() > deadline)
      throw new Error('Retained log scan did not reach captured heads');
    await delay(500);
  }
  const stored = await mongo
    .db(process.env.MONGO_DB ?? 'pulsechat')
    .collection('messages')
    .find(
      { chatId: { $in: [...chats] }, body: { $regex: '^k6-socket-' } },
      {
        projection: {
          _id: 0,
          messageId: 1,
          clientMsgId: 1,
          chatId: 1,
          body: 1,
          seq: 1,
        },
      },
    )
    .toArray();
  const ids = new Set(stored.map((m) => m.messageId));
  const result = {
    observedAt: new Date().toISOString(),
    historicalIssued: 992,
    historicalStored: 975,
    ackLedgerAvailable: false,
    heads,
    positions: Object.fromEntries(
      [...positions].map(([k, v]) => [k, v.toString()]),
    ),
    retainedSourceUnique: source.size,
    storedNow: stored.length,
    sourceNotStored: [...source.entries()]
      .filter(([id]) => !ids.has(id))
      .map(([, v]) => v),
    dlq,
    source: [...source.values()],
  };
  await writeFile(
    directory + '/retained-log-audit.json',
    JSON.stringify(result, null, 2) + '\n',
  );
  console.log(
    JSON.stringify({
      ...result,
      source: undefined,
      heads: undefined,
      positions: undefined,
    }),
  );
} finally {
  await consumer.disconnect();
  await admin.disconnect();
  await mongo.close();
}
