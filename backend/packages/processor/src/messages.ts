import {
  kafkaMessageSchema,
  storedMessageSchema,
  SequenceAllocator,
  receiptEventSchema,
  deadLetterSchema,
  UserRouter,
  type ReceiptEvent,
  type ChatReceiptRecord,
  type ServiceContext,
  type StoredMessage,
  type ServerFrame,
} from '@pulsechat/shared';
import type { Consumer, EachBatchPayload, KafkaMessage } from 'kafkajs';
import { setTimeout as delay } from 'node:timers/promises';

type InputMessage = ReturnType<typeof kafkaMessageSchema.parse>;

export class MessageProcessor {
  private readonly consumer: Consumer;
  private readonly allocator;
  private readonly producer;
  private readonly router;
  private active = false;
  private stopping = false;

  constructor(private readonly context: ServiceContext) {
    this.consumer = context.dependencies.kafka.consumer({
      groupId: context.config.KAFKA_GROUP_ID,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    });
    this.allocator = new SequenceAllocator(context.dependencies);
    this.producer = context.dependencies.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
    });
    this.router = new UserRouter(
      context.dependencies.redis,
      context.config.INSTANCE_ID,
      context.config.PRESENCE_TTL_SECONDS,
    );
    this.consumer.on(this.consumer.events.GROUP_JOIN, () => {
      this.active = true;
    });
    this.consumer.on(this.consumer.events.CRASH, ({ payload }) => {
      this.active = false;
      context.logger.error({ err: payload.error }, 'Consumer crashed');
      if (!payload.restart && !this.stopping) context.fail(payload.error);
    });
  }

  ready = () => this.active && !this.stopping;

  async start() {
    await this.consumer.connect();
    await this.producer.connect();
    await this.consumer.subscribe({
      topics: ['messages', 'receipts'],
      fromBeginning: true,
    });
    await this.consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      partitionsConsumedConcurrently: 3,
      eachBatch: (payload) => this.batch(payload),
    });
  }

  private async batch({
    batch,
    resolveOffset,
    heartbeat,
    isRunning,
    isStale,
  }: EachBatchPayload) {
    let nextOffset: string | undefined;
    for (const record of batch.messages) {
      if (!isRunning() || isStale()) break;
      const completed = await this.retryRecord(
        batch.topic,
        batch.partition,
        record,
        heartbeat,
        () => isRunning() && !isStale(),
      );
      if (!completed) break;
      resolveOffset(record.offset);
      nextOffset = (BigInt(record.offset) + 1n).toString();
      await heartbeat();
    }
    if (nextOffset && !isStale()) {
      // Only offsets whose messages have persisted successfully are committed.
      await this.consumer.commitOffsets([
        { topic: batch.topic, partition: batch.partition, offset: nextOffset },
      ]);
    }
  }

  private async retryRecord(
    topic: string,
    partition: number,
    record: KafkaMessage,
    heartbeat: () => Promise<void>,
    running: () => boolean,
  ) {
    let failure: unknown;
    const attempts = this.context.config.PROCESSING_MAX_RETRIES + 1;
    for (let attempt = 0; attempt < attempts; attempt++) {
      if (!running()) return false;
      try {
        if (!record.value) throw new Error('Kafka record has no value');
        const parsed: unknown = JSON.parse(record.value.toString('utf8'));
        const event =
          topic === 'receipts'
            ? receiptEventSchema.parse(parsed)
            : kafkaMessageSchema.parse(parsed);
        if (record.key?.toString('utf8') !== event.chatId)
          throw new Error('Kafka partition key does not match chatId');
        if ('type' in event) await this.receipt(event);
        else await this.process(event, heartbeat);
        return true;
      } catch (error) {
        failure = error;
        this.context.logger.warn(
          {
            err: error,
            topic,
            partition,
            offset: record.offset,
            attempt: attempt + 1,
          },
          'Record processing failed',
        );
        if (attempt + 1 < attempts) {
          await delay(Math.min(100 * 2 ** attempt, 1600));
          await heartbeat();
        }
      }
    }
    const error =
      failure instanceof Error ? failure : new Error(String(failure));
    const deadLetter = deadLetterSchema.parse({
      originalTopic: topic,
      partition,
      offset: record.offset,
      encoding: 'base64',
      key: record.key?.toString('base64') ?? null,
      value: record.value?.toString('base64') ?? null,
      error: { name: error.name, message: error.message, stack: error.stack },
      attempts,
      failedAt: new Date().toISOString(),
    });
    // Keep ownership and the offset until the DLQ is durably acknowledged.
    while (running()) {
      try {
        await this.producer.send({
          topic: 'messages.dlq',
          acks: -1,
          messages: [{ key: record.key, value: JSON.stringify(deadLetter) }],
        });
        return true;
      } catch (err) {
        this.context.logger.error(
          { err, topic, partition, offset: record.offset },
          'DLQ publish failed; retaining offset',
        );
        await delay(1000);
        await heartbeat();
      }
    }
    return false;
  }

  private async receipt(event: ReceiptEvent) {
    const { db } = this.context.dependencies;
    const chat = await db
      .collection<{ id: string; memberIds: string[] }>('chats')
      .findOne({ id: event.chatId, memberIds: event.userId });
    if (!chat) throw new Error('Receipt user is not a chat member');
    const collection = db.collection<ChatReceiptRecord>('chat_receipts');
    const previous = await collection.findOne({
      chatId: event.chatId,
      userId: event.userId,
    });
    const current =
      event.status === 'read' ? previous?.readSeq : previous?.deliveredSeq;
    if ((current ?? 0) >= event.seq) return;
    await collection.updateOne(
      { chatId: event.chatId, userId: event.userId },
      {
        $max: {
          deliveredSeq: event.seq,
          readSeq: event.status === 'read' ? event.seq : 0,
        },
        $set: { updatedAt: event.timestamp },
        $setOnInsert: { chatId: event.chatId, userId: event.userId },
      },
      { upsert: true },
    );
    for (const userId of chat.memberIds)
      await this.publish(userId, {
        type: 'receipt',
        chatId: event.chatId,
        userId: event.userId,
        status: event.status,
        seq: event.seq,
        timestamp: event.timestamp,
      }).catch((err: unknown) =>
        this.context.logger.warn(
          { err, chatId: event.chatId, userId },
          'Receipt live delivery unavailable',
        ),
      );
  }

  private assertSame(existing: StoredMessage, incoming: InputMessage) {
    if (
      existing.senderId !== incoming.senderId ||
      existing.chatId !== incoming.chatId ||
      existing.clientMsgId !== incoming.clientMsgId ||
      existing.body !== incoming.body
    ) {
      throw new Error('clientMsgId was reused for a different logical message');
    }
  }

  private async publish(userId: string, payload: ServerFrame) {
    await this.router.publish(userId, payload);
  }

  private async sent(message: StoredMessage) {
    await this.publish(message.senderId, {
      type: 'ack',
      clientMsgId: message.clientMsgId,
      status: 'sent',
      messageId: message.messageId,
      seq: message.seq,
    });
  }

  async process(
    input: InputMessage,
    heartbeat: () => Promise<void> = async () => undefined,
  ) {
    const { db } = this.context.dependencies;
    const messages = db.collection<StoredMessage>('messages');
    const existingRecord = await messages.findOne(
      { messageId: input.messageId },
      { projection: { _id: 0 } },
    );
    if (existingRecord) {
      const existing = storedMessageSchema.parse(existingRecord);
      this.assertSame(existing, input);
      this.context.logger.info(
        {
          messageId: input.messageId,
          chatId: input.chatId,
          userId: input.senderId,
        },
        'Duplicate message already persisted',
      );
      await this.sent(existing).catch((err: unknown) =>
        this.context.logger.warn(
          { err, messageId: input.messageId },
          'Live sender acknowledgment unavailable',
        ),
      );
      return existing;
    }
    const seq = await this.allocator.assign(
      input.chatId,
      input.messageId,
      heartbeat,
    );
    let message: StoredMessage = {
      messageId: input.messageId,
      clientMsgId: input.clientMsgId,
      chatId: input.chatId,
      senderId: input.senderId,
      body: input.body,
      createdAt: input.createdAt,
      seq,
    };
    let inserted = true;
    try {
      // The driver may add _id to its argument; keep it out of the wire object.
      await messages.insertOne({ ...message });
    } catch (error) {
      if (
        !error ||
        typeof error !== 'object' ||
        !('code' in error) ||
        error.code !== 11000
      )
        throw error;
      const canonical = await messages.findOne(
        { messageId: input.messageId },
        { projection: { _id: 0 } },
      );
      if (!canonical) throw error;
      message = storedMessageSchema.parse(canonical);
      this.assertSame(message, input);
      inserted = false;
    }
    this.context.logger.info(
      {
        messageId: message.messageId,
        chatId: message.chatId,
        userId: message.senderId,
        seq: message.seq,
      },
      'Message persisted',
    );
    // Pub/Sub is best effort after persistence; history/sync recovers missed delivery.
    await this.sent(message).catch((err: unknown) =>
      this.context.logger.warn(
        { err, messageId: message.messageId },
        'Live sender acknowledgment unavailable',
      ),
    );
    if (inserted) {
      for (const recipientId of new Set(input.recipientIds)) {
        await this.publish(recipientId, { type: 'message', ...message }).catch(
          (err: unknown) =>
            this.context.logger.warn(
              { err, messageId: message.messageId, userId: recipientId },
              'Live delivery unavailable',
            ),
        );
      }
    }
    return message;
  }

  async stop() {
    this.stopping = true;
    this.active = false;
    await this.consumer.stop();
    await this.consumer.disconnect();
    await this.producer.disconnect();
  }
}
