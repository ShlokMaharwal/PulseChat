import {
  kafkaMessageSchema,
  storedMessageSchema,
  SequenceAllocator,
  type ServiceContext,
  type StoredMessage,
  type ServerFrame,
} from '@pulsechat/shared';
import type { Consumer, EachBatchPayload } from 'kafkajs';

type InputMessage = ReturnType<typeof kafkaMessageSchema.parse>;

export class MessageProcessor {
  private readonly consumer: Consumer;
  private readonly allocator;
  private active = false;
  private stopping = false;

  constructor(private readonly context: ServiceContext) {
    this.consumer = context.dependencies.kafka.consumer({
      groupId: context.config.KAFKA_GROUP_ID,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    });
    this.allocator = new SequenceAllocator(context.dependencies);
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
    await this.consumer.subscribe({
      topics: ['messages'],
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
      if (!record.value) throw new Error('Kafka message has no value');
      const parsed: unknown = JSON.parse(record.value.toString('utf8'));
      const message = kafkaMessageSchema.parse(parsed);
      if (record.key?.toString('utf8') !== message.chatId)
        throw new Error('Kafka partition key does not match chatId');
      await this.process(message, heartbeat);
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
    const { redis } = this.context.dependencies;
    const gatewayId = await redis.get(`user:${userId}:gateway`);
    if (gatewayId)
      await redis.publish(
        `gateway:${gatewayId}`,
        JSON.stringify({ userId, payload }),
      );
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
  }
}
