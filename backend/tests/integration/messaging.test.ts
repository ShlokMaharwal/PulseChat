import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import WebSocket from 'ws';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createDependencies,
  createLogger,
  parseConfig,
  SequenceAllocator,
  deadLetterSchema,
  UserRouter,
} from '@pulsechat/shared';
import { TestClient } from '../helpers/test-client.js';

const config = parseConfig(process.env);
const dependencies = createDependencies(
  config,
  'integration',
  createLogger(config, 'integration'),
);
const clients: TestClient[] = [];
const password = 'Integration-password-123!';
const gatewayA = process.env.TEST_GATEWAY_A ?? 'http://gateway-1:8080';
const gatewayB = process.env.TEST_GATEWAY_B ?? 'http://gateway-2:8080';

async function request<T>(
  base: string,
  path: string,
  body?: unknown,
  token?: string,
): Promise<T> {
  const response = await fetch(`${base}${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: {
      'content-type': 'application/json',
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(60000),
  });
  const result: unknown = await response.json();
  if (!response.ok)
    throw new Error(`${response.status}: ${JSON.stringify(result)}`);
  return result as T;
}

async function usersAndChat() {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const a = await request<{
    token: string;
    user: { id: string; username: string };
  }>(gatewayA, '/auth/register', { username: `a_${suffix}`, password });
  const b = await request<{
    token: string;
    user: { id: string; username: string };
  }>(gatewayB, '/auth/register', { username: `b_${suffix}`, password });
  const { chat } = await request<{ chat: { id: string } }>(
    gatewayA,
    '/chats',
    { userId: b.user.id },
    a.token,
  );
  const sender = new TestClient();
  const receiver = new TestClient();
  clients.push(sender, receiver);
  const welcomeA = await sender.connect(
    gatewayA.replace(/^http/, 'ws') + '/ws',
    a.token,
  );
  const welcomeB = await receiver.connect(
    gatewayB.replace(/^http/, 'ws') + '/ws',
    b.token,
  );
  expect(
    welcomeA.type === 'welcome' &&
      welcomeB.type === 'welcome' &&
      welcomeA.gatewayId !== welcomeB.gatewayId,
  ).toBe(true);
  return { a, b, chat, sender, receiver };
}

beforeAll(async () => {
  await dependencies.connect();
});
afterAll(async () => {
  await Promise.all(clients.map((client) => client.close()));
  await dependencies.close();
});

describe('Phase 2 live messaging', () => {
  it('rejects unauthenticated upgrades and non-member sends before persistence', async () => {
    const { chat, a, b } = await usersAndChat();
    await expect(request(gatewayA, '/chats')).rejects.toThrow('401');
    const login = await request<{ token: string; user: { id: string } }>(
      gatewayA,
      '/auth/login',
      { username: a.user.username.toUpperCase(), password },
    );
    expect(login.user.id).toBe(a.user.id);
    await expect(
      request(gatewayA, '/auth/login', {
        username: a.user.username,
        password: 'wrong-password',
      }),
    ).rejects.toThrow('401');
    const storedUser = await dependencies.db
      .collection('users')
      .findOne({ id: a.user.id });
    expect(storedUser?.passwordHash).toMatch(/^\$2[aby]\$/);
    expect(storedUser).not.toHaveProperty('password');
    const invalid = new TestClient();
    clients.push(invalid);
    await expect(
      invalid.connect(gatewayA.replace(/^http/, 'ws') + '/ws', 'invalid-token'),
    ).rejects.toThrow('401');
    const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
    const outsider = await request<{ token: string }>(
      gatewayA,
      '/auth/register',
      { username: `outsider_${suffix}`, password },
    );
    const client = new TestClient();
    clients.push(client);
    await client.connect(
      gatewayB.replace(/^http/, 'ws') + '/ws',
      outsider.token,
    );
    await expect(client.message(chat.id, 'not authorized')).rejects.toThrow(
      'FORBIDDEN',
    );
    await expect(
      request(
        gatewayA,
        `/chats/${chat.id}/messages`,
        undefined,
        outsider.token,
      ),
    ).rejects.toThrow('403');
    expect(
      await dependencies.db
        .collection('messages')
        .countDocuments({ chatId: chat.id }),
    ).toBe(0);
    const existing = await request<{ chat: { id: string } }>(
      gatewayA,
      '/chats',
      { userId: b.user.id },
      a.token,
    );
    expect(existing.chat.id).toBe(chat.id);
  });

  it('delivers 1,000 ordered messages across different gateways without duplicate wire delivery', async () => {
    const { chat, sender, receiver } = await usersAndChat();
    for (let start = 0; start < 1000; start += 20) {
      await Promise.all(
        Array.from({ length: 20 }, (_, index) =>
          sender.message(chat.id, `message-${start + index}`),
        ),
      );
      // This case proves send order below the admission limit. Rejected sends
      // are retried separately and have a later Kafka append order.
      await delay(500);
    }
    await receiver.waitFor(
      (frame) =>
        frame.type === 'message' &&
        frame.chatId === chat.id &&
        frame.seq === 1000,
      180000,
    );
    const delivered = receiver.messages.filter(
      (message) => message.chatId === chat.id,
    );
    expect(delivered).toHaveLength(1000);
    expect(
      sender.frames.some(
        (frame) => frame.type === 'error' && frame.code === 'RATE_LIMITED',
      ),
    ).toBe(false);
    expect(
      receiver.frames.filter(
        (frame) => frame.type === 'message' && frame.chatId === chat.id,
      ),
    ).toHaveLength(1000);
    expect(new Set(delivered.map((message) => message.messageId)).size).toBe(
      1000,
    );
    expect(delivered.map((message) => message.seq)).toEqual(
      Array.from({ length: 1000 }, (_, index) => index + 1),
    );
    expect(delivered.map((message) => message.body)).toEqual(
      Array.from({ length: 1000 }, (_, index) => `message-${index}`),
    );
    const stored = await dependencies.db
      .collection('messages')
      .find({ chatId: chat.id })
      .sort({ seq: 1 })
      .toArray();
    expect(
      stored.map((message) => ({
        messageId: message.messageId,
        seq: message.seq,
        body: message.body,
      })),
    ).toEqual(
      delivered.map((message) => ({
        messageId: message.messageId,
        seq: message.seq,
        body: message.body,
      })),
    );
  });

  it('stores one message for five sends using the same clientMsgId', async () => {
    const { chat, sender, receiver } = await usersAndChat();
    const clientMsgId = randomUUID();
    for (let index = 0; index < 5; index++)
      await sender.message(chat.id, 'retry-safe message', clientMsgId);
    await sender.waitFor(
      (frame) =>
        frame.type === 'ack' &&
        frame.clientMsgId === clientMsgId &&
        frame.status === 'sent' &&
        sender.frames.filter(
          (item) =>
            item.type === 'ack' &&
            item.clientMsgId === clientMsgId &&
            item.status === 'sent',
        ).length >= 5,
      120000,
    );
    await receiver.waitFor(
      (frame) => frame.type === 'message' && frame.clientMsgId === clientMsgId,
    );
    expect(
      await dependencies.db
        .collection('messages')
        .countDocuments({ chatId: chat.id, clientMsgId }),
    ).toBe(1);
    expect(
      receiver.messages.filter(
        (message) => message.clientMsgId === clientMsgId,
      ),
    ).toHaveLength(1);
    expect(await dependencies.redis.get(`chat:${chat.id}:seq`)).toBe('1');
  });

  it('reuses an assigned sequence after failure before persistence and safely reseeds after counter loss', async () => {
    const allocatorA = new SequenceAllocator(dependencies);
    const allocatorB = new SequenceAllocator(dependencies);
    const chatId = randomUUID();
    const messageId = randomUUID();
    expect(await allocatorA.assign(chatId, messageId)).toBe(1);
    expect(await allocatorB.assign(chatId, messageId)).toBe(1);
    await dependencies.db.collection('messages').insertOne({
      messageId,
      chatId,
      seq: 1,
      clientMsgId: randomUUID(),
      senderId: randomUUID(),
      body: 'seed',
      createdAt: new Date().toISOString(),
    });
    await dependencies.redis.del(`chat:${chatId}:seq`, `chat:${chatId}:msgseq`);
    const allocated = await Promise.all(
      Array.from({ length: 20 }, (_, index) =>
        (index % 2 ? allocatorA : allocatorB).assign(chatId, randomUUID()),
      ),
    );
    expect(allocated.sort((a, b) => a - b)).toEqual(
      Array.from({ length: 20 }, (_, index) => index + 2),
    );
    expect(await dependencies.redis.get(`chat:${chatId}:seq`)).toBe('21');
  });

  it('recovers missed messages in order and persists delivered/read cursors', async () => {
    const { a, b, chat, sender, receiver } = await usersAndChat();
    await receiver.close();
    for (let index = 0; index < 125; index++)
      await sender.message(chat.id, `offline-${index}`);
    await sender.waitForSeq(chat.id, 125, 180000);
    await receiver.connect(gatewayB.replace(/^http/, 'ws') + '/ws', b.token);
    await receiver.waitForSeq(chat.id, 125, 120000);
    expect(
      receiver.messages
        .filter((message) => message.chatId === chat.id)
        .map((message) => message.seq),
    ).toEqual(Array.from({ length: 125 }, (_, i) => i + 1));
    await sender.waitFor(
      (frame) =>
        frame.type === 'receipt' &&
        frame.chatId === chat.id &&
        frame.userId === b.user.id &&
        frame.status === 'delivered' &&
        frame.seq === 125,
    );
    receiver.read(chat.id);
    await sender.waitFor(
      (frame) =>
        frame.type === 'receipt' &&
        frame.chatId === chat.id &&
        frame.status === 'read' &&
        frame.seq === 125,
    );
    const cursor = await dependencies.db
      .collection('chat_receipts')
      .findOne({ chatId: chat.id, userId: b.user.id });
    expect(cursor).toMatchObject({ deliveredSeq: 125, readSeq: 125 });
    receiver.read(chat.id, 1);
    const replayProducer = dependencies.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
    });
    await replayProducer.connect();
    try {
      const [append] = await replayProducer.send({
        topic: 'receipts',
        acks: -1,
        messages: [
          {
            key: chat.id,
            value: JSON.stringify({
              type: 'receipt_event',
              chatId: chat.id,
              userId: b.user.id,
              status: 'read',
              seq: 1,
              timestamp: new Date(0).toISOString(),
            }),
          },
        ],
      });
      const until = Date.now() + 60000;
      for (;;) {
        const offsets = await dependencies.admin.fetchOffsets({
          groupId: config.KAFKA_GROUP_ID,
          topics: ['receipts'],
        });
        const committed =
          offsets[0]?.partitions.find((p) => p.partition === append!.partition)
            ?.offset ?? '-1';
        if (BigInt(committed) > BigInt(append!.baseOffset!)) break;
        if (Date.now() >= until)
          throw new Error('Stale receipt was not committed');
        await delay(100);
      }
      const afterReplay = await dependencies.db
        .collection('chat_receipts')
        .findOne({ chatId: chat.id, userId: b.user.id });
      expect(afterReplay).toMatchObject({
        deliveredSeq: 125,
        readSeq: 125,
        updatedAt: cursor!.updatedAt,
      });
    } finally {
      await replayProducer.disconnect();
    }
    const summary = await request<{
      chats: { id: string; unreadCount: number }[];
    }>(gatewayB, '/chats', undefined, b.token);
    expect(summary.chats.find((item) => item.id === chat.id)?.unreadCount).toBe(
      0,
    );
    const presence = await request<{ online: boolean }>(
      gatewayA,
      `/users/${b.user.id}/presence`,
      undefined,
      a.token,
    );
    expect(presence.online).toBe(true);
  });

  it('routes to duplicate user sessions on different gateways', async () => {
    const { b, chat, sender, receiver } = await usersAndChat();
    const second = new TestClient();
    clients.push(second);
    await second.connect(gatewayA.replace(/^http/, 'ws') + '/ws', b.token);
    await dependencies.redis.sadd(`user:${b.user.id}:gateways`, 'stale-route');
    await dependencies.redis.set(
      `user:${b.user.id}:gateway`,
      'stale-route',
      'EX',
      30,
    );
    const targets = await new UserRouter(
      dependencies.redis,
      'integration',
      30,
    ).targets(b.user.id);
    expect(targets).toHaveLength(2);
    expect(
      await dependencies.redis.sismember(
        `user:${b.user.id}:gateways`,
        'stale-route',
      ),
    ).toBe(0);
    await sender.message(chat.id, 'two sessions');
    await Promise.all([
      receiver.waitForSeq(chat.id, 1),
      second.waitForSeq(chat.id, 1),
    ]);
    expect(
      receiver.messages.filter((message) => message.chatId === chat.id),
    ).toHaveLength(1);
    expect(
      second.messages.filter((message) => message.chatId === chat.id),
    ).toHaveLength(1);
    await second.close();
    await sender.message(chat.id, 'remaining session');
    await receiver.waitForSeq(chat.id, 2);
  });

  it('dead-letters a poison record and continues the same chat partition', async () => {
    const { chat, sender, receiver } = await usersAndChat();
    const consumer = dependencies.kafka.consumer({
      groupId: `dlq-test-${randomUUID()}`,
    });
    const producer = dependencies.kafka.producer();
    const poison = `poison-${randomUUID()}`;
    let found: ReturnType<typeof deadLetterSchema.parse> | undefined;
    try {
      await consumer.connect();
      await producer.connect();
      await consumer.subscribe({ topic: 'messages.dlq', fromBeginning: true });
      await consumer.run({
        eachMessage: async ({ message }) => {
          const parsed = deadLetterSchema.safeParse(
            JSON.parse(message.value!.toString()),
          );
          if (
            parsed.success &&
            parsed.data.value &&
            Buffer.from(parsed.data.value, 'base64').toString() === poison
          )
            found = parsed.data;
        },
      });
      await producer.send({
        topic: 'messages',
        acks: -1,
        messages: [{ key: chat.id, value: poison }],
      });
      await sender.message(chat.id, 'after poison');
      await receiver.waitForSeq(chat.id, 1, 120000);
      await expect.poll(() => found, { timeout: 60000 }).toBeDefined();
      expect(found).toMatchObject({
        originalTopic: 'messages',
        attempts: config.PROCESSING_MAX_RETRIES + 1,
        encoding: 'base64',
      });
    } finally {
      await consumer.disconnect();
      await producer.disconnect();
    }
  });

  it('rejects a user burst with an atomic shared rate bucket', async () => {
    const { chat, sender, a } = await usersAndChat();
    // Exhaust the bucket using server time, then exercise rejection through the socket.
    await dependencies.redis.hset(
      `rate:${a.user.id}`,
      'tokens',
      0,
      'updated',
      Date.now() + 60000,
    );
    const clientMsgId = randomUUID();
    sender.send({
      type: 'message',
      chatId: chat.id,
      body: 'rate limited',
      clientMsgId,
    });
    await sender.waitFor(
      (frame) =>
        frame.type === 'error' &&
        frame.clientMsgId === clientMsgId &&
        frame.code === 'RATE_LIMITED',
    );
    expect(
      await dependencies.db
        .collection('messages')
        .countDocuments({ chatId: chat.id }),
    ).toBe(0);
    await dependencies.redis.del(`rate:${a.user.id}`);
    await sender.message(chat.id, 'after refill');
  });

  it('rejects malformed JSON without closing a healthy text connection', async () => {
    const { chat, sender, receiver } = await usersAndChat();
    sender.sendRaw('{broken');
    await sender.waitFor(
      (frame) => frame.type === 'error' && frame.code === 'INVALID_MESSAGE',
    );
    await sender.message(chat.id, 'valid after malformed');
    await receiver.waitForSeq(chat.id, 1);
    expect(
      await dependencies.db
        .collection('messages')
        .countDocuments({ chatId: chat.id }),
    ).toBe(1);
  });

  it('closes binary and oversized sockets with the documented codes', async () => {
    const { a } = await usersAndChat();
    for (const [payload, code] of [
      [Buffer.from('binary'), 1003],
      ['x'.repeat(config.MAX_MESSAGE_BYTES + 1), 1009],
    ] as const) {
      const socket = new WebSocket(gatewayA.replace(/^http/, 'ws') + '/ws', [
        'pulsechat',
        `jwt.${a.token}`,
      ]);
      socket.on('error', () => undefined);
      try {
        await once(socket, 'open');
        const closed = new Promise<number>((resolve) =>
          socket.once('close', (closeCode) => resolve(closeCode)),
        );
        socket.send(payload);
        expect(await closed).toBe(code);
      } finally {
        socket.terminate();
      }
    }
  });
});
