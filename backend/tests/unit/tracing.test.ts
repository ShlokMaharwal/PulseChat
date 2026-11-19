import { expect, it } from 'vitest';
import { Registry } from 'prom-client';
import {
  applicationMetrics,
  observeStage,
  kafkaMessageSchema,
  clientFrameSchema,
  storedMessageSchema,
} from '@pulsechat/shared';

it('records valid stages without fabricating zero latency for negative clock samples', async () => {
  const registry = new Registry();
  const metrics = applicationMetrics(registry);
  observeStage(metrics, 'kafka_to_processor', 100, 150);
  observeStage(metrics, 'kafka_to_processor', 100, 90);
  const stages = await metrics.stage.get();
  expect(
    stages.values.find((v) => v.metricName?.endsWith('_count'))?.value,
  ).toBe(1);
  expect(stages.values.find((v) => v.metricName?.endsWith('_sum'))?.value).toBe(
    0.05,
  );
  expect((await metrics.clockSkew.get()).values[0]?.value).toBe(1);
});

it('keeps trace metadata out of client input and canonical stored records', () => {
  const message = {
    messageId: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
    clientMsgId: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
    chatId: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
    senderId: 'dddddddd-dddd-4ddd-8ddd-dddddddddddd',
    body: 'Hello',
    createdAt: new Date().toISOString(),
  };
  const trace = { gatewayReceivedAt: 100, produceStartedAt: 110 };
  expect(
    kafkaMessageSchema.safeParse({
      ...message,
      recipientIds: [message.senderId],
      trace,
    }).success,
  ).toBe(true);
  expect(
    kafkaMessageSchema.safeParse({
      ...message,
      recipientIds: [message.senderId],
    }).success,
  ).toBe(true);
  expect(
    storedMessageSchema.safeParse({ ...message, seq: 1, trace }).success,
  ).toBe(false);
  expect(
    clientFrameSchema.safeParse({
      type: 'message',
      clientMsgId: message.clientMsgId,
      chatId: message.chatId,
      body: message.body,
      trace,
    }).success,
  ).toBe(false);
});
