import { afterEach, expect, it, vi } from 'vitest';
import type { Producer } from 'kafkajs';
import { DurableProducerBatch } from '../../packages/shared/src/producer-batch.js';
afterEach(() => vi.useRealTimers());
function gate() {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
it('coalesces messages and receipts in key order, and waits for the durable ACK', async () => {
  vi.useFakeTimers();
  const ack = gate();
  const sendBatch = vi.fn<Producer['sendBatch']>(async () => {
    await ack.promise;
    return [];
  });
  const batch = new DurableProducerBatch(
    { sendBatch },
    { maxMessages: 64, lingerMs: 5 },
  );
  let completed = false;
  const pending = Promise.all([
    batch.send('messages', 'chat', 'first'),
    batch.send('receipts', 'chat', 'delivered'),
    batch.send('messages', 'chat', 'second'),
  ]).then(() => {
    completed = true;
  });
  await vi.advanceTimersByTimeAsync(5);
  expect(sendBatch).toHaveBeenCalledExactlyOnceWith({
    acks: -1,
    topicMessages: [
      {
        topic: 'messages',
        messages: [
          { key: 'chat', value: 'first' },
          { key: 'chat', value: 'second' },
        ],
      },
      { topic: 'receipts', messages: [{ key: 'chat', value: 'delivered' }] },
    ],
  });
  expect(completed).toBe(false);
  ack.resolve();
  await pending;
  await batch.close();
});
it('never overlaps durable requests and drains queued work before closing', async () => {
  vi.useFakeTimers();
  const first = gate();
  const sendBatch = vi
    .fn<Producer['sendBatch']>()
    .mockImplementationOnce(async () => {
      await first.promise;
      return [];
    })
    .mockResolvedValue([]);
  const batch = new DurableProducerBatch(
    { sendBatch },
    { maxMessages: 2, lingerMs: 5 },
  );
  const pending = [
    batch.send('messages', 'chat', 'a'),
    batch.send('messages', 'chat', 'b'),
  ];
  await vi.advanceTimersByTimeAsync(0);
  pending.push(
    batch.send('messages', 'chat', 'c'),
    batch.send('messages', 'chat', 'd'),
  );
  await vi.advanceTimersByTimeAsync(10);
  expect(sendBatch).toHaveBeenCalledTimes(1);
  const closed = batch.close();
  await expect(batch.send('messages', 'chat', 'late')).rejects.toThrow(
    'closing',
  );
  first.resolve();
  await closed;
  await Promise.all(pending);
  expect(sendBatch).toHaveBeenCalledTimes(2);
  expect(
    sendBatch.mock.calls[1]![0].topicMessages![0]!.messages.map((m) => m.value),
  ).toEqual(['c', 'd']);
});
it('rejects every member of an uncertain failed batch without reporting success', async () => {
  vi.useFakeTimers();
  const sendBatch = vi.fn<Producer['sendBatch']>(async () => {
    throw new Error('broker unavailable');
  });
  const batch = new DurableProducerBatch(
    { sendBatch },
    { maxMessages: 64, lingerMs: 5 },
  );
  const pending = Promise.allSettled([
    batch.send('messages', 'chat', 'a'),
    batch.send('receipts', 'chat', 'b'),
  ]);
  await vi.advanceTimersByTimeAsync(5);
  expect((await pending).map((item) => item.status)).toEqual([
    'rejected',
    'rejected',
  ]);
  await batch.close();
});
it('bounds admission and batch bytes while preserving FIFO work', async () => {
  vi.useFakeTimers();
  const sendBatch = vi.fn<Producer['sendBatch']>(async () => []);
  const batch = new DurableProducerBatch(
    { sendBatch },
    { maxMessages: 64, lingerMs: 5, maxQueuedMessages: 2, maxBatchBytes: 150 },
  );
  const a = batch.send('messages', 'chat', 'a');
  const b = batch.send('messages', 'chat', 'b');
  await expect(batch.send('messages', 'chat', 'c')).rejects.toThrow('full');
  await expect(batch.send('messages', 'chat', 'x'.repeat(200))).rejects.toThrow(
    'full',
  );
  await batch.close();
  await Promise.all([a, b]);
  expect(sendBatch).toHaveBeenCalledTimes(2);
  expect(
    sendBatch.mock.calls.map(
      ([request]) => request.topicMessages![0]!.messages[0]!.value,
    ),
  ).toEqual(['a', 'b']);
});
