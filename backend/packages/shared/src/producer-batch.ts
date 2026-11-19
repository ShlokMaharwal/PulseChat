import { Buffer } from 'node:buffer';
import type { Producer } from 'kafkajs';

type Topic = 'messages' | 'receipts';
interface Pending {
  topic: Topic;
  key: string;
  value: string;
  bytes: number;
  resolve: () => void;
  reject: (error: unknown) => void;
}

/** One durable request at a time; callers resolve only after their batch's ACK. */
export class DurableProducerBatch {
  private readonly queue: Pending[] = [];
  private queuedBytes = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private active: Promise<void> | undefined;
  private closing = false;

  constructor(
    private readonly producer: Pick<Producer, 'sendBatch'>,
    private readonly options: {
      maxMessages: number;
      lingerMs: number;
      maxQueuedMessages?: number;
      maxBatchBytes?: number;
      maxQueuedBytes?: number;
      queued?: (count: number) => void;
      batch?: (topic: Topic, count: number) => void;
    },
  ) {}

  send(topic: Topic, key: string, value: string): Promise<void> {
    const bytes = Buffer.byteLength(key) + Buffer.byteLength(value) + 128;
    if (this.closing)
      return Promise.reject(new Error('Kafka publisher is closing'));
    if (
      this.queue.length >= (this.options.maxQueuedMessages ?? 1024) ||
      this.queuedBytes + bytes >
        (this.options.maxQueuedBytes ?? 16 * 1024 * 1024) ||
      bytes > (this.options.maxBatchBytes ?? 512 * 1024)
    )
      return Promise.reject(new Error('Kafka publish queue is full'));
    const promise = new Promise<void>((resolve, reject) => {
      this.queue.push({ topic, key, value, bytes, resolve, reject });
      this.queuedBytes += bytes;
    });
    this.options.queued?.(this.queue.length);
    this.schedule();
    return promise;
  }

  private schedule() {
    if (this.active || !this.queue.length) return;
    if (
      this.closing ||
      this.queue.length >= this.options.maxMessages ||
      this.options.lingerMs === 0
    ) {
      clearTimeout(this.timer);
      this.timer = undefined;
      queueMicrotask(() => void this.pump());
    } else
      this.timer ??= setTimeout(() => void this.pump(), this.options.lingerMs);
  }

  private pump(): Promise<void> {
    if (this.active) return this.active;
    clearTimeout(this.timer);
    this.timer = undefined;
    if (!this.queue.length) return Promise.resolve();
    const pending: Pending[] = [];
    let bytes = 0;
    while (this.queue.length && pending.length < this.options.maxMessages) {
      const next = this.queue[0]!;
      if (
        pending.length &&
        bytes + next.bytes > (this.options.maxBatchBytes ?? 512 * 1024)
      )
        break;
      pending.push(this.queue.shift()!);
      bytes += next.bytes;
    }
    this.queuedBytes -= bytes;
    this.options.queued?.(this.queue.length);
    this.active = this.publish(pending).finally(() => {
      this.active = undefined;
      this.schedule();
    });
    return this.active;
  }

  private async publish(pending: Pending[]) {
    try {
      const topics = new Map<Topic, { key: string; value: string }[]>();
      for (const item of pending) {
        if (!topics.has(item.topic)) topics.set(item.topic, []);
        topics.get(item.topic)!.push({ key: item.key, value: item.value });
      }
      for (const [topic, messages] of topics)
        this.options.batch?.(topic, messages.length);
      await this.producer.sendBatch({
        acks: -1,
        topicMessages: [...topics].map(([topic, messages]) => ({
          topic,
          messages,
        })),
      });
      for (const item of pending) item.resolve();
    } catch (error) {
      for (const item of pending) item.reject(error);
    }
  }

  async close() {
    this.closing = true;
    clearTimeout(this.timer);
    this.timer = undefined;
    while (this.active || this.queue.length) await this.pump();
  }
}
