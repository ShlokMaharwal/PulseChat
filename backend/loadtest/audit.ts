import { Kafka, logLevel, type Consumer, type Admin } from 'kafkajs';
import { createWriteStream } from 'node:fs';
import { once } from 'node:events';
import { finished } from 'node:stream/promises';
import { resolve } from 'node:path';
import { setTimeout as pause } from 'node:timers/promises';
export class LoadAudit {
  private readonly kafka = new Kafka({
    clientId: 'pulsechat-load-audit',
    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:19092').split(','),
    logLevel: logLevel.NOTHING,
    connectionTimeout: 10000,
    requestTimeout: 10000,
    retry: { retries: 1, initialRetryTime: 300, maxRetryTime: 1000 },
  });
  private readonly consumer: Consumer;
  readonly admin: Admin;
  private readonly consumed = new Map<number, bigint>();
  private heads: { partition: number; offset: string }[] = [];
  private restartNeeded = false;
  private joined = false;
  private closing = false;
  private readonly output;
  constructor(
    directory: string,
    private readonly runId: string,
  ) {
    this.output = createWriteStream(resolve(directory, 'kafka-events.jsonl'));
    this.consumer = this.kafka.consumer({
      groupId: 'load-audit-' + runId,
      sessionTimeout: 30000,
    });
    this.admin = this.kafka.admin();
    this.consumer.on(this.consumer.events.CRASH, ({ payload }) => {
      this.joined = false;
      if (!this.closing) this.restartNeeded = !payload.restart;
      console.log(
        JSON.stringify({
          event: 'load_audit_crash',
          error: String(payload.error),
          automaticRestart: payload.restart,
        }),
      );
    });
    this.consumer.on(this.consumer.events.GROUP_JOIN, () => {
      if (!this.closing) {
        this.joined = true;
        this.pinOffsets();
      }
    });
  }
  private pinOffsets() {
    // An observer may replay after a coordinator failure. The report deduplicates
    // by ID; replay is preferable to silently skipping uncommitted observations.
    for (const head of this.heads)
      this.consumer.seek({
        topic: 'messages',
        partition: head.partition,
        offset: head.offset,
      });
  }
  async start() {
    await this.admin.connect();
    const heads = await this.admin.fetchTopicOffsets('messages');
    this.heads = heads;
    for (const p of heads) this.consumed.set(p.partition, BigInt(p.offset));
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: 'messages', fromBeginning: false });
    await this.runConsumer();
  }
  private async runConsumer() {
    await this.consumer.run({
      autoCommitInterval: 1000,
      eachMessage: async ({ partition, message }) => {
        this.consumed.set(partition, BigInt(message.offset) + 1n);
        try {
          const record = JSON.parse(message.value?.toString() ?? 'null') as {
            clientMsgId?: string;
            messageId?: string;
            chatId?: string;
            body?: string;
          };
          if (
            record &&
            typeof record.body === 'string' &&
            record.body.startsWith('{"loadRun":"' + this.runId + '"')
          ) {
            const event = {
              event: 'kafka_accepted',
              clientMsgId: record.clientMsgId,
              messageId: record.messageId,
              chatId: record.chatId,
              partition,
              offset: message.offset,
            };
            if (!this.output.write(JSON.stringify(event) + '\n'))
              await once(this.output, 'drain');
          }
        } catch (error) {
          if (error instanceof SyntaxError) return;
          throw error;
        }
      },
    });
    // "latest" is resolved at fetch time, which can follow the first test sends
    // after a coordinator delay. Pin the captured boundary instead of skipping them.
    if (this.joined) this.pinOffsets();
  }
  async lag() {
    if (this.restartNeeded && !this.closing) {
      this.restartNeeded = false;
      try {
        await this.consumer.disconnect();
        await this.consumer.connect();
        await this.runConsumer();
      } catch (error) {
        this.restartNeeded = true;
        throw error;
      }
    }
    const result: Record<string, number> = {};
    const group = await this.admin.fetchOffsets({
      groupId: process.env.KAFKA_GROUP_ID ?? 'pulsechat-processors',
      topics: ['messages', 'receipts'],
    });
    for (const topic of group) {
      for (const p of await this.admin.fetchTopicOffsets(topic.topic)) {
        const offset = BigInt(
          topic.partitions.find((item) => item.partition === p.partition)
            ?.offset ?? '-1',
        );
        result[topic.topic + ':' + p.partition] = Number(
          BigInt(p.offset) - (offset < 0n ? 0n : offset),
        );
      }
    }
    const heads = await this.admin.fetchTopicOffsets('messages');
    const auditLag = heads.reduce(
      (sum, p) =>
        sum + Number(BigInt(p.offset) - (this.consumed.get(p.partition) ?? 0n)),
      0,
    );
    return {
      processorLag: Object.values(result).reduce((a, b) => a + b, 0),
      auditLag,
      partitions: result,
    };
  }
  async drain(deadlineMs = 180000) {
    const samples: unknown[] = [];
    const until = Date.now() + deadlineMs;
    let zeros = 0;
    while (Date.now() < until) {
      try {
        const lag = await this.lag();
        const response = await fetch(
          'http://localhost:9090/api/v1/query?query=' +
            encodeURIComponent(
              'pulsechat_inflight_operations{service="gateway"}[10s]',
            ),
          { signal: AbortSignal.timeout(10000) },
        );
        if (!response.ok)
          throw new Error('Prometheus drain check ' + response.status);
        const data = (await response.json()) as {
          data: { result: { values: [number, string][] }[] };
        };
        const latest = data.data.result.map((series) => series.values.at(-1)!);
        const inflight =
          latest.length === 3
            ? latest.reduce((total, [, value]) => total + Number(value), 0)
            : null;
        const sample = {
          at: new Date().toISOString(),
          ...lag,
          auditJoined: this.joined,
          inflight,
          reportingGateways: latest.length,
          oldestScrapeAt: latest.length
            ? new Date(
                Math.min(...latest.map(([at]) => at)) * 1000,
              ).toISOString()
            : null,
        };
        samples.push(sample);
        if (samples.length === 1 || samples.length % 10 === 0)
          console.log(JSON.stringify({ event: 'load_drain', ...sample }));
        if (
          lag.processorLag === 0 &&
          lag.auditLag === 0 &&
          inflight === 0 &&
          this.joined
        )
          zeros++;
        else zeros = 0;
        if (zeros >= 3 && samples.length >= 6) {
          await this.flush();
          return { drained: true, samples };
        }
      } catch (error) {
        zeros = 0;
        const sample = { at: new Date().toISOString(), error: String(error) };
        samples.push(sample);
        console.log(JSON.stringify({ event: 'load_drain_error', ...sample }));
      }
      await pause(1000);
    }
    await this.flush();
    return { drained: false, samples };
  }
  private async flush() {
    await new Promise<void>((resolve, reject) =>
      this.output.write('', (error) => (error ? reject(error) : resolve())),
    );
  }
  async close() {
    this.closing = true;
    await this.consumer.disconnect();
    await this.admin.disconnect();
    const done = finished(this.output);
    this.output.end();
    await done;
  }
}
