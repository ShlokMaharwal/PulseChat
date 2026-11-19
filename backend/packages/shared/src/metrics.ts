import { Counter, Gauge, Histogram, type Registry } from 'prom-client';

/** Fixed labels only: user/chat/message identifiers belong in logs, not metrics. */
export function applicationMetrics(registry: Registry) {
  const registers = [registry];
  return {
    producerQueued: new Gauge({
      name: 'pulsechat_producer_queued_records',
      help: 'Records queued for the next durable Kafka produce batch',
      registers,
    }),
    producerBatch: new Histogram({
      name: 'pulsechat_producer_batch_records',
      help: 'Records per topic in each attempted durable Kafka batch',
      labelNames: ['topic'] as const,
      buckets: [1, 2, 4, 8, 16, 32, 64, 128, 256],
      registers,
    }),
    connections: new Gauge({
      name: 'pulsechat_active_connections',
      help: 'Open authenticated WebSocket connections',
      registers,
    }),
    incoming: new Counter({
      name: 'pulsechat_messages_in_total',
      help: 'Gateway message attempts or processor-consumed message records, including replay',
      registers,
    }),
    inflight: new Gauge({
      name: 'pulsechat_inflight_operations',
      help: 'Gateway WebSocket operations awaiting validation, persistence handoff, or response',
      registers,
    }),
    outgoing: new Counter({
      name: 'pulsechat_messages_out_total',
      help: 'Gateway canonical socket deliveries or newly persisted processor messages',
      registers,
    }),
    lag: new Gauge({
      name: 'pulsechat_consumer_lag',
      help: 'Group log-end minus committed offset; max across replicas before summing partitions',
      labelNames: ['topic', 'partition'] as const,
      registers,
    }),
    lagPeak: new Gauge({
      name: 'pulsechat_consumer_lag_peak',
      help: 'Largest observed pending record count per partition during this process lifetime',
      labelNames: ['topic', 'partition'] as const,
      registers,
    }),
    delivery: new Histogram({
      name: 'pulsechat_delivery_latency_seconds',
      help: 'Gateway acceptance to client delivery acknowledgment arrival, sampled at advancing cumulative receipts',
      buckets: [
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
      ],
      registers,
    }),
    stage: new Histogram({
      name: 'pulsechat_pipeline_stage_seconds',
      help: 'Pipeline stage duration; produce ACK and consume timing overlap and must not be summed',
      labelNames: ['stage'] as const,
      buckets: [
        0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
        1, 2.5, 5, 10, 30, 60, 120,
      ],
      registers,
    }),
    clockSkew: new Counter({
      name: 'pulsechat_pipeline_clock_skew_total',
      help: 'Nonfinite or negative cross-process stage samples omitted from latency histograms',
      labelNames: ['stage'] as const,
      registers,
    }),
    dedupe: new Counter({
      name: 'pulsechat_dedupe_hits_total',
      help: 'Replayed logical messages already present in MongoDB',
      registers,
    }),
    dlq: new Counter({
      name: 'pulsechat_dlq_total',
      help: 'Source records durably acknowledged by the dead-letter topic',
      registers,
    }),
    rateRejected: new Counter({
      name: 'pulsechat_rate_limit_rejections_total',
      help: 'WebSocket operations rejected by the shared per-user bucket',
      registers,
    }),
  };
}
