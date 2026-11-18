import { Counter, Gauge, Histogram, type Registry } from 'prom-client';

/** Fixed labels only: user/chat/message identifiers belong in logs, not metrics. */
export function applicationMetrics(registry: Registry) {
  const registers = [registry];
  return {
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
