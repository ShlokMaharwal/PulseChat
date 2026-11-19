import type { Histogram, Counter } from 'prom-client';

export const PIPELINE_STAGES = [
  'gateway_prepare',
  'kafka_produce_ack',
  'kafka_to_processor',
  'partition_queue',
  'processor_prepare',
  'sequence_assign',
  'mongo_persist',
  'route_lookup',
  'redis_publish',
  'publish_to_gateway_push',
] as const;
export type PipelineStage = (typeof PIPELINE_STAGES)[number];
// Cross-process traces must use the current epoch clock, like the load client.
// timeOrigin + now retains the process-start wall-clock offset after an OS clock step.
export const timestamp = () => Date.now();

export function observeStage(
  metrics: {
    stage: Histogram<'stage'>;
    clockSkew: Counter<'stage'>;
  },
  stage: PipelineStage,
  start: number,
  end = timestamp(),
) {
  const seconds = (end - start) / 1000;
  if (!Number.isFinite(seconds) || seconds < 0) {
    metrics.clockSkew.inc({ stage });
    return;
  }
  metrics.stage.observe({ stage }, seconds);
}
