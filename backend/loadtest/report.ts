import { readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { MongoClient } from 'mongodb';
export interface LoadEvent {
  event: string;
  at: number;
  index?: number;
  clientMsgId?: string;
  chatId?: string;
  steadyStartAt?: number;
  sendsEndAt?: number;
  startsAt?: number;
  closesAt?: number;
  trace?: Record<string, number>;
}
export function distribution(input: number[]) {
  const values = input.filter(Number.isFinite).sort((a, b) => a - b);
  if (!values.length)
    return {
      count: 0,
      p50: null,
      p95: null,
      p99: null,
      min: null,
      max: null,
      mean: null,
    };
  const quantile = (p: number) => {
    const at = (values.length - 1) * p;
    const lo = Math.floor(at);
    return values[lo]! + (values[Math.ceil(at)]! - values[lo]!) * (at - lo);
  };
  return {
    count: values.length,
    p50: quantile(0.5),
    p95: quantile(0.95),
    p99: quantile(0.99),
    min: values[0],
    max: values.at(-1),
    mean: values.reduce((a, b) => a + b, 0) / values.length,
  };
}
export async function readEvents(file: string): Promise<LoadEvent[]> {
  const text = await readFile(file, 'utf8');
  return text
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => {
      const value = JSON.parse(line) as LoadEvent & { msg?: string };
      return value.msg ? (JSON.parse(value.msg) as LoadEvent) : value;
    })
    .filter((event) => typeof event.event === 'string');
}
export function connectionCounts(
  events: LoadEvent[],
  steadyStart: number,
  sendsEnd: number,
) {
  const active = new Set<number>();
  let peak = 0,
    minimum: number | undefined;
  for (const event of events
    .filter((e) =>
      ['connected', 'disconnected', 'client_finished'].includes(e.event),
    )
    .sort((a, b) => a.at - b.at)) {
    if (event.at > steadyStart && minimum === undefined) minimum = active.size;
    if (event.event === 'connected') active.add(event.index!);
    else active.delete(event.index!);
    peak = Math.max(peak, active.size);
    if (event.at >= steadyStart && event.at < sendsEnd)
      minimum = Math.min(minimum ?? active.size, active.size);
  }
  return { peak, steadyMinimum: minimum ?? active.size };
}
export function classify(
  events: LoadEvent[],
  kafkaIds: Set<string>,
  persistedIds: Set<string>,
  settled = true,
) {
  const ids = (type: string) =>
    new Set(
      events
        .filter((event) => event.event === type && event.clientMsgId)
        .map((event) => event.clientMsgId!),
    );
  const issued = ids('issued'),
    acked = ids('received_ack'),
    delivered = ids('delivered');
  const pipelineAccepted = new Set([...kafkaIds, ...acked]);
  const acceptedMissing = [...pipelineAccepted].filter(
    (id) => !persistedIds.has(id),
  );
  const beforeAckFailed = [...issued].filter(
    (id) => !pipelineAccepted.has(id) && !persistedIds.has(id),
  );
  const persistedWithoutAck = [...persistedIds].filter((id) => !acked.has(id));
  const unexpected = [
    ...pipelineAccepted,
    ...persistedIds,
    ...delivered,
  ].filter((id) => !issued.has(id));
  return {
    issued: issued.size,
    receivedAcked: acked.size,
    kafkaAccepted: kafkaIds.size,
    persisted: persistedIds.size,
    peerDelivered: delivered.size,
    clientFailedBeforeAck: settled ? beforeAckFailed.length : 0,
    unresolvedBeforeAck: settled ? 0 : beforeAckFailed.length,
    persistedWithoutReceivedAck: persistedWithoutAck.length,
    acceptedButUnpersisted: acceptedMissing.length,
    unexpectedIds: [...new Set(unexpected)],
    acceptedButUnpersistedIds: acceptedMissing,
    clientFailedBeforeAckIds: settled ? beforeAckFailed : [],
    unresolvedBeforeAckIds: settled ? [] : beforeAckFailed,
  };
}
export async function analyzeRun(directory: string) {
  const file = resolve(directory, 'measured.json');
  const measured = JSON.parse(await readFile(file, 'utf8')) as Record<
    string,
    unknown
  > & {
    runId: string;
    targetConnections: number;
    peakObservedConnections: number;
    drain: { drained: boolean };
    sent?: { count: number };
  };
  const fixture = JSON.parse(
    await readFile(resolve(directory, 'fixture.json'), 'utf8'),
  ) as { accounts: { chatId: string; userId: string }[] };
  const events = await readEvents(resolve(directory, 'events.jsonl'));
  const kafkaEvents = await readEvents(
    resolve(directory, 'kafka-events.jsonl'),
  );
  const issued = new Map(
    events.filter((e) => e.event === 'issued').map((e) => [e.clientMsgId!, e]),
  );
  const acked = new Map(
    events
      .filter((e) => e.event === 'received_ack')
      .map((e) => [e.clientMsgId!, e]),
  );
  const deliveries = new Map<string, LoadEvent>();
  for (const event of events)
    if (
      event.event === 'delivered' &&
      event.clientMsgId &&
      !deliveries.has(event.clientMsgId)
    )
      deliveries.set(event.clientMsgId, event);
  const timing = events.find((e) => e.event === 'timing');
  if (!timing?.steadyStartAt || !timing.sendsEndAt)
    throw new Error('Missing k6 measurement-window timestamps');
  const resourceCpu: Record<string, number[]> = {};
  for (const line of (
    await readFile(resolve(directory, 'docker-stats.jsonl'), 'utf8')
  )
    .split(/\r?\n/)
    .filter(Boolean)) {
    const sample = JSON.parse(line) as {
      sampledAt: string;
      Name: string;
      CPUPerc: string;
    };
    const at = Date.parse(sample.sampledAt);
    if (at < timing.startsAt! || at > timing.closesAt!) continue;
    const name = sample.Name.startsWith('pulsechat-k6-')
      ? 'load-generator'
      : sample.Name;
    (resourceCpu[name] ??= []).push(Number.parseFloat(sample.CPUPerc));
  }
  const resourceCpuPercent = Object.fromEntries(
    Object.entries(resourceCpu).map(([name, samples]) => [
      name,
      distribution(samples),
    ]),
  );
  const resourceSamplingComplete = [
    'load-generator',
    ...[
      'gateway-1',
      'gateway-2',
      'gateway-3',
      'processor-1',
      'processor-2',
      'kafka',
      'redis',
      'mongo',
    ].map((role) => 'pulsechat-' + role + '-1'),
  ].every((name) => (resourceCpuPercent[name]?.count ?? 0) >= 2);
  const mongo = new MongoClient(
    process.env.MONGO_URL ?? 'mongodb://localhost:27018',
    { serverSelectionTimeoutMS: 10000 },
  );
  let records: { clientMsgId: string; chatId: string; seq: number }[];
  try {
    await mongo.connect();
    records = await mongo
      .db(process.env.MONGO_DB ?? 'pulsechat')
      .collection<{ clientMsgId: string; chatId: string; seq: number }>(
        'messages',
      )
      .find(
        {
          chatId: { $in: [...new Set(fixture.accounts.map((a) => a.chatId))] },
          body: { $regex: '^\\{"loadRun":"' + measured.runId + '"' },
        },
        { projection: { _id: 0, clientMsgId: 1, chatId: 1, seq: 1 } },
      )
      .toArray();
  } finally {
    await mongo.close();
  }
  const classification = classify(
    events,
    new Set(kafkaEvents.map((e) => e.clientMsgId!)),
    new Set(records.map((r) => r.clientMsgId)),
    measured.drain.drained,
  );
  const stages: Record<string, number[]> = {},
    invalid: Record<string, number> = {};
  const sample = (stage: string, start?: number, end?: number) => {
    if (start === undefined || end === undefined) return;
    const elapsed = end - start;
    if (!Number.isFinite(elapsed) || elapsed < 0) {
      invalid[stage] = (invalid[stage] ?? 0) + 1;
      return;
    }
    (stages[stage] ??= []).push(elapsed);
  };
  const overall: number[] = [],
    cohort: number[] = [];
  let overlap = 0,
    unexpectedRecipients = 0,
    steadyDeliveries = 0;
  for (const [id, event] of deliveries) {
    const origin = issued.get(id);
    if (!origin) continue;
    const elapsed = event.at - origin.at;
    if (elapsed >= 0) {
      overall.push(elapsed);
      if (origin.at >= timing.steadyStartAt && origin.at < timing.sendsEndAt)
        cohort.push(elapsed);
    }
    if (event.at >= timing.steadyStartAt && event.at < timing.sendsEndAt)
      steadyDeliveries++;
    if (event.index !== (origin.index! ^ 1)) unexpectedRecipients++;
    const t = event.trace;
    if (t) {
      sample('client_to_gateway', origin.at, t.gatewayReceivedAt);
      sample('gateway_prepare', t.gatewayReceivedAt, t.produceStartedAt);
      sample('kafka_to_processor', t.produceStartedAt, t.processorConsumedAt);
      sample('partition_queue', t.processorConsumedAt, t.processorStartedAt);
      sample('processor_prepare', t.processorStartedAt, t.sequenceStartedAt);
      sample('sequence_assign', t.sequenceStartedAt, t.sequenceAssignedAt);
      sample('mongo_persist', t.mongoPersistStartedAt, t.mongoPersistedAt);
      sample('persist_to_publish', t.mongoPersistedAt, t.redisPublishStartedAt);
      sample(
        'publish_to_gateway_push',
        t.redisPublishStartedAt,
        t.gatewayPushedAt,
      );
      sample('gateway_push_to_client', t.gatewayPushedAt, event.at);
      const ack = acked.get(id)?.trace;
      if (
        ack?.kafkaAcknowledgedAt !== undefined &&
        t.processorConsumedAt !== undefined &&
        ack.kafkaAcknowledgedAt > t.processorConsumedAt
      )
        overlap++;
    }
  }
  for (const event of acked.values()) {
    const t = event.trace;
    if (t)
      sample('kafka_produce_ack', t.produceStartedAt, t.kafkaAcknowledgedAt);
  }
  const connections = connectionCounts(
    events,
    timing.steadyStartAt,
    timing.sendsEndAt,
  );
  const ledgerComplete =
    measured.sent?.count === issued.size &&
    classification.unexpectedIds.length === 0;
  const steadyIssued = [...issued.values()].filter(
    (e) => e.at >= timing.steadyStartAt! && e.at < timing.sendsEndAt!,
  ).length;
  const summary = {
    ...measured,
    classification,
    ledgerComplete,
    resourceSamplingComplete,
    resourceCpuPercent,
    peakClientWelcomedConnections: connections.peak,
    targetReached: connections.peak >= measured.targetConnections,
    steadyMinimumConnections: connections.steadyMinimum,
    steadyTargetHeld: connections.steadyMinimum >= measured.targetConnections,
    deliveryLatencyMs: distribution(overall),
    steadyCohortLatencyMs: distribution(cohort),
    steadyCohortIssued: steadyIssued,
    steadyCohortUndelivered: steadyIssued - cohort.length,
    issuedNotPeerDelivered: [...issued.keys()].filter(
      (id) => !deliveries.has(id),
    ).length,
    receivedAckedNotPeerDelivered: [...acked.keys()].filter(
      (id) => !deliveries.has(id),
    ).length,
    steadyIssuedMessagesPerSecond:
      steadyIssued / ((timing.sendsEndAt - timing.steadyStartAt) / 1000),
    steadyPeerDeliveriesPerSecond:
      steadyDeliveries / ((timing.sendsEndAt - timing.steadyStartAt) / 1000),
    stageLatencyMs: Object.fromEntries(
      Object.entries(stages).map(([stage, values]) => [
        stage,
        distribution(values),
      ]),
    ),
    omittedNegativeOrInvalidStageSamples: invalid,
    produceAckConsumeOverlapSamples: overlap,
    unexpectedRecipients,
    persistenceObservedAt: new Date().toISOString(),
    timing,
  };
  await writeFile(file, JSON.stringify(summary, null, 2) + '\n');
  await writeFile(
    resolve(directory, 'message-outcomes.json'),
    JSON.stringify(records, null, 2) + '\n',
  );
  return summary;
}
