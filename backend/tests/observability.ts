import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile, rm } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import {
  createDependencies,
  createLogger,
  parseConfig,
} from '@pulsechat/shared';
import { TestClient } from './helpers/test-client.js';

const config = parseConfig({
  ...process.env,
  JWT_SECRET:
    process.env.JWT_SECRET ??
    'pulsechat-local-development-secret-change-before-deploying',
});
const dependencies = createDependencies(
  config,
  'observability',
  createLogger(config, 'observability'),
);
const base = process.env.TEST_HTTP_URL ?? 'http://localhost:8080';
const prometheus = process.env.TEST_PROMETHEUS_URL ?? 'http://localhost:9090';
const clients = [new TestClient(), new TestClient()];
const producer = dependencies.kafka.producer();
async function request<T>(
  path: string,
  body: unknown,
  token?: string,
): Promise<T> {
  const response = await fetch(base + '/api' + path, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(45000),
  });
  if (!response.ok) throw new Error(`HTTP ${response.status} ${path}`);
  return (await response.json()) as T;
}
async function query(expression: string) {
  const response = await fetch(
    `${prometheus}/api/v1/query?query=${encodeURIComponent(expression)}`,
    { signal: AbortSignal.timeout(15000) },
  );
  if (!response.ok) throw new Error(`Prometheus ${response.status}`);
  const body = (await response.json()) as {
    status: string;
    data: { result: { value: [number, string] }[] };
  };
  assert.equal(body.status, 'success');
  return Number(body.data.result[0]?.value[1] ?? 0);
}
try {
  await rm('.cache/observability.stop', { force: true });
  await dependencies.connect();
  await producer.connect();
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const a = await request<{ token: string; user: { id: string } }>(
    '/auth/register',
    { username: `metrics_a_${suffix}`, password: 'Metrics-proof-password!' },
  );
  const b = await request<{ token: string; user: { id: string } }>(
    '/auth/register',
    { username: `metrics_b_${suffix}`, password: 'Metrics-proof-password!' },
  );
  const { chat } = await request<{ chat: { id: string } }>(
    '/chats',
    { userId: b.user.id },
    a.token,
  );
  await clients[0]!.connect(base.replace(/^http/, 'ws') + '/ws', a.token);
  await clients[1]!.connect(base.replace(/^http/, 'ws') + '/ws', b.token);
  const firstId = randomUUID();
  for (let i = 0; i < 80; i++)
    await clients[0]!.message(
      chat.id,
      `observability-${i}`,
      i === 0 ? firstId : randomUUID(),
    );
  await clients[1]!.waitForSeq(chat.id, 80);
  await clients[0]!.waitFor(
    (frame) =>
      frame.type === 'receipt' &&
      frame.chatId === chat.id &&
      frame.status === 'delivered' &&
      frame.seq === 80,
  );
  clients[1]!.read(chat.id);
  for (let i = 0; i < 4; i++)
    await clients[0]!.message(chat.id, 'observability-0', firstId);
  await producer.send({
    topic: 'messages',
    acks: -1,
    messages: [{ key: chat.id, value: `intentional-metrics-poison-${suffix}` }],
  });
  await dependencies.redis.hset(
    `rate:${a.user.id}`,
    'tokens',
    0,
    'updated',
    Date.now() + 60000,
  );
  const limited = randomUUID();
  clients[0]!.send({
    type: 'message',
    chatId: chat.id,
    body: 'intentional rejection',
    clientMsgId: limited,
  });
  await clients[0]!.waitFor(
    (frame) =>
      frame.type === 'error' &&
      frame.code === 'RATE_LIMITED' &&
      frame.clientMsgId === limited,
  );
  await dependencies.redis.del(`rate:${a.user.id}`);
  const expressions = {
    connections: 'sum(pulsechat_active_connections{service="gateway"})',
    ingress: 'sum(rate(pulsechat_messages_in_total{service="gateway"}[2m]))',
    egress: 'sum(rate(pulsechat_messages_out_total{service="gateway"}[2m]))',
    latency:
      'histogram_quantile(0.95,sum by(le)(rate(pulsechat_delivery_latency_seconds_bucket{service="processor"}[5m])))',
    lagPeak:
      'sum(max by(topic,partition)(pulsechat_consumer_lag_peak{service="processor"}))',
    dedupe: 'sum(pulsechat_dedupe_hits_total{service="processor"})',
    dlq: 'sum(pulsechat_dlq_total{service="processor"})',
    rateRejections:
      'sum(pulsechat_rate_limit_rejections_total{service="gateway"})',
  };
  let snapshot: Record<string, number> = {};
  const deadline = Date.now() + 120000;
  let seq = 80;
  do {
    await clients[0]!.message(chat.id, `metric-stream-${++seq}`);
    await clients[1]!.waitForSeq(chat.id, seq);
    snapshot = Object.fromEntries(
      await Promise.all(
        Object.entries(expressions).map(async ([name, expression]) => [
          name,
          await query(expression),
        ]),
      ),
    );
    if (
      Object.values(snapshot).every(
        (value) => Number.isFinite(value) && value > 0,
      )
    )
      break;
    await delay(1000);
  } while (Date.now() < deadline);
  assert.ok(
    Object.values(snapshot).every(
      (value) => Number.isFinite(value) && value > 0,
    ),
    JSON.stringify(snapshot),
  );
  await mkdir('.cache', { recursive: true });
  await writeFile(
    '.cache/observability.json',
    JSON.stringify(
      {
        result: 'PASS',
        capturedAt: new Date().toISOString(),
        snapshot,
        expressions,
      },
      null,
      2,
    ) + '\n',
  );
  console.log(
    JSON.stringify({
      result: 'PASS',
      snapshot,
      dashboard: 'http://localhost:3000/d/pulsechat-overview',
      holdingConnections: true,
    }),
  );
  const holdUntil =
    Date.now() + Number(process.env.OBSERVABILITY_HOLD_MS ?? 300000);
  while (Date.now() < holdUntil && !existsSync('.cache/observability.stop')) {
    await clients[0]!.message(chat.id, `metric-stream-${++seq}`);
    await delay(1000);
  }
} finally {
  await Promise.all(clients.map((client) => client.close()));
  await producer.disconnect();
  await dependencies.close();
}
