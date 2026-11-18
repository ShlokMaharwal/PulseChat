import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { inspectPersistence } from './analyze.js';
import { TestClient } from '../tests/helpers/test-client.js';
const exec = promisify(execFile);
const base = process.env.TEST_HTTP_URL ?? 'http://localhost:8080';
const target = Number(process.argv[2] ?? 2000);
if (!Number.isInteger(target) || target < 1 || target > 5000)
  throw new Error('Target must be 1–5000');
const output = resolve(
  `.cache/loadtest/${new Date().toISOString().replaceAll(/[:.]/g, '-')}-${target}`,
);
await mkdir(output, { recursive: true });
async function request<T>(
  path: string,
  body: unknown,
  token?: string,
): Promise<T> {
  const response = await fetch(`${base}/api${path}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(60000),
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}: ${path}`);
  return (await response.json()) as T;
}
const accounts: { token: string; userId: string; chatId: string }[] = [];
const suffix = randomUUID().replaceAll('-', '').slice(0, 8);
for (let i = 0; i < 16; i++) {
  const a = await request<{ token: string; user: { id: string } }>(
    '/auth/register',
    { username: `load_a_${i}_${suffix}`, password: 'Load-test-password!' },
  );
  const b = await request<{ token: string; user: { id: string } }>(
    '/auth/register',
    { username: `load_b_${i}_${suffix}`, password: 'Load-test-password!' },
  );
  const { chat } = await request<{ chat: { id: string } }>(
    '/chats',
    { userId: b.user.id },
    a.token,
  );
  accounts.push(
    { token: a.token, userId: a.user.id, chatId: chat.id },
    { token: b.token, userId: b.user.id, chatId: chat.id },
  );
}
// The Node recovery client verifies the load fixture through the public protocol first.
const preflight = [new TestClient(), new TestClient()];
try {
  await preflight[0]!.connect(
    base.replace(/^http/, 'ws') + '/ws',
    accounts[0]!.token,
  );
  await preflight[1]!.connect(
    base.replace(/^http/, 'ws') + '/ws',
    accounts[1]!.token,
  );
  await preflight[0]!.message(accounts[0]!.chatId, 'load fixture preflight');
  await preflight[1]!.waitForSeq(accounts[0]!.chatId, 1);
} finally {
  await Promise.all(preflight.map((client) => client.close()));
}
await writeFile(resolve(output, 'fixture.json'), JSON.stringify({ accounts }));
const image = process.env.K6_IMAGE ?? 'grafana/k6:2.3.0';
await exec('docker', ['pull', image], {
  timeout: 180000,
  maxBuffer: 1024 * 1024,
});
const startedAt = Date.now();
const container = `pulsechat-k6-${target}-${suffix}`;
const args = [
  'run',
  '--rm',
  '--name',
  container,
  '--network',
  'pulsechat_default',
  '--memory',
  '640m',
  '-e',
  `TARGET=${target}`,
  '-v',
  `${resolve('loadtest')}:/scripts:ro`,
  '-v',
  `${output}:/results`,
  image,
  'run',
  '--quiet',
  '/scripts/chat.js',
];
const exitCode = await new Promise<number>((resolveCode, reject) => {
  const child = spawn('docker', args, { stdio: 'inherit', windowsHide: true });
  child.once('error', reject);
  child.once('exit', (code) => resolveCode(code ?? 1));
});
const endedAt = Date.now();
let summaryMissing = false;
const summary = JSON.parse(
  await readFile(resolve(output, 'k6-summary.json'), 'utf8').catch(() => {
    summaryMissing = true;
    return '{"metrics":{}}';
  }),
) as { metrics: Record<string, { values: Record<string, number> }> };
const expressions = {
  connections: 'sum(pulsechat_active_connections{service="gateway"})',
  gatewayConnections: 'pulsechat_active_connections{service="gateway"}',
  cpu: 'sum(rate(pulsechat_process_cpu_seconds_total[30s]))',
  lag: 'sum(max by(topic,partition)(pulsechat_consumer_lag{service="processor"}))',
};
const telemetry: Record<string, unknown> = {};
for (const [name, expression] of Object.entries(expressions)) {
  const url = `http://localhost:9090/api/v1/query_range?query=${encodeURIComponent(expression)}&start=${startedAt / 1000}&end=${endedAt / 1000}&step=5`;
  const response = await fetch(url, { signal: AbortSignal.timeout(30000) });
  if (!response.ok) throw new Error(`Prometheus ${response.status}`);
  const data = (await response.json()) as {
    data: { result: { values: [number, string][] }[] };
  };
  telemetry[name] = data.data.result;
}
const connectionSeries = telemetry.connections as {
  values: [number, string][];
}[];
const peakConnections = Math.max(
  0,
  ...connectionSeries.flatMap((series) =>
    series.values.map(([, value]) => Number(value)),
  ),
);
const measured = {
  targetConnections: target,
  peakObservedConnections: peakConnections,
  observationIntervalSeconds: 5,
  uniqueUsers: accounts.length,
  chats: accounts.length / 2,
  messageIntervalMsPerSocket: 20000,
  startedAt: new Date(startedAt).toISOString(),
  endedAt: new Date(endedAt).toISOString(),
  dockerWallDurationMs: endedAt - startedAt,
  k6ExitCode: exitCode,
  summaryMissing,
  socketsPerVirtualUser: 50,
  loadGeneratorMemoryLimitMiB: 640,
  sent: summary.metrics.messages_sent?.values,
  confirmed: summary.metrics.messages_confirmed?.values,
  deliveryLatencyMs: summary.metrics.delivery_latency_ms?.values,
  socketErrors: summary.metrics.socket_errors?.values ?? null,
  rateRejections: summary.metrics.rate_rejections?.values ?? null,
  duplicateWireFrames: summary.metrics.duplicate_wire_frames?.values ?? null,
  orderingViolations: summary.metrics.ordering_violations?.values ?? null,
  telemetry,
};
await writeFile(
  resolve(output, 'measured.json'),
  JSON.stringify(measured, null, 2) + '\n',
);
console.log(JSON.stringify({ ...measured, telemetry: undefined, output }));
await inspectPersistence(output);
process.exitCode = exitCode;
