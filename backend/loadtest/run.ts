import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID, createHash } from 'node:crypto';
import { mkdir, readFile, writeFile, readdir, access } from 'node:fs/promises';
import { resolve, relative } from 'node:path';
import { gzipSync } from 'node:zlib';
import { setTimeout as pause } from 'node:timers/promises';
import { loadFixture } from './fixtures.js';
import { LoadAudit } from './audit.js';
import {
  hardware,
  resources,
  promSnapshot,
  profileSignal,
  collectProfiles,
} from './telemetry.js';
import { analyzeRun } from './report.js';
const exec = promisify(execFile);
const base = process.env.TEST_HTTP_URL ?? 'http://localhost:8080';
const target = Number(process.argv[2] ?? 200);
if (
  !Number.isInteger(target) ||
  target < 2 ||
  target > 5000 ||
  target % 2 !== 0
)
  throw new Error('Target must be an even number from 2 to 5000');
const integer = (name: string, fallback: number, min = 0) => {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value < min)
    throw new Error('Invalid ' + name);
  return value;
};
const config = {
  rampMs: integer('RAMP_MS', 10000),
  warmupMs: integer('WARMUP_MS', 5000),
  steadyMs: integer('STEADY_MS', 30000, 1000),
  drainMs: integer('DRAIN_MS', 30000, 1000),
  messageIntervalMs: integer('MESSAGE_INTERVAL_MS', 1000, 1),
  socketsPerVu: integer('SOCKETS_PER_VU', 50, 1),
  readReceipts: process.env.READ_RECEIPTS !== 'false',
  cpuProfile: process.env.PROFILE_CPU === 'true',
  fixtureMode: process.env.LOAD_FIXTURE
    ? 'reuse'
    : (process.env.LOAD_FIXTURE_MODE ?? 'http'),
};
const runId = randomUUID().replaceAll('-', '');
const output = resolve(
  '.cache/loadtest/' +
    new Date().toISOString().replaceAll(/[:.]/g, '-') +
    '-' +
    target,
);
await mkdir(output, { recursive: true });
const root = resolve('..');
async function sourceSnapshot() {
  const files: string[] = [];
  async function walk(directory: string) {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      if (['node_modules', 'dist', '.cache'].includes(entry.name)) continue;
      const file = resolve(directory, entry.name);
      if (entry.isDirectory()) await walk(file);
      else if (/\.(ts|js|json)$/.test(file)) files.push(file);
    }
  }
  await walk(resolve('packages'));
  await walk(resolve('loadtest'));
  files.push(
    ...[
      'package.json',
      'package-lock.json',
      'tsconfig.base.json',
      'tsconfig.json',
      'Dockerfile',
      '../docker-compose.yml',
      '../docker-compose.performance.yml',
      '../infra/nginx/nginx.conf',
    ].map((file) => resolve(file)),
  );
  const snapshot: Record<string, string> = {};
  for (const file of files.sort())
    snapshot[relative(root, file).replaceAll('\\', '/')] = await readFile(
      file,
      'utf8',
    );
  const raw = JSON.stringify(snapshot);
  await writeFile(resolve(output, 'source-snapshot.json.gz'), gzipSync(raw));
  const revision = (
    await exec('git', [
      '-c',
      'safe.directory=' + root.replaceAll('\\', '/'),
      'rev-parse',
      'HEAD',
    ])
  ).stdout.trim();
  return {
    revision,
    sourceSha256: createHash('sha256').update(raw).digest('hex'),
  };
}
const source = await sourceSnapshot();
const fixture = await loadFixture(
  base,
  target,
  resolve(output, 'fixture.json'),
  process.env.LOAD_FIXTURE,
);
const machine = await hardware(output);
await writeFile(
  resolve(output, 'run.json'),
  JSON.stringify(
    {
      runId,
      ...source,
      targetConnections: target,
      config,
      hardware: machine,
      initialCursors: Object.fromEntries(
        fixture.accounts.map((account) => [
          account.chatId,
          account.initialSeq ?? 0,
        ]),
      ),
    },
    null,
    2,
  ) + '\n',
);
const image = process.env.K6_IMAGE ?? 'grafana/k6:2.3.0';
try {
  await exec('docker', ['image', 'inspect', image], { maxBuffer: 1024 * 1024 });
} catch {
  await exec('docker', ['pull', image], {
    timeout: 180000,
    maxBuffer: 1024 * 1024,
  });
}
const audit = new LoadAudit(output, runId);
let stopResources: ReturnType<typeof resources> | undefined;
let profileActive = false;
let profileError: string | undefined;
let exitCode = 1;
let summaryMissing = false;
const container = 'pulsechat-k6-' + target + '-' + runId.slice(0, 8);
let containerCreated = false;
let resultsCopied = false;
try {
  await audit.start();
  const beforeDrain = await audit.drain(
    integer('SETTLE_TIMEOUT_MS', 180000, 1000),
  );
  await writeFile(
    resolve(output, 'before-drain.json'),
    JSON.stringify(beforeDrain, null, 2) + '\n',
  );
  if (!beforeDrain.drained)
    throw new Error(
      'Cannot start load until the previous workload and audit have settled',
    );
  const env = {
    TARGET: String(target),
    RUN_ID: runId,
    RAMP_MS: String(config.rampMs),
    WARMUP_MS: String(config.warmupMs),
    STEADY_MS: String(config.steadyMs),
    DRAIN_MS: String(config.drainMs),
    MESSAGE_INTERVAL_MS: String(config.messageIntervalMs),
    SOCKETS_PER_VU: String(config.socketsPerVu),
    READ_RECEIPTS: String(config.readReceipts),
    FIXTURE_PATH: '/control/fixture.json',
  };
  const args = [
    'run',
    '--name',
    container,
    '--network',
    'pulsechat_default',
    '--memory',
    '640m',
    '--user',
    '0:0',
    '--entrypoint',
    '/bin/sh',
    ...Object.entries(env).flatMap(([name, value]) => [
      '-e',
      name + '=' + value,
    ]),
    '-v',
    resolve('loadtest') + ':/scripts:ro',
    '-v',
    output + ':/control',
    '-v',
    '/results',
    image,
    '-c',
    'touch /control/container-ready; while [ ! -f /control/start ]; do sleep 0.1; done; exec k6 run --quiet --log-format raw --console-output /results/events.jsonl /scripts/chat.js',
  ];
  let generatorExit: number | undefined;
  const generator = new Promise<number>((resolveCode) => {
    const child = spawn('docker', args, {
      stdio: 'inherit',
      windowsHide: true,
    });
    const deadline = setTimeout(
      () => {
        void exec('docker', ['kill', container], { timeout: 15000 }).catch(() =>
          child.kill(),
        );
      },
      config.rampMs +
        config.warmupMs +
        config.steadyMs +
        config.drainMs +
        360000,
    );
    child.once('error', () => {
      generatorExit = 1;
      clearTimeout(deadline);
      resolveCode(1);
    });
    child.once('close', (code) => {
      generatorExit = code ?? 1;
      clearTimeout(deadline);
      resolveCode(generatorExit);
    });
  });
  containerCreated = true;
  const readyUntil = Date.now() + 120000;
  for (;;) {
    if (generatorExit !== undefined)
      throw new Error('Load container exited before sampling began');
    if (
      await access(resolve(output, 'container-ready')).then(
        () => true,
        () => false,
      )
    )
      break;
    if (Date.now() >= readyUntil)
      throw new Error('Load container did not become ready');
    await pause(250);
  }
  stopResources = resources(
    output,
    [
      ...machine.containers.map((item) => item.name.replace(/^\//, '')),
      container,
    ],
    container,
  );
  await stopResources.ready;
  const before = await promSnapshot();
  if (config.cpuProfile) {
    await profileSignal('start');
    profileActive = true;
  }
  const startedAt = Date.now();
  await writeFile(resolve(output, 'start'), runId);
  console.log(
    JSON.stringify({
      event: 'load_started',
      runId,
      output,
      target,
      distinctUsers: fixture.accounts.length,
      chats: target / 2,
      config,
    }),
  );
  exitCode = await generator;
  const generatorEndedAt = Date.now();
  await exec('docker', ['cp', container + ':/results/.', output], {
    timeout: 60000,
  });
  resultsCopied = true;
  const drained = await audit.drain(integer('SETTLE_TIMEOUT_MS', 180000, 1000));
  const after = await promSnapshot();
  const endedAt = Date.now();
  if (profileActive) {
    await profileSignal('stop');
    profileActive = false;
    await pause(1000);
    await collectProfiles(output).catch((error) => {
      profileError = String(error);
    });
  }
  const runnerResources = await stopResources();
  stopResources = undefined;
  const summary = JSON.parse(
    await readFile(resolve(output, 'k6-summary.json'), 'utf8').catch(() => {
      summaryMissing = true;
      return '{"metrics":{}}';
    }),
  ) as { metrics: Record<string, { values: Record<string, number> }> };
  const telemetry: Record<string, unknown> = {};
  for (const [name, query] of Object.entries({
    connections: 'sum(pulsechat_active_connections{service="gateway"})',
    gatewayConnections: 'pulsechat_active_connections{service="gateway"}',
    cpu: 'sum(rate(pulsechat_process_cpu_seconds_total[15s]))',
    lag: 'sum(max by(topic,partition)(pulsechat_consumer_lag{service="processor"}))',
    eventLoop: 'pulsechat_nodejs_eventloop_lag_seconds',
  })) {
    const response = await fetch(
      'http://localhost:9090/api/v1/query_range?query=' +
        encodeURIComponent(query) +
        '&start=' +
        startedAt / 1000 +
        '&end=' +
        endedAt / 1000 +
        '&step=2',
      { signal: AbortSignal.timeout(30000) },
    );
    if (!response.ok) throw new Error('Prometheus ' + response.status);
    const data = (await response.json()) as { data: { result: unknown } };
    telemetry[name] = data.data.result;
  }
  const series = telemetry.connections as { values: [number, string][] }[];
  const measured = {
    runId,
    ...source,
    targetConnections: target,
    uniqueUsers: fixture.accounts.length,
    chats: target / 2,
    chatType: '1:1',
    config,
    startedAt: new Date(startedAt).toISOString(),
    generatorEndedAt: new Date(generatorEndedAt).toISOString(),
    endedAt: new Date(endedAt).toISOString(),
    k6ExitCode: exitCode,
    summaryMissing,
    sent: summary.metrics.messages_sent?.values,
    k6Metrics: summary.metrics,
    peakObservedConnections: Math.max(
      0,
      ...series.flatMap((s) => s.values.map(([, value]) => Number(value))),
    ),
    drain: drained,
    prometheusStages: { before, after },
    telemetry,
    hardware: machine,
    runnerResources,
    loadGeneratorMemoryLimitBytes: 640 * 1024 * 1024,
    generatorResultsFilesystem: 'Docker anonymous volume',
    profileError,
  };
  await writeFile(
    resolve(output, 'measured.json'),
    JSON.stringify(measured, null, 2) + '\n',
  );
  const report = await analyzeRun(output);
  console.log(
    JSON.stringify({
      event: 'load_result',
      output,
      classification: Object.fromEntries(
        Object.entries(report.classification).filter(
          ([, value]) => !Array.isArray(value),
        ),
      ),
      targetReached: report.targetReached,
      peakClientWelcomedConnections: report.peakClientWelcomedConnections,
      steadyMinimumConnections: report.steadyMinimumConnections,
      steadyTargetHeld: report.steadyTargetHeld,
      peakObservedConnections: report.peakObservedConnections,
      steadyPeerDeliveriesPerSecond: report.steadyPeerDeliveriesPerSecond,
      steadyCohortLatencyMs: report.steadyCohortLatencyMs,
      drained: drained.drained,
      ledgerComplete: report.ledgerComplete,
      resourceSamplingComplete: report.resourceSamplingComplete,
    }),
  );
  if (
    !report.ledgerComplete ||
    !report.resourceSamplingComplete ||
    !drained.drained ||
    report.classification.acceptedButUnpersisted ||
    report.unexpectedRecipients ||
    profileError
  )
    exitCode = 1;
  else if (
    (!report.targetReached || !report.steadyTargetHeld) &&
    exitCode === 0
  )
    exitCode = 99;
} catch (error) {
  exitCode = 1;
  await writeFile(
    resolve(output, 'failure.json'),
    JSON.stringify(
      { error: String(error), at: new Date().toISOString() },
      null,
      2,
    ) + '\n',
  );
  console.error(String(error));
} finally {
  if (containerCreated) {
    if (resultsCopied)
      await exec('docker', ['rm', '--volumes', '--force', container], {
        timeout: 30000,
      }).catch(() => undefined);
    else {
      await exec('docker', ['stop', '--time', '1', container], {
        timeout: 15000,
      }).catch(() => undefined);
      await writeFile(
        resolve(output, 'results-recovery.json'),
        JSON.stringify({
          container,
          command: 'docker cp ' + container + ':/results/. ' + output,
        }) + '\n',
      );
    }
  }
  if (profileActive) {
    await profileSignal('stop').catch(() => undefined);
    await pause(1000);
    await collectProfiles(output).catch(() => undefined);
  }
  if (stopResources) await stopResources().catch(() => undefined);
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      audit.close(),
      new Promise<never>((_, reject) => {
        timeout = setTimeout(
          () => reject(new Error('Audit shutdown timed out')),
          15000,
        );
      }),
    ]);
  } catch (error) {
    exitCode = 1;
    await writeFile(
      resolve(output, 'cleanup-error.json'),
      JSON.stringify({ error: String(error) }) + '\n',
    );
  } finally {
    clearTimeout(timeout);
  }
}
// A failed Kafka client can retain reconnect timers after its bounded shutdown.
process.exit(exitCode);
