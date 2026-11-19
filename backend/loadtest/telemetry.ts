import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createWriteStream } from 'node:fs';
import { writeFile, mkdir, readdir, readFile } from 'node:fs/promises';
import { finished } from 'node:stream/promises';
import { resolve } from 'node:path';
import { cpus, totalmem, platform, release } from 'node:os';
import { monitorEventLoopDelay } from 'node:perf_hooks';
const execRaw = promisify(execFile);
const exec = (
  file: string,
  args: string[],
  options: { maxBuffer?: number } = {},
) =>
  execRaw(file, args, {
    timeout: 30000,
    maxBuffer: 16 * 1024 * 1024,
    ...options,
  });
export const roles = [
  'gateway-1',
  'gateway-2',
  'gateway-3',
  'processor-1',
  'processor-2',
];
export async function hardware(directory: string) {
  const info = JSON.parse(
    (await exec('docker', ['info', '--format', '{{json .}}'])).stdout,
  ) as {
    ServerVersion: string;
    NCPU: number;
    MemTotal: number;
    OperatingSystem: string;
    KernelVersion: string;
  };
  const names = (
    await exec('docker', [
      'ps',
      '--filter',
      'label=com.docker.compose.project=pulsechat',
      '--format',
      '{{.Names}}',
    ])
  ).stdout
    .trim()
    .split(/\r?\n/)
    .filter(Boolean);
  const containers = JSON.parse(
    (await exec('docker', ['inspect', ...names])).stdout,
  ) as {
    Name: string;
    Image: string;
    Config: { Env: string[] | null };
    HostConfig: {
      Memory: number;
      NanoCpus: number;
      CpuQuota: number;
      CpuPeriod: number;
      CpusetCpus: string;
    };
  }[];
  const measured = {
    host: {
      platform: platform(),
      release: release(),
      cpuModel: cpus()[0]?.model,
      logicalCpus: cpus().length,
      memoryBytes: totalmem(),
    },
    docker: {
      version: info.ServerVersion,
      cpus: info.NCPU,
      memoryBytes: info.MemTotal,
      os: info.OperatingSystem,
      kernel: info.KernelVersion,
    },
    containers: containers.map((c) => ({
      name: c.Name,
      image: c.Image,
      runtimeSettings: Object.fromEntries(
        [
          'NODE_ENV',
          'TRACE_MESSAGES',
          'KAFKA_PARTITIONS',
          'KAFKA_REPLICATION_FACTOR',
          'KAFKA_BATCH_MAX_MESSAGES',
          'KAFKA_BATCH_LINGER_MS',
          'PROCESSOR_PARTITION_CONCURRENCY',
          'KAFKA_BROKER_SESSION_TIMEOUT_MS',
        ].map((name) => [
          name,
          (c.Config.Env ?? [])
            .find((entry) => entry.startsWith(name + '='))
            ?.slice(name.length + 1),
        ]),
      ),
      limits: {
        memoryBytes: c.HostConfig.Memory,
        nanoCpus: c.HostConfig.NanoCpus,
        cpuQuota: c.HostConfig.CpuQuota,
        cpuPeriod: c.HostConfig.CpuPeriod,
        cpuset: c.HostConfig.CpusetCpus,
      },
    })),
  };
  await writeFile(
    resolve(directory, 'hardware.json'),
    JSON.stringify(measured, null, 2) + '\n',
  );
  return measured;
}
export function parseStatsLine(line: string) {
  // docker stats prefixes its first row with terminal clear-screen escapes.
  const start = line.indexOf('{');
  if (start < 0) return undefined;
  const sample = JSON.parse(line.slice(start)) as { Name: string };
  return typeof sample.Name === 'string' ? sample : undefined;
}
export function resources(
  directory: string,
  containers: string[] = [],
  expected?: string,
) {
  const file = createWriteStream(resolve(directory, 'docker-stats.jsonl'));
  const child = spawn(
    'docker',
    ['stats', '--format', '{{json .}}', ...containers],
    {
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  const closed = new Promise<void>((resolve) =>
    child.once('close', () => resolve()),
  );
  const started = Date.now();
  const cpu = process.cpuUsage();
  const lag = monitorEventLoopDelay({ resolution: 20 });
  lag.enable();
  let buffer = '',
    stderr = '';
  let failure: Error | undefined;
  let readyResolve: () => void;
  let readyReject: (error: Error) => void;
  const ready = new Promise<void>((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });
  const readyTimeout = setTimeout(
    () =>
      readyReject(
        new Error(
          'Docker stats did not provide its first sample within 180 seconds',
        ),
      ),
    180000,
  );
  child.on('error', (error) => {
    failure = error;
    clearTimeout(readyTimeout);
    readyReject(error);
  });
  file.on('error', (error) => (failure = error));
  child.stderr.on('data', (data) => (stderr += String(data)));
  child.stdout.on('data', (data) => {
    buffer += String(data);
    let newline: number;
    while ((newline = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, newline).trim();
      buffer = buffer.slice(newline + 1);
      const sample = parseStatsLine(line);
      if (!sample) continue;
      if (sample.Name.startsWith('pulsechat-')) {
        file.write(
          JSON.stringify({ sampledAt: new Date().toISOString(), ...sample }) +
            '\n',
        );
        if (!expected || sample.Name === expected) {
          clearTimeout(readyTimeout);
          readyResolve();
        }
      }
    }
  });
  const stop = async () => {
    clearTimeout(readyTimeout);
    if (process.platform === 'win32' && child.pid) {
      // Docker Desktop's docker.exe launches com.docker.cli.exe. Killing only
      // the wrapper leaves the stats stream and its inherited pipes running.
      await exec('taskkill', ['/PID', String(child.pid), '/T', '/F']);
    } else child.kill();
    await closed;
    lag.disable();
    const done = finished(file);
    file.end();
    await done;
    const milliseconds = (n: number) => (Number.isFinite(n) ? n / 1e6 : null);
    const measured = {
      startedAt: new Date(started).toISOString(),
      endedAt: new Date().toISOString(),
      elapsedMs: Date.now() - started,
      cpuMicroseconds: process.cpuUsage(cpu),
      eventLoopDelayMs: {
        resolution: 20,
        mean: milliseconds(lag.mean),
        p95: milliseconds(lag.percentile(95)),
        p99: milliseconds(lag.percentile(99)),
        max: milliseconds(lag.max),
      },
      stderr,
      error: failure?.message,
    };
    await writeFile(
      resolve(directory, 'runner-resources.json'),
      JSON.stringify(measured, null, 2) + '\n',
    );
    if (failure) throw failure;
    return measured;
  };
  return Object.assign(stop, { ready });
}
export async function promSnapshot() {
  const expressions = {
    stages: 'sum by(stage,le)(pulsechat_pipeline_stage_seconds_bucket)',
    stageCounts: 'sum by(stage)(pulsechat_pipeline_stage_seconds_count)',
    stageSums: 'sum by(stage)(pulsechat_pipeline_stage_seconds_sum)',
    skew: 'sum by(stage)(pulsechat_pipeline_clock_skew_total)',
    producerBatchCounts:
      'sum by(topic)(pulsechat_producer_batch_records_count{service="gateway"})',
    producerBatchRecords:
      'sum by(topic)(pulsechat_producer_batch_records_sum{service="gateway"})',
  };
  const result: Record<string, unknown> = {};
  for (const [name, query] of Object.entries(expressions)) {
    const response = await fetch(
      'http://localhost:9090/api/v1/query?query=' + encodeURIComponent(query),
      { signal: AbortSignal.timeout(30000) },
    );
    if (!response.ok) throw new Error('Prometheus ' + response.status);
    const data = (await response.json()) as { data: { result: unknown } };
    result[name] = data.data.result;
  }
  return result;
}
export async function profileSignal(action: 'start' | 'stop') {
  const containers = JSON.parse(
    (
      await exec('docker', [
        'inspect',
        ...roles.map((role) => 'pulsechat-' + role + '-1'),
      ])
    ).stdout,
  ) as { Name: string; Config: { Env: string[] } }[];
  for (const container of containers)
    if (
      !container.Config.Env.some(
        (value) =>
          value.startsWith('CPU_PROFILE_DIR=') && value !== 'CPU_PROFILE_DIR=',
      )
    )
      throw new Error(
        'CPU profiling is not enabled on ' +
          container.Name +
          '; use docker-compose.performance.yml',
      );
  for (const role of roles)
    await exec('docker', [
      'kill',
      '--signal',
      action === 'start' ? 'SIGUSR2' : 'SIGURG',
      'pulsechat-' + role + '-1',
    ]);
}
export async function collectProfiles(directory: string) {
  const target = resolve(directory, 'profiles');
  await mkdir(target, { recursive: true });
  for (const role of roles) {
    const folder = resolve(target, role);
    await mkdir(folder, { recursive: true });
    await exec('docker', [
      'cp',
      'pulsechat-' + role + '-1:/tmp/pulsechat-profiles/.',
      folder,
    ]);
  }
  const summaries: unknown[] = [];
  for (const role of roles) {
    const folder = resolve(target, role);
    const newest = (await readdir(folder))
      .filter((f) => f.endsWith('.cpuprofile'))
      .sort()
      .at(-1);
    for (const file of newest ? [newest] : []) {
      const profile = JSON.parse(
        await readFile(resolve(folder, file), 'utf8'),
      ) as {
        nodes: {
          id: number;
          callFrame: { functionName: string; url: string; lineNumber: number };
        }[];
        samples: number[];
        timeDeltas: number[];
      };
      const nodes = new Map(
        profile.nodes.map((node) => [node.id, node.callFrame]),
      );
      const totals = new Map<string, number>();
      for (let i = 0; i < profile.samples.length; i++) {
        const frame = nodes.get(profile.samples[i]!);
        if (!frame) continue;
        const key =
          frame.functionName + ' ' + frame.url + ':' + (frame.lineNumber + 1);
        totals.set(key, (totals.get(key) ?? 0) + (profile.timeDeltas[i] ?? 0));
      }
      summaries.push({
        role,
        file,
        sampledMicroseconds: profile.timeDeltas.reduce((a, b) => a + b, 0),
        topSelfTime: [...totals]
          .sort((a, b) => b[1] - a[1])
          .slice(0, 30)
          .map(([frame, microseconds]) => ({ frame, microseconds })),
      });
    }
  }
  await writeFile(
    resolve(directory, 'cpu-profile-summary.json'),
    JSON.stringify(summaries, null, 2) + '\n',
  );
  return summaries;
}
