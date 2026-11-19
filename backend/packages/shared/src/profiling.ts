import { Session } from 'node:inspector/promises';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { Logger } from 'pino';
import { timestamp } from './tracing.js';

/** Operator opt-in, local inspector session; no debugger TCP listener is opened. */
export function installCpuProfiling(
  directory: string,
  name: string,
  logger: Logger,
) {
  let session: Session | undefined;
  let startedAt = 0;
  let cpu = process.cpuUsage();
  const lag = monitorEventLoopDelay({ resolution: 20 });
  let work = Promise.resolve();
  async function finish() {
    if (!session) return;
    const current = session;
    session = undefined;
    try {
      const endedAt = timestamp();
      const used = process.cpuUsage(cpu);
      const { profile } = await current.post('Profiler.stop');
      lag.disable();
      const file = join(
        directory,
        name.replace(/[^a-zA-Z0-9_.-]/g, '_') + '-' + Math.floor(startedAt),
      );
      await mkdir(directory, { recursive: true });
      await writeFile(file + '.cpuprofile', JSON.stringify(profile));
      const milliseconds = (value: number) =>
        Number.isFinite(value) ? value / 1e6 : null;
      await writeFile(
        file + '.json',
        JSON.stringify(
          {
            startedAt,
            endedAt,
            cpuMicroseconds: used,
            eventLoopDelayMs: {
              mean: milliseconds(lag.mean),
              p95: milliseconds(lag.percentile(95)),
              p99: milliseconds(lag.percentile(99)),
              max: milliseconds(lag.max),
            },
            memory: process.memoryUsage(),
          },
          null,
          2,
        ) + '\n',
      );
      logger.info({ file }, 'CPU profile saved');
    } finally {
      lag.disable();
      current.disconnect();
    }
  }
  async function start() {
    if (session) return;
    const current = new Session();
    current.connect();
    try {
      await current.post('Profiler.enable');
      await current.post('Profiler.setSamplingInterval', { interval: 10000 });
      lag.reset();
      lag.enable();
      cpu = process.cpuUsage();
      startedAt = timestamp();
      await current.post('Profiler.start');
      session = current;
      logger.info({ startedAt }, 'CPU profile started');
    } catch (error) {
      current.disconnect();
      throw error;
    }
  }
  const startSignal = () => {
    work = work
      .then(start)
      .catch((err) => logger.error({ err }, 'CPU profile failed'));
  };
  const stopSignal = () => {
    work = work
      .then(finish)
      .catch((err) => logger.error({ err }, 'CPU profile failed'));
  };
  if (directory) {
    process.on('SIGUSR2', startSignal);
    process.on('SIGURG', stopSignal);
  }
  return async () => {
    if (!directory) return;
    process.off('SIGUSR2', startSignal);
    process.off('SIGURG', stopSignal);
    await work;
    await finish();
  };
}
