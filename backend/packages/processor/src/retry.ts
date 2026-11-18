import { setTimeout as delay } from 'node:timers/promises';

export async function retryOperation(options: {
  operation: () => Promise<void>;
  healthy: () => Promise<boolean>;
  running: () => boolean;
  heartbeat: () => Promise<void>;
  failed: (error: unknown, attempt: number) => void;
  maxAttempts: number;
  pause?: (ms: number) => Promise<void>;
}): Promise<
  | { status: 'complete' }
  | { status: 'stopped' }
  | { status: 'failed'; error: unknown }
> {
  const pause = options.pause ?? delay;
  let attempts = 0;
  let failure: unknown;
  while (attempts < options.maxAttempts && options.running()) {
    try {
      await options.operation();
      return { status: 'complete' };
    } catch (error) {
      // Infrastructure outages are not evidence that this record is poison.
      // Retain partition ownership and its offset until dependencies recover.
      const healthy = await options.healthy().catch(() => false);
      if (!options.running()) return { status: 'stopped' };
      if (!healthy) {
        await options.heartbeat();
        await pause(1000);
        continue;
      }
      failure = error;
      options.failed(error, ++attempts);
      if (attempts < options.maxAttempts) {
        await pause(Math.min(100 * 2 ** (attempts - 1), 1600));
        await options.heartbeat();
      }
    }
  }
  return options.running()
    ? { status: 'failed', error: failure }
    : { status: 'stopped' };
}
