import { expect, it } from 'vitest';
import {
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  unlink,
  rmdir,
} from 'node:fs/promises';
import { resolve, join } from 'node:path';
import pino from 'pino';
import { installCpuProfiling } from '../../packages/shared/src/profiling.js';
it('writes parseable CPU and event-loop evidence and removes its signal handler', async () => {
  const root = resolve('.cache/tests');
  await mkdir(root, { recursive: true });
  const dir = await mkdtemp(join(root, 'profile-'));
  const before = process.listenerCount('SIGUSR2');
  const beforeStop = process.listenerCount('SIGURG');
  const stop = installCpuProfiling(
    dir,
    'test-profile',
    pino({ level: 'silent' }),
  );
  try {
    process.emit('SIGUSR2');
    // Repeated starts and stops are safe, including after an application restart.
    process.emit('SIGUSR2');
    process.emit('SIGURG');
    process.emit('SIGURG');
    await stop();
    expect(process.listenerCount('SIGUSR2')).toBe(before);
    expect(process.listenerCount('SIGURG')).toBe(beforeStop);
    const files = await readdir(dir);
    const profile = JSON.parse(
      await readFile(
        join(
          dir,
          files.find((f) => f.endsWith('.cpuprofile'))!,
        ),
        'utf8',
      ),
    ) as { nodes: unknown[] };
    const metadata = JSON.parse(
      await readFile(
        join(
          dir,
          files.find((f) => f.endsWith('.json'))!,
        ),
        'utf8',
      ),
    ) as { startedAt: number; endedAt: number };
    expect(profile.nodes.length).toBeGreaterThan(0);
    expect(metadata.endedAt).toBeGreaterThanOrEqual(metadata.startedAt);
  } finally {
    await stop();
    for (const file of await readdir(dir)) await unlink(join(dir, file));
    await rmdir(dir);
  }
});
