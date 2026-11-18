import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createHealthServer } from '../../packages/shared/src/health.js';
import type { DependencyHealth } from '../../packages/shared/src/dependencies.js';

const running: ReturnType<typeof createHealthServer>[] = [];
async function setup(
  check: () => Promise<DependencyHealth>,
  accepting = () => true,
  handleRequest?: Parameters<typeof createHealthServer>[0]['handleRequest'],
) {
  const onError = vi.fn();
  const health = createHealthServer({
    service: 'gateway',
    instanceId: 'test-1',
    check,
    accepting,
    onError,
    checkTimeoutMs: 1000,
    ...(handleRequest ? { handleRequest } : {}),
  });
  running.push(health);
  await new Promise<void>((resolve) =>
    health.server.listen(0, '127.0.0.1', resolve),
  );
  const base = `http://127.0.0.1:${(health.server.address() as AddressInfo).port}`;
  return { ...health, base, onError };
}
afterEach(async () => {
  await Promise.all(
    running.splice(0).map(async ({ server, registry }) => {
      registry.clear();
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
        server.closeAllConnections();
      });
    }),
  );
});

describe('health endpoints', () => {
  it('dispatches application POST routes while keeping health endpoints GET-only', async () => {
    const { base } = await setup(
      async () => ({ redis: true, mongo: true, kafka: true }),
      () => true,
      async (request, response) => {
        if (request.method !== 'POST' || request.url !== '/auth/register')
          return false;
        response.writeHead(201, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ accepted: true }));
        return true;
      },
    );
    const application = await fetch(`${base}/auth/register`, {
      method: 'POST',
    });
    expect(application.status).toBe(201);
    expect(await application.json()).toEqual({ accepted: true });
    expect((await fetch(`${base}/healthz`, { method: 'POST' })).status).toBe(
      405,
    );
  });

  it('keeps liveness independent of dependency outages', async () => {
    const check = vi.fn(async () => ({
      redis: false,
      mongo: false,
      kafka: false,
    }));
    const { base } = await setup(check);
    expect((await fetch(`${base}/healthz`)).status).toBe(200);
    expect(check).not.toHaveBeenCalled();
    expect((await fetch(`${base}/readyz`)).status).toBe(503);
  });
  it('returns ready only when all dependencies are usable', async () => {
    const { base } = await setup(async () => ({
      redis: true,
      mongo: true,
      kafka: true,
    }));
    const response = await fetch(`${base}/readyz`);
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      status: 'ready',
      instanceId: 'test-1',
    });
  });
  it.each(['redis', 'mongo', 'kafka'] as const)(
    'fails readiness during a %s outage',
    async (dependency) => {
      const { base } = await setup(async () => ({
        redis: true,
        mongo: true,
        kafka: true,
        [dependency]: false,
      }));
      expect((await fetch(`${base}/readyz`)).status).toBe(503);
    },
  );
  it('disables readiness during startup and draining without probing', async () => {
    const check = vi.fn(async () => ({
      redis: true,
      mongo: true,
      kafka: true,
    }));
    const { base } = await setup(check, () => false);
    expect((await fetch(`${base}/readyz`)).status).toBe(503);
    expect(check).not.toHaveBeenCalled();
  });
  it('shares simultaneous probes and rechecks after an outage', async () => {
    let complete: ((value: DependencyHealth) => void) | undefined;
    const check = vi.fn(
      () =>
        new Promise<DependencyHealth>((resolve) => {
          complete = resolve;
        }),
    );
    const { base, server } = await setup(check);
    const received = vi.fn();
    server.on('request', received);
    const first = fetch(`${base}/readyz`);
    const second = fetch(`${base}/readyz`);
    await vi.waitFor(() => {
      expect(received).toHaveBeenCalledTimes(2);
      expect(check).toHaveBeenCalledOnce();
    });
    complete?.({ redis: false, mongo: true, kafka: true });
    expect((await first).status).toBe(503);
    expect((await second).status).toBe(503);
    check.mockImplementation(async () => ({
      redis: true,
      mongo: true,
      kafka: true,
    }));
    expect((await fetch(`${base}/readyz`)).status).toBe(200);
    expect(check).toHaveBeenCalledTimes(2);
  });
  it('handles rejected probes without leaking internal errors', async () => {
    const { base, onError } = await setup(async () => {
      throw new Error('secret connection string');
    });
    const response = await fetch(`${base}/readyz`);
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ error: 'UNAVAILABLE' });
    expect(onError).toHaveBeenCalledOnce();
  });

  it('bounds a hung dependency probe and still serves liveness', async () => {
    const { base, onError } = await setup(
      () => new Promise<DependencyHealth>(() => undefined),
    );
    expect((await fetch(`${base}/readyz`)).status).toBe(503);
    expect(onError).toHaveBeenCalledOnce();
    expect((await fetch(`${base}/healthz`)).status).toBe(200);
  });
  it('exposes labeled Prometheus metrics and rejects unknown routes/methods', async () => {
    const { base } = await setup(async () => ({
      redis: true,
      mongo: true,
      kafka: true,
    }));
    const metrics = await fetch(`${base}/metrics`);
    expect(await metrics.text()).toContain(
      'service="gateway",instance="test-1"',
    );
    expect((await fetch(`${base}/missing`)).status).toBe(404);
    const response = await fetch(`${base}/healthz`, { method: 'POST' });
    expect(response.status).toBe(405);
    expect(response.headers.get('allow')).toBe('GET');
  });
});
