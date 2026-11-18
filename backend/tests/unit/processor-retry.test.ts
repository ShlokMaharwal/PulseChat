import { expect, it, vi } from 'vitest';
import { retryOperation } from '../../packages/processor/src/retry.js';

it('retains a record through an outage longer than its poison retry budget', async () => {
  let failures = 0;
  const failed = vi.fn();
  const heartbeat = vi.fn(async () => undefined);
  const operation = vi.fn(async () => {
    if (failures++ < 12) throw new Error('Redis disconnected');
  });
  const result = retryOperation({
    operation,
    healthy: async () => false,
    running: () => true,
    heartbeat,
    failed,
    maxAttempts: 6,
    pause: async () => undefined,
  });
  await expect(result).resolves.toEqual({ status: 'complete' });
  expect(operation).toHaveBeenCalledTimes(13);
  expect(heartbeat).toHaveBeenCalledTimes(12);
  expect(failed).not.toHaveBeenCalled();
});

it('exhausts exactly six attempts for a poison record with healthy dependencies', async () => {
  const error = new Error('Invalid record');
  const failed = vi.fn();
  const operation = vi.fn(async () => {
    throw error;
  });
  const result = retryOperation({
    operation,
    healthy: async () => true,
    running: () => true,
    heartbeat: async () => undefined,
    failed,
    maxAttempts: 6,
    pause: async () => undefined,
  });
  await expect(result).resolves.toEqual({ status: 'failed', error });
  expect(operation).toHaveBeenCalledTimes(6);
  expect(failed).toHaveBeenLastCalledWith(error, 6);
});

it('leaves the record unresolved when partition ownership is lost during an outage', async () => {
  let running = true;
  const failed = vi.fn();
  const result = await retryOperation({
    operation: async () => {
      throw new Error('Disconnected');
    },
    healthy: async () => {
      running = false;
      return false;
    },
    running: () => running,
    heartbeat: async () => undefined,
    failed,
    maxAttempts: 6,
  });
  expect(result).toEqual({ status: 'stopped' });
  expect(failed).not.toHaveBeenCalled();
});
