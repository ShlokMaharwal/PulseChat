import { describe, expect, it, vi } from 'vitest';
import type { Admin } from 'kafkajs';
import { parseConfig } from '../../packages/shared/src/config.js';
import { ensureTopics, TOPICS } from '../../packages/shared/src/kafka.js';

const config = parseConfig({
  JWT_SECRET: 'test-secret-with-more-than-32-characters',
});
function fakeAdmin(partitions = 6, leader = 1) {
  return {
    createTopics: vi.fn(async () => false),
    fetchTopicMetadata: vi.fn(async () => ({
      topics: TOPICS.map((name) => ({
        name,
        partitions: Array.from({ length: partitions }, () => ({
          leader,
          replicas: [1],
        })),
      })),
    })),
  };
}
describe('topic bootstrap', () => {
  it.each(['createTopics', 'fetchTopicMetadata'] as const)(
    'retries transient %s failures during cluster convergence',
    async (operation) => {
      const admin = fakeAdmin();
      admin[operation].mockRejectedValueOnce(
        Object.assign(new Error('Metadata is propagating'), {
          retriable: true,
        }),
      );
      await ensureTopics(admin as unknown as Admin, config);
      expect(admin[operation]).toHaveBeenCalledTimes(2);
    },
  );
  it('does not retry a non-transient bootstrap failure', async () => {
    const admin = fakeAdmin();
    admin.createTopics.mockRejectedValueOnce(new Error('Access denied'));
    await expect(
      ensureTopics(admin as unknown as Admin, config),
    ).rejects.toThrow('Access denied');
    expect(admin.createTopics).toHaveBeenCalledOnce();
  });
  it('requests all six-partition topics and verifies existing topics', async () => {
    const admin = fakeAdmin();
    await ensureTopics(admin as unknown as Admin, config);
    expect(admin.createTopics).toHaveBeenCalledWith({
      waitForLeaders: true,
      topics: TOPICS.map((topic) => ({
        topic,
        numPartitions: 6,
        replicationFactor: 1,
      })),
    });
    expect(admin.fetchTopicMetadata).toHaveBeenCalledOnce();
  });
  it.each([
    [5, 1],
    [6, -1],
  ])(
    'rejects incompatible existing topics or missing leaders',
    async (partitions, leader) => {
      await expect(
        ensureTopics(fakeAdmin(partitions, leader) as unknown as Admin, config),
      ).rejects.toThrow('incorrect partition/replication');
    },
  );
});
