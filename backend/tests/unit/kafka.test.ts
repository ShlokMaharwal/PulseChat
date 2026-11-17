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
