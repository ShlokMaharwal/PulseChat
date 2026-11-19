import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';
import kafkaJS, { Partitioners, AssignerProtocol } from 'kafkajs';
import {
  createDependencies,
  createLogger,
  parseConfig,
} from '@pulsechat/shared';
import { TestClient } from '../tests/helpers/test-client.js';

export type Scenario =
  'gateway' | 'processor' | 'redis' | 'kafka' | 'mongo' | 'redis-partition';
const exec = promisify(execFile);
const base = process.env.TEST_HTTP_URL ?? 'http://localhost:8080';
async function docker(...args: string[]) {
  return await exec('docker', ['compose', ...args], {
    cwd: '..',
    timeout: 180000,
    maxBuffer: 1024 * 1024,
  });
}
async function gatewayService(gatewayId: string) {
  const result = await exec('docker', [
    'inspect',
    '--format',
    '{{.Config.Hostname}} {{index .Config.Labels "com.docker.compose.service"}}',
    ...['gateway-1', 'gateway-2', 'gateway-3'].map(
      (name) => 'pulsechat-' + name + '-1',
    ),
  ]);
  const service = result.stdout
    .trim()
    .split(/\r?\n/)
    .map((line) => line.split(' '))
    .find(([id]) => id === gatewayId)?.[1];
  assert.ok(
    service && ['gateway-1', 'gateway-2', 'gateway-3'].includes(service),
  );
  return service;
}
async function request<T>(
  path: string,
  body: unknown,
  token?: string,
): Promise<T> {
  const response = await fetch(base + '/api' + path, {
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
async function until(
  test: () => Promise<boolean>,
  label: string,
  timeout = 180000,
) {
  const deadline = Date.now() + timeout;
  while (!(await test())) {
    if (Date.now() >= deadline) throw new Error(`Timed out: ${label}`);
    await delay(100);
  }
}

export async function runScenario(scenario: Scenario) {
  const config = parseConfig({
    ...process.env,
    JWT_SECRET:
      process.env.JWT_SECRET ??
      'pulsechat-local-development-secret-change-before-deploying',
  });
  const dependencies = createDependencies(
    config,
    `chaos-${scenario}`,
    createLogger(config, `chaos-${scenario}`),
  );
  const sender = new TestClient();
  const receiver = new TestClient();
  const issued = new Map<string, string>();
  let chatId = '';
  let restore: (() => Promise<unknown>) | undefined;
  let failure: unknown;
  let faultEvidence: Record<string, unknown> = {};
  const started = Date.now();
  const send = (index: number) => {
    const id = randomUUID();
    const body = `${scenario}-${index}`;
    issued.set(id, body);
    return sender.message(
      chatId,
      body,
      id,
      config.KAFKA_REPLICATION_FACTOR === 3 ? 180000 : 60000,
    );
  };
  try {
    await dependencies.connect();
    const suffix = randomUUID().replaceAll('-', '').slice(0, 10);
    const a = await request<{ token: string; user: { id: string } }>(
      '/auth/register',
      { username: `chaos_a_${suffix}`, password: 'Chaos-test-password!' },
    );
    const b = await request<{ token: string; user: { id: string } }>(
      '/auth/register',
      { username: `chaos_b_${suffix}`, password: 'Chaos-test-password!' },
    );
    const { chat } = await request<{ chat: { id: string } }>(
      '/chats',
      { userId: b.user.id },
      a.token,
    );
    chatId = chat.id;
    const welcome = await sender.connect(
      base.replace(/^http/, 'ws') + '/ws',
      a.token,
    );
    await receiver.connect(base.replace(/^http/, 'ws') + '/ws', b.token);
    for (let i = 0; i < 20; i++) await send(i);
    await receiver.waitForSeq(chatId, 20);
    if (scenario === 'processor') {
      const metadata = await dependencies.admin.fetchTopicMetadata({
        topics: ['messages'],
      });
      const topic = metadata.topics[0]!;
      const partition = Partitioners.DefaultPartitioner()({
        topic: 'messages',
        partitionMetadata: topic.partitions,
        message: { key: chatId, value: '' },
      });
      const group = (
        await dependencies.admin.describeGroups([config.KAFKA_GROUP_ID])
      ).groups[0]!;
      const owner = group.members.find((member) =>
        AssignerProtocol.MemberAssignment.decode(
          member.memberAssignment,
        )?.assignment.messages?.includes(partition),
      );
      assert.ok(owner, 'No processor owns the chat partition');
      const containers = JSON.parse(
        (
          await exec('docker', [
            'inspect',
            'pulsechat-processor-1-1',
            'pulsechat-processor-2-1',
          ])
        ).stdout,
      ) as { Config: { Hostname: string; Labels: Record<string, string> } }[];
      const service = containers.find(
        (container) =>
          owner.clientId === `processor-${container.Config.Hostname}`,
      )?.Config.Labels['com.docker.compose.service'];
      assert.ok(service === 'processor-1' || service === 'processor-2');
      restore = async () => {
        await docker('unpause', service).catch(() => undefined);
        await docker('start', service);
      };
      await docker('pause', service);
      await Promise.all(Array.from({ length: 100 }, (_, i) => send(i + 20)));
      await docker('unpause', service);
      await until(
        async () =>
          (await dependencies.db
            .collection('messages')
            .countDocuments({ chatId })) > 20,
        'processor begins queued batch',
      );
      await docker('kill', '-s', 'SIGKILL', service);
      const storedAtCrash = await dependencies.db
        .collection('messages')
        .countDocuments({ chatId });
      assert.ok(
        storedAtCrash > 20 && storedAtCrash < 120,
        `Did not interrupt an active partial batch: ${storedAtCrash}`,
      );
      faultEvidence = {
        service,
        partition,
        storedAtCrash,
        queuedAtCrash: 120 - storedAtCrash,
      };
      await docker('start', service);
    } else {
      let interrupt: Promise<unknown>;
      if (scenario === 'gateway') {
        assert.equal(welcome.type, 'welcome');
        const containers = JSON.parse(
          (
            await exec('docker', [
              'inspect',
              'pulsechat-gateway-1-1',
              'pulsechat-gateway-2-1',
              'pulsechat-gateway-3-1',
            ])
          ).stdout,
        ) as { Config: { Hostname: string; Labels: Record<string, string> } }[];
        const service = containers.find(
          (container) =>
            welcome.type === 'welcome' &&
            container.Config.Hostname === welcome.gatewayId,
        )?.Config.Labels['com.docker.compose.service'];
        assert.ok(
          service && ['gateway-1', 'gateway-2', 'gateway-3'].includes(service),
        );
        restore = () => docker('start', service);
        interrupt = (async () => {
          await docker('kill', '-s', 'SIGKILL', service);
          await delay(4000);
          await docker('start', service);
        })();
        faultEvidence = {
          service,
          gatewayId: welcome.type === 'welcome' ? welcome.gatewayId : '',
        };
      } else if (scenario === 'redis') {
        restore = () => docker('start', 'redis');
        interrupt = (async () => {
          await docker('stop', 'redis');
          const first = receiver.frames.length,
            unavailableAt = Date.now();
          receiver.send({ type: 'heartbeat' });
          const error = await receiver.waitFor(
            (frame) =>
              receiver.frames.indexOf(frame) >= first &&
              frame.type === 'error' &&
              frame.code === 'UNAVAILABLE',
            config.DEPENDENCY_TIMEOUT_MS + 15000,
          );
          assert.ok(
            error.type === 'error' && /unavailable|retry/i.test(error.message),
          );
          faultEvidence = {
            ...faultEvidence,
            degradedError: error.code,
            degradedResponseMs: Date.now() - unavailableAt,
          };
          await delay(12000);
          await docker('start', 'redis');
        })();
        faultEvidence = { minimumOutageMs: 12000 };
      } else if (scenario === 'mongo') {
        restore = () => docker('start', 'mongo');
        interrupt = (async () => {
          await docker('stop', 'mongo');
          await delay(8000);
          await docker('start', 'mongo');
        })();
        faultEvidence = { minimumOutageMs: 8000 };
      } else if (scenario === 'redis-partition') {
        assert.ok(welcome.type === 'welcome');
        const service = await gatewayService(welcome.gatewayId);
        const container = 'pulsechat-' + service + '-1';
        const redisNetworks = JSON.parse(
          (
            await exec('docker', [
              'inspect',
              '--format',
              '{{json .NetworkSettings.Networks}}',
              'pulsechat-redis-1',
            ])
          ).stdout,
        ) as Record<string, unknown>;
        const network = Object.keys(redisNetworks)[0];
        assert.ok(
          network && Object.keys(redisNetworks).length === 1,
          'Redis must have its dedicated network',
        );
        const before = JSON.parse(
          (
            await exec('docker', [
              'inspect',
              '--format',
              '{{json .NetworkSettings.Networks}}',
              container,
            ])
          ).stdout,
        ) as Record<string, unknown>;
        assert.ok(
          before[network] && Object.keys(before).length > 1,
          'Gateway must retain its Kafka/Mongo network',
        );
        restore = async () => {
          const networks = JSON.parse(
            (
              await exec('docker', [
                'inspect',
                '--format',
                '{{json .NetworkSettings.Networks}}',
                container,
              ])
            ).stdout,
          ) as Record<string, unknown>;
          if (!networks[network])
            await exec('docker', ['network', 'connect', network, container]);
        };
        interrupt = (async () => {
          await exec('docker', ['network', 'disconnect', network, container]);
          await delay(12000);
          let dependenciesDuringPartition: unknown;
          await until(
            async () => {
              const result = await exec('docker', [
                'exec',
                container,
                'node',
                '-e',
                "fetch('http://127.0.0.1:8080/readyz').then(r=>r.json()).then(v=>console.log(JSON.stringify(v)))",
              ]);
              const health = JSON.parse(result.stdout) as {
                dependencies?: {
                  redis: boolean;
                  mongo: boolean;
                  kafka: boolean;
                };
              };
              dependenciesDuringPartition = health.dependencies;
              return (
                health.dependencies?.redis === false &&
                health.dependencies.mongo &&
                health.dependencies.kafka
              );
            },
            'only Redis becomes unavailable on the partitioned gateway',
            60000,
          );
          faultEvidence = { ...faultEvidence, dependenciesDuringPartition };
          await restore!();
        })();
        faultEvidence = {
          service,
          network,
          minimumPartitionMs: 12000,
          otherNetworkRetained: true,
        };
      } else if (config.KAFKA_REPLICATION_FACTOR === 3) {
        const metadata = await dependencies.admin.fetchTopicMetadata({
          topics: ['messages', 'receipts', 'messages.dlq'],
        });
        assert.ok(
          metadata.topics.every((topic) =>
            topic.partitions.every(
              (partition) =>
                partition.replicas.length === 3 && partition.isr.length === 3,
            ),
          ),
          'All replicas must be in sync before the fault',
        );
        const topic = metadata.topics.find(
          (value) => value.name === 'messages',
        )!;
        const partition = Partitioners.DefaultPartitioner()({
          topic: 'messages',
          partitionMetadata: topic.partitions,
          message: { key: chatId, value: '' },
        });
        const leader = topic.partitions.find(
          (value) => value.partitionId === partition,
        )!.leader;
        const service = leader === 1 ? 'kafka' : 'kafka-' + leader;
        assert.ok([1, 2, 3].includes(leader));
        const configs = await dependencies.admin.describeConfigs({
          includeSynonyms: false,
          resources: [
            {
              type: kafkaJS.ConfigResourceTypes.TOPIC,
              name: 'messages',
              configNames: ['min.insync.replicas'],
            },
          ],
        });
        assert.equal(
          configs.resources[0]?.configEntries.find(
            (value) => value.configName === 'min.insync.replicas',
          )?.configValue,
          '2',
        );
        const container = 'pulsechat-' + service + '-1';
        const policy = (
          await exec('docker', [
            'inspect',
            '--format',
            '{{.HostConfig.RestartPolicy.Name}}',
            container,
          ])
        ).stdout.trim();
        restore = async () => {
          try {
            await docker('start', service);
          } finally {
            await exec('docker', ['update', '--restart=' + policy, container]);
          }
        };
        interrupt = (async () => {
          await exec('docker', ['update', '--restart=no', container]);
          await docker('kill', '-s', 'SIGKILL', service);
          assert.equal(
            (
              await exec('docker', [
                'inspect',
                '--format',
                '{{.State.Running}}',
                container,
              ])
            ).stdout.trim(),
            'false',
          );
          const killedAt = Date.now();
          await delay(8000);
          await until(
            async () => {
              const current = (
                await dependencies.admin.fetchTopicMetadata({
                  topics: ['messages'],
                })
              ).topics[0]!.partitions.find(
                (value) => value.partitionId === partition,
              )!;
              if (
                current.leader < 0 ||
                current.leader === leader ||
                current.isr.length < 2
              )
                return false;
              faultEvidence = {
                ...faultEvidence,
                newLeader: current.leader,
                inSyncReplicasDuringFault: current.isr,
                heldDeadMs: Date.now() - killedAt,
              };
              return true;
            },
            'HA partition elects a new leader with two in-sync replicas',
            120000,
          );
          await restore!();
        })();
        faultEvidence = {
          mode: 'ha',
          replicationFactor: 3,
          minInSyncReplicas: 2,
          partition,
          previousLeader: leader,
          killedService: service,
          signal: 'SIGKILL',
        };
      } else {
        restore = async () => {
          const state = await exec('docker', [
            'inspect',
            '--format',
            '{{.State.Paused}}',
            'pulsechat-kafka-1',
          ]);
          if (state.stdout.trim() === 'true') await docker('unpause', 'kafka');
        };
        interrupt = (async () => {
          await docker('pause', 'kafka');
          await delay(8000);
          await docker('unpause', 'kafka');
        })();
        faultEvidence = { pauseMs: 8000 };
      }
      const sends = Promise.all(
        Array.from({ length: 100 }, (_, i) => send(i + 20)),
      );
      const outcomes = await Promise.allSettled([interrupt, sends]);
      const rejected = outcomes.find(
        (outcome) => outcome.status === 'rejected',
      );
      if (rejected?.status === 'rejected') throw rejected.reason;
    }
    await receiver.waitForSeq(chatId, 120, 180000);
    if (scenario === 'kafka' && config.KAFKA_REPLICATION_FACTOR === 3) {
      await until(
        async () => {
          const metadata = await dependencies.admin.fetchTopicMetadata({
            topics: ['messages', 'receipts', 'messages.dlq'],
          });
          return metadata.topics.every((topic) =>
            topic.partitions.every((partition) => partition.isr.length === 3),
          );
        },
        'all three replicas catch up after broker restart',
        180000,
      );
      faultEvidence = { ...faultEvidence, allReplicasRestored: true };
    }
    if (scenario === 'redis') {
      await dependencies.redis.del(
        `chat:${chatId}:seq`,
        `chat:${chatId}:msgseq`,
      );
      for (let i = 120; i < 125; i++) await send(i);
      await receiver.waitForSeq(chatId, 125);
      faultEvidence = {
        ...faultEvidence,
        simulatedCounterLoss: true,
        reseededCounter: await dependencies.redis.get(`chat:${chatId}:seq`),
      };
      assert.equal(faultEvidence.reseededCounter, '125');
    }
  } catch (error) {
    failure = error;
  } finally {
    if (restore)
      await restore().catch((error: unknown) => {
        failure ??= error;
      });
    const stored = chatId
      ? await dependencies.db
          .collection('messages')
          .find({ chatId })
          .sort({ seq: 1 })
          .toArray()
          .catch(() => [])
      : [];
    const received = receiver.messages.filter(
      (message) => message.chatId === chatId,
    );
    const storedIds = new Set(
      stored.map((message) => message.clientMsgId as string),
    );
    const receivedIds = new Set(received.map((message) => message.clientMsgId));
    const duplicated = Math.max(
      stored.length - storedIds.size,
      received.length - receivedIds.size,
    );
    const lost = [...issued.keys()].filter(
      (id) => !storedIds.has(id) || !receivedIds.has(id),
    ).length;
    const outOfOrder = received.filter(
      (message, i) => i > 0 && message.seq <= received[i - 1]!.seq,
    ).length;
    const wire = receiver.frames.filter(
      (frame) => frame.type === 'message' && frame.chatId === chatId,
    );
    const wireDuplicates =
      wire.length -
      new Set(
        wire.map((frame) => (frame.type === 'message' ? frame.messageId : '')),
      ).size;
    try {
      assert.ok(issued.size >= 120);
      assert.equal(lost, 0);
      assert.equal(duplicated, 0);
      assert.equal(outOfOrder, 0);
      assert.deepEqual(
        stored.map((message) => message.seq),
        Array.from({ length: issued.size }, (_, i) => i + 1),
      );
      assert.deepEqual(
        received.map((message) => message.seq),
        stored.map((message) => message.seq),
      );
      for (const message of stored)
        assert.equal(message.body, issued.get(message.clientMsgId as string));
    } catch (error) {
      failure ??= error;
    }
    const summary = {
      scenario,
      result: failure ? 'FAIL' : 'PASS',
      sent: issued.size,
      stored: stored.length,
      received: received.length,
      duplicated,
      wireDuplicates,
      lost,
      outOfOrder,
      durationMs: Date.now() - started,
      faultEvidence,
      ...(failure
        ? {
            error: failure instanceof Error ? failure.message : String(failure),
          }
        : {}),
    };
    console.log(JSON.stringify(summary));
    await mkdir('.cache/chaos', { recursive: true });
    await writeFile(
      `.cache/chaos/${scenario}.json`,
      JSON.stringify(summary, null, 2) + '\n',
    );
    await Promise.all([sender.close(), receiver.close()]);
    await dependencies.close();
  }
  if (failure) throw failure;
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const selected = process.argv[2] ?? 'all';
  const scenarios: Scenario[] = [
    'gateway',
    'processor',
    'redis',
    'kafka',
    'mongo',
    'redis-partition',
  ];
  if (selected !== 'all' && !scenarios.includes(selected as Scenario))
    throw new Error(`Unknown scenario: ${selected}`);
  for (const scenario of selected === 'all'
    ? scenarios
    : [selected as Scenario])
    await runScenario(scenario);
}
