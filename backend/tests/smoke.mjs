import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  createDependencies,
  createLogger,
  parseConfig,
  TOPICS,
} from '@pulsechat/shared';

const config = parseConfig(process.env);
const dependencies = createDependencies(
  config,
  'smoke',
  createLogger(config, 'smoke'),
);
let passed = 0;
let failed = 0;

async function check(name, run) {
  try {
    await run();
    passed++;
    console.log(`PASS ${name}`);
  } catch (error) {
    failed++;
    console.error(
      `FAIL ${name}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

async function get(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(30000) });
  assert.equal(response.status, 200, `${url} returned ${response.status}`);
  return response;
}

try {
  const services = [
    'gateway-1',
    'gateway-2',
    'gateway-3',
    'processor-1',
    'processor-2',
  ];
  const instanceIds = new Set();
  for (const service of services) {
    await check(`${service} alive, ready, and exports metrics`, async () => {
      await get(`http://${service}:8080/healthz`);
      const ready = await (await get(`http://${service}:8080/readyz`)).json();
      assert.equal(ready.status, 'ready');
      assert.deepEqual(ready.dependencies, {
        redis: true,
        mongo: true,
        kafka: true,
      });
      assert.ok(
        !instanceIds.has(ready.instanceId),
        'Replicas must have distinct instance identities',
      );
      instanceIds.add(ready.instanceId);
      assert.match(
        await (await get(`http://${service}:8080/metrics`)).text(),
        /pulsechat_process_cpu_user_seconds_total/,
      );
    });
  }
  await check('nginx alive and proxies gateway readiness', async () => {
    await get('http://nginx/healthz');
    assert.equal(
      (await (await get('http://nginx/api/readyz')).json()).service,
      'gateway',
    );
  });
  await check('Redis, MongoDB, and Kafka are usable', async () => {
    // Do not call connect(): a smoke check must verify, not create missing topics/indexes.
    const connections = await Promise.allSettled([
      dependencies.redis.connect(),
      dependencies.mongo.connect(),
      dependencies.admin.connect(),
    ]);
    assert.ok(
      connections.every((result) => result.status === 'fulfilled'),
      'At least one dependency connection failed',
    );
    assert.deepEqual(await dependencies.check(), {
      redis: true,
      mongo: true,
      kafka: true,
    });
  });
  await check(
    'all Kafka topics have six partitions and elected leaders',
    async () => {
      const metadata = await dependencies.admin.fetchTopicMetadata({
        topics: [...TOPICS],
      });
      for (const name of TOPICS) {
        const topic = metadata.topics.find((entry) => entry.name === name);
        assert.ok(topic, `Missing topic ${name}`);
        assert.equal(topic.partitions.length, 6);
        assert.ok(topic.partitions.every((partition) => partition.leader >= 0));
      }
    },
  );
  await check('MongoDB message indexes exist', async () => {
    const indexes = await dependencies.db
      .collection('messages')
      .listIndexes()
      .toArray();
    assert.ok(
      indexes.some(
        (index) => index.name === 'messageId_unique' && index.unique,
      ),
    );
    assert.ok(
      indexes.some(
        (index) => index.name === 'chatId_seq_unique' && index.unique,
      ),
    );
  });
  await check(
    'Prometheus is ready and scrapes every backend replica',
    async () => {
      await get('http://prometheus:9090/-/ready');
      let targets = [];
      for (let attempt = 0; attempt < 15; attempt++) {
        targets = (
          await (await get('http://prometheus:9090/api/v1/targets')).json()
        ).data.activeTargets;
        if (
          targets.length === 5 &&
          targets.every((target) => target.health === 'up')
        )
          break;
        await sleep(1000);
      }
      assert.equal(targets.length, 5);
      assert.ok(
        targets.every((target) => target.health === 'up'),
        JSON.stringify(targets),
      );
    },
  );
  await check(
    'Grafana reports a healthy database and provisioned datasource',
    async () => {
      assert.equal(
        (await (await get('http://grafana:3000/api/health')).json()).database,
        'ok',
      );
      const auth = Buffer.from(
        `admin:${process.env.GRAFANA_PASSWORD ?? 'pulsechat-local'}`,
      ).toString('base64');
      const response = await fetch(
        'http://grafana:3000/api/datasources/uid/pulsechat-prometheus/health',
        {
          headers: { Authorization: `Basic ${auth}` },
          signal: AbortSignal.timeout(15000),
        },
      );
      assert.equal(response.status, 200);
      assert.equal((await response.json()).status, 'OK');
    },
  );
} catch (error) {
  failed++;
  console.error(error);
} finally {
  await dependencies.close();
}
console.log(`Phase 1 smoke: ${passed} passed, ${failed} failed`);
process.exitCode = failed ? 1 : 0;
