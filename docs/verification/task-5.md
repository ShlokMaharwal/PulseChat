# Task 5: reliability depth

Task 4 passed [the complete CI run](https://github.com/ShlokMaharwal/PulseChat/actions/runs/44546373422), including its authenticated MongoDB/Redis and non-root Redis proof, before the reliability topology was exercised.

Done when: the new scenarios print actual sent/stored/duplicate/lost/out-of-order counts, restoration is checked, and results are saved. Local default verification passed: backend 82 unit tests, frontend 14 unit tests, full source checks, 13 integration cases, eleven smoke checks, and six chaos scenarios across 725 sends. All six recorded zero lost messages, logical/wire duplicates, and order violations. [Actual summaries](task-5-chaos/). Redis returned UNAVAILABLE in 32 ms; the owning processor was interrupted at 81 stored / 39 queued.

## Default topology

Redis now has a dedicated Compose network. Gateways, processors, smoke, and integration clients join that network alongside the ordinary Kafka/Mongo network. Disconnecting one gateway from the Redis network preserves its other network. The partition scenario verifies that gateway readiness reports Redis unavailable while MongoDB and Kafka remain reachable, then reconnects the same container and recovers its ordered conversation. The network is reattached in `finally` on failure as well.

The Mongo scenario stops MongoDB for at least eight seconds during 100 queued sends, starts the same container with its original data volume, and checks the resulting 120-message conversation. The existing Redis scenario now also probes a live WebSocket during the outage: the gateway already returned a bounded `UNAVAILABLE` error, and the test verifies that behavior rather than replacing it. The frontend displays a degraded-state banner and clears it after a successful recovery response; initial failed connections also show that state.

## Optional Kafka HA profile

```sh
docker compose stop
docker compose -f docker-compose.yml -f docker-compose.ha.yml --profile ha --parallel 1 up --build -d --wait --wait-timeout 600
```

This uses three KRaft broker/controllers, six partitions per application topic, replication factor three, and `min.insync.replicas=2`. The default single-broker file remains independent. Each HA broker has its own new log volume; the original single-broker log volume is preserved. MongoDB and Redis retain their existing data. This profile is a test topology, not an automatic migration or disaster-recovery mechanism.

From `backend/`, run `npx tsx chaos/run.ts kafka` with `KAFKA_BROKERS=localhost:19092,localhost:19093,localhost:19094`, `KAFKA_REPLICATION_FACTOR=3`, `COMPOSE_PROFILES=ha`, and `COMPOSE_FILE` pointing to both Compose files (colon separator on Unix, semicolon on Windows). The test verifies three in-sync replicas and the effective topic minimum before the fault. It disables automatic restart only on the chosen active partition leader, sends SIGKILL, holds that broker down, and requires a new leader with at least two in-sync replicas. The original restart policy is restored, and all replicas must catch up after restart. The CI `kafka-ha` job runs this exact test and uploads its summary/logs.

To restore the default cluster, stop the HA services with both files/profile, then run `docker compose up -d --wait` with the default file. Preserve named volumes; `down --volumes` would also remove the shared MongoDB/Redis data. No automatic failover for MongoDB or Redis is claimed.

References: [Kafka replication and minimum ISR](https://kafka.apache.org/39/configuration/broker-configs/), [KafkaJS effective configuration queries](https://kafka.js.org/docs/admin), [Compose profiles](https://docs.docker.com/compose/how-tos/profiles/).

Local HA attempts are retained separately: the [initial broker-kill attempt](task-5-chaos/kafka-ha-initial-timeout.json) ended at its 60-second ACK wait with 28 observed stored messages and 92 unfinished sends; it did not settle the pipeline and does not establish permanent loss. The harness now waits up to 180 seconds for HA ACKs and awaits both fault and send operations before cleanup. A [second attempt](task-5-chaos/kafka-ha.json) failed during processor readiness before issuing test messages. Both restored the original default stack and preserved all volumes. The CI HA job provides the clean-run proof; its result must be checked before claiming successful broker failover.
