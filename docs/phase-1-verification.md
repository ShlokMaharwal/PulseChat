# Phase 1 verification

Date: 18 November 2026 (Asia/Kolkata).

**Status: PASS.** Phase 1's code checks, full Compose startup, and live smoke gate passed. Subsequent features are tracked in later phase commits.

## Verified checks

| Check | Actual result |
| --- | --- |
| `npm run format:check` | PASS |
| `npm run lint` | PASS, zero warnings |
| `npm run typecheck` | PASS |
| `npm test` | PASS, 33 tests across 4 files |
| `npm run build` | PASS |
| `docker compose config --quiet` | PASS |
| Full Compose startup | PASS, all 11 services healthy |
| Container smoke test | PASS, 11 assertions, 0 failures |

The code checks were rerun after updating the host database ports, smoke-test connection cleanup, and startup timeouts. Unit tests exercise invalid configuration, client/server schema validation, topic bootstrap, dependency outages, readiness recovery, concurrent probes, a hung-probe deadline, and HTTP behavior.

## Live verification commands

From the repository root:

```sh
docker compose --parallel 1 up --build -d --wait --wait-timeout 600
docker compose run --rm smoke
```

The smoke script passed readiness and metrics assertions for all three gateways and both processors, dependency connectivity, all three six-partition topics with elected leaders, MongoDB indexes, Nginx proxying, all five Prometheus scrape targets, and Grafana datasource health.

Initial local runs encountered very slow image extraction and container startup, registry TLS timeouts, and npm DNS/connection-reset retries. MongoDB's heavy shell probe timed out while its server was already running. Early gateway attempts also rejected startup on dependency timeouts and cleaned up their connections. Successful verification followed a Docker/WSL restart, cached npm downloads, one build per application role, lightweight infrastructure probes, and separate Kafka request and dependency timeouts. Functional readiness still executes actual MongoDB and Kafka operations on every backend replica; TCP probes alone do not satisfy the smoke gate.

Published Redis and MongoDB ports are `16379` and `27018` to avoid existing local database ports. The stack uses a 0.25 GiB MongoDB WiredTiger cache and a 512 MiB Kafka broker heap. Compose uses a 10-second dependency deadline and a 30-second Kafka request timeout; HTTP readiness probes have an explicit deadline. Kafka and MongoDB container probes use TCP without launching extra JVM or Mongo shell processes.

## Environment observed

- Host Node.js: 22.17.0; npm: 10.9.2.
- Docker Engine: 25.0.3; Compose: v2.24.6-desktop.1.
- Docker: 4 CPUs, 2,978,529,280 bytes of memory.
- Host physical RAM: approximately 5.87 GiB; approximately 0.56 GiB free during diagnosis.

No throughput, latency, connection-count benchmark, chaos PASS result, dashboard screenshot, or remote CI result is claimed. Those checks belong to later phases.
