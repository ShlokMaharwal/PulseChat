# PulseChat improvements

## Runtime and measurement

Kept the existing gateway/processor pipeline and its ordering, Lua sequence idempotency, unique persistence keys, manual offsets, and sync recovery. Added bounded Kafka producer batching, concurrency across assigned partitions, atomic route lookup/pruning, atomic receipt updates, and concurrent recipient publication. Work within a partition remains serial.

The distinct-user load harness now records stage histograms and traces, generator scheduling/CPU, Docker samples, source snapshots, accepted-record audit, and exact issued/ACK/persisted/peer ledgers. It requires zero consumer/audit lag and fresh zero gateway in-flight samples before final classification. Generator output uses native Docker storage after a measured Windows bind-mount I/O penalty.

The configured 200-user before/after comparison measured steady peer delivery of 4.87 versus 11.50 messages/s. The optimized run persisted and delivered all 1,367 issued sends. Independent server connection measurements dipped in both runs; the baseline left messages undelivered, making its observed latency conditional. The 500-user overload run persisted all 2,233 Kafka-accepted sends but delivered only 28 to peers during observation. [Exact results and stage percentiles](benchmarks.md).

The historical 992-attempt run contained 975 distinct retained Kafka records and 975 matching MongoDB records. The other 17 attempts never entered the durable log; original individual ACK records are unavailable. [Evidence and limits](verification/task-2.md).

## Security and reliability

Added Redis-backed authentication throttles/backoff/lockout, strict WebSocket origins and production JWT-secret checks, short access tokens, atomic refresh rotation/replay revocation, silent browser renewal, and server-confirmed logout. Added Nginx security headers, strict encoding/body validation, optional authenticated MongoDB/Redis configuration, and dependency audit gates. [Security proof](verification/task-4.md).

Six default-stack chaos cases passed across 725 sends, including Mongo restart and an isolated gateway/Redis partition, with zero loss, logical/wire duplication, or order violations. The frontend now exposes degradation and recovery. The optional three-broker Kafka profile uses replication factor three and minimum ISR two; local broker-failure attempts were inconclusive and remain recorded separately. Its dedicated CI job verifies an actual broker SIGKILL. [Results](verification/task-5.md).

## Product and documentation

Retained neutral true-black/emerald and light themes. Added retry countdown/manual reconnect, same-ID failed-send retry, unread navigation, and keyboard focus restoration. Five real browser tests cover auth, chat/receipts, offline recovery, gateway failure, and refresh/logout. Added reproducible two-session demo tooling and repository metadata text. Reorganized historical evidence, shortened the README, and added a relative-link checker.

## Remaining limits and deliberate exclusions

The final configured 200-user check ACKed, persisted, and peer-delivered all 1,384 issued sends after zero-lag drain; steady peer delivery was 11.17 messages/s, with high steady-cohort p95 of 54,422.55 ms. A fresh three-broker CI startup exposed a transient Kafka metadata-propagation race. Bootstrap now retries only Kafka-declared retriable errors, up to ten attempts, while incompatible topology and permission failures still fail immediately. Three regression cases brought backend unit coverage to 85 passing tests. [Final verification](verification/task-7.md).

The default Kafka, MongoDB, and Redis services remain single instances. Pub/Sub requires history sync after missed events; terminal DLQ records may leave sequence gaps. Refresh revocation does not invalidate an already issued access JWT before its expiry. Current load evidence establishes neither production capacity nor low latency. This change does not add groups or typing indicators because the requested essential verification took priority. No Docker resource caps were introduced.
