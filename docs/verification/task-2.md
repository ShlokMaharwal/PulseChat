# Task 2: the historical 17-message gap

Done when: distinguish attempted sends from durable acceptance, and verify that every observed acceptance persists after drain.

The [retained-log audit](legacy-2000-audit.json) scanned every partition of `messages` and `messages.dlq` from its retained low offset to a captured end offset on 19 November 2026. Every low offset was zero. It found 975 distinct historical load messages, all 975 still stored in MongoDB, no source message absent from storage, and no matching DLQ records. The historical fixture's 16 chat IDs and `k6-socket-` body prefix isolated that run. No data volumes or topics were reset during the investigation.

The original generator counted 992 send attempts. The 17-attempt difference never appeared in the retained Kafka source log. This is evidence of failure before durable append, rather than accepted-message loss or early Mongo observation. The old run recorded neither issued UUIDs nor acceptance ACKs; consequently, the individual identities and ACK status of those 17 attempts cannot be reconstructed. Do not retroactively label them as observed unacknowledged sends. No loss fix or replay was warranted by this evidence.

Reproduce the read-only scan from `backend/`: `npx tsx loadtest/legacy-audit.ts <historical-run-directory>`. The private fixture remains ignored; the committed evidence contains synthetic message IDs and bodies, not credentials.

The replacement checker was implemented with Task 1. It records each attempted UUID, acceptance ACK, independent Kafka observation, peer delivery, and Mongo outcome; requires fresh zero-in-flight gateway measurements and zero message/receipt/audit lag before final classification; and leaves unsettled missing attempts unresolved. Its unit tests cover acceptance without persistence, persistence without an observed ACK, unaccepted failures, incomplete ledgers, and unsettled runs.

| Actual overload run | Issued | Received-ACKed | Kafka accepted | Persisted after zero lag | Failed before acceptance | Accepted but unpersisted |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [200-target, default broker lease](performance/2026-11-19-batch-64-default-lease-200/measured.json) | 225 | 73 | 223 | 223 | 2 | 0 |
| [500-target](performance/2026-11-19-capacity-500/measured.json) | 2,233 | 625 | 2,233 | 2,233 | 0 | 0 |

The first run also persisted 150 sends without a received ACK reaching the client; the second persisted 1,608. Missing an ACK does not establish rejection. Both drained completely, and every received-ACKed message persisted. Neither overload run is a successful delivery-capacity benchmark.

Validation: backend formatting/lint/types, 76 unit tests and build; frontend formatting/lint/types, nine unit tests and build; ten Compose integration tests; eleven smoke checks; and [all four chaos scenarios](task-2-chaos/) passed. A smoke attempt made before Grafana was ready failed its datasource check; it passed after waiting for monitoring readiness. The chaos cases covered 485 sends with zero loss, logical duplicates, or ordering errors.
