# Resume bullets backed by recorded results

- Increased steady peer delivery from 4.87 to 11.50 messages/s (2.36×) in the configured 200-user comparison using bounded Kafka batching, concurrent partitions, and fewer Redis/Mongo round trips; all 1,367 optimized sends persisted and reached peers.
- Reconciled the historical 992-attempt stress run against retained Kafka offsets and MongoDB: all 975 distinct accepted records persisted; the remaining 17 attempts never reached the durable source log. Original per-attempt ACK identities were unavailable.
- Automated six default-stack failure scenarios covering 725 sends with zero lost messages, logical/wire duplicates, or ordering violations, including MongoDB restart and a gateway-to-Redis partition; additionally verified 120/120 deliveries after active-leader SIGKILL in a three-broker Kafka cluster with minimum ISR two.
- Hardened authentication with Redis throttling, validated WebSocket origins, 15-minute access tokens, rotating and revocable refresh sessions, and optional authenticated MongoDB/Redis; verified 85 backend unit tests, 13 integration cases, and eleven smoke checks.
- Verified 15 frontend unit tests and five real browser scenarios covering login, escaped messages, receipts, offline queuing, gateway SIGKILL recovery, silent token renewal, and logout revocation.

Evidence: [measured comparison](benchmarks.md), [17-attempt investigation](verification/task-2.md), [security verification](verification/task-4.md), [six failure summaries](verification/task-5-chaos/), and [browser verification](verification/task-6.md).

The configured 200-user comparison had independent server connection dips and a right-censored baseline; it does not prove continuously sustained 200-user capacity or an unconditional latency improvement. The 500-user profile overloaded the host. Do not claim successful 2,000–5,000-user capacity, subsecond latency, or production infrastructure high availability.
