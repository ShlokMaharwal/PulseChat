# Observability

Prometheus scrapes all three gateways and both processors. Each service exposes `/metrics` alongside process/runtime metrics. The provisioned Grafana dashboard is `http://localhost:3000/d/pulsechat-overview`; use the local admin credentials documented in the README. Authentication remains enabled. The dashboard source and file provider live in `infra/grafana/`.

| Metric | Meaning |
| --- | --- |
| `pulsechat_active_connections` | Authenticated sockets, including concurrent sessions for a user |
| `pulsechat_messages_in_total` | Valid gateway message attempts; consumed message records on processors, including replay |
| `pulsechat_messages_out_total` | Canonical socket deliveries on gateways; new message inserts on processors |
| `pulsechat_consumer_lag` | Topic log end minus the group's committed offset, polled every five seconds |
| `pulsechat_consumer_lag_peak` | Highest observed pending count per partition since the process started; batch observations preserve short bursts between scrapes |
| `pulsechat_delivery_latency_seconds` | Gateway acceptance to arrival of the client's delivery ACK, for advancing cumulative delivered cursors |
| `pulsechat_dedupe_hits_total` | Logical messages already stored when replayed |
| `pulsechat_dlq_total` | Failed source records acknowledged by the DLQ broker |
| `pulsechat_rate_limit_rejections_total` | Operations rejected by the shared Redis bucket |

The lag probe reports the same group from both processors. Aggregate with `sum(max by(topic, partition)(pulsechat_consumer_lag))`, rather than summing the two reports. The peak is historical within the process lifetime and is explicitly labeled separately from current backlog. Failed lag probes remove their current samples and emit a warning instead of reporting a fabricated zero.

Delivery timing includes the ACK return path. Receipts are cumulative and debounced, so the histogram samples advancing receipt positions rather than every individual message. Histogram quantiles are bucket estimates; they are separate from the raw client measurements used in load tests. Gateway ingress counts attempts, and socket egress counts fan-out deliveries, so their rates need not be equal.

Identifiers belong in structured JSON logs, not metric labels. Message acceptance and persistence logs include `messageId`, `chatId`, and `userId`. Receipt logs include chat, user, sequence, status, and the acknowledged message ID when available. No message bodies, passwords, or JWTs are logged by these events.

To create real data for all ten dashboard panels:

```sh
cd backend
npm run test:observability
```

This host-run probe creates isolated test accounts and a chat, sends 80 messages, retries a logical send, submits one intentional poison record, and forces one isolated test user's bucket to reject a request. It asserts positive Prometheus values for every category, writes the actual query results to ignored `.cache/observability.json`, and maintains light traffic and open sockets for five minutes to allow dashboard capture. Creating `.cache/observability.stop` ends the holding period cleanly. This probe demonstrates telemetry and failure paths; it is not a capacity benchmark.
