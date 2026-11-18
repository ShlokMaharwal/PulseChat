# Resume bullets backed by recorded results

- Built a replicated TypeScript chat pipeline with 3 WebSocket gateways, 2 Kafka processors, and 6 partitions, using MongoDB unique indexes and atomic Redis Lua assignment for effectively-once storage under the tested retry model.
- Verified 1,000 cross-gateway messages in canonical sequence order with matching MongoDB records and zero duplicate wire deliveries; five repeated logical sends produced one stored message.
- Automated four failure scenarios covering 485 logical sends with zero lost messages, duplicate logical deliveries, or ordering violations, including recovery after a processor was killed with 79 messages still queued.
- Provisioned a 10-panel Grafana dashboard and verified real message, receipt, lag, dedupe, DLQ, and rate-rejection telemetry; measured crashed-user presence expiry in 30.36 seconds.

Evidence: phase verification documents, `chaos-results/`, the actual dashboard screenshot, and `benchmarks.md`. The 2,000-connection stress target was not achieved; do not claim a successful 2,000–5,000-user benchmark or subsecond latency.
