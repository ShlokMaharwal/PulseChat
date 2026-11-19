# Performance investigation

The benchmark uses one distinct user per socket, paired into 1:1 chats. Group chats are not implemented. Setup, a public peer-delivery preflight, and sampling each chat's starting sequence happen outside the measurement window.

## Changes

Gateways previously submitted one-record message and receipt requests through the same idempotent Kafka producer with one request in flight. The bounded FIFO publisher coalesces these records: by default, 64 records and 512 KiB per batch, with a five-millisecond partial-batch timer. The queue permits 1,024 records and 16 MiB of encoded data. Every caller waits for `acks=-1`; failed batches reject their waiting callers. `KAFKA_BATCH_MAX_MESSAGES=1` disables coalescing without a linger delay. `KAFKA_BATCH_LINGER_MS` controls the timer.

Batching reduced producer wait but exposed a receipt backlog. Both the `messages` and `receipts` topics have six partitions. Each processor was limited to three concurrent partitions despite owning three partitions from each topic. `PROCESSOR_PARTITION_CONCURRENCY` now defaults to six. Records within each partition remain serial; offsets remain manually committed after successful processing or durable dead-lettering.

Routing now checks live memberships and prunes stale entries in one Redis Lua call. Receipt cursors advance in one atomic MongoDB operation returning the prior state; duplicates retain their cursors and update timestamp. Recipient publications run concurrently, with all awaited before the next record in that partition. Sequence allocation, deterministic IDs, unique indexes, and sync recovery retain their existing behavior.

## Measurement controls

The generator reports client-observed welcomed sockets and their minimum throughout the steady window. Missing that population fails the run. Its callback timestamps can lag server disconnects during stalls; compare the independent Prometheus connection series and socket errors before making a capacity claim. It measures peer ingestion, delivery sample counts, undelivered sends, generator CPU, and scheduler delay.

An I/O probe found that 300 appends of 511 bytes took 528.03–902.09 ms through the Windows bind mount, versus 6.17–13.41 ms in Docker storage. Generator logs now use an anonymous Docker volume and are copied out afterward. A startup barrier makes the generator visible to the CPU sampler before traffic begins. [Raw probe data](verification/performance/io-comparison.json) and [its script](verification/performance/io-check.cjs) are committed.

Runs record issued sends, received ACKs, an independent Kafka audit, peer deliveries, MongoDB outcomes, actual hardware/settings, sampled container CPU/memory, runner CPU/event-loop delay, Prometheus series, and a compressed source snapshot. Fixture credentials remain ignored and unpublished.

Before and after traffic, the checker requires zero processor lag, zero audit lag, and fresh zero-in-flight samples from all three gateways. Unsettled runs fail and leave unacknowledged/unpersisted sends unresolved. The audit pins initial offsets and replays after coordinator failures, deduplicating by message ID.

Stage traces use current epoch milliseconds. Negative/nonfinite spans are excluded and counted. Produce acknowledgement and consumer work overlap, so stage percentiles are not additive. Trace sample counts are explicit: stored sync results have no live trace. CPU profiles are separate diagnostics, excluded from the latency comparison.

## Reproduce

```sh
docker compose -f docker-compose.yml -f docker-compose.performance.yml up -d --build --wait
cd backend
```

For the comparison, set `WARMUP_MS=30000`, `MESSAGE_INTERVAL_MS=5000`, and `DRAIN_MS=60000`, then run `npm run test:load -- 200`. The connection ramp and steady window default to 10 and 30 seconds. Only the final five seconds of warmup generate messages. Reuse accounts with `LOAD_FIXTURE=<private fixture.json>`; starting cursors are resampled each run.

HTTP fixture creation is the default. `LOAD_FIXTURE_MODE=seed` and the stack's `JWT_SECRET` opt into synthetic MongoDB setup with one shared test-password hash. This avoids measuring password hashing during messaging setup; it does not measure registration capacity.

The performance override uses a 60-second Kafka broker lease because this host recorded scheduling delays beyond the default nine-second lease. The default Compose stack retains Kafka's normal lease. This local mitigation is separate from software improvements; no Docker resource limits were added. Set `KAFKA_BROKER_SESSION_TIMEOUT_MS=9000` to reproduce the unmitigated case.

If port 8080 is occupied, set `PULSECHAT_PORT=8081` for Compose and `TEST_HTTP_URL=http://127.0.0.1:8081` for the runner. The load container reaches Nginx through the Compose network.

Set `PROFILE_CPU=true` for a separate diagnostic. Profiling uses a local inspector session at a ten-millisecond interval: `SIGUSR2` starts it and `SIGURG` stops it. No debugger network listener opens.

Results and remaining limits are recorded in [benchmarks.md](benchmarks.md). Earlier repeated-session profiles used different workloads and cannot establish a controlled improvement ratio.
