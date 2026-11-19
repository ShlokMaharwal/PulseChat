# Load measurements

## Distinct-user comparison: 19 November 2026

Both runs used 200 distinct users in 100 1:1 chats, the same account pairs and chat keys, current starting cursors, a 10-second connection ramp, 30-second warmup (only its last five seconds sent messages), 30-second steady window, and 60-second client drain. Each socket attempted one message every five seconds; delivered/read receipts and heartbeat/sync traffic were enabled. Both client ledgers recorded 200 welcomed sockets throughout the steady window. Both passed the ledger, resource-sampling, and post-run drain checks. However, the independent Prometheus connection sums dipped to 67 in each run. Scrape availability and connection instability under this host's load prevent those client counts from proving uninterrupted server-side concurrency. These are observational runs at the same configured load, not a 200-user capacity certification.

The gateway's bounded Kafka batching and Docker-native generator logging were already enabled in this controlled pair. The compared changes were atomic Redis routing lookup, atomic receipt advancement, concurrent recipient fan-out, and increasing each processor from three to six concurrent partitions to cover its message and receipt assignments. The earlier single-record/batching-only diagnostic runs are retained under verification; they are not substituted into this comparison.

| Measurement | Before routing/processor changes | Optimized |
| --- | ---: | ---: |
| Distinct users | 200 | 200 |
| 1:1 chats | 100 | 100 |
| Peak authenticated sockets | 200 | 200 |
| Client-observed minimum welcomed sockets in steady window | 200 | 200 |
| Issued sends | 1,266 | 1,367 |
| Received-ACKed | 1,266 | 1,367 |
| Independently observed in Kafka | 1,266 | 1,367 |
| Persisted after drain | 1,266 | 1,367 |
| Peer delivered before client closure | 751 | 1,367 |
| Steady issued messages/s | 35.53 | 38.90 |
| Steady peer deliveries/s | 4.87 | 11.50 |
| Steady-cohort issued sends | 1,066 | 1,167 |
| Steady-cohort delivered samples | 551 | 1,167 |
| Steady-cohort undelivered at closure | 515 | 0 |
| Delivered steady-cohort p50 (ms) | 48,823.00 | 37,593.00 |
| Delivered steady-cohort p95 (ms) | 72,767.00 | 54,907.10 |
| Delivered steady-cohort p99 (ms) | 80,129.00 | 57,673.20 |
| Accepted but unpersisted | 0 | 0 |

The observed steady receive rate increased **2.36×**. These are the same configured workloads; actual offered rates differ and are shown. Before-run latency percentiles cover only delivered messages: its missing cohort is right-censored at closure. The after run delivered its complete cohort. Do not describe the conditional percentile change as a complete-population latency guarantee.

### Stage latency

Milliseconds, exact client-trace sample quantiles. Counts are explicit because sync recovery does not carry a live trace. Producer acknowledgement can overlap consumption; these columns must not be added into a total. The last stage measures ordered client ingestion, including client buffering where applicable.

| Stage | Before samples | Before p50 / p95 | After samples | After p50 / p95 |
| --- | ---: | ---: | ---: | ---: |
| client_to_gateway | 472 | 1,566.00 / 4,366.05 | 968 | 652.00 / 2,961.95 |
| gateway_prepare | 472 | 11,289.00 / 29,946.75 | 968 | 7,875.50 / 19,260.75 |
| kafka_produce_ack | 1266 | 2,941.00 / 8,709.75 | 1367 | 1,477.00 / 5,295.00 |
| kafka_to_processor | 472 | 6,906.50 / 37,108.70 | 968 | 9,963.00 / 31,932.65 |
| partition_queue | 472 | 6,247.00 / 32,552.70 | 968 | 10,611.50 / 28,231.70 |
| processor_prepare | 472 | 37.00 / 374.30 | 968 | 35.00 / 268.00 |
| sequence_assign | 472 | 7.00 / 63.35 | 968 | 8.00 / 67.65 |
| mongo_persist | 472 | 39.00 / 316.35 | 968 | 36.00 / 247.55 |
| persist_to_publish | 472 | 55.00 / 302.90 | 968 | 29.00 / 175.30 |
| publish_to_gateway_push | 472 | 446.50 / 1,757.65 | 968 | 139.00 / 968.20 |
| gateway_push_to_client | 472 | 29.00 / 1,896.40 | 968 | 9.00 / 761.10 |

### Hardware and limits

The host reported an Intel Core i5-3437U at 1.90 GHz, four logical CPUs, 6,299,365,376 bytes of physical memory, and Windows 10.0.22621. Docker Desktop 25.0.3 / WSL reported 4 CPUs and 2,978,533,376 bytes of memory. Existing application containers had no individual CPU/memory cgroup caps; the existing generator limit was 640 MiB. No resource limits were added. The unrelated Seekr containers were temporarily stopped during verification. Full limits, image IDs, safe runtime settings, raw per-container CPU/memory samples, and generator/runner lag are in the evidence.

The local performance override used a 60-second Kafka broker lease in both compared runs. Earlier logs showed host scheduling stalls exceeding Kafka's default nine-second lease, repeated coordinator reloads, and failed handshakes. The default Compose stack retains the normal lease. This mitigation is separate from the software changes and is not a production tuning recommendation.

### Findings and remaining bottleneck

One-record message and receipt requests shared a single idempotent producer with one request in flight. Batching reduced producer acknowledgement wait, but the first batching-only run still accumulated receipt backlog. There are twelve consumed topic partitions in total: six message and six receipt partitions. Routing required sequential Redis round trips, receipts required a read followed by an update, and fan-out was serial. The optimized code reduces those round trips and enables all assigned partitions without changing within-partition ordering or manual offsets.

Tail latency is still high: the optimized live-trace partition queue p95 was 28,231.70 ms, gateway preparation/queued-operation p95 19,260.75 ms, and producer-to-consumer p95 31,932.65 ms. Those spans overlap. Mongo insertion p95 was 247.55 ms and sequence assignment p95 67.65 ms. Shared CPU pressure, per-partition serial work, and Kafka scheduling/coordinator failures remain material limitations. The diagnostic CPU profiles showed substantial idle, GC, and native-write time rather than one isolated application hot function. No production capacity or low-latency SLO follows from these local measurements.

The generator initially wrote every event through a Windows bind mount. A same-process append probe measured 300 writes of 511 bytes in 528.03–902.09 ms on that path, versus 6.17–13.41 ms in Docker storage. Logs now use a native anonymous volume. Startup barriers, explicit container sampling, and parsing terminal-prefixed first rows prevent missing CPU evidence. [I/O measurements](verification/performance/io-comparison.json), [probe script](verification/performance/io-check.cjs).

### Evidence and reproduction

- [Before measurements](verification/performance/2026-11-19-before-processor-native-200/measured.json), [before raw evidence](verification/performance/2026-11-19-before-processor-native-200/raw-evidence.zip), [before source](verification/performance/2026-11-19-before-processor-native-200/source-snapshot.json.gz).
- [After measurements](verification/performance/2026-11-19-after-processor-native-200/measured.json), [after raw evidence](verification/performance/2026-11-19-after-processor-native-200/raw-evidence.zip), [after source](verification/performance/2026-11-19-after-processor-native-200/source-snapshot.json.gz).
- [Measurement method, commands, and guarantees](PERFORMANCE.md). Each source archive records the actual working runtime and runner; hardware files record image IDs. The Git base was 00b7700, with working-source hashes in each run. Fixture credentials are deliberately omitted.

### Capacity boundary

The next run used 500 distinct users in 250 chats with the same timing, interval, receipts, and tracing. The client ledger recorded a peak and steady minimum of 500 welcomed sockets, but 576 socket errors and the Prometheus connection series showed severe instability. Of 2,233 issued messages, 625 received an acceptance ACK; all 2,233 appeared independently in Kafka and persisted after verified zero lag. There were no accepted-but-unpersisted messages. Only 28 reached peers before client closure. Steady peer delivery fell to 0.20 messages/s; the steady cohort delivered only six of 2,014 sends, with conditional p50/p95/p99 of 89,952.50 / 92,845.50 / 93,183.50 ms. Those six samples do not characterize the undelivered population. This is an overload result, not a supported 500-user capacity claim. The ramp stopped here.

[500-user measurements](verification/performance/2026-11-19-capacity-500/measured.json), [raw evidence](verification/performance/2026-11-19-capacity-500/raw-evidence.zip), [source snapshot](verification/performance/2026-11-19-capacity-500/source-snapshot.json.gz).

## Final verification run

A fresh run on the security-hardened runtime at commit `6bda72a` used 200 distinct users / 100 chats and the same 10-second ramp, 30-second warmup, 30-second steady interval, five-second per-socket send interval, and 60-second drain. All **1,384 issued sends** received acceptance ACKs, appeared independently in Kafka, persisted, and reached peers; accepted-but-unpersisted and unresolved counts were zero. Processor message/receipt lag and audit lag reached zero, and all three gateways reported fresh zero in-flight samples. Ledgers and resource sampling passed.

Steady peer delivery was **11.17 messages/s**; all 1,184 steady-cohort sends delivered, with p50/p95/p99 **44,146 / 54,422.55 / 56,837.40 ms**. The client ledger again recorded 200 welcomed sockets, while independent Prometheus samples dipped to 66 during the observation. This remains a configured-load correctness check with high latency, not continuously sustained server capacity. This fresh fixture has different chat keys and partition placement, so it is not substituted into the controlled before/after comparison. The later startup-only fix retries Kafka-declared transient topic-bootstrap errors; no processing-path changes followed this run.

[Final measurements, including stage breakdown](verification/performance/2026-11-19-final-200/measured.json), [hardware/settings](verification/performance/2026-11-19-final-200/hardware.json), [exact run configuration](verification/performance/2026-11-19-final-200/run.json), [raw ledgers and resource samples](verification/performance/2026-11-19-final-200/raw-evidence.zip), [source snapshot](verification/performance/2026-11-19-final-200/source-snapshot.json.gz).

The earlier 500-user overload profile is retained as the capacity boundary. It was not repeated during final checks because no processing-path changes followed the measured optimization.

## Historical repeated-session measurements

### Resolution of the 17-attempt difference

A complete retained Kafka/DLQ scan found 975 distinct source messages for the historical stress chats, all persisted, and no matching DLQ records. All retained partition low offsets were zero. The difference from 992 issued attempts therefore occurred before durable Kafka append. The old adapter did not retain attempted UUIDs or acceptance ACKs, so individual identities and ACK status cannot be reconstructed. The new overload runs classify every issued UUID after zero lag: 225 issued / 73 received-ACKed / 223 Kafka accepted / 223 stored / two failed before acceptance, and 2,233 issued / 625 received-ACKed / 2,233 Kafka accepted / 2,233 stored. Both had zero accepted-but-unpersisted messages. [Full investigation and reproducible audit](verification/task-2.md).

The following records describe the older adapter and workload, not the distinct-user comparison above. Their original stored-count observation is retained; the investigation above explains its limits. The old reproduction commands below refer to the historical tooling revision, not the current adapter.



The following records describe the older adapter and workload, not the distinct-user comparison above. Their original stored-count observation is retained; Task 2 investigates the 17 absent sends separately. The old reproduction commands below refer to the historical tooling revision, not the current adapter.

Measured on 18 November 2026 with Docker Desktop/WSL: **4 Docker CPUs, 2,978,533,376 bytes (2.77 GiB) of Docker memory**, and approximately 5.87 GiB physical host memory. The unrelated Seekr containers were temporarily stopped. Three gateways, two processors, Kafka, Redis, MongoDB, Prometheus, Grafana, and the k6 container shared this allocation. The load generator was capped at 640 MiB.

**The 2,000–5,000 connection target was not achieved.** A 200-connection baseline reached 200 observed sockets. The attempted 2,000-connection run peaked at 533 observed sockets and failed k6's connection-success threshold. These are real results, not a claim of production capacity.

| Measurement | 200 target baseline | 2,000 target stress attempt |
| --- | ---: | ---: |
| Peak observed concurrent sockets | 200 | 533 |
| Issued logical sends | 458 | 992 |
| Confirmed by canonical live echo before shutdown | 404 | 685 |
| k6 reported issued messages/second | 4.1265 | 9.8450 |
| k6 reported confirmed messages/second | 3.6400 | 6.7982 |
| Delivery p50 | 3,820.50 ms | 1,595.00 ms |
| Delivery p95 | 11,082.25 ms | 40,213.20 ms |
| Delivery p99 | 17,758.11 ms | 42,484.28 ms |
| Unconfirmed when the load client stopped | 54 | 307 |
| Stored in MongoDB at the later observation | 458 | 975 |
| Issued sends absent at that MongoDB observation | 0 | 17 |
| Duplicate wire frames / ordering violations | 0 / 0 | 0 / 0 |
| Socket/application error events | 0 | 933 |
| Rate-rejection events | 0 | 0 |
| Peak sampled consumer-group lag | 48 | 102 |
| Peak summed backend Node CPU cores | 1.5273 | 1.9254 |
| k6 exit code | 0 | 99 (failed connection check) |

Latency measures the originating load socket's send time to its canonical message echo, once per originating UUID. It includes gateway, Kafka, processing, persistence, routing, and socket delivery. It is measured by the load client, separately from Grafana's cumulative-receipt histogram. Quantiles cover confirmed sends, so failed or unconfirmed attempts do not inflate the latency sample; the missing confirmation counts are shown explicitly.

The baseline's 54 unconfirmed live echoes were subsequently present in MongoDB. In the stress attempt, only 975 of the 992 issued sends were present at the later observation. The run did not record acceptance ACK counts, so the 17 absent sends cannot be classified as accepted versus unaccepted. This is an observed overload failure, and the stress run must not be described as lossless.

Both runs used **32 users, 16 chats, and up to 50 sockets per k6 virtual user**, with a send interval of 20 seconds per socket. This repeated-session profile tests socket concurrency and fan-out; it does not demonstrate 2,000 distinct users. The ramp lasts 100 seconds, stops new sends 15 seconds before the socket close deadline, and allows a bounded final drain. k6 interrupted iterations during ramp-down; the failed stress run also emitted timer-stop warnings. Docker startup adds wall time outside k6's reported message rates.

Prometheus sampled actual gateway connection gauges every five seconds. The stress attempt observed all three gateways active, with individual peaks of 187, 210, and 189 sockets. Those individual peaks are not simultaneous and must not be added to claim a higher concurrent total. The summed simultaneous peak was 533.

## Observed bottleneck

During the stress interval, gateway logs recorded Redis command timeouts: 52 route-cleanup failures, 3 registration failures, and 33 message-operation failures. Sampled backlog reached 102. The evidence points to gateway routing/Redis round trips and shared local resource pressure; the run did not profile enough detail to attribute all delay to one dependency. Handshake failures and high tail latency occurred before the requested connection count. Increasing the target without addressing these limits would not establish usable capacity.

## Reproduce

Start the stack, then:

```sh
cd backend
npm run test:load -- 200
npm run test:load -- 2000
```

The runner uses the reconnecting Node client for a fixture preflight and k6's separate asynchronous WebSocket adapter for load. It creates isolated accounts, checks the public protocol, runs the pinned `grafana/k6:2.3.0` image, and saves the raw summary, sampled Prometheus series, and post-run MongoDB observation under ignored `.cache/loadtest/`. Authentication fixtures remain ignored and are not copied into documentation. Run `npm exec -- tsx loadtest/analyze.ts <run-directory>` to repeat the storage observation.

Committed evidence: [baseline measurements](verification/load-results/baseline-200.json), [baseline k6 summary](verification/load-results/baseline-200-k6.json), [stress measurements](verification/load-results/stress-2000.json), and [stress k6 summary](verification/load-results/stress-2000-k6.json). The capture timestamps are in those files.

The load adapter uses the official [k6 WebSocket API](https://grafana.com/docs/k6/latest/javascript-api/k6-websockets/) and [custom summary API](https://grafana.com/docs/k6/latest/results-output/end-of-test/custom-summary/). Correctness under process failures is separately established by the automatically asserted chaos tests, rather than inferred from this failed stress attempt.

Version scope: the recorded load runtime was the Phase 4 implementation at `566823a`, with the verification tooling later committed in `74becd2`. The subsequent Zod security update was not re-benchmarked; no new performance number is inferred for that dependency change.
