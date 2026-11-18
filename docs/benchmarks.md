# Local load measurements

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

Committed evidence: [baseline measurements](load-results/baseline-200.json), [baseline k6 summary](load-results/baseline-200-k6.json), [stress measurements](load-results/stress-2000.json), and [stress k6 summary](load-results/stress-2000-k6.json). The capture timestamps are in those files.

The load adapter uses the official [k6 WebSocket API](https://grafana.com/docs/k6/latest/javascript-api/k6-websockets/) and [custom summary API](https://grafana.com/docs/k6/latest/results-output/end-of-test/custom-summary/). Correctness under process failures is separately established by the automatically asserted chaos tests, rather than inferred from this failed stress attempt.

Version scope: the recorded load runtime was the Phase 4 implementation at `566823a`, with the verification tooling later committed in `74becd2`. The subsequent Zod security update was not re-benchmarked; no new performance number is inferred for that dependency change.
