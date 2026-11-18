# Phase 3 verification

Date: 18 November 2026 (Asia/Kolkata).

**Status: PASS.** Reliability completion checks passed against the real Docker stack. The preceding Phase 2 commit is `7c6855a`.

| Check | Observed result |
| --- | --- |
| TypeScript, ESLint, formatting, and image builds | PASS |
| Unit tests | 60 passed across 8 files |
| Live integration tests | 10 passed; suite duration 103.84 seconds |
| Cross-gateway delivery | 1,000 messages, sequences 1–1,000, matching MongoDB records and no duplicate wire delivery |
| Offline recovery | 125 messages recovered in order through paginated sync |
| Receipts | Delivered and read cursors reached the sender and persisted as 125; chat unread count became zero |
| Duplicate sessions | The same user received messages on two gateways; closing one preserved the other |
| Poison record | Six failed attempts, base64-preserving DLQ envelope, and successful delivery of the next record in the same chat partition |
| Limits and invalid input | Rate rejection before storage; malformed JSON rejected while the connection remained usable; binary and oversized sockets closed with 1003 and 1009 |
| Hard-crash presence | PASS; presence and routing keys expired within 30.358 seconds, below the test's 40-second deadline |

The crash test uses SIGKILL, disables test-client reconnection for the affected user, checks Redis directly, and restores gateway-3 in `finally`. An earlier run observed expiry in 30.241 seconds. The strengthened run also checks the singular routing hint and per-gateway member key.

A post-test smoke attempt passed 10 of 11 checks but timed out after 15 seconds while Grafana queried Prometheus. Direct checks from inside Grafana and through its authenticated datasource API subsequently returned OK. The complete repeat smoke run passed all 11 checks with zero failures. This transient is recorded rather than counted as a successful first run.

Commands:

```sh
cd backend
npm run typecheck
npm run lint
npm run format:check
npm test
cd ..
docker compose --parallel 1 build gateway-1 processor-1 integration
docker compose --parallel 1 up -d --wait --wait-timeout 600
docker compose --profile test run --rm --no-deps integration
cd backend
npm run test:presence
cd ..
docker compose --parallel 1 up -d --wait --wait-timeout 300
docker compose run --rm --no-deps smoke
```

The 1,000-message integration case took 74.913 seconds. It is a correctness test with receipts, authorization, assertions, and test-client bookkeeping, not a throughput benchmark. Application dashboards, chaos/load measurements, CI proof, and the browser frontend remain later phases. No result for those phases is claimed here.
