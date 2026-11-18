# Phase 2 verification

Date: 18 November 2026 (Asia/Kolkata).

**Status: PASS.** Phase 2's core messaging completion checks passed against the real Docker stack. Phase 1 is separately committed as `c5b22fe`.

| Check | Actual result |
| --- | --- |
| Backend lint and TypeScript checks | PASS |
| Backend unit suite | PASS, 58 tests across 7 files |
| Gateway, processor, and integration Docker builds | PASS |
| Full stack functional readiness | PASS |
| Live integration suite | PASS, 4 cases |

The live assertions verified:

- REST authentication, valid login with normalized usernames, rejected invalid credentials and invalid WebSocket upgrades, protected history, and non-member sends rejected before persistence.
- 1,000 messages sent by user A while user B was connected to a different gateway. All 1,000 arrived, in body/send order, with sequences 1 through 1,000, no duplicate wire deliveries, and exact message ID/body/sequence agreement with MongoDB.
- Five sends using one `clientMsgId`. All five processing acknowledgments were observed, with exactly one stored message, one recipient delivery, and one assigned sequence.
- Assignment reused the same sequence after a simulated failure before persistence. After deleting a test chat's counter and mapping, two allocator instances concurrently allocated 20 unique sequences starting after MongoDB's highest stored sequence.

Commands:

```sh
cd backend
npm run lint
npm run typecheck
npm test
cd ..
docker compose --parallel 1 build gateway-1 processor-1 integration
docker compose --parallel 1 up -d --wait --wait-timeout 600
docker compose --parallel 1 run --rm integration
```

This is correctness verification rather than a throughput or latency benchmark. Receipts, heartbeat TTL presence, the reconnecting test client, rate limiting, and dead-letter behavior remain Phase 3 work. No remote CI, chaos, load, or frontend result is claimed here.
