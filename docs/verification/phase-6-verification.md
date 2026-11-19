# Frontend and final verification

Date: 19 November 2026 (Asia/Kolkata).

The frontend is an independent React 18, Vite, and TypeScript application with plain CSS. It consumes only the documented HTTP/WebSocket contract. Nginx serves the application and proxies `/api` and `/ws` on the same origin at `http://localhost:8080`.

## Source and container checks

Frontend lint, formatting, strict types, nine mock-WebSocket SDK tests, and the production build passed. Backend lint, formatting, strict types, build, and all 63 unit tests passed after the recovery fix described below. Both applications' production dependency audits reported zero advisories; this does not claim that development dependencies are advisory-free. The frontend production Docker image builds successfully, and Compose includes its health check and proxy route.

The SDK tests cover ordered gap recovery and deduplication, authoritative pagination across DLQ sequence gaps, authentication protocols, reconnect identity, persisted user-scoped cursors, monotonic receipts, reconnect hints, deliberate shutdown, storage failure, oversized sends, retry delays, and explicit offline queuing. Offline browser events retire the old socket immediately; coming online opens a new connection and resends unacknowledged sends with their original UUID.

## Real browser verification

The browser proof passed using headless Microsoft Edge with two isolated browser contexts against the actual production Compose endpoint. Accounts were created through the UI, and WebSocket welcome frames established that the accounts initially connected through different gateways. No mocked server responses were used. The [committed browser summary](phase-6-browser.json) records the final assertions and capture time.

The flow checks registration and login, conversation discovery, unread badges, bidirectional messages, online presence, read receipts, system-selected light/dark themes, and theme persistence after reload. It then takes one browser offline, kills its actual gateway, sends three messages from the other account, queues a send in the offline browser, and reconnects through another gateway. Both views and REST history must agree on all eight ordered messages, with sequence numbers 1–8, no duplicate records, the original queued-send UUID, and a persisted cursor of 8. Reload and sign-out/sign-in must restore the same history.

The layout is checked at 1440 × 1000 and 390 × 844, including mobile navigation and horizontal overflow. Computed dark theme tokens are checked against the exact requested neutral colors. Secondary text and accent-button contrast are checked against WCAG AA's 4.5:1 threshold. Actual screenshots are saved as [dark](../img/chat-dark.png), [light](../img/chat-light.png), and [mobile](../img/chat-mobile.png).

The final browser run on the rebuilt backend restored its killed gateway and reported no JavaScript page errors. Measured contrast was 4.77:1 for dark secondary text on the own-message bubble, 5.02:1 for light secondary text on its bubble, and 9.22:1 for black text on the emerald button. Screenshots were visually inspected after capture. A separate [development-browser proof](phase-6-dev-browser.json) logged in through Vite at port 5173, loaded all eight history messages through its REST proxy, and received a real welcome frame through its WebSocket proxy. The temporary development server was stopped after verification; the production Compose endpoint remains the app's entry point.

During initial cold startup, one registration attempt reached Nginx's 15-second upstream timeout. A subsequent registration succeeded. The final assertions use successful real requests; the transient timeout remains a local resource/startup limitation. An earlier browser run exposed stale connection status during browser offline simulation; explicit offline handling was added and covered by the ninth SDK test before repeating the proof. Failure testing also exposed Nginx's default 60-second upstream connection timeout for WebSocket upgrades. The `/ws` route now uses a three-second connection timeout and bounded upstream retries so a dead gateway cannot hold a new handshake for a minute.

## Final replicated-stack checks

The initial final-suite rerun passed all ten integration cases and eleven smoke checks. Its Redis chaos scenario exposed a longer restart than earlier runs: three accepted records exhausted the poison retry budget while Redis was unavailable and went to the DLQ, leaving 117 of 120 in normal history. The [failed summary](final-chaos-results/redis-before-fix.json) is preserved. Processor retries now check dependency health after an operation fails and retain the offset without spending poison attempts while infrastructure is unavailable. Three new unit cases verify a prolonged outage, exactly six healthy-dependency poison attempts, and lost partition ownership. Existing poison/DLQ integration assertions remain intact. The complete suite was rerun on the rebuilt runtime before the final commit.

After rebuilding, all ten integration cases and eleven smoke checks passed again. The 1,000-message case completed in 51.13 seconds with canonical sequence/order, matching MongoDB records, and no duplicate wire delivery. The poison-record case still verified durable dead-lettering and continuation.

All four chaos scenarios passed on the rebuilt runtime. Redis was then strengthened to stop for an explicit twelve seconds before restarting; that regression check also passed. The final raw summaries are in [final-chaos-results](final-chaos-results), separate from the original Phase 5 results.

| Final scenario | Sent / stored / recovered | Lost / duplicates / order violations | Fault evidence |
| --- | --- | --- | --- |
| Gateway kill | 120 / 120 / 120 | 0 / 0 / 0 | Actual connected gateway killed |
| Processor kill | 120 / 120 / 120 | 0 / 0 / 0 | 85 persisted and 35 queued at the kill |
| Redis outage and counter loss | 125 / 125 / 125 | 0 / 0 / 0 | Twelve-second stop, restart, and reseed through 125 |
| Kafka pause | 120 / 120 / 120 | 0 / 0 / 0 | Broker paused for eight seconds |

The [backend gate](https://github.com/ShlokMaharwal/PulseChat/actions/runs/44274398559) passed before frontend work began. The final GitHub Actions workflow validates both applications in separate jobs and runs all four backend failure scenarios on a clean runner. The workflow badge and repository Actions page show the final commit's remote result.

## Measured limits

The frontend does not change the recorded capacity result: 200 observed baseline sockets and 533 observed peak in a failed 2,000-socket attempt. The stress test reported 17 issued sends absent from MongoDB at its later observation. No successful 2,000–5,000 connection run or subsecond latency is claimed. See [benchmarks](../benchmarks.md) for all counts, timing, errors, and bottlenecks. This local stack uses one Kafka broker, one MongoDB instance, and one Redis instance; replicated application workers do not provide infrastructure high availability.
