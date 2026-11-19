# Testing and development

The backend and frontend have independent dependencies and builds. Host checks require Node.js 22.13+ and npm; containers use Node.js 22. Start the stack using the root README before functional tests.

## Source checks

Run in both `backend/` and `frontend/`:

```sh
npm ci
npm run lint
npm run format:check
npm run typecheck
npm test
npm run build
```

From the repository root, validate documentation links and run real integration/smoke checks:

```sh
node tools/check-links.mjs
docker compose --parallel 1 build integration
docker compose --profile test run --rm --no-deps integration
docker compose run --rm --no-deps smoke
```

From `backend/`:

```sh
npm run test:presence
npm run test:observability
npm run test:chaos
npm run test:load -- 200
```

Chaos scripts restore interrupted containers and save PASS/FAIL summaries under `backend/.cache/chaos/`. Run one scenario with `npx tsx chaos/run.ts gateway`, `processor`, `redis`, or `kafka`. Historical results remain under [verification](verification/); each artifact describes its own run rather than a universal crash count.

The load runner uses distinct users and 1:1 chats, classifies acceptance after verified drain, and stores raw private fixture files under ignored `backend/.cache/loadtest/`. Follow [PERFORMANCE](PERFORMANCE.md) for comparison settings, profiling, safe source snapshots, and native generator output. Larger targets are overload probes, not presumed supported capacities.

If port 8080 is occupied, set `PULSECHAT_PORT=8081` for Compose and `TEST_HTTP_URL=http://127.0.0.1:8081` for host tests. When using the performance override, set `COMPOSE_FILE` to both Compose files for host chaos commands so restart operations use the same project configuration; the separator is `:` on Unix and `;` on Windows. Do not overlap load profiles with other verification traffic.

## Development

For host backend development, start `kafka redis mongo` through Compose, set `JWT_SECRET` to at least 32 characters, then run `npm run dev:gateway` or `npm run dev:processor` in `backend/`. Host defaults are Kafka port 19092, Redis 16379, MongoDB 27018; direct-gateway REST paths omit `/api`.

For the frontend, keep Compose running and run `npm run dev` in `frontend/`. Open `http://127.0.0.1:5173`; Vite proxies REST/WebSocket requests to port 8080. See [frontend documentation](../frontend/README.md) for proxy overrides and session storage behavior.

## Numeric evidence

The older [processor run](verification/chaos-results/processor.json) crashed at 41 persisted / 79 queued. The later [final run](verification/final-chaos-results/processor.json) crashed at 85 persisted / 35 queued. Both had 120 sends and zero final loss. These were different runs; original result files are preserved. The [new performance verification](verification/task-1-chaos/processor.json) crashed at 80 persisted / 40 queued. Resume bullets must identify the evidence set they describe.
