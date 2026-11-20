# PulseChat frontend

An independent React 18 + TypeScript application. It imports no backend modules. Its HTTP and WebSocket types are written from `../docs/PROTOCOL.md`.

Choose **New conversation → New group** to name a group and select 2–19 other people. Direct chats and groups show live typing; groups also show sender names and receipts from all other members. Login and registration include an accessible show/hide password control. Chat messages and form inputs use a 16px system font in both themes.

Use Node.js 22.13+ and npm for the current lint and build tooling.

## Run

With the Compose stack running at port 8080:

```sh
npm ci
npm run dev
```

Open `http://127.0.0.1:5173`. Vite proxies `/api` and `/ws` to Nginx at `http://localhost:8080`. The production build is served by the frontend Nginx container behind the same root reverse proxy, so browser requests stay on one origin.

To develop against a host-run gateway directly in PowerShell:

```powershell
$env:PULSECHAT_PROXY_TARGET = 'http://localhost:8090'
$env:PULSECHAT_DIRECT_GATEWAY = '1'
npm run dev
```

`PULSECHAT_DIRECT_GATEWAY` removes the `/api` prefix for direct gateway REST requests. Keep it unset when targeting the root Nginx proxy.

## Checks

```sh
npm run lint
npm run format:check
npm run typecheck
npm test
npm run build
npm run test:e2e
```

The Vitest suite uses mock WebSockets to verify authentication protocols, ordered gap recovery, duplicate suppression, authoritative pagination across DLQ sequence gaps, reconnect/resend identity, persisted cursors, user isolation, receipt monotonicity, reconnect hints, deliberate shutdown, rate retry, storage failure, and typing throttling/expiry across sessions. Typing is ephemeral, clears on disconnect, and is never queued for replay.

## Recovery and browser state

`src/sdk/client.ts` owns the connection lifecycle. It reconnects with capped exponential backoff and jitter, resends sends lacking an ACK with the same UUID, and syncs from per-chat sequence cursors on welcome, a live gap, and periodic heartbeats. Browser offline events immediately retire the old socket and queue sends; returning online opens a new connection. Received ACKs transfer persistence responsibility to Kafka; history recovers a missed canonical delivery. Pending sends remain visible until their canonical record appears.

Cursors and pending sends use user-scoped localStorage keys. Authentication is held in sessionStorage. On a page reload, the UI hydrates ordered REST history before opening the socket, so a saved cursor cannot produce an empty conversation view. The immediate welcome sync covers messages that arrive during hydration. Receipts are cumulative, monotonic, debounced, and replayed after reconnect. Reading a visible conversation advances its read position; a background or unopened conversation keeps its unread badge.

Themes default to `prefers-color-scheme`. A visible toggle saves the selected theme. Dark surfaces use the exact neutral tokens from the task, with `#22C55E` as the shared accent. Read status also includes a text label, so color is not the only way to distinguish it.
