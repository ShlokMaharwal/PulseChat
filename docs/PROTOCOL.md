# PulseChat protocol

This document is the sole contract between the independent backend and frontend applications. Backend shared types are not imported into the frontend. JSON uses UTF-8, UUID identifiers, ISO 8601 timestamps, and safe integer sequence numbers. Sequence numbers are scoped to a chat and begin at 1.

Implementation status: backend Phases 1–5 and the independent React frontend implement this contract. The browser SDK handles reconnect, persisted cursors, ordered sync, offline queuing, and cumulative receipts. Phase verification documents record the tests and measured limitations.

## HTTP

Production requests use the same origin. Nginx routes `/api/*` to gateways and `/ws` to WebSocket gateways. Direct gateway requests omit the `/api` prefix. Successful JSON responses use `Content-Type: application/json`. Failures use `{ "error": { "code": "FORBIDDEN", "message": "Access denied" } }`; operational health endpoints use their documented bodies below.

Authenticated REST requests include `Authorization: Bearer <token>`. HS256 access tokens expire after at most 900 seconds and are never passed in URL query parameters. Registration and login return `{ "token": "<jwt>", "refreshToken": "<opaque-secret>", "expiresIn": 900, "user": { "id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "username": "alice" } }`. Refresh secrets have an absolute seven-day expiry by default. Each use rotates the secret atomically; reuse of a spent secret revokes its family. Logout revokes the family. Existing access JWTs remain valid until their short expiry; immediate access-token revocation is not claimed. Browser credentials stay in tab-scoped session storage, and silent renewal is shared across concurrent HTTP requests.

| Method and external path | Request | Response |
| --- | --- | --- |
| `POST /api/auth/register` | `{ "username": "alice", "password": "a-long-password" }` | `201`, authentication result |
| `POST /api/auth/login` | Same fields | `200`, authentication result |
| `POST /api/auth/refresh` | `{ "refreshToken": "<opaque-secret>" }` | `200`, a new authentication result; old refresh secret is spent |
| `POST /api/auth/logout` | Same refresh field | `200`, `{ "revoked": true }`; clears the browser session after confirmation |
| `GET /api/users?query=ali` | Authenticated; username prefix search | `200`, `{ "users": [{ "id": "...", "username": "alice" }] }` |
| `POST /api/chats` | Authenticated; `{ "userId": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" }` | `201` for a new 1:1 chat or `200` for the existing pair; `{ "chat": { "id": "...", "members": [{ "id": "...", "username": "..." }] } }` |
| `GET /api/chats` | Authenticated | `200`, `{ "chats": [...] }`, each with `id`, `members`, `lastMessage` (message or null), `lastSeq`, `createdAt`, `unreadCount`, and `receipts` |
| `GET /api/chats/:chatId/messages?afterSeq=0&limit=100` | Authenticated chat member; ascending history | `200`, `{ "messages": [...], "hasMore": false, "nextAfterSeq": 12 }` |
| `GET /api/users/:userId/presence` | Authenticated self or chat peer | `200`, `{ "userId": "...", "online": true }` |
| `GET /api/healthz` | No authentication | `200`, `{ "status": "alive", "service": "gateway", "instanceId": "..." }` |
| `GET /api/readyz` | No authentication | `200` when ready, otherwise `503`; includes `status`, `service`, `instanceId`, and, after startup, `dependencies: { "redis": true, "mongo": true, "kafka": true }` |
| `GET /api/metrics` | No authentication; operational endpoint | Prometheus text for the selected gateway |
| `GET /healthz` | Nginx liveness | `200`, `{ "status": "alive", "service": "nginx" }` |
| `GET /readyz` | Nginx proxy to gateway readiness | Same readiness response as `/api/readyz` |

Processors expose direct `/healthz`, `/readyz`, and `/metrics` on internal port 8080. Every backend readiness check contacts Redis and MongoDB and verifies Kafka topic metadata and leaders. Health endpoints permit GET only; unsupported methods return `405` and `Allow: GET`. Unknown endpoints return `404`.

Usernames contain 3–32 ASCII letters, digits, or underscores, and are normalized for uniqueness and login. Registration passwords have at least 10 characters and at most 72 UTF-8 bytes, the bcrypt input limit. Login enforces the same byte ceiling. A user cannot create a chat with themselves or an unknown user. A pair has at most one 1:1 chat. History pagination accepts `afterSeq >= 0` and `1 <= limit <= 100`.

Authentication endpoints share a Redis IP window (120 requests/minute by default). Login/register additionally share normalized username windows (ten attempts/minute). Credential failures apply exponential username backoff and a fifteen-minute lock after five failures. A `429` includes `Retry-After` seconds and `error.retryAfterMs`. Production rejects default/weak startup secrets. See [security setup and limits](verification/task-4.md).

HTTP application error codes: `INVALID_REQUEST` (400), `UNAUTHORIZED` (401), `FORBIDDEN` (403), `NOT_FOUND` (404), `CONFLICT` (409), `PAYLOAD_TOO_LARGE` (413), `RATE_LIMITED` (429), `UNAVAILABLE` (503), and `INTERNAL_ERROR` (500). Credentials are not echoed in errors. Chat non-members receive `FORBIDDEN` without message contents.

## WebSocket authentication

Open `/ws` with subprotocols `pulsechat` and `jwt.<token>`, and an exact allowlisted HTTP(S) `Origin`. Missing and unapproved origins receive `403`, including native clients; configure `WS_ALLOWED_ORIGINS` and send that header explicitly. Browsers supply it automatically. For example:

```js
new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws`, ['pulsechat', `jwt.${token}`]);
```

The server selects `pulsechat` as the response subprotocol. It does not echo the JWT. Invalid or expired credentials reject the HTTP upgrade with `401`. The connection is authenticated before it can send application messages. Each text frame contains exactly one JSON object. Binary frames are unsupported. The default complete-frame maximum is 16 KiB, including JSON framing; body UTF-8 bytes must fit within that limit.

On acceptance, the server sends `{ "type": "welcome", "userId": "...", "gatewayId": "...", "heartbeatIntervalMs": 15000 }`. Multiple sessions on different gateways all receive live messages. Routing and presence expire after 30 seconds without a heartbeat or WebSocket pong. An expired JWT closes an existing socket with 4001.

## Sending messages

Client:

```json
{"type":"message","clientMsgId":"11111111-1111-4111-8111-111111111111","chatId":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","body":"Hello"}
```

`clientMsgId` is generated once for a logical send and is reused unchanged on retries. Retrying with different chat/body values is a conflict. The gateway authenticates membership and validates before producing to Kafka with `key = chatId`. After Kafka accepts the record:

```json
{"type":"ack","clientMsgId":"11111111-1111-4111-8111-111111111111","status":"received"}
```

`received` means durable acceptance by the configured Kafka broker, rather than persistence in MongoDB. After processing and persistence:

```json
{"type":"ack","clientMsgId":"11111111-1111-4111-8111-111111111111","status":"sent","messageId":"22222222-2222-4222-8222-222222222222","seq":1}
```

Recipients receive a canonical message:

```json
{"type":"message","messageId":"22222222-2222-4222-8222-222222222222","clientMsgId":"11111111-1111-4111-8111-111111111111","chatId":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","senderId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","body":"Hello","seq":1,"createdAt":"2026-11-18T00:00:00.000Z"}
```

`messageId` is server-controlled and stable for a sender's `clientMsgId`. `seq` is assigned idempotently by the processor. Clients merge retries by message ID and display stored messages in sequence order. Kafka offsets are committed manually after successful persistence. Redis Pub/Sub provides live delivery; MongoDB history is the source for recovery.

Gateways may coalesce message and receipt records into bounded Kafka batches.
Every `received` ACK still waits for its batch's `acks=-1` result. A failed or
uncertain batch rejects its waiting operations; retrying the same logical ID is
safe. The gateway preserves each socket's operation chain, and every message
retains its chat partition key. Batch size does not change canonical storage or
sequence assignment.

### Optional diagnostic traces

With `TRACE_MESSAGES=true`, received ACKs and live message frames may contain a
server-generated `trace` object. Its values are epoch milliseconds. Diagnostic
fields do not appear in canonical stored messages or history/sync results. Client
frames cannot supply a trace object.

| Field | Observation |
| --- | --- |
| `gatewayReceivedAt` | Origin gateway receives the frame |
| `produceStartedAt` | Validated operation enters the durable publishing path |
| `kafkaAcknowledgedAt` | Batch is acknowledged; carried on the received ACK |
| `processorConsumedAt` | Consumer enters the fetched batch |
| `processorStartedAt` | Processing starts for this record |
| `sequenceStartedAt`, `sequenceAssignedAt` | Sequence allocation boundaries |
| `mongoPersistStartedAt`, `mongoPersistedAt` | Canonical insert boundaries |
| `redisPublishStartedAt` | Live publication begins for this recipient |
| `gatewayPushedAt` | Recipient gateway submits the frame to its socket |

Producer acknowledgement and processor consumption can overlap. Clock changes
can invalidate cross-process spans. The benchmark reports omitted negative
samples and the number of live traces behind each stage percentile; sync-based
recovery can deliver a message without a live trace. See
[performance measurement](PERFORMANCE.md) for timing and completeness rules.

## Receipts and read position (Phase 3)

After ingesting a message, a client sends `{ "type": "delivered", "chatId": "...", "messageId": "..." }`. When the user reads through a position, it sends `{ "type": "read", "chatId": "...", "lastReadSeq": 12 }`. Read positions are monotonic and cannot exceed the chat's stored messages. Receipt updates are persisted and delivered to the original sender:

```json
{"type":"receipt","chatId":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","userId":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","status":"read","seq":12,"timestamp":"2026-11-18T00:00:01.000Z"}
```

`status` is `delivered` or `read`. A repeated receipt does not regress stored state. Chat lists, HTTP history, and WebSocket sync responses include `receipts: [{ "userId": "...", "deliveredSeq": 12, "readSeq": 12 }]`. Both cursors are cumulative and monotonic. Read implies delivered; clients acknowledge delivered only after ordered ingestion. A client can acknowledge only a peer message in an authorized chat. Receipts travel through the `receipts` Kafka topic before MongoDB persistence and fan-out.

## Sync and gap recovery (Phase 3)

Client: `{ "type": "sync", "chatId": "...", "afterSeq": 12, "limit": 100 }`. `limit` defaults to 100. Response:

```json
{"type":"sync","chatId":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","messages":[],"hasMore":false,"nextAfterSeq":12,"receipts":[]}
```

`messages` contains canonical stored-message objects without a `type` field, sorted ascending, strictly after the requested cursor. Request the next page using `nextAfterSeq` while `hasMore` is true. Empty pages preserve the requested cursor. Pages are also bounded by encoded bytes; request another page while `hasMore` is true, even if fewer than `limit` messages arrived. Sync pages are authoritative over persisted messages: sequence numbers can have a permanent gap after a record goes to the DLQ, so clients must not wait forever for every integer to exist.

Clients keep a cursor per user and chat, resync on reconnect and live gaps, deduplicate replayed message IDs, and resend pending sends with the same `clientMsgId`. The backend test client resends sends lacking an ACK; after `received`, Kafka owns delivery and sync discovers the persisted result. Retaining a send until `sent` is also safe because processing is idempotent. A periodic 15-second sync discovers lost Pub/Sub tails even without a later live gap. Unsent pending payloads and sequence cursors are scoped to the authenticated user. A user must still be authorized for every sync request.

## Heartbeat, presence, and shutdown (Phase 3)

Every 15 seconds the client sends `{ "type": "heartbeat" }`; the server responds with the same frame and refreshes presence and routing TTLs. Presence notifications use `{ "type": "presence", "userId": "...", "online": true }`. Native WebSocket pings/pongs also refresh presence. Peer snapshots accompany heartbeats, so crashed-peer offline status becomes visible through snapshots or authenticated presence queries after the TTL. A routing entry or old connection does not by itself establish online status after the presence TTL expires.

Before graceful gateway shutdown, the server sends `{ "type": "reconnect", "retryAfterMs": 500 }`, drains accepted work, and closes with `1012`. Clients reconnect with exponential backoff and jitter, resync tracked chats, and retry pending sends.

## WebSocket errors and close codes

Errors use `{ "type": "error", "code": "RATE_LIMITED", "message": "Too many requests", "clientMsgId": "...", "retryAfterMs": 1000 }`. `clientMsgId`, `retryAfterMs`, `chatId`, and `operation` (`message`, `sync`, `delivered`, or `read`) are optional and correlate retryable operations. The default per-user Redis token bucket has capacity 100 and refills 50 tokens/second across every gateway: send costs 1, sync 5, receipt 0.2, heartbeat is exempt. Admission happens when a frame arrives, before queued persistence work. Each socket permits at most 256 queued operations and 1 MiB buffered output by default. Codes are `INVALID_MESSAGE`, `UNAUTHORIZED`, `FORBIDDEN`, `CONFLICT`, `RATE_LIMITED`, `PAYLOAD_TOO_LARGE`, `UNAVAILABLE`, and `INTERNAL_ERROR`. Invalid JSON or unauthorized chat operations do not enter Kafka. Rate-limited clients respect the retry delay and reuse the logical send ID.

| Close code | Meaning | Client action |
| --- | --- | --- |
| `1000` | Intentional normal close | Stop unless explicitly reconnecting |
| `1003` | Unsupported binary data | Correct the client |
| `1007` | Invalid UTF-8 | Correct the client |
| `1009` | Oversized frame | Do not resend the oversized frame |
| `1012` | Gateway service restart | Reconnect and sync |
| `1013` | Socket backpressure / server overload | Back off, reconnect, and sync |
| `4001` | Authentication expired or invalid after upgrade | Obtain a valid token before reconnecting |

An abnormal network closure can appear as client-observed `1006`; it is not transmitted as a close frame. Recovery uses the same reconnect, sync, and resend path.
