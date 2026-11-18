# PulseChat protocol

This document is the sole contract between the independent backend and frontend applications. Backend shared types are not imported into the frontend. JSON uses UTF-8, UUID identifiers, ISO 8601 timestamps, and safe integer sequence numbers. Sequence numbers are scoped to a chat and begin at 1.

Implementation status: health and metrics endpoints passed Phase 1. Authentication, chats, core messaging, and ordered history pagination are implemented and verified in Phase 2. Receipts, TTL presence, and automatic reconnect/gap-recovery clients are Phase 3 work. The frontend follows the backend proof phases.

## HTTP

Production requests use the same origin. Nginx routes `/api/*` to gateways and `/ws` to WebSocket gateways. Direct gateway requests omit the `/api` prefix. Successful JSON responses use `Content-Type: application/json`. Failures use `{ "error": { "code": "FORBIDDEN", "message": "Access denied" } }`; operational health endpoints use their documented bodies below.

Authenticated REST requests include `Authorization: Bearer <token>`. Tokens are signed with HS256, have a fixed expiry, and are never passed in URL query parameters. Registration and login return `{ "token": "<jwt>", "expiresIn": 86400, "user": { "id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "username": "alice" } }`.

| Method and external path | Request | Response |
| --- | --- | --- |
| `POST /api/auth/register` | `{ "username": "alice", "password": "a-long-password" }` | `201`, authentication result |
| `POST /api/auth/login` | Same fields | `200`, authentication result |
| `GET /api/users?query=ali` | Authenticated; username prefix search | `200`, `{ "users": [{ "id": "...", "username": "alice" }] }` |
| `POST /api/chats` | Authenticated; `{ "userId": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" }` | `201` for a new 1:1 chat or `200` for the existing pair; `{ "chat": { "id": "...", "members": [{ "id": "...", "username": "..." }] } }` |
| `GET /api/chats` | Authenticated | `200`, `{ "chats": [...] }`, each with `id`, `members`, `lastMessage` (message or null), and `lastSeq` |
| `GET /api/chats/:chatId/messages?afterSeq=0&limit=100` | Authenticated chat member; ascending history | `200`, `{ "messages": [...], "hasMore": false, "nextAfterSeq": 12 }` |
| `GET /api/healthz` | No authentication | `200`, `{ "status": "alive", "service": "gateway", "instanceId": "..." }` |
| `GET /api/readyz` | No authentication | `200` when ready, otherwise `503`; includes `status`, `service`, `instanceId`, and, after startup, `dependencies: { "redis": true, "mongo": true, "kafka": true }` |
| `GET /api/metrics` | No authentication; operational endpoint | Prometheus text for the selected gateway |
| `GET /healthz` | Nginx liveness | `200`, `{ "status": "alive", "service": "nginx" }` |
| `GET /readyz` | Nginx proxy to gateway readiness | Same readiness response as `/api/readyz` |

Processors expose direct `/healthz`, `/readyz`, and `/metrics` on internal port 8080. Every backend readiness check contacts Redis and MongoDB and verifies Kafka topic metadata and leaders. Health endpoints permit GET only; unsupported methods return `405` and `Allow: GET`. Unknown endpoints return `404`.

Usernames contain 3–32 ASCII letters, digits, or underscores, and are normalized for uniqueness and login. Registration passwords have at least 10 characters and at most 72 UTF-8 bytes, the bcrypt input limit. Login enforces the same byte ceiling. A user cannot create a chat with themselves or an unknown user. A pair has at most one 1:1 chat. History pagination accepts `afterSeq >= 0` and `1 <= limit <= 100`.

HTTP application error codes: `INVALID_REQUEST` (400), `UNAUTHORIZED` (401), `FORBIDDEN` (403), `NOT_FOUND` (404), `CONFLICT` (409), `PAYLOAD_TOO_LARGE` (413), `RATE_LIMITED` (429), `UNAVAILABLE` (503), and `INTERNAL_ERROR` (500). Credentials are not echoed in errors. Chat non-members receive `FORBIDDEN` without message contents.

## WebSocket authentication

Open `/ws` with subprotocols `pulsechat` and `jwt.<token>`, for example:

```js
new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws`, ['pulsechat', `jwt.${token}`]);
```

The server selects `pulsechat` as the response subprotocol. It does not echo the JWT. Invalid or expired credentials reject the HTTP upgrade with `401`. The connection is authenticated before it can send application messages. Each text frame contains exactly one JSON object. Binary frames are unsupported. The default complete-frame maximum is 16 KiB, including JSON framing; body UTF-8 bytes must fit within that limit.

On acceptance, the server sends `{ "type": "welcome", "userId": "...", "gatewayId": "...", "heartbeatIntervalMs": 15000 }`. Phase 3 adds complete multi-gateway routing for multiple connections per user and refreshes routing/presence by heartbeat with a 30-second TTL.

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

## Receipts and read position (Phase 3)

After ingesting a message, a client sends `{ "type": "delivered", "chatId": "...", "messageId": "..." }`. When the user reads through a position, it sends `{ "type": "read", "chatId": "...", "lastReadSeq": 12 }`. Read positions are monotonic and cannot exceed the chat's stored messages. Receipt updates are persisted and delivered to the original sender:

```json
{"type":"receipt","chatId":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","userId":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","status":"read","seq":12,"timestamp":"2026-11-18T00:00:01.000Z"}
```

`status` is `delivered` or `read`. A repeated receipt does not regress stored state. Offline senders recover persisted receipt state through chat/history responses when reliability support is completed.

## Sync and gap recovery (Phase 3)

Client: `{ "type": "sync", "chatId": "...", "afterSeq": 12, "limit": 100 }`. `limit` defaults to 100. Response:

```json
{"type":"sync","chatId":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","messages":[],"hasMore":false,"nextAfterSeq":12}
```

`messages` contains canonical stored-message objects without a `type` field, sorted ascending, strictly after the requested cursor. Request the next page using `nextAfterSeq` while `hasMore` is true. Empty pages preserve the requested cursor. Sync pages are authoritative over persisted messages: sequence numbers can have a permanent gap after a record goes to the DLQ, so clients must not wait forever for every integer to exist.

Clients keep a cursor per user and chat, resync on reconnect and live gaps, deduplicate replayed message IDs, and resend pending sends with the same `clientMsgId`. A `received` acknowledgment can precede a reconnect; retaining a pending send until `sent` is safe because processing is idempotent. A user must still be authorized for every sync request.

## Heartbeat, presence, and shutdown (Phase 3)

Every 15 seconds the client sends `{ "type": "heartbeat" }`; the server responds with the same frame and refreshes presence and routing TTLs. Presence notifications use `{ "type": "presence", "userId": "...", "online": true }`. A routing entry or old connection does not by itself establish online status after the presence TTL expires.

Before graceful gateway shutdown, the server sends `{ "type": "reconnect", "retryAfterMs": 500 }`, drains accepted work, and closes with `1012`. Clients reconnect with exponential backoff and jitter, resync tracked chats, and retry pending sends.

## WebSocket errors and close codes

Errors use `{ "type": "error", "code": "RATE_LIMITED", "message": "Too many requests", "clientMsgId": "...", "retryAfterMs": 1000 }`. `clientMsgId` and `retryAfterMs` are optional. Codes are `INVALID_MESSAGE`, `UNAUTHORIZED`, `FORBIDDEN`, `CONFLICT`, `RATE_LIMITED`, `PAYLOAD_TOO_LARGE`, `UNAVAILABLE`, and `INTERNAL_ERROR`. Invalid JSON or unauthorized chat operations do not enter Kafka. Rate-limited clients respect the retry delay and reuse the logical send ID.

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
