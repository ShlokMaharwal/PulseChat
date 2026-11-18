# PulseChat design

The backend consists of independently running WebSocket/HTTP gateways, Kafka consumers, MongoDB persistence, and Redis routing. The browser is a separate application and shares only `PROTOCOL.md` with the backend.

## Message ownership and order

Gateways validate authentication, membership, and input before producing. They use `chatId` as the Kafka record key, so all records for a chat are assigned to one of the six `messages` partitions. A gateway serializes incoming message operations per socket, preserving that client's send order. Across gateways, the chat's order is Kafka's append order, rather than wall-clock timestamps.

Processors share one consumer group and process each assigned partition sequentially. Independent partitions can run concurrently. Auto-commit and automatic batch offset resolution are disabled. The processor resolves an offset only after its message has persisted, and explicitly commits the next offset after the successfully processed batch. A crash before commit replays records; MongoDB and sequence idempotency make replay safe. A stale batch does not commit offsets from an old assignment. Consumer group rebalances move partition ownership; they do not make timestamps the ordering authority.

## Identity, sequence idempotency, and Redis recovery

A logical message has a UUIDv5 derived from the sender's UUID and `clientMsgId`. Different users can reuse a client UUID without colliding. A retry must retain its chat/body; conflicting reuse is rejected when identified. MongoDB has a unique `messageId` index and a unique `(chatId, seq)` index.

Sequence assignment is a Redis Lua operation. It first reads `chat:{chatId}:msgseq` for `messageId`. An existing mapping is returned unchanged. Otherwise the script increments `chat:{chatId}:seq`, stores the message-to-sequence mapping in the hash, and returns it. Both mutations are atomic, and mappings have no TTL. Failing after assignment but before insertion does not allocate another sequence on retry.

The script returns a seeding request instead of incrementing an absent counter. An initializer acquires a tokenized `chat:{chatId}:seq:init` lease, reads the chat's highest persisted sequence using MongoDB's compound index, then seeds and assigns atomically in Lua. The script verifies the same lease token before seeding and uses `SET NX` for the counter. Other initializers wait or use the initialized counter. The lease expires after a crashed initializer; release compares the token before deleting. If Redis restarts between the MongoDB read and the Lua call, the lost lease prevents the stale seed from being used, and initialization repeats with a fresh MongoDB read.

The processor checks for an already persisted message before allocating. A duplicate-key insertion is treated as success only when the canonical `messageId` can be read back and its sender, chat, client ID, and body agree. A conflicting `(chatId, seq)` error without that canonical message remains a processing failure. MongoDB's `_id` is never sent in the protocol.

Exhausting retries after a sequence was assigned leaves a permanent sequence gap. Clients recover using ordered, authoritative MongoDB sync pages and do not require every integer to exist. A numeric gap triggers recovery; it does not justify inventing or renumbering a message.

## Delivery boundaries

`received` acknowledges Kafka acceptance with `acks=-1`. `sent` acknowledges persistence. These boundaries are different from delivery to a browser. Redis routes a user's live envelopes to the owning gateway, which pushes to its local sockets. Offline users are skipped after persistence. Pub/Sub acknowledgment does not prove browser receipt, so persisted history and client sync are the recovery mechanism. Client delivery/read acknowledgments travel through Kafka receipts and update MongoDB with monotonic maximum cursors; sync recovers a missed receipt notification.

Storage is effectively once for a logical message under the documented retry and partition-ownership model: stable identity, stable sequence mapping, unique indexes, and offset replay work together. There is no distributed transaction spanning Kafka, MongoDB, Redis, and browsers. A process can die after insertion and before live publication; clients must recover from MongoDB. An infrastructure-level split brain, loss of Kafka's acknowledged local disk, or manual mutation of Redis keys is outside the single-host development guarantee.

## Current local limits

Compose has three gateway instances and two processors, built from one image per role. Kafka has a single KRaft broker with replication factor one; Redis and MongoDB are also single instances. This demonstrates application replication and recovery behavior, rather than infrastructure high availability. Kafka's broker heap is limited to 512 MiB and MongoDB's WiredTiger cache to 0.25 GiB to fit local verification. Functional readiness checks contact the dependencies and verify topic leaders; inexpensive container TCP probes avoid repeatedly starting diagnostic runtimes.

The local broker, Redis, and MongoDB are single points of infrastructure failure. Application replicas tolerate process failures within the tested model; the Compose stack does not claim multi-host high availability.

## Reliability and failure boundaries

A record receives five retries after its first attempt, with bounded exponential delays and Kafka heartbeats. After an operation fails, dependency health is checked before spending an attempt. Unhealthy Redis, MongoDB, or Kafka pauses the record with heartbeats and no offset resolution; the poison retry budget is preserved until infrastructure recovers. Shutdown or stale ownership leaves the record unresolved for replay. A persistent failure with healthy dependencies exhausts the budget and publishes a base64-preserving envelope containing original topic, partition, offset, key/value, attempt count, error, and timestamp to `messages.dlq` with `acks=-1`. Only persistence or acknowledged DLQ storage permits resolving its source offset. Failed DLQ publication retains ownership and retries without committing. Stale assignments do not commit. A poison record cannot block its partition after durable dead-lettering. DLQ replay is a deliberate operator repair: replaying an old assigned sequence after clients have advanced can backfill below their cursor, requiring a full-history reload. Automatic replay is not promised.

Redis routing uses the required `user:{id}:gateway` hint plus a TTL member key for each gateway and a discoverable gateway set. Fan-out validates member keys, so simultaneous sessions on separate gateways receive the same envelope. Compare-and-remove cleanup preserves another gateway's membership. Presence and routing TTLs refresh on client heartbeat and native pong; a killed gateway cannot refresh them. Peer snapshots detect expired presence, including crashes where no offline event could be published.

Rate admission uses Redis TIME inside one Lua token-bucket operation per user. Requests are charged on arrival before the per-socket send queue. Separate message and control queues preserve send ordering while allowing heartbeats and sync during Kafka delays. Incoming frame size, queue length, paginated response size, and buffered output are bounded. JWT expiry is checked after upgrade and during the connection lifetime.

The test client tracks an ordered cursor, buffers live gaps, and paginates authoritative MongoDB history. It accepts gaps in authoritative pages because a DLQ record may own a sequence permanently. Reconnect uses capped exponential backoff with jitter. Sends without an ACK retain their original ID; a received ACK transfers responsibility to Kafka. Periodic discovery/sync repairs a missed final Pub/Sub message, and reconnect discovers chats created while offline. Receipt cursors replay after reconnect or on heartbeat, so a lost receipt notification is recoverable.

## Kafka, Streams, and Pub/Sub trade-offs

Kafka gives the message pipeline durable partition logs, explicit offset ownership, consumer-group rebalancing, and independent receipt/DLQ streams. Redis Streams could simplify deployment and offer consumer groups, but would put ephemeral routing, rate limiting, sequence state, and the durable event log in one operational failure domain. This project accepts Kafka's higher local memory and operational cost to demonstrate partition ownership and replay. A single broker still has no replicated-disk protection.

Redis Pub/Sub is a low-latency delivery hint. It has neither replay nor an acknowledgment proving that a browser received an envelope. MongoDB remains the durable query source: reconnect, detected numeric gaps, and periodic tail sync repair missed notifications. A gateway dying after a Kafka ACK or a processor dying after insertion can therefore be recovered without pretending Pub/Sub is durable. The cost is an additional database, recovery traffic, and bounded recovery delay.

## Failure modes

| Component or boundary | Impact | Recovery |
| --- | --- | --- |
| Gateway hard crash | Its sockets close; live envelopes to those sockets can be missed | Reconnect through another gateway, reuse unacked IDs, sync from the cursor; presence/routing expire after 30 seconds |
| Gateway SIGTERM | New upgrades rejected while accepted work drains | Reconnect hint, bounded drain, 1012 close, compare-and-remove routing cleanup |
| Processor dies before insert | Kafka offset remains unresolved | New owner replays the record; Lua mapping reuses its sequence |
| Processor dies after insert, before commit or publish | A replay or lost live notification is possible | Unique message ID and canonical-record validation dedupe storage; sync repairs missing delivery |
| Consumer-group rebalance | Partitions change owners; an old batch can become stale | Sequential processing within each owned partition, stale-batch checks, manual commits, and idempotent replay |
| Redis restart | Routing, sequence access, admission, and Pub/Sub are temporarily unavailable | Reconnect subscriptions, heartbeat refresh, retryable gateway errors, MongoDB sync, and lease-guarded sequence reseeding if counters are absent |
| Kafka briefly paused | Producers/consumers wait and readiness fails while liveness remains available | Broker resumes, request retries finish, and committed-offset replay continues |
| MongoDB unavailable | Persistence cannot finish; accepted records retain their Kafka offsets | Dependency-aware waiting preserves the poison budget; processing resumes after recovery |
| Poison or permanently failing record | Its chat partition waits through the retry budget | Durable DLQ envelope permits advancing the source offset; assigned sequence gaps remain visible to sync |
| Pub/Sub notification lost | A live message or receipt update can be missing | Periodic and reconnect sync read canonical messages and receipt cursors |
| Slow socket or abusive input | Memory and event-loop pressure | Bounded input, arrival-time token bucket, capped queues/output, 1009/1013 closes |

## Guarantees and limits

Storage is effectively once for the same sender/client ID under the tested crash/retry model, rather than an end-to-end distributed exactly-once transaction. Kafka append order defines a chat's processing order. Multiple senders, multiple gateways, and concurrent retries can choose a different append order than UI wall-clock or application call order. Clients display canonical sequence order and suppress replayed callbacks. Independent chats can interleave freely across partitions.

A terminal DLQ record is an operator-visible failure, not a silently successful chat delivery. Dependency outages stop progress until recovery; durability still depends on Kafka retention and acknowledged storage surviving the outage. A persistent operation failure while dependency probes succeed can exhaust the poison budget. Manual DLQ replay below a client's saved cursor needs a full-history reload. Redis sequence mappings grow without a retention policy. JWTs have fixed expiry without refresh/revocation; HTTP authentication lacks a separate IP-based abuse policy. Chat discovery/history fan out several MongoDB queries, and this implementation has only 1:1 chats.

The local k6 fixture uses repeated sessions across 32 users and 16 chats, so its socket-concurrency results are not a benchmark of that many distinct users. Load-generator and application containers share the same constrained host. Results include the measured target, observed connection count, confirmed-message rate, latency distribution, and unconfirmed sends at shutdown. Capacity, availability, and latency beyond the recorded runs are not claimed.

Future work includes multi-broker replication, MongoDB replica sets and stronger durability settings, managed Redis failover, fenced ownership for prolonged split-brain cases, bounded sequence-map retention with a replay contract, cursor repair tooling for DLQ backfills, optimized chat queries, distinct-user load profiles, token refresh/revocation, and production TLS/secret management.
