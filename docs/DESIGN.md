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

When dead-letter handling is added in Phase 3, exhausting retries after a sequence was assigned leaves a permanent sequence gap. Clients recover using ordered, authoritative MongoDB sync pages and do not require every integer to exist. A numeric gap triggers recovery; it does not justify inventing or renumbering a message.

## Delivery boundaries

`received` acknowledges Kafka acceptance with `acks=-1`. `sent` acknowledges persistence. These boundaries are different from delivery to a browser. Redis routes a user's live envelopes to the owning gateway, which pushes to its local sockets. Offline users are skipped after persistence. Pub/Sub acknowledgment does not prove browser receipt, so persisted history and client sync are the recovery mechanism. Client delivery/read acknowledgments are added in Phase 3.

Storage is effectively once for a logical message under the documented retry and partition-ownership model: stable identity, stable sequence mapping, unique indexes, and offset replay work together. There is no distributed transaction spanning Kafka, MongoDB, Redis, and browsers. A process can die after insertion and before live publication; clients must recover from MongoDB. An infrastructure-level split brain, loss of Kafka's acknowledged local disk, or manual mutation of Redis keys is outside the single-host development guarantee.

## Current local limits

Compose has three gateway instances and two processors, built from one image per role. Kafka has a single KRaft broker with replication factor one; Redis and MongoDB are also single instances. This demonstrates application replication and recovery behavior, rather than infrastructure high availability. Kafka's broker heap is limited to 512 MiB and MongoDB's WiredTiger cache to 0.25 GiB to fit local verification. Functional readiness checks contact the dependencies and verify topic leaders; inexpensive container TCP probes avoid repeatedly starting diagnostic runtimes.

Kafka versus Redis Streams, the complete failure-mode table, chaos evidence, delivery receipt details, benchmark limits, and future work are documented as their corresponding backend phases are completed. No unmeasured availability or throughput claim is made.
