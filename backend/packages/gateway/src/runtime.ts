import type { IncomingMessage } from 'node:http';
import type { Duplex } from 'node:stream';
import { WebSocket, WebSocketServer, type RawData } from 'ws';
import { z } from 'zod';
import {
  clientFrameSchema,
  kafkaMessageSchema,
  messageIdFor,
  serverFrameSchema,
  uuidSchema,
  consumeToken,
  UserRouter,
  applicationMetrics,
  timestamp,
  observeStage,
  DurableProducerBatch,
  type ClientFrame,
  type ServerFrame,
  type ServiceContext,
  type ServiceRuntime,
} from '@pulsechat/shared';
import { AuthService, verifySession, websocketToken } from './auth.js';
import { ChatService } from './chats.js';
import { GatewayHttpApi } from './http.js';
import { HttpError } from './errors.js';

const envelopeSchema = z
  .object({ userId: uuidSchema, payload: serverFrameSchema })
  .strict();

interface Connection {
  socket: WebSocket;
  userId: string;
  chain: Promise<void>;
  pending: number;
  expiresAt: number;
  lastPong: number;
  control: Promise<void>;
  ready: boolean;
  outgoing: ServerFrame[];
  outgoingBytes: number;
}

class GatewayRuntime {
  private readonly auth;
  private readonly chats;
  private readonly http;
  private readonly producer;
  private readonly batcher;
  private readonly subscriber;
  private readonly sockets = new Map<string, Set<Connection>>();
  private readonly socketState = new WeakMap<WebSocket, Connection>();
  private readonly inFlight = new Set<Promise<void>>();
  private readonly cleanup = new Set<Promise<void>>();
  private readonly routeWork = new Map<string, Promise<void>>();
  private readonly wsServer;
  private readonly router;
  private readonly metrics;
  private timer: ReturnType<typeof setInterval> | undefined;
  private closing = false;

  constructor(private readonly context: ServiceContext) {
    const { config, dependencies, logger } = context;
    this.metrics = applicationMetrics(context.registry);
    this.auth = new AuthService(dependencies.db, config);
    this.chats = new ChatService(dependencies.db);
    this.router = new UserRouter(
      dependencies.redis,
      config.INSTANCE_ID,
      config.PRESENCE_TTL_SECONDS,
      (stage, start, end) => observeStage(this.metrics, stage, start, end),
    );
    this.http = new GatewayHttpApi(
      this.auth,
      this.chats,
      config,
      logger,
      this.router,
    );
    this.producer = dependencies.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
    });
    this.batcher = new DurableProducerBatch(this.producer, {
      maxMessages: config.KAFKA_BATCH_MAX_MESSAGES,
      lingerMs: config.KAFKA_BATCH_LINGER_MS,
      queued: (count) => this.metrics.producerQueued.set(count),
      batch: (topic, count) =>
        this.metrics.producerBatch.observe({ topic }, count),
    });
    this.subscriber = dependencies.redis.duplicate({ lazyConnect: true });
    this.subscriber.on('error', (err) => {
      if (!this.closing) logger.warn({ err }, 'Gateway subscriber error');
    });
    this.subscriber.on('message', (_, raw) => {
      try {
        const envelope = envelopeSchema.parse(JSON.parse(raw));
        for (const connection of this.sockets.get(envelope.userId) ?? [])
          this.send(connection.socket, envelope.payload);
      } catch (err) {
        logger.warn({ err }, 'Invalid gateway delivery envelope');
      }
    });
    this.wsServer = new WebSocketServer({
      noServer: true,
      maxPayload: config.MAX_MESSAGE_BYTES,
      perMessageDeflate: false,
      handleProtocols: (protocols) =>
        protocols.has('pulsechat') ? 'pulsechat' : false,
    });
    this.wsServer.on('error', (err) => context.fail(err));
    context.server.on('upgrade', this.onUpgrade);
  }

  private reject(socket: Duplex, status: number, reason: string) {
    if (!socket.destroyed)
      socket.end(
        `HTTP/1.1 ${status} ${reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`,
      );
  }

  private onUpgrade = (
    request: IncomingMessage,
    socket: Duplex,
    head: Buffer,
  ) => {
    socket.on('error', () => undefined);
    void (async () => {
      if (this.closing || !this.context.accepting()) {
        this.reject(socket, 503, 'Service Unavailable');
        return;
      }
      const url = new URL(request.url ?? '/', 'http://gateway');
      if (url.pathname !== '/ws' || url.searchParams.has('token')) {
        this.reject(socket, 404, 'Not Found');
        return;
      }
      const { userId, expiresAt } = verifySession(
        websocketToken(request.headers['sec-websocket-protocol']),
        this.context.config,
      );
      await this.auth.user(userId);
      if (
        socket.destroyed ||
        this.closing ||
        !this.context.accepting() ||
        Date.now() >= expiresAt
      ) {
        this.reject(socket, 503, 'Service Unavailable');
        return;
      }
      this.wsServer.handleUpgrade(request, socket, head, (client) =>
        this.connected(client, userId, expiresAt),
      );
    })().catch((error: unknown) => {
      this.reject(
        socket,
        error instanceof HttpError ? error.status : 503,
        error instanceof HttpError ? 'Unauthorized' : 'Service Unavailable',
      );
    });
  };

  private send(socket: WebSocket, frame: ServerFrame) {
    if (socket.readyState !== WebSocket.OPEN) return;
    const connection = this.socketState.get(socket);
    if (connection && !connection.ready && frame.type !== 'welcome') {
      const encoded = JSON.stringify(frame);
      connection.outgoingBytes += Buffer.byteLength(encoded);
      if (
        connection.outgoingBytes > this.context.config.MAX_SOCKET_BUFFER_BYTES
      )
        socket.close(1013, 'Socket backpressure');
      else connection.outgoing.push(frame);
      return;
    }
    if (frame.type === 'message' && frame.trace) {
      const pushed = timestamp();
      if (frame.trace.redisPublishStartedAt !== undefined)
        observeStage(
          this.metrics,
          'publish_to_gateway_push',
          frame.trace.redisPublishStartedAt,
          pushed,
        );
      frame = { ...frame, trace: { ...frame.trace, gatewayPushedAt: pushed } };
    }
    const encoded = JSON.stringify(frame);
    if (
      socket.bufferedAmount + Buffer.byteLength(encoded) >
      this.context.config.MAX_SOCKET_BUFFER_BYTES
    ) {
      socket.close(1013, 'Socket backpressure');
      return;
    }
    socket.send(encoded, (err) => {
      if (err) this.context.logger.debug({ err }, 'Socket send failed');
    });
    if (frame.type === 'message') this.metrics.outgoing.inc();
  }

  private trackCleanup(work: Promise<void>) {
    const tracked = work
      .catch((err: unknown) =>
        this.context.logger.warn({ err }, 'Socket route cleanup failed'),
      )
      .finally(() => this.cleanup.delete(tracked));
    this.cleanup.add(tracked);
  }

  private async removeRoute(userId: string) {
    await this.routeOperation(userId, async () => {
      if (this.sockets.get(userId)?.size) return;
      if (await this.router.remove(userId))
        await this.broadcastPresence(userId, false);
    });
  }

  private routeOperation(userId: string, work: () => Promise<void>) {
    const operation = (this.routeWork.get(userId) ?? Promise.resolve())
      .catch(() => undefined)
      .then(work);
    this.routeWork.set(userId, operation);
    void operation
      .finally(() => {
        if (this.routeWork.get(userId) === operation)
          this.routeWork.delete(userId);
      })
      .catch(() => undefined);
    return operation;
  }

  private async broadcastPresence(userId: string, online: boolean) {
    const peers = await this.chats.peers(userId);
    await Promise.all(
      peers.map((id) =>
        this.router.publish(id, { type: 'presence', userId, online }),
      ),
    );
  }

  private async refresh(connection: Connection) {
    if (Date.now() >= connection.expiresAt) {
      connection.socket.close(4001, 'Token expired');
      return;
    }
    if (connection.socket.readyState !== WebSocket.OPEN) return;
    await this.routeOperation(connection.userId, async () => {
      if (connection.socket.readyState !== WebSocket.OPEN || this.closing)
        return;
      if (await this.router.refresh(connection.userId))
        await this.broadcastPresence(connection.userId, true);
    });
    const peers = await this.chats.peers(connection.userId);
    for (const [userId, online] of await this.router.online(peers))
      this.send(connection.socket, { type: 'presence', userId, online });
  }

  private connected(socket: WebSocket, userId: string, expiresAt: number) {
    const connection: Connection = {
      socket,
      userId,
      chain: Promise.resolve(),
      pending: 0,
      expiresAt,
      lastPong: Date.now(),
      control: Promise.resolve(),
      ready: false,
      outgoing: [],
      outgoingBytes: 0,
    };
    this.socketState.set(socket, connection);
    this.metrics.connections.inc();
    const connections = this.sockets.get(userId) ?? new Set<Connection>();
    connections.add(connection);
    this.sockets.set(userId, connections);
    socket.on('error', (err) =>
      this.context.logger.debug({ err, userId }, 'WebSocket error'),
    );
    socket.once('close', () => {
      this.metrics.connections.dec();
      connections.delete(connection);
      if (!connections.size) this.sockets.delete(userId);
      this.trackCleanup(this.removeRoute(userId));
    });
    connection.chain = this.refresh(connection)
      .then(async () => {
        if (socket.readyState !== WebSocket.OPEN) {
          await this.removeRoute(userId);
          return;
        }
        connection.ready = true;
        this.send(socket, {
          type: 'welcome',
          userId,
          gatewayId: this.context.config.INSTANCE_ID,
          heartbeatIntervalMs: this.context.config.HEARTBEAT_INTERVAL_MS,
        });
        for (const frame of connection.outgoing) this.send(socket, frame);
        connection.outgoing = [];
        connection.outgoingBytes = 0;
      })
      .catch((err: unknown) => {
        this.context.logger.warn({ err, userId }, 'Socket registration failed');
        socket.close(1013, 'Routing unavailable');
      });
    connection.control = connection.chain;
    this.trackCleanup(connection.chain);
    socket.on('pong', () => {
      connection.lastPong = Date.now();
      this.trackCleanup(this.refresh(connection));
    });
    socket.on('message', (data, binary) => {
      const receivedAt = timestamp();
      if (this.closing) {
        this.send(socket, {
          type: 'error',
          code: 'UNAVAILABLE',
          message: 'Gateway is draining',
        });
        return;
      }
      if (binary) {
        socket.close(1003, 'Text JSON frames required');
        return;
      }
      if (++connection.pending > 256) {
        connection.pending--;
        socket.close(1013, 'Too many queued messages');
        return;
      }
      let control = false;
      let admission: ReturnType<typeof consumeToken> | undefined;
      try {
        const parsed = clientFrameSchema.safeParse(JSON.parse(data.toString()));
        control = parsed.success && parsed.data.type !== 'message';
        if (parsed.success && parsed.data.type !== 'heartbeat') {
          const { config, dependencies } = this.context;
          admission = consumeToken(
            dependencies.redis,
            userId,
            config.RATE_LIMIT_CAPACITY,
            config.RATE_LIMIT_REFILL_PER_SECOND,
            parsed.data.type === 'sync'
              ? 5
              : parsed.data.type === 'message'
                ? 1
                : 0.2,
          );
          // Attach immediately while the operation waits behind earlier work.
          void admission.catch(() => undefined);
        }
      } catch {
        /* Schema validation reports malformed JSON below. */
      }
      const operation = (control ? connection.control : connection.chain)
        .then(() => this.frame(connection, data, admission, receivedAt))
        .catch((err: unknown) => {
          this.context.logger.error({ err, userId }, 'Socket operation failed');
          this.send(socket, {
            type: 'error',
            code: 'UNAVAILABLE',
            message:
              'Message could not be accepted; retry with the same clientMsgId',
          });
        })
        .finally(() => {
          connection.pending--;
          this.inFlight.delete(operation);
          this.metrics.inflight.set(this.inFlight.size);
        });
      if (control) connection.control = operation;
      else connection.chain = operation;
      this.inFlight.add(operation);
      this.metrics.inflight.set(this.inFlight.size);
    });
  }

  private async frame(
    connection: Connection,
    data: RawData,
    admission?: ReturnType<typeof consumeToken>,
    receivedAt = timestamp(),
  ) {
    if (Date.now() >= connection.expiresAt) {
      connection.socket.close(4001, 'Token expired');
      return;
    }
    const bytes = Buffer.isBuffer(data)
      ? data
      : Array.isArray(data)
        ? Buffer.concat(data)
        : Buffer.from(data);
    let frame: ClientFrame;
    try {
      frame = clientFrameSchema.parse(JSON.parse(bytes.toString('utf8')));
    } catch {
      this.send(connection.socket, {
        type: 'error',
        code: 'INVALID_MESSAGE',
        message: 'Invalid JSON message or fields',
      });
      return;
    }
    const clientMsgId =
      frame.type === 'message' ? frame.clientMsgId : undefined;
    if (frame.type === 'message') this.metrics.incoming.inc();
    try {
      if (frame.type === 'heartbeat') {
        await this.refresh(connection);
        this.send(connection.socket, { type: 'heartbeat' });
        return;
      }
      const { config } = this.context;
      const bucket = await admission;
      if (!bucket) throw new Error('Missing rate admission');
      if (!bucket.allowed) {
        this.metrics.rateRejected.inc();
        this.send(connection.socket, {
          type: 'error',
          code: 'RATE_LIMITED',
          message: 'User rate limit exceeded',
          retryAfterMs: bucket.retryAfterMs,
          operation: frame.type,
          chatId: frame.chatId,
          ...(clientMsgId ? { clientMsgId } : {}),
        });
        return;
      }
      if (frame.type === 'sync') {
        const page = await this.chats.history(
          frame.chatId,
          connection.userId,
          frame.afterSeq,
          frame.limit,
        );
        const budget = Math.min(
          524288,
          Math.floor(config.MAX_SOCKET_BUFFER_BYTES / 2),
        );
        while (
          page.messages.length > 1 &&
          Buffer.byteLength(JSON.stringify(page)) > budget
        ) {
          page.messages.pop();
          page.hasMore = true;
        }
        page.nextAfterSeq = page.messages.at(-1)?.seq ?? frame.afterSeq;
        this.send(connection.socket, {
          type: 'sync',
          chatId: frame.chatId,
          ...page,
        });
        return;
      }
      if (frame.type === 'delivered' || frame.type === 'read') {
        const message =
          frame.type === 'delivered'
            ? await this.chats.messageForReceipt(
                frame.chatId,
                connection.userId,
                frame.messageId,
              )
            : undefined;
        if (frame.type === 'read')
          await this.chats.validateRead(
            frame.chatId,
            connection.userId,
            frame.lastReadSeq,
          );
        const seq =
          message?.seq ?? (frame.type === 'read' ? frame.lastReadSeq : 0);
        this.context.logger.info(
          {
            messageId: message?.messageId,
            chatId: frame.chatId,
            userId: connection.userId,
            seq,
            status: frame.type,
          },
          'Client receipt accepted',
        );
        if (seq)
          await this.batcher.send(
            'receipts',
            frame.chatId,
            JSON.stringify({
              type: 'receipt_event',
              chatId: frame.chatId,
              userId: connection.userId,
              status: frame.type,
              seq,
              timestamp: new Date().toISOString(),
              ...(message ? { messageCreatedAt: message.createdAt } : {}),
            }),
          );
        return;
      }
      if (frame.type !== 'message')
        throw new HttpError(400, 'INVALID_REQUEST', 'Unsupported operation');
      const chat = await this.chats.authorize(frame.chatId, connection.userId);
      const messageId = messageIdFor(connection.userId, frame.clientMsgId);
      const existing = await this.context.dependencies.db
        .collection('messages')
        .findOne({ messageId });
      if (
        existing &&
        (existing.chatId !== frame.chatId || existing.body !== frame.body)
      )
        throw new HttpError(
          409,
          'CONFLICT',
          'clientMsgId belongs to a different message',
        );
      const produceStartedAt = timestamp();
      observeStage(
        this.metrics,
        'gateway_prepare',
        receivedAt,
        produceStartedAt,
      );
      const trace = config.TRACE_MESSAGES
        ? { gatewayReceivedAt: receivedAt, produceStartedAt }
        : undefined;
      const record = kafkaMessageSchema.parse({
        messageId,
        clientMsgId: frame.clientMsgId,
        chatId: frame.chatId,
        body: frame.body,
        senderId: connection.userId,
        recipientIds: chat.memberIds,
        createdAt: new Date().toISOString(),
        ...(trace ? { trace } : {}),
      });
      await this.batcher.send('messages', frame.chatId, JSON.stringify(record));
      const kafkaAcknowledgedAt = timestamp();
      observeStage(
        this.metrics,
        'kafka_produce_ack',
        produceStartedAt,
        kafkaAcknowledgedAt,
      );
      this.context.logger.info(
        { messageId, chatId: frame.chatId, userId: connection.userId },
        'Message accepted by Kafka',
      );
      this.send(connection.socket, {
        type: 'ack',
        clientMsgId: frame.clientMsgId,
        status: 'received',
        ...(trace ? { trace: { ...trace, kafkaAcknowledgedAt } } : {}),
      });
    } catch (err) {
      const code =
        err instanceof HttpError && err.code === 'FORBIDDEN'
          ? 'FORBIDDEN'
          : err instanceof HttpError && err.code === 'CONFLICT'
            ? 'CONFLICT'
            : err instanceof HttpError && err.code === 'INVALID_REQUEST'
              ? 'INVALID_MESSAGE'
              : 'UNAVAILABLE';
      if (code === 'UNAVAILABLE')
        this.context.logger.error(
          { err, userId: connection.userId, clientMsgId },
          'Message operation unavailable',
        );
      this.send(connection.socket, {
        type: 'error',
        code,
        message:
          err instanceof HttpError
            ? err.message
            : 'Service is temporarily unavailable',
        ...(clientMsgId ? { clientMsgId } : {}),
        ...(frame.type !== 'heartbeat'
          ? { chatId: frame.chatId, operation: frame.type }
          : {}),
      });
    }
  }

  async start() {
    const results = await Promise.allSettled([
      this.auth.initialize(),
      this.chats.initialize(),
      this.producer.connect(),
      this.subscriber.connect(),
    ]);
    const failure = results.find((result) => result.status === 'rejected');
    if (failure?.status === 'rejected') throw failure.reason;
    await this.subscriber.subscribe(
      `gateway:${this.context.config.INSTANCE_ID}`,
    );
    this.timer = setInterval(() => {
      for (const connections of this.sockets.values())
        for (const connection of connections) {
          if (Date.now() >= connection.expiresAt)
            connection.socket.close(4001, 'Token expired');
          else if (
            Date.now() - connection.lastPong >
            this.context.config.PRESENCE_TTL_SECONDS * 1000
          )
            connection.socket.terminate();
          else if (connection.socket.readyState === WebSocket.OPEN)
            connection.socket.ping();
        }
    }, this.context.config.HEARTBEAT_INTERVAL_MS);
    this.timer.unref();
  }

  beginShutdown = () => {
    this.closing = true;
    clearInterval(this.timer);
    for (const connections of this.sockets.values())
      for (const { socket } of connections)
        this.send(socket, { type: 'reconnect', retryAfterMs: 500 });
  };

  async stop() {
    this.closing = true;
    clearInterval(this.timer);
    this.context.server.off('upgrade', this.onUpgrade);
    await Promise.allSettled([...this.inFlight]);
    const closed = new Promise<void>((resolve) =>
      this.wsServer.close(() => resolve()),
    );
    const deadline = setTimeout(() => {
      for (const socket of this.wsServer.clients) socket.terminate();
    }, 3000);
    deadline.unref();
    for (const socket of this.wsServer.clients)
      socket.close(1012, 'Gateway restart');
    await closed;
    clearTimeout(deadline);
    await Promise.allSettled([...this.cleanup]);
    await this.batcher.close();
    await this.producer.disconnect();
    this.subscriber.disconnect();
  }

  runtime(): ServiceRuntime {
    return {
      ready: () => !this.closing && this.subscriber.status === 'ready',
      handleRequest: (request, response) => this.http.handle(request, response),
      beginShutdown: this.beginShutdown,
      stop: () => this.stop(),
    };
  }
}

export async function createGatewayRuntime(
  context: ServiceContext,
): Promise<ServiceRuntime> {
  const gateway = new GatewayRuntime(context);
  try {
    await gateway.start();
    return gateway.runtime();
  } catch (error) {
    await gateway.stop().catch(() => undefined);
    throw error;
  }
}
