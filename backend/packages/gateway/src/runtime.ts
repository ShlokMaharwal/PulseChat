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
  type ClientFrame,
  type ServerFrame,
  type ServiceContext,
  type ServiceRuntime,
} from '@pulsechat/shared';
import { AuthService, verifyToken, websocketToken } from './auth.js';
import { ChatService } from './chats.js';
import { GatewayHttpApi } from './http.js';
import { HttpError } from './errors.js';

const envelopeSchema = z
  .object({ userId: uuidSchema, payload: serverFrameSchema })
  .strict();
const REMOVE_ROUTE = `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0`;

interface Connection {
  socket: WebSocket;
  userId: string;
  chain: Promise<void>;
  pending: number;
}

class GatewayRuntime {
  private readonly auth;
  private readonly chats;
  private readonly http;
  private readonly producer;
  private readonly subscriber;
  private readonly sockets = new Map<string, Set<Connection>>();
  private readonly inFlight = new Set<Promise<void>>();
  private readonly cleanup = new Set<Promise<void>>();
  private readonly wsServer;
  private closing = false;

  constructor(private readonly context: ServiceContext) {
    const { config, dependencies, logger } = context;
    this.auth = new AuthService(dependencies.db, config);
    this.chats = new ChatService(dependencies.db);
    this.http = new GatewayHttpApi(this.auth, this.chats, config, logger);
    this.producer = dependencies.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
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
      const userId = verifyToken(
        websocketToken(request.headers['sec-websocket-protocol']),
        this.context.config,
      );
      await this.auth.user(userId);
      if (socket.destroyed || this.closing || !this.context.accepting()) {
        this.reject(socket, 503, 'Service Unavailable');
        return;
      }
      this.wsServer.handleUpgrade(request, socket, head, (client) =>
        this.connected(client, userId),
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
    if (this.sockets.get(userId)?.size) return;
    await this.context.dependencies.redis.eval(
      REMOVE_ROUTE,
      1,
      `user:${userId}:gateway`,
      this.context.config.INSTANCE_ID,
    );
  }

  private connected(socket: WebSocket, userId: string) {
    const connection: Connection = {
      socket,
      userId,
      chain: Promise.resolve(),
      pending: 0,
    };
    const connections = this.sockets.get(userId) ?? new Set<Connection>();
    connections.add(connection);
    this.sockets.set(userId, connections);
    socket.on('error', (err) =>
      this.context.logger.debug({ err, userId }, 'WebSocket error'),
    );
    socket.once('close', () => {
      connections.delete(connection);
      if (!connections.size) this.sockets.delete(userId);
      this.trackCleanup(this.removeRoute(userId));
    });
    connection.chain = this.context.dependencies.redis
      .set(`user:${userId}:gateway`, this.context.config.INSTANCE_ID)
      .then(async () => {
        if (socket.readyState !== WebSocket.OPEN) {
          await this.removeRoute(userId);
          return;
        }
        this.send(socket, {
          type: 'welcome',
          userId,
          gatewayId: this.context.config.INSTANCE_ID,
          heartbeatIntervalMs: this.context.config.HEARTBEAT_INTERVAL_MS,
        });
      })
      .catch((err: unknown) => {
        this.context.logger.warn({ err, userId }, 'Socket registration failed');
        socket.close(1013, 'Routing unavailable');
      });
    socket.on('message', (data, binary) => {
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
      const operation = connection.chain
        .then(() => this.frame(connection, data))
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
        });
      connection.chain = operation;
      this.inFlight.add(operation);
    });
  }

  private async frame(connection: Connection, data: RawData) {
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
    try {
      if (frame.type === 'heartbeat') {
        this.send(connection.socket, { type: 'heartbeat' });
        return;
      }
      if (frame.type === 'sync') {
        const page = await this.chats.history(
          frame.chatId,
          connection.userId,
          frame.afterSeq,
          frame.limit,
        );
        this.send(connection.socket, {
          type: 'sync',
          chatId: frame.chatId,
          ...page,
        });
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
      const record = kafkaMessageSchema.parse({
        messageId,
        clientMsgId: frame.clientMsgId,
        chatId: frame.chatId,
        body: frame.body,
        senderId: connection.userId,
        recipientIds: chat.memberIds,
        createdAt: new Date().toISOString(),
      });
      await this.producer.send({
        topic: 'messages',
        acks: -1,
        messages: [{ key: frame.chatId, value: JSON.stringify(record) }],
      });
      this.context.logger.info(
        { messageId, chatId: frame.chatId, userId: connection.userId },
        'Message accepted by Kafka',
      );
      this.send(connection.socket, {
        type: 'ack',
        clientMsgId: frame.clientMsgId,
        status: 'received',
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
  }

  beginShutdown = () => {
    this.closing = true;
    for (const connections of this.sockets.values())
      for (const { socket } of connections)
        this.send(socket, { type: 'reconnect', retryAfterMs: 500 });
  };

  async stop() {
    this.closing = true;
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
