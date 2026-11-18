import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import WebSocket from 'ws';
import {
  serverFrameSchema,
  type ServerFrame,
  type StoredMessage,
  type ClientFrame,
} from '../../packages/shared/src/schemas.js';

/** Raw frames remain available so deduplication cannot hide duplicate wire delivery. */
export class TestClient extends EventEmitter {
  readonly frames: ServerFrame[] = [];
  readonly messages: StoredMessage[] = [];
  readonly lastSeq = new Map<string, number>();
  private socket: WebSocket | undefined;
  private failure: Error | undefined;
  private url = '';
  private token = '';
  private userId = '';
  private stopped = false;
  private attempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  private readonly pending = new Map<
    string,
    Extract<ClientFrame, { type: 'message' }>
  >();
  private readonly buffered = new Map<string, Map<number, StoredMessage>>();
  private readonly syncing = new Set<string>();
  private readonly delivered = new Map<string, StoredMessage>();
  private readonly reads = new Map<string, number>();
  private receiptTimer: ReturnType<typeof setTimeout> | undefined;
  constructor(private readonly options: { reconnect?: boolean } = {}) {
    super();
    this.setMaxListeners(256);
  }
  async connect(url: string, token: string) {
    this.url = url;
    this.token = token;
    this.stopped = false;
    this.failure = undefined;
    const start = this.frames.length;
    this.open();
    return await this.waitFor(
      (frame) =>
        frame.type === 'welcome' && this.frames.indexOf(frame) >= start,
    );
  }
  private open() {
    const socket = new WebSocket(this.url, ['pulsechat', `jwt.${this.token}`]);
    this.socket = socket;
    let welcomed = false;
    socket.on('unexpected-response', (_, response) => {
      response.resume();
      if (
        (response.statusCode ?? 0) >= 500 &&
        this.options.reconnect !== false
      ) {
        socket.terminate();
        return;
      }
      this.failure = new Error(
        `WebSocket upgrade rejected: ${response.statusCode}`,
      );
      this.stopped = true;
      socket.terminate();
      this.emit('frame');
    });
    socket.on('error', (error) => {
      if (!welcomed && this.attempt === 0) {
        this.failure = error;
        this.emit('frame');
      }
    });
    socket.on('close', (code) => {
      if (this.socket !== socket) return;
      clearInterval(this.heartbeatTimer);
      this.syncing.clear();
      if (code === 4001) {
        this.stopped = true;
        this.failure = new Error('Session expired');
        this.emit('frame');
      }
      if (!this.stopped && this.options.reconnect !== false)
        this.scheduleReconnect();
    });
    socket.on('message', (raw) => {
      if (this.socket !== socket) return;
      try {
        const frame = serverFrameSchema.parse(JSON.parse(raw.toString()));
        this.frames.push(frame);
        if (frame.type === 'welcome') {
          welcomed = true;
          this.attempt = 0;
          this.failure = undefined;
          if (this.userId && this.userId !== frame.userId) {
            this.lastSeq.clear();
            this.messages.length = 0;
            this.pending.clear();
            this.buffered.clear();
            this.delivered.clear();
            this.reads.clear();
          }
          this.userId = frame.userId;
          for (const message of this.pending.values()) this.send(message);
          void this.discover().catch(() => undefined);
          clearInterval(this.heartbeatTimer);
          this.heartbeatTimer = setInterval(() => {
            this.sendIfOpen({ type: 'heartbeat' });
            void this.discover().catch(() => undefined);
            this.flushReceipts();
          }, frame.heartbeatIntervalMs);
          this.heartbeatTimer.unref();
          this.flushReceipts();
        } else if (frame.type === 'ack') this.pending.delete(frame.clientMsgId);
        else if (frame.type === 'message') this.live(frame);
        else if (frame.type === 'sync') {
          for (const message of frame.messages) this.ingest(message);
          const buffer = this.buffered.get(frame.chatId);
          if (buffer)
            for (const seq of buffer.keys())
              if (seq <= (this.lastSeq.get(frame.chatId) ?? 0))
                buffer.delete(seq);
          this.syncing.delete(frame.chatId);
          if (frame.hasMore) this.sync(frame.chatId);
          else this.drain(frame.chatId);
        } else if (frame.type === 'reconnect')
          socket.close(1000, 'Reconnect requested');
        else if (frame.type === 'error') {
          if (frame.chatId && frame.operation === 'sync')
            this.syncing.delete(frame.chatId);
          if (
            frame.clientMsgId &&
            ['FORBIDDEN', 'CONFLICT', 'INVALID_MESSAGE'].includes(frame.code)
          )
            this.pending.delete(frame.clientMsgId);
          if (frame.code === 'RATE_LIMITED' || frame.code === 'UNAVAILABLE') {
            const retry = setTimeout(
              () => {
                if (frame.clientMsgId) {
                  const message = this.pending.get(frame.clientMsgId);
                  if (message) this.sendIfOpen(message);
                }
                if (frame.operation === 'sync' && frame.chatId)
                  this.sync(frame.chatId);
                this.flushReceipts();
              },
              Math.max(100, frame.retryAfterMs ?? 1000) + Math.random() * 200,
            );
            retry.unref();
          }
        }
        this.emit('frame', frame);
      } catch (error) {
        this.failure =
          error instanceof Error ? error : new Error(String(error));
        this.emit('frame');
      }
    });
  }
  private scheduleReconnect() {
    clearTimeout(this.reconnectTimer);
    const delay = Math.min(30000, 250 * 2 ** Math.min(this.attempt++, 7));
    this.reconnectTimer = setTimeout(
      () => {
        if (!this.stopped) this.open();
      },
      delay * (0.75 + Math.random() * 0.5),
    );
    this.reconnectTimer.unref();
  }
  private async discover() {
    if (this.stopped || this.socket?.readyState !== WebSocket.OPEN) return;
    const base = this.url.replace(/^ws/, 'http').replace(/\/ws$/, '');
    let response = await fetch(`${base}/api/chats`, {
      headers: { authorization: `Bearer ${this.token}` },
      signal: AbortSignal.timeout(15000),
    });
    if (response.status === 404)
      response = await fetch(`${base}/chats`, {
        headers: { authorization: `Bearer ${this.token}` },
        signal: AbortSignal.timeout(15000),
      });
    if (!response.ok) throw new Error(`Chat discovery: ${response.status}`);
    const result = (await response.json()) as { chats: { id: string }[] };
    for (const chat of result.chats) this.sync(chat.id);
  }
  sync(chatId: string) {
    if (
      this.syncing.has(chatId) ||
      this.socket?.readyState !== WebSocket.OPEN ||
      this.stopped
    )
      return;
    this.syncing.add(chatId);
    this.send({
      type: 'sync',
      chatId,
      afterSeq: this.lastSeq.get(chatId) ?? 0,
    });
  }
  private live(message: StoredMessage) {
    const last = this.lastSeq.get(message.chatId) ?? 0;
    if (message.seq <= last) return;
    if (message.seq === last + 1 && !this.syncing.has(message.chatId)) {
      this.ingest(message);
      this.drain(message.chatId);
      return;
    }
    const buffer =
      this.buffered.get(message.chatId) ?? new Map<number, StoredMessage>();
    buffer.set(message.seq, message);
    this.buffered.set(message.chatId, buffer);
    this.sync(message.chatId);
  }
  private drain(chatId: string) {
    const buffer = this.buffered.get(chatId);
    if (!buffer) return;
    let next = (this.lastSeq.get(chatId) ?? 0) + 1;
    while (buffer.has(next)) {
      const message = buffer.get(next)!;
      buffer.delete(next);
      this.ingest(message);
      next++;
    }
    if (buffer.size) this.sync(chatId);
  }
  private ingest(message: StoredMessage) {
    if (message.seq <= (this.lastSeq.get(message.chatId) ?? 0)) return;
    this.lastSeq.set(message.chatId, message.seq);
    this.messages.push(message);
    if (message.senderId === this.userId)
      this.pending.delete(message.clientMsgId);
    else {
      this.delivered.set(message.chatId, message);
      clearTimeout(this.receiptTimer);
      this.receiptTimer = setTimeout(() => this.flushReceipts(), 100);
      this.receiptTimer.unref();
    }
    this.emit('ingested', message);
  }
  read(chatId: string, seq = this.lastSeq.get(chatId) ?? 0) {
    this.reads.set(chatId, Math.max(seq, this.reads.get(chatId) ?? 0));
    this.flushReceipts();
  }
  private flushReceipts() {
    if (this.stopped) return;
    for (const message of this.delivered.values())
      this.sendIfOpen({
        type: 'delivered',
        chatId: message.chatId,
        messageId: message.messageId,
      });
    for (const [chatId, lastReadSeq] of this.reads)
      this.sendIfOpen({ type: 'read', chatId, lastReadSeq });
  }
  waitFor(
    predicate: (frame: ServerFrame) => boolean,
    timeout = 60000,
  ): Promise<ServerFrame> {
    return new Promise((resolve, reject) => {
      const deadline = setTimeout(() => {
        finish();
        reject(new Error('Timed out waiting for WebSocket frame'));
      }, timeout);
      const finish = () => {
        clearTimeout(deadline);
        this.off('frame', receive);
      };
      const receive = () => {
        const frame = this.frames.find(predicate);
        if (frame) {
          finish();
          resolve(frame);
        } else if (this.failure) {
          finish();
          reject(this.failure);
        }
      };
      this.on('frame', receive);
      receive();
    });
  }
  waitForSeq(chatId: string, seq: number, timeout = 60000): Promise<void> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        finish();
        reject(
          new Error(
            `Missing seq ${seq}; current ${this.lastSeq.get(chatId) ?? 0}`,
          ),
        );
      }, timeout);
      const finish = () => {
        clearTimeout(timer);
        this.off('ingested', check);
      };
      const check = () => {
        if ((this.lastSeq.get(chatId) ?? 0) >= seq) {
          finish();
          resolve();
        }
      };
      this.on('ingested', check);
      check();
    });
  }
  private sendIfOpen(frame: unknown) {
    if (!this.stopped && this.socket?.readyState === WebSocket.OPEN)
      this.send(frame);
  }
  send(frame: unknown) {
    if (this.socket?.readyState !== WebSocket.OPEN)
      throw new Error('Client is disconnected');
    this.socket.send(JSON.stringify(frame));
  }

  sendRaw(text: string) {
    if (this.socket?.readyState !== WebSocket.OPEN)
      throw new Error('Client is disconnected');
    this.socket.send(text);
  }
  async message(chatId: string, body: string, clientMsgId = randomUUID()) {
    const frame: Extract<ClientFrame, { type: 'message' }> = {
      type: 'message',
      chatId,
      body,
      clientMsgId,
    };
    this.pending.set(clientMsgId, frame);
    const accepted = this.waitFor(
      (item) =>
        (item.type === 'ack' ||
          (item.type === 'error' &&
            !['RATE_LIMITED', 'UNAVAILABLE'].includes(item.code))) &&
        item.clientMsgId === clientMsgId,
    );
    this.sendIfOpen(frame);
    const result = await accepted;
    if (result.type === 'error')
      throw new Error(`${result.code}: ${result.message}`);
    return clientMsgId;
  }
  async close() {
    this.stopped = true;
    clearTimeout(this.reconnectTimer);
    clearInterval(this.heartbeatTimer);
    clearTimeout(this.receiptTimer);
    this.syncing.clear();
    const socket = this.socket;
    if (!socket || socket.readyState === WebSocket.CLOSED) return;
    await new Promise<void>((resolve) => {
      const deadline = setTimeout(() => {
        socket.terminate();
        resolve();
      }, 3000);
      socket.once('close', () => {
        clearTimeout(deadline);
        resolve();
      });
      socket.close(1000, 'Test complete');
    });
  }
}
