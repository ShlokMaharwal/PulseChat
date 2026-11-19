import type {
  ClientFrame,
  Message,
  Receipt,
  ServerFrame,
} from '../types/protocol';
export type ConnectionStatus =
  'connecting' | 'connected' | 'reconnecting' | 'offline' | 'expired';
export interface PendingMessage {
  clientMsgId: string;
  chatId: string;
  body: string;
  createdAt: string;
  received: boolean;
  seq?: number;
  failure?: string;
}
interface Options {
  storage?: Pick<Storage, 'getItem' | 'setItem'>;
  socketFactory?: (url: string, protocols: string[]) => WebSocket;
  random?: () => number;
  onDiscover?: () => void;
  renewSession?: () => Promise<string>;
}
const validMessage = (m: Message) =>
  m &&
  typeof m.messageId === 'string' &&
  typeof m.chatId === 'string' &&
  typeof m.senderId === 'string' &&
  typeof m.clientMsgId === 'string' &&
  typeof m.body === 'string' &&
  Number.isSafeInteger(m.seq) &&
  m.seq > 0 &&
  typeof m.createdAt === 'string';
export class PulseClient {
  status: ConnectionStatus = 'offline';
  error = '';
  degraded = false;
  nextRetryAt = 0;
  gatewayId = '';
  readonly messages = new Map<string, Message[]>();
  readonly pending = new Map<string, PendingMessage>();
  readonly receipts = new Map<string, Map<string, Receipt>>();
  readonly presence = new Map<string, boolean>();
  readonly cursors = new Map<string, number>();
  private socket?: WebSocket;
  private stopped = true;
  private online = true;
  private attempt = 0;
  private renewing = false;
  private hint = 0;
  private readonly listeners = new Set<() => void>();
  private readonly tracked = new Set<string>();
  private readonly buffers = new Map<string, Map<number, Message>>();
  private readonly syncing = new Map<string, ReturnType<typeof setTimeout>>();
  private readonly delivered = new Map<string, Message>();
  private readonly reads = new Map<string, number>();
  private reconnectTimer?: ReturnType<typeof setTimeout>;
  private heartbeat?: ReturnType<typeof setInterval>;
  private receiptTimer?: ReturnType<typeof setTimeout>;
  private readonly retries = new Set<ReturnType<typeof setTimeout>>();
  private readonly storage;
  constructor(
    readonly userId: string,
    private token: string,
    private readonly url: string,
    private readonly options: Options = {},
  ) {
    this.storage = options.storage ?? globalThis.localStorage;
    try {
      const saved = JSON.parse(
        this.storage.getItem(this.key('cursors')) ?? '{}',
      ) as Record<string, number>;
      for (const [id, seq] of Object.entries(saved))
        if (Number.isSafeInteger(seq) && seq >= 0) {
          this.cursors.set(id, seq);
          this.tracked.add(id);
        }
    } catch {
      /* Storage can be disabled or cleared independently. */
    }
    try {
      const saved = JSON.parse(
        this.storage.getItem(this.key('pending')) ?? '[]',
      ) as PendingMessage[];
      for (const item of saved)
        if (
          item &&
          typeof item.clientMsgId === 'string' &&
          typeof item.chatId === 'string' &&
          typeof item.body === 'string'
        ) {
          this.pending.set(item.clientMsgId, item);
          this.tracked.add(item.chatId);
        }
    } catch {
      /* Continue with the in-memory queue. */
    }
  }
  private key(kind: string) {
    return `pulsechat.${kind}.${this.userId}`;
  }
  private persist() {
    try {
      this.storage.setItem(
        this.key('cursors'),
        JSON.stringify(Object.fromEntries(this.cursors)),
      );
      this.storage.setItem(
        this.key('pending'),
        JSON.stringify([...this.pending.values()]),
      );
    } catch {
      /* Reconnect remains available when browser storage is full. */
    }
  }
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  private changed() {
    for (const listener of this.listeners) listener();
  }
  start() {
    if (!this.stopped) return;
    this.stopped = false;
    this.open();
  }
  setOnline(online: boolean) {
    if (this.online === online) return;
    this.online = online;
    if (this.stopped) return;
    if (online) {
      this.open();
      return;
    }
    clearTimeout(this.reconnectTimer);
    clearInterval(this.heartbeat);
    for (const timer of this.syncing.values()) clearTimeout(timer);
    this.syncing.clear();
    const socket = this.socket;
    this.socket = undefined;
    this.attempt++;
    this.status = 'offline';
    socket?.close(1000, 'Browser offline');
    this.changed();
  }
  private expired() {
    try {
      const segment = this.token.split('.')[1];
      if (!segment) return false;
      const payload = JSON.parse(
        atob(segment.replaceAll('-', '+').replaceAll('_', '/')),
      ) as { exp?: number };
      return (
        typeof payload.exp === 'number' && payload.exp * 1000 <= Date.now()
      );
    } catch {
      return false;
    }
  }
  private open() {
    if (this.stopped || !this.online) return;
    if (this.expired()) {
      if (this.options.renewSession) {
        void this.renew();
        return;
      }
      this.status = 'expired';
      this.error = 'Your session expired. Sign in again.';
      this.stopped = true;
      this.changed();
      return;
    }
    this.status = this.attempt ? 'reconnecting' : 'connecting';
    this.nextRetryAt = 0;
    this.changed();
    const socket = (
      this.options.socketFactory ??
      ((url, protocols) => new WebSocket(url, protocols))
    )(this.url, ['pulsechat', `jwt.${this.token}`]);
    this.socket = socket;
    socket.onopen = () => {
      if (this.stopped) socket.close(1000);
    };
    socket.onmessage = (event) => {
      if (this.socket !== socket || this.stopped) return;
      try {
        const frame = JSON.parse(String(event.data)) as ServerFrame;
        if (!frame || typeof frame.type !== 'string')
          throw new Error('Invalid response');
        this.receive(frame);
      } catch {
        this.error =
          'A response could not be read. Reconnect to recover your conversation.';
        this.changed();
      }
    };
    socket.onerror = () => {
      if (!this.stopped) {
        this.status = 'reconnecting';
        this.degraded = true;
        this.error =
          'Chat services are temporarily unavailable. Queued messages will retry.';
        this.changed();
      }
    };
    socket.onclose = (event) => {
      if (this.socket !== socket) return;
      clearInterval(this.heartbeat);
      for (const timer of this.syncing.values()) clearTimeout(timer);
      this.syncing.clear();
      if (this.stopped) return;
      if (event.code === 4001) {
        if (this.options.renewSession) {
          void this.renew();
          return;
        }
        this.stopped = true;
        this.status = 'expired';
        this.error = 'Your session expired. Sign in again.';
      } else {
        this.status = 'reconnecting';
        const backoff = Math.min(30000, 500 * 2 ** Math.min(this.attempt++, 6));
        const delay = Math.max(
          this.hint,
          backoff * (0.75 + (this.options.random ?? Math.random)() * 0.5),
        );
        this.hint = 0;
        this.nextRetryAt = Date.now() + delay;
        this.reconnectTimer = setTimeout(() => this.open(), delay);
      }
      this.changed();
    };
  }
  private async renew() {
    if (this.renewing || this.stopped) return;
    this.renewing = true;
    this.status = 'connecting';
    this.nextRetryAt = 0;
    this.changed();
    try {
      this.token = await this.options.renewSession!();
      this.renewing = false;
      if (!this.stopped) this.open();
    } catch (error) {
      if (this.stopped) return;
      if (
        error &&
        typeof error === 'object' &&
        'status' in error &&
        error.status === 401
      ) {
        this.stopped = true;
        this.status = 'expired';
        this.error = 'Your session expired. Sign in again.';
      } else {
        this.status = 'reconnecting';
        this.error = 'Session renewal is temporarily unavailable. Retrying…';
        const delay = Math.min(30000, 1000 * 2 ** Math.min(this.attempt++, 5));
        this.nextRetryAt = Date.now() + delay;
        this.reconnectTimer = setTimeout(() => {
          void this.renew();
        }, delay);
      }
      this.changed();
    } finally {
      this.renewing = false;
    }
  }
  private receive(frame: ServerFrame) {
    if (
      this.degraded &&
      ['welcome', 'sync', 'heartbeat'].includes(frame.type)
    ) {
      this.degraded = false;
      this.error = '';
    }
    if (frame.type === 'welcome') {
      if (frame.userId !== this.userId) {
        this.socket?.close(4001, 'Session mismatch');
        return;
      }
      this.status = 'connected';
      this.nextRetryAt = 0;
      this.error = '';
      this.gatewayId = frame.gatewayId;
      this.attempt = 0;
      for (const pending of this.pending.values())
        if (!pending.received && !pending.failure)
          this.transmit({
            type: 'message',
            clientMsgId: pending.clientMsgId,
            chatId: pending.chatId,
            body: pending.body,
          });
      for (const id of this.tracked) this.sync(id);
      this.options.onDiscover?.();
      this.flushReceipts();
      clearInterval(this.heartbeat);
      this.heartbeat = setInterval(
        () => {
          this.transmit({ type: 'heartbeat' });
          for (const id of this.tracked) this.sync(id);
          this.flushReceipts();
          this.options.onDiscover?.();
        },
        Math.max(1000, Math.min(frame.heartbeatIntervalMs, 15000)),
      );
    } else if (frame.type === 'ack') {
      const pending = this.pending.get(frame.clientMsgId);
      if (pending) {
        pending.received = true;
        if (frame.status === 'sent') pending.seq = frame.seq;
        this.persist();
      }
    } else if (frame.type === 'message') {
      if (!validMessage(frame)) throw new Error('Invalid message');
      this.live(frame);
    } else if (frame.type === 'sync') {
      if (!Array.isArray(frame.messages) || !frame.messages.every(validMessage))
        throw new Error('Invalid history');
      for (const message of frame.messages) this.ingest(message);
      this.mergeReceipts(frame.chatId, frame.receipts ?? []);
      clearTimeout(this.syncing.get(frame.chatId));
      this.syncing.delete(frame.chatId);
      const buffer = this.buffers.get(frame.chatId);
      if (buffer)
        for (const seq of buffer.keys())
          if (seq <= (this.cursors.get(frame.chatId) ?? 0)) buffer.delete(seq);
      if (frame.hasMore) this.sync(frame.chatId, frame.nextAfterSeq);
      else this.drain(frame.chatId);
    } else if (frame.type === 'receipt') {
      this.mergeReceipts(frame.chatId, [
        {
          userId: frame.userId,
          deliveredSeq: frame.seq,
          readSeq: frame.status === 'read' ? frame.seq : 0,
        },
      ]);
    } else if (frame.type === 'presence')
      this.presence.set(frame.userId, frame.online);
    else if (frame.type === 'reconnect') {
      this.hint = frame.retryAfterMs;
      this.socket?.close(1000, 'Reconnect requested');
    } else if (frame.type === 'error') {
      if (frame.code === 'UNAVAILABLE') {
        this.degraded = true;
        this.error =
          'Chat services are temporarily unavailable. Queued messages will retry.';
      }
      if (frame.operation === 'sync' && frame.chatId) {
        clearTimeout(this.syncing.get(frame.chatId));
        this.syncing.delete(frame.chatId);
      }
      const retryable =
        frame.code === 'RATE_LIMITED' || frame.code === 'UNAVAILABLE';
      const pending = frame.clientMsgId
        ? this.pending.get(frame.clientMsgId)
        : undefined;
      if (retryable) {
        const timer = setTimeout(
          () => {
            this.retries.delete(timer);
            if (pending && !pending.received)
              this.transmit({
                type: 'message',
                clientMsgId: pending.clientMsgId,
                chatId: pending.chatId,
                body: pending.body,
              });
            if (frame.operation === 'sync' && frame.chatId)
              this.sync(frame.chatId);
            this.flushReceipts();
          },
          Math.max(100, frame.retryAfterMs ?? 1000) +
            (this.options.random ?? Math.random)() * 200,
        );
        this.retries.add(timer);
      } else if (pending) {
        pending.failure = frame.message;
        this.persist();
      } else {
        this.error = frame.message;
      }
    }
    this.changed();
  }
  track(chatId: string) {
    const isNew = !this.tracked.has(chatId);
    this.tracked.add(chatId);
    if (isNew) this.sync(chatId);
  }
  sync(chatId: string, afterSeq = this.cursors.get(chatId) ?? 0) {
    if (this.stopped || this.status !== 'connected' || this.syncing.has(chatId))
      return;
    this.syncing.set(
      chatId,
      setTimeout(() => {
        this.syncing.delete(chatId);
        this.sync(chatId);
      }, 20000),
    );
    this.transmit({ type: 'sync', chatId, afterSeq, limit: 100 });
  }
  seed(chatId: string, messages: Message[], receipts: Receipt[]) {
    this.tracked.add(chatId);
    for (const message of messages) {
      this.mergeMessage(message);
      if (message.seq > (this.cursors.get(chatId) ?? 0))
        this.cursors.set(chatId, message.seq);
    }
    this.mergeReceipts(chatId, receipts);
    this.persist();
    this.changed();
  }
  mergeReceipts(chatId: string, receipts: Receipt[]) {
    const state = this.receipts.get(chatId) ?? new Map<string, Receipt>();
    for (const receipt of receipts) {
      const old = state.get(receipt.userId);
      state.set(receipt.userId, {
        userId: receipt.userId,
        deliveredSeq: Math.max(
          old?.deliveredSeq ?? 0,
          receipt.deliveredSeq,
          receipt.readSeq,
        ),
        readSeq: Math.max(old?.readSeq ?? 0, receipt.readSeq),
      });
    }
    this.receipts.set(chatId, state);
  }
  private live(message: Message) {
    const discovered = !this.tracked.has(message.chatId);
    this.tracked.add(message.chatId);
    if (discovered) this.options.onDiscover?.();
    const last = this.cursors.get(message.chatId) ?? 0;
    if (message.seq <= last) return;
    if (message.seq === last + 1 && !this.syncing.has(message.chatId)) {
      this.ingest(message);
      this.drain(message.chatId);
    } else {
      const buffer =
        this.buffers.get(message.chatId) ?? new Map<number, Message>();
      buffer.set(message.seq, message);
      this.buffers.set(message.chatId, buffer);
      this.sync(message.chatId);
    }
  }
  private drain(chatId: string) {
    const buffer = this.buffers.get(chatId);
    if (!buffer) return;
    let next = (this.cursors.get(chatId) ?? 0) + 1;
    while (buffer.has(next)) {
      const message = buffer.get(next)!;
      buffer.delete(next);
      this.ingest(message);
      next++;
    }
    if (buffer.size) this.sync(chatId);
  }
  private mergeMessage(message: Message) {
    const messages = this.messages.get(message.chatId) ?? [];
    if (!messages.some((item) => item.messageId === message.messageId)) {
      messages.push(message);
      messages.sort((a, b) => a.seq - b.seq);
      this.messages.set(message.chatId, messages);
    }
    if (message.senderId === this.userId)
      this.pending.delete(message.clientMsgId);
    else {
      const last = this.delivered.get(message.chatId);
      if (!last || last.seq < message.seq)
        this.delivered.set(message.chatId, message);
      clearTimeout(this.receiptTimer);
      this.receiptTimer = setTimeout(() => this.flushReceipts(), 100);
    }
  }
  private ingest(message: Message) {
    if (message.seq <= (this.cursors.get(message.chatId) ?? 0)) return;
    this.mergeMessage(message);
    this.cursors.set(message.chatId, message.seq);
    this.persist();
  }
  read(chatId: string) {
    const seq = this.cursors.get(chatId) ?? 0;
    if (seq > (this.reads.get(chatId) ?? 0)) {
      this.reads.set(chatId, seq);
      this.flushReceipts();
    }
  }
  private flushReceipts() {
    for (const message of this.delivered.values())
      this.transmit({
        type: 'delivered',
        chatId: message.chatId,
        messageId: message.messageId,
      });
    for (const [chatId, lastReadSeq] of this.reads)
      this.transmit({ type: 'read', chatId, lastReadSeq });
  }
  sendMessage(chatId: string, body: string) {
    const clientMsgId = crypto.randomUUID();
    const frame: ClientFrame = { type: 'message', chatId, clientMsgId, body };
    if (!body.trim()) throw new Error('Write a message first.');
    if (new TextEncoder().encode(JSON.stringify(frame)).length > 16384)
      throw new Error('This message is too long. Please shorten it.');
    this.pending.set(clientMsgId, {
      clientMsgId,
      chatId,
      body,
      createdAt: new Date().toISOString(),
      received: false,
    });
    this.tracked.add(chatId);
    this.persist();
    this.transmit(frame);
    this.changed();
    return clientMsgId;
  }
  dismiss(clientMsgId: string) {
    this.pending.delete(clientMsgId);
    this.persist();
    this.changed();
  }
  retryMessage(clientMsgId: string) {
    const message = this.pending.get(clientMsgId);
    if (!message?.failure) return;
    delete message.failure;
    message.received = false;
    this.persist();
    this.transmit({
      type: 'message',
      clientMsgId,
      chatId: message.chatId,
      body: message.body,
    });
    this.changed();
  }
  retryConnection() {
    if (this.stopped || !this.online || this.status === 'connected') return;
    clearTimeout(this.reconnectTimer);
    const previous = this.socket;
    this.socket = undefined;
    previous?.close(1000, 'Retry connection');
    this.nextRetryAt = 0;
    this.open();
  }
  private transmit(frame: ClientFrame) {
    if (
      !this.stopped &&
      this.socket?.readyState === 1 &&
      this.status === 'connected'
    )
      this.socket.send(JSON.stringify(frame));
  }
  stop() {
    this.stopped = true;
    clearTimeout(this.reconnectTimer);
    clearTimeout(this.receiptTimer);
    clearInterval(this.heartbeat);
    for (const timer of this.retries) clearTimeout(timer);
    this.retries.clear();
    for (const timer of this.syncing.values()) clearTimeout(timer);
    this.syncing.clear();
    try {
      this.socket?.close(1000, 'Signed out');
    } catch {
      /* A connecting socket closes from onopen. */
    }
    this.status = 'offline';
  }
}
