import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { PulseClient } from '../src/sdk/client';
import type { ClientFrame, Message, ServerFrame } from '../src/types/protocol';
const own = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const peer = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
const chat = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc';
class MemoryStorage {
  values = new Map<string, string>();
  getItem(key: string) {
    return this.values.get(key) ?? null;
  }
  setItem(key: string, value: string) {
    this.values.set(key, value);
  }
}
class MockSocket {
  readyState = 1;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: ((event: { code: number }) => void) | null = null;
  onerror: (() => void) | null = null;
  onopen: (() => void) | null = null;
  readonly sent: ClientFrame[] = [];
  send(value: string) {
    this.sent.push(JSON.parse(value) as ClientFrame);
  }
  close(code = 1000) {
    this.readyState = 3;
    this.onclose?.({ code });
  }
  frame(frame: ServerFrame) {
    this.onmessage?.({ data: JSON.stringify(frame) });
  }
  welcome() {
    this.frame({
      type: 'welcome',
      userId: own,
      gatewayId: 'gateway-test',
      heartbeatIntervalMs: 15000,
    });
  }
}
function message(
  seq: number,
  senderId = peer,
  clientMsgId = crypto.randomUUID(),
): Message {
  return {
    seq,
    senderId,
    clientMsgId,
    messageId: `00000000-0000-4000-8000-${String(seq).padStart(12, '0')}`,
    chatId: chat,
    body: `message-${seq}`,
    createdAt: '2026-11-19T00:00:00.000Z',
  };
}
const clients: PulseClient[] = [];
function setup(storage = new MemoryStorage()) {
  const sockets: MockSocket[] = [];
  const factory = vi.fn(() => {
    const socket = new MockSocket();
    sockets.push(socket);
    return socket as unknown as WebSocket;
  });
  const client = new PulseClient(own, 'opaque-token', 'ws://localhost/ws', {
    storage,
    random: () => 0.5,
    socketFactory: factory,
  });
  clients.push(client);
  return { client, sockets, factory, storage };
}
beforeEach(() => vi.useFakeTimers());
afterEach(() => {
  clients.splice(0).forEach((client) => client.stop());
  vi.useRealTimers();
});
describe('independent browser recovery SDK', () => {
  it('marks an unavailable initial connection as degraded until welcome', () => {
    const { client, sockets } = setup();
    client.start();
    sockets[0]!.onerror?.();
    expect(client.degraded).toBe(true);
    sockets[0]!.welcome();
    expect(client.degraded).toBe(false);
  });
  it('shows dependency degradation and clears it after a successful recovery response', () => {
    const { client, sockets } = setup();
    client.start();
    sockets[0]!.welcome();
    sockets[0]!.frame({
      type: 'error',
      code: 'UNAVAILABLE',
      message: 'Service is temporarily unavailable',
    });
    expect(client.degraded).toBe(true);
    expect(client.error).toContain('temporarily unavailable');
    sockets[0]!.frame({ type: 'heartbeat' });
    expect(client.degraded).toBe(false);
    expect(client.error).toBe('');
  });
  it('opens one socket when coming online during silent token renewal', async () => {
    let finish: ((value: string) => void) | undefined;
    const renewal = new Promise<string>((resolve) => {
      finish = resolve;
    });
    const renewSession = vi.fn(() => renewal);
    const socketFactory = vi.fn(() => new MockSocket() as unknown as WebSocket);
    const expired = 'x.' + btoa(JSON.stringify({ exp: 1 })) + '.x';
    const fresh =
      'x.' + btoa(JSON.stringify({ exp: Date.now() / 1000 + 900 })) + '.x';
    const client = new PulseClient(own, expired, 'ws://localhost/ws', {
      storage: new MemoryStorage(),
      socketFactory,
      renewSession,
    });
    clients.push(client);
    client.start();
    client.setOnline(false);
    client.setOnline(true);
    expect(renewSession).toHaveBeenCalledTimes(1);
    finish!(fresh);
    await vi.advanceTimersByTimeAsync(0);
    expect(socketFactory).toHaveBeenCalledTimes(1);
  });
  it('queues while the browser is offline and resumes with the original ID', () => {
    const { client, sockets } = setup();
    client.start();
    sockets[0]!.welcome();
    client.setOnline(false);
    expect(client.status).toBe('offline');
    const id = client.sendMessage(chat, 'Waiting for the network');
    expect(sockets[0]!.sent.some((frame) => frame.type === 'message')).toBe(
      false,
    );
    vi.advanceTimersByTime(60000);
    expect(sockets).toHaveLength(1);
    client.setOnline(true);
    expect(sockets).toHaveLength(2);
    sockets[1]!.welcome();
    expect(sockets[1]!.sent).toContainEqual({
      type: 'message',
      clientMsgId: id,
      chatId: chat,
      body: 'Waiting for the network',
    });
    expect(client.status).toBe('connected');
  });
  it('authenticates through protocols and transitions only after welcome', () => {
    const { client, sockets, factory } = setup();
    client.start();
    expect(client.status).toBe('connecting');
    expect(factory).toHaveBeenCalledWith('ws://localhost/ws', [
      'pulsechat',
      'jwt.opaque-token',
    ]);
    sockets[0]!.welcome();
    expect(client.status).toBe('connected');
    expect(client.gatewayId).toBe('gateway-test');
  });
  it('buffers a live gap, merges ordered sync, dedupes replay and persists the cursor', async () => {
    const { client, sockets, storage } = setup();
    client.seed(chat, [message(1)], []);
    client.start();
    const socket = sockets[0]!;
    socket.welcome();
    socket.frame({
      type: 'sync',
      chatId: chat,
      messages: [],
      receipts: [],
      hasMore: false,
      nextAfterSeq: 1,
    });
    socket.frame({ type: 'message', ...message(3) });
    expect(client.messages.get(chat)?.map((item) => item.seq)).toEqual([1]);
    expect(socket.sent.at(-1)).toMatchObject({ type: 'sync', afterSeq: 1 });
    socket.frame({
      type: 'sync',
      chatId: chat,
      messages: [message(2), message(3)],
      receipts: [],
      hasMore: false,
      nextAfterSeq: 3,
    });
    socket.frame({ type: 'message', ...message(3) });
    expect(client.messages.get(chat)?.map((item) => item.seq)).toEqual([
      1, 2, 3,
    ]);
    expect(JSON.parse(storage.getItem(`pulsechat.cursors.${own}`)!)[chat]).toBe(
      3,
    );
    await vi.advanceTimersByTimeAsync(100);
    expect(socket.sent).toContainEqual({
      type: 'delivered',
      chatId: chat,
      messageId: message(3).messageId,
    });
    client.read(chat);
    expect(socket.sent).toContainEqual({
      type: 'read',
      chatId: chat,
      lastReadSeq: 3,
    });
  });
  it('paginates authoritative history while tolerating permanent DLQ integer gaps', () => {
    const { client, sockets } = setup();
    client.track(chat);
    client.start();
    const socket = sockets[0]!;
    socket.welcome();
    socket.frame({
      type: 'sync',
      chatId: chat,
      messages: [message(1), message(3)],
      hasMore: true,
      nextAfterSeq: 3,
      receipts: [],
    });
    expect(socket.sent.at(-1)).toMatchObject({ type: 'sync', afterSeq: 3 });
    socket.frame({
      type: 'sync',
      chatId: chat,
      messages: [message(5)],
      hasMore: false,
      nextAfterSeq: 5,
      receipts: [],
    });
    expect(client.messages.get(chat)?.map((item) => item.seq)).toEqual([
      1, 3, 5,
    ]);
  });
  it('resends unacked sends with the same UUID, then relies on sync after a received ACK', async () => {
    const { client, sockets } = setup();
    client.start();
    sockets[0]!.welcome();
    const id = client.sendMessage(chat, 'hello');
    sockets[0]!.close(1006);
    expect(client.status).toBe('reconnecting');
    await vi.advanceTimersByTimeAsync(500);
    sockets[1]!.welcome();
    expect(sockets[1]!.sent).toContainEqual({
      type: 'message',
      clientMsgId: id,
      chatId: chat,
      body: 'hello',
    });
    sockets[1]!.frame({ type: 'ack', clientMsgId: id, status: 'received' });
    sockets[1]!.close(1006);
    await vi.advanceTimersByTimeAsync(500);
    sockets[2]!.welcome();
    expect(
      sockets[2]!.sent.filter((frame) => frame.type === 'message'),
    ).toHaveLength(0);
    sockets[2]!.frame({
      type: 'sync',
      chatId: chat,
      messages: [{ ...message(1, own, id), body: 'hello' }],
      receipts: [],
      hasMore: false,
      nextAfterSeq: 1,
    });
    expect(client.pending.size).toBe(0);
    expect(client.messages.get(chat)?.[0]?.body).toBe('hello');
  });
  it('restores user-scoped cursors and never regresses receipt state', () => {
    const storage = new MemoryStorage();
    storage.setItem(`pulsechat.cursors.${own}`, JSON.stringify({ [chat]: 7 }));
    const { client, sockets } = setup(storage);
    client.start();
    sockets[0]!.welcome();
    expect(sockets[0]!.sent).toContainEqual({
      type: 'sync',
      chatId: chat,
      afterSeq: 7,
      limit: 100,
    });
    client.mergeReceipts(chat, [{ userId: peer, deliveredSeq: 5, readSeq: 5 }]);
    client.mergeReceipts(chat, [{ userId: peer, deliveredSeq: 2, readSeq: 0 }]);
    expect(client.receipts.get(chat)?.get(peer)).toEqual({
      userId: peer,
      deliveredSeq: 5,
      readSeq: 5,
    });
    const other = new PulseClient(peer, 'opaque-token', 'ws://localhost/ws', {
      storage,
    });
    clients.push(other);
    expect(other.cursors.size).toBe(0);
  });
  it('honors a reconnect hint and cancels reconnect on deliberate shutdown', async () => {
    const { client, sockets } = setup();
    client.start();
    sockets[0]!.welcome();
    sockets[0]!.frame({ type: 'reconnect', retryAfterMs: 2000 });
    await vi.advanceTimersByTimeAsync(1999);
    expect(sockets).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(sockets).toHaveLength(2);
    client.stop();
    await vi.advanceTimersByTimeAsync(60000);
    expect(sockets).toHaveLength(2);
  });
  it('keeps a pending send when storage fails and rejects oversized UTF-8 frames', () => {
    const storage = new MemoryStorage();
    storage.setItem = () => {
      throw new Error('quota');
    };
    const { client, sockets } = setup(storage);
    client.start();
    sockets[0]!.welcome();
    expect(() => client.sendMessage(chat, 'hello')).not.toThrow();
    expect(client.pending.size).toBe(1);
    expect(() => client.sendMessage(chat, 'x'.repeat(17000))).toThrow(
      'too long',
    );
  });
  it('retries rate-limited sends using their original ID', async () => {
    const { client, sockets } = setup();
    client.start();
    sockets[0]!.welcome();
    const id = client.sendMessage(chat, 'paced');
    sockets[0]!.frame({
      type: 'error',
      code: 'RATE_LIMITED',
      message: 'Wait',
      operation: 'message',
      chatId: chat,
      clientMsgId: id,
      retryAfterMs: 1000,
    });
    await vi.advanceTimersByTimeAsync(1100);
    expect(
      sockets[0]!.sent.filter(
        (frame) => frame.type === 'message' && frame.clientMsgId === id,
      ),
    ).toHaveLength(2);
  });
});
