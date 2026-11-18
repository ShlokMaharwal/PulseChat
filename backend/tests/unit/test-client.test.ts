import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { afterEach, describe, expect, it } from 'vitest';
import { WebSocketServer } from 'ws';
import { TestClient } from '../helpers/test-client.js';

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const close of cleanups.splice(0)) await close();
});
async function fixture() {
  const server = createServer((_, response) => {
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ chats: [] }));
  });
  const wss = new WebSocketServer({ server });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('No port');
  const client = new TestClient();
  cleanups.push(async () => {
    await client.close();
    for (const socket of wss.clients) socket.terminate();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });
  return {
    client,
    wss,
    url: `ws://127.0.0.1:${address.port}/ws`,
    userId: randomUUID(),
    chatId: randomUUID(),
  };
}
describe('recovery test client', () => {
  it('buffers a live gap, ingests authoritative history and suppresses duplicate callbacks', async () => {
    const { client, wss, url, userId, chatId } = await fixture();
    const messages = [1, 2, 3].map((seq) => ({
      messageId: randomUUID(),
      clientMsgId: randomUUID(),
      chatId,
      senderId: randomUUID(),
      seq,
      body: `message-${seq}`,
      createdAt: new Date().toISOString(),
    }));
    wss.on('connection', (socket) => {
      socket.send(
        JSON.stringify({
          type: 'welcome',
          userId,
          gatewayId: 'test',
          heartbeatIntervalMs: 15000,
        }),
      );
      socket.on('message', (raw) => {
        const frame = JSON.parse(raw.toString());
        if (frame.type === 'sync')
          socket.send(
            JSON.stringify({
              type: 'sync',
              chatId,
              messages: messages.filter(
                (message) => message.seq > frame.afterSeq,
              ),
              nextAfterSeq: 3,
              hasMore: false,
              receipts: [],
            }),
          );
      });
      socket.send(JSON.stringify({ type: 'message', ...messages[0] }));
      socket.send(JSON.stringify({ type: 'message', ...messages[2] }));
      socket.send(JSON.stringify({ type: 'message', ...messages[2] }));
    });
    await client.connect(url, 'test-token');
    await client.waitForSeq(chatId, 3, 5000);
    expect(client.messages.map((message) => message.seq)).toEqual([1, 2, 3]);
    expect(
      client.frames.filter(
        (frame) => frame.type === 'message' && frame.seq === 3,
      ),
    ).toHaveLength(2);
  });
  it('reconnects and resends the same logical send when the first connection loses its ACK', async () => {
    const { client, wss, url, userId, chatId } = await fixture();
    const ids: string[] = [];
    let connections = 0;
    wss.on('connection', (socket) => {
      const number = ++connections;
      socket.send(
        JSON.stringify({
          type: 'welcome',
          userId,
          gatewayId: `test-${number}`,
          heartbeatIntervalMs: 15000,
        }),
      );
      socket.on('message', (raw) => {
        const frame = JSON.parse(raw.toString());
        if (frame.type !== 'message') return;
        ids.push(frame.clientMsgId);
        if (number === 1) socket.terminate();
        else
          socket.send(
            JSON.stringify({
              type: 'ack',
              clientMsgId: frame.clientMsgId,
              status: 'received',
            }),
          );
      });
    });
    await client.connect(url, 'test-token');
    const clientMsgId = await client.message(chatId, 'retry');
    expect(connections).toBe(2);
    expect(ids).toEqual([clientMsgId, clientMsgId]);
  });
});
