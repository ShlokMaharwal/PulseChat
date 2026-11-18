import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import WebSocket from 'ws';
import {
  serverFrameSchema,
  type ServerFrame,
  type StoredMessage,
} from '../../packages/shared/src/schemas.js';

export class TestClient extends EventEmitter {
  readonly frames: ServerFrame[] = [];
  readonly messages: StoredMessage[] = [];
  private socket: WebSocket | undefined;
  private failure: Error | undefined;

  constructor() {
    super();
    this.setMaxListeners(256);
  }

  async connect(url: string, token: string) {
    this.socket = new WebSocket(url, ['pulsechat', `jwt.${token}`]);
    this.socket.on('error', (error) => {
      this.failure = error;
      this.emit('frame');
    });
    this.socket.on('message', (raw) => {
      try {
        const frame = serverFrameSchema.parse(JSON.parse(raw.toString()));
        this.frames.push(frame);
        if (frame.type === 'message') this.messages.push(frame);
        this.emit('frame', frame);
      } catch (error) {
        this.failure =
          error instanceof Error ? error : new Error(String(error));
        this.emit('frame');
      }
    });
    return await this.waitFor((frame) => frame.type === 'welcome');
  }

  waitFor(
    predicate: (frame: ServerFrame) => boolean,
    timeout = 60000,
  ): Promise<ServerFrame> {
    const existing = this.frames.find(predicate);
    if (existing) return Promise.resolve(existing);
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
        if (this.failure) {
          finish();
          reject(this.failure);
          return;
        }
        const frame = this.frames.find(predicate);
        if (frame) {
          finish();
          resolve(frame);
        }
      };
      this.on('frame', receive);
      receive();
    });
  }

  send(frame: unknown) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN)
      throw new Error('Client is disconnected');
    this.socket.send(JSON.stringify(frame));
  }

  async message(chatId: string, body: string, clientMsgId = randomUUID()) {
    const accepted = this.waitFor(
      (frame) =>
        (frame.type === 'ack' || frame.type === 'error') &&
        frame.clientMsgId === clientMsgId,
    );
    this.send({ type: 'message', chatId, body, clientMsgId });
    const result = await accepted;
    if (result.type === 'error')
      throw new Error(`${result.code}: ${result.message}`);
    return clientMsgId;
  }

  async close() {
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
