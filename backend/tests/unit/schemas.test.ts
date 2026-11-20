import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import {
  clientFrameSchema,
  registerSchema,
  serverFrameSchema,
} from '../../packages/shared/src/schemas.js';

describe('protocol validation', () => {
  const message = {
    type: 'message',
    chatId: randomUUID(),
    clientMsgId: randomUUID(),
    body: 'Hello',
  };
  it('validates ephemeral typing frames without accepting a spoofed user', () => {
    const typing = { type: 'typing', chatId: message.chatId, isTyping: true };
    expect(clientFrameSchema.parse(typing)).toEqual(typing);
    expect(
      clientFrameSchema.safeParse({ ...typing, userId: randomUUID() }).success,
    ).toBe(false);
    expect(
      clientFrameSchema.safeParse({ ...typing, isTyping: 'yes' }).success,
    ).toBe(false);
    expect(
      serverFrameSchema.safeParse({
        ...typing,
        userId: randomUUID(),
        sessionId: randomUUID(),
        expiresInMs: 6000,
      }).success,
    ).toBe(true);
    expect(
      serverFrameSchema.safeParse({
        ...typing,
        userId: randomUUID(),
        sessionId: randomUUID(),
        expiresInMs: 60000,
      }).success,
    ).toBe(false);
  });
  it('accepts a valid message and bounds sync pagination', () => {
    expect(clientFrameSchema.parse(message)).toEqual(message);
    expect(
      clientFrameSchema.parse({
        type: 'sync',
        chatId: message.chatId,
        afterSeq: 0,
      }),
    ).toMatchObject({ limit: 100 });
    expect(
      clientFrameSchema.safeParse({
        type: 'sync',
        chatId: message.chatId,
        afterSeq: -1,
      }).success,
    ).toBe(false);
    expect(
      clientFrameSchema.safeParse({
        type: 'sync',
        chatId: message.chatId,
        afterSeq: 0,
        limit: 101,
      }).success,
    ).toBe(false);
  });
  it.each([
    { ...message, body: '' },
    { ...message, body: '   ' },
    { ...message, body: 'x'.repeat(16385) },
    { ...message, chatId: 'not-a-uuid' },
    { ...message, senderId: randomUUID() },
    { type: 'unknown' },
  ])('rejects invalid or spoofed messages', (input) => {
    expect(clientFrameSchema.safeParse(input).success).toBe(false);
  });
  it('enforces bcrypt byte limits for multibyte registration passwords', () => {
    expect(
      registerSchema.safeParse({ username: 'alice', password: 'x'.repeat(72) })
        .success,
    ).toBe(true);
    expect(
      registerSchema.safeParse({ username: 'alice', password: '🔒'.repeat(19) })
        .success,
    ).toBe(false);
  });
  it('validates server errors and positive persisted sequence numbers', () => {
    expect(
      serverFrameSchema.safeParse({
        type: 'error',
        code: 'FORBIDDEN',
        message: 'Access denied',
      }).success,
    ).toBe(true);
    expect(
      serverFrameSchema.safeParse({
        type: 'ack',
        status: 'sent',
        clientMsgId: message.clientMsgId,
        seq: Number.MAX_SAFE_INTEGER + 1,
      }).success,
    ).toBe(false);
  });
});
