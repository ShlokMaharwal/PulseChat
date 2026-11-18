import { z } from 'zod';

export const uuidSchema = z.string().uuid();
export const seqSchema = z.number().int().nonnegative().safe();
export const registerSchema = z
  .object({
    username: z
      .string()
      .trim()
      .min(3)
      .max(32)
      .regex(/^[a-zA-Z0-9_]+$/),
    password: z
      .string()
      .min(10)
      .refine(
        (value) => Buffer.byteLength(value, 'utf8') <= 72,
        'Password exceeds bcrypt 72-byte limit',
      ),
  })
  .strict();
export const loginSchema = z
  .object({
    username: z.string().min(1).max(32),
    password: z.string().min(1).max(72),
  })
  .strict();
export const clientFrameSchema = z.discriminatedUnion('type', [
  z
    .object({
      type: z.literal('message'),
      clientMsgId: uuidSchema,
      chatId: uuidSchema,
      body: z
        .string()
        .min(1)
        .max(16384)
        .refine((body) => body.trim().length > 0, 'Message cannot be blank'),
    })
    .strict(),
  z
    .object({
      type: z.literal('delivered'),
      chatId: uuidSchema,
      messageId: uuidSchema,
    })
    .strict(),
  z
    .object({
      type: z.literal('read'),
      chatId: uuidSchema,
      lastReadSeq: seqSchema,
    })
    .strict(),
  z
    .object({
      type: z.literal('sync'),
      chatId: uuidSchema,
      afterSeq: seqSchema,
      limit: z.number().int().min(1).max(100).default(100),
    })
    .strict(),
  z.object({ type: z.literal('heartbeat') }).strict(),
]);
export const storedMessageSchema = z
  .object({
    messageId: uuidSchema,
    clientMsgId: uuidSchema,
    chatId: uuidSchema,
    senderId: uuidSchema,
    body: z.string().min(1).max(16384),
    seq: seqSchema.refine((value) => value > 0),
    createdAt: z.string().datetime(),
  })
  .strict();
export const kafkaMessageSchema = storedMessageSchema
  .omit({ seq: true })
  .extend({ recipientIds: z.array(uuidSchema).min(1) });
export const receiptSchema = z
  .object({
    type: z.literal('receipt'),
    chatId: uuidSchema,
    userId: uuidSchema,
    status: z.enum(['delivered', 'read']),
    seq: seqSchema,
    timestamp: z.string().datetime(),
  })
  .strict();
export const serverFrameSchema = z.discriminatedUnion('type', [
  z
    .object({
      type: z.literal('welcome'),
      userId: uuidSchema,
      gatewayId: z.string().min(1),
      heartbeatIntervalMs: z.number().int().positive(),
    })
    .strict(),
  z
    .object({
      type: z.literal('ack'),
      clientMsgId: uuidSchema,
      status: z.enum(['received', 'sent']),
      messageId: uuidSchema.optional(),
      seq: seqSchema.optional(),
    })
    .strict(),
  storedMessageSchema.extend({ type: z.literal('message') }),
  receiptSchema,
  z
    .object({
      type: z.literal('sync'),
      chatId: uuidSchema,
      messages: z.array(storedMessageSchema),
      hasMore: z.boolean(),
      nextAfterSeq: seqSchema,
    })
    .strict(),
  z
    .object({
      type: z.literal('presence'),
      userId: uuidSchema,
      online: z.boolean(),
    })
    .strict(),
  z.object({ type: z.literal('heartbeat') }).strict(),
  z
    .object({
      type: z.literal('reconnect'),
      retryAfterMs: z.number().int().nonnegative(),
    })
    .strict(),
  z
    .object({
      type: z.literal('error'),
      code: z.enum([
        'INVALID_MESSAGE',
        'UNAUTHORIZED',
        'FORBIDDEN',
        'CONFLICT',
        'RATE_LIMITED',
        'PAYLOAD_TOO_LARGE',
        'UNAVAILABLE',
        'INTERNAL_ERROR',
      ]),
      message: z.string(),
      clientMsgId: uuidSchema.optional(),
      retryAfterMs: z.number().int().nonnegative().optional(),
    })
    .strict(),
]);
export type ClientFrame = z.infer<typeof clientFrameSchema>;
export type ServerFrame = z.infer<typeof serverFrameSchema>;
export type StoredMessage = z.infer<typeof storedMessageSchema>;
