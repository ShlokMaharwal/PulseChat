import { randomUUID } from 'node:crypto';
import {
  storedMessageSchema,
  uuidSchema,
  type StoredMessage,
  type createDependencies,
} from '@pulsechat/shared';
import type { UserRecord, PublicUser } from './auth.js';
import { HttpError } from './errors.js';

type Database = ReturnType<typeof createDependencies>['db'];
export interface ChatRecord {
  id: string;
  memberIds: string[];
  pairKey: string;
  createdAt: string;
}

export class ChatService {
  private readonly chats;
  private readonly users;
  private readonly messages;

  constructor(db: Database) {
    this.chats = db.collection<ChatRecord>('chats');
    this.users = db.collection<UserRecord>('users');
    this.messages = db.collection<StoredMessage>('messages');
  }

  async initialize() {
    await this.chats.createIndexes([
      { key: { id: 1 }, unique: true, name: 'chatId_unique' },
      { key: { pairKey: 1 }, unique: true, name: 'chatPair_unique' },
      { key: { memberIds: 1 }, name: 'chatMembers' },
    ]);
  }

  async authorize(chatId: string, userId: string): Promise<ChatRecord> {
    if (!uuidSchema.safeParse(chatId).success)
      throw new HttpError(400, 'INVALID_REQUEST', 'Invalid chat identifier');
    const chat = await this.chats.findOne({ id: chatId, memberIds: userId });
    if (!chat)
      throw new HttpError(
        403,
        'FORBIDDEN',
        'You are not a member of this chat',
      );
    return chat;
  }

  async publicChat(
    chat: ChatRecord,
  ): Promise<{ id: string; members: PublicUser[] }> {
    const users = await this.users
      .find(
        { id: { $in: chat.memberIds } },
        { projection: { _id: 0, id: 1, username: 1 } },
      )
      .toArray();
    const byId = new Map(users.map((user) => [user.id, user]));
    return {
      id: chat.id,
      members: chat.memberIds.map((id) => {
        const user = byId.get(id);
        if (!user)
          throw new HttpError(404, 'NOT_FOUND', 'A chat member is unavailable');
        return { id: user.id, username: user.username };
      }),
    };
  }

  async create(userId: string, peerId: string) {
    if (!uuidSchema.safeParse(peerId).success || peerId === userId)
      throw new HttpError(400, 'INVALID_REQUEST', 'Choose another valid user');
    if (!(await this.users.findOne({ id: peerId }, { projection: { id: 1 } })))
      throw new HttpError(404, 'NOT_FOUND', 'User not found');
    const memberIds = [userId, peerId].sort();
    const pairKey = memberIds.join(':');
    const existing = await this.chats.findOne({ pairKey });
    if (existing)
      return { created: false, chat: await this.publicChat(existing) };
    const chat: ChatRecord = {
      id: randomUUID(),
      memberIds,
      pairKey,
      createdAt: new Date().toISOString(),
    };
    try {
      await this.chats.insertOne({ ...chat });
      return { created: true, chat: await this.publicChat(chat) };
    } catch (error) {
      if (
        error &&
        typeof error === 'object' &&
        'code' in error &&
        error.code === 11000
      ) {
        const winner = await this.chats.findOne({ pairKey });
        if (winner)
          return { created: false, chat: await this.publicChat(winner) };
      }
      throw error;
    }
  }

  async list(userId: string) {
    const chats = await this.chats.find({ memberIds: userId }).toArray();
    const summaries = await Promise.all(
      chats.map(async (chat) => {
        const latest = await this.messages.findOne(
          { chatId: chat.id },
          { sort: { seq: -1 }, projection: { _id: 0 } },
        );
        return {
          ...(await this.publicChat(chat)),
          lastMessage: latest ? storedMessageSchema.parse(latest) : null,
          lastSeq: latest?.seq ?? 0,
          createdAt: chat.createdAt,
        };
      }),
    );
    return summaries.sort((a, b) =>
      (b.lastMessage?.createdAt ?? b.createdAt).localeCompare(
        a.lastMessage?.createdAt ?? a.createdAt,
      ),
    );
  }

  async history(
    chatId: string,
    userId: string,
    afterSeq: number,
    limit: number,
  ) {
    await this.authorize(chatId, userId);
    const records = await this.messages
      .find({ chatId, seq: { $gt: afterSeq } }, { projection: { _id: 0 } })
      .sort({ seq: 1 })
      .limit(limit + 1)
      .toArray();
    const messages = records
      .slice(0, limit)
      .map((record) => storedMessageSchema.parse(record));
    return {
      messages,
      hasMore: records.length > limit,
      nextAfterSeq: messages.at(-1)?.seq ?? afterSeq,
    };
  }
}
