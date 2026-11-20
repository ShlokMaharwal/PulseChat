import { randomUUID } from 'node:crypto';
import {
  storedMessageSchema,
  uuidSchema,
  type StoredMessage,
  type ChatReceiptRecord,
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
  kind?: 'direct' | 'group';
  name?: string;
}

export class ChatService {
  private readonly chats;
  private readonly users;
  private readonly messages;
  private readonly receipts;

  constructor(db: Database) {
    this.chats = db.collection<ChatRecord>('chats');
    this.users = db.collection<UserRecord>('users');
    this.messages = db.collection<StoredMessage>('messages');
    this.receipts = db.collection<ChatReceiptRecord>('chat_receipts');
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

  async publicChat(chat: ChatRecord): Promise<{
    id: string;
    members: PublicUser[];
    kind: 'direct' | 'group';
    name?: string;
  }> {
    const users = await this.users
      .find(
        { id: { $in: chat.memberIds } },
        { projection: { _id: 0, id: 1, username: 1 } },
      )
      .toArray();
    const byId = new Map(users.map((user) => [user.id, user]));
    return {
      id: chat.id,
      kind: chat.kind ?? 'direct',
      ...(chat.kind === 'group' ? { name: chat.name } : {}),
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

  async createGroup(userId: string, name: string, peers: string[]) {
    if (
      peers.length < 2 ||
      peers.length > 19 ||
      new Set(peers).size !== peers.length ||
      peers.includes(userId) ||
      !peers.every((id) => uuidSchema.safeParse(id).success)
    )
      throw new HttpError(
        400,
        'INVALID_REQUEST',
        'Choose 2 to 19 different people for your group',
      );
    const memberIds = [userId, ...peers];
    if (
      (await this.users.countDocuments({ id: { $in: memberIds } })) !==
      memberIds.length
    )
      throw new HttpError(
        404,
        'NOT_FOUND',
        'A selected group member was not found',
      );
    const id = randomUUID();
    const chat: ChatRecord = {
      id,
      memberIds,
      kind: 'group',
      name,
      // Preserve the existing unique pair index without destructive migrations.
      pairKey: 'group:' + id,
      createdAt: new Date().toISOString(),
    };
    await this.chats.insertOne({ ...chat });
    return this.publicChat(chat);
  }

  async list(userId: string) {
    const chats = await this.chats.find({ memberIds: userId }).toArray();
    const summaries = await Promise.all(
      chats.map(async (chat) => {
        const latest = await this.messages.findOne(
          { chatId: chat.id },
          { sort: { seq: -1 }, projection: { _id: 0 } },
        );
        const receipts = await this.receiptState(chat.id);
        const ownRead =
          receipts.find((receipt) => receipt.userId === userId)?.readSeq ?? 0;
        const unreadCount = await this.messages.countDocuments({
          chatId: chat.id,
          senderId: { $ne: userId },
          seq: { $gt: ownRead },
        });
        return {
          ...(await this.publicChat(chat)),
          lastMessage: latest ? storedMessageSchema.parse(latest) : null,
          lastSeq: latest?.seq ?? 0,
          receipts,
          unreadCount,
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
      receipts: await this.receiptState(chatId),
    };
  }

  async receiptState(chatId: string) {
    const records = await this.receipts
      .find(
        { chatId },
        { projection: { _id: 0, userId: 1, deliveredSeq: 1, readSeq: 1 } },
      )
      .toArray();
    return records.map((record) => ({
      userId: record.userId,
      deliveredSeq: record.deliveredSeq,
      readSeq: record.readSeq,
    }));
  }

  async peers(userId: string): Promise<string[]> {
    const records = await this.chats
      .find({ memberIds: userId }, { projection: { memberIds: 1 } })
      .toArray();
    return [
      ...new Set(
        records.flatMap((chat) => chat.memberIds).filter((id) => id !== userId),
      ),
    ];
  }

  async shareChat(userId: string, peerId: string): Promise<boolean> {
    return Boolean(
      await this.chats.findOne(
        { memberIds: { $all: [userId, peerId] } },
        { projection: { id: 1 } },
      ),
    );
  }

  async messageForReceipt(chatId: string, userId: string, messageId: string) {
    await this.authorize(chatId, userId);
    const message = await this.messages.findOne(
      { chatId, messageId },
      { projection: { _id: 0 } },
    );
    if (!message || message.senderId === userId)
      throw new HttpError(
        400,
        'INVALID_REQUEST',
        'Only a received peer message can be acknowledged',
      );
    return storedMessageSchema.parse(message);
  }

  async validateRead(chatId: string, userId: string, seq: number) {
    const chat = await this.authorize(chatId, userId);
    const latest = await this.messages.findOne(
      { chatId },
      { sort: { seq: -1 }, projection: { seq: 1 } },
    );
    if (seq > (latest?.seq ?? 0))
      throw new HttpError(
        400,
        'INVALID_REQUEST',
        'Read position exceeds stored history',
      );
    return chat;
  }
}
