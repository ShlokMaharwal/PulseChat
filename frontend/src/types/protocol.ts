// Written from docs/PROTOCOL.md; the frontend imports no backend code.
export interface User {
  id: string;
  username: string;
}
export interface Account {
  user: User;
  token: string;
  expiresIn: number;
}
export interface Message {
  messageId: string;
  clientMsgId: string;
  chatId: string;
  senderId: string;
  body: string;
  seq: number;
  createdAt: string;
}
export interface Receipt {
  userId: string;
  deliveredSeq: number;
  readSeq: number;
}
export interface Chat {
  id: string;
  members: User[];
  lastMessage: Message | null;
  lastSeq: number;
  createdAt: string;
  unreadCount: number;
  receipts: Receipt[];
}
export interface HistoryPage {
  messages: Message[];
  hasMore: boolean;
  nextAfterSeq: number;
  receipts: Receipt[];
}
export type ServerFrame =
  | {
      type: 'welcome';
      userId: string;
      gatewayId: string;
      heartbeatIntervalMs: number;
    }
  | {
      type: 'ack';
      clientMsgId: string;
      status: 'received' | 'sent';
      messageId?: string;
      seq?: number;
    }
  | ({ type: 'message' } & Message)
  | {
      type: 'receipt';
      chatId: string;
      userId: string;
      status: 'delivered' | 'read';
      seq: number;
      timestamp: string;
    }
  | ({ type: 'sync'; chatId: string } & HistoryPage)
  | { type: 'presence'; userId: string; online: boolean }
  | { type: 'heartbeat' }
  | { type: 'reconnect'; retryAfterMs: number }
  | {
      type: 'error';
      code: string;
      message: string;
      clientMsgId?: string;
      retryAfterMs?: number;
      chatId?: string;
      operation?: 'message' | 'sync' | 'delivered' | 'read';
    };
export type ClientFrame =
  | { type: 'message'; chatId: string; clientMsgId: string; body: string }
  | { type: 'sync'; chatId: string; afterSeq: number; limit: number }
  | { type: 'delivered'; chatId: string; messageId: string }
  | { type: 'read'; chatId: string; lastReadSeq: number }
  | { type: 'heartbeat' };
