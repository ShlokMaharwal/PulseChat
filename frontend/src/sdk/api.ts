import type { Account, Chat, HistoryPage, User } from '../types/protocol';
import { savedAccount, sessionToken, subject, SessionError } from './session';
export class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
  }
}
export async function api<T>(
  path: string,
  token?: string,
  body?: unknown,
  signal?: AbortSignal,
): Promise<T> {
  const userId = token ? subject(token)?.sub : undefined;
  const managed = userId && savedAccount()?.user.id === userId;
  let actualToken = token;
  if (managed) {
    try {
      actualToken = await sessionToken(userId);
    } catch (error) {
      throw new ApiError(
        error instanceof SessionError ? error.status : 503,
        error instanceof Error
          ? error.message
          : 'Session renewal is temporarily unavailable.',
      );
    }
  }
  const perform = () =>
    fetch('/api' + path, {
      method: body === undefined ? 'GET' : 'POST',
      headers: {
        ...(body === undefined ? {} : { 'content-type': 'application/json' }),
        ...(actualToken ? { authorization: `Bearer ${actualToken}` } : {}),
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: signal ?? AbortSignal.timeout(20000),
    });
  let response = await perform();
  if (response.status === 401 && managed) {
    try {
      actualToken = await sessionToken(userId, true);
      response = await perform();
    } catch (error) {
      throw new ApiError(
        error instanceof SessionError ? error.status : 503,
        error instanceof Error
          ? error.message
          : 'Session renewal is temporarily unavailable.',
      );
    }
  }
  const result = await response.json().catch(() => null);
  if (!response.ok)
    throw new ApiError(
      response.status,
      result?.error?.message ?? 'Could not connect. Please try again.',
    );
  return result as T;
}
export const authenticate = (
  mode: 'login' | 'register',
  username: string,
  password: string,
) => api<Account>('/auth/' + mode, undefined, { username, password });
export const listChats = (token: string, signal?: AbortSignal) =>
  api<{ chats: Chat[] }>('/chats', token, undefined, signal);
export const searchUsers = (
  token: string,
  query: string,
  signal?: AbortSignal,
) =>
  api<{ users: User[] }>(
    '/users?query=' + encodeURIComponent(query),
    token,
    undefined,
    signal,
  );
export const createChat = (token: string, userId: string) =>
  api<{ chat: Pick<Chat, 'id' | 'members'> }>('/chats', token, { userId });
export const getPresence = (
  token: string,
  userId: string,
  signal?: AbortSignal,
) =>
  api<{ userId: string; online: boolean }>(
    `/users/${userId}/presence`,
    token,
    undefined,
    signal,
  );
export async function history(
  token: string,
  chatId: string,
  signal?: AbortSignal,
) {
  const messages: HistoryPage['messages'] = [];
  let afterSeq = 0;
  let page: HistoryPage;
  do {
    page = await api<HistoryPage>(
      `/chats/${chatId}/messages?afterSeq=${afterSeq}&limit=100`,
      token,
      undefined,
      signal,
    );
    messages.push(...page.messages);
    if (page.hasMore && page.nextAfterSeq <= afterSeq)
      throw new Error('History could not advance. Please reconnect.');
    afterSeq = page.nextAfterSeq;
  } while (page.hasMore);
  return { messages, receipts: page.receipts ?? [] };
}
