import {
  useCallback,
  useEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
} from 'react';
import { PulseClient } from '../sdk/client';
import { ApiError, getPresence, history, listChats } from '../sdk/api';
import type { Account, Chat } from '../types/protocol';
import { sessionToken } from '../sdk/session';
export function useChat(account: Account, onExpired: () => void) {
  const [chats, setChats] = useState<Chat[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [, render] = useReducer((n: number) => n + 1, 0);
  const refreshRef = useRef<() => void>(() => undefined);
  const client = useMemo(
    () =>
      new PulseClient(
        account.user.id,
        account.token,
        `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws`,
        {
          onDiscover: () => refreshRef.current(),
          renewSession: () => sessionToken(account.user.id, true),
        },
      ),
    [account],
  );
  const refreshing = useRef(false);
  const refreshAgain = useRef(false);
  const active = useRef(true);
  const hydrated = useRef(new Set<string>());
  const refresh = useCallback(
    async (signal?: AbortSignal) => {
      if (!active.current) return;
      if (refreshing.current) {
        refreshAgain.current = true;
        return;
      }
      refreshing.current = true;
      try {
        const result = await listChats(account.token, signal);
        const peers = [
          ...new Set(
            result.chats
              .flatMap((chat) => chat.members)
              .filter((user) => user.id !== account.user.id)
              .map((user) => user.id),
          ),
        ];
        await Promise.all(
          peers.map(async (id) => {
            try {
              const value = await getPresence(account.token, id, signal);
              client.presence.set(id, value.online);
            } catch {
              /* Heartbeat snapshots refresh presence if this request fails. */
            }
          }),
        );
        for (const chat of result.chats) {
          client.mergeReceipts(chat.id, chat.receipts ?? []);
          client.track(chat.id);
          if (!hydrated.current.has(chat.id)) {
            const page = await history(account.token, chat.id, signal);
            if (!active.current || signal?.aborted) return;
            client.seed(chat.id, page.messages, page.receipts);
            hydrated.current.add(chat.id);
          }
        }
        if (active.current && !signal?.aborted) {
          setChats(result.chats);
          setError('');
        }
      } catch (err) {
        if (active.current && !signal?.aborted) {
          if (err instanceof ApiError && err.status === 401) onExpired();
          else
            setError(
              err instanceof Error
                ? err.message
                : 'Could not load conversations.',
            );
        }
      } finally {
        refreshing.current = false;
        if (active.current && !signal?.aborted) setLoading(false);
        if (refreshAgain.current && active.current) {
          refreshAgain.current = false;
          queueMicrotask(() => refreshRef.current());
        }
      }
    },
    [account.token, client, onExpired],
  );
  refreshRef.current = () => {
    void refresh();
  };
  useEffect(() => {
    active.current = true;
    const controller = new AbortController();
    const unsubscribe = client.subscribe(() => render());
    client.setOnline(navigator.onLine);
    void refresh(controller.signal).then(() => {
      if (!controller.signal.aborted) client.start();
    });
    const focus = () => {
      void refresh();
      for (const chat of client.messages.keys()) client.sync(chat);
    };
    const online = () => {
      client.setOnline(true);
      focus();
    };
    const offline = () => client.setOnline(false);
    window.addEventListener('online', online);
    window.addEventListener('offline', offline);
    window.addEventListener('focus', focus);
    return () => {
      active.current = false;
      controller.abort();
      unsubscribe();
      client.stop();
      window.removeEventListener('online', online);
      window.removeEventListener('offline', offline);
      window.removeEventListener('focus', focus);
    };
  }, [client, refresh]);
  return { client, chats, loading, error, refresh: () => refresh() };
}
