import { useEffect, useRef, useState } from 'react';
import { createChat, searchUsers } from '../sdk/api';
import type { User } from '../types/protocol';
import { Avatar, Icon } from './Icons';
export function NewChat({
  token,
  close,
  onCreated,
}: {
  token: string;
  close: () => void;
  onCreated: (id: string) => Promise<void>;
}) {
  const [query, setQuery] = useState('');
  const [users, setUsers] = useState<User[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const input = useRef<HTMLInputElement>(null);
  const dialog = useRef<HTMLElement>(null);
  useEffect(() => {
    const previousFocus = document.activeElement as HTMLElement | null;
    input.current?.focus();
    const escape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') close();
      if (event.key === 'Tab') {
        const items = dialog.current?.querySelectorAll<HTMLElement>(
          'button:not(:disabled), input',
        );
        if (!items?.length) return;
        const first = items[0];
        const last = items[items.length - 1];
        if (event.shiftKey && document.activeElement === first) {
          event.preventDefault();
          last.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first.focus();
        }
      }
    };
    document.addEventListener('keydown', escape);
    return () => {
      document.removeEventListener('keydown', escape);
      previousFocus?.focus();
    };
  }, [close]);
  useEffect(() => {
    const controller = new AbortController();
    if (query.trim().length < 2) {
      setUsers([]);
      setError('');
      return;
    }
    setBusy(true);
    const timer = setTimeout(() => {
      void searchUsers(token, query.trim(), controller.signal)
        .then((result) => {
          setUsers(result.users);
          setError('');
        })
        .catch((err: unknown) => {
          if (!controller.signal.aborted)
            setError(
              err instanceof Error ? err.message : 'Search unavailable.',
            );
        })
        .finally(() => {
          if (!controller.signal.aborted) setBusy(false);
        });
    }, 250);
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [query, token]);
  async function choose(user: User) {
    setBusy(true);
    setError('');
    try {
      const result = await createChat(token, user.id);
      await onCreated(result.chat.id);
      close();
    } catch (err) {
      setError(
        err instanceof Error ? err.message : 'Could not start a conversation.',
      );
    } finally {
      setBusy(false);
    }
  }
  return (
    <div
      className="modal-backdrop"
      onClick={(event) => {
        if (event.target === event.currentTarget) close();
      }}
    >
      <section
        ref={dialog}
        className="new-chat-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="new-chat-title"
      >
        <header>
          <div>
            <div className="eyebrow">SAY HELLO</div>
            <h2 id="new-chat-title">A new conversation</h2>
          </div>
          <button
            className="icon-button"
            onClick={close}
            aria-label="Close new conversation"
          >
            <Icon name="close" />
          </button>
        </header>
        <label htmlFor="user-search">Find someone by username</label>
        <div className="search-field">
          <Icon name="search" />
          <input
            ref={input}
            id="user-search"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search usernames…"
            maxLength={32}
          />
        </div>
        <div className="search-results" aria-live="polite">
          {error ? (
            <p className="form-error">{error}</p>
          ) : query.trim().length < 2 ? (
            <p className="empty-search">
              Enter at least two characters to find someone.
            </p>
          ) : busy ? (
            <p className="empty-search">Looking for a match…</p>
          ) : users.length === 0 ? (
            <p className="empty-search">
              No usernames found. Try another search.
            </p>
          ) : (
            users.map((user) => (
              <button
                className="user-result"
                key={user.id}
                onClick={() => void choose(user)}
                disabled={busy}
              >
                <Avatar name={user.username} />
                <div>
                  <strong>{user.username}</strong>
                  <span>Start a conversation</span>
                </div>
                <Icon name="plus" />
              </button>
            ))
          )}
        </div>
      </section>
    </div>
  );
}
