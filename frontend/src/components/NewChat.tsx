import { useEffect, useRef, useState } from 'react';
import { createChat, createGroup, searchUsers } from '../sdk/api';
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
  const [searching, setSearching] = useState(false);
  const [mode, setMode] = useState<'direct' | 'group'>('direct');
  const [groupName, setGroupName] = useState('');
  const [members, setMembers] = useState<User[]>([]);
  const [error, setError] = useState('');
  const input = useRef<HTMLInputElement>(null);
  const nameInput = useRef<HTMLInputElement>(null);
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
    if (mode === 'group') nameInput.current?.focus();
    else input.current?.focus();
  }, [mode]);
  useEffect(() => {
    const controller = new AbortController();
    if (query.trim().length < 2) {
      setUsers([]);
      setError('');
      setSearching(false);
      return;
    }
    setSearching(true);
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
          if (!controller.signal.aborted) setSearching(false);
        });
    }, 250);
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [query, token]);
  async function choose(user: User) {
    if (mode === 'group') {
      setMembers((selected) =>
        selected.some((member) => member.id === user.id)
          ? selected.filter((member) => member.id !== user.id)
          : selected.length < 19
            ? [...selected, user]
            : selected,
      );
      return;
    }
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
  async function makeGroup() {
    if (busy || members.length < 2 || !groupName.trim()) return;
    setBusy(true);
    setError('');
    try {
      const result = await createGroup(
        token,
        groupName.trim(),
        members.map((member) => member.id),
      );
      await onCreated(result.chat.id);
      close();
    } catch (err) {
      setError(
        err instanceof Error ? err.message : 'Could not create the group.',
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
            <h2 id="new-chat-title">
              {mode === 'group' ? 'Create a group' : 'A new conversation'}
            </h2>
          </div>
          <button
            className="icon-button"
            onClick={close}
            aria-label="Close new conversation"
          >
            <Icon name="close" />
          </button>
        </header>
        <div
          className="conversation-type"
          role="group"
          aria-label="Conversation type"
        >
          <button
            aria-pressed={mode === 'direct'}
            onClick={() => {
              setMode('direct');
              setError('');
            }}
            disabled={busy}
          >
            Direct message
          </button>
          <button
            aria-pressed={mode === 'group'}
            onClick={() => {
              setMode('group');
              setError('');
            }}
            disabled={busy}
          >
            <Icon name="group" /> New group
          </button>
        </div>
        {mode === 'group' && (
          <>
            <label htmlFor="group-name">Group name</label>
            <input
              ref={nameInput}
              className="group-name-input"
              id="group-name"
              value={groupName}
              onChange={(event) => setGroupName(event.target.value)}
              placeholder="e.g. Project team"
              maxLength={60}
              disabled={busy}
            />
            <div className="selected-members" aria-label="Selected members">
              {members.map((member) => (
                <button
                  key={member.id}
                  onClick={() =>
                    setMembers((selected) =>
                      selected.filter((person) => person.id !== member.id),
                    )
                  }
                  aria-label={`Remove ${member.username}`}
                  disabled={busy}
                >
                  {member.username}
                  <Icon name="close" />
                </button>
              ))}
            </div>
            <p className="group-help">
              Choose at least 2 people. Up to 20 members, including you.
            </p>
          </>
        )}
        <label htmlFor="user-search">Find someone by username</label>
        <div className="search-field">
          <Icon name="search" />
          <input
            ref={input}
            id="user-search"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search usernames…"
            disabled={busy}
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
          ) : searching ? (
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
                disabled={
                  busy ||
                  (mode === 'group' &&
                    members.length >= 19 &&
                    !members.some((member) => member.id === user.id))
                }
                aria-pressed={
                  mode === 'group'
                    ? members.some((member) => member.id === user.id)
                    : undefined
                }
              >
                <Avatar name={user.username} />
                <div>
                  <strong>{user.username}</strong>
                  <span>
                    {mode === 'group'
                      ? members.some((member) => member.id === user.id)
                        ? 'Selected'
                        : 'Add to group'
                      : 'Start a conversation'}
                  </span>
                </div>
                <Icon name="plus" />
              </button>
            ))
          )}
        </div>
        {mode === 'group' && (
          <button
            className="primary-button create-group-button"
            onClick={() => void makeGroup()}
            disabled={busy || members.length < 2 || !groupName.trim()}
          >
            {busy
              ? 'Creating…'
              : `Create group${members.length ? ` · ${members.length + 1} members` : ''}`}
            <Icon name="group" />
          </button>
        )}
      </section>
    </div>
  );
}
