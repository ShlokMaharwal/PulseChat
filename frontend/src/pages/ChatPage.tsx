import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type FormEvent,
} from 'react';
import type { Account, Chat, Message } from '../types/protocol';
import { useChat } from '../hooks/useChat';
import { Avatar, Brand, Icon } from '../components/Icons';
import { NewChat } from '../components/NewChat';
const time = (value: string) =>
  new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
  }).format(new Date(value));
function peer(chat: Chat, ownId: string) {
  return chat.members.find((member) => member.id !== ownId) ?? chat.members[0]!;
}
const isGroup = (chat: Chat) => chat.kind === 'group';
const chatName = (chat: Chat, ownId: string) =>
  isGroup(chat) ? (chat.name ?? 'Group') : peer(chat, ownId).username;
export function ChatPage({
  account,
  logout,
  theme,
  toggleTheme,
}: {
  account: Account;
  logout: () => void;
  theme: string;
  toggleTheme: () => void;
}) {
  const { client, chats, loading, error, refresh } = useChat(account, logout);
  const [selected, setSelected] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);
  const [filter, setFilter] = useState('');
  const [draft, setDraft] = useState('');
  const [sendError, setSendError] = useState('');
  const [, setNow] = useState(Date.now());
  const [firstUnread, setFirstUnread] = useState<string | null>(null);
  const unreadScroll = useRef(false);
  const pinned = useRef(true);
  const scroll = useRef<HTMLDivElement>(null);
  const bottom = useRef<HTMLDivElement>(null);
  const textarea = useRef<HTMLTextAreaElement>(null);
  const firstSelection = useRef(false);
  const closeNew = useCallback(() => setCreating(false), []);
  const active = chats.find((chat) => chat.id === selected);
  const messages = selected ? (client.messages.get(selected) ?? []) : [];
  const pending = [...client.pending.values()].filter(
    (item) => item.chatId === selected,
  );
  const lastSeq = selected ? (client.cursors.get(selected) ?? 0) : 0;
  const typingPeople = active
    ? client
        .typingUsers(active.id)
        .map(
          (id) => active.members.find((member) => member.id === id)?.username,
        )
        .filter((name): name is string => Boolean(name))
    : [];
  const typingLabel = typingPeople.length
    ? `${typingPeople.slice(0, 2).join(' and ')}${typingPeople.length > 2 ? ' and others' : ''} ${typingPeople.length === 1 ? 'is' : 'are'} typing…`
    : '';
  const selectChat = useCallback(
    (id: string) => {
      const read = client.receipts.get(id)?.get(account.user.id)?.readSeq ?? 0;
      const unread = client.messages
        .get(id)
        ?.find(
          (message) =>
            message.senderId !== account.user.id && message.seq > read,
        );
      setFirstUnread(unread?.messageId ?? null);
      unreadScroll.current = true;
      pinned.current = true;
      setSelected(id);
    },
    [client, account.user.id],
  );
  useEffect(() => {
    if (client.status === 'connected' && !client.degraded) return;
    const timer = setInterval(() => setNow(Date.now()), 250);
    return () => clearInterval(timer);
  }, [client, client.status, client.degraded]);
  useEffect(() => {
    if (!loading && !firstSelection.current) {
      firstSelection.current = true;
      if (chats[0]) selectChat(chats[0].id);
    }
  }, [loading, chats, selectChat]);
  useEffect(() => {
    setDraft('');
    setSendError('');
    if (selected) client.sync(selected);
    return () => {
      if (selected) client.setTyping(selected, false);
    };
  }, [selected, client]);
  useEffect(() => {
    const stop = () => {
      if (selected) client.setTyping(selected, false);
    };
    const hidden = () => {
      if (document.visibilityState !== 'visible') stop();
    };
    window.addEventListener('blur', stop);
    document.addEventListener('visibilitychange', hidden);
    return () => {
      window.removeEventListener('blur', stop);
      document.removeEventListener('visibilitychange', hidden);
    };
  }, [selected, client]);
  useEffect(() => {
    if (unreadScroll.current && firstUnread) {
      const target = scroll.current?.querySelector<HTMLElement>(
        `[data-message-id="${firstUnread}"]`,
      );
      if (target) {
        target.scrollIntoView({ block: 'center' });
        unreadScroll.current = false;
        pinned.current = false;
        return;
      }
    }
    if (pinned.current)
      bottom.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
    unreadScroll.current = false;
  }, [messages.length, pending.length, selected]);
  useEffect(() => {
    const mark = () => {
      if (selected && document.visibilityState === 'visible')
        client.read(selected);
    };
    mark();
    document.addEventListener('visibilitychange', mark);
    window.addEventListener('focus', mark);
    return () => {
      document.removeEventListener('visibilitychange', mark);
      window.removeEventListener('focus', mark);
    };
  }, [selected, lastSeq, client]);
  useEffect(() => {
    if (textarea.current) {
      textarea.current.style.height = 'auto';
      textarea.current.style.height = `${Math.min(150, textarea.current.scrollHeight)}px`;
    }
  }, [draft]);
  const sorted = [...chats].sort((a, b) =>
    (
      (client.messages.get(b.id)?.at(-1) ?? b.lastMessage)?.createdAt ??
      b.createdAt
    ).localeCompare(
      (client.messages.get(a.id)?.at(-1) ?? a.lastMessage)?.createdAt ??
        a.createdAt,
    ),
  );
  function send(event?: FormEvent) {
    event?.preventDefault();
    if (!selected || !draft.trim()) return;
    try {
      client.sendMessage(selected, draft.trim());
      pinned.current = true;
      setDraft('');
      setSendError('');
      textarea.current?.focus();
    } catch (err) {
      setSendError(
        err instanceof Error ? err.message : 'Could not send this message.',
      );
    }
  }
  function status(message: Message) {
    const peers =
      active?.members.filter((member) => member.id !== account.user.id) ?? [];
    const allAt = (field: 'readSeq' | 'deliveredSeq') =>
      peers.length > 0 &&
      peers.every(
        (member) =>
          (client.receipts.get(message.chatId)?.get(member.id)?.[field] ?? 0) >=
          message.seq,
      );
    return allAt('readSeq')
      ? 'Read'
      : allAt('deliveredSeq')
        ? 'Delivered'
        : 'Sent';
  }
  return (
    <main className={`chat-app ${selected ? 'conversation-selected' : ''}`}>
      <aside className="sidebar">
        <header className="sidebar-header">
          <Brand />
          <div className="workspace-label">YOUR SPACE</div>
          <div className="sidebar-title">
            <h1>Messages</h1>
            <button
              className="icon-button new-button"
              aria-label="New conversation"
              title="New conversation"
              onClick={() => setCreating(true)}
            >
              <Icon name="plus" />
            </button>
          </div>
          <div className="search-field sidebar-search">
            <Icon name="search" />
            <input
              aria-label="Filter conversations"
              placeholder="Search conversations"
              value={filter}
              onChange={(event) => setFilter(event.target.value)}
            />
          </div>
        </header>
        <nav className="chat-list" aria-label="Conversations">
          {loading ? (
            <p className="sidebar-empty">Loading your conversations…</p>
          ) : (
            sorted
              .filter((chat) =>
                chatName(chat, account.user.id)
                  .toLowerCase()
                  .includes(filter.toLowerCase()),
              )
              .map((chat) => {
                const person = peer(chat, account.user.id);
                const name = chatName(chat, account.user.id);
                const typing = client.typingUsers(chat.id).length > 0;
                const last =
                  client.messages.get(chat.id)?.at(-1) ?? chat.lastMessage;
                const read =
                  client.receipts.get(chat.id)?.get(account.user.id)?.readSeq ??
                  0;
                const unread =
                  chat.id === selected && document.visibilityState === 'visible'
                    ? 0
                    : (client.messages.get(chat.id) ?? []).filter(
                        (message) =>
                          message.senderId !== account.user.id &&
                          message.seq > read,
                      ).length;
                return (
                  <button
                    key={chat.id}
                    className={`chat-row ${selected === chat.id ? 'selected' : ''}`}
                    onClick={() => selectChat(chat.id)}
                    aria-current={selected === chat.id ? 'page' : undefined}
                  >
                    <span className="avatar-wrap">
                      <Avatar name={name} group={isGroup(chat)} />
                      {!isGroup(chat) && client.presence.get(person.id) && (
                        <span className="avatar-online" />
                      )}
                    </span>
                    <span className="chat-row-copy">
                      <span className="chat-row-title">
                        <strong>{name}</strong>
                        {last && <time>{time(last.createdAt)}</time>}
                      </span>
                      <span className="chat-preview">
                        <span className={typing ? 'typing-preview' : undefined}>
                          {typing
                            ? 'typing…'
                            : last
                              ? `${last.senderId === account.user.id ? 'You: ' : isGroup(chat) ? `${chat.members.find((member) => member.id === last.senderId)?.username ?? 'Member'}: ` : ''}${last.body}`
                              : 'Say the first hello.'}
                        </span>
                        {unread > 0 && (
                          <span
                            className="unread-badge"
                            aria-label={`${unread} unread messages`}
                          >
                            {unread > 99 ? '99+' : unread}
                          </span>
                        )}
                      </span>
                    </span>
                  </button>
                );
              })
          )}
          {!loading && chats.length === 0 && (
            <div className="sidebar-empty">
              <p>A fresh start.</p>
              <span>Your conversations will appear here.</span>
              <button className="text-button" onClick={() => setCreating(true)}>
                Start a conversation <span aria-hidden="true">→</span>
              </button>
            </div>
          )}
          {filter &&
            !sorted.some((chat) =>
              chatName(chat, account.user.id)
                .toLowerCase()
                .includes(filter.toLowerCase()),
            ) && <p className="sidebar-empty">No conversations match.</p>}
        </nav>
        <footer className="profile">
          <Avatar name={account.user.username} small />
          <div>
            <strong>{account.user.username}</strong>
            <span>
              <i
                className={`status-dot ${client.status !== 'connected' ? 'muted' : ''}`}
              />
              {client.status === 'connected'
                ? 'Connected'
                : client.status === 'expired'
                  ? 'Session expired'
                  : 'Reconnecting'}
            </span>
          </div>
          <button
            className="icon-button"
            onClick={toggleTheme}
            aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}
            title="Switch theme"
          >
            <Icon name={theme === 'dark' ? 'sun' : 'moon'} />
          </button>
          <button
            className="icon-button"
            onClick={logout}
            aria-label="Sign out"
            title="Sign out"
          >
            <Icon name="logout" />
          </button>
        </footer>
      </aside>
      <section className="conversation" aria-label="Conversation">
        {(client.status !== 'connected' || client.degraded) && (
          <div
            className="notice connection-banner"
            role="status"
            aria-live="polite"
          >
            <span>
              {client.status === 'offline'
                ? 'You are offline. Messages will send when you reconnect.'
                : client.status === 'expired'
                  ? 'Your session expired. Sign in again.'
                  : client.status === 'connecting'
                    ? 'Connecting…'
                    : client.status === 'reconnecting'
                      ? `Connection interrupted. Retrying${client.nextRetryAt ? ` in ${Math.max(0, Math.ceil((client.nextRetryAt - Date.now()) / 1000))}s` : '…'}`
                      : 'Chat services are temporarily unavailable. Your queued messages will retry.'}
            </span>
            {client.status === 'reconnecting' && (
              <button
                className="text-button"
                onClick={() => client.retryConnection()}
              >
                Retry now
              </button>
            )}
            {client.status === 'expired' && (
              <button className="text-button" onClick={logout}>
                Sign in again
              </button>
            )}
          </div>
        )}
        {active ? (
          <>
            <header className="conversation-header">
              <button
                className="icon-button mobile-back"
                aria-label="Back to conversations"
                onClick={() => setSelected(null)}
              >
                <Icon name="back" />
              </button>
              <Avatar
                name={chatName(active, account.user.id)}
                group={isGroup(active)}
              />
              <div>
                <h2>{chatName(active, account.user.id)}</h2>
                <p
                  className={typingLabel ? 'typing-preview' : undefined}
                  role="status"
                  aria-live="polite"
                >
                  <span
                    className={`status-dot ${!typingLabel && !client.presence.get(peer(active, account.user.id).id) ? 'muted' : ''}`}
                  />
                  {typingLabel ||
                    (isGroup(active)
                      ? `${active.members.length} members`
                      : client.presence.get(peer(active, account.user.id).id)
                        ? 'Online'
                        : 'Offline')}
                </p>
              </div>
              <span className="conversation-label">
                {isGroup(active) ? 'GROUP CONVERSATION' : 'JUST THE TWO OF YOU'}
              </span>
            </header>
            {(error || (client.error && !client.degraded)) && (
              <div className="notice" role="alert">
                {error || client.error}
                {client.status === 'expired' ? (
                  <button className="text-button" onClick={logout}>
                    Sign in again
                  </button>
                ) : (
                  <button
                    className="text-button"
                    onClick={() => {
                      void refresh();
                      client.sync(active.id);
                    }}
                  >
                    Try again
                  </button>
                )}
              </div>
            )}
            <div
              className="message-scroll"
              ref={scroll}
              onScroll={() => {
                const box = scroll.current;
                if (box)
                  pinned.current =
                    box.scrollHeight - box.scrollTop - box.clientHeight < 80;
              }}
            >
              <div className="conversation-start">
                <Avatar
                  name={chatName(active, account.user.id)}
                  group={isGroup(active)}
                />
                <h3>
                  {isGroup(active)
                    ? `Welcome to ${chatName(active, account.user.id)}.`
                    : `This is your conversation with ${peer(active, account.user.id).username}.`}
                </h3>
                <p>
                  {isGroup(active)
                    ? active.members
                        .map((member) =>
                          member.id === account.user.id
                            ? 'You'
                            : member.username,
                        )
                        .join(', ')
                    : 'A simple hello is a good place to start.'}
                </p>
              </div>
              <div
                className="message-stack"
                role="log"
                aria-live="polite"
                aria-relevant="additions"
              >
                {messages.map((message, index) => {
                  const mine = message.senderId === account.user.id;
                  const state = mine ? status(message) : '';
                  const day = new Date(message.createdAt).toLocaleDateString();
                  const previous = messages[index - 1];
                  return (
                    <div key={message.messageId}>
                      {(!previous ||
                        new Date(previous.createdAt).toLocaleDateString() !==
                          day) && (
                        <div className="day-divider">
                          <span>
                            {new Date(message.createdAt).toLocaleDateString(
                              undefined,
                              {
                                month: 'long',
                                day: 'numeric',
                                year: 'numeric',
                              },
                            )}
                          </span>
                        </div>
                      )}
                      <article
                        className={`message ${mine ? 'own' : 'other'}`}
                        data-message-id={message.messageId}
                      >
                        {isGroup(active) && !mine && (
                          <span className="message-sender">
                            {active.members.find(
                              (member) => member.id === message.senderId,
                            )?.username ?? 'Member'}
                          </span>
                        )}
                        {message.messageId === firstUnread && (
                          <span className="unread-divider">
                            Unread messages
                          </span>
                        )}
                        <p>{message.body}</p>
                        <footer>
                          <time dateTime={message.createdAt}>
                            {time(message.createdAt)}
                          </time>
                          {mine && (
                            <span
                              className={`message-status ${state === 'Read' ? 'read' : ''}`}
                              aria-label={state}
                            >
                              <span aria-hidden="true">
                                {state === 'Sent' ? '✓' : '✓✓'}
                              </span>
                              <span className="receipt-label">{state}</span>
                            </span>
                          )}
                        </footer>
                      </article>
                    </div>
                  );
                })}
                {pending.map((message) => (
                  <article
                    className="message own pending"
                    key={message.clientMsgId}
                  >
                    <p>{message.body}</p>
                    <footer>
                      <time>{time(message.createdAt)}</time>
                      <span>
                        {message.failure
                          ? 'Failed'
                          : message.seq
                            ? 'Sent'
                            : message.received || client.status !== 'connected'
                              ? 'Queued'
                              : 'Sending…'}
                      </span>
                    </footer>
                    {message.failure && (
                      <div className="send-failure" role="alert">
                        {message.failure}
                        <button
                          onClick={() =>
                            client.retryMessage(message.clientMsgId)
                          }
                          aria-label="Retry message"
                        >
                          Retry
                        </button>
                        <button
                          onClick={() => client.dismiss(message.clientMsgId)}
                        >
                          Dismiss
                        </button>
                      </div>
                    )}
                  </article>
                ))}
                <div ref={bottom} />
              </div>
            </div>
            <div className="composer-area">
              {sendError && (
                <p className="form-error" role="alert">
                  {sendError}
                </p>
              )}
              <form className="composer" onSubmit={send}>
                <textarea
                  ref={textarea}
                  aria-label="Message"
                  placeholder={`Message ${chatName(active, account.user.id)}…`}
                  value={draft}
                  onChange={(event) => {
                    setDraft(event.target.value);
                    client.setTyping(
                      active.id,
                      Boolean(event.target.value.trim()),
                    );
                  }}
                  onBlur={() => client.setTyping(active.id, false)}
                  onKeyDown={(event) => {
                    if (
                      event.key === 'Enter' &&
                      !event.shiftKey &&
                      !event.nativeEvent.isComposing
                    ) {
                      event.preventDefault();
                      send();
                    }
                  }}
                  rows={1}
                  disabled={client.status === 'expired'}
                />
                <button
                  className="send-button"
                  type="submit"
                  aria-label="Send message"
                  disabled={!draft.trim() || client.status === 'expired'}
                >
                  <Icon name="send" />
                </button>
              </form>
              <div className="composer-hint">
                <span>
                  {client.status === 'connected'
                    ? 'A little thought, a little send.'
                    : 'Your messages will send when you reconnect.'}
                </span>
                <span>
                  Enter to send <i>·</i> Shift + Enter for a new line
                </span>
              </div>
            </div>
          </>
        ) : (
          <div className="conversation-empty">
            <div className="empty-chat-mark">
              <Icon name="chat" />
            </div>
            <div className="eyebrow">YOUR CONVERSATIONS, ALL HERE</div>
            <h2>A hello goes a long way.</h2>
            <p>
              Choose a conversation or start a new one.
              <br />
              There’s always someone worth catching up with.
            </p>
            <button
              className="primary-button"
              onClick={() => setCreating(true)}
            >
              <Icon name="plus" />
              New conversation
            </button>
            {error && (
              <p className="form-error" role="alert">
                {error}
                <button className="text-button" onClick={() => void refresh()}>
                  Try again
                </button>
              </p>
            )}
          </div>
        )}
      </section>
      {creating && (
        <NewChat
          token={account.token}
          close={closeNew}
          onCreated={async (id) => {
            await refresh();
            selectChat(id);
          }}
        />
      )}
    </main>
  );
}
