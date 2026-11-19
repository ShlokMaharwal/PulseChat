import { useCallback, useEffect, useState } from 'react';
import type { Account } from './types/protocol';
import { AuthPage } from './pages/AuthPage';
import { ChatPage } from './pages/ChatPage';
import { revokeSession, saveAccount } from './sdk/session';
export default function App() {
  const [account, setAccount] = useState<Account | null>(() => {
    try {
      const saved = JSON.parse(
        sessionStorage.getItem('pulsechat.account') ?? 'null',
      ) as Account | null;
      return saved?.token && saved.user?.id ? saved : null;
    } catch {
      return null;
    }
  });
  const [theme, setTheme] = useState(() =>
    document.documentElement.dataset.theme === 'light' ? 'light' : 'dark',
  );
  const [signoutError, setSignoutError] = useState('');
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.documentElement.style.colorScheme = theme;
  }, [theme]);
  useEffect(() => {
    const media = matchMedia('(prefers-color-scheme: dark)');
    const update = () => {
      if (!localStorage.getItem('pulsechat.theme'))
        setTheme(media.matches ? 'dark' : 'light');
    };
    media.addEventListener('change', update);
    return () => media.removeEventListener('change', update);
  }, []);
  const toggleTheme = () => {
    const next = theme === 'dark' ? 'light' : 'dark';
    localStorage.setItem('pulsechat.theme', next);
    setTheme(next);
  };
  const logout = useCallback(() => {
    void revokeSession()
      .then(() => {
        setAccount(null);
        setSignoutError('');
      })
      .catch((error) =>
        setSignoutError(
          error instanceof Error ? error.message : 'Please retry sign out.',
        ),
      );
  }, []);
  const login = (value: Account) => {
    saveAccount(value);
    setAccount(value);
  };
  return (
    <>
      {signoutError && (
        <div className="notice signout-notice" role="alert">
          {signoutError}
          <button className="text-button" onClick={logout}>
            Retry sign out
          </button>
        </div>
      )}
      {account ? (
        <ChatPage
          key={account.user.id}
          account={account}
          logout={logout}
          theme={theme}
          toggleTheme={toggleTheme}
        />
      ) : (
        <AuthPage onLogin={login} theme={theme} toggleTheme={toggleTheme} />
      )}
    </>
  );
}
