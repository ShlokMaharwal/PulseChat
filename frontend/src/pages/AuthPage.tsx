import { useState, type FormEvent } from 'react';
import { authenticate } from '../sdk/api';
import type { Account } from '../types/protocol';
import { Brand, Icon } from '../components/Icons';
export function AuthPage({
  onLogin,
  theme,
  toggleTheme,
}: {
  onLogin: (account: Account) => void;
  theme: string;
  toggleTheme: () => void;
}) {
  const [mode, setMode] = useState<'login' | 'register'>('login');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (busy) return;
    setBusy(true);
    setError('');
    try {
      onLogin(await authenticate(mode, username.trim(), password));
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not sign in.');
    } finally {
      setBusy(false);
    }
  }
  return (
    <main className="auth-page">
      <header className="auth-header">
        <Brand />
        <button
          className="icon-button"
          onClick={toggleTheme}
          aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}
        >
          <Icon name={theme === 'dark' ? 'sun' : 'moon'} />
        </button>
      </header>
      <div className="auth-layout">
        <section className="auth-intro">
          <div className="eyebrow">
            <span className="status-dot" /> A space for your conversations
          </div>
          <h1>
            A simple hello.
            <br />
            <span>A closer connection.</span>
          </h1>
          <p>
            Pick up a conversation. Start a new one.
            <br />
            Make a little room for the people in your day.
          </p>
          <div className="auth-detail">
            <span className="line-art" aria-hidden="true">
              <i />
              <i />
              <i />
            </span>
            <p>
              Good conversations
              <br />
              <strong>start with you.</strong>
            </p>
          </div>
        </section>
        <section className="auth-card" aria-labelledby="auth-title">
          <div className="card-label">YOUR NEXT CONVERSATION STARTS HERE</div>
          <h2 id="auth-title">
            {mode === 'login' ? 'Welcome back' : 'Make yourself at home'}
          </h2>
          <p>
            {mode === 'login'
              ? 'Sign in to your PulseChat account.'
              : 'Create an account and find your people.'}
          </p>
          <form onSubmit={(event) => void submit(event)}>
            <label htmlFor="username">Username</label>
            <input
              id="username"
              autoComplete="username"
              required
              minLength={mode === 'register' ? 3 : 1}
              maxLength={32}
              pattern={mode === 'register' ? '[a-zA-Z0-9_]+' : undefined}
              value={username}
              onChange={(event) => setUsername(event.target.value)}
              placeholder="Your username"
            />
            <label htmlFor="password">Password</label>
            <div className="password-field">
              <input
                id="password"
                type={showPassword ? 'text' : 'password'}
                autoComplete={
                  mode === 'login' ? 'current-password' : 'new-password'
                }
                required
                minLength={mode === 'register' ? 10 : 1}
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                placeholder={
                  mode === 'register'
                    ? 'At least 10 characters'
                    : 'Your password'
                }
              />
              <button
                type="button"
                className="password-toggle"
                aria-label={showPassword ? 'Hide password' : 'Show password'}
                aria-pressed={showPassword}
                aria-controls="password"
                title={showPassword ? 'Hide password' : 'Show password'}
                onClick={() => setShowPassword((visible) => !visible)}
              >
                <Icon name={showPassword ? 'eye-off' : 'eye'} />
              </button>
            </div>
            {mode === 'register' && (
              <p className="field-hint">
                Use 3–32 letters, numbers, or underscores for your username.
              </p>
            )}
            {error && (
              <p className="form-error" role="alert">
                {error}
              </p>
            )}
            <button className="primary-button auth-submit" disabled={busy}>
              {busy
                ? 'One moment…'
                : mode === 'login'
                  ? 'Sign in'
                  : 'Create account'}
              <Icon name="send" />
            </button>
          </form>
          <div className="auth-switch">
            {mode === 'login' ? 'New around here?' : 'Already have an account?'}{' '}
            <button
              className="text-button"
              onClick={() => {
                setMode(mode === 'login' ? 'register' : 'login');
                setShowPassword(false);
                setError('');
              }}
            >
              {mode === 'login' ? 'Create an account' : 'Sign in'}
            </button>
          </div>
        </section>
      </div>
      <footer className="auth-footer">
        <span>Less noise. More conversation.</span>
        <span>PulseChat</span>
      </footer>
    </main>
  );
}
