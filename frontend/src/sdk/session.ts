import type { Account } from '../types/protocol';
let generation = 0;
let renewing: Promise<Account> | undefined;
export class SessionError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
  }
}
export function savedAccount(): Account | null {
  try {
    return JSON.parse(
      sessionStorage.getItem('pulsechat.account') ?? 'null',
    ) as Account | null;
  } catch {
    return null;
  }
}
export function saveAccount(account: Account | null) {
  generation++;
  if (account)
    sessionStorage.setItem('pulsechat.account', JSON.stringify(account));
  else sessionStorage.removeItem('pulsechat.account');
}
export function subject(token: string) {
  try {
    return JSON.parse(
      atob(token.split('.')[1]!.replaceAll('-', '+').replaceAll('_', '/')),
    ) as { sub: string; exp: number };
  } catch {
    return null;
  }
}
export async function sessionToken(
  userId: string,
  force = false,
): Promise<string> {
  const account = savedAccount();
  if (!account || account.user.id !== userId)
    throw new SessionError(401, 'Session expired. Sign in again.');
  if (!force && (subject(account.token)?.exp ?? 0) * 1000 > Date.now() + 30000)
    return account.token;
  if (!account.refreshToken)
    throw new SessionError(401, 'Session expired. Sign in again.');
  if (!renewing) {
    const current = generation;
    renewing = (async () => {
      const response = await fetch('/api/auth/refresh', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ refreshToken: account.refreshToken }),
        signal: AbortSignal.timeout(20000),
      });
      if (!response.ok)
        throw new SessionError(
          response.status,
          response.status === 401
            ? 'Session expired. Sign in again.'
            : 'Session renewal is temporarily unavailable.',
        );
      const next = (await response.json()) as Account;
      if (!next.token || !next.refreshToken || next.user.id !== userId)
        throw new Error('Invalid renewed session');
      if (
        current !== generation ||
        savedAccount()?.refreshToken !== account.refreshToken
      )
        throw new Error('Session changed');
      sessionStorage.setItem('pulsechat.account', JSON.stringify(next));
      return next;
    })().finally(() => {
      renewing = undefined;
    });
  }
  const renewed = await renewing;
  if (renewed.user.id !== userId) throw new Error('Session changed');
  return renewed.token;
}
export async function revokeSession() {
  const account = savedAccount();
  if (account?.refreshToken) {
    const response = await fetch('/api/auth/logout', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ refreshToken: account.refreshToken }),
      signal: AbortSignal.timeout(20000),
    });
    if (!response.ok)
      throw new Error('Sign out could not be confirmed. Please try again.');
  }
  saveAccount(null);
}
