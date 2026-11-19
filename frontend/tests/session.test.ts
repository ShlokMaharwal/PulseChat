import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import {
  revokeSession,
  saveAccount,
  savedAccount,
  sessionToken,
} from '../src/sdk/session';
const user = { id: 'session-user', username: 'session_user' };
const token = (exp: number) =>
  'x.' + btoa(JSON.stringify({ sub: user.id, exp })) + '.x';
beforeEach(() => {
  const values = new Map<string, string>();
  vi.stubGlobal('sessionStorage', {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
    removeItem: (key: string) => values.delete(key),
  });
});
afterEach(() => {
  vi.unstubAllGlobals();
});
it('silently rotates once when concurrent requests need a renewed access token', async () => {
  saveAccount({ user, token: token(1), refreshToken: 'old', expiresIn: 900 });
  const next = {
    user,
    token: token(Date.now() / 1000 + 900),
    refreshToken: 'new',
    expiresIn: 900,
  };
  const fetchMock = vi
    .fn()
    .mockResolvedValue({ ok: true, json: async () => next });
  vi.stubGlobal('fetch', fetchMock);
  const result = await Promise.all([
    sessionToken(user.id),
    sessionToken(user.id),
  ]);
  expect(result).toEqual([next.token, next.token]);
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(savedAccount()?.refreshToken).toBe('new');
});
it('revokes the refresh family before clearing the local session and reports failed revocation', async () => {
  saveAccount({
    user,
    token: token(1),
    refreshToken: 'current',
    expiresIn: 900,
  });
  const fetchMock = vi
    .fn()
    .mockResolvedValueOnce({ ok: false })
    .mockResolvedValueOnce({ ok: true });
  vi.stubGlobal('fetch', fetchMock);
  await expect(revokeSession()).rejects.toThrow('could not be confirmed');
  expect(savedAccount()).not.toBeNull();
  await revokeSession();
  expect(savedAccount()).toBeNull();
  expect(fetchMock.mock.calls[1]?.[1].body).toBe(
    JSON.stringify({ refreshToken: 'current' }),
  );
});
