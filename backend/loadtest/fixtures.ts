import { randomUUID } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { TestClient } from '../tests/helpers/test-client.js';
import { seedFixture } from './seed.js';
import { MongoClient } from 'mongodb';

export interface LoadAccount {
  token: string;
  userId: string;
  chatId: string;
  peerId: string;
  initialSeq?: number;
}
export interface LoadFixture {
  accounts: LoadAccount[];
  createdAt: string;
}
export async function loadFixture(
  base: string,
  target: number,
  file: string,
  reuse?: string,
) {
  if (target < 2 || target % 2 !== 0)
    throw new Error('Use an even socket target for distinct 1:1 pairs');
  async function request<T>(
    path: string,
    body: unknown,
    token?: string,
  ): Promise<T> {
    const response = await fetch(base + '/api' + path, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        ...(token ? { authorization: 'Bearer ' + token } : {}),
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(60000),
    });
    if (!response.ok)
      throw new Error('Fixture HTTP ' + response.status + ': ' + path);
    return (await response.json()) as T;
  }
  let fixture: LoadFixture;
  if (reuse) fixture = JSON.parse(await readFile(reuse, 'utf8')) as LoadFixture;
  else if (process.env.LOAD_FIXTURE_MODE === 'seed')
    fixture = await seedFixture(target);
  else {
    const accounts = new Array<LoadAccount>(target);
    const suffix = randomUUID().replaceAll('-', '').slice(0, 10);
    let pair = 0;
    const worker = async () => {
      while (pair < target / 2) {
        const index = pair++;
        const a = await request<{ token: string; user: { id: string } }>(
          '/auth/register',
          {
            username: 'perf_a_' + index + '_' + suffix,
            password: 'Load-test-password!',
          },
        );
        const b = await request<{ token: string; user: { id: string } }>(
          '/auth/register',
          {
            username: 'perf_b_' + index + '_' + suffix,
            password: 'Load-test-password!',
          },
        );
        const { chat } = await request<{ chat: { id: string } }>(
          '/chats',
          { userId: b.user.id },
          a.token,
        );
        accounts[index * 2] = {
          token: a.token,
          userId: a.user.id,
          chatId: chat.id,
          peerId: b.user.id,
        };
        accounts[index * 2 + 1] = {
          token: b.token,
          userId: b.user.id,
          chatId: chat.id,
          peerId: a.user.id,
        };
      }
    };
    await Promise.all(Array.from({ length: Math.min(4, target / 2) }, worker));
    fixture = { accounts, createdAt: new Date().toISOString() };
  }
  if (
    fixture.accounts.length !== target ||
    new Set(fixture.accounts.map((a) => a.userId)).size !== target
  )
    throw new Error('Fixture must have exactly one distinct user per socket');
  const byUser = new Map(
    fixture.accounts.map((account) => [account.userId, account]),
  );
  for (const a of fixture.accounts) {
    const peer = byUser.get(a.peerId);
    if (!peer || peer.chatId !== a.chatId || peer.peerId !== a.userId)
      throw new Error('Invalid chat pair');
  }
  // Preserve expensive setup for a retry even if the public preflight fails.
  await writeFile(file, JSON.stringify(fixture));
  const preflight = [new TestClient(), new TestClient()];
  try {
    const url = base.replace(/^http/, 'ws') + '/ws';
    await preflight[0]!.connect(url, fixture.accounts[0]!.token);
    await preflight[1]!.connect(url, fixture.accounts[1]!.token);
    const id = randomUUID();
    await preflight[0]!.message(
      fixture.accounts[0]!.chatId,
      'Performance fixture preflight',
      id,
    );
    const message = await preflight[1]!.waitFor(
      (frame) => frame.type === 'message' && frame.clientMsgId === id,
    );
    if (message.type !== 'message' || !message.trace)
      throw new Error(
        'TRACE_MESSAGES must be enabled on gateways and processors',
      );
  } finally {
    await Promise.all(preflight.map((client) => client.close()));
  }
  const mongo = new MongoClient(
    process.env.MONGO_URL ?? 'mongodb://localhost:27018',
    { serverSelectionTimeoutMS: 10000 },
  );
  try {
    await mongo.connect();
    const tails = await mongo
      .db(process.env.MONGO_DB ?? 'pulsechat')
      .collection('messages')
      .aggregate<{ _id: string; seq: number }>([
        {
          $match: {
            chatId: {
              $in: [...new Set(fixture.accounts.map((a) => a.chatId))],
            },
          },
        },
        { $group: { _id: '$chatId', seq: { $max: '$seq' } } },
      ])
      .toArray();
    const byChat = new Map(tails.map((tail) => [tail._id, tail.seq]));
    for (const account of fixture.accounts)
      account.initialSeq = byChat.get(account.chatId) ?? 0;
  } finally {
    await mongo.close();
  }
  await writeFile(file, JSON.stringify(fixture));
  return fixture;
}
