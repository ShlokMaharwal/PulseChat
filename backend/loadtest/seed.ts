import { randomUUID } from 'node:crypto';
import bcrypt from 'bcrypt';
import jwt from 'jsonwebtoken';
import { MongoClient } from 'mongodb';
import type { LoadFixture, LoadAccount } from './fixtures.js';

/** Explicit synthetic setup, outside the load window; never a production HTTP route. */
export async function seedFixture(target: number): Promise<LoadFixture> {
  const secret = process.env.JWT_SECRET;
  if (!secret || secret.length < 32)
    throw new Error(
      'Seed setup requires the stack JWT_SECRET in the runner environment',
    );
  const mongo = new MongoClient(
    process.env.MONGO_URL ?? 'mongodb://localhost:27018',
    {
      serverSelectionTimeoutMS: 10000,
    },
  );
  try {
    await mongo.connect();
    const db = mongo.db(process.env.MONGO_DB ?? 'pulsechat');
    const suffix = randomUUID().replaceAll('-', '').slice(0, 10);
    const createdAt = new Date().toISOString();
    // All synthetic accounts use this known test password. Sharing the test hash
    // avoids measuring hundreds of bcrypt jobs while preparing a message benchmark.
    const passwordHash = await bcrypt.hash('Load-test-password!', 12);
    const users = Array.from({ length: target }, (_, index) => {
      const username = 'perf_' + index + '_' + suffix;
      return {
        id: randomUUID(),
        username,
        usernameLower: username,
        passwordHash,
        createdAt,
        loadFixtureId: suffix,
      };
    });
    const accounts: LoadAccount[] = [];
    const chats = [];
    for (let index = 0; index < target; index += 2) {
      const a = users[index]!,
        b = users[index + 1]!;
      const memberIds = [a.id, b.id].sort();
      const chatId = randomUUID();
      chats.push({
        id: chatId,
        memberIds,
        pairKey: memberIds.join(':'),
        createdAt,
        loadFixtureId: suffix,
      });
      for (const [user, peer] of [
        [a, b],
        [b, a],
      ] as const)
        accounts.push({
          userId: user.id,
          peerId: peer.id,
          chatId,
          token: jwt.sign({}, secret, {
            algorithm: 'HS256',
            subject: user.id,
            issuer: 'pulsechat',
            audience: 'pulsechat',
            expiresIn: 900,
          }),
        });
    }
    await db.collection('users').insertMany(users);
    await db.collection('chats').insertMany(chats);
    return { accounts, createdAt };
  } finally {
    await mongo.close();
  }
}
