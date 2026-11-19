import { createHash, randomBytes, randomUUID } from 'node:crypto';
import type { Config, createDependencies } from '@pulsechat/shared';
import { HttpError } from './errors.js';

type Database = ReturnType<typeof createDependencies>['db'];
interface RefreshSession {
  id: string;
  userId: string;
  hash: string;
  usedHashes: string[];
  rotations: number;
  revoked: boolean;
  expiresAt: Date;
}
const hash = (value: string) =>
  createHash('sha256').update(value).digest('hex');
const invalid = () =>
  new HttpError(401, 'UNAUTHORIZED', 'Sign in again to renew your session');
function parse(input: unknown) {
  if (typeof input !== 'string' || !/^[a-f\d-]{36}\.[\w-]{43}$/.test(input))
    throw invalid();
  return { id: input.slice(0, 36), hash: hash(input) };
}
export class RefreshSessions {
  private readonly records;
  constructor(
    db: Database,
    private readonly config: Config,
  ) {
    this.records = db.collection<RefreshSession>('refresh_sessions');
  }
  async initialize() {
    await this.records.createIndexes([
      { key: { id: 1 }, unique: true, name: 'refreshSession_unique' },
      {
        key: { expiresAt: 1 },
        expireAfterSeconds: 0,
        name: 'refreshSession_expiry',
      },
    ]);
  }
  async create(userId: string) {
    const id = randomUUID(),
      token = id + '.' + randomBytes(32).toString('base64url');
    await this.records.insertOne({
      id,
      userId,
      hash: hash(token),
      usedHashes: [],
      rotations: 0,
      revoked: false,
      expiresAt: new Date(
        Date.now() + this.config.REFRESH_EXPIRES_IN_SECONDS * 1000,
      ),
    });
    return token;
  }
  async rotate(input: unknown) {
    const old = parse(input),
      token = old.id + '.' + randomBytes(32).toString('base64url');
    const record = await this.records.findOneAndUpdate(
      {
        id: old.id,
        hash: old.hash,
        revoked: false,
        expiresAt: { $gt: new Date() },
        rotations: { $lt: 4096 },
      },
      {
        $set: { hash: hash(token) },
        $inc: { rotations: 1 },
        $push: { usedHashes: old.hash },
      },
      { returnDocument: 'after', includeResultMetadata: false },
    );
    if (!record) {
      // A known spent secret revokes its entire family; arbitrary guesses cannot.
      await this.records.updateOne(
        { id: old.id, usedHashes: old.hash },
        { $set: { revoked: true } },
      );
      throw invalid();
    }
    return { userId: record.userId, refreshToken: token };
  }
  async revoke(input: unknown) {
    const token = parse(input);
    await this.records.updateOne(
      { id: token.id, $or: [{ hash: token.hash }, { usedHashes: token.hash }] },
      { $set: { revoked: true } },
    );
  }
}
