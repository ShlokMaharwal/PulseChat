import { randomUUID } from 'node:crypto';
import bcrypt from 'bcrypt';
import jwt from 'jsonwebtoken';
import {
  loginSchema,
  registerSchema,
  uuidSchema,
  type Config,
  type createDependencies,
} from '@pulsechat/shared';
import { HttpError } from './errors.js';

type Database = ReturnType<typeof createDependencies>['db'];

export interface UserRecord {
  id: string;
  username: string;
  usernameLower: string;
  passwordHash: string;
  createdAt: string;
}

export interface PublicUser {
  id: string;
  username: string;
}

export function verifySession(
  token: string,
  config: Config,
): { userId: string; expiresAt: number } {
  try {
    const payload = jwt.verify(token, config.JWT_SECRET, {
      algorithms: ['HS256'],
      issuer: 'pulsechat',
      audience: 'pulsechat',
    });
    if (
      typeof payload === 'string' ||
      !uuidSchema.safeParse(payload.sub).success ||
      typeof payload.exp !== 'number'
    ) {
      throw new Error('Invalid token subject');
    }
    return { userId: payload.sub as string, expiresAt: payload.exp * 1000 };
  } catch {
    throw new HttpError(
      401,
      'UNAUTHORIZED',
      'A valid, unexpired token is required',
    );
  }
}

export function verifyToken(token: string, config: Config): string {
  return verifySession(token, config).userId;
}

export function bearerToken(authorization: string | undefined): string {
  const match = authorization?.match(/^Bearer ([^\s]+)$/i);
  if (!match?.[1])
    throw new HttpError(
      401,
      'UNAUTHORIZED',
      'Bearer authentication is required',
    );
  return match[1];
}

export function websocketToken(
  protocolHeader: string | string[] | undefined,
): string {
  const protocols = (
    Array.isArray(protocolHeader)
      ? protocolHeader.join(',')
      : (protocolHeader ?? '')
  )
    .split(',')
    .map((value) => value.trim());
  const tokens = protocols.filter((value) => value.startsWith('jwt.'));
  if (
    !protocols.includes('pulsechat') ||
    tokens.length !== 1 ||
    !tokens[0]?.slice(4)
  ) {
    throw new HttpError(
      401,
      'UNAUTHORIZED',
      'Authenticated pulsechat subprotocols are required',
    );
  }
  return tokens[0].slice(4);
}

export class AuthService {
  private readonly users;
  private dummyHash: string | undefined;

  constructor(
    db: Database,
    private readonly config: Config,
  ) {
    this.users = db.collection<UserRecord>('users');
  }

  async initialize() {
    await this.users.createIndexes([
      { key: { id: 1 }, unique: true, name: 'userId_unique' },
      { key: { usernameLower: 1 }, unique: true, name: 'username_unique' },
    ]);
    // Unknown usernames perform the same password comparison work as known users.
    this.dummyHash = await bcrypt.hash(randomUUID(), 12);
  }

  private result(user: PublicUser) {
    return {
      token: jwt.sign({}, this.config.JWT_SECRET, {
        algorithm: 'HS256',
        subject: user.id,
        issuer: 'pulsechat',
        audience: 'pulsechat',
        expiresIn: this.config.JWT_EXPIRES_IN_SECONDS,
      }),
      expiresIn: this.config.JWT_EXPIRES_IN_SECONDS,
      user: { id: user.id, username: user.username },
    };
  }

  async register(input: unknown) {
    const parsed = registerSchema.safeParse(input);
    if (!parsed.success)
      throw new HttpError(
        400,
        'INVALID_REQUEST',
        'Use a valid username and a password of 10 or more characters, at most 72 UTF-8 bytes',
      );
    const user: UserRecord = {
      id: randomUUID(),
      username: parsed.data.username,
      usernameLower: parsed.data.username.toLowerCase(),
      passwordHash: await bcrypt.hash(parsed.data.password, 12),
      createdAt: new Date().toISOString(),
    };
    try {
      await this.users.insertOne({ ...user });
    } catch (error) {
      if (
        error &&
        typeof error === 'object' &&
        'code' in error &&
        error.code === 11000
      ) {
        throw new HttpError(
          409,
          'CONFLICT',
          'That username is already registered',
        );
      }
      throw error;
    }
    return this.result(user);
  }

  async login(input: unknown) {
    const parsed = loginSchema.safeParse(input);
    if (
      !parsed.success ||
      Buffer.byteLength(parsed.data.password, 'utf8') > 72
    ) {
      throw new HttpError(400, 'INVALID_REQUEST', 'Invalid login details');
    }
    const user = await this.users.findOne({
      usernameLower: parsed.data.username.trim().toLowerCase(),
    });
    if (!this.dummyHash)
      throw new HttpError(503, 'UNAVAILABLE', 'Authentication is starting');
    const valid = await bcrypt.compare(
      parsed.data.password,
      user?.passwordHash ?? this.dummyHash,
    );
    if (!user || !valid)
      throw new HttpError(401, 'UNAUTHORIZED', 'Invalid username or password');
    return this.result(user);
  }

  async user(id: string): Promise<PublicUser> {
    const user = await this.users.findOne(
      { id },
      { projection: { _id: 0, id: 1, username: 1 } },
    );
    if (!user)
      throw new HttpError(401, 'UNAUTHORIZED', 'User no longer exists');
    return { id: user.id, username: user.username };
  }

  async search(query: string, selfId: string): Promise<PublicUser[]> {
    if (!/^[a-zA-Z0-9_]{2,32}$/.test(query))
      throw new HttpError(
        400,
        'INVALID_REQUEST',
        'Search requires 2–32 username characters',
      );
    return this.users
      .find(
        {
          id: { $ne: selfId },
          usernameLower: { $regex: `^${query.toLowerCase()}` },
        },
        { projection: { _id: 0, id: 1, username: 1 } },
      )
      .sort({ usernameLower: 1 })
      .limit(20)
      .map((user) => ({ id: user.id, username: user.username }))
      .toArray();
  }
}
