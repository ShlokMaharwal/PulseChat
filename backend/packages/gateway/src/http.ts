import type { IncomingMessage, ServerResponse } from 'node:http';
import type { Logger } from 'pino';
import { z } from 'zod';
import { uuidSchema, type Config, type UserRouter } from '@pulsechat/shared';
import { AuthService, bearerToken, verifyToken } from './auth.js';
import { ChatService } from './chats.js';
import { HttpError } from './errors.js';
import { AuthThrottle, clientIp } from './security.js';

export function sendJson(
  response: ServerResponse,
  status: number,
  body: unknown,
) {
  if (response.destroyed || response.writableEnded) return;
  response.writeHead(status, {
    'content-type': 'application/json',
    'cache-control': 'no-store',
  });
  response.end(JSON.stringify(body));
}

export function readJson(
  request: IncomingMessage,
  limit: number,
): Promise<unknown> {
  if (
    request.headers['content-type']?.split(';')[0]?.trim().toLowerCase() !==
    'application/json'
  ) {
    request.resume();
    return Promise.reject(
      new HttpError(400, 'INVALID_REQUEST', 'Send an application/json body'),
    );
  }
  if (Number(request.headers['content-length'] ?? 0) > limit) {
    request.resume();
    return Promise.reject(
      new HttpError(413, 'PAYLOAD_TOO_LARGE', 'Request body is too large'),
    );
  }
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let bytes = 0;
    let settled = false;
    const fail = (error: unknown) => {
      if (!settled) {
        settled = true;
        chunks.length = 0;
        reject(error);
      }
    };
    request.on('data', (chunk: Buffer) => {
      if (settled) return;
      bytes += chunk.length;
      if (bytes > limit) {
        fail(
          new HttpError(413, 'PAYLOAD_TOO_LARGE', 'Request body is too large'),
        );
        return;
      }
      chunks.push(chunk);
    });
    request.once('aborted', () =>
      fail(new HttpError(400, 'INVALID_REQUEST', 'Request was interrupted')),
    );
    request.once('error', fail);
    request.once('end', () => {
      if (settled) return;
      try {
        const result: unknown = JSON.parse(
          new TextDecoder('utf-8', { fatal: true }).decode(
            Buffer.concat(chunks),
          ),
        );
        settled = true;
        resolve(result);
      } catch {
        fail(new HttpError(400, 'INVALID_REQUEST', 'Invalid JSON'));
      }
    });
  });
}

const createChatSchema = z.object({ userId: uuidSchema }).strict();
const pageSchema = z.object({
  afterSeq: z.coerce.number().int().nonnegative().safe().default(0),
  limit: z.coerce.number().int().min(1).max(100).default(100),
});

export class GatewayHttpApi {
  constructor(
    private readonly auth: AuthService,
    private readonly chats: ChatService,
    private readonly config: Config,
    private readonly logger: Logger,
    private readonly router: UserRouter,
    private readonly throttle: AuthThrottle,
  ) {}

  async handle(
    request: IncomingMessage,
    response: ServerResponse,
  ): Promise<boolean> {
    const url = new URL(request.url ?? '/', 'http://gateway');
    if (
      ![
        '/auth/register',
        '/auth/login',
        '/auth/refresh',
        '/auth/logout',
        '/users',
        '/chats',
      ].includes(url.pathname) &&
      !url.pathname.startsWith('/chats/') &&
      !url.pathname.startsWith('/users/')
    )
      return false;
    try {
      if (request.method === 'POST' && url.pathname.startsWith('/auth/')) {
        await this.throttle.ip(await clientIp(request, this.config));
        const input = await readJson(request, this.config.MAX_MESSAGE_BYTES);
        if (
          url.pathname === '/auth/refresh' ||
          url.pathname === '/auth/logout'
        ) {
          const parsed = z
            .object({ refreshToken: z.string().max(200) })
            .strict()
            .safeParse(input);
          if (!parsed.success)
            throw new HttpError(
              400,
              'INVALID_REQUEST',
              'A refresh token is required',
            );
          if (url.pathname === '/auth/refresh')
            sendJson(
              response,
              200,
              await this.auth.refresh(parsed.data.refreshToken),
            );
          else {
            await this.auth.logout(parsed.data.refreshToken);
            sendJson(response, 200, { revoked: true });
          }
          return true;
        }
        const username =
          input && typeof input === 'object' && 'username' in input
            ? input.username
            : undefined;
        const key = await this.throttle.username(username);
        try {
          const registration = url.pathname === '/auth/register';
          const result = registration
            ? await this.auth.register(input)
            : await this.auth.login(input);
          await this.throttle.success(key);
          sendJson(response, registration ? 201 : 200, result);
        } catch (error) {
          if (error instanceof HttpError && [401, 409].includes(error.status))
            await this.throttle.failure(key);
          throw error;
        }
        return true;
      }
      {
        const userId = verifyToken(
          bearerToken(request.headers.authorization),
          this.config,
        );
        await this.auth.user(userId);
        const presence = url.pathname.match(/^\/users\/([^/]+)\/presence$/);
        if (request.method === 'GET' && presence?.[1]) {
          const peerId = uuidSchema.safeParse(presence[1]);
          if (!peerId.success)
            throw new HttpError(
              400,
              'INVALID_REQUEST',
              'Invalid user identifier',
            );
          if (
            peerId.data !== userId &&
            !(await this.chats.shareChat(userId, peerId.data))
          )
            throw new HttpError(
              403,
              'FORBIDDEN',
              'Presence is restricted to chat peers',
            );
          sendJson(response, 200, {
            userId: peerId.data,
            online:
              (await this.router.online([peerId.data])).get(peerId.data) ??
              false,
          });
        } else if (request.method === 'GET' && url.pathname === '/users') {
          sendJson(response, 200, {
            users: await this.auth.search(
              url.searchParams.get('query') ?? '',
              userId,
            ),
          });
        } else if (request.method === 'POST' && url.pathname === '/chats') {
          const parsed = createChatSchema.safeParse(
            await readJson(request, this.config.MAX_MESSAGE_BYTES),
          );
          if (!parsed.success)
            throw new HttpError(
              400,
              'INVALID_REQUEST',
              'A valid userId is required',
            );
          const result = await this.chats.create(userId, parsed.data.userId);
          sendJson(response, result.created ? 201 : 200, { chat: result.chat });
        } else if (request.method === 'GET' && url.pathname === '/chats') {
          sendJson(response, 200, { chats: await this.chats.list(userId) });
        } else {
          const history = url.pathname.match(/^\/chats\/([^/]+)\/messages$/);
          if (request.method !== 'GET' || !history?.[1])
            throw new HttpError(404, 'NOT_FOUND', 'Endpoint not found');
          const parsed = pageSchema.safeParse({
            afterSeq: url.searchParams.get('afterSeq') ?? undefined,
            limit: url.searchParams.get('limit') ?? undefined,
          });
          if (!parsed.success)
            throw new HttpError(
              400,
              'INVALID_REQUEST',
              'Invalid history pagination',
            );
          sendJson(
            response,
            200,
            await this.chats.history(
              history[1],
              userId,
              parsed.data.afterSeq,
              parsed.data.limit,
            ),
          );
        }
      }
    } catch (error) {
      if (error instanceof HttpError) {
        if (error.retryAfterMs)
          response.setHeader(
            'retry-after',
            Math.ceil(error.retryAfterMs / 1000),
          );
        sendJson(response, error.status, {
          error: {
            code: error.code,
            message: error.message,
            ...(error.retryAfterMs ? { retryAfterMs: error.retryAfterMs } : {}),
          },
        });
      } else {
        this.logger.error({ err: error }, 'HTTP API request failed');
        sendJson(response, 503, {
          error: {
            code: 'UNAVAILABLE',
            message: 'Service is temporarily unavailable',
          },
        });
      }
    }
    return true;
  }
}
