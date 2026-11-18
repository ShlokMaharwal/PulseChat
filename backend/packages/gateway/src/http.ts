import type { IncomingMessage, ServerResponse } from 'node:http';
import type { Logger } from 'pino';
import { z } from 'zod';
import { uuidSchema, type Config } from '@pulsechat/shared';
import { AuthService, bearerToken, verifyToken } from './auth.js';
import { ChatService } from './chats.js';
import { HttpError } from './errors.js';

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
          Buffer.concat(chunks).toString('utf8'),
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
  ) {}

  async handle(
    request: IncomingMessage,
    response: ServerResponse,
  ): Promise<boolean> {
    const url = new URL(request.url ?? '/', 'http://gateway');
    if (
      !['/auth/register', '/auth/login', '/users', '/chats'].includes(
        url.pathname,
      ) &&
      !url.pathname.startsWith('/chats/')
    )
      return false;
    try {
      if (request.method === 'POST' && url.pathname === '/auth/register') {
        sendJson(
          response,
          201,
          await this.auth.register(
            await readJson(request, this.config.MAX_MESSAGE_BYTES),
          ),
        );
      } else if (request.method === 'POST' && url.pathname === '/auth/login') {
        sendJson(
          response,
          200,
          await this.auth.login(
            await readJson(request, this.config.MAX_MESSAGE_BYTES),
          ),
        );
      } else {
        const userId = verifyToken(
          bearerToken(request.headers.authorization),
          this.config,
        );
        await this.auth.user(userId);
        if (request.method === 'GET' && url.pathname === '/users') {
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
        sendJson(response, error.status, {
          error: { code: error.code, message: error.message },
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
