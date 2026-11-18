import { createServer, request as httpRequest, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it } from 'vitest';
import { readJson, sendJson } from '../../packages/gateway/src/http.js';
import { HttpError } from '../../packages/gateway/src/errors.js';

const servers: Server[] = [];
async function server() {
  const instance = createServer((request, response) => {
    void readJson(request, 256)
      .then((body) => sendJson(response, 200, body))
      .catch((error: unknown) => {
        sendJson(response, error instanceof HttpError ? error.status : 500, {
          code: error instanceof HttpError ? error.code : 'INTERNAL_ERROR',
        });
      });
  });
  servers.push(instance);
  await new Promise<void>((resolve) =>
    instance.listen(0, '127.0.0.1', resolve),
  );
  return `http://127.0.0.1:${(instance.address() as AddressInfo).port}`;
}

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (instance) =>
        new Promise<void>((resolve) => {
          instance.close(() => resolve());
          instance.closeAllConnections();
        }),
    ),
  );
});

describe('bounded HTTP JSON input', () => {
  it('parses a valid UTF-8 JSON body', async () => {
    const base = await server();
    const response = await fetch(base, {
      method: 'POST',
      headers: { 'content-type': 'application/json; charset=utf-8' },
      body: JSON.stringify({ message: 'hello 🌍' }),
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ message: 'hello 🌍' });
  });
  it('rejects malformed JSON and unsupported content types', async () => {
    const base = await server();
    expect(
      (
        await fetch(base, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: '{broken',
        })
      ).status,
    ).toBe(400);
    expect(
      (
        await fetch(base, {
          method: 'POST',
          headers: { 'content-type': 'text/plain' },
          body: '{}',
        })
      ).status,
    ).toBe(400);
  });
  it('returns 413 for declared oversized bodies without resetting the response', async () => {
    const base = await server();
    const response = await fetch(base, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ message: 'x'.repeat(300) }),
    });
    expect(response.status).toBe(413);
    expect(await response.json()).toEqual({ code: 'PAYLOAD_TOO_LARGE' });
  });
  it('enforces the byte cap on chunked bodies without Content-Length', async () => {
    const base = await server();
    const result = await new Promise<{ status: number; body: string }>(
      (resolve, reject) => {
        const outgoing = httpRequest(
          base,
          {
            method: 'POST',
            headers: {
              'content-type': 'application/json',
              'transfer-encoding': 'chunked',
            },
          },
          (incoming) => {
            let body = '';
            incoming.setEncoding('utf8');
            incoming.on('data', (chunk: string) => {
              body += chunk;
            });
            incoming.on('end', () =>
              resolve({ status: incoming.statusCode ?? 0, body }),
            );
            incoming.on('error', reject);
          },
        );
        outgoing.on('error', reject);
        outgoing.write('{"message":"');
        outgoing.write('x'.repeat(300));
        outgoing.end('"}');
      },
    );
    expect(result.status).toBe(413);
    expect(JSON.parse(result.body)).toEqual({ code: 'PAYLOAD_TOO_LARGE' });
  });
});
