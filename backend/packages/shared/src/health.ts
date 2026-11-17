import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from 'node:http';
import { collectDefaultMetrics, Registry } from 'prom-client';
import type { DependencyHealth } from './dependencies.js';

interface HealthOptions {
  service: string;
  instanceId: string;
  check: () => Promise<DependencyHealth>;
  accepting: () => boolean;
  onError: (error: unknown) => void;
  checkTimeoutMs?: number;
}

export function createHealthServer(options: HealthOptions) {
  const registry = new Registry();
  registry.setDefaultLabels({
    service: options.service,
    instance: options.instanceId,
  });
  collectDefaultMetrics({ register: registry, prefix: 'pulsechat_' });
  // Concurrent scrapes/health checks share one dependency probe; results are not cached.
  let pendingCheck: Promise<DependencyHealth> | undefined;

  async function boundedCheck() {
    let deadline: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        options.check(),
        new Promise<never>((_, reject) => {
          deadline = setTimeout(
            () => reject(new Error('Dependency probe timed out')),
            options.checkTimeoutMs ?? 3000,
          );
        }),
      ]);
    } finally {
      clearTimeout(deadline);
    }
  }

  function json(response: ServerResponse, status: number, body: unknown) {
    response.writeHead(status, {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    });
    response.end(JSON.stringify(body));
  }

  async function handle(request: IncomingMessage, response: ServerResponse) {
    const path = request.url?.split('?')[0];
    if (request.method !== 'GET') {
      response.setHeader('allow', 'GET');
      json(response, 405, { error: 'METHOD_NOT_ALLOWED' });
    } else if (path === '/healthz') {
      json(response, 200, {
        status: 'alive',
        service: options.service,
        instanceId: options.instanceId,
      });
    } else if (path === '/readyz') {
      if (!options.accepting()) {
        json(response, 503, {
          status: 'not_ready',
          service: options.service,
          instanceId: options.instanceId,
        });
        return;
      }
      pendingCheck ??= boundedCheck().finally(() => {
        pendingCheck = undefined;
      });
      const dependencies = await pendingCheck;
      const ready =
        options.accepting() && Object.values(dependencies).every(Boolean);
      json(response, ready ? 200 : 503, {
        status: ready ? 'ready' : 'not_ready',
        service: options.service,
        instanceId: options.instanceId,
        dependencies,
      });
    } else if (path === '/metrics') {
      response.writeHead(200, { 'content-type': registry.contentType });
      response.end(await registry.metrics());
    } else {
      json(response, 404, { error: 'NOT_FOUND' });
    }
  }

  const server = createServer((request, response) => {
    void handle(request, response).catch((error: unknown) => {
      options.onError(error);
      if (!response.headersSent) json(response, 503, { error: 'UNAVAILABLE' });
      else response.destroy();
    });
  });
  server.requestTimeout = 10000;
  server.headersTimeout = 10000;
  return { server, registry };
}
