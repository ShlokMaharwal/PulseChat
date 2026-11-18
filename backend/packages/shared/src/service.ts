import { parseConfig } from './config.js';
import { createLogger } from './logger.js';
import { createDependencies } from './dependencies.js';
import { createHealthServer } from './health.js';
import type { IncomingMessage, ServerResponse, Server } from 'node:http';
import type { Registry } from 'prom-client';
import type { Logger } from 'pino';
import type { Config } from './config.js';

export interface ServiceRuntime {
  handleRequest?: (
    request: IncomingMessage,
    response: ServerResponse,
  ) => Promise<boolean>;
  beginShutdown?: () => void;
  stop: () => Promise<void>;
  ready?: () => boolean;
}

export interface ServiceContext {
  config: Config;
  logger: Logger;
  dependencies: ReturnType<typeof createDependencies>;
  server: Server;
  registry: Registry;
  accepting: () => boolean;
  fail: (error: unknown) => void;
}

export async function startService(
  service: 'gateway' | 'processor',
  initialize?: (context: ServiceContext) => Promise<ServiceRuntime>,
) {
  const config = parseConfig(process.env);
  const logger = createLogger(config, service);
  const dependencies = createDependencies(config, service, logger);
  let accepting = false;
  let closing = false;
  let startup: Promise<void> | undefined;
  let runtime: ServiceRuntime | undefined;
  const { server, registry } = createHealthServer({
    service,
    instanceId: config.INSTANCE_ID,
    check: dependencies.check,
    checkTimeoutMs: config.DEPENDENCY_TIMEOUT_MS,
    accepting: () => accepting && (runtime?.ready?.() ?? true),
    onError: (err) => logger.error({ err }, 'HTTP request failed'),
    handleRequest: async (request, response) => {
      if (!accepting) {
        response.writeHead(503, { 'content-type': 'application/json' });
        response.end(
          JSON.stringify({
            error: {
              code: 'UNAVAILABLE',
              message: 'Service is starting or draining',
            },
          }),
        );
        return true;
      }
      return (await runtime?.handleRequest?.(request, response)) ?? false;
    },
  });

  async function shutdown(signal: string) {
    if (closing) return;
    closing = true;
    accepting = false;
    try {
      runtime?.beginShutdown?.();
    } catch (err) {
      logger.error({ err }, 'Runtime drain hint failed');
    }
    logger.info({ signal }, 'Draining service');
    const deadline = setTimeout(() => {
      logger.fatal('Shutdown deadline exceeded');
      process.exit(1);
    }, config.SHUTDOWN_TIMEOUT_MS);
    deadline.unref();
    const httpClosed = new Promise<void>((resolve) => {
      server.close(() => resolve());
      server.closeIdleConnections();
    });
    // Let any in-flight startup connections settle before closing their clients.
    await startup?.catch(() => undefined);
    await runtime?.stop().catch((err: unknown) => {
      logger.error({ err }, 'Runtime shutdown failed');
      process.exitCode = 1;
    });
    await httpClosed;
    await dependencies.close();
    clearTimeout(deadline);
    logger.info('Service stopped');
  }

  const onTerm = () => {
    void shutdown('SIGTERM');
  };
  const onInt = () => {
    void shutdown('SIGINT');
  };
  process.once('SIGTERM', onTerm);
  process.once('SIGINT', onInt);
  try {
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(config.PORT, '0.0.0.0', () => {
        server.off('error', reject);
        resolve();
      });
    });
    if (closing) return;
    startup = (async () => {
      await dependencies.connect();
      if (initialize && !closing) {
        runtime = await initialize({
          config,
          logger,
          dependencies,
          server,
          registry,
          accepting: () => accepting && !closing,
          fail: (err) => {
            logger.fatal({ err }, 'Runtime failed');
            process.exitCode = 1;
            void shutdown('runtime_failure');
          },
        });
      }
    })();
    await startup;
    if (closing) return;
    accepting = true;
    logger.info({ port: config.PORT }, 'Service ready');
  } catch (err) {
    logger.fatal({ err }, 'Startup failed');
    process.exitCode = 1;
    await shutdown('startup_failure');
  }
}
