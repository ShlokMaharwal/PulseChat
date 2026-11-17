import { parseConfig } from './config.js';
import { createLogger } from './logger.js';
import { createDependencies } from './dependencies.js';
import { createHealthServer } from './health.js';

export async function startService(service: 'gateway' | 'processor') {
  const config = parseConfig(process.env);
  const logger = createLogger(config, service);
  const dependencies = createDependencies(config, service, logger);
  let accepting = false;
  let closing = false;
  let startup: Promise<void> | undefined;
  const { server } = createHealthServer({
    service,
    instanceId: config.INSTANCE_ID,
    check: dependencies.check,
    checkTimeoutMs: config.DEPENDENCY_TIMEOUT_MS,
    accepting: () => accepting,
    onError: (err) => logger.error({ err }, 'HTTP request failed'),
  });

  async function shutdown(signal: string) {
    if (closing) return;
    closing = true;
    accepting = false;
    logger.info({ signal }, 'Draining service');
    const deadline = setTimeout(() => {
      logger.fatal('Shutdown deadline exceeded');
      process.exit(1);
    }, config.SHUTDOWN_TIMEOUT_MS);
    deadline.unref();
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
      server.closeIdleConnections();
    });
    // Let any in-flight startup connections settle before closing their clients.
    await startup?.catch(() => undefined);
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
    startup = dependencies.connect();
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
