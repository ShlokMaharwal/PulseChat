import pino from 'pino';
import type { Config } from './config.js';

export function createLogger(config: Config, service: string) {
  return pino({
    level: config.LOG_LEVEL,
    base: { service, instanceId: config.INSTANCE_ID },
    redact: [
      'password',
      'token',
      'authorization',
      'JWT_SECRET',
      'req.headers.authorization',
    ],
    timestamp: pino.stdTimeFunctions.isoTime,
  });
}
