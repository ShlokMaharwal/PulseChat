import { startService } from '@pulsechat/shared';
import { createGatewayRuntime } from './runtime.js';

await startService('gateway', createGatewayRuntime);
