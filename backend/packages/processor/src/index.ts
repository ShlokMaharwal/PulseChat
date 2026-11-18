import { startService } from '@pulsechat/shared';
import { createProcessorRuntime } from './runtime.js';

await startService('processor', createProcessorRuntime);
