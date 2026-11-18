import type { ServiceContext, ServiceRuntime } from '@pulsechat/shared';
import { MessageProcessor } from './messages.js';

export async function createProcessorRuntime(
  context: ServiceContext,
): Promise<ServiceRuntime> {
  const processor = new MessageProcessor(context);
  try {
    await processor.start();
    return { ready: processor.ready, stop: () => processor.stop() };
  } catch (error) {
    await processor.stop().catch(() => undefined);
    throw error;
  }
}
