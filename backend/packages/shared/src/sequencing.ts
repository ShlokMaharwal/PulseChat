import { createHash, randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import type { createDependencies } from './dependencies.js';

// Namespace UUID for deterministic sender/client-message UUIDv5 identifiers.
const namespace = Buffer.from('d72cb32f2586413ea4d5ad6e48b23356', 'hex');

export function messageIdFor(senderId: string, clientMsgId: string): string {
  const bytes = createHash('sha1')
    .update(namespace)
    .update(`${senderId}:${clientMsgId}`)
    .digest()
    .subarray(0, 16);
  bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x50;
  bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

// Zero requests seeding. A stale seed cannot survive loss of its Redis lease.
export const ASSIGN_SEQUENCE_LUA = `
local previous = redis.call('HGET', KEYS[2], ARGV[1])
if previous then return tonumber(previous) end
if redis.call('EXISTS', KEYS[1]) == 0 then
  if ARGV[3] == '' or redis.call('GET', KEYS[3]) ~= ARGV[3] then return 0 end
  redis.call('SET', KEYS[1], ARGV[2], 'NX')
end
local seq = redis.call('INCR', KEYS[1])
redis.call('HSET', KEYS[2], ARGV[1], seq)
return seq
`;

const UNLOCK_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`;

type Dependencies = Pick<ReturnType<typeof createDependencies>, 'redis' | 'db'>;

export class SequenceAllocator {
  constructor(private readonly dependencies: Dependencies) {}

  async assign(
    chatId: string,
    messageId: string,
    heartbeat: () => Promise<void> = async () => undefined,
  ): Promise<number> {
    const counter = `chat:${chatId}:seq`;
    const mapping = `chat:${chatId}:msgseq`;
    const lease = `chat:${chatId}:seq:init`;
    const { redis, db } = this.dependencies;
    const expires = Date.now() + 60000;
    const evaluate = async (seed: number, token: string) => {
      const result = Number(
        await redis.eval(
          ASSIGN_SEQUENCE_LUA,
          3,
          counter,
          mapping,
          lease,
          messageId,
          seed,
          token,
        ),
      );
      if (!Number.isSafeInteger(result) || result < 0)
        throw new Error('Invalid or exhausted chat sequence');
      return result;
    };

    while (Date.now() < expires) {
      const known = await evaluate(0, '');
      if (known) return known;
      const token = randomUUID();
      const acquired = await redis.set(lease, token, 'PX', 30000, 'NX');
      if (acquired) {
        try {
          const highest = await db
            .collection<{ chatId: string; seq: number }>('messages')
            .findOne(
              { chatId },
              { sort: { seq: -1 }, projection: { _id: 0, seq: 1 } },
            );
          const result = await evaluate(highest?.seq ?? 0, token);
          if (result) return result;
        } finally {
          // The lease also expires after a crashed or disconnected initializer.
          await redis.eval(UNLOCK_LUA, 1, lease, token).catch(() => undefined);
        }
      }
      await heartbeat();
      await sleep(25 + Math.floor(Math.random() * 25));
    }
    throw new Error('Timed out acquiring the sequence initialization lease');
  }
}
