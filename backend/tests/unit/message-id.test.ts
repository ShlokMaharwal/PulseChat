import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { messageIdFor } from '../../packages/shared/src/sequencing.js';
import { uuidSchema } from '../../packages/shared/src/schemas.js';

describe('stable sender-scoped message identifiers', () => {
  it('reuses a UUID for every retry of the same logical send', () => {
    const sender = randomUUID();
    const client = randomUUID();
    const id = messageIdFor(sender, client);
    expect(uuidSchema.safeParse(id).success).toBe(true);
    expect(messageIdFor(sender, client)).toBe(id);
  });
  it('keeps separate users and separate logical sends independent', () => {
    const sender = randomUUID();
    const client = randomUUID();
    expect(messageIdFor(sender, client)).not.toBe(
      messageIdFor(randomUUID(), client),
    );
    expect(messageIdFor(sender, client)).not.toBe(
      messageIdFor(sender, randomUUID()),
    );
  });
});
