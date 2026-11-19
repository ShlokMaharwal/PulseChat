import { expect, it } from 'vitest';
import {
  classify,
  connectionCounts,
  distribution,
  type LoadEvent,
} from '../../loadtest/report.js';
it('distinguishes unaccepted attempts, lost acceptance, and persistence without an observed ACK', () => {
  const events: LoadEvent[] = ['a', 'b', 'c', 'd'].map((clientMsgId) => ({
    event: 'issued',
    at: 1,
    clientMsgId,
  }));
  events.push(
    { event: 'received_ack', at: 2, clientMsgId: 'a' },
    { event: 'received_ack', at: 2, clientMsgId: 'b' },
  );
  const result = classify(
    events,
    new Set(['a', 'b', 'c']),
    new Set(['a', 'c']),
  );
  expect(result).toMatchObject({
    issued: 4,
    receivedAcked: 2,
    kafkaAccepted: 3,
    persisted: 2,
    clientFailedBeforeAck: 1,
    persistedWithoutReceivedAck: 1,
    acceptedButUnpersisted: 1,
    unexpectedIds: [],
  });
  expect(result.acceptedButUnpersistedIds).toEqual(['b']);
  expect(result.clientFailedBeforeAckIds).toEqual(['d']);
});
it('flags a ledger that omitted an accepted send instead of claiming complete classification', () => {
  const result = classify([], new Set(['unlogged']), new Set(['unlogged']));
  expect(result.unexpectedIds).toEqual(['unlogged']);
});
it('keeps an unacknowledged send unresolved when the pipeline has not settled', () => {
  const result = classify(
    [{ event: 'issued', at: 1, clientMsgId: 'pending' }],
    new Set(),
    new Set(),
    false,
  );
  expect(result.clientFailedBeforeAck).toBe(0);
  expect(result.unresolvedBeforeAckIds).toEqual(['pending']);
});
it('distinguishes a late peak from holding the target during the measurement window', () => {
  expect(
    connectionCounts(
      [
        { event: 'connected', index: 0, at: 1 },
        { event: 'connected', index: 1, at: 21 },
        { event: 'client_finished', index: 0, at: 30 },
        { event: 'client_finished', index: 1, at: 30 },
      ],
      10,
      20,
    ),
  ).toEqual({ peak: 2, steadyMinimum: 1 });
  expect(
    connectionCounts(
      [
        { event: 'connected', index: 0, at: 1 },
        { event: 'connected', index: 1, at: 2 },
        { event: 'disconnected', index: 1, at: 12 },
        { event: 'connected', index: 1, at: 15 },
      ],
      10,
      20,
    ),
  ).toEqual({ peak: 2, steadyMinimum: 1 });
});
it('reports no latency for an empty sample and defines interpolated percentiles', () => {
  expect(distribution([])).toMatchObject({
    count: 0,
    p50: null,
    p95: null,
    p99: null,
  });
  expect(distribution([30, 10])).toMatchObject({
    count: 2,
    p50: 20,
    p95: 29,
    min: 10,
    max: 30,
  });
});
