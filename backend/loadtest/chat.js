import { WebSocket } from 'k6/websockets';
import {
  setInterval,
  clearInterval,
  setTimeout,
  clearTimeout,
} from 'k6/timers';
import { check } from 'k6';
import crypto from 'k6/crypto';
import { Counter, Trend } from 'k6/metrics';
const fixture = JSON.parse(open(__ENV.FIXTURE_PATH ?? '/results/fixture.json'));
const target = Number(__ENV.TARGET);
const ramp = Number(__ENV.RAMP_MS ?? 10000);
const steady = Number(__ENV.STEADY_MS ?? 30000);
const warm = Number(__ENV.WARMUP_MS ?? 5000);
const drain = Number(__ENV.DRAIN_MS ?? 30000);
const interval = Number(__ENV.MESSAGE_INTERVAL_MS ?? 1000);
const socketsPerVu = Number(__ENV.SOCKETS_PER_VU ?? 50);
const runId = __ENV.RUN_ID;
if (
  fixture.accounts.length !== target ||
  new Set(fixture.accounts.map((a) => a.userId)).size !== target
)
  throw new Error('Every socket requires its own distinct fixture user');
const sent = new Counter('messages_sent');
const confirmed = new Counter('messages_confirmed');
const accepted = new Counter('messages_received_acked');
const delivery = new Trend('delivery_latency_ms', true);
const ackLatency = new Trend('received_ack_latency_ms', true);
const scheduler = new Trend('generator_scheduler_delay_ms', true);
const errors = new Counter('socket_errors');
const rejected = new Counter('rate_rejections');
const duplicates = new Counter('duplicate_wire_frames');
const ordering = new Counter('ordering_violations');
const opened = new Counter('connections_opened');
const syncs = new Counter('sync_requests');
export const options = {
  scenarios: {
    chat: {
      executor: 'per-vu-iterations',
      vus: Math.ceil(target / socketsPerVu),
      iterations: 1,
      maxDuration: ramp + warm + steady + drain + 15000 + 'ms',
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(95)', 'p(99)'],
  thresholds: {
    checks: ['rate>0.99'],
    messages_confirmed: ['count>0'],
    ordering_violations: ['count==0'],
    duplicate_wire_frames: ['count==0'],
  },
};
function log(event) {
  console.log(JSON.stringify(event));
}
function uuid() {
  const bytes = new Uint8Array(crypto.randomBytes(16));
  bytes[6] = (bytes[6] & 15) | 64;
  bytes[8] = (bytes[8] & 63) | 128;
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join(
    '',
  );
  return (
    hex.slice(0, 8) +
    '-' +
    hex.slice(8, 12) +
    '-' +
    hex.slice(12, 16) +
    '-' +
    hex.slice(16, 20) +
    '-' +
    hex.slice(20)
  );
}
export function setup() {
  const startsAt = Date.now() + 2000;
  const timing = {
    startsAt,
    steadyStartAt: startsAt + ramp + warm,
    sendsEndAt: startsAt + ramp + warm + steady,
    closesAt: startsAt + ramp + warm + steady + drain,
  };
  log({ event: 'timing', ...timing });
  return timing;
}
export default function (timing) {
  for (const metric of [
    errors,
    rejected,
    duplicates,
    ordering,
    syncs,
    accepted,
  ])
    metric.add(0);
  let previous = Date.now();
  const monitor = setInterval(() => {
    const now = Date.now();
    scheduler.add(Math.max(0, now - previous - 1000));
    previous = now;
  }, 1000);
  setTimeout(
    () => clearInterval(monitor),
    Math.max(1, timing.closesAt - Date.now()),
  );
  for (let local = 0; local < socketsPerVu; local++) {
    const index = (__VU - 1) * socketsPerVu + local;
    if (index >= target) break;
    const account = fixture.accounts[index];
    let socket,
      welcomed = false,
      finished = false,
      cursor = account.initialSeq ?? 0,
      syncing = false,
      attempt = 0,
      syncTimeout;
    let heartbeat, stream;
    let lastLive = 0,
      latestPeer,
      readSeq = 0;
    const liveIds = new Set(),
      ingested = new Set(),
      acked = new Set(),
      pending = new Map(),
      buffer = new Map();
    const timers = new Set();
    const later = (fn, ms) => {
      const timer = setTimeout(
        () => {
          timers.delete(timer);
          if (!finished) fn();
        },
        Math.max(1, ms),
      );
      timers.add(timer);
      return timer;
    };
    const transmit = (frame) => {
      if (!welcomed || socket?.readyState !== WebSocket.OPEN) return false;
      try {
        socket.send(JSON.stringify(frame));
        return true;
      } catch (error) {
        errors.add(1);
        log({
          event: 'send_error',
          index,
          at: Date.now(),
          clientMsgId: frame.clientMsgId,
          message: String(error),
        });
        return false;
      }
    };
    const sync = () => {
      if (syncing || !welcomed) return;
      syncing = true;
      syncs.add(1);
      transmit({
        type: 'sync',
        chatId: account.chatId,
        afterSeq: cursor,
        limit: 100,
      });
      clearTimeout(syncTimeout);
      syncTimeout = later(() => {
        syncing = false;
        sync();
      }, 10000);
    };
    const ingest = (message) => {
      if (message.seq <= cursor || ingested.has(message.messageId)) return;
      cursor = message.seq;
      ingested.add(message.messageId);
      if (message.senderId === account.userId)
        pending.delete(message.clientMsgId);
      else {
        latestPeer = message;
        try {
          const payload = JSON.parse(message.body);
          if (payload.loadRun === runId) {
            const now = Date.now();
            delivery.add(now - payload.clientSentAt);
            confirmed.add(1);
            log({
              event: 'delivered',
              index,
              at: now,
              clientMsgId: message.clientMsgId,
              seq: message.seq,
              trace: message.trace,
            });
          }
        } catch {
          /* Fixture preflight messages are not measured traffic. */
        }
      }
    };
    const drainBuffer = () => {
      while (buffer.has(cursor + 1)) {
        const message = buffer.get(cursor + 1);
        buffer.delete(cursor + 1);
        ingest(message);
      }
      if (buffer.size) sync();
    };
    const open = () => {
      if (finished) return;
      welcomed = false;
      syncing = false;
      const current = new WebSocket(__ENV.WS_URL ?? 'ws://nginx/ws', null, {
        headers: {
          'Sec-WebSocket-Protocol': 'pulsechat, jwt.' + account.token,
        },
        tags: { name: 'PulseChat WebSocket' },
      });
      socket = current;
      let connected = false;
      current.onopen = () => {
        opened.add(1);
        if (finished) current.close(1000, 'Load test complete');
      };
      current.onmessage = (event) => {
        if (current !== socket || finished) return;
        const frame = JSON.parse(event.data);
        if (frame.type === 'welcome') {
          welcomed = true;
          connected = true;
          attempt = 0;
          if (heartbeat !== undefined) clearInterval(heartbeat);
          heartbeat = setInterval(() => {
            transmit({ type: 'heartbeat' });
            sync();
          }, 15000);
          check(true, { 'Authenticated welcome received': (value) => value });
          log({
            event: 'connected',
            index,
            at: Date.now(),
            gatewayId: frame.gatewayId,
          });
          for (const message of pending.values())
            if (!message.received) transmit(message.frame);
          sync();
        } else if (frame.type === 'ack') {
          const item = pending.get(frame.clientMsgId);
          if (frame.status === 'received') {
            if (!acked.has(frame.clientMsgId)) {
              acked.add(frame.clientMsgId);
              accepted.add(1);
              if (item) ackLatency.add(Date.now() - item.sentAt);
            }
            if (item) item.received = true;
            log({
              event: 'received_ack',
              index,
              at: Date.now(),
              clientMsgId: frame.clientMsgId,
              trace: frame.trace,
            });
          } else
            log({
              event: 'sent_ack',
              index,
              at: Date.now(),
              clientMsgId: frame.clientMsgId,
              seq: frame.seq,
            });
        } else if (frame.type === 'message') {
          if (liveIds.has(frame.messageId)) duplicates.add(1);
          else {
            liveIds.add(frame.messageId);
            if (frame.seq <= lastLive) ordering.add(1);
            lastLive = Math.max(lastLive, frame.seq);
          }
          if (frame.seq === cursor + 1 && !syncing) {
            ingest(frame);
            drainBuffer();
          } else if (frame.seq > cursor) {
            buffer.set(frame.seq, frame);
            sync();
          }
        } else if (frame.type === 'sync') {
          syncing = false;
          clearTimeout(syncTimeout);
          for (const message of frame.messages) ingest(message);
          for (const seq of buffer.keys())
            if (seq <= cursor) buffer.delete(seq);
          if (frame.hasMore) sync();
          else drainBuffer();
        } else if (frame.type === 'error') {
          if (frame.code === 'RATE_LIMITED') rejected.add(1);
          else errors.add(1);
          log({
            event: 'application_error',
            index,
            at: Date.now(),
            code: frame.code,
            clientMsgId: frame.clientMsgId,
          });
          if (frame.operation === 'sync') syncing = false;
          if (frame.code === 'RATE_LIMITED' || frame.code === 'UNAVAILABLE')
            later(
              () => {
                const item = pending.get(frame.clientMsgId);
                if (item && !item.received) transmit(item.frame);
                if (frame.operation === 'sync') sync();
              },
              Math.max(100, frame.retryAfterMs ?? 1000) + Math.random() * 200,
            );
        } else if (frame.type === 'reconnect')
          current.close(1000, 'Reconnect requested');
      };
      current.onerror = () => errors.add(1);
      current.onclose = (event) => {
        if (current !== socket) return;
        if (connected)
          log({
            event: 'disconnected',
            index,
            at: Date.now(),
            code: event.code,
          });
        else if (!finished)
          check(false, { 'Authenticated welcome received': (value) => value });
        welcomed = false;
        syncing = false;
        if (heartbeat !== undefined) clearInterval(heartbeat);
        clearTimeout(syncTimeout);
        if (!finished) {
          errors.add(1);
          later(
            open,
            Math.min(30000, 500 * 2 ** Math.min(attempt++, 6)) *
              (0.75 + Math.random() * 0.5),
          );
        }
      };
    };
    const sendOnce = () => {
      const now = Date.now();
      // Reserve the first part of warmup for connection setup before adding
      // message traffic; use the final five seconds to warm the pipeline.
      if (
        !welcomed ||
        now < timing.steadyStartAt - Math.min(warm, 5000) ||
        now >= timing.sendsEndAt
      )
        return;
      const clientMsgId = uuid();
      const frame = {
        type: 'message',
        clientMsgId,
        chatId: account.chatId,
        body: JSON.stringify({ loadRun: runId, index, clientSentAt: now }),
      };
      pending.set(clientMsgId, { frame, sentAt: now, received: false });
      sent.add(1);
      log({
        event: 'issued',
        index,
        at: now,
        clientMsgId,
        chatId: account.chatId,
      });
      transmit(frame);
    };
    later(
      () => {
        sendOnce();
        stream = setInterval(sendOnce, interval);
      },
      ((((index + 1) * 2654435761) % 10000) / 10000) * interval,
    );
    const receipts = setInterval(() => {
      if (!latestPeer) return;
      transmit({
        type: 'delivered',
        chatId: account.chatId,
        messageId: latestPeer.messageId,
      });
      if (__ENV.READ_RECEIPTS !== 'false' && cursor > readSeq) {
        transmit({ type: 'read', chatId: account.chatId, lastReadSeq: cursor });
        readSeq = cursor;
      }
      latestPeer = undefined;
    }, 200);
    later(
      open,
      Math.max(0, timing.startsAt - Date.now()) + (ramp * index) / target,
    );
    setTimeout(
      () => {
        finished = true;
        for (const timer of timers) clearTimeout(timer);
        if (stream !== undefined) clearInterval(stream);
        clearInterval(receipts);
        if (heartbeat !== undefined) clearInterval(heartbeat);
        try {
          if (socket) socket.close(1000, 'Load test complete');
        } catch {
          errors.add(1);
        }
        log({
          event: 'client_finished',
          index,
          at: Date.now(),
          unacked: [...pending.values()].filter((item) => !item.received)
            .length,
        });
      },
      Math.max(1, timing.closesAt - Date.now()),
    );
  }
}
export function handleSummary(data) {
  return {
    '/results/k6-summary.json': JSON.stringify(data, null, 2),
    stdout:
      JSON.stringify({
        sent: data.metrics.messages_sent?.values,
        receivedAcked: data.metrics.messages_received_acked?.values,
        peerConfirmed: data.metrics.messages_confirmed?.values,
        latency: data.metrics.delivery_latency_ms?.values,
        schedulerDelay: data.metrics.generator_scheduler_delay_ms?.values,
      }) + '\n',
  };
}
