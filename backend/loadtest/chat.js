import { WebSocket } from 'k6/websockets';
import {
  setInterval,
  clearInterval,
  setTimeout,
  clearTimeout,
} from 'k6/timers';
import { check, sleep } from 'k6';
import crypto from 'k6/crypto';
import { Counter, Trend } from 'k6/metrics';
const target = Number(__ENV.TARGET ?? 2000);
const socketsPerVu = 50;
const vusFor = (connections) => Math.ceil(connections / socketsPerVu);
const fixture = JSON.parse(open('/results/fixture.json'));
const sent = new Counter('messages_sent');
const confirmed = new Counter('messages_confirmed');
const delivery = new Trend('delivery_latency_ms', true);
const rejected = new Counter('rate_rejections');
const errors = new Counter('socket_errors');
const duplicates = new Counter('duplicate_wire_frames');
const ordering = new Counter('ordering_violations');
const opened = new Counter('connections_opened');
export const options = {
  scenarios: {
    chat: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '20s', target: vusFor(Math.min(100, target)) },
        { duration: '20s', target: vusFor(Math.min(500, target)) },
        { duration: '20s', target: vusFor(target) },
        { duration: '30s', target: vusFor(target) },
        { duration: '10s', target: 0 },
      ],
      gracefulRampDown: '5s',
      gracefulStop: '5s',
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
function uuid() {
  const bytes = new Uint8Array(crypto.randomBytes(16));
  bytes[6] = (bytes[6] & 15) | 64;
  bytes[8] = (bytes[8] & 63) | 128;
  const hex = Array.from(bytes, (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
export function setup() {
  return { endsAt: Date.now() + 90000 };
}
export default function (timing) {
  if (Date.now() >= timing.endsAt) {
    sleep(1);
    return;
  }
  errors.add(0);
  rejected.add(0);
  duplicates.add(0);
  ordering.add(0);
  for (let local = 0; local < socketsPerVu; local++) {
    const index = (__VU - 1) * socketsPerVu + local;
    if (index >= target) break;
    const account = fixture.accounts[index % fixture.accounts.length];
    const pending = new Map();
    const seen = new Set();
    let lastSeq = 0;
    let welcomed = false;
    let connected = false;
    const socket = new WebSocket(__ENV.WS_URL ?? 'ws://nginx/ws', null, {
      headers: { 'Sec-WebSocket-Protocol': `pulsechat, jwt.${account.token}` },
      tags: { name: 'PulseChat WebSocket' },
    });
    const send = () => {
      if (
        !welcomed ||
        socket.readyState !== WebSocket.OPEN ||
        Date.now() >= timing.endsAt - 15000
      )
        return;
      const clientMsgId = uuid();
      pending.set(clientMsgId, Date.now());
      sent.add(1);
      socket.send(
        JSON.stringify({
          type: 'message',
          clientMsgId,
          chatId: account.chatId,
          body: `k6-socket-${index}-${clientMsgId}`,
        }),
      );
    };
    const first = setTimeout(send, 500 + Math.random() * 4000);
    const stream = setInterval(
      send,
      Number(__ENV.MESSAGE_INTERVAL_MS ?? 20000),
    );
    const heartbeat = setInterval(() => {
      if (socket.readyState === WebSocket.OPEN)
        socket.send(JSON.stringify({ type: 'heartbeat' }));
    }, 15000);
    const close = setTimeout(
      () => {
        clearTimeout(first);
        clearInterval(stream);
        clearInterval(heartbeat);
        socket.close(1000, 'Load test complete');
      },
      Math.max(1, timing.endsAt - Date.now()),
    );
    socket.onopen = () => {
      connected = true;
      opened.add(1);
      check(true, { 'WebSocket upgrade succeeded': (value) => value });
    };
    socket.onmessage = (event) => {
      const frame = JSON.parse(event.data);
      if (frame.type === 'welcome') welcomed = true;
      else if (frame.type === 'message') {
        if (seen.has(frame.messageId)) duplicates.add(1);
        else {
          seen.add(frame.messageId);
          if (frame.seq < lastSeq) ordering.add(1);
          lastSeq = Math.max(lastSeq, frame.seq);
        }
        if (pending.has(frame.clientMsgId)) {
          delivery.add(Date.now() - pending.get(frame.clientMsgId));
          confirmed.add(1);
          pending.delete(frame.clientMsgId);
        }
      } else if (frame.type === 'error') {
        if (frame.code === 'RATE_LIMITED') rejected.add(1);
        else errors.add(1);
      }
    };
    socket.onerror = () => errors.add(1);
    socket.onclose = (event) => {
      clearTimeout(first);
      clearTimeout(close);
      clearInterval(stream);
      clearInterval(heartbeat);
      if (!connected)
        check(false, { 'WebSocket upgrade succeeded': (value) => value });
      if (event.code !== 1000) errors.add(1);
    };
  }
}
export function handleSummary(data) {
  return {
    '/results/k6-summary.json': JSON.stringify(data, null, 2),
    stdout:
      JSON.stringify({
        sent: data.metrics.messages_sent?.values,
        confirmed: data.metrics.messages_confirmed?.values,
        latency: data.metrics.delivery_latency_ms?.values,
        opened: data.metrics.connections_opened?.values,
        socketErrors: data.metrics.socket_errors?.values,
        rateRejections: data.metrics.rate_rejections?.values,
      }) + '\n',
  };
}
