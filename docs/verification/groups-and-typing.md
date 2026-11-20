# Groups, typing, and interface updates

## Scope

Small groups contain 3–20 members including the creator. The creator chooses a name and existing users in **New conversation → New group**. Membership is fixed at creation. Group messages retain the existing chat-keyed Kafka ordering, stable retry identifiers, MongoDB persistence, and per-member receipt cursors. The interface shows incoming sender names and marks group messages read only after every other member reads them.

Typing hints use authenticated, membership-checked Redis routing. They do not enter Kafka or message storage. Active hints are throttled; the browser sends stop on send, blur, chat switch, or 1.5 seconds of idle. Received hints expire after six seconds, are tracked separately for each socket session, and are cleared rather than replayed after disconnection.

Login and registration have an accessible show/hide password button. Messages and form inputs use 16px system fonts with larger conversation labels. No new dependencies, external font requests, database index migrations, or Docker resource limits were introduced.

## Source verification

On 20 November 2026, backend lint, format, types, build, and **86 unit tests** passed. Frontend lint, format, types, build, and **17 unit tests** passed. Added tests cover strict typing frames, sender spoof rejection, throttling, idle stop, multi-session expiry, offline clearing, and no reconnect replay. Gateway, processor, integration, and frontend images built successfully.

A real Microsoft Edge check against the standalone frontend preview passed password visibility in login and registration, preserved input values, hiding the password after switching modes, the 16px input size, and no horizontal overflow at 390px width. Desktop and mobile login screenshots were visually inspected. This check did not substitute for live backend/group verification.

## Runtime verification

The rebuilt frontend, three gateways, two processors, and Nginx all reached healthy status. The readiness response reported Redis, MongoDB, and Kafka ready at local port 8081.

The initial live integration attempt was interrupted after Docker Desktop lost its route to the Linux engine. Its API returned internal-server errors, and the browser suite timed out loading the app before registration; five later browser cases did not run. These attempts are not counted as passes. The expanded suites include group membership validation, three-sender ordering and retry deduplication, cross-gateway typing authorization, group discovery, receipts, offline sync, password visibility, and mobile layout.

After engine recovery, the **two focused live integration cases passed** (13 existing cases intentionally skipped). They verified independent groups with identical membership, invalid membership/name rejection, 30 ordered canonical messages from three concurrent senders, retry deduplication, matching delivery to all three members, outsider history denial, cross-gateway typing start/stop, non-member typing rejection, and no stored messages from typing. The focused run completed in 23.73 seconds.

```powershell
$env:PULSECHAT_PORT = '8081'
docker compose -f docker-compose.yml -f docker-compose.performance.yml --profile test run --rm --no-deps integration npm run test:integration -- --reporter=verbose -t 'groups|typing'
```

A focused real Edge browser flow then passed with three independent accounts: group creation through the modal, automatic discovery for both invited members, typing visible to both members and clearing on send, matching canonical message IDs, messages from all three senders, read receipts from all other members, offline third-member catch-up, 16px message text, and a 390px mobile layout without horizontal overflow. No browser page errors occurred. Dark desktop and light mobile group screenshots were visually inspected. The full six-case browser suite was not rerun after engine recovery; the focused flow covered the new features without repeating unrelated gateway-kill and renewal tests.

Final chat service readiness reported Redis, MongoDB, and Kafka ready at `http://localhost:8081`. Seekr's databases, API, and crawler were restored after their authorized temporary pause; its databases and API were healthy. PulseChat's Prometheus and Grafana were restarted afterward. No data volumes were removed.

Historical load, chaos, and CI results linked in the README describe earlier revisions; they were not rerun for this update. They do not establish group capacity.

See the [protocol](../PROTOCOL.md), [design](../DESIGN.md), and [test commands](../TESTING.md).
