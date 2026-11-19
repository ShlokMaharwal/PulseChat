# Task 6: product and browser verification

The existing UI already had neutral black/gray and emerald theme tokens, light/dark persistence, day grouping, optimistic sending/queued/sent states, canonical delivered/read receipts, accessible controls, and a keyboard-trapped conversation dialog. Those behaviors were retained.

The remaining changes add a connection/degradation banner with an actual reconnect countdown and manual retry, failed-send retry preserving the original UUID, first-unread navigation, scroll position preservation while reading history, and focus restoration when the dialog closes. Credentials remain in session storage; ordered cursors and pending sends retain their user-scoped storage keys.

Source verification passed lint, formatting, strict types, 15 unit tests, and a production build. All five real Playwright cases passed in 53.4 seconds: registration/login/themes/keyboard navigation, sending and escaped text/receipts/day grouping/unread navigation, offline queuing, actual gateway SIGKILL recovery, and silent renewal/logout revocation. The backend runtime remains the Task 5 version; its source, integration, smoke, and six-chaos results are linked from [Task 5](task-5.md).

## Reproduce

Start the full stack, then run from `frontend/`:

```sh
npm ci
npx playwright install chromium
npm run test:e2e
```

Set `TEST_HTTP_URL` for the actual application URL (the local default is `http://127.0.0.1:8081`). For an installed Microsoft Edge browser, set `PLAYWRIGHT_CHANNEL=msedge`. CI downloads Chromium and runs the same tests against its stack at port 8080. Browser output and failure traces go to ignored `backend/.cache/` and are uploaded as CI artifacts.

The reproducible demo command is `npm run demo`. It registers two synthetic browser accounts, captures their actual rendered chat sessions, kills their connected gateway, waits for reconnection and recovered messages, then encodes the screenshots side by side. Set `PYTHON_BIN` to a Python executable with dependencies from `tools/requirements-demo.txt`; Pillow only encodes the real screenshots. No UI responses or message content are mocked. The script restores the interrupted gateway in `finally` and writes a safe proof summary without credentials.

The [repository metadata text](../REPO_METADATA.md) lists the exact description and topics. Optional groups and typing indicators remain outside this change.

## Recorded results

[All five browser cases passed](task-6-e2e.json). The [demo proof](demo-proof.json) records two real sessions, four logical messages, gateway-2 SIGKILL, recovery on gateway-3, and zero page errors. The resulting animated GIF is 1600 × 672 pixels, seven encoded frames, and 355,784 bytes. Captions identify the actual kill/recovery phases; the app itself is unmodified. Repeated identical screenshots are coalesced by the encoder. The original screenshots and credentials remain ignored; only the rendered public demonstration and safe proof are committed.

The final source rerun passed 82 backend and 15 frontend tests plus lint, format, types, and builds. An earlier concurrent rerun hit Windows memory exhaustion; serial reruns passed. Docker also retained a missing Redis-network endpoint on one local processor after topology changes; its declared network membership was restored before further load verification. These environment failures are not counted as passing runs.
