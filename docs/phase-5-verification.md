# Backend proof

Date: 18 November 2026 (Asia/Kolkata).

Local correctness and chaos checks passed. The load test was measured honestly: the requested 2,000–5,000 connection target was not reached. The GitHub Actions workflow is included in this phase; remote results are verified from its actual run before frontend work begins.

| Scenario | Issued / stored / received | Duplicates / lost / order violations | Evidence |
| --- | --- | --- | --- |
| Gateway killed during conversation | 120 / 120 / 120 | 0 / 0 / 0 | Reconnected and recovered on the replicated stack |
| Owning processor killed during a queued batch | 120 / 120 / 120 | 0 / 0 / 0 | 41 persisted at kill; 79 remained queued |
| Redis restart and controlled counter loss | 125 / 125 / 125 | 0 / 0 / 0 | Counter/map deleted for the test chat; reseeded through sequence 125 |
| Kafka paused for eight seconds | 120 / 120 / 120 | 0 / 0 / 0 | Broker resumed and processing completed |

All four scripts ended in PASS and restored affected services. Their raw summaries are committed under `chaos-results/`. The baseline and failed stress results, including unconfirmed/absent sends, are in `benchmarks.md` and `load-results/`.

The final local backend source checks passed TypeScript, ESLint, Prettier, and 60 unit tests. The ten real integration cases and eleven infrastructure/monitoring smoke checks passed on the preceding unchanged backend runtime. Phase 5 adds verification tooling and documentation without modifying that runtime.

CI has separate backend and frontend jobs. The backend job runs all source checks, integration/smoke, and the four chaos scenarios on a clean runner. The frontend job explicitly reports the backend-first phase boundary until its independent lockfile exists, then runs its own lint, types, unit tests, and production build. A skipped pre-Phase-6 application is not described as a tested frontend.
