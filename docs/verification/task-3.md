# Task 3: documentation consistency

Done when: current documents identify their evidence set, old phase records live in the verification archive, and relative links pass an automated check.

The 79-queued and 35-queued figures describe different processor crashes: the earlier run persisted 41 of 120 sends before termination; the later run persisted 85. Original JSON summaries remain unchanged. The historical resume bullets now reference the later final results consistently. [Exact records and explanation](../TESTING.md#numeric-evidence).

Phase verification Markdown and JSON, plus both historical chaos result directories, moved under `docs/verification/`. Links were resolved against their original locations and updated after relocation. The README now contains the overview, architecture, quick start, demo, guarantees, proof table, and known limitations. Long command lists moved to [TESTING](../TESTING.md). Historical benchmark figures remain explicitly separate from the new distinct-user runs.

`node tools/check-links.mjs` checks local destinations and Markdown heading anchors across repository Markdown and runs in CI. It ignores external URLs and code examples; it does not claim external availability. Backend/frontend source checks and the unchanged application runtime's integration, smoke, and four chaos gates are recorded with Task 2's verification.
