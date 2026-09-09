# Lix simplification integration

This draft integration branch collects independently reviewed changes before final review against main. Physical-layout migrations remain supported; public API compatibility is not required.

- [x] Retire unused JSON runtime storage, fences and hashing; retain required migration decoding.
- [x] Finish typed INSERT lowering and remove custom JSON recertification.
- [x] Share one bound read access plan across native reads and SQL providers.
- [x] Stage branch lifecycle through typed control intents.
- [x] Remove the redundant TypeScript protocol export.
- [x] Remove unused BranchEquals storage preconditions.
- [x] Fix shared row/mutation contract ownership.
- [x] Centralize transactional plugin publication and cleanup.

Each implementation PR targets this integration branch and starts in draft. Validate and merge each child PR into this branch. The integration PR remains draft until final user review; do not merge it into main automatically.

Required validation includes engine all-simulations and doctests, adapter conformance for storage-contract changes, and targeted SDK, migration, SQL, branch and plugin tests. Preserve native read and batch INSERT fast-path performance.

## Audit follow-ups

Rebased with merge history preserved onto main `50ee05565d897304d0fa144391072ec841f3ed8a`, including plugin observation, GC and replica retained-work recovery fixes. Rebase resolutions retain typed branch intent validation, new recovery publication rules, read-only epoch fences and native row validation.

- [x] #1739: replace current/historical authority classification with execution disposition.
- [x] #1740: share ordinary/coherent read-batch execution and retry loops.
- [x] #1741: share single/batch idempotency preflight, retries and durable recovery.
- [x] #1742: define protocol method/path/body policy once and include merge routes in the inventory/OpenAPI.

All four PRs were created and merged as drafts, using local merge commits with CI-skip markers. The integration PR remains draft and must not merge into main automatically.

Local validation: 3,727 engine tests with all-simulations and server-protocol enabled passed (69 skipped), 10 doctests passed, 108 RocksDB/SlateDB adapter tests passed, and all four plugin file-observation regressions passed. The final engine runs used eight test workers after earlier concurrent runs exposed GC/recovery test races; full reruns passed. CI/CD was intentionally skipped for this rebase and these follow-ups.

Next decision: review [the sync import mode proposal](lix-sync-import-modes.md). No sync-mode refactor is included in these four PRs.
