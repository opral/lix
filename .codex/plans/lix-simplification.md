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

Completed #1743: [explicit internal sync import variants](lix-sync-import-modes.md), reviewed by a sub-agent and merged while draft. This follow-up preserves public APIs, wire/storage formats and shared admission/publication logic. Ref repair remains supported without a receipt. Final validation passed all 3,728 engine tests (69 skipped) and 10 doctests, including the new active repair/CAS/receipt/cursor/upload-plan regression. CI/CD remains skipped; integration remains draft.

Completed #1744: [shared plugin cold-file transition pipeline](lix-plugin-transition-pipeline.md), reviewed independently for correctness and performance and merged while draft. Fresh imports and ownership reselection share guest validation, host create-row/accounting and checkpoint/publication; bounded parallel scheduling, sparse transitions and publication ordering remain intact. Validation passed 3,728 engine tests (69 skipped), 10 doctests and nine real-plugin regressions. Three alternating baseline/candidate benchmark pairs across 12 workloads passed existing gates; measured median elapsed ratios were 0.955–1.022, maximum allocated bytes 1.055 and peak live bytes 1.050. CI/CD remains skipped and integration remains draft.

Completed #1745: [shared tracked-head proof and publication](lix-tracked-head-publication.md), merged while draft. Prepared rows and ordered journals share ancestry certification while preserving optional versus mandatory proof and parent-bound seeds; three packed materialization routes share epoch/control publication while retaining optimized writers. Validation passed 3,729 engine tests (69 skipped), 10 doctests and four durable/columnar regressions. Seven alternating process pairs on four optimized-path tests measured median wall-time ratios 0.989–1.007 and peak RSS 0.995–0.998. CI/CD remains skipped; integration remains draft.
