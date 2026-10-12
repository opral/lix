# OPFS coherent read progress decision

Status: storage and engine/browser progress qualification passed; broader migration and release gates remain separate from this decision.

## Budgets declared before measurement (2026-09-14)

The storage qualification uses actual Chromium dedicated-worker SQLite/OPFS. A coherent operation scans 1,000 128-byte values in 20 pages while another operation commits a change between every page. Repeat 20 operations. Required: all reads return their opening values without restarting; each completes within 1,000 ms; each interfering commit completes within 100 ms. No write lock may span an asynchronous read attempt. Additional historical data must be bounded by 32 MiB of key/value bytes and 512 committed generations per owner. Reads outside that retention window may expire; the engine retries complete buffered attempts for at most the existing 3-second retry budget and reports a structured progress error on exhaustion. A separate engine/browser opening workload remains required.

Baseline expectation: revocation after every commit cannot complete even two pages under this schedule. This is a deliberately adversarial progress test, not a claim about production throughput.

## Selection

Selected bounded SQLite undo history after baseline and prototype measurements and the engine/browser opening qualification below.

### Engine composition budget declared before measurement

With an already initialized repository, start an independent OPFS provider writer and keep it writing without sleep until a separate SDK engine opens and finishes 12 metadata queries. Opening must finish within 2 seconds, every query within 250 ms, every commit within 100 ms, and at least one commit must finish during opening. Record whether WASM is pinned baseline or freshly built; the former qualifies storage integration only, not the changed native executor.

### Read API bounds

Buffered reads have no deadline and no row budget: callers cancel a read by dropping it (JS: aborting its signal), and the engine yields between batches so the cancel takes effect within a time slice. The exceptions are DataFusion's in-memory sort and window evaluation, which each run as one synchronous step after their input is read: DataFusion's sort merge sorts every run inside its first poll (a multi-threaded Tokio runtime hides that by sorting runs on other threads; a single-threaded executor and the wasm worker run it inline), and the window operators then run over the sorted stream without meeting a plan leaf, where Lix yields. An abort during them waits for the step to finish (measured in review: `ORDER BY` 15 ms natively at 40M rows and about 1.2 s on the single-threaded wasm worker at 6M rows; a window over a sort 11.7 s natively at 20M rows and 4.7 s on the wasm worker at 3M rows). On the worker that also delays every other request. Key-ordered reads and streams do not enter these phases. Their output is capped at 64 MiB of result memory by default (`max_bytes`, JS `maxBytes`). DataFusion result collection checks every batch before retaining it; native results and aggregate SQL batches are checked before publication. This is a result bound, not a claim that every internal provider allocation is charged. A hydration demand fails only when its transport stops making progress for 60 seconds (`LIX_ERROR_SYNC_TRANSPORT_STALLED`). The storage expiry retry sub-budget remains three seconds.

## Measured decision

Select the bounded SQLite undo history, rather than cross-await writer locks. The original adapter completed zero reads in the declared page/commit schedule: the second page failed with `LIX_STORAGE_READ_EXPIRED`. With history, all 20 reads completed across 400 commits; a measured run had maximum read 18.6 ms, maximum commit 1.2 ms, and retained history of 103,000 accounted bytes. Tests also verify historical inserts/deletes/range rewrites, both scan orders, independent generation values, and expiry at both retention limits.

The final Chromium SDK/OPFS composition completed opening in 26.9 ms and 12 reads with maximum read 20.3 ms, maximum commit 1.6 ms, and 33 commits during opening. The 35-test browser storage/watch/progress suite passed against freshly built normal WASM including the native deadline, result-budget and deferred-publication changes. These are qualification measurements, not production latency promises.

History is an owner-local TEMP table. Each commit records the previous value of changed keys (NULL for prior absence); historical reads choose the earliest retained preimage at or after their generation. Live unchanged keys stay in the main table. Point lookups remain indexed and do not materialize the database. Retention is counted before copying preimages, and overflow expires older reads instead of blocking a writer. Failed SQLite transactions roll back history too. An owner loss still fences all handles. No durable storage-format migration is needed for this transient cache.

Typed native-input demands already separate hydration: a local read returns a structured missing-input error, its scoped handles finish, and only then does `Lix::retry_sync_demands` ask the sync worker to hydrate and open a new attempt. The coherent executor does not retry such demands. Foreground read-interest captures are now published after a successful scope; durable runtime-function statements never enter generic retry or cancellation. Deterministic plugin read rendering may run again after a preparation miss, but mutation execution and emitted results cannot. Existing catalog/preparation caches remain memoization, not user-visible result publication.
