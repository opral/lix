# Partial replica with on-demand sync: performance evidence

The public opener with storage and `server.mode: "partial_replica"` creates a **partial replica with
on-demand sync**. Optimized Chromium/WASM/OPFS measurements below show bounded
opening transfers across independently varied rows, branches, history and
unopened content: two repository requests and approximately 2.3 KiB. Total
fresh-browser opening is typically about 280 ms in this environment, including
worker/WASM/OPFS startup; the approximately 40 ms request-to-open phase is only a
part of that total. These measurements do not reproduce the user's original
23.5-second deployment baseline and should not be presented as a matched speedup.

Covered reads and prepared writes pass disconnected browser tests, including
OPFS reopen. Native, adapter, SDK and browser verification passes. See
[migration support](partial-replica-migration.md) for explicit conversion and its
remaining boundaries. Format and protocol changes are intentional, without an
eager fallback or compatibility shim.

## Final verification (build 75 / release 5)

Three independent fresh authority/browser processes per opening arm, with
alternating small/large order. All eight sequential workflows passed; source,
installed SDK JavaScript/generated glue/WASM and native authority artifacts were
verified unchanged throughout the run.

| Repository | Fresh-browser opening samples (ms) | Median | Requests | Repository body bytes |
|---|---|---:|---:|---|
| 16 KV rows | 277.0, 276.4, 274.2 | 276.4 ms | 2 each | 2,327–2,330 |
| Same rows plus 320 MiB unrelated content | 272.2, 292.4, 266.2 | 272.2 ms | 2 each | 2,311–2,324 |

Every large fixture contains **335,547,662 physical CAS bytes in 286 chunks**.
All six opening runs pass 30 disconnected read/write pairs and offline OPFS
reopen with **zero native input-fetch attempts**. Total opening includes
worker/WASM/OPFS startup. The request-to-open residual medians are 38.3/38.8 ms;
they are not total opening or pure network time. Repository bytes exclude shared
SDK resources, HTTP framing and the separate remote mutation session. The
1,600-file case within the file workflow reuses its browser startup and must not
be substituted for these fresh-browser opening measurements.

Paired local SQL, using identical artifacts/providers/statements, five warmups
and 30 measurements per case:

| Rows | Complete SELECT | Partial SELECT | Complete UPDATE | Partial UPDATE |
|---|---:|---:|---:|---:|
| 16 | 0.8 ms | 1.0 ms | 6.65 ms | 8.6 ms |
| 16,000 | 0.7 ms | 0.9 ms | 8.65 ms | 11.6 ms |

These are paired-workflow medians; the independently seeded opening arms have
approximately 1.1 ms warm point-read medians. The file workflow's 1,600-file
case measures 1,124.8 ms for cold directory loading and **1,200.3 ms for remote
insertion publication**. The earlier instrumented release 2 publication took
15,462 ms; these individual diagnostic runs do not establish a population
speedup. Bounded sibling batching and reuse of complete immutable projections
remove repeated candidate work without weakening native validation.

After public SQL preparation, 30 disconnected 96 KiB content edits, reads and
counts pass with zero native input-fetch attempts. Warm content read/write
medians are **4.7/19.75 ms**; offline OPFS reopen is 49.2 ms. Preparation itself
takes 56.5 ms and leaves content unpublished. Changed dependencies can make a
later operation cold; these figures do not promise that any SQL after one query
is warm.

| Verification | Result |
|---|---|
| Native, all simulations and server protocol, default stack/compiler limits | 3,960 passed; 80 intentionally skipped |
| GC race regression, both simulations × 20 stress iterations | 40 passed |
| Filesystem, RocksDB and SlateDB adapters | 137 passed; none skipped |
| Rust doctests | 10 passed |
| Focused SDK lifecycle/worker/client tests | 61 passed |
| Final Chromium/OPFS tests | 54 passed |
| Native SDK and optimized WASM SDK builds; TypeScript build/typecheck | Passed |
| Native JavaScript preparation/read-your-writes smoke check | Passed |
| Final browser profiling workflows | 8 passed |

Independent sub-agent reviews covered conversion future safety, worker/input
boundaries, tree batching, GC test coordination, profiling methodology and final
raw results/provenance. The native consumer tests compile without increasing
their recursion limits. The GC fixture replans only on genuine optimistic
precondition conflicts; production GC already handles those conflicts.

WASM SHA256: `e3ffe99c8c36887c1a329b77ffcd3c7f9ab5c93e824baafdba5f737c2f207326`
(47,217,605 raw bytes). Native authority SHA256:
`895789aca7b52f707c36202318feea89e6f156f75c6d5398b942e7638914d9a3`.
Raw results, source/artifact manifests and verification logs are retained in
`research/lazy-browser-sync/release-followup-5` in the parent workspace.

Reproduce with `packages/storage-opfs/tests/run-partial-release-profile.py`
using an explicitly selected current native test binary after building the
optimized browser SDK. The invocation below also applies to this final run.
The authority is already open before timing starts. Three samples per arm do
not establish tail latency, strict O(1) backend latency or a matched speedup over
the reported 23.5-second deployment. Explicit old-repository migration remains
separate; its supported cases and recovery boundaries are documented in
[the migration guide](partial-replica-migration.md).

## Earlier measurements

The following sections preserve historical results. Their statements about work
remaining describe those earlier builds; the final verification above is the
current status.

## Repeated optimized opening verification (build 57 / release 2)

WASM SHA256: `6b0e87ad60c365c322b729785450b82143350f60d08b592d78fd44ab3d632ef3`.
Raw module size: 47,036,224 bytes. Repository transfer counts exclude this shared
SDK/WASM resource, HTTP framing and the separate remote mutation session.

Three independent fresh authority/browser processes per arm, with alternating
small/large order and a fixed source/artifact manifest:

| Repository | Total opening samples (ms) | Median | Foreground requests | Repository bytes |
|---|---|---:|---:|---|
| 16 KV rows | 270.5, 277.5, 288.4 | 277.5 ms | 2 each | 2,319–2,323 |
| 16 KV rows plus 320 MiB unrelated files | 271.3, 270.9, 290.0 | 271.3 ms | 2 each | 2,324–2,338 |

The large fixture verifies **335,547,662 physical CAS bytes in 286 chunks** before
opening is timed; it is not a repeated-content logical-size fixture. All six
runs pass 30 disconnected read/write pairs and offline OPFS reopen with zero
native input-fetch attempts. Request-to-open medians are 38.5 ms and 37.6 ms;
total opening includes worker/WASM/OPFS initialization.

These samples establish bounded transfer and no repository-wide opening work
in this setup, not a statistical tail guarantee or strict O(1) storage latency.
The authority is already open before the browser starts. Native indexed point
lookups, backend/cache state, network latency and browser startup can vary.

Paired local SQL runs use the same artifact/provider/statements, five warmups
and 30 samples per case, with reversed mode order at the second size:

| Rows | Complete SELECT median | Partial SELECT median | Complete UPDATE median | Partial UPDATE median |
|---|---:|---:|---:|---:|
| 16 | 0.9 ms | 1.0 ms | 7.0 ms | 8.4 ms |
| 16,000 | 0.7 ms | 0.9 ms | 8.85 ms | 10.65 ms |

The real-file workflow passes SQL preparation without publication, 30 offline
96 KiB content edits/reads/counts, and reopen. At 1,600 files, warm content reads
have a 4.55 ms median and writes 19.55 ms. This release exposed a background
publication bottleneck: a remote insertion into retained negative/directory scopes
took 17.81 seconds and issued 137 native range requests. The batch endpoint
was never used by that phase. The release 4 follow-up below addresses this
bottleneck. Cold directory
loading is 1.30 seconds. Preparation uses the public `Lix.prepare` API and is not
directly comparable to earlier fixtures that performed an actual dummy write.

Native verification: 3,496 tests passed, 78 ignored, normal stack, no exclusions.
The SDK TypeScript build/typecheck and 61 focused lifecycle/worker tests pass.
Broader adapter/integration verification remains. The later build 59 unit run
passes all 3,940 simulation tests, with 78 ignored and no exclusions.

Reproduce after building optimized WASM/SDK and the native authority test binary:

```sh
python3 packages/storage-opfs/tests/run-partial-release-profile.py \
  --native-binary /absolute/path/to/lix-test-binary \
  --output /absolute/path/to/new-profile-directory
```

The runner preserves raw measurements, physical-size manifests, artifact hashes
and tracked/untracked source hashes. It rejects source/artifact changes during
the run. The recorded run is in `research/lazy-browser-sync/release-followup-2`
in the parent workspace.

## Follow-up publication optimization (build 59 / release 4)

The instrumented file workflow was repeated after two reviewed changes:
completed immutable-root projection reuse across candidate retries, and bounded
sibling-demand aggregation in exact-key and unlimited range traversals. The
latter requests at most 32 already-required missing tree objects; limited reads
retain sequential traversal and corruption remains an error.

| 1,600-file workflow | Before | Immutable cache reuse | Cache reuse + sibling batches |
|---|---:|---:|---:|
| Remote insertion publication | 15,462 ms | 11,949 ms | 1,331 ms |
| Cold directory listing | 1,349 ms | 1,419 ms | 1,045 ms |

These are individual diagnostic runs with newly seeded repositories, not a
population latency guarantee. Request timestamps in the baseline measured only
394 ms spent in HTTP and 14,788 ms between requests. Reducing repeated candidate
work, rather than transport bandwidth, addresses that measured bottleneck.

The final diagnostic retains two opening requests (2,329 bytes at 1,600 files),
zero offline native input-fetch attempts, SQL preparation without publication,
30 content edits/reads/counts and offline reopen. Warm 96 KiB content read/write
medians are 4.8/19.25 ms. Raw artifacts are in
`research/lazy-browser-sync/file-demand-diagnostic-release4` in the parent
workspace. Broad native simulation/adapter checks remain in progress; these
measurements do not imply that the entire release verification is complete.

The following sections retain historical diagnostic results and their original
limitations; the latest evidence above supersedes their implementation status.

## Existing sync baseline

One native filesystem replica sample per case, with an in-process Memory authority. HTTP counters include background activity and count emitted response-body bytes, excluding headers. Allocation counts are cumulative allocation traffic, not resident memory.

| Independent variation | Opening | HTTP body bytes | Attempted requests |
|---|---:|---:|---:|
| Baseline: 16 rows, 2 history updates, 1 KiB content | 40.34 ms | 161,051 | 9 |
| 1,600 rows | 730.85 ms | 3,392,966 | 114 |
| 16 additional branches | 186.07 ms | 800,795 | 27 |
| 200 history updates | 32.56 ms | 161,057 | 9 |
| Unopened 1 MiB content | 43.77 ms | 1,208,342 | 11 |

The original eager opener scales with rows, branches and unopened content. The row case allocated 829 MB cumulatively versus 19 MB at baseline. Warm point reads took 66–107 microseconds and point writes 0.96–1.67 milliseconds. These are single-sample diagnostic timings; they are not browser measurements or foreground network attribution.

Reproduce through the ignored `partial_replica_open_profile` test in `packages/e2e/tests/sync_mode.rs`, using the `tooling/Cargo.toml` workspace and `sdk-tests,server-protocol` features. The harness emits JSON with dimensions and operation counters. All 17 existing nonignored sync-mode regressions passed after adding the instrumentation.

## Immutable native tree feasibility

The prepared existing-key mutation test physically copies only the required tree frontier into separate Memory, performs 12 updates with fresh tree caches, and compares every resulting root with an independent canonical rebuild. The untouched subtrees remain absent locally. A verifier restores the untouched authority bytes afterward and checks all final rows.

| Native rows | Loaded / total nodes | Loaded / total bytes | Local update p50 / max |
|---|---:|---:|---:|
| 1,000 | 4 / 9 | 6,744 / 23,880 | 211 / 235 µs |
| 100,000 | 9 / 883 | 18,861 / 2,393,124 | 648 / 709 µs |

The timings exclude canonical verification. This establishes that the immutable tree can retain untouched state by reference. It does not prove SQL validation, commit upload, background reconciliation, checkpointing or garbage collection on a partial replica.

## Descriptor input profile

Three fresh uncached adapter reads per fixture and branch-selection mode. The following shows default selection medians; explicit selection also performed zero scans in every fixture. Fixture construction, adapter epoch setup, HTTP and engine/browser opening are excluded.

| Independent variation | Median descriptor read | Adapter calls | Input bytes | JSON bytes | Scans |
|---|---:|---:|---:|---:|---:|
| Baseline | 175 µs | 10 | 1,595 | 1,536 | 0 |
| 1,600 rows | 111 µs | 10 | 1,597 | 1,548 | 0 |
| 16,000 rows | 114 µs | 10 | 1,597 | 1,531 | 0 |
| 128 additional branches | 163 µs | 11 | 2,004 | 1,545 | 0 |
| 256 history updates | 113 µs | 10 | 1,597 | 1,542 | 0 |
| 128 additional schemas | 148 µs | 11 | 2,004 | 1,530 | 0 |

These results support the bounded descriptor primitive. They are not end-to-end opening measurements. Reproduce with the ignored `partial_replica_descriptor_large_profile` engine test.

## SQL dependency feasibility

The native dependency diagnostic passes at 128 and 4,096 rows for ordinary and root-backed branches. Each case copies the prepared native inputs into separate Memory, performs three SQL edits with immediate reads, closes/reopens locally, exports only the three authored commits and uploads them through the ordinary account-checked authority path. The authority checks all final rows. A prepared Myers jump spine keeps subsequent commit appends from demanding unrelated parent history. These tests are dependency proofs, not the production hydration/runtime path.

The isolated checkpoint also passes after preparing its declared parent topology header where absent (one 426-byte object in the ordinary cases). Observed checkpoint times were 1.58–2.82 ms across the four cases. This is still the dependency diagnostic, not production partial-sync admission. Garbage collection reaches missing retained ancestry or mutation payloads; the actual partial-replica mode now rejects the authority collector before staging deletions. A cache ownership collector and background reconciliation remain release gates.

## Descriptor-only HTTP opening

The private partial opener was measured through the canonical server handler over native TCP HTTP. Each case uses five fresh durable-Memory clients. Fixture construction and authority startup are excluded. These are debug native timings, not browser, OPFS, IndexedDB or filesystem timings. The anonymous trusted-host handshake does not measure first-time authenticated account creation.

| Independent variation | Open median / p95 | Response body | Requests | Offline reopen median |
|---|---:|---:|---:|---:|
| 16 rows | 1.995 / 3.281 ms | 2,034 B | 2 | 559 µs |
| 1,600 rows | 1.999 / 3.030 ms | 2,034 B | 2 | 585 µs |
| 16,000 rows | 1.683 / 2.598 ms | 2,032 B | 2 | 514 µs |
| 128 extra branches | 1.526 / 2.713 ms | 2,027 B | 2 | 451 µs |
| 256 history commits | 1.627 / 2.444 ms | 2,044 B | 2 | 479 µs |
| Unopened 8 MiB content | 1.524 / 2.371 ms | 2,035 B | 2 | 448 µs |
| Unopened 32 MiB content | 1.588 / 2.579 ms | 2,020 B | 2 | 460 µs |

Every open fetched zero native objects, metadata records or blob payloads before SQL. Offline reopen made zero requests with the authority stopped. Reproduce with `partial_handle_http_opening_profile`. This establishes the private native opener's bounded behavior; the public sync builder still uses eager sync.

## Actual partial SQL admission

The descriptor-only SQL gate now passes at 16 and 1,600 rows. It installs only the descriptor/bootstrap, hydrates through typed native demands, prepares a real first UPDATE and the baseline jump spine, then performs repeated direct offline SQL updates/reads and closes/reopens locally. Stale epoch writes fail without publication. No traced HOT rows or arbitrary-key copy is used.

The file-content gate also passes: one 96 KiB file hydrates through a manifest and chunk, an unrelated 1 MiB file remains absent, and repeated content edits/reads plus reopen remain local. Cold read native payload was 11.6–14.2 KB plus the requested 96 KiB chunk. Initial content-write preparation fetched 2–7 native objects and no further blobs. Timings exclude browser execution.

A controlled debug build with 30 warm iterations and test-only root-exact instrumentation found:

| Rows | Partial UPDATE median | Complete UPDATE median | Partial SELECT median | Complete SELECT median |
|---|---:|---:|---:|---:|
| 16 | 3.972 ms | 1.642 ms | 1.396 ms | 285 µs |
| 1,600 | 4.408 ms | 2.031 ms | 1.605 ms | 258 µs |

Both variants use the same SQL and binary. UPDATE and SELECT are timed separately. Root-exact work accounted for an inclusive average 190–221 µs per partial UPDATE and 338–496 µs per partial SELECT, returning 2.7–4.9 KB and 3.9–14.5 KB from local storage respectively. Actual adapter get elapsed time was only 6–12 µs per operation. Repeated native decoding explains part of the cost; it does not explain the entire write gap. After attaching the immutable cache to the actual local/global exact reader paths and adding the local catalog invalidation token, build28 reduced partial SELECT medians to **370 / 341 µs** and UPDATE medians to **2.957 / 3.081 ms** (16 / 1,600 rows). Same-binary complete SELECT medians were 290 / 241 µs and UPDATE 1.666 / 1.895 ms. Warm partial SELECT now makes zero native-object reads: only two mutable control reads, totaling 42 returned bytes per operation. The gate asserts reduced native reads so unused cache wiring cannot silently pass again. Instrumentation measures elapsed time and returned local bytes, not CPU time, network bytes or RSS.

## Release gates

Build28 passed 3,373 nonignored engine tests. Four failures were a stale bootstrap guard count, two module/fixture structure checks, and selected-checkpoint rebase correctness. The first three are corrected; the checkpoint correction is under validation. The real partial upload worker passes foreground-demand preemption after ambiguous authority acceptance. Exact-target retry preserves newer local edits, then publishes those next. The file gate now uploads all four locally edited content versions through verified canonical blob registration/chunks, preserves local controls on acknowledgment, and reopens offline. Clean full-replica conversion passes, while pending/recovery data blocks conversion without discarding the source. These are native tests, not browser measurements.

Measure descriptor reads and bytes independently from fixture construction. Require zero authority scans and bounded response size as rows, branches, history and schemas grow. Ordinary index traversal may grow with tree height.

Complete the SQL workload: cold preparation, disconnected repeated edits and reads, reopen, ordinary authority acceptance, background changes, checkpointing and garbage collection. Loaded scopes must remain locally usable after acknowledgments. Attribute foreground demands separately from background traffic. Durable read-interest restore/union and warm zero-write flush tests pass. Candidate preparation/publication, pending-write reconciliation, retention lease wire integration/GC, and conversion of repositories with pending edits remain implementation gates. Native scope preparation and lease ownership are being integrated; they are not yet a complete background sync protocol.

Finally run the browser workload with repository sizes varied independently and fixed first-page SQL output. Report opening separately from first-query loading, UI rendering and one-time migration. The feature is not publicly activated yet; a faster native opener alone does not satisfy the browser goal.

## Current implementation gates

Build30 passed the actual full and selected key/value checkpoint lifecycle tests, including native upload, authority acceptance, own acknowledgment, and offline reopen. Clean full-replica conversion also passed. Production compilation passed after exposing the HOT owner's generation-prefix helper to production code.

The leased descriptor protocol and atomic remote working-set publication are being integrated and are not covered by the earlier opening measurements. Rerun the HTTP opening profile after that integration: the required baseline lease adds bounded server writes and response metadata. Public browser opening still uses the existing full-sync path; browser/OPFS measurements remain a release gate.

## Leased protocol reprofile (build35)

The required baseline lease is now included in the actual native HTTP opening path. Five fresh durable-Memory clients per fixture used the canonical HTTP handler over TCP. Each opening made exactly **two HTTP requests**, transferred **2,319–2,363 response-body bytes**, and fetched no native objects, metadata bodies or content chunks before SQL. Offline reopening used no configured authority and asserted zero additional HTTP requests; the fixture server remained available until all five samples finished.

| Independent fixture | Median open | Median offline reopen |
|---|---:|---:|
| 16 rows | 2.398 ms | 0.625 ms |
| 1,600 rows | 2.245 ms | 0.646 ms |
| 16,000 rows | 1.830 ms | 0.499 ms |
| 128 extra branches | 1.877 ms | 0.501 ms |
| 256 history updates | 1.820 ms | 0.494 ms |
| 8 MiB unopened content | 1.866 ms | 0.504 ms |
| 32 MiB unopened content | 1.805 ms | 0.538 ms |

Warm native SQL was measured separately, without compilation or another test process competing for CPU. Across 30 operations per fixture, partial point SELECT medians were 0.372 ms (16 rows) and 0.368 ms (1,600 rows); UPDATE medians were 3.009 ms and 3.383 ms. Each warm SELECT again made **zero native-object reads**, only two mutable control reads totaling 42 bytes. These are native debug measurements, not browser/OPFS timings, and the opening profile predates attaching the descriptor watcher. First-time authenticated-account creation is not represented by the anonymous host fixture.

Raw artifacts: `/root/repos/research/lazy-browser-sync/partial-http-opening-profile-leased.json` and `/root/repos/research/lazy-browser-sync/partial-warm-sql-profile-leased.json`. Logs: `/tmp/partial-replica-http-profile2.log` and `/tmp/partial-replica-warm-profile4.log`.

Build35 passed the direct offline UPDATE-after-remote-publication gate and negative file-content capture gate. Its full native suite passed 3,393 tests, with one failure in the small-tree retained-fraction performance assertion; that assertion is being replaced with a tree-height bound plus physically absent subtree and canonical-update checks. Background watcher integration and browser validation remain outstanding.

## Live watcher and native write preparation (build37)

Build37 passed 3,403 native tests, including live background insertion into a retained negative SQL scope, cancellation after durable storage acceptance, ambiguous commit poisoning, deadline expiry while waiting on a transaction, and offline prepared writes after remote publication. One mock HTTP fixture rejected the new descriptor-watch query URL; its routing and foreground/background counters are corrected in source, awaiting recompilation.

The canonical HTTP profiler now runs concurrent handlers so an active long poll cannot block another connection. It records arrivals before handling, separates foreground requests from background requests, and stops the authority before the fifth offline-reopen sample. With the production watcher enabled, all 35 opens still made two foreground requests, totaling 2,316–2,368 response-body bytes, with no background request arriving before open returned in this run. Median native open time was 3.397–4.082 ms across the seven repository dimensions. Per-connection thread/runtime startup is included in this revised harness, so compare the earlier sequential-server times cautiously. The profile is still anonymous, native debug, durable Memory; it does not measure OPFS/browser startup.

Actual native tree preparation loaded 5/9 nodes (10,399 bytes) for 1,000 rows and 11/883 nodes (22,129 bytes) for 100,000 rows. Twelve existing-value edits then ran with the remaining nodes physically absent and verified canonical roots/full authority rows. Median native tree mutation time was 268/651 µs respectively; these are tree operations, not end-to-end SQL latencies. Preparation is bounded by tree height, not strictly constant for an arbitrary point mutation.

Artifacts: `/root/repos/research/lazy-browser-sync/partial-http-opening-profile-watcher.json`, `/tmp/partial-replica-http-profile3.log`, `/tmp/partial-replica-tree-proof37.log`, `/tmp/partial-replica-base37.log`. The initial authenticated-account counter profile failed fixture authority admission because it supplied no mutation revision; that fixture is corrected, with no account-performance result claimed yet. Clean expired-baseline recovery is now implemented in source and awaits compilation/tests.

Build38 passes all 3,412 native tests (76 ignored), including clean baseline recovery and the initial conflict-preserving native key/value merge analysis. The browser-target JS SDK check also passes after removing an unnecessary native-only Send bound from migration finalization.

Authenticated account admission profiling found a remaining size dependency: existing account validation consistently uses six point keys, 427–428 returned bytes, zero scans and zero writes (125–169 µs in this native debug run). First-time creation is bounded as selected rows/account counts increase, but scans global key/value rows: 1,642 scanned rows/325,952 bytes at1,600 global rows, and16,042 rows/2,792,266 bytes at16,000 (5.932/32.227 ms). This is an unresolved opening dependency, not an accepted constant-work claim. Source: `/root/repos/research/lazy-browser-sync/partial-account-admission-profile.json`, `/tmp/partial-replica-account-profile2.log`.

## Bounded deterministic-setting proof and browser ownership (build40)

Format79 replaces the routine missing-setting collection scan with a mandatory, generation-bound presence proof. Explicit migration validates the old collection closure and backfills that proof; regular reads fail closed if it is missing or inconsistent. First-account admission now scans exactly40 system rows at all tested widths. With16,000 unrelated global key/value rows, returned local bytes fell from2,792,266 to51,178 and elapsed native-debug time from32.227 to3.862ms. At16/1,600 global rows the new measurements were34,728/53,264 bytes and3.496/3.337ms; native index traversal still grows with tree height. Existing-account admission remains six point reads,427–428 bytes, no scans or writes. These are server admission measurements, excluding HTTP and browser startup.

Artifact: `/root/repos/research/lazy-browser-sync/partial-account-admission-profile-witness.json`; log: `/tmp/partial-replica-account-profile3.log`.

Build40 compiled successfully and passed3,412 native tests;20 failed. Sixteen failures share new registry entries being out of physical-key order, including snapshot export; the registry ordering is corrected in source. The other failures were one merge-test JSON string expectation, one old scan-count expectation, and two migration regressions (including using an ordinary authority-fenced write instead of the migration-owned write path). Those corrections await build41 validation. The subsequent WASM target check passes with browser ownership forwarding and native merge candidate settlement.

Six actual Chromium ownership tests pass, covering a competing owner, cancellation before and after grant, UTF8-equivalent OPFS filename aliases, lifetime-controlled shutdown, and real OPFS provider reads while shutdown waits for an outstanding owner. SDK TypeScript build and OPFS typecheck pass. These tests establish ownership behavior, not repository opening performance. The public opening path is still awaiting activation; real WASM/OPFS SQL profiling remains outstanding.


## First-account schema scaling diagnostic (build43)

The witness fix removed unrelated global-row enumeration but a separate custom
schema fixture exposed full catalog preparation during account creation. At
16/256/1,600 custom global schemas, admission scanned88/808/4,840 rows, returned
47,160/249,393/1,354,442 local bytes, and took6.779/26.715/135.698ms. Existing
account admission remained six point keys and zero scans. Fixture construction
was excluded; this was a native debug run without another test or compiler
competing for CPU.

Artifact: `/root/repos/research/lazy-browser-sync/partial-account-admission-profile-schemas.json`;
log: `/tmp/partial-replica-account-profile4.log`. A sealed built-in account
insertion path is now under validation. No improved schema-scaling result is
claimed until the new path passes correctness checks and profiling.


## Chromium / WASM / OPFS functional gate

The public SQL worker passed all five fixtures with actual Chromium and OPFS.
Each open made exactly two repository requests and consumed 2,318–2,355 decoded
HTTP body bytes. Across the fixtures, 150 offline SELECT/UPDATE pairs retained
read-your-writes and offline OPFS reopen preserved pending edits. The harness
aborts in-flight responses before offline operations and rejects every native
input fetch during those prepared operations, including background fetches.

These are **unoptimized development-build diagnostics**, not production latency
results. The first fixture includes cold worker/WASM startup; later fixtures
reuse browser code/module caches. Do not interpret the faster large-repository
fixture as a size-related improvement. Repository HTTP counts exclude the SDK
and WASM download. The native authority was build46; the WASM artifact predates
the pending-full-conversion integration.

| Fixture | Open ms | Repository body bytes | Cold SQL ms | Warm SELECT median ms | Warm UPDATE median ms | Offline reopen ms |
|---|---:|---:|---:|---:|---:|---:|
| rows_16 | 876.1 | 2,318 | 233.9 | 2.50 | 25.90 | 7.8 |
| rows_16000 | 32.4 | 2,331 | 147.0 | 2.10 | 24.65 | 14.9 |
| branches_128 | 32.6 | 2,355 | 138.3 | 1.90 | 20.50 | 12.9 |
| history_256 | 31.1 | 2,323 | 102.8 | 1.90 | 19.85 | 7.7 |
| blob_8mib | 32.0 | 2,327 | 112.0 | 1.80 | 19.00 | 7.4 |

Artifact: `/root/repos/research/lazy-browser-sync/partial-browser-opfs-dev-functional.json`.
The separate real Lix ownership lifecycle test also passed in Chromium.

The first public file-tree run exposed a stale live path-index cache following
remote publication: native hydration succeeded and the watcher advanced, but a
previously empty path stayed empty locally. Atomic filesystem revision rotation
and a native regression are under validation. This is an outstanding functional
gate; key/value success does not establish complete file-tree correctness.


## Bounded account preparation profile (build48)

The sealed account INSERT now avoids custom catalog/plugin enumeration. At
16/256/1,600 global custom schemas, first-account admission returned
9,226/15,326/16,110 local bytes and took2.626/3.807/3.978ms, versus1,354,442bytes
and135.698ms at1,600schemas before this fix. First-account cases scan one row
in this authority fixture at all measured selected-row/global-row/account/schema
widths; a branch-width fixture and per-space scan attribution remain to verify
the last scan does not conceal branch enumeration. Ordinary indexed lookup
work and returned bytes can vary with native tree height.

Across the measured dimensions, existing-account admission remains six point
keys,427–428bytes, no scans/writes,125–198µs. Fixture construction was excluded
and no compiler or other test workload ran concurrently with the profile.
These are native debug authority timings, not HTTP or browser timings.

Artifact: `/root/repos/research/lazy-browser-sync/partial-account-admission-profile-bounded.json`;
log: `/tmp/partial-replica-account-profile5.log`. The corresponding custom-schema
visibility test needed correction: global-only schemas must be queried through
a global session, as required by the existing catalog contract. Stronger tests
now check both global and branch-local schemas before/after account creation
and after reopen.


## Optimized Chromium / WASM / OPFS measurements

The release WASM artifact is 44 MiB raw, SHA-256
`def0388469972fb29c8f955cce552cca11b6c0803795b156ddab00b4c41faf5d`.
Fifteen samples used a fresh Chromium process for every opening, three independent
repositories per dimension, with no concurrent compilation or test workload.
Each opening made two repository requests and consumed 2,310–2,370 decoded
response bytes. SDK/WASM download and HTTP framing are excluded from that byte
count. All 450 offline SELECT/UPDATE pairs and offline OPFS reopens passed with
zero native input read attempts.

| Dimension | Total open min / median / max ms | First authority request to open median ms | Warm SELECT median ms | Warm UPDATE median ms |
|---|---:|---:|---:|---:|
| 16 rows | 277.6 / 478.0 / 485.3 | 40.1 | 1.1 | 14.7 |
| 16,000 rows | 277.4 / 281.7 / 288.5 | 39.8 | 1.1 | 16.6 |
| 128 branches | 285.1 / 287.7 / 678.4 | 39.4 | 1.2 | 15.85 |
| 256 history commits | 274.8 / 277.2 / 513.5 | 38.5 | 1.3 | 15.55 |
| Unrelated 8 MiB blob | 277.6 / 280.9 / 290.5 | 41.0 | 1.2 | 15.65 |

The approximately 40 ms interval excludes preceding worker, WASM and OPFS
initialization; it is not total browser opening time. Three openings per
dimension support descriptive ranges, not a reliable tail-latency estimate.
Artifact: `/root/repos/research/lazy-browser-sync/partial-browser-opfs-release-profile.json`.

The optimized file workflow also passed remote insertion into a previously
empty path, local counts, 30 offline 96 KiB content edits and offline reopening
at 16 and 1,600 files. Atomic path revision rotation fixed the stale-path gate
reported above. Opening consumed 2,331 / 2,327 repository bytes. This run reused
one browser process, so its 276.9 / 30.8 ms opening times are not comparable
cold-start samples.

File profiling identified two follow-ups. The reported warm content read timer
included Vitest deep byte-array equality; those figures are invalid as SQL-only
latencies and require a corrected rerun. Remote publication took 266 / 16,930 ms,
with 11 / 129 native object-range requests. Repeated candidate preparation and
serial hydration need optimization. The functional pass alone does not establish
acceptable update latency. Artifact:
`/root/repos/research/lazy-browser-sync/partial-browser-file-opfs-release2.json`.


### Corrected file timings and paired local comparison

The corrected file timer stops immediately after SQL resolves, before deep
byte-array validation. The release artifact remains unchanged. At 16 / 1,600
files, warm 96 KiB SELECT medians were 7.90 / 7.25 ms; content UPDATE medians
18.70 / 20.65 ms; COUNT medians 2.00 / 3.50 ms. Both workflows passed remote
negative-path publication, 30 offline edits, and offline reopen with zero input
read attempts. Publication remained slow at 266 / 15,413 ms before native
missing-object batching. Artifact:
`/root/repos/research/lazy-browser-sync/partial-browser-file-opfs-release3.json`.

A separate same-artifact, same-provider comparison used identical SQL, keys,
row counts and payloads, five excluded warmups, and 30 measured pairs per mode.
Seeding was excluded; mode order reversed at the second scale. No compiler or
other test workload ran concurrently.

| Rows | Complete SELECT / UPDATE median ms | Partial SELECT / UPDATE median ms |
|---|---:|---:|
| 16 | 0.90 / 6.95 | 1.00 / 12.95 |
| 16,000 | 0.70 / 9.00 | 0.90 / 14.70 |

The approximately 6 ms partial write overhead is measurable and still needs
attribution; it cannot be described as OPFS durability cost alone. All measured
partial operations ran offline with zero native input reads. Artifact:
`/root/repos/research/lazy-browser-sync/partial-browser-local-comparison-release1.json`.
