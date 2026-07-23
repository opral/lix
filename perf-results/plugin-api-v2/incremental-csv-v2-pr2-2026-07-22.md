# Production incremental CSV Wasm plugin v2

Date: 2026-07-22 through 2026-07-23 UTC

PR1: [`opral/lix#697`](https://github.com/opral/lix/pull/697), head
`a39a76eaed008e0c33b5581b6e05ac4c3a5cc01a`

PR2 branch: `codex/incremental-csv-plugin-v2`

Control commit: `65e1b92d154b7a79fb7aa161830a9684880f35bb`

Candidate commit: `abed4ef9b97905649a04b15df52c944bced19b88`

## Result

PR2 makes a warm one-row CSV edit proportional to the affected CSV region and
one semantic row. It does not materialize or render the 220,000-row semantic
state on the measured path.

The production CSV vertical slice passes all preregistered latency cells on
both required backends. The largest one-sided 95% upper candidate/control
ratio is `0.01161` for edit and `0.00822` for exact render, versus required
limits of `0.80` and `1.05`/`1.10`. The candidate also completes the full
10,680,000-byte import, cold open, localized edit, and exact render below the
64 MiB aggregate guest linear-memory ceiling.

This is a latency, aggregate guest-memory, correctness, and authoring-surface
result. It does not close the physical WAL-amplification or quiesced-live-byte
gate; those measurements remain explicitly open below.

The ordinary client contract remains a SQL blob read/write. Plugins remain
Wasm Components. Plugin authors do not receive branch/commit control, CRDT
state, conflict tables, storage KVs, observations, or cache policy.

This result admits only the production CSV slice. TSV has correctness and AX
authoring evidence, but no latency or memory acceptance gate. It does not
freeze the general five-format API. JSON remains on Component v1 and is
reported below as a separate 10,000,000-byte mechanism diagnostic.

## Exact revisions and binaries

The control is a merge of the PR1 head with `origin/main` at `8649dad5`, which
was current when the original immutable A/B campaign began. The final
`abed4ef9` acceptance rerun deliberately reused that same control after live
`main` advanced. The candidate is that exact control plus the PR2
implementation (`6dd5612b`), stale-actor cold-recovery fix (`eea45b96`), and
runtime lifecycle hardening (`abed4ef9`).

| Arm | Commit | SHA-256 |
|---|---|---|
| Control | `65e1b92d154b7a79fb7aa161830a9684880f35bb` | `5da4594050cd431e00d0ad7967d33459af6c1e263eba49d8702f5a8402b7ba99` |
| Candidate | `abed4ef9b97905649a04b15df52c944bced19b88` | `21131d39faeb2776da9eb654dcd39ee564b448820f166fad153d36d9eb61bd1c` |

After the campaign completed, live `main` advanced to `defa42a96a0debd68e9f22afc3fbb47d9b85f3a9`
when [`#698`](https://github.com/opral/lix/pull/698) merged on top of
`8649dad5`. Because that change is performance-relevant, it was not silently
inserted into either measured binary. Rebasing PR2 onto a later PR1/main must
be followed by a fresh paired acceptance run.

Host and toolchain:

- Linux `7.0.0-15-generic`, x86-64;
- 8 vCPU AMD EPYC Milan, 32.85 GB physical memory;
- `rustc 1.97.0-nightly (b954122bb 2026-05-20)`, LLVM 22.1.4;
- `cargo 1.97.0-nightly (4d1f98451 2026-05-15)`;
- Python 3.14.4; and
- `perf 7.0.12`, Inferno flamegraph tools.

## Production architecture

The versioned WIT and packet contract live in
[`packages/engine/wit/v2`](../../packages/engine/wit/v2). The production
reference guest is [`plugins/csv-v2`](../../plugins/csv-v2).

The vertical slice implements:

- one Wasmtime Store and Component instance per branch/file actor, while only
  the compiled Component is shared;
- exact observations keyed by branch, file incarnation, path, owner, plugin
  generation, actor nonce/revision, and semantic root rather than byte hash;
- a workspace-local LRU bound of eight actors, one retained predecessor per
  actor, and serialized cold construction;
- compare-and-replace publication of cold actors, so a slower stale
  reconstruction cannot overwrite a concurrently committed actor and its
  losing Store is retired;
- full actor/Store retirement after trap, timeout, cancellation, or uncertain
  completion, with stale observations failing closed;
- fresh finite fuel and epoch limits before actor instantiation, standalone
  fork/drop calls, and each top-level transition export, with the aggregate
  transition deadline rearmed only after host-side input construction;
- actor retirement when teardown, post-check, or budget-aware resource
  deletion fails, rather than reusing an uncertain Store;
- hidden propagation and validation of remote splice provenance through the
  existing SQL blob write path, plus a bounded byte-diff fallback when the
  sidecar is absent;
- immutable CSV documents backed by one byte blob, chunked compact indexes,
  stable slots, and sparse identity/order/location overlays;
- touched-row reparsing and complete sparse row upserts/deletes, with exact
  cold fallbacks for dialect or uncommon EOF cases;
- streaming initial semantic pages and host validation across all pages for
  limits, duplicates, schema correspondence, and authority;
- an incremental renderer that consumes final merge-resolved changes and
  returns base-relative byte edits;
- precommit validation of the rendered successor, followed by publication of
  that exact result only after durable commit;
- reuse of the validated materialization by LocalFilesystem sync rather than
  a second entity scan/full render;
- cache keys containing branch, incarnation, plugin generation, and expected
  committed root/version, with safe cold-render fallback after cache loss;
- preservation of every legacy UUID and durable ID, with allocation only for
  truly new identity-less rows; and
- retry-stable 128-bit namespaces plus a deterministic big-endian `u64`
  ordinal, encoded as one 32-character unpadded base64url ID. Reservations are
  mutation/file/generation-bound, durable only when a new ID commits, and
  garbage-collected with their owning state; and
- truthful path-derived media types for CSV, TSV, and JSON, with unknown
  extensions left unspecified.

The actor transition is serialized per exact document while unrelated actors
continue independently. A fair plugin-generation fence preflights owned files
and prevents stale sessions from publishing across an upgrade.

## Full-engine CSV latency/memory acceptance

### Design

The deterministic fixture has 220,000 rows and exactly 10,680,000 bytes. Each
sample changes one field in the middle row through the ordinary SQL blob API;
edits alternate between two byte strings so every sample is a real transition.

For each backend and each edit/render cell, the run used:

- 12 fresh-process paired blocks;
- exactly 6 control→candidate and 6 candidate→control blocks;
- 5 unreported warmups and 20 measured samples per arm/block;
- 240 measured observations per arm/cell;
- the same host, toolchain, build features, fixture, and 256 MiB diagnostic
  ceiling for both timing arms; and
- separate production-cap runs with the candidate's memory override omitted.

The 256 MiB timing ceiling is necessary because the v1 control cannot
initialize this fixture under 64 MiB. It does not weaken the candidate gate:
every candidate sample asserts its observed high-water is at most 64 MiB, and
the separate production run uses the real default limit.

The analyzer uses fixed seed `0x4c495832` and 10,000 hierarchical cluster
bootstrap draws. Each draw resamples paired process blocks and then
observations within both selected arms. It recomputes pooled p50 and p95 log
ratios. Edit upper bounds must be strictly below `0.80`; render upper bounds
must be at most `1.05` for p50 and `1.10` for p95.

### Latency and gate

All values are milliseconds. `Ratio` is pooled candidate/control; `Upper` is
the one-sided 95% bootstrap upper bound.

| Backend | Operation | Quantile | Control | Candidate | Ratio | Upper | Required | Result |
|---|---|---:|---:|---:|---:|---:|---:|---|
| RocksDB LocalFilesystem | edit | p50 | 6,507.439 | 63.610 | 0.009775 | 0.009930 | `< 0.80` | pass |
| RocksDB LocalFilesystem | edit | p95 | 6,906.246 | 71.741 | 0.010388 | 0.011181 | `< 0.80` | pass |
| RocksDB LocalFilesystem | exact render | p50 | 2,317.470 | 18.013 | 0.007773 | 0.008186 | `<= 1.05` | pass |
| RocksDB LocalFilesystem | exact render | p95 | 2,641.085 | 20.259 | 0.007671 | 0.008222 | `<= 1.10` | pass |
| cached SlateDB | edit | p50 | 9,659.544 | 80.184 | 0.008301 | 0.008654 | `< 0.80` | pass |
| cached SlateDB | edit | p95 | 10,321.124 | 112.149 | 0.010866 | 0.011611 | `< 0.80` | pass |
| cached SlateDB | exact render | p50 | 7,600.187 | 6.397 | 0.000842 | 0.000901 | `<= 1.05` | pass |
| cached SlateDB | exact render | p95 | 8,151.819 | 9.749 | 0.001196 | 0.001408 | `<= 1.10` | pass |

Raw samples and the independently generated gate result are in:

- [`pr2-paired-raw-abed4ef9.json`](pr2-paired-raw-abed4ef9.json); and
- [`pr2-paired-gate-abed4ef9.json`](pr2-paired-gate-abed4ef9.json).

### Warm-path work invariants

Every measured provenance-backed candidate edit asserts:

| Counter | Observed |
|---|---:|
| source calls / bytes read | 0 / 0 |
| Component import calls | 1 |
| Component boundary bytes | 479 or 500 |
| guest linear-memory high-water | 61,145,088 bytes |
| host full-diff bytes compared | 0 |
| host full-content classification bytes | 0 |
| full-state semantic rows materialized | 0 |
| change-payload requests / returned payloads | 0 / 0 |
| durable semantic changes | 1 upsert |
| private document cache hits | 1 |
| shared renderer cache hits | 1 |
| full document reparses | 0 |
| full renderer invocations | 0 |
| filesystem-sync full renders | 0 |

This removes the v1 warm-path fan-out of 220,092 requested and 220,089
returned change payloads. The counter assertions are part of the benchmark
executable, so a faster sample that regresses to document-sized semantic work
fails the run.

### Production 64 MiB boundary

With `LIX_PROFILE_WASM_MEMORY_MIB` omitted, the real full-engine smoke passed
on both backends. The warm edit high-water was 61,145,088 bytes (58.3125 MiB).
The setup, edit, counter, and fresh-process exact-render results are retained in
[`pr2-production64-abed4ef9.json`](pr2-production64-abed4ef9.json).
Direct runtime tests additionally observed:

- 53,018,624 bytes (50.5625 MiB) for the 220,000-row initial import; and
- at most 61,931,520 bytes (59.0625 MiB) across cold open and warm edit.

All are below 67,108,864 bytes. The runtime accounts aggregate linear memory
across the Store and rejects a second memory or growth beyond the boundary.

## Pre-recovery auxiliary diagnostics

The fallback, logical-I/O/RSS, and CPU-profile evidence in the next three
sections was collected from the initial `6dd5612b` implementation before the
`eea45b96` stale-actor recovery and `abed4ef9` runtime lifecycle hardening.
These diagnostics characterize the incremental mechanism but do not validate
the final candidate's recovery or lifecycle behavior. Final-SHA evidence is the
paired gate, production-cap run, JSON diagnostic, and final correctness suites.

### Missing-provenance fallback

The fallback was measured separately with the memory override, splice sidecar,
and I/O wrapper all absent. It ran under the production 64 MiB policy with one
warmup and two measured edits per backend.

| Backend | p50 | p95 | full-diff bytes | classification bytes |
|---|---:|---:|---:|---:|
| RocksDB LocalFilesystem | 69.760 ms | 74.852 ms | 21,360,002 | 10,680,000–10,680,007 |
| cached SlateDB | 100.647 ms | 109.968 ms | 21,360,002 | 10,680,000–10,680,007 |

The fallback remains document-scale in host comparison/classification, as
intended. After that bounded comparison it still emits one durable semantic
change, materializes no full semantic rows, hits both document caches, and
performs no full guest reparse/render or filesystem-sync render. These
two-sample diagnostics are not mixed into the acceptance gate.

### Logical I/O, RSS, and retained bytes

Logical I/O was collected in separate single-round, provenance-backed runs so
instrumentation could not affect the latency gate.

| Backend | get-many calls / keys / values | scan calls / rows | put calls / rows | delete calls / rows | edit max RSS | retained after edit | gross retained delta |
|---|---:|---:|---:|---:|---:|---:|---:|
| RocksDB LocalFilesystem | 720 / 805 / 795 | 1 / 6 | 11 / 354 | 1 / 1 | 612,996 KiB | 87,340,595 B | +3,157,201 B |
| cached SlateDB | 713 / 798 / 788 | 1 / 5 | 11 / 349 | 1 / 1 | 696,684 KiB | 220,709,088 B | +4,594,126 B |

Both edit processes performed 12 logical read transactions and one logical
write transaction. The row counts above include engine metadata/index work;
they are not semantic plugin payload counts. The semantic transition remains
one row.

The RocksDB retained total separates into 76,251,611 B database,
10,680,007 B materialized file, and 408,975 B plugin archive, leaving 2 B
unclassified by the compact diagnostic relative to its 87,340,595 B total. The
SlateDB retained total separates into 156,067,436 B local object store and
64,641,652 B cache. Setup-process maximum RSS was 1,107,772 KiB for RocksDB
LocalFilesystem and 1,364,040 KiB for cached SlateDB. Whole-process RSS
includes engine reopen, plugin prewarm, backend caches, and synchronization; it
is not guest memory or per-actor retained memory. Actor admission is bounded to
eight per workspace, with one predecessor each.

The compact raw diagnostic artifact is
[`pr2-diagnostics-6dd5612b.json`](pr2-diagnostics-6dd5612b.json).

The current harness does **not** expose physical WAL bytes/calls or quiesced
live bytes. GNU `time` filesystem counters are OS accounting units, not WAL
bytes. Cached SlateDB flushes after the run but does not establish
compaction/GC quiescence; this RocksDB lane has no explicit physical backend
flush. Therefore these figures are gross retained-footprint checks, not a
claim that the WAL/live-byte amplification gate is closed. Physical WAL and
quiesced-live-byte validation remains open and is explicitly not inferred from
the latency win.

### Before/after CPU profiles

The full-process 99 Hz DWARF profiles use the same 256 MiB diagnostic ceiling
for both arms, five warmups, and twenty edits. They include reopen, prewarm,
acknowledgement, edits, close, filesystem sync, and backend flush. Their widths
must not be interpreted as A/B latency ratios because the arm durations and
sample totals differ substantially.

- RocksDB LocalFilesystem:
  [control](profiles/pr2-rocksdb-fs-control-6dd5612b.svg),
  [candidate](profiles/pr2-rocksdb-fs-candidate-6dd5612b.svg)
- cached SlateDB:
  [control](profiles/pr2-slatedb-cached-control-6dd5612b.svg),
  [candidate](profiles/pr2-slatedb-cached-candidate-6dd5612b.svg)

| Backend / arm | Raw events | Capture duration | Folded stacks |
|---|---:|---:|---:|
| RocksDB control | 18,320 | 177.553 s | 781 |
| RocksDB candidate | 717 | 5.304 s | 184 |
| SlateDB control | 61,343 | 296.915 s | 2,259 |
| SlateDB candidate | 3,342 | 15.853 s | 666 |

The RocksDB control is dominated by page-fault, anonymous-page allocation,
and unmap churn; the candidate exposes a much larger share of fixed libc,
RocksDB CRC/recovery, and filesystem costs. That direction qualitatively
corroborates removal of full-state materialization. Both SlateDB arms are
dominated by Tokio wake/wait, futex, and scheduler topology.

The optimized binary and Wasmtime guest did not unwind into reliable Lix or
guest function attribution, and some candidate stacks terminate in libc or
unknown frames. The profiles therefore corroborate OS/runtime shape only.
Latency and explicit work counters carry the architectural claim.

## 10 MB JSON diagnostic

The JSON fixture is exactly 10,000,000 bytes with 220,000 properties. Each
edit changes one byte in `property_110000`. Each backend uses separate fresh
processes for edit and render, with five warmups and twenty measured samples.

| Backend | Edit p50 / p95 | Exact render p50 / p95 |
|---|---:|---:|
| RocksDB LocalFilesystem | 7,831.846 / 7,993.483 ms | 2,660.016 / 2,689.447 ms |
| cached SlateDB | 10,119.876 / 10,601.274 ms | 8,006.399 / 8,379.292 ms |

The complete samples are in
[`pr2-json-diagnostic-abed4ef9.json`](pr2-json-diagnostic-abed4ef9.json).
This is final-`abed4ef9`, candidate-only Component v1 mechanism evidence under
a 256 MiB diagnostic guest ceiling. It has no v2 control/candidate gate and
must not be used to claim that JSON is incremental or production-ready under
64 MiB. Its multi-second timings instead quantify the next format vertical
slice.

## AX authoring evaluation

The pinned 10-agent API-authoring cohort, run against the pre-recovery
`6dd5612b` authoring surface, completed successfully. The exact task was:

> Implement and test a Wasm Component v2 plugin that round-trips a two-column
> TSV file and emits one sparse entity upsert for a localized row edit.

The cohort uses model `gpt-5.6-sol`, parallelism 2, a 3,600-second per-agent
timeout, tool label `the production Lix plugin API in this repository`, and
candidate commit `6dd5612b`. This task and cohort differ from PR1's candidate
evaluations, so no cross-cohort ergonomics comparison will be claimed.

The runner could not reproduce every requested protocol control. The recorded
temperature `0` and maximum 40 turns were targets but were not enforceable by
`codex exec`; the 3,600-second process timeout was the actual bound. Codex's
built-in system/developer instructions replaced an empty system prompt, and
its shell/`apply_patch` environment replaced the requested Claude tool
allow-list. `gpt-5.6-sol` replaced unavailable `claude-opus-4-7`, and all agent
and judge processes inherited a shared Cargo target. The artifact records these
deviations, so the scores evaluate this concrete Codex authoring setup rather
than a tool- and system-neutral API cohort.

| Result | Value |
|---|---:|
| Successful implementations | 10 / 10 |
| Success rate | 100% |
| Median final score | 76 |
| p25 / p75 final score | 72.75 / 82.75 |
| Final-score range | 52–85 |
| Median completion time | 297.2 s |
| Median tool calls | 15 |

Every independent judge cited a passing exact TSV round-trip and one sparse
localized row upsert, backed by a successful `wasm32-wasip2` build and tests.
Agents used two viable discovery paths: some added focused TSV behavior to the
production CSV v2 guest, while others created a separate `plugin_tsv_v2`
crate. All completed without human intervention.

The 100% task success supports the narrow claim that the `6dd5612b` contract
and reference were discoverable and usable in this authoring setup. It does not
validate the final candidate's runtime correctness or performance. The median
score of 76 and the recovered command errors still expose authoring overhead:
packet/binding glue is reference code rather than a typed standalone SDK, and
several agents had to discover how much of the CSV implementation to reuse.
That is consistent with keeping the general API unfrozen and making typed
author tooling the next ergonomics increment.

The complete scored artifact is
[`ax-eval/production-v2-pr2-result.json`](ax-eval/production-v2-pr2-result.json).

## Correctness evidence

The production implementation and composed tests cover:

- insert, edit, reorder, delete, dialect fidelity, sparse order-key
  reassignment, duplicate-identical-row determinism, and exact cold render;
- two sessions editing different rows, deterministic same-row LWW, concurrent
  edit/delete, unseen omission preservation, and seen omission deletion;
- legacy UUID edit/reorder/delete plus a new compact ID through restart and
  cold reopen;
- exact lost-response retry reuse, durable namespace restart/tombstone
  behavior, and same mutation ID/different digest rejection;
- byte-identical private views with distinct semantic roots;
- commit abort, deterministic validation rejection, guest error/trap, timeout,
  actor retirement/eviction, and safe cold fallback;
- a stale cold-install candidate losing to a concurrently committed actor,
  exact replacement of a captured stale slot, and cross-engine cold recovery
  without clobbering the newer semantic root;
- fresh standalone fork/drop fuel and epoch limits after an idle interval,
  top-level deadline rearming after delayed host input construction, and
  retirement before guest cleanup after an aggregate deadline expires;
- file deletion/recreation, branch isolation, rename/unmatch cleanup, and
  plugin-generation preflight/fencing;
- no precommit filesystem publication and rejection of stale cached
  materializations across branch/root/incarnation; and
- complete 220,000-row import, localized edit, and exact render at 64 MiB.

Validated suites before evidence publication:

- `cargo test -p lix_engine --lib`: 1,202 passed, 6 ignored, 0 failed;
- engine `code_structure`: 27 passed;
- full `cargo test -p lix_engine`: passed, including integration tests and
  documentation tests;
- `cargo test -p lix_sdk --lib`: 23 passed, 1 ignored, 0 failed;
- composed `cargo test -p lix_sdk_tests --test e2e`: 13 passed; and
- `cargo fmt --all -- --check` and `git diff --check` passed.

Before the two recovery/lifecycle commits, the initial `6dd5612b`
implementation also passed 21 `plugin_csv_v2` unit tests plus documentation
tests, 39 server-protocol tests with 1 ignored, 24 JS remote-client tests, 4
large-file fixture/package tests, 13 engine-benchmark Python tests, and 13 AX
runner Python tests. These are useful breadth checks but are not presented as
post-`abed4ef9` lifecycle validation.

## Reproduction

Build both immutable binaries from their recorded commits with the same
profile features described in
[`experiments/plugin-api-v2/engine-bench/README.md`](../../experiments/plugin-api-v2/engine-bench/README.md),
then run:

```sh
python3 experiments/plugin-api-v2/engine-bench/run_paired.py \
  --baseline <immutable-control-binary> \
  --candidate <immutable-abed4ef9-candidate-binary> \
  --run-dir <new-dedicated-run-directory> \
  --output perf-results/plugin-api-v2/pr2-paired-raw-abed4ef9.json

python3 experiments/plugin-api-v2/engine-bench/paired_gate.py \
  perf-results/plugin-api-v2/pr2-paired-raw-abed4ef9.json \
  --output perf-results/plugin-api-v2/pr2-paired-gate-abed4ef9.json

python3 experiments/plugin-api-v2/engine-bench/run_json_diagnostic.py \
  --candidate <immutable-abed4ef9-candidate-binary> \
  --run-dir <new-dedicated-json-run-directory>

cp <new-dedicated-json-run-directory>/json-diagnostic.json \
  perf-results/plugin-api-v2/pr2-json-diagnostic-abed4ef9.json
```

The recorded fallback and instrumented edit are pre-recovery `6dd5612b`
auxiliary diagnostics. Reproducing those artifacts requires the immutable
`6dd5612b` candidate binary, fresh setup directories, and these settings for
each required backend:

```sh
# Bounded full-byte-diff fallback, production memory policy.
env -u LIX_PROFILE_SPLICE_PROVENANCE \
  -u LIX_PROFILE_IO_STATS \
  -u LIX_PROFILE_WASM_MEMORY_MIB \
  LIX_PROFILE_FORMAT=csv \
  LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_WARMUPS=1 \
  LIX_PROFILE_ROUNDS=2 \
  <immutable-6dd5612b-candidate-binary> <backend> edit <fresh-setup-case>

# Provenance-backed logical-I/O/RSS diagnostic, production memory policy.
/usr/bin/time -v env -u LIX_PROFILE_WASM_MEMORY_MIB \
  LIX_PROFILE_FORMAT=csv \
  LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_WARMUPS=0 \
  LIX_PROFILE_ROUNDS=1 \
  LIX_PROFILE_SPLICE_PROVENANCE=1 \
  LIX_PROFILE_IO_STATS=1 \
  <immutable-6dd5612b-candidate-binary> <backend> edit <fresh-setup-case>
```

The recorded profiles likewise use the immutable control and `6dd5612b`
candidate. They use `perf record -F 99 -g --call-graph dwarf,16384`, followed
by `DEBUGINFOD_URLS= perf script`, `inferno-collapse-perf --all`, and
`inferno-flamegraph --deterministic --colors rust`. Control and candidate use
five warmups, twenty rounds, separate fresh setup cases, and the common 256 MiB
diagnostic ceiling; only the candidate receives validated splice provenance.

The benchmark README documents fixture construction, production-cap behavior,
backend policy, and all environment knobs. SQLite, raw RocksDB, and cacheless
SlateDB are not used as acceptance evidence.

## Scope and open production work

- Remote object-store latency, cache misses, retry behavior, and tail latency
  remain a separate Lixray production-hardening gate. The required SlateDB
  result here uses the production cache budgets over a local object store.
- Physical WAL and quiesced-live-byte amplification were not observable in
  this harness and remain open, as described above.
- JSON and the other non-CSV formats remain on v1; the JSON numbers are a
  mechanism diagnostic, not v2 acceptance.
- The generic five-format API is not frozen. The CSV bindings and checked
  packet codec are production reference glue, not yet a standalone v2 author
  SDK or packaging CLI.
- Rebase onto any newer PR1/main revision, including performance-affecting
  `#698`, requires rebuilding both arms and rerunning the paired gate.
