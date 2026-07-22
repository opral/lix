# Current v1 full-engine baseline on `e1a57ec3`

This artifact measures the real current Wasm CSV plugin through the ordinary
Lix SQL file API at exact public commit
`e1a57ec36877751fe2a35de6219c4173ffd40e0b`. It reruns the coherent
10.68 MB / 10.19 MiB RocksDB-filesystem and cached-SlateDB baseline after the two commits
that followed `5ffab346`: `684e6b5d` (skip unused durable function state) and
`e1a57ec3` (share the RocksDB adapter with the filesystem). It is a baseline,
not evidence that the experimental incremental plugin API has already improved
full-engine performance.

`origin/main` later advanced to `4aac696c`; parent `35f7e1c6` moves DataFusion
result scalar payloads instead of cloning them. That can affect blob-return and
exact-render cost, so this immutable e1 capture is discovery evidence rather
than the paired acceptance baseline for PR2. Both PR2 arms must be rebuilt from
the same then-current main commit.

## Decision summary

- No recurring edit or render median moved by 10% relative to the clean
  `5ffab346` run, let alone the project's greater-than-20% adoption threshold.
- A one-row edit still requests 220,092 change-payload keys and roughly 226k
  keys overall on both measured storage lanes. The latest commits remove only
  21 RocksDB and 31 SlateDB tree-history reads from the prior baseline.
- RocksDB-filesystem active-sample attribution is still dominated by complete
  state render/materialization and a second filesystem-sync render.
- Cached SlateDB is dominated by issuing the large point-read set through
  `get_snapshot_values`, SST initialization, and fetch scheduling.
- The architectural target therefore remains incremental host/plugin state
  exchange: avoid loading and crossing the Wasm boundary with every entity for
  a one-entity edit. The backend-specific SlateDB point-read path is a second,
  independent target.

## Scope and controls

- Engine base: `e1a57ec36877751fe2a35de6219c4173ffd40e0b`
  (`origin/main` when this rerun began).
- Comparison base: `5ffab346508249f4c83b179bfd0c7e09130c93e7`.
- Benchmark support only: the profiling harness/runtime exposure from public
  PR ancestor commits `58f69b50ce33685261db3d5f4ea1fbbfab238f7b` and
  `6e4872cc8efa749920e3d078a9e1c101e4c205c7`. No research engine algorithm is
  present in the measured binary.
- Fixture: deterministic 220,000-row CSV, exactly 10,680,000 initial bytes.
- Edit: append `-edited` to the middle row; alternating input makes all 11
  measured rounds real one-row transitions.
- API: ordinary whole-blob `INSERT ... ON CONFLICT ... UPDATE`.
- Acknowledgement: exactly `SELECT data FROM lix_file WHERE path = $1` once
  before the measured edit series. The point read and both blob constructions
  are outside every edit timer.
- Storage lanes: production `LocalFilesystem` over RocksDB (`rocksdb-fs`) and
  cached SlateDB over a local object store using Lixray's per-workspace 64 MiB
  disk / 4 MiB block / 1 MiB metadata cache budgets. The Slate lane controls
  engine/cache behavior but excludes remote object-store latency, misses,
  retries, and tails; production Lixray claims need a separate remote run.
- SQLite was not built, run, profiled, or used as evidence. The direct raw
  RocksDB adapter is also excluded because it omits production
  `LocalFilesystem` synchronization.
- Runs were serial on an Apple M5 Pro (18 cores, 64 GiB), macOS 26.3.1 arm64,
  `rustc 1.97.0-nightly (b954122bb)`, and Samply 0.13.1.

Every accepted timing and profile phase had zero matching Cargo, rustc,
Samply, or benchmark processes both before and after it. The timing suite used
two zero-process checks separated by 30 seconds before its coordinated no-build
window; the two profiles used the same fresh two-check gate. With 11 samples,
nearest-rank p95 is the maximum and is diagnostic rather than a stable tail
estimate.

The timed and profiled results use a benchmark-only 256 MiB Wasm linear-memory
ceiling, applied to both the SQL engine and the second engine owned by
`LocalFilesystem` sync. Plugin execution remains Wasm. This is a diagnostic
capacity override, not a proposed production setting. The exact-e1 run did not
repeat the production 64 MiB capacity test; the prior `5ffab346` run failed at
64 MiB on both backends, so that historical result must not be presented as a
new exact-e1 capacity measurement.

## End-to-end latency

Times are harness intervals, not whole-process wall time. Cold initial write is
one sample. Edit and exact render each have 11 samples.

| Storage lane | Cold initial write | Warm one-row edit p50 | Edit p95/max | Exact render p50 | Render p95/max |
|---|---:|---:|---:|---:|---:|
| RocksDB filesystem | 5,841.193 ms | 2,567.816 ms | 2,718.158 ms | 854.695 ms | 960.413 ms |
| Cached SlateDB | 4,624.748 ms | 3,835.869 ms | 4,412.368 ms | 4,079.568 ms | 4,374.256 ms |

Relative to the clean exact-`5ffab346` run:

| Storage lane | Cold setup delta | Edit p50 delta | Render p50 delta |
|---|---:|---:|---:|
| RocksDB filesystem | +7.06% | +2.59% | **-5.06%** |
| Cached SlateDB | -1.33% | **-6.87%** | **-1.98%** |

The cold setup cells are single samples and are not attribution evidence. None
of the recurring medians crosses either a 10% screening threshold or the
greater-than-20% decision threshold.

Sorted raw samples, in milliseconds:

```text
rocksdb-fs edit = [2492.684125, 2512.897583, 2518.593792, 2533.746125,
  2551.899708, 2567.815958, 2572.886167, 2604.851500, 2638.386209,
  2702.479375, 2718.158084]
rocksdb-fs render = [831.314542, 839.364375, 841.308708, 852.614375,
  854.584167, 854.695208, 855.847542, 887.597459, 892.921750,
  897.812167, 960.412625]

slatedb-cached edit = [3670.990166, 3716.578750, 3758.223208,
  3795.577208, 3797.879958, 3835.868500, 3851.509458, 3915.621833,
  3960.678875, 4018.151416, 4412.368250]
slatedb-cached render = [3927.887584, 3932.775875, 4001.023959,
  4036.689584, 4071.361959, 4079.568250, 4102.456250, 4121.219250,
  4138.902875, 4342.351208, 4374.256459]
```

## Resident memory

Maximum resident set size comes from `/usr/bin/time -l` around the complete
process, including reopen, prewarm, host, store, Wasmtime, guest, and shutdown.

| Storage lane | Cold setup max RSS | 11 edits max RSS | 11 renders max RSS |
|---|---:|---:|---:|
| RocksDB filesystem | 2,723,840,000 B | 2,613,559,296 B | 1,320,910,848 B |
| Cached SlateDB | 2,235,891,712 B | 1,812,332,544 B | 1,190,445,056 B |

Relative to `5ffab346`, RocksDB RSS changed +1.51% / +26.54% / -8.89%
for setup/edit/render. SlateDB changed -0.33% / -16.88% / -0.98%. RSS is a
whole-process high-water mark and these mixed, one-run movements do not isolate
a causal engine-memory change. In particular, the RocksDB edit RSS regression
should be reproduced before any design decision.

## Gross on-disk bytes and marginal directory deltas

These are recursive directory snapshots after each process flushes and closes.
They are gross retained footprint, **not physical per-edit write amplification**.
Directory differences include retained history, compaction timing, manifest
changes, and (for SlateDB) cache population/eviction. True write amplification
requires backend physical-write counters over a steady-state compaction window.

| Backend / component | After initial write | After 11 edits | After 11 renders |
|---|---:|---:|---:|
| RocksDB filesystem, total | 75,418,490 B | 76,021,707 B | 76,490,747 B |
| RocksDB internal state | 62,663,686 B | 63,266,896 B | 63,735,936 B |
| Materialized CSV | 10,680,000 B | 10,680,007 B | 10,680,007 B |
| Installed plugin archive | 2,074,802 B | 2,074,802 B | 2,074,802 B |
| Cached SlateDB, total | 193,080,915 B | 194,754,177 B | 188,549,213 B |
| Cached SlateDB, durable object store | 128,905,740 B | 130,373,573 B | 131,695,342 B |
| Cached SlateDB, local cache | 64,175,175 B | 64,380,604 B | 56,853,871 B |

The initial RocksDB internal state is 5.867x the source blob and its complete
filesystem is 7.062x. Cached SlateDB durable state is 12.070x the blob and its
complete initial footprint is 18.079x. Gross initial footprint changed by
-0.011% for RocksDB and +0.001% for SlateDB relative to `5ffab346`.

Marginal recursive-directory deltas were +603,217 B from Rocks setup through
11 edits and +469,040 B through the following 11 read-only render processes.
SlateDB moved +1,673,262 B through edits and -6,204,964 B through renders as
compaction/cache eviction changed the snapshot. The negative SlateDB render
delta is direct evidence that these deltas are not bytes-written counters.

These measurements do not support packing individual logical KVs into generic
storage packs. RocksDB and SlateDB already physically block-pack keys; the
measured issue is the number and lifetime of logical state/changelog records,
duplicate full-file materializations, and the way those logical reads are
issued. A format-specific covering/index structure could still be valuable,
but only if it avoids logical work and beats the threshold on both dense and
sparse fixtures.

## Logical storage work for one row

Logical I/O was collected in separate fresh single-edit cases. Counters reset
after the exact acknowledgement and immediately before the ordinary SQL write.
Instrumentation timings are not latency evidence.

| Counter | RocksDB filesystem | Cached SlateDB |
|---|---:|---:|
| read transactions | 9 | 9 |
| write transactions | 1 | 1 |
| `get_many` calls | 6,187 | 6,152 |
| requested keys | 226,318 | 226,283 |
| returned values | 226,311 | 226,276 |
| scans / returned entries | 3 / 6 | 3 / 5 |
| `put_many` calls / rows | 7 / 12 | 7 / 12 |
| delete calls / rows | 1 / 1 | 1 / 1 |

RocksDB filesystem reads by storage space:

| Space | Role | Calls | Keys | Values | Scans / entries |
|---|---|---:|---:|---:|---:|
| `0x0002_0001` | JSON store | 5 | 61 | 61 | 0 / 0 |
| `0x0004_0001` | tracked-state tree chunks | 6,118 | 6,118 | 6,118 | 0 / 0 |
| `0x0004_0004` | tracked-state commit roots | 18 | 18 | 18 | 0 / 0 |
| `0x0004_0006` | live-state index | 19 | 26 | 23 | 2 / 0 |
| `0x0006_0001` | commits | 2 | 2 | 1 | 1 / 6 |
| `0x0006_0002` | change payloads | 24 | 220,092 | 220,089 | 0 / 0 |
| `0x0007_0002` | filesystem path index | 1 | 1 | 1 | 0 / 0 |

Cached SlateDB has the identical non-tree shape. Its tracked-state tree uses
6,083 calls/keys/values and its commit scan returns five entries. Relative to
`5ffab346`, total calls and requested keys fall by 21 on RocksDB and 31 on
SlateDB, entirely in tree-history reads; every semantic non-tree space is
unchanged. A one-row edit still requests all 220,092 change-payload keys.

The separate post-I/O gross directory snapshots were 75,981,465 B for RocksDB
(63,226,654 B internal) and 193,596,691 B for cached SlateDB (130,225,438 B
durable object store, 63,371,253 B cache). They are retained-footprint checks,
not write-amplification measurements.

## Samply profiles

The two exact-e1 one-edit profile bundles are retained beside this report:

- `full-engine-v1-rocksdb-fs-edit-220k-e1a57ec3.json.gz`
- `full-engine-v1-rocksdb-fs-edit-220k-e1a57ec3.json.syms.json`
- `full-engine-v1-slatedb-cached-edit-220k-e1a57ec3.json.gz`
- `full-engine-v1-slatedb-cached-edit-220k-e1a57ec3.json.syms.json`

The RocksDB profile contains 14,522 samples, 7,624 classified active. The
SlateDB profile contains 343,971 samples, 11,204 classified active after exact
binary/sidecar symbol resolution and an exact blocking-leaf whitelist; most of
the remaining Slate samples are parked or kernel-wait samples across 98
threads. Substring matching is deliberately forbidden because names such as
`Future::poll`, `spawn_fetches`, and `Unparker::unpark` are real work. Profiled edit timers were 2,787.316 ms
and 4,088.602 ms respectively. Sampling overhead and whole-process scope make
those timers descriptive only, not latency-table evidence.

The async marker future did not survive as a native stack frame:
`--under profile_file_write_phase` returns zero samples for both profiles.
Attribution is therefore over whole-process samples: reopen, prewarm,
exact acknowledged render, one timed write, filesystem sync where applicable,
and close/flush are all in scope. Inclusive shares overlap and must not be
summed. In the unfiltered total view, Rocks render/materialize/filesystem-sync/
change-load frames contain 5,286 (36.40%), 2,841 (19.56%), 2,787 (19.19%), and
1,938 (13.35%) samples. Slate `get_snapshot_values`, fetch scheduling, and
object `read_range` contain 4,028 (1.17%), 2,541 (0.74%), and 1,293 (0.38%) of
all samples; the large total denominator is why the blocking-leaf active view
below is also reported.

RocksDB filesystem:

| Active inclusive frame | Samples | Share of active samples |
|---|---:|---:|
| `render_plugin_files_for_sql` | 5,270 | 69.12% |
| `scan_tracked_branch_rows` | 3,353 | 43.98% |
| `materialize_rows_from_index_entries` | 2,838 | 37.22% |
| `LocalFilesystem::sync_from_lix` | 2,779 | 36.45% |
| `load_change_records` | 1,935 | 25.38% |
| `execute_fast_lix_file_path_writes` | 1,791 | 23.49% |
| `plugin_write_reconciliation` | 1,785 | 23.41% |
| RocksDB `DB::MultiGet` | 1,194 | 15.66% |
| Wasm component `render` | 1,458 | 19.12% |
| `detect_changes_with_component_instance` | 485 | 6.36% |
| Wasmtime component `detect_changes` | 455 | 5.97% |

Cached SlateDB:

| Active inclusive frame | Samples | Share of active samples |
|---|---:|---:|
| `get_snapshot_values` | 3,915 | 34.94% |
| `InternalSstIterator::spawn_fetches` task | 2,348 | 20.96% |
| `execute_fast_lix_file_path_writes` | 2,315 | 20.66% |
| `plugin_write_reconciliation` | 2,306 | 20.58% |
| `SstIterator::init` | 2,094 | 18.69% |
| `InternalSstIterator::advance_block` | 1,398 | 12.48% |
| object-store `read_range` | 1,114 | 9.94% |
| `scan_tracked_branch_rows` | 691 | 6.17% |
| `render_plugin_files_for_sql` | 678 | 6.05% |
| `materialize_rows_from_index_entries` | 564 | 5.03% |
| `detect_changes_with_component_instance` | 491 | 4.38% |
| Wasmtime component `detect_changes` | 456 | 4.07% |
| Wasm component `render` | 287 | 2.56% |
| `load_change_records` | 264 | 2.36% |

The exact-e1 Rocks symbols now attribute shared storage work to
`lix_rocksdb_storage`, as expected from `e1a57ec3`. The older `5ffab346`
profile remains useful historical causal context, but it is not a substitute
for this exact-e1 capture. Frame-share movement is not an A/B latency
decomposition: denominators differ, inclusive frames overlap, async work can
run on other threads, and a larger share can simply mean another component
shrunk. Repeated harness latency plus logical I/O remain the decision evidence.
Leaf categorization is not exhaustive: 1,584 active Rocks samples and 659
active Slate samples retain unresolved absolute-like system-library addresses
whose profile frames lack usable RVAs. The engine/plugin inclusive frames above
resolve, but this is another reason not to treat the table as elapsed-time
decomposition.

Profile bundle SHA-256 checksums (the sidecar must remain adjacent to its
`.json.gz` file):

```text
2b9bffa0e26612304b7a9e27e21ad8ec8fe7bf0f85f17f79064ea360ada1e455  full-engine-v1-rocksdb-fs-edit-220k-e1a57ec3.json.gz
32c4eae47ad96bb709f786502491e33aed2da39d5a50a17872d3ffbbb16e0c17  full-engine-v1-rocksdb-fs-edit-220k-e1a57ec3.json.syms.json
5c8da88361f603aea4999445acfa092c537dba03709774a88fbc13b633dafb26  full-engine-v1-slatedb-cached-edit-220k-e1a57ec3.json.gz
b31a163a826976d71deac12825fbc3ed01805a1c335c853ad2e271ad4c625344  full-engine-v1-slatedb-cached-edit-220k-e1a57ec3.json.syms.json
```

## What exact e1 establishes

1. The two latest-main commits do not materially change the large plugin file
   bottleneck: recurring medians move less than 7%, and semantic logical-I/O is
   unchanged.
2. Stable entity IDs cannot require passing every active entity into Wasm on
   every edit. The host/plugin design needs an incremental identity/index
   protocol that fetches candidate identities on demand and returns a bounded
   delta while preserving host transaction authority.
3. RocksDB's second full semantic render on filesystem sync remains a distinct
   target. Exact rendered-blob reuse or validated splice output can remove that
   work without replacing the storage engine's own physical block packing.
4. SlateDB needs an adaptive retrieval plan. Dense 220k-key point reads spend
   41% of active samples in `get_snapshot_values`, with SST initialization and
   fetch scheduling beneath it; any range/batch alternative must fall back for
   sparse key sets to avoid range explosion.
5. A packed Wasm ABI can reduce component-boundary encoding/copying but cannot
   by itself remove the dominant host storage/materialization work. It should
   accompany, not replace, incremental state exchange.

## Exact public-commit reproduction

The hashes, not moving branch names, are authoritative. Starting from a fresh
clone, fetch main and the public research branch, detach at the measured engine
commit, and restore only the benchmark-support files used for this run:

```sh
git clone https://github.com/opral/lix.git lix-e1-benchmark
cd lix-e1-benchmark
git fetch origin main codex/wasm-plugin-api-research
git switch --detach e1a57ec36877751fe2a35de6219c4173ffd40e0b
git restore --source=6e4872cc8efa749920e3d078a9e1c101e4c205c7 -- \
  Cargo.lock packages/rs-sdk/Cargo.toml \
  packages/rs-sdk/src/lib.rs
git restore --source=58f69b50ce33685261db3d5f4ea1fbbfab238f7b -- \
  packages/rs-sdk/benches/profile_merge_10k.rs \
  perf-results/plugin-api-v2/analyze_samply.py
```

The build is intentionally not `--locked`: starting from the restored research
lockfile, Cargo refreshes the `lix_sdk` benchmark dependency list against the
exact-e1 workspace graph (including the newly shared RocksDB crate), matching
the measured preparation path.

Build and set `BIN` to the emitted executable (the exact binary measured here
had SHA-256
`1d78e5b2c170696416150b5b4ae638f7cdeb1ca6416ba99519caafb20d35429a`):

```sh
cargo build --release -p lix_sdk \
  --bench profile_merge_10k \
  --features default_wasm_runtime,local_filesystem,profile_wasm_memory
BIN=target/release/deps/profile_merge_10k-5107728e8357399b
```

Before accepting a run on a shared host, verify no benchmark/build/profile
processes, wait 30 seconds, and repeat. The process-name gate is a local
contention check, not a portable isolation guarantee; a public rerun should
reserve the host and repeat the suite across independent processes.

Run each backend in its own fresh directory:

```sh
/usr/bin/time -l env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_WASM_MEMORY_MIB=256 \
  "$BIN" rocksdb-fs setup /tmp/lix-e1-rocks

/usr/bin/time -l env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_ROUNDS=11 LIX_PROFILE_WASM_MEMORY_MIB=256 \
  "$BIN" rocksdb-fs edit /tmp/lix-e1-rocks

/usr/bin/time -l env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_ROUNDS=11 LIX_PROFILE_WASM_MEMORY_MIB=256 \
  "$BIN" rocksdb-fs render /tmp/lix-e1-rocks
```

Replace `rocksdb-fs` with `slatedb-cached` and use a different fresh directory
for the SlateDB lane. Absolute times are hardware-specific; preserve the raw
samples and compare paired builds under the same host policy. Do not infer the
production 64 MiB result from this 256 MiB diagnostic run.

Logical I/O, again with a fresh case directory:

```sh
env LIX_PROFILE_INITIAL_ROWS=220000 LIX_PROFILE_ROUNDS=1 \
  LIX_PROFILE_WASM_MEMORY_MIB=256 LIX_PROFILE_IO_STATS=1 \
  "$BIN" rocksdb-fs edit /tmp/lix-e1-rocks-io
```

Profile without wrapping the target itself in `/usr/bin/env`:

```sh
LIX_PROFILE_INITIAL_ROWS=220000 LIX_PROFILE_ROUNDS=1 \
LIX_PROFILE_WASM_MEMORY_MIB=256 \
samply record --save-only --unstable-presymbolicate --rate 1000 \
  --profile-name full-engine-v1-rocksdb-fs-edit-220k-e1a57ec3 \
  --output perf-results/plugin-api-v2/full-engine-v1-rocksdb-fs-edit-220k-e1a57ec3.json.gz \
  -- "$BIN" rocksdb-fs edit /tmp/lix-e1-rocks-profile

python3 perf-results/plugin-api-v2/analyze_samply.py \
  perf-results/plugin-api-v2/full-engine-v1-rocksdb-fs-edit-220k-e1a57ec3.json.gz \
  --binary "$BIN"
```

Repeat with `slatedb-cached`, a fresh Slate directory, and the matching Slate
profile name/output. Samply's `.json.syms.json` sidecar must stay next to the
`.json.gz`, and analysis must use the exact matching binary. The raw profile
timer, inactive samples, and overlapping inclusive shares are not substitutes
for the N=11 latency result.
