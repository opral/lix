# Historical v1 full-engine baseline on `c789a2b1`

This artifact reruns the real current CSV Wasm plugin through the ordinary Lix
SQL file API after the final rebase. It is a baseline, not evidence that the
experimental v2 API has already improved the production engine.

## Scope and controls

- Engine base: `c789a2b10aada60fdd5707c247d9ef26136837ab`
  (`origin/main` when measured).
- Research tree: `5a5ee964405a8458d3b2511104a253912715a94c`, plus
  benchmark result artifacts. The engine algorithm is the `c789a2b1`
  algorithm.
- Historical comparison: [`full-engine-v1-baseline-66ad14da.md`](./full-engine-v1-baseline-66ad14da.md),
  produced with the same harness, fixture, backend policy, machine, and
  diagnostic guest ceiling before the intervening mainline optimizations.
- Fixture: deterministic 220,000-row CSV, exactly 10,680,000 initial bytes.
- Edit: append seven bytes to one field in the middle row; alternating input
  makes all 11 rounds real one-row transitions.
- API: ordinary `INSERT ... ON CONFLICT ... UPDATE` of the whole blob.
- Acknowledgement: exactly
  `SELECT data FROM lix_file WHERE path = $1` before the measured edit series.
  The point read and construction of both complete blobs are outside every
  edit timer.
- Backends: production `LocalFilesystem` over RocksDB (`rocksdb-fs`) and
  cached SlateDB with Lixray's per-workspace 64 MiB disk / 4 MiB block / 1 MiB
  metadata cache policy.
- SQLite was not built, run, or used. Direct raw RocksDB is not a headline
  result because it omits `LocalFilesystem` synchronization.
- Runs were serial on an Apple M5 Pro (18 cores, 64 GiB), macOS 26.3.1 arm64,
  `rustc 1.97.0-nightly (b954122bb)`, and Samply 0.13.1. Known overlapping
  Rust builds were excluded from reported latency and profile intervals.

The final mainline range adds an exact `lix_file` read fast path, SQL/provider
setup reductions, shared/coalesced observation results, large RocksDB read
buffers, lazy SlateDB range-delete snapshots, and CAS presence markers, among
other changes. This rerun is necessary because those changes can affect the
acknowledgement and storage work surrounding plugin execution.

## Production 64 MiB capacity check

The production per-component linear-memory ceiling remains 64 MiB. Both fresh
initial imports failed before producing the 220k-row file:

| Production backend | Exit | Whole-process wall | Max RSS |
|---|---:|---:|---:|
| RocksDB filesystem | 101 | 2.31 s | 280,641,536 B |
| Cached SlateDB | 101 | 2.01 s | 261,439,488 B |

As on `66ad14da`, the guest allocation failure crosses a `wasmtime-wasi` error
path that attempts to start a nested Tokio runtime. The visible panic is
`Cannot start a runtime from within a runtime`, not the original allocation
error. These are capacity and error-reporting results; they are not evidence
for increasing the production ceiling.

All successful timings below use a benchmark-only 256 MiB guest ceiling. For
`rocksdb-fs`, the harness applies it to both the outer SQL engine and the
second engine owned by `LocalFilesystem`. The plugin remains Wasm.

## End-to-end diagnostic latency

Times are harness intervals, not whole-process wall time. Cold initial write
has one sample. Edit and render have 11 samples. With N=11, the nearest-rank
p95 is the maximum and is highly sensitive to one scheduling or compaction
outlier.

| Production backend | Cold initial write | Warm one-row edit p50 | Edit p95/max | Exact render p50 | Render p95/max |
|---|---:|---:|---:|---:|---:|
| RocksDB filesystem | 4,877.824 ms | 2,645.585 ms | 4,597.188 ms | 926.038 ms | 1,090.421 ms |
| Cached SlateDB | 7,692.032 ms | 3,970.525 ms | 6,854.936 ms | 4,204.134 ms | 4,494.524 ms |

Ranges and outlier context:

| Production backend | Edit min-max | Tenth / max | Render min-max |
|---|---:|---:|---:|
| RocksDB filesystem | 2,485.031-4,597.188 ms | 3,578.624 / 4,597.188 ms | 869.934-1,090.421 ms |
| Cached SlateDB | 3,675.700-6,854.936 ms | 4,676.054 / 6,854.936 ms | 4,047.170-4,494.524 ms |

Sorted raw samples, in milliseconds:

```text
rocksdb-fs edit = [2485.031333, 2547.956500, 2562.809583, 2584.407666,
  2588.576583, 2645.584875, 2753.664292, 3129.729667, 3394.068167,
  3578.624000, 4597.187958]
rocksdb-fs render = [869.934125, 879.976166, 890.113875, 906.824041,
  908.762083, 926.038250, 928.188291, 944.253542, 981.065375,
  982.180333, 1090.421417]

slatedb-cached edit = [3675.700291, 3689.035125, 3859.637833,
  3873.268250, 3909.969958, 3970.524791, 3978.202459, 4005.525750,
  4159.190875, 4676.053916, 6854.936167]
slatedb-cached render = [4047.169834, 4147.281167, 4172.311375,
  4178.490375, 4203.515000, 4204.133875, 4278.258541, 4306.315750,
  4474.618875, 4478.465208, 4494.523666]
```

## Direct old-versus-new comparison

The two baselines are separate wall-clock runs, not a randomized paired A/B.
The comparison is useful for checking whether the new mainline invalidates the
architectural ranking, not for attributing a small percentage to any one
commit.

| Production backend / operation | `66ad14da` | `c789a2b1` | Change |
|---|---:|---:|---:|
| RocksDB filesystem edit p50 | 2,696.355 ms | 2,645.585 ms | -1.88% |
| RocksDB filesystem render p50 | 937.181 ms | 926.038 ms | -1.19% |
| Cached SlateDB edit p50 | 4,092.013 ms | 3,970.525 ms | -2.97% |
| Cached SlateDB render p50 | 4,190.029 ms | 4,204.134 ms | +0.34% |

No warm median moves by 20%. The exact-read and provider/setup improvements do
not remove the semantic plugin render or the current whole-state write
reconciliation. This is the decision-relevant result.

The N=11 p95/max moved +57.22% for RocksDB edits and +61.63% for SlateDB edits,
but each new lane has one conspicuous maximum. Conversely, SlateDB render
p95/max moved -8.61%. Treating any of those maxima as a causal mainline change
would require repeated, counterbalanced process-level trials. The cold SlateDB
write is also one highly variable sample and is not used to rank an
architecture.

## Resident memory

Maximum resident set size comes from `/usr/bin/time -l` around the complete
process, so it includes storage, host, Wasmtime, guest, reopen, warmup, and
close work outside the harness timer.

| Production backend | Cold setup max RSS | 11 edits max RSS | 11 renders max RSS |
|---|---:|---:|---:|
| RocksDB filesystem | 2,696,134,656 B (2.51 GiB) | 2,099,527,680 B (1.96 GiB) | 1,395,769,344 B (1.30 GiB) |
| Cached SlateDB | 2,479,161,344 B (2.31 GiB) | 2,017,361,920 B (1.88 GiB) | 1,299,382,272 B (1.21 GiB) |

Relative to `66ad14da`, edit-process RSS is 4.72% lower for RocksDB filesystem
and 4.77% lower for cached SlateDB. Setup/render differences range from -2.12%
to +11.99% and are whole-process high-water marks, not guest-only memory.
Current v1 still fails the 64 MiB guest requirement for this fixture.

## On-disk bytes

These are recursive sizes after each process flushes and closes:

| Backend / component | After initial write | After 11 edits | After 11 renders |
|---|---:|---:|---:|
| RocksDB filesystem, total | 75,414,687 B | 75,802,138 B | 76,272,876 B |
| Cached SlateDB, total | 193,109,120 B | 194,343,758 B | 198,820,617 B |
| Cached SlateDB, durable object store | 128,924,311 B | 130,397,014 B | 131,724,667 B |
| Cached SlateDB, local cache | 64,184,809 B | 63,946,744 B | 67,095,950 B |

The initial RocksDB filesystem total is 62,659,883 B internal RocksDB state,
the 10,680,000 B materialized CSV, the 2,074,802 B plugin archive, and a 2 B
`.gitignore`: 7.06x the source in total and 5.87x for internal state alone.
Initial cached SlateDB is 18.08x including cache and 12.07x for its durable
object store.

Initial durable bytes differ from `66ad14da` by less than 0.03% on both
backends. The old SlateDB post-render case happened to compact its object store
from 130.35 MB to 67.50 MB; the new case did not compact on that schedule.
Therefore the apparent +48.34% post-render total is compaction timing, not a
storage-format regression. No logical packing or storage claim should be based
on that phase snapshot.

## Logical storage work for one real row edit

Logical I/O was collected in separate fresh cases: initial bytes were present,
the exact acknowledgement ran outside the counters, and the first measured
payload was the edited blob. A same-payload diagnostic was discarded. The
wrapper perturbs timing, so these are counts only. It wraps the outer
semantic-state `Storage` passed to the Lix engine. In the RocksDB-filesystem
lane it does **not** instrument the second/internal RocksDB engine used by
`LocalFilesystem` synchronization. The counts below are therefore outer Lix
requests, not total filesystem RocksDB I/O; whole-process profiles and disk
snapshots still include both engines.

| Counter | RocksDB filesystem | Cached SlateDB |
|---|---:|---:|
| read transactions | 9 | 9 |
| write transactions | 1 | 1 |
| `get_many` calls | 6,154 | 6,155 |
| requested keys | 226,349 | 226,350 |
| returned values | 226,342 | 226,343 |
| scans / returned entries | 11 / 6 | 11 / 5 |
| `put_many` calls / rows | 7 / 11 | 7 / 11 |
| delete calls / rows | 1 / 1 | 1 / 1 |

RocksDB filesystem reads by storage space:

| Space | Role | `get_many` calls | Keys | Values | Scans / entries |
|---|---|---:|---:|---:|---:|
| `0x0002_0001` | JSON store | 7 | 91 | 91 | 0 / 0 |
| `0x0004_0001` | tracked-state tree chunks | 6,076 | 6,076 | 6,076 | 0 / 0 |
| `0x0004_0004` | tracked-state commit roots | 20 | 20 | 20 | 0 / 0 |
| `0x0004_0006` | live-state index | 21 | 29 | 26 | 10 / 0 |
| `0x0006_0001` | commits | 2 | 2 | 1 | 1 / 6 |
| `0x0006_0002` | change payloads | 28 | 220,131 | 220,128 | 0 / 0 |

Cached SlateDB has the same shape: 220,131 change-payload keys and 6,077
tracked-state tree-chunk point calls. Compared with `66ad14da`, total requested
keys fall by only 42 for RocksDB filesystem and 10 for cached SlateDB. A
one-row edit still requests more keys than the file has semantic rows.

This confirms why the latest exact-read and SQL setup work does not change the
plugin-API ranking: it removes surrounding fixed overhead while the write path
continues to load current semantic state proportional to the whole file.

## Samply profiles

Save-only 1 kHz profiles and symbol sidecars are checked in:

- [`full-engine-v1-rocksdb-fs-edit-220k-c789a2b1.json.gz`](./full-engine-v1-rocksdb-fs-edit-220k-c789a2b1.json.gz)
- [`full-engine-v1-slatedb-cached-edit-220k-c789a2b1.json.gz`](./full-engine-v1-slatedb-cached-edit-220k-c789a2b1.json.gz)

Keep each `.json.syms.json` beside its profile. Open with `samply load
<profile>` or summarize with `analyze_samply.py --binary <profile-binary>
<profile>`.

The async `profile_file_write_phase` marker again has zero samples in this
release build. The table is therefore **whole-process active-sample
attribution**: backend reopen, Wasm compile/prewarm, the exact acknowledged
render, one timed write, and close/flush are all present. Inclusive shares
overlap and must not be summed. Profiled write timers were 2,499.743 ms for
RocksDB filesystem and 3,850.173 ms for cached SlateDB; they are not used in
the N=11 latency table.

| Active inclusive frame | RocksDB filesystem | Cached SlateDB |
|---|---:|---:|
| Active samples | 5,918 | 8,187 |
| `render_plugin_files_for_sql` | 61.86% | 7.76% |
| `scan_tracked_branch_rows` | 48.92% | 7.37% |
| `LocalFilesystem::sync_from_lix` | 41.92% | n/a |
| `materialize_rows_from_index_entries` | 41.91% | 6.14% |
| `load_change_records` | 28.78% | 2.92% |
| `plugin_write_reconciliation` | 20.38% | 23.71% |
| RocksDB `multi_get_opt` | 18.30% | n/a |
| SlateDB `get_snapshot_values` | n/a | 39.60% |
| SlateDB `SstIterator::init` | n/a | 22.40% |
| SlateDB SST fetch tasks | n/a | 19.52% |
| SlateDB internal iterator init / advance | n/a | 14.68% / 14.60% |
| SlateDB object `read_range` | n/a | 6.75% |
| Wasm component `render` | 15.14% | 3.32% |
| `detect_changes_with_component_instance` | 1.55% | 5.47% |
| Wasmtime component `detect_changes` | 1.27% | 5.28% |

The profile mix changed from `66ad14da`, particularly SlateDB's Wasm-detect
share. That is not evidence that guest detection became 4.6x slower: the
denominator includes different compile, cache, storage, render, and shutdown
activity in one whole process. The stable evidence is the independent warm
latency medians and logical-I/O counts. Profiles locate remaining work; they do
not provide an isolated causal percentage for the timed edit.

## What this latest-main baseline establishes

1. Mainline work after `66ad14da` moves the four warm p50s by -2.97% to
   +0.34%, far below the project's greater-than-20% architecture gate.
2. One changed row still requests 226,349/226,350 keys, including 220,131
   change payloads. Exact point-read acceleration does not make warm semantic
   reconciliation sparse.
3. Current v1 still cannot initialize the 10.68 MB fixture under the 64 MiB
   guest ceiling. Raising the ceiling would hide, not fix, scaling.
4. SlateDB still spends large whole-process shares in per-key snapshot/SST
   work. Adaptive batching/dense-run reads remain justified after the API stops
   asking for every entity; a forced broad range remains unsafe for sparse
   keys.
5. `LocalFilesystem` still runs a second semantic render/materialization path.
   Passing through a precommit-validated renderer splice/blob remains a large
   target, subject to byte equality and commit/acknowledgement ordering.
6. The guest call is not the only problem. Even where Wasm detect reaches
   5.28% of a whole-process Slate profile, host/storage materialization and
   retrieval dominate the remaining scope. A packed ABI without sparse state
   retrieval cannot solve the end-to-end architecture.

The first production implementation target remains the B2 mechanism:
observation-selected structurally shared host source/root plus a Wasm-owned
incremental syntax/identity index. It is not accepted until it beats this
baseline by more than 20% at the full SQL boundary on both backends, passes the
64 MiB limit, and passes lifecycle/stable-ID/cold-render correctness gates.

## Reproduction

Build:

```sh
cargo build --release -p lix_sdk \
  --bench profile_plugin_large_file \
  --features default_wasm_runtime,local_filesystem,__profile_wasm_memory
```

Set `BIN` to the emitted `target/release/deps/profile_plugin_large_file-*`
executable. The retained profile predates the harness rename, so its symbols
still use `profile_merge_10k`.
Use a fresh case directory for each backend and capacity/profile lane:

```sh
# Production 64 MiB capacity attempt: omit the diagnostic override.
/usr/bin/time -l env LIX_PROFILE_INITIAL_ROWS=220000 \
  "$BIN" rocksdb-fs setup /tmp/lix-v1-c789-rocks-64m

# Diagnostic setup, 11 alternating real edits, and 11 exact reads.
/usr/bin/time -l env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_WASM_MEMORY_MIB=256 \
  "$BIN" rocksdb-fs setup /tmp/lix-v1-c789-rocks

/usr/bin/time -l env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_ROUNDS=11 LIX_PROFILE_WASM_MEMORY_MIB=256 \
  "$BIN" rocksdb-fs edit /tmp/lix-v1-c789-rocks

/usr/bin/time -l env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_ROUNDS=11 LIX_PROFILE_WASM_MEMORY_MIB=256 \
  "$BIN" rocksdb-fs render /tmp/lix-v1-c789-rocks
```

For a real one-row I/O count, use a separate freshly set-up case whose current
blob is still the original:

```sh
env LIX_PROFILE_INITIAL_ROWS=220000 LIX_PROFILE_ROUNDS=1 \
  LIX_PROFILE_WASM_MEMORY_MIB=256 LIX_PROFILE_IO_STATS=1 \
  "$BIN" rocksdb-fs edit /tmp/lix-v1-c789-rocks-io
```

Replace `rocksdb-fs` with `slatedb-cached` and use another fresh directory for
the cached SlateDB lane. No SQLite command is part of this protocol.

Profile without wrapping the target in `/usr/bin/env`, because the
Apple-signed `env` binary prevents Samply from acquiring the task port:

```sh
LIX_PROFILE_INITIAL_ROWS=220000 LIX_PROFILE_ROUNDS=1 \
LIX_PROFILE_WASM_MEMORY_MIB=256 \
samply record --save-only --unstable-presymbolicate --rate 1000 \
  --profile-name full-engine-v1-rocksdb-fs-edit-220k-c789a2b1 \
  --output perf-results/plugin-api-v2/full-engine-v1-rocksdb-fs-edit-220k-c789a2b1.json.gz \
  -- "$BIN" rocksdb-fs edit /tmp/lix-v1-c789-rocks-profile

python3 perf-results/plugin-api-v2/analyze_samply.py \
  perf-results/plugin-api-v2/full-engine-v1-rocksdb-fs-edit-220k-c789a2b1.json.gz \
  --binary "$BIN"
```
