# Current v1 full-engine baseline on latest `main`

This artifact records the real, current Wasm CSV plugin through the ordinary
Lix SQL file API. It is a baseline, not a claim that the experimental v2 API
has already improved full-engine performance.

## Scope and controls

- Engine base: `66ad14dab21bcf3e3fc05bb0e132639a536823c7`
  (`origin/main` when measured).
- Research tree: `f233e1b277f89c9035276c01ed4ca57de4e76ca8`, plus
  benchmark-only raw-sample and storage-breakdown output. The engine algorithm
  is the `66ad14da` algorithm.
- Fixture: deterministic 220,000-row CSV, exactly 10,680,000 initial bytes.
- Edit: change one field in the middle row; alternating input makes all 11
  rounds real one-row transitions.
- API: ordinary `INSERT ... ON CONFLICT ... UPDATE` of the whole blob.
- Acknowledgement: exactly
  `SELECT data FROM lix_file WHERE path = $1` once before the measured edit
  series, modeling a session that receives the file and then makes consecutive
  edits. The point read and construction of both blobs are outside every edit
  timer.
- Backends: production `LocalFilesystem` over RocksDB (`rocksdb-fs`) and
  cached SlateDB with the Lixray per-workspace 64 MiB disk / 4 MiB block /
  1 MiB metadata cache policy.
- SQLite is not built, run, or used as evidence here. The direct raw RocksDB
  adapter is also excluded from the headline because it omits production
  `LocalFilesystem` synchronization.
- Runs were serial and uncontended on an Apple M5 Pro (18 cores, 64 GiB),
  macOS 26.3.1 arm64, `rustc 1.97.0-nightly (b954122bb)`, and Samply 0.13.1.

The real plugin needs more than the production 64 MiB guest-memory ceiling for
this fixture. Timed results use a benchmark-only 256 MiB guest ceiling applied
to both the SQL engine and the second engine owned by `LocalFilesystem` sync.
This keeps all code in Wasm, but is a diagnostic capacity override rather than
a proposed production setting.

## End-to-end latency

Times are harness intervals, not whole-process wall time. Cold initial write is
one sample. Edit and render each have 11 samples. With only 11 samples the
nearest-rank p95 is the maximum; the raw samples are retained below.

| Production backend | Cold initial write | Warm one-row edit p50 | Edit p95 | Exact render p50 | Render p95 |
|---|---:|---:|---:|---:|---:|
| RocksDB filesystem | 5,451.134 ms | 2,696.355 ms | 2,924.105 ms | 937.181 ms | 1,026.872 ms |
| Cached SlateDB | 4,345.035 ms | 4,092.013 ms | 4,241.217 ms | 4,190.029 ms | 4,917.752 ms |

Ranges:

| Production backend | Edit min-max | Render min-max |
|---|---:|---:|
| RocksDB filesystem | 2,542.936-2,924.105 ms | 901.725-1,026.872 ms |
| Cached SlateDB | 3,703.807-4,241.217 ms | 3,871.871-4,917.752 ms |

Sorted raw samples, in milliseconds:

```text
rocksdb-fs edit = [2542.935500, 2550.285750, 2557.581875, 2668.615084,
  2680.632125, 2696.354542, 2753.599666, 2894.409542, 2895.655250,
  2909.430000, 2924.104666]
rocksdb-fs render = [901.724834, 918.657500, 928.424667, 931.049875,
  934.723750, 937.181292, 938.488542, 958.938333, 984.573958,
  986.471084, 1026.872000]

slatedb-cached edit = [3703.806959, 3780.185750, 3858.577417,
  3980.822500, 4048.152500, 4092.013375, 4117.911583, 4125.817000,
  4152.311375, 4194.304375, 4241.217041]
slatedb-cached render = [3871.870917, 3946.920291, 3981.729959,
  4042.841333, 4164.727125, 4190.029250, 4279.232042, 4287.258083,
  4310.772458, 4795.428625, 4917.752292]
```

## Resident memory

Maximum resident set size comes from `/usr/bin/time -l` around the complete
process. It therefore includes the store, host, Wasmtime, guest, and setup or
shutdown outside the harness timer.

| Production backend | Cold setup max RSS | 11 edits max RSS | 11 renders max RSS |
|---|---:|---:|---:|
| RocksDB filesystem | 2,662,137,856 B (2.48 GiB) | 2,203,516,928 B (2.05 GiB) | 1,425,948,672 B (1.33 GiB) |
| Cached SlateDB | 2,381,905,920 B (2.22 GiB) | 2,118,467,584 B (1.97 GiB) | 1,160,298,496 B (1.08 GiB) |

The production 64 MiB guest limit failed during the initial 220k write on both
backends. The Wasm allocation failure currently crosses a `wasmtime-wasi`
error path that attempts to start a nested Tokio runtime, so stderr shows
`Cannot start a runtime from within a runtime` instead of the original guest
OOM. The failed whole processes reached 272,236,544 B RSS on RocksDB filesystem
and 259,424,256 B on cached SlateDB. This is both a capacity result and an
error-reporting defect; it is not evidence for raising the production limit.

## On-disk bytes

The numbers below are recursive directory sizes after each process flushes and
closes. SlateDB may compact between processes, so the post-render reduction is
expected and should not be interpreted as negative render write amplification.

| Backend / component | After initial write | After 11 edits | After 11 renders |
|---|---:|---:|---:|
| RocksDB filesystem, total | 75,426,249 B | 75,813,733 B | 76,287,475 B |
| Cached SlateDB, total | 193,069,332 B | 195,568,865 B | 134,028,680 B |
| Cached SlateDB, durable object store | 128,897,924 B | 130,351,288 B | 67,496,756 B |
| Cached SlateDB, local cache | 64,171,408 B | 65,217,577 B | 66,531,924 B |

The initial RocksDB filesystem total decomposes exactly into 62,671,445 B of
internal RocksDB state, the 10,680,000 B materialized CSV, the 2,074,802 B
installed plugin archive, and a 2 B `.gitignore`. Thus the current internal
state alone is 5.87x the source blob, while the complete filesystem is 7.06x.
Initial cached SlateDB durable state is 12.07x the blob; including its local
cache it is 18.08x.

These ratios do **not** show that the underlying KV stores need another generic
packing layer. RocksDB and SlateDB already pack keys into physical blocks. The
evidence points to reducing the number and lifetime of logical changelog/state
records and avoiding duplicate full-file materializations. Any packed logical
format must beat these results by the project's greater-than-20% threshold on
real dense and sparse workloads before adoption.

## Logical storage work for one row

Logical I/O was collected in separate single-edit runs. Counters reset after
the exact acknowledgement and immediately before the ordinary SQL write.
Instrumentation significantly perturbs SlateDB scheduling, so this section is
counts only; none of its timings are latency evidence.

| Counter | RocksDB filesystem | Cached SlateDB |
|---|---:|---:|
| read transactions | 9 | 9 |
| write transactions | 1 | 1 |
| `get_many` calls | 6,196 | 6,165 |
| requested keys | 226,391 | 226,360 |
| returned values | 226,384 | 226,353 |
| scans / returned entries | 11 / 24 | 11 / 23 |
| `put_many` calls / rows | 7 / 12 | 7 / 11 |
| delete calls / rows | 1 / 1 | 1 / 1 |

RocksDB filesystem reads by storage space:

| Space | Role | `get_many` calls | Keys | Values | Scans / entries |
|---|---|---:|---:|---:|---:|
| `0x0002_0001` | JSON store | 7 | 91 | 91 | 0 / 0 |
| `0x0004_0001` | tracked-state tree chunks | 6,118 | 6,118 | 6,118 | 0 / 0 |
| `0x0004_0004` | tracked-state commit roots | 20 | 20 | 20 | 0 / 0 |
| `0x0004_0006` | live-state index | 21 | 29 | 26 | 10 / 0 |
| `0x0006_0001` | commits | 2 | 2 | 1 | 1 / 24 |
| `0x0006_0002` | change payloads | 28 | 220,131 | 220,128 | 0 / 0 |

Cached SlateDB has the same shape: 220,131 requested change-payload keys, with
only the tree-chunk calls differing (6,087 rather than 6,118) because the fresh
fixture had a slightly different accumulated history. One changed CSV row thus
still causes current v1 to request more than the file's 220,000 semantic rows.

## Samply profiles

Save-only 1 kHz profiles are checked in with symbol sidecars:

- [`full-engine-v1-rocksdb-fs-edit-220k-66ad14da.json.gz`](./full-engine-v1-rocksdb-fs-edit-220k-66ad14da.json.gz)
- [`full-engine-v1-slatedb-cached-edit-220k-66ad14da.json.gz`](./full-engine-v1-slatedb-cached-edit-220k-66ad14da.json.gz)

The corresponding `.json.syms.json` files must remain beside them. Open either
with `samply load <profile>`. `analyze_samply.py` excludes parked/idle leaf
samples and can resolve the local Mach-O's image-relative addresses with
`--binary <profile-binary>`.

The async marker future did not survive as a native stack frame in this release
build, so filtering `--under profile_file_write_phase` returned no samples.
The table is therefore whole-process active-sample attribution: reopen, plugin
prewarm, exact acknowledged render, one timed write, and close/flush are all in
scope. Inclusive shares overlap and must not be summed. Profiled write times
(3,175 ms RocksDB filesystem and 4,661 ms cached SlateDB) include sampling
overhead and are not used in the latency table.

| Active inclusive frame | RocksDB filesystem | Cached SlateDB |
|---|---:|---:|
| active samples | 6,468 | 9,639 |
| `render_plugin_files_for_sql` | 70.56% | not a leading frame |
| `scan_tracked_branch_rows` | 48.47% | 8.00% |
| `LocalFilesystem::sync_from_lix` | 42.49% | n/a |
| `materialize_rows_from_index_entries` | 41.31% | 6.57% |
| `load_change_records` | 27.77% | 2.81% |
| `plugin_write_reconciliation` | 22.93% | 21.60% |
| RocksDB `DB::MultiGet` | 14.53% | n/a |
| SlateDB `get_snapshot_values` | n/a | 41.86% |
| SlateDB `SstIterator::init` | n/a | 23.60% |
| SlateDB SST fetch tasks | n/a | 20.70% |
| SlateDB internal iterator init / advance | n/a | 15.95% / 15.91% |
| SlateDB object `read_range` | n/a | 7.92% |
| `detect_changes_with_component_instance` | 1.39% | 1.13% |
| Wasmtime component `detect_changes` | 1.00% | 0.72% |

The RocksDB profile has 42.6% of active samples on the filesystem-sync thread,
which independently renders Lix state to disk. Cached SlateDB spends 71.0% of
active samples on Tokio workers, primarily fetching individual snapshot values
through SST iterators.

## What this baseline establishes

1. The large-file cost is not mainly the single Wasm function invocation.
   Wasmtime `detect_changes` accounts for about 1% of active samples, after the
   host has already materialized and transferred the world it needs.
2. The architectural problem is whole-state work: a one-row edit requests
   roughly 226k keys and materializes/render states proportional to file size.
3. A packed Component Model ABI alone cannot remove that host and storage work.
   An integrated v2 must let a persistent Wasm document/index retain stable
   entity identity and return only a delta, while the host preserves the
   authoritative entity/changelog transaction.
4. SlateDB additionally needs adaptive batched/range retrieval. Its current
   per-key snapshot lookup and SST initialization dominate its profile, but a
   broad range must not be forced for sparse keys.
5. `LocalFilesystem` must avoid a second full semantic render when the write
   path already has an exact rendered blob or splice result. Correct
   acknowledgement and transaction ordering still govern whether that result
   can be reused.

No proposal should claim the project's greater-than-20% performance/storage
bar from the isolated mechanism benchmark alone. The next gate is an integrated
current-v1 versus v2 A/B using this same fixture, production backends, exact
acknowledgement, 64 MiB production-limit reporting, RSS, durable bytes, and
dense plus sparse edit distributions.

## Reproduction

Build:

```sh
cargo build --release -p lix_sdk \
  --bench profile_merge_10k \
  --features default_wasm_runtime,local_filesystem,profile_wasm_memory
```

Set `BIN` to the emitted `target/release/deps/profile_merge_10k-*` executable.
Use a fresh case directory per backend:

```sh
/usr/bin/time -l env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_WASM_MEMORY_MIB=256 \
  "$BIN" rocksdb-fs setup /tmp/lix-v1-rocks

/usr/bin/time -l env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_ROUNDS=11 LIX_PROFILE_WASM_MEMORY_MIB=256 \
  "$BIN" rocksdb-fs edit /tmp/lix-v1-rocks

/usr/bin/time -l env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_ROUNDS=11 LIX_PROFILE_WASM_MEMORY_MIB=256 \
  "$BIN" rocksdb-fs render /tmp/lix-v1-rocks

env LIX_PROFILE_INITIAL_ROWS=220000 LIX_PROFILE_ROUNDS=1 \
  LIX_PROFILE_WASM_MEMORY_MIB=256 LIX_PROFILE_IO_STATS=1 \
  "$BIN" rocksdb-fs edit /tmp/lix-v1-rocks
```

Replace `rocksdb-fs` with `slatedb-cached` and use another fresh directory for
the SlateDB lane. Omit `LIX_PROFILE_WASM_MEMORY_MIB` in a new setup case to
exercise the production 64 MiB ceiling.

Profile without wrapping the target in `/usr/bin/env` (the Apple-signed `env`
binary prevents Samply from acquiring the task port):

```sh
LIX_PROFILE_INITIAL_ROWS=220000 LIX_PROFILE_ROUNDS=1 \
LIX_PROFILE_WASM_MEMORY_MIB=256 \
samply record --save-only --unstable-presymbolicate --rate 1000 \
  --profile-name full-engine-v1-rocksdb-fs-edit-220k-66ad14da \
  -- "$BIN" rocksdb-fs edit /tmp/lix-v1-rocks-profile

python3 perf-results/plugin-api-v2/analyze_samply.py \
  perf-results/plugin-api-v2/full-engine-v1-rocksdb-fs-edit-220k-66ad14da.json.gz \
  --binary "$BIN"
```
