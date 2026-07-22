# Current v1 full-engine baseline on `5ffab346`

This artifact records the real current Wasm CSV plugin through the ordinary
Lix SQL file API on the latest `origin/main` available when the run started. It
compares the directly relevant SQL/storage optimizations in `5ffab346` with the
same coherent large-file baseline on `c789a2b1`. It is a baseline, not a claim
that the experimental incremental plugin API has already improved full-engine
performance.

## Scope and controls

- Engine base: `5ffab346508249f4c83b179bfd0c7e09130c93e7`
  (`origin/main` when measured).
- Comparison base: `c789a2b10aada60fdd5707c247d9ef26136837ab`.
- Benchmark support: only the committed profiling harness/runtime exposure from
  research commits `907293f1` and `5a5ee964`; no research engine algorithm is
  present in the measured binary.
- Fixture: deterministic 220,000-row CSV, exactly 10,680,000 initial bytes.
- Edit: append `-edited` to the middle row; alternating input makes all 11
  measured rounds real one-row transitions.
- API: ordinary `INSERT ... ON CONFLICT ... UPDATE` of the whole blob.
- Acknowledgement: exactly `SELECT data FROM lix_file WHERE path = $1` once
  before the measured edit series. The point read and both blob constructions
  are outside every edit timer.
- Backends: production `LocalFilesystem` over RocksDB (`rocksdb-fs`) and cached
  SlateDB using Lixray's per-workspace 64 MiB disk / 4 MiB block / 1 MiB
  metadata cache policy.
- SQLite was not built, run, or used as evidence. The direct raw RocksDB adapter
  is also excluded because it omits production `LocalFilesystem`
  synchronization.
- Runs were serial on an Apple M5 Pro (18 cores, 64 GiB), macOS 26.3.1 arm64,
  `rustc 1.97.0-nightly (b954122bb)`, and Samply 0.13.1.

Every accepted latency phase had a zero-Cargo/rustc/profiler check both before
and after it. Several earlier attempts were discarded when unrelated local
builds started between those checks. The retained RocksDB and SlateDB lanes ran
inside an explicitly coordinated no-build window. With only 11 samples,
nearest-rank p95 is the maximum and is diagnostic rather than a stable tail
estimate.

The real plugin still exceeds the production 64 MiB guest-memory ceiling on
this fixture. Timed results use a benchmark-only 256 MiB ceiling applied to
both the SQL engine and the second engine owned by `LocalFilesystem` sync. All
plugin execution remains Wasm; this is a diagnostic capacity override, not a
proposed production setting.

## Capacity at the production policy

Fresh 64 MiB initial writes failed on both backends before completing. The
allocation failure still crosses a `wasmtime-wasi` error path that attempts to
start a nested Tokio runtime, so stderr reports `Cannot start a runtime from
within a runtime` instead of the underlying guest allocation error.

| Backend | Exit | Wall to failure | Maximum RSS | `c789a2b1` maximum RSS |
|---|---:|---:|---:|---:|
| RocksDB filesystem | 101 | 2.13 s | 269,074,432 B | 280,641,536 B |
| Cached SlateDB | 101 | 0.95 s | 256,344,064 B | 261,439,488 B |

Wall time to failure is diagnostic only. The capacity verdict is unchanged,
and these failures are not evidence for raising the production limit.

## End-to-end latency

Times are harness intervals, not whole-process wall time. Cold initial write is
one sample. Edit and exact render each have 11 samples.

| Production backend | Cold initial write | Warm one-row edit p50 | Edit p95/max | Exact render p50 | Render p95/max |
|---|---:|---:|---:|---:|---:|
| RocksDB filesystem | 5,455.894 ms | 2,502.882 ms | 2,813.046 ms | 900.242 ms | 1,248.214 ms |
| Cached SlateDB | 4,687.047 ms | 4,118.763 ms | 4,947.267 ms | 4,161.882 ms | 4,856.086 ms |

Recurring medians relative to the coherent `c789a2b1` run:

| Production backend | Cold setup delta | Edit p50 delta | Render p50 delta |
|---|---:|---:|---:|
| RocksDB filesystem | +11.85% | **-5.39%** | **-2.79%** |
| Cached SlateDB | -39.07% | +3.73% | **-1.00%** |

The cold setup cells are single samples and moved in opposite directions, so
they are not attributable evidence. None of the repeated edit/render medians
improved by 10%, let alone the project's greater-than-20% adoption threshold.

Sorted raw samples, in milliseconds:

```text
rocksdb-fs edit = [2375.645125, 2406.728792, 2435.905833, 2441.551709,
  2471.292084, 2502.881792, 2505.221334, 2635.414042, 2637.179042,
  2704.887500, 2813.045833]
rocksdb-fs render = [880.234750, 884.899666, 886.978583, 890.580125,
  895.773125, 900.241917, 904.027958, 975.811583, 1017.687459,
  1080.882208, 1248.214417]

slatedb-cached edit = [3939.565083, 3954.356334, 3984.213041,
  4057.984667, 4113.850084, 4118.763209, 4278.185417, 4345.749417,
  4375.892834, 4735.876708, 4947.266667]
slatedb-cached render = [3952.786334, 4041.598625, 4045.281917,
  4056.324166, 4144.143083, 4161.881750, 4321.906875, 4336.978666,
  4351.497291, 4548.596000, 4856.086084]
```

## Resident memory

Maximum resident set size comes from `/usr/bin/time -l` around the complete
process, including reopen, prewarm, host, store, Wasmtime, guest, and shutdown.

| Production backend | Cold setup max RSS | 11 edits max RSS | 11 renders max RSS |
|---|---:|---:|---:|
| RocksDB filesystem | 2,683,387,904 B | 2,065,367,040 B | 1,449,787,392 B |
| Cached SlateDB | 2,243,362,816 B | 2,180,481,024 B | 1,202,225,152 B |

Relative to `c789a2b1`, RocksDB RSS changed -0.47% / -1.63% / +3.87%
for setup/edit/render. SlateDB changed -9.51% / +8.09% / -7.48%. The mixed
direction and sub-10% magnitudes do not establish a material memory change.

## On-disk bytes

Numbers are recursive directory sizes after each process flushes and closes.
SlateDB compaction may move bytes between its durable object store and local
cache across processes.

| Backend / component | After initial write | After 11 edits | After 11 renders |
|---|---:|---:|---:|
| RocksDB filesystem, total | 75,426,794 B | 75,823,080 B | 76,294,118 B |
| RocksDB internal state | 62,671,990 B | 63,068,269 B | 63,539,307 B |
| Materialized CSV | 10,680,000 B | 10,680,007 B | 10,680,007 B |
| Installed plugin archive | 2,074,802 B | 2,074,802 B | 2,074,802 B |
| Cached SlateDB, total | 193,079,122 B | 196,526,091 B | 189,846,562 B |
| Cached SlateDB, durable object store | 128,904,432 B | 130,351,806 B | 131,664,584 B |
| Cached SlateDB, local cache | 64,174,690 B | 66,174,285 B | 58,181,978 B |

Initial RocksDB bytes differ from `c789a2b1` by +0.016%; initial cached
SlateDB bytes differ by -0.016%. RocksDB internal state remains 5.87x the
source blob and its complete filesystem remains 7.06x. Cached SlateDB durable
state is 12.07x the blob and total initial bytes are 18.08x. The new direct
write optimizations do not materially change storage amplification.

These results still do not justify packing individual logical KVs into generic
storage packs. RocksDB and SlateDB already physically block-pack keys. The
measured issue is the number and lifetime of logical changelog/state records,
plus duplicate full-file materializations.

## Logical storage work for one row

Logical I/O was collected in separate fresh single-edit cases. Counters reset
after the exact acknowledgement and immediately before the ordinary SQL write.
Instrumentation timings are not latency evidence.

| Counter | RocksDB filesystem | Cached SlateDB | `c789a2b1` Rocks / Slate |
|---|---:|---:|---:|
| read transactions | 9 | 9 | 9 / 9 |
| write transactions | 1 | 1 | 1 / 1 |
| `get_many` calls | 6,208 | 6,183 | 6,154 / 6,155 |
| requested keys | 226,339 | 226,314 | 226,349 / 226,350 |
| returned values | 226,332 | 226,307 | 226,342 / 226,343 |
| scans / returned entries | 3 / 6 | 3 / 5 | 11 / 6, 11 / 5 |
| `put_many` calls / rows | 7 / 12 | 7 / 11 | 7 / 11, 7 / 11 |
| delete calls / rows | 1 / 1 | 1 / 1 | 1 / 1, 1 / 1 |

RocksDB filesystem reads by storage space:

| Space | Role | Calls | Keys | Values | Scans / entries |
|---|---|---:|---:|---:|---:|
| `0x0002_0001` | JSON store | 5 | 61 | 61 | 0 / 0 |
| `0x0004_0001` | tracked-state tree chunks | 6,139 | 6,139 | 6,139 | 0 / 0 |
| `0x0004_0004` | tracked-state commit roots | 18 | 18 | 18 | 0 / 0 |
| `0x0004_0006` | live-state index | 19 | 26 | 23 | 2 / 0 |
| `0x0006_0001` | commits | 2 | 2 | 1 | 1 / 6 |
| `0x0006_0002` | change payloads | 24 | 220,092 | 220,089 | 0 / 0 |
| `0x0007_0002` | filesystem path index | 1 | 1 | 1 | 0 / 0 |

Cached SlateDB has the same shape, with 6,114 tree-chunk calls and five commit
scan entries. The new indexed-path and validation shortcuts collapse scans
from 11 to 3 and reduce a few metadata/change keys, but total requested keys
fall by only 10 on RocksDB and 36 on SlateDB: less than 0.02%. A one-row edit
still requests 220,092 change-payload keys. This explains why direct SQL write
optimizations do not materially move the large plugin-backed file median.

## Samply profile

The clean RocksDB one-edit profile and symbol sidecar are retained beside this
report:

- `full-engine-v1-rocksdb-fs-edit-220k-5ffab346.json.gz`
- `full-engine-v1-rocksdb-fs-edit-220k-5ffab346.json.syms.json`

The profile contains 13,775 samples, of which 7,029 are classified active.
The profiled edit timer was 2,631.866 ms; sampling overhead makes this
descriptive rather than latency-table evidence. The post-capture process check
found only an SSH client at 0.1% local CPU provisioning another machine; its
remote command text matched the Cargo/rustc filter, but no compiler was running
locally.

The async marker future again did not survive as a native stack frame, so
`--under profile_file_write_phase` finds zero samples. The table is therefore
whole-process active-sample attribution: reopen, plugin prewarm, exact
acknowledged render, one timed write, filesystem sync, and close/flush are all
in scope. Inclusive shares overlap and must not be summed.

| Active inclusive frame | Samples | Share of active samples | `c789a2b1` share |
|---|---:|---:|---:|
| `render_plugin_files_for_sql` | 4,082 | 58.07% | 61.86% |
| `scan_tracked_branch_rows` | 3,123 | 44.43% | 48.92% |
| `materialize_rows_from_index_entries` | 2,638 | 37.53% | 41.91% |
| `LocalFilesystem::sync_from_lix` | 2,618 | 37.25% | 41.92% |
| `load_change_records` | 1,736 | 24.70% | 28.78% |
| `plugin_write_reconciliation` | 1,654 | 23.53% | 20.38% |
| Wasm component `render` | 1,388 | 19.75% | 15.14% |
| RocksDB `DB::MultiGet` | 984 | 14.00% | 18.30% |
| `detect_changes_with_component_instance` | 495 | 7.04% | 1.55% |
| Wasmtime component `detect_changes` | 474 | 6.74% | 1.27% |

Frame-share movement is not an A/B latency decomposition: whole-process sample
denominators differ, inclusive frames overlap, and one component becoming a
larger share can simply mean other work shrank. The absolute latency and
logical-I/O results remain the decision evidence.

The cached SlateDB profile is **pending**, not silently absent. Its fixture and
capture were deliberately not started after the coordinated no-build window
ended and another CPU-heavy AX evaluation began. No Slate profile claim should
be made from this artifact until a fresh clean window produces the matching
`5ffab346` profile and sidecar.

## What this baseline establishes

1. The newest direct SQL/write optimizations are real but target secondary
   work for a 10.68 MB / 10.19 MiB plugin-backed edit: scans drop from 11 to 3,
   while total requested keys and recurring medians barely move.
2. The architectural bottleneck remains whole-state work. One changed CSV row
   still requests roughly 226k keys, including 220,092 change payloads, and
   materializes/renders state proportional to the complete file.
3. A packed Component Model ABI alone cannot remove host storage and
   materialization work. An integrated incremental API must avoid loading the
   complete active entity set and let a persistent or indexed Wasm document
   return only a delta while the host remains authoritative for transactions.
4. RocksDB's profile still has a second full semantic render on the filesystem
   sync thread. Reusing an exact rendered blob or validated splice remains a
   distinct high-impact target.
5. The per-file storage amplification is logical, not evidence that the KV
   engines need generic repacking. Any logical covering/index format must beat
   the project's greater-than-20% threshold on both dense and sparse workloads.
6. SlateDB's large recurring latency remains a separate retrieval problem. The
   matching `5ffab346` profile is pending, but current I/O counts still expose
   220k point keys for one changed row.

## Reproduction

Build:

```sh
cargo build --release -p lix_sdk \
  --bench profile_plugin_large_file \
  --features default_wasm_runtime,local_filesystem,__profile_wasm_memory
```

Set `BIN` to the emitted `target/release/deps/profile_plugin_large_file-*`
executable. The retained profile was captured before the research branch renamed
the harness, so its symbols still use `profile_merge_10k`.
Use a fresh case directory for each backend and evidence type:

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
```

Replace `rocksdb-fs` with `slatedb-cached` and use a different directory for
the SlateDB lane. Omit `LIX_PROFILE_WASM_MEMORY_MIB` in another fresh setup case
to exercise the production 64 MiB ceiling.

Logical I/O:

```sh
env LIX_PROFILE_INITIAL_ROWS=220000 LIX_PROFILE_ROUNDS=1 \
  LIX_PROFILE_WASM_MEMORY_MIB=256 LIX_PROFILE_IO_STATS=1 \
  "$BIN" rocksdb-fs edit /tmp/lix-v1-rocks-io
```

Profile without wrapping the target itself in `/usr/bin/env`:

```sh
LIX_PROFILE_INITIAL_ROWS=220000 LIX_PROFILE_ROUNDS=1 \
LIX_PROFILE_WASM_MEMORY_MIB=256 \
samply record --save-only --unstable-presymbolicate --rate 1000 \
  --profile-name full-engine-v1-rocksdb-fs-edit-220k-5ffab346 \
  --output perf-results/plugin-api-v2/full-engine-v1-rocksdb-fs-edit-220k-5ffab346.json.gz \
  -- "$BIN" rocksdb-fs edit /tmp/lix-v1-rocks-profile

python3 perf-results/plugin-api-v2/analyze_samply.py \
  perf-results/plugin-api-v2/full-engine-v1-rocksdb-fs-edit-220k-5ffab346.json.gz \
  --binary "$BIN"
```
