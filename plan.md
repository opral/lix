# Binary Blob Fallback Plan

## Goal

- Add a minimal, SQL-only binary fallback path so unknown files (for example `mp4`, `png`, `gif`) for which no plugin is installed are stored efficiently and predictably.

## Requirements

- Storage must be only the SQL database (no external object store).
- Fallback behavior must be explicit in the plugin model.
- Specialized plugins can still exist for binary types and should take precedence over fallback when present.
- Minimal prototype first, measurable with replay/storage benchmarks.

## Solutions To Look At

- Fallback plugin contract:
  Define and register a built-in `binary_fallback` plugin used only when no other plugin matches.
- SQL storage model:
  Store bytes in a dedicated binary table (`file_id`, `version_id`, `data BLOB`, metadata columns as needed), with clear ownership and GC semantics.
- Dedup strategy (in-DB):
  Add content-addressed dedup (`sha256 -> blob`) and reference mapping from file/version rows to blob rows.
- Compression policy:
  Use per-blob compression flags; skip recompression for already-compressed media formats.
- Materialization path:
  Ensure reads/writes route through fallback plugin for unmatched files and preserve exact bytes.
- Observability:
  Track storage by extension/plugin and dedup ratio to validate impact.

## Benchmarking Logic (Engine-First)

- Scope:
  Benchmark in `packages/engine` first; replay-based benchmarks are secondary validation.
- Benchmark shape:
  Use deterministic synthetic datasets and file-backed SQLite so storage metrics are real.
- Core workloads:
  `ingest_binary_cold`, `update_binary_hot`, `read_point_binary`, `read_scan_binary`, and one mixed workload (`70%` read, `20%` update, `10%` insert).
- Dataset classes:
  incompressible/random bytes, already-compressed media-like bytes, and append-friendly bytes.

### Required Metrics

- Performance:
  `p50/p95 latency`, `ops/sec`, and CPU time per MB ingested.
- Storage:
  total DB bytes (`page_size * page_count`), table/index split (`dbstat`), bytes in binary cache table, and row counts.
- Efficiency:
  `storage_amp = db_bytes / logical_materialized_bytes`
  `write_amp = delta_db_bytes / bytes_written`

### Pareto Decision Rule

- Compare each candidate strategy (for example: raw blob, dedup-only, dedup+compression) on both performance and storage.
- A strategy dominates another if:
  it is no worse on all key metrics and strictly better on at least one.
- Keep only Pareto-frontier candidates; reject dominated ones.
- Final choice guidance:
  prefer the frontier point with largest storage reduction that stays within agreed latency guardrails (for example no more than `+10-15%` p95 regression on critical reads/updates).

## Baseline (Current, Naive `plugin-binary`)

- Report: `packages/engine/benches/results/binary-storage-report.json`
- DB artifact: `packages/engine/benches/results/binary-storage-1771543700033.sqlite`

### Bench Config

| Metric          | Value  |
| --------------- | ------ |
| files_per_class | 32     |
| total_files     | 96     |
| base_blob_bytes | 65,536 |
| update_rounds   | 2      |
| point_read_ops  | 500    |
| scan_read_ops   | 8      |

### Compute Timings

| Workload           | Ops | Bytes Written | Bytes Read |   Wall (ms) | p50 (ms) |  p95 (ms) |  Ops/s |
| ------------------ | --: | ------------: | ---------: | ----------: | -------: | --------: | -----: |
| ingest_binary_cold |  96 |     6,291,456 |          0 |   1,230.734 |   11.795 |    13.489 | 78.002 |
| update_binary_hot  | 192 |    12,976,128 |          0 |  93,003.927 |  482.403 |   526.954 |  2.064 |
| read_point_binary  | 500 |             0 | 34,119,680 | 117,890.028 |  200.128 |   337.982 |  4.241 |
| read_scan_binary   |   8 |             0 | 52,428,800 |   5,429.370 |  238.557 | 3,777.600 |  1.473 |

### Storage Footprint

| Snapshot     |   DB Bytes | Table Bytes | Index Bytes | File Data Cache Bytes | Freelist |
| ------------ | ---------: | ----------: | ----------: | --------------------: | -------: |
| baseline     |  6,348,800 |   5,689,344 |     598,016 |                     0 |        0 |
| after_ingest | 30,502,912 |  29,417,472 |   1,024,000 |             6,291,456 |        0 |
| after_update | 49,332,224 |  47,894,528 |   1,286,144 |             6,553,600 |       22 |
| after_reads  | 58,523,648 |  56,991,744 |   1,470,464 |             6,553,600 |        0 |

### Derived Efficiency Metrics

| Metric                   | Value |
| ------------------------ | ----: |
| ingest_write_amp         | 3.839 |
| update_write_amp         | 1.451 |
| storage_amp_after_update | 7.527 |

This is the optimization baseline to beat.

## Current (Engine Internal Fallback, Option 2)

- Report: `packages/engine/benches/results/binary-storage-report.json`
- DB artifact: `packages/engine/benches/results/binary-storage-1771548261890.sqlite`
- Config: same as baseline (`files_per_class=32`, `base_blob_bytes=65536`, `update_rounds=2`, `point_read_ops=500`, `scan_read_ops=8`)

### Delta Vs Baseline

| Metric             | Baseline | Current | Delta |
| ------------------ | -------: | ------: | ----: |
| ingest wall (ms)   | 1230.734 | 941.959 | -23.46% |
| update wall (ms)   | 93003.927 | 1769.325 | -98.10% |
| read point wall (ms) | 117890.028 | 111385.573 | -5.52% |
| read scan wall (ms) | 5429.370 | 1750.026 | -67.77% |
| ingest ops/s       | 78.002 | 101.915 | +30.66% |
| update ops/s       | 2.064 | 108.516 | +5157.56% |
| read point ops/s   | 4.241 | 4.489 | +5.85% |
| read scan ops/s    | 1.473 | 4.571 | +210.34% |

### Storage Delta Vs Baseline

| Metric                    | Baseline | Current | Delta |
| ------------------------- | -------: | ------: | ----: |
| DB bytes after ingest     | 30,502,912 | 14,475,264 | -52.54% |
| DB bytes after update     | 49,332,224 | 28,647,424 | -41.93% |
| DB bytes after reads      | 58,523,648 | 29,118,464 | -50.24% |
| table bytes after update  | 47,894,528 | 27,160,576 | -43.29% |
| index bytes after update  | 1,286,144 | 1,351,680 | +5.10% |
| ingest_write_amp          | 3.839 | 2.171 | -43.44% |
| update_write_amp          | 1.451 | 1.092 | -24.73% |
| storage_amp_after_update  | 7.527 | 4.371 | -41.93% |

## Current (Custom FastCDC-like Chunking, Pre-Crate)

- Report: `packages/engine/benches/results/binary-storage-report.json`
- DB artifact: `packages/engine/benches/results/binary-storage-1771550854278.sqlite`
- Config: same as baseline (`files_per_class=32`, `base_blob_bytes=65536`, `update_rounds=2`, `point_read_ops=500`, `scan_read_ops=8`)

### Delta Vs Option 2 (No Chunking)

| Metric               | Option 2 | Pre-Crate FastCDC | Delta |
| -------------------- | -------: | ----------------: | ----: |
| ingest wall (ms)     | 941.959 | 798.827 | -15.20% |
| update wall (ms)     | 1769.325 | 1872.277 | +5.82% |
| read point wall (ms) | 111385.573 | 114651.088 | +2.93% |
| read scan wall (ms)  | 1750.026 | 1597.142 | -8.74% |
| ingest ops/s         | 101.915 | 120.176 | +17.92% |
| update ops/s         | 108.516 | 102.549 | -5.50% |
| read point ops/s     | 4.489 | 4.361 | -2.86% |
| read scan ops/s      | 4.571 | 5.009 | +9.58% |

### Storage Delta Vs Option 2 (No Chunking)

| Metric                    | Option 2 | Pre-Crate FastCDC | Delta |
| ------------------------- | -------: | ----------------: | ----: |
| DB bytes after ingest     | 14,475,264 | 14,581,760 | +0.74% |
| DB bytes after update     | 28,647,424 | 28,848,128 | +0.70% |
| DB bytes after reads      | 29,118,464 | 29,319,168 | +0.69% |
| table bytes after update  | 27,160,576 | 27,234,304 | +0.27% |
| index bytes after update  | 1,351,680 | 1,478,656 | +9.39% |
| ingest_write_amp          | 2.171 | 2.183 | +0.55% |
| update_write_amp          | 1.092 | 1.099 | +0.64% |
| storage_amp_after_update  | 4.371 | 4.402 | +0.70% |

## FastCDC Crate (v3.2.1)

- Report: `packages/engine/benches/results/binary-storage-report.json`
- DB artifact: `packages/engine/benches/results/binary-storage-1771551593619.sqlite`
- Config: same as baseline (`files_per_class=32`, `base_blob_bytes=65536`, `update_rounds=2`, `point_read_ops=500`, `scan_read_ops=8`)

### Delta Vs Pre-Crate FastCDC-like

| Metric               | Pre-Crate FastCDC | Crate FastCDC | Delta |
| -------------------- | ----------------: | ------------: | ----: |
| ingest wall (ms)     | 798.827 | 875.764 | +9.63% |
| update wall (ms)     | 1872.277 | 1910.233 | +2.03% |
| read point wall (ms) | 114651.088 | 116485.415 | +1.60% |
| read scan wall (ms)  | 1597.142 | 1732.878 | +8.50% |
| ingest ops/s         | 120.176 | 109.619 | -8.78% |
| update ops/s         | 102.549 | 100.511 | -1.99% |
| read point ops/s     | 4.361 | 4.292 | -1.58% |
| read scan ops/s      | 5.009 | 4.617 | -7.83% |

### Storage Delta Vs Pre-Crate FastCDC-like

| Metric                    | Pre-Crate FastCDC | Crate FastCDC | Delta |
| ------------------------- | ----------------: | ------------: | ----: |
| DB bytes after ingest     | 14,581,760 | 14,630,912 | +0.34% |
| DB bytes after update     | 28,848,128 | 28,975,104 | +0.44% |
| DB bytes after reads      | 29,319,168 | 29,446,144 | +0.43% |
| table bytes after update  | 27,234,304 | 27,344,896 | +0.41% |
| index bytes after update  | 1,478,656 | 1,495,040 | +1.11% |
| ingest_write_amp          | 2.183 | 2.191 | +0.36% |
| update_write_amp          | 1.099 | 1.105 | +0.55% |
| storage_amp_after_update  | 4.402 | 4.421 | +0.44% |

## Benchmark Redesign (Up To 4 MiB)

- The benchmark in `packages/engine/benches/binary_storage.rs` was updated to use `profile=binary_4mb_focus` by default.
- This profile targets realistic binary sizes up to `4 MiB` and a mixed update pattern (`localized`, `append`, `rewrite`).
- Earlier benchmark sections in this file were run on the old small-file profile and are no longer decision-grade for FastCDC evaluation.
- Earlier numbers are kept for historical context only.

### Current 4 MiB Profile Snapshot

| Metric            | Value |
| ----------------- | ----: |
| total_files       | 96 |
| total_bytes       | 63,103,667 |
| p50_file_bytes    | 214,670 |
| p80_file_bytes    | 925,946 |
| p95_file_bytes    | 3,133,734 |
| max_file_bytes    | 4,124,547 |
| files <= 256 KiB  | 53 |
| files <= 1 MiB    | 82 |
| files <= 4 MiB    | 96 |

### Pre/Post FastCDC on 4 MiB Profile

- Pre-FastCDC (single-chunk mode):
  `packages/engine/benches/results/binary-storage-report-no-fastcdc-4mb.json`
  (`db: packages/engine/benches/results/binary-storage-no-fastcdc-4mb.sqlite`)
- Post-FastCDC:
  `packages/engine/benches/results/binary-storage-report-fastcdc-4mb.json`
  (`db: packages/engine/benches/results/binary-storage-fastcdc-4mb.sqlite`)

### Delta (Post-FastCDC Vs Pre-FastCDC)

| Metric                    | Pre-FastCDC | Post-FastCDC | Delta |
| ------------------------- | ----------: | -----------: | ----: |
| storage_amp_after_update  | 3.952 | 2.399 | -39.30% |
| ingest_write_amp          | 2.021 | 1.639 | -18.89% |
| update_write_amp          | 1.046 | 0.439 | -58.02% |
| DB bytes after ingest     | 128,356,352 | 104,271,872 | -18.76% |
| DB bytes after update     | 269,123,584 | 163,360,768 | -39.30% |
| DB bytes after reads      | 269,664,256 | 163,360,768 | -39.42% |
| ingest wall (ms)          | 1017.198 | 1242.264 | +22.13% |
| update wall (ms)          | 2219.181 | 2667.707 | +20.21% |
| read point wall (ms)      | 111015.706 | 112894.842 | +1.69% |
| read scan wall (ms)       | 2737.350 | 2631.966 | -3.85% |

### Dedup Diagnostics (4 MiB Profile)

| Metric               | Pre-FastCDC | Post-FastCDC |
| -------------------- | ----------: | -----------: |
| avg_chunks_per_blob  | 1.000 | 4.792 |
| chunk_reuse_rate     | 0.000 | 0.454 |
| bytes_dedup_saved    | 0 | 107,225,831 |

## Experiment: Bounded Binary Read Cache (`<=256 KiB`)

- Goal: test whether we can keep `lix_internal_file_data_cache` non-authoritative and bounded so storage stays low after read-heavy workloads.
- Setup:
  `packages/engine/src/execute/side_effects.rs` keeps write-time cache disabled.
  `packages/engine/src/execute/entry.rs` prunes large binary cache rows (`length(data) > 256 KiB`) after read-only queries.
- Report:
  `packages/engine/benches/results/binary-storage-report-bounded-read-cache-4mb.json`
- DB artifact:
  `packages/engine/benches/results/binary-storage-bounded-read-cache-4mb.sqlite`

### Delta Vs FastCDC 4 MiB Baseline

| Metric                          | FastCDC Baseline | Bounded Read Cache | Delta |
| ------------------------------- | ---------------: | -----------------: | ----: |
| DB bytes after update           | 163,360,768 | 94,273,536 | -42.29% |
| DB bytes after reads            | 163,360,768 | 163,065,856 | -0.18% |
| table bytes after reads         | 161,062,912 | 97,886,208 | -39.22% |
| file_data_cache bytes after reads | 68,092,152 | 5,032,913 | -92.61% |
| freelist pages after reads      | 67 | 15,423 | +22919.40% |
| read_point wall (ms)            | 112,894.842 | 261,148.138 | +131.32% |
| read_point p95 (ms)             | 406.562 | 570.419 | +40.30% |
| read_scan wall (ms)             | 2,631.966 | 5,299.681 | +101.36% |
| read_scan p95 (ms)              | 344.719 | 685.114 | +98.75% |

### Outcome

- Storage target:
  yes for logical table footprint after reads (`-39.22%`) and cache bytes (`-92.61%`).
- Read performance target:
  no, this approach is too expensive in current form (`read_point p95 +40.30%`, `read_scan p95 +98.75%`).
- Important detail:
  DB file bytes stayed almost flat after reads because deleted cache pages moved to freelist instead of shrinking the file.
  A manual `VACUUM` on this DB reduced size from `163,065,856` to `100,159,488` bytes (`-38.69%`).
- Note:
  benchmark report values are pre-`VACUUM`; the DB artifact was vacuumed once to validate reclaim potential.

Conclusion: bounded non-authoritative cache can deliver the storage win, but we need direct CAS read path (avoid write-then-delete churn) to avoid large read regressions.

## Experiment: Zstd-Compressed CAS Chunks (FastCDC + BLAKE3)

- Goal: test chunk-level zstd compression in the current engine CAS pipeline for opaque binaries.
- Implementation:
  `packages/engine/src/execute/side_effects.rs` now frames each chunk as either raw (`LIXRAW01`) or zstd (`LIXZSTD1`) and stores the smaller payload.
  `packages/engine/src/plugin/runtime.rs` decodes framed chunk payloads during materialization, with backward compatibility for legacy unframed rows.
- Report:
  `packages/engine/benches/results/binary-storage-report-zstd-4mb.json`
- DB artifact:
  `packages/engine/benches/results/binary-storage-zstd-4mb.sqlite`

### Delta Vs Current Baseline (No-Zstd CAS)

- Baseline report:
  `packages/engine/benches/results/binary-storage-report-no-write-cache-4mb.json`

| Metric                    | No-Zstd Baseline | Zstd CAS | Delta |
| ------------------------- | ---------------: | -------: | ----: |
| DB bytes after ingest     | 41,021,440 | 16,007,168 | -60.98% |
| DB bytes after update     | 94,273,536 | 35,852,288 | -61.97% |
| DB bytes after reads      | 163,057,664 | 104,636,416 | -35.83% |
| table bytes after update  | 92,446,720 | 34,025,472 | -63.19% |
| chunk_store_bytes         | 90,451,780 | 32,117,457 | -64.49% |
| bytes_dedup_saved         | 107,225,831 | 165,560,154 | +54.40% |
| ingest wall (ms)          | 1188.048 | 1430.321 | +20.39% |
| update wall (ms)          | 2704.615 | 2583.577 | -4.48% |
| read_point wall (ms)      | 110315.525 | 127442.426 | +15.53% |
| read_scan wall (ms)       | 2855.870 | 3004.858 | +5.22% |
| read_point p95 (ms)       | 487.975 | 622.898 | +27.65% |
| read_scan p95 (ms)        | 454.547 | 458.508 | +0.87% |
| ingest_write_amp          | 0.637 | 0.240 | -62.27% |
| update_write_amp          | 0.396 | 0.147 | -62.73% |

### Outcome

- Storage target:
  met with high margin (`-61.97%` after update, `-35.83%` sustained after reads).
- Performance trade-off:
  ingest and read-point got slower (`ingest +20.39%`, `read_point p95 +27.65%`), while update improved modestly (`-4.48%`) and read-scan p95 stayed nearly flat (`+0.87%`).

Conclusion: zstd chunk compression is a high-impact storage optimization and passes the `>30%` storage bar, but it introduces noticeable point-read latency overhead.
