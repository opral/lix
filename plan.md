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
