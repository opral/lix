# Binary History Storage Plan (Plan 2)

## Goal

- Store historical binary files in SQL as efficiently as possible.
- Prioritize history footprint first; ignore live/read cache optimization in this plan.

## Scope

- In scope: binary history persistence, dedup, compression, GC, and replay-scale storage benchmarks.
- Out of scope: `lix_internal_file_data_cache` tuning, point-read latency tuning, live materialization cache policy.
- Backward compatibility is not a concern for this plan (no compatibility guarantees for prior binary storage formats).

## Architecture Decision

- Opaque binaries are handled by an engine-builtin binary handler (special-case fallback), not a normal wasm binary plugin.
- Plugin system remains for semantic formats; builtin handler is only for opaque bytes.
- Engine owns canonical binary CAS storage.
- Historical versions reference CAS blobs by hash.

## History Data Plane

1. On binary write, engine ingests raw bytes into CAS.
2. Chunking: FastCDC (`min=16 KiB`, `avg=64 KiB`, `max=256 KiB`).
3. Hashing: BLAKE3 on raw chunk bytes (dedup key).
4. Compression: zstd per chunk; keep raw chunk if compression is not smaller.
5. Storage: global unique chunk store + blob manifest (ordered chunk list) + file-version -> blob-hash reference.

## SQL Model

- `lix_internal_binary_chunk_store`
- `lix_internal_binary_blob_manifest`
- `lix_internal_binary_blob_manifest_chunk`
- `lix_internal_binary_file_version_ref`

Recommended next cleanup:

- replace payload prefix framing with an explicit `codec` column (`raw|zstd`) for clarity and observability.

## Change Semantics (History)

- Binary update with changed bytes emits one metadata state change (`lix_binary_blob_ref`) containing `id`, `blob_hash`, `size_bytes`.
- Binary update with identical bytes emits no binary-history change (hash no-op).
- Binary delete emits tombstone changes for prior binary state rows.

## GC Model

- Strategy: strict referential GC (guaranteed history retrievability).
- Durable refs:
  every historical binary version must have a `file_id, version_id -> blob_hash` row in `lix_internal_binary_file_version_ref`.
- GC eligibility:
  a blob is deletable only if it has zero rows in `lix_internal_binary_file_version_ref`.
- Chunk safety:
  a chunk is deletable only if it is not referenced by any live blob manifest row.
- Enforcement:
  run GC inside one transaction; validate zero-ref predicates at delete time.
- Schema hardening (next):
  add foreign keys and `ON DELETE RESTRICT` semantics so referenced manifests/chunks cannot be removed accidentally.

## Benchmarking (History-Only)

### Primary Metrics

- DB bytes after replay (`page_size * page_count`)
- Binary-history table bytes only (`chunk_store + manifest + manifest_chunk + file_version_ref`)
- Logical history bytes (`SUM(size_bytes)` over version refs/manifests)
- Compression ratio (`logical_bytes / chunk_store_bytes`)
- Dedup ratio (`1 - unique_chunks / manifest_chunk_refs`)
- Ingest throughput (commits/s, MB/s)

### Workloads

- Synthetic 4 MiB profile with multiple revisions per file (`append`, `localized edit`, `rewrite`).
- `vscode-docs` first 100 commits (full LFS blobs present).

### Acceptance Gates

- `>=30%` history storage reduction vs naive full-dup baseline (`B0A`).
- Ingest slowdown no worse than `+20%` vs Phase 1 raw-chunk CAS checkpoint (`B1B`).
- Sublinear storage growth with version count for small-edit workloads.

## Delivery Phases (Benchmarkable)

### Phase 0: Baseline Freeze

1. Freeze `B0A` naive baseline: full binary duplication per change (no CAS dedup/chunking/compression), identical replay inputs/settings.
2. Record artifacts and metrics for later deltas.

Checkpoint:

- `B0A`: end-to-end “from square 1” reference.

Baseline result (current Phase 0 profile):

- Run tag: `phase0-b0a-baseline-20260220-121951`
- Report: `packages/engine/benches/results/phase0-b0a-baseline-20260220-121951.json`
- DB: `packages/engine/benches/results/phase0-b0a-baseline-20260220-121951.sqlite`
- Config: `files_per_class=8`, `update_rounds=2`, `history_validation_queries=1`, `max_blob_bytes=4 MiB`

| Workload                       | Ops | Bytes Written | Bytes Read |  Wall (ms) |   P95 (ms) |  Ops/s |
| ------------------------------ | --: | ------------: | ---------: | ---------: | ---------: | -----: |
| `ingest_binary_cold`           |  24 |    16,555,593 |          0 |    255.560 |     20.917 | 93.912 |
| `update_binary_hot`            |  48 |    36,637,396 |          0 |    486.038 |     18.694 | 98.758 |
| `read_history_validate_single` |   1 |             0 |  3,361,662 | 46,779.206 | 46,779.205 |  0.021 |

| Storage Snapshot |   DB Bytes | Table Bytes | Index Bytes | File Data Cache Bytes |
| ---------------- | ---------: | ----------: | ----------: | --------------------: |
| `baseline`       |    847,872 |     147,456 |     634,880 |                     0 |
| `after_ingest`   | 17,649,664 |  16,900,096 |     684,032 |                     0 |
| `after_update`   | 54,546,432 |  53,731,328 |     749,568 |                     0 |

| History Storage Snapshot (`after_update_history`) |      Bytes |
| ------------------------------------------------- | ---------: |
| `blob_store_bytes`                                | 53,305,344 |
| `chunk_store_bytes`                               |      4,096 |
| `blob_manifest_bytes`                             |      4,096 |
| `blob_manifest_chunk_bytes`                       |      4,096 |
| `file_version_ref_bytes`                          |      4,096 |
| `binary_history_index_bytes`                      |     45,056 |
| `total_binary_history_table_bytes`                | 53,321,728 |
| `total_binary_history_bytes`                      | 53,366,784 |
| `logical_history_bytes`                           | 18,781,602 |

- `history_storage_ratio_after_update = 2.839`
- `ingest_write_amp = 1.015`
- `update_write_amp = 1.007`

Baseline result (second baseline: `vscode-docs` replay, first 100 commits, full LFS):

- Run date: `2026-02-20`
- Replay output: `packages/vscode-docs-replay/results/vscode-docs-first-100-baseline-b0a.lix`
- Replay anchor: `1cf1f46f42bfb84dae7206fc9711344461d3efdb`
- Replay input repo: `artifact/vscode-docs-nosmudge`
- Notes: JS SDK reflection was rebuilt before replay (`pnpm --filter js-sdk run build`); replay executed with `VSCODE_REPLAY_RESOLVE_LFS_POINTERS=1`.

| Replay Metric         |  Value |
| --------------------- | -----: |
| `commits_replayed`    |    100 |
| `commits_applied`     |    100 |
| `commits_noop`        |      0 |
| `changed_paths_total` |  3,198 |
| `elapsed_seconds`     | 354.17 |

| Storage Metric      |       Bytes |
| ------------------- | ----------: |
| `lix_file_bytes`    | 955,736,064 |
| `sqlite_page_size`  |       4,096 |
| `sqlite_page_count` |     233,334 |
| `sqlite_estimated`  | 955,736,064 |
| `sqlite_freelist`   |           0 |

| Git vs Lix Size       |         Bytes |   Ratio |
| --------------------- | ------------: | ------: |
| `git_worktree_bytes`  |    47,960,064 |         |
| `git_git_bytes`       | 7,450,468,352 |         |
| `git_total_bytes`     | 7,498,428,416 |         |
| `lix_vs_git_total`    |             - | 0.1275x |
| `lix_vs_git_worktree` |             - |  19.93x |

| Binary History Object (dbstat)                            |       Bytes |   Pages |
| --------------------------------------------------------- | ----------: | ------: |
| `lix_internal_binary_blob_store`                          | 803,500,032 | 196,167 |
| `lix_internal_binary_file_version_ref`                    |     491,520 |     120 |
| `sqlite_autoindex_lix_internal_binary_file_version_ref_1` |     274,432 |      67 |
| `sqlite_autoindex_lix_internal_binary_blob_store_1`       |     225,280 |      55 |
| `lix_internal_binary_chunk_store`                         |       4,096 |       1 |
| `lix_internal_binary_blob_manifest`                       |       4,096 |       1 |
| `lix_internal_binary_blob_manifest_chunk`                 |       4,096 |       1 |

| Key Row Counts                         |  Count |
| -------------------------------------- | -----: |
| `lix_internal_binary_blob_store`       |  2,743 |
| `lix_internal_binary_file_version_ref` |  2,531 |
| `lix_internal_change`                  | 49,131 |
| `lix_internal_snapshot`                | 44,896 |

### Phase 1: History Contract + CAS + FastCDC

1. Freeze engine-builtin opaque-binary handler contract.
2. Ensure state rows are metadata-only (`lix_binary_blob_ref`), no payload snapshots.
3. Store canonical binary history in CAS (chunk store + manifest + file-version refs), dedup by BLAKE3 chunk hash.
4. Enable FastCDC chunk boundaries (`16/64/256 KiB`) with raw chunk payloads (no zstd yet).

Checkpoint:

- `B1A`: historical reconstruction correctness and reduced state-row payload size.
- `B1B`: storage and ingest vs `B0A` on raw-chunk CAS (ablation reference for compression-only effects).
- `B1C`: dedup ratio and storage-growth behavior on append/localized-edit workloads.
- `B1`: consolidated Phase 1 gate (all of `B1A/B1B/B1C` pass).

Phase 1 result (`B1`, implemented):

- Run tag: `phase1-b1-consolidated-20260220-124603`
- Report: `packages/engine/benches/results/phase1-b1-consolidated-20260220-124603.json`
- DB: `packages/engine/benches/results/phase1-b1-consolidated-20260220-124603.sqlite`

| Metric                               |      `B0A` |       `B1` |  Delta |
| ------------------------------------ | ---------: | ---------: | -----: |
| `history_tables_after_update`        | 53,321,728 | 21,258,240 | -60.1% |
| `history_storage_ratio_after_update` |      2.839 |      1.132 | -60.1% |
| `ingest_write_amp`                   |      1.015 |      0.591 | -41.8% |
| `update_write_amp`                   |      1.007 |      0.326 | -67.6% |

| `B1` Chunk Diagnostics |      Value |
| ---------------------- | ---------: |
| `manifest_rows`        |         72 |
| `manifest_chunk_refs`  |        332 |
| `unique_chunks`        |        170 |
| `chunk_reuse_rate`     |      0.488 |
| `bytes_dedup_saved`    | 32,147,414 |

Phase 1 replay benchmark (`vscode-docs` first 100 commits, full LFS):

- `B0A` output: `packages/vscode-docs-replay/results/vscode-docs-first-100-baseline-b0a.lix`
- `B1` output: `packages/vscode-docs-replay/results/vscode-docs-first-100-phase1-b1.lix`
- Same replay input/anchor/env as baseline (`artifact/vscode-docs-nosmudge`, `VSCODE_REPLAY_RESOLVE_LFS_POINTERS=1`).

| Replay Metric         |  `B0A` |   `B1` |  Delta |
| --------------------- | -----: | -----: | -----: |
| `elapsed_seconds`     | 354.17 | 369.62 | +4.36% |
| `commits_replayed`    |    100 |    100 |      0 |
| `commits_applied`     |    100 |    100 |      0 |
| `changed_paths_total` |  3,198 |  3,198 |      0 |

| Storage Metric                                                                        |       `B0A` |        `B1` |  Delta |
| ------------------------------------------------------------------------------------- | ----------: | ----------: | -----: |
| `lix_file_bytes`                                                                      | 955,736,064 | 958,664,704 | +0.31% |
| `sqlite_page_count`                                                                   |     233,334 |     234,049 | +0.31% |
| `binary_objects_total_bytes` (`dbstat`: `lix_internal_binary*` + binary autoindexes)  | 804,515,840 | 806,895,616 | +0.30% |
| `binary_version_ref_rows`                                                             |       2,531 |       2,536 |     +5 |
| `logical_history_bytes` (`SUM(size_bytes)` in `lix_internal_binary_file_version_ref`) | 835,601,492 | 838,730,269 | +0.37% |

| Binary Layout (`dbstat` table bytes)      |       `B0A` |        `B1` |
| ----------------------------------------- | ----------: | ----------: |
| `lix_internal_binary_blob_store`          | 803,500,032 |       4,096 |
| `lix_internal_binary_chunk_store`         |       4,096 | 801,841,152 |
| `lix_internal_binary_blob_manifest`       |       4,096 |     286,720 |
| `lix_internal_binary_blob_manifest_chunk` |       4,096 |   1,744,896 |
| `lix_internal_binary_file_version_ref`    |     491,520 |     491,520 |

### Phase 2: Zstd Per Chunk

1. Add `zstd-if-smaller` compression per chunk.
2. Keep same CAS/ref model to isolate compression effect.

Checkpoint:

- `B2`: storage reduction and ingest overhead vs `B1`.

Phase 2 result (`B2`, implemented):

- Run tag: `phase2-b2-zstd-20260220-131021`
- Report: `packages/engine/benches/results/phase2-b2-zstd-20260220-131021.json`
- DB: `packages/engine/benches/results/phase2-b2-zstd-20260220-131021.sqlite`

| Metric                               |       `B1` |      `B2` |  Delta |
| ------------------------------------ | ---------: | --------: | -----: |
| `history_tables_after_update`        | 21,258,240 | 7,311,360 | -65.6% |
| `history_storage_ratio_after_update` |      1.132 |     0.389 | -65.6% |
| `ingest_write_amp`                   |      0.591 |     0.196 | -66.9% |
| `update_write_amp`                   |      0.326 |     0.124 | -61.9% |

| Workload Wall Time (ms)        |       `B1` |       `B2` | Delta |
| ------------------------------ | ---------: | ---------: | ----: |
| `ingest_binary_cold`           |    343.969 |    315.570 | -8.3% |
| `update_binary_hot`            |    648.650 |    616.289 | -5.0% |
| `read_history_validate_single` | 44,239.718 | 46,383.973 | +4.9% |

| Chunk Diagnostics     |       `B1` |       `B2` |  Delta |
| --------------------- | ---------: | ---------: | -----: |
| `manifest_rows`       |         72 |         72 |      0 |
| `manifest_chunk_refs` |        332 |        332 |      0 |
| `unique_chunks`       |        170 |        170 |      0 |
| `chunk_reuse_rate`    |      0.488 |      0.488 |      0 |
| `chunk_store_bytes`   | 21,045,575 |  7,119,458 | -66.2% |
| `bytes_dedup_saved`   | 32,147,414 | 46,073,531 | +43.3% |

Phase 2 replay follow-up (wasm compression path enabled):

- Chunk compression enabled on wasm execution path (`ruzstd` encoder/decoder on wasm, native zstd unchanged).
- `B2 (before wasm compression)` output: `packages/vscode-docs-replay/results/vscode-docs-first-100-phase2-b2.lix`
- `B2 (after wasm compression)` output: `packages/vscode-docs-replay/results/vscode-docs-first-100-phase2-wasm-compress.lix`
- Same replay input/anchor/env as prior 100-commit replay (`artifact/vscode-docs-nosmudge`, `VSCODE_REPLAY_RESOLVE_LFS_POINTERS=1`).

| Replay Metric         | `B2` (before) | `B2` (wasm compression) |  Delta |
| --------------------- | ------------: | ----------------------: | -----: |
| `elapsed_seconds`     |        363.24 |                  379.73 | +4.54% |
| `commits_replayed`    |           100 |                     100 |      0 |
| `commits_applied`     |           100 |                     100 |      0 |
| `changed_paths_total` |         3,198 |                   3,198 |      0 |

| Storage Metric                                                                        | `B2` (before) | `B2` (wasm compression) |  Delta |
| ------------------------------------------------------------------------------------- | ------------: | ----------------------: | -----: |
| `lix_file_bytes`                                                                      |   958,820,352 |             915,156,992 | -4.55% |
| `sqlite_page_count`                                                                   |       234,087 |                 223,427 | -4.55% |
| `binary_objects_total_bytes` (`dbstat`: `lix_internal_binary*` + binary autoindexes)  |   807,043,072 |             763,379,712 | -5.41% |
| `logical_history_bytes` (`SUM(size_bytes)` in `lix_internal_binary_file_version_ref`) |   838,730,269 |             838,730,269 |      0 |

| Chunk Codec Mix            | `B2` (before) | `B2` (wasm compression) |
| -------------------------- | ------------: | ----------------------: |
| `raw_chunks` (`LIXRAW01`)  |        11,730 |                   2,135 |
| `zstd_chunks` (`LIXZSTD1`) |             0 |                   9,595 |
| `legacy_unframed_chunks`   |             0 |                       0 |

### Phase 3: History Safety Guarantees (GC)

1. Implement strict referential GC (delete only unreachable blobs/chunks).
2. Add schema constraints (`FK` + `ON DELETE RESTRICT`) to enforce invariants.

Checkpoint:

- `B3`: GC correctness (no broken history reads), reclaim behavior after GC + VACUUM.

Phase 3 result (`B3`, implemented):

- Added strict binary CAS GC pass after file side-effects:
  - prunes stale `lix_internal_binary_file_version_ref` rows without live `lix_binary_blob_ref` state
  - prunes unreachable `lix_internal_binary_blob_manifest_chunk`, `lix_internal_binary_chunk_store`, `lix_internal_binary_blob_manifest`, and legacy `lix_internal_binary_blob_store` rows
- Added schema-level FK constraints with `ON DELETE RESTRICT`:
  - `lix_internal_binary_blob_manifest_chunk.blob_hash -> lix_internal_binary_blob_manifest.blob_hash`
  - `lix_internal_binary_blob_manifest_chunk.chunk_hash -> lix_internal_binary_chunk_store.chunk_hash`
  - `lix_internal_binary_file_version_ref.blob_hash -> lix_internal_binary_blob_manifest.blob_hash`
- Enabled sqlite FK enforcement at init (`PRAGMA foreign_keys = ON`) and in sqlite test simulation connections.

Validation:

- `binary_cas_gc_prunes_unreachable_rows_after_overwrite_{sqlite,postgres}`: passed
- `binary_cas_foreign_keys_restrict_live_parent_deletes_{sqlite,postgres}`: passed
- Regression checks:
  - `file_write_uses_builtin_binary_fallback_when_no_plugin_matches_file_type_sqlite`: passed
  - `file_cache_has_no_orphan_rows_after_mixed_lifecycle_{sqlite,postgres}`: passed

### Phase 4: Codec Metadata / Observability

1. Replace payload prefix framing with explicit codec metadata (`raw`, `zstd`, `zstd+dict:<id>`).

Checkpoint:

- `B4`: no major storage change expected; improves auditability and measurement.

### Phase 5: Dictionary Recompression + Maintenance

1. Add background dictionary training by binary cohort.
2. Recompress eligible chunks with trained dictionaries.
3. Add scheduling policy for recompression + GC + optional VACUUM windows.

Checkpoint:

- `B5`: incremental storage gain vs `B2`, within accepted ingest/read overhead, plus long-run file-size stability and reclaim efficiency under replay/update churn.

## Note On Prior Replay Results

- The recent replay comparison mixed storage models, so it is not a clean apples-to-apples history baseline.
- Decision-grade comparisons must be run as: no-zstd CAS vs zstd CAS, with identical plugin/state behavior.
