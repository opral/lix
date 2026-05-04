# Optimization Log

Short running log of performance work: what changed, what moved, and what did not.

## 2026-05-01 12:12 PDT: Tracked State Single-Mutation Fast Path

Change: `TrackedStateTree::apply_mutations` now path-copies one touched leaf plus ancestors for single-row mutations instead of rebuilding the whole tree.

| Benchmark                                     |    Before |     After |       Delta |
| --------------------------------------------- | --------: | --------: | ----------: |
| `update_1_existing/100000`                    |  ~99.8 ms |  ~1.45 ms | ~69x faster |
| `append_1_new_child_commit/100000`            |  ~98.9 ms |  ~1.44 ms | ~69x faster |
| `partial_snapshot_update_1_payload_1k/100000` | ~240.8 ms |  ~3.32 ms | ~72x faster |
| `delete_1/10k`                                |  ~9.65 ms | ~0.195 ms | ~49x faster |
| `update_10pct_existing/10k`                   |  ~10.1 ms |  ~10.1 ms |   unchanged |
| `diff_update_1/10k`                           |  ~24.4 ms |  ~26.6 ms |   unchanged |

Worked: single-row update, append, partial snapshot replacement, and tombstone-style writes no longer scale with total root size.

Did not work: multi-mutation writes still rebuild; diff traversal still scans instead of skipping equal content-addressed chunks.

Next: optimize tracked-state diff traversal with subtree hash equality.

## 2026-05-01 12:22 PDT: Canonical Single-Mutation Rechunking

Change: replaced the unsafe leaf-only shortcut with canonical leaf rechunking that resyncs on existing content-addressed leaf chunks before rebuilding internal summaries.

| Benchmark                                     | Shortcut Before | Canonical After | Delta vs Shortcut | Delta vs Original |
| --------------------------------------------- | --------------: | --------------: | ----------------: | ----------------: |
| `update_1_existing/100000`                    |        ~1.65 ms |        ~54.7 ms |       ~33x slower |      ~1.8x faster |
| `append_1_new_child_commit/100000`            |        ~1.65 ms |        ~53.5 ms |       ~32x slower |      ~1.8x faster |
| `partial_snapshot_update_1_payload_1k/100000` |        ~3.32 ms |       ~131.4 ms |       ~40x slower |      ~1.8x faster |

Worked: optimized roots now match full canonical rebuilds for single update and insert cases.

Did not work: the canonical path currently reads too many leaf chunks before resync, so it gives back most of the shortcut win.

Next: collect leaf summaries from internal nodes and decode only the edited/resync window.

## 2026-05-01 12:26 PDT: Lazy Leaf Loading For Canonical Rechunking

Change: single-mutation rechunking now collects leaf summaries from internal nodes and decodes only the edited/resync leaf window.

| Benchmark                                     |    Before |    After |        Delta |
| --------------------------------------------- | --------: | -------: | -----------: |
| `update_1_existing/100000`                    |  ~54.7 ms | ~8.69 ms | ~6.3x faster |
| `append_1_new_child_commit/100000`            |  ~53.5 ms | ~9.29 ms | ~5.8x faster |
| `partial_snapshot_update_1_payload_1k/100000` | ~131.4 ms | ~21.9 ms | ~6.0x faster |

Worked: avoided decoding every leaf while preserving canonical rebuild equivalence.

Did not work: still rebuilds internal summaries from the full leaf-summary list.

Next: make the internal summary rebuild path-copy/resync too, so unchanged internal levels are reused instead of rebuilt.

## 2026-05-01 12:33 PDT: Internal Summary Path-Copy For Single Writes

Change: patched internal summary levels upward from the changed leaf range instead of rebuilding all internal summaries from the full leaf list.

| Benchmark                                     |   Before |    After |        Delta |
| --------------------------------------------- | -------: | -------: | -----------: |
| `update_1_existing/100000`                    | ~8.69 ms | ~6.73 ms | ~1.3x faster |
| `append_1_new_child_commit/100000`            | ~9.29 ms | ~6.66 ms | ~1.4x faster |
| `partial_snapshot_update_1_payload_1k/100000` | ~21.9 ms | ~16.8 ms | ~1.3x faster |

Worked: canonical roots still match full rebuilds while fewer internal chunks are encoded.

Did not work: impact was much smaller than expected because collecting summary levels still walks the internal tree and probes leaf children.

Next: replace summary collection with a real seek cursor over the mutation path, then reuse sibling summaries from the cursor stack.

## 2026-05-01 12:39 PDT: Seek-Cursor Single-Write Path

Change: replaced global summary collection with a root-to-leaf seek cursor and added right-edge append handling.

| Benchmark                                     |   Before |    After |        Delta |
| --------------------------------------------- | -------: | -------: | -----------: |
| `update_1_existing/100000`                    | ~6.73 ms | ~1.31 ms | ~5.1x faster |
| `append_1_new_child_commit/100000`            | ~6.66 ms | ~1.25 ms | ~5.3x faster |
| `partial_snapshot_update_1_payload_1k/100000` | ~16.8 ms | ~3.47 ms | ~4.8x faster |

Worked: single-row writes now scale with the mutation path and local rechunk window instead of the full leaf-summary set.

Did not work: this still only covers one-row writes; multi-mutation batches still use the full rebuild path.

Next: add a batched mutation cursor so sorted multi-row writes path-copy touched windows instead of rebuilding the whole tree.

## 2026-05-01 12:46 PDT: Batch Leaf-Window Rechunking

Change: multi-row writes now patch the touched leaf window from sorted mutations and fall back to full rebuilds for large batches.

| Benchmark                     |   Before |    After |        Delta |
| ----------------------------- | -------: | -------: | -----------: |
| `update_10pct_existing/10k`   | ~10.1 ms | ~4.45 ms | ~2.3x faster |
| `delete_10pct/10k`            | ~9.67 ms | ~3.43 ms | ~2.8x faster |
| `append_new_child_commit/10k` | ~20.4 ms | ~20.5 ms |    unchanged |
| `update_all_existing/10k`     | ~17.0 ms | fallback |    unchanged |

Worked: contiguous small batch updates and tombstones avoid decoding the full root.

Did not work: full-range rewrites and large appends are still best served by the full rebuild path.

Next: optimize append batches with an explicit right-edge batch append path.

## 2026-05-01 12:51 PDT: Chunk-Skipping Tracked-State Diff

Change: tracked-state diffs now compare content-addressed tree chunks and skip equal subtrees before decoding leaves.

| Benchmark               |   Before |    After |        Delta |
| ----------------------- | -------: | -------: | -----------: |
| `diff_equal/10k`        | ~24.6 ms |  ~101 us | ~244x faster |
| `diff_update_1/10k`     | ~24.4 ms |  ~130 us | ~188x faster |
| `diff_delete_1/10k`     | ~24.7 ms |  ~139 us | ~178x faster |
| `diff_update_10pct/10k` | ~25.2 ms | ~1.64 ms |  ~15x faster |

Worked: equal roots and small diffs now skip unchanged content-addressed subtrees.

Did not work: mismatched internal boundaries still fall back to materializing that subtree.

Next: replace the boundary fallback with a true cursor differ so insert/delete-heavy shape changes can keep skipping below shifted boundaries.

## 2026-05-01 12:56 PDT: Leaf-Cursor Resync For Shape-Changed Diffs

Change: mismatched internal diff boundaries now walk leaf-summary cursors and resync on the next equal leaf chunk instead of decoding the whole subtree.

| Benchmark               |   Before |    After |        Delta |
| ----------------------- | -------: | -------: | -----------: |
| `diff_delete_10pct/10k` | ~3.24 ms | ~2.87 ms | ~1.1x faster |
| `diff_update_10pct/10k` | ~1.99 ms | ~1.65 ms | ~1.2x faster |
| `diff_delete_1/10k`     |  ~139 us |  ~141 us |    unchanged |

Worked: shifted-boundary diffs can now skip matching suffix leaf chunks after resync.

Did not work: the implementation still gathers leaf summaries for mismatched subtrees, so it is not yet a fully streaming Dolt-style cursor.

Next: turn leaf-summary resync into a real node cursor with `advance_to`, avoiding full leaf-summary collection for large mismatched subtrees.

## 2026-05-01 13:00 PDT: Streaming Leaf Summary Cursor

Change: shape-changed diffs now stream leaf summaries from tree cursors instead of pre-collecting full leaf-summary vectors.

| Benchmark               |   Before |    After |        Delta |
| ----------------------- | -------: | -------: | -----------: |
| `diff_delete_10pct/10k` | ~2.87 ms | ~2.24 ms | ~1.3x faster |
| `diff_update_10pct/10k` | ~1.65 ms | ~1.59 ms | ~1.0x faster |
| `diff_equal/10k`        |  ~101 us |   ~82 us | ~1.2x faster |
| `diff_delete_1/10k`     |  ~141 us |  ~149 us |    unchanged |

Worked: cursor frames now yield leaf summaries directly from parent metadata and only load payload leaves for changed windows.

Did not work: this is still a leaf-level cursor; it does not yet `advance_to` at arbitrary internal levels like Dolt's chunker/differ.

Next: add range-aware scan using the same cursor primitives, then revisit full internal-level `advance_to` if shifted-boundary diffs remain hot.

## 2026-05-01 13:08 PDT: Sorted Multi-Mutation Chunker

Change: multi-row writes now stream sorted edits across old leaf summaries, reuse unchanged leaf chunks, rechunk touched windows, and resync to the old leaf stream.

| Benchmark                     |   Before |    After |        Delta |
| ----------------------------- | -------: | -------: | -----------: |
| `update_10pct_existing/10k`   | ~4.45 ms | ~3.08 ms | ~1.4x faster |
| `delete_10pct/10k`            | ~3.43 ms | ~2.49 ms | ~1.4x faster |
| `append_new_child_commit/10k` | ~20.5 ms | ~14.2 ms | ~1.4x faster |
| `update_all_existing/10k`     | ~22.5 ms | ~22.9 ms |    unchanged |

Worked: the batch path is now one sorted-edit flow instead of a narrow leaf-window patch, and append batches avoid decoding the old root.

Did not work: single-row writes still use the separate seek fast path because the generic chunker rebuilds internal summaries from leaf summaries.

Next: either add internal-level path-copy to the sorted chunker or move to range-aware scan, depending on whether write or read workloads are hotter.

## 2026-05-01 13:13 PDT: Range-Aware Tracked-State Scan

Change: tracked-state scan now builds encoded key-prefix ranges and skips internal subtrees whose summary first/last keys cannot overlap the request.

| Benchmark                            |   Before |    After |        Delta |
| ------------------------------------ | -------: | -------: | -----------: |
| `scan_schema_selectivity_1pct/10k`   | ~7.07 ms |  ~186 us |  ~38x faster |
| `scan_schema_selectivity_10pct/10k`  | ~7.11 ms |  ~906 us | ~7.8x faster |
| `scan_schema_selectivity_100pct/10k` | ~7.18 ms | ~7.59 ms |    unchanged |
| `scan_all/10k`                       | ~7.83 ms | ~7.41 ms |    unchanged |
| `scan_file/10k`                      | ~7.27 ms | ~7.46 ms |    unchanged |

Worked: schema-selective scans now use the tree as an ordered index instead of decoding every leaf.

Did not work: file-only scans cannot prune well with the current key order because `file_id` is after `schema_key`.

Next: decide whether common file-first queries need a secondary index or a different persisted key layout before locking the tracked-state storage format.

## 2026-05-01 13:25 PDT: Tracked State By-File Secondary Index

Change: tracked-state writes now maintain a file-first secondary root, and file-filter scans use it when the candidate set is small enough to beat a primary scan.

| Benchmark                         |   Before |    After |          Delta |
| --------------------------------- | -------: | -------: | -------------: |
| `scan_file_selectivity_1pct/10k`  |  ~7.5 ms | ~1.74 ms |   ~4.3x faster |
| `scan_file_selectivity_10pct/10k` |  ~7.5 ms | ~6.48 ms |   ~1.2x faster |
| `scan_file/10k`                   | ~7.46 ms | ~8.50 ms | slightly worse |
| `write_root/10k`                  | ~11.9 ms | ~22.8 ms |   ~1.9x slower |

Worked: selective file scans now have a real file-first access path.

Did not work: non-selective file scans and writes pay index-maintenance/probe overhead; the secondary index is non-covering and still fetches primary rows.

Next: decide whether by-file should be a covering index for common reads, or whether the write amplification is too high for the storage format.

## 2026-05-01 13:29 PDT: Covering By-File Secondary Index

Change: the by-file secondary index now stores the full tracked-state value, so selective file scans avoid primary-tree lookups.

| Benchmark                         | Non-Covering | Covering |        Delta |
| --------------------------------- | -----------: | -------: | -----------: |
| `scan_file_selectivity_1pct/10k`  |     ~1.74 ms |  ~343 us | ~5.1x faster |
| `scan_file_selectivity_10pct/10k` |     ~6.48 ms | ~6.02 ms | ~1.1x faster |
| `scan_file/10k`                   |     ~8.50 ms | ~8.10 ms |    unchanged |
| `write_root/10k`                  |     ~22.8 ms | ~24.3 ms | ~1.1x slower |

Worked: highly selective file scans benefit a lot when the index covers the returned row.

Did not work: 10pct scans are dominated by decoding/materializing many rows, and covering values add a little write cost.

## 2026-05-01 13:53 PDT: Snapshot Ref Prototype

Change: tracked-state tree values now store `snapshot:v1:<hash>` refs and write snapshot bytes once into a snapshot CAS namespace; reads/diffs resolve refs back to `snapshot_content`.

| Benchmark / Accounting Case                 |    Before |     After |             Delta |
| ------------------------------------------- | --------: | --------: | ----------------: |
| `by_file_covering/write_root_payload_128k`  | ~26.29 MB | ~13.17 MB | ~50% less storage |
| `by_file_covering/write_root_payload_16k`   | ~33.56 MB | ~17.00 MB | ~49% less storage |
| `by_file_covering/write_root_payload_1k`    | ~25.52 MB | ~16.40 MB | ~36% less storage |
| `by_file_covering/write_root_payload_small` |  ~4.91 MB |  ~6.56 MB | ~34% more storage |
| `scan_file_selectivity_1pct/10k`            |   ~343 us |  ~1.41 ms |      ~4.1x slower |
| `scan_file_selectivity_10pct/10k`           |  ~6.02 ms |  ~6.20 ms |         unchanged |
| `scan_file/10k`                             |  ~8.10 ms | ~11.68 ms |      ~1.4x slower |
| `write_root/10k`                            |  ~24.3 ms |  ~24.8 ms |         unchanged |

Worked: large snapshots are no longer duplicated by the covering by-file index.

Did not work: small rows now pay snapshot-ref overhead, and full-row reads pay extra snapshot lookups.

Next: make scans projection-aware so header/identity reads can avoid resolving snapshots, and consider inlining small snapshots below a size threshold.

## 2026-05-01 14:04 PDT: Inline Small Snapshots And Projection-Aware Reads

Change: snapshots <=512 bytes stay inline; scans only resolve `snapshot:v1` refs when the projection asks for `snapshot_content`.

| Benchmark / Accounting Case                           |    Before |     After |                         Delta |
| ----------------------------------------------------- | --------: | --------: | ----------------------------: |
| `by_file_covering/write_root_payload_small`           |  ~6.56 MB |  ~4.91 MB |             ~25% less storage |
| `by_file_covering/write_root_payload_1k`              | ~16.40 MB | ~16.40 MB |                     unchanged |
| `write_root/10k`                                      |  ~24.8 ms |  ~23.0 ms |                    ~7% faster |
| `scan_file_selectivity_1pct/10k`                      |  ~1.41 ms |   ~306 us |                  ~4.6x faster |
| `scan_file_selectivity_10pct/10k`                     |  ~6.20 ms |  ~6.03 ms |                     unchanged |
| `scan_file_selectivity_payload_1k_10pct/10k` full row |  ~5.01 ms |  ~4.35 ms | ~13% faster header projection |

Worked: small/default rows avoid snapshot-CAS overhead, while large rows keep the storage win from snapshot refs.

Did not work: header projections over 1KiB payloads only show a modest latency win because tree/index decoding still dominates.

Next: decide whether snapshot refs need a larger threshold or compressed blob layout before locking the tracked-state format.

## 2026-05-01 14:15 PDT: Inline Snapshot Threshold Experiment

Change: added bench-only inline threshold variants to compare `512`, `1024`, `2048`, `4096`, and `8192` byte cutoffs.

| Threshold | `1k` Storage | `1k` Snapshots | `write_root_payload_1k/10k` | `scan_file_payload_1k_10pct/10k` |
| --------: | -----------: | -------------: | --------------------------: | -------------------------------: |
|      512B |    ~16.40 MB |         10,000 |                      ~33 ms |                         ~5.07 ms |
|     1024B |    ~16.40 MB |         10,000 |                      ~34 ms |                         ~5.02 ms |
|     2048B |    ~25.52 MB |              0 |                      ~43 ms |                        ~11.88 ms |
|     4096B |    ~25.52 MB |              0 |                      ~43 ms |                        ~12.12 ms |
|     8192B |    ~25.52 MB |              0 |                      ~44 ms |                        ~12.20 ms |

Worked: the current low threshold keeps `1KiB`-class JSON out of the duplicated covering index and wins on storage, writes, and scans.

Did not work: inlining `1KiB`-class snapshots bloats the covering index and makes reads/writes slower, not faster.

Next: keep the inline threshold low for the current covering-index layout; revisit only after a header-covering index or compressed snapshot layout.

## 2026-05-01 14:27 PDT: Header-Covering By-File Index

Change: tracked-state values now carry an explicit `deleted` bit, and the by-file index covers row headers without storing `snapshot_content`.

| Benchmark / Accounting Case               | Full Covering | Header Covering |             Delta |
| ----------------------------------------- | ------------: | --------------: | ----------------: |
| `by_file/write_root_payload_small`        |      ~4.91 MB |        ~4.33 MB | ~12% less storage |
| `by_file/write_root_payload_1k`           |     ~16.40 MB |       ~15.57 MB |  ~5% less storage |
| `by_file/write_root_payload_16k`          |     ~17.00 MB |       ~16.91 MB |         unchanged |
| `write_root/10k`                          |      ~23.0 ms |        ~19.7 ms |       ~14% faster |
| `scan_file_header_selectivity_1pct/10k`   |       ~344 us |         ~224 us |       ~35% faster |
| `scan_file_selectivity_1pct/10k` full row |       ~306 us |        ~1.38 ms |      ~4.5x slower |

Worked: header scans and writes no longer decode or store duplicated payload refs in the by-file index.

Did not work: full-row file scans lose the old full-covering shortcut and must fetch primary rows.

Next: decide whether full-row file scans are hot enough to need a separate payload-covering mode; otherwise keep header-covering and optimize primary fetch batching.

## 2026-05-01 14:38 PDT: Single Tracked-State Physical Format

Change: removed tracked-state index-mode switching, extracted `ByFileIndex` and `SnapshotStore`, and made production always use primary tree + header-covering by-file tree + snapshot CAS.

| Check / Accounting Case        |    Result |
| ------------------------------ | --------: |
| `tracked_state --lib`          | 64 passed |
| `write_root_payload_small/10k` |  ~4.33 MB |
| `write_root_payload_1k/10k`    | ~15.57 MB |

Worked: production code no longer carries primary/non-covering/full-covering branches while preserving the measured header-covering format.

Did not work: no new speedup; this is a simplification pass.

Next: batch primary fetch for full-row by-file scans or compressed snapshot CAS.

## 2026-05-01 15:02 PDT: Typed Stored Snapshot

Change: replaced `snapshot:v1:<hash>` strings inside `TrackedStateValue` with `StoredSnapshot::{Missing, Inline, Ref}` and made snapshot refs encode as a typed value field.

| Check                             |    Result |
| --------------------------------- | --------: |
| `tracked_state --lib`             | 65 passed |
| `storage-benches --bench storage` |   checked |

Worked: tree values no longer overload `snapshot_content` with sentinel strings, which makes future compression/versioning cleaner.

Did not work: no expected perf change; this is a physical-format cleanup.

Next: batch primary fetch for full-row by-file scans or add compressed snapshot CAS.

## 2026-05-01 15:24 PDT: Batched Primary Fetch For By-File Full Scans

Change: by-file full-row scans now collect primary keys from the secondary index and fetch primary values through one sorted tree traversal instead of one root-to-leaf lookup per row.

| Benchmark / Accounting Case                  |    Before |     After |        Delta |
| -------------------------------------------- | --------: | --------: | -----------: |
| `scan_file_selectivity_1pct/10k`             |  ~1.36 ms |   ~353 us | ~3.9x faster |
| `scan_file_selectivity_payload_1k_1pct/10k`  |  ~2.11 ms |   ~988 us | ~2.1x faster |
| `scan_file_selectivity_10pct/10k`            |  ~5.44 ms |  ~5.67 ms |    unchanged |
| `scan_file_selectivity_payload_1k_10pct/10k` |  ~4.76 ms |  ~5.15 ms |   ~8% slower |
| `write_root_payload_small/10k` storage       |  ~4.33 MB |  ~4.33 MB |    unchanged |
| `write_root_payload_1k/10k` storage          | ~15.44 MB | ~15.44 MB |    unchanged |

Worked: the accepted 1% full-row by-file regression is mostly recovered without changing the physical format.

Did not work: 10% full-row scans do not benefit; batching adds enough sorting/collection overhead to stay near the primary-scan fallback threshold.

Next: tune the by-file-vs-primary fallback threshold for full-row scans, or move to adaptive snapshot encoding.

## 2026-05-01 15:38 PDT: Adaptive Snapshot Encoding

Change: snapshot storage now chooses `Inline` vs `Ref` from encoded tree-value size and minimum tree-byte savings instead of raw snapshot length.

| Benchmark / Accounting Case                  |    Before |     After |      Delta |
| -------------------------------------------- | --------: | --------: | ---------: |
| `write_root_payload_1k/10000`                |    ~34 ms |  ~34.1 ms |  unchanged |
| `scan_file_selectivity_payload_1k_1pct/10k`  |   ~988 us |  ~1.06 ms | ~8% slower |
| `scan_file_selectivity_payload_1k_10pct/10k` |  ~5.15 ms |  ~5.51 ms | ~7% slower |
| `write_root_payload_1k/10k` storage          | ~15.44 MB | ~15.44 MB |  unchanged |

Worked: the physical rule is now value-shape based; default keeps `1KiB` JSON out-of-line because hot-tree scan perf matters more than the small total-byte win from inlining.

Did not work: byte-minimal inlining for `1KiB` JSON still wins storage slightly (`~14.63 MB`) but makes full-row file scans much slower, so it remains off by default.

Next: tune full-row file-scan fallback or explore compressed snapshot CAS.

## 2026-05-01 16:18 PDT: Compressed Snapshot CAS

Change: snapshot refs now store `{ codec, hash, uncompressed_len }`; snapshot CAS stores zstd payloads for `16KiB+` snapshots when compression saves at least `128B`.

| Benchmark / Accounting Case           |    Before |     After |             Delta |
| ------------------------------------- | --------: | --------: | ----------------: |
| `write_root_payload_1k/10000`         |  ~34.1 ms |  ~40.7 ms |       ~19% slower |
| `write_root_payload_16k/1000`         |  ~10.9 ms |  ~19.0 ms |       ~74% slower |
| `write_root_payload_128k/100`         |  ~7.73 ms |  ~12.5 ms |       ~61% slower |
| `write_root_payload_1k/10k` storage   | ~15.44 MB | ~15.54 MB |         unchanged |
| `write_root_payload_16k/1k` storage   | ~16.90 MB |  ~0.59 MB | ~96% less storage |
| `write_root_payload_128k/100` storage | ~13.16 MB |  ~0.06 MB | ~99% less storage |

Worked: large structured JSON snapshots are now tiny on disk while the hot tree still stores only typed refs.

Did not work: compressing `1KiB` snapshots saved bytes but doubled write latency, so the default only compresses larger cold payloads.

Next: add payload hash to row headers so diff/merge can compare large snapshots without loading or decompressing CAS blobs.

## 2026-05-01 16:29 PDT: Chunked JSON Snapshot Prototype

Change: large top-level JSON arrays/objects can now store a small manifest plus content-addressed JSON child chunks, proving partial snapshot sharing across branch-local edits.

| Accounting Case                       | Compressed CAS | JSON Chunks |        Delta |
| ------------------------------------- | -------------: | ----------: | -----------: |
| `write_root_payload_16k/1k` storage   |       ~0.59 MB |    ~0.97 MB |  ~64% larger |
| `write_root_payload_128k/100` storage |       ~0.06 MB |    ~0.35 MB | ~4.9x larger |
| `tracked_state::storage::tests --lib` |              - |    8 passed |            - |

Worked: one changed item in a large JSON array shares most child chunk hashes with the previous snapshot.

Did not work: this is less compact than pure zstd CAS and currently canonicalizes JSON on load.

Next: make large JSON snapshots an explicit stable physical format with canonical-json semantics, chunk accounting, and branch-edit benchmarks.

## 2026-05-02 15:09 PDT: Json Store Base-Aware Semantic Chunks

Change: JSON chunks are now semantic byte chunks over canonical JSON, and `StoreJsonOptions::base` skips rewriting chunks already present in the base manifest.

| Benchmark / Accounting Workload                     |   Before |    After |        Delta |
| --------------------------------------------------- | -------: | -------: | -----------: |
| `write/against_base_object_update_1_of_1000/50`     | ~46.4 ms | ~45.6 ms |    unchanged |
| `write/against_base_array_update_1_of_1000/50`      | ~60.7 ms | ~67.4 ms | ~1.1x slower |
| `base_update_object_1_of_1000/50` storage bytes/row |   ~2,633 |   ~2,691 |    unchanged |
| `base_update_array_1_of_1000/50` storage bytes/row  |  ~53,868 |   ~1,860 | ~29x smaller |

Worked: nested array updates now share nearly all unchanged JSON chunks with the base.

Did not work: write latency did not improve yet because encoding still reparses and rechunks the updated JSON.

## 2026-05-02 15:23 PDT: Json Store Raw/Zstd CAS Simplification

Change: removed JSON chunk manifests/chunk writes from `json_store`; large compressible JSON now stores as one zstd CAS blob, small JSON as raw CAS, and `StoreJsonOptions::base` is ignored for now.

| Benchmark / Accounting Workload                    | Chunked Before | Simple After |        Delta |
| -------------------------------------------------- | -------------: | -----------: | -----------: |
| `write/large_structured_128k/50`                   |       ~24.6 ms |     ~4.62 ms | ~5.3x faster |
| `write/large_array_128k/50`                        |       ~27.2 ms |     ~4.63 ms | ~5.9x faster |
| `write/against_base_object_update_1_of_1000/50`    |       ~46.4 ms |     ~24.5 ms | ~1.9x faster |
| `write/against_base_array_update_1_of_1000/50`     |       ~60.7 ms |     ~38.0 ms | ~1.6x faster |
| `read_projection/top_level_1_prop_128k/50`         |       ~15.6 ms |     ~11.1 ms | ~1.4x faster |
| `structured_128k/50` storage bytes/row             |        ~35,761 |       ~1,539 | ~23x smaller |
| `base_update_array_1_of_1000/50` storage bytes/row |        ~53,868 |       ~2,132 | ~25x smaller |

Worked: the boring raw/zstd CAS format is faster and smaller for the current structured JSON workloads.

Did not work: partial updates now rewrite whole compressed blobs; add FastCDC only if future branch-edit workloads show that rewrite cost dominates.

## 2026-05-02 16:34 PDT: Changelog JsonRef Payloads

Change: `CanonicalChange` now stores `snapshot_ref` and `metadata_ref`; materialized changelog writes store JSON through `JsonStore` before appending compact FlatBuffer rows.

| Benchmark / Accounting Case         |    Before |     After |        Delta |
| ----------------------------------- | --------: | --------: | -----------: |
| `encode_only/full_row/10k`          |  ~3.26 ms |  ~2.25 ms |  ~31% faster |
| `append_changes_payload_1k/10k`     | ~14.78 ms | ~23.26 ms |  ~57% slower |
| `append_changes_payload_16k/1k`     |  ~5.24 ms | ~10.78 ms | ~106% slower |
| `append_changes_payload_128k/100`   |  ~9.30 ms |  ~6.38 ms |  ~31% faster |
| `append_changes_metadata_1k/10k`    | ~20.34 ms | ~37.08 ms |  ~82% slower |
| `append_1k/10k` storage bytes/row   |    ~1,252 |    ~1,333 |   ~6% larger |
| `append_16k/1k` storage bytes/row   |   ~16,611 |      ~375 | ~98% smaller |
| `metadata_1k/10k` storage bytes/row |    ~2,300 |    ~1,373 | ~40% smaller |

Worked: large changelog snapshots now share the same compressed JSON CAS behavior as `JsonStore`, cutting big-row storage sharply and improving `128KiB` append latency.

Did not work: small/medium appends now pay an extra JSON-store write per payload; next step is batching or inlining tiny changelog JSON refs if this path becomes hot.

## 2026-05-02 19:37 PDT: Json Store Flush-Based Writer

Change: `JsonStoreContext::writer()` now stages JSON bytes, returns refs immediately, dedupes by hash inside the writer, and persists staged payloads on `flush(tx)`.

| Benchmark / Accounting Case                     |   Before |    After |      Delta |
| ----------------------------------------------- | -------: | -------: | ---------: |
| `write/small_raw_1k/1000`                       | ~1.23 ms | ~1.22 ms |  unchanged |
| `write/medium_structured_16k/200`               | ~3.37 ms | ~3.12 ms | ~7% faster |
| `write/large_structured_128k/50`                | ~4.60 ms | ~4.54 ms |  unchanged |
| `write/large_array_128k/50`                     | ~4.67 ms | ~4.72 ms |  unchanged |
| `write/dedupe_same_16k/1000`                    | ~16.3 ms | ~15.7 ms | ~4% faster |
| `write/against_base_object_update_1_of_1000/50` | ~25.1 ms | ~24.7 ms |  unchanged |
| `write/against_base_array_update_1_of_1000/50`  | ~37.6 ms | ~37.6 ms |  unchanged |

Worked: the staged writer gives us Dolt-style flush semantics without slowing normal JsonStore writes, and duplicate payload batches avoid redundant KV writes.

Did not work: staging initially double-hashed/re-encoded at flush; fixed by preparing the stored payload during `stage_bytes()` so `flush()` only writes.

## 2026-05-02 19:55 PDT: Json Store Writer Dedupe Before Encoding

Change: `JsonStoreWriter::stage_bytes()` now validates and hashes first, skips already-staged JSON before compression/envelope encoding, and only prepares payload bytes for new refs.

| Benchmark / Accounting Case       |   Before |    After |       Delta |
| --------------------------------- | -------: | -------: | ----------: |
| `write/small_raw_1k/1000`         | ~1.22 ms | ~1.22 ms |   unchanged |
| `write/medium_structured_16k/200` | ~3.12 ms | ~3.22 ms |   unchanged |
| `write/large_structured_128k/50`  | ~4.54 ms | ~4.41 ms |  ~3% faster |
| `write/large_array_128k/50`       | ~4.72 ms | ~4.42 ms |  ~6% faster |
| `write/dedupe_same_16k/1000`      | ~15.7 ms | ~7.18 ms | ~54% faster |
| `append_changes_metadata_1k/10k`  | ~37.1 ms | ~33.7 ms |  ~9% faster |

Worked: duplicate-heavy JSON batches now avoid repeated encode/compress work as well as repeated KV writes.

Did not work: unique-write benches are mostly unchanged, which is expected for this targeted fast path.

## 2026-05-03 15:49 PDT: Storage API Backend Batch Hooks

Change: `BackendTransaction` now has `get_kv_many`, `scan_kv`, and `write_kv_batch`; the storage API maps through to those hooks, and the bench backend overrides them with one-lock batch write/read paths plus BTree seek-based scans.

| Benchmark / Workload                    |   Before |    After |        Delta |
| --------------------------------------- | -------: | -------: | -----------: |
| `write_kv_batch_put/10k`                | ~4.34 ms | ~3.39 ms |  ~22% faster |
| `write_kv_batch_mixed_put_delete/10k`   | ~6.85 ms | ~5.78 ms |  ~16% faster |
| `write_kv_batch_multi_namespace/10k`    | ~4.10 ms | ~3.44 ms |  ~16% faster |
| `write_kv_batch_duplicate_keys/10k`     | ~3.12 ms | ~2.44 ms |  ~22% faster |
| `write_kv_batch_value_size/1k`          | ~5.10 ms | ~3.44 ms |  ~33% faster |
| `write_kv_batch_value_size/128k`        | ~1.02 ms | ~0.20 ms | ~5.1x faster |
| `transaction_write_and_commit/10k`      | ~4.01 ms | ~3.38 ms |  ~16% faster |
| `get_kv_many_hit/10k`                   | ~2.26 ms | ~2.46 ms |    unchanged |
| `get_kv_many_multi_namespace/10k`       | ~5.80 ms | ~6.15 ms |    unchanged |
| `scan_kv_after_pages/10k`               |  ~263 µs |  ~725 µs | ~2.8x slower |
| `scan_kv_prefix_selectivity_1pct/10k`   |  ~308 µs |  ~289 µs |   ~6% faster |
| `scan_kv_prefix_selectivity_10pct/10k`  |  ~437 µs |  ~366 µs |  ~16% faster |
| `scan_kv_prefix_selectivity_100pct/10k` |  ~705 µs |  ~719 µs |    unchanged |

Worked: write batches now avoid per-key backend dispatch/locking in the bench backend, giving a clear API-ceiling win for large writes and large values.

Did not work: `get_kv_many` did not improve reliably on the current `(namespace, key)` BTreeMap shape, and paged scans are now semantically real full pagination instead of the previous under-scanning shortcut; next step is a storage-oriented in-memory layout or real SQLite backend batch implementation.

## 2026-05-03 18:59 PDT: Storage API Batch Hook Verification

Change: reran `storage/api/in_memory` after the explicit read/write transaction cleanup; fixed the paged-scan benchmark loop to stop on `resume_after: None`.

| Benchmark / Workload                    | Previous |  Current |       Delta |
| --------------------------------------- | -------: | -------: | ----------: |
| `write_kv_batch_put/10k`                | ~3.39 ms | ~3.05 ms | ~10% faster |
| `write_kv_batch_mixed_put_delete/10k`   | ~5.78 ms | ~5.04 ms | ~13% faster |
| `write_kv_batch_multi_namespace/10k`    | ~3.44 ms | ~3.27 ms |  ~5% faster |
| `write_kv_batch_duplicate_keys/10k`     | ~2.44 ms | ~2.35 ms |  ~4% faster |
| `write_kv_batch_value_size/1k`          | ~3.44 ms | ~3.20 ms |  ~7% faster |
| `write_kv_batch_value_size/128k`        | ~0.20 ms | ~0.18 ms | ~10% faster |
| `transaction_write_and_commit/10k`      | ~3.38 ms | ~3.04 ms | ~10% faster |
| `get_kv_many_hit/10k`                   | ~2.46 ms | ~1.68 ms | ~32% faster |
| `get_kv_many_multi_namespace/10k`       | ~6.15 ms | ~4.44 ms | ~28% faster |
| `scan_kv_after_pages/10k`               |  ~725 us |  ~689 us |  ~5% faster |
| `scan_kv_prefix_selectivity_1pct/10k`   |  ~289 us |  ~271 us |  ~6% faster |
| `scan_kv_prefix_selectivity_10pct/10k`  |  ~366 us |  ~305 us | ~17% faster |
| `scan_kv_prefix_selectivity_100pct/10k` |  ~719 us |  ~683 us |  ~5% faster |

Worked: the batch hook win holds after the storage boundary cleanup, and the paged-scan benchmark now validates the explicit cursor contract instead of looping on exact final pages.

Did not work: this is still only the in-memory API ceiling; real backend numbers should be tracked separately for SQLite/RocksDB once those adapters are stable.

## 2026-05-04 10:57 PDT: Storage API Scan Cursor Prototype

Change: `scan_kv` now returns a cursor with `next_page(limit)`; RocksDB committed scans keep a raw iterator across pages instead of reopening from `resume_after`.

| Benchmark / Workload                      | Original Page API | Cursor Prototype | Delta vs Original |
| ----------------------------------------- | ----------------: | ---------------: | ----------------: |
| `rocksdb_tempdir/scan_kv_prefix/100`      |           ~450 us |          ~941 us |      ~2.1x slower |
| `rocksdb_tempdir/scan_kv_prefix/10k`      |          ~1.71 ms |         ~3.16 ms |       ~85% slower |
| `rocksdb_tempdir/scan_kv_after_pages/10k` |          ~1.83 ms |         ~2.89 ms |       ~58% slower |
| `sqlite_memory/scan_kv_prefix/100`        |            ~58 us |          ~228 us |      ~3.9x slower |
| `sqlite_memory/scan_kv_prefix/10k`        |           ~941 us |         ~1.98 ms |      ~2.1x slower |
| `in_memory/scan_kv_after_pages/10k`       |           ~689 us |          ~939 us |       ~36% slower |

Worked: the API shape now models cursors explicitly, and RocksDB paged scans improved versus the first cursor attempt (`~4.34 ms` -> `~2.89 ms`).

Did not work: cursor paging adds trait/object and adapter overhead while still returning owned `Vec` pages; SQLite and RocksDB are slower than the original page API. Next step is either revert this API cut or make cursors return borrowed/pinned rows from backend-owned page buffers before keeping it.

## 2026-05-04 11:20 PDT: Storage API Real Backend Baseline

Change: captured a compact baseline for the real-file storage API profiles after adding `sqlite_tempfile`; Criterion ran with `--sample-size 10 --warm-up-time 1 --measurement-time 1`.

| Benchmark / Workload                               | Baseline |
| -------------------------------------------------- | -------: |
| `sqlite_tempfile/write_kv_batch_put/10k`           | ~13.1 ms |
| `sqlite_tempfile/transaction_write_and_commit/10k` | ~17.4 ms |
| `sqlite_tempfile/get_kv_many_hit/10k`              | ~9.86 ms |
| `sqlite_tempfile/get_kv_many_mixed_hit_miss/10k`   | ~9.06 ms |
| `sqlite_tempfile/scan_kv_prefix/100`               | ~2.88 ms |
| `sqlite_tempfile/scan_kv_prefix/10k`               | ~5.89 ms |
| `sqlite_tempfile/scan_kv_after_pages/10k`          | ~4.56 ms |
| `rocksdb_tempdir/write_kv_batch_put/10k`           | ~6.31 ms |
| `rocksdb_tempdir/transaction_write_and_commit/10k` | ~5.89 ms |
| `rocksdb_tempdir/get_kv_many_hit/10k`              | ~7.00 ms |
| `rocksdb_tempdir/get_kv_many_mixed_hit_miss/10k`   | ~6.50 ms |
| `rocksdb_tempdir/scan_kv_prefix/100`               | ~1.19 ms |
| `rocksdb_tempdir/scan_kv_prefix/10k`               | ~2.59 ms |
| `rocksdb_tempdir/scan_kv_after_pages/10k`          | ~3.43 ms |

Worked: the compact command is now a useful future comparison target for SQLite-file and RocksDB-file backend API changes.

Did not work: first run still paid release RocksDB compilation; future reruns should be much faster with release artifacts warm.

## 2026-05-04 12:05 PDT: Projection-Aware Storage Batches

Change: `KvScanRequest` and `KvGetRequest` now carry projections; scans can request keys only, gets can request existence only, and real backends avoid returning values when callers do not need them.

| Benchmark / Workload                      |   Before |    After |       Delta |
| ----------------------------------------- | -------: | -------: | ----------: |
| `sqlite_tempfile/get_kv_many_exists/10k`  | ~9.86 ms | ~5.43 ms | ~45% faster |
| `sqlite_tempfile/scan_kv_prefix/10k`      | ~5.89 ms | ~2.77 ms | ~53% faster |
| `sqlite_tempfile/scan_kv_after_pages/10k` | ~4.56 ms | ~3.25 ms | ~29% faster |
| `rocksdb_tempdir/get_kv_many_exists/10k`  | ~7.00 ms | ~6.27 ms | ~10% faster |
| `rocksdb_tempdir/scan_kv_prefix/10k`      | ~2.59 ms | ~2.28 ms | ~12% faster |
| `rocksdb_tempdir/scan_kv_after_pages/10k` | ~3.43 ms | ~2.51 ms | ~27% faster |

Worked: projection belongs in the batch API; SQLite benefits heavily from `SELECT key` / `SELECT 1`, and RocksDB scans benefit by skipping value copies.

Did not work: RocksDB existence gets still use `multi_get` under the hood, so the win is modest until the backend has an existence-first implementation.

## 2026-05-04 12:22 PDT: RocksDB Existence-First Gets

Change: RocksDB `KvGetProjection::Existence` now uses an ordered raw-iterator key check instead of `multi_get`, so exact existence reads do not copy values.

| Benchmark / Workload                     |   Before |    After |       Delta |
| ---------------------------------------- | -------: | -------: | ----------: |
| `rocksdb_tempdir/get_kv_many_exists/10k` | ~6.27 ms | ~2.22 ms | ~65% faster |

Worked: a backend-native existence path is much faster than projecting existence out of value reads.

Did not work: this is optimized for ordered/dense key batches; sparse random batches may need a seek-vs-scan heuristic later.

## 2026-05-04 12:23 PDT: Storage KV RowBatch Boundary

Commit: `f9890744`

Change: backend/storage get and scan results now return `KvRowBatch` / `BackendKvRowBatch` with indexed accessors instead of public per-row/per-entry structs; the final representation uses one private row vector after a parallel-vector prototype regressed scan paths.

| Benchmark / Workload                      |   Before |    After |       Delta |
| ----------------------------------------- | -------: | -------: | ----------: |
| `sqlite_tempfile/get_kv_many_hit/10k`     | ~7.25 ms | ~5.79 ms | ~20% faster |
| `sqlite_tempfile/get_kv_many_exists/10k`  | ~7.05 ms | ~5.21 ms | ~26% faster |
| `sqlite_tempfile/scan_kv_prefix/10k`      | ~3.61 ms | ~2.85 ms | ~21% faster |
| `sqlite_tempfile/scan_kv_after_pages/10k` | ~4.06 ms | ~3.14 ms | ~23% faster |
| `rocksdb_tempdir/get_kv_many_hit/10k`     | ~7.48 ms | ~6.51 ms | ~13% faster |
| `rocksdb_tempdir/get_kv_many_exists/10k`  | ~2.61 ms | ~2.02 ms | ~23% faster |
| `rocksdb_tempdir/scan_kv_prefix/10k`      | ~2.76 ms | ~2.29 ms | ~17% faster |
| `rocksdb_tempdir/scan_kv_after_pages/10k` | ~2.86 ms | ~2.79 ms |   unchanged |

Worked: the API now reads as logical KV batches, keeps physical pages/chunks out of the contributor-facing model, and improves most compact real-backend paths against a clean HEAD baseline.

Did not work: the win is mostly boundary/allocation cleanup; the next scan optimization should target backend-owned borrowed page buffers or RocksDB iterator reuse, not more result-shape churn.
