# Authenticated ForkTree versus DuckDB OLAP comparator

## Verdict

**GO for a benchmark-only architectural follow-up; no production cut in this lane.** Exact result digests, zero query writes, and cold reopen passed for all nine queries at 10K, 50K, and 500K on authenticated ForkTree over RocksDB and SlateDB and on standalone DuckDB. DuckDB has no Lix version-control, authentication, branch, or repository-authority semantics; its numbers isolate query-engine and materialization ceilings rather than provide an authority-equivalent replacement.

ForkTree's authenticated point and ten-row range path is already competitive. At 50K, RocksDB ForkTree is 25% faster than DuckDB for the point and 76% faster for the range; SlateDB is 16% slower for the point and 70% faster for the range. The general gap is after authenticated traversal: at 50K DuckDB is 14-30x faster than RocksDB ForkTree and 25-47x faster than SlateDB ForkTree for filter/group/order. The gap grows at 500K.

The smallest credible cut is to stream authenticated value-pack groups directly into bounded Arrow builders and DataFusion `RecordBatch`es. Delete the intermediate all-row `Vec<Vec<Datum>>` and the second row-to-column copy; keep one immutable blocked tree and authenticate the complete containing object before exposing any dependent field. This has a measured 51-60% provider/materialization wall ceiling at 50K on both adapters and changes working memory from all-result ownership to bounded batch ownership. A second, narrower cut is compiled predicate evaluation during authenticated pack decode so rejected rows never acquire full `Datum` ownership. No columnar authority, cache, index, second format, or DuckDB integration is proposed.

## Exact provenance

- ForkTree model head: `2a0e8512bb37c9da2050c99c366e5ac05bb01553` (tree `0b87e60`; parent authenticated range/projection cut `1047f895f7b48bf16b6114d68c112acab1988203`). A remote-ref audit found later Stage-2 compiler-red/source-review milestones but no newer accepted runnable honest OLAP subject, so this remains the primary subject.
- Semantic control: exact current main `c8c7899912a661b7bbd802eaced3c076f52876e5` (tree `e857186a`), which differs from accepted `a12b76c8` only by deleting one documentation line. It was not recompiled or profiled in this corrected lane. Existing a12 public-SQL plans/digests remain the legacy semantic control.
- DuckDB crate: exact `duckdb 1.10505.0`, bundled native library, standalone file-backed database.
- ForkTree binary SHA-256: `9ad1ec06f485be1ab051c9fbc95388bdd07f63ad036da766e015d7443c19e81c`.
- DuckDB binary SHA-256: `3348eddda982394f2dfee87e1c2292e615829ebeb6ee65bf8cadddf68788b3e7`.
- Measured ForkTree source blobs: `main.rs` `105f7143c0100b69a11b0ef858e21ec7a8be4f6b10ee16516b2f69a5743d9e7c`, `olap_common.rs` `5fae129c1d9c96e3756fbf3e6947212315b59feeb967351421aef382d89b0411`, `olap_datafusion.rs` `ede8480c99a6a4e50b8f41768d56e9a95eb0ae39df92e6f3af2a1cbe196da753`.
- A post-measurement bounded release rebuild of the same source also passed in 15m01s and produced `791677baea641b1c311f73d9e7481989fc544c7f4dd5613ef554b216a8eae03e`; its different Cargo artifact hash is not used to relabel the raw measurements, and no matrix rerun was needed.
- Three measured samples after one warm-up; fixture construction, flush, and first close are excluded. Queries execute after a cold file reopen. A final drop/reopen repeats every exact result.

## Big-O and authority boundary

Let `H` be authenticated tree height, `P` touched immutable blocks/value packs, `N` visited rows, `K` output rows, `W` selected decoded width, and `S` operator state.

- ForkTree point: `O(H)` authenticated work and `O(H)` object keys; observed 6-7 batched gets from 10K through 500K.
- ForkTree primary-key range: `O(H + P + K)` work and bytes; observed the same 6-7 get rounds for the fixed ten-row range.
- Current full scan/provider: `O(H + P + N*W + operator(N))` time and `O(N*W + S)` working ownership. Adapter calls are already `O(H)` (6-7), but each call contains all keys at that dependency level.
- Proposed block-to-Arrow stream: unchanged `O(H + P + N*W + operator(N))` time, but one decode/copy ownership and `O(batch*W + S)` memory. The meaningful win is the measured constant-factor deletion plus bounded RSS, not an asymptotic time claim.
- Filter remains `O(N)` without a second index. Predicate-on-decode changes allocation from `O(N*decoded_width)` toward `O(K*selected_width + batch)` while preserving complete pack authentication.
- Group/hash join remain `O(N)` expected; top-K order is `O(N log K)`. Bounded batches let DataFusion vectorize those operators without globally materializing provider rows.

## Causal attribution

The authenticated source boundary is not the dominant general term:

1. `ForkTree::read_projected_range` opens one coherent `StorageRead`, resolves branch/commit/root, traverses path levels with `get_many`, authenticates every object, deduplicates packs, and fully decodes a pack before projection (`model.rs:1051-1129`, object authentication at `model.rs:3274-3305`).
2. `ForkTreeScanExec::load_batch` then owns every decoded row in one vector, evaluates pushed filters with a scalar expression walk, collects selected rows, and calls `rows_to_batch` (`olap_datafusion.rs:234-273`).
3. `rows_to_batch` walks the rows once per selected column, first collecting temporary vectors and then creating Arrow arrays (`olap_datafusion.rs:975-1024`). This is a second ownership/copy plane and creates one giant batch.
4. The frozen lower-layer 1047 oracle measures the same authenticated traversal and canonical operators without the DataFusion provider conversion. At 50K its medians versus this provider leave 51-60% of wall removable on every query on both adapters: Rocks narrow 17.68->39.93 ms, filter 16.41->38.57, projection 25.92->64.72; Slate narrow 28.91->61.59, filter 27.49->61.15, projection 36.72->88.66.

The 500K allocation counts agree with this ownership diagnosis: Rocks allocates 359 MB for a filter returning 20,834 rows and 1.13 GB for two-column projection; Slate allocates 644 MB and 1.44 GB. DuckDB's Rust counters cover only the Rust output bridge, not its C++ allocator, so allocator deltas are not compared directly. Whole-process peak RSS is comparable as a coarse bound: 1.35 GiB Rocks, 1.28 GiB Slate, 1.19 GiB DuckDB.

DuckDB's physical plans use vectorized sequential scan with projection/filter pushdown, perfect-hash aggregation, Top-N, and hash join. Its point/range plans still use sequential scan in this fixture, which explains why ForkTree's authenticated key pruning wins the range despite authentication.

## Median results

Full wall medians and ratios are in `RESULTS.csv`. Representative values:

| Rows | Query | ForkTree Rocks | ForkTree Slate | DuckDB | Rocks / Duck | Slate / Duck |
|---:|---|---:|---:|---:|---:|---:|
| 10K | point | 0.249 ms | 0.287 ms | 0.227 ms | 1.10x | 1.26x |
| 10K | range (10) | 0.339 ms | 0.460 ms | 1.601 ms | 0.21x | 0.29x |
| 50K | filter | 38.57 ms | 61.15 ms | 1.295 ms | 29.79x | 47.22x |
| 50K | group | 37.47 ms | 59.50 ms | 1.722 ms | 21.76x | 34.56x |
| 50K | projection | 64.72 ms | 88.66 ms | 7.358 ms | 8.80x | 12.05x |
| 500K | filter | 424.0 ms | 682.1 ms | 5.160 ms | 82.18x | 132.19x |
| 500K | group | 417.9 ms | 623.9 ms | 2.845 ms | 146.89x | 219.31x |
| 500K | join | 481.2 ms | 709.7 ms | 78.31 ms | 6.14x | 9.06x |

Model CPU tracks wall within noise, indicating an in-process CPU/ownership term rather than backend wait. DuckDB frequently uses more than one CPU, so both wall and CPU are retained in raw logs. Query-local writes are zero in all runs.

Settled post-reopen disk at 10K/50K/500K is 0.93/4.39/41.69 MB Rocks, 0.89/4.48/43.18 MB Slate, and 3.16/10.76/99.89 MB DuckDB. These are format observations, not a ForkTree storage win: DuckDB lacks authenticated and version-control ownership, while the ForkTree model omits the full current Lix catalog/history surface.

## Slate dependency-object boundary

The known strict issue remains explicit. At 10K, Slate reads 5 physical objects for one range and 10 for the join. At 50K it reads 6 and 12, versus the legacy control's 5 and 10. At 500K medians rise to 7 for narrow scans, 11 for the wide scan, and 13 for the join. Logical `get_many` rounds remain 7 (14 for join), so this is physical object-layout amplification rather than row-at-a-time adapter calls. A production ForkTree acceptance must resolve or explicitly accept this boundary independently of the Arrow/materialization cut.

## Ranked follow-ups

1. **Bounded authenticated block-to-Arrow batches — admit.** Authenticate and structurally validate each immutable pack once, append selected fields directly into Arrow builders, yield bounded batches, and delete `Vec<Vec<Datum>>` plus `rows_to_batch`. Measured perfect-elimination ceiling: 51-60% provider wall at 50K on both adapters; major RSS/alloc reduction at 500K. This retains one immutable blocked tree.
2. **Compile pushed predicates into the decode loop — admit after (1).** The 500K filter emits 4.2% of rows but allocates 359/644 MB. Reject rows before `Datum` ownership while still authenticating the complete pack. No index or second authority; time remains `O(N)`.
3. **Keep DataFusion aggregation/Top-K/join vectorized over bounded batches — admit as a consequence of (1), not bespoke stored summaries.** DuckDB establishes a >20% engine ceiling, but authenticated aggregate summaries or a columnar second format are out of scope and would violate the authority constraint.
4. **Slate physical object coalescing — separate blocker.** Investigate the 6-vs-5 and 12-vs-10 dependency read only within the single-object authority/layout; do not mask it with a cache.

## Reproduction

```text
timeout 20m env TMPDIR=/root/projects/olap-comparator-db /usr/bin/time -v \
  /root/projects/olap-comparator-target-model-2a0/release/deps/forktree_replacement-261756d8afb8dfb9 \
  olap-datafusion {rocksdb|slatedb} forktree {10000|50000|500000} 32 3 1 1

timeout 20m env TMPDIR=/root/projects/olap-comparator-db /usr/bin/time -v \
  /root/projects/olap-comparator-target-duckdb-1.10505/release/forktree-duckdb-comparator \
  {10000|50000|500000} 3 1
```

Raw logs are setup-excluded and include wall/CPU, allocation counters, RSS, logical reads, Slate physical objects/bytes, Rocks process I/O, zero writes, result cardinality/digest, post-close disk, and cold-reopen assertions. Their immutable SHA-256 values are in `RAW_SHA256SUMS`.
