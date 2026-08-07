# ForkTree OLAP non-regression A/B

## Verdict

**BLOCKER.** The accepted ForkTree prototype does not have SQL/DataFusion provider wiring, so this is a source-equivalent lower-bound comparison, not a SQL-level acceptance result. Exact current main executes the seven workloads through the public SQL/DataFusion path. ForkTree reads the same deterministic logical rows through authenticated `read_range`/`read_relational_all` materialization and applies the identical canonical operators and result digest.

At 10,000 rows, ForkTree exceeds the 5% critical-regression threshold for every workload on both RocksDB and SlateDB. The 50,000-row matrix was therefore gated off.

## Exact inputs

- Current main: `f77f5b9e2ff582f749d1c487d95e6c0e8e4d3662`
- Accepted ForkTree prototype/model: `bc82385ec42b1789018fbd1213f637c19104a02c`
- Rows: 10,000; three measured samples after one warm-up; setup excluded.
- Queries: narrow full scan, wide full scan, filtered scan, GROUP BY aggregate, ORDER/LIMIT, simple join, and column projection.
- Correctness: every measured sample matched the canonical result count and digest; a cold reopen repeated all seven exact results on both adapters.

## Median measurements

Wall and allocation deltas are ForkTree relative to current main. Backend gets are logical adapter `get_many` calls. Slate physical reads are object count / bytes. RocksDB does not expose equivalent physical-read counters in this harness; its logical counters are reported instead.

| Adapter | Query | Current wall (ms) | ForkTree wall (ms) | Wall delta | Current alloc | ForkTree alloc | Alloc delta | Backend gets | Logical read bytes | Slate physical reads |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| RocksDB | narrow scan | 12.20 | 39.68 | +225% | 32.6 MB | 67.2 MB | +106% | 18 -> 10,165 | 0.41 -> 4.07 MB | n/a |
| RocksDB | wide scan | 37.31 | 171.89 | +361% | 74.1 MB | 614.9 MB | +730% | 18 -> 10,166 | 1.10 -> 42.09 MB | n/a |
| RocksDB | filtered scan | 9.15 | 38.27 | +318% | 32.7 MB | 65.5 MB | +101% | 18 -> 10,165 | 0.41 -> 4.07 MB | n/a |
| RocksDB | GROUP BY | 10.35 | 38.09 | +268% | 33.5 MB | 65.5 MB | +95% | 18 -> 10,165 | 0.41 -> 4.07 MB | n/a |
| RocksDB | ORDER/LIMIT | 10.67 | 38.05 | +257% | 34.3 MB | 65.7 MB | +91% | 18 -> 10,165 | 0.41 -> 4.07 MB | n/a |
| RocksDB | simple join | 12.30 | 38.44 | +213% | 38.8 MB | 66.8 MB | +72% | 33 -> 10,202 | 0.42 -> 4.08 MB | n/a |
| RocksDB | projection | 22.48 | 171.87 | +665% | 40.9 MB | 602.8 MB | +1,374% | 18 -> 10,166 | 1.10 -> 42.09 MB | n/a |
| SlateDB | narrow scan | 12.52 | 62.61 | +400% | 33.6 MB | 139.0 MB | +313% | 18 -> 10,165 | 0.41 -> 4.07 MB | 5 / 0.41 MB -> 10,164 / 4.23 MB |
| SlateDB | wide scan | 37.30 | 196.31 | +426% | 75.8 MB | 725.1 MB | +856% | 18 -> 10,166 | 1.10 -> 42.09 MB | 5 / 1.10 MB -> 10,165 / 42.25 MB |
| SlateDB | filtered scan | 9.18 | 62.82 | +584% | 33.7 MB | 137.6 MB | +308% | 18 -> 10,165 | 0.41 -> 4.07 MB | 5 / 0.41 MB -> 10,164 / 4.23 MB |
| SlateDB | GROUP BY | 10.44 | 62.50 | +499% | 34.6 MB | 137.3 MB | +297% | 18 -> 10,165 | 0.41 -> 4.07 MB | 5 / 0.41 MB -> 10,164 / 4.23 MB |
| SlateDB | ORDER/LIMIT | 10.70 | 62.73 | +486% | 35.4 MB | 137.4 MB | +288% | 18 -> 10,165 | 0.41 -> 4.07 MB | 5 / 0.41 MB -> 10,164 / 4.23 MB |
| SlateDB | simple join | 12.36 | 63.28 | +412% | 40.5 MB | 138.8 MB | +243% | 33 -> 10,202 | 0.42 -> 4.08 MB | 10 / 0.42 MB -> 10,200 / 4.24 MB |
| SlateDB | projection | 23.57 | 195.51 | +730% | 42.6 MB | 712.6 MB | +1,571% | 18 -> 10,166 | 1.10 -> 42.09 MB | 5 / 1.10 MB -> 10,165 / 42.25 MB |

CPU time tracked wall time within measurement noise in every median. Query-local RSS did not grow in the stable samples; absolute process RSS is not directly comparable because current main retains SQL/DataFusion table state while the model process retains only its synthetic tree fixtures. Settled 10K database size was approximately 1.60 MB vs 0.93 MB on RocksDB and 1.55 MB vs 0.89 MB on SlateDB, but this is not a storage-size win claim: the source-equivalent model omits current SQL/catalog/history ownership.

## Causal term and ceiling

The dominant term is row/value-at-a-time authenticated materialization. The prototype performs about 10.2K adapter reads and Slate object reads per query, while current main batches the same logical scan into 18 gets and 5 objects (33/10 for the join). Wide scan and projection additionally materialize every full wide row, increasing logical reads from about 1.10 MB to 42.09 MB. Projection therefore pays full-row width despite selecting two columns.

The perfect-elimination ceiling for request scheduling is the measured ForkTree source-layer time minus current-main time: 69-88% of wall time depending on query/adapter. Eliminating only request dispatch is insufficient for projection: full-row decoding/copying must also be avoided. These are lower-bound ceilings; SQL provider/planner overhead would be additive.

Current main scan and operator work is `O(N + Q)`, with backend request work approximately `O(P)` for batched pages/objects and projection bytes proportional to selected columns. The measured ForkTree prototype is also `O(N + Q)` algorithmically, but backend requests are `O(N)` and wide projection bytes are `O(N * W)`. A non-regressing ForkTree provider must retain one authoritative blocked tree while making request work `O(P + L)` through coherent range/path batching and projection bytes `O(N * selected_width + tree_metadata)`. This report does not implement or validate that provider.

## Implementer contract for Ryzen-V

Before production integration, the ForkTree SQL provider must:

1. expose authenticated ordered range iteration without one transaction/get per row or value;
2. batch each path level or contiguous leaf/object run coherently so Slate object reads scale with touched blocks, not rows;
3. support field/column projection before full wide-row materialization;
4. preserve the existing ObjectId/root authentication and fail-closed behavior—no cache or second authority;
5. rerun this exact seven-query 10K gate through honest SQL/DataFusion wiring, requiring every critical wall/CPU/allocation/backend metric to stay within 5% before admitting 50K.

No production source, alternate OLAP format, cache, index, Stage 2 path, or PR was created by this benchmark lane.
