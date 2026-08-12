# Neutral TPC-H benchmark

This benchmark compares DuckDB and Lix through the same Rust process and the
same owned-row result boundary. Both engines receive deterministic `tpchgen`
data with common physical types: ISO dates are `VARCHAR`, monetary values are
`DOUBLE`, and integral values are `BIGINT`. Consequently, these results must
not be compared with published native-type TPC-H results.

Run the full common-type suite with:

```sh
cargo bench -p lix_e2e --bench tpch \
  --features storage-benches,tpch
```

Set `LIX_TPCH_SCALE_FACTOR=0.2` for more than one million `lineitem` rows and
`LIX_TPCH_SAMPLES=9` for qualification measurements. Set
`LIX_TPCH_EXPLAIN=1` to emit the Lix physical profile. Set
`LIX_TPCH_QUERY` to one integer from 1 through 22 to isolate a query. All eight
tables and Q1-Q22 are live. Its JSON `suite` field deliberately says
`tpch-derived-common-types` because the shared types differ from native TPC-H
dates and decimals.

Every timed JSON record includes phase medians. Lix reports four disjoint wall
phases: SQL logical planning, DataFusion physical planning, Arrow physical
execution/collection, and public-result materialization. The remaining public
call time is reported as `lix_unattributed_overhead_median_ms`; it includes
snapshot acquisition, provider registration, and routing, so the named phases
are not made to absorb setup work they do not perform. Common result
normalization used only by this harness is reported separately.

DuckDB exposes a different honest boundary: `prepare`, `query_arrow`, Arrow
iterator collection, and owned-row materialization. `query_arrow` executes the
query and constructs DuckDB's internal result, so the benchmark deliberately
does not claim a finer physical-planning/execution split for DuckDB.

Lix also reports generic scan output rows, batches, and Arrow in-memory bytes.
`lix_scan_elapsed_operator_sum_median_ms` sums time spent polling all scan
partitions and may overlap the physical-execution wall phase; it must not be
added to the four disjoint phases. `lix_scan_arrow_bytes_median` is the Arrow
array footprint at scan output, not storage-backend I/O bytes.

Set `LIX_TPCH_OVERLAY=sparse` to update a deterministic 0.1% sample of
`lineitem`, or `LIX_TPCH_OVERLAY=moderate` for a 5% sample. The default is
`pristine`. Overlay mutations are applied to both engines after their initial
load and before validation or timing. They replace a query-visible numeric
column, so every run verifies that DuckDB and Lix expose the same logical
state while Lix reads its packed base plus committed transactional changes.
The replacement value is outside TPC-H's generated quantity range, guaranteeing
that every selected row produces a real delta.
Each JSON record includes the exact selected row count and fraction.

Q17 is a regression guard for the correlated aggregate-statistics correctness
defect fixed by <https://github.com/opral/lix/pull/1137>.

The padded storage identity columns are present in both engines and absent
from benchmark SQL. They make the initial load deterministically ordered and
allow Lix to publish its normal packed columnar base.
