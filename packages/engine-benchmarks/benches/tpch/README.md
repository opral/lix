# Neutral TPC-H benchmark

This benchmark compares DuckDB and Lix through the same Rust process and the
same owned-row result boundary. Both engines receive deterministic `tpchgen`
data with common physical types: ISO dates are `VARCHAR`, monetary values are
`DOUBLE`, and integral values are `BIGINT`. Consequently, these results must
not be compared with published native-type TPC-H results.

Run the full common-type suite with:

```sh
cargo bench -p lix_engine_benchmarks --bench tpch \
  --features storage-benches,tpch
```

Set `LIX_TPCH_SCALE_FACTOR=0.2` for more than one million `lineitem` rows and
`LIX_TPCH_SAMPLES=9` for qualification measurements. Set
`LIX_TPCH_EXPLAIN=1` to emit the Lix physical profile. Set
`LIX_TPCH_QUERY` to one integer from 1 through 22 to isolate a query. All eight
tables and Q1-Q22 are live. Its JSON `suite` field deliberately says
`tpch-derived-common-types` because the shared types differ from native TPC-H
dates and decimals.

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
allow Lix to publish its normal packed analytical base.
