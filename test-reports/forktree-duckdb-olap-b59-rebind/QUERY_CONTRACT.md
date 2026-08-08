# Frozen OLAP fixture/query contract

This is the exact query and result-order contract inherited from the nearest
comparator input. It is a future b59 acceptance requirement, not a b59 run.

## Fixture

```text
LANES = 32
WIDE_COLUMNS = 16
WIDE_PAYLOAD_BYTES = 256
narrow id      = /~forktree-olap/{ordinal:09}
lane           = ordinal % 32
score          = (ordinal * 97 + 13) % 100003
active         = ordinal % 3 != 0
wide c[column] = (ordinal * (column + 17) + column * 31) % 1000003
dimension      = (lane, dimension-{lane:02}) for lane 0..31
```

The rows are generated for each of 10,000, 50,000, and 500,000 ordinals. The
fixture is freshly materialized per adapter/size cell and is excluded from
timed query samples.

## Query order and semantics

Run in this exact order:

| # | label | SQL shape | exact result ordering/limit |
|---:|---|---|---|
| 1 | `pk_point` | `SELECT id, score FROM forktree_olap_narrow WHERE id = '/~forktree-olap/000000123'` | One row, primary-key point identity |
| 2 | `pk_range` | `SELECT id, ordinal FROM forktree_olap_narrow WHERE id >= '/~forktree-olap/000000120' AND id < '/~forktree-olap/000000130' ORDER BY id` | Ten rows, ascending `id` |
| 3 | `narrow_scan` | all narrow columns, `ORDER BY ordinal` | Every narrow row, ascending ordinal |
| 4 | `wide_scan` | all narrow + 16 wide columns + payload, `ORDER BY ordinal` | Every wide row, ascending ordinal |
| 5 | `filtered_scan` | `active = TRUE AND lane IN (7, 19) ORDER BY ordinal` | Matching rows, ascending ordinal |
| 6 | `group_by` | active rows grouped by lane with count/sum/min/max | Lane ascending; aggregate columns are count, ordinal sum, score min, score max |
| 7 | `order_limit` | active rows `ORDER BY score DESC, ordinal ASC LIMIT 1000` | Score descending, ordinal ascending tie-break; exactly min(1000, active rows) |
| 8 | `simple_join` | active narrow rows joined to dimension on lane, `ORDER BY ordinal` | Matching rows, ascending ordinal; dimension label included |
| 9 | `column_projection` | wide `id,score ORDER BY ordinal` | Every wide row, ascending ordinal |

The implementation must preserve this order after provider planning and must
not use arrival order as a semantic result order. The b59 harness must execute
the same query list against its authenticated ForkTree provider and the
standalone DuckDB control.

## Exact digest

For each result, initialize a BLAKE3 hasher and append:

1. result row count as unsigned 64-bit big-endian;
2. for each row, column count as unsigned 64-bit big-endian;
3. for each typed cell, the tag and payload below.

```text
Null:    0x00
Integer: 0x01 || i64.to_be_bytes()
Text:    0x02 || u64(len(utf8)).to_be_bytes() || utf8 bytes
Boolean: 0x03 || 0x00 or 0x01
```

The future report must publish the 64-hex digest for every query/size/backend
and prove:

```text
model digest == DuckDB warm digest == ForkTree warm digest == ForkTree cold digest
```

The available comparator report says this equality passed in its own 2a0
run, but its raw logs are not present here and the result digests are not
replayed or relabeled as b59 evidence.

## Result-row provenance and setup boundary

`RESULTS.csv` is intentionally a historical timing ledger. Its 27 rows are
the nine query labels at 10K, 50K, and 500K from the nearest cd76 comparator;
they are not b59 measurements. Every row must contain these fields:

```text
source_kind,source_ref,source_sha256,b59_cell_status,setup_excluded,
rows,query,historical_forktree_rocks_us,historical_forktree_slate_us,
historical_duckdb_us,historical_rocks_over_duck,historical_slate_over_duck,
result_digest,reopen_digest,verified,
setup_wall_ns,query_wall_ns,query_cpu_ns,alloc_bytes,rss_peak_bytes,
backend_reads,backend_read_keys,backend_read_bytes,backend_writes,
backend_write_bytes,physical_read_objects,physical_read_bytes,
physical_write_objects,physical_write_bytes,publication_count,selector_cas,
epoch_cas,vc_reads,vc_writes,oltp_calls,filesystem_calls,cold_reopen
```

Historical rows must use `source_kind=historical-cd76-timing-only`, the exact
cd76 ref and source-file SHA, `b59_cell_status=UNRUN`,
`setup_excluded=TRUE`, and `UNRUN` for every b59 digest/counter/verification
field. A future b59 row replaces only the UNRUN fields with exact measured
values and keeps its own candidate/source hash. No inherited timing or digest
may be relabeled as b59.

## Required counters

Every query sample must publish:

```text
begin_reads, authenticated_get_calls, get_keys, get_value_bytes,
scan_calls, scan_rows, provider_plan_ns, provider_filter_ns,
provider_materialize_ns, operator_ns, allocation_calls, allocation_bytes,
rss_peak_bytes, physical_read_objects, physical_read_bytes,
physical_write_objects, physical_write_bytes, query_writes,
vc_reads, vc_writes, publication_count, selector_cas, epoch_cas,
oltp_calls, filesystem_calls, cold_reopen, result_digest, verified
```

Read-only OLAP cells require zero query writes, VC writes, publication,
selector/epoch CAS, OLTP mutation calls, and filesystem mutation calls. One
coherent authenticated read is required for each logical query. DuckDB's
`logical_reads=unavailable` and Rust-only allocation scope must remain labeled
as such rather than compared as ForkTree authority counters.

## Corruption and reopen controls

Before any timed b59 query, run the named matrix in `CORRUPTION_MATRIX.md` on
fresh selector/root/object fixtures. For each of global selector, branch
selector, state root, catalog root, and checkpoint root, inject malformed,
missing, wrong-kind, and identity-substitution bytes. Require the exact typed
failure, one retained coherent read, unchanged before/after authority
fingerprint, zero writes/publication/selector-CAS/epoch-CAS, and no fallback.
An optional authenticated object that is absent must return `ValidAbsence`, not
the typed missing-required-object failure. Flush, drop, reopen, and repeat the
healthy digest and corruption controls; any surviving root or digest mismatch
is a blocker.
