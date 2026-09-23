# SQL native value handling

`sql2::value_contract::SqlValue` carries SQL values during direct evaluation.
JSONB null is a value; SQL NULL is the separate `SqlNull` variant. Timestamps
remain signed UTC microseconds, UUIDs remain UUIDs, and numbers retain their
numeric kind. Results are converted to the public `Value` contract without a
JSON round trip.

Direct and DataFusion execution share physical scalar conversion and assignment
validation. Like-type direct assignments and comparisons avoid Arrow scalar array
allocation. Floating-point equality in direct predicates matches DataFusion's
comparison behavior, including signed zero and identical NaN payloads. Cross-type
casts and BIGINT assignment use DataFusion/Arrow conversion rules, including
Float64 precision loss and truncation when casting to BIGINT. Decimal and exponent
literals use DataFusion's type resolution and casts; Lix does not retain a second
numeric literal representation for SQL evaluation.
Numeric values cannot be implicitly assigned to TIMESTAMPTZ.
JSONB-to-scalar assignments require an explicit CAST. JSONB assignment accepts
SQL text as JSON text when valid, or as a JSON string otherwise; explicit
`CAST(... AS JSONB)` requires valid JSON. Native JSONB parameters use the same
canonicalization as explicit casts; binary JSON input must be valid UTF-8.
Explicit TEXT casts create fresh TEXT metadata even if the physical input is
already UTF-8 JSONB. Unsigned results outside public BIGINT range are rejected
instead of changing their public value type to TEXT.

Generic updates and transaction-staged row evaluation retain native typed rows.
JSON serialization is limited to explicitly JSON-facing boundaries. The durable
row format already distinguishes SQL NULL and JSONB null, so this change does
not require a storage version bump. Existing repositories keep the values they
actually stored; an earlier write that already collapsed JSON null to SQL NULL
cannot be reconstructed from storage. Old JSON snapshot formats remain the
responsibility of Lix's existing repository migration logic.

## Profiling

The opt-in benchmark measures the target statement only, excluding database
creation, schema registration, and seed inserts. It rotates four cases across
rounds, discards the first round, and verifies affected row counts. Each fixture
contains 500 rows with text, BIGINT, JSONB, and TIMESTAMPTZ columns.

```sh
cargo bench -p lix --bench sql_value_handling
```

Cases: direct UPDATE, direct UPDATE with RETURNING, generic UPDATE forced by
LIKE, and timestamp-predicate UPDATE. Report compiler/profile and host with
results; debug/test builds are diagnostic comparisons, not release throughput.

### Latest-main comparison (2026-09-23)

Linux x86_64, AMD Ryzen 9 9950X, Rust nightly 2026-05-21. Release benchmark
binaries were run on CPU 0, latest `main` (`657b81881`, DataFusion 53.1.0)
first and this branch (DataFusion 55.1.0) second. Each binary reports the
median of nine measured samples per case.

| Statement, 500 affected rows | Latest main | This branch | Change |
| --- | ---: | ---: | ---: |
| Direct UPDATE | 2.933 ms | 3.169 ms | +8.1% |
| Direct UPDATE with RETURNING | 3.137 ms | 3.384 ms | +7.9% |
| Generic UPDATE using LIKE | 3.865 ms | 3.991 ms | +3.3% |
| Timestamp-predicate UPDATE | 3.316 ms | 3.509 ms | +5.8% |

The branch profile showed `serde_json::Map` `IndexMap` insertion and clone work
that does not appear in the baseline. DataFusion 55 enables
`serde_json/preserve_order` for ordered PostgreSQL JSON plan display, which
changes the map implementation across the resolved dependency graph. JSONB
normalization now sorts maps in place instead of rebuilding them, removing the
observed `IndexMap::insert_full` hotspot. This reduced allocation work but did
not erase the measured end-to-end difference. Treat the remaining difference
as a correctness-related DataFusion upgrade cost: DataFusion 55 provides the
SQL planning APIs used to delegate full `RETURNING` projections to DataFusion.
The measurements are diagnostic and come from one paired run on a shared host.

### Historical paired diagnostic run (2026-09-21)

Linux x86_64, AMD Ryzen 9 9950X, Rust nightly 2026-05-21. The driver was
linked to the test-profile engine libraries (engine opt-level 0, dependency
opt-level 1), with both variants pinned to CPU 0. The baseline contains the
original nullable-JSONB fix (`bc1fb730f`), before the native-value refactor.
Three paired runs alternated before/after order; each run contains nine measured
samples per case. The table reports the median of the three paired changes in
per-run median duration, not a ratio of unpaired aggregate timings.

| Statement, 500 affected rows | Median paired duration change |
| --- | ---: |
| Direct UPDATE | -7.29% |
| Direct UPDATE with RETURNING | -7.29% |
| Generic UPDATE using LIKE | -5.15% |
| Timestamp-predicate UPDATE | -0.61% |

These measurements show no material slowdown in the exercised paths. The host
was shared and one pair showed substantial timing drift, so they are diagnostic
regression checks, not evidence of a release-build speedup. Repeat the benchmark
under an isolated release build before making throughput claims.
