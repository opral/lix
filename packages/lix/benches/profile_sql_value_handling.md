# SQL native value handling

`sql2::value_contract::SqlValue` carries SQL values during direct evaluation.
JSONB null is a value; SQL NULL is the separate `SqlNull` variant. Timestamps
remain signed UTC microseconds, UUIDs remain UUIDs, and numbers retain their
numeric kind. Results are converted to the public `Value` contract without a
JSON round trip.

Direct and DataFusion execution share physical scalar conversion and assignment
validation. Like-type direct assignments and comparisons avoid Arrow scalar
array allocation. Floating-point equality follows Arrow's total-order semantics, including signed
zero. Cross-type casts use Arrow's conversion rules. BIGINT
assignment additionally requires an exact, in-range integer; an explicit CAST
remains a cast. Decimal/exponent predicate literals retain their source spelling
until BIGINT type resolution, so they cannot silently round through `f64`.
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
