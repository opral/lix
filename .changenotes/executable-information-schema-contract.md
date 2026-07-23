---
type: minor
---

Made `information_schema.columns` the executable Lix SQL contract.

Public columns now use canonical SQL type names, including `BYTEA` for binary
data across reads and writes. JSON-backed text is identified through
`lix_value_kind`, while `lix_insert_policy` and `column_default` describe
omission independently from read nullability. Defaulted ids may be omitted but
explicit `NULL` is rejected.

The advertised scalar type names now work as explicit casts across reads and
runtime-entity writes. Bound writes use the canonical names and retire
`BINARY` in favor of `BYTEA`; read expressions retain DataFusion's wider cast
dialect.

Typed `BIGINT` columns normalize mathematically integral JSON numbers such as
`1.0` and reject non-integral or out-of-range stored values instead of
projecting `NULL`. This contract also applies to typed history, pushed filters,
bound numeric predicates, and `DELETE ... RETURNING`. SQL decimal literals are
now kept distinct from values produced by `lix_json(...)`, so numeric
comparisons no longer acquire JSON comparison rules accidentally. BIGINT
writes and predicates parse integer, decimal, and exponent spellings exactly,
preventing out-of-range literals from rounding onto an in-range boundary.

Registered-entity defaults are materialized before `ON CONFLICT` routing and
`excluded.*` evaluation. `INSERT ... RETURNING` and `UPDATE ... RETURNING` are
now rejected explicitly until those result paths are implemented; they are no
longer accepted while silently discarding the requested rows.
