# Sparse rows after a schema amendment

`sparse-default-v81.lix` is a synthetic snapshot produced by Lix commit
`feba0dd5174f512509e46e5b2d22b7d53cddf167`, which includes the original packed-index
fix but predates the schema-amendment fixes. The producer's native SDK binary
SHA-256 was `3f19b94fa089775a1e5b1a779a63fece373a54a823a93add10b477d518a8821b`.

The snapshot contains three `legacy_default` rows (`a`, `b`, `c`, each with
`value = 1`). After those rows were written, the registered schema gained a
required `priority` column with literal default `7`. The old engine accepted the
amendment without updating the durable row payloads. This fixture ensures that
new readers and write predicates agree on defaults for already-existing sparse
rows; testing only a new amendment would exercise the new backfill instead.

Snapshot SHA-256:
`16e7c1db3a2130263401bbb538d226b5490251358fc3df79aec7d1d85a15423f`.

To regenerate with a native SDK built at the producer revision:

```sh
node packages/lix/tests/fixtures/schema-amendments/generate-sparse-default.mjs \
  /path/to/historical-checkout/packages/js-sdk/dist/index.js
```

Commit IDs and timestamps are generated, so regenerated snapshots have different
bytes. The generator asserts the old predicate behavior before replacing the
fixture and refuses to run successfully with the fixed SDK.

## Missing expression defaults from an earlier engine

`sparse-expression-v81.lix` was produced by the same `feba0dd51` source revision
using its optimized native SDK (binary SHA-256
`696588e0cb9703bb63817b697b25eb82f120d96eb19c192885702a629eece814`).
It contains one `legacy_expression` row written before the schema gained
`stable_id DEFAULT uuidv7()` and `created DEFAULT CURRENT_TIMESTAMP`.
Snapshot SHA-256:
`8ffddd2337234f7858f1550d2ce07ead287eb4e18ef2e7566e7eb79b6a387ced`.
Regenerate with `generate-sparse-expression.mjs` and the same SDK-path argument.

The fixed reader does not invent a different UUID or timestamp on each read.
To repair this previously affected repository, explicitly reapply its existing
schema definition. This materializes missing defaults in an ordinary, atomic
transaction without changing the schema document:

```sql
UPDATE lix_registered_schema
SET value = value
WHERE schema_key = 'legacy_expression';
```

The SDK witness audit checks that reads fail clearly before recovery, a rolled
back recovery leaves the repository untouched, and committed values remain
identical across repeated reads and snapshot reopen. Opening or reading a
repository never performs this repair automatically.
