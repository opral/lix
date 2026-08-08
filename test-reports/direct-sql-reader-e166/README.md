# Direct SQL ForkTree reader acceptance oracle

This is a test/report-only source gate anchored to exact Lix commit
`e1666edd0b4d814a88d985086ecc5a477b5d32e6` (tree
`c680bd7e7f7b70cd784676515839af2dcbbc7917`). It contains no production
change and does not compile or execute the repository.

The gate is deliberately discriminating. The positive checks describe the
reader behavior that already exists at the anchor; the RED checks describe
the competing current-state owners that must disappear in a corrected
successor. A control run is successful only when the positive checks pass and
all expected REDs are observed. A candidate run is successful only when the
same positive checks pass and every RED control is absent.

## Required authority and semantics

The direct SQL reader must use one operation-owned `StorageRead` and one
authenticated ForkTree coherent view. ForkTree state rows are the only
current-state authority. The reader must:

- merge global and branch-local rows in authenticated key order, with the
  branch row winning on equal keys;
- apply `entity_pk` and other key filters before applying `limit`;
- preserve `StateCell::Null` as a present, non-deleted SQL NULL;
- omit tombstones unless the request explicitly includes them;
- fail closed on view, state-range, state-point, key-decode, and row-auth
  errors;
- reject unsupported derived/history/ambiguous lanes before producing a
  partial result; and
- support untracked rows through the same operation view where that lane is
  explicitly selected.

No direct SQL path may retain or invoke a tracked-head reader, a scalar or
columnar current-state owner, an entity snapshot/columnar cache, a generic
fallback to the deleted reader, a raw read getter, or a helper that opens a
second read. No durable columnar DTO may remain a current-state authority.
Unrelated filesystem-path indexing is outside this source gate; it must not
be used to answer current-state rows.

## Replay

From the repository root, run the source gate against the isolated worktree:

```sh
bash test-reports/direct-sql-reader-e166/verify_direct_reader.sh . control
```

The control command must exit zero while printing the expected e166 REDs. A
future immutable successor can be checked with `candidate`; that mode exits
nonzero until all forbidden owners and fallback paths are removed:

```sh
bash test-reports/direct-sql-reader-e166/verify_direct_reader.sh \
  /path/to/successor candidate
```

`cases.tsv` is the machine-readable contract. `CONTROL_REPORT.md` records the
calibrated anchor result and exact source identities. This package intentionally
has no Cargo/runtime gate; the requested current turn is source/report-only.
