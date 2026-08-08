# Historical point/scan fail-closed binding — SQL reader frontier 413e08a

Status: TEST/REPORT ONLY. This successor adds no production source, adapter,
format, writer, selector, cache, compatibility, or fallback code. It binds the
previous immutable oracle 448624a to the exact SQL reader frontier 413e08a and
freezes the function-scoped acceptance contract for the missing-catalog/root
correction.

## Immutable provenance

| object | value |
|---|---|
| historical oracle head | `448624a557bca2c341f4a1820b79222a5691613a` |
| historical oracle tree | `b618b0f60e614d76c1afdb04280807d197bda8a2` |
| historical oracle parent/source | `e1666edd0b4d814a88d985086ecc5a477b5d32e6` |
| oracle parent..head full-index SHA-256 | `87df0c74ee093d2058b75c7a2d868a67d8186769efeccd28d5fd8c3cc0732967` |
| oracle patch ID | `f5cc97d2027383420f8043ae9a791e36cca36b1a` |
| source frontier head | `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d` |
| source frontier tree | `820fe560da3bbd2b00b788b0b1759c409048cd6e` |
| source frontier parent | `11442c1e0023e20307a7231d88cd557bc704fd13` |
| source frontier merge-base with e166 | `e1666edd0b4d814a88d985086ecc5a477b5d32e6` |
| e166..413 full-index SHA-256 | `70bc6bc03524855be515c9d1a5d0c75c77ebd159fbd44d5f646483ce14460329` |
| e166..413 stable patch ID | `df0747c2c7e026147361aab7edd4f741efca9b33` |
| 11442..413 full-index SHA-256 | `e9be5053f44fa9e009aaa665b69d328f6ee0ac718b18e773fb79a2eb6d7af8d4` |
| 11442..413 stable patch ID | `02310ae525c028488e654d3cb26eb7d1f85974cb` |

The prior oracle remains unchanged on its original ref. This successor is a
report-only child of 448624a; it is not a rebase or rewrite of that oracle.
The mechanical binding is source-function based: run the inherited pure model
and matrix against the replacement functions named below, while the frozen
413 verifier proves the frontier still contains the exact pre-correction RED.

## What 413 changes, and what it does not

The 11442..413 production diff is exactly:

```text
M packages/lix/src/forktree/view.rs
M packages/lix/src/live_state/context.rs
M packages/lix/src/sql2/entity_batch.rs
```

The broader e166..413 frontier also deletes the two entity column-cache files
and changes `live_state/mod.rs`, `session/execute.rs`, and
`sql2/providers/entity.rs`. 413 routes `CurrentEntitySnapshotReader` through
one `LiveStateReader::scan_batch` and removes the old direct entity snapshot
helpers. It does not modify the historical point/scan owner:

* `packages/lix/src/forktree/serving.rs:668-719`,
  `load_state_value_at_commit`, still converts a missing CommitCatalog entry
  into `Ok(None)`.
* `packages/lix/src/forktree/view.rs:288-323`,
  `ForkTreeReadFacade::load_state_rows_at_commit`, still maps that optional
  result with `rows.push(value.map(...))`.
* `packages/lix/src/live_state/forktree_reader.rs:1-210` owns the current
  selector/root scan and exact current-row path; it rejects derived/history
  requests before opening a view and uses one coherent view for exact rows.

Therefore the historical oracle remains SOURCE RED on 413. The SQL
projection cut removes a duplicate current-state reader, but it does not make
missing historical commit metadata indistinguishable from a legitimate absent
key. No runtime or adapter qualification is claimed here.

## Function-scoped correction boundary

The only permitted production edits for the narrow correction are:

1. `packages/lix/src/forktree/serving.rs`, function
   `load_state_value_at_commit` and the minimum private validation helper it
   directly calls, if required. A missing selected CommitCatalog entry must be
   a typed corruption/error before state lookup. A present entry must continue
   through commit identity, retained closure, and root authentication.
2. `packages/lix/src/forktree/view.rs`, function
   `ForkTreeReadFacade::load_state_rows_at_commit` and only its direct
   point-to-row lowering, if required to preserve error propagation and cell
   distinctions.

The correction must not edit the 413 SQL projection implementation, add a
second historical reader, or restore any deleted direct entity path. In
particular, these are forbidden for this slice: tracked-state/head readers,
branch-control readers, raw storage-space access, cache/index/format changes,
fallback or retry routes, compatibility readers, migration code, and changes
to `packages/lix/src/live_state/context.rs`,
`packages/lix/src/sql2/entity_batch.rs`, or the deleted entity-cache paths.

## Acceptance checklist

The complete function-scoped cases are frozen in `CHECKLIST.md`. The required
semantic rule is:

```text
validated commit + validated root + absent key = authenticated absence
anything before commit/root validation = corruption/error
```

The same one retained `StorageRead` must carry selector, repository root,
CommitCatalog, commit object, retained member/back-edge, and state-root reads.
No operation may begin a second read, retry, consult a fallback/cache, or
turn a missing commit/root into an empty point or scan result.

The source-only binding command is:

```bash
bash evidence/forktree-historical-failclosed-sql-413/source_verifier_413.sh --expect-red
```

Expected status is exit 0 with an explicit RED finding. It is not a candidate
approval gate; it proves that 413 still needs the narrow correction.

## Future qualification boundary

`FUTURE_COMMANDS_413.md` contains the exact serialized Memory, RocksDB, and
SlateDB commands. They are not run in this report-only task. A future
correction is not eligible for approval until all three adapters pass point
and scan cases after flush/drop/reopen, including malformed/missing/wrong-kind
commit/root substitutions, tombstone/null/value distinctions, and one-read/no
fallback counters.
