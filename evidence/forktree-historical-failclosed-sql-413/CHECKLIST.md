# Function-scoped acceptance checklist

This checklist applies to a future correction of the historical point/scan
owner on top of the exact SQL reader frontier
`413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d`. It is TEST/REPORT ONLY and does
not authorize production edits or runtime claims for 413.

## Required call graph

The candidate must preserve one call chain per operation:

```text
caller-owned StorageRead
  -> ForkTreeReadFacade::load_state_value_at_commit
     -> serving::load_state_value_at_commit
        -> authenticated selector/repository root
        -> required CommitCatalog entry for requested commit
        -> CommitObjectV1 + retained closure validation
        -> state_point_on_read on the same read

caller-owned StorageRead
  -> ForkTreeReadFacade::load_state_rows_at_commit
     -> the point function above for requested identities
     -> terminal HistoricalStateRow lowering
```

The historical point function may return `None` only after the requested
commit and its authenticated state roots have been established. A batch may
contain an empty slot only for that validated commit/root plus a genuinely
absent key.

The 413 current SQL path remains separately bounded: its
`CurrentEntitySnapshotReader` calls one `LiveStateReader::scan_batch`; the
`live_state/forktree_reader.rs` current scan rejects derived/history shapes
before opening a view, and exact current rows use one coherent view. The
correction must not route historical calls through that current-only path.

## Discriminating function cases

Run each case for both point and scan/batch forms. “Error” means an observable
typed corruption/error, not an empty row, `None`, or a successful retry.

| fixture | required result | point | scan/batch |
|---|---|---:|---:|
| valid CommitCatalog + valid commit/root + absent key | authenticated absence | `None` | empty slot/result only for that key |
| missing selected CommitCatalog entry | corruption/error | error | error before empty output |
| missing selected root object | corruption/error | error | error before empty output |
| wrong-kind/substituted root object | corruption/error | error | error |
| malformed CommitCatalog, commit envelope, selector, or root | corruption/error | error | error |
| valid `StateCell::Null` | null, not absence/tombstone | distinct null row | distinct null row |
| valid `StateCell::Tombstone` with inclusion enabled | tombstone | deleted row | deleted row |
| valid `StateCell::Value` | value bytes/metadata preserved | exact value | exact value |
| valid tombstone with inclusion disabled | filtered by the existing contract | absent from visible result | absent from visible result |
| selected commit/root exists but requested key is absent | authenticated absence | `None` | empty only after validation |

The substitution cases must use same-size and same-key replacements where
possible, so a permissive presence check cannot pass accidentally. The scan
assertion is essential: a corrupt commit/root must fail before producing an
empty filtered result.

## Lifetime and negative controls

* Instrument the operation to prove one retained `StorageRead` identity from
  selector through state lookup. No `begin_read` is allowed inside the
  function-scoped point/batch path.
* Assert zero retry, fallback, cache, legacy-reader, and second-view events.
* Do not synthesize an empty control or canonicalize malformed bytes.
* Preserve authenticated absence separately from corruption; do not broaden
  `Option` to cover missing catalog/root data.
* Verify the same behavior after flush, drop, and cold reopen on RocksDB and
  SlateDB; Memory covers the pure control flow but is not sufficient alone.
* Verify the 413 current SQL projection still performs one canonical scan and
  that no removed `scan_direct_entity_*` or entity-cache path reappears.

## Source gate and forbidden expansion

Allowed production scope is limited to:

* `packages/lix/src/forktree/serving.rs`:
  `load_state_value_at_commit` plus a directly required private validation
  helper;
* `packages/lix/src/forktree/view.rs`:
  `ForkTreeReadFacade::load_state_rows_at_commit` plus direct error-preserving
  lowering.

No edits are allowed in this correction to writers/publication, selector or
branch controls, GC/reachability, tracked-state/head modules, SQL projection
code, adapters, persisted formats, caches/indexes, compatibility readers,
migrations, or fallback paths. No deleted 413 direct entity reader/cache may
be restored under another name.

## Verdict rule

413 is RED until the missing CommitCatalog case and all scan/point cases above
are green on Memory, RocksDB, and SlateDB. A source-only correction can be
reviewed before runtime, but it must carry the exact changed-function scope,
the inherited 448624a oracle, and a new immutable runtime manifest. No
performance claim is part of this acceptance slice.
