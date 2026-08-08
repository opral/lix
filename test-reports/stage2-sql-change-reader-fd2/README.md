# ForkTree Stage2 SQL changelog-reader acceptance

This package is TEST/REPORT-ONLY. It is anchored to immutable fd2 and does
not change production SQL, changelog, ForkTree, storage, writer, or runtime
code. It is a compile/source acceptance contract for the smallest next
three-diagnostic closure identified in the fd2 dependency map.

## Immutable anchor

| item | value |
| --- | --- |
| anchor commit | `fd2be256d763f17e9f127d4c984e36fba191cb82` |
| anchor tree | `20110ca5e3c33d34217630fff0a2b784b545317a` |
| anchor parent | `cd91b9b90f7f468158b4df154adbed9551eb5d60` |
| expected first source check | `cargo check -p lix --lib` on the eventual candidate |
| current anchor status | intentionally compiler-red; no candidate implementation is claimed |

The package must be applied only as test/report evidence to a disposable
candidate worktree. It must never be merged wholesale into production.

## Current fd2 call graph and blocked owners

The current production route is:

```text
SessionContext/TransactionContext/SQL execution context
  -> changelog_query_source()
  -> SqlChangelogQuerySource<S> { store, json_reader }
  -> providers::register_lix_change_read_provider
  -> ChangeSpec::plan_scan
  -> scan_changelog_changes(query_source.store, ...)
       -> tracked_state::scan_change_records_from_commit_deltas
       -> ChangelogContext::new().reader(store.clone()).scan_changes
       -> CommitGraphContext::new().reader(store).all_nodes/load_commit_records
       -> direct COMMIT_CHANGE_ID_SPACE lookup in load_exact_change
       -> tracked_state::load_change_record_by_id
```

The fd2 source locations are fixed for review: `providers/mod.rs:365-370`
registers the surface; `providers/change.rs:54-100` stores and passes the
raw source; `change.rs:151-206` scans tracked-state changes, Changelog
records, and CommitGraph records; and `change.rs:303-356` repeats those
fallbacks for exact lookup, including `COMMIT_CHANGE_ID_SPACE` at line 329.
The source is constructed at `session/context.rs:739-744` and
`transaction/context.rs:8227-8231`, where `ChangelogQuerySource` currently
contains only `store` and `json_reader` (`sql2/context.rs:63-67`). The
already-retained `HistoryQuerySource` ForkTree field at
`sql2/context.rs:50-60` is deliberately not accepted as proof: the SQL
changelog provider has a separate source and must be wired to the same
operation-owned read explicitly.

The blocked owners are the deleted tracked-state change scanners,
`COMMIT_CHANGE_ID_SPACE`, the direct `ChangelogContext`/raw-store route in
this provider, and the independent commit-graph/raw-store construction.
`changelog::context` itself already delegates its low-level operations to
ForkTree, but this SQL provider bypasses that ownership boundary by accepting
and threading the raw `store` field and by adding its own lookup fallbacks.

## Required successor contract

The eventual candidate must make the SQL change provider consume one
caller-owned ForkTree read facade constructed from the operation's already
retained read. The intended boundary is:

```text
caller-owned retained read
  -> SqlChangelogQuerySource.forktree_reader: ForkTreeReadFacade<same read>
  -> ChangeSpec::plan_scan
  -> scan_changelog_changes(&query_source.forktree_reader, limit, route)
  -> ForkTree ChangeCatalog/CommitCatalog/CommitRecord serving
  -> terminal SQL materialization only
```

The facade is the sole authority for direct ChangeCatalog records and
commit-derived changes. It must authenticate catalog key/object identity,
object domain/kind, embedded ChangeId/CommitId, CommitCatalog membership,
commit parent/order facts, and ChangeCatalog owner/ordinal/back-edge facts
before a row is visible to SQL. A malformed or wrong-kind object is an error,
not an absent row.

The provider may retain SQL's `LixChangeRow` terminal enum and
`materialize_*` projection. It may not retain a raw storage handle, construct
another read/facade, invoke `begin_read`, or use a second changelog/commit
reader.

## Semantic cases

`SQL_CHANGE_READER_CASES.tsv` and `fixtures/` are discriminating controls,
not token-presence decoration. Every case is required on the first runnable
candidate, and the source verifier checks that the candidate's call arguments
and rejection paths remain inside the retained facade.

Required behavior:

1. Direct ChangeCatalog record is returned with its authenticated ChangeId.
2. Commit-derived `lix_commit` change is returned from one authenticated
   CommitCatalog/CommitRecord path.
3. Authenticated absent change key is `Ok(None)`/SQL absence.
4. Missing catalog record after enumeration is a typed corruption error.
5. Missing, malformed, wrong-domain, wrong-kind, or substituted change object
   is a typed corruption error.
6. A ChangeCatalog record whose embedded ChangeId differs from its requested
   key is a typed identity error.
7. Duplicate logical ChangeId records in the canonical merge are a typed
   corruption error; they must not be overwritten by a map or flattened.
8. Direct and commit-derived records are globally sorted by canonical ChangeId
   and SQL limit is applied only after authenticated merge and duplicate
   validation.
9. Exact-ID routing preserves valid absence but never hides corruption.
10. Every logical operation uses the same caller-owned read identity and has
    no second `begin_read`, refresh, raw-store extraction, or fallback.

## Deletion and source gates

The verifier requires all of the following in the eventual candidate:

* no `tracked_state::scan_change_records_from_commit_deltas`;
* no `tracked_state::load_change_record_by_id`;
* no `COMMIT_CHANGE_ID_SPACE`, `PointReadPlan`, `StorageKey`, or
  `StorageProjectedValue` in the provider closure;
* no `ChangelogContext::new().reader(...)`, `ChangelogReader`, or
  `CommitGraphContext::new().reader(...)` in the provider closure;
* no `query_source.store` argument to a change scan or exact lookup;
* no `.flatten()`/`filter_map` omission of required ChangeCatalog,
  CommitCatalog, or ChangeRecord entries;
* no `begin_read`, storage clone/reopen, cache, compatibility reader, or
  independent publication/read authority;
* `scan_changelog_changes` and `load_exact_change` receive the same
  `query_source.forktree_reader` (or a borrow of that exact operation-owned
  facade), not a raw `S`;
* the canonical sort precedes the final `truncate`, and duplicate detection
  precedes both output and limit;
* changed production paths stay within the verifier's read-facade allowlist:
  `packages/lix/src/sql2/providers/change.rs`,
  `packages/lix/src/sql2/context.rs`, the already-retained-read constructors
  in `session/context.rs` and `transaction/context.rs`, and the narrow
  ForkTree facade files `forktree/view.rs`, `forktree/serving.rs`, and
  `forktree/mod.rs`.

Any writer, selector mutation, GC, storage-format, legacy-space, cache,
fallback, second-authority, or unrelated SQL change is a hard RED.

## Replay order

```text
verify_source_contract.sh <candidate-worktree> fd2be256d763f17e9f127d4c984e36fba191cb82
git diff --check
cargo fmt --all --check
cargo check -p lix --lib
```

The source verifier is intentionally expected to exit RED on fd2 because the
three blocked provider diagnostics and the raw legacy call graph are still
present. No runtime or adapter execution is part of this package. A future
candidate may be called source-ready only after the verifier passes with the
same negative fixtures and an independent review confirms the actual retained
read identity.
