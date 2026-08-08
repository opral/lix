# Authenticated semantic replay bridge contract

This package is an acceptance oracle, not a production implementation. It is
anchored to `fd2be256d763f17e9f127d4c984e36fba191cb82`, whose compiler frontier
is intentionally red. The baseline source gate must therefore reject fd2 for
the exact legacy replay calls; the same gate must turn green only on a future
immutable successor that satisfies the structural rules below.

## One read and one publication

Every transaction/replay operation must acquire one caller-owned
`ForkTreeReadFacade`/`CoherentView` over one authenticated `StorageRead`.
The caller passes that exact typed view into replay, undo/redo, and
reconciliation helpers. Helpers must not call `begin_read`, extract a raw
adapter/store, create a cache, refresh a cursor, or open a second reader.

The replay result is in-memory input to the existing transaction boundary:

1. one transaction-owned `into_storage_plan`/write-set,
2. one existing `prepare_write_set`,
3. one existing `prepared_commit.commit`.

`PreparedPublication::commit`, `ForkTree::begin_write`, an independent backend
commit, a retry loop, or a second selector/epoch authority is forbidden.

## Authenticated semantic ownership

Topology-only work may read commit identity, ordered parents, and generation
without hydrating members. Replay/history work must lazily load the requested
semantic commit/member records from the authenticated CommitCatalog and
ChangeCatalog using the same retained view. Every result must validate:

- catalog key, object ID, domain, and authenticated root;
- commit ID, parent order, generation, and first-parent chronology;
- member object ID, source commit, exact ordinal/back-edge, uniqueness, and
  order;
- ChangeCatalog object/domain/key identity and payload hash;
- explicit wrong-kind, missing, malformed, cyclic, substituted, or truncated
  records as errors.

No missing record may become an empty vector, digest zero, tombstone default,
or a successful partial result. A commit envelope may be lazy, but a requested
semantic member is mandatory and fail-closed.

## Required caller/read binding

The source verifier binds these exact caller seams rather than accepting a
generic “some reader” claim:

- `transaction/context.rs::execute_apply_or_revert` must borrow the existing
  transaction opening ForkTree facade and pass that view to change/member
  loading; the function must not acquire a new `StorageRead`.
- `transaction/context.rs::opening_parent_complete_lifecycle_created_at`
  must receive the same typed semantic view (or a private equivalent that
  owns/borrows it), not `&(impl StorageAdapterRead + ?Sized)` and not a raw
  storage handle.
- `session/undo_redo.rs` must use the transaction-owned view and must contain
  no `tracked_state_reader()` construction.
- `sql2/providers/change.rs` must use the corrected authenticated changelog
  owner; it may not name `COMMIT_CHANGE_ID_SPACE` or deleted TrackedState
  change loaders.

The accepted owner may use a private neutral replay type, but it must be
constructed from this one view. A TrackedState compatibility wrapper, legacy
space, cache, fallback reader, raw storage escape, dual writer, or public
constructor for an alternate authority is a source rejection.

## Transition semantics

The bridge must preserve exact tracked-state transitions and public behavior:

- undo/redo chronology and final branch state;
- same-owner stale rejection versus unrelated-owner reconciliation success;
- rollback, savepoint restoration, failed-statement atomicity, no-op, and
  idempotency;
- selected source commit/ordinal and ordered member identity;
- valid tombstone versus SQL NULL versus absent row;
- cold reopen and recovery from persisted selector/root/object state.

Unsupported publication families remain typed fail-closed before plan creation
with zero writes, selector changes, epoch rotation, or receipt: GC,
reachability, init publication, replacement-part staging, current-serving
control, and multi-branch publication are deferred cohorts for later packages.
