# ForkTree TrackedState transaction/reconciliation/undo migration oracle report

## Correction freeze

This is a test/report-only correction of the prior d47 oracle. It closes the
source-gate path contradiction and strengthens the pure model's retained-view
and fail-closed transition checks. Production source is untouched.

## Immutable target

The package is bound to b59e1f11a51153e0a787a81f0f25bf104d150aaf,
tree 700fd04d21bc40c05425c9fc9e10d65c9e1eda24, parent
713455a3557907ce705d06f720fcdc4486bddd4a. It is test/report-only and makes
no production, compile, benchmark, adapter, or runtime claim.

## Current source finding

The exact b59 source still has a multi-reader tracked-state authority. The
direct inventory found the reader factory and callback in
packages/lix/src/transaction/context.rs around the tracked-state reader helper
region, fresh reader calls in stale reconciliation and checkpoint selection,
and direct callers in undo/redo, checkpoint, merge, SQL history/diff/working-
diff, and filesystem working-diff paths. The tracked_state/context.rs module
still defines TrackedStateContext::reader and TrackedStateStoreReader and
contains the reader-backed history/diff/payload implementation. The exact
path-aware source gate is therefore intentionally RED on b59; it was not
executed.

The inventory is separated from unrelated test/benchmark support. A
production caller may not be retained merely because a test fixture or
benchmark still uses the old owner.

The source verifier is now stage-aware: `baseline` proves that b59 still has
the legacy owner paths and returns intentional RED; `candidate` excludes
those deleted files from the required closure and requires them to be absent.
It also scans the legacy reader DTO/factory/wrapper/cache/fallback vocabulary
and requires exactly one opening `begin_read` across the direct closure.

## Required smallest correction

Move the complete direct closure to one caller-owned retained
StorageRead -> CoherentView. Bind immutable copies of the selector pair,
epoch, state/catalog/checkpoint roots, branch owner, view_id, and selected
snapshot bytes. Current reads use captured snapshot rows, so external
mutation leaves an existing view stable while its later CAS fails stale.
Same-owner owner-epoch drift is rejected; unrelated owner drift is
distinguished and succeeds without changing the transaction's scope.
Historical reads validate selected commits and roots before use. All
reconciliation, transition, selected-history, checkpoint-floor, undo/redo,
savepoint, idempotency, and opening-read decisions must use that same view.
Transaction-local staged cells may overlay reads but cannot become durable
state.

Intent is classified before any plan. Genuine no-op and unsupported cohorts
produce no plan, writes, prepare, commit, or epoch rotation. Supported rows,
undo/redo, checkpoint, and selected-history transitions use the existing
PreparedPublication -> into_storage_plan -> prepare_write_set -> commit path
exactly once. The plan carries raw selector/epoch preconditions and
owner/generation/expected-change identity. Any stale, malformed, missing,
wrong-kind, or injected partial-publication condition fails closed with no
durable mutation.

The state merge contract is explicit: local value overrides global value,
local tombstone suppresses global value, local absence falls through, and
NULL is a value rather than absence. Parent generation and branch scope are
authenticated; CommitId/change identity is not reconstructed from a cache or
scan. Missing source/desired commits and malformed/missing roots fail closed,
never becoming digest zero or an implicit tombstone. Cold reopen must
reproduce the selected roots and rows.

## Evidence included

- model.rs: dependency-free stateful model with 16 focused tests including
  one-view reads, external mutation stability, publication counters,
  same-owner stale versus unrelated-owner reconciliation, generation/owner scope,
  NULL/tombstone precedence, savepoint rollback, no-op/unsupported intent,
  stale epoch, idempotency, undo/redo/checkpoint floor, corruption/reopen,
  expected-change mismatch, missing/corrupt transition roots, and fault rollback.
- verify_tracked_state_transaction_source.sh: anchor-bound, stage-aware
  path/deletion gate for direct production callers, required ForkTree seams,
  exactly one opening read boundary, and exhaustive legacy owner vocabulary.
- ORACLE_README.md: source map, deletion order, future adapter command order, and
  acceptance boundary.
- MANIFEST.json: machine-readable target, closure, required/forbidden
  surfaces, dormant gates, and no-runtime status.

No file in this package changes production source. No cargo, Clippy, no-run,
Memory, RocksDB, or SlateDB command was executed for this freeze. The
permitted standalone model/source checks were run after the
correction: `rustc --edition=2021 --test model.rs` and the resulting binary
reported 16/16 tests passing; `bash -n` passed; exact b59 baseline mode
returned the expected RED marker with log SHA
ee8b91342f5f6ae006346c87c0eb62f538b6379ab94910c041bc0ec8bb952328. No
Memory, RocksDB, SlateDB, cargo, or production runtime command was executed.
