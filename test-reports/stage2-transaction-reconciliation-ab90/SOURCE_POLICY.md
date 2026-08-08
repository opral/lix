# Transaction-reconciliation source gate

This is a TEST/REPORT-ONLY policy for the successor to the a9dd transaction
oracle. It is not a production implementation and it does not treat a
compiler-red frontier as runtime evidence.

## Full-workspace inventory

The verifier enumerates every tracked source artifact in the candidate Git
tree, not a hand-maintained list of 23 files. Source artifacts are tracked
Rust, TypeScript/JavaScript, Python, Go, SQL, shell, and configuration source
files. Generated build output is not tracked source and is not included.

Legacy names are reported with path and line, then classified. A declaration
or deferred owner outside the migrated transaction function scope is not a
failure by itself. This prevents a global token scan from confusing an
inherited checkpoint/GC owner with a duplicate transaction authority.

## Function-scoped compatibility policy

The future successor must publish the exact migrated function map. The
negative rules apply only to those function bodies and their direct lowering
helpers:

- no `begin_read`, raw `StorageAdapterRead`, detached store/cache accessor,
  `TrackedStateStoreReader`, or `TrackedStateContext`;
- no `PreparedPublication::commit`, retry, compatibility decoder, fallback
  reader, mirror writer, or second transaction authority;
- no selector/epoch mutation outside the existing transaction plan.

The verifier separately inventories these names everywhere and allows
deferred owners, tests, and comments outside the mapped functions. A future
candidate that merely deletes a token globally without proving the call graph
is not green.

## Required source proof

The candidate must expose source-level evidence for all of these properties;
runtime cases below remain mandatory when the production frontier becomes
runnable:

1. Every alternate opening helper receives or borrows one immutable,
   operation-owned read/view. Only the canonical ForkTree read owner may call
   `begin_read`; transaction/reconciliation helpers cannot refresh, extract,
   or pair a detached cache.
2. `owner_epoch` and `view_id` are authenticated in both the publication plan
   and the transaction commit precondition. A value present only in a view
   descriptor is insufficient.
3. Publication enforces `reconcile_owner` before producing writes; stale,
   unrelated-owner, and mixed-conflict classification cannot be bypassed by
   the lowerer.
4. Historical state is an immutable captured view. Global/branch overlay,
   desired local state, undo/redo, and tombstone policy all consume that same
   capture. Missing, malformed, or substituted roots fail closed.
5. The transition carries desired local state explicitly; it is not inferred
   later from a mutable reader, cache, or default/zero digest.
6. The root identity is content-authenticated, not a prefix or length claim.
   A same-prefix/different-content transplant must fail before planning or
   publication.

## Discriminating runtime contract

The eight D-cases in `CASES.md` are deliberately separate from the original
16-case baseline. They must be run on Memory, RocksDB, and SlateDB when a
successor is runnable. Each records read acquisitions, plan/prepare/commit
counts, owner/view identity, selector/epoch/receipt deltas, output digest, and
whether any write became visible after a failure.

No D-case may be satisfied by rejecting a supported operation as an
unsupported capability. Typed rejection is valid only for the explicitly
deferred publication families in `CONTRACT.md`.
