# W1b-3 undo/redo and typed-transition readiness package — exact e1af

Status: test/report-only, frozen for independent review. No production edit,
Lix build, adapter runtime, benchmark, PR, or merge was performed. The
standalone model is the only artifact permitted to compile/run in this task.

## Pinned source and scope

- Anchor commit: e1af471b9ab0f598dafa7c2ddec7867667c81740
- Anchor tree: bfa0d271a723da8250ab76ada16fda90926f1099
- Anchor parent: b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
- Source worktree: /tmp/lix-w1b3-undo-transition-e1af
- Exact production allowlist: SOURCE_ALLOWLIST.md
- Package-only path: test-reports/w1b3-undo-transition-e1af/

This is W1b-3 only. W1a/W1b-1/W1b-2, checkpoint reconstruction,
working-diff, changelog, selectors/BranchRef, writer/publication, GC,
CAS/blob layout, and W3-W5 are explicitly excluded.

## Current e1af call graph

Public session entrypoints are session/undo_redo.rs:45-58:

    SessionContext::undo / SessionContext::redo
      -> undo_in_transaction:61-117
      -> redo_in_transaction:119-171

Undo reads the active head, semantic state, undo target, target's unique
first-parent, pre-target state, and target delta before applying an inverse
transition. Redo reads the durable marker cursor, target commit and target
delta before applying a replay transition. Both paths stage the marker through
the existing transaction writer.

The remaining historical-reader chain is:

    semantic_state_at:179-230
    semantic_state_for_record:254-337
    operation_marker_at:339-390
    load_commit_delta:409-420
    load_node:422-443
    apply_state_diff:458-505
      -> tracked_state_reader() at 197,299,350,416,480
      -> fresh commit_graph_reader() at 428
      -> execute_tracked_state_transition

Typed transition validation/materialization is:

    transaction/context.rs:7353-7500
      -> source active-head check
      -> clean transaction and nonempty/unique identity checks
      -> current identity-only and desired full projected rows
         through tracked_state_reader():7388
      -> exact schema/file/entity identity and change_id comparison
      -> execute_typed_state_transitions
      -> one existing staged Rows replacement

The current transition writer is not replaced in W1b-3. Its atomic
stage_write/commit boundary remains the sole publication path.

## One opening-read authority contract

The future candidate must use one transaction-owned
ForkTreeReadFacade/CoherentView over the transaction opening read for:

1. first-parent commit chronology and merge/root rejection;
2. checkpoint marker floor and undo/redo marker cursor;
3. exact target/current state rows, change IDs, NULL/tombstone/absence;
4. descriptor dependency closure and payload authentication;
5. typed transition validation and terminal inverse/replay staging inputs.

No undo/redo helper may begin, refresh, clone, extract, replace, or cross-use a
read. A branch-bound descriptor may borrow the same read, but a fresh
commit-graph reader or TrackedStateStoreReader is forbidden. There is no
fallback, retry authority, durable cache/index, alternate history owner, or
second transition writer. The source model makes begin count, reader instance,
write count, and view identity explicit.

## Required semantic gates

The future candidate must preserve:

- first-parent chronology; root and merge commits reject undo/redo;
- checkpoint marker as an undo floor, including forked floor behavior;
- exact undo target, inverse parent state, and durable redo cursor;
- exact redo target, replay state, and cursor advancement;
- ordinary commit after undo discarding the old redo path;
- atomic batch transition: one identity error means no partial staged result;
- active branch-head stale rejection and clean-transaction requirement;
- nonempty unique identity selection and exact schema/file/entity identity;
- expected/current change_id versus desired/target change_id checks;
- authenticated absence versus NULL and tombstone, plus payload/descriptor
  dependency closure;
- missing/malformed/wrong-kind/cyclic/identity-substituted topology, marker,
  root, member, or payload failing closed before staging;
- cold reopen preserving history, markers, floors, cursors, and exact state
  identities.

The standalone model covers inverse/redo identity, floor/root/merge
rejection, atomic identity failure, stale/read poisoning, missing history, and
cold reopen. It is a contract oracle, not production qualification.

## Expected exact-e1af RED calibration

verify_source_contract.sh is source-only and intentionally exits 1 on exact
e1af. It must report:

1. undo/redo use of tracked_state_reader();
2. undo/redo use of a fresh commit_graph_reader();
3. missing ForkTree facade anchor in undo/redo;
4. typed transition reload through tracked_state_reader().await.

Undo/redo entrypoints, inverse/replay, typed atomic staging, and checkpoint
marker anchors are positive controls. EXPECTED_RED.txt captures exact output.
No Lix compiler or adapter result is inferred.

## Compiler-driven deletion order

1. Add/verify authenticated ForkTree chronology, marker, exact-row, and
   dependency-closure operations; do not touch W1b-1/W1b-2 readers.
2. Convert load_node, semantic marker/delta/state readers, and apply_state_diff
   to the transaction-owned facade without changing the marker protocol.
3. Convert execute_tracked_state_transition to one retained-view exact read,
   preserving validation before any stage_write.
4. Delete only undo/redo and transition-specific legacy reader plumbing after
   compiler reachability and focused tests prove it is unused. Do not delete
   the shared TrackedStateStoreReader while other W1 partitions remain.
5. Preserve the existing atomic writer and do not add a compatibility reader,
   transition cache, alternate marker authority, or retry path.

## Future commands, each capped at 1200 seconds

The standalone model is the only command run in this task:

    timeout 1200s rustc --edition=2024 --test -D warnings test-reports/w1b3-undo-transition-e1af/undo_transition_oracle.rs -o /tmp/w1b3-undo-transition-oracle
    timeout 1200s /tmp/w1b3-undo-transition-oracle

Future immutable production candidates may run, only after the source gate:

    timeout 1200s test-reports/w1b3-undo-transition-e1af/verify_source_contract.sh "$PWD" HEAD e1af471b9ab0f598dafa7c2ddec7867667c81740
    timeout 1200s cargo test -p lix undo_redo --lib
    timeout 1200s cargo test -p lix execute_tracked_state_transition --lib
    timeout 1200s cargo test -p lix --lib --features slatedb undo_redo

Future adapter order is Memory/default, exact RocksDB, then SlateDB. Focused
controls must include undo/redo round-trip, atomic batch, checkpoint/merge/root
floors, stale source-head, descriptor cascade, corruption, cold reopen, and
zero second reads. No broad matrix is authorized before source/compiler gates.

## Review boundary

This is a readiness package, not approval of a production candidate. Any
production path outside the allowlist, W1 scope widening, second reader/view,
legacy writer/cache/fallback, altered marker/floor semantics, partial
transition staging, lost identity checks, or non-fail-closed corruption is a
blocker.
