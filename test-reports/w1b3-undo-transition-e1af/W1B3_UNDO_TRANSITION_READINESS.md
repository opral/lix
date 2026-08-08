# W1b-3 undo/redo and typed-transition readiness package — exact e1af

Status: corrected test/report-only successor. No production edit, Lix build,
adapter runtime, benchmark, PR, or merge was performed. The standalone model
is the only artifact compiled/run in this task.

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
`ForkTreeReadFacade` over the transaction opening read for:

1. first-parent commit chronology and merge/root rejection;
2. checkpoint marker floor and undo/redo marker cursor;
3. exact target/current state rows, change IDs, NULL/tombstone/absence;
4. descriptor dependency closure and payload authentication;
5. typed transition validation and terminal inverse/replay staging inputs.

No undo/redo helper may begin, refresh, clone, extract, replace, or cross-use a
read. Each operation must contain exactly

    let forktree_read =
        ForkTreeReadFacade::from_opening_read(transaction.opening_read());

and every chronology, marker, exact-row, delta, node, and inverse/replay helper
call must pass `forktree_read` as an argument. `execute_typed_state_transitions`
must receive `forktree_read: &ForkTreeReadFacade`; fresh graph/read/raw-store
paths, aliases, fallback, retry, cache, and alternate authorities are
forbidden. The source verifier balances function/call delimiters and checks
these arguments, rather than accepting a token in an unrelated comment.

## Required semantic gates

The future candidate must preserve:

- first-parent chronology and strictly increasing parent generations; root and
  merge commits reject undo/redo;
- checkpoint marker as an ordered undo floor, including a floor reached by a
  forked history but never crossed below;
- exact undo target, inverse parent state, and durable redo cursor;
- exact redo target, replay state, and cursor advancement;
- ordinary commit after undo discarding the old redo path and both redo cursor
  fields;
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

The standalone model covers inverse/redo identity, generation and first-parent
validation, ordered floors, root/merge rejection, ordinary-commit redo
invalidation, explicit redo-cursor mismatch rejection, atomic
selector/cursor rollback, stale/read/alias/raw-store/fallback/cache poisoning,
explicit absence/NULL/tombstone, missing history, duplicate/empty identities,
and cold reopen. Its nine tests are a contract oracle, not production
qualification.

## Expected exact-e1af RED calibration

verify_source_contract.sh is source-only and intentionally exits 1 on exact
e1af. It must report exactly four RED predicates:

1. undo/redo use of tracked_state_reader();
2. undo/redo use of a fresh commit_graph_reader();
3. missing ForkTree facade anchor in undo/redo;
4. typed transition reload through tracked_state_reader().await.

Undo/redo entrypoints, inverse/replay, typed atomic staging, and checkpoint
marker anchors are positive controls. `EXPECTED_RED.txt` captures exact
output. Once those four legacy predicates are absent on a future target, the
verifier additionally checks the exact facade constructor, operation-local
forbidden tokens, balanced helper-call arguments, transition facade argument,
and complete target-diff scope. No Lix compiler or adapter result is inferred.

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

The standalone model and rustfmt are the only local qualification commands run
in this task:

    timeout 1200s rustfmt --edition 2024 --check test-reports/w1b3-undo-transition-e1af/undo_transition_oracle.rs
    timeout 1200s rustc --edition=2024 --test -D warnings test-reports/w1b3-undo-transition-e1af/undo_transition_oracle.rs -o /tmp/w1b3-undo-transition-oracle
    timeout 1200s /tmp/w1b3-undo-transition-oracle --nocapture

Corrected model evidence: 9/9 passed; executable SHA-256
`4684f2749233b1015b7953b4a0c085efd0ee9a32bad226f2b8a7a5e114ff53fa`.
The exact e1af source verifier output SHA-256 is
`8741d99516d096b41f06813c7afdc2dc1ff74fa3b286f45cbfe81c6e2b2dc652` and
exits 1 with the preserved four RED predicates.

Future immutable production candidates may run, only after the source gate:

    timeout 1200s test-reports/w1b3-undo-transition-e1af/verify_source_contract.sh "$PWD" HEAD e1af471b9ab0f598dafa7c2ddec7867667c81740
    timeout 1200s cargo test -p lix undo_redo --lib
    timeout 1200s cargo test -p lix execute_tracked_state_transition --lib
    timeout 1200s cargo test -p lix --lib --features slatedb undo_redo

Future adapter order is Memory/default, exact RocksDB, then SlateDB. Focused
controls must include undo/redo round-trip, generation/first-parent, ordered
checkpoint floors, ordinary-commit redo invalidation, atomic selector/cursor
rollback, absence/NULL/tombstone, descriptor cascade, corruption, cold
reopen, and zero second reads. No broad matrix is authorized before
source/compiler gates.

## Review boundary

This is a readiness package, not approval of a production candidate. Any
production path outside the allowlist, W1 scope widening, second reader/view,
legacy writer/cache/fallback, altered marker/floor semantics, partial
transition staging, lost identity checks, or non-fail-closed corruption is a
blocker.
