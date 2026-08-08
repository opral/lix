# W1b-3 exact source allowlist

Any future production candidate replacing the undo/redo and typed transition
historical reader may change only these existing paths. The package itself
changes none of them.

    packages/lix/src/session/undo_redo.rs
    packages/lix/src/transaction/context.rs
    packages/lix/src/forktree/view.rs
    packages/lix/src/forktree/serving.rs
    packages/lix/src/forktree/tests.rs

session/undo_redo.rs owns public undo/redo state-machine reads and inverse or
replay planning. transaction/context.rs owns typed transition validation and
the existing atomic staging boundary. ForkTree paths may change only for the
smallest authenticated first-parent, marker, exact-row, or dependency-closure
operation required by those consumers.

Every other production path is forbidden for W1b-3, including:

- W1a/W1b-1/W1b-2 and merge analysis or stale reconciliation;
- checkpoint reconstruction and SQL checkpoint providers;
- working-diff providers, changelog/change-provider, selectors/BranchRef;
- writer/publication, GC, CAS/blob layout, storage adapters, upload, and W3-W5;
- Cargo manifests, compatibility wrappers, migrations, fallback readers,
  durable caches/indexes, second readers, alternate authorities, or a second
  transition writer.

Package-only changes must remain below:

    test-reports/w1b3-undo-transition-e1af/

## Structural candidate contract

The verifier is candidate-parametric: it accepts an explicit target commit and
compares the complete target diff against exact e1af. The only non-package
paths allowed in a future candidate are the five production paths above.

Each `undo_in_transaction` and `redo_in_transaction` body must contain exactly
one construction with these exact ownership and argument semantics:

    let forktree_read =
        ForkTreeReadFacade::from_opening_read(transaction.opening_read());

The facade variable is deliberately fixed as `forktree_read` so aliases cannot
hide a second reader. The operation bodies must contain no `begin_read`, raw
`StorageRead`, `commit_graph_reader`, `TrackedStateStoreReader`, tracked-state
reader, refresh/clone, fallback, retry, cache, or raw-store path. Every
chronology, marker, exact-row, delta, node, and inverse/replay helper call in
those bodies must pass `forktree_read` in its argument list.

`execute_typed_state_transitions` must receive an explicit
`forktree_read: &ForkTreeReadFacade` argument and pass that same variable to
all historical/state helpers. The verifier masks comments and literals,
balances function and call delimiters, follows every historical helper body,
checks each facade parameter and exact call argument, rejects facade
aliases/clones and second constructors, and scans added reader/authority
tokens in every allowlisted ForkTree path. It does not accept a bare token in
a different comment or function.

The exact e1af source remains RED on the original four predicates. Structural
GREEN checks run only after those legacy-reader predicates are absent, so the
original four-RED calibration remains unchanged.
