# W1b-5 candidate production scope

A future W1b-5 provider/readers candidate may change only these paths:

    packages/lix/src/forktree/view.rs
    packages/lix/src/forktree/serving.rs
    packages/lix/src/forktree/tests.rs
    packages/lix/src/sql2/context.rs
    packages/lix/src/sql2/providers/working_diff.rs
    packages/lix/src/sql2/providers/filesystem_working_diff.rs
    packages/lix/src/sql2/providers/checkpoint.rs
    packages/lix/src/session/checkpoint.rs
    packages/lix/src/session/context.rs
    packages/lix/src/filesystem/read.rs
    packages/lix/src/live_state/forktree_reader.rs

The following current-owner paths are scanned for residue but are outside this
narrow reader slice. They require their own hard-cut owners and cannot be
silently treated as migrated:

    packages/lix/src/live_state/context.rs
    packages/lix/src/live_state/tracked_head.rs
    packages/lix/src/init.rs
    packages/lix/src/gc.rs
    packages/lix/src/transaction/context.rs
    packages/lix/src/branch/refs.rs

Public SQL table/schema names, projection DTOs, and TrackedStateFilter-shaped
query filters are semantic facades only; no raw space, current layout, cache,
fallback, second reader, or alternate authority may survive behind them.
