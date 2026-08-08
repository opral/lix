# W1b-1 v2 production path contract

A future candidate may change only these existing production paths for this
reader-only slice:

    packages/lix/src/session/merge/analysis.rs
    packages/lix/src/session/merge/branch.rs
    packages/lix/src/transaction/context.rs
    packages/lix/src/tracked_state/diff.rs
    packages/lix/src/forktree/view.rs
    packages/lix/src/forktree/serving.rs
    packages/lix/src/forktree/tests.rs

No Cargo, storage adapter, SQL, changelog, writer/publication, checkpoint,
working-diff provider, selector, GC, CAS/blob, upload, compatibility, fallback,
cache/index, second writer, or alternate authority path may be added. Missing
legacy files are acceptable only when the candidate's remaining call graph
still satisfies the structural contract and the verifier's exact path scope.

