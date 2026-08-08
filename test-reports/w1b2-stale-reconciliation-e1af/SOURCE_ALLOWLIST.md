# W1b-2 exact source allowlist

Any future production candidate replacing the stale transaction/plugin/cohort
reader may change only these existing paths. The package itself changes none
of them.

    packages/lix/src/transaction/context.rs
    packages/lix/src/transaction/context/cohort.rs
    packages/lix/src/transaction/stale_commit.rs
    packages/lix/src/forktree/view.rs
    packages/lix/src/forktree/serving.rs
    packages/lix/src/forktree/tests.rs

The transaction context paths own commit-boundary stale detection and plugin
reconciliation. The cohort path owns grouped plugin replay. stale_commit.rs is
the pure overlap classifier and may only receive semantic-preserving plumbing
changes. ForkTree paths may change only for the smallest authenticated
owner/version/revision or exact-row operation that the retained view lacks.

Every other production path is forbidden, including:

- session merge analysis and branch merge code;
- W1a/changelog/change-provider and SQL history routes;
- undo/redo, typed transitions, checkpoint/history, and working-diff;
- selectors/BranchRef, publication/writer, GC, CAS/blob layout, storage
  adapters, upload, and all W3-W5 paths;
- Cargo manifests, public compatibility wrappers, migrations, fallback or
  retry authorities, durable caches, indexes, second writers, alternate
  readers, or alternate selectors.

Package-only changes must remain below:

    test-reports/w1b2-stale-reconciliation-e1af/
