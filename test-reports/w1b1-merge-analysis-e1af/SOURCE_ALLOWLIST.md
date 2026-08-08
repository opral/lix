# W1b-1 exact source allowlist

Any future production candidate replacing the merge-analysis historical owner
may change only these existing paths. The package itself changes none of them.

```text
packages/lix/src/session/merge/analysis.rs
packages/lix/src/session/merge/branch.rs
packages/lix/src/transaction/context.rs
packages/lix/src/tracked_state/diff.rs
packages/lix/src/forktree/view.rs
packages/lix/src/forktree/serving.rs
packages/lix/src/forktree/tests.rs
```

`analysis.rs` is the owner conversion. `branch.rs` and the listed
transaction context lines are direct retained-read plumbing.
`tracked_state/diff.rs` may only be converted or deleted as the merge-only
legacy implementation is proven dead. ForkTree files may change only to
expose/authenticate the smallest missing diff/member operation; existing
authenticated methods remain the preferred path.

Every other production path is forbidden for W1b-1, including:

- W1a/changelog/change-provider and SQL history routes;
- transaction stale/plugin/cohort reconciliation;
- undo/redo and typed transitions;
- checkpoint/history reconstruction and working-diff providers;
- selectors/BranchRef/branch publication, writer/publication, GC, CAS/blob
  layout, storage adapters, upload, or any W3-W5 path;
- Cargo manifests, public API compatibility wrappers, migrations, fallback
  readers, caches, indexes, second writers, or alternate authorities.

Package-only changes must remain below:

```text
test-reports/w1b1-merge-analysis-e1af/
```
