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
