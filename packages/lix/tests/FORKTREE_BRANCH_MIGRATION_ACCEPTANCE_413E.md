# Branch lifecycle and global-sequence migration oracle

Status: frozen test/report-only first-migration oracle. It is bound to the
frozen selector/epoch model and exact `413e08a` source frontier. No production
source, Cargo build, adapter runtime, compatibility path, or second authority
was added.

## Immutable binding

- oracle parent/base: `72f10a4412dbea93c3a266a20a9c2df91d02193c`
- oracle parent tree: `67c9b631fb701da23f79e2e41d057d027d304e6a`
- source frontier ref: `origin/codex/forktree-stage2-sql-entity-readers-canonical-11442`
- source frontier head/tree: `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d` /
  `820fe560da3bbd2b00b788b0b1759c409048cd6e`
- source frontier parent: `11442c1e0023e20307a7231d88cd557bc704fd13`
- e166→413e full-index binary diff:
  `70bc6bc03524855be515c9d1a5d0c75c77ebd159fbd44d5f646483ce14460329`

The source frontier is intentionally not treated as runnable: the migration
gate remains RED until all old control callers and the physical control space
are removed.

## Pure model

`forktree_branch_migration_acceptance.rs` models exactly one
`GlobalSelectorV1`, one `BranchSelectorV1` per branch, typed snapshots, and one
global epoch/CAS plane. It covers:

- initialization and current-main selector;
- create, session-local switch, delete, and cold reopen;
- deterministic global sequence publication with one epoch rotation;
- true no-op with zero selector/object writes and zero epoch rotation;
- one atomic branch publication with branch/global selector CAS;
- same-owner stale and unrelated-owner stale publication with no partial image;
- branch-first/GC-first races with exact global raw-selector fencing; and
- malformed, wrong-kind, missing-selector-object, and final-release behavior.

Successful branch/create/delete publications record the expected single
atomic write set; sequence publication records a global-only selector write.
The model never copies or updates unrelated branch selectors.

Exact standalone invocation:

```text
TMPDIR=/root/repos/.tmp-branch-migration \
  rustc --edition=2021 -D warnings \
  packages/lix/tests/forktree_branch_migration_acceptance.rs \
  -o /root/repos/evidence/forktree-branch-head-control-acceptance-e166/branch-migration-model
/root/repos/evidence/forktree-branch-head-control-acceptance-e166/branch-migration-model
```

Result: `BRANCH_MIGRATION_MODEL=GREEN cases=4`.

- model binary SHA-256: `d2d16a9ace6d4bccff12f28059c25fc125c201ace801c709209b62dbc1dc7917`
- model log SHA-256: `295a439404942fb9f7cc7ec88fc999d1d41b9ca9c712317894a6a9b55c2c7cc5`

## Source/deletion gate

The source verifier rejects `BranchHeadControlContext`, `BranchHeadControl`
cache/context/reachability names, old staging/precondition helpers,
`BRANCH_HEAD_CONTROL_SPACE`, and all wrapper/adapter/compatibility/fallback/
migration spellings. It also requires the old module path to be absent and
requires `GlobalSelectorV1`, `BranchSelectorV1`, `PreparedPublication`,
`open_coherent_view`, `SELECTOR_SPACE`, `advance_gc`, and `load_branch_head`.

Invocation against the exact disposable 413e worktree:

```text
bash packages/lix/tests/forktree_branch_migration_source_gate.sh \
  /root/repos/evidence/forktree-branch-head-control-deletion-e166/frontier-413e \
  413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d \
  820fe560da3bbd2b00b788b0b1759c409048cd6e
```

Result: exit `1`, `RESULT=RED`. The frontier still reports 82 overlapping
`BranchHeadControl` matches, 13 cache matches, 35 context matches, 2
reachability matches, 71 snake-case matches, 32 staging matches, and one
legacy physical-space match. Required ForkTree symbols pass; old module and
wrapper-specific paths are absent.

- source-gate log SHA-256: `e8218c52360a46e142e4f4540cadd9e0cb44e62e95ed9f71ad5f3572ef015138`

## Frozen source files

- `forktree_branch_migration_acceptance.rs`
  - SHA-256: `b91f67f139ce397b444e7e53c68558a812ed7a6d0bd28bdab82dd38904182666`
- `forktree_branch_migration_source_gate.sh`
  - SHA-256: `8ee5007c6a843025f8478ab7cdf92355fc9cf21a2ed9c2b54dbb5bea0c9eccdb`
- this report
  - SHA-256: `5e0341995a7a78f4c1f154c310a77a2ba9a30aaa5d591da9dc3999e9dd91db87`

The prior frozen negative compile probes for deleted control APIs remain
bound through the parent oracle. The future branch migration candidate must
also pass the tracked-state reader/space deletion gate before runtime.

## Future adapter order

These commands are dormant and must run only against an immutable
compile-green candidate with fresh isolated target/database paths. Stop at the
first failure; each cell is capped at 20 minutes:

```text
# 1. Branch migration source/deletion gate
bash packages/lix/tests/forktree_branch_migration_source_gate.sh \
  <candidate-worktree> <candidate-head> <candidate-tree>

# 2. Existing whole-module tracked-state reader deletion gate
bash packages/lix/tests/forktree_tracked_state_reader_deletion_gate.sh \
  <candidate-worktree> <candidate-head> <candidate-tree>

# 3. Formatting and diff hygiene
cargo fmt --all -- --check
git diff --check

# 4. Pure migration model
TMPDIR=<repo-local-temp> rustc --edition=2021 -D warnings \
  packages/lix/tests/forktree_branch_migration_acceptance.rs \
  -o <isolated-model-binary>
<isolated-model-binary>

# 5. Memory
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 timeout 1200 \
  cargo test -p lix_tests --test forktree_branch_migration_acceptance \
  branch_migration_memory --exact --nocapture --test-threads=1

# 6. RocksDB
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 timeout 1200 \
  cargo test -p lix_tests --test forktree_branch_migration_acceptance \
  branch_migration_rocksdb --exact --nocapture --test-threads=1

# 7. SlateDB
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 timeout 1200 \
  cargo test -p lix_tests --test forktree_branch_migration_acceptance \
  branch_migration_slatedb --exact --nocapture --test-threads=1
```

The future adapter harness must use only typed ForkTree/publication APIs. It
must prove one selector/epoch CAS plane, one atomic publication, no-op/stale
zero-write behavior, corruption/reopen, and both race orders without reading
or recreating the deleted control space. Existing H1 checkpoint and H4 SQL
provider oracles remain separate acceptance obligations.
