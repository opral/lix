# BranchHeadControl deletion and compile-fail gate

Status: frozen test/report-only successor to the 6caaa acceptance oracle.
This package does not edit production, build a candidate, register a runtime
test, add a wrapper/adapter, or accept a compatibility path. It binds the
future hard cut to the sole `GlobalSelectorV1`/`BranchSelectorV1` owner and
one global epoch/CAS plane.

## Immutable bindings

Frozen oracle base:

- acceptance ref: `origin/codex/forktree-branch-head-control-acceptance-e166`
- acceptance head: `6caaa26502345930d03c01984fe366c2d2b9cc4b`
- acceptance tree: `e2cac0d3983ce1f939434b8b5342e2242a66c8a1`
- e166 architecture base: `e1666edd0b4d814a88d985086ecc5a477b5d32e6`
- e166 tree: `c680bd7e7f7b70cd784676515839af2dcbbc7917`
- e166→acceptance full-index binary diff: `96a2a1ca02762ebcb1b2f15ae11680e843842a440d63d2e784b7e422ab715ccd`
- e166→acceptance ordinary diff: `dd174a4c8b75fbacbfa7b54d631c3b2ead586e7769086275a79969475ca700d8`
- e166→acceptance stable patch ID: `897be6f420842e9c5985b81865b035c22f457026`

Corrected source frontier under review:

- ref: `origin/codex/forktree-stage2-sql-entity-readers-canonical-11442`
- head: `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d`
- tree: `820fe560da3bbd2b00b788b0b1759c409048cd6e`
- parent: `11442c1e0023e20307a7231d88cd557bc704fd13`
- e166→frontier full-index binary diff: `70bc6bc03524855be515c9d1a5d0c75c77ebd159fbd44d5f646483ce14460329`
- e166→frontier ordinary diff: `5302fd9f85f45f45beafdcc72f1e34691c4542be3a2f4dd30dc6bf4516052f4a`
- e166→frontier stable patch ID: `df0747c2c7e026147361aab7edd4f741efca9b33`

The frontier is not promoted by this package. It is calibrated as a
compile/deletion RED frontier until the entire old owner and its callers are
gone.

## Files and source hashes

- `forktree_branch_head_control_deletion_gate.sh`
  - SHA-256: `56d7847ed88140983e3d5fb0fe6a643e2ab7a0d2834a53705d692121c1baf50f`
- `forktree_branch_head_control_compile_fail.sh`
  - SHA-256: `e08f8c891e8e7011a2b6b5da99b71fc27ead801c85ce095dfb85f6ffd598ef74`
- `forktree_branch_head_control_forbidden_api.rs`
  - SHA-256: `c29ad17a89e28bf2ff647d279976a1d3ef36f66ab971631d8f5a06d0ca494b5d`
- `forktree_branch_head_control_forbidden_space.rs`
  - SHA-256: `8b294a60900839cee9322895f9a4403110aa7e0397627a33ad01b4f1c35a4d7e`

The earlier pure model remains the semantic oracle:

- `forktree_branch_head_control_acceptance.rs`
- SHA-256: `2abfc31b5d754d2d6a1ad37ba8ed5ad36b05502a267dc423f85b3946e8cdb532`
- prior model binary SHA-256: `332d32a96bf0dea150a7640b03825cfca7258f5fb8ab44c73023b91936f25216`
- prior model log SHA-256: `cfb41461068872fa542a113a1d63146570f6cd776071688c54564b67d45affc8`

## Deletion contract

The first runnable candidate must have zero production-source matches for:

- `BranchHeadControlContext`, `BranchHeadControlCache`, the old control
  module/reexports, and every old control caller;
- `BranchHeadTrackedReachability`, direct staging/precondition helpers, and
  `untracked_lifecycle_generation`;
- `BRANCH_HEAD_CONTROL_SPACE`, its namespace/codec/key families, and legacy
  physical layout names; and
- any `BranchHeadControl` wrapper, adapter, facade, compatibility, fallback,
  or migration path.

`packages/lix/src/branch/control.rs` must be absent. Public branch/head API
vocabulary may remain only as a semantic facade that delegates to the
authenticated ForkTree selector/object owner. It may not own raw storage,
cache a durable branch head, reconstruct a selector, or publish independently.

The sole replacement must retain `GlobalSelectorV1`, `BranchSelectorV1`,
`PreparedPublication`, `open_coherent_view`, `SELECTOR_SPACE`, and `advance_gc`.
All create/switch/delete, global sequence, transaction precondition, GC
observation, initialization, no-op, stale, unrelated-owner, cold-reopen, and
partial-publication semantics remain those of the 6caaa model.

## Exact frontier result

The deletion gate was run against a disposable worktree of the exact 413e
head:

```text
bash packages/lix/tests/forktree_branch_head_control_deletion_gate.sh \
  /root/repos/evidence/forktree-branch-head-control-deletion-e166/frontier-413e \
  413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d \
  820fe560da3bbd2b00b788b0b1759c409048cd6e
```

Result: exit `1`, `RESULT=RED`.

The main overlapping residue counts were:

| pattern | matches |
| --- | ---: |
| `BranchHeadControl` | 82 |
| `BranchHeadControlCache` | 13 |
| `BranchHeadControlContext` | 35 |
| `BranchHeadTrackedReachability` | 2 |
| `branch_head_control` | 71 |
| `stage_branch_head_control` | 32 |
| `untracked_lifecycle_generation` | 5 |
| `BRANCH_HEAD_CONTROL_SPACE` | 1 |
| `BRANCH_HEAD_CONTROL_` | 4 |

Counts overlap and are not additive. Required ForkTree symbols pass, the old
module path is absent, and wrapper/compatibility-specific names are absent;
the remaining old readers, writers, reexports, cache, GC owner, and physical
space correctly keep the frontier RED.

- captured log:
  `/root/repos/evidence/forktree-branch-head-control-deletion-e166/frontier-413e-gate.log`
- log SHA-256: `0963f1158b709f0bfec997355f5381f3a666ff7acf193a6bc303acdee0e8267e`

## Dormant compile-fail probes

The two probe sources intentionally import the deleted old API and legacy
space. They are not Cargo tests and were not compiled on the non-runnable
frontier. The future compiler runner must report a nonzero compiler status
with an unresolved-import/name diagnostic for both; compiling either probe is
a hard failure.

```text
bash packages/lix/tests/forktree_branch_head_control_compile_fail.sh \
  <candidate-worktree> \
  <candidate-target>/debug/deps \
  <candidate-target>/debug/deps/<liblix-rlib>
```

This is a negative API proof, not permission to widen visibility or reopen a
raw `StorageSpace`/put/delete interface.

## Future exact acceptance order

Run only against an immutable, compile-green candidate, with a fresh isolated
target and fresh adapter paths. Stop at the first failure; each cell is capped
at 20 minutes:

```text
# 1. Exact source/deletion gate
bash packages/lix/tests/forktree_branch_head_control_deletion_gate.sh \
  <candidate-worktree> <candidate-head> <candidate-tree>

# 2. Formatting and patch hygiene
cargo fmt --all -- --check
git diff --check

# 3. Negative compiler probes, after the candidate Lix rlib exists
bash packages/lix/tests/forktree_branch_head_control_compile_fail.sh \
  <candidate-worktree> <candidate-target>/debug/deps \
  <candidate-target>/debug/deps/<liblix-rlib>

# 4. Pure model, inherited from the frozen 6caaa oracle
rustc --edition=2021 -D warnings \
  packages/lix/tests/forktree_branch_head_control_acceptance.rs \
  -o <isolated-model-binary>
<isolated-model-binary>

# 5. Memory
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 timeout 1200 \
  cargo test -p lix_tests --test forktree_branch_head_control_acceptance \
  forktree_branch_head_control_memory --exact --nocapture --test-threads=1

# 6. RocksDB
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 timeout 1200 \
  cargo test -p lix_tests --test forktree_branch_head_control_acceptance \
  forktree_branch_head_control_rocksdb --exact --nocapture --test-threads=1

# 7. SlateDB
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 timeout 1200 \
  cargo test -p lix_tests --test forktree_branch_head_control_acceptance \
  forktree_branch_head_control_slatedb --exact --nocapture --test-threads=1
```

No future adapter cell may read the deleted control space or use an old
control/cache fixture. All selectors, preconditions, branch lifecycle, GC
roots, and epoch movement must go through the single typed ForkTree owner.
