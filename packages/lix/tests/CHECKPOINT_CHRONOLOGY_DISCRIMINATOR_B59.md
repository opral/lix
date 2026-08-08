# Checkpoint chronology / reader-deletion discriminator

Test/report-only acceptance package anchored to blocked production head
`39b12568f86d02ec81327cb672b7ef5f7e936448` (tree
`03f972b76a6160b7ba7e52c92ce8096203a26269`, parent
`b59e1f11a51153e0a787a81f0f25bf104d150aaf`). It contains no production,
benchmark, adapter, or R5 source changes.

The next direct production successor must satisfy four independent gates.

## 1. Exact checkpoint chronology

The standalone vector oracle defines this immutable semantic shape:

```text
root R (no marker) -> checkpoint C (marker commit=C)
                       -> ordinary O (no marker)
                       -> checkpoint D (marker commit=D)
```

Required classification is marker IDs `[C, D]`; `O` is never classified as a
checkpoint. `R` remains the implicit root baseline. A marker whose branch or
embedded commit identity differs from the observed commit must fail closed.
The oracle also rejects duplicate markers for one observed commit.

Run the pure dependency-free oracle with:

```text
rustc --edition=2021 --test \
  packages/lix/tests/forktree_checkpoint_chronology_vectors_b59.rs \
  -o /tmp/forktree-checkpoint-discriminator-oracle
/tmp/forktree-checkpoint-discriminator-oracle --nocapture
```

Expected result: 3 passed, 0 failed. This verifies the expected contract, not
the non-compiling production implementation.

## 2. One ForkTree same-view chronology owner

Checkpoint SQL and filesystem working-diff baseline selection must call one
ForkTree-owned checkpoint/chronology seam over their existing caller-owned
`StorageRead`/`ForkTreeReadFacade`. The owner must authenticate commit
topology and state rows in that same view. A second chronology reconstruction
in `packages/lix/src/checkpoint.rs`, alongside the remaining TrackedState
reader, is forbidden.

The discriminator rejects local `checkpoint_history*` functions,
`checkpoint_marker_from_rows`, and direct `scan_state_rows_at_commit` use in
`checkpoint.rs`; it requires an owner seam under `forktree/` and a binding from
both checkpoint SQL and filesystem working-diff providers. Providers must not
call `begin_read` themselves.

## 3. Certified-history / TrackedState route deletion

The following are forbidden in `sql2/history_route.rs`, `sql2/context.rs`,
`sql2/mod.rs`, and checkpoint/file/directory/diff/working-diff providers:

```text
CertifiedHistoryStoreReader / CertifiedHistoryReader
certified_history_reader / certified_request
TrackedStateScanRequest / TrackedStateReadColumns
TrackedStateStoreReader / TrackedStateContext
tracked_state.reader( / tracked_state_reader(
```

This rejects both the old request factory and the reachable scan field, rather
than only checking that providers stopped importing a reader directly.

## 4. Exact residual/compiler accounting

The script reports exact head/tree/parent, all residual owner counts, compiler
log SHA-256, error/warning totals, and normalized diagnostic fingerprints. The
blocked 39b reference values are:

| residual/compiler measure | 39b reference |
|---|---:|
| `TrackedStateStoreReader` | 17 |
| `TrackedHeadContext` | 34 |
| `BranchHeadControlContext` | 35 |
| `BranchHeadControlCache` | 13 |
| `stage_branch_head_control` | 32 |
| `branch_head_control_precondition` | 6 |
| `untracked_lifecycle_generation` | 5 |
| library compiler frontier | 139 errors / 9 warnings |
| library+tests compiler frontier | 382 errors / 16 warnings |
| normalized library diagnostics | `22ba78779c90b943090136f47b68d5dfe2ac452f4321e2fc523dc1da1c1442f4` |
| normalized library+tests diagnostics | `17c1da26ee8108e34f6e75304d4fed03a7a249ad5975062f7ddeaa069f4d9775` |
| normalized library warnings | `4f8e8a2ea9193abe58660300ee7733587a70bdac86c4bcec1bd125b04ca7327a` |
| normalized library+tests warnings | `d5d673e2d3d9c7da229188125b8277d0383e88d425b72d9d9bc7bd9a2f3bfb42` |

Compiler checks are attribution-only and remain bounded; no runtime or broad
matrix is part of this package.

## Frozen package provenance

| artifact | SHA-256 |
|---|---|
| `forktree_checkpoint_chronology_discriminator_b59.sh` | `341b71dbbd5c40b297c585c7b7d8277c992135ee6cd3092e4167ecaca0bfe5ab` |
| `forktree_checkpoint_chronology_vectors_b59.rs` | `26d44f116a548d8f595a03409c95c5e957fcab4accd9b533f70f1e580e23ebee` |
| `forktree_checkpoint_chronology_vectors_b59.tsv` | `c032e64109674b06bb8db22bca04cf2e943b0df264353dbf528efed0e37316d8` |

Baseline replay against 39b is intentionally RED because it still has the
duplicate `checkpoint_history_for_branch_forktree` path and the reachable
certified-history request/scan. The baseline discriminator log is
`/tmp/forktree-checkpoint-discriminator-39b.log`, SHA-256
`e971630e6faebee4599e02a5135d37201183f60ea57af2f7a5ac83101a754c29`.

Successor review command:

```text
bash packages/lix/tests/forktree_checkpoint_chronology_discriminator_b59.sh \
  <candidate-worktree> <head> <tree> 39b12568f86d02ec81327cb672b7ef5f7e936448 \
  <library-log> <library-tests-log>
```

Do not run or mutate R5 from this package. A successor may be marked ready for
runtime qualification only after this discriminator is GREEN and its exact
compiler/deletion report is independently reviewed.
