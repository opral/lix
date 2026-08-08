# TrackedStateStoreReader and reader-only tracked_state deletion gate

Status: frozen test/report-only compiler/deletion gate. It is anchored at the
exact `413e08a` source frontier and bound to the previously frozen 6caaa
BranchHeadControl selector/epoch oracle. It does not edit production, compile
the candidate, run an adapter matrix, duplicate the checkpoint oracle, or
duplicate the SQL provider oracle.

## Immutable bindings

Acceptance/oracle base:

- ref: `origin/codex/forktree-branch-head-control-deletion-gate-e166`
- head/tree: `be6ea48cfea4d4a49844216aee683f6ada9ec708` /
  `837983ec8d835f7d82defc31bedf0c0e02d5ab06`
- architecture base: `e1666edd0b4d814a88d985086ecc5a477b5d32e6`
- e166→acceptance full-index binary diff:
  `271c1d114641c92cdbbbbedbabdaa7fa8a147ccd46c50cf7afc3cba601838499`

Source frontier:

- ref: `origin/codex/forktree-stage2-sql-entity-readers-canonical-11442`
- head/tree: `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d` /
  `820fe560da3bbd2b00b788b0b1759c409048cd6e`
- parent: `11442c1e0023e20307a7231d88cd557bc704fd13`
- e166→413e full-index binary diff:
  `70bc6bc03524855be515c9d1a5d0c75c77ebd159fbd44d5f646483ce14460329`
- e166→413e ordinary diff:
  `5302fd9f85f45f45beafdcc72f1e34691c4542be3a2f4dd30dc6bf4516052f4a`
- e166→413e stable patch ID: `df0747c2c7e026147361aab7edd4f741efca9b33`

The 413e frontier remains compile/deletion RED. This package records the
remaining reader and legacy physical ownership; it does not promote 413e.

## Frozen files

- `forktree_tracked_state_reader_deletion_gate.sh`
  - SHA-256: `816cc45f6f60d4812a9fa34a6e52ecf1ad1bc3e5a86801ffeb96225ca4aa854f`
- `forktree_tracked_state_compile_fail.sh`
  - SHA-256: `adfe6a51cee924fee89352d4c42a6ea79aa76cd861b20ef82eba0107a07ec13d`
- `forktree_tracked_state_forbidden_reader.rs`
  - SHA-256: `b7222ca874b0e5a6bdec75ec391f35377841bd937fd805800259fc7f8599c727`
- `forktree_tracked_state_forbidden_space.rs`
  - SHA-256: `b507f279710611c7fdfbb5d5196c6411ac4caba23365fe7f0e75fe000a25993d`

## Required ownership and deletion

The first runnable candidate must delete `TrackedStateStoreReader`, its
`TrackedStateContext::reader` constructor, reader caches/replay state, the
`tracked_state` reader reexport, and all reader-only modules:

```text
packages/lix/src/tracked_state/diff.rs
packages/lix/src/tracked_state/diff_id.rs
packages/lix/src/tracked_state/merge.rs
packages/lix/src/tracked_state/row_materialization.rs
```

Their `tracked_state/mod.rs` reexports must disappear at the same compiler
boundary. `context.rs` and `types.rs` may survive only for a separately
owned, explicitly typed writer/lowering responsibility; they may not retain a
reader constructor, historical fallback, or alternate serving authority.

All tracked-state physical spaces are forbidden, including tree chunks,
delta segments, change locators, state manifests/mutation inventories,
mutation-directory nodes, scoped-range nodes, current-state data parts and
refs, certified batches, row groups, packed current bases, and root-current
base controls. No wrapper, adapter, compatibility reader, migration, fallback,
or empty-success substitution is allowed.

The sole replacement is ForkTree: one coherent view, authenticated state
point/range, commit topology/member records, branch head, and typed selector /
epoch publication. No SQL/history/merge/checkpoint caller may retain a private
tracked-state read path.

## Exact remaining semantic uses on 413e

The gate inventories every matching line in each cohort using the union of
`TrackedStateStoreReader`, `TrackedStateContext`, and `tracked_state::` source
references. Counts below are per-file and are not additive because patterns
overlap. The captured log contains the exact line inventory.

| cohort | remaining matching lines by file |
| --- | --- |
| checkpoint | `checkpoint.rs` 4; `session/checkpoint.rs` 2; `sql2/providers/checkpoint.rs` 2 |
| history | `sql2/history_route.rs` 1; `sql2/providers/change.rs` 2; `sql2/providers/file_history.rs` 12; `sql2/providers/directory_history.rs` 2 |
| SQL diff/working diff | `sql2/providers/diff.rs` 4; `sql2/providers/working_diff.rs` 4; `sql2/providers/filesystem_working_diff.rs` 8 |
| merge analysis | `session/merge/analysis.rs` 4; `session/merge/branch.rs` 6; `session/merge/conflicts.rs` 3; `session/merge/stats.rs` 2 |
| transaction reconciliation/undo | `transaction/context.rs` 26; `session/undo_redo.rs` 1 |

Additional direct reader ownership remains in `tracked_state/context.rs`,
`tracked_state/diff.rs`, `gc.rs`, and transaction reader construction. The
legacy spaces remain in GC, commit-graph, session, storage-bench, engine, and
tracked-state code. These are deletion dependencies, not accepted exemptions.

Required migration map:

- checkpoint marker point/history resolution → selected ForkTree state,
  authenticated commit topology/history, and typed checkpoint/recovery
  selectors;
- SQL commit/change/file/directory history → CommitCatalog/ChangeCatalog and
  authenticated commit/member/ref-change objects, with missing records or
  chronology failing closed;
- SQL diff and filesystem working diff → ForkTree root-to-root hash-pruned
  diff plus transaction-local overlay, never a tracked-state scan;
- merge analysis/conflicts/stats → ForkTree diff/merge primitives with the
  semantic conflict policy retained at the public layer;
- transaction reconciliation and undo/redo → the selected coherent roots and
  typed selector publication, with one plan/prepared commit and no reader
  cache or legacy root reconstruction;
- GC/retention → selector/object closure and bounded owner traversal, not
  tracked-state manifests, ranges, locators, or tree chunks.

## Historical fail-closed prerequisite

Before deleting the reader, 413e’s derived/history boundary must remain
explicitly fail-closed: `live_state/derived.rs` contains
`request_may_include_derived`, `is_derived_schema`, and the “fail closed”
guard. A missing historical owner must not become an empty current-state
success. The replacement must use the already accepted ForkTree primitives:
`open_coherent_view`, `state_point`, `state_range`,
`load_commit_topologies`, `load_commit_member_records`,
`validate_commit_topology`, and `load_branch_head`.

The gate also requires these historical fail-closed owner tokens and reports
their presence separately from the deletion result.

## Exact gate result

Invocation against a disposable worktree of exact 413e:

```text
bash packages/lix/tests/forktree_tracked_state_reader_deletion_gate.sh \
  /root/repos/evidence/forktree-branch-head-control-deletion-e166/frontier-413e \
  413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d \
  820fe560da3bbd2b00b788b0b1759c409048cd6e
```

Result: exit `1`, `RESULT=RED`.

The exact gate log SHA-256 is recorded at freeze below. The required
ForkTree and historical fail-closed tokens pass. The gate correctly fails on
29 `TrackedStateStoreReader` references, all four reader-only module paths,
their reexports, and the tracked-state physical spaces; wrapper/compatibility
specific names are absent. The 413e source is therefore not a runnable
candidate.

## Dormant negative compiler probes

The probes intentionally import `TrackedStateStoreReader`/reader-only
`TrackedStateDiff` and `TRACKED_STATE_TREE_CHUNK_SPACE`. They are not Cargo
tests and were not compiled. A future candidate must compile its normal Lix
rlib first, then both probes must fail with unresolved import/name diagnostics;
successful compilation is a hard blocker.

```text
bash packages/lix/tests/forktree_tracked_state_compile_fail.sh \
  <candidate-worktree> \
  <candidate-target>/debug/deps \
  <candidate-target>/debug/deps/<liblix-rlib>
```

## Future dual-adapter order

Only an immutable compile-green successor may run these cells. No current
413e build or runtime is claimed. Each cell is capped at 20 minutes and the
sequence stops at the first blocker:

```text
# 1. Whole-module source/deletion residue and exact provenance
bash packages/lix/tests/forktree_tracked_state_reader_deletion_gate.sh \
  <candidate-worktree> <candidate-head> <candidate-tree>

# 2. Formatting and diff hygiene
cargo fmt --all -- --check
git diff --check

# 3. Negative reader/space compiler probes
bash packages/lix/tests/forktree_tracked_state_compile_fail.sh \
  <candidate-worktree> <candidate-target>/debug/deps \
  <candidate-target>/debug/deps/<liblix-rlib>

# 4. Existing standalone selector/epoch model, no production build
rustc --edition=2021 -D warnings \
  packages/lix/tests/forktree_branch_head_control_acceptance.rs \
  -o <isolated-model-binary>
<isolated-model-binary>

# 5. Memory semantic smoke
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 timeout 1200 \
  cargo test -p lix_tests --test forktree_tracked_state_reader_acceptance \
  tracked_state_reader_memory --exact --nocapture --test-threads=1

# 6. RocksDB semantic smoke, flush/drop/reopen and corruption
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 timeout 1200 \
  cargo test -p lix_tests --test forktree_tracked_state_reader_acceptance \
  tracked_state_reader_rocksdb --exact --nocapture --test-threads=1

# 7. SlateDB semantic smoke, flush/drop/reopen and corruption
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 timeout 1200 \
  cargo test -p lix_tests --test forktree_tracked_state_reader_acceptance \
  tracked_state_reader_slatedb --exact --nocapture --test-threads=1
```

The future runtime harness is intentionally only a command contract here; it
must use ForkTree/public typed APIs and must not recreate tracked-state
storage fixtures. H1 checkpoint and H4 SQL provider acceptance remain
separate gates.
