# TrackedStateStoreReader whole-module deletion gate, exact b59

Status: frozen TEST/REPORT-ONLY source gate. This package is anchored directly
to accepted ForkTree b59 and contains no production edits, builds, or runtime
results. It corrects the prior 72f10a4 gate’s stale 413e anchor, incomplete
factory probe, and hypothetical adapter-target commands.

## Immutable source binding

```text
head:   b59e1f11a51153e0a787a81f0f25bf104d150aaf
tree:   700fd04d21bc40c05425c9fc9e10d65c9e1eda24
parent: 713455a3557907ce705d06f720fcdc4486bddd4a
713..b59 full-index SHA-256: 4b2885709ba09034068b321be2fe5f27348d6681b1060133af1df0b7d76bb8d4
713..b59 stable patch ID: 63dcb8dcecba8a25dea0ce8be19d26cdac264729
```

The accepted preceding ForkTree source frontier is 713455a3. The former
72f10a4 deletion gate is historical evidence only; no candidate may be
rebased to 413e, 72f, or another stale frontier for this gate.

## Corrected b59 historical prerequisite

b59 owns the fail-closed historical boundary in
`packages/lix/src/forktree/serving.rs`:

- `load_required_commit_catalog_entry` looks up the selected CommitCatalog
  entry and turns absence into `selected CommitCatalog entry is absent`;
- member/point loading validates catalog/object identity and retained closure
  before consulting a state root;
- `state_point_on_read` is reached only after those checks.

The exact b59 tests in `packages/lix/src/forktree/tests.rs` are:

- `historical_absence_requires_authenticated_commit_and_root`: valid
  commit/root plus absent key is authenticated absence;
- `historical_missing_commit_catalog_fails_for_point_and_batch`: point and
  batch reads reject missing selected catalog ownership;
- `historical_missing_state_root_fails_before_empty_result`: missing root is
  corruption, not an empty historical result.

The gate requires these exact owner/test tokens on every candidate. It also
requires `request_may_include_derived`, `is_derived_schema`, and the explicit
“fail closed” guard in `live_state/derived.rs`; a reader migration may not turn
missing historical ownership into a current-state empty row.

## First runnable deletion wave

The compiler-driven cut removes the reader as a type and responsibility:

```text
packages/lix/src/tracked_state/context.rs       TrackedStateStoreReader,
                                                   reader(), replay/cache state
packages/lix/src/tracked_state/mod.rs           reader/diff/merge/materialization reexports
packages/lix/src/tracked_state/diff.rs          reader-only diff implementation
packages/lix/src/tracked_state/diff_id.rs       reader-only diff IDs
packages/lix/src/tracked_state/merge.rs         reader-only merge planner
packages/lix/src/tracked_state/row_materialization.rs
                                                reader-only row materialization
```

`context.rs` and `types.rs` may survive only for a separately owned writer or
publication-lowering responsibility. They must not retain a historical reader,
legacy fallback, cache, or alternate serving authority. No adapter, wrapper,
compatibility export, migration, empty-success branch, or replacement index is
allowed.

The same compiler wave removes every caller of the exact factories:

```text
TrackedStateContext::reader<S>      declaration in tracked_state/context.rs
tracked_state.reader(                variable-form call sites
Transaction::tracked_state_reader   transaction/context.rs helper
with_opening_tracked_reader          transaction/context.rs helper and callers
```

On clean b59 source inspection the relevant overlapping counts are:

```text
TrackedStateStoreReader       29
tracked_state_reader(         11
with_opening_tracked_reader     3
scan_batch_at_commit           37
diff_commits(                  52
crate::tracked_state::        384
```

`TrackedStateContext::reader` has zero literal qualified call strings because
production calls it through a context variable; the declaration and the
`tracked_state.reader(` call form are both probed. Existing wrapper/adapter/
compatibility spellings are also scanned explicitly, not inferred absent from
the type count.

## Cohort inventory and compiler-driven order

The source gate emits exact matching lines for every path below. Counts
overlap and are not additive.

| order | cohort | current b59 paths | ForkTree owner after migration |
| ---: | --- | --- | --- |
| 1 | checkpoint/history reconstruction | `checkpoint.rs`, `session/checkpoint.rs`, `sql2/providers/checkpoint.rs`, `sql2/history_route.rs`, `sql2/providers/change.rs`, `sql2/providers/file_history.rs`, `sql2/providers/directory_history.rs` | authenticated CommitCatalog/ChangeCatalog, commit topology, state point/range, typed checkpoint/recovery selectors |
| 2 | SQL diff and working diff | `sql2/providers/diff.rs`, `sql2/providers/working_diff.rs`, `sql2/providers/filesystem_working_diff.rs` | ForkTree root-to-root hash-pruned diff plus transaction-local overlay |
| 3 | merge analysis | `session/merge/analysis.rs`, `session/merge/branch.rs`, `session/merge/conflicts.rs`, `session/merge/stats.rs` | ForkTree merge base, historical state rows, and semantic conflict policy |
| 4 | transaction reconciliation/undo | `transaction/context.rs`, `session/undo_redo.rs` | one coherent view and one prepared publication using canonical selectors/roots |
| 5 | physical residue | GC, storage-bench, commit-graph, engine, and tracked-state references to old spaces | selector/object closure and typed epoch ownership only |

The order is reader-first: establish typed ForkTree point/range/history and
merge primitives, migrate each cohort through one retained view, compile out
every old caller, then delete the reader type/modules/reexports and old
physical-space constants/writers in the same compiler wave. Writer-last means
no legacy writer or space survives as a shadow authority after its last reader
disappears. The negative import probes must fail on the resulting rlib.

## Forbidden residue

The gate rejects these names in `packages/lix/src`:

```text
TrackedStateStoreReader, TrackedStateReaderAdapter,
TrackedStateReaderWrapper, TrackedStateReaderCompat,
tracked_state_reader_adapter, tracked_state_reader_wrapper,
tracked_state_reader_compat, tracked_state_reader_fallback,
tracked_state_reader_migration, legacy_tracked_state_reader,
history_reader_adapter, history_reader_wrapper, history_reader_compat,
state_reader_adapter, state_reader_wrapper, state_reader_compat,
columnar_history_fallback, tracked_state_compat
```

It also rejects these old physical names:

```text
TRACKED_STATE_TREE_CHUNK_SPACE
TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE
TRACKED_STATE_CHANGE_LOCATOR_SPACE
TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE
TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE
MUTATION_DIRECTORY_NODE_SPACE
SCOPED_RANGE_NODE_SPACE
CURRENT_STATE_DATA_PART_SPACE
CURRENT_STATE_DATA_PART_REFS_SPACE
CERTIFIED_ENTITY_BATCH_SPACE
CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE
CERTIFIED_ENTITY_BATCH_PAGE_SPACE
ROW_GROUP_MANIFEST_SPACE
ROW_GROUP_COLUMN_SPACE
PACKED_CURRENT_BASE_SPACE
PACKED_CURRENT_BASE_CONTROL_SPACE
PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE
ROOT_CURRENT_BASE_SPACE
```

The four reader-only module files and their `tracked_state/mod.rs` reexports
are hard deletion requirements. No fallback scan, cache, compatibility reader,
or side index can satisfy this gate.

## Concrete targets and future acceptance commands

There is no `lix_tests` crate and b59 has no existing dedicated ForkTree
dual-adapter acceptance target. The gate checks real existing targets rather
than inventing that package:

```text
packages/lix/Cargo.toml                                  package `lix`
packages/lix/tests/integration/main.rs                   test target `integration` (Memory)
packages/lix/src/forktree/tests.rs                      lib target `forktree::tests::*` (Memory)
packages/lix/tests/integration/sql/lix_file_history.rs  SQL file-history module
packages/lix/tests/integration/sql/lix_directory_history.rs
packages/lix/tests/integration/sql/diff_commands.rs
packages/lix/tests/integration/sql/checkpoint.rs
packages/lix/tests/semantic_merge.rs
packages/engine-benchmarks/tests/checkpoint_gc_replay_reopen.rs
packages/engine-benchmarks/tests/corruption_recovery_qualification.rs
packages/engine-benchmarks/benches/tracked_working_diff.rs (RocksDB/SlateDB)
```

The required future adapter target is a concrete test-only file, not a
hypothetical package or API:

```text
packages/engine-benchmarks/tests/forktree_tracked_state_reader_acceptance.rs
Cargo package/target: `lix_benchmarks` /
  `forktree_tracked_state_reader_acceptance`
required features: `storage-benches,slatedb`
test names: tracked_state_reader_memory,
            tracked_state_reader_rocksdb,
            tracked_state_reader_slatedb
```

It must use public `Engine`, `StorageAdapter`, and ForkTree-facing behavior;
it must not recreate tracked-state spaces or expose raw `StorageSpace`. The
common fixture covers point/scan, NULL/tombstone/value, valid absent key,
missing/malformed/wrong-kind CommitCatalog/root, one retained read, and cold
reopen. Existing `checkpoint_gc_replay_reopen` and
`corruption_recovery_qualification` are reusable backend lifecycle scaffolds,
but are not substitutes for this historical point/scan oracle until their
assertions move to ForkTree ownership.

After a compile-green successor, exact commands are:

```bash
# Source/provenance gate; exact b59 is intentionally RED.
bash packages/lix/tests/forktree_tracked_state_reader_deletion_gate_b59.sh \
  <candidate-worktree> <candidate-head> <candidate-tree>

# Existing Memory unit target, narrowed to b59 historical controls.
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=1 \
  cargo test -p lix --lib -- \
  forktree::tests::historical_ --nocapture --test-threads=1

# Existing Memory SQL/integration target for migrated public callers.
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=1 \
  cargo test -p lix --test integration -- \
  sql::lix_file_history --nocapture --test-threads=1

# Concrete dual-adapter target required by this gate.
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks \
  --test forktree_tracked_state_reader_acceptance \
  --features storage-benches,slatedb -- --nocapture --test-threads=1

# Negative compiler imports, only after the candidate rlib exists.
bash packages/lix/tests/forktree_tracked_state_compile_fail_b59.sh \
  <candidate-worktree> <candidate-target>/debug/deps \
  <candidate-target>/debug/deps/<liblix-rlib>
```

The adapter target runs Memory first, then RocksDB, then SlateDB, stopping at
the first failure; every cell is capped at 20 minutes. Required semantics are:
a valid commit/root plus absent key returns authenticated absence; missing
CommitCatalog, missing root, malformed catalog/root, or wrong-kind substitution
returns an observable error; NULL, tombstone, and value remain distinct; point
and scan share one retained read and have no retry, fallback, cache, or second
reader; cold reopen preserves the same fail-closed behavior.

## Frozen b59 result

This package is calibrated RED on exact b59 by source inspection only. The
historical ForkTree owner/tests and concrete target paths pass; the gate is RED
because b59 still contains the reader type/factory/callers, all four
reader-only modules/reexports, and the forbidden tracked-state physical names.
No cargo build, runtime, benchmark, or adapter matrix was run.

The dormant probes are:

```text
packages/lix/tests/forktree_tracked_state_forbidden_reader_b59.rs
packages/lix/tests/forktree_tracked_state_forbidden_space_b59.rs
```

Their successful compilation is a hard blocker once a future rlib exists.

## Frozen package files

```text
packages/lix/tests/forktree_tracked_state_reader_deletion_gate_b59.sh
packages/lix/tests/forktree_tracked_state_compile_fail_b59.sh
packages/lix/tests/forktree_tracked_state_forbidden_reader_b59.rs
packages/lix/tests/forktree_tracked_state_forbidden_space_b59.rs
packages/lix/tests/FORKTREE_TRACKED_STATE_READER_DELETION_GATE_B59.md
```

Only these test/report files are in this package. No production path is
modified, and no PR or merge is created by this lane.
