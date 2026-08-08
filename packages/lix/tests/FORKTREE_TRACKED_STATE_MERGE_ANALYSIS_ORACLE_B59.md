# ForkTree tracked-state merge-analysis migration oracle, exact b59

Status: frozen TEST/REPORT-ONLY source/model gate. This package is anchored
directly to accepted b59. It contains no production edits, build results, or
runtime results.

## Immutable binding

```text
base/head: b59e1f11a51153e0a787a81f0f25bf104d150aaf
base tree: 700fd04d21bc40c05425c9fc9e10d65c9e1eda24
parent:    713455a3557907ce705d06f720fcdc4486bddd4a
713..b59 full-index SHA-256: 4b2885709ba09034068b321be2fe5f27348d6681b1060133af1df0b7d76bb8d4
713..b59 stable patch ID: 63dcb8dcecba8a25dea0ce8be19d26cdac264729
```

The exact source verifier is
`forktree_tracked_state_merge_analysis_oracle_b59.sh`; the pure model is
`forktree_tracked_state_merge_analysis_model_b59.rs`. Both are test/report
artifacts only.

## Current ownership and callers

`packages/lix/src/session/merge/analysis.rs` currently owns the semantic
assembly around a physical reader:

```text
analyze<S>(&mut TrackedStateStoreReader<S>, MergeCommits)
  -> Result<MergeAnalysis, LixError>
```

Its exact sequence is:

1. Convert the supplied base/source/target CommitIds to the current reader
   request shape.
2. Read base→source and base→target diffs. The target diff is empty for
   already-up-to-date/fast-forward base relationships.
3. Remove internal checkpoint and undo/redo marker identities.
4. Classify the outcome as AlreadyUpToDate, FastForward, or MergeCommitted.
5. For a real merge, collect cross-change payload identities, load any missing
   payloads, and call `plan_merge`.
6. Compute public merge statistics from the diff or plan.

The transaction opening-read call graph is:

```text
SessionContext::merge_branch_preview
  -> Transaction::branch_ref_reader_on_opening_read
  -> Transaction::commit_graph_reader_on_opening_read
  -> Transaction::forktree_read_facade()       [same opening read]
  -> Transaction::with_opening_tracked_reader
       -> session::merge::analysis::analyze   [one call]

SessionContext::merge_branch
  -> same branch/ref/graph/facade path
  -> Transaction::with_opening_tracked_reader
       -> session::merge::analysis::analyze   [second call]
```

On b59, `session/merge/branch.rs` has two
`super::analysis::analyze` calls, two callback call sites, and zero
`begin_read` calls. `Transaction::opening_read` is a retained
`SharedStorageAdapterRead<StorageImpl::Read<'static>>`; cloning it shares
the logical snapshot. The separate `tracked_state_reader()` helper is used
by checkpoint and undo/redo cohorts and is not a merge-analysis caller; its
deletion belongs to the whole-module compiler wave, not this narrow oracle.

After analysis, plugin/file metadata remains owned by the existing branch
caller path: `ForkTreeReadFacade` supplies derived plugin-blob conflict
inputs, plugin merge groups, resolution rows, and resolution statistics.
The migration must preserve this handoff and must not move plugin authority
into a merge cache or a second reader.

The source verifier calibrates the current frontier as RED because the
analysis parameter, the `tracked_state.reader(...)` factory, and
`with_opening_tracked_reader` still exist. It separately proves that the
two merge callers do not acquire a second StorageRead.

## Required ForkTree-owned replacement

The smallest future cut is a serving-layer replacement for the physical
three-way read, while the session layer retains public merge semantics:

```text
ForkTreeReadFacade::load_authenticated_merge_inputs(
    base_commit_id,
    target_commit_id,
    source_commit_id,
    filter
) -> Result<ForkTreeMergeInputs, LixError>
```

This is an in-memory value over the one facade/read, not a persisted space,
index, cache, or compatibility object. It must be implemented from the
existing authenticated ForkTree owner primitives:

- commit topology/member records and commit/root validation;
- `state_point`/`state_range` over the selected immutable roots;
- ChangeCatalog/member payload ownership already authenticated by the
  CommitCatalog;
- the existing marker exclusion policy at the semantic caller boundary.

`ForkTreeMergeInputs` may contain sorted base-relative source/target
entries, identity handles, deleted/tombstone state, change IDs, commit IDs,
timestamps, and authenticated snapshot/metadata references needed for
same-final-state comparison. It must not introduce a durable merge record,
tracked-state reader, raw storage getter, fallback scan, or duplicate payload
authority. Existing `MergeAnalysis`/public conversion may remain under
`session/merge`, but its physical input must no longer be a
`TrackedStateStoreReader` or a tracked-state diff/plan type.

The replacement must:

- preserve the supplied merge-base, target-head, and source-head identities;
- derive source/target changes from the same authenticated base;
- classify Added, Modified, Removed, and unchanged deterministically;
- distinguish a live JSON null from a deleted/tombstone row;
- select source-only changes for disjoint merges;
- surface same-identity divergent changes as `SameEntityChanged`;
- treat equal live payload+metadata or equal tombstones as convergent;
- retain plugin registry/file metadata for the existing resolution handoff;
- order identities canonically and produce stable conflict/stat output;
- reject missing/malformed/wrong-kind commit, root, member, or payload objects;
- preserve one caller-owned retained StorageRead for refs, merge base,
  historical inputs, and plugin conflict probing.

A source removal without an authenticated tombstone must remain an error. A
missing payload needed for live/live equality must remain conservative
inequality/conflict, never silent equality. Missing CommitCatalog/root is
corruption, not an empty diff. The existing b59 historical point/scan tests
remain a prerequisite.

## Pure model contract

The model file uses a sorted `BTreeMap` of typed identities and distinguishes:

```text
missing row     = no map entry
JSON null       = live State::Null
ordinary value  = live State::Value
deletion        = State::Tombstone
plugin metadata = a separate retained metadata field
```

It asserts deterministic ordering, source-only picks, disjoint success,
modified/removed classifications, NULL-versus-tombstone distinction,
same-identity conflicts including metadata divergence, and malformed identity
rejection. It is deliberately independent of Lix production modules. It has
not been compiled or run in this package.

## Exact semantic acceptance matrix

The future test-only target must cover, in this order:

1. base/target/source IDs and computed merge base are preserved exactly;
2. empty source delta gives AlreadyUpToDate;
3. base==target gives FastForward and source stats;
4. Added, Modified, Removed, and unchanged rows are classified exactly;
5. source-only/disjoint changes produce a successful plan with canonical order;
6. same-identity value/value, NULL/value, metadata/value, delete/value, and
   value/delete differences produce `SameEntityChanged`;
7. equal live payload+metadata and equal tombstones converge without conflict;
8. plugin registry metadata and derived file-blob conflict rows remain available
   to the existing branch resolver;
9. missing/malformed/wrong-kind CommitCatalog, commit object, root, member,
   change payload, or tombstone fails closed;
10. stale publication/base and branch-head mismatch are rejected by the
    surrounding transaction owner;
11. preview and commit use one retained opening read and no retry/cache/fallback;
12. cold reopen reproduces the same valid results and corruption errors.

The source migration must remove the corresponding
`TrackedStateStoreReader` callback parameter, the merge-specific
`with_opening_tracked_reader` wrapper, and the merge caller’s tracked-state
factory use. The broader `tracked_state_reader()` helper and its
checkpoint/undo callers remain explicitly assigned to their later cohorts.
No compatibility wrapper may preserve the old callback under another name.

## Big-O and ceiling

Current merge analysis performs two base-relative tracked-state diffs, then a
linear two-input merge plan, with optional payload fallback loads:

```text
current: O(diff(base,source) + diff(base,target) + F + S + T)
memory:  O(S + T + retained payloads)
```

The exact replay/tree cost is owned by the old tracked-state implementation;
this report makes no runtime claim.

The proposed ForkTree cut performs authenticated hash-pruned root traversal
for the two sides and one sorted merge, reusing payload ownership already
validated by the coherent view:

```text
proposed: O(U_base→source + U_base→target + S + T)
          plus authenticated path cost for visited tree nodes
memory:   O(S + T + retained payload references)
```

The perfect-elimination ceiling is the duplicated legacy root/replay
materialization and avoidable fallback payload reads. A single shared
three-way traversal could further reduce repeated base visitation, but that is
a separate optimization and must not be smuggled into this correctness cut.
No performance acceptance is claimed here.

## Concrete future commands

These commands are frozen contracts only; no build or runtime was run.

Source/model gate:

```bash
bash packages/lix/tests/forktree_tracked_state_merge_analysis_oracle_b59.sh \
  <candidate-worktree> <candidate-head> <candidate-tree>

rustc --edition=2021 -D warnings \
  packages/lix/tests/forktree_tracked_state_merge_analysis_model_b59.rs \
  -o <isolated-model-binary>
<isolated-model-binary>
```

Memory first, using real existing targets:

```bash
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=1 \
  cargo test -p lix --test semantic_merge -- \
  --nocapture --test-threads=1

CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=1 \
  cargo test -p lix --test integration -- \
  merge --nocapture --test-threads=1
```

The dual-adapter acceptance file must be materialized as this concrete
test-only target in `packages/engine-benchmarks`:

```text
packages/engine-benchmarks/tests/forktree_merge_analysis_acceptance.rs
Cargo target: lix_benchmarks / forktree_merge_analysis_acceptance
required features: storage-benches,slatedb
tests: merge_analysis_rocksdb, merge_analysis_slatedb
```

Then run:

```bash
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks \
  --test forktree_merge_analysis_acceptance \
  --features storage-benches,slatedb -- \
  --nocapture --test-threads=1
```

That target must run RocksDB before SlateDB, with each cell capped at
20 minutes and a stop on the first blocker. The existing dual-adapter
scaffold `packages/engine-benchmarks/benches/tracked_working_diff.rs`
also has exact setup/measure commands for RocksDB and SlateDB and may be
used for non-acceptance evidence:

```bash
cargo bench -p lix_benchmarks --features storage-benches \
  --bench tracked_working_diff -- setup rocksdb <fixture> repeated 1000 100 1
cargo bench -p lix_benchmarks --features storage-benches,slatedb \
  --bench tracked_working_diff -- setup slatedb <fixture> repeated 1000 100 1
```

The existing `corruption_recovery_qualification` target is the reusable
dual-adapter corruption/reopen scaffold:

```bash
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks --test corruption_recovery_qualification \
  --features storage-benches,slatedb -- --nocapture --test-threads=1
```

No command above was executed in this package.

## Frozen result and deletion gate

Exact b59 source verification is expected to return `RESULT=RED`: the
semantic owner/call graph and fail-closed requirements pass, while the
TrackedStateStoreReader callback/factory/wrapper remain. A future green
candidate must retain the semantic source/model contract and make the
merge-specific callback/factory residue zero without adding a wrapper,
fallback, cache, or second authority.

Only these test/report files belong to this package:

```text
packages/lix/tests/forktree_tracked_state_merge_analysis_oracle_b59.sh
packages/lix/tests/forktree_tracked_state_merge_analysis_model_b59.rs
packages/lix/tests/FORKTREE_TRACKED_STATE_MERGE_ANALYSIS_ORACLE_B59.md
```

No production file, PR, or merge is part of this assignment.
