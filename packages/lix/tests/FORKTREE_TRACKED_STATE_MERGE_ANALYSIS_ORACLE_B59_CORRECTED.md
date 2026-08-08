# Corrected TrackedState merge-analysis oracle, exact b59

Status: frozen TEST/REPORT-ONLY artifact. No Lix production source is changed,
built, or executed by this commit.

## Immutable anchor

- base: b59e1f11a51153e0a787a81f0f25bf104d150aaf
- base tree: 700fd04d21bc40c05425c9fc9e10d65c9e1eda24
- parent of b59: 713455a3557907ce705d06f720fcdc4486bddd4a
- source ref used for this package: refs/heads/codex/forktree-stage2-historical-fail-closed-713

Only these three test/report paths are added:

- packages/lix/tests/FORKTREE_TRACKED_STATE_MERGE_ANALYSIS_ORACLE_B59_CORRECTED.md
- packages/lix/tests/forktree_tracked_state_merge_analysis_model_b59_corrected.rs
- packages/lix/tests/forktree_tracked_state_merge_analysis_oracle_b59_corrected.sh

The final package commit identity, tree, diff, patch ID, and file hashes are
reported in the immutable handoff after commit. They are intentionally not
embedded self-referentially in this document.

## Corrections over the 06fb oracle

The pure model now carries four explicit CommitRef values:

- merge-base identity and generation;
- selected base identity and generation;
- source-head identity and generation;
- target-head identity and generation.

It authenticates a typed object graph before producing merge inputs:

Commit -> Root + CommitCatalog + PluginRegistry -> Member + Payload +
FileOwner. Every lookup verifies object kind, object identity, commit binding,
root/catalog binding, and exact generation. Missing, wrong-kind, malformed,
identity-substituted, and mismatched-generation objects return typed errors.

The semantic model has explicit:

- Added, Updated, Deleted, and unchanged classification;
- live JSON Null distinct from authenticated Tombstone;
- missing row on a deletion path rejected as MissingTombstone;
- same-entity divergent value, metadata, file-owner, and deletion conflicts;
- source-only/disjoint picks with sorted deterministic identity order;
- convergent equal live payload digests, metadata, and tombstones;
- authenticated PluginRegistry and FileOwner handoff for source/target roots;
- payload object identity checks while equality uses authenticated payload digest.

The model's RetainedStorageRead is created by the caller and borrowed by one
MergeOperation. It has no begin_read, refresh, clone, extraction, retry, or
detached-reader method. Every topology, root, catalog, member, payload,
registry, and file-owner event records the same read owner ID; the valid
fixture asserts that the trace has one owner.

## Corrected source verifier

The shell verifier is source-only. It:

1. binds candidate head/tree;
2. verifies the corrected model and report artifacts exist;
3. checks the typed identity/object/error/read-trace model contract;
4. checks the actual merge call graph and opening-read/facade handoff;
5. scans the full merge production closure:
   session/merge, transaction opening-read context, ForkTree serving/auth,
   and plugin/file historical resolution;
6. reports every old callback, tracked-state factory, raw storage read,
   BranchHead/BranchRef authority, cache, fallback, compatibility, retry, or
   renamed merge-reader residue;
7. requires merge-specific TrackedStateStoreReader,
   tracked_state.reader(...), and with_opening_tracked_reader to be absent.

At exact b59 the verifier is expected to return RED because the old merge
callback/factory and wrapper are still present. That RED is the intended
baseline calibration, not a production acceptance result. A future corrected
production candidate is GREEN only after the model/source checks pass and the
full closure has zero forbidden merge-reader residue.

The verifier does not compile or run the model. This assignment freezes the
oracle only; future model and adapter runs are separate gates.

## Future correctness sequence

Use separate target directories and cap every cell at 20 minutes:

~~~sh
timeout 20m bash packages/lix/tests/forktree_tracked_state_merge_analysis_oracle_b59_corrected.sh \
  <candidate-worktree> <candidate-head> <candidate-tree>

timeout 20m rustc --edition=2021 -D warnings \
  packages/lix/tests/forktree_tracked_state_merge_analysis_model_b59_corrected.rs \
  -o <isolated-model-binary>
timeout 20m <isolated-model-binary>
~~~

Then run the smallest real Memory gate, followed by identical RocksDB and
SlateDB gates. The test-only adapter must use the candidate's public
ForkTreeReadFacade and existing transaction opening read; it must not expose
raw spaces or ObjectIds:

~~~sh
timeout 20m env CARGO_TARGET_DIR=<memory-target> CARGO_BUILD_JOBS=1 \
  cargo test -p lix --test forktree_merge_analysis_acceptance -- \
  --nocapture --test-threads=1 memory

timeout 20m env CARGO_TARGET_DIR=<rocks-target> CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks --test forktree_merge_analysis_acceptance \
  --features storage-benches -- --nocapture --test-threads=1 rocksdb

timeout 20m env CARGO_TARGET_DIR=<slate-target> CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks --test forktree_merge_analysis_acceptance \
  --features storage-benches,slatedb -- --nocapture --test-threads=1 slatedb
~~~

Each adapter gate must cover valid merge, disjoint success, deterministic
ordering, all identity/generation bindings, NULL/tombstone, same-entity
conflicts, plugin/file-owner handoff, missing/malformed/wrong-kind
CommitCatalog/root/member/payload/registry/owner, identity substitution,
cold reopen, and corruption fail-closed. It must record result/error digests,
one-read trace identity, and backend reads/bytes/writes. No runtime or
performance claim is part of this package.

## Frozen scope boundary

This oracle does not delete TrackedState production code. Its only future
deletion assertion is the merge-specific callback/factory/wrapper closure.
The broader checkpoint/undo/redo tracked-state reader remains outside this
narrow package until its own compiler wave. A candidate that preserves the old
merge path under a renamed wrapper, cache, fallback, retry, or alternate
authority is rejected.
