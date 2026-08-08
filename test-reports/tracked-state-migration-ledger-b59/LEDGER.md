# TrackedState checkpoint/history migration ledger

Status: **source/report-only ledger**, bound to the accepted `713 -> b59`
lineage. No production edit, build, benchmark, PR, or merge was performed.

## Immutable bindings

Accepted source lineage:

* `713455a3557907ce705d06f720fcdc4486bddd4a`, tree
  `9c15144678fa952e1f50c5259df1c4dbb0199168`, parent `ab90fc51…`;
  parent-to-head full-index diff
  `61d764ab877b75c7726a2ebea8020177a5bf819eed202abecdb359dc7a517c19`,
  patch `1a3dd2e4ddc1a6e0fbe0dcdc45e001ea0a2a94e5`.
* `b59e1f11a51153e0a787a81f0f25bf104d150aaf`, tree
  `700fd04d21bc40c05425c9fc9e10d65c9e1eda24`, parent exactly 713;
  parent-to-head full-index diff
  `4b2885709ba09034068b321be2fe5f27348d6681b1060133af1df0b7d76bb8d4`,
  patch `63dcb8dcecba8a25dea0ce8be19d26cdac264729`.

Bound oracle artifacts:

* checkpoint/history oracle `25016a99c5356045cdd9f70e928a08b512544ff3`,
  tree `970437dcfb089c4c28f90d57d207324a442065f6`, parent `97a7116d…`;
  parent-to-head full-index diff
  `df04eaf2df9db418b6a83ca65f1c24da00f27eeb405a58f9fe16e718fc8ab7a4`,
  patch `817c65c92ed3f24f1784ebb2d484b10b38672f24`, report SHA
  `3ce63a5cd35578f6485591c113728dcd3626477f50589671fedb0f4925f6da0a`.
* whole-module deletion gate `72f10a4412dbea93c3a266a20a9c2df91d02193c`,
  tree `67c9b631fb701da23f79e2e41d057d027d304e6`, parent `be6ea48c…`;
  parent-to-head full-index diff
  `8e16788c905cbcee032a3c9a8f5e9dd86c5cd94a173d57836bf759ab2359cb0e`,
  patch `a4f718e106c139d7c28851ed57c5ce04d86a7830`.

The deletion gate is an older report-only branch, not an ancestor of b59. This
ledger is the explicit rebinding: its source counts and future commands must
be replayed against b59 before any implementation is accepted.

## Current source ledger

Read-only `git grep` on exact b59 reports these baseline counts under
`packages/lix/src`:

```text
TrackedStateStoreReader                 29
TrackedStateContext::new().reader      11
tracked_state_reader()                 10
with_opening_tracked_reader             3
scan_batch_at_commit                    37
diff_commits(                           52
crate::tracked_state::                 384
```

Counts overlap and include tests/bench support; they are source-frontier
measurements, not compiler-diagnostic counts. The accepted b59 historical fix
does not reduce the reader cohort. It adds the required CommitCatalog owner at
`packages/lix/src/forktree/serving.rs:599-610` and point/batch corruption tests
at `packages/lix/src/forktree/tests.rs:841-946`.

### Caller cohorts and permitted migration

| Cohort | Exact current callers | Permitted next production paths | Deletion condition |
| --- | --- | --- | --- |
| Checkpoint marker/history | `checkpoint.rs`; `session/checkpoint.rs`; `sql2/providers/checkpoint.rs` | `forktree/view.rs` + `serving.rs` typed commit/root/checkpoint API; thin caller plumbing in the three listed files | `TrackedStateStoreReader` signatures and separate graph reader removed; first-parent chronology remains CommitObject-owned |
| SQL entity/change history | `sql2/history_route.rs`; `sql2/providers/change.rs`; `file_history.rs`; `directory_history.rs` | ForkTree authenticated commit/member/state/BlobId reads; preserve parsed file/directory and plugin registry semantics in their providers | all `TrackedStateContext::reader`/historical scans gone; no empty-success on missing owner |
| SQL diff/working diff | `sql2/providers/diff.rs`; `working_diff.rs`; `filesystem_working_diff.rs` | ForkTree root-to-root typed diff plus transaction-local overlay, with existing SQL projection/order | no legacy `diff_commits`, scan fallback, or second view |
| Merge analysis | `session/merge/analysis.rs`; `branch.rs`; `conflicts.rs`; `stats.rs` | ForkTree merge view over the retained opening read; pure conflict/stat semantics may remain under a non-reader owner | no `TrackedStateStoreReader`, `with_opening_tracked_reader`, or recovery-row chronology |
| Transaction/checkpoint/undo | `transaction/context.rs`; `session/undo_redo.rs`; `session/checkpoint.rs` | existing transaction opening read, ForkTree selector/root view, and one prepared publication | remove factories only after all callers migrate; retain stale-head/CAS and undo floor semantics |
| GC/bench/execute dependencies | `gc.rs`; `storage_bench.rs`; `session/execute.rs`; `commit_graph/context.rs` | delete old spaces/readers only after their unique owner is moved to ForkTree; keep GC reachability/selector ownership separate | compiler proves no writer/retention owner still needs the deleted space |

Allowed implementation files are limited to the existing semantic owners:

* `packages/lix/src/forktree/{serving.rs,view.rs,state.rs}` for authenticated
  historical point/range, commit topology/member, typed cell, and one-read
  operations;
* `packages/lix/src/{checkpoint.rs,session/checkpoint.rs,session/undo_redo.rs,
  session/merge/{analysis.rs,branch.rs,conflicts.rs,stats.rs}}` for caller
  lowering while preserving public semantics and publication fences;
* `packages/lix/src/sql2/providers/{checkpoint.rs,diff.rs,working_diff.rs,
  filesystem_working_diff.rs,file_history.rs,directory_history.rs,change.rs}`
  and `sql2/history_route.rs` for provider plumbing only;
* `packages/lix/src/transaction/context.rs` for deleting reader factories and
  retaining the opening read/stale-publication owner;
* `packages/lix/src/tracked_state/{context.rs,diff.rs,diff_id.rs,merge.rs,
  row_materialization.rs}` only to move pure unique semantics or delete the
  reader-only cluster after callers are gone.

No adapter, persisted format, compatibility reader, fallback, cache, new
selector, second chronology authority, or new durable space is allowed.

## Expected compiler/residue reduction

The focused source gate must show, against exact b59:

```text
TrackedStateStoreReader                   29 -> 0
TrackedStateContext::new().reader         11 -> 0
tracked_state_reader()                    10 -> 0
with_opening_tracked_reader                3 -> 0
reader-only modules                         4 -> absent
reader-only tracked_state reexports         4 -> absent
forbidden legacy space symbols             >0 -> 0
```

The broad `scan_batch_at_commit`, `diff_commits`, and
`crate::tracked_state::` counts must be reclassified by the compiler; they
cannot be declared zero while a unique writer or pure semantic helper remains.
No exact cargo error reduction is claimed without a build. The required
compile gate is a before/after `cargo check -p lix --lib --all-features` plus
warnings-denied Clippy on the eventual successor, followed by the two negative
probes. Successful compilation of either forbidden probe is a hard failure.

## Chronology, floor, and rotation invariants

The next owner must preserve all of these facts:

1. CommitObject parent list is the sole chronology authority. Checkpoint
   history follows `parent_commit_ids[0]`; graph readers must not consult
   `CheckpointRecoveryRef`, queue state, or a mutable current marker as
   chronology.
2. The checkpoint/undo floor comes from the authenticated first-parent and
   selector/control facts. Recovery `{recovered_head, checkpoint,
   interval_has_commits}` is retention/reopen evidence only.
3. After 65 checkpoint rotations, including empty rotations, history remains
   newest-first and complete; the current recovery pair describes only the
   latest interval, while older checkpoint roots remain authenticated through
   their proper retention owner.
4. A branch from historical H whose serving checkpoint is C must preserve the
   ordinary bridge semantics: first ordinary commit parents `[H, C]` in that
   order, generation greater than both, merge base C, and no permanent C→H
   parent. A later target head must never be substituted.
5. Undo stops at the checkpoint floor and redo restores the exact commit.
   Missing/malformed/wrong-kind commit, root, selector, marker, or parent/cycle
   data fails closed; valid commit+root+absent key remains authenticated
   absence; NULL, tombstone, and value remain distinct.
6. Stale publication must fail through the existing transaction head/CAS
   precondition. Analysis must not reopen or refresh a second read.

## One-read contract and future dual-adapter gate

Each migrated operation must retain one caller-owned `StorageRead` and one
ForkTree view from selector through commit catalog, topology, checkpoint,
historical state, SQL diff, file/directory history, undo/redo, and publication
validation. No `TrackedStateContext::reader`, separate `CommitGraphReader`,
retry, fallback, or cache may appear in the migrated call graph.

The future test target and commands from oracle 25016 remain a contract only;
they have not been run here. Before runtime qualification, bind an immutable
test source with exact subcases and counters, then run serialized Memory,
RocksDB, and SlateDB cells in that order, each capped at 20 minutes:

```text
cargo test -p lix_benchmarks --test forktree_checkpoint_history_migration \
  --features storage-benches checkpoint_history_migration_memory \
  -- --exact --nocapture --test-threads=1

cargo test -p lix_benchmarks --test forktree_checkpoint_history_migration \
  --features storage-benches checkpoint_history_migration_rocksdb \
  -- --exact --nocapture --test-threads=1

cargo test -p lix_benchmarks --test forktree_checkpoint_history_migration \
  --features storage-benches,slatedb checkpoint_history_migration_slatedb \
  -- --exact --nocapture --test-threads=1
```

Each cell must print read/view identity, commit parents/generation, merge
base, floor, 65-rotation history, state/diff digests, stale-publication result,
corruption-vs-absence outcomes, cold-reopen result, and zero fallback/cache
reads. No runtime or performance claim is made by this ledger.

## Successor poll

The remote ref inventory was checked after b59. No newer immutable
checkpoint/history or whole-module deletion successor was found. The latest
relevant source object remains b59; this ledger is ready to be replayed when a
successor is published.
