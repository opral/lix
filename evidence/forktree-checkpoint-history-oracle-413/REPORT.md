# Checkpoint/history reconstruction migration oracle — 413e08a

Status: TEST/REPORT ONLY, source RED. No production source, adapter, format,
writer, selector, cache, compatibility, or fallback code is changed here. The
oracle is anchored to the corrected SQL reader frontier 413e08a and the
historical fail-closed prerequisite 97a7116.

## Immutable provenance

| object | value |
|---|---|
| SQL frontier head | `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d` |
| SQL frontier tree | `820fe560da3bbd2b00b788b0b1759c409048cd6e` |
| SQL frontier parent | `11442c1e0023e20307a7231d88cd557bc704fd13` |
| SQL frontier e166..head diff SHA-256 | `70bc6bc03524855be515c9d1a5d0c75c77ebd159fbd44d5f646483ce14460329` |
| prerequisite head | `97a7116d00206954b581cf43937cc5db6c23f30b` |
| prerequisite tree | `457a3919903169ca1edd2fe81df8b81e70b06d37` |
| prerequisite parent | `448624a557bca2c341f4a1820b79222a5691613a` |
| prerequisite parent..head diff SHA-256 | `08fee7a84860b27836468f63eff9f6c000538c08947820a26bfbba1e54328cdf` |
| prerequisite patch ID | `497130c0f7744d7e42a7cb0866bb044f8c9f1209` |
| inherited e166 source | `e1666edd0b4d814a88d985086ecc5a477b5d32e6` |

This report-only successor is a child of 97a and leaves both 413 and 97a
unchanged. The inherited prerequisite verifier is run first and remains RED
for missing CommitCatalog/root versus valid absence.

## Source-backed RED map

On 413, checkpoint/history reconstruction is not yet owned by ForkTree:

1. `packages/lix/src/checkpoint.rs:95-126`
   `latest_checkpoint_at_head` accepts `&mut TrackedStateStoreReader` and
   reads the checkpoint marker through `load_projected_batch_at_commit`.
2. `packages/lix/src/checkpoint.rs:129-187` combines that legacy marker point
   read with a separate `CommitGraphReader`; `:359-423`
   `checkpoint_history_from_head` walks `parent_commit_ids.first()` and loads
   graph records directly.
3. `packages/lix/src/session/checkpoint.rs:45-175` creates checkpoint state,
   invokes `transaction.tracked_state_reader()` for latest-marker and diff
   work, and separately invokes `commit_graph_reader()` for fallback history.
4. `packages/lix/src/sql2/providers/checkpoint.rs:148-169` constructs a
   `TrackedStateContext::new().reader(store)` and passes it to the checkpoint
   history helper. `working_diff.rs` and `filesystem_working_diff.rs` retain
   the same latest-checkpoint/legacy historical path.
5. `packages/lix/src/session/undo_redo.rs:179-438` reads checkpoint and
   operation markers, deltas, and commit nodes through transaction-scoped
   tracked/graph readers. The semantic undo/redo state machine is unique and
   must remain; only its historical read owner is in scope for migration.
6. `packages/lix/src/session/merge/branch.rs:166-298` obtains merge base from
   the commit graph, while its state comparisons use historical rows. The
   branch-from-pre-checkpoint oracle must prove that graph chronology—not a
   recovery row—selects the merge base.

The recovery/retention facts are distinct. `gc.rs:168-173` defines
`CheckpointRecoveryRef` as `{recovered_head, checkpoint, interval flag}`;
`gc.rs:2052-2080` adds both recovery endpoints and authenticated queue
checkpoint roots to the GC chronology root set. That row is a retention and
recovery proof, never a merge/history chronology edge. `CheckpointGcState`
(`gc.rs:181-203`) is repository-wide collection debt and must not become a
second history floor.

The inherited 97a prerequisite remains RED because
`forktree/serving.rs::load_state_value_at_commit` returns `Ok(None)` for a
missing CommitCatalog entry, while a valid absent key also returns `None`.
Missing/wrong-kind/malformed root objects otherwise fail through typed object
and decode validation. The required correction must precede this migration
oracle's point/scan acceptance.

## Required one-view target

Each checkpoint/history/undo/redo operation must use one retained
`StorageRead` and one ForkTree `CoherentView` (or an equivalent typed
ForkTree facade bound to that read). The facade must own:

* authenticated selector, branch control, commit catalog, commit object, and
  state-root lookup;
* latest checkpoint marker lookup and first-parent chronology;
* checkpoint history reconstruction with cycle/missing/malformed/wrong-kind
  fail-closed behavior;
* historical point/scan cell semantics, including valid absence, null,
  tombstone, and value;
* the graph bridge needed for branch-from-pre-checkpoint merge base.

No caller may construct `TrackedStateStoreReader`, `TrackedStateContext`, a
second `CommitGraphReader`, or a legacy fallback/cache for these duties. The
recovery ref may contribute authenticated retention roots only; merge/history
readers must not consult it as chronology.

## Exact deletion residue after the migration

The following residue must disappear in dependency order after ForkTree
callers are moved; no adapter or wrapper should preserve it:

* `checkpoint.rs`: remove the `TrackedStateStoreReader` import and generic
  parameters from `latest_checkpoint_at_head`,
  `latest_checkpoint_for_branch`, and `checkpoint_history_for_branch`; retain
  only the semantic checkpoint records/marker model under ForkTree ownership.
* `session/checkpoint.rs`: delete the legacy reader constructions at lines
  73 and 103 and the graph/legacy fallback branch; retain publication,
  checkpoint floor, and GC-debt semantics.
* `sql2/providers/checkpoint.rs`: delete the
  `TrackedStateContext::new().reader(store)` path and route the provider to the
  one view; `working_diff.rs` and `filesystem_working_diff.rs` must lose their
  checkpoint/history legacy reader helpers.
* `sql2/providers/file_history.rs` and `directory_history.rs`: delete their
  `TrackedStateStoreReader` historical scans after the ForkTree file/history
  facade is qualified; preserve parsed-file and directory semantics.
* `session/undo_redo.rs`: delete only the old marker/delta historical reads
  (`semantic_state_at`, `semantic_state_for_record`, `operation_marker_at`,
  `load_commit_delta`, and their tracked-reader calls) after their semantic
  state machine is moved to the ForkTree view.
* `transaction/context.rs`: delete `tracked_state_reader` and
  `with_opening_tracked_reader` only after all merge, checkpoint, working-diff,
  and undo/redo callers are migrated. Do not leave a compatibility factory.
* `tracked_state/context.rs`: delete the historical reader methods and then
  the reader/module only when its remaining unique writer or semantic owner
  has already moved; this oracle does not authorize a premature broad delete.

## Source verdict

The exact source verifier prints RED because 413 still contains every legacy
checkpoint/history entry point above. The pure model and future adapter
commands are frozen in `model.rs`, `CHECKLIST.md`, and
`FUTURE_COMMANDS.md`. No runtime, correctness, or performance approval is
claimed for 413.
