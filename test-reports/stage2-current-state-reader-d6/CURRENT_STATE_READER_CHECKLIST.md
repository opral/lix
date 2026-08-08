# Current-state reader closure: independent acceptance checklist

Status: frozen source contract rebased from immutable 1f742 to approved d6; oracle semantics unchanged.

## Exact baseline

- Base/head: d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768
- Tree: 641654079f60fcd1c9ff9ccbbd06d3edcabe4096
- Parent: 1f742a382c755399b8a49ab536c4f6dc55fffdd8
- Review worktree: /root/repos/lix-stage2-reader-acceptance-d6
- This contract accepts only a clean immutable successor descended directly
  from that head, or a clearly identified single parent successor.

## Permitted successor paths

The authorized current-state reader closure may change only these production
consumer/facade paths:

- packages/lix/src/live_state/context.rs
- packages/lix/src/live_state/derived.rs
- packages/lix/src/live_state/reader.rs
- packages/lix/src/live_state/types.rs
- packages/lix/src/live_state/visibility.rs
- packages/lix/src/tracked_state/context.rs
- packages/lix/src/tracked_state/diff.rs
- packages/lix/src/tracked_state/row_materialization.rs
- packages/lix/src/tracked_state/types.rs

Tests and report-only artifacts may be added beside those owners. The existing
ForkTree owner is a read-only dependency for this slice:

- packages/lix/src/forktree/view.rs:
  CoherentView, open_coherent_view, open_coherent_view_on_read
- packages/lix/src/forktree/serving.rs:
  state_point, state_point_on_read, state_range, load_commit_records,
  load_change_records, select_historical_commit_member
- packages/lix/src/forktree/state.rs:
  StateKey, StateValue, StateCell, typed codecs
- packages/lix/src/forktree/tree.rs:
  authenticated lookup/range traversal and root validation
- packages/lix/src/forktree/model.rs:
  RepositoryRootV1, BranchSnapshotV1, selector/root model

ForkTree owner changes are outside this reader-only slice unless a successor
reports a narrowly necessary read-only signature correction. No selector,
tree, object, writer, GC, or persisted-format change is permitted here.

The following production paths are out of scope and must remain byte-identical
to 1f742 in the reader successor:

- packages/lix/src/branch/
- packages/lix/src/forktree/publication.rs
- packages/lix/src/forktree/reachability.rs
- packages/lix/src/binary_cas/
- packages/lix/src/changelog/
- packages/lix/src/gc.rs
- packages/lix/src/init.rs
- packages/lix/src/storage/
- packages/lix/src/transaction/
- packages/lix/src/sql2/
- packages/lix/src/session/
- packages/lix/src/filesystem/
- packages/lix/src/plugin/

## One-view and one-read contract

- [ ] Reader entry obtains exactly one retained CoherentView/StorageRead.
- [ ] Global selector, branch selector, repository root, branch snapshot,
      global state root, local state root, historical-global root, and semantic
      head are authenticated from that view before any row output.
- [ ] Every point, ordered-range, and historical-root read reuses that retained
      read; no helper calls begin_read, refreshes selectors, or creates a
      detached root/read pair.
- [ ] No caller-supplied root, branch ID, selector generation, or row owner can
      replace the authenticated view roots.
- [ ] A writer/GC commit between view acquisition and read completion does not
      change the result of the retained view.
- [ ] A second distinct CoherentView/read cannot be paired with the first
      view's roots, cursor, cache, or state reader.

## Global/branch overlay semantics

- [ ] Local branch state wins for an identical encoded key.
- [ ] A local value shadows the global value and reports source Branch.
- [ ] A local tombstone hides a global row when tombstones are excluded.
- [ ] include_tombstones=true returns the local tombstone in key order.
- [ ] A global tombstone is corruption, never a visible overlay row.
- [ ] A global NULL is a visible value, distinct from absence.
- [ ] A local NULL is a visible value, distinct from tombstone and absence.
- [ ] Missing local then missing global returns absence.
- [ ] Range lower/upper bounds, limit, and strict continuation preserve raw
      key ordering; duplicate local/global keys appear once.
- [ ] Point and range APIs return the same state cell/source for equivalent
      keys.
- [ ] Key/value codecs reject malformed, truncated, trailing, wrong-magic,
      wrong-kind, invalid-entity-key, and invalid-timestamp data.

## Historical diff identity

- [ ] Historical state roots are obtained from authenticated Commit objects
      through CommitCatalog, never from a caller-provided root or old manifest.
- [ ] A historical diff is keyed by the complete state identity:
      schema_key, optional file_id, and ordered EntityPk.
- [ ] Diff rows preserve source ChangeId, owning CommitId, created_at, and
      updated_at from the authenticated state row.
- [ ] Same entity under different file IDs remains distinct.
- [ ] Added, removed, modified, NULL, and tombstone rows have deterministic
      ordering and exact identity.
- [ ] A row whose commit/change identity disagrees with the authenticated
      historical commit/member authority fails closed before materialization.
- [ ] Missing, malformed, substituted, wrong-kind, non-decreasing, cyclic, or
      cross-branch historical roots fail closed.
- [ ] Diff fallback is only an explicitly authenticated existing ForkTree
      historical path; no old tracked-state manifest, delta, replacement-part,
      or changelog scan is used as a silent fallback.

## Corruption and fail-closed matrix

Each case must return an error, perform no write, and leave selectors/epochs
and any receipt or progress markers byte-identical:

- [ ] Missing global selector.
- [ ] Missing branch selector.
- [ ] Selector key/value branch-ID mismatch.
- [ ] Selector references missing or wrong-domain repository root.
- [ ] Repository root references missing/wrong-kind state root.
- [ ] Branch snapshot references missing/wrong-kind local or historical root.
- [ ] Semantic head roots disagree with branch snapshot roots.
- [ ] Truncated/malformed state internal or leaf node.
- [ ] State node has duplicate/out-of-order keys or invalid range summary.
- [ ] State row key does not decode to the requested identity.
- [ ] State row has invalid NULL/tombstone/value encoding.
- [ ] Global state contains a tombstone.
- [ ] Historical CommitCatalog/ChangeCatalog entry is missing or remapped.
- [ ] Historical commit/member source has wrong object/domain/ordinal/generation.
- [ ] Different branch root or distinct read is injected into a valid view.

## Zero-write and deletion-residue rules

- [ ] Reader code contains no StorageWriteSet, write, delete, selector update,
      epoch rotation, retry publication, or GC repair.
- [ ] Reader tests compare a pre/post selector, repository-root, branch-root,
      epoch, receipt, and progress digest and require equality on every error.
- [ ] No old tracked/live/branch-control implementation is re-exported to
      satisfy a caller.
- [ ] No compatibility decoder, migration, dual reader, mirror writer, cache
      authority, reverse index, or persisted summary is introduced.
- [ ] Remaining unsupported callers fail before writes rather than being
      silently routed through a legacy path.

## Source-forbidden symbols and spaces

The following must not occur in successor production code except in explicit
negative tests or this report:

- branch/control.rs
- BranchHeadControl
- BranchHeadControlContext
- BranchHeadTrackedReachability
- stage_branch_head_control
- branch_head_control_precondition
- untracked_lifecycle_generation
- live_state/tracked_head
- TrackedHeadContext
- CurrentStateDeltaRef
- CertifiedCurrentStatePredecessor
- TrackedWorkingDiff
- TrackedWorkingDiffEpoch
- WorkingDiffIndexCoverage
- PackedIdentityMembership
- EntityColumnarOverlayRow
- columnar_row_group
- tracked_state/codec.rs
- tracked_state/storage.rs
- tracked_state/tree.rs
- tracked_state/current_state_envelope.rs
- tracked_state/current_state_data_part.rs
- tracked_state/scoped_range.rs
- tracked_state/scoped_current_state.rs
- tracked_state/mutation_directory.rs
- tracked_state/replacement_part.rs
- tracked_state/commit_root_rebuild.rs
- load_commit_state_manifest
- load_commit_state_manifests
- load_change_record_by_id
- scan_change_records_from_commit_deltas
- load_commit_delta_replay_metadata
- stage_current_state_scoped_ranges_from_published_parent
- stage_certified_commit_state_manifest_with_handle
- MUTATION_DIRECTORY_NODE_SPACE
- SCOPED_RANGE_NODE_SPACE
- CURRENT_STATE_DATA_PART_SPACE
- CURRENT_STATE_DATA_PART_REFS_SPACE
- TRACKED_STATE_TREE_CHUNK_SPACE
- CHANGE_SPACE
- COMMIT_SPACE
- COMMIT_CHANGE_ID_SPACE
- StorageSpace::mutable

The canonical ForkTree selector space and untracked-row space are not
forbidden; they remain distinct authenticated selector/current-serving owners.
Tracked rows must not be written into the mutable untracked-row space.

## Required decision

APPROVE only if every applicable checkbox passes on one immutable head and the
reader source delta is limited to the permitted paths. BLOCK on any forbidden
owner resurrection, second read/selector authority, hidden fallback, failed
overlay/tombstone semantics, missing historical identity validation, or any
write/epoch mutation from a read or corruption path.
