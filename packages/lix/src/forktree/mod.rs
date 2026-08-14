//! Authenticated persistent physical owner for tracked repository state.
//!
//! This root module is the only boundary other engine owners may import. The
//! implementation children stay private so object encodings, tree mechanics,
//! selector fencing, and reachability cannot become competing authorities.

mod blob;
mod bootstrap;
mod codec;
mod current_pack;
mod gc_index;
mod merkle;
mod model;
mod object;
mod publication;
mod reachability;
mod serving;
mod state;
mod tree;
mod view;

pub(crate) use blob::{
    AuthenticatedBlobReader, AuthenticatedBlobStateKey, PreparedUploadPart, UploadBindingRef,
    blob_reader_on_read, prepare_upload_completion, prepare_upload_part,
};
pub(crate) use bootstrap::initialize_empty_repository;
pub(crate) use current_pack::encode_current_state_packs;
pub(crate) use merkle::canonical_blob_id_for_content;
pub(crate) use model::{
    BLOB_MERKLE_CHUNK_BYTES, BlobChunkRefV1, BlobChunkV1, BlobManifestV1, BranchSelectorV1,
    BranchSnapshotV1, CanonicalBranchId, CanonicalUploadId, ChangeCatalogEntry, ChangeCatalogOwner,
    ChangeId, ChangeObjectV1, CheckpointCursorV1, CommitCatalogEntry, CommitChangePageV3, CommitId,
    CommitMemberV3, CommitObjectV1, GlobalSelectorV1, RepositoryRootV1, SnapshotRole,
    SnapshotSelectorId, SnapshotSelectorV1, SnapshotTargetV1, UploadPartV1, UploadProgressV1,
    UploadSelectorV1, snapshot_selector_key,
};
pub(crate) use object::ObjectId;
pub(crate) use publication::{
    BranchStateTransition, OrderedBranchHistoryTransition, PreparedPublication,
    SelectorExpectation, introduced_checkpoint_marker,
};
pub(crate) use reachability::{GcBudget, GcStepStatus, advance_gc};
pub(crate) use serving::{
    CommitTopology, CommitTopologyReader, HistoricalMemberSelection, SelectedHistoricalMember,
    StateMutationAudit, StateSource, StateTreeMutation, VisibleStateRow, edit_state_tree,
    load_branch_heads_with_metadata, load_change_records, load_commit, load_commit_member_records,
    load_commit_records, load_commit_summary, put_change_catalog_entries,
    put_commit_catalog_entries, scan_change_records, select_historical_commit_members, state_point,
    state_points, state_points_on_read, state_range,
};
pub(crate) use state::{
    HistoricalStateRow, NativeRowCell, StateCell, StateCellRef, StateKey, StateKeyRef, StateValue,
    StateValueRef,
    decode_state_key, decode_state_value, encode_state_entity_prefix,
    encode_state_entity_prefix_bounds, encode_state_key, encode_state_value,
    exclusive_prefix_upper_bound,
};
pub(crate) use tree::diff_roots;
pub(crate) use tree::{
    RECEIPT_TREE_FANOUT, RECEIPT_TREE_LEAF_ENTRIES, ReceiptTreeEdit, ReceiptTreeRoot,
};
pub(crate) use view::{
    AuthenticatedHistoricalStateView, CoherentView, ForkTreeReadFacade, SELECTOR_SPACE,
    load_object_bytes, open_coherent_view, open_coherent_view_on_read,
};

#[cfg(test)]
mod tests;
