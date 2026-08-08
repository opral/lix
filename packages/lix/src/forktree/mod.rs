//! Authenticated persistent physical owner for tracked repository state.
//!
//! This root module is the only boundary other engine owners may import. The
//! implementation children stay private so object encodings, tree mechanics,
//! selector fencing, and reachability cannot become competing authorities.

mod blob;
mod codec;
mod gc_index;
mod model;
mod object;
mod publication;
mod reachability;
mod serving;
mod state;
mod tree;
mod view;

pub(crate) use blob::{UploadBindingRef, prepare_upload_completion};
pub(crate) use model::{
    BlobChunkRefV1, BlobChunkV1, BlobManifestV1, BranchSelectorV1, BranchSnapshotV1,
    CanonicalBranchId, CanonicalUploadId, ChangeCatalogEntry, ChangeCatalogOwner, ChangeId,
    ChangeObjectV1, CommitCatalogEntry, CommitId, CommitObjectV1, GlobalSelectorV1,
    RepositoryRootV1, SnapshotRole, SnapshotSelectorId, SnapshotSelectorV1, SnapshotTargetV1,
    UploadPartV1, UploadProgressV1, UploadSelectorV1,
};
pub(crate) use object::ObjectId;
pub(crate) use publication::{BranchStateTransition, PreparedPublication, SelectorExpectation};
pub(crate) use reachability::{GcBudget, GcStepStatus, abort_corrupt_gc, advance_gc};
pub(crate) use serving::{
    CatalogPage, CommitTopology, StateSource, StateTreeMutation, VisibleStateRow, edit_state_tree,
    load_branch_head, load_change, load_change_records, load_commit, load_commit_member_records,
    load_commit_records, load_commit_topologies, page_changes, page_commits,
    put_change_catalog_entries, put_commit_catalog_entries, scan_branch_heads, scan_change_records,
    scan_commit_records, scan_commit_topologies, state_point, state_range,
};
pub(crate) use state::{
    StateCell, StateCellRef, StateKey, StateKeyRef, StateValue, StateValueRef, UntrackedValueRef,
    decode_state_key, decode_state_value, encode_state_key, encode_state_prefix,
    encode_state_value,
};
pub(crate) use tree::{
    RECEIPT_TREE_FANOUT, RECEIPT_TREE_LEAF_ENTRIES, ReceiptTreeEdit, ReceiptTreeRoot,
};
pub(crate) use view::{CoherentView, open_coherent_view, open_coherent_view_on_read};

// Stage 1 is deliberately unwired. This zero-runtime compile contract keeps
// the root facade type-checked in non-test builds without connecting a reader
// or writer before the independently reviewed compiler wave.
const _: () = {
    fn facade_contract<R, S>()
    where
        R: crate::storage_adapter::StorageAdapterRead,
        S: crate::storage::Storage,
    {
        let _: Option<(
            BlobChunkRefV1,
            BlobChunkV1,
            BlobManifestV1,
            BranchSelectorV1,
            BranchSnapshotV1,
            CanonicalBranchId,
            CanonicalUploadId,
            ChangeCatalogEntry,
            ChangeCatalogOwner,
            ChangeId,
            ChangeObjectV1,
            CommitCatalogEntry,
            CommitId,
            CommitObjectV1,
            GlobalSelectorV1,
            RepositoryRootV1,
            SnapshotRole,
            SnapshotSelectorId,
            SnapshotSelectorV1,
            SnapshotTargetV1,
            UploadPartV1,
            UploadProgressV1,
            UploadSelectorV1,
        )> = None;
        let _: Option<(
            ObjectId,
            BranchStateTransition,
            PreparedPublication,
            SelectorExpectation,
            CatalogPage<(CommitId, CommitObjectV1)>,
            CommitTopology,
            StateSource,
            StateTreeMutation,
            VisibleStateRow,
            StateCell,
            StateCellRef<'static>,
            StateKey,
            StateKeyRef<'static>,
            StateValue,
            StateValueRef<'static>,
            UntrackedValueRef<'static>,
            ReceiptTreeEdit,
            ReceiptTreeRoot,
            CoherentView<R>,
            UploadBindingRef<'static>,
        )> = None;
        let _ = RECEIPT_TREE_FANOUT;
        let _ = RECEIPT_TREE_LEAF_ENTRIES;
        let _ = prepare_upload_completion::<R>;
        let _ = advance_gc::<S>;
        let _ = abort_corrupt_gc::<S>;
        let _ = GcBudget::default;
        let _: Option<GcStepStatus> = None;
        let _ = edit_state_tree::<R>;
        let _ = load_change::<R>;
        let _ = load_branch_head::<R>;
        let _ = load_commit::<R>;
        let _ = load_commit_records::<R>;
        let _ = load_commit_topologies::<R>;
        let _ = load_commit_member_records::<R>;
        let _ = load_change_records::<R>;
        let _ = scan_commit_records::<R>;
        let _ = scan_commit_topologies::<R>;
        let _ = scan_change_records::<R>;
        let _ = page_changes::<R>;
        let _ = page_commits::<R>;
        let _ = put_change_catalog_entries::<R>;
        let _ = put_commit_catalog_entries::<R>;
        let _ = state_point::<R>;
        let _ = state_range::<R>;
        let _ = scan_branch_heads::<R>;
        let _ = decode_state_key;
        let _ = decode_state_value;
        let _ = encode_state_key;
        let _ = encode_state_prefix;
        let _ = encode_state_value;
        let _ = open_coherent_view::<S>;
        let _ = open_coherent_view_on_read::<R>;
        let _ = PreparedPublication::from_branch_view::<R>;
        let _ = PreparedPublication::from_global_epoch::<R>;
        let _ = PreparedPublication::publish_new_upload;
        let _ = PreparedPublication::stage_blob_chunk;
        let _ = PreparedPublication::stage_upload_part;
        let _ = PreparedPublication::stage_upload_progress;
        let _ = PreparedPublication::stage_receipt_tree_edit;
        let _ = PreparedPublication::abort_upload;
        let _ = PreparedPublication::publish_current_snapshot_pin::<R>;
        let _ = PreparedPublication::release_snapshot_pin_with_catalog_retirement::<R>;
        let _ = PreparedPublication::publish_state_transition::<R>;
        let _ = PreparedPublication::publish_completed_upload::<R>;
        let _ = PreparedPublication::put_untracked_row;
        let _ = PreparedPublication::delete_untracked_row;
        let _ = PreparedPublication::publish_branch_retirement::<R>;
        let _ = PreparedPublication::commit::<S>;
        let _ = PreparedPublication::into_storage_plan;
        let _ = StateTreeMutation::insert;
        let _ = StateTreeMutation::update;
        let _ = StateTreeMutation::remove;
        let _ = serving::StateTreeEdit::entry_count;
        let _ = serving::StateTreeEdit::copied_nodes;
        let _ = serving::CatalogTreeEdit::entry_count;
        let _ = serving::CatalogTreeEdit::copied_nodes;
        let _ = serving::retire_commit_catalog_entries::<R>;
        let _ = serving::retire_change_catalog_entries::<R>;
        let _ = StateCellRef::Value("");
        let _ = StateCellRef::Null;
        let _ = StateCellRef::Tombstone;
        let _ = tree::empty_receipt_tree;
        let _ = tree::ImmutableObjectSet::extend;
        let _ = tree::build_commit_catalog;
        let _ = tree::build_change_catalog;
        let _ = tree::build_state_tree;
        let _ = tree::build_retention_tree;
        let _ = |build: tree::TreeBuild| {
            let _ = build.root;
            let _ = build.objects;
        };
        let _ = |edit: ReceiptTreeEdit| {
            let _ = edit.root;
            let _ = edit.copied_nodes;
            let _ = edit.inserted;
        };
        let _ = |root: ReceiptTreeRoot, id: ObjectId, part: &UploadPartV1| {
            tree::insert_receipt_part(root, id, part, |_| {
                Err(crate::storage::StorageError::InvalidCursor)
            })
        };
        let _ = |root: ObjectId, kind: &'static str, key: &[u8]| {
            tree::lookup(root, kind, key, |_| {
                Err(crate::storage::StorageError::InvalidCursor)
            })
        };
        let _ = |root: ObjectId, kind: &'static str| {
            tree::scan_all(root, kind, |_| {
                Err(crate::storage::StorageError::InvalidCursor)
            })
        };
        let _ = |key: CommitId, entry: CommitCatalogEntry| {
            tree::validate_commit_catalog_back_edge(key, entry, |_| {
                Err(crate::storage::StorageError::InvalidCursor)
            })
        };
        let _ = |key: ChangeId, entry: ChangeCatalogEntry| {
            tree::validate_change_catalog_back_edge(key, entry, |_| {
                Err(crate::storage::StorageError::InvalidCursor)
            })
        };
        let _ = |root: ReceiptTreeRoot, upload: &CanonicalUploadId| {
            tree::validate_receipt_tree(root, upload, |_| {
                Err(crate::storage::StorageError::InvalidCursor)
            })
        };
        let _ = |progress: &UploadProgressV1| {
            tree::validate_upload_progress_tree(progress, |_| {
                Err(crate::storage::StorageError::InvalidCursor)
            })
        };
        let _ = |selector: &UploadSelectorV1| {
            tree::validate_upload_selector_progress(selector, |_| {
                Err(crate::storage::StorageError::InvalidCursor)
            })
        };
        let _ = |snapshot: &BranchSnapshotV1| {
            tree::validate_branch_snapshot_ref_edge(snapshot, |_| {
                Err(crate::storage::StorageError::InvalidCursor)
            })
        };
    }

    let _: fn() = facade_contract::<
        crate::storage_adapter::StorageAdapterReadScope<crate::storage::MemoryRead>,
        crate::storage::Memory,
    >;
};

#[cfg(test)]
mod tests;
