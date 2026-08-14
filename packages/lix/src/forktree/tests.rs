use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use bytes::Bytes;

use crate::LixError;
use crate::commit_graph::CommitGraphStoreReader;
use crate::common::LixTimestamp;
use crate::row_pk::RowPk;
use crate::storage::{
    BeginScanOptions, CommitResult, CoreProjection, GetManyRequest, GetManyResult, GetOptions, Key,
    KeyRange, Memory, MemoryRead, MemoryWrite, ProjectedValue, PutBatch, ReadOptions, ScanCursor,
    Storage, StorageError, StorageRead, StorageWrite, WriteOptions,
};
use crate::storage_adapter::{
    SharedStorageAdapterRead, StorageAdapter, StorageAdapterRead, StorageAdapterReadScope,
    StorageReadOptions, StorageWriteSet, StorageWriteSetError,
};

use super::merkle::{build_blob_merkle_tree, single_leaf_manifest_for_test};
use super::model::{
    GcProgressSelectorV2, GcProgressV2, branch_selector_key, gc_progress_selector_key,
    global_selector_key, snapshot_selector_key, upload_binding_digest, upload_selector_key,
};
use super::object::{OBJECT_SPACE, ObjectDomain, encode_id, encode_object};
use super::serving::{retire_change_catalog_entries, retire_commit_catalog_entries};
use super::tree::{
    ImmutableObjectSet, build_change_catalog, build_commit_catalog, build_state_tree, diff_roots,
    empty_receipt_tree, insert_receipt_part, lookup, scan_all, validate_branch_snapshot_ref_edge,
    validate_change_catalog_back_edge, validate_commit_catalog_back_edge, validate_receipt_tree,
    validate_upload_progress_tree, validate_upload_selector_progress,
};
use super::view::SELECTOR_SPACE;
use super::{
    BLOB_MERKLE_CHUNK_BYTES, BlobChunkRefV1, BlobChunkV1, BlobManifestV1, BranchSelectorV1,
    BranchSnapshotV1, BranchStateTransition, CanonicalBranchId, CanonicalUploadId,
    ChangeCatalogEntry, ChangeCatalogOwner, ChangeId, ChangeObjectV1, CheckpointCursorV1,
    CoherentView, CommitCatalogEntry, CommitChangePageV3, CommitId, CommitMemberV3, CommitObjectV1,
    CommitTopologyReader, ForkTreeReadFacade, GcBudget, GcStepStatus, GlobalSelectorV1, ObjectId,
    PreparedPublication, RECEIPT_TREE_FANOUT, RECEIPT_TREE_LEAF_ENTRIES, ReceiptTreeEdit,
    ReceiptTreeRoot, RepositoryRootV1, SelectorExpectation, SnapshotRole, SnapshotSelectorId,
    SnapshotSelectorV1, SnapshotTargetV1, StateCell, StateCellRef, StateKey, StateKeyRef,
    StateMutationAudit, StateSource, StateTreeMutation, StateValue, StateValueRef,
    UploadBindingRef, UploadPartV1, UploadProgressV1, UploadSelectorV1, VisibleStateRow,
    advance_gc, edit_state_tree, encode_state_key, encode_state_value, load_commit,
    load_commit_member_records, load_commit_summary, open_coherent_view, prepare_upload_completion,
    prepare_upload_part, put_change_catalog_entries, put_commit_catalog_entries, state_point,
    state_points, state_range,
};

fn raw_id(byte: u8) -> [u8; 16] {
    [byte; 16]
}

fn chronology_fixture_commit(
    commit_byte: u8,
    generation: u64,
    parent_ids: Vec<ObjectId>,
    checkpoint_cursor: CheckpointCursorV1,
) -> CommitObjectV1 {
    CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(commit_byte)),
        generation,
        parent_commit_object_ids: parent_ids,
        members: Vec::new(),
        member_page_object_ids: Vec::new(),
        global_state_root: ObjectId::from_bytes([0xa1; 32]),
        local_state_root: ObjectId::from_bytes([0xa2; 32]),
        checkpoint_cursor,
        metadata: vec![commit_byte],
    }
}

#[test]
fn checkpoint_cursor_models_branch_rewind_merge_and_partial_replay() {
    let main = CanonicalBranchId::from_bytes(raw_id(0x11));
    let branch = CanonicalBranchId::from_bytes(raw_id(0x12));
    let root = chronology_fixture_commit(1, 0, Vec::new(), CheckpointCursorV1::root());
    let (root_id, _) = root.encode().expect("root cursor encodes");

    let first_cursor = CheckpointCursorV1::after_first_parent(root_id, &root, main, false)
        .expect("ordinary cursor");
    let first = chronology_fixture_commit(2, 1, vec![root_id], first_cursor);
    let (first_id, _) = first.encode().expect("ordinary commit encodes");
    assert_eq!(first_cursor.latest_for_branch(first_id, main), (root_id, 1));

    let checkpoint_cursor = CheckpointCursorV1::after_first_parent(first_id, &first, main, true)
        .expect("checkpoint cursor");
    let checkpoint = chronology_fixture_commit(3, 2, vec![first_id], checkpoint_cursor);
    let (checkpoint_id, _) = checkpoint.encode().expect("checkpoint encodes");
    assert_eq!(
        checkpoint_cursor.latest_for_branch(checkpoint_id, main),
        (checkpoint_id, 0)
    );
    assert_eq!(checkpoint_cursor.previous_checkpoint(), Some((root_id, 2)));

    let replay_cursor =
        CheckpointCursorV1::after_first_parent(checkpoint_id, &checkpoint, main, false)
            .expect("partial replay cursor");
    let replay = chronology_fixture_commit(4, 3, vec![checkpoint_id], replay_cursor);
    let (replay_id, _) = replay.encode().expect("partial replay encodes");
    assert_eq!(
        replay_cursor.latest_for_branch(replay_id, main),
        (checkpoint_id, 1)
    );

    // A merge derives chronology from its first parent only. The second edge
    // cannot seed an independent checkpoint target.
    let merge_cursor = CheckpointCursorV1::after_first_parent(replay_id, &replay, main, false)
        .expect("merge cursor");
    let merge = chronology_fixture_commit(5, 4, vec![replay_id, first_id], merge_cursor);
    let (merge_id, _) = merge.encode().expect("merge encodes");
    assert_eq!(
        merge_cursor.latest_for_branch(merge_id, main),
        (checkpoint_id, 2)
    );

    // Rewinding selects the cursor carried by that historical head. A branch
    // inheriting another owner's head sees only the implicit repository root.
    assert_eq!(
        checkpoint_cursor.latest_for_branch(checkpoint_id, main),
        (checkpoint_id, 0)
    );
    assert_eq!(
        merge_cursor.latest_for_branch(merge_id, branch),
        (root_id, 4)
    );

    // The first branch-owned checkpoint starts its own chain at the root.
    let branch_checkpoint_cursor =
        CheckpointCursorV1::after_first_parent(merge_id, &merge, branch, true)
            .expect("branch checkpoint cursor");
    assert_eq!(
        branch_checkpoint_cursor.previous_checkpoint(),
        Some((root_id, 5))
    );

    let malformed = chronology_fixture_commit(
        6,
        5,
        vec![merge_id],
        CheckpointCursorV1::Ordinary {
            owner_branch_id: main,
            root_commit_object_id: root_id,
            distance_to_root: 5,
            latest_checkpoint_object_id: checkpoint_id,
            distance_to_latest: 0,
        },
    );
    assert!(malformed.encode().is_err());
}

#[test]
fn current_commit_decoder_rejects_authenticated_pre_cursor_envelope() {
    let old_commit_id = CommitId::from_bytes(raw_id(0x0f));
    let old_global_root = ObjectId::from_bytes([0xa1; 32]);
    let old_local_root = ObjectId::from_bytes([0xa2; 32]);
    let (old_object_id, old_bytes) = encode_object(ObjectDomain::CommitV1, |encoder| {
        encoder.fixed(old_commit_id.as_bytes());
        encoder.u64(0);
        encoder.u32(0);
        encoder.u32(0);
        encode_id(encoder, old_global_root);
        encode_id(encoder, old_local_root);
        encoder.bytes(b"pre-cursor-v1")
    })
    .expect("authentic pre-cursor CommitV1 envelope");

    let error = CommitObjectV1::decode(old_object_id, &old_bytes)
        .expect_err("current decoder must reject the obsolete authenticated domain");
    assert!(
        matches!(
            error,
            StorageError::Corruption(ref message)
                if message.contains("CommitV1") && message.contains("CommitV2")
        ),
        "obsolete CommitV1 must fail at the authenticated domain boundary: {error:?}",
    );
}

#[test]
fn branch_ref_timestamp_is_authenticated_and_round_trips() {
    let first_timestamp = LixTimestamp::expect_parse("first", "2026-05-19T00:00:00.001Z");
    let second_timestamp = LixTimestamp::expect_parse("second", "2026-05-19T00:00:00.002Z");
    let branch_ref = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0x11)),
        updated_at: first_timestamp,
        branch_id: CanonicalBranchId::from_bytes(raw_id(0x22)),
        before_semantic_head_commit_object_id: None,
        after_semantic_head_commit_object_id: Some(ObjectId::from_bytes([0x33; 32])),
        previous_ref_change_object_id: None,
        payload: b"head publication".to_vec(),
        json_payload_object_ids: Vec::new(),
    };
    let (first_id, first_bytes) = branch_ref.encode().expect("first RefChange");
    let decoded = ChangeObjectV1::decode(first_id, &first_bytes).expect("decode RefChange");
    let ChangeObjectV1::BranchRef { updated_at, .. } = decoded else {
        panic!("encoded RefChange decoded as a different change kind");
    };
    assert_eq!(updated_at, first_timestamp);

    let second_ref = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0x11)),
        updated_at: second_timestamp,
        branch_id: CanonicalBranchId::from_bytes(raw_id(0x22)),
        before_semantic_head_commit_object_id: None,
        after_semantic_head_commit_object_id: Some(ObjectId::from_bytes([0x33; 32])),
        previous_ref_change_object_id: None,
        payload: b"head publication".to_vec(),
        json_payload_object_ids: Vec::new(),
    };
    let (second_id, _) = second_ref.encode().expect("second RefChange");
    assert_ne!(first_id, second_id);
}

#[test]
fn stale_state_key_merge_uses_encoded_order_and_deduplicates_overlay_keys() {
    // StateKey's derived order places file_id before row_pk, whereas the
    // authenticated wire key is schema, row_pk, file_id.  This pair makes
    // those orders disagree, and the common key appears from both roots.
    let file_first = StateKey {
        schema_key: "app.row".to_owned(),
        file_id: Some("a-file".to_owned()),
        row_pk: RowPk::single("z-row"),
    };
    let row_first = StateKey {
        schema_key: "app.row".to_owned(),
        file_id: Some("z-file".to_owned()),
        row_pk: RowPk::single("a-row"),
    };
    let merged = super::view::merge_sorted_state_keys(
        vec![file_first.clone()],
        vec![
            row_first.clone(),
            row_first.clone(),
            file_first.clone(),
        ],
    );
    assert_eq!(merged.len(), 2, "one visible row per canonical key");
    assert_eq!(
        merged[0], row_first,
        "canonical encoded order is preserved"
    );
    assert_eq!(merged[1], file_first);
    assert_eq!(
        merged.iter().take(1).count(),
        1,
        "LIMIT 1 sees the first canonical row"
    );
}

async fn commit_publication_for_test<S>(
    publication: PreparedPublication,
    storage: &S,
) -> Result<(), StorageError>
where
    S: Storage,
{
    let (writes, preconditions) = publication.into_storage_plan()?;
    StorageWriteSet::commit(
        writes,
        storage,
        WriteOptions {
            preconditions,
            ..WriteOptions::default()
        },
    )
    .await
    .map_err(|error| match error {
        StorageWriteSetError::Storage(error) => error,
        error => StorageError::Io(error.to_string()),
    })?;
    Ok(())
}

async fn commit_write_set_for_test<S>(writes: StorageWriteSet, storage: &S)
where
    S: Storage,
{
    StorageWriteSet::commit(writes, storage, WriteOptions::default())
        .await
        .expect("commit test write set");
}

#[test]
fn topology_cache_is_private_and_inseparable_from_its_storage_snapshot() {
    let serving = include_str!("serving.rs");
    let facade = include_str!("mod.rs");
    assert!(serving.contains("pub(crate) struct CommitTopologyReader<R>"));
    assert!(serving.contains("read: R,"));
    assert!(serving.contains("cache: CommitTopologyReadCache,"));
    assert!(serving.contains("struct CommitTopologyReadCache"));
    assert!(!serving.contains("pub(crate) struct CommitTopologyReadCache"));
    assert!(!serving.contains("pub(crate) async fn load_commit_topology_batch"));
    assert!(!facade.contains("CommitTopologyReadCache"));
    assert!(!facade.contains("load_commit_topology_batch"));
}

#[test]
fn blob_manifest_identity_is_an_owner_checked_integrity_copy() {
    let model = include_str!("model.rs");
    let merkle = include_str!("merkle.rs");
    let reachability = include_str!("reachability.rs");
    let manifest = model
        .split_once("pub(crate) struct BlobManifestV1")
        .and_then(|(_, rest)| rest.split_once("pub(crate) struct UploadPartV1"))
        .map(|(body, _)| body)
        .expect("BlobManifestV1 source section");
    assert!(manifest.contains("root_object_id: ObjectId"));
    assert!(manifest.contains("root_height: u32"));
    assert!(manifest.contains("chunk_bytes: u64"));
    assert!(manifest.contains("canonical_blob_id: BlobId"));
    assert!(manifest.contains("canonical_merkle_blob_id"));
    assert!(!manifest.contains("ordered_chunks: Vec<BlobChunkRefV1>"));
    assert!(!manifest.contains("content_digest: [u8; 32]"));
    assert!(merkle.contains("BlobManifestV1::from_merkle_root"));
    assert!(merkle.contains("canonical_merkle_blob_id"));
    assert!(reachability.contains("authenticated_merkle_edges"));
    assert!(!merkle.contains("BlobId::from_canonical_content"));
    assert!(!merkle.contains(concat!("BlobId::from_", "chunks")));
    let manifest_reachability = reachability
        .split_once("ObjectDomain::BlobManifest =>")
        .and_then(|(_, rest)| rest.split_once("ObjectDomain::BlobMerkleLeafV1"))
        .map(|(body, _)| body)
        .expect("BlobManifest reachability arm");
    assert!(!manifest_reachability.contains("ordered_chunks"));
    assert!(!manifest_reachability.contains("content_digest"));
}

#[tokio::test]
async fn forktree_json_object_materializes_and_rejects_corruption() {
    let value = format!(
        r#"{{"large":"{}"}}"#,
        "x".repeat(crate::json_store::JSON_INLINE_MAX_BYTES + 1)
    );
    let chunk = BlobChunkV1 {
        bytes: Bytes::from(value.clone()),
    };
    let (object_id, encoded) = chunk.encode().expect("JSON object encoding");
    let storage = Memory::new();
    let mut writes = StorageWriteSet::new();
    writes.put(
        OBJECT_SPACE,
        object_id.as_bytes().to_vec(),
        encoded.to_vec(),
    );
    commit_write_set_for_test(writes, &storage).await;

    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("JSON object read");
    let facade = ForkTreeReadFacade::new(read);
    let slot = crate::json_store::JsonSlot::ForkTreeObject(*object_id.as_bytes());
    assert_eq!(
        facade
            .load_json_slot(&slot)
            .await
            .expect("authenticated JSON object"),
        Some(value)
    );
    drop(facade);

    let mut corrupted = encoded.to_vec();
    *corrupted.last_mut().expect("encoded JSON object") ^= 1;
    assert!(
        BlobChunkV1::decode(object_id, &corrupted).is_err(),
        "corrupt authenticated JSON objects must fail closed"
    );
    let mut writes = StorageWriteSet::new();
    writes.delete(OBJECT_SPACE, object_id.as_bytes().to_vec());
    commit_write_set_for_test(writes, &storage).await;
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("corrupt JSON object read");
    let facade = ForkTreeReadFacade::new(read);
    assert!(
        facade.load_json_slot(&slot).await.is_err(),
        "missing authenticated JSON objects must fail closed"
    );
}

#[tokio::test]
async fn selected_commit_member_authenticates_canonical_owner_source_and_generation() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("open selected history view");
    let selection = super::HistoricalMemberSelection::new(
        seed.commit_id,
        seed.semantic_change_id,
        seed.state_keys[0].clone(),
    );
    let mut batch = super::serving::select_historical_commit_members(
        &view,
        seed.commit_id,
        std::slice::from_ref(&selection),
    )
    .await
    .expect("select authenticated historical member batch");
    let selected = batch
        .take_selected()
        .pop()
        .expect("selected historical member");
    let member = selected.member;
    let source_commit = selected.source_commit;
    let source_change = selected.source_change;
    assert_eq!(source_commit.commit_id, seed.commit_id);
    assert_eq!(source_change.change_id(), seed.semantic_change_id);
    assert_eq!(
        member.selected_created_at(),
        Some(LixTimestamp::from_unix_millis_utc_lossy(1))
    );
    assert_eq!(
        member.clone(),
        CommitMemberV3::selected(
            seed.semantic_change_id,
            seed.commit_object_id,
            0,
            LixTimestamp::from_unix_millis_utc_lossy(1),
        )
    );
    batch
        .consume_proof(view.view_instance_id(), 2, &member)
        .expect("publisher consumes exact selected-member proof");
    batch
        .finish_proof(view.view_instance_id())
        .expect("publisher consumes the complete proof batch");
    let entry = ChangeCatalogEntry {
        owner: ChangeCatalogOwner::CommitMember {
            commit_object_id: seed.commit_object_id,
            ordinal: 0,
        },
    };
    super::serving::validate_member_catalog_owner(
        view.test_storage_read(),
        view.repository_root().commit_catalog_root,
        content_id(0xa1),
        2,
        0,
        member.clone(),
        entry,
    )
    .await
    .expect("older selected source is valid");
    assert!(
        super::serving::validate_member_catalog_owner(
            view.test_storage_read(),
            view.repository_root().commit_catalog_root,
            content_id(0xa1),
            1,
            0,
            member,
            entry,
        )
        .await
        .is_err(),
        "same-generation selected history must fail closed"
    );
}

#[tokio::test]
async fn selected_member_batch_proof_rejects_wrong_view_owner_ordinal_generation_and_cardinality() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("open selected history view");
    let selection = super::HistoricalMemberSelection::new(
        seed.commit_id,
        seed.semantic_change_id,
        seed.state_keys[0].clone(),
    );
    let resolve = || {
        super::serving::select_historical_commit_members(
            &view,
            seed.commit_id,
            std::slice::from_ref(&selection),
        )
    };

    let mut wrong_ordinal = resolve().await.expect("wrong-ordinal proof source");
    let valid = wrong_ordinal
        .take_selected()
        .pop()
        .expect("selected member");
    let wrong = CommitMemberV3::selected(
        seed.semantic_change_id,
        seed.commit_object_id,
        1,
        LixTimestamp::from_unix_millis_utc_lossy(1),
    );
    assert!(
        wrong_ordinal
            .consume_proof(view.view_instance_id(), 2, &wrong)
            .is_err(),
        "a different source ordinal must not consume the sealed proof"
    );

    let mut wrong_owner = resolve().await.expect("wrong-owner proof source");
    let _ = wrong_owner.take_selected();
    let wrong = CommitMemberV3::selected(
        seed.semantic_change_id,
        content_id(0xa1),
        0,
        LixTimestamp::from_unix_millis_utc_lossy(1),
    );
    assert!(
        wrong_owner
            .consume_proof(view.view_instance_id(), 2, &wrong)
            .is_err(),
        "a different source object must not consume the sealed proof"
    );

    let mut same_generation = resolve().await.expect("generation proof source");
    let selected = same_generation
        .take_selected()
        .pop()
        .expect("selected member");
    assert!(
        same_generation
            .consume_proof(view.view_instance_id(), 1, &selected.member)
            .is_err(),
        "same-generation selected history must fail closed"
    );

    let second_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("open distinct retained view");
    let mut wrong_view = resolve().await.expect("cross-view proof source");
    let selected = wrong_view.take_selected().pop().expect("selected member");
    assert!(
        wrong_view
            .consume_proof(second_view.view_instance_id(), 2, &selected.member)
            .is_err(),
        "a proof must not cross retained-view instances"
    );

    let mut duplicate = resolve().await.expect("duplicate proof source");
    let selected = duplicate.take_selected().pop().expect("selected member");
    duplicate
        .consume_proof(view.view_instance_id(), 2, &selected.member)
        .expect("first proof consumption");
    assert!(
        duplicate
            .consume_proof(view.view_instance_id(), 2, &selected.member)
            .is_err(),
        "one authenticated request must not authorize two publications"
    );

    let mut unused = resolve().await.expect("unused proof source");
    let _ = unused.take_selected();
    assert!(
        unused.finish_proof(view.view_instance_id()).is_err(),
        "publication must consume the exact requested proof cardinality"
    );

    assert_eq!(valid.source_commit.generation, 1);
}

#[tokio::test]
async fn stale_selected_leaf_requires_catalog_owner_source_and_page_identity() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("open stale selected-leaf view");
    let commit = CommitObjectV1::decode(
        seed.commit_object_id,
        seed.objects
            .get(seed.commit_object_id)
            .expect("seed commit"),
    )
    .expect("decode seed commit");
    let seed_members = seed_commit_members(&seed);
    let repository = view.repository_root();
    let mut shadowed_cache = super::serving::StaleMemberAuthCache::new(view.view_instance_id());
    assert!(
        super::serving::resolve_semantic_member_with_stale_auth(
            view.test_storage_read(),
            view.view_instance_id(),
            &seed_members[0],
            &seed.state_keys[0],
            seed.commit_object_id,
            commit.generation,
            0,
            repository.commit_catalog_root,
            repository.change_catalog_root,
            &mut shadowed_cache,
        )
        .await
        .is_err(),
        "a local row with a different ChangeId must not fall through to the global root"
    );
    let member = seed_members[2].clone();
    let state_key = seed.state_keys[2].clone();
    let mut closures = super::serving::StaleMemberAuthCache::new(view.view_instance_id());
    super::serving::resolve_semantic_member_with_stale_auth(
        view.test_storage_read(),
        view.view_instance_id(),
        &member,
        &state_key,
        seed.commit_object_id,
        commit.generation,
        2,
        repository.commit_catalog_root,
        repository.change_catalog_root,
        &mut closures,
    )
    .await
    .expect("canonical selected leaf owner");

    let mut wrong_ordinal_closures =
        super::serving::StaleMemberAuthCache::new(view.view_instance_id());
    assert!(
        super::serving::resolve_semantic_member_with_stale_auth(
            view.test_storage_read(),
            view.view_instance_id(),
            &member,
            &state_key,
            seed.commit_object_id,
            commit.generation,
            1,
            repository.commit_catalog_root,
            repository.change_catalog_root,
            &mut wrong_ordinal_closures,
        )
        .await
        .is_err(),
        "a content-valid member at the wrong target ordinal must fail closed"
    );

    let empty_change_catalog = build_change_catalog(&[]).expect("empty change catalog");
    let mut missing_catalog_closures =
        super::serving::StaleMemberAuthCache::new(view.view_instance_id());
    assert!(
        super::serving::resolve_semantic_member_with_stale_auth(
            view.test_storage_read(),
            view.view_instance_id(),
            &member,
            &state_key,
            seed.commit_object_id,
            commit.generation,
            0,
            repository.commit_catalog_root,
            empty_change_catalog.root.object_id,
            &mut missing_catalog_closures,
        )
        .await
        .is_err(),
        "a selected leaf without a ChangeCatalog back-edge must fail closed"
    );

    let selected = CommitMemberV3::selected(
        member.change_id(),
        seed.commit_object_id,
        1,
        LixTimestamp::from_unix_millis_utc_lossy(1),
    );
    let mut wrong_source_closures =
        super::serving::StaleMemberAuthCache::new(view.view_instance_id());
    assert!(
        super::serving::resolve_semantic_member_with_stale_auth(
            view.test_storage_read(),
            view.view_instance_id(),
            &selected,
            &state_key,
            content_id(0xa1),
            2,
            0,
            repository.commit_catalog_root,
            repository.change_catalog_root,
            &mut wrong_source_closures,
        )
        .await
        .is_err(),
        "a selected leaf with a substituted source ordinal must fail closed"
    );

    let mut wrong_generation_closures =
        super::serving::StaleMemberAuthCache::new(view.view_instance_id());
    assert!(
        super::serving::resolve_semantic_member_with_stale_auth(
            view.test_storage_read(),
            view.view_instance_id(),
            &CommitMemberV3::selected(
                member.change_id(),
                seed.commit_object_id,
                0,
                LixTimestamp::from_unix_millis_utc_lossy(1),
            ),
            &state_key,
            content_id(0xa1),
            1,
            0,
            repository.commit_catalog_root,
            repository.change_catalog_root,
            &mut wrong_generation_closures,
        )
        .await
        .is_err(),
        "a selected source at the target generation must fail closed"
    );

    // A merge may legitimately select a page from a non-ancestor source
    // commit.  The endpoint's authenticated roots plus the page's catalog
    // membership are the required proof; serial ancestry is not.
    let mut non_ancestor = build_seed();
    let global_state_root = non_ancestor.global_state_root;
    let local_state_root = non_ancestor.local_state_root;
    let (non_ancestor_id, non_ancestor_object_id) = insert_graph_commit_with_roots(
        &mut non_ancestor,
        0x21,
        2,
        Vec::new(),
        global_state_root,
        local_state_root,
    );
    install_graph_head(
        &mut non_ancestor,
        &[(non_ancestor_id, non_ancestor_object_id)],
        non_ancestor_object_id,
        0x22,
    );
    let non_ancestor_storage = Memory::new();
    seed_storage(&non_ancestor_storage, &non_ancestor).await;
    let non_ancestor_view = open_coherent_view(&non_ancestor_storage, non_ancestor.branch_id)
        .await
        .expect("open non-ancestor root-bound view");
    let non_ancestor_repository = non_ancestor_view.repository_root();
    let mut non_ancestor_cache =
        super::serving::StaleCommitSummaryCache::new(non_ancestor_view.view_instance_id());
    let non_ancestor_summary = super::serving::load_historical_commit_state_roots_for_stale(
        non_ancestor_view.test_storage_read(),
        non_ancestor_view.view_instance_id(),
        &non_ancestor_repository,
        public_commit_id(0x21),
        &mut non_ancestor_cache,
    )
    .await
    .expect("authenticate non-ancestor endpoint summary");
    let non_ancestor_rows = super::serving::state_points_on_read_for_stale(
        &non_ancestor_repository,
        non_ancestor_summary,
        &[non_ancestor.state_keys[0].clone()],
        true,
        non_ancestor_view.view_instance_id(),
        non_ancestor_view.test_storage_read(),
    )
    .await
    .expect("root-bound non-ancestor page is valid");
    assert!(
        non_ancestor_rows[0].is_some(),
        "root-bound non-ancestor page must remain readable without serial ancestry"
    );
}

fn four_page_stale_fixture() -> (
    CommitId,
    Vec<(ObjectId, Bytes)>,
    Vec<ObjectId>,
    Vec<CommitChangePageV3>,
) {
    let commit_id = CommitId::from_bytes(raw_id(0xd0));
    let members = (0..769).map(zero_edge_page_member).collect::<Vec<_>>();
    let pages =
        CommitChangePageV3::encode_pages(commit_id, &members).expect("four-page stale fixture");
    let page_ids = pages.objects.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let decoded = pages
        .objects
        .iter()
        .map(|(id, bytes)| CommitChangePageV3::decode(*id, bytes).expect("fixture page"))
        .collect::<Vec<_>>();
    (commit_id, pages.objects, page_ids, decoded)
}

async fn validate_four_page_stale_fixture(
    commit_id: CommitId,
    objects: Vec<(ObjectId, Bytes)>,
    page_ids: Vec<ObjectId>,
    selected_page_object_id: ObjectId,
    selected_page: &CommitChangePageV3,
) -> Result<(), StorageError> {
    let storage = Memory::new();
    let mut writes = StorageWriteSet::new();
    for (object_id, bytes) in objects {
        writes.put(OBJECT_SPACE, object_id.as_bytes().to_vec(), bytes.to_vec());
    }
    commit_write_set_for_test(writes, &storage).await;
    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("four-page stale read"),
    );
    let binding_id = 1;
    let mut cache = super::serving::StaleMemberAuthCache::new(binding_id);
    super::serving::validate_stale_page_position(
        &read,
        binding_id,
        content_id(0xd1),
        commit_id,
        &page_ids,
        selected_page_object_id,
        selected_page,
        &mut cache,
    )
    .await
}

#[tokio::test]
async fn stale_page_prefix_gap_before_selected_page_fails_closed() {
    let (commit_id, mut objects, page_ids, pages) = four_page_stale_fixture();
    let mut middle = pages[1].clone();
    middle.start_ordinal += 1;
    let (middle_id, middle_bytes) = middle.encode().expect("gapped middle page");
    let mut selected = pages[3].clone();
    selected.start_ordinal += 1;
    let (selected_id, selected_bytes) = selected.encode().expect("shifted selected page");
    objects.push((middle_id, middle_bytes));
    objects.push((selected_id, selected_bytes));
    let page_ids = vec![page_ids[0], middle_id, page_ids[2], selected_id];
    assert!(
        validate_four_page_stale_fixture(commit_id, objects, page_ids, selected_id, &selected,)
            .await
            .is_err(),
        "a gap hidden before the selected page must fail closed"
    );
}

#[tokio::test]
async fn stale_page_prefix_wrong_commit_fails_closed() {
    let (commit_id, mut objects, page_ids, pages) = four_page_stale_fixture();
    let mut wrong_first = pages[0].clone();
    wrong_first.commit_id = CommitId::from_bytes(raw_id(0xd2));
    let (wrong_first_id, wrong_first_bytes) = wrong_first.encode().expect("wrong first page");
    objects.push((wrong_first_id, wrong_first_bytes));
    let page_ids = vec![wrong_first_id, page_ids[1], page_ids[2], page_ids[3]];
    let selected_page_object_id = page_ids[3];
    assert!(
        validate_four_page_stale_fixture(
            commit_id,
            objects,
            page_ids,
            selected_page_object_id,
            &pages[3],
        )
        .await
        .is_err(),
        "a wrong-commit prefix page must fail closed"
    );
}

#[tokio::test]
async fn stale_page_prefix_missing_page_fails_closed() {
    let (commit_id, objects, page_ids, pages) = four_page_stale_fixture();
    let missing_id = content_id(0xd3);
    let page_ids = vec![page_ids[0], missing_id, page_ids[2], page_ids[3]];
    let selected_page_object_id = page_ids[3];
    assert!(
        validate_four_page_stale_fixture(
            commit_id,
            objects,
            page_ids,
            selected_page_object_id,
            &pages[3],
        )
        .await
        .is_err(),
        "a missing prefix page must fail closed"
    );
}

#[tokio::test]
async fn commit_summary_defers_unaccessed_member_authentication() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;

    let mut writes = StorageWriteSet::new();
    writes.delete(
        OBJECT_SPACE,
        seed.semantic_change_object_id.as_bytes().to_vec(),
    );
    commit_write_set_for_test(writes, &storage).await;

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("open summary view");
    let summary = load_commit_summary(&view, seed.commit_id)
        .await
        .expect("authenticated commit summary")
        .expect("seed summary");
    assert_eq!(summary.commit_id, seed.commit_id);
    let selection = super::HistoricalMemberSelection::new(
        seed.commit_id,
        seed.semantic_change_id,
        seed.state_keys[0].clone(),
    );
    assert!(
        super::serving::select_historical_commit_members(
            &view,
            seed.commit_id,
            std::slice::from_ref(&selection),
        )
        .await
        .is_err(),
        "a missing authenticated member page must not issue a selected-member proof"
    );
    assert!(
        load_commit(&view, seed.commit_id).await.is_err(),
        "later member consumption must fail closed on the missing member"
    );
}

#[tokio::test]
async fn selected_commit_member_rejects_missing_or_remapped_source_catalog_entry() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;

    let seed_commit = CommitObjectV1::decode(
        seed.commit_object_id,
        seed.objects
            .get(seed.commit_object_id)
            .expect("seed commit"),
    )
    .expect("decode seed commit");
    let source_commit = CommitObjectV1 {
        commit_id: seed.commit_id,
        generation: 1,
        parent_commit_object_ids: Vec::new(),
        members: Vec::new(),
        member_page_object_ids: seed_commit.member_page_object_ids,
        global_state_root: seed.global_state_root,
        local_state_root: seed.local_state_root,
        checkpoint_cursor: CheckpointCursorV1::root(),
        metadata: b"remapped-source-commit".to_vec(),
    };
    let (remapped_source_object_id, remapped_source_bytes) =
        source_commit.encode().expect("remapped source commit");
    let remapped_catalog = build_commit_catalog(&[(
        seed.commit_id,
        CommitCatalogEntry {
            commit_object_id: remapped_source_object_id,
        },
    )])
    .expect("remapped commit catalog");
    let empty_catalog = build_commit_catalog(&[]).expect("empty commit catalog");
    let mut writes = StorageWriteSet::new();
    writes.put(
        OBJECT_SPACE,
        remapped_source_object_id.as_bytes().to_vec(),
        remapped_source_bytes.to_vec(),
    );
    for (object_id, bytes) in remapped_catalog.objects.iter() {
        writes.put(OBJECT_SPACE, object_id.as_bytes().to_vec(), bytes.to_vec());
    }
    for (object_id, bytes) in empty_catalog.objects.iter() {
        writes.put(OBJECT_SPACE, object_id.as_bytes().to_vec(), bytes.to_vec());
    }
    commit_write_set_for_test(writes, &storage).await;

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("open selected history view");
    let member = CommitMemberV3::selected(
        seed.semantic_change_id,
        seed.commit_object_id,
        0,
        LixTimestamp::from_unix_millis_utc_lossy(1),
    );
    let entry = ChangeCatalogEntry {
        owner: ChangeCatalogOwner::CommitMember {
            commit_object_id: seed.commit_object_id,
            ordinal: 0,
        },
    };
    assert!(
        super::serving::validate_member_catalog_owner(
            view.test_storage_read(),
            remapped_catalog.root.object_id,
            content_id(0xa1),
            2,
            0,
            member.clone(),
            entry,
        )
        .await
        .is_err(),
        "a remapped CommitCatalog source must fail closed"
    );
    assert!(
        super::serving::validate_member_catalog_owner(
            view.test_storage_read(),
            empty_catalog.root.object_id,
            content_id(0xa1),
            2,
            0,
            member,
            entry,
        )
        .await
        .is_err(),
        "a missing CommitCatalog source must fail closed"
    );
}

fn content_id(byte: u8) -> ObjectId {
    ObjectId::from_bytes([byte; 32])
}

fn public_commit_id(byte: u8) -> crate::changelog::CommitId {
    crate::changelog::CommitId::new(uuid::Uuid::from_bytes(raw_id(byte)))
}

fn chronology_commit_metadata(
    commit_byte: u8,
    generation: u64,
    parent_commit_ids: Vec<crate::changelog::CommitId>,
) -> Vec<u8> {
    chronology_commit_metadata_for(
        CommitId::from_bytes(raw_id(commit_byte)),
        generation,
        parent_commit_ids,
    )
}

fn chronology_commit_metadata_for(
    commit_id: CommitId,
    generation: u64,
    parent_commit_ids: Vec<crate::changelog::CommitId>,
) -> Vec<u8> {
    let public_commit_id =
        crate::changelog::CommitId::new(uuid::Uuid::from_bytes(*commit_id.as_bytes()));
    let mut change_bytes = *commit_id.as_bytes();
    change_bytes[0] ^= 0x80;
    crate::changelog::encode_forktree_commit_payload(&crate::changelog::CommitRecord {
        format_version: 2,
        commit_id: public_commit_id,
        generation,
        parent_commit_ids,
        change_id: crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(change_bytes)),
        account_id: crate::SYSTEM_ACCOUNT_ID.to_owned(),
        created_at: LixTimestamp::from_unix_millis_utc_lossy(i64::from_be_bytes(
            commit_id.as_bytes()[8..]
                .try_into()
                .expect("commit id timestamp suffix"),
        )),
    })
    .expect("chronology commit payload")
}

fn test_change_id(commit_byte: u8, key: &[u8], global: bool) -> ChangeId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"lix.forktree.test.change.v2\0");
    hasher.update(&[commit_byte]);
    hasher.update(&[u8::from(global)]);
    hasher.update(key);
    let mut id = [0_u8; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    ChangeId::from_bytes(id)
}

fn test_state_member(
    primary_key: &str,
    cell: StateCellRef<'_>,
    commit_byte: u8,
    manifests: &[ObjectId],
    global: bool,
) -> TestStateEntry {
    let row_pk = RowPk::single(primary_key);
    let key = encode_state_key(StateKeyRef {
        schema_key: "app.row",
        file_id: Some("file"),
        row_pk: &row_pk,
    });
    let change_id = test_change_id(commit_byte, &key, global);
    let cell = match cell {
        StateCellRef::Value(value) => StateCell::NativeRow(
            crate::native_row::encode(
                &lix_schema::from_value(serde_json::json!({
                    "$schema": lix_schema::SCHEMA_V1_URI,
                    "key": "app.row",
                    "columns": [
                        {"name": "id", "type": "text", "nullable": false},
                        {"name": "value", "type": "text", "nullable": true}
                    ],
                    "primary_key": ["id"]
                }))
                .expect("test app schema"),
                &row_pk,
                global,
                Some("file"),
                &serde_json::json!({"id": primary_key, "value": value}),
            )
            .expect("test native row"),
        ),
        StateCellRef::Null => StateCell::NativeRow(
            crate::native_row::encode(
                &lix_schema::from_value(serde_json::json!({
                    "$schema": lix_schema::SCHEMA_V1_URI,
                    "key": "app.row",
                    "columns": [
                        {"name": "id", "type": "text", "nullable": false},
                        {"name": "value", "type": "text", "nullable": true}
                    ],
                    "primary_key": ["id"]
                }))
                .expect("test app schema"),
                &row_pk,
                global,
                Some("file"),
                &serde_json::json!({"id": primary_key, "value": null}),
            )
            .expect("test native null row"),
        ),
        StateCellRef::Tombstone => StateCell::Tombstone,
    };
    test_state_entry(
        key,
        change_id,
        cell,
        global,
        manifests.to_vec(),
    )
}

struct TestStateEntry {
    key: Vec<u8>,
    member: CommitMemberV3,
    value: StateValue,
}

fn test_state_entry(
    key: Vec<u8>,
    change_id: ChangeId,
    cell: StateCell,
    global: bool,
    manifests: Vec<ObjectId>,
) -> TestStateEntry {
    let (layout_id, owner_digest, semantic_digest, deleted) = match &cell {
        StateCell::NativeRow(native) => (
            native.layout_id,
            native.owner_digest,
            native.semantic_digest,
            false,
        ),
        StateCell::Tombstone => ([0; 32], [0; 32], [0; 32], true),
        StateCell::Value(_) | StateCell::Null => panic!("test history must be native"),
    };
    let public_change_id =
        crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(*change_id.as_bytes()));
    TestStateEntry {
        member: CommitMemberV3::introduced(
            change_id,
            key.clone(),
            layout_id,
            global,
            owner_digest,
            semantic_digest,
            deleted,
            "forktree-test".to_owned(),
            LixTimestamp::from_unix_millis_utc_lossy(1),
            LixTimestamp::from_unix_millis_utc_lossy(2),
            None,
        ),
        value: StateValue {
            change_id: public_change_id,
            commit_id: crate::changelog::CommitId::default(),
            created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
            updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
            cell,
            metadata: None,
            origin_key: None,
            blob_manifest_object_ids: manifests,
        },
        key,
    }
}

fn test_blob_ref_member(
    primary_key: &str,
    declared_id: &str,
    blob_id: crate::binary_cas::BlobId,
    size_bytes: u64,
    commit_byte: u8,
    manifest: ObjectId,
) -> TestStateEntry {
    let row_pk = RowPk::uuid_from_canonical(primary_key).expect("canonical blob-ref id");
    let key = encode_state_key(StateKeyRef {
        schema_key: "lix_binary_blob_ref",
        file_id: Some(primary_key),
        row_pk: &row_pk,
    });
    let change_id = test_change_id(commit_byte, &key, false);
    let snapshot = serde_json::json!({
        "id": declared_id,
        "blob_hash": blob_id.to_hex(),
        "size_bytes": size_bytes,
    })
    ;
    let cell = StateCell::NativeRow(
        crate::native_row::encode(
            &crate::native_row::seed_schema("lix_binary_blob_ref").expect("blob-ref schema"),
            &row_pk,
            false,
            Some(primary_key),
            &snapshot,
        )
        .expect("native blob-ref row"),
    );
    test_state_entry(
        key,
        change_id,
        cell,
        false,
        vec![manifest],
    )
}

fn encode_test_state_entries(
    commit_byte: u8,
    entries: Vec<TestStateEntry>,
) -> (
    Vec<(Vec<u8>, Vec<u8>)>,
    Vec<CommitMemberV3>,
    Vec<(ObjectId, Bytes)>,
    Vec<(ObjectId, Bytes)>,
) {
    let members = entries
        .iter()
        .map(|entry| entry.member.clone())
        .collect::<Vec<_>>();
    let pages =
        CommitChangePageV3::encode_pages(CommitId::from_bytes(raw_id(commit_byte)), &members)
            .expect("test change pages");
    let pack_rows = entries
        .iter()
        .zip(&pages.member_locations)
        .map(|(entry, location)| {
            let global = entry.member.introduced_identity().expect("introduced").2;
            let mut value = entry.value.clone();
            value.commit_id = crate::changelog::CommitId::new(uuid::Uuid::from_bytes(raw_id(
                commit_byte,
            )));
            (
                entry.key.clone(),
                value,
                *location,
                global,
            )
        })
        .collect::<Vec<_>>();
    let mut pack_locations = BTreeMap::new();
    let mut pack_objects = Vec::new();
    for global in [true, false] {
        let packs = super::current_pack::encode_current_state_packs(
            CommitId::from_bytes(raw_id(commit_byte)),
            global,
            pack_rows
                .iter()
                .filter(|row| row.3 == global)
                .map(|(key, value, location, _)| (key.clone(), value.clone(), *location))
                .collect(),
        )
        .expect("test current-state packs");
        pack_locations.extend(
            packs
                .locations
                .into_iter()
                .map(|(key, location)| ((global, key), location)),
        );
        pack_objects.extend(packs.objects);
    }
    let rows = entries
        .into_iter()
        .map(|entry| {
            let global = entry.member.introduced_identity().expect("introduced").2;
            let location = pack_locations
                .get(&(global, entry.key.clone()))
                .expect("test pack location");
            (
                entry.key,
                encode_state_value(StateValueRef {
                    pack_object_id: location.pack_object_id,
                    pack_ordinal: location.pack_ordinal,
                })
                .expect("state value"),
            )
        })
        .collect();
    (rows, members, pages.objects, pack_objects)
}

fn state_entry(
    primary_key: &str,
    _cell: StateCellRef<'_>,
    commit_byte: u8,
    _manifests: &[ObjectId],
) -> (Vec<u8>, Vec<u8>) {
    let row_pk = RowPk::single(primary_key);
    let key = encode_state_key(StateKeyRef {
        schema_key: "app.row",
        file_id: Some("file"),
        row_pk: &row_pk,
    });
    let value = encode_state_value(StateValueRef {
        pack_object_id: content_id(commit_byte),
        pack_ordinal: 0,
    })
    .expect("state value");
    (key, value)
}

#[derive(Clone)]
struct SeedData {
    objects: ImmutableObjectSet,
    branch_id: CanonicalBranchId,
    commit_id: CommitId,
    commit_object_id: ObjectId,
    semantic_change_id: ChangeId,
    semantic_change_object_id: ObjectId,
    ref_change_id: ChangeId,
    ref_change_object_id: ObjectId,
    repository_root_id: ObjectId,
    branch_snapshot_id: ObjectId,
    global_state_root: ObjectId,
    local_state_root: ObjectId,
    global_selector: GlobalSelectorV1,
    branch_selector: BranchSelectorV1,
    state_keys: Vec<Vec<u8>>,
    orphan_object_id: ObjectId,
    orphan_object_bytes: Bytes,
}

fn build_seed() -> SeedData {
    let branch_id = CanonicalBranchId::from_bytes(raw_id(0x11));
    let commit_id = CommitId::from_bytes(raw_id(0x20));
    let ref_change_id = ChangeId::from_bytes(raw_id(0x31));
    let mut objects = ImmutableObjectSet::default();

    let (all_rows, members, member_page_objects, current_pack_objects) = encode_test_state_entries(
        0x20,
        vec![
            test_state_member("a", StateCellRef::Value("global-a"), 0x20, &[], true),
            test_state_member("b", StateCellRef::Value("global-b"), 0x20, &[], true),
            test_state_member("c", StateCellRef::Null, 0x20, &[], true),
            test_state_member("a", StateCellRef::Value("local-a"), 0x20, &[], false),
            test_state_member("b", StateCellRef::Tombstone, 0x20, &[], false),
            test_state_member("d", StateCellRef::Null, 0x20, &[], false),
        ],
    );
    let semantic_change_id = members[0].change_id();
    let semantic_change_object_id = member_page_objects[0].0;
    for (id, bytes) in member_page_objects {
        objects.insert(id, bytes).expect("commit change page");
    }
    for (id, bytes) in current_pack_objects {
        objects.insert(id, bytes).expect("current-state pack");
    }
    let mut global_rows = all_rows[..3].to_vec();
    global_rows.sort_by(|left, right| left.0.cmp(&right.0));
    let state_keys = global_rows.iter().map(|row| row.0.clone()).collect();
    let global_state = build_state_tree(&global_rows).expect("global state");
    let global_state_root = global_state.root.object_id;
    objects
        .extend(global_state.objects)
        .expect("global objects");

    let mut local_rows = all_rows[3..].to_vec();
    local_rows.sort_by(|left, right| left.0.cmp(&right.0));
    let local_state = build_state_tree(&local_rows).expect("local state");
    let local_state_root = local_state.root.object_id;
    objects.extend(local_state.objects).expect("local objects");
    let commit = CommitObjectV1 {
        commit_id,
        generation: 1,
        parent_commit_object_ids: Vec::new(),
        member_page_object_ids: CommitChangePageV3::encode_pages(commit_id, &members)
            .expect("seed member pages")
            .objects
            .iter()
            .map(|(id, _)| *id)
            .collect(),
        members: members.clone(),
        global_state_root,
        local_state_root,
        checkpoint_cursor: CheckpointCursorV1::root(),
        metadata: b"commit".to_vec(),
    };
    let (commit_object_id, commit_bytes) = commit.encode().expect("commit");
    objects
        .insert(commit_object_id, commit_bytes)
        .expect("commit object");
    let ref_change = ChangeObjectV1::BranchRef {
        change_id: ref_change_id,
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(1),
        branch_id,
        before_semantic_head_commit_object_id: None,
        after_semantic_head_commit_object_id: Some(commit_object_id),
        previous_ref_change_object_id: None,
        payload: b"create-main".to_vec(),
        json_payload_object_ids: Vec::new(),
    };
    let (ref_change_object_id, ref_change_bytes) = ref_change.encode().expect("ref change");
    objects
        .insert(ref_change_object_id, ref_change_bytes)
        .expect("ref-change object");
    let commit_catalog =
        build_commit_catalog(&[(commit_id, CommitCatalogEntry { commit_object_id })])
            .expect("commit catalog");
    let commit_catalog_root = commit_catalog.root.object_id;
    objects
        .extend(commit_catalog.objects)
        .expect("commit catalog objects");
    let mut change_entries = members
        .iter()
        .enumerate()
        .map(|(ordinal, member)| {
            (
                member.change_id(),
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id,
                        ordinal: u32::try_from(ordinal).expect("seed member ordinal"),
                    },
                },
            )
        })
        .collect::<Vec<_>>();
    change_entries.push((
        ref_change_id,
        ChangeCatalogEntry {
            owner: ChangeCatalogOwner::BranchRef {
                ref_change_object_id,
                branch_id,
            },
        },
    ));
    change_entries.sort_by_key(|(id, _)| *id.as_bytes());
    let change_catalog = build_change_catalog(&change_entries).expect("change catalog");
    let change_catalog_root = change_catalog.root.object_id;
    objects
        .extend(change_catalog.objects)
        .expect("change catalog objects");
    let repository_root = RepositoryRootV1 {
        global_state_root,
        commit_catalog_root,
        change_catalog_root,
    };
    let (repository_root_id, repository_root_bytes) =
        repository_root.encode().expect("repository root");
    objects
        .insert(repository_root_id, repository_root_bytes)
        .expect("repository object");
    let branch_snapshot = BranchSnapshotV1 {
        branch_id,
        local_state_root,
        semantic_head_commit_object_id: commit_object_id,
        latest_ref_change_object_id: Some(ref_change_object_id),
        historical_global_state_root: global_state_root,
    };
    let (branch_snapshot_id, branch_snapshot_bytes) =
        branch_snapshot.encode().expect("branch snapshot");
    objects
        .insert(branch_snapshot_id, branch_snapshot_bytes)
        .expect("branch snapshot object");
    let orphan = BlobChunkV1 {
        bytes: Bytes::from_static(b"unreachable"),
    };
    let (orphan_object_id, orphan_object_bytes) = orphan.encode().expect("orphan");
    objects
        .insert(orphan_object_id, orphan_object_bytes.clone())
        .expect("orphan object");
    SeedData {
        objects,
        branch_id,
        commit_id,
        commit_object_id,
        semantic_change_id,
        semantic_change_object_id,
        ref_change_id,
        ref_change_object_id,
        repository_root_id,
        branch_snapshot_id,
        global_state_root,
        local_state_root,
        global_selector: GlobalSelectorV1 {
            repository_root: repository_root_id,
            epoch: 1,
            selector_generation: 1,
        },
        branch_selector: BranchSelectorV1 {
            branch_id,
            branch_snapshot_object_id: branch_snapshot_id,
            selector_generation: 1,
        },
        state_keys,
        orphan_object_id,
        orphan_object_bytes,
    }
}

fn replace_selected_history_graph(
    seed: &mut SeedData,
    commits: &[(CommitId, CommitCatalogEntry)],
    changes: &[(ChangeId, ChangeCatalogEntry)],
    semantic_head_commit_object_id: ObjectId,
    latest_ref_change_object_id: ObjectId,
) {
    let mut commits = commits.to_vec();
    commits.sort_by_key(|(id, _)| *id.as_bytes());
    let commit_catalog = build_commit_catalog(&commits).expect("replacement commit catalog");
    let commit_catalog_root = commit_catalog.root.object_id;
    seed.objects
        .extend(commit_catalog.objects)
        .expect("replacement commit catalog objects");
    let mut changes = changes.to_vec();
    changes.sort_by_key(|(id, _)| *id.as_bytes());
    let change_catalog = build_change_catalog(&changes).expect("replacement change catalog");
    let change_catalog_root = change_catalog.root.object_id;
    seed.objects
        .extend(change_catalog.objects)
        .expect("replacement change catalog objects");
    let current_repository = RepositoryRootV1::decode(
        seed.repository_root_id,
        seed.objects
            .get(seed.repository_root_id)
            .expect("current repository root"),
    )
    .expect("decode current repository root");
    let repository = RepositoryRootV1 {
        commit_catalog_root,
        change_catalog_root,
        ..current_repository
    };
    let (repository_id, repository_bytes) = repository.encode().expect("replacement repository");
    seed.objects
        .insert(repository_id, repository_bytes)
        .expect("replacement repository object");
    let current_snapshot = BranchSnapshotV1::decode(
        seed.branch_snapshot_id,
        seed.objects
            .get(seed.branch_snapshot_id)
            .expect("current branch snapshot"),
    )
    .expect("decode current branch snapshot");
    let snapshot = BranchSnapshotV1 {
        semantic_head_commit_object_id,
        latest_ref_change_object_id: Some(latest_ref_change_object_id),
        ..current_snapshot
    };
    let (snapshot_id, snapshot_bytes) = snapshot.encode().expect("replacement snapshot");
    seed.objects
        .insert(snapshot_id, snapshot_bytes)
        .expect("replacement snapshot object");
    seed.repository_root_id = repository_id;
    seed.branch_snapshot_id = snapshot_id;
    seed.global_selector.repository_root = repository_id;
    seed.global_selector.selector_generation += 1;
    seed.branch_selector.branch_snapshot_object_id = snapshot_id;
    seed.branch_selector.selector_generation += 1;
}

fn insert_graph_commit(
    seed: &mut SeedData,
    byte: u8,
    generation: u64,
    parent_commit_object_ids: Vec<ObjectId>,
) -> (CommitId, ObjectId) {
    insert_graph_commit_with_options(
        seed,
        byte,
        generation,
        parent_commit_object_ids,
        seed.global_state_root,
        seed.local_state_root,
        false,
        true,
    )
}

fn insert_graph_commit_with_roots(
    seed: &mut SeedData,
    byte: u8,
    generation: u64,
    parent_commit_object_ids: Vec<ObjectId>,
    global_state_root: ObjectId,
    local_state_root: ObjectId,
) -> (CommitId, ObjectId) {
    insert_graph_commit_with_options(
        seed,
        byte,
        generation,
        parent_commit_object_ids,
        global_state_root,
        local_state_root,
        false,
        false,
    )
}

fn insert_graph_commit_with_checkpoint(
    seed: &mut SeedData,
    byte: u8,
    generation: u64,
    parent_commit_object_ids: Vec<ObjectId>,
    introduces_checkpoint: bool,
) -> (CommitId, ObjectId) {
    insert_graph_commit_with_options(
        seed,
        byte,
        generation,
        parent_commit_object_ids,
        seed.global_state_root,
        seed.local_state_root,
        introduces_checkpoint,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn insert_graph_commit_with_options(
    seed: &mut SeedData,
    byte: u8,
    generation: u64,
    parent_commit_object_ids: Vec<ObjectId>,
    global_state_root: ObjectId,
    local_state_root: ObjectId,
    introduces_checkpoint: bool,
    chronology_payload: bool,
) -> (CommitId, ObjectId) {
    let commit_id = CommitId::from_bytes(raw_id(byte));
    let parent_commit_ids = parent_commit_object_ids
        .iter()
        .map(|parent_id| {
            let parent = CommitObjectV1::decode(
                *parent_id,
                seed.objects.get(*parent_id).expect("graph parent object"),
            )
            .expect("graph parent commit");
            crate::changelog::CommitId::new(uuid::Uuid::from_bytes(*parent.commit_id.as_bytes()))
        })
        .collect();
    let checkpoint_cursor = match parent_commit_object_ids.first().copied() {
        None => CheckpointCursorV1::root(),
        Some(parent_id) => {
            let parent = CommitObjectV1::decode(
                parent_id,
                seed.objects.get(parent_id).expect("graph parent object"),
            )
            .expect("graph parent commit");
            CheckpointCursorV1::after_first_parent(
                parent_id,
                &parent,
                seed.branch_id,
                introduces_checkpoint,
            )
            .expect("graph checkpoint cursor")
        }
    };
    let members = if chronology_payload {
        chronology_checkpoint_members(seed, commit_id, introduces_checkpoint)
    } else {
        Vec::new()
    };
    let pages = CommitChangePageV3::encode_pages(commit_id, &members)
        .expect("graph chronology member pages");
    for (id, bytes) in &pages.objects {
        seed.objects
            .insert(*id, bytes.clone())
            .expect("graph chronology member page");
    }
    let commit = CommitObjectV1 {
        commit_id,
        generation,
        parent_commit_object_ids,
        members: Vec::new(),
        member_page_object_ids: pages.objects.iter().map(|(id, _)| *id).collect(),
        global_state_root,
        local_state_root,
        checkpoint_cursor,
        metadata: if chronology_payload {
            chronology_commit_metadata(byte, generation, parent_commit_ids)
        } else {
            format!("graph-{byte:02x}").into_bytes()
        },
    };
    let (object_id, bytes) = commit.encode().expect("graph commit");
    seed.objects
        .insert(object_id, bytes)
        .expect("graph commit object");
    (commit_id, object_id)
}

fn indexed_chronology_id(index: u32) -> CommitId {
    let mut bytes = [0_u8; 16];
    bytes[0] = 0xc7;
    bytes[12..].copy_from_slice(&index.to_be_bytes());
    CommitId::from_bytes(bytes)
}

fn insert_indexed_chronology_commit(
    seed: &mut SeedData,
    index: u32,
    parent: Option<(ObjectId, &CommitObjectV1)>,
    introduces_checkpoint: bool,
) -> (CommitId, ObjectId, CommitObjectV1) {
    let commit_id = indexed_chronology_id(index);
    let generation = u64::from(index) + 1;
    let (parent_commit_object_ids, parent_commit_ids, checkpoint_cursor) = match parent {
        None => (Vec::new(), Vec::new(), CheckpointCursorV1::root()),
        Some((parent_id, parent)) => (
            vec![parent_id],
            vec![crate::changelog::CommitId::new(uuid::Uuid::from_bytes(
                *parent.commit_id.as_bytes(),
            ))],
            CheckpointCursorV1::after_first_parent(
                parent_id,
                parent,
                seed.branch_id,
                introduces_checkpoint,
            )
            .expect("indexed checkpoint cursor"),
        ),
    };
    let members = chronology_checkpoint_members(seed, commit_id, introduces_checkpoint);
    let pages = CommitChangePageV3::encode_pages(commit_id, &members)
        .expect("indexed chronology member pages");
    for (id, bytes) in &pages.objects {
        seed.objects
            .insert(*id, bytes.clone())
            .expect("indexed chronology member page");
    }
    let commit = CommitObjectV1 {
        commit_id,
        generation,
        parent_commit_object_ids,
        members: Vec::new(),
        member_page_object_ids: pages.objects.iter().map(|(id, _)| *id).collect(),
        global_state_root: seed.global_state_root,
        local_state_root: seed.local_state_root,
        checkpoint_cursor,
        metadata: chronology_commit_metadata_for(commit_id, generation, parent_commit_ids),
    };
    let (object_id, bytes) = commit.encode().expect("indexed chronology commit");
    seed.objects
        .insert(object_id, bytes)
        .expect("indexed chronology object");
    (commit_id, object_id, commit)
}

fn chronology_checkpoint_members(
    seed: &SeedData,
    commit_id: CommitId,
    introduces_checkpoint: bool,
) -> Vec<CommitMemberV3> {
    if !introduces_checkpoint {
        return Vec::new();
    }
    let mut change_bytes = *commit_id.as_bytes();
    change_bytes[1] ^= 0x5a;
    let change_id = ChangeId::from_bytes(change_bytes);
    let branch_id = uuid::Uuid::from_bytes(*seed.branch_id.as_bytes()).to_string();
    let row_pk = RowPk::uuid_from_canonical(&branch_id).expect("checkpoint branch UUID");
    let key = encode_state_key(StateKeyRef {
        schema_key: crate::checkpoint::CHECKPOINT_MARKER_SCHEMA_KEY,
        file_id: None,
        row_pk: &row_pk,
    });
    let cell = StateCell::NativeRow(
        crate::native_row::encode(
            &crate::native_row::seed_schema(crate::checkpoint::CHECKPOINT_MARKER_SCHEMA_KEY)
                .expect("checkpoint schema"),
            &row_pk,
            false,
            None,
            &serde_json::json!({ "branch_id": branch_id }),
        )
        .expect("checkpoint native row"),
    );
    vec![test_state_entry(
        key,
        change_id,
        cell,
        false,
        Vec::new(),
    )
    .member]
}

fn install_graph_head(
    seed: &mut SeedData,
    commits: &[(CommitId, ObjectId)],
    head_object_id: ObjectId,
    ref_byte: u8,
) {
    let ref_change = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(ref_byte)),
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(i64::from(ref_byte)),
        branch_id: seed.branch_id,
        before_semantic_head_commit_object_id: Some(seed.commit_object_id),
        after_semantic_head_commit_object_id: Some(head_object_id),
        previous_ref_change_object_id: Some(seed.ref_change_object_id),
        payload: b"graph-head".to_vec(),
        json_payload_object_ids: Vec::new(),
    };
    let (ref_object_id, ref_bytes) = ref_change.encode().expect("graph ref");
    seed.objects
        .insert(ref_object_id, ref_bytes)
        .expect("graph ref object");

    let mut commit_entries = vec![(
        seed.commit_id,
        CommitCatalogEntry {
            commit_object_id: seed.commit_object_id,
        },
    )];
    commit_entries.extend(commits.iter().map(|(commit_id, object_id)| {
        (
            *commit_id,
            CommitCatalogEntry {
                commit_object_id: *object_id,
            },
        )
    }));
    let mut change_entries = seed_member_catalog_entries(seed, seed.commit_object_id);
    for (_, commit_object_id) in commits {
        change_entries.extend(commit_member_catalog_entries(seed, *commit_object_id));
    }
    change_entries.push((
        seed.ref_change_id,
        ChangeCatalogEntry {
            owner: ChangeCatalogOwner::BranchRef {
                ref_change_object_id: seed.ref_change_object_id,
                branch_id: seed.branch_id,
            },
        },
    ));
    change_entries.push((
        ref_change.change_id(),
        ChangeCatalogEntry {
            owner: ChangeCatalogOwner::BranchRef {
                ref_change_object_id: ref_object_id,
                branch_id: seed.branch_id,
            },
        },
    ));
    replace_selected_history_graph(
        seed,
        &commit_entries,
        &change_entries,
        head_object_id,
        ref_object_id,
    );
}

async fn seed_storage<S>(storage: &S, seed: &SeedData)
where
    S: Storage,
{
    let mut writes = StorageWriteSet::new();
    for (id, bytes) in seed.objects.iter() {
        writes.put(OBJECT_SPACE, id.as_bytes().to_vec(), bytes.to_vec());
    }
    writes.put(
        SELECTOR_SPACE,
        global_selector_key().to_vec(),
        seed.global_selector
            .encode()
            .expect("global selector")
            .to_vec(),
    );
    writes.put(
        SELECTOR_SPACE,
        branch_selector_key(seed.branch_id).to_vec(),
        seed.branch_selector
            .encode()
            .expect("branch selector")
            .to_vec(),
    );
    commit_write_set_for_test(writes, storage).await;
}

fn build_checkpoint_chronology_seed() -> (SeedData, [(CommitId, ObjectId); 4]) {
    let mut seed = build_seed();
    let root = insert_graph_commit_with_checkpoint(&mut seed, 0x90, 1, Vec::new(), false);
    let ordinary = insert_graph_commit_with_checkpoint(&mut seed, 0x91, 2, vec![root.1], false);
    let checkpoint =
        insert_graph_commit_with_checkpoint(&mut seed, 0x92, 3, vec![ordinary.1], true);
    let head = insert_graph_commit_with_checkpoint(&mut seed, 0x93, 4, vec![checkpoint.1], false);
    let commits = [root, ordinary, checkpoint, head];
    install_graph_head(&mut seed, &commits, head.1, 0x94);
    (seed, commits)
}

async fn checkpoint_history_from_seed(
    seed: &SeedData,
) -> Result<Vec<super::view::CheckpointHistoryEntry>, LixError> {
    let storage = Memory::new();
    seed_storage(&storage, seed).await;
    let adapter = StorageAdapter::new(storage);
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("one retained checkpoint read");
    let branch_id = uuid::Uuid::from_bytes(*seed.branch_id.as_bytes()).to_string();
    Ok(ForkTreeReadFacade::new(read)
        .checkpoint_history_for_branch(&branch_id, None, None, Some(1))
        .await?
        .entries)
}

#[tokio::test]
async fn checkpoint_chronology_authenticates_branch_target_commit_and_presence() {
    let (seed, _) = build_checkpoint_chronology_seed();
    let history = checkpoint_history_from_seed(&seed)
        .await
        .expect("selector-bound latest checkpoint");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].commit_id, public_commit_id(0x92));
    assert_eq!(history[0].depth, 1);

    let (mut substituted_target, commits) = build_checkpoint_chronology_seed();
    let replacement_checkpoint = insert_graph_commit_with_checkpoint(
        &mut substituted_target,
        0x98,
        3,
        vec![commits[1].1],
        true,
    );
    let current_head = CommitObjectV1::decode(
        commits[3].1,
        substituted_target
            .objects
            .get(commits[3].1)
            .expect("chronology head"),
    )
    .expect("decode chronology head");
    let substituted_head = CommitObjectV1 {
        checkpoint_cursor: CheckpointCursorV1::Ordinary {
            owner_branch_id: substituted_target.branch_id,
            root_commit_object_id: commits[0].1,
            distance_to_root: 3,
            latest_checkpoint_object_id: replacement_checkpoint.1,
            distance_to_latest: 1,
        },
        ..current_head
    };
    let (substituted_head_id, substituted_head_bytes) = substituted_head
        .encode()
        .expect("substituted chronology head");
    substituted_target
        .objects
        .insert(substituted_head_id, substituted_head_bytes)
        .expect("substituted chronology head object");
    install_graph_head(
        &mut substituted_target,
        &[
            commits[0],
            commits[1],
            commits[2],
            replacement_checkpoint,
            (substituted_head.commit_id, substituted_head_id),
        ],
        substituted_head_id,
        0x95,
    );
    let error = checkpoint_history_from_seed(&substituted_target)
        .await
        .expect_err("a coherent checkpoint target substitution must fail derivation");
    assert!(
        error.to_string().contains("does not derive"),
        "substituted checkpoint target must fail closed: {error}"
    );

    let (mut substituted_root, commits) = build_checkpoint_chronology_seed();
    let replacement_root =
        insert_graph_commit_with_checkpoint(&mut substituted_root, 0x99, 1, Vec::new(), false);
    let current_head = CommitObjectV1::decode(
        commits[3].1,
        substituted_root
            .objects
            .get(commits[3].1)
            .expect("chronology head"),
    )
    .expect("decode chronology head");
    let substituted_head = CommitObjectV1 {
        checkpoint_cursor: CheckpointCursorV1::Ordinary {
            owner_branch_id: substituted_root.branch_id,
            root_commit_object_id: replacement_root.1,
            distance_to_root: 3,
            latest_checkpoint_object_id: commits[2].1,
            distance_to_latest: 1,
        },
        ..current_head
    };
    let (substituted_head_id, substituted_head_bytes) = substituted_head
        .encode()
        .expect("root-substituted chronology head");
    substituted_root
        .objects
        .insert(substituted_head_id, substituted_head_bytes)
        .expect("root-substituted chronology head object");
    install_graph_head(
        &mut substituted_root,
        &[
            replacement_root,
            commits[0],
            commits[1],
            commits[2],
            (substituted_head.commit_id, substituted_head_id),
        ],
        substituted_head_id,
        0x96,
    );
    let error = checkpoint_history_from_seed(&substituted_root)
        .await
        .expect_err("a coherent repository-root substitution must fail derivation");
    assert!(
        error.to_string().contains("does not derive"),
        "substituted chronology root must fail closed: {error}"
    );

    let (mut wrong_branch, commits) = build_checkpoint_chronology_seed();
    let checkpoint = CommitObjectV1::decode(
        commits[2].1,
        wrong_branch
            .objects
            .get(commits[2].1)
            .expect("chronology checkpoint"),
    )
    .expect("decode chronology checkpoint");
    let foreign_checkpoint = CommitObjectV1 {
        checkpoint_cursor: CheckpointCursorV1::Checkpoint {
            owner_branch_id: CanonicalBranchId::from_bytes(raw_id(0x55)),
            root_commit_object_id: commits[0].1,
            distance_to_root: 2,
            previous_checkpoint_object_id: commits[0].1,
            distance_to_previous: 2,
        },
        ..checkpoint
    };
    let (foreign_checkpoint_id, foreign_checkpoint_bytes) =
        foreign_checkpoint.encode().expect("foreign checkpoint");
    wrong_branch
        .objects
        .insert(foreign_checkpoint_id, foreign_checkpoint_bytes)
        .expect("foreign checkpoint object");
    let head = CommitObjectV1 {
        parent_commit_object_ids: vec![foreign_checkpoint_id],
        checkpoint_cursor: CheckpointCursorV1::Ordinary {
            owner_branch_id: wrong_branch.branch_id,
            root_commit_object_id: commits[0].1,
            distance_to_root: 3,
            latest_checkpoint_object_id: foreign_checkpoint_id,
            distance_to_latest: 1,
        },
        metadata: chronology_commit_metadata(0x93, 4, vec![public_commit_id(0x92)]),
        ..CommitObjectV1::decode(
            commits[3].1,
            wrong_branch
                .objects
                .get(commits[3].1)
                .expect("chronology head"),
        )
        .expect("decode chronology head")
    };
    let (head_id, head_bytes) = head.encode().expect("wrong-branch head");
    wrong_branch
        .objects
        .insert(head_id, head_bytes)
        .expect("wrong-branch head object");
    install_graph_head(
        &mut wrong_branch,
        &[
            commits[0],
            commits[1],
            (foreign_checkpoint.commit_id, foreign_checkpoint_id),
            (head.commit_id, head_id),
        ],
        head_id,
        0xa5,
    );
    let error = checkpoint_history_from_seed(&wrong_branch)
        .await
        .expect_err("a branch-owned cursor cannot target another branch's checkpoint");
    assert!(
        error.to_string().contains("does not derive")
            || error
                .to_string()
                .contains("not owned by the selected branch")
    );

    let (mut wrong_commit, commits) = build_checkpoint_chronology_seed();
    let checkpoint = CommitObjectV1::decode(
        commits[2].1,
        wrong_commit
            .objects
            .get(commits[2].1)
            .expect("chronology checkpoint"),
    )
    .expect("decode chronology checkpoint");
    let substituted_checkpoint = CommitObjectV1 {
        metadata: {
            let mut record = crate::changelog::decode_forktree_commit_payload(&checkpoint.metadata)
                .expect("checkpoint metadata");
            record.account_id = "substituted-owner".to_owned();
            crate::changelog::encode_forktree_commit_payload(&record)
                .expect("substituted checkpoint metadata")
        },
        ..checkpoint
    };
    let (substituted_id, substituted_bytes) = substituted_checkpoint
        .encode()
        .expect("substituted checkpoint");
    wrong_commit
        .objects
        .insert(substituted_id, substituted_bytes)
        .expect("substituted checkpoint object");
    install_graph_head(
        &mut wrong_commit,
        &[
            commits[0],
            commits[1],
            (substituted_checkpoint.commit_id, substituted_id),
            commits[3],
        ],
        commits[3].1,
        0xa6,
    );
    let error = checkpoint_history_from_seed(&wrong_commit)
        .await
        .expect_err("a chronology target must match its CommitCatalog back-edge");
    assert!(error.to_string().contains("CommitCatalog"));

    let (mut wrong_kind, commits) = build_checkpoint_chronology_seed();
    let current_head = CommitObjectV1::decode(
        commits[3].1,
        wrong_kind
            .objects
            .get(commits[3].1)
            .expect("chronology head"),
    )
    .expect("decode chronology head");
    let wrong_kind_head = CommitObjectV1 {
        checkpoint_cursor: CheckpointCursorV1::Ordinary {
            owner_branch_id: wrong_kind.branch_id,
            root_commit_object_id: commits[0].1,
            distance_to_root: 3,
            latest_checkpoint_object_id: commits[1].1,
            distance_to_latest: 2,
        },
        ..current_head
    };
    let (wrong_kind_head_id, wrong_kind_head_bytes) =
        wrong_kind_head.encode().expect("wrong-kind head");
    wrong_kind
        .objects
        .insert(wrong_kind_head_id, wrong_kind_head_bytes)
        .expect("wrong-kind head object");
    install_graph_head(
        &mut wrong_kind,
        &[
            commits[0],
            commits[1],
            commits[2],
            (wrong_kind_head.commit_id, wrong_kind_head_id),
        ],
        wrong_kind_head_id,
        0xa7,
    );
    let error = checkpoint_history_from_seed(&wrong_kind)
        .await
        .expect_err("an ordinary commit cannot be substituted for a checkpoint target");
    assert!(
        error.to_string().contains("does not derive")
            || error
                .to_string()
                .contains("not owned by the selected branch")
    );

    let (mut missing, commits) = build_checkpoint_chronology_seed();
    missing.objects.remove(commits[2].1);
    let error = checkpoint_history_from_seed(&missing)
        .await
        .expect_err("a missing sealed checkpoint target must fail closed");
    assert!(
        error.to_string().contains("missing")
            || error.to_string().contains("absent")
            || error.to_string().contains("not found"),
        "missing checkpoint error must remain fail-closed: {error}"
    );

    let (mut missing_page, commits) = build_checkpoint_chronology_seed();
    let checkpoint = CommitObjectV1::decode(
        commits[2].1,
        missing_page
            .objects
            .get(commits[2].1)
            .expect("chronology checkpoint"),
    )
    .expect("decode chronology checkpoint");
    let page_id = *checkpoint
        .member_page_object_ids
        .first()
        .expect("checkpoint marker page");
    missing_page.objects.remove(page_id);
    let error = checkpoint_history_from_seed(&missing_page)
        .await
        .expect_err("a missing checkpoint marker page must fail closed");
    assert!(
        error.to_string().contains("missing")
            || error.to_string().contains("absent")
            || error.to_string().contains("not found"),
        "missing marker page must remain fail-closed: {error}"
    );

    let (mut corrupt_page, commits) = build_checkpoint_chronology_seed();
    let checkpoint = CommitObjectV1::decode(
        commits[2].1,
        corrupt_page
            .objects
            .get(commits[2].1)
            .expect("chronology checkpoint"),
    )
    .expect("decode chronology checkpoint");
    let page_id = *checkpoint
        .member_page_object_ids
        .first()
        .expect("checkpoint marker page");
    corrupt_page.objects.remove(page_id);
    corrupt_page
        .objects
        .insert(page_id, Bytes::from_static(b"wrong-domain checkpoint page"))
        .expect("corrupt checkpoint marker page");
    checkpoint_history_from_seed(&corrupt_page)
        .await
        .expect_err("a malformed checkpoint marker page must fail closed");

    let (mut cycle_attempt, commits) = build_checkpoint_chronology_seed();
    let checkpoint = CommitObjectV1::decode(
        commits[2].1,
        cycle_attempt
            .objects
            .get(commits[2].1)
            .expect("chronology checkpoint"),
    )
    .expect("decode chronology checkpoint");
    let cycle_checkpoint = CommitObjectV1 {
        checkpoint_cursor: CheckpointCursorV1::Checkpoint {
            owner_branch_id: cycle_attempt.branch_id,
            root_commit_object_id: commits[0].1,
            distance_to_root: 2,
            previous_checkpoint_object_id: commits[3].1,
            distance_to_previous: 1,
        },
        ..checkpoint
    };
    let (cycle_checkpoint_id, cycle_checkpoint_bytes) =
        cycle_checkpoint.encode().expect("cycle-shaped checkpoint");
    cycle_attempt
        .objects
        .insert(cycle_checkpoint_id, cycle_checkpoint_bytes)
        .expect("cycle-shaped checkpoint object");
    let current_head = CommitObjectV1::decode(
        commits[3].1,
        cycle_attempt
            .objects
            .get(commits[3].1)
            .expect("chronology head"),
    )
    .expect("decode chronology head");
    let cycle_head = CommitObjectV1 {
        parent_commit_object_ids: vec![cycle_checkpoint_id],
        checkpoint_cursor: CheckpointCursorV1::after_first_parent(
            cycle_checkpoint_id,
            &cycle_checkpoint,
            cycle_attempt.branch_id,
            false,
        )
        .expect("head after cycle-shaped checkpoint"),
        ..current_head
    };
    let (cycle_head_id, cycle_head_bytes) = cycle_head.encode().expect("cycle-shaped head");
    cycle_attempt
        .objects
        .insert(cycle_head_id, cycle_head_bytes)
        .expect("cycle-shaped head object");
    install_graph_head(
        &mut cycle_attempt,
        &[
            commits[0],
            commits[1],
            (cycle_checkpoint.commit_id, cycle_checkpoint_id),
            (cycle_head.commit_id, cycle_head_id),
        ],
        cycle_head_id,
        0xa8,
    );
    let error = checkpoint_history_from_seed(&cycle_attempt)
        .await
        .expect_err("a cycle-shaped stale back-edge must fail before traversal");
    assert!(
        error.to_string().contains("does not derive")
            || error.to_string().contains("CommitCatalog"),
        "cycle-shaped chronology must fail closed: {error}"
    );
}

fn build_checkpoint_height_seed(
    height: u32,
) -> (
    SeedData,
    Vec<(CommitId, ObjectId)>,
    crate::changelog::CommitId,
) {
    assert!(height >= 2);
    let mut seed = build_seed();
    let root = insert_indexed_chronology_commit(&mut seed, 0, None, false);
    let checkpoint = insert_indexed_chronology_commit(&mut seed, 1, Some((root.1, &root.2)), true);
    let checkpoint_id =
        crate::changelog::CommitId::new(uuid::Uuid::from_bytes(*checkpoint.0.as_bytes()));
    let mut commits = vec![(root.0, root.1), (checkpoint.0, checkpoint.1)];
    let mut parent = checkpoint;
    for index in 2..=height {
        let current =
            insert_indexed_chronology_commit(&mut seed, index, Some((parent.1, &parent.2)), false);
        commits.push((current.0, current.1));
        parent = current;
    }
    install_graph_head(&mut seed, &commits, parent.1, 0xe1);
    (seed, commits, checkpoint_id)
}

fn build_checkpoint_interval_seed(
    height: u32,
    interval: u32,
) -> (SeedData, Vec<(CommitId, ObjectId)>) {
    assert!(height > 0);
    assert!(interval > 0);
    let mut seed = build_seed();
    let root = insert_indexed_chronology_commit(&mut seed, 0, None, false);
    let mut commits = vec![(root.0, root.1)];
    let mut parent = root;
    for index in 1..=height {
        let current = insert_indexed_chronology_commit(
            &mut seed,
            index,
            Some((parent.1, &parent.2)),
            index % interval == 0,
        );
        commits.push((current.0, current.1));
        parent = current;
    }
    install_graph_head(&mut seed, &commits, parent.1, 0xe2);
    (seed, commits)
}

#[tokio::test]
async fn latest_checkpoint_loads_are_bounded_at_heights_10_100_and_1000() {
    let mut observed_reads = None;
    let mut observed_bytes = Vec::new();
    let mut observed_calls = Vec::new();
    let mut observed_keys = Vec::new();
    let mut observed_total_bytes = Vec::new();
    for height in [10_u32, 100, 1_000] {
        let (seed, commits, checkpoint_id) = build_checkpoint_height_seed(height);
        let commit_bytes = Arc::new(
            commits
                .iter()
                .map(|(_, object_id)| {
                    (
                        object_id.as_bytes().to_vec(),
                        seed.objects
                            .get(*object_id)
                            .expect("chronology commit object")
                            .len(),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        );
        let storage = Memory::new();
        seed_storage(&storage, &seed).await;
        let commit_reads = Arc::new(AtomicUsize::new(0));
        let requested_commit_bytes = Arc::new(AtomicUsize::new(0));
        let get_many_calls = Arc::new(AtomicUsize::new(0));
        let requested_keys = Arc::new(AtomicUsize::new(0));
        let returned_bytes = Arc::new(AtomicUsize::new(0));
        let read = ChronologyCountingRead {
            inner: StorageAdapterReadScope::new(
                storage
                    .begin_read(ReadOptions::default())
                    .await
                    .expect("one retained chronology read"),
            ),
            commit_bytes,
            commit_reads: Arc::clone(&commit_reads),
            requested_commit_bytes: Arc::clone(&requested_commit_bytes),
            get_many_calls: Arc::clone(&get_many_calls),
            requested_keys: Arc::clone(&requested_keys),
            returned_bytes: Arc::clone(&returned_bytes),
        };
        let branch_id = uuid::Uuid::from_bytes(*seed.branch_id.as_bytes()).to_string();
        let latest = ForkTreeReadFacade::new(read)
            .checkpoint_history_for_branch(&branch_id, None, None, Some(1))
            .await
            .expect("bounded latest checkpoint")
            .entries;
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].commit_id, checkpoint_id);
        assert_eq!(latest[0].depth, height - 1);

        let reads = commit_reads.load(Ordering::Relaxed);
        let bytes = requested_commit_bytes.load(Ordering::Relaxed);
        assert!(reads <= 8, "height {height} used {reads} Commit loads");
        assert_eq!(
            observed_reads.get_or_insert(reads),
            &reads,
            "LIMIT 1 Commit loads must not grow with history height"
        );
        observed_bytes.push(bytes);
        observed_calls.push(get_many_calls.load(Ordering::Relaxed));
        observed_keys.push(requested_keys.load(Ordering::Relaxed));
        observed_total_bytes.push(returned_bytes.load(Ordering::Relaxed));
    }
    let minimum = *observed_bytes.iter().min().expect("bounded byte samples");
    let maximum = *observed_bytes.iter().max().expect("bounded byte samples");
    assert!(
        maximum <= minimum + 64,
        "LIMIT 1 Commit bytes must remain fixed-size in H: {observed_bytes:?}"
    );
    let minimum_calls = *observed_calls.iter().min().expect("bounded call samples");
    let maximum_calls = *observed_calls.iter().max().expect("bounded call samples");
    assert!(
        maximum_calls <= minimum_calls + 4,
        "LIMIT 1 total get_many calls may add one catalog-tree level but must not scale with H: {observed_calls:?}"
    );
    let minimum_keys = *observed_keys.iter().min().expect("bounded key samples");
    let maximum_keys = *observed_keys.iter().max().expect("bounded key samples");
    assert!(
        maximum_keys <= minimum_keys + 4,
        "LIMIT 1 requested keys may add one catalog-tree level but must not scale with H: {observed_keys:?}"
    );
    let total_minimum = *observed_total_bytes
        .iter()
        .min()
        .expect("bounded total-byte samples");
    let total_maximum = *observed_total_bytes
        .iter()
        .max()
        .expect("bounded total-byte samples");
    assert!(
        total_maximum <= total_minimum.saturating_mul(4),
        "LIMIT 1 authenticated bytes must stay page-bounded rather than scale with H: {observed_total_bytes:?}"
    );
    eprintln!(
        "checkpoint LIMIT 1 H=[10,100,1000] calls={observed_calls:?} keys={observed_keys:?} returned_bytes={observed_total_bytes:?}"
    );
}

#[tokio::test]
async fn depth_filtered_limit_visits_checkpoint_edges_not_ordinary_commits() {
    let (seed, commits) = build_checkpoint_interval_seed(100, 10);
    let commit_bytes = Arc::new(
        commits
            .iter()
            .map(|(_, object_id)| {
                (
                    object_id.as_bytes().to_vec(),
                    seed.objects
                        .get(*object_id)
                        .expect("chronology commit object")
                        .len(),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    );
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let commit_reads = Arc::new(AtomicUsize::new(0));
    let requested_commit_bytes = Arc::new(AtomicUsize::new(0));
    let get_many_calls = Arc::new(AtomicUsize::new(0));
    let requested_keys = Arc::new(AtomicUsize::new(0));
    let returned_bytes = Arc::new(AtomicUsize::new(0));
    let read = ChronologyCountingRead {
        inner: StorageAdapterReadScope::new(
            storage
                .begin_read(ReadOptions::default())
                .await
                .expect("one retained chronology read"),
        ),
        commit_bytes,
        commit_reads: Arc::clone(&commit_reads),
        requested_commit_bytes,
        get_many_calls: Arc::clone(&get_many_calls),
        requested_keys: Arc::clone(&requested_keys),
        returned_bytes: Arc::clone(&returned_bytes),
    };
    let branch_id = uuid::Uuid::from_bytes(*seed.branch_id.as_bytes()).to_string();
    let history = ForkTreeReadFacade::new(read)
        .checkpoint_history_for_branch(&branch_id, Some(50), None, Some(1))
        .await
        .expect("depth-filtered checkpoint history")
        .entries;
    assert_eq!(history.len(), 1);
    assert_eq!(
        history[0].commit_id,
        crate::changelog::CommitId::new(uuid::Uuid::from_bytes(
            *indexed_chronology_id(50).as_bytes(),
        ))
    );
    assert_eq!(history[0].depth, 50);

    let reads = commit_reads.load(Ordering::Relaxed);
    let calls = get_many_calls.load(Ordering::Relaxed);
    let keys = requested_keys.load(Ordering::Relaxed);
    let bytes = returned_bytes.load(Ordering::Relaxed);
    assert!(
        reads <= 16,
        "six visited checkpoint envelopes must not load 50 ordinary commits: {reads}"
    );
    assert!(
        calls <= 64 && keys <= 64,
        "filtered LIMIT 1 must scale with visited checkpoint edges: calls={calls} keys={keys}"
    );
    eprintln!(
        "checkpoint filtered LIMIT 1 visited=6 calls={calls} keys={keys} returned_bytes={bytes}"
    );
}

#[tokio::test]
async fn checkpoint_chronology_survives_gc_and_cold_reopen() {
    let (seed, _, checkpoint_id) = build_checkpoint_height_seed(100);
    let storage = CrashStorage::new();
    seed_storage(&storage, &seed).await;
    sweep(&storage, seed.branch_id).await;

    let reopened = storage.reopen();
    let adapter = StorageAdapter::new(reopened);
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("cold-reopened chronology read");
    let branch_id = uuid::Uuid::from_bytes(*seed.branch_id.as_bytes()).to_string();
    let latest = ForkTreeReadFacade::new(read)
        .checkpoint_history_for_branch(&branch_id, None, None, Some(1))
        .await
        .expect("checkpoint after GC and cold reopen")
        .entries;
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].commit_id, checkpoint_id);
    assert_eq!(latest[0].depth, 99);
}

fn diff_state_rows(values: &[(usize, u8)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = values
        .iter()
        .map(|(index, value)| {
            let row_pk = RowPk::single(format!("row-{index:05}"));
            let key = encode_state_key(StateKeyRef {
                schema_key: "diff.row",
                file_id: Some("file"),
                row_pk: &row_pk,
            });
            let value = encode_state_value(StateValueRef {
                pack_object_id: content_id(value.saturating_add(1)),
                pack_ordinal: *index as u32,
            })
            .expect("state diff value");
            (key, value)
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows
}

fn build_diff_tree(values: &[(usize, u8)]) -> super::tree::TreeBuild {
    build_state_tree(&diff_state_rows(values)).expect("state diff tree")
}

async fn seed_diff_trees<S>(storage: &S, trees: &[&super::tree::TreeBuild])
where
    S: Storage,
{
    let mut writes = StorageWriteSet::new();
    for tree in trees {
        for (id, bytes) in tree.objects.iter() {
            writes.put(OBJECT_SPACE, id.as_bytes().to_vec(), bytes.to_vec());
        }
    }
    commit_write_set_for_test(writes, storage).await;
}

fn expected_diff_trees(
    left: &super::tree::TreeBuild,
    right: &super::tree::TreeBuild,
) -> BTreeSet<Vec<u8>> {
    let left = scan_all(left.root.object_id, "state", load_from(&left.objects))
        .expect("full-scan left oracle")
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let right = scan_all(right.root.object_id, "state", load_from(&right.objects))
        .expect("full-scan right oracle")
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    left.keys()
        .chain(right.keys())
        .filter(|key| left.get(*key) != right.get(*key))
        .cloned()
        .collect()
}

fn encoded_state_keys(keys: &[StateKey]) -> Vec<Vec<u8>> {
    keys.iter()
        .map(|key| {
            encode_state_key(StateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                row_pk: &key.row_pk,
            })
        })
        .collect()
}

fn load_from(
    objects: &ImmutableObjectSet,
) -> impl Fn(ObjectId) -> Result<Bytes, StorageError> + '_ {
    move |id| {
        objects
            .get(id)
            .cloned()
            .ok_or_else(|| StorageError::Corruption(format!("test object {id} is absent")))
    }
}

fn seed_commit_members(seed: &SeedData) -> Vec<CommitMemberV3> {
    let commit = CommitObjectV1::decode(
        seed.commit_object_id,
        seed.objects
            .get(seed.commit_object_id)
            .expect("seed commit"),
    )
    .expect("decode seed commit");
    commit
        .load_members_with(load_from(&seed.objects))
        .expect("load seed members")
}

fn seed_member_catalog_entries(
    seed: &SeedData,
    commit_object_id: ObjectId,
) -> Vec<(ChangeId, ChangeCatalogEntry)> {
    commit_member_catalog_entries(seed, commit_object_id)
}

fn commit_member_catalog_entries(
    seed: &SeedData,
    commit_object_id: ObjectId,
) -> Vec<(ChangeId, ChangeCatalogEntry)> {
    let commit = CommitObjectV1::decode(
        commit_object_id,
        seed.objects
            .get(commit_object_id)
            .expect("cataloged commit object"),
    )
    .expect("decode cataloged commit");
    commit
        .load_members_with(load_from(&seed.objects))
        .expect("load cataloged commit members")
        .into_iter()
        .enumerate()
        .map(|(ordinal, member)| {
            (
                member.change_id(),
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id,
                        ordinal: u32::try_from(ordinal).expect("seed member ordinal"),
                    },
                },
            )
        })
        .collect()
}

fn insert_test_change_pages(
    objects: &mut ImmutableObjectSet,
    commit_id: CommitId,
    members: &[CommitMemberV3],
) -> Vec<ObjectId> {
    let pages = CommitChangePageV3::encode_pages(commit_id, members).expect("test change pages");
    let ids = pages.objects.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    for (id, bytes) in pages.objects {
        objects.insert(id, bytes).expect("test change page object");
    }
    ids
}

async fn object_present<S: Storage>(storage: &S, id: ObjectId) -> bool {
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("object read");
    let keys = [Key(Bytes::copy_from_slice(id.as_bytes()))];
    read.get_many(&[GetManyRequest {
        space: OBJECT_SPACE,
        keys: &keys,
        opts: GetOptions {
            projection: CoreProjection::FullValue,
        },
    }])
    .await
    .expect("object point")
    .values[0]
        .is_some()
}

async fn selector_present<S: Storage>(storage: &S, key: Bytes) -> bool {
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("selector read");
    read.get_many(&[GetManyRequest {
        space: SELECTOR_SPACE,
        keys: &[Key(key)],
        opts: GetOptions {
            projection: CoreProjection::FullValue,
        },
    }])
    .await
    .expect("selector point")
    .values[0]
        .is_some()
}

async fn sweep_result<S: Storage>(storage: &S) -> Result<(), StorageError> {
    for _ in 0..20_000 {
        if matches!(
            advance_gc(storage, GcBudget::default()).await?,
            GcStepStatus::Complete { .. }
        ) {
            return Ok(());
        }
    }
    Err(StorageError::Corruption(
        "bounded GC did not finish within the test step ceiling".to_owned(),
    ))
}

async fn sweep<S: Storage>(storage: &S, _branch_id: CanonicalBranchId) {
    sweep_result(storage).await.expect("bounded sweep");
}

async fn branch_transition<R: StorageAdapterRead>(
    view: &CoherentView<R>,
    state_edit: super::serving::StateTreeEdit,
    identity: u8,
) -> BranchStateTransition {
    branch_transition_with_members(view, state_edit, identity, Vec::new()).await
}

async fn branch_transition_with_members<R: StorageAdapterRead>(
    view: &CoherentView<R>,
    state_edit: super::serving::StateTreeEdit,
    identity: u8,
    members: Vec<CommitMemberV3>,
) -> BranchStateTransition {
    let commit_id = CommitId::from_bytes(raw_id(identity));
    let member_page_object_ids = if members.is_empty() {
        Vec::new()
    } else {
        CommitChangePageV3::encode_pages(commit_id, &members)
            .expect("transition change pages")
            .objects
            .iter()
            .map(|(id, _)| *id)
            .collect()
    };
    let parent_id = view.branch_snapshot().semantic_head_commit_object_id;
    let parent_bytes = view
        .load_object_bytes(parent_id)
        .await
        .expect("transition parent object");
    let parent =
        CommitObjectV1::decode(parent_id, &parent_bytes).expect("transition parent commit");
    let semantic_commit = CommitObjectV1 {
        commit_id,
        generation: identity as u64,
        parent_commit_object_ids: vec![parent_id],
        members,
        member_page_object_ids,
        global_state_root: view.repository_root().global_state_root,
        local_state_root: state_edit.root,
        checkpoint_cursor: CheckpointCursorV1::after_first_parent(
            parent_id,
            &parent,
            view.branch_id(),
            false,
        )
        .expect("transition checkpoint cursor"),
        metadata: vec![identity],
    };
    let (commit_object_id, _) = semantic_commit.encode().expect("next commit");
    let ref_change = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(identity.wrapping_add(1))),
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(1),
        branch_id: view.branch_id(),
        before_semantic_head_commit_object_id: Some(
            view.branch_snapshot().semantic_head_commit_object_id,
        ),
        after_semantic_head_commit_object_id: Some(commit_object_id),
        previous_ref_change_object_id: view.branch_snapshot().latest_ref_change_object_id,
        payload: vec![identity],
        json_payload_object_ids: Vec::new(),
    };
    let (ref_object_id, _) = ref_change.encode().expect("next ref change");
    let commit_catalog_edit = put_commit_catalog_entries(
        view.repository_root().commit_catalog_root,
        &[(
            semantic_commit.commit_id,
            CommitCatalogEntry { commit_object_id },
        )],
        view.test_storage_read(),
    )
    .await
    .expect("commit catalog edit");
    let mut change_entries = semantic_commit
        .members
        .iter()
        .enumerate()
        .map(|(ordinal, member)| {
            (
                member.change_id(),
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id,
                        ordinal: u32::try_from(ordinal).expect("transition member ordinal"),
                    },
                },
            )
        })
        .collect::<Vec<_>>();
    change_entries.push((
        ref_change.change_id(),
        ChangeCatalogEntry {
            owner: ChangeCatalogOwner::BranchRef {
                ref_change_object_id: ref_object_id,
                branch_id: view.branch_id(),
            },
        },
    ));
    change_entries.sort_by_key(|(id, _)| *id.as_bytes());
    let change_catalog_edit = put_change_catalog_entries(
        view.repository_root().change_catalog_root,
        &change_entries,
        view.test_storage_read(),
    )
    .await
    .expect("change catalog edit");
    let local_state_root = state_edit.root;
    BranchStateTransition {
        state_edit,
        repository_root: RepositoryRootV1 {
            commit_catalog_root: commit_catalog_edit.root,
            change_catalog_root: change_catalog_edit.root,
            ..view.repository_root()
        },
        commit_catalog_edit,
        change_catalog_edit,
        semantic_commit,
        changes: vec![ref_change],
        branch_snapshot: BranchSnapshotV1 {
            branch_id: view.branch_id(),
            local_state_root,
            semantic_head_commit_object_id: commit_object_id,
            latest_ref_change_object_id: Some(ref_object_id),
            historical_global_state_root: view.repository_root().global_state_root,
        },
    }
}

#[test]
fn immutable_objects_and_typed_state_codecs_fail_closed() {
    let seed = build_seed();
    let encoded = seed
        .objects
        .get(seed.repository_root_id)
        .expect("root bytes");
    RepositoryRootV1::decode(seed.repository_root_id, encoded).expect("root authenticates");
    let mut corrupted = encoded.to_vec();
    *corrupted.last_mut().expect("nonempty") ^= 1;
    assert!(RepositoryRootV1::decode(seed.repository_root_id, &corrupted).is_err());
    assert!(BranchSnapshotV1::decode(seed.repository_root_id, encoded).is_err());

    let pack_object_id = content_id(7);
    let encoded = encode_state_value(StateValueRef {
        pack_object_id,
        pack_ordinal: 3,
    })
    .expect("typed state");
    let decoded = super::decode_state_value(&encoded).expect("typed state");
    assert_eq!(decoded.pack_object_id, pack_object_id);
    assert_eq!(decoded.pack_ordinal, 3);
    let (key, _) = state_entry("typed-key", StateCellRef::Null, 7, &[]);
    let decoded_key: StateKey = super::decode_state_key(&key).expect("typed key");
    assert_eq!(decoded_key.schema_key, "app.row");
    let row_pk = RowPk::single("typed-key");
    assert!(super::encode_state_entity_prefix("app.row", &row_pk).len() < key.len());
    assert!(build_state_tree(&[(b"opaque".to_vec(), b"opaque".to_vec())]).is_err());
}

#[cfg(any())]
#[test]
fn canonical_prefix_bounds_handle_carry_and_reject_invalid_untracked_bounds() {
    assert_eq!(
        super::exclusive_prefix_upper_bound(&[0x12, 0xff, 0xff]),
        Some(vec![0x13])
    );
    assert_eq!(
        super::exclusive_prefix_upper_bound(&[0xff, 0xff]),
        None,
        "an all-maximum prefix has an unbounded exclusive edge"
    );

    let row_pk = RowPk::single("row");
    let bounds = super::encode_state_entity_prefix_bounds("app.row", &row_pk);
    let full_key = super::encode_state_key(StateKeyRef {
        schema_key: "app.row",
        file_id: Some("file"),
        row_pk: &row_pk,
    });
    assert!(full_key.starts_with(&bounds.lower));
    assert!(bounds.upper.as_ref().is_some_and(|upper| full_key < *upper));
    let other_schema = super::encode_state_key(StateKeyRef {
        schema_key: "app.other",
        file_id: Some("file"),
        row_pk: &row_pk,
    });
    assert!(!other_schema.starts_with(&bounds.lower));

    let branch_id = CanonicalBranchId::from_bytes(raw_id(0x44));
    let lower = state_entry("z", StateCellRef::Null, 0x44, &[]).0;
    let upper = state_entry("a", StateCellRef::Null, 0x44, &[]).0;
    assert!(
        super::encode_untracked_branch_range_bounds(branch_id, Some(&lower), Some(&upper),)
            .is_err()
    );
    assert!(super::encode_untracked_branch_range_bounds(branch_id, Some(&[0xff]), None).is_err());

    let schema_prefix = super::encode_state_entity_prefix(
        "app.row",
        &RowPk {
            components: crate::row_pk::RowPkComponents::Empty,
        },
    );
    let schema_upper = super::exclusive_prefix_upper_bound(&schema_prefix);
    let bounded = super::encode_untracked_branch_range_bounds(
        branch_id,
        Some(&schema_prefix),
        schema_upper.as_deref(),
    )
    .expect("canonical schema prefix bounds");
    assert!(bounded.lower.ends_with(&schema_prefix));
    assert_eq!(
        bounded
            .upper
            .as_deref()
            .expect("finite schema prefix upper bound")
            .strip_prefix(&bounded.lower[..bounded.lower.len() - schema_prefix.len()])
            .expect("branch prefix"),
        schema_upper.as_deref().expect("schema upper")
    );
}

#[tokio::test]
async fn state_root_diff_short_circuits_and_handles_sparse_changes() {
    let left_rows = vec![(1, 1), (2, 2), (3, 3), (80, 4), (160, 5)];
    let right_rows = vec![(1, 1), (2, 9), (4, 6), (80, 4), (161, 7)];
    let left = build_diff_tree(&left_rows);
    let right = build_diff_tree(&right_rows);
    let storage = CountingStorage::new();
    seed_diff_trees(&storage, &[&left, &right]).await;
    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("diff read"),
    );

    let equal = diff_roots(Some(left.root.object_id), Some(left.root.object_id), &read)
        .await
        .expect("equal roots");
    assert!(equal.is_empty());
    assert_eq!(
        storage.object_get_many.load(Ordering::Relaxed),
        0,
        "equal roots must not load a node"
    );

    let actual = diff_roots(Some(left.root.object_id), Some(right.root.object_id), &read)
        .await
        .expect("sparse diff");
    let actual = encoded_state_keys(&actual);
    assert_eq!(
        actual,
        expected_diff_trees(&left, &right)
            .into_iter()
            .collect::<Vec<_>>()
    );

    let inserted = diff_roots(None, Some(right.root.object_id), &read)
        .await
        .expect("empty-to-tree diff");
    assert_eq!(
        encoded_state_keys(&inserted),
        diff_state_rows(&right_rows)
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn state_root_diff_randomized_matches_full_scan_oracle() {
    let mut seed = 0x9e37_79b9_u64;
    for _case in 0..24 {
        let mut left = BTreeMap::new();
        let mut right = BTreeMap::new();
        for key in 0..192_usize {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            if seed & 3 != 0 {
                left.insert(key, (seed >> 24) as u8);
            }
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            if seed & 3 != 0 {
                right.insert(key, (seed >> 24) as u8);
            }
        }
        let left_rows = left.into_iter().collect::<Vec<_>>();
        let right_rows = right.into_iter().collect::<Vec<_>>();
        let left_tree = build_diff_tree(&left_rows);
        let right_tree = build_diff_tree(&right_rows);
        let storage = CountingStorage::new();
        seed_diff_trees(&storage, &[&left_tree, &right_tree]).await;
        let read = StorageAdapterReadScope::new(
            storage
                .begin_read(ReadOptions::default())
                .await
                .expect("random diff read"),
        );
        let actual = diff_roots(
            Some(left_tree.root.object_id),
            Some(right_tree.root.object_id),
            &read,
        )
        .await
        .expect("random diff");
        let actual = encoded_state_keys(&actual)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let expected = expected_diff_trees(&left_tree, &right_tree);
        if actual != expected {
            panic!(
                "random case {_case}: missing={} extra={} extra_key={:?}",
                expected.difference(&actual).count(),
                actual.difference(&expected).count(),
                actual.difference(&expected).next()
            );
        }
    }
}

#[tokio::test]
async fn state_root_diff_rejects_corrupt_nodes_and_preserves_order() {
    let tree = build_diff_tree(&(0..256).map(|index| (index, 1)).collect::<Vec<_>>());
    let mut corrupted = tree
        .objects
        .get(tree.root.object_id)
        .expect("root bytes")
        .to_vec();
    *corrupted.first_mut().expect("root is nonempty") ^= 1;
    let mut writes = StorageWriteSet::new();
    writes.put(
        OBJECT_SPACE,
        tree.root.object_id.as_bytes().to_vec(),
        corrupted,
    );
    let storage = CountingStorage::new();
    commit_write_set_for_test(writes, &storage).await;
    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("corrupt diff read"),
    );
    assert!(
        diff_roots(None, Some(tree.root.object_id), &read)
            .await
            .is_err(),
        "a corrupted authenticated root must fail closed"
    );
}

#[tokio::test]
async fn coherent_state_point_and_range_preserve_overlay_semantics() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("coherent view");
    let a: VisibleStateRow = state_point(&view, &seed.state_keys[0], false)
        .await
        .expect("point a")
        .expect("a visible");
    assert_eq!(a.source, StateSource::Branch);
    assert!(
        matches!(a.value.cell, StateCell::Value(ref value) if <_ as AsRef<str>>::as_ref(value) == "local-a")
    );
    assert!(
        state_point(&view, &seed.state_keys[1], false)
            .await
            .expect("point b")
            .is_none()
    );
    assert!(matches!(
        state_point(&view, &seed.state_keys[2], false)
            .await
            .expect("point c")
            .expect("c visible")
            .value
            .cell,
        StateCell::Null
    ));
    let exact = state_points(
        &view,
        &[
            seed.state_keys[2].clone(),
            seed.state_keys[0].clone(),
            seed.state_keys[1].clone(),
            seed.state_keys[2].clone(),
        ],
        false,
    )
    .await
    .expect("batched exact state");
    assert_eq!(exact.len(), 4);
    assert!(matches!(
        exact[0].as_ref().map(|row| &row.value.cell),
        Some(StateCell::Null)
    ));
    assert_eq!(exact[0], exact[3]);
    assert_eq!(
        exact[1].as_ref().map(|row| row.source),
        Some(StateSource::Branch)
    );
    assert!(
        exact[2].is_none(),
        "branch tombstone must mask global state"
    );
    let view_points = view
        .points(&seed.state_keys[..2], false)
        .await
        .expect("view exact points");
    assert_eq!(view_points.len(), 2);
    let view_range = view
        .range(None, None, Some(2), false)
        .await
        .expect("view bounded range");
    assert_eq!(view_range.len(), 2);
    let exact_ranges = seed.state_keys[..2]
        .iter()
        .map(|key| {
            (
                key.clone(),
                super::exclusive_prefix_upper_bound(key)
                    .expect("canonical state key has a finite successor"),
            )
        })
        .map(|(lower, upper)| (lower, Some(upper)))
        .collect::<Vec<_>>();
    let range_slots = view
        .ranges(&exact_ranges, false)
        .await
        .expect("batched exact ranges");
    assert_eq!(range_slots.len(), 2);
    assert_eq!(range_slots[0].len(), 1);
    assert_eq!(range_slots[0][0].source, StateSource::Branch);
    assert!(
        range_slots[1].is_empty(),
        "a local tombstone must suppress the matching global row before visibility"
    );
    let tombstone_slots = view
        .ranges(&exact_ranges, true)
        .await
        .expect("tombstone-inclusive batched exact ranges");
    assert_eq!(tombstone_slots[1].len(), 1);
    assert!(tombstone_slots[1][0].value.cell.deleted());
    assert!(
        view.range(
            Some(&seed.state_keys[0]),
            Some(&seed.state_keys[0]),
            Some(1),
            false,
        )
        .await
        .expect("empty equal-bound range")
        .is_empty()
    );
    let rows = state_range(&view, None, None, Some(3), false)
        .await
        .expect("merged range");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].source, StateSource::Branch);
    assert!(rows.iter().all(|row| !row.value.cell.deleted()));
    let with_tombstone = state_range(&view, None, None, None, true)
        .await
        .expect("range with tombstones");
    assert_eq!(with_tombstone.len(), 4);
    assert!(with_tombstone.iter().any(|row| row.value.cell.deleted()));
    let (_, updated) = state_entry("a", StateCellRef::Value("updated-a"), 0x22, &[]);
    let (local_d, _) = state_entry("d", StateCellRef::Null, 0x22, &[]);
    let edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![
            StateTreeMutation::update(seed.state_keys[0].clone(), updated),
            StateTreeMutation::remove(local_d),
        ],
        view.test_storage_read(),
    )
    .await
    .expect("update/remove path copy");
    assert_eq!(edit.entry_count(), 2);
    // Both mutations are lowered through one authenticated batch edit; they
    // may share the same copied leaf and ancestor path.
    assert!(edit.copied_nodes() >= 1);

    let bounds = super::encode_state_entity_prefix_bounds("app.row", &RowPk::empty());
    let range_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![StateTreeMutation::remove_range(bounds.lower, bounds.upper)],
        view.test_storage_read(),
    )
    .await
    .expect("authenticated schema range delete");
    assert_eq!(range_edit.entry_count(), 0);
    assert!(range_edit.copied_nodes() >= 1);

    let replacement_bounds =
        super::encode_state_entity_prefix_bounds("app.row", &RowPk::empty());
    let (_, replacement_value) = state_entry("a", StateCellRef::Value("replacement-a"), 0x22, &[]);
    let replacement = view
        .replace_state_tree_range(
            view.branch_snapshot().local_state_root,
            replacement_bounds.lower,
            replacement_bounds.upper,
            vec![(
                seed.state_keys[0].clone(),
                replacement_value,
                StateMutationAudit {
                    commit_id: raw_id(0x22),
                    tombstone: false,
                    blob_manifest_object_ids: Vec::new(),
                },
            )],
        )
        .await
        .expect("authenticated complete range replacement");
    assert_eq!(replacement.entry_count(), 1);
    assert!(replacement.copied_nodes() >= 1);
    assert!(
        edit_state_tree(
            view.branch_snapshot().local_state_root,
            vec![StateTreeMutation::remove_range(vec![0xff], None)],
            view.test_storage_read(),
        )
        .await
        .is_err(),
        "a malformed state prefix must fail closed"
    );
}

#[tokio::test]
async fn branch_root_diff_resolves_global_fallback_after_local_unmask() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("initial overlay view");
    let key = encode_state_key(StateKeyRef {
        schema_key: "app.row",
        file_id: Some("file"),
        row_pk: &RowPk::single("b"),
    });
    let (masked_rows, masked_members, masked_page_objects, masked_pack_objects) =
        encode_test_state_entries(
            0x62,
            vec![test_state_member(
                "b",
                StateCellRef::Value("local-b"),
                0x62,
                &[],
                false,
            )],
        );
    let masked_value = masked_rows
        .into_iter()
        .find(|(encoded_key, _)| *encoded_key == key)
        .map(|(_, value)| value)
        .expect("masked state value");
    let mut masked_page_writes = StorageWriteSet::new();
    for (object_id, bytes) in masked_page_objects.into_iter().chain(masked_pack_objects) {
        masked_page_writes.put(OBJECT_SPACE, object_id.as_bytes().to_vec(), bytes.to_vec());
    }
    commit_write_set_for_test(masked_page_writes, &storage).await;
    let state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![StateTreeMutation::update(key.clone(), masked_value)],
        view.test_storage_read(),
    )
    .await
    .expect("local mask update");
    let transition = branch_transition_with_members(&view, state_edit, 0x62, masked_members).await;
    let masked_commit_id = transition.semantic_commit.commit_id;
    let mut publication = PreparedPublication::from_branch_view(&view).expect("publication");
    publication
        .publish_state_transition(&view, transition)
        .await
        .expect("authenticated state transition");
    drop(view);
    commit_publication_for_test(publication, &storage)
        .await
        .expect("publish transition");

    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("diff read"),
    );
    let facade = ForkTreeReadFacade::new(read);
    let before_commit_id =
        crate::changelog::CommitId::new(uuid::Uuid::from_bytes(*seed.commit_id.as_bytes()));
    let masked_commit_id =
        crate::changelog::CommitId::new(uuid::Uuid::from_bytes(*masked_commit_id.as_bytes()));
    let mask_diff = facade
        .diff_branch_state_rows_between_commits(before_commit_id, masked_commit_id)
        .await
        .expect("branch-root mask diff");
    let masked_row = mask_diff
        .into_iter()
        .find(|entry| {
            entry
                .before
                .as_ref()
                .or(entry.after.as_ref())
                .is_some_and(|row| row.key.row_pk == RowPk::single("b"))
        })
        .expect("masked global row must remain in the structural diff");
    assert!(masked_row.before.as_ref().is_some_and(|row| row.deleted));
    let masked = masked_row.after.expect("masked endpoint");
    assert!(!masked.global);
    assert!(!masked.deleted);
    assert_eq!(
        masked
            .seed_snapshot_content()
            .expect("terminal test projection")
            .as_deref(),
        Some(r#"{"id":"b","value":"local-b"}"#)
    );

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("view after local mask");
    let state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![StateTreeMutation::remove(key)],
        view.test_storage_read(),
    )
    .await
    .expect("local unmask removal");
    let transition = branch_transition(&view, state_edit, 0x64).await;
    let unmasked_commit_id = transition.semantic_commit.commit_id;
    let mut publication = PreparedPublication::from_branch_view(&view).expect("publication");
    publication
        .publish_state_transition(&view, transition)
        .await
        .expect("authenticated state transition");
    drop(view);
    commit_publication_for_test(publication, &storage)
        .await
        .expect("publish unmask transition");

    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("unmask diff read"),
    );
    let facade = ForkTreeReadFacade::new(read);
    let unmasked_commit_id =
        crate::changelog::CommitId::new(uuid::Uuid::from_bytes(*unmasked_commit_id.as_bytes()));
    let unmask_diff = facade
        .diff_branch_state_rows_between_commits(masked_commit_id, unmasked_commit_id)
        .await
        .expect("branch-root unmask diff");
    let unmasked_row = unmask_diff
        .into_iter()
        .find(|entry| {
            entry
                .before
                .as_ref()
                .or(entry.after.as_ref())
                .is_some_and(|row| row.key.row_pk == RowPk::single("b"))
        })
        .expect("unmasked global row must remain in the structural diff");
    let before = unmasked_row.before.expect("masked before endpoint");
    assert!(!before.global);
    assert!(!before.deleted);
    assert_eq!(
        before
            .seed_snapshot_content()
            .expect("terminal test projection")
            .as_deref(),
        Some(r#"{"id":"b","value":"local-b"}"#)
    );
    let after = unmasked_row.after.expect("global fallback endpoint");
    assert!(after.global);
    assert!(!after.deleted);
    assert_eq!(
        after
            .seed_snapshot_content()
            .expect("terminal test projection")
            .as_deref(),
        Some(r#"{"id":"b","value":"global-b"}"#)
    );
}

#[cfg(any())]
#[tokio::test]
async fn untracked_range_is_branch_bounded_ordered_and_limited() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let global_branch_id = CanonicalBranchId::from_bytes(
        *uuid::Uuid::parse_str(crate::GLOBAL_BRANCH_ID)
            .expect("global branch id")
            .as_bytes(),
    );
    let other_branch_id = CanonicalBranchId::from_bytes(raw_id(0x99));
    let entries = [
        (seed.branch_id, "branch-a", "branch-a"),
        (seed.branch_id, "branch-b", "branch-b"),
        (global_branch_id, "global", "global"),
        (other_branch_id, "other", "other"),
    ];
    let mut writes = StorageWriteSet::new();
    for (branch_id, primary_key, value) in entries {
        let (encoded_key, _) = state_entry(primary_key, StateCellRef::Null, 0x55, &[]);
        let decoded_key = super::decode_state_key(&encoded_key).expect("state key");
        let encoded_value = super::encode_untracked_value(UntrackedValueRef {
            created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
            updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
            cell: StateCellRef::Value(value),
            metadata: None,
            origin_key: None,
            blob_manifest_object_ids: &[],
        })
        .expect("untracked value");
        writes.put(
            UNTRACKED_ROW_SPACE,
            super::encode_untracked_key(
                branch_id,
                StateKeyRef {
                    schema_key: &decoded_key.schema_key,
                    file_id: decoded_key.file_id.as_deref(),
                    row_pk: &decoded_key.row_pk,
                },
            ),
            encoded_value,
        );
    }
    commit_write_set_for_test(writes, &storage).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("coherent view");
    let limited = view
        .scan_untracked_branch_range(None, None, Some(1))
        .await
        .expect("bounded untracked range");
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].0.file_id.as_deref(), Some("file"));
    assert!(matches!(limited[0].1.cell, StateCell::Value(_)));
    let all = view
        .scan_untracked_branch_range(None, None, None)
        .await
        .expect("all selected-branch untracked rows");
    assert_eq!(all.len(), 2);
    assert!(all.windows(2).all(|pair| pair[0].0 <= pair[1].0));

    let (corrupt_key, _) = state_entry("branch-a", StateCellRef::Null, 0x55, &[]);
    let decoded_key = super::decode_state_key(&corrupt_key).expect("state key");
    let mut corrupt_write = StorageWriteSet::new();
    corrupt_write.put(
        UNTRACKED_ROW_SPACE,
        super::encode_untracked_key(
            seed.branch_id,
            StateKeyRef {
                schema_key: &decoded_key.schema_key,
                file_id: decoded_key.file_id.as_deref(),
                row_pk: &decoded_key.row_pk,
            },
        ),
        vec![0xff],
    );
    commit_write_set_for_test(corrupt_write, &storage).await;
    let reopened = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("reopen after untracked corruption");
    assert!(
        reopened
            .scan_untracked_branch_range(None, None, None)
            .await
            .is_err(),
        "a selected-branch malformed value must fail closed"
    );
}

#[tokio::test]
async fn historical_absence_requires_authenticated_commit_and_root() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("historical absence read"),
    );
    let facade = ForkTreeReadFacade::new(read);
    let public_commit_id = public_commit_id(0x20);
    let absent_key = encode_state_key(StateKeyRef {
        schema_key: "app.row",
        file_id: Some("file"),
        row_pk: &RowPk::single("absent"),
    });
    assert!(
        facade
            .load_state_value_at_commit(public_commit_id, &absent_key, true)
            .await
            .expect("authenticated absent key")
            .is_none(),
        "a missing key is None only after commit and roots authenticate"
    );
}

#[tokio::test]
async fn historical_missing_commit_catalog_fails_for_point_and_batch() {
    let mut seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let commit_object_id = seed.commit_object_id;
    let ref_change_object_id = seed.ref_change_object_id;
    replace_selected_history_graph(&mut seed, &[], &[], commit_object_id, ref_change_object_id);
    seed_storage(&storage, &seed).await;

    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("missing catalog read"),
    );
    let facade = ForkTreeReadFacade::new(read);
    let public_commit_id = public_commit_id(0x20);
    assert!(
        facade
            .load_state_value_at_commit(public_commit_id, &seed.state_keys[0], true)
            .await
            .is_err(),
        "missing selected CommitCatalog entry must not become None"
    );
    let key = StateKey {
        schema_key: "app.row".to_owned(),
        file_id: Some("file".to_owned()),
        row_pk: RowPk::single("a"),
    };
    assert!(
        facade
            .load_state_rows_at_commit(&public_commit_id.to_string(), &[key])
            .await
            .is_err(),
        "batch lowering must propagate missing selected commit corruption"
    );
}

#[tokio::test]
async fn historical_missing_state_root_fails_before_empty_result() {
    let mut seed = build_seed();
    let storage = Memory::new();
    let commit_id = seed.commit_id;
    let semantic_change_id = seed.semantic_change_id;
    let semantic_change_object_id = seed.semantic_change_object_id;
    let ref_change_id = seed.ref_change_id;
    let ref_change_object_id = seed.ref_change_object_id;
    let branch_id = seed.branch_id;
    let seed_commit = CommitObjectV1::decode(
        seed.commit_object_id,
        seed.objects
            .get(seed.commit_object_id)
            .expect("seed commit"),
    )
    .expect("decode seed commit");
    let commit = CommitObjectV1 {
        commit_id,
        generation: 1,
        parent_commit_object_ids: Vec::new(),
        members: Vec::new(),
        member_page_object_ids: seed_commit.member_page_object_ids,
        global_state_root: content_id(0xf1),
        local_state_root: seed.local_state_root,
        checkpoint_cursor: CheckpointCursorV1::root(),
        metadata: b"missing-state-root".to_vec(),
    };
    let (commit_object_id, commit_bytes) = commit.encode().expect("missing-root commit");
    seed.objects
        .insert(commit_object_id, commit_bytes)
        .expect("missing-root commit object");
    let changes = vec![
        (
            semantic_change_id,
            ChangeCatalogEntry {
                owner: ChangeCatalogOwner::CommitMember {
                    commit_object_id,
                    ordinal: 0,
                },
            },
        ),
        (
            ref_change_id,
            ChangeCatalogEntry {
                owner: ChangeCatalogOwner::BranchRef {
                    ref_change_object_id,
                    branch_id,
                },
            },
        ),
    ];
    seed_storage(&storage, &seed).await;
    replace_selected_history_graph(
        &mut seed,
        &[(commit_id, CommitCatalogEntry { commit_object_id })],
        &changes,
        commit_object_id,
        ref_change_object_id,
    );
    seed_storage(&storage, &seed).await;

    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("missing state root read"),
    );
    let facade = ForkTreeReadFacade::new(read);
    assert!(
        facade
            .load_state_value_at_commit(public_commit_id(0x20), &seed.state_keys[2], true)
            .await
            .is_err(),
        "missing selected state root must not become an empty historical result"
    );
    assert!(
        facade
            .diff_branch_state_rows_between_commits(public_commit_id(0x20), public_commit_id(0x20),)
            .await
            .is_err(),
        "branch-root diff must reject a missing authenticated root before equality pruning"
    );
}

#[tokio::test]
async fn stale_commit_summary_authenticates_only_required_envelope_edges() {
    let mut valid = build_seed();
    let valid_parent_object_id = valid.commit_object_id;
    let (valid_child_id, valid_child_object_id) =
        insert_graph_commit(&mut valid, 0x90, 2, vec![valid_parent_object_id]);
    install_graph_head(
        &mut valid,
        &[(valid_child_id, valid_child_object_id)],
        valid_child_object_id,
        0x91,
    );
    let valid_storage = Memory::new();
    seed_storage(&valid_storage, &valid).await;
    let valid_read = StorageAdapterReadScope::new(
        valid_storage
            .begin_read(ReadOptions::default())
            .await
            .expect("valid summary read"),
    );
    let valid_facade = ForkTreeReadFacade::new(valid_read);
    let valid_repository = RepositoryRootV1::decode(
        valid.repository_root_id,
        valid
            .objects
            .get(valid.repository_root_id)
            .expect("valid repository root"),
    )
    .expect("valid repository");
    let mut valid_cache = super::serving::StaleCommitSummaryCache::new(valid_facade.operation_id());
    assert!(
        valid_facade
            .load_stale_commit_state_roots(
                &valid_repository,
                public_commit_id(0x90),
                &mut valid_cache,
            )
            .await
            .is_ok(),
        "a valid catalog/object/root/generation envelope must authenticate"
    );

    let mut bad_catalog = build_seed();
    let bad_catalog_parent_object_id = bad_catalog.commit_object_id;
    let (bad_catalog_child_id, bad_catalog_child_object_id) = insert_graph_commit(
        &mut bad_catalog,
        0x92,
        2,
        vec![bad_catalog_parent_object_id],
    );
    install_graph_head(
        &mut bad_catalog,
        &[(bad_catalog_child_id, bad_catalog_child_object_id)],
        bad_catalog_child_object_id,
        0x93,
    );
    let catalog_changes = seed_member_catalog_entries(&bad_catalog, bad_catalog.commit_object_id);
    let bad_catalog_commit_id = bad_catalog.commit_id;
    let bad_catalog_commit_object_id = bad_catalog.commit_object_id;
    let bad_catalog_orphan_object_id = bad_catalog.orphan_object_id;
    let bad_catalog_ref_change_object_id = bad_catalog.ref_change_object_id;
    replace_selected_history_graph(
        &mut bad_catalog,
        &[
            (
                bad_catalog_commit_id,
                CommitCatalogEntry {
                    commit_object_id: bad_catalog_commit_object_id,
                },
            ),
            (
                bad_catalog_child_id,
                CommitCatalogEntry {
                    commit_object_id: bad_catalog_orphan_object_id,
                },
            ),
        ],
        &catalog_changes,
        bad_catalog_child_object_id,
        bad_catalog_ref_change_object_id,
    );
    let bad_catalog_storage = Memory::new();
    seed_storage(&bad_catalog_storage, &bad_catalog).await;
    let bad_catalog_read = StorageAdapterReadScope::new(
        bad_catalog_storage
            .begin_read(ReadOptions::default())
            .await
            .expect("bad catalog summary read"),
    );
    let bad_catalog_facade = ForkTreeReadFacade::new(bad_catalog_read);
    let bad_catalog_repository = RepositoryRootV1::decode(
        bad_catalog.repository_root_id,
        bad_catalog
            .objects
            .get(bad_catalog.repository_root_id)
            .expect("bad catalog repository root"),
    )
    .expect("bad catalog repository");
    let mut bad_catalog_cache =
        super::serving::StaleCommitSummaryCache::new(bad_catalog_facade.operation_id());
    assert!(
        bad_catalog_facade
            .load_stale_commit_state_roots(
                &bad_catalog_repository,
                public_commit_id(0x92),
                &mut bad_catalog_cache,
            )
            .await
            .is_err(),
        "a catalog entry substituted with a non-Commit object must fail closed"
    );

    let mut bad_object = build_seed();
    let bad_object_parent_object_id = bad_object.commit_object_id;
    let (bad_object_child_id, bad_object_child_object_id) =
        insert_graph_commit(&mut bad_object, 0x94, 2, vec![bad_object_parent_object_id]);
    install_graph_head(
        &mut bad_object,
        &[(bad_object_child_id, bad_object_child_object_id)],
        bad_object_child_object_id,
        0x95,
    );
    let bad_object_storage = Memory::new();
    seed_storage(&bad_object_storage, &bad_object).await;
    let mut bad_object_write = StorageWriteSet::new();
    bad_object_write.delete(OBJECT_SPACE, bad_object_child_object_id.as_bytes().to_vec());
    commit_write_set_for_test(bad_object_write, &bad_object_storage).await;
    let bad_object_read = StorageAdapterReadScope::new(
        bad_object_storage
            .begin_read(ReadOptions::default())
            .await
            .expect("bad object summary read"),
    );
    let bad_object_facade = ForkTreeReadFacade::new(bad_object_read);
    let bad_object_repository = RepositoryRootV1::decode(
        bad_object.repository_root_id,
        bad_object
            .objects
            .get(bad_object.repository_root_id)
            .expect("bad object repository root"),
    )
    .expect("bad object repository");
    let mut bad_object_cache =
        super::serving::StaleCommitSummaryCache::new(bad_object_facade.operation_id());
    assert!(
        bad_object_facade
            .load_stale_commit_state_roots(
                &bad_object_repository,
                public_commit_id(0x94),
                &mut bad_object_cache,
            )
            .await
            .is_err(),
        "a missing Commit object must fail closed before stale classification"
    );

    let mut bad_root = build_seed();
    let bad_root_parent_object_id = bad_root.commit_object_id;
    let bad_root_object_id = bad_root.orphan_object_id;
    let bad_root_local_state_root = bad_root.local_state_root;
    let (bad_root_child_id, bad_root_child_object_id) = insert_graph_commit_with_roots(
        &mut bad_root,
        0x96,
        2,
        vec![bad_root_parent_object_id],
        bad_root_object_id,
        bad_root_local_state_root,
    );
    install_graph_head(
        &mut bad_root,
        &[(bad_root_child_id, bad_root_child_object_id)],
        bad_root_child_object_id,
        0x97,
    );
    let bad_root_storage = Memory::new();
    seed_storage(&bad_root_storage, &bad_root).await;
    let bad_root_read = StorageAdapterReadScope::new(
        bad_root_storage
            .begin_read(ReadOptions::default())
            .await
            .expect("bad root summary read"),
    );
    let bad_root_facade = ForkTreeReadFacade::new(bad_root_read);
    let bad_root_repository = RepositoryRootV1::decode(
        bad_root.repository_root_id,
        bad_root
            .objects
            .get(bad_root.repository_root_id)
            .expect("bad root repository root"),
    )
    .expect("bad root repository");
    let mut bad_root_cache =
        super::serving::StaleCommitSummaryCache::new(bad_root_facade.operation_id());
    assert!(
        bad_root_facade
            .load_stale_commit_state_roots(
                &bad_root_repository,
                public_commit_id(0x96),
                &mut bad_root_cache,
            )
            .await
            .is_err(),
        "a wrong-domain state root must fail closed before stale classification"
    );

    let mut bad_generation = build_seed();
    let bad_generation_parent_object_id = bad_generation.commit_object_id;
    let (bad_generation_child_id, bad_generation_child_object_id) = insert_graph_commit(
        &mut bad_generation,
        0x98,
        1,
        vec![bad_generation_parent_object_id],
    );
    install_graph_head(
        &mut bad_generation,
        &[(bad_generation_child_id, bad_generation_child_object_id)],
        bad_generation_child_object_id,
        0x99,
    );
    let bad_generation_storage = Memory::new();
    seed_storage(&bad_generation_storage, &bad_generation).await;
    let bad_generation_read = StorageAdapterReadScope::new(
        bad_generation_storage
            .begin_read(ReadOptions::default())
            .await
            .expect("bad generation summary read"),
    );
    let bad_generation_facade = ForkTreeReadFacade::new(bad_generation_read);
    let bad_generation_repository = RepositoryRootV1::decode(
        bad_generation.repository_root_id,
        bad_generation
            .objects
            .get(bad_generation.repository_root_id)
            .expect("bad generation repository root"),
    )
    .expect("bad generation repository");
    let mut bad_generation_cache =
        super::serving::StaleCommitSummaryCache::new(bad_generation_facade.operation_id());
    assert!(
        bad_generation_facade
            .load_stale_commit_state_roots(
                &bad_generation_repository,
                public_commit_id(0x98),
                &mut bad_generation_cache,
            )
            .await
            .is_err(),
        "a parent with an invalid generation edge must fail closed"
    );
}

#[test]
fn catalogs_use_one_raw_uuid_tree_and_fail_closed_on_owner_mismatch() {
    let seed = build_seed();
    let repository = RepositoryRootV1::decode(
        seed.repository_root_id,
        seed.objects.get(seed.repository_root_id).expect("root"),
    )
    .expect("repository");
    let load = load_from(&seed.objects);
    let value = lookup(
        repository.commit_catalog_root,
        "commit",
        seed.commit_id.as_bytes(),
        &load,
    )
    .expect("lookup")
    .expect("commit");
    let entry = CommitCatalogEntry::decode(&value).expect("entry");
    validate_commit_catalog_back_edge(seed.commit_id, entry, &load).expect("back edge");
    let rows = scan_all(repository.change_catalog_root, "change", &load).expect("scan");
    assert_eq!(rows.len(), 7);
    assert!(
        rows.iter()
            .any(|row| row.0 == seed.semantic_change_id.as_bytes())
    );
    assert!(
        rows.iter()
            .any(|row| row.0 == seed.ref_change_id.as_bytes())
    );
    let bad = ChangeCatalogEntry {
        owner: ChangeCatalogOwner::CommitMember {
            commit_object_id: seed.commit_object_id,
            ordinal: 9,
        },
    };
    assert!(validate_change_catalog_back_edge(seed.semantic_change_id, bad, &load).is_err());
    let semantic = CommitChangePageV3::decode(
        seed.semantic_change_object_id,
        seed.objects
            .get(seed.semantic_change_object_id)
            .expect("semantic bytes"),
    )
    .expect("semantic");
    assert_eq!(
        semantic.encode().expect("re-encode").0,
        seed.semantic_change_object_id
    );
}

#[tokio::test]
async fn recovery_selector_replacement_advances_generation_and_rejects_corruption() {
    let seed = build_seed();
    let storage = CrashStorage::new();
    seed_storage(&storage, &seed).await;
    let selector_id = SnapshotSelectorId::from_bytes(*seed.branch_id.as_bytes());

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("initial recovery view");
    let mut first = PreparedPublication::from_global_epoch(&view).expect("initial publication");
    let first_target = first
        .publish_current_snapshot_pin(
            &view,
            SnapshotRole::Recovery,
            selector_id,
            SelectorExpectation::Absent,
        )
        .expect("initial recovery selector");
    drop(view);
    commit_publication_for_test(first, &storage)
        .await
        .expect("initial recovery commit");

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("transition view");
    let state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        Vec::new(),
        view.test_storage_read(),
    )
    .await
    .expect("no-op transition state");
    let transition = branch_transition(&view, state_edit, 0x62).await;
    let mut transition_publication =
        PreparedPublication::from_branch_view(&view).expect("transition publication");
    transition_publication
        .publish_state_transition(&view, transition)
        .await
        .expect("branch transition");
    drop(view);
    commit_publication_for_test(transition_publication, &storage)
        .await
        .expect("transition commit");

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("replacement view");
    let selector_key = snapshot_selector_key(SnapshotRole::Recovery, selector_id);
    let loaded = view
        .test_storage_read()
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &[Key(selector_key)],
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("recovery selector read");
    let raw_before = match loaded.values.as_slice() {
        [Some(crate::storage::ProjectedValue::FullValue(bytes))] => bytes.clone(),
        other => panic!("expected recovery selector, got {other:?}"),
    };
    let mut replacement =
        PreparedPublication::from_global_epoch(&view).expect("replacement publication");
    let replacement_target = replacement
        .publish_current_snapshot_pin(
            &view,
            SnapshotRole::Recovery,
            selector_id,
            SelectorExpectation::Equals(raw_before.clone()),
        )
        .expect("replacement recovery selector");
    assert_ne!(first_target, replacement_target);
    drop(view);
    commit_publication_for_test(replacement, &storage)
        .await
        .expect("replacement commit");

    let reopened = storage.reopen();
    let view = open_coherent_view(&reopened, seed.branch_id)
        .await
        .expect("cold reopen after replacement");
    let loaded = view
        .test_storage_read()
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &[Key(snapshot_selector_key(
                SnapshotRole::Recovery,
                selector_id,
            ))],
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("recovery selector after reopen");
    let raw_after = match loaded.values.as_slice() {
        [Some(crate::storage::ProjectedValue::FullValue(bytes))] => bytes.clone(),
        other => panic!("expected recovery selector after reopen, got {other:?}"),
    };
    let selector = SnapshotSelectorV1::decode(&raw_after).expect("replacement selector");
    assert_eq!(selector.selector_generation, 2);
    assert_eq!(selector.target_object_id, replacement_target);

    let mut corrupted = raw_after.to_vec();
    *corrupted.last_mut().expect("encoded selector") ^= 1;
    let mut rejected = PreparedPublication::from_global_epoch(&view).expect("corrupt check");
    assert!(
        rejected
            .publish_current_snapshot_pin(
                &view,
                SnapshotRole::Recovery,
                selector_id,
                SelectorExpectation::Equals(Bytes::from(corrupted)),
            )
            .is_err()
    );
    drop(view);
    open_coherent_view(&reopened, seed.branch_id)
        .await
        .expect("corruption rejection leaves repository reopenable");
}

#[derive(Clone)]
struct CountingStorage {
    inner: Memory,
    begin_reads: Arc<AtomicUsize>,
    object_get_many: Arc<AtomicUsize>,
}

struct CountingRead {
    inner: MemoryRead,
    object_get_many: Arc<AtomicUsize>,
}

struct SharedParentCountingRead<R> {
    inner: R,
    parent_object: ObjectId,
    grandparent_object: ObjectId,
    member_object: ObjectId,
    parent_object_reads: Arc<AtomicUsize>,
    grandparent_object_reads: Arc<AtomicUsize>,
    member_object_reads: Arc<AtomicUsize>,
}

struct ChronologyCountingRead<R> {
    inner: R,
    commit_bytes: Arc<BTreeMap<Vec<u8>, usize>>,
    commit_reads: Arc<AtomicUsize>,
    requested_commit_bytes: Arc<AtomicUsize>,
    get_many_calls: Arc<AtomicUsize>,
    requested_keys: Arc<AtomicUsize>,
    returned_bytes: Arc<AtomicUsize>,
}

impl<R> StorageAdapterRead for ChronologyCountingRead<R>
where
    R: StorageAdapterRead,
{
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        self.get_many_calls.fetch_add(1, Ordering::Relaxed);
        self.requested_keys.fetch_add(
            requests.iter().map(|request| request.keys.len()).sum(),
            Ordering::Relaxed,
        );
        for request in requests {
            if request.space != OBJECT_SPACE {
                continue;
            }
            for key in request.keys {
                if let Some(bytes) = self.commit_bytes.get(key.0.as_ref()) {
                    self.commit_reads.fetch_add(1, Ordering::Relaxed);
                    self.requested_commit_bytes
                        .fetch_add(*bytes, Ordering::Relaxed);
                }
            }
        }
        let read = self.inner.get_many(requests);
        let returned_bytes = Arc::clone(&self.returned_bytes);
        async move {
            let result = read.await?;
            let bytes = result
                .values
                .iter()
                .filter_map(|value| value.as_ref())
                .map(|value| match value {
                    ProjectedValue::KeyOnly => 0,
                    ProjectedValue::FullValue(bytes) => bytes.len(),
                })
                .sum();
            returned_bytes.fetch_add(bytes, Ordering::Relaxed);
            Ok(result)
        }
    }

    fn begin_scan(
        &self,
        space: crate::storage::StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        self.inner.begin_scan(space, range, opts)
    }
}

impl<R> StorageAdapterRead for SharedParentCountingRead<R>
where
    R: StorageAdapterRead,
{
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        for request in requests {
            if request.space != OBJECT_SPACE {
                continue;
            }
            for key in request.keys {
                if key.0.as_ref() == self.parent_object.as_bytes() {
                    self.parent_object_reads.fetch_add(1, Ordering::Relaxed);
                }
                if key.0.as_ref() == self.grandparent_object.as_bytes() {
                    self.grandparent_object_reads
                        .fetch_add(1, Ordering::Relaxed);
                }
                if key.0.as_ref() == self.member_object.as_bytes() {
                    self.member_object_reads.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        self.inner.get_many(requests)
    }

    fn begin_scan(
        &self,
        space: crate::storage::StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        self.inner.begin_scan(space, range, opts)
    }
}

impl StorageRead for CountingRead {
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        self.object_get_many.fetch_add(
            requests
                .iter()
                .filter(|request| request.space == OBJECT_SPACE)
                .count(),
            Ordering::Relaxed,
        );
        self.inner.get_many(requests)
    }

    fn begin_scan(
        &self,
        space: crate::storage::StorageSpace,
        range: KeyRange,
        options: BeginScanOptions,
    ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
        self.inner.begin_scan(space, range, options)
    }
}

impl CountingStorage {
    fn new() -> Self {
        Self {
            inner: Memory::new(),
            begin_reads: Arc::new(AtomicUsize::new(0)),
            object_get_many: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Storage for CountingStorage {
    type Read<'a> = CountingRead;
    type Write<'a> = MemoryWrite;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.begin_reads.fetch_add(1, Ordering::Relaxed);
        Ok(CountingRead {
            inner: self.inner.begin_read(options).await?,
            object_get_many: Arc::clone(&self.object_get_many),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.inner.begin_write(options).await
    }
}

#[derive(Clone, Copy)]
enum CommitCrash {
    Before = 1,
    After = 2,
}

#[derive(Clone)]
struct CrashStorage {
    inner: Memory,
    crash: Arc<AtomicU8>,
}

struct CrashWrite {
    inner: MemoryWrite,
    crash: Arc<AtomicU8>,
}

impl CrashStorage {
    fn new() -> Self {
        Self {
            inner: Memory::new(),
            crash: Arc::new(AtomicU8::new(0)),
        }
    }

    fn reopen(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            crash: Arc::new(AtomicU8::new(0)),
        }
    }

    fn inject(&self, crash: CommitCrash) {
        assert_eq!(self.crash.swap(crash as u8, Ordering::SeqCst), 0);
    }
}

impl StorageWrite for CrashWrite {
    async fn put_many(
        &mut self,
        space: crate::storage::StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(
        &mut self,
        space: crate::storage::StorageSpace,
        keys: &[Key],
    ) -> Result<(), StorageError> {
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: crate::storage::StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        match self.crash.swap(0, Ordering::SeqCst) {
            value if value == CommitCrash::Before as u8 => {
                self.inner.rollback().await?;
                Err(StorageError::Io("injected pre-commit crash".to_owned()))
            }
            value if value == CommitCrash::After as u8 => {
                self.inner.commit().await?;
                Err(StorageError::Io("injected post-commit crash".to_owned()))
            }
            _ => self.inner.commit().await,
        }
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

impl Storage for CrashStorage {
    type Read<'a> = MemoryRead;
    type Write<'a> = CrashWrite;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.inner.begin_read(options).await
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(CrashWrite {
            inner: self.inner.begin_write(options).await?,
            crash: self.crash.clone(),
        })
    }
}

#[tokio::test]
async fn zero_edge_commit_change_pages_reopen_at_member_count_boundary() {
    for count in [256usize, 257] {
        let members = (0..count).map(zero_edge_page_member).collect::<Vec<_>>();
        let mut commit = CommitObjectV1 {
            commit_id: CommitId::from_bytes(raw_id(0xb1)),
            generation: 1,
            parent_commit_object_ids: Vec::new(),
            members,
            member_page_object_ids: Vec::new(),
            global_state_root: content_id(0xb2),
            local_state_root: content_id(0xb3),
            checkpoint_cursor: CheckpointCursorV1::root(),
            metadata: b"zero-edge-page-reopen".to_vec(),
        };
        let pages = commit.prepare_member_pages().expect("zero-edge pages");
        assert_eq!(
            pages.len(),
            1,
            "inline member count must not be confused with object-edge count"
        );
        let (commit_object_id, commit_bytes) = commit.encode().expect("commit envelope");

        let storage = CrashStorage::new();
        let mut writes = StorageWriteSet::new();
        writes.put(
            OBJECT_SPACE,
            commit_object_id.as_bytes().to_vec(),
            commit_bytes.to_vec(),
        );
        for (page_id, page_bytes) in &pages {
            writes.put(
                OBJECT_SPACE,
                page_id.as_bytes().to_vec(),
                page_bytes.to_vec(),
            );
        }
        commit_write_set_for_test(writes, &storage).await;

        let reopened = storage.reopen();
        let object_ids = std::iter::once(commit_object_id)
            .chain(commit.member_page_object_ids.iter().copied())
            .collect::<Vec<_>>();
        let keys = object_ids
            .iter()
            .map(|id| Key(Bytes::copy_from_slice(id.as_bytes())))
            .collect::<Vec<_>>();
        let read = reopened
            .begin_read(ReadOptions::default())
            .await
            .expect("reopen read");
        let loaded = read
            .get_many(&[GetManyRequest {
                space: OBJECT_SPACE,
                keys: &keys,
                opts: GetOptions {
                    projection: CoreProjection::FullValue,
                },
            }])
            .await
            .expect("reopen objects");
        let object_bytes = object_ids
            .into_iter()
            .zip(loaded.values.into_iter())
            .map(|(id, value)| match value {
                Some(crate::storage::ProjectedValue::FullValue(bytes)) => (id, bytes),
                other => panic!("expected reopened object {id:?}, got {other:?}"),
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let reopened_commit = CommitObjectV1::decode(
            commit_object_id,
            object_bytes
                .get(&commit_object_id)
                .expect("reopened commit bytes"),
        )
        .expect("reopened commit decodes");
        let reopened_members = reopened_commit
            .load_members_with(|id| {
                object_bytes
                    .get(&id)
                    .cloned()
                    .ok_or_else(|| StorageError::Io("missing reopened member page".to_owned()))
            })
            .expect("reopened member closure");
        assert_eq!(reopened_members, commit.members);

        let first_page_id = commit.member_page_object_ids[0];
        let mut corrupted = object_bytes
            .get(&first_page_id)
            .expect("first reopened page")
            .to_vec();
        corrupted[0] ^= 0x01;
        assert!(
            CommitChangePageV3::decode(first_page_id, &corrupted).is_err(),
            "corrupt zero-edge page must fail closed after reopen"
        );
    }
}

#[tokio::test]
async fn coherent_open_uses_one_read_and_visited_edges_fail_closed() {
    let seed = build_seed();
    let storage = CountingStorage::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("open");
    assert_eq!(storage.begin_reads.load(Ordering::Relaxed), 1);
    drop(view);

    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("manual coherent read"),
    );
    let manual = super::open_coherent_view_on_read(read, seed.branch_id)
        .await
        .expect("same-handle open");
    assert_eq!(manual.branch_id(), seed.branch_id);
    drop(manual);

    let mut writes = StorageWriteSet::new();
    writes.delete(
        OBJECT_SPACE,
        seed.semantic_change_object_id.as_bytes().to_vec(),
    );
    commit_write_set_for_test(writes, &storage).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("bounded open does not traverse an unrelated catalog member");
    assert!(
        view.load_object_bytes(seed.semantic_change_object_id)
            .await
            .is_err()
    );
}

#[cfg(any())]
#[tokio::test]
async fn exact_untracked_lookup_is_local_first_and_preserves_duplicate_slots() {
    let seed = build_seed();
    let storage = CountingStorage::new();
    seed_storage(&storage, &seed).await;
    let local_pk = RowPk::single("local");
    let global_pk = RowPk::single("global-only");
    let global_branch_id = CanonicalBranchId::from_bytes(
        *uuid::Uuid::parse_str(crate::GLOBAL_BRANCH_ID)
            .expect("global branch UUID")
            .as_bytes(),
    );
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("untracked publication view");
    let mut publication = PreparedPublication::from_global_epoch(&view).expect("publication");
    publication
        .put_untracked_row(
            CanonicalBranchId::from_bytes(
                *uuid::Uuid::parse_str(crate::GLOBAL_BRANCH_ID)
                    .expect("global branch UUID")
                    .as_bytes(),
            ),
            StateKeyRef {
                schema_key: "app.exact",
                file_id: None,
                row_pk: &local_pk,
            },
            UntrackedValueRef {
                created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
                cell: StateCellRef::Value("global-shadowed"),
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: &[],
            },
        )
        .expect("global shadowed value");
    publication
        .put_untracked_row(
            seed.branch_id,
            StateKeyRef {
                schema_key: "app.exact",
                file_id: None,
                row_pk: &local_pk,
            },
            UntrackedValueRef {
                created_at: LixTimestamp::from_unix_millis_utc_lossy(3),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(4),
                cell: StateCellRef::Tombstone,
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: &[],
            },
        )
        .expect("local tombstone");
    publication
        .put_untracked_row(
            CanonicalBranchId::from_bytes(
                *uuid::Uuid::parse_str(crate::GLOBAL_BRANCH_ID)
                    .expect("global branch UUID")
                    .as_bytes(),
            ),
            StateKeyRef {
                schema_key: "app.exact",
                file_id: None,
                row_pk: &global_pk,
            },
            UntrackedValueRef {
                created_at: LixTimestamp::from_unix_millis_utc_lossy(5),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(6),
                cell: StateCellRef::Value("global"),
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: &[],
            },
        )
        .expect("global-only value");
    drop(view);
    commit_publication_for_test(publication, &storage)
        .await
        .expect("commit untracked rows");

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("exact untracked view");
    let local_key = encode_state_key(StateKeyRef {
        schema_key: "app.exact",
        file_id: None,
        row_pk: &local_pk,
    });
    storage.untracked_get_many.store(0, Ordering::Relaxed);
    let local = view
        .load_untracked_overlay_points(&[local_key.clone(), local_key.clone()])
        .await
        .expect("local exact rows");
    assert_eq!(storage.untracked_get_many.load(Ordering::Relaxed), 1);
    assert_eq!(local.len(), 2);
    assert!(local.iter().all(|row| {
        row.as_ref()
            .is_some_and(|(_, _, value)| value.cell.deleted())
    }));
    let local_owner = view
        .load_untracked_owner_point(
            seed.branch_id,
            &encode_state_key(StateKeyRef {
                schema_key: "app.exact",
                file_id: None,
                row_pk: &local_pk,
            }),
        )
        .await
        .expect("local owner point");
    assert!(
        local_owner
            .as_ref()
            .is_some_and(|(_, value)| value.cell.deleted())
    );
    let global_owner = view
        .load_untracked_owner_point(global_branch_id, &local_key)
        .await
        .expect("global owner point");
    assert!(global_owner
        .as_ref()
        .is_some_and(|(_, value)| {
            matches!(&value.cell, StateCell::Value(cell) if <_ as AsRef<str>>::as_ref(cell) == "global-shadowed")
        }));

    let global_key = encode_state_key(StateKeyRef {
        schema_key: "app.exact",
        file_id: None,
        row_pk: &global_pk,
    });
    storage.untracked_get_many.store(0, Ordering::Relaxed);
    let global = view
        .load_untracked_overlay_points(&[global_key])
        .await
        .expect("global fallback row");
    assert_eq!(storage.untracked_get_many.load(Ordering::Relaxed), 2);
    assert_eq!(
        global[0].as_ref().map(|(_, key, _)| key.row_pk.clone()),
        Some(global_pk)
    );
}

#[tokio::test]
async fn commit_topology_batch_loads_one_shared_parent_once_and_seeds_graph_walk() {
    let mut seed = build_seed();
    let grandparent = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(0x50)),
        generation: 1,
        parent_commit_object_ids: Vec::new(),
        members: Vec::new(),
        member_page_object_ids: Vec::new(),
        global_state_root: seed.global_state_root,
        local_state_root: seed.local_state_root,
        checkpoint_cursor: CheckpointCursorV1::root(),
        metadata: b"grandparent".to_vec(),
    };
    let (grandparent_object_id, grandparent_bytes) = grandparent.encode().expect("grandparent");
    seed.objects
        .insert(grandparent_object_id, grandparent_bytes)
        .expect("grandparent object");
    let parent_commit_id = CommitId::from_bytes(raw_id(0x51));
    let parent_members = vec![seed_commit_members(&seed)[0].clone()];
    let parent_page_ids =
        insert_test_change_pages(&mut seed.objects, parent_commit_id, &parent_members);
    let parent = CommitObjectV1 {
        commit_id: parent_commit_id,
        generation: 2,
        parent_commit_object_ids: vec![grandparent_object_id],
        members: parent_members,
        member_page_object_ids: parent_page_ids,
        global_state_root: seed.global_state_root,
        local_state_root: seed.local_state_root,
        checkpoint_cursor: CheckpointCursorV1::after_first_parent(
            grandparent_object_id,
            &grandparent,
            seed.branch_id,
            false,
        )
        .expect("parent checkpoint cursor"),
        metadata: b"shared-parent".to_vec(),
    };
    let (parent_object_id, parent_bytes) = parent.encode().expect("shared parent");
    seed.objects
        .insert(parent_object_id, parent_bytes)
        .expect("shared parent object");
    let child_a = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(0x52)),
        generation: 3,
        parent_commit_object_ids: vec![parent_object_id],
        members: Vec::new(),
        member_page_object_ids: Vec::new(),
        global_state_root: seed.global_state_root,
        local_state_root: seed.local_state_root,
        checkpoint_cursor: CheckpointCursorV1::after_first_parent(
            parent_object_id,
            &parent,
            seed.branch_id,
            false,
        )
        .expect("child a checkpoint cursor"),
        metadata: b"child-a".to_vec(),
    };
    let (child_a_object_id, child_a_bytes) = child_a.encode().expect("child a");
    seed.objects
        .insert(child_a_object_id, child_a_bytes)
        .expect("child a object");
    let child_b = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(0x53)),
        generation: 3,
        parent_commit_object_ids: vec![parent_object_id],
        members: Vec::new(),
        member_page_object_ids: Vec::new(),
        global_state_root: seed.global_state_root,
        local_state_root: seed.local_state_root,
        checkpoint_cursor: CheckpointCursorV1::after_first_parent(
            parent_object_id,
            &parent,
            seed.branch_id,
            false,
        )
        .expect("child b checkpoint cursor"),
        metadata: b"child-b".to_vec(),
    };
    let (child_b_object_id, child_b_bytes) = child_b.encode().expect("child b");
    seed.objects
        .insert(child_b_object_id, child_b_bytes)
        .expect("child b object");
    let creation = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0x54)),
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(1),
        branch_id: seed.branch_id,
        before_semantic_head_commit_object_id: None,
        after_semantic_head_commit_object_id: Some(child_a_object_id),
        previous_ref_change_object_id: None,
        payload: b"shared-parent-branch".to_vec(),
        json_payload_object_ids: Vec::new(),
    };
    let (creation_object_id, creation_bytes) = creation.encode().expect("creation ref");
    seed.objects
        .insert(creation_object_id, creation_bytes)
        .expect("creation ref object");
    let branch_id = seed.branch_id;
    let semantic_change_id = seed.semantic_change_id;
    let semantic_change_object_id = parent.member_page_object_ids[0];
    replace_selected_history_graph(
        &mut seed,
        &[
            (
                grandparent.commit_id,
                CommitCatalogEntry {
                    commit_object_id: grandparent_object_id,
                },
            ),
            (
                parent.commit_id,
                CommitCatalogEntry {
                    commit_object_id: parent_object_id,
                },
            ),
            (
                child_a.commit_id,
                CommitCatalogEntry {
                    commit_object_id: child_a_object_id,
                },
            ),
            (
                child_b.commit_id,
                CommitCatalogEntry {
                    commit_object_id: child_b_object_id,
                },
            ),
        ],
        &[
            (
                semantic_change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id: parent_object_id,
                        ordinal: 0,
                    },
                },
            ),
            (
                creation.change_id(),
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::BranchRef {
                        ref_change_object_id: creation_object_id,
                        branch_id,
                    },
                },
            ),
        ],
        child_a_object_id,
        creation_object_id,
    );

    let storage = CountingStorage::new();
    seed_storage(&storage, &seed).await;
    let parent_object_reads = Arc::new(AtomicUsize::new(0));
    let grandparent_object_reads = Arc::new(AtomicUsize::new(0));
    let member_object_reads = Arc::new(AtomicUsize::new(0));
    let read = SharedParentCountingRead {
        inner: StorageAdapterReadScope::new(
            storage
                .begin_read(ReadOptions::default())
                .await
                .expect("one retained topology read"),
        ),
        parent_object: parent_object_id,
        grandparent_object: grandparent_object_id,
        member_object: semantic_change_object_id,
        parent_object_reads: Arc::clone(&parent_object_reads),
        grandparent_object_reads: Arc::clone(&grandparent_object_reads),
        member_object_reads: Arc::clone(&member_object_reads),
    };
    let mut graph = CommitGraphStoreReader::new(read);
    let sibling_ids = [public_commit_id(0x52), public_commit_id(0x53)];
    let siblings = graph
        .load_nodes(&sibling_ids)
        .await
        .expect("shared-parent sibling batch");
    assert!(siblings.into_iter().all(|(_, node)| node.is_some()));
    assert!(
        graph
            .load_node(&public_commit_id(0x51))
            .await
            .expect("visit seeded parent")
            .is_some()
    );
    assert!(
        graph
            .load_node(&public_commit_id(0x50))
            .await
            .expect("visit seeded grandparent")
            .is_some()
    );
    assert_eq!(storage.begin_reads.load(Ordering::Relaxed), 1);
    assert_eq!(parent_object_reads.load(Ordering::Relaxed), 1);
    assert_eq!(grandparent_object_reads.load(Ordering::Relaxed), 1);
    assert_eq!(member_object_reads.load(Ordering::Relaxed), 0);
    drop(graph);

    let mut writes = StorageWriteSet::new();
    writes.delete(OBJECT_SPACE, semantic_change_object_id.as_bytes().to_vec());
    commit_write_set_for_test(writes, &storage).await;

    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("post-corruption read"),
    );
    let mut topology_reader = CommitTopologyReader::new(&read);
    assert!(
        topology_reader
            .load(&[public_commit_id(0x52), public_commit_id(0x53)])
            .await
            .expect("member corruption remains latent for sibling topology")
            .requested
            .into_iter()
            .all(|topology| topology.is_some())
    );
    assert!(
        load_commit_member_records(&read, public_commit_id(0x51))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn commit_graph_diamond_records_nearest_depth_once() {
    let mut seed = build_seed();
    let (root_catalog_id, root_object_id) = insert_graph_commit(&mut seed, 0x60, 1, Vec::new());
    let root_id = public_commit_id(0x60);
    let (left_catalog_id, left_object_id) =
        insert_graph_commit(&mut seed, 0x61, 2, vec![root_object_id]);
    let left_id = public_commit_id(0x61);
    let (right_catalog_id, right_object_id) =
        insert_graph_commit(&mut seed, 0x62, 2, vec![root_object_id]);
    let right_id = public_commit_id(0x62);
    let (head_catalog_id, head_object_id) =
        insert_graph_commit(&mut seed, 0x63, 3, vec![left_object_id, right_object_id]);
    let head_id = public_commit_id(0x63);
    install_graph_head(
        &mut seed,
        &[
            (root_catalog_id, root_object_id),
            (left_catalog_id, left_object_id),
            (right_catalog_id, right_object_id),
            (head_catalog_id, head_object_id),
        ],
        head_object_id,
        0x64,
    );

    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("one retained graph read"),
    );
    let mut graph = CommitGraphStoreReader::new(read);
    let reachable = graph
        .reachable_nodes(&head_id)
        .await
        .expect("diamond graph should be reachable");

    assert_eq!(
        reachable
            .iter()
            .map(|node| (node.commit.commit_id, node.depth))
            .collect::<Vec<_>>(),
        vec![(head_id, 0), (left_id, 1), (right_id, 1), (root_id, 2),],
        "DAG traversal must retain each node once at its nearest authenticated depth"
    );
}

#[tokio::test]
async fn commit_graph_criss_cross_rejects_ambiguous_merge_base() {
    let mut seed = build_seed();
    let (left_root_catalog_id, left_root_object_id) =
        insert_graph_commit(&mut seed, 0x70, 1, Vec::new());
    let left_root_id = public_commit_id(0x70);
    let (right_root_catalog_id, right_root_object_id) =
        insert_graph_commit(&mut seed, 0x71, 1, Vec::new());
    let right_root_id = public_commit_id(0x71);
    let (left_tip_catalog_id, left_tip_object_id) =
        insert_graph_commit(&mut seed, 0x72, 2, vec![left_root_object_id]);
    let (right_tip_catalog_id, right_tip_object_id) =
        insert_graph_commit(&mut seed, 0x73, 2, vec![right_root_object_id]);
    let (left_head_catalog_id, left_head_object_id) = insert_graph_commit(
        &mut seed,
        0x74,
        3,
        vec![left_tip_object_id, right_root_object_id],
    );
    let left_head_id = public_commit_id(0x74);
    let (right_head_catalog_id, right_head_object_id) = insert_graph_commit(
        &mut seed,
        0x75,
        3,
        vec![right_tip_object_id, left_root_object_id],
    );
    let right_head_id = public_commit_id(0x75);
    install_graph_head(
        &mut seed,
        &[
            (left_root_catalog_id, left_root_object_id),
            (right_root_catalog_id, right_root_object_id),
            (left_tip_catalog_id, left_tip_object_id),
            (right_tip_catalog_id, right_tip_object_id),
            (left_head_catalog_id, left_head_object_id),
            (right_head_catalog_id, right_head_object_id),
        ],
        left_head_object_id,
        0x76,
    );

    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("one retained graph read"),
    );
    let mut graph = CommitGraphStoreReader::new(read);
    let ancestors = graph
        .best_common_ancestors(&left_head_id, &right_head_id)
        .await
        .expect("criss-cross graph should enumerate common ancestors");
    assert_eq!(
        ancestors
            .iter()
            .map(|node| node.commit_id)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([left_root_id, right_root_id]),
        "both equally good criss-cross bases must remain visible"
    );
    let error = graph
        .merge_base(&left_head_id, &right_head_id)
        .await
        .expect_err("a three-way merge must reject an ambiguous base");
    assert_eq!(error.code, LixError::CODE_AMBIGUOUS_MERGE_BASE);
}

#[tokio::test]
async fn commit_graph_cycle_and_generation_edges_fail_closed() {
    let mut seed = build_seed();
    let (first_catalog_id, first_object_id) = insert_graph_commit(&mut seed, 0x80, 2, Vec::new());
    let first_id = public_commit_id(0x80);
    let (second_catalog_id, second_object_id) =
        insert_graph_commit(&mut seed, 0x81, 1, vec![first_object_id]);
    let first_commit = CommitObjectV1 {
        commit_id: first_catalog_id,
        generation: 2,
        parent_commit_object_ids: vec![second_object_id],
        members: Vec::new(),
        member_page_object_ids: Vec::new(),
        global_state_root: seed.global_state_root,
        local_state_root: seed.local_state_root,
        checkpoint_cursor: {
            let second = CommitObjectV1::decode(
                second_object_id,
                seed.objects
                    .get(second_object_id)
                    .expect("cycle second object"),
            )
            .expect("cycle second commit");
            CheckpointCursorV1::after_first_parent(second_object_id, &second, seed.branch_id, false)
                .expect("cycle checkpoint cursor")
        },
        metadata: b"cycle-first".to_vec(),
    };
    let (first_object_id, first_bytes) = first_commit.encode().expect("cycle first");
    seed.objects
        .insert(first_object_id, first_bytes)
        .expect("replace cycle first");
    install_graph_head(
        &mut seed,
        &[
            (first_catalog_id, first_object_id),
            (second_catalog_id, second_object_id),
        ],
        first_object_id,
        0x82,
    );

    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("one retained graph read"),
    );
    let mut graph = CommitGraphStoreReader::new(read);
    let error = graph
        .reachable_nodes(&first_id)
        .await
        .expect_err("cycle must fail closed before a result is published");
    assert!(
        error.to_string().contains("cycle")
            || error.to_string().contains("lower generation")
            || error.to_string().contains("multiple Commit objects"),
        "cycle/generation rejection must identify the authenticated graph defect: {error}"
    );

    let mut bad_generation = build_seed();
    let (parent_catalog_id, parent_object_id) =
        insert_graph_commit(&mut bad_generation, 0x83, 4, Vec::new());
    let (child_catalog_id, child_object_id) =
        insert_graph_commit(&mut bad_generation, 0x84, 4, vec![parent_object_id]);
    let child_id = public_commit_id(0x84);
    install_graph_head(
        &mut bad_generation,
        &[
            (parent_catalog_id, parent_object_id),
            (child_catalog_id, child_object_id),
        ],
        child_object_id,
        0x85,
    );
    let storage = Memory::new();
    seed_storage(&storage, &bad_generation).await;
    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("one retained graph read"),
    );
    let mut graph = CommitGraphStoreReader::new(read);
    let error = graph
        .load_node(&child_id)
        .await
        .expect_err("equal parent/child generation must fail closed");
    assert!(
        error.to_string().contains("generation"),
        "generation edge must fail closed with a structural error: {error}"
    );
}

#[tokio::test]
async fn coherent_open_defers_ref_target_authentication_until_visited() {
    let mut seed = build_seed();
    let bad_ref = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0x40)),
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(1),
        branch_id: seed.branch_id,
        before_semantic_head_commit_object_id: Some(seed.semantic_change_object_id),
        after_semantic_head_commit_object_id: Some(seed.commit_object_id),
        previous_ref_change_object_id: Some(seed.ref_change_object_id),
        payload: b"wrong-domain-before".to_vec(),
        json_payload_object_ids: Vec::new(),
    };
    let (bad_ref_id, bad_ref_bytes) = bad_ref.encode().expect("bad ref envelope");
    seed.objects
        .insert(bad_ref_id, bad_ref_bytes)
        .expect("bad ref object");
    let mut catalog_entries = vec![
        (
            seed.semantic_change_id,
            ChangeCatalogEntry {
                owner: ChangeCatalogOwner::CommitMember {
                    commit_object_id: seed.commit_object_id,
                    ordinal: 0,
                },
            },
        ),
        (
            seed.ref_change_id,
            ChangeCatalogEntry {
                owner: ChangeCatalogOwner::BranchRef {
                    ref_change_object_id: seed.ref_change_object_id,
                    branch_id: seed.branch_id,
                },
            },
        ),
        (
            bad_ref.change_id(),
            ChangeCatalogEntry {
                owner: ChangeCatalogOwner::BranchRef {
                    ref_change_object_id: bad_ref_id,
                    branch_id: seed.branch_id,
                },
            },
        ),
    ];
    catalog_entries.sort_by_key(|(id, _)| *id.as_bytes());
    let catalog = build_change_catalog(&catalog_entries).expect("bad catalog");
    let change_catalog_root = catalog.root.object_id;
    seed.objects
        .extend(catalog.objects)
        .expect("catalog objects");
    let current_repository = RepositoryRootV1::decode(
        seed.repository_root_id,
        seed.objects
            .get(seed.repository_root_id)
            .expect("repository"),
    )
    .expect("repository");
    let repository = RepositoryRootV1 {
        change_catalog_root,
        ..current_repository
    };
    let (repository_id, repository_bytes) = repository.encode().expect("new repository");
    seed.objects
        .insert(repository_id, repository_bytes)
        .expect("repository object");
    let snapshot = BranchSnapshotV1 {
        latest_ref_change_object_id: Some(bad_ref_id),
        ..BranchSnapshotV1::decode(
            seed.branch_snapshot_id,
            seed.objects.get(seed.branch_snapshot_id).expect("snapshot"),
        )
        .expect("snapshot")
    };
    let (snapshot_id, snapshot_bytes) = snapshot.encode().expect("new snapshot");
    seed.objects
        .insert(snapshot_id, snapshot_bytes)
        .expect("snapshot object");
    seed.repository_root_id = repository_id;
    seed.branch_snapshot_id = snapshot_id;
    seed.global_selector.repository_root = repository_id;
    seed.global_selector.selector_generation += 1;
    seed.branch_selector.branch_snapshot_object_id = snapshot_id;
    seed.branch_selector.selector_generation += 1;
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    assert!(
        open_coherent_view(&storage, seed.branch_id).await.is_err(),
        "coherent open must reject a latest RefChange whose authenticated target is malformed"
    );
}

#[tokio::test]
async fn coherent_open_requires_latest_ref_change_catalog_owner() {
    let mut orphan = build_seed();
    let semantic_change_id = orphan.semantic_change_id;
    let semantic_change_object_id = orphan.semantic_change_object_id;
    let commit_id = orphan.commit_id;
    let commit_object_id = orphan.commit_object_id;
    let ref_change_object_id = orphan.ref_change_object_id;
    let branch_id = orphan.branch_id;
    replace_selected_history_graph(
        &mut orphan,
        &[(commit_id, CommitCatalogEntry { commit_object_id })],
        &[(
            semantic_change_id,
            ChangeCatalogEntry {
                owner: ChangeCatalogOwner::CommitMember {
                    commit_object_id,
                    ordinal: 0,
                },
            },
        )],
        commit_object_id,
        ref_change_object_id,
    );
    let storage = Memory::new();
    seed_storage(&storage, &orphan).await;
    assert!(
        open_coherent_view(&storage, branch_id).await.is_err(),
        "a missing ChangeCatalog entry for the selected RefChange must fail at open"
    );

    let mut substituted = build_seed();
    let alternate_ref = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0x44)),
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(1),
        branch_id: substituted.branch_id,
        before_semantic_head_commit_object_id: None,
        after_semantic_head_commit_object_id: Some(substituted.commit_object_id),
        previous_ref_change_object_id: None,
        payload: b"substituted ref".to_vec(),
        json_payload_object_ids: Vec::new(),
    };
    let (alternate_ref_object_id, alternate_ref_bytes) =
        alternate_ref.encode().expect("alternate ref");
    substituted
        .objects
        .insert(alternate_ref_object_id, alternate_ref_bytes)
        .expect("alternate ref object");
    let semantic_change_id = substituted.semantic_change_id;
    let semantic_change_object_id = substituted.semantic_change_object_id;
    let commit_id = substituted.commit_id;
    let commit_object_id = substituted.commit_object_id;
    let ref_change_id = substituted.ref_change_id;
    let ref_change_object_id = substituted.ref_change_object_id;
    let branch_id = substituted.branch_id;
    replace_selected_history_graph(
        &mut substituted,
        &[(commit_id, CommitCatalogEntry { commit_object_id })],
        &[
            (
                semantic_change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id,
                        ordinal: 0,
                    },
                },
            ),
            (
                ref_change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::BranchRef {
                        ref_change_object_id: alternate_ref_object_id,
                        branch_id,
                    },
                },
            ),
        ],
        commit_object_id,
        ref_change_object_id,
    );
    let storage = Memory::new();
    seed_storage(&storage, &substituted).await;
    assert!(
        open_coherent_view(&storage, branch_id).await.is_err(),
        "a substituted ChangeCatalog object must fail at open"
    );

    let mut wrong_owner = build_seed();
    let semantic_change_id = wrong_owner.semantic_change_id;
    let semantic_change_object_id = wrong_owner.semantic_change_object_id;
    let commit_id = wrong_owner.commit_id;
    let commit_object_id = wrong_owner.commit_object_id;
    let ref_change_id = wrong_owner.ref_change_id;
    let ref_change_object_id = wrong_owner.ref_change_object_id;
    let branch_id = wrong_owner.branch_id;
    replace_selected_history_graph(
        &mut wrong_owner,
        &[(commit_id, CommitCatalogEntry { commit_object_id })],
        &[
            (
                semantic_change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id,
                        ordinal: 0,
                    },
                },
            ),
            (
                ref_change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id,
                        ordinal: 0,
                    },
                },
            ),
        ],
        commit_object_id,
        ref_change_object_id,
    );
    let storage = Memory::new();
    seed_storage(&storage, &wrong_owner).await;
    assert!(
        open_coherent_view(&storage, branch_id).await.is_err(),
        "a non-BranchRef ChangeCatalog owner must fail at open"
    );
}

#[tokio::test]
async fn retained_history_gc_rejects_generation_owner_and_ref_chronology_corruption() {
    // A retained semantic member must have the exact reverse owner/ordinal in
    // the one unified ChangeCatalog.
    let mut wrong_owner = build_seed();
    let commit_id = wrong_owner.commit_id;
    let commit_object_id = wrong_owner.commit_object_id;
    let semantic_change_id = wrong_owner.semantic_change_id;
    let semantic_change_object_id = wrong_owner.semantic_change_object_id;
    let ref_change_id = wrong_owner.ref_change_id;
    let ref_change_object_id = wrong_owner.ref_change_object_id;
    let branch_id = wrong_owner.branch_id;
    replace_selected_history_graph(
        &mut wrong_owner,
        &[(commit_id, CommitCatalogEntry { commit_object_id })],
        &[
            (
                semantic_change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id,
                        ordinal: 1,
                    },
                },
            ),
            (
                ref_change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::BranchRef {
                        ref_change_object_id,
                        branch_id,
                    },
                },
            ),
        ],
        commit_object_id,
        ref_change_object_id,
    );
    let storage = Memory::new();
    seed_storage(&storage, &wrong_owner).await;
    assert!(sweep_result(&storage).await.is_err());

    // RefChange chronology is authenticated by the explicit predecessor edge
    // and the predecessor-after/successor-before head link. ChangeIds are
    // independent identities and must not be ordered as UUIDs.
    let mut out_of_uuid_order = build_seed();
    let next_commit = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(0x21)),
        generation: 2,
        parent_commit_object_ids: vec![out_of_uuid_order.commit_object_id],
        members: Vec::new(),
        member_page_object_ids: Vec::new(),
        global_state_root: out_of_uuid_order.global_state_root,
        local_state_root: out_of_uuid_order.local_state_root,
        checkpoint_cursor: {
            let parent = CommitObjectV1::decode(
                out_of_uuid_order.commit_object_id,
                out_of_uuid_order
                    .objects
                    .get(out_of_uuid_order.commit_object_id)
                    .expect("branch parent object"),
            )
            .expect("branch parent commit");
            CheckpointCursorV1::after_first_parent(
                out_of_uuid_order.commit_object_id,
                &parent,
                out_of_uuid_order.branch_id,
                false,
            )
            .expect("branch checkpoint cursor")
        },
        metadata: b"next branch head".to_vec(),
    };
    let (next_commit_object_id, next_commit_bytes) =
        next_commit.encode().expect("next branch commit");
    out_of_uuid_order
        .objects
        .insert(next_commit_object_id, next_commit_bytes)
        .expect("next branch commit object");
    let next_ref = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0x01)),
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
        branch_id: out_of_uuid_order.branch_id,
        before_semantic_head_commit_object_id: Some(out_of_uuid_order.commit_object_id),
        after_semantic_head_commit_object_id: Some(next_commit_object_id),
        previous_ref_change_object_id: Some(out_of_uuid_order.ref_change_object_id),
        payload: b"lower UUID identity, valid predecessor".to_vec(),
        json_payload_object_ids: Vec::new(),
    };
    let (next_ref_object_id, next_ref_bytes) = next_ref.encode().expect("next branch ref");
    out_of_uuid_order
        .objects
        .insert(next_ref_object_id, next_ref_bytes)
        .expect("next branch ref object");
    let seed_commit_id = out_of_uuid_order.commit_id;
    let seed_commit_object_id = out_of_uuid_order.commit_object_id;
    let seed_ref_change_id = out_of_uuid_order.ref_change_id;
    let seed_ref_change_object_id = out_of_uuid_order.ref_change_object_id;
    let seed_branch_id = out_of_uuid_order.branch_id;
    let mut valid_changes = seed_member_catalog_entries(&out_of_uuid_order, seed_commit_object_id);
    valid_changes.push((
        next_ref.change_id(),
        ChangeCatalogEntry {
            owner: ChangeCatalogOwner::BranchRef {
                ref_change_object_id: next_ref_object_id,
                branch_id: seed_branch_id,
            },
        },
    ));
    valid_changes.push((
        seed_ref_change_id,
        ChangeCatalogEntry {
            owner: ChangeCatalogOwner::BranchRef {
                ref_change_object_id: seed_ref_change_object_id,
                branch_id: seed_branch_id,
            },
        },
    ));
    let storage = Memory::new();
    replace_selected_history_graph(
        &mut out_of_uuid_order,
        &[
            (
                seed_commit_id,
                CommitCatalogEntry {
                    commit_object_id: seed_commit_object_id,
                },
            ),
            (
                next_commit.commit_id,
                CommitCatalogEntry {
                    commit_object_id: next_commit_object_id,
                },
            ),
        ],
        &valid_changes,
        next_commit_object_id,
        next_ref_object_id,
    );
    seed_storage(&storage, &out_of_uuid_order).await;
    sweep_result(&storage)
        .await
        .expect("authenticated predecessor linkage accepts UUID-independent ChangeIds");

    // A RefChange predecessor must be on the same branch and its after target
    // must equal the successor's before target.
    let mut bad_ref_history = build_seed();
    let latest = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0x33)),
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(1),
        branch_id: bad_ref_history.branch_id,
        before_semantic_head_commit_object_id: None,
        after_semantic_head_commit_object_id: Some(bad_ref_history.commit_object_id),
        previous_ref_change_object_id: Some(bad_ref_history.ref_change_object_id),
        payload: b"broken-ref-chronology".to_vec(),
        json_payload_object_ids: Vec::new(),
    };
    let (latest_id, latest_bytes) = latest.encode().expect("bad chronology ref");
    bad_ref_history
        .objects
        .insert(latest_id, latest_bytes)
        .expect("bad chronology ref object");
    let commit_id = bad_ref_history.commit_id;
    let commit_object_id = bad_ref_history.commit_object_id;
    let semantic_change_id = bad_ref_history.semantic_change_id;
    let semantic_change_object_id = bad_ref_history.semantic_change_object_id;
    let ref_change_id = bad_ref_history.ref_change_id;
    let ref_change_object_id = bad_ref_history.ref_change_object_id;
    let branch_id = bad_ref_history.branch_id;
    replace_selected_history_graph(
        &mut bad_ref_history,
        &[(commit_id, CommitCatalogEntry { commit_object_id })],
        &[
            (
                semantic_change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id,
                        ordinal: 0,
                    },
                },
            ),
            (
                ref_change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::BranchRef {
                        ref_change_object_id,
                        branch_id,
                    },
                },
            ),
            (
                latest.change_id(),
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::BranchRef {
                        ref_change_object_id: latest_id,
                        branch_id,
                    },
                },
            ),
        ],
        commit_object_id,
        latest_id,
    );
    let storage = Memory::new();
    seed_storage(&storage, &bad_ref_history).await;
    assert!(sweep_result(&storage).await.is_err());

    // Every retained parent generation must be strictly less than its child.
    let mut bad_generation = build_seed();
    let parent = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(0x41)),
        generation: 2,
        parent_commit_object_ids: Vec::new(),
        members: Vec::new(),
        member_page_object_ids: Vec::new(),
        global_state_root: bad_generation.global_state_root,
        local_state_root: bad_generation.local_state_root,
        checkpoint_cursor: CheckpointCursorV1::root(),
        metadata: b"parent".to_vec(),
    };
    let (parent_id, parent_bytes) = parent.encode().expect("parent commit");
    bad_generation
        .objects
        .insert(parent_id, parent_bytes)
        .expect("parent object");
    let child_commit_id = CommitId::from_bytes(raw_id(0x42));
    let child_members = vec![seed_commit_members(&bad_generation)[0].clone()];
    let child_page_ids =
        insert_test_change_pages(&mut bad_generation.objects, child_commit_id, &child_members);
    let child = CommitObjectV1 {
        commit_id: child_commit_id,
        generation: 2,
        parent_commit_object_ids: vec![parent_id],
        members: child_members,
        member_page_object_ids: child_page_ids,
        global_state_root: bad_generation.global_state_root,
        local_state_root: bad_generation.local_state_root,
        checkpoint_cursor: CheckpointCursorV1::after_first_parent(
            parent_id,
            &parent,
            bad_generation.branch_id,
            false,
        )
        .expect("bad-generation checkpoint cursor"),
        metadata: b"child".to_vec(),
    };
    let (child_id, child_bytes) = child.encode().expect("child commit");
    bad_generation
        .objects
        .insert(child_id, child_bytes)
        .expect("child object");
    let creation = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0x43)),
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(1),
        branch_id: bad_generation.branch_id,
        before_semantic_head_commit_object_id: None,
        after_semantic_head_commit_object_id: Some(child_id),
        previous_ref_change_object_id: None,
        payload: b"generation-branch".to_vec(),
        json_payload_object_ids: Vec::new(),
    };
    let (creation_id, creation_bytes) = creation.encode().expect("creation ref");
    bad_generation
        .objects
        .insert(creation_id, creation_bytes)
        .expect("creation ref object");
    let semantic_change_id = bad_generation.semantic_change_id;
    let branch_id = bad_generation.branch_id;
    replace_selected_history_graph(
        &mut bad_generation,
        &[
            (
                parent.commit_id,
                CommitCatalogEntry {
                    commit_object_id: parent_id,
                },
            ),
            (
                child.commit_id,
                CommitCatalogEntry {
                    commit_object_id: child_id,
                },
            ),
        ],
        &[
            (
                semantic_change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id: child_id,
                        ordinal: 0,
                    },
                },
            ),
            (
                creation.change_id(),
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::BranchRef {
                        ref_change_object_id: creation_id,
                        branch_id,
                    },
                },
            ),
        ],
        child_id,
        creation_id,
    );
    let storage = Memory::new();
    seed_storage(&storage, &bad_generation).await;
    assert!(sweep_result(&storage).await.is_err());
}

fn make_part(
    upload_id: &CanonicalUploadId,
    part_number: u64,
    byte_offset: u64,
    payload: &'static [u8],
) -> (BlobChunkV1, UploadPartV1) {
    let chunk = BlobChunkV1 {
        bytes: Bytes::from_static(payload),
    };
    let (chunk_id, _) = chunk.encode().expect("chunk");
    let part = UploadPartV1 {
        upload_id: upload_id.clone(),
        part_number,
        byte_offset,
        declared_part_len: payload.len() as u64,
        ordered_chunks: vec![BlobChunkRefV1 {
            chunk_object_id: chunk_id,
            declared_len: payload.len() as u64,
        }],
        part_digest: *blake3::hash(payload).as_bytes(),
    };
    (chunk, part)
}

#[derive(Clone)]
struct UploadData {
    upload_id: CanonicalUploadId,
    chunk: BlobChunkV1,
    chunk_id: ObjectId,
    part: UploadPartV1,
    receipt: ReceiptTreeEdit,
    progress: UploadProgressV1,
    progress_id: ObjectId,
    selector: UploadSelectorV1,
}

fn make_upload() -> UploadData {
    let upload_id = CanonicalUploadId::new("upload").expect("upload ID");
    let binding = upload_binding_digest(
        b"repository",
        b"/blob.bin",
        b"file",
        4,
        Some(*blake3::hash(b"data").as_bytes()),
    )
    .expect("binding");
    let initial = empty_receipt_tree().expect("empty receipt");
    let (chunk, part) = make_part(&upload_id, 0, 0, b"data");
    let (chunk_id, chunk_bytes) = chunk.encode().expect("chunk");
    let (part_id, part_bytes) = part.encode().expect("part");
    let mut arena = initial.objects;
    arena.insert(chunk_id, chunk_bytes).expect("chunk arena");
    arena.insert(part_id, part_bytes).expect("part arena");
    let receipt = insert_receipt_part(initial.root, part_id, &part, load_from(&arena))
        .expect("receipt insert");
    let progress = UploadProgressV1 {
        upload_id: upload_id.clone(),
        binding_digest: binding,
        receipt_tree_root: receipt.root.object_id,
        completed_part_count: receipt.root.completed_part_count,
        received_bytes: receipt.root.received_bytes,
        contiguous_prefix_bytes: receipt.root.contiguous_prefix_bytes,
    };
    let (progress_id, _) = progress.encode().expect("progress");
    let selector = UploadSelectorV1 {
        upload_id: upload_id.clone(),
        binding_digest: binding,
        progress_object_id: progress_id,
        selector_generation: 1,
    };
    UploadData {
        upload_id,
        chunk,
        chunk_id,
        part,
        receipt,
        progress,
        progress_id,
        selector,
    }
}

fn stage_upload(publication: &mut PreparedPublication, upload: &UploadData) {
    publication
        .publish_new_upload(
            std::slice::from_ref(&upload.chunk),
            std::slice::from_ref(&upload.part),
            upload.receipt.clone(),
            &upload.progress,
            &upload.selector,
        )
        .expect("publish typed upload closure");
}

#[test]
fn receipt_tree_is_path_copied_bounded_and_has_no_predecessor() {
    let upload_id = CanonicalUploadId::new("many-parts").expect("upload ID");
    let initial = empty_receipt_tree().expect("empty");
    assert_eq!(RECEIPT_TREE_LEAF_ENTRIES, 64);
    assert_eq!(RECEIPT_TREE_FANOUT, 32);
    let mut arena = initial.objects;
    let mut root: ReceiptTreeRoot = initial.root;
    for part_number in (0_u64..70).map(|part| (part * 17) % 70) {
        let payload = Box::leak(vec![part_number as u8; 8].into_boxed_slice());
        let (chunk, part) = make_part(&upload_id, part_number, part_number * 8, payload);
        let (chunk_id, chunk_bytes) = chunk.encode().expect("chunk");
        let (part_id, part_bytes) = part.encode().expect("part");
        arena.insert(chunk_id, chunk_bytes).expect("chunk arena");
        arena.insert(part_id, part_bytes).expect("part arena");
        let edit =
            insert_receipt_part(root, part_id, &part, load_from(&arena)).expect("receipt edit");
        assert!(edit.copied_nodes <= 4);
        root = edit.root;
        arena.extend(edit.objects).expect("receipt nodes");
    }
    assert_eq!(root.completed_part_count, 70);
    assert_eq!(root.contiguous_prefix_bytes, 560);
    let parts = validate_receipt_tree(root, &upload_id, load_from(&arena)).expect("closure");
    assert_eq!(parts.len(), 70);
    let duplicate = &parts[32];
    let duplicate_id = ObjectId::from_bytes(
        lookup(
            root.object_id,
            "receipt",
            &duplicate.part_number.to_be_bytes(),
            load_from(&arena),
        )
        .expect("lookup")
        .expect("part")
        .try_into()
        .expect("part ID"),
    );
    let duplicate_edit =
        insert_receipt_part(root, duplicate_id, duplicate, load_from(&arena)).expect("duplicate");
    assert!(!duplicate_edit.inserted);
    assert!(duplicate_edit.objects.is_empty());
}

#[test]
fn receipt_declared_size_digest_and_aggregate_corruption_fail_closed() {
    let upload = make_upload();
    let mut arena = upload.receipt.objects.clone();
    let (chunk_id, chunk_bytes) = upload.chunk.encode().expect("chunk");
    let (part_id, part_bytes) = upload.part.encode().expect("part");
    arena.insert(chunk_id, chunk_bytes).expect("chunk arena");
    arena.insert(part_id, part_bytes).expect("part arena");
    validate_upload_progress_tree(&upload.progress, load_from(&arena)).expect("progress closure");
    let wrong = UploadProgressV1 {
        completed_part_count: 2,
        ..upload.progress.clone()
    };
    assert!(validate_upload_progress_tree(&wrong, load_from(&arena)).is_err());
    let wrong_part = UploadPartV1 {
        declared_part_len: 5,
        ..upload.part.clone()
    };
    assert!(wrong_part.encode().is_err());
    let wrong_selector = UploadSelectorV1 {
        binding_digest: [9; 32],
        ..upload.selector.clone()
    };
    arena
        .insert(
            upload.progress_id,
            upload.progress.encode().expect("progress").1,
        )
        .expect("progress arena");
    assert!(validate_upload_selector_progress(&wrong_selector, load_from(&arena)).is_err());
}

#[tokio::test]
async fn multipart_completion_missing_prior_child_fails_before_manifest_publication() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let upload_id = CanonicalUploadId::new("missing-prior-child").expect("upload ID");
    let chunk_bytes = usize::try_from(BLOB_MERKLE_CHUNK_BYTES).expect("chunk size fits usize");
    let part_bytes = chunk_bytes * 16;
    let total_size = part_bytes as u64 + 1;
    let first = vec![0x41; part_bytes];
    let tail = [0x42];
    let binding = UploadBindingRef {
        repository_identity: b"repository",
        path: b"/missing-prior-child.bin",
        payload_domain: b"file",
        declared_total_size: total_size,
        declared_final_hash: None,
    };

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("first-part view");
    let first_part = prepare_upload_part(&view, upload_id.clone(), binding, 0, 0, &first)
        .await
        .expect("first part preparation");
    let missing_child_id = first_part.chunks[0].encode().expect("first chunk").0;
    let mut publication = PreparedPublication::from_branch_view(&view).expect("first part");
    publication
        .publish_upload_part(first_part)
        .expect("stage first part closure");
    drop(view);
    commit_publication_for_test(publication, &storage)
        .await
        .expect("publish first part");

    let expected_manifest_id = {
        let chunks = first
            .chunks(chunk_bytes)
            .map(|bytes| BlobChunkV1 {
                bytes: Bytes::copy_from_slice(bytes),
            })
            .chain(std::iter::once(BlobChunkV1 {
                bytes: Bytes::copy_from_slice(&tail),
            }))
            .collect::<Vec<_>>();
        build_blob_merkle_tree(&chunks)
            .expect("expected Merkle closure")
            .manifest
            .encode()
            .expect("expected manifest")
            .0
    };

    let mut deletion = StorageWriteSet::new();
    deletion.delete(OBJECT_SPACE, missing_child_id.as_bytes().to_vec());
    commit_write_set_for_test(deletion, &storage).await;

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("completion view");
    let error = prepare_upload_part(
        &view,
        upload_id.clone(),
        binding,
        1,
        part_bytes as u64,
        &tail,
    )
    .await
    .expect_err("missing prior child must reject completion");
    assert!(error.to_string().contains("absent"));
    assert!(
        view.load_selector_value(&upload_selector_key(&upload_id).expect("selector key"))
            .await
            .expect("selector lookup")
            .is_some(),
        "failed completion must retain the open upload selector"
    );
    drop(view);
    assert!(
        !object_present(&storage, expected_manifest_id).await,
        "failed completion must not publish a partial manifest"
    );
}

#[tokio::test]
async fn upload_publication_and_sweep_are_epoch_fenced_in_both_orders() {
    let seed = build_seed();
    let upload = make_upload();

    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let publish_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("publish view");
    let mut publish = PreparedPublication::from_global_epoch(&publish_view).expect("publish");
    stage_upload(&mut publish, &upload);
    drop(publish_view);
    commit_publication_for_test(publish, &storage)
        .await
        .expect("receipt first");
    sweep(&storage, seed.branch_id).await;
    assert!(object_present(&storage, upload.progress_id).await);

    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let publish_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("publish view");
    let mut stale_publish = PreparedPublication::from_global_epoch(&publish_view).expect("publish");
    stage_upload(&mut stale_publish, &upload);
    drop(publish_view);
    sweep(&storage, seed.branch_id).await;
    assert!(matches!(
        commit_publication_for_test(stale_publish, &storage).await,
        Err(StorageError::PreconditionFailed(_))
    ));
    let retry_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("retry view");
    let mut retry = PreparedPublication::from_global_epoch(&retry_view).expect("retry");
    stage_upload(&mut retry, &upload);
    drop(retry_view);
    commit_publication_for_test(retry, &storage)
        .await
        .expect("retry publication");
    assert!(object_present(&storage, upload.chunk_id).await);
}

#[tokio::test]
async fn publication_cancels_active_gc_without_becoming_a_global_writer_lock() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    assert_eq!(
        advance_gc(&storage, GcBudget::default())
            .await
            .expect("start GC"),
        GcStepStatus::Started
    );
    assert!(
        selector_present(&storage, gc_progress_selector_key()).await,
        "active bounded GC must publish its rebuildable continuation"
    );

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("publication view during GC");
    let publication = PreparedPublication::from_global_epoch(&view).expect("publication");
    drop(view);
    commit_publication_for_test(publication, &storage)
        .await
        .expect("publication must atomically invalidate active GC");
    assert!(
        !selector_present(&storage, gc_progress_selector_key()).await,
        "semantic publication must discard only the rebuildable GC selector"
    );
    assert_eq!(
        advance_gc(&storage, GcBudget::default())
            .await
            .expect("restart GC"),
        GcStepStatus::Started
    );
    sweep(&storage, seed.branch_id).await;
}

#[tokio::test]
async fn deterministic_reader_pin_safe_point_and_reclamation_oracle() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let checkpoint_id = SnapshotSelectorId::from_bytes(raw_id(0xe0));

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("checkpoint view");
    let mut checkpoint = PreparedPublication::from_global_epoch(&view).expect("checkpoint");
    let target_id = checkpoint
        .publish_current_snapshot_pin(
            &view,
            SnapshotRole::Checkpoint,
            checkpoint_id,
            SelectorExpectation::Absent,
        )
        .expect("checkpoint target");
    drop(view);
    commit_publication_for_test(checkpoint, &storage)
        .await
        .expect("checkpoint commit");

    let old_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("old coherent view");
    assert!(old_view.load_object_bytes(target_id).await.is_ok());

    let current = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("release view");
    let selector_key = snapshot_selector_key(SnapshotRole::Checkpoint, checkpoint_id);
    let keys = [Key(selector_key)];
    let loaded = current
        .test_storage_read()
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("checkpoint selector");
    let raw_selector = match loaded.values.as_slice() {
        [Some(crate::storage::ProjectedValue::FullValue(bytes))] => bytes.clone(),
        other => panic!("expected checkpoint selector, got {other:?}"),
    };
    let selector = SnapshotSelectorV1::decode(&raw_selector).expect("checkpoint selector");
    let commit_catalog_edit = retire_commit_catalog_entries(
        current.repository_root().commit_catalog_root,
        &[],
        current.test_storage_read(),
    )
    .await
    .expect("unchanged commit catalog");
    let change_catalog_edit = retire_change_catalog_entries(
        current.repository_root().change_catalog_root,
        &[],
        current.test_storage_read(),
    )
    .await
    .expect("unchanged change catalog");
    let repository = RepositoryRootV1 {
        commit_catalog_root: commit_catalog_edit.root,
        change_catalog_root: change_catalog_edit.root,
        ..current.repository_root()
    };
    let mut release = PreparedPublication::from_global_epoch(&current).expect("release");
    release
        .release_snapshot_pin_with_catalog_retirement(
            &current,
            selector,
            raw_selector,
            commit_catalog_edit,
            change_catalog_edit,
            repository,
        )
        .expect("release checkpoint");
    drop(current);
    commit_publication_for_test(release, &storage)
        .await
        .expect("release commit");
    sweep(&storage, seed.branch_id).await;

    assert!(!object_present(&storage, target_id).await);
    assert!(
        old_view.load_object_bytes(target_id).await.is_ok(),
        "the retained StorageRead must continue to authenticate its old object version"
    );
    let new_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("new coherent view");
    drop(new_view);
    drop(old_view);

    let reopened = storage.clone();
    assert!(!object_present(&reopened, target_id).await);
}

#[tokio::test]
async fn deterministic_crash_recovery_publication_and_gc_oracle() {
    let seed = build_seed();
    for (crash, committed) in [(CommitCrash::Before, false), (CommitCrash::After, true)] {
        let storage = CrashStorage::new();
        seed_storage(&storage, &seed).await;
        let checkpoint_id =
            SnapshotSelectorId::from_bytes(raw_id(if committed { 0xf1 } else { 0xf0 }));
        let view = open_coherent_view(&storage, seed.branch_id)
            .await
            .expect("publication view");
        let mut publication = PreparedPublication::from_global_epoch(&view).expect("publication");
        let target_id = publication
            .publish_current_snapshot_pin(
                &view,
                SnapshotRole::Recovery,
                checkpoint_id,
                SelectorExpectation::Absent,
            )
            .expect("recovery pin");
        drop(view);
        storage.inject(crash);
        assert!(
            commit_publication_for_test(publication, &storage)
                .await
                .is_err()
        );

        let reopened = storage.reopen();
        assert_eq!(
            selector_present(
                &reopened,
                snapshot_selector_key(SnapshotRole::Recovery, checkpoint_id),
            )
            .await,
            committed
        );
        assert_eq!(object_present(&reopened, target_id).await, committed);
        open_coherent_view(&reopened, seed.branch_id)
            .await
            .expect("repository must reopen entirely old or entirely new");
    }

    for crash in [CommitCrash::Before, CommitCrash::After] {
        let storage = CrashStorage::new();
        seed_storage(&storage, &seed).await;
        let view = open_coherent_view(&storage, seed.branch_id)
            .await
            .expect("orphan view");
        let mut orphan = PreparedPublication::from_global_epoch(&view).expect("orphan");
        let target = SnapshotTargetV1 {
            role: SnapshotRole::Checkpoint,
            selector_id: SnapshotSelectorId::from_bytes(raw_id(0xf2)),
            branch_id: seed.branch_id,
            branch_snapshot_object_id: seed.branch_snapshot_id,
            semantic_commit_object_id: seed.commit_object_id,
        };
        let orphan_id = orphan.stage_snapshot_target(target).expect("orphan target");
        drop(view);
        commit_publication_for_test(orphan, &storage)
            .await
            .expect("stage orphan");

        storage.inject(crash);
        assert!(advance_gc(&storage, GcBudget::default()).await.is_err());
        let reopened = storage.reopen();
        assert!(object_present(&reopened, orphan_id).await);
        sweep(&reopened, seed.branch_id).await;
        assert!(!object_present(&reopened, orphan_id).await);
        open_coherent_view(&reopened, seed.branch_id)
            .await
            .expect("GC crash recovery must preserve the selected graph");
    }
}

#[tokio::test]
async fn corrupted_persisted_gc_index_fails_closed_without_authorizing_deletion() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    assert_eq!(
        advance_gc(&storage, GcBudget::default())
            .await
            .expect("start GC"),
        GcStepStatus::Started
    );
    advance_gc(&storage, GcBudget::default())
        .await
        .expect("collect selector roots");

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("GC state read");
    let selector_keys = [Key(gc_progress_selector_key())];
    let selector_result = read
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &selector_keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("GC selector");
    let raw_selector = match selector_result.values.as_slice() {
        [Some(crate::storage::ProjectedValue::FullValue(bytes))] => bytes,
        other => panic!("expected GC selector, got {other:?}"),
    };
    let expected_raw_selector = raw_selector.clone();
    let selector = GcProgressSelectorV2::decode(raw_selector).expect("GC selector decode");
    let progress_keys = [Key(Bytes::copy_from_slice(
        selector.progress_object_id.as_bytes(),
    ))];
    let progress_result = read
        .get_many(&[GetManyRequest {
            space: OBJECT_SPACE,
            keys: &progress_keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("GC progress");
    let raw_progress = match progress_result.values.as_slice() {
        [Some(crate::storage::ProjectedValue::FullValue(bytes))] => bytes,
        other => panic!("expected GC progress object, got {other:?}"),
    };
    let progress = GcProgressV2::decode(selector.progress_object_id, raw_progress)
        .expect("GC progress decode");
    let mark_root = progress
        .mark_index_root
        .expect("selector roots produced marks");
    drop(read);

    let mut writes = StorageWriteSet::new();
    writes.delete(OBJECT_SPACE, mark_root.as_bytes().to_vec());
    commit_write_set_for_test(writes, &storage).await;

    assert!(advance_gc(&storage, GcBudget::default()).await.is_err());
    assert!(object_present(&storage, seed.repository_root_id).await);
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("post-corruption GC selector read");
    let loaded = read
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &[Key(gc_progress_selector_key())],
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("post-corruption GC selector");
    assert_eq!(
        loaded.values.as_slice(),
        &[Some(crate::storage::ProjectedValue::FullValue(
            expected_raw_selector,
        ))],
        "corrupt maintenance state must not authorize selector mutation",
    );
    open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("semantic graph survives maintenance corruption");
}

#[tokio::test]
async fn state_and_catalog_publication_inputs_are_bound_to_the_selected_view() {
    let mut seed = build_seed();
    let selected_head = seed.commit_object_id;
    let (_, unrelated_parent_id) = insert_graph_commit(&mut seed, 0x70, 2, vec![selected_head]);
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("view");

    assert!(
        edit_state_tree(
            view.branch_snapshot().local_state_root,
            vec![StateTreeMutation::remove(vec![0xff])],
            view.test_storage_read(),
        )
        .await
        .is_err(),
        "opaque state keys must fail before path copying"
    );
    assert!(
        put_commit_catalog_entries(
            view.repository_root().commit_catalog_root,
            &[
                (
                    CommitId::from_bytes(raw_id(0x72)),
                    CommitCatalogEntry {
                        commit_object_id: content_id(0x72),
                    },
                ),
                (
                    CommitId::from_bytes(raw_id(0x71)),
                    CommitCatalogEntry {
                        commit_object_id: content_id(0x71),
                    },
                ),
            ],
            view.test_storage_read(),
        )
        .await
        .is_err(),
        "catalog updates must use canonical raw-UUID order"
    );

    let reordered_state = edit_state_tree(
        view.branch_snapshot().local_state_root,
        Vec::new(),
        view.test_storage_read(),
    )
    .await
    .expect("reordered-parent state edit");
    let mut reordered = branch_transition(&view, reordered_state, 0x71).await;
    let unrelated_bytes = view
        .load_object_bytes(unrelated_parent_id)
        .await
        .expect("unrelated parent object");
    let unrelated_parent = CommitObjectV1::decode(unrelated_parent_id, &unrelated_bytes)
        .expect("unrelated parent commit");
    reordered.semantic_commit.parent_commit_object_ids = vec![unrelated_parent_id, selected_head];
    reordered.semantic_commit.checkpoint_cursor = CheckpointCursorV1::after_first_parent(
        unrelated_parent_id,
        &unrelated_parent,
        view.branch_id(),
        false,
    )
    .expect("cursor derived from unrelated first parent");
    let mut publication = PreparedPublication::from_branch_view(&view).expect("publication");
    let error = publication
        .publish_state_transition(&view, reordered)
        .await
        .expect_err("selected head in a non-first parent position must fail before staging");
    assert!(
        matches!(
            error,
            StorageError::Corruption(ref message)
                if message.contains("first parent is not the selected branch head")
        ),
        "parent ordering must fail at the publisher authority boundary: {error:?}",
    );

    let wrong_base = edit_state_tree(
        view.repository_root().global_state_root,
        Vec::new(),
        view.test_storage_read(),
    )
    .await
    .expect("wrong-base edit remains a valid standalone tree edit");
    let transition = branch_transition(&view, wrong_base, 0x72).await;
    let mut publication = PreparedPublication::from_branch_view(&view).expect("publication");
    assert!(
        publication
            .publish_state_transition(&view, transition)
            .await
            .is_err(),
        "a valid edit from another selected root must not publish"
    );

    let (key, value) = state_entry("wrong-commit", StateCellRef::Value("value"), 0x73, &[]);
    let wrong_commit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![StateTreeMutation::insert_bound(
            key,
            value,
            StateMutationAudit {
                commit_id: raw_id(0x73),
                tombstone: false,
                blob_manifest_object_ids: Vec::new(),
            },
        )],
        view.test_storage_read(),
    )
    .await
    .expect("typed state edit");
    let transition = branch_transition(&view, wrong_commit, 0x74).await;
    let mut publication = PreparedPublication::from_branch_view(&view).expect("publication");
    assert!(
        publication
            .publish_state_transition(&view, transition)
            .await
            .is_err(),
        "state rows must authenticate the semantic commit that publishes them"
    );
}

#[tokio::test]
async fn state_edit_rejects_unsorted_and_duplicate_encoded_keys() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("view");

    let (a_key, a_value) = state_entry("a", StateCellRef::Value("a"), 0x90, &[]);
    let (z_key, z_value) = state_entry("z", StateCellRef::Value("z"), 0x91, &[]);
    let unsorted = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![
            StateTreeMutation::insert(z_key, z_value),
            StateTreeMutation::insert(a_key.clone(), a_value.clone()),
        ],
        view.test_storage_read(),
    )
    .await
    .expect_err("unsorted encoded mutations must fail closed");
    assert!(matches!(
        unsorted,
        StorageError::Corruption(message)
            if message.contains("ordered-tree mutations are not strictly ordered and distinct")
    ));

    let (_, duplicate_value) = state_entry("a", StateCellRef::Value("replacement"), 0x92, &[]);
    let duplicate = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![
            StateTreeMutation::insert(a_key, a_value),
            StateTreeMutation::insert(
                encode_state_key(StateKeyRef {
                    schema_key: "app.row",
                    file_id: Some("file"),
                    row_pk: &RowPk::single("a"),
                }),
                duplicate_value,
            ),
        ],
        view.test_storage_read(),
    )
    .await
    .expect_err("duplicate encoded mutations must fail closed");
    assert!(matches!(
        duplicate,
        StorageError::Corruption(message)
            if message.contains("ordered-tree mutations are not strictly ordered and distinct")
    ));
}

#[tokio::test]
async fn upload_abort_releases_receipt_closure_after_final_selector_move() {
    let seed = build_seed();
    let upload = make_upload();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("upload view");
    let mut publication = PreparedPublication::from_global_epoch(&view).expect("upload");
    stage_upload(&mut publication, &upload);
    drop(view);
    commit_publication_for_test(publication, &storage)
        .await
        .expect("publish upload");

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("abort view");
    let keys = [Key(
        upload_selector_key(&upload.upload_id).expect("upload key")
    )];
    let loaded = view
        .test_storage_read()
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("selector read");
    let raw = match loaded.values.as_slice() {
        [Some(crate::storage::ProjectedValue::FullValue(bytes))] => bytes.clone(),
        other => panic!("expected upload selector, got {other:?}"),
    };
    let mut abort = PreparedPublication::from_global_epoch(&view).expect("abort");
    abort
        .abort_upload(&upload.selector, raw)
        .expect("typed abort");
    drop(view);
    commit_publication_for_test(abort, &storage)
        .await
        .expect("abort commit");
    sweep(&storage, seed.branch_id).await;
    assert!(!object_present(&storage, upload.progress_id).await);
    assert!(!object_present(&storage, upload.chunk_id).await);
}

#[tokio::test]
async fn upload_completion_moves_receipt_to_tracked_state_atomically() {
    let seed = build_seed();
    let upload = make_upload();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("upload view");
    let mut publication = PreparedPublication::from_global_epoch(&view).expect("upload");
    stage_upload(&mut publication, &upload);
    drop(view);
    commit_publication_for_test(publication, &storage)
        .await
        .expect("publish receipt");

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("completion view");
    let completion = prepare_upload_completion(
        &view,
        &upload.upload_id,
        UploadBindingRef {
            repository_identity: b"repository",
            path: b"/blob.bin",
            payload_domain: b"file",
            declared_total_size: 4,
            declared_final_hash: Some(*blake3::hash(b"data").as_bytes()),
        },
    )
    .await
    .expect("completion proof");
    let manifest = single_leaf_manifest_for_test(&upload.chunk)
        .expect("authenticated upload manifest")
        .0;
    let (manifest_id, _) = manifest.encode().expect("manifest");
    let blob_id = manifest.canonical_blob_id;
    let key_id = "01920000-0000-7000-8000-0000000000a1";
    let wrong_owner_value = serde_json::json!({
        "id": "not-blob",
        "blob_hash": blob_id.to_hex(),
        "size_bytes": 4,
    })
    .to_string();

    // A valid multi-chunk manifest must not be transplantable beneath a
    // same-size state owner carrying another public BlobId. The manifest's
    // canonical semantic identity is authenticated before range chunks are
    // selected, so this check remains O(chunk metadata + visited payload).
    const FIXED_CHUNK_BYTES: usize = 1024 * 1024;
    let owner_payload = vec![b'a'; FIXED_CHUNK_BYTES + 1];
    let transplanted_payload = vec![b'b'; FIXED_CHUNK_BYTES + 1];
    let owner_chunks = [
        BlobChunkV1 {
            bytes: Bytes::copy_from_slice(&owner_payload[..FIXED_CHUNK_BYTES]),
        },
        BlobChunkV1 {
            bytes: Bytes::copy_from_slice(&owner_payload[FIXED_CHUNK_BYTES..]),
        },
    ];
    let transplanted_chunks = [
        BlobChunkV1 {
            bytes: Bytes::copy_from_slice(&transplanted_payload[..FIXED_CHUNK_BYTES]),
        },
        BlobChunkV1 {
            bytes: Bytes::copy_from_slice(&transplanted_payload[FIXED_CHUNK_BYTES..]),
        },
    ];
    let owner_build = build_blob_merkle_tree(&owner_chunks).expect("owner Merkle manifest");
    let owner_manifest = owner_build.manifest;
    let transplanted_build =
        build_blob_merkle_tree(&transplanted_chunks).expect("transplanted Merkle manifest");
    let transplanted_manifest = transplanted_build.manifest;
    let owner_blob_id = owner_manifest.canonical_blob_id;
    let transplanted_blob_id = transplanted_manifest.canonical_blob_id;
    assert_ne!(owner_blob_id, transplanted_blob_id);
    let (transplanted_manifest_id, _) = transplanted_manifest
        .encode()
        .expect("transplanted manifest");
    let test_entries = vec![
        test_state_member(
            "not-blob",
            StateCellRef::Value(&wrong_owner_value),
            0x70,
            &[manifest_id],
            false,
        ),
        test_blob_ref_member(key_id, key_id, blob_id, 4, 0x70, manifest_id),
        test_blob_ref_member(
            "01920000-0000-7000-8000-0000000000a2",
            "01920000-0000-7000-8000-0000000000a2",
            blob_id,
            5,
            0x70,
            manifest_id,
        ),
        test_blob_ref_member(
            "01920000-0000-7000-8000-0000000000a3",
            "01920000-0000-7000-8000-0000000000a3",
            owner_blob_id,
            owner_payload.len() as u64,
            0x70,
            transplanted_manifest_id,
        ),
        test_blob_ref_member(
            "01920000-0000-7000-8000-0000000000a4",
            "01920000-0000-7000-8000-0000000000a4",
            transplanted_blob_id,
            transplanted_payload.len() as u64,
            0x70,
            transplanted_manifest_id,
        ),
    ];
    let (rows, members, _, pack_objects) = encode_test_state_entries(0x70, test_entries);
    let [
        wrong_owner_row,
        selected_row,
        mismatched_row,
        transplanted_row,
        valid_multichunk_row,
    ]: [(Vec<u8>, Vec<u8>); 5] = rows.try_into().expect("five state rows");
    let wrong_owner_key = wrong_owner_row.0.clone();
    let key = selected_row.0.clone();
    let mismatched_owner_key = mismatched_row.0.clone();
    let transplanted_owner_key = transplanted_row.0.clone();
    let valid_multichunk_key = valid_multichunk_row.0.clone();
    let bound_insert = |row: (Vec<u8>, Vec<u8>), manifests: Vec<ObjectId>| {
        StateTreeMutation::insert_bound(
            row.0,
            row.1,
            StateMutationAudit {
                commit_id: raw_id(0x70),
                tombstone: false,
                blob_manifest_object_ids: manifests,
            },
        )
    };
    let mut state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![
            bound_insert(wrong_owner_row, vec![manifest_id]),
            bound_insert(selected_row, vec![manifest_id]),
            bound_insert(mismatched_row, vec![manifest_id]),
            bound_insert(transplanted_row, vec![transplanted_manifest_id]),
            bound_insert(valid_multichunk_row, vec![transplanted_manifest_id]),
        ],
        view.test_storage_read(),
    )
    .await
    .expect("state edit");
    state_edit
        .stage_objects(pack_objects)
        .expect("stage completion current-state packs");
    let transition = branch_transition_with_members(&view, state_edit, 0x70, members).await;
    let mut publish = PreparedPublication::from_branch_view(&view).expect("completion publication");
    publish
        .stage_blob_merkle_build_for_test(&transplanted_build)
        .expect("stage transplanted Merkle closure");
    assert_eq!(
        publish
            .publish_completed_upload(&view, completion, transition)
            .await
            .expect("atomic handoff"),
        manifest_id
    );
    drop(view);
    commit_publication_for_test(publish, &storage)
        .await
        .expect("complete upload");

    let reopened = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("cold reopen");
    let row = state_point(&reopened, &key, false)
        .await
        .expect("blob state")
        .expect("blob row");
    assert_eq!(row.value.blob_manifest_object_ids, vec![manifest_id]);
    let blob_ref = reopened.bind_blob(&row).expect("authenticated blob edge");
    assert_eq!(blob_ref.semantic_id(), blob_id);
    let wrong_owner = state_point(&reopened, &wrong_owner_key, false)
        .await
        .expect("wrong-owner state")
        .expect("wrong-owner row");
    assert!(
        reopened.bind_blob(&wrong_owner).is_err(),
        "a non-BlobRef row cannot donate an otherwise valid manifest edge"
    );
    let mismatched_owner = state_point(&reopened, &mismatched_owner_key, false)
        .await
        .expect("mismatched-owner state")
        .expect("mismatched-owner row");
    let mismatched_ref = reopened
        .bind_blob(&mismatched_owner)
        .expect("authenticated malformed owner edge");
    assert!(
        reopened
            .load_blob_ranges_many(&[(mismatched_ref, 0..1)])
            .await
            .is_err(),
        "a transplanted manifest with mismatched owner size must fail closed"
    );
    let transplanted_owner = state_point(&reopened, &transplanted_owner_key, false)
        .await
        .expect("transplanted owner state")
        .expect("transplanted owner row");
    let transplanted_ref = reopened
        .bind_blob(&transplanted_owner)
        .expect("authenticated same-size transplanted owner edge");
    assert!(
        reopened
            .load_blob_ranges_many(&[(transplanted_ref.clone(), 0..1)])
            .await
            .is_err(),
        "a same-size multi-chunk manifest transplant must fail before range output"
    );
    assert!(
        reopened
            .load_blob_bytes_many(&[transplanted_ref])
            .await
            .is_err(),
        "a same-size multi-chunk manifest transplant must fail before full output"
    );
    let valid_multichunk_owner = state_point(&reopened, &valid_multichunk_key, false)
        .await
        .expect("valid multi-chunk state")
        .expect("valid multi-chunk row");
    let valid_multichunk_ref = reopened
        .bind_blob(&valid_multichunk_owner)
        .expect("valid multi-chunk owner edge");
    let valid_multichunk_range = reopened
        .load_blob_ranges_many(&[(valid_multichunk_ref, 0..1)])
        .await
        .expect("bounded valid multi-chunk range")
        .into_vec();
    assert_eq!(
        valid_multichunk_range[0]
            .as_ref()
            .expect("valid multi-chunk range value")
            .bytes
            .as_ref(),
        b"b".as_slice()
    );
    let same_selectors_different_read = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("second coherent read");
    assert!(
        same_selectors_different_read.bind_blob(&row).is_err(),
        "a state row must not be rebound to another coherent view"
    );
    assert!(
        same_selectors_different_read
            .load_blob_bytes_many(&[blob_ref.clone()])
            .await
            .is_err(),
        "an authenticated blob edge must not detach from its selecting StorageRead"
    );
    assert_eq!(
        reopened
            .load_blob_bytes_many(&[blob_ref.clone()])
            .await
            .expect("full blob read")
            .into_vec(),
        vec![Some(b"data".to_vec())]
    );
    let ranges = reopened
        .load_blob_ranges_many(&[(blob_ref, 1..3)])
        .await
        .expect("range blob read")
        .into_vec();
    assert_eq!(ranges.len(), 1);
    let range = ranges[0].as_ref().expect("range value");
    assert_eq!(range.bytes.as_ref(), b"at".as_slice());
    assert_eq!(range.total_size, 4);
    assert_eq!(range.range, 1..3);
    assert_eq!(
        BlobManifestV1::decode(
            manifest_id,
            &reopened
                .load_object_bytes(manifest_id)
                .await
                .expect("manifest"),
        )
        .expect("authenticated manifest")
        .logical_bytes,
        4
    );
    let selector_keys = [Key(upload_selector_key(&upload.upload_id).expect("key"))];
    let selector = reopened
        .test_storage_read()
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &selector_keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("selector read");
    assert_eq!(selector.values, vec![None]);
    drop(reopened);
    sweep(&storage, seed.branch_id).await;
    assert!(!object_present(&storage, upload.progress_id).await);
    assert!(object_present(&storage, manifest_id).await);
    assert!(object_present(&storage, upload.chunk_id).await);
}

#[tokio::test]
async fn exact_blob_reader_binds_duplicate_blob_ids_to_selected_state_key() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("duplicate-owner view");

    let valid_payload = b"aaaa";
    let wrong_payload = b"bbbb";
    let valid_chunk = BlobChunkV1 {
        bytes: Bytes::copy_from_slice(valid_payload),
    };
    let wrong_chunk = BlobChunkV1 {
        bytes: Bytes::copy_from_slice(wrong_payload),
    };
    let valid_build = build_blob_merkle_tree(std::slice::from_ref(&valid_chunk))
        .expect("valid duplicate manifest");
    let valid_manifest = valid_build.manifest;
    let wrong_build = build_blob_merkle_tree(std::slice::from_ref(&wrong_chunk))
        .expect("wrong duplicate manifest");
    let wrong_manifest = wrong_build.manifest;
    let semantic_id = valid_manifest.canonical_blob_id;
    // This is the exact duplicate-owner trap: the wrong row claims the
    // selected owner's semantic ID while pointing at different authenticated
    // Merkle leaf/chunk bytes.
    let (valid_manifest_id, _) = valid_manifest.encode().expect("valid duplicate manifest");
    let (wrong_manifest_id, _) = wrong_manifest.encode().expect("wrong duplicate manifest");
    let wrong_id = "01920000-0000-7000-8000-0000000000b1";
    let valid_id = "01920000-0000-7000-8000-0000000000b2";
    let wrong_identity_id = "01920000-0000-7000-8000-0000000000b3";
    let entries = vec![
        test_blob_ref_member(
            wrong_id,
            wrong_id,
            semantic_id,
            valid_payload.len() as u64,
            0x70,
            wrong_manifest_id,
        ),
        test_blob_ref_member(
            valid_id,
            valid_id,
            semantic_id,
            valid_payload.len() as u64,
            0x70,
            valid_manifest_id,
        ),
        test_blob_ref_member(
            wrong_identity_id,
            valid_id,
            semantic_id,
            valid_payload.len() as u64,
            0x70,
            valid_manifest_id,
        ),
    ];
    let (rows, members, _, pack_objects) = encode_test_state_entries(0x70, entries);
    let valid_key = rows[1].0.clone();
    let wrong_identity_key = rows[2].0.clone();
    let mut mutations = rows
        .into_iter()
        .zip([wrong_manifest_id, valid_manifest_id, valid_manifest_id])
        .map(|(row, manifest)| {
            StateTreeMutation::insert_bound(
                row.0,
                row.1,
                StateMutationAudit {
                    commit_id: raw_id(0x70),
                    tombstone: false,
                    blob_manifest_object_ids: vec![manifest],
                },
            )
        })
        .collect::<Vec<_>>();
    mutations.sort_by(|left, right| {
        let left_key: &[u8] = match left {
            StateTreeMutation::Insert { key, .. }
            | StateTreeMutation::Update { key, .. }
            | StateTreeMutation::Remove { key }
            | StateTreeMutation::RemoveRange { lower: key, .. } => key,
        };
        let right_key: &[u8] = match right {
            StateTreeMutation::Insert { key, .. }
            | StateTreeMutation::Update { key, .. }
            | StateTreeMutation::Remove { key }
            | StateTreeMutation::RemoveRange { lower: key, .. } => key,
        };
        left_key.cmp(right_key)
    });
    let mut state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        mutations,
        view.test_storage_read(),
    )
    .await
    .expect("duplicate-owner state edit");
    state_edit
        .stage_objects(pack_objects)
        .expect("stage duplicate-owner current-state packs");
    let transition = branch_transition_with_members(&view, state_edit, 0x70, members).await;
    let mut publication = PreparedPublication::from_branch_view(&view).expect("publication");
    publication
        .stage_blob_merkle_build_for_test(&valid_build)
        .expect("stage valid duplicate Merkle closure");
    publication
        .stage_blob_merkle_build_for_test(&wrong_build)
        .expect("stage wrong duplicate Merkle closure");
    publication
        .publish_state_transition(&view, transition)
        .await
        .expect("publish duplicate-owner state");
    drop(view);
    commit_publication_for_test(publication, &storage)
        .await
        .expect("commit duplicate-owner state");

    let reader = super::blob_reader_on_read(
        SharedStorageAdapterRead::new(StorageAdapterReadScope::new(
            storage
                .begin_read(ReadOptions::default())
                .await
                .expect("duplicate-owner read"),
        )),
        &uuid::Uuid::from_bytes(*seed.branch_id.as_bytes()).to_string(),
    )
    .expect("duplicate-owner reader");
    let selected_key = super::decode_state_key(&valid_key).expect("selected valid state key");
    let ranges = reader
        .load_ranges_for_state_keys(&[(selected_key.clone(), 0..1), (selected_key.clone(), 1..2)])
        .await
        .expect("selected duplicate-owner ranges")
        .into_vec();
    assert_eq!(ranges.len(), 2, "duplicate request slots must be preserved");
    assert_eq!(
        ranges[0].as_ref().expect("first range").bytes.as_ref(),
        b"a".as_slice()
    );
    assert_eq!(
        ranges[1].as_ref().expect("second range").bytes.as_ref(),
        b"a".as_slice()
    );
    assert_eq!(
        reader
            .load_bytes_for_state_keys(&[selected_key])
            .await
            .expect("selected duplicate-owner full read")
            .into_vec(),
        vec![Some(valid_payload.to_vec())]
    );
    let wrong_identity =
        super::decode_state_key(&wrong_identity_key).expect("wrong-identity state key");
    assert!(
        reader
            .load_bytes_for_state_keys(std::slice::from_ref(&wrong_identity))
            .await
            .is_err(),
        "a same-BlobId owner with a different declared id must fail before manifest load"
    );
    assert!(
        reader
            .load_ranges_for_state_keys(&[(wrong_identity, 0..1)])
            .await
            .is_err(),
        "a same-BlobId owner with a different declared id must fail before range selection"
    );
}

#[cfg(any())]
async fn publish_untracked_manifest(
    storage: &Memory,
    seed: &SeedData,
    primary_key: &str,
    build: &super::merkle::BlobMerkleTreeBuild,
) -> ObjectId {
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .expect("untracked view");
    let (manifest_id, _) = build.manifest.encode().expect("manifest");
    let row_pk = RowPk::single(primary_key);
    let roots = [manifest_id];
    let mut publication = PreparedPublication::from_global_epoch(&view).expect("untracked put");
    publication
        .stage_blob_merkle_build_for_test(build)
        .expect("Merkle closure");
    publication
        .put_untracked_row(
            seed.branch_id,
            StateKeyRef {
                schema_key: "app.untracked",
                file_id: None,
                row_pk: &row_pk,
            },
            UntrackedValueRef {
                created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
                cell: StateCellRef::Value("blob"),
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: &roots,
            },
        )
        .expect("untracked row");
    drop(view);
    commit_publication_for_test(publication, storage)
        .await
        .expect("untracked commit");
    manifest_id
}

#[cfg(any())]
async fn delete_untracked(storage: &Memory, seed: &SeedData, primary_key: &str) {
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .expect("delete view");
    let row_pk = RowPk::single(primary_key);
    let mut publication = PreparedPublication::from_global_epoch(&view).expect("delete");
    publication
        .delete_untracked_row(
            seed.branch_id,
            StateKeyRef {
                schema_key: "app.untracked",
                file_id: None,
                row_pk: &row_pk,
            },
        )
        .expect("delete untracked");
    drop(view);
    commit_publication_for_test(publication, storage)
        .await
        .expect("delete commit");
}

async fn seed_with_disposable_branch(storage: &Memory) -> (SeedData, CanonicalBranchId, ChangeId) {
    let mut seed = build_seed();
    let disposable = CanonicalBranchId::from_bytes(raw_id(0x12));
    let disposable_ref_id = ChangeId::from_bytes(raw_id(0x32));
    let disposable_ref = ChangeObjectV1::BranchRef {
        change_id: disposable_ref_id,
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(1),
        branch_id: disposable,
        before_semantic_head_commit_object_id: None,
        after_semantic_head_commit_object_id: Some(seed.commit_object_id),
        previous_ref_change_object_id: None,
        payload: b"create-disposable".to_vec(),
        json_payload_object_ids: Vec::new(),
    };
    let (disposable_ref_object_id, disposable_ref_bytes) =
        disposable_ref.encode().expect("disposable ref");
    seed.objects
        .insert(disposable_ref_object_id, disposable_ref_bytes)
        .expect("disposable ref object");
    let mut catalog_entries = seed_member_catalog_entries(&seed, seed.commit_object_id);
    catalog_entries.push((
        seed.ref_change_id,
        ChangeCatalogEntry {
            owner: ChangeCatalogOwner::BranchRef {
                ref_change_object_id: seed.ref_change_object_id,
                branch_id: seed.branch_id,
            },
        },
    ));
    catalog_entries.push((
        disposable_ref_id,
        ChangeCatalogEntry {
            owner: ChangeCatalogOwner::BranchRef {
                ref_change_object_id: disposable_ref_object_id,
                branch_id: disposable,
            },
        },
    ));
    catalog_entries.sort_by_key(|(id, _)| *id.as_bytes());
    let catalog = build_change_catalog(&catalog_entries).expect("disposable catalog");
    let change_catalog_root = catalog.root.object_id;
    seed.objects
        .extend(catalog.objects)
        .expect("catalog objects");
    let current_repository = RepositoryRootV1::decode(
        seed.repository_root_id,
        seed.objects
            .get(seed.repository_root_id)
            .expect("repository"),
    )
    .expect("repository");
    let repository = RepositoryRootV1 {
        change_catalog_root,
        ..current_repository
    };
    let (repository_id, repository_bytes) = repository.encode().expect("repository");
    seed.objects
        .insert(repository_id, repository_bytes)
        .expect("repository object");
    seed.repository_root_id = repository_id;
    seed.global_selector.repository_root = repository_id;
    seed.global_selector.selector_generation += 1;
    let snapshot = BranchSnapshotV1 {
        branch_id: disposable,
        local_state_root: seed.local_state_root,
        semantic_head_commit_object_id: seed.commit_object_id,
        latest_ref_change_object_id: Some(disposable_ref_object_id),
        historical_global_state_root: seed.global_state_root,
    };
    let (snapshot_id, snapshot_bytes) = snapshot.encode().expect("disposable snapshot");
    seed.objects
        .insert(snapshot_id, snapshot_bytes)
        .expect("snapshot object");
    seed_storage(storage, &seed).await;
    let selector = BranchSelectorV1 {
        branch_id: disposable,
        branch_snapshot_object_id: snapshot_id,
        selector_generation: 1,
    };
    let mut writes = StorageWriteSet::new();
    writes.put(
        SELECTOR_SPACE,
        branch_selector_key(disposable).to_vec(),
        selector.encode().expect("disposable selector").to_vec(),
    );
    commit_write_set_for_test(writes, storage).await;
    (seed, disposable, disposable_ref_id)
}

#[tokio::test]
async fn retained_checkpoint_outlives_branch_retirement_then_releases_blob() {
    let storage = Memory::new();
    let (seed, disposable, initial_ref_id) = seed_with_disposable_branch(&storage).await;
    let upload = make_upload();
    let view = open_coherent_view(&storage, disposable)
        .await
        .expect("upload view");
    let mut upload_publication = PreparedPublication::from_global_epoch(&view).expect("upload");
    stage_upload(&mut upload_publication, &upload);
    drop(view);
    commit_publication_for_test(upload_publication, &storage)
        .await
        .expect("publish upload");

    let view = open_coherent_view(&storage, disposable)
        .await
        .expect("completion view");
    let completion = prepare_upload_completion(
        &view,
        &upload.upload_id,
        UploadBindingRef {
            repository_identity: b"repository",
            path: b"/blob.bin",
            payload_domain: b"file",
            declared_total_size: 4,
            declared_final_hash: Some(*blake3::hash(b"data").as_bytes()),
        },
    )
    .await
    .expect("completion");
    let manifest = single_leaf_manifest_for_test(&upload.chunk)
        .expect("authenticated checkpoint manifest")
        .0;
    let (manifest_id, _) = manifest.encode().expect("manifest");
    let entry = test_state_member(
        "disposable-blob",
        StateCellRef::Value("blob"),
        0x80,
        &[manifest_id],
        false,
    );
    let (rows, members, _, pack_objects) = encode_test_state_entries(0x80, vec![entry]);
    let (key, value) = rows.into_iter().next().expect("state row");
    let mut state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![StateTreeMutation::insert_bound(
            key,
            value,
            StateMutationAudit {
                commit_id: raw_id(0x80),
                tombstone: false,
                blob_manifest_object_ids: vec![manifest_id],
            },
        )],
        view.test_storage_read(),
    )
    .await
    .expect("state edit");
    state_edit
        .stage_objects(pack_objects)
        .expect("stage retained current-state pack");
    let semantic_change_ids = members
        .iter()
        .map(CommitMemberV3::change_id)
        .collect::<Vec<_>>();
    let transition = branch_transition_with_members(&view, state_edit, 0x80, members).await;
    let mut complete = PreparedPublication::from_branch_view(&view).expect("complete");
    complete
        .publish_completed_upload(&view, completion, transition)
        .await
        .expect("handoff");
    drop(view);
    commit_publication_for_test(complete, &storage)
        .await
        .expect("complete upload");

    let checkpoint_id = SnapshotSelectorId::from_bytes(raw_id(0x90));
    let view = open_coherent_view(&storage, disposable)
        .await
        .expect("checkpoint view");
    let mut checkpoint = PreparedPublication::from_global_epoch(&view).expect("checkpoint");
    checkpoint
        .publish_current_snapshot_pin(
            &view,
            SnapshotRole::Checkpoint,
            checkpoint_id,
            SelectorExpectation::Absent,
        )
        .expect("checkpoint pin");
    drop(view);
    commit_publication_for_test(checkpoint, &storage)
        .await
        .expect("checkpoint commit");

    let view = open_coherent_view(&storage, disposable)
        .await
        .expect("retirement view");
    let commit_catalog_edit = retire_commit_catalog_entries(
        view.repository_root().commit_catalog_root,
        &[],
        view.test_storage_read(),
    )
    .await
    .expect("retire commit");
    let change_catalog_edit = retire_change_catalog_entries(
        view.repository_root().change_catalog_root,
        &[],
        view.test_storage_read(),
    )
    .await
    .expect("retire changes");
    let repository = RepositoryRootV1 {
        commit_catalog_root: commit_catalog_edit.root,
        change_catalog_root: change_catalog_edit.root,
        ..view.repository_root()
    };
    let mut retire = PreparedPublication::from_branch_view(&view).expect("retire branch");
    retire
        .publish_branch_retirement(&view, commit_catalog_edit, change_catalog_edit, repository)
        .expect("branch retirement");
    drop(view);
    commit_publication_for_test(retire, &storage)
        .await
        .expect("retire commit");
    let retained_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("retained catalog view");
    assert!(
        load_commit(&retained_view, CommitId::from_bytes(raw_id(0x20)))
            .await
            .expect("seed commit catalog lookup")
            .is_some()
    );
    assert!(
        load_commit(&retained_view, CommitId::from_bytes(raw_id(0x80)))
            .await
            .expect("checkpoint commit catalog lookup")
            .is_some()
    );
    drop(retained_view);
    sweep(&storage, seed.branch_id).await;
    assert!(object_present(&storage, manifest_id).await);
    assert!(object_present(&storage, upload.chunk_id).await);

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("release view");
    let key = snapshot_selector_key(SnapshotRole::Checkpoint, checkpoint_id);
    let keys = [Key(key)];
    let loaded = view
        .test_storage_read()
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("checkpoint selector read");
    let raw = match loaded.values.as_slice() {
        [Some(crate::storage::ProjectedValue::FullValue(bytes))] => bytes.clone(),
        other => panic!("expected checkpoint selector, got {other:?}"),
    };
    let selector = SnapshotSelectorV1::decode(&raw).expect("checkpoint selector");
    let commit_catalog_edit = retire_commit_catalog_entries(
        view.repository_root().commit_catalog_root,
        &[CommitId::from_bytes(raw_id(0x80))],
        view.test_storage_read(),
    )
    .await
    .expect("final commit retirement");
    let mut retired_change_ids = semantic_change_ids;
    retired_change_ids.extend([initial_ref_id, ChangeId::from_bytes(raw_id(0x81))]);
    retired_change_ids.sort_by_key(|id| *id.as_bytes());
    let change_catalog_edit = retire_change_catalog_entries(
        view.repository_root().change_catalog_root,
        &retired_change_ids,
        view.test_storage_read(),
    )
    .await
    .expect("final change retirement");
    let repository = RepositoryRootV1 {
        commit_catalog_root: commit_catalog_edit.root,
        change_catalog_root: change_catalog_edit.root,
        ..view.repository_root()
    };
    let mut release = PreparedPublication::from_global_epoch(&view).expect("release");
    release
        .release_snapshot_pin_with_catalog_retirement(
            &view,
            selector,
            raw,
            commit_catalog_edit,
            change_catalog_edit,
            repository,
        )
        .expect("release checkpoint");
    drop(view);
    commit_publication_for_test(release, &storage)
        .await
        .expect("release commit");
    sweep(&storage, seed.branch_id).await;
    assert!(!object_present(&storage, manifest_id).await);
    assert!(!object_present(&storage, upload.chunk_id).await);
}

#[cfg(any())]
#[tokio::test]
async fn untracked_and_real_shared_chunk_roots_release_only_at_final_reference() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let shared = BlobChunkV1 {
        bytes: Bytes::from(vec![b's'; BLOB_MERKLE_CHUNK_BYTES as usize]),
    };
    let first_unique = BlobChunkV1 {
        bytes: Bytes::from(vec![b'o'; BLOB_MERKLE_CHUNK_BYTES as usize]),
    };
    let second_unique = BlobChunkV1 {
        bytes: Bytes::from(vec![b't'; BLOB_MERKLE_CHUNK_BYTES as usize]),
    };
    let (shared_id, _) = shared.encode().expect("shared chunk");
    let (first_unique_id, _) = first_unique.encode().expect("first unique");
    let (second_unique_id, _) = second_unique.encode().expect("second unique");
    let first = build_blob_merkle_tree(&[first_unique.clone(), shared.clone()])
        .expect("first shared Merkle manifest");
    let second = build_blob_merkle_tree(&[second_unique.clone(), shared.clone()])
        .expect("second shared Merkle manifest");
    let first_id = publish_untracked_manifest(&storage, &seed, "one", &first).await;
    let second_id = publish_untracked_manifest(&storage, &seed, "two", &second).await;
    assert_ne!(first_id, second_id);
    sweep(&storage, seed.branch_id).await;
    assert!(object_present(&storage, shared_id).await);
    delete_untracked(&storage, &seed, "one").await;
    sweep(&storage, seed.branch_id).await;
    assert!(!object_present(&storage, first_id).await);
    assert!(!object_present(&storage, first_unique_id).await);
    assert!(object_present(&storage, second_id).await);
    assert!(object_present(&storage, shared_id).await);
    delete_untracked(&storage, &seed, "two").await;
    sweep(&storage, seed.branch_id).await;
    assert!(!object_present(&storage, second_id).await);
    assert!(!object_present(&storage, second_unique_id).await);
    assert!(!object_present(&storage, shared_id).await);
}

#[tokio::test]
async fn root_only_publication_and_gc_are_epoch_fenced_and_all_roles_are_roots() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let target = SnapshotTargetV1 {
        role: SnapshotRole::Checkpoint,
        selector_id: SnapshotSelectorId::from_bytes(raw_id(1)),
        branch_id: seed.branch_id,
        branch_snapshot_object_id: seed.branch_snapshot_id,
        semantic_commit_object_id: seed.commit_object_id,
    };
    let (target_id, _) = target.encode().expect("target");
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("orphan view");
    let mut orphan_stage = PreparedPublication::from_global_epoch(&view).expect("orphan stage");
    orphan_stage
        .stage_snapshot_target(target)
        .expect("orphan target");
    drop(view);
    commit_publication_for_test(orphan_stage, &storage)
        .await
        .expect("stage orphan target");
    let publish_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("publish view");
    let mut root_only = PreparedPublication::from_global_epoch(&publish_view).expect("root move");
    assert_eq!(
        root_only
            .publish_current_snapshot_pin(
                &publish_view,
                target.role,
                target.selector_id,
                SelectorExpectation::Absent,
            )
            .expect("checkpoint selector"),
        target_id
    );
    drop(publish_view);
    commit_publication_for_test(root_only, &storage)
        .await
        .expect("root first");
    sweep(&storage, seed.branch_id).await;

    let inverse = Memory::new();
    seed_storage(&inverse, &seed).await;
    let view = open_coherent_view(&inverse, seed.branch_id)
        .await
        .expect("orphan view");
    let mut orphan_stage = PreparedPublication::from_global_epoch(&view).expect("orphan stage");
    orphan_stage
        .stage_snapshot_target(target)
        .expect("orphan target");
    drop(view);
    commit_publication_for_test(orphan_stage, &inverse)
        .await
        .expect("stage orphan target");
    let publish_view = open_coherent_view(&inverse, seed.branch_id)
        .await
        .expect("publish view");
    let mut stale_root = PreparedPublication::from_global_epoch(&publish_view).expect("root move");
    stale_root
        .publish_current_snapshot_pin(
            &publish_view,
            target.role,
            target.selector_id,
            SelectorExpectation::Absent,
        )
        .expect("root selector");
    drop(publish_view);
    sweep(&inverse, seed.branch_id).await;
    assert!(matches!(
        commit_publication_for_test(stale_root, &inverse).await,
        Err(StorageError::PreconditionFailed(_))
    ));
    let retry_view = open_coherent_view(&inverse, seed.branch_id)
        .await
        .expect("retry view");
    let mut retry = PreparedPublication::from_global_epoch(&retry_view).expect("retry");
    retry
        .publish_current_snapshot_pin(
            &retry_view,
            target.role,
            target.selector_id,
            SelectorExpectation::Absent,
        )
        .expect("retry selector");
    drop(retry_view);
    commit_publication_for_test(retry, &inverse)
        .await
        .expect("retry root publication");
    assert!(object_present(&inverse, target_id).await);

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("role view");
    let mut roles = PreparedPublication::from_global_epoch(&view).expect("roles");
    for (index, role) in [SnapshotRole::Recovery].into_iter().enumerate() {
        let selector_id = SnapshotSelectorId::from_bytes(raw_id(index as u8 + 2));
        roles
            .publish_current_snapshot_pin(&view, role, selector_id, SelectorExpectation::Absent)
            .expect("role selector");
    }
    drop(view);
    commit_publication_for_test(roles, &storage)
        .await
        .expect("supported roles");
    sweep(&storage, seed.branch_id).await;
    assert!(object_present(&storage, target_id).await);

    for retired_role in [3_u8, 4, 5] {
        assert!(
            SnapshotRole::decode(retired_role).is_err(),
            "retired snapshot role {retired_role} must fail closed"
        );
    }
}

#[tokio::test]
async fn full_selector_scan_crosses_storage_page_and_corruption_fails_closed() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("view");
    let mut publication = PreparedPublication::from_global_epoch(&view).expect("selectors");
    let mut last_target = ObjectId::ZERO;
    for index in 0_u16..1030 {
        let mut raw = [0_u8; 16];
        raw[..2].copy_from_slice(&index.to_be_bytes());
        let selector_id = SnapshotSelectorId::from_bytes(raw);
        last_target = publication
            .publish_current_snapshot_pin(
                &view,
                SnapshotRole::Checkpoint,
                selector_id,
                SelectorExpectation::Absent,
            )
            .expect("selector");
    }
    drop(view);
    commit_publication_for_test(publication, &storage)
        .await
        .expect("selector pages");
    sweep(&storage, seed.branch_id).await;
    assert!(object_present(&storage, last_target).await);

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("corrupt upload view");
    let upload = make_upload();
    let wrong_progress = UploadProgressV1 {
        completed_part_count: 2,
        ..upload.progress.clone()
    };
    let mut corrupt = PreparedPublication::from_global_epoch(&view).expect("corrupt receipt");
    corrupt.stage_blob_chunk(&upload.chunk).expect("chunk");
    corrupt.stage_upload_part(&upload.part).expect("part");
    corrupt
        .stage_receipt_tree_edit(upload.receipt.clone())
        .expect("receipt");
    let wrong_progress_id = corrupt
        .stage_upload_progress(&wrong_progress)
        .expect("wrong progress");
    corrupt
        .put_upload_selector(
            &UploadSelectorV1 {
                progress_object_id: wrong_progress_id,
                ..upload.selector.clone()
            },
            SelectorExpectation::Absent,
        )
        .expect("wrong selector");
    drop(view);
    commit_publication_for_test(corrupt, &storage)
        .await
        .expect("publish authenticated corruption");
    assert!(sweep_result(&storage).await.is_err());
}

#[test]
fn selector_codecs_have_single_edges_and_canonical_keys() {
    let seed = build_seed();
    let raw_branch = seed.branch_selector.encode().expect("branch");
    assert_eq!(
        BranchSelectorV1::decode(&raw_branch).expect("decode"),
        seed.branch_selector
    );
    assert!(
        !raw_branch
            .windows(16)
            .any(|window| window == seed.ref_change_id.as_bytes())
    );
    assert_eq!(branch_selector_key(seed.branch_id).len(), 23);
    let checkpoint = SnapshotSelectorV1 {
        role: SnapshotRole::Checkpoint,
        selector_id: SnapshotSelectorId::from_bytes(raw_id(7)),
        target_object_id: content_id(7),
        selector_generation: 1,
    };
    assert_eq!(
        SnapshotSelectorV1::decode(&checkpoint.encode().expect("checkpoint"))
            .expect("decode checkpoint"),
        checkpoint
    );
    assert!(
        snapshot_selector_key(checkpoint.role, checkpoint.selector_id).starts_with(b"checkpoint/")
    );
    assert_eq!(
        gc_progress_selector_key(),
        Bytes::from_static(b"gc-progress")
    );
}

#[test]
fn object_and_catalog_encodings_are_canonical() {
    assert_eq!(content_id(0xab).to_string(), "ab".repeat(32));
    assert_eq!(*CommitId::from_bytes(raw_id(1)).as_bytes(), raw_id(1));
    assert_eq!(*ChangeId::from_bytes(raw_id(2)).as_bytes(), raw_id(2));
    assert_eq!(
        *CanonicalBranchId::from_bytes(raw_id(3)).as_bytes(),
        raw_id(3)
    );
    assert_eq!(
        *SnapshotSelectorId::from_bytes(raw_id(4)).as_bytes(),
        raw_id(4)
    );
    assert_eq!(*content_id(5).as_bytes(), [5; 32]);
    assert_eq!(
        CommitCatalogEntry {
            commit_object_id: content_id(1),
        }
        .encode()
        .expect("commit entry")
        .len(),
        32
    );
    assert_eq!(
        ChangeCatalogEntry {
            owner: ChangeCatalogOwner::CommitMember {
                commit_object_id: content_id(3),
                ordinal: 7,
            },
        }
        .encode()
        .expect("change entry")
        .len(),
        37
    );
}

#[test]
fn seed_provenance_and_ref_edge_are_not_aliased() {
    let seed = build_seed();
    assert_ne!(seed.commit_object_id, seed.semantic_change_object_id);
    assert_ne!(seed.ref_change_object_id, seed.semantic_change_object_id);
    assert_ne!(seed.repository_root_id, seed.branch_snapshot_id);
    let repository = RepositoryRootV1::decode(
        seed.repository_root_id,
        seed.objects
            .get(seed.repository_root_id)
            .expect("repository"),
    )
    .expect("repository decode");
    let branch = BranchSnapshotV1::decode(
        seed.branch_snapshot_id,
        seed.objects.get(seed.branch_snapshot_id).expect("branch"),
    )
    .expect("branch decode");
    assert_eq!(seed.global_state_root, repository.global_state_root);
    assert_eq!(seed.local_state_root, branch.local_state_root);
    assert_eq!(
        seed.objects.get(seed.orphan_object_id).expect("orphan"),
        &seed.orphan_object_bytes
    );
    let snapshot = BranchSnapshotV1::decode(
        seed.branch_snapshot_id,
        seed.objects.get(seed.branch_snapshot_id).expect("snapshot"),
    )
    .expect("snapshot decode");
    validate_branch_snapshot_ref_edge(&snapshot, load_from(&seed.objects)).expect("ref edge");
}

fn zero_edge_page_member(index: usize) -> CommitMemberV3 {
    page_member_with_key_bytes(index, 1)
}

fn page_member_with_key_bytes(index: usize, key_bytes: usize) -> CommitMemberV3 {
    let mut change_raw = [0_u8; 16];
    change_raw[..8].copy_from_slice(&(index as u64 + 1).to_be_bytes());
    change_raw[15] = 1;
    CommitMemberV3::introduced(
        ChangeId::from_bytes(change_raw),
        vec![b'x'; key_bytes],
        [1; 32],
        false,
        [2; 32],
        [3; 32],
        false,
        "forktree-test".to_owned(),
        LixTimestamp::from_unix_millis_utc_lossy(1),
        LixTimestamp::from_unix_millis_utc_lossy(1),
        None,
    )
}

#[test]
fn commit_member_pages_cover_boundaries_and_fail_closed_corruption() {
    fn page_member(index: usize) -> CommitMemberV3 {
        page_member_with_key_bytes(index, 1)
    }

    for count in [255usize, 256, 257, 1002] {
        let members = (0..count).map(page_member).collect::<Vec<_>>();
        let mut commit = CommitObjectV1 {
            commit_id: CommitId::from_bytes(raw_id(0xa1)),
            generation: 1,
            parent_commit_object_ids: Vec::new(),
            members,
            member_page_object_ids: Vec::new(),
            global_state_root: content_id(0x71),
            local_state_root: content_id(0x72),
            checkpoint_cursor: CheckpointCursorV1::root(),
            metadata: b"page-boundary".to_vec(),
        };
        let pages = commit.prepare_member_pages().expect("page closure");
        assert_eq!(
            pages.len(),
            match count {
                255 | 256 => 1,
                257 => 2,
                1002 => 4,
                _ => unreachable!(),
            }
        );
        let (commit_object_id, commit_bytes) = commit.encode().expect("paged commit");
        let decoded =
            CommitObjectV1::decode(commit_object_id, &commit_bytes).expect("paged commit decodes");
        let page_map = pages
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        let loaded = decoded
            .load_members_with(|id| {
                page_map
                    .get(&id)
                    .cloned()
                    .ok_or_else(|| StorageError::Io("missing member page".to_owned()))
            })
            .expect("member closure loads");
        assert_eq!(loaded, commit.members);
        assert_eq!(loaded.len(), count);
    }

    for count in [256usize, 257, 100_000] {
        let members = (0..count)
            .map(|index| {
                if count == 100_000 {
                    page_member_with_key_bytes(index, 256)
                } else {
                    zero_edge_page_member(index)
                }
            })
            .collect::<Vec<_>>();
        let pages = CommitChangePageV3::encode_pages(CommitId::from_bytes(raw_id(0xa5)), &members)
            .expect("zero-edge page closure");
        assert!(
            pages.objects.len() + 2 <= 256,
            "byte-bounded inline pages must fit the commit edge vector"
        );
        assert_eq!(pages.member_locations.len(), count);
        let mut next_ordinal = 0_u32;
        for (id, bytes) in pages.objects {
            let page = CommitChangePageV3::decode(id, &bytes).expect("zero-edge page decodes");
            assert_eq!(page.start_ordinal, next_ordinal);
            next_ordinal += u32::try_from(page.members.len()).expect("bounded test page");
        }
        assert_eq!(
            usize::try_from(next_ordinal).expect("test member count"),
            count
        );
    }

    let (zero_edge_page_id, zero_edge_page_bytes) = CommitChangePageV3 {
        commit_id: CommitId::from_bytes(raw_id(0xa6)),
        start_ordinal: 0,
        members: (0..257).map(zero_edge_page_member).collect(),
    }
    .encode()
    .expect("inline members may exceed the independent object-edge budget");
    assert_eq!(
        CommitChangePageV3::decode(zero_edge_page_id, &zero_edge_page_bytes)
            .expect("large inline page decodes")
            .members
            .len(),
        257
    );

    let members = (0..255).map(page_member).collect::<Vec<_>>();
    let mut commit = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(0xa2)),
        generation: 1,
        parent_commit_object_ids: Vec::new(),
        members,
        member_page_object_ids: Vec::new(),
        global_state_root: content_id(0x73),
        local_state_root: content_id(0x74),
        checkpoint_cursor: CheckpointCursorV1::root(),
        metadata: b"page-corruption".to_vec(),
    };
    let pages = commit.prepare_member_pages().expect("corruption pages");
    let root = commit.member_page_object_ids[0];
    let page_map = pages
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut missing = page_map.clone();
    missing.remove(&root);
    let (commit_object_id, commit_bytes) = commit.encode().expect("commit");
    let decoded = CommitObjectV1::decode(commit_object_id, &commit_bytes).expect("decode commit");
    assert!(
        decoded
            .load_members_with(|id| {
                missing
                    .get(&id)
                    .cloned()
                    .ok_or_else(|| StorageError::Io("missing member page".to_owned()))
            })
            .is_err()
    );

    let mut corrupted = page_map.clone();
    let first = corrupted.remove(&root).expect("root page");
    let mut first_bytes = first.to_vec();
    first_bytes[0] ^= 0x01;
    corrupted.insert(root, Bytes::from(first_bytes));
    assert!(
        decoded
            .load_members_with(|id| {
                corrupted
                    .get(&id)
                    .cloned()
                    .ok_or_else(|| StorageError::Io("missing member page".to_owned()))
            })
            .is_err()
    );

    let mut wrong_ordinal =
        CommitChangePageV3::decode(root, page_map.get(&root).expect("root page"))
            .expect("valid root page");
    wrong_ordinal.start_ordinal = 1;
    let (wrong_id, wrong_bytes) = wrong_ordinal.encode().expect("wrong ordinal page");
    let mut wrong_map = page_map.clone();
    wrong_map.remove(&root);
    wrong_map.insert(wrong_id, wrong_bytes);
    let mut wrong_commit = decoded.clone();
    wrong_commit.member_page_object_ids[0] = wrong_id;
    assert!(
        wrong_commit
            .load_members_with(|id| {
                wrong_map
                    .get(&id)
                    .cloned()
                    .ok_or_else(|| StorageError::Io("missing member page".to_owned()))
            })
            .is_err()
    );

    let duplicate = CommitChangePageV3 {
        commit_id: CommitId::from_bytes(raw_id(0xa3)),
        start_ordinal: 0,
        members: vec![page_member(0x81), page_member(0x81)],
    };
    assert!(duplicate.encode().is_err());
    let zero_page_edge = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(0xa4)),
        generation: 1,
        parent_commit_object_ids: Vec::new(),
        members: Vec::new(),
        member_page_object_ids: vec![ObjectId::ZERO],
        global_state_root: content_id(0x75),
        local_state_root: content_id(0x76),
        checkpoint_cursor: CheckpointCursorV1::root(),
        metadata: b"zero-page-edge".to_vec(),
    };
    assert!(zero_page_edge.encode().is_err());
}
#[derive(Clone)]
struct ThreePageStaleFixture {
    seed: SeedData,
    after_commit_id: CommitId,
    after_commit_object_id: ObjectId,
    members: Vec<CommitMemberV3>,
    selected_key: Vec<u8>,
    selected_change_id: ChangeId,
    page_ids: Vec<ObjectId>,
}

fn build_three_page_stale_fixture() -> ThreePageStaleFixture {
    let mut seed = build_seed();
    let entries = (0..513)
        .map(|index| {
            test_state_member(
                &format!("stale-page-{index:03}"),
                StateCellRef::Value("after"),
                0xc1,
                &[],
                false,
            )
        })
        .collect::<Vec<_>>();
    let (rows, members, page_objects, pack_objects) = encode_test_state_entries(0xc1, entries);
    let page_ids = page_objects.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    assert_eq!(
        page_ids.len(),
        3,
        "the actual stale fixture needs three pages"
    );
    for (id, bytes) in page_objects {
        seed.objects.insert(id, bytes).expect("stale page object");
    }
    for (id, bytes) in pack_objects {
        seed.objects
            .insert(id, bytes)
            .expect("stale current-state pack");
    }
    let selected_row = rows[512].clone();
    let selected_key = selected_row.0.clone();
    let selected_change_id = members[512].change_id();
    let mut local_rows = scan_all(seed.local_state_root, "state", &load_from(&seed.objects))
        .expect("seed local state rows");
    local_rows.push(selected_row);
    local_rows.sort_by(|left, right| left.0.cmp(&right.0));
    let state_tree = build_state_tree(&local_rows).expect("selected state tree");
    let local_state_root = state_tree.root.object_id;
    seed.objects
        .extend(state_tree.objects)
        .expect("selected state objects");

    let after_commit_id = CommitId::from_bytes(raw_id(0xc1));
    let parent_commit = CommitObjectV1::decode(
        seed.commit_object_id,
        seed.objects
            .get(seed.commit_object_id)
            .expect("three-page parent commit"),
    )
    .expect("decode three-page parent commit");
    let after_commit = CommitObjectV1 {
        commit_id: after_commit_id,
        generation: 2,
        parent_commit_object_ids: vec![seed.commit_object_id],
        members: members.clone(),
        member_page_object_ids: page_ids.clone(),
        global_state_root: seed.global_state_root,
        local_state_root,
        checkpoint_cursor: CheckpointCursorV1::after_first_parent(
            seed.commit_object_id,
            &parent_commit,
            seed.branch_id,
            false,
        )
        .expect("three-page checkpoint cursor"),
        metadata: b"three-page-stale".to_vec(),
    };
    let (after_commit_object_id, after_commit_bytes) = after_commit.encode().expect("after commit");
    seed.objects
        .insert(after_commit_object_id, after_commit_bytes)
        .expect("after commit object");
    let ref_change = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0xc2)),
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
        branch_id: seed.branch_id,
        before_semantic_head_commit_object_id: Some(seed.commit_object_id),
        after_semantic_head_commit_object_id: Some(after_commit_object_id),
        previous_ref_change_object_id: Some(seed.ref_change_object_id),
        payload: b"three-page-stale-ref".to_vec(),
        json_payload_object_ids: Vec::new(),
    };
    let (ref_object_id, ref_bytes) = ref_change.encode().expect("after ref change");
    seed.objects
        .insert(ref_object_id, ref_bytes)
        .expect("after ref object");
    let commit_entries = vec![
        (
            seed.commit_id,
            CommitCatalogEntry {
                commit_object_id: seed.commit_object_id,
            },
        ),
        (
            after_commit_id,
            CommitCatalogEntry {
                commit_object_id: after_commit_object_id,
            },
        ),
    ];
    let mut change_entries = seed_member_catalog_entries(&seed, seed.commit_object_id);
    change_entries.extend(members.iter().enumerate().map(|(ordinal, member)| {
        (
            member.change_id(),
            ChangeCatalogEntry {
                owner: ChangeCatalogOwner::CommitMember {
                    commit_object_id: after_commit_object_id,
                    ordinal: u32::try_from(ordinal).expect("after member ordinal"),
                },
            },
        )
    }));
    change_entries.push((
        ref_change.change_id(),
        ChangeCatalogEntry {
            owner: ChangeCatalogOwner::BranchRef {
                ref_change_object_id: ref_object_id,
                branch_id: seed.branch_id,
            },
        },
    ));
    replace_selected_history_graph(
        &mut seed,
        &commit_entries,
        &change_entries,
        after_commit_object_id,
        ref_object_id,
    );
    ThreePageStaleFixture {
        seed,
        after_commit_id,
        after_commit_object_id,
        members,
        selected_key,
        selected_change_id,
        page_ids,
    }
}

fn rewrite_three_page_fixture(
    fixture: &ThreePageStaleFixture,
    page_ids: Vec<ObjectId>,
    extra_objects: &[(ObjectId, Bytes)],
    selected_owner: Option<ChangeCatalogEntry>,
    ref_byte: u8,
) -> SeedData {
    rewrite_three_page_fixture_with_raw_commit(
        fixture,
        page_ids,
        extra_objects,
        selected_owner,
        ref_byte,
        None,
    )
}

fn rewrite_three_page_fixture_with_raw_commit(
    fixture: &ThreePageStaleFixture,
    page_ids: Vec<ObjectId>,
    extra_objects: &[(ObjectId, Bytes)],
    selected_owner: Option<ChangeCatalogEntry>,
    ref_byte: u8,
    raw_commit: Option<(ObjectId, Bytes)>,
) -> SeedData {
    let mut seed = fixture.seed.clone();
    for (id, bytes) in extra_objects {
        seed.objects
            .insert(*id, bytes.clone())
            .expect("replacement page object");
    }
    let original = CommitObjectV1::decode(
        fixture.after_commit_object_id,
        seed.objects
            .get(fixture.after_commit_object_id)
            .expect("original after commit"),
    )
    .expect("decode original after commit");
    let (rewritten_object_id, rewritten_bytes) = raw_commit.unwrap_or_else(|| {
        let rewritten = CommitObjectV1 {
            commit_id: original.commit_id,
            generation: original.generation,
            parent_commit_object_ids: original.parent_commit_object_ids,
            members: Vec::new(),
            member_page_object_ids: page_ids,
            global_state_root: original.global_state_root,
            local_state_root: original.local_state_root,
            checkpoint_cursor: original.checkpoint_cursor,
            metadata: original.metadata,
        };
        rewritten.encode().expect("rewrite commit")
    });
    seed.objects
        .insert(rewritten_object_id, rewritten_bytes)
        .expect("rewritten commit object");
    let ref_change = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(ref_byte)),
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(i64::from(ref_byte)),
        branch_id: seed.branch_id,
        before_semantic_head_commit_object_id: Some(seed.commit_object_id),
        after_semantic_head_commit_object_id: Some(rewritten_object_id),
        previous_ref_change_object_id: Some(seed.ref_change_object_id),
        payload: b"rewritten-stale-ref".to_vec(),
        json_payload_object_ids: Vec::new(),
    };
    let (ref_object_id, ref_bytes) = ref_change.encode().expect("rewritten ref");
    seed.objects
        .insert(ref_object_id, ref_bytes)
        .expect("rewritten ref object");
    let commit_entries = vec![
        (
            seed.commit_id,
            CommitCatalogEntry {
                commit_object_id: seed.commit_object_id,
            },
        ),
        (
            fixture.after_commit_id,
            CommitCatalogEntry {
                commit_object_id: rewritten_object_id,
            },
        ),
    ];
    let mut change_entries = seed_member_catalog_entries(&seed, seed.commit_object_id);
    change_entries.extend(fixture.members.iter().enumerate().map(|(ordinal, member)| {
        let owner = if member.change_id() == fixture.selected_change_id {
            selected_owner
                .clone()
                .unwrap_or_else(|| ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id: rewritten_object_id,
                        ordinal: u32::try_from(ordinal).expect("rewritten member ordinal"),
                    },
                })
        } else {
            ChangeCatalogEntry {
                owner: ChangeCatalogOwner::CommitMember {
                    commit_object_id: rewritten_object_id,
                    ordinal: u32::try_from(ordinal).expect("rewritten member ordinal"),
                },
            }
        };
        (member.change_id(), owner)
    }));
    change_entries.push((
        ref_change.change_id(),
        ChangeCatalogEntry {
            owner: ChangeCatalogOwner::BranchRef {
                ref_change_object_id: ref_object_id,
                branch_id: seed.branch_id,
            },
        },
    ));
    replace_selected_history_graph(
        &mut seed,
        &commit_entries,
        &change_entries,
        rewritten_object_id,
        ref_object_id,
    );
    seed
}

async fn run_stale_reconciliation_fixture(
    seed: &SeedData,
    after_commit_id: CommitId,
) -> Result<super::view::StaleStateChanges, crate::LixError> {
    let storage = Memory::new();
    seed_storage(&storage, seed).await;
    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("stale reconciliation read"),
    );
    let facade = ForkTreeReadFacade::new(read);
    facade
        .stale_state_changes_between_commits(
            public_commit_id(0x20),
            public_commit_id(after_commit_id.as_bytes()[0]),
        )
        .await
}

fn encode_duplicate_page_commit(
    fixture: &ThreePageStaleFixture,
    page_ids: &[ObjectId],
) -> (ObjectId, Bytes) {
    let original = CommitObjectV1::decode(
        fixture.after_commit_object_id,
        fixture
            .seed
            .objects
            .get(fixture.after_commit_object_id)
            .expect("three-page commit"),
    )
    .expect("decode three-page commit");
    super::object::encode_object(super::object::ObjectDomain::CommitV2, |encoder| {
        encoder.fixed(fixture.after_commit_id.as_bytes());
        encoder.u64(2);
        encoder.u32(1);
        super::object::encode_id(encoder, fixture.seed.commit_object_id);
        encoder.u32(u32::try_from(page_ids.len()).expect("duplicate page vector length"));
        for page_id in page_ids {
            super::object::encode_id(encoder, *page_id);
        }
        super::object::encode_id(encoder, fixture.seed.global_state_root);
        super::object::encode_id(encoder, fixture.seed.local_state_root);
        match original.checkpoint_cursor {
            CheckpointCursorV1::Root => encoder.u8(0),
            CheckpointCursorV1::Ordinary {
                owner_branch_id,
                root_commit_object_id,
                distance_to_root,
                latest_checkpoint_object_id,
                distance_to_latest,
            } => {
                encoder.u8(1);
                encoder.fixed(owner_branch_id.as_bytes());
                super::object::encode_id(encoder, root_commit_object_id);
                encoder.u32(distance_to_root);
                super::object::encode_id(encoder, latest_checkpoint_object_id);
                encoder.u32(distance_to_latest);
            }
            CheckpointCursorV1::Checkpoint {
                owner_branch_id,
                root_commit_object_id,
                distance_to_root,
                previous_checkpoint_object_id,
                distance_to_previous,
            } => {
                encoder.u8(2);
                encoder.fixed(owner_branch_id.as_bytes());
                super::object::encode_id(encoder, root_commit_object_id);
                encoder.u32(distance_to_root);
                super::object::encode_id(encoder, previous_checkpoint_object_id);
                encoder.u32(distance_to_previous);
            }
        }
        encoder.bytes(b"malformed-duplicate-page-vector")
    })
    .expect("malformed duplicate-page commit encoding")
}

#[tokio::test]
async fn stale_reconciliation_authenticates_three_page_prefix_after_reopen() {
    let fixture = build_three_page_stale_fixture();
    let result = run_stale_reconciliation_fixture(&fixture.seed, fixture.after_commit_id)
        .await
        .expect("unequal-endpoint stale reconciliation should resolve the selected later page");
    assert_eq!(result.identities.len(), 1);
    assert_eq!(
        result.identities[0].key,
        super::decode_state_key(&fixture.selected_key).expect("selected key decodes")
    );
    assert_eq!(
        result.identities[0]
            .after
            .as_ref()
            .expect("selected identity has an after row")
            .change_id,
        crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(
            *fixture.selected_change_id.as_bytes(),
        ))
    );

    let storage = CrashStorage::new();
    seed_storage(&storage, &fixture.seed).await;
    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("pre-reopen stale read"),
    );
    let facade = ForkTreeReadFacade::new(read);
    facade
        .stale_state_changes_between_commits(
            public_commit_id(0x20),
            public_commit_id(fixture.after_commit_id.as_bytes()[0]),
        )
        .await
        .expect("pre-reopen stale reconciliation");
    let reopened = storage.reopen();
    let read = StorageAdapterReadScope::new(
        reopened
            .begin_read(ReadOptions::default())
            .await
            .expect("cold-reopen stale read"),
    );
    let facade = ForkTreeReadFacade::new(read);
    let reopened_result = facade
        .stale_state_changes_between_commits(
            public_commit_id(0x20),
            public_commit_id(fixture.after_commit_id.as_bytes()[0]),
        )
        .await
        .expect("cold reopen must preserve actual stale reconciliation");
    assert_eq!(reopened_result.identities.len(), 1);

    // The remaining assertions deliberately exercise the public stale
    // reconciliation caller with unequal endpoints.  They must not be
    // reduced to direct calls to validate_stale_page_position: the caller
    // authenticates the selected CommitCatalog/ChangeCatalog owner and the
    // endpoint roots before resolving the selected page prefix.
    let second_page = CommitChangePageV3::decode(
        fixture.page_ids[1],
        fixture
            .seed
            .objects
            .get(fixture.page_ids[1])
            .expect("fixture second page"),
    )
    .expect("fixture second page decodes");

    let mut gap_page = second_page.clone();
    gap_page.start_ordinal = gap_page.start_ordinal.checked_add(1).expect("gap ordinal");
    let (gap_id, gap_bytes) = gap_page.encode().expect("gap page encodes");
    let mut gap_page_ids = fixture.page_ids.clone();
    gap_page_ids[1] = gap_id;
    let gap_seed =
        rewrite_three_page_fixture(&fixture, gap_page_ids, &[(gap_id, gap_bytes)], None, 0xd1);
    assert!(
        run_stale_reconciliation_fixture(&gap_seed, fixture.after_commit_id)
            .await
            .is_err(),
        "the actual stale caller rejects an earlier hidden page-prefix gap"
    );

    let mut wrong_commit_page = second_page.clone();
    wrong_commit_page.commit_id = CommitId::from_bytes(raw_id(0xd2));
    let (wrong_id, wrong_bytes) = wrong_commit_page
        .encode()
        .expect("wrong-commit page encodes");
    let mut wrong_page_ids = fixture.page_ids.clone();
    wrong_page_ids[1] = wrong_id;
    let wrong_seed = rewrite_three_page_fixture(
        &fixture,
        wrong_page_ids,
        &[(wrong_id, wrong_bytes)],
        None,
        0xd3,
    );
    assert!(
        run_stale_reconciliation_fixture(&wrong_seed, fixture.after_commit_id)
            .await
            .is_err(),
        "the actual stale caller rejects a wrong-commit hidden prefix page"
    );

    let mut missing_seed =
        rewrite_three_page_fixture(&fixture, fixture.page_ids.clone(), &[], None, 0xd4);
    missing_seed.objects.remove(fixture.page_ids[1]);
    assert!(
        run_stale_reconciliation_fixture(&missing_seed, fixture.after_commit_id)
            .await
            .is_err(),
        "the actual stale caller rejects a missing selected-prefix page"
    );

    let mut malformed_seed =
        rewrite_three_page_fixture(&fixture, fixture.page_ids.clone(), &[], None, 0xd7);
    malformed_seed.objects.remove(fixture.page_ids[1]);
    malformed_seed
        .objects
        .insert(fixture.page_ids[1], Bytes::from_static(b"malformed-page"))
        .expect("malformed page replacement");
    assert!(
        run_stale_reconciliation_fixture(&malformed_seed, fixture.after_commit_id)
            .await
            .is_err(),
        "the actual stale caller rejects a malformed selected-prefix page"
    );

    let duplicate_page_ids = vec![
        fixture.page_ids[0],
        fixture.page_ids[0],
        fixture.page_ids[2],
    ];
    let duplicate_commit = encode_duplicate_page_commit(&fixture, &duplicate_page_ids);
    let duplicate_seed = rewrite_three_page_fixture_with_raw_commit(
        &fixture,
        duplicate_page_ids,
        &[],
        None,
        0xd8,
        Some(duplicate_commit),
    );
    assert!(
        run_stale_reconciliation_fixture(&duplicate_seed, fixture.after_commit_id)
            .await
            .is_err(),
        "the actual stale caller rejects a duplicate page vector"
    );

    let wrong_owner_seed = rewrite_three_page_fixture(
        &fixture,
        fixture.page_ids.clone(),
        &[],
        Some(ChangeCatalogEntry {
            owner: ChangeCatalogOwner::CommitMember {
                commit_object_id: fixture.seed.commit_object_id,
                ordinal: 0,
            },
        }),
        0xd5,
    );
    assert!(
        run_stale_reconciliation_fixture(&wrong_owner_seed, fixture.after_commit_id)
            .await
            .is_err(),
        "the actual stale caller rejects a selected member with a substituted owner"
    );

    let branch_owner_seed = rewrite_three_page_fixture(
        &fixture,
        fixture.page_ids.clone(),
        &[],
        Some(ChangeCatalogEntry {
            owner: ChangeCatalogOwner::BranchRef {
                ref_change_object_id: fixture.seed.ref_change_object_id,
                branch_id: fixture.seed.branch_id,
            },
        }),
        0xd6,
    );
    assert!(
        run_stale_reconciliation_fixture(&branch_owner_seed, fixture.after_commit_id)
            .await
            .is_err(),
        "the actual stale caller rejects a selected member substituted with a branch ref"
    );

    let mut missing_root_seed = fixture.seed.clone();
    missing_root_seed
        .objects
        .remove(missing_root_seed.repository_root_id);
    assert!(
        run_stale_reconciliation_fixture(&missing_root_seed, fixture.after_commit_id)
            .await
            .is_err(),
        "the actual stale caller rejects a missing selector/root binding"
    );
}
