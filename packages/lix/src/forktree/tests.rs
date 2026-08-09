use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use bytes::Bytes;

use crate::commit_graph::CommitGraphContext;
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::storage::{
    BeginScanOptions, CommitResult, CoreProjection, GetManyRequest, GetManyResult, GetOptions, Key,
    KeyRange, Memory, MemoryRead, MemoryWrite, PutBatch, ReadOptions, ScanCursor, Storage,
    StorageError, StorageRead, StorageWrite, WriteOptions,
};
use crate::storage_adapter::{
    SharedStorageAdapterRead, StorageAdapter, StorageAdapterRead, StorageAdapterReadScope,
    StorageReadOptions, StorageWriteSet, StorageWriteSetError,
};

use super::model::{
    GcProgressSelectorV2, GcProgressV2, branch_selector_key, gc_progress_selector_key,
    global_selector_key, snapshot_selector_key, upload_binding_digest, upload_selector_key,
};
use super::object::OBJECT_SPACE;
use super::serving::{retire_change_catalog_entries, retire_commit_catalog_entries};
use super::tree::{
    ImmutableObjectSet, build_change_catalog, build_commit_catalog, build_retention_tree,
    build_state_tree, empty_receipt_tree, insert_receipt_part, lookup, scan_all,
    validate_branch_snapshot_ref_edge, validate_change_catalog_back_edge,
    validate_commit_catalog_back_edge, validate_receipt_tree, validate_upload_progress_tree,
    validate_upload_selector_progress,
};
use super::view::SELECTOR_SPACE;
use super::{
    BlobChunkRefV1, BlobChunkV1, BlobManifestV1, BranchSelectorV1, BranchSnapshotV1,
    BranchStateTransition, CanonicalBranchId, CanonicalUploadId, CatalogPage, ChangeCatalogEntry,
    ChangeCatalogOwner, ChangeId, ChangeObjectV1, CoherentView, CommitCatalogEntry, CommitId,
    CommitMemberV1, CommitObjectV1, ForkTreeReadFacade, GcBudget, GcStepStatus, GlobalSelectorV1,
    ObjectId, PreparedPublication, RECEIPT_TREE_FANOUT, RECEIPT_TREE_LEAF_ENTRIES, ReceiptTreeEdit,
    ReceiptTreeRoot, RepositoryRootV1, SelectorExpectation, SnapshotRole, SnapshotSelectorId,
    SnapshotSelectorV1, SnapshotTargetV1, StateCell, StateCellRef, StateKey, StateKeyRef,
    StateSource, StateTreeMutation, StateValueRef, UntrackedValueRef, UploadBindingRef,
    UploadPartV1, UploadProgressV1, UploadSelectorV1, VisibleStateRow, abort_corrupt_gc,
    advance_gc, edit_state_tree, encode_state_key, encode_state_value, load_change, load_commit,
    load_commit_member_records, load_commit_topologies, open_coherent_view, page_changes,
    page_commits, prepare_upload_completion, put_change_catalog_entries,
    put_commit_catalog_entries, state_point, state_range,
};

fn raw_id(byte: u8) -> [u8; 16] {
    [byte; 16]
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
fn topology_cache_is_private_and_inseparable_from_its_storage_read() {
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
    let blob = include_str!("blob.rs");
    let facade = include_str!("mod.rs");
    assert!(model.contains("pub(super) canonical_blob_id: BlobId"));
    assert!(!model.contains("pub(crate) canonical_blob_id"));
    assert!(!facade.contains("canonical_blob_id"));
    assert!(blob.contains("canonical_blob_id: semantic_id_builder.finish()"));
    assert!(blob.contains("manifest.canonical_blob_id != semantic_id"));
    assert!(!blob.contains("BTreeMap<crate::binary_cas::BlobId"));
    assert!(!blob.contains("fn canonical_blob_id"));
}

#[tokio::test]
async fn forktree_json_object_materializes_and_rejects_side_plane_or_corruption() {
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
    let legacy = crate::json_store::JsonSlot::Ref(crate::json_store::JsonRef::for_content(
        b"legacy side plane",
    ));
    assert!(
        facade.load_json_slot(&legacy).await.is_err(),
        "legacy JSON_SPACE references must never fall back"
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
    let (member, source_commit, source_change) = super::serving::select_historical_commit_member(
        &view,
        seed.commit_id,
        seed.semantic_change_id,
    )
    .await
    .expect("select authenticated historical member");
    assert_eq!(source_commit.commit_id, seed.commit_id);
    assert_eq!(source_change.change_id(), seed.semantic_change_id);
    assert_eq!(
        member,
        CommitMemberV1::selected(seed.semantic_change_object_id, seed.commit_object_id, 0)
    );
    let entry = ChangeCatalogEntry {
        change_object_id: seed.semantic_change_object_id,
        owner: ChangeCatalogOwner::CommitMember {
            commit_object_id: seed.commit_object_id,
            ordinal: 0,
        },
    };
    super::serving::validate_member_catalog_owner(
        view.storage_read(),
        view.repository_root().commit_catalog_root,
        content_id(0xa1),
        2,
        0,
        member,
        entry,
    )
    .await
    .expect("older selected source is valid");
    assert!(
        super::serving::validate_member_catalog_owner(
            view.storage_read(),
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
async fn selected_commit_member_rejects_missing_or_remapped_source_catalog_entry() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;

    let source_commit = CommitObjectV1 {
        commit_id: seed.commit_id,
        generation: 1,
        parent_commit_object_ids: Vec::new(),
        members: vec![CommitMemberV1::introduced(seed.semantic_change_object_id)],
        global_state_root: seed.global_state_root,
        local_state_root: seed.local_state_root,
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
    let member = CommitMemberV1::selected(seed.semantic_change_object_id, seed.commit_object_id, 0);
    let entry = ChangeCatalogEntry {
        change_object_id: seed.semantic_change_object_id,
        owner: ChangeCatalogOwner::CommitMember {
            commit_object_id: seed.commit_object_id,
            ordinal: 0,
        },
    };
    assert!(
        super::serving::validate_member_catalog_owner(
            view.storage_read(),
            remapped_catalog.root.object_id,
            content_id(0xa1),
            2,
            0,
            member,
            entry,
        )
        .await
        .is_err(),
        "a remapped CommitCatalog source must fail closed"
    );
    assert!(
        super::serving::validate_member_catalog_owner(
            view.storage_read(),
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

fn public_change_id(byte: u8) -> crate::changelog::ChangeId {
    crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(raw_id(byte)))
}

fn state_entry(
    primary_key: &str,
    cell: StateCellRef<'_>,
    commit_byte: u8,
    manifests: &[ObjectId],
) -> (Vec<u8>, Vec<u8>) {
    let entity_pk = EntityPk::single(primary_key);
    let key = encode_state_key(StateKeyRef {
        schema_key: "app.row",
        file_id: Some("file"),
        entity_pk: &entity_pk,
    });
    let value = encode_state_value(StateValueRef {
        change_id: public_change_id(commit_byte.wrapping_add(1)),
        commit_id: public_commit_id(commit_byte),
        created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
        cell,
        metadata: None,
        origin_key: None,
        blob_manifest_object_ids: manifests,
    })
    .expect("state value");
    (key, value)
}

fn blob_ref_state_entry(
    primary_key: &str,
    blob_id: crate::binary_cas::BlobId,
    size_bytes: u64,
    commit_byte: u8,
    manifest: ObjectId,
) -> (Vec<u8>, Vec<u8>) {
    let entity_pk = EntityPk::single(primary_key);
    let key = encode_state_key(StateKeyRef {
        schema_key: "lix_binary_blob_ref",
        file_id: Some(primary_key),
        entity_pk: &entity_pk,
    });
    let semantic_value = serde_json::json!({
        "id": primary_key,
        "blob_hash": blob_id.to_hex(),
        "size_bytes": size_bytes,
    })
    .to_string();
    let value = encode_state_value(StateValueRef {
        change_id: public_change_id(commit_byte.wrapping_add(1)),
        commit_id: public_commit_id(commit_byte),
        created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
        cell: StateCellRef::Value(&semantic_value),
        metadata: None,
        origin_key: None,
        blob_manifest_object_ids: &[manifest],
    })
    .expect("blob-ref state value");
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
    let semantic_change_id = ChangeId::from_bytes(raw_id(0x30));
    let ref_change_id = ChangeId::from_bytes(raw_id(0x31));
    let mut objects = ImmutableObjectSet::default();

    let mut global_rows = vec![
        state_entry("a", StateCellRef::Value("global-a"), 0x20, &[]),
        state_entry("b", StateCellRef::Value("global-b"), 0x20, &[]),
        state_entry("c", StateCellRef::Null, 0x20, &[]),
    ];
    global_rows.sort_by(|left, right| left.0.cmp(&right.0));
    let state_keys = global_rows.iter().map(|row| row.0.clone()).collect();
    let global_state = build_state_tree(&global_rows).expect("global state");
    let global_state_root = global_state.root.object_id;
    objects
        .extend(global_state.objects)
        .expect("global objects");

    let mut local_rows = vec![
        state_entry("a", StateCellRef::Value("local-a"), 0x20, &[]),
        state_entry("b", StateCellRef::Tombstone, 0x20, &[]),
        state_entry("d", StateCellRef::Null, 0x20, &[]),
    ];
    local_rows.sort_by(|left, right| left.0.cmp(&right.0));
    let local_state = build_state_tree(&local_rows).expect("local state");
    let local_state_root = local_state.root.object_id;
    objects.extend(local_state.objects).expect("local objects");
    let retention = build_retention_tree(&[]).expect("retention");
    let retention_root = retention.root.object_id;
    objects
        .extend(retention.objects)
        .expect("retention objects");

    let semantic_change = ChangeObjectV1::Semantic {
        change_id: semantic_change_id,
        payload: b"semantic-change".to_vec(),
        json_payload_object_ids: Vec::new(),
    };
    let (semantic_change_object_id, semantic_change_bytes) =
        semantic_change.encode().expect("semantic change");
    objects
        .insert(semantic_change_object_id, semantic_change_bytes)
        .expect("semantic object");
    let commit = CommitObjectV1 {
        commit_id,
        generation: 1,
        parent_commit_object_ids: Vec::new(),
        members: vec![CommitMemberV1::introduced(semantic_change_object_id)],
        global_state_root,
        local_state_root,
        metadata: b"commit".to_vec(),
    };
    let (commit_object_id, commit_bytes) = commit.encode().expect("commit");
    objects
        .insert(commit_object_id, commit_bytes)
        .expect("commit object");
    let ref_change = ChangeObjectV1::BranchRef {
        change_id: ref_change_id,
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
    let change_catalog = build_change_catalog(&[
        (
            semantic_change_id,
            ChangeCatalogEntry {
                change_object_id: semantic_change_object_id,
                owner: ChangeCatalogOwner::CommitMember {
                    commit_object_id,
                    ordinal: 0,
                },
            },
        ),
        (
            ref_change_id,
            ChangeCatalogEntry {
                change_object_id: ref_change_object_id,
                owner: ChangeCatalogOwner::BranchRef {
                    ref_change_object_id,
                    branch_id,
                },
            },
        ),
    ])
    .expect("change catalog");
    let change_catalog_root = change_catalog.root.object_id;
    objects
        .extend(change_catalog.objects)
        .expect("change catalog objects");
    let repository_root = RepositoryRootV1 {
        global_state_root,
        commit_catalog_root,
        change_catalog_root,
        retention_policy_root: retention_root,
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
    let orphan = ChangeObjectV1::Semantic {
        change_id: ChangeId::from_bytes(raw_id(0xee)),
        payload: b"unreachable".to_vec(),
        json_payload_object_ids: Vec::new(),
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
    let commit_catalog = build_commit_catalog(commits).expect("replacement commit catalog");
    let commit_catalog_root = commit_catalog.root.object_id;
    seed.objects
        .extend(commit_catalog.objects)
        .expect("replacement commit catalog objects");
    let change_catalog = build_change_catalog(changes).expect("replacement change catalog");
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
    let semantic_commit = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(identity)),
        generation: identity as u64,
        parent_commit_object_ids: vec![view.branch_snapshot().semantic_head_commit_object_id],
        members: Vec::new(),
        global_state_root: view.repository_root().global_state_root,
        local_state_root: state_edit.root,
        metadata: vec![identity],
    };
    let (commit_object_id, _) = semantic_commit.encode().expect("next commit");
    let ref_change = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(identity.wrapping_add(1))),
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
        view.storage_read(),
    )
    .await
    .expect("commit catalog edit");
    let change_catalog_edit = put_change_catalog_entries(
        view.repository_root().change_catalog_root,
        &[(
            ref_change.change_id(),
            ChangeCatalogEntry {
                change_object_id: ref_object_id,
                owner: ChangeCatalogOwner::BranchRef {
                    ref_change_object_id: ref_object_id,
                    branch_id: view.branch_id(),
                },
            },
        )],
        view.storage_read(),
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

    for cell in [
        StateCellRef::Value("value"),
        StateCellRef::Null,
        StateCellRef::Tombstone,
    ] {
        let (_, encoded) = state_entry("typed", cell, 7, &[]);
        let decoded: super::StateValue = super::decode_state_value(&encoded).expect("typed state");
        assert_eq!(
            decoded.cell.deleted(),
            matches!(cell, StateCellRef::Tombstone)
        );
    }
    let (key, _) = state_entry("typed-key", StateCellRef::Null, 7, &[]);
    let decoded_key: super::StateKey = super::decode_state_key(&key).expect("typed key");
    assert_eq!(decoded_key.schema_key, "app.row");
    assert!(super::encode_state_prefix("app.row", Some("file")).len() < key.len());
    assert!(build_state_tree(&[(b"opaque".to_vec(), b"opaque".to_vec())]).is_err());
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
        view.storage_read(),
    )
    .await
    .expect("update/remove path copy");
    assert_eq!(edit.entry_count(), 2);
    assert!(edit.copied_nodes() >= 2);
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
        entity_pk: &EntityPk::single("absent"),
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
        entity_pk: EntityPk::single("a"),
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
    let commit = CommitObjectV1 {
        commit_id,
        generation: 1,
        parent_commit_object_ids: Vec::new(),
        members: vec![CommitMemberV1::introduced(semantic_change_object_id)],
        global_state_root: content_id(0xf1),
        local_state_root: seed.local_state_root,
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
                change_object_id: semantic_change_object_id,
                owner: ChangeCatalogOwner::CommitMember {
                    commit_object_id,
                    ordinal: 0,
                },
            },
        ),
        (
            ref_change_id,
            ChangeCatalogEntry {
                change_object_id: ref_change_object_id,
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
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, seed.semantic_change_id.as_bytes());
    assert_eq!(rows[1].0, seed.ref_change_id.as_bytes());
    let bad = ChangeCatalogEntry {
        change_object_id: seed.semantic_change_object_id,
        owner: ChangeCatalogOwner::CommitMember {
            commit_object_id: seed.commit_object_id,
            ordinal: 9,
        },
    };
    assert!(validate_change_catalog_back_edge(seed.semantic_change_id, bad, &load).is_err());
    let semantic = ChangeObjectV1::decode(
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
async fn path_copy_catalog_put_and_view_bound_resume_are_bounded() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("view");
    let old_token = page_commits(&view, None, 1)
        .await
        .expect("first page")
        .resume_token
        .expect("resume token");
    let state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        Vec::new(),
        view.storage_read(),
    )
    .await
    .expect("no-op state edit");
    let transition = branch_transition(&view, state_edit, 0x60).await;
    assert!(transition.commit_catalog_edit.copied_nodes() <= 2);
    assert_eq!(transition.commit_catalog_edit.entry_count(), 2);
    let mut publication = PreparedPublication::from_branch_view(&view).expect("publication");
    publication
        .publish_state_transition(&view, transition)
        .await
        .expect("typed transition");
    drop(view);
    commit_publication_for_test(publication, &storage)
        .await
        .expect("commit transition");
    let reopened = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("reopen");
    assert!(matches!(
        page_commits(&reopened, Some(&old_token), 1).await,
        Err(StorageError::InvalidCursor)
    ));
    let first: CatalogPage<(CommitId, CommitObjectV1)> =
        page_commits(&reopened, None, 1).await.expect("page one");
    let second = page_commits(&reopened, first.resume_token.as_deref(), 1)
        .await
        .expect("page two");
    assert_eq!(first.entries.len(), 1);
    assert_eq!(second.entries.len(), 1);
    assert_eq!(
        load_commit(&reopened, CommitId::from_bytes(raw_id(0x60)))
            .await
            .expect("load")
            .expect("new commit")
            .commit_id,
        CommitId::from_bytes(raw_id(0x60))
    );
    assert!(
        load_change(&reopened, ChangeId::from_bytes(raw_id(0x61)))
            .await
            .expect("change")
            .is_some()
    );
    assert_eq!(
        page_changes(&reopened, None, 2)
            .await
            .expect("changes")
            .entries
            .len(),
        2
    );
}

#[derive(Clone)]
struct CountingStorage {
    inner: Memory,
    begin_reads: Arc<AtomicUsize>,
}

struct CountingRead {
    inner: MemoryRead,
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
async fn coherent_open_uses_one_read_and_visited_edges_fail_closed() {
    let seed = build_seed();
    let storage = CountingStorage::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("open");
    assert_eq!(storage.begin_reads.load(Ordering::Relaxed), 1);
    let token = view.bind_resume_key(
        view.repository_root().change_catalog_root,
        seed.semantic_change_id.as_bytes(),
    );
    assert_eq!(
        view.validate_resume_key(view.repository_root().change_catalog_root, &token)
            .expect("token"),
        seed.semantic_change_id.as_bytes()
    );
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
    assert!(load_change(&view, seed.semantic_change_id).await.is_err());
}

#[tokio::test]
async fn commit_topology_batch_loads_one_shared_parent_once_and_seeds_graph_walk() {
    let mut seed = build_seed();
    let grandparent = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(0x50)),
        generation: 1,
        parent_commit_object_ids: Vec::new(),
        members: Vec::new(),
        global_state_root: seed.global_state_root,
        local_state_root: seed.local_state_root,
        metadata: b"grandparent".to_vec(),
    };
    let (grandparent_object_id, grandparent_bytes) = grandparent.encode().expect("grandparent");
    seed.objects
        .insert(grandparent_object_id, grandparent_bytes)
        .expect("grandparent object");
    let parent = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(0x51)),
        generation: 2,
        parent_commit_object_ids: vec![grandparent_object_id],
        members: vec![CommitMemberV1::introduced(seed.semantic_change_object_id)],
        global_state_root: seed.global_state_root,
        local_state_root: seed.local_state_root,
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
        global_state_root: seed.global_state_root,
        local_state_root: seed.local_state_root,
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
        global_state_root: seed.global_state_root,
        local_state_root: seed.local_state_root,
        metadata: b"child-b".to_vec(),
    };
    let (child_b_object_id, child_b_bytes) = child_b.encode().expect("child b");
    seed.objects
        .insert(child_b_object_id, child_b_bytes)
        .expect("child b object");
    let creation = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0x54)),
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
    let semantic_change_object_id = seed.semantic_change_object_id;
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
                    change_object_id: semantic_change_object_id,
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id: parent_object_id,
                        ordinal: 0,
                    },
                },
            ),
            (
                creation.change_id(),
                ChangeCatalogEntry {
                    change_object_id: creation_object_id,
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
        member_object: seed.semantic_change_object_id,
        parent_object_reads: Arc::clone(&parent_object_reads),
        grandparent_object_reads: Arc::clone(&grandparent_object_reads),
        member_object_reads: Arc::clone(&member_object_reads),
    };
    let mut graph = CommitGraphContext::new().reader(read);
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
    writes.delete(
        OBJECT_SPACE,
        seed.semantic_change_object_id.as_bytes().to_vec(),
    );
    commit_write_set_for_test(writes, &storage).await;

    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("post-corruption read"),
    );
    assert!(
        load_commit_topologies(&read, &[public_commit_id(0x52), public_commit_id(0x53)],)
            .await
            .expect("member corruption remains latent for sibling topology")
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
async fn coherent_open_defers_ref_target_authentication_until_visited() {
    let mut seed = build_seed();
    let bad_ref = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0x40)),
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
    let catalog = build_change_catalog(&[
        (
            seed.semantic_change_id,
            ChangeCatalogEntry {
                change_object_id: seed.semantic_change_object_id,
                owner: ChangeCatalogOwner::CommitMember {
                    commit_object_id: seed.commit_object_id,
                    ordinal: 0,
                },
            },
        ),
        (
            seed.ref_change_id,
            ChangeCatalogEntry {
                change_object_id: seed.ref_change_object_id,
                owner: ChangeCatalogOwner::BranchRef {
                    ref_change_object_id: seed.ref_change_object_id,
                    branch_id: seed.branch_id,
                },
            },
        ),
        (
            bad_ref.change_id(),
            ChangeCatalogEntry {
                change_object_id: bad_ref_id,
                owner: ChangeCatalogOwner::BranchRef {
                    ref_change_object_id: bad_ref_id,
                    branch_id: seed.branch_id,
                },
            },
        ),
    ])
    .expect("bad catalog");
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
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("bounded open authenticates only selected root envelopes");
    assert!(load_change(&view, bad_ref.change_id()).await.is_err());
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
                    change_object_id: semantic_change_object_id,
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id,
                        ordinal: 1,
                    },
                },
            ),
            (
                ref_change_id,
                ChangeCatalogEntry {
                    change_object_id: ref_change_object_id,
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

    // A RefChange predecessor must be on the same branch and its after target
    // must equal the successor's before target.
    let mut bad_ref_history = build_seed();
    let latest = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0x33)),
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
                    change_object_id: semantic_change_object_id,
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id,
                        ordinal: 0,
                    },
                },
            ),
            (
                ref_change_id,
                ChangeCatalogEntry {
                    change_object_id: ref_change_object_id,
                    owner: ChangeCatalogOwner::BranchRef {
                        ref_change_object_id,
                        branch_id,
                    },
                },
            ),
            (
                latest.change_id(),
                ChangeCatalogEntry {
                    change_object_id: latest_id,
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
        global_state_root: bad_generation.global_state_root,
        local_state_root: bad_generation.local_state_root,
        metadata: b"parent".to_vec(),
    };
    let (parent_id, parent_bytes) = parent.encode().expect("parent commit");
    bad_generation
        .objects
        .insert(parent_id, parent_bytes)
        .expect("parent object");
    let child = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(0x42)),
        generation: 2,
        parent_commit_object_ids: vec![parent_id],
        members: vec![CommitMemberV1::introduced(
            bad_generation.semantic_change_object_id,
        )],
        global_state_root: bad_generation.global_state_root,
        local_state_root: bad_generation.local_state_root,
        metadata: b"child".to_vec(),
    };
    let (child_id, child_bytes) = child.encode().expect("child commit");
    bad_generation
        .objects
        .insert(child_id, child_bytes)
        .expect("child object");
    let creation = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0x43)),
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
    let semantic_change_object_id = bad_generation.semantic_change_object_id;
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
                    change_object_id: semantic_change_object_id,
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id: child_id,
                        ordinal: 0,
                    },
                },
            ),
            (
                creation.change_id(),
                ChangeCatalogEntry {
                    change_object_id: creation_id,
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
async fn deterministic_reader_pin_safe_point_and_cursor_oracle() {
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
    let old_catalog_root = old_view.repository_root().change_catalog_root;
    let old_resume = old_view.bind_resume_key(old_catalog_root, seed.semantic_change_id.as_bytes());
    assert!(old_view.load_object_bytes(target_id).await.is_ok());

    let current = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("release view");
    let selector_key = snapshot_selector_key(SnapshotRole::Checkpoint, checkpoint_id);
    let keys = [Key(selector_key)];
    let loaded = current
        .storage_read()
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
        current.storage_read(),
    )
    .await
    .expect("unchanged commit catalog");
    let change_catalog_edit = retire_change_catalog_entries(
        current.repository_root().change_catalog_root,
        &[],
        current.storage_read(),
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
    assert_eq!(
        old_view
            .validate_resume_key(old_catalog_root, &old_resume)
            .expect("old cursor"),
        seed.semantic_change_id.as_bytes()
    );
    let new_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("new coherent view");
    assert!(matches!(
        new_view.validate_resume_key(old_catalog_root, &old_resume),
        Err(StorageError::InvalidCursor)
    ));
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
async fn corrupted_persisted_gc_index_aborts_without_authorizing_deletion() {
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
    assert_eq!(
        abort_corrupt_gc(&storage).await.expect("abort corrupt GC"),
        GcStepStatus::AbortedCorruptProgress
    );
    assert!(!selector_present(&storage, gc_progress_selector_key()).await);
    sweep(&storage, seed.branch_id).await;
    open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("semantic graph survives maintenance corruption");
}

#[tokio::test]
async fn state_and_catalog_publication_inputs_are_bound_to_the_selected_view() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("view");

    assert!(
        edit_state_tree(
            view.branch_snapshot().local_state_root,
            vec![StateTreeMutation::remove(vec![0xff])],
            view.storage_read(),
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
            view.storage_read(),
        )
        .await
        .is_err(),
        "catalog updates must use canonical raw-UUID order"
    );

    let wrong_base = edit_state_tree(
        view.repository_root().global_state_root,
        Vec::new(),
        view.storage_read(),
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
        vec![StateTreeMutation::insert(key, value)],
        view.storage_read(),
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
        view.storage_read(),
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
                    entity_pk: &EntityPk::single("a"),
                }),
                duplicate_value,
            ),
        ],
        view.storage_read(),
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
        .storage_read()
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
    let manifest = BlobManifestV1 {
        logical_bytes: 4,
        ordered_chunks: upload.part.ordered_chunks.clone(),
        canonical_blob_id: crate::binary_cas::BlobId::from_content(b"data"),
        content_digest: *blake3::hash(b"data").as_bytes(),
    };
    let (manifest_id, _) = manifest.encode().expect("manifest");
    let blob_id = crate::binary_cas::BlobId::from_content(b"data");
    let (key, value) = blob_ref_state_entry("blob", blob_id, 4, 0x70, manifest_id);
    let wrong_owner_value = serde_json::json!({
        "id": "not-blob",
        "blob_hash": blob_id.to_hex(),
        "size_bytes": 4,
    })
    .to_string();
    let (wrong_owner_key, wrong_owner) = state_entry(
        "not-blob",
        StateCellRef::Value(&wrong_owner_value),
        0x70,
        &[manifest_id],
    );
    let (mismatched_owner_key, mismatched_owner) =
        blob_ref_state_entry("mismatched", blob_id, 5, 0x70, manifest_id);

    // A valid multi-chunk manifest must not be transplantable beneath a
    // same-size state owner carrying another public BlobId. The manifest's
    // canonical semantic identity is authenticated before range chunks are
    // selected, so this check remains O(chunk metadata + visited payload).
    const FIXED_CHUNK_BYTES: usize = 1024 * 1024;
    let owner_payload = vec![b'a'; FIXED_CHUNK_BYTES + 1];
    let transplanted_payload = vec![b'b'; FIXED_CHUNK_BYTES + 1];
    let owner_blob_id = crate::binary_cas::BlobId::from_content(&owner_payload);
    let transplanted_blob_id = crate::binary_cas::BlobId::from_content(&transplanted_payload);
    assert_ne!(owner_blob_id, transplanted_blob_id);
    let transplanted_chunks = [
        BlobChunkV1 {
            bytes: Bytes::copy_from_slice(&transplanted_payload[..FIXED_CHUNK_BYTES]),
        },
        BlobChunkV1 {
            bytes: Bytes::copy_from_slice(&transplanted_payload[FIXED_CHUNK_BYTES..]),
        },
    ];
    let transplanted_chunk_refs = transplanted_chunks
        .iter()
        .map(|chunk| {
            let (chunk_object_id, _) = chunk.encode().expect("transplanted chunk");
            BlobChunkRefV1 {
                chunk_object_id,
                declared_len: chunk.bytes.len() as u64,
            }
        })
        .collect::<Vec<_>>();
    let transplanted_manifest = BlobManifestV1 {
        logical_bytes: transplanted_payload.len() as u64,
        ordered_chunks: transplanted_chunk_refs,
        canonical_blob_id: transplanted_blob_id,
        content_digest: *blake3::hash(&transplanted_payload).as_bytes(),
    };
    let (transplanted_manifest_id, _) = transplanted_manifest
        .encode()
        .expect("transplanted manifest");
    let (transplanted_owner_key, transplanted_owner) = blob_ref_state_entry(
        "transplanted",
        owner_blob_id,
        owner_payload.len() as u64,
        0x70,
        transplanted_manifest_id,
    );
    let (valid_multichunk_key, valid_multichunk_owner) = blob_ref_state_entry(
        "valid-multichunk",
        transplanted_blob_id,
        transplanted_payload.len() as u64,
        0x70,
        transplanted_manifest_id,
    );
    let state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![
            StateTreeMutation::insert(wrong_owner_key.clone(), wrong_owner),
            StateTreeMutation::insert(key.clone(), value),
            StateTreeMutation::insert(mismatched_owner_key.clone(), mismatched_owner),
            StateTreeMutation::insert(transplanted_owner_key.clone(), transplanted_owner),
            StateTreeMutation::insert(valid_multichunk_key.clone(), valid_multichunk_owner),
        ],
        view.storage_read(),
    )
    .await
    .expect("state edit");
    let transition = branch_transition(&view, state_edit, 0x70).await;
    let mut publish = PreparedPublication::from_branch_view(&view).expect("completion publication");
    for chunk in &transplanted_chunks {
        publish
            .stage_blob_chunk(chunk)
            .expect("stage transplanted chunk");
    }
    publish
        .stage_blob_manifest(&transplanted_manifest)
        .expect("stage transplanted manifest");
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
            .load_blob_ranges_many(&[(transplanted_ref, 0..1)])
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
            .bytes,
        b"b"
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
            .load_blob_bytes_many(&[blob_ref])
            .await
            .is_err(),
        "an authenticated blob edge must not detach from its selecting StorageRead"
    );
    assert_eq!(
        reopened
            .load_blob_bytes_many(&[blob_ref])
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
    assert_eq!(range.bytes, b"at");
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
        .storage_read()
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
    let semantic_id = crate::binary_cas::BlobId::from_content(valid_payload);
    let valid_chunk = BlobChunkV1 {
        bytes: Bytes::copy_from_slice(valid_payload),
    };
    let wrong_chunk = BlobChunkV1 {
        bytes: Bytes::copy_from_slice(wrong_payload),
    };
    let (valid_chunk_id, _) = valid_chunk.encode().expect("valid duplicate chunk");
    let (wrong_chunk_id, _) = wrong_chunk.encode().expect("wrong duplicate chunk");
    let valid_manifest = BlobManifestV1 {
        logical_bytes: valid_payload.len() as u64,
        ordered_chunks: vec![BlobChunkRefV1 {
            chunk_object_id: valid_chunk_id,
            declared_len: valid_payload.len() as u64,
        }],
        canonical_blob_id: semantic_id,
        content_digest: *blake3::hash(valid_payload).as_bytes(),
    };
    let wrong_manifest = BlobManifestV1 {
        logical_bytes: wrong_payload.len() as u64,
        ordered_chunks: vec![BlobChunkRefV1 {
            chunk_object_id: wrong_chunk_id,
            declared_len: wrong_payload.len() as u64,
        }],
        // This is the exact duplicate-owner trap: the wrong row claims the
        // same semantic ID while pointing at different authenticated bytes.
        canonical_blob_id: semantic_id,
        content_digest: *blake3::hash(wrong_payload).as_bytes(),
    };
    let (valid_manifest_id, _) = valid_manifest.encode().expect("valid duplicate manifest");
    let (wrong_manifest_id, _) = wrong_manifest.encode().expect("wrong duplicate manifest");
    let (wrong_key, wrong_value) = blob_ref_state_entry(
        "duplicate-a-wrong",
        semantic_id,
        valid_payload.len() as u64,
        0x70,
        wrong_manifest_id,
    );
    let (valid_key, valid_value) = blob_ref_state_entry(
        "duplicate-b-valid",
        semantic_id,
        valid_payload.len() as u64,
        0x70,
        valid_manifest_id,
    );
    let mut mutations = vec![
        StateTreeMutation::insert(wrong_key, wrong_value),
        StateTreeMutation::insert(valid_key.clone(), valid_value),
    ];
    mutations.sort_by(|left, right| {
        let left_key: &[u8] = match left {
            StateTreeMutation::Insert { key, .. }
            | StateTreeMutation::Update { key, .. }
            | StateTreeMutation::Remove { key } => key,
        };
        let right_key: &[u8] = match right {
            StateTreeMutation::Insert { key, .. }
            | StateTreeMutation::Update { key, .. }
            | StateTreeMutation::Remove { key } => key,
        };
        left_key.cmp(right_key)
    });
    let state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        mutations,
        view.storage_read(),
    )
    .await
    .expect("duplicate-owner state edit");
    let transition = branch_transition(&view, state_edit, 0x70).await;
    let mut publication = PreparedPublication::from_branch_view(&view).expect("publication");
    publication
        .stage_blob_chunk(&valid_chunk)
        .expect("stage valid duplicate chunk");
    publication
        .stage_blob_chunk(&wrong_chunk)
        .expect("stage wrong duplicate chunk");
    publication
        .stage_blob_manifest(&valid_manifest)
        .expect("stage valid duplicate manifest");
    publication
        .stage_blob_manifest(&wrong_manifest)
        .expect("stage wrong duplicate manifest");
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
    assert_eq!(ranges[0].as_ref().expect("first range").bytes, b"a");
    assert_eq!(ranges[1].as_ref().expect("second range").bytes, b"a");
    assert_eq!(
        reader
            .load_bytes_for_state_keys(&[selected_key])
            .await
            .expect("selected duplicate-owner full read")
            .into_vec(),
        vec![Some(valid_payload.to_vec())]
    );
}

async fn publish_untracked_manifest(
    storage: &Memory,
    seed: &SeedData,
    primary_key: &str,
    manifest: &BlobManifestV1,
    chunks: &[BlobChunkV1],
) -> ObjectId {
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .expect("untracked view");
    let (manifest_id, _) = manifest.encode().expect("manifest");
    let entity_pk = EntityPk::single(primary_key);
    let roots = [manifest_id];
    let mut publication = PreparedPublication::from_global_epoch(&view).expect("untracked put");
    for chunk in chunks {
        publication.stage_blob_chunk(chunk).expect("chunk");
    }
    publication.stage_blob_manifest(manifest).expect("manifest");
    publication
        .put_untracked_row(
            seed.branch_id,
            StateKeyRef {
                schema_key: "app.untracked",
                file_id: None,
                entity_pk: &entity_pk,
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

async fn delete_untracked(storage: &Memory, seed: &SeedData, primary_key: &str) {
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .expect("delete view");
    let entity_pk = EntityPk::single(primary_key);
    let mut publication = PreparedPublication::from_global_epoch(&view).expect("delete");
    publication
        .delete_untracked_row(
            seed.branch_id,
            StateKeyRef {
                schema_key: "app.untracked",
                file_id: None,
                entity_pk: &entity_pk,
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
    let catalog = build_change_catalog(&[
        (
            seed.semantic_change_id,
            ChangeCatalogEntry {
                change_object_id: seed.semantic_change_object_id,
                owner: ChangeCatalogOwner::CommitMember {
                    commit_object_id: seed.commit_object_id,
                    ordinal: 0,
                },
            },
        ),
        (
            seed.ref_change_id,
            ChangeCatalogEntry {
                change_object_id: seed.ref_change_object_id,
                owner: ChangeCatalogOwner::BranchRef {
                    ref_change_object_id: seed.ref_change_object_id,
                    branch_id: seed.branch_id,
                },
            },
        ),
        (
            disposable_ref_id,
            ChangeCatalogEntry {
                change_object_id: disposable_ref_object_id,
                owner: ChangeCatalogOwner::BranchRef {
                    ref_change_object_id: disposable_ref_object_id,
                    branch_id: disposable,
                },
            },
        ),
    ])
    .expect("disposable catalog");
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
    let manifest = BlobManifestV1 {
        logical_bytes: 4,
        ordered_chunks: upload.part.ordered_chunks.clone(),
        canonical_blob_id: crate::binary_cas::BlobId::from_content(b"data"),
        content_digest: *blake3::hash(b"data").as_bytes(),
    };
    let (manifest_id, _) = manifest.encode().expect("manifest");
    let (key, value) = state_entry(
        "disposable-blob",
        StateCellRef::Value("blob"),
        0x80,
        &[manifest_id],
    );
    let state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![StateTreeMutation::insert(key, value)],
        view.storage_read(),
    )
    .await
    .expect("state edit");
    let transition = branch_transition(&view, state_edit, 0x80).await;
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
        view.storage_read(),
    )
    .await
    .expect("retire commit");
    let change_catalog_edit = retire_change_catalog_entries(
        view.repository_root().change_catalog_root,
        &[],
        view.storage_read(),
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
    sweep(&storage, seed.branch_id).await;
    assert!(object_present(&storage, manifest_id).await);
    assert!(object_present(&storage, upload.chunk_id).await);

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("release view");
    let key = snapshot_selector_key(SnapshotRole::Checkpoint, checkpoint_id);
    let keys = [Key(key)];
    let loaded = view
        .storage_read()
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
        view.storage_read(),
    )
    .await
    .expect("final commit retirement");
    let change_catalog_edit = retire_change_catalog_entries(
        view.repository_root().change_catalog_root,
        &[initial_ref_id, ChangeId::from_bytes(raw_id(0x81))],
        view.storage_read(),
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

#[tokio::test]
async fn untracked_and_real_shared_chunk_roots_release_only_at_final_reference() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let shared = BlobChunkV1 {
        bytes: Bytes::from_static(b"shared"),
    };
    let first_unique = BlobChunkV1 {
        bytes: Bytes::from_static(b"one"),
    };
    let second_unique = BlobChunkV1 {
        bytes: Bytes::from_static(b"two"),
    };
    let (shared_id, _) = shared.encode().expect("shared chunk");
    let (first_unique_id, _) = first_unique.encode().expect("first unique");
    let (second_unique_id, _) = second_unique.encode().expect("second unique");
    let shared_reference = BlobChunkRefV1 {
        chunk_object_id: shared_id,
        declared_len: 6,
    };
    let first = BlobManifestV1 {
        logical_bytes: 9,
        ordered_chunks: vec![
            BlobChunkRefV1 {
                chunk_object_id: first_unique_id,
                declared_len: 3,
            },
            shared_reference.clone(),
        ],
        canonical_blob_id: crate::binary_cas::BlobId::from_content(b"oneshared"),
        content_digest: *blake3::hash(b"oneshared").as_bytes(),
    };
    let second = BlobManifestV1 {
        logical_bytes: 9,
        ordered_chunks: vec![
            BlobChunkRefV1 {
                chunk_object_id: second_unique_id,
                declared_len: 3,
            },
            shared_reference,
        ],
        canonical_blob_id: crate::binary_cas::BlobId::from_content(b"twoshared"),
        content_digest: *blake3::hash(b"twoshared").as_bytes(),
    };
    let first_id = publish_untracked_manifest(
        &storage,
        &seed,
        "one",
        &first,
        &[first_unique.clone(), shared.clone()],
    )
    .await;
    let second_id = publish_untracked_manifest(
        &storage,
        &seed,
        "two",
        &second,
        &[second_unique.clone(), shared.clone()],
    )
    .await;
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
    for (index, role) in [
        SnapshotRole::Recovery,
        SnapshotRole::Undo,
        SnapshotRole::Redo,
        SnapshotRole::BranchTombstone,
    ]
    .into_iter()
    .enumerate()
    {
        let selector_id = SnapshotSelectorId::from_bytes(raw_id(index as u8 + 2));
        roles
            .publish_current_snapshot_pin(&view, role, selector_id, SelectorExpectation::Absent)
            .expect("role selector");
    }
    drop(view);
    commit_publication_for_test(roles, &storage)
        .await
        .expect("all roles");
    sweep(&storage, seed.branch_id).await;
    assert!(object_present(&storage, target_id).await);
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
            change_object_id: content_id(2),
            owner: ChangeCatalogOwner::CommitMember {
                commit_object_id: content_id(3),
                ordinal: 7,
            },
        }
        .encode()
        .expect("change entry")
        .len(),
        69
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
