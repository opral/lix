//! Test-only dual-adapter application oracle for the unwired ForkTree owner.
//!
//! The harness receives only case names and typed results. It cannot name a
//! reserved space, construct a raw object publication, or forge a sweep. The
//! fixture bootstrap is authenticated inside `publication`, and reclamation
//! advances only through the bounded persisted GC owner.

use bytes::Bytes;

use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::storage::{CoreProjection, GetManyRequest, GetOptions, Key, Storage, StorageError};
use crate::storage_adapter::StorageAdapterRead;

use super::model::{
    branch_selector_key, snapshot_selector_key, upload_binding_digest, upload_selector_key,
};
use super::publication::{OracleBootstrap, commit_oracle_bootstrap};
use super::serving::{
    StateTreeEdit, put_change_catalog_entries, put_commit_catalog_entries,
    retire_change_catalog_entries, retire_commit_catalog_entries,
};
use super::tree::{
    ImmutableObjectSet, build_change_catalog, build_commit_catalog, build_retention_tree,
    build_state_tree, empty_receipt_tree, insert_receipt_part,
};
use super::view::SELECTOR_SPACE;
use super::{
    BlobChunkRefV1, BlobChunkV1, BlobManifestV1, BranchSelectorV1, BranchSnapshotV1,
    BranchStateTransition, CanonicalBranchId, CanonicalUploadId, ChangeCatalogEntry,
    ChangeCatalogOwner, ChangeId, ChangeObjectV1, CoherentView, CommitCatalogEntry, CommitId,
    CommitObjectV1, GcBudget, GcStepStatus, GlobalSelectorV1, ObjectId, PreparedPublication,
    ReceiptTreeEdit, RepositoryRootV1, SelectorExpectation, SnapshotRole, SnapshotSelectorId,
    SnapshotSelectorV1, SnapshotTargetV1, StateCell, StateCellRef, StateKeyRef, StateSource,
    StateTreeMutation, StateValueRef, UntrackedValueRef, UploadBindingRef, UploadPartV1,
    UploadProgressV1, UploadSelectorV1, advance_gc, edit_state_tree, encode_state_key,
    encode_state_value, open_coherent_view, page_changes, page_commits, prepare_upload_completion,
    state_point, state_range,
};

fn raw_id(byte: u8) -> [u8; 16] {
    [byte; 16]
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
) -> Result<(Vec<u8>, Vec<u8>), StorageError> {
    let entity_pk = EntityPk::single(primary_key);
    Ok((
        encode_state_key(StateKeyRef {
            schema_key: "app.row",
            file_id: Some("file"),
            entity_pk: &entity_pk,
        }),
        encode_state_value(StateValueRef {
            change_id: public_change_id(commit_byte.wrapping_add(1)),
            commit_id: public_commit_id(commit_byte),
            created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
            updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
            cell,
            metadata: None,
            origin_key: None,
            blob_manifest_object_ids: manifests,
        })
        .map_err(|error| StorageError::Corruption(error.to_string()))?,
    ))
}

fn state_key(primary_key: &str) -> Vec<u8> {
    let entity_pk = EntityPk::single(primary_key);
    encode_state_key(StateKeyRef {
        schema_key: "app.row",
        file_id: Some("file"),
        entity_pk: &entity_pk,
    })
}

#[derive(Clone)]
struct SeedData {
    objects: ImmutableObjectSet,
    branch_id: CanonicalBranchId,
    semantic_change_id: ChangeId,
    ref_change_id: ChangeId,
    global_selector: GlobalSelectorV1,
    branch_selector: BranchSelectorV1,
}

fn build_seed(
    rooted_manifests: &[ObjectId],
    extras: ImmutableObjectSet,
) -> Result<SeedData, StorageError> {
    let branch_id = CanonicalBranchId::from_bytes(raw_id(0x11));
    let commit_id = CommitId::from_bytes(raw_id(0x20));
    let semantic_change_id = ChangeId::from_bytes(raw_id(0x30));
    let ref_change_id = ChangeId::from_bytes(raw_id(0x31));
    let mut objects = extras;

    let global_state = build_state_tree(&[
        state_entry("fallback", StateCellRef::Value("global"), 0x20, &[])?,
        state_entry("hidden", StateCellRef::Value("global-hidden"), 0x20, &[])?,
        state_entry("shadow", StateCellRef::Value("global-shadow"), 0x20, &[])?,
    ])?;
    let global_state_root = global_state.root.object_id;
    objects.extend(global_state.objects)?;
    let local_state = build_state_tree(&[
        state_entry("hidden", StateCellRef::Tombstone, 0x20, &[])?,
        state_entry("null", StateCellRef::Null, 0x20, &[])?,
        state_entry("root", StateCellRef::Value("blob"), 0x20, rooted_manifests)?,
        state_entry("shadow", StateCellRef::Value("local-shadow"), 0x20, &[])?,
    ])?;
    let local_state_root = local_state.root.object_id;
    objects.extend(local_state.objects)?;
    let retention = build_retention_tree(&[])?;
    let retention_root = retention.root.object_id;
    objects.extend(retention.objects)?;

    let semantic_change = ChangeObjectV1::Semantic {
        change_id: semantic_change_id,
        payload: b"oracle-semantic-change".to_vec(),
    };
    let (semantic_change_object_id, semantic_change_bytes) = semantic_change.encode()?;
    objects.insert(semantic_change_object_id, semantic_change_bytes)?;
    let commit = CommitObjectV1 {
        commit_id,
        generation: 1,
        parent_commit_object_ids: Vec::new(),
        member_change_object_ids: vec![semantic_change_object_id],
        global_state_root,
        local_state_root,
        metadata: b"oracle-commit".to_vec(),
    };
    let (commit_object_id, commit_bytes) = commit.encode()?;
    objects.insert(commit_object_id, commit_bytes)?;
    let ref_change = ChangeObjectV1::BranchRef {
        change_id: ref_change_id,
        branch_id,
        before_semantic_head_commit_object_id: None,
        after_semantic_head_commit_object_id: Some(commit_object_id),
        previous_ref_change_object_id: None,
        payload: b"oracle-create-main".to_vec(),
    };
    let (ref_change_object_id, ref_change_bytes) = ref_change.encode()?;
    objects.insert(ref_change_object_id, ref_change_bytes)?;

    let commit_catalog =
        build_commit_catalog(&[(commit_id, CommitCatalogEntry { commit_object_id })])?;
    let commit_catalog_root = commit_catalog.root.object_id;
    objects.extend(commit_catalog.objects)?;
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
    ])?;
    let change_catalog_root = change_catalog.root.object_id;
    objects.extend(change_catalog.objects)?;
    let repository = RepositoryRootV1 {
        global_state_root,
        commit_catalog_root,
        change_catalog_root,
        retention_policy_root: retention_root,
    };
    let (repository_root_id, repository_bytes) = repository.encode()?;
    objects.insert(repository_root_id, repository_bytes)?;
    let snapshot = BranchSnapshotV1 {
        branch_id,
        local_state_root,
        semantic_head_commit_object_id: commit_object_id,
        latest_ref_change_object_id: Some(ref_change_object_id),
        historical_global_state_root: global_state_root,
    };
    let (branch_snapshot_id, snapshot_bytes) = snapshot.encode()?;
    objects.insert(branch_snapshot_id, snapshot_bytes)?;
    let global_selector = GlobalSelectorV1 {
        repository_root: repository_root_id,
        epoch: 1,
        selector_generation: 1,
    };
    let branch_selector = BranchSelectorV1 {
        branch_id,
        branch_snapshot_object_id: branch_snapshot_id,
        selector_generation: 1,
    };
    Ok(SeedData {
        objects,
        branch_id,
        semantic_change_id,
        ref_change_id,
        global_selector,
        branch_selector,
    })
}

async fn seed_storage<S: Storage>(storage: &S, seed: &SeedData) -> Result<(), StorageError> {
    commit_oracle_bootstrap(
        storage,
        OracleBootstrap {
            objects: seed.objects.clone(),
            global_selector: seed.global_selector,
            branch_selector: seed.branch_selector,
        },
    )
    .await
}

async fn sweep<S: Storage>(storage: &S) -> Result<u64, StorageError> {
    for _ in 0..20_000 {
        if let GcStepStatus::Complete { reclaimed } =
            advance_gc(storage, GcBudget::default()).await?
        {
            return Ok(reclaimed);
        }
    }
    Err(StorageError::Corruption(
        "bounded GC did not finish within the oracle step ceiling".to_owned(),
    ))
}

#[derive(Clone, Copy)]
enum ExpectedObject {
    Chunk,
    Manifest,
    Progress,
    SnapshotTarget,
}

fn authenticate_expected(
    id: ObjectId,
    bytes: &[u8],
    expected: ExpectedObject,
) -> Result<(), StorageError> {
    match expected {
        ExpectedObject::Chunk => BlobChunkV1::decode(id, bytes).map(|_| ()),
        ExpectedObject::Manifest => BlobManifestV1::decode(id, bytes).map(|_| ()),
        ExpectedObject::Progress => UploadProgressV1::decode(id, bytes).map(|_| ()),
        ExpectedObject::SnapshotTarget => SnapshotTargetV1::decode(id, bytes).map(|_| ()),
    }
}

async fn object_present<S: Storage>(
    storage: &S,
    branch_id: CanonicalBranchId,
    id: ObjectId,
    expected: ExpectedObject,
) -> Result<bool, StorageError> {
    let view = open_coherent_view(storage, branch_id).await?;
    match view.load_object_bytes(id).await {
        Ok(bytes) => authenticate_expected(id, &bytes, expected).map(|()| true),
        Err(StorageError::Corruption(message)) if message.contains(" is absent") => Ok(false),
        Err(error) => Err(error),
    }
}

async fn branch_transition<R: StorageAdapterRead>(
    view: &CoherentView<R>,
    state_edit: StateTreeEdit,
    identity: u8,
) -> Result<BranchStateTransition, StorageError> {
    let semantic_commit = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(identity)),
        generation: u64::from(identity),
        parent_commit_object_ids: vec![view.branch_snapshot().semantic_head_commit_object_id],
        member_change_object_ids: Vec::new(),
        global_state_root: view.repository_root().global_state_root,
        local_state_root: state_edit.root,
        metadata: vec![identity],
    };
    let (commit_object_id, _) = semantic_commit.encode()?;
    let ref_change = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(identity.wrapping_add(1))),
        branch_id: view.branch_id(),
        before_semantic_head_commit_object_id: Some(
            view.branch_snapshot().semantic_head_commit_object_id,
        ),
        after_semantic_head_commit_object_id: Some(commit_object_id),
        previous_ref_change_object_id: view.branch_snapshot().latest_ref_change_object_id,
        payload: vec![identity],
    };
    let (ref_object_id, _) = ref_change.encode()?;
    let commit_catalog_edit = put_commit_catalog_entries(
        view.repository_root().commit_catalog_root,
        &[(
            semantic_commit.commit_id,
            CommitCatalogEntry { commit_object_id },
        )],
        view.read(),
    )
    .await?;
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
        view.read(),
    )
    .await?;
    Ok(BranchStateTransition {
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
            local_state_root: state_edit.root,
            semantic_head_commit_object_id: commit_object_id,
            latest_ref_change_object_id: Some(ref_object_id),
            historical_global_state_root: view.repository_root().global_state_root,
        },
        state_edit,
    })
}

fn load_from(
    objects: &ImmutableObjectSet,
) -> impl Fn(ObjectId) -> Result<Bytes, StorageError> + '_ {
    move |id| {
        objects
            .get(id)
            .cloned()
            .ok_or_else(|| StorageError::Corruption(format!("oracle object {id} is absent")))
    }
}

#[derive(Clone)]
struct UploadData {
    upload_id: CanonicalUploadId,
    path: Vec<u8>,
    chunk: BlobChunkV1,
    chunk_id: ObjectId,
    part: UploadPartV1,
    receipt: ReceiptTreeEdit,
    progress: UploadProgressV1,
    progress_id: ObjectId,
    selector: UploadSelectorV1,
}

fn make_upload(name: &str, payload: &'static [u8]) -> Result<UploadData, StorageError> {
    let upload_id = CanonicalUploadId::new(name)?;
    let path = format!("/{name}.bin").into_bytes();
    let binding = upload_binding_digest(
        b"repository",
        &path,
        b"file",
        payload.len() as u64,
        Some(*blake3::hash(payload).as_bytes()),
    )?;
    let chunk = BlobChunkV1 {
        bytes: Bytes::from_static(payload),
    };
    let (chunk_id, chunk_bytes) = chunk.encode()?;
    let part = UploadPartV1 {
        upload_id: upload_id.clone(),
        part_number: 0,
        byte_offset: 0,
        declared_part_len: payload.len() as u64,
        ordered_chunks: vec![BlobChunkRefV1 {
            chunk_object_id: chunk_id,
            declared_len: payload.len() as u64,
        }],
        part_digest: *blake3::hash(payload).as_bytes(),
    };
    let (part_id, part_bytes) = part.encode()?;
    let initial = empty_receipt_tree()?;
    let mut arena = initial.objects;
    arena.insert(chunk_id, chunk_bytes)?;
    arena.insert(part_id, part_bytes)?;
    let receipt = insert_receipt_part(initial.root, part_id, &part, load_from(&arena))?;
    let progress = UploadProgressV1 {
        upload_id: upload_id.clone(),
        binding_digest: binding,
        receipt_tree_root: receipt.root.object_id,
        completed_part_count: receipt.root.completed_part_count,
        received_bytes: receipt.root.received_bytes,
        contiguous_prefix_bytes: receipt.root.contiguous_prefix_bytes,
    };
    let (progress_id, _) = progress.encode()?;
    let selector = UploadSelectorV1 {
        upload_id: upload_id.clone(),
        binding_digest: binding,
        progress_object_id: progress_id,
        selector_generation: 1,
    };
    Ok(UploadData {
        upload_id,
        path,
        chunk,
        chunk_id,
        part,
        receipt,
        progress,
        progress_id,
        selector,
    })
}

fn stage_upload(
    publication: &mut PreparedPublication,
    upload: &UploadData,
) -> Result<(), StorageError> {
    publication.publish_new_upload(
        std::slice::from_ref(&upload.chunk),
        std::slice::from_ref(&upload.part),
        upload.receipt.clone(),
        &upload.progress,
        &upload.selector,
    )
}

async fn stage_new_upload<S: Storage>(
    storage: &S,
    seed: &SeedData,
    upload: &UploadData,
) -> Result<(), StorageError> {
    let view = open_coherent_view(storage, seed.branch_id).await?;
    let mut publication = PreparedPublication::from_global_epoch(&view)?;
    stage_upload(&mut publication, upload)?;
    drop(view);
    publication.commit(storage).await
}

async fn load_upload_selector<R: StorageAdapterRead>(
    view: &CoherentView<R>,
    upload: &UploadData,
) -> Result<Bytes, StorageError> {
    let keys = [Key(upload_selector_key(&upload.upload_id)?)];
    let loaded = view
        .read()
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    match loaded.values.as_slice() {
        [Some(crate::storage::ProjectedValue::FullValue(bytes))] => Ok(bytes.clone()),
        _ => Err(StorageError::Corruption(
            "oracle upload selector is absent".to_owned(),
        )),
    }
}

async fn publish_untracked_manifest<S: Storage>(
    storage: &S,
    seed: &SeedData,
    primary_key: &str,
    manifest: &BlobManifestV1,
    chunks: &[BlobChunkV1],
) -> Result<ObjectId, StorageError> {
    let view = open_coherent_view(storage, seed.branch_id).await?;
    let (manifest_id, _) = manifest.encode()?;
    let roots = [manifest_id];
    let entity_pk = EntityPk::single(primary_key);
    let mut publication = PreparedPublication::from_global_epoch(&view)?;
    for chunk in chunks {
        publication.stage_blob_chunk(chunk)?;
    }
    publication.stage_blob_manifest(manifest)?;
    publication.put_untracked_row(
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
    )?;
    drop(view);
    publication.commit(storage).await?;
    Ok(manifest_id)
}

async fn delete_untracked<S: Storage>(
    storage: &S,
    seed: &SeedData,
    primary_key: &str,
) -> Result<(), StorageError> {
    let view = open_coherent_view(storage, seed.branch_id).await?;
    let entity_pk = EntityPk::single(primary_key);
    let mut publication = PreparedPublication::from_global_epoch(&view)?;
    publication.delete_untracked_row(
        seed.branch_id,
        StateKeyRef {
            schema_key: "app.untracked",
            file_id: None,
            entity_pk: &entity_pk,
        },
    )?;
    drop(view);
    publication.commit(storage).await
}

async fn run_state_catalog<S: Storage>(storage: &S) -> Result<String, String> {
    let seed = build_seed(&[], ImmutableObjectSet::default()).map_err(|e| e.to_string())?;
    seed_storage(storage, &seed)
        .await
        .map_err(|e| e.to_string())?;
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let fallback = state_point(&view, &state_key("fallback"), false)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "global fallback row is absent".to_owned())?;
    if fallback.source != StateSource::Global
        || !matches!(fallback.value.cell, StateCell::Value(ref value) if value == "global")
    {
        return Err("global fallback precedence changed".to_owned());
    }
    let shadow = state_point(&view, &state_key("shadow"), false)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "branch override is absent".to_owned())?;
    if shadow.source != StateSource::Branch
        || !matches!(shadow.value.cell, StateCell::Value(ref value) if value == "local-shadow")
    {
        return Err("branch value did not override global value".to_owned());
    }
    if state_point(&view, &state_key("hidden"), false)
        .await
        .map_err(|e| e.to_string())?
        .is_some()
        || !matches!(
            state_point(&view, &state_key("hidden"), true)
                .await
                .map_err(|e| e.to_string())?
                .map(|row| row.value.cell),
            Some(StateCell::Tombstone)
        )
        || !matches!(
            state_point(&view, &state_key("null"), false)
                .await
                .map_err(|e| e.to_string())?
                .map(|row| row.value.cell),
            Some(StateCell::Null)
        )
    {
        return Err("NULL/tombstone semantics changed".to_owned());
    }
    let rows = state_range(&view, None, None, None, false)
        .await
        .map_err(|e| e.to_string())?;
    if rows.len() != 4
        || rows
            .windows(2)
            .any(|pair| pair[0].encoded_key >= pair[1].encoded_key)
    {
        return Err("coherent ordered range merge changed".to_owned());
    }
    let old_page = page_changes(&view, None, 1)
        .await
        .map_err(|e| e.to_string())?;
    let old_resume = old_page
        .resume_token
        .ok_or_else(|| "one-row change page did not return resume token".to_owned())?;
    let (key, value) = state_entry("shadow", StateCellRef::Value("updated"), 0x40, &[])
        .map_err(|e| e.to_string())?;
    let state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![StateTreeMutation::update(key, value)],
        view.read(),
    )
    .await
    .map_err(|e| e.to_string())?;
    if state_edit.copied_nodes() == 0 {
        return Err("path-copy update copied no authenticated nodes".to_owned());
    }
    let transition = branch_transition(&view, state_edit, 0x40)
        .await
        .map_err(|e| e.to_string())?;
    let mut publication =
        PreparedPublication::from_branch_view(&view).map_err(|e| e.to_string())?;
    publication
        .publish_state_transition(&view, transition)
        .await
        .map_err(|e| e.to_string())?;
    drop(view);
    publication
        .commit(storage)
        .await
        .map_err(|e| e.to_string())?;

    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    if !matches!(
        state_point(&view, &state_key("shadow"), false)
            .await
            .map_err(|e| e.to_string())?
            .map(|row| row.value.cell),
        Some(StateCell::Value(value)) if value == "updated"
    ) || !matches!(
        page_changes(&view, Some(&old_resume), 1).await,
        Err(StorageError::InvalidCursor)
    ) {
        return Err("path-copy state or view-bound resume changed".to_owned());
    }
    let commits = page_commits(&view, None, 8)
        .await
        .map_err(|e| e.to_string())?;
    let changes = page_changes(&view, None, 8)
        .await
        .map_err(|e| e.to_string())?;
    if commits.entries.len() != 2
        || changes.entries.len() != 3
        || changes.entries[0].0 != seed.semantic_change_id
        || changes.entries[1].0 != seed.ref_change_id
    {
        return Err("unified catalog exact ordering/back-edges changed".to_owned());
    }
    Ok("state_catalog:pass".to_owned())
}

async fn run_upload_gc<S: Storage>(storage: &S) -> Result<String, String> {
    let seed = build_seed(&[], ImmutableObjectSet::default()).map_err(|e| e.to_string())?;
    seed_storage(storage, &seed)
        .await
        .map_err(|e| e.to_string())?;
    let completed = make_upload("completed", b"data").map_err(|e| e.to_string())?;
    stage_new_upload(storage, &seed, &completed)
        .await
        .map_err(|e| e.to_string())?;
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let completion = prepare_upload_completion(
        &view,
        &completed.upload_id,
        UploadBindingRef {
            repository_identity: b"repository",
            path: &completed.path,
            payload_domain: b"file",
            declared_total_size: 4,
            declared_final_hash: Some(*blake3::hash(b"data").as_bytes()),
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    let manifest = BlobManifestV1 {
        logical_bytes: 4,
        ordered_chunks: completed.part.ordered_chunks.clone(),
        content_digest: *blake3::hash(b"data").as_bytes(),
    };
    let (manifest_id, _) = manifest.encode().map_err(|e| e.to_string())?;
    let (key, value) = state_entry(
        "completed",
        StateCellRef::Value("blob"),
        0x60,
        &[manifest_id],
    )
    .map_err(|e| e.to_string())?;
    let state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![StateTreeMutation::insert(key.clone(), value)],
        view.read(),
    )
    .await
    .map_err(|e| e.to_string())?;
    let transition = branch_transition(&view, state_edit, 0x60)
        .await
        .map_err(|e| e.to_string())?;
    let mut publication =
        PreparedPublication::from_branch_view(&view).map_err(|e| e.to_string())?;
    publication
        .publish_completed_upload(&view, completion, transition)
        .await
        .map_err(|e| e.to_string())?;
    drop(view);
    publication
        .commit(storage)
        .await
        .map_err(|e| e.to_string())?;

    let aborted = make_upload("aborted", b"gone").map_err(|e| e.to_string())?;
    stage_new_upload(storage, &seed, &aborted)
        .await
        .map_err(|e| e.to_string())?;
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let raw = load_upload_selector(&view, &aborted)
        .await
        .map_err(|e| e.to_string())?;
    let mut abort = PreparedPublication::from_global_epoch(&view).map_err(|e| e.to_string())?;
    abort
        .abort_upload(&aborted.selector, raw)
        .map_err(|e| e.to_string())?;
    drop(view);
    abort.commit(storage).await.map_err(|e| e.to_string())?;
    sweep(storage).await.map_err(|e| e.to_string())?;
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let row = state_point(&view, &key, false)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "completed upload state root disappeared".to_owned())?;
    drop(view);
    if row.value.blob_manifest_object_ids != vec![manifest_id]
        || !object_present(
            storage,
            seed.branch_id,
            manifest_id,
            ExpectedObject::Manifest,
        )
        .await
        .map_err(|e| e.to_string())?
        || !object_present(
            storage,
            seed.branch_id,
            completed.chunk_id,
            ExpectedObject::Chunk,
        )
        .await
        .map_err(|e| e.to_string())?
        || object_present(
            storage,
            seed.branch_id,
            aborted.progress_id,
            ExpectedObject::Progress,
        )
        .await
        .map_err(|e| e.to_string())?
        || object_present(
            storage,
            seed.branch_id,
            aborted.chunk_id,
            ExpectedObject::Chunk,
        )
        .await
        .map_err(|e| e.to_string())?
    {
        return Err("upload completion/abort reachability changed".to_owned());
    }
    Ok("upload_gc:pass".to_owned())
}

async fn run_shared_final<S: Storage>(storage: &S) -> Result<String, String> {
    let seed = build_seed(&[], ImmutableObjectSet::default()).map_err(|e| e.to_string())?;
    seed_storage(storage, &seed)
        .await
        .map_err(|e| e.to_string())?;
    let shared = BlobChunkV1 {
        bytes: Bytes::from_static(b"shared"),
    };
    let one = BlobChunkV1 {
        bytes: Bytes::from_static(b"one"),
    };
    let two = BlobChunkV1 {
        bytes: Bytes::from_static(b"two"),
    };
    let (shared_id, _) = shared.encode().map_err(|e| e.to_string())?;
    let (one_id, _) = one.encode().map_err(|e| e.to_string())?;
    let (two_id, _) = two.encode().map_err(|e| e.to_string())?;
    let first = BlobManifestV1 {
        logical_bytes: 9,
        ordered_chunks: vec![
            BlobChunkRefV1 {
                chunk_object_id: one_id,
                declared_len: 3,
            },
            BlobChunkRefV1 {
                chunk_object_id: shared_id,
                declared_len: 6,
            },
        ],
        content_digest: *blake3::hash(b"oneshared").as_bytes(),
    };
    let second = BlobManifestV1 {
        logical_bytes: 9,
        ordered_chunks: vec![
            BlobChunkRefV1 {
                chunk_object_id: two_id,
                declared_len: 3,
            },
            BlobChunkRefV1 {
                chunk_object_id: shared_id,
                declared_len: 6,
            },
        ],
        content_digest: *blake3::hash(b"twoshared").as_bytes(),
    };
    let first_id =
        publish_untracked_manifest(storage, &seed, "one", &first, &[one, shared.clone()])
            .await
            .map_err(|e| e.to_string())?;
    let second_id = publish_untracked_manifest(storage, &seed, "two", &second, &[two, shared])
        .await
        .map_err(|e| e.to_string())?;
    delete_untracked(storage, &seed, "one")
        .await
        .map_err(|e| e.to_string())?;
    sweep(storage).await.map_err(|e| e.to_string())?;
    if object_present(storage, seed.branch_id, first_id, ExpectedObject::Manifest)
        .await
        .map_err(|e| e.to_string())?
        || !object_present(storage, seed.branch_id, second_id, ExpectedObject::Manifest)
            .await
            .map_err(|e| e.to_string())?
        || !object_present(storage, seed.branch_id, shared_id, ExpectedObject::Chunk)
            .await
            .map_err(|e| e.to_string())?
    {
        return Err("shared chunk was not retained until its final reference".to_owned());
    }
    delete_untracked(storage, &seed, "two")
        .await
        .map_err(|e| e.to_string())?;
    sweep(storage).await.map_err(|e| e.to_string())?;
    if object_present(storage, seed.branch_id, second_id, ExpectedObject::Manifest)
        .await
        .map_err(|e| e.to_string())?
        || object_present(storage, seed.branch_id, shared_id, ExpectedObject::Chunk)
            .await
            .map_err(|e| e.to_string())?
    {
        return Err("final-reference reclamation left blob objects live".to_owned());
    }
    Ok("shared_final:pass".to_owned())
}

async fn load_snapshot_selector<R: StorageAdapterRead>(
    view: &CoherentView<R>,
    role: SnapshotRole,
    id: SnapshotSelectorId,
) -> Result<(SnapshotSelectorV1, Bytes), StorageError> {
    let keys = [Key(snapshot_selector_key(role, id))];
    let loaded = view
        .read()
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    let raw = match loaded.values.as_slice() {
        [Some(crate::storage::ProjectedValue::FullValue(bytes))] => bytes.clone(),
        _ => {
            return Err(StorageError::Corruption(
                "snapshot selector is absent".to_owned(),
            ));
        }
    };
    Ok((SnapshotSelectorV1::decode(&raw)?, raw))
}

async fn release_pin<S: Storage>(
    storage: &S,
    seed: &SeedData,
    role: SnapshotRole,
    id: SnapshotSelectorId,
) -> Result<(), StorageError> {
    let view = open_coherent_view(storage, seed.branch_id).await?;
    let (selector, raw) = load_snapshot_selector(&view, role, id).await?;
    let commits =
        retire_commit_catalog_entries(view.repository_root().commit_catalog_root, &[], view.read())
            .await?;
    let changes =
        retire_change_catalog_entries(view.repository_root().change_catalog_root, &[], view.read())
            .await?;
    let repository = RepositoryRootV1 {
        commit_catalog_root: commits.root,
        change_catalog_root: changes.root,
        ..view.repository_root()
    };
    let mut publication = PreparedPublication::from_global_epoch(&view)?;
    publication.release_snapshot_pin_with_catalog_retirement(
        &view, selector, raw, commits, changes, repository,
    )?;
    drop(view);
    publication.commit(storage).await
}

async fn run_retained_races<S: Storage>(storage: &S) -> Result<String, String> {
    let seed = build_seed(&[], ImmutableObjectSet::default()).map_err(|e| e.to_string())?;
    seed_storage(storage, &seed)
        .await
        .map_err(|e| e.to_string())?;
    let ids = [
        (
            SnapshotRole::Checkpoint,
            SnapshotSelectorId::from_bytes(raw_id(0x90)),
        ),
        (
            SnapshotRole::Recovery,
            SnapshotSelectorId::from_bytes(raw_id(0x91)),
        ),
        (
            SnapshotRole::Undo,
            SnapshotSelectorId::from_bytes(raw_id(0x92)),
        ),
    ];
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let mut publication =
        PreparedPublication::from_global_epoch(&view).map_err(|e| e.to_string())?;
    let mut target = ObjectId::ZERO;
    for (role, id) in ids {
        target = publication
            .publish_current_snapshot_pin(&view, role, id, SelectorExpectation::Absent)
            .map_err(|e| e.to_string())?;
    }
    drop(view);
    publication
        .commit(storage)
        .await
        .map_err(|e| e.to_string())?;
    let old_view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let old_root = old_view.repository_root().change_catalog_root;
    let old_resume = old_view.bind_resume_key(old_root, seed.ref_change_id.as_bytes());
    for (role, id) in ids {
        release_pin(storage, &seed, role, id)
            .await
            .map_err(|e| e.to_string())?;
    }
    sweep(storage).await.map_err(|e| e.to_string())?;
    if object_present(
        storage,
        seed.branch_id,
        target,
        ExpectedObject::SnapshotTarget,
    )
    .await
    .map_err(|e| e.to_string())?
        || old_view.load_object_bytes(target).await.is_err()
        || old_view.validate_resume_key(old_root, &old_resume).is_err()
    {
        return Err("reader-held old root or final pin release changed".to_owned());
    }
    let new_view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    if !matches!(
        new_view.validate_resume_key(old_root, &old_resume),
        Err(StorageError::InvalidCursor)
    ) {
        return Err("new coherent view accepted an old-view resume token".to_owned());
    }
    drop(new_view);
    drop(old_view);

    let upload = make_upload("race", b"race").map_err(|e| e.to_string())?;
    let publish_view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let mut stale =
        PreparedPublication::from_global_epoch(&publish_view).map_err(|e| e.to_string())?;
    stage_upload(&mut stale, &upload).map_err(|e| e.to_string())?;
    drop(publish_view);
    sweep(storage).await.map_err(|e| e.to_string())?;
    if !matches!(
        stale.commit(storage).await,
        Err(StorageError::PreconditionFailed(_))
    ) {
        return Err("GC-first ordering accepted stale publication".to_owned());
    }
    stage_new_upload(storage, &seed, &upload)
        .await
        .map_err(|e| e.to_string())?;
    sweep(storage).await.map_err(|e| e.to_string())?;
    if !object_present(
        storage,
        seed.branch_id,
        upload.progress_id,
        ExpectedObject::Progress,
    )
    .await
    .map_err(|e| e.to_string())?
    {
        return Err("publication-first receipt was not in the GC root snapshot".to_owned());
    }
    Ok("retained_races:pass".to_owned())
}

async fn run_corruption<S: Storage>(storage: &S) -> Result<String, String> {
    let chunk = BlobChunkV1 {
        bytes: Bytes::from_static(b"authenticated"),
    };
    let (chunk_id, _) = chunk.encode().map_err(|e| e.to_string())?;
    let manifest = BlobManifestV1 {
        logical_bytes: 13,
        ordered_chunks: vec![BlobChunkRefV1 {
            chunk_object_id: chunk_id,
            declared_len: 13,
        }],
        content_digest: *blake3::hash(b"authenticated").as_bytes(),
    };
    let (manifest_id, manifest_bytes) = manifest.encode().map_err(|e| e.to_string())?;
    let mut extras = ImmutableObjectSet::default();
    extras
        .insert(chunk_id, chunk.encode().map_err(|e| e.to_string())?.1)
        .map_err(|e| e.to_string())?;
    extras
        .insert(manifest_id, manifest_bytes)
        .map_err(|e| e.to_string())?;
    let seed = build_seed(&[manifest_id], extras).map_err(|e| e.to_string())?;
    seed_storage(storage, &seed)
        .await
        .map_err(|e| e.to_string())?;
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let bytes = view
        .load_object_bytes(chunk_id)
        .await
        .map_err(|e| e.to_string())?;
    BlobChunkV1::decode(chunk_id, &bytes).map_err(|e| e.to_string())?;
    let mut corrupted = bytes.to_vec();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 1;
    if BlobChunkV1::decode(chunk_id, &corrupted).is_ok()
        || ChangeObjectV1::decode(chunk_id, &bytes).is_ok()
    {
        return Err("content hash/domain corruption was accepted".to_owned());
    }
    let mut raw_branch = view.raw_branch_selector().to_vec();
    raw_branch[0] ^= 1;
    if BranchSelectorV1::decode(&raw_branch).is_ok() {
        return Err("selector corruption was accepted".to_owned());
    }
    let key = state_key("root");
    if state_point(&view, &key, false)
        .await
        .map_err(|e| e.to_string())?
        .is_none()
    {
        return Err("corruption probe mutated the selected graph".to_owned());
    }
    Ok("corruption:fail_closed".to_owned())
}

pub(crate) async fn run<S: Storage>(storage: &S, case: &str) -> Result<String, String> {
    match case {
        "state_catalog" => run_state_catalog(storage).await,
        "upload_gc" => run_upload_gc(storage).await,
        "shared_final" => run_shared_final(storage).await,
        "retained_races" => run_retained_races(storage).await,
        "corruption" => run_corruption(storage).await,
        other => Err(format!("unknown ForkTree application oracle case {other}")),
    }
}

pub(crate) async fn verify_reopen<S: Storage>(storage: &S, case: &str) -> Result<String, String> {
    let seed = build_seed(&[], ImmutableObjectSet::default()).map_err(|e| e.to_string())?;
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| format!("cold reopen {case}: {e}"))?;
    match case {
        "state_catalog" => {
            if !matches!(
                state_point(&view, &state_key("shadow"), false)
                    .await
                    .map_err(|e| e.to_string())?
                    .map(|row| row.value.cell),
                Some(StateCell::Value(value)) if value == "updated"
            ) {
                return Err("updated state did not survive cold reopen".to_owned());
            }
        }
        "upload_gc" => {
            let upload = make_upload("completed", b"data").map_err(|e| e.to_string())?;
            if !object_present(
                storage,
                seed.branch_id,
                upload.chunk_id,
                ExpectedObject::Chunk,
            )
            .await
            .map_err(|e| e.to_string())?
            {
                return Err("completed upload chunk disappeared after reopen".to_owned());
            }
        }
        "shared_final" => {}
        "retained_races" => {
            let upload = make_upload("race", b"race").map_err(|e| e.to_string())?;
            if !object_present(
                storage,
                seed.branch_id,
                upload.progress_id,
                ExpectedObject::Progress,
            )
            .await
            .map_err(|e| e.to_string())?
            {
                return Err("retried receipt disappeared after reopen".to_owned());
            }
        }
        "corruption" => {
            let manifest_id = view.branch_snapshot().local_state_root;
            if manifest_id == ObjectId::ZERO {
                return Err("corruption fixture reopened with zero state root".to_owned());
            }
        }
        other => return Err(format!("unknown ForkTree application oracle case {other}")),
    }
    let _ = branch_selector_key(seed.branch_id);
    Ok(format!("{case}:cold_reopen_pass"))
}

#[cfg(test)]
mod tests {
    use crate::storage::Memory;

    const CASES: &[&str] = &[
        "state_catalog",
        "upload_gc",
        "shared_final",
        "retained_races",
        "corruption",
    ];

    #[tokio::test]
    async fn typed_application_oracle_memory() {
        for case in CASES {
            let storage = Memory::new();
            let result = super::run(&storage, case)
                .await
                .unwrap_or_else(|error| panic!("Memory {case}: {error}"));
            eprintln!("backend=memory case={case} phase=run result={result}");
            let result = super::verify_reopen(&storage, case)
                .await
                .unwrap_or_else(|error| panic!("Memory reopen {case}: {error}"));
            eprintln!("backend=memory case={case} phase=reopen result={result}");
        }
    }
}
