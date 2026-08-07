//! Test-only dual-adapter adversarial oracle for the unwired ForkTree stage.

use bytes::Bytes;

use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::storage::{
    CoreProjection, GetManyRequest, GetOptions, Key, PutBatch, PutEntry, ReadOptions, Storage,
    StorageError, StorageRead, StorageWrite, StoredValue, WriteOptions,
};

use super::model::{
    branch_selector_key, global_selector_key, snapshot_selector_key, upload_binding_digest,
    upload_selector_key,
};
use super::serving::{put_change_catalog_entries, put_commit_catalog_entries};
use super::tree::{
    ImmutableObjectSet, build_change_catalog, build_commit_catalog, build_retention_tree,
    build_state_tree, empty_receipt_tree, insert_receipt_part,
};
use super::view::SELECTOR_SPACE;
use super::{
    BlobChunkRefV1, BlobChunkV1, BlobManifestV1, BranchSelectorV1, BranchSnapshotV1,
    BranchStateTransition, CanonicalBranchId, CanonicalUploadId, ChangeCatalogEntry,
    ChangeCatalogOwner, ChangeId, ChangeObjectV1, CoherentView, CommitCatalogEntry, CommitId,
    CommitObjectV1, GlobalSelectorV1, OBJECT_SPACE, ObjectId, PreparedPublication, ReceiptTreeEdit,
    RepositoryRootV1, SelectorExpectation, SnapshotRole, SnapshotSelectorId, SnapshotSelectorV1,
    StateCellRef, StateKeyRef, StateTreeMutation, StateValueRef, UntrackedValueRef,
    UploadBindingRef, UploadPartV1, UploadProgressV1, UploadSelectorV1, discover_sweep_plan,
    edit_state_tree, encode_state_key, encode_state_value, open_coherent_view,
    prepare_upload_completion, state_point,
};
use crate::storage_adapter::StorageAdapterRead;

fn raw_id(byte: u8) -> [u8; 16] {
    [byte; 16]
}

fn public_commit_id(byte: u8) -> crate::changelog::CommitId {
    crate::changelog::CommitId::new(uuid::Uuid::from_bytes(raw_id(byte)))
}

fn public_change_id(byte: u8) -> crate::changelog::ChangeId {
    crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(raw_id(byte)))
}

fn state_entry(primary_key: &str, commit_byte: u8, manifests: &[ObjectId]) -> (Vec<u8>, Vec<u8>) {
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
        cell: StateCellRef::Value("blob"),
        metadata: None,
        origin_key: None,
        blob_manifest_object_ids: manifests,
    })
    .expect("oracle state value");
    (key, value)
}

#[derive(Clone)]
struct SeedData {
    objects: ImmutableObjectSet,
    branch_id: CanonicalBranchId,
    merge_parent_object_id: ObjectId,
    global_selector: GlobalSelectorV1,
    branch_selector: BranchSelectorV1,
}

fn build_seed(rooted_manifests: &[ObjectId], extras: ImmutableObjectSet) -> SeedData {
    let branch_id = CanonicalBranchId::from_bytes(raw_id(0x11));
    let commit_id = CommitId::from_bytes(raw_id(0x20));
    let semantic_change_id = ChangeId::from_bytes(raw_id(0x30));
    let ref_change_id = ChangeId::from_bytes(raw_id(0x31));
    let mut objects = extras;

    let global_state = build_state_tree(&[]).expect("oracle global state");
    let global_state_root = global_state.root.object_id;
    objects
        .extend(global_state.objects)
        .expect("global objects");

    let local_state = build_state_tree(&[state_entry("root", 0x20, rooted_manifests)])
        .expect("oracle local state");
    let local_state_root = local_state.root.object_id;
    objects.extend(local_state.objects).expect("local objects");

    let retention = build_retention_tree(&[]).expect("oracle retention");
    let retention_root = retention.root.object_id;
    objects
        .extend(retention.objects)
        .expect("retention objects");

    let semantic_change = ChangeObjectV1::Semantic {
        change_id: semantic_change_id,
        payload: b"oracle-semantic-change".to_vec(),
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
        member_change_object_ids: vec![semantic_change_object_id],
        global_state_root,
        local_state_root,
        metadata: b"oracle-commit".to_vec(),
    };
    let (commit_object_id, commit_bytes) = commit.encode().expect("commit");
    objects
        .insert(commit_object_id, commit_bytes)
        .expect("commit object");
    let merge_parent = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(0x22)),
        generation: 2,
        parent_commit_object_ids: Vec::new(),
        member_change_object_ids: Vec::new(),
        global_state_root,
        local_state_root,
        metadata: b"oracle-merge-parent".to_vec(),
    };
    let (merge_parent_object_id, merge_parent_bytes) = merge_parent.encode().expect("merge parent");
    objects
        .insert(merge_parent_object_id, merge_parent_bytes)
        .expect("merge parent object");
    let ref_change = ChangeObjectV1::BranchRef {
        change_id: ref_change_id,
        branch_id,
        before_semantic_head_commit_object_id: None,
        after_semantic_head_commit_object_id: Some(commit_object_id),
        previous_ref_change_object_id: None,
        payload: b"oracle-create-main".to_vec(),
    };
    let (ref_change_object_id, ref_change_bytes) = ref_change.encode().expect("ref change");
    objects
        .insert(ref_change_object_id, ref_change_bytes)
        .expect("ref-change object");

    let commit_catalog = build_commit_catalog(&[
        (commit_id, CommitCatalogEntry { commit_object_id }),
        (
            merge_parent.commit_id,
            CommitCatalogEntry {
                commit_object_id: merge_parent_object_id,
            },
        ),
    ])
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

    let repository = RepositoryRootV1 {
        global_state_root,
        commit_catalog_root,
        change_catalog_root,
        retention_policy_root: retention_root,
    };
    let (repository_root_id, repository_bytes) = repository.encode().expect("repository");
    objects
        .insert(repository_root_id, repository_bytes)
        .expect("repository object");
    let snapshot = BranchSnapshotV1 {
        branch_id,
        local_state_root,
        semantic_head_commit_object_id: commit_object_id,
        latest_ref_change_object_id: Some(ref_change_object_id),
        historical_global_state_root: global_state_root,
    };
    let (branch_snapshot_id, snapshot_bytes) = snapshot.encode().expect("snapshot");
    objects
        .insert(branch_snapshot_id, snapshot_bytes)
        .expect("snapshot object");

    SeedData {
        objects,
        branch_id,
        merge_parent_object_id,
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
    }
}

async fn seed_storage<S: Storage>(storage: &S, seed: &SeedData) -> Result<(), StorageError> {
    let mut write = storage.begin_write(WriteOptions::default()).await?;
    write
        .put_many(
            OBJECT_SPACE,
            PutBatch {
                entries: seed
                    .objects
                    .iter()
                    .map(|(id, bytes)| PutEntry {
                        key: Key(Bytes::copy_from_slice(id.as_bytes())),
                        value: StoredValue {
                            bytes: bytes.clone(),
                        },
                    })
                    .collect(),
            },
        )
        .await?;
    write
        .put_many(
            SELECTOR_SPACE,
            PutBatch {
                entries: vec![
                    PutEntry {
                        key: Key(global_selector_key()),
                        value: StoredValue {
                            bytes: seed.global_selector.encode()?,
                        },
                    },
                    PutEntry {
                        key: Key(branch_selector_key(seed.branch_id)),
                        value: StoredValue {
                            bytes: seed.branch_selector.encode()?,
                        },
                    },
                ],
            },
        )
        .await?;
    write.commit().await.map(|_| ())
}

async fn object_present<S: Storage>(storage: &S, id: ObjectId) -> Result<bool, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    let keys = [Key(Bytes::copy_from_slice(id.as_bytes()))];
    Ok(read
        .get_many(&[GetManyRequest {
            space: OBJECT_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?
        .values[0]
        .is_some())
}

async fn sweep<S: Storage>(storage: &S, branch_id: CanonicalBranchId) -> Result<(), StorageError> {
    let view = open_coherent_view(storage, branch_id).await?;
    let plan = discover_sweep_plan(&view).await?;
    let mut publication = PreparedPublication::from_global_epoch(&view)?;
    publication.apply_sweep_plan(plan)?;
    drop(view);
    publication.commit(storage).await
}

async fn branch_transition<R: StorageAdapterRead>(
    view: &CoherentView<R>,
    state_edit: super::serving::StateTreeEdit,
    identity: u8,
) -> Result<BranchStateTransition, StorageError> {
    branch_transition_with_parents(view, state_edit, identity, &[]).await
}

async fn branch_transition_with_parents<R: StorageAdapterRead>(
    view: &CoherentView<R>,
    state_edit: super::serving::StateTreeEdit,
    identity: u8,
    additional_parents: &[ObjectId],
) -> Result<BranchStateTransition, StorageError> {
    let mut parent_commit_object_ids = vec![view.branch_snapshot().semantic_head_commit_object_id];
    parent_commit_object_ids.extend_from_slice(additional_parents);
    let semantic_commit = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(identity)),
        generation: u64::from(identity),
        parent_commit_object_ids,
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
    let local_state_root = state_edit.root;
    Ok(BranchStateTransition {
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

async fn complete_upload<S: Storage>(
    storage: &S,
    seed: &SeedData,
    upload: &UploadData,
    identity: u8,
    primary_key: &str,
) -> Result<(ObjectId, Vec<u8>), StorageError> {
    let view = open_coherent_view(storage, seed.branch_id).await?;
    let payload = upload.chunk.bytes.as_ref();
    let completion = prepare_upload_completion(
        &view,
        &upload.upload_id,
        UploadBindingRef {
            repository_identity: b"repository",
            path: &upload.path,
            payload_domain: b"file",
            declared_total_size: payload.len() as u64,
            declared_final_hash: Some(*blake3::hash(payload).as_bytes()),
        },
    )
    .await?;
    let manifest = BlobManifestV1 {
        logical_bytes: payload.len() as u64,
        ordered_chunks: upload.part.ordered_chunks.clone(),
        content_digest: *blake3::hash(payload).as_bytes(),
    };
    let (manifest_id, _) = manifest.encode()?;
    let (key, value) = state_entry(primary_key, identity, &[manifest_id]);
    let state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![StateTreeMutation::insert(key.clone(), value)],
        view.read(),
    )
    .await?;
    let transition = branch_transition(&view, state_edit, identity).await?;
    let mut publication = PreparedPublication::from_branch_view(&view)?;
    let published = publication
        .publish_completed_upload(&view, completion, transition)
        .await?;
    if published != manifest_id {
        return Err(StorageError::Corruption(
            "oracle upload completion changed manifest identity".to_string(),
        ));
    }
    drop(view);
    publication.commit(storage).await?;
    Ok((manifest_id, key))
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

async fn abort_upload<S: Storage>(
    storage: &S,
    seed: &SeedData,
    upload: &UploadData,
) -> Result<(), StorageError> {
    let view = open_coherent_view(storage, seed.branch_id).await?;
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
    let raw = match loaded.values.as_slice() {
        [Some(crate::storage::ProjectedValue::FullValue(bytes))] => bytes.clone(),
        _ => {
            return Err(StorageError::Corruption(
                "oracle expected upload selector".to_string(),
            ));
        }
    };
    let mut publication = PreparedPublication::from_global_epoch(&view)?;
    publication.abort_upload(&upload.selector, raw)?;
    drop(view);
    publication.commit(storage).await
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
    let entity_pk = EntityPk::single(primary_key);
    let roots = [manifest_id];
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

fn corruption_fixture(case: &str) -> Result<(SeedData, ObjectId), String> {
    let valid_chunk = BlobChunkV1 {
        bytes: Bytes::from_static(b"immutable-leaf"),
    };
    let (valid_chunk_id, valid_chunk_bytes) = valid_chunk.encode().map_err(|e| e.to_string())?;
    let semantic = ChangeObjectV1::Semantic {
        change_id: ChangeId::from_bytes(raw_id(0x66)),
        payload: b"wrong-domain".to_vec(),
    };
    let (wrong_domain_id, wrong_domain_bytes) = semantic.encode().map_err(|e| e.to_string())?;

    let mut extras = ImmutableObjectSet::default();
    let chunk_ref = match case {
        "corrupt_chunk" => {
            let mut corrupt = valid_chunk_bytes.to_vec();
            let last = corrupt.len() - 1;
            corrupt[last] ^= 1;
            extras
                .insert(valid_chunk_id, Bytes::from(corrupt))
                .map_err(|e| e.to_string())?;
            BlobChunkRefV1 {
                chunk_object_id: valid_chunk_id,
                declared_len: 14,
            }
        }
        "wrong_domain" => {
            extras
                .insert(wrong_domain_id, wrong_domain_bytes)
                .map_err(|e| e.to_string())?;
            BlobChunkRefV1 {
                chunk_object_id: wrong_domain_id,
                declared_len: 14,
            }
        }
        "edge_mismatch" => {
            extras
                .insert(valid_chunk_id, valid_chunk_bytes)
                .map_err(|e| e.to_string())?;
            BlobChunkRefV1 {
                chunk_object_id: valid_chunk_id,
                declared_len: 13,
            }
        }
        other => return Err(format!("unknown corruption case {other}")),
    };
    let manifest = BlobManifestV1 {
        logical_bytes: u64::from(chunk_ref.declared_len),
        ordered_chunks: vec![chunk_ref],
        content_digest: *blake3::hash(b"immutable-leaf").as_bytes(),
    };
    let (manifest_id, manifest_bytes) = manifest.encode().map_err(|e| e.to_string())?;
    extras
        .insert(manifest_id, manifest_bytes)
        .map_err(|e| e.to_string())?;
    Ok((build_seed(&[manifest_id], extras), manifest_id))
}

async fn run_corruption<S: Storage>(storage: &S, case: &str) -> Result<String, String> {
    let (seed, manifest_id) = corruption_fixture(case)?;
    seed_storage(storage, &seed)
        .await
        .map_err(|e| format!("seed {case}: {e}"))?;
    let error = match open_coherent_view(storage, seed.branch_id).await {
        Ok(view) => discover_sweep_plan(&view)
            .await
            .expect_err("corrupt rooted closure must fail closed"),
        Err(error) => error,
    };
    let failure = match error {
        StorageError::Corruption(message) => message,
        other => return Err(format!("{case}: expected corruption, got {other}")),
    };
    if !object_present(storage, manifest_id)
        .await
        .map_err(|e| e.to_string())?
    {
        return Err(format!("{case}: failed read mutated persisted fixture"));
    }
    Ok(format!("{case}:fail_closed:{failure}"))
}

async fn run_shared_identity<S: Storage>(storage: &S) -> Result<String, String> {
    let seed = build_seed(&[], ImmutableObjectSet::default());
    seed_storage(storage, &seed)
        .await
        .map_err(|e| e.to_string())?;
    let shared = BlobChunkV1 {
        bytes: Bytes::from_static(b"shared"),
    };
    let first_unique = BlobChunkV1 {
        bytes: Bytes::from_static(b"one"),
    };
    let second_unique = BlobChunkV1 {
        bytes: Bytes::from_static(b"two"),
    };
    let (shared_id, _) = shared.encode().map_err(|e| e.to_string())?;
    let (first_unique_id, _) = first_unique.encode().map_err(|e| e.to_string())?;
    let (second_unique_id, _) = second_unique.encode().map_err(|e| e.to_string())?;
    if first_unique_id == second_unique_id {
        return Err("same-length changed leaves aliased".to_string());
    }
    let shared_ref = BlobChunkRefV1 {
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
            shared_ref.clone(),
        ],
        content_digest: *blake3::hash(b"oneshared").as_bytes(),
    };
    let second = BlobManifestV1 {
        logical_bytes: 9,
        ordered_chunks: vec![
            BlobChunkRefV1 {
                chunk_object_id: second_unique_id,
                declared_len: 3,
            },
            shared_ref,
        ],
        content_digest: *blake3::hash(b"twoshared").as_bytes(),
    };
    let first_id = publish_untracked_manifest(
        storage,
        &seed,
        "one",
        &first,
        &[first_unique.clone(), shared.clone()],
    )
    .await
    .map_err(|e| e.to_string())?;
    let second_id = publish_untracked_manifest(
        storage,
        &seed,
        "two",
        &second,
        &[second_unique.clone(), shared.clone()],
    )
    .await
    .map_err(|e| e.to_string())?;
    if first_id == second_id {
        return Err("same-length changed manifests aliased".to_string());
    }
    sweep(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    delete_untracked(storage, &seed, "one")
        .await
        .map_err(|e| e.to_string())?;
    sweep(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    if object_present(storage, first_id)
        .await
        .map_err(|e| e.to_string())?
        || object_present(storage, first_unique_id)
            .await
            .map_err(|e| e.to_string())?
        || !object_present(storage, second_id)
            .await
            .map_err(|e| e.to_string())?
        || !object_present(storage, shared_id)
            .await
            .map_err(|e| e.to_string())?
    {
        return Err("shared chunk was not retained exclusively by the live root".to_string());
    }
    delete_untracked(storage, &seed, "two")
        .await
        .map_err(|e| e.to_string())?;
    sweep(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    if object_present(storage, second_id)
        .await
        .map_err(|e| e.to_string())?
        || object_present(storage, second_unique_id)
            .await
            .map_err(|e| e.to_string())?
        || object_present(storage, shared_id)
            .await
            .map_err(|e| e.to_string())?
    {
        return Err("final-reference reclamation left blob closure live".to_string());
    }
    Ok(format!(
        "shared_identity:pass:first={first_id}:second={second_id}:shared={shared_id}"
    ))
}

async fn run_upload_lifecycle<S: Storage>(storage: &S) -> Result<String, String> {
    let seed = build_seed(&[], ImmutableObjectSet::default());
    seed_storage(storage, &seed)
        .await
        .map_err(|e| e.to_string())?;

    let completed = make_upload("completed", b"data").map_err(|e| e.to_string())?;
    stage_new_upload(storage, &seed, &completed)
        .await
        .map_err(|e| e.to_string())?;
    let (manifest_id, key) = complete_upload(storage, &seed, &completed, 0x70, "completed")
        .await
        .map_err(|e| e.to_string())?;

    let aborted = make_upload("aborted", b"gone").map_err(|e| e.to_string())?;
    stage_new_upload(storage, &seed, &aborted)
        .await
        .map_err(|e| e.to_string())?;
    abort_upload(storage, &seed, &aborted)
        .await
        .map_err(|e| e.to_string())?;
    sweep(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;

    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let row = state_point(&view, &key, false)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "completed upload state row disappeared".to_string())?;
    if row.value.blob_manifest_object_ids != vec![manifest_id]
        || !object_present(storage, manifest_id)
            .await
            .map_err(|e| e.to_string())?
        || !object_present(storage, completed.chunk_id)
            .await
            .map_err(|e| e.to_string())?
        || object_present(storage, aborted.progress_id)
            .await
            .map_err(|e| e.to_string())?
        || object_present(storage, aborted.chunk_id)
            .await
            .map_err(|e| e.to_string())?
    {
        return Err("upload completion/abort reachability mismatch".to_string());
    }
    Ok(format!(
        "upload_lifecycle:pass:manifest={manifest_id}:chunk={}",
        completed.chunk_id
    ))
}

async fn run_epoch_races<S: Storage>(storage: &S) -> Result<String, String> {
    let seed = build_seed(&[], ImmutableObjectSet::default());
    seed_storage(storage, &seed)
        .await
        .map_err(|e| e.to_string())?;

    let first = make_upload("epoch-first", b"first").map_err(|e| e.to_string())?;
    let publish_view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let gc_view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let plan = discover_sweep_plan(&gc_view)
        .await
        .map_err(|e| e.to_string())?;
    let mut publish =
        PreparedPublication::from_global_epoch(&publish_view).map_err(|e| e.to_string())?;
    stage_upload(&mut publish, &first).map_err(|e| e.to_string())?;
    let mut stale_gc =
        PreparedPublication::from_global_epoch(&gc_view).map_err(|e| e.to_string())?;
    stale_gc.apply_sweep_plan(plan).map_err(|e| e.to_string())?;
    drop(publish_view);
    drop(gc_view);
    publish.commit(storage).await.map_err(|e| e.to_string())?;
    if !matches!(
        stale_gc.commit(storage).await,
        Err(StorageError::PreconditionFailed(_))
    ) {
        return Err("publication-first race accepted stale GC".to_string());
    }

    let second = make_upload("epoch-second", b"second").map_err(|e| e.to_string())?;
    let publish_view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let gc_view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let mut stale_publish =
        PreparedPublication::from_global_epoch(&publish_view).map_err(|e| e.to_string())?;
    stage_upload(&mut stale_publish, &second).map_err(|e| e.to_string())?;
    let plan = discover_sweep_plan(&gc_view)
        .await
        .map_err(|e| e.to_string())?;
    let mut gc = PreparedPublication::from_global_epoch(&gc_view).map_err(|e| e.to_string())?;
    gc.apply_sweep_plan(plan).map_err(|e| e.to_string())?;
    drop(publish_view);
    drop(gc_view);
    gc.commit(storage).await.map_err(|e| e.to_string())?;
    if !matches!(
        stale_publish.commit(storage).await,
        Err(StorageError::PreconditionFailed(_))
    ) {
        return Err("GC-first race accepted stale publication".to_string());
    }
    if object_present(storage, second.progress_id)
        .await
        .map_err(|e| e.to_string())?
        || object_present(storage, second.chunk_id)
            .await
            .map_err(|e| e.to_string())?
    {
        return Err("failed stale publication left partial immutable objects".to_string());
    }
    stage_new_upload(storage, &seed, &second)
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!(
        "epoch_races:pass:first={}:second={}",
        first.progress_id, second.progress_id
    ))
}

async fn run_checkpoint_retention<S: Storage>(storage: &S) -> Result<String, String> {
    let seed = build_seed(&[], ImmutableObjectSet::default());
    seed_storage(storage, &seed)
        .await
        .map_err(|e| e.to_string())?;
    let upload = make_upload("retained", b"keep").map_err(|e| e.to_string())?;
    stage_new_upload(storage, &seed, &upload)
        .await
        .map_err(|e| e.to_string())?;
    let (manifest_id, key) = complete_upload(storage, &seed, &upload, 0x70, "retained")
        .await
        .map_err(|e| e.to_string())?;

    let checkpoint_id = SnapshotSelectorId::from_bytes(raw_id(0x90));
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let mut checkpoint =
        PreparedPublication::from_global_epoch(&view).map_err(|e| e.to_string())?;
    checkpoint
        .publish_current_snapshot_pin(
            &view,
            SnapshotRole::Checkpoint,
            checkpoint_id,
            SelectorExpectation::Absent,
        )
        .map_err(|e| e.to_string())?;
    drop(view);
    checkpoint
        .commit(storage)
        .await
        .map_err(|e| e.to_string())?;

    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![StateTreeMutation::remove(key)],
        view.read(),
    )
    .await
    .map_err(|e| e.to_string())?;
    let transition = branch_transition(&view, state_edit, 0x80)
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
    sweep(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    if !object_present(storage, manifest_id)
        .await
        .map_err(|e| e.to_string())?
        || !object_present(storage, upload.chunk_id)
            .await
            .map_err(|e| e.to_string())?
    {
        return Err("checkpoint failed to retain historical blob closure".to_string());
    }
    Ok(format!(
        "checkpoint_retention:pass:manifest={manifest_id}:chunk={}",
        upload.chunk_id
    ))
}

async fn release_checkpoint_after_reopen<S: Storage>(storage: &S) -> Result<String, String> {
    let seed = build_seed(&[], ImmutableObjectSet::default());
    let upload = make_upload("retained", b"keep").map_err(|e| e.to_string())?;
    let manifest = BlobManifestV1 {
        logical_bytes: 4,
        ordered_chunks: upload.part.ordered_chunks.clone(),
        content_digest: *blake3::hash(b"keep").as_bytes(),
    };
    let (manifest_id, _) = manifest.encode().map_err(|e| e.to_string())?;
    let checkpoint_id = SnapshotSelectorId::from_bytes(raw_id(0x90));
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    let keys = [Key(snapshot_selector_key(
        SnapshotRole::Checkpoint,
        checkpoint_id,
    ))];
    let loaded = view
        .read()
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .map_err(|e| e.to_string())?;
    let raw = match loaded.values.as_slice() {
        [Some(crate::storage::ProjectedValue::FullValue(bytes))] => bytes.clone(),
        _ => return Err("checkpoint selector absent after cold reopen".to_string()),
    };
    let selector = SnapshotSelectorV1::decode(&raw).map_err(|e| e.to_string())?;
    let mut release = PreparedPublication::from_global_epoch(&view).map_err(|e| e.to_string())?;
    release
        .release_snapshot_pin(selector, raw)
        .map_err(|e| e.to_string())?;
    drop(view);
    release.commit(storage).await.map_err(|e| e.to_string())?;
    sweep(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    if !object_present(storage, manifest_id)
        .await
        .map_err(|e| e.to_string())?
        || !object_present(storage, upload.chunk_id)
            .await
            .map_err(|e| e.to_string())?
    {
        return Err("pin release reclaimed blob still owned by branch history".to_string());
    }
    Ok("checkpoint_retention:cold_reopen_release_branch_history_pass".to_string())
}

fn merge_blob() -> Result<(BlobChunkV1, BlobManifestV1, ObjectId), StorageError> {
    let chunk = BlobChunkV1 {
        bytes: Bytes::from_static(b"merge"),
    };
    let (chunk_id, _) = chunk.encode()?;
    let manifest = BlobManifestV1 {
        logical_bytes: 5,
        ordered_chunks: vec![BlobChunkRefV1 {
            chunk_object_id: chunk_id,
            declared_len: 5,
        }],
        content_digest: *blake3::hash(b"merge").as_bytes(),
    };
    let (manifest_id, _) = manifest.encode()?;
    Ok((chunk, manifest, manifest_id))
}

async fn run_branch_merge<S: Storage>(storage: &S) -> Result<String, String> {
    let seed = build_seed(&[], ImmutableObjectSet::default());
    seed_storage(storage, &seed)
        .await
        .map_err(|e| e.to_string())?;
    let (chunk, manifest, manifest_id) = merge_blob().map_err(|e| e.to_string())?;
    let (key, value) = state_entry("merge-result", 0x90, &[manifest_id]);
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .map_err(|e| e.to_string())?;
    if state_point(&view, &key, false)
        .await
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Err("merge diff key existed in base state".to_string());
    }
    let state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![StateTreeMutation::insert(key.clone(), value)],
        view.read(),
    )
    .await
    .map_err(|e| e.to_string())?;
    let transition =
        branch_transition_with_parents(&view, state_edit, 0x90, &[seed.merge_parent_object_id])
            .await
            .map_err(|e| e.to_string())?;
    let mut publication =
        PreparedPublication::from_branch_view(&view).map_err(|e| e.to_string())?;
    publication
        .stage_blob_chunk(&chunk)
        .map_err(|e| e.to_string())?;
    publication
        .stage_blob_manifest(&manifest)
        .map_err(|e| e.to_string())?;
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
    let row = state_point(&view, &key, false)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "merge result missing from branch state".to_string())?;
    if row.value.blob_manifest_object_ids != vec![manifest_id] {
        return Err("merge result points at another blob identity".to_string());
    }
    discover_sweep_plan(&view)
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!(
        "branch_merge:pass:second_parent={}:manifest={manifest_id}",
        seed.merge_parent_object_id
    ))
}

pub(crate) async fn run<S: Storage>(storage: &S, case: &str) -> Result<String, String> {
    match case {
        "corrupt_chunk" | "wrong_domain" | "edge_mismatch" => run_corruption(storage, case).await,
        "shared_identity" => run_shared_identity(storage).await,
        "upload_lifecycle" => run_upload_lifecycle(storage).await,
        "epoch_races" => run_epoch_races(storage).await,
        "checkpoint_retention" => run_checkpoint_retention(storage).await,
        "branch_merge" => run_branch_merge(storage).await,
        other => Err(format!("unknown ForkTree oracle case {other}")),
    }
}

pub(crate) async fn verify_reopen<S: Storage>(storage: &S, case: &str) -> Result<String, String> {
    match case {
        "corrupt_chunk" | "wrong_domain" | "edge_mismatch" => {
            let (seed, _) = corruption_fixture(case)?;
            let error = match open_coherent_view(storage, seed.branch_id).await {
                Ok(view) => discover_sweep_plan(&view)
                    .await
                    .expect_err("cold-reopened corrupt closure must fail closed"),
                Err(error) => error,
            };
            if !matches!(error, StorageError::Corruption(_)) {
                return Err(format!("{case}: cold reopen returned {error}"));
            }
            Ok(format!("{case}:cold_reopen_fail_closed"))
        }
        "shared_identity" => {
            let seed = build_seed(&[], ImmutableObjectSet::default());
            open_coherent_view(storage, seed.branch_id)
                .await
                .map_err(|e| e.to_string())?;
            Ok("shared_identity:cold_reopen_pass".to_string())
        }
        "upload_lifecycle" => {
            let seed = build_seed(&[], ImmutableObjectSet::default());
            let completed = make_upload("completed", b"data").map_err(|e| e.to_string())?;
            let manifest = BlobManifestV1 {
                logical_bytes: 4,
                ordered_chunks: completed.part.ordered_chunks.clone(),
                content_digest: *blake3::hash(b"data").as_bytes(),
            };
            let (manifest_id, _) = manifest.encode().map_err(|e| e.to_string())?;
            open_coherent_view(storage, seed.branch_id)
                .await
                .map_err(|e| e.to_string())?;
            if !object_present(storage, manifest_id)
                .await
                .map_err(|e| e.to_string())?
                || !object_present(storage, completed.chunk_id)
                    .await
                    .map_err(|e| e.to_string())?
            {
                return Err("completed upload missing after cold reopen".to_string());
            }
            Ok("upload_lifecycle:cold_reopen_pass".to_string())
        }
        "epoch_races" => {
            let seed = build_seed(&[], ImmutableObjectSet::default());
            let first = make_upload("epoch-first", b"first").map_err(|e| e.to_string())?;
            let second = make_upload("epoch-second", b"second").map_err(|e| e.to_string())?;
            open_coherent_view(storage, seed.branch_id)
                .await
                .map_err(|e| e.to_string())?;
            for id in [
                first.progress_id,
                first.chunk_id,
                second.progress_id,
                second.chunk_id,
            ] {
                if !object_present(storage, id)
                    .await
                    .map_err(|e| e.to_string())?
                {
                    return Err(format!("epoch-race root {id} absent after cold reopen"));
                }
            }
            Ok("epoch_races:cold_reopen_pass".to_string())
        }
        "checkpoint_retention" => release_checkpoint_after_reopen(storage).await,
        "branch_merge" => {
            let seed = build_seed(&[], ImmutableObjectSet::default());
            let (_, _, manifest_id) = merge_blob().map_err(|e| e.to_string())?;
            let (key, _) = state_entry("merge-result", 0x90, &[manifest_id]);
            let view = open_coherent_view(storage, seed.branch_id)
                .await
                .map_err(|e| e.to_string())?;
            let row = state_point(&view, &key, false)
                .await
                .map_err(|e| e.to_string())?
                .ok_or_else(|| "merge result absent after cold reopen".to_string())?;
            if row.value.blob_manifest_object_ids != vec![manifest_id] {
                return Err("merge blob identity changed after cold reopen".to_string());
            }
            discover_sweep_plan(&view)
                .await
                .map_err(|e| e.to_string())?;
            Ok("branch_merge:cold_reopen_pass".to_string())
        }
        other => Err(format!("unknown ForkTree oracle case {other}")),
    }
}
