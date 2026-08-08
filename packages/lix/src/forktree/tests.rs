use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use bytes::Bytes;

use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::storage::{
    BeginScanOptions, CommitResult, CoreProjection, GetManyRequest, GetManyResult, GetOptions, Key,
    KeyRange, Memory, MemoryRead, MemoryWrite, PutBatch, PutEntry, ReadOptions, ScanCursor,
    Storage, StorageError, StorageRead, StorageWrite, StoredValue, WriteOptions,
};
use crate::storage_adapter::{StorageAdapterRead, StorageAdapterReadScope};

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
    CommitObjectV1, GcBudget, GcStepStatus, GlobalSelectorV1, ObjectId, PreparedPublication,
    RECEIPT_TREE_FANOUT, RECEIPT_TREE_LEAF_ENTRIES, ReceiptTreeEdit, ReceiptTreeRoot,
    RepositoryRootV1, SelectorExpectation, SnapshotRole, SnapshotSelectorId, SnapshotSelectorV1,
    SnapshotTargetV1, StateCell, StateCellRef, StateKeyRef, StateSource, StateTreeMutation,
    StateValueRef, UntrackedValueRef, UploadBindingRef, UploadPartV1, UploadProgressV1,
    UploadSelectorV1, VisibleStateRow, abort_corrupt_gc, advance_gc, edit_state_tree,
    encode_state_key, encode_state_value, load_change, load_commit, load_commit_member_records,
    load_commit_topologies, open_coherent_view, page_changes, page_commits,
    prepare_upload_completion, put_change_catalog_entries, put_commit_catalog_entries, state_point,
    state_range,
};

fn raw_id(byte: u8) -> [u8; 16] {
    [byte; 16]
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
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("open seed write");
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
        .await
        .expect("seed objects");
    write
        .put_many(
            SELECTOR_SPACE,
            PutBatch {
                entries: vec![
                    PutEntry {
                        key: Key(global_selector_key()),
                        value: StoredValue {
                            bytes: seed.global_selector.encode().expect("global selector"),
                        },
                    },
                    PutEntry {
                        key: Key(branch_selector_key(seed.branch_id)),
                        value: StoredValue {
                            bytes: seed.branch_selector.encode().expect("branch selector"),
                        },
                    },
                ],
            },
        )
        .await
        .expect("seed selectors");
    write.commit().await.expect("commit seed");
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
        member_change_object_ids: Vec::new(),
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
    };
    let (ref_object_id, _) = ref_change.encode().expect("next ref change");
    let commit_catalog_edit = put_commit_catalog_entries(
        view.repository_root().commit_catalog_root,
        &[(
            semantic_commit.commit_id,
            CommitCatalogEntry { commit_object_id },
        )],
        view.read(),
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
        view.read(),
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
        view.read(),
    )
    .await
    .expect("update/remove path copy");
    assert_eq!(edit.entry_count(), 2);
    assert!(edit.copied_nodes() >= 2);
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
        view.read(),
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
    publication
        .commit(&storage)
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

struct TopologyCountingRead {
    inner: StorageAdapterReadScope<MemoryRead>,
    forbidden_member: ObjectId,
    member_object_reads: Arc<AtomicUsize>,
}

impl StorageAdapterRead for TopologyCountingRead {
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        let member_key = self.forbidden_member.as_bytes();
        if requests.iter().any(|request| {
            request.space == OBJECT_SPACE
                && request.keys.iter().any(|key| key.0.as_ref() == member_key)
        }) {
            self.member_object_reads.fetch_add(1, Ordering::Relaxed);
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

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("corruption write");
    write
        .delete_many(
            OBJECT_SPACE,
            &[Key(Bytes::copy_from_slice(
                seed.semantic_change_object_id.as_bytes(),
            ))],
        )
        .await
        .expect("delete selected member");
    write.commit().await.expect("commit corruption");
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("bounded open does not traverse an unrelated catalog member");
    assert!(load_change(&view, seed.semantic_change_id).await.is_err());
}

#[tokio::test]
async fn commit_topology_never_hydrates_member_changes_and_member_history_fails_closed() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let member_object_reads = Arc::new(AtomicUsize::new(0));
    let read = TopologyCountingRead {
        inner: StorageAdapterReadScope::new(
            storage
                .begin_read(ReadOptions::default())
                .await
                .expect("topology read"),
        ),
        forbidden_member: seed.semantic_change_object_id,
        member_object_reads: Arc::clone(&member_object_reads),
    };
    let public_id = public_commit_id(0x20);
    assert_eq!(
        load_commit_topologies(&read, &[public_id])
            .await
            .expect("topology")
            .into_iter()
            .next()
            .flatten(),
        Some(super::CommitTopology {
            commit_id: public_id,
            parent_commit_ids: Vec::new(),
            generation: 1,
        })
    );
    assert_eq!(member_object_reads.load(Ordering::Relaxed), 0);
    drop(read);

    let mut corrupt = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("corruption write");
    corrupt
        .delete_many(
            OBJECT_SPACE,
            &[Key(Bytes::copy_from_slice(
                seed.semantic_change_object_id.as_bytes(),
            ))],
        )
        .await
        .expect("delete member");
    corrupt.commit().await.expect("commit corruption");

    let read = StorageAdapterReadScope::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .expect("post-corruption read"),
    );
    assert!(
        load_commit_topologies(&read, &[public_id])
            .await
            .expect("member corruption remains latent for topology")
            .into_iter()
            .next()
            .flatten()
            .is_some()
    );
    assert!(load_commit_member_records(&read, public_id).await.is_err());
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
        member_change_object_ids: Vec::new(),
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
        member_change_object_ids: vec![bad_generation.semantic_change_object_id],
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
    publish.commit(&storage).await.expect("receipt first");
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
        stale_publish.commit(&storage).await,
        Err(StorageError::PreconditionFailed(_))
    ));
    let retry_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("retry view");
    let mut retry = PreparedPublication::from_global_epoch(&retry_view).expect("retry");
    stage_upload(&mut retry, &upload);
    drop(retry_view);
    retry.commit(&storage).await.expect("retry publication");
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
    publication
        .commit(&storage)
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
    checkpoint
        .commit(&storage)
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
        .read()
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
        current.read(),
    )
    .await
    .expect("unchanged commit catalog");
    let change_catalog_edit = retire_change_catalog_entries(
        current.repository_root().change_catalog_root,
        &[],
        current.read(),
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
    release.commit(&storage).await.expect("release commit");
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
        assert!(publication.commit(&storage).await.is_err());

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
        orphan.commit(&storage).await.expect("stage orphan");

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

    let mut corrupt = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("corruption write");
    corrupt
        .delete_many(
            OBJECT_SPACE,
            &[Key(Bytes::copy_from_slice(mark_root.as_bytes()))],
        )
        .await
        .expect("remove mark root");
    corrupt.commit().await.expect("commit corruption");

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
            view.read(),
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
            view.read(),
        )
        .await
        .is_err(),
        "catalog updates must use canonical raw-UUID order"
    );

    let wrong_base = edit_state_tree(
        view.repository_root().global_state_root,
        Vec::new(),
        view.read(),
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
        view.read(),
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
    publication.commit(&storage).await.expect("publish upload");

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("abort view");
    let keys = [Key(
        upload_selector_key(&upload.upload_id).expect("upload key")
    )];
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
    abort.commit(&storage).await.expect("abort commit");
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
    publication.commit(&storage).await.expect("publish receipt");

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
        content_digest: *blake3::hash(b"data").as_bytes(),
    };
    let (manifest_id, _) = manifest.encode().expect("manifest");
    let (key, value) = state_entry("blob", StateCellRef::Value("blob"), 0x70, &[manifest_id]);
    let state_edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![StateTreeMutation::insert(key.clone(), value)],
        view.read(),
    )
    .await
    .expect("state edit");
    let transition = branch_transition(&view, state_edit, 0x70).await;
    let mut publish = PreparedPublication::from_branch_view(&view).expect("completion publication");
    assert_eq!(
        publish
            .publish_completed_upload(&view, completion, transition)
            .await
            .expect("atomic handoff"),
        manifest_id
    );
    drop(view);
    publish.commit(&storage).await.expect("complete upload");

    let reopened = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("cold reopen");
    let row = state_point(&reopened, &key, false)
        .await
        .expect("blob state")
        .expect("blob row");
    assert_eq!(row.value.blob_manifest_object_ids, vec![manifest_id]);
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
        .read()
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
    publication.commit(storage).await.expect("untracked commit");
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
    publication.commit(storage).await.expect("delete commit");
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
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("disposable selector write");
    write
        .put_many(
            SELECTOR_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: Key(branch_selector_key(disposable)),
                    value: StoredValue {
                        bytes: selector.encode().expect("disposable selector"),
                    },
                }],
            },
        )
        .await
        .expect("put disposable selector");
    write.commit().await.expect("commit disposable selector");
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
    upload_publication
        .commit(&storage)
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
        view.read(),
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
    complete.commit(&storage).await.expect("complete upload");

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
    checkpoint
        .commit(&storage)
        .await
        .expect("checkpoint commit");

    let view = open_coherent_view(&storage, disposable)
        .await
        .expect("retirement view");
    let commit_catalog_edit =
        retire_commit_catalog_entries(view.repository_root().commit_catalog_root, &[], view.read())
            .await
            .expect("retire commit");
    let change_catalog_edit =
        retire_change_catalog_entries(view.repository_root().change_catalog_root, &[], view.read())
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
    retire.commit(&storage).await.expect("retire commit");
    sweep(&storage, seed.branch_id).await;
    assert!(object_present(&storage, manifest_id).await);
    assert!(object_present(&storage, upload.chunk_id).await);

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("release view");
    let key = snapshot_selector_key(SnapshotRole::Checkpoint, checkpoint_id);
    let keys = [Key(key)];
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
        .expect("checkpoint selector read");
    let raw = match loaded.values.as_slice() {
        [Some(crate::storage::ProjectedValue::FullValue(bytes))] => bytes.clone(),
        other => panic!("expected checkpoint selector, got {other:?}"),
    };
    let selector = SnapshotSelectorV1::decode(&raw).expect("checkpoint selector");
    let commit_catalog_edit = retire_commit_catalog_entries(
        view.repository_root().commit_catalog_root,
        &[CommitId::from_bytes(raw_id(0x80))],
        view.read(),
    )
    .await
    .expect("final commit retirement");
    let change_catalog_edit = retire_change_catalog_entries(
        view.repository_root().change_catalog_root,
        &[initial_ref_id, ChangeId::from_bytes(raw_id(0x81))],
        view.read(),
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
    release.commit(&storage).await.expect("release commit");
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
    orphan_stage
        .commit(&storage)
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
    root_only.commit(&storage).await.expect("root first");
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
    orphan_stage
        .commit(&inverse)
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
        stale_root.commit(&inverse).await,
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
    retry
        .commit(&inverse)
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
    roles.commit(&storage).await.expect("all roles");
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
    publication.commit(&storage).await.expect("selector pages");
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
    corrupt
        .commit(&storage)
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
