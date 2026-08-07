use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;

use crate::storage::{
    CoreProjection, GetManyRequest, GetManyResult, GetOptions, Key, KeyRange, Memory, MemoryRead,
    MemoryWrite, PutBatch, PutEntry, ReadOptions, ScanChunk, ScanOptions, Storage, StorageError,
    StorageRead, StorageWrite, StoredValue, WriteOptions,
};

use super::model::{branch_selector_key, upload_binding_digest, upload_selector_key};
use super::tree::{
    ImmutableObjectSet, build_change_catalog, build_commit_catalog, build_retention_tree,
    build_state_tree, empty_receipt_tree, insert_receipt_part, lookup, scan_all,
    validate_branch_snapshot_ref_edge, validate_change_catalog_back_edge,
    validate_commit_catalog_back_edge, validate_receipt_tree, validate_upload_progress_tree,
    validate_upload_selector_progress,
};
use super::view::SELECTOR_SPACE;
use super::{
    BlobChunkRefV1, BlobChunkV1, BranchSelectorV1, BranchSnapshotV1, CanonicalBranchId,
    CanonicalUploadId, ChangeCatalogEntry, ChangeCatalogOwner, ChangeId, ChangeObjectV1,
    CoherentView, CommitCatalogEntry, CommitId, CommitObjectV1, GlobalSelectorV1, OBJECT_SPACE,
    ObjectId, PreparedPublication, RECEIPT_TREE_FANOUT, RECEIPT_TREE_LEAF_ENTRIES, ReceiptTreeEdit,
    ReceiptTreeRoot, RepositoryRootV1, SelectorExpectation, UploadPartV1, UploadProgressV1,
    UploadSelectorV1, open_coherent_view,
};

fn raw_id(byte: u8) -> [u8; 16] {
    [byte; 16]
}

fn content_id(byte: u8) -> ObjectId {
    ObjectId::from_bytes([byte; 32])
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
    global_selector: GlobalSelectorV1,
    branch_selector: BranchSelectorV1,
    orphan_object_id: ObjectId,
    orphan_object_bytes: Bytes,
}

fn build_seed() -> SeedData {
    let branch_id = CanonicalBranchId::from_bytes(raw_id(0x11));
    let commit_id = CommitId::from_bytes(raw_id(0x20));
    let semantic_change_id = ChangeId::from_bytes(raw_id(0x30));
    let ref_change_id = ChangeId::from_bytes(raw_id(0x31));
    let mut objects = ImmutableObjectSet::default();

    let global_state =
        build_state_tree(&[(b"global/k".to_vec(), b"global".to_vec())]).expect("global state");
    let global_state_root = global_state.root.object_id;
    objects
        .extend(global_state.objects)
        .expect("global objects");
    let local_state =
        build_state_tree(&[(b"local/k".to_vec(), b"local".to_vec())]).expect("local state");
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
        orphan_object_id,
        orphan_object_bytes,
    }
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
                        key: Key(super::model::global_selector_key()),
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

fn load_from<'a>(
    objects: &'a ImmutableObjectSet,
) -> impl Fn(ObjectId) -> Result<Bytes, StorageError> + 'a {
    move |id| {
        objects
            .get(id)
            .cloned()
            .ok_or_else(|| StorageError::Corruption(format!("test object {id} is absent")))
    }
}

#[test]
fn immutable_objects_authenticate_domain_length_and_bytes() {
    let seed = build_seed();
    let encoded = seed
        .objects
        .get(seed.repository_root_id)
        .expect("repository bytes");
    let decoded = RepositoryRootV1::decode(seed.repository_root_id, encoded)
        .expect("repository root authenticates");
    assert_ne!(decoded.commit_catalog_root, ObjectId::ZERO);

    let mut corrupted = encoded.to_vec();
    *corrupted.last_mut().expect("nonempty object") ^= 1;
    assert!(matches!(
        RepositoryRootV1::decode(seed.repository_root_id, &corrupted),
        Err(StorageError::Corruption(_))
    ));
    assert!(matches!(
        BranchSnapshotV1::decode(seed.repository_root_id, encoded),
        Err(StorageError::Corruption(_))
    ));
    assert!(matches!(
        RepositoryRootV1::decode(content_id(0x55), encoded),
        Err(StorageError::Corruption(_))
    ));
}

#[test]
fn catalogs_use_one_raw_uuid_tree_for_exact_and_ordered_access() {
    let seed = build_seed();
    let repository = RepositoryRootV1::decode(
        seed.repository_root_id,
        seed.objects
            .get(seed.repository_root_id)
            .expect("repository bytes"),
    )
    .expect("repository");
    let load = load_from(&seed.objects);

    let commit_value = lookup(
        repository.commit_catalog_root,
        "commit",
        seed.commit_id.as_bytes(),
        &load,
    )
    .expect("commit lookup")
    .expect("commit present");
    let commit_entry = CommitCatalogEntry::decode(&commit_value).expect("commit entry");
    let commit = validate_commit_catalog_back_edge(seed.commit_id, commit_entry, &load)
        .expect("commit back-edge");
    assert_eq!(commit.commit_id, seed.commit_id);

    let changes =
        scan_all(repository.change_catalog_root, "change", &load).expect("ordered change scan");
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].0, seed.semantic_change_id.as_bytes());
    assert_eq!(changes[1].0, seed.ref_change_id.as_bytes());
    for (key, value) in changes {
        let entry = ChangeCatalogEntry::decode(&value).expect("change entry");
        let key = ChangeId::from_bytes(key.try_into().expect("raw UUID key"));
        validate_change_catalog_back_edge(key, entry, &load).expect("change back-edge");
    }
    let snapshot = BranchSnapshotV1::decode(
        seed.branch_snapshot_id,
        seed.objects
            .get(seed.branch_snapshot_id)
            .expect("branch snapshot bytes"),
    )
    .expect("branch snapshot");
    validate_branch_snapshot_ref_edge(&snapshot, &load).expect("branch ref edge");
}

#[test]
fn catalog_back_edges_fail_closed_without_change_owner_cycles() {
    let seed = build_seed();
    let load = load_from(&seed.objects);
    let semantic_entry = ChangeCatalogEntry {
        change_object_id: seed.semantic_change_object_id,
        owner: ChangeCatalogOwner::CommitMember {
            commit_object_id: seed.commit_object_id,
            ordinal: 0,
        },
    };
    validate_change_catalog_back_edge(seed.semantic_change_id, semantic_entry, &load)
        .expect("valid semantic owner");

    for bad in [
        ChangeCatalogEntry {
            owner: ChangeCatalogOwner::CommitMember {
                commit_object_id: seed.commit_object_id,
                ordinal: 1,
            },
            ..semantic_entry
        },
        ChangeCatalogEntry {
            owner: ChangeCatalogOwner::BranchRef {
                ref_change_object_id: seed.semantic_change_object_id,
                branch_id: seed.branch_id,
            },
            ..semantic_entry
        },
    ] {
        assert!(matches!(
            validate_change_catalog_back_edge(seed.semantic_change_id, bad, &load),
            Err(StorageError::Corruption(_))
        ));
    }
    assert!(matches!(
        validate_change_catalog_back_edge(
            ChangeId::from_bytes(raw_id(0x99)),
            semantic_entry,
            &load,
        ),
        Err(StorageError::Corruption(_))
    ));

    let ref_entry = ChangeCatalogEntry {
        change_object_id: seed.ref_change_object_id,
        owner: ChangeCatalogOwner::BranchRef {
            ref_change_object_id: seed.ref_change_object_id,
            branch_id: CanonicalBranchId::from_bytes(raw_id(0xff)),
        },
    };
    assert!(matches!(
        validate_change_catalog_back_edge(seed.ref_change_id, ref_entry, &load),
        Err(StorageError::Corruption(_))
    ));
    assert!(
        ChangeCatalogEntry {
            change_object_id: seed.ref_change_object_id,
            owner: ChangeCatalogOwner::BranchRef {
                ref_change_object_id: seed.semantic_change_object_id,
                branch_id: seed.branch_id,
            },
        }
        .encode()
        .is_err()
    );

    let change = ChangeObjectV1::decode(
        seed.semantic_change_object_id,
        seed.objects
            .get(seed.semantic_change_object_id)
            .expect("semantic bytes"),
    )
    .expect("semantic change");
    assert!(matches!(change, ChangeObjectV1::Semantic { .. }));
    assert_eq!(
        change.encode().expect("re-encode semantic").0,
        seed.semantic_change_object_id,
        "semantic Change identity is independent of its catalog owner"
    );
}

#[test]
fn selector_codecs_authenticate_and_branch_selector_has_no_change_id() {
    let seed = build_seed();
    let raw_global = seed.global_selector.encode().expect("global selector");
    assert_eq!(
        GlobalSelectorV1::decode(&raw_global).expect("decode global"),
        seed.global_selector
    );
    let raw_branch = seed.branch_selector.encode().expect("branch selector");
    assert_eq!(
        BranchSelectorV1::decode(&raw_branch).expect("decode branch"),
        seed.branch_selector
    );
    assert!(
        !raw_branch
            .windows(seed.ref_change_id.as_bytes().len())
            .any(|window| window == seed.ref_change_id.as_bytes()),
        "BranchSelector must not duplicate ref_change_id"
    );
    let mut corrupt = raw_branch.to_vec();
    corrupt[12] ^= 1;
    assert!(matches!(
        BranchSelectorV1::decode(&corrupt),
        Err(StorageError::Corruption(_))
    ));
}

#[derive(Clone)]
struct CountingStorage {
    inner: Memory,
    begin_reads: Arc<AtomicUsize>,
    get_many_calls: Arc<AtomicUsize>,
}

impl CountingStorage {
    fn new() -> Self {
        Self {
            inner: Memory::new(),
            begin_reads: Arc::new(AtomicUsize::new(0)),
            get_many_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

struct CountingRead {
    inner: MemoryRead,
    get_many_calls: Arc<AtomicUsize>,
}

impl StorageRead for CountingRead {
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }

    fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
        self.get_many_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.get_many(requests)
    }

    fn scan(
        &self,
        space: crate::storage::StorageSpace,
        range: KeyRange,
        options: ScanOptions,
    ) -> impl Future<Output = Result<ScanChunk, StorageError>> + Send {
        self.inner.scan(space, range, options)
    }
}

impl Storage for CountingStorage {
    type Read<'a>
        = CountingRead
    where
        Self: 'a;
    type Write<'a>
        = MemoryWrite
    where
        Self: 'a;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.begin_reads.fetch_add(1, Ordering::Relaxed);
        Ok(CountingRead {
            inner: self.inner.begin_read(options).await?,
            get_many_calls: Arc::clone(&self.get_many_calls),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.inner.begin_write(options).await
    }
}

#[tokio::test]
async fn coherent_view_uses_one_read_and_binds_resume_to_raw_selector_pair() {
    let seed = build_seed();
    let storage = CountingStorage::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("open coherent view");
    assert_eq!(storage.begin_reads.load(Ordering::Relaxed), 1);
    assert_eq!(
        storage.get_many_calls.load(Ordering::Relaxed),
        2,
        "one selector get_many and one root-object get_many use the same read"
    );
    assert_eq!(
        view.repository_root().commit_catalog_root != ObjectId::ZERO,
        true
    );
    assert_eq!(view.branch_snapshot().branch_id, seed.branch_id);
    assert_eq!(view.branch_selector(), seed.branch_selector);
    let _ = view.read().snapshot_cache_key();
    let token = view.bind_resume_key(
        view.repository_root().change_catalog_root,
        seed.semantic_change_id.as_bytes(),
    );
    assert_eq!(
        view.validate_resume_key(view.repository_root().change_catalog_root, &token)
            .expect("resume token"),
        seed.semantic_change_id.as_bytes()
    );

    let original_view_id = view.view_id();
    let mut publication = PreparedPublication::from_branch_view(&view).expect("publication");
    let next_branch = BranchSelectorV1 {
        selector_generation: 2,
        ..seed.branch_selector
    };
    publication
        .put_selector(
            branch_selector_key(seed.branch_id),
            next_branch.encode().expect("next branch selector"),
            SelectorExpectation::Equals(view.raw_branch_selector().clone()),
        )
        .expect("stage branch selector");
    drop(view);
    publication
        .commit(&storage)
        .await
        .expect("publish branch move");

    let next_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("open next coherent view");
    assert_ne!(next_view.view_id(), original_view_id);
    assert!(matches!(
        next_view.validate_resume_key(next_view.repository_root().change_catalog_root, &token),
        Err(StorageError::InvalidCursor)
    ));
}

fn make_part(
    upload_id: &CanonicalUploadId,
    part_number: u64,
    byte_offset: u64,
    payload: &'static [u8],
) -> (ObjectId, Bytes, UploadPartV1, ObjectId, Bytes) {
    let chunk = BlobChunkV1 {
        bytes: Bytes::from_static(payload),
    };
    let (chunk_id, chunk_bytes) = chunk.encode().expect("chunk");
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
    let (part_id, part_bytes) = part.encode().expect("part");
    (chunk_id, chunk_bytes, part, part_id, part_bytes)
}

#[test]
fn receipt_tree_path_copies_with_bounded_aggregates_and_no_predecessor() {
    let upload_id = CanonicalUploadId::new("upload-a").expect("upload id");
    let initial: ReceiptTreeEdit = empty_receipt_tree().expect("empty receipt");
    assert_eq!(RECEIPT_TREE_LEAF_ENTRIES, 64);
    assert_eq!(RECEIPT_TREE_FANOUT, 32);
    let mut arena = initial.objects;
    let mut root = initial.root;
    let order = (0_u64..70).map(|part| (part * 17) % 70).collect::<Vec<_>>();
    for part_number in order {
        let payload = Box::leak(vec![part_number as u8; 8].into_boxed_slice());
        let (chunk_id, chunk_bytes, part, part_id, part_bytes) =
            make_part(&upload_id, part_number, part_number * 8, payload);
        arena.insert(chunk_id, chunk_bytes).expect("chunk object");
        arena.insert(part_id, part_bytes).expect("part object");
        let edit = insert_receipt_part(root, part_id, &part, load_from(&arena))
            .expect("insert receipt part");
        assert!(edit.copied_nodes <= 4, "bounded path copy at 70 parts");
        root = edit.root;
        arena.extend(edit.objects).expect("tree nodes");
    }
    assert_eq!(root.completed_part_count, 70);
    assert_eq!(root.received_bytes, 560);
    assert_eq!(root.contiguous_prefix_bytes, 560);
    let parts =
        validate_receipt_tree(root, &upload_id, load_from(&arena)).expect("receipt closure");
    assert_eq!(parts.len(), 70);

    let duplicate = &parts[32];
    let duplicate_id = ObjectId::from_bytes(
        lookup(
            root.object_id,
            "receipt",
            &duplicate.part_number.to_be_bytes(),
            load_from(&arena),
        )
        .expect("duplicate lookup")
        .expect("part present")
        .try_into()
        .expect("part id"),
    );
    let duplicate_edit = insert_receipt_part(root, duplicate_id, duplicate, load_from(&arena))
        .expect("idempotent duplicate");
    assert!(!duplicate_edit.inserted);
    assert_eq!(duplicate_edit.root, root);
    assert!(duplicate_edit.objects.is_empty());

    let (_, _, conflicting, conflicting_id, conflicting_bytes) =
        make_part(&upload_id, 32, 256, b"different");
    arena
        .insert(conflicting_id, conflicting_bytes)
        .expect("conflicting part object");
    assert!(matches!(
        insert_receipt_part(root, conflicting_id, &conflicting, load_from(&arena)),
        Err(StorageError::WriteConflict)
    ));
}

#[test]
fn receipt_progress_and_part_corruption_fail_closed() {
    let upload_id = CanonicalUploadId::new("upload-corruption").expect("upload id");
    let initial = empty_receipt_tree().expect("empty receipt");
    let mut arena = initial.objects;
    let (chunk_id, chunk_bytes, part, part_id, part_bytes) =
        make_part(&upload_id, 0, 0, b"payload");
    arena.insert(chunk_id, chunk_bytes).expect("chunk");
    arena.insert(part_id, part_bytes).expect("part");
    let edit =
        insert_receipt_part(initial.root, part_id, &part, load_from(&arena)).expect("insert");
    let root = edit.root;
    arena.extend(edit.objects).expect("nodes");
    let binding_digest =
        upload_binding_digest(b"repository", b"/file", b"file", 7, None).expect("binding");
    let progress = UploadProgressV1 {
        upload_id: upload_id.clone(),
        binding_digest,
        receipt_tree_root: root.object_id,
        completed_part_count: root.completed_part_count,
        received_bytes: root.received_bytes,
        contiguous_prefix_bytes: root.contiguous_prefix_bytes,
    };
    let (progress_id, progress_bytes) = progress.encode().expect("progress");
    let decoded = UploadProgressV1::decode(progress_id, &progress_bytes).expect("decode progress");
    validate_upload_progress_tree(&decoded, load_from(&arena)).expect("progress closure");

    let wrong_count = UploadProgressV1 {
        completed_part_count: 2,
        ..progress.clone()
    };
    assert!(validate_upload_progress_tree(&wrong_count, load_from(&arena)).is_err());
    let wrong_size = UploadProgressV1 {
        received_bytes: 8,
        ..progress
    };
    assert!(validate_upload_progress_tree(&wrong_size, load_from(&arena)).is_err());

    let malformed = UploadPartV1 {
        declared_part_len: 8,
        ..part
    };
    assert!(malformed.encode().is_err());
    let mut corrupt_chunk = arena.get(chunk_id).expect("chunk bytes").to_vec();
    corrupt_chunk.push(0);
    assert!(BlobChunkV1::decode(chunk_id, &corrupt_chunk).is_err());

    let wrong_declared_chunk = UploadPartV1 {
        upload_id: upload_id.clone(),
        part_number: 1,
        byte_offset: 7,
        declared_part_len: 8,
        ordered_chunks: vec![BlobChunkRefV1 {
            chunk_object_id: chunk_id,
            declared_len: 8,
        }],
        part_digest: *blake3::hash(b"payload").as_bytes(),
    };
    let (wrong_part_id, wrong_part_bytes) = wrong_declared_chunk
        .encode()
        .expect("self-consistent part metadata");
    arena
        .insert(wrong_part_id, wrong_part_bytes)
        .expect("wrong declared part");
    let wrong_edit = insert_receipt_part(
        root,
        wrong_part_id,
        &wrong_declared_chunk,
        load_from(&arena),
    )
    .expect("tree accepts authenticated part metadata before closure validation");
    let wrong_root = wrong_edit.root;
    arena.extend(wrong_edit.objects).expect("wrong tree nodes");
    assert!(validate_receipt_tree(wrong_root, &upload_id, load_from(&arena)).is_err());
}

struct UploadPublication {
    selector_key: Bytes,
    selector_value: Bytes,
    objects: ImmutableObjectSet,
    progress_id: ObjectId,
}

fn make_upload_publication() -> UploadPublication {
    let upload_id = CanonicalUploadId::new("race-upload").expect("upload id");
    let binding_digest = upload_binding_digest(
        b"repository",
        b"/race.bin",
        b"file",
        4,
        Some(*blake3::hash(b"data").as_bytes()),
    )
    .expect("binding");
    let initial = empty_receipt_tree().expect("receipt root");
    let mut objects = initial.objects;
    let (chunk_id, chunk_bytes, part, part_id, part_bytes) = make_part(&upload_id, 0, 0, b"data");
    objects.insert(chunk_id, chunk_bytes).expect("chunk");
    objects.insert(part_id, part_bytes).expect("part");
    let edit = insert_receipt_part(initial.root, part_id, &part, load_from(&objects))
        .expect("receipt insert");
    let root = edit.root;
    objects.extend(edit.objects).expect("receipt nodes");
    let progress = UploadProgressV1 {
        upload_id: upload_id.clone(),
        binding_digest,
        receipt_tree_root: root.object_id,
        completed_part_count: root.completed_part_count,
        received_bytes: root.received_bytes,
        contiguous_prefix_bytes: root.contiguous_prefix_bytes,
    };
    let (progress_id, progress_bytes) = progress.encode().expect("progress");
    objects
        .insert(progress_id, progress_bytes)
        .expect("progress object");
    let selector = UploadSelectorV1 {
        upload_id: upload_id.clone(),
        binding_digest,
        progress_object_id: progress_id,
        selector_generation: 1,
    };
    UploadPublication {
        selector_key: upload_selector_key(&upload_id).expect("upload key"),
        selector_value: selector.encode().expect("upload selector"),
        objects,
        progress_id,
    }
}

async fn prepare_upload(
    view: &CoherentView<impl StorageRead>,
    upload: &UploadPublication,
) -> PreparedPublication {
    let mut publication = PreparedPublication::from_global_epoch(view).expect("upload fence");
    publication
        .put_selector(
            upload.selector_key.clone(),
            upload.selector_value.clone(),
            SelectorExpectation::Absent,
        )
        .expect("upload selector");
    publication
        .put_objects(upload.objects.clone())
        .expect("upload objects");
    publication
}

#[tokio::test]
async fn upload_publication_and_gc_are_epoch_fenced_in_both_orders() {
    // Publication first: stale GC cannot delete an object after the receipt
    // selector makes its closure reachable.
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let publication_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("publication view");
    let gc_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("gc view");
    let upload = make_upload_publication();
    let publication = prepare_upload(&publication_view, &upload).await;
    let mut stale_gc = PreparedPublication::from_global_epoch(&gc_view).expect("gc fence");
    stale_gc
        .delete_object(upload.progress_id)
        .expect("stage stale delete");
    drop(publication_view);
    drop(gc_view);
    publication.commit(&storage).await.expect("publish receipt");
    assert!(matches!(
        stale_gc.commit(&storage).await,
        Err(StorageError::PreconditionFailed(_))
    ));
    let reopened = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("cold reopen");
    reopened
        .load_object_bytes(upload.progress_id)
        .await
        .expect("receipt progress survives");

    // GC first: the stale receipt cannot publish against deleted/deduplicated
    // payloads. Retry reopens the epoch and restages every absent object.
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let publication_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("publication view");
    let gc_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("gc view");
    let stale_publication = prepare_upload(&publication_view, &upload).await;
    let mut gc = PreparedPublication::from_global_epoch(&gc_view).expect("gc fence");
    gc.delete_object(seed.orphan_object_id)
        .expect("delete existing orphan");
    drop(publication_view);
    drop(gc_view);
    gc.commit(&storage).await.expect("gc first");
    assert!(matches!(
        stale_publication.commit(&storage).await,
        Err(StorageError::PreconditionFailed(_))
    ));
    let retry_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("retry view");
    let retry = prepare_upload(&retry_view, &upload).await;
    drop(retry_view);
    retry.commit(&storage).await.expect("retry publication");
    let reopened = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("cold reopen");
    let progress_bytes = reopened
        .load_object_bytes(upload.progress_id)
        .await
        .expect("restaged progress");
    let progress = UploadProgressV1::decode(upload.progress_id, &progress_bytes)
        .expect("authenticated progress");
    let receipt_root = ReceiptTreeRoot {
        object_id: progress.receipt_tree_root,
        completed_part_count: progress.completed_part_count,
        received_bytes: progress.received_bytes,
        contiguous_prefix_bytes: progress.contiguous_prefix_bytes,
    };
    assert_eq!(receipt_root.completed_part_count, 1);
}

#[tokio::test]
async fn upload_abort_releases_only_the_selector_under_the_epoch() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("upload view");
    let upload = make_upload_publication();
    let publication = prepare_upload(&view, &upload).await;
    drop(view);
    publication.commit(&storage).await.expect("publish upload");

    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("abort view");
    let keys = [Key(upload.selector_key.clone())];
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
        .expect("load upload selector");
    let raw_selector = match loaded.values.into_iter().next().flatten() {
        Some(crate::storage::ProjectedValue::FullValue(bytes)) => bytes,
        value => panic!("expected upload selector bytes, got {value:?}"),
    };
    let mut abort = PreparedPublication::from_global_epoch(&view).expect("abort fence");
    abort
        .delete_selector(upload.selector_key.clone(), raw_selector)
        .expect("delete upload selector");
    drop(view);
    abort.commit(&storage).await.expect("abort upload");

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("read after abort");
    let selectors = read
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &keys,
            opts: GetOptions::default(),
        }])
        .await
        .expect("selector after abort");
    assert_eq!(selectors.values, vec![None]);
    let object_keys = [Key(Bytes::copy_from_slice(upload.progress_id.as_bytes()))];
    let objects = read
        .get_many(&[GetManyRequest {
            space: OBJECT_SPACE,
            keys: &object_keys,
            opts: GetOptions::default(),
        }])
        .await
        .expect("orphan after abort");
    assert!(objects.values[0].is_some(), "sweep owns physical deletion");
}

fn alternate_root_publication(
    seed: &SeedData,
) -> (BranchSelectorV1, ImmutableObjectSet, ObjectId, ObjectId) {
    let seed_commit = CommitObjectV1::decode(
        seed.commit_object_id,
        seed.objects
            .get(seed.commit_object_id)
            .expect("seed commit"),
    )
    .expect("decode seed commit");
    let alternate_commit = CommitObjectV1 {
        commit_id: CommitId::from_bytes(raw_id(0x44)),
        generation: 2,
        parent_commit_object_ids: vec![seed.commit_object_id],
        member_change_object_ids: Vec::new(),
        global_state_root: seed_commit.global_state_root,
        local_state_root: seed_commit.local_state_root,
        metadata: b"alternate-root".to_vec(),
    };
    let (alternate_commit_id, alternate_commit_bytes) =
        alternate_commit.encode().expect("alternate commit");
    let ref_change = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0x45)),
        branch_id: seed.branch_id,
        before_semantic_head_commit_object_id: Some(seed.commit_object_id),
        after_semantic_head_commit_object_id: Some(alternate_commit_id),
        previous_ref_change_object_id: Some(seed.ref_change_object_id),
        payload: b"root-only-move".to_vec(),
    };
    let (ref_change_id, ref_change_bytes) = ref_change.encode().expect("root ref change");
    let snapshot = BranchSnapshotV1 {
        branch_id: seed.branch_id,
        local_state_root: seed_commit.local_state_root,
        semantic_head_commit_object_id: alternate_commit_id,
        latest_ref_change_object_id: Some(ref_change_id),
        historical_global_state_root: seed_commit.global_state_root,
    };
    let (snapshot_id, snapshot_bytes) = snapshot.encode().expect("alternate snapshot");
    let mut objects = ImmutableObjectSet::default();
    objects
        .insert(alternate_commit_id, alternate_commit_bytes)
        .expect("alternate commit object");
    objects
        .insert(ref_change_id, ref_change_bytes)
        .expect("root ref-change object");
    objects
        .insert(snapshot_id, snapshot_bytes)
        .expect("alternate snapshot object");

    let commit_catalog = build_commit_catalog(&[
        (
            seed.commit_id,
            CommitCatalogEntry {
                commit_object_id: seed.commit_object_id,
            },
        ),
        (
            alternate_commit.commit_id,
            CommitCatalogEntry {
                commit_object_id: alternate_commit_id,
            },
        ),
    ])
    .expect("next commit catalog");
    let commit_catalog_root = commit_catalog.root.object_id;
    objects
        .extend(commit_catalog.objects)
        .expect("next commit catalog objects");
    let change_catalog = build_change_catalog(&[
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
            ref_change.change_id(),
            ChangeCatalogEntry {
                change_object_id: ref_change_id,
                owner: ChangeCatalogOwner::BranchRef {
                    ref_change_object_id: ref_change_id,
                    branch_id: seed.branch_id,
                },
            },
        ),
    ])
    .expect("next change catalog");
    let change_catalog_root = change_catalog.root.object_id;
    objects
        .extend(change_catalog.objects)
        .expect("next change catalog objects");
    let current_repository = RepositoryRootV1::decode(
        seed.repository_root_id,
        seed.objects
            .get(seed.repository_root_id)
            .expect("current repository root"),
    )
    .expect("decode current repository root");
    let next_repository = RepositoryRootV1 {
        commit_catalog_root,
        change_catalog_root,
        ..current_repository
    };
    let (next_repository_id, next_repository_bytes) =
        next_repository.encode().expect("next repository root");
    objects
        .insert(next_repository_id, next_repository_bytes)
        .expect("next repository object");
    (
        BranchSelectorV1 {
            branch_id: seed.branch_id,
            branch_snapshot_object_id: snapshot_id,
            selector_generation: seed.branch_selector.selector_generation + 1,
        },
        objects,
        alternate_commit_id,
        next_repository_id,
    )
}

async fn prepare_root_move(
    view: &CoherentView<impl StorageRead>,
    selector: BranchSelectorV1,
    objects: ImmutableObjectSet,
    repository_root: ObjectId,
) -> PreparedPublication {
    let mut publication = PreparedPublication::from_branch_view(view).expect("branch fence");
    publication
        .set_repository_root(repository_root)
        .expect("next repository root");
    publication
        .put_objects(objects)
        .expect("root-only immutable objects");
    publication
        .put_selector(
            branch_selector_key(view.branch_id()),
            selector.encode().expect("next branch selector"),
            SelectorExpectation::Equals(view.raw_branch_selector().clone()),
        )
        .expect("root-only selector");
    publication
}

#[tokio::test]
async fn root_only_publication_and_gc_are_epoch_fenced_in_both_orders() {
    let seed = build_seed();
    let (selector, objects, alternate_commit_id, repository_root) =
        alternate_root_publication(&seed);

    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let root_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("root view");
    let gc_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("gc view");
    let root_move = prepare_root_move(&root_view, selector, objects.clone(), repository_root).await;
    let mut stale_gc = PreparedPublication::from_global_epoch(&gc_view).expect("gc fence");
    stale_gc
        .delete_object(alternate_commit_id)
        .expect("stale root delete");
    drop(root_view);
    drop(gc_view);
    root_move.commit(&storage).await.expect("root publication");
    assert!(matches!(
        stale_gc.commit(&storage).await,
        Err(StorageError::PreconditionFailed(_))
    ));

    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let root_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("root view");
    let gc_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("gc view");
    let stale_root_move =
        prepare_root_move(&root_view, selector, objects.clone(), repository_root).await;
    let mut gc = PreparedPublication::from_global_epoch(&gc_view).expect("gc fence");
    gc.delete_object(seed.orphan_object_id)
        .expect("existing orphan delete");
    drop(root_view);
    drop(gc_view);
    gc.commit(&storage).await.expect("gc first");
    assert!(matches!(
        stale_root_move.commit(&storage).await,
        Err(StorageError::PreconditionFailed(_))
    ));
    let retry_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("retry view");
    let retry = prepare_root_move(&retry_view, selector, objects, repository_root).await;
    drop(retry_view);
    retry
        .commit(&storage)
        .await
        .expect("retry root publication");
    let reopened = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("reopen root");
    assert_eq!(
        reopened.branch_snapshot().semantic_head_commit_object_id,
        alternate_commit_id
    );
}

#[test]
fn upload_selector_has_bounded_progress_edge_and_no_predecessor_encoding() {
    let upload = make_upload_publication();
    let selector = UploadSelectorV1::decode(&upload.selector_value).expect("selector");
    assert_eq!(selector.progress_object_id, upload.progress_id);
    let progress = UploadProgressV1::decode(
        upload.progress_id,
        upload
            .objects
            .get(upload.progress_id)
            .expect("progress bytes"),
    )
    .expect("progress");
    assert_eq!(progress.completed_part_count, 1);
    assert_eq!(progress.received_bytes, 4);
    assert_eq!(progress.contiguous_prefix_bytes, 4);
    assert_eq!(
        upload
            .objects
            .iter()
            .filter(|(id, _)| *id == upload.progress_id)
            .count(),
        1,
        "exactly one current progress object is reachable by the selector"
    );
    assert!(upload.objects.len() >= 4);
}

#[tokio::test]
async fn partial_object_staging_without_selector_publication_is_not_visible() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let (selector, objects, alternate_commit_id, _) = alternate_root_publication(&seed);
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("orphan write");
    write
        .put_many(
            OBJECT_SPACE,
            PutBatch {
                entries: objects
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
        .expect("stage orphan path copy");
    write.commit().await.expect("commit orphan objects");
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("open old root");
    assert_eq!(view.branch_selector(), seed.branch_selector);
    assert_ne!(
        view.branch_snapshot().semantic_head_commit_object_id,
        alternate_commit_id
    );
    assert_ne!(selector, seed.branch_selector);
}

#[test]
fn upload_part_declared_size_and_selector_binding_mismatches_fail_closed() {
    let upload_id = CanonicalUploadId::new("binding").expect("upload id");
    let (_, _, part, _, _) = make_part(&upload_id, 0, 0, b"1234");
    let wrong_size = UploadPartV1 {
        declared_part_len: 3,
        ..part
    };
    assert!(wrong_size.encode().is_err());

    let upload = make_upload_publication();
    let selector = UploadSelectorV1::decode(&upload.selector_value).expect("selector");
    let progress = UploadProgressV1::decode(
        upload.progress_id,
        upload.objects.get(upload.progress_id).expect("progress"),
    )
    .expect("progress");
    assert_eq!(selector.upload_id, progress.upload_id);
    assert_eq!(selector.binding_digest, progress.binding_digest);
    validate_upload_selector_progress(&selector, load_from(&upload.objects))
        .expect("selector/progress binding");
    let mismatched = UploadSelectorV1 {
        binding_digest: [0xff; 32],
        ..selector
    };
    assert!(validate_upload_selector_progress(&mismatched, load_from(&upload.objects)).is_err());
}

#[test]
fn object_id_display_is_stable_hex() {
    let id = content_id(0xab);
    assert_eq!(id.to_string(), "ab".repeat(32));
    assert_eq!(id.into_bytes(), [0xab; 32]);
    assert_eq!(CommitId::from_bytes(raw_id(1)).into_bytes(), raw_id(1));
    assert_eq!(ChangeId::from_bytes(raw_id(2)).into_bytes(), raw_id(2));
    assert_eq!(
        CanonicalBranchId::from_bytes(raw_id(3)).into_bytes(),
        raw_id(3)
    );
}

#[test]
fn catalog_entry_widths_are_canonical() {
    let commit = CommitCatalogEntry {
        commit_object_id: content_id(1),
    };
    assert_eq!(commit.encode().expect("commit entry").len(), 32);
    let semantic = ChangeCatalogEntry {
        change_object_id: content_id(2),
        owner: ChangeCatalogOwner::CommitMember {
            commit_object_id: content_id(3),
            ordinal: 7,
        },
    };
    assert_eq!(semantic.encode().expect("semantic entry").len(), 69);
    let branch = ChangeCatalogEntry {
        change_object_id: content_id(4),
        owner: ChangeCatalogOwner::BranchRef {
            ref_change_object_id: content_id(4),
            branch_id: CanonicalBranchId::from_bytes(raw_id(5)),
        },
    };
    assert_eq!(branch.encode().expect("branch entry").len(), 81);
}

#[tokio::test]
async fn coherent_view_object_reads_stay_on_original_snapshot() {
    let seed = build_seed();
    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("view");
    let raw_before = view
        .load_object_bytes(seed.orphan_object_id)
        .await
        .expect("snapshot sees orphan");
    let mut publication = PreparedPublication::from_global_epoch(&view).expect("gc fence");
    publication
        .delete_object(seed.orphan_object_id)
        .expect("delete orphan");
    // Memory's write can commit while the immutable read remains alive.
    publication.commit(&storage).await.expect("delete orphan");
    assert_eq!(
        view.load_object_bytes(seed.orphan_object_id)
            .await
            .expect("old coherent snapshot remains stable"),
        raw_before
    );
    drop(view);
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("new read");
    let keys = [Key(Bytes::copy_from_slice(
        seed.orphan_object_id.as_bytes(),
    ))];
    let loaded = read
        .get_many(&[GetManyRequest {
            space: OBJECT_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("load deleted object");
    assert_eq!(loaded.values, vec![None]);
}

#[test]
fn seed_provenance_fields_are_not_accidentally_aliased() {
    let seed = build_seed();
    assert_ne!(seed.commit_object_id, seed.semantic_change_object_id);
    assert_ne!(seed.ref_change_object_id, seed.semantic_change_object_id);
    assert_ne!(seed.repository_root_id, seed.branch_snapshot_id);
    assert_eq!(
        seed.objects
            .get(seed.orphan_object_id)
            .expect("orphan bytes"),
        &seed.orphan_object_bytes
    );
}
