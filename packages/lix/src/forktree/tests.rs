use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use bytes::Bytes;

use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::storage::{
    CommitResult, CoreProjection, GetManyRequest, GetManyResult, GetOptions, Key, KeyRange, Memory,
    MemoryRead, MemoryWrite, ProjectedValue as StorageProjectedValue, PutBatch, PutEntry,
    ReadOptions, ScanChunk, ScanOptions, Storage, StorageError, StorageRead, StorageWrite,
    StoredValue, WriteOptions,
};
use crate::storage_adapter::{StorageAdapterRead, StorageAdapterReadScope};

use super::model::{
    branch_selector_key, gc_progress_selector_key, global_selector_key, snapshot_selector_key,
    upload_binding_digest, upload_selector_key,
};
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
    CommitObjectV1, GcMarkPackV1, GcProgressSelectorV1, GlobalSelectorV1, OBJECT_SPACE, ObjectId,
    PreparedPublication, RECEIPT_TREE_FANOUT, RECEIPT_TREE_LEAF_ENTRIES, ReceiptTreeEdit,
    ReceiptTreeRoot, RepositoryRootV1, SelectorExpectation, SnapshotRole, SnapshotSelectorId,
    SnapshotSelectorV1, SnapshotTargetV1, StateCell, StateCellRef, StateKeyRef, StateSource,
    StateTreeMutation, StateValueRef, UntrackedValueRef, UploadBindingRef, UploadPartV1,
    UploadProgressV1, UploadSelectorV1, VisibleStateRow, discover_sweep_plan, edit_state_tree,
    encode_state_key, encode_state_value, load_change, load_commit, open_coherent_view,
    page_changes, page_commits, prepare_upload_completion, put_change_catalog_entries,
    put_commit_catalog_entries, state_point, state_range,
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

async fn sweep<S: Storage>(storage: &S, branch_id: CanonicalBranchId) {
    let view = open_coherent_view(storage, branch_id)
        .await
        .expect("sweep view");
    let plan = discover_sweep_plan(&view).await.expect("sweep discovery");
    let mut publication = PreparedPublication::from_global_epoch(&view).expect("sweep epoch");
    publication.apply_sweep_plan(plan).expect("sweep proof");
    drop(view);
    publication.commit(storage).await.expect("sweep commit");
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

struct CrashOracleAllocator;

static CRASH_ALLOCATIONS_ENABLED: AtomicBool = AtomicBool::new(false);
static CRASH_ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static CRASH_ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static CRASH_ORACLE_ALLOCATOR: CrashOracleAllocator = CrashOracleAllocator;

unsafe impl GlobalAlloc for CrashOracleAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if CRASH_ALLOCATIONS_ENABLED.load(Ordering::Relaxed) {
            CRASH_ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            CRASH_ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: forwards the exact allocation layout to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: `pointer` and `layout` originate from the system allocator above.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if CRASH_ALLOCATIONS_ENABLED.load(Ordering::Relaxed) && new_size > layout.size() {
            CRASH_ALLOCATED_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            CRASH_ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: forwards the allocation and both sizes to the system allocator.
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

fn begin_crash_allocation_profile() {
    CRASH_ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    CRASH_ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    CRASH_ALLOCATIONS_ENABLED.store(true, Ordering::Relaxed);
}

fn end_crash_allocation_profile() -> (u64, u64) {
    CRASH_ALLOCATIONS_ENABLED.store(false, Ordering::Relaxed);
    (
        CRASH_ALLOCATED_BYTES.load(Ordering::Relaxed),
        CRASH_ALLOCATION_CALLS.load(Ordering::Relaxed),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InjectedCrash {
    BeforeDurableCommit = 1,
    AfterDurableCommit = 2,
}

#[derive(Clone, Copy, Debug, Default)]
struct CrashIoSnapshot {
    begin_reads: u64,
    begin_writes: u64,
    get_calls: u64,
    get_keys: u64,
    get_value_bytes: u64,
    scan_calls: u64,
    scan_entries: u64,
    scan_value_bytes: u64,
    durable_commits: u64,
    put_entries: u64,
    deleted_entries: u64,
    written_bytes: u64,
}

impl CrashIoSnapshot {
    fn delta(self, before: Self) -> Self {
        Self {
            begin_reads: self.begin_reads.saturating_sub(before.begin_reads),
            begin_writes: self.begin_writes.saturating_sub(before.begin_writes),
            get_calls: self.get_calls.saturating_sub(before.get_calls),
            get_keys: self.get_keys.saturating_sub(before.get_keys),
            get_value_bytes: self.get_value_bytes.saturating_sub(before.get_value_bytes),
            scan_calls: self.scan_calls.saturating_sub(before.scan_calls),
            scan_entries: self.scan_entries.saturating_sub(before.scan_entries),
            scan_value_bytes: self
                .scan_value_bytes
                .saturating_sub(before.scan_value_bytes),
            durable_commits: self.durable_commits.saturating_sub(before.durable_commits),
            put_entries: self.put_entries.saturating_sub(before.put_entries),
            deleted_entries: self.deleted_entries.saturating_sub(before.deleted_entries),
            written_bytes: self.written_bytes.saturating_sub(before.written_bytes),
        }
    }
}

#[derive(Debug, Default)]
struct CrashIoCounters {
    begin_reads: AtomicU64,
    begin_writes: AtomicU64,
    get_calls: AtomicU64,
    get_keys: AtomicU64,
    get_value_bytes: AtomicU64,
    scan_calls: AtomicU64,
    scan_entries: AtomicU64,
    scan_value_bytes: AtomicU64,
    durable_commits: AtomicU64,
    put_entries: AtomicU64,
    deleted_entries: AtomicU64,
    written_bytes: AtomicU64,
}

impl CrashIoCounters {
    fn snapshot(&self) -> CrashIoSnapshot {
        CrashIoSnapshot {
            begin_reads: self.begin_reads.load(Ordering::Relaxed),
            begin_writes: self.begin_writes.load(Ordering::Relaxed),
            get_calls: self.get_calls.load(Ordering::Relaxed),
            get_keys: self.get_keys.load(Ordering::Relaxed),
            get_value_bytes: self.get_value_bytes.load(Ordering::Relaxed),
            scan_calls: self.scan_calls.load(Ordering::Relaxed),
            scan_entries: self.scan_entries.load(Ordering::Relaxed),
            scan_value_bytes: self.scan_value_bytes.load(Ordering::Relaxed),
            durable_commits: self.durable_commits.load(Ordering::Relaxed),
            put_entries: self.put_entries.load(Ordering::Relaxed),
            deleted_entries: self.deleted_entries.load(Ordering::Relaxed),
            written_bytes: self.written_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug)]
struct CrashStorage {
    inner: Memory,
    injection: Arc<AtomicU8>,
    io: Arc<CrashIoCounters>,
}

#[derive(Clone)]
struct CrashRead {
    inner: MemoryRead,
    io: Arc<CrashIoCounters>,
}

struct CrashWrite {
    inner: MemoryWrite,
    injection: Arc<AtomicU8>,
    io: Arc<CrashIoCounters>,
}

impl CrashStorage {
    fn new() -> Self {
        Self {
            inner: Memory::new(),
            injection: Arc::new(AtomicU8::new(0)),
            io: Arc::new(CrashIoCounters::default()),
        }
    }

    fn reopen(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            injection: Arc::new(AtomicU8::new(0)),
            io: self.io.clone(),
        }
    }

    fn inject_once(&self, crash: InjectedCrash) {
        assert_eq!(
            self.injection.swap(crash as u8, Ordering::SeqCst),
            0,
            "one crash may be armed at a time"
        );
    }

    fn io_snapshot(&self) -> CrashIoSnapshot {
        self.io.snapshot()
    }
}

impl StorageRead for CrashRead {
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }

    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        self.io.get_calls.fetch_add(1, Ordering::Relaxed);
        self.io.get_keys.fetch_add(
            requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum(),
            Ordering::Relaxed,
        );
        let result = self.inner.get_many(requests).await?;
        self.io.get_value_bytes.fetch_add(
            result
                .values
                .iter()
                .filter_map(|value| match value {
                    Some(StorageProjectedValue::FullValue(bytes)) => Some(bytes.len() as u64),
                    _ => None,
                })
                .sum(),
            Ordering::Relaxed,
        );
        Ok(result)
    }

    async fn scan(
        &self,
        space: crate::storage::StorageSpace,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        self.io.scan_calls.fetch_add(1, Ordering::Relaxed);
        let chunk = self.inner.scan(space, range, options).await?;
        self.io
            .scan_entries
            .fetch_add(chunk.entries.len() as u64, Ordering::Relaxed);
        self.io.scan_value_bytes.fetch_add(
            chunk
                .entries
                .iter()
                .map(|entry| match &entry.value {
                    StorageProjectedValue::KeyOnly => 0,
                    StorageProjectedValue::FullValue(bytes) => bytes.len() as u64,
                })
                .sum(),
            Ordering::Relaxed,
        );
        Ok(chunk)
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
        let crash = self.injection.swap(0, Ordering::SeqCst);
        if crash == InjectedCrash::BeforeDurableCommit as u8 {
            self.inner.rollback().await?;
            return Err(StorageError::Io(
                "injected crash before durable adapter commit".to_string(),
            ));
        }
        let committed = self.inner.commit().await?;
        self.io.durable_commits.fetch_add(1, Ordering::Relaxed);
        self.io
            .put_entries
            .fetch_add(committed.stats.put_entries, Ordering::Relaxed);
        self.io
            .deleted_entries
            .fetch_add(committed.stats.deleted_entries, Ordering::Relaxed);
        self.io
            .written_bytes
            .fetch_add(committed.stats.written_bytes, Ordering::Relaxed);
        if crash == InjectedCrash::AfterDurableCommit as u8 {
            return Err(StorageError::Io(
                "injected crash after durable adapter commit".to_string(),
            ));
        }
        Ok(committed)
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

impl Storage for CrashStorage {
    type Read<'a> = CrashRead;
    type Write<'a> = CrashWrite;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.io.begin_reads.fetch_add(1, Ordering::Relaxed);
        Ok(CrashRead {
            inner: self.inner.begin_read(options).await?,
            io: self.io.clone(),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.io.begin_writes.fetch_add(1, Ordering::Relaxed);
        Ok(CrashWrite {
            inner: self.inner.begin_write(options).await?,
            injection: self.injection.clone(),
            io: self.io.clone(),
        })
    }
}

async fn seed_crash_storage(storage: &CrashStorage, seed: &SeedData) {
    seed_storage(storage, seed).await;
}

async fn raw_selector<S: Storage>(storage: &S, key: Bytes) -> Option<Bytes> {
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("selector read");
    let keys = [Key(key)];
    let result = read
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("selector point");
    match result.values.as_slice() {
        [Some(StorageProjectedValue::FullValue(bytes))] => Some(bytes.clone()),
        [None] => None,
        other => panic!("unexpected selector projection: {other:?}"),
    }
}

async fn prepare_state_publication(
    storage: &CrashStorage,
    identity: u8,
    primary_key: &str,
    cell: &str,
) -> (PreparedPublication, Vec<u8>) {
    let view = open_coherent_view(storage, build_seed().branch_id)
        .await
        .expect("publication view");
    let (key, value) = state_entry(primary_key, StateCellRef::Value(cell), identity, &[]);
    let mutation = if state_point(&view, &key, false)
        .await
        .expect("existing point")
        .is_some()
    {
        StateTreeMutation::update(key.clone(), value)
    } else {
        StateTreeMutation::insert(key.clone(), value)
    };
    let edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![mutation],
        view.read(),
    )
    .await
    .expect("state path copy");
    let transition = branch_transition(&view, edit, identity).await;
    let mut publication = PreparedPublication::from_branch_view(&view).expect("publication");
    publication
        .publish_state_transition(&view, transition)
        .await
        .expect("state transition");
    drop(view);
    (publication, key)
}

fn current_rss_bytes() -> u64 {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    let Some(pages) = statm.split_whitespace().nth(1) else {
        return 0;
    };
    pages.parse::<u64>().unwrap_or(0).saturating_mul(4096)
}

fn process_cpu_ticks() -> u64 {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return 0;
    };
    let Some(after_name) = stat.rsplit_once(')').map(|(_, tail)| tail) else {
        return 0;
    };
    let mut fields = after_name.split_whitespace();
    let user = fields.nth(11).and_then(|value| value.parse::<u64>().ok());
    let system = fields.next().and_then(|value| value.parse::<u64>().ok());
    user.unwrap_or(0).saturating_add(system.unwrap_or(0))
}

fn print_recovery_profile(
    phase: &str,
    crash: InjectedCrash,
    wall_micros: u128,
    cpu_ticks: u64,
    allocations: (u64, u64),
    rss_before: u64,
    rss_after: u64,
    io: CrashIoSnapshot,
) {
    println!(
        "forktree_crash_recovery,phase={phase},crash={crash:?},wall_us={wall_micros},cpu_ticks={cpu_ticks},alloc_bytes={},alloc_calls={},rss_before_bytes={rss_before},rss_after_bytes={rss_after},begin_reads={},begin_writes={},get_calls={},get_keys={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},durable_commits={},put_entries={},deleted_entries={},written_bytes={},disk_bytes=0",
        allocations.0,
        allocations.1,
        io.begin_reads,
        io.begin_writes,
        io.get_calls,
        io.get_keys,
        io.get_value_bytes,
        io.scan_calls,
        io.scan_entries,
        io.scan_value_bytes,
        io.durable_commits,
        io.put_entries,
        io.deleted_entries,
        io.written_bytes,
    );
}

async fn prepare_snapshot_publication(
    storage: &CrashStorage,
    role: SnapshotRole,
    selector_id: SnapshotSelectorId,
) -> (PreparedPublication, ObjectId) {
    let seed = build_seed();
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .expect("snapshot view");
    let mut publication = PreparedPublication::from_global_epoch(&view).expect("snapshot");
    let target_id = publication
        .publish_current_snapshot_pin(&view, role, selector_id, SelectorExpectation::Absent)
        .expect("snapshot pin");
    drop(view);
    (publication, target_id)
}

async fn prepare_upload_completion_publication(
    storage: &CrashStorage,
    upload: &UploadData,
) -> (PreparedPublication, Vec<u8>, ObjectId) {
    let seed = build_seed();
    let view = open_coherent_view(storage, seed.branch_id)
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
    let (key, value) = state_entry(
        "crash-blob",
        StateCellRef::Value("blob"),
        0xa0,
        &[manifest_id],
    );
    let edit = edit_state_tree(
        view.branch_snapshot().local_state_root,
        vec![StateTreeMutation::insert(key.clone(), value)],
        view.read(),
    )
    .await
    .expect("blob state edit");
    let transition = branch_transition(&view, edit, 0xa0).await;
    let mut publication = PreparedPublication::from_branch_view(&view).expect("completion");
    publication
        .publish_completed_upload(&view, completion, transition)
        .await
        .expect("completion handoff");
    drop(view);
    (publication, key, manifest_id)
}

async fn prepare_upload_abort_publication(
    storage: &CrashStorage,
    upload: &UploadData,
) -> PreparedPublication {
    let seed = build_seed();
    let view = open_coherent_view(storage, seed.branch_id)
        .await
        .expect("abort view");
    let raw = raw_selector(
        storage,
        upload_selector_key(&upload.upload_id).expect("upload key"),
    )
    .await
    .expect("upload selector");
    let mut publication = PreparedPublication::from_global_epoch(&view).expect("abort");
    publication
        .abort_upload(&upload.selector, raw)
        .expect("abort selector");
    drop(view);
    publication
}

async fn assert_profiled_reopen(
    storage: &CrashStorage,
    phase: &str,
    crash: InjectedCrash,
    state: Option<(&[u8], Option<&str>)>,
) {
    let before_io = storage.io_snapshot();
    let rss_before = current_rss_bytes();
    let cpu_before = process_cpu_ticks();
    begin_crash_allocation_profile();
    let started = Instant::now();
    let reopened = storage.reopen();
    let view = open_coherent_view(&reopened, build_seed().branch_id)
        .await
        .expect("recovery coherent view");
    if let Some((key, expected)) = state {
        let actual = state_point(&view, key, false)
            .await
            .expect("recovery point")
            .map(|row| row.value.cell);
        match (actual, expected) {
            (None, None) => {}
            (Some(StateCell::Value(actual)), Some(expected)) => assert_eq!(actual, expected),
            other => panic!("unexpected recovered state: {other:?}"),
        }
    }
    drop(view);
    let wall_micros = started.elapsed().as_micros();
    let allocations = end_crash_allocation_profile();
    let cpu_ticks = process_cpu_ticks().saturating_sub(cpu_before);
    let rss_after = current_rss_bytes();
    let io = storage.io_snapshot().delta(before_io);
    print_recovery_profile(
        phase,
        crash,
        wall_micros,
        cpu_ticks,
        allocations,
        rss_before,
        rss_after,
        io,
    );
}

#[tokio::test]
async fn deterministic_crash_recovery_publication_oracle() {
    let seed = build_seed();
    for crash in [
        InjectedCrash::BeforeDurableCommit,
        InjectedCrash::AfterDurableCommit,
    ] {
        let storage = CrashStorage::new();
        seed_crash_storage(&storage, &seed).await;
        let old_global = raw_selector(&storage, global_selector_key())
            .await
            .expect("old global");
        let old_branch = raw_selector(&storage, branch_selector_key(seed.branch_id))
            .await
            .expect("old branch");
        let (publication, key) =
            prepare_state_publication(&storage, 0x70, "crash-row", "new").await;
        let (stale, _) = prepare_state_publication(&storage, 0x70, "crash-row", "new").await;
        storage.inject_once(crash);
        assert!(publication.commit(&storage).await.is_err());

        let reopened = storage.reopen();
        let global = raw_selector(&reopened, global_selector_key())
            .await
            .expect("reopened global");
        let branch = raw_selector(&reopened, branch_selector_key(seed.branch_id))
            .await
            .expect("reopened branch");
        let is_new = crash == InjectedCrash::AfterDurableCommit;
        assert_eq!(global == old_global, !is_new);
        assert_eq!(branch == old_branch, !is_new);
        assert_eq!(
            global == old_global,
            branch == old_branch,
            "selector pair mixed"
        );

        let view = open_coherent_view(&reopened, seed.branch_id)
            .await
            .expect("reopened graph");
        assert_eq!(
            load_commit(&view, CommitId::from_bytes(raw_id(0x70)))
                .await
                .expect("commit lookup")
                .is_some(),
            is_new
        );
        assert_eq!(
            load_change(&view, ChangeId::from_bytes(raw_id(0x71)))
                .await
                .expect("change lookup")
                .is_some(),
            is_new
        );
        drop(view);
        assert_profiled_reopen(
            &storage,
            "transaction_catalog_selector_pair",
            crash,
            Some((&key, is_new.then_some("new"))),
        )
        .await;

        let stale_result = stale.commit(&storage).await;
        if is_new {
            assert!(matches!(
                stale_result,
                Err(StorageError::PreconditionFailed(_))
            ));
        } else {
            stale_result.expect("old view remains a valid exact-CAS writer");
        }
    }

    for (index, (phase, role)) in [
        ("checkpoint", SnapshotRole::Checkpoint),
        ("restore", SnapshotRole::Recovery),
        ("undo", SnapshotRole::Undo),
        ("redo", SnapshotRole::Redo),
    ]
    .into_iter()
    .enumerate()
    {
        for crash in [
            InjectedCrash::BeforeDurableCommit,
            InjectedCrash::AfterDurableCommit,
        ] {
            let storage = CrashStorage::new();
            seed_crash_storage(&storage, &seed).await;
            let selector_id = SnapshotSelectorId::from_bytes(raw_id(0xb0 + index as u8));
            let selector_key = snapshot_selector_key(role, selector_id);
            let old_global = raw_selector(&storage, global_selector_key())
                .await
                .expect("old global");
            let (publication, target_id) =
                prepare_snapshot_publication(&storage, role, selector_id).await;
            let (stale, _) = prepare_snapshot_publication(&storage, role, selector_id).await;
            storage.inject_once(crash);
            assert!(publication.commit(&storage).await.is_err());
            let is_new = crash == InjectedCrash::AfterDurableCommit;
            assert_eq!(
                raw_selector(&storage.reopen(), selector_key)
                    .await
                    .is_some(),
                is_new
            );
            assert_eq!(
                raw_selector(&storage.reopen(), global_selector_key())
                    .await
                    .expect("global")
                    == old_global,
                !is_new
            );
            assert_eq!(object_present(&storage.reopen(), target_id).await, is_new);
            assert_profiled_reopen(&storage, phase, crash, None).await;
            let stale_result = stale.commit(&storage).await;
            if is_new {
                assert!(matches!(
                    stale_result,
                    Err(StorageError::PreconditionFailed(_))
                ));
            } else {
                stale_result.expect("pre-crash role publication retry");
            }
        }
    }

    for (phase, replay_redo) in [
        ("restore_state_transition", false),
        ("undo_state_transition", false),
        ("redo_state_transition", true),
    ] {
        for crash in [
            InjectedCrash::BeforeDurableCommit,
            InjectedCrash::AfterDurableCommit,
        ] {
            let storage = CrashStorage::new();
            seed_crash_storage(&storage, &seed).await;
            let (first, key) = prepare_state_publication(&storage, 0x80, "history-row", "v1").await;
            first.commit(&storage).await.expect("publish v1");
            let (second, _) = prepare_state_publication(&storage, 0x82, "history-row", "v2").await;
            second.commit(&storage).await.expect("publish v2");
            if replay_redo {
                let (undo, _) =
                    prepare_state_publication(&storage, 0x84, "history-row", "v1").await;
                undo.commit(&storage).await.expect("publish undo baseline");
            }
            let before = if replay_redo { "v1" } else { "v2" };
            let after = if replay_redo { "v2" } else { "v1" };
            let (replay, _) = prepare_state_publication(&storage, 0x86, "history-row", after).await;
            storage.inject_once(crash);
            assert!(replay.commit(&storage).await.is_err());
            let expected = if crash == InjectedCrash::AfterDurableCommit {
                after
            } else {
                before
            };
            assert_profiled_reopen(&storage, phase, crash, Some((&key, Some(expected)))).await;
        }
    }
}

#[tokio::test]
async fn deterministic_crash_recovery_upload_and_gc_oracle() {
    let seed = build_seed();
    for crash in [
        InjectedCrash::BeforeDurableCommit,
        InjectedCrash::AfterDurableCommit,
    ] {
        let upload = make_upload();
        let storage = CrashStorage::new();
        seed_crash_storage(&storage, &seed).await;
        let view = open_coherent_view(&storage, seed.branch_id)
            .await
            .expect("upload view");
        let mut publication = PreparedPublication::from_global_epoch(&view).expect("upload");
        stage_upload(&mut publication, &upload);
        drop(view);
        storage.inject_once(crash);
        assert!(publication.commit(&storage).await.is_err());
        let is_new = crash == InjectedCrash::AfterDurableCommit;
        assert_eq!(
            raw_selector(
                &storage.reopen(),
                upload_selector_key(&upload.upload_id).expect("upload key")
            )
            .await
            .is_some(),
            is_new
        );
        assert_eq!(
            object_present(&storage.reopen(), upload.progress_id).await,
            is_new
        );
        assert_profiled_reopen(&storage, "upload_part", crash, None).await;
    }

    for crash in [
        InjectedCrash::BeforeDurableCommit,
        InjectedCrash::AfterDurableCommit,
    ] {
        let upload = make_upload();
        let storage = CrashStorage::new();
        seed_crash_storage(&storage, &seed).await;
        let view = open_coherent_view(&storage, seed.branch_id)
            .await
            .expect("upload view");
        let mut initial = PreparedPublication::from_global_epoch(&view).expect("upload");
        stage_upload(&mut initial, &upload);
        drop(view);
        initial.commit(&storage).await.expect("upload seed");
        let (completion, key, manifest_id) =
            prepare_upload_completion_publication(&storage, &upload).await;
        storage.inject_once(crash);
        assert!(completion.commit(&storage).await.is_err());
        let is_new = crash == InjectedCrash::AfterDurableCommit;
        assert_eq!(
            raw_selector(
                &storage.reopen(),
                upload_selector_key(&upload.upload_id).expect("upload key")
            )
            .await
            .is_none(),
            is_new
        );
        assert_eq!(object_present(&storage.reopen(), manifest_id).await, is_new);
        assert_profiled_reopen(
            &storage,
            "upload_completion",
            crash,
            Some((&key, is_new.then_some("blob"))),
        )
        .await;
    }

    for crash in [
        InjectedCrash::BeforeDurableCommit,
        InjectedCrash::AfterDurableCommit,
    ] {
        let upload = make_upload();
        let storage = CrashStorage::new();
        seed_crash_storage(&storage, &seed).await;
        let view = open_coherent_view(&storage, seed.branch_id)
            .await
            .expect("upload view");
        let mut initial = PreparedPublication::from_global_epoch(&view).expect("upload");
        stage_upload(&mut initial, &upload);
        drop(view);
        initial.commit(&storage).await.expect("upload seed");
        let abort = prepare_upload_abort_publication(&storage, &upload).await;
        storage.inject_once(crash);
        assert!(abort.commit(&storage).await.is_err());
        let is_new = crash == InjectedCrash::AfterDurableCommit;
        assert_eq!(
            raw_selector(
                &storage.reopen(),
                upload_selector_key(&upload.upload_id).expect("upload key")
            )
            .await
            .is_none(),
            is_new
        );
        assert_profiled_reopen(&storage, "upload_abort", crash, None).await;
    }

    for crash in [
        InjectedCrash::BeforeDurableCommit,
        InjectedCrash::AfterDurableCommit,
    ] {
        let storage = CrashStorage::new();
        seed_crash_storage(&storage, &seed).await;
        let target = SnapshotTargetV1 {
            role: SnapshotRole::Checkpoint,
            selector_id: SnapshotSelectorId::from_bytes(raw_id(0xc0)),
            branch_id: seed.branch_id,
            branch_snapshot_object_id: seed.branch_snapshot_id,
            semantic_commit_object_id: seed.commit_object_id,
        };
        let (target_id, _) = target.encode().expect("orphan target");
        let view = open_coherent_view(&storage, seed.branch_id)
            .await
            .expect("orphan view");
        let mut orphan = PreparedPublication::from_global_epoch(&view).expect("orphan");
        orphan.stage_snapshot_target(target).expect("stage orphan");
        drop(view);
        orphan.commit(&storage).await.expect("orphan seed");
        let view = open_coherent_view(&storage, seed.branch_id)
            .await
            .expect("GC view");
        let plan = discover_sweep_plan(&view).await.expect("GC plan");
        assert!(plan.orphan_object_ids.contains(&target_id));
        let old_global = view.raw_global_selector().clone();
        let mut gc = PreparedPublication::from_global_epoch(&view).expect("GC");
        gc.apply_sweep_plan(plan).expect("GC proof");
        drop(view);
        storage.inject_once(crash);
        assert!(gc.commit(&storage).await.is_err());
        let is_new = crash == InjectedCrash::AfterDurableCommit;
        assert_eq!(object_present(&storage.reopen(), target_id).await, !is_new);
        assert_eq!(
            raw_selector(&storage.reopen(), global_selector_key())
                .await
                .expect("global")
                == old_global,
            !is_new
        );
        assert_profiled_reopen(&storage, "gc_epoch_handoff", crash, None).await;
    }
}

#[tokio::test]
async fn deterministic_crash_recovery_corruption_oracle() {
    let seed = build_seed();

    let missing = Memory::new();
    seed_storage(&missing, &seed).await;
    let mut write = missing
        .begin_write(WriteOptions::default())
        .await
        .expect("missing-object write");
    write
        .delete_many(
            OBJECT_SPACE,
            &[Key(Bytes::copy_from_slice(
                seed.semantic_change_object_id.as_bytes(),
            ))],
        )
        .await
        .expect("delete selected member");
    write.commit().await.expect("commit missing object");
    assert!(open_coherent_view(&missing, seed.branch_id).await.is_err());

    let malformed = Memory::new();
    seed_storage(&malformed, &seed).await;
    let mut write = malformed
        .begin_write(WriteOptions::default())
        .await
        .expect("malformed-selector write");
    write
        .put_many(
            SELECTOR_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: Key(global_selector_key()),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"malformed"),
                    },
                }],
            },
        )
        .await
        .expect("malformed selector");
    write.commit().await.expect("commit malformed selector");
    assert!(
        open_coherent_view(&malformed, seed.branch_id)
            .await
            .is_err()
    );

    let mut forged = build_seed();
    let wrong_entry = CommitCatalogEntry {
        commit_object_id: forged.semantic_change_object_id,
    };
    let catalog = build_commit_catalog(&[(forged.commit_id, wrong_entry)])
        .expect("syntactically valid forged catalog");
    forged
        .objects
        .extend(catalog.objects)
        .expect("forged catalog");
    let repository = RepositoryRootV1 {
        commit_catalog_root: catalog.root.object_id,
        ..RepositoryRootV1::decode(
            seed.repository_root_id,
            seed.objects
                .get(seed.repository_root_id)
                .expect("seed root"),
        )
        .expect("seed repository")
    };
    let (repository_id, repository_bytes) = repository.encode().expect("forged root");
    forged.repository_root_id = repository_id;
    forged
        .objects
        .insert(repository_id, repository_bytes)
        .expect("forged root object");
    forged.global_selector.repository_root = repository_id;
    forged.global_selector.selector_generation += 1;
    let back_edge = Memory::new();
    seed_storage(&back_edge, &forged).await;
    assert!(
        open_coherent_view(&back_edge, seed.branch_id)
            .await
            .is_err()
    );

    let mut domain = build_seed();
    let bad_ref = ChangeObjectV1::BranchRef {
        change_id: ChangeId::from_bytes(raw_id(0xd0)),
        branch_id: domain.branch_id,
        before_semantic_head_commit_object_id: Some(domain.semantic_change_object_id),
        after_semantic_head_commit_object_id: Some(domain.commit_object_id),
        previous_ref_change_object_id: domain.branch_snapshot_id.into(),
        payload: b"wrong-domain".to_vec(),
    };
    let (bad_ref_id, bad_ref_bytes) = bad_ref.encode().expect("bad ref");
    domain
        .objects
        .insert(bad_ref_id, bad_ref_bytes)
        .expect("bad ref object");
    let bad_snapshot = BranchSnapshotV1 {
        latest_ref_change_object_id: Some(bad_ref_id),
        ..BranchSnapshotV1::decode(
            domain.branch_snapshot_id,
            domain
                .objects
                .get(domain.branch_snapshot_id)
                .expect("snapshot"),
        )
        .expect("snapshot")
    };
    let (bad_snapshot_id, bad_snapshot_bytes) = bad_snapshot.encode().expect("bad snapshot");
    domain
        .objects
        .insert(bad_snapshot_id, bad_snapshot_bytes)
        .expect("bad snapshot object");
    domain.branch_snapshot_id = bad_snapshot_id;
    domain.branch_selector.branch_snapshot_object_id = bad_snapshot_id;
    domain.branch_selector.selector_generation += 1;
    let wrong_domain = Memory::new();
    seed_storage(&wrong_domain, &domain).await;
    assert!(
        open_coherent_view(&wrong_domain, seed.branch_id)
            .await
            .is_err()
    );
}

/// Reader safe-point contract.
///
/// Removing the final durable selector makes an object logically unreachable
/// at epoch E. A sweep derived from that exact selector view may commit the
/// deletion only under the raw E selector CAS, rotating to E+1. Physical
/// reclamation is safe after that delete is durable and the adapter's active
/// read low-watermark is newer than the delete commit: every `StorageRead`
/// that could still expose the retired bytes has dropped. There is no clock
/// grace and no persisted adapter snapshot token. Cross-reopen retention is
/// represented only by authenticated owner selectors; process-local readers
/// pin their adapter snapshots by lifetime.
#[tokio::test]
async fn deterministic_reader_pin_safe_point_and_cursor_oracle() {
    let seed = build_seed();

    // Publication first, deletion second. A checkpoint target is selected,
    // observed by an old coherent read, released after a branch publication,
    // then reclaimed by a GC commit whose acknowledgement is lost.
    let storage = CrashStorage::new();
    seed_crash_storage(&storage, &seed).await;
    let checkpoint_id = SnapshotSelectorId::from_bytes(raw_id(0xe0));
    let (checkpoint, target_id) =
        prepare_snapshot_publication(&storage, SnapshotRole::Checkpoint, checkpoint_id).await;
    checkpoint.commit(&storage).await.expect("checkpoint pin");

    let old_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("old coherent reader");
    let old_catalog_root = old_view.repository_root().change_catalog_root;
    let old_resume = old_view.bind_resume_key(old_catalog_root, seed.semantic_change_id.as_bytes());
    assert_eq!(
        old_view
            .validate_resume_key(old_catalog_root, &old_resume)
            .expect("old cursor"),
        seed.semantic_change_id.as_bytes()
    );
    assert!(old_view.load_object_bytes(target_id).await.is_ok());

    let (branch_publication, new_key) =
        prepare_state_publication(&storage, 0xe2, "safe-point", "new").await;
    branch_publication
        .commit(&storage)
        .await
        .expect("advance branch and global selectors");
    let current = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("current view");
    assert!(matches!(
        current.validate_resume_key(old_catalog_root, &old_resume),
        Err(StorageError::InvalidCursor)
    ));
    let snapshot_key = snapshot_selector_key(SnapshotRole::Checkpoint, checkpoint_id);
    let raw_checkpoint = raw_selector(&storage, snapshot_key)
        .await
        .expect("checkpoint selector");
    let checkpoint_selector = SnapshotSelectorV1::decode(&raw_checkpoint).expect("checkpoint");
    let mut release = PreparedPublication::from_global_epoch(&current).expect("release");
    release
        .release_snapshot_pin(checkpoint_selector, raw_checkpoint)
        .expect("release checkpoint pin");
    drop(current);
    release.commit(&storage).await.expect("release commit");

    let gc_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("GC view");
    let plan = discover_sweep_plan(&gc_view).await.expect("GC plan");
    assert!(plan.orphan_object_ids.contains(&target_id));
    let mut gc = PreparedPublication::from_global_epoch(&gc_view).expect("GC publication");
    gc.apply_sweep_plan(plan).expect("GC proof");
    drop(gc_view);
    storage.inject_once(InjectedCrash::AfterDurableCommit);
    assert!(gc.commit(&storage).await.is_err());

    let reopened = storage.reopen();
    assert!(!object_present(&reopened, target_id).await);
    assert_profiled_reopen(
        &storage,
        "reader_pin_publication_then_delete",
        InjectedCrash::AfterDurableCommit,
        Some((&new_key, Some("new"))),
    )
    .await;
    assert!(
        old_view.load_object_bytes(target_id).await.is_ok(),
        "the old StorageRead snapshot must retain bytes deleted from the current view"
    );
    assert_eq!(
        old_view
            .validate_resume_key(old_catalog_root, &old_resume)
            .expect("bound old cursor remains valid while its view lives"),
        seed.semantic_change_id.as_bytes()
    );
    drop(old_view);
    let reopened_view = open_coherent_view(&reopened, seed.branch_id)
        .await
        .expect("post-safe-point reopen");
    assert!(matches!(
        reopened_view.validate_resume_key(old_catalog_root, &old_resume),
        Err(StorageError::InvalidCursor)
    ));

    // Deletion first, publication second. An abandoned object is visible to
    // an old reader but is not a root. GC wins the epoch CAS; the stale branch
    // publication must fail, then retry from the reopened view.
    let storage = CrashStorage::new();
    seed_crash_storage(&storage, &seed).await;
    let abandoned = SnapshotTargetV1 {
        role: SnapshotRole::Checkpoint,
        selector_id: SnapshotSelectorId::from_bytes(raw_id(0xe4)),
        branch_id: seed.branch_id,
        branch_snapshot_object_id: seed.branch_snapshot_id,
        semantic_commit_object_id: seed.commit_object_id,
    };
    let (abandoned_id, _) = abandoned.encode().expect("abandoned target");
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("abandoned-object view");
    let mut stage = PreparedPublication::from_global_epoch(&view).expect("stage orphan");
    stage
        .stage_snapshot_target(abandoned)
        .expect("stage abandoned object");
    drop(view);
    stage.commit(&storage).await.expect("durable orphan");

    let old_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("old orphan reader");
    assert!(old_view.load_object_bytes(abandoned_id).await.is_ok());
    let (stale_publication, stale_key) =
        prepare_state_publication(&storage, 0xe6, "delete-first", "new").await;
    let gc_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("delete-first GC view");
    let plan = discover_sweep_plan(&gc_view)
        .await
        .expect("delete-first plan");
    assert!(plan.orphan_object_ids.contains(&abandoned_id));
    let mut gc = PreparedPublication::from_global_epoch(&gc_view).expect("delete-first GC");
    gc.apply_sweep_plan(plan).expect("delete-first proof");
    drop(gc_view);
    storage.inject_once(InjectedCrash::AfterDurableCommit);
    assert!(gc.commit(&storage).await.is_err());
    assert!(matches!(
        stale_publication.commit(&storage).await,
        Err(StorageError::PreconditionFailed(_))
    ));
    assert!(!object_present(&storage.reopen(), abandoned_id).await);
    assert!(old_view.load_object_bytes(abandoned_id).await.is_ok());
    drop(old_view);
    let (retry, _) = prepare_state_publication(&storage, 0xe6, "delete-first", "new").await;
    retry.commit(&storage).await.expect("retry after GC epoch");
    assert_profiled_reopen(
        &storage,
        "reader_pin_delete_then_publication",
        InjectedCrash::AfterDurableCommit,
        Some((&stale_key, Some("new"))),
    )
    .await;
}

#[tokio::test]
async fn deterministic_durable_branch_upload_and_unpublished_pin_oracle() {
    let storage = CrashStorage::new();
    let (seed, child_branch, _) = seed_with_disposable_branch(&storage).await;
    let child_view = open_coherent_view(&storage, child_branch)
        .await
        .expect("child branch view");
    let child_snapshot_id = child_view.branch_selector().branch_snapshot_object_id;
    drop(child_view);

    let upload = make_upload();
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("upload view");
    let mut upload_publication = PreparedPublication::from_global_epoch(&view).expect("upload");
    stage_upload(&mut upload_publication, &upload);
    drop(view);
    upload_publication
        .commit(&storage)
        .await
        .expect("open upload");

    let abandoned = SnapshotTargetV1 {
        role: SnapshotRole::Checkpoint,
        selector_id: SnapshotSelectorId::from_bytes(raw_id(0xea)),
        branch_id: seed.branch_id,
        branch_snapshot_object_id: seed.branch_snapshot_id,
        semantic_commit_object_id: seed.commit_object_id,
    };
    let (abandoned_id, _) = abandoned.encode().expect("abandoned target");
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("abandoned view");
    let mut stage = PreparedPublication::from_global_epoch(&view).expect("stage abandoned");
    stage
        .stage_snapshot_target(abandoned)
        .expect("stage abandoned target");
    drop(view);
    stage.commit(&storage).await.expect("abandoned object");

    sweep(&storage, seed.branch_id).await;
    let reopened = storage.reopen();
    let child = open_coherent_view(&reopened, child_branch)
        .await
        .expect("selected child closure survives");
    assert_eq!(
        child.branch_selector().branch_snapshot_object_id,
        child_snapshot_id
    );
    drop(child);
    assert!(object_present(&reopened, child_snapshot_id).await);
    assert!(object_present(&reopened, upload.progress_id).await);
    assert!(object_present(&reopened, upload.chunk_id).await);
    assert!(!object_present(&reopened, abandoned_id).await);

    let upload_view = open_coherent_view(&reopened, seed.branch_id)
        .await
        .expect("upload closure view");
    prepare_upload_completion(
        &upload_view,
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
    .expect("open upload selector pins its complete authenticated receipt closure");
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

    fn scan(
        &self,
        space: crate::storage::StorageSpace,
        range: KeyRange,
        options: ScanOptions,
    ) -> impl Future<Output = Result<ScanChunk, StorageError>> + Send {
        self.inner.scan(space, range, options)
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

#[tokio::test]
async fn coherent_open_uses_one_read_and_authenticates_the_complete_graph() {
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
    assert!(open_coherent_view(&storage, seed.branch_id).await.is_err());
}

#[tokio::test]
async fn coherent_open_rejects_ref_targets_outside_the_commit_domain() {
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
    assert!(open_coherent_view(&storage, seed.branch_id).await.is_err());
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
    let gc_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("GC view");
    let stale_plan = discover_sweep_plan(&gc_view).await.expect("stale plan");
    let mut publish = PreparedPublication::from_global_epoch(&publish_view).expect("publish");
    stage_upload(&mut publish, &upload);
    let mut stale_gc = PreparedPublication::from_global_epoch(&gc_view).expect("GC");
    stale_gc.apply_sweep_plan(stale_plan).expect("sweep proof");
    drop(publish_view);
    drop(gc_view);
    publish.commit(&storage).await.expect("receipt first");
    assert!(matches!(
        stale_gc.commit(&storage).await,
        Err(StorageError::PreconditionFailed(_))
    ));
    assert!(object_present(&storage, upload.progress_id).await);

    let storage = Memory::new();
    seed_storage(&storage, &seed).await;
    let publish_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("publish view");
    let gc_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("GC view");
    let mut stale_publish = PreparedPublication::from_global_epoch(&publish_view).expect("publish");
    stage_upload(&mut stale_publish, &upload);
    let plan = discover_sweep_plan(&gc_view).await.expect("plan");
    let mut gc = PreparedPublication::from_global_epoch(&gc_view).expect("GC");
    gc.apply_sweep_plan(plan).expect("proof");
    drop(publish_view);
    drop(gc_view);
    gc.commit(&storage).await.expect("GC first");
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

async fn seed_with_disposable_branch<S: Storage>(
    storage: &S,
) -> (SeedData, CanonicalBranchId, ChangeId) {
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
    let commit_catalog_edit = retire_commit_catalog_entries(
        view.repository_root().commit_catalog_root,
        &[CommitId::from_bytes(raw_id(0x80))],
        view.read(),
    )
    .await
    .expect("retire commit");
    let change_catalog_edit = retire_change_catalog_entries(
        view.repository_root().change_catalog_root,
        &[initial_ref_id, ChangeId::from_bytes(raw_id(0x81))],
        view.read(),
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
    let mut release = PreparedPublication::from_global_epoch(&view).expect("release");
    release
        .release_snapshot_pin(selector, raw)
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
    let gc_view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("GC view");
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
    let plan = discover_sweep_plan(&gc_view).await.expect("stale plan");
    let mut stale_gc = PreparedPublication::from_global_epoch(&gc_view).expect("GC");
    stale_gc.apply_sweep_plan(plan).expect("proof");
    drop(publish_view);
    drop(gc_view);
    root_only.commit(&storage).await.expect("root first");
    assert!(matches!(
        stale_gc.commit(&storage).await,
        Err(StorageError::PreconditionFailed(_))
    ));

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
    let gc_view = open_coherent_view(&inverse, seed.branch_id)
        .await
        .expect("GC view");
    let mut stale_root = PreparedPublication::from_global_epoch(&publish_view).expect("root move");
    stale_root
        .publish_current_snapshot_pin(
            &publish_view,
            target.role,
            target.selector_id,
            SelectorExpectation::Absent,
        )
        .expect("root selector");
    let plan = discover_sweep_plan(&gc_view).await.expect("GC plan");
    let mut gc = PreparedPublication::from_global_epoch(&gc_view).expect("GC");
    gc.apply_sweep_plan(plan).expect("proof");
    drop(publish_view);
    drop(gc_view);
    gc.commit(&inverse).await.expect("GC first");
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
    let mark_pack = GcMarkPackV1 {
        object_ids: vec![target_id],
        next_pack_object_id: None,
    };
    let progress_id = roles
        .publish_gc_progress(
            &mark_pack,
            None,
            view.global_selector().epoch,
            1,
            SelectorExpectation::Absent,
        )
        .expect("GC selector");
    drop(view);
    roles.commit(&storage).await.expect("all roles");
    sweep(&storage, seed.branch_id).await;
    assert!(object_present(&storage, target_id).await);
    assert!(object_present(&storage, progress_id).await);
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("release GC progress view");
    let keys = [Key(gc_progress_selector_key())];
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
        .expect("GC selector read");
    let raw = match loaded.values.as_slice() {
        [Some(crate::storage::ProjectedValue::FullValue(bytes))] => bytes.clone(),
        other => panic!("expected GC selector, got {other:?}"),
    };
    let selector = GcProgressSelectorV1::decode(&raw).expect("GC selector");
    let mut release = PreparedPublication::from_global_epoch(&view).expect("GC release");
    release
        .release_gc_progress(selector, raw)
        .expect("release GC progress");
    drop(view);
    release.commit(&storage).await.expect("GC release commit");
    sweep(&storage, seed.branch_id).await;
    assert!(!object_present(&storage, progress_id).await);
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
    let view = open_coherent_view(&storage, seed.branch_id)
        .await
        .expect("view after corruption");
    assert!(discover_sweep_plan(&view).await.is_err());
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
