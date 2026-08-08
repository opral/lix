#![allow(clippy::large_futures)]

//! Frozen ForkTree Stage2 multimedia acceptance harness.
//!
//! This source intentionally does not compile before the test-only ForkTree
//! accounting facade lands. User-visible work uses only Engine/Session APIs.
//! Layout and GC observation uses only `lix::storage_bench::forktree`; raw
//! ForkTree spaces and crate-private modules are forbidden.

use std::alloc::{GlobalAlloc, Layout};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use lix::integration::{Engine, SessionContext};
use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, ProjectedValue,
    PutBatch, ReadOptions, ScanChunk, ScanCursor, Storage, StorageError, StorageRead,
    StorageScanSource, StorageSpace, StorageWrite, WriteOptions,
};
use lix::storage_bench::forktree::{
    ForkTreeGcRunSummary, ForkTreeInventory, GcBudget, GcTerminalStatus, inventory,
    run_gc_to_completion,
};
use lix::{
    CreateBranchOptions, ExecuteOptions, ExecuteStatementMetadata, FILE_UPLOAD_PART_BYTES,
    MergeBranchOptions, Value, VerifiedRequestBlob,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};
use sha2::{Digest as _, Sha256};

const RANGE_BYTES: u64 = 4 * 1024;
const SEED: u64 = 0x89a3_10fd_4242_73c1;
const SOURCE_BRANCH_ID: &str = "01980000-0000-7000-8000-000000000064";
const RETAINED_BRANCH_ID: &str = "01980000-0000-7000-8000-000000000065";
const RETENTION_CHECKPOINTS: usize = 64;
const UPSERT_SQL: &str = "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
                          ON CONFLICT (path) DO UPDATE SET content = excluded.content";

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;

static ALLOCATION_ENABLED: AtomicBool = AtomicBool::new(false);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && ALLOCATION_ENABLED.load(Ordering::Relaxed) {
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, new_size) };
        if !replacement.is_null()
            && new_size > layout.size()
            && ALLOCATION_ENABLED.load(Ordering::Relaxed)
        {
            ALLOCATED_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        replacement
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct IoStats {
    begin_reads: u64,
    begin_writes: u64,
    get_many_calls: u64,
    get_many_keys: u64,
    get_many_found: u64,
    get_many_value_bytes: u64,
    scan_calls: u64,
    scan_rows: u64,
    scan_value_bytes: u64,
    put_batches: u64,
    puts: u64,
    delete_batches: u64,
    deletes: u64,
    logical_write_bytes: u64,
    commits: u64,
    backend_puts: u64,
    backend_deletes: u64,
    backend_written_bytes: u64,
    backend_storage_calls: u64,
}

#[derive(Clone)]
struct CountingStorage<S> {
    inner: S,
    stats: Arc<Mutex<IoStats>>,
}

struct CountingRead<R> {
    inner: R,
    stats: Arc<Mutex<IoStats>>,
}

struct CountingWrite<W> {
    inner: W,
    stats: Arc<Mutex<IoStats>>,
}

struct CountingScanSource<'a> {
    inner: ScanCursor<'a>,
    stats: Arc<Mutex<IoStats>>,
}

impl<S> CountingStorage<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            stats: Arc::new(Mutex::new(IoStats::default())),
        }
    }

    fn reset(&self) {
        *self.stats.lock().expect("I/O stats mutex") = IoStats::default();
    }

    fn snapshot(&self) -> IoStats {
        *self.stats.lock().expect("I/O stats mutex")
    }

    fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: Storage> Storage for CountingStorage<S> {
    type Read<'a>
        = CountingRead<S::Read<'a>>
    where
        Self: 'a;
    type Write<'a>
        = CountingWrite<S::Write<'a>>
    where
        Self: 'a;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.stats.lock().expect("I/O stats mutex").begin_reads += 1;
        Ok(CountingRead {
            inner: self.inner.begin_read(options).await?,
            stats: Arc::clone(&self.stats),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.stats.lock().expect("I/O stats mutex").begin_writes += 1;
        Ok(CountingWrite {
            inner: self.inner.begin_write(options).await?,
            stats: Arc::clone(&self.stats),
        })
    }
}

impl<R: StorageRead> StorageRead for CountingRead<R> {
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.get_many_calls += 1;
            stats.get_many_keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
        }
        let result = self.inner.get_many(requests).await?;
        let mut stats = self.stats.lock().expect("I/O stats mutex");
        for value in result.values.iter().flatten() {
            stats.get_many_found += 1;
            if let ProjectedValue::FullValue(value) = value {
                stats.get_many_value_bytes += value.len() as u64;
            }
        }
        Ok(result)
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        options: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        let order = options.order;
        self.stats.lock().expect("I/O stats mutex").scan_calls += 1;
        let inner = self.inner.begin_scan(space, range.clone(), options).await?;
        ScanCursor::from_source(
            range,
            order,
            CountingScanSource {
                inner,
                stats: Arc::clone(&self.stats),
            },
        )
    }
}

impl StorageScanSource for CountingScanSource<'_> {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            let chunk = self.inner.next_page(limit_rows).await?;
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.scan_rows += chunk.entries.len() as u64;
            stats.scan_value_bytes += chunk
                .entries
                .iter()
                .map(|entry| match &entry.value {
                    ProjectedValue::KeyOnly => 0,
                    ProjectedValue::FullValue(value) => value.len() as u64,
                })
                .sum::<u64>();
            drop(stats);
            Ok(chunk)
        })
    }
}

impl<W: StorageWrite> StorageWrite for CountingWrite<W> {
    async fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.put_batches += 1;
            stats.puts += entries.entries.len() as u64;
            stats.logical_write_bytes += entries
                .entries
                .iter()
                .map(|entry| (entry.key.0.len() + entry.value.bytes.len()) as u64)
                .sum::<u64>();
        }
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.delete_batches += 1;
            stats.deletes += keys.len() as u64;
        }
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        let stats = Arc::clone(&self.stats);
        let result = self.inner.commit().await?;
        let mut captured = stats.lock().expect("I/O stats mutex");
        captured.commits += 1;
        captured.backend_puts += result.stats.put_entries;
        captured.backend_deletes += result.stats.deleted_entries;
        captured.backend_written_bytes += result.stats.written_bytes;
        captured.backend_storage_calls += result.stats.storage_calls;
        drop(captured);
        Ok(result)
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

#[async_trait]
trait QualificationStorage: Storage + Clone + Send + Sync + 'static {
    async fn qualification_flush(&self);
}

#[async_trait]
impl QualificationStorage for RocksDB {
    async fn qualification_flush(&self) {
        self.flush().expect("flush qualification RocksDB");
    }
}

#[async_trait]
impl QualificationStorage for SlateDB {
    async fn qualification_flush(&self) {
        self.flush().await.expect("flush qualification SlateDB");
    }
}

#[derive(Clone, Copy, Debug)]
struct Profile {
    name: &'static str,
    path: &'static str,
    size_mib: usize,
    edit_percent: usize,
}

impl Profile {
    fn from_environment() -> Self {
        match std::env::var("LIX_STAGE2_MEDIA_PROFILE").as_deref() {
            Ok("image-64-1") => Self {
                name: "image-64-1",
                path: "/media/fixture.png",
                size_mib: 64,
                edit_percent: 1,
            },
            Ok("audio-64-1") => Self {
                name: "audio-64-1",
                path: "/media/fixture.flac",
                size_mib: 64,
                edit_percent: 1,
            },
            Ok("archive-512-10") => Self {
                name: "archive-512-10",
                path: "/media/fixture.tar",
                size_mib: 512,
                edit_percent: 10,
            },
            Ok("video-512-10") => Self {
                name: "video-512-10",
                path: "/media/fixture.mov",
                size_mib: 512,
                edit_percent: 10,
            },
            Ok(other) => panic!("unsupported LIX_STAGE2_MEDIA_PROFILE={other}"),
            Err(_) => panic!("LIX_STAGE2_MEDIA_PROFILE is required"),
        }
    }

    fn size(self) -> usize {
        self.size_mib * 1024 * 1024
    }
}

struct PreparedPayload {
    parts: Vec<Bytes>,
    size: usize,
    edit_offset: usize,
    edit_bytes: usize,
    base_blake3: String,
    base_sha256: String,
    edited_blake3: String,
    edited_sha256: String,
}

struct ReopenExpectation {
    main_branch_id: String,
    root_present: String,
    root_absent: String,
    edited_blake3: String,
    retained_inventory: ForkTreeInventory,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual ForkTree Stage2 multimedia acceptance"]
async fn stage2_multimedia_lifecycle() {
    let profile = Profile::from_environment();
    let database = required_database_path();
    assert!(
        !database.starts_with("/tmp"),
        "large-media database must use the roomy workspace filesystem"
    );
    match std::env::var("LIX_STAGE2_MEDIA_BACKEND").as_deref() {
        Ok("rocksdb") => {
            let expectation = qualify_before_reopen(
                "rocksdb",
                profile,
                &database,
                RocksDB::open(&database).expect("open qualification RocksDB"),
                None,
            )
            .await;
            let main_branch_id = qualify_after_reopen(
                "rocksdb",
                profile,
                &database,
                RocksDB::open(&database).expect("reopen qualification RocksDB"),
                None,
                expectation,
            )
            .await;
            qualify_final_reopen(
                "rocksdb",
                profile,
                &database,
                RocksDB::open(&database).expect("final reopen qualification RocksDB"),
                main_branch_id,
            )
            .await;
        }
        Ok("slatedb") => {
            let counters = SlateDBIoCounters::default();
            let expectation = qualify_before_reopen(
                "slatedb",
                profile,
                &database,
                SlateDB::open_with_io_counters(&database, counters.clone())
                    .expect("open qualification SlateDB"),
                Some(counters),
            )
            .await;
            let reopen_counters = SlateDBIoCounters::default();
            let main_branch_id = qualify_after_reopen(
                "slatedb",
                profile,
                &database,
                SlateDB::open_with_io_counters(&database, reopen_counters.clone())
                    .expect("reopen qualification SlateDB"),
                Some(reopen_counters),
                expectation,
            )
            .await;
            qualify_final_reopen(
                "slatedb",
                profile,
                &database,
                SlateDB::open(&database).expect("final reopen qualification SlateDB"),
                main_branch_id,
            )
            .await;
        }
        Ok(other) => panic!("backend must be rocksdb or slatedb, got {other}"),
        Err(_) => panic!("LIX_STAGE2_MEDIA_BACKEND is required"),
    }
}

async fn qualify_before_reopen<S>(
    backend: &str,
    profile: Profile,
    database: &Path,
    raw_storage: S,
    slate: Option<SlateDBIoCounters>,
) -> ReopenExpectation
where
    S: QualificationStorage,
{
    let payload = prepare_payload(profile);
    let storage = CountingStorage::new(raw_storage);
    let receipt = Engine::initialize(storage.clone())
        .await
        .expect("initialize Stage2 repository");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open Stage2 repository");
    let main = engine
        .open_session(&receipt.main_branch_id)
        .await
        .expect("open main session");

    measured(
        backend,
        profile,
        "upload",
        database,
        &storage,
        slate.as_ref(),
        upload_parts(&main, profile.path, &payload),
    )
    .await;
    assert_content(&main, profile.path, payload.size, &payload.base_blake3).await;
    let uploaded = measured_inventory(backend, profile, "uploaded", database, &engine).await;
    assert_eq!(uploaded.blob_manifests, 1);
    assert_eq!(uploaded.blob_manifest_logical_bytes, payload.size as u64);
    assert!(uploaded.blob_chunk_references > 0);
    assert!(uploaded.unique_blob_chunks > 0);
    assert_eq!(uploaded.unique_blob_payload_bytes, payload.size as u64);
    assert_eq!(uploaded.upload_selectors, 0);
    assert_eq!(uploaded.receipt_tree_nodes, 0);
    assert_eq!(uploaded.upload_parts, 0);

    measured(
        backend,
        profile,
        "base_checkpoint",
        database,
        &storage,
        slate.as_ref(),
        main.create_checkpoint(),
    )
    .await
    .expect("create base checkpoint");
    let checkpointed =
        measured_inventory(backend, profile, "base_checkpoint", database, &engine).await;
    assert_blob_inventory_unchanged("base checkpoint", &uploaded, &checkpointed);

    let branch = measured(
        backend,
        profile,
        "branch_without_edit",
        database,
        &storage,
        slate.as_ref(),
        main.create_branch(CreateBranchOptions {
            id: Some(SOURCE_BRANCH_ID.to_owned()),
            name: format!("Stage2 {} edit", profile.name),
            from_commit_id: None,
        }),
    )
    .await
    .expect("create source branch");
    let branched =
        measured_inventory(backend, profile, "branch_without_edit", database, &engine).await;
    assert_blob_inventory_unchanged("branch creation", &checkpointed, &branched);
    let source = engine
        .open_session(&branch.id)
        .await
        .expect("open source branch");
    assert_content(&source, profile.path, payload.size, &payload.base_blake3).await;

    let base = source
        .read_file_content(profile.path.to_owned(), None)
        .await
        .expect("read base content")
        .expect("base content exists");
    let verified = VerifiedRequestBlob::verify(base.into_content());
    let suffix_bytes = payload.size - payload.edit_offset - payload.edit_bytes;
    let (edited, provenance) = verified
        .reconstruct_splice(
            &payload.base_sha256,
            &payload.edited_sha256,
            payload.edit_offset,
            suffix_bytes,
            vec![0xa5; payload.edit_bytes].into(),
        )
        .expect("reconstruct authenticated edit");
    let result = measured(
        backend,
        profile,
        "edit_publish",
        database,
        &storage,
        slate.as_ref(),
        source.execute_with_options_and_metadata(
            UPSERT_SQL,
            &[
                Value::Text(profile.path.to_owned()),
                Value::Blob(edited.blob().clone()),
            ],
            ExecuteOptions::default(),
            ExecuteStatementMetadata {
                parameter_blob_splices: vec![None, Some(provenance)],
                mutation_identity: None,
            },
        ),
    )
    .await
    .expect("publish authenticated edit");
    assert_eq!(result.rows_affected(), 1);
    assert_content(&source, profile.path, payload.size, &payload.edited_blake3).await;
    let edited_inventory = measured_inventory(backend, profile, "edited", database, &engine).await;
    assert_eq!(edited_inventory.blob_manifests, 2);
    assert_eq!(
        edited_inventory.blob_manifest_logical_bytes,
        (payload.size * 2) as u64
    );
    assert!(edited_inventory.unique_blob_chunks >= uploaded.unique_blob_chunks);
    assert!(edited_inventory.unique_blob_payload_bytes >= uploaded.unique_blob_payload_bytes);

    let source_head = active_commit_id(&source).await;
    let diff = measured(
        backend,
        profile,
        "diff",
        database,
        &storage,
        slate.as_ref(),
        source.execute(
            "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) \
             WHERE schema_key = 'lix_binary_blob_ref'",
            &[
                Value::Text(branch.commit_id.clone()),
                Value::Text(source_head.clone()),
            ],
        ),
    )
    .await
    .expect("diff source branch");
    assert_eq!(diff.rows()[0].get::<i64>("entries").unwrap(), 1);

    let merge = measured(
        backend,
        profile,
        "merge",
        database,
        &storage,
        slate.as_ref(),
        main.merge_branch(MergeBranchOptions {
            source_branch_id: branch.id.clone(),
        }),
    )
    .await
    .expect("merge source branch");
    assert_eq!(merge.change_stats.total, 1);
    let merged = measured_inventory(backend, profile, "merged", database, &engine).await;
    assert_blob_inventory_unchanged("merge", &edited_inventory, &merged);

    measured(
        backend,
        profile,
        "post_merge_checkpoint",
        database,
        &storage,
        slate.as_ref(),
        main.create_checkpoint(),
    )
    .await
    .expect("checkpoint merged state");
    let post_checkpoint =
        measured_inventory(backend, profile, "post_merge_checkpoint", database, &engine).await;
    assert_blob_inventory_unchanged("post-merge checkpoint", &merged, &post_checkpoint);

    measured(
        backend,
        profile,
        "undo",
        database,
        &storage,
        slate.as_ref(),
        main.undo(),
    )
    .await
    .expect("undo merged edit");
    assert_content(&main, profile.path, payload.size, &payload.base_blake3).await;
    measured(
        backend,
        profile,
        "redo",
        database,
        &storage,
        slate.as_ref(),
        main.redo(),
    )
    .await
    .expect("redo merged edit");
    assert_content(&main, profile.path, payload.size, &payload.edited_blake3).await;

    source.close().await.expect("close merged source session");
    drop(source);
    let workspace = engine
        .open_workspace_session()
        .await
        .expect("open workspace to retire merged source");
    workspace
        .execute(
            "DELETE FROM lix_branch WHERE id = $1",
            &[Value::Text(SOURCE_BRANCH_ID.to_owned())],
        )
        .await
        .expect("retire merged source branch");
    drop(workspace);

    let root_present = active_commit_id(&main).await;
    main.execute(
        "DELETE FROM lix_file WHERE path = $1",
        &[Value::Text(profile.path.to_owned())],
    )
    .await
    .expect("delete media from main");
    let root_absent = active_commit_id(&main).await;
    let retained = main
        .create_branch(CreateBranchOptions {
            id: Some(RETAINED_BRANCH_ID.to_owned()),
            name: format!("Stage2 {} retained history", profile.name),
            from_commit_id: Some(root_absent.clone()),
        })
        .await
        .expect("create retained-history branch");
    assert_eq!(retained.id, RETAINED_BRANCH_ID);
    for revision in 0..RETENTION_CHECKPOINTS {
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('stage2-retention', $1) \
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            &[Value::Text(revision.to_string())],
        )
        .await
        .expect("advance main recovery interval");
        main.create_checkpoint()
            .await
            .expect("checkpoint recovery interval");
    }

    let before_retained_gc = inventory(&engine)
        .await
        .expect("inventory before retained GC");
    let retained_gc = measured(
        backend,
        profile,
        "retained_history_gc",
        database,
        &storage,
        slate.as_ref(),
        run_gc_to_completion(&engine, GcBudget::default()),
    )
    .await
    .expect("run retained-history GC");
    assert_gc_complete(&retained_gc);
    let retained_inventory = inventory(&engine)
        .await
        .expect("inventory after retained GC");
    assert_blob_inventory_unchanged(
        "retained-history GC",
        &before_retained_gc,
        &retained_inventory,
    );
    let retained_session = engine
        .open_session(RETAINED_BRANCH_ID)
        .await
        .expect("open warm retained-history session");
    retained_session.undo().await.expect("warm retained undo");
    assert_content(
        &retained_session,
        profile.path,
        payload.size,
        &payload.edited_blake3,
    )
    .await;
    retained_session.redo().await.expect("warm retained redo");
    assert_file_absent(&retained_session, profile.path).await;
    retained_session
        .close()
        .await
        .expect("close warm retained session");
    drop(retained_session);

    measured(
        backend,
        profile,
        "flush_before_reopen",
        database,
        &storage,
        slate.as_ref(),
        storage.inner().qualification_flush(),
    )
    .await;
    main.close().await.expect("close main session");
    drop(main);
    drop(engine);
    drop(storage);

    ReopenExpectation {
        main_branch_id: receipt.main_branch_id,
        root_present,
        root_absent,
        edited_blake3: payload.edited_blake3,
        retained_inventory,
    }
}

async fn qualify_after_reopen<S>(
    backend: &str,
    profile: Profile,
    database: &Path,
    raw_storage: S,
    slate: Option<SlateDBIoCounters>,
    expected: ReopenExpectation,
) -> String
where
    S: QualificationStorage,
{
    let storage = CountingStorage::new(raw_storage);
    let engine = measured(
        backend,
        profile,
        "cold_reopen_engine",
        database,
        &storage,
        slate.as_ref(),
        Engine::new(storage.clone()),
    )
    .await
    .expect("cold reopen Stage2 repository");
    let retained = engine
        .open_session(RETAINED_BRANCH_ID)
        .await
        .expect("reopen retained-history branch");
    let diff = retained
        .execute(
            "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) \
             WHERE schema_key = 'lix_binary_blob_ref'",
            &[
                Value::Text(expected.root_present.clone()),
                Value::Text(expected.root_absent.clone()),
            ],
        )
        .await
        .expect("cold retained history diff");
    assert_eq!(diff.rows()[0].get::<i64>("entries").unwrap(), 1);
    retained.undo().await.expect("cold retained undo");
    assert_content(
        &retained,
        profile.path,
        profile.size(),
        &expected.edited_blake3,
    )
    .await;
    retained.redo().await.expect("cold retained redo");
    assert_file_absent(&retained, profile.path).await;
    let reopened_inventory = inventory(&engine).await.expect("cold inventory");
    assert_blob_inventory_unchanged(
        "cold reopen",
        &expected.retained_inventory,
        &reopened_inventory,
    );
    retained.close().await.expect("close retained session");
    drop(retained);

    let workspace = engine
        .open_workspace_session()
        .await
        .expect("open workspace session");
    workspace
        .execute(
            "DELETE FROM lix_branch WHERE id = $1",
            &[Value::Text(RETAINED_BRANCH_ID.to_owned())],
        )
        .await
        .expect("release final retained-history root");
    drop(workspace);

    let before_final = inventory(&engine).await.expect("inventory before final GC");
    let final_gc = measured(
        backend,
        profile,
        "final_reference_gc",
        database,
        &storage,
        slate.as_ref(),
        run_gc_to_completion(&engine, GcBudget::default()),
    )
    .await
    .expect("run final-reference GC");
    assert_gc_complete(&final_gc);
    let after_final =
        measured_inventory(backend, profile, "after_final_gc", database, &engine).await;
    assert_eq!(after_final.blob_manifests, 0);
    assert_eq!(after_final.blob_manifest_logical_bytes, 0);
    assert_eq!(after_final.blob_chunk_references, 0);
    assert_eq!(after_final.unique_blob_chunks, 0);
    assert_eq!(after_final.unique_blob_payload_bytes, 0);
    assert_eq!(after_final.receipt_tree_nodes, 0);
    assert_eq!(after_final.upload_parts, 0);
    assert!(final_gc.reclaimed_objects > 0);
    assert!(final_gc.reclaimed_object_bytes >= before_final.unique_blob_payload_bytes);

    let main = engine
        .open_session(&expected.main_branch_id)
        .await
        .expect("open final main session");
    assert_file_absent(&main, profile.path).await;
    main.close().await.expect("close final main session");
    measured(
        backend,
        profile,
        "final_flush",
        database,
        &storage,
        slate.as_ref(),
        storage.inner().qualification_flush(),
    )
    .await;
    drop(engine);
    drop(storage);

    let final_disk = directory_bytes(database);
    println!(
        "stage2_media_final,backend={backend},profile={},disk_bytes={final_disk},presence_authority=absent_by_design",
        profile.name
    );
    expected.main_branch_id
}

async fn qualify_final_reopen<S>(
    backend: &str,
    profile: Profile,
    database: &Path,
    storage: S,
    main_branch_id: String,
) where
    S: QualificationStorage,
{
    let engine = Engine::new(storage.clone())
        .await
        .expect("final cold reopen Stage2 repository");
    let final_inventory = inventory(&engine)
        .await
        .expect("final cold authenticated inventory");
    assert_eq!(final_inventory.blob_manifests, 0);
    assert_eq!(final_inventory.blob_chunk_references, 0);
    assert_eq!(final_inventory.unique_blob_chunks, 0);
    assert_eq!(final_inventory.unique_blob_payload_bytes, 0);
    let main = engine
        .open_session(main_branch_id)
        .await
        .expect("final cold open main session");
    assert_file_absent(&main, profile.path).await;
    main.close().await.expect("close final cold main session");
    storage.qualification_flush().await;
    drop(engine);
    drop(storage);
    println!(
        "stage2_media_final_reopen,backend={backend},profile={},disk_bytes={},inventory={final_inventory:?}",
        profile.name,
        directory_bytes(database)
    );
}

async fn upload_parts<S>(session: &SessionContext<S>, path: &str, payload: &PreparedPayload)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    for (index, part) in payload.parts.iter().enumerate() {
        let start = index * FILE_UPLOAD_PART_BYTES;
        let progress = session
            .upsert_file_content_part(
                format!("stage2-{}", path),
                path.to_owned(),
                start as u64,
                payload.size as u64,
                part.clone().into(),
            )
            .await
            .expect("upload authenticated media part");
        assert_eq!(progress.next_offset, (start + part.len()) as u64);
        assert_eq!(progress.finalized, start + part.len() == payload.size);
    }
}

fn prepare_payload(profile: Profile) -> PreparedPayload {
    let size = profile.size();
    assert_eq!(size % FILE_UPLOAD_PART_BYTES, 0);
    let edit_bytes = size * profile.edit_percent / 100;
    let edit_offset = (size / 2 + 12_345).min(size - edit_bytes);
    let edit_end = edit_offset + edit_bytes;
    let mut parts = Vec::with_capacity(size / FILE_UPLOAD_PART_BYTES);
    let mut base_blake3 = blake3::Hasher::new();
    let mut edited_blake3 = blake3::Hasher::new();
    let mut base_sha256 = Sha256::new();
    let mut edited_sha256 = Sha256::new();
    for offset in (0..size).step_by(FILE_UPLOAD_PART_BYTES) {
        let mut bytes = deterministic_bytes(FILE_UPLOAD_PART_BYTES, SEED ^ offset as u64);
        base_blake3.update(&bytes);
        base_sha256.update(&bytes);
        if offset < edit_end && offset + bytes.len() > edit_offset {
            let local_start = edit_offset.saturating_sub(offset);
            let local_end = (edit_end - offset).min(bytes.len());
            let mut changed = bytes.clone();
            changed[local_start..local_end].fill(0xa5);
            edited_blake3.update(&changed);
            edited_sha256.update(&changed);
        } else {
            edited_blake3.update(&bytes);
            edited_sha256.update(&bytes);
        }
        parts.push(Bytes::from(std::mem::take(&mut bytes)));
    }
    PreparedPayload {
        parts,
        size,
        edit_offset,
        edit_bytes,
        base_blake3: base_blake3.finalize().to_hex().to_string(),
        base_sha256: format!("{:x}", base_sha256.finalize()),
        edited_blake3: edited_blake3.finalize().to_hex().to_string(),
        edited_sha256: format!("{:x}", edited_sha256.finalize()),
    }
}

fn deterministic_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut bytes = vec![0; len];
    let mut state = seed ^ 0xd1b5_4a32_d192_ed03;
    for chunk in bytes.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let generated = state.to_le_bytes();
        chunk.copy_from_slice(&generated[..chunk.len()]);
    }
    bytes
}

async fn active_commit_id<S>(session: &SessionContext<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("load active commit id")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("decode active commit id")
}

async fn assert_content<S>(
    session: &SessionContext<S>,
    path: &str,
    size: usize,
    expected_blake3: &str,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let range_start = size as u64 / 2 + 777;
    let range = session
        .read_file_content(
            path.to_owned(),
            Some(range_start..range_start + RANGE_BYTES),
        )
        .await
        .expect("read media range")
        .expect("media exists");
    assert_eq!(range.total_size(), size as u64);
    assert_eq!(range.content().len(), RANGE_BYTES as usize);
    drop(range);
    let full = session
        .read_file_content(path.to_owned(), None)
        .await
        .expect("read full media")
        .expect("media exists");
    assert_eq!(full.total_size(), size as u64);
    assert_eq!(
        blake3::hash(full.content()).to_hex().to_string(),
        expected_blake3
    );
}

async fn assert_file_absent<S>(session: &SessionContext<S>, path: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = session
        .execute(
            "SELECT COUNT(*) AS entries FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_owned())],
        )
        .await
        .expect("query file absence");
    assert_eq!(result.rows()[0].get::<i64>("entries").unwrap(), 0);
}

fn assert_blob_inventory_unchanged(
    operation: &str,
    before: &ForkTreeInventory,
    after: &ForkTreeInventory,
) {
    assert_eq!(before.blob_manifests, after.blob_manifests, "{operation}");
    assert_eq!(
        before.blob_manifest_logical_bytes, after.blob_manifest_logical_bytes,
        "{operation}"
    );
    assert_eq!(
        before.blob_chunk_references, after.blob_chunk_references,
        "{operation}"
    );
    assert_eq!(
        before.unique_blob_chunks, after.unique_blob_chunks,
        "{operation}"
    );
    assert_eq!(
        before.unique_blob_payload_bytes, after.unique_blob_payload_bytes,
        "{operation}"
    );
    assert_eq!(
        before.receipt_tree_nodes, after.receipt_tree_nodes,
        "{operation}"
    );
    assert_eq!(before.upload_parts, after.upload_parts, "{operation}");
}

fn assert_gc_complete(summary: &ForkTreeGcRunSummary) {
    assert_eq!(summary.terminal_status, GcTerminalStatus::Complete);
    assert!(summary.steps > 0);
    assert!(summary.max_page_claims > 0 || summary.max_page_deletes > 0);
}

async fn measured_inventory<S>(
    backend: &str,
    profile: Profile,
    label: &str,
    database: &Path,
    engine: &Engine<CountingStorage<S>>,
) -> ForkTreeInventory
where
    S: QualificationStorage,
{
    let value = inventory(engine)
        .await
        .expect("authenticated ForkTree inventory");
    println!(
        "stage2_media_inventory,backend={backend},profile={},label={label},disk_bytes={},inventory={value:?},presence_authority=absent_by_design",
        profile.name,
        directory_bytes(database)
    );
    value
}

async fn measured<S, F, T>(
    backend: &str,
    profile: Profile,
    operation: &str,
    database: &Path,
    storage: &CountingStorage<S>,
    slate: Option<&SlateDBIoCounters>,
    future: F,
) -> T
where
    S: Storage,
    F: Future<Output = T>,
{
    storage.reset();
    let slate_before = slate.map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
    let disk_before = directory_bytes(database);
    let rss_before = current_rss_kib();
    let cpu_before = process_cpu_ticks();
    reset_allocations();
    let started = Instant::now();
    let value = future.await;
    let wall = started.elapsed();
    let (allocated_bytes, allocation_calls) = stop_allocations();
    let cpu_ticks = process_cpu_ticks().saturating_sub(cpu_before);
    let rss_after = current_rss_kib();
    let io = storage.snapshot();
    let slate_delta = slate
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot)
        .saturating_sub(slate_before);
    let disk_after = directory_bytes(database);
    println!(
        "stage2_media_phase,backend={backend},profile={},operation={operation},wall_ms={:.3},cpu_ticks={cpu_ticks},allocated_bytes={allocated_bytes},allocation_calls={allocation_calls},rss_before_kib={rss_before},rss_after_kib={rss_after},hwm_kib={},begin_reads={},begin_writes={},get_many_calls={},get_many_keys={},get_many_found={},get_many_value_bytes={},scan_calls={},scan_rows={},scan_value_bytes={},put_batches={},puts={},delete_batches={},deletes={},logical_write_bytes={},commits={},backend_puts={},backend_deletes={},backend_written_bytes={},backend_storage_calls={},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},disk_before_bytes={disk_before},disk_after_bytes={disk_after}",
        profile.name,
        wall.as_secs_f64() * 1_000.0,
        process_hwm_kib(),
        io.begin_reads,
        io.begin_writes,
        io.get_many_calls,
        io.get_many_keys,
        io.get_many_found,
        io.get_many_value_bytes,
        io.scan_calls,
        io.scan_rows,
        io.scan_value_bytes,
        io.put_batches,
        io.puts,
        io.delete_batches,
        io.deletes,
        io.logical_write_bytes,
        io.commits,
        io.backend_puts,
        io.backend_deletes,
        io.backend_written_bytes,
        io.backend_storage_calls,
        slate_delta.read_objects,
        slate_delta.read_bytes,
        slate_delta.write_objects,
        slate_delta.write_bytes,
    );
    value
}

fn required_database_path() -> PathBuf {
    std::env::var_os("LIX_STAGE2_MEDIA_DB")
        .map(PathBuf::from)
        .expect("LIX_STAGE2_MEDIA_DB is required")
}

fn reset_allocations() {
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATION_ENABLED.store(true, Ordering::Release);
}

fn stop_allocations() -> (u64, u64) {
    ALLOCATION_ENABLED.store(false, Ordering::Release);
    (
        ALLOCATED_BYTES.load(Ordering::Relaxed),
        ALLOCATION_CALLS.load(Ordering::Relaxed),
    )
}

fn directory_bytes(path: &Path) -> u64 {
    let mut total = 0_u64;
    let mut pending = vec![path.to_path_buf()];
    while let Some(current) = pending.pop() {
        let Ok(metadata) = std::fs::symlink_metadata(&current) else {
            continue;
        };
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        } else if metadata.is_dir() {
            let Ok(entries) = std::fs::read_dir(&current) else {
                continue;
            };
            pending.extend(entries.flatten().map(|entry| entry.path()));
        }
    }
    total
}

fn process_cpu_ticks() -> u64 {
    std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|value| {
            let end = value.rfind(')')?;
            let fields = value.get(end + 2..)?.split_whitespace().collect::<Vec<_>>();
            let user = fields.get(11)?.parse::<u64>().ok()?;
            let system = fields.get(12)?.parse::<u64>().ok()?;
            Some(user.saturating_add(system))
        })
        .unwrap_or(0)
}

fn current_rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|value| {
            value.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
        })
        .unwrap_or(0)
}

fn process_hwm_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|value| {
            value.lines().find_map(|line| {
                line.strip_prefix("VmHWM:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
        })
        .unwrap_or(0)
}
