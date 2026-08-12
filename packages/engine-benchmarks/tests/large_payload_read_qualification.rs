#![allow(clippy::large_futures)]

use std::alloc::{GlobalAlloc, Layout};
use std::future::Future;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use lix::integration::{Engine, SessionContext};
use lix::storage::{
    BeginScanOptions, CommitResult, CoreProjection, GetManyRequest, GetManyResult, Key, KeyRange,
    MAX_SCAN_PAGE_ROWS, ProjectedValue, PutBatch, ReadOptions, ScanChunk, ScanCursor, SpaceId,
    Storage, StorageError, StorageRead, StorageScanSource, StorageSpace, StorageWrite,
    WriteOptions,
};
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::{
    binary_cas_owner_layout_accounting, binary_cas_write_accounting,
    reset_binary_cas_write_accounting, take_media_structural_accounting,
};
use lix::{
    CreateBranchOptions, ExecuteOptions, ExecuteStatementMetadata, FILE_UPLOAD_PART_BYTES,
    MergeBranchOptions, Value, VerifiedRequestBlob,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};
use sha2::{Digest as _, Sha256};

const MANIFEST_SPACE: SpaceId = SpaceId(0x0005_0001);
const MANIFEST_CHUNK_SPACE: SpaceId = SpaceId(0x0005_0002);
const PAYLOAD_SPACE: SpaceId = SpaceId(0x0005_0003);
const PRESENCE_SPACE: SpaceId = SpaceId(0x0005_0004);
const PATH: &str = "/media/foreground.mov";
const EDIT_BYTES: usize = 4 * 1024;
const RANGE_BYTES: u64 = 4 * 1024;
const SEED: u64 = 0x89a3_10fd_4242_73c1;
const SOURCE_BRANCH_ID: &str = "01980000-0000-7000-8000-000000000064";
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

struct CountingScanSource<'a> {
    inner: ScanCursor<'a>,
    stats: Arc<Mutex<IoStats>>,
}

impl StorageScanSource for CountingScanSource<'_> {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            let (chunk, chunk_has_more) = self.inner.next_page(limit_rows).await?.into_parts();
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.scan_rows += chunk.len() as u64;
            stats.scan_value_bytes += chunk
                .iter()
                .map(|entry| match &entry.value {
                    ProjectedValue::KeyOnly => 0,
                    ProjectedValue::FullValue(value) => value.len() as u64,
                })
                .sum::<u64>();
            drop(stats);
            Ok(ScanChunk::new(chunk, chunk_has_more))
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SpaceAccounting {
    rows: u64,
    key_bytes: u64,
    value_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct CasLayout {
    manifest: SpaceAccounting,
    manifest_chunk: SpaceAccounting,
    payload: SpaceAccounting,
    presence: SpaceAccounting,
}

impl CasLayout {
    fn total_rows(self) -> u64 {
        self.manifest.rows + self.manifest_chunk.rows + self.payload.rows + self.presence.rows
    }

    fn total_bytes(self) -> u64 {
        self.manifest.key_bytes
            + self.manifest.value_bytes
            + self.manifest_chunk.key_bytes
            + self.manifest_chunk.value_bytes
            + self.payload.key_bytes
            + self.payload.value_bytes
            + self.presence.key_bytes
            + self.presence.value_bytes
    }
}

struct PreparedPayload {
    parts: Vec<Bytes>,
    size: usize,
    edit_offset: usize,
    base_blake3: String,
    base_sha256: String,
    edited_blake3: String,
    edited_sha256: String,
}

struct ReopenExpectation {
    main_branch_id: String,
    size: usize,
    content_blake3: String,
    content_identity: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual large-media foreground qualification"]
async fn large_media_foreground_lifecycle() {
    let size_mib = std::env::var("LIX_MEDIA_QUAL_MIB")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64);
    assert!(
        matches!(size_mib, 64 | 256 | 512),
        "qualification size must be 64, 256, or 512 MiB"
    );
    let temp = tempfile::tempdir().expect("create media qualification directory");
    let database = temp.path().join("database");
    match std::env::var("LIX_MEDIA_QUAL_BACKEND").as_deref() {
        Ok("rocksdb") => {
            let expectation = qualify_visible_lifecycle(
                "rocksdb",
                &database,
                RocksDB::open(&database).expect("open qualification RocksDB"),
                None,
                size_mib,
            )
            .await;
            qualify_reopen(
                "rocksdb",
                &database,
                RocksDB::open(&database).expect("reopen qualification RocksDB"),
                None,
                expectation,
            )
            .await;
        }
        Ok("slatedb") => {
            let counters = SlateDBIoCounters::default();
            let expectation = qualify_visible_lifecycle(
                "slatedb",
                &database,
                SlateDB::open_with_io_counters(&database, counters.clone())
                    .expect("open qualification SlateDB"),
                Some(counters),
                size_mib,
            )
            .await;
            let reopen_counters = SlateDBIoCounters::default();
            qualify_reopen(
                "slatedb",
                &database,
                SlateDB::open_with_io_counters(&database, reopen_counters.clone())
                    .expect("reopen qualification SlateDB"),
                Some(reopen_counters),
                expectation,
            )
            .await;
        }
        Ok(value) => panic!("qualification backend must be rocksdb or slatedb, got {value}"),
        Err(_) => panic!("LIX_MEDIA_QUAL_BACKEND is required"),
    }
}

async fn qualify_visible_lifecycle<S>(
    backend: &str,
    database: &Path,
    raw_storage: S,
    slate_counters: Option<SlateDBIoCounters>,
    size_mib: usize,
) -> ReopenExpectation
where
    S: QualificationStorage,
{
    let prepared = prepare_payload(size_mib * 1024 * 1024);
    let storage = CountingStorage::new(raw_storage);
    let receipt = Engine::initialize(storage.clone())
        .await
        .expect("initialize media qualification engine");
    let main_branch_id = receipt.main_branch_id.clone();
    let engine = Engine::new(storage.clone())
        .await
        .expect("open media qualification engine");
    let main = engine
        .open_session(&main_branch_id)
        .await
        .expect("open qualification main session");

    measured(
        backend,
        size_mib,
        "ingest",
        database,
        &storage,
        slate_counters.as_ref(),
        upload_parts(&main, &prepared),
    )
    .await;
    let ingest_layout = cas_layout(&storage).await;
    print_layout(backend, size_mib, "after_ingest", ingest_layout);
    assert_eq!(ingest_layout.payload.rows, size_mib as u64);
    measured(
        backend,
        size_mib,
        "post_ingest_flush",
        database,
        &storage,
        slate_counters.as_ref(),
        storage.inner().qualification_flush(),
    )
    .await;

    let range_start = prepared.size as u64 / 2 + 777;
    let range_read = measured(
        backend,
        size_mib,
        "first_range_4k",
        database,
        &storage,
        slate_counters.as_ref(),
        main.read_file_content(
            PATH.to_owned(),
            Some(range_start..range_start + RANGE_BYTES),
        ),
    )
    .await
    .expect("read ingested media range")
    .expect("ingested media exists");
    assert_eq!(range_read.total_size(), prepared.size as u64);
    assert_eq!(range_read.range(), range_start..range_start + RANGE_BYTES);
    assert_eq!(range_read.content().len(), RANGE_BYTES as usize);
    drop(range_read);

    let first_read = measured(
        backend,
        size_mib,
        "first_full_read",
        database,
        &storage,
        slate_counters.as_ref(),
        main.read_file_content(PATH.to_owned(), None),
    )
    .await
    .expect("read ingested media")
    .expect("ingested media exists");
    assert_eq!(first_read.total_size(), prepared.size as u64);
    assert_eq!(first_read.range(), 0..prepared.size as u64);
    let base_identity = first_read.content_identity().to_owned();
    assert_eq!(
        blake3::hash(first_read.content()).to_hex().to_string(),
        prepared.base_blake3
    );
    drop(first_read);

    let base_read = measured(
        backend,
        size_mib,
        "warm_full_read",
        database,
        &storage,
        slate_counters.as_ref(),
        main.read_file_content(PATH.to_owned(), None),
    )
    .await
    .expect("warm-read ingested media")
    .expect("ingested media exists");
    assert_eq!(base_read.content_identity(), base_identity);
    assert_eq!(
        blake3::hash(base_read.content()).to_hex().to_string(),
        prepared.base_blake3
    );

    measured(
        backend,
        size_mib,
        "base_checkpoint",
        database,
        &storage,
        slate_counters.as_ref(),
        main.create_checkpoint(),
    )
    .await
    .expect("create base checkpoint");
    let checkpoint_layout = cas_layout(&storage).await;
    print_amplification(
        backend,
        size_mib,
        "base_checkpoint",
        ingest_layout,
        checkpoint_layout,
    );

    let branch = measured(
        backend,
        size_mib,
        "branch_without_edit",
        database,
        &storage,
        slate_counters.as_ref(),
        main.create_branch(CreateBranchOptions {
            id: Some(SOURCE_BRANCH_ID.to_owned()),
            name: "Large media edit".to_owned(),
            from_commit_id: None,
        }),
    )
    .await
    .expect("create large media branch");
    let branch_layout = cas_layout(&storage).await;
    print_amplification(
        backend,
        size_mib,
        "branch_without_edit",
        checkpoint_layout,
        branch_layout,
    );
    assert_eq!(
        branch_layout, checkpoint_layout,
        "branch duplicated binary CAS rows"
    );

    let source = engine
        .open_session(&branch.id)
        .await
        .expect("open large media source branch");
    let branch_range = measured(
        backend,
        size_mib,
        "branch_range_4k",
        database,
        &storage,
        slate_counters.as_ref(),
        source.read_file_content(
            PATH.to_owned(),
            Some(range_start..range_start + RANGE_BYTES),
        ),
    )
    .await
    .expect("read media range on unedited branch")
    .expect("media exists on branch");
    assert_eq!(branch_range.content().len(), RANGE_BYTES as usize);
    assert_eq!(branch_range.content_identity(), base_identity);
    drop(branch_range);
    let branch_read = measured(
        backend,
        size_mib,
        "branch_exact_read",
        database,
        &storage,
        slate_counters.as_ref(),
        source.read_file_content(PATH.to_owned(), None),
    )
    .await
    .expect("read media on unedited branch")
    .expect("media exists on branch");
    assert_eq!(branch_read.content_identity(), base_identity);
    assert_eq!(branch_read.content(), base_read.content());
    drop(branch_read);

    let verified_base = measured(
        backend,
        size_mib,
        "localized_edit_verify_base",
        database,
        &storage,
        slate_counters.as_ref(),
        async { VerifiedRequestBlob::verify(base_read.into_content()) },
    )
    .await;
    assert_eq!(verified_base.sha256(), prepared.base_sha256);
    let suffix_bytes = prepared.size - prepared.edit_offset - EDIT_BYTES;
    let (verified_result, provenance) = measured(
        backend,
        size_mib,
        "localized_edit_reconstruct",
        database,
        &storage,
        slate_counters.as_ref(),
        async {
            verified_base.reconstruct_splice(
                &prepared.base_sha256,
                &prepared.edited_sha256,
                prepared.edit_offset,
                suffix_bytes,
                vec![0xa5; EDIT_BYTES].into(),
            )
        },
    )
    .await
    .expect("reconstruct authenticated localized edit");
    drop(verified_base);

    let edit_result = measured(
        backend,
        size_mib,
        "localized_edit_publish",
        database,
        &storage,
        slate_counters.as_ref(),
        source.execute_with_options_and_metadata(
            UPSERT_SQL,
            &[
                Value::Text(PATH.to_owned()),
                Value::Blob(verified_result.blob().clone()),
            ],
            ExecuteOptions::default(),
            ExecuteStatementMetadata {
                parameter_blob_splices: vec![None, Some(provenance)],
                mutation_identity: None,
            },
        ),
    )
    .await
    .expect("publish authenticated localized edit");
    assert_eq!(edit_result.rows_affected(), 1);
    assert_eq!(
        blake3::hash(verified_result.blob()).to_hex().to_string(),
        prepared.edited_blake3
    );
    drop(verified_result);
    let edit_layout = cas_layout(&storage).await;
    print_amplification(
        backend,
        size_mib,
        "localized_edit",
        branch_layout,
        edit_layout,
    );
    assert!(
        edit_layout
            .payload
            .rows
            .saturating_sub(branch_layout.payload.rows)
            <= 1,
        "localized edit rewrote unchanged payload chunks"
    );

    let source_head = active_commit_id(&source).await;
    let diff_sql = format!(
        "SELECT COUNT(*) AS entries FROM lix_diff('{}', '{}') \
         WHERE schema_key = 'lix_binary_blob_ref'",
        branch.commit_id, source_head
    );
    let diff = measured(
        backend,
        size_mib,
        "diff",
        database,
        &storage,
        slate_counters.as_ref(),
        source.execute(&diff_sql, &[]),
    )
    .await
    .expect("diff localized media edit");
    assert_eq!(
        diff.rows()[0]
            .get::<i64>("entries")
            .expect("decode media diff count"),
        1
    );

    let merge = measured(
        backend,
        size_mib,
        "merge",
        database,
        &storage,
        slate_counters.as_ref(),
        main.merge_branch(MergeBranchOptions {
            source_branch_id: branch.id.clone(),
        }),
    )
    .await
    .expect("merge localized media branch");
    assert_eq!(merge.change_stats.total, 1);
    assert_eq!(merge.target_head_after_commit_id, source_head);
    let merge_layout = cas_layout(&storage).await;
    print_amplification(backend, size_mib, "merge", edit_layout, merge_layout);
    assert_eq!(
        merge_layout, edit_layout,
        "merge duplicated binary CAS rows"
    );

    measured(
        backend,
        size_mib,
        "post_merge_checkpoint",
        database,
        &storage,
        slate_counters.as_ref(),
        main.create_checkpoint(),
    )
    .await
    .expect("checkpoint merged media");
    let final_layout = cas_layout(&storage).await;
    print_amplification(
        backend,
        size_mib,
        "post_merge_checkpoint",
        merge_layout,
        final_layout,
    );
    assert_eq!(
        final_layout, merge_layout,
        "checkpoint duplicated binary CAS rows"
    );

    let merged_read = measured(
        backend,
        size_mib,
        "merged_exact_read",
        database,
        &storage,
        slate_counters.as_ref(),
        main.read_file_content(PATH.to_owned(), None),
    )
    .await
    .expect("read merged media")
    .expect("merged media exists");
    assert_eq!(
        blake3::hash(merged_read.content()).to_hex().to_string(),
        prepared.edited_blake3
    );
    assert_ne!(merged_read.content_identity(), base_identity);
    let edited_identity = merged_read.content_identity().to_owned();
    drop(merged_read);

    measured(
        backend,
        size_mib,
        "flush",
        database,
        &storage,
        slate_counters.as_ref(),
        storage.inner().qualification_flush(),
    )
    .await;
    print_owner_inventory(backend, size_mib, &storage).await;

    source.close().await.expect("close source branch session");
    main.close().await.expect("close main branch session");
    drop(source);
    drop(main);
    drop(engine);
    drop(storage);

    ReopenExpectation {
        main_branch_id,
        size: prepared.size,
        content_blake3: prepared.edited_blake3,
        content_identity: edited_identity,
    }
}

async fn qualify_reopen<S>(
    backend: &str,
    database: &Path,
    raw_storage: S,
    slate_counters: Option<SlateDBIoCounters>,
    expected: ReopenExpectation,
) where
    S: QualificationStorage,
{
    let size_mib = expected.size / (1024 * 1024);
    let storage = CountingStorage::new(raw_storage);
    let engine = measured(
        backend,
        size_mib,
        "reopen_engine",
        database,
        &storage,
        slate_counters.as_ref(),
        Engine::new(storage.clone()),
    )
    .await
    .expect("reopen media qualification engine");
    let main = engine
        .open_session(&expected.main_branch_id)
        .await
        .expect("open reopened main session");
    let read = measured(
        backend,
        size_mib,
        "reopen_cold_full_read",
        database,
        &storage,
        slate_counters.as_ref(),
        main.read_file_content(PATH.to_owned(), None),
    )
    .await
    .expect("read reopened media")
    .expect("reopened media exists");
    assert_eq!(read.total_size(), expected.size as u64);
    assert_eq!(read.content_identity(), expected.content_identity);
    assert_eq!(
        blake3::hash(read.content()).to_hex().to_string(),
        expected.content_blake3
    );
    drop(read);
    let warm_read = measured(
        backend,
        size_mib,
        "reopen_warm_full_read",
        database,
        &storage,
        slate_counters.as_ref(),
        main.read_file_content(PATH.to_owned(), None),
    )
    .await
    .expect("warm-read reopened media")
    .expect("reopened media exists");
    assert_eq!(warm_read.content_identity(), expected.content_identity);
    assert_eq!(
        blake3::hash(warm_read.content()).to_hex().to_string(),
        expected.content_blake3
    );
    drop(warm_read);
    let range_start = expected.size as u64 / 2 + 777;
    let range = measured(
        backend,
        size_mib,
        "reopen_warm_range_4k",
        database,
        &storage,
        slate_counters.as_ref(),
        main.read_file_content(
            PATH.to_owned(),
            Some(range_start..range_start + RANGE_BYTES),
        ),
    )
    .await
    .expect("range-read reopened media")
    .expect("reopened media exists");
    assert_eq!(range.content().len(), RANGE_BYTES as usize);
    assert_eq!(range.content_identity(), expected.content_identity);
    drop(range);
    let layout = cas_layout(&storage).await;
    print_layout(backend, size_mib, "after_reopen", layout);
    print_owner_inventory(backend, size_mib, &storage).await;
    main.close().await.expect("close reopened main session");
}

async fn upload_parts<S>(session: &SessionContext<S>, payload: &PreparedPayload)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    for (index, part) in payload.parts.iter().enumerate() {
        let start = index * FILE_UPLOAD_PART_BYTES;
        let progress = session
            .upsert_file_content_part(
                "foreground-qualification".to_owned(),
                PATH.to_owned(),
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

fn prepare_payload(size: usize) -> PreparedPayload {
    assert_eq!(size % FILE_UPLOAD_PART_BYTES, 0);
    let edit_offset = size / 2 + 12_345;
    let edit_end = edit_offset + EDIT_BYTES;
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
            let mut edited = bytes.clone();
            edited[local_start..local_end].fill(0xa5);
            edited_blake3.update(&edited);
            edited_sha256.update(&edited);
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
    let result = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("load active commit id");
    result.rows()[0]
        .get::<String>("commit_id")
        .expect("decode active commit id")
}

async fn measured<S, F, T>(
    backend: &str,
    size_mib: usize,
    operation: &str,
    database: &Path,
    storage: &CountingStorage<S>,
    slate_counters: Option<&SlateDBIoCounters>,
    future: F,
) -> T
where
    S: Storage,
    F: Future<Output = T>,
{
    storage.reset();
    let _ = take_media_structural_accounting();
    reset_binary_cas_write_accounting();
    let slate_before =
        slate_counters.map_or_else(SlateDBIoSnapshot::default, |value| value.snapshot());
    let disk_before = directory_bytes(database);
    let rss_before = current_rss_kib();
    let cpu_before = process_cpu_ticks();
    let (stop_sampler, peak_rss, sampler) = start_rss_sampler();
    reset_allocations();
    let started = Instant::now();
    let value = future.await;
    let wall = started.elapsed();
    let (allocated_bytes, allocation_calls) = stop_allocations();
    stop_sampler.store(true, Ordering::Release);
    sampler.join().expect("join RSS sampler");
    let cpu_ticks = process_cpu_ticks().saturating_sub(cpu_before);
    let rss_after = current_rss_kib();
    let io = storage.snapshot();
    let slate = slate_counters
        .map_or_else(SlateDBIoSnapshot::default, |value| value.snapshot())
        .saturating_sub(slate_before);
    let disk_after = directory_bytes(database);
    let cas = binary_cas_write_accounting();
    let structural = take_media_structural_accounting();
    println!(
        "large_payload_read,backend={backend},size_mib={size_mib},operation={operation},\
         wall_ms={:.3},cpu_ticks={cpu_ticks},allocated_bytes={allocated_bytes},allocation_calls={allocation_calls},\
         rss_before_kib={rss_before},rss_after_kib={rss_after},peak_rss_kib={},hwm_kib={},\
         begin_reads={},begin_writes={},get_many_calls={},get_many_keys={},get_many_found={},get_many_value_bytes={},\
         scan_calls={},scan_rows={},scan_value_bytes={},put_batches={},puts={},delete_batches={},deletes={},logical_write_bytes={},commits={},\
         backend_puts={},backend_deletes={},backend_written_bytes={},backend_storage_calls={},\
         slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},\
         slate_reader_requests={},slate_main_read_requests={},slate_compactor_read_requests={},slate_compactor_write_requests={},\
         disk_before_bytes={disk_before},disk_after_bytes={disk_after},disk_growth_bytes={},\
         cas_chunk_lookups={},cas_lookup_batches={},cas_hits={},cas_misses={},cas_duplicates={},\
         temporary_manifest_leaves={},legacy_chunk_rows={},chunk_hash_bytes={},segment_identity_hash_bytes={}",
        wall.as_secs_f64() * 1_000.0,
        peak_rss.load(Ordering::Relaxed),
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
        slate.read_objects,
        slate.read_bytes,
        slate.write_objects,
        slate.write_bytes,
        slate.reader.read_requests,
        slate.main.read_requests,
        slate.compactor.read_requests,
        slate.compactor.write_requests,
        disk_after.saturating_sub(disk_before),
        cas.chunk_lookup_count,
        cas.chunk_lookup_batch_count,
        cas.chunk_lookup_hit_count,
        cas.chunk_lookup_miss_count,
        cas.transaction_duplicate_chunk_count,
        structural.temporary_manifest_leaf_rows,
        structural.legacy_equivalent_chunk_rows,
        structural.chunk_payload_hash_bytes,
        structural.segment_identity_hash_bytes,
    );
    value
}

async fn cas_layout<S>(storage: &S) -> CasLayout
where
    S: Storage,
{
    CasLayout {
        manifest: space_accounting(storage, MANIFEST_SPACE).await,
        manifest_chunk: space_accounting(storage, MANIFEST_CHUNK_SPACE).await,
        payload: space_accounting(storage, PAYLOAD_SPACE).await,
        presence: space_accounting(storage, PRESENCE_SPACE).await,
    }
}

async fn space_accounting<S>(storage: &S, id: SpaceId) -> SpaceAccounting
where
    S: Storage,
{
    // A space id has exactly one value semantics and the engine registry is
    // where it is declared; guessing it here scans a different physical
    // location than the engine wrote.
    let space = lix::storage_bench::storage_space_by_id(id.0);
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("open CAS accounting read");
    let mut accounting = SpaceAccounting::default();
    let mut cursor = read
        .begin_scan(
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions {
                projection: CoreProjection::FullValue,
                ..BeginScanOptions::default()
            },
        )
        .await
        .expect("begin CAS accounting scan");
    loop {
        let (page, page_has_more) = cursor
            .next_page(MAX_SCAN_PAGE_ROWS)
            .await
            .expect("scan CAS accounting space").into_parts();
        accounting.rows += page.len() as u64;
        accounting.key_bytes += page
            .iter()
            .map(|entry| entry.key.0.len() as u64)
            .sum::<u64>();
        accounting.value_bytes += page
            .iter()
            .map(|entry| match &entry.value {
                ProjectedValue::KeyOnly => 0,
                ProjectedValue::FullValue(value) => value.len() as u64,
            })
            .sum::<u64>();
        if !page_has_more {
            break;
        }
    }
    accounting
}

fn print_layout(backend: &str, size_mib: usize, label: &str, layout: CasLayout) {
    println!(
        "media_cas_layout,backend={backend},size_mib={size_mib},label={label},\
         manifests={},manifest_bytes={},manifest_chunks={},manifest_chunk_bytes={},\
         payload_chunks={},payload_bytes={},presence_rows={},presence_bytes={},total_rows={},total_bytes={}",
        layout.manifest.rows,
        layout.manifest.value_bytes,
        layout.manifest_chunk.rows,
        layout.manifest_chunk.value_bytes,
        layout.payload.rows,
        layout.payload.value_bytes,
        layout.presence.rows,
        layout.presence.value_bytes,
        layout.total_rows(),
        layout.total_bytes(),
    );
}

fn print_amplification(
    backend: &str,
    size_mib: usize,
    operation: &str,
    before: CasLayout,
    after: CasLayout,
) {
    println!(
        "media_cas_amplification,backend={backend},size_mib={size_mib},operation={operation},\
         manifest_rows_delta={},manifest_chunk_rows_delta={},payload_chunk_rows_delta={},presence_rows_delta={},\
         cas_rows_delta={},cas_bytes_delta={}",
        after.manifest.rows.saturating_sub(before.manifest.rows),
        after
            .manifest_chunk
            .rows
            .saturating_sub(before.manifest_chunk.rows),
        after.payload.rows.saturating_sub(before.payload.rows),
        after.presence.rows.saturating_sub(before.presence.rows),
        after.total_rows().saturating_sub(before.total_rows()),
        after.total_bytes().saturating_sub(before.total_bytes()),
    );
}

async fn print_owner_inventory<S>(backend: &str, size_mib: usize, storage: &S)
where
    S: Storage + Clone,
{
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(ReadOptions::default())
        .await
        .expect("open owner inventory read");
    let owners = binary_cas_owner_layout_accounting(&read)
        .await
        .expect("inventory binary CAS owners");
    println!(
        "media_owner_inventory,backend={backend},size_mib={size_mib},owners={},references={},manifests={},logical_bytes={},encoded_manifest_bytes={},chunk_values={},encoded_chunk_bytes={}",
        owners.len(),
        owners.iter().map(|owner| owner.references).sum::<u64>(),
        owners.iter().map(|owner| owner.manifests).sum::<u64>(),
        owners.iter().map(|owner| owner.logical_bytes).sum::<u64>(),
        owners
            .iter()
            .map(|owner| owner.encoded_manifest_bytes)
            .sum::<u64>(),
        owners.iter().map(|owner| owner.chunk_values).sum::<u64>(),
        owners
            .iter()
            .map(|owner| owner.encoded_chunk_bytes)
            .sum::<u64>(),
    );
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

fn start_rss_sampler() -> (Arc<AtomicBool>, Arc<AtomicU64>, std::thread::JoinHandle<()>) {
    let stop = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicU64::new(current_rss_kib()));
    let thread_stop = Arc::clone(&stop);
    let thread_peak = Arc::clone(&peak);
    let handle = std::thread::spawn(move || {
        while !thread_stop.load(Ordering::Acquire) {
            thread_peak.fetch_max(current_rss_kib(), Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(1));
        }
        thread_peak.fetch_max(current_rss_kib(), Ordering::Relaxed);
    });
    (stop, peak, handle)
}

fn current_rss_kib() -> u64 {
    proc_status_kib("VmRSS:")
}

fn process_hwm_kib() -> u64 {
    proc_status_kib("VmHWM:")
}

fn proc_status_kib(prefix: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with(prefix))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(0)
}

fn process_cpu_ticks() -> u64 {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return 0;
    };
    let Some(after_name) = stat.rsplit_once(") ").map(|(_, tail)| tail) else {
        return 0;
    };
    let fields = after_name.split_whitespace().collect::<Vec<_>>();
    let user = fields.get(11).and_then(|value| value.parse::<u64>().ok());
    let system = fields.get(12).and_then(|value| value.parse::<u64>().ok());
    user.zip(system).map_or(0, |(user, system)| user + system)
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| directory_bytes(&entry.path()))
        .sum()
}
