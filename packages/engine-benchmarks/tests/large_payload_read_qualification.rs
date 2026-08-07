#![allow(clippy::large_futures)]

use std::alloc::{GlobalAlloc, Layout};
use std::future::Future;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use lix::integration::{Engine, SessionContext};
use lix::storage::{
    CommitResult, CoreProjection, GetManyRequest, GetManyResult, Key, KeyRange, MAX_SCAN_PAGE_ROWS,
    ProjectedValue, PutBatch, ReadOptions, ScanChunk, ScanOptions, SpaceId, Storage, StorageError,
    StorageRead, StorageSpace, StorageWrite, WriteOptions,
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
const CAS_CHUNK_BYTES: usize = 1024 * 1024;
const RANGE_BYTES: u64 = 4 * 1024;
const SEED: u64 = 0x89a3_10fd_4242_73c1;
const GOP_BYTES: usize = 2 * 1024 * 1024;
const SOURCE_BRANCH_ID: &str = "01980000-0000-7000-8000-000000000064";
const RECLAIM_BRANCH_ID: &str = "01980000-0000-7000-8000-000000000065";
const UPSERT_SQL: &str = "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
                          ON CONFLICT (path) DO UPDATE SET content = excluded.content";
const DELETE_BRANCH_SQL: &str = "DELETE FROM lix_branch WHERE id = $1";

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

    async fn scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        self.stats.lock().expect("I/O stats mutex").scan_calls += 1;
        let chunk = self.inner.scan(space, range, options).await?;
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
        Ok(chunk)
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
    family: WorkloadFamily,
    parts: Vec<Bytes>,
    size: usize,
    edit_offset: usize,
    edit_bytes: usize,
    edit: Bytes,
    base_blake3: String,
    base_sha256: String,
    edited_blake3: String,
    edited_sha256: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkloadFamily {
    Image,
    Audio,
    Archive,
    Video,
}

impl WorkloadFamily {
    fn from_env() -> Self {
        match std::env::var("LIX_MEDIA_QUAL_FAMILY").as_deref() {
            Ok("image") => Self::Image,
            Ok("audio") => Self::Audio,
            Ok("archive") => Self::Archive,
            Ok("video") => Self::Video,
            Ok(value) => panic!("unknown workload family '{value}'"),
            Err(_) => panic!("LIX_MEDIA_QUAL_FAMILY is required"),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Audio => "audio",
            Self::Archive => "archive",
            Self::Video => "video",
        }
    }

    fn edit_offset(self, size: usize) -> usize {
        match self {
            Self::Image => size / 3 + 12_345,
            Self::Audio => size / 2 + 12_345,
            Self::Archive => size / 2,
            Self::Video => GOP_BYTES * 15 + 256 * 1024,
        }
    }

    fn expected_unique_payload_chunks(self, size: usize) -> u64 {
        match self {
            // The video fixture repeats one two-chunk GOP throughout the file,
            // so the authenticated manifest has 64 references but CAS stores
            // only the two unique payload chunks.
            Self::Video => (GOP_BYTES / CAS_CHUNK_BYTES) as u64,
            Self::Image | Self::Audio | Self::Archive => (size / CAS_CHUNK_BYTES) as u64,
        }
    }
}

struct ReopenExpectation {
    main_branch_id: String,
    size: usize,
    base_blake3: String,
    content_blake3: String,
    content_identity: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QualificationMode {
    Full,
    Seed,
    Run,
}

impl QualificationMode {
    fn from_env() -> Self {
        match std::env::var("LIX_MEDIA_QUAL_MODE").as_deref() {
            Ok("seed") => Self::Seed,
            Ok("run") => Self::Run,
            Ok(value) => panic!("qualification mode must be seed or run, got {value}"),
            Err(_) => Self::Full,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual large-media foreground qualification"]
async fn large_media_foreground_lifecycle() {
    let size_mib = std::env::var("LIX_MEDIA_QUAL_MIB")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64);
    assert!(
        matches!(size_mib, 64 | 512),
        "qualification size must be 64 or 512 MiB"
    );
    let mode = QualificationMode::from_env();
    let persistent_root = std::env::var_os("LIX_MEDIA_QUAL_FIXTURE_PATH").map(PathBuf::from);
    assert!(
        mode == QualificationMode::Full || persistent_root.is_some(),
        "seed/run qualification requires LIX_MEDIA_QUAL_FIXTURE_PATH"
    );
    let temp = persistent_root
        .is_none()
        .then(|| tempfile::tempdir().expect("create media qualification directory"));
    let root = persistent_root
        .as_deref()
        .or_else(|| temp.as_ref().map(tempfile::TempDir::path))
        .expect("qualification root must exist");
    std::fs::create_dir_all(root).expect("create persistent media qualification directory");
    let database = root.join("database");
    if mode == QualificationMode::Seed {
        assert!(
            !database.exists(),
            "seed qualification requires a fresh fixture path"
        );
    } else if mode == QualificationMode::Run {
        assert!(
            database.exists(),
            "run qualification requires a seeded fixture database"
        );
    }
    match std::env::var("LIX_MEDIA_QUAL_BACKEND").as_deref() {
        Ok("rocksdb") => {
            if mode == QualificationMode::Seed {
                seed_visible_fixture(
                    "rocksdb",
                    &database,
                    RocksDB::open(&database).expect("open qualification RocksDB seed"),
                    size_mib,
                )
                .await;
                return;
            }
            let expectation = qualify_visible_lifecycle(
                "rocksdb",
                &database,
                RocksDB::open(&database).expect("open qualification RocksDB"),
                None,
                size_mib,
                mode == QualificationMode::Run,
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
            if mode == QualificationMode::Seed {
                seed_visible_fixture(
                    "slatedb",
                    &database,
                    SlateDB::open(&database).expect("open qualification SlateDB seed"),
                    size_mib,
                )
                .await;
                return;
            }
            let counters = SlateDBIoCounters::default();
            let expectation = qualify_visible_lifecycle(
                "slatedb",
                &database,
                SlateDB::open_with_io_counters(&database, counters.clone())
                    .expect("open qualification SlateDB"),
                Some(counters),
                size_mib,
                mode == QualificationMode::Run,
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

async fn seed_visible_fixture<S>(backend: &str, database: &Path, raw_storage: S, size_mib: usize)
where
    S: QualificationStorage,
{
    let prepared = prepare_payload(size_mib * 1024 * 1024);
    let storage = CountingStorage::new(raw_storage);
    let receipt = Engine::initialize(storage.clone())
        .await
        .expect("initialize media qualification seed");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open media qualification seed engine");
    let main = engine
        .open_session(&receipt.main_branch_id)
        .await
        .expect("open media qualification seed session");
    upload_parts(&main, &prepared).await;
    storage.inner().qualification_flush().await;
    let layout = cas_layout(&storage).await;
    assert_eq!(layout.manifest.rows, 1);
    assert_eq!(layout.manifest_chunk.rows, size_mib as u64);
    assert_eq!(
        layout.payload.rows,
        prepared
            .family
            .expected_unique_payload_chunks(prepared.size)
    );
    assert_eq!(layout.presence.rows, layout.payload.rows);
    println!(
        "media_seed,backend={backend},family={},size_mib={size_mib},edit_bytes={},edit_offset={},\
         main_branch_id={},database_bytes={},base_blake3={},base_sha256={}",
        prepared.family.label(),
        prepared.edit_bytes,
        prepared.edit_offset,
        receipt.main_branch_id,
        directory_bytes(database),
        prepared.base_blake3,
        prepared.base_sha256,
    );
    main.close()
        .await
        .expect("close media qualification seed session");
}

async fn qualify_visible_lifecycle<S>(
    backend: &str,
    database: &Path,
    raw_storage: S,
    slate_counters: Option<SlateDBIoCounters>,
    size_mib: usize,
    seeded: bool,
) -> ReopenExpectation
where
    S: QualificationStorage,
{
    let prepared = prepare_payload(size_mib * 1024 * 1024);
    let storage = CountingStorage::new(raw_storage);
    let receipt = if seeded {
        None
    } else {
        Some(
            Engine::initialize(storage.clone())
                .await
                .expect("initialize media qualification engine"),
        )
    };
    let engine = Engine::new(storage.clone())
        .await
        .expect("open media qualification engine");
    let main = if let Some(receipt) = receipt.as_ref() {
        engine
            .open_session(&receipt.main_branch_id)
            .await
            .expect("open qualification main session")
    } else {
        engine
            .open_workspace_session()
            .await
            .expect("open seeded qualification main session")
    };
    let main_branch_id = main
        .active_branch_id()
        .await
        .expect("resolve qualification main branch");

    if !seeded {
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
    }
    println!(
        "media_edit_shape,backend={backend},family={},size_mib={size_mib},edit_bytes={},edit_offset={},edit_end={}",
        prepared.family.label(),
        prepared.edit_bytes,
        prepared.edit_offset,
        prepared.edit_offset + prepared.edit_bytes,
    );
    let ingest_layout = cas_layout(&storage).await;
    print_layout(backend, size_mib, "after_ingest", ingest_layout);
    assert_eq!(ingest_layout.manifest.rows, 1);
    assert_eq!(ingest_layout.manifest_chunk.rows, size_mib as u64);
    assert_eq!(
        ingest_layout.payload.rows,
        prepared
            .family
            .expected_unique_payload_chunks(prepared.size)
    );
    assert_eq!(ingest_layout.presence.rows, ingest_layout.payload.rows);
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
    let suffix_bytes = prepared.size - prepared.edit_offset - prepared.edit_bytes;
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
                prepared.edit.clone().into(),
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
    let first_edited_chunk = prepared.edit_offset / CAS_CHUNK_BYTES;
    let last_edited_chunk = (prepared.edit_offset + prepared.edit_bytes - 1) / CAS_CHUNK_BYTES;
    let edited_chunk_count = last_edited_chunk - first_edited_chunk + 1;
    assert!(
        edit_layout
            .payload
            .rows
            .saturating_sub(branch_layout.payload.rows)
            <= edited_chunk_count as u64,
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
        base_blake3: prepared.base_blake3,
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
    let reopened_head = active_commit_id(&main).await;
    let retained = measured(
        backend,
        size_mib,
        "retained_root_history_read",
        database,
        &storage,
        slate_counters.as_ref(),
        main.execute(
            "SELECT content FROM lix_file_history($1) \
             WHERE path = $2 ORDER BY lixcol_depth",
            &[Value::Text(reopened_head), Value::Text(PATH.to_owned())],
        ),
    )
    .await
    .expect("read retained base root through public file history");
    assert!(
        retained.rows().iter().any(|row| {
            row.get::<Vec<u8>>("content").is_ok_and(|bytes| {
                blake3::hash(&bytes).to_hex().as_str() == expected.base_blake3.as_str()
            })
        }),
        "public history must preserve the authenticated base payload",
    );
    qualify_final_reclamation(
        backend,
        database,
        &storage,
        slate_counters.as_ref(),
        &engine,
        &main,
        expected.size,
    )
    .await;
    let layout = cas_layout(&storage).await;
    print_layout(backend, size_mib, "after_reopen", layout);
    print_owner_inventory(backend, size_mib, &storage).await;
    main.close().await.expect("close reopened main session");
}

async fn qualify_final_reclamation<S>(
    backend: &str,
    database: &Path,
    storage: &CountingStorage<S>,
    slate_counters: Option<&SlateDBIoCounters>,
    engine: &Engine<CountingStorage<S>>,
    main: &SessionContext<CountingStorage<S>>,
    size: usize,
) where
    S: QualificationStorage,
{
    let family = WorkloadFamily::from_env();
    let before = cas_layout(storage).await;
    let current = measured(
        backend,
        size / (1024 * 1024),
        "reclamation_base_read",
        database,
        storage,
        slate_counters,
        main.read_file_content(PATH.to_owned(), None),
    )
    .await
    .expect("read reclamation base")
    .expect("reclamation base exists");
    let base = current.into_content();
    let edit_offset = family.edit_offset(size);
    let edit_bytes = size / 100;
    let suffix_bytes = size - edit_offset - edit_bytes;
    let replacement = family_bytes(
        family,
        edit_offset,
        edit_bytes,
        SEED ^ 0xa409_3822_299f_31d0,
    );
    let mut result_sha256 = Sha256::new();
    result_sha256.update(&base[..edit_offset]);
    result_sha256.update(&replacement);
    result_sha256.update(&base[size - suffix_bytes..]);
    let result_sha256 = format!("{:x}", result_sha256.finalize());
    let verified_base = VerifiedRequestBlob::verify(base);
    let base_sha256 = verified_base.sha256().to_owned();
    let (successor, provenance) = verified_base
        .reconstruct_splice(
            &base_sha256,
            &result_sha256,
            edit_offset,
            suffix_bytes,
            replacement.into(),
        )
        .expect("reconstruct dead-branch-only successor");
    let branch = measured(
        backend,
        size / (1024 * 1024),
        "reclamation_branch_create",
        database,
        storage,
        slate_counters,
        main.create_branch(CreateBranchOptions {
            id: Some(RECLAIM_BRANCH_ID.to_owned()),
            name: "Dead multimedia reclamation root".to_owned(),
            from_commit_id: None,
        }),
    )
    .await
    .expect("create reclamation branch");
    let dead_branch = engine
        .open_session(&branch.id)
        .await
        .expect("open reclamation branch");
    measured(
        backend,
        size / (1024 * 1024),
        "reclamation_unique_publish",
        database,
        storage,
        slate_counters,
        dead_branch.execute_with_options_and_metadata(
            UPSERT_SQL,
            &[
                Value::Text(PATH.to_owned()),
                Value::Blob(successor.blob().clone()),
            ],
            ExecuteOptions::default(),
            ExecuteStatementMetadata {
                parameter_blob_splices: vec![None, Some(provenance)],
                mutation_identity: None,
            },
        ),
    )
    .await
    .expect("publish dead-branch-only successor");
    let after_publish = cas_layout(storage).await;
    assert!(
        after_publish.total_rows() > before.total_rows(),
        "reclamation fixture must create unique CAS ownership",
    );
    dead_branch.close().await.expect("close reclamation branch");
    drop(dead_branch);
    measured(
        backend,
        size / (1024 * 1024),
        "reclamation_branch_delete",
        database,
        storage,
        slate_counters,
        main.execute(
            DELETE_BRANCH_SQL,
            &[Value::Text(RECLAIM_BRANCH_ID.to_owned())],
        ),
    )
    .await
    .expect("delete reclamation branch");
    measured(
        backend,
        size / (1024 * 1024),
        "reclamation_checkpoint_rotations",
        database,
        storage,
        slate_counters,
        async {
            for _ in 0..65 {
                main.create_checkpoint().await?;
            }
            Ok::<(), lix::LixError>(())
        },
    )
    .await
    .expect("rotate checkpoint roots for final reclamation");
    tokio::time::sleep(Duration::from_secs(1)).await;
    storage.inner().qualification_flush().await;
    let reclaimed = cas_layout(storage).await;
    println!(
        "media_final_reclamation,backend={backend},family={},before_rows={},after_publish_rows={},final_rows={},\
         before_bytes={},after_publish_bytes={},final_bytes={}",
        family.label(),
        before.total_rows(),
        after_publish.total_rows(),
        reclaimed.total_rows(),
        before.total_bytes(),
        after_publish.total_bytes(),
        reclaimed.total_bytes(),
    );
    assert_eq!(
        reclaimed, before,
        "final release must reclaim the dead-branch-only successor without deleting shared roots",
    );
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
    assert_eq!(size, 64 * 1024 * 1024, "family oracle is fixed at 64 MiB");
    let edit_percent = std::env::var("LIX_MEDIA_QUAL_EDIT_PERCENT").map_or(1, |value| {
        value
            .parse::<usize>()
            .unwrap_or_else(|_| panic!("invalid LIX_MEDIA_QUAL_EDIT_PERCENT value '{value}'"))
    });
    assert_eq!(edit_percent, 1, "family oracle edit must be exactly 1%");
    let family = WorkloadFamily::from_env();
    let edit_bytes = size / 100;
    let edit_offset = family.edit_offset(size);
    let edit_end = edit_offset
        .checked_add(edit_bytes)
        .expect("qualification edit end must fit usize");
    assert!(
        edit_end <= size,
        "qualification edit must fit inside the payload"
    );
    let edit = family_bytes(
        family,
        edit_offset,
        edit_bytes,
        SEED ^ 0x6a09_e667_f3bc_c909,
    );
    let mut parts = Vec::with_capacity(size / FILE_UPLOAD_PART_BYTES);
    let mut base_blake3 = blake3::Hasher::new();
    let mut edited_blake3 = blake3::Hasher::new();
    let mut base_sha256 = Sha256::new();
    let mut edited_sha256 = Sha256::new();
    for offset in (0..size).step_by(FILE_UPLOAD_PART_BYTES) {
        let bytes = family_bytes(family, offset, FILE_UPLOAD_PART_BYTES, SEED);
        base_blake3.update(&bytes);
        base_sha256.update(&bytes);
        if offset < edit_end && offset + bytes.len() > edit_offset {
            let local_start = edit_offset.saturating_sub(offset);
            let local_end = (edit_end - offset).min(bytes.len());
            let mut edited = bytes.clone();
            let edit_start = offset.saturating_sub(edit_offset);
            let edit_end = edit_start + local_end - local_start;
            edited[local_start..local_end].copy_from_slice(&edit[edit_start..edit_end]);
            edited_blake3.update(&edited);
            edited_sha256.update(&edited);
        } else {
            edited_blake3.update(&bytes);
            edited_sha256.update(&bytes);
        }
        parts.push(Bytes::from(bytes));
    }
    PreparedPayload {
        family,
        parts,
        size,
        edit_offset,
        edit_bytes,
        edit: Bytes::from(edit),
        base_blake3: base_blake3.finalize().to_hex().to_string(),
        base_sha256: format!("{:x}", base_sha256.finalize()),
        edited_blake3: edited_blake3.finalize().to_hex().to_string(),
        edited_sha256: format!("{:x}", edited_sha256.finalize()),
    }
}

fn family_bytes(family: WorkloadFamily, global_offset: usize, len: usize, seed: u64) -> Vec<u8> {
    match family {
        WorkloadFamily::Image => image_like_bytes(global_offset, len, seed),
        WorkloadFamily::Audio => {
            deterministic_bytes(len, seed ^ 0xa54f_f53a_5f1d_36f1 ^ global_offset as u64)
        }
        WorkloadFamily::Archive => deterministic_bytes(
            len,
            seed ^ 0x510e_527f_ade6_82d1 ^ (global_offset / CAS_CHUNK_BYTES) as u64,
        ),
        WorkloadFamily::Video => video_like_bytes(global_offset, len, seed),
    }
}

fn image_like_bytes(global_offset: usize, len: usize, seed: u64) -> Vec<u8> {
    let mut bytes = deterministic_bytes(len, seed ^ 0xbb67_ae85_84ca_a73b ^ global_offset as u64);
    for (index, byte) in bytes.iter_mut().enumerate() {
        let position = global_offset + index;
        let block = position / 4096;
        if block % 4 != 3 {
            let pixel = (position % 4096) / 4;
            let channel = position % 4;
            *byte = match channel {
                0 => (pixel as u8).wrapping_add(seed as u8),
                1 => ((pixel / 4) as u8).wrapping_add((seed >> 8) as u8),
                2 => (block as u8)
                    .wrapping_mul(3)
                    .wrapping_add((seed >> 16) as u8),
                _ => 0xff,
            };
        }
    }
    bytes
}

fn video_like_bytes(global_offset: usize, len: usize, seed: u64) -> Vec<u8> {
    let gop = deterministic_bytes(GOP_BYTES, seed ^ 0x3c6e_f372_fe94_f82b);
    let mut bytes = Vec::with_capacity(len);
    let mut position = global_offset;
    while bytes.len() < len {
        let gop_offset = position % GOP_BYTES;
        let count = (GOP_BYTES - gop_offset).min(len - bytes.len());
        bytes.extend_from_slice(&gop[gop_offset..gop_offset + count]);
        position += count;
    }
    bytes
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
    let family = WorkloadFamily::from_env().label();
    println!(
        "large_payload_read,backend={backend},family={family},size_mib={size_mib},operation={operation},\
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
        manifest: space_accounting(storage, MANIFEST_SPACE, false).await,
        manifest_chunk: space_accounting(storage, MANIFEST_CHUNK_SPACE, false).await,
        payload: space_accounting(storage, PAYLOAD_SPACE, true).await,
        presence: space_accounting(storage, PRESENCE_SPACE, false).await,
    }
}

async fn space_accounting<S>(storage: &S, id: SpaceId, immutable: bool) -> SpaceAccounting
where
    S: Storage,
{
    let space = if immutable {
        StorageSpace::immutable(id, "qualification.binary_cas_payload")
    } else {
        StorageSpace::mutable(id, "qualification.binary_cas_metadata")
    };
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("open CAS accounting read");
    let mut accounting = SpaceAccounting::default();
    let mut resume_after = None;
    loop {
        let page = read
            .scan(
                space,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                ScanOptions {
                    projection: CoreProjection::FullValue,
                    limit_rows: MAX_SCAN_PAGE_ROWS,
                    resume_after,
                },
            )
            .await
            .expect("scan CAS accounting space");
        accounting.rows += page.entries.len() as u64;
        accounting.key_bytes += page
            .entries
            .iter()
            .map(|entry| entry.key.0.len() as u64)
            .sum::<u64>();
        accounting.value_bytes += page
            .entries
            .iter()
            .map(|entry| match &entry.value {
                ProjectedValue::KeyOnly => 0,
                ProjectedValue::FullValue(value) => value.len() as u64,
            })
            .sum::<u64>();
        if !page.has_more {
            break;
        }
        resume_after = page.entries.last().map(|entry| entry.key.clone());
    }
    accounting
}

fn print_layout(backend: &str, size_mib: usize, label: &str, layout: CasLayout) {
    let family = WorkloadFamily::from_env().label();
    println!(
        "media_cas_layout,backend={backend},family={family},size_mib={size_mib},label={label},\
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
    let family = WorkloadFamily::from_env().label();
    println!(
        "media_cas_amplification,backend={backend},family={family},size_mib={size_mib},operation={operation},\
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
    let family = WorkloadFamily::from_env().label();
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(ReadOptions::default())
        .await
        .expect("open owner inventory read");
    let owners = binary_cas_owner_layout_accounting(&read)
        .await
        .expect("inventory binary CAS owners");
    println!(
        "media_owner_inventory,backend={backend},family={family},size_mib={size_mib},owners={},references={},manifests={},logical_bytes={},encoded_manifest_bytes={},chunk_values={},encoded_chunk_bytes={}",
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
