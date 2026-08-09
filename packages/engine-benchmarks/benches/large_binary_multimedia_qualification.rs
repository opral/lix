use std::alloc::{GlobalAlloc, Layout};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use blake3::Hasher;
use bytes::Bytes;
use lix::integration::{Engine, SessionContext};
use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, ProjectedValue,
    PutBatch, ReadOptions, ScanChunk, ScanCursor, Storage, StorageError, StorageRead,
    StorageScanSource, StorageSpace, StorageWrite, WriteOptions,
};
use lix::{
    CreateBranchOptions, ExecuteOptions, ExecuteStatementMetadata, FILE_UPLOAD_PART_BYTES,
    MergeBranchOptions, MergeBranchOutcome, RequestBlobSpliceProvenance, Value,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use sha2::{Digest, Sha256};

const SIZE: usize = 64 * 1024 * 1024;
const CANONICAL_CHUNK_BYTES: usize = 1024 * 1024;
const APPEND_SIZE: usize = SIZE + 1024 * 1024;
const EDIT_START: usize = SIZE / 2;
const EDIT_LEN: usize = 1024 * 1024;
const PATH: &str = "/media/foreground.mov";
const MERGE_SOURCE_PATH: &str = "/media/merge-source.bin";
const MERGE_TARGET_PATH: &str = "/media/merge-target.bin";
const SHARED_LEFT_PATH: &str = "/media/shared-left.bin";
const SHARED_RIGHT_PATH: &str = "/media/shared-right.bin";
const BRANCH_ID: &str = "01980000-0000-7000-8000-000000000064";

struct CountingAllocator;
static ALLOC_ON: AtomicBool = AtomicBool::new(false);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !ptr.is_null() && ALLOC_ON.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) };
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(ptr, layout, size) };
        if !replacement.is_null() && size > layout.size() && ALLOC_ON.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add((size - layout.size()) as u64, Ordering::Relaxed);
        }
        replacement
    }
}

#[derive(Clone, Copy, Default)]
struct Io {
    begin_reads: u64,
    begin_writes: u64,
    get_many_calls: u64,
    get_many_keys: u64,
    get_many_bytes: u64,
    scans: u64,
    scan_rows: u64,
    puts: u64,
    deletes: u64,
    logical_write_bytes: u64,
    commits: u64,
    backend_puts: u64,
    backend_deletes: u64,
    backend_bytes: u64,
}

#[derive(Clone)]
struct CountStorage<S> {
    inner: S,
    io: Arc<Mutex<Io>>,
}

impl<S> CountStorage<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            io: Arc::new(Mutex::new(Io::default())),
        }
    }
    fn reset(&self) {
        *self.io.lock().expect("io lock") = Io::default();
    }
    fn snapshot(&self) -> Io {
        *self.io.lock().expect("io lock")
    }
}

impl<S: Storage> Storage for CountStorage<S> {
    type Read<'a>
        = CountRead<S::Read<'a>>
    where
        Self: 'a;
    type Write<'a>
        = CountWrite<S::Write<'a>>
    where
        Self: 'a;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.io.lock().expect("io lock").begin_reads += 1;
        Ok(CountRead {
            inner: self.inner.begin_read(options).await?,
            io: Arc::clone(&self.io),
        })
    }
    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.io.lock().expect("io lock").begin_writes += 1;
        Ok(CountWrite {
            inner: self.inner.begin_write(options).await?,
            io: Arc::clone(&self.io),
        })
    }
}

struct CountRead<R> {
    inner: R,
    io: Arc<Mutex<Io>>,
}

impl<R: StorageRead> StorageRead for CountRead<R> {
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        {
            let mut io = self.io.lock().expect("io lock");
            io.get_many_calls += 1;
            io.get_many_keys += requests.iter().map(|r| r.keys.len() as u64).sum::<u64>();
        }
        let result = self.inner.get_many(requests).await?;
        let mut io = self.io.lock().expect("io lock");
        for value in result.values.iter().flatten() {
            if let ProjectedValue::FullValue(value) = value {
                io.get_many_bytes += value.len() as u64;
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
        self.io.lock().expect("io lock").scans += 1;
        let order = options.order;
        let inner = self.inner.begin_scan(space, range.clone(), options).await?;
        ScanCursor::from_source(
            range,
            order,
            CountScan {
                inner,
                io: Arc::clone(&self.io),
            },
        )
    }
}

struct CountScan<'a> {
    inner: ScanCursor<'a>,
    io: Arc<Mutex<Io>>,
}
impl StorageScanSource for CountScan<'_> {
    fn next_page(
        &mut self,
        limit: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            let page = self.inner.next_page(limit).await?;
            self.io.lock().expect("io lock").scan_rows += page.entries.len() as u64;
            Ok(page)
        })
    }
}

struct CountWrite<W> {
    inner: W,
    io: Arc<Mutex<Io>>,
}
impl<W: StorageWrite> StorageWrite for CountWrite<W> {
    async fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        {
            let mut io = self.io.lock().expect("io lock");
            io.puts += entries.entries.len() as u64;
            io.logical_write_bytes += entries
                .entries
                .iter()
                .map(|e| (e.key.0.len() + e.value.bytes.len()) as u64)
                .sum::<u64>();
        }
        self.inner.put_many(space, entries).await
    }
    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        self.io.lock().expect("io lock").deletes += keys.len() as u64;
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
        let io = Arc::clone(&self.io);
        let result = self.inner.commit().await?;
        let mut stats = io.lock().expect("io lock");
        stats.commits += 1;
        stats.backend_puts += result.stats.put_entries;
        stats.backend_deletes += result.stats.deleted_entries;
        stats.backend_bytes += result.stats.written_bytes;
        Ok(result)
    }
    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

#[async_trait]
trait BenchBackend: Storage + Clone + Send + Sync + 'static {
    async fn flush_backend(&self);
    fn open_backend(path: &Path) -> Self;
}
#[async_trait]
impl BenchBackend for RocksDB {
    async fn flush_backend(&self) {
        self.flush().expect("flush RocksDB");
    }
    fn open_backend(path: &Path) -> Self {
        Self::open(path).expect("open RocksDB")
    }
}
#[async_trait]
impl BenchBackend for SlateDB {
    async fn flush_backend(&self) {
        self.flush().await.expect("flush SlateDB");
    }
    fn open_backend(path: &Path) -> Self {
        Self::open(path).expect("open SlateDB")
    }
}

#[derive(Clone, Copy)]
struct Timed {
    wall_ns: u64,
    cpu_ticks: u64,
    alloc_bytes: u64,
    rss_hwm_kib: u64,
    disk_delta: i128,
    io: Io,
}

async fn timed<S, F, T>(label: &str, storage: &CountStorage<S>, path: &Path, future: F) -> T
where
    S: Storage,
    F: Future<Output = T>,
{
    storage.reset();
    let before_disk = disk_bytes(path);
    let cpu_before = cpu_ticks();
    let started = Instant::now();
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    ALLOC_ON.store(true, Ordering::Release);
    let value = future.await;
    ALLOC_ON.store(false, Ordering::Release);
    let elapsed = started.elapsed();
    let metric = Timed {
        wall_ns: elapsed.as_nanos() as u64,
        cpu_ticks: cpu_ticks().saturating_sub(cpu_before),
        alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
        rss_hwm_kib: rss_hwm_kib(),
        disk_delta: disk_bytes(path) as i128 - before_disk as i128,
        io: storage.snapshot(),
    };
    println!(
        "{}",
        serde_json::json!({"event":"metric","label":label,"wall_ms":metric.wall_ns as f64/1e6,"cpu_ticks":metric.cpu_ticks,"alloc_bytes":metric.alloc_bytes,"rss_hwm_kib":metric.rss_hwm_kib,"disk_delta":metric.disk_delta,"io":{
            "begin_reads":metric.io.begin_reads,"begin_writes":metric.io.begin_writes,"get_many_calls":metric.io.get_many_calls,"get_many_keys":metric.io.get_many_keys,"get_many_value_bytes":metric.io.get_many_bytes,"scans":metric.io.scans,"scan_rows":metric.io.scan_rows,"puts":metric.io.puts,"deletes":metric.io.deletes,"logical_write_bytes":metric.io.logical_write_bytes,"commits":metric.io.commits,"backend_puts":metric.io.backend_puts,"backend_deletes":metric.io.backend_deletes,"backend_written_bytes":metric.io.backend_bytes
        }})
    );
    value
}

async fn active_commit<S: Storage + Clone>(session: &SessionContext<S>) -> String {
    let result = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("active commit");
    result.rows()[0]
        .get::<String>("commit_id")
        .expect("commit id")
        .to_owned()
}

async fn run<S: BenchBackend>(label: &str, storage: S, path: PathBuf) {
    let counted = CountStorage::new(storage);
    let receipt = Engine::initialize(counted.clone())
        .await
        .expect("initialize");
    let engine = Engine::new(counted.clone()).await.expect("engine");
    let session = engine
        .open_session(receipt.main_branch_id.clone())
        .await
        .expect("session");
    let base = payload(SIZE, 0x1234);
    let edited = edited_payload(&base);
    let appended = payload(APPEND_SIZE, 0x5678);

    let chunks = base
        .chunks(FILE_UPLOAD_PART_BYTES)
        .map(Bytes::copy_from_slice)
        .collect::<Vec<_>>();
    timed(
        &format!("{label}/insert_64m_upload"),
        &counted,
        &path,
        async {
            for (index, part) in chunks.iter().enumerate() {
                let progress = session
                    .upsert_file_content_part(
                        "64m-bench".to_owned(),
                        PATH.to_owned(),
                        (index * FILE_UPLOAD_PART_BYTES) as u64,
                        SIZE as u64,
                        part.clone().into(),
                    )
                    .await
                    .expect("upload part");
                assert_eq!(
                    progress.next_offset,
                    ((index + 1) * FILE_UPLOAD_PART_BYTES) as u64
                );
            }
        },
    )
    .await;
    session.close().await.expect("close after insert");

    let session = engine
        .open_session(receipt.main_branch_id.clone())
        .await
        .expect("reopen session");
    let file_id = session
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(PATH.to_owned())],
        )
        .await
        .expect("file id query")
        .rows()[0]
        .get::<String>("id")
        .expect("file id")
        .to_owned();
    let first = timed(
        &format!("{label}/exact_read_64m"),
        &counted,
        &path,
        session.read_file_content(PATH.to_owned(), None),
    )
    .await
    .expect("read")
    .expect("file");
    assert_eq!(first.content().len(), SIZE);
    assert_eq!(digest(first.content()), digest(&base));
    drop(first);
    let range = timed(
        &format!("{label}/middle_range_1m"),
        &counted,
        &path,
        session.read_file_content(
            PATH.to_owned(),
            Some(EDIT_START as u64..(EDIT_START + EDIT_LEN) as u64),
        ),
    )
    .await
    .expect("range")
    .expect("range file");
    assert_eq!(range.content().len(), EDIT_LEN);
    drop(range);

    let edited_blob: lix::Blob = edited.clone().into();
    let splice_provenance = RequestBlobSpliceProvenance::new_validated(
        &base,
        &edited_blob,
        &sha256_hex(&base),
        &sha256_hex(&edited_blob),
        EDIT_START,
        SIZE - EDIT_START - EDIT_LEN,
        vec![0xa5; EDIT_LEN],
    )
    .expect("authenticated middle splice");
    let before_update = active_commit(&session).await;
    lix::storage_bench::begin_verified_inline_blob_splice_accounting();
    let _updated = timed(
        &format!("{label}/middle_overwrite_1m"),
        &counted,
        &path,
        session.execute_with_options_and_metadata(
            "UPDATE lix_file SET content = $1 WHERE id = $2",
            &[
                Value::Blob(edited_blob.clone()),
                Value::Text(file_id.clone()),
            ],
            ExecuteOptions::default(),
            ExecuteStatementMetadata {
                parameter_blob_splices: vec![Some(splice_provenance), None],
                ..ExecuteStatementMetadata::default()
            },
        ),
    )
    .await
    .expect("overwrite");
    let splice_accounting = lix::storage_bench::take_verified_inline_blob_splice_accounting();
    assert_eq!(
        splice_accounting.calls, 1,
        "SQL update must consume verified splice"
    );
    assert_eq!(splice_accounting.changed_chunks, 1);
    assert_eq!(
        splice_accounting.total_chunks,
        (SIZE / CANONICAL_CHUNK_BYTES) as u64
    );
    println!(
        "{}",
        serde_json::json!({
            "event": "verified_splice",
            "label": format!("{label}/middle_overwrite_1m"),
            "calls": splice_accounting.calls,
            "changed_chunks": splice_accounting.changed_chunks,
            "unchanged_chunks": splice_accounting.total_chunks - splice_accounting.changed_chunks,
            "total_chunks": splice_accounting.total_chunks,
        })
    );
    let after_update = active_commit(&session).await;
    let diff_sql =
        format!("SELECT COUNT(*) AS entries FROM lix_diff('{before_update}', '{after_update}')");
    let diff = session.execute(&diff_sql, &[]).await.expect("diff");
    assert!(!diff.rows().is_empty());
    let updated = session
        .read_file_content(PATH.to_owned(), None)
        .await
        .expect("updated file read")
        .expect("updated file");
    assert_eq!(digest(updated.content()), digest(&edited));
    drop(updated);

    // A same-size provenance packet from another base must fail at the
    // publication owner before selector/manifest writes. This is both the
    // wrong-base and rollback control for the SQL route; the current content
    // must remain visible and its commit identity must not advance.
    let wrong_base = payload(SIZE, 0x9abc);
    let wrong_result = edited_payload(&wrong_base);
    let wrong_result_blob: lix::Blob = wrong_result.clone().into();
    let wrong_provenance = RequestBlobSpliceProvenance::new_validated(
        &wrong_base,
        &wrong_result_blob,
        &sha256_hex(&wrong_base),
        &sha256_hex(&wrong_result_blob),
        EDIT_START,
        SIZE - EDIT_START - EDIT_LEN,
        vec![0xa5; EDIT_LEN],
    )
    .expect("wrong-base control provenance");
    let before_rejected = active_commit(&session).await;
    let rejected = session
        .execute_with_options_and_metadata(
            "UPDATE lix_file SET content = $1 WHERE id = $2",
            &[Value::Blob(wrong_result_blob), Value::Text(file_id.clone())],
            ExecuteOptions::default(),
            ExecuteStatementMetadata {
                parameter_blob_splices: vec![Some(wrong_provenance), None],
                ..ExecuteStatementMetadata::default()
            },
        )
        .await;
    assert!(
        rejected.is_err(),
        "transplanted same-size base must fail closed"
    );
    assert_eq!(active_commit(&session).await, before_rejected);
    let preserved = session
        .read_file_content(PATH.to_owned(), None)
        .await
        .expect("read after rejected splice")
        .expect("file after rejected splice");
    assert_eq!(digest(preserved.content()), digest(&edited));
    drop(preserved);

    let _append = timed(
        &format!("{label}/append_1m"),
        &counted,
        &path,
        session.upsert_file_content(PATH.to_owned(), appended.clone().into()),
    )
    .await
    .expect("append");

    let _truncated = timed(
        &format!("{label}/truncate_1m"),
        &counted,
        &path,
        session.upsert_file_content(PATH.to_owned(), base.clone().into()),
    )
    .await
    .expect("truncate");
    let truncated = session
        .read_file_content(PATH.to_owned(), None)
        .await
        .expect("truncated file read")
        .expect("truncated file");
    assert_eq!(truncated.content().len(), SIZE);
    assert_eq!(digest(truncated.content()), digest(&base));
    drop(truncated);

    session.create_checkpoint().await.expect("checkpoint");
    // Re-establish the 65 MiB branch baseline after the measured truncation;
    // this write is setup for the branch/merge controls, not a timed phase.
    session
        .upsert_file_content(PATH.to_owned(), appended.clone().into())
        .await
        .expect("restore appended branch baseline");
    let branch = session
        .create_branch(CreateBranchOptions {
            id: Some(BRANCH_ID.to_owned()),
            name: "64m benchmark branch".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("branch");
    let branch_session = engine
        .open_session(branch.id.clone())
        .await
        .expect("branch session");
    let branch_read = timed(
        &format!("{label}/branch_read"),
        &counted,
        &path,
        branch_session.read_file_content(PATH.to_owned(), None),
    )
    .await
    .expect("branch read")
    .expect("branch file");
    assert_eq!(digest(branch_read.content()), digest(&appended));
    drop(branch_read);

    let merge_source = payload(1024 * 1024, 0x9def);
    branch_session
        .upsert_file_content(MERGE_SOURCE_PATH.to_owned(), merge_source.clone().into())
        .await
        .expect("source branch write");

    let merge_target = payload(1024 * 1024, 0xace0);
    session
        .upsert_file_content(MERGE_TARGET_PATH.to_owned(), merge_target.clone().into())
        .await
        .expect("target branch write");
    let merge_receipt = timed(
        &format!("{label}/true_merge"),
        &counted,
        &path,
        session.merge_branch(MergeBranchOptions {
            source_branch_id: branch.id.clone(),
        }),
    )
    .await
    .expect("merge");
    assert_eq!(merge_receipt.outcome, MergeBranchOutcome::MergeCommitted);
    let merged_source = session
        .read_file_content(MERGE_SOURCE_PATH.to_owned(), None)
        .await
        .expect("merged source read")
        .expect("merged source");
    assert_eq!(digest(merged_source.content()), digest(&merge_source));
    drop(merged_source);
    let merged_target = session
        .read_file_content(MERGE_TARGET_PATH.to_owned(), None)
        .await
        .expect("merged target read")
        .expect("merged target");
    assert_eq!(digest(merged_target.content()), digest(&merge_target));
    drop(merged_target);

    let shared = payload(1024 * 1024, 0xbeef);
    session
        .upsert_file_content(SHARED_LEFT_PATH.to_owned(), shared.clone().into())
        .await
        .expect("shared left write");
    session
        .upsert_file_content(SHARED_RIGHT_PATH.to_owned(), shared.clone().into())
        .await
        .expect("shared right write");
    timed(
        &format!("{label}/shared_reference_release"),
        &counted,
        &path,
        session.execute(
            "DELETE FROM lix_file WHERE path = $1",
            &[Value::Text(SHARED_LEFT_PATH.to_owned())],
        ),
    )
    .await
    .expect("shared reference release");
    let shared_after_release = session
        .read_file_content(SHARED_RIGHT_PATH.to_owned(), None)
        .await
        .expect("shared survivor read")
        .expect("shared survivor");
    assert_eq!(digest(shared_after_release.content()), digest(&shared));
    drop(shared_after_release);
    timed(
        &format!("{label}/shared_final_delete"),
        &counted,
        &path,
        session.execute(
            "DELETE FROM lix_file WHERE path = $1",
            &[Value::Text(SHARED_RIGHT_PATH.to_owned())],
        ),
    )
    .await
    .expect("shared final deletion");
    assert!(
        session
            .read_file_content(SHARED_RIGHT_PATH.to_owned(), None)
            .await
            .expect("shared deleted read")
            .is_none()
    );
    branch_session.close().await.expect("close branch");
    session.close().await.expect("close main");
    drop(engine);
    counted.inner.flush_backend().await;

    let reopened_raw = S::open_backend(&path);
    let reopened = CountStorage::new(reopened_raw);
    let reopened_engine = Engine::new(reopened.clone()).await.expect("reopen engine");
    let reopened_session = reopened_engine
        .open_session(receipt.main_branch_id)
        .await
        .expect("reopen main");
    let cold = timed(
        &format!("{label}/cold_reopen_exact_read"),
        &reopened,
        &path,
        reopened_session.read_file_content(PATH.to_owned(), None),
    )
    .await
    .expect("cold read")
    .expect("cold file");
    assert_eq!(digest(cold.content()), digest(&appended));
    drop(cold);
    timed(
        &format!("{label}/final_reference_delete"),
        &reopened,
        &path,
        reopened_session.execute(
            "DELETE FROM lix_file WHERE path = $1",
            &[Value::Text(PATH.to_owned())],
        ),
    )
    .await
    .expect("final reference deletion");
    assert!(
        reopened_session
            .read_file_content(PATH.to_owned(), None)
            .await
            .expect("deleted file read")
            .is_none()
    );
    reopened_session.close().await.expect("close reopened");
    println!(
        "{}",
        serde_json::json!({"event":"result","backend":label,"final_reference_deletion":"public_sql_delete_from_lix_file","digest":digest(&appended),"bytes":appended.len()})
    );
}

fn payload(size: usize, seed: u64) -> Vec<u8> {
    let mut bytes = vec![0u8; size];
    let mut state = seed ^ 0xd1b5_4a32_d192_ed03;
    for chunk in bytes.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
    }
    bytes
}
fn edited_payload(base: &[u8]) -> Vec<u8> {
    let mut bytes = base.to_vec();
    bytes[EDIT_START..EDIT_START + EDIT_LEN].fill(0xa5);
    bytes
}
fn digest(bytes: &[u8]) -> String {
    let mut h = Hasher::new();
    h.update(bytes);
    h.finalize().to_hex().to_string()
}
fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}
fn cpu_ticks() -> u64 {
    std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|s| {
            s.rsplit_once(") ").and_then(|(_, rest)| {
                let fields = rest.split_whitespace().collect::<Vec<_>>();
                fields
                    .get(11)
                    .and_then(|v| v.parse::<u64>().ok())
                    .zip(fields.get(12).and_then(|v| v.parse::<u64>().ok()))
            })
        })
        .map(|(u, s)| u + s)
        .unwrap_or(0)
}
fn rss_hwm_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0)
}
fn disk_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .map(|e| {
            e.ok()
                .map(|e| {
                    let p = e.path();
                    if p.is_dir() {
                        disk_bytes(&p)
                    } else {
                        e.metadata().map(|m| m.len()).unwrap_or(0)
                    }
                })
                .unwrap_or(0)
        })
        .sum()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let backend = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "rocksdb".to_owned());
    let path = std::env::args()
        .nth(2)
        .map(PathBuf::from)
        .expect("database path");
    std::fs::create_dir_all(&path).expect("database parent");
    match backend.as_str() {
        "rocksdb" => run("rocksdb", RocksDB::open(&path).expect("open RocksDB"), path).await,
        "slatedb" => run("slatedb", SlateDB::open(&path).expect("open SlateDB"), path).await,
        other => panic!("backend must be rocksdb or slatedb, got {other}"),
    }
}
