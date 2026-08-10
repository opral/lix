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
    MergeBranchPreviewOptions, RequestBlobSpliceProvenance, Value,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use sha2::{Digest, Sha256};

const CANONICAL_CHUNK_BYTES: usize = 1024 * 1024;
const PATH: &str = "/media/foreground.mov";
const BRANCH_ID: &str = "01980000-0000-7000-8000-000000000064";

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    size: usize,
    append_size: usize,
    edit_start: usize,
    edit_len: usize,
}

impl Scenario {
    fn parse(value: Option<&str>) -> Self {
        let value = value.unwrap_or("64m");
        let (name, size) = match value {
            "1m" => ("1m", 1 << 20),
            "64m" => ("64m", 64 << 20),
            "256m" => ("256m", 256 << 20),
            other => panic!("size must be 1m, 64m, or 256m, got {other}"),
        };
        let edit_len = (size / 16).max(64 * 1024).min(CANONICAL_CHUNK_BYTES);
        Self {
            name,
            size,
            append_size: size + CANONICAL_CHUNK_BYTES,
            edit_start: if size > CANONICAL_CHUNK_BYTES * 2 {
                size / 2
            } else {
                0
            },
            edit_len,
        }
    }

    fn canonical_chunk_count(self, size: usize) -> usize {
        size.div_ceil(CANONICAL_CHUNK_BYTES)
    }
}

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
    timed_measured(label, storage, path, future).await.0
}

async fn timed_measured<S, F, T>(
    label: &str,
    storage: &CountStorage<S>,
    path: &Path,
    future: F,
) -> (T, Timed)
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
    (value, metric)
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
    let scenario = Scenario::parse(std::env::args().nth(3).as_deref());
    run_for_scenario(label, storage, path, scenario).await;
}

async fn run_for_scenario<S: BenchBackend>(
    label: &str,
    storage: S,
    path: PathBuf,
    scenario: Scenario,
) {
    let counted = CountStorage::new(storage);
    let receipt = Engine::initialize(counted.clone())
        .await
        .expect("initialize");
    let engine = Engine::new(counted.clone()).await.expect("engine");
    let session = engine
        .open_session(receipt.main_branch_id.clone())
        .await
        .expect("session");
    let base = payload(scenario.size, 0x1234);
    let edited = edited_payload(&base, scenario.edit_start, scenario.edit_len);
    let appended_suffix = payload(scenario.append_size - scenario.size, 0x5678);
    let mut appended = edited.clone();
    appended.extend_from_slice(&appended_suffix);

    let chunks = base
        .chunks(FILE_UPLOAD_PART_BYTES)
        .map(Bytes::copy_from_slice)
        .collect::<Vec<_>>();
    timed(
        &format!("{label}/insert_{}_upload", scenario.name),
        &counted,
        &path,
        async {
            for (index, part) in chunks.iter().enumerate() {
                let progress = session
                    .upsert_file_content_part(
                        "64m-bench".to_owned(),
                        PATH.to_owned(),
                        (index * FILE_UPLOAD_PART_BYTES) as u64,
                        scenario.size as u64,
                        part.clone().into(),
                    )
                    .await
                    .expect("upload part");
                assert_eq!(
                    progress.next_offset,
                    (((index + 1) * FILE_UPLOAD_PART_BYTES).min(scenario.size)) as u64
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
        &format!("{label}/exact_read_{}", scenario.name),
        &counted,
        &path,
        session.read_file_content(PATH.to_owned(), None),
    )
    .await
    .expect("read")
    .expect("file");
    assert_eq!(first.content().len(), scenario.size);
    assert_eq!(digest(first.content()), digest(&base));
    drop(first);
    let range = timed(
        &format!("{label}/middle_range_{}", scenario.edit_len),
        &counted,
        &path,
        session.read_file_content(
            PATH.to_owned(),
            Some(scenario.edit_start as u64..(scenario.edit_start + scenario.edit_len) as u64),
        ),
    )
    .await
    .expect("range")
    .expect("range file");
    assert_eq!(range.content().len(), scenario.edit_len);
    drop(range);

    let edited_blob: lix::Blob = edited.clone().into();
    let splice_provenance = RequestBlobSpliceProvenance::new_validated(
        &base,
        &edited_blob,
        &sha256_hex(&base),
        &sha256_hex(&edited_blob),
        scenario.edit_start,
        scenario.size - scenario.edit_start - scenario.edit_len,
        vec![0xa5; scenario.edit_len],
    )
    .expect("authenticated middle splice");
    let before_update = active_commit(&session).await;
    let _updated = timed(
        &format!("{label}/middle_overwrite_{}", scenario.edit_len),
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
    let after_update = active_commit(&session).await;
    let diff_sql =
        format!("SELECT COUNT(*) AS entries FROM lix_diff('{before_update}', '{after_update}')");
    let diff = timed(
        &format!("{label}/diff_metadata"),
        &counted,
        &path,
        session.execute(&diff_sql, &[]),
    )
    .await
    .expect("diff");
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
    let wrong_base = payload(scenario.size, 0x9abc);
    let wrong_result = edited_payload(&wrong_base, scenario.edit_start, scenario.edit_len);
    let wrong_result_blob: lix::Blob = wrong_result.clone().into();
    let wrong_provenance = RequestBlobSpliceProvenance::new_validated(
        &wrong_base,
        &wrong_result_blob,
        &sha256_hex(&wrong_base),
        &sha256_hex(&wrong_result_blob),
        scenario.edit_start,
        scenario.size - scenario.edit_start - scenario.edit_len,
        vec![0xa5; scenario.edit_len],
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

    let appended_blob: lix::Blob = appended.clone().into();
    let append_provenance = RequestBlobSpliceProvenance::new_validated(
        &edited,
        &appended_blob,
        &sha256_hex(&edited),
        &sha256_hex(&appended_blob),
        edited.len(),
        0,
        appended_suffix,
    )
    .expect("authenticated append splice");
    let (append_result, append_metric) = timed_measured(
        &format!("{label}/append_{}", scenario.name),
        &counted,
        &path,
        session.execute_with_options_and_metadata(
            "UPDATE lix_file SET content = $1 WHERE id = $2",
            &[Value::Blob(appended_blob), Value::Text(file_id.clone())],
            ExecuteOptions::default(),
            ExecuteStatementMetadata {
                parameter_blob_splices: vec![Some(append_provenance), None],
                ..ExecuteStatementMetadata::default()
            },
        ),
    )
    .await;
    append_result.expect("append");
    assert!(
        append_metric.io.puts <= 48,
        "1 MiB append emitted {} puts instead of one chunk plus a bounded Merkle path",
        append_metric.io.puts
    );
    assert!(
        append_metric.io.logical_write_bytes <= 2 * 1024 * 1024,
        "1 MiB append staged {} logical bytes",
        append_metric.io.logical_write_bytes
    );
    assert!(
        append_metric.io.backend_bytes <= 2 * 1024 * 1024,
        "1 MiB append wrote {} backend bytes",
        append_metric.io.backend_bytes
    );
    println!(
        "{}",
        serde_json::json!({
            "event": "verified_variable_splice",
            "label": format!("{label}/append_{}", scenario.name),
            "changed_chunks": 1,
            "reused_chunk_object_ids": scenario.canonical_chunk_count(edited.len()),
            "total_chunks": scenario.canonical_chunk_count(appended.len()),
        })
    );
    session.create_checkpoint().await.expect("checkpoint");
    let branch = session
        .create_branch(CreateBranchOptions {
            id: Some(BRANCH_ID.to_owned()),
            name: "64m benchmark branch".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("branch");
    let retained_branch_id = branch.id.clone();
    let merge_preview = timed(
        &format!("{label}/merge_metadata"),
        &counted,
        &path,
        session.merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: retained_branch_id.clone(),
        }),
    )
    .await
    .expect("merge preview");
    assert_eq!(merge_preview.source_branch_id, retained_branch_id);
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
        &format!("{label}/cold_reopen_exact_read_{}", scenario.name),
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
        &format!("{label}/shared_reference_delete"),
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
    let retained_branch = reopened_engine
        .open_session(retained_branch_id)
        .await
        .expect("open retained branch after main deletion");
    let retained = retained_branch
        .read_file_content(PATH.to_owned(), None)
        .await
        .expect("retained branch read after main deletion")
        .expect("shared branch reference must retain blob");
    assert_eq!(digest(retained.content()), digest(&appended));
    drop(retained);
    retained_branch
        .close()
        .await
        .expect("close retained branch");
    reopened_session.close().await.expect("close reopened");
    println!(
        "{}",
        serde_json::json!({
            "event":"result",
            "backend":label,
            "scenario":scenario.name,
            "shared_reference_deletion":"main_deleted_branch_retained",
            "digest":digest(&appended),
            "logical_payload_bytes":appended.len(),
            "physical_disk_bytes":disk_bytes(&path),
            "base_edited_shared_chunk_bytes":shared_chunk_bytes(&base, &edited),
            "edited_appended_shared_chunk_bytes":shared_chunk_bytes(&edited, &appended),
            "branch_shared_chunk_bytes":appended.len(),
            "canonical_chunk_count":scenario.canonical_chunk_count(appended.len()),
        })
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
fn edited_payload(base: &[u8], edit_start: usize, edit_len: usize) -> Vec<u8> {
    let mut bytes = base.to_vec();
    bytes[edit_start..edit_start + edit_len].fill(0xa5);
    bytes
}
fn shared_chunk_bytes(left: &[u8], right: &[u8]) -> usize {
    left.chunks(CANONICAL_CHUNK_BYTES)
        .zip(right.chunks(CANONICAL_CHUNK_BYTES))
        .filter(|(left, right)| left == right)
        .map(|(left, _)| left.len())
        .sum()
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
