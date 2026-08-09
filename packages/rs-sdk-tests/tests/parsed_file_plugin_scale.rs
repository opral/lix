#![allow(clippy::large_futures)]

//! Public-API version-control/files baseline for exact `origin/main`.
//!
//! The fixture is deliberately real: branches, SQL history/diff, Markdown
//! plugin parsing, and binary file identity all enter through `Lix`.  Storage
//! setup, plugin installation, fixture seeding, and checkpoint creation are
//! outside each timed interval.  This file is evidence harness only and is
//! not part of the production crate.

use async_trait::async_trait;
use libc::{RUSAGE_SELF, getrusage, rusage};
use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, ProjectedValue,
    PutBatch, ReadOptions, ScanChunk, ScanCursor, Storage, StorageError, StorageRead,
    StorageScanSource, StorageSpace, StorageWrite, WriteOptions,
};
use lix::{
    CreateBranchOptions, CreateBranchReceipt, Lix, MergeBranchOptions, SwitchBranchOptions, Value,
    open_lix, wasm::WasmTransitionCounters,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};
use mimalloc::MiMalloc;
use sha2::{Digest as _, Sha256};
use std::alloc::{GlobalAlloc, Layout};
use std::future::Future;
use std::io::{Cursor, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;
static ALLOCATION_ENABLED: AtomicBool = AtomicBool::new(false);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { MiMalloc.alloc(layout) };
        if !pointer.is_null() && ALLOCATION_ENABLED.load(Ordering::Relaxed) {
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { MiMalloc.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let replacement = unsafe { MiMalloc.realloc(pointer, layout, size) };
        if !replacement.is_null()
            && size > layout.size()
            && ALLOCATION_ENABLED.load(Ordering::Relaxed)
        {
            ALLOCATED_BYTES.fetch_add((size - layout.size()) as u64, Ordering::Relaxed);
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
        *self.stats.lock().unwrap() = IoStats::default();
    }
    fn snapshot(&self) -> IoStats {
        *self.stats.lock().unwrap()
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
        self.stats.lock().unwrap().begin_reads += 1;
        Ok(CountingRead {
            inner: self.inner.begin_read(options).await?,
            stats: Arc::clone(&self.stats),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.stats.lock().unwrap().begin_writes += 1;
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
            let mut stats = self.stats.lock().unwrap();
            stats.get_many_calls += 1;
            stats.get_many_keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
        }
        let result = self.inner.get_many(requests).await?;
        let mut stats = self.stats.lock().unwrap();
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
        self.stats.lock().unwrap().scan_calls += 1;
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
            let chunk = self.inner.next_page(limit_rows).await?;
            let mut stats = self.stats.lock().unwrap();
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
            let mut stats = self.stats.lock().unwrap();
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
            let mut stats = self.stats.lock().unwrap();
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
        let mut captured = stats.lock().unwrap();
        captured.commits += 1;
        captured.backend_puts += result.stats.put_entries;
        captured.backend_deletes += result.stats.deleted_entries;
        captured.backend_written_bytes += result.stats.written_bytes;
        captured.backend_storage_calls += result.stats.storage_calls;
        Ok(result)
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

#[async_trait]
trait Flushable: Storage + Clone + Send + Sync + 'static {
    async fn flush_baseline(&self);
}

#[async_trait]
impl Flushable for RocksDB {
    async fn flush_baseline(&self) {
        self.flush().expect("flush RocksDB baseline");
    }
}

#[async_trait]
impl Flushable for SlateDB {
    async fn flush_baseline(&self) {
        self.flush().await.expect("flush SlateDB baseline");
    }
}

#[async_trait]
trait Reopenable: Flushable {
    fn reopen(path: &Path) -> Self;
}

impl Reopenable for RocksDB {
    fn reopen(path: &Path) -> Self {
        RocksDB::open(path).expect("reopen RocksDB parsed-file fixture")
    }
}

impl Reopenable for SlateDB {
    fn reopen(path: &Path) -> Self {
        SlateDB::open(path).expect("reopen SlateDB parsed-file fixture")
    }
}

#[derive(Clone, Debug)]
struct Fixture {
    main_branch_id: String,
    markdown_file_id: String,
    large_file_id: String,
    large_bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default)]
struct Usage {
    user_ns: u64,
    system_ns: u64,
    rss_bytes: u64,
}

fn usage() -> Usage {
    let mut value = rusage {
        ru_utime: libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
        ru_stime: libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
        ru_maxrss: 0,
        ..unsafe { std::mem::zeroed() }
    };
    let result = unsafe { getrusage(RUSAGE_SELF, &mut value) };
    assert_eq!(result, 0, "getrusage");
    let micros = |time: libc::timeval| (time.tv_sec as u64) * 1_000_000 + time.tv_usec as u64;
    Usage {
        user_ns: micros(value.ru_utime) * 1_000,
        system_ns: micros(value.ru_stime) * 1_000,
        rss_bytes: (value.ru_maxrss as u64) * 1024,
    }
}

fn disk_bytes(path: &Path) -> u64 {
    let Ok(metadata) = std::fs::metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| disk_bytes(&entry.path()))
        .sum()
}

fn digest_bytes(parts: &[&[u8]]) -> String {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update(part);
    }
    format!("{:x}", hash.finalize())
}

fn digest_rows(result: &lix::ExecuteResult) -> String {
    let mut hash = Sha256::new();
    for row in result.rows() {
        hash.update(format!("{:?}\n", row.values()).as_bytes());
    }
    format!("{:x}", hash.finalize())
}

async fn query_digest<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
    sql: &str,
    params: &[Value],
) -> String {
    let result = lix.execute(sql, params).await.expect("baseline query");
    digest_rows(&result)
}

async fn write_file<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
    path: &str,
    data: Vec<u8>,
) {
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) ON CONFLICT(path) DO UPDATE SET content=excluded.content",
        &[Value::Text(path.to_owned()), Value::Blob(data.into())],
    ).await.expect("baseline file write");
}

async fn file_id<S: Storage + Clone + Send + Sync + 'static>(lix: &Lix<S>, path: &str) -> String {
    lix.execute(
        "SELECT id FROM lix_file WHERE path=$1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .expect("baseline file id query")
    .rows()[0]
        .get::<String>("id")
        .expect("file id")
}

fn markdown_bytes(version: usize) -> Vec<u8> {
    let mut result = format!("# Main baseline {version}\n\n").into_bytes();
    while result.len() < 64 * 1024 {
        result.extend_from_slice(
            format!(
                "## Section {}\n\nA parsed paragraph with stable words and sample {}.\n\n",
                result.len(),
                version
            )
            .as_bytes(),
        );
    }
    result.truncate(64 * 1024);
    result
}

fn plugin_archive() -> Vec<u8> {
    let mut output = std::io::Cursor::new(Vec::new());
    let mut zip = ZipWriter::new(&mut output);
    let options = SimpleFileOptions::default();
    let wasm = std::fs::read(env!("CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_plugin_markdown"))
        .expect("Markdown plugin artifact");
    zip.start_file("manifest.json", options).unwrap();
    zip.write_all(include_bytes!("../../../plugins/markdown/manifest.json"))
        .unwrap();
    zip.start_file("schema/markdown_node.json", options)
        .unwrap();
    zip.write_all(include_bytes!(
        "../../../plugins/markdown/schema/markdown_node.json"
    ))
    .unwrap();
    zip.start_file("plugin.wasm", options).unwrap();
    zip.write_all(&wasm).unwrap();
    zip.finish().unwrap();
    output.into_inner()
}

async fn seed<S: Flushable>(lix: &Lix<CountingStorage<S>>, main_branch_id: String) -> Fixture {
    let archive = plugin_archive();
    write_file(lix, "/.lix/plugins/plugin_markdown.lixplugin", archive).await;
    let markdown = markdown_bytes(0);
    write_file(lix, "/baseline.md", markdown).await;
    let markdown_file_id = file_id(lix, "/baseline.md").await;
    for index in 0..12 {
        lix.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ($1,$2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            &[Value::Text(format!("vc-seed-{index}")), Value::Text(format!("value-{index}"))],
        ).await.expect("seed key/value");
    }
    let large_bytes = (0..(4 * 1024 * 1024))
        .map(|index| (index as u8).wrapping_mul(31))
        .collect::<Vec<_>>();
    write_file(lix, "/large.bin", large_bytes.clone()).await;
    let large_file_id = file_id(lix, "/large.bin").await;
    let _ = lix.create_checkpoint().await.expect("seed checkpoint");
    Fixture {
        main_branch_id,
        markdown_file_id,
        large_file_id,
        large_bytes,
    }
}

async fn measure<S, F, Fut>(
    storage: &CountingStorage<S>,
    slate: Option<&SlateDBIoCounters>,
    root: &Path,
    backend: &str,
    operation: &str,
    sample: usize,
    action: F,
) -> String
where
    S: Storage,
    F: FnOnce() -> Fut,
    Fut: Future<Output = String>,
{
    storage.reset();
    let before_io = slate.map(SlateDBIoCounters::snapshot);
    let before_usage = usage();
    let disk_before = disk_bytes(root);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATION_ENABLED.store(true, Ordering::Relaxed);
    let started = Instant::now();
    let digest = action().await;
    let wall_ns = started.elapsed().as_nanos();
    ALLOCATION_ENABLED.store(false, Ordering::Relaxed);
    let after_usage = usage();
    let disk_after = disk_bytes(root);
    let stats = storage.snapshot();
    let io = slate.map(SlateDBIoCounters::snapshot).unwrap_or_default();
    let io_before = before_io.unwrap_or_default();
    let io_delta = io.saturating_sub(io_before);
    println!(
        "VC_BASELINE backend={backend} operation={operation} sample={sample} wall_ns={wall_ns} user_cpu_ns={} system_cpu_ns={} alloc_bytes={} alloc_calls={} rss_before={} rss_after={} begin_reads={} begin_writes={} get_many_calls={} get_many_keys={} get_many_found={} get_many_value_bytes={} scan_calls={} scan_rows={} puts={} put_batches={} deletes={} delete_batches={} logical_write_bytes={} commits={} backend_puts={} backend_deletes={} backend_written_bytes={} backend_storage_calls={} slate_read_objects={} slate_read_bytes={} slate_write_objects={} slate_write_bytes={} disk_before={} disk_after={} digest={digest}",
        after_usage.user_ns.saturating_sub(before_usage.user_ns),
        after_usage.system_ns.saturating_sub(before_usage.system_ns),
        ALLOCATED_BYTES.load(Ordering::Relaxed),
        ALLOCATION_CALLS.load(Ordering::Relaxed),
        before_usage.rss_bytes,
        after_usage.rss_bytes,
        stats.begin_reads,
        stats.begin_writes,
        stats.get_many_calls,
        stats.get_many_keys,
        stats.get_many_found,
        stats.get_many_value_bytes,
        stats.scan_calls,
        stats.scan_rows,
        stats.puts,
        stats.put_batches,
        stats.deletes,
        stats.delete_batches,
        stats.logical_write_bytes,
        stats.commits,
        stats.backend_puts,
        stats.backend_deletes,
        stats.backend_written_bytes,
        stats.backend_storage_calls,
        io_delta.read_objects,
        io_delta.read_bytes,
        io_delta.write_objects,
        io_delta.write_bytes,
        disk_before,
        disk_after,
    );
    digest
}

async fn run_common<S: Flushable>(
    raw: S,
    root: &Path,
    backend: &str,
    slate: Option<&SlateDBIoCounters>,
) -> (CountingStorage<S>, Lix<CountingStorage<S>>, Fixture) {
    let counted = CountingStorage::new(raw.clone());
    let lix = open_lix()
        .with_storage(counted.clone())
        .await
        .expect("open baseline Lix");
    let main_branch_id = lix.active_branch_id().await.expect("active branch id");
    let fixture = seed(&lix, main_branch_id).await;
    println!(
        "VC_BASELINE_FIXTURE backend={backend} markdown_file_id={} large_file_id={} large_bytes={} corpus_digest={}",
        fixture.markdown_file_id,
        fixture.large_file_id,
        fixture.large_bytes.len(),
        digest_bytes(&[&fixture.large_bytes])
    );

    let skip_all_history = std::env::var_os("LIX_VC_SKIP_HISTORY").is_some();
    let skip_key_history =
        skip_all_history || std::env::var_os("LIX_VC_SKIP_KEY_HISTORY").is_some();
    let skip_file_history =
        skip_all_history || std::env::var_os("LIX_VC_SKIP_FILE_HISTORY").is_some();
    println!(
        "VC_BASELINE_MODE backend={backend} skip_key_history={skip_key_history} skip_file_history={skip_file_history}"
    );

    for sample in 0..5 {
        let branch_id = format!("01990000-0000-7000-8000-{sample:012x}");
        let digest = measure(
            &counted,
            slate,
            root,
            backend,
            "branch_create_switch",
            sample,
            || async {
                let receipt = lix
                    .create_branch(CreateBranchOptions {
                        id: Some(branch_id.clone()),
                        name: format!("baseline-{sample}"),
                        from_commit_id: None,
                    })
                    .await
                    .expect("create branch");
                lix.switch_branch(SwitchBranchOptions {
                    branch_id: receipt.id.clone(),
                })
                .await
                .expect("switch branch");
                lix.switch_branch(SwitchBranchOptions {
                    branch_id: fixture.main_branch_id.clone(),
                })
                .await
                .expect("switch main");
                digest_bytes(&[
                    receipt.name.as_bytes(),
                    receipt.hidden.to_string().as_bytes(),
                ])
            },
        )
        .await;
        println!(
            "VC_BASELINE_CANONICAL backend={backend} operation=branch_create_switch sample={sample} digest={digest}"
        );
    }

    for sample in 0..5 {
        lix.create_checkpoint()
            .await
            .expect("diff checkpoint setup");
        lix.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ($1,$2)",
            &[
                Value::Text(format!("vc-diff-{sample}")),
                Value::Text("changed".into()),
            ],
        )
        .await
        .expect("diff mutation setup");
        let digest = measure(&counted, slate, root, backend, "diff", sample, || async {
            query_digest(&lix, "SELECT schema_key, entity_pk, diff_type FROM lix_working_diff ORDER BY schema_key, entity_pk", &[]).await
        }).await;
        println!(
            "VC_BASELINE_CANONICAL backend={backend} operation=diff sample={sample} digest={digest}"
        );
    }

    if !skip_key_history {
        for sample in 0..5 {
            let key = format!("vc-history-{sample}");
            for revision in 0..3 {
                lix.execute("INSERT INTO lix_key_value (key,value) VALUES ($1,$2) ON CONFLICT(key) DO UPDATE SET value=excluded.value", &[Value::Text(key.clone()), Value::Text(format!("revision-{revision}"))]).await.expect("history revision");
            }
            let digest = measure(&counted, slate, root, backend, "history", sample, || async {
                query_digest(&lix, "SELECT key, value, lixcol_depth FROM lix_key_value_history() WHERE key=$1 ORDER BY lixcol_depth", &[Value::Text(key.clone())]).await
            }).await;
            println!(
                "VC_BASELINE_CANONICAL backend={backend} operation=history sample={sample} digest={digest}"
            );
        }
    }

    for sample in 0..5 {
        let bytes = markdown_bytes(sample + 1);
        let digest = measure(
            &counted,
            slate,
            root,
            backend,
            "parsed_file_mutation",
            sample,
            || async {
                write_file(&lix, "/baseline.md", bytes.clone()).await;
                query_digest(
                    &lix,
                    "SELECT content FROM lix_file WHERE id=$1",
                    &[Value::Text(fixture.markdown_file_id.clone())],
                )
                .await
            },
        )
        .await;
        println!(
            "VC_BASELINE_CANONICAL backend={backend} operation=parsed_file_mutation sample={sample} digest={digest}"
        );
        if !skip_file_history {
            let head = lix
                .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
                .await
                .expect("active head")
                .rows()[0]
                .get::<String>("commit_id")
                .expect("active head id");
            let history_digest = measure(&counted, slate, root, backend, "parsed_file_history", sample, || async {
                query_digest(&lix, &format!("SELECT content,lixcol_depth FROM lix_file_history('{head}') WHERE id=$1 ORDER BY lixcol_depth LIMIT 5"), &[Value::Text(fixture.markdown_file_id.clone())]).await
            }).await;
            println!(
                "VC_BASELINE_CANONICAL backend={backend} operation=parsed_file_history sample={sample} digest={history_digest}"
            );
        }
    }

    for sample in 0..5 {
        let digest = measure(
            &counted,
            slate,
            root,
            backend,
            "large_file_read_reuse",
            sample,
            || async {
                query_digest(
                    &lix,
                    "SELECT content FROM lix_file WHERE id=$1",
                    &[Value::Text(fixture.large_file_id.clone())],
                )
                .await
            },
        )
        .await;
        println!(
            "VC_BASELINE_CANONICAL backend={backend} operation=large_file_read_reuse sample={sample} digest={digest}"
        );
        let mut edited = fixture.large_bytes.clone();
        edited[sample * 97] ^= 0x5a;
        write_file(&lix, "/large.bin", edited).await;
        assert_eq!(
            file_id(&lix, "/large.bin").await,
            fixture.large_file_id,
            "large file identity must be stable"
        );
    }

    for sample in 0..5 {
        let branch_id = format!("019a0000-0000-7000-8000-{sample:012x}");
        let receipt: CreateBranchReceipt = lix
            .create_branch(CreateBranchOptions {
                id: Some(branch_id.clone()),
                name: format!("merge-{sample}"),
                from_commit_id: None,
            })
            .await
            .expect("merge branch setup");
        lix.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ($1,$2)",
            &[
                Value::Text(format!("vc-target-{sample}")),
                Value::Text("target".into()),
            ],
        )
        .await
        .expect("target edit");
        let source = lix
            .open_session(receipt.id.clone())
            .await
            .expect("open source session");
        source
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ($1,$2)",
                &[
                    Value::Text(format!("vc-source-{sample}")),
                    Value::Text("source".into()),
                ],
            )
            .await
            .expect("source edit");
        source.close().await.expect("close source session");
        let digest = measure(&counted, slate, root, backend, "merge", sample, || async {
            let merged = lix
                .merge_branch(MergeBranchOptions {
                    source_branch_id: receipt.id.clone(),
                })
                .await
                .expect("merge branch");
            digest_bytes(&[
                merged.source_branch_id.as_bytes(),
                merged.change_stats.total.to_le_bytes().as_ref(),
                merged.change_stats.added.to_le_bytes().as_ref(),
                merged.change_stats.modified.to_le_bytes().as_ref(),
                merged.change_stats.removed.to_le_bytes().as_ref(),
            ])
        })
        .await;
        println!(
            "VC_BASELINE_CANONICAL backend={backend} operation=merge sample={sample} digest={digest}"
        );
    }
    raw_flush(&raw).await;
    (counted, lix, fixture)
}

async fn raw_flush<S: Flushable>(raw: &S) {
    raw.flush_baseline().await;
}

async fn cold_rocks(root: &Path, fixture: &Fixture, samples: usize) {
    for sample in 0..samples {
        let raw = RocksDB::open(root.join(".lix")).expect("reopen RocksDB");
        let counted = CountingStorage::new(raw.clone());
        let digest = measure(
            &counted,
            None,
            root,
            "rocksdb",
            "cold_reopen",
            sample,
            || async {
                let reopened = open_lix()
                    .with_storage(counted.clone())
                    .await
                    .expect("reopen Lix RocksDB");
                let digest = query_digest(
                    &reopened,
                    "SELECT id, content FROM lix_file WHERE id=$1",
                    &[Value::Text(fixture.large_file_id.clone())],
                )
                .await;
                reopened.close().await.expect("close reopened RocksDB Lix");
                digest
            },
        )
        .await;
        println!(
            "VC_BASELINE_CANONICAL backend=rocksdb operation=cold_reopen sample={sample} digest={digest}"
        );
        raw.flush().expect("flush reopened RocksDB");
    }
}

async fn cold_slate(root: &Path, fixture: &Fixture, samples: usize) {
    for sample in 0..samples {
        let counters = SlateDBIoCounters::default();
        let raw = SlateDB::open_with_io_counters(root.join(".lix"), counters.clone())
            .expect("reopen SlateDB");
        let counted = CountingStorage::new(raw.clone());
        let digest = measure(
            &counted,
            Some(&counters),
            root,
            "slatedb",
            "cold_reopen",
            sample,
            || async {
                let reopened = open_lix()
                    .with_storage(counted.clone())
                    .await
                    .expect("reopen Lix SlateDB");
                let digest = query_digest(
                    &reopened,
                    "SELECT id, content FROM lix_file WHERE id=$1",
                    &[Value::Text(fixture.large_file_id.clone())],
                )
                .await;
                reopened.close().await.expect("close reopened SlateDB Lix");
                digest
            },
        )
        .await;
        println!(
            "VC_BASELINE_CANONICAL backend=slatedb operation=cold_reopen sample={sample} digest={digest}"
        );
        raw.flush().await.expect("flush reopened SlateDB");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "bounded origin/main VC/files baseline; run one backend per cell"]
async fn vc_main_baseline() {
    let backend = std::env::var("LIX_VC_BACKEND").expect("set LIX_VC_BACKEND=rocksdb or slatedb");
    let root = tempfile::tempdir().expect("baseline root");
    println!(
        "VC_BASELINE_CONTRACT main=822c204ce0670969ca71045bc74f9ca25fde8093 tree=fac3f2b713683be17c34515062dd72edc8feed95 backend={backend} samples=5 setup_excluded=true public_api=true raw_row_model=false"
    );
    match backend.as_str() {
        "rocksdb" => {
            let raw = RocksDB::open(root.path().join(".lix")).expect("open RocksDB");
            let (counted, lix, fixture) =
                run_common(raw.clone(), root.path(), "rocksdb", None).await;
            lix.close().await.expect("close baseline RocksDB Lix");
            drop(counted);
            raw.flush().expect("flush baseline RocksDB");
            cold_rocks(root.path(), &fixture, 5).await;
        }
        "slatedb" => {
            let counters = SlateDBIoCounters::default();
            let raw = SlateDB::open_with_io_counters(root.path().join(".lix"), counters.clone())
                .expect("open SlateDB");
            let (counted, lix, fixture) =
                run_common(raw.clone(), root.path(), "slatedb", Some(&counters)).await;
            lix.close().await.expect("close baseline SlateDB Lix");
            drop(counted);
            raw.flush().await.expect("flush baseline SlateDB");
            cold_slate(root.path(), &fixture, 5).await;
        }
        other => panic!("unsupported LIX_VC_BACKEND={other}"),
    }
}

fn parsed_scale_bytes(plugin: &str, index: usize, version: usize) -> Vec<u8> {
    match plugin {
        "markdown" => format!(
            "# Parsed document {index}\n\nVersion {version}.\n\nA stable Markdown paragraph for the parsed-file workload.\n"
        )
        .into_bytes(),
        "text" => format!(
            "document={index}\nversion={version}\nA stable text line for the parsed-file workload.\n"
        )
        .into_bytes(),
        other => panic!("unsupported LIX_PARSED_PLUGIN={other}"),
    }
}

fn text_plugin_archive() -> Vec<u8> {
    let mut output = Cursor::new(Vec::new());
    let mut zip = ZipWriter::new(&mut output);
    let options = SimpleFileOptions::default();
    let wasm = std::fs::read(env!("CARGO_CDYLIB_FILE_PLUGIN_TEXT_plugin_text"))
        .expect("Text plugin artifact");
    zip.start_file("manifest.json", options).unwrap();
    zip.write_all(include_bytes!("../../../plugins/text/manifest.json"))
        .unwrap();
    zip.start_file("schema/text_line.json", options).unwrap();
    zip.write_all(include_bytes!(
        "../../../plugins/text/schema/text_line.json"
    ))
    .unwrap();
    zip.start_file("plugin.wasm", options).unwrap();
    zip.write_all(&wasm).unwrap();
    zip.finish().unwrap();
    output.into_inner()
}

async fn parsed_scale_digest<S: Storage + Clone + Send + Sync + 'static>(lix: &Lix<S>) -> String {
    query_digest(
        lix,
        "SELECT path, content FROM lix_file WHERE path LIKE '/plugin-scale/%' ORDER BY path",
        &[],
    )
    .await
}

fn print_plugin_counters(
    backend: &str,
    plugin: &str,
    files: usize,
    operation: &str,
    digest: &str,
    counters: WasmTransitionCounters,
) {
    println!(
        "PLUGIN_SCALE backend={backend} plugin={plugin} files={files} operation={operation} digest={digest} source_read_calls={} source_bytes_read={} file_read_calls={} file_bytes_read={} guest_export_calls={} entity_input_records={} entity_output_records={} entity_output_wire_bytes={} full_document_reparses={} full_renderer_invocations={} durable_semantic_changes={} host_content_classification_bytes={}",
        counters.source_read_calls,
        counters.source_bytes_read,
        counters.file_read_calls,
        counters.file_bytes_read,
        counters.guest_export_calls,
        counters.entity_input_records,
        counters.entity_output_records,
        counters.entity_output_wire_bytes,
        counters.full_document_reparses,
        counters.full_renderer_invocations,
        counters.durable_semantic_changes,
        counters.host_content_classification_bytes,
    );
}

async fn run_parsed_scale<S: Reopenable>(
    raw: S,
    root: &Path,
    backend: &str,
    slate: Option<&SlateDBIoCounters>,
    plugin: &str,
    files: usize,
) {
    let counted = CountingStorage::new(raw.clone());
    let lix = open_lix()
        .with_storage(counted.clone())
        .await
        .expect("open parsed-file scale workspace");
    let plugin_key = format!("plugin_{plugin}");
    let extension = if plugin == "markdown" { "md" } else { "txt" };
    let archive = if plugin == "markdown" {
        plugin_archive()
    } else {
        text_plugin_archive()
    };
    write_file(
        &lix,
        &format!("/.lix/plugins/{plugin_key}.lixplugin"),
        archive,
    )
    .await;

    lix.reset_plugin_transition_counters();
    let insert_digest = measure(
        &counted,
        slate,
        root,
        backend,
        "parsed_insert",
        files,
        || async {
            for index in 0..files {
                write_file(
                    &lix,
                    &format!("/plugin-scale/{index:05}.{extension}"),
                    parsed_scale_bytes(plugin, index, 0),
                )
                .await;
            }
            parsed_scale_digest(&lix).await
        },
    )
    .await;
    print_plugin_counters(
        backend,
        plugin,
        files,
        "parsed_insert",
        &insert_digest,
        lix.plugin_transition_counters(),
    );

    let edit_count = (files + 99) / 100;
    lix.reset_plugin_transition_counters();
    let edit_digest = measure(
        &counted,
        slate,
        root,
        backend,
        "parsed_edit_1pct",
        files,
        || async {
            for edit in 0..edit_count {
                let index = edit * 100;
                write_file(
                    &lix,
                    &format!("/plugin-scale/{index:05}.{extension}"),
                    parsed_scale_bytes(plugin, index, 1),
                )
                .await;
            }
            parsed_scale_digest(&lix).await
        },
    )
    .await;
    print_plugin_counters(
        backend,
        plugin,
        files,
        "parsed_edit_1pct",
        &edit_digest,
        lix.plugin_transition_counters(),
    );

    lix.close()
        .await
        .expect("close parsed-file scale workspace");
    drop(lix);
    drop(counted);
    raw.flush_baseline().await;
    drop(raw);

    let reopened_raw = S::reopen(&root.join(".lix"));
    let reopened_counted = CountingStorage::new(reopened_raw.clone());
    let reopened = open_lix()
        .with_storage(reopened_counted.clone())
        .await
        .expect("cold reopen parsed-file scale workspace");
    let cold_digest = measure(
        &reopened_counted,
        slate,
        root,
        backend,
        "parsed_cold_reopen",
        files,
        || async { parsed_scale_digest(&reopened).await },
    )
    .await;
    println!(
        "PLUGIN_SCALE backend={backend} plugin={plugin} files={files} operation=parsed_cold_reopen digest={cold_digest} cold_reopen=true"
    );

    let malformed_plugin = reopened
        .execute(
            "UPDATE lix_file SET content=$1 WHERE path=$2",
            &[
                Value::Blob(b"not-a-valid-plugin-archive".to_vec().into()),
                Value::Text(format!("/.lix/plugins/{plugin_key}.lixplugin")),
            ],
        )
        .await;
    let corruption_fail_closed = if malformed_plugin.is_err() {
        true
    } else {
        reopened
            .execute(
                "SELECT path, content FROM lix_file WHERE path LIKE '/plugin-scale/%' LIMIT 1",
                &[],
            )
            .await
            .is_err()
    };
    assert!(
        corruption_fail_closed,
        "malformed plugin archive must fail closed before parsed results"
    );
    println!(
        "PLUGIN_SCALE backend={backend} plugin={plugin} files={files} operation=corruption corruption_fail_closed=true"
    );
    reopened
        .close()
        .await
        .expect("close parsed-file corruption fixture");
    reopened_raw.flush_baseline().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "bounded parsed-file/plugin scale; run one backend/plugin/size per cell"]
async fn parsed_file_plugin_scale() {
    let backend =
        std::env::var("LIX_PARSED_BACKEND").expect("set LIX_PARSED_BACKEND=rocksdb or slatedb");
    let plugin = std::env::var("LIX_PARSED_PLUGIN").unwrap_or_else(|_| "markdown".to_owned());
    let files = std::env::var("LIX_PARSED_FILES")
        .expect("set LIX_PARSED_FILES=100,1000,or 10000")
        .parse::<usize>()
        .expect("LIX_PARSED_FILES must be numeric");
    assert!(matches!(files, 100 | 1_000 | 10_000));
    assert!(matches!(plugin.as_str(), "markdown" | "text"));
    assert!(matches!(backend.as_str(), "rocksdb" | "slatedb"));

    let root = tempfile::tempdir().expect("create parsed-file scale directory");
    let provenance_ref = std::env::var("LIX_PARSED_REF").unwrap_or_else(|_| "unset".to_owned());
    let provenance_tree = std::env::var("LIX_PARSED_TREE").unwrap_or_else(|_| "unset".to_owned());
    println!(
        "PLUGIN_SCALE_FIXTURE ref={provenance_ref} tree={provenance_tree} backend={backend} plugin={plugin} files={files} edit_percent=1 setup_excluded=true public_api=true raw_row_model=false"
    );
    match backend.as_str() {
        "rocksdb" => {
            let raw = RocksDB::open(root.path().join(".lix")).expect("open parsed-file RocksDB");
            run_parsed_scale(raw, root.path(), backend.as_str(), None, &plugin, files).await;
        }
        "slatedb" => {
            let counters = SlateDBIoCounters::default();
            let raw = SlateDB::open_with_io_counters(root.path().join(".lix"), counters.clone())
                .expect("open parsed-file SlateDB");
            run_parsed_scale(
                raw,
                root.path(),
                backend.as_str(),
                Some(&counters),
                &plugin,
                files,
            )
            .await;
        }
        _ => unreachable!(),
    }
}
