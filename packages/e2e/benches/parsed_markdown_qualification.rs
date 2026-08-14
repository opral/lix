//! Public parsed-Markdown qualification extracted from the retained E2E
//! workload. Setup adapters are compile-time selected and never timed.

use async_trait::async_trait;
use lix::storage::Storage;
use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, ProjectedValue,
    PutBatch, ReadOptions, ScanChunk, ScanCursor, StorageError, StorageRead, StorageScanSource,
    StorageSpace, StorageWrite, WriteOptions,
};
use lix::{CreateBranchOptions, Lix, SwitchBranchOptions, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use std::alloc::{GlobalAlloc, Layout};
use std::future::Future;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const PATH: &str = "/company/competitors.md";
const BRANCH_ID: &str = "01920000-0000-7000-8000-0000000005f1";

#[async_trait]
trait QualificationBackend: Storage + Clone + Send + Sync + 'static {
    fn open(path: &Path) -> Self;
    async fn flush_for_reopen(&self);
}

#[async_trait]
impl QualificationBackend for RocksDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open RocksDB")
    }

    async fn flush_for_reopen(&self) {
        self.flush().expect("flush RocksDB before cold reopen");
    }
}

#[async_trait]
impl QualificationBackend for SlateDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open SlateDB")
    }

    async fn flush_for_reopen(&self) {
        self.flush()
            .await
            .expect("flush SlateDB before cold reopen");
    }
}

struct CountingAllocator;
static ALLOC_ON: AtomicBool = AtomicBool::new(false);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[derive(Clone, Copy, Default)]
struct IoStats {
    get_many_calls: u64,
    get_many_keys: u64,
    scan_calls: u64,
    scan_rows: u64,
    scan_value_bytes: u64,
    write_batches: u64,
    puts: u64,
    deletes: u64,
    write_bytes: u64,
}

impl IoStats {
    fn delta(self, before: Self) -> Self {
        Self {
            get_many_calls: self.get_many_calls.saturating_sub(before.get_many_calls),
            get_many_keys: self.get_many_keys.saturating_sub(before.get_many_keys),
            scan_calls: self.scan_calls.saturating_sub(before.scan_calls),
            scan_rows: self.scan_rows.saturating_sub(before.scan_rows),
            scan_value_bytes: self
                .scan_value_bytes
                .saturating_sub(before.scan_value_bytes),
            write_batches: self.write_batches.saturating_sub(before.write_batches),
            puts: self.puts.saturating_sub(before.puts),
            deletes: self.deletes.saturating_sub(before.deletes),
            write_bytes: self.write_bytes.saturating_sub(before.write_bytes),
        }
    }
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

    fn snapshot(&self) -> IoStats {
        *self.stats.lock().expect("I/O stats mutex")
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

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        Ok(CountingRead {
            inner: self.inner.begin_read(opts).await?,
            stats: Arc::clone(&self.stats),
        })
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(CountingWrite {
            inner: self.inner.begin_write(opts).await?,
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
        self.inner.get_many(requests).await
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        let order = opts.order;
        self.stats.lock().expect("I/O stats mutex").scan_calls += 1;
        let inner = self.inner.begin_scan(space, range.clone(), opts).await?;
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
            let (chunk, has_more) = self.inner.next_page(limit_rows).await?.into_parts();
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
            Ok(ScanChunk::new(chunk, has_more))
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
            stats.write_batches += 1;
            stats.puts += entries.entries.len() as u64;
            stats.write_bytes += entries
                .entries
                .iter()
                .map(|entry| (entry.key.0.len() + entry.value.bytes.len()) as u64)
                .sum::<u64>();
        }
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        self.stats.lock().expect("I/O stats mutex").deletes += keys.len() as u64;
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
        self.inner.commit().await
    }
    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && ALLOC_ON.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, size) };
        if !replacement.is_null() && size > layout.size() && ALLOC_ON.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add((size - layout.size()) as u64, Ordering::Relaxed);
        }
        replacement
    }
}

#[cfg(not(feature = "parsed-markdown-future-adapter"))]
mod setup_adapter {
    use super::{Lix, Storage, open_lix};

    pub const NAME: &str = "main-public-api";

    pub async fn open<S>(storage: S) -> Lix<S>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        open_lix()
            .with_storage(storage)
            .await
            .expect("open repository")
    }
}

#[cfg(feature = "parsed-markdown-future-adapter")]
mod setup_adapter {
    use super::{Lix, Storage, open_lix};

    pub const NAME: &str = "future-integration-public-api";

    pub async fn open<S>(storage: S) -> Lix<S>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        open_lix()
            .with_storage(storage)
            .await
            .expect("open repository")
    }
}

#[derive(Clone, Copy)]
struct ProcessCounters {
    cpu_ticks: u64,
    rss_kib: u64,
    rss_hwm_kib: u64,
    read_bytes: u64,
    write_bytes: u64,
}

fn process_counters() -> ProcessCounters {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let fields = stat.split_ascii_whitespace().collect::<Vec<_>>();
    let cpu_ticks = fields
        .get(13..=14)
        .map(|pair| {
            pair.iter()
                .filter_map(|value| value.parse::<u64>().ok())
                .sum()
        })
        .unwrap_or(0);
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let status_value = |prefix| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(prefix))
            .and_then(|value| value.split_ascii_whitespace().next())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let io = std::fs::read_to_string("/proc/self/io").unwrap_or_default();
    let io_value = |prefix| {
        io.lines()
            .find_map(|line| line.strip_prefix(prefix))
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(0)
    };
    let rss_hwm_kib = status_value("VmHWM:");
    let rss_kib = status_value("VmRSS:");
    let read_bytes = io_value("read_bytes:");
    let write_bytes = io_value("write_bytes:");
    ProcessCounters {
        cpu_ticks,
        rss_kib,
        rss_hwm_kib,
        read_bytes,
        write_bytes,
    }
}

async fn measure<T, F, S>(
    backend: &str,
    operation: &str,
    units: u64,
    storage: &CountingStorage<S>,
    future: F,
) -> T
where
    F: Future<Output = T>,
{
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    let before = process_counters();
    let io_before = storage.snapshot();
    let started = Instant::now();
    ALLOC_ON.store(true, Ordering::Relaxed);
    let output = future.await;
    ALLOC_ON.store(false, Ordering::Relaxed);
    let elapsed = started.elapsed();
    let after = process_counters();
    let io = storage.snapshot().delta(io_before);
    println!(
        "{}",
        json!({
            "event": "metric",
            "backend": backend,
            "operation": operation,
            "elapsed_ms": elapsed.as_secs_f64() * 1000.0,
            "throughput_per_second": units as f64 / elapsed.as_secs_f64(),
            "cpu_ticks": after.cpu_ticks.saturating_sub(before.cpu_ticks),
            "alloc_bytes": ALLOC_BYTES.load(Ordering::Relaxed),
            "rss_before_kib": before.rss_kib,
            "rss_after_kib": after.rss_kib,
            "rss_hwm_kib": after.rss_hwm_kib,
            "read_bytes": after.read_bytes.saturating_sub(before.read_bytes),
            "write_bytes": after.write_bytes.saturating_sub(before.write_bytes),
            "storage_get_many_calls": io.get_many_calls,
            "storage_get_many_keys": io.get_many_keys,
            "storage_scan_calls": io.scan_calls,
            "storage_scan_rows": io.scan_rows,
            "storage_scan_value_bytes": io.scan_value_bytes,
            "storage_write_batches": io.write_batches,
            "storage_puts": io.puts,
            "storage_deletes": io.deletes,
            "storage_write_bytes": io.write_bytes,
        })
    );
    output
}

fn fixture() -> Vec<u8> {
    let mut source = b"---\nDateApproved: 6/10/2020\nOwner: team\n---\n\n# Competitors\n\n*Counter:\n\n(~26 users)\n\nA paragraph directly followed by\n- list item\n\n**knowledge base / shared workspace agents read and\nwrite to.**\n\n```rust\nlet value = *Counter;\n```\n\n".to_vec();
    for index in 0..24 {
        source.extend_from_slice(
            format!(
                "## Peer {index}\n\nPeer {index} has *single-asterisk emphasis*, Unicode λ 😀, and `code`.\n\n"
            )
            .as_bytes(),
        );
    }
    let mut padding_index = 0;
    while source.len() < 3_210 {
        source.extend_from_slice(format!("Padding paragraph {padding_index}.\n\n").as_bytes());
        padding_index += 1;
    }
    source
}

fn plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_plugin_markdown"));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built Markdown wasm at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/markdown/manifest.json").as_bytes(),
        ),
        (
            "schema/markdown_node.json",
            include_str!("../../../plugins/markdown/schema/markdown_node.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer
            .start_file(path, options)
            .expect("start archive file");
        writer.write_all(bytes).expect("write archive file");
    }
    writer.finish().expect("finish archive").into_inner()
}

async fn write_file<S>(lix: &Lix<S>, path: &str, content: Vec<u8>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        &[Value::Text(path.to_owned()), Value::Blob(content.into())],
    )
    .await
    .expect("write file");
}

async fn read_file<S>(lix: &Lix<S>, path: &str) -> Vec<u8>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT content FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .expect("read file")
    .rows()[0]
        .get::<Vec<u8>>("content")
        .expect("file content")
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn canonical_json_text(input: &str) -> String {
    fn remove_generated_identity(value: &mut JsonValue) {
        match value {
            JsonValue::Object(values) => {
                values.remove("id");
                values.remove("column_id");
                for child in values.values_mut() {
                    remove_generated_identity(child);
                }
            }
            JsonValue::Array(values) => {
                for child in values {
                    remove_generated_identity(child);
                }
            }
            _ => {}
        }
    }

    fn write(value: &JsonValue, output: &mut String) {
        match value {
            JsonValue::Array(values) => {
                output.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write(value, output);
                }
                output.push(']');
            }
            JsonValue::Object(values) => {
                output.push('{');
                let mut entries = values.iter().collect::<Vec<_>>();
                entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
                for (index, (key, value)) in entries.into_iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    output.push_str(&serde_json::to_string(key).expect("serialize JSON key"));
                    output.push(':');
                    write(value, output);
                }
                output.push('}');
            }
            scalar => output
                .push_str(&serde_json::to_string(scalar).expect("serialize canonical JSON scalar")),
        }
    }

    let mut value = serde_json::from_str(input).expect("valid payload JSON");
    // Markdown assigns durable UUIDs to inline nodes. They are intentionally
    // excluded from the plugin's semantic content signatures, so exclude the
    // same fields from cross-adapter benchmark result digests.
    remove_generated_identity(&mut value);
    let mut output = String::new();
    write(&value, &mut output);
    output
}

fn canonical_row_pk_text(input: &JsonValue) -> String {
    fn replace_generated_uuid(value: &mut JsonValue) {
        match value {
            JsonValue::String(text) if uuid::Uuid::parse_str(text).is_ok() => {
                *text = "<generated-uuid>".to_owned();
            }
            JsonValue::Array(values) => {
                for child in values {
                    replace_generated_uuid(child);
                }
            }
            JsonValue::Object(values) => {
                for child in values.values_mut() {
                    replace_generated_uuid(child);
                }
            }
            _ => {}
        }
    }

    let mut value = input.clone();
    replace_generated_uuid(&mut value);
    serde_json::to_string(&value).expect("serialize canonical row PK")
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                directory_bytes(&path)
            } else {
                entry.metadata().map(|metadata| metadata.len()).unwrap_or(0)
            }
        })
        .sum()
}

async fn install_plugin<S>(lix: &Lix<S>, archive: Vec<u8>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    write_file(lix, "/.lix/plugins/plugin_markdown.lixplugin", archive).await;
}

async fn run<S>(backend: &str, root: PathBuf)
where
    S: QualificationBackend,
{
    let expected = fixture();
    let expected_digest = digest(&expected);
    let storage = CountingStorage::new(S::open(&root.join(".lix")));
    let lix = setup_adapter::open(storage.clone()).await;

    // Fixture/plugin setup is intentionally outside timed regions.
    install_plugin(&lix, plugin_archive()).await;

    measure(backend, "parse_to_native_rows_insert", 1, &storage, async {
        write_file(&lix, PATH, expected.clone()).await
    })
    .await;
    assert_eq!(digest(&read_file(&lix, PATH).await), expected_digest);

    let file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(PATH.to_owned())],
        )
        .await
        .expect("file id")
        .rows()[0]
        .get::<String>("id")
        .expect("file id value");

    let paragraph_rows = lix
        .execute(
            "SELECT id, payload_json FROM markdown_node WHERE lixcol_file_id = $1 \
             AND kind = 'paragraph'",
            &[Value::Text(file_id.clone())],
        )
        .await
        .expect("point-query identity setup");
    let mut paragraph_candidates = paragraph_rows
        .rows()
        .iter()
        .map(|row| {
            (
                canonical_json_text(
                    &row.get::<String>("payload_json")
                        .expect("paragraph payload"),
                ),
                row.get::<String>("id").expect("paragraph id"),
            )
        })
        .collect::<Vec<_>>();
    paragraph_candidates.sort_unstable();
    let paragraph_id = paragraph_candidates[0].1.clone();

    let exact_digest = measure(backend, "exact_row_query", 1, &storage, async {
        let result = lix
            .execute(
                "SELECT id, kind, payload_json FROM markdown_node \
                 WHERE lixcol_file_id = $1 AND id = $2",
                &[
                    Value::Text(file_id.clone()),
                    Value::Text(paragraph_id.clone()),
                ],
            )
            .await
            .expect("exact row query");
        assert_eq!(result.rows().len(), 1);
        let row = &result.rows()[0];
        digest(
            format!(
                "{}\0{}",
                row.get::<String>("kind").expect("exact kind"),
                canonical_json_text(&row.get::<String>("payload_json").expect("exact payload"),)
            )
            .as_bytes(),
        )
    })
    .await;

    let (range_count, range_digest) =
        measure(backend, "bounded_row_range_query", 1, &storage, async {
            let result = lix
                .execute(
                    "SELECT id, kind, payload_json FROM markdown_node \
                 WHERE lixcol_file_id = $1 AND kind >= 'heading' AND kind <= 'paragraph' \
                 ORDER BY kind, id LIMIT 8",
                    &[Value::Text(file_id.clone())],
                )
                .await
                .expect("bounded row range query");
            let mut canonical = String::new();
            for row in result.rows() {
                canonical.push_str(&row.get::<String>("kind").expect("range kind"));
                canonical.push('\0');
                canonical.push_str(&canonical_json_text(
                    &row.get::<String>("payload_json").expect("range payload"),
                ));
                canonical.push('\n');
            }
            let count = result.rows().len();
            let mut rows = canonical.lines().collect::<Vec<_>>();
            rows.sort_unstable();
            (count, digest(rows.join("\n").as_bytes()))
        })
        .await;
    assert!(range_count > 0 && range_count <= 8);

    let (full_count, full_digest) = measure(backend, "full_typed_row_read", 1, &storage, async {
        let result = lix
            .execute(
                "SELECT id, kind, payload_json FROM markdown_node \
                 WHERE lixcol_file_id = $1 ORDER BY kind, id",
                &[Value::Text(file_id.clone())],
            )
            .await
            .expect("full typed row read");
        let mut canonical = String::new();
        for row in result.rows() {
            canonical.push_str(&row.get::<String>("kind").expect("full kind"));
            canonical.push('\0');
            canonical.push_str(&canonical_json_text(
                &row.get::<String>("payload_json").expect("full payload"),
            ));
            canonical.push('\n');
        }
        let count = result.rows().len();
        let mut rows = canonical.lines().collect::<Vec<_>>();
        rows.sort_unstable();
        (count, digest(rows.join("\n").as_bytes()))
    })
    .await;
    assert!(full_count >= range_count);

    let before_commit = lix
        .execute("SELECT lix_active_branch_commit_id() AS id", &[])
        .await
        .expect("before commit")
        .rows()[0]
        .get::<String>("id")
        .expect("before commit id");
    let edited_payload = json!({
        "inline": [{"type": "text", "value": "Peer 12 was edited after durable restore."}]
    })
    .to_string();
    measure(backend, "semantic_row_update", 1, &storage, async {
        lix.execute(
            "UPDATE markdown_node SET payload_json = $1 \
             WHERE id = $2 AND lixcol_file_id = $3",
            &[
                Value::Text(edited_payload),
                Value::Text(paragraph_id.clone()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .expect("semantic row update")
    })
    .await;
    let rendered = read_file(&lix, PATH).await;
    assert!(
        String::from_utf8(rendered.clone())
            .expect("rendered UTF-8")
            .contains("Peer 12 was edited after durable restore.")
    );

    let after_commit = lix
        .execute("SELECT lix_active_branch_commit_id() AS id", &[])
        .await
        .expect("after commit")
        .rows()[0]
        .get::<String>("id")
        .expect("after commit id");
    let (diff_count, diff_digest) = measure(backend, "historical_diff", 1, &storage, async {
        let result = lix
            .execute(
                "SELECT schema_key, row_pk, diff_type FROM lix_diff($1, $2) \
             WHERE schema_key = 'markdown_node' ORDER BY row_pk",
                &[Value::Text(before_commit), Value::Text(after_commit)],
            )
            .await
            .expect("historical diff");
        let expected_row_pk = json!([paragraph_id]);
        assert!(result.rows().iter().any(|row| {
            row.get::<JsonValue>("row_pk").expect("diff row PK") == expected_row_pk
        }));
        let mut rows = result
            .rows()
            .iter()
            .map(|row| {
                format!(
                    "{}\0{}\0{}",
                    row.get::<String>("schema_key").expect("diff schema"),
                    canonical_row_pk_text(&row.get::<JsonValue>("row_pk").expect("diff row PK")),
                    row.get::<String>("diff_type").expect("diff type")
                )
            })
            .collect::<Vec<_>>();
        rows.sort_unstable();
        (rows.len(), digest(rows.join("\n").as_bytes()))
    })
    .await;
    assert!(diff_count > 0);

    let (history_count, history_digest) =
        measure(backend, "history_depth_one", 1, &storage, async {
            let result = lix
                .execute(
                    "SELECT id, kind, payload_json FROM markdown_node_history() \
             WHERE lixcol_file_id = $1 AND lixcol_depth = 1 ORDER BY kind, id",
                    &[Value::Text(file_id.clone())],
                )
                .await
                .expect("history query");
            let mut rows = result
                .rows()
                .iter()
                .map(|row| {
                    format!(
                        "{}\0{}",
                        row.get::<String>("kind").expect("history kind"),
                        canonical_json_text(
                            &row.get::<String>("payload_json").expect("history payload")
                        )
                    )
                })
                .collect::<Vec<_>>();
            rows.sort_unstable();
            (rows.len(), digest(rows.join("\n").as_bytes()))
        })
        .await;
    assert!(history_count > 0);

    measure(backend, "transaction_insert_17_files", 17, &storage, async {
        let mut transaction = lix.begin_transaction().await.expect("begin transaction");
        for index in 0..17 {
            let path = format!("/batch/peer-{index}.md");
            let content = format!(
                "# Peer {index}\n\n*Counter:\n\n(~{} users)\n\nparagraph {index}\n- list item\n\n**wrapped strong {index}\nand Unicode λ 😀.**\n",
                index + 1
            );
            transaction
                .execute(
                    "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                    &[Value::Text(path), Value::Blob(content.into_bytes().into())],
                )
                .await
                .expect("stage batch file");
        }
        transaction.commit().await.expect("commit batch files");
    })
    .await;

    let batch_rows = lix
        .execute(
            "SELECT path, content FROM lix_file WHERE path LIKE '/batch/%' ORDER BY path",
            &[],
        )
        .await
        .expect("verify batch files");
    assert_eq!(batch_rows.rows().len(), 17);
    let mut batch_canonical = Vec::new();
    for row in batch_rows.rows() {
        batch_canonical
            .extend_from_slice(row.get::<String>("path").expect("batch path").as_bytes());
        batch_canonical.push(0);
        batch_canonical.extend_from_slice(&row.get::<Vec<u8>>("content").expect("batch content"));
        batch_canonical.push(b'\n');
    }
    let batch_digest = digest(&batch_canonical);

    let main_branch_id = lix.active_branch_id().await.expect("main branch");
    let branch_read = measure(
        backend,
        "branch_create_switch_shared_read",
        1,
        &storage,
        async {
            let branch = lix
                .create_branch(CreateBranchOptions {
                    id: Some(BRANCH_ID.to_owned()),
                    name: "Markdown qualification branch".to_owned(),
                    from_commit_id: None,
                })
                .await
                .expect("create branch");
            lix.switch_branch(SwitchBranchOptions {
                branch_id: branch.id,
            })
            .await
            .expect("switch branch");
            read_file(&lix, PATH).await
        },
    )
    .await;
    assert_eq!(digest(&branch_read), digest(&rendered));
    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id,
    })
    .await
    .expect("switch main");

    lix.close().await.expect("close repository");
    drop(lix);
    storage.inner.flush_for_reopen().await;
    let (cold, reopened_rows) =
        measure(backend, "cold_reopen_exact_file_read", 1, &storage, async {
            let reopened = setup_adapter::open(S::open(&root.join(".lix"))).await;
            let cold = read_file(&reopened, PATH).await;
            let reopened_rows = reopened
                .execute(
                    "SELECT COUNT(*) AS count FROM markdown_node WHERE lixcol_file_id = $1",
                    &[Value::Text(file_id.clone())],
                )
                .await
                .expect("reopened row count")
                .rows()[0]
                .get::<i64>("count")
                .expect("row count");
            reopened.close().await.expect("close reopened repository");
            (cold, reopened_rows)
        })
        .await;
    assert_eq!(digest(&cold), digest(&rendered));
    assert!(reopened_rows > 0);

    println!(
        "{}",
        json!({
            "event": "result",
            "backend": backend,
            "adapter": setup_adapter::NAME,
            "fixture_bytes": expected.len(),
            "fixture_digest": expected_digest,
            "rendered_digest": digest(&rendered),
            "exact_row_digest": exact_digest,
            "range_row_digest": range_digest,
            "full_row_digest": full_digest,
            "diff_row_count": diff_count,
            "diff_digest": diff_digest,
            "history_row_count": history_count,
            "history_digest": history_digest,
            "batch_17_digest": batch_digest,
            "settled_disk_bytes": directory_bytes(&root),
            "row_count_after_reopen": reopened_rows,
            "verified": true,
        })
    );
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let backend = std::env::args().nth(1).expect("backend");
    let root = PathBuf::from(std::env::args().nth(2).expect("database path"));
    std::fs::create_dir_all(&root).expect("create database root");
    match backend.as_str() {
        "rocksdb" => run::<RocksDB>("rocksdb", root).await,
        "slatedb" => run::<SlateDB>("slatedb", root).await,
        other => panic!("backend must be rocksdb or slatedb, got {other}"),
    }
}
