//! Benchmark-only E1/E2/E3 file-layout oracle over the shipping storage APIs.
//!
//! This deliberately does not use or alter a production encoding. Candidate
//! metadata is canonical binary; the current baseline retains its JSON rows.
//! Content-addressed objects are BLAKE3-256 keyed and verified on every read.
//!
//! ```text
//! cargo run -p lix_e2e --release --features rocksdb,slatedb \
//!   --example file_layout_experiment_e -- \
//!   --backends=rocksdb,slatedb --sizes=4096,1048576 \
//!   --metadata-widths=32,256 --thresholds=0,1024,32768,65536,131072,262144 \
//!   --chunk-policies=current,prototype --fanouts=64 --carrier-rows=64
//! ```
//!
//! Every flag also has a `LIX_FILE_LAYOUT_E_*` environment equivalent. Lists
//! form a bounded Cartesian sweep (at most 128 cases).

use std::alloc::{GlobalAlloc, Layout as AllocLayout};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, GetOptions, Key, KeyRange,
    ProjectedValue, PutBatch, PutEntry, ReadOptions, ScanChunk, ScanCursor, SpaceId, Storage,
    StorageError, StorageRead, StorageScanSource, StorageSpace, StorageWrite, StoredValue,
    WriteOptions,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

const ROW_SPACE: StorageSpace =
    StorageSpace::mutable(SpaceId(0x00fe_1001), "experiment.file_layout.row");
const DESCRIPTOR_SPACE: StorageSpace =
    StorageSpace::immutable(SpaceId(0x00fe_1002), "experiment.file_layout.descriptor");
const CHUNK_SPACE: StorageSpace =
    StorageSpace::immutable(SpaceId(0x00fe_1003), "experiment.file_layout.chunk");
const BLOB_REF_SPACE: StorageSpace =
    StorageSpace::mutable(SpaceId(0x00fe_1004), "experiment.file_layout.blob_ref");
const MANIFEST_SPACE: StorageSpace =
    StorageSpace::immutable(SpaceId(0x00fe_1005), "experiment.file_layout.manifest");
const MANIFEST_CHUNK_SPACE: StorageSpace = StorageSpace::immutable(
    SpaceId(0x00fe_1006),
    "experiment.file_layout.manifest_chunk",
);
const MAX_CASES: usize = 128;
const MAX_PAYLOAD_BYTES: usize = 256 * 1024 * 1024;

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;
static PROFILE_ENABLED: AtomicBool = AtomicBool::new(false);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: AllocLayout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && PROFILE_ENABLED.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: AllocLayout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: AllocLayout, new_size: usize) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, new_size) };
        if !replacement.is_null() && PROFILE_ENABLED.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        replacement
    }
}

fn begin_allocations() {
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    ALLOC_CALLS.store(0, Ordering::Relaxed);
    PROFILE_ENABLED.store(true, Ordering::Relaxed);
}

fn end_allocations() -> (u64, u64) {
    PROFILE_ENABLED.store(false, Ordering::Relaxed);
    (
        ALLOC_BYTES.load(Ordering::Relaxed),
        ALLOC_CALLS.load(Ordering::Relaxed),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Layout {
    Current,
    E1,
    E2,
    E3,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Self::Current => "current_production_shape",
            Self::E1 => "e1_inline_descriptor",
            Self::E2 => "e2_shared_descriptor",
            Self::E3 => "e3_inline_or_chunked",
        }
    }

    fn tag(self) -> u8 {
        match self {
            Self::Current => 0,
            Self::E1 => 1,
            Self::E2 => 2,
            Self::E3 => 3,
        }
    }
}

#[derive(Clone, Debug)]
struct Config {
    backends: Vec<String>,
    sizes: Vec<usize>,
    metadata_widths: Vec<usize>,
    thresholds: Vec<usize>,
    chunk_policies: Vec<ChunkPolicy>,
    fanouts: Vec<usize>,
    carrier_rows: Vec<usize>,
    shared_copies: Vec<usize>,
    root: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug)]
struct Case {
    size: usize,
    metadata_width: usize,
    threshold: usize,
    chunk_policy: ChunkPolicy,
    fanout: usize,
    carrier_rows: usize,
    shared_copies: usize,
}

#[derive(Clone, Copy, Debug)]
struct ChunkPolicy {
    name: &'static str,
    min: usize,
    avg: usize,
    max: usize,
}

const CURRENT_POLICY: ChunkPolicy = ChunkPolicy {
    name: "current_256k_1m_4m",
    min: 256 * 1024,
    avg: 1024 * 1024,
    max: 4 * 1024 * 1024,
};
const PROTOTYPE_POLICY: ChunkPolicy = ChunkPolicy {
    name: "prototype_512k_512k_2m",
    min: 512 * 1024,
    avg: 512 * 1024,
    max: 2 * 1024 * 1024,
};

#[derive(Clone, Debug, Default)]
struct IoStats {
    gets: u64,
    get_keys: u64,
    get_values: u64,
    get_bytes: u64,
    scans: u64,
    scan_rows: u64,
    scan_bytes: u64,
    write_batches: u64,
    puts: u64,
    deletes: u64,
    put_bytes: u64,
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
    fn new(inner: S) -> (Self, Arc<Mutex<IoStats>>) {
        let stats = Arc::new(Mutex::new(IoStats::default()));
        (
            Self {
                inner,
                stats: Arc::clone(&stats),
            },
            stats,
        )
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
        Ok(CountingRead {
            inner: self.inner.begin_read(options).await?,
            stats: Arc::clone(&self.stats),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
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
            let mut stats = self.stats.lock().expect("I/O counter mutex");
            stats.gets += 1;
            stats.get_keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
        }
        let result = self.inner.get_many(requests).await?;
        let mut stats = self.stats.lock().expect("I/O counter mutex");
        for value in result.values.iter().flatten() {
            stats.get_values += 1;
            stats.get_bytes += projected_len(value) as u64;
        }
        drop(stats);
        Ok(result)
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        options: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        self.stats.lock().expect("I/O counter mutex").scans += 1;
        let order = options.order;
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
            let (entries, more) = self.inner.next_page(limit_rows).await?.into_parts();
            let mut stats = self.stats.lock().expect("I/O counter mutex");
            stats.scan_rows += entries.len() as u64;
            stats.scan_bytes += entries
                .iter()
                .map(|entry| projected_len(&entry.value) as u64)
                .sum::<u64>();
            drop(stats);
            Ok(ScanChunk::new(entries, more))
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
            let mut stats = self.stats.lock().expect("I/O counter mutex");
            stats.write_batches += 1;
            stats.puts += entries.entries.len() as u64;
            stats.put_bytes += entries
                .entries
                .iter()
                .map(|entry| (entry.key.0.len() + entry.value.bytes.len()) as u64)
                .sum::<u64>();
        }
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O counter mutex");
            stats.write_batches += 1;
            stats.deletes += keys.len() as u64;
        }
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        self.stats.lock().expect("I/O counter mutex").write_batches += 1;
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

trait BenchStorage: Storage + Clone + Send + Sync + 'static {
    async fn settle(&self) -> Result<(), StorageError>;
}

impl BenchStorage for RocksDB {
    async fn settle(&self) -> Result<(), StorageError> {
        self.flush()
    }
}

impl BenchStorage for SlateDB {
    async fn settle(&self) -> Result<(), StorageError> {
        self.flush_memtable_for_diagnostics().await?;
        self.flush().await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Digest([u8; 32]);

impl Digest {
    fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    fn key(self) -> Key {
        Key(Bytes::copy_from_slice(&self.0))
    }
}

#[derive(Clone, Debug)]
struct ChunkRef {
    len: u32,
    hash: Digest,
}

#[derive(Clone, Debug)]
enum Representation {
    Current(Digest),
    Inline(Vec<u8>),
    InlineDescriptor(Vec<ChunkRef>),
    SharedDescriptor(Digest),
}

#[derive(Clone, Debug)]
struct Row {
    layout: Layout,
    payload_len: u64,
    whole_hash: Digest,
    metadata: Vec<u8>,
    representation: Representation,
}

#[derive(Clone, Debug)]
struct BuiltRow {
    encoded: Vec<u8>,
    chunks: BTreeMap<Digest, Vec<u8>>,
    descriptors: BTreeMap<Digest, Vec<u8>>,
    blob_refs: BTreeMap<Digest, Vec<u8>>,
    manifests: BTreeMap<Digest, Vec<u8>>,
    manifest_chunks: BTreeMap<Digest, Vec<u8>>,
    identity: Digest,
}

#[derive(Clone, Debug)]
struct ReopenState {
    row_key: Key,
    expected: Vec<u8>,
}

struct MeasureContext<'a> {
    backend: &'a str,
    layout: Layout,
    case: Case,
    path: &'a Path,
    stats: &'a Arc<Mutex<IoStats>>,
    slate: Option<&'a SlateDBIoCounters>,
}

#[tokio::main]
async fn main() {
    let config = parse_config();
    let cases = expand_cases(&config);
    for backend in &config.backends {
        for (case_index, case) in cases.iter().copied().enumerate() {
            for layout in [Layout::Current, Layout::E1, Layout::E2, Layout::E3] {
                match backend.as_str() {
                    "rocksdb" => run_rocksdb(&config, case_index, case, layout).await,
                    "slatedb" => run_slatedb(&config, case_index, case, layout).await,
                    other => panic!("backend must be rocksdb or slatedb, got {other}"),
                }
            }
        }
    }
}

async fn run_rocksdb(config: &Config, case_index: usize, case: Case, layout: Layout) {
    let owned;
    let path = if let Some(root) = &config.root {
        let path = root.join(format!("case-{case_index}-rocksdb-{}", layout.tag()));
        std::fs::create_dir_all(&path).expect("create RocksDB experiment path");
        path
    } else {
        owned = tempfile::tempdir().expect("create RocksDB experiment directory");
        owned.path().to_owned()
    };
    let database = RocksDB::open(&path).expect("open RocksDB experiment");
    let state = run_initial("rocksdb", layout, case, &path, database.clone(), None).await;
    database
        .settle()
        .await
        .expect("settle RocksDB before reopen");
    drop(database);
    let reopened = RocksDB::open(&path).expect("cold reopen RocksDB experiment");
    run_reopened(
        "rocksdb",
        layout,
        case,
        &path,
        reopened.clone(),
        None,
        state,
    )
    .await;
    reopened.settle().await.expect("settle reopened RocksDB");
}

async fn run_slatedb(config: &Config, case_index: usize, case: Case, layout: Layout) {
    let owned;
    let path = if let Some(root) = &config.root {
        let path = root.join(format!("case-{case_index}-slatedb-{}", layout.tag()));
        std::fs::create_dir_all(&path).expect("create SlateDB experiment path");
        path
    } else {
        owned = tempfile::tempdir().expect("create SlateDB experiment directory");
        owned.path().to_owned()
    };
    let counters = SlateDBIoCounters::default();
    let database =
        SlateDB::open_with_io_counters(&path, counters.clone()).expect("open SlateDB experiment");
    let state = run_initial(
        "slatedb",
        layout,
        case,
        &path,
        database.clone(),
        Some(&counters),
    )
    .await;
    database
        .settle()
        .await
        .expect("settle SlateDB before reopen");
    drop(database);
    let reopened = SlateDB::open_with_io_counters(&path, counters.clone())
        .expect("cold reopen SlateDB experiment");
    run_reopened(
        "slatedb",
        layout,
        case,
        &path,
        reopened.clone(),
        Some(&counters),
        state,
    )
    .await;
    reopened.settle().await.expect("settle reopened SlateDB");
}

async fn run_initial<S: BenchStorage>(
    backend: &str,
    layout: Layout,
    case: Case,
    path: &Path,
    database: S,
    slate: Option<&SlateDBIoCounters>,
) -> ReopenState {
    let (storage, stats) = CountingStorage::new(database.clone());
    let context = MeasureContext {
        backend,
        layout,
        case,
        path,
        stats: &stats,
        slate,
    };
    print_case(&context);

    let base = deterministic_payload(case.size, 0);
    let base_built = measured(&context, "base_write", async {
        let built = build_row(layout, case, &base);
        publish_row(&storage, row_key(0, 0), &built).await;
        database.settle().await.expect("settle base write");
        built
    })
    .await;

    let mut appended = base.clone();
    appended.extend(deterministic_payload((case.size / 8).max(1), 1));
    let appended_built = measured(&context, "append", async {
        let built = build_row(layout, case, &appended);
        publish_row(&storage, row_key(0, 1), &built).await;
        database.settle().await.expect("settle append");
        built
    })
    .await;

    let mut overwritten = appended.clone();
    let overwrite_at = (case.chunk_policy.min / 2).min(overwritten.len() - 1);
    overwritten[overwrite_at] ^= 0x5a;
    let overwritten_built = measured(&context, "one_chunk_overwrite", async {
        let built = build_row(layout, case, &overwritten);
        publish_row(&storage, row_key(0, 2), &built).await;
        database.settle().await.expect("settle overwrite");
        built
    })
    .await;

    measured(&context, "full_read", async {
        let actual = read_payload(&storage, &row_key(0, 2), None)
            .await
            .expect("authenticated full read");
        assert_eq!(actual, overwritten);
    })
    .await;

    measured(&context, "range_read", async {
        let start = overwritten.len() / 3;
        let end = (start + case.chunk_policy.min / 2 + 1).min(overwritten.len());
        let actual = read_payload(&storage, &row_key(0, 2), Some(start..end))
            .await
            .expect("authenticated range read");
        assert_eq!(actual, overwritten[start..end]);
    })
    .await;

    measured(&context, "branch_share", async {
        publish_row(&storage, row_key(1, 0), &overwritten_built).await;
        for copy in 0..case.shared_copies {
            publish_row(
                &storage,
                row_key(10_u32.saturating_add(copy as u32), 0),
                &overwritten_built,
            )
            .await;
        }
        database.settle().await.expect("settle branch share");
    })
    .await;

    measured(&context, "diff_merge_identity", async {
        let left = read_row(&storage, &row_key(0, 2)).await.expect("left row");
        let right = read_row(&storage, &row_key(1, 0)).await.expect("right row");
        assert_eq!(left.whole_hash, right.whole_hash, "identity-only diff");
        let encoded = encode_row(&right);
        put_rows(&storage, ROW_SPACE, vec![(row_key(2, 0), encoded)]).await;
        database.settle().await.expect("settle identity merge");
    })
    .await;

    print_inventory(&context, &storage, "shared").await;
    assert_ne!(base_built.identity, overwritten_built.identity);
    assert_ne!(appended_built.identity, overwritten_built.identity);
    ReopenState {
        row_key: row_key(0, 2),
        expected: overwritten,
    }
}

async fn run_reopened<S: BenchStorage>(
    backend: &str,
    layout: Layout,
    case: Case,
    path: &Path,
    database: S,
    slate: Option<&SlateDBIoCounters>,
    state: ReopenState,
) {
    let (storage, stats) = CountingStorage::new(database.clone());
    let context = MeasureContext {
        backend,
        layout,
        case,
        path,
        stats: &stats,
        slate,
    };
    measured(&context, "cold_reopen", async {
        let actual = read_payload(&storage, &state.row_key, None)
            .await
            .expect("authenticated cold read");
        assert_eq!(actual, state.expected);
    })
    .await;

    measured(&context, "corruption", async {
        exercise_corruption(&storage, &state.row_key).await;
        database
            .settle()
            .await
            .expect("settle corruption restoration");
    })
    .await;

    measured(&context, "delete_gc", async {
        delete_keys(&storage, ROW_SPACE, &[row_key(0, 0), row_key(0, 1)]).await;
        collect_garbage(&storage).await;
        database.settle().await.expect("settle delete/GC");
    })
    .await;
    print_inventory(&context, &storage, "after_gc").await;
}

async fn measured<T>(
    context: &MeasureContext<'_>,
    operation: &str,
    future: impl Future<Output = T>,
) -> T {
    let _ = take_stats(context.stats);
    let physical_before = context.slate.map(SlateDBIoCounters::snapshot);
    let rss_before = resident_bytes();
    let cpu_before = cpu_micros();
    begin_allocations();
    let started = Instant::now();
    let result = future.await;
    let wall_us = started.elapsed().as_micros();
    let (alloc_bytes, alloc_calls) = end_allocations();
    let cpu_us = cpu_micros().saturating_sub(cpu_before);
    let rss_after = resident_bytes();
    let io = take_stats(context.stats);
    let physical = physical_before.map_or_else(SlateDBIoSnapshot::default, |before| {
        context
            .slate
            .expect("SlateDB counters")
            .snapshot()
            .saturating_sub(before)
    });
    println!(
        "file_layout,kind=operation,backend={},layout={},operation={operation},size={},metadata_width={},threshold={},chunk_policy={},chunk_min={},chunk_avg={},chunk_max={},fanout={},carrier_rows={},shared_copies={},wall_us={wall_us},cpu_us={cpu_us},rss_before_bytes={rss_before},rss_after_bytes={rss_after},alloc_bytes={alloc_bytes},alloc_calls={alloc_calls},gets={},get_keys={},get_values={},get_bytes={},scans={},scan_rows={},scan_bytes={},write_batches={},puts={},deletes={},put_bytes={},settled_path_bytes={},physical_read_objects={},physical_read_bytes={},physical_write_objects={},physical_write_bytes={},physical_deleted_objects={}",
        context.backend,
        context.layout.name(),
        context.case.size,
        context.case.metadata_width,
        context.case.threshold,
        context.case.chunk_policy.name,
        context.case.chunk_policy.min,
        context.case.chunk_policy.avg,
        context.case.chunk_policy.max,
        context.case.fanout,
        context.case.carrier_rows,
        context.case.shared_copies,
        io.gets,
        io.get_keys,
        io.get_values,
        io.get_bytes,
        io.scans,
        io.scan_rows,
        io.scan_bytes,
        io.write_batches,
        io.puts,
        io.deletes,
        io.put_bytes,
        directory_bytes(context.path),
        physical.read_objects,
        physical.read_bytes,
        physical.write_objects,
        physical.write_bytes,
        physical.deleted_objects,
    );
    result
}

fn build_row(layout: Layout, case: Case, payload: &[u8]) -> BuiltRow {
    let whole_hash = Digest::of(payload);
    let metadata = deterministic_payload(case.metadata_width, 0x4d);
    if layout == Layout::E3 && payload.len() <= case.threshold {
        let row = Row {
            layout,
            payload_len: payload.len() as u64,
            whole_hash,
            metadata,
            representation: Representation::Inline(payload.to_vec()),
        };
        return BuiltRow {
            encoded: encode_row(&row),
            chunks: BTreeMap::new(),
            descriptors: BTreeMap::new(),
            blob_refs: BTreeMap::new(),
            manifests: BTreeMap::new(),
            manifest_chunks: BTreeMap::new(),
            identity: whole_hash,
        };
    }

    let mut chunks = BTreeMap::new();
    let refs = chunk_ranges(payload, case.chunk_policy)
        .into_iter()
        .map(|range| {
            let chunk = &payload[range];
            let hash = Digest::of(chunk);
            chunks.entry(hash).or_insert_with(|| chunk.to_vec());
            ChunkRef {
                len: chunk.len() as u32,
                hash,
            }
        })
        .collect::<Vec<_>>();
    let mut descriptors = BTreeMap::new();
    let mut blob_refs = BTreeMap::new();
    let mut manifests = BTreeMap::new();
    let mut manifest_chunks = BTreeMap::new();
    let representation = if layout == Layout::Current {
        let mut leaves = Vec::new();
        for group in refs.chunks(case.fanout) {
            let encoded = encode_leaf(group);
            let hash = Digest::of(&encoded);
            manifest_chunks.insert(hash, encoded);
            leaves.push(hash);
        }
        let manifest = encode_root(payload.len() as u64, whole_hash, &leaves);
        let blob_id = Digest::of(&manifest);
        manifests.insert(blob_id, manifest);
        blob_refs.insert(blob_id, encode_blob_ref(blob_id, whole_hash, payload.len()));
        Representation::Current(blob_id)
    } else if layout == Layout::E1 {
        Representation::InlineDescriptor(refs)
    } else {
        let mut leaves = Vec::new();
        for group in refs.chunks(case.fanout) {
            let encoded = encode_leaf(group);
            let hash = Digest::of(&encoded);
            descriptors.insert(hash, encoded);
            leaves.push(hash);
        }
        let root = encode_root(payload.len() as u64, whole_hash, &leaves);
        let root_hash = Digest::of(&root);
        descriptors.insert(root_hash, root);
        Representation::SharedDescriptor(root_hash)
    };
    let row = Row {
        layout,
        payload_len: payload.len() as u64,
        whole_hash,
        metadata,
        representation,
    };
    BuiltRow {
        encoded: encode_row(&row),
        chunks,
        descriptors,
        blob_refs,
        manifests,
        manifest_chunks,
        identity: whole_hash,
    }
}

async fn publish_row<S: Storage>(storage: &S, key: Key, built: &BuiltRow) {
    put_missing_objects(storage, DESCRIPTOR_SPACE, &built.descriptors, true).await;
    put_missing_objects(storage, MANIFEST_SPACE, &built.manifests, true).await;
    put_missing_objects(storage, MANIFEST_CHUNK_SPACE, &built.manifest_chunks, true).await;
    put_missing_objects(storage, BLOB_REF_SPACE, &built.blob_refs, false).await;
    put_missing_objects(storage, CHUNK_SPACE, &built.chunks, true).await;
    put_rows(storage, ROW_SPACE, vec![(key, built.encoded.clone())]).await;
}

async fn put_missing_objects<S: Storage>(
    storage: &S,
    space: StorageSpace,
    objects: &BTreeMap<Digest, Vec<u8>>,
    authenticate: bool,
) {
    if objects.is_empty() {
        return;
    }
    let keys = objects.keys().copied().map(Digest::key).collect::<Vec<_>>();
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin object existence read");
    let values = read
        .get_many(&[GetManyRequest {
            space,
            keys: &keys,
            opts: GetOptions::default(),
        }])
        .await
        .expect("read object existence");
    let missing = objects
        .iter()
        .zip(values.values)
        .filter_map(|((hash, bytes), existing)| match existing {
            Some(ProjectedValue::FullValue(existing)) => {
                if authenticate {
                    verify_object(*hash, &existing).expect("existing object hash");
                }
                assert_eq!(existing.as_ref(), bytes, "digest collision");
                None
            }
            Some(ProjectedValue::KeyOnly) => unreachable!("full projection"),
            None => Some((hash.key(), bytes.clone())),
        })
        .collect::<Vec<_>>();
    drop(read);
    if !missing.is_empty() {
        put_rows(storage, space, missing).await;
    }
}

async fn put_rows<S: Storage>(storage: &S, space: StorageSpace, entries: Vec<(Key, Vec<u8>)>) {
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin experiment write");
    write
        .put_many(
            space,
            PutBatch {
                entries: entries
                    .into_iter()
                    .map(|(key, bytes)| PutEntry {
                        key,
                        value: StoredValue {
                            bytes: Bytes::from(bytes),
                        },
                    })
                    .collect(),
            },
        )
        .await
        .expect("put experiment rows");
    write.commit().await.expect("commit experiment rows");
}

async fn delete_keys<S: Storage>(storage: &S, space: StorageSpace, keys: &[Key]) {
    if keys.is_empty() {
        return;
    }
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin experiment delete");
    write
        .delete_many(space, keys)
        .await
        .expect("delete experiment rows");
    write.commit().await.expect("commit experiment delete");
}

async fn read_row<S: Storage>(storage: &S, key: &Key) -> Result<Row, String> {
    let bytes = get_one(storage, ROW_SPACE, key).await?;
    decode_row(&bytes)
}

async fn read_payload<S: Storage>(
    storage: &S,
    key: &Key,
    range: Option<std::ops::Range<usize>>,
) -> Result<Vec<u8>, String> {
    let row = read_row(storage, key).await?;
    match row.representation {
        Representation::Current(blob_id) => {
            let blob_ref = get_one(storage, BLOB_REF_SPACE, &blob_id.key()).await?;
            let (blob_hash, size) = decode_blob_ref(&blob_ref, blob_id)?;
            let manifest = get_verified(storage, MANIFEST_SPACE, blob_id).await?;
            let root = decode_root(&manifest)?;
            if root.payload_len != size as u64 || root.whole_hash != blob_hash {
                return Err("BlobRef/manifest identity mismatch".to_owned());
            }
            let mut refs = Vec::new();
            for leaf_hash in root.leaves {
                let leaf = get_verified(storage, MANIFEST_CHUNK_SPACE, leaf_hash).await?;
                refs.extend(decode_leaf(&leaf)?);
            }
            let requested = range.unwrap_or(0..size);
            validate_range(&requested, size)?;
            read_chunks(storage, &refs, size as u64, blob_hash, requested).await
        }
        Representation::Inline(bytes) => {
            let requested = range.unwrap_or(0..row.payload_len as usize);
            validate_range(&requested, row.payload_len as usize)?;
            if Digest::of(&bytes) != row.whole_hash || bytes.len() as u64 != row.payload_len {
                return Err("inline payload authentication failed".to_owned());
            }
            Ok(bytes[requested].to_vec())
        }
        Representation::InlineDescriptor(refs) => {
            let requested = range.unwrap_or(0..row.payload_len as usize);
            validate_range(&requested, row.payload_len as usize)?;
            read_chunks(storage, &refs, row.payload_len, row.whole_hash, requested).await
        }
        Representation::SharedDescriptor(root_hash) => {
            let requested = range.unwrap_or(0..row.payload_len as usize);
            validate_range(&requested, row.payload_len as usize)?;
            let root_bytes = get_verified(storage, DESCRIPTOR_SPACE, root_hash).await?;
            let root = decode_root(&root_bytes)?;
            if root.payload_len != row.payload_len || root.whole_hash != row.whole_hash {
                return Err("row/root identity mismatch".to_owned());
            }
            let mut refs = Vec::new();
            for leaf_hash in root.leaves {
                let leaf = get_verified(storage, DESCRIPTOR_SPACE, leaf_hash).await?;
                refs.extend(decode_leaf(&leaf)?);
            }
            read_chunks(storage, &refs, row.payload_len, row.whole_hash, requested).await
        }
    }
}

fn validate_range(range: &std::ops::Range<usize>, payload_len: usize) -> Result<(), String> {
    if range.start > range.end || range.end > payload_len {
        Err("range outside payload".to_owned())
    } else {
        Ok(())
    }
}

async fn read_chunks<S: Storage>(
    storage: &S,
    refs: &[ChunkRef],
    payload_len: u64,
    whole_hash: Digest,
    requested: std::ops::Range<usize>,
) -> Result<Vec<u8>, String> {
    if refs.iter().map(|item| item.len as u64).sum::<u64>() != payload_len {
        return Err("descriptor length mismatch".to_owned());
    }
    let full = requested.start == 0 && requested.end == payload_len as usize;
    let mut output = Vec::with_capacity(requested.len());
    let mut full_hasher = blake3::Hasher::new();
    let mut offset = 0_usize;
    for item in refs {
        let end = offset + item.len as usize;
        if full || (offset < requested.end && end > requested.start) {
            let bytes = get_verified(storage, CHUNK_SPACE, item.hash).await?;
            if bytes.len() != item.len as usize {
                return Err("chunk length mismatch".to_owned());
            }
            if full {
                full_hasher.update(&bytes);
            }
            let copy_start = requested.start.saturating_sub(offset).min(bytes.len());
            let copy_end = requested.end.saturating_sub(offset).min(bytes.len());
            if copy_start < copy_end {
                output.extend_from_slice(&bytes[copy_start..copy_end]);
            }
        }
        offset = end;
    }
    if full && Digest(*full_hasher.finalize().as_bytes()) != whole_hash {
        return Err("whole payload authentication failed".to_owned());
    }
    Ok(output)
}

async fn get_one<S: Storage>(
    storage: &S,
    space: StorageSpace,
    key: &Key,
) -> Result<Vec<u8>, String> {
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| error.to_string())?;
    let keys = [key.clone()];
    let result = read
        .get_many(&[GetManyRequest {
            space,
            keys: &keys,
            opts: GetOptions::default(),
        }])
        .await
        .map_err(|error| error.to_string())?;
    match result.values.into_iter().next().flatten() {
        Some(ProjectedValue::FullValue(bytes)) => Ok(bytes.to_vec()),
        Some(ProjectedValue::KeyOnly) => Err("unexpected key-only value".to_owned()),
        None => Err(format!("missing key in {}", space.name)),
    }
}

async fn get_verified<S: Storage>(
    storage: &S,
    space: StorageSpace,
    hash: Digest,
) -> Result<Vec<u8>, String> {
    let bytes = get_one(storage, space, &hash.key()).await?;
    verify_object(hash, &bytes)?;
    Ok(bytes)
}

fn verify_object(hash: Digest, bytes: &[u8]) -> Result<(), String> {
    if Digest::of(bytes) == hash {
        Ok(())
    } else {
        Err("content-addressed object hash mismatch".to_owned())
    }
}

async fn exercise_corruption<S: Storage>(storage: &S, row_key: &Key) {
    let original_row = get_one(storage, ROW_SPACE, row_key)
        .await
        .expect("read row for corruption");
    let row = decode_row(&original_row).expect("decode corruption row");
    if let Representation::Inline(mut payload) = row.representation.clone() {
        if payload.is_empty() {
            let mut corrupt = original_row.clone();
            corrupt[0] ^= 1;
            put_rows(storage, ROW_SPACE, vec![(row_key.clone(), corrupt)]).await;
            assert!(read_payload(storage, row_key, None).await.is_err());
            put_rows(storage, ROW_SPACE, vec![(row_key.clone(), original_row)]).await;
            return;
        }
        payload[0] ^= 1;
        let mut corrupt = row;
        corrupt.representation = Representation::Inline(payload);
        put_rows(
            storage,
            ROW_SPACE,
            vec![(row_key.clone(), encode_row(&corrupt))],
        )
        .await;
        assert!(read_payload(storage, row_key, None).await.is_err());
        put_rows(storage, ROW_SPACE, vec![(row_key.clone(), original_row)]).await;
        return;
    }

    let refs = descriptor_refs(storage, &row)
        .await
        .expect("resolve corruption refs");
    let Some(target) = refs.first().map(|chunk| chunk.hash) else {
        let mut corrupt = original_row.clone();
        corrupt[0] ^= 1;
        put_rows(storage, ROW_SPACE, vec![(row_key.clone(), corrupt)]).await;
        assert!(read_payload(storage, row_key, None).await.is_err());
        put_rows(storage, ROW_SPACE, vec![(row_key.clone(), original_row)]).await;
        return;
    };
    let original = get_verified(storage, CHUNK_SPACE, target)
        .await
        .expect("read corruption target");
    delete_keys(storage, CHUNK_SPACE, &[target.key()]).await;
    // Immutable content-addressed spaces correctly reject replacing an object
    // under an existing digest.  Exercise the serving contract at that owner
    // boundary instead: a referenced object that is absent must fail closed.
    assert!(read_payload(storage, row_key, None).await.is_err());
    put_rows(storage, CHUNK_SPACE, vec![(target.key(), original)]).await;
}

async fn descriptor_refs<S: Storage>(storage: &S, row: &Row) -> Result<Vec<ChunkRef>, String> {
    match &row.representation {
        Representation::Current(blob_id) => {
            let root = decode_root(&get_verified(storage, MANIFEST_SPACE, *blob_id).await?)?;
            let mut refs = Vec::new();
            for leaf in root.leaves {
                refs.extend(decode_leaf(
                    &get_verified(storage, MANIFEST_CHUNK_SPACE, leaf).await?,
                )?);
            }
            Ok(refs)
        }
        Representation::InlineDescriptor(refs) => Ok(refs.clone()),
        Representation::SharedDescriptor(root_hash) => {
            let root = decode_root(&get_verified(storage, DESCRIPTOR_SPACE, *root_hash).await?)?;
            let mut refs = Vec::new();
            for leaf in root.leaves {
                refs.extend(decode_leaf(
                    &get_verified(storage, DESCRIPTOR_SPACE, leaf).await?,
                )?);
            }
            Ok(refs)
        }
        Representation::Inline(_) => Ok(Vec::new()),
    }
}

async fn collect_garbage<S: Storage>(storage: &S) {
    let rows = scan_space(storage, ROW_SPACE).await;
    let mut live_descriptors = BTreeSet::new();
    let mut live_chunks = BTreeSet::new();
    let mut live_manifests = BTreeSet::new();
    let mut live_manifest_chunks = BTreeSet::new();
    for (_, value) in rows {
        let row = decode_row(&value).expect("decode live row during GC");
        match &row.representation {
            Representation::Current(blob_id) => {
                live_manifests.insert(*blob_id);
                let root = decode_root(
                    &get_verified(storage, MANIFEST_SPACE, *blob_id)
                        .await
                        .expect("verify live manifest"),
                )
                .expect("decode live manifest");
                for leaf_hash in root.leaves {
                    live_manifest_chunks.insert(leaf_hash);
                    let leaf = get_verified(storage, MANIFEST_CHUNK_SPACE, leaf_hash)
                        .await
                        .expect("verify live manifest chunk");
                    live_chunks.extend(
                        decode_leaf(&leaf)
                            .expect("decode live manifest chunk")
                            .into_iter()
                            .map(|item| item.hash),
                    );
                }
            }
            Representation::Inline(_) => {}
            Representation::InlineDescriptor(refs) => {
                live_chunks.extend(refs.iter().map(|item| item.hash));
            }
            Representation::SharedDescriptor(root_hash) => {
                live_descriptors.insert(*root_hash);
                let root = decode_root(
                    &get_verified(storage, DESCRIPTOR_SPACE, *root_hash)
                        .await
                        .expect("verify live root"),
                )
                .expect("decode live root");
                for leaf_hash in root.leaves {
                    live_descriptors.insert(leaf_hash);
                    let leaf = get_verified(storage, DESCRIPTOR_SPACE, leaf_hash)
                        .await
                        .expect("verify live leaf");
                    live_chunks.extend(
                        decode_leaf(&leaf)
                            .expect("decode live leaf")
                            .into_iter()
                            .map(|item| item.hash),
                    );
                }
            }
        }
    }
    let descriptor_deletes = scan_space(storage, DESCRIPTOR_SPACE)
        .await
        .into_iter()
        .filter_map(|(key, _)| {
            let hash = digest_key(&key).expect("descriptor digest key");
            (!live_descriptors.contains(&hash)).then_some(key)
        })
        .collect::<Vec<_>>();
    let chunk_deletes = scan_space(storage, CHUNK_SPACE)
        .await
        .into_iter()
        .filter_map(|(key, _)| {
            let hash = digest_key(&key).expect("chunk digest key");
            (!live_chunks.contains(&hash)).then_some(key)
        })
        .collect::<Vec<_>>();
    delete_keys(storage, DESCRIPTOR_SPACE, &descriptor_deletes).await;
    delete_unmarked(storage, BLOB_REF_SPACE, &live_manifests).await;
    delete_unmarked(storage, MANIFEST_SPACE, &live_manifests).await;
    delete_unmarked(storage, MANIFEST_CHUNK_SPACE, &live_manifest_chunks).await;
    delete_keys(storage, CHUNK_SPACE, &chunk_deletes).await;
}

async fn delete_unmarked<S: Storage>(storage: &S, space: StorageSpace, live: &BTreeSet<Digest>) {
    let deletes = scan_space(storage, space)
        .await
        .into_iter()
        .filter_map(|(key, _)| {
            let hash = digest_key(&key).expect("digest object key");
            (!live.contains(&hash)).then_some(key)
        })
        .collect::<Vec<_>>();
    delete_keys(storage, space, &deletes).await;
}

async fn print_inventory<S: Storage>(context: &MeasureContext<'_>, storage: &S, stage: &str) {
    let rows = scan_space(storage, ROW_SPACE).await;
    let descriptors = scan_space(storage, DESCRIPTOR_SPACE).await;
    let blob_refs = scan_space(storage, BLOB_REF_SPACE).await;
    let manifests = scan_space(storage, MANIFEST_SPACE).await;
    let manifest_chunks = scan_space(storage, MANIFEST_CHUNK_SPACE).await;
    let chunks = scan_space(storage, CHUNK_SPACE).await;
    let blob_sizes = blob_refs
        .iter()
        .map(|(key, value)| {
            let id = digest_key(key).expect("BlobRef key");
            (
                id,
                decode_blob_ref(value, id).expect("inventory BlobRef").1 as u64,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut logical_bytes = 0_u64;
    let mut native_metadata_bytes = 0_u64;
    let mut json_file_descriptor_bytes = 0_u64;
    let mut inline_descriptor_bytes = 0_u64;
    let mut pointer_descriptor_bytes = 0_u64;
    let mut inline_payload_bytes = 0_u64;
    for (_, value) in &rows {
        let row = decode_row(value).expect("inventory row");
        if let Representation::Current(blob_id) = row.representation {
            json_file_descriptor_bytes += value.len() as u64;
            logical_bytes += blob_sizes.get(&blob_id).copied().unwrap_or(0);
            continue;
        }
        logical_bytes += row.payload_len;
        let fixed = (4 + 1 + 8 + 32 + 4 + row.metadata.len() + 1) as u64;
        native_metadata_bytes += fixed;
        match row.representation {
            Representation::Inline(bytes) => inline_payload_bytes += 8 + bytes.len() as u64,
            Representation::InlineDescriptor(_) => {
                inline_descriptor_bytes += value.len() as u64 - fixed
            }
            Representation::SharedDescriptor(_) => pointer_descriptor_bytes += 32,
            Representation::Current(_) => unreachable!(),
        }
    }
    let unique_chunk_bytes = chunks
        .iter()
        .map(|(_, value)| value.len() as u64)
        .sum::<u64>();
    let descriptor_bytes = descriptors
        .iter()
        .map(|(_, value)| value.len() as u64)
        .sum::<u64>();
    let descriptor_index_bytes = descriptors
        .iter()
        .filter(|(_, value)| value.starts_with(b"FEO1"))
        .map(|(_, value)| value.len() as u64)
        .sum::<u64>();
    let descriptor_leaf_bytes = descriptor_bytes.saturating_sub(descriptor_index_bytes);
    let json_blob_ref_bytes = blob_refs
        .iter()
        .map(|(_, value)| value.len() as u64)
        .sum::<u64>();
    let manifest_bytes = manifests
        .iter()
        .map(|(_, value)| value.len() as u64)
        .sum::<u64>();
    let manifest_chunk_bytes = manifest_chunks
        .iter()
        .map(|(_, value)| value.len() as u64)
        .sum::<u64>();
    let row_bytes = rows
        .iter()
        .map(|(_, value)| value.len() as u64)
        .sum::<u64>();
    let shared_bytes = logical_bytes.saturating_sub(unique_chunk_bytes);
    println!(
        "file_layout,kind=inventory,backend={},layout={},stage={stage},size={},metadata_width={},threshold={},chunk_policy={},chunk_min={},chunk_avg={},chunk_max={},fanout={},carrier_rows={},shared_copies={},row_objects={},isolated_carrier_bytes={row_bytes},cow_carrier_page_bytes={},native_typed_metadata_row_bytes={native_metadata_bytes},json_file_descriptor_bytes={json_file_descriptor_bytes},inline_descriptor_bytes={inline_descriptor_bytes},pointer_descriptor_bytes={pointer_descriptor_bytes},inline_payload_bytes={inline_payload_bytes},descriptor_objects={},descriptor_bytes={descriptor_leaf_bytes},descriptor_index_bytes={descriptor_index_bytes},json_blob_ref_bytes={json_blob_ref_bytes},manifest_objects={},manifest_bytes={manifest_bytes},manifest_chunk_objects={},manifest_chunk_bytes={manifest_chunk_bytes},chunk_objects={},chunk_bytes={unique_chunk_bytes},logical_referenced_bytes={logical_bytes},shared_bytes={shared_bytes},sharing_ratio={:.6},settled_path_bytes={}",
        context.backend,
        context.layout.name(),
        context.case.size,
        context.case.metadata_width,
        context.case.threshold,
        context.case.chunk_policy.name,
        context.case.chunk_policy.min,
        context.case.chunk_policy.avg,
        context.case.chunk_policy.max,
        context.case.fanout,
        context.case.carrier_rows,
        context.case.shared_copies,
        rows.len(),
        row_bytes.saturating_mul(context.case.carrier_rows as u64),
        descriptors.len(),
        manifests.len(),
        manifest_chunks.len(),
        chunks.len(),
        if logical_bytes == 0 {
            0.0
        } else {
            logical_bytes as f64 / unique_chunk_bytes.max(1) as f64
        },
        directory_bytes(context.path),
    );
    let _ = take_stats(context.stats);
}

async fn scan_space<S: Storage>(storage: &S, space: StorageSpace) -> Vec<(Key, Vec<u8>)> {
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin inventory read");
    let mut cursor = read
        .begin_scan(
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions::default(),
        )
        .await
        .expect("begin inventory scan");
    cursor
        .collect_all()
        .await
        .expect("collect inventory")
        .into_iter()
        .map(|entry| match entry.value {
            ProjectedValue::FullValue(value) => (entry.key, value.to_vec()),
            ProjectedValue::KeyOnly => unreachable!("full scan projection"),
        })
        .collect()
}

fn encode_row(row: &Row) -> Vec<u8> {
    if let Representation::Current(blob_id) = row.representation {
        return format!(
            "{{\"blob_id\":\"{}\",\"metadata\":\"{}\",\"path\":\"/bench/file.bin\",\"type\":\"file\"}}",
            hex_digest(blob_id),
            "m".repeat(row.metadata.len()),
        )
        .into_bytes();
    }
    let mut output = Vec::new();
    output.extend_from_slice(b"FER1");
    output.push(row.layout.tag());
    output.extend_from_slice(&row.payload_len.to_le_bytes());
    output.extend_from_slice(&row.whole_hash.0);
    output.extend_from_slice(&(row.metadata.len() as u32).to_le_bytes());
    output.extend_from_slice(&row.metadata);
    match &row.representation {
        Representation::Current(_) => unreachable!("current row encoded above"),
        Representation::Inline(bytes) => {
            output.push(1);
            output.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            output.extend_from_slice(bytes);
        }
        Representation::InlineDescriptor(refs) => {
            output.push(2);
            encode_refs(&mut output, refs);
        }
        Representation::SharedDescriptor(hash) => {
            output.push(3);
            output.extend_from_slice(&hash.0);
        }
    }
    output
}

fn decode_row(bytes: &[u8]) -> Result<Row, String> {
    if bytes.first() == Some(&b'{') {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        let blob_id = parse_digest_hex(
            value["blob_id"]
                .as_str()
                .ok_or_else(|| "current descriptor has no blob_id".to_owned())?,
        )?;
        let metadata = value["metadata"]
            .as_str()
            .ok_or_else(|| "current descriptor has no metadata".to_owned())?
            .as_bytes()
            .to_vec();
        return Ok(Row {
            layout: Layout::Current,
            payload_len: 0,
            whole_hash: Digest([0; 32]),
            metadata,
            representation: Representation::Current(blob_id),
        });
    }
    let mut input = Decoder::new(bytes);
    input.expect_magic(b"FER1")?;
    let layout = match input.u8()? {
        0 => Layout::Current,
        1 => Layout::E1,
        2 => Layout::E2,
        3 => Layout::E3,
        _ => return Err("unknown row layout".to_owned()),
    };
    let payload_len = input.u64()?;
    let whole_hash = input.digest()?;
    let metadata_len = input.u32()? as usize;
    let metadata = input.bytes(metadata_len)?.to_vec();
    let representation = match input.u8()? {
        1 => {
            let len = input.u64()? as usize;
            Representation::Inline(input.bytes(len)?.to_vec())
        }
        2 => Representation::InlineDescriptor(input.refs()?),
        3 => Representation::SharedDescriptor(input.digest()?),
        _ => return Err("unknown row representation".to_owned()),
    };
    input.finish()?;
    let representation_matches_layout = match layout {
        Layout::Current => false,
        Layout::E1 => matches!(representation, Representation::InlineDescriptor(_)),
        Layout::E2 => matches!(representation, Representation::SharedDescriptor(_)),
        Layout::E3 => matches!(
            representation,
            Representation::Inline(_) | Representation::SharedDescriptor(_)
        ),
    };
    if !representation_matches_layout {
        return Err("layout/representation mismatch".to_owned());
    }
    Ok(Row {
        layout,
        payload_len,
        whole_hash,
        metadata,
        representation,
    })
}

fn encode_leaf(refs: &[ChunkRef]) -> Vec<u8> {
    let mut output = b"FEL1".to_vec();
    encode_refs(&mut output, refs);
    output
}

fn decode_leaf(bytes: &[u8]) -> Result<Vec<ChunkRef>, String> {
    let mut input = Decoder::new(bytes);
    input.expect_magic(b"FEL1")?;
    let refs = input.refs()?;
    input.finish()?;
    Ok(refs)
}

struct RootDescriptor {
    payload_len: u64,
    whole_hash: Digest,
    leaves: Vec<Digest>,
}

fn encode_root(payload_len: u64, whole_hash: Digest, leaves: &[Digest]) -> Vec<u8> {
    let mut output = b"FEO1".to_vec();
    output.extend_from_slice(&payload_len.to_le_bytes());
    output.extend_from_slice(&whole_hash.0);
    output.extend_from_slice(&(leaves.len() as u32).to_le_bytes());
    for leaf in leaves {
        output.extend_from_slice(&leaf.0);
    }
    output
}

fn encode_blob_ref(blob_id: Digest, blob_hash: Digest, size: usize) -> Vec<u8> {
    format!(
        "{{\"id\":\"{}\",\"blob_hash\":\"{}\",\"size_bytes\":{size}}}",
        hex_digest(blob_id),
        hex_digest(blob_hash),
    )
    .into_bytes()
}

fn decode_blob_ref(bytes: &[u8], expected_id: Digest) -> Result<(Digest, usize), String> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    let id = parse_digest_hex(
        value["id"]
            .as_str()
            .ok_or_else(|| "BlobRef has no id".to_owned())?,
    )?;
    if id != expected_id {
        return Err("BlobRef id/key mismatch".to_owned());
    }
    let hash = parse_digest_hex(
        value["blob_hash"]
            .as_str()
            .ok_or_else(|| "BlobRef has no blob_hash".to_owned())?,
    )?;
    let size = value["size_bytes"]
        .as_u64()
        .and_then(|size| usize::try_from(size).ok())
        .ok_or_else(|| "BlobRef has invalid size_bytes".to_owned())?;
    Ok((hash, size))
}

fn hex_digest(digest: Digest) -> String {
    let mut output = String::with_capacity(64);
    for byte in digest.0 {
        use std::fmt::Write;
        write!(&mut output, "{byte:02x}").expect("write digest hex");
    }
    output
}

fn parse_digest_hex(value: &str) -> Result<Digest, String> {
    if value.len() != 64 {
        return Err("digest hex is not 64 characters".to_owned());
    }
    let mut bytes = [0_u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "invalid digest hex".to_owned())?;
    }
    Ok(Digest(bytes))
}

fn decode_root(bytes: &[u8]) -> Result<RootDescriptor, String> {
    let mut input = Decoder::new(bytes);
    input.expect_magic(b"FEO1")?;
    let payload_len = input.u64()?;
    let whole_hash = input.digest()?;
    let count = input.u32()? as usize;
    let mut leaves = Vec::with_capacity(count);
    for _ in 0..count {
        leaves.push(input.digest()?);
    }
    input.finish()?;
    Ok(RootDescriptor {
        payload_len,
        whole_hash,
        leaves,
    })
}

fn encode_refs(output: &mut Vec<u8>, refs: &[ChunkRef]) {
    output.extend_from_slice(&(refs.len() as u32).to_le_bytes());
    for item in refs {
        output.extend_from_slice(&item.len.to_le_bytes());
        output.extend_from_slice(&item.hash.0);
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| "truncated canonical value".to_owned())?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn expect_magic(&mut self, magic: &[u8]) -> Result<(), String> {
        if self.bytes(magic.len())? == magic {
            Ok(())
        } else {
            Err("canonical magic mismatch".to_owned())
        }
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.bytes(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?.try_into().expect("four bytes"),
        ))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().expect("eight bytes"),
        ))
    }

    fn digest(&mut self) -> Result<Digest, String> {
        Ok(Digest(self.bytes(32)?.try_into().expect("32 digest bytes")))
    }

    fn refs(&mut self) -> Result<Vec<ChunkRef>, String> {
        let count = self.u32()? as usize;
        if count > self.bytes.len() / 36 {
            return Err("implausible descriptor count".to_owned());
        }
        let mut refs = Vec::with_capacity(count);
        for _ in 0..count {
            refs.push(ChunkRef {
                len: self.u32()?,
                hash: self.digest()?,
            });
        }
        Ok(refs)
    }

    fn finish(&self) -> Result<(), String> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("trailing canonical bytes".to_owned())
        }
    }
}

fn row_key(branch: u32, version: u32) -> Key {
    let mut bytes = Vec::with_capacity(12);
    bytes.extend_from_slice(b"FEK1");
    bytes.extend_from_slice(&branch.to_be_bytes());
    bytes.extend_from_slice(&version.to_be_bytes());
    Key(Bytes::from(bytes))
}

fn digest_key(key: &Key) -> Result<Digest, String> {
    key.0
        .as_ref()
        .try_into()
        .map(Digest)
        .map_err(|_| "content-addressed key is not 32 bytes".to_owned())
}

fn deterministic_payload(size: usize, stream: u64) -> Vec<u8> {
    let mut output = vec![0; size];
    let mut state = stream ^ 0x9e37_79b9_7f4a_7c15;
    for (index, byte) in output.iter_mut().enumerate() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = (state ^ index as u64).to_le_bytes()[index & 7];
    }
    output
}

fn chunk_ranges(payload: &[u8], policy: ChunkPolicy) -> Vec<std::ops::Range<usize>> {
    if payload.is_empty() {
        return Vec::new();
    }
    fastcdc::v2020::FastCDC::new(
        payload,
        policy.min.try_into().expect("chunk minimum fits u32"),
        policy.avg.try_into().expect("chunk average fits u32"),
        policy.max.try_into().expect("chunk maximum fits u32"),
    )
    .map(|chunk| chunk.offset..chunk.offset + chunk.length)
    .collect()
}

fn parse_config() -> Config {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    Config {
        backends: string_list(
            &args,
            "backends",
            "LIX_FILE_LAYOUT_E_BACKENDS",
            "rocksdb,slatedb",
        ),
        sizes: number_list(&args, "sizes", "LIX_FILE_LAYOUT_E_SIZES", "4096,1048576"),
        metadata_widths: number_list(
            &args,
            "metadata-widths",
            "LIX_FILE_LAYOUT_E_METADATA_WIDTHS",
            "64",
        ),
        thresholds: number_list(
            &args,
            "thresholds",
            "LIX_FILE_LAYOUT_E_THRESHOLDS",
            "0,1024,32768,65536,131072,262144",
        ),
        chunk_policies: chunk_policies(&args),
        fanouts: number_list(&args, "fanouts", "LIX_FILE_LAYOUT_E_FANOUTS", "64"),
        carrier_rows: number_list(
            &args,
            "carrier-rows",
            "LIX_FILE_LAYOUT_E_CARRIER_ROWS",
            "64",
        ),
        shared_copies: number_list(
            &args,
            "shared-copies",
            "LIX_FILE_LAYOUT_E_SHARED_COPIES",
            "0",
        ),
        root: option_value(&args, "root", "LIX_FILE_LAYOUT_E_ROOT").map(PathBuf::from),
    }
}

fn expand_cases(config: &Config) -> Vec<Case> {
    let mut cases = Vec::new();
    for &size in &config.sizes {
        assert!(
            size <= MAX_PAYLOAD_BYTES,
            "size must be 0..={MAX_PAYLOAD_BYTES}"
        );
        for &metadata_width in &config.metadata_widths {
            assert!(
                metadata_width <= 1024 * 1024,
                "metadata width is bounded at 1 MiB"
            );
            for &threshold in &config.thresholds {
                for &chunk_policy in &config.chunk_policies {
                    for &fanout in &config.fanouts {
                        assert!(fanout > 0 && fanout <= 4096, "fanout must be 1..=4096");
                        for &carrier_rows in &config.carrier_rows {
                            for &shared_copies in &config.shared_copies {
                                assert!(carrier_rows > 0, "carrier rows must be positive");
                                cases.push(Case {
                                    size,
                                    metadata_width,
                                    threshold,
                                    chunk_policy,
                                    fanout,
                                    carrier_rows,
                                    shared_copies,
                                });
                                assert!(
                                    cases.len() <= MAX_CASES,
                                    "sweep exceeds {MAX_CASES} cases"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
    cases
}

fn option_value(args: &[String], name: &str, environment: &str) -> Option<String> {
    let prefix = format!("--{name}=");
    args.iter()
        .find_map(|argument| argument.strip_prefix(&prefix).map(str::to_owned))
        .or_else(|| std::env::var(environment).ok())
}

fn string_list(args: &[String], name: &str, environment: &str, default: &str) -> Vec<String> {
    option_value(args, name, environment)
        .unwrap_or_else(|| default.to_owned())
        .split(',')
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

fn number_list(args: &[String], name: &str, environment: &str, default: &str) -> Vec<usize> {
    string_list(args, name, environment, default)
        .into_iter()
        .map(|value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("invalid {name}: {value}"))
        })
        .collect()
}

fn chunk_policies(args: &[String]) -> Vec<ChunkPolicy> {
    string_list(
        args,
        "chunk-policies",
        "LIX_FILE_LAYOUT_E_CHUNK_POLICIES",
        "current,prototype",
    )
    .into_iter()
    .map(|name| match name.as_str() {
        "current" | "256k:1m:4m" => CURRENT_POLICY,
        "prototype" | "512k:512k:2m" => PROTOTYPE_POLICY,
        _ => panic!("unknown chunk policy '{name}'; use current or prototype"),
    })
    .collect()
}

fn print_case(context: &MeasureContext<'_>) {
    let base = deterministic_payload(context.case.size, 0);
    let base_chunk_count = chunk_ranges(&base, context.case.chunk_policy).len();
    let base_descriptor_bytes = 4_u64.saturating_add((base_chunk_count as u64).saturating_mul(36));
    println!(
        "file_layout,kind=case,backend={},layout={},size={},metadata_width={},threshold={},chunk_policy={},chunk_min={},chunk_avg={},chunk_max={},fanout={},carrier_rows={},shared_copies={},base_chunk_count={base_chunk_count},base_descriptor_bytes={base_descriptor_bytes},format=canonical_binary_v1,hash=blake3_256,current_json_envelopes=true",
        context.backend,
        context.layout.name(),
        context.case.size,
        context.case.metadata_width,
        context.case.threshold,
        context.case.chunk_policy.name,
        context.case.chunk_policy.min,
        context.case.chunk_policy.avg,
        context.case.chunk_policy.max,
        context.case.fanout,
        context.case.carrier_rows,
        context.case.shared_copies,
    );
}

fn projected_len(value: &ProjectedValue) -> usize {
    match value {
        ProjectedValue::KeyOnly => 0,
        ProjectedValue::FullValue(value) => value.len(),
    }
}

fn take_stats(stats: &Arc<Mutex<IoStats>>) -> IoStats {
    std::mem::take(&mut *stats.lock().expect("I/O counter mutex"))
}

fn resident_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmRSS:"))
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .map_or(0, |kilobytes| kilobytes.saturating_mul(1024))
}

fn cpu_micros() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return 0;
    }
    let usage = unsafe { usage.assume_init() };
    timeval_micros(usage.ru_utime).saturating_add(timeval_micros(usage.ru_stime))
}

fn timeval_micros(value: libc::timeval) -> u64 {
    (value.tv_sec as u64)
        .saturating_mul(1_000_000)
        .saturating_add(value.tv_usec as u64)
}

fn directory_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path).map_or(0, |entries| {
        entries.flatten().fold(0_u64, |total, entry| {
            let path = entry.path();
            total.saturating_add(if path.is_dir() {
                directory_bytes(&path)
            } else {
                entry.metadata().map_or(0, |metadata| metadata.len())
            })
        })
    })
}
