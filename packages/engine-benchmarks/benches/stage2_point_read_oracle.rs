//! Test/benchmark-only acceptance oracle for the Stage-2 point-read owner.
//!
//! This deliberately does not use or implement a production ForkTree. It
//! freezes the storage/read-view contract which the first runnable Stage-2
//! head must satisfy: one coherent `StorageRead` owns selector, catalog,
//! commit, authenticated tree, and value resolution for one logical point.

use std::alloc::{GlobalAlloc, Layout};
use std::collections::BTreeMap;
use std::future::Future;
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

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;

static COUNT_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
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
            && COUNT_ALLOCATIONS.load(Ordering::Relaxed)
        {
            ALLOCATED_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        replacement
    }
}

const SELECTOR_SPACE: StorageSpace =
    StorageSpace::mutable(SpaceId(0x00f2_0001), "qualification.stage2_point.selector");
const CATALOG_SPACE: StorageSpace =
    StorageSpace::immutable(SpaceId(0x00f2_0002), "qualification.stage2_point.catalog");
const OBJECT_SPACE: StorageSpace =
    StorageSpace::immutable(SpaceId(0x00f2_0003), "qualification.stage2_point.object");
const MAIN_SELECTOR: &[u8] = b"main";
const LEAF_ROWS: usize = 64;
const INTERNAL_CHILDREN: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ObjectId([u8; 32]);

impl ObjectId {
    fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    fn key(self) -> Key {
        Key(Bytes::copy_from_slice(&self.0))
    }

    fn hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

#[derive(Clone, Copy, Debug)]
enum Corruption {
    Healthy,
    MalformedSelector,
    MalformedCatalog,
    KindSubstitution,
    IdentitySubstitution,
}

impl Corruption {
    const fn label(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::MalformedSelector => "malformed_selector",
            Self::MalformedCatalog => "malformed_catalog",
            Self::KindSubstitution => "kind_substitution",
            Self::IdentitySubstitution => "identity_substitution",
        }
    }
}

#[derive(Clone, Debug)]
struct NodeRef {
    max_key: Vec<u8>,
    id: ObjectId,
}

#[derive(Clone, Debug)]
struct Fixture {
    selector: Bytes,
    catalog: BTreeMap<ObjectId, Bytes>,
    objects: BTreeMap<ObjectId, Bytes>,
    expected_digest: String,
    tree_depth: usize,
    expected_error: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, Default)]
struct IoStats {
    begin_reads: u64,
    begin_writes: u64,
    get_calls: u64,
    get_keys: u64,
    get_values: u64,
    get_value_bytes: u64,
    scan_calls: u64,
    write_batches: u64,
    write_puts: u64,
    write_bytes: u64,
    commits: u64,
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
}

impl<S> CountingStorage<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            stats: Arc::new(Mutex::new(IoStats::default())),
        }
    }

    fn reset(&self) {
        *self.stats.lock().expect("point-read stats mutex") = IoStats::default();
    }

    fn snapshot(&self) -> IoStats {
        *self.stats.lock().expect("point-read stats mutex")
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
        self.stats
            .lock()
            .expect("point-read stats mutex")
            .begin_reads += 1;
        Ok(CountingRead {
            inner: self.inner.begin_read(options).await?,
            stats: Arc::clone(&self.stats),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.stats
            .lock()
            .expect("point-read stats mutex")
            .begin_writes += 1;
        Ok(CountingWrite {
            inner: self.inner.begin_write(options).await?,
            stats: Arc::clone(&self.stats),
        })
    }
}

impl<R: StorageRead> StorageRead for CountingRead<R> {
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }

    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        {
            let mut stats = self.stats.lock().expect("point-read stats mutex");
            stats.get_calls += 1;
            stats.get_keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
        }
        let result = self.inner.get_many(requests).await?;
        let mut stats = self.stats.lock().expect("point-read stats mutex");
        for value in result.values.iter().flatten() {
            stats.get_values += 1;
            if let ProjectedValue::FullValue(bytes) = value {
                stats.get_value_bytes += bytes.len() as u64;
            }
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
        self.stats
            .lock()
            .expect("point-read stats mutex")
            .scan_calls += 1;
        let order = options.order;
        let inner = self.inner.begin_scan(space, range.clone(), options).await?;
        ScanCursor::from_source(range, order, CountingScanSource { inner })
    }
}

impl StorageScanSource for CountingScanSource<'_> {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(self.inner.next_page(limit_rows))
    }
}

impl<W: StorageWrite> StorageWrite for CountingWrite<W> {
    async fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("point-read stats mutex");
            stats.write_batches += 1;
            stats.write_puts += entries.entries.len() as u64;
            stats.write_bytes += entries
                .entries
                .iter()
                .map(|entry| (entry.key.0.len() + entry.value.bytes.len()) as u64)
                .sum::<u64>();
        }
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
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
        stats.lock().expect("point-read stats mutex").commits += 1;
        Ok(result)
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ProcessIo {
    read_calls: u64,
    write_calls: u64,
    read_bytes: u64,
    write_bytes: u64,
}

impl ProcessIo {
    fn delta(self, earlier: Self) -> Self {
        Self {
            read_calls: self.read_calls.saturating_sub(earlier.read_calls),
            write_calls: self.write_calls.saturating_sub(earlier.write_calls),
            read_bytes: self.read_bytes.saturating_sub(earlier.read_bytes),
            write_bytes: self.write_bytes.saturating_sub(earlier.write_bytes),
        }
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    assert_eq!(
        args.len(),
        5,
        "usage: stage2_point_read_oracle <rocksdb|slatedb> <fresh-path> <rows> <samples>"
    );
    let backend = args[1].as_str();
    let path = PathBuf::from(&args[2]);
    let rows = args[3].parse::<usize>().expect("rows must be an integer");
    let samples = args[4]
        .parse::<usize>()
        .expect("samples must be an integer");
    assert!(rows > 0 && samples > 0);
    assert!(
        !path.exists(),
        "oracle path must be fresh: {}",
        path.display()
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create point-read oracle runtime");
    runtime.block_on(async {
        match backend {
            "rocksdb" => run_rocks(&path, rows, samples).await,
            "slatedb" => run_slate(&path, rows, samples).await,
            other => panic!("unsupported backend '{other}'"),
        }
    });
}

async fn run_rocks(path: &Path, rows: usize, samples: usize) {
    let database = RocksDB::open(path).expect("open point-read RocksDB");
    let storage = CountingStorage::new(database.clone());
    let fixture = build_fixture(rows, Corruption::Healthy);
    seed(&storage, &fixture).await;
    database.flush().expect("flush point-read RocksDB seed");
    benchmark("rocksdb", path, rows, samples, &storage, &fixture, None).await;
    database.flush().expect("flush point-read RocksDB result");
    drop(storage);
    drop(database);

    let reopened = RocksDB::open(path).expect("cold reopen point-read RocksDB");
    cold_reopen("rocksdb", rows, CountingStorage::new(reopened), &fixture).await;
    run_rocks_corruption(path, rows).await;
}

async fn run_slate(path: &Path, rows: usize, samples: usize) {
    let counters = SlateDBIoCounters::default();
    let database =
        SlateDB::open_with_io_counters(path, counters.clone()).expect("open point-read SlateDB");
    let storage = CountingStorage::new(database.clone());
    let fixture = build_fixture(rows, Corruption::Healthy);
    seed(&storage, &fixture).await;
    database
        .flush()
        .await
        .expect("flush point-read SlateDB seed");
    benchmark(
        "slatedb",
        path,
        rows,
        samples,
        &storage,
        &fixture,
        Some(&counters),
    )
    .await;
    database
        .flush()
        .await
        .expect("flush point-read SlateDB result");
    drop(storage);
    drop(database);

    let reopened = SlateDB::open(path).expect("cold reopen point-read SlateDB");
    cold_reopen("slatedb", rows, CountingStorage::new(reopened), &fixture).await;
    run_slate_corruption(path, rows).await;
}

async fn run_rocks_corruption(path: &Path, rows: usize) {
    for corruption in corruption_cases() {
        let case_path = path.with_extension(format!("rocks.{}", corruption.label()));
        let database = RocksDB::open(&case_path).expect("open RocksDB corruption fixture");
        let storage = CountingStorage::new(database.clone());
        let fixture = build_fixture(rows, corruption);
        seed(&storage, &fixture).await;
        database.flush().expect("flush RocksDB corruption fixture");
        drop(storage);
        drop(database);
        let reopened = RocksDB::open(&case_path).expect("reopen RocksDB corruption fixture");
        corruption_gate("rocksdb", CountingStorage::new(reopened), &fixture).await;
    }
}

async fn run_slate_corruption(path: &Path, rows: usize) {
    for corruption in corruption_cases() {
        let case_path = path.with_extension(format!("slate.{}", corruption.label()));
        let database = SlateDB::open(&case_path).expect("open SlateDB corruption fixture");
        let storage = CountingStorage::new(database.clone());
        let fixture = build_fixture(rows, corruption);
        seed(&storage, &fixture).await;
        database
            .flush()
            .await
            .expect("flush SlateDB corruption fixture");
        drop(storage);
        drop(database);
        let reopened = SlateDB::open(&case_path).expect("reopen SlateDB corruption fixture");
        corruption_gate("slatedb", CountingStorage::new(reopened), &fixture).await;
    }
}

fn corruption_cases() -> [Corruption; 4] {
    [
        Corruption::MalformedSelector,
        Corruption::MalformedCatalog,
        Corruption::KindSubstitution,
        Corruption::IdentitySubstitution,
    ]
}

async fn benchmark<S: Storage>(
    backend: &str,
    path: &Path,
    rows: usize,
    samples: usize,
    storage: &CountingStorage<S>,
    fixture: &Fixture,
    slate_counters: Option<&SlateDBIoCounters>,
) {
    let warm_digest = read_all(storage, rows).await.expect("warm point reads");
    assert_eq!(warm_digest, fixture.expected_digest);
    let gets_per_point = u64::try_from(fixture.tree_depth + 4).expect("get count fits u64");

    for sample in 1..=samples {
        storage.reset();
        let slate_before = slate_counters.map(SlateDBIoCounters::snapshot);
        let process_before = process_io();
        let disk_before = directory_bytes(path);
        let rss_before = resident_bytes();
        let peak_before = peak_resident_bytes();
        let cpu_before = process_cpu_nanos();
        begin_allocation_profile();
        let started = Instant::now();
        let digest = read_all(storage, rows).await.expect("measured point reads");
        let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
        let (alloc_bytes, alloc_calls) = end_allocation_profile();
        let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
        let rss_after = resident_bytes();
        let peak_after = peak_resident_bytes();
        let disk_after = directory_bytes(path);
        let process = process_io().delta(process_before);
        let stats = storage.snapshot();
        let physical = slate_before.map_or_else(SlateDBIoSnapshot::default, |before| {
            slate_counters
                .expect("Slate counters exist")
                .snapshot()
                .saturating_sub(before)
        });
        assert_eq!(digest, fixture.expected_digest);
        assert_eq!(
            stats.begin_reads, rows as u64,
            "exactly one coherent StorageRead per point"
        );
        assert_eq!(stats.begin_writes, 0, "point read must not write");
        assert_eq!(stats.write_batches, 0, "point read must not stage writes");
        assert_eq!(stats.commits, 0, "point read must not commit");
        assert_eq!(stats.scan_calls, 0, "point read must not scan");
        assert_eq!(stats.get_calls, rows as u64 * gets_per_point);
        assert_eq!(stats.get_keys, stats.get_calls);
        println!(
            "stage2_point_read_oracle,sample={sample},backend={backend},rows={rows},wall_us={wall_us:.3},cpu_us={cpu_us:.3},alloc_bytes={alloc_bytes},alloc_calls={alloc_calls},rss_before_bytes={rss_before},rss_after_bytes={rss_after},peak_before_bytes={peak_before},peak_after_bytes={peak_after},begin_reads={},gets={},get_keys={},get_values={},get_value_bytes={},writes={},commits={},process_read_calls={},process_write_calls={},process_read_bytes={},process_write_bytes={},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},disk_before_bytes={disk_before},disk_after_bytes={disk_after},tree_depth={},digest={},verified=true",
            stats.begin_reads,
            stats.get_calls,
            stats.get_keys,
            stats.get_values,
            stats.get_value_bytes,
            stats.write_batches,
            stats.commits,
            process.read_calls,
            process.write_calls,
            process.read_bytes,
            process.write_bytes,
            physical.read_objects,
            physical.read_bytes,
            physical.write_objects,
            physical.write_bytes,
            fixture.tree_depth,
            fixture.expected_digest,
        );
    }
}

async fn cold_reopen<S: Storage>(
    backend: &str,
    rows: usize,
    storage: CountingStorage<S>,
    fixture: &Fixture,
) {
    storage.reset();
    let digest = read_all(&storage, rows)
        .await
        .expect("cold-reopen point reads");
    let stats = storage.snapshot();
    assert_eq!(digest, fixture.expected_digest);
    assert_eq!(stats.begin_reads, rows as u64);
    println!(
        "stage2_point_read_cold_reopen,backend={backend},rows={rows},begin_reads={},gets={},digest={digest},verified=true",
        stats.begin_reads, stats.get_calls
    );
}

async fn corruption_gate<S: Storage>(
    backend: &str,
    storage: CountingStorage<S>,
    fixture: &Fixture,
) {
    storage.reset();
    let error = read_point(&storage, row_key(0).as_slice())
        .await
        .expect_err("corrupt point read must fail closed");
    let expected = fixture.expected_error.expect("corruption has error token");
    assert!(
        error.contains(expected),
        "expected error token '{expected}', got '{error}'"
    );
    let stats = storage.snapshot();
    assert_eq!(
        stats.begin_reads, 1,
        "corruption must fail within its one coherent view"
    );
    assert_eq!(stats.begin_writes, 0);
    println!(
        "stage2_point_read_corruption,backend={backend},case={},begin_reads={},gets={},error_token={expected},cold_reopen=true,fail_closed=true",
        corruption_label(fixture),
        stats.begin_reads,
        stats.get_calls,
    );
}

fn corruption_label(fixture: &Fixture) -> &'static str {
    match fixture.expected_error {
        Some("selector authentication mismatch") => "malformed_selector",
        Some("malformed catalog") => "malformed_catalog",
        Some("catalog kind mismatch") => "kind_substitution",
        Some("catalog authentication mismatch") => "identity_substitution",
        _ => "unknown",
    }
}

async fn seed<S: Storage>(storage: &S, fixture: &Fixture) {
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin point-read seed");
    write
        .put_many(
            CATALOG_SPACE,
            PutBatch {
                entries: fixture
                    .catalog
                    .iter()
                    .map(|(id, bytes)| PutEntry {
                        key: id.key(),
                        value: StoredValue {
                            bytes: bytes.clone(),
                        },
                    })
                    .collect(),
            },
        )
        .await
        .expect("seed point-read catalog");
    write
        .put_many(
            OBJECT_SPACE,
            PutBatch {
                entries: fixture
                    .objects
                    .iter()
                    .map(|(id, bytes)| PutEntry {
                        key: id.key(),
                        value: StoredValue {
                            bytes: bytes.clone(),
                        },
                    })
                    .collect(),
            },
        )
        .await
        .expect("seed point-read objects");
    write
        .put_many(
            SELECTOR_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: Key(Bytes::from_static(MAIN_SELECTOR)),
                    value: StoredValue {
                        bytes: fixture.selector.clone(),
                    },
                }],
            },
        )
        .await
        .expect("seed point-read selector");
    write.commit().await.expect("commit point-read seed");
}

async fn read_all<S: Storage>(storage: &S, rows: usize) -> Result<String, String> {
    let mut hasher = blake3::Hasher::new();
    for index in 0..rows {
        let key = row_key(index);
        let value = read_point(storage, &key).await?;
        hasher.update(&(key.len() as u32).to_le_bytes());
        hasher.update(&key);
        hasher.update(&(value.len() as u32).to_le_bytes());
        hasher.update(&value);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

async fn read_point<S: Storage>(storage: &S, key: &[u8]) -> Result<Vec<u8>, String> {
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| error.to_string())?;
    read_point_in_view(&read, key).await
}

async fn read_point_in_view<R: StorageRead>(read: &R, key: &[u8]) -> Result<Vec<u8>, String> {
    let selector = get_one(read, SELECTOR_SPACE, Key(Bytes::from_static(MAIN_SELECTOR))).await?;
    let catalog_id = decode_selector(&selector)?;
    let catalog = get_authenticated(read, CATALOG_SPACE, catalog_id, "catalog").await?;
    let commit_id = decode_catalog(&catalog)?;
    let commit = get_authenticated(read, OBJECT_SPACE, commit_id, "commit").await?;
    let mut node_id = decode_commit(&commit)?;
    let value_id = loop {
        let bytes = get_authenticated(read, OBJECT_SPACE, node_id, "tree object").await?;
        match decode_node(&bytes)? {
            DecodedNode::Leaf(entries) => {
                break entries
                    .binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
                    .ok()
                    .map(|index| entries[index].1)
                    .ok_or_else(|| "point identity is absent".to_string())?;
            }
            DecodedNode::Internal(children) => {
                node_id = children
                    .iter()
                    .find(|(max_key, _)| key <= max_key.as_slice())
                    .or_else(|| children.last())
                    .map(|(_, id)| *id)
                    .ok_or_else(|| "empty internal node".to_string())?;
            }
        }
    };
    let value = get_authenticated(read, OBJECT_SPACE, value_id, "value").await?;
    decode_value(&value)
}

async fn get_one<R: StorageRead>(read: &R, space: StorageSpace, key: Key) -> Result<Bytes, String> {
    let result = read
        .get_many(&[GetManyRequest {
            space,
            keys: std::slice::from_ref(&key),
            opts: GetOptions::default(),
        }])
        .await
        .map_err(|error| error.to_string())?;
    match result.values.into_iter().next().flatten() {
        Some(ProjectedValue::FullValue(bytes)) => Ok(bytes),
        Some(ProjectedValue::KeyOnly) => {
            Err("point read unexpectedly received key-only data".into())
        }
        None => Err(format!("missing value in {space}")),
    }
}

async fn get_authenticated<R: StorageRead>(
    read: &R,
    space: StorageSpace,
    id: ObjectId,
    role: &str,
) -> Result<Bytes, String> {
    let bytes = get_one(read, space, id.key()).await?;
    if ObjectId::of(&bytes) != id {
        return Err(format!("{role} authentication mismatch for {}", id.hex()));
    }
    Ok(bytes)
}

enum DecodedNode {
    Leaf(Vec<(Vec<u8>, ObjectId)>),
    Internal(Vec<(Vec<u8>, ObjectId)>),
}

fn build_fixture(rows: usize, corruption: Corruption) -> Fixture {
    let mut objects = BTreeMap::new();
    let mut leaves = Vec::new();
    let mut expected = blake3::Hasher::new();
    for chunk_start in (0..rows).step_by(LEAF_ROWS) {
        let end = (chunk_start + LEAF_ROWS).min(rows);
        let mut entries = Vec::with_capacity(end - chunk_start);
        for index in chunk_start..end {
            let key = row_key(index);
            let value = row_value(index);
            expected.update(&(key.len() as u32).to_le_bytes());
            expected.update(&key);
            expected.update(&(value.len() as u32).to_le_bytes());
            expected.update(&value);
            let value_bytes = encode_value(&value);
            let value_id = stage(&mut objects, value_bytes);
            entries.push((key, value_id));
        }
        let max_key = entries.last().expect("leaf has rows").0.clone();
        let id = stage(&mut objects, encode_node(b"LEF1", &entries));
        leaves.push(NodeRef { max_key, id });
    }

    let mut level = leaves;
    let mut tree_depth = 1;
    while level.len() > 1 {
        level = level
            .chunks(INTERNAL_CHILDREN)
            .map(|children| {
                let entries = children
                    .iter()
                    .map(|child| (child.max_key.clone(), child.id))
                    .collect::<Vec<_>>();
                let max_key = entries.last().expect("internal has children").0.clone();
                let id = stage(&mut objects, encode_node(b"INT1", &entries));
                NodeRef { max_key, id }
            })
            .collect();
        tree_depth += 1;
    }
    let root = level[0].id;
    let commit_bytes = encode_id_object(b"CMT1", root);
    let commit_id = stage(&mut objects, commit_bytes.clone());
    let catalog_bytes = encode_id_object(b"CAT1", commit_id);
    let catalog_id = ObjectId::of(&catalog_bytes);
    let mut catalog = BTreeMap::new();
    catalog.insert(catalog_id, catalog_bytes.clone());

    let (selector, expected_error) = match corruption {
        Corruption::Healthy => (encode_selector(catalog_id), None),
        Corruption::MalformedSelector => (
            Bytes::from_static(b"SEL1 malformed"),
            Some("selector authentication mismatch"),
        ),
        Corruption::MalformedCatalog => {
            let malformed = Bytes::from_static(b"CAT1");
            let malformed_id = ObjectId::of(&malformed);
            catalog.insert(malformed_id, malformed);
            (encode_selector(malformed_id), Some("malformed catalog"))
        }
        Corruption::KindSubstitution => {
            catalog.insert(commit_id, commit_bytes);
            (encode_selector(commit_id), Some("catalog kind mismatch"))
        }
        Corruption::IdentitySubstitution => {
            catalog.insert(
                catalog_id,
                Bytes::from_static(b"CAT1 substituted physical bytes"),
            );
            (
                encode_selector(catalog_id),
                Some("catalog authentication mismatch"),
            )
        }
    };
    Fixture {
        selector,
        catalog,
        objects,
        expected_digest: expected.finalize().to_hex().to_string(),
        tree_depth,
        expected_error,
    }
}

fn stage(objects: &mut BTreeMap<ObjectId, Bytes>, bytes: Bytes) -> ObjectId {
    let id = ObjectId::of(&bytes);
    objects.insert(id, bytes);
    id
}

fn encode_selector(catalog: ObjectId) -> Bytes {
    let mut bytes = Vec::with_capacity(68);
    bytes.extend_from_slice(b"SEL1");
    bytes.extend_from_slice(&catalog.0);
    let mut checksum = blake3::Hasher::new();
    checksum.update(b"stage2-point-selector");
    checksum.update(&bytes);
    bytes.extend_from_slice(checksum.finalize().as_bytes());
    Bytes::from(bytes)
}

fn decode_selector(bytes: &[u8]) -> Result<ObjectId, String> {
    if bytes.len() != 68 || &bytes[..4] != b"SEL1" {
        return Err("selector authentication mismatch".into());
    }
    let mut checksum = blake3::Hasher::new();
    checksum.update(b"stage2-point-selector");
    checksum.update(&bytes[..36]);
    if checksum.finalize().as_bytes() != &bytes[36..] {
        return Err("selector authentication mismatch".into());
    }
    let mut id = [0; 32];
    id.copy_from_slice(&bytes[4..36]);
    Ok(ObjectId(id))
}

fn encode_id_object(kind: &[u8; 4], id: ObjectId) -> Bytes {
    let mut bytes = Vec::with_capacity(36);
    bytes.extend_from_slice(kind);
    bytes.extend_from_slice(&id.0);
    Bytes::from(bytes)
}

fn decode_catalog(bytes: &[u8]) -> Result<ObjectId, String> {
    decode_id_object(bytes, b"CAT1", "catalog")
}

fn decode_commit(bytes: &[u8]) -> Result<ObjectId, String> {
    decode_id_object(bytes, b"CMT1", "commit")
}

fn decode_id_object(bytes: &[u8], kind: &[u8; 4], role: &str) -> Result<ObjectId, String> {
    if bytes.len() != 36 {
        return Err(format!("malformed {role}"));
    }
    if &bytes[..4] != kind {
        return Err(format!("{role} kind mismatch"));
    }
    let mut id = [0; 32];
    id.copy_from_slice(&bytes[4..]);
    Ok(ObjectId(id))
}

fn encode_value(value: &[u8]) -> Bytes {
    let mut bytes = Vec::with_capacity(8 + value.len());
    bytes.extend_from_slice(b"VAL1");
    bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
    bytes.extend_from_slice(value);
    Bytes::from(bytes)
}

fn decode_value(bytes: &[u8]) -> Result<Vec<u8>, String> {
    if bytes.len() < 8 || &bytes[..4] != b"VAL1" {
        return Err("value kind or length mismatch".into());
    }
    let length = u32::from_le_bytes(bytes[4..8].try_into().expect("length bytes")) as usize;
    if bytes.len() != length + 8 {
        return Err("value length mismatch".into());
    }
    Ok(bytes[8..].to_vec())
}

fn encode_node(kind: &[u8; 4], entries: &[(Vec<u8>, ObjectId)]) -> Bytes {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(kind);
    bytes.extend_from_slice(&(entries.len() as u16).to_le_bytes());
    for (key, id) in entries {
        bytes.extend_from_slice(&(key.len() as u16).to_le_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&id.0);
    }
    Bytes::from(bytes)
}

fn decode_node(bytes: &[u8]) -> Result<DecodedNode, String> {
    if bytes.len() < 6 {
        return Err("malformed tree node".into());
    }
    let leaf = match &bytes[..4] {
        b"LEF1" => true,
        b"INT1" => false,
        _ => return Err("tree node kind mismatch".into()),
    };
    let count = u16::from_le_bytes(bytes[4..6].try_into().expect("node count")) as usize;
    let mut offset = 6;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 2 > bytes.len() {
            return Err("malformed tree node key length".into());
        }
        let key_length =
            u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("key length")) as usize;
        offset += 2;
        if offset + key_length + 32 > bytes.len() {
            return Err("malformed tree node entry".into());
        }
        let key = bytes[offset..offset + key_length].to_vec();
        offset += key_length;
        let mut id = [0; 32];
        id.copy_from_slice(&bytes[offset..offset + 32]);
        offset += 32;
        entries.push((key, ObjectId(id)));
    }
    if offset != bytes.len() || entries.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err("malformed or unordered tree node".into());
    }
    if leaf {
        Ok(DecodedNode::Leaf(entries))
    } else {
        Ok(DecodedNode::Internal(entries))
    }
}

fn row_key(index: usize) -> Vec<u8> {
    format!("row-{index:09}").into_bytes()
}

fn row_value(index: usize) -> Vec<u8> {
    format!("value-{index:09}-{:032}", index % 10_000).into_bytes()
}

fn begin_allocation_profile() {
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    COUNT_ALLOCATIONS.store(true, Ordering::Release);
}

fn end_allocation_profile() -> (u64, u64) {
    COUNT_ALLOCATIONS.store(false, Ordering::Release);
    (
        ALLOCATED_BYTES.load(Ordering::Relaxed),
        ALLOCATION_CALLS.load(Ordering::Relaxed),
    )
}

fn process_cpu_nanos() -> u64 {
    let mut timespec = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let result = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut timespec) };
    assert_eq!(result, 0, "read process CPU clock");
    (timespec.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(timespec.tv_nsec as u64)
}

fn resident_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").expect("read process statm");
    let pages = statm
        .split_whitespace()
        .nth(1)
        .expect("resident pages")
        .parse::<u64>()
        .expect("resident page count");
    pages.saturating_mul(4096)
}

fn peak_resident_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .expect("read process status")
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default()
        .saturating_mul(1024)
}

fn process_io() -> ProcessIo {
    let mut io = ProcessIo::default();
    for line in std::fs::read_to_string("/proc/self/io")
        .expect("read process I/O counters")
        .lines()
    {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim().parse::<u64>().unwrap_or_default();
        match key {
            "syscr" => io.read_calls = value,
            "syscw" => io.write_calls = value,
            "read_bytes" => io.read_bytes = value,
            "write_bytes" => io.write_bytes = value,
            _ => {}
        }
    }
    io
}

fn directory_bytes(path: &Path) -> u64 {
    fn visit(path: &Path) -> u64 {
        let Ok(metadata) = std::fs::symlink_metadata(path) else {
            return 0;
        };
        if metadata.is_file() {
            return metadata.len();
        }
        if !metadata.is_dir() {
            return 0;
        }
        std::fs::read_dir(path)
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| visit(&entry.path()))
            .sum()
    }
    visit(path)
}
