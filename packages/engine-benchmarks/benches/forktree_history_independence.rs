#![allow(clippy::large_futures)]

#[allow(dead_code)]
#[path = "forktree_replacement/model.rs"]
mod model;

use std::alloc::GlobalAlloc;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use lix::storage::{
    CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, ProjectedValue, PutBatch,
    ReadOptions, ScanChunk, ScanOptions, Storage, StorageError, StorageRead, StorageWrite,
    WriteOptions,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

use model::{ApplyAccounting, ForkTree, Mutation, ObjectId, RelationalValue, StateInspection};

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;

static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATION_ENABLED: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && ALLOCATION_ENABLED.load(Ordering::Relaxed) {
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }
}

fn begin_allocations() {
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATION_ENABLED.store(true, Ordering::Relaxed);
}

fn end_allocations() -> (u64, u64) {
    ALLOCATION_ENABLED.store(false, Ordering::Relaxed);
    (
        ALLOCATED_BYTES.load(Ordering::Relaxed),
        ALLOCATION_CALLS.load(Ordering::Relaxed),
    )
}

#[derive(Clone, Debug, Default)]
struct IoStats {
    begin_reads: u64,
    begin_writes: u64,
    get_calls: u64,
    get_keys: u64,
    get_values: u64,
    get_value_bytes: u64,
    scan_calls: u64,
    scan_entries: u64,
    scan_value_bytes: u64,
    write_batches: u64,
    write_puts: u64,
    write_deletes: u64,
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
        self.stats.lock().expect("I/O stats").begin_reads += 1;
        Ok(CountingRead {
            inner: self.inner.begin_read(options).await?,
            stats: Arc::clone(&self.stats),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.stats.lock().expect("I/O stats").begin_writes += 1;
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
            let mut stats = self.stats.lock().expect("I/O stats");
            stats.get_calls += 1;
            stats.get_keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
        }
        let result = self.inner.get_many(requests).await?;
        let mut stats = self.stats.lock().expect("I/O stats");
        for value in result.values.iter().flatten() {
            stats.get_values += 1;
            stats.get_value_bytes += projected_len(value) as u64;
        }
        drop(stats);
        Ok(result)
    }

    async fn scan(
        &self,
        space: lix::storage::StorageSpace,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        self.stats.lock().expect("I/O stats").scan_calls += 1;
        let chunk = self.inner.scan(space, range, options).await?;
        let mut stats = self.stats.lock().expect("I/O stats");
        stats.scan_entries += chunk.entries.len() as u64;
        stats.scan_value_bytes += chunk
            .entries
            .iter()
            .map(|entry| projected_len(&entry.value) as u64)
            .sum::<u64>();
        drop(stats);
        Ok(chunk)
    }
}

impl<W: StorageWrite> StorageWrite for CountingWrite<W> {
    async fn put_many(
        &mut self,
        space: lix::storage::StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats");
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

    async fn delete_many(
        &mut self,
        space: lix::storage::StorageSpace,
        keys: &[Key],
    ) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats");
            stats.write_batches += 1;
            stats.write_deletes += keys.len() as u64;
            stats.write_bytes += keys.iter().map(|key| key.0.len() as u64).sum::<u64>();
        }
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: lix::storage::StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        self.stats.lock().expect("I/O stats").commits += 1;
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

#[derive(Clone, Debug)]
struct PublicationMetric {
    name: &'static str,
    wall_us: u64,
    cpu_us: u64,
    allocated_bytes: u64,
    allocation_calls: u64,
    rss_before: u64,
    rss_after: u64,
    disk_before: u64,
    disk_after: u64,
    io: IoStats,
    physical: SlateDBIoSnapshot,
    accounting: ApplyAccounting,
}

#[derive(Clone)]
struct Variant {
    name: &'static str,
    commit: ObjectId,
    inspection: StateInspection,
}

struct PreparedGate {
    bulk: Variant,
    equal: Vec<Variant>,
    changed: Variant,
    publications: Vec<PublicationMetric>,
    expected: Vec<(Vec<u8>, Vec<u8>)>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let backend = args.get(1).map(String::as_str).unwrap_or("rocksdb");
    let rows = args
        .get(2)
        .map(|value| value.parse::<usize>().expect("rows must be usize"))
        .unwrap_or(1_000);
    assert!(rows >= 128, "history gate needs at least 128 rows");
    match backend {
        "rocksdb" => run_rocks(rows).await,
        "slatedb" => run_slate(rows).await,
        other => panic!("unknown backend '{other}'"),
    }
}

async fn run_rocks(rows: usize) {
    let directory = tempfile::tempdir().expect("RocksDB history directory");
    let database = RocksDB::open(directory.path()).expect("open RocksDB history gate");
    let (storage, stats) = CountingStorage::new(database.clone());
    let gate = prepare_gate(
        ForkTree::new(storage.clone()),
        &stats,
        directory.path(),
        None,
        rows,
    )
    .await;
    database.flush().expect("flush RocksDB history gate");
    let post_flush_disk = directory_bytes(directory.path());
    drop(storage);
    drop(database);

    let database = RocksDB::open(directory.path()).expect("reopen RocksDB history gate");
    let (storage, stats) = CountingStorage::new(database.clone());
    finish_gate(
        "rocksdb",
        ForkTree::new(storage),
        &stats,
        directory.path(),
        None,
        rows,
        gate,
        post_flush_disk,
    )
    .await;
    database
        .flush()
        .expect("flush reopened RocksDB history gate");
}

async fn run_slate(rows: usize) {
    let directory = tempfile::tempdir().expect("SlateDB history directory");
    let counters = SlateDBIoCounters::default();
    let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
        .expect("open SlateDB history gate");
    let (storage, stats) = CountingStorage::new(database.clone());
    let gate = prepare_gate(
        ForkTree::new(storage.clone()),
        &stats,
        directory.path(),
        Some(&counters),
        rows,
    )
    .await;
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush SlateDB history gate");
    let post_flush_disk = directory_bytes(directory.path());
    drop(storage);
    drop(database);

    let counters = SlateDBIoCounters::default();
    let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
        .expect("reopen SlateDB history gate");
    let (storage, stats) = CountingStorage::new(database.clone());
    finish_gate(
        "slatedb",
        ForkTree::new(storage),
        &stats,
        directory.path(),
        Some(&counters),
        rows,
        gate,
        post_flush_disk,
    )
    .await;
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush reopened SlateDB history gate");
}

async fn prepare_gate<S>(
    tree: ForkTree<CountingStorage<S>>,
    stats: &Arc<Mutex<IoStats>>,
    path: &Path,
    physical: Option<&SlateDBIoCounters>,
    rows: usize,
) -> PreparedGate
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let expected = fixture(rows);
    let bulk_metric = begin_metric("bulk_sorted", stats, path, physical);
    let bulk_commit = tree.initialize(&expected).await.expect("bulk initialize");
    let mut publications = vec![end_metric(
        bulk_metric,
        stats,
        path,
        physical,
        ApplyAccounting::default(),
    )];
    let bulk = inspect_variant(&tree, "bulk_sorted", bulk_commit).await;

    tree.create_branch("seed", Some(bulk_commit))
        .await
        .expect("create seed branch");
    let deletes = expected
        .iter()
        .filter(|(key, _)| key.as_slice() != b"meta/seed")
        .map(|(key, _)| Mutation::Delete { key: key.clone() })
        .collect::<Vec<_>>();
    let (seed, _) = tree
        .apply_sorted_mutations_on("seed", &deletes)
        .await
        .expect("reduce seed branch");

    let sorted_order = (0..rows).collect::<Vec<_>>();
    let (sorted, metric) = build_insert_history(
        &tree,
        stats,
        path,
        physical,
        "sorted_insert",
        seed,
        &expected[1..],
        &sorted_order,
        rows,
    )
    .await;
    publications.push(metric);

    let mut random_order = sorted_order.clone();
    deterministic_shuffle(&mut random_order, 0x91a7_5eed);
    let (random, metric) = build_insert_history(
        &tree,
        stats,
        path,
        physical,
        "random_batches",
        seed,
        &expected[1..],
        &random_order,
        32,
    )
    .await;
    publications.push(metric);

    let mut reverse_transactions = sorted_order
        .chunks(32)
        .rev()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    let (reverse, metric) = build_insert_history(
        &tree,
        stats,
        path,
        physical,
        "reverse_transactions",
        seed,
        &expected[1..],
        &reverse_transactions,
        32,
    )
    .await;
    publications.push(metric);

    reverse_transactions.clear();
    let adversarial_order = split_boundary_order(rows);
    let (adversarial, metric) = build_insert_history(
        &tree,
        stats,
        path,
        physical,
        "adversarial_splits",
        seed,
        &expected[1..],
        &adversarial_order,
        1,
    )
    .await;
    publications.push(metric);

    tree.create_branch("delete_reinsert", Some(sorted.commit))
        .await
        .expect("create delete/reinsert branch");
    let selected = (0..rows)
        .filter(|index| index % 16 == 15 || index % 64 == 31 || index % 64 == 32)
        .collect::<Vec<_>>();
    let metric_start = begin_metric("delete_reinsert", stats, path, physical);
    let delete_mutations = selected
        .iter()
        .map(|index| Mutation::Delete {
            key: expected[index + 1].0.clone(),
        })
        .collect::<Vec<_>>();
    let (_, mut accounting) = tree
        .apply_sorted_mutations_on("delete_reinsert", &delete_mutations)
        .await
        .expect("delete split-boundary rows");
    let insert_mutations = selected
        .iter()
        .map(|index| Mutation::Insert {
            key: expected[index + 1].0.clone(),
            value: RelationalValue::Bytes(expected[index + 1].1.clone()),
        })
        .collect::<Vec<_>>();
    let (delete_reinsert_commit, insert_accounting) = tree
        .apply_sorted_mutations_on("delete_reinsert", &insert_mutations)
        .await
        .expect("reinsert split-boundary rows");
    accounting += insert_accounting;
    publications.push(end_metric(metric_start, stats, path, physical, accounting));
    let delete_reinsert = inspect_variant(&tree, "delete_reinsert", delete_reinsert_commit).await;

    tree.create_branch("small_diff", Some(sorted.commit))
        .await
        .expect("create changed branch");
    let changed_index = rows / 2;
    let metric_start = begin_metric("small_diff", stats, path, physical);
    let changed_value = format!("changed-{changed_index:08}").into_bytes();
    let (changed_commit, accounting) = tree
        .apply_sorted_mutations_on(
            "small_diff",
            &[Mutation::Update {
                key: expected[changed_index + 1].0.clone(),
                value: RelationalValue::Bytes(changed_value),
            }],
        )
        .await
        .expect("publish one-row diff");
    publications.push(end_metric(metric_start, stats, path, physical, accounting));
    let changed = inspect_variant(&tree, "small_diff", changed_commit).await;

    let equal = vec![sorted, random, reverse, adversarial, delete_reinsert];
    for variant in &equal {
        let rows = tree
            .read_range(variant.name, &[], &[0xff])
            .await
            .expect("read equal variant");
        assert_eq!(rows, expected, "{} logical state", variant.name);
    }
    let bulk_rows = tree
        .read_range("main", &[], &[0xff])
        .await
        .expect("read bulk state");
    assert_eq!(bulk_rows, expected);

    PreparedGate {
        bulk,
        equal,
        changed,
        publications,
        expected,
    }
}

#[allow(clippy::too_many_arguments)]
async fn build_insert_history<S>(
    tree: &ForkTree<CountingStorage<S>>,
    stats: &Arc<Mutex<IoStats>>,
    path: &Path,
    physical: Option<&SlateDBIoCounters>,
    name: &'static str,
    seed: ObjectId,
    rows: &[(Vec<u8>, Vec<u8>)],
    order: &[usize],
    batch_size: usize,
) -> (Variant, PublicationMetric)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tree.create_branch(name, Some(seed))
        .await
        .expect("create history branch");
    let metric_start = begin_metric(name, stats, path, physical);
    let mut accounting = ApplyAccounting::default();
    let mut commit = seed;
    for batch in order.chunks(batch_size) {
        let mut mutations = batch
            .iter()
            .map(|index| Mutation::Insert {
                key: rows[*index].0.clone(),
                value: RelationalValue::Bytes(rows[*index].1.clone()),
            })
            .collect::<Vec<_>>();
        mutations.sort_by(|left, right| left.key().cmp(right.key()));
        let (next, observed) = tree
            .apply_sorted_mutations_on(name, &mutations)
            .await
            .expect("apply history batch");
        commit = next;
        accounting += observed;
    }
    let metric = end_metric(metric_start, stats, path, physical, accounting);
    (inspect_variant(tree, name, commit).await, metric)
}

async fn inspect_variant<S>(
    tree: &ForkTree<CountingStorage<S>>,
    name: &'static str,
    commit: ObjectId,
) -> Variant
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Variant {
        name,
        commit,
        inspection: tree.inspect_state(commit).await.expect("inspect state"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn finish_gate<S>(
    backend: &str,
    tree: ForkTree<CountingStorage<S>>,
    stats: &Arc<Mutex<IoStats>>,
    path: &Path,
    physical: Option<&SlateDBIoCounters>,
    rows: usize,
    gate: PreparedGate,
    post_flush_disk: u64,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    for metric in &gate.publications {
        print_publication(backend, rows, metric);
    }
    println!(
        "history_settled,backend={backend},rows={rows},post_flush_disk_bytes={post_flush_disk},rss_bytes={}",
        process_resident_bytes()
    );

    for variant in &gate.equal {
        let comparison = compare_shapes(&gate.bulk.inspection, &variant.inspection);
        println!(
            "history_shape,backend={backend},rows={rows},left={},right={},root_equal={},leaf_boundaries_equal={},internal_boundaries_equal={},left_state_objects={},right_state_objects={},shared_objects={},shared_bytes={},shared_pct={:.4},sync_bytes_left_to_right={},sync_bytes_right_to_left={}",
            gate.bulk.name,
            variant.name,
            gate.bulk.inspection.root == variant.inspection.root,
            gate.bulk.inspection.leaf_ranges == variant.inspection.leaf_ranges,
            gate.bulk.inspection.internal_boundaries == variant.inspection.internal_boundaries,
            gate.bulk.inspection.object_bytes.len(),
            variant.inspection.object_bytes.len(),
            comparison.shared_objects,
            comparison.shared_bytes,
            comparison.shared_pct,
            comparison.left_to_right,
            comparison.right_to_left,
        );
        measure_diff(
            backend, rows, &tree, stats, path, physical, &gate.bulk, variant, 0,
        )
        .await;
    }
    let comparison = compare_shapes(&gate.bulk.inspection, &gate.changed.inspection);
    println!(
        "history_shape,backend={backend},rows={rows},left={},right={},root_equal={},leaf_boundaries_equal={},internal_boundaries_equal={},left_state_objects={},right_state_objects={},shared_objects={},shared_bytes={},shared_pct={:.4},sync_bytes_left_to_right={},sync_bytes_right_to_left={}",
        gate.bulk.name,
        gate.changed.name,
        gate.bulk.inspection.root == gate.changed.inspection.root,
        gate.bulk.inspection.leaf_ranges == gate.changed.inspection.leaf_ranges,
        gate.bulk.inspection.internal_boundaries == gate.changed.inspection.internal_boundaries,
        gate.bulk.inspection.object_bytes.len(),
        gate.changed.inspection.object_bytes.len(),
        comparison.shared_objects,
        comparison.shared_bytes,
        comparison.shared_pct,
        comparison.left_to_right,
        comparison.right_to_left,
    );
    measure_diff(
        backend,
        rows,
        &tree,
        stats,
        path,
        physical,
        &gate.bulk,
        &gate.changed,
        1,
    )
    .await;

    let ordinary_parent = gate
        .equal
        .iter()
        .find(|variant| variant.name == "sorted_insert")
        .expect("sorted insertion history");
    let comparison = compare_shapes(&ordinary_parent.inspection, &gate.changed.inspection);
    println!(
        "history_shape,backend={backend},rows={rows},left={},right={},root_equal={},leaf_boundaries_equal={},internal_boundaries_equal={},left_state_objects={},right_state_objects={},shared_objects={},shared_bytes={},shared_pct={:.4},sync_bytes_left_to_right={},sync_bytes_right_to_left={}",
        ordinary_parent.name,
        gate.changed.name,
        ordinary_parent.inspection.root == gate.changed.inspection.root,
        ordinary_parent.inspection.leaf_ranges == gate.changed.inspection.leaf_ranges,
        ordinary_parent.inspection.internal_boundaries
            == gate.changed.inspection.internal_boundaries,
        ordinary_parent.inspection.object_bytes.len(),
        gate.changed.inspection.object_bytes.len(),
        comparison.shared_objects,
        comparison.shared_bytes,
        comparison.shared_pct,
        comparison.left_to_right,
        comparison.right_to_left,
    );
    measure_diff(
        backend,
        rows,
        &tree,
        stats,
        path,
        physical,
        ordinary_parent,
        &gate.changed,
        1,
    )
    .await;
    assert_eq!(gate.expected.len(), rows + 1);
}

#[derive(Clone, Copy)]
struct ShapeComparison {
    shared_objects: u64,
    shared_bytes: u64,
    shared_pct: f64,
    left_to_right: u64,
    right_to_left: u64,
}

fn compare_shapes(left: &StateInspection, right: &StateInspection) -> ShapeComparison {
    let shared = left
        .object_bytes
        .iter()
        .filter(|(id, _)| right.object_bytes.contains_key(id))
        .collect::<Vec<_>>();
    let shared_bytes = shared.iter().map(|(_, bytes)| **bytes).sum::<u64>();
    let left_bytes = left.object_bytes.values().sum::<u64>();
    let right_bytes = right.object_bytes.values().sum::<u64>();
    ShapeComparison {
        shared_objects: shared.len() as u64,
        shared_bytes,
        shared_pct: if left_bytes.max(right_bytes) == 0 {
            100.0
        } else {
            100.0 * shared_bytes as f64 / left_bytes.max(right_bytes) as f64
        },
        left_to_right: right
            .object_bytes
            .iter()
            .filter(|(id, _)| !left.object_bytes.contains_key(id))
            .map(|(_, bytes)| *bytes)
            .sum(),
        right_to_left: left
            .object_bytes
            .iter()
            .filter(|(id, _)| !right.object_bytes.contains_key(id))
            .map(|(_, bytes)| *bytes)
            .sum(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn measure_diff<S>(
    backend: &str,
    rows: usize,
    tree: &ForkTree<CountingStorage<S>>,
    stats: &Arc<Mutex<IoStats>>,
    path: &Path,
    physical: Option<&SlateDBIoCounters>,
    left: &Variant,
    right: &Variant,
    expected_changes: usize,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let _ = take_stats(stats);
    let physical_before = physical.map(SlateDBIoCounters::snapshot);
    let disk_before = directory_bytes(path);
    let rss_before = process_resident_bytes();
    let cpu_before = process_cpu_nanos();
    begin_allocations();
    let started = Instant::now();
    let changes = tree
        .diff_commits(left.commit, right.commit)
        .await
        .expect("cold hash-pruned diff");
    let wall_us = started.elapsed().as_micros() as u64;
    let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) / 1_000;
    let (allocated_bytes, allocation_calls) = end_allocations();
    let rss_after = process_resident_bytes();
    let disk_after = directory_bytes(path);
    let io = take_stats(stats);
    let physical = physical_delta(physical, physical_before);
    assert_eq!(changes.len(), expected_changes);
    println!(
        "history_diff,backend={backend},rows={rows},left={},right={},changes={},wall_us={wall_us},cpu_us={cpu_us},alloc_bytes={allocated_bytes},alloc_calls={allocation_calls},rss_before={rss_before},rss_after={rss_after},disk_before={disk_before},disk_after={disk_after},begin_reads={},get_calls={},get_keys={},get_values={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},physical_read_objects={},physical_read_bytes={}",
        left.name,
        right.name,
        changes.len(),
        io.begin_reads,
        io.get_calls,
        io.get_keys,
        io.get_values,
        io.get_value_bytes,
        io.scan_calls,
        io.scan_entries,
        io.scan_value_bytes,
        physical.read_objects,
        physical.read_bytes,
    );
}

struct MetricStart {
    name: &'static str,
    started: Instant,
    cpu_before: u64,
    rss_before: u64,
    disk_before: u64,
    physical_before: Option<SlateDBIoSnapshot>,
}

fn begin_metric(
    name: &'static str,
    stats: &Arc<Mutex<IoStats>>,
    path: &Path,
    physical: Option<&SlateDBIoCounters>,
) -> MetricStart {
    let _ = take_stats(stats);
    begin_allocations();
    MetricStart {
        name,
        started: Instant::now(),
        cpu_before: process_cpu_nanos(),
        rss_before: process_resident_bytes(),
        disk_before: directory_bytes(path),
        physical_before: physical.map(SlateDBIoCounters::snapshot),
    }
}

fn end_metric(
    start: MetricStart,
    stats: &Arc<Mutex<IoStats>>,
    path: &Path,
    physical: Option<&SlateDBIoCounters>,
    accounting: ApplyAccounting,
) -> PublicationMetric {
    let wall_us = start.started.elapsed().as_micros() as u64;
    let cpu_us = process_cpu_nanos().saturating_sub(start.cpu_before) / 1_000;
    let (allocated_bytes, allocation_calls) = end_allocations();
    PublicationMetric {
        name: start.name,
        wall_us,
        cpu_us,
        allocated_bytes,
        allocation_calls,
        rss_before: start.rss_before,
        rss_after: process_resident_bytes(),
        disk_before: start.disk_before,
        disk_after: directory_bytes(path),
        io: take_stats(stats),
        physical: physical_delta(physical, start.physical_before),
        accounting,
    }
}

fn print_publication(backend: &str, rows: usize, metric: &PublicationMetric) {
    println!(
        "history_publication,backend={backend},rows={rows},history={},wall_us={},cpu_us={},alloc_bytes={},alloc_calls={},rss_before={},rss_after={},disk_before={},disk_after={},begin_reads={},begin_writes={},get_calls={},get_keys={},get_values={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},write_batches={},write_puts={},write_deletes={},write_bytes={},commits={},physical_read_objects={},physical_read_bytes={},physical_write_objects={},physical_write_bytes={},object_writes={},object_bytes={},node_writes={},node_bytes={},leaf_writes={},leaf_bytes={},internal_writes={},internal_bytes={},reused_objects={}",
        metric.name,
        metric.wall_us,
        metric.cpu_us,
        metric.allocated_bytes,
        metric.allocation_calls,
        metric.rss_before,
        metric.rss_after,
        metric.disk_before,
        metric.disk_after,
        metric.io.begin_reads,
        metric.io.begin_writes,
        metric.io.get_calls,
        metric.io.get_keys,
        metric.io.get_values,
        metric.io.get_value_bytes,
        metric.io.scan_calls,
        metric.io.scan_entries,
        metric.io.scan_value_bytes,
        metric.io.write_batches,
        metric.io.write_puts,
        metric.io.write_deletes,
        metric.io.write_bytes,
        metric.io.commits,
        metric.physical.read_objects,
        metric.physical.read_bytes,
        metric.physical.write_objects,
        metric.physical.write_bytes,
        metric.accounting.object_writes,
        metric.accounting.object_bytes,
        metric.accounting.node_writes,
        metric.accounting.node_bytes,
        metric.accounting.leaf_writes,
        metric.accounting.leaf_bytes,
        metric.accounting.internal_writes,
        metric.accounting.internal_bytes,
        metric.accounting.reused_objects,
    );
}

fn fixture(rows: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut output = Vec::with_capacity(rows + 1);
    output.push((b"meta/seed".to_vec(), b"seed".to_vec()));
    output.extend((0..rows).map(|index| {
        (
            format!("row/{index:08}").into_bytes(),
            format!("value-{index:08}-{}", "compressible".repeat(4)).into_bytes(),
        )
    }));
    output
}

fn deterministic_shuffle(values: &mut [usize], mut state: u64) {
    for index in (1..values.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        values.swap(index, state as usize % (index + 1));
    }
}

fn split_boundary_order(rows: usize) -> Vec<usize> {
    let mut selected = vec![false; rows];
    let mut output = Vec::with_capacity(rows);
    for boundary in (64..rows).step_by(64) {
        for offset in [0_isize, -1, 1, -2, 2, -3, 3] {
            let candidate = boundary as isize + offset;
            if candidate >= 0 {
                let candidate = candidate as usize;
                if candidate < rows && !selected[candidate] {
                    selected[candidate] = true;
                    output.push(candidate);
                }
            }
        }
    }
    let mut low = 0;
    let mut high = rows.saturating_sub(1);
    while output.len() < rows {
        if low < rows && !selected[low] {
            selected[low] = true;
            output.push(low);
        }
        if high < rows && !selected[high] {
            selected[high] = true;
            output.push(high);
        }
        low = low.saturating_add(1);
        high = high.saturating_sub(1);
    }
    output
}

fn take_stats(stats: &Arc<Mutex<IoStats>>) -> IoStats {
    std::mem::take(&mut *stats.lock().expect("I/O stats"))
}

fn projected_len(value: &ProjectedValue) -> usize {
    match value {
        ProjectedValue::KeyOnly => 0,
        ProjectedValue::FullValue(bytes) => bytes.len(),
    }
}

fn physical_delta(
    counters: Option<&SlateDBIoCounters>,
    before: Option<SlateDBIoSnapshot>,
) -> SlateDBIoSnapshot {
    match (counters, before) {
        (Some(counters), Some(before)) => counters.snapshot().saturating_sub(before),
        _ => SlateDBIoSnapshot::default(),
    }
}

fn process_resident_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmRSS:"))
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .map_or(0, |kilobytes| kilobytes.saturating_mul(1_024))
}

fn process_cpu_nanos() -> u64 {
    let mut value = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: a successful clock_gettime initializes both timespec fields.
    let status = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, value.as_mut_ptr()) };
    if status != 0 {
        return 0;
    }
    // SAFETY: the successful call above initialized `value`.
    let value = unsafe { value.assume_init() };
    (value.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(value.tv_nsec as u64)
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries.flatten().fold(0_u64, |total, entry| {
        let path = entry.path();
        let bytes = if path.is_dir() {
            directory_bytes(&path)
        } else {
            entry.metadata().map(|metadata| metadata.len()).unwrap_or(0)
        };
        total.saturating_add(bytes)
    })
}
