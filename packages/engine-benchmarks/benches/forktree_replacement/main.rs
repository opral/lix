#![allow(clippy::large_futures)]

mod model;
mod workload;

use std::alloc::GlobalAlloc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use lix::integration::{Engine, SessionContext};
use lix::storage::{
    CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, ProjectedValue, PutBatch,
    ReadOptions, ScanChunk, ScanOptions, Storage, StorageError, StorageRead, StorageWrite,
    WriteOptions,
};
use lix::{PreparedDmlParameterBatch, Value};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

use model::{ApplyAccounting, ForkTree, Update};
use workload::{WorkloadRow, fixture_rows};

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;

static PROFILE_ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static PROFILE_ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static PROFILE_ALLOCATION_ENABLED: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && PROFILE_ALLOCATION_ENABLED.load(Ordering::Relaxed) {
            PROFILE_ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            PROFILE_ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }

    unsafe fn realloc(
        &self,
        pointer: *mut u8,
        layout: std::alloc::Layout,
        new_size: usize,
    ) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, new_size) };
        if !replacement.is_null()
            && new_size >= layout.size()
            && PROFILE_ALLOCATION_ENABLED.load(Ordering::Relaxed)
        {
            PROFILE_ALLOCATED_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            PROFILE_ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        replacement
    }
}

fn begin_allocation_profile() {
    PROFILE_ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    PROFILE_ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    PROFILE_ALLOCATION_ENABLED.store(true, Ordering::Relaxed);
}

fn end_allocation_profile() -> (u64, u64) {
    PROFILE_ALLOCATION_ENABLED.store(false, Ordering::Relaxed);
    (
        PROFILE_ALLOCATED_BYTES.load(Ordering::Relaxed),
        PROFILE_ALLOCATION_CALLS.load(Ordering::Relaxed),
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
    write_ranges: u64,
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

impl<S> Storage for CountingStorage<S>
where
    S: Storage,
{
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

impl<R> StorageRead for CountingRead<R>
where
    R: StorageRead,
{
    fn snapshot_cache_key(&self) -> Option<u128> {
        self.inner.snapshot_cache_key()
    }

    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.get_calls += 1;
            stats.get_keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
        }
        let result = self.inner.get_many(requests).await?;
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            for value in result.values.iter().flatten() {
                stats.get_values += 1;
                stats.get_value_bytes += projected_value_len(value) as u64;
            }
        }
        Ok(result)
    }

    async fn scan(
        &self,
        space: lix::storage::StorageSpace,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        self.stats.lock().expect("I/O stats mutex").scan_calls += 1;
        let chunk = self.inner.scan(space, range, options).await?;
        let mut stats = self.stats.lock().expect("I/O stats mutex");
        stats.scan_entries += chunk.entries.len() as u64;
        stats.scan_value_bytes += chunk
            .entries
            .iter()
            .map(|entry| projected_value_len(&entry.value) as u64)
            .sum::<u64>();
        drop(stats);
        Ok(chunk)
    }
}

impl<W> StorageWrite for CountingWrite<W>
where
    W: StorageWrite,
{
    async fn put_many(
        &mut self,
        space: lix::storage::StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
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
            let mut stats = self.stats.lock().expect("I/O stats mutex");
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
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.write_batches += 1;
            stats.write_ranges += 1;
        }
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        self.stats.lock().expect("I/O stats mutex").commits += 1;
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Backend {
    RocksDb,
    SlateDb,
}

impl Backend {
    fn parse(value: &str) -> Self {
        match value {
            "rocksdb" => Self::RocksDb,
            "slatedb" => Self::SlateDb,
            other => panic!("unknown backend '{other}'"),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::RocksDb => "rocksdb",
            Self::SlateDb => "slatedb",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Layout {
    Current,
    ForkTree,
}

impl Layout {
    fn parse(value: &str) -> Self {
        match value {
            "current" => Self::Current,
            "forktree" => Self::ForkTree,
            other => panic!("unknown layout '{other}'"),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Current => "current_lix",
            Self::ForkTree => "forktree",
        }
    }
}

#[derive(Clone, Copy)]
struct Parameters {
    backend: Backend,
    layout: Layout,
    rows: usize,
    updates: usize,
    samples: usize,
    warmups: usize,
    iterations: usize,
}

impl Parameters {
    fn parse() -> Self {
        let args = std::env::args().collect::<Vec<_>>();
        let rows = parse_positive(args.get(3), "rows", 1_000);
        let updates = parse_positive(args.get(4), "updates", 32);
        assert!(updates <= rows, "updates must not exceed rows");
        Self {
            backend: Backend::parse(args.get(1).map(String::as_str).unwrap_or("rocksdb")),
            layout: Layout::parse(args.get(2).map(String::as_str).unwrap_or("forktree")),
            rows,
            updates,
            samples: parse_positive(args.get(5), "samples", 7),
            warmups: parse_nonnegative(args.get(6), "warmups", 2),
            iterations: parse_positive(args.get(7), "iterations", 20),
        }
    }
}

struct CurrentFixture<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session: SessionContext<CountingStorage<S>>,
    rows: Vec<WorkloadRow>,
    selected: Vec<usize>,
    batches: [PreparedDmlParameterBatch; 2],
    updated: bool,
}

struct ReplacementFixture<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tree: ForkTree<CountingStorage<S>>,
    rows: Vec<WorkloadRow>,
    selected: Vec<usize>,
    updates: [Vec<Update>; 2],
    updated: bool,
}

#[derive(Clone, Copy, Debug)]
struct Sample {
    wall_us: f64,
    cpu_us: f64,
    allocated_bytes: f64,
    allocation_calls: f64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let parameters = Parameters::parse();
    match parameters.backend {
        Backend::RocksDb => run_rocksdb(parameters).await,
        Backend::SlateDb => run_slatedb(parameters).await,
    }
}

async fn run_rocksdb(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create RocksDB ForkTree directory");
    let database = RocksDB::open(directory.path()).expect("open RocksDB ForkTree storage");
    let (storage, stats) = CountingStorage::new(database.clone());
    match parameters.layout {
        Layout::Current => {
            let fixture = prepare_current(storage, parameters).await;
            database.flush().expect("flush current Lix RocksDB setup");
            measure_current(fixture, parameters, &stats, directory.path(), None).await;
        }
        Layout::ForkTree => {
            let fixture = prepare_replacement(storage, parameters).await;
            database.flush().expect("flush ForkTree RocksDB setup");
            measure_replacement(fixture, parameters, &stats, directory.path(), None).await;
        }
    }
    database.flush().expect("flush RocksDB final state");
    println!(
        "forktree_lifecycle,backend=rocksdb,layout={},rows={},updates={},post_flush_disk_bytes={}",
        parameters.layout.label(),
        parameters.rows,
        parameters.updates,
        directory_bytes(directory.path()),
    );
}

async fn run_slatedb(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create SlateDB ForkTree directory");
    let counters = SlateDBIoCounters::default();
    let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
        .expect("open SlateDB ForkTree storage");
    let (storage, stats) = CountingStorage::new(database.clone());
    match parameters.layout {
        Layout::Current => {
            let fixture = prepare_current(storage, parameters).await;
            database
                .flush_memtable_for_diagnostics()
                .await
                .expect("flush current Lix SlateDB setup");
            measure_current(
                fixture,
                parameters,
                &stats,
                directory.path(),
                Some(&counters),
            )
            .await;
        }
        Layout::ForkTree => {
            let fixture = prepare_replacement(storage, parameters).await;
            database
                .flush_memtable_for_diagnostics()
                .await
                .expect("flush ForkTree SlateDB setup");
            measure_replacement(
                fixture,
                parameters,
                &stats,
                directory.path(),
                Some(&counters),
            )
            .await;
        }
    }
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush SlateDB final state");
    println!(
        "forktree_lifecycle,backend=slatedb,layout={},rows={},updates={},post_flush_disk_bytes={}",
        parameters.layout.label(),
        parameters.rows,
        parameters.updates,
        directory_bytes(directory.path()),
    );
}

async fn prepare_current<S>(
    storage: CountingStorage<S>,
    parameters: Parameters,
) -> CurrentFixture<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let setup_started = Instant::now();
    let rows = fixture_rows(parameters.rows);
    let selected = selected_indices(parameters.rows, parameters.updates);
    Engine::initialize(storage.clone())
        .await
        .expect("initialize current Lix fixture");
    let engine = Engine::new(storage)
        .await
        .expect("open current Lix fixture");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open current Lix workspace session");
    register_current_schema(&session).await;
    let seed = PreparedDmlParameterBatch::from_rows(rows.iter().map(|row| {
        vec![
            Value::Text(row.path.clone()),
            Value::Text(row.value_json.clone()),
        ]
    }))
    .expect("build current Lix seed batch");
    let affected = session
        .execute_prepared_dml_batch(
            Arc::from("INSERT INTO forktree_row (path, value) VALUES ($1, $2)"),
            seed,
        )
        .await
        .expect("seed current Lix rows")
        .iter()
        .map(lix::ExecuteResult::rows_affected)
        .sum::<u64>();
    assert_eq!(affected, parameters.rows as u64);
    let batches = [
        update_batch(&rows, &selected, false),
        update_batch(&rows, &selected, true),
    ];
    println!(
        "forktree_setup,backend={},layout=current_lix,rows={},updates={},wall_ms={:.3}",
        parameters.backend.label(),
        parameters.rows,
        parameters.updates,
        setup_started.elapsed().as_secs_f64() * 1_000.0,
    );
    CurrentFixture {
        session,
        rows,
        selected,
        batches,
        updated: false,
    }
}

async fn prepare_replacement<S>(
    storage: CountingStorage<S>,
    parameters: Parameters,
) -> ReplacementFixture<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let setup_started = Instant::now();
    let rows = fixture_rows(parameters.rows);
    let selected = selected_indices(parameters.rows, parameters.updates);
    let tree = ForkTree::new(storage);
    let initial = rows
        .iter()
        .map(|row| {
            (
                row.path.as_bytes().to_vec(),
                row.value_json.as_bytes().to_vec(),
            )
        })
        .collect::<Vec<_>>();
    tree.initialize(&initial)
        .await
        .expect("initialize ForkTree fixture");
    let updates = [
        replacement_updates(&rows, &selected, false),
        replacement_updates(&rows, &selected, true),
    ];
    let (objects, bytes) = tree
        .object_inventory()
        .await
        .expect("inventory ForkTree setup");
    println!(
        "forktree_setup,backend={},layout=forktree,rows={},updates={},wall_ms={:.3},objects={},object_bytes={}",
        parameters.backend.label(),
        parameters.rows,
        parameters.updates,
        setup_started.elapsed().as_secs_f64() * 1_000.0,
        objects,
        bytes,
    );
    ReplacementFixture {
        tree,
        rows,
        selected,
        updates,
        updated: false,
    }
}

async fn register_current_schema<S>(session: &SessionContext<CountingStorage<S>>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "x-lix-key": "forktree_row",
        "x-lix-primary-key": ["/path"],
        "type": "object",
        "required": ["path", "value"],
        "properties": {
            "path": { "type": "string" },
            "value": { "type": "string" }
        },
        "additionalProperties": false
    });
    let affected = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register ForkTree comparison schema")
        .rows_affected();
    assert_eq!(affected, 1);
}

async fn measure_current<S>(
    mut fixture: CurrentFixture<S>,
    parameters: Parameters,
    stats: &Arc<Mutex<IoStats>>,
    database_path: &std::path::Path,
    physical_counters: Option<&SlateDBIoCounters>,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let _ = take_stats(stats);
    let _ = lix::storage_bench::take_crud_physical_write_accounting();
    for _ in 0..parameters.warmups {
        apply_current(&mut fixture).await;
    }
    verify_current(&fixture).await;
    let _ = take_stats(stats);
    let _ = lix::storage_bench::take_crud_physical_write_accounting();
    let mut samples = Vec::with_capacity(parameters.samples);
    for sample in 1..=parameters.samples {
        let physical_before = physical_counters.map(SlateDBIoCounters::snapshot);
        let disk_before = directory_bytes(database_path);
        let rss_before = process_resident_bytes();
        let cpu_before = process_cpu_ticks();
        begin_allocation_profile();
        let started = Instant::now();
        for _ in 0..parameters.iterations {
            apply_current(&mut fixture).await;
        }
        let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
        let (allocated_bytes, allocation_calls) = end_allocation_profile();
        let cpu_us = cpu_ticks_to_us(process_cpu_ticks().saturating_sub(cpu_before));
        let rss_after = process_resident_bytes();
        let disk_after = directory_bytes(database_path);
        let io = take_stats(stats);
        let physical = physical_delta(physical_counters, physical_before);
        let current_physical = lix::storage_bench::take_crud_physical_write_accounting();
        let iterations = parameters.iterations as f64;
        let observed = Sample {
            wall_us: wall_us / iterations,
            cpu_us: cpu_us / iterations,
            allocated_bytes: allocated_bytes as f64 / iterations,
            allocation_calls: allocation_calls as f64 / iterations,
        };
        print_sample(
            sample,
            parameters,
            observed,
            &io,
            physical,
            ApplyAccounting {
                object_writes: current_physical.puts,
                object_bytes: current_physical.written_bytes,
                logical_bytes: logical_update_bytes(&fixture.rows, &fixture.selected),
                ..ApplyAccounting::default()
            },
            rss_before,
            rss_after,
            disk_before,
            disk_after,
        );
        samples.push(observed);
        verify_current(&fixture).await;
        let _ = take_stats(stats);
    }
    print_summary(parameters, &mut samples);
}

async fn measure_replacement<S>(
    mut fixture: ReplacementFixture<S>,
    parameters: Parameters,
    stats: &Arc<Mutex<IoStats>>,
    database_path: &std::path::Path,
    physical_counters: Option<&SlateDBIoCounters>,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let _ = take_stats(stats);
    for _ in 0..parameters.warmups {
        let _ = apply_replacement(&mut fixture).await;
    }
    verify_replacement(&fixture).await;
    let _ = take_stats(stats);
    let mut samples = Vec::with_capacity(parameters.samples);
    for sample in 1..=parameters.samples {
        let physical_before = physical_counters.map(SlateDBIoCounters::snapshot);
        let disk_before = directory_bytes(database_path);
        let rss_before = process_resident_bytes();
        let cpu_before = process_cpu_ticks();
        begin_allocation_profile();
        let started = Instant::now();
        let mut accounting = ApplyAccounting::default();
        for _ in 0..parameters.iterations {
            accounting += apply_replacement(&mut fixture).await;
        }
        let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
        let (allocated_bytes, allocation_calls) = end_allocation_profile();
        let cpu_us = cpu_ticks_to_us(process_cpu_ticks().saturating_sub(cpu_before));
        let rss_after = process_resident_bytes();
        let disk_after = directory_bytes(database_path);
        let io = take_stats(stats);
        let physical = physical_delta(physical_counters, physical_before);
        let iterations = parameters.iterations as f64;
        let observed = Sample {
            wall_us: wall_us / iterations,
            cpu_us: cpu_us / iterations,
            allocated_bytes: allocated_bytes as f64 / iterations,
            allocation_calls: allocation_calls as f64 / iterations,
        };
        print_sample(
            sample,
            parameters,
            observed,
            &io,
            physical,
            accounting,
            rss_before,
            rss_after,
            disk_before,
            disk_after,
        );
        samples.push(observed);
        verify_replacement(&fixture).await;
        let _ = take_stats(stats);
    }
    let (objects, bytes) = fixture
        .tree
        .object_inventory()
        .await
        .expect("inventory measured ForkTree");
    println!(
        "forktree_inventory,backend={},layout=forktree,rows={},updates={},objects={},object_bytes={}",
        parameters.backend.label(),
        parameters.rows,
        parameters.updates,
        objects,
        bytes,
    );
    print_summary(parameters, &mut samples);
}

async fn apply_current<S>(fixture: &mut CurrentFixture<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fixture.updated = !fixture.updated;
    let batch = fixture.batches[usize::from(fixture.updated)].clone();
    let affected = fixture
        .session
        .execute_prepared_dml_batch(
            Arc::from("UPDATE forktree_row SET value = $1 WHERE path = $2"),
            batch,
        )
        .await
        .expect("apply current Lix update batch")
        .iter()
        .map(lix::ExecuteResult::rows_affected)
        .sum::<u64>();
    assert_eq!(affected, fixture.selected.len() as u64);
}

async fn apply_replacement<S>(fixture: &mut ReplacementFixture<S>) -> ApplyAccounting
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fixture.updated = !fixture.updated;
    fixture
        .tree
        .apply_sorted_updates(&fixture.updates[usize::from(fixture.updated)])
        .await
        .expect("apply ForkTree update batch")
        .1
}

async fn verify_current<S>(fixture: &CurrentFixture<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = fixture
        .session
        .execute("SELECT path, value FROM forktree_row ORDER BY path", &[])
        .await
        .expect("read current Lix comparison rows");
    assert_eq!(result.len(), fixture.rows.len());
    let mut selected = fixture.selected.iter().copied().peekable();
    for (index, (actual, expected)) in result.rows().iter().zip(&fixture.rows).enumerate() {
        assert_eq!(
            actual.get_index(0),
            Some(&Value::Text(expected.path.clone()))
        );
        let is_selected = selected.peek() == Some(&index);
        if is_selected {
            selected.next();
        }
        let expected_value = if is_selected && fixture.updated {
            &expected.updated_value_json
        } else {
            &expected.value_json
        };
        assert_eq!(
            actual.get_index(1),
            Some(&Value::Text(expected_value.clone()))
        );
    }
}

async fn verify_replacement<S>(fixture: &ReplacementFixture<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let actual = fixture
        .tree
        .read_all()
        .await
        .expect("read ForkTree comparison rows");
    assert_eq!(actual.len(), fixture.rows.len());
    let mut selected = fixture.selected.iter().copied().peekable();
    for (index, ((key, value), expected)) in actual.iter().zip(&fixture.rows).enumerate() {
        assert_eq!(key.as_slice(), expected.path.as_bytes());
        let is_selected = selected.peek() == Some(&index);
        if is_selected {
            selected.next();
        }
        let expected_value = if is_selected && fixture.updated {
            &expected.updated_value_json
        } else {
            &expected.value_json
        };
        assert_eq!(value.as_slice(), expected_value.as_bytes());
    }
}

fn update_batch(
    rows: &[WorkloadRow],
    selected: &[usize],
    updated: bool,
) -> PreparedDmlParameterBatch {
    PreparedDmlParameterBatch::from_rows(selected.iter().map(|&index| {
        let row = &rows[index];
        vec![
            Value::Text(if updated {
                row.updated_value_json.clone()
            } else {
                row.value_json.clone()
            }),
            Value::Text(row.path.clone()),
        ]
    }))
    .expect("build current Lix update batch")
}

fn replacement_updates(rows: &[WorkloadRow], selected: &[usize], updated: bool) -> Vec<Update> {
    selected
        .iter()
        .map(|&index| {
            let row = &rows[index];
            Update {
                key: row.path.as_bytes().to_vec(),
                value: if updated {
                    row.updated_value_json.as_bytes().to_vec()
                } else {
                    row.value_json.as_bytes().to_vec()
                },
            }
        })
        .collect()
}

fn selected_indices(rows: usize, updates: usize) -> Vec<usize> {
    let selected = (0..updates)
        .map(|index| (index + 1) * rows / (updates + 1))
        .collect::<Vec<_>>();
    assert!(
        selected.windows(2).all(|pair| pair[0] < pair[1]),
        "focused update selection must be unique"
    );
    selected
}

fn logical_update_bytes(rows: &[WorkloadRow], selected: &[usize]) -> u64 {
    selected
        .iter()
        .map(|&index| {
            let row = &rows[index];
            (row.path.len() + row.updated_value_json.len()) as u64
        })
        .sum()
}

#[allow(clippy::too_many_arguments)]
fn print_sample(
    sample: usize,
    parameters: Parameters,
    observed: Sample,
    io: &IoStats,
    physical: SlateDBIoSnapshot,
    accounting: ApplyAccounting,
    rss_before: u64,
    rss_after: u64,
    disk_before: u64,
    disk_after: u64,
) {
    let iterations = parameters.iterations as f64;
    println!(
        "forktree_gate,sample={sample},backend={},layout={},rows={},updates={},iterations={},wall_us_per_op={:.3},cpu_us_per_op={:.3},alloc_bytes_per_op={:.1},alloc_calls_per_op={:.1},rss_before_bytes={rss_before},rss_after_bytes={rss_after},begin_reads_per_op={:.2},begin_writes_per_op={:.2},get_calls_per_op={:.2},get_keys_per_op={:.2},get_values_per_op={:.2},get_value_bytes_per_op={:.1},scan_calls_per_op={:.2},scan_entries_per_op={:.2},scan_value_bytes_per_op={:.1},write_batches_per_op={:.2},write_puts_per_op={:.2},write_deletes_per_op={:.2},write_bytes_per_op={:.1},commits_per_op={:.2},logical_bytes_per_op={:.1},object_writes_per_op={:.2},object_bytes_per_op={:.1},node_writes_per_op={:.2},node_bytes_per_op={:.1},reused_objects_per_op={:.2},disk_before_bytes={disk_before},disk_after_bytes={disk_after},slate_read_objects_per_op={:.2},slate_read_bytes_per_op={:.1},slate_write_objects_per_op={:.2},slate_write_bytes_per_op={:.1}",
        parameters.backend.label(),
        parameters.layout.label(),
        parameters.rows,
        parameters.updates,
        parameters.iterations,
        observed.wall_us,
        observed.cpu_us,
        observed.allocated_bytes,
        observed.allocation_calls,
        io.begin_reads as f64 / iterations,
        io.begin_writes as f64 / iterations,
        io.get_calls as f64 / iterations,
        io.get_keys as f64 / iterations,
        io.get_values as f64 / iterations,
        io.get_value_bytes as f64 / iterations,
        io.scan_calls as f64 / iterations,
        io.scan_entries as f64 / iterations,
        io.scan_value_bytes as f64 / iterations,
        io.write_batches as f64 / iterations,
        io.write_puts as f64 / iterations,
        io.write_deletes as f64 / iterations,
        io.write_bytes as f64 / iterations,
        io.commits as f64 / iterations,
        accounting.logical_bytes as f64 / iterations,
        accounting.object_writes as f64 / iterations,
        accounting.object_bytes as f64 / iterations,
        accounting.node_writes as f64 / iterations,
        accounting.node_bytes as f64 / iterations,
        accounting.reused_objects as f64 / iterations,
        physical.read_objects as f64 / iterations,
        physical.read_bytes as f64 / iterations,
        physical.write_objects as f64 / iterations,
        physical.write_bytes as f64 / iterations,
    );
}

fn print_summary(parameters: Parameters, samples: &mut [Sample]) {
    samples.sort_by(|left, right| left.wall_us.total_cmp(&right.wall_us));
    let wall = samples[samples.len() / 2];
    samples.sort_by(|left, right| left.cpu_us.total_cmp(&right.cpu_us));
    let cpu = samples[samples.len() / 2];
    samples.sort_by(|left, right| left.allocated_bytes.total_cmp(&right.allocated_bytes));
    let allocation = samples[samples.len() / 2];
    println!(
        "forktree_summary,backend={},layout={},rows={},updates={},samples={},iterations={},median_wall_us={:.3},median_cpu_us={:.3},median_alloc_bytes={:.1},median_alloc_calls={:.1}",
        parameters.backend.label(),
        parameters.layout.label(),
        parameters.rows,
        parameters.updates,
        parameters.samples,
        parameters.iterations,
        wall.wall_us,
        cpu.cpu_us,
        allocation.allocated_bytes,
        allocation.allocation_calls,
    );
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

fn take_stats(stats: &Arc<Mutex<IoStats>>) -> IoStats {
    std::mem::take(&mut *stats.lock().expect("I/O stats mutex"))
}

fn projected_value_len(value: &ProjectedValue) -> usize {
    match value {
        ProjectedValue::KeyOnly => 0,
        ProjectedValue::FullValue(value) => value.len(),
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

fn process_clock_ticks_per_second() -> u64 {
    std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(100)
}

fn cpu_ticks_to_us(ticks: u64) -> f64 {
    ticks as f64 * 1_000_000.0 / process_clock_ticks_per_second() as f64
}

fn process_cpu_ticks() -> u64 {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return 0;
    };
    let Some((_, fields)) = stat.rsplit_once(") ") else {
        return 0;
    };
    let fields = fields.split_whitespace().collect::<Vec<_>>();
    fields
        .get(11)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_add(
            fields
                .get(12)
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0),
        )
}

fn directory_bytes(path: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries.flatten().fold(0, |total, entry| {
        let path = entry.path();
        let bytes = if path.is_dir() {
            directory_bytes(&path)
        } else {
            entry.metadata().map(|metadata| metadata.len()).unwrap_or(0)
        };
        total.saturating_add(bytes)
    })
}

fn parse_positive(value: Option<&String>, name: &str, default: usize) -> usize {
    let parsed = parse_nonnegative(value, name, default);
    assert!(parsed > 0, "{name} must be positive");
    parsed
}

fn parse_nonnegative(value: Option<&String>, name: &str, default: usize) -> usize {
    value.map_or(default, |value| {
        value
            .parse::<usize>()
            .unwrap_or_else(|error| panic!("invalid {name} '{value}': {error}"))
    })
}
