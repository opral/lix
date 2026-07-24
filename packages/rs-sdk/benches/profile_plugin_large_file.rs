//! Large-file profiling harness for plugin-backed CSV operations.
//!
//! `<backend> setup <dir>` builds the fixture, installs the CSV plugin, writes
//! the initial CSV, and closes. The other modes reopen the
//! prepared file, warm the plugin outside the measured region, and—before a
//! write—perform the exact blob point read that acknowledges the session's
//! semantic file view. It then runs one operation inside a named marker frame
//! so samply samples can be filtered to that frame. Backends are production
//! `rocksdb-fs`, raw-storage-control
//! `rocksdb`, cacheless-control `slatedb`, and Lixray-representative
//! `slatedb-cached`; their post-operation data directories are left on disk for
//! inspection. `slatedb-cached` persists its local object store and cache under
//! the case directory. Its per-workspace budgets match Lixray defaults: 64 MiB
//! disk (2 GiB / 32 workspaces), 4 MiB block (128 MiB / 32), and 1 MiB metadata
//! (32 MiB / 32).
//!
//! Environment variables:
//! - `LIX_PROFILE_INITIAL_ROWS` (default 10,000)
//! - `LIX_PROFILE_NEW_ROWS` (default 10,000, merge mode only)
//! - `LIX_PROFILE_ROUNDS` (default 1; repeated no-op, edit, and render samples)
//! - `LIX_PROFILE_IO_STATS=1` (print logical storage calls and row counts for
//!   the measured operation, followed by read counts sorted by `SpaceId`;
//!   warmup I/O is excluded)
//! - `LIX_PROFILE_WASM_MEMORY_MIB` (diagnostic only; wraps the SDK runtime with
//!   a non-production memory ceiling so an otherwise-OOM 10 MiB v1 operation
//!   can be timed; omitted means the production 64 MiB policy)
//!
//! Differences from the merge_10k criterion bench (benches/e2e.rs): the bench's
//! measured region includes `lix.close()`, which this marker frame excludes, and
//! the merge here runs in a fresh process against a reopened store, so cold-load
//! frames appear that the bench's in-process setup absorbs. Profile shapes are
//! comparable; absolute times are not.

use lix_rocksdb_storage::RocksDB;
use lix_sdk::{
    CommitResult, GetManyResult, GetOptions, Key, KeyRange, LocalFilesystem,
    LocalFilesystemOpenOptions, OpenLixOptions, PutBatch, ReadOptions, ScanChunk, ScanOptions,
    SpaceId, Storage, StorageError, StorageRead, StorageWrite, Value, WasmComponentInstance,
    WasmLimits, WasmRuntime, WriteOptions, open_lix,
};
use lix_slatedb_storage::SlateDB;
use lix_slatedb_storage::{SlateDBCacheOptions, SlateDBObjectStoreOptions};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use std::collections::BTreeMap;
use std::hint::black_box;
use std::io::{Cursor, Write as _};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const DEFAULT_INITIAL_ROW_COUNT: usize = 10_000;
const DEFAULT_NEW_ROW_COUNT: usize = 10_000;
const CSV_PATH: &str = "/large-merge.csv";
const CSV_PLUGIN_WARMUP_PATH: &str = "/.csv-plugin-warmup.csv";
const SLATEDB_CACHED_DB_PATH: &str = "workspace";
const SLATEDB_CACHED_DISK_CACHE_BYTES: usize = 64 * 1024 * 1024;
const SLATEDB_CACHED_BLOCK_CACHE_BYTES: u64 = 4 * 1024 * 1024;
const SLATEDB_CACHED_METADATA_CACHE_BYTES: u64 = 1024 * 1024;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (backend, mode, dir) = match args.as_slice() {
        [_, backend, mode, dir]
            if matches!(
                backend.as_str(),
                "rocksdb-fs" | "rocksdb" | "slatedb" | "slatedb-cached"
            ) && matches!(
                mode.as_str(),
                "setup" | "merge" | "noop" | "edit" | "render" | "render-noack"
            ) =>
        {
            (backend.as_str(), mode.as_str(), dir.clone())
        }
        _ => {
            // `cargo bench` invokes every bench target with harness flags
            // (--bench, filters); this profiling harness only runs when
            // called explicitly, so a plain usage note and success exit
            // keeps bench sweeps green.
            eprintln!(
                "usage: profile_plugin_large_file <rocksdb-fs|rocksdb|slatedb|slatedb-cached> \
                 <setup|merge|noop|edit|render|render-noack> <dir>"
            );
            return;
        }
    };
    let storage_path = Path::new(&dir).join(backend);
    let initial_row_count = env_usize("LIX_PROFILE_INITIAL_ROWS", DEFAULT_INITIAL_ROW_COUNT);
    let new_row_count = env_usize("LIX_PROFILE_NEW_ROWS", DEFAULT_NEW_ROW_COUNT);
    let io_stats_enabled = env_flag("LIX_PROFILE_IO_STATS");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let initial_rows = random_csv_rows("initial", initial_row_count, 0x8ae7_b4b1_9f4c_d215);
    let new_rows = random_csv_rows("new", new_row_count, 0xf3bb_91d4_6a8c_2e73);

    match backend {
        "rocksdb-fs" => {
            let options = LocalFilesystemOpenOptions::new(storage_path.clone(), true);
            let storage = if let Some(wasm_runtime) = profiling_wasm_runtime() {
                runtime.block_on(LocalFilesystem::open_with_options_and_wasm_runtime(
                    options,
                    wasm_runtime,
                ))
            } else {
                runtime.block_on(LocalFilesystem::open_with_options(options))
            }
            .expect("open production RocksDB filesystem profiling storage");
            runtime.block_on(run_with_optional_io_stats(
                storage,
                mode,
                &initial_rows,
                &new_rows,
                io_stats_enabled,
            ));
        }
        "rocksdb" => {
            let storage = RocksDB::open(&storage_path).expect("open RocksDB profiling storage");
            runtime.block_on(run_with_optional_io_stats(
                storage.clone(),
                mode,
                &initial_rows,
                &new_rows,
                io_stats_enabled,
            ));
            storage.flush().expect("flush RocksDB profiling storage");
        }
        "slatedb" => {
            let storage = SlateDB::open(&storage_path).expect("open SlateDB profiling storage");
            runtime.block_on(run_with_optional_io_stats(
                storage.clone(),
                mode,
                &initial_rows,
                &new_rows,
                io_stats_enabled,
            ));
            runtime
                .block_on(storage.flush())
                .expect("flush SlateDB profiling storage");
        }
        "slatedb-cached" => {
            let storage = open_cached_slatedb(&storage_path);
            runtime.block_on(run_with_optional_io_stats(
                storage.clone(),
                mode,
                &initial_rows,
                &new_rows,
                io_stats_enabled,
            ));
            runtime
                .block_on(storage.flush())
                .expect("flush cached SlateDB profiling storage");
        }
        _ => unreachable!("validated storage backend"),
    }
    eprintln!(
        "storage_bytes backend={backend} bytes={}",
        directory_bytes(&storage_path)
    );
    if backend == "rocksdb-fs" {
        eprintln!(
            "storage_bytes_detail backend={backend} rocksdb_bytes={} materialized_file_bytes={} plugin_archive_bytes={}",
            directory_bytes(&storage_path.join(".lix/.internal/rocksdb")),
            file_bytes(&storage_path.join(CSV_PATH.trim_start_matches('/'))),
            file_bytes(&storage_path.join(".lix/plugins/plugin_csv.lixplugin")),
        );
    } else if backend == "slatedb-cached" {
        eprintln!(
            "storage_bytes_detail backend={backend} object_store_bytes={} cache_bytes={}",
            directory_bytes(&storage_path.join("object-store")),
            directory_bytes(&storage_path.join("cache"))
        );
    }
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| {
            entry.metadata().map_or(0, |metadata| {
                if metadata.is_dir() {
                    directory_bytes(&entry.path())
                } else {
                    metadata.len()
                }
            })
        })
        .sum()
}

fn file_bytes(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |metadata| metadata.len())
}

fn open_cached_slatedb(storage_path: &Path) -> SlateDB {
    let object_store_root = storage_path.join("object-store");
    let cache_root = storage_path.join("cache");
    std::fs::create_dir_all(&object_store_root)
        .expect("create cached SlateDB local object-store root");
    let object_store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(&object_store_root)
            .expect("open cached SlateDB local object store"),
    );
    SlateDB::open_object_store_with_options(
        SLATEDB_CACHED_DB_PATH,
        object_store,
        SlateDBObjectStoreOptions {
            cache: Some(SlateDBCacheOptions {
                root_folder: cache_root,
                max_disk_cache_bytes: SLATEDB_CACHED_DISK_CACHE_BYTES,
                block_cache_bytes: SLATEDB_CACHED_BLOCK_CACHE_BYTES,
                metadata_cache_bytes: SLATEDB_CACHED_METADATA_CACHE_BYTES,
            }),
        },
    )
    .expect("open cached SlateDB profiling storage")
}

async fn run_with_optional_io_stats<S>(
    storage: S,
    mode: &str,
    initial_rows: &[String],
    new_rows: &[String],
    io_stats_enabled: bool,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    if io_stats_enabled {
        let storage = IoCountingStorage::new(storage);
        let stats = storage.stats();
        run_mode(storage, mode, initial_rows, new_rows, Some(stats)).await;
    } else {
        run_mode(storage, mode, initial_rows, new_rows, None).await;
    }
}

async fn run_mode<S>(
    storage: S,
    mode: &str,
    initial_rows: &[String],
    new_rows: &[String],
    io_stats: Option<IoStats>,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix(open_options(storage)).await.unwrap();
    if mode == "setup" {
        let plugin = build_csv_plugin();
        install_plugin(&lix, "plugin_csv", &plugin).await;
        let initial_csv = csv_bytes_from_rows(initial_rows);
        eprintln!(
            "setup rows={} bytes={}",
            initial_rows.len(),
            initial_csv.len()
        );
        if let Some(stats) = &io_stats {
            stats.reset();
        }
        let start = Instant::now();
        write_file(&lix, CSV_PATH, initial_csv).await;
        eprintln!("setup insert took {:?}", start.elapsed());
        if let Some(stats) = &io_stats {
            stats.print("setup");
        }
        lix.close().await.unwrap();
        return;
    }

    // Compile the component and prime caches outside the measured region.
    write_file(&lix, CSV_PLUGIN_WARMUP_PATH, Vec::new()).await;
    lix.execute(
        "DELETE FROM lix_file WHERE path = $1",
        &[Value::Text(CSV_PLUGIN_WARMUP_PATH.to_string())],
    )
    .await
    .unwrap();

    // Multiplayer reconciliation intentionally distinguishes a blind write
    // from an edit to bytes that this session actually received. Model the
    // ordinary client flow for every measured write: exact blob read, local
    // edit, blob write. Keep the acknowledgement/read outside the write timer.
    if matches!(mode, "merge" | "noop" | "edit") {
        profile_render_phase(&lix).await;
    }

    if let Some(stats) = &io_stats {
        stats.reset();
    }
    let start = Instant::now();
    match mode {
        "merge" => {
            let updated_csv = csv_bytes_from_rows(&randomly_merge_csv_rows(
                initial_rows,
                new_rows,
                0x6449_2c6f_179d_31b5,
            ));
            eprintln!(
                "merge initial_rows={} new_rows={} bytes={}",
                initial_rows.len(),
                new_rows.len(),
                updated_csv.len()
            );
            profile_file_write_phase(&lix, updated_csv).await;
        }
        "noop" => {
            let updated_csv = csv_bytes_from_rows(initial_rows);
            let rounds = env_usize("LIX_PROFILE_ROUNDS", 1);
            eprintln!(
                "noop rows={} bytes={} rounds={rounds}",
                initial_rows.len(),
                updated_csv.len()
            );
            let mut samples = Vec::with_capacity(rounds);
            for _ in 0..rounds {
                let payload = updated_csv.clone();
                let sample_start = Instant::now();
                profile_file_write_phase(&lix, payload).await;
                samples.push(sample_start.elapsed());
            }
            print_samples("noop", &mut samples);
        }
        "edit" => {
            let mut edited_rows = initial_rows.to_vec();
            let middle = edited_rows.len() / 2;
            edited_rows
                .get_mut(middle)
                .expect("edit mode requires at least one row")
                .push_str("-edited");
            let initial_csv = csv_bytes_from_rows(initial_rows);
            let updated_csv = csv_bytes_from_rows(&edited_rows);
            let rounds = env_usize("LIX_PROFILE_ROUNDS", 1);
            eprintln!(
                "edit rows={} bytes={} rounds={rounds}",
                edited_rows.len(),
                updated_csv.len()
            );
            let mut samples = Vec::with_capacity(rounds);
            for round in 0..rounds {
                let payload = if round % 2 == 0 {
                    updated_csv.clone()
                } else {
                    initial_csv.clone()
                };
                let sample_start = Instant::now();
                profile_file_write_phase(&lix, payload).await;
                samples.push(sample_start.elapsed());
            }
            print_samples("edit", &mut samples);
        }
        "render" | "render-noack" => {
            let rounds = env_usize("LIX_PROFILE_ROUNDS", 1);
            eprintln!("{mode} rows={} rounds={rounds}", initial_rows.len());
            let mut samples = Vec::with_capacity(rounds);
            for _ in 0..rounds {
                let sample_start = Instant::now();
                if mode == "render" {
                    profile_render_phase(&lix).await;
                } else {
                    profile_unacknowledged_render_phase(&lix).await;
                }
                samples.push(sample_start.elapsed());
            }
            print_samples(mode, &mut samples);
        }
        _ => unreachable!("validated profiling mode"),
    }
    eprintln!("{mode} took {:?}", start.elapsed());
    if let Some(stats) = &io_stats {
        stats.print(mode);
    }
    lix.close().await.unwrap();
}

fn open_options<S>(storage: S) -> OpenLixOptions<S> {
    let options = OpenLixOptions::new(storage);
    let Some(wasm_runtime) = profiling_wasm_runtime() else {
        return options;
    };
    let memory_mib = profile_wasm_memory_mib().expect("profiling runtime requires memory limit");
    eprintln!("diagnostic_wasm_memory_mib={memory_mib} production_default_mib=64");
    options.with_wasm_runtime(wasm_runtime)
}

fn profile_wasm_memory_mib() -> Option<u64> {
    let Ok(raw_memory_mib) = std::env::var("LIX_PROFILE_WASM_MEMORY_MIB") else {
        return None;
    };
    let memory_mib = raw_memory_mib
        .parse::<u64>()
        .expect("LIX_PROFILE_WASM_MEMORY_MIB must be a positive integer");
    assert!(
        memory_mib > 0,
        "LIX_PROFILE_WASM_MEMORY_MIB must be nonzero"
    );
    Some(memory_mib)
}

fn profiling_wasm_runtime() -> Option<Arc<dyn WasmRuntime>> {
    let memory_mib = profile_wasm_memory_mib()?;
    let max_memory_bytes = memory_mib
        .checked_mul(1024 * 1024)
        .expect("LIX_PROFILE_WASM_MEMORY_MIB exceeds u64 bytes");
    let inner = lix_sdk::profiling_default_wasm_runtime()
        .expect("initialize SDK Wasmtime runtime for profiling");
    Some(Arc::new(MemoryOverrideRuntime {
        inner,
        max_memory_bytes,
    }))
}

struct MemoryOverrideRuntime {
    inner: Arc<dyn WasmRuntime>,
    max_memory_bytes: u64,
}

#[async_trait::async_trait]
impl WasmRuntime for MemoryOverrideRuntime {
    async fn init_component(
        &self,
        bytes: Vec<u8>,
        mut limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, lix_sdk::LixError> {
        limits.max_memory_bytes = self.max_memory_bytes;
        self.inner.init_component(bytes, limits).await
    }
}

#[inline(never)]
async fn profile_file_write_phase<S>(lix: &lix_sdk::Lix<S>, updated_csv: Vec<u8>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    write_file(lix, CSV_PATH, updated_csv).await;
}

#[inline(never)]
async fn profile_render_phase<S>(lix: &lix_sdk::Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text(CSV_PATH.to_string())],
        )
        .await
        .unwrap();
    black_box(result);
}

/// Render the same unique point row without granting delete authority. The
/// zero offset is semantically inert, but acknowledgement deliberately rejects
/// every offset. This isolates the cost of retaining a rich session file view
/// from the plugin render and storage work shared with `render`.
#[inline(never)]
async fn profile_unacknowledged_render_phase<S>(lix: &lix_sdk::Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT data FROM lix_file WHERE path = $1 LIMIT 1 OFFSET 0",
            &[Value::Text(CSV_PATH.to_string())],
        )
        .await
        .unwrap();
    black_box(result);
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |raw| {
        raw.parse::<usize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive integer"))
    })
}

fn env_flag(name: &str) -> bool {
    match std::env::var(name) {
        Ok(raw) => !matches!(raw.as_str(), "" | "0" | "false" | "no"),
        Err(std::env::VarError::NotPresent) => false,
        Err(error) => panic!("{name} must be valid Unicode: {error}"),
    }
}

fn print_samples(label: &str, samples: &mut [std::time::Duration]) {
    samples.sort_unstable();
    let sample_ms = samples
        .iter()
        .map(|sample| sample.as_secs_f64() * 1_000.0)
        .collect::<Vec<_>>();
    let percentile = |numerator: usize| {
        let index = samples.len().saturating_mul(numerator).saturating_sub(1) / 100;
        samples[index]
    };
    eprintln!("{label} sample_ms={sample_ms:?}");
    eprintln!(
        "{label} samples={} p50_ms={:.3} p95_ms={:.3} min_ms={:.3} max_ms={:.3}",
        samples.len(),
        percentile(50).as_secs_f64() * 1_000.0,
        percentile(95).as_secs_f64() * 1_000.0,
        samples[0].as_secs_f64() * 1_000.0,
        samples[samples.len() - 1].as_secs_f64() * 1_000.0,
    );
}

#[derive(Clone, Default)]
struct IoStats(Arc<IoCounters>);

impl IoStats {
    fn reset(&self) {
        self.0.begin_read.store(0, Ordering::Relaxed);
        self.0.begin_write.store(0, Ordering::Relaxed);
        self.0.get_many_calls.store(0, Ordering::Relaxed);
        self.0.get_many_keys.store(0, Ordering::Relaxed);
        self.0.get_many_values.store(0, Ordering::Relaxed);
        self.0.scan_calls.store(0, Ordering::Relaxed);
        self.0.scan_entries.store(0, Ordering::Relaxed);
        self.0.put_calls.store(0, Ordering::Relaxed);
        self.0.put_rows.store(0, Ordering::Relaxed);
        self.0.delete_calls.store(0, Ordering::Relaxed);
        self.0.delete_rows.store(0, Ordering::Relaxed);
        self.0.delete_range_calls.store(0, Ordering::Relaxed);
        self.0
            .reads_by_space
            .lock()
            .expect("storage I/O space counters lock")
            .clear();
    }

    fn print(&self, label: &str) {
        eprintln!(
            "storage_io label={label} begin_read={} begin_write={} \
             get_many_calls={} get_many_keys={} get_many_values={} \
             scan_calls={} scan_entries={} put_calls={} put_rows={} \
             delete_calls={} delete_rows={} delete_range_calls={}",
            self.0.begin_read.load(Ordering::Relaxed),
            self.0.begin_write.load(Ordering::Relaxed),
            self.0.get_many_calls.load(Ordering::Relaxed),
            self.0.get_many_keys.load(Ordering::Relaxed),
            self.0.get_many_values.load(Ordering::Relaxed),
            self.0.scan_calls.load(Ordering::Relaxed),
            self.0.scan_entries.load(Ordering::Relaxed),
            self.0.put_calls.load(Ordering::Relaxed),
            self.0.put_rows.load(Ordering::Relaxed),
            self.0.delete_calls.load(Ordering::Relaxed),
            self.0.delete_rows.load(Ordering::Relaxed),
            self.0.delete_range_calls.load(Ordering::Relaxed),
        );
        for (space_id, counts) in self
            .0
            .reads_by_space
            .lock()
            .expect("storage I/O space counters lock")
            .iter()
        {
            eprintln!(
                "storage_io_space label={label} space_id={space_id} \
                 get_many_calls={} get_many_keys={} get_many_values={} \
                 scan_calls={} scan_entries={}",
                counts.get_many_calls,
                counts.get_many_keys,
                counts.get_many_values,
                counts.scan_calls,
                counts.scan_entries,
            );
        }
    }
}

#[derive(Default)]
struct SpaceReadCounts {
    get_many_calls: u64,
    get_many_keys: u64,
    get_many_values: u64,
    scan_calls: u64,
    scan_entries: u64,
}

#[derive(Default)]
struct IoCounters {
    begin_read: AtomicU64,
    begin_write: AtomicU64,
    get_many_calls: AtomicU64,
    get_many_keys: AtomicU64,
    get_many_values: AtomicU64,
    scan_calls: AtomicU64,
    scan_entries: AtomicU64,
    put_calls: AtomicU64,
    put_rows: AtomicU64,
    delete_calls: AtomicU64,
    delete_rows: AtomicU64,
    delete_range_calls: AtomicU64,
    reads_by_space: Mutex<BTreeMap<u32, SpaceReadCounts>>,
}

#[derive(Clone)]
struct IoCountingStorage<S> {
    inner: S,
    stats: IoStats,
}

impl<S> IoCountingStorage<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            stats: IoStats::default(),
        }
    }

    fn stats(&self) -> IoStats {
        self.stats.clone()
    }
}

impl<S> Storage for IoCountingStorage<S>
where
    S: Storage,
{
    type Read<'a>
        = IoCountingRead<S::Read<'a>>
    where
        Self: 'a;

    type Write<'a>
        = IoCountingWrite<S::Write<'a>>
    where
        Self: 'a;

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.stats.0.begin_read.fetch_add(1, Ordering::Relaxed);
        Ok(IoCountingRead {
            inner: self.inner.begin_read(opts).await?,
            stats: self.stats.clone(),
        })
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.stats.0.begin_write.fetch_add(1, Ordering::Relaxed);
        Ok(IoCountingWrite {
            inner: self.inner.begin_write(opts).await?,
            stats: self.stats.clone(),
        })
    }
}

struct IoCountingRead<R> {
    inner: R,
    stats: IoStats,
}

impl<R> StorageRead for IoCountingRead<R>
where
    R: StorageRead,
{
    async fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> Result<GetManyResult, StorageError> {
        self.stats.0.get_many_calls.fetch_add(1, Ordering::Relaxed);
        self.stats.0.get_many_keys.fetch_add(
            u64::try_from(keys.len()).expect("get_many key count fits u64"),
            Ordering::Relaxed,
        );
        let result = self.inner.get_many(space, keys, opts).await?;
        let present_values = present_value_count(&result);
        self.stats
            .0
            .get_many_values
            .fetch_add(present_values, Ordering::Relaxed);
        let mut reads_by_space = self
            .stats
            .0
            .reads_by_space
            .lock()
            .expect("storage I/O space counters lock");
        let counts = reads_by_space.entry(space.0).or_default();
        counts.get_many_calls += 1;
        counts.get_many_keys += u64::try_from(keys.len()).expect("get_many key count fits u64");
        counts.get_many_values += present_values;
        Ok(result)
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        self.stats.0.scan_calls.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.scan(space, range, opts).await?;
        self.stats.0.scan_entries.fetch_add(
            u64::try_from(result.entries.len()).expect("scan entry count fits u64"),
            Ordering::Relaxed,
        );
        let mut reads_by_space = self
            .stats
            .0
            .reads_by_space
            .lock()
            .expect("storage I/O space counters lock");
        let counts = reads_by_space.entry(space.0).or_default();
        counts.scan_calls += 1;
        counts.scan_entries +=
            u64::try_from(result.entries.len()).expect("scan entry count fits u64");
        Ok(result)
    }
}

fn present_value_count(result: &GetManyResult) -> u64 {
    u64::try_from(result.values.iter().filter(|value| value.is_some()).count())
        .expect("get_many value count fits u64")
}

struct IoCountingWrite<W> {
    inner: W,
    stats: IoStats,
}

impl<W> StorageWrite for IoCountingWrite<W>
where
    W: StorageWrite,
{
    async fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), StorageError> {
        self.stats.0.put_calls.fetch_add(1, Ordering::Relaxed);
        self.stats.0.put_rows.fetch_add(
            u64::try_from(entries.entries.len()).expect("put row count fits u64"),
            Ordering::Relaxed,
        );
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), StorageError> {
        self.stats.0.delete_calls.fetch_add(1, Ordering::Relaxed);
        self.stats.0.delete_rows.fetch_add(
            u64::try_from(keys.len()).expect("delete row count fits u64"),
            Ordering::Relaxed,
        );
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), StorageError> {
        self.stats.0.delete_calls.fetch_add(1, Ordering::Relaxed);
        self.stats
            .0
            .delete_range_calls
            .fetch_add(1, Ordering::Relaxed);
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

async fn install_plugin<S>(lix: &lix_sdk::Lix<S>, key: &str, archive: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let path = format!("/.lix/plugins/{key}.lixplugin");
    write_file(lix, &path, archive.to_vec()).await;
}

async fn write_file<S>(lix: &lix_sdk::Lix<S>, path: &str, data: Vec<u8>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET data = excluded.data",
        &[Value::Text(path.to_string()), Value::Blob(data)],
    )
    .await
    .unwrap();
}

/// Deterministic splitmix64 generator. The bench (e2e.rs) seeds rand's
/// SmallRng, a dev-dependency this harness deliberately avoids; the fixture
/// bytes therefore differ from the bench's, but the harness only needs
/// setup and merge to agree with each other.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform value in `0..bound` (bound > 0); modulo bias is irrelevant
    /// for fixture shuffling.
    fn next_below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next_u64() % bound as u64).expect("bound fits usize")
    }
}

fn random_csv_rows(prefix: &str, count: usize, seed: u64) -> Vec<String> {
    let mut rng = SplitMix64(seed);
    (0..count)
        .map(|offset| {
            format!(
                "{prefix}-{offset:05},{:016x},{:016x}",
                rng.next_u64(),
                rng.next_u64()
            )
        })
        .collect()
}

fn randomly_merge_csv_rows(initial_rows: &[String], new_rows: &[String], seed: u64) -> Vec<String> {
    let mut rng = SplitMix64(seed);
    let mut merged = Vec::with_capacity(initial_rows.len() + new_rows.len());
    let mut initial_index = 0usize;
    let mut new_index = 0usize;

    while initial_index < initial_rows.len() || new_index < new_rows.len() {
        let take_initial = if initial_index == initial_rows.len() {
            false
        } else if new_index == new_rows.len() {
            true
        } else {
            let remaining_initial = initial_rows.len() - initial_index;
            let remaining_new = new_rows.len() - new_index;
            rng.next_below(remaining_initial + remaining_new) < remaining_initial
        };

        if take_initial {
            merged.push(initial_rows[initial_index].clone());
            initial_index += 1;
        } else {
            merged.push(new_rows[new_index].clone());
            new_index += 1;
        }
    }

    merged
}

fn csv_bytes_from_rows(rows: &[String]) -> Vec<u8> {
    let mut csv = String::with_capacity(rows.iter().map(|row| row.len() + 1).sum());
    for row in rows {
        csv.push_str(row);
        csv.push('\n');
    }
    csv.into_bytes()
}

fn build_csv_plugin() -> Vec<u8> {
    // option_env: the bindep artifact env var is absent in some CI target
    // contexts; this harness is only ever run manually, so resolve at
    // runtime and fail with instructions instead of failing the compile.
    let Some(wasm_path) = option_env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv") else {
        eprintln!(
            "CSV plugin wasm path unavailable; build via `cargo build --bench \
             profile_plugin_large_file --features \
             default_wasm_runtime,local_filesystem,__profile_wasm_memory` so cargo provides the \
             bindep artifact"
        );
        std::process::exit(2);
    };
    let wasm = std::fs::read(Path::new(wasm_path)).expect("read bindep-built CSV plugin wasm");
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/csv/manifest.json").as_bytes(),
        ),
        (
            "schema/csv_table.json",
            include_str!("../../../plugins/csv/schema/csv_table.json").as_bytes(),
        ),
        (
            "schema/csv_row.json",
            include_str!("../../../plugins/csv/schema/csv_row.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}
