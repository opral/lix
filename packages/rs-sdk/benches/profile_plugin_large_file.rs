//! Large-file profiling harness for plugin-backed CSV and JSON operations.
//!
//! `<backend> setup <dir>` builds the selected fixture, installs its real Wasm
//! plugin, writes the initial file, and closes. The other modes reopen the
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
//! - `LIX_PROFILE_FORMAT` (`csv`, the default production-v2 lane, or `json`,
//!   the real v1-plugin mechanism lane)
//! - `LIX_PROFILE_INITIAL_ROWS` (CSV only; default 10,000)
//! - `LIX_PROFILE_NEW_ROWS` (CSV merge only; default 10,000)
//! - `LIX_PROFILE_WARMUPS` (default 0; unreported no-op/edit/render operations
//!   before measured rounds)
//! - `LIX_PROFILE_ROUNDS` (default 1; repeated no-op, edit, and render samples)
//! - `LIX_PROFILE_IO_STATS=1` (print logical storage calls and row counts for
//!   the measured operation, followed by read counts sorted by `SpaceId`;
//!   warmup I/O is excluded)
//! - `LIX_PROFILE_SPLICE_PROVENANCE=1` (CSV edit only; attach the same
//!   already-validated splice/hash sidecar produced by the remote protocol)
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
    CommitResult, ExecuteOptions, ExecuteStatementMetadata, GetManyResult, GetOptions, Key,
    KeyRange, LocalFilesystem, LocalFilesystemOpenOptions, MutationIdentity, OpenLixOptions,
    PutBatch, ReadOptions, RequestBlobSpliceProvenance, ScanChunk, ScanOptions, SpaceId, Storage,
    StorageError, StorageRead, StorageWrite, Value, WasmComponentInstance, WasmComponentV2Factory,
    WasmLimits, WasmRuntime, WasmTransitionCounters, WriteOptions, open_lix,
};
use lix_slatedb_storage::SlateDB;
use lix_slatedb_storage::{SlateDBCacheOptions, SlateDBObjectStoreOptions};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use sha2::{Digest as _, Sha256};
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
const JSON_PATH: &str = "/large-object.json";
const JSON_PLUGIN_WARMUP_PATH: &str = "/.json-plugin-warmup.json";
const JSON_TARGET_BYTE_COUNT: usize = 10_000_000;
const JSON_PROPERTY_COUNT: usize = 220_000;
// With 220,000 fixed-width keys and 24-byte values, the JSON object is
// 9,900,001 bytes. One extra byte in each of the first 99,999 values makes the
// fixture exactly 10,000,000 bytes without a pathological padding property.
const JSON_LONG_VALUE_COUNT: usize = 99_999;
const JSON_FIXTURE_SEED: u64 = 0x6a73_6f6e_2d31_306d;
const SLATEDB_CACHED_DB_PATH: &str = "workspace";
const SLATEDB_CACHED_DISK_CACHE_BYTES: usize = 64 * 1024 * 1024;
const SLATEDB_CACHED_BLOCK_CACHE_BYTES: u64 = 4 * 1024 * 1024;
const SLATEDB_CACHED_METADATA_CACHE_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProfileFormat {
    Csv,
    Json,
}

impl ProfileFormat {
    fn from_env() -> Self {
        match std::env::var("LIX_PROFILE_FORMAT") {
            Ok(raw) if raw.eq_ignore_ascii_case("csv") => Self::Csv,
            Ok(raw) if raw.eq_ignore_ascii_case("json") => Self::Json,
            Ok(raw) => panic!("LIX_PROFILE_FORMAT must be 'csv' or 'json', got '{raw}'"),
            Err(std::env::VarError::NotPresent) => Self::Csv,
            Err(error) => panic!("LIX_PROFILE_FORMAT must be valid Unicode: {error}"),
        }
    }
}

struct FlatJsonFixture {
    initial: Vec<u8>,
    edited_value_offset: usize,
    edited_property: String,
}

impl FlatJsonFixture {
    fn edited_bytes(&self) -> Vec<u8> {
        let mut edited = self.initial.clone();
        assert!(
            edited[self.edited_value_offset].is_ascii_hexdigit(),
            "edited JSON value starts as generated hexadecimal"
        );
        // Generated values contain lowercase hexadecimal only, so this changes
        // exactly one byte while preserving both JSON validity and file length.
        edited[self.edited_value_offset] = b'x';
        edited
    }
}

enum LargeFileFixture {
    Csv {
        initial_rows: Vec<String>,
        new_rows: Vec<String>,
    },
    Json(FlatJsonFixture),
}

impl LargeFileFixture {
    fn from_env() -> Self {
        match ProfileFormat::from_env() {
            ProfileFormat::Csv => {
                let initial_row_count =
                    env_usize("LIX_PROFILE_INITIAL_ROWS", DEFAULT_INITIAL_ROW_COUNT);
                let new_row_count = env_usize("LIX_PROFILE_NEW_ROWS", DEFAULT_NEW_ROW_COUNT);
                Self::Csv {
                    initial_rows: random_csv_rows(
                        "initial",
                        initial_row_count,
                        0x8ae7_b4b1_9f4c_d215,
                    ),
                    new_rows: random_csv_rows("new", new_row_count, 0xf3bb_91d4_6a8c_2e73),
                }
            }
            ProfileFormat::Json => Self::Json(flat_json_fixture()),
        }
    }

    fn format(&self) -> ProfileFormat {
        match self {
            Self::Csv { .. } => ProfileFormat::Csv,
            Self::Json(_) => ProfileFormat::Json,
        }
    }

    fn path(&self) -> &'static str {
        match self {
            Self::Csv { .. } => CSV_PATH,
            Self::Json(_) => JSON_PATH,
        }
    }

    fn plugin_warmup_path(&self) -> &'static str {
        match self {
            Self::Csv { .. } => CSV_PLUGIN_WARMUP_PATH,
            Self::Json(_) => JSON_PLUGIN_WARMUP_PATH,
        }
    }

    fn plugin_warmup_bytes(&self) -> Vec<u8> {
        match self {
            // The v2 actor opens from a canonical blob materialization; an
            // empty warmup would not establish that observation.
            Self::Csv { .. } => b"warmup\n".to_vec(),
            Self::Json(_) => b"{}".to_vec(),
        }
    }

    fn plugin_key(&self) -> &'static str {
        match self {
            Self::Csv { .. } => "plugin_csv_v2",
            Self::Json(_) => "plugin_json_v2",
        }
    }

    fn initial_bytes(&self) -> Vec<u8> {
        match self {
            Self::Csv { initial_rows, .. } => csv_bytes_from_rows(initial_rows),
            Self::Json(fixture) => fixture.initial.clone(),
        }
    }
}

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
    let fixture = LargeFileFixture::from_env();
    let io_stats_enabled = env_flag("LIX_PROFILE_IO_STATS");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

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
                &fixture,
                io_stats_enabled,
            ));
        }
        "rocksdb" => {
            let storage = RocksDB::open(&storage_path).expect("open RocksDB profiling storage");
            runtime.block_on(run_with_optional_io_stats(
                storage.clone(),
                mode,
                &fixture,
                io_stats_enabled,
            ));
            storage.flush().expect("flush RocksDB profiling storage");
        }
        "slatedb" => {
            let storage = SlateDB::open(&storage_path).expect("open SlateDB profiling storage");
            runtime.block_on(run_with_optional_io_stats(
                storage.clone(),
                mode,
                &fixture,
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
                &fixture,
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
            file_bytes(&storage_path.join(fixture.path().trim_start_matches('/'))),
            file_bytes(
                &storage_path.join(format!(".lix/plugins/{}.lixplugin", fixture.plugin_key()))
            ),
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
    fixture: &LargeFileFixture,
    io_stats_enabled: bool,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    if io_stats_enabled {
        let storage = IoCountingStorage::new(storage);
        let stats = storage.stats();
        run_mode(storage, mode, fixture, Some(stats)).await;
    } else {
        run_mode(storage, mode, fixture, None).await;
    }
}

async fn run_mode<S>(storage: S, mode: &str, fixture: &LargeFileFixture, io_stats: Option<IoStats>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix(open_options(storage)).await.unwrap();
    if mode == "setup" {
        let plugin = build_plugin(fixture.format());
        install_plugin(&lix, fixture.plugin_key(), &plugin).await;
        let initial_file = fixture.initial_bytes();
        match fixture {
            LargeFileFixture::Csv { initial_rows, .. } => {
                eprintln!(
                    "setup format=csv runtime=wasm-component-v2 rows={} bytes={}",
                    initial_rows.len(),
                    initial_file.len()
                );
            }
            LargeFileFixture::Json(json) => {
                eprintln!(
                    "setup format=json runtime=wasm-component-v1 mechanism_only=true properties={} bytes={} edit_property={}",
                    JSON_PROPERTY_COUNT,
                    initial_file.len(),
                    json.edited_property
                );
            }
        }
        if let Some(stats) = &io_stats {
            stats.reset();
        }
        let start = Instant::now();
        write_file(&lix, fixture.path(), initial_file).await;
        eprintln!("setup insert took {:?}", start.elapsed());
        if let Some(stats) = &io_stats {
            stats.print("setup");
        }
        lix.close().await.unwrap();
        return;
    }

    // Compile the component and prime caches outside the measured region.
    write_file(
        &lix,
        fixture.plugin_warmup_path(),
        fixture.plugin_warmup_bytes(),
    )
    .await;
    lix.execute(
        "DELETE FROM lix_file WHERE path = $1",
        &[Value::Text(fixture.plugin_warmup_path().to_string())],
    )
    .await
    .unwrap();

    // Multiplayer reconciliation intentionally distinguishes a blind write
    // from an edit to bytes that this session actually received. Model the
    // ordinary client flow for every measured write: exact blob read, local
    // edit, blob write. Keep the acknowledgement/read outside the write timer.
    if matches!(mode, "merge" | "noop" | "edit") {
        profile_render_phase(&lix, fixture.path()).await;
    }

    if let Some(stats) = &io_stats {
        stats.reset();
    }
    let start = Instant::now();
    let mut measured_elapsed = None;
    match mode {
        "merge" => {
            let LargeFileFixture::Csv {
                initial_rows,
                new_rows,
            } = fixture
            else {
                panic!("merge mode is only defined for LIX_PROFILE_FORMAT=csv");
            };
            let updated_file = csv_bytes_from_rows(&randomly_merge_csv_rows(
                initial_rows,
                new_rows,
                0x6449_2c6f_179d_31b5,
            ));
            eprintln!(
                "merge initial_rows={} new_rows={} bytes={}",
                initial_rows.len(),
                new_rows.len(),
                updated_file.len()
            );
            profile_file_write_phase(&lix, fixture.path(), updated_file).await;
        }
        "noop" => {
            let updated_file = fixture.initial_bytes();
            let warmups = env_usize("LIX_PROFILE_WARMUPS", 0);
            let rounds = env_usize("LIX_PROFILE_ROUNDS", 1);
            match fixture {
                LargeFileFixture::Csv { initial_rows, .. } => eprintln!(
                    "noop rows={} bytes={} warmups={warmups} rounds={rounds}",
                    initial_rows.len(),
                    updated_file.len()
                ),
                LargeFileFixture::Json(_) => eprintln!(
                    "noop format=json properties={} bytes={} warmups={warmups} rounds={rounds}",
                    JSON_PROPERTY_COUNT,
                    updated_file.len()
                ),
            }
            for _ in 0..warmups {
                profile_file_write_phase(&lix, fixture.path(), updated_file.clone()).await;
            }
            if let Some(stats) = &io_stats {
                stats.reset();
            }
            let measured_start = Instant::now();
            let mut samples = Vec::with_capacity(rounds);
            for _ in 0..rounds {
                let payload = updated_file.clone();
                let sample_start = Instant::now();
                profile_file_write_phase(&lix, fixture.path(), payload).await;
                samples.push(sample_start.elapsed());
            }
            measured_elapsed = Some(measured_start.elapsed());
            print_samples("noop", &mut samples);
        }
        "edit" => {
            let (initial_file, updated_file) = match fixture {
                LargeFileFixture::Csv { initial_rows, .. } => {
                    let mut edited_rows = initial_rows.clone();
                    let middle = edited_rows.len() / 2;
                    edited_rows
                        .get_mut(middle)
                        .expect("edit mode requires at least one row")
                        .push_str("-edited");
                    (
                        csv_bytes_from_rows(initial_rows),
                        csv_bytes_from_rows(&edited_rows),
                    )
                }
                LargeFileFixture::Json(json) => (json.initial.clone(), json.edited_bytes()),
            };
            let warmups = env_usize("LIX_PROFILE_WARMUPS", 0);
            let rounds = env_usize("LIX_PROFILE_ROUNDS", 1);
            let splice_provenance =
                env_flag("LIX_PROFILE_SPLICE_PROVENANCE") && fixture.format() == ProfileFormat::Csv;
            let forward_splice =
                splice_provenance.then(|| request_splice_provenance(&initial_file, &updated_file));
            let reverse_splice =
                splice_provenance.then(|| request_splice_provenance(&updated_file, &initial_file));
            match fixture {
                LargeFileFixture::Csv { initial_rows, .. } => eprintln!(
                    "edit rows={} bytes={} warmups={warmups} rounds={rounds}",
                    initial_rows.len(),
                    updated_file.len()
                ),
                LargeFileFixture::Json(json) => eprintln!(
                    "edit format=json properties={} bytes={} property={} warmups={warmups} rounds={rounds}",
                    JSON_PROPERTY_COUNT,
                    updated_file.len(),
                    json.edited_property
                ),
            }
            for warmup in 0..warmups {
                let payload = if warmup % 2 == 0 {
                    updated_file.clone()
                } else {
                    initial_file.clone()
                };
                let provenance = if warmup % 2 == 0 {
                    forward_splice.clone()
                } else {
                    reverse_splice.clone()
                };
                profile_file_write_phase_with_splice(&lix, fixture.path(), payload, provenance)
                    .await;
            }
            if let Some(stats) = &io_stats {
                stats.reset();
            }
            let measured_start = Instant::now();
            let mut samples = Vec::with_capacity(rounds);
            for round in 0..rounds {
                let transition = warmups.saturating_add(round);
                let payload = if transition % 2 == 0 {
                    updated_file.clone()
                } else {
                    initial_file.clone()
                };
                if fixture.format() == ProfileFormat::Csv {
                    lix.reset_plugin_v2_transition_counters();
                }
                let sample_start = Instant::now();
                let provenance = if transition % 2 == 0 {
                    forward_splice.clone()
                } else {
                    reverse_splice.clone()
                };
                profile_file_write_phase_with_splice(&lix, fixture.path(), payload, provenance)
                    .await;
                samples.push(sample_start.elapsed());
                if fixture.format() == ProfileFormat::Csv {
                    let counters = lix.plugin_v2_transition_counters();
                    print_v2_transition_counters("edit", round, counters);
                    assert_warm_single_row_edit_counters(round, counters, splice_provenance);
                }
            }
            measured_elapsed = Some(measured_start.elapsed());
            print_samples("edit", &mut samples);
        }
        "render" | "render-noack" => {
            let warmups = env_usize("LIX_PROFILE_WARMUPS", 0);
            let rounds = env_usize("LIX_PROFILE_ROUNDS", 1);
            match fixture {
                LargeFileFixture::Csv { initial_rows, .. } => {
                    eprintln!(
                        "{mode} rows={} warmups={warmups} rounds={rounds}",
                        initial_rows.len()
                    );
                }
                LargeFileFixture::Json(_) => {
                    eprintln!(
                        "{mode} format=json properties={} warmups={warmups} rounds={rounds}",
                        JSON_PROPERTY_COUNT,
                    );
                }
            }
            for _ in 0..warmups {
                if mode == "render" {
                    profile_render_phase(&lix, fixture.path()).await;
                } else {
                    profile_unacknowledged_render_phase(&lix, fixture.path()).await;
                }
            }
            if let Some(stats) = &io_stats {
                stats.reset();
            }
            let measured_start = Instant::now();
            let mut samples = Vec::with_capacity(rounds);
            for _ in 0..rounds {
                let sample_start = Instant::now();
                if mode == "render" {
                    profile_render_phase(&lix, fixture.path()).await;
                } else {
                    profile_unacknowledged_render_phase(&lix, fixture.path()).await;
                }
                samples.push(sample_start.elapsed());
            }
            measured_elapsed = Some(measured_start.elapsed());
            print_samples(mode, &mut samples);
        }
        _ => unreachable!("validated profiling mode"),
    }
    eprintln!(
        "{mode} took {:?}",
        measured_elapsed.unwrap_or_else(|| start.elapsed())
    );
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

    async fn compile_component_v2(
        &self,
        bytes: Vec<u8>,
        mut limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentV2Factory>, lix_sdk::LixError> {
        limits.max_memory_bytes = self.max_memory_bytes;
        self.inner.compile_component_v2(bytes, limits).await
    }
}

#[inline(never)]
async fn profile_file_write_phase<S>(lix: &lix_sdk::Lix<S>, path: &str, updated_file: Vec<u8>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    profile_file_write_phase_with_splice(lix, path, updated_file, None).await;
}

#[inline(never)]
async fn profile_file_write_phase_with_splice<S>(
    lix: &lix_sdk::Lix<S>,
    path: &str,
    updated_file: Vec<u8>,
    provenance: Option<RequestBlobSpliceProvenance>,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let metadata = ExecuteStatementMetadata {
        parameter_blob_splices: vec![None, provenance],
        mutation_identity: Some(MutationIdentity {
            namespace_seed: [0x42; 16],
            operation_proof: [0x24; 32],
        }),
    };
    lix.execute_with_options_and_metadata(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET data = excluded.data",
        &[Value::Text(path.to_string()), Value::Blob(updated_file)],
        ExecuteOptions::default(),
        metadata,
    )
    .await
    .unwrap();
}

fn request_splice_provenance(before: &[u8], after: &[u8]) -> RequestBlobSpliceProvenance {
    let prefix_bytes = before
        .iter()
        .zip(after)
        .take_while(|(left, right)| left == right)
        .count();
    let max_suffix = before
        .len()
        .saturating_sub(prefix_bytes)
        .min(after.len().saturating_sub(prefix_bytes));
    let suffix_bytes = before
        .iter()
        .rev()
        .take(max_suffix)
        .zip(after.iter().rev())
        .take_while(|(left, right)| left == right)
        .count();
    RequestBlobSpliceProvenance {
        base_sha256: sha256_hex(before),
        result_sha256: sha256_hex(after),
        prefix_bytes,
        suffix_bytes,
        insert: after[prefix_bytes..after.len() - suffix_bytes].to_vec(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

#[inline(never)]
async fn profile_render_phase<S>(lix: &lix_sdk::Lix<S>, path: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
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
async fn profile_unacknowledged_render_phase<S>(lix: &lix_sdk::Lix<S>, path: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT data FROM lix_file WHERE path = $1 LIMIT 1 OFFSET 0",
            &[Value::Text(path.to_string())],
        )
        .await
        .unwrap();
    black_box(result);
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |raw| {
        raw.parse::<usize>()
            .unwrap_or_else(|_| panic!("{name} must be a non-negative integer"))
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

fn print_v2_transition_counters(label: &str, round: usize, counters: WasmTransitionCounters) {
    eprintln!(
        "plugin_v2_counters label={label} round={round} source_read_calls={} \
         source_bytes_read={} packet_pages={} packet_records={} attachment_reads={} \
         attachment_bytes_read={} component_import_calls={} component_boundary_bytes={} \
         guest_linear_memory_high_water_bytes={} host_full_diff_bytes_compared={} \
         host_full_content_classification_bytes={} \
         full_state_semantic_rows_materialized={} \
         change_payload_requests={} returned_change_payloads={} durable_semantic_changes={} \
         private_document_cache_hits={} shared_renderer_cache_hits={} \
         full_document_reparses={} full_renderer_invocations={} \
         filesystem_sync_full_renders={}",
        counters.source_read_calls,
        counters.source_bytes_read,
        counters.packet_pages,
        counters.packet_records,
        counters.attachment_reads,
        counters.attachment_bytes_read,
        counters.component_import_calls,
        counters.component_boundary_bytes,
        counters.guest_linear_memory_high_water_bytes,
        counters.host_full_diff_bytes_compared,
        counters.host_full_content_classification_bytes,
        counters.full_state_semantic_rows_materialized,
        counters.change_payload_requests,
        counters.returned_change_payloads,
        counters.durable_semantic_changes,
        counters.private_document_cache_hits,
        counters.shared_renderer_cache_hits,
        counters.full_document_reparses,
        counters.full_renderer_invocations,
        counters.filesystem_sync_full_renders,
    );
}

fn assert_warm_single_row_edit_counters(
    round: usize,
    counters: WasmTransitionCounters,
    splice_provenance: bool,
) {
    assert!(
        counters.guest_linear_memory_high_water_bytes > 0,
        "warm CSV edit round {round} did not report guest memory high-water"
    );
    assert!(
        counters.guest_linear_memory_high_water_bytes <= 64 * 1024 * 1024,
        "warm CSV edit round {round} exceeded the 64 MiB guest limit: {} bytes",
        counters.guest_linear_memory_high_water_bytes
    );
    assert_eq!(
        counters.full_state_semantic_rows_materialized, 0,
        "warm CSV edit round {round} materialized the full semantic state"
    );
    if splice_provenance {
        assert_eq!(
            counters.host_full_diff_bytes_compared, 0,
            "warm CSV edit round {round} ignored validated splice provenance"
        );
        assert_eq!(
            counters.host_full_content_classification_bytes, 0,
            "warm CSV edit round {round} rescanned the full payload for content classification"
        );
    }
    assert!(
        counters.change_payload_requests < 64,
        "warm CSV edit round {round} requested {} change payloads (limit: <64)",
        counters.change_payload_requests
    );
    assert!(
        counters.returned_change_payloads < 64,
        "warm CSV edit round {round} returned {} change payloads (limit: <64)",
        counters.returned_change_payloads
    );
    assert_eq!(
        counters.durable_semantic_changes, 1,
        "warm CSV edit round {round} must durably change exactly one semantic row"
    );
    assert_eq!(
        counters.private_document_cache_hits, 1,
        "warm CSV edit round {round} must use exactly one private actor document"
    );
    assert_eq!(
        counters.shared_renderer_cache_hits, 1,
        "warm CSV edit round {round} must use exactly one cached renderer document"
    );
    assert_eq!(
        counters.full_document_reparses, 0,
        "warm CSV edit round {round} performed a full document reparse"
    );
    assert_eq!(
        counters.full_renderer_invocations, 0,
        "warm CSV edit round {round} performed a full renderer invocation"
    );
    assert_eq!(
        counters.filesystem_sync_full_renders, 0,
        "warm CSV edit round {round} triggered a filesystem sync full render"
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

fn flat_json_fixture() -> FlatJsonFixture {
    let mut initial = Vec::with_capacity(JSON_TARGET_BYTE_COUNT);
    let mut rng = SplitMix64(JSON_FIXTURE_SEED);
    let middle = JSON_PROPERTY_COUNT / 2;
    let edited_property = format!("property_{middle:06}");
    let mut edited_value_offset = None;

    initial.push(b'{');
    for index in 0..JSON_PROPERTY_COUNT {
        if index > 0 {
            initial.push(b',');
        }
        let first = rng.next_u64();
        let second = rng.next_u64() as u32;
        write!(
            &mut initial,
            "\"property_{index:06}\":\"{first:016x}{second:08x}"
        )
        .expect("write deterministic JSON property");
        if index == middle {
            edited_value_offset = Some(initial.len() - 24);
        }
        if index < JSON_LONG_VALUE_COUNT {
            initial.push(b'f');
        }
        initial.push(b'"');
    }
    initial.push(b'}');

    assert_eq!(
        initial.len(),
        JSON_TARGET_BYTE_COUNT,
        "flat JSON fixture must remain exactly 10,000,000 bytes"
    );
    let edited_value_offset = edited_value_offset.expect("middle JSON property exists");

    FlatJsonFixture {
        initial,
        edited_value_offset,
        edited_property,
    }
}

fn build_plugin(format: ProfileFormat) -> Vec<u8> {
    match format {
        ProfileFormat::Csv => build_csv_plugin(),
        ProfileFormat::Json => build_json_plugin(),
    }
}

fn build_csv_plugin() -> Vec<u8> {
    // option_env: the bindep artifact env var is absent in some CI target
    // contexts; this harness is only ever run manually, so resolve at
    // runtime and fail with instructions instead of failing the compile.
    let Some(wasm_path) = option_env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_V2_plugin_csv_v2") else {
        eprintln!(
            "CSV plugin wasm path unavailable; build via `cargo build --bench \
             profile_plugin_large_file --features \
             default_wasm_runtime,local_filesystem,__profile_wasm_memory` so cargo provides the \
             bindep artifact"
        );
        std::process::exit(2);
    };
    let wasm = std::fs::read(Path::new(wasm_path)).expect("read bindep-built CSV v2 plugin wasm");
    build_plugin_archive(
        &wasm,
        &[
            (
                "manifest.json",
                include_str!("../../../plugins/csv-v2/manifest.json").as_bytes(),
            ),
            (
                "schema/csv_table.json",
                include_str!("../../../plugins/csv-v2/schema/csv_table.json").as_bytes(),
            ),
            (
                "schema/csv_row.json",
                include_str!("../../../plugins/csv-v2/schema/csv_row.json").as_bytes(),
            ),
        ],
    )
}

fn build_json_plugin() -> Vec<u8> {
    let Some(wasm_path) = option_env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_V2_plugin_json_v2") else {
        eprintln!(
            "JSON plugin wasm path unavailable; build via `cargo build --bench \
             profile_plugin_large_file --features \
             default_wasm_runtime,local_filesystem,__profile_wasm_memory` so cargo provides the \
             bindep artifact"
        );
        std::process::exit(2);
    };
    let wasm = std::fs::read(Path::new(wasm_path)).expect("read bindep-built JSON plugin wasm");
    build_plugin_archive(
        &wasm,
        &[
            (
                "manifest.json",
                include_str!("../../../plugins/json/manifest.json").as_bytes(),
            ),
            (
                "schema/json_pointer.json",
                include_str!("../../../plugins/json/schema/json_pointer.json").as_bytes(),
            ),
        ],
    )
}

fn build_plugin_archive(wasm: &[u8], metadata: &[(&str, &[u8])]) -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for &(path, bytes) in metadata {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.start_file("plugin.wasm", options).unwrap();
    writer.write_all(wasm).unwrap();
    writer.finish().unwrap().into_inner()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_exact_fixture_size_is_preserved() {
        let rows = random_csv_rows("initial", 220_000, 0x8ae7_b4b1_9f4c_d215);
        assert_eq!(csv_bytes_from_rows(&rows).len(), 10_680_000);
    }

    #[test]
    fn json_fixture_is_exact_and_changes_only_the_middle_property() {
        let fixture = flat_json_fixture();
        let edited_bytes = fixture.edited_bytes();
        assert_eq!(fixture.initial.len(), JSON_TARGET_BYTE_COUNT);
        assert_eq!(edited_bytes.len(), JSON_TARGET_BYTE_COUNT);
        assert_eq!(
            fixture
                .initial
                .iter()
                .zip(&edited_bytes)
                .filter(|(before, after)| before != after)
                .count(),
            1
        );

        let initial: serde_json::Value =
            serde_json::from_slice(&fixture.initial).expect("initial fixture is valid JSON");
        let edited: serde_json::Value =
            serde_json::from_slice(&edited_bytes).expect("edited fixture is valid JSON");
        let initial = initial
            .as_object()
            .expect("initial fixture is a flat object");
        let edited = edited.as_object().expect("edited fixture is a flat object");
        assert_eq!(initial.len(), JSON_PROPERTY_COUNT);
        assert_eq!(edited.len(), JSON_PROPERTY_COUNT);

        let changed = initial
            .iter()
            .filter(|(key, before)| edited.get(*key) != Some(*before))
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(changed, [fixture.edited_property.as_str()]);
        assert!(initial.values().all(serde_json::Value::is_string));
        assert!(edited.values().all(serde_json::Value::is_string));
    }

    #[test]
    fn json_lane_packages_the_real_wasm_plugin() {
        let archive = build_json_plugin();
        let mut archive = zip::ZipArchive::new(Cursor::new(archive)).expect("open plugin archive");
        for path in ["manifest.json", "schema/json_pointer.json", "plugin.wasm"] {
            let entry = archive.by_name(path).expect("required archive entry");
            assert!(entry.size() > 0, "archive entry '{path}' must not be empty");
        }
    }

    #[test]
    fn csv_lane_packages_the_v2_component_and_manifest() {
        let archive = build_csv_plugin();
        let mut archive = zip::ZipArchive::new(Cursor::new(archive)).expect("open plugin archive");
        for path in [
            "manifest.json",
            "schema/csv_table.json",
            "schema/csv_row.json",
            "plugin.wasm",
        ] {
            let entry = archive.by_name(path).expect("required archive entry");
            assert!(entry.size() > 0, "archive entry '{path}' must not be empty");
        }
        let mut manifest = String::new();
        use std::io::Read as _;
        archive
            .by_name("manifest.json")
            .unwrap()
            .read_to_string(&mut manifest)
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_str(&manifest).unwrap();
        assert_eq!(manifest["key"], "plugin_csv_v2");
        assert_eq!(manifest["runtime"], "wasm-component-v2");
        assert_eq!(manifest["api_version"], "2.0.0");
    }
}
