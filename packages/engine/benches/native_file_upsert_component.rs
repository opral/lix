//! Direct native file-upsert component benchmark for the normal filesystem
//! layout. This deliberately excludes SQL parsing, server framing, and
//! metadata changes: it isolates the cost of ten commits versus one commit
//! for the same ten file writes.
//!
//! The benchmark builds one immutable, closed source database per backend and
//! configuration. Every sample copies that source before opening it, so all
//! timed operations start from the same file and commit cardinality without
//! paying the 5k-history setup cost repeatedly.
//!
//! Environment variables:
//! - `LIX_NATIVE_FILE_COMPONENT_FILE_COUNT` (default: 5000)
//! - `LIX_NATIVE_FILE_COMPONENT_HISTORY_COMMITS` (default: 5000)
//! - `LIX_NATIVE_FILE_COMPONENT_PAIRS` (default/minimum: 30)
//! - `LIX_NATIVE_FILE_COMPONENT_SEED_BATCH_SIZE` (default: 1000)
//! - `LIX_NATIVE_FILE_COMPONENT_BACKENDS` (default: `rocksdb,slatedb`)

use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use lix_engine::{Blob, Engine, SessionContext, Storage};
use lix_rocksdb_storage::RocksDB;
use lix_slatedb_storage::SlateDB;
use tempfile::TempDir;

const DEFAULT_FILE_COUNT: usize = 5_000;
const DEFAULT_HISTORY_COMMITS: usize = 5_000;
const DEFAULT_PAIRS: usize = 30;
const DEFAULT_SEED_BATCH_SIZE: usize = 1_000;
const MEASURED_FILE_COUNT: usize = 10;
const TEXT_FILE_BYTES: usize = 4 * 1024;
const PDF_FILE_BYTES: usize = 64 * 1024;
const PNG_FILE_BYTES: usize = 32 * 1024;
const BINARY_FILE_BYTES: usize = 256 * 1024;
// Coprime with the 100 and 500 tiles used by the 1k and 5k corpus screens.
const PREFERRED_TILE_PERMUTATION_STRIDE: usize = 37;
// A two-sided 99% Student-t critical value with 29 degrees of freedom. Keeping
// this value for more than thirty pairs is conservative.
const T_99_TWO_SIDED_DF_29: f64 = 2.756;

const FILE_KINDS: [FileKind; MEASURED_FILE_COUNT] = [
    FileKind::new("json", "json"),
    FileKind::new("csv", "csv"),
    FileKind::new("markdown", "md"),
    FileKind::new("text", "txt"),
    FileKind::new("xml", "xml"),
    FileKind::new("yaml", "yaml"),
    FileKind::new("pdf", "pdf"),
    FileKind::new("png", "png"),
    FileKind::new("binary", "bin"),
    FileKind::new("logs", "log"),
];

#[derive(Clone, Copy)]
struct FileKind {
    directory: &'static str,
    extension: &'static str,
}

impl FileKind {
    const fn new(directory: &'static str, extension: &'static str) -> Self {
        Self {
            directory,
            extension,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Backend {
    RocksDb,
    SlateDb,
}

impl Backend {
    const fn label(self) -> &'static str {
        match self {
            Self::RocksDb => "rocksdb",
            Self::SlateDb => "slatedb",
        }
    }
}

#[derive(Clone, Copy)]
enum Operation {
    TenSingles,
    OneBatch,
}

impl Operation {
    const fn label(self) -> &'static str {
        match self {
            Self::TenSingles => "10_native_single_upserts",
            Self::OneBatch => "1_native_10_entry_batch",
        }
    }
}

#[derive(Clone, Debug)]
struct Config {
    file_count: usize,
    history_commits: usize,
    pairs: usize,
    seed_batch_size: usize,
}

impl Config {
    fn from_env() -> Self {
        let file_count = env_usize("LIX_NATIVE_FILE_COMPONENT_FILE_COUNT", DEFAULT_FILE_COUNT);
        let history_commits = env_usize(
            "LIX_NATIVE_FILE_COMPONENT_HISTORY_COMMITS",
            DEFAULT_HISTORY_COMMITS,
        );
        let pairs = env_usize("LIX_NATIVE_FILE_COMPONENT_PAIRS", DEFAULT_PAIRS);
        let seed_batch_size = env_usize(
            "LIX_NATIVE_FILE_COMPONENT_SEED_BATCH_SIZE",
            DEFAULT_SEED_BATCH_SIZE,
        );

        assert!(
            file_count >= MEASURED_FILE_COUNT * 2 && file_count.is_multiple_of(MEASURED_FILE_COUNT),
            "LIX_NATIVE_FILE_COMPONENT_FILE_COUNT must be a multiple of {MEASURED_FILE_COUNT} and at least {}",
            MEASURED_FILE_COUNT * 2
        );
        assert!(
            history_commits >= 2,
            "LIX_NATIVE_FILE_COMPONENT_HISTORY_COMMITS must leave one commit for per-clone warmup"
        );
        assert!(
            pairs >= DEFAULT_PAIRS,
            "LIX_NATIVE_FILE_COMPONENT_PAIRS must be at least {DEFAULT_PAIRS} for a 99% CI"
        );
        assert!(
            seed_batch_size > 0,
            "LIX_NATIVE_FILE_COMPONENT_SEED_BATCH_SIZE must be greater than zero"
        );

        Self {
            file_count,
            history_commits,
            pairs,
            seed_batch_size: seed_batch_size.min(file_count),
        }
    }
}

#[derive(Clone, Copy)]
struct MixedFileTile {
    tile: usize,
    index_base: usize,
    payload_seed: u64,
}

impl MixedFileTile {
    const fn index_end_exclusive(self) -> usize {
        self.index_base + MEASURED_FILE_COUNT
    }
}

#[derive(Clone, Copy)]
struct TileSelection {
    warmup: MixedFileTile,
    timed: MixedFileTile,
    permutation_stride: usize,
    timed_offset: usize,
}

struct Seed {
    backend: Backend,
    // The source database must outlive every clone. It stays closed after
    // creation, which makes copying RocksDB's WAL/SST set and SlateDB's local
    // object-store tree safe.
    _root: TempDir,
    source_path: PathBuf,
    main_branch_id: String,
    source_commit_count: usize,
    source_usage: DiskUsage,
    next_clone: AtomicU64,
}

impl Seed {
    async fn create(backend: Backend, config: &Config) -> Self {
        let root = tempfile::tempdir().expect("create native component benchmark root");
        let source_path = root.path().join("source");
        let main_branch_id = match backend {
            Backend::RocksDb => {
                let storage = RocksDB::open(&source_path).expect("open native component RocksDB");
                let branch_id = seed_repository(storage.clone(), config).await;
                storage
                    .flush()
                    .expect("flush native component RocksDB source");
                branch_id
            }
            Backend::SlateDb => {
                let storage = SlateDB::open(&source_path).expect("open native component SlateDB");
                let branch_id = seed_repository(storage.clone(), config).await;
                storage
                    .flush()
                    .await
                    .expect("flush native component SlateDB source");
                branch_id
            }
        };

        // Each match arm above drops its last storage handle before this
        // recursive copy source is ever used. SlateDB's worker Drop closes the
        // DB synchronously; RocksDB's weak open registry no longer owns a DB.
        let source_usage = disk_usage(&source_path).expect("account native component source");
        Self {
            backend,
            _root: root,
            source_path,
            main_branch_id,
            source_commit_count: config.history_commits - 1,
            source_usage,
            next_clone: AtomicU64::new(0),
        }
    }

    async fn fork(&self) -> Fixture {
        let clone_number = self.next_clone.fetch_add(1, Ordering::Relaxed);
        let clone_dir = tempfile::tempdir_in(self._root.path())
            .expect("create native component benchmark clone directory");
        let database_path = clone_dir.path().join(format!("database-{clone_number:04}"));
        copy_directory(&self.source_path, &database_path)
            .expect("copy closed native component benchmark source");

        match self.backend {
            Backend::RocksDb => {
                let storage =
                    RocksDB::open(&database_path).expect("open copied native component RocksDB");
                Fixture::RocksDb(
                    open_fixture(
                        storage,
                        self.main_branch_id.clone(),
                        clone_dir,
                        database_path,
                    )
                    .await,
                )
            }
            Backend::SlateDb => {
                let storage =
                    SlateDB::open(&database_path).expect("open copied native component SlateDB");
                Fixture::SlateDb(
                    open_fixture(
                        storage,
                        self.main_branch_id.clone(),
                        clone_dir,
                        database_path,
                    )
                    .await,
                )
            }
        }
    }
}

struct ComponentFixture<S: Storage> {
    // Keep storage before the TempDir in declaration order: SessionContext and
    // storage drop before the clone directory is removed.
    session: SessionContext<S>,
    storage: S,
    database_path: PathBuf,
    _clone_dir: TempDir,
}

impl<S> ComponentFixture<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn native_batch(&self, writes: Vec<(String, Blob)>) -> u64 {
        self.session
            .upsert_file_data_batch(writes)
            .await
            .expect("native component batch upsert")
    }

    async fn native_sequential(&self, writes: Vec<(String, Blob)>) -> u64 {
        let mut rows_affected = 0;
        for (path, data) in writes {
            rows_affected += self
                .session
                .upsert_file_data(path, data)
                .await
                .expect("native component sequential upsert");
        }
        rows_affected
    }

    async fn visible_commit_count(&self) -> usize {
        count_rows(&self.session, "SELECT COUNT(*) AS count FROM lix_commit").await
    }

    async fn visible_file_count(&self) -> usize {
        count_rows(&self.session, "SELECT COUNT(*) AS count FROM lix_file").await
    }

    fn disk_usage(&self) -> DiskUsage {
        disk_usage(&self.database_path).expect("account native component clone")
    }
}

enum Fixture {
    RocksDb(ComponentFixture<RocksDB>),
    SlateDb(ComponentFixture<SlateDB>),
}

impl Fixture {
    async fn native_batch(&self, writes: Vec<(String, Blob)>) -> u64 {
        match self {
            Self::RocksDb(fixture) => fixture.native_batch(writes).await,
            Self::SlateDb(fixture) => fixture.native_batch(writes).await,
        }
    }

    async fn native_sequential(&self, writes: Vec<(String, Blob)>) -> u64 {
        match self {
            Self::RocksDb(fixture) => fixture.native_sequential(writes).await,
            Self::SlateDb(fixture) => fixture.native_sequential(writes).await,
        }
    }

    async fn visible_commit_count(&self) -> usize {
        match self {
            Self::RocksDb(fixture) => fixture.visible_commit_count().await,
            Self::SlateDb(fixture) => fixture.visible_commit_count().await,
        }
    }

    async fn visible_file_count(&self) -> usize {
        match self {
            Self::RocksDb(fixture) => fixture.visible_file_count().await,
            Self::SlateDb(fixture) => fixture.visible_file_count().await,
        }
    }

    async fn flush(&self) {
        match self {
            Self::RocksDb(fixture) => fixture
                .storage
                .flush()
                .expect("flush native component RocksDB clone"),
            Self::SlateDb(fixture) => fixture
                .storage
                .flush()
                .await
                .expect("flush native component SlateDB clone"),
        }
    }

    fn disk_usage(&self) -> DiskUsage {
        match self {
            Self::RocksDb(fixture) => fixture.disk_usage(),
            Self::SlateDb(fixture) => fixture.disk_usage(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
struct DiskUsage {
    total_bytes: u64,
    sst_bytes: u64,
    log_bytes: u64,
    manifest_bytes: u64,
    options_bytes: u64,
    other_bytes: u64,
    file_count: u64,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
struct DiskDelta {
    total_bytes: i64,
    sst_bytes: i64,
    log_bytes: i64,
    manifest_bytes: i64,
    options_bytes: i64,
    other_bytes: i64,
    file_count: i64,
}

impl DiskUsage {
    fn delta_from(self, before: Self) -> DiskDelta {
        DiskDelta {
            total_bytes: signed_delta(self.total_bytes, before.total_bytes),
            sst_bytes: signed_delta(self.sst_bytes, before.sst_bytes),
            log_bytes: signed_delta(self.log_bytes, before.log_bytes),
            manifest_bytes: signed_delta(self.manifest_bytes, before.manifest_bytes),
            options_bytes: signed_delta(self.options_bytes, before.options_bytes),
            other_bytes: signed_delta(self.other_bytes, before.other_bytes),
            file_count: signed_delta(self.file_count, before.file_count),
        }
    }
}

#[derive(Clone, Debug)]
struct Sample {
    accepted_ns: u64,
    flush_ns: u64,
    flush_complete_ns: u64,
    storage_before: DiskUsage,
    storage_after: DiskUsage,
}

impl Sample {
    fn storage_delta(&self) -> DiskDelta {
        self.storage_after.delta_from(self.storage_before)
    }
}

struct PairResult {
    sequential: Sample,
    batch: Sample,
}

fn main() {
    let config = Config::from_env();
    let backends = selected_backends();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create native component benchmark runtime");

    runtime.block_on(async {
        for backend in backends {
            run_backend(backend, &config).await;
        }
    });
}

async fn run_backend(backend: Backend, config: &Config) {
    let seed = Seed::create(backend, config).await;
    println!(
        "{}",
        serde_json::json!({
            "benchmark": "native_file_upsert_component",
            "event": "source_seed",
            "backend": backend.label(),
            "file_count": config.file_count,
            "history_commits_at_measurement_start": config.history_commits,
            "source_commit_count_before_clone_warmup": seed.source_commit_count,
            "seed_batch_size": config.seed_batch_size,
            "measured_batch_bytes": measured_batch_bytes(),
            "source_storage": seed.source_usage,
        })
    );

    let mut pairs = Vec::with_capacity(config.pairs);
    for pair_index in 0..config.pairs {
        let sequential_first = pair_index % 2 == 0;
        let (sequential, batch) = if sequential_first {
            (
                run_sample(
                    &seed,
                    config,
                    pair_index,
                    sequential_first,
                    Operation::TenSingles,
                )
                .await,
                run_sample(
                    &seed,
                    config,
                    pair_index,
                    sequential_first,
                    Operation::OneBatch,
                )
                .await,
            )
        } else {
            let batch = run_sample(
                &seed,
                config,
                pair_index,
                sequential_first,
                Operation::OneBatch,
            )
            .await;
            let sequential = run_sample(
                &seed,
                config,
                pair_index,
                sequential_first,
                Operation::TenSingles,
            )
            .await;
            (sequential, batch)
        };
        pairs.push(PairResult { sequential, batch });
    }

    print_summary(backend, config, &pairs);
}

async fn run_sample(
    seed: &Seed,
    config: &Config,
    pair_index: usize,
    sequential_first: bool,
    operation: Operation,
) -> Sample {
    let fixture = seed.fork().await;

    // This is an intentionally direct, parser-free warmup. The source starts
    // at history - 1 so this one commit leaves the timed request at exactly the
    // configured history depth on every clone.
    let selection = selected_tiles(config, pair_index);
    let warmup_tile = selection.warmup;
    let timed_tile = selection.timed;
    let warmup_rows = fixture.native_batch(tile_entries(warmup_tile)).await;
    assert_eq!(warmup_rows, MEASURED_FILE_COUNT as u64);
    // Drain the warmup before establishing the storage baseline. Otherwise
    // its WAL/SST publication would be incorrectly attributed to the timed
    // operation's storage delta.
    fixture.flush().await;

    let storage_before = fixture.disk_usage();
    // Prepare corpus paths and bytes before the timer. No SQL, framing,
    // metadata construction, or input allocation is inside the timed region.
    let writes = tile_entries(timed_tile);
    let accepted_started = Instant::now();
    let rows_affected = match operation {
        Operation::TenSingles => fixture.native_sequential(writes).await,
        Operation::OneBatch => fixture.native_batch(writes).await,
    };
    let accepted_ns = duration_ns(accepted_started.elapsed());
    assert_eq!(rows_affected, MEASURED_FILE_COUNT as u64);
    black_box(rows_affected);

    // Flush-complete latency is reported separately from accepted latency. It
    // covers this backend's explicit flush completion, not a claim about crash
    // durability under every deployment configuration.
    let flush_started = Instant::now();
    fixture.flush().await;
    let flush_ns = duration_ns(flush_started.elapsed());
    let storage_after = fixture.disk_usage();
    let flush_complete_ns = accepted_ns
        .checked_add(flush_ns)
        .expect("accepted and flush benchmark durations must fit u64 nanoseconds");

    let sample = Sample {
        accepted_ns,
        flush_ns,
        flush_complete_ns,
        storage_before,
        storage_after,
    };
    let storage_delta = sample.storage_delta();

    // Validate the clone only after timing and disk accounting. The known
    // direct-write cardinality proves that its warmed timed operation began at
    // the configured history depth without preheating metadata before timing.
    let expected_commit_count = config
        .history_commits
        .checked_add(match operation {
            Operation::TenSingles => MEASURED_FILE_COUNT,
            Operation::OneBatch => 1,
        })
        .expect("post-timing commit count must fit usize");
    let post_timing_commit_count = fixture.visible_commit_count().await;
    let post_timing_file_count = fixture.visible_file_count().await;
    assert_eq!(
        post_timing_commit_count, expected_commit_count,
        "every clone must preserve the configured history plus its known direct-write count"
    );
    assert_eq!(
        post_timing_file_count, config.file_count,
        "timed upserts must not alter the mixed corpus file count"
    );
    drop(fixture);

    println!(
        "{}",
        serde_json::json!({
            "benchmark": "native_file_upsert_component",
            "event": "sample",
            "backend": seed.backend.label(),
            "pair": pair_index,
            "order": if sequential_first { "sequential_then_batch" } else { "batch_then_sequential" },
            "operation": operation.label(),
            "file_count": config.file_count,
            "history_commits_at_start": config.history_commits,
            "measured_batch_bytes": measured_batch_bytes(),
            "tile_permutation_stride": selection.permutation_stride,
            "timed_tile_offset": selection.timed_offset,
            "warmup_tile": warmup_tile.tile,
            "warmup_index_base": warmup_tile.index_base,
            "warmup_index_end_exclusive": warmup_tile.index_end_exclusive(),
            "warmup_payload_seed": warmup_tile.payload_seed,
            "timed_tile": timed_tile.tile,
            "timed_index_base": timed_tile.index_base,
            "timed_index_end_exclusive": timed_tile.index_end_exclusive(),
            "timed_payload_seed": timed_tile.payload_seed,
            "accepted_ns": sample.accepted_ns,
            "flush_ns": sample.flush_ns,
            "flush_complete_ns": sample.flush_complete_ns,
            "post_timing_commit_count": post_timing_commit_count,
            "post_timing_file_count": post_timing_file_count,
            "post_timing_state_verified": true,
            "storage_before": sample.storage_before,
            "storage_after": sample.storage_after,
            "storage_delta": storage_delta,
        })
    );
    sample
}

fn print_summary(backend: Backend, config: &Config, pairs: &[PairResult]) {
    let mut sequential_accepted = pairs
        .iter()
        .map(|pair| pair.sequential.accepted_ns)
        .collect::<Vec<_>>();
    let mut batch_accepted = pairs
        .iter()
        .map(|pair| pair.batch.accepted_ns)
        .collect::<Vec<_>>();
    let mut sequential_flush = pairs
        .iter()
        .map(|pair| pair.sequential.flush_ns)
        .collect::<Vec<_>>();
    let mut batch_flush = pairs
        .iter()
        .map(|pair| pair.batch.flush_ns)
        .collect::<Vec<_>>();
    let mut sequential_flush_complete = pairs
        .iter()
        .map(|pair| pair.sequential.flush_complete_ns)
        .collect::<Vec<_>>();
    let mut batch_flush_complete = pairs
        .iter()
        .map(|pair| pair.batch.flush_complete_ns)
        .collect::<Vec<_>>();
    let mut sequential_storage_delta = pairs
        .iter()
        .map(|pair| pair.sequential.storage_delta().total_bytes)
        .collect::<Vec<_>>();
    let mut batch_storage_delta = pairs
        .iter()
        .map(|pair| pair.batch.storage_delta().total_bytes)
        .collect::<Vec<_>>();

    let accepted_speedup = paired_speedup(pairs, |sample| sample.accepted_ns);
    let flush_complete_speedup = paired_speedup(pairs, |sample| sample.flush_complete_ns);

    println!(
        "{}",
        serde_json::json!({
            "benchmark": "native_file_upsert_component",
            "event": "summary",
            "backend": backend.label(),
            "file_count": config.file_count,
            "history_commits_at_start": config.history_commits,
            "pairs": pairs.len(),
            "confidence_level": 0.99,
            "comparison": "10 direct native single-file commits / 1 direct native ten-file batch commit",
            "sequential_accepted_p50_ns": percentile(&mut sequential_accepted, 50),
            "sequential_accepted_p95_ns": percentile(&mut sequential_accepted, 95),
            "sequential_accepted_mean_ns": mean_u64(&sequential_accepted),
            "batch_accepted_p50_ns": percentile(&mut batch_accepted, 50),
            "batch_accepted_p95_ns": percentile(&mut batch_accepted, 95),
            "batch_accepted_mean_ns": mean_u64(&batch_accepted),
            "sequential_flush_p50_ns": percentile(&mut sequential_flush, 50),
            "batch_flush_p50_ns": percentile(&mut batch_flush, 50),
            "sequential_flush_complete_p50_ns": percentile(&mut sequential_flush_complete, 50),
            "sequential_flush_complete_p95_ns": percentile(&mut sequential_flush_complete, 95),
            "sequential_flush_complete_mean_ns": mean_u64(&sequential_flush_complete),
            "batch_flush_complete_p50_ns": percentile(&mut batch_flush_complete, 50),
            "batch_flush_complete_p95_ns": percentile(&mut batch_flush_complete, 95),
            "batch_flush_complete_mean_ns": mean_u64(&batch_flush_complete),
            "sequential_median_storage_total_delta_bytes": median_i64(&mut sequential_storage_delta),
            "batch_median_storage_total_delta_bytes": median_i64(&mut batch_storage_delta),
            "accepted_paired_geometric_mean_speedup": accepted_speedup.geometric_mean,
            "accepted_paired_99_lower_speedup": accepted_speedup.lower_99,
            "accepted_paired_99_upper_speedup": accepted_speedup.upper_99,
            "accepted_paired_99_lower_latency_reduction_percent": (1.0 - 1.0 / accepted_speedup.lower_99) * 100.0,
            "accepted_qualifies_more_than_20_percent": accepted_speedup.lower_99 > 1.25,
            "flush_complete_definition": "accepted_ns + post_operation_flush_ns; not a crash-durability claim",
            "flush_complete_paired_geometric_mean_speedup": flush_complete_speedup.geometric_mean,
            "flush_complete_paired_99_lower_speedup": flush_complete_speedup.lower_99,
            "flush_complete_paired_99_upper_speedup": flush_complete_speedup.upper_99,
            "flush_complete_paired_99_lower_latency_reduction_percent": (1.0 - 1.0 / flush_complete_speedup.lower_99) * 100.0,
            "flush_complete_qualifies_more_than_20_percent": flush_complete_speedup.lower_99 > 1.25,
            "qualifies_more_than_20_percent": flush_complete_speedup.lower_99 > 1.25,
        })
    );
}

struct PairedSpeedup {
    geometric_mean: f64,
    lower_99: f64,
    upper_99: f64,
}

fn paired_speedup(pairs: &[PairResult], value: impl Fn(&Sample) -> u64) -> PairedSpeedup {
    let log_speedups = pairs
        .iter()
        .map(|pair| {
            let sequential = value(&pair.sequential).max(1) as f64;
            let batch = value(&pair.batch).max(1) as f64;
            (sequential / batch).ln()
        })
        .collect::<Vec<_>>();
    let mean_log_speedup = mean(&log_speedups);
    let standard_error =
        sample_standard_deviation(&log_speedups) / (log_speedups.len() as f64).sqrt();
    PairedSpeedup {
        geometric_mean: mean_log_speedup.exp(),
        lower_99: (mean_log_speedup - T_99_TWO_SIDED_DF_29 * standard_error).exp(),
        upper_99: (mean_log_speedup + T_99_TWO_SIDED_DF_29 * standard_error).exp(),
    }
}

async fn seed_repository<S>(storage: S, config: &Config) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let init = Engine::initialize(storage.clone())
        .await
        .expect("initialize native component source repository");
    let engine = Engine::new(storage)
        .await
        .expect("open native component source engine");
    let session = engine
        .open_session(init.main_branch_id.clone())
        .await
        .expect("open native component source session");

    seed_files(&session, config).await;
    assert_eq!(
        count_rows(&session, "SELECT COUNT(*) AS count FROM lix_file").await,
        config.file_count,
        "the source corpus must contain the requested number of files"
    );

    // The per-clone direct batch warmup supplies the final commit so the timed
    // operation starts at exactly `history_commits` rather than history + 1.
    let target_before_clone_warmup = config.history_commits - 1;
    let mut commit_count = count_rows(&session, "SELECT COUNT(*) AS count FROM lix_commit").await;
    assert!(
        commit_count <= target_before_clone_warmup,
        "history target {target_before_clone_warmup} is too small for {} seed commits",
        commit_count
    );

    while commit_count < target_before_clone_warmup {
        // Rotate normal-sized writes across the mixed corpus. A one-byte
        // history anchor would understate the changelog/blob layout and skew
        // the later 5k-history component screen toward a pathological case.
        let file_index = commit_count % config.file_count;
        let rows_affected = session
            .upsert_file_data(
                corpus_path(file_index),
                Blob::from(corpus_payload(file_index, commit_count as u64)),
            )
            .await
            .expect("append direct native mixed-corpus history commit");
        assert_eq!(rows_affected, 1);
        commit_count += 1;
    }

    assert_eq!(
        count_rows(&session, "SELECT COUNT(*) AS count FROM lix_commit").await,
        target_before_clone_warmup,
        "source history must match the configured pre-warmup commit count"
    );
    init.main_branch_id
}

async fn seed_files<S>(session: &SessionContext<S>, config: &Config)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    for chunk_start in (0..config.file_count).step_by(config.seed_batch_size) {
        let chunk_end = (chunk_start + config.seed_batch_size).min(config.file_count);
        let writes = (chunk_start..chunk_end)
            .map(|file_index| {
                (
                    corpus_path(file_index),
                    Blob::from(corpus_payload(file_index, 0)),
                )
            })
            .collect::<Vec<_>>();
        let rows_affected = session
            .upsert_file_data_batch(writes)
            .await
            .expect("seed direct native file batch");
        assert_eq!(rows_affected, (chunk_end - chunk_start) as u64);
    }
}

async fn open_fixture<S>(
    storage: S,
    main_branch_id: String,
    clone_dir: TempDir,
    database_path: PathBuf,
) -> ComponentFixture<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let engine = Engine::new(storage.clone())
        .await
        .expect("open native component clone engine");
    let session = engine
        .open_session(main_branch_id)
        .await
        .expect("open native component clone session");
    ComponentFixture {
        session,
        storage,
        database_path,
        _clone_dir: clone_dir,
    }
}

async fn count_rows<S>(session: &SessionContext<S>, sql: &str) -> usize
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = session
        .execute(sql, &[])
        .await
        .expect("count benchmark rows");
    let row = result
        .rows()
        .first()
        .expect("count benchmark query should return a row");
    let count = row.get::<i64>("count").expect("decode benchmark row count");
    usize::try_from(count).expect("benchmark row count must be non-negative")
}

fn selected_tiles(config: &Config, pair_index: usize) -> TileSelection {
    let tile_count = config.file_count / MEASURED_FILE_COUNT;
    let permutation_stride = coprime_tile_stride(tile_count);
    let warmup_tile = pair_index.wrapping_mul(permutation_stride) % tile_count;
    // Split the paired sets across the corpus while retaining a deterministic
    // one-of-each-type tile. For 100 and 500 tiles this gives 30 samples broad
    // coverage rather than a prefix-only walk.
    let timed_offset = (tile_count / 2).max(1);
    let timed_tile = (warmup_tile + timed_offset) % tile_count;
    assert_ne!(
        warmup_tile, timed_tile,
        "configured corpus must leave a timed mixed tile disjoint from warmup"
    );

    TileSelection {
        warmup: MixedFileTile {
            tile: warmup_tile,
            index_base: warmup_tile * MEASURED_FILE_COUNT,
            payload_seed: (pair_index as u64).wrapping_mul(2).wrapping_add(1),
        },
        timed: MixedFileTile {
            tile: timed_tile,
            index_base: timed_tile * MEASURED_FILE_COUNT,
            payload_seed: (pair_index as u64).wrapping_mul(2).wrapping_add(2),
        },
        permutation_stride,
        timed_offset,
    }
}

fn coprime_tile_stride(tile_count: usize) -> usize {
    assert!(
        tile_count >= 2,
        "tile selection needs warmup and timed tiles"
    );
    let mut stride = PREFERRED_TILE_PERMUTATION_STRIDE % tile_count;
    if stride == 0 {
        stride = tile_count - 1;
    }
    while greatest_common_divisor(stride, tile_count) != 1 {
        stride -= 1;
    }
    stride
}

fn greatest_common_divisor(mut left: usize, mut right: usize) -> usize {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn tile_entries(tile: MixedFileTile) -> Vec<(String, Blob)> {
    (tile.index_base..tile.index_end_exclusive())
        .map(|file_index| {
            (
                corpus_path(file_index),
                Blob::from(corpus_payload(file_index, tile.payload_seed)),
            )
        })
        .collect()
}

fn corpus_path(file_index: usize) -> String {
    let kind = FILE_KINDS[file_index % FILE_KINDS.len()];
    format!(
        "/corpus/{}/file-{file_index:05}.{}",
        kind.directory, kind.extension
    )
}

fn corpus_payload(file_index: usize, version: u64) -> Vec<u8> {
    let kind = FILE_KINDS[file_index % FILE_KINDS.len()];
    let payload_bytes = corpus_payload_bytes(file_index);
    match kind.extension {
        "pdf" => binary_payload(
            payload_bytes,
            b"%PDF-1.7\n%lix-native-benchmark\n",
            file_index,
            version,
        ),
        "png" => binary_payload(
            payload_bytes,
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR",
            file_index,
            version,
        ),
        "bin" => binary_payload(payload_bytes, b"LIX\x00BINARY\x01", file_index, version),
        "json" => text_payload(
            payload_bytes,
            &format!(
                "{{\"kind\":\"json\",\"file\":{file_index},\"version\":{version},\"payload\":\""
            ),
            "xYz0123456789",
            "\"}\n",
        ),
        "csv" => text_payload(
            payload_bytes,
            &format!("file,version,value\n{file_index},{version},"),
            "csv-value-0123456789,",
            "\n",
        ),
        "md" => text_payload(
            payload_bytes,
            &format!("# Benchmark {file_index}\n\nversion: {version}\n\n"),
            "lix native batch markdown content ",
            "\n",
        ),
        "txt" => text_payload(
            payload_bytes,
            &format!("file={file_index} version={version}\n"),
            "plain-text-content-0123456789 ",
            "\n",
        ),
        "xml" => text_payload(
            payload_bytes,
            &format!("<file id=\"{file_index}\" version=\"{version}\">"),
            "xml-content-0123456789",
            "</file>\n",
        ),
        "yaml" => text_payload(
            payload_bytes,
            &format!("file: {file_index}\nversion: {version}\nvalue: "),
            "yaml-content-0123456789",
            "\n",
        ),
        "log" => text_payload(
            payload_bytes,
            &format!("INFO file={file_index} version={version} message="),
            "native-upsert-log-0123456789 ",
            "\n",
        ),
        extension => panic!("unsupported benchmark extension {extension}"),
    }
}

fn corpus_payload_bytes(file_index: usize) -> usize {
    match FILE_KINDS[file_index % FILE_KINDS.len()].extension {
        "pdf" => PDF_FILE_BYTES,
        "png" => PNG_FILE_BYTES,
        "bin" => BINARY_FILE_BYTES,
        _ => TEXT_FILE_BYTES,
    }
}

fn measured_batch_bytes() -> usize {
    (0..MEASURED_FILE_COUNT).map(corpus_payload_bytes).sum()
}

fn text_payload(target_bytes: usize, prefix: &str, repeated: &str, suffix: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(target_bytes);
    bytes.extend_from_slice(prefix.as_bytes());
    while bytes.len() + suffix.len() < target_bytes {
        bytes.extend_from_slice(repeated.as_bytes());
    }
    bytes.truncate(target_bytes.saturating_sub(suffix.len()));
    bytes.extend_from_slice(suffix.as_bytes());
    bytes
}

fn binary_payload(target_bytes: usize, prefix: &[u8], file_index: usize, version: u64) -> Vec<u8> {
    let mut bytes = vec![0; target_bytes];
    let prefix_len = prefix.len().min(bytes.len());
    bytes[..prefix_len].copy_from_slice(&prefix[..prefix_len]);
    let mut state = (file_index as u64)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(version)
        .wrapping_add(0xd1b5_4a32_d192_ed03);
    for byte in &mut bytes[prefix_len..] {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    bytes
}

fn copy_directory(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_directory(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            fs::copy(source_path, destination_path)?;
        } else {
            return Err(std::io::Error::other(format!(
                "unsupported source entry {} while copying benchmark database",
                source_path.display()
            )));
        }
    }
    Ok(())
}

fn disk_usage(path: &Path) -> std::io::Result<DiskUsage> {
    let mut usage = DiskUsage::default();
    accumulate_disk_usage(path, &mut usage)?;
    Ok(usage)
}

fn accumulate_disk_usage(path: &Path, usage: &mut DiskUsage) -> std::io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            accumulate_disk_usage(&entry.path(), usage)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        let bytes = entry.metadata()?.len();
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        usage.total_bytes += bytes;
        usage.file_count += 1;
        if file_name.ends_with(".sst") {
            usage.sst_bytes += bytes;
        } else if file_name.ends_with(".log") || file_name.starts_with("LOG") {
            usage.log_bytes += bytes;
        } else if file_name.starts_with("MANIFEST") {
            usage.manifest_bytes += bytes;
        } else if file_name.starts_with("OPTIONS") {
            usage.options_bytes += bytes;
        } else {
            usage.other_bytes += bytes;
        }
    }
    Ok(())
}

fn selected_backends() -> Vec<Backend> {
    let requested = std::env::var("LIX_NATIVE_FILE_COMPONENT_BACKENDS")
        .unwrap_or_else(|_| "rocksdb,slatedb".to_string());
    let mut selected = Vec::new();
    for value in requested
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let backend = match value {
            "rocksdb" => Backend::RocksDb,
            "slatedb" => Backend::SlateDb,
            other => panic!(
                "unsupported LIX_NATIVE_FILE_COMPONENT_BACKENDS value {other:?}; expected rocksdb or slatedb"
            ),
        };
        if !selected.contains(&backend) {
            selected.push(backend);
        }
    }
    assert!(
        !selected.is_empty(),
        "LIX_NATIVE_FILE_COMPONENT_BACKENDS must select at least one backend"
    );
    selected
}

fn env_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .unwrap_or_else(|error| panic!("parse {name}={value:?} as usize: {error}")),
        Err(_) => default,
    }
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("benchmark duration must fit u64 nanoseconds")
}

fn signed_delta(after: u64, before: u64) -> i64 {
    let after = i128::from(after);
    let before = i128::from(before);
    i64::try_from(after - before).expect("benchmark storage delta must fit i64")
}

fn percentile(values: &mut [u64], percentile: usize) -> u64 {
    assert!(!values.is_empty(), "benchmark percentile requires samples");
    values.sort_unstable();
    let rank = values.len().saturating_mul(percentile).div_ceil(100);
    values[rank.saturating_sub(1).min(values.len() - 1)]
}

fn median_i64(values: &mut [i64]) -> i64 {
    assert!(!values.is_empty(), "benchmark median requires samples");
    values.sort_unstable();
    values[(values.len() - 1) / 2]
}

fn mean(values: &[f64]) -> f64 {
    assert!(!values.is_empty(), "benchmark mean requires samples");
    values.iter().sum::<f64>() / values.len() as f64
}

fn mean_u64(values: &[u64]) -> f64 {
    assert!(!values.is_empty(), "benchmark mean requires samples");
    values.iter().map(|value| *value as f64).sum::<f64>() / values.len() as f64
}

fn sample_standard_deviation(values: &[f64]) -> f64 {
    assert!(
        values.len() > 1,
        "benchmark standard deviation requires multiple samples"
    );
    let mean = mean(values);
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64;
    variance.sqrt()
}
