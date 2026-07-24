//! Direct native-file-read component benchmark.
//!
//! This compares the normal exact SQL read (`SELECT data FROM lix_file WHERE
//! path = $1`) with [`SessionContext::read_file_data`]. Both arms return a
//! `Blob` before the timer stops: the comparison therefore includes SQL result
//! materialization and extraction, but deliberately excludes HTTP framing and
//! JSON/Base64 serialization, which belong to the end-to-end benchmark.
//!
//! Each sample copies a closed, immutable source database before opening it.
//! That keeps every SQL/native pair at the same file and history cardinality
//! without charging corpus/history construction to the timed read. A direct
//! warmup uses a *different* path in the same payload class and the same arm
//! as the timed request.
//!
//! The default is a diagnostic matrix. For a qualification run use at least
//! thirty pairs, for example:
//!
//! ```text
//! LIX_NATIVE_FILE_READ_COMPONENT_FILE_COUNTS=1000,5000 \
//! LIX_NATIVE_FILE_READ_COMPONENT_HISTORY_COMMITS=100,1000,5000 \
//! LIX_NATIVE_FILE_READ_COMPONENT_PAIRS=30 \
//! cargo bench -p lix_engine --bench native_file_read_component \
//!   --features storage-benches,slatedb
//! ```
//!
//! Environment variables:
//! - `LIX_NATIVE_FILE_READ_COMPONENT_FILE_COUNTS` (default: `1000,5000`)
//! - `LIX_NATIVE_FILE_READ_COMPONENT_HISTORY_COMMITS` (default: `100,1000,5000`)
//! - `LIX_NATIVE_FILE_READ_COMPONENT_PAIRS` (default: `3`, diagnostic)
//! - `LIX_NATIVE_FILE_READ_COMPONENT_SEED_BATCH_SIZE` (default: `1000`)
//! - `LIX_NATIVE_FILE_READ_COMPONENT_BACKENDS` (default: `rocksdb,slatedb`)

use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use lix_engine::{Blob, Engine, ExecuteResult, SessionContext, Storage, Value};
use lix_rocksdb_storage::RocksDB;
use lix_slatedb_storage::SlateDB;
use tempfile::TempDir;

const DEFAULT_FILE_COUNTS: &[usize] = &[1_000, 5_000];
const DEFAULT_HISTORY_COMMITS: &[usize] = &[100, 1_000, 5_000];
const DEFAULT_PAIRS: usize = 3;
const QUALIFICATION_PAIRS: usize = 30;
const DEFAULT_SEED_BATCH_SIZE: usize = 1_000;
const TARGETS_PER_CLASS: usize = 2;
const NORMAL_TEXT_FILE_BYTES: usize = 4 * 1024;
const NORMAL_PDF_FILE_BYTES: usize = 8 * 1024;
const NORMAL_PNG_FILE_BYTES: usize = 16 * 1024;
const NORMAL_BINARY_FILE_BYTES: usize = 8 * 1024;

const FILE_KINDS: [FileKind; 10] = [
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

const READ_CLASSES: [ReadClass; 3] = [
    ReadClass::new("json_4k", 0, 4 * 1024),
    ReadClass::new("pdf_32k", 6, 32 * 1024),
    ReadClass::new("binary_256k", 8, 256 * 1024),
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

#[derive(Clone, Copy)]
struct ReadClass {
    label: &'static str,
    file_kind_index: usize,
    payload_bytes: usize,
}

impl ReadClass {
    const fn new(label: &'static str, file_kind_index: usize, payload_bytes: usize) -> Self {
        Self {
            label,
            file_kind_index,
            payload_bytes,
        }
    }

    const fn kind(self) -> FileKind {
        FILE_KINDS[self.file_kind_index]
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
    SqlExact,
    Native,
}

impl Operation {
    const fn label(self) -> &'static str {
        match self {
            Self::SqlExact => "sql_exact_lix_file_read",
            Self::Native => "native_file_read_data",
        }
    }
}

#[derive(Clone, Debug)]
struct Config {
    file_counts: Vec<usize>,
    history_commits: Vec<usize>,
    pairs: usize,
    seed_batch_size: usize,
}

impl Config {
    fn from_env() -> Self {
        let file_counts = env_usize_list(
            "LIX_NATIVE_FILE_READ_COMPONENT_FILE_COUNTS",
            DEFAULT_FILE_COUNTS,
        );
        let history_commits = env_usize_list(
            "LIX_NATIVE_FILE_READ_COMPONENT_HISTORY_COMMITS",
            DEFAULT_HISTORY_COMMITS,
        );
        let pairs = env_usize("LIX_NATIVE_FILE_READ_COMPONENT_PAIRS", DEFAULT_PAIRS);
        let seed_batch_size = env_usize(
            "LIX_NATIVE_FILE_READ_COMPONENT_SEED_BATCH_SIZE",
            DEFAULT_SEED_BATCH_SIZE,
        );

        assert!(
            file_counts
                .iter()
                .all(|file_count| *file_count >= target_file_count() + FILE_KINDS.len()),
            "LIX_NATIVE_FILE_READ_COMPONENT_FILE_COUNTS values must be at least {}",
            target_file_count() + FILE_KINDS.len()
        );
        assert!(
            history_commits.iter().all(|history| *history >= 2),
            "LIX_NATIVE_FILE_READ_COMPONENT_HISTORY_COMMITS values must be at least 2"
        );
        assert!(
            pairs >= 2,
            "LIX_NATIVE_FILE_READ_COMPONENT_PAIRS must be at least 2 for a paired confidence interval"
        );
        assert!(
            seed_batch_size > 0,
            "LIX_NATIVE_FILE_READ_COMPONENT_SEED_BATCH_SIZE must be greater than zero"
        );

        Self {
            file_counts,
            history_commits,
            pairs,
            seed_batch_size,
        }
    }
}

struct Seed {
    backend: Backend,
    // The source must remain alive while its copies are created. It is closed
    // after seeding, so both RocksDB and SlateDB copies start from a complete,
    // immutable source tree rather than an open database.
    root: TempDir,
    source_path: PathBuf,
    main_branch_id: String,
    source_commit_count: usize,
    source_usage: DiskUsage,
    next_clone: AtomicU64,
}

impl Seed {
    async fn create(
        backend: Backend,
        file_count: usize,
        history_commits: usize,
        seed_batch_size: usize,
    ) -> Self {
        let root = tempfile::tempdir().expect("create native read component benchmark root");
        let source_path = root.path().join("source");
        let main_branch_id = match backend {
            Backend::RocksDb => {
                let storage =
                    RocksDB::open(&source_path).expect("open native read component RocksDB");
                let branch_id = seed_repository(
                    storage.clone(),
                    file_count,
                    history_commits,
                    seed_batch_size,
                )
                .await;
                storage
                    .flush()
                    .expect("flush native read component RocksDB source");
                branch_id
            }
            Backend::SlateDb => {
                let storage =
                    SlateDB::open(&source_path).expect("open native read component SlateDB");
                let branch_id = seed_repository(
                    storage.clone(),
                    file_count,
                    history_commits,
                    seed_batch_size,
                )
                .await;
                storage
                    .flush()
                    .await
                    .expect("flush native read component SlateDB source");
                branch_id
            }
        };
        let source_usage = disk_usage(&source_path).expect("account native read component source");

        Self {
            backend,
            root,
            source_path,
            main_branch_id,
            source_commit_count: history_commits,
            source_usage,
            next_clone: AtomicU64::new(0),
        }
    }

    async fn fork(&self) -> Fixture {
        let clone_number = self.next_clone.fetch_add(1, Ordering::Relaxed);
        let clone_dir = tempfile::tempdir_in(self.root.path())
            .expect("create native read component benchmark clone directory");
        let database_path = clone_dir.path().join(format!("database-{clone_number:04}"));
        copy_directory(&self.source_path, &database_path)
            .expect("copy closed native read component source");

        match self.backend {
            Backend::RocksDb => {
                let storage = RocksDB::open(&database_path)
                    .expect("open copied native read component RocksDB");
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
                let storage = SlateDB::open(&database_path)
                    .expect("open copied native read component SlateDB");
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
    // Keep storage before TempDir in declaration order so it closes before the
    // clone directory is removed.
    session: SessionContext<S>,
    _storage: S,
    database_path: PathBuf,
    _clone_dir: TempDir,
}

impl<S> ComponentFixture<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn sql_exact_read(&self, path: String) -> Blob {
        let result = self
            .session
            .execute(
                "SELECT data FROM lix_file WHERE path = $1",
                &[Value::Text(path)],
            )
            .await
            .expect("execute exact SQL file read");
        blob_from_exact_sql(result)
    }

    async fn native_read(&self, path: String) -> Blob {
        self.session
            .read_file_data(path)
            .await
            .expect("execute native file read")
            .expect("seeded timed file must exist")
    }

    async fn visible_commit_count(&self) -> usize {
        count_rows(&self.session, "SELECT COUNT(*) AS count FROM lix_commit").await
    }

    async fn visible_file_count(&self) -> usize {
        count_rows(&self.session, "SELECT COUNT(*) AS count FROM lix_file").await
    }

    fn disk_usage(&self) -> DiskUsage {
        disk_usage(&self.database_path).expect("account native read component clone")
    }
}

enum Fixture {
    RocksDb(ComponentFixture<RocksDB>),
    SlateDb(ComponentFixture<SlateDB>),
}

impl Fixture {
    async fn read(&self, operation: Operation, path: String) -> Blob {
        match (self, operation) {
            (Self::RocksDb(fixture), Operation::SqlExact) => fixture.sql_exact_read(path).await,
            (Self::RocksDb(fixture), Operation::Native) => fixture.native_read(path).await,
            (Self::SlateDb(fixture), Operation::SqlExact) => fixture.sql_exact_read(path).await,
            (Self::SlateDb(fixture), Operation::Native) => fixture.native_read(path).await,
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

#[derive(Clone, Copy)]
struct ReadSelection {
    class: ReadClass,
    warmup_slot: usize,
    timed_slot: usize,
}

impl ReadSelection {
    fn warmup_path(self) -> String {
        target_path(self.class, self.warmup_slot)
    }

    fn timed_path(self) -> String {
        target_path(self.class, self.timed_slot)
    }

    fn expected_hash(self) -> String {
        blake3::hash(&target_payload(self.class, self.timed_slot))
            .to_hex()
            .to_string()
    }
}

struct Sample {
    read_ns: u64,
    output_bytes: usize,
    output_hash: String,
    storage_before: DiskUsage,
    storage_after: DiskUsage,
    rss_before_bytes: Option<u64>,
    rss_after_bytes: Option<u64>,
}

impl Sample {
    fn storage_delta(&self) -> DiskDelta {
        self.storage_after.delta_from(self.storage_before)
    }
}

struct PairResult {
    sql: Sample,
    native: Sample,
}

fn main() {
    let config = Config::from_env();
    let backends = selected_backends();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create native read component benchmark runtime");

    runtime.block_on(async {
        for backend in backends {
            for &file_count in &config.file_counts {
                for &history_commits in &config.history_commits {
                    run_configuration(backend, &config, file_count, history_commits).await;
                }
            }
        }
    });
}

async fn run_configuration(
    backend: Backend,
    config: &Config,
    file_count: usize,
    history_commits: usize,
) {
    let seed = Seed::create(backend, file_count, history_commits, config.seed_batch_size).await;
    println!(
        "{}",
        serde_json::json!({
            "benchmark": "native_file_read_component",
            "event": "source_seed",
            "backend": backend.label(),
            "file_count": file_count,
            "history_commits_at_measurement_start": history_commits,
            "source_commit_count": seed.source_commit_count,
            "seed_batch_size": config.seed_batch_size,
            "pairs": config.pairs,
            "source_clone_per_sample": true,
            "timing_boundary": "from SQL/native invocation through an extracted usable Blob",
            "warmup_policy": "same arm and payload class, distinct path, excluded from timer",
            "read_classes": read_classes_json(),
            "source_storage": seed.source_usage,
        })
    );

    for (class_index, class) in READ_CLASSES.iter().copied().enumerate() {
        let mut pairs = Vec::with_capacity(config.pairs);
        for pair_index in 0..config.pairs {
            let sql_first = (pair_index + class_index).is_multiple_of(2);
            let selection = selected_targets(class, pair_index);
            let (sql, native) = if sql_first {
                (
                    run_sample(
                        &seed,
                        file_count,
                        history_commits,
                        pair_index,
                        sql_first,
                        selection,
                        Operation::SqlExact,
                    )
                    .await,
                    run_sample(
                        &seed,
                        file_count,
                        history_commits,
                        pair_index,
                        sql_first,
                        selection,
                        Operation::Native,
                    )
                    .await,
                )
            } else {
                let native = run_sample(
                    &seed,
                    file_count,
                    history_commits,
                    pair_index,
                    sql_first,
                    selection,
                    Operation::Native,
                )
                .await;
                let sql = run_sample(
                    &seed,
                    file_count,
                    history_commits,
                    pair_index,
                    sql_first,
                    selection,
                    Operation::SqlExact,
                )
                .await;
                (sql, native)
            };
            pairs.push(PairResult { sql, native });
        }
        print_summary(backend, file_count, history_commits, class, &pairs);
    }
}

async fn run_sample(
    seed: &Seed,
    file_count: usize,
    history_commits: usize,
    pair_index: usize,
    sql_first: bool,
    selection: ReadSelection,
    operation: Operation,
) -> Sample {
    let fixture = seed.fork().await;
    let warmup_path = selection.warmup_path();
    let timed_path = selection.timed_path();

    // Warm the exact route and the same payload class, but never the path
    // timed below. This remains outside both timing and disk baselines.
    let warmup = fixture.read(operation, warmup_path.clone()).await;
    assert_eq!(warmup.len(), selection.class.payload_bytes);
    black_box(warmup);

    let storage_before = fixture.disk_usage();
    let rss_before_bytes = process_resident_bytes();
    let started = Instant::now();
    // Both arms receive an owned path and finish at the same usable `Blob`
    // boundary. SQL result decoding is consequently not deferred outside its
    // timer while native data handling is inside its timer.
    let data = fixture.read(operation, timed_path.clone()).await;
    let read_ns = duration_ns(started.elapsed());
    let data = black_box(data);
    let output_bytes = data.len();
    let output_hash = blake3::hash(data.as_ref()).to_hex().to_string();
    assert_eq!(output_bytes, selection.class.payload_bytes);
    assert_eq!(output_hash, selection.expected_hash());
    drop(data);
    let rss_after_bytes = process_resident_bytes();
    let storage_after = fixture.disk_usage();

    // Read samples must not create commits, files, or storage-layout changes.
    let post_timing_commit_count = fixture.visible_commit_count().await;
    let post_timing_file_count = fixture.visible_file_count().await;
    assert_eq!(post_timing_commit_count, history_commits);
    assert_eq!(post_timing_file_count, file_count);
    drop(fixture);

    let sample = Sample {
        read_ns,
        output_bytes,
        output_hash,
        storage_before,
        storage_after,
        rss_before_bytes,
        rss_after_bytes,
    };
    println!(
        "{}",
        serde_json::json!({
            "benchmark": "native_file_read_component",
            "event": "sample",
            "backend": seed.backend.label(),
            "pair": pair_index,
            "order": if sql_first { "sql_then_native" } else { "native_then_sql" },
            "operation": operation.label(),
            "file_count": file_count,
            "history_commits_at_start": history_commits,
            "read_class": selection.class.label,
            "payload_bytes": selection.class.payload_bytes,
            "warmup_path": warmup_path,
            "timed_path": timed_path,
            "warmup_and_timed_paths_disjoint": selection.warmup_slot != selection.timed_slot,
            "timing_boundary": "from SQL/native invocation through an extracted usable Blob",
            "read_ns": sample.read_ns,
            "output_bytes": sample.output_bytes,
            "output_blake3": &sample.output_hash,
            "post_timing_commit_count": post_timing_commit_count,
            "post_timing_file_count": post_timing_file_count,
            "post_timing_state_verified": true,
            "storage_before": sample.storage_before,
            "storage_after": sample.storage_after,
            "storage_delta": sample.storage_delta(),
            "process_rss_before_bytes": sample.rss_before_bytes,
            "process_rss_after_bytes": sample.rss_after_bytes,
        })
    );
    sample
}

fn print_summary(
    backend: Backend,
    file_count: usize,
    history_commits: usize,
    class: ReadClass,
    pairs: &[PairResult],
) {
    let mut sql = pairs
        .iter()
        .map(|pair| pair.sql.read_ns)
        .collect::<Vec<_>>();
    let mut native = pairs
        .iter()
        .map(|pair| pair.native.read_ns)
        .collect::<Vec<_>>();
    let mut sql_storage_delta = pairs
        .iter()
        .map(|pair| pair.sql.storage_delta().total_bytes)
        .collect::<Vec<_>>();
    let mut native_storage_delta = pairs
        .iter()
        .map(|pair| pair.native.storage_delta().total_bytes)
        .collect::<Vec<_>>();
    let speedup = paired_speedup(pairs);
    let has_qualification_pair_count = pairs.len() >= QUALIFICATION_PAIRS;

    println!(
        "{}",
        serde_json::json!({
            "benchmark": "native_file_read_component",
            "event": "summary",
            "backend": backend.label(),
            "file_count": file_count,
            "history_commits_at_start": history_commits,
            "read_class": class.label,
            "payload_bytes": class.payload_bytes,
            "pairs": pairs.len(),
            "confidence_level": 0.99,
            "t_critical": speedup.t_critical,
            "comparison": "SQL exact lix_file read / SessionContext::read_file_data; both end at an extracted usable Blob",
            "sql_read_p50_ns": percentile(&mut sql, 50),
            "sql_read_p95_ns": percentile(&mut sql, 95),
            "sql_read_mean_ns": mean_u64(&sql),
            "native_read_p50_ns": percentile(&mut native, 50),
            "native_read_p95_ns": percentile(&mut native, 95),
            "native_read_mean_ns": mean_u64(&native),
            "sql_median_storage_total_delta_bytes": median_i64(&mut sql_storage_delta),
            "native_median_storage_total_delta_bytes": median_i64(&mut native_storage_delta),
            "paired_geometric_mean_speedup": speedup.geometric_mean,
            "paired_99_lower_speedup": speedup.lower_99,
            "paired_99_upper_speedup": speedup.upper_99,
            "paired_99_lower_latency_reduction_percent": (1.0 - 1.0 / speedup.lower_99) * 100.0,
            "minimum_pairs_for_robust_qualification": QUALIFICATION_PAIRS,
            "statistically_robust_99_ci": has_qualification_pair_count,
            "qualifies_more_than_20_percent": has_qualification_pair_count && speedup.lower_99 > 1.25,
        })
    );
}

struct PairedSpeedup {
    geometric_mean: f64,
    lower_99: f64,
    upper_99: f64,
    t_critical: f64,
}

fn paired_speedup(pairs: &[PairResult]) -> PairedSpeedup {
    let log_speedups = pairs
        .iter()
        .map(|pair| {
            let sql = exact_f64_from_u64(pair.sql.read_ns.max(1));
            let native = exact_f64_from_u64(pair.native.read_ns.max(1));
            (sql / native).ln()
        })
        .collect::<Vec<_>>();
    let mean_log_speedup = mean(&log_speedups);
    let standard_error =
        sample_standard_deviation(&log_speedups) / exact_f64_from_usize(log_speedups.len()).sqrt();
    let t_critical = t_99_two_sided_critical(log_speedups.len() - 1);
    PairedSpeedup {
        geometric_mean: mean_log_speedup.exp(),
        lower_99: (-t_critical)
            .mul_add(standard_error, mean_log_speedup)
            .exp(),
        upper_99: t_critical.mul_add(standard_error, mean_log_speedup).exp(),
        t_critical,
    }
}

async fn seed_repository<S>(
    storage: S,
    file_count: usize,
    history_commits: usize,
    seed_batch_size: usize,
) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let init = Engine::initialize(storage.clone())
        .await
        .expect("initialize native read component source repository");
    let engine = Engine::new(storage)
        .await
        .expect("open native read component source engine");
    let session = engine
        .open_session(init.main_branch_id.clone())
        .await
        .expect("open native read component source session");

    let entries = seed_entries(file_count);
    for chunk in entries.chunks(seed_batch_size) {
        let rows_affected = session
            .upsert_file_data_batch(chunk.to_vec())
            .await
            .expect("seed native read mixed corpus batch");
        assert_eq!(
            rows_affected,
            u64::try_from(chunk.len()).expect("seed rows fit u64")
        );
    }
    assert_eq!(
        count_rows(&session, "SELECT COUNT(*) AS count FROM lix_file").await,
        file_count,
        "source corpus must contain the requested number of files"
    );

    let mut commit_count = count_rows(&session, "SELECT COUNT(*) AS count FROM lix_commit").await;
    assert!(
        commit_count <= history_commits,
        "history target {history_commits} is too small for {commit_count} initialization and seed commits"
    );
    while commit_count < history_commits {
        let file_index = commit_count % generic_file_count(file_count);
        let rows_affected = session
            .upsert_file_data(
                generic_path(file_index),
                Blob::from(generic_payload(
                    file_index,
                    u64::try_from(commit_count).expect("commit count fits u64"),
                )),
            )
            .await
            .expect("append mixed-corpus history commit");
        assert_eq!(rows_affected, 1);
        commit_count += 1;
    }
    assert_eq!(
        count_rows(&session, "SELECT COUNT(*) AS count FROM lix_commit").await,
        history_commits,
        "source history must match the configured measurement depth"
    );
    init.main_branch_id
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
        .expect("open native read component clone engine");
    let session = engine
        .open_session(main_branch_id)
        .await
        .expect("open native read component clone session");
    ComponentFixture {
        session,
        _storage: storage,
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

fn blob_from_exact_sql(result: ExecuteResult) -> Blob {
    let row = result
        .rows()
        .first()
        .expect("seeded SQL exact read must return a row");
    row.get::<Blob>("data")
        .expect("SQL exact read must return a blob data column")
}

fn selected_targets(class: ReadClass, pair_index: usize) -> ReadSelection {
    let warmup_slot = pair_index % TARGETS_PER_CLASS;
    let timed_slot = (warmup_slot + 1) % TARGETS_PER_CLASS;
    assert_ne!(warmup_slot, timed_slot);
    ReadSelection {
        class,
        warmup_slot,
        timed_slot,
    }
}

fn target_file_count() -> usize {
    READ_CLASSES.len() * TARGETS_PER_CLASS
}

fn generic_file_count(file_count: usize) -> usize {
    file_count
        .checked_sub(target_file_count())
        .expect("file count must leave room for target read files")
}

fn seed_entries(file_count: usize) -> Vec<(String, Blob)> {
    let mut entries = Vec::with_capacity(file_count);
    for class in READ_CLASSES {
        for target_slot in 0..TARGETS_PER_CLASS {
            entries.push((
                target_path(class, target_slot),
                Blob::from(target_payload(class, target_slot)),
            ));
        }
    }
    for file_index in 0..generic_file_count(file_count) {
        entries.push((
            generic_path(file_index),
            Blob::from(generic_payload(file_index, 0)),
        ));
    }
    assert_eq!(entries.len(), file_count);
    entries
}

fn target_path(class: ReadClass, target_slot: usize) -> String {
    let kind = class.kind();
    format!(
        "/component-read/{}/target-{target_slot}.{}",
        kind.directory, kind.extension
    )
}

fn generic_path(file_index: usize) -> String {
    let kind = FILE_KINDS[file_index % FILE_KINDS.len()];
    format!(
        "/corpus/{}/file-{file_index:05}.{}",
        kind.directory, kind.extension
    )
}

fn target_payload(class: ReadClass, target_slot: usize) -> Vec<u8> {
    payload_for_kind(
        class.kind(),
        class.payload_bytes,
        class.file_kind_index,
        u64::try_from(target_slot).expect("target slot fits u64"),
    )
}

fn generic_payload(file_index: usize, version: u64) -> Vec<u8> {
    let kind = FILE_KINDS[file_index % FILE_KINDS.len()];
    payload_for_kind(kind, generic_payload_bytes(kind), file_index, version)
}

fn generic_payload_bytes(kind: FileKind) -> usize {
    match kind.extension {
        "pdf" => NORMAL_PDF_FILE_BYTES,
        "png" => NORMAL_PNG_FILE_BYTES,
        "bin" => NORMAL_BINARY_FILE_BYTES,
        _ => NORMAL_TEXT_FILE_BYTES,
    }
}

fn payload_for_kind(
    kind: FileKind,
    target_bytes: usize,
    file_index: usize,
    version: u64,
) -> Vec<u8> {
    match kind.extension {
        "pdf" => binary_payload(
            target_bytes,
            b"%PDF-1.7\n%lix-native-read-benchmark\n",
            file_index,
            version,
        ),
        "png" => binary_payload(
            target_bytes,
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR",
            file_index,
            version,
        ),
        "bin" => binary_payload(target_bytes, b"LIX\x00BINARY\x01", file_index, version),
        "json" => text_payload(
            target_bytes,
            &format!(
                "{{\"kind\":\"json\",\"file\":{file_index},\"version\":{version},\"payload\":\""
            ),
            "xYz0123456789",
            "\"}\n",
        ),
        "csv" => text_payload(
            target_bytes,
            &format!("file,version,value\n{file_index},{version},"),
            "csv-value-0123456789,",
            "\n",
        ),
        "md" => text_payload(
            target_bytes,
            &format!("# Benchmark {file_index}\n\nversion: {version}\n\n"),
            "lix native read markdown content ",
            "\n",
        ),
        "txt" => text_payload(
            target_bytes,
            &format!("file={file_index} version={version}\n"),
            "plain-text-content-0123456789 ",
            "\n",
        ),
        "xml" => text_payload(
            target_bytes,
            &format!("<file id=\"{file_index}\" version=\"{version}\">"),
            "xml-content-0123456789",
            "</file>\n",
        ),
        "yaml" => text_payload(
            target_bytes,
            &format!("file: {file_index}\nversion: {version}\nvalue: "),
            "yaml-content-0123456789",
            "\n",
        ),
        "log" => text_payload(
            target_bytes,
            &format!("INFO file={file_index} version={version} message="),
            "native-read-log-0123456789 ",
            "\n",
        ),
        extension => panic!("unsupported benchmark extension {extension}"),
    }
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
        *byte = state.to_le_bytes()[0];
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

fn process_resident_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let value = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?;
    let kibibytes = value.split_whitespace().next()?.parse::<u64>().ok()?;
    kibibytes.checked_mul(1024)
}

fn selected_backends() -> Vec<Backend> {
    let requested = std::env::var("LIX_NATIVE_FILE_READ_COMPONENT_BACKENDS")
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
                "unsupported LIX_NATIVE_FILE_READ_COMPONENT_BACKENDS value {other:?}; expected rocksdb or slatedb"
            ),
        };
        if !selected.contains(&backend) {
            selected.push(backend);
        }
    }
    assert!(
        !selected.is_empty(),
        "LIX_NATIVE_FILE_READ_COMPONENT_BACKENDS must select at least one backend"
    );
    selected
}

fn read_classes_json() -> Vec<serde_json::Value> {
    READ_CLASSES
        .iter()
        .map(|class| {
            serde_json::json!({
                "label": class.label,
                "payload_bytes": class.payload_bytes,
                "extension": class.kind().extension,
                "target_paths_per_class": TARGETS_PER_CLASS,
            })
        })
        .collect()
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| {
        value
            .parse::<usize>()
            .unwrap_or_else(|error| panic!("parse {name}={value:?} as usize: {error}"))
    })
}

fn env_usize_list(name: &str, defaults: &[usize]) -> Vec<usize> {
    let values = std::env::var(name).map_or_else(
        |_| defaults.to_vec(),
        |value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(|entry| {
                    entry.parse::<usize>().unwrap_or_else(|error| {
                        panic!("parse {name} entry {entry:?} as usize: {error}")
                    })
                })
                .collect::<Vec<_>>()
        },
    );
    assert!(!values.is_empty(), "{name} must contain at least one value");
    values
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
    values.iter().sum::<f64>() / exact_f64_from_usize(values.len())
}

fn mean_u64(values: &[u64]) -> f64 {
    assert!(!values.is_empty(), "benchmark mean requires samples");
    values
        .iter()
        .map(|value| exact_f64_from_u64(*value))
        .sum::<f64>()
        / exact_f64_from_usize(values.len())
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
        / exact_f64_from_usize(values.len() - 1);
    variance.sqrt()
}

fn t_99_two_sided_critical(degrees_of_freedom: usize) -> f64 {
    // Two-sided Student-t critical values for alpha=0.01. Thirty or more
    // pairs use the df=29 value, intentionally conservative for larger n.
    const VALUES: [f64; 29] = [
        63.657, 9.925, 5.841, 4.604, 4.032, 3.707, 3.499, 3.355, 3.250, 3.169, 3.106, 3.055, 3.012,
        2.977, 2.947, 2.921, 2.898, 2.878, 2.861, 2.845, 2.831, 2.819, 2.807, 2.797, 2.787, 2.779,
        2.771, 2.763, 2.756,
    ];
    assert!(degrees_of_freedom > 0, "paired CI requires two samples");
    VALUES[degrees_of_freedom.saturating_sub(1).min(28)]
}

fn exact_f64_from_usize(value: usize) -> f64 {
    exact_f64_from_u64(u64::try_from(value).expect("benchmark count must fit u64"))
}

fn exact_f64_from_u64(value: u64) -> f64 {
    assert!(
        value <= (1_u64 << f64::MANTISSA_DIGITS),
        "benchmark value {value} exceeds f64's exact integer range"
    );
    #[allow(clippy::cast_precision_loss)]
    {
        value as f64
    }
}
