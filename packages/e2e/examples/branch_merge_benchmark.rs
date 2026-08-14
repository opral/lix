//! Isolated-process branching and merge qualification benchmark.
//!
//! Run the default qualification matrix and capture JSONL:
//! `cargo run --release -p lix_e2e --example branch_merge_benchmark -- qualification > results.jsonl`
//!
//! Run one worker directly:
//! `cargo run --release -p lix_e2e --example branch_merge_benchmark -- worker rows clean 10000 100 10 100 8 64`

use std::alloc::GlobalAlloc;
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::io::{Cursor, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine as _;
use lix::storage::Storage;
use lix::{
    CreateBranchOptions, Lix, MergeBranchOptions, MergeBranchOutcome, MergeBranchPreviewOptions,
    SwitchBranchOptions, Value, open_lix,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tracing::Subscriber;
use tracing::span::{Attributes, Id};
use tracing::subscriber::Interest;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context as TracingContext, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

const SCHEMA_VERSION: u64 = 2;
const SOURCE_BRANCH_ID: &str = "01920000-0000-7000-8000-00000000b001";
const INSERT_BATCH: usize = 250;

#[global_allocator]
static GLOBAL_ALLOCATOR: TrackingAllocator = TrackingAllocator;

struct TrackingAllocator;

static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static COUNT_ALLOCATIONS: Cell<bool> = const { Cell::new(true) };
}

fn record_allocation(bytes: usize) {
    if COUNT_ALLOCATIONS.try_with(Cell::get).unwrap_or(false) {
        ALLOCATED_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
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
        if !replacement.is_null() && new_size > layout.size() {
            record_allocation(new_size - layout.size());
        }
        replacement
    }
}

#[derive(Clone, Debug)]
struct Config {
    layer: String,
    scenario: String,
    rows: usize,
    changes: usize,
    divergent_commits: usize,
    history: usize,
    branches: usize,
    payload_bytes: usize,
}

macro_rules! config {
    ($layer:expr, $scenario:expr, $rows:expr, $changes:expr, $commits:expr, $history:expr, $branches:expr, $payload:expr $(,)?) => {
        Config {
            layer: $layer.to_owned(),
            scenario: $scenario.to_owned(),
            rows: $rows,
            changes: $changes,
            divergent_commits: $commits,
            history: $history,
            branches: $branches,
            payload_bytes: $payload,
        }
    };
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RowValue {
    Present(String),
    Absent,
}

#[derive(Clone, Default)]
struct PerfSpanCollector {
    samples: Arc<Mutex<Vec<PerfSpanSample>>>,
}

struct PerfSpanSample {
    name: &'static str,
    elapsed: Duration,
}

struct StartedPerfSpan {
    name: &'static str,
    started: Instant,
}

impl PerfSpanCollector {
    fn clear(&self) {
        self.samples.lock().expect("span lock").clear();
    }

    fn take_ms(&self) -> BTreeMap<&'static str, f64> {
        let samples = std::mem::take(&mut *self.samples.lock().expect("span lock"));
        let mut result = BTreeMap::new();
        for sample in samples {
            *result.entry(sample.name).or_insert(0.0) += sample.elapsed.as_secs_f64() * 1_000.0;
        }
        result
    }
}

impl<S> Layer<S> for PerfSpanCollector
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn register_callsite(&self, metadata: &'static tracing::Metadata<'static>) -> Interest {
        if metadata.target() == "lix_perf" {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: TracingContext<'_, S>) {
        if attrs.metadata().target() != "lix_perf" {
            return;
        }
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(StartedPerfSpan {
                name: attrs.metadata().name(),
                started: Instant::now(),
            });
        }
    }

    fn on_close(&self, id: Id, ctx: TracingContext<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let Some(started) = span.extensions_mut().remove::<StartedPerfSpan>() else {
            return;
        };
        self.samples
            .lock()
            .expect("span lock")
            .push(PerfSpanSample {
                name: started.name,
                elapsed: started.started.elapsed(),
            });
    }
}

#[derive(Clone, Copy)]
struct ProcessCounters {
    cpu_ns: u64,
    read_bytes: u64,
    write_bytes: u64,
    rss_bytes: u64,
}

struct RssSampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicU64>,
    handle: Option<thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicU64::new(current_rss_bytes()));
        let thread_stop = stop.clone();
        let thread_peak = peak.clone();
        let handle = thread::spawn(move || {
            COUNT_ALLOCATIONS.with(|enabled| enabled.set(false));
            while !thread_stop.load(Ordering::Relaxed) {
                thread_peak.fetch_max(current_rss_bytes(), Ordering::Relaxed);
                thread::sleep(Duration::from_millis(1));
            }
            thread_peak.fetch_max(current_rss_bytes(), Ordering::Relaxed);
        });
        Self {
            stop,
            peak,
            handle: Some(handle),
        }
    }

    fn finish(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        self.handle
            .take()
            .expect("RSS sampler handle")
            .join()
            .expect("RSS sampler");
        self.peak.load(Ordering::Relaxed)
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    match args.get(1).map(String::as_str) {
        None | Some("qualification") => controller(false),
        Some("sweep") => controller(true),
        Some("worker") => worker(parse_config(&args[2..])),
        Some(other) => panic!("unknown mode {other:?}; expected qualification, sweep, or worker"),
    }
}

fn controller(sweep: bool) {
    let executable = std::env::current_exe().expect("resolve benchmark executable");
    let mut configs = vec![
        config!("rows", "already_up_to_date", 1_000, 10, 1, 0, 1, 32),
        config!("rows", "fast_forward", 1_000, 10, 1, 0, 1, 32),
        config!("rows", "clean", 10_000, 100, 10, 100, 8, 64),
        config!("rows", "equal_convergence", 10_000, 100, 10, 100, 8, 64),
        config!("rows", "modify_conflict", 10_000, 100, 10, 100, 8, 64),
        config!("rows", "source_delete", 10_000, 100, 10, 100, 8, 64),
        config!(
            "rows",
            "delete_modify_conflict",
            10_000,
            100,
            10,
            100,
            8,
            64,
        ),
        config!("rows", "both_delete", 10_000, 100, 10, 100, 8, 64),
        config!("rows", "add_same", 10_000, 100, 10, 100, 8, 64),
        config!("rows", "add_conflict", 10_000, 100, 10, 100, 8, 64),
        config!("rows", "mixed_conflict", 10_000, 100, 10, 100, 8, 64),
        config!("files", "all_plugins_resolvable", 2, 5, 1, 0, 1, 32),
        config!("files", "all_plugins_cold_reopen", 2, 5, 1, 0, 1, 32),
    ];
    if sweep {
        for rows in [1_000, 10_000, 100_000] {
            for changes in [1, 10, 100] {
                if changes * 2 <= rows {
                    configs.push(config!("rows", "clean", rows, changes, 1, 1_000, 16, 128));
                }
            }
        }
        for history in [0, 10, 100, 1_000, 10_000] {
            configs.push(config!("rows", "clean", 10_000, 100, 1, history, 8, 64));
        }
        for branches in [1, 8, 64, 256] {
            configs.push(config!("rows", "clean", 10_000, 100, 1, 100, branches, 64));
        }
        for payload in [16, 256, 4096] {
            configs.push(config!("rows", "clean", 10_000, 100, 1, 100, 8, payload));
        }
        for commits in [1, 10, 100] {
            configs.push(config!("rows", "clean", 10_000, 100, commits, 100, 8, 64));
        }
        configs.extend([
            config!("files", "all_plugins_resolvable", 2, 25, 1, 0, 1, 32),
            config!("files", "all_plugins_resolvable", 100, 5, 1, 0, 1, 32),
            config!("files", "all_plugins_resolvable", 3, 5, 1, 0, 1, 4096),
            config!("files", "all_plugins_resolvable", 2, 5, 1, 0, 100, 32),
        ]);
    }
    let samples = std::env::var("LIX_BRANCH_MERGE_BENCH_SAMPLES")
        .ok()
        .map(|value| parse_usize(&value, "LIX_BRANCH_MERGE_BENCH_SAMPLES"))
        .unwrap_or(1);
    for cfg in &configs {
        for sample in 0..samples {
            let status = Command::new(&executable)
                .args([
                    "worker",
                    &cfg.layer,
                    &cfg.scenario,
                    &cfg.rows.to_string(),
                    &cfg.changes.to_string(),
                    &cfg.divergent_commits.to_string(),
                    &cfg.history.to_string(),
                    &cfg.branches.to_string(),
                    &cfg.payload_bytes.to_string(),
                ])
                .env("LIX_BRANCH_MERGE_BENCH_SAMPLE", sample.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .status()
                .unwrap_or_else(|error| panic!("spawn worker for {cfg:?}: {error}"));
            assert!(status.success(), "worker failed for {cfg:?}: {status}");
        }
    }
}

fn parse_config(args: &[String]) -> Config {
    assert_eq!(
        args.len(),
        8,
        "worker expects: layer scenario rows changes divergent-commits history branches payload-bytes"
    );
    config!(
        &args[0],
        &args[1],
        parse_usize(&args[2], "rows"),
        parse_usize(&args[3], "changes"),
        parse_usize(&args[4], "divergent commits"),
        parse_usize_allow_zero(&args[5], "history"),
        parse_usize(&args[6], "branches"),
        parse_usize(&args[7], "payload bytes"),
    )
}

fn parse_usize(value: &str, name: &str) -> usize {
    let result = parse_usize_allow_zero(value, name);
    assert!(result > 0, "{name} must be positive");
    result
}

fn parse_usize_allow_zero(value: &str, name: &str) -> usize {
    value
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be an integer, got {value:?}"))
}

fn worker(cfg: Config) {
    let collector = PerfSpanCollector::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(collector.clone()));
    tracing::dispatcher::with_default(&dispatch, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("benchmark runtime")
            .block_on(async move {
                let storage = benchmark_storage_name();
                match storage.as_str() {
                    "rocksdb" => run_worker::<RocksDB>(cfg, collector).await,
                    "slatedb" => run_worker::<SlateDB>(cfg, collector).await,
                    storage => {
                        panic!(
                            "unknown LIX_BRANCH_MERGE_BENCH_STORAGE {storage:?}; expected rocksdb or slatedb"
                        )
                    }
                }
            });
    });
}

async fn run_worker<StorageImpl>(cfg: Config, collector: PerfSpanCollector)
where
    StorageImpl: BenchmarkStorage,
{
    match cfg.layer.as_str() {
        "rows" => {
            assert!(
                cfg.changes * 2 <= cfg.rows,
                "row fixture needs two disjoint change ranges"
            );
            run_row_case::<StorageImpl>(cfg, collector).await;
        }
        "files" => run_file_case::<StorageImpl>(cfg, collector).await,
        other => panic!("unknown benchmark layer {other:?}"),
    }
}

async fn run_file_case<StorageImpl>(cfg: Config, collector: PerfSpanCollector)
where
    StorageImpl: BenchmarkStorage,
{
    assert!(matches!(
        cfg.scenario.as_str(),
        "all_plugins_resolvable" | "all_plugins_cold_reopen"
    ));
    assert!(
        cfg.changes >= 5,
        "file benchmark requires the five primary affected files"
    );
    let cold_reopen = cfg.scenario == "all_plugins_cold_reopen";
    let provenance = benchmark_provenance();
    let total_started = Instant::now();
    let root = benchmark_tempdir();
    let db_path = root.path().join(".lix");
    let process_baseline_rss = current_rss_bytes();
    let open_started = Instant::now();
    let mut lix = open_benchmark::<StorageImpl>(&db_path).await;
    let open_ms = elapsed_ms(open_started);

    let plugins_started = Instant::now();
    install_all_plugins(&lix).await;
    let install_plugins_ms = elapsed_ms(plugins_started);
    let plugins_loaded_rss = current_rss_bytes();
    let mut fixture_files = plugin_base_files(&cfg);
    let control_files = plugin_control_files(&cfg);
    fixture_files.extend(control_files.clone());
    for (path, bytes) in &fixture_files {
        write_file(&lix, path, bytes.clone()).await;
    }
    let main_branch_id = lix.active_branch_id().await.expect("file main branch");
    let branch_started = Instant::now();
    let source_receipt = lix
        .create_branch(CreateBranchOptions {
            id: Some(SOURCE_BRANCH_ID.to_owned()),
            name: "all-plugin-source".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("create all-plugin source branch");
    let create_branch_ms = elapsed_ms(branch_started);

    apply_plugin_semantic_edits(&lix, true).await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: source_receipt.id.clone(),
    })
    .await
    .expect("switch workspace to plugin source");
    apply_plugin_semantic_edits(&lix, false).await;
    for (path, bytes) in plugin_extra_source_files(&cfg) {
        write_file(&lix, &path, bytes).await;
    }
    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id.clone(),
    })
    .await
    .expect("switch workspace to plugin target");
    let mut source = lix
        .open_another_session()
        .await
        .expect("open plugin source session");
    source
        .switch_branch(SwitchBranchOptions {
            branch_id: source_receipt.id.clone(),
        })
        .await
        .expect("switch plugin source session");
    if cold_reopen {
        source
            .close()
            .await
            .expect("close source before cold measurement");
        lix.close()
            .await
            .expect("close target before cold measurement");
        drop(source);
        drop(lix);
        lix = open_benchmark::<StorageImpl>(&db_path).await;
        source = lix
            .open_another_session()
            .await
            .expect("cold-open plugin source");
        source
            .switch_branch(SwitchBranchOptions {
                branch_id: SOURCE_BRANCH_ID.to_owned(),
            })
            .await
            .expect("switch cold plugin source session");
    }
    let target_before_preview = read_all_files(&lix, fixture_files.keys()).await;
    let source_before_preview = read_all_files(&source, fixture_files.keys()).await;
    let control_change_ids_before = file_change_ids(&lix, control_files.keys()).await;
    let storage_bytes_before = directory_bytes(&db_path);

    collector.clear();
    let preview_measure = measure_async(|| async {
        lix.merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: SOURCE_BRANCH_ID.to_owned(),
        })
        .await
    })
    .await;
    let preview = preview_measure.value.expect("plugin preview");
    let preview_phases = collector.take_ms();
    assert!(
        preview.conflicts.is_empty(),
        "plugin-owned conflicts must be resolver-owned: {:?}",
        preview.conflicts
    );
    assert_eq!(
        read_all_files(&lix, fixture_files.keys()).await,
        target_before_preview,
        "plugin preview mutated target files"
    );
    assert_eq!(
        read_all_files(&source, fixture_files.keys()).await,
        source_before_preview,
        "plugin preview mutated source files"
    );
    assert_eq!(
        branch_head(&lix, &main_branch_id).await,
        preview.target_head_commit_id
    );
    assert_eq!(
        branch_head(&lix, SOURCE_BRANCH_ID).await,
        preview.source_head_commit_id
    );

    collector.clear();
    let merge_measure = measure_async(|| async {
        lix.merge_branch(MergeBranchOptions {
            source_branch_id: SOURCE_BRANCH_ID.to_owned(),
        })
        .await
    })
    .await;
    let merge_phases = collector.take_ms();
    let receipt = merge_measure
        .value
        .expect("all-plugin merge should resolve");
    assert_eq!(receipt.outcome, MergeBranchOutcome::MergeCommitted);
    assert_eq!(preview.outcome, receipt.outcome);
    assert_eq!(preview.change_stats, receipt.change_stats);
    assert_eq!(preview.base_commit_id, receipt.base_commit_id);
    assert_eq!(
        branch_head(&lix, &main_branch_id).await,
        receipt.target_head_after_commit_id
    );
    assert_eq!(
        branch_head(&lix, SOURCE_BRANCH_ID).await,
        receipt.source_head_before_commit_id
    );
    assert_eq!(
        commit_parent_count(&lix, &receipt.target_head_after_commit_id).await,
        2
    );
    verify_plugin_results(&lix, &cfg).await;
    assert_eq!(
        file_change_ids(&lix, control_files.keys()).await,
        control_change_ids_before,
        "merge rewrote unaffected plugin-owned control files"
    );
    assert_eq!(
        read_all_files(&source, fixture_files.keys()).await,
        source_before_preview,
        "plugin merge mutated source files"
    );
    let repeated = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: SOURCE_BRANCH_ID.to_owned(),
        })
        .await
        .expect("repeated plugin merge");
    assert_eq!(repeated.outcome, MergeBranchOutcome::AlreadyUpToDate);
    assert!(repeated.created_merge_commit_id.is_none());
    let final_files = read_all_files(&lix, fixture_files.keys()).await;
    let storage_bytes_after = directory_bytes(&db_path);

    source.close().await.expect("close plugin source");
    lix.close().await.expect("close plugin target");
    drop(source);
    drop(lix);
    let reopen_started = Instant::now();
    let reopened = open_benchmark::<StorageImpl>(&db_path).await;
    let reopen_ms = elapsed_ms(reopen_started);
    assert_eq!(
        read_all_files(&reopened, fixture_files.keys()).await,
        final_files,
        "plugin files changed across reopen"
    );
    verify_plugin_results(&reopened, &cfg).await;
    reopened
        .close()
        .await
        .expect("close reopened plugin fixture");

    println!("{}", serde_json::to_string(&json!({
        "schema_version": SCHEMA_VERSION,
        "provenance": provenance,
        "status": "ok",
        "storage_backend": StorageImpl::NAME,
        "layer": "files",
        "scenario": cfg.scenario,
        "sample": benchmark_sample(),
        "parameters": {
            "plugins_enabled": ["text", "markdown", "json", "csv", "excalidraw"],
            "files_affected": cfg.changes,
            "unaffected_files": cfg.branches * 5,
            "semantic_rows_per_file": cfg.rows,
            "payload_bytes": cfg.payload_bytes,
            "temperature": if cold_reopen { "cold_reopen" } else { "warm" },
        },
        "latency_ms": {
            "process_total": elapsed_ms(total_started), "open": open_ms,
            "install_all_plugins": install_plugins_ms, "create_branch": create_branch_ms,
            "preview": preview_measure.wall_ms, "merge": merge_measure.wall_ms, "reopen": reopen_ms,
        },
        "cpu_ms": { "preview": preview_measure.cpu_ms, "merge": merge_measure.cpu_ms },
        "allocated_bytes": {
            "preview": preview_measure.allocated_bytes,
            "merge": merge_measure.allocated_bytes,
        },
        "rss_bytes": {
            "process_baseline": process_baseline_rss, "plugins_loaded": plugins_loaded_rss,
            "plugin_load_delta": plugins_loaded_rss.saturating_sub(process_baseline_rss),
            "preview_baseline": preview_measure.before.rss_bytes, "preview_peak": preview_measure.peak_rss_bytes,
            "preview_incremental_peak": preview_measure.peak_rss_bytes.saturating_sub(preview_measure.before.rss_bytes),
            "preview_retained": signed_delta(preview_measure.after.rss_bytes, preview_measure.before.rss_bytes),
            "merge_baseline": merge_measure.before.rss_bytes, "merge_peak": merge_measure.peak_rss_bytes,
            "merge_incremental_peak": merge_measure.peak_rss_bytes.saturating_sub(merge_measure.before.rss_bytes),
            "merge_retained": signed_delta(merge_measure.after.rss_bytes, merge_measure.before.rss_bytes),
        },
        "io_bytes": {
            "measurement": io_measurement_metadata(),
            "preview_read": preview_measure.after.read_bytes.saturating_sub(preview_measure.before.read_bytes),
            "preview_write": preview_measure.after.write_bytes.saturating_sub(preview_measure.before.write_bytes),
            "merge_read": merge_measure.after.read_bytes.saturating_sub(merge_measure.before.read_bytes),
            "merge_write": merge_measure.after.write_bytes.saturating_sub(merge_measure.before.write_bytes),
            "storage_before": storage_bytes_before, "storage_after_merge": storage_bytes_after,
            "storage_growth_after_merge": signed_delta(storage_bytes_after, storage_bytes_before),
        },
        "phase_ms": { "preview": preview_phases, "merge": merge_phases },
        "correctness": {
            "all_plugins_installed": true, "preview_non_mutating": true,
            "preview_commit_agree": true, "plugin_resolvers_invoked": true,
            "semantic_merge_oracle": true, "materialized_bytes_valid": true,
            "unaffected_plugin_files_not_rewritten": true,
            "source_branch_unchanged": true, "merge_parent_count": 2, "close_reopen_stable": true,
            "heads_and_merge_base": true, "repeated_merge_idempotent": true,
        },
        "outcome": "merge_committed",
    })).expect("serialize plugin result"));
}

async fn run_row_case<StorageImpl>(cfg: Config, collector: PerfSpanCollector)
where
    StorageImpl: BenchmarkStorage,
{
    let provenance = benchmark_provenance();
    let total_started = Instant::now();
    let root = benchmark_tempdir();
    let db_path = root.path().join(".lix");
    let open_started = Instant::now();
    let mut lix = open_benchmark::<StorageImpl>(&db_path).await;
    let open_ms = elapsed_ms(open_started);
    register_row_schema(&lix).await;
    seed_rows(&lix, &cfg).await;
    seed_history(&lix, &cfg).await;
    let setup_phases = collector.take_ms();
    lix.close().await.expect("close row setup fixture");
    drop(lix);
    let setup_reopen_started = Instant::now();
    lix = open_benchmark::<StorageImpl>(&db_path).await;
    let setup_reopen_ms = elapsed_ms(setup_reopen_started);
    let main_branch_id = lix.active_branch_id().await.expect("main branch id");

    collector.clear();
    let branch_measure = measure_async(|| async {
        let mut per_branch_ms = Vec::with_capacity(cfg.branches);
        for index in 0..cfg.branches.saturating_sub(1) {
            let started = Instant::now();
            lix.create_branch(CreateBranchOptions {
                id: Some(format!("01920000-0000-7000-8000-{:012x}", 0xc000 + index)),
                name: format!("fanout-{index}"),
                from_commit_id: None,
            })
            .await
            .expect("create fanout branch");
            per_branch_ms.push(elapsed_ms(started));
        }
        let started = Instant::now();
        let receipt = lix
            .create_branch(CreateBranchOptions {
                id: Some(SOURCE_BRANCH_ID.to_owned()),
                name: "merge-source".to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("create merge source");
        per_branch_ms.push(elapsed_ms(started));
        (receipt, per_branch_ms)
    })
    .await;
    let branch_phases = collector.take_ms();
    let (source_receipt, per_branch_ms) = &branch_measure.value;
    let mut sorted_branch_ms = per_branch_ms.clone();
    sorted_branch_ms.sort_by(f64::total_cmp);
    let branch_first_ms = per_branch_ms[0];
    let branch_median_ms = sorted_branch_ms[sorted_branch_ms.len() / 2];
    let branch_last_ms = *per_branch_ms.last().expect("source branch timing");
    let branch_max_ms = sorted_branch_ms[sorted_branch_ms.len() - 1];
    let create_branches_ms = branch_measure.wall_ms;
    let source = lix
        .open_another_session()
        .await
        .expect("open source session");
    source
        .switch_branch(SwitchBranchOptions {
            branch_id: source_receipt.id.clone(),
        })
        .await
        .expect("switch session branch");

    let switch_measure = measure_async(|| async {
        lix.switch_branch(SwitchBranchOptions {
            branch_id: source_receipt.id.clone(),
        })
        .await
        .expect("switch to source");
        lix.switch_branch(SwitchBranchOptions {
            branch_id: main_branch_id.clone(),
        })
        .await
        .expect("switch to main");
    })
    .await;
    let switch_roundtrip_ms = switch_measure.wall_ms;
    assert_eq!(
        source.active_branch_id().await.expect("pinned source"),
        SOURCE_BRANCH_ID
    );
    assert_eq!(
        lix.active_branch_id().await.expect("pinned main"),
        main_branch_id
    );

    let (base, target_ops, source_ops) = prepare_row_scenario(&lix, &source, &cfg).await;
    let expected = model_merge(&base, &target_ops, &source_ops);
    let expected_clean = expected.is_ok();
    let target_before_preview = read_rows(&lix).await;
    let source_before_preview = read_rows(&source).await;
    let storage_bytes_before = directory_bytes(&db_path);

    collector.clear();
    let preview_measure = measure_async(|| async {
        lix.merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: SOURCE_BRANCH_ID.to_owned(),
        })
        .await
    })
    .await;
    let preview = preview_measure
        .value
        .expect("merge preview should return a receipt");
    let preview_phases = collector.take_ms();
    assert_eq!(
        read_rows(&lix).await,
        target_before_preview,
        "preview mutated target state"
    );
    assert_eq!(
        read_rows(&source).await,
        source_before_preview,
        "preview mutated source state"
    );
    assert_eq!(
        preview.conflicts.is_empty(),
        expected_clean,
        "preview/model conflict disagreement"
    );
    assert_eq!(
        branch_head(&lix, &main_branch_id).await,
        preview.target_head_commit_id
    );
    assert_eq!(
        branch_head(&lix, SOURCE_BRANCH_ID).await,
        preview.source_head_commit_id
    );

    collector.clear();
    let merge_measure = measure_async(|| async {
        lix.merge_branch(MergeBranchOptions {
            source_branch_id: SOURCE_BRANCH_ID.to_owned(),
        })
        .await
    })
    .await;
    let merge_phases = collector.take_ms();
    let mut merge_outcome = "conflict".to_owned();
    let mut merge_parents = 0usize;
    match expected {
        Ok(expected_rows) => {
            let receipt = merge_measure
                .value
                .expect("model-clean merge should succeed");
            merge_outcome = outcome_name(receipt.outcome).to_owned();
            assert_eq!(
                preview.outcome, receipt.outcome,
                "preview/commit outcome disagreement"
            );
            assert_eq!(
                preview.change_stats, receipt.change_stats,
                "preview/commit stats disagreement"
            );
            assert_eq!(preview.base_commit_id, receipt.base_commit_id);
            assert_eq!(
                read_rows(&lix).await,
                expected_rows,
                "merged rows differ from independent model"
            );
            assert_eq!(
                read_rows(&source).await,
                source_before_preview,
                "merge mutated source branch"
            );
            assert_eq!(
                receipt.target_head_before_commit_id,
                preview.target_head_commit_id
            );
            assert_eq!(
                receipt.source_head_before_commit_id,
                preview.source_head_commit_id
            );
            assert_eq!(
                branch_head(&lix, &main_branch_id).await,
                receipt.target_head_after_commit_id
            );
            assert_eq!(
                branch_head(&lix, SOURCE_BRANCH_ID).await,
                receipt.source_head_before_commit_id
            );
            if receipt.outcome == MergeBranchOutcome::MergeCommitted {
                merge_parents =
                    commit_parent_count(&lix, &receipt.target_head_after_commit_id).await;
                assert_eq!(merge_parents, 2, "merge commit must have two parents");
            }
            let repeat = lix
                .merge_branch_preview(MergeBranchPreviewOptions {
                    source_branch_id: SOURCE_BRANCH_ID.to_owned(),
                })
                .await
                .expect("repeat preview");
            assert_eq!(repeat.outcome, MergeBranchOutcome::AlreadyUpToDate);
            let repeated_merge = lix
                .merge_branch(MergeBranchOptions {
                    source_branch_id: SOURCE_BRANCH_ID.to_owned(),
                })
                .await
                .expect("repeated merge should be idempotent");
            assert_eq!(repeated_merge.outcome, MergeBranchOutcome::AlreadyUpToDate);
            assert!(repeated_merge.created_merge_commit_id.is_none());
            assert_eq!(
                repeated_merge.target_head_after_commit_id,
                receipt.target_head_after_commit_id
            );
        }
        Err(conflicting_ids) => {
            assert_eq!(preview.conflicts.len(), conflicting_ids.len());
            let error = merge_measure
                .value
                .expect_err("model conflict must reject merge");
            assert_eq!(error.code, "LIX_MERGE_CONFLICT");
            assert_eq!(
                read_rows(&lix).await,
                target_before_preview,
                "failed merge partially mutated target"
            );
            assert_eq!(
                read_rows(&source).await,
                source_before_preview,
                "failed merge mutated source"
            );
            assert_eq!(
                branch_head(&lix, &main_branch_id).await,
                preview.target_head_commit_id
            );
            assert_eq!(
                branch_head(&lix, SOURCE_BRANCH_ID).await,
                preview.source_head_commit_id
            );
        }
    }

    let expected_diff = map_diff_oracle(&base, &target_before_preview);
    let diff_measure = measure_async(|| async {
        lix.execute(
            "SELECT row_pk, diff_type, before_change_id, after_change_id \
             FROM lix_diff($1, $2) WHERE schema_key = 'branch_bench_row' ORDER BY row_pk",
            &[
                Value::Text(preview.base_commit_id.clone()),
                Value::Text(preview.target_head_commit_id.clone()),
            ],
        )
        .await
        .expect("historical row diff")
    })
    .await;
    let observed_diff = diff_measure
        .value
        .rows()
        .iter()
        .map(|row| {
            let row_pk = row
                .get::<serde_json::Value>("row_pk")
                .expect("diff row identity");
            let id = row_pk
                .as_array()
                .and_then(|parts| parts.first())
                .and_then(serde_json::Value::as_str)
                .expect("single-string diff identity")
                .to_owned();
            let kind = row.get::<String>("diff_type").expect("diff type");
            let change_id = |column| match row.value(column).expect("diff change-id column") {
                Value::Null => None,
                Value::Text(id) => Some(id.clone()),
                other => panic!("unexpected {column} value {other:?}"),
            };
            let before = change_id("before_change_id");
            let after = change_id("after_change_id");
            match kind.as_str() {
                "added" => assert!(before.is_none() && after.is_some()),
                "modified" => assert!(before.is_some() && after.is_some() && before != after),
                // Removal is represented by a durable tombstone change, so
                // both endpoint change identities are present and distinct.
                "removed" => assert!(before.is_some() && after.is_some() && before != after),
                other => panic!("unexpected diff kind {other:?}"),
            }
            assert!(
                before.as_deref().is_none_or(|id| !id.is_empty())
                    && after.as_deref().is_none_or(|id| !id.is_empty()),
                "diff change identities must be non-empty"
            );
            (id, kind)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        observed_diff, expected_diff,
        "historical diff model mismatch"
    );

    let final_rows = read_rows(&lix).await;
    let storage_bytes_after = directory_bytes(&db_path);
    let fanout_branch_ids = (0..cfg.branches.saturating_sub(1))
        .map(|index| format!("01920000-0000-7000-8000-{:012x}", 0xc000 + index))
        .collect::<Vec<_>>();
    let delete_branches_measure = measure_async(|| async {
        for branch_id in &fanout_branch_ids {
            let result = lix
                .execute(
                    "DELETE FROM lix_branch WHERE id = $1",
                    &[Value::Text(branch_id.clone())],
                )
                .await
                .expect("delete fanout branch");
            assert_eq!(result.rows_affected(), 1);
        }
    })
    .await;
    for branch_id in &fanout_branch_ids {
        assert!(
            !branch_exists(&lix, branch_id).await,
            "deleted branch remained visible"
        );
    }
    let storage_bytes_after_branch_deletion = directory_bytes(&db_path);
    source.close().await.expect("close source session");
    lix.close().await.expect("close target session");
    drop(source);
    drop(lix);
    let reopen_started = Instant::now();
    let reopened = open_benchmark::<StorageImpl>(&db_path).await;
    let reopen_ms = elapsed_ms(reopen_started);
    assert_eq!(
        read_rows(&reopened).await,
        final_rows,
        "rows changed across close/reopen"
    );
    for branch_id in &fanout_branch_ids {
        assert!(
            !branch_exists(&reopened, branch_id).await,
            "deleted branch reappeared after reopen"
        );
    }
    let delete_branch_mean_ms = (!fanout_branch_ids.is_empty())
        .then_some(delete_branches_measure.wall_ms / fanout_branch_ids.len().max(1) as f64);
    reopened.close().await.expect("close reopened session");

    let result = json!({
        "schema_version": SCHEMA_VERSION,
        "provenance": provenance,
        "status": "ok",
        "storage_backend": StorageImpl::NAME,
        "layer": cfg.layer,
        "scenario": cfg.scenario,
        "sample": benchmark_sample(),
        "parameters": {
            "total_rows": cfg.rows,
            "changed_rows_per_side": cfg.changes,
            "divergent_commits_per_side": cfg.divergent_commits,
            "common_history_commits": cfg.history,
            "live_branches": cfg.branches,
            "deleted_fanout_branches": fanout_branch_ids.len(),
            "payload_bytes": cfg.payload_bytes,
        },
        "latency_ms": {
            "process_total": elapsed_ms(total_started),
            "open": open_ms,
            "setup_reopen": setup_reopen_ms,
            "create_branches_total": create_branches_ms,
            "create_branch_mean": create_branches_ms / cfg.branches as f64,
            "create_branch_first": branch_first_ms,
            "create_branch_median": branch_median_ms,
            "create_branch_last": branch_last_ms,
            "create_branch_max": branch_max_ms,
            "delete_branches_total": delete_branches_measure.wall_ms,
            "delete_branch_mean": delete_branch_mean_ms,
            "switch_roundtrip": switch_roundtrip_ms,
            "preview": preview_measure.wall_ms,
            "merge": merge_measure.wall_ms,
            "diff": diff_measure.wall_ms,
            "reopen": reopen_ms,
        },
        "cpu_ms": {
            "create_branches": branch_measure.cpu_ms, "switch_roundtrip": switch_measure.cpu_ms,
            "delete_branches": delete_branches_measure.cpu_ms,
            "preview": preview_measure.cpu_ms, "merge": merge_measure.cpu_ms,
            "diff": diff_measure.cpu_ms
        },
        "allocated_bytes": {
            "create_branches": branch_measure.allocated_bytes,
            "delete_branches": delete_branches_measure.allocated_bytes,
            "switch_roundtrip": switch_measure.allocated_bytes,
            "preview": preview_measure.allocated_bytes,
            "merge": merge_measure.allocated_bytes,
            "diff": diff_measure.allocated_bytes,
        },
        "rss_bytes": {
            "preview_baseline": preview_measure.before.rss_bytes,
            "preview_peak": preview_measure.peak_rss_bytes,
            "preview_incremental_peak": preview_measure.peak_rss_bytes.saturating_sub(preview_measure.before.rss_bytes),
            "preview_retained": signed_delta(preview_measure.after.rss_bytes, preview_measure.before.rss_bytes),
            "merge_baseline": merge_measure.before.rss_bytes,
            "merge_peak": merge_measure.peak_rss_bytes,
            "merge_incremental_peak": merge_measure.peak_rss_bytes.saturating_sub(merge_measure.before.rss_bytes),
            "merge_retained": signed_delta(merge_measure.after.rss_bytes, merge_measure.before.rss_bytes),
            "create_branches_incremental_peak": branch_measure.peak_rss_bytes.saturating_sub(branch_measure.before.rss_bytes),
            "create_branches_retained": signed_delta(branch_measure.after.rss_bytes, branch_measure.before.rss_bytes),
            "delete_branches_baseline": delete_branches_measure.before.rss_bytes,
            "delete_branches_peak": delete_branches_measure.peak_rss_bytes,
            "delete_branches_incremental_peak": delete_branches_measure.peak_rss_bytes.saturating_sub(delete_branches_measure.before.rss_bytes),
            "delete_branches_retained": signed_delta(delete_branches_measure.after.rss_bytes, delete_branches_measure.before.rss_bytes),
            "switch_incremental_peak": switch_measure.peak_rss_bytes.saturating_sub(switch_measure.before.rss_bytes),
            "switch_retained": signed_delta(switch_measure.after.rss_bytes, switch_measure.before.rss_bytes),
            "diff_incremental_peak": diff_measure.peak_rss_bytes.saturating_sub(diff_measure.before.rss_bytes),
            "diff_retained": signed_delta(diff_measure.after.rss_bytes, diff_measure.before.rss_bytes),
        },
        "io_bytes": {
            "measurement": io_measurement_metadata(),
            "preview_read": preview_measure.after.read_bytes.saturating_sub(preview_measure.before.read_bytes),
            "preview_write": preview_measure.after.write_bytes.saturating_sub(preview_measure.before.write_bytes),
            "merge_read": merge_measure.after.read_bytes.saturating_sub(merge_measure.before.read_bytes),
            "merge_write": merge_measure.after.write_bytes.saturating_sub(merge_measure.before.write_bytes),
            "diff_read": diff_measure.after.read_bytes.saturating_sub(diff_measure.before.read_bytes),
            "diff_write": diff_measure.after.write_bytes.saturating_sub(diff_measure.before.write_bytes),
            "delete_branches_read": delete_branches_measure.after.read_bytes.saturating_sub(delete_branches_measure.before.read_bytes),
            "delete_branches_write": delete_branches_measure.after.write_bytes.saturating_sub(delete_branches_measure.before.write_bytes),
            "storage_before": storage_bytes_before,
            "storage_after_merge": storage_bytes_after,
            "storage_growth_after_merge": signed_delta(storage_bytes_after, storage_bytes_before),
            "storage_after_branch_deletion": storage_bytes_after_branch_deletion,
            "storage_growth_after_branch_deletion": signed_delta(storage_bytes_after_branch_deletion, storage_bytes_before),
        },
        "phase_ms": {
            "setup": setup_phases,
            "create_branches": branch_phases,
            "preview": preview_phases,
            "merge": merge_phases,
        },
        "correctness": {
            "independent_three_way_model": true,
            "preview_non_mutating": true,
            "preview_commit_agree": true,
            "failed_merge_atomic": (!expected_clean).then_some(true),
            "source_branch_unchanged": true,
            "branch_isolation": true,
            "branch_deletion_durable": true,
            "merge_parent_count": merge_parents,
            "close_reopen_stable": true,
            "heads_and_merge_base": true,
            "repeated_merge_idempotent": true,
            "historical_diff_model": true,
            "historical_diff_rows": observed_diff.len(),
        },
        "outcome": merge_outcome,
    });
    println!(
        "{}",
        serde_json::to_string(&result).expect("serialize result")
    );
}

struct Measurement<T> {
    value: T,
    wall_ms: f64,
    cpu_ms: f64,
    allocated_bytes: u64,
    before: ProcessCounters,
    after: ProcessCounters,
    peak_rss_bytes: u64,
}

async fn measure_async<F, Fut, T>(operation: F) -> Measurement<T>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let before = process_counters();
    let sampler = RssSampler::start();
    let allocated_before = ALLOCATED_BYTES.load(Ordering::Relaxed);
    let started = Instant::now();
    let value = operation().await;
    let wall_ms = elapsed_ms(started);
    let allocated_after = ALLOCATED_BYTES.load(Ordering::Relaxed);
    let peak_rss_bytes = sampler.finish();
    let after = process_counters();
    Measurement {
        value,
        wall_ms,
        cpu_ms: (after.cpu_ns.saturating_sub(before.cpu_ns)) as f64 / 1_000_000.0,
        allocated_bytes: allocated_after.saturating_sub(allocated_before),
        before,
        after,
        peak_rss_bytes,
    }
}

trait BenchmarkStorage: Storage + Clone + Send + Sync + 'static {
    const NAME: &'static str;

    fn open_for_benchmark(path: &Path) -> Self;
}

impl BenchmarkStorage for RocksDB {
    const NAME: &'static str = "rocksdb";

    fn open_for_benchmark(path: &Path) -> Self {
        Self::open(path).expect("open benchmark RocksDB")
    }
}

impl BenchmarkStorage for SlateDB {
    const NAME: &'static str = "slatedb";

    fn open_for_benchmark(path: &Path) -> Self {
        Self::open(path).expect("open benchmark SlateDB")
    }
}

fn benchmark_storage_name() -> String {
    std::env::var("LIX_BRANCH_MERGE_BENCH_STORAGE").unwrap_or_else(|_| "rocksdb".to_owned())
}

fn benchmark_provenance() -> serde_json::Value {
    let executable = std::env::current_exe().expect("resolve benchmark executable");
    let executable_bytes = fs::read(&executable).expect("read benchmark executable for hashing");
    json!({
        "commit_sha": option_env!("LIX_BENCH_COMMIT_SHA").unwrap_or("unrecorded"),
        "binary_sha256": format!("{:x}", Sha256::digest(&executable_bytes)),
        "rustc_version": option_env!("LIX_BENCH_RUSTC_VERSION").unwrap_or("unrecorded"),
        "cargo_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "target_arch": std::env::consts::ARCH,
        "target_os": std::env::consts::OS,
    })
}

fn io_measurement_metadata() -> serde_json::Value {
    json!({
        "source": if cfg!(target_os = "linux") { "/proc/self/io" } else { "unavailable" },
        "os_page_cache_controlled": false,
        "interpretation": "process physical I/O lower bound; zero does not prove zero logical reads",
    })
}

async fn open_benchmark<StorageImpl>(path: &Path) -> Lix<StorageImpl>
where
    StorageImpl: BenchmarkStorage,
{
    let storage = StorageImpl::open_for_benchmark(path);
    open_lix()
        .with_storage(storage)
        .await
        .expect("open benchmark Lix")
}

fn benchmark_tempdir() -> tempfile::TempDir {
    let parent = std::env::var_os("LIX_BRANCH_MERGE_BENCH_ROOT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/branch-merge-tmp")
        });
    fs::create_dir_all(&parent)
        .unwrap_or_else(|error| panic!("create benchmark temp root {}: {error}", parent.display()));
    tempfile::Builder::new()
        .prefix("case-")
        .tempdir_in(&parent)
        .unwrap_or_else(|error| {
            panic!(
                "create benchmark directory in {}: {error}",
                parent.display()
            )
        })
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| {
            let path = entry.path();
            match entry.metadata() {
                Ok(metadata) if metadata.is_dir() => directory_bytes(&path),
                Ok(metadata) => metadata.len(),
                Err(_) => 0,
            }
        })
        .sum()
}

async fn register_row_schema<StorageImpl>(lix: &Lix<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let schema = json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "branch_bench_row",
        "columns": [
            { "name": "id", "type": "text", "nullable": false },
            { "name": "value", "type": "text", "nullable": false },
        ],
        "primary_key": ["id"],
    });
    let result = lix.execute(
        "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (CAST($1 AS JSONB), false, false)",
        &[Value::Text(schema.to_string())],
    ).await.expect("register benchmark schema");
    assert_eq!(result.rows_affected(), 1);
}

async fn seed_rows<StorageImpl>(lix: &Lix<StorageImpl>, cfg: &Config)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = lix
        .begin_transaction()
        .await
        .expect("begin benchmark seed transaction");
    for start in (0..cfg.rows).step_by(INSERT_BATCH) {
        let end = (start + INSERT_BATCH).min(cfg.rows);
        let mut sql = String::from("INSERT INTO branch_bench_row (id, value) VALUES ");
        let mut params = Vec::with_capacity((end - start) * 2);
        for (offset, index) in (start..end).enumerate() {
            if offset > 0 {
                sql.push(',');
            }
            write!(sql, "(${}, ${})", offset * 2 + 1, offset * 2 + 2).expect("build seed SQL");
            params.push(Value::Text(row_id(index)));
            params.push(Value::Text(payload("base", index, cfg.payload_bytes)));
        }
        let result = transaction
            .execute(&sql, &params)
            .await
            .expect("seed benchmark rows");
        assert_eq!(result.rows_affected() as usize, end - start);
    }
    transaction
        .commit()
        .await
        .expect("commit benchmark seed rows");
}

async fn seed_history<StorageImpl>(lix: &Lix<StorageImpl>, cfg: &Config)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    if cfg.history == 0 {
        return;
    }
    for commit in 0..cfg.history {
        let index = cfg.rows - 1 - (commit % (cfg.rows - cfg.changes * 2).max(1));
        lix.execute(
            "UPDATE branch_bench_row SET value = $1 WHERE id = $2",
            &[
                Value::Text(payload("history", commit, cfg.payload_bytes)),
                Value::Text(row_id(index)),
            ],
        )
        .await
        .expect("seed common history");
    }
}

async fn prepare_row_scenario<StorageImpl>(
    target: &Lix<StorageImpl>,
    source: &Lix<StorageImpl>,
    cfg: &Config,
) -> (
    BTreeMap<String, String>,
    BTreeMap<String, RowValue>,
    BTreeMap<String, RowValue>,
)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let base = read_rows(target).await;
    let mut target_ops = BTreeMap::new();
    let mut source_ops = BTreeMap::new();
    match cfg.scenario.as_str() {
        "already_up_to_date" => {}
        "fast_forward" => add_updates(&mut source_ops, 0, cfg.changes, "source", cfg.payload_bytes),
        "clean" => {
            add_updates(&mut target_ops, 0, cfg.changes, "target", cfg.payload_bytes);
            add_updates(
                &mut source_ops,
                cfg.changes,
                cfg.changes,
                "source",
                cfg.payload_bytes,
            );
        }
        "equal_convergence" => {
            add_updates(&mut target_ops, 0, cfg.changes, "equal", cfg.payload_bytes);
            add_updates(&mut source_ops, 0, cfg.changes, "equal", cfg.payload_bytes);
        }
        "modify_conflict" => {
            add_updates(&mut target_ops, 0, cfg.changes, "target", cfg.payload_bytes);
            add_updates(&mut source_ops, 0, cfg.changes, "source", cfg.payload_bytes);
        }
        "source_delete" => add_deletes(&mut source_ops, 0, cfg.changes),
        "delete_modify_conflict" => {
            add_deletes(&mut target_ops, 0, cfg.changes);
            add_updates(&mut source_ops, 0, cfg.changes, "source", cfg.payload_bytes);
        }
        "both_delete" => {
            add_deletes(&mut target_ops, 0, cfg.changes);
            add_deletes(&mut source_ops, 0, cfg.changes);
        }
        "add_same" | "add_conflict" => {
            for index in 0..cfg.changes {
                let id = format!("added-{index:012}");
                target_ops.insert(
                    id.clone(),
                    RowValue::Present(payload("added", index, cfg.payload_bytes)),
                );
                let prefix = if cfg.scenario == "add_same" {
                    "added"
                } else {
                    "source-added"
                };
                source_ops.insert(
                    id,
                    RowValue::Present(payload(prefix, index, cfg.payload_bytes)),
                );
            }
        }
        "mixed_conflict" => {
            let conflicts = cfg.changes / 2;
            add_updates(&mut target_ops, 0, conflicts, "target", cfg.payload_bytes);
            add_updates(&mut source_ops, 0, conflicts, "source", cfg.payload_bytes);
            add_updates(
                &mut source_ops,
                cfg.changes,
                cfg.changes - conflicts,
                "clean-source",
                cfg.payload_bytes,
            );
        }
        other => panic!("unknown row scenario {other:?}"),
    }
    apply_ops(target, &target_ops, cfg.divergent_commits).await;
    apply_ops(source, &source_ops, cfg.divergent_commits).await;
    (base, target_ops, source_ops)
}

fn add_updates(
    ops: &mut BTreeMap<String, RowValue>,
    start: usize,
    count: usize,
    prefix: &str,
    bytes: usize,
) {
    for index in start..start + count {
        ops.insert(
            row_id(index),
            RowValue::Present(payload(prefix, index, bytes)),
        );
    }
}

fn add_deletes(ops: &mut BTreeMap<String, RowValue>, start: usize, count: usize) {
    for index in start..start + count {
        ops.insert(row_id(index), RowValue::Absent);
    }
}

async fn apply_ops<StorageImpl>(
    lix: &Lix<StorageImpl>,
    ops: &BTreeMap<String, RowValue>,
    requested_commits: usize,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    if ops.is_empty() {
        return;
    }
    let ops = ops.iter().collect::<Vec<_>>();
    let commit_count = requested_commits.min(ops.len());
    let chunk_size = ops.len().div_ceil(commit_count);
    for chunk in ops.chunks(chunk_size) {
        let mut transaction = lix
            .begin_transaction()
            .await
            .expect("begin divergent commit");
        for (id, value) in chunk {
            match value {
                RowValue::Present(value) => {
                    transaction.execute(
                    "INSERT INTO branch_bench_row (id, value) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET value = excluded.value",
                    &[Value::Text((*id).clone()), Value::Text((*value).clone())],
                ).await.expect("apply row upsert");
                }
                RowValue::Absent => {
                    transaction
                        .execute(
                            "DELETE FROM branch_bench_row WHERE id = $1",
                            &[Value::Text((*id).clone())],
                        )
                        .await
                        .expect("apply row delete");
                }
            }
        }
        transaction
            .commit()
            .await
            .expect("commit divergent row operations");
    }
}

fn model_merge(
    base: &BTreeMap<String, String>,
    target_ops: &BTreeMap<String, RowValue>,
    source_ops: &BTreeMap<String, RowValue>,
) -> Result<BTreeMap<String, String>, BTreeSet<String>> {
    let mut ids = base.keys().cloned().collect::<BTreeSet<_>>();
    ids.extend(target_ops.keys().cloned());
    ids.extend(source_ops.keys().cloned());
    let mut merged = BTreeMap::new();
    let mut conflicts = BTreeSet::new();
    for id in ids {
        let base_value = base
            .get(&id)
            .cloned()
            .map(RowValue::Present)
            .unwrap_or(RowValue::Absent);
        let target = target_ops
            .get(&id)
            .cloned()
            .unwrap_or_else(|| base_value.clone());
        let source = source_ops
            .get(&id)
            .cloned()
            .unwrap_or_else(|| base_value.clone());
        let selected = if target == source {
            Some(target)
        } else if target == base_value {
            Some(source)
        } else if source == base_value {
            Some(target)
        } else {
            conflicts.insert(id.clone());
            None
        };
        if let Some(RowValue::Present(value)) = selected {
            merged.insert(id, value);
        }
    }
    if conflicts.is_empty() {
        Ok(merged)
    } else {
        Err(conflicts)
    }
}

fn map_diff_oracle(
    before: &BTreeMap<String, String>,
    after: &BTreeMap<String, String>,
) -> Vec<(String, String)> {
    before
        .keys()
        .chain(after.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter_map(|id| match (before.get(id), after.get(id)) {
            (None, Some(_)) => Some((id.clone(), "added".to_owned())),
            (Some(_), None) => Some((id.clone(), "removed".to_owned())),
            (Some(before), Some(after)) if before != after => {
                Some((id.clone(), "modified".to_owned()))
            }
            _ => None,
        })
        .collect()
}

async fn read_rows<StorageImpl>(lix: &Lix<StorageImpl>) -> BTreeMap<String, String>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute("SELECT id, value FROM branch_bench_row ORDER BY id", &[])
        .await
        .expect("read benchmark rows")
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("id").expect("row id"),
                row.get::<String>("value").expect("row value"),
            )
        })
        .collect()
}

async fn commit_parent_count<StorageImpl>(lix: &Lix<StorageImpl>, commit_id: &str) -> usize
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT parent_id FROM lix_commit_edge WHERE child_id = $1",
        &[Value::Text(commit_id.to_owned())],
    )
    .await
    .expect("query merge parents")
    .rows()
    .len()
}

async fn branch_head<StorageImpl>(lix: &Lix<StorageImpl>, branch_id: &str) -> String
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT commit_id FROM lix_branch WHERE id = $1",
        &[Value::Text(branch_id.to_owned())],
    )
    .await
    .expect("query branch head")
    .rows()[0]
        .get::<String>("commit_id")
        .expect("branch head commit id")
}

async fn branch_exists<StorageImpl>(lix: &Lix<StorageImpl>, branch_id: &str) -> bool
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    !lix.execute(
        "SELECT id FROM lix_branch WHERE id = $1",
        &[Value::Text(branch_id.to_owned())],
    )
    .await
    .expect("query branch existence")
    .rows()
    .is_empty()
}

async fn install_all_plugins<StorageImpl>(lix: &Lix<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    for (key, archive) in [
        ("plugin_text", build_text_plugin_archive()),
        ("plugin_markdown", build_markdown_plugin_archive()),
        ("plugin_json", build_json_plugin_archive()),
        ("plugin_csv", build_csv_plugin_archive()),
        ("plugin_excalidraw", build_excalidraw_plugin_archive()),
    ] {
        write_file(lix, &format!("/.lix/plugins/{key}.lixplugin"), archive).await;
    }
}

async fn write_file<StorageImpl>(lix: &Lix<StorageImpl>, path: &str, bytes: Vec<u8>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        &[Value::Text(path.to_owned()), Value::Blob(bytes.into())],
    ).await.unwrap_or_else(|error| panic!("write plugin benchmark file {path}: {error:?}"));
}

async fn read_file<StorageImpl>(lix: &Lix<StorageImpl>, path: &str) -> Vec<u8>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT content FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .unwrap_or_else(|error| panic!("read plugin benchmark file {path}: {error:?}"))
    .rows()[0]
        .get::<Vec<u8>>("content")
        .expect("file data")
}

async fn read_all_files<'a, StorageImpl>(
    lix: &Lix<StorageImpl>,
    paths: impl Iterator<Item = &'a String>,
) -> BTreeMap<String, Vec<u8>>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut result = BTreeMap::new();
    for path in paths {
        result.insert(path.clone(), read_file(lix, path).await);
    }
    result
}

async fn file_change_ids<'a, StorageImpl>(
    lix: &Lix<StorageImpl>,
    paths: impl Iterator<Item = &'a String>,
) -> BTreeMap<String, String>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut result = BTreeMap::new();
    for path in paths {
        let change_id = lix
            .execute(
                "SELECT lixcol_change_id FROM lix_file WHERE path = $1",
                &[Value::Text(path.clone())],
            )
            .await
            .expect("read file change id")
            .rows()[0]
            .get::<String>("lixcol_change_id")
            .expect("file change id");
        result.insert(path.clone(), change_id);
    }
    result
}

fn plugin_control_files(cfg: &Config) -> BTreeMap<String, Vec<u8>> {
    let mut files = BTreeMap::new();
    for index in 0..cfg.branches {
        files.insert(format!("/control-{index:04}.txt"), b"control\n".to_vec());
        files.insert(format!("/control-{index:04}.md"), b"control\n".to_vec());
        files.insert(
            format!("/control-{index:04}.json"),
            serde_json::to_vec(&json!({"control": true})).expect("serialize JSON control"),
        );
        files.insert(
            format!("/control-{index:04}.csv"),
            b"control,value\nstable,true\n".to_vec(),
        );
        files.insert(
            format!("/control-{index:04}.excalidraw"),
            excalidraw_bytes(1, 20, 2, 16),
        );
    }
    files
}

fn plugin_base_files(cfg: &Config) -> BTreeMap<String, Vec<u8>> {
    let rows = cfg.rows.max(2);
    let filler = |index: usize| payload("record", index, cfg.payload_bytes);
    let text = (0..rows)
        .map(|index| match index {
            0 => "alpha".to_owned(),
            index if index + 1 == rows => "beta".to_owned(),
            index => filler(index),
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let markdown = (0..rows)
        .map(|index| match index {
            0 => "alpha".to_owned(),
            index if index + 1 == rows => "beta".to_owned(),
            index => filler(index),
        })
        .collect::<Vec<_>>()
        .join("\n\n")
        + "\n";
    let csv = (0..rows)
        .map(|index| match index {
            0 => "alpha,one".to_owned(),
            index if index + 1 == rows => "beta,two".to_owned(),
            index => format!("row-{index},{}", filler(index)),
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let mut json_object = serde_json::Map::new();
    json_object.insert("pick".to_owned(), json!("base"));
    json_object.insert("stable".to_owned(), json!(true));
    for index in 2..rows {
        json_object.insert(format!("key-{index}"), json!(filler(index)));
    }
    let mut files = BTreeMap::from([
        ("/merge.txt".to_owned(), text.into_bytes()),
        ("/merge.md".to_owned(), markdown.into_bytes()),
        (
            "/merge.json".to_owned(),
            serde_json::to_vec(&json_object).expect("serialize JSON fixture"),
        ),
        ("/merge.csv".to_owned(), csv.into_bytes()),
        (
            "/merge.excalidraw".to_owned(),
            excalidraw_bytes(1, 20, rows, cfg.payload_bytes),
        ),
    ]);
    files.extend(plugin_extra_base_files(cfg));
    files
}

fn plugin_extra_base_files(cfg: &Config) -> BTreeMap<String, Vec<u8>> {
    (5..cfg.changes)
        .map(|index| extra_plugin_file(index, false))
        .collect()
}

fn plugin_extra_source_files(cfg: &Config) -> BTreeMap<String, Vec<u8>> {
    (5..cfg.changes)
        .map(|index| extra_plugin_file(index, true))
        .collect()
}

fn extra_plugin_file(index: usize, changed: bool) -> (String, Vec<u8>) {
    let value = if changed { "source" } else { "base" };
    match index % 5 {
        0 => (format!("/extra-{index:04}.txt"), format!("{value}-{index}\n").into_bytes()),
        1 => (format!("/extra-{index:04}.md"), format!("{value}-{index}\n").into_bytes()),
        2 => (format!("/extra-{index:04}.json"), serde_json::to_vec(&json!({"value": value, "index": index})).unwrap()),
        3 => (format!("/extra-{index:04}.csv"), format!("value,index\n{value},{index}\n").into_bytes()),
        _ => (format!("/extra-{index:04}.excalidraw"), serde_json::to_vec(&json!({
            "type":"excalidraw","version":2,"source":"https://excalidraw.com",
            "elements":[{"id":format!("extra-{index}"),"type":"rectangle","x":if changed { 2 } else { 1 },"y":2,"width":3,"height":4,"isDeleted":false}],
            "appState":{},"files":{}
        })).unwrap()),
    }
}

fn excalidraw_bytes(first_x: u64, second_x: u64, rows: usize, payload_bytes: usize) -> Vec<u8> {
    let mut elements = vec![
        json!({"id":"a","type":"rectangle","x":first_x,"y":2,"width":3,"height":4,"isDeleted":false}),
    ];
    for index in 1..rows.saturating_sub(1) {
        elements.push(json!({"id":format!("shape-{index}"),"type":"rectangle","x":index,"y":index,"width":3,"height":4,"isDeleted":false,"label":payload("shape", index, payload_bytes)}));
    }
    elements.push(json!({"id":"b","type":"ellipse","x":second_x,"y":30,"width":5,"height":6,"isDeleted":false}));
    serde_json::to_vec(&json!({"type":"excalidraw","version":2,"source":"https://excalidraw.com","elements":elements,"appState":{},"files":{}}))
        .expect("serialize Excalidraw fixture")
}

async fn file_id_at_path<StorageImpl>(lix: &Lix<StorageImpl>, path: &str) -> String
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT id FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .expect("query file id")
    .rows()[0]
        .get::<String>("id")
        .expect("file id")
}

async fn apply_plugin_semantic_edits<StorageImpl>(lix: &Lix<StorageImpl>, target: bool)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let text_id = file_id_at_path(lix, "/merge.txt").await;
    let text_rows = lix
        .execute(
            "SELECT id, content_base64 FROM text_line WHERE lixcol_file_id = $1 ORDER BY order_key",
            &[Value::Text(text_id.clone())],
        )
        .await
        .expect("query text lines");
    let text_index = if target {
        0
    } else {
        text_rows.rows().len() - 1
    };
    let text_row = &text_rows.rows()[text_index];
    let text_row_id = text_row.get::<String>("id").expect("text row id");
    let encoded = text_row
        .get::<String>("content_base64")
        .expect("text content");
    let original = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .expect("decode text line");
    let original = String::from_utf8(original).expect("text line UTF-8");
    let text_value = if target {
        original.replace("alpha", "ALPHA")
    } else {
        original.replace("beta", "BETA")
    };
    lix.execute(
        "UPDATE text_line SET content_base64 = $1 WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Text(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(text_value)),
            Value::Text(text_row_id),
            Value::Text(text_id),
        ],
    )
    .await
    .expect("edit text semantic line");

    let markdown_id = file_id_at_path(lix, "/merge.md").await;
    let markdown_rows = lix.execute(
        "SELECT id, payload_json FROM markdown_node WHERE lixcol_file_id = $1 AND kind = 'paragraph' ORDER BY order_key",
        &[Value::Text(markdown_id.clone())],
    ).await.expect("query Markdown nodes");
    let markdown_index = if target {
        0
    } else {
        markdown_rows.rows().len() - 1
    };
    let markdown_row = &markdown_rows.rows()[markdown_index];
    let markdown_row_id = markdown_row.get::<String>("id").expect("Markdown row id");
    let original_payload = markdown_row
        .get::<String>("payload_json")
        .expect("Markdown payload");
    let payload = if target {
        original_payload.replace("alpha", "ALPHA")
    } else {
        original_payload.replace("beta", "BETA")
    };
    lix.execute(
        "UPDATE markdown_node SET payload_json = $1 WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Text(payload),
            Value::Text(markdown_row_id),
            Value::Text(markdown_id),
        ],
    )
    .await
    .expect("edit Markdown semantic node");

    let json_id = file_id_at_path(lix, "/merge.json").await;
    let json_value = if target { r#""target""# } else { r#""source""# };
    lix.execute(
        "UPDATE json_object_member SET scalar_json = $1 WHERE parent_id = 'root' AND key = 'pick' AND lixcol_file_id = $2",
        &[Value::Text(json_value.to_owned()), Value::Text(json_id)],
    ).await.expect("edit JSON semantic member");

    let csv_id = file_id_at_path(lix, "/merge.csv").await;
    let csv_rows = lix
        .execute(
            "SELECT id FROM csv_row WHERE lixcol_file_id = $1 ORDER BY order_key",
            &[Value::Text(csv_id.clone())],
        )
        .await
        .expect("query CSV rows");
    let csv_index = if target { 0 } else { csv_rows.rows().len() - 1 };
    let csv_row_id = csv_rows.rows()[csv_index]
        .get::<String>("id")
        .expect("CSV row id");
    let cells = if target {
        json!(["ALPHA", "one"])
    } else {
        json!(["beta", "TWO"])
    };
    lix.execute(
        "UPDATE csv_row SET cells = $1 WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Jsonb(cells.into()),
            Value::Text(csv_row_id),
            Value::Text(csv_id),
        ],
    )
    .await
    .expect("edit CSV semantic row");

    let drawing_id = file_id_at_path(lix, "/merge.excalidraw").await;
    let element_id = if target { "a" } else { "b" };
    let original = lix
        .execute(
            "SELECT element_json FROM excalidraw_element WHERE id = $1 AND lixcol_file_id = $2",
            &[
                Value::Text(element_id.to_owned()),
                Value::Text(drawing_id.clone()),
            ],
        )
        .await
        .expect("query Excalidraw element")
        .rows()[0]
        .get::<String>("element_json")
        .expect("element JSON");
    let (before, after) = if target {
        (r#""x":1"#, r#""x":111"#)
    } else {
        (r#""x":20"#, r#""x":222"#)
    };
    let replacement = original.replacen(before, after, 1);
    assert_ne!(
        replacement, original,
        "Excalidraw x coordinate should be replaceable"
    );
    lix.execute(
        "UPDATE excalidraw_element SET element_json = $1 WHERE id = $2 AND lixcol_file_id = $3",
        &[
            Value::Text(replacement),
            Value::Text(element_id.to_owned()),
            Value::Text(drawing_id),
        ],
    )
    .await
    .expect("edit Excalidraw semantic element");
}

async fn verify_plugin_results<StorageImpl>(lix: &Lix<StorageImpl>, cfg: &Config)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let base = plugin_base_files(cfg);
    let expected = |path: &str, first: &str, last: &str| {
        let source = String::from_utf8(base[path].clone()).expect("UTF-8 plugin fixture");
        source
            .replacen("alpha", first, 1)
            .replacen("beta", last, 1)
            .into_bytes()
    };
    assert_eq!(
        read_file(lix, "/merge.txt").await,
        expected("/merge.txt", "ALPHA", "BETA")
    );
    assert_eq!(
        read_file(lix, "/merge.md").await,
        expected("/merge.md", "ALPHA", "BETA")
    );
    let expected_csv = String::from_utf8(base["/merge.csv"].clone())
        .expect("CSV fixture UTF-8")
        .replacen("alpha", "ALPHA", 1)
        .replacen("beta,two", "beta,TWO", 1)
        .into_bytes();
    assert_eq!(read_file(lix, "/merge.csv").await, expected_csv);
    let json_bytes = read_file(lix, "/merge.json").await;
    let json_value: serde_json::Value =
        serde_json::from_slice(&json_bytes).expect("merged JSON bytes parse");
    assert_eq!(json_value["pick"], json!("source"));
    assert_eq!(json_value["stable"], json!(true));
    assert_eq!(
        json_value.as_object().expect("merged JSON object").len(),
        cfg.rows.max(2)
    );
    let drawing_bytes = read_file(lix, "/merge.excalidraw").await;
    let drawing: serde_json::Value =
        serde_json::from_slice(&drawing_bytes).expect("merged Excalidraw bytes parse");
    let elements = drawing["elements"].as_array().expect("Excalidraw elements");
    let by_id = elements
        .iter()
        .map(|element| (element["id"].as_str().unwrap(), element))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(by_id["a"]["x"], json!(111));
    assert_eq!(by_id["b"]["x"], json!(222));
    for (path, expected) in plugin_extra_source_files(cfg) {
        assert_eq!(
            read_file(lix, &path).await,
            expected,
            "extra affected file {path} did not merge"
        );
    }
    let rows = cfg.rows.max(2) as i64;
    let extra_count = |remainder: usize| {
        (5..cfg.changes)
            .filter(|index| index % 5 == remainder)
            .count() as i64
    };
    for (table, expected) in [
        ("text_line", rows + extra_count(0) + cfg.branches as i64),
        (
            "markdown_node",
            rows + 1 + extra_count(1) * 2 + cfg.branches as i64 * 2,
        ),
        (
            "json_object_member",
            rows + extra_count(2) * 2 + cfg.branches as i64,
        ),
        (
            "csv_row",
            rows + extra_count(3) * 2 + cfg.branches as i64 * 2,
        ),
        (
            "excalidraw_element",
            rows + extra_count(4) + cfg.branches as i64 * 2,
        ),
    ] {
        let sql = format!("SELECT COUNT(*) AS count FROM {table}");
        let count = lix
            .execute(&sql, &[])
            .await
            .expect("query semantic row count")
            .rows()[0]
            .get::<i64>("count")
            .expect("semantic count");
        assert_eq!(count, expected, "unexpected semantic rows in {table}");
    }
}

fn build_text_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_TEXT_plugin_text")),
        include_str!("../../../plugins/text/manifest.json"),
        &[(
            "schema/text_line.json",
            include_str!("../../../plugins/text/schema/text_line.json"),
        )],
    )
}

fn build_markdown_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_plugin_markdown")),
        include_str!("../../../plugins/markdown/manifest.json"),
        &[(
            "schema/markdown_node.json",
            include_str!("../../../plugins/markdown/schema/markdown_node.json"),
        )],
    )
}

fn build_json_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_plugin_json")),
        include_str!("../../../plugins/json/manifest.json"),
        &[
            (
                "schema/json_root.json",
                include_str!("../../../plugins/json/schema/json_root.json"),
            ),
            (
                "schema/json_object_member.json",
                include_str!("../../../plugins/json/schema/json_object_member.json"),
            ),
            (
                "schema/json_array_item.json",
                include_str!("../../../plugins/json/schema/json_array_item.json"),
            ),
        ],
    )
}

fn build_csv_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv")),
        include_str!("../../../plugins/csv/manifest.json"),
        &[
            (
                "schema/csv_table.json",
                include_str!("../../../plugins/csv/schema/csv_table.json"),
            ),
            (
                "schema/csv_row.json",
                include_str!("../../../plugins/csv/schema/csv_row.json"),
            ),
        ],
    )
}

fn build_excalidraw_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!(
            "CARGO_CDYLIB_FILE_PLUGIN_EXCALIDRAW_plugin_excalidraw"
        )),
        include_str!("../../../plugins/excalidraw/manifest.json"),
        &[
            (
                "schema/excalidraw_scene.json",
                include_str!("../../../plugins/excalidraw/schema/excalidraw_scene.json"),
            ),
            (
                "schema/excalidraw_element.json",
                include_str!("../../../plugins/excalidraw/schema/excalidraw_element.json"),
            ),
            (
                "schema/excalidraw_file.json",
                include_str!("../../../plugins/excalidraw/schema/excalidraw_file.json"),
            ),
        ],
    )
}

fn build_plugin_archive(wasm_path: &Path, manifest: &str, schemas: &[(&str, &str)]) -> Vec<u8> {
    let wasm = fs::read(wasm_path)
        .unwrap_or_else(|error| panic!("read plugin {}: {error}", wasm_path.display()));
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer
        .start_file("manifest.json", options)
        .expect("start manifest");
    writer
        .write_all(manifest.as_bytes())
        .expect("write manifest");
    for (path, schema) in schemas {
        writer.start_file(*path, options).expect("start schema");
        writer.write_all(schema.as_bytes()).expect("write schema");
    }
    writer
        .start_file("plugin.wasm", options)
        .expect("start plugin component");
    writer.write_all(&wasm).expect("write plugin component");
    writer.finish().expect("finish plugin archive").into_inner()
}

fn row_id(index: usize) -> String {
    format!("row-{index:012}")
}

fn payload(prefix: &str, index: usize, bytes: usize) -> String {
    let head = format!("{prefix}-{index:012}-");
    if head.len() >= bytes {
        head
    } else {
        format!("{head}{}", "x".repeat(bytes - head.len()))
    }
}

fn outcome_name(outcome: MergeBranchOutcome) -> &'static str {
    match outcome {
        MergeBranchOutcome::AlreadyUpToDate => "already_up_to_date",
        MergeBranchOutcome::FastForward => "fast_forward",
        MergeBranchOutcome::MergeCommitted => "merge_committed",
    }
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000.0
}

fn benchmark_sample() -> usize {
    std::env::var("LIX_BRANCH_MERGE_BENCH_SAMPLE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn signed_delta(after: u64, before: u64) -> i64 {
    i64::try_from(after)
        .unwrap_or(i64::MAX)
        .saturating_sub(i64::try_from(before).unwrap_or(i64::MAX))
}

fn process_counters() -> ProcessCounters {
    let (read_bytes, write_bytes) = proc_io_bytes();
    ProcessCounters {
        cpu_ns: process_cpu_ns(),
        read_bytes,
        write_bytes,
        rss_bytes: current_rss_bytes(),
    }
}

#[cfg(target_os = "linux")]
fn current_rss_bytes() -> u64 {
    let status = fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
        * 1024
}

#[cfg(not(target_os = "linux"))]
fn current_rss_bytes() -> u64 {
    0
}

#[cfg(target_os = "linux")]
fn proc_io_bytes() -> (u64, u64) {
    let io = fs::read_to_string("/proc/self/io").expect("read /proc/self/io");
    let value = |prefix: &str| {
        io.lines()
            .find_map(|line| line.strip_prefix(prefix))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0)
    };
    (value("read_bytes:"), value("write_bytes:"))
}

#[cfg(not(target_os = "linux"))]
fn proc_io_bytes() -> (u64, u64) {
    (0, 0)
}

fn process_cpu_ns() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(result, 0, "getrusage failed");
    let usage = unsafe { usage.assume_init() };
    timeval_ns(usage.ru_utime).saturating_add(timeval_ns(usage.ru_stime))
}

fn timeval_ns(value: libc::timeval) -> u64 {
    (value.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add((value.tv_usec as u64).saturating_mul(1_000))
}
