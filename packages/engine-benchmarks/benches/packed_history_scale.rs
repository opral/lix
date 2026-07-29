use std::fmt::{self, Display, Formatter};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use lix_engine::storage::Storage;
use lix_engine::storage_adapter::StorageAdapter;
use lix_engine::tracked_state::bench::{
    BenchLayoutAccounting, load_packed_change, packed_history_layout, scan_packed_history,
    seed_packed_history,
};
use lix_rocksdb_storage::RocksDB;
use lix_slatedb_storage::SlateDB;

const DEFAULT_CHANGES: &[usize] = &[100_000];
const DEFAULT_COMMIT_WIDTHS: &[usize] = &[1, 10, 100, 10_000];
const DEFAULT_STORAGE_BATCH_CHANGES: usize = 100_000;
const DEFAULT_WARMUPS: usize = 1;
const DEFAULT_SAMPLES: usize = 5;
const DEFAULT_EXACT_SAMPLES: usize = 1_001;

#[derive(Clone, Copy, Debug)]
enum Backend {
    RocksDB,
    SlateDB,
}

impl Display for Backend {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::RocksDB => formatter.write_str("rocksdb"),
            Self::SlateDB => formatter.write_str("slatedb"),
        }
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create packed-history benchmark runtime");
    runtime.block_on(run());
}

async fn run() {
    let changes = env_usizes("LIX_PACKED_HISTORY_CHANGES", DEFAULT_CHANGES);
    let commit_widths = env_usizes("LIX_PACKED_HISTORY_COMMIT_WIDTHS", DEFAULT_COMMIT_WIDTHS);
    let storage_batch_changes = env_usize(
        "LIX_PACKED_HISTORY_STORAGE_BATCH_CHANGES",
        DEFAULT_STORAGE_BATCH_CHANGES,
    );
    let warmups = env_nonnegative_usize("LIX_PACKED_HISTORY_WARMUPS", DEFAULT_WARMUPS);
    let samples = env_usize("LIX_PACKED_HISTORY_SAMPLES", DEFAULT_SAMPLES).max(1);
    let exact_samples = env_usize("LIX_PACKED_HISTORY_EXACT_SAMPLES", DEFAULT_EXACT_SAMPLES).max(1);
    let flush = env_bool("LIX_PACKED_HISTORY_FLUSH", true);
    let memtable_flush = env_bool("LIX_PACKED_HISTORY_MEMTABLE_FLUSH", false);
    let account_layout = env_bool("LIX_PACKED_HISTORY_ACCOUNT_LAYOUT", true);
    let skip_seed = env_bool("LIX_PACKED_HISTORY_SKIP_SEED", false);
    let persistent_root = std::env::var_os("LIX_PACKED_HISTORY_STORAGE_PATH").map(PathBuf::from);
    if persistent_root.is_some() {
        assert_eq!(
            changes.len(),
            1,
            "persistent storage requires one history size"
        );
        assert_eq!(
            commit_widths.len(),
            1,
            "persistent storage requires one commit width"
        );
    }

    for backend in [Backend::RocksDB, Backend::SlateDB] {
        if !selected("LIX_PACKED_HISTORY_BACKENDS", &backend.to_string()) {
            continue;
        }
        for &change_count in &changes {
            for &commit_width in &commit_widths {
                if change_count % commit_width != 0 {
                    eprintln!(
                        "skip backend={backend} changes={change_count} commit_width={commit_width}: width does not divide changes"
                    );
                    continue;
                }
                match backend {
                    Backend::RocksDB => {
                        let dir = tempfile::tempdir().expect("create RocksDB benchmark directory");
                        let path = persistent_root.as_ref().map_or_else(
                            || dir.path().join("rocksdb"),
                            |root| root.join("rocksdb"),
                        );
                        let storage = RocksDB::open(&path).expect("open benchmark RocksDB");
                        run_case(
                            backend,
                            storage.clone(),
                            &path,
                            change_count,
                            commit_width,
                            storage_batch_changes,
                            warmups,
                            samples,
                            exact_samples,
                            flush,
                            false,
                            account_layout,
                            skip_seed,
                            || async {
                                storage.flush().expect("flush benchmark RocksDB");
                            },
                        )
                        .await;
                    }
                    Backend::SlateDB => {
                        let dir = tempfile::tempdir().expect("create SlateDB benchmark directory");
                        let path = persistent_root.as_ref().map_or_else(
                            || dir.path().join("slatedb"),
                            |root| root.join("slatedb"),
                        );
                        let storage = SlateDB::open(&path).expect("open benchmark SlateDB");
                        run_case(
                            backend,
                            storage.clone(),
                            &path,
                            change_count,
                            commit_width,
                            storage_batch_changes,
                            warmups,
                            samples,
                            exact_samples,
                            flush,
                            memtable_flush,
                            account_layout,
                            skip_seed,
                            || async {
                                if memtable_flush {
                                    storage
                                        .flush_memtable_for_diagnostics()
                                        .await
                                        .expect("flush benchmark SlateDB memtable");
                                } else {
                                    storage.flush().await.expect("flush benchmark SlateDB WAL");
                                }
                            },
                        )
                        .await;
                    }
                }
            }
        }
    }
}

#[expect(clippy::too_many_arguments)]
async fn run_case<S, Flush, FlushFuture>(
    backend: Backend,
    storage: S,
    storage_path: &Path,
    changes: usize,
    commit_width: usize,
    storage_batch_changes: usize,
    warmups: usize,
    samples: usize,
    exact_samples: usize,
    flush: bool,
    memtable_flush: bool,
    account_layout: bool,
    skip_seed: bool,
    flush_storage: Flush,
) where
    S: Storage + Clone,
    Flush: FnOnce() -> FlushFuture,
    FlushFuture: Future<Output = ()>,
{
    let adapter = StorageAdapter::new(storage);
    let id_shape =
        std::env::var("LIX_PACKED_HISTORY_ID_SHAPE").unwrap_or_else(|_| "deterministic".into());
    let seed_started = Instant::now();
    let writes = if skip_seed {
        None
    } else {
        Some(seed_packed_history(&adapter, changes, commit_width, storage_batch_changes).await)
    };
    let seed_elapsed = seed_started.elapsed();
    if flush {
        flush_storage().await;
    }
    let backend_bytes = directory_bytes(storage_path);

    for _ in 0..warmups {
        assert_eq!(scan_packed_history(&adapter).await, changes);
    }
    let mut timings = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        assert_eq!(scan_packed_history(&adapter).await, changes);
        timings.push(started.elapsed());
    }
    timings.sort_unstable();
    let exact_commit_index = changes / commit_width / 2;
    let exact_row_index = commit_width / 2;
    for _ in 0..warmups {
        assert!(load_packed_change(&adapter, exact_commit_index, exact_row_index).await);
    }
    let mut exact_timings = Vec::with_capacity(exact_samples);
    for _ in 0..exact_samples {
        let started = Instant::now();
        assert!(load_packed_change(&adapter, exact_commit_index, exact_row_index).await);
        exact_timings.push(started.elapsed());
    }
    exact_timings.sort_unstable();
    // Account after timing so a layout scan cannot pre-warm the measured
    // storage pages. `warmups=0,samples=1` is therefore the cold profile.
    let layout = if account_layout {
        packed_history_layout(&adapter).await
    } else {
        Vec::new()
    };
    let manifest = find_layout(&layout, "tracked_state.commit_delta_manifest.v2");
    let segments = find_layout(&layout, "tracked_state.commit_delta_segment.v2");
    let locators = find_layout(&layout, "tracked_state.change_locator.v1");

    println!(
        "packed_history_scale,backend={backend},id_shape={id_shape},changes={changes},commits={},commit_width={commit_width},\
         storage_batch_changes={storage_batch_changes},flushed={flush},warmups={warmups},samples={samples},\
         memtable_flushed={memtable_flush},skip_seed={skip_seed},\
         layout_accounted={account_layout},\
         exact_samples={exact_samples},seed_ms={},ingest_changes_per_second={:.1},staged_puts={},logical_written_bytes={},\
         scan_p50_ms={},scan_p95_ms={},scan_p99_ms={},\
         exact_p50_ms={},exact_p95_ms={},exact_p99_ms={},\
         manifest_keys={},manifest_key_bytes={},manifest_value_bytes={},\
         segment_keys={},segment_key_bytes={},segment_value_bytes={},\
         locator_keys={},locator_key_bytes={},locator_value_bytes={},backend_bytes={backend_bytes}",
        writes.map_or_else(|| changes / commit_width, |writes| writes.commits),
        seed_elapsed.as_millis(),
        changes as f64 / seed_elapsed.as_secs_f64(),
        writes.map_or(0, |writes| writes.staged_puts),
        writes.map_or(0, |writes| writes.written_bytes),
        millis(percentile(&timings, 50)),
        millis(percentile(&timings, 95)),
        millis(percentile(&timings, 99)),
        millis(percentile(&exact_timings, 50)),
        millis(percentile(&exact_timings, 95)),
        millis(percentile(&exact_timings, 99)),
        manifest.rows,
        manifest.key_bytes,
        manifest.value_bytes,
        segments.rows,
        segments.key_bytes,
        segments.value_bytes,
        locators.rows,
        locators.key_bytes,
        locators.value_bytes,
    );
}

fn find_layout(layout: &[BenchLayoutAccounting], name: &str) -> BenchLayoutAccounting {
    layout
        .iter()
        .find(|space| space.space == name)
        .copied()
        .unwrap_or(BenchLayoutAccounting {
            space_id: 0,
            space: "missing",
            rows: 0,
            key_bytes: 0,
            value_bytes: 0,
        })
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_nonnegative_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usizes(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|value| value.trim().parse().ok())
                .filter(|value| *value > 0)
                .collect()
        })
        .filter(|values: &Vec<_>| !values.is_empty())
        .unwrap_or_else(|| default.to_vec())
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name).map_or(default, |value| {
        matches!(value.as_str(), "1" | "true" | "yes")
    })
}

fn selected(variable: &str, candidate: &str) -> bool {
    std::env::var(variable).map_or(true, |selection| {
        selection
            .split(',')
            .map(str::trim)
            .any(|value| value == candidate)
    })
}

fn directory_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path).map_or(0, |entries| {
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
    })
}
