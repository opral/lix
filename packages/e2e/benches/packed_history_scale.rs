use std::fmt::{self, Display, Formatter};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use lix::registered_spaces::{
    JSON_SPACE, TRACKED_STATE_CHANGE_LOCATOR_SPACE, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
    TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
};
use lix::storage::Storage;
use lix::storage_adapter::StorageAdapter;
use lix::tracked_state::bench::{
    BenchLayoutAccounting, BenchPackedHistoryOptions, BenchPackedHistoryPayload,
    BenchPackedHistoryShape, load_packed_change, packed_history_layout, scan_packed_history,
    seed_packed_history_with_options,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

const DEFAULT_CHANGES: &[usize] = &[100_000];
const DEFAULT_COMMIT_WIDTHS: &[usize] = &[1, 10, 100, 10_000];
const DEFAULT_STORAGE_BATCH_CHANGES: usize = 100_000;
const DEFAULT_WARMUPS: usize = 1;
const DEFAULT_SAMPLES: usize = 5;
const DEFAULT_EXACT_SAMPLES: usize = 1_001;
const DEFAULT_LIVE_ROWS: usize = 10_000;
const DEFAULT_LARGE_PAYLOAD_BYTES: usize = 4 * 1024;

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
    let shapes = env_shapes();
    let payloads = env_payloads();
    let live_rows = env_usize("LIX_PACKED_HISTORY_LIVE_ROWS", DEFAULT_LIVE_ROWS);
    let large_payload_bytes = env_usize(
        "LIX_PACKED_HISTORY_LARGE_PAYLOAD_BYTES",
        DEFAULT_LARGE_PAYLOAD_BYTES,
    );
    let storage_batch_changes = env_usize(
        "LIX_PACKED_HISTORY_STORAGE_BATCH_CHANGES",
        DEFAULT_STORAGE_BATCH_CHANGES,
    );
    let warmups = env_nonnegative_usize("LIX_PACKED_HISTORY_WARMUPS", DEFAULT_WARMUPS);
    let samples = env_usize("LIX_PACKED_HISTORY_SAMPLES", DEFAULT_SAMPLES).max(1);
    let exact_samples = env_usize("LIX_PACKED_HISTORY_EXACT_SAMPLES", DEFAULT_EXACT_SAMPLES).max(1);
    let flush = env_bool("LIX_PACKED_HISTORY_FLUSH", true);
    let memtable_flush = env_bool("LIX_PACKED_HISTORY_MEMTABLE_FLUSH", false);
    let settle_ms = env_nonnegative_usize("LIX_PACKED_HISTORY_SETTLE_MS", 0) as u64;
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
        assert_eq!(
            shapes.len(),
            1,
            "persistent storage requires one history shape"
        );
        assert_eq!(
            payloads.len(),
            1,
            "persistent storage requires one payload shape"
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
                for &shape in &shapes {
                    for &payload in &payloads {
                        match backend {
                            Backend::RocksDB => {
                                let dir = tempfile::tempdir()
                                    .expect("create RocksDB benchmark directory");
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
                                    shape,
                                    payload,
                                    live_rows,
                                    large_payload_bytes,
                                    storage_batch_changes,
                                    warmups,
                                    samples,
                                    exact_samples,
                                    flush,
                                    false,
                                    settle_ms,
                                    account_layout,
                                    skip_seed,
                                    None,
                                    || async {
                                        storage.flush().expect("flush benchmark RocksDB");
                                    },
                                )
                                .await;
                            }
                            Backend::SlateDB => {
                                let dir = tempfile::tempdir()
                                    .expect("create SlateDB benchmark directory");
                                let io_counters = SlateDBIoCounters::default();
                                let path = persistent_root.as_ref().map_or_else(
                                    || dir.path().join("slatedb"),
                                    |root| root.join("slatedb"),
                                );
                                let storage =
                                    SlateDB::open_with_io_counters(&path, io_counters.clone())
                                        .expect("open benchmark SlateDB");
                                run_case(
                                    backend,
                                    storage.clone(),
                                    &path,
                                    change_count,
                                    commit_width,
                                    shape,
                                    payload,
                                    live_rows,
                                    large_payload_bytes,
                                    storage_batch_changes,
                                    warmups,
                                    samples,
                                    exact_samples,
                                    flush,
                                    memtable_flush,
                                    settle_ms,
                                    account_layout,
                                    skip_seed,
                                    Some(&io_counters),
                                    || async {
                                        if memtable_flush {
                                            storage
                                                .flush_memtable_for_diagnostics()
                                                .await
                                                .expect("flush benchmark SlateDB memtable");
                                        } else {
                                            storage
                                                .flush()
                                                .await
                                                .expect("flush benchmark SlateDB WAL");
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
    }
}

#[expect(clippy::too_many_arguments)]
async fn run_case<S, Flush, FlushFuture>(
    backend: Backend,
    storage: S,
    storage_path: &Path,
    changes: usize,
    commit_width: usize,
    shape: BenchPackedHistoryShape,
    payload: BenchPackedHistoryPayload,
    live_rows: usize,
    large_payload_bytes: usize,
    storage_batch_changes: usize,
    warmups: usize,
    samples: usize,
    exact_samples: usize,
    flush: bool,
    memtable_flush: bool,
    settle_ms: u64,
    account_layout: bool,
    skip_seed: bool,
    io_counters: Option<&SlateDBIoCounters>,
    flush_storage: Flush,
) where
    S: Storage + Clone,
    Flush: FnOnce() -> FlushFuture,
    FlushFuture: Future<Output = ()>,
{
    let adapter = StorageAdapter::new(storage);
    let id_shape =
        std::env::var("LIX_PACKED_HISTORY_ID_SHAPE").unwrap_or_else(|_| "deterministic".into());
    let seed_io_before =
        io_counters.map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
    let seed_started = Instant::now();
    let shared_large_payload = matches!(payload, BenchPackedHistoryPayload::SharedLarge)
        .then(|| format!(r#"{{"payload":"{}"}}"#, "x".repeat(large_payload_bytes)));
    let writes = if skip_seed {
        None
    } else {
        Some(
            seed_packed_history_with_options(
                &adapter,
                changes,
                commit_width,
                storage_batch_changes,
                BenchPackedHistoryOptions {
                    shape,
                    payload,
                    live_rows,
                    shared_large_payload: shared_large_payload.as_deref(),
                },
            )
            .await,
        )
    };
    let seed_elapsed = seed_started.elapsed();
    let seed_io_after =
        io_counters.map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
    if flush {
        flush_storage().await;
    }
    if settle_ms > 0 {
        tokio::time::sleep(Duration::from_millis(settle_ms)).await;
    }
    let lifecycle_io_after =
        io_counters.map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
    let backend_bytes = directory_bytes(storage_path);
    let backend_objects = directory_objects(storage_path);

    let scan_io_before = lifecycle_io_after;
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
    let scan_io_after =
        io_counters.map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
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
    let exact_io_after =
        io_counters.map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
    // Account after timing so a layout scan cannot pre-warm the measured
    // storage pages. `warmups=0,samples=1` is therefore the cold profile.
    let layout = if account_layout {
        packed_history_layout(&adapter).await
    } else {
        Vec::new()
    };
    let layout_io_after =
        io_counters.map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
    let seed_io = seed_io_after.saturating_sub(seed_io_before);
    let lifecycle_io = lifecycle_io_after.saturating_sub(seed_io_after);
    let scan_io = scan_io_after.saturating_sub(scan_io_before);
    let exact_io = exact_io_after.saturating_sub(scan_io_after);
    let layout_io = layout_io_after.saturating_sub(exact_io_after);
    let logical_written_bytes = writes.map_or(0, |writes| writes.written_bytes);
    let write_amplification = ratio(
        seed_io.write_bytes.saturating_add(lifecycle_io.write_bytes),
        logical_written_bytes,
    );
    let lifecycle_write_amplification = ratio(lifecycle_io.write_bytes, logical_written_bytes);
    let storage_amplification = ratio(backend_bytes, logical_written_bytes);
    let manifest = find_layout(&layout, TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE.name);
    let segments = find_layout(&layout, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE.name);
    let locators = find_layout(&layout, TRACKED_STATE_CHANGE_LOCATOR_SPACE.name);
    let json_payloads = find_layout(&layout, JSON_SPACE.name);

    println!(
        "packed_history_scale,backend={backend},id_shape={id_shape},history_shape={},payload_shape={},\
         live_rows={live_rows},large_payload_bytes={large_payload_bytes},\
         changes={changes},commits={},commit_width={commit_width},\
         storage_batch_changes={storage_batch_changes},flushed={flush},warmups={warmups},samples={samples},\
         memtable_flushed={memtable_flush},settle_ms={settle_ms},skip_seed={skip_seed},\
         layout_accounted={account_layout},\
         exact_samples={exact_samples},seed_ms={},ingest_changes_per_second={:.1},staged_puts={},logical_written_bytes={},\
         scan_p50_ms={},scan_p95_ms={},scan_p99_ms={},\
         exact_p50_ms={},exact_p95_ms={},exact_p99_ms={},\
         manifest_keys={},manifest_key_bytes={},manifest_value_bytes={},\
         segment_keys={},segment_key_bytes={},segment_value_bytes={},\
         locator_keys={},locator_key_bytes={},locator_value_bytes={},\
         json_payload_keys={},json_payload_key_bytes={},json_payload_value_bytes={},\
         backend_bytes={backend_bytes},backend_objects={},storage_amplification={storage_amplification:.3},\
         seed_object_reads={},seed_object_read_bytes={},seed_object_writes={},seed_object_write_bytes={},\
         lifecycle_object_reads={},lifecycle_object_read_bytes={},lifecycle_object_writes={},lifecycle_object_write_bytes={},\
         write_amplification={write_amplification:.3},lifecycle_write_amplification={lifecycle_write_amplification:.3},\
         scan_object_reads={},scan_object_read_bytes={},scan_object_writes={},scan_object_write_bytes={},\
         exact_object_reads={},exact_object_read_bytes={},exact_object_writes={},exact_object_write_bytes={},\
         layout_object_reads={},layout_object_read_bytes={},layout_object_writes={},layout_object_write_bytes={}",
        shape_name(shape),
        payload_name(payload),
        writes.map_or_else(|| changes / commit_width, |writes| writes.commits),
        seed_elapsed.as_millis(),
        changes as f64 / seed_elapsed.as_secs_f64(),
        writes.map_or(0, |writes| writes.staged_puts),
        logical_written_bytes,
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
        json_payloads.rows,
        json_payloads.key_bytes,
        json_payloads.value_bytes,
        backend_objects,
        seed_io.read_objects,
        seed_io.read_bytes,
        seed_io.write_objects,
        seed_io.write_bytes,
        lifecycle_io.read_objects,
        lifecycle_io.read_bytes,
        lifecycle_io.write_objects,
        lifecycle_io.write_bytes,
        scan_io.read_objects,
        scan_io.read_bytes,
        scan_io.write_objects,
        scan_io.write_bytes,
        exact_io.read_objects,
        exact_io.read_bytes,
        exact_io.write_objects,
        exact_io.write_bytes,
        layout_io.read_objects,
        layout_io.read_bytes,
        layout_io.write_objects,
        layout_io.write_bytes,
    );
}

/// `layout_accounting` enumerates every registered space, so a name that is not
/// present is a stale namespace in this bench, not an empty space. Failing loudly
/// is the only way the columns cannot silently rot into zeros the next time a
/// namespace version is bumped.
fn find_layout(layout: &[BenchLayoutAccounting], name: &str) -> BenchLayoutAccounting {
    layout
        .iter()
        .find(|space| space.space == name)
        .copied()
        .unwrap_or_else(|| {
            let known = layout
                .iter()
                .map(|space| space.space)
                .collect::<Vec<_>>()
                .join(", ");
            panic!("packed_history_scale asked for unregistered storage space '{name}'; registered: {known}")
        })
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        f64::NAN
    } else {
        numerator as f64 / denominator as f64
    }
}

fn shape_name(shape: BenchPackedHistoryShape) -> &'static str {
    match shape {
        BenchPackedHistoryShape::UniqueInserts => "unique_inserts",
        BenchPackedHistoryShape::RepeatedUpdates => "repeated_updates",
        BenchPackedHistoryShape::DeleteReinsert => "delete_reinsert",
    }
}

fn payload_name(payload: BenchPackedHistoryPayload) -> &'static str {
    match payload {
        BenchPackedHistoryPayload::None => "none",
        BenchPackedHistoryPayload::SmallInline => "small_inline",
        BenchPackedHistoryPayload::SharedLarge => "shared_large",
    }
}

fn env_shapes() -> Vec<BenchPackedHistoryShape> {
    env_named_values(
        "LIX_PACKED_HISTORY_SHAPES",
        &[BenchPackedHistoryShape::UniqueInserts],
        |value| match value {
            "unique" | "unique_inserts" => Some(BenchPackedHistoryShape::UniqueInserts),
            "repeated" | "repeated_updates" => Some(BenchPackedHistoryShape::RepeatedUpdates),
            "delete_reinsert" | "deletes_reinserts" => {
                Some(BenchPackedHistoryShape::DeleteReinsert)
            }
            _ => None,
        },
    )
}

fn env_payloads() -> Vec<BenchPackedHistoryPayload> {
    env_named_values(
        "LIX_PACKED_HISTORY_PAYLOADS",
        &[BenchPackedHistoryPayload::None],
        |value| match value {
            "none" => Some(BenchPackedHistoryPayload::None),
            "small" | "small_inline" => Some(BenchPackedHistoryPayload::SmallInline),
            "large" | "shared_large" => Some(BenchPackedHistoryPayload::SharedLarge),
            _ => None,
        },
    )
}

fn env_named_values<T: Copy>(
    name: &str,
    default: &[T],
    parse: impl Fn(&str) -> Option<T>,
) -> Vec<T> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|value| parse(value.trim()))
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| default.to_vec())
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

fn directory_objects(path: &Path) -> u64 {
    std::fs::read_dir(path).map_or(0, |entries| {
        entries
            .filter_map(Result::ok)
            .map(|entry| {
                if entry.metadata().is_ok_and(|metadata| metadata.is_dir()) {
                    directory_objects(&entry.path())
                } else {
                    1
                }
            })
            .sum()
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn amplification_is_unavailable_without_a_logical_baseline() {
        assert!(super::ratio(1, 0).is_nan());
        assert_eq!(super::ratio(3, 2), 1.5);
    }
}
