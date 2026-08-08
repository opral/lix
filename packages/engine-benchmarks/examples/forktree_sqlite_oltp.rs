//! Benchmark-only ForkTree/public-SQL versus standalone SQLite comparator.
//!
//! The SQLite side is an external WAL/FULL control, not a Lix adapter.  The
//! harness compares only the public `ExecuteResult` shape and a canonical
//! digest; version-control semantics remain specific to Lix and are not
//! claimed equivalent here.

use std::time::{Duration, Instant};

use blake3::Hasher;
use lix::{ExecuteResult, Value};

use crate::storage::BackendIoSnapshot;

#[path = "../benches/tracked_state_crud/raw_sqlite.rs"]
mod raw_sqlite;
#[path = "../benches/tracked_state_crud/sql_session.rs"]
mod sql_session;
#[path = "../benches/tracked_state_crud/storage.rs"]
mod storage;
#[path = "../benches/tracked_state_crud/workload.rs"]
mod workload;

const READ_MANY_PK_COUNT: usize = 10;

#[derive(Clone, Copy, Debug, Default)]
struct ResourceSnapshot {
    user_cpu_us: Option<u64>,
    system_cpu_us: Option<u64>,
    peak_rss_bytes: Option<u64>,
    read_bytes: Option<u64>,
    write_bytes: Option<u64>,
}

impl ResourceSnapshot {
    fn capture() -> Self {
        let (user_cpu_us, system_cpu_us, peak_rss_bytes) = cpu_and_rss();
        let (read_bytes, write_bytes) = process_io_bytes();
        Self {
            user_cpu_us,
            system_cpu_us,
            peak_rss_bytes,
            read_bytes,
            write_bytes,
        }
    }

    fn delta(self, earlier: Self) -> Self {
        Self {
            user_cpu_us: subtract(self.user_cpu_us, earlier.user_cpu_us),
            system_cpu_us: subtract(self.system_cpu_us, earlier.system_cpu_us),
            // RSS is a process high-water mark, so retain the post-operation
            // value instead of pretending it is an operation-local delta.
            peak_rss_bytes: self.peak_rss_bytes,
            read_bytes: subtract(self.read_bytes, earlier.read_bytes),
            write_bytes: subtract(self.write_bytes, earlier.write_bytes),
        }
    }
}

fn subtract(current: Option<u64>, earlier: Option<u64>) -> Option<u64> {
    current
        .zip(earlier)
        .map(|(current, earlier)| current.saturating_sub(earlier))
}

#[cfg(unix)]
fn cpu_and_rss() -> (Option<u64>, Option<u64>, Option<u64>) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return (None, None, None);
    }
    let usage = unsafe { usage.assume_init() };
    let peak_rss = u64::try_from(usage.ru_maxrss).ok().map(|value| {
        #[cfg(target_os = "linux")]
        {
            value.saturating_mul(1024)
        }
        #[cfg(not(target_os = "linux"))]
        {
            value
        }
    });
    (
        Some(timeval_micros(usage.ru_utime)),
        Some(timeval_micros(usage.ru_stime)),
        peak_rss,
    )
}

#[cfg(not(unix))]
fn cpu_and_rss() -> (Option<u64>, Option<u64>, Option<u64>) {
    (None, None, None)
}

#[cfg(unix)]
fn timeval_micros(value: libc::timeval) -> u64 {
    u64::try_from(value.tv_sec)
        .unwrap_or(0)
        .saturating_mul(1_000_000)
        .saturating_add(u64::try_from(value.tv_usec).unwrap_or(0))
}

#[cfg(target_os = "linux")]
fn process_io_bytes() -> (Option<u64>, Option<u64>) {
    let Ok(contents) = std::fs::read_to_string("/proc/self/io") else {
        return (None, None);
    };
    let mut read_bytes = None;
    let mut write_bytes = None;
    for line in contents.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim().parse::<u64>().ok();
        match name {
            "read_bytes" => read_bytes = value,
            "write_bytes" => write_bytes = value,
            _ => {}
        }
    }
    (read_bytes, write_bytes)
}

#[cfg(not(target_os = "linux"))]
fn process_io_bytes() -> (Option<u64>, Option<u64>) {
    (None, None)
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Insert,
    Point,
    Range,
    UpdateOne,
    UpdateAll,
    DeleteOne,
    DeleteAll,
}

impl Operation {
    fn parse(value: &str) -> Self {
        match value {
            "insert" => Self::Insert,
            "point" => Self::Point,
            "range" => Self::Range,
            "update_one" => Self::UpdateOne,
            "update_all" => Self::UpdateAll,
            "delete_one" => Self::DeleteOne,
            "delete_all" => Self::DeleteAll,
            other => panic!(
                "unknown operation {other}; expected insert, point, range, update_one, update_all, delete_one, or delete_all"
            ),
        }
    }

    const fn needs_seed(self) -> bool {
        !matches!(self, Self::Insert)
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let backend = args.next().unwrap_or_else(|| usage("missing backend"));
    let rows = args
        .next()
        .unwrap_or_else(|| usage("missing row count"))
        .parse::<usize>()
        .unwrap_or_else(|error| usage(&format!("invalid row count: {error}")));
    let operation = Operation::parse(&args.next().unwrap_or_else(|| usage("missing operation")));
    let samples = args
        .next()
        .unwrap_or_else(|| usage("missing sample count"))
        .parse::<usize>()
        .unwrap_or_else(|error| usage(&format!("invalid sample count: {error}")));
    let verify_only = matches!(args.next().as_deref(), Some("--verify-only"));
    assert!(rows > 0, "row count must be positive");
    assert!(samples > 0, "sample count must be positive");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build comparator runtime");
    runtime.block_on(run(backend, rows, operation, samples, verify_only));
}

fn usage(message: &str) -> ! {
    eprintln!(
        "{message}\nusage: forktree_sqlite_oltp <rocksdb|slatedb> <rows> <insert|point|range|update_one|update_all|delete_one|delete_all> <samples> [--verify-only]"
    );
    std::process::exit(2);
}

async fn run(
    backend: String,
    row_count: usize,
    operation: Operation,
    samples: usize,
    verify_only: bool,
) {
    let profile = match backend.as_str() {
        "rocksdb" => storage::StorageProfile::RocksDB,
        #[cfg(feature = "slatedb")]
        "slatedb" => storage::StorageProfile::SlateDB,
        other => usage(&format!(
            "unknown backend {other}; expected rocksdb or slatedb"
        )),
    };
    let rows = workload::fixture_rows(row_count);

    println!(
        "forktree_sqlite_oltp backend={backend} rows={row_count} operation={operation:?} samples={samples} verify_only={verify_only} lix_durability={} sqlite_durability={}",
        profile.durability_mode(),
        raw_sqlite::DURABILITY_MODE,
    );

    for sample in 0..samples {
        let lix_fixture = if operation.needs_seed() {
            sql_session::seeded_fixture_with_read_many_pk_count(profile, &rows, READ_MANY_PK_COUNT)
                .await
        } else {
            sql_session::empty_fixture_with_read_many_pk_count(profile, &rows, READ_MANY_PK_COUNT)
                .await
        };
        let mut sqlite_fixture = if operation.needs_seed() {
            raw_sqlite::seeded_fixture_with_read_many_pk_count(&rows, READ_MANY_PK_COUNT)
        } else {
            raw_sqlite::empty_fixture_with_read_many_pk_count(&rows, READ_MANY_PK_COUNT)
        };

        let baseline_lix = lix_fixture.read_all_result().await;
        let baseline_sqlite = sqlite_fixture.read_all_public_result();
        assert_same_digest("baseline", &baseline_lix, &baseline_sqlite);

        let lix_resource_before = (!verify_only).then(ResourceSnapshot::capture);
        let lix_backend_before = if verify_only {
            None
        } else {
            lix_fixture.backend_io_snapshot()
        };
        let lix_physical_before = if verify_only {
            None
        } else {
            Some(lix::storage_bench::take_crud_physical_write_accounting())
        };
        let lix_started = Instant::now();
        let lix_result = if verify_only {
            (run_lix(&lix_fixture, operation).await, None)
        } else {
            (
                run_lix(&lix_fixture, operation).await,
                Some(lix_started.elapsed()),
            )
        };
        let lix_resource_after = ResourceSnapshot::capture();
        let lix_backend_after = lix_fixture.backend_io_snapshot();
        let lix_physical_after = if verify_only {
            None
        } else {
            Some(lix::storage_bench::take_crud_physical_write_accounting())
        };
        let (lix_result, lix_elapsed) = lix_result;

        let sqlite_resource_before = (!verify_only).then(ResourceSnapshot::capture);
        let sqlite_started = Instant::now();
        let sqlite_result = if verify_only {
            (run_sqlite(&mut sqlite_fixture, operation), None)
        } else {
            (
                run_sqlite(&mut sqlite_fixture, operation),
                Some(sqlite_started.elapsed()),
            )
        };
        let sqlite_resource_after = ResourceSnapshot::capture();
        let (sqlite_result, sqlite_elapsed) = sqlite_result;
        assert_same_digest("operation", &lix_result, &sqlite_result);

        let final_lix = lix_fixture.read_all_result().await;
        let final_sqlite = sqlite_fixture.read_all_public_result();
        assert_same_digest("final", &final_lix, &final_sqlite);

        lix_fixture.flush_settle().await;
        sqlite_fixture.flush_settle();
        let lix_disk_bytes = lix_fixture.settled_disk_bytes();
        let sqlite_disk_bytes = sqlite_fixture.settled_disk_bytes();
        let (reopened_lix, lix_reopen_backend_total) =
            lix_fixture.cold_reopen_read_all_result().await;
        let reopened_sqlite = sqlite_fixture.cold_reopen_public_result();
        assert_same_digest("cold_reopen", &final_lix, &reopened_lix);
        assert_same_digest("cold_reopen_sqlite", &final_sqlite, &reopened_sqlite);

        let lix_resources = lix_resource_before.map(|before| lix_resource_after.delta(before));
        let sqlite_resources =
            sqlite_resource_before.map(|before| sqlite_resource_after.delta(before));
        let lix_backend = lix_backend_before
            .zip(lix_backend_after)
            .map(|(before, after)| after.saturating_sub(before));
        let lix_reopen_backend = lix_reopen_backend_total
            .zip(lix_backend_after)
            .map(|(after, before)| after.saturating_sub(before));
        let lix_physical = lix_physical_before
            .zip(lix_physical_after)
            .map(|(before, after)| physical_delta(after, before));
        let logical_bytes = logical_final_bytes(&rows, operation);

        println!(
            "sample={sample} verified=true operation_digest={} final_digest={} cold_reopen=true lix_wall_us={:?} sqlite_wall_us={:?} logical_final_bytes={} lix_disk_bytes={} sqlite_disk_bytes={} lix_disk_footprint_ratio={} sqlite_disk_footprint_ratio={} lix_write_amplification={} sqlite_write_amplification={} {} {} {} {} {}",
            digest(&lix_result),
            digest(&final_lix),
            lix_elapsed.map(|value| value.as_micros()),
            sqlite_elapsed.map(|value| value.as_micros()),
            logical_bytes,
            lix_disk_bytes,
            sqlite_disk_bytes,
            disk_footprint_ratio(lix_disk_bytes, logical_bytes),
            disk_footprint_ratio(sqlite_disk_bytes, logical_bytes),
            write_amplification(
                lix_physical
                    .as_ref()
                    .map(|metrics| metrics.written_bytes)
                    .or_else(|| lix_resources.and_then(|metrics| metrics.write_bytes)),
                logical_bytes,
            ),
            write_amplification(
                sqlite_resources.and_then(|metrics| metrics.write_bytes),
                logical_bytes,
            ),
            format_resource_metrics("lix", lix_resources),
            format_resource_metrics("sqlite", sqlite_resources),
            format_backend_metrics("lix", lix_backend),
            format_backend_metrics("lix_reopen", lix_reopen_backend),
            format_physical_metrics(lix_physical),
        );
    }
}

fn format_resource_metrics(prefix: &str, metrics: Option<ResourceSnapshot>) -> String {
    let Some(metrics) = metrics else {
        return format!(
            "{prefix}_user_cpu_us=unavailable {prefix}_system_cpu_us=unavailable {prefix}_peak_rss_bytes=unavailable {prefix}_read_bytes=unavailable {prefix}_write_bytes=unavailable {prefix}_allocations=unavailable_noninvasive"
        );
    };
    format!(
        "{prefix}_user_cpu_us={:?} {prefix}_system_cpu_us={:?} {prefix}_peak_rss_bytes={:?} {prefix}_read_bytes={:?} {prefix}_write_bytes={:?} {prefix}_allocations=unavailable_noninvasive",
        metrics.user_cpu_us,
        metrics.system_cpu_us,
        metrics.peak_rss_bytes,
        metrics.read_bytes,
        metrics.write_bytes,
    )
}

fn format_backend_metrics(prefix: &str, metrics: Option<BackendIoSnapshot>) -> String {
    let Some(metrics) = metrics else {
        return format!(
            "{prefix}_backend_read_objects=unavailable {prefix}_backend_read_bytes=unavailable {prefix}_backend_write_objects=unavailable {prefix}_backend_write_bytes=unavailable"
        );
    };
    format!(
        "{prefix}_backend_read_objects={} {prefix}_backend_read_bytes={} {prefix}_backend_write_objects={} {prefix}_backend_write_bytes={}",
        metrics.read_objects, metrics.read_bytes, metrics.write_objects, metrics.write_bytes,
    )
}

fn format_physical_metrics(
    metrics: Option<lix::storage_bench::CrudPhysicalWriteAccounting>,
) -> String {
    let Some(metrics) = metrics else {
        return "lix_physical_puts=unavailable lix_physical_deletes=unavailable lix_physical_written_bytes=unavailable".to_string();
    };
    format!(
        "lix_physical_puts={} lix_physical_deletes={} lix_physical_written_bytes={}",
        metrics.puts, metrics.deletes, metrics.written_bytes,
    )
}

fn physical_delta(
    after: lix::storage_bench::CrudPhysicalWriteAccounting,
    before: lix::storage_bench::CrudPhysicalWriteAccounting,
) -> lix::storage_bench::CrudPhysicalWriteAccounting {
    lix::storage_bench::CrudPhysicalWriteAccounting {
        puts: after.puts.saturating_sub(before.puts),
        deletes: after.deletes.saturating_sub(before.deletes),
        written_bytes: after.written_bytes.saturating_sub(before.written_bytes),
    }
}

fn logical_final_bytes(rows: &[workload::WorkloadRow], operation: Operation) -> u64 {
    let initial = rows.iter().map(row_bytes).sum::<u64>();
    match operation {
        Operation::Insert | Operation::Point | Operation::Range => initial,
        Operation::UpdateAll => rows
            .iter()
            .map(|row| row.path.len() as u64 + row.updated_value_json.len() as u64)
            .sum(),
        Operation::UpdateOne => {
            let row = &rows[rows.len() / 2];
            initial
                .saturating_sub(row.value_json.len() as u64)
                .saturating_add(row.updated_value_json.len() as u64)
        }
        Operation::DeleteAll => 0,
        Operation::DeleteOne => initial.saturating_sub(row_bytes(&rows[rows.len() / 2])),
    }
}

fn row_bytes(row: &workload::WorkloadRow) -> u64 {
    row.path.len() as u64 + row.value_json.len() as u64
}

fn disk_footprint_ratio(disk_bytes: u64, logical_bytes: u64) -> String {
    if logical_bytes == 0 {
        "unavailable_zero_logical_bytes".to_string()
    } else {
        format!("{:.3}", disk_bytes as f64 / logical_bytes as f64)
    }
}

fn write_amplification(write_bytes: Option<u64>, logical_bytes: u64) -> String {
    let Some(write_bytes) = write_bytes else {
        return "unavailable".to_string();
    };
    if logical_bytes == 0 {
        "unavailable_zero_logical_bytes".to_string()
    } else {
        format!("{:.3}", write_bytes as f64 / logical_bytes as f64)
    }
}

async fn run_lix(fixture: &sql_session::SqlFixture, operation: Operation) -> ExecuteResult {
    match operation {
        Operation::Insert => {
            fixture.insert_json_pointer_all().await;
            fixture.read_all_result().await
        }
        Operation::Point => fixture.read_one_by_pk_result().await,
        // `range` is the existing ordered full-range public scan; the
        // comparator labels it explicitly rather than implying a new API.
        Operation::Range => fixture.read_all_result().await,
        Operation::UpdateOne => {
            fixture.update_one_by_pk().await;
            fixture.read_all_result().await
        }
        Operation::UpdateAll => {
            fixture.update_all().await;
            fixture.read_all_result().await
        }
        Operation::DeleteOne => {
            fixture.delete_one_by_pk().await;
            fixture.read_all_result().await
        }
        Operation::DeleteAll => {
            fixture.delete_all().await;
            fixture.read_all_result().await
        }
    }
}

fn run_sqlite(fixture: &mut raw_sqlite::RawSqliteFixture, operation: Operation) -> ExecuteResult {
    match operation {
        Operation::Insert => {
            fixture.insert_all();
            fixture.read_all_public_result()
        }
        Operation::Point => fixture.read_one_by_pk_public_result(),
        Operation::Range => fixture.read_all_public_result(),
        Operation::UpdateOne => {
            fixture.update_one_by_pk();
            fixture.read_all_public_result()
        }
        Operation::UpdateAll => {
            fixture.update_all();
            fixture.read_all_public_result()
        }
        Operation::DeleteOne => {
            fixture.delete_one_by_pk();
            fixture.read_all_public_result()
        }
        Operation::DeleteAll => {
            fixture.delete_all();
            fixture.read_all_public_result()
        }
    }
}

fn assert_same_digest(label: &str, left: &ExecuteResult, right: &ExecuteResult) {
    assert_eq!(
        digest(left),
        digest(right),
        "{label} public result digest mismatch"
    );
}

fn digest(result: &ExecuteResult) -> String {
    let mut hasher = Hasher::new();
    feed_bytes(&mut hasher, b"forktree-sqlite-public-result-v1");
    feed_bytes(&mut hasher, &(result.columns().len() as u64).to_le_bytes());
    for column in result.columns() {
        feed_bytes(&mut hasher, column.as_bytes());
    }
    feed_bytes(&mut hasher, &(result.rows().len() as u64).to_le_bytes());
    for row in result.rows() {
        feed_bytes(&mut hasher, &(row.values().len() as u64).to_le_bytes());
        for value in row.values() {
            let encoded = serde_json::to_vec(value).expect("Value is serializable");
            feed_bytes(&mut hasher, &encoded);
        }
    }
    hasher.finalize().to_hex().to_string()
}

fn feed_bytes(hasher: &mut Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

#[allow(dead_code)]
fn _value_type_is_public(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Boolean(_) => "boolean",
        Value::Integer(_) => "integer",
        Value::Real(_) => "real",
        Value::Text(_) => "text",
        Value::Json(_) => "json",
        Value::Blob(_) => "blob",
    }
}
