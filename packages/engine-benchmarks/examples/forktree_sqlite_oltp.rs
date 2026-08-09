//! Benchmark-only ForkTree/public-SQL versus standalone SQLite comparator.
//!
//! The SQLite side is an external WAL/FULL control, not a Lix adapter.  The
//! harness compares only the public `ExecuteResult` shape and a canonical
//! digest; version-control semantics remain specific to Lix and are not
//! claimed equivalent here.

use std::time::Instant;

use blake3::Hasher;
use lix::{ExecuteResult, Value};

#[path = "../benches/tracked_state_crud/raw_sqlite.rs"]
mod raw_sqlite;
#[path = "../benches/tracked_state_crud/sql_session.rs"]
mod sql_session;
#[path = "../benches/tracked_state_crud/storage.rs"]
mod storage;
#[path = "../benches/tracked_state_crud/workload.rs"]
mod workload;

const READ_MANY_PK_COUNT: usize = 10;

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
        "forktree_sqlite_oltp backend={backend} rows={row_count} operation={operation:?} samples={samples} verify_only={verify_only} sqlite=external_wal_full"
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

        lix::storage_adapter::reset_storage_adapter_read_counters();

        let (lix_result, lix_elapsed) = if verify_only {
            (run_lix(&lix_fixture, operation).await, None)
        } else {
            let start = Instant::now();
            let result = run_lix(&lix_fixture, operation).await;
            (result, Some(start.elapsed()))
        };
        let (sqlite_result, sqlite_elapsed) = if verify_only {
            (run_sqlite(&mut sqlite_fixture, operation), None)
        } else {
            let start = Instant::now();
            let result = run_sqlite(&mut sqlite_fixture, operation);
            (result, Some(start.elapsed()))
        };
        assert_same_digest("operation", &lix_result, &sqlite_result);

        let read_counters = lix::storage_adapter::storage_adapter_read_counters();

        let final_lix = lix_fixture.read_all_result().await;
        let final_sqlite = sqlite_fixture.read_all_public_result();
        assert_same_digest("final", &final_lix, &final_sqlite);

        println!(
            "sample={sample} verified=true operation_digest={} final_digest={} lix_wall_us={} sqlite_wall_us={} adapter_get_many_calls={} adapter_requested_keys={} adapter_returned_values={} adapter_returned_bytes={}",
            digest(&lix_result),
            digest(&final_lix),
            lix_elapsed.map_or(0, |value| value.as_micros()),
            sqlite_elapsed.map_or(0, |value| value.as_micros()),
            read_counters.get_many_calls,
            read_counters.requested_keys,
            read_counters.returned_values,
            read_counters.returned_bytes,
        );
    }
}

async fn run_lix(fixture: &sql_session::SqlFixture, operation: Operation) -> ExecuteResult {
    match operation {
        Operation::Insert => {
            fixture.insert_all().await;
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
