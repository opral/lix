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
#[expect(dead_code)]
mod raw_sqlite;
#[path = "../benches/tracked_state_crud/sql_session.rs"]
#[expect(dead_code)]
mod sql_session;
#[path = "../benches/tracked_state_crud/storage.rs"]
#[expect(dead_code)]
mod storage;
#[path = "../benches/tracked_state_crud/workload.rs"]
#[expect(dead_code)]
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
    if std::env::var_os("LIX_TRACKED_STATE_CRUD_PHASES").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter("lix_perf=trace")
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .with_target(false)
            .with_ansi(false)
            .try_init()
            .expect("install benchmark phase tracing subscriber");
    }
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
        lix::storage_bench::begin_forktree_hot_pack_accounting();

        let (lix_result, lix_elapsed) = if verify_only {
            (run_lix(&lix_fixture, operation).await, None)
        } else if matches!(operation, Operation::UpdateOne)
            && std::env::var_os("LIX_TRACKED_STATE_CRUD_PHASES").is_some()
        {
            let (result, profile) = run_lix_update_profiled(&lix_fixture).await;
            print_lix_update_profile(sample, &profile);
            (result, Some(profile.total_elapsed))
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
        let hot_pack_counters = lix::storage_bench::take_forktree_hot_pack_accounting();

        let final_lix = lix_fixture.read_all_result().await;
        let final_sqlite = sqlite_fixture.read_all_public_result();
        assert_same_digest("final", &final_lix, &final_sqlite);

        println!(
            "sample={sample} verified=true operation_digest={} final_digest={} lix_wall_us={} sqlite_wall_us={} adapter_get_many_calls={} adapter_requested_keys={} adapter_returned_values={} adapter_returned_bytes={} hot_pack_index_builds={} hot_pack_index_hits={} hot_pack_closure_proofs={}",
            digest(&lix_result),
            digest(&final_lix),
            lix_elapsed.map_or(0, |value| value.as_micros()),
            sqlite_elapsed.map_or(0, |value| value.as_micros()),
            read_counters.get_many_calls,
            read_counters.requested_keys,
            read_counters.returned_values,
            read_counters.returned_bytes,
            hot_pack_counters.index_builds,
            hot_pack_counters.index_hits,
            hot_pack_counters.closure_proofs,
        );
    }
}

struct LixUpdateProfile {
    total_elapsed: std::time::Duration,
    sql_plan_execute: std::time::Duration,
    readback: std::time::Duration,
    ownership: lix::storage_bench::CrudOwnershipAccounting,
    physical_writes: lix::storage_bench::CrudPhysicalWriteAccounting,
    update_certificate: lix::storage_bench::CrudCertificateAccounting,
    commit_validation: lix::storage_bench::CrudCommitValidationAccounting,
}

async fn run_lix_update_profiled(
    fixture: &sql_session::SqlFixture,
) -> (ExecuteResult, LixUpdateProfile) {
    // These counters are feature-gated benchmark instrumentation. Resetting
    // them immediately before the measured operation keeps fixture seeding and
    // the baseline read outside the sample, matching the normal timer above.
    lix::storage_bench::begin_crud_ownership_accounting();
    let _ = lix::storage_bench::take_crud_physical_write_accounting();
    let _ = lix::storage_bench::take_certified_entity_update_value_batch_accounting();
    lix::storage_bench::begin_crud_commit_validation_accounting();

    let total_start = Instant::now();
    let sql_start = Instant::now();
    let affected = fixture.update_one_by_pk().await;
    let sql_plan_execute = sql_start.elapsed();
    assert_eq!(affected, 1);

    let read_start = Instant::now();
    let result = fixture.read_all_result().await;
    let readback = read_start.elapsed();
    let total_elapsed = total_start.elapsed();

    let ownership = lix::storage_bench::take_crud_ownership_accounting();
    let physical_writes = lix::storage_bench::take_crud_physical_write_accounting();
    let update_certificate =
        lix::storage_bench::take_certified_entity_update_value_batch_accounting();
    let commit_validation = lix::storage_bench::take_crud_commit_validation_accounting();
    (
        result,
        LixUpdateProfile {
            total_elapsed,
            sql_plan_execute,
            readback,
            ownership,
            physical_writes,
            update_certificate,
            commit_validation,
        },
    )
}

fn print_lix_update_profile(sample: usize, profile: &LixUpdateProfile) {
    println!(
        "phase_sample={sample} sql_plan_execute_us={} readback_us={} total_us={} \
         physical_puts={} physical_deletes={} physical_written_bytes={} \
         update_attempts={} update_hits={} certified_rows={} ownership_rows={} \
         ownership_key_bytes={} ownership_value_bytes={} ownership_vec_entries={} \
         ownership_string_entries={} ownership_map_entries={} \
         commit_validation_attempts={} commit_validation_successes={} \
         commit_validation_memo_hits={} commit_validation_member_bindings={}",
        profile.sql_plan_execute.as_micros(),
        profile.readback.as_micros(),
        profile.total_elapsed.as_micros(),
        profile.physical_writes.puts,
        profile.physical_writes.deletes,
        profile.physical_writes.written_bytes,
        profile.update_certificate.attempts,
        profile.update_certificate.hits,
        profile.update_certificate.certified_rows,
        profile
            .ownership
            .stages
            .iter()
            .map(|metric| metric.rows)
            .sum::<u64>(),
        profile
            .ownership
            .stages
            .iter()
            .map(|metric| metric.key_bytes)
            .sum::<u64>(),
        profile
            .ownership
            .stages
            .iter()
            .map(|metric| metric.value_bytes)
            .sum::<u64>(),
        profile
            .ownership
            .stages
            .iter()
            .map(|metric| metric.vec_entries)
            .sum::<u64>(),
        profile
            .ownership
            .stages
            .iter()
            .map(|metric| metric.string_entries)
            .sum::<u64>(),
        profile
            .ownership
            .stages
            .iter()
            .map(|metric| metric.map_entries)
            .sum::<u64>(),
        profile.commit_validation.attempts,
        profile.commit_validation.successes,
        profile.commit_validation.memo_hits,
        profile.commit_validation.member_bindings,
    );
    for (stage, metric) in profile.ownership.stages.iter().enumerate() {
        if *metric == lix::storage_bench::CrudOwnershipMetric::default() {
            continue;
        }
        println!(
            "phase_stage_sample={sample} stage={stage} name={} rows={} key_bytes={} value_bytes={} vec_entries={} string_entries={} map_entries={}",
            ownership_stage_name(stage),
            metric.rows,
            metric.key_bytes,
            metric.value_bytes,
            metric.vec_entries,
            metric.string_entries,
            metric.map_entries,
        );
    }
    for (stage, transfer) in profile.ownership.transfers.iter().enumerate() {
        if *transfer == lix::storage_bench::CrudOwnershipTransferMetric::default() {
            continue;
        }
        println!(
            "phase_transfer_sample={sample} stage={stage} name={} created_bytes={} cloned_bytes={} retained_bytes={} dropped_bytes={}",
            ownership_stage_name(stage),
            transfer.created_bytes,
            transfer.cloned_bytes,
            transfer.retained_bytes,
            transfer.dropped_bytes,
        );
    }
}

fn ownership_stage_name(stage: usize) -> &'static str {
    match stage {
        lix::storage_bench::CRUD_OWNERSHIP_SQL_BOUND => "sql_bound",
        lix::storage_bench::CRUD_OWNERSHIP_RAW_BATCH => "raw_batch",
        lix::storage_bench::CRUD_OWNERSHIP_RAW_TRANSFER => "raw_transfer",
        lix::storage_bench::CRUD_OWNERSHIP_PREPARED_BATCH => "prepared_batch",
        lix::storage_bench::CRUD_OWNERSHIP_PREPARED_CLONE => "prepared_clone",
        lix::storage_bench::CRUD_OWNERSHIP_REPLACEMENT_INPUT => "replacement_input",
        lix::storage_bench::CRUD_OWNERSHIP_REPLACEMENT_PART => "replacement_part",
        lix::storage_bench::CRUD_OWNERSHIP_AUTHORITY => "authority",
        lix::storage_bench::CRUD_OWNERSHIP_ROOT_PUBLICATION => "root_publication",
        lix::storage_bench::CRUD_OWNERSHIP_WRITE_SET => "write_set",
        lix::storage_bench::CRUD_OWNERSHIP_ADAPTER => "storage_adapter",
        lix::storage_bench::CRUD_OWNERSHIP_MUTATION_JOURNAL => "mutation_journal",
        lix::storage_bench::CRUD_OWNERSHIP_IDENTITY_ENCODING => "identity_encoding",
        lix::storage_bench::CRUD_OWNERSHIP_NORMALIZATION => "normalization",
        lix::storage_bench::CRUD_OWNERSHIP_JOURNAL_SEAL => "journal_seal",
        _ => "unknown",
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
