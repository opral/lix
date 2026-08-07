use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use lix::integration::{Engine, SessionContext};
use lix::sql_dml_bench::{SqlDmlBenchRow, SqlDmlBenchStatement, execute_sql_dml_batch_for_bench};
use lix::storage::Storage;
use lix::{ExecuteBatchStatement, Value};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

use super::model::{ForkTree, Mutation, RelationalValue};
use super::{
    Backend, CountingStorage, IoStats, Layout, Parameters, begin_allocation_profile,
    directory_bytes, end_allocation_profile, physical_delta, process_cpu_nanos,
    process_resident_bytes, take_stats,
};

const ACTIVE_BRANCH: &str = "019f0000-0000-7000-8000-000000000001";
const SCHEMA_KEY: &str = "forktree_dml_row";

const SCHEMA: &str = r#"{
  "x-lix-key":"forktree_dml_row",
  "x-lix-primary-key":["/id"],
  "type":"object",
  "properties":{
    "id":{"type":"string"},
    "value":{"type":"string","default":"default-value"},
    "counter":{"type":"integer","default":0},
    "nullable":{"type":["string","null"],"default":null},
    "payload":{"type":"string","default":""}
  },
  "required":["id"],
  "additionalProperties":false
}"#;

pub async fn run(parameters: Parameters) {
    match (parameters.backend, parameters.layout) {
        (Backend::RocksDb, Layout::Current) => run_current_rocks(parameters).await,
        (Backend::RocksDb, Layout::ForkTree) => run_model_rocks(parameters).await,
        (Backend::SlateDb, Layout::Current) => run_current_slate(parameters).await,
        (Backend::SlateDb, Layout::ForkTree) => run_model_slate(parameters).await,
    }
}

async fn run_current_rocks(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create current DML RocksDB directory");
    let database = RocksDB::open(directory.path()).expect("open current DML RocksDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    let session = prepare_current(storage, parameters.rows).await;
    database.flush().expect("flush current DML RocksDB setup");
    measure_current(session, parameters, &stats, directory.path(), None).await;
    database
        .flush()
        .expect("flush current DML RocksDB final state");
    println!(
        "forktree_dml_settled,backend=rocksdb,layout=current_lix,disk_bytes={}",
        directory_bytes(directory.path())
    );
}

async fn run_model_rocks(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create ForkTree DML RocksDB directory");
    let database = RocksDB::open(directory.path()).expect("open ForkTree DML RocksDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    let tree = prepare_model(storage, parameters.rows).await;
    database.flush().expect("flush ForkTree DML RocksDB setup");
    measure_model(tree, parameters, &stats, directory.path(), None).await;
    database
        .flush()
        .expect("flush ForkTree DML RocksDB final state");
    println!(
        "forktree_dml_settled,backend=rocksdb,layout=forktree,disk_bytes={}",
        directory_bytes(directory.path())
    );
}

async fn run_current_slate(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create current DML SlateDB directory");
    let counters = SlateDBIoCounters::default();
    let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
        .expect("open current DML SlateDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    let session = prepare_current(storage, parameters.rows).await;
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush current DML SlateDB setup");
    measure_current(
        session,
        parameters,
        &stats,
        directory.path(),
        Some(&counters),
    )
    .await;
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush current DML SlateDB final state");
    println!(
        "forktree_dml_settled,backend=slatedb,layout=current_lix,disk_bytes={}",
        directory_bytes(directory.path())
    );
}

async fn run_model_slate(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create ForkTree DML SlateDB directory");
    let counters = SlateDBIoCounters::default();
    let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
        .expect("open ForkTree DML SlateDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    let tree = prepare_model(storage, parameters.rows).await;
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush ForkTree DML SlateDB setup");
    measure_model(tree, parameters, &stats, directory.path(), Some(&counters)).await;
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush ForkTree DML SlateDB final state");
    println!(
        "forktree_dml_settled,backend=slatedb,layout=forktree,disk_bytes={}",
        directory_bytes(directory.path())
    );
}

async fn prepare_current<S>(
    storage: CountingStorage<S>,
    rows: usize,
) -> SessionContext<CountingStorage<S>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Engine::initialize(storage.clone())
        .await
        .expect("initialize current DML fixture");
    let engine = Engine::new(storage)
        .await
        .expect("open current DML fixture");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open current DML workspace session");
    let registered = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
            &[Value::Text(SCHEMA.to_string())],
        )
        .await
        .expect("register current DML schema");
    assert_eq!(registered.rows_affected(), 1);
    let seed = (0..rows)
        .map(|index| ExecuteBatchStatement {
            label: None,
            sql: "INSERT INTO forktree_dml_row (id, value, counter, nullable, payload) VALUES ($1, $2, $3, $4, $5)".to_string(),
            params: vec![
                Value::Text(format!("seed-{index:06}")),
                Value::Text(format!("value-{index:06}")),
                Value::Integer(index as i64),
                Value::Null,
                Value::Text(format!("payload-{index:06}")),
            ],
        })
        .collect::<Vec<_>>();
    let affected = session
        .execute_batch(&seed)
        .await
        .expect("seed current DML rows")
        .iter()
        .map(lix::ExecuteResult::rows_affected)
        .sum::<u64>();
    assert_eq!(affected, rows as u64);
    session
}

async fn prepare_model<S>(storage: CountingStorage<S>, rows: usize) -> ForkTree<CountingStorage<S>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let tree = ForkTree::new(storage);
    let initial = (0..rows)
        .map(|index| {
            let row = SqlDmlBenchRow {
                entity_pk: format!(r#"["seed-{index:06}"]"#),
                schema_key: SCHEMA_KEY.to_string(),
                branch_id: ACTIVE_BRANCH.to_string(),
                file_id: None,
                snapshot: Some(
                    serde_json::json!({
                        "id": format!("seed-{index:06}"),
                        "value": format!("value-{index:06}"),
                        "counter": index as i64,
                        "nullable": null,
                        "payload": format!("payload-{index:06}"),
                    })
                    .to_string(),
                ),
                metadata: None,
                global: false,
                untracked: false,
                deleted: false,
            };
            (
                row_key(&row),
                serde_json::to_vec(&row).expect("encode model row"),
            )
        })
        .collect::<Vec<_>>();
    tree.initialize(&initial)
        .await
        .expect("initialize ForkTree DML model");
    tree
}

async fn measure_current<S>(
    session: SessionContext<CountingStorage<S>>,
    parameters: Parameters,
    stats: &Arc<Mutex<IoStats>>,
    database_path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let _ = take_stats(stats);
    let physical_before = counters.map(SlateDBIoCounters::snapshot);
    let disk_before = directory_bytes(database_path);
    let rss_before = process_resident_bytes();
    let cpu_before = process_cpu_nanos();
    begin_allocation_profile();
    let started = Instant::now();
    let mut digest = String::new();
    for generation in 0..parameters.iterations {
        let statements = statements(generation);
        let public = statements
            .iter()
            .map(|statement| ExecuteBatchStatement {
                label: Some(statement.label.clone()),
                sql: statement.sql.clone(),
                params: statement.params.clone(),
            })
            .collect::<Vec<_>>();
        let results = session
            .execute_batch(&public)
            .await
            .expect("execute current DML batch");
        digest = digest_current(&results);
    }
    let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
    let (allocated_bytes, allocation_calls) = end_allocation_profile();
    let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
    let rss_after = process_resident_bytes();
    let io = take_stats(stats);
    let physical = physical_delta(counters, physical_before);
    let count = session
        .execute("SELECT COUNT(*) AS rows FROM forktree_dml_row", &[])
        .await
        .expect("verify current DML rows");
    println!(
        "forktree_dml_gate,backend={},layout=current_lix,rows={},iterations={},wall_us_per_tx={:.3},cpu_us_per_tx={:.3},alloc_bytes_per_tx={:.1},alloc_calls_per_tx={:.1},rss_delta_bytes={},begin_reads={},get_calls={},get_keys={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},begin_writes={},write_batches={},write_puts={},write_deletes={},write_bytes={},commits={},physical_read_objects={},physical_read_bytes={},physical_write_objects={},physical_write_bytes={},disk_before={},disk_after={},result_digest={},live_rows={}",
        parameters.backend.label(),
        parameters.rows,
        parameters.iterations,
        wall_us / parameters.iterations as f64,
        cpu_us / parameters.iterations as f64,
        allocated_bytes as f64 / parameters.iterations as f64,
        allocation_calls as f64 / parameters.iterations as f64,
        rss_after as i128 - rss_before as i128,
        io.begin_reads,
        io.get_calls,
        io.get_keys,
        io.get_value_bytes,
        io.scan_calls,
        io.scan_entries,
        io.scan_value_bytes,
        io.begin_writes,
        io.write_batches,
        io.write_puts,
        io.write_deletes,
        io.write_bytes,
        io.commits,
        physical.read_objects,
        physical.read_bytes,
        physical.write_objects,
        physical.write_bytes,
        disk_before,
        directory_bytes(database_path),
        digest,
        count.rows()[0].get::<i64>("rows").expect("count row"),
    );
}

async fn measure_model<S>(
    tree: ForkTree<CountingStorage<S>>,
    parameters: Parameters,
    stats: &Arc<Mutex<IoStats>>,
    database_path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let _ = take_stats(stats);
    let physical_before = counters.map(SlateDBIoCounters::snapshot);
    let disk_before = directory_bytes(database_path);
    let rss_before = process_resident_bytes();
    let cpu_before = process_cpu_nanos();
    begin_allocation_profile();
    let started = Instant::now();
    let mut digest = String::new();
    let mut object_writes = 0_u64;
    let mut object_bytes = 0_u64;
    let mut binder_scans = 0_u64;
    let mut binder_exact = 0_u64;
    let mut model_load_us = 0_u128;
    let mut binder_us = 0_u128;
    let mut publication_us = 0_u128;
    for generation in 0..parameters.iterations {
        let phase = Instant::now();
        let before = load_model_rows(&tree).await;
        model_load_us = model_load_us.saturating_add(phase.elapsed().as_micros());
        let phase = Instant::now();
        let result = execute_sql_dml_batch_for_bench(
            ACTIVE_BRANCH,
            &[SCHEMA.to_string()],
            before.values().cloned().collect(),
            &statements(generation),
        )
        .await
        .expect("execute ForkTree-bound DML batch");
        binder_us = binder_us.saturating_add(phase.elapsed().as_micros());
        digest = digest_model(&result.results);
        binder_scans = binder_scans.saturating_add(result.live_scans);
        binder_exact = binder_exact.saturating_add(result.exact_reads);
        let after = result
            .final_rows
            .into_iter()
            .filter(|row| !row.deleted)
            .map(|row| (row_key(&row), row))
            .collect::<BTreeMap<_, _>>();
        let mutations = diff_rows(&before, &after);
        let phase = Instant::now();
        let (_, accounting) = tree
            .apply_sorted_mutations(&mutations)
            .await
            .expect("publish ForkTree DML batch");
        publication_us = publication_us.saturating_add(phase.elapsed().as_micros());
        object_writes = object_writes.saturating_add(accounting.object_writes);
        object_bytes = object_bytes.saturating_add(accounting.object_bytes);
    }
    let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
    let (allocated_bytes, allocation_calls) = end_allocation_profile();
    let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
    let rss_after = process_resident_bytes();
    let io = take_stats(stats);
    let physical = physical_delta(counters, physical_before);
    // ForkTree owns no serving cache. A fresh handle clone exercises a new
    // owner traversal over the persisted selector and authenticated objects.
    let reopened = tree.clone();
    let live_rows = load_model_rows(&reopened).await.len();
    println!(
        "forktree_dml_gate,backend={},layout=forktree,rows={},iterations={},wall_us_per_tx={:.3},cpu_us_per_tx={:.3},alloc_bytes_per_tx={:.1},alloc_calls_per_tx={:.1},rss_delta_bytes={},begin_reads={},get_calls={},get_keys={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},begin_writes={},write_batches={},write_puts={},write_deletes={},write_bytes={},commits={},physical_read_objects={},physical_read_bytes={},physical_write_objects={},physical_write_bytes={},disk_before={},disk_after={},result_digest={},live_rows={},binder_scans={},binder_exact_reads={},object_writes={},object_bytes={},model_load_us={},binder_us={},publication_us={}",
        parameters.backend.label(),
        parameters.rows,
        parameters.iterations,
        wall_us / parameters.iterations as f64,
        cpu_us / parameters.iterations as f64,
        allocated_bytes as f64 / parameters.iterations as f64,
        allocation_calls as f64 / parameters.iterations as f64,
        rss_after as i128 - rss_before as i128,
        io.begin_reads,
        io.get_calls,
        io.get_keys,
        io.get_value_bytes,
        io.scan_calls,
        io.scan_entries,
        io.scan_value_bytes,
        io.begin_writes,
        io.write_batches,
        io.write_puts,
        io.write_deletes,
        io.write_bytes,
        io.commits,
        physical.read_objects,
        physical.read_bytes,
        physical.write_objects,
        physical.write_bytes,
        disk_before,
        directory_bytes(database_path),
        digest,
        live_rows,
        binder_scans,
        binder_exact,
        object_writes,
        object_bytes,
        model_load_us,
        binder_us,
        publication_us,
    );
}

fn statements(generation: usize) -> Vec<SqlDmlBenchStatement> {
    let prefix = format!("tx-{generation:06}");
    let specs = vec![
        (
            "insert-a",
            format!(
                "INSERT INTO {SCHEMA_KEY} (id, value, counter, nullable, payload) VALUES ('{prefix}-a', 'A', 1, NULL, 'blob-a') RETURNING id, value, counter, nullable, payload"
            ),
        ),
        (
            "insert-b-default",
            format!(
                "INSERT INTO {SCHEMA_KEY} (id) VALUES ('{prefix}-b') RETURNING id, value, counter, nullable, payload"
            ),
        ),
        (
            "insert-c-null",
            format!(
                "INSERT INTO {SCHEMA_KEY} (id, value, nullable) VALUES ('{prefix}-c', 'C', NULL) RETURNING id, nullable"
            ),
        ),
        (
            "update-seed-0",
            format!(
                "UPDATE {SCHEMA_KEY} SET value = '{prefix}-u0', counter = counter + 1 WHERE id = 'seed-000000' RETURNING id, value, counter"
            ),
        ),
        (
            "update-seed-1",
            format!(
                "UPDATE {SCHEMA_KEY} SET nullable = '{prefix}' WHERE id = 'seed-000001' RETURNING id, nullable"
            ),
        ),
        (
            "delete-seed-miss",
            format!("DELETE FROM {SCHEMA_KEY} WHERE id = 'missing-{prefix}' RETURNING id"),
        ),
        (
            "upsert-update",
            format!(
                "INSERT INTO {SCHEMA_KEY} (id, value) VALUES ('seed-000002', '{prefix}-upsert') ON CONFLICT (id) DO UPDATE SET value = excluded.value RETURNING id, value"
            ),
        ),
        (
            "upsert-insert",
            format!(
                "INSERT INTO {SCHEMA_KEY} (id, value) VALUES ('{prefix}-upsert-new', 'new') ON CONFLICT (id) DO UPDATE SET value = excluded.value RETURNING id, value"
            ),
        ),
        (
            "upsert-nothing",
            format!(
                "INSERT INTO {SCHEMA_KEY} (id, value) VALUES ('seed-000003', 'ignored') ON CONFLICT (id) DO NOTHING RETURNING id, value"
            ),
        ),
        (
            "multirow",
            format!(
                "INSERT INTO {SCHEMA_KEY} (id, value) VALUES ('{prefix}-m0', 'm0'), ('{prefix}-m1', 'm1') RETURNING id, value"
            ),
        ),
        (
            "update-null",
            format!(
                "UPDATE {SCHEMA_KEY} SET nullable = NULL WHERE id = '{prefix}-a' RETURNING id, nullable"
            ),
        ),
        (
            "update-payload",
            format!(
                "UPDATE {SCHEMA_KEY} SET payload = '{prefix}-blob' WHERE id = '{prefix}-a' RETURNING id, payload"
            ),
        ),
        (
            "delete-c",
            format!("DELETE FROM {SCHEMA_KEY} WHERE id = '{prefix}-c' RETURNING id, value"),
        ),
        (
            "insert-d",
            format!(
                "INSERT INTO {SCHEMA_KEY} (id, value, counter) VALUES ('{prefix}-d', 'D', 4) RETURNING id, counter"
            ),
        ),
        (
            "update-d",
            format!(
                "UPDATE {SCHEMA_KEY} SET counter = counter + 10 WHERE id = '{prefix}-d' RETURNING id, counter"
            ),
        ),
        (
            "upsert-d-nothing",
            format!(
                "INSERT INTO {SCHEMA_KEY} (id, value) VALUES ('{prefix}-d', 'ignored') ON CONFLICT (id) DO NOTHING RETURNING id"
            ),
        ),
        (
            "delete-b",
            format!("DELETE FROM {SCHEMA_KEY} WHERE id = '{prefix}-b' RETURNING id, value"),
        ),
        (
            "final-update",
            format!(
                "UPDATE {SCHEMA_KEY} SET value = '{prefix}-final' WHERE id = 'seed-000004' RETURNING id, value"
            ),
        ),
    ];
    specs
        .into_iter()
        .map(|(label, sql)| SqlDmlBenchStatement {
            label: label.to_string(),
            sql,
            params: Vec::new(),
        })
        .collect()
}

async fn load_model_rows<S>(
    tree: &ForkTree<CountingStorage<S>>,
) -> BTreeMap<Vec<u8>, SqlDmlBenchRow>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tree.read_all()
        .await
        .expect("read ForkTree DML rows")
        .into_iter()
        .map(|(key, value)| {
            let row = serde_json::from_slice(&value).expect("decode ForkTree DML row");
            (key, row)
        })
        .collect()
}

fn diff_rows(
    before: &BTreeMap<Vec<u8>, SqlDmlBenchRow>,
    after: &BTreeMap<Vec<u8>, SqlDmlBenchRow>,
) -> Vec<Mutation> {
    let mut keys = before
        .keys()
        .chain(after.keys())
        .cloned()
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter_map(|key| match (before.get(&key), after.get(&key)) {
            (None, Some(row)) => Some(Mutation::Insert {
                key,
                value: RelationalValue::Bytes(serde_json::to_vec(row).expect("encode insert")),
            }),
            (Some(old), Some(row)) if old != row => Some(Mutation::Update {
                key,
                value: RelationalValue::Bytes(serde_json::to_vec(row).expect("encode update")),
            }),
            (Some(_), None) => Some(Mutation::Delete { key }),
            _ => None,
        })
        .collect()
}

fn row_key(row: &SqlDmlBenchRow) -> Vec<u8> {
    format!(
        "{}\0{}\0{}\0{}\0{}\0{}",
        row.branch_id,
        row.schema_key,
        row.entity_pk,
        row.file_id.as_deref().unwrap_or(""),
        u8::from(row.global),
        u8::from(row.untracked),
    )
    .into_bytes()
}

fn digest_current(results: &[lix::ExecuteResult]) -> String {
    let mut hasher = blake3::Hasher::new();
    for (index, result) in results.iter().enumerate() {
        assert_eq!(result.statement_index(), Some(index));
        hasher.update(format!("{index}:{:?}:{:?}:", result.label(), result.columns()).as_bytes());
        for row in result.rows() {
            hasher.update(format!("{:?}", row.values()).as_bytes());
        }
        hasher.update(&result.rows_affected().to_be_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn digest_model(results: &[lix::sql_dml_bench::SqlDmlBenchStatementResult]) -> String {
    let mut hasher = blake3::Hasher::new();
    for result in results {
        hasher.update(
            format!(
                "{}:{:?}:{:?}:",
                result.index,
                Some(result.label.as_str()),
                result
                    .returning
                    .as_ref()
                    .map_or(&[][..], |returning| returning.columns.as_slice())
            )
            .as_bytes(),
        );
        if let Some(returning) = &result.returning {
            for row in &returning.rows {
                hasher.update(format!("{row:?}").as_bytes());
            }
        }
        hasher.update(&result.rows_affected.to_be_bytes());
    }
    hasher.finalize().to_hex().to_string()
}
