use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use lix::integration::{Engine, SessionContext};
use lix::sql_dml_bench::{
    SqlDmlBenchExactRowRequest, SqlDmlBenchFileFilter, SqlDmlBenchReadTarget, SqlDmlBenchRow,
    SqlDmlBenchScanRequest, SqlDmlBenchStatement, execute_sql_dml_batch_with_read_target_for_bench,
};
use lix::storage::{Memory, Storage};
use lix::{ExecuteBatchStatement, LixError, Value};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

use super::model::{ForkTree, ForkTreeReadView, Mutation, RelationalValue};
use super::{
    Backend, CountingStorage, IoStats, Layout, Parameters, allocation_profile_snapshot,
    begin_allocation_profile, directory_bytes, end_allocation_profile, physical_delta,
    process_cpu_nanos, process_resident_bytes, take_stats,
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
    if parameters.rows == 1_000 {
        run_filter_pushdown_oracle().await;
    }
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

async fn run_filter_pushdown_oracle() {
    let (storage, _) = CountingStorage::new(Memory::new());
    let tree = prepare_model(storage, 1_000).await;
    let mut target = ForkTreeDmlReadTarget::new(&tree)
        .await
        .expect("open ForkTree filter-pushdown oracle view");
    let statements = [
        (
            "pk-equality",
            "UPDATE forktree_dml_row SET counter = counter + 1 WHERE id = 'seed-000000' RETURNING id",
        ),
        (
            "pk-in",
            "UPDATE forktree_dml_row SET counter = counter + 1 WHERE id IN ('seed-000001', 'seed-000002') RETURNING id",
        ),
        (
            "mixed-or-residual",
            "UPDATE forktree_dml_row SET counter = counter + 1 WHERE id = 'seed-000003' OR value = 'absent' RETURNING id",
        ),
        (
            "null-residual",
            "UPDATE forktree_dml_row SET counter = counter + 1 WHERE nullable IS NOT NULL RETURNING id",
        ),
        (
            "noncanonical-like-residual",
            "UPDATE forktree_dml_row SET counter = counter + 1 WHERE id LIKE 'missing-%' RETURNING id",
        ),
    ]
    .into_iter()
    .map(|(label, sql)| SqlDmlBenchStatement {
        label: label.to_string(),
        sql: sql.to_string(),
        params: Vec::new(),
    })
    .collect::<Vec<_>>();
    let result = execute_sql_dml_batch_with_read_target_for_bench(
        ACTIVE_BRANCH,
        &[SCHEMA.to_string()],
        &mut target,
        &statements,
    )
    .await
    .expect("execute ForkTree filter-pushdown oracle");
    assert_eq!(
        result
            .results
            .iter()
            .map(|result| result.rows_affected)
            .collect::<Vec<_>>(),
        vec![1, 2, 1, 0, 0]
    );
    assert_eq!(target.broad_scans, 3);
    assert_eq!(target.point_reads, 7);
    println!(
        "forktree_dml_pushdown_oracle,result_digest={},live_scans={},point_reads={},broad_scans={},pk_equality=exact,pk_in=exact,mixed_or=residual,null=residual,noncanonical_like=residual",
        digest_model(&result.results),
        result.live_scans,
        target.point_reads,
        target.broad_scans,
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
    let mut node_writes = 0_u64;
    let mut node_bytes = 0_u64;
    let mut leaf_writes = 0_u64;
    let mut leaf_bytes = 0_u64;
    let mut internal_writes = 0_u64;
    let mut internal_bytes = 0_u64;
    let mut reused_objects = 0_u64;
    let mut logical_bytes = 0_u64;
    let mut binder_scans = 0_u64;
    let mut binder_exact = 0_u64;
    let mut target_point_reads = 0_u64;
    let mut target_broad_scans = 0_u64;
    let mut binder_us = 0_u128;
    let mut publication_us = 0_u128;
    let mut view_alloc_bytes = 0_u64;
    let mut view_alloc_calls = 0_u64;
    let mut binder_alloc_bytes = 0_u64;
    let mut binder_alloc_calls = 0_u64;
    let mut target_alloc_bytes = 0_u64;
    let mut target_alloc_calls = 0_u64;
    let mut mutation_alloc_bytes = 0_u64;
    let mut mutation_alloc_calls = 0_u64;
    let mut publication_alloc_bytes = 0_u64;
    let mut publication_alloc_calls = 0_u64;
    for generation in 0..parameters.iterations {
        let phase = Instant::now();
        let allocation_before = allocation_profile_snapshot();
        let mut target = ForkTreeDmlReadTarget::new(&tree)
            .await
            .expect("open coherent ForkTree DML read view");
        accumulate_allocation_delta(
            allocation_before,
            allocation_profile_snapshot(),
            &mut view_alloc_bytes,
            &mut view_alloc_calls,
        );
        let allocation_before = allocation_profile_snapshot();
        let result = execute_sql_dml_batch_with_read_target_for_bench(
            ACTIVE_BRANCH,
            &[SCHEMA.to_string()],
            &mut target,
            &statements(generation),
        )
        .await
        .expect("execute ForkTree-bound DML batch");
        accumulate_allocation_delta(
            allocation_before,
            allocation_profile_snapshot(),
            &mut binder_alloc_bytes,
            &mut binder_alloc_calls,
        );
        binder_us = binder_us.saturating_add(phase.elapsed().as_micros());
        target_point_reads = target_point_reads.saturating_add(target.point_reads);
        target_broad_scans = target_broad_scans.saturating_add(target.broad_scans);
        target_alloc_bytes = target_alloc_bytes.saturating_add(target.alloc_bytes);
        target_alloc_calls = target_alloc_calls.saturating_add(target.alloc_calls);
        drop(target);
        digest = digest_model(&result.results);
        binder_scans = binder_scans.saturating_add(result.live_scans);
        binder_exact = binder_exact.saturating_add(result.exact_reads);
        let allocation_before = allocation_profile_snapshot();
        let mutations = mutations_from_postimages(&tree, result.final_rows).await;
        accumulate_allocation_delta(
            allocation_before,
            allocation_profile_snapshot(),
            &mut mutation_alloc_bytes,
            &mut mutation_alloc_calls,
        );
        let phase = Instant::now();
        let allocation_before = allocation_profile_snapshot();
        let (_, accounting) = tree
            .apply_sorted_mutations(&mutations)
            .await
            .expect("publish ForkTree DML batch");
        accumulate_allocation_delta(
            allocation_before,
            allocation_profile_snapshot(),
            &mut publication_alloc_bytes,
            &mut publication_alloc_calls,
        );
        publication_us = publication_us.saturating_add(phase.elapsed().as_micros());
        object_writes = object_writes.saturating_add(accounting.object_writes);
        object_bytes = object_bytes.saturating_add(accounting.object_bytes);
        node_writes = node_writes.saturating_add(accounting.node_writes);
        node_bytes = node_bytes.saturating_add(accounting.node_bytes);
        leaf_writes = leaf_writes.saturating_add(accounting.leaf_writes);
        leaf_bytes = leaf_bytes.saturating_add(accounting.leaf_bytes);
        internal_writes = internal_writes.saturating_add(accounting.internal_writes);
        internal_bytes = internal_bytes.saturating_add(accounting.internal_bytes);
        reused_objects = reused_objects.saturating_add(accounting.reused_objects);
        logical_bytes = logical_bytes.saturating_add(accounting.logical_bytes);
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
        "forktree_dml_gate,backend={},layout=forktree_direct,rows={},iterations={},wall_us_per_tx={:.3},cpu_us_per_tx={:.3},alloc_bytes_per_tx={:.1},alloc_calls_per_tx={:.1},rss_delta_bytes={},begin_reads={},get_calls={},get_keys={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},begin_writes={},write_batches={},write_puts={},write_deletes={},write_bytes={},commits={},physical_read_objects={},physical_read_bytes={},physical_write_objects={},physical_write_bytes={},disk_before={},disk_after={},result_digest={},live_rows={},binder_scans={},binder_exact_reads={},target_point_reads={},target_broad_scans={},object_writes={},object_bytes={},node_writes={},node_bytes={},leaf_writes={},leaf_bytes={},internal_writes={},internal_bytes={},reused_objects={},logical_bytes={},view_alloc_bytes={},view_alloc_calls={},binder_alloc_bytes={},binder_alloc_calls={},target_alloc_bytes={},target_alloc_calls={},mutation_alloc_bytes={},mutation_alloc_calls={},publication_alloc_bytes={},publication_alloc_calls={},binder_us={},publication_us={}",
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
        target_point_reads,
        target_broad_scans,
        object_writes,
        object_bytes,
        node_writes,
        node_bytes,
        leaf_writes,
        leaf_bytes,
        internal_writes,
        internal_bytes,
        reused_objects,
        logical_bytes,
        view_alloc_bytes,
        view_alloc_calls,
        binder_alloc_bytes,
        binder_alloc_calls,
        target_alloc_bytes,
        target_alloc_calls,
        mutation_alloc_bytes,
        mutation_alloc_calls,
        publication_alloc_bytes,
        publication_alloc_calls,
        binder_us,
        publication_us,
    );
}

fn accumulate_allocation_delta(
    before: (u64, u64),
    after: (u64, u64),
    bytes: &mut u64,
    calls: &mut u64,
) {
    *bytes = bytes.saturating_add(after.0.saturating_sub(before.0));
    *calls = calls.saturating_add(after.1.saturating_sub(before.1));
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

struct ForkTreeDmlReadTarget<'a, S: Storage> {
    view: ForkTreeReadView<'a, CountingStorage<S>>,
    global: bool,
    untracked: bool,
    point_reads: u64,
    broad_scans: u64,
    alloc_bytes: u64,
    alloc_calls: u64,
}

impl<'a, S: Storage> ForkTreeDmlReadTarget<'a, S> {
    async fn new(tree: &'a ForkTree<CountingStorage<S>>) -> Result<Self, String>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        Ok(Self {
            view: tree.read_view("main").await?,
            global: false,
            untracked: false,
            point_reads: 0,
            broad_scans: 0,
            alloc_bytes: 0,
            alloc_calls: 0,
        })
    }
}

#[async_trait]
impl<S> SqlDmlBenchReadTarget for ForkTreeDmlReadTarget<'_, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn scan_rows(
        &mut self,
        request: &SqlDmlBenchScanRequest,
    ) -> Result<Vec<SqlDmlBenchRow>, LixError> {
        let before = allocation_profile_snapshot();
        let result = self.scan_rows_inner(request).await;
        let after = allocation_profile_snapshot();
        self.alloc_bytes = self
            .alloc_bytes
            .saturating_add(after.0.saturating_sub(before.0));
        self.alloc_calls = self
            .alloc_calls
            .saturating_add(after.1.saturating_sub(before.1));
        result
    }

    async fn load_exact_rows(
        &mut self,
        rows: &[SqlDmlBenchExactRowRequest],
        untracked: Option<bool>,
        include_tombstones: bool,
    ) -> Result<Vec<Option<SqlDmlBenchRow>>, LixError> {
        let before = allocation_profile_snapshot();
        let result = self
            .load_identity_rows(rows, untracked.unwrap_or(self.untracked))
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|row| row.filter(|row| include_tombstones || !row.deleted))
                    .collect()
            });
        let after = allocation_profile_snapshot();
        self.alloc_bytes = self
            .alloc_bytes
            .saturating_add(after.0.saturating_sub(before.0));
        self.alloc_calls = self
            .alloc_calls
            .saturating_add(after.1.saturating_sub(before.1));
        result
    }
}

impl<S> ForkTreeDmlReadTarget<'_, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn scan_rows_inner(
        &mut self,
        request: &SqlDmlBenchScanRequest,
    ) -> Result<Vec<SqlDmlBenchRow>, LixError> {
        if request.rows_none {
            return Ok(Vec::new());
        }
        if can_point_expand(request, self.untracked) {
            let mut identities = Vec::new();
            for branch_id in &request.branch_ids {
                for schema_key in &request.schema_keys {
                    for entity_pk in &request.entity_pks {
                        for file_id in point_file_ids(&request.file_ids) {
                            identities.push(SqlDmlBenchExactRowRequest {
                                schema_key: schema_key.clone(),
                                entity_pk: entity_pk.clone(),
                                branch_id: branch_id.clone(),
                                file_id,
                            });
                        }
                    }
                }
            }
            let mut rows = self
                .load_identity_rows(&identities, self.untracked)
                .await?
                .into_iter()
                .flatten()
                .filter(|row| request.include_tombstones || !row.deleted)
                .collect::<Vec<_>>();
            if let Some(limit) = request.limit {
                rows.truncate(limit);
            }
            return Ok(rows);
        }

        self.broad_scans = self.broad_scans.saturating_add(1);
        let rows = self
            .view
            .read_projected_range(b"", &[0xff], |value| {
                serde_json::from_slice::<SqlDmlBenchRow>(value)
                    .map_err(|error| format!("malformed authenticated ForkTree SQL row: {error}"))
            })
            .await
            .map_err(model_read_error)?
            .into_iter()
            .map(|(_, row)| row)
            .collect::<Vec<_>>();
        let mut rows = rows
            .into_iter()
            .filter(|row| bench_request_matches(request, row))
            .collect::<Vec<_>>();
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }
    async fn load_identity_rows(
        &mut self,
        identities: &[SqlDmlBenchExactRowRequest],
        untracked: bool,
    ) -> Result<Vec<Option<SqlDmlBenchRow>>, LixError> {
        self.point_reads = self.point_reads.saturating_add(identities.len() as u64);
        let keys = identities
            .iter()
            .map(|identity| identity_key(identity, self.global, untracked))
            .collect::<Vec<_>>();
        self.view
            .read_projected_points(&keys, |bytes| {
                serde_json::from_slice::<SqlDmlBenchRow>(bytes)
                    .map_err(|error| format!("malformed authenticated ForkTree SQL row: {error}"))
            })
            .await
            .map_err(model_read_error)
    }
}

fn can_point_expand(request: &SqlDmlBenchScanRequest, target_untracked: bool) -> bool {
    !request.schema_keys.is_empty()
        && !request.entity_pks.is_empty()
        && !request.branch_ids.is_empty()
        && request
            .untracked
            .is_none_or(|untracked| untracked == target_untracked)
        && request
            .file_ids
            .iter()
            .all(|filter| !matches!(filter, SqlDmlBenchFileFilter::Any))
}

fn point_file_ids(filters: &[SqlDmlBenchFileFilter]) -> Vec<Option<String>> {
    if filters.is_empty() {
        return vec![None];
    }
    filters
        .iter()
        .filter_map(|filter| match filter {
            SqlDmlBenchFileFilter::Any => None,
            SqlDmlBenchFileFilter::Null => Some(None),
            SqlDmlBenchFileFilter::Value(value) => Some(Some(value.clone())),
        })
        .collect()
}

fn bench_request_matches(request: &SqlDmlBenchScanRequest, row: &SqlDmlBenchRow) -> bool {
    (request.schema_keys.is_empty() || request.schema_keys.contains(&row.schema_key))
        && (request.entity_pks.is_empty() || request.entity_pks.contains(&row.entity_pk))
        && (request.branch_ids.is_empty() || request.branch_ids.contains(&row.branch_id))
        && request
            .untracked
            .is_none_or(|untracked| row.untracked == untracked)
        && (request.include_tombstones || !row.deleted)
        && (request.file_ids.is_empty()
            || request.file_ids.iter().any(|filter| match filter {
                SqlDmlBenchFileFilter::Any => true,
                SqlDmlBenchFileFilter::Null => row.file_id.is_none(),
                SqlDmlBenchFileFilter::Value(value) => row.file_id.as_ref() == Some(value),
            }))
}

async fn mutations_from_postimages<S>(
    tree: &ForkTree<CountingStorage<S>>,
    rows: Vec<SqlDmlBenchRow>,
) -> Vec<Mutation>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut mutations = Vec::with_capacity(rows.len());
    let keys = rows.iter().map(row_key).collect::<Vec<_>>();
    let prior = tree
        .read_projected_points("main", &keys, |value| Ok(value.to_vec()))
        .await
        .expect("read prior ForkTree DML rows");
    for ((row, key), prior) in rows.into_iter().zip(keys).zip(prior) {
        if row.deleted {
            if prior.is_some() {
                mutations.push(Mutation::Delete { key });
            }
            continue;
        }
        let encoded = serde_json::to_vec(&row).expect("encode postimage");
        match prior {
            None => mutations.push(Mutation::Insert {
                key,
                value: RelationalValue::Bytes(encoded),
            }),
            Some(old) if old != encoded => {
                mutations.push(Mutation::Update {
                    key,
                    value: RelationalValue::Bytes(encoded),
                });
            }
            Some(_) => {}
        }
    }
    mutations.sort_by(|left, right| left.key().cmp(right.key()));
    mutations
}

fn identity_key(row: &SqlDmlBenchExactRowRequest, global: bool, untracked: bool) -> Vec<u8> {
    format!(
        "{}\0{}\0{}\0{}\0{}\0{}",
        row.branch_id,
        row.schema_key,
        row.entity_pk,
        row.file_id.as_deref().unwrap_or(""),
        u8::from(global),
        u8::from(untracked),
    )
    .into_bytes()
}

fn model_read_error(error: String) -> LixError {
    LixError::new(
        LixError::CODE_STORAGE_ERROR,
        format!("authenticated ForkTree SQL read failed: {error}"),
    )
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
