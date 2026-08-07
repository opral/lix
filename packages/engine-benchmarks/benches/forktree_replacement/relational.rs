use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::{PreparedDmlParameterBatch, Value};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

use super::model::{
    ApplyAccounting, ForkTree, MergeOutcome, Mutation, ObjectId, RelationalValue,
    SegmentedByteSource,
};
use super::{
    Backend, CountingStorage, IoStats, Layout, Parameters, begin_allocation_profile,
    directory_bytes, end_allocation_profile, physical_delta, process_cpu_nanos,
    process_resident_bytes, take_stats,
};

#[derive(Clone, Copy, Debug)]
enum MutationKind {
    Insert,
    Delete,
    Update,
    Mixed,
}

impl MutationKind {
    fn from_environment() -> Self {
        match std::env::var("FORKTREE_RELATIONAL_KIND")
            .unwrap_or_else(|_| "mixed".to_string())
            .as_str()
        {
            "insert" => Self::Insert,
            "delete" => Self::Delete,
            "update" => Self::Update,
            "mixed" => Self::Mixed,
            other => panic!("unknown FORKTREE_RELATIONAL_KIND '{other}'"),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Delete => "delete",
            Self::Update => "update",
            Self::Mixed => "mixed",
        }
    }
}

enum Fixture<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Current(SessionContext<CountingStorage<S>>),
    ForkTree(ForkTree<CountingStorage<S>>),
}

#[derive(Clone, Copy, Debug)]
struct Observation {
    wall_us: f64,
    cpu_us: f64,
    allocated_bytes: u64,
    allocation_calls: u64,
    post_flush_disk_bytes: u64,
}

pub(super) async fn run(parameters: Parameters) {
    let kind = MutationKind::from_environment();
    println!(
        "forktree_relational_model,current_big_o=O(U*current_publication_work),proposed_big_o=O(U*log_F(N)+copied_blocks),diff_merge_big_o=O(changed_paths+output+conflicts),selector_big_o=O(1),memory=O(U+copied_blocks),kind={}",
        kind.label()
    );
    match parameters.backend {
        Backend::RocksDb => run_rocks(parameters, kind).await,
        Backend::SlateDb => run_slate(parameters, kind).await,
    }
}

async fn run_rocks(parameters: Parameters, kind: MutationKind) {
    let mut observations = Vec::with_capacity(parameters.samples);
    for sample in 0..parameters.warmups.saturating_add(parameters.samples) {
        let directory = tempfile::tempdir().expect("create relational RocksDB directory");
        let database = RocksDB::open(directory.path()).expect("open relational RocksDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let (fixture, mutations, expected) = prepare(storage, parameters, kind).await;
        database.flush().expect("flush relational RocksDB setup");
        let observation = measure_one(
            fixture,
            &mutations,
            &expected,
            parameters,
            kind,
            sample,
            &stats,
            directory.path(),
            None,
            sample >= parameters.warmups,
        )
        .await;
        database.flush().expect("flush relational RocksDB result");
        let observation = Observation {
            post_flush_disk_bytes: directory_bytes(directory.path()),
            ..observation
        };
        if sample >= parameters.warmups {
            println!(
                "forktree_relational_post_flush,sample={},backend=rocksdb,layout={},kind={},disk_bytes={}",
                sample - parameters.warmups + 1,
                parameters.layout.label(),
                kind.label(),
                observation.post_flush_disk_bytes
            );
            observations.push(observation);
        }
    }
    print_summary(parameters, kind, &observations);
    if parameters.layout == Layout::ForkTree && relational_oracle_requested() {
        run_rocks_oracle().await;
    }
}

async fn run_slate(parameters: Parameters, kind: MutationKind) {
    let mut observations = Vec::with_capacity(parameters.samples);
    for sample in 0..parameters.warmups.saturating_add(parameters.samples) {
        let directory = tempfile::tempdir().expect("create relational SlateDB directory");
        let counters = SlateDBIoCounters::default();
        let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
            .expect("open relational SlateDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let (fixture, mutations, expected) = prepare(storage, parameters, kind).await;
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush relational SlateDB setup");
        let observation = measure_one(
            fixture,
            &mutations,
            &expected,
            parameters,
            kind,
            sample,
            &stats,
            directory.path(),
            Some(&counters),
            sample >= parameters.warmups,
        )
        .await;
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush relational SlateDB result");
        let observation = Observation {
            post_flush_disk_bytes: directory_bytes(directory.path()),
            ..observation
        };
        if sample >= parameters.warmups {
            println!(
                "forktree_relational_post_flush,sample={},backend=slatedb,layout={},kind={},disk_bytes={}",
                sample - parameters.warmups + 1,
                parameters.layout.label(),
                kind.label(),
                observation.post_flush_disk_bytes
            );
            observations.push(observation);
        }
    }
    print_summary(parameters, kind, &observations);
    if parameters.layout == Layout::ForkTree && relational_oracle_requested() {
        run_slate_oracle().await;
    }
}

async fn prepare<S>(
    storage: CountingStorage<S>,
    parameters: Parameters,
    kind: MutationKind,
) -> (
    Fixture<S>,
    Vec<Mutation>,
    BTreeMap<Vec<u8>, RelationalValue>,
)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let rows = initial_rows(parameters.rows);
    let mutations = mutation_batch(parameters.rows, parameters.updates, kind);
    let expected = apply_oracle(rows.clone(), &mutations).expect("valid relational oracle batch");
    let fixture = match parameters.layout {
        Layout::Current => {
            Engine::initialize(storage.clone())
                .await
                .expect("initialize current relational fixture");
            let engine = Engine::new(storage)
                .await
                .expect("open current relational fixture");
            let session = engine
                .open_workspace_session()
                .await
                .expect("open current relational session");
            register_current_schema(&session).await;
            let seed = PreparedDmlParameterBatch::from_rows(rows.iter().map(|(key, value)| {
                vec![
                    Value::Text(String::from_utf8(key.clone()).expect("UTF-8 row identity")),
                    lix_value(value),
                ]
            }))
            .expect("build current relational seed");
            let affected = session
                .execute_prepared_dml_batch(
                    Arc::from("INSERT INTO forktree_rel_row (id, value) VALUES ($1, $2)"),
                    seed,
                )
                .await
                .expect("seed current relational rows")
                .iter()
                .map(lix::ExecuteResult::rows_affected)
                .sum::<u64>();
            assert_eq!(affected, parameters.rows as u64);
            Fixture::Current(session)
        }
        Layout::ForkTree => {
            let tree = ForkTree::new(storage);
            let byte_rows = rows
                .iter()
                .map(|(key, value)| match value {
                    RelationalValue::Bytes(value) => (key.clone(), value.clone()),
                    RelationalValue::Null => panic!("initial relational rows are non-null"),
                })
                .collect::<Vec<_>>();
            tree.initialize(&byte_rows)
                .await
                .expect("initialize relational ForkTree");
            Fixture::ForkTree(tree)
        }
    };
    (fixture, mutations, expected)
}

#[allow(clippy::too_many_arguments)]
async fn measure_one<S>(
    fixture: Fixture<S>,
    mutations: &[Mutation],
    expected: &BTreeMap<Vec<u8>, RelationalValue>,
    parameters: Parameters,
    kind: MutationKind,
    sample: usize,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    report: bool,
) -> Observation
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let _ = take_stats(stats);
    let _ = lix::storage_bench::take_crud_physical_write_accounting();
    let physical_before = counters.map(SlateDBIoCounters::snapshot);
    let disk_before = directory_bytes(path);
    let rss_before = process_resident_bytes();
    let cpu_before = process_cpu_nanos();
    begin_allocation_profile();
    let started = Instant::now();
    let accounting = match &fixture {
        Fixture::Current(session) => {
            apply_current(session, mutations).await;
            let physical = lix::storage_bench::take_crud_physical_write_accounting();
            ApplyAccounting {
                object_writes: physical.puts,
                object_bytes: physical.written_bytes,
                logical_bytes: logical_bytes(mutations),
                ..ApplyAccounting::default()
            }
        }
        Fixture::ForkTree(tree) => {
            tree.apply_sorted_mutations(mutations)
                .await
                .expect("apply relational ForkTree batch")
                .1
        }
    };
    let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
    let (allocated_bytes, allocation_calls) = end_allocation_profile();
    let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
    let rss_after = process_resident_bytes();
    let disk_after = directory_bytes(path);
    let io = take_stats(stats);
    let physical = physical_delta(counters, physical_before);
    verify(&fixture, expected).await;
    if report {
        println!(
            "forktree_relational_gate,sample={},backend={},layout={},kind={},rows={},mutations={},wall_us={:.3},cpu_us={:.3},alloc_bytes={},alloc_calls={},rss_before_bytes={},rss_after_bytes={},begin_reads={},begin_writes={},get_calls={},get_keys={},get_values={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},write_batches={},write_puts={},write_deletes={},write_bytes={},commits={},logical_bytes={},object_writes={},object_bytes={},node_writes={},node_bytes={},leaf_writes={},leaf_bytes={},internal_writes={},internal_bytes={},reused_objects={},disk_before_bytes={},disk_after_bytes={},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={}",
            sample - parameters.warmups + 1,
            parameters.backend.label(),
            parameters.layout.label(),
            kind.label(),
            parameters.rows,
            mutations.len(),
            wall_us,
            cpu_us,
            allocated_bytes,
            allocation_calls,
            rss_before,
            rss_after,
            io.begin_reads,
            io.begin_writes,
            io.get_calls,
            io.get_keys,
            io.get_values,
            io.get_value_bytes,
            io.scan_calls,
            io.scan_entries,
            io.scan_value_bytes,
            io.write_batches,
            io.write_puts,
            io.write_deletes,
            io.write_bytes,
            io.commits,
            accounting.logical_bytes,
            accounting.object_writes,
            accounting.object_bytes,
            accounting.node_writes,
            accounting.node_bytes,
            accounting.leaf_writes,
            accounting.leaf_bytes,
            accounting.internal_writes,
            accounting.internal_bytes,
            accounting.reused_objects,
            disk_before,
            disk_after,
            physical.read_objects,
            physical.read_bytes,
            physical.write_objects,
            physical.write_bytes,
        );
    }
    Observation {
        wall_us,
        cpu_us,
        allocated_bytes,
        allocation_calls,
        post_flush_disk_bytes: 0,
    }
}

async fn register_current_schema<S>(session: &SessionContext<CountingStorage<S>>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "x-lix-key": "forktree_rel_row",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "value"],
        "properties": {
            "id": { "type": "string" },
            "value": { "type": ["string", "null"] }
        },
        "additionalProperties": false
    });
    let affected = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register relational comparison schema")
        .rows_affected();
    assert_eq!(affected, 1);
}

async fn apply_current<S>(session: &SessionContext<CountingStorage<S>>, mutations: &[Mutation])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let deletes = mutations
        .iter()
        .filter_map(|mutation| match mutation {
            Mutation::Delete { key } => Some(vec![Value::Text(text_key(key))]),
            _ => None,
        })
        .collect::<Vec<_>>();
    let updates = mutations
        .iter()
        .filter_map(|mutation| match mutation {
            Mutation::Update { key, value } => {
                Some(vec![lix_value(value), Value::Text(text_key(key))])
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let inserts = mutations
        .iter()
        .filter_map(|mutation| match mutation {
            Mutation::Insert { key, value } => {
                Some(vec![Value::Text(text_key(key)), lix_value(value)])
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin current relational transaction");
    let mut affected = 0_u64;
    // Current Lix deliberately does not expose DELETE through the prepared-DML
    // page surface. Keep every delete in this one explicit transaction and use
    // its ordinary public statement path; the SQL planning cache still owns
    // statement reuse and the comparison does not invent a benchmark adapter.
    for parameters in deletes {
        affected += transaction
            .execute("DELETE FROM forktree_rel_row WHERE id = $1", &parameters)
            .await
            .expect("execute current relational delete")
            .rows_affected();
    }
    if !updates.is_empty() {
        affected += execute_page(
            &mut transaction,
            "UPDATE forktree_rel_row SET value = $1 WHERE id = $2",
            updates,
        )
        .await;
    }
    if !inserts.is_empty() {
        affected += execute_page(
            &mut transaction,
            "INSERT INTO forktree_rel_row (id, value) VALUES ($1, $2)",
            inserts,
        )
        .await;
    }
    transaction
        .commit()
        .await
        .expect("commit current relational transaction");
    assert_eq!(affected, mutations.len() as u64);
}

async fn execute_page<S>(
    transaction: &mut lix::SessionTransaction<CountingStorage<S>>,
    sql: &'static str,
    rows: Vec<Vec<Value>>,
) -> u64
where
    S: Storage + Clone + Send + Sync + 'static,
{
    transaction
        .execute_prepared_dml_batch(
            Arc::from(sql),
            PreparedDmlParameterBatch::from_rows(rows).expect("build relational DML page"),
        )
        .await
        .expect("execute relational DML page")
        .iter()
        .map(lix::ExecuteResult::rows_affected)
        .sum()
}

async fn verify<S>(fixture: &Fixture<S>, expected: &BTreeMap<Vec<u8>, RelationalValue>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let actual = match fixture {
        Fixture::Current(session) => session
            .execute("SELECT id, value FROM forktree_rel_row ORDER BY id", &[])
            .await
            .expect("verify current relational rows")
            .rows()
            .iter()
            .map(|row| {
                let key = match row.get_index(0) {
                    Some(Value::Text(key)) => key.as_bytes().to_vec(),
                    other => panic!("unexpected current relational identity {other:?}"),
                };
                let value = match row.get_index(1) {
                    Some(Value::Null) => RelationalValue::Null,
                    Some(Value::Text(value)) => RelationalValue::Bytes(value.as_bytes().to_vec()),
                    other => panic!("unexpected current relational value {other:?}"),
                };
                (key, value)
            })
            .collect::<Vec<_>>(),
        Fixture::ForkTree(tree) => tree
            .read_relational_all("main")
            .await
            .expect("verify relational ForkTree rows"),
    };
    assert_eq!(
        actual,
        expected
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>()
    );
}

struct OracleState {
    base: ObjectId,
    base_rows: BTreeMap<Vec<u8>, RelationalValue>,
    expected_main: BTreeMap<Vec<u8>, RelationalValue>,
    baseline_blob: Vec<u8>,
    edited_blob: Vec<u8>,
}

struct SpanSource {
    logical_bytes: u64,
    spans: VecDeque<Bytes>,
}

impl SpanSource {
    fn new(payload: Vec<u8>, span_bytes: usize) -> Self {
        let payload = Bytes::from(payload);
        let logical_bytes = payload.len() as u64;
        let spans = (0..payload.len())
            .step_by(span_bytes)
            .map(|start| payload.slice(start..payload.len().min(start + span_bytes)))
            .collect();
        Self {
            logical_bytes,
            spans,
        }
    }
}

impl SegmentedByteSource for SpanSource {
    fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }

    fn next_span(&mut self) -> Result<Option<Bytes>, String> {
        Ok(self.spans.pop_front())
    }
}

fn relational_oracle_requested() -> bool {
    std::env::var_os("FORKTREE_RELATIONAL_ORACLE").is_some()
}

async fn run_rocks_oracle() {
    let directory = tempfile::tempdir().expect("create relational RocksDB oracle directory");
    let state = {
        let database = RocksDB::open(directory.path()).expect("open relational RocksDB oracle");
        let (storage, stats) = CountingStorage::new(database.clone());
        let state = relational_oracle_setup(storage, &stats, directory.path(), None).await;
        database.flush().expect("flush relational RocksDB oracle");
        state
    };
    {
        let database = RocksDB::open(directory.path()).expect("reopen relational RocksDB oracle");
        let (storage, stats) = CountingStorage::new(database.clone());
        relational_oracle_reopen(storage, state, &stats, directory.path(), None).await;
        database
            .flush()
            .expect("flush reopened relational RocksDB oracle");
    }
    run_rocks_corruption_oracle().await;
    println!("forktree_relational_oracle,backend=rocksdb,status=pass");
}

async fn run_slate_oracle() {
    let directory = tempfile::tempdir().expect("create relational SlateDB oracle directory");
    let state = {
        let counters = SlateDBIoCounters::default();
        let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
            .expect("open relational SlateDB oracle");
        let (storage, stats) = CountingStorage::new(database.clone());
        let state =
            relational_oracle_setup(storage, &stats, directory.path(), Some(&counters)).await;
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush relational SlateDB oracle");
        state
    };
    {
        let counters = SlateDBIoCounters::default();
        let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
            .expect("reopen relational SlateDB oracle");
        let (storage, stats) = CountingStorage::new(database.clone());
        relational_oracle_reopen(storage, state, &stats, directory.path(), Some(&counters)).await;
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush reopened relational SlateDB oracle");
    }
    run_slate_corruption_oracle().await;
    println!("forktree_relational_oracle,backend=slatedb,status=pass");
}

async fn measure_oracle_phase<T>(
    phase: &str,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    future: impl Future<Output = T>,
) -> T {
    let _ = take_stats(stats);
    let physical_before = counters.map(SlateDBIoCounters::snapshot);
    let disk_before = directory_bytes(path);
    let rss_before = process_resident_bytes();
    let cpu_before = process_cpu_nanos();
    begin_allocation_profile();
    let started = Instant::now();
    let output = future.await;
    let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
    let (allocated_bytes, allocation_calls) = end_allocation_profile();
    let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
    let rss_after = process_resident_bytes();
    let disk_after = directory_bytes(path);
    let io = take_stats(stats);
    let physical = physical_delta(counters, physical_before);
    println!(
        "forktree_relational_phase,phase={phase},wall_us={wall_us:.3},cpu_us={cpu_us:.3},alloc_bytes={allocated_bytes},alloc_calls={allocation_calls},rss_before_bytes={rss_before},rss_after_bytes={rss_after},begin_reads={},begin_writes={},get_calls={},get_keys={},get_values={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},write_batches={},write_puts={},write_deletes={},write_bytes={},commits={},disk_before_bytes={disk_before},disk_after_bytes={disk_after},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={}",
        io.begin_reads,
        io.begin_writes,
        io.get_calls,
        io.get_keys,
        io.get_values,
        io.get_value_bytes,
        io.scan_calls,
        io.scan_entries,
        io.scan_value_bytes,
        io.write_batches,
        io.write_puts,
        io.write_deletes,
        io.write_bytes,
        io.commits,
        physical.read_objects,
        physical.read_bytes,
        physical.write_objects,
        physical.write_bytes,
    );
    output
}

async fn relational_oracle_setup<S>(
    storage: CountingStorage<S>,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) -> OracleState
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let base_rows = initial_rows(1_000);
    let tree = ForkTree::new(storage);
    tree.initialize(&byte_rows(&base_rows))
        .await
        .expect("initialize relational semantic oracle");
    let base = tree.branch_head("main").await.expect("load oracle base");
    assert_eq!(
        tree.read_relational_point("main", &row_key(20))
            .await
            .expect("read relational oracle point"),
        base_rows.get(&row_key(20)).cloned()
    );
    let range = tree
        .read_range("main", &row_key(20), &row_key(40))
        .await
        .expect("read relational oracle range");
    assert!(!range.is_empty());
    assert!(range.windows(2).all(|pair| pair[0].0 < pair[1].0));
    measure_oracle_phase(
        "checkpoint_root",
        stats,
        path,
        counters,
        tree.create_checkpoint("retained-base", base),
    )
    .await
    .expect("pin relational base checkpoint");
    measure_oracle_phase(
        "branch_root",
        stats,
        path,
        counters,
        tree.create_branch("disjoint-source", Some(base)),
    )
    .await
    .expect("create disjoint source");

    let target_mutations = sorted_mutations(vec![
        Mutation::Update {
            key: row_key(200),
            value: RelationalValue::Null,
        },
        Mutation::Delete { key: row_key(400) },
        Mutation::Insert {
            key: row_key(601),
            value: RelationalValue::Bytes(b"target-insert".to_vec()),
        },
    ]);
    let source_mutations = sorted_mutations(vec![
        Mutation::Update {
            key: row_key(800),
            value: RelationalValue::Bytes(b"source-update".to_vec()),
        },
        Mutation::Delete {
            key: row_key(1_000),
        },
        Mutation::Insert {
            key: row_key(1_201),
            value: RelationalValue::Null,
        },
    ]);
    let target_head = tree
        .apply_sorted_mutations(&target_mutations)
        .await
        .expect("apply disjoint target mutations")
        .0;
    let source_head = tree
        .apply_sorted_mutations_on("disjoint-source", &source_mutations)
        .await
        .expect("apply disjoint source mutations")
        .0;
    assert_eq!(
        measure_oracle_phase(
            "hash_pruned_diff",
            stats,
            path,
            counters,
            tree.diff_commits(base, target_head),
        )
        .await
        .expect("diff disjoint target")
        .len(),
        target_mutations.len()
    );
    assert_eq!(
        tree.diff_commits(base, source_head)
            .await
            .expect("diff disjoint source")
            .len(),
        source_mutations.len()
    );
    let merged = match measure_oracle_phase(
        "disjoint_three_way_merge",
        stats,
        path,
        counters,
        tree.merge_branches_three_way("main", "disjoint-source", base),
    )
    .await
    .expect("merge disjoint branches")
    {
        MergeOutcome::Merged { commit, accounting } => {
            assert!(
                accounting.node_writes < 1_000,
                "merge rebuilt the full tree"
            );
            commit
        }
        MergeOutcome::Conflicts(conflicts) => {
            panic!("disjoint merge reported conflicts: {conflicts:?}")
        }
    };
    let expected_target =
        apply_oracle(base_rows.clone(), &target_mutations).expect("target relational oracle state");
    let expected_main =
        apply_oracle(expected_target, &source_mutations).expect("merged relational oracle state");
    assert_eq!(
        tree.read_relational_all("main")
            .await
            .expect("read disjoint merge result"),
        expected_main
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        measure_oracle_phase("undo_root", stats, path, counters, tree.undo("main"))
            .await
            .expect("undo merge"),
        target_head
    );
    assert_eq!(
        measure_oracle_phase("redo_root", stats, path, counters, tree.redo("main"))
            .await
            .expect("redo merge"),
        merged
    );
    tree.delete_branch("disjoint-source")
        .await
        .expect("release disjoint source");

    verify_conflict_semantics(&tree, base, stats, path, counters).await;
    verify_invalid_mutations_fail_closed(&tree).await;

    let baseline_blob = deterministic_blob(8 * 1024 * 1024);
    let (baseline_blob_commit, baseline_accounting) = tree
        .ingest_blob("main", SpanSource::new(baseline_blob.clone(), 256 * 1024))
        .await
        .expect("ingest segmented relational blob smoke");
    assert!(baseline_accounting.chunks > 1);
    tree.create_branch("retained-blob", Some(baseline_blob_commit))
        .await
        .expect("pin baseline blob");
    let mut edited_blob = baseline_blob.clone();
    let edit_start = edited_blob.len() / 2;
    for byte in &mut edited_blob[edit_start..edit_start + 4 * 1024] {
        *byte ^= 0x5a;
    }
    let (edited_blob_commit, edited_accounting) = tree
        .ingest_blob("main", SpanSource::new(edited_blob.clone(), 256 * 1024))
        .await
        .expect("ingest localized segmented blob edit");
    assert!(edited_accounting.reused_chunks > 0);
    let blob_diff = tree
        .diff_blob_commits(baseline_blob_commit, edited_blob_commit)
        .await
        .expect("diff localized blob edit");
    assert!(blob_diff.shared_chunks > 0);
    assert_eq!(
        tree.read_blob("main")
            .await
            .expect("read edited relational blob")
            .materialize(),
        edited_blob
    );
    assert_eq!(
        tree.read_blob_range("main", edit_start as u64, (edit_start + 4 * 1024) as u64)
            .await
            .expect("read edited relational blob range")
            .materialize(),
        edited_blob[edit_start..edit_start + 4 * 1024]
    );
    tree.create_branch("blob-merge-target", Some(baseline_blob_commit))
        .await
        .expect("create blob merge target");
    tree.create_branch("blob-merge-source", Some(edited_blob_commit))
        .await
        .expect("create blob merge source");
    tree.merge_blob_branches(
        "blob-merge-target",
        "blob-merge-source",
        baseline_blob_commit,
    )
    .await
    .expect("merge localized blob edit");
    assert_eq!(
        tree.read_blob("blob-merge-target")
            .await
            .expect("read merged blob")
            .authenticated_hash(),
        blake3::hash(&edited_blob)
    );
    tree.delete_branch("blob-merge-target")
        .await
        .expect("release blob merge target");
    tree.delete_branch("blob-merge-source")
        .await
        .expect("release blob merge source");

    OracleState {
        base,
        base_rows,
        expected_main,
        baseline_blob,
        edited_blob,
    }
}

async fn verify_conflict_semantics<S>(
    tree: &ForkTree<CountingStorage<S>>,
    base: ObjectId,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    tree.create_branch("conflict-target", Some(base))
        .await
        .expect("create conflict target");
    tree.create_branch("conflict-source", Some(base))
        .await
        .expect("create conflict source");
    let inserted_key = row_key(101);
    let update_key = row_key(20);
    let delete_update_key = row_key(40);
    let identical_key = row_key(60);
    let target = sorted_mutations(vec![
        Mutation::Update {
            key: update_key.clone(),
            value: RelationalValue::Null,
        },
        Mutation::Delete {
            key: delete_update_key.clone(),
        },
        Mutation::Update {
            key: identical_key.clone(),
            value: RelationalValue::Null,
        },
        Mutation::Insert {
            key: inserted_key.clone(),
            value: RelationalValue::Bytes(b"target insert".to_vec()),
        },
    ]);
    let source = sorted_mutations(vec![
        Mutation::Update {
            key: update_key.clone(),
            value: RelationalValue::Bytes(b"source update".to_vec()),
        },
        Mutation::Update {
            key: delete_update_key.clone(),
            value: RelationalValue::Bytes(b"source after delete".to_vec()),
        },
        Mutation::Update {
            key: identical_key,
            value: RelationalValue::Null,
        },
        Mutation::Insert {
            key: inserted_key.clone(),
            value: RelationalValue::Bytes(b"source insert".to_vec()),
        },
    ]);
    tree.apply_sorted_mutations_on("conflict-target", &target)
        .await
        .expect("apply conflict target");
    tree.apply_sorted_mutations_on("conflict-source", &source)
        .await
        .expect("apply conflict source");
    let target_before = tree
        .branch_head("conflict-target")
        .await
        .expect("load conflict target head");
    let conflicts = match measure_oracle_phase(
        "overlapping_conflict_merge",
        stats,
        path,
        counters,
        tree.merge_branches_three_way("conflict-target", "conflict-source", base),
    )
    .await
    .expect("evaluate conflict merge")
    {
        MergeOutcome::Conflicts(conflicts) => conflicts,
        MergeOutcome::Merged { .. } => panic!("divergent same identities merged"),
    };
    let conflict_keys = conflicts
        .iter()
        .map(|conflict| conflict.key.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        conflict_keys,
        sorted_keys(vec![update_key, delete_update_key, inserted_key])
    );
    assert!(conflicts.iter().any(|conflict| {
        conflict.target.is_none() && matches!(conflict.source, Some(RelationalValue::Bytes(_)))
    }));
    assert!(conflicts.iter().any(|conflict| {
        conflict.base.is_none() && conflict.target.is_some() && conflict.source.is_some()
    }));
    assert_eq!(
        tree.branch_head("conflict-target")
            .await
            .expect("reload conflict target head"),
        target_before,
        "conflicting merge moved its target selector"
    );
    tree.delete_branch("conflict-target")
        .await
        .expect("release conflict target");
    tree.delete_branch("conflict-source")
        .await
        .expect("release conflict source");

    tree.create_branch("identical-target", Some(base))
        .await
        .expect("create identical target");
    tree.create_branch("identical-source", Some(base))
        .await
        .expect("create identical source");
    let identical = vec![Mutation::Update {
        key: row_key(140),
        value: RelationalValue::Null,
    }];
    tree.apply_sorted_mutations_on("identical-target", &identical)
        .await
        .expect("apply identical target");
    tree.apply_sorted_mutations_on("identical-source", &identical)
        .await
        .expect("apply identical source");
    match tree
        .merge_branches_three_way("identical-target", "identical-source", base)
        .await
        .expect("merge identical edits")
    {
        MergeOutcome::Merged { .. } => {}
        MergeOutcome::Conflicts(conflicts) => {
            panic!("identical semantic edits conflicted: {conflicts:?}")
        }
    }
    assert_eq!(
        tree.read_relational_point("identical-target", &row_key(140))
            .await
            .expect("read identical merged NULL"),
        Some(RelationalValue::Null)
    );
    tree.delete_branch("identical-target")
        .await
        .expect("release identical target");
    tree.delete_branch("identical-source")
        .await
        .expect("release identical source");
}

async fn verify_invalid_mutations_fail_closed<S>(tree: &ForkTree<CountingStorage<S>>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let before = tree.branch_head("main").await.expect("load main head");
    let cases = [
        Mutation::Insert {
            key: row_key(20),
            value: RelationalValue::Bytes(b"duplicate".to_vec()),
        },
        Mutation::Update {
            key: row_key(99_999_998),
            value: RelationalValue::Null,
        },
        Mutation::Delete {
            key: row_key(99_999_996),
        },
    ];
    for mutation in cases {
        assert!(tree.apply_sorted_mutations(&[mutation]).await.is_err());
        assert_eq!(
            tree.branch_head("main").await.expect("reload main head"),
            before,
            "invalid mutation moved the authoritative selector"
        );
    }
}

async fn relational_oracle_reopen<S>(
    storage: CountingStorage<S>,
    state: OracleState,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let tree = ForkTree::new(storage);
    assert_eq!(
        tree.read_relational_all("main")
            .await
            .expect("cold-read relational state"),
        state
            .expected_main
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        tree.read_blob("main")
            .await
            .expect("cold-read edited blob")
            .authenticated_hash(),
        blake3::hash(&state.edited_blob)
    );
    assert_eq!(
        tree.checkpoint_head("retained-base")
            .await
            .expect("cold-read checkpoint selector"),
        state.base
    );
    measure_oracle_phase(
        "retention_root",
        stats,
        path,
        counters,
        tree.compact_history("main"),
    )
    .await
    .expect("publish relational retention boundary");
    let retained = measure_oracle_phase(
        "retained_root_gc",
        stats,
        path,
        counters,
        tree.reclaim_unreachable(),
    )
    .await
    .expect("reclaim with relational roots retained");
    assert!(retained.reachable_objects > 0);
    assert_eq!(
        tree.read_relational_all_at(state.base)
            .await
            .expect("read retained base after GC"),
        state
            .base_rows
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        tree.read_blob("retained-blob")
            .await
            .expect("read retained baseline blob after GC")
            .authenticated_hash(),
        blake3::hash(&state.baseline_blob)
    );
    tree.delete_checkpoint("retained-base")
        .await
        .expect("release relational base checkpoint");
    tree.delete_branch("retained-blob")
        .await
        .expect("release baseline blob root");
    let released = measure_oracle_phase(
        "released_root_gc",
        stats,
        path,
        counters,
        tree.reclaim_unreachable(),
    )
    .await
    .expect("reclaim released relational roots");
    assert!(released.reclaimed_objects > 0);
    assert_eq!(
        tree.read_blob("main")
            .await
            .expect("read live edited blob after old-root reclamation")
            .authenticated_hash(),
        blake3::hash(&state.edited_blob)
    );
    tree.verify_publication_gc_races()
        .await
        .expect("verify relational publication/GC races");
    tree.delete_branch("main")
        .await
        .expect("release final relational root");
    let final_release = tree
        .reclaim_unreachable()
        .await
        .expect("reclaim final relational root");
    assert!(final_release.reclaimed_objects > 0);
    assert_eq!(
        tree.object_inventory()
            .await
            .expect("inventory final relational reclamation"),
        (0, 0)
    );
}

async fn run_rocks_corruption_oracle() {
    let tree_directory = tempfile::tempdir().expect("create RocksDB tree corruption oracle");
    let tree_database =
        RocksDB::open(tree_directory.path()).expect("open RocksDB tree corruption oracle");
    let (tree_storage, _) = CountingStorage::new(tree_database);
    relational_tree_corruption_oracle(tree_storage).await;

    let directory = tempfile::tempdir().expect("create RocksDB corruption oracle");
    let database = RocksDB::open(directory.path()).expect("open RocksDB corruption oracle");
    let (storage, _) = CountingStorage::new(database);
    blob_corruption_oracle(storage).await;
}

async fn run_slate_corruption_oracle() {
    let tree_directory = tempfile::tempdir().expect("create SlateDB tree corruption oracle");
    let tree_database =
        SlateDB::open(tree_directory.path()).expect("open SlateDB tree corruption oracle");
    let (tree_storage, _) = CountingStorage::new(tree_database);
    relational_tree_corruption_oracle(tree_storage).await;

    let directory = tempfile::tempdir().expect("create SlateDB corruption oracle");
    let database = SlateDB::open(directory.path()).expect("open SlateDB corruption oracle");
    let (storage, _) = CountingStorage::new(database);
    blob_corruption_oracle(storage).await;
}

async fn relational_tree_corruption_oracle<S>(storage: CountingStorage<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let tree = ForkTree::new(storage);
    let rows = initial_rows(32);
    tree.initialize(&byte_rows(&rows))
        .await
        .expect("initialize relational tree corruption oracle");
    tree.verify_tree_corruption_fail_closed("main")
        .await
        .expect("verify authenticated tree corruption failure");
}

async fn blob_corruption_oracle<S>(storage: CountingStorage<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let tree = ForkTree::new(storage);
    let rows = initial_rows(32);
    tree.initialize(&byte_rows(&rows))
        .await
        .expect("initialize relational corruption oracle");
    let blob = deterministic_blob(2 * 1024 * 1024);
    tree.ingest_blob("main", SpanSource::new(blob, 128 * 1024))
        .await
        .expect("ingest corruption oracle blob");
    tree.verify_blob_corruption_fail_closed("main")
        .await
        .expect("verify authenticated corruption failure");
}

fn byte_rows(rows: &BTreeMap<Vec<u8>, RelationalValue>) -> Vec<(Vec<u8>, Vec<u8>)> {
    rows.iter()
        .map(|(key, value)| match value {
            RelationalValue::Bytes(value) => (key.clone(), value.clone()),
            RelationalValue::Null => panic!("initial oracle rows are non-null"),
        })
        .collect()
}

fn sorted_mutations(mut mutations: Vec<Mutation>) -> Vec<Mutation> {
    mutations.sort_by(|left, right| left.key().cmp(right.key()));
    assert!(
        mutations
            .windows(2)
            .all(|pair| pair[0].key() < pair[1].key())
    );
    mutations
}

fn sorted_keys(mut keys: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    keys.sort();
    keys
}

fn deterministic_blob(bytes: usize) -> Vec<u8> {
    let mut state = 0x6a09_e667_f3bc_c909_u64;
    (0..bytes)
        .map(|index| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state as u8) ^ (index as u8).wrapping_mul(31)
        })
        .collect()
}

fn initial_rows(rows: usize) -> BTreeMap<Vec<u8>, RelationalValue> {
    (0..rows)
        .map(|index| {
            (
                row_key(index.saturating_mul(2)),
                RelationalValue::Bytes(format!("value-{index:010}").into_bytes()),
            )
        })
        .collect()
}

fn mutation_batch(rows: usize, mutations: usize, kind: MutationKind) -> Vec<Mutation> {
    let selected = (0..mutations)
        .map(|index| (index + 1) * rows / (mutations + 1))
        .collect::<Vec<_>>();
    let mut batch = selected
        .into_iter()
        .enumerate()
        .map(|(ordinal, index)| {
            let nullable = || {
                if ordinal % 4 == 0 {
                    RelationalValue::Null
                } else {
                    RelationalValue::Bytes(format!("changed-{ordinal:010}").into_bytes())
                }
            };
            match kind {
                MutationKind::Insert => Mutation::Insert {
                    key: row_key(index.saturating_mul(2).saturating_add(1)),
                    value: nullable(),
                },
                MutationKind::Delete => Mutation::Delete {
                    key: row_key(index.saturating_mul(2)),
                },
                MutationKind::Update => Mutation::Update {
                    key: row_key(index.saturating_mul(2)),
                    value: nullable(),
                },
                MutationKind::Mixed => match ordinal % 3 {
                    0 => Mutation::Insert {
                        key: row_key(index.saturating_mul(2).saturating_add(1)),
                        value: nullable(),
                    },
                    1 => Mutation::Delete {
                        key: row_key(index.saturating_mul(2)),
                    },
                    _ => Mutation::Update {
                        key: row_key(index.saturating_mul(2)),
                        value: nullable(),
                    },
                },
            }
        })
        .collect::<Vec<_>>();
    batch.sort_by(|left, right| left.key().cmp(right.key()));
    assert!(batch.windows(2).all(|pair| pair[0].key() < pair[1].key()));
    batch
}

fn apply_oracle(
    mut rows: BTreeMap<Vec<u8>, RelationalValue>,
    mutations: &[Mutation],
) -> Result<BTreeMap<Vec<u8>, RelationalValue>, String> {
    for mutation in mutations {
        match mutation {
            Mutation::Insert { key, value } => {
                if rows.insert(key.clone(), value.clone()).is_some() {
                    return Err("oracle duplicate insert".to_string());
                }
            }
            Mutation::Update { key, value } => {
                let slot = rows
                    .get_mut(key)
                    .ok_or_else(|| "oracle update absent identity".to_string())?;
                *slot = value.clone();
            }
            Mutation::Delete { key } => {
                if rows.remove(key).is_none() {
                    return Err("oracle delete absent identity".to_string());
                }
            }
        }
    }
    Ok(rows)
}

fn logical_bytes(mutations: &[Mutation]) -> u64 {
    mutations
        .iter()
        .map(|mutation| match mutation {
            Mutation::Insert { key, value } | Mutation::Update { key, value } => {
                key.len() as u64
                    + match value {
                        RelationalValue::Null => 1,
                        RelationalValue::Bytes(value) => value.len() as u64 + 1,
                    }
                    + 1
            }
            Mutation::Delete { key } => key.len() as u64 + 1,
        })
        .sum()
}

fn print_summary(parameters: Parameters, kind: MutationKind, observations: &[Observation]) {
    fn median(mut values: Vec<f64>) -> f64 {
        values.sort_by(f64::total_cmp);
        values[values.len() / 2]
    }
    let wall = median(observations.iter().map(|value| value.wall_us).collect());
    let cpu = median(observations.iter().map(|value| value.cpu_us).collect());
    let allocations = median(
        observations
            .iter()
            .map(|value| value.allocated_bytes as f64)
            .collect(),
    );
    let allocation_calls = median(
        observations
            .iter()
            .map(|value| value.allocation_calls as f64)
            .collect(),
    );
    let disk = median(
        observations
            .iter()
            .map(|value| value.post_flush_disk_bytes as f64)
            .collect(),
    );
    println!(
        "forktree_relational_summary,backend={},layout={},kind={},rows={},mutations={},samples={},median_wall_us={wall:.3},median_cpu_us={cpu:.3},median_alloc_bytes={allocations:.1},median_alloc_calls={allocation_calls:.1},median_post_flush_disk_bytes={disk:.1}",
        parameters.backend.label(),
        parameters.layout.label(),
        kind.label(),
        parameters.rows,
        parameters.updates,
        parameters.samples,
    );
}

fn row_key(index: usize) -> Vec<u8> {
    format!("row/{index:010}").into_bytes()
}

fn text_key(key: &[u8]) -> String {
    String::from_utf8(key.to_vec()).expect("UTF-8 relational identity")
}

fn lix_value(value: &RelationalValue) -> Value {
    match value {
        RelationalValue::Null => Value::Null,
        RelationalValue::Bytes(value) => {
            Value::Text(String::from_utf8(value.clone()).expect("UTF-8 relational value"))
        }
    }
}
