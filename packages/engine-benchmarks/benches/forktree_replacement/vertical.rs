use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::storage_adapter::StorageAdapter;
use lix::{CreateBranchOptions, MergeBranchOptions, SwitchBranchOptions, Value};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

use super::model::{ApplyAccounting, ForkTree, SegmentedByteSource, Update};
use super::{
    Backend, CountingStorage, IoStats, Layout, Parameters, Scenario, apply_current,
    apply_replacement, begin_allocation_profile, directory_bytes, end_allocation_profile,
    physical_delta, prepare_current, prepare_replacement, process_cpu_ticks,
    process_resident_bytes, settle_rocksdb_compaction, take_stats, value_for_generation,
};

const SOURCE_BRANCH_ID: &str = "019f0000-0000-7000-8000-000000000111";

#[derive(Clone)]
struct HistoryOracle {
    point_key: String,
    point_value: String,
    range_start: String,
    range_end: String,
    range_len: usize,
}

pub(super) async fn run(parameters: Parameters) {
    match parameters.backend {
        Backend::RocksDb => run_rocksdb(parameters).await,
        Backend::SlateDb => run_slatedb(parameters).await,
    }
}

async fn run_rocksdb(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create ForkTree vertical RocksDB directory");
    let oracle = {
        let database = RocksDB::open(directory.path()).expect("open ForkTree vertical RocksDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let oracle = match parameters.scenario {
            Scenario::History => {
                run_history_setup(storage, parameters, &stats, directory.path(), None).await
            }
            Scenario::Blob => {
                run_blob_setup(storage, parameters, &stats, directory.path(), None).await
            }
            Scenario::BlobProfile => {
                run_blob_profile_setup(storage, parameters, &stats, directory.path(), None).await
            }
            Scenario::Apply
            | Scenario::Relational
            | Scenario::Olap
            | Scenario::OlapMemory
            | Scenario::OlapDatafusion
            | Scenario::Dml => unreachable!(),
        };
        database
            .flush()
            .expect("flush ForkTree vertical RocksDB before close");
        println!(
            "forktree_vertical_close,scenario={:?},backend=rocksdb,layout={},disk_bytes={}",
            parameters.scenario,
            parameters.layout.label(),
            directory_bytes(directory.path())
        );
        oracle
    };

    let database = RocksDB::open(directory.path()).expect("reopen ForkTree vertical RocksDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    match oracle {
        VerticalOracle::History(oracle) => {
            run_history_reopen(storage, parameters, &stats, directory.path(), None, oracle).await;
        }
        VerticalOracle::Blob(oracle) => {
            run_blob_reopen(storage, parameters, &stats, directory.path(), None, oracle).await;
        }
        VerticalOracle::Profile => {}
    }
    database
        .flush()
        .expect("flush ForkTree vertical RocksDB final state");
    let post_flush_disk_bytes = directory_bytes(directory.path());
    println!(
        "forktree_vertical_lifecycle,scenario={:?},backend=rocksdb,layout={},post_flush_disk_bytes={}",
        parameters.scenario,
        parameters.layout.label(),
        post_flush_disk_bytes
    );
    drop(database);
    if std::env::var_os("FORKTREE_SETTLE_COMPACTION").is_some() {
        println!(
            "forktree_vertical_settled,scenario={:?},backend=rocksdb,layout={},post_flush_disk_bytes={post_flush_disk_bytes},post_compaction_disk_bytes={}",
            parameters.scenario,
            parameters.layout.label(),
            settle_rocksdb_compaction(directory.path())
        );
    }
}

async fn run_slatedb(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create ForkTree vertical SlateDB directory");
    let oracle = {
        let counters = SlateDBIoCounters::default();
        let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
            .expect("open ForkTree vertical SlateDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let oracle = match parameters.scenario {
            Scenario::History => {
                run_history_setup(
                    storage,
                    parameters,
                    &stats,
                    directory.path(),
                    Some(&counters),
                )
                .await
            }
            Scenario::Blob => {
                run_blob_setup(
                    storage,
                    parameters,
                    &stats,
                    directory.path(),
                    Some(&counters),
                )
                .await
            }
            Scenario::BlobProfile => {
                run_blob_profile_setup(
                    storage,
                    parameters,
                    &stats,
                    directory.path(),
                    Some(&counters),
                )
                .await
            }
            Scenario::Apply
            | Scenario::Relational
            | Scenario::Olap
            | Scenario::OlapMemory
            | Scenario::OlapDatafusion
            | Scenario::Dml => unreachable!(),
        };
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush ForkTree vertical SlateDB before close");
        println!(
            "forktree_vertical_close,scenario={:?},backend=slatedb,layout={},disk_bytes={}",
            parameters.scenario,
            parameters.layout.label(),
            directory_bytes(directory.path())
        );
        oracle
    };

    let counters = SlateDBIoCounters::default();
    let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
        .expect("reopen ForkTree vertical SlateDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    match oracle {
        VerticalOracle::History(oracle) => {
            run_history_reopen(
                storage,
                parameters,
                &stats,
                directory.path(),
                Some(&counters),
                oracle,
            )
            .await;
        }
        VerticalOracle::Blob(oracle) => {
            run_blob_reopen(
                storage,
                parameters,
                &stats,
                directory.path(),
                Some(&counters),
                oracle,
            )
            .await;
        }
        VerticalOracle::Profile => {}
    }
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush ForkTree vertical SlateDB final state");
    println!(
        "forktree_vertical_lifecycle,scenario={:?},backend=slatedb,layout={},post_flush_disk_bytes={}",
        parameters.scenario,
        parameters.layout.label(),
        directory_bytes(directory.path())
    );
}

enum VerticalOracle {
    History(HistoryOracle),
    Blob(BlobOracle),
    Profile,
}

async fn run_history_setup<S>(
    storage: CountingStorage<S>,
    parameters: Parameters,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) -> VerticalOracle
where
    S: Storage + Clone + Send + Sync + 'static,
{
    assert_eq!(parameters.rows, 1_000, "semantic gate uses 1K live rows");
    assert_eq!(
        parameters.updates, 1,
        "semantic history uses localized K=1 commits"
    );
    match parameters.layout {
        Layout::Current => VerticalOracle::History(
            current_history_setup(storage, parameters, stats, path, counters).await,
        ),
        Layout::ForkTree => VerticalOracle::History(
            forktree_history_setup(storage, parameters, stats, path, counters).await,
        ),
    }
}

async fn current_history_setup<S>(
    storage: CountingStorage<S>,
    parameters: Parameters,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) -> HistoryOracle
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut fixture = prepare_current(storage, parameters).await;
    let baseline = current_head(&fixture.session).await;
    let point_index = parameters.rows / 3;
    let source_index = parameters.rows / 4;
    let target_index = parameters.rows * 3 / 4;
    let range_start_index = parameters.rows / 2;
    let range_end_index = range_start_index + 31;

    measure_phase(
        "point_read",
        parameters,
        1,
        stats,
        path,
        counters,
        current_point(&fixture.session, &fixture.rows[point_index].path),
    )
    .await;
    let initial_range = measure_phase(
        "range_read_32",
        parameters,
        1,
        stats,
        path,
        counters,
        current_range(
            &fixture.session,
            &fixture.rows[range_start_index].path,
            &fixture.rows[range_end_index].path,
        ),
    )
    .await;
    assert_eq!(initial_range, 32);

    let mut logical_bytes = 0_u64;
    measure_phase(
        "history_1000_localized_updates",
        parameters,
        parameters.iterations as u64,
        stats,
        path,
        counters,
        async {
            for _ in 0..parameters.iterations {
                logical_bytes += apply_current(&mut fixture).await;
            }
        },
    )
    .await;
    let history_head = current_head(&fixture.session).await;
    let diff_count = measure_phase(
        "history_diff",
        parameters,
        1,
        stats,
        path,
        counters,
        current_diff_count(&fixture.session, &baseline, &history_head),
    )
    .await;
    assert_eq!(diff_count, 1);
    println!(
        "forktree_vertical_oracle,phase=history,layout=current_lix,logical_bytes={},diff_rows={diff_count}",
        logical_bytes
    );

    let branch = measure_phase(
        "branch_root_publication",
        parameters,
        1,
        stats,
        path,
        counters,
        fixture.session.create_branch(CreateBranchOptions {
            id: Some(SOURCE_BRANCH_ID.to_owned()),
            name: "forktree-semantic-source".to_owned(),
            from_commit_id: Some(history_head.clone()),
        }),
    )
    .await
    .expect("create current-Lix semantic source branch");
    assert_eq!(branch.commit_id, history_head);
    let main_branch = fixture
        .session
        .active_branch_id()
        .await
        .expect("load current-Lix main branch");
    fixture
        .session
        .switch_branch(SwitchBranchOptions {
            branch_id: SOURCE_BRANCH_ID.to_owned(),
        })
        .await
        .expect("switch to current-Lix source branch");
    let source_value = value_for_generation(&fixture.rows[source_index], parameters.iterations + 1);
    update_current_row(
        &fixture.session,
        &fixture.rows[source_index].path,
        &source_value,
    )
    .await;
    let source_head = current_head(&fixture.session).await;
    fixture
        .session
        .switch_branch(SwitchBranchOptions {
            branch_id: main_branch,
        })
        .await
        .expect("switch to current-Lix main branch");
    let target_value = value_for_generation(&fixture.rows[target_index], parameters.iterations + 2);
    update_current_row(
        &fixture.session,
        &fixture.rows[target_index].path,
        &target_value,
    )
    .await;
    let source_diff = current_diff_count(&fixture.session, &history_head, &source_head).await;
    assert_eq!(source_diff, 1);
    measure_phase(
        "merge",
        parameters,
        1,
        stats,
        path,
        counters,
        fixture.session.merge_branch(MergeBranchOptions {
            source_branch_id: SOURCE_BRANCH_ID.to_owned(),
        }),
    )
    .await
    .expect("merge current-Lix semantic source");
    assert_eq!(
        current_point(&fixture.session, &fixture.rows[source_index].path).await,
        source_value
    );
    let undo_value = value_for_generation(&fixture.rows[point_index], parameters.iterations + 3);
    update_current_row(
        &fixture.session,
        &fixture.rows[point_index].path,
        &undo_value,
    )
    .await;
    measure_phase(
        "undo_root_equivalent",
        parameters,
        1,
        stats,
        path,
        counters,
        fixture.session.undo(),
    )
    .await
    .expect("undo current-Lix merge");
    measure_phase(
        "redo_root_equivalent",
        parameters,
        1,
        stats,
        path,
        counters,
        fixture.session.redo(),
    )
    .await
    .expect("redo current-Lix merge");
    measure_phase(
        "checkpoint",
        parameters,
        1,
        stats,
        path,
        counters,
        fixture.session.create_checkpoint(),
    )
    .await
    .expect("checkpoint current-Lix semantic history");

    let oracle = HistoryOracle {
        point_key: fixture.rows[source_index].path.clone(),
        point_value: source_value,
        range_start: fixture.rows[range_start_index].path.clone(),
        range_end: fixture.rows[range_end_index].path.clone(),
        range_len: 32,
    };
    fixture
        .session
        .close()
        .await
        .expect("close current-Lix history session");
    oracle
}

async fn forktree_history_setup<S>(
    storage: CountingStorage<S>,
    parameters: Parameters,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) -> HistoryOracle
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut fixture = prepare_replacement(storage, parameters).await;
    let baseline = fixture
        .tree
        .branch_head("main")
        .await
        .expect("ForkTree baseline");
    let point_index = parameters.rows / 3;
    let source_index = parameters.rows / 4;
    let target_index = parameters.rows * 3 / 4;
    let range_start_index = parameters.rows / 2;
    let range_end_index = range_start_index + 31;
    let point = measure_phase(
        "point_read",
        parameters,
        1,
        stats,
        path,
        counters,
        fixture
            .tree
            .read_point("main", fixture.rows[point_index].path.as_bytes()),
    )
    .await
    .expect("ForkTree point read");
    assert_eq!(point, fixture.rows[point_index].value_json.as_bytes());
    let range = measure_phase(
        "range_read_32",
        parameters,
        1,
        stats,
        path,
        counters,
        fixture.tree.read_range(
            "main",
            fixture.rows[range_start_index].path.as_bytes(),
            fixture.rows[range_end_index].path.as_bytes(),
        ),
    )
    .await
    .expect("ForkTree range read");
    assert_eq!(range.len(), 32);

    let mut accounting = ApplyAccounting::default();
    measure_phase(
        "history_1000_localized_updates",
        parameters,
        parameters.iterations as u64,
        stats,
        path,
        counters,
        async {
            for _ in 0..parameters.iterations {
                accounting += apply_replacement(&mut fixture).await;
            }
        },
    )
    .await;
    let history_head = fixture
        .tree
        .branch_head("main")
        .await
        .expect("ForkTree history head");
    let changed = measure_phase(
        "history_diff",
        parameters,
        1,
        stats,
        path,
        counters,
        fixture.tree.diff_commits(baseline, history_head),
    )
    .await
    .expect("ForkTree history diff");
    assert_eq!(changed.len(), 1);
    println!(
        "forktree_vertical_oracle,phase=history,layout=forktree,logical_bytes={},object_writes={},object_bytes={},node_writes={},node_bytes={},diff_rows={}",
        accounting.logical_bytes,
        accounting.object_writes,
        accounting.object_bytes,
        accounting.node_writes,
        accounting.node_bytes,
        changed.len()
    );

    measure_phase(
        "branch_root_publication",
        parameters,
        1,
        stats,
        path,
        counters,
        fixture.tree.create_branch("source", Some(history_head)),
    )
    .await
    .expect("create ForkTree semantic source branch");
    let source_value = value_for_generation(&fixture.rows[source_index], parameters.iterations + 1);
    fixture
        .tree
        .apply_sorted_updates_on(
            "source",
            &[Update {
                key: fixture.rows[source_index].path.as_bytes().to_vec(),
                value: source_value.as_bytes().to_vec(),
            }],
        )
        .await
        .expect("update ForkTree source branch");
    let target_value = value_for_generation(&fixture.rows[target_index], parameters.iterations + 2);
    fixture
        .tree
        .apply_sorted_updates_on(
            "main",
            &[Update {
                key: fixture.rows[target_index].path.as_bytes().to_vec(),
                value: target_value.as_bytes().to_vec(),
            }],
        )
        .await
        .expect("update ForkTree target branch");
    let source_head = fixture
        .tree
        .branch_head("source")
        .await
        .expect("source head");
    assert_eq!(
        fixture
            .tree
            .diff_commits(history_head, source_head)
            .await
            .expect("diff ForkTree source")
            .len(),
        1
    );
    measure_phase(
        "merge",
        parameters,
        1,
        stats,
        path,
        counters,
        fixture.tree.merge_branches("main", "source", history_head),
    )
    .await
    .expect("merge ForkTree branches");
    assert_eq!(
        fixture
            .tree
            .read_point("main", fixture.rows[source_index].path.as_bytes())
            .await
            .expect("read ForkTree merged row"),
        source_value.as_bytes()
    );
    let undo_value = value_for_generation(&fixture.rows[point_index], parameters.iterations + 3);
    fixture
        .tree
        .apply_sorted_updates_on(
            "main",
            &[Update {
                key: fixture.rows[point_index].path.as_bytes().to_vec(),
                value: undo_value.into_bytes(),
            }],
        )
        .await
        .expect("publish ForkTree undo/redo edit");
    let merged_head = fixture.tree.branch_head("main").await.expect("replay head");
    measure_phase(
        "undo_root_movement",
        parameters,
        1,
        stats,
        path,
        counters,
        fixture.tree.undo("main"),
    )
    .await
    .expect("undo ForkTree merge");
    measure_phase(
        "redo_root_movement",
        parameters,
        1,
        stats,
        path,
        counters,
        fixture.tree.redo("main"),
    )
    .await
    .expect("redo ForkTree merge");
    assert_eq!(fixture.tree.branch_head("main").await.unwrap(), merged_head);
    measure_phase(
        "checkpoint_root_publication",
        parameters,
        1,
        stats,
        path,
        counters,
        fixture.tree.create_checkpoint("history", merged_head),
    )
    .await
    .expect("pin ForkTree history checkpoint");
    measure_phase(
        "retention_boundary",
        parameters,
        1,
        stats,
        path,
        counters,
        fixture.tree.compact_history("main"),
    )
    .await
    .expect("publish ForkTree retention boundary");
    let retained = fixture
        .tree
        .reclaim_unreachable()
        .await
        .expect("sweep while ForkTree checkpoint retained");
    println!(
        "forktree_vertical_gc,phase=retained,roots={},reachable={},scanned={},reclaimed={},reclaimed_bytes={},pages={},peak_frontier={}",
        retained.roots,
        retained.reachable_objects,
        retained.scanned_objects,
        retained.reclaimed_objects,
        retained.reclaimed_bytes,
        retained.pages,
        retained.peak_frontier
    );
    HistoryOracle {
        point_key: fixture.rows[source_index].path.clone(),
        point_value: source_value,
        range_start: fixture.rows[range_start_index].path.clone(),
        range_end: fixture.rows[range_end_index].path.clone(),
        range_len: 32,
    }
}

async fn run_history_reopen<S>(
    storage: CountingStorage<S>,
    parameters: Parameters,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    oracle: HistoryOracle,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    match parameters.layout {
        Layout::Current => {
            let session = measure_phase(
                "cold_reopen_recovery_read",
                parameters,
                1,
                stats,
                path,
                counters,
                async {
                    let engine = Engine::new(storage.clone())
                        .await
                        .expect("reopen current-Lix history engine");
                    let session = engine
                        .open_workspace_session()
                        .await
                        .expect("reopen current-Lix history session");
                    assert_eq!(
                        current_point(&session, &oracle.point_key).await,
                        oracle.point_value
                    );
                    assert_eq!(
                        current_range(&session, &oracle.range_start, &oracle.range_end).await,
                        oracle.range_len
                    );
                    session
                },
            )
            .await;
            session
                .execute(
                    "DELETE FROM lix_branch WHERE id = $1",
                    &[Value::Text(SOURCE_BRANCH_ID.to_owned())],
                )
                .await
                .expect("release current-Lix source branch");
            measure_phase(
                "retention_checkpoint_and_gc",
                parameters,
                1,
                stats,
                path,
                counters,
                session.create_checkpoint(),
            )
            .await
            .expect("checkpoint current-Lix after branch release");
            session
                .close()
                .await
                .expect("close reopened current-Lix session");
        }
        Layout::ForkTree => {
            let tree = ForkTree::new(storage);
            measure_phase(
                "cold_reopen_recovery_read",
                parameters,
                1,
                stats,
                path,
                counters,
                async {
                    assert_eq!(
                        tree.read_point("main", oracle.point_key.as_bytes())
                            .await
                            .expect("cold ForkTree point read"),
                        oracle.point_value.as_bytes()
                    );
                    assert_eq!(
                        tree.read_range(
                            "main",
                            oracle.range_start.as_bytes(),
                            oracle.range_end.as_bytes()
                        )
                        .await
                        .expect("cold ForkTree range read")
                        .len(),
                        oracle.range_len
                    );
                    let recovery = tree
                        .checkpoint_head("history")
                        .await
                        .expect("load ForkTree recovery checkpoint");
                    tree.create_branch("recovered", Some(recovery))
                        .await
                        .expect("publish ForkTree recovered branch");
                    assert_eq!(
                        tree.read_point("recovered", oracle.point_key.as_bytes())
                            .await
                            .expect("read recovered ForkTree branch"),
                        oracle.point_value.as_bytes()
                    );
                },
            )
            .await;
            tree.delete_branch("recovered")
                .await
                .expect("release ForkTree recovered branch");
            tree.delete_branch("source")
                .await
                .expect("release ForkTree source branch");
            tree.delete_checkpoint("history")
                .await
                .expect("release ForkTree history checkpoint");
            let reclaimed = measure_phase(
                "final_reference_reclamation",
                parameters,
                1,
                stats,
                path,
                counters,
                tree.reclaim_unreachable(),
            )
            .await
            .expect("reclaim released ForkTree history");
            assert!(reclaimed.reclaimed_objects > 0);
            assert_eq!(
                tree.read_point("main", oracle.point_key.as_bytes())
                    .await
                    .expect("read ForkTree after reclamation"),
                oracle.point_value.as_bytes()
            );
            println!(
                "forktree_vertical_gc,phase=released,roots={},reachable={},scanned={},reclaimed={},reclaimed_bytes={},pages={},peak_frontier={}",
                reclaimed.roots,
                reclaimed.reachable_objects,
                reclaimed.scanned_objects,
                reclaimed.reclaimed_objects,
                reclaimed.reclaimed_bytes,
                reclaimed.pages,
                reclaimed.peak_frontier
            );
        }
    }
}

async fn current_head<S>(session: &SessionContext<CountingStorage<S>>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let branch = session
        .active_branch_id()
        .await
        .expect("load current-Lix active branch");
    session
        .execute(
            "SELECT commit_id FROM lix_branch WHERE id = $1",
            &[Value::Text(branch)],
        )
        .await
        .expect("load current-Lix branch head")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("current-Lix branch head is text")
}

async fn current_point<S>(session: &SessionContext<CountingStorage<S>>, key: &str) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(
            "SELECT value FROM forktree_row WHERE path = $1",
            &[Value::Text(key.to_owned())],
        )
        .await
        .expect("read current-Lix point")
        .rows()[0]
        .get::<String>("value")
        .expect("current-Lix point value is text")
}

async fn current_range<S>(
    session: &SessionContext<CountingStorage<S>>,
    start: &str,
    end: &str,
) -> usize
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(
            "SELECT path, value FROM forktree_row WHERE path >= $1 AND path <= $2 ORDER BY path",
            &[Value::Text(start.to_owned()), Value::Text(end.to_owned())],
        )
        .await
        .expect("read current-Lix range")
        .len()
}

async fn current_diff_count<S>(
    session: &SessionContext<CountingStorage<S>>,
    before: &str,
    after: &str,
) -> i64
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(
            "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) WHERE schema_key = 'forktree_row'",
            &[
                Value::Text(before.to_owned()),
                Value::Text(after.to_owned()),
            ],
        )
        .await
        .expect("diff current-Lix history")
        .rows()[0]
        .get::<i64>("entries")
        .expect("current-Lix diff count is i64")
}

async fn update_current_row<S>(session: &SessionContext<CountingStorage<S>>, key: &str, value: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let affected = session
        .execute(
            "UPDATE forktree_row SET value = $1 WHERE path = $2",
            &[Value::Text(value.to_owned()), Value::Text(key.to_owned())],
        )
        .await
        .expect("update current-Lix semantic row")
        .rows_affected();
    assert_eq!(affected, 1);
}

async fn measure_phase<T>(
    phase: &str,
    parameters: Parameters,
    operations: u64,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    operation: impl Future<Output = T>,
) -> T {
    let _ = take_stats(stats);
    let physical_before = counters.map(SlateDBIoCounters::snapshot);
    let disk_before = directory_bytes(path);
    let rss_before = process_resident_bytes();
    let cpu_before = process_cpu_ticks();
    begin_allocation_profile();
    let started = Instant::now();
    let result = operation.await;
    let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
    let (allocated_bytes, allocation_calls) = end_allocation_profile();
    let cpu_ticks = process_cpu_ticks().saturating_sub(cpu_before);
    let rss_after = process_resident_bytes();
    let disk_after = directory_bytes(path);
    let io = take_stats(stats);
    let physical = physical_delta(counters, physical_before);
    let operations = operations.max(1);
    println!(
        "forktree_vertical_phase,scenario={:?},phase={phase},backend={},layout={},operations={},wall_us_total={wall_us:.3},wall_us_per_op={:.3},cpu_ticks={},alloc_bytes={},alloc_calls={},rss_before_bytes={rss_before},rss_after_bytes={rss_after},begin_reads={},begin_writes={},get_calls={},get_keys={},get_values={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},write_batches={},write_puts={},write_deletes={},write_bytes={},commits={},disk_before_bytes={disk_before},disk_after_bytes={disk_after},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={}",
        parameters.scenario,
        parameters.backend.label(),
        parameters.layout.label(),
        operations,
        wall_us / operations as f64,
        cpu_ticks,
        allocated_bytes,
        allocation_calls,
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
    result
}

#[derive(Clone)]
struct BlobOracle {
    hash: [u8; 32],
    range_start: u64,
    range_end: u64,
    range: Vec<u8>,
}

struct DeterministicBlobReader {
    output: blake3::OutputReader,
    remaining: usize,
    position: usize,
    edit_start: Option<usize>,
}

impl DeterministicBlobReader {
    fn new(bytes: usize, edit_start: Option<usize>) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"ForkTree deterministic semantic blob fixture v2");
        Self {
            output: hasher.finalize_xof(),
            remaining: bytes,
            position: 0,
            edit_start,
        }
    }
}

impl std::io::Read for DeterministicBlobReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let length = buffer.len().min(self.remaining);
        if length == 0 {
            return Ok(0);
        }
        self.output.fill(&mut buffer[..length]);
        if let Some(edit_start) = self.edit_start {
            let read_start = self.position;
            let read_end = read_start + length;
            let edit_end = edit_start + 4096;
            let overlap_start = read_start.max(edit_start);
            let overlap_end = read_end.min(edit_end);
            for absolute in overlap_start..overlap_end {
                let edit_index = absolute - edit_start;
                buffer[absolute - read_start] ^=
                    (edit_index as u8).wrapping_mul(31).wrapping_add(1);
            }
        }
        self.position += length;
        self.remaining -= length;
        Ok(length)
    }
}

const BLOB_PREFETCH_BYTES: usize = 8 * 1024 * 1024;

struct PrefetchedBlobSource {
    logical_bytes: u64,
    next: Option<(Vec<u8>, usize, bool)>,
    finished: bool,
    ready: std::sync::mpsc::Receiver<(Vec<u8>, usize, bool)>,
    recycle: std::sync::mpsc::SyncSender<Vec<u8>>,
}

impl PrefetchedBlobSource {
    fn new(bytes: usize, edit_start: Option<usize>) -> Self {
        let mut source = DeterministicBlobReader::new(bytes, edit_start);
        let mut first = vec![0_u8; BLOB_PREFETCH_BYTES];
        let (first_bytes, first_is_last) = fill_prefetch_window(&mut source, &mut first);
        let (ready_sender, ready) = std::sync::mpsc::sync_channel(1);
        let (recycle, recycled) = std::sync::mpsc::sync_channel(2);
        if !first_is_last {
            let second = vec![0_u8; BLOB_PREFETCH_BYTES];
            std::thread::spawn(move || {
                let mut buffer = second;
                loop {
                    let (buffered, is_last) = fill_prefetch_window(&mut source, &mut buffer);
                    if ready_sender.send((buffer, buffered, is_last)).is_err() || is_last {
                        break;
                    }
                    let Ok(next) = recycled.recv() else {
                        break;
                    };
                    buffer = next;
                }
            });
        }
        Self {
            logical_bytes: bytes as u64,
            next: Some((first, first_bytes, first_is_last)),
            finished: false,
            ready,
            recycle,
        }
    }
}

impl SegmentedByteSource for PrefetchedBlobSource {
    fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }

    fn next_span(&mut self) -> Result<Option<Bytes>, String> {
        if self.finished {
            return Ok(None);
        }
        let (mut buffer, bytes, is_last) = match self.next.take() {
            Some(first) => first,
            None => self.ready.recv().map_err(|_| {
                "ForkTree segmented prefetch producer stopped before completion".to_string()
            })?,
        };
        self.finished = is_last;
        if bytes == 0 {
            return if is_last {
                Ok(None)
            } else {
                Err("ForkTree segmented prefetch yielded an empty non-final span".to_string())
            };
        }
        buffer.truncate(bytes);
        Ok(Some(Bytes::from(buffer)))
    }

    fn recycle_span(&mut self, span: Bytes) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        let mut buffer = span.try_into_mut().map_err(|_| {
            "ForkTree segmented ingest retained a completed source span".to_string()
        })?;
        buffer.resize(BLOB_PREFETCH_BYTES, 0);
        // The producer may already have emitted the empty terminal marker and
        // exited for an exact-window payload. In that case this allocation no
        // longer needs recycling.
        let _ = self.recycle.send(Vec::from(buffer));
        Ok(())
    }
}

fn fill_prefetch_window(source: &mut impl std::io::Read, buffer: &mut [u8]) -> (usize, bool) {
    let mut buffered = 0;
    while buffered < buffer.len() {
        let read = source
            .read(&mut buffer[buffered..])
            .expect("fill deterministic ForkTree prefetch window");
        if read == 0 {
            return (buffered, true);
        }
        buffered += read;
    }
    (buffered, false)
}

const BLOB_PATH: &str = "/forktree/semantic-blob.bin";

fn blob_bytes() -> usize {
    std::env::var("FORKTREE_BLOB_MIB")
        .ok()
        .map(|value| value.parse::<usize>().expect("FORKTREE_BLOB_MIB is usize"))
        .unwrap_or(64)
        .checked_mul(1024 * 1024)
        .expect("ForkTree blob fixture size fits usize")
}

async fn run_blob_setup<S>(
    storage: CountingStorage<S>,
    parameters: Parameters,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) -> VerticalOracle
where
    S: Storage + Clone + Send + Sync + 'static,
{
    assert_eq!(parameters.rows, 1_000, "blob gate uses the 1K tree fixture");
    let blob_bytes = blob_bytes();
    let edit_start = blob_bytes / 2 + 123;
    let range_start = (edit_start - 32 * 1024) as u64;
    let range_end = range_start + 64 * 1024;
    let oracle = blob_oracle(blob_bytes, edit_start, range_start, range_end);
    match parameters.layout {
        Layout::Current => {
            let fixture = prepare_current(storage, parameters).await;
            let base = blob_payload(blob_bytes, None);
            let base_blob: lix::Blob = base.into();
            let affected = measure_phase(
                "blob_ingest_64m",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture
                    .session
                    .upsert_file_content(BLOB_PATH.to_owned(), base_blob),
            )
            .await
            .expect("ingest current-Lix 64 MiB blob");
            assert_eq!(affected, 1);
            let repeated_blob: lix::Blob = blob_payload(blob_bytes, None).into();
            let affected = measure_phase(
                "blob_repeated_ingest",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture
                    .session
                    .upsert_file_content(BLOB_PATH.to_owned(), repeated_blob),
            )
            .await
            .expect("repeat current-Lix blob ingest");
            assert_eq!(affected, 1);
            let base_head = current_head(&fixture.session).await;
            measure_phase(
                "blob_unchanged_branch",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture.session.create_branch(CreateBranchOptions {
                    id: Some(SOURCE_BRANCH_ID.to_owned()),
                    name: "forktree-blob-source".to_owned(),
                    from_commit_id: Some(base_head.clone()),
                }),
            )
            .await
            .expect("branch current-Lix blob root");
            let main_branch = fixture.session.active_branch_id().await.unwrap();
            fixture
                .session
                .switch_branch(SwitchBranchOptions {
                    branch_id: SOURCE_BRANCH_ID.to_owned(),
                })
                .await
                .expect("switch to current-Lix blob source");
            let edited_blob: lix::Blob = blob_payload(blob_bytes, Some(edit_start)).into();
            measure_phase(
                "blob_localized_edit",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture
                    .session
                    .upsert_file_content(BLOB_PATH.to_owned(), edited_blob),
            )
            .await
            .expect("edit current-Lix blob");
            let source_head = current_head(&fixture.session).await;
            fixture
                .session
                .switch_branch(SwitchBranchOptions {
                    branch_id: main_branch,
                })
                .await
                .expect("switch to current-Lix blob target");
            let diff_rows = measure_phase(
                "blob_diff",
                parameters,
                1,
                stats,
                path,
                counters,
                current_blob_diff_count(&fixture.session, &base_head, &source_head),
            )
            .await;
            assert!(diff_rows > 0);
            measure_phase(
                "blob_merge",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture.session.merge_branch(MergeBranchOptions {
                    source_branch_id: SOURCE_BRANCH_ID.to_owned(),
                }),
            )
            .await
            .expect("merge current-Lix blob branch");
            let range = measure_phase(
                "blob_range_read_64k",
                parameters,
                1,
                stats,
                path,
                counters,
                current_blob_read(&fixture.session, Some(oracle.range_start..oracle.range_end)),
            )
            .await;
            assert_eq!(range, oracle.range);
            let full = measure_phase(
                "blob_full_read_64m",
                parameters,
                1,
                stats,
                path,
                counters,
                current_blob_read(&fixture.session, None),
            )
            .await;
            assert_eq!(blake3::hash(&full).as_bytes(), &oracle.hash);
            measure_phase(
                "blob_checkpoint",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture.session.create_checkpoint(),
            )
            .await
            .expect("checkpoint current-Lix blob");
            fixture
                .session
                .close()
                .await
                .expect("close current-Lix blob session");
        }
        Layout::ForkTree => {
            let fixture = prepare_replacement(storage, parameters).await;
            let (_initial_head, ingest) = measure_phase(
                "blob_ingest_64m",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture
                    .tree
                    .ingest_blob("main", PrefetchedBlobSource::new(blob_bytes, None)),
            )
            .await
            .expect("ingest ForkTree 64 MiB blob");
            print_blob_accounting("ingest", ingest);
            let (base_head, repeated) = measure_phase(
                "blob_repeated_ingest",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture
                    .tree
                    .ingest_blob("main", PrefetchedBlobSource::new(blob_bytes, None)),
            )
            .await
            .expect("repeat ForkTree blob ingest");
            assert_eq!(repeated.reused_chunks, repeated.chunks);
            assert_eq!(repeated.emission_batches, 0);
            print_blob_accounting("repeated_ingest", repeated);
            measure_phase(
                "blob_unchanged_branch",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture.tree.create_branch("source", Some(base_head)),
            )
            .await
            .expect("branch ForkTree blob root");
            let (source_head, changed) = measure_phase(
                "blob_localized_edit",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture.tree.ingest_blob(
                    "source",
                    PrefetchedBlobSource::new(blob_bytes, Some(edit_start)),
                ),
            )
            .await
            .expect("edit ForkTree blob");
            assert!(changed.reused_chunks > 0);
            print_blob_accounting("localized_edit", changed);
            let diff = measure_phase(
                "blob_diff",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture.tree.diff_blob_commits(base_head, source_head),
            )
            .await
            .expect("diff ForkTree blobs");
            assert!(diff.shared_chunks > 0);
            assert!(diff.changed_chunks > 0);
            println!(
                "forktree_blob_diff,before_chunks={},after_chunks={},shared_chunks={},changed_chunks={}",
                diff.before_chunks, diff.after_chunks, diff.shared_chunks, diff.changed_chunks
            );
            let (merged_head, merge) = measure_phase(
                "blob_merge",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture
                    .tree
                    .merge_blob_branches("main", "source", base_head),
            )
            .await
            .expect("merge ForkTree blob branch");
            assert_eq!(merge.logical_bytes, 0);
            let range = measure_phase(
                "blob_range_read_64k",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture
                    .tree
                    .read_blob_range("main", oracle.range_start, oracle.range_end),
            )
            .await
            .expect("range-read ForkTree blob")
            .materialize();
            assert_eq!(range, oracle.range);
            let full = measure_phase(
                "blob_full_read_64m",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture.tree.read_blob("main"),
            )
            .await
            .expect("read ForkTree blob");
            assert_eq!(full.logical_bytes(), blob_bytes as u64);
            assert_eq!(full.authenticated_hash().as_bytes(), &oracle.hash);
            measure_phase(
                "blob_checkpoint",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture.tree.create_checkpoint("blob", merged_head),
            )
            .await
            .expect("checkpoint ForkTree blob");
            measure_phase(
                "blob_retention_boundary",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture.tree.compact_history("main"),
            )
            .await
            .expect("compact ForkTree blob history");
            let retained = fixture
                .tree
                .reclaim_unreachable()
                .await
                .expect("retain checkpointed ForkTree blob history");
            println!(
                "forktree_vertical_gc,phase=blob_retained,roots={},reachable={},scanned={},reclaimed={},reclaimed_bytes={},pages={},peak_frontier={}",
                retained.roots,
                retained.reachable_objects,
                retained.scanned_objects,
                retained.reclaimed_objects,
                retained.reclaimed_bytes,
                retained.pages,
                retained.peak_frontier
            );
        }
    }
    VerticalOracle::Blob(oracle)
}

fn print_blob_accounting(phase: &str, accounting: super::model::BlobAccounting) {
    println!(
        "forktree_blob_accounting,phase={phase},chunks={},reused_chunks={},locality_hits={},locality_misses={},object_writes={},object_bytes={},logical_bytes={},chunking_us={},source_read_us={},object_hash_us={},object_encode_us={},dedup_read_us={},emission_us={},publication_us={},emission_batches={},peak_buffer_bytes={}",
        accounting.chunks,
        accounting.reused_chunks,
        accounting.locality_hits,
        accounting.locality_misses,
        accounting.object_writes,
        accounting.object_bytes,
        accounting.logical_bytes,
        accounting.chunking_us,
        accounting.source_read_us,
        accounting.object_hash_us,
        accounting.object_encode_us,
        accounting.dedup_read_us,
        accounting.emission_us,
        accounting.publication_us,
        accounting.emission_batches,
        accounting.peak_buffer_bytes
    );
}

async fn run_blob_profile_setup<S>(
    storage: CountingStorage<S>,
    parameters: Parameters,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) -> VerticalOracle
where
    S: Storage + Clone + Send + Sync + 'static,
{
    assert_eq!(
        parameters.rows, 1_000,
        "blob profile uses the 1K tree fixture"
    );
    let bytes = blob_bytes();
    match parameters.layout {
        Layout::Current => {
            let fixture = prepare_current(storage, parameters).await;
            let payload = blob_payload(bytes, None);
            let affected = measure_phase(
                "profile_ingest",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture
                    .session
                    .upsert_file_content(BLOB_PATH.to_owned(), lix::Blob::from(payload)),
            )
            .await
            .expect("profile current-Lix blob ingest");
            assert_eq!(affected, 1);
            fixture
                .session
                .close()
                .await
                .expect("close profile session");
        }
        Layout::ForkTree => {
            let fixture = prepare_replacement(storage, parameters).await;
            let (_, accounting) = measure_phase(
                "profile_ingest",
                parameters,
                1,
                stats,
                path,
                counters,
                fixture
                    .tree
                    .ingest_blob("main", PrefetchedBlobSource::new(bytes, None)),
            )
            .await
            .expect("profile ForkTree blob ingest");
            println!(
                "forktree_blob_profile_ingest,bytes={},chunks={},locality_hits={},locality_misses={},chunking_us={},source_read_us={},object_hash_us={},object_encode_us={},dedup_read_us={},emission_us={},publication_us={},emission_batches={},peak_buffer_bytes={},object_writes={},object_bytes={}",
                bytes,
                accounting.chunks,
                accounting.locality_hits,
                accounting.locality_misses,
                accounting.chunking_us,
                accounting.source_read_us,
                accounting.object_hash_us,
                accounting.object_encode_us,
                accounting.dedup_read_us,
                accounting.emission_us,
                accounting.publication_us,
                accounting.emission_batches,
                accounting.peak_buffer_bytes,
                accounting.object_writes,
                accounting.object_bytes
            );
        }
    }
    VerticalOracle::Profile
}

async fn run_blob_reopen<S>(
    storage: CountingStorage<S>,
    parameters: Parameters,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    oracle: BlobOracle,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    match parameters.layout {
        Layout::Current => {
            let adapter_storage = storage.clone();
            let session = measure_phase(
                "blob_cold_reopen",
                parameters,
                1,
                stats,
                path,
                counters,
                async {
                    let engine = Engine::new(storage)
                        .await
                        .expect("reopen current-Lix blob engine");
                    let session = engine
                        .open_workspace_session()
                        .await
                        .expect("reopen current-Lix blob session");
                    let range =
                        current_blob_read(&session, Some(oracle.range_start..oracle.range_end))
                            .await;
                    assert_eq!(range, oracle.range);
                    let full = current_blob_read(&session, None).await;
                    assert_eq!(blake3::hash(&full).as_bytes(), &oracle.hash);
                    session
                },
            )
            .await;
            session
                .execute(
                    "DELETE FROM lix_branch WHERE id = $1",
                    &[Value::Text(SOURCE_BRANCH_ID.to_owned())],
                )
                .await
                .expect("release current-Lix blob source branch");
            session
                .create_checkpoint()
                .await
                .expect("checkpoint current-Lix blob after release");
            session
                .close()
                .await
                .expect("close reopened current-Lix blob session");
            let plan = lix::storage_bench::plan_repository_gc_for_bench(&StorageAdapter::new(
                adapter_storage,
            ))
            .await
            .expect("plan current-Lix blob reclamation");
            println!(
                "forktree_vertical_gc,phase=current_blob_release_plan,live_commits={},swept_commits={},swept_payloads={},staged_deletes={},staged_written_bytes={},total_us={}",
                plan.live_commits,
                plan.swept_commits,
                plan.swept_payloads,
                plan.staged_deletes,
                plan.staged_written_bytes,
                plan.total_us
            );
        }
        Layout::ForkTree => {
            let tree = ForkTree::new(storage);
            measure_phase(
                "blob_cold_reopen",
                parameters,
                1,
                stats,
                path,
                counters,
                async {
                    assert_eq!(
                        tree.read_blob_range("main", oracle.range_start, oracle.range_end)
                            .await
                            .expect("cold ForkTree blob range")
                            .materialize(),
                        oracle.range
                    );
                    let full = tree.read_blob("main").await.expect("cold ForkTree blob");
                    assert_eq!(full.authenticated_hash().as_bytes(), &oracle.hash);
                    let recovery = tree
                        .checkpoint_head("blob")
                        .await
                        .expect("load ForkTree blob recovery root");
                    tree.create_branch("blob-recovered", Some(recovery))
                        .await
                        .expect("recover ForkTree blob branch");
                    assert_eq!(
                        tree.read_blob_range(
                            "blob-recovered",
                            oracle.range_start,
                            oracle.range_end
                        )
                        .await
                        .expect("read recovered ForkTree blob")
                        .materialize(),
                        oracle.range
                    );
                },
            )
            .await;
            tree.delete_branch("blob-recovered")
                .await
                .expect("release recovered ForkTree blob");
            tree.delete_branch("source")
                .await
                .expect("release ForkTree blob source");
            tree.delete_checkpoint("blob")
                .await
                .expect("release ForkTree blob checkpoint");
            let reclaimed = measure_phase(
                "blob_final_reference_reclamation",
                parameters,
                1,
                stats,
                path,
                counters,
                tree.reclaim_unreachable(),
            )
            .await
            .expect("reclaim ForkTree blob final reference");
            assert!(reclaimed.reclaimed_bytes > 0);
            assert_eq!(
                tree.read_blob_range("main", oracle.range_start, oracle.range_end)
                    .await
                    .expect("read ForkTree blob after reclamation")
                    .materialize(),
                oracle.range
            );
            println!(
                "forktree_vertical_gc,phase=blob_released,roots={},reachable={},scanned={},reclaimed={},reclaimed_bytes={},pages={},peak_frontier={}",
                reclaimed.roots,
                reclaimed.reachable_objects,
                reclaimed.scanned_objects,
                reclaimed.reclaimed_objects,
                reclaimed.reclaimed_bytes,
                reclaimed.pages,
                reclaimed.peak_frontier
            );
            tree.verify_publication_gc_races()
                .await
                .expect("verify ForkTree publication/GC epoch orderings");
            tree.verify_blob_corruption_fail_closed("main")
                .await
                .expect("verify ForkTree blob corruption fails closed");
            println!(
                "forktree_vertical_oracle,scenario=Blob,backend={},publication_gc_orderings=pass,crash_orphan_retry=pass,corruption=fail_closed",
                parameters.backend.label()
            );
        }
    }
}

fn blob_payload(bytes: usize, edit_start: Option<usize>) -> Vec<u8> {
    let mut payload = vec![0_u8; bytes];
    std::io::Read::read_exact(
        &mut DeterministicBlobReader::new(bytes, edit_start),
        &mut payload,
    )
    .expect("fill deterministic ForkTree blob fixture");
    payload
}

fn blob_oracle(bytes: usize, edit_start: usize, range_start: u64, range_end: u64) -> BlobOracle {
    let mut source = DeterministicBlobReader::new(bytes, Some(edit_start));
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut hasher = blake3::Hasher::new();
    let mut range = Vec::with_capacity((range_end - range_start) as usize);
    let mut position = 0_u64;
    loop {
        let read = std::io::Read::read(&mut source, &mut buffer)
            .expect("read deterministic ForkTree blob oracle");
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        let read_end = position + read as u64;
        let overlap_start = position.max(range_start);
        let overlap_end = read_end.min(range_end);
        if overlap_start < overlap_end {
            range.extend_from_slice(
                &buffer[(overlap_start - position) as usize..(overlap_end - position) as usize],
            );
        }
        position = read_end;
    }
    assert_eq!(position, bytes as u64);
    assert_eq!(range.len(), (range_end - range_start) as usize);
    BlobOracle {
        hash: *hasher.finalize().as_bytes(),
        range_start,
        range_end,
        range,
    }
}

async fn current_blob_read<S>(
    session: &SessionContext<CountingStorage<S>>,
    range: Option<std::ops::Range<u64>>,
) -> Vec<u8>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .read_file_content(BLOB_PATH.to_owned(), range)
        .await
        .expect("read current-Lix blob")
        .expect("current-Lix blob exists")
        .content()
        .as_ref()
        .to_vec()
}

async fn current_blob_diff_count<S>(
    session: &SessionContext<CountingStorage<S>>,
    before: &str,
    after: &str,
) -> i64
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(
            "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) WHERE schema_key IN ('lix_file_descriptor', 'lix_binary_blob_ref')",
            &[Value::Text(before.to_owned()), Value::Text(after.to_owned())],
        )
        .await
        .expect("diff current-Lix blob commits")
        .rows()[0]
        .get::<i64>("entries")
        .expect("current-Lix blob diff count is i64")
}
