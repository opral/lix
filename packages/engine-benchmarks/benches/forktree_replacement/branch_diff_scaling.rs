use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::{diff_tracked_commits_for_bench, plan_repository_gc_for_bench};
use lix::{
    CreateBranchOptions, MergeBranchOptions, MergeBranchOutcome, MergeBranchPreviewOptions,
    PreparedDmlParameterBatch, Value,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

use super::model::{
    ApplyAccounting, DiffAccounting, ForkTree, MergeOutcome, Mutation, ObjectId, ObjectLayoutStats,
    RelationalValue, SelectorConflictAccounting, SharedObjectAccounting,
};
use super::{
    Backend, CountingStorage, IoStats, begin_allocation_profile, directory_bytes,
    end_allocation_profile, physical_delta, process_cpu_nanos, process_cpu_ticks,
    process_resident_bytes, settle_rocksdb_compaction, take_stats,
};

const RANGE_ROWS: usize = 100;
const CHECKPOINT_GC_ROTATIONS: usize = 67;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Layout {
    Current,
    ForkTree,
}

impl Layout {
    fn parse(value: &str) -> Self {
        match value {
            "current" => Self::Current,
            "forktree" => Self::ForkTree,
            other => panic!("unknown branch scaling layout '{other}'"),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Current => "current_main_f77",
            Self::ForkTree => "forktree_bc823",
        }
    }
}

#[derive(Clone, Copy)]
struct Parameters {
    backend: Backend,
    layout: Layout,
    rows: usize,
    branches: usize,
    edit_percent: usize,
    edits: usize,
    rows_per_edit: usize,
}

impl Parameters {
    fn from_env() -> Self {
        let args = std::env::args().collect::<Vec<_>>();
        let backend = Backend::parse(args.get(2).map(String::as_str).unwrap_or("rocksdb"));
        let layout = Layout::parse(args.get(3).map(String::as_str).unwrap_or("forktree"));
        let rows = env_usize("FORKTREE_BRANCH_ROWS");
        let branches = env_usize("FORKTREE_BRANCHES");
        let edit_percent = env_usize("FORKTREE_BRANCH_EDIT_PERCENT");
        assert!(matches!(rows, 10_000 | 50_000));
        assert!(matches!(branches, 1 | 100 | 1_000));
        assert!(matches!(edit_percent, 1 | 10 | 100));
        let edits = branches.saturating_mul(edit_percent).div_ceil(100).max(1);
        let rows_per_edit = rows.div_ceil(100);
        Self {
            backend,
            layout,
            rows,
            branches,
            edit_percent,
            edits,
            rows_per_edit,
        }
    }
}

pub(super) async fn run() {
    let parameters = Parameters::from_env();
    println!(
        "branch_diff_model,layout={},current_big_o=branch_create_O(1)_diff_O(relevant_history_plus_output)_merge_O(diff_plus_output),forktree_big_o=branch_create_O(1)_diff_O(changed_paths_plus_output)_merge_O(two_diffs_plus_output),total_fanout=O(branches),perfect_elimination_ceiling=all_per_branch_row_or_payload_copy,rows={},branches={},edit_percent={},edited_branches={},rows_per_edit={}",
        parameters.layout.label(),
        parameters.rows,
        parameters.branches,
        parameters.edit_percent,
        parameters.edits,
        parameters.rows_per_edit,
    );
    match (parameters.backend, parameters.layout) {
        (Backend::RocksDb, Layout::Current) => run_current_rocks(parameters).await,
        (Backend::SlateDb, Layout::Current) => run_current_slate(parameters).await,
        (Backend::RocksDb, Layout::ForkTree) => run_forktree_rocks(parameters).await,
        (Backend::SlateDb, Layout::ForkTree) => run_forktree_slate(parameters).await,
    }
}

#[derive(Clone)]
struct CurrentOracle {
    base_commit: String,
    branches: Vec<String>,
    edited_heads: Vec<String>,
}

#[derive(Clone)]
struct ForkTreeOracle {
    base: ObjectId,
    branches: Vec<String>,
    edited_heads: Vec<ObjectId>,
    layout_after_seed: ObjectLayoutStats,
    layout_after_branches: ObjectLayoutStats,
    layout_after_edits: ObjectLayoutStats,
    shared: SharedObjectAccounting,
    edit_accounting: ApplyAccounting,
}

async fn run_current_rocks(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create current branch-scale RocksDB directory");
    let oracle = {
        let database = RocksDB::open(directory.path()).expect("open current branch-scale RocksDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let oracle = current_setup(storage, &stats, directory.path(), None, parameters).await;
        database
            .flush()
            .expect("flush current branch-scale RocksDB setup");
        oracle
    };
    {
        let database =
            RocksDB::open(directory.path()).expect("reopen current branch-scale RocksDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        current_reopen(storage, &stats, directory.path(), None, parameters, oracle).await;
        database
            .flush()
            .expect("flush current branch-scale RocksDB final");
    }
    let immediate = directory_bytes(directory.path());
    let settled = settle_rocksdb_compaction(directory.path());
    println!(
        "branch_diff_settled,backend=rocksdb,layout={},rows={},branches={},edit_percent={},immediate_disk_bytes={immediate},settled_disk_bytes={settled}",
        parameters.layout.label(),
        parameters.rows,
        parameters.branches,
        parameters.edit_percent
    );
}

async fn run_current_slate(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create current branch-scale SlateDB directory");
    let oracle = {
        let counters = SlateDBIoCounters::default();
        let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
            .expect("open current branch-scale SlateDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let oracle = current_setup(
            storage,
            &stats,
            directory.path(),
            Some(&counters),
            parameters,
        )
        .await;
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush current branch-scale SlateDB setup");
        oracle
    };
    {
        let counters = SlateDBIoCounters::default();
        let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
            .expect("reopen current branch-scale SlateDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        current_reopen(
            storage,
            &stats,
            directory.path(),
            Some(&counters),
            parameters,
            oracle,
        )
        .await;
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush current branch-scale SlateDB final");
    }
    println!(
        "branch_diff_settled,backend=slatedb,layout={},rows={},branches={},edit_percent={},immediate_disk_bytes={},settled_disk_bytes=not_applicable",
        parameters.layout.label(),
        parameters.rows,
        parameters.branches,
        parameters.edit_percent,
        directory_bytes(directory.path())
    );
}

async fn run_forktree_rocks(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create ForkTree branch-scale RocksDB directory");
    let oracle = {
        let database = RocksDB::open(directory.path()).expect("open ForkTree branch-scale RocksDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let oracle = forktree_setup(storage, &stats, directory.path(), None, parameters).await;
        database
            .flush()
            .expect("flush ForkTree branch-scale RocksDB setup");
        oracle
    };
    {
        let database =
            RocksDB::open(directory.path()).expect("reopen ForkTree branch-scale RocksDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        forktree_reopen(storage, &stats, directory.path(), None, parameters, oracle).await;
        database
            .flush()
            .expect("flush ForkTree branch-scale RocksDB final");
    }
    let immediate = directory_bytes(directory.path());
    let settled = settle_rocksdb_compaction(directory.path());
    println!(
        "branch_diff_settled,backend=rocksdb,layout={},rows={},branches={},edit_percent={},immediate_disk_bytes={immediate},settled_disk_bytes={settled}",
        parameters.layout.label(),
        parameters.rows,
        parameters.branches,
        parameters.edit_percent
    );
}

async fn run_forktree_slate(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create ForkTree branch-scale SlateDB directory");
    let oracle = {
        let counters = SlateDBIoCounters::default();
        let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
            .expect("open ForkTree branch-scale SlateDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let oracle = forktree_setup(
            storage,
            &stats,
            directory.path(),
            Some(&counters),
            parameters,
        )
        .await;
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush ForkTree branch-scale SlateDB setup");
        oracle
    };
    {
        let counters = SlateDBIoCounters::default();
        let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
            .expect("reopen ForkTree branch-scale SlateDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        forktree_reopen(
            storage,
            &stats,
            directory.path(),
            Some(&counters),
            parameters,
            oracle,
        )
        .await;
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush ForkTree branch-scale SlateDB final");
    }
    println!(
        "branch_diff_settled,backend=slatedb,layout={},rows={},branches={},edit_percent={},immediate_disk_bytes={},settled_disk_bytes=not_applicable",
        parameters.layout.label(),
        parameters.rows,
        parameters.branches,
        parameters.edit_percent,
        directory_bytes(directory.path())
    );
}

async fn current_setup<S>(
    storage: CountingStorage<S>,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    parameters: Parameters,
) -> CurrentOracle
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Engine::initialize(storage.clone())
        .await
        .expect("initialize current branch scale");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open current branch scale");
    let main = engine
        .open_workspace_session()
        .await
        .expect("open current branch-scale main");
    register_schema(&main).await;
    seed_current_rows(&main, parameters.rows).await;
    let _ = take_stats(stats);

    let base = measured(
        "base_checkpoint",
        stats,
        path,
        counters,
        parameters,
        main.create_checkpoint(),
    )
    .await
    .expect("checkpoint current branch-scale base")
    .commit_id;

    let branches = measured(
        "create_branches",
        stats,
        path,
        counters,
        parameters,
        async {
            let mut branches = Vec::with_capacity(parameters.branches);
            for index in 0..parameters.branches {
                let id = current_branch_id(index);
                let receipt = main
                    .create_branch(CreateBranchOptions {
                        id: Some(id.clone()),
                        name: format!("branch-scale-{index:04}"),
                        from_commit_id: Some(base.clone()),
                    })
                    .await?;
                if receipt.commit_id != base {
                    return Err(lix::LixError::unknown(
                        "current branch did not bind base commit",
                    ));
                }
                branches.push(id);
            }
            Ok::<_, lix::LixError>(branches)
        },
    )
    .await
    .expect("create current fanout branches");

    measured(
        "hot_point_range_reads",
        stats,
        path,
        counters,
        parameters,
        current_read_branches(&engine, &branches, parameters.rows),
    )
    .await;

    let edited_heads = measured("edit_branches", stats, path, counters, parameters, async {
        let mut heads = Vec::with_capacity(parameters.edits);
        for (index, branch) in branches.iter().take(parameters.edits).enumerate() {
            let session = engine.open_session(branch).await?;
            update_current_rows(&session, parameters, index).await?;
            heads.push(current_head(&session).await?);
        }
        Ok::<_, lix::LixError>(heads)
    })
    .await
    .expect("edit current branch cohort");

    println!(
        "branch_diff_current_sharing,backend={},rows={},branches={},edit_percent={},branch_payload_copies=0,shared_object_bytes=not_exposed,base_commit={base}",
        parameters.backend.label(),
        parameters.rows,
        parameters.branches,
        parameters.edit_percent
    );
    CurrentOracle {
        base_commit: base,
        branches,
        edited_heads,
    }
}

async fn current_reopen<S>(
    storage: CountingStorage<S>,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    parameters: Parameters,
    oracle: CurrentOracle,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let engine = Engine::new(storage.clone())
        .await
        .expect("cold reopen current branch scale");
    let main = engine
        .open_workspace_session()
        .await
        .expect("cold reopen current main");
    measured(
        "cold_point_range_reads",
        stats,
        path,
        counters,
        parameters,
        current_read_branches(&engine, &oracle.branches, parameters.rows),
    )
    .await;

    let adapter = StorageAdapter::new(storage.clone());
    let (entries, durable_left, durable_right) =
        measured("cold_diff", stats, path, counters, parameters, async {
            let mut entries = 0usize;
            let mut durable_left = 0usize;
            let mut durable_right = 0usize;
            for head in &oracle.edited_heads {
                let diff =
                    diff_tracked_commits_for_bench(&adapter, &oracle.base_commit, head).await?;
                entries += diff.entries;
                durable_left += usize::from(diff.left_has_durable_root);
                durable_right += usize::from(diff.right_has_durable_root);
            }
            Ok::<_, lix::LixError>((entries, durable_left, durable_right))
        })
        .await
        .expect("cold diff current branches");
    assert_eq!(entries, parameters.edits * parameters.rows_per_edit);
    println!(
        "branch_diff_pruning,backend={},layout={},rows={},branches={},edit_percent={},diffs={},changes={},hash_pruned_nodes=not_exposed,durable_left={},durable_right={}",
        parameters.backend.label(),
        parameters.layout.label(),
        parameters.rows,
        parameters.branches,
        parameters.edit_percent,
        parameters.edits,
        entries,
        durable_left,
        durable_right,
    );

    let merge_targets =
        current_merge_selected(&engine, &main, &oracle, stats, path, counters, parameters).await;
    let conflicts = measured(
        "publication_conflicts",
        stats,
        path,
        counters,
        parameters,
        current_publication_conflicts(&engine, &main, &oracle.base_commit),
    )
    .await;
    println!(
        "branch_diff_conflicts,backend={},layout={},unrelated_global_epoch_false_conflicts={},unrelated_writer_successes={},same_branch_stale_rejections={}",
        parameters.backend.label(),
        parameters.layout.label(),
        conflicts.unrelated_global_epoch_conflicts,
        conflicts.unrelated_writer_success_potential,
        conflicts.same_selector_stale_rejections,
    );

    measured(
        "delete_unedited_cohort",
        stats,
        path,
        counters,
        parameters,
        async {
            for branch in oracle.branches.iter().skip(parameters.edits) {
                delete_current_branch(&main, branch).await?;
            }
            Ok::<(), lix::LixError>(())
        },
    )
    .await
    .expect("delete current unedited cohort");
    let retained = engine
        .open_session(&oracle.branches[0])
        .await
        .expect("open retained edited current branch");
    assert_current_value(&retained, selected_index(parameters.rows, 0), 0).await;

    measured(
        "delete_edited_and_merge_cohorts",
        stats,
        path,
        counters,
        parameters,
        async {
            for branch in oracle.branches.iter().take(parameters.edits) {
                delete_current_branch(&main, branch).await?;
            }
            for branch in &merge_targets {
                delete_current_branch(&main, branch).await?;
            }
            Ok::<(), lix::LixError>(())
        },
    )
    .await
    .expect("delete current edited and merge cohorts");
    drop(retained);

    let gc_plan = measured(
        "checkpoint_gc_final_reclaim",
        stats,
        path,
        counters,
        parameters,
        async {
            for _ in 0..CHECKPOINT_GC_ROTATIONS {
                main.create_checkpoint().await?;
            }
            tokio::task::yield_now().await;
            main.create_checkpoint().await?;
            tokio::task::yield_now().await;
            plan_repository_gc_for_bench(&adapter).await
        },
    )
    .await
    .expect("run current checkpoint GC final reclaim");
    println!(
        "branch_diff_gc,backend={},layout={},live_commits={},remaining_swept_commits={},remaining_swept_changes={},remaining_staged_deletes={},gc_plan_us={}",
        parameters.backend.label(),
        parameters.layout.label(),
        gc_plan.live_commits,
        gc_plan.swept_commits,
        gc_plan.swept_standalone_changes,
        gc_plan.staged_deletes,
        gc_plan.total_us,
    );
}

async fn forktree_setup<S>(
    storage: CountingStorage<S>,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    parameters: Parameters,
) -> ForkTreeOracle
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let tree = ForkTree::new(storage);
    let base = tree
        .initialize(&initial_rows(parameters.rows))
        .await
        .expect("initialize ForkTree branch scale");
    tree.create_checkpoint("base", base)
        .await
        .expect("pin ForkTree base");
    let layout_after_seed = tree
        .object_layout_stats()
        .await
        .expect("inventory ForkTree seed");
    let _ = take_stats(stats);

    let branches = measured(
        "create_branches",
        stats,
        path,
        counters,
        parameters,
        async {
            let mut branches = Vec::with_capacity(parameters.branches);
            for index in 0..parameters.branches {
                let branch = forktree_branch(index);
                tree.create_branch(&branch, Some(base)).await?;
                branches.push(branch);
            }
            Ok::<_, String>(branches)
        },
    )
    .await
    .expect("create ForkTree fanout branches");
    let layout_after_branches = tree
        .object_layout_stats()
        .await
        .expect("inventory ForkTree branches");
    assert_eq!(layout_after_seed.objects, layout_after_branches.objects);
    assert_eq!(
        layout_after_seed.object_value_bytes,
        layout_after_branches.object_value_bytes
    );

    measured(
        "hot_point_range_reads",
        stats,
        path,
        counters,
        parameters,
        forktree_read_branches(&tree, &branches, parameters.rows),
    )
    .await;

    let mut total_accounting = ApplyAccounting::default();
    let edited_heads = measured("edit_branches", stats, path, counters, parameters, async {
        let mut heads = Vec::with_capacity(parameters.edits);
        for (index, branch) in branches.iter().take(parameters.edits).enumerate() {
            let (head, accounting) = tree
                .apply_sorted_mutations_on(branch, &branch_mutations(parameters, index))
                .await?;
            add_apply(&mut total_accounting, accounting);
            heads.push(head);
        }
        Ok::<_, String>(heads)
    })
    .await
    .expect("edit ForkTree branch cohort");
    let layout_after_edits = tree
        .object_layout_stats()
        .await
        .expect("inventory ForkTree edits");
    let shared = tree
        .shared_object_inventory(base, edited_heads[0])
        .await
        .expect("inventory ForkTree shared closure");
    println!(
        "branch_diff_forktree_sharing,backend={},rows={},branches={},edit_percent={},objects_after_seed={},objects_after_branches={},objects_after_edits={},bytes_after_seed={},bytes_after_branches={},bytes_after_edits={},shared_objects={},shared_bytes={},base_closure_objects={},edited_closure_objects={},reused_objects={},new_object_writes={},new_object_bytes={},logical_write_bytes={}",
        parameters.backend.label(),
        parameters.rows,
        parameters.branches,
        parameters.edit_percent,
        layout_after_seed.objects,
        layout_after_branches.objects,
        layout_after_edits.objects,
        layout_after_seed.object_value_bytes,
        layout_after_branches.object_value_bytes,
        layout_after_edits.object_value_bytes,
        shared.shared_objects,
        shared.shared_bytes,
        shared.left_objects,
        shared.right_objects,
        total_accounting.reused_objects,
        total_accounting.object_writes,
        total_accounting.object_bytes,
        total_accounting.logical_bytes,
    );
    ForkTreeOracle {
        base,
        branches,
        edited_heads,
        layout_after_seed,
        layout_after_branches,
        layout_after_edits,
        shared,
        edit_accounting: total_accounting,
    }
}

async fn forktree_reopen<S>(
    storage: CountingStorage<S>,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    parameters: Parameters,
    oracle: ForkTreeOracle,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let tree = ForkTree::new(storage);
    measured(
        "cold_point_range_reads",
        stats,
        path,
        counters,
        parameters,
        forktree_read_branches(&tree, &oracle.branches, parameters.rows),
    )
    .await;
    let (changes, accounting) = measured("cold_diff", stats, path, counters, parameters, async {
        let mut changes = 0usize;
        let mut accounting = DiffAccounting::default();
        for head in &oracle.edited_heads {
            let (diff, one) = tree.diff_commits_profiled(oracle.base, *head).await?;
            changes += diff.len();
            accounting.changes += one.changes;
            accounting.hash_pruned_nodes += one.hash_pruned_nodes;
            accounting.decoded_nodes += one.decoded_nodes;
        }
        Ok::<_, String>((changes, accounting))
    })
    .await
    .expect("cold diff ForkTree branches");
    assert_eq!(changes, parameters.edits * parameters.rows_per_edit);
    println!(
        "branch_diff_pruning,backend={},layout={},rows={},branches={},edit_percent={},diffs={},changes={},hash_pruned_nodes={},decoded_nodes={}",
        parameters.backend.label(),
        parameters.layout.label(),
        parameters.rows,
        parameters.branches,
        parameters.edit_percent,
        parameters.edits,
        changes,
        accounting.hash_pruned_nodes,
        accounting.decoded_nodes,
    );

    let merge_targets =
        forktree_merge_selected(&tree, &oracle, stats, path, counters, parameters).await;
    let conflicts = measured(
        "publication_conflicts",
        stats,
        path,
        counters,
        parameters,
        tree.verify_selector_stale_write_and_delete_gc_races(),
    )
    .await
    .expect("verify ForkTree publication conflicts");
    println!(
        "branch_diff_conflicts,backend={},layout={},unrelated_global_epoch_false_conflicts={},unrelated_writer_success_potential={},same_branch_stale_rejections={}",
        parameters.backend.label(),
        parameters.layout.label(),
        conflicts.unrelated_global_epoch_conflicts,
        conflicts.unrelated_writer_success_potential,
        conflicts.same_selector_stale_rejections,
    );

    measured(
        "delete_unedited_cohort",
        stats,
        path,
        counters,
        parameters,
        async {
            for branch in oracle.branches.iter().skip(parameters.edits) {
                tree.delete_branch(branch).await?;
            }
            Ok::<(), String>(())
        },
    )
    .await
    .expect("delete ForkTree unedited cohort");
    let unedited_gc = measured(
        "gc_after_unedited_delete",
        stats,
        path,
        counters,
        parameters,
        tree.reclaim_unreachable(),
    )
    .await
    .expect("GC ForkTree unedited cohort");
    assert_eq!(unedited_gc.reclaimed_objects, 0);
    assert_eq!(
        tree.read_relational_point(
            &oracle.branches[0],
            &row_key(selected_index(parameters.rows, 0))
        )
        .await
        .expect("read retained ForkTree edit"),
        Some(branch_value(0))
    );

    measured(
        "delete_edited_and_merge_cohorts",
        stats,
        path,
        counters,
        parameters,
        async {
            for branch in oracle.branches.iter().take(parameters.edits) {
                tree.delete_branch(branch).await?;
            }
            for branch in &merge_targets {
                tree.delete_branch(branch).await?;
            }
            Ok::<(), String>(())
        },
    )
    .await
    .expect("delete ForkTree edited and merge cohorts");
    let final_gc = measured(
        "final_dead_branch_reclaim",
        stats,
        path,
        counters,
        parameters,
        tree.reclaim_unreachable(),
    )
    .await
    .expect("final ForkTree dead branch reclaim");
    let final_layout = tree
        .object_layout_stats()
        .await
        .expect("inventory final ForkTree branch scale");
    assert_eq!(final_layout.unreachable_objects, 0);
    println!(
        "branch_diff_gc,backend={},layout={},unedited_reclaimed={},final_reclaimed={},final_reclaimed_bytes={},final_objects={},final_object_bytes={},unreachable_objects={},seed_objects={},branch_objects={},edit_objects={},shared_bytes={},edit_object_bytes={}",
        parameters.backend.label(),
        parameters.layout.label(),
        unedited_gc.reclaimed_objects,
        final_gc.reclaimed_objects,
        final_gc.reclaimed_bytes,
        final_layout.objects,
        final_layout.object_value_bytes,
        final_layout.unreachable_objects,
        oracle.layout_after_seed.objects,
        oracle.layout_after_branches.objects,
        oracle.layout_after_edits.objects,
        oracle.shared.shared_bytes,
        oracle.edit_accounting.object_bytes,
    );
}

async fn current_merge_selected<S>(
    engine: &Engine<CountingStorage<S>>,
    main: &SessionContext<CountingStorage<S>>,
    oracle: &CurrentOracle,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    parameters: Parameters,
) -> Vec<String>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let count = parameters.edits.min(3);
    let targets = measured(
        "merge_prep_publish",
        stats,
        path,
        counters,
        parameters,
        async {
            let mut targets = Vec::with_capacity(count);
            for index in 0..count {
                let id = current_merge_target_id(index);
                main.create_branch(CreateBranchOptions {
                    id: Some(id.clone()),
                    name: format!("merge-target-{index}"),
                    from_commit_id: Some(oracle.base_commit.clone()),
                })
                .await?;
                let target = engine.open_session(&id).await?;
                target
                    .execute(
                        "INSERT INTO forktree_rel_row (id, value) VALUES ($1, $2)",
                        &[
                            Value::Text(format!("merge-only-{index:04}")),
                            Value::Text(format!("target-{index}")),
                        ],
                    )
                    .await?;
                let preview = target
                    .merge_branch_preview(MergeBranchPreviewOptions {
                        source_branch_id: oracle.branches[index].clone(),
                    })
                    .await?;
                if preview.change_stats.total != parameters.rows_per_edit
                    || !preview.conflicts.is_empty()
                {
                    return Err(lix::LixError::unknown("current merge preview mismatch"));
                }
                let receipt = target
                    .merge_branch(MergeBranchOptions {
                        source_branch_id: oracle.branches[index].clone(),
                    })
                    .await?;
                if receipt.outcome != MergeBranchOutcome::MergeCommitted
                    || receipt.change_stats.total != parameters.rows_per_edit
                {
                    return Err(lix::LixError::unknown("current merge publication mismatch"));
                }
                targets.push(id);
            }
            Ok::<_, lix::LixError>(targets)
        },
    )
    .await
    .expect("merge selected current branches");
    targets
}

async fn forktree_merge_selected<S>(
    tree: &ForkTree<CountingStorage<S>>,
    oracle: &ForkTreeOracle,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    parameters: Parameters,
) -> Vec<String>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let count = parameters.edits.min(3);
    measured(
        "merge_prep_publish",
        stats,
        path,
        counters,
        parameters,
        async {
            let mut targets = Vec::with_capacity(count);
            for index in 0..count {
                let target = format!("merge-target-{index:04}");
                tree.create_branch(&target, Some(oracle.base)).await?;
                tree.apply_sorted_mutations_on(
                    &target,
                    &[Mutation::Insert {
                        key: format!("merge-only-{index:04}").into_bytes(),
                        value: RelationalValue::Bytes(format!("target-{index}").into_bytes()),
                    }],
                )
                .await?;
                match tree
                    .merge_branches_three_way(&target, &oracle.branches[index], oracle.base)
                    .await?
                {
                    MergeOutcome::Merged { accounting, .. } => {
                        if accounting.logical_bytes == 0 {
                            return Err("ForkTree merge published no logical changes".to_string());
                        }
                    }
                    MergeOutcome::Conflicts(conflicts) => {
                        return Err(format!("ForkTree disjoint merge conflicts: {conflicts:?}"));
                    }
                }
                targets.push(target);
            }
            Ok::<_, String>(targets)
        },
    )
    .await
    .expect("merge selected ForkTree branches")
}

async fn current_publication_conflicts<S>(
    engine: &Engine<CountingStorage<S>>,
    main: &SessionContext<CountingStorage<S>>,
    base: &str,
) -> SelectorConflictAccounting
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let first_id = "019f0000-0000-7000-9000-000000000001";
    let second_id = "019f0000-0000-7000-9000-000000000002";
    let same_id = "019f0000-0000-7000-9000-000000000003";
    for (id, name) in [
        (first_id, "race first"),
        (second_id, "race second"),
        (same_id, "race same"),
    ] {
        main.create_branch(CreateBranchOptions {
            id: Some(id.to_string()),
            name: name.to_string(),
            from_commit_id: Some(base.to_string()),
        })
        .await
        .expect("create current conflict branch");
    }
    let first = engine
        .open_session(first_id)
        .await
        .expect("open race first");
    let second = engine
        .open_session(second_id)
        .await
        .expect("open race second");
    let mut first_tx = first.begin_transaction().await.expect("begin race first");
    let mut second_tx = second.begin_transaction().await.expect("begin race second");
    first_tx
        .execute(
            "UPDATE forktree_rel_row SET value = 'race-first' WHERE id = 'row-00000000'",
            &[],
        )
        .await
        .expect("stage race first");
    second_tx
        .execute(
            "UPDATE forktree_rel_row SET value = 'race-second' WHERE id = 'row-00000001'",
            &[],
        )
        .await
        .expect("stage race second");
    first_tx.commit().await.expect("commit race first");
    let unrelated_second = second_tx.commit().await;
    let mut accounting = SelectorConflictAccounting::default();
    if unrelated_second.is_ok() {
        accounting.unrelated_writer_success_potential = 1;
    } else {
        accounting.unrelated_global_epoch_conflicts = 1;
        let retry = engine
            .open_session(second_id)
            .await
            .expect("open race retry");
        retry
            .execute(
                "UPDATE forktree_rel_row SET value = 'race-second-retry' WHERE id = 'row-00000001'",
                &[],
            )
            .await
            .expect("retry unrelated current publication");
    }

    let same_first_session = engine
        .open_session(same_id)
        .await
        .expect("open race same first");
    let same_stale_session = engine
        .open_session(same_id)
        .await
        .expect("open race same stale");
    let mut same_first = same_first_session
        .begin_transaction()
        .await
        .expect("begin same first");
    let mut same_stale = same_stale_session
        .begin_transaction()
        .await
        .expect("begin same stale");
    same_first
        .execute(
            "UPDATE forktree_rel_row SET value = 'same-first' WHERE id = 'row-00000002'",
            &[],
        )
        .await
        .expect("stage same first");
    same_stale
        .execute(
            "UPDATE forktree_rel_row SET value = 'same-stale' WHERE id = 'row-00000002'",
            &[],
        )
        .await
        .expect("stage same stale");
    same_first.commit().await.expect("commit same first");
    assert!(
        same_stale.commit().await.is_err(),
        "same-branch stale writer committed"
    );
    accounting.same_selector_stale_rejections = 1;
    for id in [first_id, second_id, same_id] {
        delete_current_branch(main, id)
            .await
            .expect("delete current conflict branch");
    }
    accounting
}

async fn register_schema<S>(session: &SessionContext<CountingStorage<S>>)
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
            "value": { "type": "string" }
        },
        "additionalProperties": false
    });
    let result = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register branch-scale schema");
    assert_eq!(result.rows_affected(), 1);
}

async fn seed_current_rows<S>(session: &SessionContext<CountingStorage<S>>, rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let batch = PreparedDmlParameterBatch::from_rows((0..rows).map(|index| {
        vec![
            Value::Text(text_row_key(index)),
            Value::Text(base_value(index)),
        ]
    }))
    .expect("build current branch-scale seed");
    let affected = session
        .execute_prepared_dml_batch(
            Arc::from("INSERT INTO forktree_rel_row (id, value) VALUES ($1, $2)"),
            batch,
        )
        .await
        .expect("seed current branch-scale rows")
        .iter()
        .map(lix::ExecuteResult::rows_affected)
        .sum::<u64>();
    assert_eq!(affected, rows as u64);
}

async fn update_current_rows<S>(
    session: &SessionContext<CountingStorage<S>>,
    parameters: Parameters,
    branch: usize,
) -> Result<(), lix::LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let batch =
        PreparedDmlParameterBatch::from_rows((0..parameters.rows_per_edit).map(|ordinal| {
            vec![
                Value::Text(branch_value_text(branch)),
                Value::Text(text_row_key(selected_index(parameters.rows, ordinal))),
            ]
        }))?;
    let mut transaction = session.begin_transaction().await?;
    let affected = transaction
        .execute_prepared_dml_batch(
            Arc::from("UPDATE forktree_rel_row SET value = $1 WHERE id = $2"),
            batch,
        )
        .await?
        .iter()
        .map(lix::ExecuteResult::rows_affected)
        .sum::<u64>();
    if affected != parameters.rows_per_edit as u64 {
        return Err(lix::LixError::unknown(
            "current branch update count mismatch",
        ));
    }
    transaction.commit().await?;
    Ok(())
}

async fn current_read_branches<S>(
    engine: &Engine<CountingStorage<S>>,
    branches: &[String],
    rows: usize,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let range_end = RANGE_ROWS.min(rows).saturating_sub(1);
    for branch in branches {
        let session = engine
            .open_session(branch)
            .await
            .expect("open current branch for reads");
        let point = session
            .execute(
                "SELECT value FROM forktree_rel_row WHERE id = $1",
                &[Value::Text(text_row_key(rows / 2))],
            )
            .await
            .expect("read current branch point");
        assert_eq!(point.len(), 1);
        let range = session
            .execute(
                "SELECT id, value FROM forktree_rel_row WHERE id >= $1 AND id <= $2 ORDER BY id",
                &[
                    Value::Text(text_row_key(0)),
                    Value::Text(text_row_key(range_end)),
                ],
            )
            .await
            .expect("read current branch range");
        assert_eq!(range.len(), range_end + 1);
    }
}

async fn forktree_read_branches<S>(
    tree: &ForkTree<CountingStorage<S>>,
    branches: &[String],
    rows: usize,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let range_end = RANGE_ROWS.min(rows).saturating_sub(1);
    for branch in branches {
        assert!(
            tree.read_relational_point(branch, &row_key(rows / 2))
                .await
                .expect("read ForkTree point")
                .is_some()
        );
        let range = tree
            .read_range(branch, &row_key(0), &row_key(range_end))
            .await
            .expect("read ForkTree range");
        assert_eq!(range.len(), range_end + 1);
    }
}

async fn current_head<S>(
    session: &SessionContext<CountingStorage<S>>,
) -> Result<String, lix::LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = session
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await?;
    result
        .rows()
        .first()
        .ok_or_else(|| lix::LixError::unknown("missing current active branch head"))?
        .get::<String>("commit_id")
}

async fn assert_current_value<S>(
    session: &SessionContext<CountingStorage<S>>,
    row: usize,
    branch: usize,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = session
        .execute(
            "SELECT value FROM forktree_rel_row WHERE id = $1",
            &[Value::Text(text_row_key(row))],
        )
        .await
        .expect("read retained current branch value");
    let actual = result
        .rows()
        .first()
        .expect("retained current branch row")
        .get::<String>("value")
        .expect("retained current branch value");
    assert_eq!(actual, branch_value_text(branch));
}

async fn delete_current_branch<S>(
    main: &SessionContext<CountingStorage<S>>,
    branch: &str,
) -> Result<(), lix::LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = main
        .execute(
            "DELETE FROM lix_branch WHERE id = $1",
            &[Value::Text(branch.to_string())],
        )
        .await?;
    if result.rows_affected() != 1 {
        return Err(lix::LixError::unknown(
            "current branch delete count mismatch",
        ));
    }
    Ok(())
}

fn initial_rows(rows: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..rows)
        .map(|index| (row_key(index), base_value(index).into_bytes()))
        .collect()
}

fn branch_mutations(parameters: Parameters, branch: usize) -> Vec<Mutation> {
    (0..parameters.rows_per_edit)
        .map(|ordinal| Mutation::Update {
            key: row_key(selected_index(parameters.rows, ordinal)),
            value: branch_value(branch),
        })
        .collect()
}

fn add_apply(total: &mut ApplyAccounting, one: ApplyAccounting) {
    total.object_writes += one.object_writes;
    total.object_bytes += one.object_bytes;
    total.logical_bytes += one.logical_bytes;
    total.node_writes += one.node_writes;
    total.node_bytes += one.node_bytes;
    total.leaf_writes += one.leaf_writes;
    total.leaf_bytes += one.leaf_bytes;
    total.internal_writes += one.internal_writes;
    total.internal_bytes += one.internal_bytes;
    total.reused_objects += one.reused_objects;
}

fn selected_index(rows: usize, ordinal: usize) -> usize {
    (ordinal + 1) * rows / (rows.div_ceil(100) + 1)
}

fn row_key(index: usize) -> Vec<u8> {
    text_row_key(index).into_bytes()
}

fn text_row_key(index: usize) -> String {
    format!("row-{index:08}")
}

fn base_value(index: usize) -> String {
    format!("base-{index:08}-{}", "x".repeat(48))
}

fn branch_value(branch: usize) -> RelationalValue {
    RelationalValue::Bytes(branch_value_text(branch).into_bytes())
}

fn branch_value_text(branch: usize) -> String {
    format!("branch-{branch:08}-{}", "y".repeat(48))
}

fn current_branch_id(index: usize) -> String {
    format!("019f0000-0000-7000-8000-{index:012x}")
}

fn current_merge_target_id(index: usize) -> String {
    format!("019f0000-0000-7000-a000-{index:012x}")
}

fn forktree_branch(index: usize) -> String {
    format!("branch-scale-{index:04}")
}

async fn measured<F, T>(
    phase: &str,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    parameters: Parameters,
    future: F,
) -> T
where
    F: Future<Output = T>,
{
    let _ = take_stats(stats);
    let physical_before = counters.map(SlateDBIoCounters::snapshot);
    let disk_before = directory_bytes(path);
    let rss_before = process_resident_bytes();
    let cpu_ticks_before = process_cpu_ticks();
    let cpu_nanos_before = process_cpu_nanos();
    let (stop, peak, sampler) = start_rss_sampler(rss_before);
    begin_allocation_profile();
    let started = Instant::now();
    let result = future.await;
    let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
    let (allocated_bytes, allocation_calls) = end_allocation_profile();
    stop.store(true, Ordering::Release);
    sampler.join().expect("join branch-scale RSS sampler");
    let cpu_ticks = process_cpu_ticks().saturating_sub(cpu_ticks_before);
    let cpu_nanos = process_cpu_nanos().saturating_sub(cpu_nanos_before);
    let rss_after = process_resident_bytes();
    let peak_rss = peak.load(Ordering::Acquire);
    let io = take_stats(stats);
    let physical = physical_delta(counters, physical_before);
    let disk_after = directory_bytes(path);
    println!(
        "branch_diff_phase,backend={},layout={},rows={},branches={},edit_percent={},edited_branches={},rows_per_edit={},phase={phase},wall_us={wall_us:.3},cpu_ticks={cpu_ticks},cpu_nanos={cpu_nanos},allocated_bytes={allocated_bytes},allocation_calls={allocation_calls},rss_before_bytes={rss_before},rss_after_bytes={rss_after},peak_rss_bytes={peak_rss},begin_reads={},begin_writes={},get_calls={},get_keys={},get_values={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},write_batches={},write_puts={},write_deletes={},write_ranges={},write_bytes={},commits={},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},disk_before_bytes={disk_before},disk_after_bytes={disk_after},disk_growth_bytes={}",
        parameters.backend.label(),
        parameters.layout.label(),
        parameters.rows,
        parameters.branches,
        parameters.edit_percent,
        parameters.edits,
        parameters.rows_per_edit,
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
        io.write_ranges,
        io.write_bytes,
        io.commits,
        physical.read_objects,
        physical.read_bytes,
        physical.write_objects,
        physical.write_bytes,
        disk_after.saturating_sub(disk_before),
    );
    result
}

fn start_rss_sampler(
    initial: u64,
) -> (Arc<AtomicBool>, Arc<AtomicU64>, std::thread::JoinHandle<()>) {
    let stop = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicU64::new(initial));
    let thread_stop = Arc::clone(&stop);
    let thread_peak = Arc::clone(&peak);
    let sampler = std::thread::spawn(move || {
        while !thread_stop.load(Ordering::Acquire) {
            thread_peak.fetch_max(process_resident_bytes(), Ordering::AcqRel);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        thread_peak.fetch_max(process_resident_bytes(), Ordering::AcqRel);
    });
    (stop, peak, sampler)
}

fn env_usize(name: &str) -> usize {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} is required"))
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be an integer"))
}
