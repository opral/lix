use std::collections::BTreeSet;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use lix::storage::{Memory, Storage};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

use super::model::{
    ApplyAccounting, DiffAccounting, ForkTree, Mutation, ObjectId, RelationalValue,
};
use super::{
    CountingStorage, IoStats, begin_allocation_profile, directory_bytes, end_allocation_profile,
    physical_delta, process_cpu_nanos, process_cpu_ticks, process_resident_bytes, take_stats,
};

const SMALL_DIFF_ROWS: usize = 10;

#[derive(Clone, Copy)]
enum Backend {
    Memory,
    RocksDb,
    SlateDb,
}

impl Backend {
    fn parse(value: &str) -> Self {
        match value {
            "memory" => Self::Memory,
            "rocksdb" => Self::RocksDb,
            "slatedb" => Self::SlateDb,
            other => panic!("unknown history-independence backend '{other}'"),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::RocksDb => "rocksdb",
            Self::SlateDb => "slatedb",
        }
    }
}

#[derive(Clone)]
struct State {
    name: &'static str,
    head: ObjectId,
    real_diff_head: ObjectId,
}

struct Oracle {
    baseline: ObjectId,
    states: Vec<State>,
}

pub(super) async fn run() {
    let args = std::env::args().collect::<Vec<_>>();
    let backend = Backend::parse(args.get(2).map(String::as_str).unwrap_or("memory"));
    let rows = args
        .get(3)
        .map_or(1_000, |value| value.parse().expect("row count"));
    assert!(matches!(rows, 1_000 | 50_000));
    println!(
        "forktree_history_model,backend={},rows={},accepted_stage1=bc82385ec42b1789018fbd1213f637c19104a02c,accepted_diff_model=0be9b69b63e78a52e458d8381cd29a00cc6153bb,current_big_o=publication_O(changed_paths_plus_output)_identical_diff_O(changed_paths_implied_by_distinct_roots)_real_diff_O(changed_paths_plus_output),canonicalized_big_o=publication_at_least_O(N)_identical_diff_O(1)_real_diff_O(changed_paths_plus_output),perfect_elimination=all_identical_state_traversal_when_roots_differ",
        backend.label(),
        rows,
    );
    match backend {
        Backend::Memory => run_memory(rows).await,
        Backend::RocksDb => run_rocks(rows).await,
        Backend::SlateDb => run_slate(rows).await,
    }
}

async fn run_memory(rows: usize) {
    let directory = tempfile::tempdir().expect("create history Memory metrics directory");
    let memory = Memory::new();
    let (storage, stats) = CountingStorage::new(memory.clone());
    let oracle = setup_histories(storage, &stats, directory.path(), None, rows, "memory").await;
    let snapshot = memory.export_snapshot().expect("snapshot ForkTree Memory");
    drop(memory);
    let reopened = Memory::from_snapshot(&snapshot).expect("reopen ForkTree Memory snapshot");
    let (storage, stats) = CountingStorage::new(reopened);
    evaluate_histories(
        storage,
        &stats,
        directory.path(),
        None,
        rows,
        "memory",
        oracle,
    )
    .await;
}

async fn run_rocks(rows: usize) {
    let directory = tempfile::tempdir().expect("create history RocksDB directory");
    let oracle = {
        let database = RocksDB::open(directory.path()).expect("open history RocksDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let oracle =
            setup_histories(storage, &stats, directory.path(), None, rows, "rocksdb").await;
        database.flush().expect("flush history RocksDB setup");
        oracle
    };
    {
        let database = RocksDB::open(directory.path()).expect("reopen history RocksDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        evaluate_histories(
            storage,
            &stats,
            directory.path(),
            None,
            rows,
            "rocksdb",
            oracle,
        )
        .await;
        database.flush().expect("flush history RocksDB result");
    }
    println!(
        "forktree_history_disk,backend=rocksdb,rows={},post_close_bytes={}",
        rows,
        directory_bytes(directory.path())
    );
}

async fn run_slate(rows: usize) {
    let directory = tempfile::tempdir().expect("create history SlateDB directory");
    let oracle = {
        let counters = SlateDBIoCounters::default();
        let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
            .expect("open history SlateDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let oracle = setup_histories(
            storage,
            &stats,
            directory.path(),
            Some(&counters),
            rows,
            "slatedb",
        )
        .await;
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush history SlateDB setup");
        oracle
    };
    {
        let counters = SlateDBIoCounters::default();
        let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
            .expect("reopen history SlateDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        evaluate_histories(
            storage,
            &stats,
            directory.path(),
            Some(&counters),
            rows,
            "slatedb",
            oracle,
        )
        .await;
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush history SlateDB result");
    }
    println!(
        "forktree_history_disk,backend=slatedb,rows={},post_close_bytes={}",
        rows,
        directory_bytes(directory.path())
    );
}

async fn setup_histories<S>(
    storage: CountingStorage<S>,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    rows: usize,
    backend: &str,
) -> Oracle
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let tree = ForkTree::new(storage);
    let logical = logical_rows(rows);
    let baseline = measured(
        backend,
        "publish_sorted_load",
        stats,
        path,
        counters,
        tree.initialize(&logical),
    )
    .await
    .expect("initialize sorted history baseline");

    for name in [
        "randomized-insertion",
        "delete-reinsert",
        "branch-mutation-order",
        "split-boundary-edits",
    ] {
        tree.create_branch(name, Some(baseline))
            .await
            .expect("create history branch");
    }

    let randomized = measured(
        backend,
        "publish_randomized_insertion",
        stats,
        path,
        counters,
        rebuild_randomized(&tree, &logical),
    )
    .await
    .expect("publish randomized history");
    let delete_reinsert = measured(
        backend,
        "publish_delete_reinsert",
        stats,
        path,
        counters,
        delete_and_reinsert(&tree, &logical),
    )
    .await
    .expect("publish delete/reinsert history");
    let mutation_order = measured(
        backend,
        "publish_branch_mutation_order",
        stats,
        path,
        counters,
        mutate_and_restore(&tree, &logical, false),
    )
    .await
    .expect("publish branch-order history");
    let split_boundaries = measured(
        backend,
        "publish_split_boundary_edits",
        stats,
        path,
        counters,
        mutate_and_restore(&tree, &logical, true),
    )
    .await
    .expect("publish split-boundary history");

    let mut states = vec![
        ("sorted-load", baseline, ApplyAccounting::default()),
        ("randomized-insertion", randomized.0, randomized.1),
        ("delete-reinsert", delete_reinsert.0, delete_reinsert.1),
        ("branch-mutation-order", mutation_order.0, mutation_order.1),
        (
            "split-boundary-edits",
            split_boundaries.0,
            split_boundaries.1,
        ),
    ];
    for (name, _, accounting) in &states {
        println!(
            "forktree_history_publication,backend={backend},rows={rows},history={name},object_writes={},object_bytes={},node_writes={},node_bytes={},reused_objects={},logical_bytes={}",
            accounting.object_writes,
            accounting.object_bytes,
            accounting.node_writes,
            accounting.node_bytes,
            accounting.reused_objects,
            accounting.logical_bytes,
        );
    }

    let mut output = Vec::with_capacity(states.len());
    for (name, head, _) in states.drain(..) {
        let branch = format!("real-{name}");
        tree.create_branch(&branch, Some(head))
            .await
            .expect("create small-real-diff branch");
        let (real_diff_head, accounting) = tree
            .apply_sorted_mutations_on(&branch, &small_real_diff(&logical))
            .await
            .expect("publish small real diff");
        println!(
            "forktree_history_real_publication,backend={backend},rows={rows},history={name},changes={SMALL_DIFF_ROWS},object_writes={},object_bytes={},node_writes={},node_bytes={},reused_objects={},logical_bytes={}",
            accounting.object_writes,
            accounting.object_bytes,
            accounting.node_writes,
            accounting.node_bytes,
            accounting.reused_objects,
            accounting.logical_bytes,
        );
        output.push(State {
            name,
            head,
            real_diff_head,
        });
    }
    Oracle {
        baseline,
        states: output,
    }
}

async fn evaluate_histories<S>(
    storage: CountingStorage<S>,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    rows: usize,
    backend: &str,
    oracle: Oracle,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let tree = ForkTree::new(storage);
    let expected = tree
        .read_relational_all_at(oracle.baseline)
        .await
        .expect("read baseline logical state");
    let baseline_root = tree
        .commit_root(oracle.baseline)
        .await
        .expect("load baseline root");
    for state in &oracle.states {
        let actual = tree
            .read_relational_all_at(state.head)
            .await
            .expect("read history logical state");
        assert_eq!(actual, expected, "history state {} differs", state.name);
        let root = tree
            .commit_root(state.head)
            .await
            .expect("load history root");
        let shared = tree
            .shared_root_inventory(oracle.baseline, state.head)
            .await
            .expect("inventory live-root history sharing");
        let denominator = shared.left_bytes.min(shared.right_bytes).max(1);
        let sharing_percent = shared.shared_bytes as f64 * 100.0 / denominator as f64;
        let sync_bytes = shared.right_bytes.saturating_sub(shared.shared_bytes);
        println!(
            "forktree_history_identity,backend={backend},rows={rows},history={},root_equal={},baseline_root={},history_root={},shared_objects={},left_objects={},right_objects={},shared_bytes={},left_bytes={},right_bytes={},sharing_percent={sharing_percent:.3},sync_bytes={sync_bytes}",
            state.name,
            root == baseline_root,
            ForkTree::<CountingStorage<S>>::object_id_hex(baseline_root),
            ForkTree::<CountingStorage<S>>::object_id_hex(root),
            shared.shared_objects,
            shared.left_objects,
            shared.right_objects,
            shared.shared_bytes,
            shared.left_bytes,
            shared.right_bytes,
        );

        let phase = format!("identical_diff_{}", state.name);
        let (identical, identical_accounting) = measured(
            backend,
            &phase,
            stats,
            path,
            counters,
            tree.diff_commits_profiled(oracle.baseline, state.head),
        )
        .await
        .expect("run identical-state diff");
        assert!(identical.is_empty());
        print_diff_accounting(backend, rows, state.name, "identical", identical_accounting);

        let phase = format!("real_diff_{}", state.name);
        let (real, real_accounting) = measured(
            backend,
            &phase,
            stats,
            path,
            counters,
            tree.diff_commits_profiled(state.head, state.real_diff_head),
        )
        .await
        .expect("run small real diff");
        assert_eq!(real.len(), SMALL_DIFF_ROWS);
        print_diff_accounting(backend, rows, state.name, "real", real_accounting);
    }
}

async fn rebuild_randomized<S>(
    tree: &ForkTree<CountingStorage<S>>,
    rows: &[(Vec<u8>, Vec<u8>)],
) -> Result<(ObjectId, ApplyAccounting), String>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut total = ApplyAccounting::default();
    add_apply(
        &mut total,
        tree.apply_sorted_mutations_on(
            "randomized-insertion",
            &rows[1..]
                .iter()
                .map(|(key, _)| Mutation::Delete { key: key.clone() })
                .collect::<Vec<_>>(),
        )
        .await?
        .1,
    );
    let mut indices = (1..rows.len()).collect::<Vec<_>>();
    deterministic_shuffle(&mut indices);
    let mut head = tree.branch_head("randomized-insertion").await?;
    for index in indices {
        let (next, accounting) = tree
            .apply_sorted_mutations_on(
                "randomized-insertion",
                &[Mutation::Insert {
                    key: rows[index].0.clone(),
                    value: RelationalValue::Bytes(rows[index].1.clone()),
                }],
            )
            .await?;
        head = next;
        add_apply(&mut total, accounting);
    }
    Ok((head, total))
}

async fn delete_and_reinsert<S>(
    tree: &ForkTree<CountingStorage<S>>,
    rows: &[(Vec<u8>, Vec<u8>)],
) -> Result<(ObjectId, ApplyAccounting), String>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let selected = history_indices(rows.len(), false);
    let deletes = selected
        .iter()
        .map(|&index| Mutation::Delete {
            key: rows[index].0.clone(),
        })
        .collect::<Vec<_>>();
    let (_, mut total) = tree
        .apply_sorted_mutations_on("delete-reinsert", &deletes)
        .await?;
    let inserts = selected
        .iter()
        .map(|&index| Mutation::Insert {
            key: rows[index].0.clone(),
            value: RelationalValue::Bytes(rows[index].1.clone()),
        })
        .collect::<Vec<_>>();
    let (head, accounting) = tree
        .apply_sorted_mutations_on("delete-reinsert", &inserts)
        .await?;
    add_apply(&mut total, accounting);
    Ok((head, total))
}

async fn mutate_and_restore<S>(
    tree: &ForkTree<CountingStorage<S>>,
    rows: &[(Vec<u8>, Vec<u8>)],
    boundaries: bool,
) -> Result<(ObjectId, ApplyAccounting), String>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let branch = if boundaries {
        "split-boundary-edits"
    } else {
        "branch-mutation-order"
    };
    let mut indices = history_indices(rows.len(), boundaries);
    if !boundaries {
        deterministic_shuffle(&mut indices);
    }
    let mut total = ApplyAccounting::default();
    for &index in &indices {
        let (_, accounting) = tree
            .apply_sorted_mutations_on(
                branch,
                &[Mutation::Update {
                    key: rows[index].0.clone(),
                    value: RelationalValue::Bytes(temporary_value(index, boundaries)),
                }],
            )
            .await?;
        add_apply(&mut total, accounting);
    }
    indices.reverse();
    let mut head = tree.branch_head(branch).await?;
    for index in indices {
        let (next, accounting) = tree
            .apply_sorted_mutations_on(
                branch,
                &[Mutation::Update {
                    key: rows[index].0.clone(),
                    value: RelationalValue::Bytes(rows[index].1.clone()),
                }],
            )
            .await?;
        head = next;
        add_apply(&mut total, accounting);
    }
    Ok((head, total))
}

fn logical_rows(rows: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..rows)
        .map(|index| {
            (
                format!("row-{index:08}").into_bytes(),
                format!("value-{index:08}-{}", "x".repeat(48)).into_bytes(),
            )
        })
        .collect()
}

fn history_indices(rows: usize, boundaries: bool) -> Vec<usize> {
    if !boundaries {
        return (1..=rows.min(100))
            .map(|ordinal| ordinal * (rows - 1) / (rows.min(100) + 1))
            .filter(|&index| index > 0)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
    }
    let mut selected = BTreeSet::new();
    for boundary in (64..rows).step_by(64) {
        for index in boundary.saturating_sub(2)..=(boundary + 2).min(rows - 1) {
            selected.insert(index);
        }
    }
    for boundary in (2_048..rows).step_by(2_048) {
        for index in boundary.saturating_sub(2)..=(boundary + 2).min(rows - 1) {
            selected.insert(index);
        }
    }
    selected.into_iter().collect()
}

fn small_real_diff(rows: &[(Vec<u8>, Vec<u8>)]) -> Vec<Mutation> {
    (1..=SMALL_DIFF_ROWS)
        .map(|ordinal| ordinal * (rows.len() - 1) / (SMALL_DIFF_ROWS + 1))
        .map(|index| Mutation::Update {
            key: rows[index].0.clone(),
            value: RelationalValue::Bytes(
                format!("real-{index:08}-{}", "r".repeat(48)).into_bytes(),
            ),
        })
        .collect()
}

fn temporary_value(index: usize, boundaries: bool) -> Vec<u8> {
    format!(
        "{}-{index:08}-{}",
        if boundaries { "boundary" } else { "order" },
        "t".repeat(48)
    )
    .into_bytes()
}

fn deterministic_shuffle(values: &mut [usize]) {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    for index in (1..values.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        values.swap(index, state as usize % (index + 1));
    }
}

fn add_apply(total: &mut ApplyAccounting, one: ApplyAccounting) {
    total.object_writes += one.object_writes;
    total.object_bytes += one.object_bytes;
    total.node_writes += one.node_writes;
    total.node_bytes += one.node_bytes;
    total.leaf_writes += one.leaf_writes;
    total.leaf_bytes += one.leaf_bytes;
    total.internal_writes += one.internal_writes;
    total.internal_bytes += one.internal_bytes;
    total.reused_objects += one.reused_objects;
    total.logical_bytes += one.logical_bytes;
}

fn print_diff_accounting(
    backend: &str,
    rows: usize,
    history: &str,
    kind: &str,
    accounting: DiffAccounting,
) {
    println!(
        "forktree_history_diff,backend={backend},rows={rows},history={history},kind={kind},changes={},hash_pruned_nodes={},decoded_nodes={},commit_batches={},commit_objects={},node_batches={},node_objects={},value_batches={},value_references={},unique_value_packs={},authenticated_bytes={},commit_read_nanos={},node_read_nanos={},node_decode_nanos={},value_read_nanos={},value_decode_nanos={}",
        accounting.changes,
        accounting.hash_pruned_nodes,
        accounting.decoded_nodes,
        accounting.commit_batches,
        accounting.commit_objects,
        accounting.node_batches,
        accounting.node_objects,
        accounting.value_batches,
        accounting.value_references,
        accounting.unique_value_packs,
        accounting.authenticated_bytes,
        accounting.commit_read_nanos,
        accounting.node_read_nanos,
        accounting.node_decode_nanos,
        accounting.value_read_nanos,
        accounting.value_decode_nanos,
    );
}

async fn measured<F, T>(
    backend: &str,
    phase: &str,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
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
    sampler.join().expect("join history RSS sampler");
    let cpu_ticks = process_cpu_ticks().saturating_sub(cpu_ticks_before);
    let cpu_nanos = process_cpu_nanos().saturating_sub(cpu_nanos_before);
    let rss_after = process_resident_bytes();
    let peak_rss = peak.load(Ordering::Acquire);
    let io = take_stats(stats);
    let physical = physical_delta(counters, physical_before);
    let disk_after = directory_bytes(path);
    println!(
        "forktree_history_phase,backend={backend},phase={phase},wall_us={wall_us:.3},cpu_ticks={cpu_ticks},cpu_nanos={cpu_nanos},allocated_bytes={allocated_bytes},allocation_calls={allocation_calls},rss_before_bytes={rss_before},rss_after_bytes={rss_after},peak_rss_bytes={peak_rss},begin_reads={},begin_writes={},get_calls={},get_keys={},get_values={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},write_batches={},write_puts={},write_deletes={},write_ranges={},write_bytes={},commits={},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},disk_before_bytes={disk_before},disk_after_bytes={disk_after},disk_growth_bytes={}",
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
    let stop_worker = Arc::clone(&stop);
    let peak_worker = Arc::clone(&peak);
    let sampler = std::thread::spawn(move || {
        while !stop_worker.load(Ordering::Acquire) {
            peak_worker.fetch_max(process_resident_bytes(), Ordering::AcqRel);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        peak_worker.fetch_max(process_resident_bytes(), Ordering::AcqRel);
    });
    (stop, peak, sampler)
}
