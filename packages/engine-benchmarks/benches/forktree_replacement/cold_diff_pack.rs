use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use lix::storage::Storage;
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

use super::model::{DiffAccounting, ForkTree, Mutation, ObjectId, RelationalValue};
use super::{
    Backend, CountingStorage, IoStats, begin_allocation_profile, directory_bytes,
    end_allocation_profile, physical_delta, process_cpu_nanos, process_cpu_ticks,
    process_resident_bytes, settle_rocksdb_compaction, take_stats,
};

const ROWS: usize = 10_000;
const BRANCHES: usize = 100;
const EDITED_BRANCHES: usize = 10;
const ROWS_PER_EDIT: usize = 100;
const TARGET_PACK_BYTES: usize = 1024 * 1024;

pub(super) async fn run() {
    let backend = Backend::parse(std::env::args().nth(2).as_deref().unwrap_or("slatedb"));
    println!(
        "forktree_pack_model,backend={},rows={ROWS},branches={BRANCHES},edited_branches={EDITED_BRANCHES},rows_per_edit={ROWS_PER_EDIT},target_pack_bytes={TARGET_PACK_BYTES},object_authority=blake3_canonical_object_bytes,pack_authority=domain_separated_blake3_full_pack,locator=pure_rebuildable_non_authoritative,read_authentication=pack_hash_plus_object_hash_plus_decoder_domain",
        backend.label()
    );
    match backend {
        Backend::RocksDb => run_rocks().await,
        Backend::SlateDb => run_slate().await,
    }
}

async fn run_rocks() {
    let directory = tempfile::tempdir().expect("create packed ForkTree RocksDB directory");
    let oracle = {
        let database = RocksDB::open(directory.path()).expect("open packed ForkTree RocksDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let oracle = setup_and_pack(storage, &stats, directory.path(), None).await;
        database.flush().expect("flush packed ForkTree RocksDB");
        oracle
    };
    {
        let database = RocksDB::open(directory.path()).expect("reopen packed ForkTree RocksDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        reopen_and_diff(storage, &stats, directory.path(), None, oracle).await;
        database
            .flush()
            .expect("flush final packed ForkTree RocksDB");
    }
    let immediate = directory_bytes(directory.path());
    let settled = settle_rocksdb_compaction(directory.path());
    println!(
        "forktree_pack_settled,backend=rocksdb,immediate_disk_bytes={immediate},settled_disk_bytes={settled}"
    );
}

async fn run_slate() {
    let directory = tempfile::tempdir().expect("create packed ForkTree SlateDB directory");
    let oracle = {
        let counters = SlateDBIoCounters::default();
        let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
            .expect("open packed ForkTree SlateDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let oracle = setup_and_pack(storage, &stats, directory.path(), Some(&counters)).await;
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush packed ForkTree SlateDB");
        oracle
    };
    {
        let counters = SlateDBIoCounters::default();
        let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
            .expect("reopen packed ForkTree SlateDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        reopen_and_diff(storage, &stats, directory.path(), Some(&counters), oracle).await;
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush final packed ForkTree SlateDB");
    }
    println!(
        "forktree_pack_settled,backend=slatedb,immediate_disk_bytes={},settled_disk_bytes=not_applicable",
        directory_bytes(directory.path())
    );
}

struct Oracle {
    base: ObjectId,
    edited_heads: Vec<ObjectId>,
}

async fn setup_and_pack<S>(
    storage: CountingStorage<S>,
    stats: &Arc<std::sync::Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) -> Oracle
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let tree = ForkTree::new(storage);
    let base = tree
        .initialize(&initial_rows())
        .await
        .expect("initialize packed ForkTree gate");
    tree.create_checkpoint("base", base)
        .await
        .expect("checkpoint packed ForkTree base");
    let branches = (0..BRANCHES)
        .map(|index| format!("branch-scale-{index:04}"))
        .collect::<Vec<_>>();
    for branch in &branches {
        tree.create_branch(branch, Some(base))
            .await
            .expect("create packed ForkTree branch");
    }
    let mut edited_heads = Vec::with_capacity(EDITED_BRANCHES);
    for (index, branch) in branches.iter().take(EDITED_BRANCHES).enumerate() {
        let (head, _) = tree
            .apply_sorted_mutations_on(branch, &branch_mutations(index))
            .await
            .expect("edit packed ForkTree branch");
        edited_heads.push(head);
    }
    let (objects, object_bytes) = tree
        .object_inventory()
        .await
        .expect("inventory pre-pack ForkTree objects");
    let accounting = measured("pack_build", stats, path, counters, async {
        tree.replace_objects_with_packs(TARGET_PACK_BYTES).await
    })
    .await
    .expect("replace ForkTree objects with immutable packs");
    assert_eq!(accounting.objects, objects);
    assert_eq!(accounting.object_bytes, object_bytes);
    println!(
        "forktree_pack_build,backend={},objects={},object_bytes={},packs={},pack_bytes={},header_bytes={},write_amplification={:.6}",
        backend_label(counters),
        accounting.objects,
        accounting.object_bytes,
        accounting.packs,
        accounting.pack_bytes,
        accounting.header_bytes,
        accounting.pack_bytes as f64 / accounting.object_bytes as f64,
    );
    Oracle { base, edited_heads }
}

async fn reopen_and_diff<S>(
    storage: CountingStorage<S>,
    stats: &Arc<std::sync::Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    oracle: Oracle,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let bootstrap = ForkTree::new(storage.clone());
    let (locator, rebuild) = measured("locator_rebuild", stats, path, counters, async {
        bootstrap.rebuild_packed_locator().await
    })
    .await
    .expect("rebuild packed ForkTree locator");
    println!(
        "forktree_pack_rebuild,backend={},objects={},packs={},pack_bytes={}",
        backend_label(counters),
        rebuild.objects,
        rebuild.packs,
        rebuild.pack_bytes,
    );
    let tree = ForkTree::new_packed(storage, locator);
    let (changes, accounting) = measured("cold_diff", stats, path, counters, async {
        let mut changes = 0_usize;
        let mut accounting = DiffAccounting::default();
        for head in &oracle.edited_heads {
            let (diff, one) = tree.diff_commits_profiled(oracle.base, *head).await?;
            changes += diff.len();
            add_diff(&mut accounting, one);
        }
        Ok::<_, String>((changes, accounting))
    })
    .await
    .expect("diff packed ForkTree branches");
    assert_eq!(changes, EDITED_BRANCHES * ROWS_PER_EDIT);
    println!(
        "forktree_pack_diff,backend={},diffs={},changes={},hash_pruned_nodes={},decoded_nodes={},commit_batches={},commit_objects={},node_batches={},node_objects={},value_batches={},value_references={},unique_value_packs={},authenticated_bytes={},commit_read_nanos={},node_read_nanos={},node_decode_nanos={},value_read_nanos={},value_decode_nanos={}",
        backend_label(counters),
        EDITED_BRANCHES,
        changes,
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
    phase: &str,
    stats: &Arc<std::sync::Mutex<IoStats>>,
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
    sampler.join().expect("join packed ForkTree RSS sampler");
    let cpu_ticks = process_cpu_ticks().saturating_sub(cpu_ticks_before);
    let cpu_nanos = process_cpu_nanos().saturating_sub(cpu_nanos_before);
    let rss_after = process_resident_bytes();
    let peak_rss = peak.load(Ordering::Acquire);
    let io = take_stats(stats);
    let physical = physical_delta(counters, physical_before);
    let disk_after = directory_bytes(path);
    println!(
        "forktree_pack_phase,backend={},phase={phase},wall_us={wall_us:.3},cpu_ticks={cpu_ticks},cpu_nanos={cpu_nanos},allocated_bytes={allocated_bytes},allocation_calls={allocation_calls},rss_before_bytes={rss_before},rss_after_bytes={rss_after},peak_rss_bytes={peak_rss},begin_reads={},begin_writes={},get_calls={},get_keys={},get_values={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},write_batches={},write_puts={},write_deletes={},write_ranges={},write_bytes={},commits={},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},disk_before_bytes={disk_before},disk_after_bytes={disk_after},disk_growth_bytes={}",
        backend_label(counters),
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

fn initial_rows() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..ROWS)
        .map(|index| {
            (
                row_key(index),
                format!("base-{index:08}-{}", "x".repeat(48)).into_bytes(),
            )
        })
        .collect()
}

fn branch_mutations(branch: usize) -> Vec<Mutation> {
    (0..ROWS_PER_EDIT)
        .map(|ordinal| Mutation::Update {
            key: row_key((ordinal + 1) * ROWS / (ROWS_PER_EDIT + 1)),
            value: RelationalValue::Bytes(
                format!("branch-{branch:08}-{}", "y".repeat(48)).into_bytes(),
            ),
        })
        .collect()
}

fn row_key(index: usize) -> Vec<u8> {
    format!("row-{index:08}").into_bytes()
}

fn add_diff(total: &mut DiffAccounting, one: DiffAccounting) {
    total.changes += one.changes;
    total.hash_pruned_nodes += one.hash_pruned_nodes;
    total.decoded_nodes += one.decoded_nodes;
    total.commit_batches += one.commit_batches;
    total.commit_objects += one.commit_objects;
    total.node_batches += one.node_batches;
    total.node_objects += one.node_objects;
    total.value_batches += one.value_batches;
    total.value_references += one.value_references;
    total.unique_value_packs += one.unique_value_packs;
    total.authenticated_bytes += one.authenticated_bytes;
    total.commit_read_nanos += one.commit_read_nanos;
    total.node_read_nanos += one.node_read_nanos;
    total.node_decode_nanos += one.node_decode_nanos;
    total.value_read_nanos += one.value_read_nanos;
    total.value_decode_nanos += one.value_decode_nanos;
}

fn backend_label(counters: Option<&SlateDBIoCounters>) -> &'static str {
    if counters.is_some() {
        "slatedb"
    } else {
        "rocksdb"
    }
}
