use std::collections::{BTreeSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use lix::storage::Storage;
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

use super::model::{
    BlobAccounting, BlobIdentityInventory, ForkTree, ObjectLayoutStats, SegmentedByteSource,
};
use super::{
    Backend, CountingStorage, IoStats, begin_allocation_profile, directory_bytes,
    end_allocation_profile, physical_delta, process_cpu_nanos, process_cpu_ticks,
    process_resident_bytes, settle_rocksdb_compaction, take_stats,
};

const MIB: usize = 1024 * 1024;
const SIZE_MIB: usize = 16;
const SIZE: usize = SIZE_MIB * MIB;
const EDIT_BYTES: usize = SIZE / 100;
const EDIT_OFFSET: usize = SIZE / 2;
const RANGE_BYTES: usize = 64 * 1024;
const SEED: u64 = 0x89a3_10fd_4242_73c1;

#[derive(Clone, Copy)]
struct Parameters {
    branches: usize,
    edit_percent: usize,
    edits: usize,
}

impl Parameters {
    fn from_env() -> Self {
        let branches = env_usize("FORKTREE_FANOUT_BRANCHES");
        let edit_percent = env_usize("FORKTREE_FANOUT_EDIT_PERCENT");
        assert!(matches!(branches, 1 | 100 | 1_000));
        assert!(matches!(edit_percent, 1 | 10 | 100));
        let edits = branches.saturating_mul(edit_percent).div_ceil(100).max(1);
        Self {
            branches,
            edit_percent,
            edits,
        }
    }
}

pub(super) async fn run() {
    let parameters = Parameters::from_env();
    let backend = match std::env::args().nth(2).as_deref() {
        Some("rocksdb") | None => Backend::RocksDb,
        Some("slatedb") => Backend::SlateDb,
        Some(other) => panic!("unknown ForkTree fanout backend '{other}'"),
    };
    match backend {
        Backend::RocksDb => run_rocksdb(parameters).await,
        Backend::SlateDb => run_slatedb(parameters).await,
    }
}

async fn run_rocksdb(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create ForkTree fanout RocksDB directory");
    let oracle = {
        let database = RocksDB::open(directory.path()).expect("open ForkTree fanout RocksDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let oracle = run_setup(
            storage,
            Backend::RocksDb,
            &stats,
            directory.path(),
            None,
            parameters,
        )
        .await;
        database.flush().expect("flush ForkTree fanout RocksDB");
        println!(
            "forktree_fanout_close,backend=rocksdb,branches={},edit_percent={},disk_bytes={}",
            parameters.branches,
            parameters.edit_percent,
            directory_bytes(directory.path())
        );
        oracle
    };
    let database = RocksDB::open(directory.path()).expect("reopen ForkTree fanout RocksDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    run_reopen(
        storage,
        Backend::RocksDb,
        &stats,
        directory.path(),
        None,
        parameters,
        oracle,
    )
    .await;
    database
        .flush()
        .expect("flush final ForkTree fanout RocksDB");
    let immediate = directory_bytes(directory.path());
    drop(database);
    let settled = settle_rocksdb_compaction(directory.path());
    println!(
        "forktree_fanout_settled,backend=rocksdb,branches={},edit_percent={},immediate_disk_bytes={immediate},settled_disk_bytes={settled}",
        parameters.branches, parameters.edit_percent
    );
}

async fn run_slatedb(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create ForkTree fanout SlateDB directory");
    let oracle = {
        let counters = SlateDBIoCounters::default();
        let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
            .expect("open ForkTree fanout SlateDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let oracle = run_setup(
            storage,
            Backend::SlateDb,
            &stats,
            directory.path(),
            Some(&counters),
            parameters,
        )
        .await;
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush ForkTree fanout SlateDB");
        println!(
            "forktree_fanout_close,backend=slatedb,branches={},edit_percent={},disk_bytes={}",
            parameters.branches,
            parameters.edit_percent,
            directory_bytes(directory.path())
        );
        oracle
    };
    let counters = SlateDBIoCounters::default();
    let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
        .expect("reopen ForkTree fanout SlateDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    run_reopen(
        storage,
        Backend::SlateDb,
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
        .expect("flush final ForkTree fanout SlateDB");
    println!(
        "forktree_fanout_settled,backend=slatedb,branches={},edit_percent={},immediate_disk_bytes={},settled_disk_bytes=not_applicable",
        parameters.branches,
        parameters.edit_percent,
        directory_bytes(directory.path())
    );
}

#[derive(Clone)]
struct EditedOracle {
    branch: String,
    identity: BlobIdentityInventory,
    hash: [u8; 32],
}

struct FanoutOracle {
    base_identity: BlobIdentityInventory,
    base_hash: [u8; 32],
    edited: Vec<EditedOracle>,
    merge_targets: Vec<String>,
    layout_after_edits: ObjectLayoutStats,
}

async fn run_setup<S>(
    storage: CountingStorage<S>,
    backend: Backend,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    parameters: Parameters,
) -> FanoutOracle
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let base = Bytes::from(audio_like_bytes(SIZE, SEED));
    let common_edit = Bytes::from(deterministic_bytes(
        EDIT_BYTES,
        SEED ^ 0x6a09_e667_f3bc_c909,
    ));
    let base_hash = *blake3::hash(&base).as_bytes();
    let tree = ForkTree::new(storage);
    tree.initialize(&[(b"fanout-oracle".to_vec(), b"v1".to_vec())])
        .await
        .expect("initialize ForkTree fanout authority");
    let _ = take_stats(stats);

    let (base_head, ingest) = measured(
        backend,
        "ingest",
        stats,
        path,
        counters,
        parameters,
        tree.ingest_blob("main", SingleSpanSource::new(base.clone())),
    )
    .await
    .expect("ingest shared fanout payload");
    print_blob("ingest", ingest, parameters);
    let base_identity = tree
        .blob_identity_at_commit(base_head)
        .await
        .expect("load fanout base identity");
    let layout_before_branches = tree
        .object_layout_stats()
        .await
        .expect("inventory before fanout branches");

    measured(
        backend,
        "create_branches",
        stats,
        path,
        counters,
        parameters,
        async {
            for index in 0..parameters.branches {
                tree.create_branch(&branch_name(index), Some(base_head))
                    .await?;
            }
            Ok::<(), String>(())
        },
    )
    .await
    .expect("create fanout branches");
    let layout_after_branches = tree
        .object_layout_stats()
        .await
        .expect("inventory after fanout branches");
    assert_eq!(
        layout_after_branches.objects,
        layout_before_branches.objects
    );
    assert_eq!(
        layout_after_branches.object_value_bytes,
        layout_before_branches.object_value_bytes
    );
    assert_eq!(
        layout_after_branches.blob_chunks,
        layout_before_branches.blob_chunks
    );
    assert_eq!(
        layout_after_branches.blob_manifests,
        layout_before_branches.blob_manifests
    );
    assert_eq!(
        layout_after_branches.selectors - layout_before_branches.selectors,
        parameters.branches as u64
    );
    println!(
        "forktree_fanout_creation,backend={},branches={},edit_percent={},objects_before={},objects_after={},object_bytes_before={},object_bytes_after={},selectors_before={},selectors_after={},payload_copy_objects=0,payload_copy_bytes=0",
        backend.label(),
        parameters.branches,
        parameters.edit_percent,
        layout_before_branches.objects,
        layout_after_branches.objects,
        layout_before_branches.object_value_bytes,
        layout_after_branches.object_value_bytes,
        layout_before_branches.selectors,
        layout_after_branches.selectors,
    );

    measured(
        backend,
        "verify_shared_branch_identities",
        stats,
        path,
        counters,
        parameters,
        async {
            for index in 0..parameters.branches {
                if tree.blob_identity(&branch_name(index)).await? != base_identity {
                    return Err("fanout branch changed the shared blob identity".to_string());
                }
            }
            Ok::<(), String>(())
        },
    )
    .await
    .expect("verify fanout branch identities");

    let edited = measured(
        backend,
        "edit_branches",
        stats,
        path,
        counters,
        parameters,
        async {
            let mut edited = Vec::with_capacity(parameters.edits);
            for index in 0..parameters.edits {
                let source = SpliceSource::new(base.clone(), common_edit.clone(), index as u64);
                let (head, accounting) = tree.ingest_blob(&branch_name(index), source).await?;
                let identity = tree.blob_identity_at_commit(head).await?;
                let sharing = sharing(&base_identity, &identity);
                if sharing.shared_bytes < (SIZE * 3 / 4) as u64 {
                    return Err("fanout edit lost bounded payload sharing".to_string());
                }
                if accounting.reused_chunks < sharing.shared_refs as u64 {
                    return Err("fanout writer underreported reused chunks".to_string());
                }
                edited.push(EditedOracle {
                    branch: branch_name(index),
                    identity,
                    hash: edited_hash(&base, &common_edit, index as u64),
                });
            }
            Ok::<Vec<EditedOracle>, String>(edited)
        },
    )
    .await
    .expect("edit fanout branches");
    let layout_after_edits = tree
        .object_layout_stats()
        .await
        .expect("inventory after fanout edits");
    let shared_refs = edited
        .iter()
        .map(|edit| sharing(&base_identity, &edit.identity).shared_refs)
        .sum::<usize>();
    let shared_bytes = edited
        .iter()
        .map(|edit| sharing(&base_identity, &edit.identity).shared_bytes)
        .sum::<u64>();
    let new_unique_chunks = edited
        .iter()
        .map(|edit| sharing(&base_identity, &edit.identity).new_unique_chunks)
        .sum::<usize>();
    println!(
        "forktree_fanout_sharing,backend={},branches={},edit_percent={},edited_branches={},base_chunks={},shared_refs={},shared_bytes={},new_unique_chunks={},objects_after_branches={},objects_after_edits={},object_bytes_after_edits={}",
        backend.label(),
        parameters.branches,
        parameters.edit_percent,
        parameters.edits,
        base_identity.chunks.len(),
        shared_refs,
        shared_bytes,
        new_unique_chunks,
        layout_after_branches.objects,
        layout_after_edits.objects,
        layout_after_edits.object_value_bytes,
    );

    let selected = parameters.edits.min(3);
    let merge_targets = measured(
        backend,
        "merge_selected",
        stats,
        path,
        counters,
        parameters,
        async {
            let mut targets = Vec::with_capacity(selected);
            for (index, source) in edited.iter().take(selected).enumerate() {
                let target = format!("merge-target-{index:04}");
                tree.create_branch(&target, Some(base_head)).await?;
                let (_, accounting) = tree
                    .merge_blob_branches(&target, &source.branch, base_head)
                    .await?;
                if accounting.logical_bytes != 0 {
                    return Err("fanout merge copied payload bytes".to_string());
                }
                if tree.blob_identity(&target).await? != source.identity {
                    return Err("fanout merge changed the source identity".to_string());
                }
                targets.push(target);
            }
            Ok::<Vec<String>, String>(targets)
        },
    )
    .await
    .expect("merge selected fanout branches");

    measured(
        backend,
        "checkpoints",
        stats,
        path,
        counters,
        parameters,
        async {
            tree.create_checkpoint("fanout-base", base_head).await?;
            let edited_head = tree.branch_head(&edited[0].branch).await?;
            tree.create_checkpoint("fanout-edited", edited_head).await?;
            Ok::<(), String>(())
        },
    )
    .await
    .expect("checkpoint fanout roots");

    print_layout("before_close", layout_after_edits, backend, parameters);
    FanoutOracle {
        base_identity,
        base_hash,
        edited,
        merge_targets,
        layout_after_edits,
    }
}

async fn run_reopen<S>(
    storage: CountingStorage<S>,
    backend: Backend,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    parameters: Parameters,
    oracle: FanoutOracle,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let tree = ForkTree::new(storage);
    measured(
        backend,
        "cold_reopen_samples",
        stats,
        path,
        counters,
        parameters,
        async {
            let edited = &oracle.edited[0];
            let full = tree.read_blob(&edited.branch).await?.materialize();
            if blake3::hash(&full).as_bytes() != &edited.hash {
                return Err("cold fanout edited read mismatch".to_string());
            }
            let range = tree
                .read_blob_range(
                    &edited.branch,
                    EDIT_OFFSET as u64,
                    (EDIT_OFFSET + RANGE_BYTES) as u64,
                )
                .await?
                .materialize();
            if range.len() != RANGE_BYTES {
                return Err("cold fanout range length mismatch".to_string());
            }
            if parameters.edits < parameters.branches {
                let base = tree
                    .read_blob(&branch_name(parameters.edits))
                    .await?
                    .materialize();
                if blake3::hash(&base).as_bytes() != &oracle.base_hash {
                    return Err("cold fanout base read mismatch".to_string());
                }
            }
            Ok::<(), String>(())
        },
    )
    .await
    .expect("cold reopen fanout samples");

    let conflict_accounting = measured(
        backend,
        "selector_epoch_races",
        stats,
        path,
        counters,
        parameters,
        async {
            let conflicts = tree
                .verify_selector_stale_write_and_delete_gc_races()
                .await?;
            tree.verify_publication_gc_races().await?;
            Ok::<_, String>(conflicts)
        },
    )
    .await
    .expect("verify fanout selector/GC races");
    println!(
        "forktree_fanout_races,backend={},branches={},edit_percent={},unrelated_global_epoch_conflicts={},unrelated_writer_success_potential={},same_selector_stale_rejections={},stale_branch_delete_after_gc=rejected,stale_gc_after_branch_delete=rejected,publication_gc_orderings=pass,future_seam=scoped_branch_catalog_upload_conflict_ranges_plus_global_ordered_watermark",
        backend.label(),
        parameters.branches,
        parameters.edit_percent,
        conflict_accounting.unrelated_global_epoch_conflicts,
        conflict_accounting.unrelated_writer_success_potential,
        conflict_accounting.same_selector_stale_rejections
    );

    measured(
        backend,
        "delete_unedited_cohort",
        stats,
        path,
        counters,
        parameters,
        async {
            for index in parameters.edits..parameters.branches {
                tree.delete_branch(&branch_name(index)).await?;
            }
            Ok::<(), String>(())
        },
    )
    .await
    .expect("delete unedited fanout cohort");
    let unedited_gc = measured(
        backend,
        "gc_after_unedited_delete",
        stats,
        path,
        counters,
        parameters,
        tree.reclaim_unreachable(),
    )
    .await
    .expect("GC after unedited fanout delete");
    assert_identity_present(&tree, &oracle.base_identity).await;
    for edit in &oracle.edited {
        assert_identity_present(&tree, &edit.identity).await;
    }

    measured(
        backend,
        "delete_edited_cohort",
        stats,
        path,
        counters,
        parameters,
        async {
            for edit in &oracle.edited {
                tree.delete_branch(&edit.branch).await?;
            }
            for target in &oracle.merge_targets {
                tree.delete_branch(target).await?;
            }
            Ok::<(), String>(())
        },
    )
    .await
    .expect("delete edited fanout cohort");
    let retained_gc = measured(
        backend,
        "gc_with_edited_checkpoint",
        stats,
        path,
        counters,
        parameters,
        tree.reclaim_unreachable(),
    )
    .await
    .expect("GC with edited checkpoint retained");
    assert_identity_present(&tree, &oracle.edited[0].identity).await;

    tree.delete_checkpoint("fanout-edited")
        .await
        .expect("release edited fanout checkpoint");
    let edited_release_gc = measured(
        backend,
        "gc_after_edited_release",
        stats,
        path,
        counters,
        parameters,
        tree.reclaim_unreachable(),
    )
    .await
    .expect("GC after edited fanout release");
    for edit in &oracle.edited {
        assert_dead_only_absent(&tree, &oracle.base_identity, &edit.identity).await;
    }

    measured(
        backend,
        "release_final_base_reference",
        stats,
        path,
        counters,
        parameters,
        async {
            tree.delete_checkpoint("fanout-base").await?;
            tree.ingest_blob(
                "main",
                SingleSpanSource::new(Bytes::from_static(b"fanout-final-live-root")),
            )
            .await?;
            tree.compact_history("main").await?;
            Ok::<(), String>(())
        },
    )
    .await
    .expect("release final fanout base reference");
    let final_gc = measured(
        backend,
        "final_reference_gc",
        stats,
        path,
        counters,
        parameters,
        tree.reclaim_unreachable(),
    )
    .await
    .expect("reclaim final fanout payload reference");
    assert_identity_absent(&tree, &oracle.base_identity).await;
    for edit in &oracle.edited {
        assert_identity_absent(&tree, &edit.identity).await;
    }
    let final_layout = tree
        .object_layout_stats()
        .await
        .expect("inventory final fanout layout");
    println!(
        "forktree_fanout_gc,backend={},branches={},edit_percent={},unedited_reclaimed={},unedited_reclaimed_bytes={},retained_reclaimed={},retained_reclaimed_bytes={},edited_release_reclaimed={},edited_release_reclaimed_bytes={},final_reclaimed={},final_reclaimed_bytes={},objects_before_release={},objects_final={},payload_objects_final={},dead_payload_remaining=0",
        backend.label(),
        parameters.branches,
        parameters.edit_percent,
        unedited_gc.reclaimed_objects,
        unedited_gc.reclaimed_bytes,
        retained_gc.reclaimed_objects,
        retained_gc.reclaimed_bytes,
        edited_release_gc.reclaimed_objects,
        edited_release_gc.reclaimed_bytes,
        final_gc.reclaimed_objects,
        final_gc.reclaimed_bytes,
        oracle.layout_after_edits.objects,
        final_layout.objects,
        final_layout.blob_chunks + final_layout.blob_manifests,
    );
    print_layout("final", final_layout, backend, parameters);
}

async fn assert_identity_present<S>(
    tree: &ForkTree<CountingStorage<S>>,
    blob: &BlobIdentityInventory,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let ids = identity_ids(blob);
    let present = tree
        .present_object_ids(&ids)
        .await
        .expect("load retained fanout identity");
    assert_eq!(
        present.len(),
        ids.len(),
        "retained fanout identity is incomplete"
    );
}

async fn assert_identity_absent<S>(
    tree: &ForkTree<CountingStorage<S>>,
    blob: &BlobIdentityInventory,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let ids = identity_ids(blob);
    assert!(
        tree.present_object_ids(&ids)
            .await
            .expect("load released fanout identity")
            .is_empty(),
        "released fanout identity still has storage objects"
    );
}

async fn assert_dead_only_absent<S>(
    tree: &ForkTree<CountingStorage<S>>,
    base: &BlobIdentityInventory,
    edited: &BlobIdentityInventory,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let base_ids = identity_ids(base).into_iter().collect::<BTreeSet<_>>();
    let dead = identity_ids(edited)
        .into_iter()
        .filter(|id| !base_ids.contains(id))
        .collect::<Vec<_>>();
    assert!(
        tree.present_object_ids(&dead)
            .await
            .expect("load dead-only edited fanout objects")
            .is_empty(),
        "dead-only edited fanout objects survived release"
    );
}

fn identity_ids(blob: &BlobIdentityInventory) -> Vec<[u8; 32]> {
    let mut ids = BTreeSet::from([blob.manifest_object_id]);
    ids.extend(blob.chunks.iter().map(|chunk| chunk.object_id));
    ids.into_iter().collect()
}

#[derive(Clone, Copy)]
struct Sharing {
    shared_refs: usize,
    shared_bytes: u64,
    new_unique_chunks: usize,
}

fn sharing(base: &BlobIdentityInventory, edited: &BlobIdentityInventory) -> Sharing {
    let base_ids = base
        .chunks
        .iter()
        .map(|chunk| (chunk.object_id, chunk.declared_bytes))
        .collect::<BTreeSet<_>>();
    let edited_ids = edited
        .chunks
        .iter()
        .map(|chunk| (chunk.object_id, chunk.declared_bytes))
        .collect::<BTreeSet<_>>();
    let shared = edited
        .chunks
        .iter()
        .filter(|chunk| base_ids.contains(&(chunk.object_id, chunk.declared_bytes)))
        .collect::<Vec<_>>();
    Sharing {
        shared_refs: shared.len(),
        shared_bytes: shared.iter().map(|chunk| chunk.declared_bytes).sum(),
        new_unique_chunks: edited_ids.difference(&base_ids).count(),
    }
}

fn print_blob(phase: &str, accounting: BlobAccounting, parameters: Parameters) {
    println!(
        "forktree_fanout_blob,phase={phase},branches={},edit_percent={},chunks={},reused_chunks={},object_writes={},object_bytes={},logical_bytes={},chunking_us={},source_read_us={},object_hash_us={},dedup_read_us={},publication_us={},peak_buffer_bytes={}",
        parameters.branches,
        parameters.edit_percent,
        accounting.chunks,
        accounting.reused_chunks,
        accounting.object_writes,
        accounting.object_bytes,
        accounting.logical_bytes,
        accounting.chunking_us,
        accounting.source_read_us,
        accounting.object_hash_us,
        accounting.dedup_read_us,
        accounting.publication_us,
        accounting.peak_buffer_bytes,
    );
}

fn print_layout(label: &str, layout: ObjectLayoutStats, backend: Backend, parameters: Parameters) {
    println!(
        "forktree_fanout_layout,label={label},backend={},branches={},edit_percent={},objects={},object_bytes={},reachable={},unreachable={},blob_chunks={},blob_chunk_bytes={},blob_manifests={},blob_manifest_bytes={},commits={},deltas={},selectors={},selector_bytes={}",
        backend.label(),
        parameters.branches,
        parameters.edit_percent,
        layout.objects,
        layout.object_value_bytes,
        layout.reachable_objects,
        layout.unreachable_objects,
        layout.blob_chunks,
        layout.blob_chunk_bytes,
        layout.blob_manifests,
        layout.blob_manifest_bytes,
        layout.commits,
        layout.deltas,
        layout.selectors,
        layout.selector_value_bytes,
    );
}

async fn measured<F, T>(
    backend: Backend,
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
    sampler.join().expect("join fanout RSS sampler");
    let cpu_ticks = process_cpu_ticks().saturating_sub(cpu_ticks_before);
    let cpu_nanos = process_cpu_nanos().saturating_sub(cpu_nanos_before);
    let rss_after = process_resident_bytes();
    let peak_rss = peak.load(Ordering::Acquire);
    let io = take_stats(stats);
    let physical = physical_delta(counters, physical_before);
    let disk_after = directory_bytes(path);
    println!(
        "forktree_fanout_phase,backend={},branches={},edit_percent={},edited_branches={},size_mib={SIZE_MIB},phase={phase},wall_us={wall_us:.3},cpu_ticks={cpu_ticks},cpu_nanos={cpu_nanos},allocated_bytes={allocated_bytes},allocation_calls={allocation_calls},rss_before_bytes={rss_before},rss_after_bytes={rss_after},peak_rss_bytes={peak_rss},begin_reads={},begin_writes={},get_calls={},get_keys={},get_values={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},write_batches={},write_puts={},write_deletes={},write_ranges={},write_bytes={},commits={},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},disk_before_bytes={disk_before},disk_after_bytes={disk_after},disk_growth_bytes={}",
        backend.label(),
        parameters.branches,
        parameters.edit_percent,
        parameters.edits,
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

struct SingleSpanSource {
    logical_bytes: u64,
    spans: VecDeque<Bytes>,
}

impl SingleSpanSource {
    fn new(bytes: Bytes) -> Self {
        Self {
            logical_bytes: bytes.len() as u64,
            spans: VecDeque::from([bytes]),
        }
    }
}

impl SegmentedByteSource for SingleSpanSource {
    fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }

    fn next_span(&mut self) -> Result<Option<Bytes>, String> {
        Ok(self.spans.pop_front())
    }
}

struct SpliceSource {
    spans: VecDeque<Bytes>,
}

impl SpliceSource {
    fn new(base: Bytes, common_edit: Bytes, branch: u64) -> Self {
        let unique = Bytes::copy_from_slice(&branch.to_le_bytes());
        let edit_end = EDIT_OFFSET + EDIT_BYTES;
        Self {
            spans: VecDeque::from([
                base.slice(..EDIT_OFFSET),
                unique,
                common_edit.slice(8..),
                base.slice(edit_end..),
            ]),
        }
    }
}

impl SegmentedByteSource for SpliceSource {
    fn logical_bytes(&self) -> u64 {
        SIZE as u64
    }

    fn next_span(&mut self) -> Result<Option<Bytes>, String> {
        Ok(self.spans.pop_front())
    }
}

fn edited_hash(base: &[u8], common_edit: &[u8], branch: u64) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&base[..EDIT_OFFSET]);
    hasher.update(&branch.to_le_bytes());
    hasher.update(&common_edit[8..]);
    hasher.update(&base[EDIT_OFFSET + EDIT_BYTES..]);
    *hasher.finalize().as_bytes()
}

fn branch_name(index: usize) -> String {
    format!("fanout-{index:04}")
}

fn audio_like_bytes(len: usize, seed: u64) -> Vec<u8> {
    const FRAME_BYTES: usize = 16 * 1024;
    let mut bytes = deterministic_bytes(len, seed ^ 0xa54f_f53a_5f1d_36f1);
    for (frame, chunk) in bytes.chunks_mut(FRAME_BYTES).enumerate() {
        let header = [
            0xff,
            0xfb,
            (frame & 0xff) as u8,
            ((frame >> 8) & 0xff) as u8,
        ];
        chunk[..header.len()].copy_from_slice(&header);
    }
    bytes
}

fn deterministic_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut bytes = vec![0; len];
    let mut state = seed ^ 0xd1b5_4a32_d192_ed03;
    for chunk in bytes.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let generated = state.to_le_bytes();
        chunk.copy_from_slice(&generated[..chunk.len()]);
    }
    bytes
}

fn env_usize(name: &str) -> usize {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} is required"))
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be an integer"))
}
