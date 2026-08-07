use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use lix::storage::Storage;
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

use super::model::{BlobIdentityInventory, ForkTree, ObjectLayoutStats, SegmentedByteSource};
use super::{
    Backend, CountingStorage, IoStats, begin_allocation_profile, directory_bytes,
    end_allocation_profile, physical_delta, process_cpu_nanos, process_cpu_ticks,
    process_resident_bytes, settle_rocksdb_compaction, take_stats,
};

const SIZE_MIB: usize = 8;
const SIZE: usize = SIZE_MIB * 1024 * 1024;
const EDIT_BYTES: usize = SIZE / 100;
const EDIT_OFFSET: usize = SIZE / 3 + 12_345;
const RANGE_BYTES: usize = 64 * 1024;
const RANGE_START: usize = EDIT_OFFSET - RANGE_BYTES / 2;
const SEED: u64 = 0x89a3_10fd_4242_73c1;

pub(super) async fn run() {
    let backend = match std::env::args().nth(2).as_deref() {
        Some("rocksdb") | None => Backend::RocksDb,
        Some("slatedb") => Backend::SlateDb,
        Some(other) => panic!("unknown ForkTree multimedia backend '{other}'"),
    };
    match backend {
        Backend::RocksDb => run_rocksdb().await,
        Backend::SlateDb => run_slatedb().await,
    }
}

async fn run_rocksdb() {
    let directory = tempfile::tempdir().expect("create ForkTree multimedia RocksDB directory");
    let oracle = {
        let database = RocksDB::open(directory.path()).expect("open multimedia RocksDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let oracle = run_setup(storage, Backend::RocksDb, &stats, directory.path(), None).await;
        database
            .flush()
            .expect("flush multimedia RocksDB before close");
        println!(
            "forktree_multimedia_close,backend=rocksdb,disk_bytes={}",
            directory_bytes(directory.path())
        );
        oracle
    };

    let database = RocksDB::open(directory.path()).expect("reopen multimedia RocksDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    run_reopen(
        storage,
        Backend::RocksDb,
        &stats,
        directory.path(),
        None,
        oracle,
    )
    .await;
    database
        .flush()
        .expect("flush multimedia RocksDB final state");
    let post_flush = directory_bytes(directory.path());
    drop(database);
    let settled = settle_rocksdb_compaction(directory.path());
    println!(
        "forktree_multimedia_settled,backend=rocksdb,post_flush_disk_bytes={post_flush},post_compaction_disk_bytes={settled}"
    );
}

async fn run_slatedb() {
    let directory = tempfile::tempdir().expect("create ForkTree multimedia SlateDB directory");
    let oracle = {
        let counters = SlateDBIoCounters::default();
        let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
            .expect("open multimedia SlateDB");
        let (storage, stats) = CountingStorage::new(database.clone());
        let oracle = run_setup(
            storage,
            Backend::SlateDb,
            &stats,
            directory.path(),
            Some(&counters),
        )
        .await;
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush multimedia SlateDB before close");
        println!(
            "forktree_multimedia_close,backend=slatedb,disk_bytes={}",
            directory_bytes(directory.path())
        );
        oracle
    };

    let counters = SlateDBIoCounters::default();
    let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
        .expect("reopen multimedia SlateDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    run_reopen(
        storage,
        Backend::SlateDb,
        &stats,
        directory.path(),
        Some(&counters),
        oracle,
    )
    .await;
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush multimedia SlateDB final state");
    println!(
        "forktree_multimedia_settled,backend=slatedb,post_flush_disk_bytes={},post_compaction_disk_bytes=not_applicable",
        directory_bytes(directory.path())
    );
}

#[derive(Clone)]
struct AcceptanceOracle {
    base_hash: [u8; 32],
    edited_hash: [u8; 32],
    base_range: Vec<u8>,
    edited_range: Vec<u8>,
    base_identity: BlobIdentityInventory,
    edited_identity: BlobIdentityInventory,
}

async fn run_setup<S>(
    storage: CountingStorage<S>,
    backend: Backend,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) -> AcceptanceOracle
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let payload = PreparedImage::new();
    let tree = ForkTree::new(storage);
    tree.initialize(&[(b"multimedia-oracle".to_vec(), b"v1".to_vec())])
        .await
        .expect("initialize ForkTree multimedia authority");
    let _ = take_stats(stats);

    let (base_head, ingest) = measured(
        backend,
        "ingest",
        stats,
        path,
        counters,
        tree.ingest_blob("main", SingleSpanSource::new(payload.base.clone())),
    )
    .await
    .expect("ingest 8 MiB image fixture");
    print_blob_accounting("ingest", ingest);
    let base_identity = tree
        .blob_identity_at_commit(base_head)
        .await
        .expect("load base blob identity");
    assert_eq!(base_identity.logical_bytes, SIZE as u64);
    print_identity("base", &base_identity);
    print_layout(
        "after_ingest",
        tree.object_layout_stats()
            .await
            .expect("inventory ingest layout"),
    );

    let before_reads = tree
        .authority_fingerprint()
        .await
        .expect("fingerprint before reads");
    let full = measured(backend, "full_read", stats, path, counters, async {
        tree.read_blob("main")
            .await
            .map(|bytes| bytes.materialize())
    })
    .await
    .expect("full-read base image");
    assert_eq!(blake3::hash(&full).as_bytes(), &payload.base_hash);
    let range = measured(backend, "range_read_64k", stats, path, counters, async {
        tree.read_blob_range(
            "main",
            RANGE_START as u64,
            (RANGE_START + RANGE_BYTES) as u64,
        )
        .await
        .map(|bytes| bytes.materialize())
    })
    .await
    .expect("range-read base image");
    assert_eq!(range, payload.base_range);
    let after_reads = tree
        .authority_fingerprint()
        .await
        .expect("fingerprint after reads");
    assert_eq!(
        before_reads, after_reads,
        "reads mutated ForkTree authority"
    );

    measured(
        backend,
        "checkpoint_base",
        stats,
        path,
        counters,
        tree.create_checkpoint("base-retained", base_head),
    )
    .await
    .expect("checkpoint base image root");
    measured(
        backend,
        "branch",
        stats,
        path,
        counters,
        tree.create_branch("source", Some(base_head)),
    )
    .await
    .expect("branch image root");
    assert_eq!(
        tree.blob_identity("source").await.expect("branch identity"),
        base_identity,
        "branch duplicated or rewrote the blob identity"
    );

    let (edited_head, edit) = measured(
        backend,
        "localized_edit_1pct",
        stats,
        path,
        counters,
        tree.ingest_blob("source", SingleSpanSource::new(payload.edited.clone())),
    )
    .await
    .expect("publish 1% image edit");
    print_blob_accounting("localized_edit_1pct", edit);
    let edited_identity = tree
        .blob_identity_at_commit(edited_head)
        .await
        .expect("load edited blob identity");
    let sharing = sharing(&base_identity, &edited_identity);
    assert!(sharing.shared_chunks > 0, "localized edit shared no chunks");
    assert!(
        sharing.shared_declared_bytes > SIZE as u64 / 2,
        "localized edit failed the smallest credible sharing gate"
    );
    assert_eq!(edit.reused_chunks, sharing.shared_chunks as u64);
    println!(
        "forktree_multimedia_sharing,size_mib={SIZE_MIB},edit_bytes={EDIT_BYTES},edit_offset={EDIT_OFFSET},base_chunks={},edited_chunks={},shared_chunks={},shared_declared_bytes={},new_unique_chunks={},base_hash={},edited_hash={}",
        base_identity.chunks.len(),
        edited_identity.chunks.len(),
        sharing.shared_chunks,
        sharing.shared_declared_bytes,
        sharing.new_unique_chunks,
        hex(&payload.base_hash),
        hex(&payload.edited_hash),
    );

    let diff = measured(
        backend,
        "diff",
        stats,
        path,
        counters,
        tree.diff_blob_commits(base_head, edited_head),
    )
    .await
    .expect("diff image edit");
    assert_eq!(diff.shared_chunks as usize, sharing.shared_chunks);
    assert!(diff.changed_chunks > 0);
    println!(
        "forktree_multimedia_diff,before_chunks={},after_chunks={},shared_chunks={},changed_chunks={}",
        diff.before_chunks, diff.after_chunks, diff.shared_chunks, diff.changed_chunks
    );

    let (merged_head, merge) = measured(
        backend,
        "merge",
        stats,
        path,
        counters,
        tree.merge_blob_branches("main", "source", base_head),
    )
    .await
    .expect("merge image branch");
    assert_eq!(merge.logical_bytes, 0, "merge copied blob payload bytes");
    assert_eq!(
        tree.blob_identity("main").await.expect("merged identity"),
        edited_identity,
        "merge did not reuse the source blob identity"
    );
    measured(
        backend,
        "checkpoint_merged",
        stats,
        path,
        counters,
        tree.create_checkpoint("merged", merged_head),
    )
    .await
    .expect("checkpoint merged image root");
    print_layout(
        "before_close",
        tree.object_layout_stats()
            .await
            .expect("inventory pre-close layout"),
    );

    AcceptanceOracle {
        base_hash: payload.base_hash,
        edited_hash: payload.edited_hash,
        base_range: payload.base_range,
        edited_range: payload.edited_range,
        base_identity,
        edited_identity,
    }
}

async fn run_reopen<S>(
    storage: CountingStorage<S>,
    backend: Backend,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
    oracle: AcceptanceOracle,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let tree = ForkTree::new(storage);
    measured(backend, "cold_reopen_reads", stats, path, counters, async {
        let full = tree.read_blob("main").await?.materialize();
        if blake3::hash(&full).as_bytes() != &oracle.edited_hash {
            return Err("cold full read hash mismatch".to_string());
        }
        let range = tree
            .read_blob_range(
                "main",
                RANGE_START as u64,
                (RANGE_START + RANGE_BYTES) as u64,
            )
            .await?
            .materialize();
        if range != oracle.edited_range {
            return Err("cold range read mismatch".to_string());
        }
        Ok(())
    })
    .await
    .expect("cold reopen image reads");

    tree.delete_branch("source")
        .await
        .expect("delete source branch");
    tree.compact_history("main")
        .await
        .expect("cut main history at retained-root boundary");
    let retained = measured(
        backend,
        "retained_root_gc",
        stats,
        path,
        counters,
        tree.reclaim_unreachable(),
    )
    .await
    .expect("reclaim with base checkpoint retained");
    println!(
        "forktree_multimedia_gc,phase=retained,roots={},reachable={},scanned={},reclaimed={},reclaimed_bytes={},pages={},peak_frontier={}",
        retained.roots,
        retained.reachable_objects,
        retained.scanned_objects,
        retained.reclaimed_objects,
        retained.reclaimed_bytes,
        retained.pages,
        retained.peak_frontier,
    );
    assert_identity_present(&tree, &oracle.base_identity).await;

    let base_head = tree
        .checkpoint_head("base-retained")
        .await
        .expect("load retained base root");
    tree.create_branch("retained-base", Some(base_head))
        .await
        .expect("open retained base root");
    measured(
        backend,
        "retained_root_read",
        stats,
        path,
        counters,
        async {
            let full = tree.read_blob("retained-base").await?.materialize();
            if blake3::hash(&full).as_bytes() != &oracle.base_hash {
                return Err("retained full read hash mismatch".to_string());
            }
            let range = tree
                .read_blob_range(
                    "retained-base",
                    RANGE_START as u64,
                    (RANGE_START + RANGE_BYTES) as u64,
                )
                .await?
                .materialize();
            if range != oracle.base_range {
                return Err("retained range read mismatch".to_string());
            }
            Ok(())
        },
    )
    .await
    .expect("read retained base root");

    let edited_chunk_ids = oracle
        .edited_identity
        .chunks
        .iter()
        .map(|chunk| chunk.object_id)
        .collect::<BTreeSet<_>>();
    let mut dead_only = vec![oracle.base_identity.manifest_object_id];
    dead_only.extend(
        oracle
            .base_identity
            .chunks
            .iter()
            .map(|chunk| chunk.object_id)
            .filter(|id| !edited_chunk_ids.contains(id)),
    );
    assert_eq!(
        tree.present_object_ids(&dead_only)
            .await
            .expect("pre-release dead-only presence")
            .len(),
        dead_only.len(),
        "dead-only retained objects are absent before final release"
    );

    tree.delete_branch("retained-base")
        .await
        .expect("release retained base branch");
    tree.delete_checkpoint("base-retained")
        .await
        .expect("release base checkpoint");
    tree.delete_checkpoint("merged")
        .await
        .expect("release merged checkpoint");
    let reclaimed = measured(
        backend,
        "final_root_release_gc",
        stats,
        path,
        counters,
        tree.reclaim_unreachable(),
    )
    .await
    .expect("reclaim final retained root");
    assert!(reclaimed.reclaimed_objects > 0);
    assert!(reclaimed.reclaimed_bytes > 0);
    let leaked = tree
        .present_object_ids(&dead_only)
        .await
        .expect("post-release dead-only presence");
    assert!(
        leaked.is_empty(),
        "dead-only ForkTree objects survived final release: {leaked:?}"
    );
    assert_identity_present(&tree, &oracle.edited_identity).await;
    let final_full = tree
        .read_blob("main")
        .await
        .expect("read live image after final reclamation")
        .materialize();
    assert_eq!(blake3::hash(&final_full).as_bytes(), &oracle.edited_hash);
    println!(
        "forktree_multimedia_gc,phase=released,roots={},reachable={},scanned={},reclaimed={},reclaimed_bytes={},pages={},peak_frontier={},dead_only_objects={},dead_only_remaining=0",
        reclaimed.roots,
        reclaimed.reachable_objects,
        reclaimed.scanned_objects,
        reclaimed.reclaimed_objects,
        reclaimed.reclaimed_bytes,
        reclaimed.pages,
        reclaimed.peak_frontier,
        dead_only.len(),
    );
    print_layout(
        "after_final_release",
        tree.object_layout_stats()
            .await
            .expect("inventory final layout"),
    );
}

async fn assert_identity_present<S>(
    tree: &ForkTree<CountingStorage<S>>,
    blob: &BlobIdentityInventory,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut ids = vec![blob.manifest_object_id];
    ids.extend(blob.chunks.iter().map(|chunk| chunk.object_id));
    let present = tree
        .present_object_ids(&ids)
        .await
        .expect("load blob object presence");
    assert_eq!(
        present.len(),
        ids.len(),
        "retained blob closure is incomplete"
    );
}

#[derive(Clone, Copy, Debug)]
struct Sharing {
    shared_chunks: usize,
    shared_declared_bytes: u64,
    new_unique_chunks: usize,
}

fn sharing(base: &BlobIdentityInventory, edited: &BlobIdentityInventory) -> Sharing {
    let base_by_id = base
        .chunks
        .iter()
        .map(|chunk| (chunk.object_id, chunk.declared_bytes))
        .collect::<BTreeMap<_, _>>();
    let edited_ids = edited
        .chunks
        .iter()
        .map(|chunk| chunk.object_id)
        .collect::<BTreeSet<_>>();
    let base_ids = base_by_id.keys().copied().collect::<BTreeSet<_>>();
    let shared = edited
        .chunks
        .iter()
        .filter(|chunk| base_by_id.get(&chunk.object_id) == Some(&chunk.declared_bytes))
        .collect::<Vec<_>>();
    Sharing {
        shared_chunks: shared.len(),
        shared_declared_bytes: shared.iter().map(|chunk| chunk.declared_bytes).sum(),
        new_unique_chunks: edited_ids.difference(&base_ids).count(),
    }
}

fn print_identity(label: &str, identity: &BlobIdentityInventory) {
    println!(
        "forktree_multimedia_identity,label={label},manifest={},logical_bytes={},chunks={},chunk_bytes={}",
        hex(&identity.manifest_object_id),
        identity.logical_bytes,
        identity.chunks.len(),
        identity
            .chunks
            .iter()
            .map(|chunk| chunk.declared_bytes)
            .sum::<u64>(),
    );
}

fn print_blob_accounting(phase: &str, accounting: super::model::BlobAccounting) {
    println!(
        "forktree_multimedia_blob,phase={phase},chunks={},reused_chunks={},locality_hits={},locality_misses={},object_writes={},object_bytes={},logical_bytes={},chunking_us={},source_read_us={},object_hash_us={},object_encode_us={},dedup_read_us={},emission_us={},publication_us={},emission_batches={},peak_buffer_bytes={}",
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
        accounting.peak_buffer_bytes,
    );
}

fn print_layout(label: &str, stats: ObjectLayoutStats) {
    println!(
        "forktree_multimedia_layout,label={label},objects={},object_value_bytes={},reachable_objects={},unreachable_objects={},blob_chunks={},blob_chunk_bytes={},blob_manifests={},blob_manifest_bytes={},commits={},deltas={},selectors={},selector_value_bytes={}",
        stats.objects,
        stats.object_value_bytes,
        stats.reachable_objects,
        stats.unreachable_objects,
        stats.blob_chunks,
        stats.blob_chunk_bytes,
        stats.blob_manifests,
        stats.blob_manifest_bytes,
        stats.commits,
        stats.deltas,
        stats.selectors,
        stats.selector_value_bytes,
    );
}

async fn measured<F, T>(
    backend: Backend,
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
    sampler
        .join()
        .expect("join ForkTree multimedia RSS sampler");
    let cpu_ticks = process_cpu_ticks().saturating_sub(cpu_ticks_before);
    let cpu_nanos = process_cpu_nanos().saturating_sub(cpu_nanos_before);
    let rss_after = process_resident_bytes();
    let peak_rss = peak.load(Ordering::Acquire);
    let io = take_stats(stats);
    let physical = physical_delta(counters, physical_before);
    let disk_after = directory_bytes(path);
    println!(
        "forktree_multimedia_phase,backend={},size_mib={SIZE_MIB},family=image,phase={phase},wall_us={wall_us:.3},cpu_ticks={cpu_ticks},cpu_nanos={cpu_nanos},allocated_bytes={allocated_bytes},allocation_calls={allocation_calls},rss_before_bytes={rss_before},rss_after_bytes={rss_after},peak_rss_bytes={peak_rss},begin_reads={},begin_writes={},get_calls={},get_keys={},get_values={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},write_batches={},write_puts={},write_deletes={},write_ranges={},write_bytes={},commits={},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},disk_before_bytes={disk_before},disk_after_bytes={disk_after},disk_growth_bytes={}",
        backend.label(),
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

struct PreparedImage {
    base: Bytes,
    edited: Bytes,
    base_hash: [u8; 32],
    edited_hash: [u8; 32],
    base_range: Vec<u8>,
    edited_range: Vec<u8>,
}

impl PreparedImage {
    fn new() -> Self {
        let base = image_like_bytes(0, SIZE, SEED);
        let edit = image_like_bytes(EDIT_OFFSET, EDIT_BYTES, SEED ^ 0x6a09_e667_f3bc_c909);
        let mut edited = base.clone();
        edited[EDIT_OFFSET..EDIT_OFFSET + EDIT_BYTES].copy_from_slice(&edit);
        let base_hash = *blake3::hash(&base).as_bytes();
        let edited_hash = *blake3::hash(&edited).as_bytes();
        let base_range = base[RANGE_START..RANGE_START + RANGE_BYTES].to_vec();
        let edited_range = edited[RANGE_START..RANGE_START + RANGE_BYTES].to_vec();
        Self {
            base: Bytes::from(base),
            edited: Bytes::from(edited),
            base_hash,
            edited_hash,
            base_range,
            edited_range,
        }
    }
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

fn image_like_bytes(global_offset: usize, len: usize, seed: u64) -> Vec<u8> {
    let mut bytes = deterministic_bytes(len, seed ^ 0xbb67_ae85_84ca_a73b ^ global_offset as u64);
    for (index, byte) in bytes.iter_mut().enumerate() {
        let position = global_offset + index;
        let block = position / 4096;
        if block % 4 != 3 {
            let pixel = (position % 4096) / 4;
            let channel = position % 4;
            *byte = match channel {
                0 => (pixel as u8).wrapping_add(seed as u8),
                1 => ((pixel / 4) as u8).wrapping_add((seed >> 8) as u8),
                2 => (block as u8)
                    .wrapping_mul(3)
                    .wrapping_add((seed >> 16) as u8),
                _ => 0xff,
            };
        }
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

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}
