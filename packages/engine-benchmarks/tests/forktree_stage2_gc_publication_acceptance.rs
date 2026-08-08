//! External acceptance gate for the first runnable ForkTree Stage 2 owner.
//!
//! This test deliberately reaches only the sealed, feature-gated acceptance
//! facade. It does not import ForkTree implementation modules, storage spaces,
//! object identities, selector keys, codecs, or maintenance encodings.

use std::path::Path;

use lix::storage_bench::stage2_gc_publication_acceptance::{
    AcceptanceCheck, AcceptancePlan, AcceptanceReport, run_acceptance,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const REQUIRED_CHECKS: &[AcceptanceCheck] = &[
    AcceptanceCheck::OneStorageReadPin,
    AcceptanceCheck::CursorErrorPoisonsView,
    AcceptanceCheck::FreshExclusiveRestart,
    AcceptanceCheck::ExactGlobalEpochCas,
    AcceptanceCheck::ExactProgressCas,
    AcceptanceCheck::ExactOwnerSelectorCas,
    AcceptanceCheck::PersistedMarkPacksBounded,
    AcceptanceCheck::QueueExceedsSixtyFour,
    AcceptanceCheck::CrashReopenResumes,
    AcceptanceCheck::AbandonedUnpublishedObjectsReclaimed,
    AcceptanceCheck::OpenUploadRetained,
    AcceptanceCheck::SharedReferenceRetained,
    AcceptanceCheck::FinalReferenceReclaimed,
    AcceptanceCheck::MalformedSelectorFailsClosed,
    AcceptanceCheck::MissingGraphFailsClosed,
    AcceptanceCheck::MalformedMarkPackFailsClosed,
    AcceptanceCheck::MalformedQueuePackFailsClosed,
    AcceptanceCheck::PublicationFirstFencesGc,
    AcceptanceCheck::GcFirstFencesPublication,
    AcceptanceCheck::SameOwnerStaleWriterRejected,
];

#[tokio::test(flavor = "multi_thread")]
async fn forktree_stage2_gc_publication_acceptance() {
    let backend = std::env::var("FORKTREE_STAGE2_BACKEND").unwrap_or_else(|_| "rocksdb".to_owned());
    let directory = tempfile::tempdir().expect("acceptance directory");
    let path = directory.path().join(&backend);
    let plan = AcceptancePlan::focused();

    let report = match backend.as_str() {
        "rocksdb" => {
            let storage = RocksDB::open(&path).expect("open RocksDB acceptance storage");
            run_acceptance(storage, reopen_rocks(&path), plan)
                .await
                .expect("run RocksDB Stage 2 GC/publication acceptance")
        }
        "slatedb" => {
            let storage = SlateDB::open(&path).expect("open SlateDB acceptance storage");
            run_acceptance(storage, reopen_slate(&path), plan)
                .await
                .expect("run SlateDB Stage 2 GC/publication acceptance")
        }
        value => panic!("FORKTREE_STAGE2_BACKEND must be rocksdb or slatedb, got {value}"),
    };

    assert_acceptance(&backend, report);
}

fn reopen_rocks(
    path: &Path,
) -> impl FnMut() -> Result<RocksDB, lix::storage::StorageError> + Send + 'static {
    let path = path.to_owned();
    move || RocksDB::open(&path)
}

fn reopen_slate(
    path: &Path,
) -> impl FnMut() -> Result<SlateDB, lix::storage::StorageError> + Send + 'static {
    let path = path.to_owned();
    move || SlateDB::open(&path)
}

fn assert_acceptance(backend: &str, report: AcceptanceReport) {
    assert_eq!(
        report.checks.len(),
        REQUIRED_CHECKS.len(),
        "acceptance facade returned an unexpected check set"
    );
    for check in REQUIRED_CHECKS {
        assert!(
            report.checks.contains(check),
            "{backend} omitted required acceptance check {check:?}"
        );
    }

    // The publication must carry the exact opaque authority bytes obtained
    // from one retained StorageRead. The token representation is sealed; only
    // equality is available to this external test.
    assert_eq!(report.authority.pinned, report.authority.prepared);
    assert_eq!(report.metrics.publisher_begin_reads, 1);
    assert!(report.authority.after_gc_page.global_epoch > report.authority.prepared.global_epoch);
    assert_ne!(
        report.authority.after_gc_page.progress,
        report.authority.prepared.progress
    );
    assert_ne!(
        report.authority.after_publication.owner_selector,
        report.authority.pinned.owner_selector
    );

    // Exercise a real queue above the former 64-entry edge and force at least
    // one persisted mark-pack split. Bounds are reported by the owner rather
    // than duplicated by this harness.
    assert!(report.metrics.queue_peak_entries > 64);
    assert!(report.metrics.mark_pack_count >= 2);
    assert!(report.metrics.mark_pack_max_entries <= report.metrics.mark_pack_entry_bound);
    assert!(report.metrics.queue_pack_max_entries <= report.metrics.queue_pack_entry_bound);
    assert!(report.metrics.persisted_gc_pages >= 2);
    assert!(report.metrics.cold_reopens >= 2);
    assert!(report.metrics.abandoned_objects_reclaimed > 0);
    assert!(report.metrics.final_objects_reclaimed > 0);
    assert_eq!(report.metrics.live_objects_deleted, 0);

    println!(
        "forktree_stage2_gc_publication_acceptance backend={backend} checks={} publisher_reads={} queue_peak={} mark_packs={} mark_pack_max={}/{} queue_pack_max={}/{} gc_pages={} cold_reopens={} abandoned_reclaimed={} final_reclaimed={} live_deleted={}",
        report.checks.len(),
        report.metrics.publisher_begin_reads,
        report.metrics.queue_peak_entries,
        report.metrics.mark_pack_count,
        report.metrics.mark_pack_max_entries,
        report.metrics.mark_pack_entry_bound,
        report.metrics.queue_pack_max_entries,
        report.metrics.queue_pack_entry_bound,
        report.metrics.persisted_gc_pages,
        report.metrics.cold_reopens,
        report.metrics.abandoned_objects_reclaimed,
        report.metrics.final_objects_reclaimed,
        report.metrics.live_objects_deleted,
    );
}
