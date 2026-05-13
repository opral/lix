use std::sync::Arc;

use lix_engine::changelog::bench as changelog_bench;
use lix_engine::changelog::bench::{
    BenchGcStats, BenchRebuildMode, BenchRebuildStats, BenchStore, BenchWriteStats,
};
use lix_engine::{Backend, LixError};

use crate::fixtures::{CorpusFixture, MembershipFanoutFixture, SegmentFixture};

pub(crate) async fn prepare_store(
    backend: Arc<dyn Backend + Send + Sync>,
    fixture: &SegmentFixture,
    publish: bool,
) -> Result<BenchStore, LixError> {
    changelog_bench::prepare_store(backend, &fixture.segment, publish).await
}

pub(crate) async fn prepare_corpus_store(
    backend: Arc<dyn Backend + Send + Sync>,
    fixture: &CorpusFixture,
    publish: bool,
) -> Result<BenchStore, LixError> {
    changelog_bench::prepare_corpus_store(backend, &fixture.corpus, publish).await
}

pub(crate) async fn prepare_rebuild_store(
    backend: Arc<dyn Backend + Send + Sync>,
    fixture: &CorpusFixture,
    mode: BenchRebuildMode,
) -> Result<BenchStore, LixError> {
    changelog_bench::prepare_rebuild_store(backend, &fixture.corpus, mode).await
}

pub(crate) async fn prepare_gc_store(
    backend: Arc<dyn Backend + Send + Sync>,
    live_segments: usize,
    dead_segments: usize,
) -> Result<(BenchStore, String), LixError> {
    changelog_bench::prepare_gc_store(backend, live_segments, dead_segments, 10).await
}

pub(crate) async fn stage_segment(
    backend: Arc<dyn Backend + Send + Sync>,
    fixture: &SegmentFixture,
) -> Result<BenchWriteStats, LixError> {
    changelog_bench::stage_segment_once(backend, &fixture.segment).await
}

pub(crate) async fn stage_segment_raw(
    backend: Arc<dyn Backend + Send + Sync>,
    fixture: &SegmentFixture,
) -> Result<BenchWriteStats, LixError> {
    changelog_bench::stage_segment_raw_once(backend, &fixture.segment).await
}

pub(crate) async fn stage_corpus(
    backend: Arc<dyn Backend + Send + Sync>,
    fixture: &CorpusFixture,
) -> Result<BenchWriteStats, LixError> {
    changelog_bench::stage_corpus_once(backend, &fixture.corpus).await
}

pub(crate) async fn stage_corpus_raw(
    backend: Arc<dyn Backend + Send + Sync>,
    fixture: &CorpusFixture,
) -> Result<BenchWriteStats, LixError> {
    changelog_bench::stage_corpus_raw_once(backend, &fixture.corpus).await
}

pub(crate) async fn stage_incremental_segment(
    backend: Arc<dyn Backend + Send + Sync>,
    fixture: &CorpusFixture,
) -> Result<BenchWriteStats, LixError> {
    changelog_bench::stage_incremental_segment_once(backend, &fixture.corpus).await
}

pub(crate) async fn stage_incremental_segment_raw(
    backend: Arc<dyn Backend + Send + Sync>,
    fixture: &CorpusFixture,
) -> Result<BenchWriteStats, LixError> {
    changelog_bench::stage_incremental_segment_raw_once(backend, &fixture.corpus).await
}

pub(crate) async fn stage_publish_commit(
    backend: Arc<dyn Backend + Send + Sync>,
    fixture: &SegmentFixture,
) -> Result<BenchWriteStats, LixError> {
    changelog_bench::stage_publish_first_commit_once(backend, &fixture.segment).await
}

pub(crate) async fn stage_publish_all_commits(
    backend: Arc<dyn Backend + Send + Sync>,
    fixture: &CorpusFixture,
) -> Result<BenchWriteStats, LixError> {
    changelog_bench::stage_publish_all_commits_once(backend, &fixture.corpus).await
}

pub(crate) async fn load_n_commits_physical(
    store: &BenchStore,
    fixture: &SegmentFixture,
) -> Result<usize, LixError> {
    changelog_bench::load_commits_physical(store, &fixture.commit_ids).await
}

pub(crate) async fn load_corpus_commits_physical(
    store: &BenchStore,
    commit_ids: &[String],
) -> Result<usize, LixError> {
    changelog_bench::load_commits_physical(store, commit_ids).await
}

pub(crate) async fn load_n_commits_visible(
    store: &BenchStore,
    fixture: &SegmentFixture,
) -> Result<usize, LixError> {
    changelog_bench::load_commits_visible(store, &fixture.commit_ids).await
}

pub(crate) async fn load_corpus_commits_visible(
    store: &BenchStore,
    commit_ids: &[String],
) -> Result<usize, LixError> {
    changelog_bench::load_commits_visible(store, commit_ids).await
}

pub(crate) async fn load_n_changes_physical(
    store: &BenchStore,
    fixture: &SegmentFixture,
) -> Result<usize, LixError> {
    changelog_bench::load_changes_physical(store, &fixture.change_ids).await
}

pub(crate) async fn load_corpus_changes_physical(
    store: &BenchStore,
    change_ids: &[String],
) -> Result<usize, LixError> {
    changelog_bench::load_changes_physical(store, change_ids).await
}

pub(crate) async fn load_n_changes_visible(
    store: &BenchStore,
    fixture: &SegmentFixture,
) -> Result<usize, LixError> {
    changelog_bench::load_changes_visible(store, &fixture.change_ids).await
}

pub(crate) async fn load_corpus_changes_visible(
    store: &BenchStore,
    change_ids: &[String],
) -> Result<usize, LixError> {
    changelog_bench::load_changes_visible(store, change_ids).await
}

pub(crate) async fn lookup_by_commit_index(
    store: &BenchStore,
    commit_ids: &[String],
) -> Result<usize, LixError> {
    changelog_bench::lookup_by_commit_index(store, commit_ids).await
}

pub(crate) async fn lookup_by_change_index(
    store: &BenchStore,
    change_ids: &[String],
) -> Result<usize, LixError> {
    changelog_bench::lookup_by_change_index(store, change_ids).await
}

pub(crate) async fn scan_membership_fanout(
    backend: Arc<dyn Backend + Send + Sync>,
    fixture: &MembershipFanoutFixture,
) -> Result<usize, LixError> {
    let store = changelog_bench::prepare_store(backend, &fixture.segment, true).await?;
    changelog_bench::scan_by_change_membership_candidates(&store, &fixture.change_id).await
}

pub(crate) async fn scan_segments_decode(store: &BenchStore) -> Result<usize, LixError> {
    changelog_bench::scan_segments_decode(store).await
}

pub(crate) async fn rebuild_indexes(store: &BenchStore) -> Result<BenchRebuildStats, LixError> {
    changelog_bench::rebuild_mandatory_indexes(store).await
}

pub(crate) async fn plan_gc(
    store: &BenchStore,
    fixture: &SegmentFixture,
) -> Result<BenchGcStats, LixError> {
    changelog_bench::plan_gc(store, fixture.first_commit_id()).await
}

pub(crate) async fn plan_gc_root(
    store: &BenchStore,
    root_commit_id: &str,
) -> Result<BenchGcStats, LixError> {
    changelog_bench::plan_gc(store, root_commit_id).await
}

pub(crate) async fn collect_garbage(
    store: &BenchStore,
    fixture: &SegmentFixture,
) -> Result<BenchGcStats, LixError> {
    changelog_bench::collect_garbage(store, fixture.first_commit_id()).await
}

pub(crate) async fn collect_garbage_root(
    store: &BenchStore,
    root_commit_id: &str,
) -> Result<BenchGcStats, LixError> {
    changelog_bench::collect_garbage(store, root_commit_id).await
}
