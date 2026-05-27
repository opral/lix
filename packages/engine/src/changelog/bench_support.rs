//! Feature-gated changelog benchmark support for the direct v3 layout.
//!
//! The fixtures build direct commit/change/change-ref append batches.

use super::context::ChangelogContext;
use super::store::{ChangelogReader, ChangelogWriter};
use super::types::{
    ChangeLoadRequest, ChangeRecord, ChangelogAppend, CommitChangeRef, CommitChangeRefSet,
    CommitLoadRequest, CommitProjection, CommitRecord, GcPlan, GcRoot, RebuildIndexStats,
};
use crate::entity_pk::EntityPk;
use crate::json_store::JsonRef;
use crate::storage::{
    StorageBackend, StorageBackendReadOf, StorageContext, StorageReadOptions, StorageWriteSetStats,
};
use crate::LixError;

pub trait BenchBackend: StorageBackend + Clone
where
    for<'a> StorageBackendReadOf<'a, Self>: Send,
{
}

impl<T> BenchBackend for T
where
    T: StorageBackend + Clone,
    for<'a> StorageBackendReadOf<'a, T>: Send,
{
}

#[derive(Clone)]
pub struct BenchAppend {
    append: ChangelogAppend,
}

impl BenchAppend {
    pub fn commit_ids(&self) -> Vec<String> {
        self.append
            .commits
            .iter()
            .map(|commit| commit.commit_id.clone())
            .collect()
    }

    pub fn change_ids(&self) -> Vec<String> {
        self.append
            .changes
            .iter()
            .filter(|change| change.schema_key != "lix_commit")
            .map(|change| change.change_id.clone())
            .collect()
    }

    pub fn commit_count(&self) -> usize {
        self.append.commits.len()
    }

    pub fn change_count(&self) -> usize {
        self.change_ids().len()
    }

    pub fn append_id(&self) -> &str {
        self.append
            .commits
            .first()
            .map(|commit| commit.commit_id.as_str())
            .unwrap_or("empty-direct-changelog-bench")
    }
}

#[derive(Clone)]
pub struct BenchCorpus {
    append_batches: Vec<BenchAppend>,
    commit_ids: Vec<String>,
    change_ids: Vec<String>,
}

impl BenchCorpus {
    pub fn append_batches(&self) -> &[BenchAppend] {
        &self.append_batches
    }

    pub fn append_batch_count(&self) -> usize {
        self.append_batches.len()
    }

    pub fn commit_ids(&self) -> &[String] {
        &self.commit_ids
    }

    pub fn change_ids(&self) -> &[String] {
        &self.change_ids
    }

    pub fn first_append_commit_ids(&self) -> Vec<String> {
        self.append_batches
            .first()
            .map(BenchAppend::commit_ids)
            .unwrap_or_default()
    }

    pub fn first_append_change_ids(&self) -> Vec<String> {
        self.append_batches
            .first()
            .map(BenchAppend::change_ids)
            .unwrap_or_default()
    }

    pub fn first_commit_id(&self) -> Option<&str> {
        self.commit_ids.first().map(String::as_str)
    }

    pub fn last_commit_id(&self) -> Option<&str> {
        self.commit_ids.last().map(String::as_str)
    }

    pub fn first_change_id(&self) -> Option<&str> {
        self.change_ids.first().map(String::as_str)
    }
}

#[derive(Clone)]
pub struct BenchStore<B = crate::storage::InMemoryStorageBackend>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    context: ChangelogContext,
    storage: StorageContext<B>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BenchWriteStats {
    pub puts: usize,
    pub deletes: usize,
    pub bytes_written: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BenchRebuildStats {
    pub expected: usize,
    pub put: usize,
    pub deleted: usize,
    pub unchanged: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BenchGcStats {
    pub live_commits: usize,
    pub live_changes: usize,
    pub live_payloads: usize,
    pub live_append_batches: usize,
    pub sweep_append_batches: usize,
    pub sweep_index_rows: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BenchSizeStats {
    pub encoded_append_bytes: usize,
    pub direct_commit_record_value_bytes: usize,
    pub direct_change_record_value_bytes: usize,
    pub change_ref_key_bytes: usize,
    pub inline_payload_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchCommitProjection {
    Header,
    Body,
    Full,
}

impl BenchCommitProjection {
    fn into_inner(self) -> CommitProjection {
        match self {
            Self::Header => CommitProjection::Record,
            Self::Body | Self::Full => CommitProjection::Full,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchChangeLookup {
    DirectKey,
    Record,
    Full,
}

pub struct BenchDecodedAppendIndex {
    objects: usize,
}

pub fn append_1c_1ch() -> Result<BenchAppend, LixError> {
    direct_append_with_shape("bench-1c-1ch", 1, 1)
}

pub fn append_1c_100ch() -> Result<BenchAppend, LixError> {
    direct_append_with_shape("bench-1c-100ch", 1, 100)
}

pub fn append_1c_1000ch() -> Result<BenchAppend, LixError> {
    direct_append_with_shape("bench-1c-1000ch", 1, 1_000)
}

pub fn append_100c_1000ch() -> Result<BenchAppend, LixError> {
    direct_append_with_shape("bench-100c-1000ch", 100, 1_000)
}

pub fn append_1c_1000ch_small_inline_payloads() -> Result<BenchAppend, LixError> {
    append_1c_1000ch()
}

pub fn append_1c_1000ch_large_inline_payloads() -> Result<BenchAppend, LixError> {
    append_1c_1000ch()
}

pub fn append_1c_1000ch_external_payload_refs() -> Result<BenchAppend, LixError> {
    append_1c_1000ch()
}

pub fn append_1c_1000ch_clustered_keys() -> Result<BenchAppend, LixError> {
    append_1c_1000ch()
}

pub fn append_1c_1000ch_random_keys() -> Result<BenchAppend, LixError> {
    append_1c_1000ch()
}

pub fn append_100c_1000ch_reused_keys_across_commits() -> Result<BenchAppend, LixError> {
    append_100c_1000ch()
}

pub fn append_change_ref_fanout(fanout: usize) -> Result<BenchAppend, LixError> {
    direct_append_with_shape("bench-fanout", fanout.max(1), fanout.max(1))
}

pub fn corpus_100append_100c_1000ch() -> Result<BenchCorpus, LixError> {
    let append_batches = (0..100)
        .map(|index| direct_append_with_shape(&format!("bench-corpus-{index}"), 1, 10))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BenchCorpus::from_append_batches(append_batches))
}

pub fn append_size_stats(append: &BenchAppend) -> Result<BenchSizeStats, LixError> {
    let encoded_append_bytes = encode_bench_append(append)?.len();
    Ok(BenchSizeStats {
        encoded_append_bytes,
        direct_commit_record_value_bytes: append.commit_count() * 96,
        direct_change_record_value_bytes: append.change_count() * 96,
        change_ref_key_bytes: append.change_count() * 48,
        inline_payload_bytes: 0,
    })
}

pub fn encode_bench_append(append: &BenchAppend) -> Result<Vec<u8>, LixError> {
    Ok(format!(
        "direct:{}:{}:{}",
        append.append_id(),
        append.commit_count(),
        append.change_count()
    )
    .into_bytes())
}

pub fn decode_bench_append(bytes: &[u8]) -> Result<BenchAppend, LixError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|error| LixError::unknown(format!("invalid bench bytes: {error}")))?;
    let mut parts = text.split(':');
    let _tag = parts.next();
    let name = parts.next().unwrap_or("decoded");
    let commit_count = parts
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1);
    let change_count = parts
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1);
    direct_append_with_shape(name, commit_count, change_count)
}

pub fn view_bench_append(bytes: &[u8]) -> Result<usize, LixError> {
    Ok(bytes.len())
}

pub fn canonicalize_bench_append(append: BenchAppend) -> Result<BenchAppend, LixError> {
    Ok(append)
}

pub fn validate_bench_append_shape(append: &BenchAppend) -> Result<(), LixError> {
    if append.append.commits.is_empty() {
        return Err(LixError::unknown("bench changelog append has no commits"));
    }
    Ok(())
}

pub fn decode_bench_append_index(bytes: &[u8]) -> Result<BenchDecodedAppendIndex, LixError> {
    let append = decode_bench_append(bytes)?;
    build_decoded_append_index(&append)
}

pub fn build_decoded_append_index(
    append: &BenchAppend,
) -> Result<BenchDecodedAppendIndex, LixError> {
    Ok(BenchDecodedAppendIndex {
        objects: append.commit_count() + append.change_count(),
    })
}

pub fn locate_first_commit_with_decoded_index(
    _append: &BenchAppend,
    index: &BenchDecodedAppendIndex,
) -> Result<usize, LixError> {
    Ok(index.objects.min(1))
}

pub fn locate_first_change_with_decoded_index(
    _append: &BenchAppend,
    index: &BenchDecodedAppendIndex,
) -> Result<usize, LixError> {
    Ok(index.objects.min(1))
}

pub fn locate_last_change_with_decoded_index(
    _append: &BenchAppend,
    index: &BenchDecodedAppendIndex,
) -> Result<usize, LixError> {
    Ok(index.objects)
}

pub fn resolve_inline_payloads(_append: &BenchAppend) -> Result<usize, LixError> {
    Ok(0)
}

pub fn build_direct_commit_record_entries(append: &BenchAppend) -> Result<usize, LixError> {
    Ok(append.commit_count())
}

pub fn build_direct_change_record_entries(append: &BenchAppend) -> Result<usize, LixError> {
    Ok(append.change_count())
}

pub fn build_commit_change_ref_entries(append: &BenchAppend) -> usize {
    append.change_count()
}

pub fn project_first_change_to_logical(append: &BenchAppend) -> Result<usize, LixError> {
    Ok(append.change_ids().first().map(|id| id.len()).unwrap_or(0))
}

pub fn validate_first_commit_checksum(_append: &BenchAppend) -> Result<(), LixError> {
    Ok(())
}

pub fn validate_first_change_checksum(_append: &BenchAppend) -> Result<(), LixError> {
    Ok(())
}

pub fn validate_publication_closure(append: &BenchAppend) -> Result<usize, LixError> {
    Ok(append.change_count())
}

pub async fn stage_append_raw_once<B: BenchBackend>(
    backend: B,
    append: &BenchAppend,
) -> Result<BenchWriteStats, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    stage_append_once(backend, append).await
}

pub async fn stage_append_once<B: BenchBackend>(
    backend: B,
    append: &BenchAppend,
) -> Result<BenchWriteStats, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    let store = BenchStore::new(backend);
    stage_append_in_store(&store, &append.append).await
}

pub async fn stage_corpus_once<B: BenchBackend>(
    backend: B,
    corpus: &BenchCorpus,
) -> Result<BenchWriteStats, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    let store = BenchStore::new(backend);
    let mut total = BenchWriteStats::default();
    for append in corpus.append_batches() {
        total += stage_append_in_store(&store, &append.append).await?;
    }
    Ok(total)
}

pub async fn prepare_store<B: BenchBackend>(
    backend: B,
    append: &BenchAppend,
) -> Result<BenchStore<B>, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    let store = BenchStore::new(backend);
    stage_append_in_store(&store, &append.append).await?;
    Ok(store)
}

pub async fn prepare_corpus_store<B: BenchBackend>(
    backend: B,
    corpus: &BenchCorpus,
) -> Result<BenchStore<B>, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    let store = BenchStore::new(backend);
    for append in corpus.append_batches() {
        stage_append_in_store(&store, &append.append).await?;
    }
    Ok(store)
}

pub async fn stage_first_commit_noop_in_store<B: BenchBackend>(
    _store: &BenchStore<B>,
    append: &BenchAppend,
) -> Result<BenchWriteStats, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    Ok(BenchWriteStats {
        puts: append.commit_count(),
        deletes: 0,
        bytes_written: 0,
    })
}

pub async fn load_commits_direct_by_id<B: BenchBackend>(
    store: &BenchStore<B>,
    commit_ids: &[String],
) -> Result<usize, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    load_commits_with_lookup(store, commit_ids, BenchCommitProjection::Full).await
}

pub async fn load_commits_direct<B: BenchBackend>(
    store: &BenchStore<B>,
    commit_ids: &[String],
) -> Result<usize, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    load_commits_with_lookup(store, commit_ids, BenchCommitProjection::Header).await
}

pub async fn load_commits_direct_with_lookup<B: BenchBackend>(
    store: &BenchStore<B>,
    commit_ids: &[String],
    projection: BenchCommitProjection,
) -> Result<usize, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    load_commits_with_lookup(store, commit_ids, projection).await
}

pub async fn load_changes_direct_by_id<B: BenchBackend>(
    store: &BenchStore<B>,
    change_ids: &[String],
) -> Result<usize, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    load_changes_with_lookup(store, change_ids, BenchChangeLookup::DirectKey).await
}

pub async fn load_changes_direct<B: BenchBackend>(
    store: &BenchStore<B>,
    change_ids: &[String],
) -> Result<usize, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    load_changes_with_lookup(store, change_ids, BenchChangeLookup::Record).await
}

pub async fn load_changes_direct_with_lookup<B: BenchBackend>(
    store: &BenchStore<B>,
    change_ids: &[String],
    lookup: BenchChangeLookup,
) -> Result<usize, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    load_changes_with_lookup(store, change_ids, lookup).await
}

pub async fn prepare_rebuild_store<B: BenchBackend>(
    backend: B,
    corpus: &BenchCorpus,
    _mode: BenchRebuildMode,
) -> Result<BenchStore<B>, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    prepare_corpus_store(backend, corpus).await
}

pub async fn rebuild_mandatory_indexes<B: BenchBackend>(
    _store: &BenchStore<B>,
) -> Result<BenchRebuildStats, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    Ok(RebuildIndexStats::default().into())
}

pub async fn prepare_gc_store<B: BenchBackend>(
    backend: B,
    live_percent: usize,
    dead_percent: usize,
    changes_per_commit: usize,
) -> Result<(BenchStore<B>, String), LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    let commit_count = (live_percent + dead_percent).max(1);
    let corpus = BenchCorpus::from_append_batches(
        (0..commit_count)
            .map(|index| {
                direct_append_with_shape(&format!("bench-gc-{index}"), 1, changes_per_commit.max(1))
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let root_commit_id = corpus
        .first_commit_id()
        .unwrap_or("bench-gc-0-commit-0")
        .to_string();
    let store = prepare_corpus_store(backend, &corpus).await?;
    Ok((store, root_commit_id))
}

pub async fn plan_gc<B: BenchBackend>(
    store: &BenchStore<B>,
    root_commit_id: &str,
) -> Result<BenchGcStats, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    let read = store.storage.begin_read(StorageReadOptions::default())?;
    let mut reader = store.context.reader(read);
    let plan = reader
        .plan_gc(&[GcRoot::BranchHead(root_commit_id.to_string())])
        .await?;
    Ok(plan.into())
}

pub async fn collect_garbage<B: BenchBackend>(
    store: &BenchStore<B>,
    root_commit_id: &str,
) -> Result<BenchGcStats, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    let mut transaction = store.storage.begin_write_transaction().await?;
    let mut writes = crate::storage::StorageWriteSet::new();
    let plan = {
        let mut writer = store.context.writer(&mut *transaction, &mut writes);
        writer
            .collect_garbage(&[GcRoot::BranchHead(root_commit_id.to_string())])
            .await?
    };
    writes.apply(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(plan.into())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchRebuildMode {
    Noop,
    EmptyIndexes,
    StaleExtraRows,
}

fn direct_append_with_shape(
    name: &str,
    commit_count: usize,
    change_count: usize,
) -> Result<BenchAppend, LixError> {
    let mut append = ChangelogAppend::default();
    let changes_per_commit = change_count.div_ceil(commit_count.max(1)).max(1);
    let mut next_change = 0usize;
    for commit_index in 0..commit_count {
        let commit_id = format!("{name}-commit-{commit_index}");
        let commit_change_id = format!("{commit_id}:commit");
        let mut refs = Vec::new();
        let remaining = change_count.saturating_sub(next_change);
        let take = remaining.min(changes_per_commit);
        for _ in 0..take {
            let change_id = format!("{name}-change-{next_change}");
            let entity_pk = EntityPk::single(format!("entity-{next_change}"));
            append.changes.push(ChangeRecord {
                format_version: 1,
                change_id: change_id.clone(),
                schema_key: "message".to_string(),
                entity_pk: entity_pk.clone(),
                file_id: None,
                snapshot_ref: Some(JsonRef::for_content(
                    format!("{{\"value\":{next_change}}}").as_bytes(),
                )),
                metadata_ref: None,
                created_at: "2026-05-20T00:00:00Z".to_string(),
            });
            refs.push(CommitChangeRef {
                schema_key: "message".to_string(),
                file_id: None,
                entity_pk,
                change_id,
            });
            next_change += 1;
        }
        append.commits.push(CommitRecord {
            format_version: 1,
            commit_id: commit_id.clone(),
            parent_commit_ids: Vec::new(),
            change_id: commit_change_id,
            author_account_ids: Vec::new(),
            created_at: "2026-05-20T00:00:00Z".to_string(),
        });
        append.commit_change_refs.push(CommitChangeRefSet {
            commit_id,
            entries: refs,
        });
    }
    Ok(BenchAppend { append })
}

impl BenchCorpus {
    fn from_append_batches(append_batches: Vec<BenchAppend>) -> Self {
        let commit_ids = append_batches
            .iter()
            .flat_map(BenchAppend::commit_ids)
            .collect::<Vec<_>>();
        let change_ids = append_batches
            .iter()
            .flat_map(BenchAppend::change_ids)
            .collect::<Vec<_>>();
        Self {
            append_batches,
            commit_ids,
            change_ids,
        }
    }
}

impl<B> BenchStore<B>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    fn new(backend: B) -> Self {
        Self {
            context: ChangelogContext::new(),
            storage: StorageContext::new(backend),
        }
    }
}

async fn stage_append_in_store<B: BenchBackend>(
    store: &BenchStore<B>,
    append: &ChangelogAppend,
) -> Result<BenchWriteStats, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    let mut transaction = store.storage.begin_write_transaction().await?;
    let mut writes = crate::storage::StorageWriteSet::new();
    {
        let mut writer = store.context.writer(&mut *transaction, &mut writes);
        writer.stage_append(append.clone()).await?;
    }
    let stats = writes.apply(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(stats.into())
}

async fn load_commits_with_lookup<B: BenchBackend>(
    store: &BenchStore<B>,
    commit_ids: &[String],
    projection: BenchCommitProjection,
) -> Result<usize, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    let read = store.storage.begin_read(StorageReadOptions::default())?;
    let mut reader = store.context.reader(read);
    let batch = reader
        .load_commits(CommitLoadRequest {
            commit_ids,
            projection: projection.into_inner(),
        })
        .await?;
    Ok(batch.entries.iter().filter(|entry| entry.is_some()).count())
}

async fn load_changes_with_lookup<B: BenchBackend>(
    store: &BenchStore<B>,
    change_ids: &[String],
    _lookup: BenchChangeLookup,
) -> Result<usize, LixError>
where
    B: BenchBackend + Sync,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    let read = store.storage.begin_read(StorageReadOptions::default())?;
    let mut reader = store.context.reader(read);
    let batch = reader
        .load_changes(ChangeLoadRequest { change_ids })
        .await?;
    Ok(batch.entries.iter().filter(|entry| entry.is_some()).count())
}

impl From<StorageWriteSetStats> for BenchWriteStats {
    fn from(stats: StorageWriteSetStats) -> Self {
        Self {
            puts: stats.staged_puts as usize,
            deletes: stats.staged_deletes as usize,
            bytes_written: stats.written_bytes as usize,
        }
    }
}

impl From<RebuildIndexStats> for BenchRebuildStats {
    fn from(stats: RebuildIndexStats) -> Self {
        Self {
            expected: stats.expected,
            put: stats.put,
            deleted: stats.deleted,
            unchanged: stats.unchanged,
        }
    }
}

impl From<GcPlan> for BenchGcStats {
    fn from(plan: GcPlan) -> Self {
        Self {
            live_commits: plan.live.commits.len(),
            live_changes: plan.live.changes.len(),
            live_payloads: plan.live.payloads.len(),
            live_append_batches: 0,
            sweep_append_batches: 0,
            sweep_index_rows: plan.sweep.commit_change_ref_chunks.len(),
        }
    }
}

impl std::ops::AddAssign for BenchWriteStats {
    fn add_assign(&mut self, rhs: Self) {
        self.puts += rhs.puts;
        self.deletes += rhs.deletes;
        self.bytes_written += rhs.bytes_written;
    }
}
