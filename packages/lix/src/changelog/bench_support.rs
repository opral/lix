//! Feature-gated changelog benchmark support for the direct header/change layout.
//!
//! The fixtures build direct commit-header and standalone-change append batches.

use std::time::{Duration, Instant};

use super::context::ChangelogContext;
use super::store::{ChangelogReader, ChangelogWriter};
use super::types::{
    ChangeId, ChangeLoadRequest, ChangeRecord, ChangelogAppend, CommitId, CommitLoadRequest,
    CommitRecord, RebuildIndexStats,
};
use crate::LixError;
use crate::row_pk::RowPk;
use crate::storage_adapter::Storage;
use crate::storage_adapter::{StorageAdapter, StorageReadOptions, StorageWriteSetStats};

pub trait BenchStorage: Storage + Clone {}

impl<T> BenchStorage for T where T: Storage + Clone {}

#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct BenchAppend {
    append: ChangelogAppend,
}

impl BenchAppend {
    pub fn commit_ids(&self) -> Vec<String> {
        self.append
            .commits
            .iter()
            .map(|commit| commit.commit_id.to_string())
            .collect()
    }

    pub fn change_ids(&self) -> Vec<String> {
        self.append
            .changes
            .iter()
            .map(|change| change.change_id.to_string())
            .collect()
    }

    pub fn commit_count(&self) -> usize {
        self.append.commits.len()
    }

    pub fn change_count(&self) -> usize {
        self.append.changes.len()
    }

    pub fn append_id(&self) -> String {
        self.append
            .commits
            .first()
            .map(|commit| commit.commit_id.to_string())
            .unwrap_or_else(|| "empty-direct-changelog-bench".to_string())
    }
}

#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct BenchCorpus {
    append_batches: Vec<BenchAppend>,
    commit_ids: Vec<CommitId>,
    change_ids: Vec<ChangeId>,
}

impl BenchCorpus {
    pub fn append_batches(&self) -> &[BenchAppend] {
        &self.append_batches
    }

    pub fn append_batch_count(&self) -> usize {
        self.append_batches.len()
    }

    pub fn commit_ids(&self) -> Vec<String> {
        self.commit_ids.iter().map(ToString::to_string).collect()
    }

    pub fn change_ids(&self) -> Vec<String> {
        self.change_ids.iter().map(ToString::to_string).collect()
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

    pub fn first_commit_id(&self) -> Option<String> {
        self.commit_ids.first().map(ToString::to_string)
    }

    pub fn last_commit_id(&self) -> Option<String> {
        self.commit_ids.last().map(ToString::to_string)
    }

    pub fn first_change_id(&self) -> Option<String> {
        self.change_ids.first().map(ToString::to_string)
    }
}

#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct BenchStore<StorageImpl = crate::storage_adapter::Memory>
where
    StorageImpl: BenchStorage + Sync,
{
    context: ChangelogContext,
    storage: StorageAdapter<StorageImpl>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BenchWriteStats {
    pub puts: usize,
    pub deletes: usize,
    pub bytes_written: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BenchAppendTiming {
    pub stage_elapsed: Duration,
    pub commit_elapsed: Duration,
    pub write: BenchWriteStats,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BenchRebuildStats {
    pub expected: usize,
    pub put: usize,
    pub deleted: usize,
    pub unchanged: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BenchSizeStats {
    pub encoded_append_bytes: usize,
    pub direct_commit_record_value_bytes: usize,
    pub direct_change_record_value_bytes: usize,
    pub inline_payload_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchChangeLookup {
    DirectKey,
    Record,
    Full,
}

#[derive(Clone, Copy, Debug)]
pub struct BenchDecodedAppendIndex {
    pub objects: usize,
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

pub fn append_with_shape(
    name: &str,
    commit_count: usize,
    change_count: usize,
) -> Result<BenchAppend, LixError> {
    direct_append_with_shape(name, commit_count, change_count)
}

/// Builds commit-only public facts with production-shaped monotonic UUIDv7
/// keys. This avoids turning benchmark label hashing into random-LSM ingest.
pub fn append_ordered_commits(
    first_commit_index: usize,
    commit_count: usize,
) -> Result<BenchAppend, LixError> {
    let mut append = ChangelogAppend::default();
    append.commits.reserve(commit_count);
    for offset in 0..commit_count {
        let commit_index = first_commit_index
            .checked_add(offset)
            .ok_or_else(|| LixError::unknown("ordered benchmark commit index overflow"))?;
        let commit_id = CommitId::with_change_address_space(ordered_bench_uuid(commit_index, 0));
        append.commits.push(CommitRecord {
            touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
            format_version: 4,
            commit_id,
            generation: 0,
            parent_commit_ids: Vec::new(),
            first_parent_jump_commit_id: commit_id,
            first_parent_jump_span: 0,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: crate::common::LixTimestamp::expect_parse(
                "created_at",
                "2026-05-20T00:00:00Z",
            ),
        });
    }
    Ok(BenchAppend { append })
}

/// Builds one production-shaped first-parent chain for topology benchmarks.
pub fn append_ordered_linear_commits(commit_count: usize) -> Result<BenchAppend, LixError> {
    let mut append = ChangelogAppend::default();
    append.commits.reserve(commit_count);
    let mut parent_commit_id = None;
    for commit_index in 0..commit_count {
        let commit_id = CommitId::with_change_address_space(ordered_bench_uuid(commit_index, 0));
        let parent_commit_ids = parent_commit_id.into_iter().collect::<Vec<_>>();
        let parent = append.commits.last();
        let parent_jump = parent.map(|parent| {
            &append.commits[usize::try_from(parent.generation - parent.first_parent_jump_span)
                .expect("benchmark jump generation fits usize")]
        });
        let (first_parent_jump_commit_id, first_parent_jump_span) =
            super::next_first_parent_jump(commit_id, &parent_commit_ids, parent, parent_jump)?;
        append.commits.push(CommitRecord {
            touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
            format_version: 4,
            commit_id,
            generation: u64::try_from(commit_index).expect("benchmark commit index fits u64"),
            parent_commit_ids,
            first_parent_jump_commit_id,
            first_parent_jump_span,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: crate::common::LixTimestamp::expect_parse(
                "created_at",
                "2026-05-20T00:00:00Z",
            ),
        });
        parent_commit_id = Some(commit_id);
    }
    Ok(BenchAppend { append })
}

/// Builds a one-commit append whose commit — and therefore whose derived
/// commit change id — is pinned to `commit_label`.
///
/// A commit's change id is `commit_id` at ordinal zero of its own change
/// address space, so pinning the commit id pins both.
pub fn append_1c_with_commit_id(name: &str, commit_label: &str) -> Result<BenchAppend, LixError> {
    let mut append = direct_append_with_shape(name, 1, 1)?;
    append.append.commits[0].commit_id =
        CommitId::with_change_address_space(*CommitId::for_test_label(commit_label).as_uuid());
    Ok(append)
}

pub fn corpus_100append_100c_1000ch() -> Result<BenchCorpus, LixError> {
    let append_batches = (0..100)
        .map(|index| direct_append_with_shape(&format!("bench-corpus-{index}"), 1, 10))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BenchCorpus::from_append_batches(append_batches))
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

pub fn validate_bench_append_shape(append: &BenchAppend) -> Result<(), LixError> {
    if append.append.commits.is_empty() {
        return Err(LixError::unknown("bench changelog append has no commits"));
    }
    Ok(())
}

pub fn build_decoded_append_index(
    append: &BenchAppend,
) -> Result<BenchDecodedAppendIndex, LixError> {
    Ok(BenchDecodedAppendIndex {
        objects: append.commit_count() + append.change_count(),
    })
}

pub fn build_direct_change_record_entries(append: &BenchAppend) -> Result<usize, LixError> {
    Ok(append.change_count())
}

pub async fn stage_append_raw_once<StorageImpl>(
    storage: StorageImpl,
    append: &BenchAppend,
) -> Result<BenchWriteStats, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    stage_append_once(storage, append).await
}

pub async fn stage_append_once<StorageImpl>(
    storage: StorageImpl,
    append: &BenchAppend,
) -> Result<BenchWriteStats, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    let store = BenchStore::new(storage);
    stage_append_to_store(&store, append).await
}

pub async fn stage_append_to_store<StorageImpl>(
    store: &BenchStore<StorageImpl>,
    append: &BenchAppend,
) -> Result<BenchWriteStats, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    Ok(stage_append_timed_to_store(store, append).await?.write)
}

pub async fn stage_append_timed_to_store<StorageImpl>(
    store: &BenchStore<StorageImpl>,
    append: &BenchAppend,
) -> Result<BenchAppendTiming, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    stage_append_timed_in_store(store, &append.append).await
}

pub async fn layout_accounting<StorageImpl>(
    store: &BenchStore<StorageImpl>,
) -> Result<Vec<crate::storage_bench::StorageLayoutAccounting>, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    let read = store
        .storage
        .begin_read(StorageReadOptions::default())
        .await?;
    Ok(crate::storage_bench::layout_accounting(&read).await)
}

pub async fn stage_corpus_once<StorageImpl>(
    storage: StorageImpl,
    corpus: &BenchCorpus,
) -> Result<BenchWriteStats, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    let store = BenchStore::new(storage);
    let mut total = BenchWriteStats::default();
    for append in corpus.append_batches() {
        total += stage_append_in_store(&store, &append.append).await?;
    }
    Ok(total)
}

pub async fn prepare_store<StorageImpl>(
    storage: StorageImpl,
    append: &BenchAppend,
) -> Result<BenchStore<StorageImpl>, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    let store = BenchStore::new(storage);
    stage_append_in_store(&store, &append.append).await?;
    Ok(store)
}

pub async fn prepare_corpus_store<StorageImpl>(
    storage: StorageImpl,
    corpus: &BenchCorpus,
) -> Result<BenchStore<StorageImpl>, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    let store = BenchStore::new(storage);
    for append in corpus.append_batches() {
        stage_append_in_store(&store, &append.append).await?;
    }
    Ok(store)
}

pub async fn stage_first_commit_noop_in_store<StorageImpl>(
    _store: &BenchStore<StorageImpl>,
    append: &BenchAppend,
) -> Result<BenchWriteStats, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    Ok(BenchWriteStats {
        puts: append.commit_count(),
        deletes: 0,
        bytes_written: 0,
    })
}

pub async fn load_commits_direct<StorageImpl, S: AsRef<str> + Sync>(
    store: &BenchStore<StorageImpl>,
    commit_ids: &[S],
) -> Result<usize, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    load_commits_with_lookup(store, commit_ids).await
}

pub async fn load_changes_direct_by_id<StorageImpl, S: AsRef<str> + Sync>(
    store: &BenchStore<StorageImpl>,
    change_ids: &[S],
) -> Result<usize, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    load_changes_with_lookup(store, change_ids, BenchChangeLookup::DirectKey).await
}

pub async fn load_changes_direct<StorageImpl, S: AsRef<str> + Sync>(
    store: &BenchStore<StorageImpl>,
    change_ids: &[S],
) -> Result<usize, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    load_changes_with_lookup(store, change_ids, BenchChangeLookup::Record).await
}

pub async fn prepare_rebuild_store<StorageImpl>(
    storage: StorageImpl,
    corpus: &BenchCorpus,
    _mode: BenchRebuildMode,
) -> Result<BenchStore<StorageImpl>, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    prepare_corpus_store(storage, corpus).await
}

pub async fn rebuild_mandatory_indexes<StorageImpl>(
    _store: &BenchStore<StorageImpl>,
) -> Result<BenchRebuildStats, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    Ok(RebuildIndexStats::default().into())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchRebuildMode {
    Noop,
    EmptyIndexes,
    StaleExtraRows,
}

#[expect(clippy::unnecessary_wraps)]
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
        // Real commits are minted through `with_change_address_space`; fixtures
        // must match or their derived commit change id lands inside the packed
        // change range.
        let typed_commit_id =
            CommitId::with_change_address_space(*CommitId::for_test_label(&commit_id).as_uuid());
        let remaining = change_count.saturating_sub(next_change);
        let take = remaining.min(changes_per_commit);
        for _ in 0..take {
            let change_id = format!("{name}-change-{next_change}");
            let typed_change_id = ChangeId::for_test_label(&change_id);
            let row_pk = RowPk::single(format!("row-{next_change}"));
            append.changes.push(ChangeRecord {
                format_version: 1,
                change_id: typed_change_id,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                schema_key: "message".to_string(),
                row_pk: row_pk.clone(),
                file_id: None,
                snapshot: crate::changelog::ChangePayload::from(
                    crate::json_store::JsonSlot::from_json(&format!(
                        "{{\"value\":{next_change}}}"
                    )),
                ),
                metadata: crate::json_store::JsonSlot::None,
                created_at: crate::common::LixTimestamp::expect_parse(
                    "created_at",
                    "2026-05-20T00:00:00Z",
                ),
                origin_key: None,
            });
            next_change += 1;
        }
        append.commits.push(CommitRecord {
            touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
            format_version: 4,
            commit_id: typed_commit_id,
            generation: 0,
            parent_commit_ids: Vec::new(),
            first_parent_jump_commit_id: typed_commit_id,
            first_parent_jump_span: 0,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: crate::common::LixTimestamp::expect_parse(
                "created_at",
                "2026-05-20T00:00:00Z",
            ),
        });
    }
    Ok(BenchAppend { append })
}

fn ordered_bench_uuid(index: usize, discriminator: u8) -> uuid::Uuid {
    let timestamp = 0x0192_0000_0000u64
        .checked_add(u64::try_from(index).expect("benchmark commit index fits u64"))
        .expect("benchmark timestamp does not overflow");
    let mut bytes = [0u8; 16];
    bytes[..6].copy_from_slice(&timestamp.to_be_bytes()[2..]);
    bytes[6] = 0x70;
    bytes[7] = 0;
    let suffix = (u64::try_from(index).expect("benchmark commit index fits u64") << 1)
        | u64::from(discriminator);
    bytes[8..].copy_from_slice(&suffix.to_be_bytes());
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

impl BenchCorpus {
    fn from_append_batches(append_batches: Vec<BenchAppend>) -> Self {
        let commit_ids = append_batches
            .iter()
            .flat_map(|append| append.append.commits.iter().map(|commit| commit.commit_id))
            .collect::<Vec<_>>();
        let change_ids = append_batches
            .iter()
            .flat_map(|append| append.append.changes.iter().map(|change| change.change_id))
            .collect::<Vec<_>>();
        Self {
            append_batches,
            commit_ids,
            change_ids,
        }
    }
}

impl<StorageImpl> BenchStore<StorageImpl>
where
    StorageImpl: BenchStorage + Sync,
{
    fn new(storage: StorageImpl) -> Self {
        Self {
            context: ChangelogContext::new(),
            storage: StorageAdapter::new(storage),
        }
    }
}

async fn stage_append_in_store<StorageImpl>(
    store: &BenchStore<StorageImpl>,
    append: &ChangelogAppend,
) -> Result<BenchWriteStats, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    Ok(stage_append_timed_in_store(store, append).await?.write)
}

async fn stage_append_timed_in_store<StorageImpl>(
    store: &BenchStore<StorageImpl>,
    append: &ChangelogAppend,
) -> Result<BenchAppendTiming, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    let mut transaction = store.storage.begin_write_transaction().await?;
    let mut writes = crate::storage_adapter::StorageWriteSet::new();
    let append = append.clone();
    let stage_started = Instant::now();
    {
        let mut writer = store.context.writer(&mut *transaction, &mut writes);
        writer.stage_append(append).await?;
    }
    let stage_elapsed = stage_started.elapsed();
    let commit_started = Instant::now();
    let stats = writes.apply(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(BenchAppendTiming {
        stage_elapsed,
        commit_elapsed: commit_started.elapsed(),
        write: stats.into(),
    })
}

async fn load_commits_with_lookup<StorageImpl, S: AsRef<str> + Sync>(
    store: &BenchStore<StorageImpl>,
    commit_ids: &[S],
) -> Result<usize, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    let read = store
        .storage
        .begin_read(StorageReadOptions::default())
        .await?;
    let mut reader = store.context.reader(read);
    let commit_ids = commit_ids
        .iter()
        .map(|id| CommitId::for_test_label(id.as_ref()))
        .collect::<Vec<_>>();
    let batch = reader
        .load_commits(CommitLoadRequest {
            commit_ids: &commit_ids,
        })
        .await?;
    Ok(batch
        .into_iter()
        .filter(|(_, entry)| entry.is_some())
        .count())
}

async fn load_changes_with_lookup<StorageImpl, S: AsRef<str> + Sync>(
    store: &BenchStore<StorageImpl>,
    change_ids: &[S],
    _lookup: BenchChangeLookup,
) -> Result<usize, LixError>
where
    StorageImpl: BenchStorage + Sync,
{
    let read = store
        .storage
        .begin_read(StorageReadOptions::default())
        .await?;
    let mut reader = store.context.reader(read);
    let change_ids = change_ids
        .iter()
        .map(|id| ChangeId::for_test_label(id.as_ref()))
        .collect::<Vec<_>>();
    let batch = reader
        .load_changes(ChangeLoadRequest {
            change_ids: &change_ids,
        })
        .await?;
    Ok(batch
        .into_iter()
        .filter(|(_, entry)| entry.is_some())
        .count())
}

#[expect(clippy::cast_possible_truncation)]
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

impl std::ops::AddAssign for BenchWriteStats {
    fn add_assign(&mut self, rhs: Self) {
        self.puts += rhs.puts;
        self.deletes += rhs.deletes;
        self.bytes_written += rhs.bytes_written;
    }
}

#[cfg(test)]
mod tests {
    use super::append_ordered_commits;

    #[test]
    fn ordered_commit_fixture_uses_monotonic_distinct_v7_ids() {
        let append = append_ordered_commits(100, 3).expect("build ordered commits");
        let commits = &append.append.commits;

        assert_eq!(commits.len(), 3);
        assert!(commits.windows(2).all(|pair| {
            pair[0].commit_id < pair[1].commit_id && pair[0].change_id() < pair[1].change_id()
        }));
        // The commit change id is the commit id at ordinal zero of the commit's
        // own change address space, so the two ids now agree by construction and
        // the fixture only has to prove the commit ids are v7 and distinct.
        assert!(commits.iter().all(|commit| {
            commit.commit_id.as_uuid().get_version_num() == 7
                && commit.change_id().as_uuid() == commit.commit_id.as_uuid()
                && commit.commit_id.as_uuid().as_bytes()[12..] == [0; 4]
        }));
    }
}
