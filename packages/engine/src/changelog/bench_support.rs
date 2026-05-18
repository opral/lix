//! Feature-gated changelog benchmark support.
//!
//! This module intentionally exposes a narrow facade for external Cargo bench
//! crates. The normal changelog module stays crate-private.

use std::collections::{HashMap, HashSet};

use super::by_change_index::by_change_entries_for_segments;
use super::by_change_membership_index::by_change_membership_entries_for_segments;
use super::by_commit_index::by_commit_entries_for_segment;
use super::codec::{
    decode_by_change_entry, decode_by_commit_entry, decode_segment, encode_segment, view_segment,
};
use super::context::ChangelogContext;
use super::segment::{
    DecodedSegmentIndex, canonicalize_segment, directory_change_location,
    directory_commit_location, validate_change_checksum, validate_commit_checksum,
    validate_segment_shape,
};
use super::store::{
    BY_CHANGE_INDEX_NAMESPACE, BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE, BY_COMMIT_INDEX_NAMESPACE,
    SEGMENT_NAMESPACE, by_change_key, by_change_membership_commit_id_from_key,
    by_change_membership_key, by_change_membership_prefix, by_commit_key, segment_key,
    segment_value,
};
use super::types::{
    ChangeLoadRequest, ChangeProjection, ChangeVisibilityMode, CommitLoadRequest, CommitProjection,
    CommitVisibilityMode, GcPlan, GcRoot, RebuildIndexStats, Segment, SegmentChange,
    SegmentChangeDirectory, SegmentCommit, SegmentCommitDirectory, SegmentDirectory, SegmentHeader,
    SegmentInlinePayload, StateRowIdentity,
};
use crate::LixError;
use crate::backend::Backend;

pub trait BenchBackend: Backend + Clone {}
impl<T> BenchBackend for T where T: Backend + Clone {}
use crate::common::{CanonicalSchemaKey, EntityId, FileId};
use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::storage::{
    KvGetGroup, KvGetRequest, KvScanRange, KvScanRequest, StorageContext, StorageReader,
    StorageWriteSet,
};

#[derive(Clone)]
pub struct BenchSegment {
    inner: Segment,
}

impl BenchSegment {
    pub fn commit_ids(&self) -> Vec<String> {
        self.inner
            .commits
            .iter()
            .map(|commit| commit.header.id.clone())
            .collect()
    }

    pub fn change_ids(&self) -> Vec<String> {
        self.inner
            .changes
            .iter()
            .map(|change| change.id.clone())
            .collect()
    }

    pub fn commit_count(&self) -> usize {
        self.inner.commits.len()
    }

    pub fn change_count(&self) -> usize {
        self.inner.changes.len()
    }

    pub fn segment_id(&self) -> &str {
        &self.inner.header.segment_id
    }
}

#[derive(Clone)]
pub struct BenchCorpus {
    segments: Vec<BenchSegment>,
    commit_ids: Vec<String>,
    change_ids: Vec<String>,
}

impl BenchCorpus {
    pub fn segments(&self) -> &[BenchSegment] {
        &self.segments
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn commit_ids(&self) -> &[String] {
        &self.commit_ids
    }

    pub fn change_ids(&self) -> &[String] {
        &self.change_ids
    }

    pub fn first_segment_commit_ids(&self) -> Vec<String> {
        self.segments
            .first()
            .map(BenchSegment::commit_ids)
            .unwrap_or_default()
    }

    pub fn first_segment_change_ids(&self) -> Vec<String> {
        self.segments
            .first()
            .map(BenchSegment::change_ids)
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
pub struct BenchStore<B = crate::backend::InMemoryBackend>
where
    B: BenchBackend,
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
    pub live_segments: usize,
    pub sweep_segments: usize,
    pub sweep_index_rows: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BenchSizeStats {
    pub encoded_segment_bytes: usize,
    pub by_commit_value_bytes: usize,
    pub by_change_value_bytes: usize,
    pub by_change_membership_key_bytes: usize,
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
            Self::Header => CommitProjection::Header,
            Self::Body => CommitProjection::Body,
            Self::Full => CommitProjection::Full,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchChangeProjection {
    PhysicalLocation,
    Logical,
    Segment,
}

impl BenchChangeProjection {
    fn into_inner(self) -> ChangeProjection {
        match self {
            Self::PhysicalLocation => ChangeProjection::PhysicalLocation,
            Self::Logical => ChangeProjection::Logical,
            Self::Segment => ChangeProjection::Segment,
        }
    }
}

pub fn segment_1c_1ch() -> Result<BenchSegment, LixError> {
    segment_with_shape("segment-1c-1ch", 1, 1)
}

pub fn segment_1c_100ch() -> Result<BenchSegment, LixError> {
    segment_with_shape("segment-1c-100ch", 1, 100)
}

pub fn segment_1c_1000ch() -> Result<BenchSegment, LixError> {
    segment_with_shape("segment-1c-1000ch", 1, 1_000)
}

pub fn segment_100c_1000ch() -> Result<BenchSegment, LixError> {
    segment_with_shape("segment-100c-1000ch", 100, 1_000)
}

pub fn segment_1c_1000ch_small_inline_payloads() -> Result<BenchSegment, LixError> {
    segment_with_shape_payloads(
        "segment-1c-1000ch-small-inline-payloads",
        1,
        1_000,
        BenchPayloadShape::SmallInline,
    )
}

pub fn segment_1c_1000ch_large_inline_payloads() -> Result<BenchSegment, LixError> {
    segment_with_shape_payloads(
        "segment-1c-1000ch-large-inline-payloads",
        1,
        1_000,
        BenchPayloadShape::LargeInline,
    )
}

pub fn segment_1c_1000ch_external_payload_refs() -> Result<BenchSegment, LixError> {
    segment_with_shape_payloads(
        "segment-1c-1000ch-external-payload-refs",
        1,
        1_000,
        BenchPayloadShape::ExternalRefsOnly,
    )
}

pub fn segment_1c_1000ch_clustered_keys() -> Result<BenchSegment, LixError> {
    segment_with_shape_layout(
        "segment-1c-1000ch-clustered-keys",
        1,
        1_000,
        BenchKeyLayout::Clustered,
    )
}

pub fn segment_1c_1000ch_random_keys() -> Result<BenchSegment, LixError> {
    segment_with_shape_layout(
        "segment-1c-1000ch-random-keys",
        1,
        1_000,
        BenchKeyLayout::Random,
    )
}

pub fn segment_100c_1000ch_reused_keys_across_commits() -> Result<BenchSegment, LixError> {
    segment_with_shape_layout(
        "segment-100c-1000ch-reused-keys-across-commits",
        100,
        1_000,
        BenchKeyLayout::ReuseAcrossCommits,
    )
}

pub fn corpus_10seg_10c_100ch() -> Result<BenchCorpus, LixError> {
    corpus_with_shape("corpus-10seg-10c-100ch", 10, 10, 100, 0, 0)
}

pub fn corpus_100seg_100c_1000ch() -> Result<BenchCorpus, LixError> {
    corpus_with_shape("corpus-100seg-100c-1000ch", 100, 100, 1_000, 10_000, 10_000)
}

pub fn corpus_1000seg_1000c_10000ch() -> Result<BenchCorpus, LixError> {
    corpus_with_shape(
        "corpus-1000seg-1000c-10000ch",
        1_000,
        1_000,
        10_000,
        100_000,
        100_000,
    )
}

pub fn segment_change_membership_fanout(fanout: usize) -> Result<BenchSegment, LixError> {
    fanout_segment("segment-membership-fanout", fanout)
}

pub fn segment_size_stats(segment: &BenchSegment) -> Result<BenchSizeStats, LixError> {
    let encoded_segment_bytes = encode_segment(&segment.inner)?.len();
    let by_commit_value_bytes = by_commit_entries_for_segment(&segment.inner, &HashMap::new())?
        .iter()
        .map(|entry| super::store::by_commit_index_value(entry).map(|bytes| bytes.len()))
        .sum::<Result<usize, LixError>>()?;
    let by_change_value_bytes =
        by_change_entries_for_segments(std::slice::from_ref(&segment.inner))?
            .iter()
            .map(|entry| super::store::by_change_index_value(entry).map(|bytes| bytes.len()))
            .sum::<Result<usize, LixError>>()?;
    let by_change_membership_key_bytes =
        by_change_membership_entries_for_segments(std::slice::from_ref(&segment.inner))
            .iter()
            .map(|entry| by_change_membership_key(&entry.change_id, &entry.commit_id).len())
            .sum();
    let inline_payload_bytes = segment
        .inner
        .changes
        .iter()
        .flat_map(|change| change.inline_payloads.iter())
        .map(|payload| payload.bytes.len())
        .sum();
    Ok(BenchSizeStats {
        encoded_segment_bytes,
        by_commit_value_bytes,
        by_change_value_bytes,
        by_change_membership_key_bytes,
        inline_payload_bytes,
    })
}

pub fn encode_bench_segment(segment: &BenchSegment) -> Result<Vec<u8>, LixError> {
    encode_segment(&segment.inner)
}

pub fn decode_bench_segment(bytes: &[u8]) -> Result<BenchSegment, LixError> {
    Ok(BenchSegment {
        inner: decode_segment(bytes)?,
    })
}

pub fn view_bench_segment(bytes: &[u8]) -> Result<usize, LixError> {
    let view = view_segment(bytes)?;
    Ok(view.directory_commits.len() + view.directory_changes.len())
}

pub fn canonicalize_bench_segment(segment: BenchSegment) -> Result<BenchSegment, LixError> {
    Ok(BenchSegment {
        inner: canonicalize_segment(segment.inner)?,
    })
}

pub fn validate_bench_segment_shape(segment: &BenchSegment) -> Result<(), LixError> {
    validate_segment_shape(&segment.inner)
}

pub struct BenchDecodedSegmentIndex {
    inner: DecodedSegmentIndex,
}

pub fn decode_bench_segment_index(bytes: &[u8]) -> Result<BenchDecodedSegmentIndex, LixError> {
    Ok(BenchDecodedSegmentIndex {
        inner: DecodedSegmentIndex::decode(bytes)?,
    })
}

pub fn build_decoded_segment_index(
    segment: &BenchSegment,
) -> Result<BenchDecodedSegmentIndex, LixError> {
    let bytes = encode_segment(&segment.inner)?;
    decode_bench_segment_index(&bytes)
}

pub fn lookup_segment_directory_commits(
    segment: &BenchSegment,
    commit_ids: &[String],
) -> Result<usize, LixError> {
    let mut found = 0;
    for commit_id in commit_ids {
        if directory_commit_location(&segment.inner, commit_id).is_ok() {
            found += 1;
        }
    }
    Ok(found)
}

pub fn lookup_segment_directory_changes(
    segment: &BenchSegment,
    change_ids: &[String],
) -> Result<usize, LixError> {
    let mut found = 0;
    for change_id in change_ids {
        if directory_change_location(&segment.inner, change_id).is_ok() {
            found += 1;
        }
    }
    Ok(found)
}

pub fn lookup_decoded_segment_index_commits(
    index: &BenchDecodedSegmentIndex,
    commit_ids: &[String],
) -> usize {
    commit_ids
        .iter()
        .filter(|commit_id| index.inner.contains_commit(commit_id))
        .count()
}

pub fn lookup_decoded_segment_index_changes(
    index: &BenchDecodedSegmentIndex,
    change_ids: &[String],
) -> usize {
    change_ids
        .iter()
        .filter(|change_id| index.inner.contains_change(change_id))
        .count()
}

pub fn resolve_inline_payloads(segment: &BenchSegment) -> Result<usize, LixError> {
    let mut bytes = 0;
    for change in &segment.inner.changes {
        for location in &change.directory.payloads {
            let payload = change
                .inline_payloads
                .iter()
                .find(|payload| payload.json_ref == location.json_ref)
                .ok_or_else(|| {
                    LixError::unknown(format!(
                        "changelog bench change '{}' is missing inline payload '{:?}'",
                        change.id, location.json_ref
                    ))
                })?;
            if payload.bytes.len() as u64 != location.len {
                return Err(LixError::unknown(format!(
                    "changelog bench change '{}' inline payload '{:?}' len {} does not match directory len {}",
                    change.id,
                    location.json_ref,
                    payload.bytes.len(),
                    location.len
                )));
            }
            let ordinal = usize::try_from(location.offset).map_err(|_| {
                LixError::unknown(format!(
                    "changelog bench change '{}' inline payload '{:?}' offset overflows usize",
                    change.id, location.json_ref
                ))
            })?;
            if change
                .inline_payloads
                .get(ordinal)
                .map(|candidate| &candidate.json_ref)
                != Some(&location.json_ref)
            {
                return Err(LixError::unknown(format!(
                    "changelog bench change '{}' inline payload '{:?}' offset does not resolve to matching payload",
                    change.id, location.json_ref
                )));
            }
            bytes += payload.bytes.len();
        }
    }
    Ok(bytes)
}

pub fn build_by_commit_entries(segment: &BenchSegment) -> Result<usize, LixError> {
    Ok(by_commit_entries_for_segment(&segment.inner, &HashMap::new())?.len())
}

pub fn build_by_change_entries(segment: &BenchSegment) -> Result<usize, LixError> {
    Ok(by_change_entries_for_segments(std::slice::from_ref(&segment.inner))?.len())
}

pub fn build_by_change_membership_entries(segment: &BenchSegment) -> usize {
    by_change_membership_entries_for_segments(std::slice::from_ref(&segment.inner)).len()
}

pub fn project_first_change_to_logical(segment: &BenchSegment) -> Result<usize, LixError> {
    let change = segment.inner.changes.first().ok_or_else(|| {
        LixError::unknown("changelog bench segment has no changes to project".to_string())
    })?;
    let projected = super::types::Change {
        id: change.id.clone(),
        authored_commit_id: change.authored_commit_id.clone(),
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        file_id: change.file_id.clone(),
        snapshot_ref: change.snapshot_ref,
        metadata_ref: change.metadata_ref,
        created_at: change.created_at.clone(),
    };
    Ok(projected.id.len()
        + projected.schema_key.len()
        + projected
            .file_id
            .as_ref()
            .map(|file_id| file_id.len())
            .unwrap_or(0)
        + projected.created_at.len()
        + usize::from(projected.snapshot_ref.is_some())
        + usize::from(projected.metadata_ref.is_some()))
}

pub fn validate_first_commit_checksum(segment: &BenchSegment) -> Result<(), LixError> {
    let commit = segment.inner.commits.first().ok_or_else(|| {
        LixError::unknown("changelog bench segment has no commits to validate".to_string())
    })?;
    validate_commit_checksum(&commit.checksum, &commit.header.id, commit)
}

pub fn validate_first_change_checksum(segment: &BenchSegment) -> Result<(), LixError> {
    let change = segment.inner.changes.first().ok_or_else(|| {
        LixError::unknown("changelog bench segment has no changes to validate".to_string())
    })?;
    let checksum = segment
        .inner
        .directory
        .changes
        .iter()
        .find_map(|(change_id, location)| {
            if change_id == &change.id {
                Some(location.checksum.as_str())
            } else {
                None
            }
        })
        .ok_or_else(|| {
            LixError::unknown(format!(
                "changelog bench segment missing directory entry for change '{}'",
                change.id
            ))
        })?;
    validate_change_checksum(checksum, &change.id, change)
}

pub fn validate_publication_closure(segment: &BenchSegment) -> Result<usize, LixError> {
    validate_segment_shape(&segment.inner)?;
    let changes_by_id = segment
        .inner
        .changes
        .iter()
        .map(|change| Ok((change.id.clone(), state_row_identity_for_change(change)?)))
        .collect::<Result<HashMap<_, _>, LixError>>()?;

    let mut checked = 0;
    for commit in &segment.inner.commits {
        if commit.header.membership_count as usize != commit.body.membership.len() {
            return Err(LixError::unknown(format!(
                "changelog bench commit '{}' membership_count drift",
                commit.header.id
            )));
        }
        let membership_ids = commit
            .body
            .membership
            .iter()
            .map(|membership| membership.member_change_id.as_str())
            .collect::<HashSet<_>>();
        for membership in &commit.body.membership {
            if !changes_by_id.contains_key(&membership.member_change_id) {
                return Err(LixError::unknown(format!(
                    "changelog bench commit '{}' references missing change '{}'",
                    commit.header.id, membership.member_change_id
                )));
            }
            checked += 1;
        }
        for (identity, change_id) in &commit.directory.state_row_identities {
            if !membership_ids.contains(change_id.as_str()) {
                return Err(LixError::unknown(format!(
                    "changelog bench commit '{}' directory points to non-member change '{}'",
                    commit.header.id, change_id
                )));
            }
            let Some(change_identity) = changes_by_id.get(change_id) else {
                return Err(LixError::unknown(format!(
                    "changelog bench commit '{}' directory points to missing change '{}'",
                    commit.header.id, change_id
                )));
            };
            if identity != change_identity {
                return Err(LixError::unknown(format!(
                    "changelog bench commit '{}' directory identity does not match change '{}'",
                    commit.header.id, change_id
                )));
            }
        }
    }
    Ok(checked)
}

pub fn new_store<B: BenchBackend>(backend: B) -> BenchStore<B>
where
    B: BenchBackend,
{
    BenchStore {
        context: ChangelogContext::new(),
        storage: StorageContext::new(backend),
    }
}

pub async fn stage_segment_once<B: BenchBackend>(
    backend: B,
    segment: &BenchSegment,
) -> Result<BenchWriteStats, LixError>
where
    B: BenchBackend,
{
    let store = new_store(backend);
    stage_segment_in_store(&store, segment).await
}

pub async fn stage_segment_raw_once<B: BenchBackend>(
    backend: B,
    segment: &BenchSegment,
) -> Result<BenchWriteStats, LixError>
where
    B: BenchBackend,
{
    let store = new_store(backend);
    write_corpus_segments_raw(&store, &BenchCorpus::from_segments(vec![segment.clone()])).await
}

pub async fn prepare_store<B: BenchBackend>(
    backend: B,
    segment: &BenchSegment,
    publish: bool,
) -> Result<BenchStore<B>, LixError>
where
    B: BenchBackend,
{
    prepare_corpus_store(
        backend,
        &BenchCorpus::from_segments(vec![segment.clone()]),
        publish,
    )
    .await
}

pub async fn prepare_corpus_store<B: BenchBackend>(
    backend: B,
    corpus: &BenchCorpus,
    publish: bool,
) -> Result<BenchStore<B>, LixError>
where
    B: BenchBackend,
{
    let store = new_store(backend);
    let mut transaction = store.storage.begin_write_transaction().await?;
    let mut writes = StorageWriteSet::new();
    {
        let mut writer = store.context.writer(&mut *transaction, &mut writes);
        for segment in &corpus.segments {
            writer.stage_segment(segment.inner.clone()).await?;
        }
        if publish {
            for commit_id in &corpus.commit_ids {
                writer.stage_publish_commit(commit_id).await?;
            }
        }
    }
    writes.apply(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(store)
}

pub async fn prepare_rebuild_store<B: BenchBackend>(
    backend: B,
    corpus: &BenchCorpus,
    mode: BenchRebuildMode,
) -> Result<BenchStore<B>, LixError>
where
    B: BenchBackend,
{
    let store = match mode {
        BenchRebuildMode::Noop => prepare_corpus_store(backend, corpus, false).await?,
        BenchRebuildMode::EmptyIndexes => {
            let store = new_store(backend);
            write_corpus_segments_raw(&store, corpus).await?;
            store
        }
        BenchRebuildMode::StaleExtraRows => {
            let store = prepare_corpus_store(backend, corpus, false).await?;
            inject_stale_index_rows(&store).await?;
            store
        }
        BenchRebuildMode::CorruptValues => {
            let store = prepare_corpus_store(backend, corpus, false).await?;
            inject_corrupt_index_values(&store, corpus).await?;
            store
        }
    };
    Ok(store)
}

pub async fn prepare_gc_store<B: BenchBackend>(
    backend: B,
    live_segments: usize,
    dead_segments: usize,
    changes_per_segment: usize,
) -> Result<(BenchStore<B>, String), LixError>
where
    B: BenchBackend,
{
    let live_segments = live_segments.max(1);
    let changes_per_segment = changes_per_segment.max(1);
    let live = corpus_with_shape(
        "gc-live",
        live_segments,
        live_segments,
        live_segments * changes_per_segment,
        0,
        0,
    )?;
    let dead = if dead_segments == 0 {
        BenchCorpus::from_segments(Vec::new())
    } else {
        corpus_with_shape(
            "gc-dead",
            dead_segments,
            dead_segments,
            dead_segments * changes_per_segment,
            1_000_000,
            1_000_000,
        )?
    };

    let mut segments = live.segments.clone();
    segments.extend(dead.segments.clone());
    let corpus = BenchCorpus::from_segments(segments);
    let store = new_store(backend);
    let mut transaction = store.storage.begin_write_transaction().await?;
    let mut writes = StorageWriteSet::new();
    {
        let mut writer = store.context.writer(&mut *transaction, &mut writes);
        for segment in &corpus.segments {
            writer.stage_segment(segment.inner.clone()).await?;
        }
        for commit_id in live.commit_ids() {
            writer.stage_publish_commit(commit_id).await?;
        }
    }
    writes.apply(&mut *transaction).await?;
    transaction.commit().await?;
    let root_commit_id = live
        .last_commit_id()
        .ok_or_else(|| LixError::unknown("changelog gc bench has no live root".to_string()))?
        .to_string();
    Ok((store, root_commit_id))
}

pub async fn stage_publish_first_commit_once<B: BenchBackend>(
    backend: B,
    segment: &BenchSegment,
) -> Result<BenchWriteStats, LixError>
where
    B: BenchBackend,
{
    let store = prepare_store(backend, segment, false).await?;
    stage_publish_first_commit_in_store(&store, segment).await
}

pub async fn stage_publish_first_commit_in_store<B: BenchBackend>(
    store: &BenchStore<B>,
    segment: &BenchSegment,
) -> Result<BenchWriteStats, LixError> {
    let first_commit = segment.commit_ids().into_iter().next().ok_or_else(|| {
        LixError::unknown("changelog bench segment has no commit to publish".to_string())
    })?;
    let mut transaction = store.storage.begin_write_transaction().await?;
    let mut writes = StorageWriteSet::new();
    {
        let mut writer = store.context.writer(&mut *transaction, &mut writes);
        writer.stage_publish_commit(&first_commit).await?;
    }
    let stats = writes.apply(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(bench_write_stats(stats))
}

pub async fn stage_corpus_once<B: BenchBackend>(
    backend: B,
    corpus: &BenchCorpus,
) -> Result<BenchWriteStats, LixError>
where
    B: BenchBackend,
{
    let store = new_store(backend);
    let mut transaction = store.storage.begin_write_transaction().await?;
    let mut writes = StorageWriteSet::new();
    {
        let mut writer = store.context.writer(&mut *transaction, &mut writes);
        for segment in &corpus.segments {
            writer.stage_segment(segment.inner.clone()).await?;
        }
    }
    let stats = writes.apply(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(bench_write_stats(stats))
}

pub async fn stage_corpus_raw_once<B: BenchBackend>(
    backend: B,
    corpus: &BenchCorpus,
) -> Result<BenchWriteStats, LixError>
where
    B: BenchBackend,
{
    let store = new_store(backend);
    write_corpus_segments_raw(&store, corpus).await
}

pub async fn stage_incremental_segment_once<B: BenchBackend>(
    backend: B,
    corpus: &BenchCorpus,
) -> Result<BenchWriteStats, LixError>
where
    B: BenchBackend,
{
    let store = prepare_corpus_store(backend, corpus, false).await?;
    let segment = incremental_segment_for_corpus(corpus)?;
    stage_segment_in_store(&store, &segment).await
}

pub async fn stage_incremental_segment_raw_once<B: BenchBackend>(
    backend: B,
    corpus: &BenchCorpus,
) -> Result<BenchWriteStats, LixError>
where
    B: BenchBackend,
{
    let store = new_store(backend);
    write_corpus_segments_raw(&store, corpus).await?;
    let segment = incremental_segment_for_corpus(corpus)?;
    write_corpus_segments_raw(&store, &BenchCorpus::from_segments(vec![segment])).await
}

pub async fn stage_publish_all_commits_once<B: BenchBackend>(
    backend: B,
    corpus: &BenchCorpus,
) -> Result<BenchWriteStats, LixError>
where
    B: BenchBackend,
{
    let store = prepare_corpus_store(backend, corpus, false).await?;
    let mut transaction = store.storage.begin_write_transaction().await?;
    let mut writes = StorageWriteSet::new();
    {
        let mut writer = store.context.writer(&mut *transaction, &mut writes);
        for commit_id in corpus.commit_ids() {
            writer.stage_publish_commit(commit_id).await?;
        }
    }
    let stats = writes.apply(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(bench_write_stats(stats))
}

pub async fn load_commits_physical<B: BenchBackend>(
    store: &BenchStore<B>,
    commit_ids: &[String],
) -> Result<usize, LixError> {
    load_commits_physical_with_projection(store, commit_ids, BenchCommitProjection::Header).await
}

pub async fn load_commits_physical_with_projection<B: BenchBackend>(
    store: &BenchStore<B>,
    commit_ids: &[String],
    projection: BenchCommitProjection,
) -> Result<usize, LixError> {
    let mut reader = store.context.reader(store.storage.clone());
    let batch = reader
        .load_commits(CommitLoadRequest {
            commit_ids,
            projection: projection.into_inner(),
            visibility: CommitVisibilityMode::PhysicalOnly,
        })
        .await?;
    Ok(batch.entries.iter().filter(|entry| entry.is_some()).count())
}

pub async fn load_commits_visible<B: BenchBackend>(
    store: &BenchStore<B>,
    commit_ids: &[String],
) -> Result<usize, LixError> {
    load_commits_visible_with_projection(store, commit_ids, BenchCommitProjection::Header).await
}

pub async fn load_commits_visible_with_projection<B: BenchBackend>(
    store: &BenchStore<B>,
    commit_ids: &[String],
    projection: BenchCommitProjection,
) -> Result<usize, LixError> {
    let mut reader = store.context.reader(store.storage.clone());
    let batch = reader
        .load_commits(CommitLoadRequest {
            commit_ids,
            projection: projection.into_inner(),
            visibility: CommitVisibilityMode::RequireVisible,
        })
        .await?;
    Ok(batch.entries.iter().filter(|entry| entry.is_some()).count())
}

pub async fn load_changes_physical<B: BenchBackend>(
    store: &BenchStore<B>,
    change_ids: &[String],
) -> Result<usize, LixError> {
    load_changes_physical_with_projection(
        store,
        change_ids,
        BenchChangeProjection::PhysicalLocation,
    )
    .await
}

pub async fn load_changes_physical_with_projection<B: BenchBackend>(
    store: &BenchStore<B>,
    change_ids: &[String],
    projection: BenchChangeProjection,
) -> Result<usize, LixError> {
    let mut reader = store.context.reader(store.storage.clone());
    let batch = reader
        .load_changes(ChangeLoadRequest {
            change_ids,
            projection: projection.into_inner(),
            visibility: ChangeVisibilityMode::PhysicalOnly,
        })
        .await?;
    Ok(batch.entries.iter().filter(|entry| entry.is_some()).count())
}

pub async fn load_changes_visible<B: BenchBackend>(
    store: &BenchStore<B>,
    change_ids: &[String],
) -> Result<usize, LixError> {
    load_changes_visible_with_projection(store, change_ids, BenchChangeProjection::PhysicalLocation)
        .await
}

pub async fn load_changes_visible_with_projection<B: BenchBackend>(
    store: &BenchStore<B>,
    change_ids: &[String],
    projection: BenchChangeProjection,
) -> Result<usize, LixError> {
    let mut reader = store.context.reader(store.storage.clone());
    let batch = reader
        .load_changes(ChangeLoadRequest {
            change_ids,
            projection: projection.into_inner(),
            visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
        })
        .await?;
    Ok(batch.entries.iter().filter(|entry| entry.is_some()).count())
}

pub async fn lookup_by_commit_index<B: BenchBackend>(
    store: &BenchStore<B>,
    commit_ids: &[String],
) -> Result<usize, LixError> {
    let values = get_values(
        store,
        BY_COMMIT_INDEX_NAMESPACE,
        commit_ids.iter().map(|commit_id| by_commit_key(commit_id)),
    )
    .await?;
    let mut found = 0;
    for (value, commit_id) in values.into_iter().zip(commit_ids.iter()) {
        if let Some(bytes) = value {
            let entry = decode_by_commit_entry(&bytes)?;
            if entry.commit_id != *commit_id {
                return Err(LixError::unknown(format!(
                    "by_commit key for '{commit_id}' contains commit_id '{}'",
                    entry.commit_id
                )));
            }
            found += 1;
        }
    }
    Ok(found)
}

pub async fn lookup_by_change_index<B: BenchBackend>(
    store: &BenchStore<B>,
    change_ids: &[String],
) -> Result<usize, LixError> {
    let values = get_values(
        store,
        BY_CHANGE_INDEX_NAMESPACE,
        change_ids.iter().map(|change_id| by_change_key(change_id)),
    )
    .await?;
    let mut found = 0;
    for (value, change_id) in values.into_iter().zip(change_ids.iter()) {
        if let Some(bytes) = value {
            let entry = decode_by_change_entry(&bytes)?;
            if entry.change_id != *change_id {
                return Err(LixError::unknown(format!(
                    "by_change key for '{change_id}' contains change_id '{}'",
                    entry.change_id
                )));
            }
            found += 1;
        }
    }
    Ok(found)
}

pub async fn scan_by_change_membership_candidates<B: BenchBackend>(
    store: &BenchStore<B>,
    change_id: &str,
) -> Result<usize, LixError> {
    let prefix = by_change_membership_prefix(change_id);
    let mut storage = store.storage.clone();
    let mut after = None;
    let mut found = 0;
    loop {
        let page = storage
            .scan_keys(KvScanRequest {
                namespace: BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE.to_string(),
                range: KvScanRange::prefix(prefix.clone()),
                after,
                limit: 256,
            })
            .await?;
        for index in 0..page.keys.len() {
            let Some(key) = page.keys.get(index) else {
                continue;
            };
            if by_change_membership_commit_id_from_key(change_id, key)?.is_some() {
                found += 1;
            }
        }
        let Some(next_after) = page.resume_after else {
            break;
        };
        after = Some(next_after);
    }
    Ok(found)
}

pub async fn scan_segments_decode<B: BenchBackend>(
    store: &BenchStore<B>,
) -> Result<usize, LixError> {
    let mut storage = store.storage.clone();
    let mut after = None;
    let mut decoded_objects = 0;
    loop {
        let page = storage
            .scan_values(KvScanRequest {
                namespace: SEGMENT_NAMESPACE.to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after,
                limit: 256,
            })
            .await?;
        for value in page.values.iter() {
            let segment = decode_segment(value)?;
            decoded_objects += 1 + segment.commits.len() + segment.changes.len();
        }
        let Some(next_after) = page.resume_after else {
            break;
        };
        after = Some(next_after);
    }
    Ok(decoded_objects)
}

pub async fn rebuild_mandatory_indexes<B: BenchBackend>(
    store: &BenchStore<B>,
) -> Result<BenchRebuildStats, LixError> {
    let mut transaction = store.storage.begin_write_transaction().await?;
    let mut writes = StorageWriteSet::new();
    let stats = {
        let mut writer = store.context.writer(&mut *transaction, &mut writes);
        writer.rebuild_mandatory_indexes().await?
    };
    writes.apply(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(stats.into())
}

pub async fn plan_gc<B: BenchBackend>(
    store: &BenchStore<B>,
    root_commit_id: &str,
) -> Result<BenchGcStats, LixError> {
    let mut reader = store.context.reader(store.storage.clone());
    let plan = reader
        .plan_gc(&[GcRoot::VersionHead(root_commit_id.to_string())])
        .await?;
    Ok(plan.into())
}

pub async fn collect_garbage<B: BenchBackend>(
    store: &BenchStore<B>,
    root_commit_id: &str,
) -> Result<BenchGcStats, LixError> {
    let mut transaction = store.storage.begin_write_transaction().await?;
    let mut writes = StorageWriteSet::new();
    let plan = {
        let mut writer = store.context.writer(&mut *transaction, &mut writes);
        writer
            .collect_garbage(&[GcRoot::VersionHead(root_commit_id.to_string())])
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
    CorruptValues,
}

async fn stage_segment_in_store<B: BenchBackend>(
    store: &BenchStore<B>,
    segment: &BenchSegment,
) -> Result<BenchWriteStats, LixError> {
    let mut transaction = store.storage.begin_write_transaction().await?;
    let mut writes = StorageWriteSet::new();
    {
        let mut writer = store.context.writer(&mut *transaction, &mut writes);
        writer.stage_segment(segment.inner.clone()).await?;
    }
    let stats = writes.apply(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(bench_write_stats(stats))
}

fn segment_with_shape(
    segment_id: &str,
    commit_count: usize,
    change_count: usize,
) -> Result<BenchSegment, LixError> {
    segment_with_shape_at(
        segment_id,
        commit_count,
        change_count,
        0,
        0,
        None,
        BenchKeyLayout::Interleaved,
        BenchPayloadShape::None,
    )
}

fn segment_with_shape_layout(
    segment_id: &str,
    commit_count: usize,
    change_count: usize,
    key_layout: BenchKeyLayout,
) -> Result<BenchSegment, LixError> {
    segment_with_shape_at(
        segment_id,
        commit_count,
        change_count,
        0,
        0,
        None,
        key_layout,
        BenchPayloadShape::None,
    )
}

fn segment_with_shape_payloads(
    segment_id: &str,
    commit_count: usize,
    change_count: usize,
    payload_shape: BenchPayloadShape,
) -> Result<BenchSegment, LixError> {
    segment_with_shape_at(
        segment_id,
        commit_count,
        change_count,
        0,
        0,
        None,
        BenchKeyLayout::Interleaved,
        payload_shape,
    )
}

fn segment_with_shape_at(
    segment_id: &str,
    commit_count: usize,
    change_count: usize,
    commit_start: usize,
    change_start: usize,
    parent_before: Option<String>,
    key_layout: BenchKeyLayout,
    payload_shape: BenchPayloadShape,
) -> Result<BenchSegment, LixError> {
    let changes = (0..change_count)
        .map(|index| {
            let global_change_index = change_start + index;
            SegmentChange {
                id: change_id(global_change_index),
                authored_commit_id: Some(commit_id(
                    commit_start + commit_ordinal_for_change(index, commit_count, change_count),
                )),
                entity_id: EntityIdentity::single(entity_id_for_layout(
                    global_change_index,
                    key_layout,
                )),
                schema_key: "message".to_string(),
                file_id: Some(file_id_for_layout(global_change_index, key_layout)),
                snapshot_ref: payload_shape.snapshot_ref(global_change_index),
                metadata_ref: payload_shape.metadata_ref(global_change_index),
                created_at: timestamp(global_change_index),
                inline_payloads: payload_shape.inline_payloads(global_change_index),
                directory: SegmentChangeDirectory::default(),
            }
        })
        .collect::<Vec<_>>();

    let mut commits = Vec::with_capacity(commit_count);
    let mut next_change = 0;
    for commit_ordinal in 0..commit_count {
        let assigned = changes_for_commit(commit_ordinal, commit_count, change_count);
        let mut membership = Vec::with_capacity(assigned);
        let mut state_row_identities = Vec::with_capacity(assigned);
        let mut membership_ordinals = Vec::with_capacity(assigned);
        for member_ordinal in 0..assigned {
            let change_index = next_change;
            next_change += 1;
            let global_change_index = change_start + change_index;
            let change_id = change_id(global_change_index);
            membership.push(super::types::MembershipRecord {
                member_change_id: change_id.clone(),
                role: super::types::MembershipRole::Authored,
                source_parent_ordinal: None,
            });
            state_row_identities.push((
                state_row_identity_for_layout(global_change_index, key_layout)?,
                change_id.clone(),
            ));
            membership_ordinals.push((change_id, member_ordinal as u32));
        }
        let global_commit_ordinal = commit_start + commit_ordinal;
        commits.push(SegmentCommit {
            header: super::types::CommitHeader {
                id: commit_id(global_commit_ordinal),
                parent_commit_ids: if commit_ordinal == 0 {
                    parent_before.clone().into_iter().collect()
                } else {
                    vec![commit_id(global_commit_ordinal - 1)]
                },
                derivable_change_id: format!("derivable-change-{global_commit_ordinal:06}"),
                author_account_ids: vec!["bench-account".to_string()],
                created_at: timestamp(global_commit_ordinal),
                membership_count: 0,
            },
            body: super::types::CommitBody { membership },
            directory: SegmentCommitDirectory {
                state_row_identities,
                membership_ordinals,
            },
            checksum: String::new(),
        });
    }

    Ok(BenchSegment {
        inner: canonicalize_segment(Segment {
            header: SegmentHeader {
                segment_id: segment_id.to_string(),
                format_version: 1,
                commit_count: 0,
                change_count: 0,
                byte_count: 0,
                payload_count: 0,
                checksum: String::new(),
            },
            directory: SegmentDirectory::default(),
            commits,
            changes,
        })?,
    })
}

fn corpus_with_shape(
    label: &str,
    segment_count: usize,
    commit_count: usize,
    change_count: usize,
    commit_start: usize,
    change_start: usize,
) -> Result<BenchCorpus, LixError> {
    let mut segments = Vec::with_capacity(segment_count);
    let mut next_commit = commit_start;
    let mut next_change = change_start;
    let mut parent_before = None;
    for segment_ordinal in 0..segment_count {
        let segment_commits = distributed_count(segment_ordinal, segment_count, commit_count);
        let segment_changes = distributed_count(segment_ordinal, segment_count, change_count);
        if segment_commits == 0 {
            continue;
        }
        let segment = segment_with_shape_at(
            &format!("{label}-segment-{segment_ordinal:06}"),
            segment_commits,
            segment_changes,
            next_commit,
            next_change,
            parent_before.clone(),
            BenchKeyLayout::Interleaved,
            BenchPayloadShape::None,
        )?;
        parent_before = segment.commit_ids().last().cloned();
        next_commit += segment_commits;
        next_change += segment_changes;
        segments.push(segment);
    }
    Ok(BenchCorpus::from_segments(segments))
}

fn incremental_segment_for_corpus(corpus: &BenchCorpus) -> Result<BenchSegment, LixError> {
    let unique_offset = 3_000_000;
    segment_with_shape_at(
        "incremental-append-segment",
        1,
        1,
        unique_offset + corpus.commit_ids.len(),
        unique_offset + corpus.change_ids.len(),
        corpus.last_commit_id().map(str::to_owned),
        BenchKeyLayout::Interleaved,
        BenchPayloadShape::None,
    )
}

fn fanout_segment(segment_id: &str, fanout: usize) -> Result<BenchSegment, LixError> {
    let fanout = fanout.max(1);
    let change_id = change_id(2_000_000);
    let identity = state_row_identity(2_000_000)?;
    let changes = vec![SegmentChange {
        id: change_id.clone(),
        authored_commit_id: Some(commit_id(2_000_000)),
        entity_id: EntityIdentity::single(entity_id(2_000_000)),
        schema_key: "message".to_string(),
        file_id: Some(file_id(2_000_000)),
        snapshot_ref: None,
        metadata_ref: None,
        created_at: timestamp(2_000_000),
        inline_payloads: Vec::new(),
        directory: SegmentChangeDirectory::default(),
    }];
    let mut commits = Vec::with_capacity(fanout);
    for ordinal in 0..fanout {
        let commit_index = 2_000_000 + ordinal;
        let role = if ordinal == 0 {
            super::types::MembershipRole::Authored
        } else {
            super::types::MembershipRole::Adopted
        };
        commits.push(SegmentCommit {
            header: super::types::CommitHeader {
                id: commit_id(commit_index),
                parent_commit_ids: if ordinal == 0 {
                    Vec::new()
                } else {
                    vec![commit_id(commit_index - 1)]
                },
                derivable_change_id: format!("derivable-change-{commit_index:06}"),
                author_account_ids: vec!["bench-account".to_string()],
                created_at: timestamp(commit_index),
                membership_count: 0,
            },
            body: super::types::CommitBody {
                membership: vec![super::types::MembershipRecord {
                    member_change_id: change_id.clone(),
                    role,
                    source_parent_ordinal: if ordinal == 0 { None } else { Some(0) },
                }],
            },
            directory: SegmentCommitDirectory {
                state_row_identities: vec![(identity.clone(), change_id.clone())],
                membership_ordinals: vec![(change_id.clone(), 0)],
            },
            checksum: String::new(),
        });
    }

    Ok(BenchSegment {
        inner: canonicalize_segment(Segment {
            header: SegmentHeader {
                segment_id: segment_id.to_string(),
                format_version: 1,
                commit_count: 0,
                change_count: 0,
                byte_count: 0,
                payload_count: 0,
                checksum: String::new(),
            },
            directory: SegmentDirectory::default(),
            commits,
            changes,
        })?,
    })
}

fn distributed_count(ordinal: usize, buckets: usize, total: usize) -> usize {
    let base = total / buckets;
    let remainder = total % buckets;
    base + usize::from(ordinal < remainder)
}

impl BenchCorpus {
    fn from_segments(segments: Vec<BenchSegment>) -> Self {
        let commit_ids = segments
            .iter()
            .flat_map(BenchSegment::commit_ids)
            .collect::<Vec<_>>();
        let change_ids = segments
            .iter()
            .flat_map(BenchSegment::change_ids)
            .collect::<Vec<_>>();
        Self {
            segments,
            commit_ids,
            change_ids,
        }
    }
}

async fn write_corpus_segments_raw<B: BenchBackend>(
    store: &BenchStore<B>,
    corpus: &BenchCorpus,
) -> Result<BenchWriteStats, LixError> {
    let mut transaction = store.storage.begin_write_transaction().await?;
    let mut writes = StorageWriteSet::new();
    for segment in &corpus.segments {
        writes.put(
            SEGMENT_NAMESPACE,
            segment_key(segment.segment_id()),
            segment_value(&segment.inner)?,
        );
    }
    let stats = writes.apply(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(bench_write_stats(stats))
}

async fn inject_stale_index_rows<B: BenchBackend>(
    store: &BenchStore<B>,
) -> Result<BenchWriteStats, LixError> {
    let mut transaction = store.storage.begin_write_transaction().await?;
    let mut writes = StorageWriteSet::new();
    writes.put(
        BY_COMMIT_INDEX_NAMESPACE,
        by_commit_key("stale-commit"),
        b"stale-by-commit".to_vec(),
    );
    writes.put(
        BY_CHANGE_INDEX_NAMESPACE,
        by_change_key("stale-change"),
        b"stale-by-change".to_vec(),
    );
    writes.put(
        BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE,
        by_change_membership_key("stale-change", "stale-commit"),
        b"stale-by-change-membership".to_vec(),
    );
    let stats = writes.apply(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(bench_write_stats(stats))
}

async fn inject_corrupt_index_values<B: BenchBackend>(
    store: &BenchStore<B>,
    corpus: &BenchCorpus,
) -> Result<BenchWriteStats, LixError> {
    let first_commit = corpus.first_commit_id().ok_or_else(|| {
        LixError::unknown("changelog corrupt-index bench has no commit".to_string())
    })?;
    let first_change = corpus.first_change_id().ok_or_else(|| {
        LixError::unknown("changelog corrupt-index bench has no change".to_string())
    })?;
    let mut transaction = store.storage.begin_write_transaction().await?;
    let mut writes = StorageWriteSet::new();
    writes.put(
        BY_COMMIT_INDEX_NAMESPACE,
        by_commit_key(first_commit),
        b"corrupt-by-commit".to_vec(),
    );
    writes.put(
        BY_CHANGE_INDEX_NAMESPACE,
        by_change_key(first_change),
        b"corrupt-by-change".to_vec(),
    );
    writes.put(
        BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE,
        by_change_membership_key(first_change, first_commit),
        b"corrupt-by-change-membership".to_vec(),
    );
    let stats = writes.apply(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(bench_write_stats(stats))
}

async fn get_values<B: BenchBackend>(
    store: &BenchStore<B>,
    namespace: &'static str,
    keys: impl IntoIterator<Item = Vec<u8>>,
) -> Result<Vec<Option<Vec<u8>>>, LixError> {
    let mut storage = store.storage.clone();
    let batch = storage
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: namespace.to_string(),
                keys: keys.into_iter().collect(),
            }],
        })
        .await?;
    let Some(group) = batch.groups.first() else {
        return Ok(Vec::new());
    };
    Ok(group
        .values_iter()
        .map(|value| value.map(<[u8]>::to_vec))
        .collect())
}

fn commit_ordinal_for_change(
    change_index: usize,
    commit_count: usize,
    change_count: usize,
) -> usize {
    let mut start = 0;
    for commit_ordinal in 0..commit_count {
        let count = changes_for_commit(commit_ordinal, commit_count, change_count);
        if change_index < start + count {
            return commit_ordinal;
        }
        start += count;
    }
    commit_count.saturating_sub(1)
}

fn changes_for_commit(commit_ordinal: usize, commit_count: usize, change_count: usize) -> usize {
    let base = change_count / commit_count;
    let remainder = change_count % commit_count;
    base + usize::from(commit_ordinal < remainder)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BenchKeyLayout {
    Interleaved,
    Clustered,
    Random,
    ReuseAcrossCommits,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BenchPayloadShape {
    None,
    SmallInline,
    LargeInline,
    ExternalRefsOnly,
}

impl BenchPayloadShape {
    fn snapshot_ref(self, index: usize) -> Option<JsonRef> {
        match self {
            Self::None => None,
            Self::SmallInline | Self::LargeInline | Self::ExternalRefsOnly => {
                Some(json_ref(index, 1))
            }
        }
    }

    fn metadata_ref(self, index: usize) -> Option<JsonRef> {
        match self {
            Self::ExternalRefsOnly => Some(json_ref(index, 2)),
            Self::None | Self::SmallInline | Self::LargeInline => None,
        }
    }

    fn inline_payloads(self, index: usize) -> Vec<SegmentInlinePayload> {
        match self {
            Self::None | Self::ExternalRefsOnly => Vec::new(),
            Self::SmallInline => vec![SegmentInlinePayload {
                json_ref: json_ref(index, 1),
                bytes: payload_bytes(index, 64),
            }],
            Self::LargeInline => vec![SegmentInlinePayload {
                json_ref: json_ref(index, 1),
                bytes: payload_bytes(index, 8 * 1024),
            }],
        }
    }
}

fn state_row_identity(index: usize) -> Result<StateRowIdentity, LixError> {
    state_row_identity_for_layout(index, BenchKeyLayout::Interleaved)
}

fn state_row_identity_for_layout(
    index: usize,
    layout: BenchKeyLayout,
) -> Result<StateRowIdentity, LixError> {
    Ok(StateRowIdentity {
        schema_key: CanonicalSchemaKey::new("message")?,
        file_id: FileId::new(file_id_for_layout(index, layout))?,
        entity_id: EntityId::new(entity_id_for_layout(index, layout))?,
    })
}

fn state_row_identity_for_change(change: &SegmentChange) -> Result<StateRowIdentity, LixError> {
    let file_id = change.file_id.as_deref().ok_or_else(|| {
        LixError::unknown(format!(
            "changelog bench change '{}' is missing file_id",
            change.id
        ))
    })?;
    Ok(StateRowIdentity {
        schema_key: CanonicalSchemaKey::new(change.schema_key.clone())?,
        file_id: FileId::new(file_id.to_string())?,
        entity_id: EntityId::new(change.entity_id.as_single_string_owned()?)?,
    })
}

fn commit_id(index: usize) -> String {
    format!("commit-{index:06}")
}

fn change_id(index: usize) -> String {
    format!("change-{index:06}")
}

fn file_id(index: usize) -> String {
    format!("file-{:03}", index % 16)
}

fn entity_id(index: usize) -> String {
    format!("entity-{index:06}")
}

fn file_id_for_layout(index: usize, layout: BenchKeyLayout) -> String {
    match layout {
        BenchKeyLayout::Interleaved => file_id(index),
        BenchKeyLayout::Clustered => format!("file-{:03}", index / 100),
        BenchKeyLayout::Random => format!("file-{:03}", stable_hash(index) % 128),
        BenchKeyLayout::ReuseAcrossCommits => format!("file-{:03}", index % 10),
    }
}

fn entity_id_for_layout(index: usize, layout: BenchKeyLayout) -> String {
    match layout {
        BenchKeyLayout::Interleaved => entity_id(index),
        BenchKeyLayout::Clustered => entity_id(index),
        BenchKeyLayout::Random => format!("entity-{:010}", stable_hash(index)),
        BenchKeyLayout::ReuseAcrossCommits => format!("entity-{:06}", index % 10),
    }
}

fn stable_hash(index: usize) -> usize {
    let mut value = index as u64;
    value ^= value >> 33;
    value = value.wrapping_mul(0xff51afd7ed558ccd);
    value ^= value >> 33;
    value = value.wrapping_mul(0xc4ceb9fe1a85ec53);
    value ^= value >> 33;
    value as usize
}

fn json_ref(index: usize, salt: u8) -> JsonRef {
    let hash = blake3::hash(&payload_bytes(index.wrapping_add(salt as usize), 64));
    JsonRef::from_hash_bytes(*hash.as_bytes())
}

fn payload_bytes(index: usize, len: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(len);
    for offset in 0..len {
        bytes.push(((index.wrapping_mul(31) + offset.wrapping_mul(17)) & 0xff) as u8);
    }
    bytes
}

fn timestamp(index: usize) -> String {
    format!("2026-05-12T00:{:02}:{:02}Z", (index / 60) % 60, index % 60)
}

fn bench_write_stats(stats: crate::storage::KvWriteStats) -> BenchWriteStats {
    BenchWriteStats {
        puts: stats.puts,
        deletes: stats.deletes,
        bytes_written: stats.bytes_written,
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
            live_segments: plan.live.segments.len(),
            sweep_segments: plan.sweep.segments.len(),
            sweep_index_rows: plan.sweep.by_commit.len()
                + plan.sweep.by_change.len()
                + plan.sweep.by_change_membership.len(),
        }
    }
}
