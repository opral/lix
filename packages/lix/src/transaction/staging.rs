#![allow(
    clippy::cloned_instead_of_copied,
    clippy::large_enum_variant,
    clippy::option_as_ref_cloned,
    clippy::option_if_let_else,
    clippy::ref_option,
    clippy::unnecessary_wraps,
    clippy::unused_self
)]

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use smallvec::SmallVec;

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::{BlobBytesBatch, BlobId};
use crate::catalog::SchemaPlanId;
use crate::changelog::{ChangeId, CommitId};
use crate::common::{LixTimestamp, SharedStr};
use crate::domain::{Domain, DomainRowIdentity};
#[cfg(test)]
use crate::functions::FunctionProvider;
use crate::functions::FunctionProviderHandle;
use crate::gc::CheckpointPublication;
#[cfg(test)]
use crate::hot_state::HotStateRowRequest;
#[cfg(test)]
use crate::hot_state::MaterializedHotStateRow;
use crate::hot_state::{
    CertifiedCurrentStatePredecessor, HotStateExactBatchRequest, HotStateExactRowRequest,
    HotStateScanRequest, MaterializedHotStateBatch, MaterializedHotStateBatchBuilder,
    MaterializedHotStateExactBatch,
};
use crate::row_pk::RowPk;
use crate::transaction::staged_commit_changes::StagedCommitChangeBatch;
use crate::transaction::staged_commit_changes::StagedCommitChangeRefs;
use crate::transaction_types::{
    CertifiedParameterReplacementBatch, CertifiedRawWriteBatchPreparation,
    CompleteCollectionReplacementProof, LogicalPrimaryKey, PreparedRowFacts, PreparedStateBatch,
    PreparedStateRowRef, PreparedTransactionWrite, StageJson, TransactionFileContent,
    TransactionJson, TransactionWriteMode, TransactionWriteOperation, TransactionWriteOrigin,
    TransactionWriteOutcome,
};
#[cfg(test)]
use crate::transaction_types::{TestPreparedStateRow, stage_json_from_value};
use crate::{LixError, NullableKeyFilter};

/// Transaction-local write buffer after transaction-boundary preparation.
///
/// This is the engine seam between SQL execution and transaction ownership:
/// write frontends pass one typed `RawWriteBatch` to `Transaction`, the
/// transaction prepares it into a stable `PreparedStateBatch`, reads build a
/// `PreparedStateRowOverlay` over that batch, and commit drains the same owner.
pub(crate) struct TransactionWriteBuffer {
    functions: FunctionProviderHandle,
    rows: Mutex<StagedPreparedRows>,
    ordered_mutations: Mutex<Option<OrderedMutationJournal>>,
    commit_change_refs_by_branch: Mutex<BTreeMap<String, StagedCommitChangeRefs>>,
    first_commit_parent_override_by_branch: Mutex<BTreeMap<String, CommitId>>,
    checkpoint_publications: Mutex<Vec<CheckpointPublication>>,
    extra_commit_parents_by_branch: Mutex<BTreeMap<String, Vec<CommitId>>>,
    intermediate_commits: Mutex<Vec<StagedIntermediateCommit>>,
    file_content_writes: Mutex<Vec<TransactionFileContent>>,
}

/// A transaction-local statement checkpoint.
///
/// SQL writes coalesce rows, update their commit membership, and can attach
/// file payloads, so a row-count marker cannot restore the previous journal.
/// This owns the prepared-row owners and transaction control structures needed
/// to restore an explicit transaction after a post-stage SQL error.
pub(crate) struct TransactionWriteBufferCheckpoint {
    rows: StagedPreparedRows,
    ordered_mutations: Option<OrderedMutationJournal>,
    commit_change_refs_by_branch: BTreeMap<String, StagedCommitChangeRefs>,
    first_commit_parent_override_by_branch: BTreeMap<String, CommitId>,
    checkpoint_publications: Vec<CheckpointPublication>,
    extra_commit_parents_by_branch: BTreeMap<String, Vec<CommitId>>,
    intermediate_commits: Vec<StagedIntermediateCommit>,
    file_content_writes: Vec<TransactionFileContent>,
}

/// One immutable, fixed-shape journal chunk produced by typed SQL mutation
/// ingress. Identity and canonical JSON remain columnar owners; commit can
/// borrow them directly while encoding immutable mutation parts.
#[derive(Clone, Debug)]
pub(crate) struct ImmutableMutationJournalChunk {
    schema_plan_id: SchemaPlanId,
    schema_key: SharedStr,
    branch_id: SharedStr,
    origin_key: Option<SharedStr>,
    identity_arena: SharedStr,
    identity_offsets: Arc<[(u32, u32)]>,
    snapshot_arena: SharedStr,
    snapshot_offsets: Arc<[(u32, u32)]>,
    large_snapshot_refs: Arc<[(u32, crate::json_store::JsonRef)]>,
    sealed_replacement_parts: Option<Arc<[crate::tracked_state::EncodedReplacementPart]>>,
    durable_predecessors: Option<Arc<[CertifiedCurrentStatePredecessor]>>,
    timestamp: LixTimestamp,
}

impl PartialEq for ImmutableMutationJournalChunk {
    fn eq(&self, other: &Self) -> bool {
        self.schema_plan_id == other.schema_plan_id
            && self.schema_key == other.schema_key
            && self.branch_id == other.branch_id
            && self.origin_key == other.origin_key
            && self.identity_arena == other.identity_arena
            && self.identity_offsets == other.identity_offsets
            && self.snapshot_arena == other.snapshot_arena
            && self.snapshot_offsets == other.snapshot_offsets
            && self.large_snapshot_refs == other.large_snapshot_refs
            && self.sealed_replacement_parts == other.sealed_replacement_parts
            && self.timestamp == other.timestamp
            && self.durable_predecessors.as_ref().map(|values| {
                values
                    .iter()
                    .map(CertifiedCurrentStatePredecessor::created_at)
                    .collect::<Result<Vec<_>, _>>()
                    .ok()
            }) == other.durable_predecessors.as_ref().map(|values| {
                values
                    .iter()
                    .map(CertifiedCurrentStatePredecessor::created_at)
                    .collect::<Result<Vec<_>, _>>()
                    .ok()
            })
    }
}

impl Eq for ImmutableMutationJournalChunk {}

impl ImmutableMutationJournalChunk {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn try_new_single_string_identities(
        schema_plan_id: SchemaPlanId,
        schema_key: SharedStr,
        branch_id: SharedStr,
        origin_key: Option<SharedStr>,
        identity_arena: Vec<u8>,
        identity_offsets: Vec<(usize, usize)>,
        snapshot_arena: Vec<u8>,
        snapshot_offsets: Vec<(usize, usize)>,
        durable_predecessors: Option<Vec<CertifiedCurrentStatePredecessor>>,
        timestamp: LixTimestamp,
    ) -> Result<Self, LixError> {
        if identity_offsets.len() != snapshot_offsets.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable mutation identity arena is misaligned",
            ));
        }
        let identity_arena = SharedStr::from_utf8(Bytes::from(identity_arena)).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable mutation identity arena is not UTF-8",
            )
        })?;
        let mut previous_end = 0usize;
        let mut offsets = Vec::with_capacity(identity_offsets.len());
        let mut previous_identity = None;
        for (start, end) in identity_offsets {
            if start != previous_end || end < start || end > identity_arena.len() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation identity offsets are invalid",
                ));
            }
            let value = identity_arena.as_str().get(start..end).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation identity offset splits UTF-8",
                )
            })?;
            if previous_identity.is_some_and(|previous| previous >= value) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation journal identities are not strictly ordered",
                ));
            }
            offsets.push((
                u32::try_from(start).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "immutable mutation identity arena exceeds u32",
                    )
                })?,
                u32::try_from(end).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "immutable mutation identity arena exceeds u32",
                    )
                })?,
            ));
            previous_identity = Some(value);
            previous_end = end;
        }
        if previous_end != identity_arena.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable mutation identity offsets do not cover the arena",
            ));
        }
        Self::try_new_validated_single_string_identities(
            schema_plan_id,
            schema_key,
            branch_id,
            origin_key,
            identity_arena,
            offsets.into(),
            snapshot_arena,
            snapshot_offsets,
            durable_predecessors,
            timestamp,
        )
    }

    #[expect(clippy::too_many_arguments)]
    fn try_new_validated_single_string_identities(
        schema_plan_id: SchemaPlanId,
        schema_key: SharedStr,
        branch_id: SharedStr,
        origin_key: Option<SharedStr>,
        identity_arena: SharedStr,
        identity_offsets: Arc<[(u32, u32)]>,
        snapshot_arena: Vec<u8>,
        snapshot_offsets: Vec<(usize, usize)>,
        durable_predecessors: Option<Vec<CertifiedCurrentStatePredecessor>>,
        timestamp: LixTimestamp,
    ) -> Result<Self, LixError> {
        let row_count = identity_offsets.len();
        if row_count == 0 || row_count != snapshot_offsets.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable mutation journal columns are empty or misaligned",
            ));
        }
        if durable_predecessors
            .as_ref()
            .is_some_and(|predecessors| predecessors.len() != row_count)
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable mutation predecessor column is misaligned",
            ));
        }
        let snapshot_arena = SharedStr::from_utf8(Bytes::from(snapshot_arena)).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable mutation journal arena is not UTF-8",
            )
        })?;
        let arena_len = snapshot_arena.len();
        let mut previous_end = 0usize;
        let mut offsets = Vec::with_capacity(snapshot_offsets.len());
        for (start, end) in snapshot_offsets {
            if start != previous_end || end <= start || end > arena_len {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation journal offsets are invalid",
                ));
            }
            snapshot_arena.as_str().get(start..end).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation journal offset splits UTF-8",
                )
            })?;
            offsets.push((
                u32::try_from(start).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "immutable mutation journal arena exceeds u32",
                    )
                })?,
                u32::try_from(end).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "immutable mutation journal arena exceeds u32",
                    )
                })?,
            ));
            previous_end = end;
        }
        if previous_end != arena_len {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable mutation journal offsets do not cover the arena",
            ));
        }
        let large_snapshot_refs = offsets
            .iter()
            .enumerate()
            .filter_map(|(index, &(start, end))| {
                let bytes = &snapshot_arena.as_bytes()[start as usize..end as usize];
                (bytes.len() > crate::json_store::JSON_INLINE_MAX_BYTES).then(|| {
                    (
                        u32::try_from(index)
                            .expect("immutable mutation chunk row ordinal fits u32"),
                        crate::json_store::JsonRef::for_content(bytes),
                    )
                })
            })
            .collect::<Vec<_>>();
        Ok(Self {
            schema_plan_id,
            schema_key,
            branch_id,
            origin_key,
            identity_arena,
            identity_offsets,
            snapshot_arena,
            snapshot_offsets: offsets.into(),
            large_snapshot_refs: large_snapshot_refs.into(),
            sealed_replacement_parts: None,
            durable_predecessors: durable_predecessors.map(Into::into),
            timestamp,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.identity_offsets.len()
    }

    pub(crate) fn materialized_row_pks(&self) -> Arc<[RowPk]> {
        self.identity_offsets
            .iter()
            .map(|&(start, end)| {
                let value = self
                    .identity_arena
                    .slice(start as usize..end as usize)
                    .expect("validated immutable mutation identity remains in its arena");
                RowPk::from_validated_shared_string(value)
            })
            .collect::<Vec<_>>()
            .into()
    }

    pub(crate) fn attach_durable_predecessors(
        &mut self,
        predecessors: Vec<CertifiedCurrentStatePredecessor>,
    ) -> Result<(), LixError> {
        if predecessors.len() != self.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable mutation predecessor column does not match its rows",
            ));
        }
        self.durable_predecessors = Some(predecessors.into());
        Ok(())
    }

    fn identity(&self, index: usize) -> &str {
        let (start, end) = self.identity_offsets[index];
        self.identity_arena
            .as_str()
            .get(start as usize..end as usize)
            .expect("validated immutable mutation identity UTF-8")
    }

    pub(crate) fn snapshot(&self, index: usize) -> &str {
        let (start, end) = self.snapshot_offsets[index];
        self.snapshot_arena
            .as_str()
            .get(start as usize..end as usize)
            .expect("validated immutable mutation journal UTF-8")
    }

    pub(crate) fn snapshot_slot(&self, index: usize) -> crate::json_store::JsonSlotRef<'_> {
        let snapshot = self.snapshot(index);
        if snapshot.len() <= crate::json_store::JSON_INLINE_MAX_BYTES {
            return crate::json_store::JsonSlotRef::Inline(snapshot);
        }
        let index = u32::try_from(index).expect("immutable mutation chunk row ordinal fits u32");
        let position = self
            .large_snapshot_refs
            .binary_search_by_key(&index, |(ordinal, _)| *ordinal)
            .expect("large immutable mutation snapshot has a content ref");
        crate::json_store::JsonSlotRef::Ref(&self.large_snapshot_refs[position].1)
    }

    pub(crate) fn seal_replacement_parts(
        &mut self,
        finalize_tail: bool,
        compressor: &mut Option<crate::compression::ZstdLevel1Compressor>,
    ) -> Result<(), LixError> {
        if self.sealed_replacement_parts.is_some() {
            return Ok(());
        }
        if !finalize_tail
            && !self
                .len()
                .is_multiple_of(crate::tracked_state::REPLACEMENT_PART_MAX_ROWS)
        {
            return Ok(());
        }
        let mut parts = Vec::with_capacity(
            self.len()
                .div_ceil(crate::tracked_state::REPLACEMENT_PART_MAX_ROWS),
        );
        #[cfg(feature = "storage-benches")]
        let mut generated_key_bytes = 0usize;
        let mut first = 0usize;
        while first < self.len() {
            let max_candidate_len =
                (self.len() - first).min(crate::tracked_state::REPLACEMENT_PART_MAX_ROWS);
            let mut key_arena = Vec::new();
            let mut key_offsets = Vec::with_capacity(max_candidate_len);
            for index in first..first + max_candidate_len {
                let start = key_arena.len();
                crate::tracked_state::encode_single_string_key_ref_into(
                    &mut key_arena,
                    self.schema_key(),
                    None,
                    self.identity(index),
                );
                #[cfg(feature = "storage-benches")]
                {
                    generated_key_bytes =
                        generated_key_bytes.saturating_add(key_arena.len().saturating_sub(start));
                }
                key_offsets.push((start, key_arena.len()));
            }
            let mut candidate_len = max_candidate_len;
            let encoded = loop {
                let rows = key_offsets[..candidate_len]
                    .iter()
                    .enumerate()
                    .map(
                        |(offset, &(start, end))| crate::tracked_state::ReplacementPartRowRef {
                            encoded_key: &key_arena[start..end],
                            snapshot: self.snapshot_slot(first + offset),
                            metadata: crate::json_store::JsonSlotRef::None,
                        },
                    )
                    .collect::<Vec<_>>();
                match crate::tracked_state::encode_replacement_part_with_compressor(
                    &rows, compressor,
                ) {
                    Ok(encoded)
                        if encoded.bytes().len()
                            <= crate::tracked_state::REPLACEMENT_PART_TARGET_BYTES
                            || candidate_len == 1 =>
                    {
                        break encoded;
                    }
                    Ok(_) | Err(_) if candidate_len > 1 => {
                        // The canonical commit encoder retains the rejected
                        // suffix and admits following rows into it. A
                        // non-final journal chunk cannot reproduce that
                        // state across its boundary, so leave this chunk and
                        // every later chunk for the one-pass commit encoder.
                        if !finalize_tail {
                            return Ok(());
                        }
                        candidate_len = candidate_len.div_ceil(2);
                    }
                    Err(error) => return Err(error),
                    Ok(_) => unreachable!("single-row replacement part satisfies the size guard"),
                }
            };
            first += candidate_len;
            parts.push(encoded);
        }
        #[cfg(feature = "storage-benches")]
        {
            let encoded_bytes = parts.iter().map(|part| part.bytes().len()).sum::<usize>();
            crate::storage_bench::record_crud_ownership(
                crate::storage_bench::CRUD_OWNERSHIP_JOURNAL_SEAL,
                self.len(),
                generated_key_bytes,
                encoded_bytes,
                parts.len(),
                0,
                0,
            );
            crate::storage_bench::record_crud_ownership_transfer(
                crate::storage_bench::CRUD_OWNERSHIP_JOURNAL_SEAL,
                generated_key_bytes.saturating_add(encoded_bytes),
                0,
                encoded_bytes,
                generated_key_bytes,
            );
        }
        self.sealed_replacement_parts = Some(parts.into());
        Ok(())
    }

    pub(crate) fn sealed_replacement_parts(
        &self,
    ) -> Option<&[crate::tracked_state::EncodedReplacementPart]> {
        self.sealed_replacement_parts.as_deref()
    }

    pub(crate) fn schema_key(&self) -> &str {
        self.schema_key.as_str()
    }

    pub(crate) fn branch_id(&self) -> &str {
        self.branch_id.as_str()
    }

    pub(crate) fn origin_key(&self) -> Option<&str> {
        self.origin_key.as_deref()
    }

    pub(crate) fn timestamp(&self) -> LixTimestamp {
        self.timestamp
    }

    pub(crate) fn into_prepared(
        self,
        allow_missing_predecessors: bool,
    ) -> Result<PreparedStateBatch, LixError> {
        let row_pks = self.materialized_row_pks();
        let offsets = self
            .snapshot_offsets
            .iter()
            .map(|&(start, end)| (start as usize, end as usize))
            .collect();
        let mut rows = CertifiedParameterReplacementBatch::new(
            row_pks.iter().cloned().collect(),
            TransactionJson::from_certified_row_content_arena(
                self.snapshot_arena.as_bytes().to_vec(),
                offsets,
            )?,
            self.schema_key,
            self.branch_id,
            CertifiedRawWriteBatchPreparation {
                schema_plan_id: self.schema_plan_id,
                facts: PreparedRowFacts {
                    row_content_validated: true,
                    requires_transaction_validation: false,
                },
                tracked_keys_strictly_ordered: true,
                complete_collection_replacement: None,
            },
        )?
        .into_dense_prepared(self.origin_key.as_ref(), self.timestamp)?;
        let Some(predecessors) = self.durable_predecessors else {
            if allow_missing_predecessors {
                return Ok(rows);
            }
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "partial immutable mutation journal lacks durable predecessor evidence",
            ));
        };
        for (index, predecessor) in predecessors.iter().cloned().enumerate() {
            rows.set_durable_predecessor(index, Some(predecessor));
        }
        Ok(rows)
    }
}

/// Certified transaction-wide sequence of immutable journal chunks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OrderedMutationJournal {
    /// Persistent chunk directory. Statement checkpoints clone this owner on
    /// every scalar execute, so the directory itself must remain O(1) to
    /// checkpoint; only the once-per-4K append performs copy-on-write.
    chunks: Arc<Vec<ImmutableMutationJournalChunk>>,
    row_count: usize,
    commit_id: CommitId,
    replacement_proof: Option<CompleteCollectionReplacementProof>,
    overlay_uniform_created_at: Option<LixTimestamp>,
}

#[derive(Clone, Copy)]
pub(crate) struct OrderedMutationJournalRowRef<'a> {
    chunk: &'a ImmutableMutationJournalChunk,
    row_index: usize,
}

impl<'a> OrderedMutationJournalRowRef<'a> {
    pub(crate) fn identity(&self) -> &'a str {
        self.chunk.identity(self.row_index)
    }

    pub(crate) fn snapshot(&self) -> &'a str {
        self.chunk.snapshot(self.row_index)
    }

    pub(crate) fn snapshot_slot(&self) -> crate::json_store::JsonSlotRef<'a> {
        self.chunk.snapshot_slot(self.row_index)
    }
}

#[derive(Clone)]
pub(crate) struct OrderedMutationJournalRows<'a> {
    journal: &'a OrderedMutationJournal,
    chunk_index: usize,
    row_index: usize,
    remaining: usize,
}

/// Cheap read-only identity projection used to bulk-hydrate provisional
/// predecessor evidence before generic lowering.
#[derive(Clone)]
pub(crate) struct ProvisionalMutationJournalDescriptor {
    schema_key: SharedStr,
    branch_id: SharedStr,
    row_pk_chunks: Vec<Arc<[RowPk]>>,
    predecessors_complete: bool,
}

pub(crate) enum ImmutableMutationChunkStage {
    Staged,
    RequiresGeneric(ImmutableMutationJournalChunk),
}

impl ProvisionalMutationJournalDescriptor {
    pub(crate) fn schema_key(&self) -> &str {
        self.schema_key.as_str()
    }

    pub(crate) fn branch_id(&self) -> &str {
        self.branch_id.as_str()
    }

    pub(crate) fn row_pk_chunks(&self) -> &[Arc<[RowPk]>] {
        &self.row_pk_chunks
    }

    pub(crate) fn predecessors_complete(&self) -> bool {
        self.predecessors_complete
    }
}

impl<'a> Iterator for OrderedMutationJournalRows<'a> {
    type Item = OrderedMutationJournalRowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.chunk_index < self.journal.chunks.len() {
            let chunk = &self.journal.chunks[self.chunk_index];
            if self.row_index < chunk.len() {
                let row = OrderedMutationJournalRowRef {
                    chunk,
                    row_index: self.row_index,
                };
                self.row_index += 1;
                self.remaining -= 1;
                return Some(row);
            }
            self.chunk_index += 1;
            self.row_index = 0;
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for OrderedMutationJournalRows<'_> {}

impl OrderedMutationJournal {
    pub(crate) fn iter(&self) -> OrderedMutationJournalRows<'_> {
        OrderedMutationJournalRows {
            journal: self,
            chunk_index: 0,
            row_index: 0,
            remaining: self.row_count,
        }
    }

    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn sealed_replacement_prefix(
        &self,
    ) -> (usize, Vec<crate::tracked_state::EncodedReplacementPart>) {
        let mut parts = Vec::new();
        let mut row_count = 0usize;
        for chunk in self.chunks.iter() {
            let Some(sealed) = chunk.sealed_replacement_parts() else {
                break;
            };
            parts.extend_from_slice(sealed);
            row_count += chunk.len();
        }
        (row_count, parts)
    }

    pub(crate) fn commit_id(&self) -> CommitId {
        self.commit_id
    }

    pub(crate) fn schema_key(&self) -> &str {
        self.chunks[0].schema_key()
    }

    pub(crate) fn branch_id(&self) -> &str {
        self.chunks[0].branch_id()
    }

    pub(crate) fn timestamp(&self) -> LixTimestamp {
        self.chunks[0].timestamp()
    }

    pub(crate) fn replacement_proof(&self) -> CompleteCollectionReplacementProof {
        self.replacement_proof
            .expect("drained ordered mutation journal is replacement-certified")
    }

    fn into_prepared(self) -> Result<PreparedStateBatch, LixError> {
        let proof = self.replacement_proof;
        let schema_key = self.schema_key().to_owned();
        let branch_id = self.branch_id().to_owned();
        let mut chunks = Arc::try_unwrap(self.chunks)
            .unwrap_or_else(|chunks| (*chunks).clone())
            .into_iter();
        let Some(first) = chunks.next() else {
            return Ok(PreparedStateBatch::new());
        };
        let mut rows = first.into_prepared(proof.is_some())?;
        for chunk in chunks {
            rows.append(chunk.into_prepared(proof.is_some())?);
        }
        rows.set_commit_id_all(self.commit_id);
        if let Some(proof) = proof {
            if !rows.certify_complete_collection_replacement(
                &schema_key,
                &branch_id,
                u64::try_from(rows.len()).expect("prepared replacement row count fits u64"),
                proof.ordered_identity_digest,
            ) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable replacement proof changed during generic lowering",
                ));
            }
        }
        Ok(rows)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StagedIntermediateCommit {
    pub(crate) branch_id: String,
    pub(crate) parent_commit_id: CommitId,
    pub(crate) change_refs: StagedCommitChangeRefs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum RowSlot {
    State(usize),
}

#[derive(Clone, Default)]
struct StagedScanFileCandidates {
    slots_by_value: HashMap<SharedStr, SmallVec<[RowSlot; 1]>>,
    null_slots: SmallVec<[RowSlot; 1]>,
}

/// Narrows row- or file-constrained scans over an indexed transaction
/// overlay without changing the journal's identity/coalescing semantics.
/// Branch and durability remain post-filter checks because one indexed
/// candidate can legitimately have multiple such physical rows while staged.
#[derive(Clone, Default)]
struct StagedScanCandidateIndex {
    slots_by_schema: HashMap<SharedStr, SmallVec<[RowSlot; 1]>>,
    slots_by_schema_and_row: HashMap<SharedStr, HashMap<RowPk, SmallVec<[RowSlot; 1]>>>,
    slots_by_schema_and_file: HashMap<SharedStr, StagedScanFileCandidates>,
}

impl StagedScanCandidateIndex {
    fn insert(&mut self, row: PreparedStateRowRef<'_>, slot: RowSlot) {
        self.slots_by_schema
            .entry(row.schema_key.clone())
            .or_default()
            .push(slot);
        self.slots_by_schema_and_row
            .entry(row.schema_key.clone())
            .or_default()
            .entry(row.row_pk.clone())
            .or_default()
            .push(slot);

        let by_file = self
            .slots_by_schema_and_file
            .entry(row.schema_key.clone())
            .or_default();
        if let Some(file_id) = row.file_id {
            by_file
                .slots_by_value
                .entry(file_id.clone())
                .or_default()
                .push(slot);
        } else {
            by_file.null_slots.push(slot);
        }
    }

    /// Returns a strict superset of the staged rows a scan can observe when
    /// schema plus either row or file identity are constrained. The
    /// remaining scan filters are deliberately applied by the established
    /// matcher afterwards.
    fn slots_for_filter<'a>(
        &'a self,
        filter: &crate::hot_state::HotStateFilter,
    ) -> Option<Cow<'a, [RowSlot]>> {
        if filter.schema_keys.is_empty() {
            return None;
        }

        if !filter.row_pks.is_empty() {
            if let ([schema_key], [row_pk]) =
                (filter.schema_keys.as_slice(), filter.row_pks.as_slice())
            {
                let slots = self
                    .slots_by_schema_and_row
                    .get(schema_key.as_str())
                    .and_then(|by_row| by_row.get(row_pk))
                    .map(SmallVec::as_slice)
                    .unwrap_or(&[]);
                return Some(Cow::Borrowed(slots));
            }

            let mut slots = Vec::new();
            for schema_key in &filter.schema_keys {
                let Some(by_row) = self.slots_by_schema_and_row.get(schema_key.as_str()) else {
                    continue;
                };
                for row_pk in &filter.row_pks {
                    if let Some(candidate_slots) = by_row.get(row_pk) {
                        slots.extend(candidate_slots.iter().copied());
                    }
                }
            }
            slots.sort_unstable();
            slots.dedup();
            return Some(Cow::Owned(slots));
        }

        if filter.file_ids.is_empty()
            || filter
                .file_ids
                .iter()
                .any(|file_id| matches!(file_id, NullableKeyFilter::Any))
        {
            if let [schema_key] = filter.schema_keys.as_slice() {
                return Some(Cow::Borrowed(
                    self.slots_by_schema
                        .get(schema_key.as_str())
                        .map(SmallVec::as_slice)
                        .unwrap_or(&[]),
                ));
            }

            let mut slots = Vec::new();
            for schema_key in &filter.schema_keys {
                if let Some(candidate_slots) = self.slots_by_schema.get(schema_key.as_str()) {
                    slots.extend(candidate_slots.iter().copied());
                }
            }
            slots.sort_unstable();
            slots.dedup();
            return Some(Cow::Owned(slots));
        }

        if let ([schema_key], [file_id]) =
            (filter.schema_keys.as_slice(), filter.file_ids.as_slice())
        {
            let slots = self
                .slots_by_schema_and_file
                .get(schema_key.as_str())
                .map(|by_file| match file_id {
                    NullableKeyFilter::Null => by_file.null_slots.as_slice(),
                    NullableKeyFilter::Value(file_id) => by_file
                        .slots_by_value
                        .get(file_id.as_str())
                        .map(SmallVec::as_slice)
                        .unwrap_or(&[]),
                    NullableKeyFilter::Any => unreachable!("handled above"),
                })
                .unwrap_or(&[]);
            return Some(Cow::Borrowed(slots));
        }

        let mut slots = Vec::new();
        for schema_key in &filter.schema_keys {
            let Some(by_file) = self.slots_by_schema_and_file.get(schema_key.as_str()) else {
                continue;
            };
            for file_id in &filter.file_ids {
                match file_id {
                    NullableKeyFilter::Null => {
                        slots.extend(by_file.null_slots.iter().copied());
                    }
                    NullableKeyFilter::Value(file_id) => {
                        if let Some(candidate_slots) = by_file.slots_by_value.get(file_id.as_str())
                        {
                            slots.extend(candidate_slots.iter().copied());
                        }
                    }
                    NullableKeyFilter::Any => unreachable!("handled above"),
                }
            }
        }
        slots.sort_unstable();
        slots.dedup();
        Some(Cow::Owned(slots))
    }
}

/// The normal write path is an ordered journal. It becomes an indexed overlay
/// only if a later write overlaps it or a transaction-local read actually
/// needs read-your-writes semantics.
#[derive(Clone)]
enum StagedPreparedRows {
    AppendOnly {
        rows: PreparedStateBatch,
        insert_selection: PreparedInsertSelection,
        last_key: Option<TrackedStateKey>,
    },
    Indexed {
        rows: PreparedStateBatch,
        insert_selection: PreparedInsertSelection,
        by_identity: HashMap<PreparedStateRowIdentity, RowSlot>,
        by_candidate: StagedScanCandidateIndex,
    },
}

impl Default for StagedPreparedRows {
    fn default() -> Self {
        Self::AppendOnly {
            rows: PreparedStateBatch::new(),
            insert_selection: PreparedInsertSelection::new(),
            last_key: None,
        }
    }
}

enum AppendOnlyStage {
    Staged,
    Fallback(PreparedStateBatch),
}

/// The ordering used by the tracked-state tree. This deliberately differs
/// from [`PreparedStateRowIdentity`], whose row-first order is for exact
/// lookup rather than bulk root construction.
#[derive(Clone)]
struct TrackedStateKey {
    schema_key: SharedStr,
    file_id: Option<SharedStr>,
    row_pk: RowPk,
}

impl TrackedStateKey {
    fn from_row(row: PreparedStateRowRef<'_>) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.cloned(),
            row_pk: row.row_pk.clone(),
        }
    }
}

/// Drained prepared transaction writes ready for commit.
#[derive(Clone)]
pub(crate) struct PreparedWriteSet {
    pub(crate) state_rows: PreparedStateBatch,
    pub(crate) insert_selection: PreparedInsertSelection,
    pub(crate) commit_change_refs_by_branch: BTreeMap<String, StagedCommitChangeRefs>,
    pub(crate) first_commit_parent_override_by_branch: BTreeMap<String, CommitId>,
    pub(crate) checkpoint_publications: Vec<CheckpointPublication>,
    pub(crate) extra_commit_parents_by_branch: BTreeMap<String, Vec<CommitId>>,
    pub(crate) intermediate_commits: Vec<StagedIntermediateCommit>,
    pub(crate) file_content_writes: Vec<TransactionFileContent>,
}

#[derive(Clone)]
pub(crate) struct DrainedMutationJournalDescriptor {
    pub(crate) commit_id: CommitId,
    pub(crate) schema_key: String,
    pub(crate) branch_id: String,
    pub(crate) row_pk_chunks: Vec<Arc<[RowPk]>>,
}

pub(crate) struct PreparedWriteValidationSet<'a> {
    state_rows: &'a PreparedStateBatch,
    insert_selection: &'a PreparedInsertSelection,
    rows: Vec<PreparedValidationRow<'a>>,
    constraint_rows: Vec<PreparedValidationRow<'a>>,
    insert_ordinals: Vec<u32>,
}

pub(crate) struct PreparedWriteValidationIndex<'a> {
    state_rows: &'a PreparedStateBatch,
    insert_selection: &'a PreparedInsertSelection,
    rows_by_schema_scope: BTreeMap<Domain, Vec<PreparedValidationRow<'a>>>,
    insert_ordinals_by_schema_scope: BTreeMap<Domain, Vec<u32>>,
}

/// Compact logical-INSERT metadata aligned with a [`PreparedStateBatch`].
///
/// The bitset identifies final row ordinals that still carry INSERT absence
/// semantics after transaction-local coalescing. Original SQL origin metadata
/// stays in one contiguous parallel column so an INSERT followed by an UPDATE
/// retains the original primary-key error surface without cloning the row's
/// schema, file, branch, or row identity.
#[derive(Debug, Clone, Default)]
pub(crate) struct PreparedInsertSelection {
    row_count: usize,
    count: usize,
    bits: Vec<u64>,
    origins: Vec<Option<TransactionWriteOrigin>>,
    statement_indices: Vec<u32>,
    statement_indices_are_row_ordinals: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct PreparedInsertRef<'a> {
    pub(crate) row_index: usize,
    pub(crate) row: PreparedStateRowRef<'a>,
    pub(crate) origin: Option<&'a TransactionWriteOrigin>,
    pub(crate) statement_index: Option<usize>,
}

impl PreparedInsertSelection {
    const WORD_BITS: usize = u64::BITS as usize;
    const NO_STATEMENT_INDEX: u32 = u32::MAX;

    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn with_row_capacity(row_capacity: usize) -> Self {
        Self {
            row_count: 0,
            count: 0,
            bits: Vec::with_capacity(row_capacity.div_ceil(Self::WORD_BITS)),
            origins: Vec::new(),
            statement_indices: Vec::new(),
            statement_indices_are_row_ordinals: false,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.count
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub(crate) fn covers_all(&self, row_count: usize) -> bool {
        self.row_count == row_count && self.count == row_count
    }

    pub(crate) fn contains(&self, row_index: usize) -> bool {
        if row_index >= self.row_count {
            return false;
        }
        let word = row_index / Self::WORD_BITS;
        let bit = row_index % Self::WORD_BITS;
        self.bits
            .get(word)
            .is_some_and(|word| word & (1_u64 << bit) != 0)
    }

    pub(crate) fn origin(&self, row_index: usize) -> Option<&TransactionWriteOrigin> {
        self.contains(row_index)
            .then(|| self.origins.get(row_index).and_then(Option::as_ref))
            .flatten()
    }

    pub(crate) fn iter<'a>(
        &'a self,
        rows: &'a PreparedStateBatch,
    ) -> impl Iterator<Item = PreparedInsertRef<'a>> + 'a {
        (0..self.row_count)
            .filter(|&row_index| self.contains(row_index))
            .map(|row_index| PreparedInsertRef {
                row_index,
                row: rows.row(row_index),
                origin: self.origins.get(row_index).and_then(Option::as_ref),
                statement_index: self.statement_index(row_index),
            })
    }

    fn push(&mut self, origin: Option<&TransactionWriteOrigin>, statement_index: Option<usize>) {
        self.materialize_statement_indices();
        let row_index = self.row_count;
        self.resize_rows(row_index + 1);
        self.mark(row_index, origin, statement_index);
    }

    fn push_certified_ordinal_inserts(&mut self, row_count: usize) {
        if self.row_count != 0 || self.count != 0 {
            self.reserve_rows(row_count, true);
            for statement_index in 0..row_count {
                self.push(None, Some(statement_index));
            }
            return;
        }
        self.row_count = row_count;
        self.count = row_count;
        self.bits = vec![u64::MAX; row_count.div_ceil(Self::WORD_BITS)];
        if let Some(last) = self.bits.last_mut()
            && !row_count.is_multiple_of(Self::WORD_BITS)
        {
            *last = (1_u64 << (row_count % Self::WORD_BITS)) - 1;
        }
        self.statement_indices_are_row_ordinals = true;
    }

    pub(crate) fn is_complete_ordinal_selection(&self, row_count: usize) -> bool {
        self.row_count == row_count
            && self.count == row_count
            && self.statement_indices_are_row_ordinals
            && self.origins.is_empty()
    }

    fn push_not_insert(&mut self) {
        self.resize_rows(self.row_count + 1);
    }

    fn reserve_rows(&mut self, additional: usize, may_insert: bool) {
        if !may_insert && self.is_empty() {
            return;
        }
        if !self.origins.is_empty() {
            self.origins.reserve(additional);
        }
        let final_words = self
            .row_count
            .saturating_add(additional)
            .div_ceil(Self::WORD_BITS);
        self.bits
            .reserve(final_words.saturating_sub(self.bits.len()));
    }

    fn resize_rows(&mut self, row_count: usize) {
        debug_assert!(row_count >= self.row_count);
        if !self.origins.is_empty() {
            self.origins.resize(row_count, None);
        }
        if !self.bits.is_empty() {
            self.bits.resize(row_count.div_ceil(Self::WORD_BITS), 0);
        }
        if !self.statement_indices.is_empty() {
            self.statement_indices
                .resize(row_count, Self::NO_STATEMENT_INDEX);
        }
        self.row_count = row_count;
    }

    fn mark(
        &mut self,
        row_index: usize,
        origin: Option<&TransactionWriteOrigin>,
        statement_index: Option<usize>,
    ) {
        self.materialize_statement_indices();
        debug_assert!(row_index < self.row_count);
        let word = row_index / Self::WORD_BITS;
        let bit = row_index % Self::WORD_BITS;
        let mask = 1_u64 << bit;
        if self.bits.is_empty() {
            self.bits
                .resize(self.row_count.div_ceil(Self::WORD_BITS), 0);
        }
        if self.bits[word] & mask == 0 {
            self.bits[word] |= mask;
            self.count += 1;
            if let Some(origin) = origin {
                if self.origins.is_empty() {
                    self.origins.resize(self.row_count, None);
                }
                self.origins[row_index] = Some(origin.clone());
            } else if !self.origins.is_empty() {
                self.origins[row_index] = None;
            }
        }
        if let Some(statement_index) = statement_index {
            let statement_index =
                u32::try_from(statement_index).expect("batch statement index must fit u32");
            if self.statement_indices.is_empty() {
                self.statement_indices
                    .resize(self.row_count, Self::NO_STATEMENT_INDEX);
            }
            self.statement_indices[row_index] = statement_index;
        }
    }

    fn materialize_statement_indices(&mut self) {
        if self.statement_indices_are_row_ordinals {
            self.statement_indices = (0..self.row_count)
                .map(|index| u32::try_from(index).expect("batch statement index must fit u32"))
                .collect();
            self.statement_indices_are_row_ordinals = false;
        }
    }

    fn statement_index(&self, row_index: usize) -> Option<usize> {
        if self.statement_indices_are_row_ordinals && row_index < self.row_count {
            return Some(row_index);
        }
        self.statement_indices
            .get(row_index)
            .copied()
            .filter(|index| *index != Self::NO_STATEMENT_INDEX)
            .map(|index| index as usize)
    }

    pub(crate) fn select_rows(&mut self, source_by_destination: &[usize]) {
        if self.is_empty() {
            self.row_count = source_by_destination.len();
            return;
        }
        let mut selected = Self::with_row_capacity(source_by_destination.len());
        for &source in source_by_destination {
            if self.contains(source) {
                selected.push(
                    self.origins.get(source).and_then(Option::as_ref),
                    self.statement_index(source),
                );
            } else {
                selected.push_not_insert();
            }
        }
        *self = selected;
    }

    pub(crate) fn append(&mut self, other: Self) {
        let other_rows = other.row_count;
        self.reserve_rows(other_rows, !other.is_empty());
        for row_index in 0..other_rows {
            if other.contains(row_index) {
                self.push(
                    other.origins.get(row_index).and_then(Option::as_ref),
                    other.statement_index(row_index),
                );
            } else {
                self.push_not_insert();
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn large_buffer_count(&self) -> usize {
        usize::from(!self.bits.is_empty()) + usize::from(!self.origins.is_empty())
    }

    #[cfg(test)]
    pub(crate) fn identity_copy_count(&self) -> usize {
        0
    }
}

#[derive(Clone, Copy)]
pub(crate) enum PreparedValidationRow<'a> {
    State(PreparedStateRowRef<'a>),
}

impl<'a> PreparedValidationRow<'a> {
    pub(crate) fn row_pk(&self) -> &RowPk {
        match self {
            Self::State(row) => row.row_pk,
        }
    }

    pub(crate) fn schema_plan_id(&self) -> SchemaPlanId {
        match self {
            Self::State(row) => row.schema_plan_id,
        }
    }

    pub(crate) fn schema_key(&self) -> &str {
        match self {
            Self::State(row) => row.schema_key.as_str(),
        }
    }

    pub(crate) fn file_id(&self) -> Option<&str> {
        match self {
            Self::State(row) => row.file_id.map(SharedStr::as_str),
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot_content(&self) -> Option<&str> {
        match self {
            Self::State(row) => row.snapshot.map(StageJson::normalized),
        }
    }

    pub(crate) fn snapshot_json(self) -> Option<&'a serde_json::Value> {
        match self {
            Self::State(row) => row.snapshot.map(StageJson::value),
        }
    }

    pub(crate) fn metadata_json(self) -> Option<&'a serde_json::Value> {
        match self {
            Self::State(row) => row.metadata.map(StageJson::value),
        }
    }

    pub(crate) fn row_content_validated(self) -> bool {
        match self {
            Self::State(row) => row.facts.row_content_validated,
        }
    }

    pub(crate) fn requires_transaction_validation(self) -> bool {
        match self {
            Self::State(row) => row.facts.requires_transaction_validation,
        }
    }

    pub(crate) fn origin(&self) -> Option<&TransactionWriteOrigin> {
        match self {
            Self::State(row) => row.origin,
        }
    }

    pub(crate) fn is_tombstone(&self) -> bool {
        match self {
            Self::State(row) => row.snapshot.is_none(),
        }
    }

    pub(crate) fn untracked(&self) -> bool {
        match self {
            Self::State(row) => row.untracked,
        }
    }

    pub(crate) fn branch_id(&self) -> &str {
        match self {
            Self::State(row) => &row.branch_id,
        }
    }

    pub(crate) fn domain(&self) -> Domain {
        Domain::exact_file(
            self.branch_id().to_string(),
            self.untracked(),
            self.file_id().map(str::to_owned),
        )
    }

    pub(crate) fn domain_row_identity(&self) -> DomainRowIdentity {
        DomainRowIdentity::in_domain(
            self.domain(),
            self.schema_key().to_string(),
            self.row_pk().clone(),
        )
    }
}

impl<'a> PreparedWriteValidationIndex<'a> {
    pub(crate) fn schema_scopes(&self) -> impl Iterator<Item = &Domain> {
        self.rows_by_schema_scope.keys()
    }

    pub(crate) fn validation_set_for_schema_scope(
        &self,
        schema_scope: &Domain,
    ) -> PreparedWriteValidationSet<'a> {
        let constraint_rows = self
            .rows_by_schema_scope
            .iter()
            .flat_map(|(target_scope, rows)| {
                rows.iter().copied().filter(move |row| {
                    schema_scope.validation_scope_contains_constraint_domain(target_scope)
                        || (row.is_tombstone()
                            && target_scope.tombstone_domain_affects_validation_scope(schema_scope))
                })
            })
            .collect();
        PreparedWriteValidationSet {
            state_rows: self.state_rows,
            insert_selection: self.insert_selection,
            rows: self
                .rows_by_schema_scope
                .get(schema_scope)
                .cloned()
                .unwrap_or_default(),
            constraint_rows,
            insert_ordinals: self
                .insert_ordinals_by_schema_scope
                .get(schema_scope)
                .cloned()
                .unwrap_or_default(),
        }
    }
}

impl<'a> PreparedWriteValidationSet<'a> {
    pub(crate) fn rows(&self) -> impl Iterator<Item = PreparedValidationRow<'a>> + '_ {
        self.rows.iter().copied()
    }

    pub(crate) fn constraint_rows(&self) -> impl Iterator<Item = PreparedValidationRow<'a>> + '_ {
        self.constraint_rows.iter().copied()
    }

    pub(crate) fn inserts(&self) -> impl Iterator<Item = PreparedInsertRef<'a>> + '_ {
        self.insert_ordinals.iter().map(|&ordinal| {
            let row_index = ordinal as usize;
            PreparedInsertRef {
                row_index,
                row: self.state_rows.row(row_index),
                origin: self.insert_selection.origin(row_index),
                statement_index: self.insert_selection.statement_index(row_index),
            }
        })
    }
}

impl PreparedWriteSet {
    pub(crate) fn ordered_mutation_journal_descriptors(
        &self,
    ) -> Vec<DrainedMutationJournalDescriptor> {
        self.commit_change_refs_by_branch
            .values()
            .filter_map(|refs| {
                refs.ordered_mutation_journal()
                    .map(|journal| DrainedMutationJournalDescriptor {
                        commit_id: refs.commit_id,
                        schema_key: journal.schema_key().to_owned(),
                        branch_id: journal.branch_id().to_owned(),
                        row_pk_chunks: journal
                            .chunks
                            .iter()
                            .map(ImmutableMutationJournalChunk::materialized_row_pks)
                            .collect(),
                    })
            })
            .collect()
    }

    pub(crate) fn hydrate_and_lower_ordered_mutation_journals(
        &mut self,
        mut predecessors_by_commit: BTreeMap<CommitId, Vec<CertifiedCurrentStatePredecessor>>,
    ) -> Result<(), LixError> {
        let journals = self
            .commit_change_refs_by_branch
            .values_mut()
            .filter_map(|refs| {
                refs.take_ordered_mutation_journal()
                    .map(|journal| (refs.commit_id, journal))
            })
            .collect::<Vec<_>>();
        for (commit_id, journal) in journals {
            let mut journal = Arc::try_unwrap(journal).unwrap_or_else(|journal| (*journal).clone());
            let mut predecessors = predecessors_by_commit
                .remove(&commit_id)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "stale immutable journal lacks hydrated predecessor evidence",
                    )
                })?
                .into_iter();
            for chunk in Arc::make_mut(&mut journal.chunks) {
                let values = predecessors.by_ref().take(chunk.len()).collect::<Vec<_>>();
                chunk.attach_durable_predecessors(values)?;
            }
            if predecessors.next().is_some() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "stale immutable journal has excess predecessor evidence",
                ));
            }
            let rows = journal.into_prepared()?;
            let previous_len = self.state_rows.len();
            let row_count = rows.len();
            self.state_rows.append(rows);
            self.insert_selection.resize_rows(previous_len);
            for _ in 0..row_count {
                self.insert_selection.push_not_insert();
            }
        }
        if !predecessors_by_commit.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "predecessor evidence does not belong to an immutable journal",
            ));
        }
        Ok(())
    }

    pub(crate) fn append_cohort_member(
        &mut self,
        mut other: Self,
        branch_id: &str,
        cohort_commit_id: CommitId,
    ) -> Result<(), LixError> {
        if !other.first_commit_parent_override_by_branch.is_empty()
            || !other.checkpoint_publications.is_empty()
            || !other.extra_commit_parents_by_branch.is_empty()
            || !other.intermediate_commits.is_empty()
        {
            return Err(LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                "transaction uses history controls that cannot join a commit cohort",
            ));
        }
        let other_refs = other
            .commit_change_refs_by_branch
            .remove(branch_id)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_TRANSACTION_CONFLICT,
                    "transaction is missing its active-branch commit membership",
                )
            })?;
        if !other.commit_change_refs_by_branch.is_empty() {
            return Err(LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                "transaction writes more than one branch and cannot join a commit cohort",
            ));
        }
        let cohort_refs = self
            .commit_change_refs_by_branch
            .get_mut(branch_id)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "commit cohort leader is missing active-branch membership",
                )
            })?;
        cohort_refs.absorb_cohort_membership(other_refs);
        other.state_rows.set_commit_id_all(cohort_commit_id);
        self.insert_selection.append(other.insert_selection);
        self.state_rows.append(other.state_rows);
        self.file_content_writes
            .append(&mut other.file_content_writes);
        Ok(())
    }

    /// Replaces every original row whose identity appears in `replacement`.
    /// `file_ids` additionally selects projected byte writes to supersede; it
    /// is intentionally empty for reconciliation of ordinary unfiled rows.
    pub(crate) fn replace_reconciled_writes(
        &mut self,
        mut replacement: PreparedWriteSet,
        file_ids: &BTreeSet<String>,
    ) {
        let replacement_keys = replacement
            .state_rows
            .iter()
            .map(PreparedStateRowIdentity::from)
            .collect::<BTreeSet<_>>();
        let retained = self
            .state_rows
            .iter()
            .enumerate()
            .filter_map(|(index, row)| {
                (!replacement_keys.contains(&PreparedStateRowIdentity::from(row))).then_some(index)
            })
            .collect::<Vec<_>>();
        self.insert_selection.select_rows(&retained);
        self.state_rows.select_rows(&retained);
        let replacement_count = replacement.state_rows.len();
        self.state_rows.append(replacement.state_rows);
        self.insert_selection
            .resize_rows(self.state_rows.len().saturating_sub(replacement_count));
        for _ in 0..replacement_count {
            self.insert_selection.push_not_insert();
        }
        self.file_content_writes
            .retain(|write| !file_ids.contains(&write.file_id));
        self.file_content_writes
            .append(&mut replacement.file_content_writes);
        for change_refs in self.commit_change_refs_by_branch.values_mut() {
            change_refs.tracked_change_count = self
                .state_rows
                .iter()
                .filter(|row| !row.untracked && row.branch_id.as_str() != GLOBAL_BRANCH_ID)
                .count();
        }
    }

    #[cfg(test)]
    pub(crate) fn validation_rows(&self) -> impl Iterator<Item = PreparedValidationRow<'_>> + '_ {
        self.state_rows.iter().map(PreparedValidationRow::State)
    }

    pub(crate) fn validation_index(&self) -> PreparedWriteValidationIndex<'_> {
        let mut rows_by_schema_scope = BTreeMap::<Domain, Vec<PreparedValidationRow<'_>>>::new();
        for row in &self.state_rows {
            let row = PreparedValidationRow::State(row);
            rows_by_schema_scope
                .entry(row.domain().schema_catalog_domain())
                .or_default()
                .push(row);
        }
        let mut insert_ordinals_by_schema_scope = BTreeMap::<Domain, Vec<u32>>::new();
        for insert in self.insert_selection.iter(&self.state_rows) {
            insert_ordinals_by_schema_scope
                .entry(
                    Domain::exact_file(
                        insert.row.branch_id.to_string(),
                        insert.row.untracked,
                        insert.row.file_id.map(ToString::to_string),
                    )
                    .schema_catalog_domain(),
                )
                .or_default()
                .push(
                    u32::try_from(insert.row_index)
                        .expect("prepared insert row ordinal must fit u32"),
                );
        }

        PreparedWriteValidationIndex {
            state_rows: &self.state_rows,
            insert_selection: &self.insert_selection,
            rows_by_schema_scope,
            insert_ordinals_by_schema_scope,
        }
    }

    #[cfg(test)]
    pub(crate) fn validation_set_for_tests(&self) -> PreparedWriteValidationSet<'_> {
        let rows: Vec<_> = self.validation_rows().collect();
        let insert_ordinals = self
            .insert_selection
            .iter(&self.state_rows)
            .map(|insert| {
                u32::try_from(insert.row_index).expect("prepared insert row ordinal must fit u32")
            })
            .collect();
        PreparedWriteValidationSet {
            state_rows: &self.state_rows,
            insert_selection: &self.insert_selection,
            constraint_rows: rows.clone(),
            rows,
            insert_ordinals,
        }
    }

    #[cfg(test)]
    pub(crate) fn remember_insert_identity_for_tests(&mut self, row: &TestPreparedStateRow) {
        if self.insert_selection.row_count != self.state_rows.len() {
            assert!(
                self.insert_selection.is_empty(),
                "test INSERT selection cannot be realigned after rows were marked"
            );
            self.insert_selection =
                PreparedInsertSelection::with_row_capacity(self.state_rows.len());
            self.insert_selection.resize_rows(self.state_rows.len());
        }
        let identity = PreparedStateRowIdentity::from(row);
        let row_index = self
            .state_rows
            .iter()
            .position(|candidate| PreparedStateRowIdentity::from(candidate) == identity)
            .expect("test INSERT row must already exist in the prepared batch");
        self.insert_selection
            .mark(row_index, row.origin.as_ref(), None);
    }
}

impl TransactionWriteBuffer {
    pub(crate) fn new(functions: FunctionProviderHandle) -> Self {
        Self {
            functions,
            rows: Mutex::new(StagedPreparedRows::default()),
            ordered_mutations: Mutex::new(None),
            commit_change_refs_by_branch: Mutex::new(BTreeMap::new()),
            first_commit_parent_override_by_branch: Mutex::new(BTreeMap::new()),
            checkpoint_publications: Mutex::new(Vec::new()),
            extra_commit_parents_by_branch: Mutex::new(BTreeMap::new()),
            intermediate_commits: Mutex::new(Vec::new()),
            file_content_writes: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn certify_complete_collection_replacement(
        &self,
        expected_schema_key: &str,
        expected_branch_id: &str,
        expected_live_count: u64,
        expected_ordered_identity_digest: [u8; 32],
    ) -> Result<bool, LixError> {
        let mut ordered = self.ordered_mutations.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire immutable transaction mutation journal",
            )
        })?;
        if let Some(journal) = ordered.as_mut() {
            if journal.row_count != usize::try_from(expected_live_count).unwrap_or(usize::MAX)
                || journal.schema_key() != expected_schema_key
                || journal.branch_id() != expected_branch_id
                || journal
                    .chunks
                    .iter()
                    .any(|chunk| chunk.origin_key().is_some())
            {
                return Ok(false);
            }
            let mut identity_hasher = blake3::Hasher::new();
            for identity in journal
                .chunks
                .iter()
                .flat_map(|chunk| (0..chunk.len()).map(|index| chunk.identity(index)))
            {
                identity_hasher.update(&(identity.len() as u64).to_le_bytes());
                identity_hasher.update(identity.as_bytes());
            }
            if *identity_hasher.finalize().as_bytes() != expected_ordered_identity_digest {
                return Ok(false);
            }
            let replay_bytes = journal.chunks.iter().try_fold(0_u64, |bytes, chunk| {
                (0..chunk.len()).try_fold(bytes, |bytes, index| {
                    let row_bytes = chunk
                        .schema_key()
                        .len()
                        .checked_add(chunk.identity(index).len())
                        .and_then(|value| value.checked_add(128))
                        .and_then(|value| value.checked_add(chunk.snapshot(index).len()))
                        .and_then(|value| u64::try_from(value).ok())
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "immutable mutation replay bytes exceed u64",
                            )
                        })?;
                    bytes.checked_add(row_bytes).ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "immutable mutation replay bytes exceed u64",
                        )
                    })
                })
            })?;
            journal.replacement_proof = Some(CompleteCollectionReplacementProof {
                ordered_identity_digest: expected_ordered_identity_digest,
                replay_bytes,
            });
            return Ok(true);
        }
        drop(ordered);
        let mut rows = self.rows.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction journal rows for replacement certification",
            )
        })?;
        let batch = match &mut *rows {
            StagedPreparedRows::AppendOnly { rows, .. }
            | StagedPreparedRows::Indexed { rows, .. } => rows,
        };
        Ok(batch.certify_complete_collection_replacement(
            expected_schema_key,
            expected_branch_id,
            expected_live_count,
            expected_ordered_identity_digest,
        ))
    }

    /// Appends one immutable, strictly ordered typed mutation chunk without
    /// reconstructing generic prepared rows. The transaction must later
    /// certify the accumulated journal as a complete replacement; otherwise
    /// durable predecessor evidence is required for generic lowering.
    pub(crate) fn stage_immutable_mutation_chunk(
        &self,
        chunk: ImmutableMutationJournalChunk,
    ) -> Result<ImmutableMutationChunkStage, LixError> {
        let count = chunk.len();
        let rows = self.rows.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction prepared rows",
            )
        })?;
        let rows_are_empty = match &*rows {
            StagedPreparedRows::AppendOnly { rows, .. }
            | StagedPreparedRows::Indexed { rows, .. } => rows.is_empty(),
        };
        drop(rows);
        if !rows_are_empty {
            return Ok(ImmutableMutationChunkStage::RequiresGeneric(chunk));
        }

        let mut ordered = self.ordered_mutations.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire immutable transaction mutation journal",
            )
        })?;
        if let Some(existing) = ordered.as_ref() {
            let compatible = existing.schema_key() == chunk.schema_key()
                && existing.branch_id() == chunk.branch_id()
                && existing.timestamp() == chunk.timestamp()
                && existing.chunks[0].origin_key() == chunk.origin_key()
                && existing
                    .chunks
                    .last()
                    .and_then(|previous| {
                        previous
                            .len()
                            .checked_sub(1)
                            .map(|index| previous.identity(index))
                    })
                    .zip((chunk.len() > 0).then(|| chunk.identity(0)))
                    .is_some_and(|(previous, next)| previous < next);
            if !compatible {
                return Ok(ImmutableMutationChunkStage::RequiresGeneric(chunk));
            }
        }

        let mut commit_change_refs = self.commit_change_refs_by_branch.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged commit change refs",
            )
        })?;
        let branch_id = chunk.branch_id().to_owned();
        let refs = commit_change_refs.entry(branch_id).or_insert_with(|| {
            let timestamp = self.functions.call_timestamp();
            StagedCommitChangeRefs::new(
                CommitId::with_change_address_space(self.functions.call_uuid_v7()),
                ChangeId::from(self.functions.call_uuid_v7()),
                timestamp,
            )
        });
        refs.add_change_count(count);
        match ordered.as_mut() {
            Some(existing) => {
                existing.row_count = existing.row_count.checked_add(count).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "immutable mutation journal row count overflowed",
                    )
                })?;
                Arc::make_mut(&mut existing.chunks).push(chunk);
                existing.replacement_proof = None;
            }
            None => {
                *ordered = Some(OrderedMutationJournal {
                    chunks: Arc::new(vec![chunk]),
                    row_count: count,
                    commit_id: refs.commit_id,
                    replacement_proof: None,
                    overlay_uniform_created_at: None,
                });
            }
        }
        Ok(ImmutableMutationChunkStage::Staged)
    }

    /// Attaches exact current-state predecessor evidence to provisional
    /// immutable chunks before a partial/mixed journal is lowered. Callers
    /// obtain this column with one bulk exact read; boolean membership alone
    /// is deliberately insufficient because it does not preserve created_at.
    pub(crate) fn hydrate_immutable_mutation_predecessors(
        &self,
        predecessors: Vec<CertifiedCurrentStatePredecessor>,
    ) -> Result<(), LixError> {
        let mut ordered = self.ordered_mutations.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire immutable transaction mutation journal",
            )
        })?;
        let journal = ordered.as_mut().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "transaction has no provisional immutable mutation journal",
            )
        })?;
        if predecessors.len() != journal.row_count {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "bulk predecessor evidence does not align with immutable mutation rows",
            ));
        }
        let mut predecessors = predecessors.into_iter();
        for chunk in Arc::make_mut(&mut journal.chunks) {
            let values = predecessors.by_ref().take(chunk.len()).collect::<Vec<_>>();
            debug_assert_eq!(values.len(), chunk.len());
            chunk.durable_predecessors = Some(values.into());
        }
        debug_assert!(predecessors.next().is_none());
        Ok(())
    }

    pub(crate) fn set_ordered_mutation_overlay_created_at(
        &self,
        created_at: LixTimestamp,
    ) -> Result<(), LixError> {
        let mut ordered = self.ordered_mutations.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire immutable transaction mutation journal",
            )
        })?;
        let journal = ordered.as_mut().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "transaction has no provisional immutable mutation journal",
            )
        })?;
        journal.overlay_uniform_created_at = Some(created_at);
        Ok(())
    }

    pub(crate) fn provisional_mutation_journal_descriptor(
        &self,
    ) -> Result<Option<ProvisionalMutationJournalDescriptor>, LixError> {
        let ordered = self.ordered_mutations.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire immutable transaction mutation journal",
            )
        })?;
        Ok(ordered
            .as_ref()
            .map(|journal| ProvisionalMutationJournalDescriptor {
                schema_key: journal.chunks[0].schema_key.clone(),
                branch_id: journal.chunks[0].branch_id.clone(),
                row_pk_chunks: journal
                    .chunks
                    .iter()
                    .map(ImmutableMutationJournalChunk::materialized_row_pks)
                    .collect(),
                predecessors_complete: journal
                    .chunks
                    .iter()
                    .all(|chunk| chunk.durable_predecessors.is_some()),
            }))
    }

    /// Materializes a provisional journal only for semantics that require the
    /// generic transaction overlay. Partial journals must first attach durable
    /// predecessor evidence; certified complete replacements may lower
    /// without it because their lifecycle is recovered from manifest history.
    pub(crate) fn lower_immutable_mutations_to_prepared(&self) -> Result<(), LixError> {
        let mut ordered = self.ordered_mutations.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire immutable transaction mutation journal",
            )
        })?;
        if ordered.as_ref().is_some_and(|journal| {
            journal.replacement_proof.is_none()
                && journal
                    .chunks
                    .iter()
                    .any(|chunk| chunk.durable_predecessors.is_none())
        }) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "partial immutable mutation journal must bulk-hydrate durable predecessors before lowering",
            ));
        }
        let journal = ordered.take();
        drop(ordered);
        let Some(journal) = journal else {
            return Ok(());
        };
        let rows = journal.into_prepared()?;
        let last_key = rows.last().map(TrackedStateKey::from_row);
        let mut insert_selection = PreparedInsertSelection::new();
        insert_selection.resize_rows(rows.len());
        let mut staged = self.rows.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction prepared rows",
            )
        })?;
        let existing_is_empty = match &*staged {
            StagedPreparedRows::AppendOnly { rows, .. }
            | StagedPreparedRows::Indexed { rows, .. } => rows.is_empty(),
        };
        if !existing_is_empty {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "cannot lower immutable mutations over existing prepared rows",
            ));
        }
        *staged = StagedPreparedRows::AppendOnly {
            rows,
            insert_selection,
            last_key,
        };
        Ok(())
    }

    pub(crate) fn is_file_cohort_eligible(&self, branch_id: &str) -> bool {
        let rows = self.rows.lock().unwrap_or_else(|error| error.into_inner());
        let rows = match &*rows {
            StagedPreparedRows::AppendOnly { rows, .. }
            | StagedPreparedRows::Indexed { rows, .. } => rows,
        };
        let has_rows = !rows.is_empty();
        let invalid_row = rows
            .iter()
            .any(|row| row.untracked || row.global || row.branch_id.as_str() != branch_id);
        if !has_rows || invalid_row {
            return false;
        }
        let eligible = self
            .first_commit_parent_override_by_branch
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty()
            && self
                .checkpoint_publications
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty();
        eligible
            && self
                .extra_commit_parents_by_branch
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty()
            && self
                .intermediate_commits
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty()
    }

    /// Captures every mutable staging structure before a statement that may
    /// need post-stage SQL projection. Restoring this checkpoint preserves
    /// earlier explicit-transaction statements even when the current one
    /// fails after it has staged rows.
    pub(crate) fn checkpoint(&self) -> Result<TransactionWriteBufferCheckpoint, LixError> {
        let rows = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let ordered_mutations = self.ordered_mutations.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire immutable transaction mutation journal",
            )
        })?;
        let file_content_writes = self.file_content_writes.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged file data lock",
            )
        })?;
        let commit_change_refs_by_branch =
            self.commit_change_refs_by_branch.lock().map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "failed to acquire transaction staged commit change refs lock",
                )
            })?;
        let extra_commit_parents_by_branch =
            self.extra_commit_parents_by_branch.lock().map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "failed to acquire transaction staged extra commit parents lock",
                )
            })?;
        let intermediate_commits = self.intermediate_commits.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged intermediate commits lock",
            )
        })?;
        let first_commit_parent_override_by_branch = self
            .first_commit_parent_override_by_branch
            .lock()
            .map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "failed to acquire transaction staged first commit parent overrides lock",
                )
            })?;
        let checkpoint_publications = self.checkpoint_publications.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged checkpoint publications lock",
            )
        })?;

        Ok(TransactionWriteBufferCheckpoint {
            rows: rows.clone(),
            ordered_mutations: ordered_mutations.clone(),
            commit_change_refs_by_branch: commit_change_refs_by_branch.clone(),
            first_commit_parent_override_by_branch: first_commit_parent_override_by_branch.clone(),
            checkpoint_publications: checkpoint_publications.clone(),
            extra_commit_parents_by_branch: extra_commit_parents_by_branch.clone(),
            intermediate_commits: intermediate_commits.clone(),
            file_content_writes: file_content_writes.clone(),
        })
    }

    /// Restores a complete journal checkpoint. The lock order matches
    /// [`Self::drain`] so rollback cannot observe a partially restored write
    /// set through another staging operation.
    pub(crate) fn restore(
        &self,
        checkpoint: TransactionWriteBufferCheckpoint,
    ) -> Result<(), LixError> {
        let TransactionWriteBufferCheckpoint {
            rows,
            ordered_mutations,
            commit_change_refs_by_branch,
            first_commit_parent_override_by_branch,
            checkpoint_publications,
            extra_commit_parents_by_branch,
            intermediate_commits,
            file_content_writes,
        } = checkpoint;
        let mut rows_guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let mut ordered_mutations_guard = self.ordered_mutations.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire immutable transaction mutation journal",
            )
        })?;
        let mut file_content_guard = self.file_content_writes.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged file data lock",
            )
        })?;
        let mut commit_change_refs_guard =
            self.commit_change_refs_by_branch.lock().map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "failed to acquire transaction staged commit change refs lock",
                )
            })?;
        let mut extra_parents_guard = self.extra_commit_parents_by_branch.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged extra commit parents lock",
            )
        })?;
        let mut intermediate_commits_guard = self.intermediate_commits.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged intermediate commits lock",
            )
        })?;
        let mut first_parent_overrides_guard = self
            .first_commit_parent_override_by_branch
            .lock()
            .map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged first commit parent overrides lock",
            )
        })?;
        let mut checkpoint_publications_guard =
            self.checkpoint_publications.lock().map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "failed to acquire transaction staged checkpoint publications lock",
                )
            })?;

        *rows_guard = rows;
        *ordered_mutations_guard = ordered_mutations;
        *file_content_guard = file_content_writes;
        *commit_change_refs_guard = commit_change_refs_by_branch;
        *extra_parents_guard = extra_commit_parents_by_branch;
        *intermediate_commits_guard = intermediate_commits;
        *first_parent_overrides_guard = first_commit_parent_override_by_branch;
        *checkpoint_publications_guard = checkpoint_publications;
        Ok(())
    }

    /// Returns whether this transaction has changed the schema catalog used
    /// by `domain` without promoting the append-only staging journal.
    ///
    /// Typed SQL certificates are bound to the session's pinned catalog.
    /// A transaction-local registered-schema row therefore invalidates those
    /// certificates for the same branch/durability scope.
    pub(crate) fn has_staged_schema_catalog_change(
        &self,
        domain: &Domain,
    ) -> Result<bool, LixError> {
        let rows = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let registered_schema_key = crate::transaction::normalization::REGISTERED_SCHEMA_KEY;
        let (rows, candidate_slots, ordered_range) = match &*rows {
            StagedPreparedRows::AppendOnly { rows, .. } => (
                rows,
                None,
                Some(ordered_schema_row_range(rows, registered_schema_key)),
            ),
            StagedPreparedRows::Indexed {
                rows, by_candidate, ..
            } => (
                rows,
                Some(
                    by_candidate
                        .slots_by_schema
                        .get(registered_schema_key)
                        .map(SmallVec::as_slice)
                        .unwrap_or(&[]),
                ),
                None,
            ),
        };
        let matches_domain = |row: PreparedStateRowRef<'_>| {
            row.schema_key.as_str() == registered_schema_key
                && row.branch_id.as_str() == domain.branch_id()
                && (row.untracked == domain.untracked() || (domain.untracked() && !row.untracked))
        };
        Ok(match candidate_slots {
            Some(slots) => slots.iter().any(|slot| match slot {
                RowSlot::State(index) => matches_domain(rows.row(*index)),
            }),
            None => ordered_range
                .expect("append-only rows carry their ordered schema range")
                .any(|index| matches_domain(rows.row(index))),
        })
    }

    /// Promotes the compact ordered journal into the existing identity overlay
    /// only when a read or an irregular write needs read-your-writes lookup.
    fn ensure_identity_index(&self, materialize_empty: bool) -> Result<(), LixError> {
        let mut guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let StagedPreparedRows::AppendOnly {
            rows,
            insert_selection,
            ..
        } = &mut *guard
        else {
            return Ok(());
        };
        // Schema normalization consults an empty transaction overlay before
        // its first normal write. Keeping that read in journal mode lets the
        // upcoming sorted batch take the append-only lane.
        if rows.is_empty() && !materialize_empty {
            return Ok(());
        }
        let rows = std::mem::take(rows);
        let insert_selection = std::mem::take(insert_selection);
        let mut by_identity = HashMap::with_capacity(rows.len());
        let mut by_candidate = StagedScanCandidateIndex::default();
        for (index, row) in rows.iter().enumerate() {
            let identity = PreparedStateRowIdentity::from(row);
            let slot = RowSlot::State(index);
            by_candidate.insert(row, slot);
            let previous = by_identity.insert(identity, slot);
            debug_assert!(previous.is_none(), "append-only rows must be unique");
        }
        *guard = StagedPreparedRows::Indexed {
            rows,
            insert_selection,
            by_identity,
            by_candidate,
        };
        Ok(())
    }

    /// Proves that an exact read lies strictly after an ordered tracked
    /// journal without materializing its identity indexes.
    ///
    /// Append-only rows are normal tracked rows in tracked-tree order. If
    /// every requested key is greater than the journal tail, none can overlap
    /// a staged row. Sequential point updates use exactly this shape: the next
    /// key probes committed state, then extends the same compact journal.
    fn append_only_exact_batch_is_definitely_absent(
        &self,
        request: &HotStateExactBatchRequest,
    ) -> Result<bool, LixError> {
        let guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let StagedPreparedRows::AppendOnly { rows, last_key, .. } = &*guard else {
            return Ok(false);
        };
        if rows.is_empty() || request.untracked == Some(true) {
            return Ok(true);
        }
        let Some(last_key) = last_key else {
            return Ok(false);
        };
        Ok(request.rows.iter().all(|row| {
            compare_tracked_key_to_exact_request(last_key, row) == std::cmp::Ordering::Less
        }))
    }

    /// Proves a keyed scan lies after the ordered journal tail.
    ///
    /// Scan identity vectors form a Cartesian product. Comparing the minimum
    /// possible tracked key is therefore sufficient: if it is after the tail,
    /// every requested identity is after the tail. An empty or `Any` file
    /// filter includes `NULL`, which is the minimum file component.
    fn append_only_scan_is_definitely_absent(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<bool, LixError> {
        let guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let StagedPreparedRows::AppendOnly { rows, last_key, .. } = &*guard else {
            return Ok(false);
        };
        if rows.is_empty() || request.filter.untracked == Some(true) {
            return Ok(true);
        }
        let Some(last_key) = last_key else {
            return Ok(false);
        };
        let Some(schema_key) = request.filter.schema_keys.iter().min() else {
            return Ok(false);
        };
        let Some(row_pk) = request.filter.row_pks.iter().min() else {
            return Ok(false);
        };
        let file_id =
            if request.filter.file_ids.is_empty()
                || request.filter.file_ids.iter().any(|file_id| {
                    matches!(file_id, NullableKeyFilter::Any | NullableKeyFilter::Null)
                })
            {
                None
            } else {
                request
                    .filter
                    .file_ids
                    .iter()
                    .filter_map(|file_id| match file_id {
                        NullableKeyFilter::Value(value) => Some(value.as_str()),
                        NullableKeyFilter::Any | NullableKeyFilter::Null => None,
                    })
                    .min()
            };
        Ok(
            compare_tracked_key_to_parts(last_key, schema_key, file_id, row_pk)
                == std::cmp::Ordering::Less,
        )
    }

    /// Takes the normal tracked write lane directly into the transaction
    /// journal. Any duplicate, cross-scope, untracked, global, or otherwise
    /// irregular batch falls back to the indexed overlay.
    fn stage_append_only_if_possible(
        &self,
        mode: Option<TransactionWriteMode>,
        mut rows: PreparedStateBatch,
        statement_indices: Option<&[u32]>,
    ) -> Result<AppendOnlyStage, LixError> {
        let inserts = mode == Some(TransactionWriteMode::Insert);
        let certified_tracked_keys_strictly_ordered =
            inserts && rows.certified_tracked_keys_strictly_ordered();
        if !matches!(
            mode,
            Some(TransactionWriteMode::Replace | TransactionWriteMode::Insert)
        ) || (!certified_tracked_keys_strictly_ordered
            && inserts
            && !rows
                .iter()
                .all(|row| row_is_insert(mode, row) && row.snapshot.is_some()))
        {
            return Ok(AppendOnlyStage::Fallback(rows));
        }
        let mut staged_rows = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let StagedPreparedRows::AppendOnly {
            rows: existing_rows,
            insert_selection,
            last_key,
        } = &mut *staged_rows
        else {
            return Ok(AppendOnlyStage::Fallback(rows));
        };
        let append_shape_matches = if certified_tracked_keys_strictly_ordered {
            rows.first().is_none_or(|first| {
                is_normal_tracked_append_row(first)
                    && first.snapshot.is_some()
                    && last_key.as_ref().is_none_or(|previous| {
                        compare_tracked_key_to_row(previous, first) == std::cmp::Ordering::Less
                    })
            })
        } else {
            rows_are_append_only_tracked(&rows, last_key.as_ref())
        };
        if !append_shape_matches {
            return Ok(AppendOnlyStage::Fallback(rows));
        }

        let mut commit_change_refs = self.commit_change_refs_by_branch.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged commit change refs lock",
            )
        })?;
        if let Some(first_row) = rows.first() {
            if !commit_change_refs.contains_key(first_row.branch_id.as_str()) {
                commit_change_refs.insert(first_row.branch_id.to_string(), {
                    let timestamp = self.functions.call_timestamp();
                    StagedCommitChangeRefs::new(
                        CommitId::with_change_address_space(self.functions.call_uuid_v7()),
                        ChangeId::from(self.functions.call_uuid_v7()),
                        timestamp,
                    )
                });
            }
            let change_refs = commit_change_refs
                .get_mut(first_row.branch_id.as_str())
                .expect("branch change refs were inserted above");
            let commit_id = change_refs.commit_id;
            change_refs.add_change_count(rows.len());
            rows.set_commit_id_all(commit_id);
        }
        if certified_tracked_keys_strictly_ordered {
            insert_selection.push_certified_ordinal_inserts(rows.len());
        } else {
            insert_selection.reserve_rows(rows.len(), inserts);
            for (row_index, row) in rows.iter().enumerate() {
                if inserts {
                    insert_selection.push(
                        row.origin,
                        statement_indices.map(|indices| indices[row_index] as usize),
                    );
                } else {
                    insert_selection.push_not_insert();
                }
            }
        }
        if let Some(row) = rows.last() {
            *last_key = Some(TrackedStateKey::from_row(row));
        }
        if existing_rows.is_empty() {
            *existing_rows = rows;
        } else {
            existing_rows.append(rows);
        }
        Ok(AppendOnlyStage::Staged)
    }

    /// Reorders one fresh tracked file batch into canonical tree order and
    /// retains it as the compact journal.
    ///
    /// Plugin-backed imports arrive as one unique batch plus file payloads,
    /// but cursor/schema grouping is not tracked-tree order. Building the
    /// transaction read indexes eagerly for that one-shot commit duplicates
    /// every identity and then sorts the same rows again during root
    /// materialization. One dense permutation validates uniqueness without
    /// row-owned keys, reorders the existing row allocation in place, and
    /// leaves index construction lazy for the uncommon read/overlap case.
    fn stage_fresh_tracked_file_batch_if_possible(
        &self,
        mode: Option<TransactionWriteMode>,
        mut rows: PreparedStateBatch,
    ) -> Result<AppendOnlyStage, LixError> {
        if !matches!(
            mode,
            Some(TransactionWriteMode::Replace | TransactionWriteMode::Insert)
        ) {
            return Ok(AppendOnlyStage::Fallback(rows));
        }
        let Some(first) = rows.first() else {
            return Ok(AppendOnlyStage::Staged);
        };
        if !is_normal_tracked_append_row(first)
            || rows.iter().any(|row| {
                !is_normal_tracked_append_row(row)
                    || row.branch_id != first.branch_id
                    || row.facts.requires_transaction_validation
                    || (row_is_insert(mode, row) && row.snapshot.is_none())
            })
        {
            return Ok(AppendOnlyStage::Fallback(rows));
        }

        // `order[new_position] = old_position`. The original position is the
        // tiebreaker so declining the fast path never changes source-order
        // error precedence in the generic staging path.
        let mut order = (0..rows.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|&left, &right| {
            compare_rows_by_tracked_key(rows.row(left), rows.row(right))
                .then_with(|| left.cmp(&right))
        });
        if order.windows(2).any(|pair| {
            compare_rows_by_tracked_key(rows.row(pair[0]), rows.row(pair[1]))
                == std::cmp::Ordering::Equal
        }) {
            return Ok(AppendOnlyStage::Fallback(rows));
        }

        let mut staged_rows = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let StagedPreparedRows::AppendOnly {
            rows: existing_rows,
            insert_selection,
            last_key,
        } = &mut *staged_rows
        else {
            return Ok(AppendOnlyStage::Fallback(rows));
        };
        if !existing_rows.is_empty() {
            return Ok(AppendOnlyStage::Fallback(rows));
        }

        let mut commit_change_refs = self.commit_change_refs_by_branch.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged commit change refs lock",
            )
        })?;
        reorder_rows_by_source_permutation(&mut rows, &mut order);
        debug_assert!((1..rows.len()).all(|index| {
            compare_rows_by_tracked_key(rows.row(index - 1), rows.row(index))
                == std::cmp::Ordering::Less
        }));

        let branch_id = rows.row(0).branch_id.clone();
        if !commit_change_refs.contains_key(branch_id.as_str()) {
            commit_change_refs.insert(branch_id.to_string(), {
                let timestamp = self.functions.call_timestamp();
                StagedCommitChangeRefs::new(
                    CommitId::with_change_address_space(self.functions.call_uuid_v7()),
                    ChangeId::from(self.functions.call_uuid_v7()),
                    timestamp,
                )
            });
        }
        let change_refs = commit_change_refs
            .get_mut(branch_id.as_str())
            .expect("branch change refs were inserted above");
        let commit_id = change_refs.commit_id;
        change_refs.add_change_count(rows.len());
        let has_inserts = rows.iter().any(|row| row_is_insert(mode, row));
        insert_selection.reserve_rows(rows.len(), has_inserts);
        for index in 0..rows.len() {
            let row = rows.row(index);
            if row_is_insert(mode, row) {
                insert_selection.push(row.origin, None);
            } else {
                insert_selection.push_not_insert();
            }
            rows.set_commit_id(index, Some(commit_id));
        }

        *last_key = rows.last().map(TrackedStateKey::from_row);
        *existing_rows = rows;
        Ok(AppendOnlyStage::Staged)
    }

    /// Drains staged writes for commit.
    pub(crate) fn drain(&self) -> Result<PreparedWriteSet, LixError> {
        let mut rows_guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let mut ordered_mutations_guard = self.ordered_mutations.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire immutable transaction mutation journal",
            )
        })?;
        let mut file_content_guard = self.file_content_writes.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged file data lock",
            )
        })?;
        let mut commit_change_refs_guard =
            self.commit_change_refs_by_branch.lock().map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "failed to acquire transaction staged commit change refs lock",
                )
            })?;
        let mut extra_parents_guard = self.extra_commit_parents_by_branch.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged extra commit parents lock",
            )
        })?;
        let mut intermediate_commits_guard = self.intermediate_commits.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged intermediate commits lock",
            )
        })?;
        let mut first_parent_overrides_guard = self
            .first_commit_parent_override_by_branch
            .lock()
            .map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged first commit parent overrides lock",
            )
        })?;
        let mut checkpoint_publications_guard =
            self.checkpoint_publications.lock().map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "failed to acquire transaction staged checkpoint publications lock",
                )
            })?;
        let (state_rows, insert_selection) = match std::mem::take(&mut *rows_guard) {
            StagedPreparedRows::AppendOnly {
                rows,
                insert_selection,
                ..
            }
            | StagedPreparedRows::Indexed {
                rows,
                insert_selection,
                ..
            } => (rows, insert_selection),
        };
        let ordered_replacement = std::mem::take(&mut *ordered_mutations_guard);
        if ordered_replacement
            .as_ref()
            .is_some_and(|journal| journal.replacement_proof.is_none())
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable mutation journal reached commit without complete-replacement certification",
            ));
        }
        if ordered_replacement.is_some() && !state_rows.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable replacement journal overlaps generic prepared rows",
            ));
        }
        if let Some(journal) = ordered_replacement {
            let refs = commit_change_refs_guard
                .get_mut(journal.branch_id())
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "immutable mutation journal has no commit membership",
                    )
                })?;
            if refs.commit_id != journal.commit_id() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation journal commit owner changed",
                ));
            }
            refs.attach_ordered_mutation_journal(Arc::new(journal))?;
        }
        Ok(PreparedWriteSet {
            state_rows,
            insert_selection,
            commit_change_refs_by_branch: std::mem::take(&mut *commit_change_refs_guard),
            first_commit_parent_override_by_branch: std::mem::take(
                &mut *first_parent_overrides_guard,
            ),
            checkpoint_publications: std::mem::take(&mut *checkpoint_publications_guard),
            extra_commit_parents_by_branch: std::mem::take(&mut *extra_parents_guard),
            intermediate_commits: std::mem::take(&mut *intermediate_commits_guard),
            file_content_writes: std::mem::take(&mut *file_content_guard),
        })
    }

    pub(crate) fn add_checkpoint_publication(
        &self,
        publication: CheckpointPublication,
    ) -> Result<(), LixError> {
        self.checkpoint_publications
            .lock()
            .map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "failed to acquire transaction staged checkpoint publications lock",
                )
            })?
            .push(publication);
        Ok(())
    }

    pub(crate) fn commit_id_for_branch(
        &self,
        branch_id: &str,
    ) -> Result<Option<CommitId>, LixError> {
        Ok(self
            .commit_change_refs_by_branch
            .lock()
            .map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "failed to acquire transaction staged commit change refs lock",
                )
            })?
            .get(branch_id)
            .map(|change_refs| change_refs.commit_id))
    }

    /// Overrides the normal branch-head first parent for a staged commit.
    ///
    /// Checkpoint compaction uses the previous checkpoint as the new commit's
    /// first parent, making intervening auto-commits unreachable from the
    /// branch while preserving their net state in the checkpoint commit.
    pub(crate) fn set_first_commit_parent(
        &self,
        branch_id: String,
        parent_commit_id: CommitId,
    ) -> Result<(), LixError> {
        self.first_commit_parent_override_by_branch
            .lock()
            .map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "failed to acquire transaction staged first commit parent overrides lock",
                )
            })?
            .insert(branch_id, parent_commit_id);
        Ok(())
    }

    /// Records an additional parent for the commit generated for `branch_id`.
    ///
    /// Normal writes parent the new commit to the branch's previous head.
    /// Merges add the source branch head as an extra parent so the commit graph
    /// preserves branch ancestry while tracked-state roots still apply source
    /// rows onto the target root.
    pub(crate) fn add_commit_parent(
        &self,
        branch_id: String,
        parent_commit_id: CommitId,
    ) -> Result<(), LixError> {
        let mut guard = self.extra_commit_parents_by_branch.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged extra commit parents lock",
            )
        })?;
        let parents = guard.entry(branch_id).or_default();
        if !parents.contains(&parent_commit_id) {
            parents.push(parent_commit_id);
        }
        Ok(())
    }

    pub(crate) fn stage_selected_commit_change_refs(
        &self,
        branch_id: String,
        selected_changes: StagedCommitChangeBatch,
    ) -> Result<String, LixError> {
        let functions = self.functions.clone();
        let mut guard = self.commit_change_refs_by_branch.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged commit change refs lock",
            )
        })?;
        let change_refs = guard.entry(branch_id).or_insert_with(|| {
            let timestamp = functions.call_timestamp();
            StagedCommitChangeRefs::new(
                CommitId::with_change_address_space(functions.call_uuid_v7()),
                ChangeId::from(functions.call_uuid_v7()),
                timestamp,
            )
        });
        change_refs.allow_empty();
        change_refs.add_selected_change_batch(selected_changes);
        Ok(change_refs.commit_id.to_string())
    }

    pub(crate) fn stage_intermediate_commit(
        &self,
        branch_id: String,
        parent_commit_id: CommitId,
        selected_changes: StagedCommitChangeBatch,
    ) -> Result<CommitId, LixError> {
        let timestamp = self.functions.call_timestamp();
        let mut change_refs = StagedCommitChangeRefs::new(
            CommitId::with_change_address_space(self.functions.call_uuid_v7()),
            ChangeId::from(self.functions.call_uuid_v7()),
            timestamp,
        );
        change_refs.allow_empty();
        change_refs.add_selected_change_batch(selected_changes);
        let commit_id = change_refs.commit_id;
        self.intermediate_commits
            .lock()
            .map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "failed to acquire transaction staged intermediate commits lock",
                )
            })?
            .push(StagedIntermediateCommit {
                branch_id,
                parent_commit_id,
                change_refs,
            });
        Ok(commit_id)
    }

    /// Builds the transaction-local read overlay from currently staged writes.
    pub(crate) fn staging_overlay(self: &Arc<Self>) -> Result<PreparedStateRowOverlay, LixError> {
        Ok(PreparedStateRowOverlay {
            staged_writes: Arc::clone(self),
        })
    }

    /// Returns whether a staged row can override this exact tracked identity.
    /// The normal monotonically ordered journal answers future identities from
    /// its tail without building an index; irregular/overlapping transactions
    /// already have the exact identity map.
    pub(crate) fn staged_identity_may_affect(
        &self,
        branch_id: &str,
        schema_key: &str,
        file_id: Option<&str>,
        row_pk: &RowPk,
    ) -> Result<bool, LixError> {
        let ordered = self.ordered_mutations.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire immutable transaction mutation journal",
            )
        })?;
        if ordered.as_ref().is_some_and(|journal| {
            ordered_mutation_journal_row(journal, branch_id, schema_key, file_id, row_pk).is_some()
        }) {
            return Ok(true);
        }
        drop(ordered);
        let rows = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        Ok(match &*rows {
            StagedPreparedRows::AppendOnly { last_key, .. } => {
                last_key.as_ref().is_some_and(|last_key| {
                    compare_tracked_key_to_parts(last_key, schema_key, file_id, row_pk)
                        != std::cmp::Ordering::Less
                })
            }
            StagedPreparedRows::Indexed { by_identity, .. } => {
                by_identity.contains_key(&PreparedStateRowIdentity {
                    schema_key: schema_key.into(),
                    row_pk: row_pk.clone(),
                    file_id: file_id.map(Into::into),
                    branch_id: branch_id.into(),
                })
            }
        })
    }

    pub(crate) fn has_staged_state_rows(&self) -> Result<bool, LixError> {
        let rows = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        Ok(match &*rows {
            StagedPreparedRows::AppendOnly { rows, .. }
            | StagedPreparedRows::Indexed { rows, .. } => !rows.is_empty(),
        })
    }

    #[cfg(test)]
    fn uses_identity_index_for_tests(&self) -> bool {
        matches!(
            &*self
                .rows
                .lock()
                .expect("staged rows lock should not be poisoned"),
            StagedPreparedRows::Indexed { .. }
        )
    }

    /// Returns transaction-local file bytes addressed by their eventual CAS hash.
    ///
    /// File data is flushed into the binary CAS only during commit, while SQL reads
    /// can observe the staged `lix_binary_blob_ref` rows immediately. This lookup
    /// lets transaction-scoped blob readers satisfy those hashes from the same
    /// staged file payloads that commit will later write.
    pub(crate) fn load_staged_file_bytes_many(
        &self,
        hashes: &[BlobId],
    ) -> Result<BlobBytesBatch, LixError> {
        if hashes.is_empty() {
            return Ok(BlobBytesBatch::new(Vec::new()));
        }
        let file_content_guard = self.file_content_writes.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged file data lock",
            )
        })?;
        let mut requested = hashes
            .iter()
            .copied()
            .map(|hash| (hash, None))
            .collect::<BTreeMap<BlobId, Option<&[u8]>>>();
        let mut remaining = requested.len();
        'writes: for write in file_content_guard.iter() {
            let Some(data) = write.inline_data() else {
                // Prepared CAS content is already durable. Leaving its slot
                // unresolved lets the transaction reader fall through to CAS.
                continue;
            };
            let hash = write
                .blob_hash()
                .unwrap_or_else(|| BlobId::from_content(data));
            if let Some(bytes) = requested.get_mut(&hash)
                && bytes.is_none()
            {
                *bytes = Some(data);
                remaining -= 1;
                if remaining == 0 {
                    break 'writes;
                }
            }
            for payload in write.auxiliary_payloads() {
                let hash = payload
                    .hash()
                    .unwrap_or_else(|| BlobId::from_content(payload.bytes()));
                if let Some(bytes) = requested.get_mut(&hash)
                    && bytes.is_none()
                {
                    *bytes = Some(payload.bytes());
                    remaining -= 1;
                    if remaining == 0 {
                        break 'writes;
                    }
                }
            }
        }
        Ok(BlobBytesBatch::new(
            hashes
                .iter()
                .map(|hash| requested.get(hash).copied().flatten().map(<[u8]>::to_vec))
                .collect(),
        ))
    }

    /// Stages one prepared write batch into this transaction.
    ///
    /// Frontends hand a `RawWriteBatch` to `Transaction`; normalization
    /// prepares a stable `PreparedStateBatch` before this method indexes it for
    /// transaction-local reads and commit routing.
    pub(crate) fn stage_write(
        &self,
        write: PreparedTransactionWrite,
    ) -> Result<TransactionWriteOutcome, LixError> {
        self.stage_write_inner(write, None)
    }

    pub(crate) fn stage_parameter_batch_insert(
        &self,
        write: PreparedTransactionWrite,
        statement_indices: Vec<u32>,
    ) -> Result<TransactionWriteOutcome, LixError> {
        self.stage_write_inner(write, Some(statement_indices))
    }

    pub(crate) fn stage_certified_parameter_batch_insert(
        &self,
        write: PreparedTransactionWrite,
    ) -> Result<TransactionWriteOutcome, LixError> {
        let (mode, count) = match &write {
            PreparedTransactionWrite::Rows { mode, rows } => (Some(*mode), rows.len() as u64),
            PreparedTransactionWrite::RowsWithFileContent { .. } => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "certified parameter INSERT unexpectedly contains file data",
                ));
            }
        };
        let (rows, file_content_writes) = self.state_rows_from_stage_write(write);
        debug_assert!(file_content_writes.is_empty());
        match self.stage_append_only_if_possible(mode, rows, None)? {
            AppendOnlyStage::Staged => Ok(TransactionWriteOutcome { count }),
            AppendOnlyStage::Fallback(rows) => {
                let statement_indices = (0..rows.len())
                    .map(|index| {
                        u32::try_from(index).map_err(|_| {
                            LixError::new(
                                LixError::CODE_INVALID_PARAM,
                                "parameter batch row count exceeds u32",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.stage_write_inner(
                    PreparedTransactionWrite::Rows {
                        mode: TransactionWriteMode::Insert,
                        rows,
                    },
                    Some(statement_indices),
                )
            }
        }
    }

    fn stage_write_inner(
        &self,
        write: PreparedTransactionWrite,
        statement_indices: Option<Vec<u32>>,
    ) -> Result<TransactionWriteOutcome, LixError> {
        let (mode, count) = match &write {
            PreparedTransactionWrite::Rows { mode, rows } => (Some(*mode), rows.len() as u64),
            PreparedTransactionWrite::RowsWithFileContent { mode, count, .. } => {
                (Some(*mode), *count)
            }
        };
        let (mut rows, file_content_writes) = self.state_rows_from_stage_write(write);
        if let Some(statement_indices) = &statement_indices {
            debug_assert_eq!(mode, Some(TransactionWriteMode::Insert));
            debug_assert_eq!(statement_indices.len(), rows.len());
        }
        if rows.is_empty() {
            if !file_content_writes.is_empty() {
                self.file_content_writes
                    .lock()
                    .map_err(|_| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            "failed to acquire transaction staged file data lock",
                        )
                    })?
                    .extend(file_content_writes);
            }
            return Ok(TransactionWriteOutcome { count });
        }
        if file_content_writes.is_empty() {
            match self.stage_append_only_if_possible(mode, rows, statement_indices.as_deref())? {
                AppendOnlyStage::Staged => return Ok(TransactionWriteOutcome { count }),
                AppendOnlyStage::Fallback(fallback_rows) => rows = fallback_rows,
            }
        } else {
            match self.stage_fresh_tracked_file_batch_if_possible(mode, rows)? {
                AppendOnlyStage::Staged => {
                    self.file_content_writes
                        .lock()
                        .map_err(|_| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                "failed to acquire transaction staged file data lock",
                            )
                        })?
                        .extend(file_content_writes);
                    return Ok(TransactionWriteOutcome { count });
                }
                AppendOnlyStage::Fallback(fallback_rows) => rows = fallback_rows,
            }
        }
        let identities = rows
            .iter()
            .map(PreparedStateRowIdentity::from)
            .collect::<Vec<_>>();
        let identities_are_unique = validate_batch_row_identities(&rows, &identities)?;
        self.ensure_identity_index(true)?;
        let mut guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let StagedPreparedRows::Indexed {
            rows: staged_rows,
            insert_selection,
            by_identity,
            by_candidate,
        } = &mut *guard
        else {
            unreachable!("generic staging must promote the identity index");
        };
        let mut commit_change_refs_guard =
            self.commit_change_refs_by_branch.lock().map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "failed to acquire transaction staged commit change refs lock",
                )
            })?;
        for (row, identity) in rows.iter().zip(&identities) {
            if row.global && row.branch_id != GLOBAL_BRANCH_ID {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "global staged rows must use the global branch id",
                ));
            }
            let Some(RowSlot::State(index)) = by_identity.get(identity).copied() else {
                continue;
            };
            let Some(previous) = staged_rows.get(index) else {
                continue;
            };
            if previous.untracked != row.untracked {
                return Err(mixed_durability_error(row));
            }
        }
        // Reject the entire batch before mutating any journal index or commit
        // metadata. In particular, a later conflicting INSERT must not leave
        // earlier rows from the failed batch as phantom indexed entries.
        let insert_count = rows.iter().filter(|row| row_is_insert(mode, *row)).count();
        if insert_count != 0 {
            let mut insert_order = rows
                .iter()
                .enumerate()
                .filter_map(|(row_index, row)| row_is_insert(mode, row).then_some(row_index))
                .collect::<Vec<_>>();
            insert_order.sort_unstable_by(|&left, &right| {
                identities[left]
                    .cmp(&identities[right])
                    .then_with(|| left.cmp(&right))
            });
            let duplicate_in_batch = insert_order
                .windows(2)
                .filter(|pair| identities[pair[0]] == identities[pair[1]])
                .map(|pair| pair[1])
                .min();
            let duplicate_staged = insert_order
                .iter()
                .copied()
                .filter(|&row_index| by_identity.contains_key(&identities[row_index]))
                .min();
            if let Some(row_index) = duplicate_in_batch.into_iter().chain(duplicate_staged).min() {
                return Err(duplicate_insert_identity_error(rows.row(row_index)));
            }
        }
        // The common file-import write enters the indexed lane because file
        // data accompanies the semantic rows, but the journal is still empty.
        // Populate indexes against the incoming vector and then transfer its
        // allocation intact instead of pushing every large row into a second
        // backing buffer.
        if identities_are_unique && staged_rows.is_empty() && by_identity.is_empty() {
            insert_selection.reserve_rows(rows.len(), insert_count != 0);
            for index in 0..rows.len() {
                let row = rows.row(index);
                let is_insert = row_is_insert(mode, row);
                let commit_id = add_row_to_commit_change_refs(
                    &mut commit_change_refs_guard,
                    row,
                    &self.functions,
                );
                let identity = PreparedStateRowIdentity::from(row);
                if is_insert {
                    insert_selection.push(
                        row.origin,
                        statement_indices
                            .as_ref()
                            .map(|indices| indices[index] as usize),
                    );
                } else {
                    insert_selection.push_not_insert();
                }
                let slot = RowSlot::State(index);
                by_candidate.insert(row, slot);
                let previous = by_identity.insert(identity, slot);
                debug_assert!(previous.is_none(), "validated batch identities are unique");
                rows.set_commit_id(index, commit_id);
            }
            *staged_rows = rows;
            if !file_content_writes.is_empty() {
                self.file_content_writes
                    .lock()
                    .map_err(|_| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            "failed to acquire transaction staged file data lock",
                        )
                    })?
                    .extend(file_content_writes);
            }
            return Ok(TransactionWriteOutcome { count });
        }
        let staged_len = staged_rows.len();
        insert_selection.reserve_rows(rows.len(), insert_count != 0);
        let mut next_destination = staged_len;
        let mut placements = Vec::with_capacity(rows.len());
        let mut inserted_destinations = Vec::with_capacity(insert_count);
        let mut latest_incoming_source_by_destination =
            HashMap::<usize, usize>::with_capacity(rows.len());
        let mut new_candidate_destinations = Vec::new();
        for (source_index, identity) in identities.into_iter().enumerate() {
            let row = rows.row(source_index);
            let is_insert = row_is_insert(mode, row);
            let existing_slot = by_identity.get(&identity).copied();
            let mut requires_transaction_validation = row.facts.requires_transaction_validation;
            if let Some(RowSlot::State(index)) = existing_slot {
                let previous = if let Some(previous_source) =
                    latest_incoming_source_by_destination.get(&index)
                {
                    rows.row(*previous_source)
                } else {
                    staged_rows.row(index)
                };
                requires_transaction_validation |= previous.facts.requires_transaction_validation;
                remove_row_from_commit_change_refs(&mut commit_change_refs_guard, previous);
            }
            if requires_transaction_validation != row.facts.requires_transaction_validation {
                rows.set_requires_transaction_validation(source_index, true);
            }
            let row = rows.row(source_index);
            let commit_id =
                add_row_to_commit_change_refs(&mut commit_change_refs_guard, row, &self.functions);
            let identity = PreparedStateRowIdentity::from(row);
            let insert_metadata = if is_insert {
                Some((
                    row.origin.cloned(),
                    statement_indices
                        .as_ref()
                        .map(|indices| indices[source_index] as usize),
                ))
            } else {
                None
            };
            rows.set_commit_id(source_index, commit_id);
            let destination = match existing_slot {
                Some(RowSlot::State(index)) => index,
                None => {
                    let index = next_destination;
                    next_destination += 1;
                    new_candidate_destinations.push(index);
                    index
                }
            };
            if let Some(metadata) = insert_metadata {
                inserted_destinations.push((destination, metadata));
            }
            placements.push((destination, source_index));
            latest_incoming_source_by_destination.insert(destination, source_index);
            by_identity.insert(identity, RowSlot::State(destination));
        }
        staged_rows.append(rows);
        // Incoming source slots are appended in source order. Before source N
        // is handled, all newly assigned destinations are strictly before its
        // appended source slot, so prior swaps cannot displace that source.
        for (destination, source_index) in placements {
            let source = staged_len + source_index;
            if destination != source {
                staged_rows.swap_rows(destination, source);
            }
        }
        staged_rows.truncate_rows(next_destination);
        insert_selection.resize_rows(next_destination);
        for (destination, (origin, statement_index)) in inserted_destinations {
            insert_selection.mark(destination, origin.as_ref(), statement_index);
        }
        for index in new_candidate_destinations {
            by_candidate.insert(staged_rows.row(index), RowSlot::State(index));
        }
        if !file_content_writes.is_empty() {
            self.file_content_writes
                .lock()
                .map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "failed to acquire transaction staged file data lock",
                    )
                })?
                .extend(file_content_writes);
        }
        Ok(TransactionWriteOutcome { count })
    }

    fn state_rows_from_stage_write(
        &self,
        write: PreparedTransactionWrite,
    ) -> (PreparedStateBatch, Vec<TransactionFileContent>) {
        match write {
            PreparedTransactionWrite::Rows { rows, .. } => (rows, Vec::new()),
            PreparedTransactionWrite::RowsWithFileContent {
                rows, file_content, ..
            } => (rows, file_content),
        }
    }
}

fn row_is_insert(mode: Option<TransactionWriteMode>, row: PreparedStateRowRef<'_>) -> bool {
    mode == Some(TransactionWriteMode::Insert)
        && !row
            .origin
            .as_ref()
            .is_some_and(|origin| origin.operation == TransactionWriteOperation::Update)
}

fn rows_are_append_only_tracked(
    rows: &PreparedStateBatch,
    previous_last: Option<&TrackedStateKey>,
) -> bool {
    let Some(first) = rows.first() else {
        return true;
    };
    if !is_normal_tracked_append_row(first)
        || previous_last.is_some_and(|previous| {
            compare_tracked_key_to_row(previous, first) != std::cmp::Ordering::Less
        })
        || rows
            .iter()
            .any(|row| !is_normal_tracked_append_row(row) || row.branch_id != first.branch_id)
    {
        return false;
    }
    (1..rows.len()).all(|index| {
        let left = rows.row(index - 1);
        let right = rows.row(index);
        is_normal_tracked_append_row(right)
            && compare_rows_by_tracked_key(left, right) == std::cmp::Ordering::Less
    })
}

fn is_normal_tracked_append_row(row: PreparedStateRowRef<'_>) -> bool {
    !row.untracked && !row.global && row.change_id.is_some()
}

fn compare_rows_by_tracked_key(
    left: PreparedStateRowRef<'_>,
    right: PreparedStateRowRef<'_>,
) -> std::cmp::Ordering {
    left.schema_key
        .cmp(&right.schema_key)
        .then_with(|| {
            left.file_id
                .map(|value| value.as_str())
                .cmp(&right.file_id.map(SharedStr::as_str))
        })
        .then_with(|| left.row_pk.cmp(right.row_pk))
}

fn compare_tracked_key_to_row(
    left: &TrackedStateKey,
    right: PreparedStateRowRef<'_>,
) -> std::cmp::Ordering {
    left.schema_key
        .cmp(&right.schema_key)
        .then_with(|| {
            left.file_id
                .as_ref()
                .map(SharedStr::as_str)
                .cmp(&right.file_id.map(SharedStr::as_str))
        })
        .then_with(|| left.row_pk.cmp(right.row_pk))
}

fn compare_tracked_key_to_exact_request(
    left: &TrackedStateKey,
    right: &HotStateExactRowRequest,
) -> std::cmp::Ordering {
    compare_tracked_key_to_parts(
        left,
        right.schema_key.as_str(),
        right.file_id.as_deref(),
        &right.row_pk,
    )
}

fn compare_tracked_key_to_parts(
    left: &TrackedStateKey,
    schema_key: &str,
    file_id: Option<&str>,
    row_pk: &RowPk,
) -> std::cmp::Ordering {
    left.schema_key
        .as_str()
        .cmp(schema_key)
        .then_with(|| left.file_id.as_ref().map(SharedStr::as_str).cmp(&file_id))
        .then_with(|| left.row_pk.cmp(row_pk))
}

fn reorder_rows_by_source_permutation(
    rows: &mut PreparedStateBatch,
    source_by_destination: &mut [usize],
) {
    debug_assert_eq!(rows.len(), source_by_destination.len());
    let len = source_by_destination.len();
    if len < 2 {
        return;
    }

    // Invert `destination -> source` in place. Adding `len` marks visited
    // slots; a Vec cannot have enough elements for that addition to overflow.
    for start in 0..len {
        if source_by_destination[start] >= len {
            continue;
        }
        let mut destination = start;
        let mut source = source_by_destination[destination];
        while source_by_destination[source] < len {
            let next_source = source_by_destination[source];
            source_by_destination[source] = destination + len;
            destination = source;
            source = next_source;
        }
    }
    for destination in source_by_destination.iter_mut() {
        *destination -= len;
    }

    // The inverted permutation maps each current source slot to its final
    // destination. Swapping both arrays fixes one cycle without row copies.
    for source in 0..len {
        while source_by_destination[source] != source {
            let destination = source_by_destination[source];
            rows.swap_rows(source, destination);
            source_by_destination.swap(source, destination);
        }
    }
}

/// Read overlay derived from staged transaction writes.
#[derive(Clone)]
pub(crate) struct PreparedStateRowOverlay {
    staged_writes: Arc<TransactionWriteBuffer>,
}

#[cfg(test)]
pub(crate) struct StagedScanParts {
    pub(crate) rows: Vec<MaterializedHotStateRow>,
}

impl PreparedStateRowOverlay {
    /// Returns staged rows visible for a scan request.
    #[cfg(test)]
    pub(crate) fn scan(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<Vec<MaterializedHotStateRow>, LixError> {
        Ok(crate::hot_state::resolve_visible_batch(
            self.scan_batch(request)?,
            MaterializedHotStateBatch::default(),
            &crate::hot_state::VisibilityRequest {
                branch_scope: crate::hot_state::VisibilityBranchScope::BranchIds {
                    branch_ids: request.filter.branch_ids.clone(),
                },
                include_tombstones: request.filter.include_tombstones,
                limit: None,
            },
        )
        .into_rows())
    }

    /// Returns staged rows and base-row identities hidden by staged rows in one pass.
    ///
    /// Tombstones hide base rows even when the request does not include
    /// tombstone rows in the visible result set.
    #[cfg(test)]
    pub(crate) fn scan_parts(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<StagedScanParts, LixError> {
        Ok(StagedScanParts {
            rows: self.scan_batch(request)?.into_rows(),
        })
    }

    fn scan_batch(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        if matches!(
            request.filter.rows,
            crate::hot_state::HotStateRowFilter::None
        ) {
            return Ok(MaterializedHotStateBatch::default());
        }

        let mut rows = MaterializedHotStateBatchBuilder::with_capacity(0);
        if append_matching_ordered_mutations(&mut rows, &self.staged_writes, request)? {
            return Ok(rows.finish());
        }

        if self
            .staged_writes
            .append_only_scan_is_definitely_absent(request)?
        {
            return Ok(rows.finish());
        }

        self.staged_writes.ensure_identity_index(false)?;
        let rows_guard = self.staged_writes.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let (staged_rows, by_identity, by_candidate) = match &*rows_guard {
            StagedPreparedRows::Indexed {
                rows,
                by_identity,
                by_candidate,
                ..
            } => (rows, by_identity, by_candidate),
            StagedPreparedRows::AppendOnly {
                rows: staged_rows, ..
            } => {
                debug_assert!(
                    staged_rows.is_empty(),
                    "nonempty reads must promote the journal"
                );
                return Ok(rows.finish());
            }
        };

        if let Some(slots) = by_candidate.slots_for_filter(&request.filter) {
            // `slots_for_filter` already selected these rows by schema and,
            // when present, row primary key. Rechecking a large row-PK
            // predicate here turns a keyed transaction-overlay read into an
            // O(candidates * row_pks) scan. The remaining branch,
            // retention, and file filters still need their established final
            // matching because candidate slots intentionally retain their
            // possible global fallbacks.
            append_matching_staged_rows(
                &mut rows,
                slots.iter().copied(),
                staged_rows,
                request,
                true,
            );
        } else {
            append_matching_staged_rows(
                &mut rows,
                by_identity.values().copied(),
                staged_rows,
                request,
                false,
            );
        }
        Ok(rows.finish())
    }

    /// Returns a staged exact-row answer, if this transaction has one.
    #[cfg(test)]
    pub(crate) fn load_exact(&self, request: &HotStateRowRequest) -> Option<StagedExactRow> {
        let identity = PreparedStateRowIdentity::from_row_request(request)?;
        if let Some(row) = self.load_state_slot(&identity) {
            return Some(if row.deleted {
                StagedExactRow::Tombstone
            } else {
                StagedExactRow::Row(row)
            });
        }
        None
    }

    #[cfg(test)]
    fn load_state_slot(
        &self,
        identity: &PreparedStateRowIdentity,
    ) -> Option<MaterializedHotStateRow> {
        self.staged_writes.ensure_identity_index(false).ok()?;
        let rows_guard = self.staged_writes.rows.lock().ok()?;
        let StagedPreparedRows::Indexed {
            rows, by_identity, ..
        } = &*rows_guard
        else {
            return None;
        };
        let Some(RowSlot::State(index)) = by_identity.get(identity).copied() else {
            return None;
        };
        rows.get(index).map(MaterializedHotStateRow::from)
    }
}

impl crate::hot_state::StagedHotStateRows for PreparedStateRowOverlay {
    fn staged_batch(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        self.scan_batch(request)
    }

    fn load_exact_batch(
        &self,
        request: &HotStateExactBatchRequest,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        if request.rows.is_empty() {
            return Ok(MaterializedHotStateExactBatch::default());
        }
        let mut builder = MaterializedHotStateBatchBuilder::with_capacity(request.rows.len());
        let mut slots =
            load_ordered_mutation_exact_batch(&mut builder, &self.staged_writes, request)?;
        if self
            .staged_writes
            .append_only_exact_batch_is_definitely_absent(request)?
        {
            return MaterializedHotStateExactBatch::new(builder.finish(), slots);
        }
        self.staged_writes.ensure_identity_index(false)?;
        let rows_guard = self.staged_writes.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let (staged_rows, by_identity) = match &*rows_guard {
            StagedPreparedRows::Indexed {
                rows, by_identity, ..
            } => (rows, by_identity),
            StagedPreparedRows::AppendOnly { rows, .. } => {
                debug_assert!(rows.is_empty(), "nonempty reads must promote the journal");
                return MaterializedHotStateExactBatch::new(builder.finish(), slots);
            }
        };
        for (request_index, request_row) in request.rows.iter().enumerate() {
            if slots[request_index].is_some() {
                continue;
            }
            let identity = PreparedStateRowIdentity::from_exact_request(request_row);
            let Some(RowSlot::State(index)) = by_identity.get(&identity).copied() else {
                continue;
            };
            let Some(row) = staged_rows.get(index) else {
                continue;
            };
            if request
                .untracked
                .is_some_and(|untracked| row.untracked != untracked)
            {
                continue;
            }
            if row.snapshot.is_none() && !request.include_tombstones {
                continue;
            } else {
                slots[request_index] = Some(
                    u32::try_from(push_prepared_materialized(&mut builder, row))
                        .expect("staged exact batch ordinal must fit u32"),
                );
            }
        }
        MaterializedHotStateExactBatch::new(builder.finish(), slots)
    }

    fn collection_replaced(
        &self,
        branch_id: &str,
        schema_key: &str,
        file_id: Option<&str>,
    ) -> Result<bool, LixError> {
        let rows_guard = self.staged_writes.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let (rows, marker_slots) = match &*rows_guard {
            StagedPreparedRows::AppendOnly { rows, .. } => {
                return append_only_collection_replaced(rows, branch_id, schema_key, file_id);
            }
            StagedPreparedRows::Indexed {
                rows, by_candidate, ..
            } => (
                rows,
                by_candidate
                    .slots_by_schema
                    .get(crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY),
            ),
        };
        let Some(marker_slots) = marker_slots else {
            return Ok(false);
        };
        for slot in marker_slots {
            let RowSlot::State(index) = *slot;
            let Some(row) = rows.get(index) else {
                continue;
            };
            if row.branch_id.as_str() != branch_id || row.snapshot.is_none() {
                continue;
            }
            let (target_schema_key, target_file_id) =
                crate::collection_generation::collection_scope_from_row_pk(row.row_pk)?;
            if target_schema_key == schema_key
                && (target_file_id.is_none() || target_file_id.as_deref() == file_id)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn ordered_mutation_journal_row<'a>(
    journal: &'a OrderedMutationJournal,
    branch_id: &str,
    schema_key: &str,
    file_id: Option<&str>,
    row_pk: &RowPk,
) -> Option<(&'a ImmutableMutationJournalChunk, usize)> {
    if file_id.is_some() || journal.branch_id() != branch_id || journal.schema_key() != schema_key {
        return None;
    }
    let row_pk = row_pk.as_single_string().ok()?;
    let chunk_index = journal
        .chunks
        .partition_point(|chunk| chunk.len() > 0 && chunk.identity(chunk.len() - 1) < row_pk);
    let chunk = journal.chunks.get(chunk_index)?;
    chunk
        .identity_offsets
        .binary_search_by(|&(start, end)| {
            chunk
                .identity_arena
                .as_str()
                .get(start as usize..end as usize)
                .expect("validated immutable mutation identity UTF-8")
                .cmp(row_pk)
        })
        .ok()
        .map(|index| (chunk, index))
}

fn push_ordered_mutation_materialized(
    output: &mut MaterializedHotStateBatchBuilder,
    journal: &OrderedMutationJournal,
    chunk: &ImmutableMutationJournalChunk,
    row_index: usize,
) -> Result<usize, LixError> {
    let (snapshot_start, snapshot_end) = chunk.snapshot_offsets[row_index];
    let snapshot = chunk
        .snapshot_arena
        .slice(snapshot_start as usize..snapshot_end as usize)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable mutation snapshot escaped its shared arena",
            )
        })?;
    let created_at = if let Some(created_at) = journal.overlay_uniform_created_at {
        created_at
    } else {
        chunk
            .durable_predecessors
            .as_ref()
            .and_then(|predecessors| predecessors.get(row_index))
            .map(CertifiedCurrentStatePredecessor::created_at)
            .transpose()?
            .unwrap_or_else(|| chunk.timestamp())
    };
    let (identity_start, identity_end) = chunk.identity_offsets[row_index];
    let identity = chunk
        .identity_arena
        .slice(identity_start as usize..identity_end as usize)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable mutation identity escaped its shared arena",
            )
        })?;
    let row_pk = RowPk::from_validated_shared_string(identity);
    let ordinal = output.push_materialized_ref(
        &row_pk,
        chunk.schema_key(),
        None,
        Some(snapshot),
        None,
        false,
        created_at,
        chunk.timestamp(),
        false,
        None,
        Some(journal.commit_id()),
        false,
        chunk.branch_id(),
    );
    if let Some(predecessor) = chunk
        .durable_predecessors
        .as_ref()
        .and_then(|predecessors| predecessors.get(row_index))
    {
        output.set_durable_predecessor(ordinal, predecessor.clone());
    }
    Ok(ordinal)
}

fn append_matching_ordered_mutations(
    output: &mut MaterializedHotStateBatchBuilder,
    staged_writes: &TransactionWriteBuffer,
    request: &HotStateScanRequest,
) -> Result<bool, LixError> {
    let ordered = staged_writes.ordered_mutations.lock().map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "failed to acquire immutable transaction mutation journal",
        )
    })?;
    let Some(journal) = ordered.as_ref() else {
        return Ok(false);
    };
    if request.filter.untracked == Some(true)
        || (!request.filter.schema_keys.is_empty()
            && !request
                .filter
                .schema_keys
                .iter()
                .any(|schema_key| schema_key == journal.schema_key()))
        || (!request.filter.branch_ids.is_empty()
            && !request
                .filter
                .branch_ids
                .iter()
                .any(|branch_id| branch_id == journal.branch_id()))
        || !nullable_key_matches_filters(None, &request.filter.file_ids)
    {
        return Ok(true);
    }
    if request.filter.row_pks.is_empty() {
        let lower = match request.filter.row_pk_lower.as_ref() {
            Some(bound) => match bound.row_pk.as_single_string() {
                Ok(value) => Some((value, bound.inclusive)),
                Err(_) => return Ok(true),
            },
            None => None,
        };
        let upper = match request.filter.row_pk_upper.as_ref() {
            Some(bound) => match bound.row_pk.as_single_string() {
                Ok(value) => Some((value, bound.inclusive)),
                Err(_) => return Ok(true),
            },
            None => None,
        };
        for chunk in journal.chunks.iter() {
            let start = lower.map_or(0, |(bound, inclusive)| {
                chunk.identity_offsets.partition_point(|&(start, end)| {
                    let identity = chunk
                        .identity_arena
                        .as_str()
                        .get(start as usize..end as usize)
                        .expect("validated immutable mutation identity UTF-8");
                    identity < bound || (!inclusive && identity == bound)
                })
            });
            let end = upper.map_or(chunk.len(), |(bound, inclusive)| {
                chunk.identity_offsets.partition_point(|&(start, end)| {
                    let identity = chunk
                        .identity_arena
                        .as_str()
                        .get(start as usize..end as usize)
                        .expect("validated immutable mutation identity UTF-8");
                    identity < bound || (inclusive && identity == bound)
                })
            });
            for row_index in start..end.max(start) {
                push_ordered_mutation_materialized(output, journal, chunk, row_index)?;
            }
        }
        return Ok(true);
    }

    // Preserve the journal's identity order while routing each requested key
    // through the chunk directory. This avoids the previous O(journal rows ×
    // requested keys) membership loop for large IN predicates.
    let mut requested = request.filter.row_pks.iter().collect::<Vec<_>>();
    requested.sort_unstable();
    requested.dedup();
    for row_pk in requested {
        if !request.filter.matches_row_pk(row_pk) {
            continue;
        }
        let Some((chunk, row_index)) = ordered_mutation_journal_row(
            journal,
            journal.branch_id(),
            journal.schema_key(),
            None,
            row_pk,
        ) else {
            continue;
        };
        push_ordered_mutation_materialized(output, journal, chunk, row_index)?;
    }
    Ok(true)
}

fn load_ordered_mutation_exact_batch(
    output: &mut MaterializedHotStateBatchBuilder,
    staged_writes: &TransactionWriteBuffer,
    request: &HotStateExactBatchRequest,
) -> Result<Vec<Option<u32>>, LixError> {
    let ordered = staged_writes.ordered_mutations.lock().map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "failed to acquire immutable transaction mutation journal",
        )
    })?;
    let Some(journal) = ordered.as_ref() else {
        return Ok(vec![None; request.rows.len()]);
    };
    request
        .rows
        .iter()
        .map(|request_row| {
            if request.untracked == Some(true) {
                return Ok(None);
            }
            let Some((chunk, row_index)) = ordered_mutation_journal_row(
                journal,
                &request_row.branch_id,
                &request_row.schema_key,
                request_row.file_id.as_deref(),
                &request_row.row_pk,
            ) else {
                return Ok(None);
            };
            Ok(Some(
                u32::try_from(push_ordered_mutation_materialized(
                    output, journal, chunk, row_index,
                )?)
                .expect("staged exact batch ordinal must fit u32"),
            ))
        })
        .collect()
}

/// Answers the uncommon collection-generation probe directly from the sorted
/// journal. Promoting every normal point-write transaction to the identity
/// index just to prove that no marker exists defeats append-only staging.
fn append_only_collection_replaced(
    rows: &PreparedStateBatch,
    branch_id: &str,
    schema_key: &str,
    file_id: Option<&str>,
) -> Result<bool, LixError> {
    let marker_schema = crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY;
    for row_index in ordered_schema_row_range(rows, marker_schema) {
        let row = rows.row(row_index);
        if row.branch_id.as_str() != branch_id || row.snapshot.is_none() {
            continue;
        }
        let (target_schema_key, target_file_id) =
            crate::collection_generation::collection_scope_from_row_pk(row.row_pk)?;
        if target_schema_key == schema_key
            && (target_file_id.is_none() || target_file_id.as_deref() == file_id)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Locates one schema's contiguous slice in tracked-key order without
/// constructing transaction-wide identity or candidate indexes.
fn ordered_schema_row_range(rows: &PreparedStateBatch, schema_key: &str) -> std::ops::Range<usize> {
    let mut lower = 0;
    let mut upper = rows.len();
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        if rows.row(middle).schema_key.as_str() < schema_key {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    let start = lower;
    upper = rows.len();
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        if rows.row(middle).schema_key.as_str() <= schema_key {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    start..lower
}

#[cfg(test)]
pub(crate) enum StagedExactRow {
    Row(MaterializedHotStateRow),
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PreparedStateRowIdentity {
    schema_key: SharedStr,
    row_pk: RowPk,
    file_id: Option<SharedStr>,
    branch_id: SharedStr,
}

impl PreparedStateRowIdentity {
    fn from_staged_row(row: PreparedStateRowRef<'_>) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            row_pk: row.row_pk.clone(),
            file_id: row.file_id.cloned(),
            branch_id: row.branch_id.clone(),
        }
    }

    fn from_exact_request(request: &HotStateExactRowRequest) -> Self {
        Self {
            schema_key: request.schema_key.as_str().into(),
            row_pk: request.row_pk.clone(),
            file_id: request.file_id.as_deref().map(Into::into),
            branch_id: request.branch_id.as_str().into(),
        }
    }

    #[cfg(test)]
    fn from_row_request(request: &HotStateRowRequest) -> Option<Self> {
        let file_id = match &request.file_id {
            NullableKeyFilter::Null => None,
            NullableKeyFilter::Value(value) => Some(value.clone()),
            // Exact overlay lookup requires a concrete row identity.
            NullableKeyFilter::Any => return None,
        };
        Some(Self {
            schema_key: request.schema_key.as_str().into(),
            row_pk: request.row_pk.clone(),
            file_id: file_id.map(Into::into),
            branch_id: request.branch_id.as_str().into(),
        })
    }
}

#[cfg(test)]
impl From<&TestPreparedStateRow> for PreparedStateRowIdentity {
    fn from(row: &TestPreparedStateRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            row_pk: row.row_pk.clone(),
            file_id: row.file_id.clone(),
            branch_id: row.branch_id.clone(),
        }
    }
}

impl From<PreparedStateRowRef<'_>> for PreparedStateRowIdentity {
    fn from(row: PreparedStateRowRef<'_>) -> Self {
        Self::from_staged_row(row)
    }
}

impl From<&PreparedStateRowRef<'_>> for PreparedStateRowIdentity {
    fn from(row: &PreparedStateRowRef<'_>) -> Self {
        Self::from_staged_row(*row)
    }
}

#[cfg(test)]
impl From<&MaterializedHotStateRow> for PreparedStateRowIdentity {
    fn from(row: &MaterializedHotStateRow) -> Self {
        Self {
            schema_key: row.schema_key.as_str().into(),
            row_pk: row.row_pk.clone(),
            file_id: row.file_id.as_deref().map(Into::into),
            branch_id: row.branch_id.as_ref().into(),
        }
    }
}

fn validate_batch_row_identities(
    rows: &PreparedStateBatch,
    identities: &[PreparedStateRowIdentity],
) -> Result<bool, LixError> {
    debug_assert_eq!(rows.len(), identities.len());
    if identities.windows(2).all(|pair| pair[0] <= pair[1]) {
        return validate_rows_in_identity_order(rows, identities, 0..rows.len());
    }

    // Irregular frontend batches share one dense ordering buffer rather than
    // allocating BTree nodes across the batch. The source index is the
    // tiebreaker so equal-identity transitions retain their original order.
    let mut order = (0..rows.len()).collect::<Vec<_>>();
    order.sort_unstable_by(|&left, &right| {
        identities[left]
            .cmp(&identities[right])
            .then_with(|| left.cmp(&right))
    });
    validate_rows_in_identity_order(rows, identities, order)
}

enum BatchIdentityViolation<'a> {
    MixedDurability(PreparedStateRowRef<'a>),
    DuplicatePresent {
        row: PreparedStateRowRef<'a>,
        previous: PreparedStateRowRef<'a>,
    },
}

fn validate_rows_in_identity_order<'a>(
    rows: &'a PreparedStateBatch,
    identities: &[PreparedStateRowIdentity],
    order: impl IntoIterator<Item = usize>,
) -> Result<bool, LixError> {
    let mut previous_identity = None::<&PreparedStateRowIdentity>;
    let mut group_untracked = false;
    let mut pending_present = None::<PreparedStateRowRef<'a>>;
    let mut unique_count = 0usize;
    let mut earliest_violation = None::<(usize, BatchIdentityViolation<'a>)>;

    for index in order {
        let row = rows.row(index);
        let identity = &identities[index];
        if previous_identity != Some(identity) {
            previous_identity = Some(identity);
            group_untracked = row.untracked;
            pending_present = row.snapshot.map(|_| row);
            unique_count += 1;
            continue;
        }

        if group_untracked != row.untracked {
            retain_earliest_batch_identity_violation(
                &mut earliest_violation,
                index,
                BatchIdentityViolation::MixedDurability(row),
            );
        }
        if row.snapshot.is_none() {
            pending_present = None;
        } else if let Some(previous) = pending_present.replace(row) {
            retain_earliest_batch_identity_violation(
                &mut earliest_violation,
                index,
                BatchIdentityViolation::DuplicatePresent { row, previous },
            );
        }
    }

    if let Some((_, violation)) = earliest_violation {
        return Err(match violation {
            BatchIdentityViolation::MixedDurability(row) => mixed_durability_error(row),
            BatchIdentityViolation::DuplicatePresent { row, previous } => {
                duplicate_staged_present_row_error(row, previous)
            }
        });
    }
    Ok(unique_count == rows.len())
}

fn retain_earliest_batch_identity_violation<'a>(
    earliest: &mut Option<(usize, BatchIdentityViolation<'a>)>,
    index: usize,
    violation: BatchIdentityViolation<'a>,
) {
    if earliest
        .as_ref()
        .is_none_or(|(earliest_index, _)| index < *earliest_index)
    {
        *earliest = Some((index, violation));
    }
}

fn mixed_durability_error(row: PreparedStateRowRef<'_>) -> LixError {
    let row_pk = row
        .row_pk
        .as_json_array_text()
        .unwrap_or_else(|_| "<invalid row_pk>".to_string());
    LixError::new(
        LixError::CODE_INVALID_PARAM,
        format!(
            "cannot mix tracked and untracked writes for schema '{}' row_pk '{}' in branch '{}' within one transaction; commit or roll back before changing durability",
            row.schema_key, row_pk, row.branch_id
        ),
    )
}

fn duplicate_staged_present_row_error(
    row: PreparedStateRowRef<'_>,
    previous: PreparedStateRowRef<'_>,
) -> LixError {
    let message = logical_primary_key_violation_message(row.origin)
        .unwrap_or_else(|| {
            format!(
                "primary-key constraint violation on schema '{}': duplicate staged rows for row_pk '{}' in branch '{}'",
                row.schema_key,
                previous
                    .row_pk
                    .as_json_array_text()
                    .unwrap_or_else(|_| "<invalid row_pk>".to_string()),
                row.branch_id
            )
        });
    LixError::new(LixError::CODE_UNIQUE, message)
}

pub(crate) fn duplicate_insert_identity_message(
    schema_key: &str,
    row_pk: &RowPk,
    branch_id: Option<&str>,
    origin: Option<&TransactionWriteOrigin>,
) -> String {
    if let Some(message) = logical_primary_key_violation_message(origin) {
        return message;
    }
    let row_pk = row_pk
        .as_json_array_text()
        .unwrap_or_else(|_| "<invalid row_pk>".to_string());
    match branch_id {
        Some(branch_id) => format!(
            "primary-key constraint violation on schema '{schema_key}': INSERT would duplicate row_pk '{row_pk}' in branch '{branch_id}'"
        ),
        None => format!(
            "primary-key constraint violation on schema '{schema_key}': INSERT would duplicate row_pk '{row_pk}'"
        ),
    }
}

fn duplicate_insert_identity_error(row: PreparedStateRowRef<'_>) -> LixError {
    let message = duplicate_insert_identity_message(
        row.schema_key,
        row.row_pk,
        Some(row.branch_id),
        row.origin,
    );
    LixError::new(LixError::CODE_UNIQUE, message)
}

fn logical_primary_key_violation_message(
    origin: Option<&TransactionWriteOrigin>,
) -> Option<String> {
    let origin = origin?;
    if origin.operation != TransactionWriteOperation::Insert {
        return None;
    }
    let primary_key = origin.primary_key.as_ref()?;
    Some(format!(
        "primary-key constraint violation on table '{}': INSERT would duplicate {}",
        origin.surface,
        format_logical_primary_key(primary_key)
    ))
}

fn format_logical_primary_key(primary_key: &LogicalPrimaryKey) -> String {
    primary_key
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let value = primary_key
                .values
                .get(index)
                .map(String::as_str)
                .unwrap_or("<missing>");
            format!("{column} '{value}'")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn add_row_to_commit_change_refs(
    change_refs_by_branch: &mut BTreeMap<String, StagedCommitChangeRefs>,
    row: PreparedStateRowRef<'_>,
    functions: &FunctionProviderHandle,
) -> Option<CommitId> {
    if row.untracked {
        return row.commit_id;
    }
    let change_id = row
        .change_id
        .expect("tracked staged rows must carry change_id for commit change refs");
    if !change_refs_by_branch.contains_key(row.branch_id.as_str()) {
        change_refs_by_branch.insert(row.branch_id.to_string(), {
            let timestamp = functions.call_timestamp();
            StagedCommitChangeRefs::new(
                CommitId::with_change_address_space(functions.call_uuid_v7()),
                ChangeId::from(functions.call_uuid_v7()),
                timestamp,
            )
        });
    }
    let change_refs = change_refs_by_branch
        .get_mut(row.branch_id.as_str())
        .expect("branch change refs were inserted above");
    change_refs.add_change_id(change_id);
    Some(change_refs.commit_id)
}

fn remove_row_from_commit_change_refs(
    change_refs_by_branch: &mut BTreeMap<String, StagedCommitChangeRefs>,
    row: PreparedStateRowRef<'_>,
) {
    if row.untracked {
        return;
    }
    let Some(change_refs) = change_refs_by_branch.get_mut(row.branch_id.as_str()) else {
        return;
    };
    let Some(change_id) = row.change_id.as_ref() else {
        return;
    };
    change_refs.remove_change_id(change_id);
    if change_refs.is_empty() {
        change_refs_by_branch.remove(row.branch_id.as_str());
    }
}

fn append_matching_staged_rows(
    output: &mut MaterializedHotStateBatchBuilder,
    slots: impl IntoIterator<Item = RowSlot>,
    staged_rows: &PreparedStateBatch,
    request: &HotStateScanRequest,
    candidate_index_matched_schema_and_row: bool,
) {
    for slot in slots {
        let RowSlot::State(index) = slot;
        let Some(row) = staged_rows.get(index) else {
            continue;
        };
        if staged_row_identity_matches_scan(row, request, candidate_index_matched_schema_and_row) {
            push_prepared_materialized(output, row);
        }
    }
}

fn push_prepared_materialized(
    output: &mut MaterializedHotStateBatchBuilder,
    row: PreparedStateRowRef<'_>,
) -> usize {
    output.push_materialized_ref(
        row.row_pk,
        row.schema_key.as_str(),
        row.file_id.map(SharedStr::as_str),
        row.snapshot.map(StageJson::materialize_shared),
        row.metadata.map(StageJson::materialize_shared),
        row.snapshot.is_none(),
        row.created_at,
        row.updated_at,
        row.global,
        (!row.addressable_change_id || row.untracked)
            .then_some(row.change_id)
            .flatten(),
        row.commit_id,
        row.untracked,
        row.branch_id.as_str(),
    )
}

fn staged_row_identity_matches_scan(
    row: PreparedStateRowRef<'_>,
    request: &HotStateScanRequest,
    candidate_index_matched_schema_and_row: bool,
) -> bool {
    if !candidate_index_matched_schema_and_row
        && !request.filter.schema_keys.is_empty()
        && !request
            .filter
            .schema_keys
            .iter()
            .any(|schema_key| schema_key == row.schema_key.as_str())
    {
        return false;
    }
    if !request.filter.matches_row_pk(row.row_pk) {
        return false;
    }
    if !staged_branch_matches_scan(row.branch_id, request) {
        return false;
    }
    if request
        .filter
        .untracked
        .is_some_and(|untracked| row.untracked != untracked)
    {
        return false;
    }
    nullable_key_matches_filters(row.file_id.map(SharedStr::as_str), &request.filter.file_ids)
}

fn nullable_key_matches_filters(
    value: Option<&str>,
    filters: &[NullableKeyFilter<String>],
) -> bool {
    filters.is_empty()
        || filters
            .iter()
            .any(|filter| nullable_key_matches_filter(value, filter))
}

fn staged_branch_matches_scan(branch_id: &str, request: &HotStateScanRequest) -> bool {
    request.filter.branch_ids.is_empty()
        || request
            .filter
            .branch_ids
            .iter()
            .any(|requested| requested == branch_id)
        || (branch_id == GLOBAL_BRANCH_ID
            && request
                .filter
                .branch_ids
                .iter()
                .any(|requested| requested != GLOBAL_BRANCH_ID))
}

fn nullable_key_matches_filter(value: Option<&str>, filter: &NullableKeyFilter<String>) -> bool {
    match filter {
        NullableKeyFilter::Any => true,
        NullableKeyFilter::Null => value.is_none(),
        NullableKeyFilter::Value(expected) => value == Some(expected.as_str()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hot_state::{
        HotStateExactBatchRequest, HotStateExactRowRequest, HotStateFilter, HotStateRowRequest,
        StagedHotStateRows,
    };

    macro_rules! prepared_rows {
        ($($row:expr),* $(,)?) => {
            PreparedStateBatch::from_test_rows(vec![$($row),*])
        };
    }

    #[test]
    fn immutable_journal_single_string_identities_share_one_arena() {
        let first_snapshot = r#"{"path":"a","value":1}"#;
        let second_snapshot = r#"{"path":"b","value":2}"#;
        let snapshots = format!("{first_snapshot}{second_snapshot}");
        let chunk = ImmutableMutationJournalChunk::try_new_single_string_identities(
            SchemaPlanId::for_test(0),
            "schema".into(),
            "branch".into(),
            None,
            b"ab".to_vec(),
            vec![(0, 1), (1, 2)],
            snapshots.into_bytes(),
            vec![
                (0, first_snapshot.len()),
                (
                    first_snapshot.len(),
                    first_snapshot.len() + second_snapshot.len(),
                ),
            ],
            None,
            LixTimestamp::expect_parse("timestamp", "2026-01-01T00:00:00Z"),
        )
        .expect("shared identity chunk");

        assert_eq!(chunk.identity(0), "a");
        assert_eq!(chunk.identity(1), "b");
        assert_eq!(chunk.identity_arena.len(), 2);
    }

    #[test]
    fn immutable_journal_overlay_applies_primary_key_bounds_before_materialization() {
        let staged_writes = test_staged_writes();
        let snapshots = ["a", "b", "c", "d"]
            .into_iter()
            .map(|identity| format!(r#"{{"id":"{identity}"}}"#))
            .collect::<Vec<_>>();
        let snapshot_arena = snapshots.concat();
        let mut cursor = 0_usize;
        let snapshot_offsets = snapshots
            .iter()
            .map(|snapshot| {
                let start = cursor;
                cursor += snapshot.len();
                (start, cursor)
            })
            .collect();
        let chunk = ImmutableMutationJournalChunk::try_new_single_string_identities(
            SchemaPlanId::for_test(0),
            "schema".into(),
            "branch".into(),
            None,
            b"abcd".to_vec(),
            vec![(0, 1), (1, 2), (2, 3), (3, 4)],
            snapshot_arena.into_bytes(),
            snapshot_offsets,
            None,
            LixTimestamp::expect_parse("timestamp", "2026-01-01T00:00:00Z"),
        )
        .expect("bounded overlay chunk");
        assert!(matches!(
            staged_writes
                .stage_immutable_mutation_chunk(chunk)
                .expect("immutable chunk should stage"),
            ImmutableMutationChunkStage::Staged
        ));
        let overlay = staged_writes
            .staging_overlay()
            .expect("bounded immutable overlay");
        let request = |lower: (&str, bool), upper: (&str, bool)| HotStateScanRequest {
            filter: HotStateFilter {
                branch_ids: vec!["branch".into()],
                schema_keys: vec!["schema".into()],
                file_ids: vec![NullableKeyFilter::Null],
                row_pk_lower: Some(crate::tracked_state::RowPkRangeBound {
                    row_pk: RowPk::single(lower.0),
                    inclusive: lower.1,
                }),
                row_pk_upper: Some(crate::tracked_state::RowPkRangeBound {
                    row_pk: RowPk::single(upper.0),
                    inclusive: upper.1,
                }),
                ..HotStateFilter::default()
            },
            ..HotStateScanRequest::default()
        };

        let rows = overlay
            .scan(&request(("a", false), ("d", false)))
            .expect("open staged range");
        assert_eq!(
            rows.into_iter()
                .map(|row| row.row_pk.into_parts())
                .collect::<Vec<_>>(),
            vec![vec!["b"], vec!["c"]]
        );
        let mut exact_bounded = request(("b", true), ("c", true));
        exact_bounded.filter.row_pks = vec![
            RowPk::single("a"),
            RowPk::single("b"),
            RowPk::single("d"),
        ];
        let exact_rows = overlay
            .scan(&exact_bounded)
            .expect("exact staged candidates must intersect range bounds");
        assert_eq!(
            exact_rows
                .into_iter()
                .map(|row| row.row_pk.into_parts())
                .collect::<Vec<_>>(),
            vec![vec!["b"]]
        );
        assert!(
            overlay
                .scan(&request(("d", true), ("a", true)))
                .expect("inverted staged range")
                .is_empty()
        );
        assert!(
            !staged_writes.uses_identity_index_for_tests(),
            "bounded reads must retain the compact ordered journal"
        );
    }

    #[test]
    fn immutable_journal_rejects_invalid_utf8_and_split_offsets() {
        assert!(
            ImmutableMutationJournalChunk::try_new_single_string_identities(
                SchemaPlanId::for_test(0),
                "schema".into(),
                "branch".into(),
                None,
                vec![0xff],
                vec![(0, 1)],
                b"{}".to_vec(),
                vec![(0, 2)],
                None,
                LixTimestamp::expect_parse("timestamp", "2026-01-01T00:00:00Z"),
            )
            .is_err(),
            "the journal must reject an invalid identity arena"
        );

        let snapshots = "é".as_bytes().to_vec();
        assert!(
            ImmutableMutationJournalChunk::try_new_single_string_identities(
                SchemaPlanId::for_test(0),
                "schema".into(),
                "branch".into(),
                None,
                b"ab".to_vec(),
                vec![(0, 1), (1, 2)],
                snapshots,
                vec![(0, 1), (1, 2)],
                None,
                LixTimestamp::expect_parse("timestamp", "2026-01-01T00:00:00Z"),
            )
            .is_err(),
            "the journal must reject snapshot offsets inside a UTF-8 scalar"
        );
    }

    #[test]
    fn immutable_journal_large_snapshot_refs_support_more_than_u16_rows() {
        const ROW_COUNT: usize = 65_537;
        let mut identities = Vec::with_capacity(ROW_COUNT * 6);
        let mut identity_offsets = Vec::with_capacity(ROW_COUNT);
        let mut snapshots = Vec::with_capacity(ROW_COUNT * 2 + 512);
        let mut snapshot_offsets = Vec::with_capacity(ROW_COUNT);
        for row in 0..ROW_COUNT {
            let start = identities.len();
            identities.extend_from_slice(format!("{row:06}").as_bytes());
            identity_offsets.push((start, identities.len()));

            let start = snapshots.len();
            if row + 1 == ROW_COUNT {
                snapshots.extend(std::iter::repeat_n(
                    b'x',
                    crate::json_store::JSON_INLINE_MAX_BYTES + 1,
                ));
            } else {
                snapshots.extend_from_slice(b"{}");
            }
            snapshot_offsets.push((start, snapshots.len()));
        }

        let chunk = ImmutableMutationJournalChunk::try_new_single_string_identities(
            SchemaPlanId::for_test(0),
            "schema".into(),
            "branch".into(),
            None,
            identities,
            identity_offsets,
            snapshots,
            snapshot_offsets,
            None,
            LixTimestamp::expect_parse("timestamp", "2026-01-01T00:00:00Z"),
        )
        .expect("journal chunk above the u16 row boundary");

        assert!(matches!(
            chunk.snapshot_slot(ROW_COUNT - 1),
            crate::json_store::JsonSlotRef::Ref(_)
        ));
    }

    #[test]
    fn immutable_journal_does_not_seal_adaptive_residual_at_chunk_boundary() {
        const ROW_COUNT: usize = 8 * crate::tracked_state::REPLACEMENT_PART_MAX_ROWS;
        let mut identity_values = Vec::with_capacity(ROW_COUNT);
        let mut random = 0x9e37_79b9_7f4a_7c15_u64;
        for _ in 0..ROW_COUNT {
            let mut identity = String::with_capacity(640);
            for _ in 0..40 {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                use std::fmt::Write as _;
                write!(&mut identity, "{random:016x}").expect("write identity");
            }
            identity_values.push(identity);
        }
        identity_values.sort_unstable();

        let mut identities = Vec::with_capacity(ROW_COUNT * 640);
        let mut identity_offsets = Vec::with_capacity(ROW_COUNT);
        let mut snapshots = Vec::with_capacity(ROW_COUNT * 2);
        let mut snapshot_offsets = Vec::with_capacity(ROW_COUNT);
        for identity in identity_values {
            let start = identities.len();
            identities.extend_from_slice(identity.as_bytes());
            identity_offsets.push((start, identities.len()));

            let start = snapshots.len();
            snapshots.extend_from_slice(b"{}");
            snapshot_offsets.push((start, snapshots.len()));
        }

        let mut chunk = ImmutableMutationJournalChunk::try_new_single_string_identities(
            SchemaPlanId::for_test(0),
            "schema".into(),
            "branch".into(),
            None,
            identities,
            identity_offsets,
            snapshots,
            snapshot_offsets,
            None,
            LixTimestamp::expect_parse("timestamp", "2026-01-01T00:00:00Z"),
        )
        .expect("large-key journal chunk");
        let mut compressor = None;

        chunk
            .seal_replacement_parts(false, &mut compressor)
            .expect("non-final sealing probe");
        assert!(chunk.sealed_replacement_parts().is_none());

        chunk
            .seal_replacement_parts(true, &mut compressor)
            .expect("final canonical sealing");
        assert!(chunk.sealed_replacement_parts().is_some());
    }

    #[test]
    fn generic_insert_after_certified_batch_does_not_inherit_statement_index() {
        let mut selection = PreparedInsertSelection::new();
        selection.push_certified_ordinal_inserts(2);
        assert!(selection.is_complete_ordinal_selection(2));
        assert!(selection.origins.is_empty());
        selection.push(None, None);

        assert!(!selection.is_complete_ordinal_selection(3));
        assert!(selection.origins.is_empty());
        assert_eq!(selection.statement_index(0), Some(0));
        assert_eq!(selection.statement_index(1), Some(1));
        assert_eq!(selection.statement_index(2), None);
    }

    #[test]
    fn ordered_tracked_batches_stay_append_only_until_a_read() {
        let staged_writes = test_staged_writes();
        for rows in [
            prepared_rows![
                tracked_append_row("row-a", "first"),
                tracked_append_row("row-b", "second"),
            ],
            prepared_rows![
                tracked_append_row("row-c", "third"),
                tracked_append_row("row-d", "fourth"),
            ],
        ] {
            staged_writes
                .stage_write(PreparedTransactionWrite::Rows {
                    mode: TransactionWriteMode::Replace,
                    rows,
                })
                .expect("ordered tracked batch should stage in the journal");
        }

        assert!(
            !staged_writes.uses_identity_index_for_tests(),
            "SQL-shaped repeated sorted batches should not build an identity index"
        );
        let drained = staged_writes.drain().expect("journal should drain");
        assert_eq!(drained.state_rows.len(), 4);
        let refs = drained
            .commit_change_refs_by_branch
            .get("01920000-0000-7000-8000-0000000000a1")
            .expect("tracked branch should have commit refs");
        assert_eq!(refs.tracked_change_count, 4);
        assert!(
            drained
                .state_rows
                .iter()
                .all(|row| row.commit_id == Some(refs.commit_id))
        );
    }

    #[test]
    fn future_exact_reads_keep_ordered_tracked_journal_append_only() {
        let staged_writes = test_staged_writes();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![tracked_append_row("row-a", "first")],
            })
            .expect("first ordered replacement should stage");
        let overlay = staged_writes
            .staging_overlay()
            .expect("ordered journal overlay should build");
        let exact = |row: &str| HotStateExactRowRequest {
            schema_key: "lix_key_value".into(),
            branch_id: "01920000-0000-7000-8000-0000000000a1".into(),
            row_pk: RowPk::single(row),
            file_id: None,
        };

        let future_scan = overlay
            .scan(&scan_request_for_key("row-b", false))
            .expect("future keyed scan should be proven absent");
        assert!(future_scan.is_empty());
        assert!(
            !staged_writes.uses_identity_index_for_tests(),
            "a keyed scan after the journal tail must not materialize read indexes"
        );

        let future = StagedHotStateRows::load_exact_batch(
            &overlay,
            &HotStateExactBatchRequest {
                rows: vec![exact("row-b")],
                ..Default::default()
            },
        )
        .expect("future exact row should be proven absent");
        assert!(future.row(0).is_none());
        assert!(
            !staged_writes.uses_identity_index_for_tests(),
            "a key after the journal tail must not materialize read indexes"
        );

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![tracked_append_row("row-b", "second")],
            })
            .expect("future ordered replacement should extend the journal");
        assert!(
            !staged_writes.uses_identity_index_for_tests(),
            "future exact absence proof must preserve append-only staging"
        );

        let overlap = StagedHotStateRows::load_exact_batch(
            &overlay,
            &HotStateExactBatchRequest {
                rows: vec![exact("row-a")],
                ..Default::default()
            },
        )
        .expect("overlapping exact row should load from the journal");
        assert!(overlap.row(0).is_some());
        assert!(
            staged_writes.uses_identity_index_for_tests(),
            "an overlapping read must retain normal read-your-writes semantics"
        );
    }

    #[test]
    fn collection_marker_probe_does_not_promote_ordered_journal() {
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let ordinary_writes = test_staged_writes();
        ordinary_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![tracked_append_row("row-a", "first")],
            })
            .expect("ordinary replacement should stage");
        let ordinary_overlay = ordinary_writes
            .staging_overlay()
            .expect("ordinary overlay should build");
        assert!(
            !StagedHotStateRows::collection_replaced(
                &ordinary_overlay,
                branch_id,
                "json_pointer",
                None,
            )
            .expect("ordinary journal marker probe should succeed")
        );
        assert!(
            !ordinary_writes.uses_identity_index_for_tests(),
            "proving that an ordered journal has no marker must not build identity indexes"
        );

        let marker_writes = test_staged_writes();
        marker_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    state_row("[\"json_pointer\",null]", "marker")
                        .with_schema(crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY)
                        .with_tracked()
                        .with_branch(branch_id)
                        .with_change_id("collection-marker")
                ],
            })
            .expect("collection marker should stage");
        let marker_overlay = marker_writes
            .staging_overlay()
            .expect("marker overlay should build");
        assert!(
            StagedHotStateRows::collection_replaced(
                &marker_overlay,
                branch_id,
                "json_pointer",
                None,
            )
            .expect("marker journal probe should succeed")
        );
        assert!(
            !marker_writes.uses_identity_index_for_tests(),
            "reading a sorted collection marker must not build identity indexes"
        );
    }

    #[test]
    fn ordered_tracked_insert_batches_stay_append_only_with_absence_guards() {
        let staged_writes = test_staged_writes();
        for rows in [
            prepared_rows![
                tracked_append_row("row-a", "first"),
                tracked_append_row("row-b", "second"),
            ],
            prepared_rows![
                tracked_append_row("row-c", "third"),
                tracked_append_row("row-d", "fourth"),
            ],
        ] {
            staged_writes
                .stage_write(PreparedTransactionWrite::Rows {
                    mode: TransactionWriteMode::Insert,
                    rows,
                })
                .expect("ordered tracked inserts should stage in the journal");
        }

        assert!(
            !staged_writes.uses_identity_index_for_tests(),
            "ordered SQL INSERT batches should not build an identity index"
        );
        let drained = staged_writes.drain().expect("journal should drain");
        assert_eq!(drained.state_rows.len(), 4);
        assert_eq!(
            drained.insert_selection.len(),
            4,
            "the terminal root still needs INSERT absence guards"
        );
        assert_eq!(
            drained
                .commit_change_refs_by_branch
                .get("01920000-0000-7000-8000-0000000000a1")
                .expect("tracked branch should have commit refs")
                .tracked_change_count,
            4
        );
    }

    #[test]
    fn overlapping_tracked_insert_promotes_and_rejects_the_duplicate() {
        let staged_writes = test_staged_writes();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: prepared_rows![
                    tracked_append_row("row-a", "first"),
                    tracked_append_row("row-b", "second"),
                ],
            })
            .expect("initial ordered insert batch should use the journal");

        let error = staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: prepared_rows![
                    tracked_append_row("row-a", "duplicate").with_change_id("duplicate-row-a"),
                ],
            })
            .expect_err("a later INSERT must reject a journaled identity");
        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            staged_writes.uses_identity_index_for_tests(),
            "an overlap must promote before generic INSERT validation"
        );

        let drained = staged_writes
            .drain()
            .expect("first batch should remain intact");
        assert_eq!(drained.state_rows.len(), 2);
        assert_eq!(drained.insert_selection.len(), 2);
    }

    #[test]
    fn failed_insert_batch_does_not_leave_phantom_rows_or_commit_metadata() {
        let staged_writes = test_staged_writes();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: prepared_rows![tracked_append_row("row-a", "first")],
            })
            .expect("initial INSERT should stage");

        let error = staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: prepared_rows![
                    tracked_append_row("row-c", "must-not-stage"),
                    tracked_append_row("row-a", "duplicate").with_change_id("duplicate-row-a"),
                ],
            })
            .expect_err("the later duplicate must reject the whole incoming batch");
        assert_eq!(error.code, LixError::CODE_UNIQUE);

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should remain internally consistent");
        assert!(
            overlay
                .load_exact(&exact_request_for_branch_key(
                    "01920000-0000-7000-8000-0000000000a1",
                    "row-c",
                ))
                .is_none(),
            "the successful prefix of a failed batch must not become a phantom overlay row"
        );

        let drained = staged_writes.drain().expect("original INSERT should drain");
        assert_eq!(drained.state_rows.len(), 1);
        assert_eq!(
            drained
                .state_rows
                .row(0)
                .row_pk
                .as_single_string()
                .expect("scalar row"),
            "row-a"
        );
        assert_eq!(drained.insert_selection.len(), 1);
        assert_eq!(
            drained
                .commit_change_refs_by_branch
                .get("01920000-0000-7000-8000-0000000000a1")
                .expect("tracked branch metadata")
                .tracked_change_count,
            1
        );
    }

    #[test]
    fn empty_overlay_scan_does_not_disable_first_tracked_append_batch() {
        let staged_writes = test_staged_writes();
        let overlay = staged_writes
            .staging_overlay()
            .expect("empty overlay should build");
        assert!(
            overlay
                .scan_parts(&scan_request_for_key("schema-probe", false))
                .expect("empty overlay scan should succeed")
                .rows
                .is_empty()
        );
        assert!(
            !staged_writes.uses_identity_index_for_tests(),
            "schema normalization's empty overlay lookup must retain journal mode"
        );

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    tracked_append_row("row-a", "first"),
                    tracked_append_row("row-b", "second"),
                ],
            })
            .expect("first tracked batch should still use the journal");
        assert!(!staged_writes.uses_identity_index_for_tests());
    }

    #[test]
    fn transaction_read_lazily_promotes_the_append_journal_once() {
        let staged_writes = test_staged_writes();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    tracked_append_row("row-a", "first"),
                    tracked_append_row("row-b", "second"),
                ],
            })
            .expect("tracked batch should stage");
        assert!(!staged_writes.uses_identity_index_for_tests());

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build lazily");
        let row = overlay
            .load_exact(&exact_request_for_branch_key(
                "01920000-0000-7000-8000-0000000000a1",
                "row-b",
            ))
            .expect("staged row should answer the exact read");
        assert!(matches!(row, StagedExactRow::Row(_)));
        assert!(staged_writes.uses_identity_index_for_tests());

        assert!(
            overlay
                .load_exact(&exact_request_for_branch_key(
                    "01920000-0000-7000-8000-0000000000a1",
                    "row-a"
                ))
                .is_some()
        );
        assert!(
            staged_writes.uses_identity_index_for_tests(),
            "reads keep the single materialized index rather than rebuilding it"
        );
    }

    #[test]
    fn indexed_schema_catalog_probe_uses_schema_candidates_and_preserves_domains() {
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let staged_writes = test_staged_writes();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![tracked_append_row("row-a", "ordinary")],
            })
            .expect("ordinary row should stage");
        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build");
        assert!(
            overlay
                .load_exact(&exact_request_for_branch_key(branch_id, "row-a"))
                .is_some()
        );
        assert!(staged_writes.uses_identity_index_for_tests());

        let tracked_domain = Domain::schema_catalog(branch_id, false);
        assert!(
            !staged_writes
                .has_staged_schema_catalog_change(&tracked_domain)
                .expect("indexed catalog probe should succeed")
        );

        let mut schema_row = tracked_append_row("schema-a", "schema");
        schema_row.schema_key = crate::transaction::normalization::REGISTERED_SCHEMA_KEY.into();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![schema_row],
            })
            .expect("registered schema row should stage into the index");

        assert!(
            staged_writes
                .has_staged_schema_catalog_change(&tracked_domain)
                .expect("tracked catalog change should be detected")
        );
        assert!(
            staged_writes
                .has_staged_schema_catalog_change(&Domain::schema_catalog(branch_id, true))
                .expect("untracked catalog includes tracked schema definitions")
        );
        assert!(
            !staged_writes
                .has_staged_schema_catalog_change(&Domain::schema_catalog(
                    "01920000-0000-7000-8000-0000000000b1",
                    false,
                ))
                .expect("other branch should remain independent")
        );
    }

    #[test]
    fn schema_catalog_probe_binary_searches_ordered_journal() {
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let tracked_domain = Domain::schema_catalog(branch_id, false);
        let ordinary_writes = test_staged_writes();
        ordinary_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![tracked_append_row("row-a", "ordinary")],
            })
            .expect("ordinary row should stage");
        assert!(
            !ordinary_writes
                .has_staged_schema_catalog_change(&tracked_domain)
                .expect("ordered catalog absence probe should succeed")
        );
        assert!(
            !ordinary_writes.uses_identity_index_for_tests(),
            "catalog absence must not promote the ordered journal"
        );

        let schema_writes = test_staged_writes();
        let mut schema_row = tracked_append_row("schema-a", "schema");
        schema_row.schema_key = crate::transaction::normalization::REGISTERED_SCHEMA_KEY.into();
        schema_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![schema_row],
            })
            .expect("registered schema row should stage");
        assert!(
            schema_writes
                .has_staged_schema_catalog_change(&tracked_domain)
                .expect("ordered catalog marker probe should succeed")
        );
        assert!(
            !schema_writes.uses_identity_index_for_tests(),
            "catalog detection must preserve ordered journal storage"
        );
    }

    #[test]
    fn overlapping_tracked_batch_promotes_and_keeps_last_write() {
        let staged_writes = test_staged_writes();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    tracked_append_row("row-a", "first"),
                    tracked_append_row("row-b", "before"),
                ],
            })
            .expect("initial tracked batch should stage");
        assert!(!staged_writes.uses_identity_index_for_tests());

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    tracked_append_row("row-b", "after").with_change_id("row-b-after"),
                ],
            })
            .expect("overlapping write should use the indexed fallback");
        assert!(staged_writes.uses_identity_index_for_tests());

        let drained = staged_writes.drain().expect("writes should drain");
        assert_eq!(drained.state_rows.len(), 2);
        assert!(drained.state_rows.iter().any(|row| {
            row.row_pk == &RowPk::single("row-b")
                && row.snapshot.as_ref().map(|snapshot| snapshot.normalized())
                    == Some("{\"key\":\"row-b\",\"value\":\"after\"}")
        }));
        assert_eq!(
            drained
                .commit_change_refs_by_branch
                .get("01920000-0000-7000-8000-0000000000a1")
                .expect("branch commit refs should remain")
                .tracked_change_count,
            2
        );
    }

    #[test]
    fn tracked_append_order_uses_file_before_row() {
        let staged_writes = test_staged_writes();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    tracked_append_row("row-z", "first")
                        .with_file_id("01920000-0000-7000-8000-0000000000a2"),
                    tracked_append_row("row-a", "second")
                        .with_file_id("01920000-0000-7000-8000-0000000000b2"),
                ],
            })
            .expect("tracked-tree order should accept file-before-rows");
        assert!(
            !staged_writes.uses_identity_index_for_tests(),
            "the journal must use tracked-tree order, not exact-lookup identity order"
        );
    }

    #[tokio::test]
    async fn update_origin_rows_replace_staged_identity_under_outer_insert_mode() {
        let staged_writes = test_staged_writes();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("engine-owned-key", "first")],
            })
            .expect("initial internal identity should stage");

        let mut replacement = state_row("engine-owned-key", "second");
        replacement.origin = Some(TransactionWriteOrigin {
            surface: "plugin_reconciliation".into(),
            operation: TransactionWriteOperation::Update,
            primary_key: None,
        });
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: prepared_rows![replacement],
            })
            .expect("derived update row should replace under outer insert mode");

        let drained = staged_writes.drain().expect("drain should succeed");
        assert!(drained.insert_selection.is_empty());
        assert_eq!(drained.state_rows.len(), 1);
        assert_eq!(
            drained
                .state_rows
                .row(0)
                .snapshot
                .expect("replacement snapshot")
                .normalized(),
            "{\"key\":\"engine-owned-key\",\"value\":\"second\"}"
        );

        let normal_insert = test_staged_writes();
        normal_insert
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("public-key", "first")],
            })
            .expect("initial public identity should stage");
        let error = normal_insert
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: prepared_rows![state_row("public-key", "second")],
            })
            .expect_err("ordinary insert must still reject a staged duplicate");
        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[tokio::test]
    async fn staging_overlay_uses_last_staged_row_for_exact_load() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("sql2-duplicate-key", "first")],
            })
            .expect("initial row should stage");
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("sql2-duplicate-key", "second")],
            })
            .expect("staging rows should succeed");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged rows");
        let row = overlay
            .load_exact(&HotStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
                row_pk: RowPk::single("sql2-duplicate-key"),
                file_id: NullableKeyFilter::Null,
            })
            .expect("staged row should be visible");

        let StagedExactRow::Row(row) = row else {
            panic!("latest staged row should not be a tombstone");
        };
        assert_eq!(
            row.snapshot_content.as_deref(),
            Some("{\"key\":\"sql2-duplicate-key\",\"value\":\"second\"}")
        );
    }

    #[test]
    fn staging_overlay_exact_batch_is_correlated_aligned_and_tombstone_aware() {
        let staged_writes = test_staged_writes();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    state_row("row-a", "cross-pair")
                        .with_file_id("01920000-0000-7000-8000-0000000000b2"),
                    tombstone_row("deleted").with_file_id("deleted"),
                ],
            })
            .expect("rows should stage");
        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build");
        let exact = |row: &str, file_id: &str| HotStateExactRowRequest {
            schema_key: "lix_key_value".into(),
            branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".into(),
            row_pk: RowPk::single(row),
            file_id: Some(file_id.to_string()),
        };
        let cross_pair = exact("row-a", "01920000-0000-7000-8000-0000000000b2");
        let exact_request = HotStateExactBatchRequest {
            rows: vec![
                cross_pair.clone(),
                exact("row-a", "01920000-0000-7000-8000-0000000000a2"),
                exact("row-b", "01920000-0000-7000-8000-0000000000b2"),
                cross_pair,
                exact("missing", "missing"),
                exact("deleted", "deleted"),
            ],
            ..Default::default()
        };
        let batch = StagedHotStateRows::load_exact_batch(&overlay, &exact_request)
            .expect("exact staged batch should load directly");
        let first = batch.row(0).expect("first exact row");
        let duplicate = batch.row(3).expect("duplicate exact row");
        assert_eq!(first.row_pk(), duplicate.row_pk());
        assert!(std::ptr::eq(first.schema_key(), duplicate.schema_key()));
        assert!(batch.row(1).is_none());
        assert!(batch.row(2).is_none());
        assert!(batch.row(4).is_none());
        assert!(batch.row(5).is_none());

        let rows = StagedHotStateRows::load_exact_batch(&overlay, &exact_request)
            .expect("exact staged rows should load")
            .into_rows();

        assert!(rows[0].is_some());
        assert_eq!(rows[0], rows[3]);
        assert_eq!(rows[1], None);
        assert_eq!(rows[2], None);
        assert_eq!(rows[4], None);
        assert_eq!(rows[5], None, "tombstone should be hidden by default");

        let tombstone = StagedHotStateRows::load_exact_batch(
            &overlay,
            &HotStateExactBatchRequest {
                rows: vec![exact("deleted", "deleted")],
                include_tombstones: true,
                ..Default::default()
            },
        )
        .expect("exact staged tombstone should load")
        .into_rows()
        .pop()
        .flatten()
        .expect("tombstone should be returned");
        assert!(tombstone.deleted);
    }

    #[tokio::test]
    async fn staging_overlay_scan_returns_only_latest_row_per_identity() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("sql2-duplicate-key", "first")],
            })
            .expect("initial row should stage");
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("sql2-duplicate-key", "second")],
            })
            .expect("staging rows should succeed");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged rows");
        let rows = overlay
            .scan(&scan_request_for_key("sql2-duplicate-key", false))
            .expect("overlay scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"sql2-duplicate-key\",\"value\":\"second\"}")
        );
    }

    #[tokio::test]
    async fn staging_overlay_delete_hides_prior_staged_insert() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    state_row("sql2-delete-key", "visible"),
                    tombstone_row("sql2-delete-key"),
                ],
            })
            .expect("staging rows should succeed");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged rows");
        let exact = overlay
            .load_exact(&exact_request_for_key("sql2-delete-key"))
            .expect("staged tombstone should answer exact load");
        assert!(matches!(exact, StagedExactRow::Tombstone));
        assert!(
            overlay
                .scan(&scan_request_for_key("sql2-delete-key", false))
                .expect("overlay scan should succeed")
                .is_empty()
        );

        let tombstones = overlay
            .scan(&scan_request_for_key("sql2-delete-key", true))
            .expect("overlay scan should succeed");
        assert_eq!(tombstones.len(), 1);
        assert_eq!(tombstones[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn staging_overlay_insert_after_delete_resurrects_row() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    tombstone_row("sql2-resurrect-key"),
                    state_row("sql2-resurrect-key", "visible-again"),
                ],
            })
            .expect("staging rows should succeed");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged rows");
        let exact = overlay
            .load_exact(&exact_request_for_key("sql2-resurrect-key"))
            .expect("staged row should answer exact load");

        let StagedExactRow::Row(row) = exact else {
            panic!("latest staged row should be visible");
        };
        assert_eq!(
            row.snapshot_content.as_deref(),
            Some("{\"key\":\"sql2-resurrect-key\",\"value\":\"visible-again\"}")
        );
        assert_eq!(
            overlay
                .scan(&scan_request_for_key("sql2-resurrect-key", false))
                .expect("overlay scan should succeed")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn staged_writes_drain_returns_coalesced_latest_rows() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    state_row("sql2-key-a", "first"),
                    state_row("sql2-key-b", "only"),
                ],
            })
            .expect("initial rows should stage");
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("sql2-key-a", "second")],
            })
            .expect("staging rows should succeed");

        let drained = staged_writes.drain().expect("drain should succeed");

        assert_eq!(drained.state_rows.len(), 2);
        assert!(drained.state_rows.iter().any(|row| {
            row.row_pk == &RowPk::single("sql2-key-a")
                && row.snapshot.as_ref().map(|snapshot| snapshot.normalized())
                    == Some("{\"key\":\"sql2-key-a\",\"value\":\"second\"}")
        }));
        assert!(drained.state_rows.iter().any(|row| {
            row.row_pk == &RowPk::single("sql2-key-b")
                && row.snapshot.as_ref().map(|snapshot| snapshot.normalized())
                    == Some("{\"key\":\"sql2-key-b\",\"value\":\"only\"}")
        }));
    }

    #[tokio::test]
    async fn coalesced_replacement_preserves_prior_validation_requirement() {
        let staged_writes = test_staged_writes();
        let mut first = prepared_rows![state_row("validation-key", "constraint-change")];
        first.set_requires_transaction_validation(0, true);
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: first,
            })
            .expect("constraint-changing row should stage");
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("validation-key", "unrelated-change")],
            })
            .expect("later replacement should stage");

        let drained = staged_writes.drain().expect("drain should succeed");
        assert_eq!(drained.state_rows.len(), 1);
        let row = drained.state_rows.row(0);
        assert!(
            row.facts.requires_transaction_validation,
            "a later neutral replacement must not erase an earlier validation requirement"
        );
    }

    #[tokio::test]
    async fn staged_writes_drain_preserves_file_content_payloads() {
        let staged_writes = test_staged_writes();
        let result: crate::Blob = b"hello".as_slice().into();
        let provenance = crate::common::RequestBlobSpliceProvenance::new_validated_for_test(
            b"heo",
            &result,
            2,
            1,
            b"ll".to_vec(),
        );
        let mut file_content = TransactionFileContent::new(
            "01920000-0000-7000-8000-0000000000d2".to_string(),
            Some("/readme.md".to_string()),
            Some("readme.md".to_string()),
            "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
            true,
            true,
            result,
        );
        file_content.set_splice_provenance(Some(provenance.clone()));

        let rows = prepared_rows![state_row(
            "01920000-0000-7000-8000-0000000000d2",
            "descriptor",
        )];
        let input_rows_allocation = rows.slot_allocation_ptr() as usize;
        staged_writes
            .stage_write(PreparedTransactionWrite::RowsWithFileContent {
                mode: TransactionWriteMode::Replace,
                rows,
                file_content: vec![file_content],
                count: 1,
            })
            .expect("staging rows with file data should succeed");

        let drained = staged_writes.drain().expect("drain should succeed");

        assert_eq!(drained.state_rows.len(), 1);
        assert_eq!(
            drained.state_rows.slot_allocation_ptr() as usize,
            input_rows_allocation,
            "indexed file writes must retain one prepared-row allocation through staging and drain"
        );
        assert_eq!(drained.file_content_writes.len(), 1);
        assert_eq!(
            drained.file_content_writes[0].file_id,
            "01920000-0000-7000-8000-0000000000d2"
        );
        assert_eq!(drained.file_content_writes[0].content(), b"hello");
        assert_eq!(
            drained.file_content_writes[0].splice_provenance(),
            Some(&provenance)
        );
    }

    #[test]
    fn fresh_tracked_file_batch_reorders_once_and_stays_unindexed() {
        let staged_writes = test_staged_writes();
        let rows = prepared_rows![
            tracked_append_row("row-c", "third"),
            tracked_append_row("row-a", "first"),
            tracked_append_row("row-b", "second"),
        ];
        let input_rows_allocation = rows.slot_allocation_ptr() as usize;
        let file_content = TransactionFileContent::new(
            "01920000-0000-7000-8000-0000000000a2".to_string(),
            Some("/batch.json".to_string()),
            Some("batch.json".to_string()),
            "01920000-0000-7000-8000-0000000000a1".to_string(),
            false,
            false,
            b"payload".to_vec(),
        );

        staged_writes
            .stage_write(PreparedTransactionWrite::RowsWithFileContent {
                mode: TransactionWriteMode::Replace,
                rows,
                file_content: vec![file_content],
                count: 1,
            })
            .expect("fresh tracked file batch should stage");

        assert!(
            !staged_writes.uses_identity_index_for_tests(),
            "a one-shot unique file batch must keep transaction indexes lazy"
        );
        let drained = staged_writes.drain().expect("file batch should drain");
        assert_eq!(
            drained.state_rows.slot_allocation_ptr() as usize,
            input_rows_allocation,
            "in-place ordering must retain the prepared-row allocation"
        );
        assert_eq!(
            drained
                .state_rows
                .iter()
                .map(|row| row.row_pk.as_single_string().unwrap())
                .collect::<Vec<_>>(),
            ["row-a", "row-b", "row-c"]
        );
        let refs = drained
            .commit_change_refs_by_branch
            .get("01920000-0000-7000-8000-0000000000a1")
            .expect("tracked file batch should have commit refs");
        assert_eq!(refs.tracked_change_count, 3);
        assert!(
            drained
                .state_rows
                .iter()
                .all(|row| row.commit_id == Some(refs.commit_id))
        );
        assert_eq!(drained.file_content_writes.len(), 1);
    }

    #[test]
    fn cross_row_file_batch_keeps_source_order_in_the_generic_lane() {
        let staged_writes = test_staged_writes();
        let mut first = tracked_append_row("row-z", "first-source-row");
        first.facts.requires_transaction_validation = true;
        let mut second = tracked_append_row("row-a", "second-source-row");
        second.facts.requires_transaction_validation = true;
        let file_content = TransactionFileContent::new(
            "01920000-0000-7000-8000-0000000000a2".to_string(),
            Some("/batch.json".to_string()),
            Some("batch.json".to_string()),
            "01920000-0000-7000-8000-0000000000a1".to_string(),
            false,
            false,
            b"payload".to_vec(),
        );

        staged_writes
            .stage_write(PreparedTransactionWrite::RowsWithFileContent {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![first, second],
                file_content: vec![file_content],
                count: 1,
            })
            .expect("cross-row file batch should stage through the generic lane");
        assert!(
            staged_writes.uses_identity_index_for_tests(),
            "transaction-wide validation rows must not enter the sorted file fast path"
        );
        let drained = staged_writes.drain().expect("file batch should drain");
        assert_eq!(
            drained
                .state_rows
                .iter()
                .map(|row| row.row_pk.as_single_string().unwrap())
                .collect::<Vec<_>>(),
            ["row-z", "row-a"],
            "commit validation must observe source/cursor order"
        );
    }

    #[test]
    fn file_content_lane_coalesces_repeated_identity_for_reads_and_drain() {
        let staged_writes = test_staged_writes();
        let file_content = TransactionFileContent::new(
            "resurrected-file".to_string(),
            Some("/resurrected.json".to_string()),
            Some("resurrected.json".to_string()),
            "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
            true,
            true,
            b"payload".to_vec(),
        );

        staged_writes
            .stage_write(PreparedTransactionWrite::RowsWithFileContent {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    tombstone_row("resurrected-file"),
                    state_row("resurrected-file", "latest"),
                ],
                file_content: vec![file_content],
                count: 1,
            })
            .expect("file-data replacement sequence should stage");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build");
        let StagedExactRow::Row(visible) = overlay
            .load_exact(&exact_request_for_key("resurrected-file"))
            .expect("latest replacement should be visible")
        else {
            panic!("latest replacement must supersede its tombstone");
        };
        assert_eq!(
            visible.snapshot_content.as_deref(),
            Some("{\"key\":\"resurrected-file\",\"value\":\"latest\"}")
        );

        let drained = staged_writes.drain().expect("write should drain");
        assert_eq!(
            drained.state_rows.len(),
            1,
            "same-identity rows must coalesce before durable lowering"
        );
        assert_eq!(
            drained
                .state_rows
                .row(0)
                .snapshot
                .map(StageJson::normalized),
            Some("{\"key\":\"resurrected-file\",\"value\":\"latest\"}")
        );
        assert_eq!(drained.file_content_writes.len(), 1);
        assert_eq!(drained.file_content_writes[0].content(), b"payload");
    }

    #[test]
    fn staged_file_byte_lookup_filters_main_and_auxiliary_payloads_before_copying() {
        let staged_writes = test_staged_writes();
        let mut requested_write = TransactionFileContent::new(
            "requested-file".to_string(),
            Some("/requested.bin".to_string()),
            Some("requested.bin".to_string()),
            "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
            true,
            true,
            b"requested-main".to_vec(),
        );
        requested_write.add_auxiliary_payload(b"requested-auxiliary".to_vec());
        let unrelated_write = TransactionFileContent::new(
            "unrelated-file".to_string(),
            Some("/unrelated.bin".to_string()),
            Some("unrelated.bin".to_string()),
            "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
            true,
            true,
            b"unrelated-main".to_vec(),
        );
        staged_writes
            .stage_write(PreparedTransactionWrite::RowsWithFileContent {
                mode: TransactionWriteMode::Replace,
                rows: PreparedStateBatch::new(),
                file_content: vec![unrelated_write, requested_write],
                count: 2,
            })
            .expect("file payloads should stage");

        let auxiliary_hash = BlobId::from_content(b"requested-auxiliary");
        let missing_hash = BlobId::from_content(b"missing");
        let main_hash = BlobId::from_content(b"requested-main");
        let loaded = staged_writes
            .load_staged_file_bytes_many(&[auxiliary_hash, missing_hash, main_hash, auxiliary_hash])
            .expect("staged payload lookup should succeed")
            .into_vec();

        assert_eq!(
            loaded,
            vec![
                Some(b"requested-auxiliary".to_vec()),
                None,
                Some(b"requested-main".to_vec()),
                Some(b"requested-auxiliary".to_vec()),
            ]
        );
    }

    #[tokio::test]
    async fn staged_writes_track_commit_members_for_tracked_global_rows() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("tracked-key", "value").with_tracked()],
            })
            .expect("tracked global row should stage");

        let drained = staged_writes.drain().expect("drain should succeed");
        let change_refs = drained
            .commit_change_refs_by_branch
            .get("ffffffff-ffff-7fff-bfff-ffffffffffff")
            .expect("global commit change_refs should exist");
        assert_eq!(change_refs.tracked_change_count, 1);
    }

    #[tokio::test]
    async fn staged_writes_do_not_track_untracked_rows_as_commit_members() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("untracked-key", "value")],
            })
            .expect("untracked row should stage");

        let drained = staged_writes.drain().expect("drain should succeed");
        assert!(drained.commit_change_refs_by_branch.is_empty());
    }

    #[tokio::test]
    async fn staged_writes_replace_commit_member_on_tracked_overwrite() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    state_row("overwrite-key", "first")
                        .with_tracked()
                        .with_change_id("change-first"),
                ],
            })
            .expect("initial tracked row should stage");
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    state_row("overwrite-key", "second")
                        .with_tracked()
                        .with_change_id("change-second"),
                ],
            })
            .expect("tracked overwrite should stage");

        let drained = staged_writes.drain().expect("drain should succeed");
        let change_refs = drained
            .commit_change_refs_by_branch
            .get("ffffffff-ffff-7fff-bfff-ffffffffffff")
            .expect("global commit change_refs should exist");
        assert_eq!(change_refs.tracked_change_count, 1);
    }

    #[tokio::test]
    async fn staged_writes_reject_mixed_durability_in_one_batch() {
        let staged_writes = test_staged_writes();

        let error = staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    state_row("tracked-to-untracked-key", "tracked")
                        .with_tracked()
                        .with_change_id("change-tracked"),
                    state_row("tracked-to-untracked-key", "untracked")
                        .with_change_id("change-untracked"),
                ],
            })
            .expect_err("mixed durability should be rejected");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(
            error
                .message
                .contains("cannot mix tracked and untracked writes")
        );
        assert!(staged_writes.drain().unwrap().state_rows.is_empty());
    }

    #[tokio::test]
    async fn staged_writes_reject_duplicate_present_rows_in_one_batch() {
        let staged_writes = test_staged_writes();

        let error = staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    state_row("duplicate-present-key", "first"),
                    state_row("duplicate-present-key", "second"),
                ],
            })
            .expect_err("same-batch duplicate present rows should fail");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error.message.contains("primary-key constraint violation"),
            "error should explain the duplicate primary key: {error:?}"
        );
    }

    #[test]
    fn batch_identity_validation_preserves_nonadjacent_transition_order() {
        let rows = prepared_rows![
            state_row("shared", "first"),
            state_row("other", "other"),
            tombstone_row("shared"),
            state_row("shared", "replacement"),
        ];
        let identities = rows
            .iter()
            .map(PreparedStateRowIdentity::from)
            .collect::<Vec<_>>();

        assert!(
            !validate_batch_row_identities(&rows, &identities)
                .expect("present, tombstone, present should remain a valid replacement sequence"),
            "repeated identity should select the generic coalescing lane"
        );
    }

    #[test]
    fn batch_identity_validation_rejects_nonadjacent_duplicate_present_rows() {
        let rows = prepared_rows![
            state_row("shared", "first"),
            state_row("other", "other"),
            state_row("shared", "duplicate"),
        ];
        let identities = rows
            .iter()
            .map(PreparedStateRowIdentity::from)
            .collect::<Vec<_>>();

        let error = validate_batch_row_identities(&rows, &identities)
            .expect_err("nonadjacent duplicate present rows must fail");
        assert_eq!(error.code, LixError::CODE_UNIQUE);
    }

    #[test]
    fn batch_identity_validation_preserves_source_order_error_precedence() {
        let rows = prepared_rows![
            state_row("z", "tracked")
                .with_tracked()
                .with_change_id("z-tracked"),
            state_row("z", "untracked").with_change_id("z-untracked"),
            state_row("a", "first"),
            state_row("a", "duplicate"),
        ];
        let identities = rows
            .iter()
            .map(PreparedStateRowIdentity::from)
            .collect::<Vec<_>>();

        let error = validate_batch_row_identities(&rows, &identities)
            .expect_err("the earliest source-order violation must win");
        assert_eq!(
            error.code,
            LixError::CODE_INVALID_PARAM,
            "the row-1 durability error precedes the row-3 duplicate even though 'a' sorts first"
        );
        assert!(error.message.contains("cannot mix tracked and untracked"));
    }

    #[tokio::test]
    async fn staged_writes_reject_mixed_durability_across_calls() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("shared-domain-key", "tracked").with_tracked()],
            })
            .expect("tracked row should stage");
        let error = staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("shared-domain-key", "untracked")],
            })
            .expect_err("durability switch should fail");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        let drained = staged_writes.drain().expect("drain should succeed");
        assert_eq!(drained.state_rows.len(), 1);
        assert!(!drained.state_rows.row(0).untracked);
    }

    #[tokio::test]
    async fn same_durability_replacement_keeps_only_the_latest_row() {
        let staged_writes = test_staged_writes();
        for row in [
            state_row("alternating-key", "tracked-first").with_tracked(),
            state_row("alternating-key", "tracked-final").with_tracked(),
        ] {
            staged_writes
                .stage_write(PreparedTransactionWrite::Rows {
                    mode: TransactionWriteMode::Replace,
                    rows: prepared_rows![row],
                })
                .expect("alternating row should stage");
        }

        let drained = staged_writes.drain().expect("drain should succeed");
        assert_eq!(drained.state_rows.len(), 1);
        assert!(!drained.state_rows.row(0).untracked);
        assert_eq!(
            drained
                .state_rows
                .row(0)
                .snapshot
                .map(StageJson::materialize_shared)
                .as_deref(),
            Some("{\"key\":\"alternating-key\",\"value\":\"tracked-final\"}")
        );
    }

    #[tokio::test]
    async fn staged_writes_track_active_branch_members_separately() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    state_row("active-branch-key", "value")
                        .with_tracked()
                        .with_branch("01920000-0000-7000-8000-0000000000a1"),
                ],
            })
            .expect("active-branch tracked staging should accumulate change_refs");

        let drained = staged_writes.drain().expect("drain should succeed");
        let change_refs = drained
            .commit_change_refs_by_branch
            .get("01920000-0000-7000-8000-0000000000a1")
            .expect("active-branch commit change_refs should exist");
        assert_eq!(change_refs.tracked_change_count, 1);
    }

    #[tokio::test]
    async fn staged_writes_reject_global_rows_with_non_global_branch_id() {
        let staged_writes = test_staged_writes();

        let error = staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![{
                    let mut row = state_row("invalid-global-key", "value");
                    row.branch_id = "01920000-0000-7000-8000-0000000000a1".into();
                    row
                }],
            })
            .expect_err("global row with non-global branch should fail");

        assert!(
            error
                .message
                .contains("global staged rows must use the global branch id")
        );
    }

    #[tokio::test]
    async fn staging_overlay_identity_matches_hot_state_conflict_key() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("shared-row", "base")],
            })
            .expect("initial same-identity row should stage");
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    state_row("shared-row", "latest"),
                    state_row("shared-row", "other-branch")
                        .with_branch("01920000-0000-7000-8000-0000000000b1"),
                    state_row("shared-row", "other-schema").with_schema("other_schema"),
                    state_row("shared-row", "other-file")
                        .with_file_id("01920000-0000-7000-8000-0000000000a2"),
                ],
            })
            .expect("staging rows should succeed");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged rows");
        let rows = overlay
            .scan(&HotStateScanRequest {
                filter: HotStateFilter {
                    row_pks: vec![RowPk::single("shared-row")],
                    include_tombstones: true,
                    ..HotStateFilter::default()
                },
                ..HotStateScanRequest::default()
            })
            .expect("overlay scan should succeed");

        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows.iter()
                .filter(|row| row.row_pk == RowPk::single("shared-row")
                    && row.branch_id.as_ref() == "ffffffff-ffff-7fff-bfff-ffffffffffff"
                    && row.schema_key == "lix_key_value"
                    && row.file_id.is_none())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn staging_overlay_keyed_scan_keeps_branch_and_file_candidates_for_final_matching() {
        let staged_writes = test_staged_writes();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    state_row("selected", "ffffffff-ffff-7fff-bfff-ffffffffffff"),
                    state_row("other", "other-row"),
                    state_row("selected", "active")
                        .with_branch("01920000-0000-7000-8000-0000000000a1"),
                    state_row("selected", "other-file")
                        .with_file_id("01920000-0000-7000-8000-0000000000a2"),
                    state_row("selected", "other-schema").with_schema("other_schema"),
                ],
            })
            .expect("rows should stage");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged rows");
        let rows = overlay
            .scan_parts(&HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    row_pks: vec![RowPk::single("selected")],
                    branch_ids: vec!["01920000-0000-7000-8000-0000000000a1".to_string()],
                    file_ids: vec![NullableKeyFilter::Null],
                    include_tombstones: true,
                    ..HotStateFilter::default()
                },
                ..HotStateScanRequest::default()
            })
            .expect("keyed scan should succeed")
            .rows;

        assert_eq!(
            rows.len(),
            2,
            "active and global fallback both remain candidates"
        );
        assert!(rows.iter().all(|row| {
            row.schema_key == "lix_key_value"
                && row.row_pk == RowPk::single("selected")
                && row.file_id.is_none()
                && matches!(
                    row.branch_id.as_ref(),
                    "01920000-0000-7000-8000-0000000000a1" | GLOBAL_BRANCH_ID
                )
        }));
    }

    #[tokio::test]
    async fn staging_overlay_file_scan_uses_file_candidates_before_final_matching() {
        let staged_writes = test_staged_writes();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    state_row("global", "selected").with_file_id("selected-file"),
                    state_row("active", "selected")
                        .with_file_id("selected-file")
                        .with_branch("01920000-0000-7000-8000-0000000000a1"),
                    state_row("other-file", "ignored")
                        .with_file_id("01920000-0000-7000-8000-0000000000a2"),
                    state_row("other-schema", "ignored")
                        .with_schema("other_schema")
                        .with_file_id("selected-file"),
                ],
            })
            .expect("rows should stage");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged rows");
        let rows = overlay
            .scan_parts(&HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    branch_ids: vec!["01920000-0000-7000-8000-0000000000a1".to_string()],
                    file_ids: vec![NullableKeyFilter::Value("selected-file".to_string())],
                    include_tombstones: true,
                    ..HotStateFilter::default()
                },
                ..HotStateScanRequest::default()
            })
            .expect("file scan should succeed")
            .rows;

        assert_eq!(
            rows.len(),
            2,
            "active and global fallback both remain candidates"
        );
        assert!(rows.iter().all(|row| {
            row.schema_key == "lix_key_value"
                && row.file_id.as_deref() == Some("selected-file")
                && matches!(
                    row.branch_id.as_ref(),
                    "01920000-0000-7000-8000-0000000000a1" | GLOBAL_BRANCH_ID
                )
        }));
    }

    #[test]
    fn keyed_staged_scan_deduplicates_multi_key_candidates_by_slot_order() {
        let mut index = StagedScanCandidateIndex::default();
        index.insert(state_row("first", "one").borrowed(), RowSlot::State(4));
        index.insert(state_row("second", "two").borrowed(), RowSlot::State(9));

        let filter = HotStateFilter {
            schema_keys: vec!["lix_key_value".to_string(), "lix_key_value".to_string()],
            row_pks: vec![
                RowPk::single("second"),
                RowPk::single("first"),
                RowPk::single("first"),
            ],
            ..HotStateFilter::default()
        };
        let Some(Cow::Owned(slots)) = index.slots_for_filter(&filter) else {
            panic!("multi-key filter should use the candidate index");
        };

        assert_eq!(slots, vec![RowSlot::State(4), RowSlot::State(9)]);
    }

    #[test]
    fn file_staged_scan_deduplicates_multi_key_candidates_by_slot_order() {
        let mut index = StagedScanCandidateIndex::default();
        index.insert(
            state_row("first", "one")
                .with_file_id("selected")
                .borrowed(),
            RowSlot::State(4),
        );
        index.insert(
            state_row("second", "two").with_file_id("other").borrowed(),
            RowSlot::State(9),
        );

        let filter = HotStateFilter {
            schema_keys: vec!["lix_key_value".to_string(), "lix_key_value".to_string()],
            file_ids: vec![
                NullableKeyFilter::Value("other".to_string()),
                NullableKeyFilter::Value("selected".to_string()),
                NullableKeyFilter::Value("selected".to_string()),
            ],
            ..HotStateFilter::default()
        };
        let Some(Cow::Owned(slots)) = index.slots_for_filter(&filter) else {
            panic!("multi-key file filter should use the candidate index");
        };

        assert_eq!(slots, vec![RowSlot::State(4), RowSlot::State(9)]);
    }

    #[test]
    fn file_staged_scan_indexes_null_and_uses_schema_candidates_for_any_filter() {
        let mut index = StagedScanCandidateIndex::default();
        index.insert(state_row("null", "one").borrowed(), RowSlot::State(4));
        index.insert(
            state_row("file", "two").with_file_id("selected").borrowed(),
            RowSlot::State(9),
        );

        let null_filter = HotStateFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            file_ids: vec![NullableKeyFilter::Null],
            ..HotStateFilter::default()
        };
        let Some(Cow::Borrowed(null_slots)) = index.slots_for_filter(&null_filter) else {
            panic!("single null file filter should borrow indexed candidates");
        };
        assert_eq!(null_slots, &[RowSlot::State(4)]);

        let any_filter = HotStateFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            file_ids: vec![NullableKeyFilter::Any],
            ..HotStateFilter::default()
        };
        let Some(Cow::Borrowed(any_slots)) = index.slots_for_filter(&any_filter) else {
            panic!("an Any file filter should use schema candidates");
        };
        assert_eq!(
            any_slots,
            &[RowSlot::State(4), RowSlot::State(9)],
            "the established matcher still applies the Any file predicate"
        );
    }

    #[test]
    fn schema_staged_scan_deduplicates_multi_schema_candidates_by_slot_order() {
        let mut index = StagedScanCandidateIndex::default();
        index.insert(state_row("first", "one").borrowed(), RowSlot::State(4));
        index.insert(
            state_row("second", "two")
                .with_schema("other_schema")
                .borrowed(),
            RowSlot::State(9),
        );

        let filter = HotStateFilter {
            schema_keys: vec![
                "other_schema".to_string(),
                "lix_key_value".to_string(),
                "lix_key_value".to_string(),
            ],
            ..HotStateFilter::default()
        };
        let Some(Cow::Owned(slots)) = index.slots_for_filter(&filter) else {
            panic!("multi-schema filter should use the candidate index");
        };

        assert_eq!(slots, vec![RowSlot::State(4), RowSlot::State(9)]);
    }

    #[test]
    fn keyed_staged_scan_keeps_the_common_single_slot_inline() {
        let row = state_row("single", "value");
        let mut index = StagedScanCandidateIndex::default();
        index.insert(row.borrowed(), RowSlot::State(7));

        let slots = index
            .slots_by_schema_and_row
            .get(row.schema_key.as_str())
            .and_then(|by_row| by_row.get(&row.row_pk))
            .expect("indexed row should retain its candidate slot");
        assert_eq!(slots.as_slice(), &[RowSlot::State(7)]);
        assert!(
            !slots.spilled(),
            "one exact-read candidate must not allocate a row-local slot buffer"
        );

        let file_slots = index
            .slots_by_schema_and_file
            .get(row.schema_key.as_str())
            .and_then(|by_file| by_file.null_slots.first().map(|_| &by_file.null_slots))
            .expect("indexed null file should retain its candidate slot");
        assert_eq!(file_slots.as_slice(), &[RowSlot::State(7)]);
        assert!(
            !file_slots.spilled(),
            "one file candidate must not allocate a row-local slot buffer"
        );
    }

    #[test]
    fn keyed_staged_scan_keeps_reused_slot_after_promotion_and_tombstone() {
        let staged_writes = test_staged_writes();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    tracked_append_row("selected", "initial"),
                    tracked_append_row("other", "other"),
                ],
            })
            .expect("initial append-only rows should stage");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged rows");
        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec!["lix_key_value".to_string()],
                row_pks: vec![RowPk::single("selected")],
                branch_ids: vec!["01920000-0000-7000-8000-0000000000a1".to_string()],
                file_ids: vec![NullableKeyFilter::Null],
                include_tombstones: true,
                ..HotStateFilter::default()
            },
            ..HotStateScanRequest::default()
        };
        overlay
            .scan_parts(&request)
            .expect("keyed scan should promote the journal");
        assert!(staged_writes.uses_identity_index_for_tests());

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    tracked_append_row("selected", "replacement")
                        .with_change_id("selected-replacement"),
                ],
            })
            .expect("replacement should reuse the indexed slot");
        let mut tombstone =
            tracked_append_row("selected", "deleted").with_change_id("selected-tombstone");
        tombstone.snapshot = None;
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![tombstone],
            })
            .expect("tombstone should reuse the indexed slot");

        let rows = overlay
            .scan_parts(&request)
            .expect("keyed scan should retain the reused slot")
            .rows;
        assert_eq!(rows.len(), 1);
        assert!(rows[0].deleted);
        assert_eq!(
            rows[0].branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
    }

    #[tokio::test]
    async fn staged_writes_use_injected_function_provider_for_commit_metadata() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("sql2-functions-key", "value").with_tracked()],
            })
            .expect("staging rows should succeed");

        let drained = staged_writes.drain().expect("drain should succeed");
        let change_refs = drained
            .commit_change_refs_by_branch
            .get("ffffffff-ffff-7fff-bfff-ffffffffffff")
            .expect("global commit change_refs should exist");
        assert_eq!(change_refs.commit_id, test_commit_id(1));
        assert_eq!(
            change_refs.created_at.to_string(),
            "2026-01-01T00:00:00.001Z"
        );
    }

    #[tokio::test]
    async fn staged_writes_stamp_tracked_rows_with_commit_id_during_staging() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("tracked-commit-key", "value").with_tracked()],
            })
            .expect("tracked row should stage");

        let drained = staged_writes.drain().expect("drain should succeed");
        assert_eq!(drained.state_rows.len(), 1);
        assert_eq!(drained.state_rows.row(0).commit_id, Some(test_commit_id(1)));
        assert_eq!(
            drained
                .commit_change_refs_by_branch
                .get("ffffffff-ffff-7fff-bfff-ffffffffffff")
                .expect("global commit change_refs should exist")
                .commit_id,
            test_commit_id(1)
        );
    }

    #[expect(trivial_casts)]
    fn test_staged_writes() -> Arc<TransactionWriteBuffer> {
        Arc::new(TransactionWriteBuffer::new(FunctionProviderHandle::shared(
            Box::new(TestFunctionProvider::default()) as Box<dyn FunctionProvider + Send>,
        )))
    }

    #[derive(Default)]
    struct TestFunctionProvider {
        uuid_count: usize,
        timestamp_count: usize,
    }

    impl FunctionProvider for TestFunctionProvider {
        fn uuid_v7(&mut self) -> uuid::Uuid {
            self.uuid_count += 1;
            test_uuid_value(self.uuid_count)
        }

        fn timestamp(&mut self) -> LixTimestamp {
            self.timestamp_count += 1;
            LixTimestamp::expect_parse(
                "timestamp",
                &format!("2026-01-01T00:00:00.{:03}Z", self.timestamp_count),
            )
        }
    }

    fn test_uuid_value(index: usize) -> uuid::Uuid {
        uuid::Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0000 + index as u128)
    }

    fn test_commit_id(index: usize) -> CommitId {
        CommitId::with_change_address_space(test_uuid_value(index))
    }

    fn state_row(key: &str, value: &str) -> TestPreparedStateRow {
        let snapshot = stage_json_from_value(
            TransactionJson::from_value_for_test(serde_json::json!({ "key": key, "value": value })),
            "test staged row snapshot_content",
        )
        .expect("test snapshot should prepare");
        TestPreparedStateRow {
            schema_plan_id: SchemaPlanId::for_test(0),
            facts: PreparedRowFacts::default(),
            row_pk: RowPk::single(key),
            schema_key: "lix_key_value".into(),
            file_id: None,
            snapshot: Some(snapshot),
            metadata: None,
            origin: None,
            origin_key: None,
            created_at: LixTimestamp::expect_parse("created_at", "2026-01-01T00:00:00.000Z"),
            updated_at: LixTimestamp::expect_parse("updated_at", "2026-01-01T00:00:00.000Z"),
            global: true,
            change_id: None,
            commit_id: None,
            untracked: true,
            branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".into(),
        }
    }

    fn tombstone_row(key: &str) -> TestPreparedStateRow {
        let mut row = state_row(key, "deleted");
        row.snapshot = None;
        row
    }

    fn exact_request_for_key(key: &str) -> HotStateRowRequest {
        HotStateRowRequest {
            schema_key: "lix_key_value".to_string(),
            branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
            row_pk: RowPk::single(key),
            file_id: NullableKeyFilter::Null,
        }
    }

    fn exact_request_for_branch_key(branch_id: &str, key: &str) -> HotStateRowRequest {
        HotStateRowRequest {
            schema_key: "lix_key_value".to_string(),
            branch_id: branch_id.to_string(),
            row_pk: RowPk::single(key),
            file_id: NullableKeyFilter::Null,
        }
    }

    fn tracked_append_row(key: &str, value: &str) -> TestPreparedStateRow {
        state_row(key, value)
            .with_tracked()
            .with_branch("01920000-0000-7000-8000-0000000000a1")
            .with_change_id(&format!("append-{key}"))
    }

    #[test]
    fn ten_thousand_inserts_use_one_dense_buffer_and_no_identity_copies() {
        const ROW_COUNT: usize = 10_000;
        let staged_writes = test_staged_writes();
        let mut rows = PreparedStateBatch::with_capacity(ROW_COUNT);
        for row_index in 0..ROW_COUNT {
            let key = format!("row-{row_index:05}");
            rows.push_test_row(tracked_append_row(&key, "value"));
        }
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows,
            })
            .expect("dense ordered INSERT batch should stage");

        assert!(
            !staged_writes.uses_identity_index_for_tests(),
            "the 10k INSERT happy path must not build a row-identity hash table"
        );
        let drained = staged_writes
            .drain()
            .expect("10k INSERT batch should drain");
        assert_eq!(drained.insert_selection.len(), ROW_COUNT);
        assert_eq!(drained.insert_selection.large_buffer_count(), 1);
        assert_eq!(drained.insert_selection.identity_copy_count(), 0);
        assert!(
            (0..ROW_COUNT).all(|row_index| drained.insert_selection.contains(row_index)),
            "the dense bitmap must preserve every INSERT ordinal"
        );
    }

    fn scan_request_for_key(key: &str, include_tombstones: bool) -> HotStateScanRequest {
        HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec!["lix_key_value".to_string()],
                row_pks: vec![RowPk::single(key)],
                branch_ids: vec!["ffffffff-ffff-7fff-bfff-ffffffffffff".to_string()],
                file_ids: vec![NullableKeyFilter::Null],
                include_tombstones,
                ..HotStateFilter::default()
            },
            ..HotStateScanRequest::default()
        }
    }

    trait StateRowTestExt {
        fn with_schema(self, schema_key: &str) -> Self;
        fn with_file_id(self, file_id: &str) -> Self;
        fn with_tracked(self) -> Self;
        fn with_branch(self, branch_id: &str) -> Self;
        fn with_change_id(self, change_id: &str) -> Self;
    }

    impl StateRowTestExt for TestPreparedStateRow {
        fn with_schema(mut self, schema_key: &str) -> Self {
            self.schema_key = schema_key.into();
            self
        }

        fn with_file_id(mut self, file_id: &str) -> Self {
            self.file_id = Some(file_id.into());
            self
        }

        fn with_tracked(mut self) -> Self {
            self.untracked = false;
            if self.change_id.is_none() {
                self.change_id = Some(ChangeId::for_test_label("test-change-id"));
            }
            self
        }

        fn with_branch(mut self, branch_id: &str) -> Self {
            self.branch_id = branch_id.into();
            self.global = branch_id == GLOBAL_BRANCH_ID;
            self
        }

        fn with_change_id(mut self, change_id: &str) -> Self {
            self.change_id = Some(ChangeId::for_test_label(change_id));
            self
        }
    }
}
