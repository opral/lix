#![allow(
    clippy::cloned_instead_of_copied,
    clippy::large_enum_variant,
    clippy::option_as_ref_cloned,
    clippy::option_if_let_else,
    clippy::ref_option,
    clippy::unnecessary_wraps,
    clippy::unused_self
)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::binary_cas::BlobId;
use crate::catalog::SchemaPlanId;
use crate::changelog::{ChangeId, CommitId};
use crate::common::{LixTimestamp, SharedStr};
use crate::domain::{Domain, DomainRowIdentity};
use crate::entity_pk::EntityPk;
use crate::forktree::{ObjectId, StateCell, StateKey, StateValue};
#[cfg(test)]
use crate::functions::FunctionProvider;
use crate::functions::FunctionProviderHandle;
use crate::gc::CheckpointPublication;
use crate::state::{CertifiedStatePredecessor, StagedStateRow};
#[cfg(test)]
use crate::state::{ForkTreeStateView, TransactionStateView};
use crate::transaction::types::StagedCommitChangeRefs;
use crate::transaction::types::{
    CertifiedParameterReplacementBatch, CertifiedRawWriteBatchPreparation,
    CompleteCollectionReplacementProof, LogicalPrimaryKey, PreparedRowFacts, PreparedStateBatch,
    PreparedStateRowRef, PreparedTransactionWrite, StageJson, StagedCommitChangeBatch,
    TransactionFileContent, TransactionJson, TransactionWriteMode, TransactionWriteOperation,
    TransactionWriteOrigin, TransactionWriteOutcome,
};
#[cfg(test)]
use crate::transaction::types::{TestPreparedStateRow, stage_json_from_value};

/// Transaction-local write buffer after transaction-boundary preparation.
///
/// This is the engine seam between SQL execution and transaction ownership:
/// write frontends pass one typed `RawWriteBatch` to `Transaction`, the
/// transaction prepares it into a stable `PreparedStateBatch`, and commit
/// drains the same owner. Read visibility is owned by `TransactionStateView`.
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
    branch_ref_intents: Mutex<Vec<BranchRefPublicationIntent>>,
    historical_blob_manifest_edges: Mutex<HistoricalBlobManifestEdges>,
}

/// Authenticated manifest edges carried by a payload-free historical state
/// transition. The branch discriminator is part of the key so a global row
/// cannot be accidentally reused for a branch-local publication.
pub(crate) type HistoricalBlobManifestEdges = BTreeMap<(String, StateKey), Vec<ObjectId>>;

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
    branch_ref_intents: Vec<BranchRefPublicationIntent>,
    historical_blob_manifest_edges: HistoricalBlobManifestEdges,
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
    identity_arena: Bytes,
    identity_offsets: Arc<[(u32, u32)]>,
    snapshot_arena: Bytes,
    snapshot_offsets: Arc<[(u32, u32)]>,
    durable_predecessors: Option<Arc<[CertifiedStatePredecessor]>>,
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
            && self.timestamp == other.timestamp
            && self.durable_predecessors.as_ref().map(|values| {
                values
                    .iter()
                    .map(CertifiedStatePredecessor::created_at)
                    .collect::<Result<Vec<_>, _>>()
                    .ok()
            }) == other.durable_predecessors.as_ref().map(|values| {
                values
                    .iter()
                    .map(CertifiedStatePredecessor::created_at)
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
        durable_predecessors: Option<Vec<CertifiedStatePredecessor>>,
        timestamp: LixTimestamp,
    ) -> Result<Self, LixError> {
        if identity_offsets.len() != snapshot_offsets.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable mutation identity arena is misaligned",
            ));
        }
        let identity_bytes = Bytes::from(identity_arena);
        let mut previous_end = 0usize;
        let mut offsets = Vec::with_capacity(identity_offsets.len());
        let mut previous_identity = None;
        for (start, end) in identity_offsets {
            if start != previous_end || end < start || end > identity_bytes.len() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation identity offsets are invalid",
                ));
            }
            let value = std::str::from_utf8(&identity_bytes[start..end]).map_err(|_| {
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
        if previous_end != identity_bytes.len() {
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
            identity_bytes,
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
        identity_arena: Bytes,
        identity_offsets: Arc<[(u32, u32)]>,
        snapshot_arena: Vec<u8>,
        snapshot_offsets: Vec<(usize, usize)>,
        durable_predecessors: Option<Vec<CertifiedStatePredecessor>>,
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
        let arena_len = snapshot_arena.len();
        std::str::from_utf8(&snapshot_arena).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable mutation journal arena is not UTF-8",
            )
        })?;
        let mut previous_end = 0usize;
        let mut offsets = Vec::with_capacity(snapshot_offsets.len());
        for (start, end) in snapshot_offsets {
            if start != previous_end || end <= start || end > arena_len {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable mutation journal offsets are invalid",
                ));
            }
            std::str::from_utf8(&snapshot_arena[start..end]).map_err(|_| {
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
        Ok(Self {
            schema_plan_id,
            schema_key,
            branch_id,
            origin_key,
            identity_arena,
            identity_offsets,
            snapshot_arena: Bytes::from(snapshot_arena),
            snapshot_offsets: offsets.into(),
            durable_predecessors: durable_predecessors.map(Into::into),
            timestamp,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.identity_offsets.len()
    }

    pub(crate) fn materialized_entity_pks(&self) -> Arc<[EntityPk]> {
        self.identity_offsets
            .iter()
            .map(|&(start, end)| {
                let value = std::str::from_utf8(&self.identity_arena[start as usize..end as usize])
                    .expect("validated immutable mutation identity UTF-8");
                let value = SharedStr::from_utf8_slice(self.identity_arena.clone(), value)
                    .expect("validated immutable mutation identity remains in its arena");
                EntityPk::from_validated_shared_string(value)
            })
            .collect::<Vec<_>>()
            .into()
    }

    pub(crate) fn attach_durable_predecessors(
        &mut self,
        predecessors: Vec<CertifiedStatePredecessor>,
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
        std::str::from_utf8(&self.identity_arena[start as usize..end as usize])
            .expect("validated immutable mutation identity UTF-8")
    }

    pub(crate) fn snapshot(&self, index: usize) -> &str {
        let (start, end) = self.snapshot_offsets[index];
        std::str::from_utf8(&self.snapshot_arena[start as usize..end as usize])
            .expect("validated immutable mutation journal UTF-8")
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
        let entity_pks = self.materialized_entity_pks();
        let offsets = self
            .snapshot_offsets
            .iter()
            .map(|&(start, end)| (start as usize, end as usize))
            .collect();
        let mut rows = CertifiedParameterReplacementBatch::new(
            entity_pks.iter().cloned().collect(),
            TransactionJson::from_certified_row_content_arena(
                self.snapshot_arena.to_vec(),
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

/// Cheap read-only identity projection used to bulk-hydrate provisional
/// predecessor evidence before generic lowering.
#[derive(Clone)]
pub(crate) struct ProvisionalMutationJournalDescriptor {
    schema_key: SharedStr,
    branch_id: SharedStr,
    entity_pk_chunks: Vec<Arc<[EntityPk]>>,
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

    pub(crate) fn entity_pk_chunks(&self) -> &[Arc<[EntityPk]>] {
        &self.entity_pk_chunks
    }

    pub(crate) fn predecessors_complete(&self) -> bool {
        self.predecessors_complete
    }
}

impl OrderedMutationJournal {
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

    pub(crate) fn into_prepared(self) -> Result<PreparedStateBatch, LixError> {
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
        for index in 0..rows.len() {
            let ordinal = u32::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(1))
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "immutable mutation journal change ordinal exceeds u32",
                    )
                })?;
            let change_id =
                ChangeId::for_commit_ordinal(self.commit_id, ordinal).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "immutable mutation journal commit has no packed change address space",
                    )
                })?;
            rows.set_change_id(index, Some(change_id));
        }
        if let Some(created_at) = self.overlay_uniform_created_at {
            rows.set_created_at_all(created_at);
        }
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

/// The normal write path is an ordered journal. It becomes an indexed overlay
/// only if a later write overlaps it or a transaction-local read actually
/// needs read-your-writes semantics.
#[derive(Clone)]
enum StagedPreparedRows {
    AppendOnly {
        rows: PreparedStateBatch,
        insert_selection: PreparedInsertSelection,
        last_key: Option<StateKey>,
    },
    Indexed {
        rows: PreparedStateBatch,
        insert_selection: PreparedInsertSelection,
        by_identity: HashMap<PreparedStateRowIdentity, RowSlot>,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PreparedStateRowIdentity {
    schema_key: SharedStr,
    entity_pk: EntityPk,
    file_id: Option<SharedStr>,
    branch_id: SharedStr,
}

impl PreparedStateRowIdentity {
    fn from_staged_row(row: PreparedStateRowRef<'_>) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            entity_pk: row.entity_pk.clone(),
            file_id: row.file_id.cloned(),
            branch_id: row.branch_id.clone(),
        }
    }
}

#[cfg(test)]
impl From<&TestPreparedStateRow> for PreparedStateRowIdentity {
    fn from(row: &TestPreparedStateRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            entity_pk: row.entity_pk.clone(),
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

/// Converts one prepared row into the native ForkTree state identity used for
/// append-order validation.
fn state_key_from_row(row: PreparedStateRowRef<'_>) -> StateKey {
    StateKey {
        schema_key: row.schema_key.to_string(),
        file_id: row.file_id.map(ToString::to_string),
        entity_pk: row.entity_pk.clone(),
    }
}

fn staged_cell(snapshot: Option<&StageJson>) -> StateCell {
    match snapshot {
        None => StateCell::Tombstone,
        Some(snapshot) if snapshot.normalized() == "null" => StateCell::Null,
        Some(snapshot) => StateCell::Value(snapshot.normalized().into()),
    }
}

fn state_value_from_prepared(row: PreparedStateRowRef<'_>) -> StateValue {
    StateValue {
        change_id: row.change_id.unwrap_or_default(),
        commit_id: row.commit_id.unwrap_or_default(),
        created_at: row.created_at,
        updated_at: row.updated_at,
        cell: staged_cell(row.snapshot),
        metadata: row.metadata.map(|metadata| metadata.normalized().into()),
        origin_key: row.origin_key.map(ToString::to_string),
        // File payload publication is represented by the staged filesystem
        // row and upload owner. The state overlay must not invent a second
        // BlobRef authority while projecting read-your-writes rows.
        blob_manifest_object_ids: Vec::new(),
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
    pub(crate) branch_ref_intents: Vec<BranchRefPublicationIntent>,
    pub(crate) historical_blob_manifest_edges: HistoricalBlobManifestEdges,
}

/// Transaction-local branch selector intent. This is deliberately not a
/// live-state row: selector publication consumes it after the caller-owned
/// coherent read is opened and lowers it into the same PreparedPublication.
#[derive(Clone, Debug)]
pub(crate) struct BranchRefPublicationIntent {
    pub(crate) branch_id: String,
    pub(crate) commit_id: Option<CommitId>,
    pub(crate) create: bool,
    pub(crate) change_id: ChangeId,
    pub(crate) updated_at: LixTimestamp,
}

#[derive(Clone)]
pub(crate) struct DrainedMutationJournalDescriptor {
    pub(crate) commit_id: CommitId,
    pub(crate) schema_key: String,
    pub(crate) branch_id: String,
    pub(crate) entity_pk_chunks: Vec<Arc<[EntityPk]>>,
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
/// schema, file, branch, or entity identity.
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
    pub(crate) fn entity_pk(&self) -> &EntityPk {
        match self {
            Self::State(row) => row.entity_pk,
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

    pub(crate) fn global(&self) -> bool {
        match self {
            Self::State(row) => row.global,
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
            self.entity_pk().clone(),
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
    /// Lowers only journals carrying a complete authenticated replacement
    /// proof. This remains a temporary consumer boundary while ForkTree's
    /// root-transition publisher consumes the immutable columns directly;
    /// partial journals still require durable predecessor hydration.
    pub(crate) fn lower_certified_ordered_mutation_journals(&mut self) -> Result<(), LixError> {
        let journals = self
            .commit_change_refs_by_branch
            .values_mut()
            .filter_map(|refs| {
                refs.take_ordered_mutation_journal()
                    .map(|journal| (refs.commit_id, journal))
            })
            .collect::<Vec<_>>();
        for (_commit_id, journal) in journals {
            if journal.replacement_proof.is_none() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "partial immutable mutation journal reached complete-root lowering",
                ));
            }
            let rows = Arc::try_unwrap(journal)
                .unwrap_or_else(|journal| (*journal).clone())
                .into_prepared()?;
            let previous_len = self.state_rows.len();
            let row_count = rows.len();
            self.state_rows.append(rows);
            self.insert_selection.resize_rows(previous_len);
            for _ in 0..row_count {
                self.insert_selection.push_not_insert();
            }
        }
        Ok(())
    }

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
                        entity_pk_chunks: journal
                            .chunks
                            .iter()
                            .map(ImmutableMutationJournalChunk::materialized_entity_pks)
                            .collect(),
                    })
            })
            .collect()
    }

    pub(crate) fn hydrate_and_lower_ordered_mutation_journals(
        &mut self,
        mut predecessors_by_commit: BTreeMap<CommitId, Vec<CertifiedStatePredecessor>>,
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
            || !other.branch_ref_intents.is_empty()
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
        for (key, edge) in other.historical_blob_manifest_edges {
            if let Some(previous) = self
                .historical_blob_manifest_edges
                .insert(key, edge.clone())
                && previous != edge
            {
                return Err(LixError::new(
                    LixError::CODE_TRANSACTION_CONFLICT,
                    "cohort members carry conflicting historical blob manifest edges",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn replace_reconciled_file_writes(
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
        for (key, edge) in replacement.historical_blob_manifest_edges {
            self.historical_blob_manifest_edges.insert(key, edge);
        }
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
            branch_ref_intents: Mutex::new(Vec::new()),
            historical_blob_manifest_edges: Mutex::new(BTreeMap::new()),
        }
    }

    /// Projects the live prepared-row owner into the native transaction read
    /// overlay. The projection is a short-lived snapshot of this buffer: it
    /// never opens storage, creates a committed-only view, or retains an
    /// index separate from `TransactionStateView`.
    pub(crate) fn state_overlay_rows(
        &self,
        active_branch_id: &str,
    ) -> Result<Vec<StagedStateRow>, LixError> {
        let rows = self.rows.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let rows = match &*rows {
            StagedPreparedRows::AppendOnly { rows, .. }
            | StagedPreparedRows::Indexed { rows, .. } => rows,
        };
        let mut tracked = Vec::<(Vec<u8>, bool, StagedStateRow)>::new();
        for row in rows.iter() {
            let is_global = row.global || row.branch_id.as_str() == GLOBAL_BRANCH_ID;
            let is_active = row.branch_id.as_str() == active_branch_id;
            if !is_global && !is_active {
                continue;
            }
            let key = StateKey {
                schema_key: row.schema_key.to_string(),
                file_id: row.file_id.map(ToString::to_string),
                entity_pk: row.entity_pk.clone(),
            };
            let encoded_key = crate::forktree::encode_state_key(crate::forktree::StateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            });
            if row.untracked {
                return Err(LixError::new(
                    LixError::CODE_UNSUPPORTED_SQL,
                    "untracked state is no longer supported",
                ));
            }
            let staged_key = encoded_key.clone();
            tracked.push((
                encoded_key,
                is_global,
                StagedStateRow::new(staged_key, state_value_from_prepared(row)),
            ));
        }

        tracked.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        let mut tracked_rows: Vec<StagedStateRow> = Vec::with_capacity(tracked.len());
        for (_, is_global, row) in tracked {
            if let Some(previous) = tracked_rows.last() {
                if previous.key == row.key {
                    // Local staged state masks a global staged value. Two
                    // writes in the same owner should already have been
                    // coalesced by the indexed buffer and are corruption.
                    if is_global {
                        continue;
                    }
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "duplicate staged tracked state identity",
                    ));
                }
            }
            tracked_rows.push(row);
        }

        Ok(tracked_rows)
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
        predecessors: Vec<CertifiedStatePredecessor>,
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
                entity_pk_chunks: journal
                    .chunks
                    .iter()
                    .map(ImmutableMutationJournalChunk::materialized_entity_pks)
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
        let last_key = rows.last().map(state_key_from_row);
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
        let has_file = rows.iter().any(|row| row.file_id.is_some());
        let invalid_row = rows
            .iter()
            .any(|row| row.untracked || row.global || row.branch_id.as_str() != branch_id);
        if !has_file || invalid_row {
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
                LixError::CODE_INTERNAL_ERROR,
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
                LixError::CODE_INTERNAL_ERROR,
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
            *last_key = Some(state_key_from_row(row));
        }
        if existing_rows.is_empty() {
            *existing_rows = rows;
        } else {
            existing_rows.append(rows);
        }
        Ok(AppendOnlyStage::Staged)
    }

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
                LixError::CODE_INTERNAL_ERROR,
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
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged commit change refs lock",
            )
        })?;
        reorder_rows_by_source_permutation(&mut rows, &mut order);
        let branch_id = rows.row(0).branch_id.clone();
        if !commit_change_refs.contains_key(branch_id.as_str()) {
            let timestamp = self.functions.call_timestamp();
            commit_change_refs.insert(
                branch_id.to_string(),
                StagedCommitChangeRefs::new(
                    CommitId::with_change_address_space(self.functions.call_uuid_v7()),
                    ChangeId::from(self.functions.call_uuid_v7()),
                    ChangeId::from(self.functions.call_uuid_v7()),
                    timestamp,
                ),
            );
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
        *last_key = rows.last().map(state_key_from_row);
        *existing_rows = rows;
        Ok(AppendOnlyStage::Staged)
    }

    pub(crate) fn drain(&self) -> Result<PreparedWriteSet, LixError> {
        let mut rows_guard = self.rows.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let mut ordered_guard = self.ordered_mutations.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire immutable transaction mutation journal",
            )
        })?;
        let mut file_guard = self.file_content_writes.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged file data lock",
            )
        })?;
        let mut refs_guard = self.commit_change_refs_by_branch.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged commit change refs lock",
            )
        })?;
        let mut parents_guard = self.extra_commit_parents_by_branch.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged extra commit parents lock",
            )
        })?;
        let mut intermediate_guard = self.intermediate_commits.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged intermediate commits lock",
            )
        })?;
        let mut first_parent_guard =
            self.first_commit_parent_override_by_branch
                .lock()
                .map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "failed to acquire transaction staged first commit parent overrides lock",
                    )
                })?;
        let mut publication_guard = self.checkpoint_publications.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged checkpoint publications lock",
            )
        })?;
        let mut branch_ref_guard = self.branch_ref_intents.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged branch selector intents lock",
            )
        })?;
        let mut historical_blob_manifest_guard =
            self.historical_blob_manifest_edges.lock().map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "failed to acquire transaction staged historical blob manifest lock",
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
        let ordered_replacement = std::mem::take(&mut *ordered_guard);
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
            let refs = refs_guard.get_mut(journal.branch_id()).ok_or_else(|| {
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
            commit_change_refs_by_branch: std::mem::take(&mut *refs_guard),
            first_commit_parent_override_by_branch: std::mem::take(&mut *first_parent_guard),
            checkpoint_publications: std::mem::take(&mut *publication_guard),
            extra_commit_parents_by_branch: std::mem::take(&mut *parents_guard),
            intermediate_commits: std::mem::take(&mut *intermediate_guard),
            file_content_writes: std::mem::take(&mut *file_guard),
            branch_ref_intents: std::mem::take(&mut *branch_ref_guard),
            historical_blob_manifest_edges: std::mem::take(&mut *historical_blob_manifest_guard),
        })
    }

    pub(crate) fn stage_branch_ref_intent(
        &self,
        intent: BranchRefPublicationIntent,
    ) -> Result<(), LixError> {
        self.branch_ref_intents
            .lock()
            .map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "failed to acquire transaction staged branch selector intents lock",
                )
            })?
            .push(intent);
        Ok(())
    }

    pub(crate) fn stage_historical_blob_manifest_edges(
        &self,
        edges: HistoricalBlobManifestEdges,
    ) -> Result<(), LixError> {
        let mut guard = self.historical_blob_manifest_edges.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged historical blob manifest lock",
            )
        })?;
        for (key, edge) in edges {
            if edge.is_empty() {
                continue;
            }
            if let Some(previous) = guard.insert(key, edge.clone())
                && previous != edge
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "conflicting authenticated historical blob manifest edges",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn add_checkpoint_publication(
        &self,
        publication: CheckpointPublication,
    ) -> Result<(), LixError> {
        self.checkpoint_publications
            .lock()
            .map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
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
                    LixError::CODE_INTERNAL_ERROR,
                    "failed to acquire transaction staged commit change refs lock",
                )
            })?
            .get(branch_id)
            .map(|refs| refs.commit_id))
    }

    pub(crate) fn set_first_commit_parent(
        &self,
        branch_id: String,
        parent_commit_id: CommitId,
    ) -> Result<(), LixError> {
        self.first_commit_parent_override_by_branch
            .lock()
            .map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "failed to acquire transaction staged first commit parent overrides lock",
                )
            })?
            .insert(branch_id, parent_commit_id);
        Ok(())
    }

    pub(crate) fn add_commit_parent(
        &self,
        branch_id: String,
        parent_commit_id: CommitId,
    ) -> Result<(), LixError> {
        let mut guard = self.extra_commit_parents_by_branch.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
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
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged commit change refs lock",
            )
        })?;
        let refs = guard.entry(branch_id).or_insert_with(|| {
            let timestamp = functions.call_timestamp();
            StagedCommitChangeRefs::new(
                CommitId::with_change_address_space(functions.call_uuid_v7()),
                ChangeId::from(functions.call_uuid_v7()),
                ChangeId::from(functions.call_uuid_v7()),
                timestamp,
            )
        });
        refs.allow_empty();
        refs.add_selected_change_batch(selected_changes);
        Ok(refs.commit_id.to_string())
    }

    pub(crate) fn stage_intermediate_commit(
        &self,
        branch_id: String,
        parent_commit_id: CommitId,
        selected_changes: StagedCommitChangeBatch,
    ) -> Result<CommitId, LixError> {
        let timestamp = self.functions.call_timestamp();
        let mut refs = StagedCommitChangeRefs::new(
            CommitId::with_change_address_space(self.functions.call_uuid_v7()),
            ChangeId::from(self.functions.call_uuid_v7()),
            ChangeId::from(self.functions.call_uuid_v7()),
            timestamp,
        );
        refs.allow_empty();
        refs.add_selected_change_batch(selected_changes);
        let commit_id = refs.commit_id;
        self.intermediate_commits
            .lock()
            .map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "failed to acquire transaction staged intermediate commits lock",
                )
            })?
            .push(StagedIntermediateCommit {
                branch_id,
                parent_commit_id,
                change_refs: refs,
            });
        Ok(commit_id)
    }

    pub(crate) fn stage_intermediate_rows(
        &self,
        commit_id: CommitId,
        mut batch: PreparedStateBatch,
    ) -> Result<(), LixError> {
        let mut intermediate_commits = self.intermediate_commits.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged intermediate commits lock",
            )
        })?;
        let commit = intermediate_commits
            .iter_mut()
            .find(|commit| commit.change_refs.commit_id == commit_id)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("unknown staged intermediate commit '{commit_id}'"),
                )
            })?;
        if batch
            .iter()
            .any(|row| row.branch_id.as_str() != commit.branch_id || row.untracked)
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "intermediate commit row has an incompatible branch or durability",
            ));
        }
        for index in 0..batch.len() {
            batch.set_commit_id(index, Some(commit_id));
        }
        let identities = batch
            .iter()
            .map(PreparedStateRowIdentity::from)
            .collect::<Vec<_>>();
        self.ensure_identity_index(true)?;
        let mut rows = self.rows.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let StagedPreparedRows::Indexed {
            rows,
            insert_selection,
            by_identity,
        } = &mut *rows
        else {
            unreachable!("intermediate row staging requires the identity index");
        };
        if identities
            .iter()
            .any(|identity| by_identity.contains_key(identity))
        {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                "intermediate checkpoint marker overlaps an existing staged row",
            ));
        }
        let start = rows.len();
        let count = batch.len();
        insert_selection.reserve_rows(count, false);
        for _ in 0..count {
            insert_selection.push_not_insert();
        }
        rows.append(batch);
        for (offset, identity) in identities.into_iter().enumerate() {
            by_identity.insert(identity, RowSlot::State(start + offset));
        }
        commit.change_refs.add_change_count(count);
        Ok(())
    }

    pub(crate) fn load_staged_file_bytes_for_owner(
        &self,
        branch_id: &str,
        file_id: &str,
        expected: BlobId,
    ) -> Result<Option<Vec<u8>>, LixError> {
        let writes = self.file_content_writes.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged file data lock",
            )
        })?;
        for write in writes.iter().rev() {
            if write.branch_id != branch_id || write.file_id != file_id {
                continue;
            }
            if let Some(bytes) = write.inline_data()
                && BlobId::from_canonical_content(bytes) == expected
            {
                return Ok(Some(bytes.to_vec()));
            }
        }
        Ok(None)
    }

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
        let (rows, file_content_writes) = Self::state_rows_from_stage_write(write);
        debug_assert!(file_content_writes.is_empty());
        self.reject_mixed_staged_retention(&rows)?;
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
        let (mut rows, file_content_writes) = Self::state_rows_from_stage_write(write);
        if let Some(indices) = &statement_indices {
            debug_assert_eq!(indices.len(), rows.len());
        }
        if rows.is_empty() {
            if !file_content_writes.is_empty() {
                self.file_content_writes
                    .lock()
                    .map_err(|_| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "failed to acquire transaction staged file data lock",
                        )
                    })?
                    .extend(file_content_writes);
            }
            return Ok(TransactionWriteOutcome { count });
        }
        self.reject_mixed_staged_retention(&rows)?;
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
                                LixError::CODE_INTERNAL_ERROR,
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
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let StagedPreparedRows::Indexed {
            rows: staged_rows,
            insert_selection,
            by_identity,
        } = &mut *guard
        else {
            unreachable!("generic staging must promote the identity index");
        };
        let mut refs = self.commit_change_refs_by_branch.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged commit change refs lock",
            )
        })?;
        for row in rows.iter() {
            if row.global && row.branch_id != GLOBAL_BRANCH_ID {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "global staged rows must use the global branch id",
                ));
            }
            if let Some(RowSlot::State(index)) = by_identity
                .get(&PreparedStateRowIdentity::from(row))
                .copied()
                && let Some(previous) = staged_rows.get(index)
                && previous.untracked != row.untracked
            {
                return Err(mixed_durability_error(row));
            }
        }
        let insert_count = rows.iter().filter(|row| row_is_insert(mode, *row)).count();
        if insert_count != 0 {
            let mut insert_order = rows
                .iter()
                .enumerate()
                .filter_map(|(index, row)| row_is_insert(mode, row).then_some(index))
                .collect::<Vec<_>>();
            insert_order.sort_unstable_by(|&left, &right| {
                identities[left]
                    .cmp(&identities[right])
                    .then(left.cmp(&right))
            });
            let duplicate_in_batch = insert_order
                .windows(2)
                .find(|pair| identities[pair[0]] == identities[pair[1]])
                .map(|pair| pair[1]);
            let duplicate_staged = insert_order
                .iter()
                .copied()
                .find(|&index| by_identity.contains_key(&identities[index]));
            if let Some(index) = duplicate_in_batch.into_iter().chain(duplicate_staged).min() {
                return Err(duplicate_insert_identity_error(rows.row(index)));
            }
        }
        if identities_are_unique && staged_rows.is_empty() && by_identity.is_empty() {
            insert_selection.reserve_rows(rows.len(), insert_count != 0);
            for index in 0..rows.len() {
                let row = rows.row(index);
                let commit_id = add_row_to_commit_change_refs(&mut refs, row, &self.functions);
                if row_is_insert(mode, row) {
                    insert_selection.push(
                        row.origin,
                        statement_indices
                            .as_ref()
                            .map(|indices| indices[index] as usize),
                    );
                } else {
                    insert_selection.push_not_insert();
                }
                by_identity.insert(PreparedStateRowIdentity::from(row), RowSlot::State(index));
                rows.set_commit_id(index, commit_id);
            }
            *staged_rows = rows;
            if !file_content_writes.is_empty() {
                self.file_content_writes
                    .lock()
                    .map_err(|_| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
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
        for (source_index, identity) in identities.into_iter().enumerate() {
            let row = rows.row(source_index);
            let is_insert = row_is_insert(mode, row);
            let existing_slot = by_identity.get(&identity).copied();
            let mut requires_validation = row.facts.requires_transaction_validation;
            if let Some(RowSlot::State(index)) = existing_slot {
                let previous = latest_incoming_source_by_destination
                    .get(&index)
                    .map_or_else(|| staged_rows.row(index), |source| rows.row(*source));
                requires_validation |= previous.facts.requires_transaction_validation;
                remove_row_from_commit_change_refs(&mut refs, previous);
            }
            if requires_validation != row.facts.requires_transaction_validation {
                rows.set_requires_transaction_validation(source_index, true);
            }
            let row = rows.row(source_index);
            let commit_id = add_row_to_commit_change_refs(&mut refs, row, &self.functions);
            let insert_metadata = is_insert.then_some((
                row.origin.cloned(),
                statement_indices
                    .as_ref()
                    .map(|indices| indices[source_index] as usize),
            ));
            rows.set_commit_id(source_index, commit_id);
            let destination = existing_slot.map_or_else(
                || {
                    let index = next_destination;
                    next_destination += 1;
                    index
                },
                |RowSlot::State(index)| index,
            );
            if let Some(metadata) = insert_metadata {
                inserted_destinations.push((destination, metadata));
            }
            placements.push((destination, source_index));
            latest_incoming_source_by_destination.insert(destination, source_index);
            by_identity.insert(identity, RowSlot::State(destination));
        }
        staged_rows.append(rows);
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
        if !file_content_writes.is_empty() {
            self.file_content_writes
                .lock()
                .map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "failed to acquire transaction staged file data lock",
                    )
                })?
                .extend(file_content_writes);
        }
        Ok(TransactionWriteOutcome { count })
    }

    /// Rejects a same-transaction identity switch before the append-only
    /// journal can bypass the indexed mixed-durability check.  The identity
    /// excludes retention by design; changing only `untracked` for the same
    /// state owner is a semantic conflict, not a second row.
    fn reject_mixed_staged_retention(&self, incoming: &PreparedStateBatch) -> Result<(), LixError> {
        let guard = self.rows.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let existing = match &*guard {
            StagedPreparedRows::AppendOnly { rows, .. }
            | StagedPreparedRows::Indexed { rows, .. } => rows,
        };
        for row in incoming.iter() {
            let identity = PreparedStateRowIdentity::from(row);
            if existing.iter().any(|previous| {
                PreparedStateRowIdentity::from(previous) == identity
                    && previous.untracked != row.untracked
            }) {
                return Err(mixed_durability_error(row));
            }
        }
        Ok(())
    }

    fn state_rows_from_stage_write(
        write: PreparedTransactionWrite,
    ) -> (PreparedStateBatch, Vec<TransactionFileContent>) {
        match write {
            PreparedTransactionWrite::Rows { rows, .. } => (rows, Vec::new()),
            PreparedTransactionWrite::RowsWithFileContent {
                rows, file_content, ..
            } => (rows, file_content),
        }
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
        let branch_ref_intents = self.branch_ref_intents.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged branch selector intents lock",
            )
        })?;
        let historical_blob_manifest_edges =
            self.historical_blob_manifest_edges.lock().map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "failed to acquire transaction staged historical blob manifest lock",
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
            branch_ref_intents: branch_ref_intents.clone(),
            historical_blob_manifest_edges: historical_blob_manifest_edges.clone(),
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
            branch_ref_intents,
            historical_blob_manifest_edges,
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
        let mut branch_ref_intents_guard = self.branch_ref_intents.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged branch selector intents lock",
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
        *branch_ref_intents_guard = branch_ref_intents;
        *self.historical_blob_manifest_edges.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged historical blob manifest lock",
            )
        })? = historical_blob_manifest_edges;
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
        let rows = match &*rows {
            StagedPreparedRows::AppendOnly { rows, .. }
            | StagedPreparedRows::Indexed { rows, .. } => rows,
        };
        let matches_domain = |row: PreparedStateRowRef<'_>| {
            row.schema_key.as_str() == registered_schema_key
                && row.branch_id.as_str() == domain.branch_id()
                && (row.untracked == domain.untracked() || (domain.untracked() && !row.untracked))
        };
        Ok(rows.iter().any(matches_domain))
    }

    pub(crate) fn has_staged_collection_rows(
        &self,
        branch_id: &str,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<bool, LixError> {
        let rows = self.rows.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let rows = match &*rows {
            StagedPreparedRows::AppendOnly { rows, .. }
            | StagedPreparedRows::Indexed { rows, .. } => rows,
        };
        Ok(rows.iter().any(|row| {
            row.branch_id.as_str() == branch_id
                && row.schema_key.as_str() == scope.schema_key
                && row.file_id.map(SharedStr::as_str) == scope.file_id
        }))
    }

    pub(crate) fn collection_replaced(
        &self,
        branch_id: &str,
        schema_key: &str,
        file_id: Option<&str>,
    ) -> Result<bool, LixError> {
        let rows = self.rows.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let rows = match &*rows {
            StagedPreparedRows::AppendOnly { rows, .. }
            | StagedPreparedRows::Indexed { rows, .. } => rows,
        };
        for row in rows.iter().filter(|row| {
            row.schema_key.as_str()
                == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
                && row.branch_id.as_str() == branch_id
                && row.snapshot.is_some()
        }) {
            let (target_schema_key, target_file_id) =
                crate::collection_generation::collection_scope_from_entity_pk(row.entity_pk)?;
            if target_schema_key == schema_key
                && (target_file_id.is_none() || target_file_id.as_deref() == file_id)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Promotes the compact ordered journal into the identity index when an
    /// irregular write needs exact staged-row ownership.
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
        for (index, row) in rows.iter().enumerate() {
            let identity = PreparedStateRowIdentity::from(row);
            let slot = RowSlot::State(index);
            let previous = by_identity.insert(identity, slot);
            debug_assert!(previous.is_none(), "append-only rows must be unique");
        }
        *guard = StagedPreparedRows::Indexed {
            rows,
            insert_selection,
            by_identity,
        };
        Ok(())
    }
}

#[cfg(test)]
mod staging_semantics_tests {
    use super::*;

    use crate::common::LixTimestamp;
    use crate::forktree::{
        ForkTreeReadFacade, StateCell, StateKey, StateKeyRef, StateValue,
        encode_state_entity_prefix, encode_state_key, exclusive_prefix_upper_bound,
    };
    use crate::storage::{Memory, MemoryRead};
    use crate::storage_adapter::{SharedStorageAdapterRead, StorageAdapter, StorageReadOptions};
    use crate::transaction::types::FileContent;

    type TestRead = SharedStorageAdapterRead<MemoryRead>;

    const TEST_BRANCH: &str = "01920000-0000-7000-8000-0000000000a1";
    const TEST_GLOBAL: &str = GLOBAL_BRANCH_ID;

    fn test_staged_writes() -> Arc<TransactionWriteBuffer> {
        let functions: Box<dyn FunctionProvider + Send> = Box::new(TestFunctionProvider::default());
        Arc::new(TransactionWriteBuffer::new(FunctionProviderHandle::shared(
            functions,
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
            TransactionJson::from_value_for_test(serde_json::json!({
                "key": key,
                "value": value
            })),
            "test staged row snapshot_content",
        )
        .expect("test snapshot should prepare");
        TestPreparedStateRow {
            schema_plan_id: SchemaPlanId::for_test(0),
            facts: PreparedRowFacts::default(),
            entity_pk: EntityPk::single(key),
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
            branch_id: TEST_GLOBAL.into(),
        }
    }

    fn tracked_append_row(key: &str, value: &str) -> TestPreparedStateRow {
        let mut row = state_row(key, value);
        row.untracked = false;
        row.global = false;
        row.branch_id = TEST_BRANCH.into();
        row.change_id = Some(ChangeId::for_test_label(key));
        row
    }

    fn tracked_tombstone_row(key: &str) -> TestPreparedStateRow {
        let mut row = tracked_append_row(key, "deleted");
        row.snapshot = None;
        row
    }

    fn untracked_branch_row(key: &str, value: &str) -> TestPreparedStateRow {
        let mut row = state_row(key, value);
        row.global = false;
        row.branch_id = TEST_BRANCH.into();
        row
    }

    fn prepared_rows(rows: impl IntoIterator<Item = TestPreparedStateRow>) -> PreparedStateBatch {
        PreparedStateBatch::from_test_rows(rows.into_iter().collect())
    }

    fn staged_values(set: &PreparedWriteSet) -> Vec<(String, Option<String>, bool, bool)> {
        set.state_rows
            .iter()
            .map(|row| {
                (
                    row.entity_pk
                        .as_single_string_owned()
                        .expect("test identity is one string"),
                    row.snapshot.map(|value| value.normalized().to_owned()),
                    row.untracked,
                    row.global,
                )
            })
            .collect()
    }

    fn native_state_value(cell: StateCell, ordinal: usize) -> StateValue {
        StateValue {
            change_id: ChangeId::for_test_label(&format!("state-change-{ordinal}")),
            commit_id: CommitId::for_test_label(&format!("state-commit-{ordinal}")),
            created_at: LixTimestamp::expect_parse("created_at", "2026-01-01T00:00:00.000Z"),
            updated_at: LixTimestamp::expect_parse("updated_at", "2026-01-01T00:00:00.001Z"),
            cell,
            metadata: None,
            origin_key: None,
            blob_manifest_object_ids: Vec::new(),
        }
    }

    fn native_key(entity: &str) -> Vec<u8> {
        let entity_pk = EntityPk::single(entity);
        encode_state_key(StateKeyRef {
            schema_key: "lix_key_value",
            file_id: None,
            entity_pk: &entity_pk,
        })
    }

    async fn empty_committed_view() -> ForkTreeStateView<TestRead> {
        let storage = StorageAdapter::new(Memory::new());
        crate::forktree::initialize_empty_repository(storage.clone())
            .await
            .expect("initialize the native in-memory repository");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open one retained native read");
        let read = SharedStorageAdapterRead::new(read);
        let committed = ForkTreeStateView::from_facade(ForkTreeReadFacade::new(read), TEST_GLOBAL)
            .await
            .expect("open the retained native branch view");
        committed
    }

    #[test]
    fn staged_replace_drains_ordered_rows_and_commit_membership() {
        let staged = test_staged_writes();
        staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([
                    tracked_append_row("a", "one"),
                    tracked_append_row("b", "two"),
                ]),
            })
            .expect("ordered replacement should stage");

        let drained = staged.drain().expect("staged rows should drain");
        assert_eq!(drained.state_rows.len(), 2);
        assert_eq!(
            drained
                .state_rows
                .row(0)
                .entity_pk
                .as_single_string_owned()
                .unwrap(),
            "a"
        );
        assert_eq!(
            drained
                .state_rows
                .row(1)
                .entity_pk
                .as_single_string_owned()
                .unwrap(),
            "b"
        );
        assert_eq!(
            drained
                .commit_change_refs_by_branch
                .get(TEST_BRANCH)
                .expect("tracked branch membership")
                .tracked_change_count,
            2
        );
        assert_eq!(drained.state_rows.row(0).commit_id, Some(test_commit_id(1)));
    }

    #[test]
    fn staged_insert_rejects_duplicate_identity_in_one_batch() {
        let staged = test_staged_writes();
        let error = staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: prepared_rows([
                    tracked_append_row("duplicate", "one"),
                    tracked_append_row("duplicate", "two"),
                ]),
            })
            .expect_err("duplicate INSERT identity must fail closed");
        assert!(error.to_string().contains("duplicate"));
        assert!(
            staged
                .drain()
                .expect("failed batch remains atomic")
                .state_rows
                .is_empty()
        );
    }

    #[test]
    fn staged_replacement_coalesces_to_the_latest_row() {
        let staged = test_staged_writes();
        staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([tracked_append_row("same", "before")]),
            })
            .expect("initial replacement should stage");
        staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([tracked_append_row("same", "after")]),
            })
            .expect("replacement should coalesce");

        let drained = staged.drain().expect("coalesced rows should drain");
        assert_eq!(drained.state_rows.len(), 1);
        assert_eq!(
            drained
                .state_rows
                .row(0)
                .snapshot
                .expect("snapshot")
                .normalized(),
            r#"{"key":"same","value":"after"}"#
        );
    }

    #[test]
    fn staged_replacement_preserves_prior_validation_requirement() {
        let staged = test_staged_writes();
        let mut prior = tracked_append_row("validated", "before");
        prior.facts.requires_transaction_validation = true;
        staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([prior]),
            })
            .expect("validated row should stage");
        staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([tracked_append_row("validated", "after")]),
            })
            .expect("replacement should stage");
        let drained = staged.drain().expect("replacement should drain");
        assert!(
            drained
                .state_rows
                .row(0)
                .facts
                .requires_transaction_validation
        );
    }

    #[test]
    fn staged_delete_then_insert_resurrects_the_latest_row() {
        let staged = test_staged_writes();
        staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([tracked_append_row("resurrect", "before")]),
            })
            .expect("initial row should stage");
        staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([tracked_tombstone_row("resurrect")]),
            })
            .expect("tombstone should stage");
        staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([tracked_append_row("resurrect", "after")]),
            })
            .expect("resurrection should stage");

        let drained = staged.drain().expect("resurrection should drain");
        assert_eq!(drained.state_rows.len(), 1);
        assert!(drained.state_rows.row(0).snapshot.is_some());
    }

    #[test]
    fn staged_checkpoint_restore_rolls_back_rows_and_commit_metadata() {
        let staged = test_staged_writes();
        staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([tracked_append_row("kept", "one")]),
            })
            .expect("first row should stage");
        let checkpoint = staged
            .checkpoint()
            .expect("checkpoint should capture all owners");
        staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([tracked_append_row("rolled-back", "two")]),
            })
            .expect("second row should stage");
        staged
            .restore(checkpoint)
            .expect("restore should be atomic");
        let drained = staged.drain().expect("restored rows should drain");
        assert_eq!(staged_values(&drained).len(), 1);
        assert_eq!(staged_values(&drained)[0].0, "kept");
        assert_eq!(drained.commit_change_refs_by_branch.len(), 1);
    }

    #[test]
    fn staged_mixed_durability_rejects_same_identity() {
        let staged = test_staged_writes();
        staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([tracked_append_row("durability", "tracked")]),
            })
            .expect("tracked row should stage");
        let error = staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([untracked_branch_row("durability", "untracked")]),
            })
            .expect_err("same identity cannot change durability");
        assert!(error.to_string().contains("durability"));
    }

    #[test]
    fn staged_file_content_survives_native_drain() {
        let staged = test_staged_writes();
        let row = tracked_append_row("file-row", "metadata").with_file_id("file-1");
        let content = TransactionFileContent::new(
            "file-1".to_string(),
            Some("file.txt".to_string()),
            Some("file.txt".to_string()),
            TEST_BRANCH.to_string(),
            false,
            false,
            FileContent::from(vec![1_u8, 2, 3]),
        );
        staged
            .stage_write(PreparedTransactionWrite::RowsWithFileContent {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([row]),
                file_content: vec![content],
                count: 1,
            })
            .expect("file row and payload should stage");
        let drained = staged.drain().expect("file write should drain");
        assert_eq!(drained.file_content_writes.len(), 1);
        assert_eq!(drained.file_content_writes[0].file_id, "file-1");
        assert_eq!(drained.file_content_writes[0].content().len(), 3);
    }

    #[test]
    fn staged_commit_membership_excludes_untracked_rows() {
        let staged = test_staged_writes();
        staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([state_row("untracked", "value")]),
            })
            .expect("untracked row should stage");
        let drained = staged.drain().expect("untracked row should drain");
        assert!(drained.commit_change_refs_by_branch.is_empty());
    }

    #[test]
    fn staged_global_rows_require_the_global_branch_identity() {
        let staged = test_staged_writes();
        let mut row = state_row("bad-global", "value");
        row.branch_id = TEST_BRANCH.into();
        let error = staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([row]),
            })
            .expect_err("global rows cannot use a local branch identity");
        assert!(error.to_string().contains("global"));
    }

    #[test]
    fn staged_parameter_insert_preserves_statement_slot_order() {
        let staged = test_staged_writes();
        staged
            .stage_parameter_batch_insert(
                PreparedTransactionWrite::Rows {
                    mode: TransactionWriteMode::Insert,
                    rows: prepared_rows([
                        tracked_append_row("slot-a", "a"),
                        tracked_append_row("slot-b", "b"),
                    ]),
                },
                vec![7, 3],
            )
            .expect("parameter INSERT should stage");
        let drained = staged.drain().expect("parameter INSERT should drain");
        assert_eq!(drained.insert_selection.len(), 2);
        assert_eq!(drained.insert_selection.statement_index(0), Some(7));
        assert_eq!(drained.insert_selection.statement_index(1), Some(3));
    }

    #[test]
    fn staged_schema_catalog_change_invalidates_the_matching_native_domain() {
        let staged = test_staged_writes();
        let mut row = state_row("schema-row", "schema");
        row.schema_key = crate::transaction::normalization::REGISTERED_SCHEMA_KEY.into();
        row.global = false;
        row.branch_id = TEST_BRANCH.into();
        staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([row]),
            })
            .expect("schema catalog row should stage");
        let domain = Domain::exact_file(TEST_BRANCH.to_string(), true, None);
        assert!(
            staged
                .has_staged_schema_catalog_change(&domain)
                .expect("schema catalog probe should succeed")
        );
    }

    #[test]
    fn failed_file_batch_restores_without_leaking_payload_or_rows() {
        let staged = test_staged_writes();
        staged
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([tracked_append_row("before-failure", "one")]),
            })
            .expect("baseline row should stage");
        let checkpoint = staged
            .checkpoint()
            .expect("checkpoint should capture payload owners");
        staged
            .stage_write(PreparedTransactionWrite::RowsWithFileContent {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows([tracked_append_row("failed-file", "two")]),
                file_content: vec![TransactionFileContent::new(
                    "failed-file".to_string(),
                    None,
                    None,
                    TEST_BRANCH.to_string(),
                    false,
                    false,
                    FileContent::from(vec![9_u8, 8, 7]),
                )],
                count: 1,
            })
            .expect("file staging itself is valid before simulated SQL failure");
        staged
            .restore(checkpoint)
            .expect("rollback should restore all owners");
        let drained = staged.drain().expect("restored baseline should drain");
        assert_eq!(drained.state_rows.len(), 1);
        assert!(drained.file_content_writes.is_empty());
    }

    #[tokio::test]
    async fn native_points_preserve_slots_duplicates_and_tombstones() {
        let committed = empty_committed_view().await;
        let key_a = native_key("a");
        let key_b = native_key("b");
        let view = TransactionStateView::new(
            committed,
            vec![
                StagedStateRow::new(
                    key_a.clone(),
                    native_state_value(StateCell::Value("a".into()), 1),
                ),
                StagedStateRow::new(key_b.clone(), native_state_value(StateCell::Tombstone, 2)),
            ],
        )
        .expect("native rows are ordered");

        let rows = view
            .points(&[key_b.clone(), key_a.clone(), key_b.clone()], true)
            .await
            .expect("native exact points should resolve");
        assert!(matches!(
            rows[0].as_ref().map(|row| &row.value.cell),
            Some(StateCell::Tombstone)
        ));
        assert!(matches!(
            rows[1].as_ref().map(|row| &row.value.cell),
            Some(StateCell::Value(_))
        ));
        assert!(matches!(
            rows[2].as_ref().map(|row| &row.value.cell),
            Some(StateCell::Tombstone)
        ));

        let hidden = view
            .points(&[key_b], false)
            .await
            .expect("tombstone omission should preserve slot");
        assert_eq!(hidden, vec![None]);
    }

    #[tokio::test]
    async fn native_range_applies_limit_after_tombstone_visibility() {
        let committed = empty_committed_view().await;
        let key_a = native_key("a");
        let key_b = native_key("b");
        let key_c = native_key("c");
        let view = TransactionStateView::new(
            committed,
            vec![
                StagedStateRow::new(key_a.clone(), native_state_value(StateCell::Tombstone, 1)),
                StagedStateRow::new(
                    key_b.clone(),
                    native_state_value(StateCell::Value("b".into()), 2),
                ),
                StagedStateRow::new(key_c, native_state_value(StateCell::Value("c".into()), 3)),
            ],
        )
        .expect("native rows are ordered");
        let rows = view
            .range(Some(&key_a), None, Some(1), false)
            .await
            .expect("native bounded range should resolve");
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0].value.cell, StateCell::Value(_)));
    }

    #[tokio::test]
    async fn native_batched_ranges_preserve_slots_order_and_staged_tombstones() {
        let committed = empty_committed_view().await;
        let key_a = native_key("a");
        let key_b = native_key("b");
        let key_c = native_key("c");
        let view = TransactionStateView::new(
            committed,
            vec![
                StagedStateRow::new(
                    key_a.clone(),
                    native_state_value(StateCell::Value("a".into()), 1),
                ),
                StagedStateRow::new(key_b.clone(), native_state_value(StateCell::Tombstone, 2)),
                StagedStateRow::new(
                    key_c.clone(),
                    native_state_value(StateCell::Value("c".into()), 3),
                ),
            ],
        )
        .expect("native rows are ordered");
        let ranges = vec![
            (
                key_a.clone(),
                crate::forktree::exclusive_prefix_upper_bound(&key_a),
            ),
            (key_b, crate::forktree::exclusive_prefix_upper_bound(&key_c)),
        ];
        let visible = view
            .branch_ranges(TEST_GLOBAL, &ranges, false)
            .await
            .expect("native batched ranges should resolve");
        assert_eq!(visible.len(), 2);
        assert_eq!(visible[0].len(), 1);
        assert_eq!(visible[1].len(), 1);
        assert_eq!(visible[1][0].key, key_c);

        let with_tombstones = view
            .branch_ranges(TEST_GLOBAL, &ranges, true)
            .await
            .expect("tombstone-inclusive native batched ranges should resolve");
        assert_eq!(with_tombstones[1].len(), 2);
        assert!(matches!(
            with_tombstones[1][0].value.cell,
            StateCell::Tombstone
        ));
    }

    #[cfg(any())]
    #[tokio::test]
    async fn native_staged_untracked_tombstone_preserves_owner_and_masks_slot() {
        let committed = empty_committed_view().await;
        let owner = crate::forktree::CanonicalBranchId::from_bytes(
            *uuid::Uuid::parse_str(TEST_GLOBAL)
                .expect("global branch UUID")
                .as_bytes(),
        );
        let key = StateKey {
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            entity_pk: EntityPk::single("untracked"),
        };
        let value = UntrackedValue {
            created_at: LixTimestamp::expect_parse("created_at", "2026-01-01T00:00:00.000Z"),
            updated_at: LixTimestamp::expect_parse("updated_at", "2026-01-01T00:00:00.001Z"),
            cell: StateCell::Tombstone,
            metadata: None,
            origin_key: None,
            blob_manifest_object_ids: Vec::new(),
        };
        let view = TransactionStateView::new_with_untracked(
            committed,
            Vec::new(),
            vec![StagedUntrackedStateRow::new(owner, key.clone(), value)],
        )
        .expect("native untracked overlay should be ordered");
        let encoded = encode_state_key(StateKeyRef {
            schema_key: &key.schema_key,
            file_id: key.file_id.as_deref(),
            entity_pk: &key.entity_pk,
        });
        assert_eq!(
            view.untracked_points(&[encoded.clone()], false)
                .await
                .unwrap(),
            vec![None]
        );
        let rows = view
            .untracked_points(&[encoded], true)
            .await
            .expect("tombstone-inclusive untracked point should resolve");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].as_ref().expect("owner retained").owner, owner);
    }

    #[cfg(any())]
    #[tokio::test]
    async fn native_staged_untracked_rows_use_canonical_key_order_across_owners() {
        let committed = empty_committed_view().await;
        let low = native_untracked_key("app.order", "a");
        let high = native_untracked_key("app.order", "b");
        let low_owner = crate::forktree::CanonicalBranchId::from_bytes([9; 16]);
        let high_owner = crate::forktree::CanonicalBranchId::from_bytes([1; 16]);
        let _view = TransactionStateView::new_with_untracked(
            committed,
            Vec::new(),
            vec![
                StagedUntrackedStateRow::new(
                    low_owner,
                    low,
                    native_untracked_value(StateCell::Value("low".into())),
                ),
                StagedUntrackedStateRow::new(
                    high_owner,
                    high,
                    native_untracked_value(StateCell::Value("high".into())),
                ),
            ],
        )
        .expect("canonical state-key order must not depend on owner prefix");
    }

    #[cfg(any())]
    #[tokio::test]
    async fn native_staged_untracked_exact_and_range_share_tombstone_visibility() {
        let committed = empty_committed_view().await;
        let owner = crate::forktree::CanonicalBranchId::from_bytes(
            *uuid::Uuid::parse_str(TEST_GLOBAL)
                .expect("global branch UUID")
                .as_bytes(),
        );
        let tombstone = native_untracked_key("app.parity", "a");
        let visible = native_untracked_key("app.parity", "b");
        let prefix = encode_state_entity_prefix(
            "app.parity",
            &EntityPk {
                components: crate::entity_pk::EntityPkComponents::Empty,
            },
        );
        let upper = exclusive_prefix_upper_bound(&prefix);
        let view = TransactionStateView::new_with_untracked(
            committed,
            Vec::new(),
            vec![
                StagedUntrackedStateRow::new(
                    owner,
                    tombstone.clone(),
                    native_untracked_value(StateCell::Tombstone),
                ),
                StagedUntrackedStateRow::new(
                    owner,
                    visible.clone(),
                    native_untracked_value(StateCell::Value("visible".into())),
                ),
            ],
        )
        .expect("staged parity rows should be canonical");
        let exact_keys = [
            encode_state_key(StateKeyRef {
                schema_key: &tombstone.schema_key,
                file_id: tombstone.file_id.as_deref(),
                entity_pk: &tombstone.entity_pk,
            }),
            encode_state_key(StateKeyRef {
                schema_key: &visible.schema_key,
                file_id: visible.file_id.as_deref(),
                entity_pk: &visible.entity_pk,
            }),
        ];
        let exact = view
            .untracked_points(&exact_keys, false)
            .await
            .expect("exact staged overlay");
        assert!(exact[0].is_none());
        assert!(exact[1].is_some());

        let range = view
            .untracked_overlay_branch_range_for_branch(
                TEST_GLOBAL,
                Some(&prefix),
                upper.as_deref(),
                Some(1),
                false,
            )
            .await
            .expect("range staged overlay");
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].key, visible);
    }

    #[cfg(any())]
    #[tokio::test]
    async fn native_staged_untracked_range_uses_canonical_schema_bounds() {
        let committed = empty_committed_view().await;
        let owner = CanonicalBranchId::from_bytes(
            *uuid::Uuid::parse_str(TEST_GLOBAL)
                .expect("global branch UUID")
                .as_bytes(),
        );
        let in_schema = native_untracked_key("app.range", "inside");
        let outside_schema = native_untracked_key("zzz.other", "outside");
        let prefix = encode_state_entity_prefix(
            "app.range",
            &EntityPk {
                components: crate::entity_pk::EntityPkComponents::Empty,
            },
        );
        let upper = exclusive_prefix_upper_bound(&prefix);
        let view = TransactionStateView::new_with_untracked(
            committed,
            Vec::new(),
            vec![
                StagedUntrackedStateRow::new(
                    owner,
                    in_schema.clone(),
                    native_untracked_value(StateCell::Value("inside".into())),
                ),
                StagedUntrackedStateRow::new(
                    owner,
                    outside_schema,
                    native_untracked_value(StateCell::Value("outside".into())),
                ),
            ],
        )
        .expect("staged rows are canonical-key ordered");

        let rows = view
            .untracked_branch_range(Some(&prefix), upper.as_deref(), None)
            .await
            .expect("canonical schema-prefix range should resolve");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, in_schema);
    }

    #[cfg(any())]
    #[tokio::test]
    async fn native_staged_untracked_range_filters_tombstones_before_limit() {
        let committed = empty_committed_view().await;
        let owner = CanonicalBranchId::from_bytes(
            *uuid::Uuid::parse_str(TEST_GLOBAL)
                .expect("global branch UUID")
                .as_bytes(),
        );
        let tombstone = native_untracked_key("app.tombstones", "a");
        let visible = native_untracked_key("app.tombstones", "b");
        let prefix = encode_state_entity_prefix(
            "app.tombstones",
            &EntityPk {
                components: crate::entity_pk::EntityPkComponents::Empty,
            },
        );
        let upper = exclusive_prefix_upper_bound(&prefix);
        let view = TransactionStateView::new_with_untracked(
            committed,
            Vec::new(),
            vec![
                StagedUntrackedStateRow::new(
                    owner,
                    tombstone,
                    native_untracked_value(StateCell::Tombstone),
                ),
                StagedUntrackedStateRow::new(
                    owner,
                    visible.clone(),
                    native_untracked_value(StateCell::Value("visible".into())),
                ),
            ],
        )
        .expect("staged rows are canonical-key ordered");

        let rows = view
            .untracked_overlay_branch_range_for_branch(
                TEST_GLOBAL,
                Some(&prefix),
                upper.as_deref(),
                Some(1),
                false,
            )
            .await
            .expect("tombstone-inclusive range should resolve before filtering");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, visible);
    }

    #[tokio::test]
    async fn native_overlay_rejects_unsorted_rows_without_materializing_a_map() {
        let committed = empty_committed_view().await;
        let result = TransactionStateView::new(
            committed,
            vec![
                StagedStateRow::new(native_key("b"), native_state_value(StateCell::Null, 1)),
                StagedStateRow::new(native_key("a"), native_state_value(StateCell::Null, 2)),
            ],
        );
        assert!(
            result.is_err(),
            "native overlay must require intrinsic order"
        );
    }
}

#[cfg(test)]
trait StagingTestRowExt {
    fn with_file_id(self, file_id: &str) -> Self;
}

#[cfg(test)]
impl StagingTestRowExt for TestPreparedStateRow {
    fn with_file_id(mut self, file_id: &str) -> Self {
        self.file_id = Some(file_id.into());
        self
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
    previous_last: Option<&StateKey>,
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
        .as_str()
        .cmp(right.schema_key.as_str())
        .then_with(|| {
            left.file_id
                .map(|value| value.as_str())
                .cmp(&right.file_id.map(SharedStr::as_str))
        })
        .then_with(|| left.entity_pk.cmp(right.entity_pk))
}

fn compare_tracked_key_to_row(
    left: &StateKey,
    right: PreparedStateRowRef<'_>,
) -> std::cmp::Ordering {
    left.schema_key
        .as_str()
        .cmp(right.schema_key.as_str())
        .then_with(|| {
            left.file_id
                .as_deref()
                .cmp(&right.file_id.map(SharedStr::as_str))
        })
        .then_with(|| left.entity_pk.cmp(right.entity_pk))
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
    let entity_pk = row
        .entity_pk
        .as_json_array_text()
        .unwrap_or_else(|_| "<invalid entity_pk>".to_string());
    LixError::new(
        LixError::CODE_INVALID_PARAM,
        format!(
            "cannot mix tracked and untracked writes for schema '{}' entity_pk '{}' in branch '{}' within one transaction; commit or roll back before changing durability",
            row.schema_key, entity_pk, row.branch_id
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
                "primary-key constraint violation on schema '{}': duplicate staged rows for entity_pk '{}' in branch '{}'",
                row.schema_key,
                previous
                    .entity_pk
                    .as_json_array_text()
                    .unwrap_or_else(|_| "<invalid entity_pk>".to_string()),
                row.branch_id
            )
        });
    LixError::new(LixError::CODE_UNIQUE, message)
}

pub(crate) fn duplicate_insert_identity_message(
    schema_key: &str,
    entity_pk: &EntityPk,
    branch_id: Option<&str>,
    origin: Option<&TransactionWriteOrigin>,
) -> String {
    if let Some(message) = logical_primary_key_violation_message(origin) {
        return message;
    }
    let entity_pk = entity_pk
        .as_json_array_text()
        .unwrap_or_else(|_| "<invalid entity_pk>".to_string());
    match branch_id {
        Some(branch_id) => format!(
            "primary-key constraint violation on schema '{schema_key}': INSERT would duplicate entity_pk '{entity_pk}' in branch '{branch_id}'"
        ),
        None => format!(
            "primary-key constraint violation on schema '{schema_key}': INSERT would duplicate entity_pk '{entity_pk}'"
        ),
    }
}

fn duplicate_insert_identity_error(row: PreparedStateRowRef<'_>) -> LixError {
    let message = duplicate_insert_identity_message(
        row.schema_key,
        row.entity_pk,
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

#[cfg(test)]
mod transaction_overlay_tests {
    use super::*;

    const ACTIVE_BRANCH_ID: &str = "01920000-0000-7000-8000-0000000000b1";

    fn prepared_row(
        branch_id: &str,
        schema_key: &str,
        value: Option<serde_json::Value>,
        global: bool,
        untracked: bool,
    ) -> TestPreparedStateRow {
        let timestamp = LixTimestamp::expect_parse("test timestamp", "2026-08-10T00:00:00.000Z");
        TestPreparedStateRow {
            schema_plan_id: SchemaPlanId::for_test(0),
            facts: PreparedRowFacts::default(),
            entity_pk: EntityPk::single("row"),
            schema_key: schema_key.into(),
            file_id: None,
            snapshot: value.map(|value| {
                stage_json_from_value(
                    TransactionJson::from_value_for_test(value),
                    "transaction overlay test",
                )
                .expect("test JSON should stage")
            }),
            metadata: None,
            origin: None,
            origin_key: None,
            created_at: timestamp,
            updated_at: timestamp,
            global,
            change_id: Some(ChangeId::for_test_label("staged-change")),
            commit_id: None,
            untracked,
            branch_id: branch_id.into(),
        }
    }

    fn buffer_with_rows(rows: Vec<TestPreparedStateRow>) -> TransactionWriteBuffer {
        let buffer = TransactionWriteBuffer::new(FunctionProviderHandle::system());
        *buffer
            .rows
            .lock()
            .expect("test staged rows lock should not be poisoned") =
            StagedPreparedRows::AppendOnly {
                rows: PreparedStateBatch::from_test_rows(rows),
                insert_selection: PreparedInsertSelection::new(),
                last_key: None,
            };
        buffer
    }

    #[test]
    fn state_overlay_rows_preserves_owner_and_tombstone_semantics() {
        let buffer = buffer_with_rows(vec![
            prepared_row(
                GLOBAL_BRANCH_ID,
                "tracked_schema",
                Some(serde_json::json!({"value": "global"})),
                true,
                false,
            ),
            prepared_row(
                ACTIVE_BRANCH_ID,
                "tracked_schema",
                Some(serde_json::json!({"value": "local"})),
                false,
                false,
            ),
            prepared_row(
                "01920000-0000-7000-8000-0000000000b2",
                "ignored_schema",
                Some(serde_json::json!({"value": "ignored"})),
                false,
                false,
            ),
        ]);

        let tracked = buffer
            .state_overlay_rows(ACTIVE_BRANCH_ID)
            .expect("native staging overlay should project");
        assert_eq!(tracked.len(), 1);

        let tracked_key = crate::forktree::decode_state_key(&tracked[0].key)
            .expect("tracked overlay key should be canonical");
        assert_eq!(tracked_key.schema_key, "tracked_schema");
        match &tracked[0].value.cell {
            StateCell::Value(value) => assert!(value.contains("local")),
            other => panic!("expected local tracked value, got {other:?}"),
        }
    }

    #[test]
    fn staged_collection_probe_is_scoped_to_branch_schema_and_file() {
        let buffer = buffer_with_rows(vec![prepared_row(
            ACTIVE_BRANCH_ID,
            "lix_collection_generation",
            Some(serde_json::json!({"live_count": 1})),
            false,
            false,
        )]);
        let scope = crate::collection_generation::CollectionScopeRef {
            schema_key: "lix_collection_generation",
            file_id: None,
        };
        assert!(
            buffer
                .has_staged_collection_rows(ACTIVE_BRANCH_ID, scope)
                .expect("collection probe should read staged rows")
        );
        assert!(
            !buffer
                .has_staged_collection_rows(GLOBAL_BRANCH_ID, scope)
                .expect("collection probe should preserve branch scope")
        );
    }
}
