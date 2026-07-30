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
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use smallvec::SmallVec;

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::{BlobBytesBatch, BlobHash};
use crate::catalog::SchemaPlanId;
use crate::changelog::{ChangeId, CommitId};
use crate::common::SharedStr;
use crate::domain::{Domain, DomainRowIdentity};
use crate::entity_pk::EntityPk;
#[cfg(test)]
use crate::functions::FunctionProvider;
use crate::functions::FunctionProviderHandle;
use crate::gc::CheckpointPublication;
#[cfg(test)]
use crate::live_state::LiveStateRowRequest;
#[cfg(test)]
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{
    LiveStateExactBatchRequest, LiveStateExactRowRequest, LiveStateScanRequest,
    MaterializedLiveStateBatch, MaterializedLiveStateBatchBuilder, MaterializedLiveStateExactBatch,
};
use crate::transaction::types::StagedCommitChangeRefs;
use crate::transaction::types::{
    LogicalPrimaryKey, PreparedStateBatch, PreparedStateRowRef, PreparedTransactionWrite,
    StageJson, StagedCommitChangeBatch, TransactionFileData, TransactionWriteMode,
    TransactionWriteOperation, TransactionWriteOrigin, TransactionWriteOutcome,
};
#[cfg(test)]
use crate::transaction::types::{TestPreparedStateRow, TransactionJson, stage_json_from_value};
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
    commit_change_refs_by_branch: Mutex<BTreeMap<String, StagedCommitChangeRefs>>,
    first_commit_parent_override_by_branch: Mutex<BTreeMap<String, CommitId>>,
    checkpoint_publications: Mutex<Vec<CheckpointPublication>>,
    extra_commit_parents_by_branch: Mutex<BTreeMap<String, Vec<CommitId>>>,
    intermediate_commits: Mutex<Vec<StagedIntermediateCommit>>,
    file_data_writes: Mutex<Vec<TransactionFileData>>,
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

#[derive(Default)]
struct StagedScanFileCandidates {
    slots_by_value: HashMap<SharedStr, SmallVec<[RowSlot; 1]>>,
    null_slots: SmallVec<[RowSlot; 1]>,
}

/// Narrows entity- or file-constrained scans over an indexed transaction
/// overlay without changing the journal's identity/coalescing semantics.
/// Branch and durability remain post-filter checks because one indexed
/// candidate can legitimately have multiple such physical rows while staged.
#[derive(Default)]
struct StagedScanCandidateIndex {
    slots_by_schema: HashMap<SharedStr, SmallVec<[RowSlot; 1]>>,
    slots_by_schema_and_entity: HashMap<SharedStr, HashMap<EntityPk, SmallVec<[RowSlot; 1]>>>,
    slots_by_schema_and_file: HashMap<SharedStr, StagedScanFileCandidates>,
}

impl StagedScanCandidateIndex {
    fn insert(&mut self, row: PreparedStateRowRef<'_>, slot: RowSlot) {
        self.slots_by_schema
            .entry(row.schema_key.clone())
            .or_default()
            .push(slot);
        self.slots_by_schema_and_entity
            .entry(row.schema_key.clone())
            .or_default()
            .entry(row.entity_pk.clone())
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
    /// schema plus either entity or file identity are constrained. The
    /// remaining scan filters are deliberately applied by the established
    /// matcher afterwards.
    fn slots_for_filter<'a>(
        &'a self,
        filter: &crate::live_state::LiveStateFilter,
    ) -> Option<Cow<'a, [RowSlot]>> {
        if filter.schema_keys.is_empty() {
            return None;
        }

        if !filter.entity_pks.is_empty() {
            if let ([schema_key], [entity_pk]) =
                (filter.schema_keys.as_slice(), filter.entity_pks.as_slice())
            {
                let slots = self
                    .slots_by_schema_and_entity
                    .get(schema_key.as_str())
                    .and_then(|by_entity| by_entity.get(entity_pk))
                    .map(SmallVec::as_slice)
                    .unwrap_or(&[]);
                return Some(Cow::Borrowed(slots));
            }

            let mut slots = Vec::new();
            for schema_key in &filter.schema_keys {
                let Some(by_entity) = self.slots_by_schema_and_entity.get(schema_key.as_str())
                else {
                    continue;
                };
                for entity_pk in &filter.entity_pks {
                    if let Some(candidate_slots) = by_entity.get(entity_pk) {
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
/// from [`PreparedStateRowIdentity`], whose entity-first order is for exact
/// lookup rather than bulk root construction.
#[derive(Clone)]
struct TrackedStateKey {
    schema_key: SharedStr,
    file_id: Option<SharedStr>,
    entity_pk: EntityPk,
}

impl TrackedStateKey {
    fn from_row(row: PreparedStateRowRef<'_>) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.cloned(),
            entity_pk: row.entity_pk.clone(),
        }
    }
}

/// Drained prepared transaction writes ready for commit.
pub(crate) struct PreparedWriteSet {
    pub(crate) state_rows: PreparedStateBatch,
    pub(crate) insert_selection: PreparedInsertSelection,
    pub(crate) commit_change_refs_by_branch: BTreeMap<String, StagedCommitChangeRefs>,
    pub(crate) first_commit_parent_override_by_branch: BTreeMap<String, CommitId>,
    pub(crate) checkpoint_publications: Vec<CheckpointPublication>,
    pub(crate) extra_commit_parents_by_branch: BTreeMap<String, Vec<CommitId>>,
    pub(crate) intermediate_commits: Vec<StagedIntermediateCommit>,
    pub(crate) file_data_writes: Vec<TransactionFileData>,
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
#[derive(Debug, Default)]
pub(crate) struct PreparedInsertSelection {
    row_count: usize,
    count: usize,
    bits: Vec<u64>,
    origins: Vec<Option<TransactionWriteOrigin>>,
}

#[derive(Clone, Copy)]
pub(crate) struct PreparedInsertRef<'a> {
    pub(crate) row_index: usize,
    pub(crate) row: PreparedStateRowRef<'a>,
    pub(crate) origin: Option<&'a TransactionWriteOrigin>,
}

impl PreparedInsertSelection {
    const WORD_BITS: usize = u64::BITS as usize;

    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn with_row_capacity(row_capacity: usize) -> Self {
        Self {
            row_count: 0,
            count: 0,
            bits: Vec::with_capacity(row_capacity.div_ceil(Self::WORD_BITS)),
            origins: Vec::with_capacity(row_capacity),
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
            .then(|| self.origins[row_index].as_ref())
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
                origin: self.origins[row_index].as_ref(),
            })
    }

    fn push(&mut self, origin: Option<&TransactionWriteOrigin>) {
        let row_index = self.row_count;
        self.resize_rows(row_index + 1);
        self.mark(row_index, origin);
    }

    fn push_not_insert(&mut self) {
        self.resize_rows(self.row_count + 1);
    }

    fn reserve_rows(&mut self, additional: usize, may_insert: bool) {
        if !may_insert && self.is_empty() {
            return;
        }
        if self.origins.is_empty() {
            self.origins
                .reserve(self.row_count.saturating_add(additional));
        } else {
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
        self.row_count = row_count;
    }

    fn mark(&mut self, row_index: usize, origin: Option<&TransactionWriteOrigin>) {
        debug_assert!(row_index < self.row_count);
        let word = row_index / Self::WORD_BITS;
        let bit = row_index % Self::WORD_BITS;
        let mask = 1_u64 << bit;
        if self.bits.is_empty() {
            self.bits
                .resize(self.row_count.div_ceil(Self::WORD_BITS), 0);
            self.origins.resize(self.row_count, None);
        }
        if self.bits[word] & mask == 0 {
            self.bits[word] |= mask;
            self.count += 1;
            self.origins[row_index] = origin.cloned();
        }
    }

    pub(crate) fn select_rows(&mut self, source_by_destination: &[usize]) {
        if self.is_empty() {
            self.row_count = source_by_destination.len();
            return;
        }
        let mut selected = Self::with_row_capacity(source_by_destination.len());
        for &source in source_by_destination {
            if self.contains(source) {
                selected.push(self.origins[source].as_ref());
            } else {
                selected.push_not_insert();
            }
        }
        *self = selected;
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
            }
        })
    }
}

impl PreparedWriteSet {
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
        self.insert_selection.mark(row_index, row.origin.as_ref());
    }
}

impl TransactionWriteBuffer {
    pub(crate) fn new(functions: FunctionProviderHandle) -> Self {
        Self {
            functions,
            rows: Mutex::new(StagedPreparedRows::default()),
            commit_change_refs_by_branch: Mutex::new(BTreeMap::new()),
            first_commit_parent_override_by_branch: Mutex::new(BTreeMap::new()),
            checkpoint_publications: Mutex::new(Vec::new()),
            extra_commit_parents_by_branch: Mutex::new(BTreeMap::new()),
            intermediate_commits: Mutex::new(Vec::new()),
            file_data_writes: Mutex::new(Vec::new()),
        }
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

    /// Takes the normal tracked write lane directly into the transaction
    /// journal. Any duplicate, cross-scope, untracked, global, or otherwise
    /// irregular batch falls back to the indexed overlay.
    fn stage_append_only_if_possible(
        &self,
        mode: Option<TransactionWriteMode>,
        mut rows: PreparedStateBatch,
    ) -> Result<AppendOnlyStage, LixError> {
        let inserts = mode == Some(TransactionWriteMode::Insert);
        if !matches!(
            mode,
            Some(TransactionWriteMode::Replace | TransactionWriteMode::Insert)
        ) || (inserts
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
        if !rows_are_append_only_tracked(&rows, last_key.as_ref()) {
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
                        CommitId::from(self.functions.call_uuid_v7()),
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
            for index in 0..rows.len() {
                rows.set_commit_id(index, Some(commit_id));
            }
        }
        insert_selection.reserve_rows(rows.len(), inserts);
        for row in &rows {
            if inserts {
                insert_selection.push(row.origin);
            } else {
                insert_selection.push_not_insert();
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
                    CommitId::from(self.functions.call_uuid_v7()),
                    ChangeId::from(self.functions.call_uuid_v7()),
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
                insert_selection.push(row.origin);
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
        let mut file_data_guard = self.file_data_writes.lock().map_err(|_| {
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
            file_data_writes: std::mem::take(&mut *file_data_guard),
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
                CommitId::from(functions.call_uuid_v7()),
                ChangeId::from(functions.call_uuid_v7()),
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
            CommitId::from(self.functions.call_uuid_v7()),
            ChangeId::from(self.functions.call_uuid_v7()),
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

    pub(crate) fn stage_intermediate_rows(
        &self,
        commit_id: CommitId,
        mut batch: PreparedStateBatch,
    ) -> Result<(), LixError> {
        let mut intermediate_commits = self.intermediate_commits.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
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
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let StagedPreparedRows::Indexed {
            rows,
            insert_selection,
            by_identity,
            by_candidate,
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
            let index = start + offset;
            let slot = RowSlot::State(index);
            let row = rows.row(index);
            by_candidate.insert(row, slot);
            by_identity.insert(identity, slot);
        }
        commit.change_refs.add_change_count(count);
        Ok(())
    }

    /// Builds the transaction-local read overlay from currently staged writes.
    pub(crate) fn staging_overlay(self: &Arc<Self>) -> Result<PreparedStateRowOverlay, LixError> {
        Ok(PreparedStateRowOverlay {
            staged_writes: Arc::clone(self),
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
        hashes: &[BlobHash],
    ) -> Result<BlobBytesBatch, LixError> {
        if hashes.is_empty() {
            return Ok(BlobBytesBatch::new(Vec::new()));
        }
        let file_data_guard = self.file_data_writes.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged file data lock",
            )
        })?;
        let mut requested = hashes
            .iter()
            .copied()
            .map(|hash| (hash, None))
            .collect::<BTreeMap<BlobHash, Option<&[u8]>>>();
        let mut remaining = requested.len();
        'writes: for write in file_data_guard.iter() {
            let hash = write
                .blob_hash()
                .unwrap_or_else(|| BlobHash::from_content(write.data()));
            if let Some(bytes) = requested.get_mut(&hash)
                && bytes.is_none()
            {
                *bytes = Some(write.data());
                remaining -= 1;
                if remaining == 0 {
                    break 'writes;
                }
            }
            for payload in write.auxiliary_payloads() {
                let hash = payload
                    .hash()
                    .unwrap_or_else(|| BlobHash::from_content(payload.bytes()));
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
        let (mode, count) = match &write {
            PreparedTransactionWrite::Rows { mode, rows } => (Some(*mode), rows.len() as u64),
            PreparedTransactionWrite::RowsWithFileData { mode, count, .. } => (Some(*mode), *count),
        };
        let (mut rows, file_data_writes) = self.state_rows_from_stage_write(write);
        if rows.is_empty() {
            if !file_data_writes.is_empty() {
                self.file_data_writes
                    .lock()
                    .map_err(|_| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            "failed to acquire transaction staged file data lock",
                        )
                    })?
                    .extend(file_data_writes);
            }
            return Ok(TransactionWriteOutcome { count });
        }
        if file_data_writes.is_empty() {
            match self.stage_append_only_if_possible(mode, rows)? {
                AppendOnlyStage::Staged => return Ok(TransactionWriteOutcome { count }),
                AppendOnlyStage::Fallback(fallback_rows) => rows = fallback_rows,
            }
        } else {
            match self.stage_fresh_tracked_file_batch_if_possible(mode, rows)? {
                AppendOnlyStage::Staged => {
                    self.file_data_writes
                        .lock()
                        .map_err(|_| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                "failed to acquire transaction staged file data lock",
                            )
                        })?
                        .extend(file_data_writes);
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
                    insert_selection.push(row.origin);
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
            if !file_data_writes.is_empty() {
                self.file_data_writes
                    .lock()
                    .map_err(|_| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            "failed to acquire transaction staged file data lock",
                        )
                    })?
                    .extend(file_data_writes);
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
            if let Some(RowSlot::State(index)) = existing_slot {
                let previous = if let Some(previous_source) =
                    latest_incoming_source_by_destination.get(&index)
                {
                    rows.row(*previous_source)
                } else {
                    staged_rows.row(index)
                };
                remove_row_from_commit_change_refs(&mut commit_change_refs_guard, previous);
            }
            let commit_id =
                add_row_to_commit_change_refs(&mut commit_change_refs_guard, row, &self.functions);
            let identity = PreparedStateRowIdentity::from(row);
            let insert_origin = if is_insert {
                Some(row.origin.cloned())
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
            if let Some(origin) = insert_origin {
                inserted_destinations.push((destination, origin));
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
        for (destination, origin) in inserted_destinations {
            insert_selection.mark(destination, origin.as_ref());
        }
        for index in new_candidate_destinations {
            by_candidate.insert(staged_rows.row(index), RowSlot::State(index));
        }
        if !file_data_writes.is_empty() {
            self.file_data_writes
                .lock()
                .map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "failed to acquire transaction staged file data lock",
                    )
                })?
                .extend(file_data_writes);
        }
        Ok(TransactionWriteOutcome { count })
    }

    fn state_rows_from_stage_write(
        &self,
        write: PreparedTransactionWrite,
    ) -> (PreparedStateBatch, Vec<TransactionFileData>) {
        match write {
            PreparedTransactionWrite::Rows { rows, .. } => (rows, Vec::new()),
            PreparedTransactionWrite::RowsWithFileData {
                rows, file_data, ..
            } => (rows, file_data),
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
        .then_with(|| left.entity_pk.cmp(right.entity_pk))
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

/// Read overlay derived from staged transaction writes.
#[derive(Clone)]
pub(crate) struct PreparedStateRowOverlay {
    staged_writes: Arc<TransactionWriteBuffer>,
}

#[cfg(test)]
pub(crate) struct StagedScanParts {
    pub(crate) rows: Vec<MaterializedLiveStateRow>,
}

impl PreparedStateRowOverlay {
    /// Returns staged rows visible for a scan request.
    #[cfg(test)]
    pub(crate) fn scan(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        Ok(crate::live_state::resolve_visible_batch(
            self.scan_batch(request)?,
            MaterializedLiveStateBatch::default(),
            &crate::live_state::VisibilityRequest {
                branch_scope: crate::live_state::VisibilityBranchScope::BranchIds {
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
        request: &LiveStateScanRequest,
    ) -> Result<StagedScanParts, LixError> {
        Ok(StagedScanParts {
            rows: self.scan_batch(request)?.into_rows(),
        })
    }

    fn scan_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        if matches!(
            request.filter.rows,
            crate::live_state::LiveStateRowFilter::None
        ) {
            return Ok(MaterializedLiveStateBatch::default());
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
            StagedPreparedRows::AppendOnly { rows, .. } => {
                debug_assert!(rows.is_empty(), "nonempty reads must promote the journal");
                return Ok(MaterializedLiveStateBatch::default());
            }
        };

        let mut rows = MaterializedLiveStateBatchBuilder::with_capacity(staged_rows.len());
        if let Some(slots) = by_candidate.slots_for_filter(&request.filter) {
            append_matching_staged_rows(&mut rows, slots.iter().copied(), staged_rows, request);
        } else {
            append_matching_staged_rows(
                &mut rows,
                by_identity.values().copied(),
                staged_rows,
                request,
            );
        }
        Ok(rows.finish())
    }

    /// Returns a staged exact-row answer, if this transaction has one.
    #[cfg(test)]
    pub(crate) fn load_exact(&self, request: &LiveStateRowRequest) -> Option<StagedExactRow> {
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
    ) -> Option<MaterializedLiveStateRow> {
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
        rows.get(index).map(MaterializedLiveStateRow::from)
    }
}

impl crate::live_state::StagedLiveStateRows for PreparedStateRowOverlay {
    fn staged_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        self.scan_batch(request)
    }

    fn load_exact_batch(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        if request.rows.is_empty() {
            return Ok(MaterializedLiveStateExactBatch::default());
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
                return MaterializedLiveStateExactBatch::new(
                    MaterializedLiveStateBatch::default(),
                    vec![None; request.rows.len()],
                );
            }
        };
        let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(request.rows.len());
        let slots = request
            .rows
            .iter()
            .map(|request_row| {
                let identity = PreparedStateRowIdentity::from_exact_request(request_row);
                let Some(RowSlot::State(index)) = by_identity.get(&identity).copied() else {
                    return None;
                };
                let row = staged_rows.get(index)?;
                if request
                    .untracked
                    .is_some_and(|untracked| row.untracked != untracked)
                {
                    return None;
                }
                if row.snapshot.is_none() && !request.include_tombstones {
                    None
                } else {
                    Some(
                        u32::try_from(push_prepared_materialized(&mut builder, row))
                            .expect("staged exact batch ordinal must fit u32"),
                    )
                }
            })
            .collect();
        MaterializedLiveStateExactBatch::new(builder.finish(), slots)
    }

    fn collection_replaced(
        &self,
        branch_id: &str,
        schema_key: &str,
        file_id: Option<&str>,
    ) -> Result<bool, LixError> {
        self.staged_writes.ensure_identity_index(false)?;
        let rows_guard = self.staged_writes.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let StagedPreparedRows::Indexed { rows, .. } = &*rows_guard else {
            return Ok(false);
        };
        for index in 0..rows.len() {
            let row = rows.row(index);
            if row.schema_key.as_str()
                != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
                || row.branch_id.as_str() != branch_id
                || row.snapshot.is_none()
            {
                continue;
            }
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
}

#[cfg(test)]
pub(crate) enum StagedExactRow {
    Row(MaterializedLiveStateRow),
    Tombstone,
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

    fn from_exact_request(request: &LiveStateExactRowRequest) -> Self {
        Self {
            schema_key: request.schema_key.as_str().into(),
            entity_pk: request.entity_pk.clone(),
            file_id: request.file_id.as_deref().map(Into::into),
            branch_id: request.branch_id.as_str().into(),
        }
    }

    #[cfg(test)]
    fn from_row_request(request: &LiveStateRowRequest) -> Option<Self> {
        let file_id = match &request.file_id {
            NullableKeyFilter::Null => None,
            NullableKeyFilter::Value(value) => Some(value.clone()),
            // Exact overlay lookup requires a concrete row identity.
            NullableKeyFilter::Any => return None,
        };
        Some(Self {
            schema_key: request.schema_key.as_str().into(),
            entity_pk: request.entity_pk.clone(),
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

#[cfg(test)]
impl From<&MaterializedLiveStateRow> for PreparedStateRowIdentity {
    fn from(row: &MaterializedLiveStateRow) -> Self {
        Self {
            schema_key: row.schema_key.as_str().into(),
            entity_pk: row.entity_pk.clone(),
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
                CommitId::from(functions.call_uuid_v7()),
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

fn append_matching_staged_rows(
    output: &mut MaterializedLiveStateBatchBuilder,
    slots: impl IntoIterator<Item = RowSlot>,
    staged_rows: &PreparedStateBatch,
    request: &LiveStateScanRequest,
) {
    for slot in slots {
        let RowSlot::State(index) = slot;
        let Some(row) = staged_rows.get(index) else {
            continue;
        };
        if staged_row_identity_matches_scan(row, request) {
            push_prepared_materialized(output, row);
        }
    }
}

fn push_prepared_materialized(
    output: &mut MaterializedLiveStateBatchBuilder,
    row: PreparedStateRowRef<'_>,
) -> usize {
    output.push_materialized_ref(
        row.entity_pk,
        row.schema_key.as_str(),
        row.file_id.map(SharedStr::as_str),
        row.snapshot.map(StageJson::materialize_shared),
        row.metadata.map(StageJson::materialize_shared),
        row.snapshot.is_none(),
        row.created_at,
        row.updated_at,
        row.global,
        row.change_id,
        row.commit_id,
        row.untracked,
        row.branch_id.as_str(),
    )
}

fn staged_row_identity_matches_scan(
    row: PreparedStateRowRef<'_>,
    request: &LiveStateScanRequest,
) -> bool {
    if !request.filter.schema_keys.is_empty()
        && !request
            .filter
            .schema_keys
            .iter()
            .any(|schema_key| schema_key == row.schema_key.as_str())
    {
        return false;
    }
    if !request.filter.entity_pks.is_empty() && !request.filter.entity_pks.contains(row.entity_pk) {
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

fn staged_branch_matches_scan(branch_id: &str, request: &LiveStateScanRequest) -> bool {
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
    use crate::live_state::{
        LiveStateExactBatchRequest, LiveStateExactRowRequest, LiveStateFilter, LiveStateRowRequest,
        StagedLiveStateRows,
    };

    macro_rules! prepared_rows {
        ($($row:expr),* $(,)?) => {
            PreparedStateBatch::from_test_rows(vec![$($row),*])
        };
    }

    #[test]
    fn ordered_tracked_batches_stay_append_only_until_a_read() {
        let staged_writes = test_staged_writes();
        for rows in [
            prepared_rows![
                tracked_append_row("entity-a", "first"),
                tracked_append_row("entity-b", "second"),
            ],
            prepared_rows![
                tracked_append_row("entity-c", "third"),
                tracked_append_row("entity-d", "fourth"),
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
    fn ordered_tracked_insert_batches_stay_append_only_with_absence_guards() {
        let staged_writes = test_staged_writes();
        for rows in [
            prepared_rows![
                tracked_append_row("entity-a", "first"),
                tracked_append_row("entity-b", "second"),
            ],
            prepared_rows![
                tracked_append_row("entity-c", "third"),
                tracked_append_row("entity-d", "fourth"),
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
                    tracked_append_row("entity-a", "first"),
                    tracked_append_row("entity-b", "second"),
                ],
            })
            .expect("initial ordered insert batch should use the journal");

        let error = staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: prepared_rows![
                    tracked_append_row("entity-a", "duplicate")
                        .with_change_id("duplicate-entity-a"),
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
                rows: prepared_rows![tracked_append_row("entity-a", "first")],
            })
            .expect("initial INSERT should stage");

        let error = staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: prepared_rows![
                    tracked_append_row("entity-c", "must-not-stage"),
                    tracked_append_row("entity-a", "duplicate")
                        .with_change_id("duplicate-entity-a"),
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
                    "entity-c",
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
                .entity_pk
                .as_single_string()
                .expect("scalar entity"),
            "entity-a"
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
                    tracked_append_row("entity-a", "first"),
                    tracked_append_row("entity-b", "second"),
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
                    tracked_append_row("entity-a", "first"),
                    tracked_append_row("entity-b", "second"),
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
                "entity-b",
            ))
            .expect("staged row should answer the exact read");
        assert!(matches!(row, StagedExactRow::Row(_)));
        assert!(staged_writes.uses_identity_index_for_tests());

        assert!(
            overlay
                .load_exact(&exact_request_for_branch_key(
                    "01920000-0000-7000-8000-0000000000a1",
                    "entity-a"
                ))
                .is_some()
        );
        assert!(
            staged_writes.uses_identity_index_for_tests(),
            "reads keep the single materialized index rather than rebuilding it"
        );
    }

    #[test]
    fn overlapping_tracked_batch_promotes_and_keeps_last_write() {
        let staged_writes = test_staged_writes();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    tracked_append_row("entity-a", "first"),
                    tracked_append_row("entity-b", "before"),
                ],
            })
            .expect("initial tracked batch should stage");
        assert!(!staged_writes.uses_identity_index_for_tests());

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    tracked_append_row("entity-b", "after").with_change_id("entity-b-after"),
                ],
            })
            .expect("overlapping write should use the indexed fallback");
        assert!(staged_writes.uses_identity_index_for_tests());

        let drained = staged_writes.drain().expect("writes should drain");
        assert_eq!(drained.state_rows.len(), 2);
        assert!(drained.state_rows.iter().any(|row| {
            row.entity_pk == &EntityPk::single("entity-b")
                && row.snapshot.as_ref().map(|snapshot| snapshot.normalized())
                    == Some("{\"key\":\"entity-b\",\"value\":\"after\"}")
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
    fn tracked_append_order_uses_file_before_entity() {
        let staged_writes = test_staged_writes();
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    tracked_append_row("entity-z", "first")
                        .with_file_id("01920000-0000-7000-8000-0000000000a2"),
                    tracked_append_row("entity-a", "second")
                        .with_file_id("01920000-0000-7000-8000-0000000000b2"),
                ],
            })
            .expect("tracked-tree order should accept file-before-entity rows");
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
            .load_exact(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
                entity_pk: EntityPk::single("sql2-duplicate-key"),
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
                    state_row("entity-a", "cross-pair")
                        .with_file_id("01920000-0000-7000-8000-0000000000b2"),
                    tombstone_row("deleted").with_file_id("deleted"),
                ],
            })
            .expect("rows should stage");
        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build");
        let exact = |entity: &str, file_id: &str| LiveStateExactRowRequest {
            schema_key: "lix_key_value".into(),
            branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".into(),
            entity_pk: EntityPk::single(entity),
            file_id: Some(file_id.to_string()),
        };
        let cross_pair = exact("entity-a", "01920000-0000-7000-8000-0000000000b2");
        let exact_request = LiveStateExactBatchRequest {
            rows: vec![
                cross_pair.clone(),
                exact("entity-a", "01920000-0000-7000-8000-0000000000a2"),
                exact("entity-b", "01920000-0000-7000-8000-0000000000b2"),
                cross_pair,
                exact("missing", "missing"),
                exact("deleted", "deleted"),
            ],
            ..Default::default()
        };
        let batch = StagedLiveStateRows::load_exact_batch(&overlay, &exact_request)
            .expect("exact staged batch should load directly");
        let first = batch.row(0).expect("first exact row");
        let duplicate = batch.row(3).expect("duplicate exact row");
        assert_eq!(first.entity_pk(), duplicate.entity_pk());
        assert!(std::ptr::eq(first.schema_key(), duplicate.schema_key()));
        assert!(batch.row(1).is_none());
        assert!(batch.row(2).is_none());
        assert!(batch.row(4).is_none());
        assert!(batch.row(5).is_none());

        let rows = StagedLiveStateRows::load_exact_batch(&overlay, &exact_request)
            .expect("exact staged rows should load")
            .into_rows();

        assert!(rows[0].is_some());
        assert_eq!(rows[0], rows[3]);
        assert_eq!(rows[1], None);
        assert_eq!(rows[2], None);
        assert_eq!(rows[4], None);
        assert_eq!(rows[5], None, "tombstone should be hidden by default");

        let tombstone = StagedLiveStateRows::load_exact_batch(
            &overlay,
            &LiveStateExactBatchRequest {
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
            row.entity_pk == &EntityPk::single("sql2-key-a")
                && row.snapshot.as_ref().map(|snapshot| snapshot.normalized())
                    == Some("{\"key\":\"sql2-key-a\",\"value\":\"second\"}")
        }));
        assert!(drained.state_rows.iter().any(|row| {
            row.entity_pk == &EntityPk::single("sql2-key-b")
                && row.snapshot.as_ref().map(|snapshot| snapshot.normalized())
                    == Some("{\"key\":\"sql2-key-b\",\"value\":\"only\"}")
        }));
    }

    #[tokio::test]
    async fn staged_writes_drain_preserves_file_data_payloads() {
        let staged_writes = test_staged_writes();
        let result: crate::Blob = b"hello".as_slice().into();
        let provenance = crate::common::RequestBlobSpliceProvenance::new_validated_for_test(
            b"heo",
            &result,
            2,
            1,
            b"ll".to_vec(),
        );
        let mut file_data = TransactionFileData::new(
            "01920000-0000-7000-8000-0000000000d2".to_string(),
            Some("/readme.md".to_string()),
            Some("readme.md".to_string()),
            "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
            true,
            true,
            result,
        );
        file_data.set_splice_provenance(Some(provenance.clone()));

        let rows = prepared_rows![state_row(
            "01920000-0000-7000-8000-0000000000d2",
            "descriptor",
        )];
        let input_rows_allocation = rows.slot_allocation_ptr() as usize;
        staged_writes
            .stage_write(PreparedTransactionWrite::RowsWithFileData {
                mode: TransactionWriteMode::Replace,
                rows,
                file_data: vec![file_data],
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
        assert_eq!(drained.file_data_writes.len(), 1);
        assert_eq!(
            drained.file_data_writes[0].file_id,
            "01920000-0000-7000-8000-0000000000d2"
        );
        assert_eq!(drained.file_data_writes[0].data(), b"hello");
        assert_eq!(
            drained.file_data_writes[0].splice_provenance(),
            Some(&provenance)
        );
    }

    #[test]
    fn fresh_tracked_file_batch_reorders_once_and_stays_unindexed() {
        let staged_writes = test_staged_writes();
        let rows = prepared_rows![
            tracked_append_row("entity-c", "third"),
            tracked_append_row("entity-a", "first"),
            tracked_append_row("entity-b", "second"),
        ];
        let input_rows_allocation = rows.slot_allocation_ptr() as usize;
        let file_data = TransactionFileData::new(
            "01920000-0000-7000-8000-0000000000a2".to_string(),
            Some("/batch.json".to_string()),
            Some("batch.json".to_string()),
            "01920000-0000-7000-8000-0000000000a1".to_string(),
            false,
            false,
            b"payload".to_vec(),
        );

        staged_writes
            .stage_write(PreparedTransactionWrite::RowsWithFileData {
                mode: TransactionWriteMode::Replace,
                rows,
                file_data: vec![file_data],
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
                .map(|row| row.entity_pk.as_single_string().unwrap())
                .collect::<Vec<_>>(),
            ["entity-a", "entity-b", "entity-c"]
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
        assert_eq!(drained.file_data_writes.len(), 1);
    }

    #[test]
    fn cross_row_file_batch_keeps_source_order_in_the_generic_lane() {
        let staged_writes = test_staged_writes();
        let mut first = tracked_append_row("entity-z", "first-source-row");
        first.facts.requires_transaction_validation = true;
        let mut second = tracked_append_row("entity-a", "second-source-row");
        second.facts.requires_transaction_validation = true;
        let file_data = TransactionFileData::new(
            "01920000-0000-7000-8000-0000000000a2".to_string(),
            Some("/batch.json".to_string()),
            Some("batch.json".to_string()),
            "01920000-0000-7000-8000-0000000000a1".to_string(),
            false,
            false,
            b"payload".to_vec(),
        );

        staged_writes
            .stage_write(PreparedTransactionWrite::RowsWithFileData {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![first, second],
                file_data: vec![file_data],
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
                .map(|row| row.entity_pk.as_single_string().unwrap())
                .collect::<Vec<_>>(),
            ["entity-z", "entity-a"],
            "commit validation must observe source/cursor order"
        );
    }

    #[test]
    fn file_data_lane_coalesces_repeated_identity_for_reads_and_drain() {
        let staged_writes = test_staged_writes();
        let file_data = TransactionFileData::new(
            "resurrected-file".to_string(),
            Some("/resurrected.json".to_string()),
            Some("resurrected.json".to_string()),
            "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
            true,
            true,
            b"payload".to_vec(),
        );

        staged_writes
            .stage_write(PreparedTransactionWrite::RowsWithFileData {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    tombstone_row("resurrected-file"),
                    state_row("resurrected-file", "latest"),
                ],
                file_data: vec![file_data],
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
        assert_eq!(drained.file_data_writes.len(), 1);
        assert_eq!(drained.file_data_writes[0].data(), b"payload");
    }

    #[test]
    fn staged_file_byte_lookup_filters_main_and_auxiliary_payloads_before_copying() {
        let staged_writes = test_staged_writes();
        let mut requested_write = TransactionFileData::new(
            "requested-file".to_string(),
            Some("/requested.bin".to_string()),
            Some("requested.bin".to_string()),
            "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
            true,
            true,
            b"requested-main".to_vec(),
        );
        requested_write.add_auxiliary_payload(b"requested-auxiliary".to_vec());
        let unrelated_write = TransactionFileData::new(
            "unrelated-file".to_string(),
            Some("/unrelated.bin".to_string()),
            Some("unrelated.bin".to_string()),
            "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
            true,
            true,
            b"unrelated-main".to_vec(),
        );
        staged_writes
            .stage_write(PreparedTransactionWrite::RowsWithFileData {
                mode: TransactionWriteMode::Replace,
                rows: PreparedStateBatch::new(),
                file_data: vec![unrelated_write, requested_write],
                count: 2,
            })
            .expect("file payloads should stage");

        let auxiliary_hash = BlobHash::from_content(b"requested-auxiliary");
        let missing_hash = BlobHash::from_content(b"missing");
        let main_hash = BlobHash::from_content(b"requested-main");
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
    async fn staging_overlay_identity_matches_live_state_conflict_key() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![state_row("shared-entity", "base")],
            })
            .expect("initial same-identity row should stage");
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: prepared_rows![
                    state_row("shared-entity", "latest"),
                    state_row("shared-entity", "other-branch")
                        .with_branch("01920000-0000-7000-8000-0000000000b1"),
                    state_row("shared-entity", "other-schema").with_schema("other_schema"),
                    state_row("shared-entity", "other-file")
                        .with_file_id("01920000-0000-7000-8000-0000000000a2"),
                ],
            })
            .expect("staging rows should succeed");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged rows");
        let rows = overlay
            .scan(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    entity_pks: vec![EntityPk::single("shared-entity")],
                    include_tombstones: true,
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
            })
            .expect("overlay scan should succeed");

        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows.iter()
                .filter(|row| row.entity_pk == EntityPk::single("shared-entity")
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
                    state_row("other", "other-entity"),
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
            .scan_parts(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    entity_pks: vec![EntityPk::single("selected")],
                    branch_ids: vec!["01920000-0000-7000-8000-0000000000a1".to_string()],
                    file_ids: vec![NullableKeyFilter::Null],
                    include_tombstones: true,
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
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
                && row.entity_pk == EntityPk::single("selected")
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
            .scan_parts(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    branch_ids: vec!["01920000-0000-7000-8000-0000000000a1".to_string()],
                    file_ids: vec![NullableKeyFilter::Value("selected-file".to_string())],
                    include_tombstones: true,
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
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

        let filter = LiveStateFilter {
            schema_keys: vec!["lix_key_value".to_string(), "lix_key_value".to_string()],
            entity_pks: vec![
                EntityPk::single("second"),
                EntityPk::single("first"),
                EntityPk::single("first"),
            ],
            ..LiveStateFilter::default()
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

        let filter = LiveStateFilter {
            schema_keys: vec!["lix_key_value".to_string(), "lix_key_value".to_string()],
            file_ids: vec![
                NullableKeyFilter::Value("other".to_string()),
                NullableKeyFilter::Value("selected".to_string()),
                NullableKeyFilter::Value("selected".to_string()),
            ],
            ..LiveStateFilter::default()
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

        let null_filter = LiveStateFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            file_ids: vec![NullableKeyFilter::Null],
            ..LiveStateFilter::default()
        };
        let Some(Cow::Borrowed(null_slots)) = index.slots_for_filter(&null_filter) else {
            panic!("single null file filter should borrow indexed candidates");
        };
        assert_eq!(null_slots, &[RowSlot::State(4)]);

        let any_filter = LiveStateFilter {
            schema_keys: vec!["lix_key_value".to_string()],
            file_ids: vec![NullableKeyFilter::Any],
            ..LiveStateFilter::default()
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

        let filter = LiveStateFilter {
            schema_keys: vec![
                "other_schema".to_string(),
                "lix_key_value".to_string(),
                "lix_key_value".to_string(),
            ],
            ..LiveStateFilter::default()
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
            .slots_by_schema_and_entity
            .get(row.schema_key.as_str())
            .and_then(|by_entity| by_entity.get(&row.entity_pk))
            .expect("indexed entity should retain its candidate slot");
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
        let request = LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec!["lix_key_value".to_string()],
                entity_pks: vec![EntityPk::single("selected")],
                branch_ids: vec!["01920000-0000-7000-8000-0000000000a1".to_string()],
                file_ids: vec![NullableKeyFilter::Null],
                include_tombstones: true,
                ..LiveStateFilter::default()
            },
            ..LiveStateScanRequest::default()
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
        assert_eq!(change_refs.commit_change_id, test_change_id(2));
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

        fn timestamp(&mut self) -> crate::common::LixTimestamp {
            self.timestamp_count += 1;
            crate::common::LixTimestamp::expect_parse(
                "timestamp",
                &format!("2026-01-01T00:00:00.{:03}Z", self.timestamp_count),
            )
        }
    }

    fn test_uuid(index: usize) -> String {
        test_uuid_value(index).to_string()
    }

    fn test_uuid_value(index: usize) -> uuid::Uuid {
        uuid::Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0000 + index as u128)
    }

    fn test_commit_id(index: usize) -> CommitId {
        CommitId::parse(&test_uuid(index)).expect("test uuid should parse as commit id")
    }

    fn test_change_id(index: usize) -> ChangeId {
        ChangeId::parse(&test_uuid(index)).expect("test uuid should parse as change id")
    }

    fn state_row(key: &str, value: &str) -> TestPreparedStateRow {
        let snapshot = stage_json_from_value(
            TransactionJson::from_value_for_test(serde_json::json!({ "key": key, "value": value })),
            "test staged row snapshot_content",
        )
        .expect("test snapshot should prepare");
        TestPreparedStateRow {
            schema_plan_id: SchemaPlanId::for_test(0),
            facts: crate::transaction::types::PreparedRowFacts::default(),
            entity_pk: EntityPk::single(key),
            schema_key: "lix_key_value".into(),
            file_id: None,
            snapshot: Some(snapshot),
            metadata: None,
            origin: None,
            origin_key: None,
            created_at: crate::common::LixTimestamp::expect_parse(
                "created_at",
                "2026-01-01T00:00:00.000Z",
            ),
            updated_at: crate::common::LixTimestamp::expect_parse(
                "updated_at",
                "2026-01-01T00:00:00.000Z",
            ),
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

    fn exact_request_for_key(key: &str) -> LiveStateRowRequest {
        LiveStateRowRequest {
            schema_key: "lix_key_value".to_string(),
            branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
            entity_pk: EntityPk::single(key),
            file_id: NullableKeyFilter::Null,
        }
    }

    fn exact_request_for_branch_key(branch_id: &str, key: &str) -> LiveStateRowRequest {
        LiveStateRowRequest {
            schema_key: "lix_key_value".to_string(),
            branch_id: branch_id.to_string(),
            entity_pk: EntityPk::single(key),
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
    fn ten_thousand_inserts_use_two_dense_buffers_and_no_identity_copies() {
        const ROW_COUNT: usize = 10_000;
        let staged_writes = test_staged_writes();
        let mut rows = PreparedStateBatch::with_capacity(ROW_COUNT);
        for row_index in 0..ROW_COUNT {
            let key = format!("entity-{row_index:05}");
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
        assert_eq!(drained.insert_selection.large_buffer_count(), 2);
        assert_eq!(drained.insert_selection.identity_copy_count(), 0);
        assert!(
            (0..ROW_COUNT).all(|row_index| drained.insert_selection.contains(row_index)),
            "the dense bitmap must preserve every INSERT ordinal"
        );
    }

    fn scan_request_for_key(key: &str, include_tombstones: bool) -> LiveStateScanRequest {
        LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec!["lix_key_value".to_string()],
                entity_pks: vec![EntityPk::single(key)],
                branch_ids: vec!["ffffffff-ffff-7fff-bfff-ffffffffffff".to_string()],
                file_ids: vec![NullableKeyFilter::Null],
                include_tombstones,
                ..LiveStateFilter::default()
            },
            ..LiveStateScanRequest::default()
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
