#![allow(
    clippy::cloned_instead_of_copied,
    clippy::large_enum_variant,
    clippy::option_as_ref_cloned,
    clippy::option_if_let_else,
    clippy::ref_option,
    clippy::unnecessary_wraps,
    clippy::unused_self
)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::{BlobBytesBatch, BlobHash};
use crate::catalog::SchemaPlanId;
use crate::changelog::{ChangeId, CommitId};
use crate::domain::{Domain, DomainRowIdentity};
use crate::entity_pk::EntityPk;
#[cfg(test)]
use crate::functions::FunctionProvider;
use crate::functions::FunctionProviderHandle;
#[cfg(test)]
use crate::live_state::LiveStateRowRequest;
use crate::live_state::{LiveStateScanRequest, MaterializedLiveStateRow};
use crate::transaction::types::{
    LogicalPrimaryKey, PreparedTransactionWrite, StagedCommitChangeRef, TransactionFileData,
    TransactionWriteMode, TransactionWriteOperation, TransactionWriteOrigin,
    TransactionWriteOutcome,
};
use crate::transaction::types::{PreparedStateRow, StagedCommitChangeRefs};
#[cfg(test)]
use crate::transaction::types::{TransactionJson, stage_json_from_value};
use crate::{LixError, NullableKeyFilter};

/// Transaction-local write buffer after transaction-boundary preparation.
///
/// This is the engine seam between SQL execution and transaction ownership:
/// write frontends pass decoded `TransactionWriteRow`s to `Transaction`, the
/// transaction prepares them into stable `PreparedStateRow`s, reads build a
/// `PreparedStateRowOverlay` from those rows, and commit drains the same rows.
pub(crate) struct TransactionWriteBuffer {
    functions: FunctionProviderHandle,
    rows: Mutex<Vec<Option<PreparedStateRow>>>,
    by_identity: Mutex<HashMap<PreparedStateRowIdentity, RowSlot>>,
    insert_identities: Mutex<BTreeMap<PreparedStateRowIdentity, PreparedInsertIdentity>>,
    commit_change_refs_by_branch: Mutex<BTreeMap<String, StagedCommitChangeRefs>>,
    extra_commit_parents_by_branch: Mutex<BTreeMap<String, Vec<CommitId>>>,
    file_data_writes: Mutex<Vec<TransactionFileData>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowSlot {
    State(usize),
}

/// Drained prepared transaction writes ready for commit.
pub(crate) struct PreparedWriteSet {
    pub(crate) state_rows: Vec<PreparedStateRow>,
    pub(crate) insert_identities: BTreeMap<PreparedStateRowIdentity, PreparedInsertIdentity>,
    pub(crate) commit_change_refs_by_branch: BTreeMap<String, StagedCommitChangeRefs>,
    pub(crate) extra_commit_parents_by_branch: BTreeMap<String, Vec<CommitId>>,
    pub(crate) file_data_writes: Vec<TransactionFileData>,
}

pub(crate) struct PreparedWriteValidationSet<'a> {
    rows: Vec<PreparedValidationRow<'a>>,
    constraint_rows: Vec<PreparedValidationRow<'a>>,
    insert_identities: Vec<(&'a PreparedStateRowIdentity, &'a PreparedInsertIdentity)>,
}

pub(crate) struct PreparedWriteValidationIndex<'a> {
    rows_by_schema_scope: BTreeMap<Domain, Vec<PreparedValidationRow<'a>>>,
    insert_identities_by_schema_scope:
        BTreeMap<Domain, Vec<(&'a PreparedStateRowIdentity, &'a PreparedInsertIdentity)>>,
}

#[derive(Clone, Copy)]
pub(crate) enum PreparedValidationRow<'a> {
    State(&'a PreparedStateRow),
}

impl<'a> PreparedValidationRow<'a> {
    pub(crate) fn entity_pk(&self) -> &EntityPk {
        match self {
            Self::State(row) => &row.entity_pk,
        }
    }

    pub(crate) fn schema_plan_id(&self) -> SchemaPlanId {
        match self {
            Self::State(row) => row.schema_plan_id,
        }
    }

    pub(crate) fn schema_key(&self) -> &str {
        match self {
            Self::State(row) => &row.schema_key,
        }
    }

    pub(crate) fn file_id(&self) -> &Option<String> {
        match self {
            Self::State(row) => &row.file_id,
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot_content(&self) -> Option<&str> {
        match self {
            Self::State(row) => row
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref()),
        }
    }

    pub(crate) fn snapshot_json(self) -> Option<&'a serde_json::Value> {
        match self {
            Self::State(row) => row
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.value.as_ref()),
        }
    }

    pub(crate) fn metadata_json(self) -> Option<&'a serde_json::Value> {
        match self {
            Self::State(row) => row
                .metadata
                .as_ref()
                .map(|metadata| metadata.value.as_ref()),
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
            self.file_id().clone(),
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
            rows: self
                .rows_by_schema_scope
                .get(schema_scope)
                .cloned()
                .unwrap_or_default(),
            constraint_rows,
            insert_identities: self
                .insert_identities_by_schema_scope
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

    pub(crate) fn insert_identities(
        &self,
    ) -> impl Iterator<
        Item = (
            &PreparedStateRowIdentity,
            bool,
            Option<&TransactionWriteOrigin>,
        ),
    > {
        self.insert_identities
            .iter()
            .map(|(identity, insert)| (*identity, insert.untracked, insert.origin.as_ref()))
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
        let mut insert_identities_by_schema_scope =
            BTreeMap::<Domain, Vec<(&PreparedStateRowIdentity, &PreparedInsertIdentity)>>::new();
        for (identity, insert) in &self.insert_identities {
            insert_identities_by_schema_scope
                .entry(identity.domain(insert.untracked).schema_catalog_domain())
                .or_default()
                .push((identity, insert));
        }

        PreparedWriteValidationIndex {
            rows_by_schema_scope,
            insert_identities_by_schema_scope,
        }
    }

    #[cfg(test)]
    pub(crate) fn validation_set_for_tests(&self) -> PreparedWriteValidationSet<'_> {
        let rows: Vec<_> = self.validation_rows().collect();
        let insert_identities = self.insert_identities.iter().collect();
        PreparedWriteValidationSet {
            constraint_rows: rows.clone(),
            rows,
            insert_identities,
        }
    }
}

impl TransactionWriteBuffer {
    pub(crate) fn new(functions: FunctionProviderHandle) -> Self {
        Self {
            functions,
            rows: Mutex::new(Vec::new()),
            by_identity: Mutex::new(HashMap::new()),
            insert_identities: Mutex::new(BTreeMap::new()),
            commit_change_refs_by_branch: Mutex::new(BTreeMap::new()),
            extra_commit_parents_by_branch: Mutex::new(BTreeMap::new()),
            file_data_writes: Mutex::new(Vec::new()),
        }
    }

    /// Drains staged writes for commit.
    pub(crate) fn drain(&self) -> Result<PreparedWriteSet, LixError> {
        let mut rows_guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let mut by_identity_guard = self.by_identity.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged identity index lock",
            )
        })?;
        let mut file_data_guard = self.file_data_writes.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged file data lock",
            )
        })?;
        let mut insert_identities_guard = self.insert_identities.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged insert identity lock",
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
        let result = Ok(PreparedWriteSet {
            state_rows: std::mem::take(&mut *rows_guard)
                .into_iter()
                .flatten()
                .collect(),
            insert_identities: std::mem::take(&mut *insert_identities_guard),
            commit_change_refs_by_branch: std::mem::take(&mut *commit_change_refs_guard),
            extra_commit_parents_by_branch: std::mem::take(&mut *extra_parents_guard),
            file_data_writes: std::mem::take(&mut *file_data_guard),
        });
        by_identity_guard.clear();
        result
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
        selected_change_refs: impl IntoIterator<Item = StagedCommitChangeRef>,
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
        for change_ref in selected_change_refs {
            change_refs.add_selected_change_ref(change_ref);
        }
        Ok(change_refs.commit_id.to_string())
    }

    /// Builds the transaction-local read overlay from currently staged writes.
    pub(crate) fn staging_overlay(self: &Arc<Self>) -> Result<PreparedStateRowOverlay, LixError> {
        Ok(PreparedStateRowOverlay {
            staged_writes: Arc::clone(self),
        })
    }

    pub(crate) fn has_staged_filesystem_descriptors(&self) -> Result<bool, LixError> {
        let rows = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        Ok(rows.iter().flatten().any(|row| {
            matches!(
                row.schema_key.as_str(),
                "lix_file_descriptor" | "lix_directory_descriptor"
            )
        }))
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
        let mut bytes_by_hash = BTreeMap::<BlobHash, Vec<u8>>::new();
        for write in file_data_guard.iter() {
            let hash = write
                .blob_hash()
                .unwrap_or_else(|| BlobHash::from_content(write.data()));
            bytes_by_hash
                .entry(hash)
                .or_insert_with(|| write.data().to_vec());
        }
        Ok(BlobBytesBatch::new(
            hashes
                .iter()
                .map(|hash| bytes_by_hash.get(hash).cloned())
                .collect(),
        ))
    }

    /// Stages one prepared write batch into this transaction.
    ///
    /// Frontends hand raw `TransactionWriteRow`s to `Transaction`; normalization prepares
    /// stable `PreparedStateRow`s before this method indexes them for transaction-
    /// local reads and commit routing.
    pub(crate) fn stage_write(
        &self,
        write: PreparedTransactionWrite,
    ) -> Result<TransactionWriteOutcome, LixError> {
        let (mode, count) = match &write {
            PreparedTransactionWrite::Rows { mode, rows } => (Some(*mode), rows.len() as u64),
            PreparedTransactionWrite::RowsWithFileData { mode, count, .. } => (Some(*mode), *count),
        };
        let functions = self.functions.clone();
        let (rows, file_data_writes) = self.state_rows_from_stage_write(write);
        reject_mixed_durability_rows_in_batch(&rows)?;
        reject_duplicate_present_rows_in_batch(&rows)?;
        let mut guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let mut by_identity_guard = self.by_identity.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged identity index lock",
            )
        })?;
        let mut commit_change_refs_guard =
            self.commit_change_refs_by_branch.lock().map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "failed to acquire transaction staged commit change refs lock",
                )
            })?;
        let mut insert_identities_guard = self.insert_identities.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged insert identity lock",
            )
        })?;
        for row in &rows {
            if row.global && row.branch_id != GLOBAL_BRANCH_ID {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "global staged rows must use the global branch id",
                ));
            }
            let identity = PreparedStateRowIdentity::from(row);
            let Some(RowSlot::State(index)) = by_identity_guard.get(&identity).copied() else {
                continue;
            };
            let Some(previous) = guard.get(index).and_then(Option::as_ref) else {
                continue;
            };
            if previous.untracked != row.untracked {
                return Err(mixed_durability_error(row));
            }
        }
        for mut row in rows {
            let identity = PreparedStateRowIdentity::from(&row);
            if mode == Some(TransactionWriteMode::Insert)
                && by_identity_guard.contains_key(&identity)
            {
                return Err(duplicate_insert_identity_error(&row));
            }
            let existing_slot = by_identity_guard.get(&identity).copied();
            if let Some(RowSlot::State(index)) = existing_slot {
                if let Some(previous) = guard.get_mut(index).and_then(Option::take) {
                    remove_row_from_commit_change_refs(&mut commit_change_refs_guard, &previous);
                }
            }
            add_row_to_commit_change_refs(&mut commit_change_refs_guard, &mut row, &functions);
            let identity = PreparedStateRowIdentity::from(&row);
            let is_insert = mode == Some(TransactionWriteMode::Insert)
                && !row
                    .origin
                    .as_ref()
                    .is_some_and(|origin| origin.operation == TransactionWriteOperation::Update);
            if is_insert {
                insert_identities_guard.insert(
                    identity.clone(),
                    PreparedInsertIdentity {
                        untracked: row.untracked,
                        origin: row.origin.clone(),
                    },
                );
            }
            let slot = match existing_slot {
                Some(RowSlot::State(index)) => {
                    guard[index] = Some(row);
                    RowSlot::State(index)
                }
                None => {
                    let index = guard.len();
                    guard.push(Some(row));
                    RowSlot::State(index)
                }
            };
            by_identity_guard.insert(identity, slot);
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
    ) -> (Vec<PreparedStateRow>, Vec<TransactionFileData>) {
        let mut state_rows = Vec::new();
        let mut file_data_writes = Vec::new();
        match write {
            PreparedTransactionWrite::Rows { rows, .. } => {
                state_rows.extend(rows);
            }
            PreparedTransactionWrite::RowsWithFileData {
                rows, file_data, ..
            } => {
                state_rows.extend(rows);
                file_data_writes.extend(file_data);
            }
        }
        (state_rows, file_data_writes)
    }
}

/// Read overlay derived from staged transaction writes.
#[derive(Clone)]
pub(crate) struct PreparedStateRowOverlay {
    staged_writes: Arc<TransactionWriteBuffer>,
}

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
        Ok(crate::live_state::resolve_visible_rows(
            self.scan_parts(request)?.rows,
            Vec::new(),
            &crate::live_state::VisibilityRequest {
                branch_scope: crate::live_state::VisibilityBranchScope::BranchIds {
                    branch_ids: request.filter.branch_ids.clone(),
                },
                include_tombstones: request.filter.include_tombstones,
                limit: None,
            },
        ))
    }

    /// Returns staged rows and base-row identities hidden by staged rows in one pass.
    ///
    /// Tombstones hide base rows even when the request does not include
    /// tombstone rows in the visible result set.
    pub(crate) fn scan_parts(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<StagedScanParts, LixError> {
        if matches!(
            request.filter.rows,
            crate::live_state::LiveStateRowFilter::None
        ) {
            return Ok(StagedScanParts { rows: Vec::new() });
        }

        let rows_guard = self.staged_writes.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let by_identity_guard = self.staged_writes.by_identity.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged identity index lock",
            )
        })?;

        let mut rows = Vec::new();
        for slot in by_identity_guard.values() {
            match *slot {
                RowSlot::State(index) => {
                    let Some(row) = rows_guard.get(index).and_then(Option::as_ref) else {
                        continue;
                    };
                    if !staged_row_identity_matches_scan(row, request) {
                        continue;
                    }
                    rows.push(MaterializedLiveStateRow::from(row));
                }
            }
        }
        Ok(StagedScanParts { rows })
    }

    /// Returns a staged exact-row answer, if this transaction has one.
    #[cfg(test)]
    pub(crate) fn load_exact(&self, request: &LiveStateRowRequest) -> Option<StagedExactRow> {
        let identity = PreparedStateRowIdentity::from_exact_request(request)?;
        if let Some(row) = self.load_state_slot(&identity) {
            return Some(if row.snapshot.is_none() {
                StagedExactRow::Tombstone
            } else {
                StagedExactRow::Row(MaterializedLiveStateRow::from(&row))
            });
        }
        None
    }

    #[cfg(test)]
    fn load_state_slot(&self, identity: &PreparedStateRowIdentity) -> Option<PreparedStateRow> {
        let rows_guard = self.staged_writes.rows.lock().ok()?;
        let by_identity_guard = self.staged_writes.by_identity.lock().ok()?;
        let Some(RowSlot::State(index)) = by_identity_guard.get(identity).copied() else {
            return None;
        };
        rows_guard.get(index)?.as_ref().cloned()
    }
}

impl crate::live_state::StagedLiveStateRows for PreparedStateRowOverlay {
    fn staged_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        Ok(self.scan_parts(request)?.rows)
    }
}

#[cfg(test)]
pub(crate) enum StagedExactRow {
    Row(MaterializedLiveStateRow),
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PreparedStateRowIdentity {
    schema_key: String,
    entity_pk: EntityPk,
    file_id: Option<String>,
    branch_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedInsertIdentity {
    untracked: bool,
    origin: Option<TransactionWriteOrigin>,
}

impl PreparedStateRowIdentity {
    fn from_staged_row(row: &PreparedStateRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            entity_pk: row.entity_pk.clone(),
            file_id: row.file_id.clone(),
            branch_id: row.branch_id.clone(),
        }
    }

    #[cfg(test)]
    fn from_exact_request(request: &LiveStateRowRequest) -> Option<Self> {
        let file_id = match &request.file_id {
            NullableKeyFilter::Null => None,
            NullableKeyFilter::Value(value) => Some(value.clone()),
            // Exact overlay lookup requires a concrete row identity.
            NullableKeyFilter::Any => return None,
        };
        Some(Self {
            schema_key: request.schema_key.clone(),
            entity_pk: request.entity_pk.clone(),
            file_id,
            branch_id: request.branch_id.clone(),
        })
    }

    pub(crate) fn schema_key(&self) -> &str {
        &self.schema_key
    }

    pub(crate) fn entity_pk(&self) -> &EntityPk {
        &self.entity_pk
    }

    pub(crate) fn domain(&self, untracked: bool) -> Domain {
        Domain::exact_file(self.branch_id.clone(), untracked, self.file_id.clone())
    }
}

impl From<&PreparedStateRow> for PreparedStateRowIdentity {
    fn from(row: &PreparedStateRow) -> Self {
        Self::from_staged_row(row)
    }
}

impl From<&MaterializedLiveStateRow> for PreparedStateRowIdentity {
    fn from(row: &MaterializedLiveStateRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            entity_pk: row.entity_pk.clone(),
            file_id: row.file_id.clone(),
            branch_id: row.branch_id.clone(),
        }
    }
}

fn reject_mixed_durability_rows_in_batch(rows: &[PreparedStateRow]) -> Result<(), LixError> {
    let mut durability_by_identity = BTreeMap::<PreparedStateRowIdentity, bool>::new();
    for row in rows {
        let identity = PreparedStateRowIdentity::from(row);
        if durability_by_identity
            .insert(identity, row.untracked)
            .is_some_and(|untracked| untracked != row.untracked)
        {
            return Err(mixed_durability_error(row));
        }
    }
    Ok(())
}

fn mixed_durability_error(row: &PreparedStateRow) -> LixError {
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

fn reject_duplicate_present_rows_in_batch(rows: &[PreparedStateRow]) -> Result<(), LixError> {
    let mut pending_present_rows = BTreeMap::<PreparedStateRowIdentity, &PreparedStateRow>::new();
    for row in rows {
        let identity = PreparedStateRowIdentity::from(row);
        if row.snapshot.is_none() {
            pending_present_rows.remove(&identity);
            continue;
        }
        if let Some(previous) = pending_present_rows.insert(identity, row) {
            return Err(duplicate_staged_present_row_error(row, previous));
        }
    }
    Ok(())
}

fn duplicate_staged_present_row_error(
    row: &PreparedStateRow,
    previous: &PreparedStateRow,
) -> LixError {
    let message = logical_primary_key_violation_message(row.origin.as_ref())
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

fn duplicate_insert_identity_error(row: &PreparedStateRow) -> LixError {
    let message = duplicate_insert_identity_message(
        &row.schema_key,
        &row.entity_pk,
        Some(&row.branch_id),
        row.origin.as_ref(),
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
    row: &mut PreparedStateRow,
    functions: &FunctionProviderHandle,
) {
    if row.untracked {
        return;
    }
    let change_id = row
        .change_id
        .expect("tracked staged rows must carry change_id for commit change refs");
    let change_refs = change_refs_by_branch
        .entry(row.branch_id.clone())
        .or_insert_with(|| {
            let timestamp = functions.call_timestamp();
            StagedCommitChangeRefs::new(
                CommitId::from(functions.call_uuid_v7()),
                ChangeId::from(functions.call_uuid_v7()),
                ChangeId::from(functions.call_uuid_v7()),
                timestamp,
            )
        });
    row.commit_id = Some(change_refs.commit_id);
    change_refs.add_change_id(change_id);
}

fn remove_row_from_commit_change_refs(
    change_refs_by_branch: &mut BTreeMap<String, StagedCommitChangeRefs>,
    row: &PreparedStateRow,
) {
    if row.untracked {
        return;
    }
    let Some(change_refs) = change_refs_by_branch.get_mut(&row.branch_id) else {
        return;
    };
    let Some(change_id) = row.change_id.as_ref() else {
        return;
    };
    change_refs.remove_change_id(change_id);
    if change_refs.is_empty() {
        change_refs_by_branch.remove(&row.branch_id);
    }
}

fn staged_row_identity_matches_scan(
    row: &PreparedStateRow,
    request: &LiveStateScanRequest,
) -> bool {
    if !request.filter.schema_keys.is_empty()
        && !request.filter.schema_keys.contains(&row.schema_key)
    {
        return false;
    }
    if !request.filter.entity_pks.is_empty() && !request.filter.entity_pks.contains(&row.entity_pk)
    {
        return false;
    }
    if !staged_branch_matches_scan(&row.branch_id, request) {
        return false;
    }
    if request
        .filter
        .untracked
        .is_some_and(|untracked| row.untracked != untracked)
    {
        return false;
    }
    nullable_key_matches_filters(&row.file_id, &request.filter.file_ids)
}

fn nullable_key_matches_filters(
    value: &Option<String>,
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

fn nullable_key_matches_filter(value: &Option<String>, filter: &NullableKeyFilter<String>) -> bool {
    match filter {
        NullableKeyFilter::Any => true,
        NullableKeyFilter::Null => value.is_none(),
        NullableKeyFilter::Value(expected) => value.as_ref() == Some(expected),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_state::{LiveStateFilter, LiveStateRowRequest};

    #[tokio::test]
    async fn staging_overlay_uses_last_staged_row_for_exact_load() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![state_row("sql2-duplicate-key", "first")],
            })
            .expect("initial row should stage");
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![state_row("sql2-duplicate-key", "second")],
            })
            .expect("staging rows should succeed");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged rows");
        let row = overlay
            .load_exact(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: "global".to_string(),
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

    #[tokio::test]
    async fn staging_overlay_scan_returns_only_latest_row_per_identity() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![state_row("sql2-duplicate-key", "first")],
            })
            .expect("initial row should stage");
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![state_row("sql2-duplicate-key", "second")],
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
                rows: vec![
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
                rows: vec![
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
                rows: vec![
                    state_row("sql2-key-a", "first"),
                    state_row("sql2-key-b", "only"),
                ],
            })
            .expect("initial rows should stage");
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![state_row("sql2-key-a", "second")],
            })
            .expect("staging rows should succeed");

        let drained = staged_writes.drain().expect("drain should succeed");

        assert_eq!(drained.state_rows.len(), 2);
        assert!(drained.state_rows.iter().any(|row| {
            row.entity_pk == EntityPk::single("sql2-key-a")
                && row
                    .snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.normalized.as_ref())
                    == Some("{\"key\":\"sql2-key-a\",\"value\":\"second\"}")
        }));
        assert!(drained.state_rows.iter().any(|row| {
            row.entity_pk == EntityPk::single("sql2-key-b")
                && row
                    .snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.normalized.as_ref())
                    == Some("{\"key\":\"sql2-key-b\",\"value\":\"only\"}")
        }));
    }

    #[tokio::test]
    async fn staged_writes_drain_preserves_file_data_payloads() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::RowsWithFileData {
                mode: TransactionWriteMode::Replace,
                rows: vec![state_row("file-readme", "descriptor")],
                file_data: vec![TransactionFileData::new(
                    "file-readme".to_string(),
                    Some("/readme.md".to_string()),
                    Some("readme.md".to_string()),
                    "global".to_string(),
                    true,
                    true,
                    b"hello".to_vec(),
                )],
                count: 1,
            })
            .expect("staging rows with file data should succeed");

        let drained = staged_writes.drain().expect("drain should succeed");

        assert_eq!(drained.state_rows.len(), 1);
        assert_eq!(drained.file_data_writes.len(), 1);
        assert_eq!(drained.file_data_writes[0].file_id, "file-readme");
        assert_eq!(drained.file_data_writes[0].data(), b"hello");
    }

    #[tokio::test]
    async fn staged_writes_track_commit_members_for_tracked_global_rows() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![state_row("tracked-key", "value").with_tracked()],
            })
            .expect("tracked global row should stage");

        let drained = staged_writes.drain().expect("drain should succeed");
        let change_refs = drained
            .commit_change_refs_by_branch
            .get("global")
            .expect("global commit change_refs should exist");
        assert_eq!(
            change_refs.change_ids.iter().cloned().collect::<Vec<_>>(),
            vec![labeled_change_id("test-change-id")]
        );
    }

    #[tokio::test]
    async fn staged_writes_do_not_track_untracked_rows_as_commit_members() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![state_row("untracked-key", "value")],
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
                rows: vec![
                    state_row("overwrite-key", "first")
                        .with_tracked()
                        .with_change_id("change-first"),
                ],
            })
            .expect("initial tracked row should stage");
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![
                    state_row("overwrite-key", "second")
                        .with_tracked()
                        .with_change_id("change-second"),
                ],
            })
            .expect("tracked overwrite should stage");

        let drained = staged_writes.drain().expect("drain should succeed");
        let change_refs = drained
            .commit_change_refs_by_branch
            .get("global")
            .expect("global commit change_refs should exist");
        assert_eq!(
            change_refs.change_ids.iter().cloned().collect::<Vec<_>>(),
            vec![labeled_change_id("change-second")]
        );
    }

    #[tokio::test]
    async fn staged_writes_reject_mixed_durability_in_one_batch() {
        let staged_writes = test_staged_writes();

        let error = staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![
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
                rows: vec![
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

    #[tokio::test]
    async fn staged_writes_reject_mixed_durability_across_calls() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![state_row("shared-domain-key", "tracked").with_tracked()],
            })
            .expect("tracked row should stage");
        let error = staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![state_row("shared-domain-key", "untracked")],
            })
            .expect_err("durability switch should fail");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        let drained = staged_writes.drain().expect("drain should succeed");
        assert_eq!(drained.state_rows.len(), 1);
        assert!(!drained.state_rows[0].untracked);
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
                    rows: vec![row],
                })
                .expect("alternating row should stage");
        }

        let drained = staged_writes.drain().expect("drain should succeed");
        assert_eq!(drained.state_rows.len(), 1);
        assert!(!drained.state_rows[0].untracked);
        assert_eq!(
            drained.state_rows[0]
                .snapshot
                .as_ref()
                .map(crate::transaction::types::StageJson::materialize)
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
                rows: vec![
                    state_row("active-branch-key", "value")
                        .with_tracked()
                        .with_branch("branch-a"),
                ],
            })
            .expect("active-branch tracked staging should accumulate change_refs");

        let drained = staged_writes.drain().expect("drain should succeed");
        let change_refs = drained
            .commit_change_refs_by_branch
            .get("branch-a")
            .expect("active-branch commit change_refs should exist");
        assert_eq!(
            change_refs.change_ids.iter().cloned().collect::<Vec<_>>(),
            vec![labeled_change_id("test-change-id")]
        );
    }

    #[tokio::test]
    async fn staged_writes_reject_global_rows_with_non_global_branch_id() {
        let staged_writes = test_staged_writes();

        let error = staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![{
                    let mut row = state_row("invalid-global-key", "value");
                    row.branch_id = "branch-a".to_string();
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
                rows: vec![state_row("shared-entity", "base")],
            })
            .expect("initial same-identity row should stage");
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![
                    state_row("shared-entity", "latest"),
                    state_row("shared-entity", "other-branch").with_branch("branch-b"),
                    state_row("shared-entity", "other-schema").with_schema("other_schema"),
                    state_row("shared-entity", "other-file").with_file_id("file-a"),
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
                    && row.branch_id == "global"
                    && row.schema_key == "lix_key_value"
                    && row.file_id.is_none())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn staged_writes_use_injected_function_provider_for_commit_metadata() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![state_row("sql2-functions-key", "value").with_tracked()],
            })
            .expect("staging rows should succeed");

        let drained = staged_writes.drain().expect("drain should succeed");
        let change_refs = drained
            .commit_change_refs_by_branch
            .get("global")
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
                rows: vec![state_row("tracked-commit-key", "value").with_tracked()],
            })
            .expect("tracked row should stage");

        let drained = staged_writes.drain().expect("drain should succeed");
        assert_eq!(drained.state_rows.len(), 1);
        assert_eq!(drained.state_rows[0].commit_id, Some(test_commit_id(1)));
        assert_eq!(
            drained
                .commit_change_refs_by_branch
                .get("global")
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

    fn labeled_change_id(label: &str) -> ChangeId {
        ChangeId::for_test_label(label)
    }

    fn state_row(key: &str, value: &str) -> PreparedStateRow {
        let snapshot = stage_json_from_value(
            TransactionJson::from_value_for_test(serde_json::json!({ "key": key, "value": value })),
            "test staged row snapshot_content",
        )
        .expect("test snapshot should prepare");
        PreparedStateRow {
            schema_plan_id: SchemaPlanId::for_test(0),
            facts: crate::transaction::types::PreparedRowFacts::default(),
            entity_pk: EntityPk::single(key),
            schema_key: "lix_key_value".to_string(),
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
            branch_id: "global".to_string(),
        }
    }

    fn tombstone_row(key: &str) -> PreparedStateRow {
        let mut row = state_row(key, "deleted");
        row.snapshot = None;
        row
    }

    fn exact_request_for_key(key: &str) -> LiveStateRowRequest {
        LiveStateRowRequest {
            schema_key: "lix_key_value".to_string(),
            branch_id: "global".to_string(),
            entity_pk: EntityPk::single(key),
            file_id: NullableKeyFilter::Null,
        }
    }

    fn scan_request_for_key(key: &str, include_tombstones: bool) -> LiveStateScanRequest {
        LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec!["lix_key_value".to_string()],
                entity_pks: vec![EntityPk::single(key)],
                branch_ids: vec!["global".to_string()],
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

    impl StateRowTestExt for PreparedStateRow {
        fn with_schema(mut self, schema_key: &str) -> Self {
            self.schema_key = schema_key.to_string();
            self
        }

        fn with_file_id(mut self, file_id: &str) -> Self {
            self.file_id = Some(file_id.to_string());
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
            self.branch_id = branch_id.to_string();
            self.global = branch_id == GLOBAL_BRANCH_ID;
            self
        }

        fn with_change_id(mut self, change_id: &str) -> Self {
            self.change_id = Some(ChangeId::for_test_label(change_id));
            self
        }
    }
}
