use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use crate::catalog::SchemaPlanId;
use crate::domain::{Domain, DomainRowIdentity};
use crate::entity_identity::EntityIdentity;
use crate::functions::{FunctionProvider, FunctionProviderHandle};
#[cfg(test)]
use crate::live_state::LiveStateRowRequest;
use crate::live_state::{LiveStateScanRequest, MaterializedLiveStateRow};
#[cfg(test)]
use crate::transaction::types::{stage_json_from_value, TransactionJson};
use crate::transaction::types::{
    LogicalPrimaryKey, PreparedTransactionWrite, TransactionFileData, TransactionWriteMode,
    TransactionWriteOperation, TransactionWriteOrigin, TransactionWriteOutcome,
};
use crate::transaction::types::{PreparedAdoptedStateRow, PreparedStateRow, StagedCommitMembers};
use crate::GLOBAL_VERSION_ID;
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
    adopted_rows: Mutex<Vec<Option<PreparedAdoptedStateRow>>>,
    by_identity: Mutex<HashMap<PreparedStateRowIdentity, RowSlot>>,
    insert_identities: Mutex<BTreeMap<PreparedStateRowIdentity, Option<TransactionWriteOrigin>>>,
    commit_members_by_version: Mutex<BTreeMap<String, StagedCommitMembers>>,
    extra_commit_parents_by_version: Mutex<BTreeMap<String, Vec<String>>>,
    file_data_writes: Mutex<Vec<TransactionFileData>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowSlot {
    State(usize),
    Adopted(usize),
}

/// Drained prepared transaction writes ready for commit.
pub(crate) struct PreparedWriteSet {
    pub(crate) state_rows: Vec<PreparedStateRow>,
    pub(crate) adopted_rows: Vec<PreparedAdoptedStateRow>,
    pub(crate) insert_identities:
        BTreeMap<PreparedStateRowIdentity, Option<TransactionWriteOrigin>>,
    pub(crate) commit_members_by_version: BTreeMap<String, StagedCommitMembers>,
    pub(crate) extra_commit_parents_by_version: BTreeMap<String, Vec<String>>,
    pub(crate) file_data_writes: Vec<TransactionFileData>,
}

pub(crate) struct PreparedWriteValidationSet<'a> {
    rows: Vec<PreparedValidationRow<'a>>,
    constraint_rows: Vec<PreparedValidationRow<'a>>,
    insert_identities: Vec<(
        &'a PreparedStateRowIdentity,
        Option<&'a TransactionWriteOrigin>,
    )>,
}

pub(crate) struct PreparedWriteValidationIndex<'a> {
    rows_by_schema_scope: BTreeMap<Domain, Vec<PreparedValidationRow<'a>>>,
    insert_identities_by_schema_scope: BTreeMap<
        Domain,
        Vec<(
            &'a PreparedStateRowIdentity,
            Option<&'a TransactionWriteOrigin>,
        )>,
    >,
}

#[derive(Clone, Copy)]
pub(crate) enum PreparedValidationRow<'a> {
    State(&'a PreparedStateRow),
    Adopted(&'a PreparedAdoptedStateRow),
}

impl<'a> PreparedValidationRow<'a> {
    pub(crate) fn entity_id(&self) -> &EntityIdentity {
        match self {
            Self::State(row) => &row.entity_id,
            Self::Adopted(row) => &row.entity_id,
        }
    }

    pub(crate) fn schema_plan_id(&self) -> SchemaPlanId {
        match self {
            Self::State(row) => row.schema_plan_id,
            Self::Adopted(row) => row.schema_plan_id,
        }
    }

    pub(crate) fn schema_key(&self) -> &str {
        match self {
            Self::State(row) => &row.schema_key,
            Self::Adopted(row) => &row.schema_key,
        }
    }

    pub(crate) fn file_id(&self) -> &Option<String> {
        match self {
            Self::State(row) => &row.file_id,
            Self::Adopted(row) => &row.file_id,
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot_content(&self) -> Option<&str> {
        match self {
            Self::State(row) => row
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.normalized.as_ref()),
            Self::Adopted(row) => row
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
            Self::Adopted(row) => row
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
            Self::Adopted(row) => row
                .metadata
                .as_ref()
                .map(|metadata| metadata.value.as_ref()),
        }
    }

    pub(crate) fn untracked(&self) -> bool {
        match self {
            Self::State(row) => row.untracked,
            Self::Adopted(_) => false,
        }
    }

    pub(crate) fn version_id(&self) -> &str {
        match self {
            Self::State(row) => &row.version_id,
            Self::Adopted(row) => &row.version_id,
        }
    }

    pub(crate) fn domain(&self) -> Domain {
        Domain::exact_file(
            self.version_id().to_string(),
            self.untracked(),
            self.file_id().clone(),
        )
    }

    pub(crate) fn domain_row_identity(&self) -> DomainRowIdentity {
        DomainRowIdentity::in_domain(
            self.domain(),
            self.schema_key().to_string(),
            self.entity_id().clone(),
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
                        || (row.snapshot_json().is_none()
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
    ) -> impl Iterator<Item = (&PreparedStateRowIdentity, Option<&TransactionWriteOrigin>)> {
        self.insert_identities
            .iter()
            .map(|(identity, origin)| (*identity, *origin))
    }
}

impl PreparedWriteSet {
    #[cfg(test)]
    pub(crate) fn validation_rows(&self) -> impl Iterator<Item = PreparedValidationRow<'_>> + '_ {
        self.state_rows
            .iter()
            .map(PreparedValidationRow::State)
            .chain(self.adopted_rows.iter().map(PreparedValidationRow::Adopted))
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
        for row in &self.adopted_rows {
            let row = PreparedValidationRow::Adopted(row);
            rows_by_schema_scope
                .entry(row.domain().schema_catalog_domain())
                .or_default()
                .push(row);
        }

        let mut insert_identities_by_schema_scope = BTreeMap::<
            Domain,
            Vec<(&PreparedStateRowIdentity, Option<&TransactionWriteOrigin>)>,
        >::new();
        for (identity, origin) in &self.insert_identities {
            insert_identities_by_schema_scope
                .entry(identity.domain().schema_catalog_domain())
                .or_default()
                .push((identity, origin.as_ref()));
        }

        PreparedWriteValidationIndex {
            rows_by_schema_scope,
            insert_identities_by_schema_scope,
        }
    }

    #[cfg(test)]
    pub(crate) fn validation_set_for_tests(&self) -> PreparedWriteValidationSet<'_> {
        let rows: Vec<_> = self.validation_rows().collect();
        let insert_identities = self
            .insert_identities
            .iter()
            .map(|(identity, origin)| (identity, origin.as_ref()))
            .collect();
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
            adopted_rows: Mutex::new(Vec::new()),
            by_identity: Mutex::new(HashMap::new()),
            insert_identities: Mutex::new(BTreeMap::new()),
            commit_members_by_version: Mutex::new(BTreeMap::new()),
            extra_commit_parents_by_version: Mutex::new(BTreeMap::new()),
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
        let mut adopted_rows_guard = self.adopted_rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged adopted writes lock",
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
        let mut commit_members_guard = self.commit_members_by_version.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged commit membership lock",
            )
        })?;
        let mut extra_parents_guard =
            self.extra_commit_parents_by_version.lock().map_err(|_| {
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
            adopted_rows: std::mem::take(&mut *adopted_rows_guard)
                .into_iter()
                .flatten()
                .collect(),
            insert_identities: std::mem::take(&mut *insert_identities_guard),
            commit_members_by_version: std::mem::take(&mut *commit_members_guard),
            extra_commit_parents_by_version: std::mem::take(&mut *extra_parents_guard),
            file_data_writes: std::mem::take(&mut *file_data_guard),
        });
        by_identity_guard.clear();
        result
    }

    /// Records an additional parent for the commit generated for `version_id`.
    ///
    /// Normal writes parent the new commit to the version's previous head.
    /// Merges add the source version head as an extra parent so the commit graph
    /// preserves branch ancestry while tracked-state roots still apply source
    /// rows onto the target root.
    pub(crate) fn add_commit_parent(
        &self,
        version_id: String,
        parent_commit_id: String,
    ) -> Result<(), LixError> {
        let mut guard = self.extra_commit_parents_by_version.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged extra commit parents lock",
            )
        })?;
        let parents = guard.entry(version_id).or_default();
        if !parents.contains(&parent_commit_id) {
            parents.push(parent_commit_id);
        }
        Ok(())
    }

    pub(crate) fn staged_commit_id(&self, version_id: &str) -> Result<Option<String>, LixError> {
        let guard = self.commit_members_by_version.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged commit membership lock",
            )
        })?;
        Ok(guard
            .get(version_id)
            .map(|members| members.commit_id.clone()))
    }

    /// Stages a commit for `version_id` even if no tracked state rows changed.
    ///
    /// Merge uses this to record graph ancestry for convergent merges where the
    /// target already has the same final state as the source, but the source
    /// head is not reachable from the target head.
    pub(crate) fn stage_empty_commit(&self, version_id: String) -> Result<String, LixError> {
        let mut functions = self.functions.clone();
        let mut guard = self.commit_members_by_version.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged commit membership lock",
            )
        })?;
        let members = guard.entry(version_id).or_insert_with(|| {
            StagedCommitMembers::new(
                functions.uuid_v7(),
                functions.uuid_v7(),
                functions.timestamp(),
            )
        });
        members.allow_empty();
        Ok(members.commit_id.clone())
    }

    /// Builds the transaction-local read overlay from currently staged writes.
    pub(crate) fn staging_overlay(self: &Arc<Self>) -> Result<PreparedStateRowOverlay, LixError> {
        Ok(PreparedStateRowOverlay {
            staged_writes: Arc::clone(self),
        })
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
            PreparedTransactionWrite::AdoptedChanges { rows } => (None, rows.len() as u64),
        };
        let mut functions = self.functions.clone();
        let (rows, adopted_rows, file_data_writes) = self.state_rows_from_stage_write(write)?;
        for row in &rows {
            validate_commit_membership_support(row)?;
        }
        for row in &adopted_rows {
            validate_adopted_commit_membership_support(row)?;
        }
        reject_duplicate_present_rows_in_batch(&rows)?;
        let mut guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let mut adopted_guard = self.adopted_rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged adopted writes lock",
            )
        })?;
        let mut by_identity_guard = self.by_identity.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged identity index lock",
            )
        })?;
        let mut commit_members_guard = self.commit_members_by_version.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged commit membership lock",
            )
        })?;
        let mut insert_identities_guard = self.insert_identities.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged insert identity lock",
            )
        })?;
        for mut row in rows {
            let identity = PreparedStateRowIdentity::from(&row);
            if mode == Some(TransactionWriteMode::Insert)
                && by_identity_guard.contains_key(&identity)
            {
                return Err(duplicate_insert_identity_error(&row));
            }
            if matches!(by_identity_guard.get(&identity), Some(RowSlot::Adopted(_))) {
                return Err(conflicting_adopted_identity_error(&row));
            }
            let existing_slot = by_identity_guard.remove(&identity);
            if let Some(RowSlot::State(index)) = existing_slot {
                if let Some(previous) = guard.get_mut(index).and_then(Option::take) {
                    remove_row_from_commit_members(&mut commit_members_guard, &previous);
                }
            }
            add_row_to_commit_members(&mut commit_members_guard, &mut row, &mut functions);
            let identity = PreparedStateRowIdentity::from(&row);
            if mode == Some(TransactionWriteMode::Insert) {
                insert_identities_guard.insert(identity.clone(), row.origin.clone());
            }
            let slot = match existing_slot {
                Some(RowSlot::State(index)) => {
                    guard[index] = Some(row);
                    RowSlot::State(index)
                }
                _ => {
                    let index = guard.len();
                    guard.push(Some(row));
                    RowSlot::State(index)
                }
            };
            by_identity_guard.insert(identity, slot);
        }
        for mut row in adopted_rows {
            let identity = PreparedStateRowIdentity::from(&row);
            if by_identity_guard.contains_key(&identity) {
                return Err(conflicting_adopted_projection_error(&row));
            }
            add_adopted_row_to_commit_members(&mut commit_members_guard, &mut row, &mut functions);
            let identity = PreparedStateRowIdentity::from(&row);
            let index = adopted_guard.len();
            adopted_guard.push(Some(row));
            by_identity_guard.insert(identity, RowSlot::Adopted(index));
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
    ) -> Result<
        (
            Vec<PreparedStateRow>,
            Vec<PreparedAdoptedStateRow>,
            Vec<TransactionFileData>,
        ),
        LixError,
    > {
        let mut state_rows = Vec::new();
        let mut adopted_rows = Vec::new();
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
            PreparedTransactionWrite::AdoptedChanges { rows } => {
                adopted_rows.extend(rows);
            }
        }
        Ok((state_rows, adopted_rows, file_data_writes))
    }
}

/// Read overlay derived from staged transaction writes.
pub(crate) struct PreparedStateRowOverlay {
    staged_writes: Arc<TransactionWriteBuffer>,
}

pub(crate) struct StagedScanParts {
    pub(crate) rows: Vec<MaterializedLiveStateRow>,
    pub(crate) hidden_identities: BTreeSet<PreparedStateRowIdentity>,
}

impl PreparedStateRowOverlay {
    /// Returns staged rows visible for a scan request.
    #[cfg(test)]
    pub(crate) fn scan(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        Ok(self.scan_parts(request)?.rows)
    }

    /// Returns staged rows and base-row identities hidden by staged rows in one pass.
    ///
    /// Tombstones hide base rows even when the request does not include
    /// tombstone rows in the visible result set.
    pub(crate) fn scan_parts(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<StagedScanParts, LixError> {
        if request.filter.no_match {
            return Ok(StagedScanParts {
                rows: Vec::new(),
                hidden_identities: BTreeSet::new(),
            });
        }

        let rows_guard = self.staged_writes.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let adopted_guard = self.staged_writes.adopted_rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged adopted writes lock",
            )
        })?;
        let by_identity_guard = self.staged_writes.by_identity.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged identity index lock",
            )
        })?;

        let mut rows = Vec::new();
        let mut hidden_identities = BTreeSet::new();
        for (identity, slot) in by_identity_guard.iter() {
            match *slot {
                RowSlot::State(index) => {
                    let Some(row) = rows_guard.get(index).and_then(Option::as_ref) else {
                        continue;
                    };
                    if !staged_row_identity_matches_scan(row, request) {
                        continue;
                    }
                    hidden_identities.insert(identity.clone());
                    if row.snapshot.is_some() || request.filter.include_tombstones {
                        rows.push(MaterializedLiveStateRow::from(row));
                    }
                }
                RowSlot::Adopted(index) => {
                    let Some(row) = adopted_guard.get(index).and_then(Option::as_ref) else {
                        continue;
                    };
                    if !adopted_row_identity_matches_scan(row, request) {
                        continue;
                    }
                    hidden_identities.insert(identity.clone());
                    if row.snapshot.is_some() || request.filter.include_tombstones {
                        rows.push(MaterializedLiveStateRow::from(row));
                    }
                }
            }
        }
        Ok(StagedScanParts {
            rows,
            hidden_identities,
        })
    }

    /// Returns a staged exact-row answer, if this transaction has one.
    #[cfg(test)]
    pub(crate) fn load_exact(&self, request: &LiveStateRowRequest) -> Option<StagedExactRow> {
        let untracked_identity = PreparedStateRowIdentity::from_exact_request(request, true)?;
        if let Some(row) = self.load_state_slot(&untracked_identity) {
            return Some(if row.snapshot.is_none() {
                StagedExactRow::Tombstone
            } else {
                StagedExactRow::Row(MaterializedLiveStateRow::from(&row))
            });
        }

        let identity = PreparedStateRowIdentity::from_exact_request(request, false)?;
        if let Some(row) = self.load_state_slot(&identity) {
            return Some(if row.snapshot.is_none() {
                StagedExactRow::Tombstone
            } else {
                StagedExactRow::Row(MaterializedLiveStateRow::from(&row))
            });
        }
        self.load_adopted_slot(&identity).map(|row| {
            if row.snapshot.is_none() {
                StagedExactRow::Tombstone
            } else {
                StagedExactRow::Row(MaterializedLiveStateRow::from(&row))
            }
        })
    }

    #[cfg(test)]
    fn load_state_slot(&self, identity: &PreparedStateRowIdentity) -> Option<PreparedStateRow> {
        let rows_guard = self.staged_writes.rows.lock().ok()?;
        let _adopted_guard = self.staged_writes.adopted_rows.lock().ok()?;
        let by_identity_guard = self.staged_writes.by_identity.lock().ok()?;
        let Some(RowSlot::State(index)) = by_identity_guard.get(identity).copied() else {
            return None;
        };
        rows_guard.get(index)?.as_ref().cloned()
    }

    #[cfg(test)]
    fn load_adopted_slot(
        &self,
        identity: &PreparedStateRowIdentity,
    ) -> Option<PreparedAdoptedStateRow> {
        let _rows_guard = self.staged_writes.rows.lock().ok()?;
        let adopted_guard = self.staged_writes.adopted_rows.lock().ok()?;
        let by_identity_guard = self.staged_writes.by_identity.lock().ok()?;
        let Some(RowSlot::Adopted(index)) = by_identity_guard.get(identity).copied() else {
            return None;
        };
        adopted_guard.get(index)?.as_ref().cloned()
    }
}

#[cfg(test)]
pub(crate) enum StagedExactRow {
    Row(MaterializedLiveStateRow),
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PreparedStateRowIdentity {
    untracked: bool,
    schema_key: String,
    entity_id: crate::entity_identity::EntityIdentity,
    file_id: Option<String>,
    version_id: String,
}

impl PreparedStateRowIdentity {
    fn from_staged_row(row: &PreparedStateRow) -> Self {
        Self {
            untracked: row.untracked,
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
            version_id: row.version_id.clone(),
        }
    }

    #[cfg(test)]
    fn from_exact_request(request: &LiveStateRowRequest, untracked: bool) -> Option<Self> {
        let file_id = match &request.file_id {
            NullableKeyFilter::Null => None,
            NullableKeyFilter::Value(value) => Some(value.clone()),
            // Exact overlay lookup requires a concrete row identity.
            NullableKeyFilter::Any => return None,
        };
        Some(Self {
            untracked,
            schema_key: request.schema_key.clone(),
            entity_id: request.entity_id.clone(),
            file_id,
            version_id: request.version_id.clone(),
        })
    }

    pub(crate) fn schema_key(&self) -> &str {
        &self.schema_key
    }

    pub(crate) fn entity_id(&self) -> &crate::entity_identity::EntityIdentity {
        &self.entity_id
    }

    pub(crate) fn domain(&self) -> Domain {
        Domain::exact_file(
            self.version_id.clone(),
            self.untracked,
            self.file_id.clone(),
        )
    }
}

impl From<&PreparedStateRow> for PreparedStateRowIdentity {
    fn from(row: &PreparedStateRow) -> Self {
        Self::from_staged_row(row)
    }
}

impl From<&PreparedAdoptedStateRow> for PreparedStateRowIdentity {
    fn from(row: &PreparedAdoptedStateRow) -> Self {
        Self {
            untracked: false,
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
            version_id: row.version_id.clone(),
        }
    }
}

impl From<&MaterializedLiveStateRow> for PreparedStateRowIdentity {
    fn from(row: &MaterializedLiveStateRow) -> Self {
        Self {
            untracked: row.untracked,
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
            version_id: row.version_id.clone(),
        }
    }
}

fn validate_commit_membership_support(row: &PreparedStateRow) -> Result<(), LixError> {
    if row.global && row.version_id != GLOBAL_VERSION_ID {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine global staged rows must use the global version id",
        ));
    }
    Ok(())
}

fn validate_adopted_commit_membership_support(
    row: &PreparedAdoptedStateRow,
) -> Result<(), LixError> {
    if row.global && row.version_id != GLOBAL_VERSION_ID {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine global adopted rows must use the global version id",
        ));
    }
    Ok(())
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
                "primary-key constraint violation on schema '{}': duplicate staged rows for entity_id '{}' in version '{}'",
                row.schema_key,
                previous
                    .entity_id
                    .as_json_array_text()
                    .unwrap_or_else(|_| "<invalid entity_id>".to_string()),
                row.version_id
            )
        });
    LixError::new(LixError::CODE_UNIQUE, message)
}

pub(crate) fn duplicate_insert_identity_message(
    schema_key: &str,
    entity_id: &crate::entity_identity::EntityIdentity,
    version_id: Option<&str>,
    origin: Option<&TransactionWriteOrigin>,
) -> String {
    if let Some(message) = logical_primary_key_violation_message(origin) {
        return message;
    }
    let entity_id = entity_id
        .as_json_array_text()
        .unwrap_or_else(|_| "<invalid entity_id>".to_string());
    match version_id {
        Some(version_id) => format!(
            "primary-key constraint violation on schema '{schema_key}': INSERT would duplicate entity_id '{entity_id}' in version '{version_id}'"
        ),
        None => format!(
            "primary-key constraint violation on schema '{schema_key}': INSERT would duplicate entity_id '{entity_id}'"
        ),
    }
}

fn duplicate_insert_identity_error(row: &PreparedStateRow) -> LixError {
    let message = duplicate_insert_identity_message(
        &row.schema_key,
        &row.entity_id,
        Some(&row.version_id),
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

fn conflicting_adopted_identity_error(row: &PreparedStateRow) -> LixError {
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "transaction cannot stage a new row and an adopted projection for schema '{}' entity_id '{}' in version '{}'",
            row.schema_key,
            row.entity_id
                .as_json_array_text()
                .unwrap_or_else(|_| "<invalid entity_id>".to_string()),
            row.version_id
        ),
    )
}

fn conflicting_adopted_projection_error(row: &PreparedAdoptedStateRow) -> LixError {
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "transaction cannot stage duplicate adopted projections for schema '{}' entity_id '{}' in version '{}'",
            row.schema_key,
            row.entity_id
                .as_json_array_text()
                .unwrap_or_else(|_| "<invalid entity_id>".to_string()),
            row.version_id
        ),
    )
}

fn add_row_to_commit_members(
    members_by_version: &mut BTreeMap<String, StagedCommitMembers>,
    row: &mut PreparedStateRow,
    functions: &mut dyn FunctionProvider,
) {
    if row.untracked {
        return;
    }
    let change_id = row
        .change_id
        .clone()
        .expect("tracked staged rows must carry change_id for commit membership");
    let members = members_by_version
        .entry(row.version_id.clone())
        .or_insert_with(|| {
            StagedCommitMembers::new(
                functions.uuid_v7(),
                functions.uuid_v7(),
                functions.timestamp(),
            )
        });
    row.commit_id = Some(members.commit_id.clone());
    members.add_change_id(change_id);
}

fn add_adopted_row_to_commit_members(
    members_by_version: &mut BTreeMap<String, StagedCommitMembers>,
    row: &mut PreparedAdoptedStateRow,
    functions: &mut dyn FunctionProvider,
) {
    let members = members_by_version
        .entry(row.version_id.clone())
        .or_insert_with(|| {
            StagedCommitMembers::new(
                functions.uuid_v7(),
                functions.uuid_v7(),
                functions.timestamp(),
            )
        });
    row.commit_id = members.commit_id.clone();
    members.add_change_id(row.change_id.clone());
}

fn remove_row_from_commit_members(
    members_by_version: &mut BTreeMap<String, StagedCommitMembers>,
    row: &PreparedStateRow,
) {
    if row.untracked {
        return;
    }
    let Some(members) = members_by_version.get_mut(&row.version_id) else {
        return;
    };
    let Some(change_id) = row.change_id.as_deref() else {
        return;
    };
    members.remove_change_id(change_id);
    if members.is_empty() {
        members_by_version.remove(&row.version_id);
    }
}

fn adopted_row_identity_matches_scan(
    row: &PreparedAdoptedStateRow,
    request: &LiveStateScanRequest,
) -> bool {
    if !request.filter.schema_keys.is_empty()
        && !request.filter.schema_keys.contains(&row.schema_key)
    {
        return false;
    }
    if !request.filter.entity_ids.is_empty() && !request.filter.entity_ids.contains(&row.entity_id)
    {
        return false;
    }
    if !request.filter.version_ids.is_empty()
        && !request.filter.version_ids.contains(&row.version_id)
    {
        return false;
    }
    if request.filter.untracked == Some(true) {
        return false;
    }
    nullable_key_matches_filters(&row.file_id, &request.filter.file_ids)
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
    if !request.filter.entity_ids.is_empty() && !request.filter.entity_ids.contains(&row.entity_id)
    {
        return false;
    }
    if !request.filter.version_ids.is_empty()
        && !request.filter.version_ids.contains(&row.version_id)
    {
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
    use crate::functions::SharedFunctionProvider;
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
                version_id: "global".to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("sql2-duplicate-key"),
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
        assert!(overlay
            .scan(&scan_request_for_key("sql2-delete-key", false))
            .expect("overlay scan should succeed")
            .is_empty());

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
            row.entity_id == crate::entity_identity::EntityIdentity::single("sql2-key-a")
                && row
                    .snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.normalized.as_ref())
                    == Some("{\"key\":\"sql2-key-a\",\"value\":\"second\"}")
        }));
        assert!(drained.state_rows.iter().any(|row| {
            row.entity_id == crate::entity_identity::EntityIdentity::single("sql2-key-b")
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
                file_data: vec![TransactionFileData {
                    file_id: "file-readme".to_string(),
                    version_id: "global".to_string(),
                    untracked: true,
                    data: b"hello".to_vec(),
                }],
                count: 1,
            })
            .expect("staging rows with file data should succeed");

        let drained = staged_writes.drain().expect("drain should succeed");

        assert_eq!(drained.state_rows.len(), 1);
        assert_eq!(drained.file_data_writes.len(), 1);
        assert_eq!(drained.file_data_writes[0].file_id, "file-readme");
        assert_eq!(drained.file_data_writes[0].data, b"hello");
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
        let members = drained
            .commit_members_by_version
            .get("global")
            .expect("global commit members should exist");
        assert_eq!(
            members.change_ids.iter().cloned().collect::<Vec<_>>(),
            vec!["test-change-id".to_string()]
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
        assert!(drained.commit_members_by_version.is_empty());
    }

    #[tokio::test]
    async fn staged_writes_replace_commit_member_on_tracked_overwrite() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![state_row("overwrite-key", "first")
                    .with_tracked()
                    .with_change_id("change-first")],
            })
            .expect("initial tracked row should stage");
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![state_row("overwrite-key", "second")
                    .with_tracked()
                    .with_change_id("change-second")],
            })
            .expect("tracked overwrite should stage");

        let drained = staged_writes.drain().expect("drain should succeed");
        let members = drained
            .commit_members_by_version
            .get("global")
            .expect("global commit members should exist");
        assert_eq!(
            members.change_ids.iter().cloned().collect::<Vec<_>>(),
            vec!["change-second".to_string()]
        );
    }

    #[tokio::test]
    async fn staged_writes_keep_tracked_and_untracked_domains_separate() {
        let staged_writes = test_staged_writes();

        staged_writes
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
            .expect("untracked overwrite should stage");

        let drained = staged_writes.drain().expect("drain should succeed");
        assert_eq!(drained.state_rows.len(), 2);
        assert!(drained
            .state_rows
            .iter()
            .any(|row| { row.change_id.as_deref() == Some("change-tracked") && !row.untracked }));
        assert!(drained
            .state_rows
            .iter()
            .any(|row| { row.change_id.as_deref() == Some("change-untracked") && row.untracked }));
        let members = drained
            .commit_members_by_version
            .get("global")
            .expect("tracked commit member should remain in tracked domain");
        assert_eq!(
            members.change_ids.iter().cloned().collect::<Vec<_>>(),
            vec!["change-tracked".to_string()]
        );
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
    async fn staged_writes_insert_keeps_tracked_and_untracked_rows_as_distinct_identities() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: vec![
                    state_row("shared-domain-key", "tracked").with_tracked(),
                    state_row("shared-domain-key", "untracked"),
                ],
            })
            .expect("tracked and untracked rows are distinct domain identities");

        let drained = staged_writes.drain().expect("drain should succeed");
        assert_eq!(drained.state_rows.len(), 2);
        assert!(drained.state_rows.iter().any(|row| {
            row.entity_id == crate::entity_identity::EntityIdentity::single("shared-domain-key")
                && !row.untracked
        }));
        assert!(drained.state_rows.iter().any(|row| {
            row.entity_id == crate::entity_identity::EntityIdentity::single("shared-domain-key")
                && row.untracked
        }));
    }

    #[tokio::test]
    async fn staged_writes_track_active_version_members_separately() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![state_row("active-version-key", "value")
                    .with_tracked()
                    .with_version("version-a")],
            })
            .expect("active-version tracked staging should accumulate members");

        let drained = staged_writes.drain().expect("drain should succeed");
        let members = drained
            .commit_members_by_version
            .get("version-a")
            .expect("active-version commit members should exist");
        assert_eq!(
            members.change_ids.iter().cloned().collect::<Vec<_>>(),
            vec!["test-change-id".to_string()]
        );
    }

    #[tokio::test]
    async fn staged_writes_reject_global_rows_with_non_global_version_id() {
        let staged_writes = test_staged_writes();

        let error = staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![{
                    let mut row = state_row("invalid-global-key", "value");
                    row.version_id = "version-a".to_string();
                    row
                }],
            })
            .expect_err("global row with non-global version should fail");

        assert!(error
            .message
            .contains("global staged rows must use the global version id"));
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
                    state_row("shared-entity", "base"),
                    state_row("shared-entity", "other-version").with_version("version-b"),
                    state_row("shared-entity", "other-schema").with_schema("other_schema"),
                    state_row("shared-entity", "other-file").with_file_id("file-a"),
                    state_row("shared-entity", "tracked").with_tracked(),
                ],
            })
            .expect("staging rows should succeed");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged rows");
        let rows = overlay
            .scan(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    entity_ids: vec![crate::entity_identity::EntityIdentity::single(
                        "shared-entity",
                    )],
                    include_tombstones: true,
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
            })
            .expect("overlay scan should succeed");

        assert_eq!(rows.len(), 5);
        assert_eq!(
            rows.iter()
                .filter(|row| row.entity_id
                    == crate::entity_identity::EntityIdentity::single("shared-entity")
                    && row.version_id == "global"
                    && row.schema_key == "lix_key_value"
                    && row.file_id.is_none())
                .count(),
            2
        );
        assert!(rows.iter().any(|row| {
            row.snapshot_content.as_deref()
                == Some("{\"key\":\"shared-entity\",\"value\":\"tracked\"}")
        }));
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
        let members = drained
            .commit_members_by_version
            .get("global")
            .expect("global commit members should exist");
        assert_eq!(members.commit_id, "test-uuid-1");
        assert_eq!(members.commit_change_id, "test-uuid-2");
        assert_eq!(members.created_at, "test-timestamp-1");
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
        assert_eq!(
            drained.state_rows[0].commit_id.as_deref(),
            Some("test-uuid-1")
        );
        assert_eq!(
            drained
                .commit_members_by_version
                .get("global")
                .expect("global commit members should exist")
                .commit_id,
            "test-uuid-1"
        );
    }

    fn test_staged_writes() -> Arc<TransactionWriteBuffer> {
        Arc::new(TransactionWriteBuffer::new(SharedFunctionProvider::new(
            Box::new(TestFunctionProvider::default()) as Box<dyn FunctionProvider + Send>,
        )))
    }

    #[derive(Default)]
    struct TestFunctionProvider {
        uuid_count: usize,
        timestamp_count: usize,
    }

    impl FunctionProvider for TestFunctionProvider {
        fn uuid_v7(&mut self) -> String {
            self.uuid_count += 1;
            format!("test-uuid-{}", self.uuid_count)
        }

        fn timestamp(&mut self) -> String {
            self.timestamp_count += 1;
            format!("test-timestamp-{}", self.timestamp_count)
        }
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
            entity_id: crate::entity_identity::EntityIdentity::single(key),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot: Some(snapshot),
            metadata: None,
            origin: None,
            created_at: "test-created-at".to_string(),
            updated_at: "test-updated-at".to_string(),
            global: true,
            change_id: None,
            commit_id: None,
            untracked: true,
            version_id: "global".to_string(),
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
            version_id: "global".to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single(key),
            file_id: NullableKeyFilter::Null,
        }
    }

    fn scan_request_for_key(key: &str, include_tombstones: bool) -> LiveStateScanRequest {
        LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec!["lix_key_value".to_string()],
                entity_ids: vec![crate::entity_identity::EntityIdentity::single(key)],
                version_ids: vec!["global".to_string()],
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
        fn with_version(self, version_id: &str) -> Self;
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
                self.change_id = Some("test-change-id".to_string());
            }
            self
        }

        fn with_version(mut self, version_id: &str) -> Self {
            self.version_id = version_id.to_string();
            self.global = version_id == GLOBAL_VERSION_ID;
            self
        }

        fn with_change_id(mut self, change_id: &str) -> Self {
            self.change_id = Some(change_id.to_string());
            self
        }
    }
}
