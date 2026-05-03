use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use crate::functions::{FunctionProvider, FunctionProviderHandle};
use crate::live_state::{
    LiveStateRow, LiveStateRowIdentity, LiveStateRowRequest, LiveStateScanRequest,
};
use crate::transaction::normalization::{normalize_stage_row, TransactionSchemaCatalog};
use crate::transaction::types::{
    StageAdoptedChange, StageFileData, StageRow, StageWrite, StageWriteMode, StageWriteOutcome,
};
use crate::transaction::types::{StagedAdoptedStateRow, StagedCommitMembers, StagedStateRow};
use crate::GLOBAL_VERSION_ID;
use crate::{LixError, NullableKeyFilter};

/// Transaction-local writes decoded by DataFusion provider hooks.
///
/// This is the engine2 seam between SQL execution and transaction ownership:
/// write frontends stage decoded writes here, the transaction normalizes them into
/// stable `StagedStateRow`s, reads build a `StagedStateRowOverlay` from those rows,
/// and commit later drains the same rows.
pub(crate) struct TransactionStagedWrites {
    functions: FunctionProviderHandle,
    schema_catalog: Mutex<TransactionSchemaCatalog>,
    rows: Mutex<BTreeMap<StagedStateRowIdentity, StagedStateRow>>,
    adopted_rows: Mutex<BTreeMap<StagedStateRowIdentity, StagedAdoptedStateRow>>,
    insert_identities: Mutex<BTreeSet<LiveStateRowIdentity>>,
    commit_members_by_version: Mutex<BTreeMap<String, StagedCommitMembers>>,
    extra_commit_parents_by_version: Mutex<BTreeMap<String, Vec<String>>>,
    file_data_writes: Mutex<Vec<StageFileData>>,
}

/// Drained transaction-local writes ready for commit.
pub(crate) struct StagedWriteSet {
    pub(crate) state_rows: Vec<StagedStateRow>,
    pub(crate) adopted_rows: Vec<StagedAdoptedStateRow>,
    pub(crate) insert_identities: BTreeSet<LiveStateRowIdentity>,
    pub(crate) commit_members_by_version: BTreeMap<String, StagedCommitMembers>,
    pub(crate) extra_commit_parents_by_version: BTreeMap<String, Vec<String>>,
    pub(crate) file_data_writes: Vec<StageFileData>,
}

impl StagedWriteSet {
    pub(crate) fn state_rows_for_validation(&self) -> Vec<StagedStateRow> {
        self.state_rows
            .iter()
            .cloned()
            .chain(self.adopted_rows.iter().map(StagedStateRow::from))
            .collect()
    }
}

impl TransactionStagedWrites {
    pub(crate) fn new(
        functions: FunctionProviderHandle,
        schema_catalog: TransactionSchemaCatalog,
    ) -> Self {
        Self {
            functions,
            schema_catalog: Mutex::new(schema_catalog),
            rows: Mutex::new(BTreeMap::new()),
            adopted_rows: Mutex::new(BTreeMap::new()),
            insert_identities: Mutex::new(BTreeSet::new()),
            commit_members_by_version: Mutex::new(BTreeMap::new()),
            extra_commit_parents_by_version: Mutex::new(BTreeMap::new()),
            file_data_writes: Mutex::new(Vec::new()),
        }
    }

    /// Drains staged writes for commit.
    pub(crate) fn drain(&self) -> Result<StagedWriteSet, LixError> {
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
        Ok(StagedWriteSet {
            state_rows: std::mem::take(&mut *rows_guard).into_values().collect(),
            adopted_rows: std::mem::take(&mut *adopted_rows_guard)
                .into_values()
                .collect(),
            insert_identities: std::mem::take(&mut *insert_identities_guard),
            commit_members_by_version: std::mem::take(&mut *commit_members_guard),
            extra_commit_parents_by_version: std::mem::take(&mut *extra_parents_guard),
            file_data_writes: std::mem::take(&mut *file_data_guard),
        })
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

    /// Builds the transaction-local read overlay from currently staged writes.
    pub(crate) fn staging_overlay(&self) -> Result<StagedStateRowOverlay, LixError> {
        let guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        let adopted_guard = self.adopted_rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged adopted writes lock",
            )
        })?;
        Ok(StagedStateRowOverlay::new(
            guard.clone(),
            adopted_guard.clone(),
        ))
    }

    /// Stages one decoded write batch into this transaction.
    ///
    /// This is the single hydration boundary for engine2 writes:
    /// frontends hand us `StageRow`s, and this method assigns timestamps,
    /// change ids, commit ids, and commit membership before commit routing ever
    /// sees the rows.
    pub(crate) fn stage_write(&self, write: StageWrite) -> Result<StageWriteOutcome, LixError> {
        let (mode, count) = match &write {
            StageWrite::Rows { mode, rows } => (Some(*mode), rows.len() as u64),
            StageWrite::RowsWithFileData { mode, count, .. } => (Some(*mode), *count),
            StageWrite::AdoptedChanges { changes } => (None, changes.len() as u64),
        };
        let mut functions = self.functions.clone();
        let (rows, adopted_rows, file_data_writes) =
            self.state_rows_from_stage_write(write, &mut functions)?;
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
            let identity = StagedStateRowIdentity::from(&row);
            if mode == Some(StageWriteMode::Insert)
                && (guard.contains_key(&identity)
                    || guard.contains_key(&identity.opposite_untracked())
                    || adopted_guard.contains_key(&identity))
            {
                return Err(duplicate_insert_identity_error(&row));
            }
            if adopted_guard.contains_key(&identity) {
                return Err(conflicting_adopted_identity_error(&row));
            }
            if let Some(previous) = guard.remove(&identity.opposite_untracked()) {
                remove_row_from_commit_members(&mut commit_members_guard, &previous);
            }
            if let Some(previous) = guard.remove(&identity) {
                remove_row_from_commit_members(&mut commit_members_guard, &previous);
            }
            add_row_to_commit_members(&mut commit_members_guard, &mut row, &mut functions);
            let identity = StagedStateRowIdentity::from(&row);
            if mode == Some(StageWriteMode::Insert) {
                insert_identities_guard.insert(live_state_identity_from_staged_row(&row));
            }
            guard.insert(identity, row);
        }
        for mut row in adopted_rows {
            let identity = StagedStateRowIdentity::from(&row);
            if guard.contains_key(&identity) || adopted_guard.contains_key(&identity) {
                return Err(conflicting_adopted_projection_error(&row));
            }
            add_adopted_row_to_commit_members(&mut commit_members_guard, &mut row, &mut functions);
            let identity = StagedStateRowIdentity::from(&row);
            adopted_guard.insert(identity, row);
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
        Ok(StageWriteOutcome { count })
    }

    fn state_rows_from_stage_write(
        &self,
        write: StageWrite,
        functions: &mut dyn FunctionProvider,
    ) -> Result<
        (
            Vec<StagedStateRow>,
            Vec<StagedAdoptedStateRow>,
            Vec<StageFileData>,
        ),
        LixError,
    > {
        let mut state_rows = Vec::new();
        let mut adopted_rows = Vec::new();
        let mut file_data_writes = Vec::new();
        match write {
            StageWrite::Rows { rows, .. } => {
                self.push_state_rows(&mut state_rows, rows, functions)?;
            }
            StageWrite::RowsWithFileData {
                rows, file_data, ..
            } => {
                self.push_state_rows(&mut state_rows, rows, functions)?;
                file_data_writes.extend(file_data);
            }
            StageWrite::AdoptedChanges { changes } => {
                self.push_adopted_rows(&mut adopted_rows, changes)?;
            }
        }
        Ok((state_rows, adopted_rows, file_data_writes))
    }

    fn push_state_rows(
        &self,
        state_rows: &mut Vec<StagedStateRow>,
        rows: Vec<StageRow>,
        functions: &mut dyn FunctionProvider,
    ) -> Result<(), LixError> {
        state_rows.reserve(rows.len());
        let mut schema_catalog = self.schema_catalog.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction schema catalog lock",
            )
        })?;
        for row in rows {
            let row = normalize_stage_row(row, &mut schema_catalog, self.functions.clone())?;
            state_rows.push(hydrate_state_write_row(row, functions)?);
        }
        Ok(())
    }

    fn push_adopted_rows(
        &self,
        adopted_rows: &mut Vec<StagedAdoptedStateRow>,
        changes: Vec<StageAdoptedChange>,
    ) -> Result<(), LixError> {
        adopted_rows.reserve(changes.len());
        for change in changes {
            adopted_rows.push(hydrate_adopted_state_row(change)?);
        }
        Ok(())
    }
}

/// Read overlay derived from staged transaction writes.
pub(crate) struct StagedStateRowOverlay {
    rows: BTreeMap<StagedStateRowIdentity, StagedStateRow>,
    adopted_rows: BTreeMap<StagedStateRowIdentity, StagedAdoptedStateRow>,
}

impl StagedStateRowOverlay {
    fn new(
        rows: BTreeMap<StagedStateRowIdentity, StagedStateRow>,
        adopted_rows: BTreeMap<StagedStateRowIdentity, StagedAdoptedStateRow>,
    ) -> Self {
        Self { rows, adopted_rows }
    }

    /// Returns staged rows visible for a scan request.
    pub(crate) fn scan(&self, request: &LiveStateScanRequest) -> Vec<LiveStateRow> {
        self.rows
            .values()
            .filter(|row| staged_row_matches_scan(row, request))
            .map(LiveStateRow::from)
            .chain(
                self.adopted_rows
                    .values()
                    .filter(|row| adopted_row_matches_scan(row, request))
                    .map(LiveStateRow::from),
            )
            .collect()
    }

    /// Returns staged identities that should suppress base live-state rows.
    ///
    /// Tombstones also suppress base live-state rows, even when the caller is not
    /// asking to see tombstone rows.
    pub(crate) fn identities_matching_scan(
        &self,
        request: &LiveStateScanRequest,
    ) -> BTreeSet<StagedStateRowIdentity> {
        self.rows
            .values()
            .filter(|row| staged_row_identity_matches_scan(row, request))
            .map(StagedStateRowIdentity::from)
            .chain(
                self.adopted_rows
                    .values()
                    .filter(|row| adopted_row_identity_matches_scan(row, request))
                    .map(StagedStateRowIdentity::from),
            )
            .collect()
    }

    /// Returns a staged exact-row answer, if this transaction has one.
    pub(crate) fn load_exact(&self, request: &LiveStateRowRequest) -> Option<StagedExactRow> {
        let untracked_identity = StagedStateRowIdentity::from_exact_request(request, true)?;
        if let Some(row) = self.rows.get(&untracked_identity) {
            return Some(if row.snapshot_content.is_none() {
                StagedExactRow::Tombstone
            } else {
                StagedExactRow::Row(LiveStateRow::from(row))
            });
        }

        let identity = StagedStateRowIdentity::from_exact_request(request, false)?;
        if let Some(row) = self.rows.get(&identity) {
            return Some(if row.snapshot_content.is_none() {
                StagedExactRow::Tombstone
            } else {
                StagedExactRow::Row(LiveStateRow::from(row))
            });
        }
        self.adopted_rows.get(&identity).map(|row| {
            if row.snapshot_content.is_none() {
                StagedExactRow::Tombstone
            } else {
                StagedExactRow::Row(LiveStateRow::from(row))
            }
        })
    }
}

pub(crate) enum StagedExactRow {
    Row(LiveStateRow),
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct StagedStateRowIdentity {
    untracked: bool,
    schema_key: String,
    entity_id: crate::entity_identity::EntityIdentity,
    file_id: Option<String>,
    version_id: String,
}

impl StagedStateRowIdentity {
    fn from_staged_row(row: &StagedStateRow) -> Self {
        Self {
            untracked: row.untracked,
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
            version_id: row.version_id.clone(),
        }
    }

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

    fn opposite_untracked(&self) -> Self {
        Self {
            untracked: !self.untracked,
            schema_key: self.schema_key.clone(),
            entity_id: self.entity_id.clone(),
            file_id: self.file_id.clone(),
            version_id: self.version_id.clone(),
        }
    }
}

impl From<&StagedStateRow> for StagedStateRowIdentity {
    fn from(row: &StagedStateRow) -> Self {
        Self::from_staged_row(row)
    }
}

impl From<&StagedAdoptedStateRow> for StagedStateRowIdentity {
    fn from(row: &StagedAdoptedStateRow) -> Self {
        Self {
            untracked: false,
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
            version_id: row.version_id.clone(),
        }
    }
}

impl From<&LiveStateRow> for StagedStateRowIdentity {
    fn from(row: &LiveStateRow) -> Self {
        Self {
            untracked: row.untracked,
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
            version_id: row.version_id.clone(),
        }
    }
}

fn hydrate_state_write_row(
    row: StageRow,
    functions: &mut dyn FunctionProvider,
) -> Result<StagedStateRow, LixError> {
    let updated_at = row.updated_at.unwrap_or_else(|| functions.timestamp());
    Ok(StagedStateRow {
        entity_id: row.entity_id.ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "normalized staged row is missing entity_id",
            )
        })?,
        schema_key: row.schema_key,
        file_id: row.file_id,
        snapshot_content: row.snapshot_content,
        metadata: row.metadata,
        schema_version: row.schema_version,
        created_at: row.created_at.unwrap_or_else(|| updated_at.clone()),
        updated_at,
        global: row.global,
        change_id: if row.untracked {
            row.change_id
        } else {
            Some(row.change_id.unwrap_or_else(|| functions.uuid_v7()))
        },
        commit_id: row.commit_id,
        untracked: row.untracked,
        version_id: row.version_id,
    })
}

fn hydrate_adopted_state_row(
    change: StageAdoptedChange,
) -> Result<StagedAdoptedStateRow, LixError> {
    if change.change_id != change.projected_row.change_id {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "adopted change '{}' does not match projected row change_id '{}'",
                change.change_id, change.projected_row.change_id
            ),
        ));
    }
    let row = change.projected_row;
    Ok(StagedAdoptedStateRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        file_id: row.file_id,
        snapshot_content: row.snapshot_content,
        metadata: row.metadata,
        schema_version: row.schema_version,
        created_at: row.created_at,
        updated_at: row.updated_at,
        global: change.version_id == GLOBAL_VERSION_ID,
        change_id: change.change_id,
        commit_id: String::new(),
        version_id: change.version_id,
    })
}

fn validate_commit_membership_support(row: &StagedStateRow) -> Result<(), LixError> {
    if row.global && row.version_id != GLOBAL_VERSION_ID {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine2 global staged rows must use the global version id",
        ));
    }
    Ok(())
}

fn validate_adopted_commit_membership_support(row: &StagedAdoptedStateRow) -> Result<(), LixError> {
    if row.global && row.version_id != GLOBAL_VERSION_ID {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine2 global adopted rows must use the global version id",
        ));
    }
    Ok(())
}

fn reject_duplicate_present_rows_in_batch(rows: &[StagedStateRow]) -> Result<(), LixError> {
    let mut pending_present_rows = BTreeMap::<StagedStateRowIdentity, &StagedStateRow>::new();
    for row in rows {
        let identity = StagedStateRowIdentity::from(row);
        if row.snapshot_content.is_none() {
            pending_present_rows.remove(&identity);
            continue;
        }
        if let Some(previous) = pending_present_rows.insert(identity, row) {
            return Err(LixError::new(
                LixError::CODE_UNIQUE,
                format!(
                    "primary-key constraint violation on schema '{}' version '{}': duplicate staged rows for entity_id '{}' in version '{}'",
                    row.schema_key,
                    row.schema_version,
                    previous.entity_id.as_string()?,
                    row.version_id
                ),
            ));
        }
    }
    Ok(())
}

fn duplicate_insert_identity_error(row: &StagedStateRow) -> LixError {
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "primary-key constraint violation on schema '{}' version '{}': INSERT would duplicate entity_id '{}' in version '{}'",
            row.schema_key,
            row.schema_version,
            row.entity_id
                .as_string()
                .unwrap_or_else(|_| "<invalid entity_id>".to_string()),
            row.version_id
        ),
    )
}

fn conflicting_adopted_identity_error(row: &StagedStateRow) -> LixError {
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "transaction cannot stage a new row and an adopted projection for schema '{}' entity_id '{}' in version '{}'",
            row.schema_key,
            row.entity_id
                .as_string()
                .unwrap_or_else(|_| "<invalid entity_id>".to_string()),
            row.version_id
        ),
    )
}

fn conflicting_adopted_projection_error(row: &StagedAdoptedStateRow) -> LixError {
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "transaction cannot stage duplicate adopted projections for schema '{}' entity_id '{}' in version '{}'",
            row.schema_key,
            row.entity_id
                .as_string()
                .unwrap_or_else(|_| "<invalid entity_id>".to_string()),
            row.version_id
        ),
    )
}

fn live_state_identity_from_staged_row(row: &StagedStateRow) -> LiveStateRowIdentity {
    LiveStateRowIdentity {
        version_id: row.version_id.clone(),
        schema_key: row.schema_key.clone(),
        entity_id: row.entity_id.clone(),
        file_id: row.file_id.clone(),
    }
}

fn add_row_to_commit_members(
    members_by_version: &mut BTreeMap<String, StagedCommitMembers>,
    row: &mut StagedStateRow,
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
                functions.uuid_v7(),
                functions.timestamp(),
            )
        });
    row.commit_id = Some(members.commit_id.clone());
    members.add_change_id(change_id);
}

fn add_adopted_row_to_commit_members(
    members_by_version: &mut BTreeMap<String, StagedCommitMembers>,
    row: &mut StagedAdoptedStateRow,
    functions: &mut dyn FunctionProvider,
) {
    let members = members_by_version
        .entry(row.version_id.clone())
        .or_insert_with(|| {
            StagedCommitMembers::new(
                functions.uuid_v7(),
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
    row: &StagedStateRow,
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

fn staged_row_matches_scan(row: &StagedStateRow, request: &LiveStateScanRequest) -> bool {
    staged_row_identity_matches_scan(row, request)
        && (row.snapshot_content.is_some() || request.filter.include_tombstones)
}

fn adopted_row_matches_scan(row: &StagedAdoptedStateRow, request: &LiveStateScanRequest) -> bool {
    adopted_row_identity_matches_scan(row, request)
        && (row.snapshot_content.is_some() || request.filter.include_tombstones)
}

fn adopted_row_identity_matches_scan(
    row: &StagedAdoptedStateRow,
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
    nullable_key_matches_filters(&row.file_id, &request.filter.file_ids)
}

fn staged_row_identity_matches_scan(row: &StagedStateRow, request: &LiveStateScanRequest) -> bool {
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
    use crate::schema::builtin_schema_definition;

    #[tokio::test]
    async fn staging_overlay_uses_last_staged_row_for_exact_load() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
                rows: vec![state_row("sql2-duplicate-key", "first")],
            })
            .expect("initial row should stage");
        staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
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
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
                rows: vec![state_row("sql2-duplicate-key", "first")],
            })
            .expect("initial row should stage");
        staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
                rows: vec![state_row("sql2-duplicate-key", "second")],
            })
            .expect("staging rows should succeed");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged rows");
        let rows = overlay.scan(&scan_request_for_key("sql2-duplicate-key", false));

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
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
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
            .is_empty());

        let tombstones = overlay.scan(&scan_request_for_key("sql2-delete-key", true));
        assert_eq!(tombstones.len(), 1);
        assert_eq!(tombstones[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn staging_overlay_insert_after_delete_resurrects_row() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
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
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn staged_writes_drain_returns_coalesced_latest_rows() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
                rows: vec![
                    state_row("sql2-key-a", "first"),
                    state_row("sql2-key-b", "only"),
                ],
            })
            .expect("initial rows should stage");
        staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
                rows: vec![state_row("sql2-key-a", "second")],
            })
            .expect("staging rows should succeed");

        let drained = staged_writes.drain().expect("drain should succeed");

        assert_eq!(drained.state_rows.len(), 2);
        assert!(drained.state_rows.iter().any(|row| {
            row.entity_id == crate::entity_identity::EntityIdentity::single("sql2-key-a")
                && row.snapshot_content.as_deref()
                    == Some("{\"key\":\"sql2-key-a\",\"value\":\"second\"}")
        }));
        assert!(drained.state_rows.iter().any(|row| {
            row.entity_id == crate::entity_identity::EntityIdentity::single("sql2-key-b")
                && row.snapshot_content.as_deref()
                    == Some("{\"key\":\"sql2-key-b\",\"value\":\"only\"}")
        }));
    }

    #[tokio::test]
    async fn staged_writes_drain_preserves_file_data_payloads() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(StageWrite::RowsWithFileData {
                mode: StageWriteMode::Replace,
                rows: vec![state_row("file-readme", "descriptor")],
                file_data: vec![StageFileData {
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
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
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
            vec!["test-uuid-1".to_string()]
        );
    }

    #[tokio::test]
    async fn staged_writes_do_not_track_untracked_rows_as_commit_members() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
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
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
                rows: vec![state_row("overwrite-key", "first")
                    .with_tracked()
                    .with_change_id("change-first")],
            })
            .expect("initial tracked row should stage");
        staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
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
    async fn staged_writes_untracked_overwrite_removes_tracked_commit_member() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
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
        assert_eq!(drained.state_rows.len(), 1);
        assert_eq!(
            drained.state_rows[0].change_id.as_deref(),
            Some("change-untracked")
        );
        assert!(drained.commit_members_by_version.is_empty());
    }

    #[tokio::test]
    async fn staged_writes_reject_duplicate_present_rows_in_one_batch() {
        let staged_writes = test_staged_writes();

        let error = staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
                rows: vec![
                    state_row("duplicate-present-key", "first"),
                    state_row("duplicate-present-key", "second"),
                ],
            })
            .expect_err("same-batch duplicate present rows should fail");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert!(
            error
                .description
                .contains("primary-key constraint violation"),
            "error should explain the duplicate primary key: {error:?}"
        );
    }

    #[tokio::test]
    async fn staged_writes_track_active_version_members_separately() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
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
            vec!["test-uuid-1".to_string()]
        );
    }

    #[tokio::test]
    async fn staged_writes_reject_global_rows_with_non_global_version_id() {
        let staged_writes = test_staged_writes();

        let error = staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
                rows: vec![{
                    let mut row = state_row("invalid-global-key", "value");
                    row.version_id = "version-a".to_string();
                    row
                }],
            })
            .expect_err("global row with non-global version should fail");

        assert!(error
            .description
            .contains("global staged rows must use the global version id"));
    }

    #[tokio::test]
    async fn staging_overlay_identity_matches_live_state_conflict_key() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
                rows: vec![
                    state_row("shared-entity", "other-schema-version").with_schema_version("2")
                ],
            })
            .expect("initial same-identity row should stage");
        staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
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
        let rows = overlay.scan(&LiveStateScanRequest {
            filter: LiveStateFilter {
                entity_ids: vec![crate::entity_identity::EntityIdentity::single(
                    "shared-entity",
                )],
                include_tombstones: true,
                ..LiveStateFilter::default()
            },
            ..LiveStateScanRequest::default()
        });

        assert_eq!(rows.len(), 4);
        assert!(rows.iter().any(|row| {
            row.snapshot_content.as_deref()
                == Some("{\"key\":\"shared-entity\",\"value\":\"tracked\"}")
        }));
    }

    #[tokio::test]
    async fn staged_writes_use_injected_function_provider_for_row_metadata() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
                rows: vec![state_row("sql2-functions-key", "value").with_tracked()],
            })
            .expect("staging rows should succeed");

        let drained = staged_writes.drain().expect("drain should succeed");
        assert_eq!(drained.state_rows.len(), 1);
        assert_eq!(
            drained.state_rows[0].change_id.as_deref(),
            Some("test-uuid-1")
        );
        assert_eq!(
            drained.state_rows[0].created_at.as_str(),
            "test-timestamp-1"
        );
        assert_eq!(
            drained.state_rows[0].updated_at.as_str(),
            "test-timestamp-1"
        );
    }

    #[tokio::test]
    async fn staged_writes_stamp_tracked_rows_with_commit_id_during_staging() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Replace,
                rows: vec![state_row("tracked-commit-key", "value").with_tracked()],
            })
            .expect("tracked row should stage");

        let drained = staged_writes.drain().expect("drain should succeed");
        assert_eq!(drained.state_rows.len(), 1);
        assert_eq!(
            drained.state_rows[0].commit_id.as_deref(),
            Some("test-uuid-2")
        );
        assert_eq!(
            drained
                .commit_members_by_version
                .get("global")
                .expect("global commit members should exist")
                .commit_id,
            "test-uuid-2"
        );
    }

    fn test_staged_writes() -> TransactionStagedWrites {
        let visible_schemas = vec![
            builtin_schema_definition("lix_key_value")
                .expect("lix_key_value schema")
                .clone(),
            test_schema_definition("other_schema", "1"),
            test_schema_definition("lix_key_value", "2"),
        ];
        let schema_catalog = TransactionSchemaCatalog::from_visible_schemas(&visible_schemas)
            .expect("schema catalog should build");
        TransactionStagedWrites::new(
            SharedFunctionProvider::new(
                Box::new(TestFunctionProvider::default()) as Box<dyn FunctionProvider + Send>
            ),
            schema_catalog,
        )
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

    fn state_row(key: &str, value: &str) -> StageRow {
        StageRow {
            entity_id: Some(crate::entity_identity::EntityIdentity::single(key)),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: None,
            updated_at: None,
            global: true,
            change_id: None,
            commit_id: None,
            untracked: true,
            version_id: "global".to_string(),
        }
    }

    fn test_schema_definition(schema_key: &str, schema_version: &str) -> serde_json::Value {
        serde_json::json!({
            "x-lix-key": schema_key,
            "x-lix-version": schema_version,
            "type": "object",
            "additionalProperties": true,
            "properties": {
                "key": { "type": "string" },
                "value": { "type": "string" }
            },
            "x-lix-primary-key": ["/key"]
        })
    }

    fn tombstone_row(key: &str) -> StageRow {
        StageRow {
            snapshot_content: None,
            ..state_row(key, "deleted")
        }
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
        fn with_schema_version(self, schema_version: &str) -> Self;
        fn with_file_id(self, file_id: &str) -> Self;
        fn with_tracked(self) -> Self;
        fn with_version(self, version_id: &str) -> Self;
        fn with_change_id(self, change_id: &str) -> Self;
    }

    impl StateRowTestExt for StageRow {
        fn with_schema(mut self, schema_key: &str) -> Self {
            self.schema_key = schema_key.to_string();
            self
        }

        fn with_schema_version(mut self, schema_version: &str) -> Self {
            self.schema_version = schema_version.to_string();
            self
        }

        fn with_file_id(mut self, file_id: &str) -> Self {
            self.file_id = Some(file_id.to_string());
            self
        }

        fn with_tracked(mut self) -> Self {
            self.untracked = false;
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
