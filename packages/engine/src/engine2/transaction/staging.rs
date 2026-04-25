use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use async_trait::async_trait;

use crate::functions::{DynFunctionProvider, LixFunctionProvider};
use crate::live_state::{ExactRowRequest, LiveRow, LiveStateScanRequest};
use crate::sql2::{FileDataWrite, SqlWriteIntent, SqlWriteOutcome, SqlWriteStager, StateRow};
use crate::{LixError, NullableKeyFilter};

/// Transaction-local writes decoded by DataFusion provider hooks.
///
/// This is the engine2 seam between SQL execution and transaction ownership:
/// providers stage SQL write intents here, the transaction normalizes them into
/// stable `StateRow`s, reads build a `StagedStateRowOverlay` from those rows,
/// and commit later drains the same rows.
pub(crate) struct TransactionStagedWrites {
    functions: DynFunctionProvider,
    rows: Mutex<BTreeMap<StagedStateRowIdentity, StateRow>>,
    file_data_writes: Mutex<Vec<FileDataWrite>>,
}

/// Drained transaction-local writes ready for commit.
pub(crate) struct StagedWriteSet {
    pub(crate) state_rows: Vec<StateRow>,
    pub(crate) file_data_writes: Vec<FileDataWrite>,
}

impl TransactionStagedWrites {
    pub(crate) fn new(functions: DynFunctionProvider) -> Self {
        Self {
            functions,
            rows: Mutex::new(BTreeMap::new()),
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
        let mut file_data_guard = self.file_data_writes.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged file data lock",
            )
        })?;
        Ok(StagedWriteSet {
            state_rows: std::mem::take(&mut *rows_guard).into_values().collect(),
            file_data_writes: std::mem::take(&mut *file_data_guard),
        })
    }

    /// Builds the transaction-local read overlay from currently staged writes.
    pub(crate) fn staging_overlay(&self) -> Result<StagedStateRowOverlay, LixError> {
        let guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        Ok(StagedStateRowOverlay::new(guard.clone()))
    }
}

#[async_trait]
impl SqlWriteStager for TransactionStagedWrites {
    async fn stage_write(&self, write: SqlWriteIntent) -> Result<SqlWriteOutcome, LixError> {
        let count = match &write {
            SqlWriteIntent::WriteRows { rows } => rows.len() as u64,
            SqlWriteIntent::WriteRowsWithFileData { count, .. } => *count,
        };
        let (rows, file_data_writes) =
            state_rows_from_write_intent(write, &mut self.functions.clone())?;
        let mut guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        for row in rows {
            guard.insert(StagedStateRowIdentity::from(&row), row);
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
        Ok(SqlWriteOutcome { count })
    }
}

/// Read overlay derived from staged transaction writes.
pub(crate) struct StagedStateRowOverlay {
    rows: BTreeMap<StagedStateRowIdentity, StateRow>,
}

impl StagedStateRowOverlay {
    fn new(rows: BTreeMap<StagedStateRowIdentity, StateRow>) -> Self {
        Self { rows }
    }

    /// Returns staged rows visible for a scan request.
    pub(crate) fn scan(&self, request: &LiveStateScanRequest) -> Vec<LiveRow> {
        self.rows
            .values()
            .filter(|row| staged_row_matches_scan(row, request))
            .map(|row| live_row_from_state_row_ref(row))
            .collect::<Result<Vec<_>, _>>()
            .expect("engine2 staged rows should already be normalized")
    }

    /// Converts staged rows for commit into the live_state adapter shape.
    pub(crate) fn into_live_rows(rows: Vec<StateRow>) -> Result<Vec<LiveRow>, LixError> {
        rows.into_iter().map(live_row_from_state_row).collect()
    }

    /// Returns staged identities that should suppress committed rows.
    ///
    /// Tombstones also suppress committed rows, even when the caller is not
    /// asking to see tombstone rows.
    pub(crate) fn identities_matching_scan(
        &self,
        request: &LiveStateScanRequest,
    ) -> BTreeSet<StagedStateRowIdentity> {
        self.rows
            .values()
            .filter(|row| staged_row_identity_matches_scan(row, request))
            .map(StagedStateRowIdentity::from)
            .collect()
    }

    /// Returns a staged exact-row answer, if this transaction has one.
    pub(crate) fn load_exact(&self, request: &ExactRowRequest) -> Option<StagedExactRow> {
        let identity = StagedStateRowIdentity::from_exact_request(request)?;
        self.rows.get(&identity).map(|row| {
            if row.snapshot_content.is_none() {
                StagedExactRow::Tombstone
            } else {
                StagedExactRow::Row(
                    live_row_from_state_row_ref(row)
                        .expect("engine2 staged rows should already be normalized"),
                )
            }
        })
    }
}

pub(crate) enum StagedExactRow {
    Row(LiveRow),
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct StagedStateRowIdentity {
    untracked: bool,
    schema_key: String,
    entity_id: String,
    file_id: Option<String>,
    version_id: String,
}

impl StagedStateRowIdentity {
    fn from_state_row(row: &StateRow) -> Self {
        Self {
            untracked: row.untracked,
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
            version_id: row.version_id.clone(),
        }
    }

    pub(crate) fn from_live_row(row: &LiveRow) -> Self {
        Self {
            untracked: row.untracked,
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
            version_id: row.version_id.clone(),
        }
    }

    fn from_exact_request(request: &ExactRowRequest) -> Option<Self> {
        let file_id = match &request.file_id {
            NullableKeyFilter::Null => None,
            NullableKeyFilter::Value(value) => Some(value.clone()),
            // Exact overlay lookup requires a concrete row identity.
            NullableKeyFilter::Any => return None,
        };
        Some(Self {
            untracked: request.untracked,
            schema_key: request.schema_key.clone(),
            entity_id: request.entity_id.clone(),
            file_id,
            version_id: request.version_id.clone(),
        })
    }
}

impl From<&StateRow> for StagedStateRowIdentity {
    fn from(row: &StateRow) -> Self {
        Self::from_state_row(row)
    }
}

fn state_rows_from_write_intent(
    write: SqlWriteIntent,
    functions: &mut dyn LixFunctionProvider,
) -> Result<(Vec<StateRow>, Vec<FileDataWrite>), LixError> {
    let mut state_rows = Vec::new();
    let mut file_data_writes = Vec::new();
    match write {
        SqlWriteIntent::WriteRows { rows } => {
            push_state_rows(&mut state_rows, rows, functions)?;
        }
        SqlWriteIntent::WriteRowsWithFileData {
            rows, file_data, ..
        } => {
            push_state_rows(&mut state_rows, rows, functions)?;
            file_data_writes.extend(file_data);
        }
    }
    Ok((state_rows, file_data_writes))
}

fn push_state_rows(
    state_rows: &mut Vec<StateRow>,
    rows: Vec<StateRow>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<(), LixError> {
    state_rows.reserve(rows.len());
    for row in rows {
        state_rows.push(normalize_state_row(row, functions)?);
    }
    Ok(())
}

fn normalize_state_row(
    mut row: StateRow,
    functions: &mut dyn LixFunctionProvider,
) -> Result<StateRow, LixError> {
    if row.schema_version.is_none() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine2 staged write requires schema_version for staging overlay",
        ));
    }
    let updated_at = row.updated_at.unwrap_or_else(|| functions.timestamp());
    row.created_at = row.created_at.or_else(|| Some(updated_at.clone()));
    row.updated_at = Some(updated_at);
    row.change_id = row.change_id.or_else(|| Some(functions.uuid_v7()));
    Ok(row)
}

pub(crate) fn live_row_from_state_row(row: StateRow) -> Result<LiveRow, LixError> {
    let schema_version = row.schema_version.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine2 staged write requires schema_version for staging overlay",
        )
    })?;

    Ok(LiveRow {
        entity_id: row.entity_id,
        file_id: row.file_id,
        schema_key: row.schema_key,
        schema_version,
        version_id: row.version_id,
        plugin_key: row.plugin_key,
        metadata: row.metadata,
        change_id: row.change_id,
        commit_id: row.commit_id,
        global: row.global,
        untracked: row.untracked,
        created_at: row.created_at,
        updated_at: row.updated_at,
        snapshot_content: row.snapshot_content,
    })
}

fn live_row_from_state_row_ref(row: &StateRow) -> Result<LiveRow, LixError> {
    live_row_from_state_row(row.clone())
}

fn staged_row_matches_scan(row: &StateRow, request: &LiveStateScanRequest) -> bool {
    staged_row_identity_matches_scan(row, request)
        && (row.snapshot_content.is_some() || request.filter.include_tombstones)
}

fn staged_row_identity_matches_scan(row: &StateRow, request: &LiveStateScanRequest) -> bool {
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
        && nullable_key_matches_filters(&row.plugin_key, &request.filter.plugin_keys)
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
    use crate::live_state::{ExactRowRequest, LiveStateFilter};

    #[tokio::test]
    async fn staging_overlay_uses_last_staged_row_for_exact_load() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(SqlWriteIntent::WriteRows {
                rows: vec![
                    state_row("sql2-duplicate-key", "first"),
                    state_row("sql2-duplicate-key", "second"),
                ],
            })
            .await
            .expect("staging rows should succeed");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged rows");
        let row = overlay
            .load_exact(&ExactRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: "global".to_string(),
                entity_id: "sql2-duplicate-key".to_string(),
                file_id: NullableKeyFilter::Null,
                untracked: true,
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
            .stage_write(SqlWriteIntent::WriteRows {
                rows: vec![
                    state_row("sql2-duplicate-key", "first"),
                    state_row("sql2-duplicate-key", "second"),
                ],
            })
            .await
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
            .stage_write(SqlWriteIntent::WriteRows {
                rows: vec![
                    state_row("sql2-delete-key", "visible"),
                    tombstone_row("sql2-delete-key"),
                ],
            })
            .await
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
            .stage_write(SqlWriteIntent::WriteRows {
                rows: vec![
                    tombstone_row("sql2-resurrect-key"),
                    state_row("sql2-resurrect-key", "visible-again"),
                ],
            })
            .await
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
            .stage_write(SqlWriteIntent::WriteRows {
                rows: vec![
                    state_row("sql2-key-a", "first"),
                    state_row("sql2-key-a", "second"),
                    state_row("sql2-key-b", "only"),
                ],
            })
            .await
            .expect("staging rows should succeed");

        let drained = staged_writes.drain().expect("drain should succeed");

        assert_eq!(drained.state_rows.len(), 2);
        assert!(drained.state_rows.iter().any(|row| {
            row.entity_id == "sql2-key-a"
                && row.snapshot_content.as_deref()
                    == Some("{\"key\":\"sql2-key-a\",\"value\":\"second\"}")
        }));
        assert!(drained.state_rows.iter().any(|row| {
            row.entity_id == "sql2-key-b"
                && row.snapshot_content.as_deref()
                    == Some("{\"key\":\"sql2-key-b\",\"value\":\"only\"}")
        }));
    }

    #[tokio::test]
    async fn staged_writes_drain_preserves_file_data_payloads() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(SqlWriteIntent::WriteRowsWithFileData {
                rows: vec![state_row("file-readme", "descriptor")],
                file_data: vec![FileDataWrite {
                    file_id: "file-readme".to_string(),
                    version_id: "global".to_string(),
                    untracked: true,
                    data: b"hello".to_vec(),
                }],
                count: 1,
            })
            .await
            .expect("staging rows with file data should succeed");

        let drained = staged_writes.drain().expect("drain should succeed");

        assert_eq!(drained.state_rows.len(), 1);
        assert_eq!(drained.file_data_writes.len(), 1);
        assert_eq!(drained.file_data_writes[0].file_id, "file-readme");
        assert_eq!(drained.file_data_writes[0].data, b"hello");
    }

    #[tokio::test]
    async fn staging_overlay_identity_matches_live_state_conflict_key() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(SqlWriteIntent::WriteRows {
                rows: vec![
                    state_row("shared-entity", "base"),
                    state_row("shared-entity", "other-version").with_version("version-b"),
                    state_row("shared-entity", "other-schema").with_schema("other_schema"),
                    state_row("shared-entity", "other-file").with_file_id("file-a"),
                    state_row("shared-entity", "other-plugin").with_plugin_key("plugin-a"),
                    state_row("shared-entity", "other-schema-version").with_schema_version("2"),
                    state_row("shared-entity", "tracked").with_tracked(),
                ],
            })
            .await
            .expect("staging rows should succeed");

        let overlay = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged rows");
        let rows = overlay.scan(&LiveStateScanRequest {
            filter: LiveStateFilter {
                entity_ids: vec!["shared-entity".to_string()],
                include_tombstones: true,
                ..LiveStateFilter::default()
            },
            ..LiveStateScanRequest::default()
        });

        assert_eq!(rows.len(), 5);
        assert!(rows.iter().any(|row| {
            row.snapshot_content.as_deref()
                == Some("{\"key\":\"shared-entity\",\"value\":\"other-schema-version\"}")
        }));
        assert!(rows.iter().any(|row| {
            row.snapshot_content.as_deref()
                == Some("{\"key\":\"shared-entity\",\"value\":\"tracked\"}")
        }));
    }

    #[tokio::test]
    async fn staged_writes_use_injected_function_provider_for_row_metadata() {
        let staged_writes = test_staged_writes();

        staged_writes
            .stage_write(SqlWriteIntent::WriteRows {
                rows: vec![state_row("sql2-functions-key", "value")],
            })
            .await
            .expect("staging rows should succeed");

        let drained = staged_writes.drain().expect("drain should succeed");
        assert_eq!(drained.state_rows.len(), 1);
        assert_eq!(
            drained.state_rows[0].change_id.as_deref(),
            Some("test-uuid-1")
        );
        assert_eq!(
            drained.state_rows[0].created_at.as_deref(),
            Some("test-timestamp-1")
        );
        assert_eq!(
            drained.state_rows[0].updated_at.as_deref(),
            Some("test-timestamp-1")
        );
    }

    fn test_staged_writes() -> TransactionStagedWrites {
        TransactionStagedWrites::new(SharedFunctionProvider::new(Box::new(
            TestFunctionProvider::default(),
        )
            as Box<dyn LixFunctionProvider + Send>))
    }

    #[derive(Default)]
    struct TestFunctionProvider {
        uuid_count: usize,
        timestamp_count: usize,
    }

    impl LixFunctionProvider for TestFunctionProvider {
        fn uuid_v7(&mut self) -> String {
            self.uuid_count += 1;
            format!("test-uuid-{}", self.uuid_count)
        }

        fn timestamp(&mut self) -> String {
            self.timestamp_count += 1;
            format!("test-timestamp-{}", self.timestamp_count)
        }
    }

    fn state_row(key: &str, value: &str) -> StateRow {
        StateRow {
            entity_id: key.to_string(),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")),
            metadata: None,
            schema_version: Some("1".to_string()),
            created_at: None,
            updated_at: None,
            global: true,
            change_id: None,
            commit_id: None,
            untracked: true,
            version_id: "global".to_string(),
        }
    }

    fn tombstone_row(key: &str) -> StateRow {
        StateRow {
            snapshot_content: None,
            ..state_row(key, "deleted")
        }
    }

    fn exact_request_for_key(key: &str) -> ExactRowRequest {
        ExactRowRequest {
            schema_key: "lix_key_value".to_string(),
            version_id: "global".to_string(),
            entity_id: key.to_string(),
            file_id: NullableKeyFilter::Null,
            untracked: true,
        }
    }

    fn scan_request_for_key(key: &str, include_tombstones: bool) -> LiveStateScanRequest {
        LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec!["lix_key_value".to_string()],
                entity_ids: vec![key.to_string()],
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
        fn with_plugin_key(self, plugin_key: &str) -> Self;
        fn with_tracked(self) -> Self;
        fn with_version(self, version_id: &str) -> Self;
    }

    impl StateRowTestExt for StateRow {
        fn with_schema(mut self, schema_key: &str) -> Self {
            self.schema_key = schema_key.to_string();
            self
        }

        fn with_schema_version(mut self, schema_version: &str) -> Self {
            self.schema_version = Some(schema_version.to_string());
            self
        }

        fn with_file_id(mut self, file_id: &str) -> Self {
            self.file_id = Some(file_id.to_string());
            self
        }

        fn with_plugin_key(mut self, plugin_key: &str) -> Self {
            self.plugin_key = Some(plugin_key.to_string());
            self
        }

        fn with_tracked(mut self) -> Self {
            self.untracked = false;
            self
        }

        fn with_version(mut self, version_id: &str) -> Self {
            self.version_id = version_id.to_string();
            self
        }
    }
}
