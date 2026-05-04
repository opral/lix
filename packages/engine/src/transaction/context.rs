use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::binary_cas::BinaryCasContext;
use crate::changelog::ChangelogContext;
use crate::commit_graph::{CommitGraphContext, CommitGraphStoreReader};
use crate::entity_identity::EntityIdentity;
use crate::functions::{FunctionContext, FunctionProviderHandle};
use crate::live_state::{
    LiveStateContext, LiveStateRow, LiveStateRowRequest, LiveStateScanRequest,
};
use crate::schema_registry::SchemaRegistry;
use crate::session::{SessionMode, WORKSPACE_VERSION_KEY};
use crate::sql2::SqlWriteExecutionContext;
use crate::storage::{StorageContext, StorageWriteTransaction};
use crate::tracked_state::{TrackedStateContext, TrackedStateStoreReader};
use crate::transaction::commit;
use crate::transaction::live_state_overlay::overlay_scan_rows;
use crate::transaction::normalization::TransactionSchemaCatalog;
use crate::transaction::staging::TransactionStagedWrites;
use crate::transaction::types::{StageRow, StageWrite, StageWriteMode, StageWriteOutcome};
use crate::transaction::validation::{validate_staged_writes, TransactionValidationInput};
use crate::version::{VersionContext, VersionRefReader};
use crate::GLOBAL_VERSION_ID;
use crate::{LixError, NullableKeyFilter};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TransactionCommitOutcome;

/// One execution-scoped transaction capability for engine2 write paths.
///
/// This is intentionally not a session-wide kitchen sink. It owns the backend
/// write transaction for one `SessionContext::execute(...)` call and projects
/// staged SQL writes back into the SQL DAG through an engine2-local live-state
/// overlay.
///
/// Transaction invariant: this is the capability for engine2 operations
/// that may write. Write-relevant reads must be exposed from this transaction,
/// after the backend write transaction has begun, rather than from session-level
/// helpers.
pub(crate) struct Transaction {
    active_version_id: String,
    live_state: Arc<LiveStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    changelog: Arc<ChangelogContext>,
    version_ctx: Arc<VersionContext>,
    staged_writes: Arc<TransactionStagedWrites>,
    storage_transaction: Box<dyn StorageWriteTransaction + Send + Sync + 'static>,
    visible_schemas: Vec<JsonValue>,
    functions: FunctionProviderHandle,
}

impl Transaction {
    /// Opens a backend write transaction and creates an execution-scoped
    /// staging area for SQL/provider hooks.
    async fn open(
        mode: &SessionMode,
        storage: StorageContext,
        live_state: Arc<LiveStateContext>,
        tracked_state: Arc<TrackedStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        changelog: Arc<ChangelogContext>,
        version_ctx: Arc<VersionContext>,
        schema_registry: Arc<SchemaRegistry>,
    ) -> Result<OpenTransaction, LixError> {
        let mut storage_transaction = storage.begin_write_transaction().await?;
        let active_version_id = resolve_active_version_id(
            mode,
            live_state.as_ref(),
            version_ctx.as_ref(),
            storage_transaction.as_mut(),
        )
        .await?;
        let runtime_functions = {
            let runtime_live_state = live_state.reader(storage_transaction.as_mut());
            FunctionContext::prepare(&runtime_live_state).await?
        };
        let functions = runtime_functions.provider();
        let visible_schemas = {
            let visible_live_state = live_state.reader(storage_transaction.as_mut());
            schema_registry
                .visible_schemas(&visible_live_state, &active_version_id)
                .await?
        };
        let schema_catalog = TransactionSchemaCatalog::from_visible_schemas(&visible_schemas)?;
        let staged_writes = Arc::new(TransactionStagedWrites::new(
            functions.clone(),
            schema_catalog,
        ));
        Ok(OpenTransaction {
            transaction: Self {
                active_version_id,
                live_state,
                tracked_state,
                binary_cas,
                changelog,
                version_ctx,
                staged_writes,
                storage_transaction,
                visible_schemas,
                functions,
            },
            runtime_functions,
        })
    }

    /// Commits staged writes, runtime function state, and the backend transaction.
    ///
    /// Commit owns the execution boundary: provider-staged rows become
    /// changelog facts, `lix_commit` rows, version-ref updates, and visible
    /// live_state rows before the backend transaction is committed.
    pub(crate) async fn commit(
        mut self,
        runtime_functions: &FunctionContext,
    ) -> Result<TransactionCommitOutcome, LixError> {
        let staged_writes = match self.staged_writes.drain() {
            Ok(staged_writes) => staged_writes,
            Err(error) => {
                let _ = self.storage_transaction.rollback().await;
                return Err(error);
            }
        };
        let live_state_reader = self.live_state.reader(self.storage_transaction.as_mut());
        if let Err(error) = validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &self.visible_schemas,
            &live_state_reader,
        ))
        .await
        {
            let _ = self.storage_transaction.rollback().await;
            return Err(error);
        }
        if let Err(error) = commit::commit_staged_writes(
            &self.binary_cas,
            &self.changelog,
            &self.live_state,
            self.version_ctx.as_ref(),
            self.storage_transaction.as_mut(),
            staged_writes,
        )
        .await
        {
            let _ = self.storage_transaction.rollback().await;
            return Err(error);
        }
        if let Err(error) = runtime_functions
            .persist_if_needed(&mut self.live_state.writer(self.storage_transaction.as_mut()))
            .await
        {
            let _ = self.storage_transaction.rollback().await;
            return Err(error);
        }
        self.storage_transaction.commit().await?;
        Ok(TransactionCommitOutcome::default())
    }

    /// Rolls back the backend transaction.
    ///
    /// This is the explicit failure path for a write execution. Dropping the
    /// buffered transaction without commit is not the API we want callers to
    /// rely on.
    #[allow(dead_code)]
    pub(crate) async fn rollback(self) -> Result<(), LixError> {
        self.storage_transaction.rollback().await
    }

    /// Stages one decoded write batch into this transaction.
    ///
    /// This is the programmatic write entrypoint used by non-SQL APIs. The
    /// transaction still owns hydration from `StageRow` into `StagedStateRow`,
    /// so generated timestamps, change ids, commit ids, and commit membership
    /// stay in one place.
    #[allow(dead_code)]
    pub(crate) fn stage_write(&self, write: StageWrite) -> Result<(), LixError> {
        self.staged_writes.stage_write(write)?;
        Ok(())
    }

    /// Convenience helper for programmatic APIs that only stage state rows.
    #[allow(dead_code)]
    pub(crate) fn stage_rows(&self, rows: Vec<StageRow>) -> Result<(), LixError> {
        self.stage_write(StageWrite::Rows {
            mode: StageWriteMode::Replace,
            rows,
        })
    }

    /// Returns the active version resolved inside this write transaction.
    pub(crate) fn active_version_id(&self) -> &str {
        &self.active_version_id
    }

    /// Returns this transaction's prepared runtime functions.
    pub(crate) fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    /// Adds an extra parent to the commit generated for `version_id`.
    ///
    /// Merge uses this to preserve source-branch ancestry. Ordinary writes do
    /// not call this because commit finalization already parents to the
    /// version's previous head.
    pub(crate) fn add_commit_parent(
        &self,
        version_id: String,
        parent_commit_id: String,
    ) -> Result<(), LixError> {
        self.staged_writes
            .add_commit_parent(version_id, parent_commit_id)
    }

    /// Advances a version ref without staging tracked rows.
    ///
    /// Fast-forward merges use this path because the commit graph already
    /// contains the source head; the target ref only needs to move to it.
    pub(crate) async fn advance_version_ref(
        &mut self,
        version_id: &str,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let timestamp = self.functions.call_timestamp();
        self.version_ctx
            .advance_ref(
                self.storage_transaction.as_mut(),
                version_id,
                commit_id,
                &timestamp,
            )
            .await
    }

    /// Returns the commit id currently staged for `version_id`, if tracked rows
    /// have been staged for that version.
    pub(crate) fn staged_commit_id(&self, version_id: &str) -> Result<Option<String>, LixError> {
        self.staged_writes.staged_commit_id(version_id)
    }

    /// Stages a commit for `version_id` even if no tracked rows changed.
    pub(crate) fn stage_empty_commit(&self, version_id: String) -> Result<String, LixError> {
        self.staged_writes.stage_empty_commit(version_id)
    }

    /// Creates a version-ref reader scoped to this write transaction.
    pub(crate) fn version_ref_reader(&mut self) -> impl VersionRefReader + '_ {
        self.version_ctx
            .ref_reader(self.storage_transaction.as_mut())
    }

    /// Creates a tracked-state reader scoped to this write transaction.
    pub(crate) fn tracked_state_reader(
        &mut self,
    ) -> TrackedStateStoreReader<&mut dyn StorageWriteTransaction> {
        self.tracked_state.reader(self.storage_transaction.as_mut())
    }

    /// Creates a commit-graph reader scoped to this write transaction.
    pub(crate) fn commit_graph_reader(
        &mut self,
    ) -> CommitGraphStoreReader<&mut dyn StorageWriteTransaction> {
        CommitGraphContext::new(self.changelog.as_ref().clone())
            .reader(self.storage_transaction.as_mut())
    }
}

pub(crate) struct OpenTransaction {
    pub(crate) transaction: Transaction,
    pub(crate) runtime_functions: FunctionContext,
}

pub(crate) async fn open_transaction(
    mode: &SessionMode,
    storage: StorageContext,
    live_state: Arc<LiveStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    changelog: Arc<ChangelogContext>,
    version_ctx: Arc<VersionContext>,
    schema_registry: Arc<SchemaRegistry>,
) -> Result<OpenTransaction, LixError> {
    Transaction::open(
        mode,
        storage,
        live_state,
        tracked_state,
        binary_cas,
        changelog,
        version_ctx,
        schema_registry,
    )
    .await
}

#[async_trait]
impl SqlWriteExecutionContext for Transaction {
    fn active_version_id(&self) -> &str {
        &self.active_version_id
    }

    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        Ok(self.visible_schemas.clone())
    }

    async fn load_blob_data_by_hash(&mut self, blob_hash: &str) -> Result<Option<Vec<u8>>, LixError> {
        self.binary_cas
            .reader(self.storage_transaction.as_mut())
            .load_blob_data_by_hash(blob_hash)
            .await
    }

    async fn scan_live_state(
        &mut self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<LiveStateRow>, LixError> {
        let staged = self.staged_writes.staging_overlay()?;
        let base = self.live_state.reader(self.storage_transaction.as_mut());
        overlay_scan_rows(&base, &staged, request).await
    }

    async fn load_version_head(&mut self, version_id: &str) -> Result<Option<String>, LixError> {
        self.version_ctx
            .ref_reader(self.storage_transaction.as_mut())
            .load_head_commit_id(version_id)
            .await
    }

    async fn stage_write(&mut self, write: StageWrite) -> Result<StageWriteOutcome, LixError> {
        self.staged_writes.stage_write(write)
    }
}

async fn resolve_active_version_id(
    mode: &SessionMode,
    live_state: &LiveStateContext,
    version_ctx: &VersionContext,
    transaction: &mut dyn StorageWriteTransaction,
) -> Result<String, LixError> {
    match mode {
        SessionMode::Pinned { version_id } => Ok(version_id.clone()),
        SessionMode::Workspace => {
            load_workspace_version_id(live_state, version_ctx, transaction).await
        }
    }
}

async fn load_workspace_version_id(
    live_state: &LiveStateContext,
    version_ctx: &VersionContext,
    transaction: &mut dyn StorageWriteTransaction,
) -> Result<String, LixError> {
    let row = live_state
        .reader(&mut *transaction)
        .load_row(&LiveStateRowRequest {
            schema_key: "lix_key_value".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: EntityIdentity::single(WORKSPACE_VERSION_KEY),
            file_id: NullableKeyFilter::Null,
        })
        .await?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "workspace version selector is missing lix_key_value:lix_workspace_version_id",
            )
        })?;
    let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "workspace version selector is missing snapshot_content",
        )
    })?;
    let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("workspace version selector snapshot is invalid JSON: {error}"),
        )
    })?;
    let version_id = snapshot
        .get("value")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "workspace version selector value must be a non-empty string",
            )
        })?
        .to_string();

    let head = version_ctx
        .ref_reader(&mut *transaction)
        .load_head_commit_id(&version_id)
        .await?;
    if head.is_none() {
        return Err(LixError::version_not_found(
            version_id,
            "load_workspace_version_id",
            "workspace_selector",
        ));
    }

    Ok(version_id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::backend::testing::UnitTestBackend;
    use crate::changelog::ChangelogScanRequest;
    use crate::tracked_state::{TrackedStateRowRequest, TrackedStateScanRequest};
    use crate::untracked_state::{UntrackedStateContext, UntrackedStateRowRequest};
    use crate::version::VersionContext;
    use crate::Backend;
    use crate::NullableKeyFilter;
    use crate::GLOBAL_VERSION_ID;

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            crate::untracked_state::UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(crate::changelog::ChangelogContext::new()),
        )
    }

    #[tokio::test]
    async fn stage_rows_routes_tracked_and_untracked_rows_without_sql() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = Arc::new(live_state_context());
        let binary_cas = Arc::new(BinaryCasContext::new());
        let changelog = Arc::new(ChangelogContext::new());
        let version_ctx = Arc::new(VersionContext::new(Arc::new(UntrackedStateContext::new())));
        let schema_registry = Arc::new(SchemaRegistry::new());
        let opened = open_transaction(
            &SessionMode::Pinned {
                version_id: GLOBAL_VERSION_ID.to_string(),
            },
            storage.clone(),
            Arc::clone(&live_state),
            Arc::new(crate::tracked_state::TrackedStateContext::new()),
            Arc::clone(&binary_cas),
            Arc::clone(&changelog),
            Arc::clone(&version_ctx),
            Arc::clone(&schema_registry),
        )
        .await
        .expect("transaction should open");
        let transaction = opened.transaction;
        let runtime_functions = opened.runtime_functions;

        transaction
            .stage_rows(vec![
                key_value_stage_row("tracked-programmatic", "tracked", false),
                key_value_stage_row("untracked-programmatic", "untracked", true),
            ])
            .expect("programmatic rows should stage");
        transaction
            .commit(&runtime_functions)
            .await
            .expect("transaction should commit");

        let changes = changelog
            .reader(storage.clone())
            .scan_changes(&ChangelogScanRequest::default())
            .await
            .expect("changelog should scan");
        assert!(
            changes
                .iter()
                .any(|change| change.entity_id.as_string().as_deref() == Ok("tracked-programmatic")),
            "tracked staged row should be appended to changelog"
        );
        assert!(
            !changes
                .iter()
                .any(|change| change.entity_id.as_string().as_deref()
                    == Ok("untracked-programmatic")),
            "untracked staged row must not be appended to changelog"
        );

        let head_commit_id = version_ctx
            .ref_reader(storage.clone())
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref should load")
            .expect("tracked commit should advance the global version ref");

        let tracked_row = crate::tracked_state::TrackedStateContext::new()
            .reader(storage.clone())
            .load_row_at_commit(
                &head_commit_id,
                &TrackedStateRowRequest {
                    schema_key: "lix_key_value".to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single(
                        "tracked-programmatic",
                    ),
                    file_id: NullableKeyFilter::Null,
                },
            )
            .await
            .expect("tracked state should load")
            .expect("tracked row should be present in tracked state");
        assert_eq!(tracked_row.commit_id, head_commit_id);
        assert_eq!(
            tracked_row.snapshot_content.as_deref(),
            Some(r#"{"key":"tracked-programmatic","value":"tracked"}"#)
        );

        let untracked_row = crate::untracked_state::UntrackedStateContext::new()
            .reader(storage.clone())
            .load_row(&UntrackedStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("untracked-programmatic"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("untracked state should load")
            .expect("untracked row should be present in untracked state");
        assert_eq!(
            untracked_row.snapshot_content.as_deref(),
            Some(r#"{"key":"untracked-programmatic","value":"untracked"}"#)
        );

        let live_untracked_row = live_state
            .reader(storage.clone())
            .load_row(&crate::live_state::LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("untracked-programmatic"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("live state should load")
            .expect("untracked row should be visible through live state");
        assert!(live_untracked_row.untracked);
        assert!(live_untracked_row.global);
        assert_eq!(live_untracked_row.version_id, GLOBAL_VERSION_ID);

        let tracked_rows = crate::tracked_state::TrackedStateContext::new()
            .reader(storage.clone())
            .scan_rows_at_commit(&head_commit_id, &TrackedStateScanRequest::default())
            .await
            .expect("tracked state should scan");
        assert!(
            tracked_rows
                .iter()
                .all(|row| row.entity_id.as_string().as_deref() != Ok("untracked-programmatic")),
            "untracked staged rows should not be written into tracked state"
        );
    }

    #[tokio::test]
    async fn commit_validates_staged_rows_before_persistence() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let live_state = Arc::new(live_state_context());
        let binary_cas = Arc::new(BinaryCasContext::new());
        let changelog = Arc::new(ChangelogContext::new());
        let version_ctx = Arc::new(VersionContext::new(Arc::new(UntrackedStateContext::new())));
        let schema_registry = Arc::new(SchemaRegistry::new());
        let opened = open_transaction(
            &SessionMode::Pinned {
                version_id: GLOBAL_VERSION_ID.to_string(),
            },
            storage.clone(),
            Arc::clone(&live_state),
            Arc::new(crate::tracked_state::TrackedStateContext::new()),
            Arc::clone(&binary_cas),
            Arc::clone(&changelog),
            Arc::clone(&version_ctx),
            Arc::clone(&schema_registry),
        )
        .await
        .expect("transaction should open");
        let transaction = opened.transaction;
        let runtime_functions = opened.runtime_functions;

        let mut invalid_row = key_value_stage_row("invalid-programmatic", "invalid", false);
        invalid_row.snapshot_content = Some("{\"key\":\"invalid-programmatic\"}".to_string());
        transaction
            .stage_rows(vec![invalid_row])
            .expect("invalid row should still reach commit validation");

        let error = transaction
            .commit(&runtime_functions)
            .await
            .expect_err("validation should reject before persistence");
        assert!(
            error
                .description
                .contains("snapshot_content validation failed"),
            "validation error should explain the rejected schema data: {error:?}"
        );

        let changes = changelog
            .reader(storage.clone())
            .scan_changes(&ChangelogScanRequest::default())
            .await
            .expect("changelog should scan after failed commit");
        assert!(
            changes.is_empty(),
            "validation failure must happen before changelog persistence"
        );
        let head = version_ctx
            .ref_reader(storage.clone())
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref should load after failed commit");
        assert_eq!(
            head, None,
            "validation failure must happen before version-ref persistence"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_unknown_schema_key_without_sql() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let (_live_state, _binary_cas, _changelog, _version_ref, _runtime_functions, transaction) =
            open_test_transaction(&backend).await;

        let mut row = key_value_stage_row("unknown-schema", "value", false);
        row.schema_key = "missing_schema".to_string();

        let error = transaction
            .stage_rows(vec![row])
            .expect_err("unknown schema should be rejected while staging");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error
                .description
                .contains("schema 'missing_schema' version '1' is not visible"),
            "error should explain missing schema visibility: {error:?}"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_unknown_schema_version_without_sql() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let (_live_state, _binary_cas, _changelog, _version_ref, _runtime_functions, transaction) =
            open_test_transaction(&backend).await;

        let mut row = key_value_stage_row("unknown-version", "value", false);
        row.schema_version = "999".to_string();

        let error = transaction
            .stage_rows(vec![row])
            .expect_err("unknown schema version should be rejected while staging");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error
                .description
                .contains("schema 'lix_key_value' version '999' is not visible"),
            "error should explain missing schema version visibility: {error:?}"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_invalid_snapshot_json_without_sql() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let (_live_state, _binary_cas, _changelog, _version_ref, _runtime_functions, transaction) =
            open_test_transaction(&backend).await;

        let mut row = key_value_stage_row("invalid-json", "value", false);
        row.snapshot_content = Some("{".to_string());

        let error = transaction
            .stage_rows(vec![row])
            .expect_err("invalid JSON should be rejected while staging");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error.description.contains("invalid JSON"),
            "error should explain invalid JSON: {error:?}"
        );
    }

    #[tokio::test]
    async fn commit_rejects_snapshot_that_violates_json_schema_without_sql() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let (live_state, _binary_cas, changelog, version_ref, runtime_functions, transaction) =
            open_test_transaction(&backend).await;

        let mut row = key_value_stage_row("schema-mismatch", "value", false);
        row.snapshot_content = Some(r#"{"key":"schema-mismatch"}"#.to_string());
        transaction
            .stage_rows(vec![row])
            .expect("row should stage before JSON Schema validation");

        let error = transaction
            .commit(&runtime_functions)
            .await
            .expect_err("JSON Schema mismatch should fail commit validation");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error
                .description
                .contains("snapshot_content validation failed"),
            "error should explain JSON Schema validation: {error:?}"
        );
        assert_no_persistence_after_validation_failure(
            storage.clone(),
            &live_state,
            &changelog,
            &version_ref,
        )
        .await;
    }

    #[tokio::test]
    async fn stage_rows_rejects_malformed_registered_schema_without_sql() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let (_live_state, _binary_cas, _changelog, _version_ref, _runtime_functions, transaction) =
            open_test_transaction(&backend).await;

        let mut row = key_value_stage_row("malformed-registered-schema", "value", false);
        row.schema_key = "lix_registered_schema".to_string();
        row.snapshot_content = Some(
            json!({
                "value": {
                    "x-lix-key": "malformed_registered_schema"
                }
            })
            .to_string(),
        );
        row.entity_id = None;

        let error = transaction
            .stage_rows(vec![row])
            .expect_err("malformed registered schema should be rejected while staging");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error.description.contains("x-lix-version")
                || error.description.contains("primary-key pointer"),
            "error should explain malformed registered schema: {error:?}"
        );
    }

    #[tokio::test]
    async fn stage_rows_rejects_primary_key_entity_id_mismatch_without_sql() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let (_live_state, _binary_cas, _changelog, _version_ref, _runtime_functions, transaction) =
            open_test_transaction(&backend).await;

        let mut row = key_value_stage_row("right-id", "value", false);
        row.entity_id = Some(crate::entity_identity::EntityIdentity::single("wrong-id"));

        let error = transaction
            .stage_rows(vec![row])
            .expect_err("entity id mismatch should be rejected while staging");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(
            error
                .description
                .contains("does not match x-lix-primary-key derived entity_id"),
            "error should explain entity id mismatch: {error:?}"
        );
    }

    async fn open_test_transaction(
        backend: &Arc<dyn Backend + Send + Sync>,
    ) -> (
        Arc<LiveStateContext>,
        Arc<BinaryCasContext>,
        Arc<ChangelogContext>,
        Arc<VersionContext>,
        FunctionContext,
        Transaction,
    ) {
        let storage = StorageContext::new(Arc::clone(backend));
        let live_state = Arc::new(live_state_context());
        let binary_cas = Arc::new(BinaryCasContext::new());
        let changelog = Arc::new(ChangelogContext::new());
        let version_ctx = Arc::new(VersionContext::new(Arc::new(UntrackedStateContext::new())));
        let schema_registry = Arc::new(SchemaRegistry::new());
        let opened = open_transaction(
            &SessionMode::Pinned {
                version_id: GLOBAL_VERSION_ID.to_string(),
            },
            storage,
            Arc::clone(&live_state),
            Arc::new(crate::tracked_state::TrackedStateContext::new()),
            Arc::clone(&binary_cas),
            Arc::clone(&changelog),
            Arc::clone(&version_ctx),
            schema_registry,
        )
        .await
        .expect("transaction should open");
        let transaction = opened.transaction;
        let runtime_functions = opened.runtime_functions;

        (
            live_state,
            binary_cas,
            changelog,
            version_ctx,
            runtime_functions,
            transaction,
        )
    }

    async fn assert_no_persistence_after_validation_failure(
        storage: StorageContext,
        live_state: &LiveStateContext,
        changelog: &ChangelogContext,
        version_ctx: &VersionContext,
    ) {
        let changes = changelog
            .reader(storage.clone())
            .scan_changes(&ChangelogScanRequest::default())
            .await
            .expect("changelog should scan after failed commit");
        assert!(
            changes.is_empty(),
            "validation failure must happen before changelog persistence"
        );
        let head = version_ctx
            .ref_reader(storage.clone())
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref should load after failed commit");
        assert_eq!(
            head, None,
            "validation failure must happen before version-ref persistence"
        );
        let row = live_state
            .reader(storage)
            .load_row(&crate::live_state::LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single("schema-mismatch"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("live state should load after failed commit");
        assert_eq!(
            row, None,
            "validation failure must happen before live-state persistence"
        );
    }

    fn key_value_stage_row(key: &str, value: &str, untracked: bool) -> StageRow {
        StageRow {
            entity_id: Some(crate::entity_identity::EntityIdentity::single(key)),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some(
                json!({
                    "key": key,
                    "value": value,
                })
                .to_string(),
            ),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: None,
            updated_at: None,
            global: true,
            change_id: None,
            commit_id: None,
            untracked,
            version_id: GLOBAL_VERSION_ID.to_string(),
        }
    }
}
