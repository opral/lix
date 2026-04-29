use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::backend::TransactionBeginMode;
use crate::binary_cas::{BinaryCasContext, BlobDataReader};
use crate::engine2::changelog::{ChangelogContext, ChangelogReader};
use crate::engine2::commit_graph::{CommitGraphContext, CommitGraphReader};
use crate::engine2::functions::{FunctionContext, FunctionProviderHandle};
use crate::engine2::live_state::{LiveStateContext, LiveStateReader};
use crate::engine2::schema_registry::SchemaRegistry;
use crate::engine2::transaction::commit;
use crate::engine2::transaction::live_state_overlay::TransactionLiveStateContext;
use crate::engine2::transaction::staging::TransactionStagedWrites;
use crate::engine2::transaction::types::{StageRow, StageWrite, StageWriteStager};
use crate::engine2::version_ref::{VersionRefContext, VersionRefReader};
use crate::sql2::SqlExecutionContext;
use crate::transaction::TransactionCommitOutcome;
use crate::{LixBackend, LixBackendTransaction, LixError};

/// One execution-scoped write transaction for the engine2 SQL path.
///
/// This is intentionally not a session-wide kitchen sink. It owns the backend
/// write transaction for one `SessionContext::execute(...)` call and projects
/// staged SQL writes back into the SQL DAG through an engine2-local live-state
/// overlay.
pub(crate) struct Transaction<'a> {
    active_version_id: String,
    backend: &'a Arc<dyn LixBackend + Send + Sync>,
    live_state: Arc<LiveStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    changelog: Arc<ChangelogContext>,
    version_ref: Arc<VersionRefContext>,
    staged_writes: Arc<TransactionStagedWrites>,
    backend_transaction: Box<dyn LixBackendTransaction + 'a>,
    visible_schemas: Vec<JsonValue>,
    functions: FunctionProviderHandle,
}

impl<'a> Transaction<'a> {
    /// Opens a backend write transaction and creates an execution-scoped
    /// staging area for SQL provider hooks.
    pub(crate) async fn open(
        active_version_id: String,
        backend: &'a Arc<dyn LixBackend + Send + Sync>,
        live_state: Arc<LiveStateContext>,
        binary_cas: Arc<BinaryCasContext>,
        changelog: Arc<ChangelogContext>,
        version_ref: Arc<VersionRefContext>,
        schema_registry: Arc<SchemaRegistry>,
        functions: FunctionProviderHandle,
    ) -> Result<Self, LixError> {
        let staged_writes = Arc::new(TransactionStagedWrites::new(functions.clone()));
        let visible_live_state =
            transaction_live_state(backend, Arc::clone(&live_state), Arc::clone(&staged_writes))?;
        let visible_schemas = schema_registry
            .visible_schemas(visible_live_state, &active_version_id)
            .await?;
        let backend_transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await?;
        Ok(Self {
            active_version_id,
            backend,
            live_state,
            binary_cas,
            changelog,
            version_ref,
            staged_writes,
            backend_transaction,
            visible_schemas,
            functions,
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
        let staged_writes = self.staged_writes.drain()?;
        commit::commit_staged_writes(
            &self.binary_cas,
            &self.changelog,
            &self.live_state,
            &self.version_ref,
            self.backend_transaction.as_mut(),
            staged_writes,
        )
        .await?;
        runtime_functions
            .persist_if_needed(&mut self.live_state.writer(self.backend_transaction.as_mut()))
            .await?;
        self.backend_transaction.commit().await?;
        Ok(TransactionCommitOutcome::default())
    }

    /// Rolls back the backend transaction.
    ///
    /// This is the explicit failure path for a write execution. Dropping the
    /// buffered transaction without commit is not the API we want callers to
    /// rely on.
    #[allow(dead_code)]
    pub(crate) async fn rollback(self) -> Result<(), LixError> {
        self.backend_transaction.rollback().await
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
        self.stage_write(StageWrite::Rows { rows })
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

    /// Exposes this transaction's KV snapshot to engine2 storage readers.
    ///
    /// Programmatic write APIs use this when a read influences staged writes,
    /// for example reading the current version head before creating a new
    /// version ref. Keeping that read inside the same backend transaction
    /// avoids a stale read/write split.
    pub(crate) fn kv_store(&mut self) -> &mut dyn LixBackendTransaction {
        self.backend_transaction.as_mut()
    }
}

impl SqlExecutionContext for Transaction<'_> {
    /// Returns the version that active-version SQL surfaces should resolve to.
    fn active_version_id(&self) -> &str {
        &self.active_version_id
    }

    /// Returns live state with this transaction's staged rows overlaid on top.
    fn live_state(&self) -> Arc<dyn LiveStateReader> {
        transaction_live_state(
            self.backend,
            Arc::clone(&self.live_state),
            Arc::clone(&self.staged_writes),
        )
        .expect("engine2 transaction should build staging overlay")
    }

    fn changelog(&self) -> Arc<dyn ChangelogReader> {
        Arc::new(self.changelog.reader(Arc::clone(self.backend)))
    }

    fn commit_graph(&self) -> Box<dyn CommitGraphReader> {
        Box::new(CommitGraphContext::new(ChangelogContext::new()).reader(Arc::clone(self.backend)))
    }

    fn version_ref(&self) -> Arc<dyn VersionRefReader> {
        Arc::new(self.version_ref.reader(Arc::clone(self.backend)))
    }

    /// Returns the same function provider used by the owning session.
    fn functions(&self) -> FunctionProviderHandle {
        self.functions.clone()
    }

    /// Provides blob reads for file/data surfaces during SQL execution.
    fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
        Arc::new(self.binary_cas.reader(Arc::clone(self.backend))) as Arc<dyn BlobDataReader>
    }

    /// Provides the transaction-scoped write stager used by DataFusion provider
    /// hooks while this statement executes.
    fn write_stager(&self) -> Option<Arc<dyn StageWriteStager>> {
        Some(Arc::clone(&self.staged_writes) as Arc<dyn StageWriteStager>)
    }

    /// Returns the transaction-scoped schema snapshot for SQL surface
    /// registration.
    fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
        Ok(self.visible_schemas.clone())
    }
}

fn transaction_live_state(
    backend: &Arc<dyn LixBackend + Send + Sync>,
    live_state: Arc<LiveStateContext>,
    staged_writes: Arc<TransactionStagedWrites>,
) -> Result<Arc<dyn LiveStateReader>, LixError> {
    let staged = staged_writes.staging_overlay()?;
    let base: Arc<dyn LiveStateReader> = Arc::new(live_state.reader(Arc::clone(backend)));
    Ok(Arc::new(TransactionLiveStateContext::new(base, staged)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::backend::testing::UnitTestBackend;
    use crate::engine2::changelog::ChangelogScanRequest;
    use crate::engine2::tracked_state::{TrackedStateRowRequest, TrackedStateScanRequest};
    use crate::engine2::untracked_state::{UntrackedStateContext, UntrackedStateRowRequest};
    use crate::engine2::version_ref::VersionRefContext;
    use crate::version::GLOBAL_VERSION_ID;
    use crate::NullableKeyFilter;

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            crate::engine2::tracked_state::TrackedStateContext::new(),
            crate::engine2::untracked_state::UntrackedStateContext::new(),
            crate::engine2::commit_graph::CommitGraphContext::new(
                crate::engine2::changelog::ChangelogContext::new(),
            ),
        )
    }

    #[tokio::test]
    async fn stage_rows_routes_tracked_and_untracked_rows_without_sql() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = Arc::new(live_state_context());
        let binary_cas = Arc::new(BinaryCasContext::new());
        let changelog = Arc::new(ChangelogContext::new());
        let version_ref = Arc::new(VersionRefContext::new(Arc::new(
            UntrackedStateContext::new(),
        )));
        let schema_registry = Arc::new(SchemaRegistry::new());
        let runtime_live_state = live_state.reader(Arc::clone(&backend));
        let runtime_functions = FunctionContext::prepare(&runtime_live_state).await;
        let runtime_functions = runtime_functions.expect("runtime functions should prepare");

        let transaction = Transaction::open(
            GLOBAL_VERSION_ID.to_string(),
            &backend,
            Arc::clone(&live_state),
            Arc::clone(&binary_cas),
            Arc::clone(&changelog),
            Arc::clone(&version_ref),
            Arc::clone(&schema_registry),
            runtime_functions.provider(),
        )
        .await
        .expect("transaction should open");

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
            .reader(Arc::clone(&backend))
            .scan_changes(&ChangelogScanRequest::default())
            .await
            .expect("changelog should scan");
        assert!(
            changes
                .iter()
                .any(|change| change.entity_id == "tracked-programmatic"),
            "tracked staged row should be appended to changelog"
        );
        assert!(
            !changes
                .iter()
                .any(|change| change.entity_id == "untracked-programmatic"),
            "untracked staged row must not be appended to changelog"
        );

        let head_commit_id = version_ref
            .reader(Arc::clone(&backend))
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref should load")
            .expect("tracked commit should advance the global version ref");

        let tracked_row = crate::engine2::tracked_state::TrackedStateContext::new()
            .reader(Arc::clone(&backend))
            .load_row_at_commit(
                &head_commit_id,
                &TrackedStateRowRequest {
                    schema_key: "lix_key_value".to_string(),
                    entity_id: "tracked-programmatic".to_string(),
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

        let untracked_row = crate::engine2::untracked_state::UntrackedStateContext::new()
            .reader(Arc::clone(&backend))
            .load_row(&UntrackedStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: "untracked-programmatic".to_string(),
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
            .reader(Arc::clone(&backend))
            .load_row(&crate::engine2::live_state::LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: "untracked-programmatic".to_string(),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("live state should load")
            .expect("untracked row should be visible through live state");
        assert!(live_untracked_row.untracked);
        assert!(live_untracked_row.global);
        assert_eq!(live_untracked_row.version_id, GLOBAL_VERSION_ID);

        let tracked_rows = crate::engine2::tracked_state::TrackedStateContext::new()
            .reader(Arc::clone(&backend))
            .scan_rows_at_commit(&head_commit_id, &TrackedStateScanRequest::default())
            .await
            .expect("tracked state should scan");
        assert!(
            tracked_rows
                .iter()
                .all(|row| row.entity_id != "untracked-programmatic"),
            "untracked staged rows should not be written into tracked state"
        );
    }

    fn key_value_stage_row(key: &str, value: &str, untracked: bool) -> StageRow {
        StageRow {
            entity_id: key.to_string(),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            plugin_key: None,
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
