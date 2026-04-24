use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use serde_json::Value as JsonValue;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::binary_cas::BlobDataReader;
use crate::history::{StateHistoryRequest, StateHistoryRow};
use crate::live_state::LiveStateContext;
use crate::sql::{
    MutationOperation, MutationRow, OptionalTextPatch, PlannedFilesystemFile,
    PlannedFilesystemState,
};
use crate::transaction::{
    build_direct_mutation_transaction_write_delta,
    build_direct_mutation_transaction_write_delta_with_filesystem_state,
    PreparedWriteStatementStager,
};
use crate::{LixError, QueryResult, Value};

use super::directory_provider::register_lix_directory_providers;
use super::entity_provider::register_entity_providers;
use super::file_provider::register_lix_file_providers;
use super::history_provider::register_history_providers;
use super::lix_state_provider::register_lix_state_providers;
use super::udf::register_sql2_udfs;

/// Single execution boundary for `sql2::execute_sql(...)`.
///
/// Session and transaction orchestration stay above `sql2`. They provide the
/// execution-scoped visible live-state context for each call.
///
/// Catalog lookup/registration will likely join this boundary later, but we
/// are intentionally not carrying it yet until the new DataFusion-owned path
/// actually needs it.
#[allow(dead_code)]
pub(crate) trait SqlExecutionContext {
    fn active_version_id(&self) -> &str;
    fn live_state(&self) -> Arc<dyn LiveStateContext>;
    fn history(&self) -> Option<Arc<dyn HistoryContext>> {
        None
    }
    fn blob_reader(&self) -> Arc<dyn BlobDataReader>;
    fn write_stager(&self) -> Option<Arc<dyn SqlWriteStager>> {
        None
    }
    fn list_visible_schemas(&self, version_id: &str) -> Result<Vec<JsonValue>, LixError>;
}

#[async_trait]
#[allow(dead_code)]
pub(crate) trait HistoryContext: Send + Sync {
    async fn scan_state_history(
        &self,
        request: &StateHistoryRequest,
    ) -> Result<Vec<StateHistoryRow>, LixError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SqlWriteIntent {
    WriteRows {
        rows: Vec<StateRow>,
    },
    WriteRowsWithFileData {
        rows: Vec<StateRow>,
        file_data: Vec<FileDataWrite>,
        count: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) schema_version: Option<String>,
    pub(crate) created_at: Option<String>,
    pub(crate) updated_at: Option<String>,
    pub(crate) global: bool,
    pub(crate) change_id: Option<String>,
    pub(crate) commit_id: Option<String>,
    pub(crate) untracked: bool,
    pub(crate) version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileDataWrite {
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlWriteOutcome {
    pub(crate) count: u64,
}

/// Execution-scoped authority for staging SQL writes into the current Lix
/// transaction.
///
/// `LiveStateContext` stays read-only and visibility-oriented. Write execution
/// plans use this boundary to stage mutations through the transaction pipeline.
#[async_trait]
#[allow(dead_code)]
pub(crate) trait SqlWriteStager: Send + Sync {
    async fn stage_write(&self, write: SqlWriteIntent) -> Result<SqlWriteOutcome, LixError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlStatementKind {
    Read,
    Write,
    Other,
}

#[allow(dead_code)]
pub(crate) struct SqlLogicalPlan {
    session: SessionContext,
    plan: LogicalPlan,
    kind: SqlStatementKind,
}

impl SqlLogicalPlan {
    #[allow(dead_code)]
    pub(crate) fn kind(&self) -> SqlStatementKind {
        self.kind
    }

    #[allow(dead_code)]
    pub(crate) fn is_write(&self) -> bool {
        self.kind == SqlStatementKind::Write
    }
}

#[async_trait]
impl<T> SqlWriteStager for Mutex<T>
where
    T: PreparedWriteStatementStager + Send + 'static,
{
    async fn stage_write(&self, write: SqlWriteIntent) -> Result<SqlWriteOutcome, LixError> {
        let mut stager = self.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire buffered write stager lock",
            )
        })?;
        stage_decoded_write(&mut *stager, write)
    }
}

pub(crate) fn stage_decoded_write(
    stager: &mut dyn PreparedWriteStatementStager,
    write: SqlWriteIntent,
) -> Result<SqlWriteOutcome, LixError> {
    match write {
        SqlWriteIntent::WriteRows { rows } => {
            let count = rows.len() as u64;
            let mutations = rows
                .into_iter()
                .map(mutation_row_from_state_row)
                .collect::<Result<Vec<_>, _>>()?;
            let delta = build_direct_mutation_transaction_write_delta(mutations, None)?;
            stager.stage_transaction_write_delta(delta)?;
            Ok(SqlWriteOutcome { count })
        }
        SqlWriteIntent::WriteRowsWithFileData {
            rows,
            file_data,
            count,
        } => {
            let mutations = rows
                .into_iter()
                .map(mutation_row_from_state_row)
                .collect::<Result<Vec<_>, _>>()?;
            let filesystem_state = filesystem_state_from_file_data_writes(file_data);
            let delta = build_direct_mutation_transaction_write_delta_with_filesystem_state(
                mutations,
                None,
                filesystem_state,
            )?;
            stager.stage_transaction_write_delta(delta)?;
            Ok(SqlWriteOutcome { count })
        }
    }
}

fn filesystem_state_from_file_data_writes(file_data: Vec<FileDataWrite>) -> PlannedFilesystemState {
    let mut filesystem_state = PlannedFilesystemState::default();
    for write in file_data {
        filesystem_state.files.insert(
            (write.file_id.clone(), write.version_id.clone()),
            PlannedFilesystemFile {
                file_id: write.file_id,
                version_id: write.version_id,
                untracked: write.untracked,
                descriptor: None,
                metadata_patch: OptionalTextPatch::Unchanged,
                data: Some(write.data),
                deleted: false,
            },
        );
    }
    filesystem_state
}

fn mutation_row_from_state_row(row: StateRow) -> Result<MutationRow, LixError> {
    reject_read_only_lix_state_insert_field("created_at", &row.created_at)?;
    reject_read_only_lix_state_insert_field("updated_at", &row.updated_at)?;
    reject_read_only_lix_state_insert_field("change_id", &row.change_id)?;
    reject_read_only_lix_state_insert_field("commit_id", &row.commit_id)?;
    let schema_version = row.schema_version.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "INSERT into lix_state requires schema_version before staging",
        )
    })?;
    let operation = if row.snapshot_content.is_none() {
        MutationOperation::Delete
    } else {
        MutationOperation::Insert
    };
    let snapshot_content = row
        .snapshot_content
        .map(|snapshot| {
            serde_json::from_str::<JsonValue>(&snapshot).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("INSERT into lix_state has invalid snapshot_content JSON: {error}"),
                )
            })
        })
        .transpose()?;

    Ok(MutationRow {
        operation,
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        schema_version,
        file_id: row.file_id,
        version_id: row.version_id,
        plugin_key: row.plugin_key,
        snapshot_content,
        metadata: row.metadata,
        untracked: row.untracked,
    })
}

fn reject_read_only_lix_state_insert_field(
    field_name: &str,
    value: &Option<String>,
) -> Result<(), LixError> {
    if value.is_some() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("INSERT into lix_state cannot stage read-only column '{field_name}'"),
        ));
    }
    Ok(())
}

/// Minimal top-level sql2 entrypoint.
///
/// The final implementation will build the DataFusion session from the
/// execution context and source rows from `live_state()`.
///
/// `catalog()` is intentionally omitted from the MVP boundary for now.
#[allow(dead_code)]
pub(crate) async fn execute_sql(
    ctx: &dyn SqlExecutionContext,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    let plan = create_logical_plan(ctx, sql).await?;
    execute_logical_plan(ctx, plan, params).await
}

pub(crate) async fn create_logical_plan(
    ctx: &dyn SqlExecutionContext,
    sql: &str,
) -> Result<SqlLogicalPlan, LixError> {
    let session = build_session(ctx).await?;
    let plan = session
        .state()
        .create_logical_plan(sql)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let kind = classify_logical_plan(&plan);

    Ok(SqlLogicalPlan {
        session,
        plan,
        kind,
    })
}

pub(crate) async fn execute_logical_plan(
    _ctx: &dyn SqlExecutionContext,
    plan: SqlLogicalPlan,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    let SqlLogicalPlan {
        session,
        plan,
        kind: _,
    } = plan;

    let mut dataframe = session
        .execute_logical_plan(plan)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    if !params.is_empty() {
        dataframe = dataframe
            .with_param_values(
                params
                    .iter()
                    .map(scalar_value_from_lix_value)
                    .collect::<Vec<_>>(),
            )
            .map_err(datafusion_error_to_lix_error)?;
    }

    let result_columns = dataframe
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    let batches = dataframe
        .collect()
        .await
        .map_err(datafusion_error_to_lix_error)?;
    query_result_from_batches(&result_columns, &batches)
}

async fn build_session(ctx: &dyn SqlExecutionContext) -> Result<SessionContext, LixError> {
    let session = SessionContext::new();
    register_sql2_udfs(&session);
    let history = ctx.history();
    register_lix_state_providers(
        &session,
        ctx.active_version_id(),
        ctx.live_state(),
        ctx.write_stager(),
    )
    .await?;
    let state_history_provider = register_history_providers(
        &session,
        ctx.active_version_id(),
        history.as_ref().map(Arc::clone),
    )
    .await?;
    register_lix_directory_providers(
        &session,
        ctx.active_version_id(),
        ctx.live_state(),
        ctx.write_stager(),
        history.as_ref().map(Arc::clone),
    )
    .await?;
    register_lix_file_providers(
        &session,
        ctx.active_version_id(),
        ctx.live_state(),
        ctx.blob_reader(),
        ctx.write_stager(),
        history.as_ref().map(Arc::clone),
    )
    .await?;
    register_entity_providers(
        &session,
        ctx.active_version_id(),
        ctx.live_state(),
        ctx.write_stager(),
        state_history_provider.is_some(),
        &ctx.list_visible_schemas(ctx.active_version_id())?,
    )
    .await?;

    Ok(session)
}

fn classify_logical_plan(plan: &LogicalPlan) -> SqlStatementKind {
    match plan {
        LogicalPlan::Dml(_) => SqlStatementKind::Write,
        LogicalPlan::Ddl(_) | LogicalPlan::Statement(_) | LogicalPlan::Copy(_) => {
            SqlStatementKind::Other
        }
        _ => SqlStatementKind::Read,
    }
}

fn scalar_value_from_lix_value(value: &Value) -> ScalarValue {
    match value {
        Value::Null => ScalarValue::Null,
        Value::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        Value::Integer(value) => ScalarValue::Int64(Some(*value)),
        Value::Real(value) => ScalarValue::Float64(Some(*value)),
        Value::Text(value) => ScalarValue::Utf8(Some(value.clone())),
        Value::Json(value) => ScalarValue::Utf8(Some(value.to_string())),
        Value::Blob(value) => ScalarValue::Binary(Some(value.clone())),
    }
}

fn datafusion_error_to_lix_error(error: datafusion::error::DataFusionError) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

fn query_result_from_batches(
    result_columns: &[String],
    batches: &[RecordBatch],
) -> Result<QueryResult, LixError> {
    let mut rows = Vec::<Vec<Value>>::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::<Value>::with_capacity(batch.num_columns());
            for array in batch.columns() {
                let scalar = ScalarValue::try_from_array(array.as_ref(), row_index)
                    .map_err(datafusion_error_to_lix_error)?;
                row.push(scalar_value_to_lix_value(&scalar));
            }
            rows.push(row);
        }
    }

    Ok(QueryResult {
        rows,
        columns: result_columns.to_vec(),
    })
}

fn scalar_value_to_lix_value(value: &ScalarValue) -> Value {
    match value {
        ScalarValue::Null => Value::Null,
        ScalarValue::Boolean(Some(value)) => Value::Boolean(*value),
        ScalarValue::Boolean(None) => Value::Null,
        ScalarValue::Int8(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int8(None) => Value::Null,
        ScalarValue::Int16(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int16(None) => Value::Null,
        ScalarValue::Int32(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int32(None) => Value::Null,
        ScalarValue::Int64(Some(value)) => Value::Integer(*value),
        ScalarValue::Int64(None) => Value::Null,
        ScalarValue::UInt8(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt8(None) => Value::Null,
        ScalarValue::UInt16(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt16(None) => Value::Null,
        ScalarValue::UInt32(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt32(None) => Value::Null,
        ScalarValue::UInt64(Some(value)) => match i64::try_from(*value) {
            Ok(value) => Value::Integer(value),
            Err(_) => Value::Text(value.to_string()),
        },
        ScalarValue::UInt64(None) => Value::Null,
        ScalarValue::Float32(Some(value)) => Value::Real(f64::from(*value)),
        ScalarValue::Float32(None) => Value::Null,
        ScalarValue::Float64(Some(value)) => Value::Real(*value),
        ScalarValue::Float64(None) => Value::Null,
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Value::Text(value.clone()),
        ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) | ScalarValue::LargeUtf8(None) => {
            Value::Null
        }
        ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value)) => {
            Value::Blob(value.clone())
        }
        ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => Value::Null,
        other => Value::Text(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use serde_json::json;
    use serde_json::Value as JsonValue;

    use super::{
        execute_sql, stage_decoded_write, HistoryContext, SqlExecutionContext, SqlWriteIntent,
        SqlWriteStager, StateRow,
    };
    use crate::binary_cas::BlobDataReader;
    use crate::history::{
        StateHistoryContentMode, StateHistoryLineageScope, StateHistoryRequest, StateHistoryRow,
        StateHistoryVersionScope,
    };
    use crate::live_state::{
        CommittedLiveStateContext, ExactRowRequest, LiveRow, LiveStateContext, LiveStateScanRequest,
    };
    use crate::test_support::boot_test_engine;
    use crate::transaction::{PendingOverlay, PreparedWriteStatementStager, TransactionWriteDelta};
    use crate::{CreateVersionOptions, LixError, Value};

    struct DummyBlobReader;
    struct DummyLiveStateContext;
    struct RowsLiveStateContext {
        rows: Vec<LiveRow>,
    }
    struct RowsHistoryContext {
        rows: Vec<StateHistoryRow>,
        requests: Arc<Mutex<Vec<StateHistoryRequest>>>,
    }
    struct RowsBlobReader {
        blobs: BTreeMap<String, Vec<u8>>,
    }
    struct BackendBlobReader(Arc<dyn crate::LixBackend + Send + Sync>);
    #[derive(Default)]
    struct CapturingPreparedWriteStager {
        deltas: Vec<TransactionWriteDelta>,
        refresh_pending: bool,
    }

    struct DummySqlExecutionContext<'a> {
        active_version_id: &'a str,
        blob_reader: Arc<dyn BlobDataReader>,
        live_state: Arc<dyn LiveStateContext>,
        write_stager: Option<Arc<dyn SqlWriteStager>>,
        schema_definitions: Vec<JsonValue>,
    }

    struct HistorySqlExecutionContext<'a> {
        active_version_id: &'a str,
        blob_reader: Arc<dyn BlobDataReader>,
        live_state: Arc<dyn LiveStateContext>,
        history: Arc<dyn HistoryContext>,
        schema_definitions: Vec<JsonValue>,
    }

    impl<'a> SqlExecutionContext for DummySqlExecutionContext<'a> {
        fn active_version_id(&self) -> &str {
            self.active_version_id
        }

        fn live_state(&self) -> Arc<dyn LiveStateContext> {
            Arc::clone(&self.live_state)
        }

        fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
            Arc::clone(&self.blob_reader)
        }

        fn write_stager(&self) -> Option<Arc<dyn SqlWriteStager>> {
            self.write_stager.as_ref().map(Arc::clone)
        }

        fn list_visible_schemas(&self, version_id: &str) -> Result<Vec<JsonValue>, LixError> {
            let _ = version_id;
            Ok(self.schema_definitions.clone())
        }
    }

    impl<'a> SqlExecutionContext for HistorySqlExecutionContext<'a> {
        fn active_version_id(&self) -> &str {
            self.active_version_id
        }

        fn live_state(&self) -> Arc<dyn LiveStateContext> {
            Arc::clone(&self.live_state)
        }

        fn history(&self) -> Option<Arc<dyn HistoryContext>> {
            Some(Arc::clone(&self.history))
        }

        fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
            Arc::clone(&self.blob_reader)
        }

        fn list_visible_schemas(&self, version_id: &str) -> Result<Vec<JsonValue>, LixError> {
            let _ = version_id;
            Ok(self.schema_definitions.clone())
        }
    }

    impl PreparedWriteStatementStager for CapturingPreparedWriteStager {
        fn mark_public_surface_registry_refresh_pending(&mut self) {
            self.refresh_pending = true;
        }

        fn stage_transaction_write_delta(
            &mut self,
            delta: TransactionWriteDelta,
        ) -> Result<(), LixError> {
            self.deltas.push(delta);
            Ok(())
        }
    }

    #[async_trait]
    impl LiveStateContext for DummyLiveStateContext {
        async fn scan(&self, _request: &LiveStateScanRequest) -> Result<Vec<LiveRow>, LixError> {
            Ok(vec![])
        }

        async fn load_exact(
            &self,
            _request: &ExactRowRequest,
        ) -> Result<Option<LiveRow>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl LiveStateContext for RowsLiveStateContext {
        async fn scan(&self, _request: &LiveStateScanRequest) -> Result<Vec<LiveRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_exact(
            &self,
            _request: &ExactRowRequest,
        ) -> Result<Option<LiveRow>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl BlobDataReader for DummyBlobReader {
        async fn load_blob_data_by_hash(
            &self,
            _blob_hash: &str,
        ) -> Result<Option<Vec<u8>>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl BlobDataReader for RowsBlobReader {
        async fn load_blob_data_by_hash(
            &self,
            blob_hash: &str,
        ) -> Result<Option<Vec<u8>>, LixError> {
            Ok(self.blobs.get(blob_hash).cloned())
        }
    }

    #[async_trait]
    impl BlobDataReader for BackendBlobReader {
        async fn load_blob_data_by_hash(
            &self,
            blob_hash: &str,
        ) -> Result<Option<Vec<u8>>, LixError> {
            crate::binary_cas::load_blob_data_by_hash(self.0.as_ref(), blob_hash).await
        }
    }

    #[async_trait]
    impl HistoryContext for RowsHistoryContext {
        async fn scan_state_history(
            &self,
            request: &StateHistoryRequest,
        ) -> Result<Vec<StateHistoryRow>, LixError> {
            self.requests
                .lock()
                .expect("history request lock")
                .push(request.clone());
            Ok(self
                .rows
                .iter()
                .filter(|row| {
                    request.schema_keys.is_empty()
                        || request
                            .schema_keys
                            .iter()
                            .any(|schema_key| schema_key == &row.schema_key)
                })
                .cloned()
                .collect())
        }
    }

    fn minimal_lix_state_write_row() -> StateRow {
        StateRow {
            entity_id: "entity-1".to_string(),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some("{\"key\":\"hello\",\"value\":\"world\"}".to_string()),
            metadata: None,
            schema_version: Some("1".to_string()),
            created_at: None,
            updated_at: None,
            global: false,
            change_id: None,
            commit_id: None,
            untracked: false,
            version_id: "version-a".to_string(),
        }
    }

    fn live_lix_state_row(entity_id: &str, metadata: Option<&str>) -> LiveRow {
        LiveRow {
            entity_id: entity_id.to_string(),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some("{\"key\":\"hello\",\"value\":\"world\"}".to_string()),
            metadata: metadata.map(ToOwned::to_owned),
            schema_version: "1".to_string(),
            version_id: "version-a".to_string(),
            change_id: Some(format!("change-{entity_id}")),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: Some("2026-04-23T00:00:00Z".to_string()),
            updated_at: Some("2026-04-23T01:00:00Z".to_string()),
        }
    }

    fn live_entity_row(entity_id: &str, version_id: &str, value: &str) -> LiveRow {
        LiveRow {
            entity_id: entity_id.to_string(),
            schema_key: "test_state_schema".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: Some(format!("{{\"source\":\"{entity_id}\"}}")),
            schema_version: "1".to_string(),
            version_id: version_id.to_string(),
            change_id: Some(format!("change-{entity_id}")),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: Some("2026-04-23T00:00:00Z".to_string()),
            updated_at: Some("2026-04-23T01:00:00Z".to_string()),
        }
    }

    fn live_directory_row(
        entity_id: &str,
        version_id: &str,
        parent_id: Option<&str>,
        name: &str,
        hidden: bool,
    ) -> LiveRow {
        LiveRow {
            entity_id: entity_id.to_string(),
            schema_key: "lix_directory_descriptor".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(
                json!({
                    "id": entity_id,
                    "parent_id": parent_id,
                    "name": name,
                    "hidden": hidden
                })
                .to_string(),
            ),
            metadata: Some(format!("{{\"source\":\"{entity_id}\"}}")),
            schema_version: "1".to_string(),
            version_id: version_id.to_string(),
            change_id: Some(format!("change-{entity_id}")),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: Some("2026-04-23T00:00:00Z".to_string()),
            updated_at: Some("2026-04-23T01:00:00Z".to_string()),
        }
    }

    fn live_file_row(
        entity_id: &str,
        version_id: &str,
        directory_id: Option<&str>,
        name: &str,
        extension: Option<&str>,
        hidden: bool,
    ) -> LiveRow {
        LiveRow {
            entity_id: entity_id.to_string(),
            schema_key: "lix_file_descriptor".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(
                json!({
                    "id": entity_id,
                    "directory_id": directory_id,
                    "name": name,
                    "extension": extension,
                    "hidden": hidden
                })
                .to_string(),
            ),
            metadata: Some(format!("{{\"source\":\"{entity_id}\"}}")),
            schema_version: "1".to_string(),
            version_id: version_id.to_string(),
            change_id: Some(format!("change-{entity_id}")),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: Some("2026-04-23T00:00:00Z".to_string()),
            updated_at: Some("2026-04-23T01:00:00Z".to_string()),
        }
    }

    #[test]
    fn stage_decoded_write_stages_lix_state_insert_in_buffered_delta() {
        let mut stager = CapturingPreparedWriteStager::default();
        let mut row = minimal_lix_state_write_row();
        row.metadata = Some("{\"source\":\"sql\"}".to_string());

        let outcome =
            stage_decoded_write(&mut stager, SqlWriteIntent::WriteRows { rows: vec![row] })
                .expect("write intent should stage");

        assert_eq!(outcome.count, 1);
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-1");
        assert_eq!(rows[0].schema_version, "1");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"world\"}")
        );
        assert_eq!(rows[0].metadata.as_deref(), Some("{\"source\":\"sql\"}"));
    }

    #[test]
    fn stage_decoded_write_rejects_read_only_lix_state_columns() {
        let mut row = minimal_lix_state_write_row();
        row.change_id = Some("change-a".to_string());
        let mut stager = CapturingPreparedWriteStager::default();

        let error = stage_decoded_write(&mut stager, SqlWriteIntent::WriteRows { rows: vec![row] })
            .expect_err("read-only fields should be rejected");

        assert!(
            error.description.contains("read-only column 'change_id'"),
            "unexpected error: {error:?}"
        );
        assert!(stager.deltas.is_empty());
    }

    #[test]
    fn stage_decoded_write_stages_lix_state_delete_in_buffered_delta() {
        let mut stager = CapturingPreparedWriteStager::default();
        let mut row = minimal_lix_state_write_row();
        row.snapshot_content = None;
        row.metadata = Some("{\"source\":\"delete\"}".to_string());

        let outcome =
            stage_decoded_write(&mut stager, SqlWriteIntent::WriteRows { rows: vec![row] })
                .expect("delete intent should stage");

        assert_eq!(outcome.count, 1);
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-1");
        assert_eq!(rows[0].version_id, "version-a");
        assert!(rows[0].tombstone);
        assert_eq!(rows[0].snapshot_content, None);
        assert_eq!(rows[0].metadata.as_deref(), Some("{\"source\":\"delete\"}"));
    }

    #[tokio::test]
    async fn mutex_prepared_write_stager_implements_sql_write_stager() {
        let stager = Mutex::new(CapturingPreparedWriteStager::default());

        let outcome = stager
            .stage_write(SqlWriteIntent::WriteRows {
                rows: vec![minimal_lix_state_write_row()],
            })
            .await
            .expect("mutex stager should bridge into buffered staging");

        assert_eq!(outcome.count, 1);
        let stager = stager
            .into_inner()
            .expect("stager lock should not be poisoned");
        assert_eq!(stager.deltas.len(), 1);
    }

    #[tokio::test]
    async fn sql_execution_context_exposes_live_state_and_blob_reader() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateContext);
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader: Arc::clone(&blob_reader),
            live_state: Arc::clone(&live_state) as Arc<dyn LiveStateContext>,
            write_stager: None,
            schema_definitions: vec![],
        };

        let actual = ctx.live_state();
        let expected = live_state as Arc<dyn LiveStateContext>;
        assert_eq!(ctx.active_version_id(), "version-a");
        assert!(Arc::ptr_eq(&actual, &expected));
        assert!(Arc::ptr_eq(&ctx.blob_reader(), &blob_reader));
    }

    #[tokio::test]
    async fn execute_sql_uses_execution_context_boundary() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateContext);
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: None,
            schema_definitions: vec![],
        };

        let result = execute_sql(&ctx, "SELECT 1", &[])
            .await
            .expect("sql2 execute should support literal-only queries");
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
    }

    #[tokio::test]
    async fn execute_sql_reads_lix_state_history_from_history_context() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateContext);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let history = Arc::new(RowsHistoryContext {
            rows: vec![StateHistoryRow {
                entity_id: "entity-history".to_string(),
                schema_key: "test_state_schema".to_string(),
                file_id: Some("file-a".to_string()),
                plugin_key: None,
                snapshot_content: Some("{\"value\":\"A\"}".to_string()),
                metadata: Some("{\"source\":\"history\"}".to_string()),
                schema_version: "1".to_string(),
                change_id: "change-a".to_string(),
                commit_id: "commit-a".to_string(),
                commit_created_at: "2026-01-01T00:00:00Z".to_string(),
                root_commit_id: "root-a".to_string(),
                depth: 0,
                version_id: "version-a".to_string(),
            }],
            requests: Arc::clone(&requests),
        });
        let ctx = HistorySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            history,
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "SELECT entity_id, snapshot_content, metadata, depth, version_id \
             FROM lix_state_history \
             WHERE schema_key = 'test_state_schema' AND version_id = 'version-a' AND depth >= 0",
            &[],
        )
        .await
        .expect("sql2 execute should read lix_state_history through history context");

        assert_eq!(
            result.columns,
            vec![
                "entity_id",
                "snapshot_content",
                "metadata",
                "depth",
                "version_id"
            ]
        );
        assert_eq!(
            result.rows,
            vec![vec![
                Value::Text("entity-history".to_string()),
                Value::Text("{\"value\":\"A\"}".to_string()),
                Value::Text("{\"source\":\"history\"}".to_string()),
                Value::Integer(0),
                Value::Text("version-a".to_string()),
            ]]
        );

        let requests = requests.lock().expect("history request lock");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].lineage_scope,
            StateHistoryLineageScope::ActiveVersion
        );
        assert_eq!(requests[0].lineage_version_id.as_deref(), Some("version-a"));
        assert_eq!(requests[0].schema_keys, vec!["test_state_schema"]);
        assert_eq!(
            requests[0].version_scope,
            StateHistoryVersionScope::RequestedVersions(vec!["version-a".to_string()])
        );
        assert_eq!(requests[0].min_depth, Some(0));
        assert_eq!(
            requests[0].content_mode,
            StateHistoryContentMode::IncludeSnapshotContent
        );
    }

    #[tokio::test]
    async fn execute_sql_reads_entity_history_view_from_history_context() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateContext);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let history = Arc::new(RowsHistoryContext {
            rows: vec![StateHistoryRow {
                entity_id: "entity-history".to_string(),
                schema_key: "test_state_schema".to_string(),
                file_id: Some("file-a".to_string()),
                plugin_key: None,
                snapshot_content: Some("{\"count\":7,\"value\":\"A\"}".to_string()),
                metadata: Some("{\"source\":\"history\"}".to_string()),
                schema_version: "1".to_string(),
                change_id: "change-a".to_string(),
                commit_id: "commit-a".to_string(),
                commit_created_at: "2026-01-01T00:00:00Z".to_string(),
                root_commit_id: "root-a".to_string(),
                depth: 2,
                version_id: "version-a".to_string(),
            }],
            requests: Arc::clone(&requests),
        });
        let ctx = HistorySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            history,
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "count": { "type": "integer" },
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_sql(
            &ctx,
            "SELECT value, count, lixcol_entity_id, lixcol_root_commit_id, lixcol_depth, lixcol_version_id \
             FROM test_state_schema_history \
             WHERE lixcol_version_id = 'version-a'",
            &[],
        )
        .await
        .expect("sql2 execute should read entity history view through history context");

        assert_eq!(
            result.columns,
            vec![
                "value",
                "count",
                "lixcol_entity_id",
                "lixcol_root_commit_id",
                "lixcol_depth",
                "lixcol_version_id",
            ]
        );
        assert_eq!(
            result.rows,
            vec![vec![
                Value::Text("A".to_string()),
                Value::Integer(7),
                Value::Text("entity-history".to_string()),
                Value::Text("root-a".to_string()),
                Value::Integer(2),
                Value::Text("version-a".to_string()),
            ]]
        );

        let requests = requests.lock().expect("history request lock");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].schema_keys, vec!["test_state_schema"]);
        assert_eq!(requests[0].lineage_version_id.as_deref(), Some("version-a"));
    }

    #[tokio::test]
    async fn execute_sql_reads_directory_history_view_from_history_context() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateContext);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let history = Arc::new(RowsHistoryContext {
            rows: vec![StateHistoryRow {
                entity_id: "dir-docs".to_string(),
                schema_key: "lix_directory_descriptor".to_string(),
                file_id: None,
                plugin_key: None,
                snapshot_content: Some(
                    "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\",\"hidden\":false}"
                        .to_string(),
                ),
                metadata: None,
                schema_version: "1".to_string(),
                change_id: "change-dir".to_string(),
                commit_id: "commit-dir".to_string(),
                commit_created_at: "2026-01-01T00:00:00Z".to_string(),
                root_commit_id: "root-dir".to_string(),
                depth: 1,
                version_id: "version-a".to_string(),
            }],
            requests: Arc::clone(&requests),
        });
        let ctx = HistorySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            history,
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "SELECT id, parent_id, name, path, hidden, lixcol_root_commit_id, lixcol_depth, lixcol_version_id \
             FROM lix_directory_history \
             WHERE id = 'dir-docs'",
            &[],
        )
        .await
        .expect("sql2 execute should read directory history through history context");

        assert_eq!(
            result.columns,
            vec![
                "id",
                "parent_id",
                "name",
                "path",
                "hidden",
                "lixcol_root_commit_id",
                "lixcol_depth",
                "lixcol_version_id",
            ]
        );
        assert_eq!(
            result.rows,
            vec![vec![
                Value::Text("dir-docs".to_string()),
                Value::Null,
                Value::Text("docs".to_string()),
                Value::Text("/docs/".to_string()),
                Value::Boolean(false),
                Value::Text("root-dir".to_string()),
                Value::Integer(1),
                Value::Text("version-a".to_string()),
            ]]
        );

        let requests = requests.lock().expect("history request lock");
        assert!(!requests.is_empty());
        assert!(requests
            .iter()
            .any(|request| request.schema_keys == vec!["lix_directory_descriptor"]));
        assert!(requests
            .iter()
            .all(|request| request.lineage_version_id.as_deref() == Some("version-a")));
    }

    #[tokio::test]
    async fn execute_sql_reads_file_history_view_from_history_context() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(RowsBlobReader {
            blobs: BTreeMap::from([("blob-a".to_string(), b"hello".to_vec())]),
        });
        let live_state = Arc::new(DummyLiveStateContext);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let history = Arc::new(RowsHistoryContext {
            rows: vec![
                StateHistoryRow {
                    entity_id: "file-a".to_string(),
                    schema_key: "lix_file_descriptor".to_string(),
                    file_id: Some("file-a".to_string()),
                    plugin_key: None,
                    snapshot_content: Some(
                        "{\"id\":\"file-a\",\"directory_id\":\"dir-docs\",\"name\":\"readme\",\"extension\":\"md\",\"hidden\":false}"
                            .to_string(),
                    ),
                    metadata: None,
                    schema_version: "1".to_string(),
                    change_id: "change-file".to_string(),
                    commit_id: "commit-file".to_string(),
                    commit_created_at: "2026-01-01T00:00:00Z".to_string(),
                    root_commit_id: "root-file".to_string(),
                    depth: 1,
                    version_id: "version-a".to_string(),
                },
                StateHistoryRow {
                    entity_id: "dir-docs".to_string(),
                    schema_key: "lix_directory_descriptor".to_string(),
                    file_id: None,
                    plugin_key: None,
                    snapshot_content: Some(
                        "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\",\"hidden\":false}"
                            .to_string(),
                    ),
                    metadata: None,
                    schema_version: "1".to_string(),
                    change_id: "change-dir".to_string(),
                    commit_id: "commit-dir".to_string(),
                    commit_created_at: "2026-01-01T00:00:00Z".to_string(),
                    root_commit_id: "root-file".to_string(),
                    depth: 1,
                    version_id: "version-a".to_string(),
                },
                StateHistoryRow {
                    entity_id: "blob-ref-a".to_string(),
                    schema_key: "lix_binary_blob_ref".to_string(),
                    file_id: Some("file-a".to_string()),
                    plugin_key: None,
                    snapshot_content: Some(
                        "{\"id\":\"file-a\",\"blob_hash\":\"blob-a\",\"size_bytes\":5}"
                            .to_string(),
                    ),
                    metadata: None,
                    schema_version: "1".to_string(),
                    change_id: "change-blob".to_string(),
                    commit_id: "commit-blob".to_string(),
                    commit_created_at: "2026-01-01T00:00:01Z".to_string(),
                    root_commit_id: "root-file".to_string(),
                    depth: 0,
                    version_id: "version-a".to_string(),
                },
            ],
            requests: Arc::clone(&requests),
        });
        let ctx = HistorySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            history,
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "SELECT id, path, data, hidden, lixcol_root_commit_id, lixcol_depth, lixcol_version_id \
             FROM lix_file_history \
             WHERE id = 'file-a' \
             ORDER BY lixcol_depth",
            &[],
        )
        .await
        .expect("sql2 execute should read file history through history context");

        assert_eq!(
            result.columns,
            vec![
                "id",
                "path",
                "data",
                "hidden",
                "lixcol_root_commit_id",
                "lixcol_depth",
                "lixcol_version_id",
            ]
        );
        assert_eq!(
            result.rows,
            vec![vec![
                Value::Text("file-a".to_string()),
                Value::Text("/docs/readme.md".to_string()),
                Value::Blob(b"hello".to_vec()),
                Value::Boolean(false),
                Value::Text("root-file".to_string()),
                Value::Integer(0),
                Value::Text("version-a".to_string()),
            ]]
        );

        let requests = requests.lock().expect("history request lock");
        assert!(requests
            .iter()
            .any(|request| request.schema_keys == vec!["lix_file_descriptor"]));
        assert!(requests
            .iter()
            .any(|request| request.schema_keys == vec!["lix_directory_descriptor"]));
        assert!(requests
            .iter()
            .any(|request| request.schema_keys == vec!["lix_binary_blob_ref"]));
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_values_stages_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateContext);
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "INSERT INTO lix_state (\
             entity_id, schema_key, file_id, plugin_key, snapshot_content, metadata, schema_version, global, untracked\
             ) VALUES (\
             'entity-1', 'lix_key_value', NULL, NULL, '{\"key\":\"hello\",\"value\":\"world\"}', '{\"source\":\"sql\"}', '1', false, false\
             )",
            &[],
        )
        .await
        .expect("INSERT INTO lix_state VALUES should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-1");
        assert_eq!(rows[0].schema_version, "1");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"world\"}")
        );
        assert_eq!(rows[0].metadata.as_deref(), Some("{\"source\":\"sql\"}"));
    }

    #[tokio::test]
    async fn execute_sql_insert_into_lix_state_select_stages_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateContext);
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "INSERT INTO lix_state (\
             entity_id, schema_key, file_id, plugin_key, snapshot_content, metadata, schema_version, global, untracked\
             ) \
             SELECT \
             'entity-from-select' AS entity_id, \
             'lix_key_value' AS schema_key, \
             NULL AS file_id, \
             NULL AS plugin_key, \
             '{\"key\":\"hello\",\"value\":\"from-select\"}' AS snapshot_content, \
             '{\"source\":\"select\"}' AS metadata, \
             '1' AS schema_version, \
             false AS global, \
             false AS untracked",
            &[],
        )
        .await
        .expect("INSERT INTO lix_state SELECT should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-from-select");
        assert_eq!(rows[0].schema_version, "1");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"from-select\"}")
        );
        assert_eq!(rows[0].metadata.as_deref(), Some("{\"source\":\"select\"}"));
    }

    #[tokio::test]
    async fn execute_sql_insert_into_entity_by_version_stages_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateContext);
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_sql(
            &ctx,
            "INSERT INTO test_state_schema_by_version (\
             lixcol_entity_id, lixcol_version_id, value\
             ) VALUES ('entity-c', 'version-b', 'C')",
            &[],
        )
        .await
        .expect("INSERT INTO entity by-version surface should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-c");
        assert_eq!(rows[0].schema_version, "1");
        assert_eq!(rows[0].version_id, "version-b");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"C\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_entity_defaults_active_version() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateContext);
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_sql(
            &ctx,
            "INSERT INTO test_state_schema (lixcol_entity_id, value) \
             VALUES ('entity-c', 'C')",
            &[],
        )
        .await
        .expect("INSERT INTO active entity surface should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-c");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"C\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_directory_by_version_stages_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateContext);
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "INSERT INTO lix_directory_by_version (\
             id, parent_id, name, hidden, lixcol_version_id\
             ) VALUES ('dir-docs', NULL, 'docs', false, 'version-b')",
            &[],
        )
        .await
        .expect("INSERT INTO lix_directory_by_version should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_directory_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "dir-docs");
        assert_eq!(rows[0].schema_version, "1");
        assert_eq!(rows[0].version_id, "version-b");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"hidden\":false,\"id\":\"dir-docs\",\"name\":\"docs\",\"parent_id\":null}")
        );
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_directory_defaults_active_version() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateContext);
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "INSERT INTO lix_directory (id, parent_id, name, hidden) \
             VALUES ('dir-docs', NULL, 'docs', false)",
            &[],
        )
        .await
        .expect("INSERT INTO lix_directory should stage write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_directory_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "dir-docs");
        assert_eq!(rows[0].version_id, "version-a");
    }

    #[tokio::test]
    async fn execute_sql_update_directory_stages_rewritten_descriptor() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateContext {
            rows: vec![
                live_directory_row("dir-docs", "version-a", None, "docs", false),
                live_directory_row("dir-guides", "version-a", Some("dir-docs"), "guides", false),
            ],
        });
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "UPDATE lix_directory \
             SET name = 'docs-updated', hidden = true, lixcol_metadata = '{\"source\":\"directory-update\"}' \
             WHERE id = 'dir-docs'",
            &[],
        )
        .await
        .expect("UPDATE lix_directory should stage rewritten descriptor");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_directory_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "dir-docs");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some(
                "{\"hidden\":true,\"id\":\"dir-docs\",\"name\":\"docs-updated\",\"parent_id\":null}"
            )
        );
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"source\":\"directory-update\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_update_directory_rejects_path_assignment() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateContext {
            rows: vec![live_directory_row(
                "dir-docs",
                "version-a",
                None,
                "docs",
                false,
            )],
        });
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let error = execute_sql(
            &ctx,
            "UPDATE lix_directory SET path = '/renamed/' WHERE id = 'dir-docs'",
            &[],
        )
        .await
        .expect_err("path should remain read-only");

        assert!(
            error.description.contains("read-only column 'path'"),
            "unexpected error: {error:?}"
        );
        assert!(stager.lock().expect("stager lock").deltas.is_empty());
    }

    #[tokio::test]
    async fn execute_sql_delete_directory_by_version_stages_tombstone() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateContext {
            rows: vec![
                live_directory_row("dir-docs", "version-a", None, "docs", false),
                live_directory_row("dir-guides", "version-b", Some("dir-docs"), "guides", false),
            ],
        });
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "DELETE FROM lix_directory_by_version \
             WHERE id = 'dir-guides' AND lixcol_version_id = 'version-b'",
            &[],
        )
        .await
        .expect("DELETE lix_directory_by_version should stage tombstone");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "dir-guides");
        assert_eq!(rows[0].version_id, "version-b");
        assert!(rows[0].tombstone);
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_file_by_version_stages_descriptor_write() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateContext);
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "INSERT INTO lix_file_by_version (\
             id, directory_id, name, extension, hidden, lixcol_version_id\
             ) VALUES ('file-readme', 'dir-docs', 'readme', 'md', false, 'version-b')",
            &[],
        )
        .await
        .expect("INSERT INTO lix_file_by_version should stage descriptor write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "file-readme");
        assert_eq!(rows[0].schema_version, "1");
        assert_eq!(rows[0].version_id, "version-b");
        let snapshot: JsonValue =
            serde_json::from_str(rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "readme");
        assert_eq!(snapshot["extension"], "md");
        assert_eq!(snapshot["hidden"], false);
    }

    #[tokio::test]
    async fn execute_sql_insert_into_active_file_defaults_active_version() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateContext);
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "INSERT INTO lix_file (id, directory_id, name, extension, hidden) \
             VALUES ('file-readme', 'dir-docs', 'readme', 'md', false)",
            &[],
        )
        .await
        .expect("INSERT INTO lix_file should stage descriptor write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "file-readme");
        assert_eq!(rows[0].version_id, "version-a");
    }

    #[tokio::test]
    async fn execute_sql_insert_into_file_with_data_stages_blob_ref() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(DummyLiveStateContext);
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "INSERT INTO lix_file_by_version (\
             id, directory_id, name, extension, hidden, data, lixcol_version_id\
             ) VALUES ('file-readme', 'dir-docs', 'readme', 'md', false, X'4142', 'version-b')",
            &[],
        )
        .await
        .expect("INSERT INTO lix_file_by_version should stage descriptor and data writes");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let descriptor_rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(descriptor_rows.len(), 1);
        assert_eq!(descriptor_rows[0].entity_id, "file-readme");
        let blob_ref_rows = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_ref_rows.len(), 1);
        assert_eq!(blob_ref_rows[0].entity_id, "file-readme");
        assert_eq!(blob_ref_rows[0].file_id.as_deref(), Some("file-readme"));
        assert_eq!(blob_ref_rows[0].version_id, "version-b");
        let snapshot: JsonValue =
            serde_json::from_str(blob_ref_rows[0].snapshot_content.as_deref().unwrap())
                .expect("blob ref snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["size_bytes"], 2);
        assert!(snapshot["blob_hash"]
            .as_str()
            .is_some_and(|value| !value.is_empty()));
    }

    #[tokio::test]
    async fn execute_sql_update_file_stages_rewritten_descriptor() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateContext {
            rows: vec![
                live_file_row(
                    "file-readme",
                    "version-a",
                    Some("dir-docs"),
                    "readme",
                    Some("md"),
                    false,
                ),
                live_file_row(
                    "file-guide",
                    "version-a",
                    Some("dir-docs"),
                    "guide",
                    Some("md"),
                    false,
                ),
            ],
        });
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "UPDATE lix_file \
             SET name = 'readme-updated', extension = 'txt', hidden = true, lixcol_metadata = '{\"source\":\"file-update\"}' \
             WHERE id = 'file-readme'",
            &[],
        )
        .await
        .expect("UPDATE lix_file should stage rewritten descriptor");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_file_descriptor");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "file-readme");
        assert_eq!(rows[0].version_id, "version-a");
        let snapshot: JsonValue =
            serde_json::from_str(rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "readme-updated");
        assert_eq!(snapshot["extension"], "txt");
        assert_eq!(snapshot["hidden"], true);
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"source\":\"file-update\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_update_file_stages_data_blob_ref() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateContext {
            rows: vec![live_file_row(
                "file-readme",
                "version-a",
                Some("dir-docs"),
                "readme",
                Some("md"),
                false,
            )],
        });
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "UPDATE lix_file SET data = X'4142' WHERE id = 'file-readme'",
            &[],
        )
        .await
        .expect("UPDATE lix_file should stage data write");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        assert!(overlay
            .visible_semantic_rows(false, "lix_file_descriptor")
            .is_empty());
        let blob_ref_rows = overlay.visible_semantic_rows(false, "lix_binary_blob_ref");
        assert_eq!(blob_ref_rows.len(), 1);
        assert_eq!(blob_ref_rows[0].entity_id, "file-readme");
        let snapshot: JsonValue =
            serde_json::from_str(blob_ref_rows[0].snapshot_content.as_deref().unwrap())
                .expect("blob ref snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["size_bytes"], 2);
    }

    #[tokio::test]
    async fn execute_sql_update_file_rejects_path_assignment() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateContext {
            rows: vec![live_file_row(
                "file-readme",
                "version-a",
                Some("dir-docs"),
                "readme",
                Some("md"),
                false,
            )],
        });
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let error = execute_sql(
            &ctx,
            "UPDATE lix_file SET path = '/docs/renamed.md' WHERE id = 'file-readme'",
            &[],
        )
        .await
        .expect_err("path should remain read-only");

        assert!(
            error.description.contains("read-only column 'path'"),
            "unexpected error: {error:?}"
        );
        assert!(stager.lock().expect("stager lock").deltas.is_empty());
    }

    #[tokio::test]
    async fn execute_sql_delete_file_by_version_stages_descriptor_tombstone() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateContext {
            rows: vec![
                live_file_row(
                    "file-readme",
                    "version-a",
                    Some("dir-docs"),
                    "readme",
                    Some("md"),
                    false,
                ),
                live_file_row(
                    "file-guide",
                    "version-b",
                    Some("dir-docs"),
                    "guide",
                    Some("md"),
                    false,
                ),
            ],
        });
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "DELETE FROM lix_file_by_version \
             WHERE id = 'file-guide' AND lixcol_version_id = 'version-b'",
            &[],
        )
        .await
        .expect("DELETE lix_file_by_version should stage descriptor tombstone");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "file-guide");
        assert_eq!(rows[0].version_id, "version-b");
        assert!(rows[0].tombstone);
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn execute_sql_update_entity_surface_stages_rewritten_snapshot() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateContext {
            rows: vec![
                live_entity_row("entity-a", "version-a", "A"),
                live_entity_row("entity-b", "version-a", "B"),
            ],
        });
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_sql(
            &ctx,
            "UPDATE test_state_schema \
             SET value = 'updated', lixcol_metadata = '{\"source\":\"entity-update\"}' \
             WHERE value = 'A'",
            &[],
        )
        .await
        .expect("UPDATE entity surface should stage rewritten row");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "test_state_schema");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-a");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"updated\"}")
        );
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"source\":\"entity-update\"}")
        );
    }

    #[tokio::test]
    async fn execute_sql_delete_entity_by_version_stages_tombstone() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateContext {
            rows: vec![
                live_entity_row("entity-a", "version-a", "A"),
                live_entity_row("entity-b", "version-b", "B"),
            ],
        });
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![json!({
                "x-lix-key": "test_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            })],
        };

        let result = execute_sql(
            &ctx,
            "DELETE FROM test_state_schema_by_version \
             WHERE lixcol_version_id = 'version-b'",
            &[],
        )
        .await
        .expect("DELETE entity by-version surface should stage tombstone");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-b");
        assert_eq!(rows[0].version_id, "version-b");
        assert!(rows[0].tombstone);
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn execute_sql_update_lix_state_stages_rewritten_rows() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateContext {
            rows: vec![
                live_lix_state_row("entity-1", Some("{\"source\":\"match\"}")),
                live_lix_state_row("entity-2", Some("{\"source\":\"skip\"}")),
            ],
        });
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let result = execute_sql(
            &ctx,
            "UPDATE lix_state \
             SET snapshot_content = '{\"key\":\"hello\",\"value\":\"updated\"}', \
                 metadata = schema_key \
             WHERE metadata = '{\"source\":\"match\"}'",
            &[],
        )
        .await
        .expect("UPDATE lix_state should stage rewritten rows");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-1");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"updated\"}")
        );
        assert_eq!(rows[0].metadata.as_deref(), Some("lix_key_value"));
    }

    #[tokio::test]
    async fn execute_sql_delete_lix_state_without_where_stages_all_rows() {
        let blob_reader: Arc<dyn BlobDataReader> = Arc::new(DummyBlobReader);
        let live_state = Arc::new(RowsLiveStateContext {
            rows: vec![
                live_lix_state_row("entity-1", Some("{\"source\":\"one\"}")),
                live_lix_state_row("entity-2", Some("{\"source\":\"two\"}")),
            ],
        });
        let stager = Arc::new(Mutex::new(CapturingPreparedWriteStager::default()));
        let ctx = DummySqlExecutionContext {
            active_version_id: "version-a",
            blob_reader,
            live_state,
            write_stager: Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
            schema_definitions: vec![],
        };

        let result = execute_sql(&ctx, "DELETE FROM lix_state", &[])
            .await
            .expect("DELETE FROM lix_state should follow DataFusion delete-all semantics");

        assert_eq!(result.columns, vec!["count"]);
        assert_eq!(result.rows, vec![vec![Value::Integer(2)]]);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_all_semantic_rows();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.tombstone));
        assert!(rows.iter().all(|row| row.snapshot_content.is_none()));
        assert!(rows.iter().any(|row| row.entity_id == "entity-1"));
        assert!(rows.iter().any(|row| row.entity_id == "entity-2"));
    }

    struct BackendSqlExecutionContext<'a> {
        active_version_id: &'a str,
        blob_reader: Arc<dyn BlobDataReader>,
        live_state: Arc<dyn LiveStateContext>,
        schema_definitions: Vec<JsonValue>,
    }

    impl SqlExecutionContext for BackendSqlExecutionContext<'_> {
        fn active_version_id(&self) -> &str {
            self.active_version_id
        }

        fn live_state(&self) -> Arc<dyn LiveStateContext> {
            Arc::clone(&self.live_state)
        }

        fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
            Arc::clone(&self.blob_reader)
        }

        fn list_visible_schemas(&self, version_id: &str) -> Result<Vec<JsonValue>, LixError> {
            let _ = version_id;
            Ok(self.schema_definitions.clone())
        }
    }

    async fn setup_sql2_state_fixture() -> Result<
        (
            crate::test_support::TestSqliteBackend,
            crate::Session,
            JsonValue,
        ),
        crate::LixError,
    > {
        let (backend, _lix, session) = boot_test_engine().await?;
        let schema_definition = json!({
            "x-lix-key": "test_state_schema",
            "x-lix-version": "1",
            "type": "object",
            "properties": {
                "value": { "type": "string" }
            },
            "required": ["value"],
            "additionalProperties": false
        });
        session.register_schema(&schema_definition).await?;
        session
            .create_version(CreateVersionOptions {
                id: Some("version-a".to_string()),
                name: Some("version-a".to_string()),
                ..CreateVersionOptions::default()
            })
            .await?;
        session
            .create_version(CreateVersionOptions {
                id: Some("version-b".to_string()),
                name: Some("version-b".to_string()),
                ..CreateVersionOptions::default()
            })
            .await?;
        session
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-a', 'test_state_schema', NULL, 'version-a', NULL, '{\"value\":\"A\"}', '1'\
                 )",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-b', 'test_state_schema', NULL, 'version-b', NULL, '{\"value\":\"B\"}', '1'\
                 )",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'dir-docs', 'lix_directory_descriptor', NULL, 'version-a', NULL, '{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\",\"hidden\":false}', '1'\
                 )",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                 VALUES ('file-a', '/docs/readme.md', X'4142', 'version-a')",
                &[],
            )
            .await?;
        Ok((backend, session, schema_definition))
    }

    fn run_async_test_with_large_stack(
        test: impl FnOnce() -> futures_util::future::LocalBoxFuture<'static, ()> + Send + 'static,
    ) {
        std::thread::Builder::new()
            .name("sql2-execute-test".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime should build")
                    .block_on(test());
            })
            .expect("test thread should spawn")
            .join()
            .expect("test thread should join");
    }

    #[test]
    fn execute_sql_reads_lix_state_by_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::LixBackend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(CommittedLiveStateContext::new(Arc::clone(&backend_ref))),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT entity_id, version_id, snapshot_content, commit_id \
                     FROM lix_state_by_version \
                     WHERE version_id = 'version-b' AND schema_key = 'test_state_schema'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_state_by_version");

                assert_eq!(
                    result.columns,
                    vec!["entity_id", "version_id", "snapshot_content", "commit_id"]
                );
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("entity-b".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("version-b".to_string()));
                assert_eq!(
                    result.rows[0][2],
                    Value::Text("{\"value\":\"B\"}".to_string())
                );
                match &result.rows[0][3] {
                    Value::Text(commit_id) => assert!(!commit_id.is_empty()),
                    other => panic!("expected non-null commit_id text, got {other:?}"),
                }
            })
        });
    }

    #[test]
    fn execute_sql_supports_broad_lix_state_by_version_reads() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::LixBackend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(CommittedLiveStateContext::new(Arc::clone(&backend_ref))),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT entity_id FROM lix_state_by_version WHERE schema_key = 'test_state_schema'",
                    &[],
                )
                .await
                .expect("broad by-version read should succeed");

                assert!(
                    result.rows.iter().any(|row| row[0] == Value::Text("entity-a".to_string()))
                        && result.rows.iter().any(|row| row[0] == Value::Text("entity-b".to_string())),
                    "expected broad by-version read to include rows from multiple visible versions: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_state_from_active_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::LixBackend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(CommittedLiveStateContext::new(Arc::clone(&backend_ref))),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT entity_id, snapshot_content \
                     FROM lix_state \
                     WHERE schema_key = 'test_state_schema'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_state");

                assert_eq!(result.columns, vec!["entity_id", "snapshot_content"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("entity-a".to_string()));
                assert_eq!(
                    result.rows[0][1],
                    Value::Text("{\"value\":\"A\"}".to_string())
                );
            })
        });
    }

    #[test]
    fn execute_sql_reads_entity_view_from_active_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::LixBackend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(CommittedLiveStateContext::new(Arc::clone(&backend_ref))),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT value, lixcol_entity_id \
                     FROM test_state_schema",
                    &[],
                )
                .await
                .expect("sql2 execute should read entity view");

                assert_eq!(result.columns, vec!["value", "lixcol_entity_id"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("A".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("entity-a".to_string()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_entity_by_version_view() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::LixBackend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(CommittedLiveStateContext::new(Arc::clone(&backend_ref))),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT value, lixcol_version_id \
                     FROM test_state_schema_by_version \
                     WHERE lixcol_version_id = 'version-b'",
                    &[],
                )
                .await
                .expect("sql2 execute should read entity by-version view");

                assert_eq!(result.columns, vec!["value", "lixcol_version_id"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("B".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("version-b".to_string()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_directory_by_version_view() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::LixBackend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(CommittedLiveStateContext::new(Arc::clone(&backend_ref))),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name, lixcol_version_id \
                     FROM lix_directory_by_version \
                     WHERE id = 'dir-docs' AND lixcol_version_id = 'version-a'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_directory_by_version");

                assert_eq!(result.columns, vec!["path", "name", "lixcol_version_id"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("/docs/".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("docs".to_string()));
                assert_eq!(result.rows[0][2], Value::Text("version-a".to_string()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_directory_from_active_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::LixBackend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(CommittedLiveStateContext::new(Arc::clone(&backend_ref))),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name \
                     FROM lix_directory \
                     WHERE id = 'dir-docs'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_directory");

                assert_eq!(result.columns, vec!["path", "name"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("/docs/".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("docs".to_string()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_file_by_version_view() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::LixBackend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(CommittedLiveStateContext::new(Arc::clone(&backend_ref))),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name, extension, data, lixcol_version_id \
                     FROM lix_file_by_version \
                     WHERE id = 'file-a' AND lixcol_version_id = 'version-a'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_file_by_version");

                assert_eq!(
                    result.columns,
                    vec!["path", "name", "extension", "data", "lixcol_version_id"]
                );
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0][0],
                    Value::Text("/docs/readme.md".to_string())
                );
                assert_eq!(result.rows[0][1], Value::Text("readme".to_string()));
                assert_eq!(result.rows[0][2], Value::Text("md".to_string()));
                assert_eq!(result.rows[0][3], Value::Blob(vec![0x41, 0x42]));
                assert_eq!(result.rows[0][4], Value::Text("version-a".to_string()));
            })
        });
    }

    #[test]
    fn execute_sql_reads_lix_file_from_active_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session, schema_definition) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend = Arc::new(backend);
                let backend_ref: Arc<dyn crate::LixBackend + Send + Sync> = backend;
                let blob_reader: Arc<dyn BlobDataReader> =
                    Arc::new(BackendBlobReader(Arc::clone(&backend_ref)));
                let ctx = BackendSqlExecutionContext {
                    active_version_id: "version-a",
                    blob_reader: Arc::clone(&blob_reader),
                    live_state: Arc::new(CommittedLiveStateContext::new(Arc::clone(&backend_ref))),
                    schema_definitions: vec![schema_definition],
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT path, name, extension, data \
                     FROM lix_file \
                     WHERE id = 'file-a'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_file");

                assert_eq!(result.columns, vec!["path", "name", "extension", "data"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0][0],
                    Value::Text("/docs/readme.md".to_string())
                );
                assert_eq!(result.rows[0][1], Value::Text("readme".to_string()));
                assert_eq!(result.rows[0][2], Value::Text("md".to_string()));
                assert_eq!(result.rows[0][3], Value::Blob(vec![0x41, 0x42]));
            })
        });
    }
}
