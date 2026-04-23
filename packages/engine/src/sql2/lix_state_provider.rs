use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::{MemTable, TableType};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::prelude::SessionContext;

use crate::live_state::{LiveRow, LiveStateContext, LiveStateScanRequest};
use crate::LixError;

pub(crate) async fn register_lix_state_providers(
    session: &SessionContext,
    live_state: Arc<dyn LiveStateContext>,
) -> Result<(), LixError> {
    session
        .register_table(
            "lix_state_by_version",
            Arc::new(LixStateByVersionProvider::new(live_state)),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

pub(crate) struct LixStateByVersionProvider {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateContext>,
}

impl std::fmt::Debug for LixStateByVersionProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixStateByVersionProvider").finish()
    }
}

impl LixStateByVersionProvider {
    pub(crate) fn new(live_state: Arc<dyn LiveStateContext>) -> Self {
        Self {
            schema: lix_state_by_version_schema(),
            live_state,
        }
    }
}

#[async_trait]
impl TableProvider for LixStateByVersionProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let rows = self
            .live_state
            .scan(&LiveStateScanRequest::default())
            .await
            .map_err(lix_error_to_datafusion_error)?;
        let batch = lix_state_by_version_record_batch(Arc::clone(&self.schema), &rows)
            .map_err(lix_error_to_datafusion_error)?;
        let table = MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?;
        table.scan(state, projection, filters, limit).await
    }
}

fn lix_state_by_version_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("plugin_key", DataType::Utf8, true),
        Field::new("snapshot_content", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("schema_version", DataType::Utf8, true),
        Field::new("created_at", DataType::Utf8, true),
        Field::new("updated_at", DataType::Utf8, true),
        Field::new("global", DataType::Boolean, false),
        Field::new("change_id", DataType::Utf8, true),
        Field::new("commit_id", DataType::Utf8, true),
        Field::new("untracked", DataType::Boolean, false),
        Field::new("version_id", DataType::Utf8, false),
    ]))
}

fn lix_state_by_version_record_batch(
    schema: SchemaRef,
    rows: &[LiveRow],
) -> Result<RecordBatch, LixError> {
    let columns: Vec<ArrayRef> = vec![
        string_array(rows.iter().map(|row| Some(row.entity_id.as_str()))),
        string_array(rows.iter().map(|row| Some(row.schema_key.as_str()))),
        string_array(rows.iter().map(|row| row.file_id.as_deref())),
        string_array(rows.iter().map(|row| row.plugin_key.as_deref())),
        string_array(rows.iter().map(|row| row.snapshot_content.as_deref())),
        string_array(rows.iter().map(|row| row.metadata.as_deref())),
        string_array(rows.iter().map(|row| Some(row.schema_version.as_str()))),
        string_array(rows.iter().map(|row| row.created_at.as_deref())),
        string_array(rows.iter().map(|row| row.updated_at.as_deref())),
        Arc::new(BooleanArray::from(
            rows.iter().map(|row| row.global).collect::<Vec<_>>(),
        )) as ArrayRef,
        string_array(rows.iter().map(|row| row.change_id.as_deref())),
        string_array(rows.iter().map(|row| row.commit_id.as_deref())),
        Arc::new(BooleanArray::from(
            rows.iter().map(|row| row.untracked).collect::<Vec<_>>(),
        )) as ArrayRef,
        string_array(rows.iter().map(|row| Some(row.version_id.as_str()))),
    ];

    RecordBatch::try_new(schema, columns).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build lix_state_by_version batch: {error}"),
        )
    })
}

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    let values = values
        .map(|value| value.map(ToOwned::to_owned))
        .collect::<Vec<_>>();
    Arc::new(StringArray::from(values)) as ArrayRef
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    DataFusionError::Execution(format!("sql2 live-state provider error: {error}"))
}
