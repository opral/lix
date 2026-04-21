use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning};
use datafusion::prelude::SessionContext;
use datafusion::{datasource::TableType, physical_plan::SendableRecordBatchStream};
use futures_util::stream;

use crate::{LixBackend, LixError, QueryResult};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedSql2ReadArtifact {
    pub(crate) sql: String,
    pub(crate) active_version_id: String,
    pub(crate) surface_names: Vec<String>,
}

pub(crate) async fn execute_read_with_backend(
    _backend: &dyn LixBackend,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<QueryResult, LixError> {
    let ctx = build_session_for_read(artifact)?;
    let dataframe = ctx
        .sql(&artifact.sql)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let result = dataframe
        .collect()
        .await
        .map_err(datafusion_error_to_lix_error);

    match result {
        Ok(_batches) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "sql2 phase-1 reads reached DataFusion unexpectedly without a provider-backed stream implementation",
        )),
        Err(error) => Err(error),
    }
}

fn build_session_for_read(artifact: &PreparedSql2ReadArtifact) -> Result<SessionContext, LixError> {
    let ctx = SessionContext::new();
    for surface_name in &artifact.surface_names {
        match surface_name.as_str() {
            "lix_state" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixStateProvider::new(artifact.active_version_id.clone())),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            other => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("sql2 phase-1 does not support surface '{other}' yet"),
                ));
            }
        }
    }
    Ok(ctx)
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

#[derive(Debug)]
struct LixStateProvider {
    active_version_id: String,
    schema: SchemaRef,
}

impl LixStateProvider {
    fn new(active_version_id: String) -> Self {
        Self {
            active_version_id,
            schema: lix_state_schema(),
        }
    }
}

#[async_trait]
impl TableProvider for LixStateProvider {
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = projected_schema(&self.schema, projection)?;
        Ok(Arc::new(LixStateScanExec::new(
            projected_schema,
            self.active_version_id.clone(),
        )))
    }
}

#[derive(Debug)]
struct LixStateScanExec {
    schema: SchemaRef,
    active_version_id: String,
    properties: Arc<PlanProperties>,
}

impl LixStateScanExec {
    fn new(schema: SchemaRef, active_version_id: String) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            schema,
            active_version_id,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixStateScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixStateScanExec(active_version_id={})",
                    self.active_version_id
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "LixStateScanExec")
            }
        }
    }
}

impl ExecutionPlan for LixStateScanExec {
    fn name(&self) -> &str {
        "LixStateScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Execution(
                "LixStateScanExec does not accept children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "LixStateScanExec only exposes one partition, got {partition}"
            )));
        }

        let schema = Arc::clone(&self.schema);
        let active_version_id = self.active_version_id.clone();
        let stream = stream::once(async move {
            Err(DataFusionError::NotImplemented(format!(
                "sql2 phase-1 provider stream for lix_state is not implemented yet (active_version_id={active_version_id})"
            )))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn lix_state_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("plugin_key", DataType::Utf8, true),
        Field::new("snapshot_content", DataType::Utf8, false),
        Field::new("metadata", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
        Field::new("version_id", DataType::Utf8, false),
        Field::new("created_at", DataType::Utf8, false),
        Field::new("updated_at", DataType::Utf8, false),
        Field::new("global", DataType::Boolean, false),
        Field::new("change_id", DataType::Utf8, true),
        Field::new("commit_id", DataType::Utf8, true),
        Field::new("untracked", DataType::Boolean, false),
    ]))
}

fn projected_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    let Some(projection) = projection else {
        return Ok(Arc::clone(schema));
    };

    let projected = schema.project(projection).map_err(|error| {
        DataFusionError::Execution(format!("sql2 failed to project lix_state schema: {error}"))
    })?;
    Ok(Arc::new(projected))
}

#[cfg(test)]
mod tests {
    use super::{build_session_for_read, PreparedSql2ReadArtifact};

    #[tokio::test]
    async fn builds_session_and_plans_lix_state_query() {
        let artifact = PreparedSql2ReadArtifact {
            sql: "SELECT entity_id FROM lix_state LIMIT 1".to_string(),
            active_version_id: "version-main".to_string(),
            surface_names: vec!["lix_state".to_string()],
        };
        let ctx = build_session_for_read(&artifact).expect("session should build");
        let dataframe = ctx.sql(&artifact.sql).await.expect("query should plan");
        let error = dataframe
            .collect()
            .await
            .expect_err("execution should be stubbed");
        assert!(error
            .to_string()
            .contains("sql2 phase-1 provider stream for lix_state is not implemented yet"));
    }
}
