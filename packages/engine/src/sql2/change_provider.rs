use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use futures_util::stream;

use crate::changelog::{CanonicalChange, ChangelogReader, ChangelogScanRequest};
use crate::LixError;

pub(crate) async fn register_lix_change_provider(
    session: &datafusion::prelude::SessionContext,
    changelog: Arc<dyn ChangelogReader>,
) -> Result<(), LixError> {
    session
        .register_table("lix_change", Arc::new(LixChangeProvider::new(changelog)))
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

struct LixChangeProvider {
    schema: SchemaRef,
    changelog: Arc<dyn ChangelogReader>,
}

impl std::fmt::Debug for LixChangeProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixChangeProvider").finish()
    }
}

impl LixChangeProvider {
    fn new(changelog: Arc<dyn ChangelogReader>) -> Self {
        Self {
            schema: lix_change_schema(),
            changelog,
        }
    }
}

#[async_trait]
impl TableProvider for LixChangeProvider {
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
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(LixChangeScanExec::new(
            Arc::clone(&self.changelog),
            projected_schema(&self.schema, projection),
            projection.cloned(),
            limit,
        )))
    }
}

struct LixChangeScanExec {
    changelog: Arc<dyn ChangelogReader>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixChangeScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixChangeScanExec").finish()
    }
}

impl LixChangeScanExec {
    fn new(
        changelog: Arc<dyn ChangelogReader>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            changelog,
            schema,
            projection,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixChangeScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixChangeScanExec")
            }
            DisplayFormatType::TreeRender => write!(f, "LixChangeScanExec"),
        }
    }
}

impl ExecutionPlan for LixChangeScanExec {
    fn name(&self) -> &str {
        "LixChangeScanExec"
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
                "LixChangeScanExec does not accept children".to_string(),
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
                "LixChangeScanExec only exposes one partition, got {partition}"
            )));
        }

        let changelog = Arc::clone(&self.changelog);
        let projection = change_projection_for_scan(self.projection.as_ref());
        let limit = self.limit;
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let changes = changelog
                .scan_changes(&ChangelogScanRequest { limit })
                .await
                .map_err(lix_error_to_datafusion_error)?;
            change_record_batch(&projection, &changes)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug, Clone, Copy)]
enum ChangeColumn {
    Id,
    EntityId,
    SchemaKey,
    SchemaVersion,
    FileId,
    Metadata,
    CreatedAt,
    SnapshotContent,
}

fn lix_change_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("created_at", DataType::Utf8, false),
        Field::new("snapshot_content", DataType::Utf8, true),
    ]))
}

fn change_projection_for_scan(projection: Option<&Vec<usize>>) -> Vec<ChangeColumn> {
    let all_columns = vec![
        ChangeColumn::Id,
        ChangeColumn::EntityId,
        ChangeColumn::SchemaKey,
        ChangeColumn::SchemaVersion,
        ChangeColumn::FileId,
        ChangeColumn::Metadata,
        ChangeColumn::CreatedAt,
        ChangeColumn::SnapshotContent,
    ];
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

fn projected_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
    match projection {
        Some(projection) => Arc::new(schema.project(projection).expect("projection is valid")),
        None => Arc::clone(schema),
    }
}

fn change_record_batch(
    projection: &[ChangeColumn],
    changes: &[CanonicalChange],
) -> Result<RecordBatch> {
    let arrays = projection
        .iter()
        .map(|column| match column {
            ChangeColumn::Id => string_array(changes.iter().map(|row| Some(row.id.as_str()))),
            ChangeColumn::EntityId => Arc::new(StringArray::from(
                changes
                    .iter()
                    .map(|row| {
                        Some(
                            row.entity_id
                                .as_string()
                                .expect("canonical change entity identity should project"),
                        )
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            ChangeColumn::SchemaKey => {
                string_array(changes.iter().map(|row| Some(row.schema_key.as_str())))
            }
            ChangeColumn::SchemaVersion => {
                string_array(changes.iter().map(|row| Some(row.schema_version.as_str())))
            }
            ChangeColumn::FileId => string_array(changes.iter().map(|row| row.file_id.as_deref())),
            ChangeColumn::Metadata => {
                string_array(changes.iter().map(|row| row.metadata.as_deref()))
            }
            ChangeColumn::CreatedAt => {
                string_array(changes.iter().map(|row| Some(row.created_at.as_str())))
            }
            ChangeColumn::SnapshotContent => {
                string_array(changes.iter().map(|row| row.snapshot_content.as_deref()))
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(change_schema(projection), arrays).map_err(|error| {
        DataFusionError::Execution(format!("failed to build lix_change batch: {error}"))
    })
}

fn change_schema(projection: &[ChangeColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| match column {
                ChangeColumn::Id => Field::new("id", DataType::Utf8, false),
                ChangeColumn::EntityId => Field::new("entity_id", DataType::Utf8, false),
                ChangeColumn::SchemaKey => Field::new("schema_key", DataType::Utf8, false),
                ChangeColumn::SchemaVersion => Field::new("schema_version", DataType::Utf8, false),
                ChangeColumn::FileId => Field::new("file_id", DataType::Utf8, true),
                ChangeColumn::Metadata => Field::new("metadata", DataType::Utf8, true),
                ChangeColumn::CreatedAt => Field::new("created_at", DataType::Utf8, false),
                ChangeColumn::SnapshotContent => {
                    Field::new("snapshot_content", DataType::Utf8, true)
                }
            })
            .collect::<Vec<_>>(),
    ))
}

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from(values.collect::<Vec<_>>())) as ArrayRef
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    DataFusionError::Execution(format!("sql2 changelog provider error: {error}"))
}
