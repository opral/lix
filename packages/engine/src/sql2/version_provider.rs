use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
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
use serde_json::Value as JsonValue;

use crate::engine2::live_state::{
    LiveStateFilter, LiveStateReader, LiveStateRow, LiveStateScanRequest,
};
use crate::engine2::version_ref::VersionRefReader;
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

pub(crate) async fn register_lix_version_provider(
    session: &datafusion::prelude::SessionContext,
    live_state: Arc<dyn LiveStateReader>,
    version_ref: Arc<dyn VersionRefReader>,
) -> Result<(), LixError> {
    session
        .register_table(
            "lix_version",
            Arc::new(LixVersionProvider::new(live_state, version_ref)),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

struct LixVersionProvider {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateReader>,
    version_ref: Arc<dyn VersionRefReader>,
}

impl std::fmt::Debug for LixVersionProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixVersionProvider").finish()
    }
}

impl LixVersionProvider {
    fn new(live_state: Arc<dyn LiveStateReader>, version_ref: Arc<dyn VersionRefReader>) -> Self {
        Self {
            schema: lix_version_schema(),
            live_state,
            version_ref,
        }
    }
}

#[async_trait]
impl TableProvider for LixVersionProvider {
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
        Ok(Arc::new(LixVersionScanExec::new(
            Arc::clone(&self.live_state),
            Arc::clone(&self.version_ref),
            projected_schema(&self.schema, projection),
            projection.cloned(),
        )))
    }
}

struct LixVersionScanExec {
    live_state: Arc<dyn LiveStateReader>,
    version_ref: Arc<dyn VersionRefReader>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixVersionScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixVersionScanExec").finish()
    }
}

impl LixVersionScanExec {
    fn new(
        live_state: Arc<dyn LiveStateReader>,
        version_ref: Arc<dyn VersionRefReader>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            live_state,
            version_ref,
            schema,
            projection,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixVersionScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixVersionScanExec")
            }
            DisplayFormatType::TreeRender => write!(f, "LixVersionScanExec"),
        }
    }
}

impl ExecutionPlan for LixVersionScanExec {
    fn name(&self) -> &str {
        "LixVersionScanExec"
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
                "LixVersionScanExec does not accept children".to_string(),
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
                "LixVersionScanExec only exposes one partition, got {partition}"
            )));
        }

        let live_state = Arc::clone(&self.live_state);
        let version_ref = Arc::clone(&self.version_ref);
        let projection = version_projection_for_scan(self.projection.as_ref());
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let rows = load_version_rows(live_state, version_ref)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            version_record_batch(&projection, &rows)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VersionRow {
    id: String,
    name: String,
    hidden: bool,
    commit_id: String,
}

#[derive(Debug, Clone, Copy)]
enum VersionColumn {
    Id,
    Name,
    Hidden,
    CommitId,
}

async fn load_version_rows(
    live_state: Arc<dyn LiveStateReader>,
    version_ref: Arc<dyn VersionRefReader>,
) -> Result<Vec<VersionRow>, LixError> {
    let descriptor_rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec!["lix_version_descriptor".to_string()],
                version_ids: vec![GLOBAL_VERSION_ID.to_string()],
                ..LiveStateFilter::default()
            },
            projection: Default::default(),
            limit: None,
        })
        .await?;

    let mut out = Vec::new();
    for descriptor_row in descriptor_rows {
        let descriptor = parse_descriptor(&descriptor_row)?;
        let Some(commit_id) = version_ref.load_head_commit_id(&descriptor.id).await? else {
            continue;
        };
        out.push(VersionRow {
            commit_id,
            id: descriptor.id,
            name: descriptor.name,
            hidden: descriptor.hidden,
        });
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VersionDescriptor {
    id: String,
    name: String,
    hidden: bool,
}

fn parse_descriptor(row: &LiveStateRow) -> Result<VersionDescriptor, LixError> {
    let snapshot = parse_snapshot(row, "lix_version_descriptor")?;
    let id = snapshot
        .get("id")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", "lix_version_descriptor is missing id"))?
        .to_string();
    let name = snapshot
        .get("name")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "lix_version_descriptor is missing name",
            )
        })?
        .to_string();
    let hidden = snapshot
        .get("hidden")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    Ok(VersionDescriptor { id, name, hidden })
}

fn parse_snapshot(row: &LiveStateRow, schema_key: &str) -> Result<JsonValue, LixError> {
    let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{schema_key} row is missing snapshot_content"),
        )
    })?;
    serde_json::from_str(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{schema_key} snapshot_content is invalid JSON: {error}"),
        )
    })
}

fn lix_version_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("hidden", DataType::Boolean, false),
        Field::new("commit_id", DataType::Utf8, false),
    ]))
}

fn version_projection_for_scan(projection: Option<&Vec<usize>>) -> Vec<VersionColumn> {
    let all_columns = vec![
        VersionColumn::Id,
        VersionColumn::Name,
        VersionColumn::Hidden,
        VersionColumn::CommitId,
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

fn version_record_batch(projection: &[VersionColumn], rows: &[VersionRow]) -> Result<RecordBatch> {
    let arrays = projection
        .iter()
        .map(|column| match column {
            VersionColumn::Id => string_array(rows.iter().map(|row| Some(row.id.as_str()))),
            VersionColumn::Name => string_array(rows.iter().map(|row| Some(row.name.as_str()))),
            VersionColumn::Hidden => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.hidden).collect::<Vec<_>>(),
            )) as ArrayRef,
            VersionColumn::CommitId => {
                string_array(rows.iter().map(|row| Some(row.commit_id.as_str())))
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(version_schema(projection), arrays).map_err(|error| {
        DataFusionError::Execution(format!("failed to build lix_version batch: {error}"))
    })
}

fn version_schema(projection: &[VersionColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| match column {
                VersionColumn::Id => Field::new("id", DataType::Utf8, false),
                VersionColumn::Name => Field::new("name", DataType::Utf8, false),
                VersionColumn::Hidden => Field::new("hidden", DataType::Boolean, false),
                VersionColumn::CommitId => Field::new("commit_id", DataType::Utf8, false),
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
    DataFusionError::Execution(format!("sql2 version provider error: {error}"))
}
