use std::any::Any;
use std::collections::BTreeSet;
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
use tokio::sync::Mutex;

use crate::commit_graph::{CommitGraphCommit, CommitGraphReader};
use crate::sql2::version_scope::{resolve_provider_version_ids, VersionBinding};
use crate::version_ref::VersionRefReader;
use crate::LixError;
use crate::GLOBAL_VERSION_ID;

pub(crate) async fn register_commit_providers(
    session: &datafusion::prelude::SessionContext,
    active_version_id: &str,
    commit_graph: Box<dyn CommitGraphReader>,
    version_ref: Arc<dyn VersionRefReader>,
) -> Result<(), LixError> {
    let commit_graph = Arc::new(Mutex::new(commit_graph));
    for surface in CommitSurface::all() {
        let provider = Arc::new(CommitSurfaceProvider::new(
            surface,
            active_version_id.to_string(),
            Arc::clone(&commit_graph),
            Arc::clone(&version_ref),
        ));
        session
            .register_table(surface.table_name(), provider)
            .map_err(datafusion_error_to_lix_error)?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum CommitSurface {
    Commit,
    CommitByVersion,
    CommitEdge,
    CommitEdgeByVersion,
    ChangeSet,
    ChangeSetByVersion,
    ChangeSetElement,
    ChangeSetElementByVersion,
}

impl CommitSurface {
    fn all() -> [Self; 8] {
        [
            Self::Commit,
            Self::CommitByVersion,
            Self::CommitEdge,
            Self::CommitEdgeByVersion,
            Self::ChangeSet,
            Self::ChangeSetByVersion,
            Self::ChangeSetElement,
            Self::ChangeSetElementByVersion,
        ]
    }

    fn table_name(self) -> &'static str {
        match self {
            Self::Commit => "lix_commit",
            Self::CommitByVersion => "lix_commit_by_version",
            Self::CommitEdge => "lix_commit_edge",
            Self::CommitEdgeByVersion => "lix_commit_edge_by_version",
            Self::ChangeSet => "lix_change_set",
            Self::ChangeSetByVersion => "lix_change_set_by_version",
            Self::ChangeSetElement => "lix_change_set_element",
            Self::ChangeSetElementByVersion => "lix_change_set_element_by_version",
        }
    }

    fn schema(self) -> SchemaRef {
        match self {
            Self::Commit => commit_schema(false),
            Self::CommitByVersion => commit_schema(true),
            Self::CommitEdge => commit_edge_schema(false),
            Self::CommitEdgeByVersion => commit_edge_schema(true),
            Self::ChangeSet => change_set_schema(false),
            Self::ChangeSetByVersion => change_set_schema(true),
            Self::ChangeSetElement => change_set_element_schema(false),
            Self::ChangeSetElementByVersion => change_set_element_schema(true),
        }
    }

    fn by_version(self) -> bool {
        matches!(
            self,
            Self::CommitByVersion
                | Self::CommitEdgeByVersion
                | Self::ChangeSetByVersion
                | Self::ChangeSetElementByVersion
        )
    }
}

struct CommitSurfaceProvider {
    surface: CommitSurface,
    active_version_id: String,
    schema: SchemaRef,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    version_ref: Arc<dyn VersionRefReader>,
}

impl std::fmt::Debug for CommitSurfaceProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitSurfaceProvider")
            .field("surface", &self.surface)
            .finish()
    }
}

impl CommitSurfaceProvider {
    fn new(
        surface: CommitSurface,
        active_version_id: String,
        commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
        version_ref: Arc<dyn VersionRefReader>,
    ) -> Self {
        Self {
            surface,
            active_version_id,
            schema: surface.schema(),
            commit_graph,
            version_ref,
        }
    }
}

#[async_trait]
impl TableProvider for CommitSurfaceProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
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
        Ok(Arc::new(CommitSurfaceScanExec::new(
            self.surface,
            self.active_version_id.clone(),
            Arc::clone(&self.commit_graph),
            Arc::clone(&self.version_ref),
            projected_schema(&self.schema, projection),
            projection.cloned(),
            limit,
        )))
    }
}

struct CommitSurfaceScanExec {
    surface: CommitSurface,
    active_version_id: String,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    version_ref: Arc<dyn VersionRefReader>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for CommitSurfaceScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitSurfaceScanExec")
            .field("surface", &self.surface)
            .finish()
    }
}

impl CommitSurfaceScanExec {
    fn new(
        surface: CommitSurface,
        active_version_id: String,
        commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
        version_ref: Arc<dyn VersionRefReader>,
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
            surface,
            active_version_id,
            commit_graph,
            version_ref,
            schema,
            projection,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for CommitSurfaceScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CommitSurfaceScanExec")
            }
            DisplayFormatType::TreeRender => write!(f, "CommitSurfaceScanExec"),
        }
    }
}

impl ExecutionPlan for CommitSurfaceScanExec {
    fn name(&self) -> &str {
        "CommitSurfaceScanExec"
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
                "CommitSurfaceScanExec does not accept children".to_string(),
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
                "CommitSurfaceScanExec only exposes one partition, got {partition}"
            )));
        }

        let surface = self.surface;
        let active_version_id = self.active_version_id.clone();
        let commit_graph = Arc::clone(&self.commit_graph);
        let version_ref = Arc::clone(&self.version_ref);
        let projection = self.projection.clone();
        let limit = self.limit;
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let version_binding = if surface.by_version() {
                VersionBinding::explicit()
            } else {
                VersionBinding::active(active_version_id)
            };
            let version_ids =
                resolve_provider_version_ids(version_ref.as_ref(), &version_binding, Vec::new())
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
            let rows = rows_for_surface(
                surface,
                &version_ids,
                Arc::clone(&commit_graph),
                Arc::clone(&version_ref),
            )
            .await
            .map_err(lix_error_to_datafusion_error)?;
            let rows = match limit {
                Some(limit) => rows.into_iter().take(limit).collect::<Vec<_>>(),
                None => rows,
            };
            surface_record_batch(surface, projection.as_ref(), &rows)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug, Clone)]
enum SurfaceRow {
    Commit {
        version_id: Option<String>,
        id: String,
        change_set_id: String,
    },
    CommitEdge {
        version_id: Option<String>,
        parent_id: String,
        child_id: String,
    },
    ChangeSet {
        version_id: Option<String>,
        id: String,
    },
    ChangeSetElement {
        version_id: Option<String>,
        change_set_id: String,
        change_id: String,
        entity_id: String,
        schema_key: String,
        file_id: Option<String>,
    },
}

async fn rows_for_surface(
    surface: CommitSurface,
    version_ids: &[String],
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    version_ref: Arc<dyn VersionRefReader>,
) -> Result<Vec<SurfaceRow>, LixError> {
    let mut rows = Vec::new();
    let mut seen = BTreeSet::<String>::new();
    let mut graph = commit_graph.lock().await;

    for version_id in version_ids {
        let commits = visible_commits_for_version(
            &mut **graph,
            version_ref.as_ref(),
            version_id,
            surface.by_version(),
        )
        .await?;
        match surface {
            CommitSurface::Commit | CommitSurface::CommitByVersion => {
                for commit in commits {
                    let key = format!("{version_id}\0commit\0{}", commit.commit_id);
                    if seen.insert(key) {
                        rows.push(SurfaceRow::Commit {
                            version_id: surface.by_version().then(|| version_id.clone()),
                            id: commit.commit_id,
                            change_set_id: commit.change_set_id,
                        });
                    }
                }
            }
            CommitSurface::CommitEdge | CommitSurface::CommitEdgeByVersion => {
                for edge in graph.commit_edges(&commits) {
                    let key = format!(
                        "{version_id}\0edge\0{}\0{}",
                        edge.parent_commit_id, edge.child_commit_id
                    );
                    if seen.insert(key) {
                        rows.push(SurfaceRow::CommitEdge {
                            version_id: surface.by_version().then(|| version_id.clone()),
                            parent_id: edge.parent_commit_id,
                            child_id: edge.child_commit_id,
                        });
                    }
                }
            }
            CommitSurface::ChangeSet | CommitSurface::ChangeSetByVersion => {
                for change_set in graph.change_sets(&commits) {
                    let key = format!("{version_id}\0change_set\0{}", change_set.id);
                    if seen.insert(key) {
                        rows.push(SurfaceRow::ChangeSet {
                            version_id: surface.by_version().then(|| version_id.clone()),
                            id: change_set.id,
                        });
                    }
                }
            }
            CommitSurface::ChangeSetElement | CommitSurface::ChangeSetElementByVersion => {
                for element in graph.change_set_elements(&commits).await? {
                    let key = format!(
                        "{version_id}\0change_set_element\0{}\0{}",
                        element.change_set_id, element.change.id
                    );
                    if seen.insert(key) {
                        rows.push(SurfaceRow::ChangeSetElement {
                            version_id: surface.by_version().then(|| version_id.clone()),
                            change_set_id: element.change_set_id,
                            entity_id: element.change.entity_id.as_string()?,
                            change_id: element.change.id,
                            schema_key: element.change.schema_key,
                            file_id: element.change.file_id,
                        });
                    }
                }
            }
        }
    }
    Ok(rows)
}

async fn visible_commits_for_version(
    commit_graph: &mut dyn CommitGraphReader,
    version_ref: &dyn VersionRefReader,
    version_id: &str,
    by_version: bool,
) -> Result<Vec<CommitGraphCommit>, LixError> {
    if by_version && version_id == GLOBAL_VERSION_ID {
        return commit_graph.all_commits().await;
    }
    let Some(head_commit_id) = version_ref.load_head_commit_id(version_id).await? else {
        return Ok(Vec::new());
    };
    Ok(commit_graph
        .reachable_commits(&head_commit_id)
        .await?
        .into_iter()
        .map(|reachable| reachable.commit)
        .collect())
}

fn surface_record_batch(
    surface: CommitSurface,
    projection: Option<&Vec<usize>>,
    rows: &[SurfaceRow],
) -> Result<RecordBatch> {
    let columns = surface_columns(surface, projection);
    let arrays = columns
        .iter()
        .map(|column| column.array(rows))
        .collect::<Vec<_>>();
    RecordBatch::try_new(surface_schema(&columns), arrays).map_err(|error| {
        DataFusionError::Execution(format!(
            "failed to build {} batch: {error}",
            surface.table_name()
        ))
    })
}

#[derive(Debug, Clone, Copy)]
enum SurfaceColumn {
    Id,
    ChangeSetId,
    ParentId,
    ChildId,
    ChangeId,
    EntityId,
    SchemaKey,
    FileId,
    VersionId,
    Global,
    Untracked,
}

impl SurfaceColumn {
    fn field(self) -> Field {
        match self {
            Self::Id => Field::new("id", DataType::Utf8, false),
            Self::ChangeSetId => Field::new("change_set_id", DataType::Utf8, false),
            Self::ParentId => Field::new("parent_id", DataType::Utf8, false),
            Self::ChildId => Field::new("child_id", DataType::Utf8, false),
            Self::ChangeId => Field::new("change_id", DataType::Utf8, false),
            Self::EntityId => Field::new("entity_id", DataType::Utf8, false),
            Self::SchemaKey => Field::new("schema_key", DataType::Utf8, false),
            Self::FileId => Field::new("file_id", DataType::Utf8, true),
            Self::VersionId => Field::new("lixcol_version_id", DataType::Utf8, false),
            Self::Global => Field::new("lixcol_global", DataType::Boolean, false),
            Self::Untracked => Field::new("lixcol_untracked", DataType::Boolean, false),
        }
    }

    fn array(self, rows: &[SurfaceRow]) -> ArrayRef {
        match self {
            Self::Id => string_array(rows.iter().map(|row| match row {
                SurfaceRow::Commit { id, .. } | SurfaceRow::ChangeSet { id, .. } => {
                    Some(id.as_str())
                }
                _ => None,
            })),
            Self::ChangeSetId => string_array(rows.iter().map(|row| match row {
                SurfaceRow::Commit { change_set_id, .. }
                | SurfaceRow::ChangeSetElement { change_set_id, .. } => {
                    Some(change_set_id.as_str())
                }
                _ => None,
            })),
            Self::ParentId => string_array(rows.iter().map(|row| match row {
                SurfaceRow::CommitEdge { parent_id, .. } => Some(parent_id.as_str()),
                _ => None,
            })),
            Self::ChildId => string_array(rows.iter().map(|row| match row {
                SurfaceRow::CommitEdge { child_id, .. } => Some(child_id.as_str()),
                _ => None,
            })),
            Self::ChangeId => string_array(rows.iter().map(|row| match row {
                SurfaceRow::ChangeSetElement { change_id, .. } => Some(change_id.as_str()),
                _ => None,
            })),
            Self::EntityId => string_array(rows.iter().map(|row| match row {
                SurfaceRow::ChangeSetElement { entity_id, .. } => Some(entity_id.as_str()),
                _ => None,
            })),
            Self::SchemaKey => string_array(rows.iter().map(|row| match row {
                SurfaceRow::ChangeSetElement { schema_key, .. } => Some(schema_key.as_str()),
                _ => None,
            })),
            Self::FileId => string_array(rows.iter().map(|row| match row {
                SurfaceRow::ChangeSetElement { file_id, .. } => file_id.as_deref(),
                _ => None,
            })),
            Self::VersionId => string_array(rows.iter().map(|row| match row {
                SurfaceRow::Commit { version_id, .. }
                | SurfaceRow::CommitEdge { version_id, .. }
                | SurfaceRow::ChangeSet { version_id, .. }
                | SurfaceRow::ChangeSetElement { version_id, .. } => version_id.as_deref(),
            })),
            Self::Global => Arc::new(BooleanArray::from(vec![true; rows.len()])) as ArrayRef,
            Self::Untracked => Arc::new(BooleanArray::from(vec![false; rows.len()])) as ArrayRef,
        }
    }
}

fn surface_columns(surface: CommitSurface, projection: Option<&Vec<usize>>) -> Vec<SurfaceColumn> {
    let all_columns = match surface {
        CommitSurface::Commit => vec![
            SurfaceColumn::Id,
            SurfaceColumn::ChangeSetId,
            SurfaceColumn::Global,
            SurfaceColumn::Untracked,
        ],
        CommitSurface::CommitByVersion => vec![
            SurfaceColumn::Id,
            SurfaceColumn::ChangeSetId,
            SurfaceColumn::VersionId,
            SurfaceColumn::Global,
            SurfaceColumn::Untracked,
        ],
        CommitSurface::CommitEdge => vec![
            SurfaceColumn::ParentId,
            SurfaceColumn::ChildId,
            SurfaceColumn::Global,
            SurfaceColumn::Untracked,
        ],
        CommitSurface::CommitEdgeByVersion => vec![
            SurfaceColumn::ParentId,
            SurfaceColumn::ChildId,
            SurfaceColumn::VersionId,
            SurfaceColumn::Global,
            SurfaceColumn::Untracked,
        ],
        CommitSurface::ChangeSet => vec![
            SurfaceColumn::Id,
            SurfaceColumn::Global,
            SurfaceColumn::Untracked,
        ],
        CommitSurface::ChangeSetByVersion => vec![
            SurfaceColumn::Id,
            SurfaceColumn::VersionId,
            SurfaceColumn::Global,
            SurfaceColumn::Untracked,
        ],
        CommitSurface::ChangeSetElement => vec![
            SurfaceColumn::ChangeSetId,
            SurfaceColumn::ChangeId,
            SurfaceColumn::EntityId,
            SurfaceColumn::SchemaKey,
            SurfaceColumn::FileId,
            SurfaceColumn::Global,
            SurfaceColumn::Untracked,
        ],
        CommitSurface::ChangeSetElementByVersion => vec![
            SurfaceColumn::ChangeSetId,
            SurfaceColumn::ChangeId,
            SurfaceColumn::EntityId,
            SurfaceColumn::SchemaKey,
            SurfaceColumn::FileId,
            SurfaceColumn::VersionId,
            SurfaceColumn::Global,
            SurfaceColumn::Untracked,
        ],
    };
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

fn surface_schema(columns: &[SurfaceColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| column.field())
            .collect::<Vec<_>>(),
    ))
}

fn commit_schema(by_version: bool) -> SchemaRef {
    surface_schema(&surface_columns(
        if by_version {
            CommitSurface::CommitByVersion
        } else {
            CommitSurface::Commit
        },
        None,
    ))
}

fn commit_edge_schema(by_version: bool) -> SchemaRef {
    surface_schema(&surface_columns(
        if by_version {
            CommitSurface::CommitEdgeByVersion
        } else {
            CommitSurface::CommitEdge
        },
        None,
    ))
}

fn change_set_schema(by_version: bool) -> SchemaRef {
    surface_schema(&surface_columns(
        if by_version {
            CommitSurface::ChangeSetByVersion
        } else {
            CommitSurface::ChangeSet
        },
        None,
    ))
}

fn change_set_element_schema(by_version: bool) -> SchemaRef {
    surface_schema(&surface_columns(
        if by_version {
            CommitSurface::ChangeSetElementByVersion
        } else {
            CommitSurface::ChangeSetElement
        },
        None,
    ))
}

fn projected_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
    match projection {
        Some(projection) => Arc::new(schema.project(projection).expect("projection is valid")),
        None => Arc::clone(schema),
    }
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
    DataFusionError::Execution(format!("sql2 commit provider error: {error}"))
}
