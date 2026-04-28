use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
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

use crate::engine2::changelog::{CanonicalChange, ChangelogReader, ChangelogScanRequest};
use crate::engine2::live_state::{LiveStateFilter, LiveStateReader, LiveStateScanRequest};
use crate::sql2::version_scope::resolve_provider_version_ids;
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

pub(crate) async fn register_commit_providers(
    session: &datafusion::prelude::SessionContext,
    active_version_id: &str,
    changelog: Arc<dyn ChangelogReader>,
    live_state: Arc<dyn LiveStateReader>,
) -> Result<(), LixError> {
    for surface in CommitSurface::all() {
        let provider = Arc::new(CommitSurfaceProvider::new(
            surface,
            active_version_id.to_string(),
            Arc::clone(&changelog),
            Arc::clone(&live_state),
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
    changelog: Arc<dyn ChangelogReader>,
    live_state: Arc<dyn LiveStateReader>,
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
        changelog: Arc<dyn ChangelogReader>,
        live_state: Arc<dyn LiveStateReader>,
    ) -> Self {
        Self {
            surface,
            active_version_id,
            schema: surface.schema(),
            changelog,
            live_state,
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
            Arc::clone(&self.changelog),
            Arc::clone(&self.live_state),
            projected_schema(&self.schema, projection),
            projection.cloned(),
            limit,
        )))
    }
}

struct CommitSurfaceScanExec {
    surface: CommitSurface,
    active_version_id: String,
    changelog: Arc<dyn ChangelogReader>,
    live_state: Arc<dyn LiveStateReader>,
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
        changelog: Arc<dyn ChangelogReader>,
        live_state: Arc<dyn LiveStateReader>,
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
            changelog,
            live_state,
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
        let changelog = Arc::clone(&self.changelog);
        let live_state = Arc::clone(&self.live_state);
        let projection = self.projection.clone();
        let limit = self.limit;
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let version_ids = resolve_provider_version_ids(
                Arc::clone(&live_state),
                (!surface.by_version()).then_some(active_version_id.as_str()),
                Vec::new(),
            )
            .await
            .map_err(lix_error_to_datafusion_error)?;
            let model = CommitSurfaceModel::load(changelog, live_state)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let rows = model.rows_for_surface(surface, &version_ids);
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
struct ParsedCommit {
    id: String,
    change_set_id: String,
    change_ids: Vec<String>,
    parent_commit_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct VersionHead {
    version_id: String,
    commit_id: String,
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

struct CommitSurfaceModel {
    commits: BTreeMap<String, ParsedCommit>,
    changes: BTreeMap<String, CanonicalChange>,
    version_heads: Vec<VersionHead>,
}

impl CommitSurfaceModel {
    async fn load(
        changelog: Arc<dyn ChangelogReader>,
        live_state: Arc<dyn LiveStateReader>,
    ) -> Result<Self, LixError> {
        let changes = changelog
            .scan_changes(&ChangelogScanRequest::default())
            .await?;
        let changes_by_id = changes
            .into_iter()
            .map(|change| (change.id.clone(), change))
            .collect::<BTreeMap<_, _>>();
        let commits = changes_by_id
            .values()
            .filter(|change| change.schema_key == "lix_commit")
            .map(parse_commit_change)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|commit| (commit.id.clone(), commit))
            .collect::<BTreeMap<_, _>>();
        let version_heads = load_version_heads(live_state).await?;
        Ok(Self {
            commits,
            changes: changes_by_id,
            version_heads,
        })
    }

    fn rows_for_surface(&self, surface: CommitSurface, version_ids: &[String]) -> Vec<SurfaceRow> {
        let mut rows = Vec::new();
        let mut seen = BTreeSet::<String>::new();
        for version_id in version_ids {
            let commit_ids = self.visible_commit_ids(version_id, surface.by_version());
            for commit_id in commit_ids {
                let Some(commit) = self.commits.get(&commit_id) else {
                    continue;
                };
                match surface {
                    CommitSurface::Commit | CommitSurface::CommitByVersion => {
                        let key = format!("{version_id}\0commit\0{}", commit.id);
                        if seen.insert(key) {
                            rows.push(SurfaceRow::Commit {
                                version_id: surface.by_version().then(|| version_id.clone()),
                                id: commit.id.clone(),
                                change_set_id: commit.change_set_id.clone(),
                            });
                        }
                    }
                    CommitSurface::CommitEdge | CommitSurface::CommitEdgeByVersion => {
                        for parent_id in &commit.parent_commit_ids {
                            let key = format!("{version_id}\0edge\0{parent_id}\0{}", commit.id);
                            if seen.insert(key) {
                                rows.push(SurfaceRow::CommitEdge {
                                    version_id: surface.by_version().then(|| version_id.clone()),
                                    parent_id: parent_id.clone(),
                                    child_id: commit.id.clone(),
                                });
                            }
                        }
                    }
                    CommitSurface::ChangeSet | CommitSurface::ChangeSetByVersion => {
                        let key = format!("{version_id}\0change_set\0{}", commit.change_set_id);
                        if seen.insert(key) {
                            rows.push(SurfaceRow::ChangeSet {
                                version_id: surface.by_version().then(|| version_id.clone()),
                                id: commit.change_set_id.clone(),
                            });
                        }
                    }
                    CommitSurface::ChangeSetElement | CommitSurface::ChangeSetElementByVersion => {
                        for change_id in &commit.change_ids {
                            let Some(change) = self.changes.get(change_id) else {
                                continue;
                            };
                            let key = format!(
                                "{version_id}\0change_set_element\0{}\0{}",
                                commit.change_set_id, change.id
                            );
                            if seen.insert(key) {
                                rows.push(SurfaceRow::ChangeSetElement {
                                    version_id: surface.by_version().then(|| version_id.clone()),
                                    change_set_id: commit.change_set_id.clone(),
                                    change_id: change.id.clone(),
                                    entity_id: change.entity_id.clone(),
                                    schema_key: change.schema_key.clone(),
                                    file_id: change.file_id.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }
        rows
    }

    fn visible_commit_ids(&self, version_id: &str, by_version: bool) -> Vec<String> {
        if by_version && version_id == GLOBAL_VERSION_ID {
            return self.commits.keys().cloned().collect();
        }
        self.reachable_commit_ids(version_id)
    }

    fn reachable_commit_ids(&self, version_id: &str) -> Vec<String> {
        let Some(head) = self
            .version_heads
            .iter()
            .find(|head| head.version_id == version_id)
        else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        self.collect_reachable(&head.commit_id, &mut seen, &mut out);
        out
    }

    fn collect_reachable(
        &self,
        commit_id: &str,
        seen: &mut BTreeSet<String>,
        out: &mut Vec<String>,
    ) {
        if !seen.insert(commit_id.to_string()) {
            return;
        }
        let Some(commit) = self.commits.get(commit_id) else {
            return;
        };
        out.push(commit.id.clone());
        for parent_id in &commit.parent_commit_ids {
            self.collect_reachable(parent_id, seen, out);
        }
    }
}

async fn load_version_heads(
    live_state: Arc<dyn LiveStateReader>,
) -> Result<Vec<VersionHead>, LixError> {
    let rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec!["lix_version_ref".to_string()],
                version_ids: vec![GLOBAL_VERSION_ID.to_string()],
                ..LiveStateFilter::default()
            },
            projection: Default::default(),
            limit: None,
        })
        .await?;
    rows.into_iter()
        .map(|row| parse_version_head(&row))
        .collect()
}

fn parse_version_head(
    row: &crate::engine2::live_state::LiveStateRow,
) -> Result<VersionHead, LixError> {
    let snapshot = parse_snapshot(row.snapshot_content.as_deref(), "lix_version_ref")?;
    let commit_id = required_string(&snapshot, "commit_id", "lix_version_ref")?;
    Ok(VersionHead {
        version_id: row.entity_id.clone(),
        commit_id,
    })
}

fn parse_commit_change(change: &CanonicalChange) -> Result<ParsedCommit, LixError> {
    let snapshot = parse_snapshot(change.snapshot_content.as_deref(), "lix_commit")?;
    Ok(ParsedCommit {
        id: required_string(&snapshot, "id", "lix_commit")?,
        change_set_id: required_string(&snapshot, "change_set_id", "lix_commit")?,
        change_ids: string_array_field(&snapshot, "change_ids", "lix_commit")?,
        parent_commit_ids: string_array_field(&snapshot, "parent_commit_ids", "lix_commit")?,
    })
}

fn parse_snapshot(snapshot_content: Option<&str>, schema_key: &str) -> Result<JsonValue, LixError> {
    let snapshot_content = snapshot_content.ok_or_else(|| {
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

fn required_string(
    snapshot: &JsonValue,
    field: &str,
    schema_key: &str,
) -> Result<String, LixError> {
    snapshot
        .get(field)
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("{schema_key} snapshot_content is missing {field}"),
            )
        })
}

fn string_array_field(
    snapshot: &JsonValue,
    field: &str,
    schema_key: &str,
) -> Result<Vec<String>, LixError> {
    Ok(snapshot
        .get(field)
        .and_then(JsonValue::as_array)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("{schema_key} snapshot_content is missing {field}"),
            )
        })?
        .iter()
        .map(|value| {
            value.as_str().map(str::to_string).ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("{schema_key}.{field} must contain only strings"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?)
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
