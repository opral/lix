use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::Expr;
use tokio::sync::Mutex;

use crate::LixError;
use crate::branch::BranchRefReader;
use crate::checkpoint::{CHECKPOINT_MARKER_SCHEMA_KEY, latest_checkpoint_for_branch};
use crate::commit_graph::CommitGraphReader;
use crate::sql2::result_metadata::json_field;
use crate::sql2::{SqlChangelogQuerySource, WriteAccess};
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{TrackedStateContext, TrackedStateDiffKind, TrackedStateDiffRequest};

use super::checkpoint::selected_heads;
use super::columns::{Col, ColumnTable, ColumnTableError};
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, row_source};
use crate::sql2::error::lix_error_to_datafusion_error;

pub(super) async fn register_working_change_provider<S>(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    active_branch_id: Option<String>,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Box<dyn CommitGraphReader>,
    query_source: SqlChangelogQuerySource<S>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    register_spec_table(
        session,
        surface_name,
        Arc::new(WorkingChangeSpec {
            by_branch: active_branch_id.is_none(),
            active_branch_id,
            branch_ref,
            commit_graph: Arc::new(Mutex::new(commit_graph)),
            store: query_source.store,
        }),
        WriteAccess::read_only(),
    )
}

struct WorkingChangeSpec<S> {
    by_branch: bool,
    active_branch_id: Option<String>,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    store: S,
}

#[async_trait]
impl<S> TableSpec for WorkingChangeSpec<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn table_name(&self) -> &str {
        if self.by_branch {
            "lix_working_change_by_branch"
        } else {
            "lix_working_change"
        }
    }

    fn schema(&self) -> SchemaRef {
        working_change_schema(self.by_branch)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let schema = projected_schema(&self.schema(), projection);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            load: row_source(
                (
                    self.active_branch_id.clone(),
                    Arc::clone(&self.branch_ref),
                    Arc::clone(&self.commit_graph),
                    self.store.clone(),
                    schema,
                ),
                move |(active_branch_id, branch_ref, commit_graph, store, schema)| async move {
                    let heads = selected_heads(branch_ref.as_ref(), active_branch_id.as_deref())
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                    let mut graph = commit_graph.lock().await;
                    let mut tracked = TrackedStateContext::new().reader(store);
                    let mut rows = Vec::new();
                    for head in heads {
                        let checkpoint_commit_id = latest_checkpoint_for_branch(
                            graph.as_mut(),
                            &mut tracked,
                            &head.commit_id,
                            &head.branch_id,
                        )
                        .await
                        .map_err(lix_error_to_datafusion_error)?
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "branch '{}' has no checkpoint baseline",
                                head.branch_id
                            ))
                        })?;
                        let diff = tracked
                            .diff_commits(
                                &checkpoint_commit_id.to_string(),
                                &head.commit_id.to_string(),
                                &TrackedStateDiffRequest::default(),
                            )
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                        rows.extend(diff.entries.into_iter().filter_map(|entry| {
                            if entry.identity.schema_key == CHECKPOINT_MARKER_SCHEMA_KEY {
                                return None;
                            }
                            Some(WorkingChangeSqlRow {
                                entity_pk: entry.identity.entity_pk.as_json_array_text(),
                                schema_key: entry.identity.schema_key,
                                file_id: entry.identity.file_id,
                                change_kind: match entry.kind {
                                    TrackedStateDiffKind::Added => "added",
                                    TrackedStateDiffKind::Modified => "modified",
                                    TrackedStateDiffKind::Removed => "removed",
                                },
                                before_change_id: entry.before.map(|row| row.change_id.to_string()),
                                after_change_id: entry.after.map(|row| row.change_id.to_string()),
                                branch_id: head.branch_id.clone(),
                            })
                        }));
                    }
                    if let Some(limit) = limit {
                        rows.truncate(limit);
                    }
                    WORKING_CHANGE_COLS
                        .build(schema, &rows)
                        .map_err(working_change_batch_error)
                },
            ),
        })
    }
}

pub(super) fn working_change_schema(by_branch: bool) -> SchemaRef {
    let mut fields = vec![
        json_field("entity_pk", false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("change_kind", DataType::Utf8, false),
        Field::new("before_change_id", DataType::Utf8, true),
        Field::new("after_change_id", DataType::Utf8, true),
    ];
    if by_branch {
        fields.push(Field::new("lixcol_branch_id", DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

struct WorkingChangeSqlRow {
    entity_pk: Result<String, LixError>,
    schema_key: String,
    file_id: Option<String>,
    change_kind: &'static str,
    before_change_id: Option<String>,
    after_change_id: Option<String>,
    branch_id: String,
}

static WORKING_CHANGE_COLS: ColumnTable<WorkingChangeSqlRow> = ColumnTable {
    columns: &[
        (
            "entity_pk",
            Col::Utf8Fallible(|row| row.entity_pk.clone().map(Some)),
        ),
        ("schema_key", Col::Utf8(|row| Some(&row.schema_key))),
        ("file_id", Col::Utf8(|row| row.file_id.as_deref())),
        ("change_kind", Col::Utf8(|row| Some(row.change_kind))),
        (
            "before_change_id",
            Col::Utf8(|row| row.before_change_id.as_deref()),
        ),
        (
            "after_change_id",
            Col::Utf8(|row| row.after_change_id.as_deref()),
        ),
        ("lixcol_branch_id", Col::Utf8(|row| Some(&row.branch_id))),
    ],
};

fn working_change_batch_error(error: ColumnTableError) -> DataFusionError {
    match error {
        ColumnTableError::UnsupportedColumn(column) => {
            DataFusionError::Execution(format!("unsupported working-change column '{column}'"))
        }
        ColumnTableError::Arrow(error) | ColumnTableError::ArrowZeroColumn(error) => {
            DataFusionError::from(error)
        }
        ColumnTableError::Row(error) => lix_error_to_datafusion_error(error),
    }
}
