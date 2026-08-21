use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use tokio::sync::Mutex;

use crate::branch::{BranchHeadControlContext, BranchRefReader};
use crate::checkpoint::{CHECKPOINT_SCHEMA_KEY, checkpoint_commit_id_at_head};
use crate::commit_graph::CommitGraphReader;
use crate::hot_state::TrackedHeadContext;
use crate::row_pk::RowPk;
use crate::sql2::SqlChangelogQuerySource;
use crate::sql2::result_metadata::json_field;
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    TrackedStateContext, TrackedStateDiffKind, TrackedStateDiffRequest, TrackedStateFilter,
};
use crate::{LixError, NullableKeyFilter};

use super::branch_selection::{filter_conjuncts, selected_heads};
use super::columns::{Col, ColumnTable, ColumnTableError};
use super::file::{FileIdConstraint, exact_string_column_constraint_from_filters};
use super::spec::{PlannedScan, SpecTableProvider, TableSpec, projected_schema, scan_row_source};
use crate::sql2::error::lix_error_to_datafusion_error;

pub(super) async fn register_working_diff_provider<S>(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    active_branch_id: String,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Box<dyn CommitGraphReader>,
    query_source: SqlChangelogQuerySource<S>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let provider: Arc<dyn TableProvider> =
        Arc::new(SpecTableProvider::new(Arc::new(WorkingDiffSpec {
            active_branch_id,
            branch_ref,
            commit_graph: Arc::new(Mutex::new(commit_graph)),
            store: query_source.store,
        })));
    session.register_udtf(
        surface_name,
        Arc::new(ZeroArgumentTableFunction {
            name: surface_name.to_string(),
            provider,
        }),
    );
    Ok(())
}

struct ZeroArgumentTableFunction {
    name: String,
    provider: Arc<dyn TableProvider>,
}

impl std::fmt::Debug for ZeroArgumentTableFunction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ZeroArgumentTableFunction")
            .field(&self.name)
            .finish()
    }
}

impl datafusion::catalog::TableFunctionImpl for ZeroArgumentTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if !args.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "{} expects no arguments",
                self.name
            )));
        }
        Ok(Arc::clone(&self.provider))
    }
}

struct WorkingDiffSpec<S> {
    active_branch_id: String,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    store: S,
}

#[async_trait]
impl<S> TableSpec for WorkingDiffSpec<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn table_name(&self) -> &str {
        "lix_working_diff"
    }

    fn schema(&self) -> SchemaRef {
        working_diff_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if filter
            .column_refs()
            .iter()
            .any(|column| matches!(column.name.as_str(), "row_pk" | "schema_key" | "file_id"))
        {
            TableProviderFilterPushDown::Inexact
        } else {
            TableProviderFilterPushDown::Unsupported
        }
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let schema = projected_schema(&self.schema(), projection);
        let read = WorkingDiffReadColumns::from_schema(&schema);
        let route = WorkingDiffRoute::from_filters(filters)?;
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&schema),
                (
                    self.active_branch_id.clone(),
                    Arc::clone(&self.branch_ref),
                    Arc::clone(&self.commit_graph),
                    self.store.clone(),
                    schema,
                    route,
                    read,
                ),
                move |(active_branch_id, branch_ref, _commit_graph, store, schema, route, read)| async move {
                    if limit == Some(0) || route.contradictory {
                        return WORKING_DIFF_COLS
                            .build(schema, &[])
                            .map_err(working_diff_batch_error);
                    }
                    let heads = selected_heads(
                        branch_ref.as_ref(),
                        Some(active_branch_id.as_str()),
                        &FileIdConstraint::All,
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                    let tracked_head = TrackedHeadContext::new();
                    // Normal tracked reads use the direct head epoch and do
                    // not need the historical graph or tracked-state reader.
                    // Keep both fallback-only so the accelerated route does
                    // not serialize behind an unrelated historical diff.
                    let mut tracked = None;
                    let mut rows = Vec::new();
                    let mut row_count = 0;
                    for head in heads {
                        if limit.is_some_and(|limit| row_count >= limit) {
                            break;
                        }
                        let direct_diff = match BranchHeadControlContext::new()
                            .reader(store.clone())
                            .load(&head.branch_id)
                            .await
                            .map_err(lix_error_to_datafusion_error)?
                        {
                            Some(control) if control.head_commit_id == head.commit_id => {
                                tracked_head
                                    .reader(store.clone())
                                    .working_diff_for_control(
                                        &head.branch_id,
                                        control,
                                        &route.diff_request,
                                    )
                                    .await
                                    .map_err(lix_error_to_datafusion_error)?
                            }
                            _ => None,
                        };
                        let diff = if let Some(direct) = direct_diff {
                            direct.diff
                        } else {
                            let tracked = tracked.get_or_insert_with(|| {
                                TrackedStateContext::new().reader(store.clone())
                            });
                            let checkpoint_commit_id = checkpoint_commit_id_at_head(
                                store.clone(),
                                &head.branch_id,
                                head.commit_id,
                            )
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                            tracked
                                .diff_commits(
                                    &checkpoint_commit_id.to_string(),
                                    &head.commit_id.to_string(),
                                    &route.diff_request,
                                )
                                .await
                                .map_err(lix_error_to_datafusion_error)?
                        };
                        for entry in diff.entries {
                            if entry.identity.schema_key() == CHECKPOINT_SCHEMA_KEY
                                || entry.identity.schema_key()
                                    == crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
                            {
                                continue;
                            }
                            row_count += 1;
                            if schema.fields().is_empty() {
                                if limit.is_some_and(|limit| row_count >= limit) {
                                    break;
                                }
                                continue;
                            }
                            rows.push(WorkingDiffSqlRow {
                                diff_id: if read.diff_id {
                                    entry.diff_id()
                                } else {
                                    Ok(String::new())
                                },
                                row_pk: if read.row_pk {
                                    entry.identity.row_pk().as_json_array_text()
                                } else {
                                    Ok(String::new())
                                },
                                schema_key: read
                                    .schema_key
                                    .then(|| entry.identity.schema_key().to_owned())
                                    .unwrap_or_default(),
                                file_id: if read.file_id {
                                    entry.identity.file_id().map(str::to_owned)
                                } else {
                                    None
                                },
                                diff_type: if read.diff_type {
                                    match entry.kind {
                                        TrackedStateDiffKind::Added => "added",
                                        TrackedStateDiffKind::Modified => "modified",
                                        TrackedStateDiffKind::Removed => "removed",
                                    }
                                } else {
                                    ""
                                },
                                before_change_id: if read.before_change_id {
                                    entry.before.map(|row| row.change_id.to_string())
                                } else {
                                    None
                                },
                                after_change_id: if read.after_change_id {
                                    entry.after.map(|row| row.change_id.to_string())
                                } else {
                                    None
                                },
                            });
                            if limit.is_some_and(|limit| row_count >= limit) {
                                break;
                            }
                        }
                    }
                    if schema.fields().is_empty() {
                        return RecordBatch::try_new_with_options(
                            schema,
                            Vec::new(),
                            &RecordBatchOptions::new().with_row_count(Some(row_count)),
                        )
                        .map_err(DataFusionError::from);
                    }
                    WORKING_DIFF_COLS
                        .build(schema, &rows)
                        .map_err(working_diff_batch_error)
                },
            ),
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct WorkingDiffReadColumns {
    diff_id: bool,
    row_pk: bool,
    schema_key: bool,
    file_id: bool,
    diff_type: bool,
    before_change_id: bool,
    after_change_id: bool,
}

impl WorkingDiffReadColumns {
    fn from_schema(schema: &Schema) -> Self {
        Self {
            diff_id: schema.index_of("diff_id").is_ok(),
            row_pk: schema.index_of("row_pk").is_ok(),
            schema_key: schema.index_of("schema_key").is_ok(),
            file_id: schema.index_of("file_id").is_ok(),
            diff_type: schema.index_of("diff_type").is_ok(),
            before_change_id: schema.index_of("before_change_id").is_ok(),
            after_change_id: schema.index_of("after_change_id").is_ok(),
        }
    }
}

#[derive(Clone, Debug)]
struct WorkingDiffRoute {
    diff_request: TrackedStateDiffRequest,
    contradictory: bool,
}

impl WorkingDiffRoute {
    fn from_filters(filters: &[Expr]) -> Result<Self> {
        let conjuncts = filter_conjuncts(filters);
        let schema_keys = string_constraint_values(exact_string_column_constraint_from_filters(
            &conjuncts,
            "schema_key",
        )?);
        let row_pk_values = string_constraint_values(exact_string_column_constraint_from_filters(
            &conjuncts, "row_pk",
        )?);
        let file_ids = string_constraint_values(exact_string_column_constraint_from_filters(
            &conjuncts, "file_id",
        )?);

        let mut contradictory = schema_keys.as_ref().is_some_and(Vec::is_empty)
            || row_pk_values.as_ref().is_some_and(Vec::is_empty)
            || file_ids.as_ref().is_some_and(Vec::is_empty);
        let row_pk_filter_is_explicit = row_pk_values.is_some();
        let row_pks = row_pk_values
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row_pk| RowPk::from_json_array_text(&row_pk).ok())
            .collect::<Vec<_>>();
        contradictory |= row_pk_filter_is_explicit && row_pks.is_empty();

        Ok(Self {
            diff_request: TrackedStateDiffRequest {
                filter: TrackedStateFilter {
                    schema_keys: schema_keys.unwrap_or_default(),
                    row_pks,
                    file_ids: file_ids
                        .unwrap_or_default()
                        .into_iter()
                        .map(NullableKeyFilter::Value)
                        .collect(),
                    row_pk_lower: None,
                    row_pk_upper: None,
                    include_tombstones: true,
                },
                retain_payloads: false,
            },
            contradictory,
        })
    }
}

fn string_constraint_values(constraint: FileIdConstraint) -> Option<Vec<String>> {
    match constraint {
        FileIdConstraint::All => None,
        FileIdConstraint::None => Some(Vec::new()),
        FileIdConstraint::Ids(values) => Some(values.into_iter().collect()),
    }
}

pub(crate) fn working_diff_schema() -> SchemaRef {
    let fields = vec![
        Field::new("diff_id", DataType::Utf8, false),
        json_field("row_pk", false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("diff_type", DataType::Utf8, false),
        Field::new("before_change_id", DataType::Utf8, true),
        Field::new("after_change_id", DataType::Utf8, true),
    ];
    Arc::new(Schema::new(fields))
}

struct WorkingDiffSqlRow {
    diff_id: Result<String, LixError>,
    row_pk: Result<String, LixError>,
    schema_key: String,
    file_id: Option<String>,
    diff_type: &'static str,
    before_change_id: Option<String>,
    after_change_id: Option<String>,
}

static WORKING_DIFF_COLS: ColumnTable<WorkingDiffSqlRow> = ColumnTable {
    columns: &[
        (
            "diff_id",
            Col::Utf8Fallible(|row| row.diff_id.clone().map(Some)),
        ),
        (
            "row_pk",
            Col::Utf8Fallible(|row| row.row_pk.clone().map(Some)),
        ),
        ("schema_key", Col::Utf8(|row| Some(&row.schema_key))),
        ("file_id", Col::Utf8(|row| row.file_id.as_deref())),
        ("diff_type", Col::Utf8(|row| Some(row.diff_type))),
        (
            "before_change_id",
            Col::Utf8(|row| row.before_change_id.as_deref()),
        ),
        (
            "after_change_id",
            Col::Utf8(|row| row.after_change_id.as_deref()),
        ),
    ],
};

fn working_diff_batch_error(error: ColumnTableError) -> DataFusionError {
    match error {
        ColumnTableError::UnsupportedColumn(column) => {
            DataFusionError::Execution(format!("unsupported working-diff column '{column}'"))
        }
        ColumnTableError::Arrow(error) | ColumnTableError::ArrowZeroColumn(error) => {
            DataFusionError::from(error)
        }
        ColumnTableError::Row(error) => lix_error_to_datafusion_error(error),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::{col, lit};

    use crate::NullableKeyFilter;

    use super::{WorkingDiffReadColumns, WorkingDiffRoute, working_diff_schema};

    #[test]
    fn projected_working_diff_columns_skip_unrequested_values() {
        let schema = working_diff_schema()
            .project(&[1, 2, 3])
            .expect("project working-diff aggregate inputs");
        let read = WorkingDiffReadColumns::from_schema(&schema);

        assert!(read.row_pk);
        assert!(read.schema_key);
        assert!(read.file_id);
        assert!(!read.diff_id);
        assert!(!read.diff_type);
        assert!(!read.before_change_id);
        assert!(!read.after_change_id);
    }

    #[test]
    fn routes_exact_tracked_identity_filters() {
        let route = WorkingDiffRoute::from_filters(&[
            col("schema_key").eq(lit("acme_task")),
            col("row_pk").eq(lit("[\"task-a\"]")),
            col("file_id").eq(lit("01920000-0000-7000-8000-0000000000a2")),
        ])
        .expect("exact working-diff filters should route");

        assert_eq!(
            route.diff_request.filter.schema_keys,
            vec!["acme_task".to_string()]
        );
        assert_eq!(
            route.diff_request.filter.row_pks[0]
                .as_json_array_text()
                .expect("row pk should encode"),
            "[\"task-a\"]"
        );
        assert_eq!(
            route.diff_request.filter.file_ids,
            vec![NullableKeyFilter::Value(
                "01920000-0000-7000-8000-0000000000a2".to_string()
            )]
        );
        assert!(!route.contradictory);
    }

    #[test]
    fn contradictory_exact_filters_short_circuit() {
        let route = WorkingDiffRoute::from_filters(&[
            col("schema_key").eq(lit("acme_task")),
            col("schema_key").eq(lit("acme_note")),
        ])
        .expect("contradictory filters should still plan");

        assert!(route.contradictory);
    }
}
