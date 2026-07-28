use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use tokio::sync::Mutex;

use crate::branch::{BranchHeadControlContext, BranchRefReader};
use crate::checkpoint::{CHECKPOINT_MARKER_SCHEMA_KEY, latest_checkpoint_for_branch};
use crate::commit_graph::CommitGraphReader;
use crate::entity_pk::EntityPk;
use crate::live_state::TrackedHeadContext;
use crate::sql2::result_metadata::json_field;
use crate::sql2::{SqlChangelogQuerySource, WriteAccess};
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    TrackedStateContext, TrackedStateDiffKind, TrackedStateDiffRequest, TrackedStateFilter,
};
use crate::{LixError, NullableKeyFilter};

use super::checkpoint::{filter_conjuncts, selected_heads};
use super::columns::{Col, ColumnTable, ColumnTableError};
use super::file::{FileIdConstraint, exact_string_column_constraint_from_filters};
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

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if filter.column_refs().iter().any(|column| {
            matches!(
                column.name.as_str(),
                "entity_pk" | "schema_key" | "file_id" | "lixcol_branch_id"
            )
        }) {
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
        let route = WorkingChangeRoute::from_filters(filters)?;
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
                    route,
                ),
                move |(active_branch_id, branch_ref, commit_graph, store, schema, route)| async move {
                    if route.contradictory {
                        return WORKING_CHANGE_COLS
                            .build(schema, &[])
                            .map_err(working_change_batch_error);
                    }
                    let heads = selected_heads(
                        branch_ref.as_ref(),
                        active_branch_id.as_deref(),
                        &route.branch_ids,
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                    let tracked_head = TrackedHeadContext::new();
                    // Normal tracked reads use the direct head epoch and do
                    // not need the historical graph or tracked-state reader.
                    // Keep both fallback-only so the accelerated route does
                    // not serialize behind an unrelated historical diff.
                    let mut graph = None;
                    let mut tracked = None;
                    let mut rows = Vec::new();
                    for head in heads {
                        if limit.is_some_and(|limit| rows.len() >= limit) {
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
                            if graph.is_none() {
                                graph = Some(commit_graph.lock().await);
                            }
                            let graph = graph
                                .as_mut()
                                .expect("historical working-change graph should be initialized");
                            let tracked = tracked.get_or_insert_with(|| {
                                TrackedStateContext::new().reader(store.clone())
                            });
                            let checkpoint_commit_id = latest_checkpoint_for_branch(
                                graph.as_mut(),
                                tracked,
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
                            if entry.identity.schema_key() == CHECKPOINT_MARKER_SCHEMA_KEY {
                                continue;
                            }
                            rows.push(WorkingChangeSqlRow {
                                entity_pk: entry.identity.entity_pk().as_json_array_text(),
                                schema_key: entry.identity.schema_key().to_owned(),
                                file_id: entry.identity.file_id().map(str::to_owned),
                                change_kind: match entry.kind {
                                    TrackedStateDiffKind::Added => "added",
                                    TrackedStateDiffKind::Modified => "modified",
                                    TrackedStateDiffKind::Removed => "removed",
                                },
                                before_change_id: entry.before.map(|row| row.change_id.to_string()),
                                after_change_id: entry.after.map(|row| row.change_id.to_string()),
                                branch_id: head.branch_id.clone(),
                            });
                            if limit.is_some_and(|limit| rows.len() >= limit) {
                                break;
                            }
                        }
                    }
                    WORKING_CHANGE_COLS
                        .build(schema, &rows)
                        .map_err(working_change_batch_error)
                },
            ),
        })
    }
}

#[derive(Clone, Debug)]
struct WorkingChangeRoute {
    branch_ids: FileIdConstraint,
    diff_request: TrackedStateDiffRequest,
    contradictory: bool,
}

impl WorkingChangeRoute {
    fn from_filters(filters: &[Expr]) -> Result<Self> {
        let conjuncts = filter_conjuncts(filters);
        let branch_ids =
            exact_string_column_constraint_from_filters(&conjuncts, "lixcol_branch_id")?;
        let schema_keys = string_constraint_values(exact_string_column_constraint_from_filters(
            &conjuncts,
            "schema_key",
        )?);
        let entity_pk_values = string_constraint_values(
            exact_string_column_constraint_from_filters(&conjuncts, "entity_pk")?,
        );
        let file_ids = string_constraint_values(exact_string_column_constraint_from_filters(
            &conjuncts, "file_id",
        )?);

        let mut contradictory = matches!(branch_ids, FileIdConstraint::None)
            || schema_keys.as_ref().is_some_and(Vec::is_empty)
            || entity_pk_values.as_ref().is_some_and(Vec::is_empty)
            || file_ids.as_ref().is_some_and(Vec::is_empty);
        let entity_pk_filter_is_explicit = entity_pk_values.is_some();
        let entity_pks = entity_pk_values
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entity_pk| EntityPk::from_json_array_text(&entity_pk).ok())
            .collect::<Vec<_>>();
        contradictory |= entity_pk_filter_is_explicit && entity_pks.is_empty();

        Ok(Self {
            branch_ids,
            diff_request: TrackedStateDiffRequest {
                filter: TrackedStateFilter {
                    schema_keys: schema_keys.unwrap_or_default(),
                    entity_pks,
                    file_ids: file_ids
                        .unwrap_or_default()
                        .into_iter()
                        .map(NullableKeyFilter::Value)
                        .collect(),
                    include_tombstones: true,
                },
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

#[cfg(test)]
mod tests {
    use datafusion::prelude::{col, lit};

    use crate::NullableKeyFilter;

    use super::{FileIdConstraint, WorkingChangeRoute};

    #[test]
    fn routes_exact_branch_and_tracked_identity_filters() {
        let route = WorkingChangeRoute::from_filters(&[
            col("lixcol_branch_id").eq(lit("01920000-0000-7000-8000-0000000000a1")),
            col("schema_key").eq(lit("acme_task")),
            col("entity_pk").eq(lit("[\"task-a\"]")),
            col("file_id").eq(lit("01920000-0000-7000-8000-0000000000a2")),
        ])
        .expect("exact working-change filters should route");

        assert_eq!(
            route.branch_ids,
            FileIdConstraint::Ids(["01920000-0000-7000-8000-0000000000a1".to_string()].into())
        );
        assert_eq!(
            route.diff_request.filter.schema_keys,
            vec!["acme_task".to_string()]
        );
        assert_eq!(
            route.diff_request.filter.entity_pks[0]
                .as_json_array_text()
                .expect("entity pk should encode"),
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
        let route = WorkingChangeRoute::from_filters(&[
            col("schema_key").eq(lit("acme_task")),
            col("schema_key").eq(lit("acme_note")),
        ])
        .expect("contradictory filters should still plan");

        assert!(route.contradictory);
    }
}
