use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};

use crate::checkpoint::CHECKPOINT_SCHEMA_KEY;
use crate::row_pk::RowPk;
use crate::sql2::SqlChangelogQuerySource;
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::result_metadata::json_field;
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    TrackedStateContext, TrackedStateDiffKind, TrackedStateDiffRequest, TrackedStateFilter,
};
use crate::{LixError, NullableKeyFilter};

use super::branch_selection::filter_conjuncts;
use super::columns::{Col, ColumnTable, ColumnTableError};
use super::file::{FileIdConstraint, exact_string_column_constraint_from_filters};
use super::spec::{PlannedScan, SpecTableProvider, TableSpec, projected_schema, scan_row_source};

pub(crate) fn register_diff_function<S>(
    session: &datafusion::prelude::SessionContext,
    query_source: SqlChangelogQuerySource<S>,
) where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    session.register_udtf(
        "lix_diff",
        Arc::new(DiffFunction {
            store: query_source.store,
        }),
    );
}

struct DiffFunction<S> {
    store: S,
}

impl<S> fmt::Debug for DiffFunction<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiffFunction")
            .finish_non_exhaustive()
    }
}

impl<S> TableFunctionImpl for DiffFunction<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let [from_commit_id, to_commit_id] = args else {
            return Err(DataFusionError::Plan(
                "lix_diff requires exactly two commit ID arguments".to_string(),
            ));
        };
        let from_commit_id = commit_id_argument(from_commit_id, 1)?;
        let to_commit_id = commit_id_argument(to_commit_id, 2)?;
        Ok(Arc::new(SpecTableProvider::new(Arc::new(DiffSpec {
            store: self.store.clone(),
            from_commit_id,
            to_commit_id,
        }))))
    }
}

fn commit_id_argument(argument: &Expr, position: usize) -> Result<String> {
    let Expr::Literal(value, _) = argument else {
        return Err(DataFusionError::Plan(format!(
            "lix_diff argument {position} must be a commit ID literal or parameter"
        )));
    };
    value
        .try_as_str()
        .flatten()
        .map(ToString::to_string)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "lix_diff argument {position} must be a non-null text commit ID"
            ))
        })
}

struct DiffSpec<S> {
    store: S,
    from_commit_id: String,
    to_commit_id: String,
}

#[async_trait]
impl<S> TableSpec for DiffSpec<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn table_name(&self) -> &str {
        "lix_diff"
    }

    fn schema(&self) -> SchemaRef {
        diff_schema()
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
        let schema = projected_schema(&diff_schema(), projection);
        let route = DiffRoute::from_filters(filters);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&schema),
                (
                    self.store.clone(),
                    schema,
                    route,
                    self.from_commit_id.clone(),
                    self.to_commit_id.clone(),
                ),
                move |(store, schema, route, from_commit_id, to_commit_id)| async move {
                    if limit == Some(0) || route.contradictory {
                        return DIFF_COLS.build(schema, &[]).map_err(diff_batch_error);
                    }
                    let mut tracked = TrackedStateContext::new().reader(store);
                    let diff = tracked
                        .diff_commits(&from_commit_id, &to_commit_id, &route.request)
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                    let mut rows = Vec::with_capacity(diff.entries.len());
                    for entry in diff.entries {
                        if entry.identity.schema_key() == CHECKPOINT_SCHEMA_KEY
                            || entry.identity.schema_key()
                                == crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
                        {
                            continue;
                        }
                        rows.push(DiffSqlRow {
                            diff_id: entry.diff_id(),
                            row_pk: entry.identity.row_pk().as_json_array_text(),
                            schema_key: entry.identity.schema_key().to_owned(),
                            file_id: entry.identity.file_id().map(str::to_owned),
                            diff_type: match entry.kind {
                                TrackedStateDiffKind::Added => "added",
                                TrackedStateDiffKind::Modified => "modified",
                                TrackedStateDiffKind::Removed => "removed",
                            },
                            before_change_id: entry.before.map(|row| row.change_id.to_string()),
                            after_change_id: entry.after.map(|row| row.change_id.to_string()),
                        });
                        if limit.is_some_and(|limit| rows.len() >= limit) {
                            break;
                        }
                    }
                    DIFF_COLS.build(schema, &rows).map_err(diff_batch_error)
                },
            ),
        })
    }
}

#[derive(Clone, Debug)]
struct DiffRoute {
    request: TrackedStateDiffRequest,
    contradictory: bool,
}

impl DiffRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let conjuncts = filter_conjuncts(filters);
        let schema_keys = optional_values(&conjuncts, "schema_key");
        let row_pk_values = optional_values(&conjuncts, "row_pk");
        let file_ids = optional_values(&conjuncts, "file_id");
        let mut contradictory = schema_keys.as_ref().is_some_and(Vec::is_empty)
            || row_pk_values.as_ref().is_some_and(Vec::is_empty)
            || file_ids.as_ref().is_some_and(Vec::is_empty);
        let explicit_row_filter = row_pk_values.is_some();
        let row_pks = row_pk_values
            .unwrap_or_default()
            .into_iter()
            .filter_map(|value| RowPk::from_json_array_text(&value).ok())
            .collect::<Vec<_>>();
        contradictory |= explicit_row_filter && row_pks.is_empty();
        Self {
            request: TrackedStateDiffRequest {
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
        }
    }
}

fn optional_values(conjuncts: &[Expr], column: &'static str) -> Option<Vec<String>> {
    match exact_string_column_constraint_from_filters(conjuncts, column) {
        Ok(FileIdConstraint::All) | Err(_) => None,
        Ok(FileIdConstraint::None) => Some(Vec::new()),
        Ok(FileIdConstraint::Ids(values)) => Some(values.into_iter().collect()),
    }
}

fn diff_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("diff_id", DataType::Utf8, false),
        json_field("row_pk", false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("diff_type", DataType::Utf8, false),
        Field::new("before_change_id", DataType::Utf8, true),
        Field::new("after_change_id", DataType::Utf8, true),
    ]))
}

struct DiffSqlRow {
    diff_id: Result<String, LixError>,
    row_pk: Result<String, LixError>,
    schema_key: String,
    file_id: Option<String>,
    diff_type: &'static str,
    before_change_id: Option<String>,
    after_change_id: Option<String>,
}

static DIFF_COLS: ColumnTable<DiffSqlRow> = ColumnTable {
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

fn diff_batch_error(error: ColumnTableError) -> DataFusionError {
    match error {
        ColumnTableError::UnsupportedColumn(column) => {
            DataFusionError::Execution(format!("unsupported diff column '{column}'"))
        }
        ColumnTableError::Arrow(error) | ColumnTableError::ArrowZeroColumn(error) => {
            DataFusionError::from(error)
        }
        ColumnTableError::Row(error) => lix_error_to_datafusion_error(error),
    }
}
