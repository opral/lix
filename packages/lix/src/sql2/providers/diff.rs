use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};

use crate::checkpoint::CHECKPOINT_MARKER_SCHEMA_KEY;
use crate::row_pk::RowPk;
use crate::forktree::{ForkTreeReadFacade, HistoricalStateRow};
use crate::sql2::SqlChangelogQuerySource;
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::result_metadata::json_field;
use crate::state::encode_diff_id;
use crate::storage_adapter::StorageAdapterRead;
use crate::{LixError, NullableKeyFilter};

use super::checkpoint::filter_conjuncts;
use super::columns::{Col, ColumnTable, ColumnTableError};
use super::file::{FileIdConstraint, exact_string_column_constraint_from_filters};
use super::history_util::StateFilter;
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
            forktree_reader: query_source.forktree_reader,
        }),
    );
}

struct DiffFunction<S> {
    forktree_reader: ForkTreeReadFacade<S>,
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
            forktree_reader: self.forktree_reader.clone(),
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
    forktree_reader: ForkTreeReadFacade<S>,
    from_commit_id: String,
    to_commit_id: String,
}

#[async_trait]
impl<S> TableSpec<S> for DiffSpec<S>
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
                    self.forktree_reader.clone(),
                    schema,
                    route,
                    self.from_commit_id.clone(),
                    self.to_commit_id.clone(),
                ),
                move |(forktree_reader, schema, route, from_commit_id, to_commit_id)| async move {
                    if route.contradictory {
                        return DIFF_COLS.build(schema, &[]).map_err(diff_batch_error);
                    }
                    let from_commit_id =
                        crate::changelog::CommitId::parse_lix(&from_commit_id, "diff from commit")
                            .map_err(lix_error_to_datafusion_error)?;
                    let to_commit_id =
                        crate::changelog::CommitId::parse_lix(&to_commit_id, "diff to commit")
                            .map_err(lix_error_to_datafusion_error)?;
                    let changes = forktree_reader
                        .diff_state_rows_between_commits(from_commit_id, to_commit_id)
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                    let mut rows = Vec::with_capacity(changes.len());
                    for change in changes {
                        let before = change
                            .before
                            .filter(|row| diff_row_matches(row, &route.filter));
                        let after = change
                            .after
                            .filter(|row| diff_row_matches(row, &route.filter));
                        if before.is_none() && after.is_none() {
                            continue;
                        }
                        if before
                            .as_ref()
                            .or(after.as_ref())
                            .is_some_and(|row| is_internal_marker_schema(&row.key.schema_key))
                        {
                            continue;
                        }
                        if before
                            .as_ref()
                            .zip(after.as_ref())
                            .is_some_and(|(before, after)| same_authenticated_state(before, after))
                        {
                            continue;
                        }
                        let kind = match (before.as_ref(), after.as_ref()) {
                            (None, Some(_)) => "added",
                            (Some(_), None) => "removed",
                            (Some(_), Some(row)) if row.deleted => "removed",
                            (Some(row), Some(_)) if row.deleted => "added",
                            (Some(_), Some(_)) => "modified",
                            (None, None) => continue,
                        };
                        let identity = before
                            .as_ref()
                            .or(after.as_ref())
                            .expect("diff entry has one side")
                            .key
                            .clone();
                        rows.push(DiffSqlRow {
                            diff_id: encode_diff_id(
                                before.as_ref().map(|row| row.change_id),
                                after.as_ref().map(|row| row.change_id),
                            ),
                            row_pk: identity.row_pk.as_json_array_text(),
                            schema_key: identity.schema_key,
                            file_id: identity.file_id,
                            diff_type: kind,
                            before_change_id: before.map(|row| row.change_id.to_string()),
                            after_change_id: after.map(|row| row.change_id.to_string()),
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

/// State-page placement is authenticated provenance, not part of a row's
/// semantic identity. In particular, a merge may republish an unchanged
/// source member into a new commit page while preserving its ChangeId and
/// payload. Such a row must not appear as a public diff modification.
fn same_authenticated_state(before: &HistoricalStateRow, after: &HistoricalStateRow) -> bool {
    before.key == after.key
        && before.global == after.global
        && before.change_id == after.change_id
        && before.created_at == after.created_at
        && before.updated_at == after.updated_at
        && before.snapshot_content == after.snapshot_content
        && before.metadata == after.metadata
        && before.deleted == after.deleted
        && before.blob_manifest_object_ids == after.blob_manifest_object_ids
}

#[derive(Clone, Debug)]
struct DiffRoute {
    filter: StateFilter,
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
            filter: StateFilter {
                schema_keys: schema_keys.unwrap_or_default(),
                row_pks,
                file_ids: file_ids
                    .unwrap_or_default()
                    .into_iter()
                    .map(NullableKeyFilter::Value)
                    .collect(),
                include_tombstones: true,
            },
            contradictory,
        }
    }
}

fn diff_row_matches(row: &HistoricalStateRow, filter: &StateFilter) -> bool {
    (filter.schema_keys.is_empty() || filter.schema_keys.contains(&row.key.schema_key))
        && (filter.row_pks.is_empty() || filter.row_pks.contains(&row.key.row_pk))
        && (filter.file_ids.is_empty()
            || filter.file_ids.iter().any(|file_id| match file_id {
                NullableKeyFilter::Any => true,
                NullableKeyFilter::Null => row.key.file_id.is_none(),
                NullableKeyFilter::Value(file_id) => {
                    row.key.file_id.as_deref() == Some(file_id.as_str())
                }
            }))
}

fn is_internal_marker_schema(schema_key: &str) -> bool {
    schema_key == CHECKPOINT_MARKER_SCHEMA_KEY
        || schema_key == crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
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

#[cfg(test)]
mod tests {
    use super::{is_internal_marker_schema, same_authenticated_state};
    use crate::changelog::{ChangeId, CommitId};
    use crate::checkpoint::CHECKPOINT_MARKER_SCHEMA_KEY;
    use crate::common::{LixTimestamp, SharedStr};
    use crate::row_pk::RowPk;
    use crate::forktree::{HistoricalStateRow, ObjectId, StateKey};
    use crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY;

    fn semantic_row() -> HistoricalStateRow {
        HistoricalStateRow {
            key: StateKey {
                schema_key: "test".to_owned(),
                file_id: Some("file".to_owned()),
                row_pk: RowPk::single("row"),
            },
            global: false,
            change_id: ChangeId::for_test_label("change"),
            commit_id: CommitId::for_test_label("page-placement"),
            created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
            updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
            snapshot_content: Some(SharedStr::from("value")),
            metadata: Some(SharedStr::from("metadata")),
            deleted: false,
            blob_manifest_object_ids: vec![ObjectId::from_bytes([0x11; 32])],
        }
    }

    #[test]
    fn internal_markers_are_suppressed_when_present_only_after() {
        assert!(is_internal_marker_schema(CHECKPOINT_MARKER_SCHEMA_KEY));
        assert!(is_internal_marker_schema(UNDO_REDO_MARKER_SCHEMA_KEY));
        assert!(!is_internal_marker_schema("lix_file_descriptor"));
    }

    #[test]
    fn semantic_diff_equality_ignores_only_page_provenance() {
        let cases: [(&str, Box<dyn Fn(&mut HistoricalStateRow)>); 10] = [
            ("global", Box::new(|row| row.global = true)),
            (
                "change_id",
                Box::new(|row| row.change_id = ChangeId::for_test_label("other-change")),
            ),
            (
                "key",
                Box::new(|row| row.key.row_pk = RowPk::single("other-row")),
            ),
            (
                "created_at",
                Box::new(|row| row.created_at = LixTimestamp::from_unix_millis_utc_lossy(3)),
            ),
            (
                "updated_at",
                Box::new(|row| row.updated_at = LixTimestamp::from_unix_millis_utc_lossy(4)),
            ),
            ("value_to_null", Box::new(|row| row.snapshot_content = None)),
            ("deleted", Box::new(|row| row.deleted = true)),
            (
                "metadata",
                Box::new(|row| row.metadata = Some(SharedStr::from("other-metadata"))),
            ),
            (
                "blob_manifest",
                Box::new(|row| {
                    row.blob_manifest_object_ids = vec![ObjectId::from_bytes([0x22; 32])]
                }),
            ),
            (
                "null_to_value",
                Box::new(|row| row.snapshot_content = Some(SharedStr::from("other-value"))),
            ),
        ];

        for (field, mutate) in cases {
            let mut changed = semantic_row();
            mutate(&mut changed);
            assert!(
                !same_authenticated_state(&semantic_row(), &changed),
                "semantic field {field} must remain visible"
            );
        }

        let mut page_republished = semantic_row();
        page_republished.commit_id = CommitId::for_test_label("republished-page");
        assert!(same_authenticated_state(&semantic_row(), &page_republished));
    }
}
