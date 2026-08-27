use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};

use crate::LixError;
use crate::changelog::{
    ChangeId, ChangeLoadRequest, ChangeRecord, ChangeScanRequest, ChangelogContext, ChangelogReader,
};
use crate::serialize_row_metadata;

use crate::sql2::SqlChangelogQuerySource;
use crate::sql2::WriteAccess;
use crate::sql2::change_materialization::{
    ChangePayloadProjection, MaterializedChange, materialize_changelog_change_record,
    materialize_commit_graph_change, public_change_row_ref,
};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::result_metadata::{json_field, row_ref_field};
use crate::storage_adapter::StorageAdapterRead;

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, scan_row_source};

pub(super) async fn register_lix_change_read_provider<S>(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    query_source: SqlChangelogQuerySource<S>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    register_spec_table(
        session,
        surface_name,
        Arc::new(ChangeSpec { query_source }),
        WriteAccess::read_only(),
    )
}

/// SQL spec for `lix_change`.
///
/// `lix_change` is the unscoped durable change surface: it scans direct
/// `changelog.change` records and unions derived `lix_commit` changes from
/// `changelog.commit`. It does not prove branch reachability. History
/// providers are the reachability-aware SQL surfaces.
struct ChangeSpec<S> {
    query_source: SqlChangelogQuerySource<S>,
}

#[async_trait]
impl<S> TableSpec for ChangeSpec<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    #[expect(clippy::unnecessary_literal_bound)]
    fn table_name(&self) -> &str {
        "lix_change"
    }

    fn schema(&self) -> SchemaRef {
        lix_change_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if exact_change_id_filter(filter).is_some() {
            TableProviderFilterPushDown::Exact
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
        let pushed_limit = if filters.is_empty() { limit } else { None };
        let route = change_scan_route(filters);
        let schema = projected_schema(&lix_change_schema(), projection);
        let payload_projection = change_payload_projection(schema.as_ref(), filters);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&schema),
                (self.query_source.clone(), schema),
                move |(query_source, schema)| async move {
                    let canonical_changes =
                        scan_changelog_changes(query_source.store, pushed_limit, route)
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                    let mut changes = Vec::with_capacity(canonical_changes.len());
                    for change in canonical_changes {
                        match change {
                            LixChangeRow::Direct(change) => changes.push(
                                materialize_changelog_change_record(change, payload_projection)
                                    .map_err(lix_error_to_datafusion_error)?,
                            ),
                            LixChangeRow::DerivedCommit(change) => changes.push(
                                materialize_commit_graph_change(change, payload_projection)
                                    .map_err(lix_error_to_datafusion_error)?,
                            ),
                        }
                    }
                    LIX_CHANGE_COLS
                        .build(schema, &changes)
                        .map_err(change_batch_error)
                },
            ),
        })
    }
}

fn change_payload_projection(schema: &Schema, filters: &[Expr]) -> ChangePayloadProjection {
    let needs = |column_name: &str| {
        schema.field_with_name(column_name).is_ok()
            || filters.iter().any(|filter| {
                filter
                    .column_refs()
                    .iter()
                    .any(|column| column.name.as_str() == column_name)
            })
    };
    ChangePayloadProjection {
        snapshot_content: needs("snapshot_content"),
        metadata: needs("metadata"),
    }
}

async fn scan_changelog_changes<S>(
    store: S,
    limit: Option<usize>,
    route: ChangeScanRoute,
) -> Result<Vec<LixChangeRow>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    match route {
        ChangeScanRoute::Empty => return Ok(Vec::new()),
        ChangeScanRoute::Exact(change_id) => {
            return load_exact_change(store, change_id)
                .await
                .map(|change| change.into_iter().collect());
        }
        ChangeScanRoute::All => {
            if limit == Some(0) {
                return Ok(Vec::new());
            }
        }
    }
    let packed_changes =
        crate::tracked_state::scan_change_records_from_commit_deltas(&store).await?;
    let mut reader = ChangelogContext::new().reader(store.clone());
    let mut changes = packed_changes
        .into_iter()
        .map(LixChangeRow::Direct)
        .collect::<Vec<_>>();
    let mut start_after = None::<String>;
    loop {
        let scan = reader
            .scan_changes(ChangeScanRequest {
                start_after: start_after.as_deref(),
                limit: Some(1024),
            })
            .await?;
        changes.extend(scan.entries.into_iter().map(LixChangeRow::Direct));
        let Some(next) = scan.next_start_after else {
            break;
        };
        start_after = Some(next.to_string());
    }
    let mut graph_reader = crate::commit_graph::CommitGraphContext::new().reader(store);
    for commit in graph_reader.all_nodes().await? {
        changes.push(LixChangeRow::DerivedCommit(
            crate::commit_graph::canonical_commit_change(&commit),
        ));
    }
    changes.sort_by_key(LixChangeRow::change_id);
    if let Some(limit) = limit {
        changes.truncate(limit);
    }
    Ok(changes)
}

#[derive(Clone, Copy)]
enum ChangeScanRoute {
    All,
    Exact(ChangeId),
    Empty,
}

fn change_scan_route(filters: &[Expr]) -> ChangeScanRoute {
    let mut exact = None;
    for filter in filters {
        let Some(candidate) = exact_change_id_filter(filter) else {
            continue;
        };
        let Some(candidate) = candidate else {
            return ChangeScanRoute::Empty;
        };
        if exact.is_some_and(|current| current != candidate) {
            return ChangeScanRoute::Empty;
        }
        exact = Some(candidate);
    }
    exact.map_or(ChangeScanRoute::All, ChangeScanRoute::Exact)
}

fn exact_change_id_filter(filter: &Expr) -> Option<Option<ChangeId>> {
    let Expr::BinaryExpr(binary) = filter else {
        return None;
    };
    if binary.op != Operator::Eq {
        return None;
    }
    let literal = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(column), literal) | (literal, Expr::Column(column))
            if column.name == "id" =>
        {
            literal
        }
        _ => return None,
    };
    let Expr::Literal(
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)),
        _,
    ) = literal
    else {
        return None;
    };
    Some(ChangeId::parse(value).ok())
}

async fn load_exact_change<S>(
    store: S,
    change_id: ChangeId,
) -> Result<Option<LixChangeRow>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    if let Some(change) = crate::tracked_state::load_change_record_by_id(&store, change_id).await? {
        return Ok(Some(LixChangeRow::Direct(change)));
    }

    let mut reader = ChangelogContext::new().reader(store.clone());
    if let Some(change) = reader
        .load_changes(ChangeLoadRequest {
            change_ids: std::slice::from_ref(&change_id),
        })
        .await?
        .into_iter()
        .next()
        .and_then(|(_, value)| value)
    {
        return Ok(Some(LixChangeRow::Direct(change)));
    }

    // A commit's synthetic `lix_commit` change is its commit id at ordinal
    // zero of the commit's own change address space, so the reverse lookup is
    // arithmetic plus the commit read we would have done anyway.
    let Some(commit_id) = change_id.as_commit_change() else {
        return Ok(None);
    };
    let Some(commit) = crate::commit_graph::CommitGraphContext::new()
        .reader(store)
        .load_node(&commit_id)
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(LixChangeRow::DerivedCommit(
        crate::commit_graph::canonical_commit_change(&commit),
    )))
}

enum LixChangeRow {
    Direct(ChangeRecord),
    DerivedCommit(crate::commit_graph::CommitGraphChange),
}

impl LixChangeRow {
    fn change_id(&self) -> ChangeId {
        match self {
            Self::Direct(change) => change.change_id,
            Self::DerivedCommit(change) => change.id,
        }
    }
}

pub(super) fn lix_change_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("account_id", DataType::Utf8, false),
        row_ref_field("row_ref", true),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        json_field("metadata", true),
        Field::new("created_at", DataType::Utf8, false),
        Field::new("origin_key", DataType::Utf8, true),
        json_field("snapshot_content", true),
    ]))
}

static LIX_CHANGE_COLS: ColumnTable<MaterializedChange> = ColumnTable {
    columns: &[
        ("id", Col::Utf8(|row| Some(row.id.as_str()))),
        ("account_id", Col::Utf8(|row| Some(row.account_id.as_str()))),
        (
            "row_ref",
            Col::Utf8Fallible(change_public_row_ref),
        ),
        ("schema_key", Col::Utf8(|row| Some(row.schema_key.as_str()))),
        ("file_id", Col::Utf8(|row| row.file_id.as_deref())),
        (
            "metadata",
            Col::Utf8Owned(|row| row.metadata.as_deref().map(serialize_row_metadata)),
        ),
        ("created_at", Col::Utf8(|row| Some(row.created_at.as_str()))),
        ("origin_key", Col::Utf8(|row| row.origin_key.as_deref())),
        (
            "snapshot_content",
            Col::Utf8(|row| row.snapshot_content.as_deref()),
        ),
    ],
};

fn change_public_row_ref(row: &MaterializedChange) -> Result<Option<String>, LixError> {
    public_change_row_ref(row).map(|row_ref| row_ref.map(|value| value.to_string()))
}

fn change_batch_error(error: ColumnTableError) -> DataFusionError {
    match error {
        ColumnTableError::UnsupportedColumn(column) => DataFusionError::Execution(format!(
            "sql2 does not support lix_change column '{column}'"
        )),
        ColumnTableError::Arrow(error) | ColumnTableError::ArrowZeroColumn(error) => {
            DataFusionError::Execution(format!("failed to build lix_change batch: {error}"))
        }
        ColumnTableError::Row(error) => lix_error_to_datafusion_error(error),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::logical_expr::{Expr, col, lit};

    use super::{ChangeScanRoute, change_payload_projection, change_scan_route, lix_change_schema};

    #[test]
    fn identity_projection_skips_json_payloads() {
        let full_schema = lix_change_schema();
        let projected = Schema::new(vec![
            full_schema.field_with_name("id").expect("id").clone(),
            full_schema
                .field_with_name("origin_key")
                .expect("origin_key")
                .clone(),
        ]);

        let projection = change_payload_projection(&projected, &[]);

        assert!(!projection.snapshot_content);
        assert!(!projection.metadata);
    }

    #[test]
    fn payload_filter_requires_materialization() {
        let full_schema = lix_change_schema();
        let projected = Schema::new(vec![full_schema.field_with_name("id").expect("id").clone()]);
        let filters = vec![Expr::IsNotNull(Box::new(col("metadata")))];

        let projection = change_payload_projection(&projected, &filters);

        assert!(!projection.snapshot_content);
        assert!(projection.metadata);
    }

    #[test]
    fn exact_change_id_route_accepts_uuid_and_rejects_impossible_literals() {
        let id = crate::changelog::ChangeId::for_test_label("exact-change-route");
        let route = change_scan_route(&[col("id").eq(lit(id.to_string()))]);
        assert!(matches!(route, ChangeScanRoute::Exact(actual) if actual == id));
        assert!(matches!(
            change_scan_route(&[col("id").eq(lit("not-a-uuid"))]),
            ChangeScanRoute::Empty
        ));
        assert!(matches!(
            change_scan_route(&[col("schema_key").eq(lit("example"))]),
            ChangeScanRoute::All
        ));
    }
}
