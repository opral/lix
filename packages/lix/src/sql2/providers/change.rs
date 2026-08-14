use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};

use crate::LixError;
#[cfg(test)]
use crate::changelog::CommitId;
use crate::changelog::{ChangeId, ChangeRecord};
use crate::forktree::ForkTreeReadFacade;
use crate::serialize_row_metadata;

use crate::sql2::SqlChangelogQuerySource;
use crate::sql2::WriteAccess;
use crate::sql2::change_materialization::{
    ChangePayloadProjection, MaterializedChange, materialize_changelog_change_record,
    materialize_commit_graph_change,
};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::result_metadata::json_field;
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
impl<S> TableSpec<S> for ChangeSpec<S>
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
                    let mut forktree_reader = query_source.forktree_reader;
                    let canonical_changes = match route {
                        ChangeScanRoute::Exact(change_id) => {
                            load_exact_change(&forktree_reader, change_id)
                                .await
                                .map(|change| change.into_iter().collect())
                                .map_err(lix_error_to_datafusion_error)?
                        }
                        ChangeScanRoute::All | ChangeScanRoute::Empty => {
                            scan_changelog_changes(&forktree_reader, pushed_limit, route)
                                .await
                                .map_err(lix_error_to_datafusion_error)?
                        }
                    };
                    let mut changes = Vec::with_capacity(canonical_changes.len());
                    for change in canonical_changes {
                        match change {
                            LixChangeRow::Direct(change) => changes.push(
                                materialize_changelog_change_record(
                                    &mut forktree_reader,
                                    change,
                                    payload_projection,
                                )
                                .await
                                .map_err(lix_error_to_datafusion_error)?,
                            ),
                            LixChangeRow::DerivedCommit(change) => changes.push(
                                materialize_commit_graph_change(
                                    &mut forktree_reader,
                                    change,
                                    payload_projection,
                                )
                                .await
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
    reader: &ForkTreeReadFacade<S>,
    limit: Option<usize>,
    route: ChangeScanRoute,
) -> Result<Vec<LixChangeRow>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    match route {
        ChangeScanRoute::Empty => return Ok(Vec::new()),
        ChangeScanRoute::Exact(_) => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "exact changelog routes must use the operation-owned reader at the SQL boundary",
            ));
        }
        ChangeScanRoute::All => {}
    }
    let mut seen = BTreeSet::new();
    let mut changes = Vec::new();
    let mut start_after = None;
    loop {
        let page = reader.scan_change_records(start_after, 1024).await?;
        let next_start_after = page.last().map(|change| change.change_id);
        let page_is_full = page.len() == 1024;
        for change in page {
            push_unique_change(&mut changes, &mut seen, LixChangeRow::Direct(change))?;
        }
        if !page_is_full {
            break;
        }
        start_after = next_start_after;
    }
    let mut start_after = None;
    loop {
        let page = reader.scan_commit_records(start_after, 1024).await?;
        let next_start_after = page.last().map(|record| record.commit_id);
        let page_is_full = page.len() == 1024;
        for record in page {
            push_unique_change(
                &mut changes,
                &mut seen,
                LixChangeRow::DerivedCommit(crate::commit_graph::canonical_commit_change(&record)),
            )?;
        }
        if !page_is_full {
            break;
        }
        start_after = next_start_after;
    }
    changes.sort_by_key(LixChangeRow::change_id);
    if let Some(limit) = limit {
        changes.truncate(limit);
    }
    Ok(changes)
}

fn push_unique_change(
    changes: &mut Vec<LixChangeRow>,
    seen: &mut BTreeSet<ChangeId>,
    change: LixChangeRow,
) -> Result<(), LixError> {
    let change_id = change.change_id();
    if !seen.insert(change_id) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("ChangeCatalog enumeration returned duplicate change '{change_id}'"),
        ));
    }
    changes.push(change);
    Ok(())
}

#[cfg(test)]
fn require_commit_records(
    expected_commit_ids: &[CommitId],
    records: Vec<Option<crate::changelog::CommitRecord>>,
) -> Result<Vec<crate::changelog::CommitRecord>, LixError> {
    if records.len() != expected_commit_ids.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "CommitCatalog enumeration returned {} records for {} requested commits",
                records.len(),
                expected_commit_ids.len()
            ),
        ));
    }
    expected_commit_ids
        .iter()
        .copied()
        .zip(records)
        .map(|(expected_commit_id, record)| {
            let record = record.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "CommitCatalog enumeration omitted authenticated commit '{expected_commit_id}'"
                    ),
                )
            })?;
            if record.commit_id != expected_commit_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "CommitCatalog enumeration returned commit '{}' for requested '{}'",
                        record.commit_id, expected_commit_id
                    ),
                ));
            }
            Ok(record)
        })
        .collect()
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
    reader: &ForkTreeReadFacade<S>,
    change_id: ChangeId,
) -> Result<Option<LixChangeRow>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    if let Some(change) = reader
        .load_change_records(std::slice::from_ref(&change_id))
        .await?
        .into_iter()
        .next()
        .flatten()
    {
        return Ok(Some(LixChangeRow::Direct(change)));
    }

    let mut start_after = None;
    loop {
        let page = reader.scan_commit_records(start_after, 1024).await?;
        let next_start_after = page.last().map(|record| record.commit_id);
        let page_is_full = page.len() == 1024;
        if let Some(record) = page
            .into_iter()
            .find(|record| crate::commit_graph::canonical_commit_change(record).id == change_id)
        {
            return Ok(Some(LixChangeRow::DerivedCommit(
                crate::commit_graph::canonical_commit_change(&record),
            )));
        }
        if !page_is_full {
            break;
        }
        start_after = next_start_after;
    }
    Ok(None)
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
        json_field("row_pk", false),
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
            "row_pk",
            Col::Utf8Owned(|row| {
                Some(
                    row.row_pk
                        .as_json_array_text()
                        .expect("canonical change row primary key should project"),
                )
            }),
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

    use super::{
        ChangeScanRoute, change_payload_projection, change_scan_route, lix_change_schema,
        require_commit_records,
    };

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

    #[test]
    fn commit_catalog_missing_or_reordered_records_fail_closed() {
        let first_id = crate::changelog::CommitId::for_test_label("catalog-first");
        let second_id = crate::changelog::CommitId::for_test_label("catalog-second");
        let record = |commit_id| crate::changelog::CommitRecord {
            format_version: 2,
            commit_id,
            generation: 0,
            parent_commit_ids: Vec::new(),
            change_id: crate::changelog::ChangeId::for_test_label("catalog-change"),
            account_id: "test".to_owned(),
            created_at: crate::common::LixTimestamp::expect_parse(
                "catalog test timestamp",
                "2026-05-12T00:00:00Z",
            ),
        };
        let expected = [first_id, second_id];

        assert!(require_commit_records(&expected, vec![Some(record(first_id)), None]).is_err());
        assert!(
            require_commit_records(
                &expected,
                vec![Some(record(first_id)), Some(record(first_id))],
            )
            .is_err()
        );
        assert!(
            require_commit_records(
                &expected,
                vec![Some(record(second_id)), Some(record(first_id))],
            )
            .is_err()
        );
    }
}
