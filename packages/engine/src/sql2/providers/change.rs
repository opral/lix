use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};

use crate::LixError;
use crate::changelog::{
    ChangeId, ChangeLoadRequest, ChangeRecord, ChangeScanRequest, ChangelogContext,
    ChangelogReader, CommitLoadEntry, CommitProjection, CommitScanRequest,
};
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
use super::file::{FileIdConstraint, exact_string_column_constraint_from_filters};
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, row_source};

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

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if change_filter_has_exact_string_conjunct(filter, "id")
            || change_filter_has_exact_string_conjunct(filter, "file_id")
        {
            // Keep the residual filter: either constraint alone still needs the
            // complete direct-plus-derived semantics. Together they unlock the
            // direct point read in `plan_scan`.
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
        let pushed_limit = if filters.is_empty() { limit } else { None };
        let schema = projected_schema(&lix_change_schema(), projection);
        let payload_projection = change_payload_projection(schema.as_ref(), filters);
        let point_lookup = change_point_lookup_from_filters(filters)?;
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            load: row_source(
                (self.query_source.clone(), schema, point_lookup),
                move |(query_source, schema, point_lookup)| async move {
                    let mut json_reader = query_source.json_reader;
                    let canonical_changes = scan_changelog_changes(
                        query_source.store,
                        pushed_limit,
                        point_lookup.as_ref(),
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                    let mut changes = Vec::with_capacity(canonical_changes.len());
                    for change in canonical_changes {
                        match change {
                            LixChangeRow::Direct(change) => changes.push(
                                materialize_changelog_change_record(
                                    &mut json_reader,
                                    change,
                                    payload_projection,
                                )
                                .await
                                .map_err(lix_error_to_datafusion_error)?,
                            ),
                            LixChangeRow::DerivedCommit(change) => changes.push(
                                materialize_commit_graph_change(
                                    &mut json_reader,
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

/// An exact direct-change lookup is safe only when an exact `file_id` predicate
/// excludes the derived `lix_commit` rows that share the `lix_change` surface.
/// Other filter shapes retain the complete direct-plus-derived scan below.
#[derive(Clone)]
struct ChangePointLookup {
    change_ids: Vec<ChangeId>,
    file_ids: BTreeSet<String>,
}

fn change_point_lookup_from_filters(filters: &[Expr]) -> Result<Option<ChangePointLookup>> {
    let conjuncts = change_filter_conjuncts(filters);
    let FileIdConstraint::Ids(change_ids) =
        exact_string_column_constraint_from_filters(&conjuncts, "id")?
    else {
        return Ok(None);
    };
    let FileIdConstraint::Ids(file_ids) =
        exact_string_column_constraint_from_filters(&conjuncts, "file_id")?
    else {
        return Ok(None);
    };
    Ok(Some(ChangePointLookup {
        // Invalid SQL text cannot name a persisted UUID key, so it contributes
        // no direct record while preserving the empty result of the full scan.
        change_ids: change_ids
            .iter()
            .filter_map(|change_id| ChangeId::parse(change_id).ok())
            .collect(),
        file_ids,
    }))
}

fn change_filter_has_exact_string_conjunct(filter: &Expr, column_name: &'static str) -> bool {
    let mut conjuncts = Vec::new();
    collect_and_conjuncts(filter, &mut conjuncts);
    conjuncts.into_iter().any(|conjunct| {
        matches!(
            exact_string_column_constraint_from_filters(&[conjunct], column_name),
            Ok(FileIdConstraint::Ids(_))
        )
    })
}

fn change_filter_conjuncts(filters: &[Expr]) -> Vec<Expr> {
    let mut conjuncts = Vec::new();
    for filter in filters {
        collect_and_conjuncts(filter, &mut conjuncts);
    }
    conjuncts
}

fn collect_and_conjuncts(filter: &Expr, conjuncts: &mut Vec<Expr>) {
    if let Expr::BinaryExpr(binary_expr) = filter
        && binary_expr.op == Operator::And
    {
        collect_and_conjuncts(&binary_expr.left, conjuncts);
        collect_and_conjuncts(&binary_expr.right, conjuncts);
    } else {
        conjuncts.push(filter.clone());
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
    point_lookup: Option<&ChangePointLookup>,
) -> Result<Vec<LixChangeRow>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let mut reader = ChangelogContext::new().reader(store);
    if let Some(point_lookup) = point_lookup {
        let loaded = reader
            .load_changes(ChangeLoadRequest {
                change_ids: &point_lookup.change_ids,
            })
            .await?;
        let mut changes = loaded
            .entries
            .into_iter()
            .flatten()
            .filter(|change| {
                change
                    .file_id
                    .as_deref()
                    .is_some_and(|file_id| point_lookup.file_ids.contains(file_id))
            })
            .map(LixChangeRow::Direct)
            .collect::<Vec<_>>();
        changes.sort_by_key(LixChangeRow::change_id);
        return Ok(changes);
    }
    let mut changes = Vec::<LixChangeRow>::new();
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
    let mut start_after = None::<String>;
    loop {
        let scan = reader
            .scan_commits(CommitScanRequest {
                start_after: start_after.as_deref(),
                limit: Some(1024),
                projection: CommitProjection::Record,
            })
            .await?;
        for entry in scan.entries {
            let CommitLoadEntry::Record(commit) = entry else {
                continue;
            };
            changes.push(LixChangeRow::DerivedCommit(commit_record_canonical_change(
                &commit,
            )));
        }
        let Some(next) = scan.next_start_after else {
            break;
        };
        start_after = Some(next.to_string());
    }
    changes.sort_by_key(LixChangeRow::change_id);
    if let Some(limit) = limit {
        changes.truncate(limit);
    }
    Ok(changes)
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

fn commit_record_canonical_change(
    commit: &crate::changelog::CommitRecord,
) -> crate::commit_graph::CommitGraphChange {
    let snapshot_content =
        crate::changelog::commit_row_snapshot_json(&commit.commit_id.to_string())
            .expect("lix_commit snapshot serialization should not fail");
    crate::commit_graph::CommitGraphChange {
        id: commit.change_id,
        entity_pk: crate::entity_pk::EntityPk::single(commit.commit_id),
        schema_key: "lix_commit".to_string(),
        file_id: None,
        snapshot: crate::json_store::JsonSlot::from_json(&snapshot_content),
        metadata: crate::json_store::JsonSlot::None,
        created_at: commit.created_at,
        origin_key: None,
    }
}

pub(super) fn lix_change_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        json_field("entity_pk", false),
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
        (
            "entity_pk",
            Col::Utf8Owned(|row| {
                Some(
                    row.entity_pk
                        .as_json_array_text()
                        .expect("canonical change entity primary key should project"),
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
    use std::collections::BTreeSet;

    use datafusion::arrow::datatypes::Schema;
    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator, col};

    use crate::changelog::ChangeId;

    use super::{
        change_filter_has_exact_string_conjunct, change_payload_projection,
        change_point_lookup_from_filters, lix_change_schema,
    };

    fn equals(column_name: &str, value: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name(column_name))),
            Operator::Eq,
            Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(value.to_string())),
                None,
            )),
        ))
    }

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
    fn exact_change_and_file_id_filters_use_direct_lookup() {
        let change_id = "019f7d6c-fb53-7423-9a32-a80ec52b128b";
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(equals("id", change_id)),
            Operator::And,
            Box::new(equals("file_id", "active-markdown")),
        ));
        let lookup = change_point_lookup_from_filters(&[filter.clone()])
            .unwrap()
            .expect("the exact direct change shape should route");

        assert_eq!(lookup.change_ids, vec![ChangeId::parse(change_id).unwrap()]);
        assert_eq!(
            lookup.file_ids,
            BTreeSet::from(["active-markdown".to_string()])
        );
        assert!(change_filter_has_exact_string_conjunct(&filter, "id"));
        assert!(change_filter_has_exact_string_conjunct(&filter, "file_id"));
    }

    #[test]
    fn change_lookup_requires_exact_file_id_and_skips_invalid_change_ids() {
        assert!(
            change_point_lookup_from_filters(&[equals(
                "id",
                "019f7d6c-fb53-7423-9a32-a80ec52b128b"
            )])
            .unwrap()
            .is_none()
        );

        let invalid = change_point_lookup_from_filters(&[
            equals("id", "not-a-change-id"),
            equals("file_id", "active-markdown"),
        ])
        .unwrap()
        .expect("the invalid id still has the safe direct-only shape");
        assert!(invalid.change_ids.is_empty());
    }
}
