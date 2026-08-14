use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::prelude::SessionContext;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::LixError;
use crate::commit_graph::CommitGraphReader;
use crate::serialize_row_metadata;
use crate::sql2::change_materialization::MaterializedChange;

use crate::sql2::SqlHistoryQuerySource;
use crate::sql2::WriteAccess;
use crate::sql2::catalog::{
    SchemaColumnType, SchemaSurfaceShape, SchemaSurfaceSpec, schema_surface_schema,
};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::history_projection::{HistoryIdentityProjection, tombstone_identity_column_value};
use crate::sql2::history_route::{
    HISTORY_COL_AS_OF_COMMIT_ID, HISTORY_COL_CHANGE_CREATED_AT, HISTORY_COL_IS_DELETED,
    HistoryMetadataProjection, HistoryRoute, HistoryViewDescriptor, load_history_entries,
    parse_history_filter, validate_history_anchor_filter,
};
use crate::sql2::providers::schema::{
    parse_snapshot, row_f64_value, row_i64_value, row_json_text_value,
};
use crate::storage_adapter::StorageAdapterRead;

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::schema::{RowPrimaryKeyFilterAnalyzer, row_pks_from_primary_key_filters};
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, scan_row_source};

pub(super) fn register_row_history_surface<S>(
    session: &SessionContext,
    surface_name: &str,
    spec: Arc<SchemaSurfaceSpec>,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    register_spec_table(
        session,
        surface_name,
        Arc::new(SchemaHistorySpec {
            surface_name: surface_name.to_string(),
            schema: schema_surface_schema(&spec, SchemaSurfaceShape::History),
            spec,
            commit_graph,
            query_source,
        }),
        WriteAccess::read_only(),
    )
}

/// Schema-specific history surface backed directly by the commit graph.
///
/// The spec uses the commit graph primitive directly, then shapes canonical
/// changes into the typed row columns for one registered schema.
struct SchemaHistorySpec<S> {
    surface_name: String,
    spec: Arc<SchemaSurfaceSpec>,
    schema: SchemaRef,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
}

#[async_trait]
impl<S> TableSpec for SchemaHistorySpec<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn table_name(&self) -> &str {
        &self.surface_name
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn history_anchor_column(&self) -> Option<&'static str> {
        Some(HISTORY_COL_AS_OF_COMMIT_ID)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        let identity_analyzer = RowPrimaryKeyFilterAnalyzer::new(&self.spec);
        if parse_history_filter(filter).is_some() || identity_analyzer.supports(filter) {
            TableProviderFilterPushDown::Exact
        } else if identity_analyzer.contains_routable_conjunct(filter) {
            // Keep DataFusion's residual evaluation for mixed predicates while
            // still receiving exact identity conjuncts for commit-graph routing.
            TableProviderFilterPushDown::Inexact
        } else {
            TableProviderFilterPushDown::Unsupported
        }
    }

    fn validate_filter_pushdown(&self, filter: &Expr) -> Result<()> {
        validate_history_anchor_filter(filter).map_err(lix_error_to_datafusion_error)
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let mut route = row_history_route_from_filters(&self.spec, filters)?;
        route.default_to_as_of_commit_id(&self.query_source.default_as_of_commit_id);
        let schema = projected_schema(&self.schema, projection);
        let metadata_projection = HistoryMetadataProjection::from_scan(&schema, filters);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&schema),
                (
                    Arc::clone(&self.spec),
                    Arc::clone(&self.commit_graph),
                    self.query_source.clone(),
                    route,
                    schema,
                    metadata_projection,
                ),
                move |(spec, commit_graph, query_source, route, schema, metadata_projection)| async move {
                    let rows = load_row_history_rows(
                        &spec,
                        commit_graph,
                        query_source,
                        &route,
                        limit,
                        metadata_projection,
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                    row_history_record_batch(&schema, &spec, &rows)
                },
            ),
        })
    }
}

fn row_history_route_from_filters(
    spec: &SchemaSurfaceSpec,
    filters: &[Expr],
) -> Result<HistoryRoute> {
    let mut route = HistoryRoute::from_filters(filters);
    if let Some(row_pks) = row_pks_from_primary_key_filters(spec, filters)? {
        let surface_row_pks = row_pks
            .iter()
            .map(crate::row_pk::RowPk::as_json_array_text)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(lix_error_to_datafusion_error)?;
        route.constrain_row_pks(surface_row_pks);
        if !route.is_contradictory() {
            route.set_resolved_row_pks(row_pks);
        }
    }
    Ok(route)
}

#[derive(Debug, Clone)]
struct SchemaHistoryRow {
    change: MaterializedChange,
    observed_commit_id: String,
    commit_created_at: Option<String>,
    as_of_commit_id: String,
    depth: u32,
}

async fn load_row_history_rows<S>(
    spec: &SchemaSurfaceSpec,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    route: &HistoryRoute,
    limit: Option<usize>,
    metadata_projection: HistoryMetadataProjection,
) -> Result<Vec<SchemaHistoryRow>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let history_view_name = format!("{}_history", spec.schema_key);
    let entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: history_view_name.as_str(),
            as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
        },
        commit_graph,
        query_source,
        route,
        vec![spec.schema_key.clone()],
        metadata_projection,
        limit,
    )
    .await?;
    let mut rows = entries
        .into_iter()
        .map(|entry| SchemaHistoryRow {
            change: entry.change,
            observed_commit_id: entry.observed_commit_id,
            commit_created_at: entry.commit_created_at,
            as_of_commit_id: entry.as_of_commit_id,
            depth: entry.depth,
        })
        .collect::<Vec<_>>();
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

/// The `lixcol_*` system-column tail every schema history surface shares.
/// The row-payload columns are spec-dependent (typed per registered
/// schema), so they stay in [`row_history_column_array`]; only the fixed
/// system columns live in the static table.
static ROW_HISTORY_SYSTEM_COLS: ColumnTable<SchemaHistoryRow> = ColumnTable {
    columns: &[
        (
            "lixcol_row_pk",
            Col::Utf8Owned(|row| {
                Some(
                    row.change
                        .row_pk
                        .as_json_array_text()
                        .expect("canonical change row primary key should project"),
                )
            }),
        ),
        (
            "lixcol_schema_key",
            Col::Utf8(|row| Some(row.change.schema_key.as_str())),
        ),
        (
            "lixcol_file_id",
            Col::Utf8(|row| row.change.file_id.as_deref()),
        ),
        (
            "lixcol_metadata",
            Col::Utf8Owned(|row| row.change.metadata.as_deref().map(serialize_row_metadata)),
        ),
        (
            "lixcol_change_id",
            Col::Utf8(|row| Some(row.change.id.as_str())),
        ),
        (
            HISTORY_COL_CHANGE_CREATED_AT,
            Col::Utf8(|row| Some(row.change.created_at.as_str())),
        ),
        (
            "lixcol_origin_key",
            Col::Utf8(|row| row.change.origin_key.as_deref()),
        ),
        (
            "lixcol_observed_commit_id",
            Col::Utf8(|row| Some(row.observed_commit_id.as_str())),
        ),
        (
            "lixcol_commit_created_at",
            Col::Utf8(|row| row.commit_created_at.as_deref()),
        ),
        (
            HISTORY_COL_AS_OF_COMMIT_ID,
            Col::Utf8(|row| Some(row.as_of_commit_id.as_str())),
        ),
        ("lixcol_depth", Col::I64(|row| Some(i64::from(row.depth)))),
        (
            HISTORY_COL_IS_DELETED,
            Col::Bool(|row| Some(row.change.snapshot_content.is_none())),
        ),
    ],
};

fn row_history_batch_error(error: ColumnTableError) -> DataFusionError {
    match error {
        ColumnTableError::UnsupportedColumn(column) => DataFusionError::Execution(format!(
            "sql2 row history provider does not support system column '{column}'"
        )),
        ColumnTableError::Arrow(error) | ColumnTableError::ArrowZeroColumn(error) => {
            DataFusionError::from(error)
        }
        ColumnTableError::Row(error) => lix_error_to_datafusion_error(error),
    }
}

fn row_history_record_batch(
    schema: &SchemaRef,
    spec: &SchemaSurfaceSpec,
    rows: &[SchemaHistoryRow],
) -> Result<RecordBatch> {
    // Parse each authenticated history payload once for the complete Arrow
    // projection. The former per-column path reparsed the same whole-row JSON
    // for every projected field.
    let snapshots = rows
        .iter()
        .map(|row| parse_snapshot(row.change.snapshot_content.as_deref()))
        .collect::<Result<Vec<_>>>()?;
    let system_fields = schema
        .fields()
        .iter()
        .filter(|field| field.name().starts_with("lixcol_"))
        .cloned()
        .collect::<Vec<_>>();
    let system_batch = ROW_HISTORY_SYSTEM_COLS
        .build(Arc::new(Schema::new(system_fields)), rows)
        .map_err(row_history_batch_error)?;
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            system_batch.column_by_name(field.name()).map_or_else(
                || row_history_column_array(field.name(), spec, rows, &snapshots),
                |array| Ok(Arc::clone(array)),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )?)
}

#[expect(trivial_casts)]
fn row_history_column_array(
    column_name: &str,
    spec: &SchemaSurfaceSpec,
    rows: &[SchemaHistoryRow],
    snapshots: &[Option<JsonValue>],
) -> Result<ArrayRef> {
    let column_type = spec
        .visible_column(column_name)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "sql2 row history provider '{}' does not expose column '{}'",
                spec.schema_key, column_name
            ))
        })?
        .column_type;
    let projected_values = rows
        .iter()
        .zip(snapshots)
        .map(|(row, snapshot)| row_history_column_value(row, snapshot.as_ref(), spec, column_name))
        .collect::<Result<Vec<_>>>()?;

    Ok(match column_type {
        SchemaColumnType::String | SchemaColumnType::Json => Arc::new(StringArray::from(
            projected_values
                .iter()
                .map(|snapshot| row_json_text_value(snapshot.as_deref(), column_type))
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        SchemaColumnType::Integer => Arc::new(Int64Array::from(
            projected_values
                .iter()
                .map(|snapshot| row_i64_value(snapshot.as_deref(), &spec.schema_key, column_name))
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        SchemaColumnType::Number => Arc::new(Float64Array::from(
            projected_values
                .iter()
                .map(|snapshot| row_f64_value(snapshot.as_deref(), &spec.schema_key, column_name))
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        SchemaColumnType::Boolean => Arc::new(BooleanArray::from(
            projected_values
                .iter()
                .map(|snapshot| snapshot.as_deref().and_then(JsonValue::as_bool))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        SchemaColumnType::Timestamptz => Arc::new(
            TimestampMicrosecondArray::from(
                projected_values
                    .iter()
                    .map(|snapshot| {
                        let Some(value) = snapshot.as_deref() else {
                            return Ok(None);
                        };
                        if value.is_null() {
                            return Ok(None);
                        }
                        let text = value.as_str().ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "{}.{} expected timestamptz text",
                                spec.schema_key, column_name
                            ))
                        })?;
                        chrono::DateTime::parse_from_rfc3339(text)
                            .map(|timestamp| Some(timestamp.timestamp_micros()))
                            .map_err(|error| {
                                DataFusionError::Execution(format!(
                                    "{}.{} contains invalid timestamptz: {error}",
                                    spec.schema_key, column_name
                                ))
                            })
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
            .with_timezone("UTC"),
        ) as ArrayRef,
    })
}

fn row_history_column_value<'a>(
    row: &SchemaHistoryRow,
    snapshot: Option<&'a JsonValue>,
    spec: &SchemaSurfaceSpec,
    column_name: &str,
) -> Result<Option<Cow<'a, JsonValue>>> {
    if let Some(snapshot) = snapshot {
        return Ok(snapshot.get(column_name).map(Cow::Borrowed));
    }

    let row_pk = row.change.row_pk.as_json_array_text().map_err(|error| {
        DataFusionError::Execution(format!(
            "sql2 row history provider failed to project row pk: {error}"
        ))
    })?;
    tombstone_identity_column_value(
        column_name,
        &row_pk,
        HistoryIdentityProjection::PrimaryKeyPaths(&spec.primary_key_paths),
    )
    .map(|value| value.map(Cow::Owned))
    .map_err(|error| DataFusionError::Execution(error.to_string()))
}

#[cfg(test)]
mod tests {
    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
    use serde_json::json;

    use crate::sql2::catalog::derive_schema_surface_spec_from_schema;

    use super::row_history_route_from_filters;

    #[test]
    fn public_composite_key_filters_route_in_schema_order() {
        let spec = derive_schema_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "localized_message",
            "columns": [
                { "name": "key", "type": "text", "nullable": false },
                { "name": "locale", "type": "text", "nullable": false },
                { "name": "body", "type": "text", "nullable": false },
            ],
            "primary_key": ["locale", "key"],
        }))
        .expect("schema should derive");
        let filters = vec![
            eq("lixcol_as_of_commit_id", "commit-head"),
            // Deliberately reverse the declared primary-key path order.
            eq("key", "welcome"),
            eq("locale", "en"),
        ];

        let route =
            row_history_route_from_filters(&spec, &filters).expect("history route should derive");

        assert_eq!(route.as_of_commit_ids, vec!["commit-head"]);
        assert_eq!(route.row_pks, vec![r#"["en","welcome"]"#]);
        assert!(!route.is_contradictory());
    }

    #[test]
    fn public_and_opaque_identity_filters_intersect() {
        let spec = derive_schema_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "localized_message",
            "columns": [
                { "name": "key", "type": "text", "nullable": false },
                { "name": "locale", "type": "text", "nullable": false },
            ],
            "primary_key": ["locale", "key"],
        }))
        .expect("schema should derive");
        let filters = vec![
            eq("key", "welcome"),
            eq("locale", "en"),
            eq("lixcol_row_pk", r#"["fr","welcome"]"#),
        ];

        let route =
            row_history_route_from_filters(&spec, &filters).expect("history route should derive");

        assert!(route.is_contradictory());

        let wrong_arity_route = row_history_route_from_filters(
            &spec,
            &[
                eq("key", "welcome"),
                eq("locale", "en"),
                eq("lixcol_row_pk", r#"["en"]"#),
            ],
        )
        .expect("wrong-arity identity should produce a route");
        assert!(wrong_arity_route.is_contradictory());
    }

    fn eq(column: &str, value: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name(column))),
            Operator::Eq,
            Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(value.to_string())),
                None,
            )),
        ))
    }
}
