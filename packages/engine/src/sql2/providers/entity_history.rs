use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
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
    EntityColumnType, EntitySurfaceShape, EntitySurfaceSpec, entity_surface_schema,
};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::history_projection::{HistoryIdentityProjection, tombstone_identity_column_value};
use crate::sql2::history_route::{
    HISTORY_COL_START_COMMIT_ID, HistoryColumnStyle, HistoryRoute, HistoryViewDescriptor,
    load_history_entries, parse_history_filter,
};
use crate::sql2::providers::entity::{
    entity_f64_value, entity_i64_value, entity_json_text_value, parse_snapshot,
};
use crate::storage::StorageRead;

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, row_source};

pub(super) fn register_entity_history_surface<S>(
    session: &SessionContext,
    surface_name: &str,
    spec: Arc<EntitySurfaceSpec>,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
) -> Result<(), LixError>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    register_spec_table(
        session,
        surface_name,
        Arc::new(EntityHistorySpec {
            surface_name: surface_name.to_string(),
            schema: entity_surface_schema(&spec, EntitySurfaceShape::History),
            spec,
            commit_graph,
            query_source,
        }),
        WriteAccess::read_only(),
    )
}

/// Schema-specific history surface backed directly by the commit graph.
///
/// The spec does not query `lix_state_history` through SQL. It uses the same
/// commit graph primitive as the generic history surface, then shapes canonical
/// changes into the typed entity columns for one registered schema.
struct EntityHistorySpec<S> {
    surface_name: String,
    spec: Arc<EntitySurfaceSpec>,
    schema: SchemaRef,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
}

#[async_trait]
impl<S> TableSpec for EntityHistorySpec<S>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    fn table_name(&self) -> &str {
        &self.surface_name
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if parse_history_filter(filter, HistoryColumnStyle::Prefixed).is_some() {
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
        let route = HistoryRoute::from_filters(filters, HistoryColumnStyle::Prefixed);
        let schema = projected_schema(&self.schema, projection);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            load: row_source(
                (
                    Arc::clone(&self.spec),
                    Arc::clone(&self.commit_graph),
                    self.query_source.clone(),
                    route,
                    schema,
                ),
                move |(spec, commit_graph, query_source, route, schema)| async move {
                    let rows =
                        load_entity_history_rows(&spec, commit_graph, query_source, &route, limit)
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                    entity_history_record_batch(&schema, &spec, &rows)
                },
            ),
        })
    }
}

#[derive(Debug, Clone)]
struct EntityHistoryRow {
    change: MaterializedChange,
    observed_commit_id: String,
    commit_created_at: String,
    start_commit_id: String,
    depth: u32,
}

async fn load_entity_history_rows<S>(
    spec: &EntitySurfaceSpec,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    route: &HistoryRoute,
    limit: Option<usize>,
) -> Result<Vec<EntityHistoryRow>, LixError>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    let history_view_name = format!("{}_history", spec.schema_key);
    let entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: history_view_name.as_str(),
            start_commit_column: HISTORY_COL_START_COMMIT_ID,
        },
        commit_graph,
        query_source.json_reader,
        route,
        vec![spec.schema_key.clone()],
    )
    .await?;
    let mut rows = entries
        .into_iter()
        .map(|entry| EntityHistoryRow {
            change: entry.change,
            observed_commit_id: entry.observed_commit_id,
            commit_created_at: entry.commit_created_at,
            start_commit_id: entry.start_commit_id,
            depth: entry.depth,
        })
        .collect::<Vec<_>>();
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

/// The `lixcol_*` system-column tail every entity history surface shares.
/// The entity-payload columns are spec-dependent (typed per registered
/// schema), so they stay in [`entity_history_column_array`]; only the fixed
/// system columns live in the static table.
static ENTITY_HISTORY_SYSTEM_COLS: ColumnTable<EntityHistoryRow> = ColumnTable {
    columns: &[
        (
            "lixcol_entity_pk",
            Col::Utf8Owned(|row| {
                Some(
                    row.change
                        .entity_pk
                        .as_json_array_text()
                        .expect("canonical change entity primary key should project"),
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
            "lixcol_snapshot_content",
            Col::Utf8(|row| row.change.snapshot_content.as_deref()),
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
            "lixcol_observed_commit_id",
            Col::Utf8(|row| Some(row.observed_commit_id.as_str())),
        ),
        (
            "lixcol_commit_created_at",
            Col::Utf8(|row| Some(row.commit_created_at.as_str())),
        ),
        (
            "lixcol_start_commit_id",
            Col::Utf8(|row| Some(row.start_commit_id.as_str())),
        ),
        ("lixcol_depth", Col::I64(|row| Some(i64::from(row.depth)))),
    ],
};

fn entity_history_batch_error(error: ColumnTableError) -> DataFusionError {
    match error {
        ColumnTableError::UnsupportedColumn(column) => DataFusionError::Execution(format!(
            "sql2 entity history provider does not support system column '{column}'"
        )),
        ColumnTableError::Arrow(error) | ColumnTableError::ArrowZeroColumn(error) => {
            DataFusionError::from(error)
        }
        ColumnTableError::Row(error) => lix_error_to_datafusion_error(error),
    }
}

fn entity_history_record_batch(
    schema: &SchemaRef,
    spec: &EntitySurfaceSpec,
    rows: &[EntityHistoryRow],
) -> Result<RecordBatch> {
    let system_fields = schema
        .fields()
        .iter()
        .filter(|field| field.name().starts_with("lixcol_"))
        .cloned()
        .collect::<Vec<_>>();
    let system_batch = ENTITY_HISTORY_SYSTEM_COLS
        .build(Arc::new(Schema::new(system_fields)), rows)
        .map_err(entity_history_batch_error)?;
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            system_batch.column_by_name(field.name()).map_or_else(
                || entity_history_column_array(field.name(), spec, rows),
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
fn entity_history_column_array(
    column_name: &str,
    spec: &EntitySurfaceSpec,
    rows: &[EntityHistoryRow],
) -> Result<ArrayRef> {
    let column_type = spec
        .visible_column(column_name)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "sql2 entity history provider '{}' does not expose column '{}'",
                spec.schema_key, column_name
            ))
        })?
        .column_type;
    let projected_values = rows
        .iter()
        .map(|row| entity_history_column_value(row, spec, column_name))
        .collect::<Result<Vec<_>>>()?;

    Ok(match column_type {
        EntityColumnType::String | EntityColumnType::Json => Arc::new(StringArray::from(
            projected_values
                .iter()
                .map(|snapshot| entity_json_text_value(snapshot.as_ref(), column_type))
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        EntityColumnType::Integer => Arc::new(Int64Array::from(
            projected_values
                .iter()
                .map(|snapshot| entity_i64_value(snapshot.as_ref()))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        EntityColumnType::Number => Arc::new(Float64Array::from(
            projected_values
                .iter()
                .map(|snapshot| entity_f64_value(snapshot.as_ref()))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        EntityColumnType::Boolean => Arc::new(BooleanArray::from(
            projected_values
                .iter()
                .map(|snapshot| snapshot.as_ref().and_then(JsonValue::as_bool))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
    })
}

fn entity_history_column_value(
    row: &EntityHistoryRow,
    spec: &EntitySurfaceSpec,
    column_name: &str,
) -> Result<Option<JsonValue>> {
    let snapshot = parse_snapshot(row.change.snapshot_content.as_deref())?;
    if let Some(snapshot) = snapshot {
        return Ok(snapshot.get(column_name).cloned());
    }

    let entity_pk = row.change.entity_pk.as_json_array_text().map_err(|error| {
        DataFusionError::Execution(format!(
            "sql2 entity history provider failed to project entity pk: {error}"
        ))
    })?;
    tombstone_identity_column_value(
        column_name,
        &entity_pk,
        HistoryIdentityProjection::PrimaryKeyPaths(&spec.primary_key_paths),
    )
    .map_err(|error| DataFusionError::Execution(error.to_string()))
}
