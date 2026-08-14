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
    EntityColumnType, EntitySurfaceShape, EntitySurfaceSpec, entity_surface_schema,
};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::history_projection::{HistoryIdentityProjection, tombstone_identity_column_value};
use crate::sql2::history_route::{
    HISTORY_COL_AS_OF_COMMIT_ID, HISTORY_COL_CHANGE_CREATED_AT, HISTORY_COL_IS_DELETED,
    HistoryMetadataProjection, HistoryRoute, HistoryViewDescriptor, load_history_entries,
    parse_history_filter, validate_history_anchor_filter,
};
use crate::sql2::providers::entity::{
    entity_f64_value, entity_i64_value, entity_json_text_value,
};
use crate::storage_adapter::StorageAdapterRead;

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::entity::{EntityPrimaryKeyFilterAnalyzer, entity_pks_from_primary_key_filters};
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, scan_row_source};

pub(super) fn register_entity_history_surface<S>(
    session: &SessionContext,
    surface_name: &str,
    spec: Arc<EntitySurfaceSpec>,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
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
/// The spec uses the commit graph primitive directly, then shapes canonical
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
        let identity_analyzer = EntityPrimaryKeyFilterAnalyzer::new(&self.spec);
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
        let mut route = entity_history_route_from_filters(&self.spec, filters)?;
        route.typed_entity_payloads = true;
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
                    let rows = load_entity_history_rows(
                        &spec,
                        commit_graph,
                        query_source,
                        &route,
                        limit,
                        metadata_projection,
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                    entity_history_record_batch(&schema, &spec, &rows)
                },
            ),
        })
    }
}

fn entity_history_route_from_filters(
    spec: &EntitySurfaceSpec,
    filters: &[Expr],
) -> Result<HistoryRoute> {
    let mut route = HistoryRoute::from_filters(filters);
    if let Some(entity_pks) = entity_pks_from_primary_key_filters(spec, filters)? {
        let surface_entity_pks = entity_pks
            .iter()
            .map(crate::entity_pk::EntityPk::as_json_array_text)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(lix_error_to_datafusion_error)?;
        route.constrain_entity_pks(surface_entity_pks);
        if !route.is_contradictory() {
            route.set_resolved_entity_pks(entity_pks);
        }
    }
    Ok(route)
}

#[derive(Debug, Clone)]
struct EntityHistoryRow {
    change: MaterializedChange,
    observed_commit_id: String,
    commit_created_at: Option<String>,
    as_of_commit_id: String,
    depth: u32,
}

async fn load_entity_history_rows<S>(
    spec: &EntitySurfaceSpec,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    route: &HistoryRoute,
    limit: Option<usize>,
    metadata_projection: HistoryMetadataProjection,
) -> Result<Vec<EntityHistoryRow>, LixError>
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
        .map(|entry| EntityHistoryRow {
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
            Col::Bool(|row| {
                Some(
                    row.change
                        .typed_snapshot
                        .as_ref()
                        .is_some_and(|snapshot| snapshot.deleted)
                        || (row.change.snapshot_content.is_none()
                            && row.change.typed_snapshot.is_none()),
                )
            }),
        ),
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
    for row in rows {
        validate_typed_history_row(row, spec)?;
    }
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

fn validate_typed_history_row(row: &EntityHistoryRow, spec: &EntitySurfaceSpec) -> Result<()> {
    let Some(snapshot) = row.change.typed_snapshot.as_ref() else {
        if row.change.snapshot_content.is_some() {
            return Err(DataFusionError::Execution(format!(
                "{} registered entity history row omitted its authenticated typed payload",
                spec.schema_key
            )));
        }
        return Ok(());
    };
    if snapshot.schema_layout_fingerprint != spec.columnar_layout_fingerprint() {
        return Err(DataFusionError::Execution(format!(
            "{} typed history schema fingerprint mismatch",
            spec.schema_key
        )));
    }
    if snapshot.deleted {
        if !snapshot.fields.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "{} typed history tombstone contains payload fields",
                spec.schema_key
            )));
        }
        return Ok(());
    }
    let primary_key_roots = spec
        .primary_key_paths
        .iter()
        .filter_map(|path| path.first().map(String::as_str))
        .collect::<std::collections::BTreeSet<_>>();
    let expected = spec
        .columns
        .iter()
        .filter(|column| !primary_key_roots.contains(column.name.as_str()))
        .map(|column| (column.name.as_str(), column))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut seen = std::collections::BTreeSet::new();
    for field in &snapshot.fields {
        let Some(column) = expected.get(field.name.as_str()) else {
            return Err(DataFusionError::Execution(format!(
                "{}.{} typed history payload contains an undeclared or duplicated identity field",
                spec.schema_key, field.name
            )));
        };
        if !seen.insert(field.name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "{}.{} typed history payload contains a duplicate field",
                spec.schema_key, field.name
            )));
        }
        if field.value.is_none() && !column.read_nullable {
            return Err(DataFusionError::Execution(format!(
                "{}.{} typed history payload contains NULL for a non-null column",
                spec.schema_key, field.name
            )));
        }
        let type_matches = match (&field.value, column.column_type) {
            (None, _) => true,
            (Some(crate::changelog::TypedHistoryScalar::String(_)), EntityColumnType::String)
            | (Some(crate::changelog::TypedHistoryScalar::Jsonb(_)), EntityColumnType::Json)
            | (Some(crate::changelog::TypedHistoryScalar::Int64(_)), EntityColumnType::Integer)
            | (
                Some(crate::changelog::TypedHistoryScalar::Float64Bits(_)),
                EntityColumnType::Number,
            )
            | (
                Some(crate::changelog::TypedHistoryScalar::Boolean(_)),
                EntityColumnType::Boolean,
            )
            | (
                Some(crate::changelog::TypedHistoryScalar::TimestampMicros(_)),
                EntityColumnType::Timestamptz,
            ) => true,
            _ => false,
        };
        if !type_matches {
            return Err(DataFusionError::Execution(format!(
                "{}.{} has the wrong typed history scalar",
                spec.schema_key, field.name
            )));
        }
        if let Some(crate::changelog::TypedHistoryScalar::Jsonb(value)) = &field.value {
            serde_json::from_str::<serde_json::Value>(value).map_err(|error| {
                DataFusionError::Execution(format!(
                    "{}.{} typed history JSONB is malformed: {error}",
                    spec.schema_key, field.name
                ))
            })?;
        }
    }
    if seen.len() != expected.len() {
        return Err(DataFusionError::Execution(format!(
            "{} typed history payload omitted a declared field",
            spec.schema_key
        )));
    }
    Ok(())
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
    let typed_values = rows
        .iter()
        .map(|row| typed_history_column_value(row, spec, column_name))
        .collect::<Result<Vec<_>>>()?;

    Ok(match column_type {
        EntityColumnType::String | EntityColumnType::Json => Arc::new(StringArray::from(
            typed_values
                .iter()
                .map(|value| match value {
                    ProjectedHistoryValue::Typed(Some(
                        crate::changelog::TypedHistoryScalar::String(value)
                        | crate::changelog::TypedHistoryScalar::Jsonb(value),
                    )) => Ok(Some(value.clone())),
                    ProjectedHistoryValue::Typed(None) => Ok(None),
                    ProjectedHistoryValue::Identity(value) => {
                        entity_json_text_value(value.as_ref(), column_type)
                    }
                    ProjectedHistoryValue::Typed(Some(_)) => Err(DataFusionError::Execution(
                        format!("{}.{} has the wrong typed history scalar", spec.schema_key, column_name),
                    )),
                })
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        EntityColumnType::Integer => Arc::new(Int64Array::from(
            typed_values
                .iter()
                .map(|value| match value {
                    ProjectedHistoryValue::Typed(Some(crate::changelog::TypedHistoryScalar::Int64(value))) => Ok(Some(*value)),
                    ProjectedHistoryValue::Typed(None) => Ok(None),
                    ProjectedHistoryValue::Identity(value) => entity_i64_value(value.as_ref(), &spec.schema_key, column_name),
                    ProjectedHistoryValue::Typed(Some(_)) => Err(DataFusionError::Execution(format!("{}.{} has the wrong typed history scalar", spec.schema_key, column_name))),
                })
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        EntityColumnType::Number => Arc::new(Float64Array::from(
            typed_values
                .iter()
                .map(|value| match value {
                    ProjectedHistoryValue::Typed(Some(crate::changelog::TypedHistoryScalar::Float64Bits(value))) => Ok(Some(f64::from_bits(*value))),
                    ProjectedHistoryValue::Typed(None) => Ok(None),
                    ProjectedHistoryValue::Identity(value) => entity_f64_value(value.as_ref(), &spec.schema_key, column_name),
                    ProjectedHistoryValue::Typed(Some(_)) => Err(DataFusionError::Execution(format!("{}.{} has the wrong typed history scalar", spec.schema_key, column_name))),
                })
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        EntityColumnType::Boolean => Arc::new(BooleanArray::from(
            typed_values
                .iter()
                .map(|value| match value {
                    ProjectedHistoryValue::Typed(Some(crate::changelog::TypedHistoryScalar::Boolean(value))) => Ok(Some(*value)),
                    ProjectedHistoryValue::Typed(None) => Ok(None),
                    ProjectedHistoryValue::Identity(value) => Ok(value.as_ref().and_then(JsonValue::as_bool)),
                    ProjectedHistoryValue::Typed(Some(_)) => Err(DataFusionError::Execution(format!("{}.{} has the wrong typed history scalar", spec.schema_key, column_name))),
                })
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        EntityColumnType::Timestamptz => Arc::new(
            TimestampMicrosecondArray::from(
                typed_values
                    .iter()
                    .map(|projected| {
                        if let ProjectedHistoryValue::Typed(value) = projected {
                            return match value {
                                Some(crate::changelog::TypedHistoryScalar::TimestampMicros(value)) => Ok(Some(*value)),
                                None => Ok(None),
                                Some(_) => Err(DataFusionError::Execution(format!("{}.{} has the wrong typed history scalar", spec.schema_key, column_name))),
                            };
                        }
                        let ProjectedHistoryValue::Identity(snapshot) = projected else { unreachable!() };
                        let Some(value) = snapshot.as_ref() else {
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

#[derive(Debug)]
enum ProjectedHistoryValue<'a> {
    Typed(Option<&'a crate::changelog::TypedHistoryScalar>),
    Identity(Option<JsonValue>),
}

fn typed_history_column_value<'a>(
    row: &'a EntityHistoryRow,
    spec: &EntitySurfaceSpec,
    column_name: &str,
) -> Result<ProjectedHistoryValue<'a>> {
    let Some(snapshot) = row.change.typed_snapshot.as_ref() else {
        if row.change.snapshot_content.is_some() {
            return Err(DataFusionError::Execution(format!(
                "{}.{} registered entity history row omitted its authenticated typed payload",
                spec.schema_key, column_name
            )));
        }
        if spec
            .primary_key_paths
            .iter()
            .any(|path| path.first().is_some_and(|root| root == column_name))
        {
            return entity_history_identity_column_value(row, spec, column_name)
                .map(ProjectedHistoryValue::Identity);
        }
        return Ok(ProjectedHistoryValue::Typed(None));
    };
    if snapshot.schema_layout_fingerprint != spec.columnar_layout_fingerprint() {
        return Err(DataFusionError::Execution(format!(
            "{}.{} typed history schema fingerprint mismatch",
            spec.schema_key, column_name
        )));
    }
    if spec
        .primary_key_paths
        .iter()
        .any(|path| path.first().is_some_and(|root| root == column_name))
    {
        return entity_history_identity_column_value(row, spec, column_name)
            .map(ProjectedHistoryValue::Identity);
    }
    if snapshot.deleted {
        return Ok(ProjectedHistoryValue::Typed(None));
    }
    if let Some(field) = snapshot.fields.iter().find(|field| field.name == column_name) {
        return Ok(ProjectedHistoryValue::Typed(field.value.as_ref()));
    }
    Err(DataFusionError::Execution(format!(
        "{}.{} typed history payload omitted a declared field",
        spec.schema_key, column_name
    )))
}

fn entity_history_identity_column_value(
    row: &EntityHistoryRow,
    spec: &EntitySurfaceSpec,
    column_name: &str,
) -> Result<Option<JsonValue>> {
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

#[cfg(test)]
mod tests {
    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
    use serde_json::json;

    use crate::sql2::catalog::derive_entity_surface_spec_from_schema;

    use super::{
        EntityHistoryRow, ProjectedHistoryValue, entity_history_route_from_filters,
        typed_history_column_value,
    };

    fn seven_type_spec() -> crate::sql2::catalog::EntitySurfaceSpec {
        derive_entity_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "seven_type_history",
            "columns": [
                {"name":"id", "type":"uuid", "nullable":false},
                {"name":"label", "type":"text", "nullable":false},
                {"name":"count", "type":"int8", "nullable":false},
                {"name":"ratio", "type":"float8", "nullable":false},
                {"name":"active", "type":"boolean", "nullable":false},
                {"name":"metadata", "type":"jsonb", "nullable":false},
                {"name":"created_at", "type":"timestamptz", "nullable":false}
            ],
            "primary_key": ["id"]
        }))
        .expect("seven-type history schema should derive")
    }

    fn typed_history_row(
        fingerprint: String,
        fields: Vec<crate::changelog::TypedHistoryField>,
    ) -> EntityHistoryRow {
        EntityHistoryRow {
            change: crate::sql2::change_materialization::MaterializedChange {
                id: "change-1".to_owned(),
                account_id: "account-1".to_owned(),
                entity_pk: crate::entity_pk::EntityPk::from_json_array_value(&json!([
                    "01900000-0000-7000-8000-000000000001"
                ]))
                .expect("typed history identity"),
                schema_key: "seven_type_history".to_owned(),
                file_id: None,
                snapshot_content: None,
            typed_snapshot: Some(crate::changelog::TypedHistorySnapshot {
                schema_layout_fingerprint: fingerprint,
                deleted: false,
                primary_key_paths: vec![vec!["id".to_owned()]],
                fields,
            }),
                metadata: None,
                created_at: "2026-01-01T00:00:00Z".to_owned(),
                origin_key: None,
            },
            observed_commit_id: "commit-1".to_owned(),
            commit_created_at: Some("2026-01-01T00:00:00Z".to_owned()),
            as_of_commit_id: "commit-1".to_owned(),
            depth: 0,
        }
    }

    #[test]
    fn typed_history_projects_native_scalars_and_authenticated_identity_without_json() {
        use crate::changelog::{TypedHistoryField, TypedHistoryScalar};

        let spec = seven_type_spec();
        let row = typed_history_row(
            spec.columnar_layout_fingerprint(),
            vec![
                TypedHistoryField { name: "label".to_owned(), value: Some(TypedHistoryScalar::String("ready".to_owned())) },
                TypedHistoryField { name: "count".to_owned(), value: Some(TypedHistoryScalar::Int64(42)) },
                TypedHistoryField { name: "ratio".to_owned(), value: Some(TypedHistoryScalar::Float64Bits(1.5_f64.to_bits())) },
                TypedHistoryField { name: "active".to_owned(), value: Some(TypedHistoryScalar::Boolean(true)) },
                TypedHistoryField { name: "metadata".to_owned(), value: Some(TypedHistoryScalar::Jsonb(r#"{"answer":42}"#.to_owned())) },
                TypedHistoryField { name: "created_at".to_owned(), value: Some(TypedHistoryScalar::TimestampMicros(1_767_225_600_000_000)) },
            ],
        );

        assert!(matches!(
            typed_history_column_value(&row, &spec, "label").expect("label"),
            ProjectedHistoryValue::Typed(Some(TypedHistoryScalar::String(value))) if value == "ready"
        ));
        assert!(matches!(
            typed_history_column_value(&row, &spec, "metadata").expect("metadata"),
            ProjectedHistoryValue::Typed(Some(TypedHistoryScalar::Jsonb(value))) if value == r#"{"answer":42}"#
        ));
        assert!(matches!(
            typed_history_column_value(&row, &spec, "id").expect("authenticated identity"),
            ProjectedHistoryValue::Identity(Some(serde_json::Value::String(value)))
                if value == "01900000-0000-7000-8000-000000000001"
        ));
    }

    #[test]
    fn typed_history_rejects_wrong_fingerprint_and_missing_declared_field() {
        let spec = seven_type_spec();
        let wrong = typed_history_row("wrong-layout".to_owned(), Vec::new());
        assert!(typed_history_column_value(&wrong, &spec, "label")
            .expect_err("wrong schema fingerprint must fail")
            .to_string()
            .contains("schema fingerprint mismatch"));

        let missing = typed_history_row(spec.columnar_layout_fingerprint(), Vec::new());
        assert!(typed_history_column_value(&missing, &spec, "label")
            .expect_err("missing typed field must fail")
            .to_string()
            .contains("omitted a declared field"));

        let mut legacy = typed_history_row(spec.columnar_layout_fingerprint(), Vec::new());
        legacy.change.typed_snapshot = None;
        legacy.change.snapshot_content = Some(r#"{"label":"legacy"}"#.into());
        assert!(typed_history_column_value(&legacy, &spec, "label")
            .expect_err("registered entity history must not fall back to JSON")
            .to_string()
            .contains("omitted its authenticated typed payload"));
    }

    #[tokio::test]
    async fn registered_entity_write_publishes_typed_history_as_sole_commit_payload() {
        use crate::engine::Engine;
        use crate::storage_adapter::{Memory, StorageReadOptions};

        let storage = Memory::default();
        Engine::initialize(storage.clone())
            .await
            .expect("repository should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("engine should open");
        let session = engine.open_session().await.expect("session should open");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('typed-history-authority', 'one')",
                &[],
            )
            .await
            .expect("tracked entity write should succeed");
        let branch_id = session
            .active_branch_id()
            .await
            .expect("active branch should resolve");
        let commit_id = engine
            .load_branch_head_commit_id(&branch_id)
            .await
            .expect("head should load")
            .expect("head should exist");
        let commit_id = crate::changelog::CommitId::parse_lix(&commit_id, "typed history head")
            .expect("head should be canonical");
        let read = engine
            .storage()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("retained history read should open");
        let manifest = crate::tracked_state::load_commit_state_manifest(&read, commit_id)
            .await
            .expect("commit authority should load")
            .expect("commit authority should exist");
        let parts = manifest
            .mutations
            .columnar_parts
            .as_ref()
            .unwrap_or_else(|| {
                panic!(
                    "registered entity history must publish the typed group as sole authority: {:?}",
                    manifest.mutations
                )
            });
        assert_eq!(parts.schema_key, "lix_key_value");
        assert!(manifest.mutations.inline_part.is_empty());
        assert!(manifest.mutations.parts.is_empty());

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('typed-z', 'z'), ('typed-a', 'a')",
                &[],
            )
            .await
            .expect("reversed authored keys should canonicalize before publication");
        let reversed_head = engine
            .load_branch_head_commit_id(&branch_id)
            .await
            .expect("reversed head should load")
            .expect("reversed head should exist");
        let reversed_head = crate::changelog::CommitId::parse_lix(
            &reversed_head,
            "reversed typed history head",
        )
        .expect("reversed head should be canonical");
        let read = engine
            .storage()
            .begin_read(StorageReadOptions::default())
            .await
            .expect("reversed history read should open");
        let reversed = crate::tracked_state::load_commit_state_manifest(&read, reversed_head)
            .await
            .expect("reversed commit authority should load")
            .expect("reversed commit authority should exist");
        let reversed_parts = reversed
            .mutations
            .columnar_parts
            .as_ref()
            .expect("reversed batch must retain one typed authority");
        assert_eq!(reversed_parts.row_count, 2);
        assert!(reversed_parts.first_key < reversed_parts.last_key);
        assert!(reversed.mutations.inline_part.is_empty());
        assert!(reversed.mutations.parts.is_empty());

        let duplicate = session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('typed-duplicate', 'one'), ('typed-duplicate', 'two')",
                &[],
            )
            .await
            .expect_err("duplicate authored keys must fail before publication");
        assert!(!duplicate.code.is_empty());

        session
            .execute(
                "DELETE FROM lix_key_value WHERE key = 'typed-history-authority'",
                &[],
            )
            .await
            .expect("typed tombstone should publish");
        assert_eq!(
            crate::tracked_state::take_columnar_history_json_projections(),
            0
        );
        let deleted = session
            .execute(
                "SELECT key, lixcol_is_deleted FROM lix_key_value_history() \
                 WHERE key = 'typed-history-authority' ORDER BY lixcol_depth",
                &[],
            )
            .await
            .expect("typed tombstone history should project");
        assert!(deleted.rows().iter().any(|row| {
            row.get::<bool>("lixcol_is_deleted").unwrap_or(false)
        }));
        assert_eq!(
            crate::tracked_state::take_columnar_history_json_projections(),
            0,
            "scalar-only public history must not reconstruct JSON snapshots"
        );
    }

    #[test]
    fn public_composite_key_filters_route_in_schema_order() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "localized_message",
            "x-lix-primary-key": ["/locale", "/key"],
            "type": "object",
            "properties": {
                "key": { "type": "string" },
                "locale": { "type": "string" },
                "body": { "type": "string" }
            },
            "required": ["key", "locale", "body"]
        }))
        .expect("schema should derive");
        let filters = vec![
            eq("lixcol_as_of_commit_id", "commit-head"),
            // Deliberately reverse the declared primary-key path order.
            eq("key", "welcome"),
            eq("locale", "en"),
        ];

        let route = entity_history_route_from_filters(&spec, &filters)
            .expect("history route should derive");

        assert_eq!(route.as_of_commit_ids, vec!["commit-head"]);
        assert_eq!(route.entity_pks, vec![r#"["en","welcome"]"#]);
        assert!(!route.is_contradictory());
    }

    #[test]
    fn public_and_opaque_identity_filters_intersect() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "localized_message",
            "x-lix-primary-key": ["/locale", "/key"],
            "type": "object",
            "properties": {
                "key": { "type": "string" },
                "locale": { "type": "string" }
            },
            "required": ["key", "locale"]
        }))
        .expect("schema should derive");
        let filters = vec![
            eq("key", "welcome"),
            eq("locale", "en"),
            eq("lixcol_entity_pk", r#"["fr","welcome"]"#),
        ];

        let route = entity_history_route_from_filters(&spec, &filters)
            .expect("history route should derive");

        assert!(route.is_contradictory());

        let wrong_arity_route = entity_history_route_from_filters(
            &spec,
            &[
                eq("key", "welcome"),
                eq("locale", "en"),
                eq("lixcol_entity_pk", r#"["en"]"#),
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
