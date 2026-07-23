use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result, not_impl_err};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use serde_json::{Value as JsonValue, json};

use crate::LixError;
use crate::sql2::catalog::{EntityColumnType, PublicCatalog, PublicColumn, PublicSurfaceKind};
use crate::sql2::result_metadata::json_field;
use crate::sql2::{SqlWriteContext, WriteAccess};

use super::spec::{
    InsertApply, PlannedDml, PlannedScan, TableSpec, projected_schema, register_spec_table,
    row_source,
};

pub(super) fn register_lix_schema_read_provider(
    session: &SessionContext,
    surface_name: &str,
    catalog: &PublicCatalog,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(SchemaCatalogSpec::new(catalog)?),
        WriteAccess::read_only(),
    )
}

pub(super) fn register_lix_schema_definition_read_provider(
    session: &SessionContext,
    surface_name: &str,
    catalog: &PublicCatalog,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(SchemaDefinitionSpec::new(catalog)),
        WriteAccess::read_only(),
    )
}

pub(super) fn register_lix_schema_definition_write_provider(
    session: &SessionContext,
    surface_name: &str,
    catalog: &PublicCatalog,
    write_ctx: SqlWriteContext,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(SchemaDefinitionSpec::new(catalog)),
        WriteAccess::write(write_ctx),
    )
}

#[derive(Clone)]
struct SchemaCatalogRow {
    key: String,
    table_name: Option<String>,
    by_branch_table_name: Option<String>,
    history_table_name: Option<String>,
    primary_key: String,
    columns: String,
    surfaces: String,
    definition: String,
}

struct SchemaCatalogSpec {
    rows: Arc<Vec<SchemaCatalogRow>>,
}

impl SchemaCatalogSpec {
    fn new(catalog: &PublicCatalog) -> Result<Self, LixError> {
        let mut rows = Vec::new();
        for (key, definition) in catalog.schema_definitions() {
            let table_name = entity_surface_name(catalog, key, EntitySurfaceVariant::Base);
            let by_branch_table_name =
                entity_surface_name(catalog, key, EntitySurfaceVariant::ByBranch);
            let history_table_name =
                entity_surface_name(catalog, key, EntitySurfaceVariant::History);
            let surfaces = [
                table_name.clone(),
                by_branch_table_name.clone(),
                history_table_name.clone(),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            let primary_key = definition
                .get("x-lix-primary-key")
                .cloned()
                .unwrap_or_else(|| json!([]));
            rows.push(SchemaCatalogRow {
                key: key.to_string(),
                table_name,
                by_branch_table_name,
                history_table_name,
                primary_key: render_json(&primary_key, key, "primary key")?,
                columns: render_json(&schema_column_contracts(catalog, key), key, "columns")?,
                surfaces: render_json(&json!(surfaces), key, "surfaces")?,
                definition: render_json(definition, key, "definition")?,
            });
        }
        Ok(Self {
            rows: Arc::new(rows),
        })
    }
}

#[derive(Clone)]
struct SchemaDefinitionRow {
    key: String,
    definition: String,
}

struct SchemaDefinitionSpec {
    rows: Arc<Vec<SchemaDefinitionRow>>,
}

impl SchemaDefinitionSpec {
    fn new(catalog: &PublicCatalog) -> Self {
        let rows = catalog
            .schema_definitions()
            .map(|(key, definition)| SchemaDefinitionRow {
                key: key.to_string(),
                definition: definition.to_string(),
            })
            .collect();
        Self {
            rows: Arc::new(rows),
        }
    }
}

#[derive(Clone, Copy)]
enum EntitySurfaceVariant {
    Base,
    ByBranch,
    History,
}

fn entity_surface_name(
    catalog: &PublicCatalog,
    schema_key: &str,
    variant: EntitySurfaceVariant,
) -> Option<String> {
    catalog
        .surfaces()
        .find(|surface| match (&surface.kind, variant) {
            (
                PublicSurfaceKind::EntityBase {
                    schema_key: candidate,
                },
                EntitySurfaceVariant::Base,
            )
            | (
                PublicSurfaceKind::EntityByBranch {
                    schema_key: candidate,
                },
                EntitySurfaceVariant::ByBranch,
            )
            | (
                PublicSurfaceKind::EntityHistory {
                    schema_key: candidate,
                },
                EntitySurfaceVariant::History,
            ) => candidate == schema_key,
            _ => false,
        })
        .map(|surface| surface.name.clone())
}

fn schema_column_contracts(catalog: &PublicCatalog, schema_key: &str) -> JsonValue {
    let Some(spec) = catalog.entity_spec(schema_key) else {
        return json!([]);
    };
    let base_surface = entity_surface_name(catalog, schema_key, EntitySurfaceVariant::Base)
        .and_then(|name| catalog.surface(&name));
    JsonValue::Array(
        spec.columns
            .iter()
            .map(|column| {
                let public = base_surface.and_then(|surface| surface.public_column(&column.name));
                json!({
                    "name": column.name,
                    "data_type": public_sql_type(column.column_type),
                    "lix_value_kind": (column.column_type == EntityColumnType::Json)
                        .then_some("JSON"),
                    "is_nullable": public.map_or(column.read_nullable, |column| column.read_nullable),
                    "is_insertable": public.is_some_and(PublicColumn::is_insertable),
                    "is_updatable": public.is_some_and(PublicColumn::is_updatable),
                    "lix_insert_policy": public.map(|column| column.insert_policy.as_str()),
                    "column_default": public
                        .and_then(|column| column.column_default.as_deref())
                        .or(column.default_expression.as_deref()),
                })
            })
            .collect(),
    )
}

fn public_sql_type(column_type: EntityColumnType) -> &'static str {
    match column_type {
        EntityColumnType::String | EntityColumnType::Json => "TEXT",
        EntityColumnType::Integer => "BIGINT",
        EntityColumnType::Number => "DOUBLE PRECISION",
        EntityColumnType::Boolean => "BOOLEAN",
    }
}

fn render_json(value: &JsonValue, schema_key: &str, field: &str) -> Result<String, LixError> {
    serde_json::to_string(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("could not render lix_schema {field} for '{schema_key}': {error}"),
        )
    })
}

#[async_trait]
impl TableSpec for SchemaCatalogSpec {
    fn table_name(&self) -> &'static str {
        "lix_schema"
    }

    fn schema(&self) -> SchemaRef {
        lix_schema_schema()
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let schema = projected_schema(&lix_schema_schema(), projection);
        let limit = filters.is_empty().then_some(limit).flatten();
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            load: row_source(
                (Arc::clone(&self.rows), schema, limit),
                |(rows, schema, limit)| async move {
                    schema_catalog_record_batch(schema, rows.as_slice(), limit)
                },
            ),
        })
    }
}

#[async_trait]
impl TableSpec for SchemaDefinitionSpec {
    fn table_name(&self) -> &'static str {
        "lix_schema_definition"
    }

    fn schema(&self) -> SchemaRef {
        lix_schema_definition_schema()
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let schema = projected_schema(&lix_schema_definition_schema(), projection);
        let limit = filters.is_empty().then_some(limit).flatten();
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            load: row_source(
                (Arc::clone(&self.rows), schema, limit),
                |(rows, schema, limit)| async move {
                    schema_definition_record_batch(schema, rows.as_slice(), limit)
                },
            ),
        })
    }

    // Schema-definition writes have identity derived from the JSON definition.
    // The bound executor owns that semantic transform; raw DataFusion DML must
    // not treat the nullable planning placeholder for `key` as stored data.
    async fn plan_insert(
        &self,
        _write_ctx: SqlWriteContext,
        _input: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<InsertApply>> {
        not_impl_err!(
            "raw DataFusion INSERT is disabled; use the lix_schema_definition SQL surface"
        )
    }

    async fn plan_update(
        &self,
        _write_ctx: SqlWriteContext,
        _assignments: Vec<(String, Arc<dyn datafusion::physical_expr::PhysicalExpr>)>,
        _filters: &[Expr],
    ) -> Result<PlannedDml> {
        not_impl_err!(
            "raw DataFusion UPDATE is disabled; use the lix_schema_definition SQL surface"
        )
    }
}

pub(super) fn lix_schema_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, true),
        Field::new("by_branch_table_name", DataType::Utf8, true),
        Field::new("history_table_name", DataType::Utf8, true),
        json_field("primary_key", false),
        json_field("columns", false),
        json_field("surfaces", false),
        json_field("definition", false),
    ]))
}

pub(super) fn lix_schema_definition_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        // Nullable only in the physical planning schema so INSERT may omit the
        // read-only derived key. PublicCatalog advertises non-null read rows.
        Field::new("key", DataType::Utf8, true),
        json_field("definition", false),
    ]))
}

fn schema_catalog_record_batch(
    schema: SchemaRef,
    rows: &[SchemaCatalogRow],
    limit: Option<usize>,
) -> Result<RecordBatch> {
    let rows = &rows[..limit.unwrap_or(rows.len()).min(rows.len())];
    record_batch_from_columns(schema, rows.len(), |column| {
        Ok(match column {
            "key" => string_array(rows.iter().map(|row| Some(row.key.as_str()))),
            "table_name" => string_array(rows.iter().map(|row| row.table_name.as_deref())),
            "by_branch_table_name" => {
                string_array(rows.iter().map(|row| row.by_branch_table_name.as_deref()))
            }
            "history_table_name" => {
                string_array(rows.iter().map(|row| row.history_table_name.as_deref()))
            }
            "primary_key" => string_array(rows.iter().map(|row| Some(row.primary_key.as_str()))),
            "columns" => string_array(rows.iter().map(|row| Some(row.columns.as_str()))),
            "surfaces" => string_array(rows.iter().map(|row| Some(row.surfaces.as_str()))),
            "definition" => string_array(rows.iter().map(|row| Some(row.definition.as_str()))),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "lix_schema does not expose column '{other}'"
                )));
            }
        })
    })
}

fn schema_definition_record_batch(
    schema: SchemaRef,
    rows: &[SchemaDefinitionRow],
    limit: Option<usize>,
) -> Result<RecordBatch> {
    let rows = &rows[..limit.unwrap_or(rows.len()).min(rows.len())];
    record_batch_from_columns(schema, rows.len(), |column| {
        Ok(match column {
            "key" => string_array(rows.iter().map(|row| Some(row.key.as_str()))),
            "definition" => string_array(rows.iter().map(|row| Some(row.definition.as_str()))),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "lix_schema_definition does not expose column '{other}'"
                )));
            }
        })
    })
}

fn string_array<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(values))
}

fn record_batch_from_columns(
    schema: SchemaRef,
    row_count: usize,
    column: impl Fn(&str) -> Result<ArrayRef>,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        return RecordBatch::try_new_with_options(
            schema,
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
        .map_err(DataFusionError::from);
    }
    let arrays = schema
        .fields()
        .iter()
        .map(|field| column(field.name()))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays).map_err(DataFusionError::from)
}
