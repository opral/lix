use std::any::Any;
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::information_schema::{
    INFORMATION_SCHEMA, INFORMATION_SCHEMA_TABLES, InformationSchemaProvider,
};
use datafusion::catalog::{CatalogProviderList, SchemaProvider, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use crate::LixError;

use super::catalog::{
    PublicCatalog, PublicColumnInsertPolicy, PublicRelationKind, PublicSurfaceClass,
    PublicSurfaceKind,
};
use super::result_metadata::{field_is_json, field_is_row_ref};

const LIX_VALUE_KIND_JSONB: &str = "JSONB";
const LIX_VALUE_KIND_ROW_REF: &str = "ROW_REF";

fn field_value_kind(field: &Field) -> Option<String> {
    if field_is_row_ref(field) {
        Some(LIX_VALUE_KIND_ROW_REF.to_owned())
    } else {
        field_is_json(field).then(|| LIX_VALUE_KIND_JSONB.to_owned())
    }
}
const TABLE_FUNCTIONS: &str = "table_functions";
const LIX_SURFACES: &str = "lix_surfaces";

/// Installs Lix's SQL-level column contract while retaining DataFusion's other
/// standard information-schema views.
///
/// Arrow schemas remain the execution representation. The public catalog must
/// instead advertise spellings that Lix SQL accepts, plus the distinction
/// between read nullability and insert-time omission/default behavior.
pub(crate) fn register(
    session: &SessionContext,
    public_catalog: Arc<PublicCatalog>,
) -> Result<(), LixError> {
    // Borrow the live session state. `SessionState::clone` deep-copies the
    // `String`-keyed registries for every built-in scalar, aggregate and window
    // function; this call site only needs two config strings and the catalog
    // list `Arc`.
    let state_ref = session.state_ref();
    let state = state_ref.read();
    let catalog_name = state.config_options().catalog.default_catalog.clone();
    let schema_name = state.config_options().catalog.default_schema.clone();
    let catalog_list = Arc::clone(state.catalog_list());
    drop(state);
    let catalog = catalog_list.catalog(&catalog_name).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("SQL default catalog '{catalog_name}' is missing"),
        )
    })?;
    let provider: Arc<dyn SchemaProvider> = Arc::new(LixInformationSchemaProvider::new(
        catalog_list,
        public_catalog,
        catalog_name.clone(),
        schema_name,
    ));
    catalog
        .register_schema(INFORMATION_SCHEMA, provider)
        .map_err(super::error::datafusion_error_to_lix_error)?;
    Ok(())
}

#[derive(Debug)]
struct LixInformationSchemaProvider {
    // This provider is itself registered inside the catalog list. A strong
    // reference here would create a cycle that retains every session table
    // provider and its storage read handles.
    catalog_list: Weak<dyn CatalogProviderList>,
    public_catalog: Arc<PublicCatalog>,
    public_catalog_name: String,
    public_schema_name: String,
}

impl LixInformationSchemaProvider {
    fn new(
        catalog_list: Arc<dyn CatalogProviderList>,
        public_catalog: Arc<PublicCatalog>,
        public_catalog_name: String,
        public_schema_name: String,
    ) -> Self {
        Self {
            catalog_list: Arc::downgrade(&catalog_list),
            public_catalog,
            public_catalog_name,
            public_schema_name,
        }
    }

    async fn columns_table(&self) -> Result<Arc<dyn TableProvider>> {
        let schema = columns_schema();
        let mut rows = ColumnsRows::default();
        let catalog_list = self.catalog_list.upgrade().ok_or_else(|| {
            DataFusionError::Execution("SQL catalog closed while reading information_schema".into())
        })?;
        let delegate = InformationSchemaProvider::new(Arc::clone(&catalog_list));
        let mut catalog_names = catalog_list.catalog_names();
        catalog_names.sort();
        for catalog_name in catalog_names {
            let Some(catalog) = catalog_list.catalog(&catalog_name) else {
                continue;
            };
            let mut schema_names = catalog.schema_names();
            schema_names.sort();
            for schema_name in schema_names {
                if schema_name == INFORMATION_SCHEMA {
                    continue;
                }
                let Some(schema_provider) = catalog.schema(&schema_name) else {
                    continue;
                };
                let mut table_names = schema_provider.table_names();
                table_names.sort();
                for table_name in table_names {
                    let Some(table) = schema_provider.table(&table_name).await? else {
                        continue;
                    };
                    rows.add_table(
                        &catalog_name,
                        &schema_name,
                        &table_name,
                        table.schema().as_ref(),
                        (catalog_name == self.public_catalog_name
                            && schema_name == self.public_schema_name)
                            .then_some(&self.public_catalog),
                    );
                }
            }

            for table_name in INFORMATION_SCHEMA_TABLES
                .iter()
                .copied()
                .chain([TABLE_FUNCTIONS, LIX_SURFACES])
            {
                let table_schema = if table_name == "columns" {
                    Arc::clone(&schema)
                } else if table_name == TABLE_FUNCTIONS {
                    table_functions_schema()
                } else if table_name == LIX_SURFACES {
                    lix_surfaces_schema()
                } else {
                    delegate
                        .table(table_name)
                        .await?
                        .map(|table| table.schema())
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "information_schema.{table_name} is missing"
                            ))
                        })?
                };
                rows.add_table(
                    &catalog_name,
                    INFORMATION_SCHEMA,
                    table_name,
                    table_schema.as_ref(),
                    None,
                );
            }
        }

        let batch = rows.finish(Arc::clone(&schema))?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    fn table_functions_table(&self) -> Result<Arc<dyn TableProvider>> {
        let schema = table_functions_schema();
        let mut function_catalog = Vec::new();
        let mut function_schema = Vec::new();
        let mut function_name = Vec::new();
        let mut source_relation = Vec::new();
        let mut argument_signature = Vec::new();
        let mut result_column = Vec::new();
        let mut ordinal_position = Vec::new();
        let mut is_nullable = Vec::new();
        let mut data_type = Vec::new();
        let mut lix_value_kind = Vec::new();

        for history in self.public_catalog.history_relations() {
            let provider_schema = self
                .public_catalog
                .history_relation_schema(&history.relation_name)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "history relation '{}' is missing its result schema",
                        history.relation_name
                    ))
                })?;
            for (position, column) in history
                .columns
                .iter()
                .filter(|column| column.is_public())
                .enumerate()
            {
                let field = provider_schema.field_with_name(&column.name)?;
                function_catalog.push(self.public_catalog_name.clone());
                function_schema.push(self.public_schema_name.clone());
                function_name.push("lix_history".to_string());
                source_relation.push(Some(history.relation_name.clone()));
                argument_signature
                    .push("(relation TEXT) | (relation TEXT, as_of TEXT)".to_string());
                result_column.push(column.name.clone());
                ordinal_position.push((position + 1) as u64);
                is_nullable.push(if column.read_nullable { "YES" } else { "NO" }.to_string());
                data_type.push(public_sql_type(field.data_type()));
                lix_value_kind.push(field_value_kind(field));
            }
        }

        for surface in self
            .public_catalog
            .surfaces()
            .filter(|surface| surface.class == PublicSurfaceClass::TableFunction)
        {
            if matches!(
                surface.kind,
                PublicSurfaceKind::DiffFunction | PublicSurfaceKind::StateAtFunction
            ) {
                for relation in self.public_catalog.surfaces().filter(|relation| {
                    matches!(
                        relation.kind,
                        PublicSurfaceKind::File
                            | PublicSurfaceKind::Directory
                            | PublicSurfaceKind::SchemaBase { .. }
                    )
                }) {
                    let provider_schema = if surface.kind == PublicSurfaceKind::DiffFunction {
                        super::providers::relation_diff_schema(
                            self.public_catalog.as_ref(),
                            &relation.name,
                        )?
                    } else {
                        self.public_catalog.surface_schema(&relation.name).ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "state relation '{}' is missing its result schema",
                                relation.name
                            ))
                        })?
                    };
                    for (position, field) in provider_schema.fields().iter().enumerate() {
                        function_catalog.push(self.public_catalog_name.clone());
                        function_schema.push(self.public_schema_name.clone());
                        function_name.push(surface.name.clone());
                        source_relation.push(Some(relation.name.clone()));
                        argument_signature.push(if surface.kind == PublicSurfaceKind::DiffFunction {
                            "(relation TEXT) | (relation TEXT, from_commit_id TEXT, to_commit_id TEXT)".to_string()
                        } else {
                            "(relation TEXT, commit_id TEXT)".to_string()
                        });
                        result_column.push(field.name().clone());
                        ordinal_position.push((position + 1) as u64);
                        is_nullable
                            .push(if field.is_nullable() { "YES" } else { "NO" }.to_string());
                        data_type.push(public_sql_type(field.data_type()));
                        lix_value_kind.push(
                            field_value_kind(field),
                        );
                    }
                }
                continue;
            }
            let (signature, provider_schema) = match surface.kind {
                PublicSurfaceKind::HistoryFunction => continue,
                PublicSurfaceKind::CommitAncestryFunction => (
                    "() | (commit_id TEXT)",
                    super::providers::commit_ancestry_schema(),
                ),
                PublicSurfaceKind::CheckpointFunction => (
                    "() | (row_refs ROW_REF[])",
                    Arc::new(Schema::new(vec![Field::new(
                        "commit_id",
                        DataType::Utf8,
                        false,
                    )])),
                ),
                PublicSurfaceKind::DiffFunction => unreachable!("relation-specific diffs handled above"),
                PublicSurfaceKind::StateAtFunction => unreachable!("relation-specific state handled above"),
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "table function '{}' has a non-function semantic kind",
                        surface.name
                    )));
                }
            };
            for (position, field) in provider_schema.fields().iter().enumerate() {
                function_catalog.push(self.public_catalog_name.clone());
                function_schema.push(self.public_schema_name.clone());
                function_name.push(surface.name.clone());
                source_relation.push(None);
                argument_signature.push(signature.to_string());
                result_column.push(field.name().clone());
                ordinal_position.push((position + 1) as u64);
                is_nullable.push(if field.is_nullable() { "YES" } else { "NO" }.to_string());
                data_type.push(public_sql_type(field.data_type()));
                lix_value_kind.push(field_value_kind(field));
            }
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(function_catalog)),
                Arc::new(StringArray::from(function_schema)),
                Arc::new(StringArray::from(function_name)),
                Arc::new(StringArray::from(source_relation)),
                Arc::new(StringArray::from(argument_signature)),
                Arc::new(StringArray::from(result_column)),
                Arc::new(UInt64Array::from(ordinal_position)),
                Arc::new(StringArray::from(is_nullable)),
                Arc::new(StringArray::from(data_type)),
                Arc::new(StringArray::from(lix_value_kind)),
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    fn lix_surfaces_table(&self) -> Result<Arc<dyn TableProvider>> {
        let schema = lix_surfaces_schema();
        let mut surface_catalog = Vec::new();
        let mut surface_schema = Vec::new();
        let mut surface_name = Vec::new();
        let mut surface_class = Vec::new();
        let mut relation_kind = Vec::new();
        let mut can_read = Vec::new();
        let mut can_insert = Vec::new();
        let mut can_update = Vec::new();
        let mut can_delete = Vec::new();
        let mut is_side_effecting = Vec::new();

        for surface in self.public_catalog.surfaces() {
            surface_catalog.push(self.public_catalog_name.clone());
            surface_schema.push(self.public_schema_name.clone());
            surface_name.push(surface.name.clone());
            surface_class.push(surface.class.sql_name().to_string());
            relation_kind.push(match surface.class {
                PublicSurfaceClass::Relation(PublicRelationKind::Base) => {
                    Some("BASE".to_string())
                }
                PublicSurfaceClass::Relation(PublicRelationKind::View) => {
                    Some("VIEW".to_string())
                }
                _ => None,
            });
            can_read.push(!matches!(surface.class, PublicSurfaceClass::CommandSink));
            can_insert.push(surface.capabilities.insert);
            can_update.push(surface.capabilities.update);
            can_delete.push(surface.capabilities.delete);
            is_side_effecting.push(
                matches!(surface.class, PublicSurfaceClass::CommandSink)
                    || surface.kind == PublicSurfaceKind::CheckpointFunction,
            );
        }
        for function in self.public_catalog.scalar_functions() {
            surface_catalog.push(self.public_catalog_name.clone());
            surface_schema.push(self.public_schema_name.clone());
            surface_name.push(function.name.clone());
            surface_class.push(function.class.sql_name().to_string());
            relation_kind.push(None);
            can_read.push(true);
            can_insert.push(false);
            can_update.push(false);
            can_delete.push(false);
            is_side_effecting.push(false);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(surface_catalog)),
                Arc::new(StringArray::from(surface_schema)),
                Arc::new(StringArray::from(surface_name)),
                Arc::new(StringArray::from(surface_class)),
                Arc::new(StringArray::from(relation_kind)),
                Arc::new(BooleanArray::from(can_read)),
                Arc::new(BooleanArray::from(can_insert)),
                Arc::new(BooleanArray::from(can_update)),
                Arc::new(BooleanArray::from(can_delete)),
                Arc::new(BooleanArray::from(is_side_effecting)),
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

#[async_trait]
impl SchemaProvider for LixInformationSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        INFORMATION_SCHEMA_TABLES
            .iter()
            .map(|name| (*name).to_string())
            .chain([TABLE_FUNCTIONS.to_string(), LIX_SURFACES.to_string()])
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if name.eq_ignore_ascii_case("columns") {
            return self.columns_table().await.map(Some);
        }
        if name.eq_ignore_ascii_case(TABLE_FUNCTIONS) {
            return self.table_functions_table().map(Some);
        }
        if name.eq_ignore_ascii_case(LIX_SURFACES) {
            return self.lix_surfaces_table().map(Some);
        }
        let catalog_list = self.catalog_list.upgrade().ok_or_else(|| {
            DataFusionError::Execution("SQL catalog closed while reading information_schema".into())
        })?;
        InformationSchemaProvider::new(catalog_list)
            .table(name)
            .await
    }

    fn table_exist(&self, name: &str) -> bool {
        INFORMATION_SCHEMA_TABLES
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(name))
            || name.eq_ignore_ascii_case(TABLE_FUNCTIONS)
            || name.eq_ignore_ascii_case(LIX_SURFACES)
    }
}

fn table_functions_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("function_catalog", DataType::Utf8, false),
        Field::new("function_schema", DataType::Utf8, false),
        Field::new("function_name", DataType::Utf8, false),
        Field::new("source_relation", DataType::Utf8, true),
        Field::new("argument_signature", DataType::Utf8, false),
        Field::new("result_column", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::UInt64, false),
        Field::new("is_nullable", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("lix_value_kind", DataType::Utf8, true),
    ]))
}

fn lix_surfaces_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("surface_catalog", DataType::Utf8, false),
        Field::new("surface_schema", DataType::Utf8, false),
        Field::new("surface_name", DataType::Utf8, false),
        Field::new("surface_class", DataType::Utf8, false),
        Field::new("relation_kind", DataType::Utf8, true),
        Field::new("can_read", DataType::Boolean, false),
        Field::new("can_insert", DataType::Boolean, false),
        Field::new("can_update", DataType::Boolean, false),
        Field::new("can_delete", DataType::Boolean, false),
        Field::new("is_side_effecting", DataType::Boolean, false),
    ]))
}

fn columns_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::UInt64, false),
        Field::new("column_default", DataType::Utf8, true),
        Field::new("is_nullable", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("character_maximum_length", DataType::UInt64, true),
        Field::new("character_octet_length", DataType::UInt64, true),
        Field::new("numeric_precision", DataType::UInt64, true),
        Field::new("numeric_precision_radix", DataType::UInt64, true),
        Field::new("numeric_scale", DataType::UInt64, true),
        Field::new("datetime_precision", DataType::UInt64, true),
        Field::new("interval_type", DataType::Utf8, true),
        Field::new("lix_value_kind", DataType::Utf8, true),
        Field::new("lix_insert_policy", DataType::Utf8, false),
    ]))
}

#[derive(Default)]
struct ColumnsRows {
    table_catalog: Vec<String>,
    table_schema: Vec<String>,
    table_name: Vec<String>,
    column_name: Vec<String>,
    ordinal_position: Vec<u64>,
    column_default: Vec<Option<String>>,
    is_nullable: Vec<String>,
    data_type: Vec<String>,
    character_maximum_length: Vec<Option<u64>>,
    character_octet_length: Vec<Option<u64>>,
    numeric_precision: Vec<Option<u64>>,
    numeric_precision_radix: Vec<Option<u64>>,
    numeric_scale: Vec<Option<u64>>,
    datetime_precision: Vec<Option<u64>>,
    interval_type: Vec<Option<String>>,
    lix_value_kind: Vec<Option<String>>,
    lix_insert_policy: Vec<String>,
}

impl ColumnsRows {
    fn add_table(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        schema: &Schema,
        public_catalog: Option<&PublicCatalog>,
    ) {
        for (index, field) in schema.fields().iter().enumerate() {
            let column_contract = public_catalog
                .and_then(|catalog| catalog.surface(table_name))
                .and_then(|surface| {
                    surface
                        .columns
                        .iter()
                        .find(|column| column.name == field.name().as_str())
                        .map(|column| (surface, column))
                });
            let column_default = column_contract.as_ref().and_then(|(surface, column)| {
                surface
                    .capabilities
                    .insert
                    .then(|| column.column_default.clone())
                    .flatten()
            });
            let insert_policy =
                column_contract.map_or(PublicColumnInsertPolicy::ReadOnly, |(surface, column)| {
                    if surface.capabilities.insert && column.is_insertable() {
                        column.insert_policy
                    } else {
                        PublicColumnInsertPolicy::ReadOnly
                    }
                });
            let (character_maximum_length, character_octet_length) =
                character_lengths(field.data_type());
            let (numeric_precision, numeric_precision_radix, numeric_scale) =
                numeric_metadata(field.data_type());

            self.table_catalog.push(catalog_name.to_string());
            self.table_schema.push(schema_name.to_string());
            self.table_name.push(table_name.to_string());
            self.column_name.push(field.name().clone());
            self.ordinal_position.push((index + 1) as u64);
            self.column_default.push(column_default);
            let read_nullable = column_contract
                .as_ref()
                .map_or_else(|| field.is_nullable(), |(_, column)| column.read_nullable);
            self.is_nullable
                .push(if read_nullable { "YES" } else { "NO" }.to_string());
            self.data_type.push(public_sql_type(field.data_type()));
            self.character_maximum_length.push(character_maximum_length);
            self.character_octet_length.push(character_octet_length);
            self.numeric_precision.push(numeric_precision);
            self.numeric_precision_radix.push(numeric_precision_radix);
            self.numeric_scale.push(numeric_scale);
            self.datetime_precision.push(None);
            self.interval_type.push(None);
            self.lix_value_kind
                .push(field_value_kind(field));
            self.lix_insert_policy
                .push(insert_policy.as_str().to_string());
        }
    }

    fn finish(self, schema: SchemaRef) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(self.table_catalog)),
            Arc::new(StringArray::from(self.table_schema)),
            Arc::new(StringArray::from(self.table_name)),
            Arc::new(StringArray::from(self.column_name)),
            Arc::new(UInt64Array::from(self.ordinal_position)),
            Arc::new(StringArray::from(self.column_default)),
            Arc::new(StringArray::from(self.is_nullable)),
            Arc::new(StringArray::from(self.data_type)),
            Arc::new(UInt64Array::from(self.character_maximum_length)),
            Arc::new(UInt64Array::from(self.character_octet_length)),
            Arc::new(UInt64Array::from(self.numeric_precision)),
            Arc::new(UInt64Array::from(self.numeric_precision_radix)),
            Arc::new(UInt64Array::from(self.numeric_scale)),
            Arc::new(UInt64Array::from(self.datetime_precision)),
            Arc::new(StringArray::from(self.interval_type)),
            Arc::new(StringArray::from(self.lix_value_kind)),
            Arc::new(StringArray::from(self.lix_insert_policy)),
        ];
        Ok(RecordBatch::try_new(schema, arrays)?)
    }
}

fn public_sql_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "TEXT".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BYTEA".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 | DataType::UInt8 | DataType::Int16 | DataType::UInt16 => {
            "SMALLINT".to_string()
        }
        DataType::Int32 | DataType::UInt32 => "INTEGER".to_string(),
        DataType::Int64 | DataType::UInt64 => "BIGINT".to_string(),
        DataType::Float16 | DataType::Float32 => "REAL".to_string(),
        DataType::Float64 => "DOUBLE PRECISION".to_string(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        DataType::Null => "NULL".to_string(),
        other => other.to_string(),
    }
}

fn character_lengths(data_type: &DataType) -> (Option<u64>, Option<u64>) {
    match data_type {
        DataType::Utf8 | DataType::Binary => (None, Some(i32::MAX as u64)),
        // Arrow's large variable-width types are bounded by an implementation
        // offset, not by the public SQL column contract. Advertising i64::MAX
        // as an octet length is both misleading and outside JavaScript's safe
        // integer range, which makes SELECT * on information_schema.columns
        // impossible to return through the JS SDK.
        DataType::LargeUtf8 | DataType::LargeBinary => (None, None),
        _ => (None, None),
    }
}

fn numeric_metadata(data_type: &DataType) -> (Option<u64>, Option<u64>, Option<u64>) {
    match data_type {
        DataType::Int8 | DataType::UInt8 => (Some(8), Some(2), None),
        DataType::Int16 | DataType::UInt16 => (Some(16), Some(2), None),
        DataType::Int32 | DataType::UInt32 => (Some(32), Some(2), None),
        DataType::Int64 | DataType::UInt64 => (Some(64), Some(2), None),
        DataType::Float16 => (Some(11), Some(2), None),
        DataType::Float32 => (Some(24), Some(2), None),
        DataType::Float64 => (Some(53), Some(2), None),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => (
            Some((*precision).into()),
            Some(10),
            u64::try_from(*scale).ok(),
        ),
        _ => (None, None, None),
    }
}
