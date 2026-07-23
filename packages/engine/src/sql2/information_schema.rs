use std::any::Any;
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringArray, UInt64Array};
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

use super::catalog::{PublicCatalog, PublicColumnInsertPolicy};
use super::result_metadata::field_is_json;

const LIX_VALUE_KIND_JSON: &str = "JSON";

/// Installs Lix's SQL-level column contract while retaining DataFusion's other
/// standard information-schema views.
///
/// Arrow schemas remain the execution representation. The public catalog must
/// instead advertise spellings that Lix SQL accepts, plus the distinction
/// between read nullability and insert-time omission/default behavior.
pub(crate) fn register(
    session: &SessionContext,
    public_catalog: &PublicCatalog,
) -> Result<(), LixError> {
    let state = session.state();
    let catalog_name = state.config_options().catalog.default_catalog.clone();
    let schema_name = state.config_options().catalog.default_schema.clone();
    let catalog_list = Arc::clone(state.catalog_list());
    let catalog = catalog_list.catalog(&catalog_name).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("SQL default catalog '{catalog_name}' is missing"),
        )
    })?;
    let provider: Arc<dyn SchemaProvider> = Arc::new(LixInformationSchemaProvider::new(
        catalog_list,
        public_catalog.clone(),
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
    public_catalog: PublicCatalog,
    public_catalog_name: String,
    public_schema_name: String,
}

impl LixInformationSchemaProvider {
    fn new(
        catalog_list: Arc<dyn CatalogProviderList>,
        public_catalog: PublicCatalog,
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

            for table_name in INFORMATION_SCHEMA_TABLES {
                let table_schema = if *table_name == "columns" {
                    Arc::clone(&schema)
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
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if name.eq_ignore_ascii_case("columns") {
            return self.columns_table().await.map(Some);
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
    }
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
                .push(field_is_json(field).then(|| LIX_VALUE_KIND_JSON.to_string()));
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
        DataType::LargeUtf8 | DataType::LargeBinary => (None, Some(i64::MAX as u64)),
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
