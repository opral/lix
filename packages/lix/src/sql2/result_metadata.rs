use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};

use crate::{LixError, ResultColumnType};

pub(crate) const LIX_VALUE_TYPE_METADATA_KEY: &str = "lix.value_type";
pub(crate) const LIX_VALUE_TYPE_JSONB: &str = "jsonb";
pub(crate) const LIX_VALUE_TYPE_ROW_REF: &str = "row_ref";

pub(crate) fn json_field(name: impl Into<String>, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable)
        .with_metadata(json_field_metadata_map())
}

pub(crate) fn mark_json_field(field: Field) -> Field {
    field.with_metadata(json_field_metadata_map())
}

pub(crate) fn row_ref_field(name: impl Into<String>, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable).with_metadata(HashMap::from([(
        LIX_VALUE_TYPE_METADATA_KEY.to_string(),
        LIX_VALUE_TYPE_ROW_REF.to_string(),
    )]))
}

pub(crate) fn field_is_row_ref(field: &Field) -> bool {
    field
        .metadata()
        .get(LIX_VALUE_TYPE_METADATA_KEY)
        .is_some_and(|value| value == LIX_VALUE_TYPE_ROW_REF)
}

pub(crate) fn field_is_json(field: &Field) -> bool {
    field
        .metadata()
        .get(LIX_VALUE_TYPE_METADATA_KEY)
        .is_some_and(|value| value == LIX_VALUE_TYPE_JSONB)
}

pub(crate) fn result_column_type(field: &Field) -> Result<ResultColumnType, LixError> {
    let column_type = match field.data_type() {
        DataType::Null => ResultColumnType::Null,
        DataType::Boolean => ResultColumnType::Boolean,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => ResultColumnType::Integer,
        DataType::Float32 | DataType::Float64 => ResultColumnType::Real,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            if field_is_row_ref(field) {
                ResultColumnType::RowRef
            } else if field_is_json(field) {
                ResultColumnType::Jsonb
            } else {
                ResultColumnType::Text
            }
        }
        DataType::Binary | DataType::LargeBinary => ResultColumnType::Blob,
        DataType::Timestamp(TimeUnit::Microsecond, _) => ResultColumnType::Timestamptz,
        other => {
            return Err(LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                format!("SQL query produced an unsupported result column type {other}"),
            )
            .with_hint(
                "Cast the column to a supported Lix result type such as TEXT, BIGINT, DOUBLE, BOOLEAN, or BYTEA.",
            ));
        }
    };
    Ok(column_type)
}

fn json_field_metadata_map() -> HashMap<String, String> {
    HashMap::from([(
        LIX_VALUE_TYPE_METADATA_KEY.to_string(),
        LIX_VALUE_TYPE_JSONB.to_string(),
    )])
}
