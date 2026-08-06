use std::collections::HashMap;

use datafusion::arrow::datatypes::Field;

pub(crate) const LIX_VALUE_TYPE_METADATA_KEY: &str = "lix.value_type";
pub(crate) const LIX_VALUE_TYPE_JSON: &str = "json";

pub(crate) fn json_field(name: impl Into<String>, nullable: bool) -> Field {
    Field::new(name, datafusion::arrow::datatypes::DataType::Utf8, nullable)
        .with_metadata(json_field_metadata_map())
}

pub(crate) fn mark_json_field(field: Field) -> Field {
    field.with_metadata(json_field_metadata_map())
}

pub(crate) fn field_is_json(field: &Field) -> bool {
    field
        .metadata()
        .get(LIX_VALUE_TYPE_METADATA_KEY)
        .is_some_and(|value| value == LIX_VALUE_TYPE_JSON)
}

fn json_field_metadata_map() -> HashMap<String, String> {
    HashMap::from([(
        LIX_VALUE_TYPE_METADATA_KEY.to_string(),
        LIX_VALUE_TYPE_JSON.to_string(),
    )])
}
