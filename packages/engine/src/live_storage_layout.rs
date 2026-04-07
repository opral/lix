use serde_json::Value as JsonValue;

use crate::LixError;

pub(crate) fn tracked_relation_name(schema_key: &str) -> String {
    crate::live_schema_access::tracked_relation_name(schema_key)
}

pub(crate) fn payload_column_name_for_schema(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    property_name: &str,
) -> Result<String, LixError> {
    crate::live_schema_access::payload_column_name_for_schema(
        schema_key,
        schema_definition,
        property_name,
    )
}
