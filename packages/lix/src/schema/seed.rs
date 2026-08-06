use serde_json::Value as JsonValue;

pub(crate) fn is_seed_schema_key(schema_key: &str) -> bool {
    super::builtin::is_seed_schema_key(schema_key)
}

pub(crate) fn seed_schema_definition(schema_key: &str) -> Option<&'static JsonValue> {
    super::builtin::seed_schema_definition(schema_key)
}

pub(crate) fn seed_schema_definitions() -> Vec<&'static JsonValue> {
    super::builtin::seed_schema_definitions()
}
