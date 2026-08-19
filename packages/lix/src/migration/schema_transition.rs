//! Pure Schema v1 document transitions needed by the v68 -> v69 hard cut.

use lix_schema::{DataType, Schema};
use serde_json::Value as JsonValue;

/// Schema keys shipped by the ten bundled file-format plugins.
pub(crate) const BUNDLED_PLUGIN_SCHEMA_KEYS: [&str; 10] = [
    "csv_row",
    "csv_table",
    "excalidraw_element",
    "excalidraw_file",
    "excalidraw_scene",
    "json_array_item",
    "json_object_member",
    "json_root",
    "markdown_node",
    "text_line",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaTransitionError(String);

impl SchemaTransitionError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for SchemaTransitionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SchemaTransitionError {}

pub(crate) fn is_bundled_plugin_schema_key(schema_key: &str) -> bool {
    BUNDLED_PLUGIN_SCHEMA_KEYS.contains(&schema_key)
}

/// Returns the v69 form of one historical Schema v1 document.
///
/// Non-bundled schemas are returned unchanged. Bundled schemas permit only
/// the hard-cut type transitions listed in [`target_type`], and this function
/// is idempotent over schemas that have already been transitioned.
pub(crate) fn schema_to_v69(historical: &Schema) -> Result<Schema, SchemaTransitionError> {
    historical.validate().map_err(|error| {
        SchemaTransitionError::new(format!(
            "historical schema '{}' is invalid: {error}",
            historical.key
        ))
    })?;
    if !is_bundled_plugin_schema_key(&historical.key) {
        return Ok(historical.clone());
    }

    let mut transitioned = historical.clone();
    for column in &mut transitioned.columns {
        let Some(expected) = target_type(&transitioned.key, &column.name) else {
            continue;
        };
        let old = expected.old;
        let target = expected.target;
        if column.data_type == old {
            column.data_type = target;
        } else if column.data_type != target {
            return Err(SchemaTransitionError::new(format!(
                "unsupported v68 -> v69 type for '{}.{}': expected {} or {}, found {}",
                transitioned.key,
                column.name,
                old.postgres_name(),
                target.postgres_name(),
                column.data_type.postgres_name()
            )));
        }
    }

    for expected in expected_transitions(&transitioned.key) {
        if !transitioned
            .columns
            .iter()
            .any(|column| column.name == expected.column)
        {
            return Err(SchemaTransitionError::new(format!(
                "bundled schema '{}' is missing transitioned column '{}'",
                transitioned.key, expected.column
            )));
        }
    }
    transitioned.validate().map_err(|error| {
        SchemaTransitionError::new(format!(
            "transitioned schema '{}' is invalid: {error}",
            transitioned.key
        ))
    })?;
    Ok(transitioned)
}

/// Transitions the embedded `value` in a `lix_registered_schema` outer row.
///
/// Rows for non-bundled schema keys are returned byte-for-byte equivalent at
/// the JSON value level without inspecting their embedded schema document.
pub(crate) fn registered_schema_row_to_v69(
    outer_row: &JsonValue,
) -> Result<JsonValue, SchemaTransitionError> {
    let object = outer_row.as_object().ok_or_else(|| {
        SchemaTransitionError::new("lix_registered_schema row must be a JSON object")
    })?;
    let schema_key = object
        .get("schema_key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            SchemaTransitionError::new("lix_registered_schema row is missing string schema_key")
        })?;
    if !is_bundled_plugin_schema_key(schema_key) {
        return Ok(outer_row.clone());
    }

    let value = object.get("value").cloned().ok_or_else(|| {
        SchemaTransitionError::new(format!(
            "lix_registered_schema row for '{schema_key}' is missing value"
        ))
    })?;
    let historical = lix_schema::from_value(value).map_err(|error| {
        SchemaTransitionError::new(format!(
            "registered schema '{schema_key}' is invalid: {error}"
        ))
    })?;
    if historical.key != schema_key {
        return Err(SchemaTransitionError::new(format!(
            "registered schema key '{schema_key}' does not match embedded key '{}'",
            historical.key
        )));
    }
    let transitioned = schema_to_v69(&historical)?;
    let value = serde_json::to_value(transitioned).map_err(|error| {
        SchemaTransitionError::new(format!(
            "failed to serialize transitioned schema '{schema_key}': {error}"
        ))
    })?;
    let mut outer_row = outer_row.clone();
    outer_row
        .as_object_mut()
        .expect("registered schema row was proven to be an object")
        .insert("value".to_owned(), value);
    Ok(outer_row)
}

#[derive(Clone, Copy)]
struct TypeTransition {
    column: &'static str,
    old: DataType,
    target: DataType,
}

const TEXT_TO_JSONB: &[TypeTransition] = &[
    TypeTransition {
        column: "element_json",
        old: DataType::Text,
        target: DataType::Jsonb,
    },
    TypeTransition {
        column: "file_json",
        old: DataType::Text,
        target: DataType::Jsonb,
    },
    TypeTransition {
        column: "scalar_json",
        old: DataType::Text,
        target: DataType::Jsonb,
    },
    TypeTransition {
        column: "format_json",
        old: DataType::Text,
        target: DataType::Jsonb,
    },
    TypeTransition {
        column: "payload_json",
        old: DataType::Text,
        target: DataType::Jsonb,
    },
];

const MARKDOWN_PARENT_ID: TypeTransition = TypeTransition {
    column: "parent_id",
    old: DataType::Text,
    target: DataType::Uuid,
};

fn expected_transitions(schema_key: &str) -> impl Iterator<Item = TypeTransition> {
    TEXT_TO_JSONB
        .iter()
        .copied()
        .filter(move |transition| transition_applies(schema_key, transition.column))
        .chain(
            (schema_key == "markdown_node")
                .then_some(MARKDOWN_PARENT_ID)
                .into_iter(),
        )
}

fn target_type(schema_key: &str, column_name: &str) -> Option<TypeTransition> {
    expected_transitions(schema_key).find(|transition| transition.column == column_name)
}

fn transition_applies(schema_key: &str, column_name: &str) -> bool {
    matches!(
        (schema_key, column_name),
        ("excalidraw_element", "element_json")
            | ("excalidraw_file", "file_json")
            | ("json_array_item", "scalar_json")
            | ("json_object_member", "scalar_json")
            | ("json_root", "scalar_json")
            | ("markdown_node", "format_json")
            | ("markdown_node", "payload_json")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema(key: &str, columns: JsonValue) -> Schema {
        lix_schema::from_value(json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": key,
            "columns": columns,
            "primary_key": ["id"]
        }))
        .expect("test schema")
    }

    fn column_type(schema: &Schema, name: &str) -> DataType {
        schema
            .columns
            .iter()
            .find(|column| column.name == name)
            .expect("test column")
            .data_type
    }

    #[test]
    fn identifies_exactly_the_ten_bundled_plugin_schema_keys() {
        assert_eq!(BUNDLED_PLUGIN_SCHEMA_KEYS.len(), 10);
        for key in BUNDLED_PLUGIN_SCHEMA_KEYS {
            assert!(is_bundled_plugin_schema_key(key), "{key}");
        }
        assert!(!is_bundled_plugin_schema_key("lix_registered_schema"));
        assert!(!is_bundled_plugin_schema_key("custom_schema"));
    }

    #[test]
    fn applies_all_markdown_transitions_and_is_idempotent() {
        let historical = schema(
            "markdown_node",
            json!([
                {"name": "id", "type": "uuid", "nullable": false},
                {"name": "format_json", "type": "text", "nullable": false},
                {"name": "parent_id", "type": "text", "nullable": true},
                {"name": "payload_json", "type": "text", "nullable": false}
            ]),
        );

        let transitioned = schema_to_v69(&historical).expect("transition should succeed");
        assert_eq!(column_type(&transitioned, "format_json"), DataType::Jsonb);
        assert_eq!(column_type(&transitioned, "parent_id"), DataType::Uuid);
        assert_eq!(column_type(&transitioned, "payload_json"), DataType::Jsonb);
        assert_eq!(
            schema_to_v69(&transitioned).expect("transition should be idempotent"),
            transitioned
        );
    }

    #[test]
    fn applies_each_of_the_seven_text_to_jsonb_transitions() {
        let cases = [
            ("excalidraw_element", "element_json"),
            ("excalidraw_file", "file_json"),
            ("json_array_item", "scalar_json"),
            ("json_object_member", "scalar_json"),
            ("json_root", "scalar_json"),
            ("markdown_node", "format_json"),
            ("markdown_node", "payload_json"),
        ];
        for (key, transitioned_column) in cases {
            let mut columns = vec![json!({
                "name": "id",
                "type": "text",
                "nullable": false
            })];
            for transition in expected_transitions(key) {
                columns.push(json!({
                    "name": transition.column,
                    "type": transition.old.postgres_name(),
                    "nullable": transition.column == "scalar_json" || transition.column == "parent_id"
                }));
            }
            let historical = schema(key, JsonValue::Array(columns));
            let transitioned = schema_to_v69(&historical).expect("transition should succeed");
            assert_eq!(
                column_type(&transitioned, transitioned_column),
                DataType::Jsonb,
                "{key}.{transitioned_column}"
            );
        }
    }

    #[test]
    fn leaves_non_bundled_schemas_and_unaffected_bundled_schemas_unchanged() {
        for key in [
            "custom_schema",
            "csv_row",
            "csv_table",
            "excalidraw_scene",
            "text_line",
        ] {
            let historical = schema(
                key,
                json!([{"name": "id", "type": "text", "nullable": false}]),
            );
            assert_eq!(schema_to_v69(&historical).unwrap(), historical, "{key}");
        }
    }

    #[test]
    fn rejects_wrong_types_and_missing_transition_columns() {
        let wrong_type = schema(
            "json_root",
            json!([
                {"name": "id", "type": "text", "nullable": false},
                {"name": "scalar_json", "type": "boolean", "nullable": true}
            ]),
        );
        assert!(
            schema_to_v69(&wrong_type)
                .unwrap_err()
                .to_string()
                .contains("expected text or jsonb, found boolean")
        );

        let missing = schema(
            "excalidraw_file",
            json!([{"name": "id", "type": "text", "nullable": false}]),
        );
        assert!(
            schema_to_v69(&missing)
                .unwrap_err()
                .to_string()
                .contains("missing transitioned column 'file_json'")
        );
    }

    #[test]
    fn transitions_only_the_embedded_value_for_bundled_registered_rows() {
        let historical = schema(
            "json_root",
            json!([
                {"name": "id", "type": "text", "nullable": false},
                {"name": "scalar_json", "type": "text", "nullable": true}
            ]),
        );
        let row = json!({
            "schema_key": "json_root",
            "value": serde_json::to_value(historical).unwrap(),
            "preserved": {"nested": true}
        });

        let transitioned = registered_schema_row_to_v69(&row).unwrap();
        assert_eq!(transitioned["schema_key"], row["schema_key"]);
        assert_eq!(transitioned["preserved"], row["preserved"]);
        let embedded = lix_schema::from_value(transitioned["value"].clone()).unwrap();
        assert_eq!(column_type(&embedded, "scalar_json"), DataType::Jsonb);
        assert_eq!(
            registered_schema_row_to_v69(&transitioned).unwrap(),
            transitioned
        );
    }

    #[test]
    fn does_not_inspect_non_bundled_registered_schema_values() {
        let row = json!({
            "schema_key": "custom_schema",
            "value": {"not": "a Schema v1 document"},
            "preserved": 42
        });
        assert_eq!(registered_schema_row_to_v69(&row).unwrap(), row);
    }

    #[test]
    fn rejects_malformed_or_mismatched_bundled_registered_rows() {
        let missing_value = json!({"schema_key": "json_root"});
        assert!(
            registered_schema_row_to_v69(&missing_value)
                .unwrap_err()
                .to_string()
                .contains("missing value")
        );

        let embedded = schema(
            "json_root",
            json!([
                {"name": "id", "type": "text", "nullable": false},
                {"name": "scalar_json", "type": "text", "nullable": true}
            ]),
        );
        let mismatch = json!({
            "schema_key": "json_array_item",
            "value": serde_json::to_value(embedded).unwrap()
        });
        assert!(
            registered_schema_row_to_v69(&mismatch)
                .unwrap_err()
                .to_string()
                .contains("does not match embedded key 'json_root'")
        );
    }
}
