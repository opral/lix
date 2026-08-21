//! Host-side conversion of v68 JSON row snapshots to v69 native typed rows.
//!
//! This module is deliberately pure: callers own the physical v68 read and
//! v69 write. A compiled plan can be reused for every row of one schema.

use std::collections::BTreeSet;

use lix_schema::{CompiledSchema, DataType, Jsonb, Row, Schema, Value};
use serde_json::Value as JsonValue;

use crate::plugin::wire::typed::encode_native_row_payload;
use crate::row_pk::RowPk;

/// The result needed by a physical v68 -> v69 changelog rewrite.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ConvertedRow {
    pub(crate) row: Row,
    /// Primary-key values in Schema v1 declaration order.
    pub(crate) primary_key: Vec<Value>,
    pub(crate) row_pk: RowPk,
    pub(crate) durable_payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConversionError(String);

impl ConversionError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ConversionError {}

#[derive(Debug, Clone)]
struct ColumnPlan {
    name: String,
    old_type: DataType,
    target_type: DataType,
    old_nullable: bool,
    default_value: Option<JsonValue>,
}

#[derive(Debug, Clone)]
struct AddedColumnPlan {
    name: String,
    data_type: DataType,
    nullable: bool,
    default_value: Option<JsonValue>,
    default_expression: Option<String>,
}

/// Reusable conversion plan for one old/target Schema v1 pair.
#[derive(Debug, Clone)]
pub(crate) struct ConversionPlan {
    schema_key: String,
    columns: Vec<ColumnPlan>,
    added_columns: Vec<AddedColumnPlan>,
    primary_key: Vec<String>,
    target: CompiledSchema,
    target_fingerprint: [u8; 32],
    engine_compact: bool,
}

impl ConversionPlan {
    pub(crate) fn compile(old: &Schema, target: &Schema) -> Result<Self, ConversionError> {
        old.validate()
            .map_err(|error| ConversionError::new(format!("invalid v68 schema: {error}")))?;
        target
            .validate()
            .map_err(|error| ConversionError::new(format!("invalid target schema: {error}")))?;
        if old.key != target.key {
            return Err(ConversionError::new(format!(
                "schema key changed from '{}' to '{}'",
                old.key, target.key
            )));
        }
        if old.primary_key != target.primary_key {
            return Err(ConversionError::new(format!(
                "schema '{}' changed its primary key during v68 -> v69 migration",
                old.key
            )));
        }

        if old.columns.len() > target.columns.len() {
            return Err(ConversionError::new(format!(
                "schema '{}' removed columns during v68 -> v69 migration",
                old.key
            )));
        }
        if old.unique != target.unique || old.foreign_keys != target.foreign_keys {
            return Err(ConversionError::new(format!(
                "schema '{}' changed constraints during v68 -> v69 migration",
                old.key
            )));
        }

        let mut columns = Vec::with_capacity(target.columns.len());
        for (old_column, target_column) in old.columns.iter().zip(&target.columns) {
            if old_column.name != target_column.name {
                return Err(ConversionError::new(format!(
                    "schema '{}' reordered or renamed columns during v68 -> v69 migration",
                    old.key
                )));
            }
            if old_column.nullable != target_column.nullable
                || old_column.default_value != target_column.default_value
                || old_column.default_expression != target_column.default_expression
            {
                return Err(ConversionError::new(format!(
                    "schema '{}.{}' changed nullability or defaults during v68 -> v69 migration",
                    old.key, target_column.name
                )));
            }
            if old_column.data_type != target_column.data_type
                && !known_type_transition(
                    &old.key,
                    &target_column.name,
                    old_column.data_type,
                    target_column.data_type,
                )
            {
                return Err(ConversionError::new(format!(
                    "unsupported v68 -> v69 type change for '{}.{}': {} -> {}",
                    old.key,
                    target_column.name,
                    old_column.data_type.postgres_name(),
                    target_column.data_type.postgres_name()
                )));
            }
            columns.push(ColumnPlan {
                name: target_column.name.clone(),
                old_type: old_column.data_type,
                target_type: target_column.data_type,
                old_nullable: old_column.nullable,
                default_value: target_column.default_value.clone(),
            });
        }
        let added_columns = target
            .columns
            .iter()
            .skip(old.columns.len())
            .map(|column| AddedColumnPlan {
                name: column.name.clone(),
                data_type: column.data_type,
                nullable: column.nullable,
                default_value: column.default_value.clone(),
                default_expression: column.default_expression.clone(),
            })
            .collect::<Vec<_>>();

        let target_fingerprint = *target
            .wire_fingerprint()
            .map_err(|error| {
                ConversionError::new(format!("failed to fingerprint target schema: {error}"))
            })?
            .as_bytes();
        let engine_compact = crate::schema::seed_schema_definition(&target.key)
            .and_then(|value| lix_schema::from_value(value.clone()).ok())
            .and_then(|schema| schema.wire_fingerprint().ok())
            .is_some_and(|fingerprint| fingerprint.as_bytes() == &target_fingerprint);
        let target = CompiledSchema::compile(target).map_err(|error| {
            ConversionError::new(format!("failed to compile target schema: {error}"))
        })?;
        Ok(Self {
            schema_key: old.key.clone(),
            columns,
            added_columns,
            primary_key: old.primary_key.clone(),
            target,
            target_fingerprint,
            engine_compact,
        })
    }

    /// Converts one complete v68 outer row object and verifies its durable
    /// identity against the `RowPk` carried by the v68 record envelope.
    pub(crate) fn convert(
        &self,
        outer_row: &JsonValue,
        expected_row_pk: &RowPk,
    ) -> Result<ConvertedRow, ConversionError> {
        let object = outer_row.as_object().ok_or_else(|| {
            ConversionError::new(format!(
                "v68 row for schema '{}' must be a JSON object",
                self.schema_key
            ))
        })?;
        let declared = self
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<BTreeSet<_>>();
        if let Some(name) = object.keys().find(|name| !declared.contains(name.as_str())) {
            return Err(column_error(&self.schema_key, name, "unknown column"));
        }

        let mut row = Row::with_capacity(self.columns.len());
        for column in &self.columns {
            let value = match object.get(&column.name) {
                Some(value) => convert_column(&self.schema_key, column, value)?,
                None if column.default_value.is_some() => convert_column(
                    &self.schema_key,
                    column,
                    column
                        .default_value
                        .as_ref()
                        .expect("guarded literal default"),
                )?,
                None if column.old_nullable => Value::Null,
                None => {
                    return Err(column_error(
                        &self.schema_key,
                        &column.name,
                        "missing non-null column",
                    ));
                }
            };
            row.insert(column.name.clone(), value);
        }
        for column in &self.added_columns {
            let value = if let Some(default) = column.default_value.as_ref() {
                convert_column(
                    &self.schema_key,
                    &ColumnPlan {
                        name: column.name.clone(),
                        old_type: column.data_type,
                        target_type: column.data_type,
                        old_nullable: column.nullable,
                        default_value: column.default_value.clone(),
                    },
                    default,
                )?
            } else if column.nullable {
                Value::Null
            } else if let Some(expression) = column.default_expression.as_deref() {
                return Err(column_error(
                    &self.schema_key,
                    &column.name,
                    format!(
                        "cannot reconstruct appended generated default '{expression}' for a historical row"
                    ),
                ));
            } else {
                return Err(column_error(
                    &self.schema_key,
                    &column.name,
                    "appended column has neither nullability nor a default",
                ));
            };
            row.insert(column.name.clone(), value);
        }
        self.target.validate_complete_row(&row).map_err(|error| {
            ConversionError::new(format!(
                "converted row for schema '{}' is invalid: {error}",
                self.schema_key
            ))
        })?;

        let primary_key = self
            .primary_key
            .iter()
            .map(|name| {
                row.get(name).cloned().ok_or_else(|| {
                    column_error(&self.schema_key, name, "primary-key column is missing")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let row_pk = RowPk::from_schema_values(&primary_key).map_err(|error| {
            ConversionError::new(format!(
                "schema '{}' row has an invalid typed primary key: {error}",
                self.schema_key
            ))
        })?;
        if !expected_row_pk.matches_schema_values(&primary_key) || &row_pk != expected_row_pk {
            return Err(ConversionError::new(format!(
                "schema '{}' row primary key does not match its v68 record envelope",
                self.schema_key
            )));
        }

        let durable_payload = if self.engine_compact {
            let payload = crate::plugin::wire::typed::encode_engine_row_payload(&self.target, &row)
                .map_err(|error| {
                    ConversionError::new(format!(
                        "failed to encode schema '{}' compact engine row payload: {error:?}",
                        self.schema_key
                    ))
                })?;
            crate::plugin::runtime::compress_durable_payload(payload).map_err(|error| {
                ConversionError::new(format!(
                    "failed to compress schema '{}' engine row payload: {error:?}",
                    self.schema_key
                ))
            })?
        } else {
            encode_native_row_payload(&self.target_fingerprint, &primary_key, &row).map_err(
                |error| {
                    ConversionError::new(format!(
                        "failed to encode schema '{}' native row payload: {error:?}",
                        self.schema_key
                    ))
                },
            )?
        };
        Ok(ConvertedRow {
            row,
            primary_key,
            row_pk,
            durable_payload,
        })
    }
}

/// One-shot form for callers that do not need to reuse a compiled plan.
pub(crate) fn convert(
    old: &Schema,
    target: &Schema,
    outer_row: &JsonValue,
    expected_row_pk: &RowPk,
) -> Result<ConvertedRow, ConversionError> {
    ConversionPlan::compile(old, target)?.convert(outer_row, expected_row_pk)
}

fn known_type_transition(
    schema_key: &str,
    column_name: &str,
    old_type: DataType,
    target_type: DataType,
) -> bool {
    (old_type == DataType::Text
        && target_type == DataType::Jsonb
        && matches!(
            (schema_key, column_name),
            ("excalidraw_element", "element_json")
                | ("excalidraw_file", "file_json")
                | ("json_array_item", "scalar_json")
                | ("json_object_member", "scalar_json")
                | ("json_root", "scalar_json")
                | ("markdown_node", "format_json")
                | ("markdown_node", "payload_json")
        ))
        || (schema_key == "markdown_node"
            && column_name == "parent_id"
            && old_type == DataType::Text
            && target_type == DataType::Uuid)
}

fn convert_column(
    schema_key: &str,
    column: &ColumnPlan,
    value: &JsonValue,
) -> Result<Value, ConversionError> {
    // This branch must use the old type. In a nullable text -> jsonb column,
    // outer `null` was SQL NULL, while the text "null" becomes JSONB null.
    if value.is_null() {
        return if column.old_type == DataType::Jsonb {
            Ok(Value::Jsonb(Jsonb::from_value(JsonValue::Null)))
        } else if column.old_nullable {
            Ok(Value::Null)
        } else {
            Err(column_error(
                schema_key,
                &column.name,
                "must not be SQL NULL",
            ))
        };
    }

    if column.old_type == DataType::Text && column.target_type == DataType::Jsonb {
        let text = value
            .as_str()
            .ok_or_else(|| column_error(schema_key, &column.name, "expected v68 text"))?;
        let json = serde_json::from_str(text).map_err(|error| {
            column_error(
                schema_key,
                &column.name,
                format!("invalid JSON text for jsonb conversion: {error}"),
            )
        })?;
        return checked_jsonb(schema_key, &column.name, json);
    }

    match column.target_type {
        DataType::Text => {
            let text = value
                .as_str()
                .ok_or_else(|| column_error(schema_key, &column.name, "expected text"))?;
            if text.contains('\0') {
                return Err(column_error(
                    schema_key,
                    &column.name,
                    "text contains Unicode NUL",
                ));
            }
            Ok(Value::Text(text.to_owned()))
        }
        DataType::Uuid => value
            .as_str()
            .ok_or_else(|| column_error(schema_key, &column.name, "expected UUID text"))
            .and_then(|value| {
                uuid::Uuid::parse_str(value)
                    .map(Value::Uuid)
                    .map_err(|error| column_error(schema_key, &column.name, error.to_string()))
            }),
        DataType::Int8 => value
            .as_i64()
            .map(Value::Int8)
            .ok_or_else(|| column_error(schema_key, &column.name, "expected int8")),
        DataType::Float8 => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(Value::Float8)
            .ok_or_else(|| column_error(schema_key, &column.name, "expected finite float8")),
        DataType::Boolean => value
            .as_bool()
            .map(Value::Boolean)
            .ok_or_else(|| column_error(schema_key, &column.name, "expected boolean")),
        DataType::Jsonb => checked_jsonb(schema_key, &column.name, value.clone()),
        DataType::Timestamptz => value
            .as_str()
            .ok_or_else(|| column_error(schema_key, &column.name, "expected RFC 3339 text"))
            .and_then(|value| {
                chrono::DateTime::parse_from_rfc3339(value)
                    .map(|value| Value::Timestamptz(value.timestamp_micros()))
                    .map_err(|error| column_error(schema_key, &column.name, error.to_string()))
            }),
    }
}

fn checked_jsonb(
    schema_key: &str,
    column_name: &str,
    value: JsonValue,
) -> Result<Value, ConversionError> {
    let value = Jsonb::from_value(value);
    if !value.is_valid() {
        return Err(column_error(
            schema_key,
            column_name,
            "value is not representable as jsonb",
        ));
    }
    Ok(Value::Jsonb(value))
}

fn column_error(
    schema_key: &str,
    column_name: &str,
    message: impl std::fmt::Display,
) -> ConversionError {
    ConversionError::new(format!("{schema_key}.{column_name}: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema(key: &str, payload_type: &str, nullable: bool) -> Schema {
        lix_schema::from_value(json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": key,
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "payload_json", "type": payload_type, "nullable": nullable}
            ],
            "primary_key": ["id"]
        }))
        .expect("test schema")
    }

    #[test]
    fn converts_known_text_jsonb_and_builds_verified_durable_payload() {
        let old = schema("markdown_node", "text", false);
        let target = schema("markdown_node", "jsonb", false);
        let expected = RowPk::single("node-1");

        let converted = convert(
            &old,
            &target,
            &json!({"id": "node-1", "payload_json": "{\"z\":2,\"a\":null}"}),
            &expected,
        )
        .expect("known conversion");

        assert_eq!(converted.row_pk, expected);
        assert_eq!(converted.primary_key, [Value::Text("node-1".to_owned())]);
        assert_eq!(
            converted.row.get("payload_json"),
            Some(&Value::Jsonb(Jsonb::from_value(json!({"z": 2, "a": null}))))
        );
        let decoded = crate::plugin::runtime::WasmTypedRow::decode_durable_payload(
            converted.durable_payload.into(),
            "markdown_node",
            &expected,
        )
        .expect("durable payload should decode against its outer identity");
        assert_eq!(
            decoded.schema_fingerprint,
            *target.wire_fingerprint().unwrap().as_bytes()
        );
        assert_eq!(decoded.row_pk.as_ref(), converted.primary_key);
        assert_eq!(decoded.row, converted.row);
    }

    #[test]
    fn converts_builtin_engine_rows_to_the_compact_v69_payload() {
        let schema = lix_schema::from_value(
            crate::schema::seed_schema_definition("lix_key_value")
                .expect("built-in key/value schema")
                .clone(),
        )
        .expect("built-in schema should parse");
        let expected = RowPk::single("migration-probe");
        let converted = convert(
            &schema,
            &schema,
            &json!({"key": "migration-probe", "value": {"typed": true}}),
            &expected,
        )
        .expect("built-in row should convert");

        assert_eq!(
            converted.durable_payload.first().copied(),
            Some(crate::plugin::wire::typed::ENGINE_ROW_PAYLOAD_VERSION)
        );
        let decoded = crate::plugin::runtime::WasmTypedRow::decode_durable_payload(
            converted.durable_payload.into(),
            "lix_key_value",
            &expected,
        )
        .expect("compact built-in payload should decode");
        assert_eq!(decoded.row, converted.row);
        assert_eq!(
            decoded.schema_fingerprint,
            *schema.wire_fingerprint().unwrap().as_bytes()
        );
    }

    #[test]
    fn preserves_sql_null_and_json_null_across_nullable_text_to_jsonb() {
        let old = schema("markdown_node", "text", true);
        let target = schema("markdown_node", "jsonb", true);
        let plan = ConversionPlan::compile(&old, &target).unwrap();

        let sql_null = plan
            .convert(
                &json!({"id": "sql", "payload_json": null}),
                &RowPk::single("sql"),
            )
            .unwrap();
        let omitted_sql_null = plan
            .convert(&json!({"id": "omitted"}), &RowPk::single("omitted"))
            .unwrap();
        let json_null = plan
            .convert(
                &json!({"id": "json", "payload_json": "null"}),
                &RowPk::single("json"),
            )
            .unwrap();

        assert_eq!(sql_null.row.get("payload_json"), Some(&Value::Null));
        assert_eq!(omitted_sql_null.row.get("payload_json"), Some(&Value::Null));
        assert_eq!(
            json_null.row.get("payload_json"),
            Some(&Value::Jsonb(Jsonb::from_value(JsonValue::Null)))
        );
    }

    #[test]
    fn materializes_nullable_columns_appended_by_a_valid_schema_amendment() {
        let old = lix_schema::from_value(json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "custom_note",
            "columns": [
                {"name": "id", "type": "text", "nullable": false}
            ],
            "primary_key": ["id"]
        }))
        .unwrap();
        let target = lix_schema::from_value(json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "custom_note",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "annotation", "type": "text", "nullable": true}
            ],
            "primary_key": ["id"]
        }))
        .unwrap();

        let converted = convert(
            &old,
            &target,
            &json!({"id": "note-1"}),
            &RowPk::single("note-1"),
        )
        .expect("valid appended nullable column should migrate");
        assert_eq!(converted.row.get("annotation"), Some(&Value::Null));
    }

    #[test]
    fn rejects_unknown_or_malformed_type_changes() {
        let old = schema("custom_schema", "text", false);
        let target = schema("custom_schema", "jsonb", false);
        assert!(
            ConversionPlan::compile(&old, &target)
                .unwrap_err()
                .to_string()
                .contains("unsupported v68 -> v69 type change")
        );

        let old = schema("markdown_node", "text", false);
        let mut target = schema("markdown_node", "jsonb", false);
        target.columns[1].data_type = DataType::Boolean;
        assert!(
            ConversionPlan::compile(&old, &target)
                .unwrap_err()
                .to_string()
                .contains("unsupported v68 -> v69 type change")
        );
    }

    #[test]
    fn rejects_invalid_json_unknown_columns_and_row_pk_mismatch() {
        let old = schema("markdown_node", "text", false);
        let target = schema("markdown_node", "jsonb", false);
        let plan = ConversionPlan::compile(&old, &target).unwrap();

        assert!(
            plan.convert(
                &json!({"id": "node-1", "payload_json": "{"}),
                &RowPk::single("node-1"),
            )
            .unwrap_err()
            .to_string()
            .contains("invalid JSON text")
        );
        assert!(
            plan.convert(
                &json!({"id": "node-1", "payload_json": "{}", "extra": 1}),
                &RowPk::single("node-1"),
            )
            .unwrap_err()
            .to_string()
            .contains("unknown column")
        );
        assert!(
            plan.convert(
                &json!({"id": "node-1", "payload_json": "{}"}),
                &RowPk::single("different"),
            )
            .unwrap_err()
            .to_string()
            .contains("does not match")
        );
    }

    #[test]
    fn recognizes_only_the_bundled_text_to_jsonb_fields() {
        let known = [
            ("excalidraw_element", "element_json"),
            ("excalidraw_file", "file_json"),
            ("json_array_item", "scalar_json"),
            ("json_object_member", "scalar_json"),
            ("json_root", "scalar_json"),
            ("markdown_node", "format_json"),
            ("markdown_node", "payload_json"),
        ];
        for (schema, column) in known {
            assert!(
                known_type_transition(schema, column, DataType::Text, DataType::Jsonb),
                "{schema}.{column}"
            );
        }
        assert!(!known_type_transition(
            "markdown_node",
            "other_json",
            DataType::Text,
            DataType::Jsonb
        ));
        assert!(!known_type_transition(
            "custom",
            "payload_json",
            DataType::Text,
            DataType::Jsonb
        ));
    }
}
