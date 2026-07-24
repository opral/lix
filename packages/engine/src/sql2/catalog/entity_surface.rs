use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use serde_json::Value as JsonValue;

use crate::LixError;
use crate::sql2::history_route::{
    HISTORY_COL_AS_OF_COMMIT_ID, HISTORY_COL_CHANGE_CREATED_AT, HISTORY_COL_CHANGE_ID,
    HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH, HISTORY_COL_ENTITY_PK, HISTORY_COL_FILE_ID,
    HISTORY_COL_IS_DELETED, HISTORY_COL_METADATA, HISTORY_COL_OBSERVED_COMMIT_ID,
    HISTORY_COL_ORIGIN_KEY, HISTORY_COL_SCHEMA_KEY, HISTORY_COL_SNAPSHOT_CONTENT,
};
use crate::sql2::result_metadata::{json_field, mark_json_field};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntitySurfaceShape {
    Active,
    ByBranch,
    History,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntityColumnType {
    String,
    Json,
    Integer,
    Number,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EntitySurfaceColumn {
    pub(crate) name: String,
    pub(crate) column_type: EntityColumnType,
    pub(crate) read_nullable: bool,
    pub(crate) insert_required: bool,
    pub(crate) default_expression: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EntitySurfaceSpec {
    pub(crate) schema_key: String,
    pub(crate) primary_key_paths: Vec<Vec<String>>,
    pub(crate) columns: Vec<EntitySurfaceColumn>,
    pub(crate) defaults: crate::catalog::DefaultPlan,
}

impl EntitySurfaceSpec {
    #[cfg(test)]
    pub(crate) fn visible_column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|column| column.name.as_str())
    }

    pub(crate) fn visible_column(&self, column_name: &str) -> Option<&EntitySurfaceColumn> {
        self.columns
            .iter()
            .find(|column| column.name == column_name)
    }
}

pub(crate) fn derive_entity_surface_spec_from_schema(
    schema: &JsonValue,
) -> Result<EntitySurfaceSpec, LixError> {
    let schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "schema is missing string x-lix-key".to_string(),
            )
        })?;

    let properties = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("schema '{schema_key}' must define object properties"),
            )
        })?;
    let required = schema
        .get("required")
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
        .filter_map(JsonValue::as_str)
        .collect::<BTreeSet<_>>();
    let primary_key_paths = parse_primary_key_paths(schema)?;
    let primary_key_roots = primary_key_paths
        .iter()
        .filter_map(|path| path.first())
        .collect::<BTreeSet<_>>();

    let mut columns = properties
        .iter()
        .filter(|(key, _)| !key.starts_with("lixcol_"))
        .map(|(key, property_schema)| {
            let column_type = entity_column_type_from_schema(property_schema).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "schema '{schema_key}' property '/{key}' must declare a SQL-projectable JSON Schema type"
                    ),
                )
                .with_hint("Use an explicit type such as string, number, integer, boolean, object, array, or a supported union of those types.")
            })?;
            Ok(EntitySurfaceColumn {
                name: key.clone(),
                column_type,
                read_nullable: !primary_key_roots.contains(key)
                    && (!required.contains(key.as_str())
                        || entity_schema_allows_null(property_schema)),
                insert_required: required.contains(key.as_str()),
                default_expression: property_schema
                    .get("x-lix-default")
                    .map(lix_default_expression)
                    .or_else(|| {
                        property_schema
                            .get("default")
                            .map(json_schema_default_expression)
                    }),
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    columns.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(EntitySurfaceSpec {
        schema_key: schema_key.to_string(),
        primary_key_paths,
        columns,
        defaults: crate::catalog::DefaultPlan::from_schema(schema),
    })
}

pub(crate) fn schema_exposed_as_entity_surface(schema_key: &str) -> bool {
    !matches!(schema_key, "lix_active_account" | "lix_change")
}

pub(crate) fn schema_exposed_as_entity_history_surface(schema_key: &str) -> bool {
    !matches!(schema_key, "lix_commit" | "lix_commit_edge")
}

pub(crate) fn entity_surface_schema(
    spec: &EntitySurfaceSpec,
    shape: EntitySurfaceShape,
) -> SchemaRef {
    let history_identity_roots = if shape == EntitySurfaceShape::History {
        spec.primary_key_paths
            .iter()
            .filter_map(|path| path.first())
            .cloned()
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    let mut fields = spec
        .columns
        .iter()
        .map(|column| {
            let read_nullable = if shape == EntitySurfaceShape::History {
                !history_identity_roots.contains(&column.name)
            } else {
                column.read_nullable
            };
            let field = Field::new(
                &column.name,
                arrow_data_type_for_entity_column_type(column.column_type),
                read_nullable,
            );
            if column.column_type == EntityColumnType::Json {
                mark_json_field(field)
            } else {
                field
            }
        })
        .collect::<Vec<_>>();

    fields.extend(entity_system_fields(shape));
    Arc::new(Schema::new(fields))
}

pub(crate) fn entity_system_fields(shape: EntitySurfaceShape) -> Vec<Field> {
    if shape == EntitySurfaceShape::History {
        return vec![
            json_field(HISTORY_COL_ENTITY_PK, false),
            Field::new(HISTORY_COL_SCHEMA_KEY, DataType::Utf8, false),
            Field::new(HISTORY_COL_FILE_ID, DataType::Utf8, true),
            json_field(HISTORY_COL_SNAPSHOT_CONTENT, true),
            json_field(HISTORY_COL_METADATA, true),
            Field::new(HISTORY_COL_CHANGE_ID, DataType::Utf8, false),
            Field::new(HISTORY_COL_CHANGE_CREATED_AT, DataType::Utf8, false),
            Field::new(HISTORY_COL_ORIGIN_KEY, DataType::Utf8, true),
            Field::new(HISTORY_COL_OBSERVED_COMMIT_ID, DataType::Utf8, false),
            Field::new(HISTORY_COL_COMMIT_CREATED_AT, DataType::Utf8, false),
            Field::new(HISTORY_COL_AS_OF_COMMIT_ID, DataType::Utf8, false),
            Field::new(HISTORY_COL_DEPTH, DataType::Int64, false),
            Field::new(HISTORY_COL_IS_DELETED, DataType::Boolean, false),
        ];
    }

    let mut fields = vec![
        json_field("lixcol_entity_pk", true),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
        Field::new("lixcol_file_id", DataType::Utf8, true),
        json_field("lixcol_snapshot_content", true),
        json_field("lixcol_metadata", true),
        Field::new("lixcol_created_at", DataType::Utf8, true),
        Field::new("lixcol_updated_at", DataType::Utf8, true),
        Field::new("lixcol_global", DataType::Boolean, true),
        Field::new("lixcol_change_id", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, true),
        Field::new("lixcol_untracked", DataType::Boolean, true),
    ];
    if shape == EntitySurfaceShape::ByBranch {
        fields.push(Field::new("lixcol_branch_id", DataType::Utf8, false));
    }
    fields
}

fn parse_primary_key_paths(schema: &JsonValue) -> Result<Vec<Vec<String>>, LixError> {
    let Some(primary_key) = schema.get("x-lix-primary-key") else {
        return Ok(Vec::new());
    };
    let primary_key = primary_key.as_array().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "schema x-lix-primary-key must be an array of JSON Pointers".to_string(),
        )
    })?;

    primary_key
        .iter()
        .enumerate()
        .map(|(index, pointer)| {
            let pointer = pointer.as_str().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("schema x-lix-primary-key entry at index {index} must be a string"),
                )
            })?;
            parse_json_pointer(pointer)
        })
        .collect()
}

// TODO(engine): share JSON Pointer parsing with schema/canonical validation once
// those helpers have a clean module boundary for SQL providers.
fn parse_json_pointer(pointer: &str) -> Result<Vec<String>, LixError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("invalid JSON pointer '{pointer}'"),
        ));
    }
    pointer[1..]
        .split('/')
        .map(decode_json_pointer_segment)
        .collect()
}

fn decode_json_pointer_segment(segment: &str) -> Result<String, LixError> {
    let mut out = String::new();
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') => out.push('~'),
                Some('1') => out.push('/'),
                _ => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid JSON pointer segment '{segment}'"),
                    ));
                }
            }
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

fn entity_column_type_from_schema(schema: &JsonValue) -> Option<EntityColumnType> {
    let mut kinds = BTreeSet::new();
    collect_entity_type_kinds(schema, &mut kinds);
    kinds.remove("null");

    if kinds.is_empty() {
        return None;
    }

    if kinds.len() == 1 {
        return match kinds.into_iter().next() {
            Some("boolean") => Some(EntityColumnType::Boolean),
            Some("integer") => Some(EntityColumnType::Integer),
            Some("number") => Some(EntityColumnType::Number),
            Some("string") => Some(EntityColumnType::String),
            Some("object" | "array") => Some(EntityColumnType::Json),
            _ => None,
        };
    }

    Some(EntityColumnType::Json)
}

fn entity_schema_allows_null(schema: &JsonValue) -> bool {
    let mut kinds = BTreeSet::new();
    collect_entity_type_kinds(schema, &mut kinds);
    kinds.contains("null")
}

fn lix_default_expression(default: &JsonValue) -> String {
    match default {
        JsonValue::String(value) => value.clone(),
        value => value.to_string(),
    }
}

fn json_schema_default_expression(default: &JsonValue) -> String {
    match default {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::Bool(value) => value.to_string().to_ascii_uppercase(),
        JsonValue::Number(value) => value.to_string(),
        JsonValue::String(value) => format!("'{}'", value.replace('\'', "''")),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            format!("lix_json('{}')", default.to_string().replace('\'', "''"))
        }
    }
}

fn arrow_data_type_for_entity_column_type(column_type: EntityColumnType) -> DataType {
    match column_type {
        EntityColumnType::String | EntityColumnType::Json => DataType::Utf8,
        EntityColumnType::Integer => DataType::Int64,
        EntityColumnType::Number => DataType::Float64,
        EntityColumnType::Boolean => DataType::Boolean,
    }
}

fn collect_entity_type_kinds<'a>(schema: &'a JsonValue, out: &mut BTreeSet<&'a str>) {
    match schema.get("type") {
        Some(JsonValue::String(kind)) => {
            out.insert(kind.as_str());
        }
        Some(JsonValue::Array(kinds)) => {
            for kind in kinds.iter().filter_map(JsonValue::as_str) {
                out.insert(kind);
            }
        }
        _ => {}
    }

    for keyword in ["anyOf", "oneOf", "allOf"] {
        if let Some(JsonValue::Array(branches)) = schema.get(keyword) {
            for branch in branches {
                collect_entity_type_kinds(branch, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        EntitySurfaceShape, derive_entity_surface_spec_from_schema, entity_surface_schema,
    };

    #[test]
    fn history_identity_roots_are_non_null_even_for_nested_keys() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "localized_document",
            "x-lix-primary-key": ["/identity/tenant", "/identity/id", "/locale"],
            "type": "object",
            "properties": {
                "identity": {
                    "type": "object",
                    "properties": {
                        "tenant": { "type": "string" },
                        "id": { "type": "string" }
                    },
                    "required": ["tenant", "id"]
                },
                "locale": { "type": "string" },
                "body": { "type": "string" }
            },
            "required": ["identity", "locale", "body"]
        }))
        .expect("schema should derive");

        let history = entity_surface_schema(&spec, EntitySurfaceShape::History);
        assert!(
            !history
                .field_with_name("identity")
                .expect("nested identity root")
                .is_nullable()
        );
        assert!(
            !history
                .field_with_name("locale")
                .expect("top-level identity")
                .is_nullable()
        );
        assert!(
            history
                .field_with_name("body")
                .expect("payload")
                .is_nullable()
        );

        let active = entity_surface_schema(&spec, EntitySurfaceShape::Active);
        assert!(
            !active
                .field_with_name("identity")
                .expect("active identity input")
                .is_nullable(),
            "read nullability is independent from omission/default input semantics"
        );
    }
}
