use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use serde_json::Value as JsonValue;

use crate::LixError;
use crate::entity_pk::EntityPkComponentType;
use crate::sql2::history_route::{
    HISTORY_COL_AS_OF_COMMIT_ID, HISTORY_COL_CHANGE_CREATED_AT, HISTORY_COL_CHANGE_ID,
    HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH, HISTORY_COL_ENTITY_PK, HISTORY_COL_FILE_ID,
    HISTORY_COL_IS_DELETED, HISTORY_COL_METADATA, HISTORY_COL_OBSERVED_COMMIT_ID,
    HISTORY_COL_ORIGIN_KEY, HISTORY_COL_SCHEMA_KEY,
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
    pub(crate) primary_key_component_types: Vec<EntityPkComponentType>,
    pub(crate) columns: Vec<EntitySurfaceColumn>,
    pub(crate) defaults: crate::catalog::DefaultPlan,
    /// Columns this schema already declares as a foreign key or as unique,
    /// which are therefore the columns the hot index plane can serve.
    ///
    /// Derived from `x-lix-foreign-keys` and `x-lix-unique` so indexing adds no
    /// user-facing concept. Order is stable and defines each column's ordinal
    /// in the index key, so it must not be reordered without retiring the
    /// index namespace.
    pub(crate) indexed_columns: Vec<EntityIndexedColumn>,
    /// Whether changing one row can invalidate another row.
    ///
    /// Homogeneous point updates may be lowered into one physical write batch
    /// only when every row is independent. JSON Schema validation is row-local;
    /// uniqueness and foreign-key declarations are the inter-row exceptions.
    pub(crate) has_inter_row_constraints: bool,
    /// This exact schema shape proves that replacing `value` while preserving
    /// the already-valid string `path` produces complete valid row content.
    pub(crate) certifies_path_value_replacement: bool,
    /// Every accepted snapshot is exactly the declared, required top-level
    /// columns, so typed Arrow values can reconstruct canonical JSON without
    /// a second persisted snapshot payload.
    pub(crate) columnar_snapshot_bijective: bool,
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

    /// Stable identity of the registered schema properties that determine an
    /// entity columnar sidecar's physical meaning.
    ///
    /// In particular, String and Json both use Arrow Utf8. A name/type-only
    /// comparison cannot distinguish scalar string bytes from canonical JSON
    /// text after a registered-schema amendment.
    pub(crate) fn columnar_layout_fingerprint(&self) -> String {
        fn update_part(hasher: &mut blake3::Hasher, bytes: &[u8]) {
            hasher.update(&(bytes.len() as u64).to_be_bytes());
            hasher.update(bytes);
        }

        let mut hasher = blake3::Hasher::new_derive_key("lix entity columnar layout v1");
        update_part(&mut hasher, self.schema_key.as_bytes());
        hasher.update(&(self.columns.len() as u64).to_be_bytes());
        for column in &self.columns {
            update_part(&mut hasher, column.name.as_bytes());
            hasher.update(&[match column.column_type {
                EntityColumnType::String => 1,
                EntityColumnType::Json => 2,
                EntityColumnType::Integer => 3,
                EntityColumnType::Number => 4,
                EntityColumnType::Boolean => 5,
            }]);
            hasher.update(&[u8::from(column.read_nullable)]);
        }
        hasher.update(&(self.primary_key_paths.len() as u64).to_be_bytes());
        for path in &self.primary_key_paths {
            hasher.update(&(path.len() as u64).to_be_bytes());
            for segment in path {
                update_part(&mut hasher, segment.as_bytes());
            }
        }
        hasher.update(&(self.primary_key_component_types.len() as u64).to_be_bytes());
        for component_type in &self.primary_key_component_types {
            hasher.update(&[match component_type {
                EntityPkComponentType::Uuid => 1,
                EntityPkComponentType::Integer => 2,
                EntityPkComponentType::String => 3,
                EntityPkComponentType::Bytes => 4,
            }]);
        }
        hasher.finalize().to_hex().to_string()
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
    let primary_key_component_types = primary_key_paths
        .iter()
        .map(|path| {
            let [property] = path.as_slice() else {
                return EntityPkComponentType::String;
            };
            let property = properties
                .get(property)
                .expect("validated primary-key property must exist");
            if property.get("type").and_then(JsonValue::as_str) == Some("integer") {
                EntityPkComponentType::Integer
            } else if property.get("format").and_then(JsonValue::as_str) == Some("uuid") {
                EntityPkComponentType::Uuid
            } else if property.get("contentEncoding").and_then(JsonValue::as_str) == Some("base64")
            {
                EntityPkComponentType::Bytes
            } else {
                EntityPkComponentType::String
            }
        })
        .collect();
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

    let certifies_path_value_replacement = certifies_path_value_replacement(schema);
    let columnar_snapshot_bijective = schema.get("additionalProperties")
        == Some(&JsonValue::Bool(false))
        && properties.len() == columns.len()
        && columns.iter().all(|column| {
            column.insert_required
                && matches!(
                    column.column_type,
                    EntityColumnType::String
                        | EntityColumnType::Integer
                        | EntityColumnType::Boolean
                )
        });
    let indexed_columns = derive_indexed_columns(schema, &columns, &primary_key_paths);
    Ok(EntitySurfaceSpec {
        schema_key: schema_key.to_string(),
        primary_key_paths,
        primary_key_component_types,
        indexed_columns,
        columns,
        defaults: crate::catalog::DefaultPlan::from_schema(schema),
        has_inter_row_constraints: ["x-lix-unique", "x-lix-foreign-keys"].into_iter().any(
            |keyword| {
                schema
                    .get(keyword)
                    .and_then(JsonValue::as_array)
                    .is_some_and(|values| !values.is_empty())
            },
        ),
        certifies_path_value_replacement,
        columnar_snapshot_bijective,
    })
}

/// One column the hot index plane can serve, and its position in the index key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EntityIndexedColumn {
    pub(crate) name: String,
    pub(crate) ordinal: u16,
    pub(crate) column_type: EntityColumnType,
}

/// Indexable columns, from declarations the schema already carries.
///
/// Deliberately narrow, and every exclusion is a case the collection scan still
/// serves correctly:
///
/// - single-column groups only — a composite group needs a composite key
///   encoding and no measured workload asks for one yet;
/// - `String` and `Integer` only — those are the types with an order-preserving
///   key encoding;
/// - primary-key columns are skipped — the hot row key already indexes them,
///   and a second access path to the same rows would be a second mechanism.
fn derive_indexed_columns(
    schema: &JsonValue,
    columns: &[EntitySurfaceColumn],
    primary_key_paths: &[Vec<String>],
) -> Vec<EntityIndexedColumn> {
    let mut names = Vec::new();
    let mut push = |name: &str| {
        if !names.iter().any(|existing: &String| existing == name) {
            names.push(name.to_string());
        }
    };
    for group in schema
        .get("x-lix-unique")
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
    {
        if let Some([pointer]) = group.as_array().map(Vec::as_slice)
            && let Some(name) = pointer.as_str().and_then(single_property_pointer)
        {
            push(name);
        }
    }
    for foreign_key in schema
        .get("x-lix-foreign-keys")
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
    {
        if let Some([pointer]) = foreign_key
            .get("properties")
            .and_then(JsonValue::as_array)
            .map(Vec::as_slice)
            && let Some(name) = pointer.as_str().and_then(single_property_pointer)
        {
            push(name);
        }
    }
    names.sort();
    names
        .into_iter()
        .filter(|name| {
            !primary_key_paths
                .iter()
                .any(|path| matches!(path.as_slice(), [component] if component == name))
        })
        .filter_map(|name| {
            let column = columns.iter().find(|column| column.name == name)?;
            matches!(
                column.column_type,
                EntityColumnType::String | EntityColumnType::Integer
            )
            .then(|| EntityIndexedColumn {
                name,
                ordinal: 0,
                column_type: column.column_type,
            })
        })
        .enumerate()
        .map(|(ordinal, column)| EntityIndexedColumn {
            ordinal: u16::try_from(ordinal).unwrap_or(u16::MAX),
            ..column
        })
        .collect()
}

/// `"/name"` for a top-level property, `None` for anything nested.
fn single_property_pointer(pointer: &str) -> Option<&str> {
    let name = pointer.strip_prefix('/')?;
    (!name.is_empty() && !name.contains('/')).then_some(name)
}

fn certifies_path_value_replacement(schema: &JsonValue) -> bool {
    let Some(object) = schema.as_object() else {
        return false;
    };
    let allowed_schema_keys = [
        "$id",
        "$schema",
        "$comment",
        "title",
        "description",
        "type",
        "properties",
        "required",
        "additionalProperties",
        "x-lix-key",
        "x-lix-primary-key",
    ];
    if object
        .keys()
        .any(|key| !allowed_schema_keys.contains(&key.as_str()))
        || object.get("type").and_then(JsonValue::as_str) != Some("object")
        || object.get("additionalProperties") != Some(&JsonValue::Bool(false))
    {
        return false;
    }
    let Some(properties) = object.get("properties").and_then(JsonValue::as_object) else {
        return false;
    };
    if properties.len() != 2
        || !properties.contains_key("path")
        || !properties
            .get("path")
            .is_some_and(schema_accepts_string_identity)
        || !properties
            .get("value")
            .is_some_and(schema_accepts_every_json_value)
    {
        return false;
    }
    let required = object
        .get("required")
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
        .filter_map(JsonValue::as_str)
        .collect::<BTreeSet<_>>();
    required == BTreeSet::from(["path", "value"])
        && object
            .get("x-lix-primary-key")
            .and_then(JsonValue::as_array)
            .is_some_and(|paths| paths.as_slice() == [JsonValue::String("/path".to_string())])
}

fn schema_accepts_string_identity(schema: &JsonValue) -> bool {
    schema
        .as_object()
        .and_then(|schema| schema.get("type"))
        .and_then(JsonValue::as_str)
        == Some("string")
}

fn schema_accepts_every_json_value(schema: &JsonValue) -> bool {
    if schema == &JsonValue::Bool(true) {
        return true;
    }
    let Some(schema) = schema.as_object() else {
        return false;
    };
    let annotations = ["$comment", "title", "description", "default", "examples"];
    if schema.is_empty() || schema.keys().all(|key| annotations.contains(&key.as_str())) {
        return true;
    }
    let validation_keys = schema
        .keys()
        .filter(|key| !annotations.contains(&key.as_str()))
        .collect::<Vec<_>>();
    if validation_keys.len() == 1 && validation_keys[0].as_str() == "type" {
        let Some(types) = schema.get("type").and_then(JsonValue::as_array) else {
            return false;
        };
        let accepted = types
            .iter()
            .filter_map(JsonValue::as_str)
            .collect::<BTreeSet<_>>();
        return accepted
            == BTreeSet::from([
                "array", "boolean", "integer", "null", "number", "object", "string",
            ]);
    }
    if validation_keys.len() != 1 || validation_keys[0].as_str() != "anyOf" {
        return false;
    }
    let Some(branches) = schema.get("anyOf").and_then(JsonValue::as_array) else {
        return false;
    };
    let accepted = branches
        .iter()
        .filter_map(|branch| {
            let branch = branch.as_object()?;
            branch
                .keys()
                .all(|key| key == "type" || annotations.contains(&key.as_str()))
                .then(|| branch.get("type")?.as_str())
                .flatten()
        })
        .collect::<BTreeSet<_>>();
    accepted == BTreeSet::from(["array", "boolean", "null", "number", "object", "string"])
}

pub(crate) fn schema_exposed_as_entity_surface(schema_key: &str) -> bool {
    !matches!(
        schema_key,
        "lix_binary_blob_ref"
            | "lix_change"
            | "lix_directory_descriptor"
            | "lix_file_descriptor"
            | "lix_undo_redo_marker"
            | "lix_collection_generation"
    )
}

pub(crate) fn schema_exposed_as_entity_history_surface(schema_key: &str) -> bool {
    schema_exposed_as_entity_surface(schema_key)
        && !matches!(schema_key, "lix_commit" | "lix_commit_edge")
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

pub(crate) fn entity_visible_fields(spec: &EntitySurfaceSpec) -> Vec<Field> {
    entity_surface_schema(spec, EntitySurfaceShape::Active).fields()[..spec.columns.len()]
        .iter()
        .map(|field| field.as_ref().clone())
        .collect()
}

pub(crate) fn entity_system_fields(shape: EntitySurfaceShape) -> Vec<Field> {
    if shape == EntitySurfaceShape::History {
        return vec![
            json_field(HISTORY_COL_ENTITY_PK, false),
            Field::new(HISTORY_COL_SCHEMA_KEY, DataType::Utf8, false),
            Field::new(HISTORY_COL_FILE_ID, DataType::Utf8, true),
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

pub(crate) fn arrow_data_type_for_entity_column_type(column_type: EntityColumnType) -> DataType {
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

    fn path_value_schema(value_schema: serde_json::Value) -> serde_json::Value {
        json!({
            "x-lix-key": "arbitrary_name",
            "x-lix-primary-key": ["/path"],
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "value": value_schema
            },
            "required": ["path", "value"],
            "additionalProperties": false
        })
    }

    #[test]
    fn certifies_complete_path_value_rows_when_value_accepts_all_json() {
        let spec = derive_entity_surface_spec_from_schema(&path_value_schema(json!({
            "anyOf": [
                { "type": "object" },
                { "type": "array" },
                { "type": "string" },
                { "type": "number" },
                { "type": "boolean" },
                { "type": "null" }
            ]
        })))
        .expect("schema should derive");

        assert!(spec.certifies_path_value_replacement);
    }

    #[test]
    fn columnar_snapshot_certificate_requires_exact_reversible_columns() {
        let strings = derive_entity_surface_spec_from_schema(&path_value_schema(json!({
            "type": "string"
        })))
        .expect("string schema should derive");
        assert!(strings.columnar_snapshot_bijective);

        let numbers = derive_entity_surface_spec_from_schema(&path_value_schema(json!({
            "type": "number"
        })))
        .expect("number schema should derive");
        assert!(!numbers.columnar_snapshot_bijective);

        let mut reserved = path_value_schema(json!({ "type": "string" }));
        reserved["properties"]["lixcol_user_value"] = json!({ "type": "string" });
        reserved["required"] = json!(["path", "value", "lixcol_user_value"]);
        let reserved = derive_entity_surface_spec_from_schema(&reserved)
            .expect("reserved-name schema should still derive");
        assert!(!reserved.columnar_snapshot_bijective);
    }

    #[test]
    fn does_not_certify_path_value_rows_with_value_constraints() {
        let spec = derive_entity_surface_spec_from_schema(&path_value_schema(json!({
            "type": "object"
        })))
        .expect("schema should derive");
        assert!(!spec.certifies_path_value_replacement);

        let mut schema = path_value_schema(json!({
            "anyOf": [
                { "type": "object" },
                { "type": "array" },
                { "type": "string" },
                { "type": "number" },
                { "type": "boolean" },
                { "type": "null" }
            ]
        }));
        schema
            .as_object_mut()
            .expect("object schema")
            .insert("minProperties".to_string(), json!(3));
        let spec = derive_entity_surface_spec_from_schema(&schema).expect("schema should derive");
        assert!(!spec.certifies_path_value_replacement);
    }

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

    #[test]
    fn columnar_layout_fingerprint_distinguishes_string_from_json_utf8() {
        let string_spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "payload",
            "type": "object",
            "properties": { "value": { "type": "string" } }
        }))
        .expect("string spec");
        let json_spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "payload",
            "type": "object",
            "properties": { "value": { "type": ["string", "object"] } }
        }))
        .expect("json spec");

        assert_ne!(
            string_spec.columnar_layout_fingerprint(),
            json_spec.columnar_layout_fingerprint()
        );
        assert_eq!(
            string_spec.columnar_layout_fingerprint(),
            string_spec.clone().columnar_layout_fingerprint()
        );
    }

    /// The load-bearing bridge under every certificate fast path: a column is
    /// only indexable because the schema declared it through `x-lix-unique` or
    /// `x-lix-foreign-keys`, and those are exactly the declarations that set
    /// `has_inter_row_constraints`.
    ///
    /// Four write certificates in `bound_public_write.rs` decline outright on
    /// `has_inter_row_constraints`. This implication is what turns those four
    /// bails into a proof that no row carrying an indexed column can reach
    /// commit without passing through transaction validation, where the hot
    /// index values are extracted.
    #[test]
    fn indexed_columns_imply_inter_row_constraints() {
        let cases = [
            json!({
                "x-lix-key": "bypass_pk_only",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": { "id": { "type": "string" }, "payload": { "type": "string" } }
            }),
            json!({
                "x-lix-key": "bypass_unique",
                "x-lix-primary-key": ["/id"],
                "x-lix-unique": [["/slug"]],
                "type": "object",
                "properties": { "id": { "type": "string" }, "slug": { "type": "string" } }
            }),
            json!({
                "x-lix-key": "bypass_fk",
                "x-lix-primary-key": ["/id"],
                "x-lix-foreign-keys": [{
                    "properties": ["/parentId"],
                    "references": { "schemaKey": "bypass_pk_only", "properties": ["/id"] }
                }],
                "type": "object",
                "properties": { "id": { "type": "string" }, "parentId": { "type": "string" } }
            }),
            json!({
                "x-lix-key": "bypass_composite_unique",
                "x-lix-primary-key": ["/id"],
                "x-lix-unique": [["/a", "/b"]],
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "a": { "type": "string" },
                    "b": { "type": "string" }
                }
            }),
        ];
        for schema in cases {
            let spec = derive_entity_surface_spec_from_schema(&schema).expect("spec");
            assert!(
                spec.indexed_columns.is_empty() || spec.has_inter_row_constraints,
                "{} declares indexed columns without inter-row constraints",
                spec.schema_key
            );
        }

        let unique = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "bypass_unique",
            "x-lix-primary-key": ["/id"],
            "x-lix-unique": [["/slug"]],
            "type": "object",
            "properties": { "id": { "type": "string" }, "slug": { "type": "string" } }
        }))
        .expect("spec");
        assert_eq!(
            unique.indexed_columns.len(),
            1,
            "a single-column unique group is the indexable shape this relies on"
        );
    }
}
