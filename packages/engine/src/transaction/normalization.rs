use std::collections::BTreeMap;

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::common::normalize_path_segment;
use crate::entity_identity::{EntityIdentity, EntityIdentityError};
use crate::functions::FunctionProviderHandle;
use crate::schema::{
    apply_schema_defaults_with_shared_runtime, is_seed_schema_key,
    reject_unsupported_registered_schema_version, schema_from_registered_snapshot,
    schema_key_from_definition, validate_lix_schema, validate_lix_schema_definition, SchemaKey,
};
use crate::transaction::types::StageRow;
use crate::LixError;

pub(crate) const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";

/// Transaction-local schema catalog used while raw writes are staged.
///
/// Normalization has to happen before rows are keyed in the staged-write map:
/// defaults may fill primary-key fields and primary keys may derive the final
/// entity id. The catalog starts with session-visible schemas and is updated as
/// pending `lix_registered_schema` rows are staged, so later rows in the same
/// transaction can target newly registered schemas.
#[derive(Debug, Clone, Default)]
pub(crate) struct TransactionSchemaCatalog {
    schemas_by_key: BTreeMap<SchemaCatalogKey, JsonValue>,
}

impl TransactionSchemaCatalog {
    pub(crate) fn from_visible_schemas(visible_schemas: &[JsonValue]) -> Result<Self, LixError> {
        let mut catalog = Self::default();
        for schema in visible_schemas {
            let key = schema_key_from_definition(schema)?;
            catalog.insert_schema(key, schema.clone());
        }
        Ok(catalog)
    }

    pub(crate) fn schema(&self, schema_key: &str, schema_version: &str) -> Option<&JsonValue> {
        self.schemas_by_key.get(&SchemaCatalogKey {
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
        })
    }

    pub(crate) fn insert_schema(&mut self, key: SchemaKey, schema: JsonValue) {
        self.schemas_by_key
            .insert(SchemaCatalogKey::from_schema_key(key), schema);
    }

    pub(crate) fn contains(&self, schema_key: &str, schema_version: &str) -> bool {
        self.schemas_by_key.contains_key(&SchemaCatalogKey {
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
        })
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.schemas_by_key.len()
    }

    pub(crate) fn schema_by_key(&self, schema_key: &str) -> Option<&JsonValue> {
        self.schemas_by_key
            .iter()
            .find_map(|(key, schema)| (key.schema_key == schema_key).then_some(schema))
    }

    pub(crate) fn schema_key_by_key(&self, schema_key: &str) -> Option<SchemaCatalogKey> {
        self.schemas_by_key
            .keys()
            .find(|key| key.schema_key == schema_key)
            .cloned()
    }

    pub(crate) fn schemas(&self) -> impl Iterator<Item = (&SchemaCatalogKey, &JsonValue)> {
        self.schemas_by_key.iter()
    }
}

/// Normalizes one incoming row into a row with final snapshot/entity identity.
///
/// This is the canonical schema-semantics boundary for staged writes. It owns
/// schema default application, primary-key identity derivation, and explicit
/// identity mismatch validation. SQL providers should not pre-derive primary
/// keys for schemas that can be normalized here; they should stage decoded
/// snapshots and let this layer complete them.
///
/// This function intentionally does not assign timestamps, change ids, or
/// commit ids; those are transaction hydration fields handled by staging after
/// semantic normalization has produced the final identity.
pub(crate) fn normalize_stage_row(
    mut row: StageRow,
    schema_catalog: &mut TransactionSchemaCatalog,
    functions: FunctionProviderHandle,
) -> Result<StageRow, LixError> {
    validate_stage_row_schema_identity(&row)?;

    let Some(schema) = schema_catalog
        .schema(&row.schema_key, &row.schema_version)
        .cloned()
    else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "schema '{}' version '{}' is not visible to this transaction",
                row.schema_key, row.schema_version
            ),
        ));
    };

    if let Some(snapshot_content) = row.snapshot_content.as_deref() {
        let mut snapshot = parse_snapshot_object(snapshot_content, &row)?;
        apply_defaults(&mut snapshot, &schema, &row, functions)?;
        normalize_filesystem_descriptor_snapshot(&row, &mut snapshot)?;
        let snapshot = JsonValue::Object(snapshot);
        row.entity_id = Some(resolve_entity_id(&row, &schema, &snapshot)?);
        row.snapshot_content = Some(serde_json::to_string(&snapshot).map_err(|error| {
            LixError::new(
                LixError::CODE_UNKNOWN,
                format!(
                    "failed to serialize normalized snapshot_content for schema '{}' version '{}': {error}",
                    row.schema_key, row.schema_version
                ),
            )
        })?);
    } else if row.entity_id.is_none() {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "tombstone for schema '{}' version '{}' requires entity_id",
                row.schema_key, row.schema_version
            ),
        ));
    }

    if row.schema_key == REGISTERED_SCHEMA_KEY {
        remember_pending_registered_schema(row.snapshot_content.as_deref(), schema_catalog)?;
    }

    Ok(row)
}

fn validate_stage_row_schema_identity(row: &StageRow) -> Result<(), LixError> {
    if row.schema_key.is_empty() {
        return Err(LixError::new(
            LixError::CODE_UNKNOWN,
            "engine2 transaction staging requires non-empty schema_key",
        ));
    }
    if row.schema_version.is_empty() {
        return Err(LixError::new(
            LixError::CODE_UNKNOWN,
            "engine2 transaction staging requires non-empty schema_version",
        ));
    }
    Ok(())
}

fn parse_snapshot_object(
    snapshot_content: &str,
    row: &StageRow,
) -> Result<JsonMap<String, JsonValue>, LixError> {
    let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content for schema '{}' version '{}' is invalid JSON: {error}",
                row.schema_key, row.schema_version
            ),
        )
    })?;
    snapshot.as_object().cloned().ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content for schema '{}' version '{}' must be a JSON object",
                row.schema_key, row.schema_version
            ),
        )
    })
}

fn apply_defaults(
    snapshot: &mut JsonMap<String, JsonValue>,
    schema: &JsonValue,
    row: &StageRow,
    functions: FunctionProviderHandle,
) -> Result<(), LixError> {
    apply_schema_defaults_with_shared_runtime(
        snapshot,
        schema,
        functions,
        &row.schema_key,
        &row.schema_version,
    )?;
    Ok(())
}

fn normalize_filesystem_descriptor_snapshot(
    row: &StageRow,
    snapshot: &mut JsonMap<String, JsonValue>,
) -> Result<(), LixError> {
    match row.schema_key.as_str() {
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY => normalize_directory_descriptor_snapshot(row, snapshot),
        FILE_DESCRIPTOR_SCHEMA_KEY => normalize_file_descriptor_snapshot(row, snapshot),
        _ => Ok(()),
    }
}

fn normalize_directory_descriptor_snapshot(
    row: &StageRow,
    snapshot: &mut JsonMap<String, JsonValue>,
) -> Result<(), LixError> {
    let Some(name) = optional_string_field(snapshot, "name", row)? else {
        return Ok(());
    };
    let normalized_name = normalize_path_segment(name)?;
    snapshot.insert("name".to_string(), JsonValue::String(normalized_name));
    Ok(())
}

fn normalize_file_descriptor_snapshot(
    row: &StageRow,
    snapshot: &mut JsonMap<String, JsonValue>,
) -> Result<(), LixError> {
    let Some(name) = optional_string_field(snapshot, "name", row)? else {
        return Ok(());
    };
    let normalized_name = normalize_path_segment(name)?;
    snapshot.insert("name".to_string(), JsonValue::String(normalized_name));
    Ok(())
}

fn optional_string_field<'a>(
    snapshot: &'a JsonMap<String, JsonValue>,
    field: &str,
    row: &StageRow,
) -> Result<Option<&'a str>, LixError> {
    let Some(value) = snapshot.get(field) else {
        return Ok(None);
    };
    value.as_str().map(Some).ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content for schema '{}' version '{}' field '{}' must be a string",
                row.schema_key, row.schema_version, field
            ),
        )
    })
}

fn resolve_entity_id(
    row: &StageRow,
    schema: &JsonValue,
    snapshot: &JsonValue,
) -> Result<EntityIdentity, LixError> {
    let Some(primary_key_paths) = primary_key_paths(schema)? else {
        return row.entity_id.clone().ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "write for schema '{}' version '{}' requires entity_id because the schema has no x-lix-primary-key",
                    row.schema_key, row.schema_version
                ),
            )
        });
    };
    let derived = EntityIdentity::from_primary_key_paths(snapshot, &primary_key_paths)
        .map_err(|error| entity_id_derivation_error(row, &primary_key_paths, error))?;
    if let Some(entity_id) = row.entity_id.as_ref() {
        if entity_id != &derived {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "entity_id '{}' does not match x-lix-primary-key derived entity_id '{}' for schema '{}' version '{}'",
                    entity_id.as_string()?, derived.as_string()?, row.schema_key, row.schema_version
                ),
            ));
        }
    }
    Ok(derived)
}

fn primary_key_paths(schema: &JsonValue) -> Result<Option<Vec<Vec<String>>>, LixError> {
    let Some(primary_key) = schema.get("x-lix-primary-key") else {
        return Ok(None);
    };
    let primary_key = primary_key.as_array().ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "schema x-lix-primary-key must be an array of JSON Pointers",
        )
    })?;
    primary_key
        .iter()
        .enumerate()
        .map(|(index, pointer)| {
            let pointer = pointer.as_str().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!("schema x-lix-primary-key entry at index {index} must be a string"),
                )
            })?;
            parse_json_pointer(pointer)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn parse_json_pointer(pointer: &str) -> Result<Vec<String>, LixError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
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
                        LixError::CODE_SCHEMA_DEFINITION,
                        "invalid JSON pointer escape",
                    ))
                }
            }
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

fn entity_id_derivation_error(
    row: &StageRow,
    primary_key_paths: &[Vec<String>],
    error: EntityIdentityError,
) -> LixError {
    let detail = match error {
        EntityIdentityError::EmptyPrimaryKey => "empty x-lix-primary-key".to_string(),
        EntityIdentityError::EmptyPrimaryKeyPath { index } => {
            format!("empty x-lix-primary-key pointer at index {index}")
        }
        EntityIdentityError::MissingPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("missing value at primary-key pointer '{pointer}'")
        }
        EntityIdentityError::NullPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("null value at primary-key pointer '{pointer}'")
        }
        EntityIdentityError::EmptyPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("empty value at primary-key pointer '{pointer}'")
        }
        EntityIdentityError::UnsupportedPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("unsupported non-scalar value at primary-key pointer '{pointer}'")
        }
        EntityIdentityError::InvalidEncodedEntityIdentity => {
            "invalid encoded entity identity".to_string()
        }
    };
    LixError::new(
        LixError::CODE_SCHEMA_VALIDATION,
        format!(
            "failed to derive entity_id for schema '{}' version '{}': {detail}",
            row.schema_key, row.schema_version
        ),
    )
}

fn format_json_pointer(segments: &[String]) -> String {
    if segments.is_empty() {
        return String::new();
    }
    format!(
        "/{}",
        segments
            .iter()
            .map(|segment| segment.replace('~', "~0").replace('/', "~1"))
            .collect::<Vec<_>>()
            .join("/")
    )
}

pub(crate) fn remember_pending_registered_schema(
    snapshot_content: Option<&str>,
    schema_catalog: &mut TransactionSchemaCatalog,
) -> Result<(), LixError> {
    let Some(snapshot_content) = snapshot_content else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "lix_registered_schema rows cannot be deleted yet; schema deletion is not supported",
        ));
    };
    let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("pending registered schema snapshot_content is invalid JSON: {error}"),
        )
    })?;
    let registered_schema_definition = schema_catalog
        .schema(REGISTERED_SCHEMA_KEY, "1")
        .cloned()
        .ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "lix_registered_schema schema is not visible to this transaction",
        )
    })?;
    if !snapshot.get("value").is_some_and(JsonValue::is_object) {
        validate_lix_schema(&registered_schema_definition, &snapshot)?;
    }
    let (key, schema) = schema_from_registered_snapshot(&snapshot)?;
    reject_unsupported_registered_schema_version(&key)?;
    if is_seed_schema_key(&key.schema_key) {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "schema '{}' is a system schema and cannot be registered at runtime",
                key.schema_key
            ),
        ));
    }
    validate_lix_schema_definition(&schema)?;
    validate_lix_schema(&registered_schema_definition, &snapshot)?;
    schema_catalog.insert_schema(key, schema);
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SchemaCatalogKey {
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
}

impl SchemaCatalogKey {
    pub(crate) fn from_schema_key(key: SchemaKey) -> Self {
        Self {
            schema_key: key.schema_key,
            schema_version: key.schema_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::functions::{FunctionProvider, SharedFunctionProvider};
    use crate::schema::seed_schema_definition;

    #[test]
    fn normalization_derives_entity_id_from_primary_key() {
        let mut catalog = catalog_with(vec![schema_with_default_id()]);
        let row = StageRow {
            entity_id: None,
            schema_key: "normalization_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot_content: Some(r#"{"id":"entity-from-snapshot","value":"hello"}"#.to_string()),
            ..base_stage_row()
        };

        let row = normalize_stage_row(row, &mut catalog, functions()).expect("normalize row");

        assert_eq!(
            row.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single(
                "entity-from-snapshot"
            ))
        );
    }

    #[test]
    fn normalization_applies_json_and_cel_defaults_before_identity_derivation() {
        let mut catalog = catalog_with(vec![schema_with_default_id()]);
        let row = StageRow {
            entity_id: None,
            schema_key: "normalization_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot_content: Some(r#"{}"#.to_string()),
            ..base_stage_row()
        };

        let row = normalize_stage_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot: JsonValue =
            serde_json::from_str(row.snapshot_content.as_deref().unwrap()).unwrap();

        assert_eq!(
            row.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single(
                "uuid-default"
            ))
        );
        assert_eq!(snapshot["id"], "uuid-default");
        assert_eq!(snapshot["value"], "literal-default");
    }

    #[test]
    fn normalization_applies_cel_defaults_from_snapshot_context() {
        let mut catalog = catalog_with(vec![schema_with_cel_field_default()]);
        let row = StageRow {
            entity_id: None,
            schema_key: "cel_field_default_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot_content: Some(r#"{"id":"entity-1","name":"Sample"}"#.to_string()),
            ..base_stage_row()
        };

        let row = normalize_stage_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot: JsonValue =
            serde_json::from_str(row.snapshot_content.as_deref().unwrap()).unwrap();

        assert_eq!(snapshot["slug"], "Sample-slug");
    }

    #[test]
    fn normalization_x_lix_default_overrides_json_default() {
        let mut catalog = catalog_with(vec![schema_with_overridden_default()]);
        let row = StageRow {
            entity_id: None,
            schema_key: "overridden_default_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot_content: Some(r#"{"id":"entity-1"}"#.to_string()),
            ..base_stage_row()
        };

        let row = normalize_stage_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot: JsonValue =
            serde_json::from_str(row.snapshot_content.as_deref().unwrap()).unwrap();

        assert_eq!(snapshot["status"], "computed");
    }

    #[test]
    fn normalization_does_not_overwrite_explicit_null_with_default() {
        let mut catalog = catalog_with(vec![schema_with_nullable_default()]);
        let row = StageRow {
            entity_id: None,
            schema_key: "nullable_default_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot_content: Some(r#"{"id":"entity-1","status":null}"#.to_string()),
            ..base_stage_row()
        };

        let row = normalize_stage_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot: JsonValue =
            serde_json::from_str(row.snapshot_content.as_deref().unwrap()).unwrap();

        assert_eq!(snapshot["status"], JsonValue::Null);
    }

    #[test]
    fn normalization_applies_timestamp_function_default() {
        let mut catalog = catalog_with(vec![schema_with_timestamp_default()]);
        let row = StageRow {
            entity_id: None,
            schema_key: "timestamp_default_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot_content: Some(r#"{"id":"entity-1"}"#.to_string()),
            ..base_stage_row()
        };

        let row = normalize_stage_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot: JsonValue =
            serde_json::from_str(row.snapshot_content.as_deref().unwrap()).unwrap();

        assert_eq!(snapshot["created_at"], "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn normalization_surfaces_cel_default_errors() {
        let mut catalog = catalog_with(vec![schema_with_unknown_cel_default()]);
        let row = StageRow {
            entity_id: None,
            schema_key: "unknown_cel_default_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot_content: Some(r#"{"id":"entity-1"}"#.to_string()),
            ..base_stage_row()
        };

        let error =
            normalize_stage_row(row, &mut catalog, functions()).expect_err("default should fail");

        assert!(error.message.contains("failed to evaluate x-lix-default"));
        assert!(error.message.contains("unknown_cel_default_schema.slug"));
    }

    #[test]
    fn normalization_rejects_entity_id_that_disagrees_with_primary_key() {
        let mut catalog = catalog_with(vec![schema_with_default_id()]);
        let row = StageRow {
            entity_id: Some(crate::entity_identity::EntityIdentity::single("wrong-id")),
            schema_key: "normalization_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot_content: Some(r#"{"id":"right-id","value":"hello"}"#.to_string()),
            ..base_stage_row()
        };

        let error =
            normalize_stage_row(row, &mut catalog, functions()).expect_err("id mismatch fails");

        assert!(error
            .message
            .contains("does not match x-lix-primary-key derived entity_id"));
    }

    #[test]
    fn normalization_derives_opaque_entity_id_for_composite_primary_key() {
        let mut catalog = catalog_with(vec![composite_key_schema()]);
        let row = StageRow {
            entity_id: None,
            schema_key: "composite_key_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot_content: Some(r#"{"namespace":"a~b","key":"1"}"#.to_string()),
            ..base_stage_row()
        };

        let row = normalize_stage_row(row, &mut catalog, functions()).expect("normalize row");
        let entity_id = row.entity_id.expect("composite entity id");
        let projected_entity_id = entity_id.as_string().expect("entity id should project");

        assert!(projected_entity_id.starts_with("pk:v1:"));
        assert_ne!(projected_entity_id, "a~b~1");
    }

    #[test]
    fn normalization_validates_explicit_composite_entity_id_against_projection() {
        let mut catalog = catalog_with(vec![composite_key_schema()]);
        let snapshot = json!({
            "namespace": "a~b",
            "key": "1",
        });
        let derived = EntityIdentity::from_primary_key_paths(
            &snapshot,
            &[vec!["namespace".to_string()], vec!["key".to_string()]],
        )
        .expect("identity should derive");
        let row = StageRow {
            entity_id: Some(derived.clone()),
            schema_key: "composite_key_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot_content: Some(snapshot.to_string()),
            ..base_stage_row()
        };

        let row = normalize_stage_row(row, &mut catalog, functions()).expect("normalize row");

        assert_eq!(row.entity_id.as_ref(), Some(&derived));
    }

    #[test]
    fn normalization_makes_pending_registered_schema_visible_to_later_rows() {
        let mut catalog = catalog_with(vec![seed_schema_definition(REGISTERED_SCHEMA_KEY)
            .expect("registered schema builtin")
            .clone()]);
        let registered = StageRow {
            entity_id: None,
            schema_key: REGISTERED_SCHEMA_KEY.to_string(),
            schema_version: "1".to_string(),
            snapshot_content: Some(
                json!({
                    "value": dynamic_schema_definition(),
                })
                .to_string(),
            ),
            ..base_stage_row()
        };

        normalize_stage_row(registered, &mut catalog, functions()).expect("register schema");

        let dynamic = StageRow {
            entity_id: None,
            schema_key: "dynamic_schema".to_string(),
            schema_version: "1".to_string(),
            snapshot_content: Some(r#"{"id":"dynamic-1"}"#.to_string()),
            ..base_stage_row()
        };
        let dynamic = normalize_stage_row(dynamic, &mut catalog, functions()).expect("dynamic row");

        assert_eq!(
            dynamic.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single("dynamic-1"))
        );
    }

    #[test]
    fn normalization_canonicalizes_filesystem_descriptor_segments() {
        let mut catalog = catalog_with(vec![
            builtin_schema(FILE_DESCRIPTOR_SCHEMA_KEY),
            builtin_schema(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        ]);

        let file = StageRow {
            entity_id: None,
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
            snapshot_content: Some(
                json!({
                    "id": "file-cafe",
                    "directory_id": null,
                    "name": "Cafe\u{301}.txt",
                })
                .to_string(),
            ),
            global: false,
            ..base_stage_row()
        };
        let file = normalize_stage_row(file, &mut catalog, functions()).expect("normalize file");
        let file_snapshot: JsonValue =
            serde_json::from_str(file.snapshot_content.as_deref().unwrap()).unwrap();
        assert_eq!(file_snapshot["name"], "Café.txt");

        let directory = StageRow {
            entity_id: None,
            schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            snapshot_content: Some(
                json!({
                    "id": "dir-cafe",
                    "parent_id": null,
                    "name": "Cafe\u{301}",
                })
                .to_string(),
            ),
            global: false,
            ..base_stage_row()
        };
        let directory =
            normalize_stage_row(directory, &mut catalog, functions()).expect("normalize directory");
        let directory_snapshot: JsonValue =
            serde_json::from_str(directory.snapshot_content.as_deref().unwrap()).unwrap();
        assert_eq!(directory_snapshot["name"], "Café");
    }

    #[test]
    fn normalization_rejects_invalid_filesystem_descriptor_segments() {
        let mut catalog = catalog_with(vec![
            builtin_schema(FILE_DESCRIPTOR_SCHEMA_KEY),
            builtin_schema(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        ]);

        let dot_segment = normalize_stage_row(
            StageRow {
                entity_id: None,
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                snapshot_content: Some(
                    json!({
                        "id": "file-dotdot",
                        "directory_id": null,
                        "name": "..",
                    })
                    .to_string(),
                ),
                global: false,
                ..base_stage_row()
            },
            &mut catalog,
            functions(),
        )
        .expect_err("file descriptor name should reject dot segments");
        assert_eq!(dot_segment.code, "LIX_ERROR_PATH_DOT_SEGMENT");

        let bidi = normalize_stage_row(
            StageRow {
                entity_id: None,
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                snapshot_content: Some(
                    json!({
                        "id": "file-bidi",
                        "directory_id": null,
                        "name": "safe\u{202E}txt",
                    })
                    .to_string(),
                ),
                global: false,
                ..base_stage_row()
            },
            &mut catalog,
            functions(),
        )
        .expect_err("file descriptor name should reject bidi formatting characters");
        assert_eq!(bidi.code, "LIX_ERROR_PATH_INVALID_SEGMENT_CODE_POINT");

        let zero_width = normalize_stage_row(
            StageRow {
                entity_id: None,
                schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                snapshot_content: Some(
                    json!({
                        "id": "dir-zero-width",
                        "parent_id": null,
                        "name": "zero\u{200D}width",
                    })
                    .to_string(),
                ),
                global: false,
                ..base_stage_row()
            },
            &mut catalog,
            functions(),
        )
        .expect_err("directory descriptor name should reject zero-width characters");
        assert_eq!(zero_width.code, "LIX_ERROR_PATH_INVALID_SEGMENT_CODE_POINT");
    }

    #[test]
    fn normalization_keeps_file_descriptor_name_opaque() {
        let mut catalog = catalog_with(vec![builtin_schema(FILE_DESCRIPTOR_SCHEMA_KEY)]);

        let row = normalize_stage_row(
            StageRow {
                entity_id: None,
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                snapshot_content: Some(
                    json!({
                        "id": "file-opaque-name",
                        "directory_id": null,
                        "name": "foo.bar",
                    })
                    .to_string(),
                ),
                global: false,
                ..base_stage_row()
            },
            &mut catalog,
            functions(),
        )
        .expect("file descriptor name should be an opaque basename");

        let snapshot: JsonValue =
            serde_json::from_str(row.snapshot_content.as_deref().unwrap()).unwrap();
        assert_eq!(snapshot["name"], "foo.bar");
    }

    fn catalog_with(schemas: Vec<JsonValue>) -> TransactionSchemaCatalog {
        TransactionSchemaCatalog::from_visible_schemas(&schemas).expect("catalog")
    }

    fn builtin_schema(schema_key: &str) -> JsonValue {
        seed_schema_definition(schema_key)
            .unwrap_or_else(|| panic!("{schema_key} builtin schema should exist"))
            .clone()
    }

    fn base_stage_row() -> StageRow {
        StageRow {
            entity_id: Some(crate::entity_identity::EntityIdentity::single("entity-1")),
            schema_key: "normalization_schema".to_string(),
            file_id: None,
            snapshot_content: Some(r#"{"id":"entity-1","value":"hello"}"#.to_string()),
            metadata: None,
            origin: None,
            schema_version: "1".to_string(),
            created_at: None,
            updated_at: None,
            global: true,
            change_id: None,
            commit_id: None,
            untracked: false,
            version_id: crate::GLOBAL_VERSION_ID.to_string(),
        }
    }

    fn schema_with_default_id() -> JsonValue {
        json!({
            "x-lix-key": "normalization_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string", "x-lix-default": "lix_uuid_v7()" },
                "value": { "type": "string", "default": "literal-default" }
            },
            "required": ["id", "value"],
            "additionalProperties": false
        })
    }

    fn schema_with_cel_field_default() -> JsonValue {
        json!({
            "x-lix-key": "cel_field_default_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string" },
                "slug": { "type": "string", "x-lix-default": "name + '-slug'" }
            },
            "required": ["id", "name"],
            "additionalProperties": false
        })
    }

    fn schema_with_overridden_default() -> JsonValue {
        json!({
            "x-lix-key": "overridden_default_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "status": {
                    "type": "string",
                    "default": "literal",
                    "x-lix-default": "'computed'"
                }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    fn schema_with_nullable_default() -> JsonValue {
        json!({
            "x-lix-key": "nullable_default_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "status": {
                    "anyOf": [{ "type": "string" }, { "type": "null" }],
                    "x-lix-default": "'computed'"
                }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    fn schema_with_timestamp_default() -> JsonValue {
        json!({
            "x-lix-key": "timestamp_default_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "created_at": { "type": "string", "x-lix-default": "lix_timestamp()" }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    fn schema_with_unknown_cel_default() -> JsonValue {
        json!({
            "x-lix-key": "unknown_cel_default_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "slug": { "type": "string", "x-lix-default": "missing_var + '-slug'" }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    fn composite_key_schema() -> JsonValue {
        json!({
            "x-lix-key": "composite_key_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/namespace", "/key"],
            "type": "object",
            "properties": {
                "namespace": { "type": "string" },
                "key": { "type": "string" }
            },
            "required": ["namespace", "key"],
            "additionalProperties": false
        })
    }

    fn dynamic_schema_definition() -> JsonValue {
        json!({
            "x-lix-key": "dynamic_schema",
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" }
            },
            "required": ["id"],
            "additionalProperties": false
        })
    }

    fn functions() -> FunctionProviderHandle {
        SharedFunctionProvider::new(Box::new(FixedFunctions) as Box<dyn FunctionProvider + Send>)
    }

    struct FixedFunctions;

    impl FunctionProvider for FixedFunctions {
        fn uuid_v7(&mut self) -> String {
            "uuid-default".to_string()
        }

        fn timestamp(&mut self) -> String {
            "1970-01-01T00:00:00.000Z".to_string()
        }
    }
}
