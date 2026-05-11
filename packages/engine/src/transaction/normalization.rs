use std::sync::Arc;

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::catalog::{CatalogSnapshot, SchemaPlan, SchemaPlanId};
use crate::common::format_json_pointer;
use crate::common::normalize_path_segment;
use crate::domain::Domain;
use crate::entity_identity::{EntityIdentity, EntityIdentityError};
use crate::functions::FunctionProviderHandle;
use crate::schema::{
    is_seed_schema_key, schema_from_registered_snapshot, validate_lix_schema,
    validate_lix_schema_definition,
};
use crate::transaction::types::{PreparedRowFacts, TransactionJson, TransactionWriteRow};
use crate::LixError;

pub(crate) const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedTransactionWriteRow {
    pub(crate) row: TransactionWriteRow,
    pub(crate) snapshot: Option<TransactionJson>,
    pub(crate) schema_plan_id: SchemaPlanId,
    pub(crate) facts: PreparedRowFacts,
}

/// Normalizes one incoming row into a row with final snapshot/entity identity.
///
/// This is the canonical schema-semantics boundary for transaction writes. It owns
/// schema default application, primary-key identity derivation, and explicit
/// identity mismatch validation. SQL providers should not pre-derive primary
/// keys for schemas that can be normalized here; they should pass decoded
/// snapshots and let this layer complete them.
///
/// This function intentionally does not assign timestamps, change ids, or
/// commit ids; those are prepared-row fields assigned after semantic
/// normalization has produced the final identity.
pub(crate) fn normalize_transaction_write_row(
    mut row: TransactionWriteRow,
    schema_catalog: &mut CatalogSnapshot,
    functions: FunctionProviderHandle,
) -> Result<NormalizedTransactionWriteRow, LixError> {
    validate_transaction_write_row_schema_identity(&row)?;

    let Some((schema_plan_id, schema_plan)) = schema_catalog.plan_for_key(&row.schema_key) else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "schema '{}' is not visible to this transaction",
                row.schema_key
            ),
        ));
    };

    let normalized_snapshot = if let Some(snapshot) = row.snapshot.take() {
        let (mut snapshot, normalized) = snapshot_object_from_transaction_json(snapshot, &row)?;
        let defaults_changed = apply_defaults(&mut snapshot, schema_plan, &row, functions)?;
        let descriptor_changed = normalize_filesystem_descriptor_snapshot(&row, &mut snapshot)?;
        let snapshot = JsonValue::Object(snapshot);
        row.entity_id = Some(resolve_entity_id(&row, schema_plan, &snapshot)?);
        if defaults_changed || descriptor_changed {
            Some(TransactionJson::from_value(
                snapshot,
                "normalized transaction snapshot_content",
            )?)
        } else {
            Some(TransactionJson::from_parts(Arc::new(snapshot), normalized))
        }
    } else if row.entity_id.is_none() {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "tombstone for schema '{}' requires entity_id",
                row.schema_key
            ),
        ));
    } else {
        None
    };

    if row.schema_key == REGISTERED_SCHEMA_KEY {
        if row.file_id.is_some() {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                "lix_registered_schema rows must not be scoped to a file",
            )
            .with_hint("Schema definitions are scoped by version and durability only; write them with null file_id."));
        }
        let schema_domain =
            Domain::schema_catalog(row.schema_scope_version_id().to_string(), row.untracked);
        remember_pending_registered_schema(
            normalized_snapshot.as_ref().map(TransactionJson::value),
            schema_domain,
            schema_catalog,
        )?;
    }

    Ok(NormalizedTransactionWriteRow {
        row,
        snapshot: normalized_snapshot,
        schema_plan_id,
        facts: PreparedRowFacts::default(),
    })
}

fn validate_transaction_write_row_schema_identity(
    row: &TransactionWriteRow,
) -> Result<(), LixError> {
    if row.schema_key.is_empty() {
        return Err(LixError::new(
            LixError::CODE_UNKNOWN,
            "engine transaction staging requires non-empty schema_key",
        ));
    }
    Ok(())
}

fn snapshot_object_from_transaction_json(
    snapshot: TransactionJson,
    row: &TransactionWriteRow,
) -> Result<(JsonMap<String, JsonValue>, Arc<str>), LixError> {
    let (snapshot, normalized) = snapshot.into_parts();
    let snapshot = match Arc::try_unwrap(snapshot) {
        Ok(snapshot) => snapshot,
        Err(snapshot) => snapshot.as_ref().clone(),
    };
    match snapshot {
        JsonValue::Object(snapshot) => Ok((snapshot, normalized)),
        _ => Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content for schema '{}' must be a JSON object",
                row.schema_key
            ),
        )),
    }
}

fn apply_defaults(
    snapshot: &mut JsonMap<String, JsonValue>,
    schema_plan: &SchemaPlan,
    row: &TransactionWriteRow,
    functions: FunctionProviderHandle,
) -> Result<bool, LixError> {
    schema_plan
        .defaults
        .apply(snapshot, functions, &row.schema_key)
}

fn normalize_filesystem_descriptor_snapshot(
    row: &TransactionWriteRow,
    snapshot: &mut JsonMap<String, JsonValue>,
) -> Result<bool, LixError> {
    match row.schema_key.as_str() {
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY => normalize_directory_descriptor_snapshot(row, snapshot),
        FILE_DESCRIPTOR_SCHEMA_KEY => normalize_file_descriptor_snapshot(row, snapshot),
        _ => Ok(false),
    }
}

fn normalize_directory_descriptor_snapshot(
    row: &TransactionWriteRow,
    snapshot: &mut JsonMap<String, JsonValue>,
) -> Result<bool, LixError> {
    let Some(name) = optional_string_field(snapshot, "name", row)? else {
        return Ok(false);
    };
    let normalized_name = normalize_path_segment(name)?;
    if name == normalized_name {
        return Ok(false);
    }
    snapshot.insert("name".to_string(), JsonValue::String(normalized_name));
    Ok(true)
}

fn normalize_file_descriptor_snapshot(
    row: &TransactionWriteRow,
    snapshot: &mut JsonMap<String, JsonValue>,
) -> Result<bool, LixError> {
    let Some(name) = optional_string_field(snapshot, "name", row)? else {
        return Ok(false);
    };
    let normalized_name = normalize_path_segment(name)?;
    if name == normalized_name {
        return Ok(false);
    }
    snapshot.insert("name".to_string(), JsonValue::String(normalized_name));
    Ok(true)
}

fn optional_string_field<'a>(
    snapshot: &'a JsonMap<String, JsonValue>,
    field: &str,
    row: &TransactionWriteRow,
) -> Result<Option<&'a str>, LixError> {
    let Some(value) = snapshot.get(field) else {
        return Ok(None);
    };
    value.as_str().map(Some).ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content for schema '{}' field '{}' must be a string",
                row.schema_key, field
            ),
        )
    })
}

fn resolve_entity_id(
    row: &TransactionWriteRow,
    schema_plan: &SchemaPlan,
    snapshot: &JsonValue,
) -> Result<EntityIdentity, LixError> {
    let Some(primary_key_paths) = schema_plan.primary_key.as_ref() else {
        return row.entity_id.clone().ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "write for schema '{}' requires entity_id because the schema has no x-lix-primary-key",
                    row.schema_key
                ),
            )
        });
    };
    let derived = EntityIdentity::from_primary_key_paths(snapshot, primary_key_paths)
        .map_err(|error| entity_id_derivation_error(row, primary_key_paths, error))?;
    if let Some(entity_id) = row.entity_id.as_ref() {
        if entity_id != &derived {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "entity_id '{}' does not match x-lix-primary-key derived entity_id '{}' for schema '{}'",
                    entity_id.as_json_array_text()?, derived.as_json_array_text()?, row.schema_key
                ),
            ));
        }
    }
    Ok(derived)
}

fn entity_id_derivation_error(
    row: &TransactionWriteRow,
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
        EntityIdentityError::UnsupportedPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&primary_key_paths[index]);
            format!("non-string value at primary-key pointer '{pointer}'")
        }
        EntityIdentityError::InvalidEncodedEntityIdentity => {
            "invalid encoded entity identity".to_string()
        }
    };
    LixError::new(
        LixError::CODE_SCHEMA_VALIDATION,
        format!(
            "failed to derive entity_id for schema '{}': {detail}",
            row.schema_key
        ),
    )
}

pub(crate) fn remember_pending_registered_schema(
    snapshot: Option<&JsonValue>,
    domain: Domain,
    schema_catalog: &mut CatalogSnapshot,
) -> Result<(), LixError> {
    let Some(snapshot) = snapshot else {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "lix_registered_schema rows cannot be deleted yet; schema deletion is not supported",
        ));
    };
    if let Some(schema) = snapshot.get("value") {
        validate_lix_schema_definition(schema)?;
    }
    {
        let registered_schema_definition = schema_catalog
            .schema(REGISTERED_SCHEMA_KEY)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    "lix_registered_schema schema is not visible to this transaction",
                )
            })?;
        validate_lix_schema(registered_schema_definition, &snapshot)?;
    }
    let (key, schema) = schema_from_registered_snapshot(&snapshot)?;
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
    schema_catalog.insert_schema_for_domain(domain, key, schema)?;
    Ok(())
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
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "normalization_schema".to_string(),
            snapshot: Some(snapshot_json(
                r#"{"id":"entity-from-snapshot","value":"hello"}"#,
            )),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");

        assert_eq!(
            row.row.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single(
                "entity-from-snapshot"
            ))
        );
    }

    #[test]
    fn normalization_applies_json_and_cel_defaults_before_identity_derivation() {
        let mut catalog = catalog_with(vec![schema_with_default_id()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "normalization_schema".to_string(),
            snapshot: Some(snapshot_json(r#"{}"#)),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(
            row.row.entity_id.as_ref(),
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
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "cel_field_default_schema".to_string(),
            snapshot: Some(snapshot_json(r#"{"id":"entity-1","name":"Sample"}"#)),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["slug"], "Sample-slug");
    }

    #[test]
    fn normalization_x_lix_default_overrides_json_default() {
        let mut catalog = catalog_with(vec![schema_with_overridden_default()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "overridden_default_schema".to_string(),
            snapshot: Some(snapshot_json(r#"{"id":"entity-1"}"#)),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["status"], "computed");
    }

    #[test]
    fn normalization_does_not_overwrite_explicit_null_with_default() {
        let mut catalog = catalog_with(vec![schema_with_nullable_default()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "nullable_default_schema".to_string(),
            snapshot: Some(snapshot_json(r#"{"id":"entity-1","status":null}"#)),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["status"], JsonValue::Null);
    }

    #[test]
    fn normalization_applies_timestamp_function_default() {
        let mut catalog = catalog_with(vec![schema_with_timestamp_default()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "timestamp_default_schema".to_string(),
            snapshot: Some(snapshot_json(r#"{"id":"entity-1"}"#)),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");
        let snapshot = normalized_snapshot(&row);

        assert_eq!(snapshot["created_at"], "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn normalization_surfaces_cel_default_errors() {
        let mut catalog = catalog_with(vec![schema_with_unknown_cel_default()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "unknown_cel_default_schema".to_string(),
            snapshot: Some(snapshot_json(r#"{"id":"entity-1"}"#)),
            ..base_stage_row()
        };

        let error = normalize_transaction_write_row(row, &mut catalog, functions())
            .expect_err("default should fail");

        assert!(error.message.contains("failed to evaluate x-lix-default"));
        assert!(error.message.contains("unknown_cel_default_schema.slug"));
    }

    #[test]
    fn normalization_rejects_entity_id_that_disagrees_with_primary_key() {
        let mut catalog = catalog_with(vec![schema_with_default_id()]);
        let row = TransactionWriteRow {
            entity_id: Some(crate::entity_identity::EntityIdentity::single("wrong-id")),
            schema_key: "normalization_schema".to_string(),
            snapshot: Some(snapshot_json(r#"{"id":"right-id","value":"hello"}"#)),
            ..base_stage_row()
        };

        let error = normalize_transaction_write_row(row, &mut catalog, functions())
            .expect_err("id mismatch fails");

        assert!(error
            .message
            .contains("does not match x-lix-primary-key derived entity_id"));
    }

    #[test]
    fn normalization_derives_json_array_entity_id_for_composite_primary_key() {
        let mut catalog = catalog_with(vec![composite_key_schema()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "composite_key_schema".to_string(),
            snapshot: Some(snapshot_json(r#"{"namespace":"a~b","key":"1"}"#)),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");
        let entity_id = row.row.entity_id.expect("composite entity id");
        let projected_entity_id = entity_id
            .as_json_array_text()
            .expect("entity id should project");

        assert_eq!(projected_entity_id, "[\"a~b\",\"1\"]");
    }

    #[test]
    fn normalization_rejects_non_string_primary_key_values() {
        let mut catalog = catalog_with(vec![composite_key_schema()]);
        let row = TransactionWriteRow {
            entity_id: None,
            schema_key: "composite_key_schema".to_string(),
            snapshot: Some(snapshot_json(r#"{"namespace":"a~b","key":1}"#)),
            ..base_stage_row()
        };

        let error = normalize_transaction_write_row(row, &mut catalog, functions())
            .expect_err("non-string primary key values should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(error
            .message
            .contains("non-string value at primary-key pointer '/key'"));
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
        let row = TransactionWriteRow {
            entity_id: Some(derived.clone()),
            schema_key: "composite_key_schema".to_string(),
            snapshot: Some(transaction_json(snapshot.clone())),
            ..base_stage_row()
        };

        let row =
            normalize_transaction_write_row(row, &mut catalog, functions()).expect("normalize row");

        assert_eq!(row.row.entity_id.as_ref(), Some(&derived));
    }

    #[test]
    fn normalization_makes_pending_registered_schema_visible_to_later_rows() {
        let mut catalog = catalog_with(vec![seed_schema_definition(REGISTERED_SCHEMA_KEY)
            .expect("registered schema builtin")
            .clone()]);
        let registered = TransactionWriteRow {
            entity_id: None,
            schema_key: REGISTERED_SCHEMA_KEY.to_string(),
            snapshot: Some(transaction_json(json!({
                "value": dynamic_schema_definition(),
            }))),
            ..base_stage_row()
        };

        normalize_transaction_write_row(registered, &mut catalog, functions())
            .expect("register schema");

        let dynamic = TransactionWriteRow {
            entity_id: None,
            schema_key: "dynamic_schema".to_string(),
            snapshot: Some(snapshot_json(r#"{"id":"dynamic-1"}"#)),
            ..base_stage_row()
        };
        let dynamic = normalize_transaction_write_row(dynamic, &mut catalog, functions())
            .expect("dynamic row");

        assert_eq!(
            dynamic.row.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single("dynamic-1"))
        );
    }

    #[test]
    fn normalization_canonicalizes_filesystem_descriptor_segments() {
        let mut catalog = catalog_with(vec![
            builtin_schema(FILE_DESCRIPTOR_SCHEMA_KEY),
            builtin_schema(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        ]);

        let file = TransactionWriteRow {
            entity_id: None,
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
            snapshot: Some(transaction_json(json!({
                "id": "file-cafe",
                "directory_id": null,
                "name": "Cafe\u{301}.txt",
            }))),
            global: false,
            ..base_stage_row()
        };
        let file = normalize_transaction_write_row(file, &mut catalog, functions())
            .expect("normalize file");
        let file_snapshot = normalized_snapshot(&file);
        assert_eq!(file_snapshot["name"], "Café.txt");

        let directory = TransactionWriteRow {
            entity_id: None,
            schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            snapshot: Some(transaction_json(json!({
                "id": "dir-cafe",
                "parent_id": null,
                "name": "Cafe\u{301}",
            }))),
            global: false,
            ..base_stage_row()
        };
        let directory = normalize_transaction_write_row(directory, &mut catalog, functions())
            .expect("normalize directory");
        let directory_snapshot = normalized_snapshot(&directory);
        assert_eq!(directory_snapshot["name"], "Café");
    }

    #[test]
    fn normalization_rejects_invalid_filesystem_descriptor_segments() {
        let mut catalog = catalog_with(vec![
            builtin_schema(FILE_DESCRIPTOR_SCHEMA_KEY),
            builtin_schema(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        ]);

        let dot_segment = normalize_transaction_write_row(
            TransactionWriteRow {
                entity_id: None,
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                snapshot: Some(transaction_json(json!({
                    "id": "file-dotdot",
                    "directory_id": null,
                    "name": "..",
                }))),
                global: false,
                ..base_stage_row()
            },
            &mut catalog,
            functions(),
        )
        .expect_err("file descriptor name should reject dot segments");
        assert_eq!(dot_segment.code, "LIX_ERROR_PATH_DOT_SEGMENT");

        let bidi = normalize_transaction_write_row(
            TransactionWriteRow {
                entity_id: None,
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                snapshot: Some(transaction_json(json!({
                    "id": "file-bidi",
                    "directory_id": null,
                    "name": "safe\u{202E}txt",
                }))),
                global: false,
                ..base_stage_row()
            },
            &mut catalog,
            functions(),
        )
        .expect_err("file descriptor name should reject bidi formatting characters");
        assert_eq!(bidi.code, "LIX_ERROR_PATH_INVALID_SEGMENT_CODE_POINT");

        let zero_width = normalize_transaction_write_row(
            TransactionWriteRow {
                entity_id: None,
                schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                snapshot: Some(transaction_json(json!({
                    "id": "dir-zero-width",
                    "parent_id": null,
                    "name": "zero\u{200D}width",
                }))),
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

        let row = normalize_transaction_write_row(
            TransactionWriteRow {
                entity_id: None,
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                snapshot: Some(transaction_json(json!({
                    "id": "file-opaque-name",
                    "directory_id": null,
                    "name": "foo.bar",
                }))),
                global: false,
                ..base_stage_row()
            },
            &mut catalog,
            functions(),
        )
        .expect("file descriptor name should be an opaque basename");

        let snapshot = normalized_snapshot(&row);
        assert_eq!(snapshot["name"], "foo.bar");
    }

    fn normalized_snapshot(row: &NormalizedTransactionWriteRow) -> &JsonValue {
        row.snapshot
            .as_ref()
            .expect("normalized test row should have a snapshot")
            .value()
    }

    fn catalog_with(schemas: Vec<JsonValue>) -> CatalogSnapshot {
        let mut visible_schemas = schemas;
        if visible_schemas.iter().any(|schema| {
            schema.get("x-lix-key").and_then(JsonValue::as_str) == Some(FILE_DESCRIPTOR_SCHEMA_KEY)
        }) && !visible_schemas.iter().any(|schema| {
            schema.get("x-lix-key").and_then(JsonValue::as_str)
                == Some(DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        }) {
            visible_schemas.push(builtin_schema(DIRECTORY_DESCRIPTOR_SCHEMA_KEY));
        }
        CatalogSnapshot::from_visible_schemas(&visible_schemas).expect("catalog")
    }

    fn builtin_schema(schema_key: &str) -> JsonValue {
        seed_schema_definition(schema_key)
            .unwrap_or_else(|| panic!("{schema_key} builtin schema should exist"))
            .clone()
    }

    fn transaction_json(value: JsonValue) -> TransactionJson {
        TransactionJson::from_value_for_test(value)
    }

    fn snapshot_json(value: &str) -> TransactionJson {
        transaction_json(serde_json::from_str(value).expect("test snapshot should parse"))
    }

    fn base_stage_row() -> TransactionWriteRow {
        TransactionWriteRow {
            entity_id: Some(crate::entity_identity::EntityIdentity::single("entity-1")),
            schema_key: "normalization_schema".to_string(),
            file_id: None,
            snapshot: Some(snapshot_json(r#"{"id":"entity-1","value":"hello"}"#)),
            metadata: None,
            origin: None,
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
