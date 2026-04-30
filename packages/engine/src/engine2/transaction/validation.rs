use std::collections::BTreeMap;

use jsonschema::JSONSchema;
use serde_json::Value as JsonValue;

use crate::engine2::transaction::staging::StagedWriteSet;
use crate::engine2::transaction::types::StagedStateRow;
use crate::schema::{
    builtin_schema_definition, compile_lix_schema, format_lix_schema_validation_errors,
    schema_from_registered_snapshot, schema_key_from_definition, validate_lix_schema,
    validate_lix_schema_definition, SchemaKey,
};
use crate::LixError;

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

/// Immutable view of the final transaction write set before persistence.
///
/// Validation intentionally runs after staging has coalesced overwrites and
/// hydrated generated fields, but before changelog, tracked-state, untracked
/// state, or binary CAS writes are flushed.
pub(crate) struct TransactionValidationInput<'a> {
    staged_writes: &'a StagedWriteSet,
    visible_schemas: &'a [JsonValue],
}

impl<'a> TransactionValidationInput<'a> {
    pub(crate) fn new(staged_writes: &'a StagedWriteSet, visible_schemas: &'a [JsonValue]) -> Self {
        Self {
            staged_writes,
            visible_schemas,
        }
    }
}

/// Validates the final transaction write set before durable persistence.
///
/// This first chunk only establishes the transaction-owned validation
/// boundary. Semantic checks such as JSON Schema validation, uniqueness, and
/// foreign-key enforcement should be added here so every write frontend shares
/// the same transaction-visible rules.
pub(crate) async fn validate_staged_writes(
    input: TransactionValidationInput<'_>,
) -> Result<(), LixError> {
    let schema_catalog = SchemaCatalogSnapshot::from_transaction_input(&input)?;
    let mut compiled_schemas = CompiledSchemaCatalog::new(&schema_catalog);
    for row in &input.staged_writes.state_rows {
        validate_staged_row_shape(row)?;
        validate_schema_exists(row, &schema_catalog)?;
        validate_snapshot_content(row, &mut compiled_schemas)?;
    }
    Ok(())
}

fn validate_staged_row_shape(row: &StagedStateRow) -> Result<(), LixError> {
    if row.schema_key.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine2 transaction validation requires non-empty schema_key",
        ));
    }
    if row.schema_version.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine2 transaction validation requires non-empty schema_version",
        ));
    }
    Ok(())
}

fn validate_schema_exists(
    row: &StagedStateRow,
    schema_catalog: &SchemaCatalogSnapshot,
) -> Result<(), LixError> {
    if !schema_catalog.contains(&row.schema_key, &row.schema_version) {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!(
                "schema '{}' version '{}' is not visible to this transaction",
                row.schema_key, row.schema_version
            ),
        ));
    }
    Ok(())
}

fn validate_snapshot_content(
    row: &StagedStateRow,
    compiled_schemas: &mut CompiledSchemaCatalog<'_>,
) -> Result<(), LixError> {
    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Ok(());
    };
    let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content for schema '{}' version '{}' is invalid JSON: {error}",
                row.schema_key, row.schema_version
            ),
        )
    })?;
    let compiled_schema = compiled_schemas.compiled_schema(&row.schema_key, &row.schema_version)?;
    if let Err(errors) = compiled_schema.validate(&snapshot) {
        let details = format_lix_schema_validation_errors(errors);
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!(
                "snapshot_content validation failed for schema '{}' version '{}': {details}",
                row.schema_key, row.schema_version
            ),
        ));
    }
    Ok(())
}

/// Transaction-visible schema definitions indexed by exact schema identity.
///
/// The snapshot starts from schemas visible before this write, then applies
/// pending `lix_registered_schema` rows from the final staged write set. That
/// lets one transaction register a schema and write rows for it without
/// re-scanning schemas for every staged row.
#[derive(Debug, Clone, Default)]
struct SchemaCatalogSnapshot {
    schemas_by_key: BTreeMap<SchemaCatalogKey, JsonValue>,
}

impl SchemaCatalogSnapshot {
    fn from_transaction_input(input: &TransactionValidationInput<'_>) -> Result<Self, LixError> {
        let mut snapshot = Self::default();
        snapshot.remember_visible_schemas(input.visible_schemas)?;
        snapshot.remember_pending_registered_schemas(&input.staged_writes.state_rows)?;
        Ok(snapshot)
    }

    fn remember_visible_schemas(&mut self, visible_schemas: &[JsonValue]) -> Result<(), LixError> {
        for schema in visible_schemas {
            let key = schema_key_from_definition(schema)?;
            self.insert_schema(key, schema.clone());
        }
        Ok(())
    }

    fn remember_pending_registered_schemas(
        &mut self,
        rows: &[StagedStateRow],
    ) -> Result<(), LixError> {
        let mut pending_keys = BTreeMap::<SchemaCatalogKey, crate::engine2::entity_identity::EntityIdentity>::new();
        for row in rows {
            if row.schema_key != REGISTERED_SCHEMA_KEY {
                continue;
            }
            let (key, schema) = validate_pending_registered_schema(row)?;
            let catalog_key = SchemaCatalogKey::from_schema_key(key.clone());
            if let Some(existing_entity_id) =
                pending_keys.insert(catalog_key.clone(), row.entity_id.clone())
            {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "duplicate pending registered schema '{}' version '{}' in transaction: rows '{}' and '{}'",
                        catalog_key.schema_key,
                        catalog_key.schema_version,
                        existing_entity_id.as_string()?,
                        row.entity_id.as_string()?
                    ),
                ));
            }
            self.insert_schema(key, schema);
        }
        Ok(())
    }

    fn insert_schema(&mut self, key: SchemaKey, schema: JsonValue) {
        self.schemas_by_key
            .insert(SchemaCatalogKey::from_schema_key(key), schema);
    }

    fn contains(&self, schema_key: &str, schema_version: &str) -> bool {
        self.schemas_by_key.contains_key(&SchemaCatalogKey {
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
        })
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.schemas_by_key.len()
    }

    fn schema(&self, schema_key: &str, schema_version: &str) -> Option<&JsonValue> {
        self.schemas_by_key.get(&SchemaCatalogKey {
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
        })
    }
}

/// Per-transaction compiled schema cache.
///
/// Compilation is lazy and keyed by exact `(schema_key, schema_version)`, so a
/// transaction that writes many rows for one schema pays the JSON Schema
/// compilation cost only once.
struct CompiledSchemaCatalog<'a> {
    schema_catalog: &'a SchemaCatalogSnapshot,
    compiled_by_key: BTreeMap<SchemaCatalogKey, JSONSchema>,
}

impl<'a> CompiledSchemaCatalog<'a> {
    fn new(schema_catalog: &'a SchemaCatalogSnapshot) -> Self {
        Self {
            schema_catalog,
            compiled_by_key: BTreeMap::new(),
        }
    }

    fn compiled_schema(
        &mut self,
        schema_key: &str,
        schema_version: &str,
    ) -> Result<&JSONSchema, LixError> {
        let key = SchemaCatalogKey {
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
        };
        if !self.compiled_by_key.contains_key(&key) {
            let schema = self
                .schema_catalog
                .schema(schema_key, schema_version)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_DEFINITION,
                        format!(
                            "schema '{schema_key}' version '{schema_version}' is not visible to this transaction"
                        ),
                    )
                })?;
            let compiled = compile_lix_schema(schema)?;
            self.compiled_by_key.insert(key.clone(), compiled);
        }
        self.compiled_by_key.get(&key).ok_or_else(|| {
            LixError::new(
                LixError::CODE_UNKNOWN,
                format!(
                    "compiled schema cache lookup failed for schema '{schema_key}' version '{schema_version}'"
                ),
            )
        })
    }

    #[cfg(test)]
    fn compiled_count(&self) -> usize {
        self.compiled_by_key.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SchemaCatalogKey {
    schema_key: String,
    schema_version: String,
}

impl SchemaCatalogKey {
    fn from_schema_key(key: SchemaKey) -> Self {
        Self {
            schema_key: key.schema_key,
            schema_version: key.schema_version,
        }
    }
}

fn validate_pending_registered_schema(
    row: &StagedStateRow,
) -> Result<(SchemaKey, JsonValue), LixError> {
    let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            "registered schema write requires snapshot_content",
        )
    })?;
    let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            LixError::CODE_SCHEMA_DEFINITION,
            format!("pending registered schema snapshot_content is invalid JSON: {error}"),
        )
    })?;

    let registered_schema_definition = builtin_schema_definition(REGISTERED_SCHEMA_KEY)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                "missing builtin lix_registered_schema definition",
            )
        })?;
    validate_lix_schema(registered_schema_definition, &snapshot)?;

    let (key, schema) = schema_from_registered_snapshot(&snapshot)?;
    validate_lix_schema_definition(&schema)?;
    Ok((key, schema))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn schema_catalog_indexes_visible_schemas_by_key_and_version() {
        let visible_schemas = vec![json!({
            "x-lix-key": "visible_schema",
            "x-lix-version": "1",
            "type": "object",
        })];
        let staged_writes = empty_staged_write_set();
        let input = TransactionValidationInput::new(&staged_writes, &visible_schemas);

        let catalog = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect("schema catalog should build");

        assert_eq!(catalog.len(), 1);
        assert!(catalog.contains("visible_schema", "1"));
    }

    #[test]
    fn schema_catalog_includes_pending_registered_schema_rows() {
        let visible_schemas = vec![json!({
            "x-lix-key": "visible_schema",
            "x-lix-version": "1",
            "type": "object",
        })];
        let staged_writes = StagedWriteSet {
            state_rows: vec![pending_registered_schema_row("pending_schema", "2")],
            ..empty_staged_write_set()
        };
        let input = TransactionValidationInput::new(&staged_writes, &visible_schemas);

        let catalog = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect("schema catalog should build");

        assert_eq!(catalog.len(), 2);
        assert!(catalog.contains("visible_schema", "1"));
        assert!(catalog.contains("pending_schema", "2"));
    }

    #[test]
    fn schema_catalog_pending_schema_overrides_same_visible_identity() {
        let visible_schemas = vec![json!({
            "x-lix-key": "same_schema",
            "x-lix-version": "1",
            "type": "object",
            "properties": {
                "old": { "type": "string" }
            }
        })];
        let staged_writes = StagedWriteSet {
            state_rows: vec![pending_registered_schema_row("same_schema", "1")],
            ..empty_staged_write_set()
        };
        let input = TransactionValidationInput::new(&staged_writes, &visible_schemas);

        let catalog = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect("schema catalog should build");

        assert_eq!(catalog.len(), 1);
        assert!(catalog.contains("same_schema", "1"));
    }

    #[test]
    fn pending_registered_schema_requires_snapshot_content() {
        let mut row = pending_registered_schema_row("missing_snapshot", "1");
        row.snapshot_content = None;

        let error = validate_pending_registered_schema(&row)
            .expect_err("registered schema writes require snapshot_content");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.description.contains("snapshot_content"));
    }

    #[test]
    fn pending_registered_schema_rejects_invalid_snapshot_json() {
        let mut row = pending_registered_schema_row("invalid_json", "1");
        row.snapshot_content = Some("{not-json".to_string());

        let error = validate_pending_registered_schema(&row).expect_err("invalid JSON should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.description.contains("invalid JSON"));
    }

    #[test]
    fn pending_registered_schema_uses_builtin_schema_for_outer_value_shape() {
        let mut row = pending_registered_schema_row("missing_value", "1");
        row.snapshot_content = Some(json!({}).to_string());

        let error = validate_pending_registered_schema(&row)
            .expect_err("builtin lix_registered_schema validation should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(error.description.contains("value"));
    }

    #[test]
    fn pending_registered_schema_rejects_malformed_nested_lix_schema_definition() {
        let mut row = pending_registered_schema_row("bad_schema_version", "v1");
        row.snapshot_content = Some(
            json!({
                "value": {
                    "x-lix-key": "bad_schema_version",
                    "x-lix-version": "v1",
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    },
                    "required": ["id"],
                    "additionalProperties": false,
                }
            })
            .to_string(),
        );

        let error = validate_pending_registered_schema(&row)
            .expect_err("nested Lix schema definition should be rejected");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(error.description.contains("x-lix-version"));
    }

    #[test]
    fn schema_catalog_rejects_duplicate_pending_registered_schema_identity() {
        let mut duplicate = pending_registered_schema_row("duplicate_schema", "1");
        duplicate.entity_id = registered_schema_entity_id("duplicate_schema_duplicate", "1");
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                pending_registered_schema_row("duplicate_schema", "1"),
                duplicate,
            ],
            ..empty_staged_write_set()
        };
        let visible_schemas = Vec::new();
        let input = TransactionValidationInput::new(&staged_writes, &visible_schemas);

        let error = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect_err("duplicate pending schema keys should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error
            .description
            .contains("duplicate pending registered schema"));
    }

    #[tokio::test]
    async fn validation_rejects_unknown_schema_key() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![staged_row(
                "unknown_schema",
                "1",
                Some(json!({}).to_string()),
            )],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
        ))
        .await
        .expect_err("unknown schema_key should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.description.contains("unknown_schema"));
    }

    #[tokio::test]
    async fn validation_rejects_unknown_schema_version() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![staged_row(
                "lix_key_value",
                "2",
                Some(json!({ "key": "k", "value": "v" }).to_string()),
            )],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
        ))
        .await
        .expect_err("unknown schema_version should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.description.contains("version '2'"));
    }

    #[tokio::test]
    async fn validation_checks_schema_existence_for_tombstones() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![staged_row("unknown_schema", "1", None)],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
        ))
        .await
        .expect_err("tombstone with unknown schema should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.description.contains("unknown_schema"));
    }

    #[tokio::test]
    async fn validation_allows_pending_registered_schema_to_validate_later_rows() {
        let visible_schemas = vec![key_value_schema(), registered_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![
                pending_registered_schema_row("pending_schema", "1"),
                staged_row(
                    "pending_schema",
                    "1",
                    Some(json!({ "id": "entity-1" }).to_string()),
                ),
            ],
            ..empty_staged_write_set()
        };

        validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
        ))
        .await
        .expect("pending registered schema should be visible to later staged rows");
    }

    #[tokio::test]
    async fn validation_validates_snapshot_content_against_schema() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![staged_row(
                "lix_key_value",
                "1",
                Some(json!({ "key": "k" }).to_string()),
            )],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
        ))
        .await
        .expect_err("missing required snapshot field should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(error.description.contains("value"));
    }

    #[tokio::test]
    async fn validation_rejects_invalid_snapshot_json() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![staged_row(
                "lix_key_value",
                "1",
                Some("{not-json".to_string()),
            )],
            ..empty_staged_write_set()
        };

        let error = validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
        ))
        .await
        .expect_err("invalid snapshot JSON should fail");

        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
        assert!(error.description.contains("invalid JSON"));
    }

    #[tokio::test]
    async fn validation_skips_snapshot_validation_for_tombstones() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = StagedWriteSet {
            state_rows: vec![staged_row("lix_key_value", "1", None)],
            ..empty_staged_write_set()
        };

        validate_staged_writes(TransactionValidationInput::new(
            &staged_writes,
            &visible_schemas,
        ))
        .await
        .expect("tombstone should only require schema existence");
    }

    #[test]
    fn compiled_schema_catalog_compiles_each_schema_once() {
        let visible_schemas = vec![key_value_schema()];
        let staged_writes = empty_staged_write_set();
        let input = TransactionValidationInput::new(&staged_writes, &visible_schemas);
        let catalog = SchemaCatalogSnapshot::from_transaction_input(&input)
            .expect("schema catalog should build");
        let mut compiled = CompiledSchemaCatalog::new(&catalog);

        compiled
            .compiled_schema("lix_key_value", "1")
            .expect("schema should compile");
        compiled
            .compiled_schema("lix_key_value", "1")
            .expect("schema should be cached");

        assert_eq!(compiled.compiled_count(), 1);
    }

    fn empty_staged_write_set() -> StagedWriteSet {
        StagedWriteSet {
            state_rows: Vec::new(),
            commit_members_by_version: BTreeMap::new(),
            extra_commit_parents_by_version: BTreeMap::new(),
            file_data_writes: Vec::new(),
        }
    }

    fn pending_registered_schema_row(schema_key: &str, schema_version: &str) -> StagedStateRow {
        StagedStateRow {
            entity_id: registered_schema_entity_id(schema_key, schema_version),
            schema_key: REGISTERED_SCHEMA_KEY.to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(
                json!({
                    "value": {
                        "x-lix-key": schema_key,
                        "x-lix-version": schema_version,
                        "type": "object",
                        "properties": {
                            "id": { "type": "string" }
                        },
                        "required": ["id"],
                        "additionalProperties": false,
                    }
                })
                .to_string(),
            ),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-04-29T00:00:00.000Z".to_string(),
            updated_at: "2026-04-29T00:00:00.000Z".to_string(),
            global: true,
            change_id: Some("change-registered-schema".to_string()),
            commit_id: Some("commit-registered-schema".to_string()),
            untracked: false,
            version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
        }
    }

    fn registered_schema_entity_id(
        schema_key: &str,
        schema_version: &str,
    ) -> crate::engine2::entity_identity::EntityIdentity {
        crate::engine2::entity_identity::EntityIdentity::from_primary_key_paths(
            &serde_json::json!({
                "value": {
                    "x-lix-key": schema_key,
                    "x-lix-version": schema_version,
                }
            }),
            &[
                vec!["value".to_string(), "x-lix-key".to_string()],
                vec!["value".to_string(), "x-lix-version".to_string()],
            ],
        )
        .expect("registered schema identity should derive")
    }

    fn key_value_schema() -> JsonValue {
        builtin_schema_definition("lix_key_value")
            .expect("lix_key_value builtin schema should exist")
            .clone()
    }

    fn registered_schema() -> JsonValue {
        builtin_schema_definition(REGISTERED_SCHEMA_KEY)
            .expect("lix_registered_schema builtin schema should exist")
            .clone()
    }

    fn staged_row(
        schema_key: &str,
        schema_version: &str,
        snapshot_content: Option<String>,
    ) -> StagedStateRow {
        StagedStateRow {
            entity_id: crate::engine2::entity_identity::EntityIdentity::single("entity-1"),
            schema_key: schema_key.to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content,
            metadata: None,
            schema_version: schema_version.to_string(),
            created_at: "2026-04-29T00:00:00.000Z".to_string(),
            updated_at: "2026-04-29T00:00:00.000Z".to_string(),
            global: true,
            change_id: Some("change-1".to_string()),
            commit_id: Some("commit-1".to_string()),
            untracked: false,
            version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
        }
    }
}
