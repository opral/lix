use std::collections::BTreeMap;
use std::sync::OnceLock;

use serde_json::Value as JsonValue;

use crate::contracts::surface::{
    builtin_surface_descriptors, entity_surface_descriptors, CatalogEpoch, CatalogSource,
    DynamicEntitySurfaceSpec, SurfaceColumnType, SurfaceOverridePredicate, SurfaceOverrideValue,
    SurfaceRegistry,
};
use crate::live_state::{decode_registered_schema_row, scan_live_rows, LiveRowQuery, RowReadMode};
use crate::runtime::cel::shared_runtime;
use crate::schema::{
    builtin_schema_definition, builtin_schema_keys, collect_lixcol_overrides,
    schema_from_registered_snapshot, LixcolOverrideValue, SchemaKey,
};
use crate::{LixBackend, LixError};

mod relation_policy;

pub(crate) use relation_policy::{
    builtin_relation_inventory, classify_builtin_relation_name, classify_relation_name,
    object_name_is_internal_storage_relation, object_name_is_protected_builtin_ddl_target,
    protected_builtin_public_surface_names, relation_policy_choice_summary, RelationPolicy,
};

pub(crate) fn build_builtin_surface_registry() -> SurfaceRegistry {
    let mut registry = SurfaceRegistry::new();
    registry.insert_descriptors(builtin_surface_descriptors());
    register_builtin_entity_surfaces(&mut registry);
    registry
}

pub(crate) fn register_dynamic_entity_surface_spec(
    registry: &mut SurfaceRegistry,
    spec: DynamicEntitySurfaceSpec,
) -> CatalogEpoch {
    let changed =
        registry.insert_descriptors(entity_surface_descriptors(&spec, CatalogSource::Dynamic));
    if changed {
        registry.advance_catalog_epoch();
    }
    registry.catalog_epoch()
}

pub(crate) fn remove_dynamic_entity_surfaces_for_schema_key(
    registry: &mut SurfaceRegistry,
    schema_key: &str,
) -> bool {
    let removed = registry.remove_descriptors_matching(|descriptor| {
        descriptor.catalog_source == CatalogSource::Dynamic
            && descriptor.implicit_overrides.fixed_schema_key.as_deref() == Some(schema_key)
    });
    if removed {
        registry.advance_catalog_epoch();
    }
    removed
}

pub(crate) fn apply_registered_schema_snapshot_to_surface_registry(
    registry: &mut SurfaceRegistry,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let (key, schema) = schema_from_registered_snapshot(snapshot)?;
    remove_dynamic_entity_surfaces_for_schema_key(registry, &key.schema_key);
    let spec = entity_surface_spec_from_schema(&schema)?;
    register_dynamic_entity_surface_spec(registry, spec);
    Ok(())
}

pub(crate) async fn load_public_surface_registry_with_backend(
    backend: &dyn LixBackend,
) -> Result<SurfaceRegistry, LixError> {
    let mut registry = build_builtin_surface_registry();
    for (_, schema) in load_latest_registered_schemas(backend).await? {
        let spec = entity_surface_spec_from_schema(&schema)?;
        register_dynamic_entity_surface_spec(&mut registry, spec);
    }
    Ok(registry)
}

async fn load_latest_registered_schemas(
    backend: &dyn LixBackend,
) -> Result<Vec<(SchemaKey, JsonValue)>, LixError> {
    let rows = scan_live_rows(
        backend,
        &LiveRowQuery {
            schema_key: "lix_registered_schema".to_string(),
            version_id: "global".to_string(),
            mode: RowReadMode::Tracked,
            constraints: Vec::new(),
            include_tombstones: false,
        },
    )
    .await?;

    let mut latest_by_schema_key = BTreeMap::<String, (SchemaKey, JsonValue)>::new();
    for row in &rows {
        let Some((key, schema)) = decode_registered_schema_row(row)? else {
            continue;
        };

        let should_replace = latest_by_schema_key
            .get(&key.schema_key)
            .map(|(existing, _)| schema_key_is_newer(&key, existing))
            .unwrap_or(true);
        if should_replace {
            latest_by_schema_key.insert(key.schema_key.clone(), (key, schema));
        }
    }

    Ok(latest_by_schema_key.into_values().collect())
}

fn builtin_surface_registry() -> &'static SurfaceRegistry {
    static BUILTIN_SURFACE_REGISTRY: OnceLock<SurfaceRegistry> = OnceLock::new();
    BUILTIN_SURFACE_REGISTRY.get_or_init(build_builtin_surface_registry)
}

pub(crate) fn builtin_public_surface_names() -> Vec<String> {
    builtin_surface_registry().public_surface_names()
}

pub(crate) fn builtin_public_surface_columns(relation_name: &str) -> Option<Vec<String>> {
    builtin_surface_registry().public_surface_columns(relation_name)
}

fn register_builtin_entity_surfaces(registry: &mut SurfaceRegistry) {
    for schema_key in builtin_schema_keys() {
        if !builtin_schema_exposed_as_entity_surface(schema_key) {
            continue;
        }
        let Some(schema) = builtin_schema_definition(schema_key) else {
            continue;
        };
        let Ok(spec) = entity_surface_spec_from_schema(schema) else {
            continue;
        };
        registry.insert_descriptors(entity_surface_descriptors(&spec, CatalogSource::Builtin));
    }
}

fn builtin_schema_exposed_as_entity_surface(schema_key: &str) -> bool {
    !matches!(schema_key, "lix_active_version" | "lix_active_account")
}

fn entity_surface_spec_from_schema(
    schema: &JsonValue,
) -> Result<DynamicEntitySurfaceSpec, LixError> {
    let schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "schema is missing string x-lix-key".to_string(),
        })?;

    let mut visible_columns = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            let mut columns = properties
                .keys()
                .filter(|key| !key.starts_with("lixcol_"))
                .cloned()
                .collect::<Vec<_>>();
            columns.sort();
            columns
        })
        .unwrap_or_default();
    visible_columns.dedup();
    let column_types = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            properties
                .iter()
                .filter(|(key, _)| !key.starts_with("lixcol_"))
                .filter_map(|(key, property_schema)| {
                    surface_column_type_from_schema(property_schema).map(|kind| (key.clone(), kind))
                })
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    let predicate_overrides = collect_override_predicates(schema, schema_key)?;

    Ok(DynamicEntitySurfaceSpec {
        schema_key: schema_key.to_string(),
        visible_columns,
        column_types,
        predicate_overrides,
    })
}

fn schema_key_is_newer(candidate: &SchemaKey, existing: &SchemaKey) -> bool {
    match (candidate.version_number(), existing.version_number()) {
        (Some(candidate_version), Some(existing_version)) => candidate_version > existing_version,
        _ => candidate.schema_version > existing.schema_version,
    }
}

fn collect_override_predicates(
    schema: &JsonValue,
    schema_key: &str,
) -> Result<Vec<SurfaceOverridePredicate>, LixError> {
    let mut predicates = Vec::new();
    for override_entry in collect_lixcol_overrides(schema, schema_key, shared_runtime())? {
        let Some(column) = (match override_entry.key.as_str() {
            "lixcol_entity_id" => Some("entity_id"),
            "lixcol_file_id" => Some("file_id"),
            "lixcol_plugin_key" => Some("plugin_key"),
            "lixcol_global" => Some("global"),
            "lixcol_metadata" => Some("metadata"),
            "lixcol_untracked" => Some("untracked"),
            _ => None,
        }) else {
            continue;
        };
        let value = match override_entry.value {
            LixcolOverrideValue::Null => SurfaceOverrideValue::Null,
            LixcolOverrideValue::Boolean(value) => SurfaceOverrideValue::Boolean(value),
            LixcolOverrideValue::Number(value) => SurfaceOverrideValue::Number(value),
            LixcolOverrideValue::String(value) => SurfaceOverrideValue::String(value),
        };
        predicates.push(SurfaceOverridePredicate {
            column: column.to_string(),
            value,
        });
    }
    Ok(predicates)
}

fn surface_column_type_from_schema(schema: &JsonValue) -> Option<SurfaceColumnType> {
    let types = match schema.get("type") {
        Some(JsonValue::String(kind)) => vec![kind.as_str()],
        Some(JsonValue::Array(kinds)) => kinds
            .iter()
            .filter_map(JsonValue::as_str)
            .collect::<Vec<_>>(),
        _ => return None,
    };

    if types.iter().any(|kind| *kind == "boolean") {
        return Some(SurfaceColumnType::Boolean);
    }
    if types.iter().any(|kind| *kind == "integer") {
        return Some(SurfaceColumnType::Integer);
    }
    if types.iter().any(|kind| *kind == "number") {
        return Some(SurfaceColumnType::Number);
    }
    if types.iter().any(|kind| *kind == "string") {
        return Some(SurfaceColumnType::String);
    }
    if types.iter().any(|kind| matches!(*kind, "object" | "array")) {
        return Some(SurfaceColumnType::Json);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{entity_surface_spec_from_schema, load_public_surface_registry_with_backend};
    use crate::contracts::surface::{SurfaceFamily, SurfaceOverrideValue};
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn entity_surface_spec_is_derived_from_schema_properties() {
        let spec = entity_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "properties": {
                "message": { "type": "string" },
                "id": { "type": "string" }
            }
        }))
        .expect("schema spec should derive");

        assert_eq!(spec.schema_key, "project_message");
        assert_eq!(
            spec.visible_columns,
            vec!["id".to_string(), "message".to_string()]
        );
    }

    #[test]
    fn entity_surface_spec_evaluates_override_metadata() {
        let spec = entity_surface_spec_from_schema(&json!({
            "x-lix-key": "message",
            "x-lix-version": "1",
            "x-lix-override-lixcols": {
                "lixcol_file_id": "\"lix\"",
                "lixcol_plugin_key": "\"lix\"",
                "lixcol_global": "true"
            },
            "properties": {
                "body": { "type": "string" },
                "id": { "type": "string" }
            }
        }))
        .expect("schema spec should derive");

        assert_eq!(spec.predicate_overrides.len(), 3);
        assert!(spec.predicate_overrides.iter().any(|predicate| {
            predicate.column == "global" && predicate.value == SurfaceOverrideValue::Boolean(true)
        }));
    }

    #[test]
    fn entity_surface_spec_rejects_removed_lixcol_version_override() {
        let err = entity_surface_spec_from_schema(&json!({
            "x-lix-key": "message",
            "x-lix-version": "1",
            "x-lix-override-lixcols": {
                "lixcol_version_id": "\"global\""
            },
            "properties": {
                "id": { "type": "string" }
            }
        }))
        .expect_err("removed lixcol_version_id override should be rejected");

        assert!(
            err.description
                .contains("x-lix-override-lixcols.lixcol_version_id"),
            "unexpected error: {err:?}"
        );
    }

    #[derive(Default)]
    struct FakeBackend {
        schema_rows: HashMap<String, String>,
    }

    fn is_registered_schema_live_scan(sql: &str) -> bool {
        sql.contains("lix_internal_live_v1_lix_registered_schema")
    }

    fn registered_schema_live_scan_rows(schema_rows: &HashMap<String, String>) -> Vec<Vec<Value>> {
        schema_rows
            .iter()
            .map(|(schema_key, snapshot)| {
                let value_json = serde_json::from_str::<serde_json::Value>(snapshot)
                    .ok()
                    .and_then(|value| value.get("value").cloned())
                    .unwrap_or(serde_json::Value::Null)
                    .to_string();
                vec![
                    Value::Text(format!("{schema_key}~1")),
                    Value::Text("lix_registered_schema".to_string()),
                    Value::Text("1".to_string()),
                    Value::Text("lix".to_string()),
                    Value::Text("global".to_string()),
                    Value::Boolean(true),
                    Value::Text("lix".to_string()),
                    Value::Null,
                    Value::Text(format!("change-{schema_key}")),
                    Value::Text("1970-01-01T00:00:00Z".to_string()),
                    Value::Text("1970-01-01T00:00:00Z".to_string()),
                    Value::Text(value_json),
                ]
            })
            .collect()
    }

    fn registered_schema_live_rows_for_projection(
        sql: &str,
        schema_rows: &HashMap<String, String>,
    ) -> (Vec<Vec<Value>>, Vec<String>) {
        if sql.contains("SELECT schema_version, value_json") {
            let rows = schema_rows
                .iter()
                .map(|(_, snapshot)| {
                    let value_json = serde_json::from_str::<serde_json::Value>(snapshot)
                        .ok()
                        .and_then(|value| value.get("value").cloned())
                        .unwrap_or(serde_json::Value::Null)
                        .to_string();
                    vec![Value::Text("1".to_string()), Value::Text(value_json)]
                })
                .collect::<Vec<_>>();
            return (
                rows,
                vec!["schema_version".to_string(), "value_json".to_string()],
            );
        }

        (
            registered_schema_live_scan_rows(schema_rows),
            vec![
                "entity_id".to_string(),
                "schema_key".to_string(),
                "schema_version".to_string(),
                "file_id".to_string(),
                "version_id".to_string(),
                "global".to_string(),
                "plugin_key".to_string(),
                "metadata".to_string(),
                "change_id".to_string(),
                "created_at".to_string(),
                "updated_at".to_string(),
                "value_json".to_string(),
            ],
        )
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if is_registered_schema_live_scan(sql) {
                let (rows, columns) =
                    registered_schema_live_rows_for_projection(sql, &self.schema_rows);
                return Ok(QueryResult { rows, columns });
            }
            if sql.contains("FROM lix_internal_registered_schema_bootstrap") {
                let rows = self
                    .schema_rows
                    .values()
                    .cloned()
                    .map(|snapshot| vec![Value::Text(snapshot)])
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["snapshot_content".to_string()],
                });
            }

            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            _mode: crate::TransactionMode,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "transactions are not needed in this test backend".to_string(),
            })
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "begin_savepoint not supported in test backend",
            ))
        }
    }

    #[tokio::test]
    async fn load_public_surface_registry_with_backend_loads_dynamic_schema_surfaces() {
        let mut backend = FakeBackend::default();
        backend.schema_rows.insert(
            "message".to_string(),
            r#"{"value":{"x-lix-key":"message","x-lix-version":"1","type":"object","properties":{"id":{"type":"string"},"body":{"type":"string"}}}}"#.to_string(),
        );

        let registry = load_public_surface_registry_with_backend(&backend)
            .await
            .expect("registry should bootstrap");
        let binding = registry
            .bind_relation_name("message")
            .expect("dynamic registered schema surface should bind");

        assert_eq!(binding.descriptor.surface_family, SurfaceFamily::Entity);
        assert!(binding.catalog_epoch.is_some());
        assert_eq!(
            binding.exposed_columns,
            vec!["body".to_string(), "id".to_string()]
        );
    }
}
