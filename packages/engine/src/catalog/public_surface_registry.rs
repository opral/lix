use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use crate::catalog::{
    build_builtin_surface_registry, dynamic_entity_surface_spec_from_schema,
    register_dynamic_entity_surface_spec, remove_dynamic_entity_surfaces_for_schema_key,
    SurfaceRegistry,
};
use crate::functions::DynFunctionProvider;
use crate::live_state::{
    decode_registered_schema_row, scan_live_rows, LiveRowQuery, LiveRowSource,
};
use crate::schema::schema_from_registered_snapshot;
use crate::schema::SchemaAnnotationEvaluator;
use crate::schema::SchemaKey;
use crate::{LixBackend, LixError};

pub(crate) async fn load_public_surface_registry_with_backend(
    backend: &dyn LixBackend,
    evaluator: &dyn SchemaAnnotationEvaluator,
    functions: &DynFunctionProvider,
) -> Result<SurfaceRegistry, LixError> {
    let mut registry = build_builtin_surface_registry();
    for (_, schema) in load_latest_registered_schemas(backend).await? {
        let spec = dynamic_entity_surface_spec_from_schema(&schema, evaluator, functions)?;
        register_dynamic_entity_surface_spec(&mut registry, spec);
    }
    Ok(registry)
}

pub(crate) fn apply_registered_schema_snapshot_to_surface_registry(
    registry: &mut SurfaceRegistry,
    snapshot: &JsonValue,
    evaluator: &dyn SchemaAnnotationEvaluator,
    functions: &DynFunctionProvider,
) -> Result<(), LixError> {
    let (key, schema) = schema_from_registered_snapshot(snapshot)?;
    remove_dynamic_entity_surfaces_for_schema_key(registry, &key.schema_key);
    let spec = dynamic_entity_surface_spec_from_schema(&schema, evaluator, functions)?;
    register_dynamic_entity_surface_spec(registry, spec);
    Ok(())
}

async fn load_latest_registered_schemas(
    backend: &dyn LixBackend,
) -> Result<Vec<(SchemaKey, JsonValue)>, LixError> {
    let rows = scan_live_rows(
        backend,
        &LiveRowQuery {
            schema_key: "lix_registered_schema".to_string(),
            version_id: "global".to_string(),
            source: LiveRowSource::Tracked,
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

fn schema_key_is_newer(candidate: &SchemaKey, existing: &SchemaKey) -> bool {
    match (candidate.version_number(), existing.version_number()) {
        (Some(candidate_version), Some(existing_version)) => candidate_version > existing_version,
        _ => candidate.schema_version > existing.schema_version,
    }
}

#[cfg(test)]
mod tests {
    use super::load_public_surface_registry_with_backend;
    use crate::catalog::{
        dynamic_entity_surface_spec_from_schema, SurfaceFamily, SurfaceOverrideValue,
    };
    use crate::cel::shared_runtime;
    use crate::functions::SystemFunctionProvider;
    use crate::functions::{clone_boxed_function_provider, SharedFunctionProvider};
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use serde_json::json;
    use std::collections::HashMap;

    fn system_functions() -> crate::functions::DynFunctionProvider {
        clone_boxed_function_provider(&SharedFunctionProvider::new(SystemFunctionProvider))
    }

    #[test]
    fn entity_surface_spec_is_derived_from_schema_properties() {
        let functions = system_functions();
        let spec = dynamic_entity_surface_spec_from_schema(
            &json!({
                "x-lix-key": "project_message",
                "properties": {
                    "message": { "type": "string" },
                    "id": { "type": "string" }
                }
            }),
            shared_runtime(),
            &functions,
        )
        .expect("schema spec should derive");

        assert_eq!(spec.schema_key, "project_message");
        assert_eq!(
            spec.visible_columns,
            vec!["id".to_string(), "message".to_string()]
        );
    }

    #[test]
    fn entity_surface_spec_evaluates_override_metadata() {
        let functions = system_functions();
        let spec = dynamic_entity_surface_spec_from_schema(
            &json!({
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
            }),
            shared_runtime(),
            &functions,
        )
        .expect("schema spec should derive");

        assert_eq!(spec.predicate_overrides.len(), 3);
        assert!(spec.predicate_overrides.iter().any(|predicate| {
            predicate.column == "global" && predicate.value == SurfaceOverrideValue::Boolean(true)
        }));
    }

    #[test]
    fn entity_surface_spec_rejects_removed_lixcol_version_override() {
        let functions = system_functions();
        let err = dynamic_entity_surface_spec_from_schema(
            &json!({
                "x-lix-key": "message",
                "x-lix-version": "1",
                "x-lix-override-lixcols": {
                    "lixcol_version_id": "\"global\""
                },
                "properties": {
                    "id": { "type": "string" }
                }
            }),
            shared_runtime(),
            &functions,
        )
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
            _mode: crate::backend::TransactionBeginMode,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "transactions are not needed in this test",
            ))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "savepoints are not needed in this test",
            ))
        }
    }

    #[tokio::test]
    async fn load_public_surface_registry_with_backend_loads_dynamic_schema_surfaces() {
        let backend = FakeBackend {
            schema_rows: HashMap::from([(
                "message".to_string(),
                json!({
                    "value": {
                        "x-lix-key": "message",
                        "x-lix-version": "1",
                        "properties": {
                            "id": { "type": "string" },
                            "body": { "type": "string" }
                        }
                    }
                })
                .to_string(),
            )]),
        };

        let functions = system_functions();
        let registry =
            load_public_surface_registry_with_backend(&backend, shared_runtime(), &functions)
                .await
                .expect("load surface registry");

        let descriptor = registry
            .bind_relation_name("message")
            .map(|binding| binding.descriptor)
            .expect("dynamic message surface should exist");
        assert_eq!(descriptor.surface_family, SurfaceFamily::Entity);
        assert_eq!(
            descriptor.visible_columns,
            vec!["body".to_string(), "id".to_string()]
        );
    }
}
