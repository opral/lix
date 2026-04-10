use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use crate::catalog::{
    build_builtin_surface_registry, dynamic_entity_surface_spec_from_schema,
    register_dynamic_entity_surface_spec, SurfaceRegistry,
};
use crate::live_state::{decode_registered_schema_row, scan_live_rows, LiveRowQuery, RowReadMode};
use crate::runtime::cel::shared_runtime;
use crate::schema::SchemaKey;
use crate::{LixBackend, LixError};

pub(crate) async fn load_public_surface_registry_with_backend(
    backend: &dyn LixBackend,
) -> Result<SurfaceRegistry, LixError> {
    let mut registry = build_builtin_surface_registry();
    let evaluator = shared_runtime();
    for (_, schema) in load_latest_registered_schemas(backend).await? {
        let spec = dynamic_entity_surface_spec_from_schema(&schema, evaluator)?;
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
    use crate::runtime::cel::shared_runtime;
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn entity_surface_spec_is_derived_from_schema_properties() {
        let spec = dynamic_entity_surface_spec_from_schema(
            &json!({
                "x-lix-key": "project_message",
                "properties": {
                    "message": { "type": "string" },
                    "id": { "type": "string" }
                }
            }),
            shared_runtime(),
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
        )
        .expect("schema spec should derive");

        assert_eq!(spec.predicate_overrides.len(), 3);
        assert!(spec.predicate_overrides.iter().any(|predicate| {
            predicate.column == "global" && predicate.value == SurfaceOverrideValue::Boolean(true)
        }));
    }

    #[test]
    fn entity_surface_spec_rejects_removed_lixcol_version_override() {
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
