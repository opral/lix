use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::live_state::{
    decode_registered_schema_row, LiveStateContext, LiveStateFilter, LiveStateScanRequest,
};
use crate::schema::{builtin_schema_definition, builtin_schema_keys, schema_key_from_definition};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixError, NullableKeyFilter};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

/// Engine2 schema visibility boundary.
///
/// SQL planning receives a schema snapshot from here instead of reaching into
/// backend storage or hardcoding a small list of surfaces. Builtin schemas are
/// the bootstrap base; registered-schema rows visible through live_state can
/// extend or override that base for the requested version.
pub(crate) struct SchemaRegistry;

impl SchemaRegistry {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Loads schema definitions visible for SQL planning at `version_id`.
    pub(crate) async fn visible_schemas(
        &self,
        live_state: Arc<dyn LiveStateContext>,
        version_id: &str,
    ) -> Result<Vec<JsonValue>, LixError> {
        let mut schemas = builtin_schema_definitions()?;
        for version_id in [GLOBAL_VERSION_ID, version_id] {
            for row in live_state
                .scan(&LiveStateScanRequest {
                    filter: LiveStateFilter {
                        schema_keys: vec![REGISTERED_SCHEMA_KEY.to_string()],
                        version_ids: vec![version_id.to_string()],
                        file_ids: vec![NullableKeyFilter::Null],
                        include_tombstones: false,
                        ..LiveStateFilter::default()
                    },
                    ..LiveStateScanRequest::default()
                })
                .await?
            {
                let Some((key, schema)) = decode_registered_schema_row(&row)? else {
                    continue;
                };
                upsert_latest_schema(&mut schemas, key, schema);
            }
        }
        Ok(schemas.into_values().map(|(_, schema)| schema).collect())
    }
}

fn builtin_schema_definitions(
) -> Result<BTreeMap<String, (crate::schema::SchemaKey, JsonValue)>, LixError> {
    let mut schemas = BTreeMap::new();
    for schema_key in builtin_schema_keys() {
        let schema = builtin_schema_definition(schema_key)
            .ok_or_else(|| LixError::unknown(format!("missing builtin schema '{schema_key}'")))?;
        let key = schema_key_from_definition(schema)?;
        upsert_latest_schema(&mut schemas, key, schema.clone());
    }
    Ok(schemas)
}

fn upsert_latest_schema(
    schemas: &mut BTreeMap<String, (crate::schema::SchemaKey, JsonValue)>,
    key: crate::schema::SchemaKey,
    schema: JsonValue,
) {
    let should_replace = schemas
        .get(&key.schema_key)
        .is_none_or(|(existing, _)| !schema_key_is_older(&key, existing));
    if should_replace {
        schemas.insert(key.schema_key.clone(), (key, schema));
    }
}

fn schema_key_is_older(
    candidate: &crate::schema::SchemaKey,
    existing: &crate::schema::SchemaKey,
) -> bool {
    match (candidate.version_number(), existing.version_number()) {
        (Some(candidate_version), Some(existing_version)) => candidate_version < existing_version,
        _ => candidate.schema_version < existing.schema_version,
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::live_state::{ExactRowRequest, LiveRow};

    #[tokio::test]
    async fn visible_schemas_include_builtin_registered_schema() {
        let registry = SchemaRegistry::new();

        let schemas = registry
            .visible_schemas(Arc::new(RowsLiveStateContext::new(Vec::new())), "global")
            .await
            .expect("schema visibility should load");

        assert!(schemas.iter().any(|schema| {
            schema.get("x-lix-key").and_then(JsonValue::as_str) == Some("lix_registered_schema")
        }));
        assert!(schemas.iter().any(|schema| {
            schema.get("x-lix-key").and_then(JsonValue::as_str) == Some("lix_key_value")
        }));
    }

    #[tokio::test]
    async fn visible_schemas_include_registered_schema_rows() {
        let registry = SchemaRegistry::new();

        let schemas = registry
            .visible_schemas(
                Arc::new(RowsLiveStateContext::new(vec![registered_schema_row(
                    "engine2_dynamic_schema",
                    "1",
                )])),
                "global",
            )
            .await
            .expect("schema visibility should load");

        assert!(schemas.iter().any(|schema| {
            schema.get("x-lix-key").and_then(JsonValue::as_str) == Some("engine2_dynamic_schema")
        }));
    }

    struct RowsLiveStateContext {
        rows: Vec<LiveRow>,
    }

    impl RowsLiveStateContext {
        fn new(rows: Vec<LiveRow>) -> Self {
            Self { rows }
        }
    }

    #[async_trait]
    impl LiveStateContext for RowsLiveStateContext {
        async fn scan(&self, request: &LiveStateScanRequest) -> Result<Vec<LiveRow>, LixError> {
            Ok(self
                .rows
                .iter()
                .filter(|row| {
                    request.filter.schema_keys.is_empty()
                        || request.filter.schema_keys.contains(&row.schema_key)
                })
                .filter(|row| {
                    request.filter.version_ids.is_empty()
                        || request.filter.version_ids.contains(&row.version_id)
                })
                .cloned()
                .collect())
        }

        async fn load_exact(
            &self,
            _request: &ExactRowRequest,
        ) -> Result<Option<LiveRow>, LixError> {
            Ok(None)
        }
    }

    fn registered_schema_row(schema_key: &str, schema_version: &str) -> LiveRow {
        LiveRow {
            entity_id: format!("{schema_key}~{schema_version}"),
            file_id: None,
            schema_key: REGISTERED_SCHEMA_KEY.to_string(),
            schema_version: "1".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            plugin_key: None,
            metadata: None,
            change_id: None,
            commit_id: None,
            global: true,
            untracked: true,
            created_at: None,
            updated_at: None,
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
                        "additionalProperties": false
                    }
                })
                .to_string(),
            ),
        }
    }
}
