use std::collections::HashMap;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::{LixBackend, LixError, Value};

use super::key::SchemaKey;

const STORED_SCHEMA_TABLE: &str = "lix_internal_state_materialized_v1_lix_stored_schema";
const GLOBAL_VERSION: &str = "global";

#[async_trait(?Send)]
pub trait SchemaProvider {
    async fn load_schema(&mut self, key: &SchemaKey) -> Result<JsonValue, LixError>;
    async fn load_latest_schema(&mut self, schema_key: &str) -> Result<JsonValue, LixError>;
}

pub struct SqlStoredSchemaProvider<'a> {
    backend: &'a dyn LixBackend,
    cache: HashMap<SchemaKey, JsonValue>,
}

impl<'a> SqlStoredSchemaProvider<'a> {
    pub fn new(backend: &'a dyn LixBackend) -> Self {
        Self {
            backend,
            cache: HashMap::new(),
        }
    }

    pub(crate) async fn load_latest_schema_entry(
        &mut self,
        schema_key: &str,
    ) -> Result<Option<(SchemaKey, JsonValue)>, LixError> {
        let prefix = format!("{schema_key}~");
        let prefix_escaped = escape_sql_string(&prefix);
        let prefix_len = prefix.len();
        let sql = format!(
            "SELECT schema_version, snapshot_content \
             FROM {table} \
             WHERE substr(entity_id, 1, {prefix_len}) = '{prefix_escaped}' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
             ORDER BY CAST(schema_version AS INTEGER) DESC \
             LIMIT 1",
            table = STORED_SCHEMA_TABLE,
            prefix_len = prefix_len,
            prefix_escaped = prefix_escaped,
            global_version = GLOBAL_VERSION,
        );

        let result = self.backend.execute(&sql, &[]).await?;
        let Some(row) = result.rows.first() else {
            return Ok(None);
        };

        let schema_version = value_to_string(&row[0], "schema_version")?;
        let snapshot_content = value_to_string(&row[1], "snapshot_content")?;
        let schema = schema_from_snapshot_content(&snapshot_content)?;
        let key = SchemaKey::new(schema_key.to_string(), schema_version);

        self.cache.insert(key.clone(), schema.clone());

        Ok(Some((key, schema)))
    }

    async fn load_schema_row(&self, key: &SchemaKey) -> Result<Option<JsonValue>, LixError> {
        let entity_id = escape_sql_string(&key.entity_id());
        let sql = format!(
            "SELECT snapshot_content FROM {table} \
             WHERE entity_id = '{entity_id}' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
             LIMIT 1",
            table = STORED_SCHEMA_TABLE,
            entity_id = entity_id,
            global_version = GLOBAL_VERSION,
        );

        let result = self.backend.execute(&sql, &[]).await?;
        let Some(row) = result.rows.first() else {
            return Ok(None);
        };

        let snapshot_content = value_to_string(&row[0], "snapshot_content")?;
        let schema = schema_from_snapshot_content(&snapshot_content)?;

        Ok(Some(schema))
    }
}

#[async_trait(?Send)]
impl SchemaProvider for SqlStoredSchemaProvider<'_> {
    async fn load_schema(&mut self, key: &SchemaKey) -> Result<JsonValue, LixError> {
        if let Some(schema) = self.cache.get(key) {
            return Ok(schema.clone());
        }

        let schema = self.load_schema_row(key).await?.ok_or_else(|| LixError {
            message: format!(
                "schema '{}' ({}) is not stored",
                key.schema_key, key.schema_version
            ),
        })?;

        self.cache.insert(key.clone(), schema.clone());

        Ok(schema)
    }

    async fn load_latest_schema(&mut self, schema_key: &str) -> Result<JsonValue, LixError> {
        let Some((_, schema)) = self.load_latest_schema_entry(schema_key).await? else {
            return Err(LixError {
                message: format!("schema '{}' is not stored", schema_key),
            });
        };

        Ok(schema)
    }
}

fn schema_from_snapshot_content(raw: &str) -> Result<JsonValue, LixError> {
    let parsed: JsonValue = serde_json::from_str(raw).map_err(|err| LixError {
        message: format!("stored schema snapshot_content invalid JSON: {err}"),
    })?;

    parsed.get("value").cloned().ok_or_else(|| LixError {
        message: "stored schema snapshot_content missing value".to_string(),
    })
}

fn escape_sql_string(input: &str) -> String {
    input.replace('\'', "''")
}

fn value_to_string(value: &Value, name: &str) -> Result<String, LixError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            message: format!("expected text value for {name}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use serde_json::{json, Value as JsonValue};

    use crate::{LixBackend, LixError, QueryResult, Value};

    use super::{SchemaKey, SchemaProvider, SqlStoredSchemaProvider};

    #[derive(Default)]
    struct FakeBackend {
        schema_rows: HashMap<String, String>,
        latest_rows: HashMap<String, (String, String)>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl FakeBackend {
        fn with_schema(mut self, entity_id: &str, snapshot_content: &str) -> Self {
            self.schema_rows
                .insert(entity_id.to_string(), snapshot_content.to_string());
            self
        }

        fn with_latest(
            mut self,
            schema_key: &str,
            schema_version: &str,
            snapshot_content: &str,
        ) -> Self {
            self.latest_rows.insert(
                schema_key.to_string(),
                (schema_version.to_string(), snapshot_content.to_string()),
            );
            self
        }

        fn query_count_containing(&self, needle: &str) -> usize {
            self.calls
                .lock()
                .expect("calls mutex poisoned")
                .iter()
                .filter(|sql| sql.contains(needle))
                .count()
        }
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        async fn execute(&self, sql: &str, _: &[Value]) -> Result<QueryResult, LixError> {
            self.calls
                .lock()
                .expect("calls mutex poisoned")
                .push(sql.to_string());

            if sql.contains("SELECT snapshot_content FROM") {
                let entity_id = extract_single_quoted(sql, "entity_id = '");
                if let Some(entity_id) = entity_id {
                    if let Some(snapshot_content) = self.schema_rows.get(&entity_id) {
                        return Ok(QueryResult {
                            rows: vec![vec![Value::Text(snapshot_content.clone())]],
                        });
                    }
                }
                return Ok(QueryResult { rows: Vec::new() });
            }

            if sql.contains("SELECT schema_version, snapshot_content") {
                for (schema_key, (schema_version, snapshot_content)) in &self.latest_rows {
                    if sql.contains(&format!("'{schema_key}~'")) {
                        return Ok(QueryResult {
                            rows: vec![vec![
                                Value::Text(schema_version.clone()),
                                Value::Text(snapshot_content.clone()),
                            ]],
                        });
                    }
                }
                return Ok(QueryResult { rows: Vec::new() });
            }

            Err(LixError {
                message: format!("unexpected SQL in FakeBackend: {sql}"),
            })
        }
    }

    fn extract_single_quoted(sql: &str, prefix: &str) -> Option<String> {
        let start = sql.find(prefix)?;
        let from = start + prefix.len();
        let tail = &sql[from..];
        let end = tail.find('\'')?;
        Some(tail[..end].to_string())
    }

    fn stored_snapshot(schema: JsonValue) -> String {
        json!({ "value": schema }).to_string()
    }

    #[tokio::test]
    async fn load_schema_uses_cache_after_first_fetch() {
        let backend = FakeBackend::default().with_schema(
            "users~1",
            &stored_snapshot(json!({
                "x-lix-key": "users",
                "x-lix-version": "1",
                "type": "object"
            })),
        );
        let mut provider = SqlStoredSchemaProvider::new(&backend);
        let key = SchemaKey::new("users", "1");

        let first = provider.load_schema(&key).await.expect("first load");
        let second = provider.load_schema(&key).await.expect("second load");

        assert_eq!(first, second);
        assert_eq!(
            backend.query_count_containing("SELECT snapshot_content FROM"),
            1
        );
    }

    #[tokio::test]
    async fn load_schema_returns_missing_error() {
        let backend = FakeBackend::default();
        let mut provider = SqlStoredSchemaProvider::new(&backend);
        let key = SchemaKey::new("missing", "1");

        let err = provider
            .load_schema(&key)
            .await
            .expect_err("should return missing schema error");
        assert!(err.message.contains("is not stored"), "{err:?}");
    }

    #[tokio::test]
    async fn load_latest_populates_cache_for_exact_version() {
        let backend = FakeBackend::default().with_latest(
            "users",
            "2",
            &stored_snapshot(json!({
                "x-lix-key": "users",
                "x-lix-version": "2",
                "type": "object"
            })),
        );
        let mut provider = SqlStoredSchemaProvider::new(&backend);

        let latest = provider
            .load_latest_schema("users")
            .await
            .expect("latest schema");
        assert_eq!(latest["x-lix-version"], json!("2"));

        let cached = provider
            .load_schema(&SchemaKey::new("users", "2"))
            .await
            .expect("cached schema");
        assert_eq!(cached["x-lix-key"], json!("users"));
        assert_eq!(
            backend.query_count_containing("SELECT snapshot_content FROM"),
            0
        );
        assert_eq!(
            backend.query_count_containing("SELECT schema_version, snapshot_content"),
            1
        );
    }

    #[tokio::test]
    async fn load_schema_rejects_invalid_snapshot_content() {
        let backend = FakeBackend::default().with_schema("users~1", "{not-json");
        let mut provider = SqlStoredSchemaProvider::new(&backend);

        let err = provider
            .load_schema(&SchemaKey::new("users", "1"))
            .await
            .expect_err("should fail");
        assert!(err.message.contains("invalid JSON"), "{err:?}");
    }
}
