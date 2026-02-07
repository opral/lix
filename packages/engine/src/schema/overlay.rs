use std::cmp::Ordering;
use std::collections::HashMap;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::{LixBackend, LixError};

use super::key::{schema_from_stored_snapshot, SchemaKey};
use super::provider::{SchemaProvider, SqlStoredSchemaProvider};

pub struct OverlaySchemaProvider<'a> {
    base: SqlStoredSchemaProvider<'a>,
    pending: HashMap<SchemaKey, JsonValue>,
}

impl<'a> OverlaySchemaProvider<'a> {
    pub fn from_backend(backend: &'a dyn LixBackend) -> Self {
        Self::new(SqlStoredSchemaProvider::new(backend))
    }

    pub fn new(base: SqlStoredSchemaProvider<'a>) -> Self {
        Self {
            base,
            pending: HashMap::new(),
        }
    }

    pub fn remember_pending_schema(&mut self, key: SchemaKey, schema: JsonValue) {
        self.pending.insert(key, schema);
    }

    pub fn remember_pending_schema_from_snapshot(
        &mut self,
        snapshot: &JsonValue,
    ) -> Result<(), LixError> {
        let (key, schema) = schema_from_stored_snapshot(snapshot)?;
        self.pending.insert(key, schema);
        Ok(())
    }

    fn latest_pending_schema(&self, schema_key: &str) -> Option<(SchemaKey, JsonValue)> {
        self.pending
            .iter()
            .filter(|(key, _)| key.schema_key == schema_key)
            .max_by(|(left, _), (right, _)| compare_schema_keys(left, right))
            .map(|(key, schema)| (key.clone(), schema.clone()))
    }
}

#[async_trait(?Send)]
impl SchemaProvider for OverlaySchemaProvider<'_> {
    async fn load_schema(&mut self, key: &SchemaKey) -> Result<JsonValue, LixError> {
        if let Some(schema) = self.pending.get(key) {
            return Ok(schema.clone());
        }

        self.base.load_schema(key).await
    }

    async fn load_latest_schema(&mut self, schema_key: &str) -> Result<JsonValue, LixError> {
        let pending_latest = self.latest_pending_schema(schema_key);
        let stored_latest = self.base.load_latest_schema_entry(schema_key).await?;

        match (pending_latest, stored_latest) {
            (Some((pending_key, pending_schema)), Some((stored_key, stored_schema))) => {
                if compare_schema_keys(&pending_key, &stored_key) != Ordering::Less {
                    Ok(pending_schema)
                } else {
                    Ok(stored_schema)
                }
            }
            (Some((_, pending_schema)), None) => Ok(pending_schema),
            (None, Some((_, stored_schema))) => Ok(stored_schema),
            (None, None) => Err(LixError {
                message: format!("schema '{}' is not stored", schema_key),
            }),
        }
    }
}

fn compare_schema_keys(left: &SchemaKey, right: &SchemaKey) -> Ordering {
    match (left.version_number(), right.version_number()) {
        (Some(left_version), Some(right_version)) => left_version.cmp(&right_version),
        _ => left.schema_version.cmp(&right.schema_version),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use serde_json::{json, Value as JsonValue};

    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};

    use super::{OverlaySchemaProvider, SchemaKey, SchemaProvider, SqlStoredSchemaProvider};

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
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

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
    async fn load_schema_prefers_pending_without_fallback_query() {
        let backend = FakeBackend::default().with_schema(
            "users~1",
            &stored_snapshot(json!({
                "x-lix-key": "users",
                "x-lix-version": "1",
                "type": "object",
                "title": "stored"
            })),
        );
        let base = SqlStoredSchemaProvider::new(&backend);
        let mut provider = OverlaySchemaProvider::new(base);
        provider.remember_pending_schema(
            SchemaKey::new("users", "1"),
            json!({
                "x-lix-key": "users",
                "x-lix-version": "1",
                "type": "object",
                "title": "pending"
            }),
        );

        let loaded = provider
            .load_schema(&SchemaKey::new("users", "1"))
            .await
            .expect("load pending schema");

        assert_eq!(loaded["title"], json!("pending"));
        assert_eq!(
            backend.query_count_containing("SELECT snapshot_content FROM"),
            0
        );
    }

    #[tokio::test]
    async fn load_latest_prefers_pending_when_newer() {
        let backend = FakeBackend::default().with_latest(
            "users",
            "1",
            &stored_snapshot(json!({
                "x-lix-key": "users",
                "x-lix-version": "1",
                "type": "object"
            })),
        );
        let base = SqlStoredSchemaProvider::new(&backend);
        let mut provider = OverlaySchemaProvider::new(base);
        provider.remember_pending_schema(
            SchemaKey::new("users", "2"),
            json!({
                "x-lix-key": "users",
                "x-lix-version": "2",
                "type": "object"
            }),
        );

        let latest = provider
            .load_latest_schema("users")
            .await
            .expect("latest schema");

        assert_eq!(latest["x-lix-version"], json!("2"));
    }

    #[tokio::test]
    async fn load_latest_prefers_stored_when_newer() {
        let backend = FakeBackend::default().with_latest(
            "users",
            "10",
            &stored_snapshot(json!({
                "x-lix-key": "users",
                "x-lix-version": "10",
                "type": "object"
            })),
        );
        let base = SqlStoredSchemaProvider::new(&backend);
        let mut provider = OverlaySchemaProvider::new(base);
        provider.remember_pending_schema(
            SchemaKey::new("users", "2"),
            json!({
                "x-lix-key": "users",
                "x-lix-version": "2",
                "type": "object"
            }),
        );

        let latest = provider
            .load_latest_schema("users")
            .await
            .expect("latest schema");

        assert_eq!(latest["x-lix-version"], json!("10"));
    }

    #[test]
    fn remember_pending_schema_from_snapshot_validates_shape() {
        let backend = FakeBackend::default();
        let base = SqlStoredSchemaProvider::new(&backend);
        let mut provider = OverlaySchemaProvider::new(base);

        let err = provider
            .remember_pending_schema_from_snapshot(&json!({}))
            .expect_err("should fail");
        assert!(err.message.contains("missing value"), "{err:?}");
    }
}
