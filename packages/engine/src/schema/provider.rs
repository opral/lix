use std::collections::HashMap;

use async_trait::async_trait;
use serde_json::{json, Value as JsonValue};

use crate::sql::storage::sql_text::escape_sql_string;
use crate::{LixBackend, LixError, Value};

use super::builtin::{builtin_schema_definition, builtin_schema_keys};
use super::key::{schema_from_registered_snapshot, schema_key_from_definition, SchemaKey};

const REGISTERED_SCHEMA_TABLE: &str = "lix_internal_registered_schema_bootstrap";
const GLOBAL_VERSION: &str = "global";
#[async_trait(?Send)]
pub trait SchemaProvider {
    async fn load_schema(&mut self, key: &SchemaKey) -> Result<JsonValue, LixError>;
    async fn load_latest_schema(&mut self, schema_key: &str) -> Result<JsonValue, LixError>;
    async fn load_visible_schema_entries(
        &mut self,
    ) -> Result<Vec<(SchemaKey, JsonValue)>, LixError>;
}

pub struct SqlRegisteredSchemaProvider<'a> {
    backend: &'a dyn LixBackend,
    cache: HashMap<SchemaKey, JsonValue>,
}

impl<'a> SqlRegisteredSchemaProvider<'a> {
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
            table = REGISTERED_SCHEMA_TABLE,
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

    pub(crate) async fn load_latest_schema_entries(
        &mut self,
    ) -> Result<Vec<(SchemaKey, JsonValue)>, LixError> {
        let sql = format!(
            "SELECT snapshot_content FROM {table} \
             WHERE version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL",
            table = REGISTERED_SCHEMA_TABLE,
            global_version = GLOBAL_VERSION,
        );

        let result = self.backend.execute(&sql, &[]).await?;
        let mut latest_by_schema_key = HashMap::<String, (SchemaKey, JsonValue)>::new();
        for row in result.rows {
            let snapshot_content = value_to_string(&row[0], "snapshot_content")?;
            let snapshot: JsonValue =
                serde_json::from_str(&snapshot_content).map_err(|err| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!("registered schema snapshot_content invalid JSON: {err}"),
                })?;
            let (key, schema) = schema_from_registered_snapshot(&snapshot)?;
            self.cache.insert(key.clone(), schema.clone());

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

    pub(crate) async fn load_stored_schema_entries(
        &mut self,
    ) -> Result<Vec<(SchemaKey, JsonValue)>, LixError> {
        let sql = format!(
            "SELECT snapshot_content FROM {table} \
             WHERE version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL",
            table = REGISTERED_SCHEMA_TABLE,
            global_version = GLOBAL_VERSION,
        );

        let result = self.backend.execute(&sql, &[]).await?;
        let mut entries = Vec::new();
        for row in result.rows {
            let snapshot_content = value_to_string(&row[0], "snapshot_content")?;
            let snapshot: JsonValue =
                serde_json::from_str(&snapshot_content).map_err(|err| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!("registered schema snapshot_content invalid JSON: {err}"),
                })?;
            let (key, schema) = schema_from_registered_snapshot(&snapshot)?;
            self.cache.insert(key.clone(), schema.clone());
            entries.push((key, schema));
        }

        Ok(entries)
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
            table = REGISTERED_SCHEMA_TABLE,
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
impl SchemaProvider for SqlRegisteredSchemaProvider<'_> {
    async fn load_schema(&mut self, key: &SchemaKey) -> Result<JsonValue, LixError> {
        if let Some(schema) = builtin_schema_for_key(key) {
            self.cache.insert(key.clone(), schema.clone());
            return Ok(schema);
        }

        if let Some(schema) = self.cache.get(key) {
            return Ok(schema.clone());
        }

        let schema = self.load_schema_row(key).await?.ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "schema '{}' ({}) is not stored",
                key.schema_key, key.schema_version
            ),
        })?;

        self.cache.insert(key.clone(), schema.clone());

        Ok(schema)
    }

    async fn load_latest_schema(&mut self, schema_key: &str) -> Result<JsonValue, LixError> {
        if let Some(schema) = whitelisted_internal_schema(schema_key) {
            let schema_version = builtin_schema_version(&schema)?;
            let key = SchemaKey::new(schema_key.to_string(), schema_version);
            self.cache.insert(key, schema.clone());
            return Ok(schema);
        }

        if let Some(schema) = builtin_schema_definition(schema_key) {
            let schema_version = builtin_schema_version(schema)?;
            let key = SchemaKey::new(schema_key.to_string(), schema_version);
            self.cache.insert(key, schema.clone());
            return Ok(schema.clone());
        }

        let Some((_, schema)) = self.load_latest_schema_entry(schema_key).await? else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("schema '{}' is not stored", schema_key),
            });
        };

        Ok(schema)
    }

    async fn load_visible_schema_entries(
        &mut self,
    ) -> Result<Vec<(SchemaKey, JsonValue)>, LixError> {
        let mut entries_by_key = HashMap::<SchemaKey, JsonValue>::new();

        if let Some(schema) = whitelisted_internal_schema("lix_state") {
            let schema_version = builtin_schema_version(&schema)?;
            let key = SchemaKey::new("lix_state", schema_version);
            self.cache.insert(key.clone(), schema.clone());
            entries_by_key.insert(key, schema);
        }

        for schema_key in builtin_schema_keys() {
            let Some(schema) = builtin_schema_definition(schema_key) else {
                continue;
            };
            let key = schema_key_from_definition(schema)?;
            self.cache.insert(key.clone(), schema.clone());
            entries_by_key.insert(key, schema.clone());
        }

        for (key, schema) in self.load_stored_schema_entries().await? {
            entries_by_key.insert(key, schema);
        }

        Ok(entries_by_key.into_iter().collect())
    }
}

fn builtin_schema_for_key(key: &SchemaKey) -> Option<JsonValue> {
    if let Some(schema) = whitelisted_internal_schema(&key.schema_key) {
        let schema_version = schema.get("x-lix-version").and_then(JsonValue::as_str)?;
        if schema_version == key.schema_version {
            return Some(schema);
        }
    }

    let schema = builtin_schema_definition(&key.schema_key)?;
    let schema_version = schema.get("x-lix-version").and_then(JsonValue::as_str)?;
    if schema_version != key.schema_version {
        return None;
    }
    Some(schema.clone())
}

fn builtin_schema_version(schema: &JsonValue) -> Result<String, LixError> {
    schema
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "builtin schema is missing string x-lix-version".to_string(),
        })
}

fn whitelisted_internal_schema(schema_key: &str) -> Option<JsonValue> {
    match schema_key {
        "lix_state" => Some(json!({
            "x-lix-key": "lix_state",
            "x-lix-version": "1",
            "x-lix-primary-key": [
                "/entity_id",
                "/schema_key",
                "/file_id"
            ],
            "type": "object",
            "properties": {
                "entity_id": { "type": "string" },
                "schema_key": { "type": "string" },
                "file_id": { "type": "string" }
            },
            "required": [
                "entity_id",
                "schema_key",
                "file_id"
            ],
            "additionalProperties": true
        })),
        _ => None,
    }
}

fn schema_from_snapshot_content(raw: &str) -> Result<JsonValue, LixError> {
    let parsed: JsonValue = serde_json::from_str(raw).map_err(|err| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("registered schema snapshot_content invalid JSON: {err}"),
    })?;

    parsed.get("value").cloned().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "registered schema snapshot_content missing value".to_string(),
    })
}

fn value_to_string(value: &Value, name: &str) -> Result<String, LixError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("expected text value for {name}"),
        }),
    }
}

fn schema_key_is_newer(candidate: &SchemaKey, existing: &SchemaKey) -> bool {
    match (candidate.version_number(), existing.version_number()) {
        (Some(candidate), Some(existing)) => candidate > existing,
        _ => candidate.schema_version > existing.schema_version,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use serde_json::{json, Value as JsonValue};

    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};

    use super::{SchemaKey, SchemaProvider, SqlRegisteredSchemaProvider};

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
                            columns: vec!["snapshot_content".to_string()],
                        });
                    }
                } else {
                    return Ok(QueryResult {
                        rows: self
                            .schema_rows
                            .values()
                            .cloned()
                            .map(|snapshot_content| vec![Value::Text(snapshot_content)])
                            .collect(),
                        columns: vec!["snapshot_content".to_string()],
                    });
                }
                return Ok(QueryResult {
                    rows: Vec::new(),
                    columns: Vec::new(),
                });
            }

            if sql.contains("SELECT schema_version, snapshot_content") {
                for (schema_key, (schema_version, snapshot_content)) in &self.latest_rows {
                    if sql.contains(&format!("'{schema_key}~'")) {
                        return Ok(QueryResult {
                            rows: vec![vec![
                                Value::Text(schema_version.clone()),
                                Value::Text(snapshot_content.clone()),
                            ]],
                            columns: vec![
                                "schema_version".to_string(),
                                "snapshot_content".to_string(),
                            ],
                        });
                    }
                }
                return Ok(QueryResult {
                    rows: Vec::new(),
                    columns: Vec::new(),
                });
            }

            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("unexpected SQL in FakeBackend: {sql}"),
            })
        }

        async fn begin_transaction(
            &self,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "FakeBackend does not support transactions".to_string(),
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
        let mut provider = SqlRegisteredSchemaProvider::new(&backend);
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
        let mut provider = SqlRegisteredSchemaProvider::new(&backend);
        let key = SchemaKey::new("missing", "1");

        let err = provider
            .load_schema(&key)
            .await
            .expect_err("should return missing schema error");
        assert!(err.description.contains("is not stored"), "{err:?}");
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
        let mut provider = SqlRegisteredSchemaProvider::new(&backend);

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
        let mut provider = SqlRegisteredSchemaProvider::new(&backend);

        let err = provider
            .load_schema(&SchemaKey::new("users", "1"))
            .await
            .expect_err("should fail");
        assert!(err.description.contains("invalid JSON"), "{err:?}");
    }

    #[tokio::test]
    async fn load_latest_uses_internal_whitelist_for_lix_state() {
        let backend = FakeBackend::default();
        let mut provider = SqlRegisteredSchemaProvider::new(&backend);

        let schema = provider
            .load_latest_schema("lix_state")
            .await
            .expect("loads internal schema");

        assert_eq!(schema["x-lix-key"], json!("lix_state"));
        assert_eq!(
            backend.query_count_containing("SELECT schema_version, snapshot_content"),
            0
        );
    }

    #[tokio::test]
    async fn load_schema_uses_internal_whitelist_for_lix_state() {
        let backend = FakeBackend::default();
        let mut provider = SqlRegisteredSchemaProvider::new(&backend);

        let schema = provider
            .load_schema(&SchemaKey::new("lix_state", "1"))
            .await
            .expect("loads internal schema");

        assert_eq!(schema["x-lix-key"], json!("lix_state"));
        assert_eq!(
            backend.query_count_containing("SELECT snapshot_content FROM"),
            0
        );
    }

    #[tokio::test]
    async fn load_latest_schema_entries_returns_latest_version_per_schema_key() {
        let backend = FakeBackend::default()
            .with_schema(
                "users~1",
                &stored_snapshot(json!({
                    "x-lix-key": "users",
                    "x-lix-version": "1",
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                })),
            )
            .with_schema(
                "users~2",
                &stored_snapshot(json!({
                    "x-lix-key": "users",
                    "x-lix-version": "2",
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "name": { "type": "string" }
                    }
                })),
            )
            .with_schema(
                "projects~3",
                &stored_snapshot(json!({
                    "x-lix-key": "projects",
                    "x-lix-version": "3",
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" }
                    }
                })),
            );
        let mut provider = SqlRegisteredSchemaProvider::new(&backend);
        let mut entries = provider
            .load_latest_schema_entries()
            .await
            .expect("entries should load");
        entries.sort_by(|left, right| left.0.schema_key.cmp(&right.0.schema_key));

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, SchemaKey::new("projects", "3"));
        assert_eq!(entries[1].0, SchemaKey::new("users", "2"));
        assert_eq!(entries[1].1["properties"]["name"]["type"], json!("string"));
    }
}
