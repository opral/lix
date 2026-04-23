use super::layout::{
    builtin_live_table_layout, live_table_layout_from_schema, merge_live_table_layouts,
    LiveTableLayout,
};
use crate::live_state::store::LiveStateBackendRef;
use crate::schema::schema_from_registered_snapshot;
use crate::{LixError, Value};
use async_trait::async_trait;
use serde_json::Value as JsonValue;

pub(crate) async fn load_live_table_layout_with_backend(
    backend: LiveStateBackendRef<'_>,
    schema_key: &str,
) -> Result<LiveTableLayout, LixError> {
    let mut provider = BackendSchemaLayoutProvider { backend };
    if schema_key.starts_with("same_request") {
        eprintln!("versionless layout lookup for {}", schema_key);
    }
    match load_live_table_layout_with_provider(&mut provider, schema_key).await {
        Ok(layout) => Ok(layout),
        Err(error) if is_schema_not_stored_error(&error, schema_key) => {
            load_live_table_layout_from_registered_schema_live_table(backend, schema_key)
                .await
                .or(Err(error))
        }
        Err(error) => Err(error),
    }
}

pub(crate) async fn load_live_table_layout_for_version_with_backend(
    backend: LiveStateBackendRef<'_>,
    schema_key: &str,
    requested_version_id: &str,
) -> Result<LiveTableLayout, LixError> {
    if let Some(layout) = builtin_live_table_layout(schema_key)? {
        return Ok(layout);
    }

    let mut provider = BackendVersionScopedSchemaLayoutProvider {
        backend,
        requested_version_id,
    };
    let provider_rows = provider.load_registered_schema_rows().await?;
    match compile_registered_live_layout(schema_key, provider_rows) {
        Ok(layout) => Ok(layout),
        Err(error) if is_schema_not_stored_error(&error, schema_key) => {
            compile_registered_live_layout(
                schema_key,
                crate::live_state::storage::load_registered_schema_live_table_layout_rows_for_version(
                    backend,
                    requested_version_id,
                )
                .await?,
            )
            .or(Err(error))
        }
        Err(error) => Err(error),
    }
}

#[async_trait]
trait RegisteredSchemaLayoutProvider: Send {
    async fn load_registered_schema_rows(&mut self) -> Result<Vec<Vec<Value>>, LixError>;
}

struct BackendSchemaLayoutProvider<'a> {
    backend: LiveStateBackendRef<'a>,
}

struct BackendVersionScopedSchemaLayoutProvider<'a> {
    backend: LiveStateBackendRef<'a>,
    requested_version_id: &'a str,
}

#[async_trait]
impl RegisteredSchemaLayoutProvider for BackendSchemaLayoutProvider<'_> {
    async fn load_registered_schema_rows(&mut self) -> Result<Vec<Vec<Value>>, LixError> {
        crate::live_state::storage::load_registered_schema_layout_rows_with_backend(self.backend)
            .await
    }
}

#[async_trait]
impl RegisteredSchemaLayoutProvider for BackendVersionScopedSchemaLayoutProvider<'_> {
    async fn load_registered_schema_rows(&mut self) -> Result<Vec<Vec<Value>>, LixError> {
        crate::live_state::storage::load_registered_schema_layout_rows_for_version_with_backend(
            self.backend,
            self.requested_version_id,
        )
        .await
    }
}

async fn load_live_table_layout_with_provider(
    provider: &mut dyn RegisteredSchemaLayoutProvider,
    schema_key: &str,
) -> Result<LiveTableLayout, LixError> {
    if let Some(layout) = builtin_live_table_layout(schema_key)? {
        return Ok(layout);
    }

    compile_registered_live_layout(schema_key, provider.load_registered_schema_rows().await?)
}

async fn load_live_table_layout_from_registered_schema_live_table(
    backend: LiveStateBackendRef<'_>,
    schema_key: &str,
) -> Result<LiveTableLayout, LixError> {
    compile_registered_live_layout(
        schema_key,
        crate::live_state::storage::load_registered_schema_live_table_layout_rows(backend).await?,
    )
}

pub(crate) fn compile_registered_live_layout(
    schema_key: &str,
    rows: Vec<Vec<Value>>,
) -> Result<LiveTableLayout, LixError> {
    let mut layouts = Vec::new();
    for row in rows {
        let Some(Value::Text(snapshot_content)) = row.first() else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "registered schema bootstrap lookup for '{}' returned a non-text snapshot_content",
                    schema_key
                ),
            ));
        };
        let snapshot: JsonValue = serde_json::from_str(snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "registered schema bootstrap snapshot_content for '{}' is invalid JSON: {error}",
                    schema_key
                ),
            )
        })?;
        let (key, schema) = schema_from_registered_snapshot(&snapshot)?;
        if key.schema_key != schema_key {
            continue;
        }
        layouts.push(live_table_layout_from_schema(&schema)?);
    }

    if layouts.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("schema '{}' is not stored", schema_key),
        ));
    }

    merge_live_table_layouts(schema_key, layouts)
}

fn is_schema_not_stored_error(error: &LixError, schema_key: &str) -> bool {
    error
        .description
        .starts_with(&format!("schema '{}' is not stored", schema_key))
}

#[cfg(test)]
mod tests {
    use super::compile_registered_live_layout;
    use crate::Value;

    #[test]
    fn compile_registered_live_layout_extracts_matching_schema() {
        let rows = vec![vec![Value::Text(
            serde_json::json!({
                "value": {
                    "x-lix-key": "profile",
                    "x-lix-version": "1",
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" }
                    }
                }
            })
            .to_string(),
        )]];

        let layout =
            compile_registered_live_layout("profile", rows).expect("layout should compile");
        assert_eq!(layout.schema_key, "profile");
        assert_eq!(layout.columns.len(), 1);
        assert_eq!(layout.columns[0].column_name, "name");
    }
}
