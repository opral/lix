use crate::contracts::surface::SurfaceRegistry;
use crate::schema::{SchemaProvider, SqlRegisteredSchemaProvider};
use crate::version::load_current_committed_version_frontier_with_backend;
use crate::{LixBackend, LixError};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default)]
pub(crate) struct SqlCompilerMetadata {
    pub(crate) known_live_schema_definitions: BTreeMap<String, JsonValue>,
    pub(crate) current_version_heads: Option<BTreeMap<String, String>>,
}

pub(crate) async fn load_sql_compiler_metadata(
    backend: &dyn LixBackend,
    registry: &SurfaceRegistry,
) -> Result<SqlCompilerMetadata, LixError> {
    let mut provider = SqlRegisteredSchemaProvider::new(backend);
    let mut known_live_schema_definitions = BTreeMap::new();
    for schema_key in registry.registered_state_surface_schema_keys() {
        known_live_schema_definitions.insert(
            schema_key.clone(),
            provider.load_latest_schema(&schema_key).await?,
        );
    }

    let current_version_heads =
        match load_current_committed_version_frontier_with_backend(backend).await {
            Ok(frontier) => Some(frontier.version_heads),
            Err(error)
                if error
                    .description
                    .contains("schema 'lix_version' is not stored") =>
            {
                None
            }
            Err(error) => return Err(error),
        };

    Ok(SqlCompilerMetadata {
        known_live_schema_definitions,
        current_version_heads,
    })
}
