use crate::contracts::surface::SurfaceRegistry;
use crate::refs::load_current_committed_version_frontier_with_backend;
use crate::schema::{SchemaProvider, SqlRegisteredSchemaProvider};
use crate::sql::prepare::SqlCompilerMetadata;
use crate::{LixBackend, LixError};

pub(crate) async fn load_sql_compiler_metadata(
    backend: &dyn LixBackend,
    registry: &SurfaceRegistry,
) -> Result<SqlCompilerMetadata, LixError> {
    let mut provider = SqlRegisteredSchemaProvider::new(backend);
    let mut known_live_schema_definitions = std::collections::BTreeMap::new();
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
