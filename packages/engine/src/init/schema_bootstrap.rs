use crate::common::errors;
use crate::engine::builtin_schema_entity_id;
use crate::init::InitExecutor;
use crate::live_state::register_schema;
use crate::schema::{builtin_schema_definition, builtin_schema_keys};
use crate::Value;
use crate::{LixBackend, LixError};

pub(crate) async fn init_builtin_schema_storage(backend: &dyn LixBackend) -> Result<(), LixError> {
    for schema_key in builtin_schema_keys() {
        register_schema(backend, *schema_key).await?;
    }
    Ok(())
}

pub(crate) async fn seed_builtin_registered_schemas(
    executor: &mut InitExecutor<'_, '_>,
) -> Result<(), LixError> {
    executor.seed_builtin_registered_schemas().await
}

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) async fn seed_builtin_registered_schemas(&mut self) -> Result<(), LixError> {
        for schema_key in builtin_schema_keys() {
            let schema = builtin_schema_definition(schema_key).ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("builtin schema '{schema_key}' is not available"),
            })?;
            let entity_id = builtin_schema_entity_id(schema)?;

            let existing = self
                .execute_internal(
                    "SELECT 1 FROM lix_state_by_version \
                     WHERE schema_key = 'lix_registered_schema' \
                       AND entity_id = $1 \
                       AND version_id = 'global' \
                       AND snapshot_content IS NOT NULL \
                     LIMIT 1",
                    &[Value::Text(entity_id.clone())],
                )
                .await?;
            let [statement] = existing.statements.as_slice() else {
                return Err(errors::unexpected_statement_count_error(
                    "builtin schema existence query",
                    1,
                    existing.statements.len(),
                ));
            };
            if !statement.rows.is_empty() {
                continue;
            }

            let snapshot_content = serde_json::json!({
                "value": schema
            })
            .to_string();
            self.execute_internal(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, created_at, updated_at, untracked\
                 ) VALUES ($1, 'lix_registered_schema', 'lix', 'global', 'lix', $2, '1', '1970-01-01T00:00:00Z', '1970-01-01T00:00:00Z', true)",
                &[
                    Value::Text(entity_id),
                    Value::Text(snapshot_content),
                ],
            )
            .await?;
        }

        Ok(())
    }
}
