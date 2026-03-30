use super::layout::{
    builtin_live_table_layout, live_table_layout_from_schema, merge_live_table_layouts,
    LiveTableLayout,
};
use super::sql::ensure_schema_live_table_sql_statements;
use crate::live_state::SchemaRegistration;
use crate::schema::schema_from_registered_snapshot;
use crate::{LixBackend, LixBackendTransaction, LixError, Value};
use async_trait::async_trait;
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveTableRequirement {
    pub(crate) schema_key: String,
    pub(crate) layout: Option<LiveTableLayout>,
}

pub(crate) async fn register_schema(
    backend: &dyn LixBackend,
    registration: &SchemaRegistration,
) -> Result<(), LixError> {
    ensure_schema_live_table_with_requirement(
        backend,
        &requirement_from_registration(registration)?,
    )
    .await
}

pub(crate) async fn register_schema_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    registration: &SchemaRegistration,
) -> Result<(), LixError> {
    ensure_schema_live_table_with_requirement_in_transaction(
        transaction,
        &requirement_from_registration(registration)?,
    )
    .await
}

pub(crate) async fn ensure_schema_live_table_with_requirement(
    backend: &dyn LixBackend,
    requirement: &LiveTableRequirement,
) -> Result<(), LixError> {
    let layout = match requirement.layout.as_ref() {
        Some(layout) => layout.clone(),
        None => load_live_table_layout_with_backend(backend, &requirement.schema_key).await?,
    };
    for statement in
        ensure_schema_live_table_sql_statements(&requirement.schema_key, backend.dialect(), &layout)
    {
        backend.execute(&statement, &[]).await?;
    }
    Ok(())
}

pub(crate) async fn ensure_schema_live_table_with_requirement_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    requirement: &LiveTableRequirement,
) -> Result<(), LixError> {
    let layout = match requirement.layout.as_ref() {
        Some(layout) => layout.clone(),
        None => load_live_table_layout_in_transaction(transaction, &requirement.schema_key).await?,
    };
    for statement in ensure_schema_live_table_sql_statements(
        &requirement.schema_key,
        transaction.dialect(),
        &layout,
    ) {
        transaction.execute(&statement, &[]).await?;
    }
    Ok(())
}

pub(crate) async fn load_live_table_layout_with_backend(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<LiveTableLayout, LixError> {
    let mut provider = BackendSchemaLayoutProvider { backend };
    load_live_table_layout_with_provider(&mut provider, schema_key).await
}

pub(crate) async fn load_live_table_layout_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    schema_key: &str,
) -> Result<LiveTableLayout, LixError> {
    let mut provider = TransactionSchemaLayoutProvider { transaction };
    load_live_table_layout_with_provider(&mut provider, schema_key).await
}

#[async_trait(?Send)]
trait RegisteredSchemaLayoutProvider {
    async fn load_registered_schema_rows(&mut self) -> Result<Vec<Vec<Value>>, LixError>;
}

struct BackendSchemaLayoutProvider<'a> {
    backend: &'a dyn LixBackend,
}

#[async_trait(?Send)]
impl RegisteredSchemaLayoutProvider for BackendSchemaLayoutProvider<'_> {
    async fn load_registered_schema_rows(&mut self) -> Result<Vec<Vec<Value>>, LixError> {
        let result = self
            .backend
            .execute(REGISTERED_SCHEMA_LAYOUT_SQL, &[])
            .await?;
        Ok(result.rows.into_iter().collect())
    }
}

struct TransactionSchemaLayoutProvider<'a> {
    transaction: &'a mut dyn LixBackendTransaction,
}

#[async_trait(?Send)]
impl RegisteredSchemaLayoutProvider for TransactionSchemaLayoutProvider<'_> {
    async fn load_registered_schema_rows(&mut self) -> Result<Vec<Vec<Value>>, LixError> {
        let result = self
            .transaction
            .execute(REGISTERED_SCHEMA_LAYOUT_SQL, &[])
            .await?;
        Ok(result.rows.into_iter().collect())
    }
}

const REGISTERED_SCHEMA_LAYOUT_SQL: &str = "SELECT snapshot_content \
     FROM lix_internal_registered_schema_bootstrap \
     WHERE schema_key = 'lix_registered_schema' \
       AND version_id = 'global' \
       AND is_tombstone = 0 \
       AND snapshot_content IS NOT NULL";

async fn load_live_table_layout_with_provider(
    provider: &mut dyn RegisteredSchemaLayoutProvider,
    schema_key: &str,
) -> Result<LiveTableLayout, LixError> {
    if let Some(layout) = builtin_live_table_layout(schema_key)? {
        return Ok(layout);
    }

    compile_registered_live_layout(schema_key, provider.load_registered_schema_rows().await?)
}

fn requirement_from_registration(
    registration: &SchemaRegistration,
) -> Result<LiveTableRequirement, LixError> {
    let layout = if let Some(schema_definition) = registration.schema_definition_override() {
        Some(live_table_layout_from_schema(schema_definition)?)
    } else {
        registration
            .registered_snapshot()
            .map(|snapshot| {
                let (_, schema) = schema_from_registered_snapshot(snapshot)?;
                live_table_layout_from_schema(&schema)
            })
            .transpose()?
    };
    Ok(LiveTableRequirement {
        schema_key: registration.schema_key().to_string(),
        layout,
    })
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
