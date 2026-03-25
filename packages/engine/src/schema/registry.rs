use crate::schema::live_layout::{
    builtin_live_table_layout, merge_live_table_layouts, LiveTableLayout,
};
use crate::schema::schema_from_registered_snapshot;
use crate::sql::execution::contracts::planned_statement::SchemaLiveTableRequirement;
use crate::{LixBackend, LixBackendTransaction, LixError};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub fn coalesce_live_table_requirements(
    requirements: &[SchemaLiveTableRequirement],
) -> Vec<SchemaLiveTableRequirement> {
    let mut by_schema = BTreeMap::<String, SchemaLiveTableRequirement>::new();
    for requirement in requirements {
        by_schema
            .entry(requirement.schema_key.clone())
            .and_modify(|existing| {
                if existing.layout.is_none() && requirement.layout.is_some() {
                    existing.layout = requirement.layout.clone();
                }
            })
            .or_insert_with(|| requirement.clone());
    }
    by_schema.into_values().collect()
}

pub(crate) async fn load_live_table_layout_with_backend(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<LiveTableLayout, LixError> {
    if let Some(layout) = builtin_live_table_layout(schema_key)? {
        return Ok(layout);
    }

    let sql = format!(
        "SELECT snapshot_content \
         FROM lix_internal_registered_schema_bootstrap \
         WHERE schema_key = 'lix_registered_schema' \
           AND version_id = 'global' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL",
    );
    let result = backend.execute(&sql, &[]).await?;
    compile_registered_live_layout(schema_key, result.rows.into_iter().collect())
}

pub(crate) async fn load_live_table_layout_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    schema_key: &str,
) -> Result<LiveTableLayout, LixError> {
    if let Some(layout) = builtin_live_table_layout(schema_key)? {
        return Ok(layout);
    }

    let sql = format!(
        "SELECT snapshot_content \
         FROM lix_internal_registered_schema_bootstrap \
         WHERE schema_key = 'lix_registered_schema' \
           AND version_id = 'global' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL",
    );
    let result = transaction.execute(&sql, &[]).await?;
    compile_registered_live_layout(schema_key, result.rows.into_iter().collect())
}

pub(crate) fn compile_registered_live_layout(
    schema_key: &str,
    rows: Vec<Vec<crate::Value>>,
) -> Result<LiveTableLayout, LixError> {
    let mut layouts = Vec::new();
    for row in rows {
        let Some(crate::Value::Text(snapshot_content)) = row.first() else {
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
        layouts.push(crate::schema::live_layout::live_table_layout_from_schema(
            &schema,
        )?);
    }

    if layouts.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("schema '{}' is not stored", schema_key),
        ));
    }

    merge_live_table_layouts(schema_key, layouts)
}
