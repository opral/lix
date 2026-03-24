use crate::engine::Engine;
use crate::schema::live_layout::{
    builtin_live_table_layout, live_column_name_for_property, untracked_live_table_name,
};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
};
use crate::{LixError, Value};

impl Engine {
    pub(crate) async fn load_and_cache_active_version(&self) -> Result<(), LixError> {
        let layout = builtin_live_table_layout(active_version_schema_key())?.ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "builtin active version schema must compile to a live layout",
            )
        })?;
        let payload_version_column = live_column_name_for_property(&layout, "version_id")
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "active version live layout is missing version_id",
                )
            })?;
        let result = self
            .backend
            .execute(
                &format!(
                    "SELECT {payload_version_column} \
                     FROM {table_name} \
                     WHERE file_id = $1 \
                       AND version_id = $2 \
                       AND untracked = true \
                       AND {payload_version_column} IS NOT NULL \
                     ORDER BY updated_at DESC \
                     LIMIT 1",
                    payload_version_column = payload_version_column,
                    table_name = untracked_live_table_name(active_version_schema_key()),
                ),
                &[
                    Value::Text(active_version_file_id().to_string()),
                    Value::Text(active_version_storage_version_id().to_string()),
                ],
            )
            .await?;

        if let Some(row) = result.rows.first() {
            let active_version_id = row.first().ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "active version query row is missing version_id".to_string(),
            })?;
            let active_version_id = match active_version_id {
                Value::Text(value) => value.clone(),
                other => {
                    return Err(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: format!("active version id must be text, got {other:?}"),
                    })
                }
            };
            self.set_active_version_id(active_version_id);
            return Ok(());
        }

        self.clear_active_version_id();
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "engine invariant violation: active version row is missing".to_string(),
        })
    }
}
