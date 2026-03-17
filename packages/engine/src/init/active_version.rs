use crate::engine::Engine;
use crate::schema::live_layout::{
    builtin_live_table_layout, live_column_name_for_property, tracked_live_table_name,
    untracked_live_table_name,
};
use crate::state::commit::load_committed_version_head_commit_id_from_live_state;
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    GLOBAL_VERSION_ID,
};
use crate::{LixError, Value};

impl Engine {
    pub(crate) async fn load_latest_commit_id(&self) -> Result<Option<String>, LixError> {
        let mut executor = &*self.backend;
        if let Some(commit_id) =
            load_committed_version_head_commit_id_from_live_state(&mut executor, GLOBAL_VERSION_ID)
                .await?
        {
            return Ok(Some(commit_id));
        }

        let commit_table = tracked_live_table_name("lix_commit");
        let has_commits = self
            .backend
            .execute(
                &format!(
                    "SELECT 1 \
                     FROM {commit_table} \
                     WHERE schema_key = 'lix_commit' \
                       AND version_id = 'global' \
                       AND is_tombstone = 0 \
                     LIMIT 1"
                ),
                &[],
            )
            .await?
            .rows
            .first()
            .is_some();
        if has_commits {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description:
                    "init invariant violation: commits exist but hidden global version ref is missing"
                        .to_string(),
            });
        }

        Ok(None)
    }

    pub(crate) async fn generate_runtime_uuid(&self) -> Result<String, LixError> {
        let (settings, sequence_start, functions) = self
            .prepare_runtime_functions_with_backend(self.backend.as_ref(), false)
            .await?;
        let uuid = functions.call_uuid_v7();
        self.persist_runtime_sequence_with_backend(
            self.backend.as_ref(),
            settings,
            sequence_start,
            &functions,
        )
        .await?;
        Ok(uuid)
    }

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
