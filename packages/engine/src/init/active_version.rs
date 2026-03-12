use crate::engine::Engine;
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    parse_active_version_snapshot, version_pointer_file_id, version_pointer_schema_key,
    version_pointer_storage_version_id, GLOBAL_VERSION_ID,
};
use crate::{LixError, Value};

impl Engine {
    pub(crate) async fn load_latest_commit_id(&self) -> Result<Option<String>, LixError> {
        let pointer_result = self
            .backend
            .execute(
                "SELECT snapshot_content \
                 FROM lix_internal_state_materialized_v1_lix_version_pointer \
                 WHERE schema_key = $1 \
                   AND entity_id = $2 \
                   AND file_id = $3 \
                   AND version_id = $4 \
                   AND is_tombstone = 0 \
                   AND snapshot_content IS NOT NULL \
                 ORDER BY updated_at DESC, created_at DESC, change_id DESC \
                 LIMIT 1",
                &[
                    Value::Text(version_pointer_schema_key().to_string()),
                    Value::Text(GLOBAL_VERSION_ID.to_string()),
                    Value::Text(version_pointer_file_id().to_string()),
                    Value::Text(version_pointer_storage_version_id().to_string()),
                ],
            )
            .await?;
        if let Some(row) = pointer_result.rows.first() {
            if let Some(Value::Text(snapshot_content)) = row.first() {
                let snapshot: crate::schema::builtin::types::LixVersionPointer =
                    serde_json::from_str(snapshot_content).map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                            "global version pointer snapshot_content invalid JSON while loading latest commit id: {error}"
                        ),
                    })?;
                if !snapshot.commit_id.is_empty() {
                    return Ok(Some(snapshot.commit_id));
                }
            }
        }

        let has_commits = self
            .backend
            .execute(
                "SELECT 1 \
                 FROM lix_internal_state_materialized_v1_lix_commit \
                 WHERE schema_key = 'lix_commit' \
                   AND version_id = 'global' \
                   AND is_tombstone = 0 \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
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
                    "init invariant violation: commits exist but hidden global version pointer is missing"
                        .to_string(),
            });
        }

        Ok(None)
    }

    pub(crate) async fn generate_runtime_uuid(&self) -> Result<String, LixError> {
        let (settings, sequence_start, functions) = self
            .prepare_runtime_functions_with_backend(self.backend.as_ref())
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
        let result = self
            .backend
            .execute(
                "SELECT snapshot_content \
                 FROM lix_internal_state_untracked \
                 WHERE schema_key = $1 \
                   AND file_id = $2 \
                   AND version_id = $3 \
                   AND snapshot_content IS NOT NULL \
                 ORDER BY updated_at DESC \
                 LIMIT 1",
                &[
                    Value::Text(active_version_schema_key().to_string()),
                    Value::Text(active_version_file_id().to_string()),
                    Value::Text(active_version_storage_version_id().to_string()),
                ],
            )
            .await?;

        if let Some(row) = result.rows.first() {
            let snapshot_content = row.first().ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "active version query row is missing snapshot_content".to_string(),
            })?;
            let snapshot_content = match snapshot_content {
                Value::Text(value) => value.as_str(),
                other => {
                    return Err(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: format!(
                            "active version snapshot_content must be text, got {other:?}"
                        ),
                    })
                }
            };
            let active_version_id = parse_active_version_snapshot(snapshot_content)?;
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
