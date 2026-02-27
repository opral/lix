use super::*;

impl Engine {
    pub(crate) async fn load_latest_commit_id(&self) -> Result<Option<String>, LixError> {
        let pointer_result = self
            .backend
            .execute(
                "SELECT snapshot_content \
                 FROM lix_internal_state_materialized_v1_lix_version_pointer \
                 WHERE schema_key = 'lix_version_pointer' \
                   AND entity_id = 'global' \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND is_tombstone = 0 \
                   AND snapshot_content IS NOT NULL \
                 ORDER BY updated_at DESC, created_at DESC, change_id DESC \
                 LIMIT 1",
                &[],
            )
            .await?;
        if let Some(row) = pointer_result.rows.first() {
            if let Some(Value::Text(snapshot_content)) = row.first() {
                let snapshot: crate::builtin_schema::types::LixVersionPointer =
                    serde_json::from_str(snapshot_content).map_err(|error| LixError {
                        message: format!(
                            "version pointer snapshot_content invalid JSON while loading latest commit id: {error}"
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
                message:
                    "init invariant violation: commits exist but global version pointer is missing"
                        .to_string(),
            });
        }

        Ok(None)
    }

    pub(crate) async fn generate_runtime_uuid(&self) -> Result<String, LixError> {
        let result = self
            .execute("SELECT lix_uuid_v7()", &[], ExecuteOptions::default())
            .await?;
        let row = result.rows.first().ok_or_else(|| LixError {
            message: "lix_uuid_v7 query returned no rows".to_string(),
        })?;
        let value = row.first().ok_or_else(|| LixError {
            message: "lix_uuid_v7 query returned no columns".to_string(),
        })?;
        match value {
            Value::Text(text) => Ok(text.clone()),
            other => Err(LixError {
                message: format!("lix_uuid_v7 query returned non-text value: {other:?}"),
            }),
        }
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
                message: "active version query row is missing snapshot_content".to_string(),
            })?;
            let snapshot_content = match snapshot_content {
                Value::Text(value) => value.as_str(),
                other => {
                    return Err(LixError {
                        message: format!(
                            "active version snapshot_content must be text, got {other:?}"
                        ),
                    })
                }
            };
            let active_version_id = parse_active_version_snapshot(snapshot_content)?;
            self.set_active_version_id(active_version_id);
            return Ok(());
        }

        self.set_active_version_id(GLOBAL_VERSION_ID.to_string());
        Ok(())
    }
}
