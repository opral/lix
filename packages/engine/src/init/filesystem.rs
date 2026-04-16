use crate::live_state::tracked_relation_name;
use crate::LixError;
use crate::Value;

use super::seed::{system_directory_name, text_value, InitExecutor};

pub(crate) async fn seed_bootstrap(executor: &mut InitExecutor<'_, '_>) -> Result<(), LixError> {
    executor.seed_global_system_directories().await
}

const SYSTEM_ROOT_DIRECTORY_PATH: &str = "/.lix/";
const SYSTEM_APP_DATA_DIRECTORY_PATH: &str = "/.lix/app_data/";
const SYSTEM_PLUGIN_DIRECTORY_PATH: &str = "/.lix/plugins/";

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) async fn seed_global_system_directories(&mut self) -> Result<(), LixError> {
        let bootstrap_commit_id = self.load_global_version_commit_id().await?;
        let root_id = self
            .ensure_seeded_system_directory(&bootstrap_commit_id, SYSTEM_ROOT_DIRECTORY_PATH, None)
            .await?;
        self.ensure_seeded_system_directory(
            &bootstrap_commit_id,
            SYSTEM_APP_DATA_DIRECTORY_PATH,
            Some(root_id.as_str()),
        )
        .await?;
        self.ensure_seeded_system_directory(
            &bootstrap_commit_id,
            SYSTEM_PLUGIN_DIRECTORY_PATH,
            Some(root_id.as_str()),
        )
        .await?;

        Ok(())
    }

    async fn ensure_seeded_system_directory(
        &mut self,
        bootstrap_commit_id: &str,
        path: &str,
        parent_id: Option<&str>,
    ) -> Result<String, LixError> {
        let name = system_directory_name(path);
        if let Some(entity_id) = self
            .load_seeded_system_directory_id(&name, parent_id)
            .await?
        {
            return Ok(entity_id);
        }

        let entity_id = self.generate_runtime_uuid().await?;
        let parent_id_json = parent_id.map(ToString::to_string);
        let snapshot_content = serde_json::json!({
            "id": entity_id,
            "parent_id": parent_id_json,
            "name": name,
            "hidden": true,
        })
        .to_string();
        self.insert_bootstrap_tracked_row(
            Some(bootstrap_commit_id),
            &entity_id,
            "lix_directory_descriptor",
            "1",
            None,
            "global",
            None,
            &snapshot_content,
        )
        .await?;

        Ok(entity_id)
    }

    async fn load_seeded_system_directory_id(
        &mut self,
        name: &str,
        parent_id: Option<&str>,
    ) -> Result<Option<String>, LixError> {
        let directory_table = tracked_relation_name("lix_directory_descriptor");
        let existing = match parent_id {
            Some(parent_id) => {
                self.execute_backend(
                    &format!(
                        "SELECT entity_id \
                         FROM {directory_table} \
                         WHERE schema_key = 'lix_directory_descriptor' \
                           AND file_id IS NULL \
                           AND version_id = 'global' \
                           AND untracked = false \
                           AND is_tombstone = 0 \
                           AND name = $1 \
                           AND parent_id = $2 \
                         ORDER BY updated_at DESC, created_at DESC, entity_id DESC \
                         LIMIT 1"
                    ),
                    &[
                        Value::Text(name.to_string()),
                        Value::Text(parent_id.to_string()),
                    ],
                )
                .await?
            }
            None => {
                self.execute_backend(
                    &format!(
                        "SELECT entity_id \
                         FROM {directory_table} \
                         WHERE schema_key = 'lix_directory_descriptor' \
                           AND file_id IS NULL \
                           AND version_id = 'global' \
                           AND untracked = false \
                           AND is_tombstone = 0 \
                           AND name = $1 \
                           AND parent_id IS NULL \
                         ORDER BY updated_at DESC, created_at DESC, entity_id DESC \
                         LIMIT 1"
                    ),
                    &[Value::Text(name.to_string())],
                )
                .await?
            }
        };
        Ok(existing
            .rows
            .first()
            .map(|row| text_value(row.first(), "system directory entity_id"))
            .transpose()?)
    }
}
