use crate::init::seed::{system_directory_name, text_value};
use crate::init::InitExecutor;
use crate::LixError;
use crate::Value;

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
        let existing = match parent_id {
            Some(parent_id) => {
                self.execute_internal(
                    "SELECT id \
                     FROM lix_directory_by_version \
                     WHERE lixcol_version_id = 'global' \
                       AND name = $1 \
                       AND parent_id = $2 \
                     LIMIT 1",
                    &[
                        Value::Text(name.clone()),
                        Value::Text(parent_id.to_string()),
                    ],
                )
                .await?
            }
            None => {
                self.execute_internal(
                    "SELECT id \
                     FROM lix_directory_by_version \
                     WHERE lixcol_version_id = 'global' \
                       AND name = $1 \
                       AND parent_id IS NULL \
                     LIMIT 1",
                    &[Value::Text(name.clone())],
                )
                .await?
            }
        };
        let [statement] = existing.statements.as_slice() else {
            return Err(crate::common::unexpected_statement_count_error(
                "system directory existence query",
                1,
                existing.statements.len(),
            ));
        };
        if let Some(row) = statement.rows.first() {
            return text_value(row.first(), "system directory entity_id");
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
            "lix",
            "global",
            "lix",
            &snapshot_content,
        )
        .await?;

        Ok(entity_id)
    }
}
