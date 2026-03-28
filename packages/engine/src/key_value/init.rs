use crate::init::seed::{
    normalized_insert_columns_sql, normalized_insert_literals_sql, normalized_seed_values,
    quote_ident,
};
use crate::init::InitExecutor;
use crate::live_state::schema_access::tracked_relation_name;
use crate::sql_support::text::escape_sql_string;
use crate::{LixBackend, LixError};

pub(crate) async fn init(_backend: &dyn LixBackend) -> Result<(), LixError> {
    Ok(())
}

pub(crate) async fn seed_bootstrap(
    executor: &mut InitExecutor<'_, '_>,
    default_active_version_id: &str,
) -> Result<(), LixError> {
    executor
        .seed_boot_key_values(default_active_version_id)
        .await?;
    executor.seed_lix_id().await
}

const LIX_ID_KEY: &str = "lix_id";

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) async fn seed_boot_key_values(
        &mut self,
        default_active_version_id: &str,
    ) -> Result<(), LixError> {
        let mut bootstrap_commit_id: Option<String> = None;
        for key_value in self.boot_key_values().to_vec() {
            let version_id = if key_value.lixcol_global.unwrap_or(false) {
                super::KEY_VALUE_GLOBAL_VERSION.to_string()
            } else {
                default_active_version_id.to_string()
            };
            let untracked = key_value.lixcol_untracked.unwrap_or(true);
            let snapshot_content = serde_json::json!({
                "key": key_value.key,
                "value": key_value.value,
            })
            .to_string();

            if untracked {
                self.insert_bootstrap_untracked_row(
                    &key_value.key,
                    super::key_value_schema_key(),
                    super::key_value_schema_version(),
                    super::key_value_file_id(),
                    &version_id,
                    super::key_value_plugin_key(),
                    &snapshot_content,
                )
                .await?;
            } else {
                let commit_id = match &bootstrap_commit_id {
                    Some(commit_id) => commit_id.clone(),
                    None => {
                        let commit_id = self.load_global_version_commit_id().await?;
                        bootstrap_commit_id = Some(commit_id.clone());
                        commit_id
                    }
                };
                self.insert_bootstrap_tracked_row(
                    Some(&commit_id),
                    &key_value.key,
                    super::key_value_schema_key(),
                    super::key_value_schema_version(),
                    super::key_value_file_id(),
                    &version_id,
                    super::key_value_plugin_key(),
                    &snapshot_content,
                )
                .await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn seed_lix_id(&mut self) -> Result<(), LixError> {
        let table = tracked_relation_name(super::key_value_schema_key());
        let check_sql = format!(
            "SELECT 1 \
             FROM {table} \
             WHERE schema_key = '{schema_key}' \
               AND entity_id = '{entity_id}' \
               AND file_id = '{file_id}' \
               AND is_tombstone = 0 \
             LIMIT 1",
            table = quote_ident(&table),
            schema_key = escape_sql_string(super::key_value_schema_key()),
            entity_id = escape_sql_string(LIX_ID_KEY),
            file_id = escape_sql_string(super::key_value_file_id()),
        );
        let existing = self.execute_backend(&check_sql, &[]).await?;
        if !existing.rows.is_empty() {
            return Ok(());
        }

        let lix_id_value = self.generate_runtime_uuid().await?;
        let timestamp = self.generate_runtime_timestamp().await?;
        let version_id = super::KEY_VALUE_GLOBAL_VERSION;
        let snapshot_content = serde_json::json!({
            "key": LIX_ID_KEY,
            "value": lix_id_value,
        })
        .to_string();

        let change_id = self.generate_runtime_uuid().await?;
        let normalized_values =
            normalized_seed_values(super::key_value_schema_key(), Some(&snapshot_content))?;
        let insert_sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, change_id, metadata, writer_key, is_tombstone, created_at, updated_at{normalized_columns}\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', true, '{plugin_key}', '{change_id}', NULL, NULL, 0, '{timestamp}', '{timestamp}'{normalized_literals}\
             )",
            table = quote_ident(&table),
            entity_id = escape_sql_string(LIX_ID_KEY),
            schema_key = escape_sql_string(super::key_value_schema_key()),
            schema_version = escape_sql_string(super::key_value_schema_version()),
            file_id = escape_sql_string(super::key_value_file_id()),
            version_id = escape_sql_string(version_id),
            plugin_key = escape_sql_string(super::key_value_plugin_key()),
            change_id = escape_sql_string(&change_id),
            timestamp = escape_sql_string(&timestamp),
            normalized_columns = normalized_insert_columns_sql(&normalized_values),
            normalized_literals = normalized_insert_literals_sql(&normalized_values),
        );
        self.execute_backend(&insert_sql, &[]).await?;

        self.insert_change_row_for_snapshot(
            LIX_ID_KEY,
            super::key_value_schema_key(),
            super::key_value_schema_version(),
            super::key_value_file_id(),
            super::key_value_plugin_key(),
            &snapshot_content,
            &change_id,
            &timestamp,
        )
        .await?;
        Ok(())
    }
}
