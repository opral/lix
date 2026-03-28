use crate::init::seed::{
    live_payload_column_name, normalized_insert_columns_sql, normalized_insert_literals_sql,
    normalized_seed_values, quote_ident,
};
use crate::init::InitExecutor;
use crate::sql_support::text::escape_sql_string;
use crate::Value;
use crate::{LixBackend, LixError};

pub(crate) async fn init(_backend: &dyn LixBackend) -> Result<(), LixError> {
    Ok(())
}

pub(crate) async fn seed_bootstrap(
    executor: &mut InitExecutor<'_, '_>,
) -> Result<String, LixError> {
    executor.seed_default_versions().await
}

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) async fn seed_default_versions(&mut self) -> Result<String, LixError> {
        let bootstrap_commit_id = match self.load_latest_commit_id().await? {
            Some(commit_id) => commit_id,
            None => {
                let bootstrap_change_set_id = self.generate_runtime_uuid().await?;
                let bootstrap_commit_id = self.generate_runtime_uuid().await?;
                self.seed_bootstrap_change_set(&bootstrap_change_set_id)
                    .await?;
                self.seed_bootstrap_commit(&bootstrap_commit_id, &bootstrap_change_set_id)
                    .await?;
                bootstrap_commit_id
            }
        };
        self.assert_commit_change_set_integrity(&bootstrap_commit_id)
            .await?;

        let main_version_id = match self
            .find_version_id_by_name(super::DEFAULT_ACTIVE_VERSION_NAME)
            .await?
        {
            Some(version_id) => version_id,
            None => {
                let generated_main_id = self.generate_runtime_uuid().await?;
                let desc_change_id = self
                    .seed_canonical_version_descriptor(
                        &bootstrap_commit_id,
                        &generated_main_id,
                        super::DEFAULT_ACTIVE_VERSION_NAME,
                    )
                    .await?;
                self.seed_materialized_version_descriptor(
                    &generated_main_id,
                    super::DEFAULT_ACTIVE_VERSION_NAME,
                    &desc_change_id,
                )
                .await?;
                generated_main_id
            }
        };

        let global_desc_change_id = self
            .seed_canonical_version_descriptor(
                &bootstrap_commit_id,
                super::GLOBAL_VERSION_ID,
                super::GLOBAL_VERSION_ID,
            )
            .await?;
        self.seed_materialized_version_descriptor(
            super::GLOBAL_VERSION_ID,
            super::GLOBAL_VERSION_ID,
            &global_desc_change_id,
        )
        .await?;
        self.seed_materialized_version_ref(super::GLOBAL_VERSION_ID, &bootstrap_commit_id)
            .await?;
        self.seed_materialized_version_ref(&main_version_id, &bootstrap_commit_id)
            .await?;

        Ok(main_version_id)
    }

    pub(crate) async fn seed_materialized_version_descriptor(
        &mut self,
        entity_id: &str,
        name: &str,
        change_id: &str,
    ) -> Result<(), LixError> {
        let table = crate::live_state::schema_access::tracked_relation_name(
            super::version_descriptor_schema_key(),
        );
        let check_sql = format!(
            "SELECT 1 \
             FROM {table} \
             WHERE schema_key = '{schema_key}' \
               AND entity_id = '{entity_id}' \
               AND file_id = '{file_id}' \
               AND version_id = '{version_id}' \
             LIMIT 1",
            table = table,
            schema_key = escape_sql_string(super::version_descriptor_schema_key()),
            entity_id = escape_sql_string(entity_id),
            file_id = escape_sql_string(super::version_descriptor_file_id()),
            version_id = escape_sql_string(super::version_descriptor_storage_version_id()),
        );
        let existing = self.execute_backend(&check_sql, &[]).await?;
        if !existing.rows.is_empty() {
            return Ok(());
        }

        let snapshot_content = super::version_descriptor_snapshot_content(
            entity_id,
            name,
            entity_id == super::GLOBAL_VERSION_ID,
        );
        let timestamp = self.generate_runtime_timestamp().await?;
        let normalized_values = normalized_seed_values(
            super::version_descriptor_schema_key(),
            Some(&snapshot_content),
        )?;
        let insert_sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, change_id, metadata, writer_key, is_tombstone, created_at, updated_at{normalized_columns}\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', true, '{plugin_key}', '{change_id}', NULL, NULL, 0, '{timestamp}', '{timestamp}'{normalized_literals}\
             )",
            table = quote_ident(&table),
            entity_id = escape_sql_string(entity_id),
            schema_key = escape_sql_string(super::version_descriptor_schema_key()),
            schema_version = escape_sql_string(super::version_descriptor_schema_version()),
            file_id = escape_sql_string(super::version_descriptor_file_id()),
            version_id = escape_sql_string(super::version_descriptor_storage_version_id()),
            plugin_key = escape_sql_string(super::version_descriptor_plugin_key()),
            change_id = escape_sql_string(change_id),
            timestamp = escape_sql_string(&timestamp),
            normalized_columns = normalized_insert_columns_sql(&normalized_values),
            normalized_literals = normalized_insert_literals_sql(&normalized_values),
        );
        self.execute_backend(&insert_sql, &[]).await?;
        Ok(())
    }

    pub(crate) async fn find_version_id_by_name(
        &mut self,
        name: &str,
    ) -> Result<Option<String>, LixError> {
        let table = crate::live_state::schema_access::tracked_relation_name(
            super::version_descriptor_schema_key(),
        );
        let name_column = quote_ident(&live_payload_column_name(
            super::version_descriptor_schema_key(),
            "name",
        ));
        let sql = format!(
            "SELECT entity_id, {name_column} \
             FROM {table} \
             WHERE schema_key = '{schema_key}' \
               AND file_id = '{file_id}' \
               AND version_id = '{version_id}' \
               AND is_tombstone = 0 \
               AND {name_column} IS NOT NULL",
            table = table,
            schema_key = escape_sql_string(super::version_descriptor_schema_key()),
            file_id = escape_sql_string(super::version_descriptor_file_id()),
            version_id = escape_sql_string(super::version_descriptor_storage_version_id()),
        );
        let result = self.execute_backend(&sql, &[]).await?;

        for row in result.rows {
            if row.len() < 2 {
                continue;
            }
            let entity_id = match &row[0] {
                Value::Text(value) => value,
                _ => continue,
            };
            let snapshot_name = match &row[1] {
                Value::Text(value) => value,
                _ => continue,
            };
            if snapshot_name == name {
                return Ok(Some(entity_id.to_string()));
            }
        }

        Ok(None)
    }

    pub(crate) async fn seed_materialized_version_ref(
        &mut self,
        entity_id: &str,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let snapshot_content = super::version_ref_snapshot_content(entity_id, commit_id);
        let table = crate::live_state::schema_access::tracked_relation_name(
            super::version_ref_schema_key(),
        );
        let check_sql = format!(
            "SELECT 1 \
             FROM {table} \
             WHERE schema_key = '{schema_key}' \
               AND entity_id = '{entity_id}' \
               AND file_id = '{file_id}' \
               AND version_id = '{version_id}' \
             LIMIT 1",
            table = table,
            schema_key = escape_sql_string(super::version_ref_schema_key()),
            entity_id = escape_sql_string(entity_id),
            file_id = escape_sql_string(super::version_ref_file_id()),
            version_id = escape_sql_string(super::version_ref_storage_version_id()),
        );
        let existing = self.execute_backend(&check_sql, &[]).await?;
        if existing.rows.is_empty() {
            let timestamp = self.generate_runtime_timestamp().await?;
            let normalized_values =
                normalized_seed_values(super::version_ref_schema_key(), Some(&snapshot_content))?;
            let insert_sql = format!(
                "INSERT INTO {table} (\
                 entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, metadata, writer_key, untracked, created_at, updated_at{normalized_columns}\
                 ) VALUES (\
                 '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', true, '{plugin_key}', NULL, NULL, true, '{timestamp}', '{timestamp}'{normalized_literals}\
                 )",
                table = quote_ident(&table),
                entity_id = escape_sql_string(entity_id),
                schema_key = escape_sql_string(super::version_ref_schema_key()),
                schema_version = escape_sql_string(super::version_ref_schema_version()),
                file_id = escape_sql_string(super::version_ref_file_id()),
                version_id = escape_sql_string(super::version_ref_storage_version_id()),
                plugin_key = escape_sql_string(super::version_ref_plugin_key()),
                timestamp = escape_sql_string(&timestamp),
                normalized_columns = normalized_insert_columns_sql(&normalized_values),
                normalized_literals = normalized_insert_literals_sql(&normalized_values),
            );
            self.execute_backend(&insert_sql, &[]).await?;
        }

        Ok(())
    }

    pub(crate) async fn assert_commit_change_set_integrity(
        &mut self,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let commit_row = self
            .execute_internal(
                "SELECT lix_json_extract(snapshot_content, 'change_set_id') \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_commit' \
                   AND entity_id = $1 \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 ORDER BY updated_at DESC, created_at DESC, change_id DESC \
                 LIMIT 1",
                &[Value::Text(commit_id.to_string())],
            )
            .await?;
        let [statement] = commit_row.statements.as_slice() else {
            return Err(crate::errors::unexpected_statement_count_error(
                "commit integrity commit query",
                1,
                commit_row.statements.len(),
            ));
        };
        let Some(row) = statement.rows.first() else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "init invariant violation: commit '{commit_id}' is missing from lix_commit"
                ),
            });
        };
        let Some(Value::Text(change_set_id)) = row.first() else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "init invariant violation: commit '{commit_id}' has non-text change_set_id"
                ),
            });
        };
        if change_set_id.is_empty() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "init invariant violation: commit '{commit_id}' has empty change_set_id"
                ),
            });
        }

        let existing = self
            .execute_internal(
                "SELECT 1 \
                 FROM lix_state_by_version \
                   WHERE schema_key = 'lix_change_set' \
                   AND entity_id = $1 \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[Value::Text(change_set_id.clone())],
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(crate::errors::unexpected_statement_count_error(
                "commit integrity change_set query",
                1,
                existing.statements.len(),
            ));
        };
        if statement.rows.is_empty() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "init invariant violation: commit '{commit_id}' references missing change_set '{change_set_id}'"
                ),
            });
        }

        Ok(())
    }
}
