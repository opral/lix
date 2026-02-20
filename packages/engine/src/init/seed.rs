use super::*;

impl Engine {
    pub(crate) async fn ensure_builtin_schemas_installed(&self) -> Result<(), LixError> {
        for schema_key in builtin_schema_keys() {
            let schema = builtin_schema_definition(schema_key).ok_or_else(|| LixError {
                message: format!("builtin schema '{schema_key}' is not available"),
            })?;
            let entity_id = builtin_schema_entity_id(schema)?;

            let existing = self
                .execute_internal(
                    "SELECT 1 FROM lix_internal_state_vtable \
                     WHERE schema_key = 'lix_stored_schema' \
                       AND entity_id = $1 \
                       AND version_id = 'global' \
                       AND snapshot_content IS NOT NULL \
                     LIMIT 1",
                    &[Value::Text(entity_id.clone())],
                    ExecuteOptions::default(),
                )
                .await?;
            if !existing.rows.is_empty() {
                continue;
            }

            let snapshot_content = serde_json::json!({
                "value": schema
            })
            .to_string();
            self.execute_internal(
                "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) \
                 VALUES ('lix_stored_schema', $1)",
                &[Value::Text(snapshot_content)],
                ExecuteOptions::default(),
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn seed_boot_key_values(&self) -> Result<(), LixError> {
        for key_value in &self.boot_key_values {
            let version_id = key_value
                .version_id
                .as_deref()
                .unwrap_or(KEY_VALUE_GLOBAL_VERSION);
            let snapshot_content = serde_json::json!({
                "key": key_value.key,
                "value": key_value.value,
            })
            .to_string();

            self.execute_internal(
                "INSERT INTO lix_internal_state_vtable (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, 1)",
                &[
                    Value::Text(key_value.key.clone()),
                    Value::Text(key_value_schema_key().to_string()),
                    Value::Text(key_value_file_id().to_string()),
                    Value::Text(version_id.to_string()),
                    Value::Text(key_value_plugin_key().to_string()),
                    Value::Text(snapshot_content),
                    Value::Text(key_value_schema_version().to_string()),
                ],
                ExecuteOptions::default(),
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn seed_default_checkpoint_label(&self) -> Result<(), LixError> {
        let existing = self
            .execute_internal(
                "SELECT snapshot_content \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_label' \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL",
                &[],
                ExecuteOptions::default(),
            )
            .await?;
        for row in &existing.rows {
            let Some(value) = row.first() else {
                continue;
            };
            let Value::Text(snapshot_content) = value else {
                continue;
            };
            let parsed: JsonValue =
                serde_json::from_str(snapshot_content.as_str()).map_err(|error| LixError {
                    message: format!("checkpoint label snapshot invalid JSON: {error}"),
                })?;
            if parsed.get("name").and_then(JsonValue::as_str) == Some("checkpoint") {
                return Ok(());
            }
        }

        let label_id = self.generate_runtime_uuid().await?;
        let snapshot_content = serde_json::json!({
            "id": label_id,
            "name": "checkpoint",
        })
        .to_string();
        self.execute_internal(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
             ) VALUES ($1, 'lix_label', 'lix', 'global', 'lix', $2, '1', 1)",
            &[Value::Text(label_id), Value::Text(snapshot_content)],
            ExecuteOptions::default(),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn seed_default_versions(&self) -> Result<String, LixError> {
        self.seed_materialized_version_descriptor(GLOBAL_VERSION_ID, GLOBAL_VERSION_ID, None)
            .await?;
        let main_version_id = match self
            .find_version_id_by_name(DEFAULT_ACTIVE_VERSION_NAME)
            .await?
        {
            Some(version_id) => version_id,
            None => {
                let generated_main_id = self.generate_runtime_uuid().await?;
                self.seed_materialized_version_descriptor(
                    &generated_main_id,
                    DEFAULT_ACTIVE_VERSION_NAME,
                    Some(GLOBAL_VERSION_ID),
                )
                .await?;
                generated_main_id
            }
        };

        let bootstrap_commit_id = self
            .load_latest_commit_id()
            .await?
            .unwrap_or_else(|| GLOBAL_VERSION_ID.to_string());
        if bootstrap_commit_id == GLOBAL_VERSION_ID {
            self.seed_bootstrap_change_set().await?;
            self.seed_bootstrap_commit().await?;
        }
        self.seed_materialized_version_pointer(GLOBAL_VERSION_ID, &bootstrap_commit_id)
            .await?;
        self.seed_materialized_version_pointer(&main_version_id, &bootstrap_commit_id)
            .await?;

        Ok(main_version_id)
    }

    pub(crate) async fn seed_boot_account(&self) -> Result<(), LixError> {
        let Some(account) = &self.boot_active_account else {
            return Ok(());
        };

        let exists = self
            .execute_internal(
                "SELECT 1 \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = $1 \
                   AND entity_id = $2 \
                   AND file_id = $3 \
                   AND version_id = $4 \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[
                    Value::Text(account_schema_key().to_string()),
                    Value::Text(account.id.clone()),
                    Value::Text(account_file_id().to_string()),
                    Value::Text(account_storage_version_id().to_string()),
                ],
                ExecuteOptions::default(),
            )
            .await?;
        if exists.rows.is_empty() {
            self.execute_internal(
                "INSERT INTO lix_internal_state_vtable (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    Value::Text(account.id.clone()),
                    Value::Text(account_schema_key().to_string()),
                    Value::Text(account_file_id().to_string()),
                    Value::Text(account_storage_version_id().to_string()),
                    Value::Text(account_plugin_key().to_string()),
                    Value::Text(account_snapshot_content(&account.id, &account.name)),
                    Value::Text(account_schema_version().to_string()),
                ],
                ExecuteOptions::default(),
            )
            .await?;
        }

        self.execute_internal(
            "DELETE FROM lix_internal_state_vtable \
             WHERE untracked = 1 \
               AND schema_key = $1 \
               AND file_id = $2 \
               AND version_id = $3",
            &[
                Value::Text(active_account_schema_key().to_string()),
                Value::Text(active_account_file_id().to_string()),
                Value::Text(active_account_storage_version_id().to_string()),
            ],
            ExecuteOptions::default(),
        )
        .await?;

        self.execute_internal(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, 1)",
            &[
                Value::Text(account.id.clone()),
                Value::Text(active_account_schema_key().to_string()),
                Value::Text(active_account_file_id().to_string()),
                Value::Text(active_account_storage_version_id().to_string()),
                Value::Text(active_account_plugin_key().to_string()),
                Value::Text(active_account_snapshot_content(&account.id)),
                Value::Text(active_account_schema_version().to_string()),
            ],
            ExecuteOptions::default(),
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn seed_materialized_version_descriptor(
        &self,
        entity_id: &str,
        name: &str,
        inherits_from_version_id: Option<&str>,
    ) -> Result<(), LixError> {
        let table = format!(
            "lix_internal_state_materialized_v1_{}",
            version_descriptor_schema_key()
        );
        let check_sql = format!(
            "SELECT 1 FROM {table} \
             WHERE schema_key = '{schema_key}' \
               AND entity_id = '{entity_id}' \
               AND file_id = '{file_id}' \
               AND version_id = '{version_id}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
             LIMIT 1",
            table = table,
            schema_key = escape_sql_string(version_descriptor_schema_key()),
            entity_id = escape_sql_string(entity_id),
            file_id = escape_sql_string(version_descriptor_file_id()),
            version_id = escape_sql_string(version_descriptor_storage_version_id()),
        );
        let existing = self.backend.execute(&check_sql, &[]).await?;
        if !existing.rows.is_empty() {
            return Ok(());
        }

        let snapshot_content =
            version_descriptor_snapshot_content(entity_id, name, inherits_from_version_id, false);
        let insert_sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content, \
             change_id, is_tombstone, created_at, updated_at\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', '{plugin_key}', '{snapshot_content}', \
             'bootstrap', 0, '1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000Z'\
             ) \
             ON CONFLICT (entity_id, file_id, version_id) DO NOTHING",
            table = table,
            entity_id = escape_sql_string(entity_id),
            schema_key = escape_sql_string(version_descriptor_schema_key()),
            schema_version = escape_sql_string(version_descriptor_schema_version()),
            file_id = escape_sql_string(version_descriptor_file_id()),
            version_id = escape_sql_string(version_descriptor_storage_version_id()),
            plugin_key = escape_sql_string(version_descriptor_plugin_key()),
            snapshot_content = escape_sql_string(&snapshot_content),
        );
        self.backend.execute(&insert_sql, &[]).await?;

        Ok(())
    }

    pub(crate) async fn find_version_id_by_name(
        &self,
        name: &str,
    ) -> Result<Option<String>, LixError> {
        let table = format!(
            "lix_internal_state_materialized_v1_{}",
            version_descriptor_schema_key()
        );
        let sql = format!(
            "SELECT entity_id, snapshot_content \
             FROM {table} \
             WHERE schema_key = '{schema_key}' \
               AND file_id = '{file_id}' \
               AND version_id = '{version_id}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL",
            table = table,
            schema_key = escape_sql_string(version_descriptor_schema_key()),
            file_id = escape_sql_string(version_descriptor_file_id()),
            version_id = escape_sql_string(version_descriptor_storage_version_id()),
        );
        let result = self.backend.execute(&sql, &[]).await?;

        for row in result.rows {
            if row.len() < 2 {
                continue;
            }
            let entity_id = match &row[0] {
                Value::Text(value) => value,
                _ => continue,
            };
            let snapshot_content = match &row[1] {
                Value::Text(value) => value,
                _ => continue,
            };
            let snapshot: LixVersionDescriptor =
                serde_json::from_str(snapshot_content).map_err(|error| LixError {
                    message: format!("version descriptor snapshot_content invalid JSON: {error}"),
                })?;
            let Some(snapshot_name) = snapshot.name.as_deref() else {
                continue;
            };
            if snapshot_name != name {
                continue;
            }
            let snapshot_id = if snapshot.id.is_empty() {
                entity_id.as_str()
            } else {
                snapshot.id.as_str()
            };
            return Ok(Some(snapshot_id.to_string()));
        }

        Ok(None)
    }

    pub(crate) async fn seed_materialized_version_pointer(
        &self,
        entity_id: &str,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let table = format!(
            "lix_internal_state_materialized_v1_{}",
            version_pointer_schema_key()
        );
        let check_sql = format!(
            "SELECT 1 FROM {table} \
             WHERE schema_key = '{schema_key}' \
               AND entity_id = '{entity_id}' \
               AND file_id = '{file_id}' \
               AND version_id = '{version_id}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
             LIMIT 1",
            table = table,
            schema_key = escape_sql_string(version_pointer_schema_key()),
            entity_id = escape_sql_string(entity_id),
            file_id = escape_sql_string(version_pointer_file_id()),
            version_id = escape_sql_string(version_pointer_storage_version_id()),
        );
        let existing = self.backend.execute(&check_sql, &[]).await?;
        if !existing.rows.is_empty() {
            return Ok(());
        }

        let snapshot_content = version_pointer_snapshot_content(entity_id, commit_id, commit_id);
        let insert_sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content, \
             change_id, is_tombstone, created_at, updated_at\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', '{plugin_key}', '{snapshot_content}', \
             'bootstrap', 0, '1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000Z'\
             ) \
             ON CONFLICT (entity_id, file_id, version_id) DO NOTHING",
            table = table,
            entity_id = escape_sql_string(entity_id),
            schema_key = escape_sql_string(version_pointer_schema_key()),
            schema_version = escape_sql_string(version_pointer_schema_version()),
            file_id = escape_sql_string(version_pointer_file_id()),
            version_id = escape_sql_string(version_pointer_storage_version_id()),
            plugin_key = escape_sql_string(version_pointer_plugin_key()),
            snapshot_content = escape_sql_string(&snapshot_content),
        );
        self.backend.execute(&insert_sql, &[]).await?;

        Ok(())
    }

    pub(crate) async fn seed_bootstrap_commit(&self) -> Result<(), LixError> {
        let existing = self
            .execute_internal(
                "SELECT 1 \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_commit' \
                   AND entity_id = $1 \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[Value::Text(GLOBAL_VERSION_ID.to_string())],
                ExecuteOptions::default(),
            )
            .await?;
        if !existing.rows.is_empty() {
            return Ok(());
        }

        let snapshot_content = serde_json::json!({
            "id": GLOBAL_VERSION_ID,
            "change_set_id": GLOBAL_VERSION_ID,
            "parent_commit_ids": [],
            "change_ids": [],
        })
        .to_string();
        self.execute_internal(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
             ) VALUES ($1, 'lix_commit', 'lix', 'global', 'lix', $2, '1', 1)",
            &[
                Value::Text(GLOBAL_VERSION_ID.to_string()),
                Value::Text(snapshot_content),
            ],
            ExecuteOptions::default(),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn seed_bootstrap_change_set(&self) -> Result<(), LixError> {
        let existing = self
            .execute_internal(
                "SELECT 1 \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_change_set' \
                   AND entity_id = $1 \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[Value::Text(GLOBAL_VERSION_ID.to_string())],
                ExecuteOptions::default(),
            )
            .await?;
        if !existing.rows.is_empty() {
            return Ok(());
        }

        let snapshot_content = serde_json::json!({
            "id": GLOBAL_VERSION_ID,
        })
        .to_string();
        self.execute_internal(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
             ) VALUES ($1, 'lix_change_set', 'lix', 'global', 'lix', $2, '1', 1)",
            &[
                Value::Text(GLOBAL_VERSION_ID.to_string()),
                Value::Text(snapshot_content),
            ],
            ExecuteOptions::default(),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn seed_default_active_version(
        &self,
        version_id: &str,
    ) -> Result<(), LixError> {
        let check_sql = format!(
            "SELECT 1 \
             FROM lix_internal_state_untracked \
             WHERE schema_key = '{schema_key}' \
               AND file_id = '{file_id}' \
               AND version_id = '{storage_version_id}' \
               AND snapshot_content IS NOT NULL \
             LIMIT 1",
            schema_key = escape_sql_string(active_version_schema_key()),
            file_id = escape_sql_string(active_version_file_id()),
            storage_version_id = escape_sql_string(active_version_storage_version_id()),
        );
        let existing = self.backend.execute(&check_sql, &[]).await?;
        if !existing.rows.is_empty() {
            return Ok(());
        }

        let entity_id = self.generate_runtime_uuid().await?;
        let snapshot_content = active_version_snapshot_content(&entity_id, version_id);
        let insert_sql = format!(
            "INSERT INTO lix_internal_state_untracked (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, created_at, updated_at\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{file_id}', '{storage_version_id}', '{plugin_key}', '{snapshot_content}', '{schema_version}', '1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000Z'\
             )",
            entity_id = escape_sql_string(&entity_id),
            schema_key = escape_sql_string(active_version_schema_key()),
            file_id = escape_sql_string(active_version_file_id()),
            storage_version_id = escape_sql_string(active_version_storage_version_id()),
            plugin_key = escape_sql_string(active_version_plugin_key()),
            snapshot_content = escape_sql_string(&snapshot_content),
            schema_version = escape_sql_string(active_version_schema_version()),
        );
        self.backend.execute(&insert_sql, &[]).await?;
        Ok(())
    }
}
