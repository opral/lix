use super::*;
use crate::errors;
use crate::version::DEFAULT_ACTIVE_VERSION_NAME;

const SYSTEM_ROOT_DIRECTORY_PATH: &str = "/.lix/";
const SYSTEM_APP_DATA_DIRECTORY_PATH: &str = "/.lix/app_data/";
const SYSTEM_PLUGIN_DIRECTORY_PATH: &str = "/.lix/plugins/";
const BOOTSTRAP_CHANGE_SET_ID: &str = "00000000-0000-7000-8000-000000000001";
const BOOTSTRAP_COMMIT_ID: &str = "00000000-0000-7000-8000-000000000002";

impl Engine {
    pub(crate) async fn ensure_builtin_schemas_installed(&self) -> Result<(), LixError> {
        for schema_key in builtin_schema_keys() {
            let schema = builtin_schema_definition(schema_key).ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("builtin schema '{schema_key}' is not available"),
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
            let [statement] = existing.statements.as_slice() else {
                return Err(errors::unexpected_statement_count_error(
                    "builtin schema existence query",
                    1,
                    existing.statements.len(),
                ));
            };
            if !statement.rows.is_empty() {
                continue;
            }

            let snapshot_content = serde_json::json!({
                "value": schema
            })
            .to_string();
            self.execute_internal(
                "INSERT INTO lix_internal_state_vtable (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, created_at, updated_at, untracked\
                 ) VALUES ($1, 'lix_stored_schema', 'lix', 'global', 'lix', $2, '1', '1970-01-01T00:00:00Z', '1970-01-01T00:00:00Z', true)",
                &[
                    Value::Text(entity_id),
                    Value::Text(snapshot_content),
                ],
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
            let untracked = key_value.untracked.unwrap_or(true);
            let snapshot_content = serde_json::json!({
                "key": key_value.key,
                "value": key_value.value,
            })
            .to_string();

            self.execute_internal(
                "INSERT INTO lix_internal_state_vtable (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[
                    Value::Text(key_value.key.clone()),
                    Value::Text(key_value_schema_key().to_string()),
                    Value::Text(key_value_file_id().to_string()),
                    Value::Text(version_id.to_string()),
                    Value::Text(key_value_plugin_key().to_string()),
                    Value::Text(snapshot_content),
                    Value::Text(key_value_schema_version().to_string()),
                    Value::Boolean(untracked),
                ],
                ExecuteOptions::default(),
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn seed_global_system_directories(&self) -> Result<(), LixError> {
        let directories = [
            SYSTEM_ROOT_DIRECTORY_PATH,
            SYSTEM_APP_DATA_DIRECTORY_PATH,
            SYSTEM_PLUGIN_DIRECTORY_PATH,
        ];

        for path in directories {
            let existing = self
                .execute_internal(
                    "SELECT 1 \
                     FROM lix_directory_by_version \
                     WHERE path = $1 \
                       AND lixcol_version_id = 'global' \
                     LIMIT 1",
                    &[Value::Text(path.to_string())],
                    ExecuteOptions::default(),
                )
                .await?;
            let [statement] = existing.statements.as_slice() else {
                return Err(errors::unexpected_statement_count_error(
                    "global system directory existence query",
                    1,
                    existing.statements.len(),
                ));
            };
            if !statement.rows.is_empty() {
                continue;
            }

            self.execute_internal(
                "INSERT INTO lix_directory_by_version (\
                 path, hidden, lixcol_version_id, lixcol_untracked\
                 ) VALUES ($1, true, 'global', true)",
                &[Value::Text(path.to_string())],
                ExecuteOptions::default(),
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn seed_default_checkpoint_label(&self) -> Result<(), LixError> {
        let bootstrap_commit_id = self.load_global_version_commit_id().await?;
        let existing = self
            .execute_internal(
                "SELECT entity_id, snapshot_content \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_label' \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL",
                &[],
                ExecuteOptions::default(),
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(errors::unexpected_statement_count_error(
                "default checkpoint label query",
                1,
                existing.statements.len(),
            ));
        };
        for row in &statement.rows {
            let Some(Value::Text(row_entity_id)) = row.first() else {
                continue;
            };
            let Some(Value::Text(snapshot_content)) = row.get(1) else {
                continue;
            };
            let parsed: JsonValue =
                serde_json::from_str(snapshot_content.as_str()).map_err(|error| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!("checkpoint label snapshot invalid JSON: {error}"),
                })?;
            if parsed.get("name").and_then(JsonValue::as_str) == Some("checkpoint") {
                let label_id = parsed
                    .get("id")
                    .and_then(JsonValue::as_str)
                    .unwrap_or(row_entity_id.as_str())
                    .to_string();
                self.ensure_checkpoint_label_on_bootstrap_commit(&bootstrap_commit_id, &label_id)
                    .await?;
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
             ) VALUES ($1, 'lix_label', 'lix', 'global', 'lix', $2, '1', true)",
            &[Value::Text(label_id.clone()), Value::Text(snapshot_content)],
            ExecuteOptions::default(),
        )
        .await?;
        self.ensure_checkpoint_label_on_bootstrap_commit(&bootstrap_commit_id, &label_id)
            .await?;
        Ok(())
    }

    async fn load_global_version_commit_id(&self) -> Result<String, LixError> {
        let rows = self
            .execute_internal(
                "SELECT lix_json_extract(snapshot_content, 'commit_id') AS commit_id \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_version_pointer' \
                   AND entity_id = 'global' \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 ORDER BY updated_at DESC, created_at DESC, change_id DESC \
                 LIMIT 1",
                &[],
                ExecuteOptions::default(),
            )
            .await?;
        let [statement] = rows.statements.as_slice() else {
            return Err(errors::unexpected_statement_count_error(
                "hidden global version commit_id query",
                1,
                rows.statements.len(),
            ));
        };
        let Some(first) = statement.rows.first() else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "init invariant violation: hidden global version pointer is missing"
                    .to_string(),
            });
        };
        text_value(first.first(), "lix_version_pointer.commit_id")
    }

    async fn ensure_checkpoint_label_on_bootstrap_commit(
        &self,
        bootstrap_commit_id: &str,
        label_id: &str,
    ) -> Result<(), LixError> {
        let entity_label_id = format!("{bootstrap_commit_id}~lix_commit~lix~{label_id}");
        let existing = self
            .execute_internal(
                "SELECT 1 \
                 FROM lix_internal_state_vtable \
                 WHERE entity_id = $1 \
                   AND schema_key = 'lix_entity_label' \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[Value::Text(entity_label_id.clone())],
                ExecuteOptions::default(),
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(errors::unexpected_statement_count_error(
                "checkpoint label bootstrap link existence query",
                1,
                existing.statements.len(),
            ));
        };
        if !statement.rows.is_empty() {
            return Ok(());
        }

        self.execute_internal(
            "DELETE FROM lix_internal_state_vtable \
             WHERE entity_id = $1 \
               AND schema_key = 'lix_entity_label' \
               AND file_id = 'lix' \
               AND version_id = 'global'",
            &[Value::Text(entity_label_id.clone())],
            ExecuteOptions::default(),
        )
        .await?;

        let snapshot_content = serde_json::json!({
            "entity_id": bootstrap_commit_id,
            "schema_key": "lix_commit",
            "file_id": "lix",
            "label_id": label_id,
        })
        .to_string();
        self.execute_internal(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
             ) VALUES ($1, 'lix_entity_label', 'lix', 'global', 'lix', $2, '1', true)",
            &[Value::Text(entity_label_id), Value::Text(snapshot_content)],
            ExecuteOptions::default(),
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn seed_default_versions(&self) -> Result<String, LixError> {
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
                )
                .await?;
                generated_main_id
            }
        };

        let bootstrap_commit_id = self
            .load_latest_commit_id()
            .await?
            .unwrap_or_else(|| BOOTSTRAP_COMMIT_ID.to_string());
        if bootstrap_commit_id == BOOTSTRAP_COMMIT_ID {
            self.seed_bootstrap_change_set(BOOTSTRAP_CHANGE_SET_ID)
                .await?;
            self.seed_bootstrap_commit(BOOTSTRAP_COMMIT_ID, BOOTSTRAP_CHANGE_SET_ID)
                .await?;
        }
        self.assert_commit_change_set_integrity(&bootstrap_commit_id)
            .await?;
        self.seed_materialized_version_descriptor(GLOBAL_VERSION_ID, GLOBAL_VERSION_ID)
            .await?;
        self.seed_materialized_version_pointer(GLOBAL_VERSION_ID, &bootstrap_commit_id)
            .await?;
        self.seed_materialized_version_pointer(&main_version_id, &bootstrap_commit_id)
            .await?;

        Ok(main_version_id)
    }

    pub(crate) async fn seed_commit_ancestry(&self) -> Result<(), LixError> {
        let ancestry_count_result = self
            .backend
            .execute("SELECT COUNT(*) FROM lix_internal_commit_ancestry", &[])
            .await?;
        let ancestry_count =
            read_scalar_count(&ancestry_count_result, "lix_internal_commit_ancestry count")?;
        if ancestry_count > 0 {
            return Ok(());
        }

        let commit_count_result = self
            .backend
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_internal_state_materialized_v1_lix_commit \
                 WHERE schema_key = 'lix_commit' \
                   AND version_id = 'global' \
                   AND is_tombstone = 0 \
                   AND snapshot_content IS NOT NULL",
                &[],
            )
            .await?;
        let commit_count = read_scalar_count(&commit_count_result, "lix_commit count")?;
        if commit_count == 0 {
            return Ok(());
        }

        self.backend
            .execute(
                "WITH RECURSIVE \
                   commits AS ( \
                     SELECT entity_id AS commit_id \
                     FROM lix_internal_state_materialized_v1_lix_commit \
                     WHERE schema_key = 'lix_commit' \
                       AND version_id = 'global' \
                       AND is_tombstone = 0 \
                       AND snapshot_content IS NOT NULL \
                   ), \
                   edges AS ( \
                     SELECT \
                       lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
                       lix_json_extract(snapshot_content, 'child_id') AS child_id \
                     FROM lix_internal_state_materialized_v1_lix_commit_edge \
                     WHERE schema_key = 'lix_commit_edge' \
                       AND version_id = 'global' \
                       AND is_tombstone = 0 \
                       AND snapshot_content IS NOT NULL \
                       AND lix_json_extract(snapshot_content, 'parent_id') IS NOT NULL \
                       AND lix_json_extract(snapshot_content, 'child_id') IS NOT NULL \
                   ), \
                   walk(commit_id, ancestor_id, depth) AS ( \
                     SELECT c.commit_id, c.commit_id AS ancestor_id, 0 AS depth \
                     FROM commits c \
                     UNION ALL \
                     SELECT w.commit_id, e.parent_id AS ancestor_id, w.depth + 1 AS depth \
                     FROM walk w \
                     JOIN edges e ON e.child_id = w.ancestor_id \
                     WHERE w.depth < 512 \
                   ) \
                 INSERT INTO lix_internal_commit_ancestry (commit_id, ancestor_id, depth) \
                 SELECT commit_id, ancestor_id, MIN(depth) AS depth \
                 FROM walk \
                 GROUP BY commit_id, ancestor_id",
                &[],
            )
            .await?;

        Ok(())
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
        let [statement] = exists.statements.as_slice() else {
            return Err(errors::unexpected_statement_count_error(
                "seed_boot_account existence query",
                1,
                exists.statements.len(),
            ));
        };
        if statement.rows.is_empty() {
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
             WHERE untracked = true \
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
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, true)",
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
    ) -> Result<(), LixError> {
        let table = format!(
            "lix_internal_state_materialized_v1_{}",
            version_descriptor_schema_key()
        );
        let check_sql = format!(
            "SELECT 1 \
             FROM {table} \
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

        let snapshot_content = version_descriptor_snapshot_content(
            entity_id,
            name,
            entity_id == GLOBAL_VERSION_ID,
        );
        let change_id = format!("seed~{}~{}", version_descriptor_schema_key(), entity_id);
        let insert_sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, snapshot_content, change_id, metadata, writer_key, is_tombstone, created_at, updated_at\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', true, '{plugin_key}', '{snapshot_content}', '{change_id}', NULL, NULL, 0, '1970-01-01T00:00:00Z', '1970-01-01T00:00:00Z'\
             )",
            table = table,
            entity_id = escape_sql_string(entity_id),
            schema_key = escape_sql_string(version_descriptor_schema_key()),
            schema_version = escape_sql_string(version_descriptor_schema_version()),
            file_id = escape_sql_string(version_descriptor_file_id()),
            version_id = escape_sql_string(version_descriptor_storage_version_id()),
            plugin_key = escape_sql_string(version_descriptor_plugin_key()),
            snapshot_content = escape_sql_string(&snapshot_content),
            change_id = escape_sql_string(&change_id),
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
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "version descriptor snapshot_content invalid JSON: {error}"
                    ),
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
        let snapshot_content = version_pointer_snapshot_content(entity_id, commit_id);
        let change_id = format!("seed~{}~{}", version_pointer_schema_key(), entity_id);
        let table = format!(
            "lix_internal_state_materialized_v1_{}",
            version_pointer_schema_key()
        );
        let check_sql = format!(
            "SELECT 1 \
             FROM {table} \
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
        if existing.rows.is_empty() {
            let insert_sql = format!(
                "INSERT INTO {table} (\
                 entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, snapshot_content, change_id, metadata, writer_key, is_tombstone, created_at, updated_at\
                 ) VALUES (\
                 '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', true, '{plugin_key}', '{snapshot_content}', '{change_id}', NULL, NULL, 0, '1970-01-01T00:00:00Z', '1970-01-01T00:00:00Z'\
                 )",
                table = table,
                entity_id = escape_sql_string(entity_id),
                schema_key = escape_sql_string(version_pointer_schema_key()),
                schema_version = escape_sql_string(version_pointer_schema_version()),
                file_id = escape_sql_string(version_pointer_file_id()),
                version_id = escape_sql_string(version_pointer_storage_version_id()),
                plugin_key = escape_sql_string(version_pointer_plugin_key()),
                snapshot_content = escape_sql_string(&snapshot_content),
                change_id = escape_sql_string(&change_id),
            );
            self.backend.execute(&insert_sql, &[]).await?;
        }

        self.seed_committed_pointer_change(
            entity_id,
            version_pointer_schema_key(),
            version_pointer_schema_version(),
            version_pointer_file_id(),
            version_pointer_plugin_key(),
            &snapshot_content,
            &change_id,
        )
        .await
    }

    async fn seed_committed_pointer_change(
        &self,
        entity_id: &str,
        schema_key: &str,
        schema_version: &str,
        file_id: &str,
        plugin_key: &str,
        snapshot_content: &str,
        change_id: &str,
    ) -> Result<(), LixError> {
        let snapshot_id = format!("{change_id}~snapshot");
        self.backend
            .execute(
                "INSERT INTO lix_internal_snapshot (id, content) \
                 SELECT $1, $2 \
                 WHERE NOT EXISTS (SELECT 1 FROM lix_internal_snapshot WHERE id = $1)",
                &[
                    Value::Text(snapshot_id.clone()),
                    Value::Text(snapshot_content.to_string()),
                ],
            )
            .await?;
        self.backend
            .execute(
                "INSERT INTO lix_internal_change (\
                 id, entity_id, schema_key, schema_version, file_id, plugin_key, snapshot_id, metadata, created_at\
                 ) \
                 SELECT $1, $2, $3, $4, $5, $6, $7, NULL, '1970-01-01T00:00:00Z' \
                 WHERE NOT EXISTS (SELECT 1 FROM lix_internal_change WHERE id = $1)",
                &[
                    Value::Text(change_id.to_string()),
                    Value::Text(entity_id.to_string()),
                    Value::Text(schema_key.to_string()),
                    Value::Text(schema_version.to_string()),
                    Value::Text(file_id.to_string()),
                    Value::Text(plugin_key.to_string()),
                    Value::Text(snapshot_id),
                ],
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn insert_last_checkpoint_for_version(
        &self,
        version_id: &str,
        checkpoint_commit_id: &str,
    ) -> Result<(), LixError> {
        self.backend
            .execute(
                "INSERT INTO lix_internal_last_checkpoint (version_id, checkpoint_commit_id) \
                 VALUES ($1, $2)",
                &[
                    Value::Text(version_id.to_string()),
                    Value::Text(checkpoint_commit_id.to_string()),
                ],
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn rebuild_internal_last_checkpoint(&self) -> Result<(), LixError> {
        let versions = self
            .execute_internal(
                "SELECT id, commit_id \
                 FROM lix_version \
                 ORDER BY id",
                &[],
                ExecuteOptions::default(),
            )
            .await?;
        let [statement] = versions.statements.as_slice() else {
            return Err(errors::unexpected_statement_count_error(
                "rebuild_internal_last_checkpoint query",
                1,
                versions.statements.len(),
            ));
        };

        self.backend
            .execute("DELETE FROM lix_internal_last_checkpoint", &[])
            .await?;

        let global_commit_id = self.load_global_version_commit_id().await?;
        let global_checkpoint_commit_id = self
            .resolve_last_checkpoint_commit_id_for_tip(&global_commit_id)
            .await?
            .unwrap_or_else(|| global_commit_id.clone());
        self.insert_last_checkpoint_for_version(GLOBAL_VERSION_ID, &global_checkpoint_commit_id)
            .await?;

        for row in &statement.rows {
            let version_id = text_value(row.get(0), "lix_version.id")?;
            if version_id == GLOBAL_VERSION_ID {
                continue;
            }
            let commit_id = text_value(row.get(1), "lix_version.commit_id")?;
            let checkpoint_commit_id = self
                .resolve_last_checkpoint_commit_id_for_tip(&commit_id)
                .await?
                .unwrap_or_else(|| commit_id.clone());
            self.insert_last_checkpoint_for_version(&version_id, &checkpoint_commit_id)
                .await?;
        }

        Ok(())
    }

    async fn resolve_last_checkpoint_commit_id_for_tip(
        &self,
        tip_commit_id: &str,
    ) -> Result<Option<String>, LixError> {
        let rows = self
            .execute_internal(
                "SELECT anc.ancestor_id \
                 FROM lix_internal_commit_ancestry anc \
                 JOIN ( \
                   SELECT \
                     lix_json_extract(snapshot_content, 'entity_id') AS entity_id, \
                     lix_json_extract(snapshot_content, 'schema_key') AS schema_key, \
                     lix_json_extract(snapshot_content, 'label_id') AS label_id \
                   FROM lix_internal_state_vtable \
                   WHERE schema_key = 'lix_entity_label' \
                     AND file_id = 'lix' \
                     AND version_id = 'global' \
                     AND snapshot_content IS NOT NULL \
                 ) el \
                   ON el.entity_id = anc.ancestor_id \
                  AND el.schema_key = 'lix_commit' \
                 JOIN ( \
                   SELECT \
                     entity_id AS id, \
                     lix_json_extract(snapshot_content, 'name') AS name \
                   FROM lix_internal_state_vtable \
                   WHERE schema_key = 'lix_label' \
                     AND file_id = 'lix' \
                     AND version_id = 'global' \
                     AND snapshot_content IS NOT NULL \
                 ) l \
                   ON l.id = el.label_id \
                  AND l.name = 'checkpoint' \
                 LEFT JOIN ( \
                   SELECT entity_id AS id, created_at \
                   FROM lix_internal_state_vtable \
                   WHERE schema_key = 'lix_commit' \
                     AND file_id = 'lix' \
                     AND version_id = 'global' \
                     AND snapshot_content IS NOT NULL \
                 ) c ON c.id = anc.ancestor_id \
                 WHERE anc.commit_id = $1 \
                 ORDER BY \
                   anc.depth ASC, \
                   c.created_at DESC, \
                   anc.ancestor_id DESC \
                 LIMIT 1",
                &[Value::Text(tip_commit_id.to_string())],
                ExecuteOptions::default(),
            )
            .await?;
        let [statement] = rows.statements.as_slice() else {
            return Err(errors::unexpected_statement_count_error(
                "resolve checkpoint ancestor query",
                1,
                rows.statements.len(),
            ));
        };
        let Some(first) = statement.rows.first() else {
            return Ok(None);
        };
        Ok(Some(text_value(first.get(0), "checkpoint ancestor id")?))
    }

    pub(crate) async fn seed_bootstrap_commit(
        &self,
        commit_id: &str,
        change_set_id: &str,
    ) -> Result<(), LixError> {
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
                &[Value::Text(commit_id.to_string())],
                ExecuteOptions::default(),
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(errors::unexpected_statement_count_error(
                "seed_bootstrap_commit existence query",
                1,
                existing.statements.len(),
            ));
        };
        if !statement.rows.is_empty() {
            return Ok(());
        }

        let snapshot_content = serde_json::json!({
            "id": commit_id,
            "change_set_id": change_set_id,
            "parent_commit_ids": [],
            "change_ids": [],
        })
        .to_string();
        self.execute_internal(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
             ) VALUES ($1, 'lix_commit', 'lix', 'global', 'lix', $2, '1', true)",
            &[
                Value::Text(commit_id.to_string()),
                Value::Text(snapshot_content),
            ],
            ExecuteOptions::default(),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn seed_bootstrap_change_set(
        &self,
        change_set_id: &str,
    ) -> Result<(), LixError> {
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
                &[Value::Text(change_set_id.to_string())],
                ExecuteOptions::default(),
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(errors::unexpected_statement_count_error(
                "seed_bootstrap_change_set existence query",
                1,
                existing.statements.len(),
            ));
        };
        if !statement.rows.is_empty() {
            return Ok(());
        }

        let snapshot_content = serde_json::json!({
            "id": change_set_id,
        })
        .to_string();
        self.execute_internal(
            "INSERT INTO lix_internal_state_vtable (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
             ) VALUES ($1, 'lix_change_set', 'lix', 'global', 'lix', $2, '1', true)",
            &[
                Value::Text(change_set_id.to_string()),
                Value::Text(snapshot_content),
            ],
            ExecuteOptions::default(),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn assert_commit_change_set_integrity(
        &self,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let commit_row = self
            .execute_internal(
                "SELECT lix_json_extract(snapshot_content, 'change_set_id') \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_commit' \
                   AND entity_id = $1 \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 ORDER BY updated_at DESC, created_at DESC, change_id DESC \
                 LIMIT 1",
                &[Value::Text(commit_id.to_string())],
                ExecuteOptions::default(),
            )
            .await?;
        let [statement] = commit_row.statements.as_slice() else {
            return Err(errors::unexpected_statement_count_error(
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
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_change_set' \
                   AND entity_id = $1 \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[Value::Text(change_set_id.clone())],
                ExecuteOptions::default(),
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(errors::unexpected_statement_count_error(
                "commit integrity change_set query",
                1,
                existing.statements.len(),
            ));
        };
        if statement.rows.is_empty() {
            return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description: format!(
                    "init invariant violation: commit '{commit_id}' references missing change_set '{change_set_id}'"
                ),
            });
        }

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
             entity_id, schema_key, file_id, version_id, global, plugin_key, snapshot_content, schema_version, created_at, updated_at\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{file_id}', '{storage_version_id}', true, '{plugin_key}', '{snapshot_content}', '{schema_version}', '1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000Z'\
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

fn read_scalar_count(result: &crate::QueryResult, label: &str) -> Result<i64, LixError> {
    let value = result
        .rows
        .first()
        .and_then(|row| row.first())
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{label} query returned no rows"),
        })?;
    match value {
        Value::Integer(number) => Ok(*number),
        Value::Text(raw) => raw.parse::<i64>().map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{label} query returned invalid integer '{raw}': {error}"),
        }),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{label} query returned non-integer value: {other:?}"),
        }),
    }
}

fn text_value(value: Option<&Value>, label: &str) -> Result<String, LixError> {
    match value {
        Some(Value::Text(text)) if !text.is_empty() => Ok(text.clone()),
        Some(Value::Text(_)) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{label} must not be empty"),
        }),
        Some(Value::Integer(number)) => Ok(number.to_string()),
        Some(other) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{label} must be text-like, got {other:?}"),
        }),
        None => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("missing {label}"),
        }),
    }
}
