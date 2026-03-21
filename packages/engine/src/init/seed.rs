use crate::account::{
    account_file_id, account_plugin_key, account_schema_key, account_schema_version,
    account_snapshot_content, account_storage_version_id, active_account_file_id,
    active_account_plugin_key, active_account_schema_key, active_account_schema_version,
    active_account_snapshot_content, active_account_storage_version_id,
};
use crate::engine::{builtin_schema_entity_id, Engine, ExecuteOptions};
use crate::errors;
use crate::key_value::{
    key_value_file_id, key_value_plugin_key, key_value_schema_key, key_value_schema_version,
    KEY_VALUE_GLOBAL_VERSION,
};
use crate::schema::builtin::{builtin_schema_definition, builtin_schema_keys};
use crate::schema::live_layout::{
    builtin_live_table_layout, live_column_name_for_property, normalized_live_column_values,
    tracked_live_table_name, untracked_live_table_name,
};
use crate::schema::registry::ensure_schema_live_table;
use crate::sql::storage::sql_text::escape_sql_string;
use crate::state::checkpoint::{
    checkpoint_commit_label_entity_id, checkpoint_commit_label_snapshot, CHECKPOINT_LABEL_ID,
    CHECKPOINT_LABEL_NAME,
};
use crate::state::commit::{build_commit_generation_seed_sql, COMMIT_GRAPH_NODE_TABLE};
use crate::version::DEFAULT_ACTIVE_VERSION_NAME;
use crate::version::{
    active_version_file_id, active_version_plugin_key, active_version_schema_key,
    active_version_schema_version, active_version_snapshot_content,
    active_version_storage_version_id, version_descriptor_file_id, version_descriptor_plugin_key,
    version_descriptor_schema_key, version_descriptor_schema_version,
    version_descriptor_snapshot_content, version_descriptor_storage_version_id,
    version_ref_file_id, version_ref_plugin_key, version_ref_schema_key,
    version_ref_schema_version, version_ref_snapshot_content, version_ref_storage_version_id,
    GLOBAL_VERSION_ID,
};
use crate::{LixError, Value};
use serde_json::Value as JsonValue;

const SYSTEM_ROOT_DIRECTORY_PATH: &str = "/.lix/";
const SYSTEM_APP_DATA_DIRECTORY_PATH: &str = "/.lix/app_data/";
const SYSTEM_PLUGIN_DIRECTORY_PATH: &str = "/.lix/plugins/";
const LIX_ID_KEY: &str = "lix_id";

impl Engine {
    pub(crate) async fn ensure_builtin_schemas_installed(&self) -> Result<(), LixError> {
        for schema_key in builtin_schema_keys() {
            ensure_schema_live_table(self.backend.as_ref(), schema_key).await?;
        }

        for schema_key in builtin_schema_keys() {
            let schema = builtin_schema_definition(schema_key).ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("builtin schema '{schema_key}' is not available"),
            })?;
            let entity_id = builtin_schema_entity_id(schema)?;

            let existing = self
                .execute_internal(
                    "SELECT 1 FROM lix_internal_state_vtable \
                     WHERE schema_key = 'lix_registered_schema' \
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
                 ) VALUES ($1, 'lix_registered_schema', 'lix', 'global', 'lix', $2, '1', '1970-01-01T00:00:00Z', '1970-01-01T00:00:00Z', true)",
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
        for key_value in self.boot_key_values() {
            let version_id = if key_value.lixcol_global.unwrap_or(false) {
                KEY_VALUE_GLOBAL_VERSION.to_string()
            } else {
                self.load_boot_active_version_id().await?
            };
            let untracked = key_value.lixcol_untracked.unwrap_or(true);
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
                    Value::Text(version_id),
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

    async fn load_boot_active_version_id(&self) -> Result<String, LixError> {
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
                    payload_version_column = quote_ident(payload_version_column),
                    table_name =
                        quote_ident(&untracked_live_table_name(active_version_schema_key())),
                ),
                &[
                    Value::Text(active_version_file_id().to_string()),
                    Value::Text(active_version_storage_version_id().to_string()),
                ],
            )
            .await?;

        let row = result.rows.first().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "boot key-value seeding requires an active version".to_string(),
        })?;
        let version_id = row.first().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "active version query row is missing version_id".to_string(),
        })?;
        match version_id {
            Value::Text(value) => Ok(value.clone()),
            other => Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("active version id must be text, got {other:?}"),
            }),
        }
    }

    pub(crate) async fn seed_global_system_directories(&self) -> Result<(), LixError> {
        ensure_schema_live_table(self.backend.as_ref(), "lix_directory_descriptor").await?;
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
        &self,
        bootstrap_commit_id: &str,
        path: &str,
        parent_id: Option<&str>,
    ) -> Result<String, LixError> {
        let table_name = quote_ident(&tracked_live_table_name("lix_directory_descriptor"));
        let name = system_directory_name(path);
        let existing = match parent_id {
            Some(parent_id) => {
                self.backend
                    .execute(
                        &format!(
                            "SELECT entity_id \
                         FROM {table_name} \
                         WHERE file_id = 'lix' \
                           AND version_id = 'global' \
                           AND name = $1 \
                           AND parent_id = $2 \
                         ORDER BY updated_at DESC, created_at DESC \
                         LIMIT 1",
                        ),
                        &[
                            Value::Text(name.clone()),
                            Value::Text(parent_id.to_string()),
                        ],
                    )
                    .await?
            }
            None => {
                self.backend
                    .execute(
                        &format!(
                            "SELECT entity_id \
                         FROM {table_name} \
                         WHERE file_id = 'lix' \
                           AND version_id = 'global' \
                           AND name = $1 \
                           AND parent_id IS NULL \
                         ORDER BY updated_at DESC, created_at DESC \
                         LIMIT 1"
                        ),
                        &[Value::Text(name.clone())],
                    )
                    .await?
            }
        };
        if let Some(row) = existing.rows.first() {
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

    pub(crate) async fn seed_default_checkpoint_label(&self) -> Result<(), LixError> {
        let bootstrap_commit_id = self.load_global_version_commit_id().await?;
        let existing = self
            .execute_internal(
                "SELECT snapshot_content \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_label' \
                   AND entity_id = $1 \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 ORDER BY updated_at DESC, created_at DESC, change_id DESC \
                 LIMIT 1",
                &[Value::Text(CHECKPOINT_LABEL_ID.to_string())],
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
        if let Some(row) = statement.rows.first() {
            let Some(Value::Text(snapshot_content)) = row.first() else {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "checkpoint label snapshot_content must be text",
                ));
            };
            let parsed: JsonValue =
                serde_json::from_str(snapshot_content.as_str()).map_err(|error| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!("checkpoint label snapshot invalid JSON: {error}"),
                })?;
            let id = parsed.get("id").and_then(JsonValue::as_str);
            let name = parsed.get("name").and_then(JsonValue::as_str);
            if id != Some(CHECKPOINT_LABEL_ID) || name != Some(CHECKPOINT_LABEL_NAME) {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "checkpoint label canonical row is present but invalid",
                ));
            }
            self.ensure_checkpoint_label_on_bootstrap_commit(
                &bootstrap_commit_id,
                CHECKPOINT_LABEL_ID,
            )
            .await?;
            return Ok(());
        }

        let snapshot_content = serde_json::json!({
            "id": CHECKPOINT_LABEL_ID,
            "name": CHECKPOINT_LABEL_NAME,
        })
        .to_string();
        self.insert_bootstrap_tracked_row(
            Some(&bootstrap_commit_id),
            CHECKPOINT_LABEL_ID,
            "lix_label",
            "1",
            "lix",
            "global",
            "lix",
            &snapshot_content,
        )
        .await?;

        self.ensure_checkpoint_label_on_bootstrap_commit(&bootstrap_commit_id, CHECKPOINT_LABEL_ID)
            .await?;
        Ok(())
    }

    async fn load_global_version_commit_id(&self) -> Result<String, LixError> {
        let rows = self
            .execute_internal(
                "SELECT lix_json_extract(snapshot_content, 'commit_id') AS commit_id \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_version_ref' \
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
                description: "init invariant violation: hidden global version ref is missing"
                    .to_string(),
            });
        };
        text_value(first.first(), "lix_version_ref.commit_id")
    }

    async fn ensure_checkpoint_label_on_bootstrap_commit(
        &self,
        bootstrap_commit_id: &str,
        label_id: &str,
    ) -> Result<(), LixError> {
        let entity_label_id = checkpoint_commit_label_entity_id(bootstrap_commit_id);
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

        if label_id != CHECKPOINT_LABEL_ID {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("unexpected checkpoint label id '{label_id}'"),
            ));
        }
        let snapshot_content = checkpoint_commit_label_snapshot(bootstrap_commit_id);
        self.insert_bootstrap_tracked_row(
            Some(bootstrap_commit_id),
            &entity_label_id,
            "lix_entity_label",
            "1",
            "lix",
            "global",
            "lix",
            &snapshot_content,
        )
        .await?;

        Ok(())
    }

    async fn add_change_id_to_commit(
        &self,
        commit_id: &str,
        change_id: &str,
    ) -> Result<(), LixError> {
        let snapshot_row = self
            .backend
            .execute(
                "SELECT s.content \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.entity_id = $1 \
                   AND c.schema_key = 'lix_commit' \
                   AND c.file_id = 'lix' \
                 ORDER BY c.created_at DESC \
                 LIMIT 1",
                &[Value::Text(commit_id.to_string())],
            )
            .await?;

        let current_snapshot = snapshot_row
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|value| match value {
                Value::Text(text) => Some(text.as_str()),
                _ => None,
            })
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: commit '{commit_id}' canonical snapshot not found"
                    ),
                )
            })?;

        let mut parsed: JsonValue =
            serde_json::from_str(current_snapshot).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: invalid JSON in commit '{commit_id}' snapshot: {error}"
                    ),
                )
            })?;

        let change_ids = parsed
            .get_mut("change_ids")
            .and_then(JsonValue::as_array_mut)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: commit '{commit_id}' snapshot missing change_ids array"
                    ),
                )
            })?;
        change_ids.push(JsonValue::String(change_id.to_string()));

        let updated_snapshot = parsed.to_string();

        // Update the canonical snapshot
        let snapshot_id_row = self
            .backend
            .execute(
                "SELECT c.snapshot_id \
                 FROM lix_internal_change c \
                 WHERE c.entity_id = $1 \
                   AND c.schema_key = 'lix_commit' \
                   AND c.file_id = 'lix' \
                 ORDER BY c.created_at DESC \
                 LIMIT 1",
                &[Value::Text(commit_id.to_string())],
            )
            .await?;
        let snapshot_id = snapshot_id_row
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|value| match value {
                Value::Text(text) => Some(text.clone()),
                _ => None,
            })
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: could not find snapshot_id for commit '{commit_id}' change"
                    ),
                )
            })?;

        self.backend
            .execute(
                "UPDATE lix_internal_snapshot SET content = $1 WHERE id = $2",
                &[
                    Value::Text(updated_snapshot.clone()),
                    Value::Text(snapshot_id),
                ],
            )
            .await?;

        let normalized_values = normalized_seed_values("lix_commit", Some(&updated_snapshot))?;
        let set_sql = normalized_values
            .iter()
            .map(|(column, value)| format!("{} = {}", quote_ident(column), sql_literal(value)))
            .collect::<Vec<_>>()
            .join(", ");
        self.backend
            .execute(
                &format!(
                    "UPDATE {table} \
                     SET {set_sql} \
                     WHERE entity_id = $1 \
                       AND schema_key = 'lix_commit' \
                       AND file_id = 'lix' \
                       AND version_id = 'global'",
                    table = quote_ident(&tracked_live_table_name("lix_commit")),
                    set_sql = set_sql,
                ),
                &[Value::Text(commit_id.to_string())],
            )
            .await?;

        Ok(())
    }

    pub(crate) async fn seed_lix_id(&self) -> Result<(), LixError> {
        ensure_schema_live_table(self.backend.as_ref(), key_value_schema_key()).await?;
        let table = tracked_live_table_name(key_value_schema_key());
        let check_sql = format!(
            "SELECT 1 \
             FROM {table} \
             WHERE schema_key = '{schema_key}' \
               AND entity_id = '{entity_id}' \
               AND file_id = '{file_id}' \
               AND is_tombstone = 0 \
             LIMIT 1",
            table = quote_ident(&table),
            schema_key = escape_sql_string(key_value_schema_key()),
            entity_id = escape_sql_string(LIX_ID_KEY),
            file_id = escape_sql_string(key_value_file_id()),
        );
        let existing = self.backend.execute(&check_sql, &[]).await?;
        if !existing.rows.is_empty() {
            return Ok(());
        }

        let lix_id_value = self.generate_runtime_uuid().await?;
        let timestamp = self.generate_runtime_timestamp().await?;
        let version_id = KEY_VALUE_GLOBAL_VERSION;
        let snapshot_content = serde_json::json!({
            "key": LIX_ID_KEY,
            "value": lix_id_value,
        })
        .to_string();

        let change_id = self.generate_runtime_uuid().await?;
        let normalized_values =
            normalized_seed_values(key_value_schema_key(), Some(&snapshot_content))?;
        let insert_sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, change_id, metadata, writer_key, is_tombstone, created_at, updated_at{normalized_columns}\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', true, '{plugin_key}', '{change_id}', NULL, NULL, 0, '{timestamp}', '{timestamp}'{normalized_literals}\
             )",
            table = quote_ident(&table),
            entity_id = escape_sql_string(LIX_ID_KEY),
            schema_key = escape_sql_string(key_value_schema_key()),
            schema_version = escape_sql_string(key_value_schema_version()),
            file_id = escape_sql_string(key_value_file_id()),
            version_id = escape_sql_string(version_id),
            plugin_key = escape_sql_string(key_value_plugin_key()),
            change_id = escape_sql_string(&change_id),
            timestamp = escape_sql_string(&timestamp),
            normalized_columns = normalized_insert_columns_sql(&normalized_values),
            normalized_literals = normalized_insert_literals_sql(&normalized_values),
        );
        self.backend.execute(&insert_sql, &[]).await?;

        self.insert_change_row_for_snapshot(
            LIX_ID_KEY,
            key_value_schema_key(),
            key_value_schema_version(),
            key_value_file_id(),
            key_value_plugin_key(),
            &snapshot_content,
            &change_id,
            &timestamp,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn seed_default_versions(&self) -> Result<String, LixError> {
        // Bootstrap commit + change set must be seeded first so that
        // `add_change_id_to_commit` can find the canonical snapshot.
        let bootstrap_commit_id = match self.load_latest_commit_id().await? {
            Some(commit_id) => commit_id,
            None => {
                let bootstrap_change_set_id = self.generate_runtime_uuid().await?;
                let bootstrap_commit_id = self.generate_runtime_uuid().await?;
                self.seed_bootstrap_change_set(&bootstrap_change_set_id)
                    .await?;
                self.seed_bootstrap_commit(&bootstrap_commit_id, &bootstrap_change_set_id)
                    .await?;
                // Change set canonical storage does not need a change_ids entry
                // because lix_change_set is discovered via the commit snapshot's
                // change_set_id property, not through change_ids membership.
                bootstrap_commit_id
            }
        };
        self.assert_commit_change_set_integrity(&bootstrap_commit_id)
            .await?;

        let main_version_id = match self
            .find_version_id_by_name(DEFAULT_ACTIVE_VERSION_NAME)
            .await?
        {
            Some(version_id) => version_id,
            None => {
                let generated_main_id = self.generate_runtime_uuid().await?;
                let desc_change_id = self
                    .seed_canonical_version_descriptor(
                        &bootstrap_commit_id,
                        &generated_main_id,
                        DEFAULT_ACTIVE_VERSION_NAME,
                    )
                    .await?;
                self.seed_materialized_version_descriptor(
                    &generated_main_id,
                    DEFAULT_ACTIVE_VERSION_NAME,
                    &desc_change_id,
                )
                .await?;
                generated_main_id
            }
        };

        let global_desc_change_id = self
            .seed_canonical_version_descriptor(
                &bootstrap_commit_id,
                GLOBAL_VERSION_ID,
                GLOBAL_VERSION_ID,
            )
            .await?;
        self.seed_materialized_version_descriptor(
            GLOBAL_VERSION_ID,
            GLOBAL_VERSION_ID,
            &global_desc_change_id,
        )
        .await?;
        self.seed_materialized_version_ref(GLOBAL_VERSION_ID, &bootstrap_commit_id)
            .await?;
        self.seed_materialized_version_ref(&main_version_id, &bootstrap_commit_id)
            .await?;

        Ok(main_version_id)
    }

    pub(crate) async fn seed_commit_graph_nodes(&self) -> Result<(), LixError> {
        let graph_count_result = self
            .backend
            .execute(
                &format!("SELECT COUNT(*) FROM {COMMIT_GRAPH_NODE_TABLE}"),
                &[],
            )
            .await?;
        let graph_count =
            read_scalar_count(&graph_count_result, "lix_internal_commit_graph_node count")?;
        if graph_count > 0 {
            return Ok(());
        }

        let commit_table = tracked_live_table_name("lix_commit");
        let commit_count_result = self
            .backend
            .execute(
                &format!(
                    "SELECT COUNT(*) \
                     FROM {commit_table} \
                     WHERE schema_key = 'lix_commit' \
                       AND version_id = 'global' \
                       AND is_tombstone = 0"
                ),
                &[],
            )
            .await?;
        let commit_count = read_scalar_count(&commit_count_result, "lix_commit count")?;
        if commit_count == 0 {
            return Ok(());
        }

        self.backend
            .execute(&build_commit_generation_seed_sql(), &[])
            .await?;

        Ok(())
    }

    pub(crate) async fn seed_boot_account(&self) -> Result<(), LixError> {
        let Some(account) = self.boot_active_account() else {
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
        change_id: &str,
    ) -> Result<(), LixError> {
        ensure_schema_live_table(self.backend.as_ref(), version_descriptor_schema_key()).await?;
        let table = tracked_live_table_name(version_descriptor_schema_key());
        let check_sql = format!(
            "SELECT 1 \
             FROM {table} \
             WHERE schema_key = '{schema_key}' \
               AND entity_id = '{entity_id}' \
               AND file_id = '{file_id}' \
               AND version_id = '{version_id}' \
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
            version_descriptor_snapshot_content(entity_id, name, entity_id == GLOBAL_VERSION_ID);
        let timestamp = self.generate_runtime_timestamp().await?;
        builtin_live_table_layout(version_descriptor_schema_key())?.ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "builtin version descriptor schema must compile to a live layout",
            )
        })?;
        let normalized_values =
            normalized_seed_values(version_descriptor_schema_key(), Some(&snapshot_content))?;
        let insert_sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, change_id, metadata, writer_key, is_tombstone, created_at, updated_at{normalized_columns}\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', true, '{plugin_key}', '{change_id}', NULL, NULL, 0, '{timestamp}', '{timestamp}'{normalized_literals}\
             )",
            table = quote_ident(&table),
            entity_id = escape_sql_string(entity_id),
            schema_key = escape_sql_string(version_descriptor_schema_key()),
            schema_version = escape_sql_string(version_descriptor_schema_version()),
            file_id = escape_sql_string(version_descriptor_file_id()),
            version_id = escape_sql_string(version_descriptor_storage_version_id()),
            plugin_key = escape_sql_string(version_descriptor_plugin_key()),
            change_id = escape_sql_string(&change_id),
            timestamp = escape_sql_string(&timestamp),
            normalized_columns = normalized_insert_columns_sql(&normalized_values),
            normalized_literals = normalized_insert_literals_sql(&normalized_values),
        );
        self.backend.execute(&insert_sql, &[]).await?;

        Ok(())
    }

    pub(crate) async fn seed_canonical_version_descriptor(
        &self,
        bootstrap_commit_id: &str,
        entity_id: &str,
        name: &str,
    ) -> Result<String, LixError> {
        let snapshot_content =
            version_descriptor_snapshot_content(entity_id, name, entity_id == GLOBAL_VERSION_ID);
        let change_id = self.generate_runtime_uuid().await?;
        let timestamp = self.generate_runtime_timestamp().await?;
        self.insert_change_row_for_snapshot(
            entity_id,
            version_descriptor_schema_key(),
            version_descriptor_schema_version(),
            version_descriptor_file_id(),
            version_descriptor_plugin_key(),
            &snapshot_content,
            &change_id,
            &timestamp,
        )
        .await?;
        self.add_change_id_to_commit(bootstrap_commit_id, &change_id)
            .await?;
        Ok(change_id)
    }

    async fn insert_bootstrap_tracked_row(
        &self,
        attach_to_commit_id: Option<&str>,
        entity_id: &str,
        schema_key: &str,
        schema_version: &str,
        file_id: &str,
        version_id: &str,
        plugin_key: &str,
        snapshot_content: &str,
    ) -> Result<(), LixError> {
        ensure_schema_live_table(self.backend.as_ref(), schema_key).await?;
        let change_id = self.generate_runtime_uuid().await?;
        let timestamp = self.generate_runtime_timestamp().await?;
        let normalized_values = normalized_seed_values(schema_key, Some(snapshot_content))?;
        let insert_sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, change_id, metadata, writer_key, is_tombstone, created_at, updated_at{normalized_columns}\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', {global}, '{plugin_key}', '{change_id}', NULL, NULL, 0, '{timestamp}', '{timestamp}'{normalized_literals}\
             )",
            table = quote_ident(&tracked_live_table_name(schema_key)),
            entity_id = escape_sql_string(entity_id),
            schema_key = escape_sql_string(schema_key),
            schema_version = escape_sql_string(schema_version),
            file_id = escape_sql_string(file_id),
            version_id = escape_sql_string(version_id),
            global = if version_id == GLOBAL_VERSION_ID {
                "true"
            } else {
                "false"
            },
            plugin_key = escape_sql_string(plugin_key),
            change_id = escape_sql_string(&change_id),
            timestamp = escape_sql_string(&timestamp),
            normalized_columns = normalized_insert_columns_sql(&normalized_values),
            normalized_literals = normalized_insert_literals_sql(&normalized_values),
        );
        self.backend.execute(&insert_sql, &[]).await?;

        self.insert_change_row_for_snapshot(
            entity_id,
            schema_key,
            schema_version,
            file_id,
            plugin_key,
            snapshot_content,
            &change_id,
            &timestamp,
        )
        .await?;

        if let Some(commit_id) = attach_to_commit_id {
            self.add_change_id_to_commit(commit_id, &change_id).await?;
        }

        Ok(())
    }

    async fn insert_change_row_for_snapshot(
        &self,
        entity_id: &str,
        schema_key: &str,
        schema_version: &str,
        file_id: &str,
        plugin_key: &str,
        snapshot_content: &str,
        change_id: &str,
        created_at: &str,
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
                 SELECT $1, $2, $3, $4, $5, $6, $7, NULL, $8 \
                 WHERE NOT EXISTS (SELECT 1 FROM lix_internal_change WHERE id = $1)",
                &[
                    Value::Text(change_id.to_string()),
                    Value::Text(entity_id.to_string()),
                    Value::Text(schema_key.to_string()),
                    Value::Text(schema_version.to_string()),
                    Value::Text(file_id.to_string()),
                    Value::Text(plugin_key.to_string()),
                    Value::Text(snapshot_id),
                    Value::Text(created_at.to_string()),
                ],
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn find_version_id_by_name(
        &self,
        name: &str,
    ) -> Result<Option<String>, LixError> {
        let table = tracked_live_table_name(version_descriptor_schema_key());
        let name_column = quote_ident(&live_payload_column_name(
            version_descriptor_schema_key(),
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
            name_column = name_column,
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
            let snapshot_name = match &row[1] {
                Value::Text(value) => value,
                _ => continue,
            };
            if snapshot_name != name {
                continue;
            }
            return Ok(Some(entity_id.to_string()));
        }

        Ok(None)
    }

    pub(crate) async fn seed_materialized_version_ref(
        &self,
        entity_id: &str,
        commit_id: &str,
    ) -> Result<(), LixError> {
        ensure_schema_live_table(self.backend.as_ref(), version_ref_schema_key()).await?;
        let snapshot_content = version_ref_snapshot_content(entity_id, commit_id);
        let table = untracked_live_table_name(version_ref_schema_key());
        builtin_live_table_layout(version_ref_schema_key())?.ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "builtin version ref schema must compile to a live layout",
            )
        })?;
        let check_sql = format!(
            "SELECT 1 \
             FROM {table} \
             WHERE schema_key = '{schema_key}' \
               AND entity_id = '{entity_id}' \
               AND file_id = '{file_id}' \
               AND version_id = '{version_id}' \
             LIMIT 1",
            table = table,
            schema_key = escape_sql_string(version_ref_schema_key()),
            entity_id = escape_sql_string(entity_id),
            file_id = escape_sql_string(version_ref_file_id()),
            version_id = escape_sql_string(version_ref_storage_version_id()),
        );
        let existing = self.backend.execute(&check_sql, &[]).await?;
        if existing.rows.is_empty() {
            let timestamp = self.generate_runtime_timestamp().await?;
            let normalized_values =
                normalized_seed_values(version_ref_schema_key(), Some(&snapshot_content))?;
            let insert_sql = format!(
                "INSERT INTO {table} (\
                 entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, metadata, writer_key, created_at, updated_at{normalized_columns}\
                 ) VALUES (\
                 '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', true, '{plugin_key}', NULL, NULL, '{timestamp}', '{timestamp}'{normalized_literals}\
                 )",
                table = quote_ident(&table),
                entity_id = escape_sql_string(entity_id),
                schema_key = escape_sql_string(version_ref_schema_key()),
                schema_version = escape_sql_string(version_ref_schema_version()),
                file_id = escape_sql_string(version_ref_file_id()),
                version_id = escape_sql_string(version_ref_storage_version_id()),
                plugin_key = escape_sql_string(version_ref_plugin_key()),
                timestamp = escape_sql_string(&timestamp),
                normalized_columns = normalized_insert_columns_sql(&normalized_values),
                normalized_literals = normalized_insert_literals_sql(&normalized_values),
            );
            self.backend.execute(&insert_sql, &[]).await?;
        }

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
        head_commit_id: &str,
    ) -> Result<Option<String>, LixError> {
        let commit_edge_layout =
            builtin_live_table_layout("lix_commit_edge")?.ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "builtin schema layout missing for lix_commit_edge",
                )
            })?;
        let commit_edge_parent = live_column_name_for_property(&commit_edge_layout, "parent_id")
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "normalized live layout missing parent_id for lix_commit_edge",
                )
            })?;
        let commit_edge_child = live_column_name_for_property(&commit_edge_layout, "child_id")
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "normalized live layout missing child_id for lix_commit_edge",
                )
            })?;
        let commit_edge_table = tracked_live_table_name("lix_commit_edge");
        let rows = self
            .execute_internal(
                &format!(
                    "WITH RECURSIVE reachable(commit_id, depth) AS ( \
                       SELECT $1 AS commit_id, 0 AS depth \
                       UNION ALL \
                       SELECT \
                         edge.__PARENT_ID__ AS commit_id, \
                         reachable.depth + 1 AS depth \
                       FROM reachable \
                       JOIN {commit_edge_table} edge \
                         ON edge.__CHILD_ID__ = reachable.commit_id \
                       WHERE edge.schema_key = 'lix_commit_edge' \
                         AND edge.version_id = 'global' \
                         AND edge.is_tombstone = 0 \
                         AND edge.__PARENT_ID__ IS NOT NULL \
                     ) \
                     SELECT reachable.commit_id \
                     FROM reachable \
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
                       ON el.entity_id = reachable.commit_id \
                      AND el.schema_key = 'lix_commit' \
                      AND el.label_id = '{checkpoint_label_id}' \
                     LEFT JOIN ( \
                       SELECT entity_id AS id, created_at \
                       FROM lix_internal_state_vtable \
                       WHERE schema_key = 'lix_commit' \
                         AND file_id = 'lix' \
                         AND version_id = 'global' \
                         AND snapshot_content IS NOT NULL \
                     ) c ON c.id = reachable.commit_id \
                     ORDER BY \
                       reachable.depth ASC, \
                       c.created_at DESC, \
                       reachable.commit_id DESC \
                     LIMIT 1",
                    checkpoint_label_id = escape_sql_string(CHECKPOINT_LABEL_ID),
                )
                .replace("__PARENT_ID__", commit_edge_parent)
                .replace("__CHILD_ID__", commit_edge_child),
                &[Value::Text(head_commit_id.to_string())],
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
        self.insert_bootstrap_tracked_row(
            None,
            commit_id,
            "lix_commit",
            "1",
            "lix",
            "global",
            "lix",
            &snapshot_content,
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
        self.insert_bootstrap_tracked_row(
            None,
            change_set_id,
            "lix_change_set",
            "1",
            "lix",
            "global",
            "lix",
            &snapshot_content,
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
        ensure_schema_live_table(self.backend.as_ref(), active_version_schema_key()).await?;
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
        let check_sql = format!(
            "SELECT 1 \
             FROM {table_name} \
             WHERE file_id = '{file_id}' \
               AND file_id = '{file_id}' \
               AND version_id = '{storage_version_id}' \
               AND {payload_version_column} IS NOT NULL \
             LIMIT 1",
            table_name = quote_ident(&untracked_live_table_name(active_version_schema_key())),
            file_id = escape_sql_string(active_version_file_id()),
            storage_version_id = escape_sql_string(active_version_storage_version_id()),
            payload_version_column = quote_ident(payload_version_column),
        );
        let existing = self.backend.execute(&check_sql, &[]).await?;
        if !existing.rows.is_empty() {
            return Ok(());
        }

        let entity_id = self.generate_runtime_uuid().await?;
        let timestamp = self.generate_runtime_timestamp().await?;
        let snapshot_content = active_version_snapshot_content(&entity_id, version_id);
        builtin_live_table_layout(active_version_schema_key())?.ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "builtin active version schema must compile to a live layout",
            )
        })?;
        let normalized_values =
            normalized_seed_values(active_version_schema_key(), Some(&snapshot_content))?;
        let insert_sql = format!(
            "INSERT INTO {table_name} (\
             entity_id, schema_key, file_id, version_id, global, plugin_key, schema_version, created_at, updated_at{normalized_columns}\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{file_id}', '{storage_version_id}', true, '{plugin_key}', '{schema_version}', '{timestamp}', '{timestamp}'{normalized_literals}\
             )",
            table_name = quote_ident(&untracked_live_table_name(active_version_schema_key())),
            entity_id = escape_sql_string(&entity_id),
            schema_key = escape_sql_string(active_version_schema_key()),
            file_id = escape_sql_string(active_version_file_id()),
            storage_version_id = escape_sql_string(active_version_storage_version_id()),
            plugin_key = escape_sql_string(active_version_plugin_key()),
            schema_version = escape_sql_string(active_version_schema_version()),
            timestamp = escape_sql_string(&timestamp),
            normalized_columns = normalized_insert_columns_sql(&normalized_values),
            normalized_literals = normalized_insert_literals_sql(&normalized_values),
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

fn normalized_seed_values(
    schema_key: &str,
    snapshot_content: Option<&str>,
) -> Result<Vec<(String, Value)>, LixError> {
    let layout = builtin_live_table_layout(schema_key)?;
    let Some(layout) = layout.as_ref() else {
        return Ok(Vec::new());
    };
    Ok(normalized_live_column_values(layout, snapshot_content)?
        .into_iter()
        .collect())
}

fn normalized_insert_columns_sql(values: &[(String, Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(column, _)| format!(", {}", quote_ident(column)))
        .collect::<String>()
}

fn normalized_insert_literals_sql(values: &[(String, Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(_, value)| format!(", {}", sql_literal(value)))
        .collect::<String>()
}

fn sql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Boolean(value) => {
            if *value {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Integer(value) => value.to_string(),
        Value::Real(value) => value.to_string(),
        Value::Text(value) => format!("'{}'", escape_sql_string(value)),
        Value::Json(value) => format!("'{}'", escape_sql_string(&value.to_string())),
        Value::Blob(_) => "NULL".to_string(),
    }
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn live_payload_column_name(schema_key: &str, property_name: &str) -> String {
    let layout = builtin_live_table_layout(schema_key)
        .expect("builtin live layout lookup should succeed")
        .expect("builtin live layout should exist");
    live_column_name_for_property(&layout, property_name)
        .unwrap_or_else(|| {
            panic!("builtin live layout '{schema_key}' must include '{property_name}'")
        })
        .to_string()
}

fn system_directory_name(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    trimmed
        .rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty())
        .unwrap_or(".lix")
        .to_string()
}
