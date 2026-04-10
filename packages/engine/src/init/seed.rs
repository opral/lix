use crate::contracts::artifacts::ExecuteOptions;
use crate::contracts::GLOBAL_VERSION_ID;
use crate::execution::write::buffered_write_transaction::BorrowedBufferedWriteTransaction;
use crate::live_state::{
    key_value_file_id, key_value_plugin_key, key_value_schema_key, key_value_schema_version,
    write_live_rows, LiveRow,
};
use crate::runtime::execution_state::ExecutionRuntimeState;
use crate::runtime::TransactionBackendAdapter;
use crate::session::execution_context::{ExecutionContext, SessionExecutionRuntime};
use crate::session::version_ops::load_version_head_commit_id_with_executor;
use crate::session::write_preparation::execute_parsed_statements_in_borrowed_write_transaction;
use crate::sql::parser::parse_sql;
use crate::{Lix, LixBackendTransaction, LixError, QueryResult, Value};
use serde_json::Value as JsonValue;

pub(crate) const LIX_ID_KEY: &str = "lix_id";

pub(crate) struct InitExecutor<'engine, 'tx> {
    lix: &'engine Lix,
    write_transaction: BorrowedBufferedWriteTransaction<'tx>,
    context: ExecutionContext,
}

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) fn new(
        lix: &'engine Lix,
        transaction: &'tx mut dyn LixBackendTransaction,
    ) -> Result<Self, LixError> {
        Ok(Self {
            lix,
            write_transaction: BorrowedBufferedWriteTransaction::new(transaction),
            context: ExecutionContext::new(
                ExecuteOptions::default(),
                lix.public_surface_registry(),
                SessionExecutionRuntime::new(),
                GLOBAL_VERSION_ID.to_string(),
                Vec::new(),
            ),
        })
    }

    pub(crate) fn boot_key_values(&self) -> &[crate::BootKeyValue] {
        self.lix.boot_key_values()
    }

    pub(crate) async fn execute_internal(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<crate::ExecuteResult, LixError> {
        let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
        let result = execute_parsed_statements_in_borrowed_write_transaction(
            self.lix,
            &mut self.write_transaction,
            parsed_statements,
            params,
            true,
            &mut self.context,
            None,
        )
        .await?;
        let mut execution_input = self.context.buffered_write_execution_input();
        self.write_transaction
            .flush_buffered_write_journal(self.lix, &mut execution_input)
            .await?;
        self.context
            .apply_buffered_write_execution_input(&execution_input);
        Ok(result)
    }

    pub(crate) async fn execute_backend(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        self.write_transaction
            .backend_transaction_mut()
            .execute(sql, params)
            .await
    }

    pub(crate) fn backend_adapter(&mut self) -> TransactionBackendAdapter<'_> {
        TransactionBackendAdapter::new(self.write_transaction.backend_transaction_mut())
    }

    pub(crate) fn backend_transaction_mut(
        &mut self,
    ) -> Result<&mut dyn LixBackendTransaction, LixError> {
        Ok(self.write_transaction.backend_transaction_mut())
    }

    pub(crate) async fn generate_runtime_uuid(&mut self) -> Result<String, LixError> {
        let runtime_state = self.ensure_runtime_state().await?;
        let mut runtime_functions = runtime_state.provider().clone();
        crate::runtime::deterministic_mode::ensure_runtime_sequence_initialized_in_transaction(
            self.write_transaction.backend_transaction_mut(),
            &mut runtime_functions,
        )
        .await?;
        Ok(runtime_state.provider().call_uuid_v7())
    }

    pub(crate) async fn generate_runtime_timestamp(&mut self) -> Result<String, LixError> {
        let runtime_state = self.ensure_runtime_state().await?;
        let mut runtime_functions = runtime_state.provider().clone();
        crate::runtime::deterministic_mode::ensure_runtime_sequence_initialized_in_transaction(
            self.write_transaction.backend_transaction_mut(),
            &mut runtime_functions,
        )
        .await?;
        Ok(runtime_state.provider().call_timestamp())
    }

    pub(crate) async fn persist_runtime_state(&mut self) -> Result<(), LixError> {
        let Some(runtime_state) = self.context.execution_runtime_state().cloned() else {
            return Ok(());
        };
        crate::runtime::deterministic_mode::persist_runtime_sequence_in_transaction(
            self.write_transaction.backend_transaction_mut(),
            runtime_state.provider(),
        )
        .await
    }

    async fn ensure_runtime_state(&mut self) -> Result<ExecutionRuntimeState, LixError> {
        if let Some(runtime_state) = self.context.execution_runtime_state().cloned() {
            return Ok(runtime_state);
        }
        let backend =
            TransactionBackendAdapter::new(self.write_transaction.backend_transaction_mut());
        let (settings, functions) = self
            .lix
            .prepare_runtime_functions_with_backend(&backend)
            .await?;
        let runtime_state = ExecutionRuntimeState::from_prepared_parts(settings, functions);
        self.context
            .set_execution_runtime_state(runtime_state.clone());
        Ok(runtime_state)
    }

    pub(crate) async fn load_latest_commit_id(&mut self) -> Result<Option<String>, LixError> {
        let mut backend =
            TransactionBackendAdapter::new(self.write_transaction.backend_transaction_mut());
        if let Some(commit_id) =
            load_version_head_commit_id_with_executor(&mut backend, GLOBAL_VERSION_ID).await?
        {
            return Ok(Some(commit_id));
        }

        let has_commits = self
            .execute_backend(
                "SELECT 1 \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.schema_key = 'lix_commit' \
                   AND c.file_id = 'lix' \
                   AND s.content IS NOT NULL \
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
                    "init invariant violation: commits exist but the local global version head is missing"
                        .to_string(),
            });
        }

        Ok(None)
    }

    pub(crate) async fn load_global_version_commit_id(&mut self) -> Result<String, LixError> {
        let mut backend = self.backend_adapter();
        let Some(commit_id) =
            load_version_head_commit_id_with_executor(&mut backend, GLOBAL_VERSION_ID).await?
        else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "init invariant violation: local global version head is missing"
                    .to_string(),
            });
        };
        Ok(commit_id)
    }

    pub(crate) async fn insert_bootstrap_key_value(
        &mut self,
        key: &str,
        value: &JsonValue,
        version_id: &str,
        untracked: bool,
        tracked_commit_id: Option<&str>,
    ) -> Result<(), LixError> {
        let snapshot_content = serde_json::json!({
            "key": key,
            "value": value,
        })
        .to_string();

        if untracked {
            self.insert_bootstrap_untracked_row(
                key,
                key_value_schema_key(),
                key_value_schema_version(),
                key_value_file_id(),
                version_id,
                key_value_plugin_key(),
                &snapshot_content,
            )
            .await
        } else {
            self.insert_bootstrap_tracked_row(
                tracked_commit_id,
                key,
                key_value_schema_key(),
                key_value_schema_version(),
                key_value_file_id(),
                version_id,
                key_value_plugin_key(),
                &snapshot_content,
            )
            .await
        }
    }

    pub(crate) async fn seed_boot_config_key_values(
        &mut self,
        default_active_version_id: &str,
    ) -> Result<(), LixError> {
        if self
            .boot_key_values()
            .iter()
            .any(|key_value| key_value.key == LIX_ID_KEY)
        {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("boot key `{LIX_ID_KEY}` is reserved for engine-owned identity state"),
            ));
        }

        let mut bootstrap_commit_id: Option<String> = None;
        for key_value in self.boot_key_values().to_vec() {
            let version_id = if key_value.lixcol_global.unwrap_or(false) {
                GLOBAL_VERSION_ID.to_string()
            } else {
                default_active_version_id.to_string()
            };
            let untracked = key_value.lixcol_untracked.unwrap_or(true);

            let tracked_commit_id = if untracked {
                None
            } else {
                Some(match &bootstrap_commit_id {
                    Some(commit_id) => commit_id.clone(),
                    None => {
                        let commit_id = self.load_global_version_commit_id().await?;
                        bootstrap_commit_id = Some(commit_id.clone());
                        commit_id
                    }
                })
            };

            self.insert_bootstrap_key_value(
                &key_value.key,
                &key_value.value,
                &version_id,
                untracked,
                tracked_commit_id.as_deref(),
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn seed_lix_id_key(&mut self) -> Result<(), LixError> {
        let lix_id_value = self.generate_runtime_uuid().await?;
        self.insert_bootstrap_key_value(
            LIX_ID_KEY,
            &JsonValue::String(lix_id_value),
            GLOBAL_VERSION_ID,
            false,
            None,
        )
        .await
    }

    pub(crate) async fn add_change_id_to_commit(
        &mut self,
        commit_id: &str,
        change_id: &str,
    ) -> Result<(), LixError> {
        let snapshot_rows = self
            .execute_backend(
                "SELECT c.snapshot_id, s.content \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.entity_id = $1 \
                   AND c.schema_key = 'lix_commit' \
                   AND c.file_id = 'lix' \
                   AND s.content IS NOT NULL",
                &[Value::Text(commit_id.to_string())],
            )
            .await?;

        let [snapshot_row] = snapshot_rows.rows.as_slice() else {
            return Err(if snapshot_rows.rows.is_empty() {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: commit '{commit_id}' canonical snapshot not found"
                    ),
                )
            } else {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: expected exactly one canonical snapshot for commit '{commit_id}', got {}",
                        snapshot_rows.rows.len()
                    ),
                )
            });
        };
        let snapshot_id = match snapshot_row.first() {
            Some(Value::Text(text)) if !text.is_empty() => text.clone(),
            Some(other) => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: commit '{commit_id}' snapshot_id must be text, got {other:?}"
                    ),
                ));
            }
            None => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: commit '{commit_id}' canonical snapshot row missing snapshot_id"
                    ),
                ));
            }
        };
        let current_snapshot = match snapshot_row.get(1) {
            Some(Value::Text(text)) => text.as_str(),
            Some(other) => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: commit '{commit_id}' snapshot content must be text, got {other:?}"
                    ),
                ));
            }
            None => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "add_change_id_to_commit: commit '{commit_id}' canonical snapshot row missing content"
                    ),
                ));
            }
        };

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
        if !change_ids
            .iter()
            .any(|existing| existing.as_str() == Some(change_id))
        {
            change_ids.push(JsonValue::String(change_id.to_string()));
        }

        let updated_snapshot = parsed.to_string();

        self.execute_backend(
            "UPDATE lix_internal_snapshot SET content = $1 WHERE id = $2",
            &[
                Value::Text(updated_snapshot.clone()),
                Value::Text(snapshot_id),
            ],
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn insert_bootstrap_tracked_row(
        &mut self,
        attach_to_commit_id: Option<&str>,
        entity_id: &str,
        schema_key: &str,
        schema_version: &str,
        file_id: &str,
        version_id: &str,
        plugin_key: &str,
        snapshot_content: &str,
    ) -> Result<(), LixError> {
        let change_id = self.generate_runtime_uuid().await?;
        let timestamp = self.generate_runtime_timestamp().await?;
        let row = LiveRow {
            entity_id: entity_id.to_string(),
            file_id: file_id.to_string(),
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
            version_id: version_id.to_string(),
            plugin_key: plugin_key.to_string(),
            metadata: None,
            change_id: Some(change_id.clone()),
            writer_key: None,
            global: version_id == GLOBAL_VERSION_ID,
            untracked: false,
            created_at: Some(timestamp.clone()),
            updated_at: Some(timestamp.clone()),
            snapshot_content: Some(snapshot_content.to_string()),
        };
        write_live_rows(self.backend_transaction_mut()?, &[row]).await?;

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

    pub(crate) async fn insert_bootstrap_untracked_row(
        &mut self,
        entity_id: &str,
        schema_key: &str,
        schema_version: &str,
        file_id: &str,
        version_id: &str,
        plugin_key: &str,
        snapshot_content: &str,
    ) -> Result<(), LixError> {
        let timestamp = self.generate_runtime_timestamp().await?;
        let row = LiveRow {
            entity_id: entity_id.to_string(),
            file_id: file_id.to_string(),
            schema_key: schema_key.to_string(),
            schema_version: schema_version.to_string(),
            version_id: version_id.to_string(),
            plugin_key: plugin_key.to_string(),
            metadata: None,
            change_id: None,
            writer_key: None,
            global: version_id == GLOBAL_VERSION_ID,
            untracked: true,
            created_at: Some(timestamp.clone()),
            updated_at: Some(timestamp),
            snapshot_content: Some(snapshot_content.to_string()),
        };
        write_live_rows(self.backend_transaction_mut()?, &[row]).await?;
        Ok(())
    }

    pub(crate) async fn insert_change_row_for_snapshot(
        &mut self,
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
        self.execute_backend(
            "INSERT INTO lix_internal_snapshot (id, content) \
             SELECT $1, $2 \
             WHERE NOT EXISTS (SELECT 1 FROM lix_internal_snapshot WHERE id = $1)",
            &[
                Value::Text(snapshot_id.clone()),
                Value::Text(snapshot_content.to_string()),
            ],
        )
        .await?;
        self.execute_backend(
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
}

pub(crate) fn read_scalar_count(result: &crate::QueryResult, label: &str) -> Result<i64, LixError> {
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

pub(crate) fn text_value(value: Option<&Value>, label: &str) -> Result<String, LixError> {
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

pub(crate) fn system_directory_name(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    trimmed
        .rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty())
        .unwrap_or(".lix")
        .to_string()
}
