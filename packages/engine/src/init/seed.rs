use crate::canonical::readers::load_committed_version_head_commit_id;
use crate::engine::{Engine, ExecuteOptions, TransactionBackendAdapter};
use crate::live_state::schema_access::{normalized_values_for_schema, tracked_relation_name};
use crate::sql::common::text::escape_sql_string;
use crate::sql::executor::execution_program::{ExecutionContext, SessionExecutionRuntime};
use crate::sql::executor::runtime_state::ExecutionRuntimeState;
use crate::sql::parser::parse_sql;
use crate::transaction::{
    execute_parsed_statements_in_borrowed_write_transaction, BorrowedWriteTransaction,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError, QueryResult, Value};
use serde_json::Value as JsonValue;

pub(crate) struct InitExecutor<'engine, 'tx> {
    engine: &'engine Engine,
    write_transaction: BorrowedWriteTransaction<'tx>,
    context: ExecutionContext,
}

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) fn new(
        engine: &'engine Engine,
        transaction: &'tx mut dyn LixBackendTransaction,
    ) -> Result<Self, LixError> {
        Ok(Self {
            engine,
            write_transaction: BorrowedWriteTransaction::new(transaction),
            context: ExecutionContext::new(
                ExecuteOptions::default(),
                engine.public_surface_registry(),
                SessionExecutionRuntime::new(),
                GLOBAL_VERSION_ID.to_string(),
                Vec::new(),
            ),
        })
    }

    pub(crate) fn boot_key_values(&self) -> &[crate::BootKeyValue] {
        self.engine.boot_key_values()
    }

    pub(crate) fn boot_active_account(&self) -> Option<&crate::BootAccount> {
        self.engine.boot_active_account()
    }

    pub(crate) async fn execute_internal(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<crate::ExecuteResult, LixError> {
        let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
        let result = execute_parsed_statements_in_borrowed_write_transaction(
            self.engine,
            &mut self.write_transaction,
            parsed_statements,
            params,
            true,
            &mut self.context,
            None,
        )
        .await?;
        self.write_transaction
            .flush_buffered_write_journal(self.engine, &mut self.context)
            .await?;
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
        runtime_state
            .ensure_sequence_initialized_in_transaction(
                self.engine,
                self.write_transaction.backend_transaction_mut(),
            )
            .await?;
        Ok(runtime_state.provider().call_uuid_v7())
    }

    pub(crate) async fn generate_runtime_timestamp(&mut self) -> Result<String, LixError> {
        let runtime_state = self.ensure_runtime_state().await?;
        runtime_state
            .ensure_sequence_initialized_in_transaction(
                self.engine,
                self.write_transaction.backend_transaction_mut(),
            )
            .await?;
        Ok(runtime_state.provider().call_timestamp())
    }

    pub(crate) async fn persist_runtime_state(&mut self) -> Result<(), LixError> {
        let Some(runtime_state) = self.context.execution_runtime_state().cloned() else {
            return Ok(());
        };
        runtime_state
            .flush_in_transaction(
                self.engine,
                self.write_transaction.backend_transaction_mut(),
            )
            .await
    }

    async fn ensure_runtime_state(&mut self) -> Result<ExecutionRuntimeState, LixError> {
        if let Some(runtime_state) = self.context.execution_runtime_state().cloned() {
            return Ok(runtime_state);
        }
        let backend =
            TransactionBackendAdapter::new(self.write_transaction.backend_transaction_mut());
        let runtime_state = ExecutionRuntimeState::prepare(self.engine, &backend).await?;
        self.context
            .set_execution_runtime_state(runtime_state.clone());
        Ok(runtime_state)
    }

    pub(crate) async fn load_latest_commit_id(&mut self) -> Result<Option<String>, LixError> {
        let mut backend =
            TransactionBackendAdapter::new(self.write_transaction.backend_transaction_mut());
        if let Some(commit_id) =
            load_committed_version_head_commit_id(&mut backend, GLOBAL_VERSION_ID).await?
        {
            return Ok(Some(commit_id));
        }

        let commit_table = tracked_relation_name("lix_commit");
        let has_commits = self
            .execute_backend(
                &format!(
                    "SELECT 1 \
                     FROM {commit_table} \
                     WHERE schema_key = 'lix_commit' \
                       AND version_id = 'global' \
                       AND is_tombstone = 0 \
                     LIMIT 1"
                ),
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
            load_committed_version_head_commit_id(&mut backend, GLOBAL_VERSION_ID).await?
        else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "init invariant violation: local global version head is missing"
                    .to_string(),
            });
        };
        Ok(commit_id)
    }

    pub(crate) async fn add_change_id_to_commit(
        &mut self,
        commit_id: &str,
        change_id: &str,
    ) -> Result<(), LixError> {
        let snapshot_row = self
            .execute_backend(
                "SELECT s.content \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.entity_id = $1 \
                   AND c.schema_key = 'lix_commit' \
                   AND c.file_id = 'lix' \
                 ORDER BY c.created_at DESC, c.id DESC \
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

        let snapshot_id_row = self
            .execute_backend(
                "SELECT c.snapshot_id \
                 FROM lix_internal_change c \
                 WHERE c.entity_id = $1 \
                   AND c.schema_key = 'lix_commit' \
                   AND c.file_id = 'lix' \
                 ORDER BY c.created_at DESC, c.id DESC \
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

        self.execute_backend(
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
        self.execute_backend(
            &format!(
                "UPDATE {table} \
                 SET {set_sql} \
                 WHERE entity_id = $1 \
                   AND schema_key = 'lix_commit' \
                   AND file_id = 'lix' \
                   AND version_id = 'global'",
                table = quote_ident(&tracked_relation_name("lix_commit")),
            ),
            &[Value::Text(commit_id.to_string())],
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
        let normalized_values = normalized_seed_values(schema_key, Some(snapshot_content))?;
        let insert_sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, change_id, metadata, writer_key, is_tombstone, created_at, updated_at{normalized_columns}\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', {global}, '{plugin_key}', '{change_id}', NULL, NULL, 0, '{timestamp}', '{timestamp}'{normalized_literals}\
             )",
            table = quote_ident(&tracked_relation_name(schema_key)),
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
        self.execute_backend(&insert_sql, &[]).await?;

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
        let normalized_values = normalized_seed_values(schema_key, Some(snapshot_content))?;
        let insert_sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, metadata, writer_key, untracked, created_at, updated_at{normalized_columns}\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', {global}, '{plugin_key}', NULL, NULL, true, '{timestamp}', '{timestamp}'{normalized_literals}\
             )",
            table = quote_ident(&tracked_relation_name(schema_key)),
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
            timestamp = escape_sql_string(&timestamp),
            normalized_columns = normalized_insert_columns_sql(&normalized_values),
            normalized_literals = normalized_insert_literals_sql(&normalized_values),
        );
        self.execute_backend(&insert_sql, &[]).await?;
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

pub(crate) fn normalized_seed_values(
    schema_key: &str,
    snapshot_content: Option<&str>,
) -> Result<Vec<(String, Value)>, LixError> {
    Ok(
        normalized_values_for_schema(schema_key, None, snapshot_content)?
            .into_iter()
            .collect(),
    )
}

pub(crate) fn normalized_insert_columns_sql(values: &[(String, Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(column, _)| format!(", {}", quote_ident(column)))
        .collect::<String>()
}

pub(crate) fn normalized_insert_literals_sql(values: &[(String, Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(_, value)| format!(", {}", sql_literal(value)))
        .collect::<String>()
}

pub(crate) fn sql_literal(value: &Value) -> String {
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

pub(crate) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
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
