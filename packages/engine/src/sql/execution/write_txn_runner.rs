use std::collections::BTreeSet;

use crate::engine::{dedupe_filesystem_payload_domain_changes, should_run_binary_cas_gc, Engine};
use crate::functions::LixFunctionProvider;
use crate::schema::live_layout::{normalized_live_column_values, untracked_live_table_name};
use crate::schema::registry::{
    ensure_schema_live_table_in_transaction, load_live_table_layout_in_transaction,
};
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::execute::{self, SqlExecutionOutcome};
use crate::sql::execution::runtime_effects::build_filesystem_payload_domain_changes_insert;
use crate::sql::execution::shared_path::{
    empty_public_write_execution_outcome, PendingPublicCommitSession,
};
use crate::sql::execution::tracked_write_runner::run_tracked_write_txn_plan_with_transaction;
use crate::sql::execution::write_txn_plan::{
    InternalWriteTxnPlan, PublicUntrackedWriteTxnPlan, WriteTxnPlan, WriteTxnRunMode, WriteTxnUnit,
};
use crate::sql::storage::sql_text::escape_sql_string;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixError, LixTransaction, QueryResult, Value};

pub(crate) async fn run_write_txn_plan_with_backend(
    engine: &Engine,
    plan: &WriteTxnPlan,
    pending_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
) -> Result<SqlExecutionOutcome, LixError> {
    let mut transaction = engine.backend.begin_transaction().await?;
    let result = run_write_txn_plan_with_transaction(
        engine,
        transaction.as_mut(),
        plan,
        WriteTxnRunMode::Owned,
        pending_commit_session,
    )
    .await;
    match result {
        Ok(result) => {
            transaction.commit().await?;
            Ok(result)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

pub(crate) async fn run_write_txn_plan_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    plan: &WriteTxnPlan,
    mode: WriteTxnRunMode,
    mut pending_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
) -> Result<SqlExecutionOutcome, LixError> {
    let mut combined = None;

    for unit in &plan.units {
        let outcome = match unit {
            WriteTxnUnit::PublicTracked(tracked) => {
                run_tracked_write_txn_plan_with_transaction(
                    engine,
                    transaction,
                    tracked,
                    pending_commit_session.as_deref_mut(),
                )
                .await?
            }
            WriteTxnUnit::PublicUntracked(untracked) => {
                run_public_untracked_write_txn_with_transaction(
                    engine,
                    transaction,
                    untracked,
                    mode,
                )
                .await?
            }
            WriteTxnUnit::Internal(internal) => {
                run_internal_write_txn_with_transaction(engine, transaction, internal, mode).await?
            }
        };

        if let Some(outcome) = outcome {
            merge_sql_execution_outcome(&mut combined, outcome);
        }
    }

    Ok(combined.unwrap_or_else(empty_public_write_execution_outcome))
}

async fn run_public_untracked_write_txn_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    plan: &PublicUntrackedWriteTxnPlan,
    mode: WriteTxnRunMode,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    let mut runtime_functions = plan.functions.clone();
    let timestamp = runtime_functions.timestamp();

    if plan.execution.persist_filesystem_payloads_before_write {
        engine
            .persist_pending_file_data_updates_in_transaction(
                transaction,
                &plan.pending_file_writes,
            )
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "public untracked filesystem payload persistence failed inside write txn: {}",
                    error.description
                ),
            })?;
    }

    apply_public_untracked_rows(transaction, &plan.execution.intended_post_state, &timestamp)
        .await?;

    let filesystem_payload_domain_changes = dedupe_filesystem_payload_domain_changes(
        &engine
            .collect_live_filesystem_payload_domain_changes_in_transaction(
                transaction,
                &plan.pending_file_writes,
                &plan.pending_file_delete_targets,
                plan.writer_key.as_deref(),
            )
            .await?,
    );
    // Public untracked writes already materialize their intended post-state directly into the
    // normalized per-schema untracked live tables via apply_public_untracked_rows(). Re-persisting
    // the derived payload-domain changes here is both redundant and unsafe, because the legacy
    // vtable-based insertion path is not part of the unified runner contract anymore.
    if should_run_binary_cas_gc(&[], &filesystem_payload_domain_changes) {
        engine
            .garbage_collect_unreachable_binary_cas_in_transaction(transaction)
            .await?;
    }

    engine
        .persist_runtime_sequence_in_transaction(
            transaction,
            plan.settings,
            plan.sequence_start,
            &plan.functions,
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "public untracked runtime-sequence persistence failed inside write txn: {}",
                error.description
            ),
        })?;

    if matches!(mode, WriteTxnRunMode::Owned)
        && !plan
            .execution
            .semantic_effects
            .state_commit_stream_changes
            .is_empty()
    {
        engine
            .append_observe_tick_in_transaction(transaction, plan.writer_key.as_deref())
            .await?;
    }

    Ok(Some(SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        postprocess_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        plan_effects_override: Some(plan.execution.semantic_effects.clone()),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: matches!(mode, WriteTxnRunMode::Owned)
            && !plan
                .execution
                .semantic_effects
                .state_commit_stream_changes
                .is_empty(),
    }))
}

async fn run_internal_write_txn_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    plan: &InternalWriteTxnPlan,
    mode: WriteTxnRunMode,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    let execution = execute::execute_plan_sql_with_transaction(
        transaction,
        &plan.plan,
        plan.plan.requirements.should_refresh_file_cache,
        &plan.functions,
        plan.writer_key.as_deref(),
    )
    .await
    .map_err(LixError::from)?;

    let filesystem_payload_domain_changes = dedupe_filesystem_payload_domain_changes(
        &engine
            .collect_live_filesystem_payload_domain_changes_in_transaction(
                transaction,
                &plan.pending_file_writes,
                &plan.pending_file_delete_targets,
                plan.writer_key.as_deref(),
            )
            .await?,
    );
    if !plan.pending_file_writes.is_empty() {
        engine
            .persist_pending_file_data_updates_in_transaction(
                transaction,
                &plan.pending_file_writes,
            )
            .await?;
    }
    persist_filesystem_payload_domain_changes_direct(
        transaction,
        &filesystem_payload_domain_changes,
    )
    .await?;
    if should_run_binary_cas_gc(
        &plan.plan.preprocess.mutations,
        &filesystem_payload_domain_changes,
    ) {
        engine
            .garbage_collect_unreachable_binary_cas_in_transaction(transaction)
            .await?;
    }

    engine
        .persist_runtime_sequence_in_transaction(
            transaction,
            plan.settings,
            plan.sequence_start,
            &plan.functions,
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "internal write runtime-sequence persistence failed inside write txn: {}",
                error.description
            ),
        })?;

    let active_effects = execution
        .plan_effects_override
        .as_ref()
        .unwrap_or(&plan.plan.effects);
    let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
    state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
    if matches!(mode, WriteTxnRunMode::Owned) && !state_commit_stream_changes.is_empty() {
        engine
            .append_observe_tick_in_transaction(transaction, plan.writer_key.as_deref())
            .await?;
    }

    Ok(Some(execution))
}

fn merge_sql_execution_outcome(
    combined: &mut Option<SqlExecutionOutcome>,
    outcome: SqlExecutionOutcome,
) {
    let Some(existing) = combined.as_mut() else {
        *combined = Some(outcome);
        return;
    };

    existing
        .postprocess_file_cache_targets
        .extend(outcome.postprocess_file_cache_targets);
    existing.plugin_changes_committed |= outcome.plugin_changes_committed;
    existing
        .state_commit_stream_changes
        .extend(outcome.state_commit_stream_changes);
    existing.observe_tick_emitted |= outcome.observe_tick_emitted;
    merge_plan_effects_override(
        &mut existing.plan_effects_override,
        outcome.plan_effects_override,
    );
}

fn merge_plan_effects_override(existing: &mut Option<PlanEffects>, next: Option<PlanEffects>) {
    match (existing, next) {
        (_, None) => {}
        (slot @ None, Some(next)) => {
            *slot = Some(next);
        }
        (Some(current), Some(next)) => {
            current
                .state_commit_stream_changes
                .extend(next.state_commit_stream_changes);
            current
                .file_cache_refresh_targets
                .extend(next.file_cache_refresh_targets);
            if next.next_active_version_id.is_some() {
                current.next_active_version_id = next.next_active_version_id;
            }
        }
    }
}

async fn apply_public_untracked_rows(
    transaction: &mut dyn LixTransaction,
    rows: &[crate::sql::public::planner::ir::PlannedStateRow],
    timestamp: &str,
) -> Result<(), LixError> {
    for row in rows {
        if row.tombstone {
            apply_public_untracked_delete(transaction, row).await?;
        } else {
            apply_public_untracked_upsert(transaction, row, timestamp).await?;
        }
    }
    Ok(())
}

async fn apply_public_untracked_upsert(
    transaction: &mut dyn LixTransaction,
    row: &crate::sql::public::planner::ir::PlannedStateRow,
    timestamp: &str,
) -> Result<(), LixError> {
    ensure_schema_live_table_in_transaction(transaction, &row.schema_key).await?;

    let file_id = planned_row_text_value(row, "file_id")?;
    let plugin_key = planned_row_text_value(row, "plugin_key")?;
    let schema_version = planned_row_text_value(row, "schema_version")?;
    let snapshot_content = planned_row_json_text_value(row, "snapshot_content")?;
    let metadata_sql = planned_row_optional_text_value(row, "metadata")
        .map(|value| format!("'{}'", escape_sql_string(value)))
        .unwrap_or_else(|| "NULL".to_string());
    let writer_key_sql = planned_row_optional_text_value(row, "writer_key")
        .map(|value| format!("'{}'", escape_sql_string(value)))
        .unwrap_or_else(|| "NULL".to_string());
    let global = row
        .values
        .get("global")
        .and_then(value_as_bool)
        .unwrap_or_else(|| row.version_id.as_deref() == Some(GLOBAL_VERSION_ID));

    let layout = load_live_table_layout_in_transaction(transaction, &row.schema_key).await?;
    let normalized_values = normalized_untracked_live_column_values_for_row(
        Some(&layout),
        Some(snapshot_content.as_str()),
    )?;
    let normalized_columns_sql = normalized_insert_columns_sql(&normalized_values);
    let normalized_values_sql = normalized_insert_values_sql(&normalized_values);
    let normalized_update_sql = normalized_update_assignments_sql(&normalized_values);
    let sql = format!(
        "INSERT INTO {table} (\
         entity_id, schema_key, file_id, version_id, global, plugin_key, metadata, writer_key, schema_version, created_at, updated_at{normalized_columns}\
         ) VALUES (\
         '{entity_id}', '{schema_key}', '{file_id}', '{version_id}', {global}, '{plugin_key}', {metadata}, {writer_key}, '{schema_version}', '{timestamp}', '{timestamp}'{normalized_values}\
         ) ON CONFLICT (entity_id, file_id, version_id) DO UPDATE SET \
         global = excluded.global, \
         plugin_key = excluded.plugin_key, \
         metadata = excluded.metadata, \
         writer_key = excluded.writer_key, \
         schema_version = excluded.schema_version, \
         updated_at = excluded.updated_at{normalized_updates}",
        table = quote_ident(&untracked_live_table_name(&row.schema_key)),
        entity_id = escape_sql_string(&row.entity_id),
        schema_key = escape_sql_string(&row.schema_key),
        file_id = escape_sql_string(file_id),
        version_id = escape_sql_string(row.version_id.as_deref().unwrap_or(GLOBAL_VERSION_ID)),
        global = if global { "true" } else { "false" },
        plugin_key = escape_sql_string(plugin_key),
        metadata = metadata_sql,
        writer_key = writer_key_sql,
        schema_version = escape_sql_string(schema_version),
        timestamp = escape_sql_string(timestamp),
        normalized_columns = normalized_columns_sql,
        normalized_values = normalized_values_sql,
        normalized_updates = normalized_update_sql,
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

async fn apply_public_untracked_delete(
    transaction: &mut dyn LixTransaction,
    row: &crate::sql::public::planner::ir::PlannedStateRow,
) -> Result<(), LixError> {
    ensure_schema_live_table_in_transaction(transaction, &row.schema_key).await?;

    let file_id = planned_row_text_value(row, "file_id")?;
    let sql = format!(
        "DELETE FROM {table} \
         WHERE entity_id = '{entity_id}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}'",
        table = quote_ident(&untracked_live_table_name(&row.schema_key)),
        entity_id = escape_sql_string(&row.entity_id),
        file_id = escape_sql_string(file_id),
        version_id = escape_sql_string(row.version_id.as_deref().unwrap_or(GLOBAL_VERSION_ID)),
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

fn planned_row_text_value<'a>(
    row: &'a crate::sql::public::planner::ir::PlannedStateRow,
    key: &str,
) -> Result<&'a str, LixError> {
    planned_row_optional_text_value(row, key).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("public untracked execution requires '{key}' in the resolved row"),
    })
}

fn planned_row_optional_text_value<'a>(
    row: &'a crate::sql::public::planner::ir::PlannedStateRow,
    key: &str,
) -> Option<&'a str> {
    row.values.get(key).and_then(|value| match value {
        Value::Text(value) => Some(value.as_str()),
        _ => None,
    })
}

fn planned_row_json_text_value(
    row: &crate::sql::public::planner::ir::PlannedStateRow,
    key: &str,
) -> Result<String, LixError> {
    row.values
        .get(key)
        .and_then(|value| match value {
            Value::Text(value) => Some(value.clone()),
            Value::Json(value) => Some(value.to_string()),
            _ => None,
        })
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("public untracked execution requires JSON '{key}'"),
        })
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(value) => Some(*value),
        _ => None,
    }
}

fn normalized_untracked_live_column_values_for_row(
    layout: Option<&crate::schema::live_layout::LiveTableLayout>,
    snapshot_content: Option<&str>,
) -> Result<Vec<(String, Value)>, LixError> {
    let Some(layout) = layout else {
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

fn normalized_insert_values_sql(values: &[(String, Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(_, value)| format!(", {}", sql_literal(value)))
        .collect::<String>()
}

fn normalized_update_assignments_sql(values: &[(String, Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(column, _)| {
            format!(
                ", {} = excluded.{}",
                quote_ident(column),
                quote_ident(column)
            )
        })
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
        Value::Blob(value) => {
            let mut out = String::from("X'");
            for byte in value {
                out.push_str(&format!("{byte:02X}"));
            }
            out.push('\'');
            out
        }
    }
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

async fn persist_filesystem_payload_domain_changes_direct(
    transaction: &mut dyn LixTransaction,
    changes: &[crate::sql::execution::contracts::effects::FilesystemPayloadDomainChange],
) -> Result<(), LixError> {
    let tracked = changes
        .iter()
        .filter(|change| !change.untracked)
        .cloned()
        .collect::<Vec<_>>();
    if !tracked.is_empty() {
        let (sql, params) = build_filesystem_payload_domain_changes_insert(&tracked, false);
        transaction.execute(&sql, &params).await?;
    }

    let untracked = changes
        .iter()
        .filter(|change| change.untracked)
        .cloned()
        .collect::<Vec<_>>();
    if !untracked.is_empty() {
        let (sql, params) = build_filesystem_payload_domain_changes_insert(&untracked, true);
        transaction.execute(&sql, &params).await?;
    }

    Ok(())
}
