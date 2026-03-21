use std::collections::BTreeSet;

use crate::deterministic_mode::DeterministicSettings;
use crate::engine::Engine;
use crate::functions::LixFunctionProvider;
use crate::schema::live_layout::{normalized_live_column_values, untracked_live_table_name};
use crate::schema::registry::{
    coalesce_live_table_requirements, ensure_schema_live_table_in_transaction,
    ensure_schema_live_table_with_requirement_in_transaction,
    load_live_table_layout_in_transaction,
};
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::execution_program::{
    execute_internal_execution_with_transaction, SqlExecutionOutcome,
};
use crate::sql::execution::runtime_effects::build_filesystem_payload_domain_changes_insert;
use crate::sql::execution::shared_path::{
    apply_public_version_last_checkpoint_side_effects, build_pending_public_commit_session,
    create_commit_error_to_lix_error, empty_public_write_execution_outcome,
    merge_public_domain_change_batch_into_pending_commit,
    mirror_public_registered_schema_bootstrap_rows, pending_session_matches_create_commit,
    PendingPublicCommitSession, PublicCommitInvariantChecker,
};
use crate::sql::execution::write_txn_plan::{
    InternalTxnUnit, PublicUntrackedTxnUnit, TxnDelta, TxnMaterializationUnit,
};
use crate::sql::public::planner::ir::WriteLane;
use crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch;
use crate::sql::storage::sql_text::escape_sql_string;
use crate::state::commit::{
    create_commit, CreateCommitArgs, CreateCommitDisposition, CreateCommitInvariantChecker,
    CreateCommitWriteLane,
};
use crate::state::live_state::{
    build_mark_live_state_ready_sql, load_latest_canonical_watermark_in_transaction,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixError, LixTransaction, QueryResult, Value};

pub(crate) async fn run_txn_delta_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    delta: &TxnDelta,
    mut pending_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
) -> Result<SqlExecutionOutcome, LixError> {
    let mut combined = None;

    for unit in &delta.materialization_plan().units {
        let outcome = match unit {
            TxnMaterializationUnit::PublicTracked(tracked) => {
                materialize_tracked_append_phase(
                    engine,
                    transaction,
                    tracked,
                    pending_commit_session.as_deref_mut(),
                )
                .await?
            }
            TxnMaterializationUnit::PublicUntracked(untracked) => {
                run_public_untracked_write_txn_with_transaction(engine, transaction, untracked)
                    .await?
            }
            TxnMaterializationUnit::Internal(internal) => {
                run_internal_write_txn_with_transaction(engine, transaction, internal).await?
            }
        };

        if let Some(outcome) = outcome {
            merge_sql_execution_outcome(&mut combined, outcome);
        }
    }

    Ok(combined.unwrap_or_else(empty_public_write_execution_outcome))
}

async fn materialize_tracked_append_phase(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    unit: &crate::sql::public::runtime::TrackedTxnUnit,
    mut pending_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    for requirement in
        coalesce_live_table_requirements(&unit.execution.schema_live_table_requirements)
    {
        ensure_schema_live_table_with_requirement_in_transaction(transaction, &requirement)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "public tracked write schema live-table ensure failed for '{}': {}",
                    requirement.schema_key, error.description
                ),
            })?;
    }

    if unit
        .execution
        .domain_change_batch
        .as_ref()
        .is_some_and(|batch| batch.changes.is_empty())
        && !unit.has_compiler_only_filesystem_changes()
    {
        return Ok(Some(empty_public_write_execution_outcome()));
    }

    let mut create_commit_functions = unit.functions.clone();
    if let Some(session_slot) = pending_commit_session.as_mut() {
        let can_merge = !unit.has_compiler_only_filesystem_changes()
            && session_slot.as_ref().is_some_and(|session| {
                pending_session_matches_create_commit(session, &unit.execution.create_preconditions)
            });
        if can_merge {
            let binary_blob_writes =
                crate::sql::execution::runtime_effects::binary_blob_writes_from_filesystem_state(
                    &unit.filesystem_state,
                );
            engine
                .ensure_runtime_sequence_initialized_in_transaction(
                    transaction,
                    &create_commit_functions,
                )
                .await?;
            let timestamp = create_commit_functions.timestamp();
            let mut invariant_checker =
                PublicCommitInvariantChecker::new(&unit.public_write.planned_write);
            invariant_checker
                .recheck_invariants(transaction)
                .await
                .map_err(create_commit_error_to_lix_error)?;
            let session = session_slot
                .as_mut()
                .expect("session should exist when can_merge is true");
            merge_public_domain_change_batch_into_pending_commit(
                transaction,
                session,
                unit.execution
                    .domain_change_batch
                    .as_ref()
                    .expect("merged tracked writes should have a domain change batch"),
                &binary_blob_writes,
                &mut create_commit_functions,
                &timestamp,
            )
            .await?;
            if create_commit_functions
                .deterministic_sequence_persist_highest_seen()
                .is_some()
            {
                let mut settings = DeterministicSettings::disabled();
                settings.enabled = create_commit_functions.deterministic_sequence_enabled();
                engine
                    .persist_runtime_sequence_in_transaction(
                        transaction,
                        settings,
                        0,
                        &create_commit_functions,
                    )
                    .await?;
            }

            return Ok(Some(SqlExecutionOutcome {
                public_result: QueryResult {
                    rows: Vec::new(),
                    columns: Vec::new(),
                },
                postprocess_file_cache_targets: BTreeSet::new(),
                plugin_changes_committed: true,
                plan_effects_override: Some(unit.execution.semantic_effects.clone()),
                state_commit_stream_changes: Vec::new(),
                observe_tick_emitted: false,
            }));
        }
    }

    let mut invariant_checker = PublicCommitInvariantChecker::new(&unit.public_write.planned_write);
    let invariant_checker = if unit.is_merged_transaction_plan() {
        None
    } else {
        Some(&mut invariant_checker as &mut dyn CreateCommitInvariantChecker)
    };
    let create_result = create_commit(
        transaction,
        CreateCommitArgs {
            timestamp: None,
            changes: unit
                .execution
                .domain_change_batch
                .as_ref()
                .map(|batch| batch.changes.clone())
                .unwrap_or_default(),
            filesystem_state: unit.filesystem_state.clone(),
            preconditions: unit.execution.create_preconditions.clone(),
            lane_parent_commit_ids_override: None,
            allow_empty_commit: false,
            should_emit_observe_tick: unit.should_emit_observe_tick(),
            observe_tick_writer_key: unit.writer_key.clone(),
            writer_key: unit.writer_key.clone(),
        },
        &mut create_commit_functions,
        invariant_checker,
    )
    .await
    .map_err(create_commit_error_to_lix_error)?;

    if let Some(applied_output) = create_result.applied_output.as_ref() {
        mirror_public_registered_schema_bootstrap_rows(
            transaction,
            &crate::state::commit::GenerateCommitResult {
                canonical_output: applied_output.canonical_output.clone(),
                derived_apply_input: applied_output.derived_apply_input.clone(),
            },
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "public tracked write registered-schema bootstrap mirroring failed: {}",
                error.description
            ),
        })?;
    }

    let applied_domain_change_batch =
        if matches!(create_result.disposition, CreateCommitDisposition::Applied) {
            Some(DomainChangeBatch {
                changes: create_result.applied_domain_changes.clone(),
                write_lane: unit
                    .execution
                    .domain_change_batch
                    .as_ref()
                    .map(|batch| batch.write_lane.clone())
                    .unwrap_or_else(|| match &unit.execution.create_preconditions.write_lane {
                        CreateCommitWriteLane::Version(version_id) => {
                            WriteLane::SingleVersion(version_id.clone())
                        }
                        CreateCommitWriteLane::GlobalAdmin => WriteLane::GlobalAdmin,
                    }),
                writer_key: unit
                    .execution
                    .domain_change_batch
                    .as_ref()
                    .and_then(|batch| batch.writer_key.clone())
                    .or_else(|| {
                        unit.public_write
                            .planned_write
                            .command
                            .execution_context
                            .writer_key
                            .clone()
                    }),
                semantic_effects: Vec::new(),
            })
        } else {
            None
        };
    if let Some(applied_domain_change_batch) = applied_domain_change_batch.as_ref() {
        apply_public_version_last_checkpoint_side_effects(
            transaction,
            &unit.public_write,
            applied_domain_change_batch,
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "public tracked write version checkpoint side effects failed: {}",
                error.description
            ),
        })?;
    }

    let plugin_changes_committed =
        matches!(create_result.disposition, CreateCommitDisposition::Applied);
    if let Some(session_slot) = pending_commit_session.as_mut() {
        **session_slot = if plugin_changes_committed {
            if let Some(applied_output) = create_result.applied_output.as_ref() {
                Some(
                    build_pending_public_commit_session(
                        transaction,
                        unit.execution.create_preconditions.write_lane.clone(),
                        &crate::state::commit::GenerateCommitResult {
                            canonical_output: applied_output.canonical_output.clone(),
                            derived_apply_input: applied_output.derived_apply_input.clone(),
                        },
                    )
                    .await?,
                )
            } else {
                None
            }
        } else {
            None
        };
    }

    let plan_effects_override = if plugin_changes_committed {
        if unit.has_compiler_only_filesystem_changes() {
            crate::sql::public::runtime::semantic_plan_effects_from_domain_changes(
                &create_result.applied_domain_changes,
                crate::sql::public::runtime::state_commit_stream_operation(
                    unit.public_write.planned_write.command.operation_kind,
                ),
            )?
        } else {
            unit.execution.semantic_effects.clone()
        }
    } else {
        PlanEffects::default()
    };

    Ok(Some(SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        postprocess_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed,
        plan_effects_override: Some(plan_effects_override),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: plugin_changes_committed && unit.should_emit_observe_tick(),
    }))
}

async fn run_public_untracked_write_txn_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    plan: &PublicUntrackedTxnUnit,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    let mut runtime_functions = plan.functions.clone();
    let timestamp = runtime_functions.timestamp();

    if plan.execution.persist_filesystem_payloads_before_write {
        // Untracked filesystem writes materialize blob payloads eagerly, but keep
        // descriptor-domain visibility in the untracked live tables owned here.
    }

    apply_public_untracked_rows(transaction, &plan.execution.intended_post_state, &timestamp)
        .await?;

    let filesystem_finalization = engine
        .compile_filesystem_finalization_from_state_in_transaction(
            transaction,
            &plan.filesystem_state,
            plan.writer_key.as_deref(),
            &[],
        )
        .await?;
    if plan.execution.persist_filesystem_payloads_before_write
        && !filesystem_finalization.binary_blob_writes.is_empty()
    {
        engine
            .persist_binary_blob_writes_in_transaction(
                transaction,
                &filesystem_finalization.binary_blob_writes,
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
    // Public untracked writes already materialize their intended post-state directly into the
    // normalized per-schema untracked live tables via apply_public_untracked_rows(). Re-persisting
    // the derived payload-domain changes here is both redundant and unsafe, because the legacy
    // vtable-based insertion path is not part of the unified runner contract anymore.
    if filesystem_finalization.should_run_gc {
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

    Ok(Some(SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        postprocess_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        plan_effects_override: Some(plan.execution.semantic_effects.clone()),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: false,
    }))
}

async fn run_internal_write_txn_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    plan: &InternalTxnUnit,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    let mut execution = execute_internal_execution_with_transaction(
        transaction,
        &plan.execution,
        plan.result_contract,
        &plan.functions,
        plan.writer_key.as_deref(),
    )
    .await
    .map_err(LixError::from)?;

    let filesystem_finalization = engine
        .compile_filesystem_finalization_from_state_in_transaction(
            transaction,
            &plan.filesystem_state,
            plan.writer_key.as_deref(),
            &plan.execution.mutations,
        )
        .await?;
    if !filesystem_finalization.binary_blob_writes.is_empty() {
        engine
            .persist_binary_blob_writes_in_transaction(
                transaction,
                &filesystem_finalization.binary_blob_writes,
            )
            .await?;
    }
    persist_filesystem_payload_domain_changes_direct(
        transaction,
        &filesystem_finalization.payload_domain_changes(),
    )
    .await?;
    if filesystem_finalization.should_run_gc {
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
        .unwrap_or(&plan.effects);
    let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
    state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
    if execution.plan_effects_override.is_none() {
        execution.plan_effects_override = Some(plan.effects.clone());
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

/// Writes the live-state watermark to match the latest canonical change,
/// called once just before a write transaction commits. This ensures the
/// watermark is always consistent regardless of how many statements or
/// merged commits ran inside the transaction.
pub(crate) async fn stamp_watermark_before_commit(
    transaction: &mut dyn LixTransaction,
) -> Result<(), LixError> {
    if let Some(watermark) = load_latest_canonical_watermark_in_transaction(transaction).await? {
        transaction
            .execute(&build_mark_live_state_ready_sql(&watermark), &[])
            .await?;
    }
    Ok(())
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
