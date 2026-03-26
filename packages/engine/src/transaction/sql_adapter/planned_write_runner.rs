use std::collections::BTreeSet;

use crate::canonical::append::{
    append_tracked_with_pending_public_session, BufferedTrackedAppendArgs, CreateCommitDisposition,
    CreateCommitError, CreateCommitErrorKind, CreateCommitExpectedHead, CreateCommitIdempotencyKey,
    CreateCommitInvariantChecker, CreateCommitPreconditions, CreateCommitWriteLane,
};
use crate::canonical::ProposedDomainChange;
use crate::deterministic_mode::DeterministicSettings;
use crate::engine::Engine;
use crate::engine::TransactionBackendAdapter;
use crate::functions::LixFunctionProvider;
use crate::schema::live_layout::{normalized_live_column_values, tracked_live_table_name};
use crate::schema::registry::load_live_table_layout_in_transaction;
use crate::sql::public::validation::{validate_commit_time_write, SchemaCache};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError, QueryResult, Value};

use super::{
    apply_public_version_last_checkpoint_side_effects,
    build_filesystem_payload_domain_changes_insert, empty_public_write_execution_outcome,
    escape_sql_string, execute_internal_execution_with_transaction,
    mirror_public_registered_schema_bootstrap_rows, semantic_plan_effects_from_domain_changes,
    state_commit_stream_operation, DomainChangeBatch, PendingPublicCommitSession, PlanEffects,
    PlannedInternalWriteUnit, PlannedPublicUntrackedWriteUnit, PlannedStateRow, PlannedWriteDelta,
    PlannedWriteUnit, SqlExecutionOutcome, WriteLane,
};

struct PublicCommitInvariantChecker<'a> {
    planned_write: &'a crate::sql::public::planner::ir::PlannedWrite,
    schema_cache: SchemaCache,
}

impl<'a> PublicCommitInvariantChecker<'a> {
    fn new(planned_write: &'a crate::sql::public::planner::ir::PlannedWrite) -> Self {
        Self {
            planned_write,
            schema_cache: SchemaCache::new(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl CreateCommitInvariantChecker for PublicCommitInvariantChecker<'_> {
    async fn recheck_invariants(
        &mut self,
        transaction: &mut dyn LixBackendTransaction,
    ) -> Result<(), CreateCommitError> {
        let backend = TransactionBackendAdapter::new(transaction);
        validate_commit_time_write(&backend, &self.schema_cache, self.planned_write)
            .await
            .map_err(|error| CreateCommitError {
                kind: CreateCommitErrorKind::Internal,
                message: error.description,
            })
    }
}

pub(crate) async fn execute_planned_write_delta(
    engine: &Engine,
    transaction: &mut dyn LixBackendTransaction,
    delta: &PlannedWriteDelta,
    mut pending_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
) -> Result<SqlExecutionOutcome, LixError> {
    let mut combined = None;

    for unit in &delta.materialization_plan().units {
        let outcome = match unit {
            PlannedWriteUnit::PublicTracked(tracked) => {
                materialize_tracked_append_phase(
                    engine,
                    transaction,
                    tracked,
                    pending_commit_session.as_deref_mut(),
                )
                .await?
            }
            PlannedWriteUnit::PublicUntracked(untracked) => {
                run_public_untracked_write_txn_with_transaction(engine, transaction, untracked)
                    .await?
            }
            PlannedWriteUnit::Internal(internal) => {
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
    transaction: &mut dyn LixBackendTransaction,
    unit: &super::TrackedTxnUnit,
    mut pending_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
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
    let canonical_preconditions = canonical_create_commit_preconditions_for_tracked_unit(unit)?;
    if pending_commit_session
        .as_ref()
        .is_some_and(|slot| slot.as_ref().is_some())
        && !unit.has_compiler_only_filesystem_changes()
    {
        engine
            .ensure_runtime_sequence_initialized_in_transaction(
                transaction,
                &create_commit_functions,
            )
            .await?;
    }

    let mut invariant_checker = PublicCommitInvariantChecker::new(&unit.public_write.planned_write);
    let invariant_checker = if unit.is_merged_transaction_plan() {
        None
    } else {
        Some(&mut invariant_checker as &mut dyn CreateCommitInvariantChecker)
    };
    let append_outcome = append_tracked_with_pending_public_session(
        transaction,
        BufferedTrackedAppendArgs {
            timestamp: None,
            changes: unit
                .execution
                .domain_change_batch
                .as_ref()
                .map(|batch| public_domain_changes_to_proposed(&batch.changes))
                .transpose()?
                .unwrap_or_default(),
            filesystem_state: unit.filesystem_state.clone(),
            preconditions: canonical_preconditions.clone(),
            writer_key: unit.writer_key.clone(),
            should_emit_observe_tick: unit.should_emit_observe_tick(),
        },
        &mut create_commit_functions,
        invariant_checker,
        pending_commit_session.as_deref_mut(),
        !unit.has_compiler_only_filesystem_changes(),
    )
    .await?;

    if append_outcome.merged_into_pending_session
        && create_commit_functions
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

    if let Some(applied_output) = append_outcome.applied_output.as_ref() {
        mirror_public_registered_schema_bootstrap_rows(transaction, applied_output)
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
        if matches!(append_outcome.disposition, CreateCommitDisposition::Applied) {
            Some(DomainChangeBatch {
                changes: public_domain_changes_from_proposed(
                    &append_outcome.applied_domain_changes,
                ),
                write_lane: unit
                    .execution
                    .domain_change_batch
                    .as_ref()
                    .map(|batch| batch.write_lane.clone())
                    .unwrap_or_else(|| match &unit.execution.create_preconditions.write_lane {
                        crate::sql::public::planner::ir::WriteLane::SingleVersion(version_id) => {
                            WriteLane::SingleVersion(version_id.clone())
                        }
                        crate::sql::public::planner::ir::WriteLane::ActiveVersion => {
                            WriteLane::ActiveVersion
                        }
                        crate::sql::public::planner::ir::WriteLane::GlobalAdmin => {
                            WriteLane::GlobalAdmin
                        }
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
        matches!(append_outcome.disposition, CreateCommitDisposition::Applied);

    let plan_effects_override = if plugin_changes_committed {
        if unit.has_compiler_only_filesystem_changes() {
            semantic_plan_effects_from_domain_changes(
                &append_outcome.applied_domain_changes,
                state_commit_stream_operation(
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
        internal_write_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed,
        plan_effects_override: Some(plan_effects_override),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: plugin_changes_committed && unit.should_emit_observe_tick(),
    }))
}

async fn run_public_untracked_write_txn_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixBackendTransaction,
    plan: &PlannedPublicUntrackedWriteUnit,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    let mut runtime_functions = plan.functions.clone();
    let timestamp = runtime_functions.timestamp();

    if plan.execution.persist_filesystem_payloads_before_write {
        // Untracked filesystem writes materialize blob payloads eagerly, but keep
        // descriptor-domain visibility in the untracked live tables owned here.
    }

    apply_public_untracked_rows(
        transaction,
        &plan.execution.intended_post_state,
        &timestamp,
        plan.writer_key.as_deref(),
    )
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
        internal_write_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        plan_effects_override: Some(plan.execution.semantic_effects.clone()),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: false,
    }))
}

async fn run_internal_write_txn_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixBackendTransaction,
    plan: &PlannedInternalWriteUnit,
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
        .internal_write_file_cache_targets
        .extend(outcome.internal_write_file_cache_targets);
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
    transaction: &mut dyn LixBackendTransaction,
    rows: &[PlannedStateRow],
    timestamp: &str,
    execution_writer_key: Option<&str>,
) -> Result<(), LixError> {
    for row in rows {
        if row.tombstone {
            apply_public_untracked_delete(transaction, row).await?;
        } else {
            apply_public_untracked_upsert(transaction, row, timestamp, execution_writer_key)
                .await?;
        }
    }
    Ok(())
}

async fn apply_public_untracked_upsert(
    transaction: &mut dyn LixBackendTransaction,
    row: &PlannedStateRow,
    timestamp: &str,
    execution_writer_key: Option<&str>,
) -> Result<(), LixError> {
    let file_id = planned_row_text_value(row, "file_id")?;
    let plugin_key = planned_row_text_value(row, "plugin_key")?;
    let schema_version = planned_row_text_value(row, "schema_version")?;
    let snapshot_content = planned_row_json_text_value(row, "snapshot_content")?;
    let metadata_sql = planned_row_optional_text_value(row, "metadata")
        .map(|value| format!("'{}'", escape_sql_string(value)))
        .unwrap_or_else(|| "NULL".to_string());
    let writer_key_sql = planned_row_optional_text_value(row, "writer_key")
        .or(execution_writer_key)
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
         entity_id, schema_key, file_id, version_id, global, plugin_key, change_id, metadata, writer_key, schema_version, is_tombstone, untracked, created_at, updated_at{normalized_columns}\
         ) VALUES (\
         '{entity_id}', '{schema_key}', '{file_id}', '{version_id}', {global}, '{plugin_key}', NULL, {metadata}, {writer_key}, '{schema_version}', 0, true, '{timestamp}', '{timestamp}'{normalized_values}\
         ) ON CONFLICT (entity_id, file_id, version_id, untracked) DO UPDATE SET \
         global = excluded.global, \
         change_id = excluded.change_id, \
         plugin_key = excluded.plugin_key, \
         metadata = excluded.metadata, \
         writer_key = excluded.writer_key, \
         schema_version = excluded.schema_version, \
         is_tombstone = excluded.is_tombstone, \
         untracked = excluded.untracked, \
         updated_at = excluded.updated_at{normalized_updates}",
        table = quote_ident(&tracked_live_table_name(&row.schema_key)),
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
    transaction: &mut dyn LixBackendTransaction,
    row: &PlannedStateRow,
) -> Result<(), LixError> {
    let file_id = planned_row_text_value(row, "file_id")?;
    let sql = format!(
        "DELETE FROM {table} \
         WHERE entity_id = '{entity_id}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}' \
           AND untracked = true",
        table = quote_ident(&tracked_live_table_name(&row.schema_key)),
        entity_id = escape_sql_string(&row.entity_id),
        file_id = escape_sql_string(file_id),
        version_id = escape_sql_string(row.version_id.as_deref().unwrap_or(GLOBAL_VERSION_ID)),
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

fn planned_row_text_value<'a>(row: &'a PlannedStateRow, key: &str) -> Result<&'a str, LixError> {
    planned_row_optional_text_value(row, key).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("public untracked execution requires '{key}' in the resolved row"),
    })
}

fn planned_row_optional_text_value<'a>(row: &'a PlannedStateRow, key: &str) -> Option<&'a str> {
    row.values.get(key).and_then(|value| match value {
        Value::Text(value) => Some(value.as_str()),
        _ => None,
    })
}

fn planned_row_json_text_value(row: &PlannedStateRow, key: &str) -> Result<String, LixError> {
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
    transaction: &mut dyn LixBackendTransaction,
    changes: &[super::FilesystemPayloadDomainChange],
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

fn canonical_create_commit_preconditions_for_tracked_unit(
    unit: &super::TrackedTxnUnit,
) -> Result<CreateCommitPreconditions, LixError> {
    canonical_create_commit_preconditions_from_public_write(
        &unit.execution.create_preconditions,
        unit.execution.domain_change_batch.as_ref(),
        &unit.public_write,
    )
}

fn canonical_create_commit_preconditions_from_public_write(
    commit_preconditions: &crate::sql::public::planner::ir::CommitPreconditions,
    batch: Option<&DomainChangeBatch>,
    public_write: &super::PreparedPublicWrite,
) -> Result<CreateCommitPreconditions, LixError> {
    let write_lane = match &commit_preconditions.write_lane {
        crate::sql::public::planner::ir::WriteLane::SingleVersion(version_id) => {
            CreateCommitWriteLane::Version(version_id.clone())
        }
        crate::sql::public::planner::ir::WriteLane::ActiveVersion => {
            let version_id = batch
                .into_iter()
                .flat_map(|batch| batch.changes.first())
                .map(|change| change.version_id.clone())
                .next()
                .or_else(|| {
                    public_write
                        .planned_write
                        .command
                        .execution_context
                        .requested_version_id
                        .clone()
                })
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "public commit execution requires a concrete active version id",
                    )
                })?;
            CreateCommitWriteLane::Version(version_id)
        }
        crate::sql::public::planner::ir::WriteLane::GlobalAdmin => {
            CreateCommitWriteLane::GlobalAdmin
        }
    };
    let expected_head = match &commit_preconditions.expected_head {
        crate::sql::public::planner::ir::ExpectedHead::CurrentHead => {
            CreateCommitExpectedHead::CurrentHead
        }
        crate::sql::public::planner::ir::ExpectedHead::CommitId(commit_id) => {
            CreateCommitExpectedHead::CommitId(commit_id.clone())
        }
        crate::sql::public::planner::ir::ExpectedHead::CreateIfMissing => {
            CreateCommitExpectedHead::CreateIfMissing
        }
    };

    Ok(CreateCommitPreconditions {
        write_lane,
        expected_head,
        idempotency_key: match &commit_preconditions.expected_head {
            crate::sql::public::planner::ir::ExpectedHead::CurrentHead => {
                CreateCommitIdempotencyKey::CurrentHeadFingerprint(
                    commit_preconditions.idempotency_key.0.clone(),
                )
            }
            _ => CreateCommitIdempotencyKey::Exact(commit_preconditions.idempotency_key.0.clone()),
        },
    })
}

fn public_domain_changes_to_proposed(
    changes: &[crate::sql::public::planner::semantics::domain_changes::PublicDomainChange],
) -> Result<Vec<ProposedDomainChange>, LixError> {
    changes
        .iter()
        .map(public_domain_change_to_proposed)
        .collect()
}

fn public_domain_change_to_proposed(
    change: &crate::sql::public::planner::semantics::domain_changes::PublicDomainChange,
) -> Result<ProposedDomainChange, LixError> {
    Ok(ProposedDomainChange {
        entity_id: crate::EntityId::new(change.entity_id.clone())?,
        schema_key: crate::CanonicalSchemaKey::new(change.schema_key.clone())?,
        schema_version: change
            .schema_version
            .clone()
            .map(crate::CanonicalSchemaVersion::new)
            .transpose()?,
        file_id: change.file_id.clone().map(crate::FileId::new).transpose()?,
        plugin_key: change
            .plugin_key
            .clone()
            .map(crate::CanonicalPluginKey::new)
            .transpose()?,
        snapshot_content: change.snapshot_content.clone(),
        metadata: change.metadata.clone(),
        version_id: crate::VersionId::new(change.version_id.clone())?,
        writer_key: change.writer_key.clone(),
    })
}

fn public_domain_changes_from_proposed(
    changes: &[ProposedDomainChange],
) -> Vec<crate::sql::public::planner::semantics::domain_changes::PublicDomainChange> {
    changes
        .iter()
        .map(
            |change| crate::sql::public::planner::semantics::domain_changes::PublicDomainChange {
                entity_id: change.entity_id.to_string(),
                schema_key: change.schema_key.to_string(),
                schema_version: change.schema_version.as_ref().map(ToString::to_string),
                file_id: change.file_id.as_ref().map(ToString::to_string),
                plugin_key: change.plugin_key.as_ref().map(ToString::to_string),
                snapshot_content: change.snapshot_content.clone(),
                metadata: change.metadata.clone(),
                version_id: change.version_id.to_string(),
                writer_key: change.writer_key.clone(),
            },
        )
        .collect()
}
