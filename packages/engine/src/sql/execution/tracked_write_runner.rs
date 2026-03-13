use std::collections::BTreeSet;

use crate::engine::Engine;
use crate::functions::LixFunctionProvider;
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::execute::SqlExecutionOutcome;
use crate::sql::execution::shared_path::{
    append_commit_error_to_lix_error, apply_public_version_last_checkpoint_side_effects,
    build_pending_public_append_session, empty_public_write_execution_outcome,
    merge_public_domain_change_batch_into_pending_commit,
    mirror_public_stored_schema_bootstrap_rows, pending_session_matches_append,
    PendingPublicAppendSession, Sql2AppendInvariantChecker,
};
use crate::sql::public::planner::ir::WriteLane;
use crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch;
use crate::sql::public::runtime::{
    semantic_plan_effects_from_domain_changes, state_commit_stream_operation, TrackedWriteTxnPlan,
};
use crate::state::commit::{
    append_commit_if_preconditions_hold, AppendCommitArgs, AppendCommitDisposition,
    AppendCommitInvariantChecker, AppendWriteLane,
};
use crate::{LixError, LixTransaction, QueryResult};

pub(crate) async fn run_tracked_write_txn_plan_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    plan: &TrackedWriteTxnPlan,
    mut pending_append_session: Option<&mut Option<PendingPublicAppendSession>>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
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
                    "public tracked filesystem payload persistence failed before append: {}",
                    error.description
                ),
            })?;
    }

    for registration in &plan.execution.schema_registrations {
        for statement in crate::schema::registry::register_schema_sql_statements(
            &registration.schema_key,
            transaction.dialect(),
        ) {
            transaction
                .execute(&statement, &[])
                .await
                .map_err(|error| LixError {
                    code: error.code,
                    description: format!(
                        "public tracked write schema registration failed for '{}': {}",
                        registration.schema_key, error.description
                    ),
                })?;
        }
    }

    if plan
        .execution
        .domain_change_batch
        .as_ref()
        .is_some_and(|batch| batch.changes.is_empty())
        && !plan.has_lazy_exact_file_metadata_update()
    {
        return Ok(Some(empty_public_write_execution_outcome()));
    }

    let mut append_functions = plan.functions.clone();
    if let Some(session_slot) = pending_append_session.as_mut() {
        let can_merge = !plan.has_lazy_exact_file_metadata_update()
            && session_slot.as_ref().is_some_and(|session| {
                pending_session_matches_append(session, &plan.execution.append_preconditions)
            });
        if can_merge {
            engine
                .ensure_runtime_sequence_initialized_in_transaction(transaction, &append_functions)
                .await?;
            let timestamp = append_functions.timestamp();
            let mut invariant_checker =
                Sql2AppendInvariantChecker::new(&plan.public_write.planned_write);
            invariant_checker
                .recheck_invariants(transaction)
                .await
                .map_err(append_commit_error_to_lix_error)?;
            let session = session_slot
                .as_mut()
                .expect("session should exist when can_merge is true");
            merge_public_domain_change_batch_into_pending_commit(
                transaction,
                session,
                plan.execution
                    .domain_change_batch
                    .as_ref()
                    .expect("merged tracked writes should have a domain change batch"),
                &mut append_functions,
                &timestamp,
            )
            .await?;

            return Ok(Some(SqlExecutionOutcome {
                public_result: QueryResult {
                    rows: Vec::new(),
                    columns: Vec::new(),
                },
                postprocess_file_cache_targets: BTreeSet::new(),
                plugin_changes_committed: true,
                plan_effects_override: Some(plan.execution.semantic_effects.clone()),
                state_commit_stream_changes: Vec::new(),
            }));
        }
    }

    let mut invariant_checker = Sql2AppendInvariantChecker::new(&plan.public_write.planned_write);
    let append_result = append_commit_if_preconditions_hold(
        transaction,
        AppendCommitArgs {
            timestamp: None,
            changes: plan
                .execution
                .domain_change_batch
                .as_ref()
                .map(|batch| batch.changes.clone())
                .unwrap_or_default(),
            lazy_exact_file_metadata_update: plan.execution.lazy_exact_file_metadata_update.clone(),
            preconditions: plan.execution.append_preconditions.clone(),
            should_emit_observe_tick: plan.should_emit_observe_tick(),
            observe_tick_writer_key: plan.writer_key.clone(),
        },
        &mut append_functions,
        Some(&mut invariant_checker),
    )
    .await
    .map_err(append_commit_error_to_lix_error)?;

    if let Some(commit_result) = append_result.commit_result.as_ref() {
        mirror_public_stored_schema_bootstrap_rows(transaction, commit_result)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "public tracked write stored-schema bootstrap mirroring failed: {}",
                    error.description
                ),
            })?;
    }

    let applied_domain_change_batch =
        if matches!(append_result.disposition, AppendCommitDisposition::Applied) {
            Some(DomainChangeBatch {
                changes: append_result.applied_domain_changes.clone(),
                write_lane: plan
                    .execution
                    .domain_change_batch
                    .as_ref()
                    .map(|batch| batch.write_lane.clone())
                    .unwrap_or_else(|| match &plan.execution.append_preconditions.write_lane {
                        AppendWriteLane::Version(version_id) => {
                            WriteLane::SingleVersion(version_id.clone())
                        }
                        AppendWriteLane::GlobalAdmin => WriteLane::GlobalAdmin,
                    }),
                writer_key: plan
                    .execution
                    .domain_change_batch
                    .as_ref()
                    .and_then(|batch| batch.writer_key.clone())
                    .or_else(|| {
                        plan.public_write
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
            &plan.public_write,
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
        matches!(append_result.disposition, AppendCommitDisposition::Applied);
    if let Some(session_slot) = pending_append_session.as_mut() {
        **session_slot = if plugin_changes_committed {
            if let Some(commit_result) = append_result.commit_result.as_ref() {
                Some(
                    build_pending_public_append_session(
                        transaction,
                        plan.execution.append_preconditions.write_lane.clone(),
                        commit_result,
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
        if plan.has_lazy_exact_file_metadata_update() {
            semantic_plan_effects_from_domain_changes(
                &append_result.applied_domain_changes,
                state_commit_stream_operation(
                    plan.public_write.planned_write.command.operation_kind,
                ),
            )?
        } else {
            plan.execution.semantic_effects.clone()
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
    }))
}
