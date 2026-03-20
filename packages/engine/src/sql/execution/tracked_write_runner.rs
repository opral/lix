use std::collections::BTreeSet;

use crate::deterministic_mode::DeterministicSettings;
use crate::engine::Engine;
use crate::functions::LixFunctionProvider;
use crate::schema::registry::{
    coalesce_live_table_requirements, ensure_schema_live_table_with_requirement_in_transaction,
};
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::execute::SqlExecutionOutcome;
use crate::sql::execution::runtime_effects::binary_blob_writes_from_filesystem_state;
use crate::sql::execution::shared_path::{
    apply_public_version_last_checkpoint_side_effects, build_pending_public_commit_session,
    create_commit_error_to_lix_error, empty_public_write_execution_outcome,
    merge_public_domain_change_batch_into_pending_commit,
    mirror_public_registered_schema_bootstrap_rows, pending_session_matches_create_commit,
    PendingPublicCommitSession, PublicCommitInvariantChecker,
};
use crate::sql::public::planner::ir::WriteLane;
use crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch;
use crate::sql::public::runtime::{
    semantic_plan_effects_from_domain_changes, state_commit_stream_operation, TrackedWriteTxnPlan,
};
use crate::state::commit::{
    create_commit, CreateCommitArgs, CreateCommitDisposition, CreateCommitInvariantChecker,
    CreateCommitWriteLane,
};
use crate::{LixError, LixTransaction, QueryResult};

pub(crate) async fn run_tracked_write_txn_plan_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    plan: &TrackedWriteTxnPlan,
    mut pending_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    for requirement in
        coalesce_live_table_requirements(&plan.execution.schema_live_table_requirements)
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

    if plan
        .execution
        .domain_change_batch
        .as_ref()
        .is_some_and(|batch| batch.changes.is_empty())
        && !plan.has_compiler_only_filesystem_changes()
    {
        return Ok(Some(empty_public_write_execution_outcome()));
    }

    let mut create_commit_functions = plan.functions.clone();
    if let Some(session_slot) = pending_commit_session.as_mut() {
        let can_merge = !plan.has_compiler_only_filesystem_changes()
            && session_slot.as_ref().is_some_and(|session| {
                pending_session_matches_create_commit(session, &plan.execution.create_preconditions)
            });
        if can_merge {
            let binary_blob_writes =
                binary_blob_writes_from_filesystem_state(&plan.filesystem_state);
            engine
                .ensure_runtime_sequence_initialized_in_transaction(
                    transaction,
                    &create_commit_functions,
                )
                .await?;
            let timestamp = create_commit_functions.timestamp();
            let mut invariant_checker =
                PublicCommitInvariantChecker::new(&plan.public_write.planned_write);
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
                plan.execution
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
                plan_effects_override: Some(plan.execution.semantic_effects.clone()),
                state_commit_stream_changes: Vec::new(),
                observe_tick_emitted: false,
            }));
        }
    }

    let mut invariant_checker = PublicCommitInvariantChecker::new(&plan.public_write.planned_write);
    let invariant_checker = if plan.is_merged_transaction_plan() {
        None
    } else {
        Some(&mut invariant_checker as &mut dyn CreateCommitInvariantChecker)
    };
    let create_result = create_commit(
        transaction,
        CreateCommitArgs {
            timestamp: None,
            changes: plan
                .execution
                .domain_change_batch
                .as_ref()
                .map(|batch| batch.changes.clone())
                .unwrap_or_default(),
            filesystem_state: plan.filesystem_state.clone(),
            preconditions: plan.execution.create_preconditions.clone(),
            should_emit_observe_tick: plan.should_emit_observe_tick(),
            observe_tick_writer_key: plan.writer_key.clone(),
            writer_key: plan.writer_key.clone(),
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
                write_lane: plan
                    .execution
                    .domain_change_batch
                    .as_ref()
                    .map(|batch| batch.write_lane.clone())
                    .unwrap_or_else(|| match &plan.execution.create_preconditions.write_lane {
                        CreateCommitWriteLane::Version(version_id) => {
                            WriteLane::SingleVersion(version_id.clone())
                        }
                        CreateCommitWriteLane::GlobalAdmin => WriteLane::GlobalAdmin,
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
        matches!(create_result.disposition, CreateCommitDisposition::Applied);
    if let Some(session_slot) = pending_commit_session.as_mut() {
        **session_slot = if plugin_changes_committed {
            if let Some(applied_output) = create_result.applied_output.as_ref() {
                Some(
                    build_pending_public_commit_session(
                        transaction,
                        plan.execution.create_preconditions.write_lane.clone(),
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
        if plan.has_compiler_only_filesystem_changes() {
            semantic_plan_effects_from_domain_changes(
                &create_result.applied_domain_changes,
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
        observe_tick_emitted: plugin_changes_committed && plan.should_emit_observe_tick(),
    }))
}
