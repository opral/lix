use std::collections::BTreeSet;

use crate::commit::{
    append_commit_if_preconditions_hold, AppendCommitArgs, AppendCommitPreconditions,
    AppendExpectedTip, AppendWriteLane,
};
use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::engine::Engine;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::schema_registry::register_schema_sql_statements;
use crate::sql2::runtime::{
    prepare_sql2_read, prepare_sql2_write, Sql2PreparedRead, Sql2PreparedWrite,
};
use crate::validation::{validate_inserts, validate_updates};
use crate::{LixBackend, LixError, LixTransaction, QueryResult, Value};

use super::super::contracts::execution_plan::ExecutionPlan;
use super::super::contracts::result_contract::ResultContract;
use super::super::planning::derive_requirements::derive_plan_requirements;
use super::super::planning::plan::build_execution_plan;
use super::intent::{
    authoritative_pending_file_write_targets, collect_execution_intent_with_backend,
    ExecutionIntent, IntentCollectionPolicy,
};
use super::run::SqlExecutionOutcome;
use sqlparser::ast::Statement;

pub(crate) struct PreparationPolicy {
    pub(crate) skip_side_effect_collection: bool,
}

pub(crate) struct PreparedExecutionContext {
    pub(crate) intent: ExecutionIntent,
    pub(crate) settings: DeterministicSettings,
    pub(crate) sequence_start: i64,
    pub(crate) functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    pub(crate) plan: ExecutionPlan,
    pub(crate) sql2_read: Option<Sql2PreparedRead>,
    pub(crate) sql2_write: Option<Sql2PreparedWrite>,
}

pub(crate) struct CacheTargets {
    pub(crate) file_cache_refresh_targets: BTreeSet<(String, String)>,
}

pub(crate) async fn prepare_execution_with_backend(
    engine: &Engine,
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    policy: PreparationPolicy,
) -> Result<PreparedExecutionContext, LixError> {
    let (settings, sequence_start, functions) = engine
        .prepare_runtime_functions_with_backend(backend)
        .await?;

    let mut statements = parsed_statements.to_vec();
    crate::filesystem::pending_file_writes::ensure_file_insert_ids_for_data_writes(
        &mut statements,
        &functions,
    )?;

    let requirements = derive_plan_requirements(&statements);

    engine
        .maybe_materialize_reads_with_backend_from_statements(
            backend,
            &statements,
            active_version_id,
        )
        .await?;

    let sql2_read =
        prepare_sql2_read(backend, &statements, params, active_version_id, writer_key).await;
    let sql2_write =
        prepare_sql2_write(backend, &statements, params, active_version_id, writer_key).await;

    let intent = collect_execution_intent_with_backend(
        engine,
        backend,
        &statements,
        params,
        active_version_id,
        writer_key,
        &requirements,
        IntentCollectionPolicy {
            skip_side_effect_collection: policy.skip_side_effect_collection,
        },
    )
    .await?;

    let plan = build_execution_plan(
        backend,
        &engine.cel_evaluator,
        statements,
        params,
        sql2_read
            .as_ref()
            .and_then(|prepared| prepared.dependency_spec.clone()),
        functions.clone(),
        &intent.detected_file_domain_changes_by_statement,
        &intent.pending_file_delete_targets,
        &authoritative_pending_file_write_targets(&intent.pending_file_writes),
        writer_key,
    )
    .await
    .map_err(LixError::from)?;

    if !plan.preprocess.mutations.is_empty() {
        validate_inserts(backend, &engine.schema_cache, &plan.preprocess.mutations).await?;
    }
    if !plan.preprocess.update_validations.is_empty() {
        validate_updates(
            backend,
            &engine.schema_cache,
            &plan.preprocess.update_validations,
            params,
        )
        .await?;
    }

    Ok(PreparedExecutionContext {
        intent,
        settings,
        sequence_start,
        functions,
        plan,
        sql2_read,
        sql2_write,
    })
}

pub(crate) fn derive_cache_targets(
    plan: &ExecutionPlan,
    postprocess_file_cache_targets: BTreeSet<(String, String)>,
) -> CacheTargets {
    let file_cache_refresh_targets = if plan.requirements.should_refresh_file_cache {
        let mut targets = plan.effects.file_cache_refresh_targets.clone();
        targets.extend(postprocess_file_cache_targets.clone());
        targets
    } else {
        BTreeSet::new()
    };

    CacheTargets {
        file_cache_refresh_targets,
    }
}

pub(crate) async fn maybe_execute_sql2_write_with_backend(
    engine: &Engine,
    prepared: &PreparedExecutionContext,
    writer_key: Option<&str>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    if !sql2_tracked_insert_is_live(prepared) {
        return Ok(None);
    }

    let mut transaction = engine.backend.begin_transaction().await?;
    let execution =
        match maybe_execute_sql2_write_with_transaction(transaction.as_mut(), prepared, writer_key)
            .await?
        {
            Some(execution) => execution,
            None => return Ok(None),
        };
    transaction.commit().await?;
    Ok(Some(execution))
}

pub(crate) async fn maybe_execute_sql2_write_with_transaction(
    transaction: &mut dyn LixTransaction,
    prepared: &PreparedExecutionContext,
    writer_key: Option<&str>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    if !sql2_tracked_insert_is_live(prepared) {
        return Ok(None);
    }

    let Some(sql2_write) = prepared.sql2_write.as_ref() else {
        return Ok(None);
    };
    let Some(domain_change_batch) = sql2_write.domain_change_batch.as_ref() else {
        return Ok(None);
    };
    let Some(commit_preconditions) = sql2_write.planned_write.commit_preconditions.as_ref() else {
        return Ok(None);
    };

    for registration in &prepared.plan.preprocess.registrations {
        for statement in
            register_schema_sql_statements(&registration.schema_key, transaction.dialect())
        {
            transaction.execute(&statement, &[]).await?;
        }
    }

    let mut append_functions = prepared.functions.clone();
    let timestamp = append_functions.timestamp();
    append_commit_if_preconditions_hold(
        transaction,
        AppendCommitArgs {
            timestamp,
            changes: domain_change_batch.changes.clone(),
            preconditions: append_preconditions(
                sql2_write,
                domain_change_batch,
                commit_preconditions,
            )?,
        },
        &mut append_functions,
    )
    .await
    .map_err(append_commit_error_to_lix_error)?;

    let _ = writer_key;
    Ok(Some(SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        postprocess_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: true,
        state_commit_stream_changes: Vec::new(),
    }))
}

fn sql2_tracked_insert_is_live(prepared: &PreparedExecutionContext) -> bool {
    let Some(sql2_write) = prepared.sql2_write.as_ref() else {
        return false;
    };

    matches!(
        prepared.plan.result_contract,
        ResultContract::DmlNoReturning
    ) && matches!(
        sql2_write.planned_write.command.operation_kind,
        crate::sql2::planner::ir::WriteOperationKind::Insert
    ) && matches!(
        sql2_write.planned_write.command.mode,
        crate::sql2::planner::ir::WriteMode::Tracked
    ) && sql2_write.domain_change_batch.is_some()
        && sql2_write.planned_write.commit_preconditions.is_some()
        && prepared.plan.preprocess.postprocess.is_none()
        && prepared.plan.preprocess.update_validations.is_empty()
        && prepared.intent.detected_file_domain_changes.is_empty()
        && prepared.intent.pending_file_writes.is_empty()
        && prepared.intent.pending_file_delete_targets.is_empty()
        && prepared
            .intent
            .untracked_filesystem_update_domain_changes
            .is_empty()
}

fn append_preconditions(
    sql2_write: &Sql2PreparedWrite,
    batch: &crate::sql2::planner::semantics::domain_changes::DomainChangeBatch,
    commit_preconditions: &crate::sql2::planner::ir::CommitPreconditions,
) -> Result<AppendCommitPreconditions, LixError> {
    let write_lane = match &commit_preconditions.write_lane {
        crate::sql2::planner::ir::WriteLane::SingleVersion(version_id) => {
            AppendWriteLane::Version(version_id.clone())
        }
        crate::sql2::planner::ir::WriteLane::ActiveVersion => {
            let version_id = batch
                .changes
                .first()
                .map(|change| change.version_id.clone())
                .or_else(|| {
                    sql2_write
                        .planned_write
                        .command
                        .execution_context
                        .requested_version_id
                        .clone()
                })
                .ok_or_else(|| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "sql2 append execution requires a concrete active version id"
                        .to_string(),
                })?;
            AppendWriteLane::Version(version_id)
        }
        crate::sql2::planner::ir::WriteLane::GlobalAdmin => AppendWriteLane::GlobalAdmin,
    };
    let expected_tip = match &commit_preconditions.expected_tip {
        crate::sql2::planner::ir::ExpectedTip::CommitId(commit_id) => {
            AppendExpectedTip::CommitId(commit_id.clone())
        }
        crate::sql2::planner::ir::ExpectedTip::CreateIfMissing => {
            AppendExpectedTip::CreateIfMissing
        }
    };

    Ok(AppendCommitPreconditions {
        write_lane,
        expected_tip,
        idempotency_key: commit_preconditions.idempotency_key.0.clone(),
    })
}

fn append_commit_error_to_lix_error(error: crate::commit::AppendCommitError) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.message,
    }
}
