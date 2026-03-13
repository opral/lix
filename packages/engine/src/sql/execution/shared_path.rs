use std::collections::{BTreeMap, BTreeSet};

use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::engine::{Engine, TransactionBackendAdapter};
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::schema::registry::register_schema_sql_statements;
use crate::sql::public::runtime::{
    finalize_public_write_execution, prepare_public_execution_with_internal_access,
    prepare_public_execution_with_registry_and_internal_access,
    prepared_public_write_mutates_public_surface_registry, PreparedPublicExecution,
    PreparedPublicRead, PreparedPublicWrite, PublicWriteExecutionPartition,
};
use crate::sql::storage::sql_text::escape_sql_string;
use crate::state::commit::{
    append_commit_if_preconditions_hold, bind_statement_batch_for_dialect,
    build_statement_batch_from_generate_commit_result, generate_commit,
    load_commit_active_accounts, load_version_info_for_versions, AppendCommitArgs,
    AppendCommitDisposition, AppendCommitError, AppendCommitErrorKind,
    AppendCommitInvariantChecker, AppendCommitPreconditions, AppendExpectedTip, AppendWriteLane,
    CommitQueryExecutor, DomainChangeInput, GenerateCommitArgs, GenerateCommitResult,
    MaterializedStateRow, VersionInfo,
};
use crate::state::validation::{
    validate_inserts, validate_sql2_append_time_write, validate_sql2_batch_local_write,
    validate_updates,
};
use crate::{LixBackend, LixError, LixTransaction, QueryResult, Value};

use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::contracts::execution_plan::ExecutionPlan;
use crate::sql::execution::contracts::planned_statement::{
    PlannedStatementSet, SchemaRegistration,
};
use crate::sql::execution::contracts::requirements::PlanRequirements;
use crate::sql::execution::contracts::result_contract::ResultContract;
use crate::sql::execution::derive_requirements::derive_plan_requirements;
use crate::sql::execution::execute::SqlExecutionOutcome;
use crate::sql::execution::intent::{
    authoritative_pending_file_write_targets, collect_execution_intent_with_backend,
    ExecutionIntent, IntentCollectionPolicy,
};
use crate::sql::execution::plan::build_execution_plan;
use crate::sql::execution::write_program_runner::execute_write_program_with_transaction;
use crate::state::internal::write_program::WriteProgram;
use serde_json::{json, Value as JsonValue};
use sqlparser::ast::Statement;

const STORED_SCHEMA_KEY: &str = "lix_stored_schema";
const STORED_SCHEMA_BOOTSTRAP_TABLE: &str = "lix_internal_stored_schema_bootstrap";
const UNTRACKED_TABLE: &str = "lix_internal_live_untracked_v1";
const GLOBAL_VERSION_ID: &str = "global";

pub(crate) struct PreparationPolicy {
    pub(crate) skip_side_effect_collection: bool,
}

pub(crate) struct PreparedExecutionContext {
    pub(crate) intent: ExecutionIntent,
    pub(crate) settings: DeterministicSettings,
    pub(crate) sequence_start: i64,
    pub(crate) functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    pub(crate) plan: ExecutionPlan,
    pub(crate) public_read: Option<PreparedPublicRead>,
    pub(crate) public_write: Option<PreparedPublicWrite>,
}

pub(crate) fn prepared_execution_mutates_public_surface_registry(
    prepared: &PreparedExecutionContext,
) -> Result<bool, LixError> {
    if prepared.public_write.is_some() {
        return prepared
            .public_write
            .as_ref()
            .map(prepared_public_write_mutates_public_surface_registry)
            .transpose()
            .map(|value| value.unwrap_or(false));
    }

    if prepared.plan.preprocess.mutations.iter().any(|row| {
        row.schema_key == STORED_SCHEMA_KEY && row.version_id == GLOBAL_VERSION_ID && !row.untracked
    }) {
        return Ok(true);
    }

    let dirty = match prepared.plan.preprocess.internal_state.as_ref() {
        Some(crate::state::internal::InternalStatePlan {
            postprocess: Some(crate::state::internal::PostprocessPlan::VtableUpdate(plan)),
        }) => plan.schema_key == STORED_SCHEMA_KEY,
        Some(crate::state::internal::InternalStatePlan {
            postprocess: Some(crate::state::internal::PostprocessPlan::VtableDelete(plan)),
        }) => plan.schema_key == STORED_SCHEMA_KEY,
        _ => false,
    };

    Ok(dirty)
}

pub(crate) struct CacheTargets {
    pub(crate) file_cache_refresh_targets: BTreeSet<(String, String)>,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingPublicAppendSession {
    pub(crate) lane: AppendWriteLane,
    pub(crate) commit_id: String,
    pub(crate) change_set_id: String,
    pub(crate) commit_change_id: String,
    pub(crate) commit_change_snapshot_id: String,
    pub(crate) commit_materialized_change_id: String,
    pub(crate) commit_schema_version: String,
    pub(crate) commit_file_id: String,
    pub(crate) commit_plugin_key: String,
    pub(crate) commit_snapshot: JsonValue,
}

struct Sql2AppendInvariantChecker<'a> {
    planned_write: &'a crate::sql::public::planner::ir::PlannedWrite,
    schema_cache: crate::state::validation::SchemaCache,
}

impl<'a> Sql2AppendInvariantChecker<'a> {
    fn new(planned_write: &'a crate::sql::public::planner::ir::PlannedWrite) -> Self {
        Self {
            planned_write,
            schema_cache: crate::state::validation::SchemaCache::new(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl AppendCommitInvariantChecker for Sql2AppendInvariantChecker<'_> {
    async fn recheck_invariants(
        &mut self,
        transaction: &mut dyn LixTransaction,
    ) -> Result<(), AppendCommitError> {
        let backend = TransactionBackendAdapter::new(transaction);
        validate_sql2_append_time_write(&backend, &self.schema_cache, self.planned_write)
            .await
            .map_err(|error| AppendCommitError {
                kind: AppendCommitErrorKind::Internal,
                message: error.description,
            })
    }
}

struct TransactionCommitExecutor<'a> {
    transaction: &'a mut dyn LixTransaction,
}

#[async_trait::async_trait(?Send)]
impl CommitQueryExecutor for TransactionCommitExecutor<'_> {
    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.transaction.execute(sql, params).await
    }
}

pub(crate) async fn prepare_execution_with_backend(
    engine: &Engine,
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    public_surface_registry_override: Option<&crate::sql::public::catalog::SurfaceRegistry>,
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
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "prepare_execution_with_backend read materialization failed: {}",
                error.description
            ),
        })?;

    let public_execution = match public_surface_registry_override {
        Some(registry) => {
            prepare_public_execution_with_registry_and_internal_access(
                backend,
                registry,
                &statements,
                params,
                active_version_id,
                writer_key,
                allow_internal_tables,
            )
            .await
        }
        None => {
            prepare_public_execution_with_internal_access(
                backend,
                &statements,
                params,
                active_version_id,
                writer_key,
                allow_internal_tables,
            )
            .await
        }
    }
    .map_err(|error| LixError {
        code: error.code,
        description: format!(
            "prepare_execution_with_backend public preparation failed: {}",
            error.description
        ),
    })?;
    let (public_read, mut public_write) = match public_execution {
        Some(PreparedPublicExecution::Read(prepared)) => (Some(prepared), None),
        Some(PreparedPublicExecution::Write(prepared)) => (None, Some(prepared)),
        None => (None, None),
    };
    let plan_statements = public_read
        .as_ref()
        .map(|prepared| prepared.lowered_read.statements.clone())
        .unwrap_or_else(|| statements.clone());

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
    .await
    .map_err(|error| LixError {
        code: error.code,
        description: format!(
            "prepare_execution_with_backend intent collection failed: {}",
            error.description
        ),
    })?;

    let public_write_owns_execution = public_write.is_some();
    if let Some(public_write) = public_write.as_mut() {
        if let Some(execution) = public_write.execution.as_mut() {
            let planned_write = &public_write.planned_write;
            finalize_public_write_execution(
                execution,
                planned_write,
                &intent.pending_file_writes,
                &intent.pending_file_delete_targets,
            )
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "prepare_execution_with_backend public execution finalization failed: {}",
                    error.description
                ),
            })?;
        }
    }

    let plan = if public_write_owns_execution {
        passthrough_execution_plan_for_public_write(
            &statements,
            public_write
                .as_ref()
                .map(|prepared| {
                    prepared
                        .execution
                        .as_ref()
                        .map(|execution| {
                            execution
                                .partitions
                                .iter()
                                .filter_map(|partition| match partition {
                                    PublicWriteExecutionPartition::Tracked(execution) => {
                                        Some(execution.schema_registrations.clone())
                                    }
                                    PublicWriteExecutionPartition::Untracked(_) => None,
                                })
                                .flatten()
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                })
                .unwrap_or_default(),
        )
    } else {
        build_execution_plan(
            backend,
            &engine.cel_evaluator,
            plan_statements,
            params,
            public_read
                .as_ref()
                .and_then(|prepared| prepared.dependency_spec.clone()),
            functions.clone(),
            &intent.pending_file_delete_targets,
            &authoritative_pending_file_write_targets(&intent.pending_file_writes),
            writer_key,
        )
        .await
        .map_err(LixError::from)
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "prepare_execution_with_backend plan building failed: {}",
                error.description
            ),
        })?
    };

    if !public_write_owns_execution && !plan.preprocess.mutations.is_empty() {
        validate_inserts(backend, &engine.schema_cache, &plan.preprocess.mutations)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "prepare_execution_with_backend insert validation failed: {}",
                    error.description
                ),
            })?;
    }
    if !public_write_owns_execution && !plan.preprocess.update_validations.is_empty() {
        validate_updates(
            backend,
            &engine.schema_cache,
            &plan.preprocess.update_validations,
            params,
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "prepare_execution_with_backend update validation failed: {}",
                error.description
            ),
        })?;
    }
    if let Some(public_write) = public_write.as_ref() {
        validate_sql2_batch_local_write(backend, &engine.schema_cache, &public_write.planned_write)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "prepare_execution_with_backend public batch-local validation failed: {}",
                    error.description
                ),
            })?;
    }

    Ok(PreparedExecutionContext {
        intent,
        settings,
        sequence_start,
        functions,
        plan,
        public_read,
        public_write,
    })
}

fn passthrough_execution_plan_for_public_write(
    statements: &[Statement],
    registrations: Vec<SchemaRegistration>,
) -> ExecutionPlan {
    ExecutionPlan {
        preprocess: PlannedStatementSet {
            sql: String::new(),
            prepared_statements: Vec::new(),
            registrations,
            internal_state: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        },
        result_contract: derive_result_contract_for_statements(statements),
        requirements: PlanRequirements::default(),
        dependency_spec: crate::sql::common::dependency_spec::DependencySpec::default(),
        effects: PlanEffects::default(),
    }
}

fn derive_result_contract_for_statements(statements: &[Statement]) -> ResultContract {
    match statements.last() {
        Some(Statement::Query(_) | Statement::Explain { .. }) => ResultContract::Select,
        Some(Statement::Insert(insert)) => {
            if insert.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        Some(Statement::Update(update)) => {
            if update.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        Some(Statement::Delete(delete)) => {
            if delete.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        Some(_) | None => ResultContract::Other,
    }
}

#[cfg(test)]
fn top_level_write_target_name(statement: &Statement) -> Option<String> {
    match statement {
        Statement::Insert(insert) => match &insert.table {
            sqlparser::ast::TableObject::TableName(name) => Some(name.to_string()),
            _ => None,
        },
        Statement::Update(update) => match &update.table.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => Some(name.to_string()),
            _ => None,
        },
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
            };
            match &tables.first()?.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => Some(name.to_string()),
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::top_level_write_target_name;
    use crate::sql::ast::utils::parse_sql_statements;

    #[test]
    fn detects_top_level_write_targets() {
        let statements = parse_sql_statements(
            "UPDATE lix_file SET data = X'01' WHERE id = 'f1'; \
             DELETE FROM some_other_table WHERE id = 'x'",
        )
        .expect("parse");
        assert_eq!(
            top_level_write_target_name(&statements[0]).as_deref(),
            Some("lix_file")
        );

        let statements = parse_sql_statements(
            "INSERT INTO lix_directory_by_version (id, path, lixcol_version_id) VALUES ('d1', '/docs', 'v1')",
        )
        .expect("parse");
        assert_eq!(
            top_level_write_target_name(&statements[0]).as_deref(),
            Some("lix_directory_by_version")
        );

        let statements =
            parse_sql_statements("DELETE FROM lix_file_history WHERE id = 'f1'").expect("parse");
        assert_eq!(
            top_level_write_target_name(&statements[0]).as_deref(),
            Some("lix_file_history")
        );

        let statements =
            parse_sql_statements("SELECT * FROM lix_file WHERE id = 'f1'").expect("parse");
        assert_eq!(top_level_write_target_name(&statements[0]), None);
    }
}

pub(crate) fn derive_cache_targets(
    plan: &ExecutionPlan,
    active_effects: &PlanEffects,
    effects_are_authoritative: bool,
    postprocess_file_cache_targets: BTreeSet<(String, String)>,
) -> CacheTargets {
    let file_cache_refresh_targets =
        if effects_are_authoritative || plan.requirements.should_refresh_file_cache {
            let mut targets = active_effects.file_cache_refresh_targets.clone();
            targets.extend(postprocess_file_cache_targets.clone());
            targets
        } else {
            BTreeSet::new()
        };

    CacheTargets {
        file_cache_refresh_targets,
    }
}

pub(crate) async fn maybe_execute_public_write_with_backend(
    engine: &Engine,
    prepared: &PreparedExecutionContext,
    writer_key: Option<&str>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    let Some(public_write) = prepared.public_write.as_ref() else {
        return Ok(None);
    };
    if public_write.execution.is_none() {
        return Ok(None);
    }

    let mut transaction: Box<dyn LixTransaction> = engine.backend.begin_transaction().await?;
    let execution = match maybe_execute_public_write_with_transaction(
        engine,
        transaction.as_mut(),
        prepared,
        writer_key,
        None,
    )
    .await?
    {
        Some(execution) => execution,
        None => return Ok(None),
    };
    engine
        .persist_runtime_sequence_in_transaction(
            transaction.as_mut(),
            prepared.settings,
            prepared.sequence_start,
            &prepared.functions,
        )
        .await?;
    let should_emit_observe_tick = execution
        .plan_effects_override
        .as_ref()
        .map(|effects| !effects.state_commit_stream_changes.is_empty())
        .unwrap_or(false)
        || !execution.state_commit_stream_changes.is_empty();
    if should_emit_observe_tick {
        engine
            .append_observe_tick_in_transaction(transaction.as_mut(), writer_key)
            .await?;
    }
    transaction.commit().await?;
    Ok(Some(execution))
}

pub(crate) async fn maybe_execute_public_write_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    prepared: &PreparedExecutionContext,
    writer_key: Option<&str>,
    pending_append_session: Option<&mut Option<PendingPublicAppendSession>>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    let mut pending_append_session = pending_append_session;
    let Some(public_write) = prepared.public_write.as_ref() else {
        return Ok(None);
    };
    let Some(execution) = public_write.execution.as_ref() else {
        return Ok(None);
    };

    let mut combined_outcome = None;
    for partition in &execution.partitions {
        let outcome = match partition {
            PublicWriteExecutionPartition::Untracked(execution) => {
                if let Some(session_slot) = pending_append_session.as_mut() {
                    **session_slot = None;
                }
                execute_public_untracked_write_with_transaction(
                    engine,
                    transaction,
                    execution,
                    prepared,
                )
                .await?
            }
            PublicWriteExecutionPartition::Tracked(execution) => {
                execute_public_tracked_write_with_transaction(
                    engine,
                    transaction,
                    execution,
                    public_write,
                    prepared,
                    writer_key,
                    pending_append_session.as_deref_mut(),
                )
                .await?
            }
        };

        if let Some(outcome) = outcome {
            merge_public_write_execution_outcome(&mut combined_outcome, outcome);
        }
    }

    Ok(Some(
        combined_outcome.unwrap_or_else(empty_public_write_execution_outcome),
    ))
}

async fn execute_public_tracked_write_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    execution: &crate::sql::public::runtime::TrackedWriteExecution,
    public_write: &PreparedPublicWrite,
    prepared: &PreparedExecutionContext,
    writer_key: Option<&str>,
    mut pending_append_session: Option<&mut Option<PendingPublicAppendSession>>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    if execution.persist_filesystem_payloads_before_write {
        engine
            .persist_pending_file_data_updates_in_transaction(
                transaction,
                &prepared.intent.pending_file_writes,
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

    for registration in &execution.schema_registrations {
        for statement in
            register_schema_sql_statements(&registration.schema_key, transaction.dialect())
        {
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

    if execution.domain_change_batch.changes.is_empty() {
        return Ok(Some(empty_public_write_execution_outcome()));
    }

    let mut append_functions = prepared.functions.clone();
    let timestamp = append_functions.timestamp();
    if let Some(session_slot) = pending_append_session.as_mut() {
        let can_merge = session_slot.as_ref().is_some_and(|session| {
            pending_session_matches_append(session, &execution.append_preconditions)
        });
        if can_merge {
            let mut invariant_checker =
                Sql2AppendInvariantChecker::new(&public_write.planned_write);
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
                &execution.domain_change_batch,
                &mut append_functions,
                &timestamp,
            )
            .await?;

            let _ = writer_key;
            return Ok(Some(SqlExecutionOutcome {
                public_result: QueryResult {
                    rows: Vec::new(),
                    columns: Vec::new(),
                },
                postprocess_file_cache_targets: BTreeSet::new(),
                plugin_changes_committed: true,
                plan_effects_override: Some(execution.semantic_effects.clone()),
                state_commit_stream_changes: Vec::new(),
            }));
        }
    }

    let mut invariant_checker = Sql2AppendInvariantChecker::new(&public_write.planned_write);
    let append_result = append_commit_if_preconditions_hold(
        transaction,
        AppendCommitArgs {
            timestamp,
            changes: execution.domain_change_batch.changes.clone(),
            preconditions: execution.append_preconditions.clone(),
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
    if matches!(append_result.disposition, AppendCommitDisposition::Applied) {
        apply_public_version_last_checkpoint_side_effects(
            transaction,
            public_write,
            &execution.domain_change_batch,
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
                        execution.append_preconditions.write_lane.clone(),
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
        execution.semantic_effects.clone()
    } else {
        PlanEffects::default()
    };

    let _ = writer_key;
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

fn empty_public_write_execution_outcome() -> SqlExecutionOutcome {
    SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        postprocess_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        plan_effects_override: Some(PlanEffects::default()),
        state_commit_stream_changes: Vec::new(),
    }
}

fn merge_public_write_execution_outcome(
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

fn pending_session_matches_append(
    session: &PendingPublicAppendSession,
    preconditions: &AppendCommitPreconditions,
) -> bool {
    session.lane == preconditions.write_lane
        && match &preconditions.expected_tip {
            AppendExpectedTip::CurrentTip => true,
            AppendExpectedTip::CommitId(commit_id) => commit_id == &session.commit_id,
            AppendExpectedTip::CreateIfMissing => false,
        }
}

async fn build_pending_public_append_session(
    transaction: &mut dyn LixTransaction,
    lane: AppendWriteLane,
    commit_result: &GenerateCommitResult,
) -> Result<PendingPublicAppendSession, LixError> {
    let commit_row = commit_result
        .live_state_rows
        .iter()
        .find(|row| row.schema_key == "lix_commit")
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public append session requires a lix_commit materialized row",
            )
        })?;
    let commit_snapshot = commit_row.snapshot_content.as_deref().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public append session requires commit snapshot_content",
        )
    })?;
    let commit_snapshot: JsonValue = serde_json::from_str(commit_snapshot).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("public append session commit snapshot is invalid JSON: {error}"),
        )
    })?;
    let change_set_id = commit_snapshot
        .get("change_set_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public append session commit snapshot is missing change_set_id",
            )
        })?
        .to_string();
    let commit_change_id = commit_result
        .changes
        .iter()
        .find(|row| row.schema_key == "lix_commit" && row.entity_id == commit_row.entity_id)
        .map(|row| row.id.clone())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public append session requires a lix_commit change row",
            )
        })?;
    let snapshot_id_result = transaction
        .execute(
            "SELECT snapshot_id \
             FROM lix_internal_change \
             WHERE id = $1 \
               AND schema_key = 'lix_commit' \
               AND entity_id = $2 \
             LIMIT 1",
            &[
                Value::Text(commit_change_id.clone()),
                Value::Text(commit_row.entity_id.clone()),
            ],
        )
        .await?;
    let commit_change_snapshot_id = snapshot_id_result
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
                "public append session could not load commit snapshot_id",
            )
        })?;

    Ok(PendingPublicAppendSession {
        lane,
        commit_id: commit_row.entity_id.clone(),
        change_set_id,
        commit_change_id,
        commit_change_snapshot_id,
        commit_materialized_change_id: commit_row.id.clone(),
        commit_schema_version: commit_row.schema_version.clone(),
        commit_file_id: commit_row.file_id.clone(),
        commit_plugin_key: commit_row.plugin_key.clone(),
        commit_snapshot,
    })
}

async fn merge_public_domain_change_batch_into_pending_commit(
    transaction: &mut dyn LixTransaction,
    session: &mut PendingPublicAppendSession,
    batch: &crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
    timestamp: &str,
) -> Result<(), LixError> {
    let domain_changes = batch
        .changes
        .iter()
        .map(|change| {
            Ok::<DomainChangeInput, LixError>(DomainChangeInput {
                id: functions.uuid_v7(),
                entity_id: change.entity_id.clone(),
                schema_key: change.schema_key.clone(),
                schema_version: change.schema_version.clone().ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "public merge requires schema_version for '{}:{}'",
                            change.schema_key, change.entity_id
                        ),
                    )
                })?,
                file_id: change.file_id.clone().ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "public merge requires file_id for '{}:{}'",
                            change.schema_key, change.entity_id
                        ),
                    )
                })?,
                plugin_key: change.plugin_key.clone().ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "public merge requires plugin_key for '{}:{}'",
                            change.schema_key, change.entity_id
                        ),
                    )
                })?,
                snapshot_content: change.snapshot_content.clone(),
                metadata: change.metadata.clone(),
                created_at: timestamp.to_string(),
                version_id: change.version_id.clone(),
                writer_key: change.writer_key.clone(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let active_accounts = {
        let mut executor = TransactionCommitExecutor { transaction };
        load_commit_active_accounts(&mut executor, &domain_changes).await?
    };
    let versions = load_version_info_for_domain_changes(transaction, &domain_changes).await?;
    let generated = generate_commit(
        GenerateCommitArgs {
            timestamp: timestamp.to_string(),
            active_accounts: active_accounts.clone(),
            changes: domain_changes.clone(),
            versions,
        },
        || functions.uuid_v7(),
    )?;

    extend_json_array_strings(
        &mut session.commit_snapshot,
        "change_ids",
        domain_changes.iter().map(|change| change.id.clone()),
    );
    extend_json_array_strings(
        &mut session.commit_snapshot,
        "author_account_ids",
        active_accounts.iter().cloned(),
    );

    transaction
        .execute(
            "UPDATE lix_internal_snapshot \
             SET content = $1 \
             WHERE id = $2",
            &[
                Value::Text(session.commit_snapshot.to_string()),
                Value::Text(session.commit_change_snapshot_id.clone()),
            ],
        )
        .await?;

    let rewritten = rewrite_generated_commit_result_for_pending_session(
        session,
        generated,
        domain_changes.len(),
        timestamp,
    )?;
    execute_generated_commit_result(transaction, rewritten, functions).await
}

async fn load_version_info_for_domain_changes(
    transaction: &mut dyn LixTransaction,
    domain_changes: &[DomainChangeInput],
) -> Result<BTreeMap<String, VersionInfo>, LixError> {
    let affected_versions = domain_changes
        .iter()
        .map(|change| change.version_id.clone())
        .collect::<BTreeSet<_>>();
    let mut executor = TransactionCommitExecutor { transaction };
    load_version_info_for_versions(&mut executor, &affected_versions).await
}

fn rewrite_generated_commit_result_for_pending_session(
    session: &PendingPublicAppendSession,
    generated: GenerateCommitResult,
    domain_change_count: usize,
    timestamp: &str,
) -> Result<GenerateCommitResult, LixError> {
    let temporary_commit_id = generated
        .live_state_rows
        .iter()
        .find(|row| row.schema_key == "lix_commit")
        .map(|row| row.entity_id.clone())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public merge rewrite requires a generated lix_commit row",
            )
        })?;
    let temporary_change_set_id = generated
        .live_state_rows
        .iter()
        .find(|row| row.schema_key == "lix_change_set")
        .map(|row| row.entity_id.clone())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public merge rewrite requires a generated lix_change_set row",
            )
        })?;
    let version_pointer_entity_id = pending_session_version_pointer_entity_id(&session.lane);

    let mut live_state_rows = Vec::new();
    for mut row in generated.live_state_rows {
        if is_pending_commit_meta_row(
            &row,
            &temporary_commit_id,
            &temporary_change_set_id,
            version_pointer_entity_id,
        )? {
            continue;
        }

        match row.schema_key.as_str() {
            "lix_change_set_element" => {
                let (entity_id, snapshot_content) = rewrite_change_set_element_snapshot(
                    row.snapshot_content.as_deref(),
                    &session.change_set_id,
                )?;
                row.entity_id = entity_id;
                row.snapshot_content = Some(snapshot_content);
                row.lixcol_commit_id = session.commit_id.clone();
            }
            "lix_change_author" => {
                row.id = session.commit_change_id.clone();
                row.lixcol_commit_id = session.commit_id.clone();
            }
            _ => {
                row.lixcol_commit_id = session.commit_id.clone();
            }
        }
        live_state_rows.push(row);
    }

    live_state_rows.push(MaterializedStateRow {
        id: session.commit_materialized_change_id.clone(),
        entity_id: session.commit_id.clone(),
        schema_key: "lix_commit".to_string(),
        schema_version: session.commit_schema_version.clone(),
        file_id: session.commit_file_id.clone(),
        plugin_key: session.commit_plugin_key.clone(),
        snapshot_content: Some(session.commit_snapshot.to_string()),
        metadata: None,
        created_at: timestamp.to_string(),
        lixcol_version_id: GLOBAL_VERSION_ID.to_string(),
        lixcol_commit_id: session.commit_id.clone(),
        writer_key: None,
    });

    Ok(GenerateCommitResult {
        changes: generated
            .changes
            .into_iter()
            .take(domain_change_count)
            .collect(),
        live_state_rows,
    })
}

fn is_pending_commit_meta_row(
    row: &MaterializedStateRow,
    temporary_commit_id: &str,
    temporary_change_set_id: &str,
    version_pointer_entity_id: &str,
) -> Result<bool, LixError> {
    match row.schema_key.as_str() {
        "lix_change_set" => Ok(row.entity_id == temporary_change_set_id),
        "lix_commit" => Ok(row.entity_id == temporary_commit_id),
        "lix_commit_edge" => Ok(row.entity_id.ends_with(&format!("~{temporary_commit_id}"))),
        "lix_version_pointer" if row.entity_id == version_pointer_entity_id => {
            let snapshot = row.snapshot_content.as_deref().unwrap_or("");
            let parsed: JsonValue = serde_json::from_str(snapshot).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("public merge rewrite saw invalid version pointer JSON: {error}"),
                )
            })?;
            Ok(parsed
                .get("commit_id")
                .and_then(JsonValue::as_str)
                .is_some_and(|value| value == temporary_commit_id))
        }
        _ => Ok(false),
    }
}

fn rewrite_change_set_element_snapshot(
    snapshot: Option<&str>,
    change_set_id: &str,
) -> Result<(String, String), LixError> {
    let snapshot = snapshot.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public merge rewrite requires change_set_element snapshot_content",
        )
    })?;
    let mut parsed: JsonValue = serde_json::from_str(snapshot).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("public merge rewrite saw invalid change_set_element JSON: {error}"),
        )
    })?;
    let change_id = parsed
        .get("change_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public merge rewrite requires change_set_element change_id",
            )
        })?
        .to_string();
    parsed["change_set_id"] = JsonValue::String(change_set_id.to_string());
    Ok((format!("{change_set_id}~{change_id}"), parsed.to_string()))
}

fn pending_session_version_pointer_entity_id(lane: &AppendWriteLane) -> &str {
    match lane {
        AppendWriteLane::Version(version_id) => version_id.as_str(),
        AppendWriteLane::GlobalAdmin => GLOBAL_VERSION_ID,
    }
}

fn extend_json_array_strings<I>(snapshot: &mut JsonValue, key: &str, values: I)
where
    I: IntoIterator<Item = String>,
{
    if !snapshot.is_object() {
        *snapshot = json!({});
    }
    let JsonValue::Object(map) = snapshot else {
        return;
    };
    let entry = map
        .entry(key.to_string())
        .or_insert_with(|| JsonValue::Array(Vec::new()));
    if !entry.is_array() {
        *entry = JsonValue::Array(Vec::new());
    }
    let JsonValue::Array(array) = entry else {
        return;
    };
    let mut seen = array
        .iter()
        .filter_map(|value| value.as_str().map(ToString::to_string))
        .collect::<BTreeSet<_>>();
    for value in values {
        if seen.insert(value.clone()) {
            array.push(JsonValue::String(value));
        }
    }
}

async fn execute_generated_commit_result(
    transaction: &mut dyn LixTransaction,
    result: GenerateCommitResult,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<(), LixError> {
    let prepared = bind_statement_batch_for_dialect(
        build_statement_batch_from_generate_commit_result(
            result,
            functions,
            0,
            transaction.dialect(),
        )?,
        transaction.dialect(),
    )?;
    let mut program = WriteProgram::new();
    program.push_batch(prepared);
    execute_write_program_with_transaction(transaction, program).await?;
    Ok(())
}

async fn execute_public_untracked_write_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    execution: &crate::sql::public::runtime::UntrackedWriteExecution,
    prepared: &PreparedExecutionContext,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    let mut runtime_functions = prepared.functions.clone();
    let timestamp = runtime_functions.timestamp();
    if execution.persist_filesystem_payloads_before_write {
        engine
            .persist_pending_file_data_updates_in_transaction(
                transaction,
                &prepared.intent.pending_file_writes,
            )
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "public untracked filesystem payload persistence failed before state apply: {}",
                    error.description
                ),
            })?;
    }
    apply_public_untracked_rows(transaction, &execution.intended_post_state, &timestamp).await?;

    Ok(Some(SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        postprocess_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        plan_effects_override: Some(execution.semantic_effects.clone()),
        state_commit_stream_changes: Vec::new(),
    }))
}

pub(crate) fn public_write_filesystem_payload_changes_already_committed(
    prepared: &PreparedExecutionContext,
) -> bool {
    let Some(public_write) = prepared.public_write.as_ref() else {
        return false;
    };
    public_write.execution.as_ref().is_some_and(|execution| {
        execution.partitions.iter().any(|partition| {
            matches!(
                partition,
                PublicWriteExecutionPartition::Tracked(execution)
                    if execution.filesystem_payload_changes_committed_by_write
            )
        })
    })
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

    let sql = format!(
        "INSERT INTO {table} (\
         entity_id, schema_key, file_id, version_id, global, plugin_key, snapshot_content, metadata, writer_key, schema_version, created_at, updated_at\
         ) VALUES (\
         '{entity_id}', '{schema_key}', '{file_id}', '{version_id}', {global}, '{plugin_key}', '{snapshot_content}', {metadata}, {writer_key}, '{schema_version}', '{timestamp}', '{timestamp}'\
         ) ON CONFLICT (entity_id, schema_key, file_id, version_id) DO UPDATE SET \
         global = excluded.global, \
         plugin_key = excluded.plugin_key, \
         snapshot_content = excluded.snapshot_content, \
         metadata = excluded.metadata, \
         writer_key = excluded.writer_key, \
         schema_version = excluded.schema_version, \
         updated_at = excluded.updated_at",
        table = UNTRACKED_TABLE,
        entity_id = escape_sql_string(&row.entity_id),
        schema_key = escape_sql_string(&row.schema_key),
        file_id = escape_sql_string(file_id),
        version_id = escape_sql_string(row.version_id.as_deref().unwrap_or(GLOBAL_VERSION_ID)),
        global = if global { "true" } else { "false" },
        plugin_key = escape_sql_string(plugin_key),
        snapshot_content = escape_sql_string(&snapshot_content),
        metadata = metadata_sql,
        writer_key = writer_key_sql,
        schema_version = escape_sql_string(schema_version),
        timestamp = escape_sql_string(timestamp),
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

async fn apply_public_untracked_delete(
    transaction: &mut dyn LixTransaction,
    row: &crate::sql::public::planner::ir::PlannedStateRow,
) -> Result<(), LixError> {
    let file_id = planned_row_text_value(row, "file_id")?;
    let sql = format!(
        "DELETE FROM {table} \
         WHERE entity_id = '{entity_id}' \
           AND schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}'",
        table = UNTRACKED_TABLE,
        entity_id = escape_sql_string(&row.entity_id),
        schema_key = escape_sql_string(&row.schema_key),
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

fn planned_row_json_text_value(
    row: &crate::sql::public::planner::ir::PlannedStateRow,
    key: &str,
) -> Result<String, LixError> {
    planned_row_optional_json_text_value(row, key)
        .map(|value| value.into_owned())
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("public untracked execution requires '{key}' in the resolved row"),
        })
}

fn planned_row_optional_text_value<'a>(
    row: &'a crate::sql::public::planner::ir::PlannedStateRow,
    key: &str,
) -> Option<&'a str> {
    match row.values.get(key) {
        Some(Value::Text(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn planned_row_optional_json_text_value<'a>(
    row: &'a crate::sql::public::planner::ir::PlannedStateRow,
    key: &str,
) -> Option<std::borrow::Cow<'a, str>> {
    match row.values.get(key) {
        Some(Value::Text(value)) => Some(std::borrow::Cow::Borrowed(value.as_str())),
        Some(Value::Json(value)) => Some(std::borrow::Cow::Owned(value.to_string())),
        _ => None,
    }
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(value) => Some(*value),
        Value::Integer(value) => Some(*value != 0),
        Value::Text(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Some(true),
            "0" | "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn append_commit_error_to_lix_error(error: crate::state::commit::AppendCommitError) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.message,
    }
}

async fn mirror_public_stored_schema_bootstrap_rows(
    transaction: &mut dyn LixTransaction,
    commit_result: &crate::state::commit::GenerateCommitResult,
) -> Result<(), LixError> {
    for row in &commit_result.live_state_rows {
        if row.schema_key != STORED_SCHEMA_KEY || row.lixcol_version_id != GLOBAL_VERSION_ID {
            continue;
        }

        let snapshot_sql = row
            .snapshot_content
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let metadata_sql = row
            .metadata
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let writer_key_sql = row
            .writer_key
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let is_tombstone = if row.snapshot_content.is_some() { 0 } else { 1 };

        let sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, snapshot_content, change_id, metadata, writer_key, is_tombstone, created_at, updated_at\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', true, '{plugin_key}', {snapshot_content}, '{change_id}', {metadata}, {writer_key}, {is_tombstone}, '{created_at}', '{updated_at}'\
             ) ON CONFLICT (entity_id, file_id, version_id) DO UPDATE SET \
             schema_key = excluded.schema_key, \
             schema_version = excluded.schema_version, \
             global = excluded.global, \
             plugin_key = excluded.plugin_key, \
             snapshot_content = excluded.snapshot_content, \
             change_id = excluded.change_id, \
             metadata = excluded.metadata, \
             writer_key = excluded.writer_key, \
             is_tombstone = excluded.is_tombstone, \
             updated_at = excluded.updated_at",
            table = STORED_SCHEMA_BOOTSTRAP_TABLE,
            entity_id = escape_sql_string(&row.entity_id),
            schema_key = escape_sql_string(&row.schema_key),
            schema_version = escape_sql_string(&row.schema_version),
            file_id = escape_sql_string(&row.file_id),
            version_id = escape_sql_string(&row.lixcol_version_id),
            plugin_key = escape_sql_string(&row.plugin_key),
            snapshot_content = snapshot_sql,
            change_id = escape_sql_string(&row.id),
            metadata = metadata_sql,
            writer_key = writer_key_sql,
            is_tombstone = is_tombstone,
            created_at = escape_sql_string(&row.created_at),
            updated_at = escape_sql_string(&row.created_at),
        );

        transaction.execute(&sql, &[]).await?;
    }

    Ok(())
}

async fn apply_public_version_last_checkpoint_side_effects(
    transaction: &mut dyn LixTransaction,
    public_write: &PreparedPublicWrite,
    batch: &crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch,
) -> Result<(), LixError> {
    if public_write
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        != "lix_version"
    {
        return Ok(());
    }

    match public_write.planned_write.command.operation_kind {
        crate::sql::public::planner::ir::WriteOperationKind::Insert => {
            upsert_last_checkpoint_rows(transaction, &version_checkpoint_rows(batch), true).await
        }
        crate::sql::public::planner::ir::WriteOperationKind::Update => {
            upsert_last_checkpoint_rows(transaction, &version_checkpoint_rows(batch), false).await
        }
        crate::sql::public::planner::ir::WriteOperationKind::Delete => {
            let version_ids = batch
                .changes
                .iter()
                .map(|change| change.entity_id.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            delete_last_checkpoint_rows(transaction, &version_ids).await
        }
    }
}

fn version_checkpoint_rows(
    batch: &crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch,
) -> Vec<(String, String)> {
    batch
        .changes
        .iter()
        .filter(|change| change.schema_key == crate::version::version_pointer_schema_key())
        .filter_map(|change| {
            change.snapshot_content.as_deref().and_then(|snapshot| {
                serde_json::from_str::<serde_json::Value>(snapshot)
                    .ok()
                    .and_then(|snapshot| {
                        snapshot
                            .get("commit_id")
                            .and_then(serde_json::Value::as_str)
                            .map(|commit_id| (change.entity_id.clone(), commit_id.to_string()))
                    })
            })
        })
        .collect()
}

async fn upsert_last_checkpoint_rows(
    transaction: &mut dyn LixTransaction,
    rows: &[(String, String)],
    update_existing: bool,
) -> Result<(), LixError> {
    if rows.is_empty() {
        return Ok(());
    }

    let values_sql = rows
        .iter()
        .map(|(version_id, checkpoint_commit_id)| {
            format!(
                "('{}', '{}')",
                escape_sql_string(version_id),
                escape_sql_string(checkpoint_commit_id)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let on_conflict = if update_existing {
        "DO UPDATE SET checkpoint_commit_id = excluded.checkpoint_commit_id"
    } else {
        "DO NOTHING"
    };
    let sql = format!(
        "INSERT INTO lix_internal_last_checkpoint (version_id, checkpoint_commit_id) \
         VALUES {values_sql} \
         ON CONFLICT (version_id) {on_conflict}"
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

async fn delete_last_checkpoint_rows(
    transaction: &mut dyn LixTransaction,
    version_ids: &[String],
) -> Result<(), LixError> {
    if version_ids.is_empty() {
        return Ok(());
    }

    let in_list = version_ids
        .iter()
        .map(|id| format!("'{}'", escape_sql_string(id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("DELETE FROM lix_internal_last_checkpoint WHERE version_id IN ({in_list})");
    transaction.execute(&sql, &[]).await?;
    Ok(())
}
