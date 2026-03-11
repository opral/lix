use std::collections::{BTreeMap, BTreeSet};

use crate::commit::{
    append_commit_if_preconditions_hold, bind_statement_batch_for_dialect,
    build_statement_batch_from_generate_commit_result, generate_commit,
    load_commit_active_accounts, load_version_info_for_versions, AppendCommitArgs,
    AppendCommitDisposition, AppendCommitError, AppendCommitErrorKind,
    AppendCommitInvariantChecker, AppendCommitPreconditions, AppendExpectedTip, AppendWriteLane,
    CommitQueryExecutor, DomainChangeInput, GenerateCommitArgs, GenerateCommitResult,
    MaterializedStateRow, VersionInfo,
};
use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::engine::query_storage::sql_text::escape_sql_string;
use crate::engine::{Engine, TransactionBackendAdapter};
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::schema::schema_from_stored_snapshot;
use crate::schema_registry::register_schema_sql_statements;
use crate::sql2::catalog::{SurfaceCapability, SurfaceRegistry};
use crate::sql2::runtime::{prepare_sql2_read, try_prepare_sql2_write, Sql2PreparedWrite};
use crate::state_commit_stream::{
    state_commit_stream_changes_from_domain_changes, state_commit_stream_changes_from_planned_rows,
    StateCommitStreamOperation,
};
use crate::validation::{
    validate_inserts, validate_sql2_append_time_write, validate_sql2_batch_local_write,
    validate_updates,
};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    parse_active_version_snapshot,
};
use crate::{LixBackend, LixError, LixTransaction, QueryResult, Value};

use crate::query_runtime::contracts::effects::PlanEffects;
use crate::query_runtime::contracts::execution_plan::ExecutionPlan;
use crate::query_runtime::contracts::planned_statement::{PlannedStatementSet, SchemaRegistration};
use crate::query_runtime::contracts::requirements::PlanRequirements;
use crate::query_runtime::contracts::result_contract::ResultContract;
use crate::query_runtime::derive_requirements::derive_plan_requirements;
use crate::query_runtime::execute::SqlExecutionOutcome;
use crate::query_runtime::intent::{
    authoritative_pending_file_write_targets, collect_execution_intent_with_backend,
    ExecutionIntent, IntentCollectionPolicy,
};
use crate::query_runtime::plan::build_execution_plan;
use serde_json::{json, Value as JsonValue};
use sqlparser::ast::Statement;

const STORED_SCHEMA_KEY: &str = "lix_stored_schema";
const STORED_SCHEMA_BOOTSTRAP_TABLE: &str = "lix_internal_stored_schema_bootstrap";
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
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
    pub(crate) sql2_write: Option<Sql2PreparedWrite>,
}

pub(crate) struct CacheTargets {
    pub(crate) file_cache_refresh_targets: BTreeSet<(String, String)>,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingSql2AppendSession {
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

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

struct Sql2AppendInvariantChecker<'a> {
    planned_write: &'a crate::sql2::planner::ir::PlannedWrite,
    schema_cache: crate::validation::SchemaCache,
}

impl<'a> Sql2AppendInvariantChecker<'a> {
    fn new(planned_write: &'a crate::sql2::planner::ir::PlannedWrite) -> Self {
        Self {
            planned_write,
            schema_cache: crate::validation::SchemaCache::new(),
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

    let sql2_read =
        prepare_sql2_read(backend, &statements, params, active_version_id, writer_key).await;
    let sql2_write =
        try_prepare_sql2_write(backend, &statements, params, active_version_id, writer_key)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "prepare_execution_with_backend sql2 write preparation failed: {}",
                    error.description
                ),
            })?;
    if let Some(target_name) =
        migrated_public_write_target_name_with_backend(backend, &statements).await?
    {
        if sql2_write.is_none() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("public write target '{target_name}' must route through sql2"),
            });
        }
    }
    let plan_statements = sql2_read
        .as_ref()
        .and_then(|prepared| prepared.lowered_read.as_ref())
        .map(|program| program.statements.clone())
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

    let sql2_write_owned_execution = sql2_write.is_some();
    let plan = if sql2_write_owned_execution {
        passthrough_execution_plan_for_sql2_write(
            &statements,
            sql2_write
                .as_ref()
                .map(sql2_schema_registrations)
                .unwrap_or_default(),
        )
    } else {
        build_execution_plan(
            backend,
            &engine.cel_evaluator,
            plan_statements,
            params,
            sql2_read
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

    if !sql2_write_owned_execution && !plan.preprocess.mutations.is_empty() {
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
    if !sql2_write_owned_execution && !plan.preprocess.update_validations.is_empty() {
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
    if let Some(sql2_write) = sql2_write.as_ref() {
        validate_sql2_batch_local_write(backend, &engine.schema_cache, &sql2_write.planned_write)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "prepare_execution_with_backend sql2 batch-local validation failed: {}",
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
        sql2_write,
    })
}

fn passthrough_execution_plan_for_sql2_write(
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
        dependency_spec: crate::sql_shared::dependency_spec::DependencySpec::default(),
        effects: PlanEffects::default(),
    }
}

fn sql2_schema_registrations(sql2_write: &Sql2PreparedWrite) -> Vec<SchemaRegistration> {
    let mut schema_keys = BTreeSet::new();
    if let Some(resolved) = sql2_write.planned_write.resolved_write_plan.as_ref() {
        for row in &resolved.intended_post_state {
            if row.schema_key != STORED_SCHEMA_KEY {
                schema_keys.insert(row.schema_key.clone());
            }

            if row.schema_key != STORED_SCHEMA_KEY || row.tombstone {
                continue;
            }

            let Some(Value::Text(snapshot_content)) = row.values.get("snapshot_content") else {
                continue;
            };
            let Ok(snapshot) = serde_json::from_str(snapshot_content) else {
                continue;
            };
            let Ok((schema_key, _)) = schema_from_stored_snapshot(&snapshot) else {
                continue;
            };
            schema_keys.insert(schema_key.schema_key);
        }
    }

    schema_keys
        .into_iter()
        .map(|schema_key| SchemaRegistration { schema_key })
        .collect()
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

async fn migrated_public_write_target_name_with_backend(
    backend: &dyn LixBackend,
    statements: &[Statement],
) -> Result<Option<String>, LixError> {
    if statements.len() != 1 {
        return Ok(None);
    }
    if statement_is_multi_row_insert(&statements[0]) {
        return Ok(None);
    }
    let registry = SurfaceRegistry::bootstrap_with_backend(backend).await?;

    Ok(statements.iter().find_map(|statement| {
        let target_name = top_level_write_target_name(statement)?;
        let binding = registry.bind_relation_name(&target_name)?;
        (binding.capability == SurfaceCapability::ReadWrite
            && binding.descriptor.surface_family != crate::sql2::catalog::SurfaceFamily::Entity
            && binding.resolution_capabilities.semantic_write)
            .then_some(binding.descriptor.public_name)
    }))
}

fn statement_is_multi_row_insert(statement: &Statement) -> bool {
    let Statement::Insert(insert) = statement else {
        return false;
    };
    let Some(source) = insert.source.as_ref() else {
        return false;
    };
    let sqlparser::ast::SetExpr::Values(values) = source.body.as_ref() else {
        return false;
    };
    values.rows.len() > 1
}

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
    use crate::engine::sql_ast::utils::parse_sql_statements;

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

pub(crate) async fn maybe_execute_sql2_write_with_backend(
    engine: &Engine,
    prepared: &PreparedExecutionContext,
    writer_key: Option<&str>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    if !sql2_tracked_write_is_live(prepared) && !sql2_untracked_write_is_live(prepared) {
        return Ok(None);
    }

    let mut transaction: Box<dyn LixTransaction> = engine.backend.begin_transaction().await?;
    let execution = match maybe_execute_sql2_write_with_transaction(
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
    transaction.commit().await?;
    Ok(Some(execution))
}

pub(crate) async fn maybe_execute_sql2_write_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    prepared: &PreparedExecutionContext,
    writer_key: Option<&str>,
    pending_append_session: Option<&mut Option<PendingSql2AppendSession>>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    let mut pending_append_session = pending_append_session;
    if sql2_untracked_write_is_live(prepared) {
        if let Some(session_slot) = pending_append_session.as_mut() {
            **session_slot = None;
        }
        return execute_sql2_untracked_write_with_transaction(engine, transaction, prepared).await;
    }

    if !sql2_tracked_write_is_live(prepared) {
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
    if sql2_commits_filesystem_payload_domain_changes(prepared) {
        engine
            .persist_pending_file_data_updates_in_transaction(
                transaction,
                &prepared.intent.pending_file_writes,
            )
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "sql2 tracked filesystem payload persistence failed before append: {}",
                    error.description
                ),
            })?;
    }
    let stream_operation = state_commit_stream_operation(sql2_write);

    for registration in &prepared.plan.preprocess.registrations {
        for statement in
            register_schema_sql_statements(&registration.schema_key, transaction.dialect())
        {
            transaction
                .execute(&statement, &[])
                .await
                .map_err(|error| LixError {
                    code: error.code,
                    description: format!(
                        "sql2 tracked write schema registration failed for '{}': {}",
                        registration.schema_key, error.description
                    ),
                })?;
        }
    }

    if domain_change_batch.changes.is_empty() {
        return Ok(Some(SqlExecutionOutcome {
            public_result: QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            },
            postprocess_file_cache_targets: BTreeSet::new(),
            plugin_changes_committed: false,
            plan_effects_override: Some(PlanEffects::default()),
            state_commit_stream_changes: Vec::new(),
        }));
    }

    let mut append_functions = prepared.functions.clone();
    let timestamp = append_functions.timestamp();
    let append_preconditions =
        append_preconditions(sql2_write, domain_change_batch, commit_preconditions)?;
    if let Some(session_slot) = pending_append_session.as_mut() {
        let session_slot = &mut **session_slot;
        let can_merge = session_slot
            .as_ref()
            .is_some_and(|session| pending_session_matches_append(session, &append_preconditions));
        if can_merge {
            let mut invariant_checker = Sql2AppendInvariantChecker::new(&sql2_write.planned_write);
            invariant_checker
                .recheck_invariants(transaction)
                .await
                .map_err(append_commit_error_to_lix_error)?;
            let session = session_slot
                .as_mut()
                .expect("session should exist when can_merge is true");
            merge_sql2_domain_change_batch_into_pending_commit(
                transaction,
                session,
                domain_change_batch,
                &mut append_functions,
                &timestamp,
            )
            .await?;

            let plan_effects_override = semantic_plan_effects_from_domain_changes(
                &domain_change_batch.changes,
                stream_operation,
            )?;

            let _ = writer_key;
            return Ok(Some(SqlExecutionOutcome {
                public_result: QueryResult {
                    rows: Vec::new(),
                    columns: Vec::new(),
                },
                postprocess_file_cache_targets: BTreeSet::new(),
                plugin_changes_committed: true,
                plan_effects_override: Some(plan_effects_override),
                state_commit_stream_changes: Vec::new(),
            }));
        }
    }

    let mut invariant_checker = Sql2AppendInvariantChecker::new(&sql2_write.planned_write);
    let append_result = append_commit_if_preconditions_hold(
        transaction,
        AppendCommitArgs {
            timestamp,
            changes: domain_change_batch.changes.clone(),
            preconditions: append_preconditions.clone(),
        },
        &mut append_functions,
        Some(&mut invariant_checker),
    )
    .await
    .map_err(append_commit_error_to_lix_error)?;

    if let Some(commit_result) = append_result.commit_result.as_ref() {
        mirror_sql2_stored_schema_bootstrap_rows(transaction, commit_result)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "sql2 tracked write stored-schema bootstrap mirroring failed: {}",
                    error.description
                ),
            })?;
    }
    if matches!(append_result.disposition, AppendCommitDisposition::Applied) {
        apply_sql2_version_last_checkpoint_side_effects(transaction, sql2_write)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "sql2 tracked write version checkpoint side effects failed: {}",
                    error.description
                ),
            })?;
    }

    let plugin_changes_committed =
        matches!(append_result.disposition, AppendCommitDisposition::Applied);
    if let Some(session_slot) = pending_append_session.as_mut() {
        let session_slot = &mut **session_slot;
        *session_slot = if plugin_changes_committed {
            if let Some(commit_result) = append_result.commit_result.as_ref() {
                Some(
                    build_pending_sql2_append_session(
                        transaction,
                        append_preconditions.write_lane.clone(),
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
        semantic_plan_effects_from_domain_changes(&domain_change_batch.changes, stream_operation)?
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

fn pending_session_matches_append(
    session: &PendingSql2AppendSession,
    preconditions: &AppendCommitPreconditions,
) -> bool {
    session.lane == preconditions.write_lane
        && matches!(
            &preconditions.expected_tip,
            AppendExpectedTip::CommitId(commit_id) if commit_id == &session.commit_id
        )
}

async fn build_pending_sql2_append_session(
    transaction: &mut dyn LixTransaction,
    lane: AppendWriteLane,
    commit_result: &GenerateCommitResult,
) -> Result<PendingSql2AppendSession, LixError> {
    let commit_row = commit_result
        .materialized_state
        .iter()
        .find(|row| row.schema_key == "lix_commit")
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "sql2 append session requires a lix_commit materialized row",
            )
        })?;
    let commit_snapshot = commit_row.snapshot_content.as_deref().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "sql2 append session requires commit snapshot_content",
        )
    })?;
    let commit_snapshot: JsonValue = serde_json::from_str(commit_snapshot).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 append session commit snapshot is invalid JSON: {error}"),
        )
    })?;
    let change_set_id = commit_snapshot
        .get("change_set_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "sql2 append session commit snapshot is missing change_set_id",
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
                "sql2 append session requires a lix_commit change row",
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
                "sql2 append session could not load commit snapshot_id",
            )
        })?;

    Ok(PendingSql2AppendSession {
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

async fn merge_sql2_domain_change_batch_into_pending_commit(
    transaction: &mut dyn LixTransaction,
    session: &mut PendingSql2AppendSession,
    batch: &crate::sql2::planner::semantics::domain_changes::DomainChangeBatch,
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
                            "sql2 merge requires schema_version for '{}:{}'",
                            change.schema_key, change.entity_id
                        ),
                    )
                })?,
                file_id: change.file_id.clone().ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "sql2 merge requires file_id for '{}:{}'",
                            change.schema_key, change.entity_id
                        ),
                    )
                })?,
                plugin_key: change.plugin_key.clone().ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "sql2 merge requires plugin_key for '{}:{}'",
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
    session: &PendingSql2AppendSession,
    generated: GenerateCommitResult,
    domain_change_count: usize,
    timestamp: &str,
) -> Result<GenerateCommitResult, LixError> {
    let temporary_commit_id = generated
        .materialized_state
        .iter()
        .find(|row| row.schema_key == "lix_commit")
        .map(|row| row.entity_id.clone())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "sql2 merge rewrite requires a generated lix_commit row",
            )
        })?;
    let temporary_change_set_id = generated
        .materialized_state
        .iter()
        .find(|row| row.schema_key == "lix_change_set")
        .map(|row| row.entity_id.clone())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "sql2 merge rewrite requires a generated lix_change_set row",
            )
        })?;
    let version_pointer_entity_id = pending_session_version_pointer_entity_id(&session.lane);

    let mut materialized_state = Vec::new();
    for mut row in generated.materialized_state {
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
        materialized_state.push(row);
    }

    materialized_state.push(MaterializedStateRow {
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
        materialized_state,
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
                    format!("sql2 merge rewrite saw invalid version pointer JSON: {error}"),
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
            "sql2 merge rewrite requires change_set_element snapshot_content",
        )
    })?;
    let mut parsed: JsonValue = serde_json::from_str(snapshot).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 merge rewrite saw invalid change_set_element JSON: {error}"),
        )
    })?;
    let change_id = parsed
        .get("change_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "sql2 merge rewrite requires change_set_element change_id",
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
    for statement in prepared {
        transaction
            .execute(&statement.sql, &statement.params)
            .await?;
    }
    Ok(())
}

async fn execute_sql2_untracked_write_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    prepared: &PreparedExecutionContext,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    let Some(sql2_write) = prepared.sql2_write.as_ref() else {
        return Ok(None);
    };
    let Some(resolved) = sql2_write.planned_write.resolved_write_plan.as_ref() else {
        return Ok(None);
    };

    let mut runtime_functions = prepared.functions.clone();
    let timestamp = runtime_functions.timestamp();
    if sql2_persists_filesystem_payload_writes(prepared) {
        engine
            .persist_pending_file_data_updates_in_transaction(
                transaction,
                &prepared.intent.pending_file_writes,
            )
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "sql2 untracked filesystem payload persistence failed before state apply: {}",
                    error.description
                ),
            })?;
    }
    apply_sql2_untracked_rows(transaction, &resolved.intended_post_state, &timestamp).await?;
    let plan_effects_override =
        semantic_plan_effects_from_untracked_sql2_write(prepared, sql2_write)?;

    Ok(Some(SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        postprocess_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        plan_effects_override: Some(plan_effects_override),
        state_commit_stream_changes: Vec::new(),
    }))
}

pub(crate) fn sql2_commits_filesystem_payload_domain_changes(
    prepared: &PreparedExecutionContext,
) -> bool {
    let Some(sql2_write) = prepared.sql2_write.as_ref() else {
        return false;
    };
    matches!(
        sql2_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    ) && matches!(
        sql2_write.planned_write.command.mode,
        crate::sql2::planner::ir::WriteMode::Tracked
    ) && sql2_tracked_write_is_live(prepared)
}

fn sql2_persists_filesystem_payload_writes(prepared: &PreparedExecutionContext) -> bool {
    let Some(sql2_write) = prepared.sql2_write.as_ref() else {
        return false;
    };
    matches!(
        sql2_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    ) && matches!(
        sql2_write.planned_write.command.mode,
        crate::sql2::planner::ir::WriteMode::Tracked
            | crate::sql2::planner::ir::WriteMode::Untracked
    ) && !prepared.intent.pending_file_writes.is_empty()
}

fn sql2_tracked_write_is_live(prepared: &PreparedExecutionContext) -> bool {
    let Some(sql2_write) = prepared.sql2_write.as_ref() else {
        return false;
    };
    matches!(
        prepared.plan.result_contract,
        ResultContract::DmlNoReturning
    ) && matches!(
        sql2_write.planned_write.command.mode,
        crate::sql2::planner::ir::WriteMode::Tracked
    ) && sql2_write.domain_change_batch.is_some()
        && sql2_write.planned_write.commit_preconditions.is_some()
        && live_sql2_operation_supported(sql2_write)
}

fn sql2_untracked_write_is_live(prepared: &PreparedExecutionContext) -> bool {
    let Some(sql2_write) = prepared.sql2_write.as_ref() else {
        return false;
    };
    matches!(
        prepared.plan.result_contract,
        ResultContract::DmlNoReturning
    ) && matches!(
        sql2_write.planned_write.command.mode,
        crate::sql2::planner::ir::WriteMode::Untracked
    ) && matches!(
        sql2_write
            .planned_write
            .command
            .target
            .descriptor
            .surface_family,
        crate::sql2::catalog::SurfaceFamily::State
            | crate::sql2::catalog::SurfaceFamily::Entity
            | crate::sql2::catalog::SurfaceFamily::Filesystem
    ) || matches!(
        prepared.plan.result_contract,
        ResultContract::DmlNoReturning
    ) && matches!(
        sql2_write.planned_write.command.mode,
        crate::sql2::planner::ir::WriteMode::Untracked
    ) && matches!(
        sql2_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_active_version" | "lix_active_account"
    )
}

fn live_sql2_operation_supported(sql2_write: &Sql2PreparedWrite) -> bool {
    match sql2_write.planned_write.command.operation_kind {
        crate::sql2::planner::ir::WriteOperationKind::Insert => true,
        crate::sql2::planner::ir::WriteOperationKind::Update
        | crate::sql2::planner::ir::WriteOperationKind::Delete => matches!(
            sql2_write
                .planned_write
                .commit_preconditions
                .as_ref()
                .map(|preconditions| &preconditions.write_lane),
            Some(crate::sql2::planner::ir::WriteLane::SingleVersion(_))
                | Some(crate::sql2::planner::ir::WriteLane::ActiveVersion)
                | Some(crate::sql2::planner::ir::WriteLane::GlobalAdmin)
        ),
    }
}

fn state_commit_stream_operation(sql2_write: &Sql2PreparedWrite) -> StateCommitStreamOperation {
    match sql2_write.planned_write.command.operation_kind {
        crate::sql2::planner::ir::WriteOperationKind::Insert => StateCommitStreamOperation::Insert,
        crate::sql2::planner::ir::WriteOperationKind::Update => StateCommitStreamOperation::Update,
        crate::sql2::planner::ir::WriteOperationKind::Delete => StateCommitStreamOperation::Delete,
    }
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

async fn apply_sql2_untracked_rows(
    transaction: &mut dyn LixTransaction,
    rows: &[crate::sql2::planner::ir::PlannedStateRow],
    timestamp: &str,
) -> Result<(), LixError> {
    for row in rows {
        if row.tombstone {
            apply_sql2_untracked_delete(transaction, row).await?;
        } else {
            apply_sql2_untracked_upsert(transaction, row, timestamp).await?;
        }
    }
    Ok(())
}

async fn apply_sql2_untracked_upsert(
    transaction: &mut dyn LixTransaction,
    row: &crate::sql2::planner::ir::PlannedStateRow,
    timestamp: &str,
) -> Result<(), LixError> {
    let file_id = planned_row_text_value(row, "file_id")?;
    let plugin_key = planned_row_text_value(row, "plugin_key")?;
    let schema_version = planned_row_text_value(row, "schema_version")?;
    let snapshot_content = planned_row_text_value(row, "snapshot_content")?;
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
        snapshot_content = escape_sql_string(snapshot_content),
        metadata = metadata_sql,
        writer_key = writer_key_sql,
        schema_version = escape_sql_string(schema_version),
        timestamp = escape_sql_string(timestamp),
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

async fn apply_sql2_untracked_delete(
    transaction: &mut dyn LixTransaction,
    row: &crate::sql2::planner::ir::PlannedStateRow,
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
    row: &'a crate::sql2::planner::ir::PlannedStateRow,
    key: &str,
) -> Result<&'a str, LixError> {
    planned_row_optional_text_value(row, key).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("sql2 untracked execution requires '{key}' in the resolved row"),
    })
}

fn planned_row_optional_text_value<'a>(
    row: &'a crate::sql2::planner::ir::PlannedStateRow,
    key: &str,
) -> Option<&'a str> {
    match row.values.get(key) {
        Some(Value::Text(value)) => Some(value.as_str()),
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

fn semantic_plan_effects_from_untracked_sql2_write(
    prepared: &PreparedExecutionContext,
    sql2_write: &Sql2PreparedWrite,
) -> Result<PlanEffects, LixError> {
    let mut effects = PlanEffects::default();
    let Some(resolved) = sql2_write.planned_write.resolved_write_plan.as_ref() else {
        return Ok(effects);
    };
    effects.state_commit_stream_changes = state_commit_stream_changes_from_planned_rows(
        &resolved.intended_post_state,
        state_commit_stream_operation(sql2_write),
        true,
        sql2_write
            .planned_write
            .command
            .execution_context
            .writer_key
            .as_deref(),
    )?;
    if matches!(
        sql2_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    ) {
        effects.file_cache_refresh_targets =
            authoritative_pending_file_write_targets(&prepared.intent.pending_file_writes);
        effects
            .file_cache_refresh_targets
            .extend(prepared.intent.pending_file_delete_targets.iter().cloned());
    }
    if sql2_write
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        != "lix_active_version"
    {
        return Ok(effects);
    }
    for row in resolved.intended_post_state.iter().rev() {
        if row.schema_key != active_version_schema_key()
            || planned_row_optional_text_value(row, "file_id") != Some(active_version_file_id())
            || row.version_id.as_deref() != Some(active_version_storage_version_id())
            || row.tombstone
        {
            continue;
        }
        let Some(snapshot_content) = planned_row_optional_text_value(row, "snapshot_content")
        else {
            continue;
        };
        effects.next_active_version_id = Some(parse_active_version_snapshot(snapshot_content)?);
        break;
    }
    Ok(effects)
}

fn append_commit_error_to_lix_error(error: crate::commit::AppendCommitError) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.message,
    }
}

fn semantic_plan_effects_from_domain_changes(
    changes: &[crate::commit::ProposedDomainChange],
    stream_operation: StateCommitStreamOperation,
) -> Result<PlanEffects, LixError> {
    Ok(PlanEffects {
        state_commit_stream_changes: state_commit_stream_changes_from_domain_changes(
            changes,
            stream_operation,
        )?,
        next_active_version_id: next_active_version_id_from_domain_changes(changes)?,
        file_cache_refresh_targets: file_cache_refresh_targets_from_domain_changes(changes),
    })
}

fn next_active_version_id_from_domain_changes(
    changes: &[crate::commit::ProposedDomainChange],
) -> Result<Option<String>, LixError> {
    for change in changes.iter().rev() {
        if change.schema_key != active_version_schema_key()
            || change.file_id.as_deref() != Some(active_version_file_id())
            || change.version_id != active_version_storage_version_id()
        {
            continue;
        }

        let Some(snapshot_content) = change.snapshot_content.as_deref() else {
            continue;
        };
        return parse_active_version_snapshot(snapshot_content).map(Some);
    }

    Ok(None)
}

fn file_cache_refresh_targets_from_domain_changes(
    changes: &[crate::commit::ProposedDomainChange],
) -> BTreeSet<(String, String)> {
    changes
        .iter()
        .filter(|change| change.file_id.as_deref() != Some("lix"))
        .filter(|change| change.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY)
        .filter(|change| change.schema_key != DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        .filter_map(|change| {
            change
                .file_id
                .as_ref()
                .map(|file_id| (file_id.clone(), change.version_id.clone()))
        })
        .collect()
}

async fn mirror_sql2_stored_schema_bootstrap_rows(
    transaction: &mut dyn LixTransaction,
    commit_result: &crate::commit::GenerateCommitResult,
) -> Result<(), LixError> {
    for row in &commit_result.materialized_state {
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

async fn apply_sql2_version_last_checkpoint_side_effects(
    transaction: &mut dyn LixTransaction,
    sql2_write: &Sql2PreparedWrite,
) -> Result<(), LixError> {
    if sql2_write
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        != "lix_version"
    {
        return Ok(());
    }

    let Some(resolved) = sql2_write.planned_write.resolved_write_plan.as_ref() else {
        return Ok(());
    };

    match sql2_write.planned_write.command.operation_kind {
        crate::sql2::planner::ir::WriteOperationKind::Insert => {
            upsert_last_checkpoint_rows(
                transaction,
                &version_checkpoint_rows(&resolved.intended_post_state),
                true,
            )
            .await
        }
        crate::sql2::planner::ir::WriteOperationKind::Update => {
            upsert_last_checkpoint_rows(
                transaction,
                &version_checkpoint_rows(&resolved.intended_post_state),
                false,
            )
            .await
        }
        crate::sql2::planner::ir::WriteOperationKind::Delete => {
            let version_ids = resolved
                .authoritative_pre_state
                .iter()
                .map(|row| row.entity_id.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            delete_last_checkpoint_rows(transaction, &version_ids).await
        }
    }
}

fn version_checkpoint_rows(
    rows: &[crate::sql2::planner::ir::PlannedStateRow],
) -> Vec<(String, String)> {
    rows.iter()
        .filter(|row| row.schema_key == crate::version::version_pointer_schema_key())
        .filter_map(|row| {
            row.values
                .get("snapshot_content")
                .and_then(|value| match value {
                    Value::Text(text) => Some(text.as_str()),
                    _ => None,
                })
                .and_then(|snapshot| serde_json::from_str::<serde_json::Value>(snapshot).ok())
                .and_then(|snapshot| {
                    snapshot
                        .get("commit_id")
                        .and_then(serde_json::Value::as_str)
                        .map(|commit_id| (row.entity_id.clone(), commit_id.to_string()))
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
