use std::collections::{BTreeMap, BTreeSet};

use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::engine::{Engine, TransactionBackendAdapter};
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::sql::public::runtime::{
    finalize_public_write_execution, prepare_public_execution_with_internal_access,
    prepare_public_execution_with_registry_and_internal_access,
    prepared_public_write_mutates_public_surface_registry, PreparedPublicExecution,
    PreparedPublicRead, PreparedPublicReadExecution, PreparedPublicWrite,
    PublicWriteExecutionPartition,
};
use crate::sql::storage::sql_text::escape_sql_string;
use crate::state::commit::{
    bind_statement_batch_for_dialect, build_statement_batch_from_generate_commit_result,
    generate_commit, load_commit_active_accounts, load_version_info_for_versions,
    CommitQueryExecutor, CreateCommitError, CreateCommitErrorKind, CreateCommitExpectedHead,
    CreateCommitInvariantChecker, CreateCommitPreconditions, CreateCommitWriteLane,
    DomainChangeInput, GenerateCommitArgs, GenerateCommitResult, MaterializedStateRow, VersionInfo,
};
use crate::state::validation::{
    validate_batch_local_write, validate_commit_time_write, validate_inserts, validate_updates,
};
use crate::{LixBackend, LixError, LixTransaction, QueryResult, Value};

use crate::schema::live_layout::{builtin_live_table_layout, LiveTableLayout};
use crate::schema::registry::load_live_table_layout_in_transaction;
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::contracts::execution_plan::ExecutionPlan;
use crate::sql::execution::contracts::planned_statement::{
    PlannedStatementSet, SchemaLiveTableRequirement,
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
use crate::sql::execution::runtime_effects::{
    build_binary_blob_fastcdc_write_program, BinaryBlobWriteInput,
};
use crate::sql::execution::write_program_runner::execute_write_program_with_transaction;
use crate::state::internal::write_program::WriteProgram;
use crate::CanonicalJson;
use serde_json::{json, Value as JsonValue};
use sqlparser::ast::Statement;

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const REGISTERED_SCHEMA_BOOTSTRAP_TABLE: &str = "lix_internal_registered_schema_bootstrap";
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
        row.schema_key == REGISTERED_SCHEMA_KEY
            && row.version_id == GLOBAL_VERSION_ID
            && !row.untracked
    }) {
        return Ok(true);
    }

    let dirty = match prepared.plan.preprocess.internal_state.as_ref() {
        Some(crate::state::internal::InternalStatePlan {
            postprocess: Some(crate::state::internal::PostprocessPlan::VtableUpdate(plan)),
        }) => plan.schema_key == REGISTERED_SCHEMA_KEY,
        Some(crate::state::internal::InternalStatePlan {
            postprocess: Some(crate::state::internal::PostprocessPlan::VtableDelete(plan)),
        }) => plan.schema_key == REGISTERED_SCHEMA_KEY,
        _ => false,
    };

    Ok(dirty)
}

#[derive(Debug, Clone)]
pub(crate) struct PendingPublicCommitSession {
    pub(crate) lane: CreateCommitWriteLane,
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

pub(crate) struct PublicCommitInvariantChecker<'a> {
    planned_write: &'a crate::sql::public::planner::ir::PlannedWrite,
    schema_cache: crate::state::validation::SchemaCache,
}

impl<'a> PublicCommitInvariantChecker<'a> {
    pub(crate) fn new(planned_write: &'a crate::sql::public::planner::ir::PlannedWrite) -> Self {
        Self {
            planned_write,
            schema_cache: crate::state::validation::SchemaCache::new(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl CreateCommitInvariantChecker for PublicCommitInvariantChecker<'_> {
    async fn recheck_invariants(
        &mut self,
        transaction: &mut dyn LixTransaction,
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

struct TransactionCommitExecutor<'a> {
    transaction: &'a mut dyn LixTransaction,
}

#[async_trait::async_trait(?Send)]
impl CommitQueryExecutor for TransactionCommitExecutor<'_> {
    fn dialect(&self) -> crate::SqlDialect {
        self.transaction.dialect()
    }

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
    let defer_runtime_sequence_load = !allow_internal_tables
        && !crate::filesystem::pending_file_writes::statements_require_generated_file_insert_ids(
            parsed_statements,
        );
    let (settings, sequence_start, functions) = engine
        .prepare_runtime_functions_with_backend(backend, defer_runtime_sequence_load)
        .await?;

    let mut statements = parsed_statements.to_vec();
    crate::filesystem::pending_file_writes::ensure_file_insert_ids_for_data_writes(
        &mut statements,
        &functions,
    )?;

    let requirements = derive_plan_requirements(&statements);

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
        .and_then(|prepared| {
            prepared
                .lowered_read()
                .map(|lowered| lowered.statements.clone())
        })
        .unwrap_or_else(|| statements.clone());

    let skip_side_effect_collection = policy.skip_side_effect_collection
        || public_write.as_ref().is_some_and(|prepared| {
            prepared.execution.as_ref().is_some_and(|execution| {
                execution.partitions.iter().any(|partition| {
                    matches!(
                        partition,
                        PublicWriteExecutionPartition::Tracked(execution)
                            if execution.lazy_exact_file_updates.iter().any(|update| {
                                matches!(
                                    update,
                                    crate::sql::public::planner::ir::LazyExactFileUpdate::Data(_)
                                )
                            })
                    )
                })
            })
        });

    let public_read_owns_execution = public_read.as_ref().is_some_and(|prepared| {
        matches!(prepared.execution, PreparedPublicReadExecution::Direct(_))
    });

    let intent = if let Some(public_write) = public_write.as_ref() {
        derived_public_execution_intent(public_write)
    } else if public_read_owns_execution {
        ExecutionIntent {
            pending_file_writes: Vec::new(),
            pending_file_delete_targets: BTreeSet::new(),
        }
    } else {
        collect_execution_intent_with_backend(
            engine,
            backend,
            &statements,
            params,
            active_version_id,
            writer_key,
            &requirements,
            IntentCollectionPolicy {
                skip_side_effect_collection,
            },
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "prepare_execution_with_backend intent collection failed: {}",
                error.description
            ),
        })?
    };

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
                                        Some(execution.schema_live_table_requirements.clone())
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
    } else if public_read_owns_execution {
        passthrough_execution_plan_for_public_read(&statements)
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

    if !public_write_owns_execution
        && !public_read_owns_execution
        && !plan.preprocess.mutations.is_empty()
    {
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
    if !public_write_owns_execution
        && !public_read_owns_execution
        && !plan.preprocess.update_validations.is_empty()
    {
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
        validate_batch_local_write(backend, &engine.schema_cache, &public_write.planned_write)
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

fn derived_public_execution_intent(
    prepared: &PreparedPublicWrite,
) -> crate::sql::execution::intent::ExecutionIntent {
    let Some(resolved) = prepared.planned_write.resolved_write_plan.as_ref() else {
        return crate::sql::execution::intent::ExecutionIntent {
            pending_file_writes: Vec::new(),
            pending_file_delete_targets: BTreeSet::new(),
        };
    };

    let pending_file_writes = resolved
        .filesystem_payload_writes()
        .map(
            |write| crate::filesystem::pending_file_writes::PendingFileWrite {
                file_id: write.file_id.clone(),
                version_id: write.version_id.clone(),
                untracked: write.untracked,
                before_path: None,
                after_path: None,
                data_is_authoritative: true,
                before_data: None,
                after_data: write.data.clone(),
            },
        )
        .collect();
    let pending_file_delete_targets = resolved
        .filesystem_payload_delete_targets()
        .cloned()
        .collect();

    crate::sql::execution::intent::ExecutionIntent {
        pending_file_writes,
        pending_file_delete_targets,
    }
}

fn passthrough_execution_plan_for_public_write(
    statements: &[Statement],
    live_table_requirements: Vec<SchemaLiveTableRequirement>,
) -> ExecutionPlan {
    ExecutionPlan {
        preprocess: PlannedStatementSet {
            sql: String::new(),
            prepared_statements: Vec::new(),
            live_table_requirements,
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

fn passthrough_execution_plan_for_public_read(statements: &[Statement]) -> ExecutionPlan {
    let mut requirements = PlanRequirements::default();
    requirements.read_only_query = true;

    ExecutionPlan {
        preprocess: PlannedStatementSet {
            sql: String::new(),
            prepared_statements: Vec::new(),
            live_table_requirements: Vec::new(),
            internal_state: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        },
        result_contract: derive_result_contract_for_statements(statements),
        requirements,
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

pub(crate) fn empty_public_write_execution_outcome() -> SqlExecutionOutcome {
    SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        postprocess_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        plan_effects_override: Some(PlanEffects::default()),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: false,
    }
}

pub(crate) fn pending_session_matches_create_commit(
    session: &PendingPublicCommitSession,
    preconditions: &CreateCommitPreconditions,
) -> bool {
    session.lane == preconditions.write_lane
        && match &preconditions.expected_head {
            CreateCommitExpectedHead::CurrentHead => true,
            CreateCommitExpectedHead::CommitId(commit_id) => commit_id == &session.commit_id,
            CreateCommitExpectedHead::CreateIfMissing => false,
        }
}

pub(crate) async fn build_pending_public_commit_session(
    transaction: &mut dyn LixTransaction,
    lane: CreateCommitWriteLane,
    commit_result: &GenerateCommitResult,
) -> Result<PendingPublicCommitSession, LixError> {
    let commit_row = commit_result
        .derived_apply_input
        .live_state_rows
        .iter()
        .find(|row| row.schema_key == "lix_commit")
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public commit session requires a lix_commit materialized row",
            )
        })?;
    let commit_snapshot = commit_row.snapshot_content.as_deref().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public commit session requires commit snapshot_content",
        )
    })?;
    let commit_snapshot: JsonValue = serde_json::from_str(commit_snapshot).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("public commit session commit snapshot is invalid JSON: {error}"),
        )
    })?;
    let change_set_id = commit_snapshot
        .get("change_set_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public commit session commit snapshot is missing change_set_id",
            )
        })?
        .to_string();
    let commit_change_id = commit_result
        .canonical_output
        .changes
        .iter()
        .find(|row| row.schema_key == "lix_commit" && row.entity_id == commit_row.entity_id)
        .map(|row| row.id.clone())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public commit session requires a lix_commit change row",
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
                "public commit session could not load commit snapshot_id",
            )
        })?;

    Ok(PendingPublicCommitSession {
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

pub(crate) async fn merge_public_domain_change_batch_into_pending_commit(
    transaction: &mut dyn LixTransaction,
    session: &mut PendingPublicCommitSession,
    batch: &crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch,
    additional_binary_blob_payloads: &[Vec<u8>],
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
                snapshot_content: canonicalize_optional_json_text(
                    change.snapshot_content.as_deref(),
                    "snapshot_content",
                    &change.schema_key,
                    &change.entity_id,
                )?,
                metadata: canonicalize_optional_json_text(
                    change.metadata.as_deref(),
                    "metadata",
                    &change.schema_key,
                    &change.entity_id,
                )?,
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
    execute_generated_commit_result(
        transaction,
        rewritten,
        additional_binary_blob_payloads,
        functions,
    )
    .await
}

fn canonicalize_optional_json_text(
    value: Option<&str>,
    field_name: &str,
    schema_key: &str,
    entity_id: &str,
) -> Result<Option<CanonicalJson>, LixError> {
    value
        .map(CanonicalJson::from_text)
        .transpose()
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "public merge requires valid JSON {field_name} for '{schema_key}:{entity_id}': {}",
                    error.description
                ),
            )
        })
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
    session: &PendingPublicCommitSession,
    generated: GenerateCommitResult,
    domain_change_count: usize,
    timestamp: &str,
) -> Result<GenerateCommitResult, LixError> {
    let temporary_commit_id = generated
        .derived_apply_input
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
        .derived_apply_input
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
    let version_ref_entity_id = pending_session_version_ref_entity_id(&session.lane);

    let mut live_state_rows = Vec::new();
    for mut row in generated.derived_apply_input.live_state_rows {
        if is_pending_commit_meta_row(
            &row,
            &temporary_commit_id,
            &temporary_change_set_id,
            version_ref_entity_id,
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
        snapshot_content: Some(CanonicalJson::from_value(session.commit_snapshot.clone())?),
        metadata: None,
        created_at: timestamp.to_string(),
        lixcol_version_id: GLOBAL_VERSION_ID.to_string(),
        lixcol_commit_id: session.commit_id.clone(),
        writer_key: None,
    });

    Ok(GenerateCommitResult {
        canonical_output: crate::state::commit::CanonicalCommitOutput {
            changes: generated
                .canonical_output
                .changes
                .into_iter()
                .take(domain_change_count)
                .collect(),
        },
        derived_apply_input: crate::state::commit::DerivedCommitApplyInput {
            live_state_rows,
            live_layouts: generated.derived_apply_input.live_layouts,
        },
    })
}

fn is_pending_commit_meta_row(
    row: &MaterializedStateRow,
    temporary_commit_id: &str,
    temporary_change_set_id: &str,
    version_ref_entity_id: &str,
) -> Result<bool, LixError> {
    match row.schema_key.as_str() {
        "lix_change_set" => Ok(row.entity_id == temporary_change_set_id),
        "lix_commit" => Ok(row.entity_id == temporary_commit_id),
        "lix_commit_edge" => Ok(row.entity_id.ends_with(&format!("~{temporary_commit_id}"))),
        "lix_version_ref" if row.entity_id == version_ref_entity_id => {
            let snapshot = row.snapshot_content.as_deref().unwrap_or("");
            let parsed: JsonValue = serde_json::from_str(snapshot).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("public merge rewrite saw invalid version ref JSON: {error}"),
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
) -> Result<(String, CanonicalJson), LixError> {
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
    Ok((
        format!("{change_set_id}~{change_id}"),
        CanonicalJson::from_value(parsed)?,
    ))
}

fn pending_session_version_ref_entity_id(lane: &CreateCommitWriteLane) -> &str {
    match lane {
        CreateCommitWriteLane::Version(version_id) => version_id.as_str(),
        CreateCommitWriteLane::GlobalAdmin => GLOBAL_VERSION_ID,
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
    mut result: GenerateCommitResult,
    additional_binary_blob_payloads: &[Vec<u8>],
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<(), LixError> {
    result.derived_apply_input.live_layouts = load_live_layouts_for_rows_in_transaction(
        transaction,
        &result.derived_apply_input.live_state_rows,
    )
    .await?;
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
    if !additional_binary_blob_payloads.is_empty() {
        let payloads = additional_binary_blob_payloads
            .iter()
            .map(|data| BinaryBlobWriteInput {
                file_id: "",
                version_id: "",
                data,
            })
            .collect::<Vec<_>>();
        program.extend(build_binary_blob_fastcdc_write_program(
            transaction.dialect(),
            &payloads,
        )?);
    }
    program.push_batch(prepared);
    execute_write_program_with_transaction(transaction, program).await?;
    Ok(())
}

async fn load_live_layouts_for_rows_in_transaction(
    transaction: &mut dyn LixTransaction,
    rows: &[MaterializedStateRow],
) -> Result<BTreeMap<String, LiveTableLayout>, LixError> {
    let mut layouts = BTreeMap::new();
    let schema_keys = rows
        .iter()
        .map(|row| row.schema_key.clone())
        .collect::<BTreeSet<_>>();
    for schema_key in schema_keys {
        if let Some(layout) = builtin_live_table_layout(&schema_key)? {
            layouts.insert(schema_key, layout);
            continue;
        }
        layouts.insert(
            schema_key.clone(),
            load_live_table_layout_in_transaction(transaction, &schema_key).await?,
        );
    }
    Ok(layouts)
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

pub(crate) fn create_commit_error_to_lix_error(
    error: crate::state::commit::CreateCommitError,
) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.message,
    }
}

pub(crate) async fn mirror_public_registered_schema_bootstrap_rows(
    transaction: &mut dyn LixTransaction,
    commit_result: &crate::state::commit::GenerateCommitResult,
) -> Result<(), LixError> {
    for row in &commit_result.derived_apply_input.live_state_rows {
        if row.schema_key != REGISTERED_SCHEMA_KEY || row.lixcol_version_id != GLOBAL_VERSION_ID {
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
            table = REGISTERED_SCHEMA_BOOTSTRAP_TABLE,
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

pub(crate) async fn apply_public_version_last_checkpoint_side_effects(
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
            upsert_last_checkpoint_rows(
                transaction,
                &version_checkpoint_rows_from_resolved_write(public_write, batch),
                true,
            )
            .await
        }
        crate::sql::public::planner::ir::WriteOperationKind::Update => {
            upsert_last_checkpoint_rows(
                transaction,
                &version_checkpoint_rows_from_resolved_write(public_write, batch),
                false,
            )
            .await
        }
        crate::sql::public::planner::ir::WriteOperationKind::Delete => {
            let version_ids = version_ids_from_resolved_write(public_write, batch);
            delete_last_checkpoint_rows(transaction, &version_ids).await
        }
    }
}

fn version_checkpoint_rows_from_resolved_write(
    public_write: &PreparedPublicWrite,
    batch: &crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch,
) -> Vec<(String, String)> {
    if let Some(resolved) = public_write.planned_write.resolved_write_plan.as_ref() {
        let rows = resolved
            .partitions
            .iter()
            .flat_map(|partition| partition.intended_post_state.iter())
            .filter(|row| {
                row.schema_key == crate::version::version_ref_schema_key() && !row.tombstone
            })
            .filter_map(|row| {
                row.values
                    .get("snapshot_content")
                    .and_then(|value| match value {
                        Value::Text(snapshot) => {
                            serde_json::from_str::<serde_json::Value>(snapshot)
                                .ok()
                                .and_then(|snapshot| {
                                    snapshot
                                        .get("commit_id")
                                        .and_then(serde_json::Value::as_str)
                                        .map(|commit_id| {
                                            (row.entity_id.clone(), commit_id.to_string())
                                        })
                                })
                        }
                        _ => None,
                    })
            })
            .collect::<Vec<_>>();
        if !rows.is_empty() {
            return rows;
        }
    }

    batch
        .changes
        .iter()
        .filter(|change| change.schema_key == crate::version::version_ref_schema_key())
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

fn version_ids_from_resolved_write(
    public_write: &PreparedPublicWrite,
    batch: &crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch,
) -> Vec<String> {
    if let Some(resolved) = public_write.planned_write.resolved_write_plan.as_ref() {
        let version_ids = resolved
            .partitions
            .iter()
            .flat_map(|partition| partition.intended_post_state.iter())
            .filter(|row| {
                matches!(
                    row.schema_key.as_str(),
                    "lix_version_ref" | "lix_version_descriptor"
                )
            })
            .map(|row| row.entity_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if !version_ids.is_empty() {
            return version_ids;
        }
    }

    batch
        .changes
        .iter()
        .map(|change| change.entity_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
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
