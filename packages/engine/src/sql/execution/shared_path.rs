use std::collections::BTreeSet;

use crate::engine::Engine;
use crate::sql::public::runtime::{
    finalize_public_write_execution,
    prepare_public_execution_with_internal_access,
    prepare_public_execution_with_registry_and_internal_access_and_pending_transaction_view,
    prepared_public_write_mutates_public_surface_registry,
    try_prepare_public_read_with_registry_and_internal_access, try_prepare_public_write,
    try_prepare_public_write_with_registry, PreparedPublicExecution, PreparedPublicWrite,
    PublicWriteExecutionPartition,
};
use crate::sql_support::text::escape_sql_string;
use crate::sql::public::validation::{validate_batch_local_write, validate_inserts, validate_updates};
use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, Value};

use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::contracts::planned_statement::PlannedStatementSet;
use crate::sql::execution::contracts::requirements::PlanRequirements;
use crate::sql::execution::contracts::result_contract::ResultContract;
use crate::sql::execution::derive_effects::derive_plan_effects;
use crate::sql::execution::derive_requirements::derive_plan_requirements;
use crate::sql::execution::execution_program::{
    BoundStatementTemplateInstance, StatementTemplateOwnership,
};
use crate::sql::execution::intent::{
    collect_execution_intent_with_backend, ExecutionIntent, IntentCollectionPolicy,
};
use crate::sql::execution::preprocess::preprocess_with_surfaces_to_plan;
use crate::transaction::PendingTransactionView;
use crate::transaction::sql_adapter::{
    CompiledExecution, CompiledExecutionBody, CompiledExecutionStep, CompiledInternalExecution,
    SqlExecutionOutcome,
};
use sqlparser::ast::Statement;

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const REGISTERED_SCHEMA_BOOTSTRAP_TABLE: &str = "lix_internal_registered_schema_bootstrap";
const GLOBAL_VERSION_ID: &str = "global";

pub(crate) struct PreparationPolicy {
    pub(crate) skip_side_effect_collection: bool,
}

#[derive(Clone, Copy)]
struct StaticCompilationArtifacts<'a> {
    ownership_hint: Option<StatementTemplateOwnership>,
    plan_requirements: Option<&'a PlanRequirements>,
    requires_generated_filesystem_insert_id: Option<bool>,
}

pub(crate) fn prepared_execution_mutates_public_surface_registry(
    prepared: &CompiledExecution,
) -> Result<bool, LixError> {
    if prepared.public_write().is_some() {
        return prepared
            .public_write()
            .map(prepared_public_write_mutates_public_surface_registry)
            .transpose()
            .map(|value| value.unwrap_or(false));
    }

    let Some(internal) = prepared.internal_execution() else {
        return Ok(false);
    };

    if internal.mutations.iter().any(|row| {
        row.schema_key == REGISTERED_SCHEMA_KEY
            && row.version_id == GLOBAL_VERSION_ID
            && !row.untracked
    }) {
        return Ok(true);
    }

    let dirty = match internal.postprocess.as_ref() {
        Some(crate::sql::internal::PostprocessPlan::VtableUpdate(plan)) => {
            plan.schema_key == REGISTERED_SCHEMA_KEY
        }
        Some(crate::sql::internal::PostprocessPlan::VtableDelete(plan)) => {
            plan.schema_key == REGISTERED_SCHEMA_KEY
        }
        None => false,
    };

    Ok(dirty)
}

async fn compile_execution_with_backend(
    engine: &Engine,
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&PendingTransactionView>,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    public_surface_registry_override: Option<&crate::sql::public::catalog::SurfaceRegistry>,
    policy: PreparationPolicy,
    static_artifacts: StaticCompilationArtifacts<'_>,
) -> Result<CompiledExecution, LixError> {
    let requires_generated_filesystem_insert_id = static_artifacts
        .requires_generated_filesystem_insert_id
        .unwrap_or_else(|| {
            crate::filesystem::statements_require_generated_filesystem_insert_ids(parsed_statements)
        });
    let defer_runtime_sequence_load =
        !allow_internal_tables && !requires_generated_filesystem_insert_id;
    let (settings, sequence_start, functions) = engine
        .prepare_runtime_functions_with_backend(backend, defer_runtime_sequence_load)
        .await?;

    let mut statements = parsed_statements.to_vec();
    crate::filesystem::ensure_generated_filesystem_insert_ids(&mut statements, &functions)?;

    let requirements = static_artifacts
        .plan_requirements
        .cloned()
        .unwrap_or_else(|| derive_plan_requirements(&statements));

    let public_execution = prepare_public_execution_for_compile(
        backend,
        pending_transaction_view,
        &statements,
        params,
        active_version_id,
        writer_key,
        allow_internal_tables,
        public_surface_registry_override,
        static_artifacts.ownership_hint,
    )
    .await?;
    let (public_read, mut public_write) = match public_execution {
        Some(PreparedPublicExecution::Read(prepared)) => (Some(prepared), None),
        Some(PreparedPublicExecution::Write(prepared)) => (None, Some(prepared)),
        None => (None, None),
    };
    let skip_side_effect_collection = policy.skip_side_effect_collection
        || public_write.as_ref().is_some_and(|prepared| {
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .is_some_and(|resolved| {
                    resolved
                        .filesystem_state()
                        .files
                        .values()
                        .any(|file| file.data.is_some())
                })
        });

    let public_read_owns_execution = public_read.is_some();

    let intent = if let Some(public_write) = public_write.as_ref() {
        derived_public_execution_intent(public_write)
    } else if public_read_owns_execution {
        ExecutionIntent {
            filesystem_state: Default::default(),
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
        let planned_write = public_write.planned_write.clone();
        if let Some(execution) = public_write.materialization_mut() {
            finalize_public_write_execution(execution, &planned_write, &intent.filesystem_state)
                .map_err(|error| LixError {
                    code: error.code,
                    description: format!(
                        "prepare_execution_with_backend public execution finalization failed: {}",
                        error.description
                    ),
                })?;
        }
    }

    let result_contract = derive_result_contract_for_statements(&statements);
    let (effects, internal_execution) = if public_write_owns_execution || public_read_owns_execution
    {
        (PlanEffects::default(), None)
    } else {
        let preprocess = preprocess_with_surfaces_to_plan(
            backend,
            &engine.cel_evaluator,
            statements.clone(),
            params,
            functions.clone(),
            writer_key,
        )
        .await
        .map_err(LixError::from)
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "prepare_execution_with_backend internal compilation failed: {}",
                error.description
            ),
        })?;
        validate_compiled_internal_execution(&preprocess, result_contract)?;

        if !preprocess.mutations.is_empty() {
            validate_inserts(backend, &engine.schema_cache, &preprocess.mutations)
                .await
                .map_err(|error| LixError {
                    code: error.code,
                    description: format!(
                        "prepare_execution_with_backend insert validation failed: {}",
                        error.description
                    ),
                })?;
        }
        if !preprocess.update_validations.is_empty() {
            validate_updates(
                backend,
                &engine.schema_cache,
                &preprocess.update_validations,
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

        let effects = derive_plan_effects(&preprocess, writer_key).map_err(LixError::from)?;
        let internal_execution = CompiledInternalExecution {
            prepared_statements: preprocess.prepared_statements,
            live_table_requirements: preprocess.live_table_requirements,
            postprocess: preprocess.internal_state.and_then(|plan| plan.postprocess),
            mutations: preprocess.mutations,
            update_validations: preprocess.update_validations,
            should_refresh_file_cache: requirements.should_refresh_file_cache,
        };
        (effects, Some(internal_execution))
    };

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

    let body = match (public_read, public_write, internal_execution) {
        (Some(public_read), None, None) => CompiledExecutionBody::PublicRead(public_read),
        (None, Some(public_write), None) => CompiledExecutionBody::PublicWrite(public_write),
        (None, None, Some(internal_execution)) => {
            CompiledExecutionBody::Internal(internal_execution)
        }
        (public_read, public_write, internal_execution) => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "compiled execution must have exactly one body; got public_read={}, public_write={}, internal={}",
                    public_read.is_some(),
                    public_write.is_some(),
                    internal_execution.is_some()
                ),
            ));
        }
    };

    Ok(CompiledExecution {
        intent,
        settings,
        sequence_start,
        functions,
        result_contract,
        effects,
        read_only_query: requirements.read_only_query,
        body,
    })
}

async fn prepare_public_execution_for_compile(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&PendingTransactionView>,
    statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    public_surface_registry_override: Option<&crate::sql::public::catalog::SurfaceRegistry>,
    ownership_hint: Option<StatementTemplateOwnership>,
) -> Result<Option<PreparedPublicExecution>, LixError> {
    let prepared = match ownership_hint {
        Some(StatementTemplateOwnership::PublicRead) => match public_surface_registry_override {
            Some(registry) => try_prepare_public_read_with_registry_and_internal_access(
                backend,
                registry,
                statements,
                params,
                active_version_id,
                writer_key,
                allow_internal_tables,
            )
            .await?
            .map(PreparedPublicExecution::Read),
            None => prepare_public_execution_with_internal_access(
                backend,
                statements,
                params,
                active_version_id,
                writer_key,
                allow_internal_tables,
            )
            .await?,
        },
        Some(StatementTemplateOwnership::PublicWrite) => match public_surface_registry_override {
            Some(registry) => try_prepare_public_write_with_registry(
                backend,
                registry,
                statements,
                params,
                active_version_id,
                writer_key,
                pending_transaction_view,
            )
            .await?
            .map(PreparedPublicExecution::Write),
            None => try_prepare_public_write(
                backend,
                statements,
                params,
                active_version_id,
                writer_key,
            )
            .await?
            .map(PreparedPublicExecution::Write),
        },
        Some(StatementTemplateOwnership::Internal) => None,
        None => match public_surface_registry_override {
            Some(registry) => {
                prepare_public_execution_with_registry_and_internal_access_and_pending_transaction_view(
                    backend,
                    registry,
                    statements,
                    params,
                    active_version_id,
                    writer_key,
                    allow_internal_tables,
                    pending_transaction_view,
                )
                .await?
            }
            None => {
                prepare_public_execution_with_internal_access(
                    backend,
                    statements,
                    params,
                    active_version_id,
                    writer_key,
                    allow_internal_tables,
                )
                .await?
            }
        },
    };

    if matches!(
        ownership_hint,
        Some(StatementTemplateOwnership::PublicRead | StatementTemplateOwnership::PublicWrite)
    ) && prepared.is_none()
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "statement template ownership hint no longer matches compile route",
        ));
    }

    Ok(prepared)
}

pub(crate) async fn compile_execution_step_from_template_instance_with_backend(
    engine: &Engine,
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&PendingTransactionView>,
    template_instance: &BoundStatementTemplateInstance,
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    public_surface_registry_override: Option<&crate::sql::public::catalog::SurfaceRegistry>,
    policy: PreparationPolicy,
) -> Result<CompiledExecutionStep, LixError> {
    let prepared = compile_execution_with_backend(
        engine,
        backend,
        pending_transaction_view,
        std::slice::from_ref(template_instance.statement()),
        template_instance.params(),
        active_version_id,
        writer_key,
        allow_internal_tables,
        public_surface_registry_override,
        policy,
        StaticCompilationArtifacts {
            ownership_hint: template_instance.ownership_hint(),
            plan_requirements: Some(template_instance.plan_requirements()),
            requires_generated_filesystem_insert_id: Some(
                template_instance.requires_generated_filesystem_insert_id(),
            ),
        },
    )
    .await?;
    CompiledExecutionStep::compile(prepared, writer_key)
}

fn derived_public_execution_intent(
    prepared: &PreparedPublicWrite,
) -> crate::sql::execution::intent::ExecutionIntent {
    let Some(resolved) = prepared.planned_write.resolved_write_plan.as_ref() else {
        return crate::sql::execution::intent::ExecutionIntent {
            filesystem_state: Default::default(),
        };
    };

    crate::sql::execution::intent::ExecutionIntent {
        filesystem_state: resolved.filesystem_state(),
    }
}

fn validate_compiled_internal_execution(
    preprocess: &PlannedStatementSet,
    result_contract: ResultContract,
) -> Result<(), LixError> {
    let postprocess = preprocess
        .internal_state
        .as_ref()
        .and_then(|plan| plan.postprocess.as_ref());

    if preprocess.prepared_statements.is_empty()
        && !matches!(result_contract, ResultContract::DmlNoReturning)
        && postprocess.is_none()
        && preprocess.mutations.is_empty()
        && preprocess.update_validations.is_empty()
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "sql compiler produced an internal execution without statements",
        ));
    }
    if crate::sql::internal::requires_single_statement_postprocess(postprocess)
        && preprocess.prepared_statements.len() != 1
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "sql compiler produced invalid postprocess execution with multiple statements",
        ));
    }
    if postprocess.is_some() && !preprocess.mutations.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "sql compiler produced postprocess execution with unexpected mutation rows",
        ));
    }
    if let Some(postprocess) = postprocess {
        crate::sql::internal::validate_internal_state_plan(Some(
            &crate::sql::internal::InternalStatePlan {
                postprocess: Some(postprocess.clone()),
            },
        ))?;
    }
    if postprocess.is_some()
        && matches!(
            result_contract,
            ResultContract::Select | ResultContract::Other
        )
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "sql compiler produced postprocess execution for non-DML contract",
        ));
    }
    if postprocess.is_some() && result_contract.expects_postprocess_output() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "sql compiler cannot expose postprocess internal rows as public DML RETURNING output",
        ));
    }
    Ok(())
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
pub(crate) fn top_level_write_target_name(statement: &Statement) -> Option<String> {
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
    use crate::sql_support::binding::parse_sql_statements;

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

pub(crate) fn public_write_filesystem_payload_changes_already_committed(
    prepared: &CompiledExecution,
) -> bool {
    let Some(public_write) = prepared.public_write() else {
        return false;
    };
    matches!(
        public_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    ) && public_write.materialization().is_some_and(|execution| {
        execution
            .partitions
            .iter()
            .any(|partition| matches!(partition, PublicWriteExecutionPartition::Tracked(_)))
    })
}

pub(crate) async fn mirror_public_registered_schema_bootstrap_rows(
    transaction: &mut dyn LixBackendTransaction,
    applied_output: &crate::canonical::append::CreateCommitAppliedOutput,
) -> Result<(), LixError> {
    for row in &applied_output.derived_apply_input.live_state_rows {
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
             ) ON CONFLICT (entity_id, file_id, version_id, untracked) DO UPDATE SET \
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
    transaction: &mut dyn LixBackendTransaction,
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
                                            (row.entity_id.to_string(), commit_id.to_string())
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
                            .map(|commit_id| (change.entity_id.to_string(), commit_id.to_string()))
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
            .map(|row| row.entity_id.to_string())
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
        .map(|change| change.entity_id.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
}

async fn upsert_last_checkpoint_rows(
    transaction: &mut dyn LixBackendTransaction,
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
    transaction: &mut dyn LixBackendTransaction,
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
