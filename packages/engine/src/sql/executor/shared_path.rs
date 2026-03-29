use crate::engine::Engine;
use crate::functions::SharedFunctionProvider;
use crate::sql::executor::public_runtime::{
    finalize_public_write_execution, prepare_public_execution_with_internal_access_and_functions,
    prepare_public_execution_with_registry_and_internal_access_and_pending_transaction_view_and_functions,
    prepared_public_write_mutates_public_surface_registry,
    try_prepare_public_read_with_registry_and_internal_access,
    try_prepare_public_write_with_functions, try_prepare_public_write_with_registry_and_functions,
    PreparedPublicExecution, PreparedPublicWrite,
};
use crate::sql::semantic_ir::validation::{
    validate_batch_local_write, validate_inserts, validate_updates,
};
use crate::{LixBackend, LixError, Value};

use crate::sql::executor::compiled::{
    CompiledExecution, CompiledExecutionBody, CompiledInternalExecution,
};
use crate::sql::executor::contracts::effects::PlanEffects;
use crate::sql::executor::contracts::planned_statement::PlannedStatementSet;
use crate::sql::executor::contracts::requirements::PlanRequirements;
use crate::sql::executor::contracts::result_contract::ResultContract;
use crate::sql::executor::derive_effects::derive_plan_effects;
use crate::sql::executor::derive_requirements::derive_plan_requirements;
use crate::sql::executor::execution_program::{
    BoundStatementTemplateInstance, StatementTemplateOwnership,
};
use crate::sql::executor::intent::{
    collect_execution_intent_with_backend, ExecutionIntent, IntentCollectionPolicy,
};
use crate::sql::executor::preprocess::preprocess_with_surfaces_to_plan;
use crate::sql::executor::runtime_state::ExecutionRuntimeState;
use crate::transaction::PendingTransactionView;
use sqlparser::ast::Statement;

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const GLOBAL_VERSION_ID: &str = "global";

pub(crate) struct PreparationPolicy {
    pub(crate) skip_side_effect_collection: bool,
}

#[derive(Clone, Copy)]
struct StaticCompilationArtifacts<'a> {
    ownership_hint: Option<StatementTemplateOwnership>,
    plan_requirements: Option<&'a PlanRequirements>,
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

    Ok(false)
}

async fn compile_execution_with_backend(
    engine: &Engine,
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&PendingTransactionView>,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_account_ids: &[String],
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    public_surface_registry_override: Option<&crate::sql::catalog::SurfaceRegistry>,
    policy: PreparationPolicy,
    runtime_state: Option<&ExecutionRuntimeState>,
    static_artifacts: StaticCompilationArtifacts<'_>,
) -> Result<CompiledExecution, LixError> {
    let owned_runtime_state = match runtime_state {
        Some(_) => None,
        None => Some(ExecutionRuntimeState::prepare(engine, backend).await?),
    };
    let runtime_state = runtime_state.unwrap_or_else(|| {
        owned_runtime_state
            .as_ref()
            .expect("owned runtime state should exist when no caller-owned state is provided")
    });
    let functions = runtime_state.provider().clone();

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
        active_account_ids,
        writer_key,
        allow_internal_tables,
        public_surface_registry_override,
        static_artifacts.ownership_hint,
        functions.clone(),
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
        runtime_state: runtime_state.clone(),
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
    active_account_ids: &[String],
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    public_surface_registry_override: Option<&crate::sql::catalog::SurfaceRegistry>,
    ownership_hint: Option<StatementTemplateOwnership>,
    functions: SharedFunctionProvider<crate::deterministic_mode::RuntimeFunctionProvider>,
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
            None => prepare_public_execution_with_internal_access_and_functions(
                backend,
                statements,
                params,
                active_version_id,
                active_account_ids,
                writer_key,
                allow_internal_tables,
                functions.clone(),
            )
            .await?,
        },
        Some(StatementTemplateOwnership::PublicWrite) => match public_surface_registry_override {
            Some(registry) => try_prepare_public_write_with_registry_and_functions(
                backend,
                registry,
                statements,
                params,
                active_version_id,
                active_account_ids,
                writer_key,
                pending_transaction_view,
                functions.clone(),
            )
            .await?
            .map(PreparedPublicExecution::Write),
            None => try_prepare_public_write_with_functions(
                backend,
                statements,
                params,
                active_version_id,
                active_account_ids,
                writer_key,
                functions.clone(),
            )
            .await?
            .map(PreparedPublicExecution::Write),
        },
        Some(StatementTemplateOwnership::Internal) => None,
        None => match public_surface_registry_override {
            Some(registry) => {
                prepare_public_execution_with_registry_and_internal_access_and_pending_transaction_view_and_functions(
                    backend,
                    registry,
                    statements,
                    params,
                    active_version_id,
                    active_account_ids,
                    writer_key,
                    allow_internal_tables,
                    pending_transaction_view,
                    functions.clone(),
                )
                .await?
            }
            None => {
                prepare_public_execution_with_internal_access_and_functions(
                    backend,
                    statements,
                    params,
                    active_version_id,
                    active_account_ids,
                    writer_key,
                    allow_internal_tables,
                    functions.clone(),
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

pub(crate) async fn compile_execution_from_template_instance_with_backend(
    engine: &Engine,
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&PendingTransactionView>,
    template_instance: &BoundStatementTemplateInstance,
    active_version_id: &str,
    active_account_ids: &[String],
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    public_surface_registry_override: Option<&crate::sql::catalog::SurfaceRegistry>,
    runtime_state: Option<&ExecutionRuntimeState>,
    policy: PreparationPolicy,
) -> Result<CompiledExecution, LixError> {
    compile_execution_with_backend(
        engine,
        backend,
        pending_transaction_view,
        std::slice::from_ref(template_instance.statement()),
        template_instance.params(),
        active_version_id,
        active_account_ids,
        writer_key,
        allow_internal_tables,
        public_surface_registry_override,
        policy,
        runtime_state,
        StaticCompilationArtifacts {
            ownership_hint: template_instance.ownership_hint(),
            plan_requirements: Some(template_instance.plan_requirements()),
        },
    )
    .await
}

fn derived_public_execution_intent(
    prepared: &PreparedPublicWrite,
) -> crate::sql::executor::intent::ExecutionIntent {
    let Some(resolved) = prepared.planned_write.resolved_write_plan.as_ref() else {
        return crate::sql::executor::intent::ExecutionIntent {
            filesystem_state: Default::default(),
        };
    };

    crate::sql::executor::intent::ExecutionIntent {
        filesystem_state: resolved.filesystem_state(),
    }
}

fn validate_compiled_internal_execution(
    preprocess: &PlannedStatementSet,
    result_contract: ResultContract,
) -> Result<(), LixError> {
    if preprocess.prepared_statements.is_empty()
        && !matches!(result_contract, ResultContract::DmlNoReturning)
        && preprocess.mutations.is_empty()
        && preprocess.update_validations.is_empty()
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "sql compiler produced an internal execution without statements",
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
    use crate::sql::parser::parse_sql_statements;

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
