use crate::contracts::surface::SurfaceRegistry;
use crate::contracts::traits::CompiledSchemaCache;
use crate::runtime::cel::CelEvaluator;
use crate::runtime::deterministic_mode::RuntimeFunctionProvider;
use crate::runtime::functions::SharedFunctionProvider;
use crate::sql::explain::{
    build_internal_explain_artifacts, unsupported_explain_analyze_error, unwrap_explain_statement,
    ExplainRequest, ExplainStage, ExplainTimingCollector, InternalExplainBuildInput,
};
use crate::sql::physical_plan::PhysicalPlan;
use crate::{LixError, SqlDialect, Value};
use serde_json::Value as JsonValue;
use sqlparser::ast::Statement;
use std::collections::BTreeMap;
use std::time::Duration;
use std::time::Instant;

use super::compiled::{CompiledExecution, CompiledExecutionBody, CompiledInternalExecution};
use super::contracts::effects::PlanEffects;
use super::contracts::planned_statement::PlannedStatementSet;
use super::contracts::requirements::PlanRequirements;
use super::derive_effects::derive_plan_effects;
use super::derive_requirements::derive_plan_requirements;
use super::execution_program::{BoundStatementTemplateInstance, StatementTemplateOwnership};
use super::intent::{collect_execution_intent, ExecutionIntent, IntentCollectionPolicy};
use super::preprocess::preprocess_with_surfaces_to_logical_plan;
use super::public_surface::{
    finalize_public_write_execution, prepare_public_execution_with_registry_context_and_functions,
    prepared_public_write_mutates_public_surface_registry,
    try_prepare_public_read_with_registry_and_internal_access,
    try_prepare_public_write_with_registry_and_functions, PreparedPublicExecution,
    PreparedPublicWrite,
};
use crate::sql::logical_plan::{result_contract_for_statements, ResultContract};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const GLOBAL_VERSION_ID: &str = "global";

pub(crate) struct PreparationPolicy {
    pub(crate) skip_side_effect_collection: bool,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SqlCompilerMetadata {
    pub(crate) known_live_schema_definitions: BTreeMap<String, JsonValue>,
    pub(crate) current_version_heads: Option<BTreeMap<String, String>>,
}

pub(crate) trait SqlPreparationContext {
    fn dialect(&self) -> SqlDialect;

    fn cel_evaluator(&self) -> &CelEvaluator;

    fn schema_cache(&self) -> &dyn CompiledSchemaCache;

    fn functions(&self) -> &SharedFunctionProvider<RuntimeFunctionProvider>;

    fn surface_registry(&self) -> &SurfaceRegistry;

    fn compiler_metadata(&self) -> &SqlCompilerMetadata;

    fn active_history_root_commit_id(&self) -> Option<&str> {
        None
    }
}

pub(crate) struct DefaultSqlPreparationContext<'a> {
    pub(crate) dialect: SqlDialect,
    pub(crate) cel_evaluator: &'a CelEvaluator,
    pub(crate) schema_cache: &'a dyn CompiledSchemaCache,
    pub(crate) functions: &'a SharedFunctionProvider<RuntimeFunctionProvider>,
    pub(crate) surface_registry: &'a SurfaceRegistry,
    pub(crate) compiler_metadata: &'a SqlCompilerMetadata,
    pub(crate) active_history_root_commit_id: Option<&'a str>,
}

impl SqlPreparationContext for DefaultSqlPreparationContext<'_> {
    fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    fn cel_evaluator(&self) -> &CelEvaluator {
        self.cel_evaluator
    }

    fn schema_cache(&self) -> &dyn CompiledSchemaCache {
        self.schema_cache
    }

    fn functions(&self) -> &SharedFunctionProvider<RuntimeFunctionProvider> {
        self.functions
    }

    fn surface_registry(&self) -> &SurfaceRegistry {
        self.surface_registry
    }

    fn compiler_metadata(&self) -> &SqlCompilerMetadata {
        self.compiler_metadata
    }

    fn active_history_root_commit_id(&self) -> Option<&str> {
        self.active_history_root_commit_id
    }
}

#[derive(Clone, Copy)]
struct StaticCompilationArtifacts<'a> {
    parse_duration: Option<Duration>,
    ownership_hint: Option<StatementTemplateOwnership>,
    plan_requirements: Option<&'a PlanRequirements>,
}

pub(crate) fn prepared_execution_mutates_public_surface_registry(
    prepared: &CompiledExecution,
) -> Result<bool, LixError> {
    if prepared.explain().is_some() {
        return Ok(false);
    }
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

async fn compile_execution_with_context(
    preparation_context: &dyn SqlPreparationContext,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_account_ids: &[String],
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    policy: PreparationPolicy,
    static_artifacts: StaticCompilationArtifacts<'_>,
) -> Result<CompiledExecution, LixError> {
    let dialect = preparation_context.dialect();
    let functions = preparation_context.functions().clone();

    let mut statements = parsed_statements.to_vec();
    crate::filesystem::ensure_generated_filesystem_insert_ids(&mut statements, &functions)?;
    let explained = if statements.len() == 1 {
        Some(unwrap_explain_statement(&statements[0])?)
    } else {
        None
    };
    let explain_request = explained
        .as_ref()
        .and_then(|explained| explained.request.clone());

    let requirements = static_artifacts
        .plan_requirements
        .cloned()
        .unwrap_or_else(|| derive_plan_requirements(&statements));

    let public_execution = prepare_public_execution_for_compile(
        dialect,
        preparation_context.surface_registry(),
        preparation_context.compiler_metadata(),
        &statements,
        params,
        active_version_id,
        preparation_context.active_history_root_commit_id(),
        active_account_ids,
        writer_key,
        allow_internal_tables,
        static_artifacts.parse_duration,
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
                .is_some_and(|resolved| resolved.filesystem_state().has_binary_payloads())
        });

    let public_read_owns_execution = public_read.is_some();
    let explain_internal_execution =
        explain_request.is_some() && !public_read_owns_execution && public_write.is_none();

    let intent = if let Some(public_write) = public_write.as_ref() {
        derived_public_execution_intent(public_write)
    } else if public_read_owns_execution {
        ExecutionIntent {
            filesystem_state: Default::default(),
        }
    } else if explain_internal_execution {
        ExecutionIntent {
            filesystem_state: Default::default(),
        }
    } else {
        collect_execution_intent(
            &requirements,
            IntentCollectionPolicy {
                skip_side_effect_collection,
            },
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "compile_execution intent collection failed: {}",
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
                        "compile_execution public execution finalization failed: {}",
                        error.description
                    ),
                })?;
        }
    }

    let result_contract = result_contract_for_statements(&statements);
    let mut internal_explain = None;
    let (effects, internal_execution) = if public_write_owns_execution || public_read_owns_execution
    {
        (PlanEffects::default(), None)
    } else {
        let internal_source_statements = explained
            .as_ref()
            .and_then(|explained| {
                explained
                    .request
                    .as_ref()
                    .map(|_| vec![explained.statement.clone()])
            })
            .unwrap_or_else(|| statements.clone());
        let internal_logical_planning_started = Instant::now();
        let internal_logical_plan = preprocess_with_surfaces_to_logical_plan(
            dialect,
            preparation_context.surface_registry(),
            preparation_context.cel_evaluator(),
            internal_source_statements,
            params,
            functions.clone(),
            writer_key,
        )
        .await
        .map_err(LixError::from)
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "compile_execution internal compilation failed: {}",
                error.description
            ),
        })?;
        let internal_logical_planning_duration = internal_logical_planning_started.elapsed();
        let preprocess: PlannedStatementSet =
            internal_logical_plan.normalized_statements.clone().into();
        validate_compiled_internal_execution(&preprocess, internal_logical_plan.result_contract)?;
        let effects = derive_plan_effects(&preprocess, writer_key).map_err(LixError::from)?;
        let internal_execution = CompiledInternalExecution {
            prepared_statements: preprocess.prepared_statements,
            live_table_requirements: preprocess.live_table_requirements,
            mutations: preprocess.mutations,
            update_validations: preprocess.update_validations,
            should_refresh_file_cache: requirements.should_refresh_file_cache,
        };
        if let Some(request) = explain_request.clone() {
            let mut stage_timings = ExplainTimingCollector::new(static_artifacts.parse_duration);
            stage_timings.record(
                ExplainStage::LogicalPlanning,
                internal_logical_planning_duration,
            );
            internal_explain = Some(build_internal_explain_artifacts(
                InternalExplainBuildInput {
                    request,
                    logical_plan: internal_logical_plan.clone(),
                    stage_timings: stage_timings.finish(),
                },
            ));
        }
        (effects, Some(internal_execution))
    };

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
    if let Some(request) = explain_request.as_ref() {
        validate_explain_execution_support(request, &body, requirements.read_only_query)?;
    }
    let physical_plan = match &body {
        CompiledExecutionBody::PublicRead(read) => {
            Some(PhysicalPlan::PublicRead(read.execution.clone()))
        }
        CompiledExecutionBody::PublicWrite(write) => {
            Some(PhysicalPlan::PublicWrite(write.execution.clone()))
        }
        CompiledExecutionBody::Internal(_) => None,
    };
    let explain = match &body {
        CompiledExecutionBody::PublicRead(read) => {
            read.explain.request.as_ref().map(|_| read.explain.clone())
        }
        CompiledExecutionBody::PublicWrite(write) => write
            .explain
            .request
            .as_ref()
            .map(|_| write.explain.clone()),
        CompiledExecutionBody::Internal(_) => internal_explain,
    };

    Ok(CompiledExecution {
        intent,
        physical_plan,
        explain,
        result_contract,
        effects,
        read_only_query: requirements.read_only_query,
        body,
    })
}

async fn prepare_public_execution_for_compile(
    dialect: SqlDialect,
    registry: &crate::contracts::surface::SurfaceRegistry,
    compiler_metadata: &SqlCompilerMetadata,
    statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_history_root_commit_id: Option<&str>,
    active_account_ids: &[String],
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    parse_duration: Option<Duration>,
    ownership_hint: Option<StatementTemplateOwnership>,
) -> Result<Option<PreparedPublicExecution>, LixError> {
    let prepared = match ownership_hint {
        Some(StatementTemplateOwnership::PublicRead) => {
            try_prepare_public_read_with_registry_and_internal_access(
                dialect,
                registry,
                compiler_metadata,
                statements,
                params,
                active_version_id,
                active_history_root_commit_id,
                writer_key,
                allow_internal_tables,
                parse_duration,
            )
            .await?
            .map(PreparedPublicExecution::Read)
        }
        Some(StatementTemplateOwnership::PublicWrite) => {
            try_prepare_public_write_with_registry_and_functions(
                dialect,
                registry,
                statements,
                params,
                active_version_id,
                active_account_ids,
                writer_key,
                parse_duration,
            )
            .await?
            .map(PreparedPublicExecution::Write)
        }
        Some(StatementTemplateOwnership::Internal) => None,
        None => {
            prepare_public_execution_with_registry_context_and_functions(
                dialect,
                registry,
                compiler_metadata,
                statements,
                params,
                active_version_id,
                active_history_root_commit_id,
                active_account_ids,
                writer_key,
                allow_internal_tables,
                parse_duration,
            )
            .await?
        }
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

pub(crate) async fn compile_execution_from_template_instance_with_context(
    preparation_context: &dyn SqlPreparationContext,
    template_instance: &BoundStatementTemplateInstance,
    active_version_id: &str,
    active_account_ids: &[String],
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    policy: PreparationPolicy,
) -> Result<CompiledExecution, LixError> {
    compile_execution_with_context(
        preparation_context,
        std::slice::from_ref(template_instance.statement()),
        template_instance.params(),
        active_version_id,
        active_account_ids,
        writer_key,
        allow_internal_tables,
        policy,
        StaticCompilationArtifacts {
            parse_duration: template_instance.parse_duration(),
            ownership_hint: template_instance.ownership_hint(),
            plan_requirements: Some(template_instance.plan_requirements()),
        },
    )
    .await
}

fn derived_public_execution_intent(
    prepared: &PreparedPublicWrite,
) -> crate::sql::prepare::intent::ExecutionIntent {
    let Some(resolved) = prepared.planned_write.resolved_write_plan.as_ref() else {
        return crate::sql::prepare::intent::ExecutionIntent {
            filesystem_state: Default::default(),
        };
    };

    crate::sql::prepare::intent::ExecutionIntent {
        filesystem_state: resolved.filesystem_state(),
    }
}

fn validate_explain_execution_support(
    request: &ExplainRequest,
    body: &CompiledExecutionBody,
    read_only_query: bool,
) -> Result<(), LixError> {
    if !request.requires_execution() {
        return Ok(());
    }

    match body {
        CompiledExecutionBody::PublicRead(_) => Ok(()),
        CompiledExecutionBody::Internal(_) if read_only_query => Ok(()),
        CompiledExecutionBody::PublicWrite(_) => {
            Err(unsupported_explain_analyze_error("public write statements"))
        }
        CompiledExecutionBody::Internal(_) => Err(unsupported_explain_analyze_error(
            "mutating internal statements",
        )),
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
