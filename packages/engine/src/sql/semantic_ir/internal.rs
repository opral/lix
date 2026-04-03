//! Semantic ownership for normalized internal statement batches.
//!
//! This stage owns the front-end normalization output for internal SQL after
//! parse/bind but before later planning and execution-specific wrapping.

use crate::backend::prepared::PreparedStatement;
use crate::cel::CelEvaluator;
use crate::functions::LixFunctionProvider;
use crate::functions::SharedFunctionProvider;
use crate::sql::ast::lowering::lower_statement;
use crate::sql::internal::inline_functions::inline_lix_functions_with_provider;
use crate::sql::internal::param_context::normalize_statement_placeholders_in_batch;
pub(crate) use crate::sql::parser::placeholders::PlaceholderState;
use crate::sql::prepare::contracts::planned_statement::{
    MutationRow, PlannedStatementSet, SchemaLiveTableRequirement, UpdateValidationPlan,
};
use crate::{LixBackend, LixError, SqlDialect, Value};
use serde_json::Value as JsonValue;
use sqlparser::ast::Statement;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InternalStatementRewrite {
    pub(crate) statements: Vec<Statement>,
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) effect_only: bool,
    pub(crate) params: Vec<Value>,
    pub(crate) live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NormalizedInternalStatements {
    pub(crate) sql: String,
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

#[derive(Debug, Clone, PartialEq)]
struct StatementRewriteOutput {
    statements: Vec<Statement>,
    prepared_statements: Vec<PreparedStatement>,
    params: Vec<Value>,
    live_table_requirements: Vec<SchemaLiveTableRequirement>,
    mutations: Vec<MutationRow>,
    update_validations: Vec<UpdateValidationPlan>,
}

#[derive(Debug, Clone)]
enum RewrittenStatementBinding {
    Ast {
        statement: Statement,
        appended_params: Arc<Vec<Value>>,
    },
    Prepared(PreparedStatement),
}

impl NormalizedInternalStatements {
    pub(crate) fn semantic_statement(&self) -> super::public::SemanticStatement {
        super::public::SemanticStatement::Internal(self.clone())
    }
}

impl From<NormalizedInternalStatements> for PlannedStatementSet {
    fn from(output: NormalizedInternalStatements) -> Self {
        let _ = output.semantic_statement();
        Self {
            sql: output.sql,
            prepared_statements: output.prepared_statements,
            live_table_requirements: output.live_table_requirements,
            mutations: output.mutations,
            update_validations: output.update_validations,
        }
    }
}

pub(crate) async fn rewrite_internal_statement_with_backend<P>(
    _backend: &dyn LixBackend,
    statement: Statement,
    _params: &[Value],
    _writer_key: Option<&str>,
    _known_live_schema_definitions: &BTreeMap<String, JsonValue>,
    _provider: &mut P,
) -> Result<InternalStatementRewrite, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    let output = passthrough_output(statement);
    validate_statement_output(&output)?;
    Ok(output)
}

fn passthrough_output(statement: Statement) -> InternalStatementRewrite {
    InternalStatementRewrite {
        statements: vec![statement],
        prepared_statements: Vec::new(),
        effect_only: false,
        params: Vec::new(),
        live_table_requirements: Vec::new(),
        mutations: Vec::new(),
        update_validations: Vec::new(),
    }
}

fn validate_statement_output(output: &InternalStatementRewrite) -> Result<(), LixError> {
    if output.statements.is_empty()
        && output.prepared_statements.is_empty()
        && !(output.effect_only
            && output.mutations.is_empty()
            && output.update_validations.is_empty())
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "statement rewrite produced no statements".to_string(),
        });
    }
    if !output.mutations.is_empty() && !output.update_validations.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "mutation rewrites cannot emit update validations".to_string(),
        });
    }
    if !output.update_validations.is_empty()
        && !output.statements.iter().all(|statement| {
            matches!(
                statement,
                sqlparser::ast::Statement::Update(_) | sqlparser::ast::Statement::Delete(_)
            )
        })
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "update validations require an UPDATE or DELETE statement output"
                .to_string(),
        });
    }
    Ok(())
}

pub(crate) async fn prepare_internal_statements_with_backend_to_plan<P>(
    backend: &dyn LixBackend,
    _evaluator: &CelEvaluator,
    statements: Vec<Statement>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    writer_key: Option<&str>,
) -> Result<NormalizedInternalStatements, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let mut statements = statements;
    normalize_statement_placeholders_in_batch(&mut statements)?;
    let mut provider = functions.clone();
    prepare_rewritten_statements_with_backend(
        backend,
        statements,
        params,
        &mut provider,
        writer_key,
    )
    .await
}

async fn prepare_rewritten_statements_with_backend<P>(
    backend: &dyn LixBackend,
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    writer_key: Option<&str>,
) -> Result<NormalizedInternalStatements, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    let mut rewritten = Vec::with_capacity(statements.len());
    let mut live_table_requirements: Vec<SchemaLiveTableRequirement> = Vec::new();
    let mut known_live_schema_definitions = BTreeMap::<String, JsonValue>::new();
    let mut mutations: Vec<MutationRow> = Vec::new();
    let mut update_validations: Vec<UpdateValidationPlan> = Vec::new();

    for (statement_index, statement) in statements.into_iter().enumerate() {
        let output = Box::pin(rewrite_internal_statement_with_backend(
            backend,
            statement,
            params,
            writer_key,
            &known_live_schema_definitions,
            provider,
        ))
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "prepare_internal_statements_with_backend_to_plan backend rewrite failed for statement {}: {}",
                statement_index, error.description
            ),
        })?;

        accumulate_rewrite_output(
            from_internal_rewrite(output),
            provider,
            backend.dialect(),
            &mut rewritten,
            &mut live_table_requirements,
            &mut mutations,
            &mut update_validations,
        )?;
        for requirement in &live_table_requirements {
            if let Some(schema_definition) = requirement.schema_definition.as_ref() {
                known_live_schema_definitions
                    .insert(requirement.schema_key.clone(), schema_definition.clone());
            }
        }
    }

    let (normalized_sql, prepared_statements) =
        render_statements_with_params(&rewritten, params, backend.dialect())?;

    Ok(NormalizedInternalStatements {
        sql: normalized_sql,
        prepared_statements,
        live_table_requirements,
        mutations,
        update_validations,
    })
}

fn accumulate_rewrite_output<P: LixFunctionProvider>(
    output: StatementRewriteOutput,
    provider: &mut P,
    dialect: SqlDialect,
    rewritten: &mut Vec<RewrittenStatementBinding>,
    live_table_requirements: &mut Vec<SchemaLiveTableRequirement>,
    mutations: &mut Vec<MutationRow>,
    update_validations: &mut Vec<UpdateValidationPlan>,
) -> Result<(), LixError> {
    live_table_requirements.extend(output.live_table_requirements);
    mutations.extend(output.mutations);
    update_validations.extend(output.update_validations);

    let appended_params = Arc::new(output.params);
    for prepared in output.prepared_statements {
        rewritten.push(RewrittenStatementBinding::Prepared(prepared));
    }
    for statement in output.statements {
        let inlined = inline_lix_functions_with_provider(statement, provider);
        rewritten.push(RewrittenStatementBinding::Ast {
            statement: lower_statement(inlined, dialect)?,
            appended_params: Arc::clone(&appended_params),
        });
    }

    Ok(())
}

fn render_statements_with_params(
    statements: &[RewrittenStatementBinding],
    base_params: &[Value],
    dialect: SqlDialect,
) -> Result<(String, Vec<PreparedStatement>), LixError> {
    let mut rendered = Vec::with_capacity(statements.len());
    let mut prepared_statements = Vec::with_capacity(statements.len());
    let mut placeholder_state = PlaceholderState::new();

    for statement in statements {
        match statement {
            RewrittenStatementBinding::Prepared(prepared) => {
                rendered.push(prepared.sql.clone());
                prepared_statements.push(prepared.clone());
            }
            RewrittenStatementBinding::Ast {
                statement,
                appended_params,
            } => {
                let bound = crate::sql::binder::bind_sql_with_state_and_appended_params(
                    &statement.to_string(),
                    base_params,
                    appended_params.as_slice(),
                    dialect,
                    placeholder_state,
                )?;
                placeholder_state = bound.state;
                rendered.push(bound.sql.clone());
                prepared_statements.push(PreparedStatement {
                    sql: bound.sql,
                    params: bound.params,
                });
            }
        }
    }

    Ok((rendered.join("; "), prepared_statements))
}

fn from_internal_rewrite(output: InternalStatementRewrite) -> StatementRewriteOutput {
    StatementRewriteOutput {
        statements: output.statements,
        prepared_statements: output.prepared_statements,
        params: output.params,
        live_table_requirements: output.live_table_requirements,
        mutations: output.mutations,
        update_validations: output.update_validations,
    }
}
