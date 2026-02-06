use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::cel::CelEvaluator;
use crate::default_values::apply_vtable_insert_defaults;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
use crate::sql::materialize_vtable_insert_select_sources;
use crate::sql::route::rewrite_statement;
use crate::sql::steps::inline_lix_functions::inline_lix_functions_with_provider;
use crate::sql::types::{PostprocessPlan, PreprocessOutput, SchemaRegistration};
use crate::{LixBackend, LixError, Value};

pub fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, LixError> {
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|err| LixError {
        message: err.to_string(),
    })
}

pub fn preprocess_statements(
    statements: Vec<Statement>,
    params: &[Value],
) -> Result<PreprocessOutput, LixError> {
    let mut provider = SystemFunctionProvider;
    preprocess_statements_with_provider(statements, params, &mut provider)
}

pub fn preprocess_statements_with_provider<P: LixFunctionProvider>(
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
) -> Result<PreprocessOutput, LixError> {
    let mut registrations: Vec<SchemaRegistration> = Vec::new();
    let mut postprocess: Option<PostprocessPlan> = None;
    let mut rewritten = Vec::with_capacity(statements.len());
    let mut mutations = Vec::new();
    let mut update_validations = Vec::new();
    for statement in statements {
        let output = rewrite_statement(statement, params, provider)?;
        registrations.extend(output.registrations);
        if let Some(plan) = output.postprocess {
            if postprocess.is_some() {
                return Err(LixError {
                    message: "only one postprocess rewrite is supported per query".to_string(),
                });
            }
            postprocess = Some(plan);
        }
        mutations.extend(output.mutations);
        update_validations.extend(output.update_validations);
        for rewritten_statement in output.statements {
            rewritten.push(inline_lix_functions_with_provider(
                rewritten_statement,
                provider,
            ));
        }
    }

    if postprocess.is_some() && rewritten.len() != 1 {
        return Err(LixError {
            message: "postprocess rewrites require a single statement".to_string(),
        });
    }

    let normalized_sql = rewritten
        .iter()
        .map(|statement| statement.to_string())
        .collect::<Vec<_>>()
        .join("; ");

    let params = if sql_contains_placeholders(&normalized_sql) {
        params.to_vec()
    } else {
        Vec::new()
    };

    Ok(PreprocessOutput {
        sql: normalized_sql,
        params,
        registrations,
        postprocess,
        mutations,
        update_validations,
    })
}

#[allow(dead_code)]
pub async fn preprocess_sql(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql: &str,
    params: &[Value],
) -> Result<PreprocessOutput, LixError> {
    let functions = SharedFunctionProvider::new(SystemFunctionProvider);
    preprocess_sql_with_provider(backend, evaluator, sql, params, functions).await
}

pub async fn preprocess_sql_with_provider<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql: &str,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
) -> Result<PreprocessOutput, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let params = params.to_vec();
    let mut statements = parse_sql_statements(sql)?;
    materialize_vtable_insert_select_sources(backend, &mut statements, &params).await?;
    apply_vtable_insert_defaults(
        backend,
        evaluator,
        &mut statements,
        &params,
        functions.clone(),
    )
    .await?;
    let mut provider = functions.clone();
    preprocess_statements_with_provider(statements, &params, &mut provider)
}

#[allow(dead_code)]
pub fn preprocess_sql_rewrite_only(sql: &str) -> Result<PreprocessOutput, LixError> {
    preprocess_statements(parse_sql_statements(sql)?, &[])
}

fn sql_contains_placeholders(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut in_single_quoted = false;

    while i < bytes.len() {
        let byte = bytes[i];
        if in_single_quoted {
            if byte == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_single_quoted = false;
            }
            i += 1;
            continue;
        }

        if byte == b'\'' {
            in_single_quoted = true;
            i += 1;
            continue;
        }

        if byte == b'?' {
            return true;
        }

        if byte == b'$' {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 {
                return true;
            }
        }

        i += 1;
    }

    false
}
