use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::sql::route::rewrite_statement;
use crate::sql::steps::inline_lix_functions::inline_lix_functions;
use crate::sql::types::{PostprocessPlan, PreprocessOutput, SchemaRegistration};
use crate::LixError;

pub fn preprocess_sql(sql: &str) -> Result<PreprocessOutput, LixError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|err| LixError {
        message: err.to_string(),
    })?;

    let mut registrations: Vec<SchemaRegistration> = Vec::new();
    let mut postprocess: Option<PostprocessPlan> = None;
    let mut rewritten = Vec::with_capacity(statements.len());
    for statement in statements {
        let output = rewrite_statement(statement)?;
        registrations.extend(output.registrations);
        if let Some(plan) = output.postprocess {
            if postprocess.is_some() {
                return Err(LixError {
                    message: "only one postprocess rewrite is supported per query".to_string(),
                });
            }
            postprocess = Some(plan);
        }
        for rewritten_statement in output.statements {
            rewritten.push(inline_lix_functions(rewritten_statement));
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

    Ok(PreprocessOutput {
        sql: normalized_sql,
        registrations,
        postprocess,
    })
}
