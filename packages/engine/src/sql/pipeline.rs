use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::sql::route::rewrite_statement;
use crate::sql::types::{PreprocessOutput, SchemaRegistration};
use crate::LixError;

pub fn preprocess_sql(sql: &str) -> Result<PreprocessOutput, LixError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|err| LixError {
        message: err.to_string(),
    })?;

    let mut registrations: Vec<SchemaRegistration> = Vec::new();
    let mut rewritten = Vec::with_capacity(statements.len());
    for statement in statements {
        let output = rewrite_statement(statement)?;
        registrations.extend(output.registrations);
        rewritten.push(output.statement);
    }

    let normalized_sql = rewritten
        .iter()
        .map(|statement| statement.to_string())
        .collect::<Vec<_>>()
        .join("; ");

    Ok(PreprocessOutput {
        sql: normalized_sql,
        registrations,
    })
}
