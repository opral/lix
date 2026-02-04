use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::sql::route::rewrite_statement;
use crate::LixError;

pub fn preprocess_sql(sql: &str) -> Result<String, LixError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|err| LixError {
        message: err.to_string(),
    })?;

    let mut rewritten = Vec::with_capacity(statements.len());
    for statement in statements {
        rewritten.push(rewrite_statement(statement)?);
    }

    let normalized_sql = rewritten
        .iter()
        .map(|statement| statement.to_string())
        .collect::<Vec<_>>()
        .join("; ");

    Ok(normalized_sql)
}
