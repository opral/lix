use sqlparser::ast::{Ident, ObjectName, ObjectNamePart, Query, Statement, TableAlias};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::LixError;

pub(crate) fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}

pub(crate) fn default_alias(name: &str) -> TableAlias {
    TableAlias {
        explicit: false,
        name: Ident::new(name),
        columns: Vec::new(),
    }
}

pub(crate) fn parse_single_query(sql: &str) -> Result<Query, LixError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        message: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "expected a single SELECT statement".to_string(),
        });
    }
    let statement = statements.remove(0);
    match statement {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            message: "expected SELECT statement".to_string(),
        }),
    }
}

pub(crate) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
