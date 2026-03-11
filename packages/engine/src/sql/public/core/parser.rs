use super::contracts::StatementKind;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};

pub(crate) fn parse_sql_script(sql: &str) -> Result<Vec<Statement>, ParserError> {
    Parser::parse_sql(&GenericDialect {}, sql)
}

pub(crate) fn statement_kind(statement: &Statement) -> StatementKind {
    match statement {
        Statement::Query(_) => StatementKind::Query,
        Statement::Insert(_) => StatementKind::Insert,
        Statement::Update(_) => StatementKind::Update,
        Statement::Delete(_) => StatementKind::Delete,
        Statement::Explain { .. } | Statement::ExplainTable { .. } => StatementKind::Explain,
        _ => StatementKind::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_sql_script, statement_kind};
    use crate::sql::public::core::contracts::StatementKind;

    #[test]
    fn parses_statement_scripts() {
        let statements = parse_sql_script("SELECT 1; DELETE FROM lix_state WHERE entity_id = 'e1'")
            .expect("script should parse");

        assert_eq!(statements.len(), 2);
        assert_eq!(statement_kind(&statements[0]), StatementKind::Query);
        assert_eq!(statement_kind(&statements[1]), StatementKind::Delete);
    }
}
