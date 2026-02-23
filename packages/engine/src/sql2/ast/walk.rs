#[cfg(test)]
use super::nodes::Statement;
#[cfg(test)]
use super::utils::is_transaction_control_statement;

#[cfg(test)]
pub(crate) fn contains_transaction_control_statement(statements: &[Statement]) -> bool {
    statements.iter().any(is_transaction_control_statement)
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::contains_transaction_control_statement;

    #[test]
    fn detects_transaction_control_statements() {
        let statements =
            Parser::parse_sql(&GenericDialect {}, "BEGIN; SELECT 1; COMMIT;").expect("parse SQL");
        assert!(contains_transaction_control_statement(&statements));
    }

    #[test]
    fn ignores_non_transaction_control_statements() {
        let statements =
            Parser::parse_sql(&GenericDialect {}, "SELECT 1; SELECT 2;").expect("parse SQL");
        assert!(!contains_transaction_control_statement(&statements));
    }
}
