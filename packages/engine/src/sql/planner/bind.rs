use sqlparser::ast::Statement;

use crate::sql::extract_explicit_transaction_script_from_statements;
use crate::LixError;

use super::types::StatementBlock;

pub(crate) fn prepare_statement_block_with_transaction_flag(
    statements: Vec<Statement>,
) -> Result<StatementBlock, LixError> {
    let (statements, explicit_transaction_script) =
        if let Some(inner) = extract_explicit_transaction_script_from_statements(&statements)? {
            (inner, true)
        } else {
            (statements, false)
        };

    Ok(StatementBlock {
        statements,
        explicit_transaction_script,
    })
}

#[cfg(test)]
mod tests {
    use super::prepare_statement_block_with_transaction_flag;
    use crate::sql::parse_sql_statements;

    #[test]
    fn unwraps_begin_commit_scripts_before_execution_planning() {
        let statements =
            parse_sql_statements("BEGIN; SELECT ?; SELECT ?; COMMIT;").expect("parse SQL");
        let block = prepare_statement_block_with_transaction_flag(statements)
            .expect("prepare statement block");

        assert!(block.explicit_transaction_script);
        assert_eq!(block.statements.len(), 2);
    }

    #[test]
    fn keeps_non_script_statement_blocks_unchanged() {
        let statements = parse_sql_statements("SELECT ?; SELECT ?").expect("parse SQL");
        let block = prepare_statement_block_with_transaction_flag(statements)
            .expect("prepare statement block");

        assert!(!block.explicit_transaction_script);
        assert_eq!(block.statements.len(), 2);
    }

    #[test]
    fn rejects_nested_transaction_statements_inside_scripts() {
        let statements =
            parse_sql_statements("BEGIN; SELECT 1; ROLLBACK; COMMIT;").expect("parse SQL");

        let error = prepare_statement_block_with_transaction_flag(statements)
            .expect_err("nested transaction script should fail");
        assert!(
            error
                .message
                .contains("nested transaction statements are not supported"),
            "unexpected message: {}",
            error.message
        );
    }
}
