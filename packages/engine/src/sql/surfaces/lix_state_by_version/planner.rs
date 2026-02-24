use super::super::matcher::statement_matches_any_table;
use sqlparser::ast::Statement;

const TABLE_PATTERNS: &[&str] = &["lix_state_by_version"];

pub(crate) fn matches(statement: &Statement) -> bool {
    statement_matches_any_table(statement, TABLE_PATTERNS)
}
