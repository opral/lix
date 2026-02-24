use super::super::super::ast::nodes::Statement;
use super::super::matcher::statement_matches_any_table;

const TABLE_PATTERNS: &[&str] = &["lix_state"];

pub(crate) fn matches(statement: &Statement) -> bool {
    statement_matches_any_table(statement, TABLE_PATTERNS)
}
