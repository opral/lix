use super::super::matcher::statement_matches_any_table;
use sqlparser::ast::Statement;

const TABLE_PATTERNS: &[&str] = &[
    "lix_file",
    "lix_file_by_version",
    "lix_file_history",
    "lix_directory",
    "lix_directory_by_version",
    "lix_directory_history",
];

pub(crate) fn matches(statement: &Statement) -> bool {
    statement_matches_any_table(statement, TABLE_PATTERNS)
}
