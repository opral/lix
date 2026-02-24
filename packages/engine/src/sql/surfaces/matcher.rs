use std::ops::ControlFlow;

use sqlparser::ast::visit_relations;

use super::super::ast::nodes::Statement;
use super::super::ast::walk::object_name_matches;

pub(crate) fn statement_matches_any_table(statement: &Statement, table_names: &[&str]) -> bool {
    if table_names.is_empty() {
        return false;
    }

    let visit_result = visit_relations(statement, |relation| {
        if table_names
            .iter()
            .any(|table_name| object_name_matches(relation, table_name))
        {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });

    matches!(visit_result, ControlFlow::Break(()))
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::statement_matches_any_table;

    #[test]
    fn matches_relation_in_query() {
        let statements = Parser::parse_sql(&GenericDialect {}, "SELECT * FROM lix_state")
            .expect("parse SQL");
        assert!(statement_matches_any_table(&statements[0], &["lix_state"]));
    }

    #[test]
    fn matches_relation_in_mutation_target() {
        let statements =
            Parser::parse_sql(&GenericDialect {}, "UPDATE lix_state SET key = 'x' WHERE key = 'y'")
                .expect("parse SQL");
        assert!(statement_matches_any_table(&statements[0], &["lix_state"]));
    }

    #[test]
    fn ignores_relation_name_in_string_literal() {
        let statements = Parser::parse_sql(
            &GenericDialect {},
            "SELECT 'lix_state' AS marker FROM unrelated_table",
        )
        .expect("parse SQL");
        assert!(!statement_matches_any_table(&statements[0], &["lix_state"]));
    }
}
