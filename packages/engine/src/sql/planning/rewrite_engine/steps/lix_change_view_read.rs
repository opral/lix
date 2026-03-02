use sqlparser::ast::{Query, Select, TableFactor};

use crate::engine::sql::planning::rewrite_engine::{
    default_alias, object_name_matches, parse_single_query, rewrite_query_with_select_rewriter,
    rewrite_table_factors_in_select,
};
use crate::LixError;

const LIX_CHANGE_VIEW_NAME: &str = "lix_change";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_with_select_rewriter(query, &mut rewrite_select)
}

fn rewrite_select(select: &mut Select, changed: &mut bool) -> Result<(), LixError> {
    rewrite_table_factors_in_select(select, &mut rewrite_table_factor, changed)
}

fn rewrite_table_factor(relation: &mut TableFactor, changed: &mut bool) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if object_name_matches(name, LIX_CHANGE_VIEW_NAME) =>
        {
            let derived_query = build_lix_change_view_query()?;
            let derived_alias = alias.clone().or_else(|| Some(default_lix_change_alias()));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            *changed = true;
        }
        _ => {}
    }
    Ok(())
}

fn build_lix_change_view_query() -> Result<Query, LixError> {
    parse_single_query(
        "SELECT \
            ch.id AS id, \
            ch.entity_id AS entity_id, \
            ch.schema_key AS schema_key, \
            ch.schema_version AS schema_version, \
            ch.file_id AS file_id, \
            ch.plugin_key AS plugin_key, \
            ch.metadata AS metadata, \
            ch.created_at AS created_at, \
            CASE \
                WHEN ch.snapshot_id = 'no-content' THEN NULL \
                ELSE s.content \
            END AS snapshot_content \
         FROM lix_internal_change ch \
         LEFT JOIN lix_internal_snapshot s \
            ON s.id = ch.snapshot_id",
    )
}

fn default_lix_change_alias() -> sqlparser::ast::TableAlias {
    default_alias(LIX_CHANGE_VIEW_NAME)
}

#[cfg(test)]
mod tests {
    use super::rewrite_query;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn rewrites_lix_change_to_derived_query() {
        let query = parse_query(
            "SELECT id, schema_key, snapshot_content \
             FROM lix_change \
             WHERE entity_id = 'entity-1'",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("FROM (SELECT ch.id AS id"));
        assert!(sql.contains("FROM lix_internal_change ch"));
        assert!(sql.contains("LEFT JOIN lix_internal_snapshot s"));
        assert!(!sql.contains("FROM lix_change"));
        assert!(sql.contains("WHERE entity_id = 'entity-1'"));
    }

    #[test]
    fn preserves_explicit_alias() {
        let query = parse_query("SELECT c.id FROM lix_change AS c");
        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();
        assert!(sql.contains(") AS c"));
    }

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            other => panic!("expected query, got {other:?}"),
        }
    }
}
