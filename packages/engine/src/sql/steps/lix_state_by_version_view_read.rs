use sqlparser::ast::{Expr, Query, Select, TableFactor, TableWithJoins};

use crate::sql::planner::effective_state_read::build_effective_state_by_version_query;
use crate::sql::steps::state_pushdown::{
    select_supports_count_fast_path, take_pushdown_predicates,
};
use crate::sql::{default_alias, object_name_matches, rewrite_query_with_select_rewriter};
use crate::LixError;

const LIX_STATE_BY_VERSION_VIEW_NAME: &str = "lix_state_by_version";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_with_select_rewriter(query, &mut rewrite_select)
}

fn rewrite_select(select: &mut Select, changed: &mut bool) -> Result<(), LixError> {
    let count_fast_path = select_supports_count_fast_path(select);
    let allow_unqualified = select.from.len() == 1 && select.from[0].joins.is_empty();
    for table in &mut select.from {
        rewrite_table_with_joins(
            table,
            &mut select.selection,
            allow_unqualified,
            count_fast_path,
            changed,
        )?;
    }
    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    selection: &mut Option<Expr>,
    allow_unqualified: bool,
    count_fast_path: bool,
    changed: &mut bool,
) -> Result<(), LixError> {
    rewrite_table_factor(
        &mut table.relation,
        selection,
        allow_unqualified,
        count_fast_path,
        changed,
    )?;
    for join in &mut table.joins {
        rewrite_table_factor(&mut join.relation, selection, false, false, changed)?;
    }
    Ok(())
}

fn rewrite_table_factor(
    relation: &mut TableFactor,
    selection: &mut Option<Expr>,
    allow_unqualified: bool,
    count_fast_path: bool,
    changed: &mut bool,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if object_name_matches(name, LIX_STATE_BY_VERSION_VIEW_NAME) =>
        {
            let relation_name = alias
                .as_ref()
                .map(|value| value.name.value.clone())
                .unwrap_or_else(|| LIX_STATE_BY_VERSION_VIEW_NAME.to_string());
            let pushdown = take_pushdown_predicates(selection, &relation_name, allow_unqualified);
            let derived_query = build_effective_state_by_version_query(
                &pushdown,
                count_fast_path && selection.is_none(),
            )?;
            let derived_alias = alias
                .clone()
                .or_else(|| Some(default_lix_state_by_version_alias()));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            *changed = true;
            Ok(())
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => rewrite_table_with_joins(
            table_with_joins,
            selection,
            allow_unqualified,
            count_fast_path,
            changed,
        ),
        _ => Ok(()),
    }
}

fn default_lix_state_by_version_alias() -> sqlparser::ast::TableAlias {
    default_alias(LIX_STATE_BY_VERSION_VIEW_NAME)
}

#[cfg(test)]
mod tests {
    use super::rewrite_query;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn pushes_alias_qualified_filters_into_lix_state_by_version_derived_query() {
        let query = parse_query(
            "SELECT COUNT(*) FROM lix_state_by_version AS sv \
             WHERE sv.file_id = ?1 AND sv.plugin_key = 'plugin_json'",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("s.file_id = ?1"));
        assert!(sql.contains("ranked.plugin_key = 'plugin_json'"));
        assert!(!sql.contains("sv.file_id = ?1"));
        assert!(!sql.contains("commit_by_version"));
        assert!(!sql.contains("change_set_element_by_version"));
        assert!(!sql.contains("change_commit_by_change_id"));
    }

    #[test]
    fn does_not_push_down_bare_placeholders_when_it_would_reorder_bindings() {
        let query = parse_query(
            "SELECT COUNT(*) FROM lix_state_by_version AS sv \
             WHERE sv.plugin_key = ? AND sv.file_id = ?",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("ranked.plugin_key = ?"));
        assert!(!sql.contains("s.file_id = ?"));
        assert!(sql.contains("sv.plugin_key = ?"));
        assert!(sql.contains("sv.file_id = ?"));
    }

    #[test]
    fn pushes_version_id_eq_into_lix_state_by_version_source() {
        let query = parse_query(
            "SELECT COUNT(*) FROM lix_state_by_version AS sv \
             WHERE sv.schema_key = 'lix_file_descriptor' AND sv.version_id = 'bench-v-023'",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("FROM version_descriptor WHERE version_id = 'bench-v-023'"));
        assert!(sql.contains("FROM lix_internal_state_vtable WHERE version_id = 'bench-v-023'"));
        assert!(!sql.contains("FROM all_target_versions"));
        assert!(!sql.contains("ranked.version_id = 'bench-v-023'"));
        assert!(!sql.contains("sv.version_id = 'bench-v-023'"));
    }

    #[test]
    fn pushes_version_id_in_list_into_lix_state_by_version_source() {
        let query = parse_query(
            "SELECT COUNT(*) FROM lix_state_by_version AS sv \
             WHERE sv.schema_key = 'lix_file_descriptor' \
               AND sv.version_id IN ('bench-v-022', 'bench-v-023')",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains(
            "FROM version_descriptor WHERE version_id IN ('bench-v-022', 'bench-v-023')"
        ));
        assert!(sql.contains(
            "FROM lix_internal_state_vtable WHERE version_id IN ('bench-v-022', 'bench-v-023')"
        ));
        assert!(!sql.contains("FROM all_target_versions"));
        assert!(!sql.contains("ranked.version_id IN ('bench-v-022', 'bench-v-023')"));
        assert!(!sql.contains("sv.version_id IN ('bench-v-022', 'bench-v-023')"));
    }

    #[test]
    fn pushes_version_id_in_subquery_into_lix_state_by_version_source() {
        let query = parse_query(
            "SELECT COUNT(*) FROM lix_state_by_version AS sv \
             WHERE sv.schema_key = 'lix_file_descriptor' \
               AND sv.version_id IN ( \
                 SELECT lix_json_text(snapshot_content, 'version_id') \
                 FROM lix_internal_state_untracked \
                 WHERE schema_key = 'lix_version_pointer' \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
               )",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("FROM all_target_versions WHERE version_id IN (SELECT"));
        assert!(!sql.contains("ranked.version_id IN (SELECT"));
        assert!(sql.contains("FROM lix_internal_state_untracked"));
        assert!(!sql.contains("sv.version_id IN (SELECT"));
    }

    #[test]
    fn pushes_safe_bare_placeholders_for_schema_and_version() {
        let query = parse_query(
            "SELECT COUNT(*) FROM lix_state_by_version AS sv \
             WHERE sv.schema_key = ? AND sv.version_id = ?",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("s.schema_key = ?"));
        assert!(sql.contains("version_id = ?"));
        assert!(!sql.contains("ranked.version_id = ?"));
        assert!(!sql.contains("sv.schema_key = ?"));
        assert!(!sql.contains("sv.version_id = ?"));
    }

    #[test]
    fn pushes_safe_bare_placeholders_for_version_in_list() {
        let query = parse_query(
            "SELECT COUNT(*) FROM lix_state_by_version AS sv \
             WHERE sv.schema_key = ? AND sv.version_id IN (?, ?)",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("s.schema_key = ?"));
        assert!(sql.contains("version_id IN ("));
        assert!(!sql.contains("ranked.version_id IN (?, ?)"));
        assert!(!sql.contains("sv.schema_key = ?"));
        assert!(!sql.contains("sv.version_id IN (?, ?)"));
    }

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("valid SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            _ => panic!("expected query"),
        }
    }
}
