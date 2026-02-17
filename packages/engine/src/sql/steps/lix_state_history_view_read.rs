use sqlparser::ast::{Expr, Query, Select, TableFactor, TableWithJoins};

use crate::sql::planner::state_history_read::{
    build_lix_state_history_view_query, take_history_pushdown_predicates,
};
use crate::sql::steps::state_pushdown::select_supports_count_fast_path;
use crate::sql::{default_alias, object_name_matches, rewrite_query_with_select_rewriter};
use crate::LixError;

const LIX_STATE_HISTORY_VIEW_NAME: &str = "lix_state_history";

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
            if object_name_matches(name, LIX_STATE_HISTORY_VIEW_NAME) =>
        {
            let relation_name = alias
                .as_ref()
                .map(|value| value.name.value.clone())
                .unwrap_or_else(|| LIX_STATE_HISTORY_VIEW_NAME.to_string());
            let pushdown =
                take_history_pushdown_predicates(selection, &relation_name, allow_unqualified);
            let derived_query = build_lix_state_history_view_query(
                &pushdown,
                count_fast_path && selection.is_none(),
            )?;
            let derived_alias = alias
                .clone()
                .or_else(|| Some(default_lix_state_history_alias()));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            *changed = true;
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_with_joins(
                table_with_joins,
                selection,
                allow_unqualified,
                count_fast_path,
                changed,
            )?;
        }
        _ => {}
    }
    Ok(())
}
fn default_lix_state_history_alias() -> sqlparser::ast::TableAlias {
    default_alias(LIX_STATE_HISTORY_VIEW_NAME)
}

#[cfg(test)]
mod tests {
    use super::rewrite_query;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn pushes_history_schema_and_root_commit_filters_into_ctes_for_count_fast_path() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM lix_state_history AS sh \
             WHERE sh.schema_key = ? \
               AND sh.root_commit_id = ? \
               AND sh.snapshot_content IS NOT NULL",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("ic.schema_key = ?"));
        assert!(sql.contains("c.id = ?"));
        assert!(!sql.contains("sh.schema_key = ?"));
        assert!(!sql.contains("sh.root_commit_id = ?"));
        assert!(sql.contains("SELECT COUNT(*) AS count FROM ranked_cse ranked WHERE ranked.rn = 1"));
        assert!(!sql.contains("ranked.snapshot_content IS NOT NULL"));
    }

    #[test]
    fn does_not_push_down_bare_placeholders_when_it_would_reorder_bindings() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM lix_state_history AS sh \
             WHERE sh.root_commit_id = ? \
               AND sh.schema_key = ?",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("c.id = ?"));
        assert!(!sql.contains("ic.schema_key = ?"));
        assert!(sql.contains("sh.root_commit_id = ?"));
        assert!(sql.contains("sh.schema_key = ?"));
    }

    #[test]
    fn pushes_numbered_placeholders_even_when_predicate_order_reorders() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM lix_state_history AS sh \
             WHERE sh.root_commit_id = ?1 \
               AND sh.schema_key = ?2",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("c.id = ?1"));
        assert!(sql.contains("ic.schema_key = ?2"));
        assert!(!sql.contains("sh.root_commit_id = ?1"));
        assert!(!sql.contains("sh.schema_key = ?2"));
        assert!(sql.contains("SELECT COUNT(*) AS count FROM ranked_cse ranked WHERE ranked.rn = 1"));
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
