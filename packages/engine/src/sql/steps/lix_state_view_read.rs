use sqlparser::ast::{Expr, GroupByExpr, Query, Select, SelectItem, TableFactor, TableWithJoins};

use crate::sql::steps::state_pushdown::{
    select_supports_count_fast_path, take_pushdown_predicates,
};
use crate::sql::steps::vtable_read::build_effective_state_active_query;
use crate::sql::{
    default_alias, expr_references_column_name, object_name_matches,
    rewrite_query_with_select_rewriter, ColumnReferenceOptions,
};
use crate::LixError;

const LIX_STATE_VIEW_NAME: &str = "lix_state";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_with_select_rewriter(query, &mut rewrite_select)
}

fn rewrite_select(select: &mut Select, changed: &mut bool) -> Result<(), LixError> {
    let count_fast_path = select_supports_count_fast_path(select);
    let include_commit_mapping = select_requires_commit_mapping(select);
    let allow_unqualified = select.from.len() == 1 && select.from[0].joins.is_empty();
    for table in &mut select.from {
        rewrite_table_with_joins(
            table,
            &mut select.selection,
            allow_unqualified,
            count_fast_path,
            include_commit_mapping,
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
    include_commit_mapping: bool,
    changed: &mut bool,
) -> Result<(), LixError> {
    rewrite_table_factor(
        &mut table.relation,
        selection,
        allow_unqualified,
        count_fast_path,
        include_commit_mapping,
        changed,
    )?;
    for join in &mut table.joins {
        rewrite_table_factor(
            &mut join.relation,
            selection,
            false,
            false,
            include_commit_mapping,
            changed,
        )?;
    }
    Ok(())
}

fn rewrite_table_factor(
    relation: &mut TableFactor,
    selection: &mut Option<Expr>,
    allow_unqualified: bool,
    count_fast_path: bool,
    include_commit_mapping: bool,
    changed: &mut bool,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if object_name_matches(name, LIX_STATE_VIEW_NAME) =>
        {
            let relation_name = alias
                .as_ref()
                .map(|value| value.name.value.clone())
                .unwrap_or_else(|| LIX_STATE_VIEW_NAME.to_string());
            let pushdown = take_pushdown_predicates(selection, &relation_name, allow_unqualified);
            let derived_query = build_effective_state_active_query(
                &pushdown,
                count_fast_path && selection.is_none(),
                include_commit_mapping,
            )?;
            let derived_alias = alias.clone().or_else(|| Some(default_lix_state_alias()));
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
                include_commit_mapping,
                changed,
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn select_requires_commit_mapping(select: &Select) -> bool {
    // Keep commit mapping enabled for complex query shapes where
    // projection/column inference is less predictable.
    if select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        return true;
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) => {
            if !exprs.is_empty() || !modifiers.is_empty() {
                return true;
            }
        }
        GroupByExpr::All(_) => return true,
    }

    if select
        .projection
        .iter()
        .any(select_item_requires_commit_mapping)
    {
        return true;
    }

    select
        .selection
        .as_ref()
        .is_some_and(expr_requires_commit_mapping)
}

fn select_item_requires_commit_mapping(item: &SelectItem) -> bool {
    match item {
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => true,
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_requires_commit_mapping(expr)
        }
    }
}

fn expr_requires_commit_mapping(expr: &Expr) -> bool {
    let options = ColumnReferenceOptions::default();
    expr_references_column_name(expr, "commit_id", options)
        || expr_references_column_name(expr, "lixcol_commit_id", options)
}

fn default_lix_state_alias() -> sqlparser::ast::TableAlias {
    default_alias(LIX_STATE_VIEW_NAME)
}

#[cfg(test)]
mod tests {
    use super::rewrite_query;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn pushes_file_id_and_plugin_key_filters_into_lix_state_derived_query() {
        let query = parse_query(
            "SELECT COUNT(*) FROM lix_state WHERE file_id = ? AND plugin_key = 'plugin_json'",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("s.file_id = ?"));
        assert!(sql.contains("ranked.plugin_key = 'plugin_json'"));
        assert!(!sql.contains("WHERE file_id = ?"));
        assert!(!sql.contains("commit_by_version"));
        assert!(!sql.contains("change_set_element_by_version"));
        assert!(!sql.contains("change_commit_by_change_id"));
    }

    #[test]
    fn does_not_push_down_bare_placeholders_when_it_would_reorder_bindings() {
        let query =
            parse_query("SELECT COUNT(*) FROM lix_state WHERE plugin_key = ? AND file_id = ?");

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("ranked.plugin_key = ?"));
        assert!(!sql.contains("s.file_id = ?"));
        assert!(sql.contains("plugin_key = ?"));
        assert!(sql.contains("file_id = ?"));
    }

    #[test]
    fn non_count_query_without_commit_id_omits_commit_mapping_ctes() {
        let query = parse_query(
            "SELECT entity_id, untracked \
             FROM lix_state \
             WHERE schema_key = 'lix_file_descriptor' \
               AND entity_id = 'bench-file-1'",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("commit_by_version"));
        assert!(!sql.contains("change_set_element_by_version"));
        assert!(!sql.contains("change_commit_by_change_id"));
    }

    #[test]
    fn non_count_query_with_commit_id_keeps_commit_mapping_ctes() {
        let query = parse_query(
            "SELECT commit_id \
             FROM lix_state \
             WHERE schema_key = 'lix_file_descriptor' \
               AND entity_id = 'bench-file-1'",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("commit_by_version"));
        assert!(sql.contains("change_set_element_by_version"));
        assert!(sql.contains("change_commit_by_change_id"));
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
