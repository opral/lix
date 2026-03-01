use sqlparser::ast::{Expr, GroupByExpr, Query, Select, SelectItem, TableFactor, TableWithJoins};

use crate::engine::sql::planning::param_context::{
    normalize_query_placeholders, PlaceholderOrdinalState,
};
use crate::engine::sql::planning::rewrite_engine::steps::state_pushdown::{
    select_supports_count_fast_path, take_pushdown_predicates,
};
use crate::engine::sql::planning::rewrite_engine::steps::vtable_read::build_effective_state_by_version_query;
use crate::engine::sql::planning::rewrite_engine::{
    default_alias, expr_references_column_name, object_name_matches,
    rewrite_query_with_select_rewriter, ColumnReferenceOptions,
};
use crate::LixError;

const LIX_STATE_BY_VERSION_VIEW_NAME: &str = "lix_state_by_version";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    let mut query = query;
    normalize_query_placeholders(&mut query, &mut PlaceholderOrdinalState::new())?;
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
                include_commit_mapping,
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
            include_commit_mapping,
            changed,
        ),
        _ => Ok(()),
    }
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
    fn canonicalizes_bare_placeholders_and_pushes_down_predicates() {
        let query = parse_query(
            "SELECT COUNT(*) FROM lix_state_by_version AS sv \
             WHERE sv.plugin_key = ? AND sv.file_id = ?",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("ranked.plugin_key = ?1"));
        assert!(sql.contains("s.file_id = ?2"));
        assert!(!sql.contains("sv.plugin_key = ? AND sv.file_id = ?"));
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
                 SELECT lix_json_extract(snapshot_content, 'version_id') \
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
    fn canonicalizes_bare_placeholders_for_schema_and_version() {
        let query = parse_query(
            "SELECT COUNT(*) FROM lix_state_by_version AS sv \
             WHERE sv.schema_key = ? AND sv.version_id = ?",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("s.schema_key = ?1"));
        assert!(sql.contains("version_id = ?2"));
        assert!(!sql.contains("ranked.version_id = ?2"));
        assert!(!sql.contains("sv.schema_key = ?"));
        assert!(!sql.contains("sv.version_id = ?"));
    }

    #[test]
    fn canonicalizes_bare_placeholders_for_version_in_list() {
        let query = parse_query(
            "SELECT COUNT(*) FROM lix_state_by_version AS sv \
             WHERE sv.schema_key = ? AND sv.version_id IN (?, ?)",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("s.schema_key = ?1"));
        assert!(sql.contains("version_id IN ("));
        assert!(!sql.contains("ranked.version_id IN (?2, ?3)"));
        assert!(!sql.contains("sv.schema_key = ?"));
        assert!(!sql.contains("sv.version_id IN (?, ?)"));
    }

    #[test]
    fn non_count_query_without_commit_id_omits_commit_mapping_ctes() {
        let query = parse_query(
            "SELECT sv.entity_id, sv.untracked \
             FROM lix_state_by_version AS sv \
             WHERE sv.schema_key = 'lix_file_descriptor' \
               AND sv.version_id = 'bench-v-023' \
               AND sv.entity_id = 'bench-file-1'",
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
            "SELECT sv.commit_id \
             FROM lix_state_by_version AS sv \
             WHERE sv.schema_key = 'lix_file_descriptor' \
               AND sv.version_id = 'bench-v-023' \
               AND sv.entity_id = 'bench-file-1'",
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
