use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, Select, SelectItem, TableFactor,
    TableWithJoins,
};

use crate::engine::sql::planning::param_context::{
    normalize_query_placeholders, PlaceholderOrdinalState,
};
use crate::engine::sql::planning::rewrite_engine::steps::state_pushdown::{
    take_pushdown_predicates, StatePushdown,
};
use crate::engine::sql::planning::rewrite_engine::{
    default_alias, escape_sql_string, object_name_matches, parse_single_query,
    rewrite_query_with_select_rewriter,
};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
};
use crate::LixError;

const LIX_STATE_VIEW_NAME: &str = "lix_state";
const LIX_STATE_BY_VERSION_VIEW_NAME: &str = "lix_state_by_version";
const ACTIVE_VERSION_TABLE: &str = "lix_internal_state_untracked";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    let mut query = query;
    normalize_query_placeholders(&mut query, &mut PlaceholderOrdinalState::new())?;
    rewrite_query_with_select_rewriter(query, &mut rewrite_select)
}

fn rewrite_select(select: &mut Select, changed: &mut bool) -> Result<(), LixError> {
    let allow_unqualified = select.from.len() == 1 && select.from[0].joins.is_empty();
    let projection = select.projection.clone();
    let mut selection = select.selection.clone();
    let prewhere = select.prewhere.clone();
    let having = select.having.clone();
    let qualify = select.qualify.clone();
    for table in &mut select.from {
        rewrite_table_with_joins(
            table,
            &projection,
            &mut selection,
            prewhere.as_ref(),
            having.as_ref(),
            qualify.as_ref(),
            allow_unqualified,
            changed,
        )?;
    }
    select.selection = selection;
    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    projection: &[SelectItem],
    selection: &mut Option<Expr>,
    prewhere: Option<&Expr>,
    having: Option<&Expr>,
    qualify: Option<&Expr>,
    allow_unqualified: bool,
    changed: &mut bool,
) -> Result<(), LixError> {
    rewrite_table_factor(
        &mut table.relation,
        projection,
        selection,
        prewhere,
        having,
        qualify,
        allow_unqualified,
        changed,
    )?;
    for join in &mut table.joins {
        rewrite_table_factor(
            &mut join.relation,
            projection,
            selection,
            prewhere,
            having,
            qualify,
            false,
            changed,
        )?;
    }
    Ok(())
}

fn rewrite_table_factor(
    relation: &mut TableFactor,
    projection: &[SelectItem],
    selection: &mut Option<Expr>,
    prewhere: Option<&Expr>,
    having: Option<&Expr>,
    qualify: Option<&Expr>,
    allow_unqualified: bool,
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
            validate_no_version_column_references(
                projection,
                selection.as_ref(),
                prewhere,
                having,
                qualify,
                &relation_name,
                allow_unqualified,
            )?;
            let pushdown_predicates = if allow_unqualified {
                let pushdown =
                    take_pushdown_predicates(selection, &relation_name, allow_unqualified);
                wrapper_pushdown_predicates(&pushdown)
            } else {
                Vec::new()
            };
            let derived_query = build_lix_state_wrapper_query(&pushdown_predicates)?;
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
                projection,
                selection,
                prewhere,
                having,
                qualify,
                allow_unqualified,
                changed,
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_no_version_column_references(
    projection: &[SelectItem],
    selection: Option<&Expr>,
    prewhere: Option<&Expr>,
    having: Option<&Expr>,
    qualify: Option<&Expr>,
    relation_name: &str,
    allow_unqualified: bool,
) -> Result<(), LixError> {
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                if expr_references_lix_state_version_column(expr, relation_name, allow_unqualified)
                {
                    return Err(version_column_read_error());
                }
            }
            SelectItem::QualifiedWildcard(
                sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
                _,
            ) => {
                if expr_references_lix_state_version_column(expr, relation_name, allow_unqualified)
                {
                    return Err(version_column_read_error());
                }
            }
            _ => {}
        }
    }

    for expr in [selection, prewhere, having, qualify].into_iter().flatten() {
        if expr_references_lix_state_version_column(expr, relation_name, allow_unqualified) {
            return Err(version_column_read_error());
        }
    }

    Ok(())
}

fn expr_references_lix_state_version_column(
    expr: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
) -> bool {
    match expr {
        Expr::Identifier(ident) => {
            allow_unqualified
                && (ident.value.eq_ignore_ascii_case("version_id")
                    || ident.value.eq_ignore_ascii_case("lixcol_version_id"))
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = &parts[parts.len() - 2].value;
            let column = &parts[parts.len() - 1].value;
            qualifier.eq_ignore_ascii_case(relation_name)
                && (column.eq_ignore_ascii_case("version_id")
                    || column.eq_ignore_ascii_case("lixcol_version_id"))
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_references_lix_state_version_column(left, relation_name, allow_unqualified)
                || expr_references_lix_state_version_column(right, relation_name, allow_unqualified)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Nested(expr) => {
            expr_references_lix_state_version_column(expr, relation_name, allow_unqualified)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_lix_state_version_column(expr, relation_name, allow_unqualified)
                || expr_references_lix_state_version_column(low, relation_name, allow_unqualified)
                || expr_references_lix_state_version_column(high, relation_name, allow_unqualified)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            expr_references_lix_state_version_column(expr, relation_name, allow_unqualified)
                || expr_references_lix_state_version_column(
                    pattern,
                    relation_name,
                    allow_unqualified,
                )
        }
        Expr::InList { expr, list, .. } => {
            expr_references_lix_state_version_column(expr, relation_name, allow_unqualified)
                || list.iter().any(|item| {
                    expr_references_lix_state_version_column(item, relation_name, allow_unqualified)
                })
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_ref().is_some_and(|value| {
                expr_references_lix_state_version_column(value, relation_name, allow_unqualified)
            }) || conditions.iter().any(|condition| {
                expr_references_lix_state_version_column(
                    &condition.condition,
                    relation_name,
                    allow_unqualified,
                ) || expr_references_lix_state_version_column(
                    &condition.result,
                    relation_name,
                    allow_unqualified,
                )
            }) || else_result.as_ref().is_some_and(|value| {
                expr_references_lix_state_version_column(value, relation_name, allow_unqualified)
            })
        }
        Expr::Tuple(items) => items.iter().any(|item| {
            expr_references_lix_state_version_column(item, relation_name, allow_unqualified)
        }),
        Expr::Function(function) => match &function.args {
            FunctionArguments::List(list) => list.args.iter().any(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                    expr_references_lix_state_version_column(
                        inner,
                        relation_name,
                        allow_unqualified,
                    )
                }
                FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => match arg {
                    FunctionArgExpr::Expr(inner) => expr_references_lix_state_version_column(
                        inner,
                        relation_name,
                        allow_unqualified,
                    ),
                    _ => false,
                },
                _ => false,
            }),
            _ => false,
        },
        Expr::InSubquery { expr, .. } => {
            expr_references_lix_state_version_column(expr, relation_name, allow_unqualified)
        }
        _ => false,
    }
}

fn version_column_read_error() -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description:
            "lix_state does not expose version_id; use lix_state_by_version for explicit version reads"
                .to_string(),
    }
}

fn build_lix_state_wrapper_query(extra_predicates: &[String]) -> Result<Query, LixError> {
    let extra_where = if extra_predicates.is_empty() {
        String::new()
    } else {
        format!(" AND {}", extra_predicates.join(" AND "))
    };
    let sql = format!(
        "SELECT \
             s.entity_id AS entity_id, \
             s.schema_key AS schema_key, \
             s.file_id AS file_id, \
             s.plugin_key AS plugin_key, \
             s.snapshot_content AS snapshot_content, \
             s.schema_version AS schema_version, \
             s.created_at AS created_at, \
             s.updated_at AS updated_at, \
             s.inherited_from_version_id AS inherited_from_version_id, \
             s.change_id AS change_id, \
             s.commit_id AS commit_id, \
             s.untracked AS untracked, \
             s.writer_key AS writer_key, \
             s.metadata AS metadata \
         FROM {by_version_table} AS s \
         WHERE s.version_id = ( \
           SELECT lix_json_extract(snapshot_content, 'version_id') \
           FROM {active_table} \
           WHERE schema_key = '{active_schema_key}' \
             AND file_id = '{active_file_id}' \
             AND version_id = '{active_storage_version_id}' \
             AND snapshot_content IS NOT NULL \
           ORDER BY updated_at DESC \
           LIMIT 1 \
         ){extra_where}",
        by_version_table = LIX_STATE_BY_VERSION_VIEW_NAME,
        active_table = ACTIVE_VERSION_TABLE,
        active_schema_key = escape_sql_string(active_version_schema_key()),
        active_file_id = escape_sql_string(active_version_file_id()),
        active_storage_version_id = escape_sql_string(active_version_storage_version_id()),
        extra_where = extra_where,
    );
    parse_single_query(&sql)
}

fn wrapper_pushdown_predicates(pushdown: &StatePushdown) -> Vec<String> {
    let mut out = Vec::new();
    out.extend(pushdown.source_predicates.iter().cloned());
    out.extend(
        pushdown
            .ranked_predicates
            .iter()
            .map(|predicate| predicate.replacen("ranked.", "s.", 1)),
    );
    out
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
    fn rewrites_lix_state_to_by_version_wrapper_query() {
        let query = parse_query("SELECT entity_id, schema_key FROM lix_state");

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("FROM lix_state_by_version AS s"));
        assert!(sql.contains(
            "WHERE s.version_id = (SELECT lix_json_extract(snapshot_content, 'version_id')"
        ));
        assert!(!sql.contains("FROM lix_state "));
    }

    #[test]
    fn wrapper_projection_excludes_version_id_columns() {
        let query = parse_query("SELECT * FROM lix_state");

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("s.entity_id AS entity_id"));
        assert!(sql.contains("s.metadata AS metadata"));
        assert!(!sql.contains("s.version_id AS version_id"));
        assert!(!sql.contains("s.lixcol_version_id"));
    }

    #[test]
    fn rejects_version_id_projection_for_lix_state() {
        let query = parse_query("SELECT version_id FROM lix_state");
        let error = rewrite_query(query).expect_err("version_id projection should be rejected");
        assert!(error.description.contains("does not expose version_id"));
    }

    #[test]
    fn rejects_lixcol_version_id_projection_for_lix_state() {
        let query = parse_query("SELECT lixcol_version_id FROM lix_state");
        let error =
            rewrite_query(query).expect_err("lixcol_version_id projection should be rejected");
        assert!(error.description.contains("does not expose version_id"));
    }

    #[test]
    fn rejects_version_id_predicate_for_lix_state() {
        let query = parse_query("SELECT entity_id FROM lix_state WHERE version_id = 'v-1'");
        let error = rewrite_query(query).expect_err("version predicate should be rejected");
        assert!(error.description.contains("does not expose version_id"));
    }

    #[test]
    fn pushes_schema_and_file_predicates_into_wrapper_query() {
        let query = parse_query(
            "SELECT entity_id \
             FROM lix_state \
             WHERE schema_key = 'lix_entity_label' \
               AND file_id = 'lix'",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("s.schema_key = 'lix_entity_label'"));
        assert!(sql.contains("s.file_id = 'lix'"));
        assert!(!sql.contains("WHERE schema_key = 'lix_entity_label'"));
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
