use crate::filesystem::live_projection::build_filesystem_state_history_source_sql;
use crate::SqlDialect;
use sqlparser::ast::{BinaryOperator, Expr};

pub(crate) fn build_filesystem_history_source_sql(
    dialect: SqlDialect,
    pushdown_predicates: &[String],
    force_active_scope: bool,
) -> String {
    let requested_root_predicates = history_requested_root_predicates(pushdown_predicates);
    let requested_version_predicates = history_requested_version_predicates(pushdown_predicates);
    let requested_roots_where = render_where_clause_sql(&requested_root_predicates, " AND ");
    let requested_versions_where = render_where_clause_sql(&requested_version_predicates, " AND ");
    let default_root_scope = if force_active_scope && requested_root_predicates.is_empty() {
        "AND ( \
           d.root_commit_id IS NOT NULL \
           OR c.entity_id IN (SELECT root_commit_id FROM default_root_commits) \
         )"
        .to_string()
    } else {
        String::new()
    };
    build_filesystem_state_history_source_sql(
        dialect,
        &requested_roots_where,
        &requested_versions_where,
        &default_root_scope,
        force_active_scope,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilesystemHistoryFilterColumn {
    RootCommitId,
    VersionId,
}

pub(crate) fn collect_filesystem_history_pushdown_predicates(
    selection: Option<&Expr>,
    relation_name: &str,
    allow_unqualified: bool,
) -> Vec<String> {
    let Some(selection) = selection else {
        return Vec::new();
    };
    let mut predicates = Vec::new();
    collect_filesystem_history_pushdown_predicates_from_expr(
        selection,
        relation_name,
        allow_unqualified,
        &mut predicates,
    );
    predicates
}

fn collect_filesystem_history_pushdown_predicates_from_expr(
    expr: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
    predicates: &mut Vec<String>,
) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_filesystem_history_pushdown_predicates_from_expr(
                left,
                relation_name,
                allow_unqualified,
                predicates,
            );
            collect_filesystem_history_pushdown_predicates_from_expr(
                right,
                relation_name,
                allow_unqualified,
                predicates,
            );
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(column) =
                extract_filesystem_history_filter_column(left, relation_name, allow_unqualified)
            {
                predicates.push(format!(
                    "{} {} {}",
                    filesystem_history_filter_column_name(column),
                    op,
                    right
                ));
            } else if let Some(column) =
                extract_filesystem_history_filter_column(right, relation_name, allow_unqualified)
            {
                if let Some(inverted) = invert_filesystem_history_binary_operator(op.clone()) {
                    predicates.push(format!(
                        "{} {} {}",
                        filesystem_history_filter_column_name(column),
                        inverted,
                        left
                    ));
                }
            }
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            if let Some(column) =
                extract_filesystem_history_filter_column(expr, relation_name, allow_unqualified)
            {
                let not_sql = if *negated { " NOT" } else { "" };
                predicates.push(format!(
                    "{}{} IN ({})",
                    filesystem_history_filter_column_name(column),
                    not_sql,
                    subquery
                ));
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if let Some(column) =
                extract_filesystem_history_filter_column(expr, relation_name, allow_unqualified)
            {
                let not_sql = if *negated { " NOT" } else { "" };
                predicates.push(format!(
                    "{}{} IN ({})",
                    filesystem_history_filter_column_name(column),
                    not_sql,
                    list.iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        }
        Expr::IsNull(inner) => {
            if let Some(column) =
                extract_filesystem_history_filter_column(inner, relation_name, allow_unqualified)
            {
                predicates.push(format!(
                    "{} IS NULL",
                    filesystem_history_filter_column_name(column)
                ));
            }
        }
        Expr::IsNotNull(inner) => {
            if let Some(column) =
                extract_filesystem_history_filter_column(inner, relation_name, allow_unqualified)
            {
                predicates.push(format!(
                    "{} IS NOT NULL",
                    filesystem_history_filter_column_name(column)
                ));
            }
        }
        Expr::Nested(inner) => collect_filesystem_history_pushdown_predicates_from_expr(
            inner,
            relation_name,
            allow_unqualified,
            predicates,
        ),
        _ => {}
    }
}

fn extract_filesystem_history_filter_column(
    expr: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
) -> Option<FilesystemHistoryFilterColumn> {
    let column = match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            if !parts[0].value.eq_ignore_ascii_case(relation_name) {
                return None;
            }
            parts[1].value.as_str()
        }
        Expr::Identifier(identifier) if allow_unqualified => identifier.value.as_str(),
        Expr::Nested(inner) => {
            return extract_filesystem_history_filter_column(
                inner,
                relation_name,
                allow_unqualified,
            );
        }
        _ => return None,
    };

    match column.to_ascii_lowercase().as_str() {
        "lixcol_root_commit_id" | "root_commit_id" => {
            Some(FilesystemHistoryFilterColumn::RootCommitId)
        }
        "lixcol_version_id" | "version_id" => Some(FilesystemHistoryFilterColumn::VersionId),
        _ => None,
    }
}

fn filesystem_history_filter_column_name(column: FilesystemHistoryFilterColumn) -> &'static str {
    match column {
        FilesystemHistoryFilterColumn::RootCommitId => "root_commit_id",
        FilesystemHistoryFilterColumn::VersionId => "version_id",
    }
}

fn invert_filesystem_history_binary_operator(op: BinaryOperator) -> Option<BinaryOperator> {
    match op {
        BinaryOperator::Eq => Some(BinaryOperator::Eq),
        BinaryOperator::NotEq => Some(BinaryOperator::NotEq),
        BinaryOperator::Gt => Some(BinaryOperator::Lt),
        BinaryOperator::GtEq => Some(BinaryOperator::LtEq),
        BinaryOperator::Lt => Some(BinaryOperator::Gt),
        BinaryOperator::LtEq => Some(BinaryOperator::GtEq),
        _ => None,
    }
}

fn history_requested_root_predicates(pushdown_predicates: &[String]) -> Vec<String> {
    pushdown_predicates
        .iter()
        .filter_map(|predicate| {
            if predicate.contains("lixcol_root_commit_id") {
                Some(predicate.replace("lixcol_root_commit_id", "c.entity_id"))
            } else if predicate.contains("root_commit_id") {
                Some(predicate.replace("root_commit_id", "c.entity_id"))
            } else {
                None
            }
        })
        .collect()
}

fn history_requested_version_predicates(pushdown_predicates: &[String]) -> Vec<String> {
    pushdown_predicates
        .iter()
        .filter_map(|predicate| {
            if predicate.contains("lixcol_version_id") {
                Some(predicate.replace("lixcol_version_id", "d.root_version_id"))
            } else if predicate.contains("version_id") {
                Some(predicate.replace("version_id", "d.root_version_id"))
            } else {
                None
            }
        })
        .collect()
}

fn render_where_clause_sql(predicates: &[String], prefix: &str) -> String {
    if predicates.is_empty() {
        String::new()
    } else {
        format!("{prefix}{}", predicates.join(" AND "))
    }
}
