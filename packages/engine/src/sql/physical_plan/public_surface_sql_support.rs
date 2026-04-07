use crate::contracts::surface::{SurfaceBinding, SurfaceFamily, SurfaceVariant};
use crate::SqlDialect;
use sqlparser::ast::{Expr, Ident, Value as SqlValue, Visit, Visitor};
use std::ops::ControlFlow;

pub(crate) fn split_effective_state_pushdown_predicates(
    pushdown_predicates: &[Expr],
) -> (Vec<Expr>, Vec<Expr>) {
    let mut target_version_predicates = Vec::new();
    let mut source_predicates = Vec::new();
    for predicate in pushdown_predicates {
        if expr_references_identifier(predicate, &["version_id", "lixcol_version_id"])
            && !expr_references_identifier(predicate, &["root_commit_id", "lixcol_root_commit_id"])
        {
            target_version_predicates.push(predicate.clone());
        } else {
            source_predicates.push(predicate.clone());
        }
    }
    (target_version_predicates, source_predicates)
}

pub(crate) fn render_where_clause_sql(predicates: &[Expr], prefix: &str) -> String {
    if predicates.is_empty() {
        String::new()
    } else {
        format!(
            "{prefix}{}",
            predicates
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" AND ")
        )
    }
}

pub(crate) fn render_qualified_where_clause_sql(
    predicates: &[Expr],
    prefix: &str,
    table_alias: &str,
) -> String {
    if predicates.is_empty() {
        String::new()
    } else {
        format!(
            "{prefix}{}",
            predicates
                .iter()
                .map(|predicate| render_qualified_predicate_sql(predicate, table_alias))
                .collect::<Vec<_>>()
                .join(" AND ")
        )
    }
}

pub(crate) fn render_qualified_predicate_sql(expr: &Expr, table_alias: &str) -> String {
    match expr {
        Expr::BinaryOp { left, op, right } => format!(
            "{} {op} {}",
            render_qualified_predicate_sql(left, table_alias),
            render_qualified_predicate_sql(right, table_alias),
        ),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let not = if *negated { " NOT" } else { "" };
            format!(
                "{}{not} IN ({})",
                render_qualified_predicate_sql(expr, table_alias),
                list.iter()
                    .map(|item| render_qualified_predicate_sql(item, table_alias))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        Expr::Nested(inner) => format!("({})", render_qualified_predicate_sql(inner, table_alias)),
        Expr::Identifier(identifier) => format!(
            "{}.{}",
            quote_ident(table_alias),
            quote_ident(&identifier.value)
        ),
        Expr::CompoundIdentifier(identifiers) => identifiers
            .last()
            .map(|identifier| {
                format!(
                    "{}.{}",
                    quote_ident(table_alias),
                    quote_ident(&identifier.value)
                )
            })
            .unwrap_or_else(|| expr.to_string()),
        _ => expr.to_string(),
    }
}

pub(crate) fn is_live_state_raw_envelope_column(column: &str) -> bool {
    matches!(
        column,
        "entity_id"
            | "schema_key"
            | "file_id"
            | "version_id"
            | "plugin_key"
            | "schema_version"
            | "metadata"
            | "created_at"
            | "updated_at"
            | "global"
            | "change_id"
            | "commit_id"
            | "untracked"
            | "writer_key"
            | "root_commit_id"
            | "depth"
    )
}

pub(crate) fn entity_surface_has_live_payload_collisions(surface_binding: &SurfaceBinding) -> bool {
    surface_binding.descriptor.surface_family == SurfaceFamily::Entity
        && surface_binding.descriptor.surface_variant != SurfaceVariant::History
        && surface_binding
            .exposed_columns
            .iter()
            .any(|column| is_live_state_raw_envelope_column(column))
}

pub(crate) fn entity_surface_uses_payload_alias(
    surface_binding: &SurfaceBinding,
    column: &str,
) -> bool {
    entity_surface_has_live_payload_collisions(surface_binding)
        && surface_binding
            .exposed_columns
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(column))
        && is_live_state_raw_envelope_column(column)
}

pub(crate) fn entity_surface_payload_alias(column: &str) -> String {
    format!("payload__{column}")
}

pub(crate) fn json_array_text_join_sql(
    dialect: SqlDialect,
    json_column: &str,
    field: &str,
    alias: &str,
    value_column: &str,
) -> (String, String) {
    match dialect {
        SqlDialect::Sqlite => (
            format!("JOIN json_each({json_column}, '$.{field}') AS {alias}"),
            format!("{alias}.value"),
        ),
        SqlDialect::Postgres => (
            format!(
                "JOIN LATERAL jsonb_array_elements_text(CAST({json_column} AS JSONB) -> '{field}') AS {alias}({value_column}) ON TRUE"
            ),
            format!("{alias}.{value_column}"),
        ),
    }
}

pub(crate) fn expr_references_identifier(expr: &Expr, accepted_names: &[&str]) -> bool {
    struct IdentifierVisitor<'a> {
        accepted_names: &'a [&'a str],
        matched: bool,
    }

    impl Visitor for IdentifierVisitor<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            let matched_name = match expr {
                Expr::Identifier(identifier) => Some(identifier.value.as_str()),
                Expr::CompoundIdentifier(identifiers) => identifiers
                    .last()
                    .map(|identifier| identifier.value.as_str()),
                _ => None,
            };

            if matched_name.is_some_and(|name| {
                self.accepted_names
                    .iter()
                    .any(|accepted| name.eq_ignore_ascii_case(accepted))
            }) {
                self.matched = true;
                return ControlFlow::Break(());
            }

            ControlFlow::Continue(())
        }
    }

    let mut visitor = IdentifierVisitor {
        accepted_names,
        matched: false,
    };
    let _ = expr.visit(&mut visitor);
    visitor.matched
}

pub(crate) fn expr_contains_string_literal(expr: &Expr, expected: &str) -> bool {
    struct LiteralVisitor<'a> {
        expected: &'a str,
        matched: bool,
    }

    impl Visitor for LiteralVisitor<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            let matches = match expr {
                Expr::Value(value) => match &value.value {
                    SqlValue::SingleQuotedString(text) | SqlValue::DoubleQuotedString(text) => {
                        text == self.expected
                    }
                    _ => false,
                },
                _ => false,
            };

            if matches {
                self.matched = true;
                return ControlFlow::Break(());
            }

            ControlFlow::Continue(())
        }
    }

    let mut visitor = LiteralVisitor {
        expected,
        matched: false,
    };
    let _ = expr.visit(&mut visitor);
    visitor.matched
}

pub(crate) fn render_identifier(value: &str) -> String {
    Ident::new(value).to_string()
}

pub(crate) fn escape_sql_string(value: &str) -> String {
    crate::common::text::escape_sql_string(value)
}

pub(crate) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
