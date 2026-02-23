use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, JoinConstraint, JoinOperator, ObjectName, Query, Select, TableFactor,
    TableWithJoins, UnaryOperator, Value as AstValue, ValueWithSpan, Visit, Visitor,
};

use crate::sql::{object_name_matches, visit_query_selects, visit_table_factors_in_select};
use crate::LixError;

const LOGICAL_READ_VIEW_NAMES: &[&str] = &[
    "lix_active_account",
    "lix_active_version",
    "lix_state",
    "lix_state_by_version",
    "lix_state_history",
    "lix_version",
    "lix_file",
    "lix_file_by_version",
    "lix_file_history",
    "lix_directory",
    "lix_directory_by_version",
    "lix_directory_history",
];
const MATERIALIZED_STATE_TABLE_PREFIX: &str = "lix_internal_state_materialized_v1_";

pub(crate) fn validate_final_read_query(query: &Query) -> Result<(), LixError> {
    validate_final_read_query_with_options(query, true)
}

pub(crate) fn validate_final_read_query_with_options(
    query: &Query,
    enforce_materialized_state_semantics: bool,
) -> Result<(), LixError> {
    validate_no_unresolved_logical_read_views(query)?;
    validate_unique_explicit_relation_aliases(query)?;
    validate_placeholder_mapping_contract(query)?;
    if enforce_materialized_state_semantics {
        validate_materialized_state_semantics(query)?;
    }
    Ok(())
}

fn validate_no_unresolved_logical_read_views(query: &Query) -> Result<(), LixError> {
    validate_no_unresolved_logical_read_views_except(query, &[])
}

fn validate_unique_explicit_relation_aliases(query: &Query) -> Result<(), LixError> {
    visit_query_selects(query, &mut |select| {
        let mut aliases = BTreeSet::new();
        visit_table_factors_in_select(select, &mut |relation| {
            let alias = match relation {
                sqlparser::ast::TableFactor::Table {
                    alias: Some(alias), ..
                }
                | sqlparser::ast::TableFactor::Derived {
                    alias: Some(alias), ..
                } => alias.name.value.to_ascii_lowercase(),
                _ => return Ok(()),
            };
            if !aliases.insert(alias.clone()) {
                return Err(LixError {
                    message: format!(
                        "read rewrite produced duplicate explicit relation alias '{alias}'"
                    ),
                });
            }
            Ok(())
        })
    })
}

fn validate_materialized_state_semantics(query: &Query) -> Result<(), LixError> {
    visit_query_selects(query, &mut |select| {
        let materialized_relations = collect_materialized_relations(select);
        if materialized_relations.is_empty() {
            return Ok(());
        }

        let predicates = collect_select_predicates(select);
        for relation in &materialized_relations {
            let allow_unqualified = materialized_relations.len() == 1;
            let has_live_row_filter = predicates.iter().any(|predicate| {
                expr_contains_tombstone_filter_value(
                    predicate,
                    Some(&relation.qualifier),
                    0,
                ) || (allow_unqualified
                    && expr_contains_tombstone_filter_value(predicate, None, 0))
            });
            let has_tombstone_row_filter = predicates.iter().any(|predicate| {
                expr_contains_tombstone_filter_value(
                    predicate,
                    Some(&relation.qualifier),
                    1,
                ) || expr_contains_snapshot_content_null_filter(
                    predicate,
                    Some(&relation.qualifier),
                ) || (allow_unqualified
                    && (expr_contains_tombstone_filter_value(predicate, None, 1)
                        || expr_contains_snapshot_content_null_filter(predicate, None)))
            });
            if !has_live_row_filter && !has_tombstone_row_filter {
                return Err(LixError {
                    message: format!(
                        "read rewrite produced materialized relation '{}' without live-row tombstone filter",
                        relation.display_name
                    ),
                });
            }
            if has_tombstone_row_filter && !has_live_row_filter {
                continue;
            }

            let has_schema_key_filter = predicates.iter().any(|predicate| {
                expr_contains_schema_key_filter(
                    predicate,
                    Some(&relation.qualifier),
                    &relation.expected_schema_key,
                ) || (allow_unqualified
                    && expr_contains_schema_key_filter(
                        predicate,
                        None,
                        &relation.expected_schema_key,
                    ))
            });
            if !has_schema_key_filter {
                return Err(LixError {
                    message: format!(
                        "read rewrite produced materialized relation '{}' without schema_key = '{}' filter",
                        relation.display_name, relation.expected_schema_key
                    ),
                });
            }

            if requires_snapshot_content_not_null_filter(&relation.expected_schema_key) {
                let has_snapshot_filter = predicates.iter().any(|predicate| {
                    expr_contains_snapshot_content_not_null_filter(
                        predicate,
                        Some(&relation.qualifier),
                    ) || (allow_unqualified
                        && expr_contains_snapshot_content_not_null_filter(predicate, None))
                });
                if !has_snapshot_filter {
                    return Err(LixError {
                        message: format!(
                            "read rewrite produced materialized relation '{}' without snapshot_content IS NOT NULL filter",
                            relation.display_name
                        ),
                    });
                }
            }
        }

        Ok(())
    })
}

#[derive(Debug)]
struct MaterializedRelation {
    display_name: String,
    qualifier: String,
    expected_schema_key: String,
}

fn collect_materialized_relations(select: &Select) -> Vec<MaterializedRelation> {
    let mut relations = Vec::new();
    for table in &select.from {
        collect_materialized_relations_from_table_with_joins(table, &mut relations);
    }
    relations
}

fn collect_materialized_relations_from_table_with_joins(
    table: &TableWithJoins,
    relations: &mut Vec<MaterializedRelation>,
) {
    collect_materialized_relations_from_table_factor(&table.relation, relations);
    for join in &table.joins {
        collect_materialized_relations_from_table_factor(&join.relation, relations);
    }
}

fn collect_materialized_relations_from_table_factor(
    table: &TableFactor,
    relations: &mut Vec<MaterializedRelation>,
) {
    let TableFactor::Table { name, alias, .. } = table else {
        return;
    };
    let Some(base_name) = object_name_last_identifier(name) else {
        return;
    };
    let Some(expected_schema_key) = materialized_schema_key_for_table_name(&base_name) else {
        return;
    };

    let qualifier = alias
        .as_ref()
        .map(|alias| alias.name.value.clone())
        .unwrap_or_else(|| base_name.clone());
    relations.push(MaterializedRelation {
        display_name: base_name,
        qualifier,
        expected_schema_key,
    });
}

fn materialized_schema_key_for_table_name(base_name: &str) -> Option<String> {
    let lowercase = base_name.to_ascii_lowercase();
    if !lowercase.starts_with(MATERIALIZED_STATE_TABLE_PREFIX) {
        return None;
    }
    Some(lowercase[MATERIALIZED_STATE_TABLE_PREFIX.len()..].to_string())
}

fn requires_snapshot_content_not_null_filter(schema_key: &str) -> bool {
    !schema_key.eq_ignore_ascii_case("lix_change")
        && !schema_key.eq_ignore_ascii_case("lix_commit")
}

fn object_name_last_identifier(name: &ObjectName) -> Option<String> {
    let last = name.0.last()?;
    match last {
        sqlparser::ast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
        _ => Some(last.to_string()),
    }
}

fn collect_select_predicates(select: &Select) -> Vec<&Expr> {
    let mut predicates = Vec::new();
    if let Some(selection) = &select.selection {
        predicates.push(selection);
    }
    for table in &select.from {
        for join in &table.joins {
            collect_join_operator_predicates(&join.join_operator, &mut predicates);
        }
    }
    predicates
}

fn collect_join_operator_predicates<'a>(
    operator: &'a JoinOperator,
    predicates: &mut Vec<&'a Expr>,
) {
    match operator {
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => {
            predicates.push(match_condition);
            if let JoinConstraint::On(expr) = constraint {
                predicates.push(expr);
            }
        }
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint)
        | JoinOperator::StraightJoin(constraint) => {
            if let JoinConstraint::On(expr) = constraint {
                predicates.push(expr);
            }
        }
        JoinOperator::CrossApply | JoinOperator::OuterApply => {}
    }
}

fn expr_contains_live_tombstone_filter(expr: &Expr, qualifier: Option<&str>) -> bool {
    expr_contains_tombstone_filter_value(expr, qualifier, 0)
        || expr_contains_tombstone_filter_value(expr, qualifier, 1)
}

fn expr_contains_tombstone_filter_value(
    expr: &Expr,
    qualifier: Option<&str>,
    expected_value: i64,
) -> bool {
    struct LiveTombstoneFilterVisitor<'a> {
        qualifier: Option<&'a str>,
        expected_value: i64,
        found: bool,
    }

    impl Visitor for LiveTombstoneFilterVisitor<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if self.found {
                return ControlFlow::Break(());
            }
            if let Expr::BinaryOp { left, op, right } = expr {
                if *op == BinaryOperator::Eq
                    && ((expr_is_tombstone_column(left, self.qualifier)
                        && expr_is_numeric_value(right, self.expected_value))
                        || (expr_is_tombstone_column(right, self.qualifier)
                            && expr_is_numeric_value(left, self.expected_value)))
                {
                    self.found = true;
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut visitor = LiveTombstoneFilterVisitor {
        qualifier,
        expected_value,
        found: false,
    };
    let _ = expr.visit(&mut visitor);
    visitor.found
}

fn expr_is_tombstone_column(expr: &Expr, qualifier: Option<&str>) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("is_tombstone"),
        Expr::CompoundIdentifier(identifiers) => {
            let Some(last) = identifiers.last() else {
                return false;
            };
            if !last.value.eq_ignore_ascii_case("is_tombstone") {
                return false;
            }
            let Some(qualifier) = qualifier else {
                return true;
            };
            identifiers.len() < 2
                || identifiers[identifiers.len() - 2]
                    .value
                    .eq_ignore_ascii_case(qualifier)
        }
        _ => false,
    }
}

fn expr_contains_schema_key_filter(
    expr: &Expr,
    qualifier: Option<&str>,
    expected_schema_key: &str,
) -> bool {
    struct SchemaKeyFilterVisitor<'a> {
        qualifier: Option<&'a str>,
        expected_schema_key: &'a str,
        found: bool,
    }

    impl Visitor for SchemaKeyFilterVisitor<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if self.found {
                return ControlFlow::Break(());
            }
            match expr {
                Expr::BinaryOp { left, op, right }
                    if *op == BinaryOperator::Eq
                        && ((expr_is_schema_key_column(left, self.qualifier)
                            && expr_is_expected_schema_key_literal(
                                right,
                                self.expected_schema_key,
                            ))
                            || (expr_is_schema_key_column(right, self.qualifier)
                                && expr_is_expected_schema_key_literal(
                                    left,
                                    self.expected_schema_key,
                                ))) =>
                {
                    self.found = true;
                    ControlFlow::Break(())
                }
                Expr::InList {
                    expr,
                    list,
                    negated: false,
                } if expr_is_schema_key_column(expr, self.qualifier)
                    && list.iter().any(|item| {
                        expr_is_expected_schema_key_literal(item, self.expected_schema_key)
                    }) =>
                {
                    self.found = true;
                    ControlFlow::Break(())
                }
                _ => ControlFlow::Continue(()),
            }
        }
    }

    let mut visitor = SchemaKeyFilterVisitor {
        qualifier,
        expected_schema_key,
        found: false,
    };
    let _ = expr.visit(&mut visitor);
    visitor.found
}

fn expr_is_schema_key_column(expr: &Expr, qualifier: Option<&str>) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("schema_key"),
        Expr::CompoundIdentifier(identifiers) => {
            let Some(last) = identifiers.last() else {
                return false;
            };
            if !last.value.eq_ignore_ascii_case("schema_key") {
                return false;
            }
            let Some(qualifier) = qualifier else {
                return true;
            };
            identifiers.len() < 2
                || identifiers[identifiers.len() - 2]
                    .value
                    .eq_ignore_ascii_case(qualifier)
        }
        _ => false,
    }
}

fn expr_is_expected_schema_key_literal(expr: &Expr, expected_schema_key: &str) -> bool {
    match expr {
        Expr::Value(ValueWithSpan {
            value: AstValue::SingleQuotedString(value),
            ..
        }) => value.eq_ignore_ascii_case(expected_schema_key),
        Expr::Cast { expr, .. } => expr_is_expected_schema_key_literal(expr, expected_schema_key),
        _ => false,
    }
}

fn expr_contains_snapshot_content_not_null_filter(expr: &Expr, qualifier: Option<&str>) -> bool {
    struct SnapshotContentFilterVisitor<'a> {
        qualifier: Option<&'a str>,
        found: bool,
    }

    impl Visitor for SnapshotContentFilterVisitor<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if self.found {
                return ControlFlow::Break(());
            }
            match expr {
                Expr::IsNotNull(expr) if expr_is_snapshot_content_column(expr, self.qualifier) => {
                    self.found = true;
                    ControlFlow::Break(())
                }
                Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr,
                } => {
                    if let Expr::IsNull(inner) = expr.as_ref() {
                        if expr_is_snapshot_content_column(inner, self.qualifier) {
                            self.found = true;
                            return ControlFlow::Break(());
                        }
                    }
                    ControlFlow::Continue(())
                }
                _ => ControlFlow::Continue(()),
            }
        }
    }

    let mut visitor = SnapshotContentFilterVisitor {
        qualifier,
        found: false,
    };
    let _ = expr.visit(&mut visitor);
    visitor.found
}

fn expr_contains_snapshot_content_null_filter(expr: &Expr, qualifier: Option<&str>) -> bool {
    struct SnapshotContentNullFilterVisitor<'a> {
        qualifier: Option<&'a str>,
        found: bool,
    }

    impl Visitor for SnapshotContentNullFilterVisitor<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if self.found {
                return ControlFlow::Break(());
            }
            match expr {
                Expr::IsNull(expr) if expr_is_snapshot_content_column(expr, self.qualifier) => {
                    self.found = true;
                    ControlFlow::Break(())
                }
                Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr,
                } => {
                    if let Expr::IsNotNull(inner) = expr.as_ref() {
                        if expr_is_snapshot_content_column(inner, self.qualifier) {
                            self.found = true;
                            return ControlFlow::Break(());
                        }
                    }
                    ControlFlow::Continue(())
                }
                _ => ControlFlow::Continue(()),
            }
        }
    }

    let mut visitor = SnapshotContentNullFilterVisitor {
        qualifier,
        found: false,
    };
    let _ = expr.visit(&mut visitor);
    visitor.found
}

fn expr_is_snapshot_content_column(expr: &Expr, qualifier: Option<&str>) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("snapshot_content"),
        Expr::CompoundIdentifier(identifiers) => {
            let Some(last) = identifiers.last() else {
                return false;
            };
            if !last.value.eq_ignore_ascii_case("snapshot_content") {
                return false;
            }
            let Some(qualifier) = qualifier else {
                return true;
            };
            identifiers.len() < 2
                || identifiers[identifiers.len() - 2]
                    .value
                    .eq_ignore_ascii_case(qualifier)
        }
        _ => false,
    }
}

fn expr_is_numeric_zero(expr: &Expr) -> bool {
    match expr {
        Expr::Value(ValueWithSpan {
            value: AstValue::Number(number, _),
            ..
        }) => number.parse::<i64>().ok() == Some(0),
        Expr::Cast { expr, .. } => expr_is_numeric_zero(expr),
        _ => false,
    }
}

fn expr_is_numeric_one(expr: &Expr) -> bool {
    match expr {
        Expr::Value(ValueWithSpan {
            value: AstValue::Number(number, _),
            ..
        }) => number.parse::<i64>().ok() == Some(1),
        Expr::Cast { expr, .. } => expr_is_numeric_one(expr),
        _ => false,
    }
}

fn expr_is_numeric_zero_or_one(expr: &Expr) -> bool {
    expr_is_numeric_zero(expr) || expr_is_numeric_one(expr)
}

fn expr_is_numeric_value(expr: &Expr, expected: i64) -> bool {
    match expr {
        Expr::Value(ValueWithSpan {
            value: AstValue::Number(number, _),
            ..
        }) => number.parse::<i64>().ok() == Some(expected),
        Expr::Cast { expr, .. } => expr_is_numeric_value(expr, expected),
        _ => false,
    }
}

fn validate_placeholder_mapping_contract(query: &Query) -> Result<(), LixError> {
    struct PlaceholderContractVisitor {
        has_bare: bool,
        has_numbered: bool,
        invalid_tokens: BTreeSet<String>,
    }

    impl Visitor for PlaceholderContractVisitor {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            let Expr::Value(ValueWithSpan {
                value: AstValue::Placeholder(token),
                ..
            }) = expr
            else {
                return ControlFlow::Continue(());
            };
            let trimmed = token.trim();
            if let Some(rest) = trimmed.strip_prefix('?') {
                if rest.is_empty() {
                    self.has_bare = true;
                } else {
                    match rest.parse::<usize>() {
                        Ok(index) if index > 0 => {
                            self.has_numbered = true;
                        }
                        _ => {
                            self.invalid_tokens.insert(trimmed.to_string());
                        }
                    }
                }
                return ControlFlow::Continue(());
            }
            if let Some(rest) = trimmed.strip_prefix('$') {
                match rest.parse::<usize>() {
                    Ok(index) if index > 0 => {
                        self.has_numbered = true;
                    }
                    _ => {
                        self.invalid_tokens.insert(trimmed.to_string());
                    }
                }
                return ControlFlow::Continue(());
            }
            self.invalid_tokens.insert(trimmed.to_string());
            ControlFlow::Continue(())
        }
    }

    let mut visitor = PlaceholderContractVisitor {
        has_bare: false,
        has_numbered: false,
        invalid_tokens: BTreeSet::new(),
    };
    let _ = query.visit(&mut visitor);

    if !visitor.invalid_tokens.is_empty() {
        return Err(LixError {
            message: format!(
                "read rewrite produced invalid placeholder tokens: {}",
                visitor.invalid_tokens.into_iter().collect::<Vec<_>>().join(", ")
            ),
        });
    }
    if visitor.has_bare && visitor.has_numbered {
        return Err(LixError {
            message: "read rewrite produced mixed bare and numbered placeholders".to_string(),
        });
    }
    Ok(())
}

fn validate_no_unresolved_logical_read_views_except(
    query: &Query,
    allowed: &[&str],
) -> Result<(), LixError> {
    let allowed: BTreeSet<&str> = allowed.iter().copied().collect();
    let mut unresolved = BTreeSet::new();
    visit_query_selects(query, &mut |select| {
        visit_table_factors_in_select(select, &mut |relation| {
            let sqlparser::ast::TableFactor::Table { name, .. } = relation else {
                return Ok(());
            };
            for candidate in LOGICAL_READ_VIEW_NAMES {
                if object_name_matches(name, candidate) {
                    if allowed.contains(candidate) {
                        continue;
                    }
                    unresolved.insert((*candidate).to_string());
                }
            }
            Ok(())
        })
    })?;

    if unresolved.is_empty() {
        return Ok(());
    }

    Err(LixError {
        message: format!(
            "read rewrite left unresolved logical views: {}",
            unresolved.into_iter().collect::<Vec<_>>().join(", ")
        ),
    })
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Query, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::validate_final_read_query;

    fn parse_query(sql: &str) -> Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            other => panic!("expected query, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unresolved_logical_read_views() {
        let query = parse_query("SELECT * FROM lix_state_by_version");
        let error = validate_final_read_query(&query)
            .expect_err("validator should reject unresolved logical reads");
        assert!(error.message.contains("lix_state_by_version"));
    }

    #[test]
    fn rejects_duplicate_explicit_relation_aliases() {
        let query = parse_query("SELECT * FROM one AS t JOIN two AS t ON 1 = 1");
        let error = validate_final_read_query(&query)
            .expect_err("validator should reject duplicate explicit aliases");
        assert!(error.message.contains("duplicate explicit relation alias"));
    }
}
