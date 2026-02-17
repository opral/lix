use std::ops::ControlFlow;

use sqlparser::ast::{
    Ident, ObjectName, ObjectNamePart, Query, Select, SetExpr, Statement, TableAlias, TableFactor,
};
use sqlparser::ast::{Visit, VisitMut, Visitor, VisitorMut};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::LixError;

pub(crate) fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}

pub(crate) fn default_alias(name: &str) -> TableAlias {
    TableAlias {
        explicit: false,
        name: Ident::new(name),
        columns: Vec::new(),
    }
}

pub(crate) fn parse_single_query(sql: &str) -> Result<Query, LixError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        message: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "expected a single SELECT statement".to_string(),
        });
    }
    let statement = statements.remove(0);
    match statement {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            message: "expected SELECT statement".to_string(),
        }),
    }
}

pub(crate) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RewriteDecision {
    Changed,
    Unchanged,
}

impl RewriteDecision {
    fn merge(self, other: Self) -> Self {
        if self == Self::Changed || other == Self::Changed {
            Self::Changed
        } else {
            Self::Unchanged
        }
    }
}

pub(crate) fn rewrite_query_selects(
    query: Query,
    rewrite_select: &mut dyn FnMut(&mut Select) -> Result<RewriteDecision, LixError>,
) -> Result<Option<Query>, LixError> {
    let mut changed = false;
    let mut new_query = query.clone();
    let mut visitor = SelectRewriteVisitor {
        rewrite_select,
        changed: &mut changed,
    };
    if let ControlFlow::Break(error) = VisitMut::visit(&mut new_query, &mut visitor) {
        return Err(error);
    }
    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

pub(crate) fn visit_query_selects(
    query: &Query,
    visit_select: &mut dyn FnMut(&Select) -> Result<(), LixError>,
) -> Result<(), LixError> {
    let mut visitor = SelectVisitVisitor { visit_select };
    if let ControlFlow::Break(error) = Visit::visit(query, &mut visitor) {
        return Err(error);
    }
    Ok(())
}

// Compatibility wrapper for existing rewrite steps.
pub(crate) fn rewrite_query_with_select_rewriter(
    query: Query,
    rewrite_select: &mut dyn FnMut(&mut Select, &mut bool) -> Result<(), LixError>,
) -> Result<Option<Query>, LixError> {
    rewrite_query_selects(query, &mut |select| {
        let mut changed = false;
        rewrite_select(select, &mut changed)?;
        if changed {
            Ok(RewriteDecision::Changed)
        } else {
            Ok(RewriteDecision::Unchanged)
        }
    })
}

pub(crate) fn rewrite_table_factors_in_select_decision(
    select: &mut Select,
    rewrite_table_factor: &mut dyn FnMut(&mut TableFactor) -> Result<RewriteDecision, LixError>,
) -> Result<RewriteDecision, LixError> {
    let mut decision = RewriteDecision::Unchanged;
    for table in &mut select.from {
        decision = decision.merge(rewrite_table_factor_in_place_decision(
            &mut table.relation,
            rewrite_table_factor,
        )?);
        for join in &mut table.joins {
            decision = decision.merge(rewrite_table_factor_in_place_decision(
                &mut join.relation,
                rewrite_table_factor,
            )?);
        }
    }
    Ok(decision)
}

pub(crate) fn visit_table_factors_in_select(
    select: &Select,
    visit_table_factor: &mut dyn FnMut(&TableFactor) -> Result<(), LixError>,
) -> Result<(), LixError> {
    for table in &select.from {
        visit_table_factor_in_place(&table.relation, visit_table_factor)?;
        for join in &table.joins {
            visit_table_factor_in_place(&join.relation, visit_table_factor)?;
        }
    }
    Ok(())
}

// Compatibility wrapper for existing rewrite steps.
pub(crate) fn rewrite_table_factors_in_select(
    select: &mut Select,
    rewrite_table_factor: &mut dyn FnMut(&mut TableFactor, &mut bool) -> Result<(), LixError>,
    changed: &mut bool,
) -> Result<(), LixError> {
    let decision = rewrite_table_factors_in_select_decision(select, &mut |relation| {
        let mut local_changed = false;
        rewrite_table_factor(relation, &mut local_changed)?;
        if local_changed {
            Ok(RewriteDecision::Changed)
        } else {
            Ok(RewriteDecision::Unchanged)
        }
    })?;
    if decision == RewriteDecision::Changed {
        *changed = true;
    }
    Ok(())
}

struct SelectRewriteVisitor<'a> {
    rewrite_select: &'a mut dyn FnMut(&mut Select) -> Result<RewriteDecision, LixError>,
    changed: &'a mut bool,
}

impl VisitorMut for SelectRewriteVisitor<'_> {
    type Break = LixError;

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        match rewrite_selects_in_set_expr(query.body.as_mut(), self.rewrite_select, self.changed) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    }
}

struct SelectVisitVisitor<'a> {
    visit_select: &'a mut dyn FnMut(&Select) -> Result<(), LixError>,
}

impl Visitor for SelectVisitVisitor<'_> {
    type Break = LixError;

    fn post_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        match visit_selects_in_set_expr(query.body.as_ref(), self.visit_select) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    }
}

fn rewrite_selects_in_set_expr(
    set_expr: &mut SetExpr,
    rewrite_select: &mut dyn FnMut(&mut Select) -> Result<RewriteDecision, LixError>,
    changed: &mut bool,
) -> Result<(), LixError> {
    match set_expr {
        SetExpr::Select(select) => {
            if rewrite_select(select.as_mut())? == RewriteDecision::Changed {
                *changed = true;
            }
            Ok(())
        }
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_selects_in_set_expr(left.as_mut(), rewrite_select, changed)?;
            rewrite_selects_in_set_expr(right.as_mut(), rewrite_select, changed)
        }
        // Nested Query bodies are visited as their own Query nodes by VisitorMut.
        _ => Ok(()),
    }
}

fn visit_selects_in_set_expr(
    set_expr: &SetExpr,
    visit_select: &mut dyn FnMut(&Select) -> Result<(), LixError>,
) -> Result<(), LixError> {
    match set_expr {
        SetExpr::Select(select) => visit_select(select.as_ref()),
        SetExpr::SetOperation { left, right, .. } => {
            visit_selects_in_set_expr(left.as_ref(), visit_select)?;
            visit_selects_in_set_expr(right.as_ref(), visit_select)
        }
        // Nested Query bodies are visited as their own Query nodes by Visitor.
        _ => Ok(()),
    }
}

fn rewrite_table_factor_in_place_decision(
    relation: &mut TableFactor,
    rewrite_table_factor: &mut dyn FnMut(&mut TableFactor) -> Result<RewriteDecision, LixError>,
) -> Result<RewriteDecision, LixError> {
    let mut decision = rewrite_table_factor(relation)?;
    if let TableFactor::NestedJoin {
        table_with_joins, ..
    } = relation
    {
        decision = decision.merge(rewrite_table_factor_in_place_decision(
            &mut table_with_joins.relation,
            rewrite_table_factor,
        )?);
        for join in &mut table_with_joins.joins {
            decision = decision.merge(rewrite_table_factor_in_place_decision(
                &mut join.relation,
                rewrite_table_factor,
            )?);
        }
    }
    Ok(decision)
}

fn visit_table_factor_in_place(
    relation: &TableFactor,
    visit_table_factor: &mut dyn FnMut(&TableFactor) -> Result<(), LixError>,
) -> Result<(), LixError> {
    visit_table_factor(relation)?;
    if let TableFactor::NestedJoin {
        table_with_joins, ..
    } = relation
    {
        visit_table_factor_in_place(&table_with_joins.relation, visit_table_factor)?;
        for join in &table_with_joins.joins {
            visit_table_factor_in_place(&join.relation, visit_table_factor)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Query, Select, Statement, TableFactor};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use crate::sql::{
        default_alias, object_name_matches, parse_single_query, rewrite_query_selects,
        rewrite_table_factors_in_select_decision, visit_query_selects,
        visit_table_factors_in_select, RewriteDecision,
    };

    fn parse_query(sql: &str) -> Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            other => panic!("expected query, got {other:?}"),
        }
    }

    fn rewrite_foo(select: &mut Select) -> Result<RewriteDecision, crate::LixError> {
        rewrite_table_factors_in_select_decision(select, &mut |relation| {
            let TableFactor::Table { name, alias, .. } = relation else {
                return Ok(RewriteDecision::Unchanged);
            };
            if !object_name_matches(name, "foo") {
                return Ok(RewriteDecision::Unchanged);
            }
            let derived_query = parse_single_query("SELECT 1 AS id")?;
            let derived_alias = alias.clone().or_else(|| Some(default_alias("foo")));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            Ok(RewriteDecision::Changed)
        })
    }

    #[test]
    fn rewrites_table_in_expression_subquery() {
        let query = parse_query(
            "SELECT 1 \
             WHERE 'x' IN (SELECT f.id FROM foo AS f)",
        );
        let rewritten = rewrite_query_selects(query, &mut rewrite_foo)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("FROM foo"));
        assert!(sql.contains("FROM (SELECT 1 AS id) AS f"));
    }

    #[test]
    fn rewrites_table_in_exists_subquery() {
        let query = parse_query(
            "SELECT 1 \
             WHERE EXISTS (SELECT 1 FROM foo)",
        );
        let rewritten = rewrite_query_selects(query, &mut rewrite_foo)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("FROM foo"));
        assert!(sql.contains("SELECT 1 AS id"));
    }

    #[test]
    fn rewrites_table_in_cte_and_top_level() {
        let query = parse_query(
            "WITH c AS (SELECT id FROM foo) \
             SELECT * FROM c JOIN foo ON 1 = 1",
        );
        let rewritten = rewrite_query_selects(query, &mut rewrite_foo)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("FROM foo"));
        assert_eq!(sql.matches("SELECT 1 AS id").count(), 2);
    }

    #[test]
    fn rewrites_table_in_set_operation_branches() {
        let query = parse_query("SELECT id FROM foo UNION SELECT id FROM foo");
        let rewritten = rewrite_query_selects(query, &mut rewrite_foo)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("FROM foo"));
        assert_eq!(sql.matches("SELECT 1 AS id").count(), 2);
    }

    #[test]
    fn visits_selects_in_set_operation_branches() {
        let query = parse_query("SELECT id FROM foo UNION SELECT id FROM foo");
        let mut count = 0usize;
        visit_query_selects(&query, &mut |select| {
            visit_table_factors_in_select(select, &mut |relation| {
                if let TableFactor::Table { name, .. } = relation {
                    if object_name_matches(name, "foo") {
                        count += 1;
                    }
                }
                Ok(())
            })
        })
        .expect("visit should succeed");
        assert_eq!(count, 2);
    }
}
