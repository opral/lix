use std::ops::ControlFlow;

use sqlparser::ast::{
    Ident, ObjectName, ObjectNamePart, Query, Select, Statement, TableAlias, TableFactor,
};
use sqlparser::ast::{VisitMut, VisitorMut};
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

pub(crate) fn rewrite_query_with_select_rewriter(
    query: Query,
    rewrite_select: &mut dyn FnMut(&mut Select, &mut bool) -> Result<(), LixError>,
) -> Result<Option<Query>, LixError> {
    let mut changed = false;
    let mut new_query = query.clone();
    let mut visitor = SelectRewriteVisitor {
        rewrite_select,
        changed: &mut changed,
    };
    if let ControlFlow::Break(error) = new_query.visit(&mut visitor) {
        return Err(error);
    }
    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

pub(crate) fn rewrite_table_factors_in_select(
    select: &mut Select,
    rewrite_table_factor: &mut dyn FnMut(&mut TableFactor, &mut bool) -> Result<(), LixError>,
    changed: &mut bool,
) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_table_factor_in_place(&mut table.relation, rewrite_table_factor, changed)?;
        for join in &mut table.joins {
            rewrite_table_factor_in_place(&mut join.relation, rewrite_table_factor, changed)?;
        }
    }
    Ok(())
}

struct SelectRewriteVisitor<'a> {
    rewrite_select: &'a mut dyn FnMut(&mut Select, &mut bool) -> Result<(), LixError>,
    changed: &'a mut bool,
}

impl VisitorMut for SelectRewriteVisitor<'_> {
    type Break = LixError;

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
            return ControlFlow::Continue(());
        };
        match (self.rewrite_select)(select.as_mut(), self.changed) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    }
}

fn rewrite_table_factor_in_place(
    relation: &mut TableFactor,
    rewrite_table_factor: &mut dyn FnMut(&mut TableFactor, &mut bool) -> Result<(), LixError>,
    changed: &mut bool,
) -> Result<(), LixError> {
    rewrite_table_factor(relation, changed)?;
    if let TableFactor::NestedJoin {
        table_with_joins, ..
    } = relation
    {
        rewrite_table_factor_in_place(
            &mut table_with_joins.relation,
            rewrite_table_factor,
            changed,
        )?;
        for join in &mut table_with_joins.joins {
            rewrite_table_factor_in_place(&mut join.relation, rewrite_table_factor, changed)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Query, Select, Statement, TableFactor};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use crate::sql::{default_alias, object_name_matches, parse_single_query};

    use super::{rewrite_query_with_select_rewriter, rewrite_table_factors_in_select};

    fn parse_query(sql: &str) -> Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            other => panic!("expected query, got {other:?}"),
        }
    }

    fn rewrite_foo(select: &mut Select, changed: &mut bool) -> Result<(), crate::LixError> {
        rewrite_table_factors_in_select(
            select,
            &mut |relation, changed| {
                let TableFactor::Table { name, alias, .. } = relation else {
                    return Ok(());
                };
                if !object_name_matches(name, "foo") {
                    return Ok(());
                }
                let derived_query = parse_single_query("SELECT 1 AS id")?;
                let derived_alias = alias.clone().or_else(|| Some(default_alias("foo")));
                *relation = TableFactor::Derived {
                    lateral: false,
                    subquery: Box::new(derived_query),
                    alias: derived_alias,
                };
                *changed = true;
                Ok(())
            },
            changed,
        )
    }

    #[test]
    fn rewrites_table_in_expression_subquery() {
        let query = parse_query(
            "SELECT 1 \
             WHERE 'x' IN (SELECT f.id FROM foo AS f)",
        );
        let rewritten = rewrite_query_with_select_rewriter(query, &mut rewrite_foo)
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
        let rewritten = rewrite_query_with_select_rewriter(query, &mut rewrite_foo)
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
        let rewritten = rewrite_query_with_select_rewriter(query, &mut rewrite_foo)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("FROM foo"));
        assert_eq!(sql.matches("SELECT 1 AS id").count(), 2);
    }
}
