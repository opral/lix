use std::ops::ControlFlow;

use sqlparser::ast::{ObjectNamePart, Visit, Visitor};

use crate::LixError;

use super::nodes::{ObjectName, Query, Select, SetExpr, TableFactor};
#[cfg(test)]
use super::nodes::Statement;
#[cfg(test)]
use super::utils::is_transaction_control_statement;

pub(crate) fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
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
pub(crate) fn contains_transaction_control_statement(statements: &[Statement]) -> bool {
    statements.iter().any(is_transaction_control_statement)
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::contains_transaction_control_statement;

    #[test]
    fn detects_transaction_control_statements() {
        let statements =
            Parser::parse_sql(&GenericDialect {}, "BEGIN; SELECT 1; COMMIT;").expect("parse SQL");
        assert!(contains_transaction_control_statement(&statements));
    }

    #[test]
    fn ignores_non_transaction_control_statements() {
        let statements =
            Parser::parse_sql(&GenericDialect {}, "SELECT 1; SELECT 2;").expect("parse SQL");
        assert!(!contains_transaction_control_statement(&statements));
    }
}
