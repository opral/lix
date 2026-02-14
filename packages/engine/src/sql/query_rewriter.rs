use sqlparser::ast::{Query, Select, SetExpr, TableFactor};

use crate::LixError;

pub(crate) fn rewrite_query_with_select_rewriter(
    query: Query,
    rewrite_select: &mut dyn FnMut(&mut Select, &mut bool) -> Result<(), LixError>,
) -> Result<Option<Query>, LixError> {
    let mut changed = false;
    let mut new_query = query.clone();
    rewrite_query_in_place(&mut new_query, rewrite_select, &mut changed)?;
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

fn rewrite_query_in_place(
    query: &mut Query,
    rewrite_select: &mut dyn FnMut(&mut Select, &mut bool) -> Result<(), LixError>,
    changed: &mut bool,
) -> Result<(), LixError> {
    if let Some(with) = query.with.as_mut() {
        for cte in &mut with.cte_tables {
            rewrite_query_in_place(&mut cte.query, rewrite_select, changed)?;
        }
    }
    rewrite_set_expr_in_place(query.body.as_mut(), rewrite_select, changed)
}

fn rewrite_set_expr_in_place(
    expr: &mut SetExpr,
    rewrite_select: &mut dyn FnMut(&mut Select, &mut bool) -> Result<(), LixError>,
    changed: &mut bool,
) -> Result<(), LixError> {
    match expr {
        SetExpr::Select(select) => {
            rewrite_derived_subqueries_in_select(select.as_mut(), rewrite_select, changed)?;
            rewrite_select(select.as_mut(), changed)
        }
        SetExpr::Query(query) => rewrite_query_in_place(query.as_mut(), rewrite_select, changed),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr_in_place(left.as_mut(), rewrite_select, changed)?;
            rewrite_set_expr_in_place(right.as_mut(), rewrite_select, changed)?;
            Ok(())
        }
        _ => Ok(()),
    }
}

fn rewrite_derived_subqueries_in_select(
    select: &mut Select,
    rewrite_select: &mut dyn FnMut(&mut Select, &mut bool) -> Result<(), LixError>,
    changed: &mut bool,
) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_derived_subqueries_in_table_factor(&mut table.relation, rewrite_select, changed)?;
        for join in &mut table.joins {
            rewrite_derived_subqueries_in_table_factor(
                &mut join.relation,
                rewrite_select,
                changed,
            )?;
        }
    }
    Ok(())
}

fn rewrite_derived_subqueries_in_table_factor(
    relation: &mut TableFactor,
    rewrite_select: &mut dyn FnMut(&mut Select, &mut bool) -> Result<(), LixError>,
    changed: &mut bool,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Derived { subquery, .. } => {
            rewrite_query_in_place(subquery.as_mut(), rewrite_select, changed)?;
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_derived_subqueries_in_table_factor(
                &mut table_with_joins.relation,
                rewrite_select,
                changed,
            )?;
            for join in &mut table_with_joins.joins {
                rewrite_derived_subqueries_in_table_factor(
                    &mut join.relation,
                    rewrite_select,
                    changed,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
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
