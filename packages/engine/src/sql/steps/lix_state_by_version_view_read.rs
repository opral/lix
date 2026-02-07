use sqlparser::ast::{
    BinaryOperator, Expr, Ident, ObjectName, ObjectNamePart, Query, Select, SetExpr, TableFactor,
    TableWithJoins,
};

use crate::LixError;

const LIX_STATE_BY_VERSION_VIEW_NAME: &str = "lix_state_by_version";
const VTABLE_NAME: &str = "lix_internal_state_vtable";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    if !top_level_select_targets_lix_state_by_version(&query) {
        return Ok(None);
    }

    let mut changed = false;
    let mut new_query = query.clone();
    new_query.body = Box::new(rewrite_set_expr(*query.body, &mut changed)?);

    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

fn top_level_select_targets_lix_state_by_version(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select
        .from
        .iter()
        .any(table_with_joins_targets_lix_state_by_version)
}

fn table_with_joins_targets_lix_state_by_version(table: &TableWithJoins) -> bool {
    table_factor_is_lix_state_by_version(&table.relation)
        || table
            .joins
            .iter()
            .any(|join| table_factor_is_lix_state_by_version(&join.relation))
}

fn table_factor_is_lix_state_by_version(relation: &TableFactor) -> bool {
    matches!(
        relation,
        TableFactor::Table { name, .. } if object_name_matches(name, LIX_STATE_BY_VERSION_VIEW_NAME)
    )
}

fn rewrite_set_expr(expr: SetExpr, changed: &mut bool) -> Result<SetExpr, LixError> {
    Ok(match expr {
        SetExpr::Select(select) => {
            let mut select = *select;
            rewrite_select(&mut select, changed)?;
            SetExpr::Select(Box::new(select))
        }
        SetExpr::Query(query) => {
            let mut query = *query;
            query.body = Box::new(rewrite_set_expr(*query.body, changed)?);
            SetExpr::Query(Box::new(query))
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => SetExpr::SetOperation {
            op,
            set_quantifier,
            left: Box::new(rewrite_set_expr(*left, changed)?),
            right: Box::new(rewrite_set_expr(*right, changed)?),
        },
        other => other,
    })
}

fn rewrite_select(select: &mut Select, changed: &mut bool) -> Result<(), LixError> {
    let mut has_state_target = false;
    for table in &mut select.from {
        if rewrite_table_with_joins(table, changed)? {
            has_state_target = true;
        }
    }

    if !has_state_target {
        return Ok(());
    }

    let visible_predicate = snapshot_not_null_predicate_expr();
    select.selection = Some(match select.selection.take() {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(existing),
            op: BinaryOperator::And,
            right: Box::new(visible_predicate),
        },
        None => visible_predicate,
    });
    *changed = true;
    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    changed: &mut bool,
) -> Result<bool, LixError> {
    let mut rewrote_target = rewrite_table_factor(&mut table.relation, changed)?;
    for join in &mut table.joins {
        if rewrite_table_factor(&mut join.relation, changed)? {
            rewrote_target = true;
        }
    }
    Ok(rewrote_target)
}

fn rewrite_table_factor(relation: &mut TableFactor, changed: &mut bool) -> Result<bool, LixError> {
    match relation {
        TableFactor::Table { name, .. }
            if object_name_matches(name, LIX_STATE_BY_VERSION_VIEW_NAME) =>
        {
            *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(VTABLE_NAME))]);
            *changed = true;
            Ok(true)
        }
        TableFactor::Derived { subquery, .. } => {
            if let Some(rewritten) = rewrite_query((**subquery).clone())? {
                *subquery = Box::new(rewritten);
                *changed = true;
            }
            Ok(false)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => rewrite_table_with_joins(table_with_joins, changed),
        _ => Ok(false),
    }
}

fn snapshot_not_null_predicate_expr() -> Expr {
    Expr::IsNotNull(Box::new(Expr::Identifier(Ident::new("snapshot_content"))))
}

fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}
