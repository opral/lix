use sqlparser::ast::{
    BinaryOperator, Expr, Ident, ObjectName, ObjectNamePart, Query, Select, SetExpr, Statement,
    TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
};
use crate::LixError;

const LIX_STATE_VIEW_NAME: &str = "lix_state";
const VTABLE_NAME: &str = "lix_internal_state_vtable";
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    if !top_level_select_targets_lix_state(&query) {
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

fn top_level_select_targets_lix_state(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select.from.iter().any(table_with_joins_targets_lix_state)
}

fn table_with_joins_targets_lix_state(table: &TableWithJoins) -> bool {
    table_factor_is_lix_state(&table.relation)
        || table
            .joins
            .iter()
            .any(|join| table_factor_is_lix_state(&join.relation))
}

fn table_factor_is_lix_state(relation: &TableFactor) -> bool {
    matches!(
        relation,
        TableFactor::Table { name, .. } if object_name_matches(name, LIX_STATE_VIEW_NAME)
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

    let version_predicate = active_version_predicate_expr()?;
    select.selection = Some(match select.selection.take() {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(existing),
            op: BinaryOperator::And,
            right: Box::new(version_predicate),
        },
        None => version_predicate,
    });
    *changed = true;

    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    changed: &mut bool,
) -> Result<bool, LixError> {
    let mut rewrote_state_target = rewrite_table_factor(&mut table.relation, changed)?;
    for join in &mut table.joins {
        if rewrite_table_factor(&mut join.relation, changed)? {
            rewrote_state_target = true;
        }
    }
    Ok(rewrote_state_target)
}

fn rewrite_table_factor(relation: &mut TableFactor, changed: &mut bool) -> Result<bool, LixError> {
    match relation {
        TableFactor::Table { name, .. } if object_name_matches(name, LIX_STATE_VIEW_NAME) => {
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

fn active_version_predicate_expr() -> Result<Expr, LixError> {
    let sql = format!(
        "version_id = (\
         SELECT lix_json_text(snapshot_content, 'version_id') \
         FROM {untracked_table} \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{storage_version_id}' \
           AND snapshot_content IS NOT NULL \
         LIMIT 1\
         )",
        untracked_table = UNTRACKED_TABLE,
        schema_key = escape_sql_string(active_version_schema_key()),
        file_id = escape_sql_string(active_version_file_id()),
        storage_version_id = escape_sql_string(active_version_storage_version_id()),
    );
    parse_predicate_expr(&sql)
}

fn parse_predicate_expr(predicate_sql: &str) -> Result<Expr, LixError> {
    let sql = format!("SELECT 1 WHERE {predicate_sql}");
    let mut statements = Parser::parse_sql(&GenericDialect {}, &sql).map_err(|error| LixError {
        message: format!("failed to parse predicate expression: {error}"),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "expected a single predicate expression statement".to_string(),
        });
    }

    let statement = statements.remove(0);
    let Statement::Query(query) = statement else {
        return Err(LixError {
            message: "predicate expression did not parse as SELECT".to_string(),
        });
    };

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(LixError {
            message: "predicate expression did not parse as SELECT body".to_string(),
        });
    };

    select.selection.as_ref().cloned().ok_or_else(|| LixError {
        message: "predicate expression is missing WHERE clause".to_string(),
    })
}

fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
