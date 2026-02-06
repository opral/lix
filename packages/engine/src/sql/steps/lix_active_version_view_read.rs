use sqlparser::ast::{
    Ident, ObjectName, ObjectNamePart, Query, Select, SetExpr, Statement, TableAlias, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
};
use crate::LixError;

const LIX_ACTIVE_VERSION_VIEW_NAME: &str = "lix_active_version";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    if !top_level_select_targets_lix_active_version(&query) {
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

fn top_level_select_targets_lix_active_version(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select
        .from
        .iter()
        .any(table_with_joins_targets_lix_active_version)
}

fn table_with_joins_targets_lix_active_version(table: &TableWithJoins) -> bool {
    table_factor_is_lix_active_version(&table.relation)
        || table
            .joins
            .iter()
            .any(|join| table_factor_is_lix_active_version(&join.relation))
}

fn table_factor_is_lix_active_version(relation: &TableFactor) -> bool {
    matches!(
        relation,
        TableFactor::Table { name, .. } if object_name_matches(name, LIX_ACTIVE_VERSION_VIEW_NAME)
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
    for table in &mut select.from {
        rewrite_table_with_joins(table, changed)?;
    }
    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    changed: &mut bool,
) -> Result<(), LixError> {
    rewrite_table_factor(&mut table.relation, changed)?;
    for join in &mut table.joins {
        rewrite_table_factor(&mut join.relation, changed)?;
    }
    Ok(())
}

fn rewrite_table_factor(relation: &mut TableFactor, changed: &mut bool) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if object_name_matches(name, LIX_ACTIVE_VERSION_VIEW_NAME) =>
        {
            let derived_query = build_lix_active_version_view_query()?;
            let derived_alias = alias
                .clone()
                .or_else(|| Some(default_lix_active_version_alias()));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            *changed = true;
        }
        TableFactor::Derived { subquery, .. } => {
            if let Some(rewritten) = rewrite_query((**subquery).clone())? {
                *subquery = Box::new(rewritten);
                *changed = true;
            }
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_with_joins(table_with_joins, changed)?;
        }
        _ => {}
    }
    Ok(())
}

fn build_lix_active_version_view_query() -> Result<Query, LixError> {
    let sql = format!(
        "SELECT \
             entity_id AS id, \
             lix_json_text(snapshot_content, 'version_id') AS version_id, \
             schema_key, \
             file_id, \
             version_id AS lixcol_version_id, \
             schema_version, \
             untracked, \
             created_at, \
             updated_at, \
             change_id \
         FROM lix_internal_state_vtable \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{storage_version_id}' \
           AND untracked = 1 \
           AND snapshot_content IS NOT NULL",
        schema_key = escape_sql_string(active_version_schema_key()),
        file_id = escape_sql_string(active_version_file_id()),
        storage_version_id = escape_sql_string(active_version_storage_version_id()),
    );
    parse_single_query(&sql)
}

fn default_lix_active_version_alias() -> TableAlias {
    TableAlias {
        explicit: false,
        name: Ident::new(LIX_ACTIVE_VERSION_VIEW_NAME),
        columns: Vec::new(),
    }
}

fn parse_single_query(sql: &str) -> Result<Query, LixError> {
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
