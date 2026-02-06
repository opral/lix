use sqlparser::ast::{
    Ident, ObjectName, Query, Select, SetExpr, Statement, TableAlias, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::LixError;

const LIX_VERSION_VIEW_NAME: &str = "lix_version";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    let mut changed = false;
    let mut new_query = query.clone();
    new_query.body = Box::new(rewrite_set_expr(*query.body, &mut changed)?);

    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
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
            if object_name_matches(name, LIX_VERSION_VIEW_NAME) =>
        {
            let derived_query = build_lix_version_view_query()?;
            let derived_alias = alias.clone().or_else(|| Some(default_lix_version_alias()));
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

fn build_lix_version_view_query() -> Result<Query, LixError> {
    let dialect = GenericDialect {};
    let sql = "SELECT \
                 d.id AS id, \
                 d.name AS name, \
                 d.inherits_from_version_id AS inherits_from_version_id, \
                 d.hidden AS hidden, \
                 t.commit_id AS commit_id, \
                 t.working_commit_id AS working_commit_id, \
                 d.entity_id AS entity_id, \
                 'lix_version' AS schema_key, \
                 d.file_id AS file_id, \
                 d.version_id AS version_id, \
                 'lix' AS plugin_key, \
                 d.schema_version AS schema_version, \
                 COALESCE(t.change_id, d.change_id) AS change_id, \
                 COALESCE(d.created_at, t.created_at) AS created_at, \
                 COALESCE(t.updated_at, d.updated_at) AS updated_at, \
                 0 AS untracked \
               FROM ( \
                 SELECT \
                   entity_id, \
                   file_id, \
                   version_id, \
                   schema_version, \
                   change_id, \
                   created_at, \
                   updated_at, \
                   lix_json_text(snapshot_content, 'id') AS id, \
                   lix_json_text(snapshot_content, 'name') AS name, \
                   lix_json_text(snapshot_content, 'inherits_from_version_id') AS inherits_from_version_id, \
                   lix_json_text(snapshot_content, 'hidden') AS hidden \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_version_descriptor' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
               ) AS d \
               LEFT JOIN ( \
                 SELECT \
                   entity_id, \
                   change_id, \
                   created_at, \
                   updated_at, \
                   lix_json_text(snapshot_content, 'id') AS id, \
                   lix_json_text(snapshot_content, 'commit_id') AS commit_id, \
                   lix_json_text(snapshot_content, 'working_commit_id') AS working_commit_id \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_version_tip' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
               ) AS t \
                 ON t.id = d.id";

    let mut statements = Parser::parse_sql(&dialect, sql).map_err(|err| LixError {
        message: err.to_string(),
    })?;

    if statements.len() != 1 {
        return Err(LixError {
            message: "expected single derived query statement".to_string(),
        });
    }

    match statements.remove(0) {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            message: "derived query did not parse as SELECT".to_string(),
        }),
    }
}

fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}

fn default_lix_version_alias() -> TableAlias {
    TableAlias {
        explicit: false,
        name: Ident::new(LIX_VERSION_VIEW_NAME),
        columns: Vec::new(),
    }
}
