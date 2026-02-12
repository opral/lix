use sqlparser::ast::{
    Ident, ObjectName, ObjectNamePart, Query, Select, SetExpr, Statement, TableAlias, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::sql::escape_sql_string;
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    version_descriptor_file_id, version_descriptor_schema_key,
    version_descriptor_storage_version_id, GLOBAL_VERSION_ID,
};
use crate::LixError;

const LIX_STATE_VIEW_NAME: &str = "lix_state";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    if !query_targets_lix_state(&query) {
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

fn query_targets_lix_state(query: &Query) -> bool {
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
            if object_name_matches(name, LIX_STATE_VIEW_NAME) =>
        {
            let derived_query = build_lix_state_view_query()?;
            let derived_alias = alias.clone().or_else(|| Some(default_lix_state_alias()));
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

fn build_lix_state_view_query() -> Result<Query, LixError> {
    let descriptor_table = quote_ident(&format!(
        "lix_internal_state_materialized_v1_{}",
        version_descriptor_schema_key()
    ));
    let sql = format!(
        "SELECT \
             ranked.entity_id AS entity_id, \
             ranked.schema_key AS schema_key, \
             ranked.file_id AS file_id, \
             ranked.version_id AS version_id, \
             ranked.plugin_key AS plugin_key, \
             ranked.snapshot_content AS snapshot_content, \
             ranked.schema_version AS schema_version, \
             ranked.created_at AS created_at, \
             ranked.updated_at AS updated_at, \
             ranked.inherited_from_version_id AS inherited_from_version_id, \
             ranked.change_id AS change_id, \
             ranked.commit_id AS commit_id, \
             ranked.untracked AS untracked, \
             ranked.writer_key AS writer_key, \
             ranked.metadata AS metadata \
         FROM ( \
           WITH RECURSIVE active_version AS ( \
             SELECT lix_json_text(snapshot_content, 'version_id') AS version_id \
             FROM lix_internal_state_untracked \
             WHERE schema_key = '{active_schema_key}' \
               AND file_id = '{active_file_id}' \
               AND version_id = '{active_storage_version_id}' \
               AND snapshot_content IS NOT NULL \
             ORDER BY updated_at DESC \
             LIMIT 1 \
           ), \
           version_chain(version_id, depth) AS ( \
             SELECT version_id, 0 AS depth \
             FROM active_version \
             UNION ALL \
             SELECT \
               lix_json_text(vd.snapshot_content, 'inherits_from_version_id') AS version_id, \
               vc.depth + 1 AS depth \
             FROM version_chain vc \
             JOIN {descriptor_table} vd \
               ON lix_json_text(vd.snapshot_content, 'id') = vc.version_id \
             WHERE vd.schema_key = '{descriptor_schema_key}' \
               AND vd.file_id = '{descriptor_file_id}' \
               AND vd.version_id = '{descriptor_storage_version_id}' \
               AND vd.is_tombstone = 0 \
               AND vd.snapshot_content IS NOT NULL \
               AND lix_json_text(vd.snapshot_content, 'inherits_from_version_id') IS NOT NULL \
               AND vc.depth < 64 \
           ), \
           commit_by_version AS ( \
             SELECT \
               COALESCE(lix_json_text(snapshot_content, 'id'), entity_id) AS commit_id, \
               lix_json_text(snapshot_content, 'change_set_id') AS change_set_id \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND snapshot_content IS NOT NULL \
           ), \
           change_set_element_by_version AS ( \
             SELECT \
               lix_json_text(snapshot_content, 'change_set_id') AS change_set_id, \
               lix_json_text(snapshot_content, 'change_id') AS change_id \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_change_set_element' \
               AND version_id = '{global_version}' \
               AND snapshot_content IS NOT NULL \
           ), \
           change_commit_by_change_id AS ( \
             SELECT \
               cse.change_id AS change_id, \
               MAX(cbv.commit_id) AS commit_id \
             FROM change_set_element_by_version cse \
             JOIN commit_by_version cbv \
               ON cbv.change_set_id = cse.change_set_id \
             WHERE cse.change_id IS NOT NULL \
             GROUP BY cse.change_id \
           ) \
           SELECT \
             s.entity_id AS entity_id, \
             s.schema_key AS schema_key, \
             s.file_id AS file_id, \
             av.version_id AS version_id, \
             s.plugin_key AS plugin_key, \
             s.snapshot_content AS snapshot_content, \
             s.schema_version AS schema_version, \
             s.created_at AS created_at, \
             s.updated_at AS updated_at, \
             CASE \
               WHEN s.inherited_from_version_id IS NOT NULL THEN s.inherited_from_version_id \
               WHEN vc.depth = 0 THEN NULL \
               ELSE s.version_id \
             END AS inherited_from_version_id, \
             s.change_id AS change_id, \
             COALESCE(cc.commit_id, CASE WHEN s.untracked = 1 THEN 'untracked' ELSE NULL END) AS commit_id, \
             s.untracked AS untracked, \
             s.writer_key AS writer_key, \
             s.metadata AS metadata, \
             ROW_NUMBER() OVER ( \
               PARTITION BY s.entity_id, s.schema_key, s.file_id \
               ORDER BY vc.depth ASC \
             ) AS rn \
           FROM lix_internal_state_vtable s \
           JOIN version_chain vc \
             ON vc.version_id = s.version_id \
           LEFT JOIN change_commit_by_change_id cc \
             ON cc.change_id = s.change_id \
           CROSS JOIN active_version av \
         ) AS ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL",
        active_schema_key = escape_sql_string(active_version_schema_key()),
        active_file_id = escape_sql_string(active_version_file_id()),
        active_storage_version_id = escape_sql_string(active_version_storage_version_id()),
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_storage_version_id = escape_sql_string(version_descriptor_storage_version_id()),
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
    );
    parse_single_query(&sql)
}

fn default_lix_state_alias() -> TableAlias {
    TableAlias {
        explicit: false,
        name: Ident::new(LIX_STATE_VIEW_NAME),
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

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}
