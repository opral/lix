use sqlparser::ast::{
    Ident, ObjectName, ObjectNamePart, Query, Select, SetExpr, Statement, TableAlias, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

const LIX_STATE_HISTORY_VIEW_NAME: &str = "lix_state_history";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    let mut changed = false;
    let mut new_query = query.clone();
    if let Some(with) = new_query.with.as_mut() {
        for cte in &mut with.cte_tables {
            if let Some(rewritten) = rewrite_query((*cte.query).clone())? {
                cte.query = Box::new(rewritten);
                changed = true;
            }
        }
    }
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
            if object_name_matches(name, LIX_STATE_HISTORY_VIEW_NAME) =>
        {
            let derived_query = build_lix_state_history_view_query()?;
            let derived_alias = alias
                .clone()
                .or_else(|| Some(default_lix_state_history_alias()));
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

fn build_lix_state_history_view_query() -> Result<Query, LixError> {
    let sql = format!(
        "SELECT \
           es.entity_id AS entity_id, \
           es.schema_key AS schema_key, \
           es.file_id AS file_id, \
           es.plugin_key AS plugin_key, \
           es.snapshot_content AS snapshot_content, \
           es.metadata AS metadata, \
           es.schema_version AS schema_version, \
           es.target_change_id AS change_id, \
           es.origin_commit_id AS commit_id, \
           es.root_commit_id AS root_commit_id, \
           es.commit_depth AS depth, \
           '{global_version}' AS version_id \
         FROM ( \
           WITH RECURSIVE \
             commit_by_version AS ( \
               SELECT \
                 COALESCE(lix_json_text(snapshot_content, 'id'), entity_id) AS id, \
                 lix_json_text(snapshot_content, 'change_set_id') AS change_set_id, \
                 version_id AS lixcol_version_id \
               FROM lix_internal_state_vtable \
               WHERE schema_key = 'lix_commit' \
                 AND version_id = '{global_version}' \
                 AND snapshot_content IS NOT NULL \
             ), \
             change_set_element_by_version AS ( \
               SELECT \
                 lix_json_text(snapshot_content, 'change_set_id') AS change_set_id, \
                 lix_json_text(snapshot_content, 'change_id') AS change_id, \
                 lix_json_text(snapshot_content, 'entity_id') AS entity_id, \
                 lix_json_text(snapshot_content, 'schema_key') AS schema_key, \
                 lix_json_text(snapshot_content, 'file_id') AS file_id, \
                 version_id AS lixcol_version_id \
               FROM lix_internal_state_vtable \
               WHERE schema_key = 'lix_change_set_element' \
                 AND version_id = '{global_version}' \
                 AND snapshot_content IS NOT NULL \
             ), \
             commit_edge_by_version AS ( \
               SELECT \
                 lix_json_text(snapshot_content, 'parent_id') AS parent_id, \
                 lix_json_text(snapshot_content, 'child_id') AS child_id, \
                 version_id AS lixcol_version_id \
               FROM lix_internal_state_vtable \
               WHERE schema_key = 'lix_commit_edge' \
                 AND version_id = '{global_version}' \
                 AND snapshot_content IS NOT NULL \
             ), \
             all_changes_with_snapshots AS ( \
               SELECT \
                 ic.id, \
                 ic.entity_id, \
                 ic.schema_key, \
                 ic.file_id, \
                 ic.plugin_key, \
                 ic.schema_version, \
                 ic.created_at, \
                 CASE \
                   WHEN ic.snapshot_id = 'no-content' THEN NULL \
                   ELSE s.content \
                 END AS snapshot_content, \
                 ic.metadata AS metadata \
               FROM lix_internal_change ic \
               LEFT JOIN lix_internal_snapshot s ON s.id = ic.snapshot_id \
             ), \
             requested_commits AS ( \
               SELECT DISTINCT c.id AS commit_id \
               FROM commit_by_version c \
               WHERE c.lixcol_version_id = '{global_version}' \
             ), \
             reachable_commits_from_requested(id, root_commit_id, depth) AS ( \
               SELECT \
                 commit_id, \
                 commit_id AS root_commit_id, \
                 0 AS depth \
               FROM requested_commits \
               UNION ALL \
               SELECT \
                 ce.parent_id, \
                 r.root_commit_id, \
                 r.depth + 1 \
               FROM commit_edge_by_version ce \
               JOIN reachable_commits_from_requested r ON ce.child_id = r.id \
               WHERE ce.lixcol_version_id = '{global_version}' \
                 AND r.depth < 512 \
             ), \
             commit_changesets AS ( \
               SELECT \
                 c.id AS commit_id, \
                 c.change_set_id AS change_set_id, \
                 rc.root_commit_id, \
                 rc.depth AS commit_depth \
               FROM commit_by_version c \
               JOIN reachable_commits_from_requested rc ON c.id = rc.id \
               WHERE c.lixcol_version_id = '{global_version}' \
             ), \
             cse_in_reachable_commits AS ( \
               SELECT \
                 cse_raw.entity_id AS target_entity_id, \
                 cse_raw.file_id AS target_file_id, \
                 cse_raw.schema_key AS target_schema_key, \
                 cse_raw.change_id AS target_change_id, \
                 cc_raw.commit_id AS origin_commit_id, \
                 cc_raw.root_commit_id AS root_commit_id, \
                 cc_raw.commit_depth AS commit_depth \
               FROM change_set_element_by_version cse_raw \
               JOIN commit_changesets cc_raw \
                 ON cse_raw.change_set_id = cc_raw.change_set_id \
               WHERE cse_raw.lixcol_version_id = '{global_version}' \
             ), \
             ranked_cse AS ( \
               SELECT \
                 r.target_entity_id, \
                 r.target_file_id, \
                 r.target_schema_key, \
                 r.target_change_id, \
                 r.origin_commit_id, \
                 r.root_commit_id, \
                 r.commit_depth, \
                 ROW_NUMBER() OVER ( \
                   PARTITION BY \
                     r.target_entity_id, \
                     r.target_file_id, \
                     r.target_schema_key, \
                     r.root_commit_id, \
                     r.commit_depth \
                   ORDER BY \
                     target_change.created_at DESC, \
                     target_change.id DESC \
                 ) AS rn \
               FROM cse_in_reachable_commits r \
               JOIN all_changes_with_snapshots target_change \
                 ON target_change.id = r.target_change_id \
             ) \
           SELECT \
             target_change.entity_id AS entity_id, \
             target_change.schema_key AS schema_key, \
             target_change.file_id AS file_id, \
             target_change.plugin_key AS plugin_key, \
             target_change.snapshot_content AS snapshot_content, \
             target_change.metadata AS metadata, \
             target_change.schema_version AS schema_version, \
             ranked.target_change_id AS target_change_id, \
             ranked.origin_commit_id AS origin_commit_id, \
             ranked.root_commit_id AS root_commit_id, \
             ranked.commit_depth AS commit_depth \
           FROM ranked_cse ranked \
           JOIN all_changes_with_snapshots target_change \
             ON target_change.id = ranked.target_change_id \
           WHERE ranked.rn = 1 \
         ) AS es \
         WHERE es.snapshot_content IS NOT NULL",
        global_version = GLOBAL_VERSION_ID,
    );
    parse_single_query(&sql)
}

fn default_lix_state_history_alias() -> TableAlias {
    TableAlias {
        explicit: false,
        name: Ident::new(LIX_STATE_HISTORY_VIEW_NAME),
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
