use sqlparser::ast::{Query, Select, TableFactor};

use crate::engine::sql::planning::rewrite_engine::{
    default_alias, object_name_matches, parse_single_query, rewrite_query_with_select_rewriter,
    rewrite_table_factors_in_select,
};
use crate::LixError;

const LIX_VERSION_VIEW_NAME: &str = "lix_version";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_with_select_rewriter(query, &mut rewrite_select)
}

fn rewrite_select(select: &mut Select, changed: &mut bool) -> Result<(), LixError> {
    rewrite_table_factors_in_select(select, &mut rewrite_table_factor, changed)
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
        _ => {}
    }
    Ok(())
}

fn build_lix_version_view_query() -> Result<Query, LixError> {
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
                   lix_json_extract(snapshot_content, 'id') AS id, \
                   lix_json_extract(snapshot_content, 'name') AS name, \
                   lix_json_extract(snapshot_content, 'inherits_from_version_id') AS inherits_from_version_id, \
                   lix_json_extract(snapshot_content, 'hidden') AS hidden \
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
                   lix_json_extract(snapshot_content, 'id') AS id, \
                   lix_json_extract(snapshot_content, 'commit_id') AS commit_id, \
                   lix_json_extract(snapshot_content, 'working_commit_id') AS working_commit_id \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'lix_version_pointer' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
               ) AS t \
                 ON t.id = d.id";
    parse_single_query(sql)
}
fn default_lix_version_alias() -> sqlparser::ast::TableAlias {
    default_alias(LIX_VERSION_VIEW_NAME)
}
