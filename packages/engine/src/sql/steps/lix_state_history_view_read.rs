use sqlparser::ast::{Query, Select, TableFactor};

use crate::sql::{
    default_alias, object_name_matches, parse_single_query, rewrite_query_with_select_rewriter,
    rewrite_table_factors_in_select,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

const LIX_STATE_HISTORY_VIEW_NAME: &str = "lix_state_history";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_with_select_rewriter(query, &mut rewrite_select)
}

fn rewrite_select(select: &mut Select, changed: &mut bool) -> Result<(), LixError> {
    rewrite_table_factors_in_select(select, &mut rewrite_table_factor, changed)
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

fn default_lix_state_history_alias() -> sqlparser::ast::TableAlias {
    default_alias(LIX_STATE_HISTORY_VIEW_NAME)
}
