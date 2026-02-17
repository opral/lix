use sqlparser::ast::Query;

use crate::sql::steps::state_pushdown::StatePushdown;
use crate::sql::{escape_sql_string, parse_single_query, quote_ident};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    version_descriptor_file_id, version_descriptor_schema_key,
    version_descriptor_storage_version_id, GLOBAL_VERSION_ID,
};
use crate::LixError;

const VTABLE_NAME: &str = "lix_internal_state_vtable";

pub(crate) fn build_effective_state_by_version_query(
    pushdown: &StatePushdown,
    count_only: bool,
) -> Result<Query, LixError> {
    if count_only {
        build_effective_state_by_version_count_query(pushdown)
    } else {
        build_effective_state_by_version_view_query(pushdown)
    }
}

pub(crate) fn build_effective_state_active_query(
    pushdown: &StatePushdown,
    count_only: bool,
) -> Result<Query, LixError> {
    if count_only {
        build_effective_state_active_count_query(pushdown)
    } else {
        build_effective_state_active_view_query(pushdown)
    }
}

fn build_effective_state_by_version_view_query(
    pushdown: &StatePushdown,
) -> Result<Query, LixError> {
    let (target_version_pushdown, ranked_predicates) =
        split_effective_by_version_ranked_pushdown(pushdown);
    let source_pushdown = if pushdown.source_predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", pushdown.source_predicates.join(" AND "))
    };
    let ranked_pushdown = if ranked_predicates.is_empty() {
        String::new()
    } else {
        format!(" AND {}", ranked_predicates.join(" AND "))
    };
    let target_versions_cte =
        build_effective_state_target_versions_cte(&target_version_pushdown, VTABLE_NAME);
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
           WITH RECURSIVE \
             version_descriptor AS ( \
               SELECT \
                 lix_json_text(snapshot_content, 'id') AS version_id, \
                 lix_json_text(snapshot_content, 'inherits_from_version_id') AS inherits_from_version_id \
               FROM {descriptor_table} \
               WHERE schema_key = '{descriptor_schema_key}' \
                 AND file_id = '{descriptor_file_id}' \
                 AND version_id = '{descriptor_storage_version_id}' \
                 AND is_tombstone = 0 \
                 AND snapshot_content IS NOT NULL \
             ), \
             {target_versions_cte}, \
             version_chain(target_version_id, ancestor_version_id, depth) AS ( \
               SELECT \
                 version_id AS target_version_id, \
                 version_id AS ancestor_version_id, \
                 0 AS depth \
               FROM target_versions \
               UNION ALL \
               SELECT \
                 vc.target_version_id, \
                 vd.inherits_from_version_id AS ancestor_version_id, \
                 vc.depth + 1 AS depth \
               FROM version_chain vc \
               JOIN version_descriptor vd \
                 ON vd.version_id = vc.ancestor_version_id \
               WHERE vd.inherits_from_version_id IS NOT NULL \
                 AND vc.depth < 64 \
             ), \
             commit_by_version AS ( \
               SELECT \
                 COALESCE(lix_json_text(snapshot_content, 'id'), entity_id) AS commit_id, \
                 lix_json_text(snapshot_content, 'change_set_id') AS change_set_id \
               FROM {vtable_name} \
               WHERE schema_key = 'lix_commit' \
                 AND version_id = '{global_version}' \
                 AND snapshot_content IS NOT NULL \
             ), \
             change_set_element_by_version AS ( \
               SELECT \
                 lix_json_text(snapshot_content, 'change_set_id') AS change_set_id, \
                 lix_json_text(snapshot_content, 'change_id') AS change_id \
               FROM {vtable_name} \
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
             vc.target_version_id AS version_id, \
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
               PARTITION BY vc.target_version_id, s.entity_id, s.schema_key, s.file_id \
               ORDER BY vc.depth ASC \
             ) AS rn \
           FROM {vtable_name} s \
           JOIN version_chain vc \
             ON vc.ancestor_version_id = s.version_id \
           LEFT JOIN change_commit_by_change_id cc \
             ON cc.change_id = s.change_id \
           {source_pushdown} \
         ) AS ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL\
           {ranked_pushdown}",
        descriptor_table = descriptor_table,
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_storage_version_id = escape_sql_string(version_descriptor_storage_version_id()),
        vtable_name = VTABLE_NAME,
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        source_pushdown = source_pushdown,
        ranked_pushdown = ranked_pushdown,
        target_versions_cte = target_versions_cte,
    );
    parse_single_query(&sql)
}

fn build_effective_state_by_version_count_query(
    pushdown: &StatePushdown,
) -> Result<Query, LixError> {
    let (target_version_pushdown, ranked_predicates) =
        split_effective_by_version_ranked_pushdown(pushdown);
    let source_pushdown = if pushdown.source_predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", pushdown.source_predicates.join(" AND "))
    };
    let ranked_pushdown = if ranked_predicates.is_empty() {
        String::new()
    } else {
        format!(" AND {}", ranked_predicates.join(" AND "))
    };
    let target_versions_cte =
        build_effective_state_target_versions_cte(&target_version_pushdown, VTABLE_NAME);
    let descriptor_table = quote_ident(&format!(
        "lix_internal_state_materialized_v1_{}",
        version_descriptor_schema_key()
    ));
    let sql = format!(
        "SELECT \
             ranked.entity_id AS entity_id \
         FROM ( \
           WITH RECURSIVE \
             version_descriptor AS ( \
               SELECT \
                 lix_json_text(snapshot_content, 'id') AS version_id, \
                 lix_json_text(snapshot_content, 'inherits_from_version_id') AS inherits_from_version_id \
               FROM {descriptor_table} \
               WHERE schema_key = '{descriptor_schema_key}' \
                 AND file_id = '{descriptor_file_id}' \
                 AND version_id = '{descriptor_storage_version_id}' \
                 AND is_tombstone = 0 \
                 AND snapshot_content IS NOT NULL \
             ), \
             {target_versions_cte}, \
             version_chain(target_version_id, ancestor_version_id, depth) AS ( \
               SELECT \
                 version_id AS target_version_id, \
                 version_id AS ancestor_version_id, \
                 0 AS depth \
               FROM target_versions \
               UNION ALL \
               SELECT \
                 vc.target_version_id, \
                 vd.inherits_from_version_id AS ancestor_version_id, \
                 vc.depth + 1 AS depth \
               FROM version_chain vc \
               JOIN version_descriptor vd \
                 ON vd.version_id = vc.ancestor_version_id \
               WHERE vd.inherits_from_version_id IS NOT NULL \
                 AND vc.depth < 64 \
             ) \
           SELECT \
             s.entity_id AS entity_id, \
             s.schema_key AS schema_key, \
             s.file_id AS file_id, \
             vc.target_version_id AS version_id, \
             s.plugin_key AS plugin_key, \
             s.snapshot_content AS snapshot_content, \
             ROW_NUMBER() OVER ( \
               PARTITION BY vc.target_version_id, s.entity_id, s.schema_key, s.file_id \
               ORDER BY vc.depth ASC \
             ) AS rn \
           FROM {vtable_name} s \
           JOIN version_chain vc \
             ON vc.ancestor_version_id = s.version_id \
           {source_pushdown} \
         ) AS ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL\
           {ranked_pushdown}",
        descriptor_table = descriptor_table,
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_storage_version_id = escape_sql_string(version_descriptor_storage_version_id()),
        vtable_name = VTABLE_NAME,
        source_pushdown = source_pushdown,
        ranked_pushdown = ranked_pushdown,
        target_versions_cte = target_versions_cte,
    );
    parse_single_query(&sql)
}

fn build_effective_state_target_versions_cte(
    target_version_pushdown: &[String],
    vtable_name: &str,
) -> String {
    if target_version_pushdown.is_empty() {
        return format!(
            "all_target_versions AS ( \
               SELECT version_id FROM version_descriptor \
               UNION \
               SELECT DISTINCT version_id FROM {vtable_name} \
             ), \
             target_versions AS ( \
               SELECT version_id \
               FROM all_target_versions \
             )",
            vtable_name = vtable_name
        );
    }

    let target_version_filter = target_version_pushdown.join(" AND ");
    if target_version_pushdown.iter().any(|predicate| {
        predicate.contains('?') || predicate.to_ascii_lowercase().contains("select")
    }) {
        return format!(
            "all_target_versions AS ( \
               SELECT version_id FROM version_descriptor \
               UNION \
               SELECT DISTINCT version_id FROM {vtable_name} \
             ), \
             target_versions AS ( \
               SELECT version_id \
               FROM all_target_versions \
               WHERE {target_version_filter} \
             )",
            vtable_name = vtable_name,
            target_version_filter = target_version_filter
        );
    }

    format!(
        "target_versions AS ( \
           SELECT version_id \
           FROM version_descriptor \
           WHERE {target_version_filter} \
           UNION \
           SELECT DISTINCT version_id \
           FROM {vtable_name} \
           WHERE {target_version_filter} \
         )",
        target_version_filter = target_version_filter,
        vtable_name = vtable_name
    )
}

fn split_effective_by_version_ranked_pushdown(
    pushdown: &StatePushdown,
) -> (Vec<String>, Vec<String>) {
    let mut target_version = Vec::new();
    let mut ranked = Vec::new();
    for predicate in &pushdown.ranked_predicates {
        if let Some(stripped) = predicate.strip_prefix("ranked.version_id ") {
            target_version.push(format!("version_id {stripped}"));
            continue;
        }
        ranked.push(predicate.clone());
    }
    (target_version, ranked)
}

fn build_effective_state_active_view_query(pushdown: &StatePushdown) -> Result<Query, LixError> {
    let source_pushdown = if pushdown.source_predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", pushdown.source_predicates.join(" AND "))
    };
    let ranked_pushdown = if pushdown.ranked_predicates.is_empty() {
        String::new()
    } else {
        format!(" AND {}", pushdown.ranked_predicates.join(" AND "))
    };
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
             FROM {vtable_name} \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND snapshot_content IS NOT NULL \
           ), \
           change_set_element_by_version AS ( \
             SELECT \
               lix_json_text(snapshot_content, 'change_set_id') AS change_set_id, \
               lix_json_text(snapshot_content, 'change_id') AS change_id \
             FROM {vtable_name} \
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
           FROM {vtable_name} s \
           JOIN version_chain vc \
             ON vc.version_id = s.version_id \
           LEFT JOIN change_commit_by_change_id cc \
             ON cc.change_id = s.change_id \
           CROSS JOIN active_version av \
           {source_pushdown} \
         ) AS ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL\
           {ranked_pushdown}",
        active_schema_key = escape_sql_string(active_version_schema_key()),
        active_file_id = escape_sql_string(active_version_file_id()),
        active_storage_version_id = escape_sql_string(active_version_storage_version_id()),
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_storage_version_id = escape_sql_string(version_descriptor_storage_version_id()),
        vtable_name = VTABLE_NAME,
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        source_pushdown = source_pushdown,
        ranked_pushdown = ranked_pushdown,
    );
    parse_single_query(&sql)
}

fn build_effective_state_active_count_query(pushdown: &StatePushdown) -> Result<Query, LixError> {
    let source_pushdown = if pushdown.source_predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", pushdown.source_predicates.join(" AND "))
    };
    let ranked_pushdown = if pushdown.ranked_predicates.is_empty() {
        String::new()
    } else {
        format!(" AND {}", pushdown.ranked_predicates.join(" AND "))
    };
    let descriptor_table = quote_ident(&format!(
        "lix_internal_state_materialized_v1_{}",
        version_descriptor_schema_key()
    ));
    let sql = format!(
        "SELECT \
             ranked.entity_id AS entity_id \
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
           ) \
           SELECT \
             s.entity_id AS entity_id, \
             s.schema_key AS schema_key, \
             s.file_id AS file_id, \
             av.version_id AS version_id, \
             s.plugin_key AS plugin_key, \
             s.snapshot_content AS snapshot_content, \
             ROW_NUMBER() OVER ( \
               PARTITION BY s.entity_id, s.schema_key, s.file_id \
               ORDER BY vc.depth ASC \
             ) AS rn \
           FROM {vtable_name} s \
           JOIN version_chain vc \
             ON vc.version_id = s.version_id \
           CROSS JOIN active_version av \
           {source_pushdown} \
         ) AS ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL\
           {ranked_pushdown}",
        active_schema_key = escape_sql_string(active_version_schema_key()),
        active_file_id = escape_sql_string(active_version_file_id()),
        active_storage_version_id = escape_sql_string(active_version_storage_version_id()),
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_storage_version_id = escape_sql_string(version_descriptor_storage_version_id()),
        vtable_name = VTABLE_NAME,
        source_pushdown = source_pushdown,
        ranked_pushdown = ranked_pushdown,
    );
    parse_single_query(&sql)
}
