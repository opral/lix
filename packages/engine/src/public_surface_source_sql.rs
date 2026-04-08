//! Compiler-owned public surface source SQL builders.

use crate::schema::annotations::writer_key::WORKSPACE_WRITER_KEY_TABLE;
use crate::contracts::artifacts::EffectiveStateRequest;
use crate::contracts::surface::{SurfaceBinding, SurfaceVariant};
use crate::schema::access::{
    normalized_projection_sql_for_schema, payload_column_name_for_schema,
    snapshot_select_expr_for_schema, tracked_relation_name,
};
use crate::sql::physical_plan::public_surface_sql_support::{
    entity_surface_payload_alias, entity_surface_uses_payload_alias, escape_sql_string,
    expr_contains_string_literal, json_array_text_join_sql, quote_ident, render_identifier,
    render_qualified_where_clause_sql, render_where_clause_sql,
    split_effective_state_pushdown_predicates,
};
use crate::version_state::{
    version_descriptor_schema_key, version_ref_schema_key, GLOBAL_VERSION_ID,
};
use crate::{LixError, SqlDialect};
use serde_json::Value as JsonValue;
use sqlparser::ast::Expr;
use std::collections::BTreeMap;

pub(crate) fn build_effective_public_read_source_sql(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    effective_state_request: &EffectiveStateRequest,
    surface_binding: &SurfaceBinding,
    pushdown_predicates: &[Expr],
    known_live_layouts: &BTreeMap<String, JsonValue>,
    include_snapshot_content: bool,
) -> Result<String, LixError> {
    let schema_keys = effective_state_request
        .schema_set
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    if schema_keys.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "public state read lowerer requires a bounded schema set for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    }

    let (target_version_predicates, source_predicates) =
        split_effective_state_pushdown_predicates(pushdown_predicates);
    let commit_table = tracked_relation_name("lix_commit");
    let cse_table = tracked_relation_name("lix_change_set_element");
    let commit_change_set_id_column =
        quote_ident(&builtin_payload_column_name("lix_commit", "change_set_id"));
    let cse_change_set_id_column = quote_ident(&builtin_payload_column_name(
        "lix_change_set_element",
        "change_set_id",
    ));
    let cse_change_id_column = quote_ident(&builtin_payload_column_name(
        "lix_change_set_element",
        "change_id",
    ));
    let target_versions_cte = match surface_binding.descriptor.surface_variant {
        SurfaceVariant::Default => {
            active_target_versions_cte_sql(active_version_id.ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "public read '{}' requires a session-requested active version id",
                        surface_binding.descriptor.public_name
                    ),
                )
            })?)
        }
        SurfaceVariant::ByVersion => {
            explicit_target_versions_cte_sql(&schema_keys, &target_version_predicates)
        }
        _ => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "state lowerer expected default or by-version surface".to_string(),
            });
        }
    };
    let schema_winner_rows_sql = effective_state_schema_winner_rows_sql(
        dialect,
        surface_binding,
        &schema_keys,
        &source_predicates,
        effective_state_request,
        known_live_layouts,
        include_snapshot_content,
    );
    Ok(format!(
        "WITH \
           {target_versions_cte}, \
           commit_by_version AS ( \
             SELECT \
               entity_id AS commit_id, \
               {commit_change_set_id_column} AS change_set_id \
             FROM {commit_table} \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
           ), \
           change_set_element_by_version AS ( \
             SELECT \
               {cse_change_set_id_column} AS change_set_id, \
               {cse_change_id_column} AS change_id \
             FROM {cse_table} \
             WHERE schema_key = 'lix_change_set_element' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
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
         {schema_winner_rows_sql}",
        target_versions_cte = target_versions_cte,
        schema_winner_rows_sql = schema_winner_rows_sql,
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        commit_table = commit_table,
        cse_table = cse_table,
        commit_change_set_id_column = commit_change_set_id_column,
        cse_change_set_id_column = cse_change_set_id_column,
        cse_change_id_column = cse_change_id_column,
    ))
}

pub(crate) fn build_working_changes_public_read_source_sql(
    dialect: SqlDialect,
    active_version_id: &str,
) -> String {
    let version_ref_table = tracked_relation_name("lix_version_ref");
    let version_ref_commit_id_column = quote_ident(&builtin_payload_column_name(
        version_ref_schema_key(),
        "commit_id",
    ));
    let (parent_join_sql, parent_value_expr) = json_array_text_join_sql(
        dialect,
        "commit_rows.commit_snapshot_content",
        "parent_commit_ids",
        "parent_rows",
        "parent_id",
    );
    let (change_id_join_sql, change_id_value_expr) = json_array_text_join_sql(
        dialect,
        "commit_rows.commit_snapshot_content",
        "change_ids",
        "member_change_rows",
        "change_id",
    );
    let active_version_cte = format!(
        "active_version AS ( \
            SELECT '{active_version_id}' AS version_id \
        )",
        active_version_id = escape_sql_string(active_version_id),
    );

    format!(
        "WITH RECURSIVE \
            {active_version_cte}, \
            scope_heads AS ( \
                SELECT \
                    'local' AS scope, \
                    (SELECT version_id FROM active_version) AS checkpoint_version_id, \
                    ( \
                        SELECT {version_ref_commit_id_column} \
                        FROM {version_ref_table} \
                        WHERE file_id = 'lix' \
                          AND entity_id = (SELECT version_id FROM active_version) \
                          AND version_id = 'global' \
                          AND untracked = true \
                          AND {version_ref_commit_id_column} IS NOT NULL \
                        LIMIT 1 \
                    ) AS head_commit_id \
                UNION ALL \
                SELECT \
                    'global' AS scope, \
                    'global' AS checkpoint_version_id, \
                    ( \
                        SELECT {version_ref_commit_id_column} \
                        FROM {version_ref_table} \
                        WHERE file_id = 'lix' \
                          AND entity_id = 'global' \
                          AND version_id = 'global' \
                          AND untracked = true \
                          AND {version_ref_commit_id_column} IS NOT NULL \
                        LIMIT 1 \
                    ) AS head_commit_id \
            ), \
            scope_baselines AS ( \
                SELECT \
                    scope, \
                    head_commit_id, \
                    COALESCE( \
                        ( \
                            SELECT checkpoint_commit_id \
                            FROM lix_internal_last_checkpoint \
                            WHERE version_id = checkpoint_version_id \
                            LIMIT 1 \
                        ), \
                        head_commit_id \
                    ) AS baseline_commit_id \
                FROM scope_heads \
            ), \
            commit_rows AS ( \
                SELECT \
                    commit_change.entity_id AS id, \
                    commit_change.created_at AS created_at, \
                    commit_snapshot.content AS commit_snapshot_content \
                FROM lix_internal_change commit_change \
                LEFT JOIN lix_internal_snapshot commit_snapshot \
                    ON commit_snapshot.id = commit_change.snapshot_id \
                WHERE commit_change.schema_key = 'lix_commit' \
                  AND commit_snapshot.content IS NOT NULL \
            ), \
            change_rows AS ( \
                SELECT \
                    ch.id AS change_id, \
                    ch.entity_id AS entity_id, \
                    ch.schema_key AS schema_key, \
                    ch.file_id AS file_id, \
                    snap.content AS row_snapshot \
                FROM lix_internal_change ch \
                LEFT JOIN lix_internal_snapshot snap \
                    ON snap.id = ch.snapshot_id \
            ), \
            commit_edges AS ( \
                SELECT \
                    {parent_value_expr} AS parent_id, \
                    commit_rows.id AS child_id \
                FROM commit_rows \
                {parent_join_sql} \
                WHERE {parent_value_expr} IS NOT NULL \
            ), \
            commit_members AS ( \
                SELECT \
                    commit_rows.id AS commit_id, \
                    commit_rows.created_at AS commit_created_at, \
                    {change_id_value_expr} AS change_id \
                FROM commit_rows \
                {change_id_join_sql} \
                WHERE {change_id_value_expr} IS NOT NULL \
            ), \
            tip_ancestry_walk AS ( \
                SELECT \
                    scope.scope AS scope, \
                    scope.head_commit_id AS commit_id, \
                    0 AS depth \
                FROM scope_baselines scope \
                UNION ALL \
                SELECT \
                    walk.scope AS scope, \
                    edge.parent_id AS commit_id, \
                    walk.depth + 1 AS depth \
                FROM tip_ancestry_walk walk \
                JOIN commit_edges edge \
                    ON edge.child_id = walk.commit_id \
                AND walk.depth < 512 \
            ), \
            tip_ancestry AS ( \
                SELECT scope, commit_id, MIN(depth) AS depth \
                FROM tip_ancestry_walk \
                GROUP BY scope, commit_id \
            ), \
            baseline_ancestry_walk AS ( \
                SELECT \
                    scope.scope AS scope, \
                    scope.baseline_commit_id AS commit_id, \
                    0 AS depth \
                FROM scope_baselines scope \
                UNION ALL \
                SELECT \
                    walk.scope AS scope, \
                    edge.parent_id AS commit_id, \
                    walk.depth + 1 AS depth \
                FROM baseline_ancestry_walk walk \
                JOIN commit_edges edge \
                    ON edge.child_id = walk.commit_id \
                  AND walk.depth < 512 \
            ), \
            baseline_ancestry AS ( \
                SELECT scope, commit_id, MIN(depth) AS depth \
                FROM baseline_ancestry_walk \
                GROUP BY scope, commit_id \
            ), \
            tip_candidates AS ( \
                SELECT \
                    anc.scope AS scope, \
                    ch.entity_id, \
                    ch.schema_key, \
                    ch.file_id, \
                    members.change_id, \
                    anc.depth, \
                    members.commit_created_at AS commit_created_at \
                FROM tip_ancestry anc \
                JOIN commit_members members \
                    ON members.commit_id = anc.commit_id \
                JOIN change_rows ch \
                    ON ch.change_id = members.change_id \
            ), \
            tip_min_depth AS ( \
                SELECT \
                    scope, \
                    entity_id, \
                    schema_key, \
                    file_id, \
                    MIN(depth) AS depth \
                FROM tip_candidates \
                GROUP BY scope, entity_id, schema_key, file_id \
            ), \
            tip_best_created_at AS ( \
                SELECT \
                    tc.scope, \
                    tc.entity_id, \
                    tc.schema_key, \
                    tc.file_id, \
                    MAX(tc.commit_created_at) AS commit_created_at \
                FROM tip_candidates tc \
                JOIN tip_min_depth d \
                    ON d.scope = tc.scope \
                   AND d.entity_id = tc.entity_id \
                   AND d.schema_key = tc.schema_key \
                   AND d.file_id = tc.file_id \
                   AND d.depth = tc.depth \
                GROUP BY tc.scope, tc.entity_id, tc.schema_key, tc.file_id \
            ), \
            tip_entries AS ( \
                SELECT \
                    tc.scope, \
                    tc.entity_id, \
                    tc.schema_key, \
                    tc.file_id, \
                    MAX(tc.change_id) AS change_id \
                FROM tip_candidates tc \
                JOIN tip_min_depth d \
                    ON d.scope = tc.scope \
                   AND d.entity_id = tc.entity_id \
                   AND d.schema_key = tc.schema_key \
                   AND d.file_id = tc.file_id \
                   AND d.depth = tc.depth \
                JOIN tip_best_created_at bc \
                    ON bc.scope = tc.scope \
                   AND bc.entity_id = tc.entity_id \
                   AND bc.schema_key = tc.schema_key \
                   AND bc.file_id = tc.file_id \
                   AND bc.commit_created_at = tc.commit_created_at \
                GROUP BY tc.scope, tc.entity_id, tc.schema_key, tc.file_id \
            ), \
            baseline_candidates AS ( \
                SELECT \
                    anc.scope AS scope, \
                    ch.entity_id, \
                    ch.schema_key, \
                    ch.file_id, \
                    members.change_id, \
                    anc.depth, \
                    members.commit_created_at AS commit_created_at \
                FROM baseline_ancestry anc \
                JOIN commit_members members \
                    ON members.commit_id = anc.commit_id \
                JOIN change_rows ch \
                    ON ch.change_id = members.change_id \
            ), \
            baseline_min_depth AS ( \
                SELECT \
                    scope, \
                    entity_id, \
                    schema_key, \
                    file_id, \
                    MIN(depth) AS depth \
                FROM baseline_candidates \
                GROUP BY scope, entity_id, schema_key, file_id \
            ), \
            baseline_best_created_at AS ( \
                SELECT \
                    bc.scope, \
                    bc.entity_id, \
                    bc.schema_key, \
                    bc.file_id, \
                    MAX(bc.commit_created_at) AS commit_created_at \
                FROM baseline_candidates bc \
                JOIN baseline_min_depth d \
                    ON d.scope = bc.scope \
                   AND d.entity_id = bc.entity_id \
                   AND d.schema_key = bc.schema_key \
                   AND d.file_id = bc.file_id \
                   AND d.depth = bc.depth \
                GROUP BY bc.scope, bc.entity_id, bc.schema_key, bc.file_id \
            ), \
            baseline_entries AS ( \
                SELECT \
                    bc.scope, \
                    bc.entity_id, \
                    bc.schema_key, \
                    bc.file_id, \
                    MAX(bc.change_id) AS change_id \
                FROM baseline_candidates bc \
                JOIN baseline_min_depth d \
                    ON d.scope = bc.scope \
                   AND d.entity_id = bc.entity_id \
                   AND d.schema_key = bc.schema_key \
                   AND d.file_id = bc.file_id \
                   AND d.depth = bc.depth \
                JOIN baseline_best_created_at bca \
                    ON bca.scope = bc.scope \
                   AND bca.entity_id = bc.entity_id \
                   AND bca.schema_key = bc.schema_key \
                   AND bca.file_id = bc.file_id \
                   AND bca.commit_created_at = bc.commit_created_at \
                GROUP BY bc.scope, bc.entity_id, bc.schema_key, bc.file_id \
            ), \
            paired_entries AS ( \
                SELECT \
                    tip.scope AS scope, \
                    tip.entity_id AS entity_id, \
                    tip.schema_key AS schema_key, \
                    tip.file_id AS file_id, \
                    base.change_id AS before_change_id, \
                    tip.change_id AS after_change_id \
                FROM tip_entries tip \
                LEFT JOIN baseline_entries base \
                    ON base.scope = tip.scope \
                   AND base.entity_id = tip.entity_id \
                   AND base.schema_key = tip.schema_key \
                   AND base.file_id = tip.file_id \
                UNION ALL \
                SELECT \
                    base.scope AS scope, \
                    base.entity_id AS entity_id, \
                    base.schema_key AS schema_key, \
                    base.file_id AS file_id, \
                    base.change_id AS before_change_id, \
                    NULL AS after_change_id \
                FROM baseline_entries base \
                LEFT JOIN tip_entries tip \
                    ON tip.scope = base.scope \
                   AND tip.entity_id = base.entity_id \
                   AND tip.schema_key = base.schema_key \
                   AND tip.file_id = base.file_id \
                WHERE tip.entity_id IS NULL \
            ), \
            resolved_rows AS ( \
                SELECT \
                    pair.scope AS scope, \
                    pair.entity_id AS entity_id, \
                    pair.schema_key AS schema_key, \
                    pair.file_id AS file_id, \
                    pair.before_change_id AS before_change_id, \
                    pair.after_change_id AS after_change_id, \
                    before_change.row_snapshot AS before_row_snapshot, \
                    after_change.row_snapshot AS after_row_snapshot \
                FROM paired_entries pair \
                LEFT JOIN change_rows before_change \
                    ON before_change.change_id = pair.before_change_id \
                LEFT JOIN change_rows after_change \
                    ON after_change.change_id = pair.after_change_id \
            ) \
            SELECT * FROM ( \
                SELECT \
                    entity_id, \
                    schema_key, \
                    file_id, \
                    CASE WHEN scope = 'global' THEN true ELSE false END AS lixcol_global, \
                    CASE \
                        WHEN before_row_snapshot IS NULL AND after_row_snapshot IS NOT NULL THEN NULL \
                        ELSE before_change_id \
                    END AS before_change_id, \
                    CASE \
                        WHEN before_row_snapshot IS NOT NULL AND after_row_snapshot IS NULL THEN NULL \
                        ELSE after_change_id \
                    END AS after_change_id, \
                    CASE \
                        WHEN before_row_snapshot IS NULL AND after_row_snapshot IS NOT NULL THEN NULL \
                        ELSE ( \
                            SELECT baseline_commit_id \
                            FROM scope_baselines scope \
                            WHERE scope.scope = resolved_rows.scope \
                            LIMIT 1 \
                        ) \
                    END AS before_commit_id, \
                    CASE \
                        WHEN before_row_snapshot IS NOT NULL AND after_row_snapshot IS NULL THEN NULL \
                        ELSE ( \
                            SELECT head_commit_id \
                            FROM scope_baselines scope \
                            WHERE scope.scope = resolved_rows.scope \
                            LIMIT 1 \
                        ) \
                    END AS after_commit_id, \
                    CASE \
                        WHEN before_row_snapshot IS NOT NULL AND after_row_snapshot IS NULL THEN 'removed' \
                        WHEN before_row_snapshot IS NULL AND after_row_snapshot IS NOT NULL THEN 'added' \
                        WHEN before_row_snapshot IS NOT NULL \
                             AND after_row_snapshot IS NOT NULL \
                             AND before_change_id != after_change_id THEN 'modified' \
                    END AS status \
                FROM resolved_rows \
            ) AS working_changes \
            WHERE status IS NOT NULL",
        active_version_cte = active_version_cte,
        version_ref_table = version_ref_table,
        version_ref_commit_id_column = version_ref_commit_id_column,
        parent_join_sql = parent_join_sql,
        parent_value_expr = parent_value_expr,
        change_id_join_sql = change_id_join_sql,
        change_id_value_expr = change_id_value_expr,
    )
}

fn active_target_versions_cte_sql(active_version_id: &str) -> String {
    format!(
        "target_versions AS ( \
           SELECT '{active_version_id}' AS version_id \
         )",
        active_version_id = escape_sql_string(active_version_id),
    )
}

fn explicit_target_versions_cte_sql(
    schema_keys: &[String],
    target_version_predicates: &[Expr],
) -> String {
    let version_descriptor_table = tracked_relation_name("lix_version_descriptor");
    let version_descriptor_hidden_column = quote_ident(&builtin_payload_column_name(
        version_descriptor_schema_key(),
        "hidden",
    ));
    let hidden_global_requested = target_version_predicates
        .iter()
        .any(|predicate| expr_contains_string_literal(predicate, GLOBAL_VERSION_ID));
    let version_descriptor_predicates = vec![
        format!(
            "schema_key = '{}'",
            escape_sql_string(version_descriptor_schema_key())
        ),
        format!("version_id = '{}'", escape_sql_string(GLOBAL_VERSION_ID)),
        "is_tombstone = 0".to_string(),
        format!(
            "COALESCE({version_descriptor_hidden_column}, false) = false",
            version_descriptor_hidden_column = version_descriptor_hidden_column
        ),
    ];
    let schema_local_rows = schema_keys
        .iter()
        .map(|schema_key| {
            format!(
                "SELECT DISTINCT version_id \
                 FROM {table_name} \
                 WHERE version_id <> '{global_version}' \
                   AND untracked = false",
                table_name = quote_ident(&tracked_relation_name(schema_key)),
                global_version = escape_sql_string(GLOBAL_VERSION_ID),
            )
        })
        .chain(schema_keys.iter().map(|schema_key| {
            format!(
                "SELECT DISTINCT version_id \
                 FROM {table_name} \
                 WHERE version_id <> '{global_version}' \
                   AND untracked = true",
                table_name = quote_ident(&tracked_relation_name(schema_key)),
                global_version = escape_sql_string(GLOBAL_VERSION_ID),
            )
        }))
        .collect::<Vec<_>>();
    let all_target_versions = if schema_local_rows.is_empty() {
        if hidden_global_requested {
            format!(
                " UNION SELECT '{}' AS version_id",
                escape_sql_string(GLOBAL_VERSION_ID)
            )
        } else {
            String::new()
        }
    } else {
        let mut unions = Vec::new();
        unions.push(schema_local_rows.join(" UNION "));
        if hidden_global_requested {
            unions.push(format!(
                "SELECT '{}' AS version_id",
                escape_sql_string(GLOBAL_VERSION_ID)
            ));
        }
        format!(" UNION {}", unions.join(" UNION "))
    };
    let target_versions_where = render_where_clause_sql(target_version_predicates, " WHERE ");
    format!(
        "all_target_versions AS ( \
           SELECT DISTINCT entity_id AS version_id \
           FROM {version_descriptor_table} \
           WHERE {version_descriptor_predicates}\
           {all_target_versions} \
         ), \
         target_versions AS ( \
           SELECT version_id \
           FROM all_target_versions \
           {target_versions_where} \
         )",
        version_descriptor_table = version_descriptor_table,
        version_descriptor_predicates = version_descriptor_predicates.join(" AND "),
        all_target_versions = all_target_versions,
        target_versions_where = target_versions_where,
    )
}

fn effective_state_schema_winner_rows_sql(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
    schema_keys: &[String],
    source_predicates: &[Expr],
    effective_state_request: &EffectiveStateRequest,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    include_snapshot_content: bool,
) -> String {
    let payload_columns = effective_state_payload_columns(effective_state_request, surface_binding);
    schema_keys
        .iter()
        .map(|schema_key| {
            let table_name = quote_ident(&tracked_relation_name(schema_key));
            let untracked_table = quote_ident(&tracked_relation_name(schema_key));
            let workspace_writer_key_table = quote_ident(WORKSPACE_WRITER_KEY_TABLE);
            let tracked_full_projection = normalized_projection_sql_for_schema(
                schema_key,
                known_live_layouts.get(schema_key),
                Some("t"),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "live layout lookup for '{schema_key}' failed: {}",
                    error.description
                )
            });
            let untracked_full_projection = normalized_projection_sql_for_schema(
                schema_key,
                known_live_layouts.get(schema_key),
                Some("u"),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "live layout lookup for '{schema_key}' failed: {}",
                    error.description
                )
            });
            let ranked_payload_projection = render_state_payload_projection_list(
                dialect,
                surface_binding,
                schema_key,
                "ranked",
                &payload_columns,
                known_live_layouts,
            );
            let final_snapshot_projection = if include_snapshot_content {
                format!(
                    "{} AS snapshot_content, ",
                    snapshot_select_expr_for_schema(
                        schema_key,
                        known_live_layouts.get(schema_key),
                        dialect,
                        Some("ranked"),
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "live layout lookup for '{schema_key}' failed: {}",
                            error.description
                        )
                    })
                )
            } else {
                String::new()
            };
            let tracked_predicates =
                render_qualified_where_clause_sql(source_predicates, " AND ", "t");
            let untracked_predicates =
                render_qualified_where_clause_sql(source_predicates, " AND ", "u");
            format!(
                "SELECT \
                   ranked.effective_entity_id AS entity_id, \
                   ranked.effective_schema_key AS schema_key, \
                   ranked.effective_file_id AS file_id, \
                   ranked.effective_version_id AS version_id, \
                   ranked.effective_plugin_key AS plugin_key, \
                   {final_snapshot_projection} \
                   ranked.effective_schema_version AS schema_version, \
                   ranked.effective_created_at AS created_at, \
                   ranked.effective_updated_at AS updated_at, \
                   ranked.effective_global AS global, \
                   ranked.effective_change_id AS change_id, \
                   ranked.effective_commit_id AS commit_id, \
                   ranked.effective_untracked AS untracked, \
                   ranked.effective_writer_key AS writer_key, \
                   ranked.effective_metadata AS metadata{ranked_payload_projection} \
                 FROM ( \
                   SELECT \
                     c.*, \
                     ROW_NUMBER() OVER ( \
                       PARTITION BY c.effective_version_id, c.effective_schema_key, c.effective_file_id, c.effective_entity_id \
                       ORDER BY c.precedence ASC, c.effective_updated_at DESC, c.effective_created_at DESC, c.effective_change_id DESC \
                     ) AS rn \
                   FROM ( \
                     SELECT \
                       t.entity_id AS effective_entity_id, \
                       t.schema_key AS effective_schema_key, \
                       t.file_id AS effective_file_id, \
                       tv.version_id AS effective_version_id, \
                       t.plugin_key AS effective_plugin_key, \
                       t.schema_version AS effective_schema_version, \
                       t.created_at AS effective_created_at, \
                       t.updated_at AS effective_updated_at, \
                       CASE WHEN tv.version_id = '{global_version}' THEN true ELSE false END AS effective_global, \
                       t.change_id AS effective_change_id, \
                       cc.commit_id AS effective_commit_id, \
                       false AS effective_untracked, \
                       wk.writer_key AS effective_writer_key, \
                       t.metadata AS effective_metadata, \
                       t.is_tombstone AS is_tombstone{tracked_full_projection}, \
                       2 AS precedence \
                     FROM {table_name} t \
                     JOIN target_versions tv \
                       ON tv.version_id = t.version_id \
                     LEFT JOIN change_commit_by_change_id cc \
                       ON cc.change_id = t.change_id \
                     LEFT JOIN {workspace_writer_key_table} wk \
                       ON wk.version_id = t.version_id \
                      AND wk.schema_key = t.schema_key \
                      AND wk.entity_id = t.entity_id \
                      AND wk.file_id = t.file_id \
                     WHERE t.untracked = false{tracked_predicates} \
                     UNION ALL \
                     SELECT \
                       t.entity_id AS effective_entity_id, \
                       t.schema_key AS effective_schema_key, \
                       t.file_id AS effective_file_id, \
                       tv.version_id AS effective_version_id, \
                       t.plugin_key AS effective_plugin_key, \
                       t.schema_version AS effective_schema_version, \
                       t.created_at AS effective_created_at, \
                       t.updated_at AS effective_updated_at, \
                       true AS effective_global, \
                       t.change_id AS effective_change_id, \
                       cc.commit_id AS effective_commit_id, \
                       false AS effective_untracked, \
                       gwk.writer_key AS effective_writer_key, \
                       t.metadata AS effective_metadata, \
                       t.is_tombstone AS is_tombstone{tracked_full_projection}, \
                       4 AS precedence \
                     FROM {table_name} t \
                     JOIN target_versions tv \
                       ON tv.version_id <> '{global_version}' \
                      AND t.version_id = '{global_version}' \
                     LEFT JOIN change_commit_by_change_id cc \
                       ON cc.change_id = t.change_id \
                     LEFT JOIN {workspace_writer_key_table} gwk \
                       ON gwk.version_id = t.version_id \
                      AND gwk.schema_key = t.schema_key \
                      AND gwk.entity_id = t.entity_id \
                      AND gwk.file_id = t.file_id \
                     WHERE t.version_id = '{global_version}' \
                       AND t.untracked = false{tracked_predicates} \
                     UNION ALL \
                     SELECT \
                       u.entity_id AS effective_entity_id, \
                       u.schema_key AS effective_schema_key, \
                       u.file_id AS effective_file_id, \
                       tv.version_id AS effective_version_id, \
                       u.plugin_key AS effective_plugin_key, \
                       u.schema_version AS effective_schema_version, \
                       u.created_at AS effective_created_at, \
                       u.updated_at AS effective_updated_at, \
                       CASE WHEN tv.version_id = '{global_version}' THEN true ELSE false END AS effective_global, \
                       NULL AS effective_change_id, \
                       'untracked' AS effective_commit_id, \
                       true AS effective_untracked, \
                       u.writer_key AS effective_writer_key, \
                       u.metadata AS effective_metadata, \
                       0 AS is_tombstone{untracked_full_projection}, \
                       1 AS precedence \
                     FROM {untracked_table} u \
                     JOIN target_versions tv \
                       ON tv.version_id = u.version_id \
                     WHERE u.untracked = true{untracked_predicates} \
                     UNION ALL \
                     SELECT \
                       u.entity_id AS effective_entity_id, \
                       u.schema_key AS effective_schema_key, \
                       u.file_id AS effective_file_id, \
                       tv.version_id AS effective_version_id, \
                       u.plugin_key AS effective_plugin_key, \
                       u.schema_version AS effective_schema_version, \
                       u.created_at AS effective_created_at, \
                       u.updated_at AS effective_updated_at, \
                       true AS effective_global, \
                       NULL AS effective_change_id, \
                       'untracked' AS effective_commit_id, \
                       true AS effective_untracked, \
                       u.writer_key AS effective_writer_key, \
                       u.metadata AS effective_metadata, \
                       0 AS is_tombstone{untracked_full_projection}, \
                       3 AS precedence \
                     FROM {untracked_table} u \
                     JOIN target_versions tv \
                       ON tv.version_id <> '{global_version}' \
                      AND u.version_id = '{global_version}' \
                     WHERE u.version_id = '{global_version}' \
                       AND u.untracked = true{untracked_predicates} \
                   ) AS c \
                 ) AS ranked \
                 WHERE ranked.rn = 1 \
                   AND ranked.is_tombstone = 0",
                final_snapshot_projection = final_snapshot_projection,
                ranked_payload_projection = ranked_payload_projection,
                global_version = escape_sql_string(GLOBAL_VERSION_ID),
                tracked_full_projection = tracked_full_projection,
                tracked_predicates = tracked_predicates,
                untracked_full_projection = untracked_full_projection,
                untracked_predicates = untracked_predicates,
                table_name = table_name,
                untracked_table = untracked_table,
                workspace_writer_key_table = workspace_writer_key_table,
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

fn builtin_payload_column_name(schema_key: &str, property_name: &str) -> String {
    payload_column_name_for_schema(schema_key, None, property_name).unwrap_or_else(|error| {
        panic!(
            "builtin live schema '{schema_key}' must include '{property_name}': {}",
            error.description
        )
    })
}

fn effective_state_payload_columns(
    effective_state_request: &EffectiveStateRequest,
    surface_binding: &SurfaceBinding,
) -> Vec<String> {
    effective_state_request
        .required_columns
        .iter()
        .filter(|column| {
            !is_live_state_envelope_column(column)
                || entity_surface_uses_payload_alias(surface_binding, column)
        })
        .cloned()
        .collect()
}

fn is_live_state_envelope_column(column: &str) -> bool {
    matches!(
        column,
        "entity_id"
            | "schema_key"
            | "file_id"
            | "version_id"
            | "plugin_key"
            | "schema_version"
            | "metadata"
            | "created_at"
            | "updated_at"
            | "global"
            | "change_id"
            | "commit_id"
            | "untracked"
            | "writer_key"
            | "lixcol_entity_id"
            | "lixcol_schema_key"
            | "lixcol_file_id"
            | "lixcol_version_id"
            | "lixcol_plugin_key"
            | "lixcol_schema_version"
            | "lixcol_change_id"
            | "lixcol_commit_id"
            | "lixcol_created_at"
            | "lixcol_updated_at"
            | "lixcol_global"
            | "lixcol_untracked"
            | "lixcol_writer_key"
            | "lixcol_metadata"
            | "snapshot_content"
            | "commit_created_at"
            | "root_commit_id"
            | "depth"
            | "lixcol_root_commit_id"
            | "lixcol_depth"
    )
}

fn render_state_payload_projection_list(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
    schema_key: &str,
    table_alias: &str,
    payload_columns: &[String],
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> String {
    if payload_columns.is_empty() {
        return String::new();
    }

    format!(
        ", {}",
        payload_columns
            .iter()
            .map(|column| {
                let expression = render_live_payload_column_expr(
                    dialect,
                    schema_key,
                    known_live_layouts.get(schema_key),
                    table_alias,
                    column,
                );
                let alias = if entity_surface_uses_payload_alias(surface_binding, column) {
                    entity_surface_payload_alias(column)
                } else {
                    column.clone()
                };
                format!("{expression} AS {}", render_identifier(&alias))
            })
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn render_live_payload_column_expr(
    dialect: SqlDialect,
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    table_alias: &str,
    public_column: &str,
) -> String {
    let Ok(column_name) =
        payload_column_name_for_schema(schema_key, schema_definition, public_column)
    else {
        return "NULL".to_string();
    };
    let qualified = format!("{}.{}", quote_ident(table_alias), quote_ident(&column_name));
    match public_column {
        "metadata" => qualified,
        _ if column_name.ends_with("_json") => match dialect {
            SqlDialect::Sqlite => format!(
                "CASE WHEN {qualified} IS NULL THEN NULL ELSE json_extract({qualified}, '$') || '' END"
            ),
            SqlDialect::Postgres => format!(
                "CASE WHEN {qualified} IS NULL THEN NULL ELSE (CAST({qualified} AS JSONB) #>> '{{}}') END"
            ),
        },
        _ => qualified,
    }
}
