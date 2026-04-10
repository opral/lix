//! Compiler-owned public surface source SQL builders.

use crate::catalog::{RelationBinding, SurfaceBinding, SurfaceVariant};
use crate::contracts::EffectiveStateRequest;
use crate::contracts::GLOBAL_VERSION_ID;
use crate::contracts::{
    version_descriptor_schema_key, version_ref_file_id, version_ref_plugin_key,
    version_ref_schema_key, version_ref_schema_version,
};
use crate::live_state::tracked_relation_name;
use crate::live_state::{
    normalized_projection_sql_for_schema, payload_column_name_for_schema,
    snapshot_select_expr_for_schema, WRITER_KEY_TABLE,
};
use crate::sql::physical_plan::catalog_relation_sql::{
    build_filesystem_relation_sql, build_version_relation_sql,
};
use crate::sql::physical_plan::public_surface_sql_support::{
    entity_surface_payload_alias, entity_surface_uses_payload_alias, escape_sql_string,
    expr_contains_string_literal, json_array_text_join_sql, quote_ident, render_identifier,
    render_qualified_where_clause_sql, render_where_clause_sql,
    split_effective_state_pushdown_predicates,
};
use crate::{LixError, SqlDialect};
use serde_json::Value as JsonValue;
use sqlparser::ast::Expr;
use std::collections::BTreeMap;

pub(crate) fn lower_catalog_relation_binding_to_source_sql(
    dialect: SqlDialect,
    binding: &RelationBinding,
) -> Result<String, LixError> {
    match binding {
        RelationBinding::VersionRelation(binding) => Ok(build_version_relation_sql(dialect, binding)),
        RelationBinding::FilesystemRelation(binding) => build_filesystem_relation_sql(binding, dialect),
        RelationBinding::SchemaRelation(binding) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "sql source lowering for schema relation '{}' is handled by state/entity lowerers, not direct catalog source lowering",
                binding.public_name
            ),
        )),
    }
}

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
           {lazy_change_commit_ctes} \
         {schema_winner_rows_sql}",
        target_versions_cte = target_versions_cte,
        lazy_change_commit_ctes = build_lazy_change_commit_by_change_id_ctes_sql(dialect),
        schema_winner_rows_sql = schema_winner_rows_sql,
    ))
}

pub(crate) fn build_lazy_change_commit_by_change_id_ctes_sql(dialect: SqlDialect) -> String {
    let change_set_id_expr = match dialect {
        SqlDialect::Sqlite => {
            "json_extract(commit_rows.commit_snapshot_content, '$.change_set_id')".to_string()
        }
        SqlDialect::Postgres => {
            "(CAST(commit_rows.commit_snapshot_content AS JSONB) ->> 'change_set_id')".to_string()
        }
    };
    let (change_id_join_sql, change_id_value_expr) = json_array_text_join_sql(
        dialect,
        "commit_rows.commit_snapshot_content",
        "change_ids",
        "member_change_rows",
        "change_id",
    );

    format!(
        "commit_rows AS ( \
             SELECT \
               commit_change.entity_id AS commit_id, \
               commit_snapshot.content AS commit_snapshot_content \
             FROM lix_internal_change commit_change \
             LEFT JOIN lix_internal_snapshot commit_snapshot \
               ON commit_snapshot.id = commit_change.snapshot_id \
             WHERE commit_change.schema_key = 'lix_commit' \
               AND commit_snapshot.content IS NOT NULL \
         ), \
         commit_members AS ( \
             SELECT \
               commit_rows.commit_id AS commit_id, \
               {change_set_id_expr} AS change_set_id, \
               {change_id_value_expr} AS change_id \
             FROM commit_rows \
             {change_id_join_sql} \
             WHERE {change_set_id_expr} IS NOT NULL \
               AND {change_id_value_expr} IS NOT NULL \
         ), \
         change_commit_by_change_id AS ( \
             SELECT \
               commit_members.change_id AS change_id, \
               MAX(commit_members.commit_id) AS commit_id \
             FROM commit_members \
             WHERE commit_members.change_id IS NOT NULL \
             GROUP BY commit_members.change_id \
         )",
        change_set_id_expr = change_set_id_expr,
        change_id_join_sql = change_id_join_sql,
        change_id_value_expr = change_id_value_expr,
    )
}

pub(crate) fn build_direct_canonical_commit_source_sql(dialect: SqlDialect) -> String {
    let change_set_id_expr = match dialect {
        SqlDialect::Sqlite => "json_extract(s.content, '$.change_set_id')".to_string(),
        SqlDialect::Postgres => "(CAST(s.content AS JSONB) ->> 'change_set_id')".to_string(),
    };
    let change_ids_expr = match dialect {
        SqlDialect::Sqlite => {
            "CASE WHEN s.content IS NULL THEN NULL ELSE json_extract(s.content, '$.change_ids') || '' END"
                .to_string()
        }
        SqlDialect::Postgres => {
            "CASE WHEN s.content IS NULL THEN NULL ELSE (CAST(s.content AS JSONB) -> 'change_ids')::text END"
                .to_string()
        }
    };
    let author_account_ids_expr = match dialect {
        SqlDialect::Sqlite => {
            "CASE WHEN s.content IS NULL THEN NULL ELSE json_extract(s.content, '$.author_account_ids') || '' END"
                .to_string()
        }
        SqlDialect::Postgres => {
            "CASE WHEN s.content IS NULL THEN NULL ELSE (CAST(s.content AS JSONB) -> 'author_account_ids')::text END"
                .to_string()
        }
    };
    let parent_commit_ids_expr = match dialect {
        SqlDialect::Sqlite => {
            "CASE WHEN s.content IS NULL THEN NULL ELSE json_extract(s.content, '$.parent_commit_ids') || '' END"
                .to_string()
        }
        SqlDialect::Postgres => {
            "CASE WHEN s.content IS NULL THEN NULL ELSE (CAST(s.content AS JSONB) -> 'parent_commit_ids')::text END"
                .to_string()
        }
    };

    format!(
        "SELECT \
            ch.entity_id AS entity_id, \
            ch.schema_key AS schema_key, \
            ch.file_id AS file_id, \
            'global' AS version_id, \
            ch.plugin_key AS plugin_key, \
            ch.schema_version AS schema_version, \
            ch.created_at AS created_at, \
            ch.created_at AS updated_at, \
            true AS global, \
            ch.id AS change_id, \
            ch.entity_id AS commit_id, \
            false AS untracked, \
            NULL AS writer_key, \
            ch.metadata AS metadata, \
            ch.entity_id AS id, \
            {change_set_id_expr} AS change_set_id, \
            {change_ids_expr} AS change_ids, \
            {author_account_ids_expr} AS author_account_ids, \
            {parent_commit_ids_expr} AS parent_commit_ids, \
            CASE WHEN ch.snapshot_id = 'no-content' THEN NULL ELSE s.content END AS snapshot_content \
         FROM lix_internal_change ch \
         LEFT JOIN lix_internal_snapshot s \
           ON s.id = ch.snapshot_id \
         WHERE ch.schema_key = 'lix_commit'",
        change_set_id_expr = change_set_id_expr,
        change_ids_expr = change_ids_expr,
        author_account_ids_expr = author_account_ids_expr,
        parent_commit_ids_expr = parent_commit_ids_expr,
    )
}

pub(crate) fn build_direct_canonical_change_set_source_sql() -> String {
    "SELECT \
        ch.entity_id AS entity_id, \
        ch.schema_key AS schema_key, \
        ch.file_id AS file_id, \
        'global' AS version_id, \
        ch.plugin_key AS plugin_key, \
        ch.schema_version AS schema_version, \
        ch.created_at AS created_at, \
        ch.created_at AS updated_at, \
        true AS global, \
        ch.id AS change_id, \
        ch.entity_id AS commit_id, \
        false AS untracked, \
        NULL AS writer_key, \
        ch.metadata AS metadata, \
        ch.entity_id AS id, \
        NULL AS change_set_id, \
        NULL AS change_ids, \
        NULL AS author_account_ids, \
        NULL AS parent_commit_ids, \
        CASE WHEN ch.snapshot_id = 'no-content' THEN NULL ELSE s.content END AS snapshot_content \
     FROM lix_internal_change ch \
     LEFT JOIN lix_internal_snapshot s \
       ON s.id = ch.snapshot_id \
     WHERE ch.schema_key = 'lix_change_set'"
        .to_string()
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
    let include_global_target_version = hidden_global_requested
        || schema_keys
            .iter()
            .any(|schema_key| schema_key_requires_global_target_version(schema_key));
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
        .filter(|schema_key| schema_key_uses_live_version_discovery(schema_key))
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
        .chain(
            schema_keys
                .iter()
                .filter(|schema_key| schema_key_uses_live_version_discovery(schema_key))
                .map(|schema_key| {
                    format!(
                        "SELECT DISTINCT version_id \
                         FROM {table_name} \
                         WHERE version_id <> '{global_version}' \
                           AND untracked = true",
                        table_name = quote_ident(&tracked_relation_name(schema_key)),
                        global_version = escape_sql_string(GLOBAL_VERSION_ID),
                    )
                }),
        )
        .collect::<Vec<_>>();
    let all_target_versions = if schema_local_rows.is_empty() {
        if include_global_target_version {
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
        if include_global_target_version {
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

fn schema_key_uses_live_version_discovery(schema_key: &str) -> bool {
    !matches!(
        schema_key,
        "lix_commit"
            | "lix_change_set"
            | "lix_change_set_element"
            | "lix_commit_edge"
            | "lix_change_author"
    )
}

fn schema_key_requires_global_target_version(schema_key: &str) -> bool {
    matches!(
        schema_key,
        "lix_commit"
            | "lix_change_set"
            | "lix_change_set_element"
            | "lix_commit_edge"
            | "lix_change_author"
    )
}

fn tracked_writer_key_join_sql(row_alias: &str, writer_alias: &str) -> String {
    format!(
        "LEFT JOIN {writer_key_table} {writer_alias} \
           ON {writer_alias}.version_id = {row_alias}.version_id \
          AND {writer_alias}.schema_key = {row_alias}.schema_key \
          AND {writer_alias}.entity_id = {row_alias}.entity_id \
          AND {writer_alias}.file_id = {row_alias}.file_id",
        writer_key_table = WRITER_KEY_TABLE,
        row_alias = quote_ident(row_alias),
        writer_alias = quote_ident(writer_alias),
    )
}

fn render_overlay_where_clause_sql(
    predicates: &[Expr],
    prefix: &str,
    row_alias: &str,
    writer_alias: &str,
) -> String {
    let mut rendered = render_qualified_where_clause_sql(predicates, prefix, row_alias);
    let row_writer_key = format!("{}.{}", quote_ident(row_alias), quote_ident("writer_key"));
    let row_lixcol_writer_key = format!(
        "{}.{}",
        quote_ident(row_alias),
        quote_ident("lixcol_writer_key")
    );
    let overlay_writer_key = format!(
        "{}.{}",
        quote_ident(writer_alias),
        quote_ident("writer_key")
    );
    rendered = rendered.replace(&row_writer_key, &overlay_writer_key);
    rendered.replace(&row_lixcol_writer_key, &overlay_writer_key)
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
            if schema_key == "lix_commit" {
                return build_direct_canonical_header_state_rows_sql(
                    dialect,
                    surface_binding,
                    "lix_commit",
                    &payload_columns,
                    known_live_layouts,
                    build_direct_canonical_commit_source_sql(dialect),
                    source_predicates,
                    include_snapshot_content,
                );
            }
            if schema_key == "lix_change_set" {
                return build_direct_canonical_header_state_rows_sql(
                    dialect,
                    surface_binding,
                    "lix_change_set",
                    &payload_columns,
                    known_live_layouts,
                    build_direct_canonical_change_set_source_sql(),
                    source_predicates,
                    include_snapshot_content,
                );
            }
            if schema_key == "lix_change_set_element" {
                return build_change_set_element_state_rows_sql(
                    dialect,
                    surface_binding,
                    &payload_columns,
                    known_live_layouts,
                    surface_binding.descriptor.surface_variant,
                    source_predicates,
                    include_snapshot_content,
                );
            }
            if schema_key == "lix_commit_edge" {
                return build_commit_edge_state_rows_sql(
                    dialect,
                    surface_binding,
                    &payload_columns,
                    known_live_layouts,
                    surface_binding.descriptor.surface_variant,
                    source_predicates,
                    include_snapshot_content,
                );
            }
            if schema_key == "lix_change_author" {
                return build_change_author_state_rows_sql(
                    dialect,
                    surface_binding,
                    &payload_columns,
                    known_live_layouts,
                    surface_binding.descriptor.surface_variant,
                    source_predicates,
                    include_snapshot_content,
                );
            }
            let table_name = quote_ident(&tracked_relation_name(schema_key));
            let untracked_table = quote_ident(&tracked_relation_name(schema_key));
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
                render_overlay_where_clause_sql(source_predicates, " AND ", "t", "wk");
            let tracked_global_predicates = render_overlay_where_clause_sql(
                source_predicates,
                " AND ",
                "t",
                "wk_global",
            );
            let untracked_predicates =
                render_overlay_where_clause_sql(source_predicates, " AND ", "u", "uwk");
            let untracked_global_predicates = render_overlay_where_clause_sql(
                source_predicates,
                " AND ",
                "u",
                "uwk_global",
            );
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
                     {tracked_writer_key_join_sql} \
                     LEFT JOIN change_commit_by_change_id cc \
                       ON cc.change_id = t.change_id \
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
                       wk_global.writer_key AS effective_writer_key, \
                       t.metadata AS effective_metadata, \
                       t.is_tombstone AS is_tombstone{tracked_full_projection}, \
                       4 AS precedence \
                     FROM {table_name} t \
                     JOIN target_versions tv \
                       ON tv.version_id <> '{global_version}' \
                      AND t.version_id = '{global_version}' \
                     {tracked_global_writer_key_join_sql} \
                     LEFT JOIN change_commit_by_change_id cc \
                       ON cc.change_id = t.change_id \
                     WHERE t.version_id = '{global_version}' \
                       AND t.untracked = false{tracked_global_predicates} \
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
                       uwk.writer_key AS effective_writer_key, \
                       u.metadata AS effective_metadata, \
                       0 AS is_tombstone{untracked_full_projection}, \
                       1 AS precedence \
                     FROM {untracked_table} u \
                     JOIN target_versions tv \
                       ON tv.version_id = u.version_id \
                     {untracked_writer_key_join_sql} \
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
                       uwk_global.writer_key AS effective_writer_key, \
                       u.metadata AS effective_metadata, \
                       0 AS is_tombstone{untracked_full_projection}, \
                       3 AS precedence \
                     FROM {untracked_table} u \
                     JOIN target_versions tv \
                       ON tv.version_id <> '{global_version}' \
                      AND u.version_id = '{global_version}' \
                     {untracked_global_writer_key_join_sql} \
                     WHERE u.version_id = '{global_version}' \
                       AND u.untracked = true{untracked_global_predicates} \
                   ) AS c \
                 ) AS ranked \
                 WHERE ranked.rn = 1 \
                   AND ranked.is_tombstone = 0",
                final_snapshot_projection = final_snapshot_projection,
                ranked_payload_projection = ranked_payload_projection,
                global_version = escape_sql_string(GLOBAL_VERSION_ID),
                tracked_full_projection = tracked_full_projection,
                tracked_predicates = tracked_predicates,
                tracked_global_predicates = tracked_global_predicates,
                tracked_writer_key_join_sql = tracked_writer_key_join_sql("t", "wk"),
                tracked_global_writer_key_join_sql = tracked_writer_key_join_sql("t", "wk_global"),
                untracked_full_projection = untracked_full_projection,
                untracked_predicates = untracked_predicates,
                untracked_global_predicates = untracked_global_predicates,
                untracked_writer_key_join_sql = tracked_writer_key_join_sql("u", "uwk"),
                untracked_global_writer_key_join_sql =
                    tracked_writer_key_join_sql("u", "uwk_global"),
                table_name = table_name,
                untracked_table = untracked_table,
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

fn build_direct_canonical_header_state_rows_sql(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
    schema_key: &str,
    payload_columns: &[String],
    known_live_layouts: &BTreeMap<String, JsonValue>,
    base_source_sql: String,
    source_predicates: &[Expr],
    include_snapshot_content: bool,
) -> String {
    let direct_predicates =
        render_qualified_where_clause_sql(source_predicates, " AND ", "direct_rows");
    let id_column = builtin_payload_column_name(schema_key, "id");
    let header_payload_projection = match schema_key {
        "lix_commit" => {
            let change_set_id_column = builtin_payload_column_name(schema_key, "change_set_id");
            let change_ids_column = builtin_payload_column_name(schema_key, "change_ids");
            let author_account_ids_column =
                builtin_payload_column_name(schema_key, "author_account_ids");
            let parent_commit_ids_column =
                builtin_payload_column_name(schema_key, "parent_commit_ids");
            format!(
                ", \
                 base.change_set_id AS {change_set_id_column}, \
                 base.change_ids AS {change_ids_column}, \
                 base.author_account_ids AS {author_account_ids_column}, \
                 base.parent_commit_ids AS {parent_commit_ids_column}",
                change_set_id_column = change_set_id_column,
                change_ids_column = change_ids_column,
                author_account_ids_column = author_account_ids_column,
                parent_commit_ids_column = parent_commit_ids_column,
            )
        }
        "lix_change_set" => String::new(),
        _ => panic!("unsupported direct canonical header schema: {schema_key}"),
    };
    let payload_projection = render_state_payload_projection_list(
        dialect,
        surface_binding,
        schema_key,
        "direct_rows",
        payload_columns,
        known_live_layouts,
    );
    let final_snapshot_projection = if include_snapshot_content {
        "direct_rows.snapshot_content AS snapshot_content, ".to_string()
    } else {
        String::new()
    };
    format!(
        "SELECT \
            direct_rows.entity_id AS entity_id, \
            direct_rows.schema_key AS schema_key, \
            direct_rows.file_id AS file_id, \
            direct_rows.version_id AS version_id, \
            direct_rows.plugin_key AS plugin_key, \
            {final_snapshot_projection}\
            direct_rows.schema_version AS schema_version, \
            direct_rows.created_at AS created_at, \
            direct_rows.updated_at AS updated_at, \
            direct_rows.global AS global, \
            direct_rows.change_id AS change_id, \
            direct_rows.commit_id AS commit_id, \
            direct_rows.untracked AS untracked, \
            direct_rows.writer_key AS writer_key, \
            direct_rows.metadata AS metadata{payload_projection} \
         FROM ( \
           SELECT \
             base.entity_id AS entity_id, \
             base.schema_key AS schema_key, \
             base.file_id AS file_id, \
             tv.version_id AS version_id, \
             base.plugin_key AS plugin_key, \
             base.snapshot_content AS snapshot_content, \
             base.schema_version AS schema_version, \
             base.created_at AS created_at, \
             base.updated_at AS updated_at, \
             true AS global, \
             base.change_id AS change_id, \
             base.commit_id AS commit_id, \
             base.untracked AS untracked, \
             base.writer_key AS writer_key, \
             base.metadata AS metadata, \
             base.id AS {id_column}{header_payload_projection} \
           FROM ({base_source_sql}) AS base \
           JOIN target_versions tv \
             ON true \
         ) AS direct_rows \
         WHERE 1 = 1{direct_predicates}",
        final_snapshot_projection = final_snapshot_projection,
        payload_projection = payload_projection,
        base_source_sql = base_source_sql,
        direct_predicates = direct_predicates,
        id_column = id_column,
        header_payload_projection = header_payload_projection,
    )
}

fn build_commit_rows_scope_cte_sql(surface_variant: SurfaceVariant) -> String {
    let version_ref_table = tracked_relation_name(version_ref_schema_key());
    let version_ref_commit_id_column = quote_ident(&builtin_payload_column_name(
        version_ref_schema_key(),
        "commit_id",
    ));
    match surface_variant {
        SurfaceVariant::Default => "commit_rows AS ( \
                 SELECT \
                     commit_change.entity_id AS commit_id, \
                     commit_change.created_at AS commit_created_at, \
                     commit_change.metadata AS commit_metadata, \
                     commit_snapshot.content AS commit_snapshot_content, \
                     'global' AS target_version_id \
                 FROM lix_internal_change commit_change \
                 LEFT JOIN lix_internal_snapshot commit_snapshot \
                   ON commit_snapshot.id = commit_change.snapshot_id \
                 WHERE commit_change.schema_key = 'lix_commit' \
                   AND commit_snapshot.content IS NOT NULL \
             )"
        .to_string(),
        SurfaceVariant::ByVersion => format!(
            "current_refs AS ( \
                 SELECT \
                     tv.version_id AS version_id, \
                     vr.{version_ref_commit_id_column} AS head_commit_id \
                 FROM target_versions tv \
                 JOIN {version_ref_table} vr \
                   ON vr.entity_id = tv.version_id \
                  AND vr.schema_key = '{version_ref_schema_key}' \
                  AND vr.schema_version = '{version_ref_schema_version}' \
                  AND vr.file_id = '{version_ref_file_id}' \
                  AND vr.plugin_key = '{version_ref_plugin_key}' \
                  AND vr.version_id = '{global_version}' \
                  AND vr.untracked = true \
                  AND vr.is_tombstone = 0 \
                  AND vr.{version_ref_commit_id_column} IS NOT NULL \
                  AND vr.{version_ref_commit_id_column} <> '' \
             ), \
             commit_rows AS ( \
                 SELECT \
                     commit_change.entity_id AS commit_id, \
                     commit_change.created_at AS commit_created_at, \
                     commit_change.metadata AS commit_metadata, \
                     commit_snapshot.content AS commit_snapshot_content, \
                     current_refs.version_id AS target_version_id \
                 FROM current_refs \
                 JOIN lix_internal_change commit_change \
                   ON commit_change.entity_id = current_refs.head_commit_id \
                 LEFT JOIN lix_internal_snapshot commit_snapshot \
                   ON commit_snapshot.id = commit_change.snapshot_id \
                 WHERE commit_change.schema_key = 'lix_commit' \
                   AND commit_snapshot.content IS NOT NULL \
             )",
            version_ref_table = version_ref_table,
            version_ref_commit_id_column = version_ref_commit_id_column,
            version_ref_schema_key = escape_sql_string(version_ref_schema_key()),
            version_ref_schema_version = escape_sql_string(version_ref_schema_version()),
            version_ref_file_id = escape_sql_string(version_ref_file_id()),
            version_ref_plugin_key = escape_sql_string(version_ref_plugin_key()),
            global_version = escape_sql_string(GLOBAL_VERSION_ID),
        ),
        SurfaceVariant::History | SurfaceVariant::WorkingChanges => {
            unreachable!("derived commit-family state rows only support default/by-version")
        }
    }
}

fn json_object_sql(dialect: SqlDialect, fields: &[(&str, &str)]) -> String {
    match dialect {
        SqlDialect::Sqlite => format!(
            "json_object({})",
            fields
                .iter()
                .map(|(key, expr)| format!("'{}', {}", escape_sql_string(key), expr))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        SqlDialect::Postgres => format!(
            "json_build_object({})::text",
            fields
                .iter()
                .map(|(key, expr)| format!("'{}', {}", escape_sql_string(key), expr))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn build_change_set_element_state_rows_sql(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
    payload_columns: &[String],
    known_live_layouts: &BTreeMap<String, JsonValue>,
    surface_variant: SurfaceVariant,
    source_predicates: &[Expr],
    include_snapshot_content: bool,
) -> String {
    let commit_rows_cte_sql = build_commit_rows_scope_cte_sql(surface_variant);
    let (change_id_join_sql, change_id_value_expr) = json_array_text_join_sql(
        dialect,
        "commit_rows.commit_snapshot_content",
        "change_ids",
        "member_change_rows",
        "change_id",
    );
    let change_set_id_expr = match dialect {
        SqlDialect::Sqlite => {
            "json_extract(commit_rows.commit_snapshot_content, '$.change_set_id')".to_string()
        }
        SqlDialect::Postgres => {
            "(CAST(commit_rows.commit_snapshot_content AS JSONB) ->> 'change_set_id')".to_string()
        }
    };
    let snapshot_expr = json_object_sql(
        dialect,
        &[
            ("change_set_id", "direct_rows.change_set_id"),
            ("change_id", "direct_rows.change_id_value"),
            ("entity_id", "direct_rows.entity_id_value"),
            ("schema_key", "direct_rows.schema_key_value"),
            ("file_id", "direct_rows.file_id_value"),
        ],
    );
    let direct_predicates =
        render_qualified_where_clause_sql(source_predicates, " AND ", "direct_rows");
    let payload_projection = render_state_payload_projection_list(
        dialect,
        surface_binding,
        "lix_change_set_element",
        "direct_rows",
        payload_columns,
        known_live_layouts,
    );
    let final_snapshot_projection = if include_snapshot_content {
        format!("{snapshot_expr} AS snapshot_content, ")
    } else {
        String::new()
    };
    let change_id_column = builtin_payload_column_name("lix_change_set_element", "change_id");
    let entity_id_column = builtin_payload_column_name("lix_change_set_element", "entity_id");
    let schema_key_column = builtin_payload_column_name("lix_change_set_element", "schema_key");
    let file_id_column = builtin_payload_column_name("lix_change_set_element", "file_id");
    let change_set_id_column =
        builtin_payload_column_name("lix_change_set_element", "change_set_id");
    format!(
        "SELECT direct_rows.entity_id AS entity_id, \
                direct_rows.schema_key AS schema_key, \
                direct_rows.file_id AS file_id, \
                direct_rows.version_id AS version_id, \
                direct_rows.plugin_key AS plugin_key, \
                {final_snapshot_projection}\
                direct_rows.schema_version AS schema_version, \
                direct_rows.created_at AS created_at, \
                direct_rows.updated_at AS updated_at, \
                direct_rows.global AS global, \
                direct_rows.change_id AS change_id, \
                direct_rows.commit_id AS commit_id, \
                direct_rows.untracked AS untracked, \
                direct_rows.writer_key AS writer_key, \
                direct_rows.metadata AS metadata{payload_projection} \
         FROM ( \
             WITH {commit_rows_cte_sql}, \
             change_rows AS ( \
                 SELECT \
                     ch.id AS change_id, \
                     ch.entity_id AS member_entity_id, \
                     ch.schema_key AS member_schema_key, \
                     ch.file_id AS member_file_id, \
                     ch.metadata AS member_metadata, \
                     ch.created_at AS member_created_at \
                 FROM lix_internal_change ch \
             ), \
             commit_members AS ( \
                 SELECT \
                     commit_rows.target_version_id AS version_id, \
                     commit_rows.commit_id AS commit_id, \
                     {change_set_id_expr} AS change_set_id, \
                     {change_id_value_expr} AS member_change_id \
                 FROM commit_rows \
                 {change_id_join_sql} \
                 WHERE {change_set_id_expr} IS NOT NULL \
                   AND {change_id_value_expr} IS NOT NULL \
             ) \
             SELECT \
                 commit_members.change_set_id || '~' || change_rows.change_id AS entity_id, \
                 'lix_change_set_element' AS schema_key, \
                 'lix' AS file_id, \
                 commit_members.version_id AS version_id, \
                 'lix' AS plugin_key, \
                 '1' AS schema_version, \
                 change_rows.member_created_at AS created_at, \
                 change_rows.member_created_at AS updated_at, \
                 true AS global, \
                 change_rows.change_id AS change_id, \
                 commit_members.commit_id AS commit_id, \
                 false AS untracked, \
                 NULL AS writer_key, \
                 change_rows.member_metadata AS metadata, \
                 commit_members.change_set_id AS {change_set_id_column}, \
                 change_rows.change_id AS {change_id_column}, \
                 change_rows.member_entity_id AS {entity_id_column}, \
                 change_rows.member_schema_key AS {schema_key_column}, \
                 change_rows.member_file_id AS {file_id_column} \
             FROM commit_members \
             JOIN change_rows \
               ON change_rows.change_id = commit_members.member_change_id \
         ) AS direct_rows \
         WHERE 1 = 1{direct_predicates}",
        final_snapshot_projection = final_snapshot_projection,
        payload_projection = payload_projection,
        commit_rows_cte_sql = commit_rows_cte_sql,
        change_set_id_expr = change_set_id_expr,
        change_id_join_sql = change_id_join_sql,
        change_id_value_expr = change_id_value_expr,
        change_set_id_column = change_set_id_column,
        change_id_column = change_id_column,
        entity_id_column = entity_id_column,
        schema_key_column = schema_key_column,
        file_id_column = file_id_column,
        direct_predicates = direct_predicates,
    )
}

fn build_commit_edge_state_rows_sql(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
    payload_columns: &[String],
    known_live_layouts: &BTreeMap<String, JsonValue>,
    surface_variant: SurfaceVariant,
    source_predicates: &[Expr],
    include_snapshot_content: bool,
) -> String {
    let commit_rows_cte_sql = build_commit_rows_scope_cte_sql(surface_variant);
    let (parent_join_sql, parent_value_expr) = json_array_text_join_sql(
        dialect,
        "commit_rows.commit_snapshot_content",
        "parent_commit_ids",
        "parent_rows",
        "parent_id",
    );
    let snapshot_expr = json_object_sql(
        dialect,
        &[
            ("parent_id", "direct_rows.parent_id"),
            ("child_id", "direct_rows.child_id"),
        ],
    );
    let direct_predicates =
        render_qualified_where_clause_sql(source_predicates, " AND ", "direct_rows");
    let payload_projection = render_state_payload_projection_list(
        dialect,
        surface_binding,
        "lix_commit_edge",
        "direct_rows",
        payload_columns,
        known_live_layouts,
    );
    let final_snapshot_projection = if include_snapshot_content {
        format!("{snapshot_expr} AS snapshot_content, ")
    } else {
        String::new()
    };
    let parent_id_column = builtin_payload_column_name("lix_commit_edge", "parent_id");
    let child_id_column = builtin_payload_column_name("lix_commit_edge", "child_id");
    format!(
        "SELECT direct_rows.entity_id AS entity_id, \
                direct_rows.schema_key AS schema_key, \
                direct_rows.file_id AS file_id, \
                direct_rows.version_id AS version_id, \
                direct_rows.plugin_key AS plugin_key, \
                {final_snapshot_projection}\
                direct_rows.schema_version AS schema_version, \
                direct_rows.created_at AS created_at, \
                direct_rows.updated_at AS updated_at, \
                direct_rows.global AS global, \
                direct_rows.change_id AS change_id, \
                direct_rows.commit_id AS commit_id, \
                direct_rows.untracked AS untracked, \
                direct_rows.writer_key AS writer_key, \
                direct_rows.metadata AS metadata{payload_projection} \
         FROM ( \
             WITH {commit_rows_cte_sql}, \
             commit_edges AS ( \
                 SELECT \
                     commit_rows.target_version_id AS version_id, \
                     {parent_value_expr} AS parent_id, \
                     commit_rows.commit_id AS child_id, \
                     commit_rows.commit_created_at AS created_at, \
                     commit_rows.commit_metadata AS metadata \
                 FROM commit_rows \
                 {parent_join_sql} \
                 WHERE {parent_value_expr} IS NOT NULL \
             ) \
             SELECT \
                 commit_edges.parent_id || '~' || commit_edges.child_id AS entity_id, \
                 'lix_commit_edge' AS schema_key, \
                 'lix' AS file_id, \
                 commit_edges.version_id AS version_id, \
                 'lix' AS plugin_key, \
                 '1' AS schema_version, \
                 commit_edges.created_at AS created_at, \
                 commit_edges.created_at AS updated_at, \
                 true AS global, \
                 NULL AS change_id, \
                 commit_edges.child_id AS commit_id, \
                 false AS untracked, \
                 NULL AS writer_key, \
                 commit_edges.metadata AS metadata, \
                 commit_edges.parent_id AS {parent_id_column}, \
                 commit_edges.child_id AS {child_id_column} \
             FROM commit_edges \
         ) AS direct_rows \
         WHERE 1 = 1{direct_predicates}",
        final_snapshot_projection = final_snapshot_projection,
        payload_projection = payload_projection,
        commit_rows_cte_sql = commit_rows_cte_sql,
        parent_join_sql = parent_join_sql,
        parent_value_expr = parent_value_expr,
        parent_id_column = parent_id_column,
        child_id_column = child_id_column,
        direct_predicates = direct_predicates,
    )
}

fn build_change_author_state_rows_sql(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
    payload_columns: &[String],
    known_live_layouts: &BTreeMap<String, JsonValue>,
    surface_variant: SurfaceVariant,
    source_predicates: &[Expr],
    include_snapshot_content: bool,
) -> String {
    let commit_rows_cte_sql = build_commit_rows_scope_cte_sql(surface_variant);
    let (change_id_join_sql, change_id_value_expr) = json_array_text_join_sql(
        dialect,
        "commit_rows.commit_snapshot_content",
        "change_ids",
        "member_change_rows",
        "change_id",
    );
    let (author_join_sql, author_value_expr) = json_array_text_join_sql(
        dialect,
        "commit_rows.commit_snapshot_content",
        "author_account_ids",
        "author_rows",
        "account_id",
    );
    let snapshot_expr = json_object_sql(
        dialect,
        &[
            ("change_id", "direct_rows.change_id_value"),
            ("account_id", "direct_rows.account_id"),
        ],
    );
    let direct_predicates =
        render_qualified_where_clause_sql(source_predicates, " AND ", "direct_rows");
    let payload_projection = render_state_payload_projection_list(
        dialect,
        surface_binding,
        "lix_change_author",
        "direct_rows",
        payload_columns,
        known_live_layouts,
    );
    let final_snapshot_projection = if include_snapshot_content {
        format!("{snapshot_expr} AS snapshot_content, ")
    } else {
        String::new()
    };
    let change_id_column = builtin_payload_column_name("lix_change_author", "change_id");
    let account_id_column = builtin_payload_column_name("lix_change_author", "account_id");
    format!(
        "SELECT direct_rows.entity_id AS entity_id, \
                direct_rows.schema_key AS schema_key, \
                direct_rows.file_id AS file_id, \
                direct_rows.version_id AS version_id, \
                direct_rows.plugin_key AS plugin_key, \
                {final_snapshot_projection}\
                direct_rows.schema_version AS schema_version, \
                direct_rows.created_at AS created_at, \
                direct_rows.updated_at AS updated_at, \
                direct_rows.global AS global, \
                direct_rows.change_id AS change_id, \
                direct_rows.commit_id AS commit_id, \
                direct_rows.untracked AS untracked, \
                direct_rows.writer_key AS writer_key, \
                direct_rows.metadata AS metadata{payload_projection} \
         FROM ( \
             WITH {commit_rows_cte_sql}, \
             change_authors AS ( \
                 SELECT \
                     commit_rows.target_version_id AS version_id, \
                     commit_rows.commit_id AS commit_id, \
                     commit_rows.commit_created_at AS created_at, \
                     commit_rows.commit_metadata AS metadata, \
                     {change_id_value_expr} AS member_change_id, \
                     {author_value_expr} AS account_id \
                 FROM commit_rows \
                 {change_id_join_sql} \
                 {author_join_sql} \
                 WHERE {change_id_value_expr} IS NOT NULL \
                   AND {author_value_expr} IS NOT NULL \
             ) \
             SELECT \
                 change_authors.member_change_id || '~' || change_authors.account_id AS entity_id, \
                 'lix_change_author' AS schema_key, \
                 'lix' AS file_id, \
                 change_authors.version_id AS version_id, \
                 'lix' AS plugin_key, \
                 '1' AS schema_version, \
                 change_authors.created_at AS created_at, \
                 change_authors.created_at AS updated_at, \
                 true AS global, \
                 change_authors.member_change_id AS change_id, \
                 change_authors.commit_id AS commit_id, \
                 false AS untracked, \
                 NULL AS writer_key, \
                 change_authors.metadata AS metadata, \
                 change_authors.member_change_id AS {change_id_column}, \
                 change_authors.account_id AS {account_id_column} \
             FROM change_authors \
         ) AS direct_rows \
         WHERE 1 = 1{direct_predicates}",
        final_snapshot_projection = final_snapshot_projection,
        payload_projection = payload_projection,
        commit_rows_cte_sql = commit_rows_cte_sql,
        change_id_join_sql = change_id_join_sql,
        change_id_value_expr = change_id_value_expr,
        author_join_sql = author_join_sql,
        author_value_expr = author_value_expr,
        change_id_column = change_id_column,
        account_id_column = account_id_column,
        direct_predicates = direct_predicates,
    )
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
