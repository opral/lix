use sqlparser::ast::{Query, Select, TableFactor};

use crate::engine::sql::planning::rewrite_engine::{
    default_alias, object_name_matches, parse_single_query, rewrite_query_with_select_rewriter,
    rewrite_table_factors_in_select,
};
use crate::LixError;

const LIX_WORKING_CHANGES_VIEW_NAME: &str = "lix_working_changes";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_with_select_rewriter(query, &mut rewrite_select)
}

fn rewrite_select(select: &mut Select, changed: &mut bool) -> Result<(), LixError> {
    rewrite_table_factors_in_select(select, &mut rewrite_table_factor, changed)
}

fn rewrite_table_factor(relation: &mut TableFactor, changed: &mut bool) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if object_name_matches(name, LIX_WORKING_CHANGES_VIEW_NAME) =>
        {
            let derived_query = build_lix_working_changes_view_query()?;
            let derived_alias = alias
                .clone()
                .or_else(|| Some(default_lix_working_changes_alias()));
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

fn build_lix_working_changes_view_query() -> Result<Query, LixError> {
    let sql = "WITH \
            active_version AS ( \
                SELECT lix_json_extract(snapshot_content, 'version_id') AS version_id \
                FROM lix_internal_state_untracked \
                WHERE schema_key = 'lix_active_version' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND snapshot_content IS NOT NULL \
                ORDER BY updated_at DESC \
                LIMIT 1 \
            ), \
            scope_heads AS ( \
                SELECT \
                    'local' AS scope, \
                    (SELECT version_id FROM active_version) AS checkpoint_version_id, \
                    ( \
                        SELECT lix_json_extract(snapshot_content, 'commit_id') \
                        FROM lix_internal_state_materialized_v1_lix_version_pointer \
                        WHERE schema_key = 'lix_version_pointer' \
                          AND entity_id = (SELECT version_id FROM active_version) \
                          AND file_id = 'lix' \
                          AND version_id = 'global' \
                          AND is_tombstone = 0 \
                          AND snapshot_content IS NOT NULL \
                        LIMIT 1 \
                    ) AS tip_commit_id \
                UNION ALL \
                SELECT \
                    'global' AS scope, \
                    'global' AS checkpoint_version_id, \
                    ( \
                        SELECT lix_json_extract(snapshot_content, 'commit_id') \
                        FROM lix_internal_state_materialized_v1_lix_global_pointer \
                        WHERE schema_key = 'lix_global_pointer' \
                          AND entity_id = 'global' \
                          AND file_id = 'lix' \
                          AND version_id = 'global' \
                          AND global = true \
                          AND is_tombstone = 0 \
                          AND snapshot_content IS NOT NULL \
                        LIMIT 1 \
                    ) AS tip_commit_id \
            ), \
            scope_baselines AS ( \
                SELECT \
                    scope, \
                    tip_commit_id, \
                    COALESCE( \
                        ( \
                            SELECT checkpoint_commit_id \
                            FROM lix_internal_last_checkpoint \
                            WHERE version_id = checkpoint_version_id \
                            LIMIT 1 \
                        ), \
                        tip_commit_id \
                    ) AS baseline_commit_id \
                FROM scope_heads \
            ), \
            commit_rows AS ( \
                SELECT \
                    entity_id AS id, \
                    lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id, \
                    created_at \
                FROM lix_internal_state_untracked \
                WHERE schema_key = 'lix_commit' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND snapshot_content IS NOT NULL \
 \
                UNION \
 \
                SELECT \
                    entity_id AS id, \
                    lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id, \
                    created_at \
                FROM lix_internal_state_materialized_v1_lix_commit \
                WHERE schema_key = 'lix_commit' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND is_tombstone = 0 \
                  AND snapshot_content IS NOT NULL \
            ), \
            change_rows AS ( \
                SELECT \
                    ch.id AS change_id, \
                    snap.content AS row_snapshot \
                FROM lix_internal_change ch \
                LEFT JOIN lix_internal_snapshot snap \
                    ON snap.id = ch.snapshot_id \
            ), \
            change_set_element_rows AS ( \
                SELECT \
                    lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id, \
                    lix_json_extract(snapshot_content, 'change_id') AS change_id, \
                    lix_json_extract(snapshot_content, 'entity_id') AS entity_id, \
                    lix_json_extract(snapshot_content, 'schema_key') AS schema_key, \
                    lix_json_extract(snapshot_content, 'file_id') AS file_id \
                FROM lix_internal_state_untracked \
                WHERE schema_key = 'lix_change_set_element' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND snapshot_content IS NOT NULL \
 \
                UNION \
 \
                SELECT \
                    lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id, \
                    lix_json_extract(snapshot_content, 'change_id') AS change_id, \
                    lix_json_extract(snapshot_content, 'entity_id') AS entity_id, \
                    lix_json_extract(snapshot_content, 'schema_key') AS schema_key, \
                    lix_json_extract(snapshot_content, 'file_id') AS file_id \
                FROM lix_internal_state_materialized_v1_lix_change_set_element \
                WHERE schema_key = 'lix_change_set_element' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND is_tombstone = 0 \
                  AND snapshot_content IS NOT NULL \
            ), \
            tip_ancestry AS ( \
                SELECT \
                    scope.scope AS scope, \
                    anc.ancestor_id AS commit_id, \
                    anc.depth AS depth \
                FROM scope_baselines scope \
                JOIN lix_internal_commit_ancestry anc \
                    ON anc.commit_id = scope.tip_commit_id \
            ), \
            baseline_ancestry AS ( \
                SELECT \
                    scope.scope AS scope, \
                    anc.ancestor_id AS commit_id, \
                    anc.depth AS depth \
                FROM scope_baselines scope \
                JOIN lix_internal_commit_ancestry anc \
                    ON anc.commit_id = scope.baseline_commit_id \
            ), \
            tip_candidates AS ( \
                SELECT \
                    anc.scope AS scope, \
                    cse.entity_id, \
                    cse.schema_key, \
                    cse.file_id, \
                    cse.change_id, \
                    anc.depth, \
                    c.created_at AS commit_created_at \
                FROM tip_ancestry anc \
                JOIN commit_rows c \
                    ON c.id = anc.commit_id \
                JOIN change_set_element_rows cse \
                    ON cse.change_set_id = c.change_set_id \
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
                    cse.entity_id, \
                    cse.schema_key, \
                    cse.file_id, \
                    cse.change_id, \
                    anc.depth, \
                    c.created_at AS commit_created_at \
                FROM baseline_ancestry anc \
                JOIN commit_rows c \
                    ON c.id = anc.commit_id \
                JOIN change_set_element_rows cse \
                    ON cse.change_set_id = c.change_set_id \
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
 \
                UNION ALL \
 \
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
                            SELECT tip_commit_id \
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
            WHERE status IS NOT NULL";
    parse_single_query(sql)
}

fn default_lix_working_changes_alias() -> sqlparser::ast::TableAlias {
    default_alias(LIX_WORKING_CHANGES_VIEW_NAME)
}

#[cfg(test)]
mod tests {
    use super::rewrite_query;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn rewrites_lix_working_changes_to_derived_query() {
        let query = parse_query(
            "SELECT entity_id, status \
             FROM lix_working_changes \
             WHERE file_id = 'f'",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("FROM lix_working_changes"));
        assert!(sql.contains("lix_internal_last_checkpoint"));
        assert!(sql.contains("tip_entries"));
        assert!(sql.contains("baseline_entries"));
        assert!(sql.contains("paired_entries"));
        assert!(sql.contains("WHERE status IS NOT NULL"));
    }

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("valid SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            _ => panic!("expected query"),
        }
    }
}
