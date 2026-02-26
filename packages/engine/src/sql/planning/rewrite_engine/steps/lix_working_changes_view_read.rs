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
            version_pointer AS ( \
                SELECT snapshot_content \
                FROM lix_internal_state_materialized_v1_lix_version_pointer \
                WHERE schema_key = 'lix_version_pointer' \
                  AND entity_id = (SELECT version_id FROM active_version) \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND is_tombstone = 0 \
                  AND snapshot_content IS NOT NULL \
                LIMIT 1 \
            ), \
            wc AS ( \
                SELECT lix_json_extract(snapshot_content, 'working_commit_id') AS id \
                FROM version_pointer \
                LIMIT 1 \
            ), \
            commit_rows AS ( \
                SELECT \
                    entity_id AS id, \
                    lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id \
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
                    lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id \
                FROM lix_internal_state_materialized_v1_lix_commit \
                WHERE schema_key = 'lix_commit' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND is_tombstone = 0 \
                  AND snapshot_content IS NOT NULL \
            ), \
            working_change_rows AS ( \
                SELECT \
                    entity_id AS change_id, \
                    lix_json_extract(snapshot_content, 'entity_id') AS entity_id, \
                    lix_json_extract(snapshot_content, 'schema_key') AS schema_key, \
                    lix_json_extract(snapshot_content, 'file_id') AS file_id, \
                    lix_json_extract(snapshot_content, 'snapshot_content') AS row_snapshot \
                FROM lix_internal_state_untracked \
                WHERE schema_key = 'lix_change' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND snapshot_content IS NOT NULL \
            ), \
            working_change_set_element_rows AS ( \
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
            ), \
            commit_edge_rows AS ( \
                SELECT \
                    lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_extract(snapshot_content, 'child_id') AS child_id \
                FROM lix_internal_state_untracked \
                WHERE schema_key = 'lix_commit_edge' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND snapshot_content IS NOT NULL \
 \
                UNION \
 \
                SELECT \
                    lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
                    lix_json_extract(snapshot_content, 'child_id') AS child_id \
                FROM lix_internal_state_materialized_v1_lix_commit_edge \
                WHERE schema_key = 'lix_commit_edge' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND is_tombstone = 0 \
                  AND snapshot_content IS NOT NULL \
            ), \
            label_rows AS ( \
                SELECT \
                    entity_id AS id, \
                    lix_json_extract(snapshot_content, 'name') AS name \
                FROM lix_internal_state_untracked \
                WHERE schema_key = 'lix_label' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND snapshot_content IS NOT NULL \
 \
                UNION \
 \
                SELECT \
                    entity_id AS id, \
                    lix_json_extract(snapshot_content, 'name') AS name \
                FROM lix_internal_state_materialized_v1_lix_label \
                WHERE schema_key = 'lix_label' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND is_tombstone = 0 \
                  AND snapshot_content IS NOT NULL \
            ), \
            entity_label_rows AS ( \
                SELECT \
                    lix_json_extract(snapshot_content, 'entity_id') AS entity_id, \
                    lix_json_extract(snapshot_content, 'schema_key') AS schema_key, \
                    lix_json_extract(snapshot_content, 'label_id') AS label_id \
                FROM lix_internal_state_untracked \
                WHERE schema_key = 'lix_entity_label' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND snapshot_content IS NOT NULL \
 \
                UNION \
 \
                SELECT \
                    lix_json_extract(snapshot_content, 'entity_id') AS entity_id, \
                    lix_json_extract(snapshot_content, 'schema_key') AS schema_key, \
                    lix_json_extract(snapshot_content, 'label_id') AS label_id \
                FROM lix_internal_state_materialized_v1_lix_entity_label \
                WHERE schema_key = 'lix_entity_label' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND is_tombstone = 0 \
                  AND snapshot_content IS NOT NULL \
            ), \
            checkpoint_change_rows AS ( \
                SELECT \
                    entity_id AS change_id, \
                    lix_json_extract(snapshot_content, 'entity_id') AS entity_id, \
                    lix_json_extract(snapshot_content, 'schema_key') AS schema_key, \
                    lix_json_extract(snapshot_content, 'file_id') AS file_id \
                FROM lix_internal_state_untracked \
                WHERE schema_key = 'lix_change' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND (metadata IS NULL OR metadata != '{\"lix_internal_working_projection\":true}') \
                  AND snapshot_content IS NOT NULL \
 \
                UNION \
 \
                SELECT \
                    entity_id AS change_id, \
                    lix_json_extract(snapshot_content, 'entity_id') AS entity_id, \
                    lix_json_extract(snapshot_content, 'schema_key') AS schema_key, \
                    lix_json_extract(snapshot_content, 'file_id') AS file_id \
                FROM lix_internal_state_materialized_v1_lix_change \
                WHERE schema_key = 'lix_change' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND is_tombstone = 0 \
                  AND snapshot_content IS NOT NULL \
            ), \
            checkpoint_change_set_element_rows AS ( \
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
                  AND (metadata IS NULL OR metadata != '{\"lix_internal_working_projection\":true}') \
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
            cc AS ( \
                SELECT COALESCE( \
                    ( \
                        SELECT ce.parent_id \
                        FROM commit_edge_rows ce \
                        INNER JOIN entity_label_rows el \
                            ON el.entity_id = ce.parent_id \
                           AND el.schema_key = 'lix_commit' \
                        INNER JOIN label_rows l ON l.id = el.label_id \
                        WHERE ce.child_id = (SELECT id FROM wc) \
                          AND l.name = 'checkpoint' \
                        LIMIT 1 \
                    ), \
                    (SELECT id FROM wc) \
                ) AS id \
            ), \
            wcs AS ( \
                SELECT c.change_set_id \
                FROM commit_rows c \
                WHERE c.id = (SELECT id FROM wc) \
            ), \
            ccs AS ( \
                SELECT c.change_set_id \
                FROM commit_rows c \
                WHERE c.id = (SELECT id FROM cc) \
            ) \
            SELECT * \
            FROM ( \
                SELECT \
                    ch.entity_id AS entity_id, \
                    ch.schema_key AS schema_key, \
                    ch.file_id AS file_id, \
                    bcse.change_id AS before_change_id, \
                    ch.change_id AS after_change_id, \
                    (SELECT id FROM cc) AS before_commit_id, \
                    (SELECT id FROM wc) AS after_commit_id, \
                    CASE \
                        WHEN bcse.change_id IS NOT NULL AND ch.row_snapshot IS NULL THEN 'removed' \
                        WHEN bcse.change_id IS NULL AND ch.row_snapshot IS NOT NULL THEN 'added' \
                        WHEN bcse.change_id IS NOT NULL \
                             AND ch.row_snapshot IS NOT NULL \
                             AND bcse.change_id != ch.change_id THEN 'modified' \
                        ELSE 'unchanged' \
                    END AS status \
                FROM working_change_rows ch \
                INNER JOIN working_change_set_element_rows cse ON cse.change_id = ch.change_id \
                LEFT JOIN checkpoint_change_set_element_rows bcse \
                    ON bcse.entity_id = ch.entity_id \
                   AND bcse.schema_key = ch.schema_key \
                   AND bcse.file_id = ch.file_id \
                   AND bcse.change_set_id = (SELECT change_set_id FROM ccs) \
                WHERE cse.change_set_id = (SELECT change_set_id FROM wcs) \
 \
                UNION ALL \
 \
                SELECT \
                    bc.entity_id AS entity_id, \
                    bc.schema_key AS schema_key, \
                    bc.file_id AS file_id, \
                    bc.change_id AS before_change_id, \
                    bc.change_id AS after_change_id, \
                    (SELECT id FROM cc) AS before_commit_id, \
                    (SELECT id FROM wc) AS after_commit_id, \
                    'unchanged' AS status \
                FROM checkpoint_change_set_element_rows bcse \
                INNER JOIN checkpoint_change_rows bc ON bc.change_id = bcse.change_id \
                LEFT JOIN working_change_set_element_rows wcse \
                    ON wcse.entity_id = bc.entity_id \
                   AND wcse.schema_key = bc.schema_key \
                   AND wcse.file_id = bc.file_id \
                   AND wcse.change_set_id = (SELECT change_set_id FROM wcs) \
                WHERE bcse.change_set_id = (SELECT change_set_id FROM ccs) \
                  AND wcse.change_id IS NULL \
            ) AS working_changes";
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
        assert!(sql.contains("FROM lix_internal_state_untracked"));
        assert!(sql.contains("working_change_set_element_rows"));
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
