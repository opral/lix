use sqlparser::ast::{Expr, Query, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::sql::planner::vtable_read as planner_vtable_read;
use crate::sql::quote_ident;
use crate::{LixBackend, LixError};

const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    let op = planner_vtable_read::infer_op(&query);
    let root_plan = planner_vtable_read::plan_without_backend(&op);
    planner_vtable_read::rewrite_query_with_plan(query, &root_plan, &mut |plan| {
        build_untracked_union_query_from_plan(plan)
    })
}

pub async fn rewrite_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
) -> Result<Option<Query>, LixError> {
    let op = planner_vtable_read::infer_op(&query);
    let root_plan = planner_vtable_read::plan_with_backend(backend, &op).await?;
    planner_vtable_read::rewrite_query_with_plan(query, &root_plan, &mut |plan| {
        build_untracked_union_query_from_plan(plan)
    })
}

fn build_untracked_union_query(
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
) -> Result<Query, LixError> {
    let dialect = GenericDialect {};
    let predicate_sql = pushdown_predicate.map(ToString::to_string);
    let effective_schema_keys = schema_keys.to_vec();

    let schema_list = effective_schema_keys
        .iter()
        .map(|key| format!("'{}'", escape_string_literal(key)))
        .collect::<Vec<_>>()
        .join(", ");
    let schema_filter = if effective_schema_keys.is_empty() {
        None
    } else {
        Some(format!("schema_key IN ({schema_list})"))
    };
    let untracked_where = match (schema_filter.as_ref(), predicate_sql.as_ref()) {
        (Some(schema_filter), Some(predicate)) => {
            format!("{schema_filter} AND ({predicate})")
        }
        (Some(schema_filter), None) => schema_filter.clone(),
        (None, Some(predicate)) => format!("({predicate})"),
        (None, None) => "1=1".to_string(),
    };

    let mut union_parts = Vec::new();
    union_parts.push(format!(
        "SELECT entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, \
                created_at, updated_at, NULL AS inherited_from_version_id, 'untracked' AS change_id, NULL AS writer_key, 1 AS untracked, 1 AS priority \
         FROM {untracked} \
         WHERE {untracked_where}",
        untracked = UNTRACKED_TABLE
    ));

    for key in &effective_schema_keys {
        let materialized_table = format!("{MATERIALIZED_PREFIX}{key}");
        let materialized_ident = quote_ident(&materialized_table);
        let materialized_where = predicate_sql
            .as_ref()
            .map(|predicate| format!(" WHERE ({predicate})"))
            .unwrap_or_default();
        union_parts.push(format!(
            "SELECT entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, \
                    created_at, updated_at, inherited_from_version_id, change_id, writer_key, 0 AS untracked, 2 AS priority \
             FROM {materialized}{materialized_where}",
            materialized = materialized_ident,
            materialized_where = materialized_where
        ));
    }

    let union_sql = union_parts.join(" UNION ALL ");

    let sql = format!(
        "SELECT entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, \
                created_at, updated_at, inherited_from_version_id, change_id, writer_key, untracked \
         FROM (\
             SELECT entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, \
                    created_at, updated_at, inherited_from_version_id, change_id, writer_key, untracked, \
                    ROW_NUMBER() OVER (PARTITION BY entity_id, schema_key, file_id, version_id ORDER BY priority) AS rn \
             FROM ({union_sql}) AS lix_state_union\
         ) AS lix_state_ranked \
         WHERE rn = 1",
    );

    let mut statements = Parser::parse_sql(&dialect, &sql).map_err(|err| LixError {
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

fn build_untracked_union_query_from_plan(
    plan: &planner_vtable_read::VtableReadPlan,
) -> Result<Query, LixError> {
    build_untracked_union_query(
        &plan.effective_schema_keys,
        plan.pushdown_predicate.as_ref(),
    )
}

fn escape_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
fn extract_plugin_keys_from_query(query: &Query) -> Option<Vec<String>> {
    planner_vtable_read::extract_plugin_keys_from_query(query)
}

#[cfg(test)]
fn extract_plugin_keys_from_top_level_derived_subquery(query: &Query) -> Option<Vec<String>> {
    planner_vtable_read::extract_plugin_keys_from_top_level_derived_subquery(query)
}

#[cfg(test)]
fn extract_schema_keys_from_query_deep(query: &Query) -> Vec<String> {
    planner_vtable_read::extract_schema_keys_from_query_deep(query)
}

#[cfg(test)]
mod tests {
    use super::{
        build_untracked_union_query_from_plan, extract_plugin_keys_from_query,
        extract_plugin_keys_from_top_level_derived_subquery, extract_schema_keys_from_query_deep,
        planner_vtable_read,
    };
    use crate::sql::preprocess_sql_rewrite_only as preprocess_sql;
    use sqlparser::ast::{Query, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn compact_sql(sql: &str) -> String {
        sql.chars().filter(|c| !c.is_whitespace()).collect()
    }

    fn union_segment(sql: &str) -> &str {
        let end = sql
            .find(")ASlix_state_union")
            .expect("union segment end not found");
        let start = sql[..end]
            .rfind("FROM(")
            .expect("union segment start not found");
        &sql[start + 5..end]
    }

    fn assert_branch_contains_all(sql: &str, table_marker: &str, needles: &[&str]) {
        let union_sql = union_segment(sql);
        let start = union_sql
            .find(table_marker)
            .or_else(|| union_sql.find(&table_marker.replace('"', "")))
            .expect("table marker not found");
        let slice = &union_sql[start..];
        let end = slice.find("UNIONALL").unwrap_or(slice.len());
        let branch = &slice[..end];
        for needle in needles {
            assert!(
                branch.contains(needle),
                "expected branch for {table_marker} to contain {needle}, got: {branch}"
            );
        }
    }

    fn assert_branch_not_contains(sql: &str, table_marker: &str, needle: &str) {
        let union_sql = union_segment(sql);
        let start = union_sql
            .find(table_marker)
            .or_else(|| union_sql.find(&table_marker.replace('"', "")))
            .expect("table marker not found");
        let slice = &union_sql[start..];
        let end = slice.find("UNIONALL").unwrap_or(slice.len());
        let branch = &slice[..end];
        assert!(
            !branch.contains(needle),
            "expected branch for {table_marker} to not contain {needle}, got: {branch}"
        );
    }

    #[test]
    fn rewrite_pushes_down_predicates_for_schema_key_in() {
        let sql = "SELECT * FROM lix_internal_state_vtable WHERE schema_key IN ('schema_a', 'schema_b') AND entity_id = 'entity-1'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &[
                "schema_keyIN('schema_a','schema_b')",
                "entity_id='entity-1'",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &[
                "schema_keyIN('schema_a','schema_b')",
                "entity_id='entity-1'",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_b"#,
            &[
                "schema_keyIN('schema_a','schema_b')",
                "entity_id='entity-1'",
            ],
        );
    }

    #[test]
    fn rewrite_pushes_down_like_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND entity_id LIKE 'entity-%'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_key='schema_a'", "entity_idLIKE'entity-%'"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &["entity_idLIKE'entity-%'"],
        );
    }

    #[test]
    fn rewrite_pushes_down_or_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key IN ('schema_a', 'schema_b') \
            AND (entity_id = 'entity-1' OR file_id = 'file-1')";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &[
                "schema_keyIN('schema_a','schema_b')",
                "entity_id='entity-1'ORfile_id='file-1'",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &["entity_id='entity-1'ORfile_id='file-1'"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_b"#,
            &["entity_id='entity-1'ORfile_id='file-1'"],
        );
    }

    #[test]
    fn rewrite_skips_or_with_non_schema_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' OR entity_id = 'entity-1'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert!(
            !compact.contains("lix_internal_state_untracked"),
            "expected no rewrite for OR with non-schema predicate, got: {compact}"
        );
    }

    #[test]
    fn rewrite_does_not_pushdown_qualified_identifiers() {
        let sql = "SELECT * FROM lix_internal_state_vtable AS a \
            WHERE a.schema_key = 'schema_a' AND a.entity_id = 'entity-1'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_keyIN('schema_a')"],
        );
        assert_branch_not_contains(
            &compact,
            "FROMlix_internal_state_untracked",
            "entity_id='entity-1'",
        );
        assert_branch_not_contains(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            "entity_id='entity-1'",
        );
    }

    #[test]
    fn rewrite_pushes_down_comparison_predicates() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND file_id >= 'file-2' AND entity_id <> 'entity-1'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &[
                "schema_key='schema_a'",
                "file_id>='file-2'",
                "entity_id<>'entity-1'",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &["file_id>='file-2'", "entity_id<>'entity-1'"],
        );
    }

    #[test]
    fn rewrite_pushes_down_not_in_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND entity_id NOT IN ('entity-1', 'entity-2')";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &[
                "schema_key='schema_a'",
                "entity_idNOTIN('entity-1','entity-2')",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &["entity_idNOTIN('entity-1','entity-2')"],
        );
    }

    #[test]
    fn rewrite_pushes_down_is_null_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND snapshot_content IS NULL";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_key='schema_a'", "snapshot_contentISNULL"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &["snapshot_contentISNULL"],
        );
    }

    #[test]
    fn rewrite_pushes_down_between_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND entity_id BETWEEN 'a' AND 'm'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_key='schema_a'", "entity_idBETWEEN'a'AND'm'"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &["entity_idBETWEEN'a'AND'm'"],
        );
    }

    #[test]
    fn rewrite_pushes_down_not_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND NOT (entity_id = 'entity-1')";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_key='schema_a'", "NOT(entity_id='entity-1')"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &["NOT(entity_id='entity-1')"],
        );
    }

    #[test]
    fn extracts_plugin_keys_from_eq_and_in_list() {
        let query = parse_query(
            "SELECT * FROM lix_internal_state_vtable \
             WHERE plugin_key = 'plugin_json' OR plugin_key IN ('plugin_text', 'plugin_json')",
        );
        let keys = extract_plugin_keys_from_query(&query).expect("plugin keys should be extracted");
        assert_eq!(
            keys,
            vec!["plugin_json".to_string(), "plugin_text".to_string()]
        );
    }

    #[test]
    fn extracts_plugin_keys_from_qualified_identifier() {
        let query = parse_query(
            "SELECT * FROM lix_internal_state_vtable AS s WHERE s.plugin_key = 'plugin_json'",
        );
        let keys = extract_plugin_keys_from_query(&query).expect("plugin keys should be extracted");
        assert_eq!(keys, vec!["plugin_json".to_string()]);
    }

    #[test]
    fn extracts_plugin_keys_from_derived_subquery_filter() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM (SELECT * FROM lix_internal_state_vtable WHERE plugin_key = 'plugin_json') AS ranked",
        );
        let keys = extract_plugin_keys_from_top_level_derived_subquery(&query)
            .expect("plugin keys should be extracted");
        assert_eq!(keys, vec!["plugin_json".to_string()]);
    }

    #[test]
    fn plugin_key_extraction_skips_mixed_or_predicate() {
        let query = parse_query(
            "SELECT * FROM lix_internal_state_vtable \
             WHERE plugin_key = 'plugin_json' OR schema_key = 'json_pointer'",
        );
        assert!(
            extract_plugin_keys_from_query(&query).is_none(),
            "mixed OR should not produce a plugin-only key set"
        );
    }

    #[test]
    fn extracts_schema_keys_from_nested_derived_subquery_filter() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM (SELECT * FROM lix_internal_state_vtable WHERE schema_key = 'schema_a') AS ranked",
        );
        let keys = extract_schema_keys_from_query_deep(&query);
        assert_eq!(keys, vec!["schema_a".to_string()]);
    }

    #[test]
    fn narrows_materialized_union_to_schema_predicate_intersection() {
        let query = parse_query(
            "SELECT * FROM lix_internal_state_vtable \
             WHERE schema_key = 'schema_a' AND entity_id = 'entity-1'",
        );
        let root_plan = planner_vtable_read::RootVtableReadPlan {
            schema_keys: vec![
                "schema_a".to_string(),
                "schema_b".to_string(),
                "schema_c".to_string(),
            ],
        };
        let rewritten =
            planner_vtable_read::rewrite_query_with_plan(query, &root_plan, &mut |plan| {
                build_untracked_union_query_from_plan(plan)
            })
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let compact = compact_sql(&rewritten.to_string());

        assert!(compact.contains(r#"lix_internal_state_materialized_v1_schema_a"#));
        assert!(!compact.contains(r#"lix_internal_state_materialized_v1_schema_b"#));
        assert!(!compact.contains(r#"lix_internal_state_materialized_v1_schema_c"#));
    }

    fn parse_query(sql: &str) -> Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("valid SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            _ => panic!("expected query"),
        }
    }
}
