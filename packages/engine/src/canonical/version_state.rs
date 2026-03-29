use std::collections::BTreeMap;

use crate::backend::QueryExecutor;
use crate::version::{
    parse_version_descriptor_snapshot, version_descriptor_file_id, version_descriptor_plugin_key,
    version_descriptor_schema_key, version_descriptor_schema_version, GLOBAL_VERSION_ID,
};
use crate::{LixBackend, LixError, SqlDialect, Value};

use super::readers::load_committed_version_head_commit_id;
use super::refs::load_all_committed_version_refs_with_executor;
use super::state_source::{
    load_exact_committed_state_row_from_commit_with_executor, ExactCommittedStateRowRequest,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionDescriptorRow {
    pub(crate) version_id: String,
    pub(crate) name: String,
    pub(crate) hidden: bool,
    pub(crate) change_id: Option<String>,
}

pub(crate) async fn load_version_descriptor_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<VersionDescriptorRow>, LixError> {
    let mut executor = backend;
    load_version_descriptor_with_executor(&mut executor, version_id).await
}

pub(crate) async fn load_version_descriptor_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<VersionDescriptorRow>, LixError> {
    let Some(global_head_commit_id) =
        load_committed_version_head_commit_id(executor, GLOBAL_VERSION_ID).await?
    else {
        return Ok(None);
    };
    let row = load_exact_committed_state_row_from_commit_with_executor(
        executor,
        &global_head_commit_id,
        &ExactCommittedStateRowRequest {
            entity_id: version_id.to_string(),
            schema_key: version_descriptor_schema_key().to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            exact_filters: BTreeMap::from([
                (
                    "file_id".to_string(),
                    Value::Text(version_descriptor_file_id().to_string()),
                ),
                (
                    "plugin_key".to_string(),
                    Value::Text(version_descriptor_plugin_key().to_string()),
                ),
                (
                    "schema_version".to_string(),
                    Value::Text(version_descriptor_schema_version().to_string()),
                ),
            ]),
        },
    )
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let Some(Value::Text(snapshot_content)) = row.values.get("snapshot_content") else {
        return Ok(None);
    };
    Ok(Some(parse_descriptor_row(
        snapshot_content,
        row.source_change_id,
    )?))
}

pub(crate) async fn load_all_version_descriptors_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<VersionDescriptorRow>, LixError> {
    let mut descriptors = Vec::new();
    for version_ref in load_all_committed_version_refs_with_executor(executor).await? {
        if let Some(descriptor) =
            load_version_descriptor_with_executor(executor, &version_ref.version_id).await?
        {
            descriptors.push(descriptor);
        }
    }
    descriptors.sort_by(|left, right| left.version_id.cmp(&right.version_id));
    Ok(descriptors)
}

pub(crate) async fn find_version_id_by_name_with_backend(
    backend: &dyn LixBackend,
    name: &str,
) -> Result<Option<String>, LixError> {
    let mut executor = backend;
    find_version_id_by_name_with_executor(&mut executor, name).await
}

pub(crate) async fn find_version_id_by_name_with_executor(
    executor: &mut dyn QueryExecutor,
    name: &str,
) -> Result<Option<String>, LixError> {
    for descriptor in load_all_version_descriptors_with_executor(executor).await? {
        if descriptor.name == name {
            return Ok(Some(descriptor.version_id));
        }
    }
    Ok(None)
}

pub(crate) async fn version_exists_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<bool, LixError> {
    Ok(load_version_descriptor_with_backend(backend, version_id)
        .await?
        .is_some())
}

pub(crate) async fn version_exists_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<bool, LixError> {
    Ok(load_version_descriptor_with_executor(executor, version_id)
        .await?
        .is_some())
}

pub(crate) fn build_admin_version_source_sql(dialect: SqlDialect) -> String {
    let current_refs_cte_sql = build_current_version_refs_unique_cte_sql(dialect);
    let name_expr = json_text_extract_sql(dialect, "d.snapshot_content", "name");
    let hidden_expr = json_boolean_extract_sql(dialect, "d.snapshot_content", "hidden");
    let (parent_join_sql, parent_value_expr) = json_array_text_join_sql(
        dialect,
        "commit_headers.commit_snapshot_content",
        "parent_commit_ids",
        "parent_rows",
        "parent_commit_id",
    );
    let (change_join_sql, change_value_expr, change_position_expr) =
        json_array_text_join_with_position_sql(
            dialect,
            "commit_headers.commit_snapshot_content",
            "change_ids",
            "change_rows",
            "change_id",
            "change_position",
        );
    format!(
        "WITH RECURSIVE \
         {current_refs_cte_sql}\
         global_head AS ( \
             SELECT commit_id \
             FROM current_refs \
             WHERE version_id = '{global_version}' \
         ), \
         reachable_global_commit_walk AS ( \
             SELECT commit_id, 0 AS depth \
             FROM global_head \
             UNION ALL \
             SELECT \
               {parent_value_expr} AS commit_id, \
               walk.depth + 1 AS depth \
             FROM reachable_global_commit_walk walk \
             JOIN canonical_commit_headers commit_headers \
               ON commit_headers.commit_id = walk.commit_id \
             {parent_join_sql} \
             WHERE {parent_value_expr} IS NOT NULL \
         ), \
         reachable_global_commits AS ( \
             SELECT commit_id, MIN(depth) AS depth \
             FROM reachable_global_commit_walk \
             GROUP BY commit_id \
         ), \
         descriptor_members AS ( \
             SELECT \
               descriptor_change.entity_id AS entity_id, \
               descriptor_change.id AS change_id, \
               descriptor_snapshot.content AS snapshot_content, \
               reachable.depth AS depth, \
               {change_position_expr} AS change_position \
             FROM reachable_global_commits reachable \
             JOIN canonical_commit_headers commit_headers \
               ON commit_headers.commit_id = reachable.commit_id \
             {change_join_sql} \
             JOIN lix_internal_change descriptor_change \
               ON descriptor_change.id = {change_value_expr} \
             LEFT JOIN lix_internal_snapshot descriptor_snapshot \
               ON descriptor_snapshot.id = descriptor_change.snapshot_id \
             WHERE descriptor_change.schema_key = '{descriptor_schema_key}' \
               AND descriptor_change.schema_version = '{descriptor_schema_version}' \
               AND descriptor_change.file_id = '{descriptor_file_id}' \
               AND descriptor_change.plugin_key = '{descriptor_plugin_key}' \
         ), \
         ranked_descriptors AS ( \
             SELECT \
               entity_id, \
               snapshot_content, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY entity_id \
                 ORDER BY depth ASC, change_position DESC \
               ) AS rn \
             FROM descriptor_members \
         ), \
         descriptor_state AS ( \
             SELECT entity_id, snapshot_content \
             FROM ranked_descriptors \
             WHERE rn = 1 \
               AND snapshot_content IS NOT NULL \
         ) \
         SELECT \
             d.entity_id AS id, \
             COALESCE({name_expr}, '') AS name, \
             COALESCE({hidden_expr}, false) AS hidden, \
             COALESCE(r.commit_id, '') AS commit_id \
         FROM descriptor_state d \
         LEFT JOIN current_refs r \
           ON r.version_id = d.entity_id \
         ORDER BY d.entity_id ASC",
        current_refs_cte_sql = current_refs_cte_sql,
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        parent_join_sql = parent_join_sql,
        parent_value_expr = parent_value_expr,
        change_join_sql = change_join_sql,
        change_value_expr = change_value_expr,
        change_position_expr = change_position_expr,
        name_expr = name_expr,
        hidden_expr = hidden_expr,
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_schema_version = escape_sql_string(version_descriptor_schema_version()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_plugin_key = escape_sql_string(version_descriptor_plugin_key()),
    )
}

fn parse_descriptor_row(
    snapshot_content: &str,
    change_id: Option<String>,
) -> Result<VersionDescriptorRow, LixError> {
    let snapshot = parse_version_descriptor_snapshot(snapshot_content)?;
    Ok(VersionDescriptorRow {
        version_id: snapshot.id,
        name: snapshot.name.unwrap_or_default(),
        hidden: snapshot.hidden,
        change_id,
    })
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn build_current_version_refs_unique_cte_sql(dialect: SqlDialect) -> String {
    let (parent_join_sql, parent_value_expr) = json_array_text_join_sql(
        dialect,
        "commit_headers.commit_snapshot_content",
        "parent_commit_ids",
        "parent_rows",
        "parent_commit_id",
    );
    let commit_id_expr = json_text_extract_sql(dialect, "ref_snapshot.content", "commit_id");
    format!(
        "canonical_commit_headers AS ( \
             SELECT \
               commit_change.entity_id AS commit_id, \
               commit_snapshot.content AS commit_snapshot_content \
             FROM lix_internal_change commit_change \
             LEFT JOIN lix_internal_snapshot commit_snapshot \
               ON commit_snapshot.id = commit_change.snapshot_id \
             WHERE commit_change.schema_key = 'lix_commit' \
               AND commit_change.file_id = 'lix' \
               AND commit_change.plugin_key = 'lix' \
               AND commit_snapshot.content IS NOT NULL \
         ), \
         version_ref_facts AS ( \
             SELECT DISTINCT \
               ref_change.entity_id AS version_id, \
               {commit_id_expr} AS commit_id \
             FROM lix_internal_change ref_change \
             LEFT JOIN lix_internal_snapshot ref_snapshot \
               ON ref_snapshot.id = ref_change.snapshot_id \
             WHERE ref_change.schema_key = '{ref_schema_key}' \
               AND ref_change.schema_version = '{ref_schema_version}' \
               AND ref_change.file_id = '{ref_file_id}' \
               AND ref_change.plugin_key = '{ref_plugin_key}' \
               AND ref_snapshot.content IS NOT NULL \
               AND COALESCE({commit_id_expr}, '') <> '' \
         ), \
         ancestry_walk AS ( \
             SELECT \
               facts.version_id AS version_id, \
               facts.commit_id AS head_commit_id, \
               facts.commit_id AS ancestor_commit_id \
             FROM version_ref_facts facts \
             UNION ALL \
             SELECT \
               walk.version_id AS version_id, \
               walk.head_commit_id AS head_commit_id, \
               {parent_value_expr} AS ancestor_commit_id \
             FROM ancestry_walk walk \
             JOIN canonical_commit_headers commit_headers \
               ON commit_headers.commit_id = walk.ancestor_commit_id \
             {parent_join_sql} \
             WHERE {parent_value_expr} IS NOT NULL \
         ), \
         overshadowed AS ( \
             SELECT DISTINCT \
               older.version_id AS version_id, \
               older.commit_id AS commit_id \
             FROM version_ref_facts older \
             JOIN ancestry_walk walk \
               ON walk.version_id = older.version_id \
              AND walk.ancestor_commit_id = older.commit_id \
              AND walk.head_commit_id <> older.commit_id \
         ), \
         current_ref_candidates AS ( \
             SELECT \
               facts.version_id AS version_id, \
               facts.commit_id AS commit_id \
             FROM version_ref_facts facts \
             LEFT JOIN overshadowed \
               ON overshadowed.version_id = facts.version_id \
              AND overshadowed.commit_id = facts.commit_id \
             WHERE overshadowed.commit_id IS NULL \
         ), \
         current_ref_counts AS ( \
             SELECT version_id, COUNT(*) AS candidate_count \
             FROM current_ref_candidates \
             GROUP BY version_id \
         ), \
         current_refs AS ( \
             SELECT \
               candidates.version_id AS version_id, \
               candidates.commit_id AS commit_id \
             FROM current_ref_candidates candidates \
             JOIN current_ref_counts counts \
               ON counts.version_id = candidates.version_id \
             WHERE counts.candidate_count = 1 \
         ), ",
        ref_schema_key = escape_sql_string(crate::version::version_ref_schema_key()),
        ref_schema_version = escape_sql_string(crate::version::version_ref_schema_version()),
        ref_file_id = escape_sql_string(crate::version::version_ref_file_id()),
        ref_plugin_key = escape_sql_string(crate::version::version_ref_plugin_key()),
        parent_value_expr = parent_value_expr,
        parent_join_sql = parent_join_sql,
        commit_id_expr = commit_id_expr,
    )
}

fn json_array_text_join_sql(
    dialect: SqlDialect,
    json_column: &str,
    field: &str,
    alias: &str,
    value_column: &str,
) -> (String, String) {
    match dialect {
        SqlDialect::Sqlite => (
            format!("JOIN json_each({json_column}, '$.{field}') AS {alias}"),
            format!("{alias}.value"),
        ),
        SqlDialect::Postgres => (
            format!(
                "JOIN LATERAL jsonb_array_elements_text(CAST({json_column} AS JSONB) -> '{field}') AS {alias}({value_column}) ON TRUE"
            ),
            format!("{alias}.{value_column}"),
        ),
    }
}

fn json_array_text_join_with_position_sql(
    dialect: SqlDialect,
    json_column: &str,
    field: &str,
    alias: &str,
    value_column: &str,
    position_column: &str,
) -> (String, String, String) {
    match dialect {
        SqlDialect::Sqlite => (
            format!("JOIN json_each({json_column}, '$.{field}') AS {alias}"),
            format!("{alias}.value"),
            format!("CAST({alias}.key AS INTEGER)"),
        ),
        SqlDialect::Postgres => (
            format!(
                "JOIN LATERAL jsonb_array_elements_text(CAST({json_column} AS JSONB) -> '{field}') WITH ORDINALITY AS {alias}({value_column}, {position_column}) ON TRUE"
            ),
            format!("{alias}.{value_column}"),
            format!("{alias}.{position_column}"),
        ),
    }
}

fn json_text_extract_sql(dialect: SqlDialect, json_column: &str, field: &str) -> String {
    match dialect {
        SqlDialect::Sqlite => format!("json_extract({json_column}, '$.{field}')"),
        SqlDialect::Postgres => format!("CAST({json_column} AS JSONB) ->> '{field}'"),
    }
}

fn json_boolean_extract_sql(dialect: SqlDialect, json_column: &str, field: &str) -> String {
    match dialect {
        SqlDialect::Sqlite => format!("json_extract({json_column}, '$.{field}')"),
        SqlDialect::Postgres => {
            format!("CAST((CAST({json_column} AS JSONB) ->> '{field}') AS BOOLEAN)")
        }
    }
}
