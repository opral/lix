use crate::canonical::graph::build_reachable_commits_from_requested_cte_sql;
use crate::live_state::{
    builtin_live_table_layout, live_column_name_for_property, tracked_live_table_name,
    untracked_live_table_name,
};
use crate::sql_support::text::escape_sql_string;
use crate::version::{
    version_ref_file_id, version_ref_schema_key, version_ref_storage_version_id, GLOBAL_VERSION_ID,
};
use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};

use super::timeline::ensure_state_history_timeline_materialized_for_root;
use super::types::{
    StateHistoryContentMode, StateHistoryLineageScope, StateHistoryOrder, StateHistoryRequest,
    StateHistoryRootScope, StateHistoryRow, StateHistoryVersionScope,
};

pub(crate) async fn load_state_history_rows(
    backend: &dyn LixBackend,
    request: &StateHistoryRequest,
) -> Result<Vec<StateHistoryRow>, LixError> {
    let required_depth = request.max_depth.unwrap_or(512);
    if let StateHistoryRootScope::RequestedRoots(root_commit_ids) = &request.root_scope {
        for root_commit_id in root_commit_ids {
            ensure_state_history_timeline_materialized_for_root(
                backend,
                root_commit_id,
                required_depth,
            )
            .await?;
        }
    }

    let sql = build_state_history_query_sql(backend.dialect(), request)?;
    let result = backend.execute(&sql, &[]).await?;
    parse_state_history_rows(result)
}

fn build_state_history_query_sql(
    dialect: SqlDialect,
    request: &StateHistoryRequest,
) -> Result<String, LixError> {
    let source_sql = build_state_history_source_sql(dialect, request)?;
    let mut predicates = Vec::new();
    if !request.entity_ids.is_empty() {
        predicates.push(render_text_in_predicate(
            "history.entity_id",
            &request.entity_ids,
        ));
    }
    if !request.file_ids.is_empty() {
        predicates.push(render_text_in_predicate(
            "history.file_id",
            &request.file_ids,
        ));
    }
    if !request.schema_keys.is_empty() {
        predicates.push(render_text_in_predicate(
            "history.schema_key",
            &request.schema_keys,
        ));
    }
    if !request.plugin_keys.is_empty() {
        predicates.push(render_text_in_predicate(
            "history.plugin_key",
            &request.plugin_keys,
        ));
    }
    if let Some(min_depth) = request.min_depth {
        predicates.push(format!("history.depth >= {min_depth}"));
    }
    if let Some(max_depth) = request.max_depth {
        predicates.push(format!("history.depth <= {max_depth}"));
    }

    let where_sql = render_where_clause_sql(&predicates, "WHERE ");
    let order_sql = match request.order {
        StateHistoryOrder::EntityFileSchemaDepthAsc => {
            "ORDER BY history.entity_id ASC, history.file_id ASC, history.schema_key ASC, history.depth ASC"
        }
    };

    Ok(format!(
        "SELECT \
           history.entity_id, \
           history.schema_key, \
           history.file_id, \
           history.plugin_key, \
           history.snapshot_content, \
           history.metadata, \
           history.schema_version, \
           history.change_id, \
           history.commit_id, \
           history.commit_created_at, \
           history.root_commit_id, \
           history.depth, \
           history.version_id \
         FROM ({source_sql}) history \
         {where_sql} \
         {order_sql}",
        source_sql = source_sql,
        where_sql = where_sql,
        order_sql = order_sql,
    ))
}

fn build_state_history_source_sql(
    dialect: SqlDialect,
    request: &StateHistoryRequest,
) -> Result<String, LixError> {
    let version_ref_table = untracked_live_table_name("lix_version_ref");
    let commit_table = tracked_live_table_name("lix_commit");
    let version_ref_commit_id_column = quote_ident(&live_payload_column_name(
        version_ref_schema_key(),
        "commit_id",
    ));
    let commit_change_set_id_column =
        quote_ident(&live_payload_column_name("lix_commit", "change_set_id"));

    let requested_root_predicates = match &request.root_scope {
        StateHistoryRootScope::RequestedRoots(root_commit_ids) => root_commit_ids
            .iter()
            .map(|value| format!("c.id = '{}'", escape_sql_string(value)))
            .collect::<Vec<_>>(),
        StateHistoryRootScope::AllRoots => Vec::new(),
    };
    let requested_version_predicates = match &request.version_scope {
        StateHistoryVersionScope::RequestedVersions(version_ids) => version_ids
            .iter()
            .map(|value| format!("d.root_version_id = '{}'", escape_sql_string(value)))
            .collect::<Vec<_>>(),
        StateHistoryVersionScope::Any => Vec::new(),
    };

    let mut requested_predicates = Vec::new();
    requested_predicates.extend(requested_root_predicates.clone());
    requested_predicates.extend(requested_version_predicates);
    if request.lineage_scope == StateHistoryLineageScope::ActiveVersion
        && requested_root_predicates.is_empty()
    {
        requested_predicates
            .push("c.id IN (SELECT root_commit_id FROM default_root_commits)".to_string());
    }
    let requested_where_sql = render_where_clause_sql(&requested_predicates, "WHERE ");

    let default_root_commits_sql =
        if request.lineage_scope == StateHistoryLineageScope::ActiveVersion {
            let active_version_id = required_active_version_id(request)?;
            format!(
                "default_root_commits AS ( \
               SELECT DISTINCT \
                 vp.{version_ref_commit_id_column} AS root_commit_id, \
                 vp.entity_id AS root_version_id \
               FROM {version_ref_table} vp \
               WHERE vp.schema_key = '{schema_key}' \
                 AND vp.file_id = '{file_id}' \
                 AND vp.version_id = '{storage_version_id}' \
                 AND vp.untracked = true \
                 AND vp.entity_id = '{active_version_id}' \
             ), ",
                version_ref_commit_id_column = version_ref_commit_id_column,
                version_ref_table = version_ref_table,
                schema_key = escape_sql_string(version_ref_schema_key()),
                file_id = escape_sql_string(version_ref_file_id()),
                storage_version_id = escape_sql_string(version_ref_storage_version_id()),
                active_version_id = escape_sql_string(active_version_id),
            )
        } else {
            format!(
                "default_root_commits AS ( \
               SELECT DISTINCT \
                 vp.{version_ref_commit_id_column} AS root_commit_id, \
                 vp.entity_id AS root_version_id \
               FROM {version_ref_table} vp \
               WHERE vp.schema_key = '{schema_key}' \
                 AND vp.file_id = '{file_id}' \
                 AND vp.version_id = '{storage_version_id}' \
                 AND vp.untracked = true \
             ), ",
                version_ref_commit_id_column = version_ref_commit_id_column,
                version_ref_table = version_ref_table,
                schema_key = escape_sql_string(version_ref_schema_key()),
                file_id = escape_sql_string(version_ref_file_id()),
                storage_version_id = escape_sql_string(version_ref_storage_version_id()),
            )
        };

    let reachable_commits_cte_sql =
        build_reachable_commits_from_requested_cte_sql(dialect, "requested_commits", 512);
    let snapshot_projection = match request.content_mode {
        StateHistoryContentMode::MetadataOnly => "NULL AS snapshot_content".to_string(),
        StateHistoryContentMode::IncludeSnapshotContent => {
            "s.content AS snapshot_content".to_string()
        }
    };
    let snapshot_join = match request.content_mode {
        StateHistoryContentMode::MetadataOnly => String::new(),
        StateHistoryContentMode::IncludeSnapshotContent => "LEFT JOIN lix_internal_snapshot s \
             ON s.id = h.snapshot_id "
            .to_string(),
    };

    Ok(format!(
        "WITH RECURSIVE \
           {default_root_commits_sql}\
           commit_by_version AS ( \
             SELECT \
               entity_id AS id, \
               {commit_change_set_id_column} AS change_set_id, \
               created_at AS created_at, \
               version_id AS lixcol_version_id \
             FROM {commit_table} \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
           ), \
           requested_commits AS ( \
             SELECT DISTINCT \
               c.id AS commit_id, \
               COALESCE(d.root_version_id, c.lixcol_version_id) AS root_version_id \
             FROM commit_by_version c \
             LEFT JOIN default_root_commits d \
               ON d.root_commit_id = c.id \
             {requested_where_sql} \
           ), \
           {reachable_commits_cte_sql}\
           filtered_reachable_commits AS ( \
             SELECT \
               rc.commit_id, \
               rc.root_commit_id, \
               rc.root_version_id, \
               rc.commit_depth, \
               c.created_at AS commit_created_at \
             FROM reachable_commits rc \
             JOIN commit_by_version c \
               ON c.id = rc.commit_id \
           ), \
           breakpoint_rows AS ( \
             SELECT \
               bp.root_commit_id, \
               bp.entity_id, \
               bp.schema_key, \
               bp.file_id, \
               bp.plugin_key, \
               bp.schema_version, \
               bp.metadata, \
               bp.snapshot_id, \
               bp.change_id, \
               bp.from_depth \
             FROM lix_internal_entity_state_timeline_breakpoint bp \
             JOIN requested_commits requested \
               ON requested.commit_id = bp.root_commit_id \
           ), \
           history_rows AS ( \
             SELECT \
               bp.entity_id, \
               bp.schema_key, \
               bp.file_id, \
               bp.plugin_key, \
               bp.schema_version, \
               bp.metadata, \
               bp.snapshot_id, \
               bp.change_id, \
               rc.commit_id AS commit_id, \
               rc.commit_created_at AS commit_created_at, \
               rc.root_commit_id AS root_commit_id, \
               rc.root_version_id AS version_id, \
               rc.commit_depth AS depth \
             FROM filtered_reachable_commits rc \
             JOIN breakpoint_rows bp \
               ON bp.root_commit_id = rc.root_commit_id \
              AND rc.commit_depth = bp.from_depth \
           ) \
         SELECT \
           h.entity_id AS entity_id, \
           h.schema_key AS schema_key, \
           h.file_id AS file_id, \
           h.plugin_key AS plugin_key, \
           {snapshot_projection}, \
           h.metadata AS metadata, \
           h.schema_version AS schema_version, \
           h.change_id AS change_id, \
           h.commit_id AS commit_id, \
           h.commit_created_at AS commit_created_at, \
           h.root_commit_id AS root_commit_id, \
           h.depth AS depth, \
           h.version_id AS version_id \
         FROM history_rows h \
         {snapshot_join}\
         WHERE h.snapshot_id != 'no-content'",
        default_root_commits_sql = default_root_commits_sql,
        commit_change_set_id_column = commit_change_set_id_column,
        commit_table = commit_table,
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        requested_where_sql = requested_where_sql,
        reachable_commits_cte_sql = reachable_commits_cte_sql,
        snapshot_projection = snapshot_projection,
        snapshot_join = snapshot_join,
    ))
}

fn parse_state_history_rows(result: QueryResult) -> Result<Vec<StateHistoryRow>, LixError> {
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        rows.push(StateHistoryRow {
            entity_id: required_text_value(&row, 0, "entity_id")?,
            schema_key: required_text_value(&row, 1, "schema_key")?,
            file_id: required_text_value(&row, 2, "file_id")?,
            plugin_key: required_text_value(&row, 3, "plugin_key")?,
            snapshot_content: optional_text_value(&row, 4, "snapshot_content")?,
            metadata: optional_text_value(&row, 5, "metadata")?,
            schema_version: required_text_value(&row, 6, "schema_version")?,
            change_id: required_text_value(&row, 7, "change_id")?,
            commit_id: required_text_value(&row, 8, "commit_id")?,
            commit_created_at: required_text_value(&row, 9, "commit_created_at")?,
            root_commit_id: required_text_value(&row, 10, "root_commit_id")?,
            depth: required_integer_value(&row, 11, "depth")?,
            version_id: required_text_value(&row, 12, "version_id")?,
        });
    }
    Ok(rows)
}

fn render_text_in_predicate(column: &str, values: &[String]) -> String {
    if values.len() == 1 {
        return format!("{column} = '{}'", escape_sql_string(&values[0]));
    }
    format!(
        "{column} IN ({})",
        values
            .iter()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn render_where_clause_sql(predicates: &[String], prefix: &str) -> String {
    if predicates.is_empty() {
        String::new()
    } else {
        format!("{prefix}{}", predicates.join(" AND "))
    }
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn required_active_version_id(request: &StateHistoryRequest) -> Result<&str, LixError> {
    request
        .active_version_id
        .as_deref()
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "state history active-version reads require a session-requested version id",
            )
        })
}

fn live_payload_column_name(schema_key: &str, property_name: &str) -> String {
    let layout = builtin_live_table_layout(schema_key)
        .expect("builtin live layout lookup should succeed")
        .expect("builtin live layout should exist");
    live_column_name_for_property(&layout, property_name)
        .unwrap_or_else(|| {
            panic!("builtin live layout '{schema_key}' must include '{property_name}'")
        })
        .to_string()
}

fn required_text_value(row: &[Value], index: usize, field: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.clone()),
        Some(other) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("expected text for {field}, got {other:?}"),
        }),
        None => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("missing column {field} at index {index}"),
        }),
    }
}

fn optional_text_value(
    row: &[Value],
    index: usize,
    field: &str,
) -> Result<Option<String>, LixError> {
    match row.get(index) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Text(value)) => Ok(Some(value.clone())),
        Some(other) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("expected nullable text for {field}, got {other:?}"),
        }),
    }
}

fn required_integer_value(row: &[Value], index: usize, field: &str) -> Result<i64, LixError> {
    match row.get(index) {
        Some(value) => match value {
            Value::Integer(value) => Ok(*value),
            Value::Real(value) => Ok(*value as i64),
            Value::Text(value) => value.parse::<i64>().map_err(|_| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("expected integer for {field}, got {value:?}"),
            }),
            other => Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("expected integer for {field}, got {other:?}"),
            }),
        },
        None => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("missing column {field} at index {index}"),
        }),
    }
}
