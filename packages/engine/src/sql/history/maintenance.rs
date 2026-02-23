use crate::backend::SqlDialect;
use crate::sql::escape_sql_string;
use crate::sql::history::requirements::HistoryRequirements;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, QueryResult, Value};

const TIMELINE_BREAKPOINT_TABLE: &str = "lix_internal_entity_state_timeline_breakpoint";
const TIMELINE_STATUS_TABLE: &str = "lix_internal_timeline_status";

pub(crate) async fn ensure_history_timelines_materialized_for_requirements(
    backend: &dyn LixBackend,
    requirements: &HistoryRequirements,
) -> Result<(), LixError> {
    if requirements.requested_root_commit_ids.is_empty() || requirements.required_max_depth <= 0 {
        return Ok(());
    }

    for root_commit_id in &requirements.requested_root_commit_ids {
        ensure_history_timeline_materialized_for_root(
            backend,
            root_commit_id,
            requirements.required_max_depth,
        )
        .await?;
    }

    Ok(())
}

async fn ensure_history_timeline_materialized_for_root(
    backend: &dyn LixBackend,
    root_commit_id: &str,
    required_depth: i64,
) -> Result<(), LixError> {
    let built_max_depth = load_timeline_built_max_depth(backend, root_commit_id).await?;
    if built_max_depth.is_some_and(|built| built >= required_depth) {
        return Ok(());
    }

    let start_depth = built_max_depth.map_or(0, |built| built.saturating_add(1));
    let query_start = if start_depth > 0 { start_depth - 1 } else { 0 };
    let source_rows = load_phase1_source_rows_for_root_range(
        backend,
        root_commit_id,
        query_start,
        required_depth,
    )
    .await?;
    let breakpoints = derive_breakpoints_from_source_rows(root_commit_id, start_depth, source_rows);
    insert_breakpoints(backend, &breakpoints).await?;
    upsert_timeline_status(backend, root_commit_id, required_depth).await?;

    Ok(())
}

async fn load_timeline_built_max_depth(
    backend: &dyn LixBackend,
    root_commit_id: &str,
) -> Result<Option<i64>, LixError> {
    let sql = format!(
        "SELECT built_max_depth \
         FROM {status_table} \
         WHERE root_commit_id = '{root_commit_id}' \
         LIMIT 1",
        status_table = TIMELINE_STATUS_TABLE,
        root_commit_id = escape_sql_string(root_commit_id),
    );
    let result = backend.execute(&sql, &[]).await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    Ok(row.first().and_then(integer_from_value))
}

async fn load_phase1_source_rows_for_root_range(
    backend: &dyn LixBackend,
    root_commit_id: &str,
    start_depth: i64,
    end_depth: i64,
) -> Result<Vec<TimelineSourceRow>, LixError> {
    if start_depth > end_depth {
        return Ok(Vec::new());
    }

    let sql =
        build_phase1_source_query_sql(backend.dialect(), root_commit_id, start_depth, end_depth);
    let result = backend.execute(&sql, &[]).await?;
    parse_timeline_source_rows(result)
}

fn build_phase1_source_query_sql(
    dialect: SqlDialect,
    root_commit_id: &str,
    start_depth: i64,
    end_depth: i64,
) -> String {
    let change_set_id_sql = json_text_expr_sql(dialect, "snapshot_content", "change_set_id");
    let cse_change_set_id_sql = json_text_expr_sql(dialect, "snapshot_content", "change_set_id");
    let cse_change_id_sql = json_text_expr_sql(dialect, "snapshot_content", "change_id");
    let cse_entity_id_sql = json_text_expr_sql(dialect, "snapshot_content", "entity_id");
    let cse_schema_key_sql = json_text_expr_sql(dialect, "snapshot_content", "schema_key");
    let cse_file_id_sql = json_text_expr_sql(dialect, "snapshot_content", "file_id");
    format!(
        "WITH \
           commit_by_version AS ( \
             SELECT \
               entity_id AS id, \
               {change_set_id_sql} AS change_set_id, \
               version_id AS lixcol_version_id \
             FROM lix_internal_state_materialized_v1_lix_commit \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           change_set_element_by_version AS ( \
             SELECT \
               {cse_change_set_id_sql} AS change_set_id, \
               {cse_change_id_sql} AS change_id, \
               {cse_entity_id_sql} AS entity_id, \
               {cse_schema_key_sql} AS schema_key, \
               {cse_file_id_sql} AS file_id, \
               version_id AS lixcol_version_id \
             FROM lix_internal_state_materialized_v1_lix_change_set_element \
             WHERE schema_key = 'lix_change_set_element' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           all_changes AS ( \
             SELECT \
               ic.id, \
               ic.plugin_key, \
               ic.schema_version, \
               ic.metadata, \
               ic.snapshot_id, \
               ic.created_at \
             FROM lix_internal_change ic \
           ), \
           reachable_commits AS ( \
             SELECT \
               ancestry.ancestor_id AS commit_id, \
               ancestry.depth AS commit_depth \
             FROM lix_internal_commit_ancestry ancestry \
             WHERE ancestry.commit_id = '{root_commit_id}' \
               AND ancestry.depth BETWEEN {start_depth} AND {end_depth} \
           ), \
           commit_changesets AS ( \
             SELECT \
               c.id AS commit_id, \
               c.change_set_id AS change_set_id, \
               rc.commit_depth AS commit_depth \
             FROM commit_by_version c \
             JOIN reachable_commits rc ON c.id = rc.commit_id \
             WHERE c.lixcol_version_id = '{global_version}' \
           ), \
           cse_in_reachable AS ( \
             SELECT \
               cse.entity_id AS entity_id, \
               cse.schema_key AS schema_key, \
               cse.file_id AS file_id, \
               cse.change_id AS change_id, \
               cc.commit_depth AS commit_depth \
             FROM change_set_element_by_version cse \
             JOIN commit_changesets cc \
               ON cse.change_set_id = cc.change_set_id \
             WHERE cse.lixcol_version_id = '{global_version}' \
           ), \
           ranked AS ( \
             SELECT \
               r.entity_id, \
               r.schema_key, \
               r.file_id, \
               changes.plugin_key, \
               changes.schema_version, \
               changes.metadata, \
               changes.snapshot_id, \
               r.change_id, \
               r.commit_depth, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY r.entity_id, r.file_id, r.schema_key, r.commit_depth \
                 ORDER BY changes.created_at DESC, changes.id DESC \
               ) AS rn \
             FROM cse_in_reachable r \
             JOIN all_changes changes ON changes.id = r.change_id \
           ) \
         SELECT \
           ranked.entity_id, \
           ranked.schema_key, \
           ranked.file_id, \
           ranked.plugin_key, \
           ranked.schema_version, \
           ranked.metadata, \
           ranked.snapshot_id, \
           ranked.change_id, \
           ranked.commit_depth \
         FROM ranked \
         WHERE ranked.rn = 1 \
         ORDER BY \
           ranked.entity_id ASC, \
           ranked.file_id ASC, \
           ranked.schema_key ASC, \
           ranked.commit_depth ASC",
        global_version = GLOBAL_VERSION_ID,
        change_set_id_sql = change_set_id_sql,
        cse_change_set_id_sql = cse_change_set_id_sql,
        cse_change_id_sql = cse_change_id_sql,
        cse_entity_id_sql = cse_entity_id_sql,
        cse_schema_key_sql = cse_schema_key_sql,
        cse_file_id_sql = cse_file_id_sql,
        root_commit_id = escape_sql_string(root_commit_id),
        start_depth = start_depth,
        end_depth = end_depth,
    )
}

fn json_text_expr_sql(dialect: SqlDialect, column: &str, field: &str) -> String {
    match dialect {
        SqlDialect::Sqlite => format!("json_extract({column}, '$.\"{field}\"')"),
        SqlDialect::Postgres => {
            format!("jsonb_extract_path_text(CAST({column} AS JSONB), '{field}')")
        }
    }
}

fn parse_timeline_source_rows(result: QueryResult) -> Result<Vec<TimelineSourceRow>, LixError> {
    let mut out = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        let entity_id = required_text_value(&row, 0, "entity_id")?;
        let schema_key = required_text_value(&row, 1, "schema_key")?;
        let file_id = required_text_value(&row, 2, "file_id")?;
        let plugin_key = required_text_value(&row, 3, "plugin_key")?;
        let schema_version = required_text_value(&row, 4, "schema_version")?;
        let metadata = optional_text_value(&row, 5, "metadata")?;
        let snapshot_id = required_text_value(&row, 6, "snapshot_id")?;
        let change_id = required_text_value(&row, 7, "change_id")?;
        let depth = required_integer_value(&row, 8, "commit_depth")?;

        out.push(TimelineSourceRow {
            entity_id,
            schema_key,
            file_id,
            plugin_key,
            schema_version,
            metadata,
            snapshot_id,
            change_id,
            depth,
        });
    }
    Ok(out)
}

fn derive_breakpoints_from_source_rows(
    root_commit_id: &str,
    start_depth: i64,
    source_rows: Vec<TimelineSourceRow>,
) -> Vec<TimelineBreakpointRow> {
    let mut breakpoints = Vec::new();
    let mut current_key: Option<TimelineEntityKey> = None;
    let mut current_signature: Option<TimelineStateSignature> = None;

    for row in source_rows {
        let key = TimelineEntityKey {
            entity_id: row.entity_id.clone(),
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
        };
        let signature = TimelineStateSignature {
            plugin_key: row.plugin_key.clone(),
            schema_version: row.schema_version.clone(),
            metadata: row.metadata.clone(),
            snapshot_id: row.snapshot_id.clone(),
            change_id: row.change_id.clone(),
        };

        if current_key.as_ref() != Some(&key) {
            current_key = Some(key.clone());
            current_signature = None;
        }

        if row.depth < start_depth {
            current_signature = Some(signature);
            continue;
        }

        if current_signature.as_ref() != Some(&signature) {
            breakpoints.push(TimelineBreakpointRow {
                root_commit_id: root_commit_id.to_string(),
                entity_id: key.entity_id,
                schema_key: key.schema_key,
                file_id: key.file_id,
                from_depth: row.depth,
                plugin_key: row.plugin_key,
                schema_version: row.schema_version,
                metadata: row.metadata,
                snapshot_id: row.snapshot_id,
                change_id: row.change_id,
            });
        }

        current_signature = Some(signature);
    }

    breakpoints
}

async fn insert_breakpoints(
    backend: &dyn LixBackend,
    breakpoints: &[TimelineBreakpointRow],
) -> Result<(), LixError> {
    for breakpoint in breakpoints {
        let metadata_sql = breakpoint
            .metadata
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let sql = format!(
            "INSERT INTO {table} (\
               root_commit_id, entity_id, schema_key, file_id, from_depth, \
               plugin_key, schema_version, metadata, snapshot_id, change_id\
             ) VALUES (\
               '{root_commit_id}', '{entity_id}', '{schema_key}', '{file_id}', {from_depth}, \
               '{plugin_key}', '{schema_version}', {metadata_sql}, '{snapshot_id}', '{change_id}'\
             ) \
             ON CONFLICT (root_commit_id, entity_id, schema_key, file_id, from_depth) DO NOTHING",
            table = TIMELINE_BREAKPOINT_TABLE,
            root_commit_id = escape_sql_string(&breakpoint.root_commit_id),
            entity_id = escape_sql_string(&breakpoint.entity_id),
            schema_key = escape_sql_string(&breakpoint.schema_key),
            file_id = escape_sql_string(&breakpoint.file_id),
            from_depth = breakpoint.from_depth,
            plugin_key = escape_sql_string(&breakpoint.plugin_key),
            schema_version = escape_sql_string(&breakpoint.schema_version),
            metadata_sql = metadata_sql,
            snapshot_id = escape_sql_string(&breakpoint.snapshot_id),
            change_id = escape_sql_string(&breakpoint.change_id),
        );
        backend.execute(&sql, &[]).await?;
    }
    Ok(())
}

async fn upsert_timeline_status(
    backend: &dyn LixBackend,
    root_commit_id: &str,
    built_max_depth: i64,
) -> Result<(), LixError> {
    let sql = format!(
        "INSERT INTO {table} (root_commit_id, built_max_depth, built_at) \
         VALUES ('{root_commit_id}', {built_max_depth}, CURRENT_TIMESTAMP) \
         ON CONFLICT (root_commit_id) DO UPDATE \
         SET built_max_depth = CASE \
               WHEN excluded.built_max_depth > {table}.built_max_depth THEN excluded.built_max_depth \
               ELSE {table}.built_max_depth \
             END, \
             built_at = CASE \
               WHEN excluded.built_max_depth > {table}.built_max_depth THEN excluded.built_at \
               ELSE {table}.built_at \
             END",
        table = TIMELINE_STATUS_TABLE,
        root_commit_id = escape_sql_string(root_commit_id),
        built_max_depth = built_max_depth,
    );
    backend.execute(&sql, &[]).await?;
    Ok(())
}

#[derive(Clone, PartialEq, Eq)]
struct TimelineEntityKey {
    entity_id: String,
    schema_key: String,
    file_id: String,
}

#[derive(Clone)]
struct TimelineSourceRow {
    entity_id: String,
    schema_key: String,
    file_id: String,
    plugin_key: String,
    schema_version: String,
    metadata: Option<String>,
    snapshot_id: String,
    change_id: String,
    depth: i64,
}

#[derive(Clone, PartialEq, Eq)]
struct TimelineStateSignature {
    plugin_key: String,
    schema_version: String,
    metadata: Option<String>,
    snapshot_id: String,
    change_id: String,
}

struct TimelineBreakpointRow {
    root_commit_id: String,
    entity_id: String,
    schema_key: String,
    file_id: String,
    from_depth: i64,
    plugin_key: String,
    schema_version: String,
    metadata: Option<String>,
    snapshot_id: String,
    change_id: String,
}

fn required_text_value(row: &[Value], index: usize, field: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.clone()),
        Some(other) => Err(LixError {
            message: format!("expected text for {field}, got {other:?}"),
        }),
        None => Err(LixError {
            message: format!("missing column {field} at index {index}"),
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
            message: format!("expected nullable text for {field}, got {other:?}"),
        }),
    }
}

fn required_integer_value(row: &[Value], index: usize, field: &str) -> Result<i64, LixError> {
    match row.get(index) {
        Some(value) => integer_from_value(value).ok_or_else(|| LixError {
            message: format!("expected integer for {field}, got {value:?}"),
        }),
        None => Err(LixError {
            message: format!("missing column {field} at index {index}"),
        }),
    }
}

fn integer_from_value(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(value) => Some(*value),
        Value::Real(value) => Some(*value as i64),
        Value::Text(value) => value.parse::<i64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::ensure_history_timelines_materialized_for_requirements;
    use crate::backend::{LixBackend, LixTransaction, SqlDialect};
    use crate::sql::history::requirements::HistoryRequirements;
    use crate::{LixError, QueryResult, Value};
    use async_trait::async_trait;
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    struct MaintenanceTestBackend {
        executed: Mutex<Vec<String>>,
    }

    struct MaintenanceTestTransaction;

    #[async_trait(?Send)]
    impl LixBackend for MaintenanceTestBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.executed.lock().expect("lock").push(sql.to_string());
            Ok(QueryResult { rows: Vec::new() })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Ok(Box::new(MaintenanceTestTransaction))
        }
    }

    #[async_trait(?Send)]
    impl LixTransaction for MaintenanceTestTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(
            &mut self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<QueryResult, LixError> {
            Ok(QueryResult { rows: Vec::new() })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_op_when_requirements_have_no_roots() {
        let backend = MaintenanceTestBackend {
            executed: Mutex::new(Vec::new()),
        };

        ensure_history_timelines_materialized_for_requirements(
            &backend,
            &HistoryRequirements::default(),
        )
        .await
        .expect("no-op should succeed");

        assert!(backend.executed.lock().expect("lock").is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn materializes_requested_root_up_to_required_depth() {
        let backend = MaintenanceTestBackend {
            executed: Mutex::new(Vec::new()),
        };
        let mut roots = BTreeSet::new();
        roots.insert("root-a".to_string());
        let requirements = HistoryRequirements {
            requested_root_commit_ids: roots,
            required_max_depth: 3,
            requires_file_history_data_materialization: false,
        };

        ensure_history_timelines_materialized_for_requirements(&backend, &requirements)
            .await
            .expect("materialization should succeed");

        let executed = backend.executed.lock().expect("lock");
        assert!(executed
            .iter()
            .any(|sql| sql.contains("SELECT built_max_depth")
                && sql.contains("lix_internal_timeline_status")));
        assert!(executed.iter().any(|sql| {
            sql.contains("ancestry.depth BETWEEN 0 AND 3")
                && sql.contains("WHERE ancestry.commit_id = 'root-a'")
        }));
        assert!(executed
            .iter()
            .any(|sql| sql.contains("INSERT INTO lix_internal_timeline_status")));
    }
}
