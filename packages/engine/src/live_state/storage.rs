#![allow(dead_code)]

//! SQL-backed adapter slot for live-state persistence.
//!
//! This module is the intended home for `LiveStateReadStore` and
//! `LiveStateWriteStore` implementations that still rely on `LixBackend`,
//! `LixBackendTransaction`, or lower `backend/*` helpers during the MVP.

pub(crate) mod layout {
    #[allow(unused_imports)]
    pub(crate) use crate::live_state::table_storage::live_table_layout_from_schema;
}

pub(crate) mod sql {
    pub(crate) use crate::live_state::table_storage::sql::*;
}

use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};

use crate::backend::QueryExecutor;
use crate::backend::{add_column_if_missing, add_column_if_missing_with_executor};
use crate::common::{escape_sql_string, storage_scope_key_for_file_id, STORAGE_SCOPE_KEY_COLUMN};
use crate::live_state::lifecycle::{
    build_mark_live_state_ready_sql, build_mark_live_state_ready_without_cursor_sql,
    build_set_live_state_mode_sql, build_set_live_state_mode_with_cursor_and_frontier_sql,
    default_live_state_status, parse_latest_replay_cursor, parse_nullable_live_state_status_result,
    LiveStateSnapshot, LIVE_STATE_SCHEMA_EPOCH, LIVE_STATE_STATUS_CREATE_TABLE_SQL,
    LIVE_STATE_STATUS_SEED_ROW_SQL, LIVE_STATE_STATUS_TABLE,
};
use crate::live_state::store::{
    LiveStateBackendRef, LiveStateFrontierReadStore, LiveStateLifecycleAdminStore,
    LiveStateLifecycleReadStore, LiveStateLifecycleWriteStore, LiveStateMaterializeStore,
    LiveStateReadStore, LiveStateWriteStore,
};
use crate::live_state::untracked::load_exact_row_with_executor as load_exact_untracked_row_with_executor;
use crate::live_state::{
    ExactUntrackedRowRequest, LiveRow, LiveStateMode, ReplayCursor, SchemaRegistration,
};
use crate::schema::schema_from_registered_snapshot;
use crate::streams::{
    delete_durable_state_commit_consumer_cursor_in_transaction,
    load_latest_untracked_visibility_append_seq,
    load_latest_untracked_visibility_append_seq_in_transaction,
    upsert_durable_state_commit_consumer_cursor_in_transaction,
    upsert_durable_state_commit_consumer_cursor_with_backend, DurableStateCommitCursor,
    LIVE_STATE_DURABLE_CONSUMER_KEY,
};
use crate::version::CommittedVersionFrontier;
use crate::version::{
    version_ref_schema_key, version_ref_schema_version, version_ref_storage_version_id,
};
use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, SqlDialect, Value};

use crate::live_state::naming::tracked_relation_name;
use crate::live_state::row_queries;
pub(crate) use crate::live_state::table_storage::{
    build_partitioned_scan_sql, ensure_schema_live_table_sql_statements,
    normalized_insert_columns_sql, normalized_insert_values_sql, normalized_live_column_values,
    normalized_update_assignments_sql, quoted_live_table_name, required_bool_cell,
    required_text_cell, selected_columns, selected_projection_sql, text_from_value,
    tracked_live_table_name, ScanSqlRequest,
};
pub(crate) use crate::live_state::table_storage::{
    builtin_live_table_layout, compile_registered_live_layout, json_value_from_live_row_cell,
    live_table_layout_from_schema, load_live_row_access_for_table_name,
    load_live_row_access_with_backend, load_live_row_access_with_executor,
    load_live_table_layout_with_executor, logical_snapshot_from_projected_row, LiveColumnKind,
    LiveColumnSpec, LiveRowAccess, LiveTableLayout,
};

pub(crate) enum SqlLiveStateAccess<'a> {
    Backend(&'a dyn LixBackend),
    Transaction(&'a mut dyn LixBackendTransaction),
    Executor(&'a mut dyn QueryExecutor),
}

/// Thin owner-local SQL adapter with multiple narrow live-state capabilities.
pub(crate) struct SqlLiveStateStore<'a> {
    access: SqlLiveStateAccess<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveTableRequirement {
    schema_key: String,
    layout: Option<LiveTableLayout>,
}

impl<'a> SqlLiveStateStore<'a> {
    pub(crate) fn from_backend(backend: &'a dyn LixBackend) -> Self {
        Self {
            access: SqlLiveStateAccess::Backend(backend),
        }
    }

    pub(crate) fn from_transaction(transaction: &'a mut dyn LixBackendTransaction) -> Self {
        Self {
            access: SqlLiveStateAccess::Transaction(transaction),
        }
    }

    pub(crate) fn from_executor(executor: &'a mut dyn QueryExecutor) -> Self {
        Self {
            access: SqlLiveStateAccess::Executor(executor),
        }
    }

    fn backend(&self) -> Result<&'a dyn LixBackend, LixError> {
        match &self.access {
            SqlLiveStateAccess::Backend(backend) => Ok(*backend),
            _ => Err(invalid_live_state_store_access(
                "backend-backed live-state store",
            )),
        }
    }

    fn transaction(&mut self) -> Result<&mut dyn LixBackendTransaction, LixError> {
        match &mut self.access {
            SqlLiveStateAccess::Transaction(transaction) => Ok(*transaction),
            _ => Err(invalid_live_state_store_access(
                "transaction-backed live-state store",
            )),
        }
    }
}

async fn execute_query_with_backend(
    backend: &dyn LixBackend,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    backend.execute(sql, params).await
}

async fn execute_query_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    transaction.execute(sql, params).await
}

async fn execute_query_with_executor(
    executor: &mut dyn QueryExecutor,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    executor.execute(sql, params).await
}

pub(crate) fn transaction_executor_view(
    transaction: &mut dyn LixBackendTransaction,
) -> impl QueryExecutor + '_ {
    crate::backend::transaction_backend_view(transaction)
}

async fn execute_ddl_batch_with_backend(
    backend: LiveStateBackendRef<'_>,
    batch_name: &str,
    statements: &[&str],
) -> Result<(), LixError> {
    crate::backend::execute_ddl_batch(backend, batch_name, statements).await
}

async fn add_column_if_missing_with_backend(
    backend: LiveStateBackendRef<'_>,
    table_name: &str,
    column_name: &str,
    column_sql: &str,
) -> Result<(), LixError> {
    add_column_if_missing(backend, table_name, column_name, column_sql).await
}

pub(crate) async fn init_storage(backend: LiveStateBackendRef<'_>) -> Result<(), LixError> {
    const LIVE_STATE_CREATE_TABLE_STATEMENTS: &[&str] = &[
        "CREATE TABLE IF NOT EXISTS lix_internal_registered_schema_bootstrap (\
         entity_id TEXT NOT NULL,\
         schema_key TEXT NOT NULL,\
         schema_version TEXT NOT NULL,\
         file_id TEXT,\
         storage_scope_key TEXT NOT NULL,\
         version_id TEXT NOT NULL,\
         global BOOLEAN NOT NULL DEFAULT false,\
         plugin_key TEXT,\
         snapshot_content TEXT,\
         change_id TEXT NOT NULL,\
         metadata TEXT,\
         is_tombstone INTEGER NOT NULL DEFAULT 0,\
         untracked BOOLEAN NOT NULL DEFAULT false,\
         created_at TEXT NOT NULL,\
         updated_at TEXT NOT NULL,\
         PRIMARY KEY (entity_id, storage_scope_key, version_id, untracked)\
         )",
        "CREATE TABLE IF NOT EXISTS lix_internal_file_data_cache (\
         file_id TEXT NOT NULL,\
         version_id TEXT NOT NULL,\
         data BYTEA NOT NULL,\
         PRIMARY KEY (file_id, version_id)\
         )",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_file_data_cache_version_id \
         ON lix_internal_file_data_cache (version_id)",
        "CREATE TABLE IF NOT EXISTS lix_internal_file_path_cache (\
         file_id TEXT NOT NULL,\
         version_id TEXT NOT NULL,\
         directory_id TEXT,\
         name TEXT NOT NULL,\
         extension TEXT,\
         path TEXT NOT NULL,\
         PRIMARY KEY (file_id, version_id)\
         )",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_file_path_cache_version_path \
         ON lix_internal_file_path_cache (version_id, path, file_id)",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_file_path_cache_version_directory \
         ON lix_internal_file_path_cache (version_id, directory_id)",
        "CREATE TABLE IF NOT EXISTS lix_internal_file_lixcol_cache (\
         file_id TEXT NOT NULL,\
         version_id TEXT NOT NULL,\
         latest_change_id TEXT,\
         latest_commit_id TEXT,\
         created_at TEXT,\
         updated_at TEXT,\
         PRIMARY KEY (file_id, version_id)\
         )",
    ];
    const LIVE_STATE_INDEX_STATEMENTS: &[&str] = &[
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_lix_internal_registered_schema_bootstrap_scope_identity \
         ON lix_internal_registered_schema_bootstrap (entity_id, storage_scope_key, version_id, untracked)",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_version_id \
         ON lix_internal_registered_schema_bootstrap (version_id)",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_global_version \
         ON lix_internal_registered_schema_bootstrap (global, version_id)",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_vfe \
         ON lix_internal_registered_schema_bootstrap (version_id, file_id, entity_id)",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_vse \
         ON lix_internal_registered_schema_bootstrap (version_id, storage_scope_key, entity_id)",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_live_vfe \
         ON lix_internal_registered_schema_bootstrap (version_id, file_id, entity_id) \
         WHERE is_tombstone = 0 AND snapshot_content IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_registered_schema_bootstrap_live_vse \
         ON lix_internal_registered_schema_bootstrap (version_id, storage_scope_key, entity_id) \
         WHERE is_tombstone = 0 AND snapshot_content IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_file_lixcol_cache_lookup \
         ON lix_internal_file_lixcol_cache (file_id, version_id)",
    ];

    execute_ddl_batch_with_backend(
        backend,
        "live_state.tables",
        LIVE_STATE_CREATE_TABLE_STATEMENTS,
    )
    .await?;
    add_column_if_missing_with_backend(
        backend,
        "lix_internal_registered_schema_bootstrap",
        STORAGE_SCOPE_KEY_COLUMN,
        &format!(
            "TEXT NOT NULL DEFAULT '{}'",
            storage_scope_key_for_file_id(None)
        ),
    )
    .await?;
    execute_query_with_backend(
        backend,
        &format!(
            "UPDATE lix_internal_registered_schema_bootstrap \
             SET {storage_scope_key} = CASE \
               WHEN file_id IS NULL THEN '{engine_scope}' \
               ELSE 'file:' || file_id \
             END",
            storage_scope_key = STORAGE_SCOPE_KEY_COLUMN,
            engine_scope = storage_scope_key_for_file_id(None),
        ),
        &[],
    )
    .await?;
    execute_ddl_batch_with_backend(backend, "live_state.indexes", LIVE_STATE_INDEX_STATEMENTS).await
}

pub(crate) async fn load_change_commit_id_map(
    backend: LiveStateBackendRef<'_>,
    change_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, String>, LixError> {
    if change_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let in_list = change_ids
        .iter()
        .map(|change_id| format!("'{}'", escape_sql_string(change_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "WITH {change_commit_cte} \
         SELECT change_id, commit_id \
         FROM change_commit_by_change_id \
         WHERE change_id IN ({in_list})",
        change_commit_cte =
            crate::canonical::build_lazy_change_commit_by_change_id_ctes_sql(backend.dialect()),
        in_list = in_list,
    );
    let result = execute_query_with_backend(backend, &sql, &[]).await?;
    let mut rows = BTreeMap::new();
    for row in result.rows {
        let Some(Value::Text(change_id)) = row.first() else {
            continue;
        };
        let Some(Value::Text(commit_id)) = row.get(1) else {
            continue;
        };
        rows.insert(change_id.clone(), commit_id.clone());
    }
    Ok(rows)
}

pub(crate) async fn load_registered_schema_layout_rows_with_backend(
    backend: LiveStateBackendRef<'_>,
) -> Result<Vec<Vec<Value>>, LixError> {
    let result = execute_query_with_backend(
        backend,
        "SELECT snapshot_content \
         FROM lix_internal_registered_schema_bootstrap \
         WHERE schema_key = 'lix_registered_schema' \
           AND version_id = 'global' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL",
        &[],
    )
    .await?;
    Ok(result.rows)
}

pub(crate) async fn load_registered_schema_layout_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<Vec<Vec<Value>>, LixError> {
    let result = execute_query_with_transaction(
        transaction,
        "SELECT snapshot_content \
         FROM lix_internal_registered_schema_bootstrap \
         WHERE schema_key = 'lix_registered_schema' \
           AND version_id = 'global' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL",
        &[],
    )
    .await?;
    Ok(result.rows)
}

pub(crate) async fn load_registered_schema_layout_rows_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<Vec<Value>>, LixError> {
    let result = execute_query_with_executor(
        executor,
        "SELECT snapshot_content \
         FROM lix_internal_registered_schema_bootstrap \
         WHERE schema_key = 'lix_registered_schema' \
           AND version_id = 'global' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL",
        &[],
    )
    .await?;
    Ok(result.rows)
}

pub(crate) async fn load_registered_schema_live_table_layout_rows(
    backend: LiveStateBackendRef<'_>,
) -> Result<Vec<Vec<Value>>, LixError> {
    let registered_schema_table = tracked_relation_name("lix_registered_schema");
    let snapshot_expr = crate::live_state::schema_access::snapshot_select_expr_for_schema(
        "lix_registered_schema",
        None,
        backend.dialect(),
        Some("m"),
    )?;
    let sql = format!(
        "SELECT {snapshot_expr} AS snapshot_content \
         FROM {registered_schema_table} m \
         WHERE m.schema_key = 'lix_registered_schema' \
           AND m.version_id = 'global' \
           AND m.is_tombstone = 0",
        snapshot_expr = snapshot_expr,
        registered_schema_table = registered_schema_table,
    );
    Ok(execute_query_with_backend(backend, &sql, &[]).await?.rows)
}

pub(crate) async fn load_plugin_archive_ref_rows(
    backend: LiveStateBackendRef<'_>,
) -> Result<Vec<Vec<Value>>, LixError> {
    let rows = execute_query_with_backend(
        backend,
        &format!(
            "SELECT binary_ref.file_id, binary_ref.version_id, path_cache.path, binary_ref.blob_hash \
             FROM {binary_file_version_ref} AS binary_ref \
             INNER JOIN {file_path_cache} AS path_cache \
                 ON path_cache.file_id = binary_ref.file_id \
                AND path_cache.version_id = binary_ref.version_id \
             WHERE binary_ref.version_id = 'global' \
               AND path_cache.path LIKE '/.lix/plugins/%.lixplugin' \
               AND path_cache.path NOT LIKE '/.lix/plugins/%/%' \
             ORDER BY path_cache.path",
            binary_file_version_ref = crate::binary_cas::binary_file_version_ref_relation_name(),
            file_path_cache = crate::live_state::FILE_PATH_CACHE_TABLE,
        ),
        &[],
    )
    .await?;
    Ok(rows.rows)
}

pub(crate) async fn live_storage_relation_exists(
    backend: LiveStateBackendRef<'_>,
    relation_name: &str,
) -> Result<bool, LixError> {
    let result = match backend.dialect() {
        SqlDialect::Sqlite => {
            execute_query_with_backend(
                backend,
                "SELECT 1 \
                 FROM sqlite_master \
                 WHERE name = $1 \
                   AND type IN ('table', 'view') \
                 LIMIT 1",
                &[Value::Text(relation_name.to_string())],
            )
            .await?
        }
        SqlDialect::Postgres => {
            execute_query_with_backend(
                backend,
                "SELECT 1 \
                 FROM information_schema.tables \
                 WHERE table_name = $1 \
                 LIMIT 1",
                &[Value::Text(relation_name.to_string())],
            )
            .await?
        }
    };
    Ok(!result.rows.is_empty())
}

pub(crate) async fn scan_live_partition_with_executor(
    executor: &mut dyn QueryExecutor,
    request: ScanSqlRequest<'_>,
) -> Result<QueryResult, LixError> {
    let sql = build_partitioned_scan_sql(request)?;
    execute_query_with_executor(executor, &sql, &[]).await
}

pub(crate) async fn load_registered_schema_bootstrap_layout_rows_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<Vec<Value>>, LixError> {
    let result = execute_query_with_executor(
        executor,
        "SELECT snapshot_content \
         FROM lix_internal_registered_schema_bootstrap \
         WHERE schema_key = 'lix_registered_schema' \
           AND version_id = 'global' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL",
        &[],
    )
    .await?;
    Ok(result.rows)
}

pub(crate) async fn load_materialization_change_rows_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<Vec<Value>>, LixError> {
    let result = execute_query_with_executor(
        executor,
        "SELECT c.id, c.entity_id, c.schema_key, c.schema_version, c.file_id, c.plugin_key, s.content AS snapshot_content, c.metadata, c.created_at \
         FROM lix_internal_change c \
         LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id",
        &[],
    )
    .await?;
    Ok(result.rows)
}

pub(crate) async fn load_untracked_visibility_rows_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<Vec<Value>>, LixError> {
    let result = execute_query_with_executor(
        executor,
        "SELECT id, change_id, version_id, visibility_kind, entity_id, schema_key, file_id, created_at, append_seq \
         FROM lix_internal_untracked_change_visibility",
        &[],
    )
    .await?;
    Ok(result.rows)
}

pub(crate) async fn set_live_state_mode_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    mode: LiveStateMode,
) -> Result<(), LixError> {
    execute_query_with_transaction(transaction, &build_set_live_state_mode_sql(mode), &[]).await?;
    Ok(())
}

pub(crate) async fn upsert_live_state_rebuild_row_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    write: &crate::live_state::LiveStateWrite,
) -> Result<(), LixError> {
    let is_tombstone = match write.op {
        crate::live_state::LiveStateWriteOp::Upsert => 0,
        crate::live_state::LiveStateWriteOp::Tombstone => 1,
    };
    let global_sql = if write.global { "true" } else { "false" };
    let untracked_sql = if write.untracked { "true" } else { "false" };
    let storage_scope_key = crate::live_state::constraints::escape_sql_string(
        &storage_scope_key_for_file_id(write.file_id.as_deref()),
    );
    let file_id_sql = write
        .file_id
        .as_ref()
        .map(|value| {
            format!(
                "'{}'",
                crate::live_state::constraints::escape_sql_string(value.as_str())
            )
        })
        .unwrap_or_else(|| "NULL".to_string());
    let plugin_key_sql = write
        .plugin_key
        .as_ref()
        .map(|value| {
            format!(
                "'{}'",
                crate::live_state::constraints::escape_sql_string(value.as_str())
            )
        })
        .unwrap_or_else(|| "NULL".to_string());
    let metadata_sql = write
        .metadata
        .as_ref()
        .map(|value| {
            format!(
                "'{}'",
                crate::live_state::constraints::escape_sql_string(value.as_str())
            )
        })
        .unwrap_or_else(|| "NULL".to_string());
    let mut executor = crate::backend::transaction_backend_view(transaction);
    let layout = load_live_table_layout_with_executor(&mut executor, &write.schema_key).await?;
    let normalized_values =
        normalized_live_column_values(&layout, write.snapshot_content.as_deref())?
            .into_iter()
            .collect::<Vec<_>>();
    let sql = format!(
        "INSERT INTO {table} (\
         entity_id, schema_key, schema_version, file_id, storage_scope_key, version_id, global, plugin_key, change_id, metadata, is_tombstone, untracked, created_at, updated_at{normalized_columns}\
         ) VALUES (\
         '{entity_id}', '{schema_key}', '{schema_version}', {file_id}, '{storage_scope_key}', '{version_id}', {global}, {plugin_key}, '{change_id}', {metadata}, {is_tombstone}, {untracked}, '{created_at}', '{updated_at}'{normalized_values}\
         ) ON CONFLICT (entity_id, storage_scope_key, version_id, untracked) DO UPDATE SET \
         schema_key = excluded.schema_key, \
         schema_version = excluded.schema_version, \
         file_id = excluded.file_id, \
         global = excluded.global, \
         plugin_key = excluded.plugin_key, \
         change_id = excluded.change_id, \
         metadata = excluded.metadata, \
         is_tombstone = excluded.is_tombstone, \
         created_at = excluded.created_at, \
         updated_at = excluded.updated_at{normalized_updates}",
        table = quoted_live_table_name(&write.schema_key),
        entity_id = crate::live_state::constraints::escape_sql_string(&write.entity_id),
        schema_key = crate::live_state::constraints::escape_sql_string(&write.schema_key),
        schema_version =
            crate::live_state::constraints::escape_sql_string(&write.schema_version),
        file_id = file_id_sql,
        storage_scope_key = storage_scope_key,
        version_id = crate::live_state::constraints::escape_sql_string(&write.version_id),
        global = global_sql,
        plugin_key = plugin_key_sql,
        change_id = crate::live_state::constraints::escape_sql_string(&write.change_id),
        metadata = metadata_sql,
        is_tombstone = is_tombstone,
        untracked = untracked_sql,
        created_at = crate::live_state::constraints::escape_sql_string(&write.created_at),
        updated_at = crate::live_state::constraints::escape_sql_string(&write.updated_at),
        normalized_columns = normalized_insert_columns_sql(&normalized_values),
        normalized_values = normalized_insert_values_sql(&normalized_values),
        normalized_updates = normalized_update_assignments_sql(&normalized_values),
    );
    execute_query_with_transaction(transaction, &sql, &[]).await?;
    Ok(())
}

pub(crate) async fn count_live_scope_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    table_name: &str,
    version_filter: Option<&str>,
    lane_predicate: &str,
) -> Result<usize, LixError> {
    let sql = if let Some(in_list) = version_filter {
        format!(
            "SELECT COUNT(*) FROM {table_name} \
             WHERE version_id IN ({in_list}){lane_predicate}",
            table_name = table_name,
            in_list = in_list,
            lane_predicate = lane_predicate,
        )
    } else {
        format!(
            "SELECT COUNT(*) FROM {table_name} WHERE 1 = 1{lane_predicate}",
            table_name = table_name,
            lane_predicate = lane_predicate,
        )
    };
    let rows = execute_query_with_transaction(transaction, &sql, &[])
        .await?
        .rows;
    let Some(value) = rows.first().and_then(|row| row.first()) else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "materialization apply: count query returned no rows",
        ));
    };
    match value {
        Value::Integer(count) if *count >= 0 => Ok(*count as usize),
        Value::Text(text) => text.parse::<usize>().map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "materialization apply: invalid count text '{}': {}",
                    text, error
                ),
            )
        }),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "materialization apply: count query returned non-integer value",
        )),
    }
}

pub(crate) async fn delete_live_scope_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    table_name: &str,
    version_filter: Option<&str>,
    lane_predicate: &str,
) -> Result<(), LixError> {
    let sql = if let Some(in_list) = version_filter {
        format!(
            "DELETE FROM {table_name} \
             WHERE version_id IN ({in_list}){lane_predicate}",
            table_name = table_name,
            in_list = in_list,
            lane_predicate = lane_predicate,
        )
    } else {
        format!(
            "DELETE FROM {table_name} WHERE 1 = 1{lane_predicate}",
            table_name = table_name,
            lane_predicate = lane_predicate,
        )
    };
    execute_query_with_transaction(transaction, &sql, &[]).await?;
    Ok(())
}

pub(crate) async fn upsert_tracked_live_row_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    row: &crate::live_state::LiveWriteRow,
    snapshot_content: Option<&str>,
    is_tombstone: bool,
) -> Result<(), LixError> {
    let mut executor = crate::backend::transaction_backend_view(transaction);
    let layout = load_live_table_layout_with_executor(&mut executor, &row.schema_key).await?;
    let normalized_values = normalized_live_column_values(&layout, snapshot_content)?
        .into_iter()
        .collect::<Vec<_>>();
    let created_at = row.created_at.as_deref().unwrap_or(&row.updated_at);
    let metadata_sql = row
        .metadata
        .as_deref()
        .map(crate::live_state::constraints::sql_literal_text)
        .unwrap_or_else(|| "NULL".to_string());
    let file_id_sql = row
        .file_id
        .as_deref()
        .map(crate::live_state::constraints::sql_literal_text)
        .unwrap_or_else(|| "NULL".to_string());
    let storage_scope_key_sql = crate::live_state::constraints::sql_literal_text(
        &storage_scope_key_for_file_id(row.file_id.as_deref()),
    );
    let plugin_key_sql = row
        .plugin_key
        .as_deref()
        .map(crate::live_state::constraints::sql_literal_text)
        .unwrap_or_else(|| "NULL".to_string());
    let sql = format!(
        "INSERT INTO {table} (\
         entity_id, schema_key, schema_version, file_id, storage_scope_key, version_id, global, plugin_key, change_id, metadata, is_tombstone, created_at, updated_at{normalized_columns}\
         ) VALUES (\
         '{entity_id}', '{schema_key}', '{schema_version}', {file_id}, {storage_scope_key}, '{version_id}', {global}, {plugin_key}, '{change_id}', {metadata}, {is_tombstone}, '{created_at}', '{updated_at}'{normalized_values}\
         ) ON CONFLICT (entity_id, storage_scope_key, version_id, untracked) DO UPDATE SET \
         schema_key = excluded.schema_key, \
         schema_version = excluded.schema_version, \
         file_id = excluded.file_id, \
         global = excluded.global, \
         plugin_key = excluded.plugin_key, \
         change_id = excluded.change_id, \
         metadata = excluded.metadata, \
         is_tombstone = excluded.is_tombstone, \
         created_at = excluded.created_at, \
         updated_at = excluded.updated_at{normalized_updates}",
        table = quoted_live_table_name(&row.schema_key),
        entity_id = crate::live_state::constraints::escape_sql_string(&row.entity_id),
        schema_key = crate::live_state::constraints::escape_sql_string(&row.schema_key),
        schema_version = crate::live_state::constraints::escape_sql_string(&row.schema_version),
        version_id = crate::live_state::constraints::escape_sql_string(&row.version_id),
        global = if row.global { "true" } else { "false" },
        change_id = crate::live_state::constraints::escape_sql_string(&row.change_id),
        metadata = metadata_sql,
        is_tombstone = if is_tombstone { "1" } else { "0" },
        created_at = crate::live_state::constraints::escape_sql_string(created_at),
        updated_at = crate::live_state::constraints::escape_sql_string(&row.updated_at),
        file_id = file_id_sql,
        storage_scope_key = storage_scope_key_sql,
        plugin_key = plugin_key_sql,
        normalized_columns = normalized_insert_columns_sql(&normalized_values),
        normalized_values = normalized_insert_values_sql(&normalized_values),
        normalized_updates = normalized_update_assignments_sql(&normalized_values),
    );
    execute_query_with_transaction(transaction, &sql, &[]).await?;
    Ok(())
}

pub(crate) async fn upsert_untracked_live_row_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    row: &crate::live_state::LiveWriteRow,
) -> Result<(), LixError> {
    let mut executor = crate::backend::transaction_backend_view(transaction);
    let layout = load_live_table_layout_with_executor(&mut executor, &row.schema_key).await?;
    let normalized_values =
        normalized_live_column_values(&layout, row.snapshot_content.as_deref())?
            .into_iter()
            .collect::<Vec<_>>();
    let created_at = row.created_at.as_deref().unwrap_or(&row.updated_at);
    let metadata_sql = row
        .metadata
        .as_deref()
        .map(crate::live_state::constraints::sql_literal_text)
        .unwrap_or_else(|| "NULL".to_string());
    let change_id_sql = crate::live_state::constraints::sql_literal_text(&row.change_id);
    let file_id_sql = row
        .file_id
        .as_deref()
        .map(crate::live_state::constraints::sql_literal_text)
        .unwrap_or_else(|| "NULL".to_string());
    let storage_scope_key_sql = crate::live_state::constraints::sql_literal_text(
        &storage_scope_key_for_file_id(row.file_id.as_deref()),
    );
    let plugin_key_sql = row
        .plugin_key
        .as_deref()
        .map(crate::live_state::constraints::sql_literal_text)
        .unwrap_or_else(|| "NULL".to_string());
    let sql = format!(
        "INSERT INTO {table} (\
         entity_id, schema_key, schema_version, file_id, storage_scope_key, version_id, global, plugin_key, change_id, metadata, untracked, created_at, updated_at{normalized_columns}\
         ) VALUES (\
         '{entity_id}', '{schema_key}', '{schema_version}', {file_id}, {storage_scope_key}, '{version_id}', {global}, {plugin_key}, {change_id}, {metadata}, true, '{created_at}', '{updated_at}'{normalized_values}\
         ) ON CONFLICT (entity_id, storage_scope_key, version_id, untracked) DO UPDATE SET \
         schema_key = excluded.schema_key, \
         schema_version = excluded.schema_version, \
         file_id = excluded.file_id, \
         global = excluded.global, \
         plugin_key = excluded.plugin_key, \
         change_id = excluded.change_id, \
         metadata = excluded.metadata, \
         updated_at = excluded.updated_at{normalized_updates}",
        table = quoted_live_table_name(&row.schema_key),
        entity_id = crate::live_state::constraints::escape_sql_string(&row.entity_id),
        schema_key = crate::live_state::constraints::escape_sql_string(&row.schema_key),
        schema_version = crate::live_state::constraints::escape_sql_string(&row.schema_version),
        version_id = crate::live_state::constraints::escape_sql_string(&row.version_id),
        global = if row.global { "true" } else { "false" },
        change_id = change_id_sql,
        metadata = metadata_sql,
        created_at = crate::live_state::constraints::escape_sql_string(created_at),
        updated_at = crate::live_state::constraints::escape_sql_string(&row.updated_at),
        file_id = file_id_sql,
        storage_scope_key = storage_scope_key_sql,
        plugin_key = plugin_key_sql,
        normalized_columns = normalized_insert_columns_sql(&normalized_values),
        normalized_values = normalized_insert_values_sql(&normalized_values),
        normalized_updates = normalized_update_assignments_sql(&normalized_values),
    );
    execute_query_with_transaction(transaction, &sql, &[]).await?;
    Ok(())
}

pub(crate) async fn delete_untracked_live_row_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    row: &crate::live_state::LiveWriteRow,
) -> Result<(), LixError> {
    let sql = format!(
        "DELETE FROM {table} \
         WHERE entity_id = '{entity_id}' \
           AND {file_id_predicate} \
           AND version_id = '{version_id}' \
           AND untracked = true",
        table = quoted_live_table_name(&row.schema_key),
        entity_id = crate::live_state::constraints::escape_sql_string(&row.entity_id),
        file_id_predicate = row
            .file_id
            .as_deref()
            .map(|file_id| format!(
                "file_id = {}",
                crate::live_state::constraints::sql_literal_text(file_id)
            ))
            .unwrap_or_else(|| "file_id IS NULL".to_string()),
        version_id = crate::live_state::constraints::escape_sql_string(&row.version_id),
    );
    execute_query_with_transaction(transaction, &sql, &[]).await?;
    Ok(())
}

pub(crate) async fn load_file_payload_cache_data(
    backend: LiveStateBackendRef<'_>,
    file_id: &str,
    version_id: &str,
) -> Result<Vec<u8>, LixError> {
    let result = execute_query_with_backend(
        backend,
        "SELECT data \
         FROM lix_internal_file_data_cache \
         WHERE file_id = $1 AND version_id = $2 \
         LIMIT 1",
        &[
            Value::Text(file_id.to_string()),
            Value::Text(version_id.to_string()),
        ],
    )
    .await?;
    let Some(row) = result.rows.first() else {
        return Ok(Vec::new());
    };
    match row.first() {
        Some(Value::Blob(bytes)) => Ok(bytes.clone()),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!(
                "plugin materialization: expected blob column 'data' at index 0, got {other:?}"
            ),
        )),
        None => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "plugin materialization: row missing column 'data' at index 0",
        )),
    }
}

pub(crate) async fn upsert_file_payload_cache_data(
    backend: LiveStateBackendRef<'_>,
    file_id: &str,
    version_id: &str,
    data: &[u8],
) -> Result<(), LixError> {
    execute_query_with_backend(
        backend,
        "INSERT INTO lix_internal_file_data_cache (file_id, version_id, data) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (file_id, version_id) DO UPDATE SET \
         data = EXCLUDED.data",
        &[
            Value::Text(file_id.to_string()),
            Value::Text(version_id.to_string()),
            Value::Blob(data.to_vec()),
        ],
    )
    .await?;
    Ok(())
}

pub(crate) async fn delete_file_payload_cache_data(
    backend: LiveStateBackendRef<'_>,
    file_id: &str,
    version_id: &str,
) -> Result<(), LixError> {
    execute_query_with_backend(
        backend,
        "DELETE FROM lix_internal_file_data_cache \
         WHERE file_id = $1 AND version_id = $2",
        &[
            Value::Text(file_id.to_string()),
            Value::Text(version_id.to_string()),
        ],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
pub(crate) async fn delete_live_state_status_row_for_tests(
    backend: LiveStateBackendRef<'_>,
) -> Result<(), LixError> {
    execute_query_with_backend(
        backend,
        "DELETE FROM lix_internal_live_state_status WHERE singleton_id = 1",
        &[],
    )
    .await?;
    Ok(())
}

#[async_trait(?Send)]
impl LiveStateReadStore for SqlLiveStateStore<'_> {
    async fn require_ready(&self) -> Result<(), LixError> {
        match &self.access {
            SqlLiveStateAccess::Backend(_) => {
                crate::live_state::lifecycle::require_ready(self).await
            }
            _ => Err(invalid_live_state_store_access(
                "backend-backed live-state store",
            )),
        }
    }

    async fn projection_status(&self) -> Result<crate::live_state::ProjectionStatus, LixError> {
        match &self.access {
            SqlLiveStateAccess::Backend(_) => Ok(
                crate::live_state::projection::projection_status_from_live_state(
                    crate::live_state::lifecycle::load_projection_status(self).await?,
                ),
            ),
            _ => Err(invalid_live_state_store_access(
                "backend-backed live-state store",
            )),
        }
    }

    async fn scan_live_rows(
        &self,
        request: &crate::live_state::LiveRowQuery,
    ) -> Result<Vec<LiveRow>, LixError> {
        match &self.access {
            SqlLiveStateAccess::Backend(backend) => {
                row_queries::scan_live_rows(*backend, request).await
            }
            _ => Err(invalid_live_state_store_access(
                "backend-backed live-state store",
            )),
        }
    }

    async fn load_exact_live_row(
        &self,
        request: &crate::live_state::ExactLiveRowQuery,
    ) -> Result<Option<LiveRow>, LixError> {
        match &self.access {
            SqlLiveStateAccess::Backend(backend) => {
                row_queries::load_exact_live_row(*backend, request).await
            }
            _ => Err(invalid_live_state_store_access(
                "backend-backed live-state store",
            )),
        }
    }
}

#[async_trait(?Send)]
impl LiveStateWriteStore for SqlLiveStateStore<'_> {
    async fn register_schema(&mut self, registration: &SchemaRegistration) -> Result<(), LixError> {
        let requirement = live_table_requirement_from_registration(registration)?;
        match &mut self.access {
            SqlLiveStateAccess::Backend(backend) => {
                let mut executor = *backend;
                ensure_schema_live_table_with_requirement_with_executor(&mut executor, &requirement)
                    .await
            }
            SqlLiveStateAccess::Transaction(transaction) => {
                let mut executor = crate::backend::transaction_backend_view(*transaction);
                ensure_schema_live_table_with_requirement_with_executor(&mut executor, &requirement)
                    .await
            }
            SqlLiveStateAccess::Executor(executor) => {
                ensure_schema_live_table_with_requirement_with_executor(*executor, &requirement)
                    .await
            }
        }
    }

    async fn write_live_rows(&mut self, rows: &[LiveRow]) -> Result<(), LixError> {
        match &mut self.access {
            SqlLiveStateAccess::Transaction(transaction) => {
                row_queries::write_live_rows(*transaction, rows).await
            }
            _ => Err(invalid_live_state_store_access(
                "transaction-backed live-state store",
            )),
        }
    }

    async fn mark_ready_at_latest_replay_cursor(&mut self) -> Result<ReplayCursor, LixError> {
        match &mut self.access {
            SqlLiveStateAccess::Transaction(_) => {
                crate::live_state::lifecycle::mark_live_state_ready_at_latest_replay_cursor_in_transaction(self).await
            }
            _ => Err(invalid_live_state_store_access(
                "transaction-backed live-state store",
            )),
        }
    }
}

#[async_trait(?Send)]
impl LiveStateMaterializeStore for SqlLiveStateStore<'_> {
    async fn rebuild_plan(
        &mut self,
        request: &crate::live_state::LiveStateRebuildRequest,
    ) -> Result<crate::live_state::LiveStateRebuildPlan, LixError> {
        match &mut self.access {
            SqlLiveStateAccess::Backend(backend) => {
                crate::live_state::materialize::rebuild_plan_with_backend(*backend, request).await
            }
            SqlLiveStateAccess::Transaction(transaction) => {
                crate::live_state::materialize::rebuild_plan_with_transaction(*transaction, request)
                    .await
            }
            SqlLiveStateAccess::Executor(executor) => {
                crate::live_state::materialize::rebuild_plan_with_executor(*executor, request).await
            }
        }
    }

    async fn apply_rebuild_plan(
        &mut self,
        plan: &crate::live_state::LiveStateRebuildPlan,
    ) -> Result<crate::live_state::LiveStateApplyReport, LixError> {
        match &mut self.access {
            SqlLiveStateAccess::Transaction(transaction) => {
                crate::live_state::materialize::apply_rebuild_plan_in_transaction(
                    *transaction,
                    plan,
                )
                .await
            }
            _ => Err(invalid_live_state_store_access(
                "transaction-backed live-state store",
            )),
        }
    }

    async fn rebuild_scope(
        &mut self,
        request: &crate::live_state::LiveStateRebuildRequest,
    ) -> Result<crate::live_state::LiveStateApplyReport, LixError> {
        match &mut self.access {
            SqlLiveStateAccess::Transaction(transaction) => {
                let plan = crate::live_state::materialize::rebuild_plan_with_transaction(
                    *transaction,
                    request,
                )
                .await?;
                let (rows_deleted, tables_touched) =
                    crate::live_state::materialize::apply_rebuild_scope_in_transaction(
                        *transaction,
                        &plan,
                    )
                    .await?;
                Ok(crate::live_state::LiveStateApplyReport {
                    run_id: plan.run_id.clone(),
                    rows_written: plan.writes.len(),
                    rows_deleted,
                    tables_touched: tables_touched.into_iter().collect(),
                })
            }
            _ => Err(invalid_live_state_store_access(
                "transaction-backed live-state store",
            )),
        }
    }
}

#[async_trait(?Send)]
impl LiveStateLifecycleReadStore for SqlLiveStateStore<'_> {
    async fn load_live_state_snapshot(&self) -> Result<LiveStateSnapshot, LixError> {
        let backend = self.backend()?;
        let mut executor = backend;
        Ok(LiveStateSnapshot {
            status: load_nullable_live_state_status_with_backend(backend).await?,
            latest_replay_cursor: self.load_latest_replay_cursor().await?,
            current_committed_frontier: load_current_committed_version_frontier_with_executor(
                &mut executor,
            )
            .await?,
        })
    }

    async fn load_latest_replay_cursor(&self) -> Result<Option<ReplayCursor>, LixError> {
        let result = self
            .backend()?
            .execute(
                "SELECT id, created_at \
                 FROM lix_internal_change \
                 ORDER BY created_at DESC, id DESC \
                 LIMIT 1",
                &[],
            )
            .await?;
        parse_latest_replay_cursor(&result)
    }

    async fn load_live_state_mode(&self) -> Result<LiveStateMode, LixError> {
        Ok(
            load_nullable_live_state_status_with_backend(self.backend()?)
                .await?
                .unwrap_or_else(default_live_state_status)
                .mode,
        )
    }
}

#[async_trait(?Send)]
impl LiveStateLifecycleAdminStore for SqlLiveStateStore<'_> {
    async fn init_live_state_status_storage(&self) -> Result<(), LixError> {
        let backend = self.backend()?;
        backend
            .execute(LIVE_STATE_STATUS_CREATE_TABLE_SQL, &[])
            .await?;
        add_column_if_missing(
            backend,
            LIVE_STATE_STATUS_TABLE,
            "applied_committed_frontier",
            "TEXT",
        )
        .await?;
        backend.execute(LIVE_STATE_STATUS_SEED_ROW_SQL, &[]).await?;
        Ok(())
    }

    async fn try_claim_live_state_bootstrap(&self) -> Result<bool, LixError> {
        let backend = self.backend()?;
        let result = backend
            .execute(
                "UPDATE lix_internal_live_state_status \
                 SET mode = 'bootstrapping', \
                     latest_change_id = NULL, \
                     latest_change_created_at = NULL, \
                     applied_committed_frontier = NULL, \
                     schema_epoch = $1, \
                     updated_at = CURRENT_TIMESTAMP \
                 WHERE singleton_id = 1 \
                   AND mode = 'uninitialized' \
                 RETURNING singleton_id",
                &[Value::Text(LIVE_STATE_SCHEMA_EPOCH.to_string())],
            )
            .await?;
        Ok(result.rows.first().is_some())
    }

    async fn load_current_committed_frontier(&self) -> Result<CommittedVersionFrontier, LixError> {
        let mut executor = self.backend()?;
        load_current_committed_version_frontier_with_executor(&mut executor).await
    }

    async fn mark_live_state_mode(&self, mode: LiveStateMode) -> Result<(), LixError> {
        self.backend()?
            .execute(&build_set_live_state_mode_sql(mode), &[])
            .await?;
        Ok(())
    }

    async fn mark_live_state_ready(
        &self,
        cursor: &ReplayCursor,
        frontier: &CommittedVersionFrontier,
    ) -> Result<(), LixError> {
        self.backend()?
            .execute(&build_mark_live_state_ready_sql(cursor, frontier), &[])
            .await?;
        Ok(())
    }

    async fn stamp_live_state_durable_consumer_cursor(
        &self,
        cursor: &ReplayCursor,
    ) -> Result<(), LixError> {
        let backend = self.backend()?;
        upsert_durable_state_commit_consumer_cursor_with_backend(
            backend,
            LIVE_STATE_DURABLE_CONSUMER_KEY,
            &durable_state_commit_cursor_from_replay_with_backend(backend, cursor).await?,
        )
        .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VersionHeadRef {
    version_id: String,
    commit_id: String,
}

#[async_trait(?Send)]
impl LiveStateLifecycleWriteStore for SqlLiveStateStore<'_> {
    async fn load_live_state_snapshot(&mut self) -> Result<LiveStateSnapshot, LixError> {
        let transaction = self.transaction()?;
        Ok(LiveStateSnapshot {
            status: load_nullable_live_state_status_in_transaction(transaction).await?,
            latest_replay_cursor: self.load_latest_replay_cursor().await?,
            current_committed_frontier: self.load_current_committed_frontier().await?,
        })
    }

    async fn load_latest_replay_cursor(&mut self) -> Result<Option<ReplayCursor>, LixError> {
        let result = self
            .transaction()?
            .execute(
                "SELECT id, created_at \
                 FROM lix_internal_change \
                 ORDER BY created_at DESC, id DESC \
                 LIMIT 1",
                &[],
            )
            .await?;
        parse_latest_replay_cursor(&result)
    }

    async fn ensure_live_state_status_row(&mut self) -> Result<(), LixError> {
        let transaction = self.transaction()?;
        transaction
            .execute(LIVE_STATE_STATUS_CREATE_TABLE_SQL, &[])
            .await?;
        if !live_state_status_column_exists_in_transaction(
            transaction,
            "applied_committed_frontier",
        )
        .await?
        {
            transaction
                .execute(
                    "ALTER TABLE lix_internal_live_state_status \
                     ADD COLUMN applied_committed_frontier TEXT",
                    &[],
                )
                .await?;
        }
        transaction
            .execute(LIVE_STATE_STATUS_SEED_ROW_SQL, &[])
            .await?;
        Ok(())
    }

    async fn try_claim_live_state_bootstrap(&mut self) -> Result<bool, LixError> {
        let result = self
            .transaction()?
            .execute(
                "UPDATE lix_internal_live_state_status \
                 SET mode = 'bootstrapping', \
                     latest_change_id = NULL, \
                     latest_change_created_at = NULL, \
                     applied_committed_frontier = NULL, \
                     schema_epoch = $1, \
                     updated_at = CURRENT_TIMESTAMP \
                 WHERE singleton_id = 1 \
                   AND mode = 'uninitialized' \
                 RETURNING singleton_id",
                &[Value::Text(LIVE_STATE_SCHEMA_EPOCH.to_string())],
            )
            .await?;
        Ok(result.rows.first().is_some())
    }

    async fn load_current_committed_frontier(
        &mut self,
    ) -> Result<CommittedVersionFrontier, LixError> {
        let mut executor = self.transaction()?;
        crate::live_state::load_current_committed_version_frontier_with_executor(&mut executor)
            .await
    }

    async fn load_current_applied_frontier(
        &mut self,
    ) -> Result<Option<CommittedVersionFrontier>, LixError> {
        Ok(
            load_nullable_live_state_status_in_transaction(self.transaction()?)
                .await?
                .and_then(|status| status.applied_committed_frontier),
        )
    }

    async fn mark_live_state_ready(
        &mut self,
        cursor: &ReplayCursor,
        frontier: &CommittedVersionFrontier,
    ) -> Result<(), LixError> {
        self.transaction()?
            .execute(&build_mark_live_state_ready_sql(cursor, frontier), &[])
            .await?;
        Ok(())
    }

    async fn mark_live_state_ready_without_cursor(
        &mut self,
        frontier: &CommittedVersionFrontier,
    ) -> Result<(), LixError> {
        self.transaction()?
            .execute(
                &build_mark_live_state_ready_without_cursor_sql(frontier),
                &[],
            )
            .await?;
        Ok(())
    }

    async fn mark_live_state_mode(&mut self, mode: LiveStateMode) -> Result<(), LixError> {
        self.transaction()?
            .execute(&build_set_live_state_mode_sql(mode), &[])
            .await?;
        Ok(())
    }

    async fn mark_live_state_mode_with_cursor_and_frontier(
        &mut self,
        mode: LiveStateMode,
        cursor: &ReplayCursor,
        frontier: Option<&CommittedVersionFrontier>,
    ) -> Result<(), LixError> {
        self.transaction()?
            .execute(
                &build_set_live_state_mode_with_cursor_and_frontier_sql(mode, cursor, frontier),
                &[],
            )
            .await?;
        Ok(())
    }

    async fn stamp_live_state_durable_consumer_cursor(
        &mut self,
        cursor: &ReplayCursor,
    ) -> Result<(), LixError> {
        let transaction = self.transaction()?;
        let durable_cursor =
            durable_state_commit_cursor_from_replay_in_transaction(transaction, cursor).await?;
        upsert_durable_state_commit_consumer_cursor_in_transaction(
            transaction,
            LIVE_STATE_DURABLE_CONSUMER_KEY,
            &durable_cursor,
        )
        .await
    }

    async fn clear_live_state_durable_consumer_cursor(&mut self) -> Result<(), LixError> {
        delete_durable_state_commit_consumer_cursor_in_transaction(
            self.transaction()?,
            LIVE_STATE_DURABLE_CONSUMER_KEY,
        )
        .await
    }
}

#[async_trait(?Send)]
impl LiveStateFrontierReadStore for SqlLiveStateStore<'_> {
    async fn load_version_head_commit_id(
        &mut self,
        version_id: &str,
    ) -> Result<Option<String>, LixError> {
        match &mut self.access {
            SqlLiveStateAccess::Backend(backend) => {
                let mut executor = *backend;
                load_version_head_commit_id_with_executor(&mut executor, version_id).await
            }
            SqlLiveStateAccess::Transaction(transaction) => {
                let mut executor = crate::backend::transaction_backend_view(*transaction);
                load_version_head_commit_id_with_executor(&mut executor, version_id).await
            }
            SqlLiveStateAccess::Executor(executor) => {
                load_version_head_commit_id_with_executor(*executor, version_id).await
            }
        }
    }

    async fn load_version_head_commit_map(
        &mut self,
    ) -> Result<Option<BTreeMap<String, String>>, LixError> {
        match &mut self.access {
            SqlLiveStateAccess::Backend(backend) => {
                let mut executor = *backend;
                load_version_head_commit_map_with_executor(&mut executor).await
            }
            SqlLiveStateAccess::Transaction(transaction) => {
                let mut executor = crate::backend::transaction_backend_view(*transaction);
                load_version_head_commit_map_with_executor(&mut executor).await
            }
            SqlLiveStateAccess::Executor(executor) => {
                load_version_head_commit_map_with_executor(*executor).await
            }
        }
    }

    async fn load_current_committed_version_frontier(
        &mut self,
    ) -> Result<CommittedVersionFrontier, LixError> {
        match &mut self.access {
            SqlLiveStateAccess::Backend(backend) => {
                let mut executor = *backend;
                load_current_committed_version_frontier_with_executor(&mut executor).await
            }
            SqlLiveStateAccess::Transaction(transaction) => {
                let mut executor = crate::backend::transaction_backend_view(*transaction);
                load_current_committed_version_frontier_with_executor(&mut executor).await
            }
            SqlLiveStateAccess::Executor(executor) => {
                load_current_committed_version_frontier_with_executor(*executor).await
            }
        }
    }
}

async fn load_nullable_live_state_status_with_backend(
    backend: &dyn LixBackend,
) -> Result<Option<crate::live_state::lifecycle::LiveStateStatusRow>, LixError> {
    let result = backend
        .execute(
            "SELECT mode, latest_change_id, latest_change_created_at, schema_epoch, applied_committed_frontier \
             FROM lix_internal_live_state_status \
             WHERE singleton_id = 1 \
             LIMIT 1",
            &[],
        )
        .await;
    parse_nullable_live_state_status_result(result)
}

async fn load_nullable_live_state_status_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<Option<crate::live_state::lifecycle::LiveStateStatusRow>, LixError> {
    let result = transaction
        .execute(
            "SELECT mode, latest_change_id, latest_change_created_at, schema_epoch, applied_committed_frontier \
             FROM lix_internal_live_state_status \
             WHERE singleton_id = 1 \
             LIMIT 1",
            &[],
        )
        .await;
    parse_nullable_live_state_status_result(result)
}

pub(crate) async fn load_nullable_live_state_status_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Option<crate::live_state::lifecycle::LiveStateStatusRow>, LixError> {
    let result = executor
        .execute(
            "SELECT mode, latest_change_id, latest_change_created_at, schema_epoch, applied_committed_frontier \
             FROM lix_internal_live_state_status \
             WHERE singleton_id = 1 \
             LIMIT 1",
            &[],
        )
        .await;
    parse_nullable_live_state_status_result(result)
}

pub(crate) async fn load_latest_replay_cursor_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Option<ReplayCursor>, LixError> {
    let result = executor
        .execute(
            "SELECT id, created_at \
             FROM lix_internal_change \
             ORDER BY created_at DESC, id DESC \
             LIMIT 1",
            &[],
        )
        .await?;
    parse_latest_replay_cursor(&result)
}

async fn durable_state_commit_cursor_from_replay_with_backend(
    backend: &dyn LixBackend,
    cursor: &ReplayCursor,
) -> Result<DurableStateCommitCursor, LixError> {
    Ok(DurableStateCommitCursor {
        change_id: cursor.change_id.clone(),
        created_at: cursor.created_at.clone(),
        visibility_append_seq: load_latest_untracked_visibility_append_seq(backend).await?,
    })
}

async fn durable_state_commit_cursor_from_replay_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    cursor: &ReplayCursor,
) -> Result<DurableStateCommitCursor, LixError> {
    Ok(DurableStateCommitCursor {
        change_id: cursor.change_id.clone(),
        created_at: cursor.created_at.clone(),
        visibility_append_seq: load_latest_untracked_visibility_append_seq_in_transaction(
            transaction,
        )
        .await?,
    })
}

async fn live_state_status_column_exists_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    column: &str,
) -> Result<bool, LixError> {
    let exists: QueryResult = match transaction.dialect() {
        SqlDialect::Sqlite => {
            transaction
                .execute(
                    "SELECT 1 \
                     FROM pragma_table_info('lix_internal_live_state_status') \
                     WHERE name = $1 \
                     LIMIT 1",
                    &[Value::Text(column.to_string())],
                )
                .await?
        }
        SqlDialect::Postgres => {
            transaction
                .execute(
                    "SELECT 1 \
                     FROM information_schema.columns \
                     WHERE table_schema = current_schema() \
                       AND table_name = $1 \
                       AND column_name = $2 \
                     LIMIT 1",
                    &[
                        Value::Text(LIVE_STATE_STATUS_TABLE.to_string()),
                        Value::Text(column.to_string()),
                    ],
                )
                .await?
        }
    };
    Ok(!exists.rows.is_empty())
}

async fn load_version_head_commit_id_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    let Some(row) = load_exact_untracked_row_with_executor(
        executor,
        &ExactUntrackedRowRequest {
            schema_key: version_ref_schema_key().to_string(),
            version_id: version_ref_storage_version_id().to_string(),
            entity_id: version_id.to_string(),
            file_id: crate::NullableKeyFilter::Null,
        },
    )
    .await?
    else {
        return Ok(None);
    };

    let Some(commit_id) = row
        .property_text("commit_id")
        .filter(|value| !value.trim().is_empty())
    else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("local version head for '{version_id}' has empty commit_id"),
        ));
    };

    Ok(Some(commit_id))
}

async fn load_version_head_commit_map_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Option<BTreeMap<String, String>>, LixError> {
    Ok(load_all_version_head_refs_with_executor(executor)
        .await?
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.version_id, row.commit_id))
                .collect()
        }))
}

async fn load_current_committed_version_frontier_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<CommittedVersionFrontier, LixError> {
    Ok(CommittedVersionFrontier {
        version_heads: load_all_version_head_refs_with_executor(executor)
            .await?
            .unwrap_or_default()
            .into_iter()
            .map(|row| (row.version_id, row.commit_id))
            .collect(),
    })
}

async fn load_all_version_head_refs_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Option<Vec<VersionHeadRef>>, LixError> {
    let result = match executor
        .execute(
            &format!(
                "SELECT entity_id, commit_id \
                 FROM {table} \
                 WHERE schema_key = $1 \
                   AND schema_version = $2 \
                   AND file_id IS NULL \
                   AND version_id = $3 \
                   AND plugin_key IS NULL \
                   AND untracked = true \
                   AND is_tombstone = 0 \
                   AND commit_id IS NOT NULL \
                   AND commit_id <> '' \
                 ORDER BY entity_id ASC, updated_at DESC",
                table = tracked_relation_name(version_ref_schema_key()),
            ),
            &[
                Value::Text(version_ref_schema_key().to_string()),
                Value::Text(version_ref_schema_version().to_string()),
                Value::Text(version_ref_storage_version_id().to_string()),
            ],
        )
        .await
    {
        Ok(result) => result,
        Err(error) if crate::common::is_missing_relation_error(&error) => return Ok(None),
        Err(error) => return Err(error),
    };

    let mut rows = Vec::with_capacity(result.rows.len());
    let mut previous_version_id: Option<String> = None;
    for row in &result.rows {
        let parsed = parse_version_head_ref_row(row)?;
        if matches!(previous_version_id.as_ref(), Some(previous) if previous == &parsed.version_id)
        {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "local version-head resolution for version '{}' found multiple exact rows",
                    parsed.version_id
                ),
            ));
        }
        previous_version_id = Some(parsed.version_id.clone());
        rows.push(parsed);
    }
    Ok(Some(rows))
}

fn parse_version_head_ref_row(row: &[Value]) -> Result<VersionHeadRef, LixError> {
    let version_id = local_required_text_cell(row, 0, "entity_id")?;
    let commit_id = local_required_text_cell(row, 1, "commit_id")?;
    if commit_id.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "local version head for '{}' has empty commit_id",
                version_id
            ),
        ));
    }
    Ok(VersionHeadRef {
        version_id,
        commit_id,
    })
}

fn local_required_text_cell(
    row: &[Value],
    index: usize,
    column_name: &str,
) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.clone()),
        Some(Value::Integer(value)) => Ok(value.to_string()),
        Some(_) | None => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("local version-ref row is missing text column '{column_name}'"),
        )),
    }
}

fn invalid_live_state_store_access(expected: &'static str) -> LixError {
    LixError::new(
        "LIX_ERROR_INVALID_OPERATION",
        format!("live-state SQL adapter requires {expected}"),
    )
}

pub(crate) async fn load_visible_registered_schema_snapshot_contents(
    backend: &dyn LixBackend,
) -> Result<BTreeMap<String, String>, LixError> {
    let sql = format!(
        "SELECT snapshot_content FROM {table} \
         WHERE version_id = '{global_version}' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL",
        table = crate::live_state::REGISTERED_SCHEMA_BOOTSTRAP_TABLE,
        global_version = crate::version::GLOBAL_VERSION_ID,
    );
    let result = backend.execute(&sql, &[]).await?;
    let mut rows = BTreeMap::new();
    for row in result.rows {
        let Some(Value::Text(snapshot_content)) = row.first() else {
            continue;
        };
        let snapshot: serde_json::Value =
            serde_json::from_str(snapshot_content).map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("registered schema snapshot_content invalid JSON: {error}"),
                hint: None,
            })?;
        let (key, _) = crate::schema::schema_from_registered_snapshot(&snapshot)?;
        rows.insert(key.entity_id(), snapshot_content.clone());
    }
    Ok(rows)
}

fn live_table_requirement_from_registration(
    registration: &SchemaRegistration,
) -> Result<LiveTableRequirement, LixError> {
    let layout = if let Some(schema_definition) = registration.schema_definition_override() {
        Some(live_table_layout_from_schema(schema_definition)?)
    } else {
        registration
            .registered_snapshot()
            .map(|snapshot| {
                let (_, schema) = schema_from_registered_snapshot(snapshot)?;
                live_table_layout_from_schema(&schema)
            })
            .transpose()?
    };

    Ok(LiveTableRequirement {
        schema_key: registration.schema_key().to_string(),
        layout,
    })
}

async fn ensure_schema_live_table_with_requirement_with_executor(
    executor: &mut dyn QueryExecutor,
    requirement: &LiveTableRequirement,
) -> Result<(), LixError> {
    let layout = match requirement.layout.as_ref() {
        Some(layout) => layout.clone(),
        None => load_live_table_layout_with_executor(executor, &requirement.schema_key).await?,
    };
    let statements = ensure_schema_live_table_sql_statements(
        &requirement.schema_key,
        executor.dialect(),
        &layout,
    );
    let table_name = tracked_relation_name(&requirement.schema_key);

    if let Some(create_table) = statements.first() {
        executor.execute(create_table, &[]).await?;
    }

    add_column_if_missing_with_executor(
        executor,
        &table_name,
        STORAGE_SCOPE_KEY_COLUMN,
        &format!(
            "TEXT NOT NULL DEFAULT '{}'",
            storage_scope_key_for_file_id(None)
        ),
    )
    .await?;
    executor
        .execute(
            &format!(
                "UPDATE {table_name} \
                 SET {storage_scope_key} = CASE \
                   WHEN file_id IS NULL THEN '{engine_scope}' \
                   ELSE 'file:' || file_id \
                 END",
                storage_scope_key = STORAGE_SCOPE_KEY_COLUMN,
                engine_scope = storage_scope_key_for_file_id(None),
            ),
            &[],
        )
        .await?;

    for statement in statements.iter().skip(1) {
        executor.execute(statement, &[]).await?;
    }
    Ok(())
}
