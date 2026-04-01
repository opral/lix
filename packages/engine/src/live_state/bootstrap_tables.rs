use crate::live_state::register_schema;
use crate::live_state::schema_access::{snapshot_select_expr_for_schema, tracked_relation_name};
use crate::schema::builtin::builtin_schema_keys;
use crate::{LixBackend, LixError, SqlDialect, Value};

const INIT_STATEMENTS: &[&str] = &[
    "CREATE TABLE lix_internal_snapshot (\
     id TEXT PRIMARY KEY,\
     content TEXT\
     )",
    "INSERT INTO lix_internal_snapshot (id, content) \
     SELECT 'no-content', NULL \
     WHERE NOT EXISTS ( \
       SELECT 1 FROM lix_internal_snapshot WHERE id = 'no-content' \
     )",
    "CREATE TABLE lix_internal_change (\
     id TEXT PRIMARY KEY,\
     entity_id TEXT NOT NULL,\
     schema_key TEXT NOT NULL,\
     schema_version TEXT NOT NULL,\
     file_id TEXT NOT NULL,\
     plugin_key TEXT NOT NULL,\
     snapshot_id TEXT NOT NULL,\
     metadata TEXT,\
     created_at TEXT NOT NULL\
     )",
    "CREATE TABLE lix_internal_commit_idempotency (\
     write_lane TEXT NOT NULL,\
     idempotency_key TEXT NOT NULL,\
     idempotency_kind TEXT NOT NULL,\
     idempotency_value TEXT NOT NULL,\
     parent_head_snapshot_content TEXT NOT NULL,\
     commit_id TEXT NOT NULL,\
     created_at TEXT NOT NULL,\
     PRIMARY KEY (write_lane, idempotency_kind, idempotency_value, parent_head_snapshot_content)\
     )",
    "CREATE INDEX idx_lix_internal_commit_idempotency_commit_id \
     ON lix_internal_commit_idempotency (commit_id)",
    "CREATE INDEX idx_lix_internal_commit_idempotency_legacy \
     ON lix_internal_commit_idempotency (write_lane, idempotency_key)",
    "CREATE TABLE lix_internal_registered_schema_bootstrap (\
     entity_id TEXT NOT NULL,\
     schema_key TEXT NOT NULL,\
     schema_version TEXT NOT NULL,\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     global BOOLEAN NOT NULL DEFAULT false,\
     plugin_key TEXT NOT NULL,\
     snapshot_content TEXT,\
     change_id TEXT NOT NULL,\
     metadata TEXT,\
     writer_key TEXT,\
     is_tombstone INTEGER NOT NULL DEFAULT 0,\
     untracked BOOLEAN NOT NULL DEFAULT false,\
     created_at TEXT NOT NULL,\
     updated_at TEXT NOT NULL,\
     PRIMARY KEY (entity_id, file_id, version_id, untracked)\
     )",
    "CREATE INDEX idx_lix_internal_registered_schema_bootstrap_version_id \
     ON lix_internal_registered_schema_bootstrap (version_id)",
    "CREATE INDEX idx_lix_internal_registered_schema_bootstrap_global_version \
     ON lix_internal_registered_schema_bootstrap (global, version_id)",
    "CREATE INDEX idx_lix_internal_registered_schema_bootstrap_vfe \
     ON lix_internal_registered_schema_bootstrap (version_id, file_id, entity_id)",
    "CREATE INDEX idx_lix_internal_registered_schema_bootstrap_live_vfe \
     ON lix_internal_registered_schema_bootstrap (version_id, file_id, entity_id) \
     WHERE is_tombstone = 0 AND snapshot_content IS NOT NULL",
    "CREATE TABLE lix_internal_file_data_cache (\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     data BYTEA NOT NULL,\
     PRIMARY KEY (file_id, version_id)\
     )",
    "CREATE INDEX idx_lix_internal_file_data_cache_version_id \
     ON lix_internal_file_data_cache (version_id)",
    "CREATE TABLE lix_internal_commit_graph_node (\
     commit_id TEXT PRIMARY KEY,\
     generation BIGINT NOT NULL\
     )",
    "CREATE INDEX idx_lix_internal_commit_graph_node_generation \
     ON lix_internal_commit_graph_node (generation)",
    "CREATE TABLE lix_internal_last_checkpoint (\
     version_id TEXT PRIMARY KEY,\
     checkpoint_commit_id TEXT NOT NULL\
     )",
    "CREATE INDEX idx_lix_internal_last_checkpoint_commit \
     ON lix_internal_last_checkpoint (checkpoint_commit_id)",
    "CREATE TABLE lix_internal_undo_redo_operation (\
     version_id TEXT NOT NULL,\
     operation_commit_id TEXT PRIMARY KEY,\
     operation_kind TEXT NOT NULL,\
     target_commit_id TEXT NOT NULL,\
     created_at TEXT NOT NULL\
     )",
    "CREATE INDEX idx_lix_internal_undo_redo_operation_version_created \
     ON lix_internal_undo_redo_operation (version_id, created_at)",
    "CREATE INDEX idx_lix_internal_undo_redo_operation_target \
     ON lix_internal_undo_redo_operation (target_commit_id)",
    "CREATE TABLE lix_internal_entity_state_timeline_breakpoint (\
     root_commit_id TEXT NOT NULL,\
     entity_id TEXT NOT NULL,\
     schema_key TEXT NOT NULL,\
     file_id TEXT NOT NULL,\
     from_depth BIGINT NOT NULL,\
     plugin_key TEXT NOT NULL,\
     schema_version TEXT NOT NULL,\
     metadata TEXT,\
     snapshot_id TEXT NOT NULL,\
     change_id TEXT NOT NULL,\
     PRIMARY KEY (root_commit_id, entity_id, schema_key, file_id, from_depth)\
     )",
    "CREATE INDEX idx_lix_internal_entity_state_timeline_breakpoint_root_depth \
     ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, from_depth)",
    "CREATE INDEX idx_lix_internal_entity_state_timeline_breakpoint_lookup \
     ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, entity_id, file_id, schema_key, from_depth)",
    "CREATE INDEX idx_lix_internal_entity_state_timeline_breakpoint_filters \
     ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, file_id, plugin_key, schema_key, entity_id, from_depth)",
    "CREATE TABLE lix_internal_timeline_status (\
     root_commit_id TEXT PRIMARY KEY,\
     built_max_depth BIGINT NOT NULL,\
     built_at TEXT NOT NULL\
     )",
    "CREATE TABLE lix_internal_file_path_cache (\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     directory_id TEXT,\
     name TEXT NOT NULL,\
     extension TEXT,\
     path TEXT NOT NULL,\
     PRIMARY KEY (file_id, version_id)\
     )",
    "CREATE INDEX idx_lix_internal_file_path_cache_version_path \
     ON lix_internal_file_path_cache (version_id, path, file_id)",
    "CREATE INDEX idx_lix_internal_file_path_cache_version_directory \
     ON lix_internal_file_path_cache (version_id, directory_id)",
    "CREATE TABLE lix_internal_file_lixcol_cache (\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     latest_change_id TEXT,\
     latest_commit_id TEXT,\
     created_at TEXT,\
     updated_at TEXT,\
     writer_key TEXT,\
     PRIMARY KEY (file_id, version_id)\
     )",
    "CREATE INDEX idx_file_lixcol_cache_lookup \
     ON lix_internal_file_lixcol_cache (file_id, version_id)",
];

pub(crate) async fn create_backend_tables(backend: &dyn LixBackend) -> Result<(), LixError> {
    if backend.dialect() == SqlDialect::Sqlite {
        backend.execute("PRAGMA foreign_keys = ON", &[]).await?;
    }
    for (index, statement) in INIT_STATEMENTS.iter().enumerate() {
        backend.execute(statement, &[]).await.map_err(|error| {
            LixError::new(
                &error.code,
                &format!(
                    "create_backend_tables statement #{index} failed: {} :: {}",
                    compact_sql(statement),
                    error.description
                ),
            )
        })?;
    }
    create_live_table_for_schema(backend, "lix_registered_schema")
        .await
        .map_err(|error| {
            LixError::new(
                &error.code,
                &format!(
                    "create_backend_tables create_live_table_for_schema(lix_registered_schema) failed: {}",
                    error.description
                ),
            )
        })?;
    seed_registered_schema_bootstrap_rows(backend)
        .await
        .map_err(|error| {
            LixError::new(
                &error.code,
                &format!(
                    "create_backend_tables seed_registered_schema_bootstrap_rows failed: {}",
                    error.description
                ),
            )
        })?;
    crate::binary_cas::init(backend)
        .await
        .map_err(|error| {
            LixError::new(
                &error.code,
                &format!(
                    "create_backend_tables binary_cas::init failed: {}",
                    error.description
                ),
            )
        })?;
    create_observe_tick_table(backend).await.map_err(|error| {
        LixError::new(
            &error.code,
            &format!(
                "create_backend_tables create_observe_tick_table failed: {}",
                error.description
            ),
        )
    })?;
    Ok(())
}

pub(crate) async fn create_builtin_schema_tables(backend: &dyn LixBackend) -> Result<(), LixError> {
    for schema_key in builtin_schema_keys() {
        create_live_table_for_schema(backend, schema_key).await?;
    }
    Ok(())
}

async fn create_live_table_for_schema(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<(), LixError> {
    register_schema(backend, schema_key).await
}

async fn seed_registered_schema_bootstrap_rows(backend: &dyn LixBackend) -> Result<(), LixError> {
    let registered_schema_table = tracked_relation_name("lix_registered_schema");
    let snapshot_expr =
        snapshot_select_expr_for_schema("lix_registered_schema", None, backend.dialect(), Some("m"))?;
    backend
        .execute(
            &format!(
                "INSERT INTO lix_internal_registered_schema_bootstrap (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, snapshot_content, change_id, metadata, writer_key, is_tombstone, untracked, created_at, updated_at\
             ) \
             SELECT m.entity_id, m.schema_key, m.schema_version, m.file_id, m.version_id, m.global, m.plugin_key, {snapshot_expr}, COALESCE(m.change_id, ''), m.metadata, m.writer_key, m.is_tombstone, m.untracked, m.created_at, m.updated_at \
             FROM {registered_schema_table} m \
             WHERE NOT EXISTS (\
               SELECT 1 \
               FROM lix_internal_registered_schema_bootstrap b \
               WHERE b.entity_id = m.entity_id \
                 AND b.file_id = m.file_id \
                 AND b.version_id = m.version_id \
                 AND b.untracked = m.untracked\
             )",
                snapshot_expr = snapshot_expr,
            ),
            &[],
        )
        .await?;
    Ok(())
}

async fn create_observe_tick_table(backend: &dyn LixBackend) -> Result<(), LixError> {
    match backend.dialect() {
        SqlDialect::Sqlite => {
            backend
                .execute(
                    "CREATE TABLE lix_internal_observe_tick (\
                     tick_seq INTEGER PRIMARY KEY AUTOINCREMENT,\
                     created_at TEXT NOT NULL,\
                     writer_key TEXT\
                     )",
                    &[],
                )
                .await?;
        }
        SqlDialect::Postgres => {
            backend
                .execute(
                    "CREATE TABLE lix_internal_observe_tick (\
                     tick_seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,\
                     created_at TEXT NOT NULL,\
                     writer_key TEXT\
                     )",
                    &[],
                )
                .await?;
        }
    }
    Ok(())
}

async fn add_column_if_missing(
    backend: &dyn LixBackend,
    table: &str,
    column: &str,
    column_ddl: &str,
) -> Result<(), LixError> {
    if column_exists(backend, table, column).await? {
        return Ok(());
    }

    let alter = format!("ALTER TABLE {table} ADD COLUMN {column} {column_ddl}");
    backend.execute(&alter, &[]).await?;
    Ok(())
}

async fn column_exists(
    backend: &dyn LixBackend,
    table: &str,
    column: &str,
) -> Result<bool, LixError> {
    let exists = match backend.dialect() {
        SqlDialect::Sqlite => {
            backend
                .execute(
                    &format!(
                        "SELECT 1 \
                         FROM pragma_table_info('{table}') \
                         WHERE name = $1 \
                         LIMIT 1"
                    ),
                    &[Value::Text(column.to_string())],
                )
                .await?
        }
        SqlDialect::Postgres => {
            backend
                .execute(
                    "SELECT 1 \
                     FROM information_schema.columns \
                     WHERE table_schema = current_schema() \
                       AND table_name = $1 \
                       AND column_name = $2 \
                     LIMIT 1",
                    &[
                        Value::Text(table.to_string()),
                        Value::Text(column.to_string()),
                    ],
                )
                .await?
        }
    };
    Ok(!exists.rows.is_empty())
}

fn compact_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}
