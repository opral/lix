use std::collections::BTreeSet;

use crate::{LixBackend, LixError, SqlDialect, Value};

const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";
const FILE_LIXCOL_CACHE_TABLE: &str = "lix_internal_file_lixcol_cache";
const WRITER_KEY_COLUMN: &str = "writer_key";

const INIT_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS lix_internal_snapshot (\
     id TEXT PRIMARY KEY,\
     content TEXT\
     )",
    "INSERT INTO lix_internal_snapshot (id, content) VALUES ('no-content', NULL) \
     ON CONFLICT (id) DO NOTHING",
    "CREATE TABLE IF NOT EXISTS lix_internal_change (\
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
    "CREATE TABLE IF NOT EXISTS lix_internal_state_materialized_v1_lix_stored_schema (\
     entity_id TEXT NOT NULL,\
     schema_key TEXT NOT NULL,\
     schema_version TEXT NOT NULL,\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     plugin_key TEXT NOT NULL,\
     snapshot_content TEXT,\
     inherited_from_version_id TEXT,\
     change_id TEXT NOT NULL,\
     metadata TEXT,\
     writer_key TEXT,\
     is_tombstone INTEGER NOT NULL DEFAULT 0,\
     created_at TEXT NOT NULL,\
     updated_at TEXT NOT NULL,\
     PRIMARY KEY (entity_id, file_id, version_id)\
     )",
    "CREATE TABLE IF NOT EXISTS lix_internal_state_untracked (\
     entity_id TEXT NOT NULL,\
     schema_key TEXT NOT NULL,\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     plugin_key TEXT NOT NULL,\
     snapshot_content TEXT,\
     metadata TEXT,\
     schema_version TEXT NOT NULL,\
     created_at TEXT NOT NULL,\
     updated_at TEXT NOT NULL,\
     PRIMARY KEY (entity_id, schema_key, file_id, version_id)\
     )",
    "CREATE TABLE IF NOT EXISTS lix_internal_file_data_cache (\
     file_id TEXT NOT NULL,\
     version_id TEXT NOT NULL,\
     data BYTEA NOT NULL,\
     PRIMARY KEY (file_id, version_id)\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_file_data_cache_version_id \
     ON lix_internal_file_data_cache (version_id)",
    "CREATE TABLE IF NOT EXISTS lix_internal_file_history_data_cache (\
     file_id TEXT NOT NULL,\
     root_commit_id TEXT NOT NULL,\
     depth BIGINT NOT NULL,\
     data BYTEA NOT NULL,\
     PRIMARY KEY (file_id, root_commit_id, depth)\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_file_history_data_cache_root_depth \
     ON lix_internal_file_history_data_cache (root_commit_id, depth)",
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
     writer_key TEXT,\
     PRIMARY KEY (file_id, version_id)\
     )",
    "CREATE INDEX IF NOT EXISTS idx_file_lixcol_cache_lookup \
     ON lix_internal_file_lixcol_cache (file_id, version_id)",
    "CREATE TABLE IF NOT EXISTS lix_internal_plugin (\
     key TEXT PRIMARY KEY,\
     runtime TEXT NOT NULL,\
     api_version TEXT NOT NULL,\
     detect_changes_glob TEXT NOT NULL,\
     entry TEXT NOT NULL,\
     manifest_json TEXT NOT NULL,\
     wasm BYTEA NOT NULL,\
     created_at TEXT NOT NULL,\
     updated_at TEXT NOT NULL\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_plugin_runtime \
     ON lix_internal_plugin (runtime)",
];

pub async fn init_backend(backend: &dyn LixBackend) -> Result<(), LixError> {
    for statement in INIT_STATEMENTS {
        backend.execute(statement, &[]).await?;
    }
    ensure_writer_key_column_migrations(backend).await?;
    Ok(())
}

async fn ensure_writer_key_column_migrations(backend: &dyn LixBackend) -> Result<(), LixError> {
    let mut table_names = fetch_materialized_table_names(backend).await?;
    table_names.insert(FILE_LIXCOL_CACHE_TABLE.to_string());

    for table_name in table_names {
        ensure_table_has_writer_key_column(backend, &table_name).await?;
    }

    Ok(())
}

async fn fetch_materialized_table_names(
    backend: &dyn LixBackend,
) -> Result<BTreeSet<String>, LixError> {
    let sql = match backend.dialect() {
        SqlDialect::Sqlite => {
            "SELECT name FROM sqlite_master \
             WHERE type = 'table' \
               AND name LIKE 'lix_internal_state_materialized_v1_%'"
        }
        SqlDialect::Postgres => {
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' \
               AND table_type = 'BASE TABLE' \
               AND table_name LIKE 'lix_internal_state_materialized_v1_%'"
        }
    };
    let result = backend.execute(sql, &[]).await?;

    let mut table_names = BTreeSet::new();
    for row in result.rows {
        let Some(Value::Text(name)) = row.first() else {
            continue;
        };
        if name.starts_with(MATERIALIZED_PREFIX) {
            table_names.insert(name.clone());
        }
    }

    Ok(table_names)
}

async fn ensure_table_has_writer_key_column(
    backend: &dyn LixBackend,
    table_name: &str,
) -> Result<(), LixError> {
    if table_has_column(backend, table_name, WRITER_KEY_COLUMN).await? {
        return Ok(());
    }

    let alter_sql = format!(
        "ALTER TABLE {} ADD COLUMN {} TEXT",
        quote_ident(table_name),
        quote_ident(WRITER_KEY_COLUMN)
    );
    backend.execute(&alter_sql, &[]).await?;

    Ok(())
}

async fn table_has_column(
    backend: &dyn LixBackend,
    table_name: &str,
    column_name: &str,
) -> Result<bool, LixError> {
    match backend.dialect() {
        SqlDialect::Sqlite => {
            let pragma_sql = format!("PRAGMA table_info({})", quote_ident(table_name));
            let result = backend.execute(&pragma_sql, &[]).await?;
            for row in result.rows {
                let Some(Value::Text(name)) = row.get(1) else {
                    continue;
                };
                if name.eq_ignore_ascii_case(column_name) {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        SqlDialect::Postgres => {
            let info_sql = format!(
                "SELECT 1 \
                 FROM information_schema.columns \
                 WHERE table_schema = 'public' \
                   AND table_name = '{}' \
                   AND column_name = '{}' \
                 LIMIT 1",
                escape_sql_string(table_name),
                escape_sql_string(column_name),
            );
            let result = backend.execute(&info_sql, &[]).await?;
            Ok(!result.rows.is_empty())
        }
    }
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
