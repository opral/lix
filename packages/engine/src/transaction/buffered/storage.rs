use crate::common::{escape_sql_string, storage_scope_key_for_file_id};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError};

pub(crate) struct RegisteredSchemaMirrorRow<'a> {
    pub(crate) entity_id: &'a str,
    pub(crate) schema_version: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) version_id: &'a str,
    pub(crate) plugin_key: Option<&'a str>,
    pub(crate) snapshot_content: Option<&'a str>,
    pub(crate) metadata: Option<&'a str>,
    pub(crate) change_id: &'a str,
    pub(crate) untracked: bool,
    pub(crate) created_at: &'a str,
}

const REGISTERED_SCHEMA_BOOTSTRAP_TABLE: &str = "lix_internal_registered_schema_bootstrap";
const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

pub(crate) async fn upsert_registered_schema_mirror_row_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    row: RegisteredSchemaMirrorRow<'_>,
) -> Result<(), LixError> {
    let snapshot_sql = row
        .snapshot_content
        .map(|value| format!("'{}'", escape_sql_string(value)))
        .unwrap_or_else(|| "NULL".to_string());
    let file_id_sql = row
        .file_id
        .map(|value| format!("'{}'", escape_sql_string(value)))
        .unwrap_or_else(|| "NULL".to_string());
    let storage_scope_key_sql = format!(
        "'{}'",
        escape_sql_string(&storage_scope_key_for_file_id(row.file_id))
    );
    let plugin_key_sql = row
        .plugin_key
        .map(|value| format!("'{}'", escape_sql_string(value)))
        .unwrap_or_else(|| "NULL".to_string());
    let metadata_sql = row
        .metadata
        .map(|value| format!("'{}'", escape_sql_string(value)))
        .unwrap_or_else(|| "NULL".to_string());
    let sql = format!(
        "INSERT INTO {table} (\
         entity_id, schema_key, schema_version, file_id, storage_scope_key, version_id, global, plugin_key, snapshot_content, change_id, metadata, is_tombstone, untracked, created_at, updated_at\
         ) VALUES (\
         '{entity_id}', '{schema_key}', '{schema_version}', {file_id}, {storage_scope_key}, '{version_id}', {global}, {plugin_key}, {snapshot_content}, '{change_id}', {metadata}, {is_tombstone}, {untracked}, '{created_at}', '{updated_at}'\
         ) ON CONFLICT (entity_id, storage_scope_key, version_id, untracked) DO UPDATE SET \
         schema_key = excluded.schema_key, \
         schema_version = excluded.schema_version, \
         file_id = excluded.file_id, \
         global = excluded.global, \
         plugin_key = excluded.plugin_key, \
         snapshot_content = excluded.snapshot_content, \
         change_id = excluded.change_id, \
         metadata = excluded.metadata, \
         is_tombstone = excluded.is_tombstone, \
         updated_at = excluded.updated_at",
        table = REGISTERED_SCHEMA_BOOTSTRAP_TABLE,
        entity_id = escape_sql_string(row.entity_id),
        schema_key = REGISTERED_SCHEMA_KEY,
        schema_version = escape_sql_string(row.schema_version),
        file_id = file_id_sql,
        storage_scope_key = storage_scope_key_sql,
        version_id = escape_sql_string(row.version_id),
        global = if row.version_id == GLOBAL_VERSION_ID {
            "true"
        } else {
            "false"
        },
        plugin_key = plugin_key_sql,
        snapshot_content = snapshot_sql,
        change_id = escape_sql_string(row.change_id),
        metadata = metadata_sql,
        is_tombstone = if row.snapshot_content.is_some() { 0 } else { 1 },
        untracked = if row.untracked { "true" } else { "false" },
        created_at = escape_sql_string(row.created_at),
        updated_at = escape_sql_string(row.created_at),
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}
