use std::collections::BTreeSet;

use crate::schema::live_layout::{
    load_live_table_layout_with_executor, normalized_live_column_values, tracked_live_table_name,
};
use crate::schema::registry::{ensure_schema_live_table, ensure_schema_live_table_in_transaction};
use crate::version::{
    active_version_file_id, active_version_plugin_key, active_version_schema_key,
    active_version_schema_version, active_version_snapshot_content,
    active_version_storage_version_id, version_ref_file_id, version_ref_plugin_key,
    version_ref_schema_key, version_ref_schema_version, version_ref_snapshot_content,
    version_ref_storage_version_id,
};
use crate::{LixBackend, LixError, LixTransaction};

use super::contracts::{UntrackedWriteOperation, UntrackedWriteRow};
use super::shared::{
    escape_sql_string, normalized_insert_columns_sql, normalized_insert_values_sql,
    normalized_update_assignments_sql, quote_ident, sql_literal_text,
};

pub async fn ensure_storage_with_backend(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<(), LixError> {
    ensure_schema_live_table(backend, schema_key).await
}

pub async fn apply_write_batch_with_backend(
    backend: &dyn LixBackend,
    batch: &[UntrackedWriteRow],
) -> Result<(), LixError> {
    if batch.is_empty() {
        return Ok(());
    }

    let mut transaction = backend.begin_transaction().await?;
    let result = apply_write_batch_in_transaction(transaction.as_mut(), batch).await;
    match result {
        Ok(()) => transaction.commit().await,
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

pub fn active_version_write_row(
    entity_id: &str,
    version_id: &str,
    timestamp: &str,
) -> UntrackedWriteRow {
    UntrackedWriteRow {
        entity_id: entity_id.to_string(),
        schema_key: active_version_schema_key().to_string(),
        schema_version: active_version_schema_version().to_string(),
        file_id: active_version_file_id().to_string(),
        version_id: active_version_storage_version_id().to_string(),
        global: true,
        plugin_key: active_version_plugin_key().to_string(),
        metadata: None,
        writer_key: None,
        snapshot_content: Some(active_version_snapshot_content(entity_id, version_id)),
        created_at: Some(timestamp.to_string()),
        updated_at: timestamp.to_string(),
        operation: UntrackedWriteOperation::Upsert,
    }
}

pub fn version_ref_write_row(
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> UntrackedWriteRow {
    UntrackedWriteRow {
        entity_id: version_id.to_string(),
        schema_key: version_ref_schema_key().to_string(),
        schema_version: version_ref_schema_version().to_string(),
        file_id: version_ref_file_id().to_string(),
        version_id: version_ref_storage_version_id().to_string(),
        global: true,
        plugin_key: version_ref_plugin_key().to_string(),
        metadata: None,
        writer_key: None,
        snapshot_content: Some(version_ref_snapshot_content(version_id, commit_id)),
        created_at: Some(timestamp.to_string()),
        updated_at: timestamp.to_string(),
        operation: UntrackedWriteOperation::Upsert,
    }
}

pub(crate) async fn ensure_storage_in_transaction(
    transaction: &mut dyn LixTransaction,
    schema_key: &str,
) -> Result<(), LixError> {
    ensure_schema_live_table_in_transaction(transaction, schema_key).await
}

pub(crate) async fn apply_write_batch_in_transaction(
    transaction: &mut dyn LixTransaction,
    batch: &[UntrackedWriteRow],
) -> Result<(), LixError> {
    if batch.is_empty() {
        return Ok(());
    }

    let mut schemas = BTreeSet::new();
    for row in batch {
        schemas.insert(row.schema_key.clone());
    }
    for schema_key in schemas {
        ensure_storage_in_transaction(transaction, &schema_key).await?;
    }

    for row in batch {
        match row.operation {
            UntrackedWriteOperation::Upsert => {
                apply_upsert_in_transaction(transaction, row).await?
            }
            UntrackedWriteOperation::Delete => {
                apply_delete_in_transaction(transaction, row).await?
            }
        }
    }

    Ok(())
}

async fn apply_upsert_in_transaction(
    transaction: &mut dyn LixTransaction,
    row: &UntrackedWriteRow,
) -> Result<(), LixError> {
    let layout = {
        let mut executor = &mut *transaction;
        load_live_table_layout_with_executor(&mut executor, &row.schema_key).await?
    };
    let normalized_values =
        normalized_live_column_values(&layout, row.snapshot_content.as_deref())?
            .into_iter()
            .collect::<Vec<_>>();
    let normalized_columns = normalized_insert_columns_sql(&normalized_values);
    let normalized_values_sql = normalized_insert_values_sql(&normalized_values);
    let normalized_updates = normalized_update_assignments_sql(&normalized_values);
    let created_at = row.created_at.as_deref().unwrap_or(&row.updated_at);
    let metadata_sql = row
        .metadata
        .as_deref()
        .map(sql_literal_text)
        .unwrap_or_else(|| "NULL".to_string());
    let writer_key_sql = row
        .writer_key
        .as_deref()
        .map(sql_literal_text)
        .unwrap_or_else(|| "NULL".to_string());
    let sql = format!(
        "INSERT INTO {table} (\
         entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, metadata, writer_key, untracked, created_at, updated_at{normalized_columns}\
         ) VALUES (\
         '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', {global}, '{plugin_key}', {metadata}, {writer_key}, true, '{created_at}', '{updated_at}'{normalized_values}\
         ) ON CONFLICT (entity_id, file_id, version_id, untracked) DO UPDATE SET \
         schema_key = excluded.schema_key, \
         schema_version = excluded.schema_version, \
         global = excluded.global, \
         plugin_key = excluded.plugin_key, \
         metadata = excluded.metadata, \
         writer_key = excluded.writer_key, \
         updated_at = excluded.updated_at{normalized_updates}",
        table = quote_ident(&tracked_live_table_name(&row.schema_key)),
        entity_id = escape_sql_string(&row.entity_id),
        schema_key = escape_sql_string(&row.schema_key),
        schema_version = escape_sql_string(&row.schema_version),
        file_id = escape_sql_string(&row.file_id),
        version_id = escape_sql_string(&row.version_id),
        global = if row.global { "true" } else { "false" },
        plugin_key = escape_sql_string(&row.plugin_key),
        metadata = metadata_sql,
        writer_key = writer_key_sql,
        created_at = escape_sql_string(created_at),
        updated_at = escape_sql_string(&row.updated_at),
        normalized_columns = normalized_columns,
        normalized_values = normalized_values_sql,
        normalized_updates = normalized_updates,
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

async fn apply_delete_in_transaction(
    transaction: &mut dyn LixTransaction,
    row: &UntrackedWriteRow,
) -> Result<(), LixError> {
    let sql = format!(
        "DELETE FROM {table} \
         WHERE entity_id = '{entity_id}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}' \
           AND untracked = true",
        table = quote_ident(&tracked_live_table_name(&row.schema_key)),
        entity_id = escape_sql_string(&row.entity_id),
        file_id = escape_sql_string(&row.file_id),
        version_id = escape_sql_string(&row.version_id),
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}
