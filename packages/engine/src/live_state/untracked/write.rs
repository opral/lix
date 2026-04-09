use crate::live_state::storage::{
    load_live_table_layout_with_executor, normalized_insert_columns_sql,
    normalized_insert_values_sql, normalized_live_column_values, normalized_update_assignments_sql,
    quoted_live_table_name,
};
use crate::{LixBackendTransaction, LixError};

use super::contracts::{UntrackedWriteOperation, UntrackedWriteRow};

pub(crate) async fn apply_write_batch_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    batch: &[UntrackedWriteRow],
) -> Result<(), LixError> {
    if batch.is_empty() {
        return Ok(());
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
    transaction: &mut dyn LixBackendTransaction,
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
        .map(crate::live_state::constraints::sql_literal_text)
        .unwrap_or_else(|| "NULL".to_string());
    let sql = format!(
        "INSERT INTO {table} (\
         entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, metadata, untracked, created_at, updated_at{normalized_columns}\
         ) VALUES (\
         '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', {global}, '{plugin_key}', {metadata}, true, '{created_at}', '{updated_at}'{normalized_values}\
         ) ON CONFLICT (entity_id, file_id, version_id, untracked) DO UPDATE SET \
         schema_key = excluded.schema_key, \
         schema_version = excluded.schema_version, \
         global = excluded.global, \
         plugin_key = excluded.plugin_key, \
         metadata = excluded.metadata, \
         updated_at = excluded.updated_at{normalized_updates}",
        table = quoted_live_table_name(&row.schema_key),
        entity_id = crate::live_state::constraints::escape_sql_string(&row.entity_id),
        schema_key = crate::live_state::constraints::escape_sql_string(&row.schema_key),
        schema_version = crate::live_state::constraints::escape_sql_string(&row.schema_version),
        file_id = crate::live_state::constraints::escape_sql_string(&row.file_id),
        version_id = crate::live_state::constraints::escape_sql_string(&row.version_id),
        global = if row.global { "true" } else { "false" },
        plugin_key = crate::live_state::constraints::escape_sql_string(&row.plugin_key),
        metadata = metadata_sql,
        created_at = crate::live_state::constraints::escape_sql_string(created_at),
        updated_at = crate::live_state::constraints::escape_sql_string(&row.updated_at),
        normalized_columns = normalized_columns,
        normalized_values = normalized_values_sql,
        normalized_updates = normalized_updates,
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

async fn apply_delete_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    row: &UntrackedWriteRow,
) -> Result<(), LixError> {
    let sql = format!(
        "DELETE FROM {table} \
         WHERE entity_id = '{entity_id}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}' \
           AND untracked = true",
        table = quoted_live_table_name(&row.schema_key),
        entity_id = crate::live_state::constraints::escape_sql_string(&row.entity_id),
        file_id = crate::live_state::constraints::escape_sql_string(&row.file_id),
        version_id = crate::live_state::constraints::escape_sql_string(&row.version_id),
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}
