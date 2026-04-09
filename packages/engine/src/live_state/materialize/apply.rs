use std::collections::BTreeSet;

use super::types::{
    LiveStateApplyReport, LiveStateRebuildPlan, LiveStateRebuildScope, LiveStateWriteOp,
};
use crate::live_state::lifecycle::build_set_live_state_mode_sql;
use crate::live_state::storage::{
    load_live_table_layout_in_transaction, normalized_insert_columns_sql,
    normalized_insert_values_sql, normalized_live_column_values, normalized_update_assignments_sql,
    quoted_live_table_name,
};
use crate::live_state::LiveStateMode;
use crate::live_state::{
    mark_live_state_ready_at_latest_replay_cursor_in_transaction, register_schema_in_transaction,
};
use crate::{LixBackend, LixBackendTransaction, LixError, TransactionMode, Value};

pub(crate) async fn apply_live_state_rebuild_plan_internal(
    backend: &dyn LixBackend,
    plan: &LiveStateRebuildPlan,
) -> Result<LiveStateApplyReport, LixError> {
    let mut transaction = backend.begin_transaction(TransactionMode::Write).await?;
    transaction
        .execute(
            &build_set_live_state_mode_sql(LiveStateMode::Rebuilding),
            &[],
        )
        .await?;
    let (rows_deleted, tables_touched) =
        apply_live_state_scope_in_transaction(transaction.as_mut(), plan).await?;

    if matches!(plan.scope, LiveStateRebuildScope::Full) {
        mark_live_state_ready_at_latest_replay_cursor_in_transaction(transaction.as_mut()).await?;
    } else {
        transaction
            .execute(
                &build_set_live_state_mode_sql(LiveStateMode::NeedsRebuild),
                &[],
            )
            .await?;
    }

    transaction.commit().await?;

    Ok(LiveStateApplyReport {
        run_id: plan.run_id.clone(),
        rows_written: plan.writes.len(),
        rows_deleted,
        tables_touched: tables_touched.into_iter().collect(),
    })
}

pub(crate) async fn apply_live_state_scope_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    plan: &LiveStateRebuildPlan,
) -> Result<(usize, BTreeSet<String>), LixError> {
    let mut tables_touched = BTreeSet::new();

    let mut schema_keys = BTreeSet::new();
    for write in &plan.writes {
        schema_keys.insert(write.schema_key.to_string());
    }

    let rows_deleted =
        clear_scope_rows(transaction, &schema_keys, &plan.scope, &mut tables_touched).await?;

    for write in &plan.writes {
        let table_name = quoted_live_table_name(&write.schema_key);
        tables_touched.insert(table_name.clone());

        let is_tombstone = match write.op {
            LiveStateWriteOp::Upsert => 0,
            LiveStateWriteOp::Tombstone => 1,
        };
        let global_sql = if write.global { "true" } else { "false" };
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
        let writer_key_sql = "NULL".to_string();
        let layout = load_live_table_layout_in_transaction(transaction, &write.schema_key).await?;
        let normalized_values =
            normalized_live_column_values(&layout, write.snapshot_content.as_deref())?
                .into_iter()
                .collect::<Vec<_>>();

        let sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, change_id, metadata, writer_key, is_tombstone, created_at, updated_at{normalized_columns}\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', {global}, '{plugin_key}', '{change_id}', {metadata}, {writer_key}, {is_tombstone}, '{created_at}', '{updated_at}'{normalized_values}\
             ) ON CONFLICT (entity_id, file_id, version_id, untracked) DO UPDATE SET \
             schema_key = excluded.schema_key, \
             schema_version = excluded.schema_version, \
             global = excluded.global, \
             plugin_key = excluded.plugin_key, \
             change_id = excluded.change_id, \
             metadata = excluded.metadata, \
             writer_key = excluded.writer_key, \
             is_tombstone = excluded.is_tombstone, \
             created_at = excluded.created_at, \
             updated_at = excluded.updated_at{normalized_updates}",
            table = table_name,
            entity_id = crate::live_state::constraints::escape_sql_string(&write.entity_id),
            schema_key = crate::live_state::constraints::escape_sql_string(&write.schema_key),
            schema_version =
                crate::live_state::constraints::escape_sql_string(&write.schema_version),
            file_id = crate::live_state::constraints::escape_sql_string(&write.file_id),
            version_id = crate::live_state::constraints::escape_sql_string(&write.version_id),
            global = global_sql,
            plugin_key = crate::live_state::constraints::escape_sql_string(&write.plugin_key),
            change_id = crate::live_state::constraints::escape_sql_string(&write.change_id),
            metadata = metadata_sql,
            writer_key = writer_key_sql,
            is_tombstone = is_tombstone,
            created_at = crate::live_state::constraints::escape_sql_string(&write.created_at),
            updated_at = crate::live_state::constraints::escape_sql_string(&write.updated_at),
            normalized_columns = normalized_insert_columns_sql(&normalized_values),
            normalized_values = normalized_insert_values_sql(&normalized_values),
            normalized_updates = normalized_update_assignments_sql(&normalized_values),
        );

        transaction.execute(&sql, &[]).await?;
    }

    Ok((rows_deleted, tables_touched))
}

async fn clear_scope_rows(
    transaction: &mut dyn LixBackendTransaction,
    schema_keys: &BTreeSet<String>,
    scope: &LiveStateRebuildScope,
    tables_touched: &mut BTreeSet<String>,
) -> Result<usize, LixError> {
    if schema_keys.is_empty() {
        return Ok(0);
    }

    let version_filter = match scope {
        LiveStateRebuildScope::Full => None,
        LiveStateRebuildScope::Versions(versions) if versions.is_empty() => return Ok(0),
        LiveStateRebuildScope::Versions(versions) => Some(in_clause_values(versions)),
    };
    let mut rows_deleted = 0usize;

    for schema_key in schema_keys {
        register_schema_in_transaction(transaction, schema_key.as_str()).await?;
        let table_name = quoted_live_table_name(schema_key);
        tables_touched.insert(table_name.clone());

        let (count_sql, delete_sql) = if let Some(in_list) = version_filter.as_ref() {
            (
                format!(
                    "SELECT COUNT(*) FROM {table_name} \
                     WHERE version_id IN ({in_list}) \
                       AND untracked = false",
                    table_name = table_name,
                    in_list = in_list,
                ),
                format!(
                    "DELETE FROM {table_name} \
                     WHERE version_id IN ({in_list}) \
                       AND untracked = false",
                    table_name = table_name,
                    in_list = in_list,
                ),
            )
        } else {
            (
                format!(
                    "SELECT COUNT(*) FROM {table_name} WHERE untracked = false",
                    table_name = table_name,
                ),
                format!(
                    "DELETE FROM {table_name} WHERE untracked = false",
                    table_name = table_name,
                ),
            )
        };

        let count_result = transaction.execute(&count_sql, &[]).await?;
        rows_deleted += parse_count_result(&count_result.rows)?;

        transaction.execute(&delete_sql, &[]).await?;
    }

    Ok(rows_deleted)
}

fn parse_count_result(rows: &[Vec<Value>]) -> Result<usize, LixError> {
    let Some(value) = rows.first().and_then(|row| row.first()) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "materialization apply: count query returned no rows".to_string(),
        });
    };

    match value {
        Value::Integer(count) if *count >= 0 => Ok(*count as usize),
        Value::Text(text) => text.parse::<usize>().map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "materialization apply: invalid count text '{}': {}",
                text, error
            ),
        }),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "materialization apply: count query returned non-integer value"
                .to_string(),
        }),
    }
}

fn in_clause_values(values: &BTreeSet<String>) -> String {
    values
        .iter()
        .map(|value| {
            format!(
                "'{}'",
                crate::live_state::constraints::escape_sql_string(value)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}
