use std::collections::BTreeSet;

use crate::schema::live_layout::{normalized_live_column_values, tracked_live_table_name};
use crate::schema::registry::{
    ensure_schema_live_table_in_transaction, load_live_table_layout_in_transaction,
};
use crate::sql::storage::sql_text::escape_sql_string;
use crate::state::live_state::{
    build_mark_live_state_ready_sql, build_set_live_state_mode_sql,
    load_latest_canonical_watermark_in_transaction, LiveStateMode,
};
use crate::state::materialization::types::{
    LiveStateApplyReport, LiveStateRebuildPlan, LiveStateRebuildScope, LiveStateWriteOp,
};
use crate::{LixBackend, LixError, LixTransaction, Value};

pub(crate) async fn apply_live_state_rebuild_plan_internal(
    backend: &dyn LixBackend,
    plan: &LiveStateRebuildPlan,
) -> Result<LiveStateApplyReport, LixError> {
    let mut transaction = backend.begin_transaction().await?;
    transaction
        .execute(
            &build_set_live_state_mode_sql(LiveStateMode::Rebuilding),
            &[],
        )
        .await?;
    let mut tables_touched = BTreeSet::new();

    let mut schema_keys = BTreeSet::new();
    for write in &plan.writes {
        schema_keys.insert(write.schema_key.clone());
    }

    let rows_deleted = clear_scope_rows(
        transaction.as_mut(),
        &schema_keys,
        &plan.scope,
        &mut tables_touched,
    )
    .await?;

    for write in &plan.writes {
        let table_name = tracked_live_table_name(&write.schema_key);
        tables_touched.insert(table_name.clone());

        let is_tombstone = match write.op {
            LiveStateWriteOp::Upsert => 0,
            LiveStateWriteOp::Tombstone => 1,
        };
        let global_sql = if write.global { "true" } else { "false" };
        let metadata_sql = write
            .metadata
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value.as_str())))
            .unwrap_or_else(|| "NULL".to_string());
        let layout =
            load_live_table_layout_in_transaction(transaction.as_mut(), &write.schema_key).await?;
        let normalized_values = normalized_live_column_values_for_write(
            Some(&layout),
            write.snapshot_content.as_deref(),
        )?;
        let normalized_columns_sql = normalized_insert_columns_sql(&normalized_values);
        let normalized_values_sql = normalized_insert_values_sql(&normalized_values);
        let normalized_update_sql = normalized_update_assignments_sql(&normalized_values);

        let sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, change_id, metadata, is_tombstone, created_at, updated_at{normalized_columns}\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', {global}, '{plugin_key}', '{change_id}', {metadata}, {is_tombstone}, '{created_at}', '{updated_at}'{normalized_values}\
             ) ON CONFLICT (entity_id, file_id, version_id) DO UPDATE SET \
             schema_key = excluded.schema_key, \
             schema_version = excluded.schema_version, \
             global = excluded.global, \
             plugin_key = excluded.plugin_key, \
             change_id = excluded.change_id, \
             metadata = excluded.metadata, \
             is_tombstone = excluded.is_tombstone, \
             created_at = excluded.created_at, \
             updated_at = excluded.updated_at{normalized_updates}",
            table = quote_ident(&table_name),
            entity_id = escape_sql_string(&write.entity_id),
            schema_key = escape_sql_string(&write.schema_key),
            schema_version = escape_sql_string(&write.schema_version),
            file_id = escape_sql_string(&write.file_id),
            version_id = escape_sql_string(&write.version_id),
            global = global_sql,
            plugin_key = escape_sql_string(&write.plugin_key),
            change_id = escape_sql_string(&write.change_id),
            metadata = metadata_sql,
            is_tombstone = is_tombstone,
            created_at = escape_sql_string(&write.created_at),
            updated_at = escape_sql_string(&write.updated_at),
            normalized_columns = normalized_columns_sql,
            normalized_values = normalized_values_sql,
            normalized_updates = normalized_update_sql,
        );

        transaction.execute(&sql, &[]).await?;
    }

    if matches!(plan.scope, LiveStateRebuildScope::Full) {
        let Some(watermark) =
            load_latest_canonical_watermark_in_transaction(transaction.as_mut()).await?
        else {
            transaction.rollback().await?;
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "live-state rebuild expected canonical watermark for full rebuild",
            ));
        };
        transaction
            .execute(&build_mark_live_state_ready_sql(&watermark), &[])
            .await?;
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

async fn clear_scope_rows(
    transaction: &mut dyn LixTransaction,
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
        ensure_schema_live_table_in_transaction(transaction, schema_key).await?;
        let table_name = tracked_live_table_name(schema_key);
        tables_touched.insert(table_name.clone());

        let (count_sql, delete_sql) = if let Some(in_list) = version_filter.as_ref() {
            (
                format!(
                    "SELECT COUNT(*) FROM {table_name} WHERE version_id IN ({in_list})",
                    table_name = quote_ident(&table_name),
                    in_list = in_list,
                ),
                format!(
                    "DELETE FROM {table_name} WHERE version_id IN ({in_list})",
                    table_name = quote_ident(&table_name),
                    in_list = in_list,
                ),
            )
        } else {
            (
                format!(
                    "SELECT COUNT(*) FROM {table_name}",
                    table_name = quote_ident(&table_name),
                ),
                format!(
                    "DELETE FROM {table_name}",
                    table_name = quote_ident(&table_name),
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
        .map(|value| format!("'{}'", escape_sql_string(value)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn normalized_live_column_values_for_write(
    layout: Option<&crate::schema::live_layout::LiveTableLayout>,
    snapshot_content: Option<&str>,
) -> Result<Vec<(String, crate::Value)>, LixError> {
    let Some(layout) = layout else {
        return Ok(Vec::new());
    };
    Ok(normalized_live_column_values(layout, snapshot_content)?
        .into_iter()
        .collect())
}

fn normalized_insert_columns_sql(values: &[(String, crate::Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(column, _)| format!(", {}", quote_ident(column)))
        .collect::<String>()
}

fn normalized_insert_values_sql(values: &[(String, crate::Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(_, value)| format!(", {}", sql_literal(value)))
        .collect::<String>()
}

fn normalized_update_assignments_sql(values: &[(String, crate::Value)]) -> String {
    if values.is_empty() {
        return String::new();
    }
    values
        .iter()
        .map(|(column, _)| {
            format!(
                ", {} = excluded.{}",
                quote_ident(column),
                quote_ident(column)
            )
        })
        .collect::<String>()
}

fn sql_literal(value: &crate::Value) -> String {
    match value {
        crate::Value::Null => "NULL".to_string(),
        crate::Value::Boolean(value) => {
            if *value {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        crate::Value::Integer(value) => value.to_string(),
        crate::Value::Real(value) => value.to_string(),
        crate::Value::Text(value) => format!("'{}'", escape_sql_string(value)),
        crate::Value::Json(value) => format!("'{}'", escape_sql_string(&value.to_string())),
        crate::Value::Blob(value) => {
            let hex = value
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>();
            format!("X'{hex}'")
        }
    }
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}
