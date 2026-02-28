use std::collections::BTreeSet;

use crate::engine::sql::storage::sql_text::escape_sql_string;
use crate::materialization::types::{
    MaterializationApplyReport, MaterializationPlan, MaterializationScope, MaterializationWriteOp,
};
use crate::schema_registry::register_schema;
use crate::{LixBackend, LixError, Value};

pub(crate) async fn apply_materialization_plan_internal(
    backend: &dyn LixBackend,
    plan: &MaterializationPlan,
) -> Result<MaterializationApplyReport, LixError> {
    let mut tables_touched = BTreeSet::new();

    let mut schema_keys = BTreeSet::new();
    for write in &plan.writes {
        schema_keys.insert(write.schema_key.clone());
    }

    let rows_deleted =
        clear_scope_rows(backend, &schema_keys, &plan.scope, &mut tables_touched).await?;

    for write in &plan.writes {
        let table_name = materialized_table_name(&write.schema_key);
        tables_touched.insert(table_name.clone());

        let is_tombstone = match write.op {
            MaterializationWriteOp::Upsert => 0,
            MaterializationWriteOp::Tombstone => 1,
        };
        let snapshot_sql = write
            .snapshot_content
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let inherited_from_version_sql = write
            .inherited_from_version_id
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let metadata_sql = write
            .metadata
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());

        let sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content, inherited_from_version_id, change_id, metadata, is_tombstone, created_at, updated_at\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', '{plugin_key}', {snapshot_content}, {inherited_from_version_id}, '{change_id}', {metadata}, {is_tombstone}, '{created_at}', '{updated_at}'\
             ) ON CONFLICT (entity_id, file_id, version_id) DO UPDATE SET \
             schema_key = excluded.schema_key, \
             schema_version = excluded.schema_version, \
             plugin_key = excluded.plugin_key, \
             snapshot_content = excluded.snapshot_content, \
             inherited_from_version_id = excluded.inherited_from_version_id, \
             change_id = excluded.change_id, \
             metadata = excluded.metadata, \
             is_tombstone = excluded.is_tombstone, \
             created_at = excluded.created_at, \
             updated_at = excluded.updated_at",
            table = quote_ident(&table_name),
            entity_id = escape_sql_string(&write.entity_id),
            schema_key = escape_sql_string(&write.schema_key),
            schema_version = escape_sql_string(&write.schema_version),
            file_id = escape_sql_string(&write.file_id),
            version_id = escape_sql_string(&write.version_id),
            plugin_key = escape_sql_string(&write.plugin_key),
            snapshot_content = snapshot_sql,
            inherited_from_version_id = inherited_from_version_sql,
            change_id = escape_sql_string(&write.change_id),
            metadata = metadata_sql,
            is_tombstone = is_tombstone,
            created_at = escape_sql_string(&write.created_at),
            updated_at = escape_sql_string(&write.updated_at),
        );

        backend.execute(&sql, &[]).await?;
    }

    Ok(MaterializationApplyReport {
        run_id: plan.run_id.clone(),
        rows_written: plan.writes.len(),
        rows_deleted,
        tables_touched: tables_touched.into_iter().collect(),
    })
}

async fn clear_scope_rows(
    backend: &dyn LixBackend,
    schema_keys: &BTreeSet<String>,
    scope: &MaterializationScope,
    tables_touched: &mut BTreeSet<String>,
) -> Result<usize, LixError> {
    if schema_keys.is_empty() {
        return Ok(0);
    }

    let version_filter = match scope {
        MaterializationScope::Full => None,
        MaterializationScope::Versions(versions) if versions.is_empty() => return Ok(0),
        MaterializationScope::Versions(versions) => Some(in_clause_values(versions)),
    };
    let mut rows_deleted = 0usize;

    for schema_key in schema_keys {
        register_schema(backend, schema_key).await?;
        let table_name = materialized_table_name(schema_key);
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

        let count_result = backend.execute(&count_sql, &[]).await?;
        rows_deleted += parse_count_result(&count_result.rows)?;

        backend.execute(&delete_sql, &[]).await?;
    }

    Ok(rows_deleted)
}

fn parse_count_result(rows: &[Vec<Value>]) -> Result<usize, LixError> {
    let Some(value) = rows.first().and_then(|row| row.first()) else {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "materialization apply: count query returned no rows".to_string(),
        });
    };

    match value {
        Value::Integer(count) if *count >= 0 => Ok(*count as usize),
        Value::Text(text) => text.parse::<usize>().map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!(
                "materialization apply: invalid count text '{}': {}",
                text, error
            ),
        }),
        _ => Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "materialization apply: count query returned non-integer value".to_string(),
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

fn materialized_table_name(schema_key: &str) -> String {
    format!("lix_internal_state_materialized_v1_{}", schema_key)
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}
