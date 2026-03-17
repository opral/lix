use std::collections::BTreeMap;

use crate::backend::QueryExecutor;
use crate::errors::classification::is_missing_relation_error;
use crate::schema::live_layout::{
    json_value_from_live_row_cell, load_live_row_access_with_executor, tracked_live_table_name,
    untracked_live_table_name, LiveColumnKind, LiveRowAccess,
};
use crate::{LixError, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveRowScope {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoadedLiveRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) plugin_key: String,
    pub(crate) metadata: Option<String>,
    pub(crate) change_id: Option<String>,
    pub(crate) values: BTreeMap<String, Value>,
}

impl LoadedLiveRow {
    pub(crate) fn property_text(&self, property_name: &str) -> Option<String> {
        self.values.get(property_name).and_then(text_from_value)
    }
}

pub(crate) async fn load_exact_live_row_with_executor(
    executor: &mut dyn QueryExecutor,
    scope: LiveRowScope,
    schema_key: &str,
    filters: &BTreeMap<&str, String>,
) -> Result<Option<LoadedLiveRow>, LixError> {
    let result =
        load_live_rows_with_executor(executor, scope, schema_key, filters, &[], Some(2)).await?;
    if result.is_empty() {
        return Ok(None);
    }
    if result.len() > 1 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!(
                "expected at most one live row for schema '{}' but found {}",
                schema_key,
                result.len()
            ),
        ));
    }
    Ok(result.into_iter().next())
}

pub(crate) async fn load_live_rows_with_executor(
    executor: &mut dyn QueryExecutor,
    scope: LiveRowScope,
    schema_key: &str,
    filters: &BTreeMap<&str, String>,
    order_by: &[&str],
    limit: Option<usize>,
) -> Result<Vec<LoadedLiveRow>, LixError> {
    let access = load_live_row_access_with_executor(executor, schema_key).await?;
    let normalized_projection = access.normalized_projection_sql(None);
    let envelope_projection = match scope {
        LiveRowScope::Tracked => {
            "entity_id, schema_key, schema_version, file_id, version_id, plugin_key, metadata, change_id"
        }
        LiveRowScope::Untracked => {
            "entity_id, schema_key, schema_version, file_id, version_id, plugin_key, metadata"
        }
    };
    let table_name = match scope {
        LiveRowScope::Tracked => tracked_live_table_name(schema_key),
        LiveRowScope::Untracked => untracked_live_table_name(schema_key),
    };

    let mut sql = format!(
        "SELECT {envelope_projection}{normalized_projection} \
         FROM {table_name} \
         WHERE schema_key = '{schema_key}'",
        envelope_projection = envelope_projection,
        normalized_projection = normalized_projection,
        table_name = quote_ident(&table_name),
        schema_key = escape_sql_string(schema_key),
    );
    if matches!(scope, LiveRowScope::Tracked) {
        sql.push_str(" AND is_tombstone = 0");
    }
    for (column, value) in filters {
        sql.push_str(&format!(
            " AND {column} = '{value}'",
            column = quote_ident(column),
            value = escape_sql_string(value),
        ));
    }
    if !order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(
            &order_by
                .iter()
                .map(|column| quote_ident(column))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }

    let result = match executor.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(err) if is_missing_relation_error(&err) => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    result
        .rows
        .iter()
        .map(|row| decode_loaded_live_row(row, &access, scope, schema_key))
        .collect()
}

pub(crate) fn logical_snapshot_text(
    access: &LiveRowAccess,
    row: &LoadedLiveRow,
) -> Result<Option<String>, LixError> {
    let mut object = serde_json::Map::new();
    for column in access.columns() {
        let Some(value) = row.values.get(&column.property_name) else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "loaded live row for schema '{}' missing property '{}'",
                    row.schema_key, column.property_name
                ),
            ));
        };
        let json_value = json_value_from_live_row_cell(
            value,
            live_column_kind(column),
            &row.schema_key,
            &column.column_name,
        )?;
        if !json_value.is_null() {
            object.insert(column.property_name.clone(), json_value);
        }
    }
    serde_json::to_string(&serde_json::Value::Object(object))
        .map(Some)
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "failed to serialize logical live snapshot for schema '{}': {error}",
                    row.schema_key
                ),
            )
        })
}

pub(crate) async fn load_untracked_live_rows_by_property_with_executor(
    executor: &mut dyn QueryExecutor,
    schema_key: &str,
    property_name: &str,
    filters: &BTreeMap<&str, String>,
    require_non_null: bool,
    order_by: &[&str],
) -> Result<Vec<LoadedLiveRow>, LixError> {
    let access = load_live_row_access_with_executor(executor, schema_key).await?;
    let payload_column = access
        .payload_column_name(property_name)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "live layout for schema '{}' is missing property '{}'",
                    schema_key, property_name
                ),
            )
        })?
        .to_string();
    let normalized_projection = access.normalized_projection_sql(None);
    let mut sql = format!(
        "SELECT entity_id, schema_key, schema_version, file_id, version_id, plugin_key, metadata{normalized_projection} \
         FROM {table_name} \
         WHERE schema_key = '{schema_key}'",
        normalized_projection = normalized_projection,
        table_name = quote_ident(&untracked_live_table_name(schema_key)),
        schema_key = escape_sql_string(schema_key),
    );
    for (column, value) in filters {
        sql.push_str(&format!(
            " AND {column} = '{value}'",
            column = quote_ident(column),
            value = escape_sql_string(value),
        ));
    }
    if require_non_null {
        sql.push_str(&format!(
            " AND {} IS NOT NULL",
            quote_ident(&payload_column)
        ));
    }
    if order_by.is_empty() {
        sql.push_str(" ORDER BY entity_id ASC");
    } else {
        sql.push_str(" ORDER BY ");
        sql.push_str(
            &order_by
                .iter()
                .map(|column| quote_ident(column))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }

    let result = match executor.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(err) if is_missing_relation_error(&err) => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    result
        .rows
        .iter()
        .map(|row| decode_loaded_live_row(row, &access, LiveRowScope::Untracked, schema_key))
        .collect()
}

fn decode_loaded_live_row(
    row: &[Value],
    access: &LiveRowAccess,
    scope: LiveRowScope,
    schema_key: &str,
) -> Result<LoadedLiveRow, LixError> {
    let entity_id = required_text_cell(row, 0, schema_key, "entity_id")?;
    let schema_key_value = required_text_cell(row, 1, schema_key, "schema_key")?;
    let schema_version = required_text_cell(row, 2, schema_key, "schema_version")?;
    let file_id = required_text_cell(row, 3, schema_key, "file_id")?;
    let version_id = required_text_cell(row, 4, schema_key, "version_id")?;
    let plugin_key = required_text_cell(row, 5, schema_key, "plugin_key")?;
    let metadata = row.get(6).and_then(text_from_value);
    let change_id = match scope {
        LiveRowScope::Tracked => row.get(7).and_then(text_from_value),
        LiveRowScope::Untracked => None,
    };
    let normalized_start_index = match scope {
        LiveRowScope::Tracked => 8,
        LiveRowScope::Untracked => 7,
    };
    let mut values = BTreeMap::new();
    for (offset, column) in access.columns().iter().enumerate() {
        let value = row.get(normalized_start_index + offset).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "loaded live row for schema '{}' missing column '{}'",
                    schema_key, column.column_name
                ),
            )
        })?;
        values.insert(column.property_name.clone(), value.clone());
    }
    Ok(LoadedLiveRow {
        entity_id,
        schema_key: schema_key_value,
        schema_version,
        file_id,
        version_id,
        plugin_key,
        metadata,
        change_id,
        values,
    })
}

fn live_column_kind(column: &crate::schema::live_layout::LiveColumnSpec) -> LiveColumnKind {
    column.kind
}

fn required_text_cell(
    row: &[Value],
    index: usize,
    schema_key: &str,
    column_name: &str,
) -> Result<String, LixError> {
    row.get(index).and_then(text_from_value).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!(
                "loaded live row for schema '{}' missing text column '{}'",
                schema_key, column_name
            ),
        )
    })
}

fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        Value::Integer(integer) => Some(integer.to_string()),
        Value::Boolean(boolean) => Some(boolean.to_string()),
        Value::Real(real) => Some(real.to_string()),
        _ => None,
    }
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
