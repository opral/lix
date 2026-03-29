//! Append/write helpers for canonical change facts.
//!
//! Despite the historical `change_log` name, this module is not the semantic
//! model boundary. It persists canonical changes into local storage so replay
//! systems can rebuild derived state from them later.

use crate::backend::prepared::{PreparedBatch, PreparedStatement};
use crate::functions::LixFunctionProvider;
use crate::sql::binder::bind_sql;
use crate::{LixError, SqlDialect, Value};

use super::types::CanonicalCommitOutput;

const SNAPSHOT_TABLE: &str = "lix_internal_snapshot";
const CHANGE_TABLE: &str = "lix_internal_change";
const SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 32_766;
const POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 65_535;
const SNAPSHOT_INSERT_PARAM_COLUMNS: usize = 2;
const CHANGE_INSERT_PARAM_COLUMNS: usize = 9;

#[derive(Debug, Clone)]
struct SnapshotInsertRow {
    id: String,
    content: String,
}

#[derive(Debug, Clone)]
struct CanonicalChangeInsertRow {
    id: String,
    entity_id: String,
    schema_key: String,
    schema_version: String,
    file_id: String,
    plugin_key: String,
    snapshot_id: String,
    metadata: Option<String>,
    created_at: String,
}

pub(crate) fn build_prepared_batch_from_canonical_output(
    canonical_output: &CanonicalCommitOutput,
    functions: &mut dyn LixFunctionProvider,
    dialect: SqlDialect,
) -> Result<PreparedBatch, LixError> {
    let mut ensure_no_content = false;
    let mut snapshot_rows = Vec::new();
    let mut change_rows = Vec::new();

    for change in &canonical_output.changes {
        let snapshot_id = match &change.snapshot_content {
            Some(content) => {
                let id = functions.uuid_v7();
                snapshot_rows.push(SnapshotInsertRow {
                    id: id.clone(),
                    content: content.as_str().to_string(),
                });
                id
            }
            None => {
                ensure_no_content = true;
                "no-content".to_string()
            }
        };

        change_rows.push(CanonicalChangeInsertRow {
            id: change.id.clone(),
            entity_id: change.entity_id.to_string(),
            schema_key: change.schema_key.to_string(),
            schema_version: change.schema_version.to_string(),
            file_id: change.file_id.to_string(),
            plugin_key: change.plugin_key.to_string(),
            snapshot_id,
            metadata: change
                .metadata
                .as_ref()
                .map(|value| value.as_str().to_string()),
            created_at: change.created_at.clone(),
        });
    }

    let mut prepared = PreparedBatch { steps: Vec::new() };

    if ensure_no_content {
        push_bound_statement(
            &mut prepared,
            "INSERT INTO lix_internal_snapshot (id, content) VALUES (?1, NULL) \
             ON CONFLICT (id) DO UPDATE SET content = excluded.content",
            vec![Value::Text("no-content".to_string())],
            dialect,
        )?;
    }

    push_chunked_prepared_insert_statements(
        &mut prepared,
        SNAPSHOT_TABLE,
        &["id", "content"],
        &snapshot_rows,
        Some("ON CONFLICT (id) DO UPDATE SET content = excluded.content"),
        max_rows_per_insert_for_dialect(dialect, SNAPSHOT_INSERT_PARAM_COLUMNS),
        dialect,
        |row, next_placeholder, params| {
            vec![
                text_param_value(&row.id, next_placeholder, params),
                text_param_value(&row.content, next_placeholder, params),
            ]
        },
    )?;

    push_chunked_prepared_insert_statements(
        &mut prepared,
        CHANGE_TABLE,
        &[
            "id",
            "entity_id",
            "schema_key",
            "schema_version",
            "file_id",
            "plugin_key",
            "snapshot_id",
            "metadata",
            "created_at",
        ],
        &change_rows,
        None,
        max_rows_per_insert_for_dialect(dialect, CHANGE_INSERT_PARAM_COLUMNS),
        dialect,
        |row, next_placeholder, params| {
            vec![
                text_param_value(&row.id, next_placeholder, params),
                text_param_value(&row.entity_id, next_placeholder, params),
                text_param_value(&row.schema_key, next_placeholder, params),
                text_param_value(&row.schema_version, next_placeholder, params),
                text_param_value(&row.file_id, next_placeholder, params),
                text_param_value(&row.plugin_key, next_placeholder, params),
                text_param_value(&row.snapshot_id, next_placeholder, params),
                optional_text_param_value(row.metadata.as_deref(), next_placeholder, params),
                text_param_value(&row.created_at, next_placeholder, params),
            ]
        },
    )?;

    Ok(prepared)
}

fn max_bind_parameters_for_dialect(dialect: SqlDialect) -> usize {
    match dialect {
        SqlDialect::Sqlite => SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT,
        SqlDialect::Postgres => POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT,
    }
}

fn max_rows_per_insert_for_dialect(dialect: SqlDialect, params_per_row: usize) -> usize {
    (max_bind_parameters_for_dialect(dialect) / params_per_row).max(1)
}

fn push_chunked_prepared_insert_statements<Row, F>(
    prepared: &mut PreparedBatch,
    table: &str,
    columns: &[&str],
    rows: &[Row],
    on_conflict_sql: Option<&str>,
    max_rows_per_statement: usize,
    dialect: SqlDialect,
    mut build_row_values: F,
) -> Result<(), LixError>
where
    F: FnMut(&Row, &mut usize, &mut Vec<Value>) -> Vec<String>,
{
    if rows.is_empty() {
        return Ok(());
    }

    for chunk in rows.chunks(max_rows_per_statement.max(1)) {
        let mut params = Vec::new();
        let mut next_placeholder = 1usize;
        let values_sql = chunk
            .iter()
            .map(|row| {
                format!(
                    "({})",
                    build_row_values(row, &mut next_placeholder, &mut params).join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!(
            "INSERT INTO {table} ({columns}) VALUES {values_sql}",
            table = table,
            columns = columns.join(", "),
            values_sql = values_sql,
        );
        if let Some(on_conflict_sql) = on_conflict_sql {
            sql.push(' ');
            sql.push_str(on_conflict_sql);
        }
        push_bound_statement(prepared, &sql, params, dialect)?;
    }

    Ok(())
}

fn push_bound_statement(
    prepared: &mut PreparedBatch,
    sql: &str,
    params: Vec<Value>,
    dialect: SqlDialect,
) -> Result<(), LixError> {
    let bound = bind_sql(sql, &params, dialect)?;
    prepared.push_statement(PreparedStatement {
        sql: bound.sql,
        params: bound.params,
    });
    Ok(())
}

fn text_param_value(value: &str, next_placeholder: &mut usize, params: &mut Vec<Value>) -> String {
    let index = *next_placeholder;
    *next_placeholder += 1;
    params.push(Value::Text(value.to_string()));
    format!("?{index}")
}

fn optional_text_param_value(
    value: Option<&str>,
    next_placeholder: &mut usize,
    params: &mut Vec<Value>,
) -> String {
    match value {
        Some(value) => text_param_value(value, next_placeholder, params),
        None => "NULL".to_string(),
    }
}
