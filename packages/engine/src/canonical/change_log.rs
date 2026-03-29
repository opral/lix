use crate::backend::prepared::{PreparedBatch, PreparedStatement};
use crate::backend::QueryExecutor;
use crate::functions::LixFunctionProvider;
use crate::sql_support::binding::bind_sql;
use crate::{LixError, SqlDialect, Value};

use super::types::CanonicalCommitOutput;

const SNAPSHOT_TABLE: &str = "lix_internal_snapshot";
const CHANGE_TABLE: &str = "lix_internal_change";
const SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 32_766;
const POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 65_535;
const SNAPSHOT_INSERT_PARAM_COLUMNS: usize = 2;
const CHANGE_INSERT_PARAM_COLUMNS: usize = 10;

#[derive(Debug, Clone)]
struct SnapshotInsertRow {
    id: String,
    content: String,
}

#[derive(Debug, Clone)]
struct CanonicalChangeInsertRow {
    id: String,
    change_ordinal: i64,
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
    starting_change_ordinal: i64,
) -> Result<PreparedBatch, LixError> {
    let mut ensure_no_content = false;
    let mut snapshot_rows = Vec::new();
    let mut change_rows = Vec::new();

    for (index, change) in canonical_output.changes.iter().enumerate() {
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
            change_ordinal: starting_change_ordinal + index as i64,
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
            "change_ordinal",
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
                bigint_param_value(row.change_ordinal, next_placeholder, params),
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

pub(crate) async fn load_next_change_ordinal_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<i64, LixError> {
    let result = executor
        .execute(
            "SELECT COALESCE(MAX(change_ordinal), -1) + 1 FROM lix_internal_change",
            &[],
        )
        .await?;
    let Some(row) = result.rows.first() else {
        return Ok(0);
    };
    match row.first() {
        Some(Value::Integer(value)) => Ok(*value),
        Some(Value::Text(value)) => value.parse::<i64>().map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("next canonical change ordinal is invalid text: {error}"),
        }),
        Some(other) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "next canonical change ordinal returned non-integer value: {other:?}"
            ),
        }),
        None => Ok(0),
    }
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

fn bigint_param_value(value: i64, next_placeholder: &mut usize, params: &mut Vec<Value>) -> String {
    let index = *next_placeholder;
    *next_placeholder += 1;
    params.push(Value::Integer(value));
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
