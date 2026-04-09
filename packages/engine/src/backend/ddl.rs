use crate::{LixBackend, LixError, SqlDialect, Value};

pub(crate) async fn execute_ddl_batch(
    backend: &dyn LixBackend,
    component: &str,
    statements: &[&str],
) -> Result<(), LixError> {
    for (index, statement) in statements.iter().enumerate() {
        backend.execute(statement, &[]).await.map_err(|error| {
            LixError::new(
                &error.code,
                format!(
                    "{component} init statement #{index} failed: {} :: {}",
                    compact_sql(statement),
                    error.description
                ),
            )
        })?;
    }
    Ok(())
}

pub(crate) async fn add_column_if_missing(
    backend: &dyn LixBackend,
    table: &str,
    column: &str,
    column_ddl: &str,
) -> Result<(), LixError> {
    if column_exists(backend, table, column).await? {
        return Ok(());
    }

    let alter = format!("ALTER TABLE {table} ADD COLUMN {column} {column_ddl}");
    backend.execute(&alter, &[]).await?;
    Ok(())
}

pub(crate) async fn table_exists(backend: &dyn LixBackend, table: &str) -> Result<bool, LixError> {
    let exists = match backend.dialect() {
        SqlDialect::Sqlite => {
            backend
                .execute(
                    "SELECT 1 \
                     FROM sqlite_master \
                     WHERE type = 'table' \
                       AND name = $1 \
                     LIMIT 1",
                    &[Value::Text(table.to_string())],
                )
                .await?
        }
        SqlDialect::Postgres => {
            backend
                .execute(
                    "SELECT 1 \
                     FROM information_schema.tables \
                     WHERE table_schema = current_schema() \
                       AND table_name = $1 \
                     LIMIT 1",
                    &[Value::Text(table.to_string())],
                )
                .await?
        }
    };
    Ok(!exists.rows.is_empty())
}

async fn column_exists(
    backend: &dyn LixBackend,
    table: &str,
    column: &str,
) -> Result<bool, LixError> {
    let exists = match backend.dialect() {
        SqlDialect::Sqlite => {
            backend
                .execute(
                    &format!(
                        "SELECT 1 \
                         FROM pragma_table_info('{table}') \
                         WHERE name = $1 \
                         LIMIT 1"
                    ),
                    &[Value::Text(column.to_string())],
                )
                .await?
        }
        SqlDialect::Postgres => {
            backend
                .execute(
                    "SELECT 1 \
                     FROM information_schema.columns \
                     WHERE table_schema = current_schema() \
                       AND table_name = $1 \
                       AND column_name = $2 \
                     LIMIT 1",
                    &[
                        Value::Text(table.to_string()),
                        Value::Text(column.to_string()),
                    ],
                )
                .await?
        }
    };
    Ok(!exists.rows.is_empty())
}

fn compact_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}
