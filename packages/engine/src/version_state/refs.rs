use crate::backend::QueryExecutor;
use crate::common::errors::classification::is_missing_relation_error;
use crate::{LixError, SqlDialect, Value};
use std::collections::BTreeMap;

use super::{
    version_ref_file_id, version_ref_plugin_key, version_ref_schema_key,
    version_ref_schema_version, version_ref_storage_version_id,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalVersionRefRow {
    pub(crate) version_id: String,
    pub(crate) commit_id: String,
}

pub(crate) async fn load_local_version_ref_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<LocalVersionRefRow>, LixError> {
    if !local_version_ref_relation_exists_with_executor(executor).await? {
        return Ok(None);
    }

    let sql = format!(
        "SELECT entity_id, commit_id \
         FROM {table} \
         WHERE schema_key = $1 \
           AND schema_version = $2 \
           AND file_id = $3 \
           AND version_id = $4 \
           AND plugin_key = $5 \
           AND entity_id = $6 \
           AND untracked = true \
           AND is_tombstone = 0 \
           AND commit_id IS NOT NULL \
           AND commit_id <> '' \
         ORDER BY updated_at DESC \
         LIMIT 2",
        table = local_version_ref_relation_name(),
    );
    let result = match executor
        .execute(
            &sql,
            &[
                Value::Text(version_ref_schema_key().to_string()),
                Value::Text(version_ref_schema_version().to_string()),
                Value::Text(version_ref_file_id().to_string()),
                Value::Text(version_ref_storage_version_id().to_string()),
                Value::Text(version_ref_plugin_key().to_string()),
                Value::Text(version_id.to_string()),
            ],
        )
        .await
    {
        Ok(result) => result,
        Err(error) if is_missing_relation_error(&error) => return Ok(None),
        Err(error) => return Err(error),
    };

    if result.rows.len() > 1 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "expected at most one untracked row for schema '{}' entity '{}' version '{}'",
                version_ref_schema_key(),
                version_id,
                version_ref_storage_version_id()
            ),
        ));
    }

    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    Ok(Some(parse_local_version_ref_row(row)?))
}

pub(crate) async fn load_local_version_head_commit_id_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    Ok(load_local_version_ref_with_executor(executor, version_id)
        .await?
        .map(|row| row.commit_id))
}

pub(crate) async fn load_all_local_version_refs_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<LocalVersionRefRow>, LixError> {
    if !local_version_ref_relation_exists_with_executor(executor).await? {
        return Ok(Vec::new());
    }

    let sql = format!(
        "SELECT entity_id, commit_id \
         FROM {table} \
         WHERE schema_key = $1 \
           AND schema_version = $2 \
           AND file_id = $3 \
           AND version_id = $4 \
           AND plugin_key = $5 \
           AND untracked = true \
           AND is_tombstone = 0 \
           AND commit_id IS NOT NULL \
           AND commit_id <> '' \
         ORDER BY entity_id ASC, updated_at DESC",
        table = local_version_ref_relation_name(),
    );
    let result = match executor
        .execute(
            &sql,
            &[
                Value::Text(version_ref_schema_key().to_string()),
                Value::Text(version_ref_schema_version().to_string()),
                Value::Text(version_ref_file_id().to_string()),
                Value::Text(version_ref_storage_version_id().to_string()),
                Value::Text(version_ref_plugin_key().to_string()),
            ],
        )
        .await
    {
        Ok(result) => result,
        Err(error) if is_missing_relation_error(&error) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };

    let mut rows = Vec::with_capacity(result.rows.len());
    let mut previous_version_id: Option<String> = None;
    for row in &result.rows {
        let parsed = parse_local_version_ref_row(row)?;
        if matches!(previous_version_id.as_ref(), Some(previous) if previous == &parsed.version_id)
        {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "local version-head resolution for version '{}' found multiple exact rows",
                    parsed.version_id
                ),
            ));
        }
        previous_version_id = Some(parsed.version_id.clone());
        rows.push(parsed);
    }

    Ok(rows)
}

pub(crate) async fn load_local_version_ref_heads_map_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Option<BTreeMap<String, String>>, LixError> {
    if !local_version_ref_relation_exists_with_executor(executor).await? {
        return Ok(None);
    }

    let rows = load_all_local_version_refs_with_executor(executor).await?;
    Ok(Some(
        rows.into_iter()
            .map(|row| (row.version_id, row.commit_id))
            .collect(),
    ))
}

pub(crate) fn build_local_version_ref_heads_source_sql() -> String {
    format!(
        "SELECT \
            entity_id AS version_id, \
            commit_id AS commit_id \
         FROM {table} \
         WHERE schema_key = '{ref_schema_key}' \
           AND schema_version = '{ref_schema_version}' \
           AND file_id = '{ref_file_id}' \
           AND plugin_key = '{ref_plugin_key}' \
           AND version_id = '{storage_version_id}' \
           AND untracked = true \
           AND is_tombstone = 0 \
           AND commit_id IS NOT NULL \
           AND commit_id <> ''",
        table = local_version_ref_relation_name(),
        ref_schema_key = escape_sql_string(version_ref_schema_key()),
        ref_schema_version = escape_sql_string(version_ref_schema_version()),
        ref_file_id = escape_sql_string(version_ref_file_id()),
        ref_plugin_key = escape_sql_string(version_ref_plugin_key()),
        storage_version_id = escape_sql_string(version_ref_storage_version_id()),
    )
}

fn local_version_ref_relation_name() -> String {
    format!(
        "{}{}",
        internal_live_table_prefix(),
        version_ref_schema_key()
    )
}

fn internal_live_table_prefix() -> &'static str {
    concat!("lix_internal_live_v1", "_")
}

async fn local_version_ref_relation_exists_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<bool, LixError> {
    let relation_name = local_version_ref_relation_name();
    let result = match executor.dialect() {
        SqlDialect::Sqlite => {
            executor
                .execute(
                    "SELECT 1 \
                     FROM sqlite_master \
                     WHERE name = $1 \
                       AND type IN ('table', 'view') \
                     LIMIT 1",
                    &[Value::Text(relation_name)],
                )
                .await?
        }
        SqlDialect::Postgres => {
            executor
                .execute(
                    "SELECT 1 \
                     FROM information_schema.tables \
                     WHERE table_name = $1 \
                     LIMIT 1",
                    &[Value::Text(relation_name)],
                )
                .await?
        }
    };
    Ok(!result.rows.is_empty())
}

fn parse_local_version_ref_row(row: &[Value]) -> Result<LocalVersionRefRow, LixError> {
    let version_id = required_text_cell(row, 0, "entity_id")?;
    let commit_id = required_text_cell(row, 1, "commit_id")?;
    if commit_id.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "local version head for '{}' has empty commit_id",
                version_id
            ),
        ));
    }
    Ok(LocalVersionRefRow {
        version_id,
        commit_id,
    })
}

fn required_text_cell(row: &[Value], index: usize, column_name: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.clone()),
        Some(Value::Integer(value)) => Ok(value.to_string()),
        Some(_) | None => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("local version-ref row is missing text column '{column_name}'"),
        )),
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
