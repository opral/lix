//! Canonical committed-ref resolution.
//!
//! Canonical refs are semantic selectors for committed heads and roots. They
//! must be resolved from canonical change facts, not from live-state mirrors or
//! replay cursors. Version refs are maintained as their own canonical fact
//! stream outside commit membership, so current-ref selection is based on the
//! latest ref fact for each version rather than on commit-member ordering.

use crate::backend::QueryExecutor;
use crate::errors::classification::is_missing_relation_error;
use crate::version::{
    version_ref_file_id, version_ref_plugin_key, version_ref_schema_key, version_ref_schema_version,
};
use crate::{CommittedVersionFrontier, LixBackend, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct VersionRefRow {
    pub(crate) version_id: String,
    pub(crate) commit_id: String,
}

pub(crate) async fn load_committed_version_ref_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    let mut executor = backend;
    load_committed_version_ref_with_executor(&mut executor, version_id).await
}

pub(crate) async fn load_committed_version_ref_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    load_committed_version_ref_from_canonical(executor, version_id).await
}

pub(crate) async fn load_committed_version_head_commit_id(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    let Some(version_ref) = load_committed_version_ref_with_executor(executor, version_id).await?
    else {
        return Ok(None);
    };
    if version_ref.commit_id.is_empty() {
        return Ok(None);
    }
    Ok(Some(version_ref.commit_id))
}

pub(crate) async fn load_all_committed_version_refs_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<VersionRefRow>, LixError> {
    load_all_committed_version_refs_from_canonical(executor).await
}

pub(crate) async fn load_current_committed_version_frontier_with_backend(
    backend: &dyn LixBackend,
) -> Result<CommittedVersionFrontier, LixError> {
    let mut executor = backend;
    load_current_committed_version_frontier_with_executor(&mut executor).await
}

pub(crate) async fn load_current_committed_version_frontier_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<CommittedVersionFrontier, LixError> {
    Ok(CommittedVersionFrontier::from_version_ref_rows(
        load_all_committed_version_refs_from_canonical(executor).await?,
    ))
}

async fn load_committed_version_ref_from_canonical(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    let sql = build_current_version_ref_rows_sql(executor.dialect(), Some(version_id));
    let result = match executor.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(err) if is_missing_relation_error(&err) => return Ok(None),
        Err(err) => return Err(err),
    };
    parse_unique_version_ref_row(&result.rows, version_id)
}

async fn load_all_committed_version_refs_from_canonical(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<VersionRefRow>, LixError> {
    let sql = build_current_version_ref_rows_sql(executor.dialect(), None);
    let result = match executor.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(err) if is_missing_relation_error(&err) => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    parse_all_version_ref_rows(&result.rows)
}

fn build_current_version_ref_rows_sql(
    dialect: crate::SqlDialect,
    scoped_version_id: Option<&str>,
) -> String {
    let scoped_version_sql = scoped_version_id
        .map(|value| format!(" AND ref_change.entity_id = '{}'", escape_sql_string(value)))
        .unwrap_or_default();
    let commit_id_expr = json_text_extract_sql(dialect, "ref_snapshot.content", "commit_id");

    format!(
        "WITH ranked_ref_facts AS ( \
             SELECT \
               ref_change.entity_id AS version_id, \
               {commit_id_expr} AS commit_id, \
               ref_snapshot.content AS snapshot_content, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY ref_change.entity_id \
                 ORDER BY ref_change.created_at DESC, ref_change.id DESC \
               ) AS rn \
             FROM lix_internal_change ref_change \
             LEFT JOIN lix_internal_snapshot ref_snapshot \
               ON ref_snapshot.id = ref_change.snapshot_id \
             WHERE ref_change.schema_key = '{schema_key}' \
               AND ref_change.schema_version = '{schema_version}' \
               AND ref_change.file_id = '{file_id}' \
               AND ref_change.plugin_key = '{plugin_key}' \
               {scoped_version_sql} \
           ), \
           current_refs AS ( \
             SELECT \
               version_id, \
               commit_id \
             FROM ranked_ref_facts \
             WHERE rn = 1 \
               AND snapshot_content IS NOT NULL \
               AND COALESCE(commit_id, '') <> '' \
           ) \
         SELECT version_id, commit_id \
         FROM current_refs \
         ORDER BY version_id ASC, commit_id ASC",
        schema_key = escape_sql_string(version_ref_schema_key()),
        schema_version = escape_sql_string(version_ref_schema_version()),
        file_id = escape_sql_string(version_ref_file_id()),
        plugin_key = escape_sql_string(version_ref_plugin_key()),
        scoped_version_sql = scoped_version_sql,
        commit_id_expr = commit_id_expr,
    )
}

fn parse_unique_version_ref_row(
    rows: &[Vec<Value>],
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    let parsed = parse_all_version_ref_rows(rows)?;
    match parsed.as_slice() {
        [] => Ok(None),
        [row] => Ok(Some(row.clone())),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "committed ref resolution for version '{}' found multiple incomparable heads",
                version_id
            ),
        )),
    }
}

fn parse_all_version_ref_rows(rows: &[Vec<Value>]) -> Result<Vec<VersionRefRow>, LixError> {
    let mut version_refs = Vec::new();
    let mut previous_version_id: Option<String> = None;

    for row in rows {
        let version_id = required_text(row.first(), "current_refs.version_id")?;
        let commit_id = required_text(row.get(1), "current_refs.commit_id")?;
        if let Some(previous) = previous_version_id.as_ref() {
            if previous == &version_id {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "committed ref resolution for version '{}' found multiple incomparable heads",
                        version_id
                    ),
                ));
            }
        }
        previous_version_id = Some(version_id.clone());
        version_refs.push(VersionRefRow {
            version_id,
            commit_id,
        });
    }

    Ok(version_refs)
}

fn required_text(value: Option<&Value>, field: &str) -> Result<String, LixError> {
    match value {
        Some(Value::Text(text)) if !text.is_empty() => Ok(text.clone()),
        Some(Value::Integer(number)) => Ok(number.to_string()),
        Some(Value::Text(_)) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{field} is empty"),
        )),
        Some(Value::Null) | None => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("missing {field}"),
        )),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("expected text-like {field}, got {other:?}"),
        )),
    }
}

fn json_text_extract_sql(dialect: crate::SqlDialect, json_column: &str, field: &str) -> String {
    match dialect {
        crate::SqlDialect::Sqlite => format!("json_extract({json_column}, '$.{field}')"),
        crate::SqlDialect::Postgres => {
            format!("CAST({json_column} AS JSONB) ->> '{field}'")
        }
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{QueryResult, SqlDialect};
    use async_trait::async_trait;

    struct FakeQueryExecutor {
        current_ref_rows: Vec<(String, String)>,
    }

    #[async_trait(?Send)]
    impl QueryExecutor for FakeQueryExecutor {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM current_refs") && sql.contains("schema_key = 'lix_version_ref'") {
                let rows = self
                    .current_ref_rows
                    .iter()
                    .filter(|(version_id, _)| {
                        !sql.contains("ref_change.entity_id = '")
                            || sql.contains(&format!("ref_change.entity_id = '{}'", version_id))
                    })
                    .map(|(version_id, commit_id)| {
                        vec![
                            Value::Text(version_id.clone()),
                            Value::Text(commit_id.clone()),
                        ]
                    })
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["version_id".to_string(), "commit_id".to_string()],
                });
            }

            Err(LixError::unknown(format!("unexpected sql: {sql}")))
        }
    }

    #[tokio::test]
    async fn committed_ref_lookup_resolves_current_head_from_canonical_ref_facts() {
        let mut executor = FakeQueryExecutor {
            current_ref_rows: vec![("main".to_string(), "commit-canonical".to_string())],
        };

        let version_ref = load_committed_version_ref_with_executor(&mut executor, "main")
            .await
            .expect("lookup should succeed")
            .expect("ref should exist");

        assert_eq!(version_ref.commit_id, "commit-canonical");
    }

    #[tokio::test]
    async fn committed_ref_lookup_does_not_fall_back_to_mirror_when_canonical_fact_is_absent() {
        let mut executor = FakeQueryExecutor {
            current_ref_rows: Vec::new(),
        };

        let version_ref = load_committed_version_ref_with_executor(&mut executor, "main")
            .await
            .expect("lookup should succeed");

        assert!(version_ref.is_none());
    }

    #[tokio::test]
    async fn committed_ref_lookup_rejects_multiple_incomparable_heads_for_one_version() {
        let mut executor = FakeQueryExecutor {
            current_ref_rows: vec![
                ("main".to_string(), "commit-a".to_string()),
                ("main".to_string(), "commit-b".to_string()),
            ],
        };

        let error = load_committed_version_ref_with_executor(&mut executor, "main")
            .await
            .expect_err("lookup should reject multiple heads");

        assert!(error.description.contains("multiple incomparable heads"));
    }
}
