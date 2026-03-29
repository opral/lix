use std::collections::BTreeSet;

use crate::backend::QueryExecutor;
use crate::errors::classification::is_missing_relation_error;
use crate::schema::builtin::types::LixVersionRef;
use crate::version::{
    version_ref_file_id, version_ref_plugin_key, version_ref_schema_key, version_ref_schema_version,
};
use crate::{LixBackend, LixError, Value};

use super::receipt::CanonicalWatermark;

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

pub(crate) async fn load_committed_version_ids_updated_between_watermarks_with_backend(
    backend: &dyn LixBackend,
    older_exclusive: &CanonicalWatermark,
    newer_inclusive: &CanonicalWatermark,
) -> Result<Option<BTreeSet<String>>, LixError> {
    let mut executor = backend;
    load_committed_version_ids_updated_between_watermarks_with_executor(
        &mut executor,
        older_exclusive,
        newer_inclusive,
    )
    .await
}

pub(crate) async fn load_committed_version_ids_updated_between_watermarks_with_executor(
    executor: &mut dyn QueryExecutor,
    older_exclusive: &CanonicalWatermark,
    newer_inclusive: &CanonicalWatermark,
) -> Result<Option<BTreeSet<String>>, LixError> {
    if !newer_inclusive.is_newer_than(older_exclusive) {
        return Ok(Some(BTreeSet::new()));
    }

    let sql = format!(
        "SELECT DISTINCT c.entity_id \
         FROM lix_internal_change c \
         WHERE c.schema_key = '{schema_key}' \
           AND c.schema_version = '{schema_version}' \
           AND c.file_id = '{file_id}' \
           AND c.plugin_key = '{plugin_key}' \
           AND (\
             c.created_at > '{older_created_at}' \
             OR (c.created_at = '{older_created_at}' AND c.id > '{older_change_id}')\
           ) \
           AND (\
             c.created_at < '{newer_created_at}' \
             OR (c.created_at = '{newer_created_at}' AND c.id <= '{newer_change_id}')\
           ) \
         ORDER BY c.entity_id ASC",
        schema_key = escape_sql_string(version_ref_schema_key()),
        schema_version = escape_sql_string(version_ref_schema_version()),
        file_id = escape_sql_string(version_ref_file_id()),
        plugin_key = escape_sql_string(version_ref_plugin_key()),
        older_created_at = escape_sql_string(&older_exclusive.created_at),
        older_change_id = escape_sql_string(&older_exclusive.change_id),
        newer_created_at = escape_sql_string(&newer_inclusive.created_at),
        newer_change_id = escape_sql_string(&newer_inclusive.change_id),
    );

    let result = match executor.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(err) if is_missing_relation_error(&err) => return Ok(None),
        Err(err) => return Err(err),
    };

    let mut version_ids = BTreeSet::new();
    for row in &result.rows {
        match row.first() {
            Some(Value::Text(version_id)) if !version_id.is_empty() => {
                version_ids.insert(version_id.clone());
            }
            Some(Value::Null) | None => {}
            Some(other) => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("version ref delta query returned non-text entity_id: {other:?}"),
                ));
            }
        }
    }

    Ok(Some(version_ids))
}

async fn load_committed_version_ref_from_canonical(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    let sql = format!(
        "SELECT s.content AS snapshot_content \
         FROM lix_internal_change c \
         LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
         WHERE c.schema_key = '{schema_key}' \
           AND c.schema_version = '{schema_version}' \
           AND c.entity_id = '{entity_id}' \
           AND c.file_id = '{file_id}' \
           AND c.plugin_key = '{plugin_key}' \
         ORDER BY c.created_at DESC, c.id DESC \
         LIMIT 1",
        schema_key = escape_sql_string(version_ref_schema_key()),
        schema_version = escape_sql_string(version_ref_schema_version()),
        entity_id = escape_sql_string(version_id),
        file_id = escape_sql_string(version_ref_file_id()),
        plugin_key = escape_sql_string(version_ref_plugin_key()),
    );

    let snapshot_content = match executor.execute(&sql, &[]).await {
        Ok(result) => result.rows.first().and_then(|row| row.first()).cloned(),
        Err(err) if is_missing_relation_error(&err) => return Ok(None),
        Err(err) => return Err(err),
    };

    let version_ref =
        parse_version_ref_snapshot(snapshot_content.as_ref())?.map(|version_ref| VersionRefRow {
            version_id: version_ref.id,
            commit_id: version_ref.commit_id,
        });
    Ok(version_ref)
}

async fn load_all_committed_version_refs_from_canonical(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<VersionRefRow>, LixError> {
    let sql = format!(
        "WITH ranked AS (\
             SELECT c.entity_id, \
                    s.content AS snapshot_content, \
                    ROW_NUMBER() OVER (PARTITION BY c.entity_id ORDER BY c.created_at DESC, c.id DESC) AS rn \
             FROM lix_internal_change c \
             LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
             WHERE c.schema_key = '{schema_key}' \
               AND c.schema_version = '{schema_version}' \
               AND c.file_id = '{file_id}' \
               AND c.plugin_key = '{plugin_key}'\
         ) \
         SELECT entity_id, snapshot_content \
         FROM ranked \
         WHERE rn = 1",
        schema_key = escape_sql_string(version_ref_schema_key()),
        schema_version = escape_sql_string(version_ref_schema_version()),
        file_id = escape_sql_string(version_ref_file_id()),
        plugin_key = escape_sql_string(version_ref_plugin_key()),
    );

    let result = match executor.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(err) if is_missing_relation_error(&err) => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };

    let mut version_refs = Vec::new();
    for row in &result.rows {
        let Some(version_ref) = parse_version_ref_snapshot(row.get(1))? else {
            continue;
        };
        if version_ref.commit_id.is_empty() {
            continue;
        }
        version_refs.push(VersionRefRow {
            version_id: version_ref.id,
            commit_id: version_ref.commit_id,
        });
    }
    version_refs.sort_by(|left, right| left.version_id.cmp(&right.version_id));
    Ok(version_refs)
}

fn parse_version_ref_snapshot(value: Option<&Value>) -> Result<Option<LixVersionRef>, LixError> {
    let Some(raw_snapshot) = value else {
        return Ok(None);
    };
    let raw_snapshot = match raw_snapshot {
        Value::Text(value) => value,
        Value::Null => return Ok(None),
        _ => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "version ref snapshot_content must be text".to_string(),
            });
        }
    };

    let snapshot: LixVersionRef = serde_json::from_str(raw_snapshot).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("version ref snapshot_content invalid JSON: {error}"),
    })?;
    Ok(Some(snapshot))
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
        canonical_snapshot: Option<Option<String>>,
        updated_version_ids: Option<Vec<String>>,
    }

    #[async_trait(?Send)]
    impl QueryExecutor for FakeQueryExecutor {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_change c")
                && sql.contains("AND c.entity_id = 'main'")
            {
                let rows = match &self.canonical_snapshot {
                    Some(Some(snapshot)) => vec![vec![Value::Text(snapshot.clone())]],
                    Some(None) => vec![vec![Value::Null]],
                    None => Vec::new(),
                };
                return Ok(QueryResult {
                    rows,
                    columns: vec!["snapshot_content".to_string()],
                });
            }

            if sql.contains("SELECT DISTINCT c.entity_id")
                && sql.contains("schema_key = 'lix_version_ref'")
            {
                return Ok(QueryResult {
                    rows: self
                        .updated_version_ids
                        .clone()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|version_id| vec![Value::Text(version_id)])
                        .collect(),
                    columns: vec!["entity_id".to_string()],
                });
            }

            Err(LixError::unknown(format!("unexpected sql: {sql}")))
        }
    }

    #[tokio::test]
    async fn committed_ref_lookup_prefers_canonical_fact_over_mirror() {
        let mut executor = FakeQueryExecutor {
            canonical_snapshot: Some(Some(
                "{\"id\":\"main\",\"commit_id\":\"commit-canonical\"}".to_string(),
            )),
            updated_version_ids: None,
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
            canonical_snapshot: None,
            updated_version_ids: None,
        };

        let version_ref = load_committed_version_ref_with_executor(&mut executor, "main")
            .await
            .expect("lookup should succeed");

        assert!(version_ref.is_none());
    }

    #[tokio::test]
    async fn committed_ref_delta_lookup_returns_versions_between_watermarks() {
        let mut executor = FakeQueryExecutor {
            canonical_snapshot: None,
            updated_version_ids: Some(vec!["main".to_string(), "version-b".to_string()]),
        };

        let version_ids = load_committed_version_ids_updated_between_watermarks_with_executor(
            &mut executor,
            &CanonicalWatermark {
                change_id: "change-1".to_string(),
                created_at: "2026-03-28T10:00:00Z".to_string(),
            },
            &CanonicalWatermark {
                change_id: "change-2".to_string(),
                created_at: "2026-03-28T10:00:01Z".to_string(),
            },
        )
        .await
        .expect("delta lookup should succeed")
        .expect("canonical ref facts should be available");

        assert_eq!(
            version_ids,
            BTreeSet::from(["main".to_string(), "version-b".to_string()])
        );
    }
}
