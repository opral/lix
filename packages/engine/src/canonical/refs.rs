use crate::backend::QueryExecutor;
use crate::errors::classification::is_missing_relation_error;
use crate::live_state::constraints::{ScanConstraint, ScanField, ScanOperator};
use crate::live_state::untracked::{
    load_exact_row_with_executor, scan_rows_with_executor, ExactUntrackedRowRequest,
    UntrackedScanRequest, UntrackedWriteBatch, UntrackedWriteOperation, UntrackedWriteParticipant,
    UntrackedWriteRow,
};
use crate::schema::builtin::types::LixVersionRef;
use crate::version::{
    version_ref_file_id, version_ref_plugin_key, version_ref_schema_key,
    version_ref_schema_version, version_ref_snapshot_content, version_ref_storage_version_id,
};
use crate::{LixBackend, LixBackendTransaction, LixError, Value};

use super::receipt::UpdatedVersionRef;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct VersionRefRow {
    pub(crate) version_id: String,
    pub(crate) commit_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CanonicalVersionRefLookupState {
    Resolved,
    Unavailable,
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
    match load_committed_version_ref_from_canonical(executor, version_id).await? {
        (CanonicalVersionRefLookupState::Resolved, version_ref) => Ok(version_ref),
        (CanonicalVersionRefLookupState::Unavailable, _) => {
            load_committed_version_ref_mirror_with_executor(executor, version_id).await
        }
    }
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
    let (lookup_state, version_refs) =
        load_all_committed_version_refs_from_canonical(executor).await?;
    if lookup_state == CanonicalVersionRefLookupState::Resolved {
        return Ok(version_refs);
    }
    load_all_committed_version_ref_mirrors_with_executor(executor).await
}

pub(crate) async fn apply_committed_version_ref_updates_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_ref_updates: &[UpdatedVersionRef],
) -> Result<(), LixError> {
    let version_ref_batch =
        committed_version_ref_mirror_write_batch_from_updates(version_ref_updates);
    UntrackedWriteParticipant::apply_write_batch(transaction, &version_ref_batch).await
}

pub(crate) fn committed_version_ref_mirror_write_row(
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> UntrackedWriteRow {
    // This persisted untracked row is a SQL-facing mirror for convenience
    // surfaces. Canonical change rows remain the semantic source of truth.
    UntrackedWriteRow {
        entity_id: version_id.to_string(),
        schema_key: version_ref_schema_key().to_string(),
        schema_version: version_ref_schema_version().to_string(),
        file_id: version_ref_file_id().to_string(),
        version_id: version_ref_storage_version_id().to_string(),
        global: true,
        plugin_key: version_ref_plugin_key().to_string(),
        metadata: None,
        writer_key: None,
        snapshot_content: Some(version_ref_snapshot_content(version_id, commit_id)),
        created_at: Some(timestamp.to_string()),
        updated_at: timestamp.to_string(),
        operation: UntrackedWriteOperation::Upsert,
    }
}

fn committed_version_ref_mirror_write_batch_from_updates(
    version_ref_updates: &[UpdatedVersionRef],
) -> UntrackedWriteBatch {
    version_ref_updates
        .iter()
        .map(committed_version_ref_mirror_write_row_from_update)
        .collect()
}

async fn load_committed_version_ref_from_canonical(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<(CanonicalVersionRefLookupState, Option<VersionRefRow>), LixError> {
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
        Ok(result) => {
            if let Some(value) = result.rows.first().and_then(|row| row.first()).cloned() {
                Some(value)
            } else if canonical_version_ref_facts_exist(executor).await? {
                None
            } else {
                return Ok((CanonicalVersionRefLookupState::Unavailable, None));
            }
        }
        Err(err) if is_missing_relation_error(&err) => {
            return Ok((CanonicalVersionRefLookupState::Unavailable, None));
        }
        Err(err) => return Err(err),
    };

    let version_ref =
        parse_version_ref_snapshot(snapshot_content.as_ref())?.map(|version_ref| VersionRefRow {
            version_id: version_ref.id,
            commit_id: version_ref.commit_id,
        });
    Ok((CanonicalVersionRefLookupState::Resolved, version_ref))
}

async fn load_all_committed_version_refs_from_canonical(
    executor: &mut dyn QueryExecutor,
) -> Result<(CanonicalVersionRefLookupState, Vec<VersionRefRow>), LixError> {
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
        Err(err) if is_missing_relation_error(&err) => {
            return Ok((CanonicalVersionRefLookupState::Unavailable, Vec::new()));
        }
        Err(err) => return Err(err),
    };

    if result.rows.is_empty() && !canonical_version_ref_facts_exist(executor).await? {
        return Ok((CanonicalVersionRefLookupState::Unavailable, Vec::new()));
    }

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
    Ok((CanonicalVersionRefLookupState::Resolved, version_refs))
}

async fn canonical_version_ref_facts_exist(
    executor: &mut dyn QueryExecutor,
) -> Result<bool, LixError> {
    let sql = format!(
        "SELECT 1 \
         FROM lix_internal_change \
         WHERE schema_key = '{schema_key}' \
           AND schema_version = '{schema_version}' \
           AND file_id = '{file_id}' \
           AND plugin_key = '{plugin_key}' \
         LIMIT 1",
        schema_key = escape_sql_string(version_ref_schema_key()),
        schema_version = escape_sql_string(version_ref_schema_version()),
        file_id = escape_sql_string(version_ref_file_id()),
        plugin_key = escape_sql_string(version_ref_plugin_key()),
    );
    match executor.execute(&sql, &[]).await {
        Ok(result) => Ok(!result.rows.is_empty()),
        Err(err) if is_missing_relation_error(&err) => Ok(false),
        Err(err) => Err(err),
    }
}

async fn load_all_committed_version_ref_mirrors_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<VersionRefRow>, LixError> {
    let rows = scan_rows_with_executor(
        executor,
        &UntrackedScanRequest {
            schema_key: version_ref_schema_key().to_string(),
            version_id: version_ref_storage_version_id().to_string(),
            constraints: vec![
                ScanConstraint {
                    field: ScanField::FileId,
                    operator: ScanOperator::Eq(Value::Text(version_ref_file_id().to_string())),
                },
                ScanConstraint {
                    field: ScanField::PluginKey,
                    operator: ScanOperator::Eq(Value::Text(version_ref_plugin_key().to_string())),
                },
            ],
            required_columns: vec!["commit_id".to_string()],
        },
    )
    .await?;

    let mut resolved = Vec::new();
    for row in rows {
        let Some(commit_id) = row.property_text("commit_id") else {
            continue;
        };
        if commit_id.is_empty() {
            continue;
        }
        resolved.push(VersionRefRow {
            version_id: row.entity_id,
            commit_id,
        });
    }
    resolved.sort_by(|left, right| left.version_id.cmp(&right.version_id));
    Ok(resolved)
}

async fn load_committed_version_ref_mirror_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    let Some(row) = load_exact_row_with_executor(
        executor,
        &ExactUntrackedRowRequest {
            schema_key: version_ref_schema_key().to_string(),
            version_id: version_ref_storage_version_id().to_string(),
            entity_id: version_id.to_string(),
            file_id: Some(version_ref_file_id().to_string()),
        },
    )
    .await?
    else {
        return Ok(None);
    };
    if row.plugin_key != version_ref_plugin_key() {
        return Ok(None);
    }
    let commit_id = row.property_text("commit_id").ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("version ref mirror row for '{version_id}' is missing commit_id"),
        )
    })?;
    Ok(Some(VersionRefRow {
        version_id: row.entity_id,
        commit_id,
    }))
}

fn committed_version_ref_mirror_write_row_from_update(
    update: &UpdatedVersionRef,
) -> UntrackedWriteRow {
    committed_version_ref_mirror_write_row(
        update.version_id.as_str(),
        &update.commit_id,
        &update.created_at,
    )
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
    use crate::live_state::live_schema_column_names;
    use crate::live_state::schema_access::normalized_values_for_schema;
    use crate::{QueryResult, SqlDialect};
    use async_trait::async_trait;

    struct FakeQueryExecutor {
        canonical_snapshot: Option<Option<String>>,
        canonical_facts_exist: bool,
        mirror_snapshot: Option<String>,
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

            if sql.contains("SELECT 1")
                && sql.contains("FROM lix_internal_change")
                && sql.contains("schema_key = 'lix_version_ref'")
            {
                return Ok(QueryResult {
                    rows: if self.canonical_facts_exist {
                        vec![vec![Value::Integer(1)]]
                    } else {
                        Vec::new()
                    },
                    columns: vec!["1".to_string()],
                });
            }

            if sql.contains("FROM \"lix_internal_live_v1_lix_version_ref\"")
                && sql.contains("'main'")
                && sql.contains("ORDER BY updated_at DESC")
            {
                let rows = self
                    .mirror_snapshot
                    .as_ref()
                    .map(|commit_id| vec![fake_version_ref_mirror_row("main", commit_id)])
                    .unwrap_or_default();
                return Ok(QueryResult {
                    rows,
                    columns: Vec::new(),
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
            canonical_facts_exist: true,
            mirror_snapshot: Some("commit-mirror".to_string()),
        };

        let version_ref = load_committed_version_ref_with_executor(&mut executor, "main")
            .await
            .expect("lookup should succeed")
            .expect("ref should exist");

        assert_eq!(version_ref.commit_id, "commit-canonical");
    }

    #[tokio::test]
    async fn committed_ref_lookup_falls_back_to_mirror_when_canonical_facts_are_absent() {
        let mut executor = FakeQueryExecutor {
            canonical_snapshot: None,
            canonical_facts_exist: false,
            mirror_snapshot: Some("commit-mirror".to_string()),
        };

        let version_ref = load_committed_version_ref_with_executor(&mut executor, "main")
            .await
            .expect("lookup should succeed")
            .expect("mirror ref should exist");

        assert_eq!(version_ref.commit_id, "commit-mirror");
    }

    fn fake_version_ref_mirror_row(version_id: &str, commit_id: &str) -> Vec<Value> {
        let snapshot = version_ref_snapshot_content(version_id, commit_id);
        let normalized =
            normalized_values_for_schema(version_ref_schema_key(), None, Some(&snapshot))
                .expect("snapshot should normalize");
        let mut row = vec![
            Value::Text(version_id.to_string()),
            Value::Text(version_ref_schema_key().to_string()),
            Value::Text(version_ref_schema_version().to_string()),
            Value::Text(version_ref_file_id().to_string()),
            Value::Text(version_ref_storage_version_id().to_string()),
            Value::Boolean(true),
            Value::Text(version_ref_plugin_key().to_string()),
            Value::Null,
            Value::Null,
            Value::Text("2026-03-28T00:00:00Z".to_string()),
            Value::Text("2026-03-28T00:00:00Z".to_string()),
        ];
        for column_name in live_schema_column_names(version_ref_schema_key(), None)
            .expect("version ref schema should expose column names")
        {
            row.push(normalized.get(&column_name).cloned().unwrap_or(Value::Null));
        }
        row
    }
}
