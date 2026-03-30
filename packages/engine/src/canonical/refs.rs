//! Replica-local committed-head resolution.
//!
//! `lix_version_ref` is local runtime state that chooses which canonical commit
//! a version resolves to on this replica. It is intentionally not canonical
//! history, so head lookup must read the exact local row rather than infer a
//! winner from canonical change order.

use crate::backend::QueryExecutor;
use crate::live_state::schema_access::live_storage_relation_exists_with_executor;
use crate::live_state::untracked::{
    load_exact_row_with_executor, scan_rows_with_executor, ExactUntrackedRowRequest,
    UntrackedScanRequest,
};
use crate::version::{
    version_ref_file_id, version_ref_plugin_key, version_ref_schema_key, version_ref_schema_version,
    version_ref_storage_version_id,
};
use crate::{CommittedVersionFrontier, LixBackend, LixError};

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
    load_committed_version_ref_from_local_state(executor, version_id).await
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
    load_all_committed_version_refs_from_local_state(executor).await
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
        load_all_committed_version_refs_from_local_state(executor).await?,
    ))
}

async fn load_committed_version_ref_from_local_state(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    if !live_storage_relation_exists_with_executor(executor, version_ref_schema_key()).await? {
        return Ok(None);
    }
    let row = load_exact_row_with_executor(
        executor,
        &ExactUntrackedRowRequest {
            schema_key: version_ref_schema_key().to_string(),
            version_id: version_ref_storage_version_id().to_string(),
            entity_id: version_id.to_string(),
            file_id: Some(version_ref_file_id().to_string()),
        },
    )
    .await?;
    match row {
        Some(row) => Ok(Some(parse_version_ref_row_from_untracked(
            row.entity_id.clone(),
            &row,
        )?)),
        None => Ok(None),
    }
}

async fn load_all_committed_version_refs_from_local_state(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<VersionRefRow>, LixError> {
    if !live_storage_relation_exists_with_executor(executor, version_ref_schema_key()).await? {
        return Ok(Vec::new());
    }
    let rows = scan_rows_with_executor(
        executor,
        &UntrackedScanRequest {
            schema_key: version_ref_schema_key().to_string(),
            version_id: version_ref_storage_version_id().to_string(),
            constraints: Vec::new(),
            required_columns: Vec::new(),
        },
    )
    .await?;
    let mut version_refs = Vec::with_capacity(rows.len());
    let mut previous_version_id: Option<String> = None;

    for row in rows {
        if row.file_id != version_ref_file_id()
            || row.schema_version != version_ref_schema_version()
            || row.plugin_key != version_ref_plugin_key()
        {
            continue;
        }
        if let Some(previous) = previous_version_id.as_ref() {
            if previous == &row.entity_id {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "local version-head resolution for version '{}' found multiple exact rows",
                        row.entity_id
                    ),
                ));
            }
        }
        previous_version_id = Some(row.entity_id.clone());
        version_refs.push(parse_version_ref_row_from_untracked(row.entity_id.clone(), &row)?);
    }

    Ok(version_refs)
}

fn parse_version_ref_row_from_untracked(
    version_id: String,
    row: &crate::live_state::untracked::UntrackedRow,
) -> Result<VersionRefRow, LixError> {
    let commit_id = row.property_text("commit_id").ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("local version head for '{}' is missing commit_id", version_id),
        )
    })?;
    if commit_id.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("local version head for '{}' has empty commit_id", version_id),
        ));
    }
    Ok(VersionRefRow {
        version_id,
        commit_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_state::live_relation_name;
    use crate::live_state::schema_access::normalized_values_for_schema;
    use crate::live_state::live_schema_column_names;
    use crate::{QueryResult, SqlDialect, Value};
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
            if sql.contains(&live_relation_name("lix_version_ref")) && sql.contains("untracked = true")
            {
                let rows = self
                    .current_ref_rows
                    .iter()
                    .filter(|(version_id, _)| {
                        !sql.contains("entity_id = '")
                            || sql.contains(&format!("entity_id = '{}'", version_id))
                    })
                    .map(|(version_id, commit_id)| fake_version_ref_live_row(version_id, commit_id))
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec![
                        "entity_id".to_string(),
                        "schema_key".to_string(),
                        "schema_version".to_string(),
                        "file_id".to_string(),
                        "version_id".to_string(),
                        "global".to_string(),
                        "plugin_key".to_string(),
                        "metadata".to_string(),
                        "writer_key".to_string(),
                        "created_at".to_string(),
                        "updated_at".to_string(),
                        "commit_id".to_string(),
                        "id".to_string(),
                    ],
                });
            }

            Err(LixError::unknown(format!("unexpected sql: {sql}")))
        }
    }

    fn fake_version_ref_live_row(version_id: &str, commit_id: &str) -> Vec<Value> {
        let snapshot = crate::version::version_ref_snapshot_content(version_id, commit_id);
        let normalized = normalized_values_for_schema(
            crate::version::version_ref_schema_key(),
            None,
            Some(&snapshot),
        )
        .expect("snapshot should normalize");
        let mut row = vec![
            Value::Text(version_id.to_string()),
            Value::Text(crate::version::version_ref_schema_key().to_string()),
            Value::Text(crate::version::version_ref_schema_version().to_string()),
            Value::Text(crate::version::version_ref_file_id().to_string()),
            Value::Text(crate::version::version_ref_storage_version_id().to_string()),
            Value::Boolean(true),
            Value::Text(crate::version::version_ref_plugin_key().to_string()),
            Value::Null,
            Value::Null,
            Value::Text("2026-03-06T14:22:00.000Z".to_string()),
            Value::Text("2026-03-06T14:22:00.000Z".to_string()),
        ];
        for column_name in live_schema_column_names(crate::version::version_ref_schema_key(), None)
            .expect("version ref schema should expose column names")
        {
            row.push(normalized.get(&column_name).cloned().unwrap_or(Value::Null));
        }
        row
    }

    #[tokio::test]
    async fn committed_ref_lookup_resolves_current_head_from_local_version_head_row() {
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
    async fn committed_ref_lookup_returns_none_when_local_version_head_row_is_absent() {
        let mut executor = FakeQueryExecutor {
            current_ref_rows: Vec::new(),
        };

        let version_ref = load_committed_version_ref_with_executor(&mut executor, "main")
            .await
            .expect("lookup should succeed");

        assert!(version_ref.is_none());
    }

    #[tokio::test]
    async fn committed_ref_lookup_rejects_multiple_exact_rows_for_one_version() {
        let mut executor = FakeQueryExecutor {
            current_ref_rows: vec![
                ("main".to_string(), "commit-a".to_string()),
                ("main".to_string(), "commit-b".to_string()),
            ],
        };

        let error = load_committed_version_ref_with_executor(&mut executor, "main")
            .await
            .expect_err("lookup should reject multiple heads");

        assert!(error.description.contains("expected at most one untracked row"));
    }
}
