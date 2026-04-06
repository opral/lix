//! Replica-local committed-head resolution.
//!
//! `lix_version_ref` is local runtime state that chooses which canonical commit
//! a version resolves to on this replica. It is intentionally not canonical
//! history, so head lookup must read the exact local row rather than infer a
//! winner from canonical change order.

use crate::backend::QueryExecutor;
use crate::version::{
    load_all_local_version_refs_with_executor, load_local_version_head_commit_id_with_executor,
    load_local_version_ref_with_executor, LocalVersionRefRow,
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
    Ok(load_local_version_ref_with_executor(executor, version_id)
        .await?
        .map(version_ref_row_from_local))
}

pub(crate) async fn load_committed_version_head_commit_id(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    load_local_version_head_commit_id_with_executor(executor, version_id).await
}

pub(crate) async fn load_all_committed_version_refs_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<VersionRefRow>, LixError> {
    Ok(load_all_local_version_refs_with_executor(executor)
        .await?
        .into_iter()
        .map(version_ref_row_from_local)
        .collect())
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
    Ok(CommittedVersionFrontier {
        version_heads: load_all_committed_version_refs_with_executor(executor)
            .await?
            .into_iter()
            .map(|row| (row.version_id, row.commit_id))
            .collect(),
    })
}

fn version_ref_row_from_local(row: LocalVersionRefRow) -> VersionRefRow {
    VersionRefRow {
        version_id: row.version_id,
        commit_id: row.commit_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_state::constraints::{quote_ident, sql_literal};
    use crate::live_state::{
        live_relation_name, live_schema_column_names, normalized_values_for_schema,
    };
    use crate::test_support::{init_test_backend_core, seed_local_version_head, TestSqliteBackend};
    use crate::version::{
        version_ref_file_id, version_ref_plugin_key, version_ref_schema_key,
        version_ref_schema_version, version_ref_storage_version_id,
    };
    use crate::{LixBackend, Value};

    async fn init_refs_backend() -> TestSqliteBackend {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");
        backend
    }

    async fn rebuild_local_version_head_storage_without_exact_key(
        backend: &TestSqliteBackend,
    ) -> Result<(), LixError> {
        let relation_name = live_relation_name(version_ref_schema_key());
        let relation_ident = quote_ident(&relation_name);
        let normalized_column_defs = live_schema_column_names(version_ref_schema_key(), None)?
            .into_iter()
            .map(|column_name| format!(", {} TEXT", quote_ident(&column_name)))
            .collect::<String>();
        backend
            .execute(&format!("DROP TABLE IF EXISTS {relation_ident}"), &[])
            .await?;
        backend
            .execute(
                &format!(
                    "CREATE TABLE {relation_ident} (\
                     entity_id TEXT NOT NULL,\
                     schema_key TEXT NOT NULL,\
                     schema_version TEXT NOT NULL,\
                     file_id TEXT NOT NULL,\
                     version_id TEXT NOT NULL,\
                     global BOOLEAN NOT NULL DEFAULT false,\
                     plugin_key TEXT NOT NULL,\
                     change_id TEXT,\
                     metadata TEXT,\
                     writer_key TEXT,\
                     is_tombstone INTEGER NOT NULL DEFAULT 0,\
                     untracked BOOLEAN NOT NULL DEFAULT false,\
                     created_at TEXT NOT NULL,\
                     updated_at TEXT NOT NULL\
                     {normalized_column_defs}\
                     )"
                ),
                &[],
            )
            .await?;
        Ok(())
    }

    async fn insert_corrupt_local_version_head_row(
        backend: &TestSqliteBackend,
        version_id: &str,
        commit_id: &str,
        timestamp: &str,
    ) -> Result<(), LixError> {
        let snapshot = crate::version::version_ref_snapshot_content(version_id, commit_id);
        let normalized = normalized_values_for_schema(
            crate::version::version_ref_schema_key(),
            None,
            Some(&snapshot),
        )
        .expect("snapshot should normalize");
        let mut columns = vec![
            "entity_id".to_string(),
            "schema_key".to_string(),
            "schema_version".to_string(),
            "file_id".to_string(),
            "version_id".to_string(),
            "global".to_string(),
            "plugin_key".to_string(),
            "change_id".to_string(),
            "metadata".to_string(),
            "writer_key".to_string(),
            "is_tombstone".to_string(),
            "untracked".to_string(),
            "created_at".to_string(),
            "updated_at".to_string(),
        ];
        let mut values = vec![
            Value::Text(version_id.to_string()),
            Value::Text(version_ref_schema_key().to_string()),
            Value::Text(version_ref_schema_version().to_string()),
            Value::Text(version_ref_file_id().to_string()),
            Value::Text(version_ref_storage_version_id().to_string()),
            Value::Boolean(true),
            Value::Text(version_ref_plugin_key().to_string()),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Integer(0),
            Value::Boolean(true),
            Value::Text(timestamp.to_string()),
            Value::Text(timestamp.to_string()),
        ];
        for column_name in live_schema_column_names(version_ref_schema_key(), None)
            .expect("version ref schema should expose column names")
        {
            columns.push(column_name.clone());
            values.push(normalized.get(&column_name).cloned().unwrap_or(Value::Null));
        }
        let sql = format!(
            "INSERT INTO {table} ({columns}) VALUES ({values})",
            table = quote_ident(&live_relation_name(version_ref_schema_key())),
            columns = columns
                .into_iter()
                .map(|column_name| quote_ident(&column_name))
                .collect::<Vec<_>>()
                .join(", "),
            values = values
                .iter()
                .map(sql_literal)
                .collect::<Vec<_>>()
                .join(", "),
        );
        backend.execute(&sql, &[]).await?;
        Ok(())
    }

    #[tokio::test]
    async fn committed_ref_lookup_resolves_current_head_from_local_version_head_row() {
        let backend = init_refs_backend().await;
        seed_local_version_head(
            &backend,
            "main",
            "commit-canonical",
            "2026-03-06T14:22:00.000Z",
        )
        .await
        .expect("local version head seed should succeed");

        let version_ref = load_committed_version_ref_with_backend(&backend, "main")
            .await
            .expect("lookup should succeed")
            .expect("ref should exist");

        assert_eq!(version_ref.commit_id, "commit-canonical");
    }

    #[tokio::test]
    async fn committed_ref_lookup_returns_none_when_local_version_head_row_is_absent() {
        let backend = init_refs_backend().await;

        let version_ref = load_committed_version_ref_with_backend(&backend, "main")
            .await
            .expect("lookup should succeed");

        assert!(version_ref.is_none());
    }

    #[tokio::test]
    async fn committed_ref_lookup_rejects_multiple_exact_rows_for_one_version() {
        let backend = init_refs_backend().await;
        // Corruption-only setup: the real untracked write path enforces one exact
        // local head row, so duplicates must be injected below that contract.
        rebuild_local_version_head_storage_without_exact_key(&backend)
            .await
            .expect("corrupt local head storage should be rebuildable");
        insert_corrupt_local_version_head_row(
            &backend,
            "main",
            "commit-a",
            "2026-03-06T14:22:00.000Z",
        )
        .await
        .expect("first corrupt local version head should insert");
        insert_corrupt_local_version_head_row(
            &backend,
            "main",
            "commit-b",
            "2026-03-06T14:23:00.000Z",
        )
        .await
        .expect("second corrupt local version head should insert");

        let error = load_committed_version_ref_with_backend(&backend, "main")
            .await
            .expect_err("lookup should reject multiple heads");

        assert!(error
            .description
            .contains("expected at most one untracked row"));
    }
}
