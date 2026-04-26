use std::collections::BTreeMap;

use crate::binary_cas::{BinaryBlobWrite, BinaryCasContext};
use crate::engine2::changelog::{CanonicalChange, ChangelogContext};
use crate::engine2::live_state::{write_state_rows, LiveStateRow};
use crate::engine2::transaction::staging::StagedWriteSet;
use crate::engine2::transaction::types::{StagedCommitMembers, StagedStateRow};
use crate::functions::{DynFunctionProvider, LixFunctionProvider};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError};

/// Commits transaction-staged rows into the durable changelog and live_state.
///
/// Providers decode DataFusion DML into hydrated `StagedStateRow`s. Commit
/// finalization adds normal `lix_commit` rows for tracked changes, appends all
/// finalized rows as canonical changelog facts, then mirrors the same rows into
/// live_state so reads can stay simple while live_state catch-up is still MVP.
pub(crate) async fn commit_staged_writes(
    binary_cas: &BinaryCasContext,
    changelog: &ChangelogContext,
    transaction: &mut dyn LixBackendTransaction,
    staged_writes: StagedWriteSet,
    functions: DynFunctionProvider,
) -> Result<(), LixError> {
    if !staged_writes.file_data_writes.is_empty() {
        let blob_writes = staged_writes
            .file_data_writes
            .iter()
            .map(|write| BinaryBlobWrite {
                file_id: &write.file_id,
                version_id: &write.version_id,
                data: &write.data,
            })
            .collect::<Vec<_>>();
        binary_cas
            .writer(transaction)
            .put_blob_writes(&blob_writes)
            .await?;
    }

    let mut staged_rows = staged_writes.state_rows;
    staged_rows.extend(finalize_commit_rows(
        staged_writes.commit_members_by_version,
        functions,
    )?);

    if staged_rows.is_empty() {
        return Ok(());
    }

    let canonical_changes = staged_rows
        .clone()
        .into_iter()
        .map(CanonicalChange::from)
        .collect::<Vec<_>>();
    {
        let mut writer = changelog.writer(transaction);
        writer.append_changes(&canonical_changes).await?;
    }

    // TODO(engine2): live_state should eventually catch up from changelog
    // rather than being mirrored here. Keeping this bridge makes the current
    // SQL read path see committed writes immediately while changelog becomes
    // the source of truth.
    let live_state_rows = staged_rows
        .into_iter()
        .map(LiveStateRow::from)
        .collect::<Vec<_>>();

    write_state_rows(transaction, &live_state_rows).await
}

/// Materializes tracked staged membership into `lix_commit` rows.
///
/// Staging only accumulates `version_id -> change_ids` because commit ids,
/// parent heads, and commit-row timestamps belong to transaction finalization.
/// This function turns those membership sets into normal `StagedStateRow`s with
/// `schema_key = "lix_commit"`, so the changelog/live_state flush can treat
/// commit rows exactly like any other staged state row.
///
/// MVP limitation: parent head resolution is not implemented yet. Global
/// commits temporarily use an empty parent list, while active-version
/// finalization fails loudly until the same parent-resolution step exists for
/// all versions.
fn finalize_commit_rows(
    commit_members_by_version: BTreeMap<String, StagedCommitMembers>,
    functions: DynFunctionProvider,
) -> Result<Vec<StagedStateRow>, LixError> {
    let mut functions = functions.clone();
    let mut rows = Vec::new();

    for (version_id, members) in commit_members_by_version {
        if members.is_empty() {
            continue;
        }
        if version_id != GLOBAL_VERSION_ID {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "engine2 commit finalization for active versions is not implemented yet",
            ));
        }

        let commit_id = functions.uuid_v7();
        let commit_change_id = functions.uuid_v7();
        let timestamp = functions.timestamp();
        let change_ids = members.change_ids.into_iter().collect::<Vec<_>>();
        // TODO(engine2): resolve parent commit ids for every version, including
        // the global version. Empty parents are only acceptable for this MVP
        // bootstrap path.
        let snapshot_content = serde_json::to_string(&serde_json::json!({
            "id": commit_id,
            "change_ids": change_ids,
            "parent_commit_ids": [],
        }))
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("engine2 commit row snapshot serialization failed: {error}"),
            )
        })?;

        rows.push(StagedStateRow {
            entity_id: commit_id.clone(),
            schema_key: "lix_commit".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(snapshot_content),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: timestamp.clone(),
            updated_at: timestamp,
            global: true,
            change_id: commit_change_id,
            commit_id: Some(commit_id),
            untracked: false,
            version_id: GLOBAL_VERSION_ID.to_string(),
        });
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use serde_json::Value as JsonValue;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
    use crate::engine2::changelog::ChangelogContext;
    use crate::functions::SharedFunctionProvider;

    #[tokio::test]
    async fn commit_staged_writes_appends_changelog_changes_before_live_state_mirror() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let binary_cas = BinaryCasContext::new(Arc::clone(&backend));
        let changelog = ChangelogContext::new(Arc::clone(&backend));
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        commit_staged_writes(
            &binary_cas,
            &changelog,
            transaction.as_mut(),
            StagedWriteSet {
                state_rows: vec![tracked_global_row("change-1")],
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members(["change-1"]),
                )]),
                file_data_writes: Vec::new(),
            },
            test_functions(),
        )
        .await
        .expect("commit should flush staged rows");
        transaction
            .commit()
            .await
            .expect("commit should persist kv");

        let changes = changelog
            .scan_changes(crate::engine2::changelog::ChangelogScanRequest::default())
            .await
            .expect("changelog scan should succeed");
        let change_ids = changes
            .iter()
            .map(|change| change.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(change_ids, vec!["change-1", "test-uuid-2"]);
        assert!(changes
            .iter()
            .any(|change| change.schema_key == "lix_commit"));
    }

    #[test]
    fn finalize_commit_rows_creates_global_commit_row_for_global_members() {
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_VERSION_ID.to_string(),
                members(["change-a", "change-b"]),
            )]),
            test_functions(),
        )
        .expect("global commit row should finalize");

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.entity_id, "test-uuid-1");
        assert_eq!(row.schema_key, "lix_commit");
        assert_eq!(row.schema_version, "1");
        assert_eq!(row.change_id, "test-uuid-2");
        assert_eq!(row.commit_id.as_deref(), Some("test-uuid-1"));
        assert!(row.global);
        assert!(!row.untracked);
        assert_eq!(row.version_id, GLOBAL_VERSION_ID);
        assert_eq!(row.created_at, "test-timestamp-1");
        assert_eq!(row.updated_at, "test-timestamp-1");

        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot_content
                .as_deref()
                .expect("commit row should have snapshot"),
        )
        .expect("commit snapshot should be JSON");
        assert_eq!(
            snapshot.get("id").and_then(JsonValue::as_str),
            Some("test-uuid-1")
        );
        assert_eq!(
            snapshot
                .get("change_ids")
                .and_then(JsonValue::as_array)
                .expect("change_ids should be array")
                .iter()
                .map(|value| value.as_str().expect("change id should be string"))
                .collect::<Vec<_>>(),
            vec!["change-a", "change-b"]
        );
        assert_eq!(
            snapshot
                .get("parent_commit_ids")
                .and_then(JsonValue::as_array)
                .expect("parent_commit_ids should be array")
                .len(),
            0
        );
    }

    #[test]
    fn finalize_commit_rows_skips_empty_members() {
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_VERSION_ID.to_string(),
                StagedCommitMembers::default(),
            )]),
            test_functions(),
        )
        .expect("empty members should be ignored");

        assert!(rows.is_empty());
    }

    #[test]
    fn finalize_commit_rows_rejects_active_version_for_now() {
        let error = finalize_commit_rows(
            BTreeMap::from([("version-a".to_string(), members(["change-a"]))]),
            test_functions(),
        )
        .expect_err("active-version commit finalization should fail for now");

        assert!(error
            .description
            .contains("active versions is not implemented yet"));
    }

    fn members<const N: usize>(change_ids: [&str; N]) -> StagedCommitMembers {
        let mut members = StagedCommitMembers::default();
        for change_id in change_ids {
            members.add_change_id(change_id.to_string());
        }
        members
    }

    fn test_functions() -> DynFunctionProvider {
        SharedFunctionProvider::new(
            Box::new(TestFunctionProvider::default()) as Box<dyn LixFunctionProvider + Send>
        )
    }

    #[derive(Default)]
    struct TestFunctionProvider {
        uuid_count: usize,
        timestamp_count: usize,
    }

    impl LixFunctionProvider for TestFunctionProvider {
        fn uuid_v7(&mut self) -> String {
            self.uuid_count += 1;
            format!("test-uuid-{}", self.uuid_count)
        }

        fn timestamp(&mut self) -> String {
            self.timestamp_count += 1;
            format!("test-timestamp-{}", self.timestamp_count)
        }
    }

    fn tracked_global_row(change_id: &str) -> StagedStateRow {
        StagedStateRow {
            entity_id: "entity-1".to_string(),
            schema_key: "test_schema".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some("{\"value\":1}".to_string()),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: true,
            change_id: change_id.to_string(),
            commit_id: None,
            untracked: false,
            version_id: GLOBAL_VERSION_ID.to_string(),
        }
    }
}
