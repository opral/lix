use std::collections::BTreeMap;

use crate::binary_cas::{BinaryBlobWrite, BinaryCasContext};
use crate::engine2::changelog::{CanonicalChange, ChangelogContext};
use crate::engine2::live_state::{LiveStateContext, LiveStateRow, LiveStateRowRequest};
use crate::engine2::transaction::staging::StagedWriteSet;
use crate::engine2::transaction::types::{StagedCommitMembers, StagedStateRow};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError};

const VERSION_REF_SCHEMA_KEY: &str = "lix_version_ref";
const VERSION_REF_SCHEMA_VERSION: &str = "1";

/// Commits transaction-staged rows into durable tracked and untracked stores.
///
/// Providers decode DataFusion DML into hydrated `StagedStateRow`s. Untracked
/// rows are durable local overlay state and bypass changelog/commit rows.
/// Tracked rows receive normal `lix_commit` rows, append canonical changelog
/// facts, then mirror into live_state while catch-up is still MVP.
pub(crate) async fn commit_staged_writes(
    binary_cas: &BinaryCasContext,
    changelog: &ChangelogContext,
    live_state: &LiveStateContext,
    transaction: &mut dyn LixBackendTransaction,
    staged_writes: StagedWriteSet,
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
            .writer(&mut *transaction)
            .put_blob_writes(&blob_writes)
            .await?;
    }

    let (mut changelog_rows, untracked_rows): (Vec<_>, Vec<_>) = staged_writes
        .state_rows
        .into_iter()
        .partition(|row| !row.untracked);
    let finalized = finalize_commit_rows(
        staged_writes.commit_members_by_version,
        live_state,
        transaction,
    )
    .await?;
    changelog_rows.extend(finalized.commit_rows);
    let version_ref_rows = finalized.version_ref_rows;

    if changelog_rows.is_empty() && untracked_rows.is_empty() && version_ref_rows.is_empty() {
        return Ok(());
    }

    if !changelog_rows.is_empty() {
        let canonical_changes = changelog_rows
            .iter()
            .map(canonical_change_from_staged_row)
            .collect::<Result<Vec<_>, _>>()?;
        {
            let mut writer = changelog.writer(&mut *transaction);
            writer.append_changes(&canonical_changes).await?;
        }
    }

    // TODO(engine2): live_state should eventually catch up from changelog
    // rather than being mirrored here. Keeping this bridge makes the current
    // SQL read path see durable writes immediately while changelog becomes
    // the source of truth.
    let live_state_rows = changelog_rows
        .into_iter()
        .chain(untracked_rows)
        .chain(version_ref_rows)
        .map(LiveStateRow::from)
        .collect::<Vec<_>>();

    live_state
        .writer(transaction)
        .write_rows(&live_state_rows)
        .await
}

fn canonical_change_from_staged_row(row: &StagedStateRow) -> Result<CanonicalChange, LixError> {
    let Some(change_id) = row.change_id.as_ref() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked staged row is missing change_id before changelog append",
        ));
    };

    Ok(CanonicalChange {
        id: change_id.clone(),
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: row.schema_version.clone(),
        file_id: row.file_id.clone(),
        plugin_key: row.plugin_key.clone(),
        snapshot_content: row.snapshot_content.clone(),
        metadata: row.metadata.clone(),
        created_at: row.created_at.clone(),
    })
}

/// Materializes tracked staged membership into `lix_commit` rows.
///
/// Staging only accumulates `version_id -> change_ids` because commit ids,
/// parent heads, and commit-row timestamps belong to transaction finalization.
/// This function turns those membership sets into normal `StagedStateRow`s with
/// `schema_key = "lix_commit"`, so the changelog/live_state flush can treat
/// commit rows exactly like any other staged state row.
///
/// Commit finalization output split by durability target.
///
/// `commit_rows` are tracked rows. They are ordinary changelog facts and are
/// also mirrored into live_state for immediate visibility.
///
/// `version_ref_rows` are moving refs. They are untracked rows and must only be
/// written through live_state/untracked_state, never appended to changelog.
struct FinalizedCommitRows {
    commit_rows: Vec<StagedStateRow>,
    version_ref_rows: Vec<StagedStateRow>,
}

async fn finalize_commit_rows(
    commit_members_by_version: BTreeMap<String, StagedCommitMembers>,
    live_state: &LiveStateContext,
    transaction: &mut dyn LixBackendTransaction,
) -> Result<FinalizedCommitRows, LixError> {
    let mut commit_rows = Vec::new();
    let mut version_ref_rows = Vec::new();

    for (version_id, members) in commit_members_by_version {
        if members.is_empty() {
            continue;
        }

        let commit_id = members.commit_id;
        let commit_change_id = members.commit_change_id;
        let timestamp = members.created_at;
        let change_ids = members.change_ids.into_iter().collect::<Vec<_>>();
        let parent_commit_ids = load_version_ref_head(live_state, transaction, &version_id)
            .await?
            .into_iter()
            .collect::<Vec<_>>();
        let snapshot_content = serde_json::to_string(&serde_json::json!({
            "id": commit_id,
            "change_ids": change_ids,
            "parent_commit_ids": parent_commit_ids,
        }))
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("engine2 commit row snapshot serialization failed: {error}"),
            )
        })?;

        commit_rows.push(StagedStateRow {
            entity_id: commit_id.clone(),
            schema_key: "lix_commit".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(snapshot_content),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: timestamp.clone(),
            updated_at: timestamp.clone(),
            global: true,
            change_id: Some(commit_change_id),
            commit_id: Some(commit_id.clone()),
            untracked: false,
            version_id: GLOBAL_VERSION_ID.to_string(),
        });
        version_ref_rows.push(plan_version_ref_row(version_id, commit_id, timestamp)?);
    }

    Ok(FinalizedCommitRows {
        commit_rows,
        version_ref_rows,
    })
}

async fn load_version_ref_head(
    live_state: &LiveStateContext,
    transaction: &mut dyn LixBackendTransaction,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    let Some(row) = ({
        let reader = live_state.reader(&mut *transaction);
        reader
            .load_row(&LiveStateRowRequest {
                schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: version_id.to_string(),
                file_id: crate::NullableKeyFilter::Null,
            })
            .await?
    }) else {
        return Ok(None);
    };
    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Ok(None);
    };
    let snapshot =
        serde_json::from_str::<serde_json::Value>(snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("engine2 version-ref snapshot parse failed: {error}"),
            )
        })?;
    Ok(snapshot
        .get("commit_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string))
}

/// Plans the untracked version-ref row that advances one version head.
///
/// Version refs are moving local pointers, not changelog facts. They are
/// written through live_state as untracked rows after the corresponding
/// tracked `lix_commit` row has been generated.
fn plan_version_ref_row(
    version_id: String,
    commit_id: String,
    timestamp: String,
) -> Result<StagedStateRow, LixError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "id": version_id,
        "commit_id": commit_id,
    }))
    .map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("engine2 version-ref snapshot serialization failed: {error}"),
        )
    })?;

    Ok(StagedStateRow {
        entity_id: version_id,
        schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
        file_id: None,
        plugin_key: None,
        snapshot_content: Some(snapshot_content),
        metadata: None,
        schema_version: VERSION_REF_SCHEMA_VERSION.to_string(),
        created_at: timestamp.clone(),
        updated_at: timestamp,
        global: true,
        change_id: None,
        commit_id: None,
        untracked: true,
        version_id: GLOBAL_VERSION_ID.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use serde_json::Value as JsonValue;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
    use crate::engine2::changelog::ChangelogContext;
    use crate::engine2::live_state::{LiveStateContext, LiveStateRowRequest};
    use crate::engine2::untracked_state::{
        UntrackedStateContext, UntrackedStateRow, UntrackedStateRowRequest,
    };
    use crate::NullableKeyFilter;

    #[tokio::test]
    async fn commit_staged_writes_appends_changelog_changes_before_live_state_mirror() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let live_state = LiveStateContext::new();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        commit_staged_writes(
            &binary_cas,
            &changelog,
            &live_state,
            transaction.as_mut(),
            StagedWriteSet {
                state_rows: vec![tracked_global_row("change-1")],
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members(["change-1"]),
                )]),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("commit should flush staged rows");
        transaction
            .commit()
            .await
            .expect("commit should persist kv");

        let changes = {
            let mut reader = changelog.reader(Arc::clone(&backend));
            reader
                .scan_changes(&crate::engine2::changelog::ChangelogScanRequest::default())
                .await
        }
        .expect("changelog scan should succeed");
        let change_ids = changes
            .iter()
            .map(|change| change.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(change_ids, vec!["change-1", "test-uuid-2"]);
        assert!(changes
            .iter()
            .any(|change| change.schema_key == "lix_commit"));
        assert!(!changes
            .iter()
            .any(|change| change.schema_key == VERSION_REF_SCHEMA_KEY));

        let version_ref = live_state
            .reader(Arc::clone(&backend))
            .load_row(&LiveStateRowRequest {
                schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: GLOBAL_VERSION_ID.to_string(),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("version ref load should succeed")
            .expect("version ref should be visible through live_state");
        assert!(version_ref.untracked);
        let version_ref_snapshot = serde_json::from_str::<JsonValue>(
            version_ref
                .snapshot_content
                .as_deref()
                .expect("version ref should have snapshot"),
        )
        .expect("version ref snapshot should be JSON");
        assert_eq!(
            version_ref_snapshot
                .get("commit_id")
                .and_then(JsonValue::as_str),
            Some("test-uuid-1")
        );
    }

    #[tokio::test]
    async fn commit_with_only_untracked_writes_does_not_create_lix_commit() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let live_state = LiveStateContext::new();
        let untracked_state = UntrackedStateContext::new();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        commit_staged_writes(
            &binary_cas,
            &changelog,
            &live_state,
            transaction.as_mut(),
            StagedWriteSet {
                state_rows: vec![untracked_global_row("change-untracked")],
                commit_members_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("commit should flush untracked row");
        transaction
            .commit()
            .await
            .expect("commit should persist kv");

        let changes = {
            let mut reader = changelog.reader(Arc::clone(&backend));
            reader
                .scan_changes(&crate::engine2::changelog::ChangelogScanRequest::default())
                .await
        }
        .expect("changelog scan should succeed");
        assert!(changes.is_empty());

        let loaded = {
            let mut untracked_reader = untracked_state.reader(Arc::clone(&backend));
            untracked_reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "test_schema".to_string(),
                    version_id: GLOBAL_VERSION_ID.to_string(),
                    entity_id: "entity-1".to_string(),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("untracked row load should succeed")
        .expect("untracked row should be persisted");
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );
    }

    #[tokio::test]
    async fn tracked_write_deletes_matching_untracked_overlay() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let untracked_state = UntrackedStateContext::new();
        let live_state = LiveStateContext::new();

        let mut seed_transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("seed transaction should open");
        untracked_state
            .writer(seed_transaction.as_mut())
            .write_rows(&[UntrackedStateRow::from(untracked_global_row(
                "change-untracked",
            ))])
            .await
            .expect("untracked seed should write");
        seed_transaction
            .commit()
            .await
            .expect("seed transaction should persist");

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        commit_staged_writes(
            &binary_cas,
            &changelog,
            &live_state,
            transaction.as_mut(),
            StagedWriteSet {
                state_rows: vec![tracked_global_row("change-tracked")],
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members(["change-tracked"]),
                )]),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("tracked commit should flush");
        transaction
            .commit()
            .await
            .expect("commit should persist kv");

        let untracked = {
            let mut untracked_reader = untracked_state.reader(Arc::clone(&backend));
            untracked_reader.load_row(&untracked_request()).await
        }
        .expect("untracked load should succeed");
        assert_eq!(untracked, None);

        let visible = live_state
            .reader(Arc::clone(&backend))
            .load_row(&live_state_request())
            .await
            .expect("live-state load should succeed")
            .expect("tracked row should be visible");
        assert!(!visible.untracked);
        assert_eq!(visible.change_id.as_deref(), Some("change-tracked"));
        assert_eq!(visible.snapshot_content.as_deref(), Some("{\"value\":1}"));
    }

    #[tokio::test]
    async fn finalize_commit_rows_parents_global_commit_to_existing_version_ref() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = LiveStateContext::new();
        seed_version_ref(&backend, &live_state, GLOBAL_VERSION_ID, "initial-commit").await;

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_VERSION_ID.to_string(),
                members(["change-a", "change-b"]),
            )]),
            &live_state,
            transaction.as_mut(),
        )
        .await
        .expect("global commit row should finalize");

        assert_eq!(rows.commit_rows.len(), 1);
        assert_eq!(rows.version_ref_rows.len(), 1);
        let row = &rows.commit_rows[0];
        assert_eq!(row.entity_id, "test-uuid-1");
        assert_eq!(row.schema_key, "lix_commit");
        assert_eq!(row.schema_version, "1");
        assert_eq!(row.change_id.as_deref(), Some("test-uuid-2"));
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
                .iter()
                .map(|value| value.as_str().expect("parent id should be string"))
                .collect::<Vec<_>>(),
            vec!["initial-commit"]
        );

        let version_ref_row = &rows.version_ref_rows[0];
        assert_eq!(version_ref_row.entity_id, GLOBAL_VERSION_ID);
        assert_eq!(version_ref_row.schema_key, VERSION_REF_SCHEMA_KEY);
        assert!(version_ref_row.untracked);
        assert_eq!(version_ref_row.change_id, None);
        assert_eq!(version_ref_row.commit_id, None);
    }

    #[tokio::test]
    async fn finalize_commit_rows_skips_empty_members() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = LiveStateContext::new();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_VERSION_ID.to_string(),
                StagedCommitMembers::default(),
            )]),
            &live_state,
            transaction.as_mut(),
        )
        .await
        .expect("empty members should be ignored");

        assert!(rows.commit_rows.is_empty());
        assert!(rows.version_ref_rows.is_empty());
    }

    #[tokio::test]
    async fn finalize_commit_rows_uses_existing_version_ref_as_parent() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let live_state = LiveStateContext::new();
        seed_version_ref(&backend, &live_state, "version-a", "previous-commit").await;

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([("version-a".to_string(), members(["change-a"]))]),
            &live_state,
            transaction.as_mut(),
        )
        .await
        .expect("active-version commit finalization should resolve parent");

        let snapshot = serde_json::from_str::<JsonValue>(
            rows.commit_rows[0]
                .snapshot_content
                .as_deref()
                .expect("commit row should have snapshot"),
        )
        .expect("commit snapshot should be JSON");
        assert_eq!(
            snapshot
                .get("parent_commit_ids")
                .and_then(JsonValue::as_array)
                .expect("parent_commit_ids should be array")
                .iter()
                .map(|value| value.as_str().expect("parent id should be text"))
                .collect::<Vec<_>>(),
            vec!["previous-commit"]
        );
        assert_eq!(rows.version_ref_rows[0].entity_id, "version-a");
    }

    #[test]
    fn plan_version_ref_row_creates_untracked_global_ref_for_touched_version() {
        let row = plan_version_ref_row(
            "version-a".to_string(),
            "commit-a".to_string(),
            "2026-01-01T00:00:00Z".to_string(),
        )
        .expect("version ref row should plan");

        assert_eq!(row.entity_id, "version-a");
        assert_eq!(row.schema_key, VERSION_REF_SCHEMA_KEY);
        assert_eq!(row.schema_version, VERSION_REF_SCHEMA_VERSION);
        assert!(row.global);
        assert!(row.untracked);
        assert_eq!(row.version_id, GLOBAL_VERSION_ID);
        assert_eq!(row.created_at, "2026-01-01T00:00:00Z");
        assert_eq!(row.updated_at, "2026-01-01T00:00:00Z");

        let snapshot = serde_json::from_str::<JsonValue>(
            row.snapshot_content
                .as_deref()
                .expect("version ref should have snapshot"),
        )
        .expect("version ref snapshot should be JSON");
        assert_eq!(
            snapshot.get("id").and_then(JsonValue::as_str),
            Some("version-a")
        );
        assert_eq!(
            snapshot.get("commit_id").and_then(JsonValue::as_str),
            Some("commit-a")
        );
    }

    fn members<const N: usize>(change_ids: [&str; N]) -> StagedCommitMembers {
        let mut members = StagedCommitMembers::new(
            "test-uuid-1".to_string(),
            "test-uuid-2".to_string(),
            "test-timestamp-1".to_string(),
        );
        for change_id in change_ids {
            members.add_change_id(change_id.to_string());
        }
        members
    }

    async fn seed_version_ref(
        backend: &Arc<dyn LixBackend + Send + Sync>,
        live_state: &LiveStateContext,
        version_id: &str,
        commit_id: &str,
    ) {
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("seed transaction should open");
        live_state
            .writer(transaction.as_mut())
            .write_rows(&[LiveStateRow::from(
                plan_version_ref_row(
                    version_id.to_string(),
                    commit_id.to_string(),
                    "2026-01-01T00:00:00Z".to_string(),
                )
                .expect("version ref should plan"),
            )])
            .await
            .expect("version ref should write");
        transaction
            .commit()
            .await
            .expect("seed transaction should persist");
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
            change_id: Some(change_id.to_string()),
            commit_id: Some("test-uuid-1".to_string()),
            untracked: false,
            version_id: GLOBAL_VERSION_ID.to_string(),
        }
    }

    fn untracked_global_row(change_id: &str) -> StagedStateRow {
        StagedStateRow {
            snapshot_content: Some("{\"value\":\"untracked\"}".to_string()),
            change_id: None,
            commit_id: None,
            untracked: true,
            ..tracked_global_row(change_id)
        }
    }

    fn untracked_request() -> UntrackedStateRowRequest {
        UntrackedStateRowRequest {
            schema_key: "test_schema".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: "entity-1".to_string(),
            file_id: NullableKeyFilter::Null,
        }
    }

    fn live_state_request() -> LiveStateRowRequest {
        LiveStateRowRequest {
            schema_key: "test_schema".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: "entity-1".to_string(),
            file_id: NullableKeyFilter::Null,
        }
    }
}
