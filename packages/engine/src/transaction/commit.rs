use std::collections::BTreeMap;

use crate::binary_cas::{BinaryBlobWrite, BinaryCasContext};
use crate::changelog::{CanonicalChange, ChangelogContext};
use crate::live_state::{LiveStateContext, LiveStateRow};
use crate::transaction::staging::StagedWriteSet;
use crate::transaction::types::{StagedAdoptedStateRow, StagedCommitMembers, StagedStateRow};
use crate::version::{VersionContext, VersionRefReader};
use crate::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError};

/// Commits transaction-staged rows into durable tracked and untracked stores.
///
/// Providers decode DataFusion DML into hydrated `StagedStateRow`s. Untracked
/// rows are durable local overlay state and bypass changelog/commit rows.
/// Tracked rows receive normal `lix_commit` rows, append canonical changelog
/// facts, then update the live-state serving projection. The tracked side of
/// that projection is a prolly root keyed by the new commit id.
pub(crate) async fn commit_staged_writes(
    binary_cas: &BinaryCasContext,
    changelog: &ChangelogContext,
    live_state: &LiveStateContext,
    version_ctx: &VersionContext,
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
    let adopted_rows = staged_writes.adopted_rows;
    let finalized = finalize_commit_rows(
        staged_writes.commit_members_by_version,
        staged_writes.extra_commit_parents_by_version,
        version_ctx,
        transaction,
    )
    .await?;
    changelog_rows.extend(finalized.commit_rows);
    let version_heads = finalized.version_heads;

    if changelog_rows.is_empty()
        && adopted_rows.is_empty()
        && untracked_rows.is_empty()
        && version_heads.is_empty()
    {
        return Ok(());
    }

    if !changelog_rows.is_empty() {
        let canonical_changes =
            new_canonical_changes(changelog, transaction, &changelog_rows).await?;
        {
            let mut writer = changelog.writer(&mut *transaction);
            writer.append_changes(&canonical_changes).await?;
        }
    }
    if !adopted_rows.is_empty() {
        validate_adopted_canonical_changes(changelog, transaction, &adopted_rows).await?;
    }

    // The serving projection is updated in the same backend transaction as the
    // changelog append. Tracked rows become prolly mutations under their owning
    // commit root; untracked rows remain in the separate local overlay store.
    let live_state_rows = changelog_rows
        .into_iter()
        .map(LiveStateRow::from)
        .chain(adopted_rows.into_iter().map(LiveStateRow::from))
        .chain(untracked_rows.into_iter().map(LiveStateRow::from))
        .collect::<Vec<_>>();

    {
        let mut writer = live_state.writer(&mut *transaction);
        writer.write_rows(&live_state_rows).await?;
    }

    for version_head in version_heads {
        version_ctx
            .advance_ref(
                &mut *transaction,
                &version_head.version_id,
                &version_head.commit_id,
                &version_head.timestamp,
            )
            .await?;
    }
    Ok(())
}

async fn new_canonical_changes(
    changelog: &ChangelogContext,
    transaction: &mut dyn LixBackendTransaction,
    rows: &[StagedStateRow],
) -> Result<Vec<CanonicalChange>, LixError> {
    let reader = changelog.reader(&mut *transaction);
    let mut changes = Vec::new();
    for row in rows {
        let change = canonical_change_from_staged_row(row)?;
        match reader.load_change(&change.id).await? {
            Some(existing) => {
                let entity_id = existing
                    .entity_id
                    .as_string()
                    .unwrap_or_else(|_| "<invalid entity_id>".to_string());
                return Err(LixError::new(
                    LixError::CODE_UNIQUE,
                    format!(
                        "canonical change id '{}' already exists with different content for schema '{}' entity '{}'",
                        change.id,
                        existing.schema_key,
                        entity_id
                    ),
                ));
            }
            None => changes.push(change),
        }
    }
    Ok(changes)
}

async fn validate_adopted_canonical_changes(
    changelog: &ChangelogContext,
    transaction: &mut dyn LixBackendTransaction,
    rows: &[StagedAdoptedStateRow],
) -> Result<(), LixError> {
    let reader = changelog.reader(&mut *transaction);
    for row in rows {
        let expected = canonical_change_from_adopted_row(row);
        match reader.load_change(&expected.id).await? {
            Some(existing) if existing == expected => {}
            Some(existing) => {
                let entity_id = existing
                    .entity_id
                    .as_string()
                    .unwrap_or_else(|_| "<invalid entity_id>".to_string());
                return Err(LixError::new(
                    LixError::CODE_UNIQUE,
                    format!(
                        "adopted canonical change id '{}' exists with different content for schema '{}' entity '{}'",
                        expected.id, existing.schema_key, entity_id
                    ),
                ));
            }
            None => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "adopted canonical change id '{}' does not exist in the changelog",
                        expected.id
                    ),
                ));
            }
        }
    }
    Ok(())
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
        snapshot_content: row.snapshot_content.clone(),
        metadata: row.metadata.clone(),
        created_at: row.created_at.clone(),
    })
}

fn canonical_change_from_adopted_row(row: &StagedAdoptedStateRow) -> CanonicalChange {
    CanonicalChange {
        id: row.change_id.clone(),
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: row.schema_version.clone(),
        file_id: row.file_id.clone(),
        snapshot_content: row.snapshot_content.clone(),
        metadata: row.metadata.clone(),
        created_at: row.created_at.clone(),
    }
}

/// Materializes tracked staged membership into `lix_commit` rows.
///
/// Staging only accumulates `version_id -> change_ids` because commit ids,
/// parent heads, and commit-row timestamps belong to transaction finalization.
/// The `change_ids` list is the ordered set of canonical changes whose effects
/// the commit introduces relative to its first parent; merge commits may later
/// populate this list with existing source-parent changes instead of copied
/// changelog facts.
/// This function turns those membership sets into normal `StagedStateRow`s with
/// `schema_key = "lix_commit"`, so the changelog/live_state flush can treat
/// commit rows exactly like any other staged state row.
///
/// Commit finalization output split by durability target.
///
/// `commit_rows` are ordinary changelog facts. live_state later projects them
/// from commit_graph; tracked_state roots do not store commit graph facts.
///
/// `version_heads` are moving refs. They are written through `VersionContext`
/// and must never be appended to changelog.
struct FinalizedCommitRows {
    commit_rows: Vec<StagedStateRow>,
    version_heads: Vec<PendingVersionHead>,
}

struct PendingVersionHead {
    version_id: String,
    commit_id: String,
    timestamp: String,
}

async fn finalize_commit_rows(
    commit_members_by_version: BTreeMap<String, StagedCommitMembers>,
    extra_commit_parents_by_version: BTreeMap<String, Vec<String>>,
    version_ctx: &VersionContext,
    transaction: &mut dyn LixBackendTransaction,
) -> Result<FinalizedCommitRows, LixError> {
    let mut commit_rows = Vec::new();
    let mut version_heads = Vec::new();

    for (version_id, members) in commit_members_by_version {
        if members.is_empty() {
            continue;
        }

        let commit_id = members.commit_id;
        let commit_change_id = members.commit_change_id;
        let change_set_id = members.change_set_id;
        let timestamp = members.created_at;
        let change_ids = members.change_ids.into_iter().collect::<Vec<_>>();
        let parent_commit_ids = version_ctx
            .ref_reader(&mut *transaction)
            .load_head_commit_id(&version_id)
            .await?
            .into_iter()
            .collect::<Vec<_>>();
        let parent_commit_ids = merge_parent_commit_ids(
            parent_commit_ids,
            extra_commit_parents_by_version
                .get(&version_id)
                .cloned()
                .unwrap_or_default(),
        );
        let snapshot_content = serde_json::to_string(&serde_json::json!({
            "id": commit_id,
            "change_set_id": change_set_id,
            "change_ids": change_ids,
            "author_account_ids": [],
            "parent_commit_ids": parent_commit_ids,
        }))
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("engine2 commit row snapshot serialization failed: {error}"),
            )
        })?;

        commit_rows.push(StagedStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single(&commit_id),
            schema_key: "lix_commit".to_string(),
            file_id: None,
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
        version_heads.push(PendingVersionHead {
            version_id,
            commit_id,
            timestamp,
        });
    }

    Ok(FinalizedCommitRows {
        commit_rows,
        version_heads,
    })
}

fn merge_parent_commit_ids(mut base: Vec<String>, extra: Vec<String>) -> Vec<String> {
    for parent in extra {
        if !base.contains(&parent) {
            base.push(parent);
        }
    }
    base
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use serde_json::Value as JsonValue;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
    use crate::changelog::ChangelogContext;
    use crate::live_state::{LiveStateContext, LiveStateRowRequest};
    use crate::untracked_state::{
        UntrackedStateContext, UntrackedStateRow, UntrackedStateRowRequest,
    };
    use crate::version::VersionContext;
    use crate::NullableKeyFilter;

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            crate::untracked_state::UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(crate::changelog::ChangelogContext::new()),
        )
    }

    #[tokio::test]
    async fn commit_staged_writes_appends_changelog_and_updates_serving_projection() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let live_state = Arc::new(live_state_context());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        commit_staged_writes(
            &binary_cas,
            &changelog,
            live_state.as_ref(),
            &version_ctx,
            transaction.as_mut(),
            StagedWriteSet {
                insert_identities: BTreeSet::new(),
                state_rows: vec![tracked_global_row("change-1")],
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members(["change-1"]),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
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
            let reader = changelog.reader(Arc::clone(&backend));
            reader
                .scan_changes(&crate::changelog::ChangelogScanRequest::default())
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
            .any(|change| change.schema_key == "lix_version_ref"));

        let loaded_head = version_ctx
            .ref_reader(Arc::clone(&backend))
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref load should succeed");
        assert_eq!(loaded_head.as_deref(), Some("test-uuid-1"));
    }

    #[tokio::test]
    async fn commit_with_only_untracked_writes_does_not_create_lix_commit() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let live_state = Arc::new(live_state_context());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        let untracked_state = UntrackedStateContext::new();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");

        commit_staged_writes(
            &binary_cas,
            &changelog,
            live_state.as_ref(),
            &version_ctx,
            transaction.as_mut(),
            StagedWriteSet {
                insert_identities: BTreeSet::new(),
                state_rows: vec![untracked_global_row("change-untracked")],
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::new(),
                extra_commit_parents_by_version: BTreeMap::new(),
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
            let reader = changelog.reader(Arc::clone(&backend));
            reader
                .scan_changes(&crate::changelog::ChangelogScanRequest::default())
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
                    entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
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
        let live_state = Arc::new(live_state_context());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));

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
            live_state.as_ref(),
            &version_ctx,
            transaction.as_mut(),
            StagedWriteSet {
                insert_identities: BTreeSet::new(),
                state_rows: vec![tracked_global_row("change-tracked")],
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members(["change-tracked"]),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
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
    async fn non_global_tracked_write_creates_one_commit_and_advances_only_touched_version() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let live_state = Arc::new(live_state_context());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(
            backend.as_ref(),
            GLOBAL_VERSION_ID,
            "global-before",
        )
        .await;
        crate::test_support::seed_version_head(backend.as_ref(), "version-a", "version-a-before")
            .await;

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        commit_staged_writes(
            &binary_cas,
            &changelog,
            live_state.as_ref(),
            &version_ctx,
            transaction.as_mut(),
            StagedWriteSet {
                insert_identities: BTreeSet::new(),
                state_rows: vec![tracked_version_row("version-a", "change-version-a")],
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::from([(
                    "version-a".to_string(),
                    members(["change-version-a"]),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("version commit should flush");
        transaction
            .commit()
            .await
            .expect("commit should persist kv");

        let changes = changelog
            .reader(Arc::clone(&backend))
            .scan_changes(&crate::changelog::ChangelogScanRequest::default())
            .await
            .expect("changelog scan should succeed");
        let commit_changes = changes
            .iter()
            .filter(|change| change.schema_key == "lix_commit")
            .collect::<Vec<_>>();
        assert_eq!(
            commit_changes.len(),
            1,
            "a write to one non-global version must create exactly one commit"
        );
        assert_eq!(
            commit_changes[0]
                .entity_id
                .as_string()
                .expect("commit entity id should project"),
            "test-uuid-1"
        );
        assert!(changes.iter().any(|change| change.id == "change-version-a"));
        assert!(!changes
            .iter()
            .any(|change| change.schema_key == "lix_version_ref"));

        let global_head = version_ctx
            .ref_reader(Arc::clone(&backend))
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("global head should load");
        let version_head = version_ctx
            .ref_reader(Arc::clone(&backend))
            .load_head_commit_id("version-a")
            .await
            .expect("version head should load");
        assert_eq!(global_head.as_deref(), Some("global-before"));
        assert_eq!(version_head.as_deref(), Some("test-uuid-1"));
    }

    #[tokio::test]
    async fn finalize_commit_rows_parents_global_commit_to_existing_version_ref() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(
            backend.as_ref(),
            GLOBAL_VERSION_ID,
            "initial-commit",
        )
        .await;

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_VERSION_ID.to_string(),
                members(["change-a", "change-b"]),
            )]),
            BTreeMap::new(),
            &version_ctx,
            transaction.as_mut(),
        )
        .await
        .expect("global commit row should finalize");

        assert_eq!(rows.commit_rows.len(), 1);
        assert_eq!(rows.version_heads.len(), 1);
        let row = &rows.commit_rows[0];
        assert_eq!(row.entity_id.as_string().as_deref(), Ok("test-uuid-1"));
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

        let version_head = &rows.version_heads[0];
        assert_eq!(version_head.version_id, GLOBAL_VERSION_ID);
        assert_eq!(version_head.commit_id, "test-uuid-1");
    }

    #[tokio::test]
    async fn finalize_commit_rows_skips_empty_members() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_VERSION_ID.to_string(),
                StagedCommitMembers::default(),
            )]),
            BTreeMap::new(),
            &version_ctx,
            transaction.as_mut(),
        )
        .await
        .expect("empty members should be ignored");

        assert!(rows.commit_rows.is_empty());
        assert!(rows.version_heads.is_empty());
    }

    #[tokio::test]
    async fn finalize_commit_rows_uses_existing_version_ref_as_parent() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(
            backend.as_ref(),
            GLOBAL_VERSION_ID,
            "global-before",
        )
        .await;
        crate::test_support::seed_version_head(backend.as_ref(), "version-a", "previous-commit")
            .await;

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([("version-a".to_string(), members(["change-a"]))]),
            BTreeMap::new(),
            &version_ctx,
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
        assert_eq!(rows.version_heads[0].version_id, "version-a");
    }

    #[tokio::test]
    async fn finalize_commit_rows_appends_extra_merge_parent_after_target_head() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(backend.as_ref(), "version-a", "target-head").await;

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([("version-a".to_string(), members(["change-a"]))]),
            BTreeMap::from([("version-a".to_string(), vec!["source-head".to_string()])]),
            &version_ctx,
            transaction.as_mut(),
        )
        .await
        .expect("merge commit finalization should resolve parents");

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
            vec!["target-head", "source-head"]
        );
    }

    fn members<const N: usize>(change_ids: [&str; N]) -> StagedCommitMembers {
        let mut members = StagedCommitMembers::new(
            "test-uuid-1".to_string(),
            "test-uuid-2".to_string(),
            "test-uuid-3".to_string(),
            "test-timestamp-1".to_string(),
        );
        for change_id in change_ids {
            members.add_change_id(change_id.to_string());
        }
        members
    }

    fn tracked_global_row(change_id: &str) -> StagedStateRow {
        tracked_version_row(GLOBAL_VERSION_ID, change_id)
    }

    fn tracked_version_row(version_id: &str, change_id: &str) -> StagedStateRow {
        StagedStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot_content: Some("{\"value\":1}".to_string()),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == GLOBAL_VERSION_ID,
            change_id: Some(change_id.to_string()),
            commit_id: Some("test-uuid-1".to_string()),
            untracked: false,
            version_id: version_id.to_string(),
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
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            file_id: NullableKeyFilter::Null,
        }
    }

    fn live_state_request() -> LiveStateRowRequest {
        LiveStateRowRequest {
            schema_key: "test_schema".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            file_id: NullableKeyFilter::Null,
        }
    }
}
