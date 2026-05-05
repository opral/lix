use std::collections::BTreeMap;

use crate::binary_cas::BinaryCasContext;
use crate::changelog::{CanonicalChange, ChangelogContext};
use crate::functions::FunctionContext;
use crate::json_store::{JsonRef, JsonStoreContext, JsonStoreWriter};
use crate::live_state::{
    LiveStateContext, LiveStateRow, LiveStateTrackedRootWrite, LiveStateWriteBatch,
};
use crate::storage::{StorageReader, StorageWriteSet, StorageWriteTransaction};
use crate::transaction::prepare_version_ref_row;
use crate::transaction::staging::StagedWriteSet;
use crate::transaction::types::{StagedAdoptedStateRow, StagedCommitMembers, StagedStateRow};
use crate::version::{VersionContext, VersionRefReader};
use crate::GLOBAL_VERSION_ID;
use crate::{serialize_row_metadata, LixError, RowMetadata};

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
    runtime_functions: Option<&FunctionContext>,
    transaction: &mut (impl StorageWriteTransaction + ?Sized),
    staged_writes: StagedWriteSet,
) -> Result<(), LixError> {
    let mut writes = StorageWriteSet::new();
    let mut json_writer = JsonStoreContext::new().writer();

    if !staged_writes.file_data_writes.is_empty() {
        let mut blob_writer = binary_cas.writer(&mut writes);
        for write in &staged_writes.file_data_writes {
            blob_writer.stage_bytes(&write.data)?;
        }
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
    let tracked_roots = finalized.tracked_roots;

    if let Some(runtime_functions) = runtime_functions {
        let mut writer = live_state.writer(&mut *transaction);
        runtime_functions
            .stage_persist_if_needed(&mut writer, &mut writes, &mut json_writer)
            .await?;
    }

    if changelog_rows.is_empty()
        && adopted_rows.is_empty()
        && untracked_rows.is_empty()
        && version_heads.is_empty()
        && writes.is_empty()
    {
        return Ok(());
    }

    let canonical_changes = if !changelog_rows.is_empty() {
        let canonical_changes = new_canonical_changes(
            changelog,
            transaction,
            &mut writes,
            &mut json_writer,
            &changelog_rows,
        )
        .await?;
        {
            let mut writer = changelog.writer(&mut writes);
            writer.stage_changes(&canonical_changes)?;
        }
        canonical_changes
    } else {
        Vec::new()
    };
    let adopted_changes = if !adopted_rows.is_empty() {
        validate_adopted_canonical_changes(changelog, transaction, &adopted_rows).await?
    } else {
        Vec::new()
    };

    // The serving projection is updated in the same backend transaction as the
    // changelog append. Tracked rows become prolly mutations under their owning
    // commit root; untracked rows remain in the separate local overlay store.
    let live_state_batch = live_state_batch_from_committed_rows(
        &mut writes,
        &mut json_writer,
        &changelog_rows,
        &canonical_changes,
        &adopted_rows,
        &adopted_changes,
        &untracked_rows,
        tracked_roots,
    )?;

    {
        let mut writer = live_state.writer(&mut *transaction);
        writer.stage_rows(&mut writes, live_state_batch).await?;
    }

    for version_head in version_heads {
        let canonical_row = prepare_version_ref_row(
            &mut writes,
            &mut json_writer,
            &version_head.version_id,
            &version_head.commit_id,
            &version_head.timestamp,
        )?;
        version_ctx.stage_canonical_ref_rows(&mut writes, &[canonical_row])?;
    }

    writes.apply(transaction).await?;
    Ok(())
}

async fn new_canonical_changes(
    changelog: &ChangelogContext,
    transaction: &mut (impl StorageReader + ?Sized),
    writes: &mut StorageWriteSet,
    json_writer: &mut JsonStoreWriter,
    rows: &[StagedStateRow],
) -> Result<Vec<CanonicalChange>, LixError> {
    let reader = changelog.reader(&mut *transaction);
    let mut changes = Vec::new();
    for row in rows {
        let change = canonical_change_from_staged_row(writes, json_writer, row)?;
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
    transaction: &mut (impl StorageReader + ?Sized),
    rows: &[StagedAdoptedStateRow],
) -> Result<Vec<CanonicalChange>, LixError> {
    let mut writes = StorageWriteSet::new();
    let mut json_writer = JsonStoreContext::new().writer();
    let reader = changelog.reader(&mut *transaction);
    let mut changes = Vec::with_capacity(rows.len());
    for row in rows {
        let expected = canonical_change_from_adopted_row(&mut writes, &mut json_writer, row)?;
        match reader.load_change(&expected.id).await? {
            Some(existing) if existing == expected => changes.push(existing),
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
    Ok(changes)
}

fn live_state_batch_from_committed_rows(
    writes: &mut StorageWriteSet,
    json_writer: &mut JsonStoreWriter,
    changelog_rows: &[StagedStateRow],
    canonical_changes: &[CanonicalChange],
    adopted_rows: &[StagedAdoptedStateRow],
    adopted_changes: &[CanonicalChange],
    untracked_rows: &[StagedStateRow],
    tracked_roots: Vec<PendingTrackedRoot>,
) -> Result<LiveStateWriteBatch, LixError> {
    let mut tracked_rows_by_commit = BTreeMap::<String, Vec<LiveStateRow>>::new();
    for (row, change) in changelog_rows.iter().zip(canonical_changes) {
        let Some(commit_id) = row.commit_id.as_ref() else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked staged row is missing commit_id before live_state write",
            ));
        };
        tracked_rows_by_commit
            .entry(commit_id.clone())
            .or_default()
            .push(live_state_row_from_canonical_change(row, change, commit_id));
    }
    for (row, change) in adopted_rows.iter().zip(adopted_changes) {
        tracked_rows_by_commit
            .entry(row.commit_id.clone())
            .or_default()
            .push(live_state_row_from_adopted_change(row, change));
    }

    let mut live_tracked_roots = Vec::new();
    for root in tracked_roots {
        let rows = tracked_rows_by_commit
            .remove(&root.commit_id)
            .unwrap_or_default();
        live_tracked_roots.push(LiveStateTrackedRootWrite {
            commit_id: root.commit_id,
            parent_commit_id: root.parent_commit_id,
            rows,
        });
    }
    if !tracked_rows_by_commit.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked live_state rows have no finalized root metadata for commit ids: {}",
                tracked_rows_by_commit
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    }

    let untracked_rows = untracked_rows
        .iter()
        .map(|row| live_state_row_from_untracked_staged_row(writes, json_writer, row))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(LiveStateWriteBatch {
        untracked_rows,
        tracked_roots: live_tracked_roots,
    })
}

fn live_state_row_from_canonical_change(
    row: &StagedStateRow,
    change: &CanonicalChange,
    commit_id: &str,
) -> LiveStateRow {
    LiveStateRow {
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        file_id: change.file_id.clone(),
        snapshot_ref: change.snapshot_ref.clone(),
        metadata_ref: change.metadata_ref.clone(),
        schema_version: change.schema_version.clone(),
        created_at: change.created_at.clone(),
        updated_at: row.updated_at.clone(),
        global: row.global,
        change_id: Some(change.id.clone()),
        commit_id: Some(commit_id.to_string()),
        untracked: false,
        version_id: row.version_id.clone(),
    }
}

fn live_state_row_from_adopted_change(
    row: &StagedAdoptedStateRow,
    change: &CanonicalChange,
) -> LiveStateRow {
    LiveStateRow {
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        file_id: change.file_id.clone(),
        snapshot_ref: change.snapshot_ref.clone(),
        metadata_ref: change.metadata_ref.clone(),
        schema_version: change.schema_version.clone(),
        created_at: change.created_at.clone(),
        updated_at: row.updated_at.clone(),
        global: row.global,
        change_id: Some(change.id.clone()),
        commit_id: Some(row.commit_id.clone()),
        untracked: false,
        version_id: row.version_id.clone(),
    }
}

fn live_state_row_from_untracked_staged_row(
    writes: &mut StorageWriteSet,
    json_writer: &mut JsonStoreWriter,
    row: &StagedStateRow,
) -> Result<LiveStateRow, LixError> {
    Ok(LiveStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot_ref: stage_optional_json(writes, json_writer, row.snapshot_content.as_deref())?,
        metadata_ref: stage_optional_metadata(writes, json_writer, row.metadata.as_ref())?,
        schema_version: row.schema_version.clone(),
        created_at: row.created_at.clone(),
        updated_at: row.updated_at.clone(),
        global: row.global,
        change_id: None,
        commit_id: None,
        untracked: true,
        version_id: row.version_id.clone(),
    })
}

fn canonical_change_from_staged_row(
    writes: &mut StorageWriteSet,
    json_writer: &mut JsonStoreWriter,
    row: &StagedStateRow,
) -> Result<CanonicalChange, LixError> {
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
        snapshot_ref: stage_optional_json(writes, json_writer, row.snapshot_content.as_deref())?,
        metadata_ref: stage_optional_metadata(writes, json_writer, row.metadata.as_ref())?,
        created_at: row.created_at.clone(),
    })
}

fn stage_optional_json(
    writes: &mut StorageWriteSet,
    json_writer: &mut JsonStoreWriter,
    value: Option<&str>,
) -> Result<Option<JsonRef>, LixError> {
    let Some(value) = value else {
        return Ok(None);
    };
    json_writer.stage_bytes(writes, value.as_bytes()).map(Some)
}

fn stage_optional_metadata(
    writes: &mut StorageWriteSet,
    json_writer: &mut JsonStoreWriter,
    value: Option<&RowMetadata>,
) -> Result<Option<JsonRef>, LixError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let serialized = serialize_row_metadata(value);
    json_writer
        .stage_bytes(writes, serialized.as_bytes())
        .map(Some)
}

fn canonical_change_from_adopted_row(
    writes: &mut StorageWriteSet,
    json_writer: &mut JsonStoreWriter,
    row: &StagedAdoptedStateRow,
) -> Result<CanonicalChange, LixError> {
    Ok(CanonicalChange {
        id: row.change_id.clone(),
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: row.schema_version.clone(),
        file_id: row.file_id.clone(),
        snapshot_ref: stage_optional_json(writes, json_writer, row.snapshot_content.as_deref())?,
        metadata_ref: stage_optional_metadata(writes, json_writer, row.metadata.as_ref())?,
        created_at: row.created_at.clone(),
    })
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
    tracked_roots: Vec<PendingTrackedRoot>,
}

struct PendingVersionHead {
    version_id: String,
    commit_id: String,
    timestamp: String,
}

struct PendingTrackedRoot {
    commit_id: String,
    parent_commit_id: Option<String>,
}

async fn finalize_commit_rows(
    commit_members_by_version: BTreeMap<String, StagedCommitMembers>,
    extra_commit_parents_by_version: BTreeMap<String, Vec<String>>,
    version_ctx: &VersionContext,
    transaction: &mut (impl StorageReader + ?Sized),
) -> Result<FinalizedCommitRows, LixError> {
    let mut commit_rows = Vec::new();
    let mut version_heads = Vec::new();
    let mut tracked_roots = Vec::new();

    for (version_id, members) in commit_members_by_version {
        if members.is_empty() && !members.allow_empty {
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
        let parent_commit_id = parent_commit_ids.first().cloned();
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
            origin: None,
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
            commit_id: commit_id.clone(),
            timestamp,
        });
        tracked_roots.push(PendingTrackedRoot {
            commit_id,
            parent_commit_id,
        });
    }

    Ok(FinalizedCommitRows {
        commit_rows,
        version_heads,
        tracked_roots,
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
    use std::collections::BTreeMap;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use async_trait::async_trait;
    use serde_json::Value as JsonValue;

    use super::*;
    use crate::backend::{
        testing::UnitTestBackend, Backend, BackendKvEntryPage, BackendKvExistsBatch,
        BackendKvGetRequest, BackendKvKeyPage, BackendKvScanRequest, BackendKvValueBatch,
        BackendKvValuePage, BackendKvWriteBatch, BackendKvWriteStats, BackendReadTransaction,
        BackendWriteTransaction,
    };
    use crate::changelog::ChangelogContext;
    use crate::live_state::{
        LiveStateContext, LiveStateRow, LiveStateRowRequest, LiveStateWriteBatch,
    };
    use crate::storage::StorageContext;
    use crate::untracked_state::{
        MaterializedUntrackedStateRow, UntrackedStateContext, UntrackedStateRowRequest,
    };
    use crate::version::VersionContext;
    use crate::NullableKeyFilter;

    const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
    const DETERMINISTIC_SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            crate::untracked_state::UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(crate::changelog::ChangelogContext::new()),
        )
    }

    #[tokio::test]
    async fn commit_staged_writes_appends_changelog_and_updates_serving_projection() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let live_state = Arc::new(live_state_context());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        commit_staged_writes(
            &binary_cas,
            &changelog,
            live_state.as_ref(),
            &version_ctx,
            None,
            transaction.as_mut(),
            StagedWriteSet {
                insert_identities: BTreeMap::new(),
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
            let reader = changelog.reader(storage.clone());
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
            .ref_reader(storage.clone())
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref load should succeed");
        assert_eq!(loaded_head.as_deref(), Some("test-uuid-1"));
    }

    #[tokio::test]
    async fn commit_with_only_untracked_writes_does_not_create_lix_commit() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let live_state = Arc::new(live_state_context());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        let untracked_state = UntrackedStateContext::new();
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        commit_staged_writes(
            &binary_cas,
            &changelog,
            live_state.as_ref(),
            &version_ctx,
            None,
            transaction.as_mut(),
            StagedWriteSet {
                insert_identities: BTreeMap::new(),
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
            let reader = changelog.reader(storage.clone());
            reader
                .scan_changes(&crate::changelog::ChangelogScanRequest::default())
                .await
        }
        .expect("changelog scan should succeed");
        assert!(changes.is_empty());

        let loaded = {
            let mut untracked_reader = untracked_state.reader(storage.clone());
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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let untracked_state = UntrackedStateContext::new();
        let live_state = Arc::new(live_state_context());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));

        let mut seed_transaction = storage
            .begin_write_transaction()
            .await
            .expect("seed transaction should open");
        let mut writes = StorageWriteSet::new();
        let canonical_row = {
            let mut json_writer = JsonStoreContext::new().writer();
            crate::test_support::untracked_state_row_from_materialized(
                &mut writes,
                &mut json_writer,
                &MaterializedUntrackedStateRow::from(untracked_global_row("change-untracked")),
            )
            .expect("untracked seed should canonicalize")
        };
        untracked_state
            .writer(&mut writes)
            .stage_rows(&[canonical_row])
            .expect("untracked seed should write");
        writes
            .apply(&mut seed_transaction.as_mut())
            .await
            .expect("untracked seed should apply");
        seed_transaction
            .commit()
            .await
            .expect("seed transaction should persist");

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        commit_staged_writes(
            &binary_cas,
            &changelog,
            live_state.as_ref(),
            &version_ctx,
            None,
            transaction.as_mut(),
            StagedWriteSet {
                insert_identities: BTreeMap::new(),
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
            let mut untracked_reader = untracked_state.reader(storage.clone());
            untracked_reader.load_row(&untracked_request()).await
        }
        .expect("untracked load should succeed");
        assert_eq!(untracked, None);

        let visible = live_state
            .reader(storage.clone())
            .load_row(&live_state_request())
            .await
            .expect("live-state load should succeed")
            .expect("tracked row should be visible");
        assert!(!visible.untracked);
        assert_eq!(visible.change_id.as_deref(), Some("change-tracked"));
        assert_eq!(visible.snapshot_content.as_deref(), Some("{\"value\":1}"));
    }

    #[tokio::test]
    async fn commit_staged_writes_applies_cross_subsystem_rows_as_one_backend_batch() {
        let counting_backend = Arc::new(CountingBackend::new());
        let write_batches = counting_backend.write_batches();
        let backend: Arc<dyn Backend + Send + Sync> = counting_backend;
        let storage = StorageContext::new(backend);
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let live_state = Arc::new(live_state_context());
        let untracked_state = UntrackedStateContext::new();
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_global_version_head(storage.clone()).await;
        {
            let mut seed_transaction = storage
                .begin_write_transaction()
                .await
                .expect("seed transaction should open");
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            let mode_snapshot = serde_json::to_string(&serde_json::json!({
                "key": DETERMINISTIC_MODE_KEY,
                "value": { "enabled": true },
            }))
            .expect("mode snapshot should serialize");
            let mode_snapshot_ref = json_writer
                .stage_bytes(&mut writes, mode_snapshot.as_bytes())
                .expect("deterministic mode snapshot should stage");
            {
                let mut writer = live_state.writer(seed_transaction.as_mut());
                writer
                    .stage_rows(
                        &mut writes,
                        LiveStateWriteBatch {
                            untracked_rows: vec![LiveStateRow {
                                entity_id: crate::entity_identity::EntityIdentity::single(
                                    DETERMINISTIC_MODE_KEY,
                                ),
                                schema_key: "lix_key_value".to_string(),
                                file_id: None,
                                snapshot_ref: Some(mode_snapshot_ref),
                                metadata_ref: None,
                                schema_version: "1".to_string(),
                                created_at: "2026-01-01T00:00:00Z".to_string(),
                                updated_at: "2026-01-01T00:00:00Z".to_string(),
                                global: true,
                                change_id: None,
                                commit_id: None,
                                untracked: true,
                                version_id: GLOBAL_VERSION_ID.to_string(),
                            }],
                            tracked_roots: Vec::new(),
                        },
                    )
                    .await
                    .expect("deterministic mode should stage");
            }
            writes
                .apply(&mut seed_transaction.as_mut())
                .await
                .expect("deterministic mode should apply");
            seed_transaction
                .commit()
                .await
                .expect("seed transaction should persist");
        }
        write_batches.store(0, Ordering::SeqCst);
        let runtime_functions = {
            let reader = live_state.reader(storage.clone());
            FunctionContext::prepare(&reader)
                .await
                .expect("runtime context should prepare")
        };
        runtime_functions.provider().call_uuid_v7();
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        let mut untracked_row = untracked_global_row("change-untracked");
        untracked_row.entity_id = crate::entity_identity::EntityIdentity::single("entity-2");

        commit_staged_writes(
            &binary_cas,
            &changelog,
            live_state.as_ref(),
            &version_ctx,
            Some(&runtime_functions),
            transaction.as_mut(),
            StagedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![tracked_global_row("change-tracked"), untracked_row],
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
        .expect("cross-subsystem commit should stage and apply");

        assert_eq!(
            write_batches.load(Ordering::SeqCst),
            1,
            "tracked, json, untracked, changelog, and version refs must apply as one backend write batch"
        );

        transaction
            .commit()
            .await
            .expect("commit should persist kv");
        assert_eq!(write_batches.load(Ordering::SeqCst), 1);

        let changes = changelog
            .reader(storage.clone())
            .scan_changes(&crate::changelog::ChangelogScanRequest::default())
            .await
            .expect("changelog scan should succeed");
        assert!(changes.iter().any(|change| change.id == "change-tracked"));
        assert!(changes
            .iter()
            .any(|change| change.schema_key == "lix_commit"));

        let loaded_head = version_ctx
            .ref_reader(storage.clone())
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref load should succeed");
        assert_eq!(loaded_head.as_deref(), Some("test-uuid-1"));

        let untracked = {
            let mut untracked_reader = untracked_state.reader(storage.clone());
            untracked_reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "test_schema".to_string(),
                    version_id: GLOBAL_VERSION_ID.to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single("entity-2"),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("untracked row load should succeed")
        .expect("untracked row should persist");
        assert_eq!(
            untracked.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );

        let sequence_row = live_state
            .reader(storage.clone())
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single(
                    DETERMINISTIC_SEQUENCE_KEY,
                ),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("deterministic sequence should load")
            .expect("deterministic sequence should persist");
        assert_eq!(
            sequence_row.snapshot_content.as_deref(),
            Some("{\"key\":\"lix_deterministic_sequence_number\",\"value\":0}")
        );
    }

    #[tokio::test]
    async fn non_global_tracked_write_creates_one_commit_and_advances_only_touched_version() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let live_state = Arc::new(live_state_context());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(storage.clone(), GLOBAL_VERSION_ID, "global-before")
            .await;
        crate::test_support::seed_version_head(storage.clone(), "version-a", "version-a-before")
            .await;

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        commit_staged_writes(
            &binary_cas,
            &changelog,
            live_state.as_ref(),
            &version_ctx,
            None,
            transaction.as_mut(),
            StagedWriteSet {
                insert_identities: BTreeMap::new(),
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
            .reader(storage.clone())
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
            .ref_reader(storage.clone())
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("global head should load");
        let version_head = version_ctx
            .ref_reader(storage.clone())
            .load_head_commit_id("version-a")
            .await
            .expect("version head should load");
        assert_eq!(global_head.as_deref(), Some("global-before"));
        assert_eq!(version_head.as_deref(), Some("test-uuid-1"));
    }

    #[tokio::test]
    async fn finalize_commit_rows_parents_global_commit_to_existing_version_ref() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(
            storage.clone(),
            GLOBAL_VERSION_ID,
            "initial-commit",
        )
        .await;

        let mut transaction = storage
            .begin_write_transaction()
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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        let mut transaction = storage
            .begin_write_transaction()
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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(storage.clone(), GLOBAL_VERSION_ID, "global-before")
            .await;
        crate::test_support::seed_version_head(storage.clone(), "version-a", "previous-commit")
            .await;

        let mut transaction = storage
            .begin_write_transaction()
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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(storage.clone(), "version-a", "target-head").await;

        let mut transaction = storage
            .begin_write_transaction()
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
            origin: None,
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

    struct CountingBackend {
        inner: UnitTestBackend,
        write_batches: Arc<AtomicUsize>,
    }

    impl CountingBackend {
        fn new() -> Self {
            Self {
                inner: UnitTestBackend::new(),
                write_batches: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn write_batches(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.write_batches)
        }
    }

    #[async_trait]
    impl Backend for CountingBackend {
        async fn begin_read_transaction(
            &self,
        ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
            self.inner.begin_read_transaction().await
        }

        async fn begin_write_transaction(
            &self,
        ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
            Ok(Box::new(CountingWriteTransaction {
                inner: self.inner.begin_write_transaction().await?,
                write_batches: Arc::clone(&self.write_batches),
            }))
        }
    }

    struct CountingWriteTransaction {
        inner: Box<dyn BackendWriteTransaction + Send + Sync + 'static>,
        write_batches: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BackendReadTransaction for CountingWriteTransaction {
        async fn get_values(
            &mut self,
            request: BackendKvGetRequest,
        ) -> Result<BackendKvValueBatch, LixError> {
            self.inner.get_values(request).await
        }

        async fn exists_many(
            &mut self,
            request: BackendKvGetRequest,
        ) -> Result<BackendKvExistsBatch, LixError> {
            self.inner.exists_many(request).await
        }

        async fn scan_keys(
            &mut self,
            request: BackendKvScanRequest,
        ) -> Result<BackendKvKeyPage, LixError> {
            self.inner.scan_keys(request).await
        }

        async fn scan_values(
            &mut self,
            request: BackendKvScanRequest,
        ) -> Result<BackendKvValuePage, LixError> {
            self.inner.scan_values(request).await
        }

        async fn scan_entries(
            &mut self,
            request: BackendKvScanRequest,
        ) -> Result<BackendKvEntryPage, LixError> {
            self.inner.scan_entries(request).await
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            let Self { inner, .. } = *self;
            inner.rollback().await
        }
    }

    #[async_trait]
    impl BackendWriteTransaction for CountingWriteTransaction {
        async fn write_kv_batch(
            &mut self,
            batch: BackendKvWriteBatch,
        ) -> Result<BackendKvWriteStats, LixError> {
            self.write_batches.fetch_add(1, Ordering::SeqCst);
            self.inner.write_kv_batch(batch).await
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            let Self { inner, .. } = *self;
            inner.commit().await
        }
    }
}
