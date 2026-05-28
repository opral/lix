use std::sync::Arc;

use std::collections::BTreeMap;

use crate::branch::BranchContext;
use crate::changelog::{
    ChangeLoadRequest, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogReader,
    ChangelogWriter, CommitChangeRef, CommitChangeRefSet, CommitRecord,
};
use crate::json_store::{JsonRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
use crate::storage::StorageContext;
use crate::storage::StorageRead;
use crate::storage::StorageWriteSet;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateContext, TrackedStateDeltaRef,
};
use crate::transaction::prepare_branch_ref_row;
use crate::untracked_state::{
    MaterializedUntrackedStateRow, UntrackedStateContext, UntrackedStateRow,
};

fn prepare_json_ref(value: &str) -> crate::json_store::JsonRef {
    crate::json_store::JsonRef::for_content(value.as_bytes())
}
use crate::GLOBAL_BRANCH_ID;

pub(crate) const TEST_EMPTY_ROOT_COMMIT_ID: &str = "test-empty-root";
const TEST_TIMESTAMP: &str = "1970-01-01T00:00:00.000Z";

fn test_timestamp() -> crate::common::LixTimestamp {
    crate::common::LixTimestamp::expect_parse("timestamp", TEST_TIMESTAMP)
}

/// Seeds a branch head and matching tracked root for unit tests.
///
/// A branch ref that points at a commit without a tracked root is invalid for
/// the serving state. This helper keeps that invariant in one place while
/// still letting low-level tests use synthetic commit ids.
pub(crate) async fn seed_branch_head(storage: StorageContext, branch_id: &str, commit_id: &str) {
    seed_branch_head_with_rows(storage, branch_id, commit_id, &[]).await;
}

/// Seeds the global branch head to an empty tracked root for unit tests.
pub(crate) async fn seed_global_branch_head(storage: StorageContext) {
    seed_branch_head(storage, GLOBAL_BRANCH_ID, TEST_EMPTY_ROOT_COMMIT_ID).await;
}

/// Seeds a branch head and writes the tracked root contents for its commit.
pub(crate) async fn seed_branch_head_with_rows(
    storage: StorageContext,
    branch_id: &str,
    commit_id: &str,
    rows: &[MaterializedTrackedStateRow],
) {
    let mut read = storage
        .begin_read(crate::storage::StorageReadOptions::default())
        .expect("seed read should open");
    let branch_ctx = BranchContext::new(Arc::new(UntrackedStateContext::new()));
    let mut writes = StorageWriteSet::new();
    let canonical_row = prepare_branch_ref_row(branch_id, commit_id, TEST_TIMESTAMP)
        .expect("branch ref should canonicalize");
    branch_ctx
        .stage_canonical_ref_rows(&mut writes, &[canonical_row.row])
        .expect("branch ref should stage");
    stage_tracked_root_from_materialized(
        &mut read,
        &mut writes,
        &TrackedStateContext::new(),
        commit_id,
        None,
        rows,
    )
    .await
    .expect("tracked root should write");
    storage
        .commit_write_set(writes, crate::storage::StorageWriteOptions::default())
        .expect("seed should commit");
}

pub(crate) async fn stage_tracked_root_from_materialized(
    read: &mut (impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    tracked_state: &TrackedStateContext,
    commit_id: &str,
    parent_commit_id: Option<&str>,
    rows: &[MaterializedTrackedStateRow],
) -> Result<(), crate::LixError> {
    let changes = rows
        .iter()
        .map(tracked_change_from_materialized)
        .collect::<Result<Vec<_>, _>>()?;
    let parent_ids = parent_commit_id
        .map(|parent| vec![parent.to_string()])
        .unwrap_or_default();
    let commit_change_id = format!("{commit_id}:commit");
    let staged = stage_test_changelog_commit(
        read,
        writes,
        commit_id,
        &commit_change_id,
        &parent_ids,
        rows,
        &changes,
    )
    .await?;
    let commit_snapshot = commit_row_snapshot_content(commit_id)?;
    let commit_snapshot_ref = JsonRef::for_content(commit_snapshot.as_bytes());
    let commit_entity_pk = crate::entity_pk::EntityPk::single(commit_id);
    let mut deltas = staged
        .change_commit_ids
        .iter()
        .map(|(row_index, change_commit_id)| {
            let change = &changes[*row_index];
            let row = &rows[*row_index];
            TrackedStateDeltaRef {
                schema_key: &change.schema_key,
                file_id: change.file_id.as_deref(),
                entity_pk: &change.entity_pk,
                change_id: &change.change_id,
                commit_id: change_commit_id,
                snapshot_ref: change.snapshot_ref.as_ref(),
                metadata_ref: change.metadata_ref.as_ref(),
                deleted: change.snapshot_ref.is_none(),
                created_at: crate::common::LixTimestamp::expect_parse(
                    "created_at",
                    &row.created_at,
                ),
                updated_at: crate::common::LixTimestamp::expect_parse(
                    "updated_at",
                    &row.updated_at,
                ),
            }
        })
        .collect::<Vec<_>>();
    deltas.push(TrackedStateDeltaRef {
        schema_key: "lix_commit",
        file_id: None,
        entity_pk: &commit_entity_pk,
        change_id: &staged.commit_change_id,
        commit_id,
        snapshot_ref: Some(&commit_snapshot_ref),
        metadata_ref: None,
        deleted: false,
        created_at: staged.commit_created_at,
        updated_at: staged.commit_created_at,
    });
    tracked_state
        .writer(read, writes)
        .stage_commit_root(commit_id, parent_commit_id, deltas)
        .await?;
    Ok(())
}

pub(crate) async fn stage_tracked_root_from_materialized_with_parents(
    read: &mut (impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    tracked_state: &TrackedStateContext,
    commit_id: &str,
    parent_ids: &[String],
    commit_root_parent_commit_id: Option<&str>,
    rows: &[MaterializedTrackedStateRow],
) -> Result<(), crate::LixError> {
    let changes = rows
        .iter()
        .map(tracked_change_from_materialized)
        .collect::<Result<Vec<_>, _>>()?;
    let commit_change_id = format!("{commit_id}:commit");
    let staged = stage_test_changelog_commit(
        read,
        writes,
        commit_id,
        &commit_change_id,
        parent_ids,
        rows,
        &changes,
    )
    .await?;
    let commit_snapshot = commit_row_snapshot_content(commit_id)?;
    let commit_snapshot_ref = JsonRef::for_content(commit_snapshot.as_bytes());
    let commit_entity_pk = crate::entity_pk::EntityPk::single(commit_id);
    let mut deltas = staged
        .change_commit_ids
        .iter()
        .map(|(row_index, change_commit_id)| {
            let change = &changes[*row_index];
            let row = &rows[*row_index];
            TrackedStateDeltaRef {
                schema_key: &change.schema_key,
                file_id: change.file_id.as_deref(),
                entity_pk: &change.entity_pk,
                change_id: &change.change_id,
                commit_id: change_commit_id,
                snapshot_ref: change.snapshot_ref.as_ref(),
                metadata_ref: change.metadata_ref.as_ref(),
                deleted: change.snapshot_ref.is_none(),
                created_at: crate::common::LixTimestamp::expect_parse(
                    "created_at",
                    &row.created_at,
                ),
                updated_at: crate::common::LixTimestamp::expect_parse(
                    "updated_at",
                    &row.updated_at,
                ),
            }
        })
        .collect::<Vec<_>>();
    deltas.push(TrackedStateDeltaRef {
        schema_key: "lix_commit",
        file_id: None,
        entity_pk: &commit_entity_pk,
        change_id: &staged.commit_change_id,
        commit_id,
        snapshot_ref: Some(&commit_snapshot_ref),
        metadata_ref: None,
        deleted: false,
        created_at: staged.commit_created_at,
        updated_at: staged.commit_created_at,
    });
    tracked_state
        .writer(read, writes)
        .stage_commit_root(commit_id, commit_root_parent_commit_id, deltas)
        .await?;
    Ok(())
}

pub(crate) async fn stage_empty_changelog_commit(
    read: &mut (impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: &str,
    parent_commit_id: Option<&str>,
) -> Result<(), crate::LixError> {
    let parent_ids = parent_commit_id
        .map(|parent| vec![parent.to_string()])
        .unwrap_or_default();
    let commit_change_id = format!("{commit_id}:commit");
    stage_test_changelog_commit(
        read,
        writes,
        commit_id,
        &commit_change_id,
        &parent_ids,
        &[],
        &[],
    )
    .await?;
    Ok(())
}

pub(crate) async fn stage_empty_changelog_commit_with_parents(
    read: &mut (impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: &str,
    parent_ids: &[String],
) -> Result<(), crate::LixError> {
    let commit_change_id = format!("{commit_id}:commit");
    stage_test_changelog_commit(
        read,
        writes,
        commit_id,
        &commit_change_id,
        parent_ids,
        &[],
        &[],
    )
    .await?;
    Ok(())
}

async fn stage_test_changelog_commit(
    mut read: &mut (impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: &str,
    commit_change_id: &str,
    parent_ids: &[String],
    rows: &[MaterializedTrackedStateRow],
    changes: &[ChangeRecord],
) -> Result<TestStagedChangelogCommit, crate::LixError> {
    let winner_indices = final_state_row_winner_indices(rows)?;
    let winner_change_ids = winner_indices
        .iter()
        .map(|&index| changes[index].change_id.clone())
        .collect::<Vec<_>>();
    let existing_change_ids = load_existing_changelog_change_ids(read, &winner_change_ids).await?;
    let mut append = ChangelogAppend::default();
    let mut refs = Vec::new();
    let mut change_commit_ids = Vec::new();
    let mut json_payloads = Vec::new();
    let mut seen_json_refs = std::collections::BTreeSet::new();
    for &row_index in &winner_indices {
        let row = &rows[row_index];
        let change = &changes[row_index];
        if !existing_change_ids.contains(&change.change_id) {
            for (json_ref, payload) in json_payloads_from_materialized(row) {
                if seen_json_refs.insert(json_ref.as_hash_bytes().to_vec()) {
                    json_payloads.push((json_ref, payload));
                }
            }
            append.changes.push(change.clone());
        }
        refs.push(commit_change_ref_from_change(change));
        change_commit_ids.push((row_index, row.commit_id.clone()));
    }
    let commit_snapshot = commit_row_snapshot_content(commit_id)?;
    let commit_snapshot_ref = JsonRef::for_content(commit_snapshot.as_bytes());
    json_payloads.push((commit_snapshot_ref, commit_snapshot));
    stage_json_payloads(writes, &json_payloads)?;
    let created_at = rows
        .first()
        .map(|row| crate::common::LixTimestamp::expect_parse("created_at", &row.created_at))
        .unwrap_or_else(test_timestamp);
    append.commits.push(CommitRecord {
        format_version: 1,
        commit_id: commit_id.to_string(),
        parent_commit_ids: parent_ids.to_vec(),
        change_id: commit_change_id.to_string(),
        author_account_ids: Vec::new(),
        created_at,
    });
    append.commit_change_refs.push(CommitChangeRefSet {
        commit_id: commit_id.to_string(),
        entries: refs,
    });
    let mut writer = ChangelogContext::new().writer(&mut read, writes);
    writer.stage_append(append).await?;
    change_commit_ids.sort_by_key(|(row_index, _)| *row_index);
    Ok(TestStagedChangelogCommit {
        change_commit_ids,
        commit_change_id: commit_change_id.to_string(),
        commit_created_at: created_at,
    })
}

fn commit_row_snapshot_content(commit_id: &str) -> Result<String, crate::LixError> {
    serde_json::to_string(&serde_json::json!({
        "id": commit_id,
    }))
    .map_err(|error| {
        crate::LixError::new(
            crate::LixError::CODE_INTERNAL_ERROR,
            format!("failed to encode lix_commit snapshot: {error}"),
        )
    })
}

struct TestStagedChangelogCommit {
    change_commit_ids: Vec<(usize, String)>,
    commit_change_id: String,
    commit_created_at: crate::common::LixTimestamp,
}

async fn load_existing_changelog_change_ids(
    read: &mut (impl StorageRead + Send + Sync + ?Sized),
    change_ids: &[String],
) -> Result<std::collections::BTreeSet<String>, crate::LixError> {
    if change_ids.is_empty() {
        return Ok(std::collections::BTreeSet::new());
    }
    let mut unique = change_ids.to_vec();
    unique.sort();
    unique.dedup();
    let mut reader = ChangelogContext::new().reader(&mut *read);
    let batch = reader
        .load_changes(ChangeLoadRequest {
            change_ids: &unique,
        })
        .await?;
    Ok(unique
        .into_iter()
        .zip(batch.entries)
        .filter_map(|(change_id, entry)| entry.map(|_| change_id))
        .collect())
}

fn final_state_row_winner_indices(
    rows: &[MaterializedTrackedStateRow],
) -> Result<Vec<usize>, crate::LixError> {
    let mut winners =
        BTreeMap::<(String, Option<String>, crate::entity_pk::EntityPk), usize>::new();
    for (index, row) in rows.iter().enumerate() {
        winners.insert(
            (
                row.schema_key.clone(),
                row.file_id.clone(),
                row.entity_pk.clone(),
            ),
            index,
        );
    }
    let mut indices = winners.into_values().collect::<Vec<_>>();
    indices.sort_unstable();
    Ok(indices)
}

fn json_payloads_from_materialized(
    row: &MaterializedTrackedStateRow,
) -> Vec<(crate::json_store::JsonRef, String)> {
    let mut payloads = Vec::new();
    if let Some(snapshot) = row.snapshot_content.as_deref() {
        payloads.push((prepare_json_ref(snapshot), snapshot.to_string()));
    }
    if let Some(metadata) = row.metadata.as_ref() {
        let serialized = crate::serialize_row_metadata(metadata);
        payloads.push((prepare_json_ref(&serialized), serialized));
    }
    payloads
}

fn stage_json_payloads(
    writes: &mut StorageWriteSet,
    payloads: &[(crate::json_store::JsonRef, String)],
) -> Result<(), crate::LixError> {
    let payloads = payloads
        .iter()
        .map(|(json_ref, payload)| NormalizedJsonRef::trusted_prehashed(payload, *json_ref))
        .collect::<Vec<_>>();
    JsonStoreContext::new().writer().stage_batch(
        writes,
        JsonWritePlacementRef::OutOfBand,
        payloads,
    )?;
    Ok(())
}

fn commit_change_ref_from_change(change: &ChangeRecord) -> CommitChangeRef {
    CommitChangeRef {
        schema_key: change.schema_key.clone(),
        file_id: change.file_id.clone(),
        entity_pk: change.entity_pk.clone(),
        change_id: change.change_id.clone(),
    }
}

pub(crate) fn tracked_change_from_materialized(
    row: &MaterializedTrackedStateRow,
) -> Result<ChangeRecord, crate::LixError> {
    Ok(ChangeRecord {
        format_version: 1,
        change_id: row.change_id.clone(),
        entity_pk: row.entity_pk.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot_ref: row.snapshot_content.as_deref().map(prepare_json_ref),
        metadata_ref: row.metadata.as_ref().map(|value| {
            let serialized = crate::serialize_row_metadata(value);
            prepare_json_ref(&serialized)
        }),
        created_at: crate::common::LixTimestamp::expect_parse("created_at", &row.updated_at),
    })
}

pub(crate) fn untracked_state_row_from_materialized(
    _writes: &mut StorageWriteSet,
    row: &MaterializedUntrackedStateRow,
) -> Result<UntrackedStateRow, crate::LixError> {
    Ok(UntrackedStateRow {
        entity_pk: row.entity_pk.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot_content: row.snapshot_content.clone(),
        metadata: row.metadata.as_ref().map(crate::serialize_row_metadata),
        created_at: crate::common::LixTimestamp::expect_parse("created_at", &row.created_at),
        updated_at: crate::common::LixTimestamp::expect_parse("updated_at", &row.updated_at),
        global: row.global,
        branch_id: row.branch_id.clone(),
    })
}
