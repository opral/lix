use crate::changelog::CommitId;
use crate::changelog::{
    ChangeId, ChangeLoadRequest, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogReader,
    ChangelogWriter, CommitChangeRefSet, CommitRecord,
};
use crate::json_store::{JsonRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
#[cfg(test)]
use crate::storage_adapter::StorageAdapter;
use crate::storage_adapter::StorageAdapterRead;
use crate::storage_adapter::StorageWriteSet;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateContext, TrackedStateDeltaRef,
};
use std::collections::BTreeMap;

fn prepare_json_ref(value: &str) -> JsonRef {
    JsonRef::for_content(value.as_bytes())
}
#[cfg(test)]
use crate::GLOBAL_BRANCH_ID;

#[cfg(test)]
pub(crate) const TEST_EMPTY_ROOT_COMMIT_ID: &str = "01920000-0000-7000-8000-000000000001";
const TEST_TIMESTAMP: &str = "1970-01-01T00:00:00.000Z";

fn test_timestamp() -> crate::common::LixTimestamp {
    crate::common::LixTimestamp::expect_parse("timestamp", TEST_TIMESTAMP)
}

fn test_commit_id(value: &str) -> CommitId {
    CommitId::for_test_label(value)
}

fn test_change_id(value: &str) -> ChangeId {
    ChangeId::for_test_label(value)
}

/// Seeds a branch head and matching tracked root for unit tests.
///
/// A branch ref that points at a commit without a tracked root is invalid for
/// the serving state. This helper keeps that invariant in one place while
/// still letting low-level tests use synthetic commit ids.
#[cfg(test)]
pub(crate) async fn seed_branch_head(storage: StorageAdapter, branch_id: &str, commit_id: &str) {
    seed_branch_head_with_rows(storage, branch_id, commit_id, &[]).await;
}

/// Seeds the global branch head to an empty tracked root for unit tests.
#[cfg(test)]
pub(crate) async fn seed_global_branch_head(storage: StorageAdapter) {
    seed_branch_head(storage, GLOBAL_BRANCH_ID, TEST_EMPTY_ROOT_COMMIT_ID).await;
}

/// Seeds a branch head and writes the tracked root contents for its commit.
#[cfg(test)]
pub(crate) async fn seed_branch_head_with_rows(
    storage: StorageAdapter,
    branch_id: &str,
    commit_id: &str,
    rows: &[MaterializedTrackedStateRow],
) {
    let commit_id = test_commit_id(commit_id);
    let commit_id_text = commit_id.to_string();
    let mut read = storage
        .begin_read(crate::storage_adapter::StorageReadOptions::default())
        .await
        .expect("seed read should open");
    let mut writes = StorageWriteSet::new();
    stage_tracked_root_from_materialized(
        &mut read,
        &mut writes,
        &TrackedStateContext::new(),
        &commit_id_text,
        None,
        rows,
    )
    .await
    .expect("tracked root should write");

    let branch_ref_change_id = test_change_id(&format!("branch-ref-{branch_id}"));
    let branch_ref_entity_pk = crate::entity_pk::EntityPk::single(branch_id);
    let branch_ref_snapshot = serde_json::json!({
        "id": branch_id,
        "commit_id": commit_id,
    })
    .to_string();
    {
        let mut changelog_read = &mut read;
        ChangelogContext::new()
            .writer(&mut changelog_read, &mut writes)
            .stage_append(ChangelogAppend {
                changes: vec![ChangeRecord {
                    format_version: 2,
                    change_id: branch_ref_change_id,
                    entity_pk: branch_ref_entity_pk.clone(),
                    schema_key: crate::branch::BRANCH_REF_SCHEMA_KEY.to_string(),
                    file_id: None,
                    snapshot: crate::json_store::JsonSlot::from_json(&branch_ref_snapshot),
                    metadata: crate::json_store::JsonSlot::None,
                    created_at: test_timestamp(),
                    origin_key: None,
                }],
                ..ChangelogAppend::default()
            })
            .await
            .expect("branch ref change should stage");
    }
    crate::live_state::LiveStateIndexContext::new()
        .writer(&read, &mut writes)
        .stage_branch_rows(
            GLOBAL_BRANCH_ID,
            [crate::live_state::LiveStateIndexDeltaRef {
                schema_key: crate::branch::BRANCH_REF_SCHEMA_KEY,
                file_id: None,
                entity_pk: &branch_ref_entity_pk,
                change_id: branch_ref_change_id,
                commit_id: None,
                deleted: false,
                created_at: test_timestamp(),
                updated_at: test_timestamp(),
            }]
            .into_iter()
            .chain(
                rows.iter()
                    .filter(|_| branch_id == GLOBAL_BRANCH_ID)
                    .map(|row| crate::live_state::LiveStateIndexDeltaRef {
                        schema_key: &row.schema_key,
                        file_id: row.file_id.as_deref(),
                        entity_pk: &row.entity_pk,
                        change_id: row.change_id,
                        commit_id: Some(row.commit_id),
                        deleted: row.deleted,
                        created_at: crate::common::LixTimestamp::expect_parse(
                            "created_at",
                            &row.created_at,
                        ),
                        updated_at: crate::common::LixTimestamp::expect_parse(
                            "updated_at",
                            &row.updated_at,
                        ),
                    }),
            ),
        )
        .await
        .expect("branch ref current row should stage");
    if !rows.is_empty() && branch_id != GLOBAL_BRANCH_ID {
        crate::live_state::LiveStateIndexContext::new()
            .writer(&read, &mut writes)
            .stage_branch_rows(
                branch_id,
                rows.iter()
                    .map(|row| crate::live_state::LiveStateIndexDeltaRef {
                        schema_key: &row.schema_key,
                        file_id: row.file_id.as_deref(),
                        entity_pk: &row.entity_pk,
                        change_id: row.change_id,
                        commit_id: Some(row.commit_id),
                        deleted: row.deleted,
                        created_at: crate::common::LixTimestamp::expect_parse(
                            "created_at",
                            &row.created_at,
                        ),
                        updated_at: crate::common::LixTimestamp::expect_parse(
                            "updated_at",
                            &row.updated_at,
                        ),
                    }),
            )
            .await
            .expect("branch current rows should stage");
    }
    storage
        .commit_write_set(
            writes,
            crate::storage_adapter::StorageWriteOptions::default(),
        )
        .await
        .expect("seed should commit");
}

pub(crate) async fn stage_tracked_root_from_materialized(
    read: &mut (impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    tracked_state: &TrackedStateContext,
    commit_id: &str,
    parent_commit_id: Option<&str>,
    rows: &[MaterializedTrackedStateRow],
) -> Result<(), crate::LixError> {
    let commit_id_text = test_commit_id(commit_id).to_string();
    let parent_commit_id_text = parent_commit_id.map(|parent| test_commit_id(parent).to_string());
    let changes = rows
        .iter()
        .map(tracked_change_from_materialized)
        .collect::<Result<Vec<_>, _>>()?;
    let parent_ids = parent_commit_id_text
        .as_ref()
        .map(|parent| vec![parent.clone()])
        .unwrap_or_default();
    let commit_change_id = format!("{commit_id_text}:commit");
    let staged = stage_test_changelog_commit(
        read,
        writes,
        &commit_id_text,
        &commit_change_id,
        &parent_ids,
        rows,
        &changes,
    )
    .await?;
    let deltas = staged
        .change_commit_ids
        .iter()
        .map(|(row_index, change_commit_id)| {
            let change = &changes[*row_index];
            let row = &rows[*row_index];
            TrackedStateDeltaRef {
                schema_key: &change.schema_key,
                file_id: change.file_id.as_deref(),
                entity_pk: &change.entity_pk,
                change_id: change.change_id,
                commit_id: *change_commit_id,
                deleted: change.snapshot.is_none(),
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
    tracked_state
        .writer(read, writes)
        .stage_commit_root(&commit_id_text, parent_commit_id_text.as_deref(), deltas)
        .await?;
    Ok(())
}

#[cfg(test)]
pub(crate) async fn stage_tracked_root_from_materialized_with_parents(
    read: &mut (impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    tracked_state: &TrackedStateContext,
    commit_id: &str,
    parent_ids: &[String],
    commit_root_parent_commit_id: Option<&str>,
    rows: &[MaterializedTrackedStateRow],
) -> Result<(), crate::LixError> {
    let commit_id_text = test_commit_id(commit_id).to_string();
    let parent_id_texts = parent_ids
        .iter()
        .map(|parent| test_commit_id(parent).to_string())
        .collect::<Vec<_>>();
    let commit_root_parent_commit_id_text =
        commit_root_parent_commit_id.map(|parent| test_commit_id(parent).to_string());
    let changes = rows
        .iter()
        .map(tracked_change_from_materialized)
        .collect::<Result<Vec<_>, _>>()?;
    let commit_change_id = format!("{commit_id_text}:commit");
    let staged = stage_test_changelog_commit(
        read,
        writes,
        &commit_id_text,
        &commit_change_id,
        &parent_id_texts,
        rows,
        &changes,
    )
    .await?;
    let deltas = staged
        .change_commit_ids
        .iter()
        .map(|(row_index, change_commit_id)| {
            let change = &changes[*row_index];
            let row = &rows[*row_index];
            TrackedStateDeltaRef {
                schema_key: &change.schema_key,
                file_id: change.file_id.as_deref(),
                entity_pk: &change.entity_pk,
                change_id: change.change_id,
                commit_id: *change_commit_id,
                deleted: change.snapshot.is_none(),
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
    tracked_state
        .writer(read, writes)
        .stage_commit_root(
            &commit_id_text,
            commit_root_parent_commit_id_text.as_deref(),
            deltas,
        )
        .await?;
    Ok(())
}

#[cfg(test)]
pub(crate) async fn stage_empty_changelog_commit(
    read: &mut (impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: &str,
    parent_commit_id: Option<&str>,
) -> Result<(), crate::LixError> {
    let commit_id_text = test_commit_id(commit_id).to_string();
    let parent_commit_id_text = parent_commit_id.map(|parent| test_commit_id(parent).to_string());
    let parent_ids = parent_commit_id_text
        .as_ref()
        .map(|parent| vec![parent.clone()])
        .unwrap_or_default();
    let commit_change_id = format!("{commit_id_text}:commit");
    stage_test_changelog_commit(
        read,
        writes,
        &commit_id_text,
        &commit_change_id,
        &parent_ids,
        &[],
        &[],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
pub(crate) async fn stage_empty_changelog_commit_with_parents(
    read: &mut (impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: &str,
    parent_ids: &[String],
) -> Result<(), crate::LixError> {
    let commit_id_text = test_commit_id(commit_id).to_string();
    let parent_id_texts = parent_ids
        .iter()
        .map(|parent| test_commit_id(parent).to_string())
        .collect::<Vec<_>>();
    let commit_change_id = format!("{commit_id_text}:commit");
    stage_test_changelog_commit(
        read,
        writes,
        &commit_id_text,
        &commit_change_id,
        &parent_id_texts,
        &[],
        &[],
    )
    .await?;
    Ok(())
}

async fn stage_test_changelog_commit(
    mut read: &mut (impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: &str,
    commit_change_id: &str,
    parent_ids: &[String],
    rows: &[MaterializedTrackedStateRow],
    changes: &[ChangeRecord],
) -> Result<TestStagedChangelogCommit, crate::LixError> {
    let typed_commit_id = test_commit_id(commit_id);
    let typed_parent_ids = parent_ids
        .iter()
        .map(|parent| test_commit_id(parent))
        .collect::<Vec<_>>();
    let typed_commit_change_id = test_change_id(commit_change_id);
    let winner_indices = final_state_row_winner_indices(rows)?;
    let winner_change_ids = winner_indices
        .iter()
        .map(|&index| changes[index].change_id)
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
        change_commit_ids.push((row_index, row.commit_id));
    }
    stage_json_payloads(writes, &json_payloads)?;
    let created_at = rows
        .first()
        .map(|row| crate::common::LixTimestamp::expect_parse("created_at", &row.created_at))
        .unwrap_or_else(test_timestamp);
    append.commits.push(CommitRecord {
        format_version: 1,
        commit_id: typed_commit_id,
        parent_commit_ids: typed_parent_ids,
        change_id: typed_commit_change_id,
        author_account_ids: Vec::new(),
        created_at,
    });
    append.commit_change_refs.push(CommitChangeRefSet {
        commit_id: typed_commit_id,
        entries: refs,
    });
    let mut writer = ChangelogContext::new().writer(&mut read, writes);
    writer.stage_append(append).await?;
    change_commit_ids.sort_by_key(|(row_index, _)| *row_index);
    Ok(TestStagedChangelogCommit { change_commit_ids })
}

struct TestStagedChangelogCommit {
    change_commit_ids: Vec<(usize, CommitId)>,
}

async fn load_existing_changelog_change_ids(
    read: &mut (impl StorageAdapterRead + ?Sized),
    change_ids: &[ChangeId],
) -> Result<std::collections::BTreeSet<ChangeId>, crate::LixError> {
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

#[expect(clippy::unnecessary_wraps)]
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

fn json_payloads_from_materialized(row: &MaterializedTrackedStateRow) -> Vec<(JsonRef, String)> {
    // Mirror production staging: only payloads above the inline threshold
    // get json_store rows.
    let mut payloads = Vec::new();
    if let Some(snapshot) = row.snapshot_content.as_deref() {
        if snapshot.len() > crate::json_store::JSON_INLINE_MAX_BYTES {
            payloads.push((prepare_json_ref(snapshot), snapshot.to_string()));
        }
    }
    if let Some(metadata) = row.metadata.as_ref() {
        let serialized = crate::serialize_row_metadata(metadata);
        if serialized.len() > crate::json_store::JSON_INLINE_MAX_BYTES {
            payloads.push((prepare_json_ref(&serialized), serialized));
        }
    }
    payloads
}

fn stage_json_payloads(
    writes: &mut StorageWriteSet,
    payloads: &[(JsonRef, String)],
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

fn commit_change_ref_from_change(change: &ChangeRecord) -> ChangeId {
    change.change_id
}

#[expect(clippy::unnecessary_wraps)]
pub(crate) fn tracked_change_from_materialized(
    row: &MaterializedTrackedStateRow,
) -> Result<ChangeRecord, crate::LixError> {
    Ok(ChangeRecord {
        format_version: 1,
        change_id: row.change_id,
        entity_pk: row.entity_pk.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot: row
            .snapshot_content
            .as_deref()
            .map_or(crate::json_store::JsonSlot::None, |content| {
                crate::json_store::JsonSlot::from_json(content)
            }),
        metadata: row
            .metadata
            .as_ref()
            .map_or(crate::json_store::JsonSlot::None, |value| {
                let serialized = crate::serialize_row_metadata(value);
                crate::json_store::JsonSlot::from_json(&serialized)
            }),
        created_at: crate::common::LixTimestamp::expect_parse("created_at", &row.updated_at),
        origin_key: None,
    })
}
