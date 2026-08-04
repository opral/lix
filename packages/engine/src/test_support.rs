use crate::changelog::CommitId;
use crate::changelog::{
    ChangeId, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogReader, ChangelogWriter,
    CommitLoadRequest, CommitRecord,
};
use crate::json_store::{JsonRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
#[cfg(test)]
use crate::storage_adapter::StorageAdapter;
use crate::storage_adapter::StorageAdapterRead;
use crate::storage_adapter::StorageWriteSet;
use crate::tracked_state::{
    CommitStateManifest, CommitStateMutationInventory, MaterializedTrackedStateRow,
    TrackedStateCommitDeltaRef, TrackedStateContext, TrackedStateDeltaRef, TrackedStateKey,
};
use std::collections::{BTreeMap, BTreeSet};

fn prepare_json_ref(value: &str) -> JsonRef {
    JsonRef::for_content(value.as_bytes())
}
#[cfg(test)]
use crate::GLOBAL_BRANCH_ID;
#[cfg(test)]
use crate::branch::{BranchHeadControl, stage_branch_head_control};
#[cfg(test)]
use crate::live_state::{CurrentStateDeltaRef, TrackedHeadContext};

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
    let branch_ref_entity_pk = crate::entity_pk::EntityPk::uuid_from_canonical(branch_id)
        .expect("test branch ID must be a canonical UUID");
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
                    account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
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
    let snapshots = rows
        .iter()
        .map(|row| {
            row.snapshot_content.as_deref().map_or(
                crate::json_store::JsonSlot::None,
                crate::json_store::JsonSlot::from_json,
            )
        })
        .collect::<Vec<_>>();
    let metadata = rows
        .iter()
        .map(|row| {
            row.metadata
                .as_ref()
                .map_or(crate::json_store::JsonSlot::None, |value| {
                    let serialized = crate::serialize_row_metadata(value);
                    crate::json_store::JsonSlot::from_json(&serialized)
                })
        })
        .collect::<Vec<_>>();
    let deltas = rows
        .iter()
        .zip(snapshots.iter())
        .zip(metadata.iter())
        .map(|((row, snapshot), metadata)| CurrentStateDeltaRef {
            schema_key: &row.schema_key,
            file_id: row.file_id.as_deref(),
            entity_pk: &row.entity_pk,
            change_id: Some(row.change_id),
            commit_id: Some(row.commit_id),
            untracked: false,
            deleted: row.deleted,
            created_at: crate::common::LixTimestamp::expect_parse("created_at", &row.created_at),
            updated_at: crate::common::LixTimestamp::expect_parse("updated_at", &row.updated_at),
            snapshot: snapshot.as_ref_slot(),
            metadata: metadata.as_ref_slot(),
            columnar_base_coordinate: None,
        })
        .collect::<Vec<_>>();
    let generation = TrackedHeadContext::new()
        .writer(&read, &mut writes)
        .stage_current_state(
            branch_id,
            None,
            commit_id,
            &deltas,
            &BTreeSet::new(),
            None,
            None,
        )
        .await
        .expect("current-state seed should stage");
    stage_branch_head_control(
        &mut writes,
        branch_id,
        BranchHeadControl {
            head_commit_id: commit_id,
            generation,
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
            untracked_schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id: None,
            created_at: test_timestamp(),
            updated_at: test_timestamp(),
            ref_change_id: branch_ref_change_id,
        },
    )
    .expect("direct branch-head control should stage");
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
    _tracked_state: &TrackedStateContext,
    commit_id: &str,
    parent_commit_id: Option<&str>,
    rows: &[MaterializedTrackedStateRow],
) -> Result<(), crate::LixError> {
    stage_tracked_root_from_materialized_with_certified_replacement_markers(
        read,
        writes,
        _tracked_state,
        commit_id,
        parent_commit_id,
        rows,
        &BTreeSet::new(),
    )
    .await
}

pub(crate) async fn stage_tracked_root_from_materialized_with_certified_replacement_markers(
    read: &mut (impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    _tracked_state: &TrackedStateContext,
    commit_id: &str,
    parent_commit_id: Option<&str>,
    rows: &[MaterializedTrackedStateRow],
    certified_replacement_markers: &BTreeSet<TrackedStateKey>,
) -> Result<(), crate::LixError> {
    let commit_id = test_commit_id(commit_id);
    let commit_id_text = commit_id.to_string();
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
    )
    .await?;
    let root_deltas = staged
        .change_commit_ids
        .iter()
        .map(|(row_index, _)| {
            let change = &changes[*row_index];
            let row = &rows[*row_index];
            TrackedStateDeltaRef {
                schema_key: &change.schema_key,
                file_id: change.file_id.as_deref(),
                entity_pk: &change.entity_pk,
                change_id: change.change_id,
                commit_id,
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
    let commit_deltas = staged
        .change_commit_ids
        .iter()
        .zip(root_deltas.iter().copied())
        .map(|((row_index, _), delta)| {
            let change = &changes[*row_index];
            TrackedStateCommitDeltaRef {
                delta,
                snapshot: change.snapshot.as_ref_slot(),
                metadata: change.metadata.as_ref_slot(),
                origin_key: change.origin_key.as_deref(),
                base_coordinate: None,
                authored: true,
            }
        })
        .collect::<Vec<_>>();
    let mut inventories = stage_test_commit_deltas_by_owner(writes, &commit_deltas)?;
    let _ = certified_replacement_markers;
    let mutations = inventories.remove(&commit_id).unwrap_or_default();
    reject_unconsumed_test_inventories(&inventories, commit_id)?;
    stage_test_commit_state_manifest(read, writes, &staged.record, mutations, rows).await?;
    Ok(())
}

fn stage_test_commit_deltas_by_owner(
    writes: &mut StorageWriteSet,
    deltas: &[TrackedStateCommitDeltaRef<'_>],
) -> Result<BTreeMap<CommitId, CommitStateMutationInventory>, crate::LixError> {
    let mut by_owner = BTreeMap::<CommitId, Vec<TrackedStateCommitDeltaRef<'_>>>::new();
    for delta in deltas {
        let owner = delta.delta.commit_id;
        by_owner.entry(owner).or_default().push(*delta);
    }
    let mut canonical_locators = BTreeMap::new();
    let mut inventories = BTreeMap::new();
    for (owner, owner_deltas) in &by_owner {
        let staged =
            crate::tracked_state::stage_commit_deltas_for_commit_state(writes, owner_deltas)?;
        inventories.insert(*owner, staged.mutation_inventory().clone());
        for locator in staged.locators {
            canonical_locators
                .entry(locator.change_id)
                .or_insert(locator);
        }
    }
    crate::tracked_state::stage_change_locators(
        writes,
        &canonical_locators.into_values().collect::<Vec<_>>(),
    );
    Ok(inventories)
}

fn reject_unconsumed_test_inventories(
    inventories: &BTreeMap<CommitId, CommitStateMutationInventory>,
    expected_owner: CommitId,
) -> Result<(), crate::LixError> {
    if inventories.is_empty() {
        return Ok(());
    }
    Err(crate::LixError::unknown(format!(
        "test fixture for commit '{expected_owner}' staged mutation inventories for unexpected owners: {:?}",
        inventories.keys().collect::<Vec<_>>()
    )))
}

pub(crate) async fn stage_test_commit_state_manifest(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    record: &CommitRecord,
    mut mutations: CommitStateMutationInventory,
    rows: &[MaterializedTrackedStateRow],
) -> Result<(), crate::LixError> {
    let state_parent_commit_id = record.parent_commit_ids.first().copied();
    let staged_parent = state_parent_commit_id
        .map(|parent_id| {
            crate::tracked_state::staged_commit_state_manifest_for_test(writes, parent_id)
        })
        .transpose()?
        .flatten();
    let loaded_parent = if staged_parent.is_none() {
        match state_parent_commit_id {
            Some(parent_id) => {
                crate::tracked_state::load_commit_state_manifest(read, parent_id).await?
            }
            None => None,
        }
    } else {
        None
    };
    let parent = staged_parent.as_ref().or(loaded_parent.as_ref());
    let mut planned_members = crate::tracked_state::staged_commit_delta_members_for_write(
        read,
        writes,
        record.commit_id,
        &mutations,
    )
    .await?;
    let mut by_scope = BTreeMap::<
        crate::tracked_state::CommitDeltaReplacementScope,
        Vec<&MaterializedTrackedStateRow>,
    >::new();
    for row in rows {
        by_scope
            .entry(crate::tracked_state::CommitDeltaReplacementScope {
                schema_key: row.schema_key.clone(),
                file_id: row.file_id.clone(),
            })
            .or_default()
            .push(row);
    }
    let mut arrow_mutations = crate::live_state::EntityColumnarWriteSets::new();
    for (scope, scoped_rows) in by_scope {
        let mut owned = scoped_rows
            .into_iter()
            .map(|row| {
                let encoded_key = crate::tracked_state::encode_key_ref(
                    crate::tracked_state::TrackedStateKeyRef {
                        schema_key: &row.schema_key,
                        file_id: row.file_id.as_deref(),
                        entity_pk: &row.entity_pk,
                    },
                );
                let snapshot = if row.deleted {
                    crate::json_store::JsonSlot::None
                } else {
                    row.snapshot_content
                        .as_deref()
                        .map(crate::json_store::JsonSlot::from_json)
                        .unwrap_or(crate::json_store::JsonSlot::None)
                };
                let metadata = row
                    .metadata
                    .as_deref()
                    .map(crate::json_store::JsonSlot::from_json)
                    .unwrap_or(crate::json_store::JsonSlot::None);
                (encoded_key, snapshot, metadata, row)
            })
            .collect::<Vec<_>>();
        owned.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let input_rows = owned
            .iter()
            .map(|(encoded_key, snapshot, metadata, row)| {
                crate::tracked_state::ArrowStateInputRowRef {
                    encoded_key,
                    value: crate::tracked_state::TrackedStateIndexValueRef {
                        change_id: row.change_id,
                        commit_id: row.commit_id,
                        deleted: row.deleted,
                        created_at: crate::common::LixTimestamp::expect_parse(
                            "created_at",
                            &row.created_at,
                        ),
                        updated_at: crate::common::LixTimestamp::expect_parse(
                            "updated_at",
                            &row.updated_at,
                        ),
                    },
                    snapshot: snapshot.as_ref_slot(),
                    metadata: metadata.as_ref_slot(),
                }
            })
            .collect::<Vec<_>>();
        let (row_group_set, _) =
            crate::tracked_state::encode_authoritative_arrow_state_rows(&scope, &input_rows)?;
        arrow_mutations.insert_scope(record.commit_id, scope, row_group_set);
    }
    let publication = crate::tracked_state::stage_current_state_catalog_from_test_parent(
        read,
        writes,
        parent,
        record.commit_id,
        &mutations,
        &planned_members,
        Some(&arrow_mutations),
    )
    .await?;
    for member in planned_members.iter_mut().filter(|member| member.authored) {
        let encoded_key = crate::tracked_state::encode_key(&member.key);
        member.base_coordinate = publication.coordinates().get(&encoded_key).copied();
        if !member.value.deleted && member.base_coordinate.is_none() {
            return Err(crate::LixError::unknown(
                "test authored event has no canonical Arrow coordinate",
            ));
        }
    }
    mutations = crate::tracked_state::finalize_commit_delta_event_coordinates(
        writes,
        record.commit_id,
        &mutations,
        &planned_members,
    )?;
    let manifest = CommitStateManifest {
        commit_id: record.commit_id,
        generation: record.generation,
        parent_commit_ids: record.parent_commit_ids.clone(),
        state_parent_commit_id,
        commit_change_id: record.change_id,
        account_id: record.account_id.clone(),
        created_at: record.created_at,
        mutations,
        current_state_catalog: publication.root(),
    };
    crate::tracked_state::stage_certified_commit_state_manifest_with_handle(
        writes,
        &manifest,
        &publication,
    )?;
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
    let staged = stage_test_changelog_commit(
        read,
        writes,
        &commit_id_text,
        &commit_change_id,
        &parent_ids,
        &[],
    )
    .await?;
    stage_test_commit_state_manifest(
        read,
        writes,
        &staged.record,
        CommitStateMutationInventory::default(),
        &[],
    )
    .await
}

async fn stage_test_changelog_commit(
    mut read: &mut (impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: &str,
    commit_change_id: &str,
    parent_ids: &[String],
    rows: &[MaterializedTrackedStateRow],
) -> Result<TestStagedChangelogCommit, crate::LixError> {
    let typed_commit_id = test_commit_id(commit_id);
    let typed_parent_ids = parent_ids
        .iter()
        .map(|parent| test_commit_id(parent))
        .collect::<Vec<_>>();
    let typed_commit_change_id = test_change_id(commit_change_id);
    let mut parent_generations = Vec::with_capacity(typed_parent_ids.len());
    for parent_id in &typed_parent_ids {
        if let Some(parent) =
            crate::tracked_state::staged_commit_state_manifest_for_test(writes, *parent_id)?
        {
            parent_generations.push(parent.generation);
            continue;
        }
        let generation = ChangelogContext::new()
            .reader(&mut *read)
            .load_commits(CommitLoadRequest {
                commit_ids: std::slice::from_ref(parent_id),
            })
            .await?
            .into_iter()
            .next()
            .and_then(|(_, record)| record)
            .map(|record| record.generation)
            .ok_or_else(|| crate::LixError::unknown("test changelog parent commit is missing"))?;
        parent_generations.push(generation);
    }
    let generation = parent_generations
        .into_iter()
        .max()
        .map_or(Ok(0), |generation| {
            generation
                .checked_add(1)
                .ok_or_else(|| crate::LixError::unknown("test commit generation exceeds u64"))
        })?;
    let winner_indices = final_state_row_winner_indices(rows)?;
    let mut append = ChangelogAppend::default();
    let mut change_commit_ids = Vec::new();
    let mut json_payloads = Vec::new();
    let mut seen_json_refs = BTreeSet::new();
    for &row_index in &winner_indices {
        let row = &rows[row_index];
        for (json_ref, payload) in json_payloads_from_materialized(row) {
            if seen_json_refs.insert(json_ref.as_hash_bytes().to_vec()) {
                json_payloads.push((json_ref, payload));
            }
        }
        change_commit_ids.push((row_index, row.commit_id));
    }
    stage_json_payloads(writes, &json_payloads)?;
    let created_at = rows
        .first()
        .map(|row| crate::common::LixTimestamp::expect_parse("created_at", &row.created_at))
        .unwrap_or_else(test_timestamp);
    let record = CommitRecord {
        format_version: 1,
        commit_id: typed_commit_id,
        generation,
        parent_commit_ids: typed_parent_ids,
        change_id: typed_commit_change_id,
        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
        created_at,
    };
    append.commits.push(record.clone());
    let mut writer = ChangelogContext::new().writer(&mut read, writes);
    writer.stage_append(append).await?;
    change_commit_ids.sort_by_key(|(row_index, _)| *row_index);
    Ok(TestStagedChangelogCommit {
        record,
        change_commit_ids,
    })
}

struct TestStagedChangelogCommit {
    record: CommitRecord,
    change_commit_ids: Vec<(usize, CommitId)>,
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

#[expect(clippy::unnecessary_wraps)]
pub(crate) fn tracked_change_from_materialized(
    row: &MaterializedTrackedStateRow,
) -> Result<ChangeRecord, crate::LixError> {
    Ok(ChangeRecord {
        format_version: 1,
        change_id: row.change_id,
        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
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
