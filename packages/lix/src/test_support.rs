#[cfg(test)]
use crate::changelog::ChangeId;
use crate::changelog::CommitId;
use crate::changelog::{
    ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogReader, ChangelogWriter,
    CommitLoadRequest, CommitRecord,
};
use crate::json_store::{JsonRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
#[cfg(test)]
use crate::storage_adapter::StorageAdapter;
use crate::storage_adapter::StorageAdapterRead;
use crate::storage_adapter::StorageWriteSet;
use crate::tracked_state::{
    CommitStateManifest, CommitStateMutationInventory, CommitStateReplayDebt,
    MaterializedTrackedStateRow, TrackedStateCommitDeltaRef, TrackedStateCommitRoot,
    TrackedStateContext, TrackedStateDeltaRef, TrackedStateKey,
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
use crate::hot_state::{CurrentStateDeltaRef, TrackedHeadContext, WorkingDiffIndexCoverage};

#[cfg(test)]
pub(crate) const TEST_EMPTY_ROOT_COMMIT_ID: &str = "01920000-0000-7000-8000-000000000001";
const TEST_TIMESTAMP: &str = "1970-01-01T00:00:00.000Z";

fn test_timestamp() -> crate::common::LixTimestamp {
    crate::common::LixTimestamp::expect_parse("timestamp", TEST_TIMESTAMP)
}

fn test_commit_id(value: &str) -> CommitId {
    CommitId::for_test_label(value)
}

#[cfg(test)]
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
                    typed_snapshot: None,
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
    let mut working_diff_coverage = WorkingDiffIndexCoverage::default();
    let generation = TrackedHeadContext::new()
        .writer(&read, &mut writes)
        .stage_current_state_with_working_diff(
            branch_id,
            None,
            commit_id,
            &deltas,
            &BTreeSet::new(),
            None,
            None,
            None,
            &mut working_diff_coverage,
        )
        .await
        .expect("current-state seed should stage");
    stage_branch_head_control(
        &mut writes,
        branch_id,
        BranchHeadControl {
            head_commit_id: commit_id,
            tracked_generation: generation,
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
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
    tracked_state: &TrackedStateContext,
    commit_id: &str,
    parent_commit_id: Option<&str>,
    rows: &[MaterializedTrackedStateRow],
) -> Result<(), crate::LixError> {
    stage_tracked_root_from_materialized_with_certified_replacement_markers(
        read,
        writes,
        tracked_state,
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
    tracked_state: &TrackedStateContext,
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
    let staged =
        stage_test_changelog_commit(read, writes, &commit_id_text, &parent_ids, rows, false)
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
    // Production stages the packed replay index for every tracked commit,
    // including commits that also receive a durable root. Keep rooted test
    // fixtures faithful to that invariant so deleting a root exercises the
    // same rootless recovery lane as a real repository.
    let commit_deltas = staged
        .change_commit_ids
        .iter()
        .zip(root_deltas.iter().copied())
        .map(|((row_index, _), delta)| {
            let change = &changes[*row_index];
            TrackedStateCommitDeltaRef {
                typed_snapshot: None,
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
    let mut root_writer = tracked_state.writer(read, writes);
    root_writer
        .stage_commit_root_with_absence_guards(
            &commit_id_text,
            parent_commit_id_text.as_deref(),
            root_deltas,
            &BTreeSet::new(),
            certified_replacement_markers,
        )
        .await?;
    let snapshot_root = root_writer
        .staged_commit_roots()
        .find(|root| root.commit_id == commit_id)
        .cloned()
        .ok_or_else(|| crate::LixError::unknown("test rooted commit did not stage a root"))?;
    drop(root_writer);
    let mutations = inventories.remove(&commit_id).unwrap_or_default();
    reject_unconsumed_test_inventories(&inventories, commit_id)?;
    stage_test_commit_state_manifest(writes, &staged, mutations, Some(snapshot_root))?;
    Ok(())
}

#[cfg(test)]
pub(crate) async fn stage_rootless_tracked_commit_from_materialized(
    read: &mut (impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: &str,
    parent_commit_id: Option<&str>,
    rows: &[MaterializedTrackedStateRow],
) -> Result<(), crate::LixError> {
    let commit_id = test_commit_id(commit_id);
    let commit_id_text = commit_id.to_string();
    let parent_id_texts = parent_commit_id
        .map(|parent| vec![test_commit_id(parent).to_string()])
        .unwrap_or_default();
    let changes = rows
        .iter()
        .map(tracked_change_from_materialized)
        .collect::<Result<Vec<_>, _>>()?;
    let staged =
        stage_test_changelog_commit(read, writes, &commit_id_text, &parent_id_texts, rows, true)
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
                typed_snapshot: None,
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
    let mutations = inventories.remove(&commit_id).unwrap_or_default();
    reject_unconsumed_test_inventories(&inventories, commit_id)?;
    stage_test_commit_state_manifest(writes, &staged, mutations, None)
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
    let staged =
        stage_test_changelog_commit(read, writes, &commit_id_text, &parent_id_texts, rows, false)
            .await?;
    let root_deltas = staged
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
    let commit_deltas = staged
        .change_commit_ids
        .iter()
        .zip(root_deltas.iter().copied())
        .map(|((row_index, _), delta)| {
            let change = &changes[*row_index];
            TrackedStateCommitDeltaRef {
                typed_snapshot: None,
                delta,
                snapshot: change.snapshot.as_ref_slot(),
                metadata: change.metadata.as_ref_slot(),
                origin_key: change.origin_key.as_deref(),
                base_coordinate: None,
                authored: true,
            }
        })
        .collect::<Vec<_>>();
    let typed_commit_id = test_commit_id(&commit_id_text);
    let mut inventories = stage_test_commit_deltas_by_owner(writes, &commit_deltas)?;
    let mutations = inventories.remove(&typed_commit_id).unwrap_or_default();
    for (owner_commit_id, owner_mutations) in inventories {
        let mut owner_manifest = crate::tracked_state::load_commit_state_manifest(
            &*read,
            owner_commit_id,
        )
        .await?
        .ok_or_else(|| {
            crate::LixError::unknown(format!(
                "test fixture staged mutations for owner commit '{owner_commit_id}' without an existing commit-state authority"
            ))
        })?;
        owner_manifest.mutations = owner_mutations;
        owner_manifest.current_state_scoped_ranges = None;
        crate::tracked_state::stage_commit_state_manifest(writes, &owner_manifest)?;
    }
    let mut root_writer = tracked_state.writer(read, writes);
    root_writer
        .stage_commit_root(
            &commit_id_text,
            commit_root_parent_commit_id_text.as_deref(),
            root_deltas,
        )
        .await?;
    let snapshot_root = root_writer
        .staged_commit_roots()
        .find(|root| root.commit_id == typed_commit_id)
        .cloned()
        .ok_or_else(|| crate::LixError::unknown("test rooted commit did not stage a root"))?;
    drop(root_writer);
    stage_test_commit_state_manifest(writes, &staged, mutations, Some(snapshot_root))?;
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

fn stage_test_commit_state_manifest(
    writes: &mut StorageWriteSet,
    staged: &TestStagedChangelogCommit,
    mutations: CommitStateMutationInventory,
    snapshot_root: Option<TrackedStateCommitRoot>,
) -> Result<(), crate::LixError> {
    let replay_debt = if snapshot_root.is_some() {
        CommitStateReplayDebt::default()
    } else {
        staged.replay_debt
    };
    let manifest = CommitStateManifest {
        commit_id: staged.record.commit_id,
        change_account_id: staged.record.account_id.clone(),
        replay_debt,
        mutations,
        touched_scope_filter: Default::default(),
        current_state_scoped_ranges: None,
        snapshot_root: snapshot_root.map(Box::new),
    };
    crate::tracked_state::stage_commit_state_manifest(writes, &manifest)
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
    let staged =
        stage_test_changelog_commit(read, writes, &commit_id_text, &parent_ids, &[], false).await?;
    let tracked_state = TrackedStateContext::new();
    let mut root_writer = tracked_state.writer(&*read, writes);
    root_writer
        .stage_commit_root(&commit_id_text, parent_commit_id_text.as_deref(), [])
        .await?;
    let snapshot_root = root_writer
        .staged_commit_roots()
        .find(|root| root.commit_id == staged.record.commit_id)
        .cloned()
        .ok_or_else(|| crate::LixError::unknown("empty test commit did not stage a root"))?;
    drop(root_writer);
    stage_test_commit_state_manifest(
        writes,
        &staged,
        CommitStateMutationInventory::default(),
        Some(snapshot_root),
    )
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
    let staged =
        stage_test_changelog_commit(read, writes, &commit_id_text, &parent_id_texts, &[], false)
            .await?;
    let first_parent = parent_id_texts.first().map(String::as_str);
    let tracked_state = TrackedStateContext::new();
    let mut root_writer = tracked_state.writer(&*read, writes);
    root_writer
        .stage_commit_root(&commit_id_text, first_parent, [])
        .await?;
    let snapshot_root = root_writer
        .staged_commit_roots()
        .find(|root| root.commit_id == staged.record.commit_id)
        .cloned()
        .ok_or_else(|| crate::LixError::unknown("empty test commit did not stage a root"))?;
    drop(root_writer);
    stage_test_commit_state_manifest(
        writes,
        &staged,
        CommitStateMutationInventory::default(),
        Some(snapshot_root),
    )
}

async fn stage_test_changelog_commit(
    mut read: &mut (impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: &str,
    parent_ids: &[String],
    rows: &[MaterializedTrackedStateRow],
    tracked_state_rootless: bool,
) -> Result<TestStagedChangelogCommit, crate::LixError> {
    let typed_commit_id = test_commit_id(commit_id);
    let typed_parent_ids = parent_ids
        .iter()
        .map(|parent| test_commit_id(parent))
        .collect::<Vec<_>>();
    let parent_records = ChangelogContext::new()
        .reader(&mut *read)
        .load_commits(CommitLoadRequest {
            commit_ids: &typed_parent_ids,
        })
        .await?;
    let parent_manifests =
        crate::tracked_state::load_commit_state_manifests(&*read, &typed_parent_ids).await?;
    let first_parent_debt = parent_manifests
        .first()
        .and_then(Option::as_ref)
        .map_or(CommitStateReplayDebt::default(), |manifest| {
            manifest.replay_debt
        });
    let tracked_state_rootless_depth = if tracked_state_rootless {
        first_parent_debt.depth.saturating_add(1)
    } else {
        0
    };
    let tracked_state_rootless_rows = if tracked_state_rootless {
        first_parent_debt
            .rows
            .saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX))
    } else {
        0
    };
    let tracked_state_rootless_bytes = tracked_state_rootless_rows;
    let generation = parent_records
        .iter()
        .map(|(_, record)| {
            record
                .map(|record| record.generation)
                .ok_or_else(|| crate::LixError::unknown("test changelog parent commit is missing"))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .map_or(Ok(0), |generation| {
            generation
                .checked_add(1)
                .ok_or_else(|| crate::LixError::unknown("test commit generation exceeds u64"))
        })?;
    let parent_record = match typed_parent_ids.as_slice() {
        [_] => parent_records
            .iter()
            .next()
            .and_then(|(_, record)| record)
            .cloned(),
        _ => None,
    };
    let parent_jump_record = if let Some(parent) = &parent_record {
        ChangelogContext::new()
            .reader(&mut *read)
            .load_commits(CommitLoadRequest {
                commit_ids: std::slice::from_ref(&parent.first_parent_jump_commit_id),
            })
            .await?
            .into_iter()
            .next()
            .and_then(|(_, record)| record)
    } else {
        None
    };
    let (first_parent_jump_commit_id, first_parent_jump_span) =
        crate::changelog::next_first_parent_jump(
            typed_commit_id,
            &typed_parent_ids,
            parent_record.as_ref(),
            parent_jump_record.as_ref(),
        )?;
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
        touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
        format_version: 4,
        commit_id: typed_commit_id,
        generation,
        parent_commit_ids: typed_parent_ids,
        first_parent_jump_commit_id,
        first_parent_jump_span,
        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
        created_at,
    };
    append.commits.push(record.clone());
    let mut writer = ChangelogContext::new().writer(&mut read, writes);
    writer.stage_append(append).await?;
    change_commit_ids.sort_by_key(|(row_index, _)| *row_index);
    Ok(TestStagedChangelogCommit {
        record,
        replay_debt: CommitStateReplayDebt {
            depth: tracked_state_rootless_depth,
            rows: tracked_state_rootless_rows,
            bytes: tracked_state_rootless_bytes,
        },
        change_commit_ids,
    })
}

struct TestStagedChangelogCommit {
    record: CommitRecord,
    replay_debt: CommitStateReplayDebt,
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
        typed_snapshot: None,
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
