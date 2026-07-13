#![allow(
    clippy::implicit_clone,
    clippy::unnecessary_mut_passed,
    clippy::unnecessary_wraps
)]

use crate::LixError;
use crate::binary_cas::BinaryCasContext;
use crate::branch::{BranchContext, BranchRefReader};
use crate::changelog::{
    ChangeId, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogWriter, CommitChangeRefSet,
    CommitId, CommitRecord,
};
use crate::common::LixTimestamp;
use crate::current_state::{
    CurrentStateContext, CurrentStateDeltaRef, CurrentStateFilter, CurrentStateRowRequest,
    CurrentStateScanRequest,
};
use crate::entity_pk::EntityPk;
use crate::functions::FunctionContext;
use crate::json_store::{JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
use crate::storage::{StorageRead, StorageWriteSet};
use crate::tracked_state::{TrackedStateContext, TrackedStateDeltaRef};
use crate::transaction::staging::PreparedWriteSet;
use crate::transaction::types::{PreparedStateRow, StagedCommitChangeRef, StagedCommitChangeRefs};
use std::collections::{BTreeMap, BTreeSet};

type RowIndex = usize;

/// Commits prepared transaction rows into the unified change ledger and the
/// canonical current-state roots.
///
/// Providers decode DataFusion DML into hydrated `PreparedStateRow`s. Every row
/// stages a canonical changelog fact. Tracked rows additionally become commit
/// members and update immutable history roots; untracked rows update only the
/// mutable current root.
pub(crate) async fn commit_prepared_writes(
    binary_cas: &BinaryCasContext,
    branch_ctx: &BranchContext,
    runtime_functions: Option<&FunctionContext>,
    read: &mut impl StorageRead,
    prepared_writes: PreparedWriteSet,
) -> Result<StorageWriteSet, LixError> {
    let mut writes = StorageWriteSet::new();
    let mut json_writer = JsonStoreContext::new().writer();

    if !prepared_writes.file_data_writes.is_empty() {
        let mut blob_writer = binary_cas.writer_skipping_existing_chunks(&*read, &mut writes);
        for write in &prepared_writes.file_data_writes {
            blob_writer.stage_payload(write.payload()).await?;
        }
    }

    let mut state_rows = prepared_writes.state_rows;
    let finalized = finalize_commit_rows(
        prepared_writes.commit_change_refs_by_branch,
        prepared_writes.extra_commit_parents_by_branch,
        branch_ctx,
        &*read,
    )
    .await?;
    let commit_rows = finalized.commit_rows;
    let branch_heads = finalized.branch_heads;
    let tracked_roots = finalized.tracked_roots;
    let mut engine_rows = branch_heads
        .iter()
        .map(branch_ref_current_row)
        .collect::<Result<Vec<_>, _>>()?;
    if let Some((highest_seen, timestamp, change_id)) =
        runtime_functions.and_then(FunctionContext::deterministic_sequence_checkpoint)
    {
        engine_rows.push(deterministic_sequence_current_row(
            highest_seen,
            timestamp,
            change_id,
        )?);
    }
    state_rows = retain_untracked_rows_not_superseded_by_engine(state_rows, &engine_rows);
    let row_index = index_prepared_rows(&state_rows)?;

    if state_rows.is_empty()
        && commit_rows.is_empty()
        && branch_heads.is_empty()
        && engine_rows.is_empty()
        && writes.is_empty()
    {
        return Ok(writes);
    }

    let staged_commits = stage_changelog_commits(
        read,
        &mut writes,
        &state_rows,
        &engine_rows,
        &compactable_current_change_ids(read, &state_rows, &engine_rows, &commit_rows).await?,
        &row_index.tracked_row_indices_by_commit,
        &commit_rows,
    )
    .await?;

    stage_state_json_payloads(&mut json_writer, &mut writes, &state_rows)?;

    stage_tracked_roots(
        read,
        &mut writes,
        &state_rows,
        row_index.tracked_row_indices_by_commit,
        tracked_roots,
        staged_commits,
    )
    .await?;
    stage_current_roots(read, &mut writes, &state_rows, &engine_rows, &commit_rows).await?;

    Ok(writes)
}

fn retain_untracked_rows_not_superseded_by_engine(
    rows: Vec<PreparedStateRow>,
    engine_rows: &[EngineCurrentRow],
) -> Vec<PreparedStateRow> {
    let engine_identities = engine_rows
        .iter()
        .map(|row| {
            (
                row.branch_id.as_str(),
                row.change.schema_key.as_str(),
                &row.change.entity_pk,
                row.change.file_id.as_deref(),
            )
        })
        .collect::<BTreeSet<_>>();
    rows.into_iter()
        .filter(|row| {
            !row.untracked
                || !engine_identities.contains(&(
                    row.branch_id.as_str(),
                    row.schema_key.as_str(),
                    &row.entity_pk,
                    row.file_id.as_deref(),
                ))
        })
        .collect()
}

fn stage_state_json_payloads(
    json_writer: &mut crate::json_store::JsonStoreWriter,
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
) -> Result<(), LixError> {
    json_writer.stage_batch(
        writes,
        JsonWritePlacementRef::OutOfBand,
        state_rows.iter().flat_map(json_payloads_from_state_row),
    )?;
    Ok(())
}

fn json_payloads_from_state_row(
    row: &PreparedStateRow,
) -> impl Iterator<Item = NormalizedJsonRef<'_>> {
    row.snapshot
        .iter()
        .chain(row.metadata.iter())
        .filter(|json| !json.is_inline())
        .map(|json| NormalizedJsonRef::trusted_prehashed(json.normalized.as_ref(), json.json_ref))
}

struct PreparedRowIndex {
    tracked_row_indices_by_commit: BTreeMap<CommitId, Vec<RowIndex>>,
}

fn index_prepared_rows(rows: &[PreparedStateRow]) -> Result<PreparedRowIndex, LixError> {
    let mut tracked_row_indices_by_commit = BTreeMap::<CommitId, Vec<RowIndex>>::new();

    for (row_index, row) in rows.iter().enumerate() {
        if row.untracked {
            continue;
        }
        let Some(commit_id) = row.commit_id.as_ref() else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked prepared row is missing commit_id before commit indexing",
            ));
        };
        tracked_row_indices_by_commit
            .entry(*commit_id)
            .or_default()
            .push(row_index);
    }

    Ok(PreparedRowIndex {
        tracked_row_indices_by_commit,
    })
}

#[derive(Clone, Debug)]
struct StagedChangelogCommit {
    change_ids: Vec<ChangeId>,
    selected_change_refs: Vec<StagedCommitChangeRef>,
    commit_change_id: ChangeId,
    commit_created_at: LixTimestamp,
}

async fn stage_changelog_commits(
    read: &mut impl StorageRead,
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    branch_ref_rows: &[EngineCurrentRow],
    compact_change_ids: &[ChangeId],
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    commit_rows: &[FinalizedCommitRow],
) -> Result<BTreeMap<CommitId, StagedChangelogCommit>, LixError> {
    let mut commits = Vec::with_capacity(commit_rows.len());
    let changes = state_rows
        .iter()
        .map(change_record_from_state_row)
        .chain(branch_ref_rows.iter().map(|row| Ok(row.change.clone())))
        .collect::<Result<Vec<_>, _>>()?;
    let mut commit_change_refs = Vec::with_capacity(commit_rows.len());
    let mut staged = BTreeMap::<CommitId, StagedChangelogCommit>::new();
    for commit_row in commit_rows {
        let state_row_indices = tracked_row_indices_by_commit
            .get(&commit_row.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mut refs = Vec::with_capacity(state_row_indices.len());
        let mut change_ids =
            Vec::with_capacity(state_row_indices.len() + commit_row.selected_change_refs.len());
        for &row_index in state_row_indices {
            let row = &state_rows[row_index];
            let change_id = row.change_id.as_ref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked staged row is missing change_id before changelog append",
                )
            })?;
            refs.push(*change_id);
            change_ids.push(*change_id);
        }
        for change_ref in &commit_row.selected_change_refs {
            refs.push(change_ref.change_id);
            change_ids.push(change_ref.change_id);
        }
        commits.push(CommitRecord {
            format_version: 1,
            commit_id: commit_row.commit_id,
            parent_commit_ids: commit_row.parent_commit_ids.clone(),
            change_id: commit_row.change_id,
            author_account_ids: Vec::new(),
            created_at: commit_row.created_at,
        });
        commit_change_refs.push(CommitChangeRefSet {
            commit_id: commit_row.commit_id,
            entries: refs,
        });
        staged.insert(
            commit_row.commit_id,
            StagedChangelogCommit {
                change_ids,
                selected_change_refs: commit_row.selected_change_refs.clone(),
                commit_change_id: commit_row.change_id,
                commit_created_at: commit_row.created_at,
            },
        );
    }

    let append = ChangelogAppend {
        commits,
        changes,
        commit_change_refs,
    };

    let mut writer = ChangelogContext::new().writer(read, writes);
    writer
        .stage_delete_standalone_changes(compact_change_ids)
        .await?;
    writer.stage_append(append).await?;
    Ok(staged)
}

fn change_record_from_state_row(row: &PreparedStateRow) -> Result<ChangeRecord, LixError> {
    let Some(change_id) = row.change_id.as_ref() else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "staged row is missing change_id before changelog change construction",
        ));
    };
    Ok(ChangeRecord {
        format_version: 2,
        change_id: *change_id,
        entity_pk: row.entity_pk.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot: row
            .snapshot
            .as_ref()
            .map_or(crate::json_store::JsonSlot::None, |snapshot| {
                snapshot.slot()
            }),
        metadata: row
            .metadata
            .as_ref()
            .map_or(crate::json_store::JsonSlot::None, |metadata| {
                metadata.slot()
            }),
        created_at: row.updated_at,
        origin_key: row.origin_key.clone(),
    })
}

#[derive(Clone, Debug)]
struct EngineCurrentRow {
    branch_id: String,
    change: ChangeRecord,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
}

fn branch_ref_current_row(head: &PendingBranchHead) -> Result<EngineCurrentRow, LixError> {
    let snapshot = serde_json::to_string(&serde_json::json!({
        "id": head.branch_id,
        "commit_id": head.commit_id.to_string(),
    }))
    .map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to serialize branch-ref current change: {error}"),
        )
    })?;
    if snapshot.len() > crate::json_store::JSON_INLINE_MAX_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!(
                "branch id is too long: its serialized branch ref is {} bytes, but the maximum is {} bytes",
                snapshot.len(),
                crate::json_store::JSON_INLINE_MAX_BYTES,
            ),
        ));
    }
    Ok(EngineCurrentRow {
        branch_id: crate::GLOBAL_BRANCH_ID.to_string(),
        change: ChangeRecord {
            format_version: 2,
            change_id: head.change_id,
            schema_key: crate::branch::BRANCH_REF_SCHEMA_KEY.to_string(),
            entity_pk: EntityPk::single(&head.branch_id),
            file_id: None,
            snapshot: crate::json_store::JsonSlot::from_json(&snapshot),
            metadata: crate::json_store::JsonSlot::None,
            created_at: head.timestamp,
            origin_key: None,
        },
        created_at: head.timestamp,
        updated_at: head.timestamp,
    })
}

fn deterministic_sequence_current_row(
    highest_seen: i64,
    timestamp: LixTimestamp,
    change_id: ChangeId,
) -> Result<EngineCurrentRow, LixError> {
    let entity_pk = EntityPk::single(crate::functions::DETERMINISTIC_SEQUENCE_KEY);
    let snapshot = serde_json::to_string(&serde_json::json!({
        "key": crate::functions::DETERMINISTIC_SEQUENCE_KEY,
        "value": highest_seen,
    }))
    .map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to serialize deterministic sequence change: {error}"),
        )
    })?;
    Ok(EngineCurrentRow {
        branch_id: crate::GLOBAL_BRANCH_ID.to_string(),
        change: ChangeRecord {
            format_version: 2,
            change_id,
            schema_key: "lix_key_value".to_string(),
            entity_pk,
            file_id: None,
            snapshot: crate::json_store::JsonSlot::from_json(&snapshot),
            metadata: crate::json_store::JsonSlot::None,
            created_at: timestamp,
            origin_key: None,
        },
        created_at: timestamp,
        updated_at: timestamp,
    })
}

async fn compactable_current_change_ids(
    read: &(impl StorageRead + Send + Sync + ?Sized),
    state_rows: &[PreparedStateRow],
    engine_rows: &[EngineCurrentRow],
    commit_rows: &[FinalizedCommitRow],
) -> Result<Vec<ChangeId>, LixError> {
    let current = CurrentStateContext::new();
    let reader = current.reader(read);
    let mut compact = BTreeSet::new();
    for request in state_rows
        .iter()
        .map(|row| CurrentStateRowRequest {
            branch_id: row.branch_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_pk: row.entity_pk.clone(),
            file_id: row.file_id.clone(),
        })
        .chain(engine_rows.iter().map(|row| CurrentStateRowRequest {
            branch_id: row.branch_id.clone(),
            schema_key: row.change.schema_key.clone(),
            entity_pk: row.change.entity_pk.clone(),
            file_id: row.change.file_id.clone(),
        }))
        .chain(commit_rows.iter().flat_map(|row| {
            row.selected_change_refs
                .iter()
                .map(|change_ref| CurrentStateRowRequest {
                    branch_id: row.branch_id.clone(),
                    schema_key: change_ref.schema_key.clone(),
                    entity_pk: change_ref.entity_pk.clone(),
                    file_id: change_ref.file_id.clone(),
                })
        }))
    {
        if let Some(previous) = reader.load_index_row(&request).await?
            && previous.untracked()
        {
            compact.insert(previous.change_id);
        }
    }
    let new_ids = state_rows
        .iter()
        .filter_map(|row| row.change_id)
        .chain(engine_rows.iter().map(|row| row.change.change_id))
        .collect::<BTreeSet<_>>();
    compact.retain(|change_id| !new_ids.contains(change_id));
    Ok(compact.into_iter().collect())
}

async fn stage_current_roots(
    read: &(impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    engine_rows: &[EngineCurrentRow],
    commit_rows: &[FinalizedCommitRow],
) -> Result<(), LixError> {
    let mut referenced_roots = BTreeMap::new();
    let mut deleted_branch_roots = BTreeSet::new();
    let current = CurrentStateContext::new();
    let current_reader = current.reader(read);
    for row in state_rows
        .iter()
        .filter(|row| row.untracked && row.schema_key == crate::branch::BRANCH_REF_SCHEMA_KEY)
    {
        let branch_id = row.entity_pk.as_single_string_owned()?;
        let Some(snapshot) = row.snapshot.as_ref() else {
            if current_reader.load_branch_root(&branch_id)?.is_some()
                && branch_has_local_untracked_rows(&current_reader, &branch_id).await?
            {
                return Err(branch_ref_with_untracked_rows_error(&branch_id, true));
            }
            deleted_branch_roots.insert(branch_id);
            continue;
        };
        let Some(commit_id) = snapshot
            .value
            .get("commit_id")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let existing_commit_id =
            load_current_branch_ref_commit_id(&current_reader, &branch_id).await?;
        if existing_commit_id.as_deref() == Some(commit_id) {
            // Updating descriptor fields or assigning the current head again
            // must not disturb branch-local current state.
            continue;
        }
        if existing_commit_id.is_some()
            && branch_has_local_untracked_rows(&current_reader, &branch_id).await?
        {
            return Err(branch_ref_with_untracked_rows_error(&branch_id, false));
        }
        let root_id = crate::tracked_state::load_root(read, commit_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("branch ref targets commit '{commit_id}' without a tracked root"),
                )
            })?;
        referenced_roots.insert(branch_id, root_id);
    }

    let branch_ids = state_rows
        .iter()
        .map(|row| row.branch_id.clone())
        .chain(engine_rows.iter().map(|row| row.branch_id.clone()))
        .chain(commit_rows.iter().map(|row| row.branch_id.clone()))
        .chain(referenced_roots.keys().cloned())
        .collect::<BTreeSet<_>>();
    let mut writer = current.writer(read, writes);
    for branch_id in &branch_ids {
        let state_deltas = state_rows
            .iter()
            .filter(|row| row.branch_id == *branch_id)
            .map(current_delta_from_state_row)
            .collect::<Result<Vec<_>, _>>()?;
        let engine_deltas = engine_rows
            .iter()
            .filter(|row| row.branch_id == *branch_id)
            .map(current_delta_from_engine_row)
            .collect::<Vec<_>>();
        let selected_deltas = commit_rows
            .iter()
            .filter(|row| row.branch_id == *branch_id)
            .flat_map(|row| {
                row.selected_change_refs.iter().map(move |change_ref| {
                    current_delta_from_selected_change_ref(change_ref, row.commit_id)
                })
            })
            .collect::<Vec<_>>();
        let new_deltas = state_deltas
            .into_iter()
            .chain(engine_deltas)
            .chain(selected_deltas)
            .collect::<Vec<_>>();

        if let Some(root_id) = referenced_roots.get(branch_id) {
            if new_deltas.is_empty() {
                writer.stage_branch_root_from_existing(branch_id, root_id)?;
            } else {
                writer
                    .stage_branch_rows_from_existing_root(branch_id, root_id, new_deltas)
                    .await?;
            }
        } else if !new_deltas.is_empty() {
            writer.stage_branch_rows(branch_id, new_deltas).await?;
        }
    }
    for branch_id in deleted_branch_roots {
        writer.stage_delete_branch_root(&branch_id);
    }
    Ok(())
}

async fn load_current_branch_ref_commit_id<S>(
    reader: &crate::current_state::CurrentStateStoreReader<S>,
    branch_id: &str,
) -> Result<Option<String>, LixError>
where
    S: StorageRead + Send + Sync,
{
    let Some(row) = reader
        .load_row(&CurrentStateRowRequest {
            branch_id: crate::GLOBAL_BRANCH_ID.to_string(),
            schema_key: crate::branch::BRANCH_REF_SCHEMA_KEY.to_string(),
            entity_pk: EntityPk::single(branch_id),
            file_id: None,
        })
        .await?
    else {
        return Ok(None);
    };
    let Some(snapshot) = row.snapshot_content.as_deref() else {
        return Ok(None);
    };
    let value = serde_json::from_str::<serde_json::Value>(snapshot).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("invalid current branch ref for '{branch_id}': {error}"),
        )
    })?;
    Ok(value
        .get("commit_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string))
}

async fn branch_has_local_untracked_rows<S>(
    reader: &crate::current_state::CurrentStateStoreReader<S>,
    branch_id: &str,
) -> Result<bool, LixError>
where
    S: StorageRead + Send + Sync,
{
    Ok(reader
        .scan_rows(&CurrentStateScanRequest {
            branch_id: branch_id.to_string(),
            filter: CurrentStateFilter {
                include_tombstones: true,
                ..CurrentStateFilter::default()
            },
            projection: Vec::new(),
            limit: None,
        })
        .await?
        .into_iter()
        .any(|row| row.untracked))
}

fn branch_ref_with_untracked_rows_error(branch_id: &str, deletion: bool) -> LixError {
    let operation = if deletion { "delete" } else { "repoint" };
    LixError::new(
        LixError::CODE_INVALID_PARAM,
        format!(
            "cannot {operation} branch '{branch_id}' while it has branch-local untracked current rows; delete or track those rows first"
        ),
    )
}

fn current_delta_from_selected_change_ref(
    change_ref: &StagedCommitChangeRef,
    commit_id: CommitId,
) -> CurrentStateDeltaRef<'_> {
    CurrentStateDeltaRef {
        schema_key: &change_ref.schema_key,
        file_id: change_ref.file_id.as_deref(),
        entity_pk: &change_ref.entity_pk,
        change_id: change_ref.change_id,
        commit_id: Some(commit_id),
        deleted: change_ref.deleted,
        created_at: change_ref.created_at,
        updated_at: change_ref.updated_at,
    }
}

fn current_delta_from_state_row(
    row: &PreparedStateRow,
) -> Result<CurrentStateDeltaRef<'_>, LixError> {
    let change_id = row.change_id.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "current-state row is missing change_id",
        )
    })?;
    Ok(CurrentStateDeltaRef {
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        entity_pk: &row.entity_pk,
        change_id,
        commit_id: (!row.untracked).then_some(row.commit_id).flatten(),
        deleted: row.snapshot.is_none(),
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

fn current_delta_from_engine_row(row: &EngineCurrentRow) -> CurrentStateDeltaRef<'_> {
    CurrentStateDeltaRef {
        schema_key: &row.change.schema_key,
        file_id: row.change.file_id.as_deref(),
        entity_pk: &row.change.entity_pk,
        change_id: row.change.change_id,
        commit_id: None,
        deleted: row.change.snapshot.is_none(),
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn tracked_delta_from_state_row(
    row: &PreparedStateRow,
) -> Result<TrackedStateDeltaRef<'_>, LixError> {
    let Some(change_id) = row.change_id else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked staged row is missing change_id before tracked root staging",
        ));
    };
    let Some(commit_id) = row.commit_id else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked staged row is missing commit_id before tracked root staging",
        ));
    };
    Ok(TrackedStateDeltaRef {
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        entity_pk: &row.entity_pk,
        change_id,
        commit_id,
        deleted: row.snapshot.is_none(),
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

fn tracked_delta_from_selected_change_ref(
    change_ref: &StagedCommitChangeRef,
    commit_id: CommitId,
) -> Result<TrackedStateDeltaRef<'_>, LixError> {
    Ok(TrackedStateDeltaRef {
        schema_key: &change_ref.schema_key,
        file_id: change_ref.file_id.as_deref(),
        entity_pk: &change_ref.entity_pk,
        change_id: change_ref.change_id,
        commit_id,
        deleted: change_ref.deleted,
        created_at: change_ref.created_at,
        updated_at: change_ref.updated_at,
    })
}

async fn stage_tracked_roots(
    read: &(impl StorageRead + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    tracked_row_indices_by_commit: BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_roots: Vec<PendingTrackedRoot>,
    staged_commits: BTreeMap<CommitId, StagedChangelogCommit>,
) -> Result<(), LixError> {
    let tracked_state = TrackedStateContext::new();
    let mut tracked_writer = tracked_state.writer(read, writes);
    for root in tracked_roots_parent_first(&tracked_roots)? {
        let staged = staged_commits.get(&root.commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked-state root for commit '{}' has no staged changelog facts",
                    root.commit_id
                ),
            )
        })?;
        let state_row_indices = tracked_row_indices_by_commit
            .get(&root.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if state_row_indices.len() > staged.change_ids.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "commit '{}' has {} tracked rows but only {} changelog changes",
                    root.commit_id,
                    state_row_indices.len(),
                    staged.change_ids.len()
                ),
            ));
        }
        let commit_entity_pk = EntityPk::single(root.commit_id.to_string());
        let mut deltas = state_row_indices
            .iter()
            .map(|&row_index| tracked_delta_from_state_row(&state_rows[row_index]))
            .chain(staged.selected_change_refs.iter().map(|change_ref| {
                tracked_delta_from_selected_change_ref(change_ref, root.commit_id)
            }))
            .collect::<Result<Vec<_>, _>>()?;
        deltas.push(TrackedStateDeltaRef {
            schema_key: "lix_commit",
            file_id: None,
            entity_pk: &commit_entity_pk,
            change_id: staged.commit_change_id,
            commit_id: root.commit_id,
            deleted: false,
            created_at: staged.commit_created_at,
            updated_at: staged.commit_created_at,
        });
        let commit_id_text = root.commit_id.to_string();
        let parent_commit_id_text = root.parent_commit_id.map(|id| id.to_string());
        tracked_writer
            .stage_commit_root(&commit_id_text, parent_commit_id_text.as_deref(), deltas)
            .await?;
    }
    let rooted_commit_ids = tracked_roots
        .iter()
        .map(|root| root.commit_id)
        .collect::<BTreeSet<_>>();
    let extra_tracked = tracked_row_indices_by_commit
        .keys()
        .filter(|commit_id| !rooted_commit_ids.contains(commit_id))
        .copied()
        .collect::<BTreeSet<_>>();
    if !extra_tracked.is_empty() {
        let mut commit_ids = tracked_row_indices_by_commit
            .keys()
            .copied()
            .collect::<Vec<_>>();
        commit_ids.sort();
        commit_ids.dedup();
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked live_state rows have no finalized root metadata for commit ids: {}",
                commit_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    }
    if !staged_commits.is_empty() {
        let commit_ids = staged_commits
            .keys()
            .filter(|commit_id| !rooted_commit_ids.contains(commit_id))
            .copied()
            .collect::<Vec<_>>();
        if commit_ids.is_empty() {
            return Ok(());
        }
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "changelog staged commits without tracked root metadata: {}",
                commit_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    }
    Ok(())
}

fn tracked_roots_parent_first(
    tracked_roots: &[PendingTrackedRoot],
) -> Result<Vec<&PendingTrackedRoot>, LixError> {
    let mut roots_by_id = BTreeMap::new();
    for root in tracked_roots {
        if roots_by_id.insert(root.commit_id, root).is_some() {
            return Err(LixError::unknown(format!(
                "cannot stage duplicate tracked_state root '{}'",
                root.commit_id
            )));
        }
    }

    let mut ordered = Vec::with_capacity(tracked_roots.len());
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for root in tracked_roots {
        visit_tracked_root_parent_first(
            root.commit_id,
            &roots_by_id,
            &mut visiting,
            &mut visited,
            &mut ordered,
        )?;
    }
    Ok(ordered)
}

fn visit_tracked_root_parent_first<'a>(
    commit_id: CommitId,
    roots_by_id: &BTreeMap<CommitId, &'a PendingTrackedRoot>,
    visiting: &mut BTreeSet<CommitId>,
    visited: &mut BTreeSet<CommitId>,
    ordered: &mut Vec<&'a PendingTrackedRoot>,
) -> Result<(), LixError> {
    if visited.contains(&commit_id) {
        return Ok(());
    }
    let Some(root) = roots_by_id.get(&commit_id).copied() else {
        return Ok(());
    };
    if !visiting.insert(root.commit_id) {
        return Err(LixError::unknown(format!(
            "cannot stage tracked_state root '{}' because staged root parents contain a cycle",
            root.commit_id
        )));
    }
    if let Some(parent_id) = root.parent_commit_id {
        if roots_by_id.contains_key(&parent_id) {
            visit_tracked_root_parent_first(parent_id, roots_by_id, visiting, visited, ordered)?;
        }
    }
    visiting.remove(&root.commit_id);
    visited.insert(root.commit_id);
    ordered.push(root);
    Ok(())
}

/// Materializes tracked staged change refs into changelog commits.
///
/// Staging only accumulates `branch_id -> change_ids` because commit ids,
/// parent heads, and commit-row timestamps belong to transaction finalization.
/// The `change_ids` list is the ordered set of canonical changes whose effects
/// the commit introduces relative to its first parent.
/// This function turns those change-ref sets into finalized commit facts.
///
/// Commit finalization output split by durability target.
///
/// `commit_rows` are canonical changelog commit facts. tracked_state roots store
/// serving commit roots keyed by the corresponding commit id.
///
/// `branch_heads` are moving refs. Their changes enter the canonical ledger
/// without becoming members of the commits they point at.
struct FinalizedCommitRows {
    commit_rows: Vec<FinalizedCommitRow>,
    branch_heads: Vec<PendingBranchHead>,
    tracked_roots: Vec<PendingTrackedRoot>,
}

struct FinalizedCommitRow {
    branch_id: String,
    commit_id: CommitId,
    parent_commit_ids: Vec<CommitId>,
    created_at: LixTimestamp,
    change_id: ChangeId,
    selected_change_refs: Vec<StagedCommitChangeRef>,
}

struct PendingBranchHead {
    branch_id: String,
    commit_id: CommitId,
    change_id: ChangeId,
    timestamp: LixTimestamp,
}

struct PendingTrackedRoot {
    commit_id: CommitId,
    parent_commit_id: Option<CommitId>,
}

async fn finalize_commit_rows(
    commit_change_refs_by_branch: BTreeMap<String, StagedCommitChangeRefs>,
    extra_commit_parents_by_branch: BTreeMap<String, Vec<CommitId>>,
    branch_ctx: &BranchContext,
    read: &(impl StorageRead + ?Sized),
) -> Result<FinalizedCommitRows, LixError> {
    let mut commit_rows = Vec::new();
    let mut branch_heads = Vec::new();
    let mut tracked_roots = Vec::new();

    for (branch_id, change_refs) in commit_change_refs_by_branch {
        if change_refs.is_empty() && !change_refs.allow_empty {
            continue;
        }

        let commit_id = change_refs.commit_id;
        let commit_change_id = change_refs.commit_change_id;
        let branch_ref_change_id = change_refs.branch_ref_change_id;
        let timestamp = change_refs.created_at;
        let selected_change_refs = change_refs.selected_change_refs;
        let parent_commit_ids = branch_ctx
            .ref_reader(read)
            .load_head_commit_id(&branch_id)
            .await?
            .into_iter()
            .collect::<Vec<_>>();
        let parent_commit_ids = merge_parent_commit_ids(
            parent_commit_ids,
            extra_commit_parents_by_branch
                .get(&branch_id)
                .cloned()
                .unwrap_or_default(),
        );
        let parent_commit_id = parent_commit_ids.first().copied();

        commit_rows.push(FinalizedCommitRow {
            branch_id: branch_id.clone(),
            commit_id,
            parent_commit_ids: parent_commit_ids.clone(),
            created_at: timestamp,
            change_id: commit_change_id,
            selected_change_refs,
        });
        branch_heads.push(PendingBranchHead {
            branch_id: branch_id.clone(),
            commit_id,
            change_id: branch_ref_change_id,
            timestamp,
        });
        tracked_roots.push(PendingTrackedRoot {
            commit_id,
            parent_commit_id,
        });
    }

    Ok(FinalizedCommitRows {
        commit_rows,
        branch_heads,
        tracked_roots,
    })
}

fn merge_parent_commit_ids(mut base: Vec<CommitId>, extra: Vec<CommitId>) -> Vec<CommitId> {
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
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::backend::{
        Backend, BackendError, BackendWrite, CommitResult, KeyRange, PutBatch, SpaceId,
    };
    use crate::branch::BranchContext;
    use crate::catalog::SchemaPlanId;
    use crate::changelog::ChangelogReader;
    use crate::current_state::{CurrentStateContext, CurrentStateRowRequest};
    use crate::live_state::{LiveStateContext, LiveStateRowRequest};
    use crate::storage::{
        InMemoryStorageBackend, InMemoryStorageRead, InMemoryStorageWrite, StorageContext,
        StorageKey, StorageReadOptions, StorageWriteOptions,
    };
    use crate::transaction::types::PreparedRowFacts;
    use crate::{GLOBAL_BRANCH_ID, NullableKeyFilter};

    fn ts(value: &str) -> LixTimestamp {
        LixTimestamp::expect_parse("timestamp", value)
    }

    const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
    const DETERMINISTIC_SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            TrackedStateContext::new(),
            CurrentStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        )
    }

    #[test]
    fn branch_ref_current_row_rejects_snapshot_that_cannot_be_inlined() {
        let error = branch_ref_current_row(&PendingBranchHead {
            branch_id: "b".repeat(crate::json_store::JSON_INLINE_MAX_BYTES),
            commit_id: CommitId::for_test_label("long-branch-head"),
            change_id: ChangeId::for_test_label("long-branch-ref-change"),
            timestamp: ts("2026-01-01T00:00:00Z"),
        })
        .expect_err("oversized engine-authored branch ref should fail clearly");

        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("branch id is too long"));
    }

    #[tokio::test]
    async fn commit_staged_writes_appends_changelog_and_updates_commit_root() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let state_rows = vec![tracked_global_row("change-1")];
        let writes = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs(["change-1"]),
                )]),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("commit should flush staged rows");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("writes should commit");

        let mut changelog_reader = ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &[commit_id("test-uuid-1")],
                projection: crate::changelog::CommitProjection::Full,
            })
            .await
            .expect("changelog commit should load");
        let Some(crate::changelog::CommitLoadEntry::Full {
            record,
            change_ref_chunks,
        }) = commits.entries.into_iter().next().flatten()
        else {
            panic!("changelog commit should exist");
        };
        assert_eq!(record.change_id, change_id("test-uuid-2"));
        assert!(
            change_ref_chunks
                .iter()
                .flat_map(|chunk| chunk.entries.iter())
                .any(|entry| *entry == change_id("change-1"))
        );
        let changes = changelog_reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &[change_id("change-1"), record.change_id],
            })
            .await
            .expect("changelog change should load");
        let mut loaded_changes = changes.entries.into_iter();
        let Some(change) = loaded_changes.next().flatten() else {
            panic!("changelog change should exist");
        };
        assert_eq!(change.change_id, change_id("change-1"));
        assert_eq!(change.schema_key, "test_schema");
        assert!(
            loaded_changes.next().flatten().is_none(),
            "commit row change is derived from changelog.commit, not stored as changelog.change"
        );

        let mut tracked_reader = TrackedStateContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let commit_id_text = commit_id_text("test-uuid-1");
        let commit_rows = tracked_reader
            .scan_rows_at_commit(
                &commit_id_text,
                &crate::tracked_state::TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["lix_commit".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("commit root should scan");
        assert!(
            commit_rows
                .iter()
                .any(|row| row.change_id == record.change_id && row.snapshot_content.is_some()),
            "commit root should surface the derived lix_commit row"
        );

        let loaded_head = branch_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_head_commit_id(GLOBAL_BRANCH_ID)
            .await
            .expect("branch ref load should succeed");
        assert_eq!(loaded_head, Some(record.commit_id));
    }

    #[tokio::test]
    async fn stage_changelog_commits_orders_staged_parents_before_children() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let mut writes = StorageWriteSet::new();
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut parent_row = tracked_global_row("parent-change");
        parent_row.commit_id = Some(CommitId::for_test_label("parent-commit"));
        let mut child_row = tracked_global_row("child-change");
        child_row.commit_id = Some(CommitId::for_test_label("child-commit"));

        let commits = vec![
            FinalizedCommitRow {
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                commit_id: CommitId::for_test_label("child-commit"),
                parent_commit_ids: vec![CommitId::for_test_label("parent-commit")],
                created_at: ts("2026-01-01T00:00:01Z"),
                change_id: ChangeId::for_test_label("child-commit-change"),
                selected_change_refs: Vec::new(),
            },
            FinalizedCommitRow {
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                commit_id: CommitId::for_test_label("parent-commit"),
                parent_commit_ids: Vec::new(),
                created_at: ts("2026-01-01T00:00:00Z"),
                change_id: ChangeId::for_test_label("parent-commit-change"),
                selected_change_refs: Vec::new(),
            },
        ];
        stage_changelog_commits(
            &mut read,
            &mut writes,
            &[parent_row, child_row],
            &[],
            &[],
            &BTreeMap::from([
                (CommitId::for_test_label("parent-commit"), vec![0]),
                (CommitId::for_test_label("child-commit"), vec![1]),
            ]),
            &commits,
        )
        .await
        .expect("child-before-parent input should still stage parent first");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("writes should persist");

        let mut changelog_reader = ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &[
                    CommitId::for_test_label("parent-commit"),
                    CommitId::for_test_label("child-commit"),
                ],
                projection: crate::changelog::CommitProjection::Record,
            })
            .await
            .expect("commits should load");
        assert!(commits.entries.iter().all(Option::is_some));
    }

    #[tokio::test]
    async fn commit_with_only_untracked_writes_does_not_create_lix_commit() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let state_rows = vec![untracked_global_row("change-untracked")];
        let writes = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                commit_change_refs_by_branch: BTreeMap::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("commit should flush untracked row");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("writes should commit");

        let loaded = CurrentStateContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&current_state_request("entity-1"))
            .await
            .expect("current row load should succeed")
            .expect("untracked row should be persisted in current state");
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );
        assert!(loaded.untracked);
        assert_eq!(loaded.change_id, change_id("change-untracked"));

        let mut changelog_reader = ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let changes = changelog_reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &[change_id("change-untracked")],
            })
            .await
            .expect("untracked changelog change should load");
        assert!(matches!(
            changes.entries.as_slice(),
            [Some(change)] if change.change_id == change_id("change-untracked")
        ));
        let commits = changelog_reader
            .scan_commits(crate::changelog::CommitScanRequest {
                start_after: None,
                limit: None,
                projection: crate::changelog::CommitProjection::Record,
            })
            .await
            .expect("commit scan should succeed");
        assert!(
            commits.entries.is_empty(),
            "an untracked-only transaction must not create a commit"
        );
    }

    #[tokio::test]
    async fn tracked_write_replaces_matching_untracked_current_row() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let binary_cas = BinaryCasContext::new();
        let live_state = Arc::new(live_state_context());
        let branch_ctx = BranchContext::new();

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let writes = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![untracked_global_row("change-untracked")],
                commit_change_refs_by_branch: BTreeMap::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("untracked seed should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("untracked seed should commit");

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let state_rows = vec![tracked_global_row("change-tracked")];
        let writes = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs(["change-tracked"]),
                )]),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("tracked commit should flush");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("writes should commit");

        let visible = live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&live_state_request())
            .await
            .expect("live-state load should succeed")
            .expect("tracked row should be visible");
        assert!(!visible.untracked);
        let expected_change_id = change_id("change-tracked");
        assert_eq!(visible.change_id, Some(expected_change_id));
        assert_eq!(visible.snapshot_content.as_deref(), Some("{\"value\":1}"));

        let mut changelog_reader = ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open"),
        );
        let old_untracked = changelog_reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &[change_id("change-untracked")],
            })
            .await
            .expect("superseded untracked change should load deterministically");
        assert_eq!(
            old_untracked.entries,
            vec![None],
            "replacing an untracked current row should compact its old change"
        );
    }

    #[tokio::test]
    async fn commit_staged_writes_applies_cross_subsystem_rows_as_one_backend_batch() {
        let counting_backend = CountingBackend::new();
        let write_batches = counting_backend.write_batches();
        let storage = StorageContext::new(counting_backend);
        let binary_cas = BinaryCasContext::new();
        let live_state = Arc::new(live_state_context());
        let branch_ctx = BranchContext::new();
        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .expect("setup head read should open");
            let mut setup_row = tracked_global_row("setup-tracked-change");
            setup_row.commit_id = Some(commit_id("setup-commit"));
            let writes = commit_prepared_writes(
                &binary_cas,
                &branch_ctx,
                None,
                &mut read,
                PreparedWriteSet {
                    insert_identities: BTreeMap::new(),
                    state_rows: vec![setup_row],
                    commit_change_refs_by_branch: BTreeMap::from([(
                        GLOBAL_BRANCH_ID.to_string(),
                        change_refs_with(
                            ["setup-tracked-change"],
                            "setup-commit",
                            "setup-commit-change",
                            "setup-branch-ref-change",
                        ),
                    )]),
                    extra_commit_parents_by_branch: BTreeMap::new(),
                    file_data_writes: Vec::new(),
                },
            )
            .await
            .expect("setup head should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("setup head should commit");
        }
        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .expect("deterministic mode read should open");
            let writes = commit_prepared_writes(
                &binary_cas,
                &branch_ctx,
                None,
                &mut read,
                PreparedWriteSet {
                    insert_identities: BTreeMap::new(),
                    state_rows: vec![untracked_key_value_row(
                        DETERMINISTIC_MODE_KEY,
                        serde_json::json!({ "enabled": true }),
                        "deterministic-mode-change",
                    )],
                    commit_change_refs_by_branch: BTreeMap::new(),
                    extra_commit_parents_by_branch: BTreeMap::new(),
                    file_data_writes: Vec::new(),
                },
            )
            .await
            .expect("deterministic mode should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("deterministic mode should commit");
        }
        write_batches.store(0, Ordering::SeqCst);
        let runtime_functions = {
            let reader = live_state.reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            );
            FunctionContext::prepare(&reader)
                .await
                .expect("runtime context should prepare")
        };
        runtime_functions.provider().call_uuid_v7();
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let tracked_row = tracked_global_row("change-tracked");
        let mut untracked_row = untracked_global_row("change-untracked");
        untracked_row.entity_pk = EntityPk::single("entity-2");

        let writes = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            Some(&runtime_functions),
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![tracked_row, untracked_row],
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs(["change-tracked"]),
                )]),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("cross-subsystem commit should stage and apply");

        assert_eq!(
            write_batches.load(Ordering::SeqCst),
            0,
            "prepared writes should not touch the backend before the write set is committed"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("writes should commit");
        assert_eq!(write_batches.load(Ordering::SeqCst), 1);

        let mut changelog_reader = ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &[commit_id("test-uuid-1")],
                projection: crate::changelog::CommitProjection::Record,
            })
            .await
            .expect("changelog commit should load");
        let Some(crate::changelog::CommitLoadEntry::Record(commit)) =
            commits.entries.into_iter().next().flatten()
        else {
            panic!("changelog commit should exist");
        };
        assert_eq!(commit.change_id, change_id("test-uuid-2"));
        let changes = changelog_reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &[change_id("change-tracked"), change_id("change-untracked")],
            })
            .await
            .expect("tracked and untracked changelog changes should load");
        assert!(matches!(
            changes.entries.as_slice(),
            [Some(tracked), Some(untracked)]
                if tracked.change_id == change_id("change-tracked")
                    && untracked.change_id == change_id("change-untracked")
        ));

        let loaded_head = branch_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_head_commit_id(GLOBAL_BRANCH_ID)
            .await
            .expect("branch ref load should succeed");
        let expected_commit_id = commit_id("test-uuid-1");
        assert_eq!(loaded_head, Some(expected_commit_id));

        let untracked = CurrentStateContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&current_state_request("entity-2"))
            .await
            .expect("untracked row load should succeed")
            .expect("untracked row should persist in current state");
        assert_eq!(
            untracked.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );
        assert!(untracked.untracked);

        let sequence_row = live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single(DETERMINISTIC_SEQUENCE_KEY),
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
    async fn non_global_tracked_write_creates_one_commit_and_advances_only_touched_branch() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        crate::test_support::seed_branch_head(storage.clone(), GLOBAL_BRANCH_ID, "global-before")
            .await;
        crate::test_support::seed_branch_head(storage.clone(), "branch-a", "branch-a-before").await;

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let state_rows = vec![tracked_branch_row("branch-a", "change-branch-a")];
        let writes = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                commit_change_refs_by_branch: BTreeMap::from([(
                    "branch-a".to_string(),
                    change_refs(["change-branch-a"]),
                )]),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("branch commit should flush");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("writes should commit");

        let mut changelog_reader = ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &[commit_id("test-uuid-1")],
                projection: crate::changelog::CommitProjection::Record,
            })
            .await
            .expect("changelog commit should load");
        let Some(crate::changelog::CommitLoadEntry::Record(commit)) =
            commits.entries.into_iter().next().flatten()
        else {
            panic!("changelog commit should exist");
        };
        assert_eq!(commit.change_id, change_id("test-uuid-2"));
        assert_eq!(
            commit.parent_commit_ids,
            vec![CommitId::for_test_label("branch-a-before")]
        );

        let global_head = branch_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_head_commit_id(GLOBAL_BRANCH_ID)
            .await
            .expect("global head should load");
        let branch_head = branch_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_head_commit_id("branch-a")
            .await
            .expect("branch head should load");
        let expected_global_head = commit_id("global-before");
        let expected_branch_head = commit_id("test-uuid-1");
        assert_eq!(global_head, Some(expected_global_head));
        assert_eq!(branch_head, Some(expected_branch_head));
    }

    #[tokio::test]
    async fn finalize_commit_rows_parents_global_commit_to_existing_branch_ref() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let branch_ctx = BranchContext::new();
        crate::test_support::seed_branch_head(storage.clone(), GLOBAL_BRANCH_ID, "initial-commit")
            .await;

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_BRANCH_ID.to_string(),
                change_refs(["change-a", "change-b"]),
            )]),
            BTreeMap::new(),
            &branch_ctx,
            &mut read,
        )
        .await
        .expect("global commit row should finalize");

        assert_eq!(rows.commit_rows.len(), 1);
        assert_eq!(rows.branch_heads.len(), 1);
        let row = &rows.commit_rows[0];
        assert_eq!(row.commit_id, commit_id("test-uuid-1"));
        assert_eq!(row.change_id, change_id("test-uuid-2"));
        assert_eq!(row.created_at.to_string(), "2026-01-01T00:00:00.001Z");
        assert_eq!(
            row.parent_commit_ids,
            vec![CommitId::for_test_label("initial-commit")]
        );

        let branch_head = &rows.branch_heads[0];
        assert_eq!(branch_head.branch_id, GLOBAL_BRANCH_ID);
        assert_eq!(branch_head.commit_id, commit_id("test-uuid-1"));
    }

    #[tokio::test]
    async fn finalize_commit_rows_skips_empty_members() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let branch_ctx = BranchContext::new();
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_BRANCH_ID.to_string(),
                StagedCommitChangeRefs::default(),
            )]),
            BTreeMap::new(),
            &branch_ctx,
            &mut read,
        )
        .await
        .expect("empty change_refs should be ignored");

        assert!(rows.commit_rows.is_empty());
        assert!(rows.branch_heads.is_empty());
    }

    #[tokio::test]
    async fn finalize_commit_rows_uses_existing_branch_ref_as_parent() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let branch_ctx = BranchContext::new();
        crate::test_support::seed_branch_head(storage.clone(), GLOBAL_BRANCH_ID, "global-before")
            .await;
        crate::test_support::seed_branch_head(storage.clone(), "branch-a", "previous-commit").await;

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([("branch-a".to_string(), change_refs(["change-a"]))]),
            BTreeMap::new(),
            &branch_ctx,
            &mut read,
        )
        .await
        .expect("active-branch commit finalization should resolve parent");

        assert_eq!(
            rows.commit_rows[0].parent_commit_ids,
            vec![CommitId::for_test_label("previous-commit")]
        );
        assert_eq!(rows.branch_heads[0].branch_id, "branch-a");
    }

    #[tokio::test]
    async fn finalize_commit_rows_appends_extra_merge_parent_after_target_head() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let branch_ctx = BranchContext::new();
        crate::test_support::seed_branch_head(storage.clone(), "branch-a", "target-head").await;

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let rows = finalize_commit_rows(
            BTreeMap::from([("branch-a".to_string(), change_refs(["change-a"]))]),
            BTreeMap::from([(
                "branch-a".to_string(),
                vec![CommitId::for_test_label("source-head")],
            )]),
            &branch_ctx,
            &mut read,
        )
        .await
        .expect("merge commit finalization should resolve parents");

        assert_eq!(
            rows.commit_rows[0].parent_commit_ids,
            vec![
                CommitId::for_test_label("target-head"),
                CommitId::for_test_label("source-head")
            ]
        );
    }

    fn change_refs<const N: usize>(change_ids: [&str; N]) -> StagedCommitChangeRefs {
        change_refs_with(change_ids, "test-uuid-1", "test-uuid-2", "test-uuid-3")
    }

    fn change_refs_with<const N: usize>(
        change_ids: [&str; N],
        commit_id_label: &str,
        commit_change_id_label: &str,
        branch_ref_change_id_label: &str,
    ) -> StagedCommitChangeRefs {
        let mut change_refs = StagedCommitChangeRefs::new(
            commit_id(commit_id_label),
            change_id(commit_change_id_label),
            change_id(branch_ref_change_id_label),
            ts("2026-01-01T00:00:00.001Z"),
        );
        for change_id in change_ids {
            change_refs.add_change_id(self::change_id(change_id));
        }
        change_refs
    }

    fn tracked_global_row(change_id: &str) -> PreparedStateRow {
        tracked_branch_row(GLOBAL_BRANCH_ID, change_id)
    }

    fn tracked_branch_row(branch_id: &str, change_id: &str) -> PreparedStateRow {
        PreparedStateRow {
            schema_plan_id: SchemaPlanId::for_test(0),
            facts: PreparedRowFacts::default(),
            entity_pk: EntityPk::single("entity-1"),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot: Some(
                crate::transaction::types::stage_json_from_value(
                    crate::transaction::types::TransactionJson::from_value_for_test(
                        serde_json::json!({ "value": 1 }),
                    ),
                    "test tracked row snapshot",
                )
                .expect("test snapshot should stage"),
            ),
            metadata: None,
            origin: None,
            origin_key: None,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            global: branch_id == GLOBAL_BRANCH_ID,
            change_id: Some(ChangeId::for_test_label(change_id)),
            commit_id: Some(commit_id("test-uuid-1")),
            untracked: false,
            branch_id: branch_id.to_string(),
        }
    }

    fn commit_id(label: &str) -> CommitId {
        CommitId::for_test_label(label)
    }

    fn change_id(label: &str) -> ChangeId {
        ChangeId::for_test_label(label)
    }

    fn commit_id_text(label: &str) -> String {
        commit_id(label).to_string()
    }

    fn untracked_global_row(change_id: &str) -> PreparedStateRow {
        let mut row = tracked_global_row(change_id);
        row.snapshot = Some(
            crate::transaction::types::stage_json_from_value(
                crate::transaction::types::TransactionJson::from_value_for_test(
                    serde_json::json!({ "value": "untracked" }),
                ),
                "test untracked row snapshot",
            )
            .expect("test snapshot should stage"),
        );
        PreparedStateRow {
            change_id: Some(ChangeId::for_test_label(change_id)),
            commit_id: None,
            untracked: true,
            ..row
        }
    }

    fn current_state_request(entity_pk: &str) -> CurrentStateRowRequest {
        CurrentStateRowRequest {
            schema_key: "test_schema".to_string(),
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            entity_pk: EntityPk::single(entity_pk),
            file_id: None,
        }
    }

    fn untracked_key_value_row(
        key: &str,
        value: serde_json::Value,
        change_id: &str,
    ) -> PreparedStateRow {
        let mut row = untracked_global_row(change_id);
        row.entity_pk = EntityPk::single(key);
        row.schema_key = "lix_key_value".to_string();
        row.snapshot = Some(
            crate::transaction::types::stage_json_from_value(
                crate::transaction::types::TransactionJson::from_value_for_test(
                    serde_json::json!({ "key": key, "value": value }),
                ),
                "test untracked key-value snapshot",
            )
            .expect("test key-value snapshot should stage"),
        );
        row
    }

    fn live_state_request() -> LiveStateRowRequest {
        LiveStateRowRequest {
            schema_key: "test_schema".to_string(),
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            entity_pk: EntityPk::single("entity-1"),
            file_id: NullableKeyFilter::Null,
        }
    }

    struct CountingBackend {
        inner: InMemoryStorageBackend,
        write_batches: Arc<AtomicUsize>,
    }

    impl CountingBackend {
        fn new() -> Self {
            Self {
                inner: InMemoryStorageBackend::new(),
                write_batches: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn write_batches(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.write_batches)
        }
    }

    impl Backend for CountingBackend {
        type Read<'a>
            = InMemoryStorageRead
        where
            Self: 'a;

        type Write<'a>
            = CountingWrite
        where
            Self: 'a;
        async fn begin_read(
            &self,
            opts: StorageReadOptions,
        ) -> Result<Self::Read<'_>, BackendError> {
            self.inner.begin_read(opts).await
        }

        async fn begin_write(
            &self,
            opts: StorageWriteOptions,
        ) -> Result<Self::Write<'_>, BackendError> {
            Ok(CountingWrite {
                inner: self.inner.begin_write(opts).await?,
                write_batches: Arc::clone(&self.write_batches),
            })
        }
    }

    struct CountingWrite {
        inner: InMemoryStorageWrite,
        write_batches: Arc<AtomicUsize>,
    }

    impl BackendWrite for CountingWrite {
        async fn put_many(
            &mut self,
            space: SpaceId,
            entries: PutBatch,
        ) -> Result<(), BackendError> {
            self.inner.put_many(space, entries).await
        }

        async fn delete_many(
            &mut self,
            space: SpaceId,
            keys: &[StorageKey],
        ) -> Result<(), BackendError> {
            self.inner.delete_many(space, keys).await
        }

        async fn delete_range(
            &mut self,
            space: SpaceId,
            range: KeyRange,
        ) -> Result<(), BackendError> {
            self.inner.delete_range(space, range).await
        }

        async fn commit(self) -> Result<CommitResult, BackendError> {
            self.write_batches.fetch_add(1, Ordering::SeqCst);
            self.inner.commit().await
        }

        async fn rollback(self) -> Result<(), BackendError> {
            self.inner.rollback().await
        }
    }
}
