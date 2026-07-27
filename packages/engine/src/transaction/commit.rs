#![allow(
    clippy::implicit_clone,
    clippy::unnecessary_mut_passed,
    clippy::unnecessary_wraps
)]

use crate::LixError;
use crate::binary_cas::BinaryCasContext;
use crate::branch::{
    BRANCH_REF_SCHEMA_KEY, BranchContext, BranchHeadControl, BranchHeadControlContext,
    BranchHeadControlObservation, BranchRefReader, branch_head_control_precondition,
    stage_branch_head_control, stage_delete_branch_head_control,
};
use crate::changelog::{
    ChangeId, ChangeRecord, ChangeRecordProjection, ChangelogContext, ChangelogReader,
    ChangelogWriter, CommitChangeRefSet, CommitId, CommitLoadRequest as ChangelogCommitLoadRequest,
    CommitProjection as ChangelogCommitProjection, CommitRecord, TransactionChangeRecordRef,
    TransactionChangelogAppend, materialize_change_payloads,
};
use crate::common::{LixTimestamp, compose_directory_path, compose_file_path};
use crate::entity_pk::EntityPk;
use crate::filesystem::stage_path_index_revision;
use crate::functions::FunctionContext;
use crate::json_store::{JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
use crate::live_state::{
    HotTrackedSnapshot, MaterializedLiveStateRow, TrackedHeadContext, TrackedWorkingDiffEpoch,
    WorkingDiffIndexCoverage, stage_tracked_working_diff_epoch,
};
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition, StorageWriteSet};
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateContext, TrackedStateDeltaRef, TrackedStateFilter,
    TrackedStateKey, TrackedStateKeyRef, TrackedStateReadColumns, TrackedStateRootMutationRef,
    TrackedStateScanRequest, encode_key_ref, stage_commit_deltas,
};
use crate::transaction::staging::{
    PreparedInsertIdentity, PreparedStateRowIdentity, PreparedWriteSet,
};
use crate::transaction::types::{PreparedStateRow, StagedCommitChangeRef, StagedCommitChangeRefs};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use tracing::Instrument as _;

type RowIndex = usize;

/// Commits prepared transaction rows into tracked history and unified current
/// live state.
///
/// Providers decode DataFusion DML into hydrated `PreparedStateRow`s. Tracked
/// rows become changelog facts, commit members, immutable history roots, and
/// current-state members. Untracked rows update only their current-state
/// members.
#[cfg(test)]
pub(crate) async fn commit_prepared_writes(
    binary_cas: &BinaryCasContext,
    branch_ctx: &BranchContext,
    runtime_functions: Option<&FunctionContext>,
    read: &mut impl StorageAdapterRead,
    prepared_writes: PreparedWriteSet,
) -> Result<(StorageWriteSet, Vec<StoragePrecondition>), LixError> {
    let commit_parent_heads =
        resolve_prepared_commit_parent_heads(branch_ctx, &*read, &prepared_writes, false).await?;
    commit_prepared_writes_with_parent_heads(
        binary_cas,
        runtime_functions,
        &commit_parent_heads,
        read,
        prepared_writes,
    )
    .await
}

/// Materializes a prepared commit with branch heads already resolved from the
/// caller's coherent commit snapshot.
pub(crate) async fn commit_prepared_writes_with_parent_heads(
    binary_cas: &BinaryCasContext,
    runtime_functions: Option<&FunctionContext>,
    commit_parent_heads: &BTreeMap<String, Option<CommitId>>,
    read: &mut impl StorageAdapterRead,
    prepared_writes: PreparedWriteSet,
) -> Result<(StorageWriteSet, Vec<StoragePrecondition>), LixError> {
    let certified_fresh_plugin_file_id =
        crate::transaction::validation::fresh_plugin_file_import_certificate(&prepared_writes)
            .is_some()
            .then(|| prepared_writes.file_data_writes[0].file_id.clone());
    let mut writes = StorageWriteSet::new();
    let mut preconditions = Vec::new();
    for publication in &prepared_writes.checkpoint_publications {
        crate::gc::stage_recovery_ref_rotation(&mut writes, &publication.recovery_ref)?;
        crate::gc::stage_checkpoint_gc_state(&mut writes, &publication.gc_state)?;
    }
    let mut json_writer = JsonStoreContext::new().writer();

    if !prepared_writes.file_data_writes.is_empty() {
        let mut blob_writer = binary_cas.writer_skipping_existing_chunks(&*read, &mut writes);
        for write in &prepared_writes.file_data_writes {
            blob_writer
                .stage_file_payload(write.payload(), write.same_length_blob_splice())
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.binary_cas_stage_payload"
                ))
                .await?;
            for payload in write.auxiliary_payloads() {
                blob_writer.stage_payload(payload).await?;
            }
        }
    }

    let filesystem_view_changed = prepared_writes.state_rows.iter().any(|row| {
        matches!(
            row.schema_key.as_str(),
            "lix_file_descriptor" | "lix_directory_descriptor" | BRANCH_REF_SCHEMA_KEY
        )
    }) || prepared_writes
        .commit_change_refs_by_branch
        .values()
        .flat_map(|change_refs| change_refs.selected_change_refs.iter())
        .any(|change_ref| {
            matches!(
                change_ref.schema_key.as_str(),
                "lix_file_descriptor" | "lix_directory_descriptor"
            )
        });
    let mut state_rows = prepared_writes.state_rows;
    let insert_identities = prepared_writes.insert_identities;
    let finalized = finalize_commit_rows(
        prepared_writes.commit_change_refs_by_branch,
        prepared_writes.first_commit_parent_override_by_branch,
        prepared_writes.extra_commit_parents_by_branch,
        commit_parent_heads,
    )
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.materialization.finalize_commit_rows"
    ))
    .await?;
    let commit_rows = finalized.commit_rows;
    let tracked_roots = finalized.tracked_roots;
    let checkpoint_epochs = checkpoint_epoch_bindings(&prepared_writes.checkpoint_publications)?;
    // The current-state protocol removes the automatic mutable branch-ref
    // row for a normal branch-head advance, but `lix_change` remains an
    // unscoped public ledger. Retain one tiny direct change fact per
    // published control.
    let branch_head_changes = tracked_roots
        .iter()
        .map(branch_ref_change_record)
        .collect::<Result<Vec<_>, _>>()?;
    let has_checkpoint_publication = !prepared_writes.checkpoint_publications.is_empty();
    // The current-state protocol publishes automatic tracked heads through
    // one direct control record.
    // Do not also synthesize a mutable `lix_branch_ref` current row for every
    // normal commit: `branch_head_changes` above preserves the immutable
    // public `lix_change` ledger fact. Explicit branch-management writes
    // retain their legacy row lowering below while control records are the
    // sole authority readers consult.
    let mut engine_rows = Vec::new();
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
        && engine_rows.is_empty()
        && writes.is_empty()
    {
        return Ok((writes, preconditions));
    }

    let staged_commits = stage_changelog_commits(
        read,
        &mut writes,
        &state_rows,
        &branch_head_changes,
        &engine_rows,
        &[],
        &row_index.tracked_row_indices_by_commit,
        &commit_rows,
        has_checkpoint_publication,
    )
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.materialization.changelog"
    ))
    .await?;

    ensure_explicit_branch_ref_targets_exist(read, &state_rows, &staged_commits).await?;

    stage_tracked_commit_delta_index(
        &mut writes,
        &state_rows,
        &row_index.tracked_row_indices_by_commit,
        &tracked_roots,
        &staged_commits,
    )?;

    stage_state_json_payloads(&mut json_writer, &mut writes, &state_rows)?;

    let branch_control_observations =
        observe_branch_head_controls(read, &tracked_roots, &state_rows, &engine_rows).await?;

    reject_explicit_branch_ref_lifecycle_with_untracked_rows(
        read,
        &state_rows,
        &engine_rows,
        &branch_control_observations,
    )
    .await?;

    stage_tracked_roots(
        read,
        &mut writes,
        &state_rows,
        &row_index.tracked_row_indices_by_commit,
        &tracked_roots,
        &staged_commits,
        &insert_identities,
        has_checkpoint_publication,
    )
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.materialization.tracked_roots"
    ))
    .await?;
    let staged_hot_heads = stage_tracked_head(
        read,
        &mut writes,
        &state_rows,
        &engine_rows,
        &row_index.tracked_row_indices_by_commit,
        &tracked_roots,
        &staged_commits,
        &insert_identities,
        certified_fresh_plugin_file_id.as_deref(),
        &branch_control_observations,
        &checkpoint_epochs,
    )
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.materialization.tracked_head"
    ))
    .await?;
    stage_checkpoint_working_diff_epochs(
        &mut writes,
        &prepared_writes.checkpoint_publications,
        &staged_hot_heads.controls,
    )?;
    stage_branch_head_control_publications(
        read,
        &mut writes,
        &staged_hot_heads.controls,
        &staged_hot_heads.tracked_snapshots,
        &state_rows,
        &engine_rows,
        &insert_identities,
        &prepared_writes.checkpoint_publications,
        &mut preconditions,
        &branch_control_observations,
    )
    .await?;
    if filesystem_view_changed {
        stage_path_index_revision(&mut writes);
    }
    Ok((writes, preconditions))
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
    change_count: usize,
    selected_change_refs: Vec<StagedCommitChangeRef>,
}

async fn stage_changelog_commits(
    read: &mut impl StorageAdapterRead,
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    branch_head_changes: &[ChangeRecord],
    _branch_ref_rows: &[EngineCurrentRow],
    compact_change_ids: &[ChangeId],
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    commit_rows: &[FinalizedCommitRow],
    force_root_fence: bool,
) -> Result<BTreeMap<CommitId, StagedChangelogCommit>, LixError> {
    let mut commits = Vec::with_capacity(commit_rows.len());
    let changes = state_rows
        .iter()
        // Ordinary untracked members are intentionally current-state only in
        // V15. `lix_branch_ref` is the one control-plane exception: its
        // published control retains a public ref_change_id, so that immutable
        // ledger fact must remain available to `lix_change` and GC even
        // though it is not a commit member.
        .filter(|row| !row.untracked || row.schema_key == BRANCH_REF_SCHEMA_KEY)
        .map(transaction_change_record_from_state_row)
        .chain(
            branch_head_changes
                .iter()
                .map(|change| Ok(TransactionChangeRecordRef::from(change))),
        )
        // Engine-owned untracked state follows the same current-only rule.
        .collect::<Result<Vec<_>, _>>()?;
    let mut commit_change_refs = Vec::with_capacity(commit_rows.len());
    let mut staged = BTreeMap::<CommitId, StagedChangelogCommit>::new();
    for commit_row in commit_rows {
        let state_row_indices = tracked_row_indices_by_commit
            .get(&commit_row.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        validate_selected_change_refs(
            commit_row.commit_id,
            state_rows,
            state_row_indices,
            &commit_row.selected_change_refs,
        )?;
        let mut refs = Vec::with_capacity(state_row_indices.len());
        for &row_index in state_row_indices {
            let row = &state_rows[row_index];
            let change_id = row.change_id.as_ref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked staged row is missing change_id before changelog append",
                )
            })?;
            refs.push(*change_id);
        }
        for change_ref in &commit_row.selected_change_refs {
            refs.push(change_ref.change_id);
        }
        commits.push(CommitRecord {
            format_version: 1,
            commit_id: commit_row.commit_id,
            parent_commit_ids: commit_row.parent_commit_ids.clone(),
            // Every commit carries absolute per-identity deltas against its
            // first parent. Additional ancestry and selected historical refs
            // therefore do not require eagerly materializing the full root.
            tracked_state_rootless: !force_root_fence,
            change_id: commit_row.change_id,
            author_account_ids: Vec::new(),
            created_at: commit_row.created_at,
        });
        let change_count = refs.len();
        commit_change_refs.push(CommitChangeRefSet {
            commit_id: commit_row.commit_id,
            entries: refs,
        });
        staged.insert(
            commit_row.commit_id,
            StagedChangelogCommit {
                change_count,
                selected_change_refs: commit_row.selected_change_refs.clone(),
            },
        );
    }

    let append = TransactionChangelogAppend {
        commits,
        changes,
        commit_change_refs,
    };

    let mut writer = ChangelogContext::new().writer(read, writes);
    writer
        .stage_delete_standalone_changes(compact_change_ids)
        .await?;
    writer.stage_transaction_append(append)?;
    Ok(staged)
}

/// The terminal transaction append deliberately skips the changelog writer's
/// read-heavy validation because ordinary prepared rows are freshly generated
/// and already canonical. Selected historical refs are the one irregular
/// lane: validate their combined commit membership locally before appending.
fn validate_selected_change_refs(
    commit_id: CommitId,
    state_rows: &[PreparedStateRow],
    state_row_indices: &[RowIndex],
    selected_change_refs: &[StagedCommitChangeRef],
) -> Result<(), LixError> {
    if selected_change_refs.is_empty() {
        return Ok(());
    }

    let mut change_ids = BTreeSet::new();
    let mut identities = BTreeSet::new();
    for &row_index in state_row_indices {
        let row = &state_rows[row_index];
        let change_id = row.change_id.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked staged row is missing change_id before changelog append",
            )
        })?;
        if !change_ids.insert(change_id) {
            return Err(LixError::unknown(format!(
                "commit '{commit_id}' has duplicate change ref '{change_id}'"
            )));
        }
        if !identities.insert((
            row.schema_key.as_str(),
            row.file_id.as_deref(),
            &row.entity_pk,
        )) {
            return Err(LixError::unknown(format!(
                "commit '{commit_id}' has duplicate change ref key"
            )));
        }
    }
    for change_ref in selected_change_refs {
        if !change_ids.insert(change_ref.change_id) {
            return Err(LixError::unknown(format!(
                "commit '{commit_id}' has duplicate change ref '{}'",
                change_ref.change_id
            )));
        }
        if !identities.insert((
            change_ref.schema_key.as_str(),
            change_ref.file_id.as_deref(),
            &change_ref.entity_pk,
        )) {
            return Err(LixError::unknown(format!(
                "commit '{commit_id}' has duplicate change ref key"
            )));
        }
    }
    Ok(())
}

fn transaction_change_record_from_state_row(
    row: &PreparedStateRow,
) -> Result<TransactionChangeRecordRef<'_>, LixError> {
    let Some(change_id) = row.change_id.as_ref() else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "staged row is missing change_id before changelog change construction",
        ));
    };
    Ok(TransactionChangeRecordRef {
        format_version: 2,
        change_id: *change_id,
        entity_pk: &row.entity_pk,
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        snapshot: row.snapshot.as_ref().map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction::types::StageJson::slot_ref,
        ),
        metadata: row.metadata.as_ref().map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction::types::StageJson::slot_ref,
        ),
        created_at: row.updated_at,
        origin_key: row.origin_key.as_deref(),
    })
}

#[derive(Clone, Debug)]
struct EngineCurrentRow {
    branch_id: String,
    change: ChangeRecord,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
}

/// Builds the standalone public ledger fact for an automatic branch-head
/// advance. The current-state control owns current visibility; this fact
/// keeps the existing `lix_change` contract without reintroducing a mutable
/// current row.
fn branch_ref_change_record(root: &PendingTrackedRoot) -> Result<ChangeRecord, LixError> {
    let snapshot = serde_json::to_string(&serde_json::json!({
        "id": root.branch_id,
        "commit_id": root.commit_id.to_string(),
    }))
    .map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to serialize direct branch-ref change: {error}"),
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
    Ok(ChangeRecord {
        format_version: 2,
        change_id: root.ref_change_id,
        schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
        entity_pk: EntityPk::single(&root.branch_id),
        file_id: None,
        snapshot: crate::json_store::JsonSlot::from_json(&snapshot),
        metadata: crate::json_store::JsonSlot::None,
        created_at: root.ref_updated_at,
        origin_key: None,
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

fn current_state_delta_from_state_row(
    row: &PreparedStateRow,
) -> Result<crate::live_state::CurrentStateDeltaRef<'_>, LixError> {
    let Some(change_id) = row.change_id else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "staged row is missing change_id before current-state staging",
        ));
    };
    let commit_id = if row.untracked {
        None
    } else {
        Some(row.commit_id.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked staged row is missing commit_id before current-state staging",
            )
        })?)
    };
    Ok(crate::live_state::CurrentStateDeltaRef {
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        entity_pk: &row.entity_pk,
        change_id: (!row.untracked).then_some(change_id),
        commit_id,
        untracked: row.untracked,
        deleted: row.snapshot.is_none(),
        created_at: row.created_at,
        updated_at: row.updated_at,
        snapshot: row.snapshot.as_ref().map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction::types::StageJson::slot_ref,
        ),
        metadata: row.metadata.as_ref().map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction::types::StageJson::slot_ref,
        ),
    })
}

fn current_state_delta_from_engine_row(
    row: &EngineCurrentRow,
) -> crate::live_state::CurrentStateDeltaRef<'_> {
    crate::live_state::CurrentStateDeltaRef {
        schema_key: &row.change.schema_key,
        file_id: row.change.file_id.as_deref(),
        entity_pk: &row.change.entity_pk,
        change_id: None,
        commit_id: None,
        untracked: true,
        deleted: row.change.snapshot == crate::json_store::JsonSlot::None,
        created_at: row.created_at,
        updated_at: row.updated_at,
        snapshot: row.change.snapshot.as_ref_slot(),
        metadata: row.change.metadata.as_ref_slot(),
    }
}

/// Stages a compact, identity-addressable change record for every tracked
/// commit. Sparse immutable roots remain the scan/checkpoint structure; this
/// index is the missing point-read structure for rootless first-parent
/// history.
fn stage_tracked_commit_delta_index(
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_roots: &[PendingTrackedRoot],
    staged_commits: &BTreeMap<CommitId, StagedChangelogCommit>,
) -> Result<(), LixError> {
    for root in tracked_roots {
        let state_row_indices = tracked_row_indices_by_commit
            .get(&root.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let staged = staged_commits.get(&root.commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked commit '{}' has no changelog facts for commit-delta staging",
                    root.commit_id
                ),
            )
        })?;
        let mut deltas =
            Vec::with_capacity(state_row_indices.len() + staged.selected_change_refs.len());
        for &row_index in state_row_indices {
            deltas.push(tracked_delta_from_state_row(&state_rows[row_index])?);
        }
        for change_ref in &staged.selected_change_refs {
            deltas.push(tracked_delta_from_selected_change_ref(
                change_ref,
                root.commit_id,
            )?);
        }
        stage_commit_deltas(writes, &deltas)?;
    }
    Ok(())
}

struct StagedHotHeads {
    controls: BTreeMap<String, BranchHeadControl>,
    tracked_snapshots: BTreeMap<CommitId, HotTrackedSnapshot>,
}

/// Returns the commit snapshots that must be materialized before publication.
/// Normal serial commits and entity-only selected refs stay on the
/// O(changed-rows) hot mutation path. A lifecycle discontinuity (checkpoint,
/// staged parent, or branch creation) and selected refs whose filesystem
/// invariants span rows need a complete tracked snapshot so the serving
/// control never points at a partially reconstructed view.
fn lifecycle_snapshot_commit_ids(
    state_rows: &[PreparedStateRow],
    tracked_roots: &[PendingTrackedRoot],
    staged_commits: &BTreeMap<CommitId, StagedChangelogCommit>,
    observations: &BTreeMap<String, BranchHeadControlObservation>,
    checkpoint_epochs: &BTreeMap<String, CommitId>,
) -> Result<BTreeSet<CommitId>, LixError> {
    let roots_by_id = tracked_roots
        .iter()
        .map(|root| (root.commit_id, root))
        .collect::<BTreeMap<_, _>>();
    let mut required = BTreeSet::new();
    for root in tracked_roots {
        let staged = staged_commits.get(&root.commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked head for commit '{}' has no staged changelog facts",
                    root.commit_id
                ),
            )
        })?;
        let parent_is_published = matches!(
            (
                root.parent_commit_id,
                observations.get(&root.branch_id).and_then(|observation| observation.control),
            ),
            (Some(parent_commit_id), Some(control))
                if control.head_commit_id == parent_commit_id
        );
        if !parent_is_published
            || selected_refs_require_complete_snapshot(&staged.selected_change_refs)
            || checkpoint_epochs.get(&root.branch_id) == Some(&root.commit_id)
        {
            required.insert(root.commit_id);
        }
    }
    for target in explicit_branch_head_targets(state_rows)?.into_values() {
        if let Some(commit_id) = target.head_commit_id
            && roots_by_id.contains_key(&commit_id)
        {
            required.insert(commit_id);
        }
    }

    // If a required snapshot has a parent created in this transaction, that
    // parent must itself be materialized in memory first.  This is an
    // explicit transaction-local snapshot dependency, not a storage overlay.
    loop {
        let parents = required
            .iter()
            .filter_map(|commit_id| {
                roots_by_id
                    .get(commit_id)
                    .and_then(|root| root.parent_commit_id)
            })
            .filter(|parent| roots_by_id.contains_key(parent))
            .collect::<Vec<_>>();
        let mut changed = false;
        for parent in parents {
            changed |= required.insert(parent);
        }
        if !changed {
            break;
        }
    }
    Ok(required)
}

fn selected_refs_require_complete_snapshot(selected_change_refs: &[StagedCommitChangeRef]) -> bool {
    selected_change_refs.iter().any(|change_ref| {
        matches!(
            change_ref.schema_key.as_str(),
            FILE_DESCRIPTOR_SCHEMA_KEY
                | DIRECTORY_DESCRIPTOR_SCHEMA_KEY
                | DERIVED_FILE_REF_SCHEMA_KEY
        )
    })
}

/// Resolves the full tracked view for only the rare lifecycle commits selected
/// by [`lifecycle_snapshot_commit_ids`].  Persisted ancestors come from the
/// canonical tracked-state history.  Pending ancestors are replayed from the
/// transaction's already validated first-parent deltas, so a same-write-set
/// branch/ref target does not require reading an uncommitted write set.
async fn build_lifecycle_tracked_snapshots(
    read: &(impl StorageAdapterRead + ?Sized),
    state_rows: &[PreparedStateRow],
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_roots: &[PendingTrackedRoot],
    staged_commits: &BTreeMap<CommitId, StagedChangelogCommit>,
    insert_identities: &BTreeMap<PreparedStateRowIdentity, PreparedInsertIdentity>,
    required: &BTreeSet<CommitId>,
) -> Result<BTreeMap<CommitId, HotTrackedSnapshot>, LixError> {
    if required.is_empty() {
        return Ok(BTreeMap::new());
    }

    let roots_by_id = tracked_roots
        .iter()
        .map(|root| (root.commit_id, root))
        .collect::<BTreeMap<_, _>>();
    let mut prepared_by_change = HashMap::new();
    for row in state_rows.iter().filter(|row| !row.untracked) {
        let change_id = row.change_id.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked lifecycle snapshot row is missing change_id",
            )
        })?;
        let live = MaterializedLiveStateRow::from(row);
        let tracked = MaterializedTrackedStateRow::try_from(&live)?;
        if prepared_by_change.insert(change_id, tracked).is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("tracked lifecycle snapshot contains duplicate change '{change_id}'"),
            ));
        }
    }
    let selected_change_ids = tracked_roots
        .iter()
        .filter(|root| required.contains(&root.commit_id))
        .flat_map(|root| {
            staged_commits
                .get(&root.commit_id)
                .into_iter()
                .flat_map(|staged| staged.selected_change_refs.iter())
        })
        .map(|change_ref| change_ref.change_id)
        .filter(|change_id| !prepared_by_change.contains_key(change_id))
        .collect::<BTreeSet<_>>();
    let selected_payloads = materialize_change_payloads(
        read,
        selected_change_ids.iter().copied(),
        ChangeRecordProjection::full(),
        "lifecycle tracked snapshot",
    )
    .await?;

    let mut snapshots =
        BTreeMap::<CommitId, BTreeMap<TrackedStateKey, MaterializedTrackedStateRow>>::new();
    for root in tracked_roots_parent_first(tracked_roots)? {
        if !required.contains(&root.commit_id) {
            continue;
        }
        let mut rows = match root.parent_commit_id {
            None => BTreeMap::new(),
            Some(parent_commit_id) if snapshots.contains_key(&parent_commit_id) => snapshots
                .get(&parent_commit_id)
                .expect("snapshot presence was checked")
                .clone(),
            Some(parent_commit_id) if roots_by_id.contains_key(&parent_commit_id) => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "lifecycle snapshot for '{}' is missing staged parent '{}'",
                        root.commit_id, parent_commit_id
                    ),
                ));
            }
            Some(parent_commit_id) => {
                load_persisted_lifecycle_tracked_snapshot(read, parent_commit_id).await?
            }
        };
        let row_indices = tracked_row_indices_by_commit
            .get(&root.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for &row_index in row_indices {
            let row = &state_rows[row_index];
            let live = MaterializedLiveStateRow::from(row);
            let tracked = MaterializedTrackedStateRow::try_from(&live)?;
            apply_lifecycle_tracked_snapshot_row(
                &mut rows,
                tracked,
                tracked_row_requires_absence(row, insert_identities),
            )?;
        }
        let staged = staged_commits.get(&root.commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "lifecycle snapshot for commit '{}' has no staged changelog facts",
                    root.commit_id
                ),
            )
        })?;
        for change_ref in &staged.selected_change_refs {
            let source = prepared_by_change.get(&change_ref.change_id);
            let payload = selected_payloads.get(&change_ref.change_id);
            let tracked =
                lifecycle_selected_tracked_row(change_ref, root.commit_id, source, payload)?;
            apply_lifecycle_tracked_snapshot_row(&mut rows, tracked, false)?;
        }
        validate_lifecycle_derived_materialization_paths(&rows, &root.branch_id, root.commit_id)?;
        snapshots.insert(root.commit_id, rows);
    }

    required
        .iter()
        .map(|commit_id| {
            let rows = snapshots.get(commit_id).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("lifecycle snapshot for commit '{commit_id}' was not built"),
                )
            })?;
            Ok((
                *commit_id,
                HotTrackedSnapshot::from_materialized_rows(rows.values().cloned().collect())?,
            ))
        })
        .collect()
}

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const DERIVED_FILE_REF_SCHEMA_KEY: &str = "lix_derived_file_ref";

/// A derived materialization is valid only for the exact descriptor path that
/// was supplied to its renderer. Normal transactions reject path-only moves
/// before staging; lifecycle snapshots additionally cover selected historical
/// refs, such as merge picks, which bypass ordinary write reconciliation.
///
/// Keep this at the complete-snapshot seam rather than teaching every caller
/// that can select historical refs about plugin semantics. It makes the
/// invariant hold for all future lifecycle publications without adding a
/// full-state scan to ordinary O(changed-rows) commits.
fn validate_lifecycle_derived_materialization_paths(
    rows: &BTreeMap<TrackedStateKey, MaterializedTrackedStateRow>,
    branch_id: &str,
    commit_id: CommitId,
) -> Result<(), LixError> {
    let mut directories = BTreeMap::<String, LifecycleDirectoryDescriptor>::new();
    let mut files = BTreeMap::<String, LifecycleFileDescriptor>::new();
    let mut proofs = Vec::<LifecycleDerivedFileRef>::new();

    for row in rows.values().filter(|row| !row.deleted) {
        let Some(snapshot) = row.snapshot_content.as_deref() else {
            continue;
        };
        match row.schema_key.as_str() {
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                let descriptor: LifecycleDirectoryDescriptor = serde_json::from_str(snapshot)
                    .map_err(|error| {
                        lifecycle_derived_materialization_error(format!(
                            "branch '{branch_id}' commit '{commit_id}' has an invalid directory descriptor: {error}"
                        ))
                    })?;
                if row.entity_pk.as_single_string().ok() != Some(descriptor.id.as_str()) {
                    return Err(lifecycle_derived_materialization_error(format!(
                        "branch '{branch_id}' commit '{commit_id}' has a directory descriptor whose id does not match its primary key"
                    )));
                }
                if directories
                    .insert(descriptor.id.clone(), descriptor)
                    .is_some()
                {
                    return Err(lifecycle_derived_materialization_error(format!(
                        "branch '{branch_id}' commit '{commit_id}' has duplicate directory descriptor ids"
                    )));
                }
            }
            FILE_DESCRIPTOR_SCHEMA_KEY => {
                let descriptor: LifecycleFileDescriptor = serde_json::from_str(snapshot).map_err(
                    |error| {
                        lifecycle_derived_materialization_error(format!(
                            "branch '{branch_id}' commit '{commit_id}' has an invalid file descriptor: {error}"
                        ))
                    },
                )?;
                if row.entity_pk.as_single_string().ok() != Some(descriptor.id.as_str()) {
                    return Err(lifecycle_derived_materialization_error(format!(
                        "branch '{branch_id}' commit '{commit_id}' has a file descriptor whose id does not match its primary key"
                    )));
                }
                if files.insert(descriptor.id.clone(), descriptor).is_some() {
                    return Err(lifecycle_derived_materialization_error(format!(
                        "branch '{branch_id}' commit '{commit_id}' has duplicate file descriptor ids"
                    )));
                }
            }
            DERIVED_FILE_REF_SCHEMA_KEY => {
                let proof: LifecycleDerivedFileRef = serde_json::from_str(snapshot).map_err(
                    |error| {
                        lifecycle_derived_materialization_error(format!(
                            "branch '{branch_id}' commit '{commit_id}' has an invalid derived materialization proof: {error}"
                        ))
                    },
                )?;
                if row.entity_pk.as_single_string().ok() != Some(proof.id.as_str())
                    || row.file_id.as_deref() != Some(proof.id.as_str())
                {
                    return Err(lifecycle_derived_materialization_error(format!(
                        "branch '{branch_id}' commit '{commit_id}' has a derived materialization proof whose id does not match its row identity"
                    )));
                }
                proofs.push(proof);
            }
            _ => {}
        }
    }

    if proofs.is_empty() {
        return Ok(());
    }
    let mut directory_paths = BTreeMap::new();
    for proof in proofs {
        let file = files.get(&proof.id).ok_or_else(|| {
            lifecycle_derived_materialization_error(format!(
                "derived-materialization file '{}' on branch '{branch_id}' commit '{commit_id}' has no live descriptor",
                proof.id,
            ))
        })?;
        let parent_path = match file.directory_id.as_deref() {
            Some(directory_id) => Some(lifecycle_directory_path(
                directory_id,
                &directories,
                &mut directory_paths,
                &mut BTreeSet::new(),
                branch_id,
                commit_id,
            )?),
            None => None,
        };
        let path = compose_file_path(parent_path.as_deref(), &file.name)?;
        if proof.path != path {
            return Err(lifecycle_derived_materialization_error(format!(
                "derived-materialization file '{}' on branch '{branch_id}' commit '{commit_id}' was rendered at '{}' but its final descriptor resolves to '{}'",
                proof.id, proof.path, path,
            )));
        }
    }
    Ok(())
}

fn lifecycle_directory_path(
    id: &str,
    directories: &BTreeMap<String, LifecycleDirectoryDescriptor>,
    paths: &mut BTreeMap<String, String>,
    visiting: &mut BTreeSet<String>,
    branch_id: &str,
    commit_id: CommitId,
) -> Result<String, LixError> {
    if let Some(path) = paths.get(id) {
        return Ok(path.clone());
    }
    if !visiting.insert(id.to_string()) {
        return Err(lifecycle_derived_materialization_error(format!(
            "derived-materialization validation found a directory cycle at '{id}' on branch '{branch_id}' commit '{commit_id}'"
        )));
    }
    let directory = directories.get(id).ok_or_else(|| {
        lifecycle_derived_materialization_error(format!(
            "derived-materialization validation found missing directory '{id}' on branch '{branch_id}' commit '{commit_id}'"
        ))
    })?;
    let parent_path = match directory.parent_id.as_deref() {
        Some(parent_id) => Some(lifecycle_directory_path(
            parent_id,
            directories,
            paths,
            visiting,
            branch_id,
            commit_id,
        )?),
        None => None,
    };
    let path = compose_directory_path(parent_path.as_deref(), &directory.name)?;
    visiting.remove(id);
    paths.insert(id.to_string(), path.clone());
    Ok(path)
}

fn lifecycle_derived_materialization_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_CONSTRAINT_VIOLATION, message).with_hint(
        "Keep a derived file at the path recorded by its proof, or recreate it at the destination so its plugin can publish a new proof.",
    )
}

#[derive(Debug, Deserialize)]
struct LifecycleDirectoryDescriptor {
    id: String,
    parent_id: Option<String>,
    name: String,
}

#[derive(Debug, Deserialize)]
struct LifecycleFileDescriptor {
    id: String,
    directory_id: Option<String>,
    name: String,
}

#[derive(Debug, Deserialize)]
struct LifecycleDerivedFileRef {
    id: String,
    path: String,
}

async fn load_persisted_lifecycle_tracked_snapshot(
    read: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<BTreeMap<TrackedStateKey, MaterializedTrackedStateRow>, LixError> {
    let rows = TrackedStateContext::new()
        .reader(read)
        .scan_rows_at_commit(
            &commit_id.to_string(),
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    include_tombstones: true,
                    ..TrackedStateFilter::default()
                },
                read_columns: TrackedStateReadColumns::default(),
                limit: None,
            },
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let key = TrackedStateKey {
                schema_key: row.schema_key.clone(),
                entity_pk: row.entity_pk.clone(),
                file_id: row.file_id.clone(),
            };
            (key, row)
        })
        .collect())
}

fn lifecycle_selected_tracked_row(
    change_ref: &StagedCommitChangeRef,
    commit_id: CommitId,
    prepared: Option<&MaterializedTrackedStateRow>,
    payload: Option<&crate::changelog::MaterializedChangePayload>,
) -> Result<MaterializedTrackedStateRow, LixError> {
    let (schema_key, entity_pk, file_id, snapshot_content, metadata) = if let Some(row) = prepared {
        (
            row.schema_key.clone(),
            row.entity_pk.clone(),
            row.file_id.clone(),
            row.snapshot_content.clone(),
            row.metadata.clone(),
        )
    } else {
        let payload = payload.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "selected lifecycle change '{}' is missing from the changelog",
                    change_ref.change_id
                ),
            )
        })?;
        let identity = payload.identity.as_ref().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "selected lifecycle change '{}' omitted its identity",
                    change_ref.change_id
                ),
            )
        })?;
        (
            identity.schema_key.clone(),
            identity.entity_pk.clone(),
            identity.file_id.clone(),
            payload.snapshot_content.clone(),
            payload.metadata.clone(),
        )
    };
    if schema_key != change_ref.schema_key
        || entity_pk != change_ref.entity_pk
        || file_id != change_ref.file_id
        || snapshot_content.is_none() != change_ref.deleted
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "selected lifecycle change '{}' does not match its staged identity or deletion flag",
                change_ref.change_id
            ),
        ));
    }
    Ok(MaterializedTrackedStateRow {
        entity_pk,
        schema_key,
        file_id,
        snapshot_content,
        metadata,
        deleted: change_ref.deleted,
        created_at: change_ref.created_at.to_string(),
        updated_at: change_ref.updated_at.to_string(),
        change_id: change_ref.change_id,
        commit_id,
    })
}

fn apply_lifecycle_tracked_snapshot_row(
    rows: &mut BTreeMap<TrackedStateKey, MaterializedTrackedStateRow>,
    mut next: MaterializedTrackedStateRow,
    require_absence: bool,
) -> Result<(), LixError> {
    let key = TrackedStateKey {
        schema_key: next.schema_key.clone(),
        entity_pk: next.entity_pk.clone(),
        file_id: next.file_id.clone(),
    };
    if let Some(previous) = rows.get(&key) {
        if require_absence && !previous.deleted {
            return Err(lifecycle_duplicate_tracked_row_error(&key));
        }
        next.created_at.clone_from(&previous.created_at);
    }
    rows.insert(key, next);
    Ok(())
}

fn lifecycle_duplicate_tracked_row_error(key: &TrackedStateKey) -> LixError {
    let entity_pk = key
        .entity_pk
        .as_json_array_text()
        .unwrap_or_else(|_| "<invalid entity_pk>".to_string());
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "primary-key constraint violation on schema '{}': INSERT would duplicate entity_pk '{entity_pk}'",
            key.schema_key
        ),
    )
}

fn lifecycle_generation(
    branch_id: &str,
    head_commit_id: CommitId,
    ref_change_id: ChangeId,
) -> CommitId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"lix.live_state.lifecycle_generation.v1");
    hasher.update(&(branch_id.len() as u64).to_be_bytes());
    hasher.update(branch_id.as_bytes());
    hasher.update(head_commit_id.as_uuid().as_bytes());
    hasher.update(ref_change_id.as_uuid().as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    CommitId::new(uuid::Uuid::from_bytes(bytes))
}

/// Stages the hot serving plane.  Serial normal commits mutate their current
/// generation; every lifecycle discontinuity publishes a complete fresh
/// generation before its branch control is made visible.
async fn stage_tracked_head(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    engine_rows: &[EngineCurrentRow],
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_roots: &[PendingTrackedRoot],
    staged_commits: &BTreeMap<CommitId, StagedChangelogCommit>,
    insert_identities: &BTreeMap<PreparedStateRowIdentity, PreparedInsertIdentity>,
    certified_fresh_plugin_file_id: Option<&str>,
    observations: &BTreeMap<String, BranchHeadControlObservation>,
    checkpoint_epochs: &BTreeMap<String, CommitId>,
) -> Result<StagedHotHeads, LixError> {
    let lifecycle_ids = lifecycle_snapshot_commit_ids(
        state_rows,
        tracked_roots,
        staged_commits,
        observations,
        checkpoint_epochs,
    )?;
    let mut tracked_snapshots = build_lifecycle_tracked_snapshots(
        read,
        state_rows,
        tracked_row_indices_by_commit,
        tracked_roots,
        staged_commits,
        insert_identities,
        &lifecycle_ids,
    )
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.materialization.tracked_head.lifecycle"
    ))
    .await?;
    let explicit_branches = explicit_branch_head_targets(state_rows)?
        .into_keys()
        .collect::<BTreeSet<_>>();
    let tracked_head = TrackedHeadContext::new();
    let mut controls = BTreeMap::new();

    for root in tracked_roots_parent_first(tracked_roots)? {
        let staged = staged_commits.get(&root.commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked head for commit '{}' has no staged changelog facts",
                    root.commit_id
                ),
            )
        })?;
        let parent_control = observations
            .get(&root.branch_id)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "missing current-state branch-control observation for normal publication branch '{}'",
                        root.branch_id
                    ),
                )
            })?
            .control;
        let state_row_indices = tracked_row_indices_by_commit
            .get(&root.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let mut tracked_deltas = {
            let _span = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.materialization.tracked_head.deltas"
            )
            .entered();
            state_row_indices
                .iter()
                .map(|&row_index| current_state_delta_from_state_row(&state_rows[row_index]))
                .collect::<Result<Vec<_>, _>>()?
        };
        let selected_materialization = if !staged.selected_change_refs.is_empty()
            && !tracked_snapshots.contains_key(&root.commit_id)
        {
            let payloads = materialize_change_payloads(
                read,
                staged
                    .selected_change_refs
                    .iter()
                    .map(|change_ref| change_ref.change_id),
                ChangeRecordProjection::full(),
                "selected current-state delta",
            )
            .await?;
            let selected_rows = staged
                .selected_change_refs
                .iter()
                .map(|change_ref| {
                    lifecycle_selected_tracked_row(
                        change_ref,
                        root.commit_id,
                        None,
                        payloads.get(&change_ref.change_id),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let selected_snapshots = selected_rows
                .iter()
                .map(|row| {
                    row.snapshot_content.as_deref().map_or(
                        crate::json_store::JsonSlot::None,
                        crate::json_store::JsonSlot::from_json,
                    )
                })
                .collect::<Vec<crate::json_store::JsonSlot>>();
            let selected_metadata = selected_rows
                .iter()
                .map(|row| {
                    row.metadata.as_deref().map_or(
                        crate::json_store::JsonSlot::None,
                        crate::json_store::JsonSlot::from_json,
                    )
                })
                .collect::<Vec<crate::json_store::JsonSlot>>();
            Some((selected_rows, selected_snapshots, selected_metadata))
        } else {
            None
        };
        if let Some((selected_rows, selected_snapshots, selected_metadata)) =
            &selected_materialization
        {
            tracked_deltas.extend(
                staged
                    .selected_change_refs
                    .iter()
                    .zip(selected_rows)
                    .zip(selected_snapshots.iter().zip(selected_metadata))
                    .map(|((change_ref, row), (snapshot, metadata))| {
                        crate::live_state::CurrentStateDeltaRef {
                            schema_key: &row.schema_key,
                            file_id: row.file_id.as_deref(),
                            entity_pk: &row.entity_pk,
                            change_id: Some(change_ref.change_id),
                            commit_id: Some(root.commit_id),
                            untracked: false,
                            deleted: change_ref.deleted,
                            created_at: change_ref.created_at,
                            updated_at: change_ref.updated_at,
                            snapshot: snapshot.as_ref_slot(),
                            metadata: metadata.as_ref_slot(),
                        }
                    }),
            );
        }
        let mut untracked_deltas = state_rows
            .iter()
            .filter(|row| {
                row.untracked
                    && row.branch_id == root.branch_id
                    && row.schema_key != BRANCH_REF_SCHEMA_KEY
            })
            .map(current_state_delta_from_state_row)
            .collect::<Result<Vec<_>, _>>()?;
        untracked_deltas.extend(
            engine_rows
                .iter()
                .filter(|row| row.branch_id == root.branch_id)
                .map(current_state_delta_from_engine_row),
        );
        let absence_guards = {
            let _span = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.materialization.tracked_head.absence_guards"
            )
            .entered();
            if insert_identities.is_empty() {
                BTreeSet::new()
            } else {
                state_rows
                    .iter()
                    .filter(|row| {
                        row.branch_id == root.branch_id
                            && row.schema_key != BRANCH_REF_SCHEMA_KEY
                            && row.snapshot.is_some()
                            && insert_identities.contains_key(&PreparedStateRowIdentity::from(*row))
                    })
                    .map(|row| TrackedStateKey {
                        schema_key: row.schema_key.clone(),
                        file_id: row.file_id.clone(),
                        entity_pk: row.entity_pk.clone(),
                    })
                    .collect()
            }
        };
        let parent_generation = match (root.parent_commit_id, parent_control) {
            (Some(parent_commit_id), Some(control))
                if control.head_commit_id == parent_commit_id =>
            {
                Some(control.generation)
            }
            _ => None,
        };
        let checkpoint_commit_id = checkpoint_epochs.get(&root.branch_id).copied();

        if !staged.selected_change_refs.is_empty() {
            reject_selected_tracked_refs_with_untracked_rows(
                read,
                &root.branch_id,
                parent_control,
                &staged.selected_change_refs,
                state_rows,
                engine_rows,
            )
            .await?;
        }

        if let Some(final_tracked) = tracked_snapshots.get(&root.commit_id).cloned() {
            let generation =
                lifecycle_generation(&root.branch_id, root.commit_id, root.ref_change_id);
            let mut coverage = WorkingDiffIndexCoverage::default();
            let final_tracked = tracked_head
                .writer(read, writes)
                .stage_complete_current_state_with_working_diff(
                    &root.branch_id,
                    generation,
                    final_tracked,
                    parent_control.map(|control| control.generation),
                    &[],
                    &untracked_deltas,
                    &absence_guards,
                    checkpoint_commit_id,
                    &mut coverage,
                )
                .await?;
            tracked_snapshots.insert(root.commit_id, final_tracked);
            insert_direct_branch_control(
                &mut controls,
                &root.branch_id,
                normal_branch_head_control(root, parent_control, generation, checkpoint_commit_id)?,
            )?;
            continue;
        }

        let parent_generation = parent_generation.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "normal hot publication for '{}' lacks a complete parent generation",
                    root.commit_id
                ),
            )
        })?;
        let working_diff_epoch = if checkpoint_commit_id.is_some() {
            None
        } else {
            match (root.parent_commit_id, parent_control) {
                (Some(parent_commit_id), Some(control))
                    if control.head_commit_id == parent_commit_id =>
                {
                    match tracked_head
                        .reader(read)
                        .working_diff_epoch(&root.branch_id)
                        .await
                    {
                        Ok(Some(epoch))
                            if epoch.generation == parent_generation
                                && control.working_diff_checkpoint_commit_id
                                    == Some(epoch.checkpoint_commit_id) =>
                        {
                            Some(epoch)
                        }
                        _ => None,
                    }
                }
                _ => None,
            }
        };
        let working_diff_capture_checkpoint_commit_id = working_diff_epoch
            .as_ref()
            .map(|epoch| epoch.checkpoint_commit_id);
        let working_diff_checkpoint_commit_id =
            checkpoint_commit_id.or(working_diff_capture_checkpoint_commit_id);
        let mut coverage = working_diff_epoch
            .as_ref()
            .map(|epoch| epoch.coverage)
            .unwrap_or_default();
        let mut deltas = tracked_deltas;
        deltas.extend(untracked_deltas);
        // Every absence guard above is derived from one of these exact
        // transaction deltas. The fresh-file certificate likewise proves its
        // complete file-scoped namespace absent. The branch-control CAS
        // protects both proofs through publication.
        let has_validated_insert_deltas = staged.selected_change_refs.is_empty()
            && (!absence_guards.is_empty() || certified_fresh_plugin_file_id.is_some());
        let mut writer = tracked_head.writer(read, writes);
        let generation = if has_validated_insert_deltas {
            writer
                .stage_validated_insert_current_state_with_working_diff(
                    &root.branch_id,
                    Some(parent_generation),
                    root.commit_id,
                    &deltas,
                    &absence_guards,
                    None,
                    None,
                    working_diff_capture_checkpoint_commit_id,
                    &mut coverage,
                    certified_fresh_plugin_file_id,
                )
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.materialization.tracked_head.stage_current_state"
                ))
                .await?
        } else {
            writer
                .stage_current_state_with_working_diff(
                    &root.branch_id,
                    Some(parent_generation),
                    root.commit_id,
                    &deltas,
                    &absence_guards,
                    None,
                    None,
                    working_diff_capture_checkpoint_commit_id,
                    &mut coverage,
                )
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.materialization.tracked_head.stage_current_state"
                ))
                .await?
        };
        if let Some(epoch) = working_diff_epoch {
            let next_epoch = TrackedWorkingDiffEpoch {
                checkpoint_commit_id: epoch.checkpoint_commit_id,
                generation,
                coverage,
            };
            if next_epoch != epoch {
                stage_tracked_working_diff_epoch(writes, &root.branch_id, next_epoch)?;
            }
        }
        insert_direct_branch_control(
            &mut controls,
            &root.branch_id,
            normal_branch_head_control(
                root,
                parent_control,
                generation,
                working_diff_checkpoint_commit_id,
            )?,
        )?;
    }

    // An untracked-only transaction touches the same hot rows without
    // creating a commit.  Explicit branch ref publication handles its own
    // branch-local untracked mutations in the fresh generation below.
    let rooted_branches = tracked_roots
        .iter()
        .map(|root| root.branch_id.as_str())
        .collect::<BTreeSet<_>>();
    let current_only_branches = state_rows
        .iter()
        .filter(|row| row.untracked && row.schema_key != BRANCH_REF_SCHEMA_KEY)
        .map(|row| row.branch_id.as_str())
        .chain(engine_rows.iter().map(|row| row.branch_id.as_str()))
        .filter(|branch_id| {
            !rooted_branches.contains(branch_id) && !explicit_branches.contains(*branch_id)
        })
        .collect::<BTreeSet<_>>();
    for branch_id in current_only_branches {
        let control = observations
            .get(branch_id)
            .and_then(|observation| observation.control)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "missing current branch control for untracked current-state mutation on '{branch_id}'"
                    ),
                )
            })?;
        let mut deltas = state_rows
            .iter()
            .filter(|row| {
                row.untracked
                    && row.branch_id == branch_id
                    && row.schema_key != BRANCH_REF_SCHEMA_KEY
            })
            .map(current_state_delta_from_state_row)
            .collect::<Result<Vec<_>, _>>()?;
        deltas.extend(
            engine_rows
                .iter()
                .filter(|row| row.branch_id == branch_id)
                .map(current_state_delta_from_engine_row),
        );
        let absence_guards = if insert_identities.is_empty() {
            BTreeSet::new()
        } else {
            state_rows
                .iter()
                .filter(|row| {
                    row.untracked
                        && row.branch_id == branch_id
                        && row.schema_key != BRANCH_REF_SCHEMA_KEY
                        && row.snapshot.is_some()
                        && insert_identities.contains_key(&PreparedStateRowIdentity::from(*row))
                })
                .map(|row| TrackedStateKey {
                    schema_key: row.schema_key.clone(),
                    file_id: row.file_id.clone(),
                    entity_pk: row.entity_pk.clone(),
                })
                .collect()
        };
        let mut coverage = WorkingDiffIndexCoverage::default();
        tracked_head
            .writer(read, writes)
            .stage_current_state_with_working_diff(
                branch_id,
                Some(control.generation),
                control.head_commit_id,
                &deltas,
                &absence_guards,
                None,
                None,
                None,
                &mut coverage,
            )
            .await?;
        insert_direct_branch_control(
            &mut controls,
            branch_id,
            control.next_current_state_revision()?,
        )?;
    }
    Ok(StagedHotHeads {
        controls,
        tracked_snapshots,
    })
}

/// A selected historical change becomes visible through a newly materialized
/// hot generation while that publication retains the branch's untracked
/// members. The two retention modes must never own the same logical identity.
///
/// This is a merge/checkpoint lifecycle fence, not a normal CRUD-path check.
/// Point-loading only the selected identities keeps large untracked workspaces
/// out of the publication cost.
async fn reject_selected_tracked_refs_with_untracked_rows(
    read: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    control: Option<BranchHeadControl>,
    selected_change_refs: &[StagedCommitChangeRef],
    state_rows: &[PreparedStateRow],
    engine_rows: &[EngineCurrentRow],
) -> Result<(), LixError> {
    let selected_identities = selected_change_refs
        .iter()
        .map(|change_ref| TrackedStateKey {
            schema_key: change_ref.schema_key.clone(),
            file_id: change_ref.file_id.clone(),
            entity_pk: change_ref.entity_pk.clone(),
        })
        .collect::<BTreeSet<_>>();
    if selected_identities.is_empty() {
        return Ok(());
    }

    let selected_keys = selected_identities.iter().cloned().collect::<Vec<_>>();
    let mut untracked_identities = if let Some(control) = control {
        TrackedHeadContext::new()
            .reader(read)
            .load_projected_live_rows(
                branch_id,
                control,
                &selected_keys,
                &ChangeRecordProjection {
                    snapshot_content: false,
                    metadata: false,
                },
            )
            .await?
            .into_iter()
            .flatten()
            .filter(|row| row.untracked)
            .map(|row| TrackedStateKey {
                schema_key: row.schema_key,
                file_id: row.file_id,
                entity_pk: row.entity_pk,
            })
            .collect()
    } else {
        BTreeSet::new()
    };

    apply_pending_untracked_identities(
        &mut untracked_identities,
        branch_id,
        state_rows,
        engine_rows,
    );

    let Some(identity) = selected_identities
        .iter()
        .find(|identity| untracked_identities.contains(*identity))
    else {
        return Ok(());
    };
    Err(selected_tracked_ref_untracked_collision_error(
        branch_id, identity,
    ))
}

/// Applies the transaction's history-free mutations to an identity set.
///
/// Lifecycle decisions use the final current state, not merely whether an
/// untracked row was mentioned by the transaction. A physical untracked
/// delete therefore removes its identity before either decision is made.
fn apply_pending_untracked_identities(
    identities: &mut BTreeSet<TrackedStateKey>,
    branch_id: &str,
    state_rows: &[PreparedStateRow],
    engine_rows: &[EngineCurrentRow],
) {
    for row in state_rows.iter().filter(|row| {
        row.untracked && row.branch_id == branch_id && row.schema_key != BRANCH_REF_SCHEMA_KEY
    }) {
        let identity = TrackedStateKey {
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            entity_pk: row.entity_pk.clone(),
        };
        if row.snapshot.is_some() {
            identities.insert(identity);
        } else {
            identities.remove(&identity);
        }
    }
    for row in engine_rows.iter().filter(|row| row.branch_id == branch_id) {
        let identity = TrackedStateKey {
            schema_key: row.change.schema_key.clone(),
            file_id: row.change.file_id.clone(),
            entity_pk: row.change.entity_pk.clone(),
        };
        if row.change.snapshot == crate::json_store::JsonSlot::None {
            identities.remove(&identity);
        } else {
            identities.insert(identity);
        }
    }
}

fn selected_tracked_ref_untracked_collision_error(
    branch_id: &str,
    identity: &TrackedStateKey,
) -> LixError {
    LixError::new(
        LixError::CODE_MERGE_CONFLICT,
        format!(
            "cannot publish selected tracked change on branch '{branch_id}': it conflicts with an untracked current row for schema '{}' entity_pk {:?}",
            identity.schema_key, identity.entity_pk
        ),
    )
    .with_hint("Resolve the tracked and untracked identity conflict before retrying.")
    .with_details(serde_json::json!({
        "kind": "trackedUntrackedIdentityCollision",
        "branchId": branch_id,
        "schemaKey": &identity.schema_key,
        "entityPk": &identity.entity_pk,
        "fileId": &identity.file_id,
    }))
}

/// A checkpoint materializes one complete hot generation. Every tracked row
/// in that generation is encoded as a clean row-local baseline, so its sparse
/// dirty-key index starts empty and later ordinary writes capture their own
/// first-before images without querying a separate diff store.
fn stage_checkpoint_working_diff_epochs(
    writes: &mut StorageWriteSet,
    publications: &[crate::gc::CheckpointPublication],
    controls: &BTreeMap<String, BranchHeadControl>,
) -> Result<(), LixError> {
    let mut branches = BTreeSet::new();
    for publication in publications {
        let recovery = &publication.recovery_ref;
        if !branches.insert(recovery.branch_id.as_str()) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "transaction publishes more than one working-diff checkpoint epoch for branch '{}'",
                    recovery.branch_id
                ),
            ));
        }
        let control = controls.get(&recovery.branch_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "checkpoint '{}' has no complete hot control for '{}'",
                    recovery.checkpoint_commit_id, recovery.branch_id
                ),
            )
        })?;
        if control.head_commit_id != recovery.checkpoint_commit_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "checkpoint '{}' does not match staged hot control '{}' for '{}'",
                    recovery.checkpoint_commit_id, control.head_commit_id, recovery.branch_id
                ),
            ));
        }
        stage_tracked_working_diff_epoch(
            writes,
            &recovery.branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: recovery.checkpoint_commit_id,
                generation: control.generation,
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )?;
    }
    Ok(())
}

fn normal_branch_head_control(
    root: &PendingTrackedRoot,
    previous: Option<BranchHeadControl>,
    generation: CommitId,
    working_diff_checkpoint_commit_id: Option<CommitId>,
) -> Result<BranchHeadControl, LixError> {
    let current_state_revision = match previous {
        Some(control) => control
            .current_state_revision
            .checked_add(1)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "branch current-state revision overflowed",
                )
            })?,
        None => 0,
    };
    Ok(BranchHeadControl {
        head_commit_id: root.commit_id,
        generation,
        current_state_revision,
        working_diff_checkpoint_commit_id,
        created_at: previous.map_or(root.ref_updated_at, |control| control.created_at),
        updated_at: root.ref_updated_at,
        ref_change_id: root.ref_change_id,
    })
}

fn insert_direct_branch_control(
    controls: &mut BTreeMap<String, BranchHeadControl>,
    branch_id: &str,
    control: BranchHeadControl,
) -> Result<(), LixError> {
    if controls.insert(branch_id.to_string(), control).is_some() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked direct plane received multiple normal publications for branch '{branch_id}' in one commit"
            ),
        ));
    }
    Ok(())
}

/// Publishes every current-state branch control under an exact-byte CAS token.
///
/// Normal tracked commits arrive as `normal_controls`, built from the same
/// parent/generation decision that wrote the current-state hot rows. Explicit branch
/// management still enters the prepared-row pipeline for validation and
/// changelog compatibility, but its authoritative moving head is lowered
/// here as well. This deliberately keeps the rare lifecycle lane compatible
/// while removing automatic `lix_branch_ref` materialization from normal
/// CRUD commits.
#[allow(clippy::too_many_arguments)]
async fn stage_branch_head_control_publications(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    normal_controls: &BTreeMap<String, BranchHeadControl>,
    tracked_snapshots: &BTreeMap<CommitId, HotTrackedSnapshot>,
    state_rows: &[PreparedStateRow],
    engine_rows: &[EngineCurrentRow],
    insert_identities: &BTreeMap<PreparedStateRowIdentity, PreparedInsertIdentity>,
    checkpoint_publications: &[crate::gc::CheckpointPublication],
    preconditions: &mut Vec<StoragePrecondition>,
    observations: &BTreeMap<String, BranchHeadControlObservation>,
) -> Result<(), LixError> {
    let checkpoint_epochs = checkpoint_epoch_bindings(checkpoint_publications)?;
    let explicit_targets = explicit_branch_head_targets(state_rows)?;
    let mut publications = normal_controls
        .iter()
        .map(|(branch_id, control)| (branch_id.clone(), Some(*control)))
        .collect::<BTreeMap<String, Option<BranchHeadControl>>>();
    let tracked_head = TrackedHeadContext::new();

    for (branch_id, target) in explicit_targets {
        if publications.contains_key(&branch_id) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "cannot publish an explicit branch ref and a normal tracked commit for branch '{branch_id}' in one transaction"
                ),
            ));
        }
        let existing = observations
            .get(&branch_id)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "missing current-state branch-control observation for explicit publication branch '{branch_id}'"
                    ),
                )
            })?
            .control;
        let desired = match target.head_commit_id {
            None => None,
            Some(head_commit_id) => {
                let tracked = if let Some(snapshot) = tracked_snapshots.get(&head_commit_id) {
                    snapshot.clone()
                } else {
                    let rows =
                        load_persisted_lifecycle_tracked_snapshot(read, head_commit_id).await?;
                    HotTrackedSnapshot::from_materialized_rows(rows.into_values().collect())?
                };
                let mut untracked_deltas = state_rows
                    .iter()
                    .filter(|row| {
                        row.untracked
                            && row.branch_id == branch_id
                            && row.schema_key != BRANCH_REF_SCHEMA_KEY
                    })
                    .map(current_state_delta_from_state_row)
                    .collect::<Result<Vec<_>, _>>()?;
                untracked_deltas.extend(
                    engine_rows
                        .iter()
                        .filter(|row| row.branch_id == branch_id)
                        .map(current_state_delta_from_engine_row),
                );
                let absence_guards = if insert_identities.is_empty() {
                    BTreeSet::new()
                } else {
                    state_rows
                        .iter()
                        .filter(|row| {
                            row.untracked
                                && row.branch_id == branch_id
                                && row.schema_key != BRANCH_REF_SCHEMA_KEY
                                && row.snapshot.is_some()
                                && insert_identities
                                    .contains_key(&PreparedStateRowIdentity::from(*row))
                        })
                        .map(|row| TrackedStateKey {
                            schema_key: row.schema_key.clone(),
                            file_id: row.file_id.clone(),
                            entity_pk: row.entity_pk.clone(),
                        })
                        .collect()
                };
                let generation =
                    lifecycle_generation(&branch_id, head_commit_id, target.ref_change_id);
                let mut coverage = WorkingDiffIndexCoverage::default();
                tracked_head
                    .writer(read, writes)
                    .stage_complete_current_state_with_working_diff(
                        &branch_id,
                        generation,
                        tracked,
                        existing.map(|control| control.generation),
                        &[],
                        &untracked_deltas,
                        &absence_guards,
                        None,
                        &mut coverage,
                    )
                    .await?;
                Some(BranchHeadControl {
                    head_commit_id,
                    generation,
                    current_state_revision: match existing {
                        Some(control) => {
                            control
                                .current_state_revision
                                .checked_add(1)
                                .ok_or_else(|| {
                                    LixError::new(
                                        LixError::CODE_INTERNAL_ERROR,
                                        "branch current-state revision overflowed",
                                    )
                                })?
                        }
                        None => 0,
                    },
                    working_diff_checkpoint_commit_id: None,
                    created_at: existing.map_or(target.created_at, |control| control.created_at),
                    updated_at: target.updated_at,
                    ref_change_id: target.ref_change_id,
                })
            }
        };
        publications.insert(branch_id, desired);
    }

    if publications.is_empty() {
        if !checkpoint_epochs.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "checkpoint epoch publication has no corresponding branch-control publication",
            ));
        }
        return Ok(());
    }
    for (branch_id, desired) in &mut publications {
        if let Some(checkpoint_commit_id) = checkpoint_epochs.get(branch_id) {
            let control = desired.as_mut().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "checkpoint '{checkpoint_commit_id}' has no published branch control for '{branch_id}'"
                    ),
                )
            })?;
            if control.head_commit_id != *checkpoint_commit_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "checkpoint '{}' does not match published branch head '{}' for '{}'",
                        checkpoint_commit_id, control.head_commit_id, branch_id
                    ),
                ));
            }
            control.working_diff_checkpoint_commit_id = Some(*checkpoint_commit_id);
        }
        let observation = observations.get(branch_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "missing current-state branch-control observation for publication branch '{branch_id}'"
                ),
            )
        })?;
        preconditions.push(branch_head_control_precondition(
            branch_id,
            observation.raw_token.clone(),
        )?);
        match desired {
            Some(control) => stage_branch_head_control(writes, branch_id, *control)?,
            None => stage_delete_branch_head_control(writes, branch_id)?,
        }
    }
    Ok(())
}

fn checkpoint_epoch_bindings(
    publications: &[crate::gc::CheckpointPublication],
) -> Result<BTreeMap<String, CommitId>, LixError> {
    let mut bindings = BTreeMap::new();
    for publication in publications {
        let recovery = &publication.recovery_ref;
        if bindings
            .insert(recovery.branch_id.clone(), recovery.checkpoint_commit_id)
            .is_some()
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "transaction publishes more than one working-diff checkpoint epoch for branch '{}'",
                    recovery.branch_id
                ),
            ));
        }
    }
    Ok(bindings)
}

/// Returns explicit public branch-ref targets. `None` is a deletion; `Some`
/// is a validated commit id. The current-state control record remains the
/// authority, while retaining these rows in generic lifecycle lowering keeps
/// target-existence checks in force.
struct ExplicitBranchHeadTarget {
    head_commit_id: Option<CommitId>,
    ref_change_id: ChangeId,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
}

fn explicit_branch_head_targets(
    state_rows: &[PreparedStateRow],
) -> Result<BTreeMap<String, ExplicitBranchHeadTarget>, LixError> {
    let mut targets = BTreeMap::new();
    for row in state_rows {
        if row.schema_key != BRANCH_REF_SCHEMA_KEY || !row.untracked {
            continue;
        }
        let branch_id = row.entity_pk.as_single_string_owned()?;
        let head_commit_id = row
            .snapshot
            .as_ref()
            .map(|snapshot| {
                let commit_id = snapshot
                    .value
                    .get("commit_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            format!(
                                "branch ref for branch '{branch_id}' is missing commit_id before current-state publication"
                            ),
                        )
                    })?;
                CommitId::parse_lix(commit_id, "current-state branch-head control target")
            })
            .transpose()?;
        let ref_change_id = row.change_id.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "explicit branch ref for branch '{branch_id}' is missing its public change id"
                ),
            )
        })?;
        let target = ExplicitBranchHeadTarget {
            head_commit_id,
            ref_change_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
        };
        if targets.insert(branch_id.clone(), target).is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "transaction contains multiple explicit branch-ref publications for branch '{branch_id}'"
                ),
            ));
        }
    }
    Ok(targets)
}

/// A destructive branch-ref change cannot preserve branch-local untracked
/// rows. Deletion would orphan their only serving scope and repointing would
/// attach them to unrelated tracked history. Assigning the already-published
/// head is not destructive, so it keeps local untracked state intact.
///
/// The observed branch-control token is published as an exact-byte CAS below,
/// so a concurrent untracked mutation makes this safety decision retry rather
/// than racing a deletion.
async fn reject_explicit_branch_ref_lifecycle_with_untracked_rows(
    read: &(impl StorageAdapterRead + ?Sized),
    state_rows: &[PreparedStateRow],
    engine_rows: &[EngineCurrentRow],
    observations: &BTreeMap<String, BranchHeadControlObservation>,
) -> Result<(), LixError> {
    let targets = explicit_branch_head_targets(state_rows)?;
    let current_state = TrackedHeadContext::new().reader(read);
    for (branch_id, target) in targets {
        let Some(existing) = observations
            .get(&branch_id)
            .and_then(|observation| observation.control)
        else {
            continue;
        };
        let destructive = target
            .head_commit_id
            .is_none_or(|head_commit_id| head_commit_id != existing.head_commit_id);
        if !destructive {
            continue;
        }
        let mut untracked_identities = current_state
            .scan_live_rows(
                &branch_id,
                existing,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        include_tombstones: true,
                        ..TrackedStateFilter::default()
                    },
                    read_columns: TrackedStateReadColumns::default(),
                    limit: None,
                },
            )
            .await?
            .into_iter()
            .filter(|row| row.untracked)
            .map(|row| TrackedStateKey {
                schema_key: row.schema_key,
                entity_pk: row.entity_pk,
                file_id: row.file_id,
            })
            .collect();
        apply_pending_untracked_identities(
            &mut untracked_identities,
            &branch_id,
            state_rows,
            engine_rows,
        );
        if !untracked_identities.is_empty() {
            return Err(branch_ref_with_untracked_rows_error(
                &branch_id,
                target.head_commit_id.is_none(),
            ));
        }
    }
    Ok(())
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

/// Validates explicit branch-ref targets against the canonical commit ledger.
///
/// A ref may point at a commit staged by this same transaction, but never at
/// an arbitrary UUID. The check is deliberately bounded to the explicit
/// targets and runs before any branch control is published.
async fn ensure_explicit_branch_ref_targets_exist(
    read: &(impl StorageAdapterRead + ?Sized),
    state_rows: &[PreparedStateRow],
    staged_commits: &BTreeMap<CommitId, StagedChangelogCommit>,
) -> Result<(), LixError> {
    let target_ids = explicit_branch_head_targets(state_rows)?
        .into_values()
        .filter_map(|target| target.head_commit_id)
        .filter(|commit_id| !staged_commits.contains_key(commit_id))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if target_ids.is_empty() {
        return Ok(());
    }

    let commits = ChangelogContext::new()
        .reader(read)
        .load_commits(ChangelogCommitLoadRequest {
            commit_ids: &target_ids,
            projection: ChangelogCommitProjection::Record,
        })
        .await?;
    for (commit_id, entry) in target_ids.into_iter().zip(commits.entries) {
        if entry.is_none() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("branch ref targets unknown commit '{commit_id}'"),
            ));
        }
    }
    Ok(())
}

/// Takes one coherent raw point batch for every control this materialization
/// can publish. The result is threaded through current-state generation selection and
/// final CAS staging, so the control plane adds exactly one control batch to
/// a commit rather than a head lookup plus a second token lookup.
async fn observe_branch_head_controls(
    read: &(impl StorageAdapterRead + ?Sized),
    tracked_roots: &[PendingTrackedRoot],
    state_rows: &[PreparedStateRow],
    engine_rows: &[EngineCurrentRow],
) -> Result<BTreeMap<String, BranchHeadControlObservation>, LixError> {
    let mut branch_ids = tracked_roots
        .iter()
        .map(|root| root.branch_id.clone())
        .collect::<BTreeSet<_>>();
    for row in state_rows {
        if row.schema_key == BRANCH_REF_SCHEMA_KEY && row.untracked {
            branch_ids.insert(row.entity_pk.as_single_string_owned()?);
        } else if row.untracked {
            branch_ids.insert(row.branch_id.clone());
        }
    }
    branch_ids.extend(engine_rows.iter().map(|row| row.branch_id.clone()));
    let branch_ids = branch_ids.into_iter().collect::<Vec<_>>();
    let observations = BranchHeadControlContext::new()
        .reader(read)
        .load_observed(&branch_ids)
        .await?;
    Ok(branch_ids.into_iter().zip(observations).collect())
}

async fn stage_tracked_roots(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_roots: &[PendingTrackedRoot],
    staged_commits: &BTreeMap<CommitId, StagedChangelogCommit>,
    insert_identities: &BTreeMap<PreparedStateRowIdentity, PreparedInsertIdentity>,
    force_root_fence: bool,
) -> Result<(), LixError> {
    let root_fence_ids = tracked_root_fence_ids(tracked_roots, force_root_fence);
    if root_fence_ids.is_empty() {
        return Ok(());
    }
    let tracked_state = TrackedStateContext::new();
    let mut tracked_writer = tracked_state.writer(read, writes);
    for root in tracked_roots_parent_first(tracked_roots)? {
        if !root_fence_ids.contains(&root.commit_id) {
            continue;
        }
        if let Some(parent_commit_id) = root.parent_commit_id
            && !root_fence_ids.contains(&parent_commit_id)
        {
            // Ordinary commits deliberately omit immutable roots. A
            // merge/checkpoint fence still needs a canonical first-parent
            // root, so reconstruct the cold chain in this same root writer
            // before staging the fence. Keeping one overlay makes the newly
            // staged ancestors visible to the fence without a second storage
            // transaction.
            tracked_writer
                .stage_missing_commit_root_chain(&parent_commit_id.to_string())
                .await?;
        }
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
        if state_row_indices.len() > staged.change_count {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "commit '{}' has {} tracked rows but only {} changelog changes",
                    root.commit_id,
                    state_row_indices.len(),
                    staged.change_count
                ),
            ));
        }
        // Normal entity batches are already in canonical primary-key order.
        // When they cover a substantial fraction of a parent root, stream the
        // parent/changes directly into canonical chunks instead of point
        // reading every key and materializing two more full-workload vectors.
        if !state_row_indices.is_empty()
            && staged.selected_change_refs.is_empty()
            && tracked_state_rows_are_strictly_sorted(state_rows, state_row_indices)
        {
            let commit_id_text = root.commit_id.to_string();
            let parent_commit_id_text = root.parent_commit_id.map(|id| id.to_string());
            let first_row = &state_rows[state_row_indices[0]];
            let first_mutation_key = encode_key_ref(TrackedStateKeyRef {
                schema_key: &first_row.schema_key,
                file_id: first_row.file_id.as_deref(),
                entity_pk: &first_row.entity_pk,
            });
            if tracked_writer
                .try_stage_bulk_parent_root_from_ordered_mutations(
                    &commit_id_text,
                    parent_commit_id_text.as_deref(),
                    state_row_indices.len(),
                    &first_mutation_key,
                    state_row_indices.iter().map(|&row_index| {
                        let row = &state_rows[row_index];
                        Ok(TrackedStateRootMutationRef {
                            delta: tracked_delta_from_state_row(row)?,
                            require_absence: tracked_row_requires_absence(row, insert_identities),
                        })
                    }),
                )
                .await?
                .is_some()
            {
                continue;
            }
        }
        let deltas = state_row_indices
            .iter()
            .map(|&row_index| tracked_delta_from_state_row(&state_rows[row_index]))
            .chain(staged.selected_change_refs.iter().map(|change_ref| {
                tracked_delta_from_selected_change_ref(change_ref, root.commit_id)
            }))
            .collect::<Result<Vec<_>, _>>()?;
        let absence_guards = if insert_identities.is_empty() {
            BTreeSet::new()
        } else {
            state_row_indices
                .iter()
                .filter_map(|&row_index| {
                    let row = &state_rows[row_index];
                    if row.snapshot.is_none() || row.untracked {
                        return None;
                    }
                    let insert = insert_identities.get(&PreparedStateRowIdentity::from(row))?;
                    if insert.untracked() {
                        return None;
                    }
                    Some(TrackedStateKey {
                        schema_key: row.schema_key.clone(),
                        file_id: row.file_id.clone(),
                        entity_pk: row.entity_pk.clone(),
                    })
                })
                .collect()
        };
        // Commit facts are canonical in changelog.commit and live-state derives
        // lix_commit rows from the commit graph. Keeping them out of this tree
        // also preserves the one-mutation path for ordinary singleton writes.
        let commit_id_text = root.commit_id.to_string();
        let parent_commit_id_text = root.parent_commit_id.map(|id| id.to_string());
        tracked_writer
            .stage_commit_root_with_absence_guards(
                &commit_id_text,
                parent_commit_id_text.as_deref(),
                deltas,
                &absence_guards,
            )
            .await?;
    }
    Ok(())
}

/// Immutable roots are a cold-path history structure. Keep them at topology
/// and checkpoint fences, plus any staged first-parent ancestors necessary to
/// build that fence in one atomic write set. Ordinary serial commits are
/// represented only by the changelog and the durable current-state projection.
fn tracked_root_fence_ids(
    tracked_roots: &[PendingTrackedRoot],
    force_all: bool,
) -> BTreeSet<CommitId> {
    if force_all {
        return tracked_roots.iter().map(|root| root.commit_id).collect();
    }
    BTreeSet::new()
}

fn tracked_state_rows_are_strictly_sorted(
    state_rows: &[PreparedStateRow],
    row_indices: &[RowIndex],
) -> bool {
    row_indices.windows(2).all(|pair| {
        let left = &state_rows[pair[0]];
        let right = &state_rows[pair[1]];
        left.schema_key
            .cmp(&right.schema_key)
            .then_with(|| left.file_id.cmp(&right.file_id))
            .then_with(|| left.entity_pk.cmp(&right.entity_pk))
            .is_lt()
    })
}

fn tracked_row_requires_absence(
    row: &PreparedStateRow,
    insert_identities: &BTreeMap<PreparedStateRowIdentity, PreparedInsertIdentity>,
) -> bool {
    if insert_identities.is_empty() {
        return false;
    }
    row.snapshot.is_some()
        && !row.untracked
        && insert_identities
            .get(&PreparedStateRowIdentity::from(row))
            .is_some_and(|insert| !insert.untracked())
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
/// Moving heads publish through the direct current-state branch-control plane after the
/// immutable commit facts are staged. They are not synthetic changelog
/// changes and do not become members of the commits they point at.
struct FinalizedCommitRows {
    commit_rows: Vec<FinalizedCommitRow>,
    tracked_roots: Vec<PendingTrackedRoot>,
}

struct FinalizedCommitRow {
    commit_id: CommitId,
    parent_commit_ids: Vec<CommitId>,
    created_at: LixTimestamp,
    change_id: ChangeId,
    selected_change_refs: Vec<StagedCommitChangeRef>,
}

struct PendingTrackedRoot {
    branch_id: String,
    commit_id: CommitId,
    parent_commit_id: Option<CommitId>,
    /// Metadata for the public synthesized `lix_branch_ref` row.
    ref_change_id: ChangeId,
    ref_updated_at: LixTimestamp,
}

async fn finalize_commit_rows(
    commit_change_refs_by_branch: BTreeMap<String, StagedCommitChangeRefs>,
    first_commit_parent_override_by_branch: BTreeMap<String, CommitId>,
    extra_commit_parents_by_branch: BTreeMap<String, Vec<CommitId>>,
    commit_parent_heads: &BTreeMap<String, Option<CommitId>>,
) -> Result<FinalizedCommitRows, LixError> {
    let mut commit_rows = Vec::new();
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
        let parent_commit_ids =
            if let Some(parent) = first_commit_parent_override_by_branch.get(&branch_id) {
                vec![*parent]
            } else {
                commit_parent_heads
                    .get(&branch_id)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("commit parent head was not resolved for branch '{branch_id}'"),
                        )
                    })?
                    .as_ref()
                    .copied()
                    .into_iter()
                    .collect::<Vec<_>>()
            };
        let parent_commit_ids = merge_parent_commit_ids(
            parent_commit_ids,
            extra_commit_parents_by_branch
                .get(&branch_id)
                .cloned()
                .unwrap_or_default(),
        );
        let parent_commit_id = parent_commit_ids.first().copied();
        commit_rows.push(FinalizedCommitRow {
            commit_id,
            parent_commit_ids: parent_commit_ids.clone(),
            created_at: timestamp,
            change_id: commit_change_id,
            selected_change_refs,
        });
        tracked_roots.push(PendingTrackedRoot {
            branch_id,
            commit_id,
            parent_commit_id,
            ref_change_id: branch_ref_change_id,
            ref_updated_at: timestamp,
        });
    }

    Ok(FinalizedCommitRows {
        commit_rows,
        tracked_roots,
    })
}

/// Resolves every branch touched by a prepared commit from the same coherent
/// read that validation and materialization use. Production callers require
/// non-global targets to exist; low-level materialization may opt out to
/// preserve its root-construction primitive.
pub(crate) async fn resolve_prepared_commit_parent_heads(
    branch_ctx: &BranchContext,
    read: &(impl StorageAdapterRead + ?Sized),
    prepared_writes: &PreparedWriteSet,
    require_existing_non_global_targets: bool,
) -> Result<BTreeMap<String, Option<CommitId>>, LixError> {
    let commit_parent_branch_ids = prepared_writes
        .commit_change_refs_by_branch
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut required_branch_ids = prepared_writes
        .state_rows
        .iter()
        .map(|row| row.branch_id.as_str())
        .chain(
            prepared_writes
                .file_data_writes
                .iter()
                .map(|write| write.branch_id.as_str()),
        )
        .chain(
            prepared_writes
                .first_commit_parent_override_by_branch
                .keys()
                .map(String::as_str),
        )
        .chain(
            prepared_writes
                .extra_commit_parents_by_branch
                .keys()
                .map(String::as_str),
        )
        .collect::<BTreeSet<_>>();
    required_branch_ids.extend(commit_parent_branch_ids.iter().copied());

    let branch_ref = branch_ctx.ref_reader(read);
    let mut parent_heads = BTreeMap::new();
    for branch_id in required_branch_ids {
        let head = branch_ref.load_head_commit_id(branch_id).await?;
        if require_existing_non_global_targets
            && branch_id != crate::GLOBAL_BRANCH_ID
            && head.is_none()
        {
            return Err(LixError::branch_not_found(
                branch_id.to_string(),
                "commit",
                "target",
            ));
        }
        if commit_parent_branch_ids.contains(&branch_id) {
            parent_heads.insert(branch_id.to_string(), head);
        }
    }
    Ok(parent_heads)
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
    use crate::branch::BranchContext;
    use crate::catalog::SchemaPlanId;
    use crate::changelog::ChangelogReader;
    use crate::live_state::{
        LiveStateContext, LiveStateExactBatchRequest, LiveStateExactRowRequest,
        LiveStateProjection, LiveStateRowRequest,
    };
    use crate::storage::{
        CommitResult, GetManyResult, KeyRange, PutBatch, ScanChunk, ScanOptions, SpaceId, Storage,
        StorageError, StorageRead, StorageWrite,
    };
    use crate::storage_adapter::{
        Memory, MemoryRead, MemoryWrite, StorageAdapter, StorageAdapterReadScope, StorageKey,
        StorageReadOptions, StorageSpace, StorageWriteOptions,
    };
    use crate::transaction::types::PreparedRowFacts;
    use crate::{GLOBAL_BRANCH_ID, NullableKeyFilter};

    fn ts(value: &str) -> LixTimestamp {
        LixTimestamp::expect_parse("timestamp", value)
    }

    const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
    const DETERMINISTIC_SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";
    // `tracked_state::storage` intentionally keeps this internal; this test
    // observes the durable space rather than reaching through that module.
    const TRACKED_STATE_TREE_CHUNK_SPACE_ID: SpaceId = SpaceId(0x0004_0001);
    const TRACKED_STATE_COMMIT_ROOT_SPACE_ID: SpaceId = SpaceId(0x0004_0004);
    const TRACKED_STATE_TREE_CHUNK_SPACE: StorageSpace = StorageSpace::new(
        TRACKED_STATE_TREE_CHUNK_SPACE_ID,
        "tracked_state.tree_chunk",
    );
    const TRACKED_STATE_COMMIT_ROOT_SPACE: StorageSpace = StorageSpace::new(
        TRACKED_STATE_COMMIT_ROOT_SPACE_ID,
        "tracked_state.commit_root",
    );
    // V11 has no tracked-head marker space. Keep the retired v10 ID here only
    // as a negative test sentinel: normal serving and staging must never read
    // it after the branch control became the publication authority.
    const V10_TRACKED_HEAD_MARKER_SPACE_ID: SpaceId = SpaceId(0x0004_0014);

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            TrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        )
    }

    #[derive(Default)]
    struct TrackedHeadReadCounts {
        branch_control_get_many_calls: AtomicUsize,
        v10_marker_get_many_calls: AtomicUsize,
        row_get_many_calls: AtomicUsize,
        row_scan_calls: AtomicUsize,
        tree_chunk_get_many_calls: AtomicUsize,
        tree_chunk_scan_calls: AtomicUsize,
        commit_root_get_many_calls: AtomicUsize,
        commit_root_scan_calls: AtomicUsize,
    }

    struct CountingTrackedHeadRead {
        inner: MemoryRead,
        counts: Arc<TrackedHeadReadCounts>,
    }

    impl StorageRead for CountingTrackedHeadRead {
        async fn get_many(
            &self,
            requests: &[crate::storage::GetManyRequest<'_>],
        ) -> Result<GetManyResult, StorageError> {
            for request in requests {
                let space = request.space;
                if space == crate::branch::BRANCH_HEAD_CONTROL_SPACE.id {
                    self.counts
                        .branch_control_get_many_calls
                        .fetch_add(1, Ordering::Relaxed);
                }
                if space == V10_TRACKED_HEAD_MARKER_SPACE_ID {
                    self.counts
                        .v10_marker_get_many_calls
                        .fetch_add(1, Ordering::Relaxed);
                }
                if space == crate::live_state::HOT_ROW_SPACE.id
                    || space == crate::live_state::HOT_FILE_SPACE.id
                {
                    self.counts
                        .row_get_many_calls
                        .fetch_add(1, Ordering::Relaxed);
                }
                if space == TRACKED_STATE_TREE_CHUNK_SPACE_ID {
                    self.counts
                        .tree_chunk_get_many_calls
                        .fetch_add(1, Ordering::Relaxed);
                }
                if space == TRACKED_STATE_COMMIT_ROOT_SPACE_ID {
                    self.counts
                        .commit_root_get_many_calls
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            self.inner.get_many(requests).await
        }

        async fn scan(
            &self,
            space: SpaceId,
            range: KeyRange,
            opts: ScanOptions,
        ) -> Result<ScanChunk, StorageError> {
            if space == crate::live_state::HOT_ROW_SPACE.id
                || space == crate::live_state::HOT_FILE_SPACE.id
            {
                self.counts.row_scan_calls.fetch_add(1, Ordering::Relaxed);
            }
            if space == TRACKED_STATE_TREE_CHUNK_SPACE_ID {
                self.counts
                    .tree_chunk_scan_calls
                    .fetch_add(1, Ordering::Relaxed);
            }
            if space == TRACKED_STATE_COMMIT_ROOT_SPACE_ID {
                self.counts
                    .commit_root_scan_calls
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.inner.scan(space, range, opts).await
        }
    }

    #[test]
    fn selected_change_refs_reject_overlap_with_normal_rows() {
        let row = tracked_global_row("normal-change");
        let error = validate_selected_change_refs(
            commit_id("test-uuid-1"),
            &[row],
            &[0],
            &[selected_change_ref("selected-change", "entity-1")],
        )
        .expect_err("selected ref must not duplicate a normal row identity");
        assert!(error.message.contains("duplicate change ref key"));

        let row = tracked_global_row("normal-change");
        let error = validate_selected_change_refs(
            commit_id("test-uuid-1"),
            &[row],
            &[0],
            &[selected_change_ref("normal-change", "other-entity")],
        )
        .expect_err("selected ref must not duplicate a normal row change id");
        assert!(error.message.contains("duplicate change ref '"));
    }

    #[tokio::test]
    async fn ordinary_tracked_commit_appends_changelog_without_materializing_root() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let state_rows = vec![tracked_global_row("change-1")];
        let (writes, _) = commit_prepared_writes(
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
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("commit should flush staged rows");
        assert!(
            !writes.has_mutations_in_space(TRACKED_STATE_TREE_CHUNK_SPACE),
            "an ordinary tracked commit must not write immutable tree chunks"
        );
        assert!(
            !writes.has_mutations_in_space(TRACKED_STATE_COMMIT_ROOT_SPACE),
            "an ordinary tracked commit must not write commit-root metadata"
        );
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
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["lix_commit".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("rootless commit history should replay");
        assert!(
            commit_rows.is_empty(),
            "commit rows are derived from changelog.commit, not stored in tracked roots"
        );
        let derived_commit_rows = live_state_context()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_rows(&crate::live_state::LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    schema_keys: vec!["lix_commit".to_string()],
                    branch_ids: vec![GLOBAL_BRANCH_ID.to_string()],
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("derived commit rows should scan");
        assert!(
            derived_commit_rows
                .iter()
                .any(|row| row.change_id == Some(record.change_id)),
            "live state should derive the commit row from changelog.commit"
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
    async fn direct_branch_ref_update_rejects_unknown_commit_target() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let branch_id = "branch-ref-unknown-target";
        crate::test_support::seed_branch_head(storage.clone(), branch_id, "branch-ref-known-head")
            .await;

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("unknown-target branch-ref read should open");
        let error = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            prepared_direct_branch_ref_update(
                branch_id,
                "unknown-branch-ref-target",
                "unknown-direct-branch-ref-change",
            ),
        )
        .await
        .expect_err("direct branch ref must not publish an unknown commit target");

        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(error.message.contains("branch ref targets unknown commit"));
    }

    #[tokio::test]
    async fn direct_branch_ref_delete_rejects_pending_local_untracked_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let branch_id = "branch-ref-pending-untracked";
        crate::test_support::seed_branch_head(storage.clone(), branch_id, "branch-ref-head").await;

        let mut branch_ref_delete = untracked_global_row("delete-branch-ref");
        branch_ref_delete.entity_pk = EntityPk::single(branch_id);
        branch_ref_delete.schema_key = BRANCH_REF_SCHEMA_KEY.to_string();
        branch_ref_delete.snapshot = None;

        let mut pending_untracked = tracked_branch_row(branch_id, "pending-untracked-row");
        pending_untracked.untracked = true;
        pending_untracked.commit_id = None;
        pending_untracked.global = false;

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("branch-ref delete read should open");
        let error = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![branch_ref_delete, pending_untracked],
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect_err("branch-ref delete must reject a pending local untracked row");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("branch-local untracked"));
    }

    #[tokio::test]
    async fn direct_branch_ref_delete_requires_final_untracked_state_to_be_empty() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let branch_id = "branch-ref-persisted-untracked";
        crate::test_support::seed_branch_head(storage.clone(), branch_id, "branch-ref-head").await;

        let mut persisted_untracked = tracked_branch_row(branch_id, "persisted-untracked-row");
        persisted_untracked.untracked = true;
        persisted_untracked.commit_id = None;
        persisted_untracked.global = false;
        let mut untracked_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("untracked seed read should open");
        let (untracked_writes, untracked_preconditions) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut untracked_read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![persisted_untracked],
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("local untracked row should stage");
        storage
            .commit_write_set(
                untracked_writes,
                StorageWriteOptions {
                    preconditions: untracked_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("local untracked row should persist");

        let mut branch_ref_delete = untracked_global_row("delete-persisted-branch-ref");
        branch_ref_delete.entity_pk = EntityPk::single(branch_id);
        branch_ref_delete.schema_key = BRANCH_REF_SCHEMA_KEY.to_string();
        branch_ref_delete.snapshot = None;
        let mut delete_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("persisted branch-ref delete read should open");
        let error = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut delete_read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![branch_ref_delete],
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect_err("branch-ref delete must reject persisted local untracked state");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("branch-local untracked"));

        // The same atomic publication is valid once it physically removes
        // the branch-local untracked member. Branch deletion reasons about
        // final current state, not an intermediate overlay.
        let mut untracked_delete = tracked_branch_row(branch_id, "delete-persisted-untracked");
        untracked_delete.untracked = true;
        untracked_delete.commit_id = None;
        untracked_delete.global = false;
        untracked_delete.snapshot = None;
        let mut cleanup_branch_ref_delete =
            untracked_global_row("delete-persisted-branch-ref-after-cleanup");
        cleanup_branch_ref_delete.entity_pk = EntityPk::single(branch_id);
        cleanup_branch_ref_delete.schema_key = BRANCH_REF_SCHEMA_KEY.to_string();
        cleanup_branch_ref_delete.snapshot = None;
        let mut cleanup_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("atomic cleanup read should open");
        let (cleanup_writes, cleanup_preconditions) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut cleanup_read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![cleanup_branch_ref_delete, untracked_delete],
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("branch-ref delete should stage after atomic untracked cleanup");
        storage
            .commit_write_set(
                cleanup_writes,
                StorageWriteOptions {
                    preconditions: cleanup_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("branch-ref delete should commit after atomic untracked cleanup");
        assert!(
            BranchHeadControlContext::new()
                .reader(
                    storage
                        .begin_read(StorageReadOptions::default())
                        .await
                        .expect("branch-control verification read should open"),
                )
                .load(branch_id)
                .await
                .expect("branch control should load")
                .is_none(),
            "branch deletion must remove its current-state control"
        );
    }

    #[tokio::test]
    async fn direct_branch_ref_may_target_commit_staged_by_same_transaction() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let target_commit = "same-write-branch-target";
        let target_branch = "same-write-branch";
        let row_change = "same-write-branch-row-change";
        let mut tracked_row = tracked_global_row(row_change);
        tracked_row.commit_id = Some(commit_id(target_commit));
        let prepared = PreparedWriteSet {
            insert_identities: BTreeMap::new(),
            state_rows: vec![
                tracked_row,
                direct_branch_ref_row(target_branch, target_commit, "same-write-branch-ref-change"),
            ],
            commit_change_refs_by_branch: BTreeMap::from([(
                GLOBAL_BRANCH_ID.to_string(),
                change_refs_with(
                    [row_change],
                    target_commit,
                    "same-write-branch-commit-change",
                    "same-write-global-ref-change",
                ),
            )]),
            first_commit_parent_override_by_branch: BTreeMap::new(),
            checkpoint_publications: Vec::new(),
            extra_commit_parents_by_branch: BTreeMap::new(),
            file_data_writes: Vec::new(),
        };
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("same-write branch-ref read should open");
        let (writes, preconditions) =
            commit_prepared_writes(&binary_cas, &branch_ctx, None, &mut read, prepared)
                .await
                .expect("branch ref may target a commit staged by the same transaction");
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("same-write branch ref should commit");

        let head = branch_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("same-write branch-ref verification read should open"),
            )
            .load_head_commit_id(target_branch)
            .await
            .expect("same-write branch head should load");
        assert_eq!(head, Some(commit_id(target_commit)));
    }

    #[tokio::test]
    async fn direct_branch_ref_update_rejects_a_stale_control_token() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let branch_id = "branch-ref-race";
        crate::test_support::seed_branch_head(
            storage.clone(),
            branch_id,
            "branch-ref-initial-head",
        )
        .await;
        seed_empty_commit(&storage, "stale-branch-ref-target").await;
        seed_empty_commit(&storage, "winner-branch-ref-target").await;

        let mut stale_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("stale branch-ref read should open");
        let (stale_writes, stale_preconditions) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut stale_read,
            prepared_direct_branch_ref_update(
                branch_id,
                "stale-branch-ref-target",
                "stale-direct-branch-ref-change",
            ),
        )
        .await
        .expect("stale direct branch-ref update should stage");
        assert!(stale_preconditions.iter().any(|precondition| {
            matches!(
                precondition,
                StoragePrecondition::KeyValueEquals { space, .. }
                    if *space == crate::branch::BRANCH_HEAD_CONTROL_SPACE.id
            )
        }));

        let mut winner_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("winner branch-ref read should open");
        let (winner_writes, winner_preconditions) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut winner_read,
            prepared_direct_branch_ref_update(
                branch_id,
                "winner-branch-ref-target",
                "winner-direct-branch-ref-change",
            ),
        )
        .await
        .expect("winner direct branch-ref update should stage");
        storage
            .commit_write_set(
                winner_writes,
                StorageWriteOptions {
                    preconditions: winner_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("winner direct branch-ref update should commit");

        let error = storage
            .commit_write_set(
                stale_writes,
                StorageWriteOptions {
                    preconditions: stale_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect_err("stale direct branch-ref update must not overwrite the winner");
        assert!(matches!(
            error,
            crate::storage_adapter::StorageWriteSetError::Storage(
                StorageError::PreconditionFailed(_)
            )
        ));

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("branch-ref verification read should open");
        let control = BranchHeadControlContext::new()
            .reader(read)
            .load(branch_id)
            .await
            .expect("winner branch control should load")
            .expect("winner branch control should remain present");
        assert_eq!(
            control.ref_change_id,
            change_id("winner-direct-branch-ref-change"),
            "the stale write must not replace the winner's branch control"
        );
        let head = branch_ctx
            .ref_reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("branch-ref head verification read should open"),
            )
            .load_head_commit_id(branch_id)
            .await
            .expect("winner branch-ref head should load");
        assert_eq!(head, Some(commit_id("winner-branch-ref-target")));
    }

    #[tokio::test]
    async fn normal_commit_rejects_stale_engine_branch_ref_publication() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();

        let mut stale_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("stale normal-commit read should open");
        let (stale_writes, stale_preconditions) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut stale_read,
            prepared_normal_global_commit(
                "stale-normal-change",
                "stale-normal-commit",
                "stale-normal-commit-change",
                "stale-normal-branch-ref-change",
            ),
        )
        .await
        .expect("stale normal commit should stage");
        assert!(stale_preconditions.iter().any(|precondition| {
            matches!(
                precondition,
                StoragePrecondition::KeyAbsent { space, .. }
                    if *space == crate::branch::BRANCH_HEAD_CONTROL_SPACE.id
            )
        }));

        let mut winner_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("winner normal-commit read should open");
        let (winner_writes, winner_preconditions) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut winner_read,
            prepared_normal_global_commit(
                "winner-normal-change",
                "winner-normal-commit",
                "winner-normal-commit-change",
                "winner-normal-branch-ref-change",
            ),
        )
        .await
        .expect("winner normal commit should stage");
        storage
            .commit_write_set(
                winner_writes,
                StorageWriteOptions {
                    preconditions: winner_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("winner normal commit should commit");

        let error = storage
            .commit_write_set(
                stale_writes,
                StorageWriteOptions {
                    preconditions: stale_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect_err("stale normal commit must not publish a stale branch ref");
        assert!(matches!(
            error,
            crate::storage_adapter::StorageWriteSetError::Storage(
                StorageError::PreconditionFailed(_)
            )
        ));

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("normal branch-ref verification read should open");
        let control = BranchHeadControlContext::new()
            .reader(read)
            .load(GLOBAL_BRANCH_ID)
            .await
            .expect("winner direct branch control should load")
            .expect("winner direct branch control should remain present");
        assert_eq!(
            control.ref_change_id,
            change_id("winner-normal-branch-ref-change"),
            "the stale commit must not replace the winner's public branch-ref metadata"
        );
    }

    #[tokio::test]
    async fn rootless_serial_history_replays_scan_and_diff() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();

        let mut first = tracked_global_row("rootless-first-change");
        first.commit_id = Some(commit_id("rootless-first-commit"));
        first.created_at = ts("2026-01-01T00:00:00Z");
        first.updated_at = first.created_at;
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("first commit read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![first],
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs_with(
                        ["rootless-first-change"],
                        "rootless-first-commit",
                        "rootless-first-commit-change",
                        "rootless-first-branch-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("first rootless commit should stage");
        assert!(
            !writes.has_mutations_in_space(TRACKED_STATE_TREE_CHUNK_SPACE)
                && !writes.has_mutations_in_space(TRACKED_STATE_COMMIT_ROOT_SPACE),
            "first ordinary commit must remain rootless"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("first rootless commit should persist");

        let mut second = tracked_global_row("rootless-second-change");
        second.commit_id = Some(commit_id("rootless-second-commit"));
        second.created_at = ts("2026-01-02T00:00:00Z");
        second.updated_at = second.created_at;
        second.snapshot = Some(
            crate::transaction::types::stage_json_from_value(
                crate::transaction::types::TransactionJson::from_value_for_test(
                    serde_json::json!({ "value": 2 }),
                ),
                "second rootless tracked row snapshot",
            )
            .expect("second snapshot should stage"),
        );
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("second commit read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![second],
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs_with(
                        ["rootless-second-change"],
                        "rootless-second-commit",
                        "rootless-second-commit-change",
                        "rootless-second-branch-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("second rootless commit should stage");
        assert!(
            !writes.has_mutations_in_space(TRACKED_STATE_TREE_CHUNK_SPACE)
                && !writes.has_mutations_in_space(TRACKED_STATE_COMMIT_ROOT_SPACE),
            "serial ordinary commit must remain rootless"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("second rootless commit should persist");

        let mut third = tracked_global_row("rootless-third-change");
        third.commit_id = Some(commit_id("rootless-third-commit"));
        third.created_at = ts("2026-01-03T00:00:00Z");
        third.updated_at = third.created_at;
        third.snapshot = Some(
            crate::transaction::types::stage_json_from_value(
                crate::transaction::types::TransactionJson::from_value_for_test(
                    serde_json::json!({ "value": 3 }),
                ),
                "third rootless tracked row snapshot",
            )
            .expect("third snapshot should stage"),
        );
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("third commit read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![third],
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs_with(
                        ["rootless-third-change"],
                        "rootless-third-commit",
                        "rootless-third-commit-change",
                        "rootless-third-branch-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("third rootless commit should stage");
        assert!(
            !writes.has_mutations_in_space(TRACKED_STATE_TREE_CHUNK_SPACE)
                && !writes.has_mutations_in_space(TRACKED_STATE_COMMIT_ROOT_SPACE),
            "serial ordinary commit must remain rootless"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("third rootless commit should persist");

        let mut deleted = tracked_global_row("rootless-delete-change");
        deleted.commit_id = Some(commit_id("rootless-delete-commit"));
        deleted.created_at = ts("2026-01-04T00:00:00Z");
        deleted.updated_at = deleted.created_at;
        deleted.snapshot = None;
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("delete commit read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![deleted],
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs_with(
                        ["rootless-delete-change"],
                        "rootless-delete-commit",
                        "rootless-delete-commit-change",
                        "rootless-delete-branch-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("rootless delete commit should stage");
        assert!(
            !writes.has_mutations_in_space(TRACKED_STATE_TREE_CHUNK_SPACE)
                && !writes.has_mutations_in_space(TRACKED_STATE_COMMIT_ROOT_SPACE),
            "serial delete commit must remain rootless"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("rootless delete commit should persist");

        let mut reader = TrackedStateContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("historical read should open"),
        );
        let request = TrackedStateScanRequest::default();
        let first_rows = reader
            .scan_rows_at_commit(&commit_id_text("rootless-first-commit"), &request)
            .await
            .expect("first rootless commit should replay");
        assert!(matches!(
            first_rows.as_slice(),
            [row] if row.change_id == change_id("rootless-first-change")
                && row.snapshot_content.as_deref() == Some("{\"value\":1}")
        ));
        let second_rows = reader
            .scan_rows_at_commit(&commit_id_text("rootless-second-commit"), &request)
            .await
            .expect("second rootless commit should replay");
        assert!(matches!(
            second_rows.as_slice(),
            [row] if row.change_id == change_id("rootless-second-change")
                && row.created_at == "2026-01-01T00:00:00.000Z"
                && row.snapshot_content.as_deref() == Some("{\"value\":2}")
        ));
        let third_rows = reader
            .scan_rows_at_commit(&commit_id_text("rootless-third-commit"), &request)
            .await
            .expect("third rootless commit should replay");
        assert!(matches!(
            third_rows.as_slice(),
            [row] if row.change_id == change_id("rootless-third-change")
                && row.created_at == "2026-01-01T00:00:00.000Z"
                && row.snapshot_content.as_deref() == Some("{\"value\":3}")
        ));
        let diff = reader
            .diff_commits(
                &commit_id_text("rootless-first-commit"),
                &commit_id_text("rootless-third-commit"),
                &crate::tracked_state::TrackedStateDiffRequest::default(),
            )
            .await
            .expect("rootless commits should diff from replayed state");
        assert!(matches!(
            diff.entries.as_slice(),
            [entry] if entry.kind == crate::tracked_state::TrackedStateDiffKind::Modified
                && entry.after.as_ref().map(|row| row.change_id)
                    == Some(change_id("rootless-third-change"))
        ));
        let reverse_diff = reader
            .diff_commits(
                &commit_id_text("rootless-third-commit"),
                &commit_id_text("rootless-first-commit"),
                &crate::tracked_state::TrackedStateDiffRequest::default(),
            )
            .await
            .expect("reverse rootless commits should diff from replayed state");
        assert!(matches!(
            reverse_diff.entries.as_slice(),
            [entry] if entry.kind == crate::tracked_state::TrackedStateDiffKind::Modified
                && entry.before.as_ref().map(|row| row.change_id)
                    == Some(change_id("rootless-third-change"))
                && entry.after.as_ref().map(|row| row.change_id)
                    == Some(change_id("rootless-first-change"))
        ));
        let deleted_rows = reader
            .scan_rows_at_commit(&commit_id_text("rootless-delete-commit"), &request)
            .await
            .expect("delete rootless commit should replay");
        assert!(deleted_rows.is_empty());
        let delete_diff = reader
            .diff_commits(
                &commit_id_text("rootless-first-commit"),
                &commit_id_text("rootless-delete-commit"),
                &crate::tracked_state::TrackedStateDiffRequest::default(),
            )
            .await
            .expect("rootless delete commits should diff from replayed state");
        assert!(
            matches!(
                delete_diff.entries.as_slice(),
                [entry] if entry.kind == crate::tracked_state::TrackedStateDiffKind::Removed
                    && entry.before.as_ref().map(|row| row.change_id)
                        == Some(change_id("rootless-first-change"))
                    && entry.after.as_ref().is_some_and(|row|
                        row.deleted && row.change_id == change_id("rootless-delete-change")
                    )
            ),
            "unexpected rootless delete diff: {delete_diff:#?}"
        );
        let reverse_delete_diff = reader
            .diff_commits(
                &commit_id_text("rootless-delete-commit"),
                &commit_id_text("rootless-first-commit"),
                &crate::tracked_state::TrackedStateDiffRequest::default(),
            )
            .await
            .expect("reverse rootless delete commits should diff from replayed state");
        assert!(matches!(
            reverse_delete_diff.entries.as_slice(),
            [entry] if entry.kind == crate::tracked_state::TrackedStateDiffKind::Added
                && entry.before.as_ref().is_some_and(|row|
                    row.deleted && row.change_id == change_id("rootless-delete-change")
                )
                && entry.after.as_ref().map(|row| row.change_id)
                    == Some(change_id("rootless-first-change"))
        ));
    }

    #[tokio::test]
    async fn selected_reference_commit_stays_rootless_and_replays_first_parent() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();

        let mut normal = tracked_global_row("fence-normal-change");
        normal.commit_id = Some(commit_id("fence-normal-commit"));
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("normal commit read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![normal],
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs_with(
                        ["fence-normal-change"],
                        "fence-normal-commit",
                        "fence-normal-commit-change",
                        "fence-normal-branch-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("normal rootless commit should stage");
        assert!(
            !writes.has_mutations_in_space(TRACKED_STATE_COMMIT_ROOT_SPACE),
            "ordinary parent must not materialize a root"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("normal rootless commit should persist");

        let mut fence_refs = change_refs_with(
            [],
            "fence-commit",
            "fence-commit-change",
            "fence-branch-ref-change",
        );
        fence_refs.add_selected_change_ref(StagedCommitChangeRef {
            schema_key: "test_schema".to_string(),
            file_id: None,
            entity_pk: EntityPk::single("entity-1"),
            change_id: change_id("fence-normal-change"),
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
        });
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("fence commit read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: Vec::new(),
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    fence_refs,
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("selected-reference commit should stage");
        assert!(
            !writes.has_mutations_in_space(TRACKED_STATE_TREE_CHUNK_SPACE)
                && !writes.has_mutations_in_space(TRACKED_STATE_COMMIT_ROOT_SPACE),
            "absolute selected-reference deltas must keep the commit rootless"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("fence should persist atomically");

        let rows = TrackedStateContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("fence read should open"),
            )
            .scan_rows_at_commit(
                &commit_id_text("fence-commit"),
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("rootless selected-reference commit should replay history");
        assert!(matches!(
            rows.as_slice(),
            [row] if row.change_id == change_id("fence-normal-change")
        ));
    }

    #[tokio::test]
    async fn normal_tracked_commit_live_reads_select_head_projection() {
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("commit read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![tracked_global_row("tracked-head-change")],
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs(["tracked-head-change"]),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("normal tracked commit should stage");
        assert!(
            !writes.has_mutations_in_space(TRACKED_STATE_TREE_CHUNK_SPACE),
            "an ordinary tracked commit must not write immutable tree chunks"
        );
        assert!(
            !writes.has_mutations_in_space(TRACKED_STATE_COMMIT_ROOT_SPACE),
            "an ordinary tracked commit must not write commit-root metadata"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("normal tracked commit should persist");

        let counts = Arc::new(TrackedHeadReadCounts::default());
        let scanned = live_state_context()
            .reader(StorageAdapterReadScope::new(CountingTrackedHeadRead {
                inner: memory
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("counted scan read should open"),
                counts: Arc::clone(&counts),
            }))
            .scan_rows(&crate::live_state::LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    branch_ids: vec![GLOBAL_BRANCH_ID.to_string()],
                    schema_keys: vec!["test_schema".to_string()],
                    untracked: Some(false),
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("tracked scan should succeed");
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].change_id, Some(change_id("tracked-head-change")));
        assert!(
            counts.branch_control_get_many_calls.load(Ordering::Relaxed) > 0,
            "the read must load the branch control before serving tracked rows"
        );
        assert_eq!(
            counts.v10_marker_get_many_calls.load(Ordering::Relaxed),
            0,
            "v11 serving must not read the retired tracked-head marker"
        );
        assert!(
            counts.row_scan_calls.load(Ordering::Relaxed) > 0,
            "the normal tracked scan must range-scan the head projection"
        );
        assert_eq!(
            counts.tree_chunk_get_many_calls.load(Ordering::Relaxed),
            0,
            "a current head projection must avoid immutable-tree chunk reads"
        );
        assert_eq!(
            counts.tree_chunk_scan_calls.load(Ordering::Relaxed),
            0,
            "a current head projection must avoid immutable-tree chunk scans"
        );
        assert_eq!(
            counts.commit_root_get_many_calls.load(Ordering::Relaxed),
            0,
            "a current head projection must not first look up root metadata"
        );
        assert_eq!(
            counts.commit_root_scan_calls.load(Ordering::Relaxed),
            0,
            "a current head projection must not scan root metadata"
        );

        let loaded = live_state_context()
            .reader(StorageAdapterReadScope::new(CountingTrackedHeadRead {
                inner: memory
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("counted point read should open"),
                counts: Arc::clone(&counts),
            }))
            .load_row(&live_state_request())
            .await
            .expect("tracked point read should succeed")
            .expect("tracked row should be visible");
        assert_eq!(loaded.change_id, Some(change_id("tracked-head-change")));
        assert!(
            counts.row_get_many_calls.load(Ordering::Relaxed) > 0,
            "the exact tracked lookup must point-read the head projection"
        );

        let exact_rows = live_state_context()
            .reader(StorageAdapterReadScope::new(CountingTrackedHeadRead {
                inner: memory
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("counted exact batch read should open"),
                counts: Arc::clone(&counts),
            }))
            .load_exact_rows(&LiveStateExactBatchRequest {
                rows: vec![LiveStateExactRowRequest {
                    schema_key: "test_schema".to_string(),
                    branch_id: GLOBAL_BRANCH_ID.to_string(),
                    entity_pk: EntityPk::single("entity-1"),
                    file_id: None,
                }],
                projection: LiveStateProjection::default(),
                untracked: Some(false),
                include_tombstones: false,
            })
            .await
            .expect("exact tracked batch should succeed");
        assert!(matches!(
            exact_rows.as_slice(),
            [Some(row)] if row.change_id == Some(change_id("tracked-head-change"))
        ));
        assert_eq!(
            counts.tree_chunk_get_many_calls.load(Ordering::Relaxed),
            0,
            "neither serving read may fall back to immutable-tree chunks"
        );
        assert_eq!(
            counts.tree_chunk_scan_calls.load(Ordering::Relaxed),
            0,
            "neither serving read may scan immutable-tree chunks"
        );
        assert_eq!(
            counts.commit_root_get_many_calls.load(Ordering::Relaxed),
            0,
            "neither serving read may look up root metadata"
        );
        assert_eq!(
            counts.commit_root_scan_calls.load(Ordering::Relaxed),
            0,
            "neither serving read may scan root metadata"
        );
    }

    #[tokio::test]
    async fn serial_local_commit_reads_branch_control_for_parent_and_publication() {
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();

        let mut first = tracked_branch_row("branch-a", "first-local-change");
        first.commit_id = Some(commit_id("first-local-commit"));
        let mut first_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("first commit read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut first_read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![first],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "branch-a".to_string(),
                    change_refs_with(
                        ["first-local-change"],
                        "first-local-commit",
                        "first-local-commit-change",
                        "first-local-branch-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("first local commit should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("first local commit should persist");

        let counts = Arc::new(TrackedHeadReadCounts::default());
        let mut second_read = StorageAdapterReadScope::new(CountingTrackedHeadRead {
            inner: memory
                .begin_read(StorageReadOptions::default())
                .await
                .expect("second counted read should open"),
            counts: Arc::clone(&counts),
        });
        let mut second = tracked_branch_row("branch-a", "second-local-change");
        second.commit_id = Some(commit_id("second-local-commit"));
        let (_writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut second_read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![second],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "branch-a".to_string(),
                    change_refs_with(
                        ["second-local-change"],
                        "second-local-commit",
                        "second-local-commit-change",
                        "second-local-branch-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("second local commit should stage");

        assert_eq!(
            counts.branch_control_get_many_calls.load(Ordering::Relaxed),
            2,
            "serial tracked-head staging must read the control once for the parent and once for its CAS publication"
        );
        assert_eq!(
            counts.v10_marker_get_many_calls.load(Ordering::Relaxed),
            0,
            "v11 staging must not read the retired tracked-head marker"
        );
    }

    #[tokio::test]
    async fn stale_working_diff_epoch_is_not_promoted_into_the_next_head() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let first_commit = commit_id("epoch-first-commit");

        let mut first = tracked_branch_row("branch-a", "epoch-first-change");
        first.commit_id = Some(first_commit);
        let mut first_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open first commit read");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut first_read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![first],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "branch-a".to_string(),
                    change_refs_with(
                        ["epoch-first-change"],
                        "epoch-first-commit",
                        "epoch-first-commit-change",
                        "epoch-first-branch-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("first commit should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("first commit should persist");

        // This deliberately names the right serving generation but a
        // checkpoint that the marker never bound. It models a stale or
        // corrupted auxiliary epoch; the next serial commit must not turn it
        // into authoritative visibility.
        let mut stale_epoch_writes = StorageWriteSet::new();
        stage_tracked_working_diff_epoch(
            &mut stale_epoch_writes,
            "branch-a",
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: commit_id("wrong-checkpoint"),
                generation: first_commit,
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )
        .expect("stage stale epoch");
        storage
            .commit_write_set(stale_epoch_writes, StorageWriteOptions::default())
            .await
            .expect("persist stale epoch");

        let second_commit = commit_id("epoch-second-commit");
        let mut second = tracked_branch_row("branch-a", "epoch-second-change");
        second.commit_id = Some(second_commit);
        let mut second_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open second commit read");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut second_read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![second],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "branch-a".to_string(),
                    change_refs_with(
                        ["epoch-second-change"],
                        "epoch-second-commit",
                        "epoch-second-commit-change",
                        "epoch-second-branch-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("second commit should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("second commit should persist");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open direct-diff verification read");
        let control = BranchHeadControlContext::new()
            .reader(&read)
            .load("branch-a")
            .await
            .expect("load branch control")
            .expect("branch control must exist");
        assert_eq!(control.head_commit_id, second_commit);
        assert!(
            TrackedHeadContext::new()
                .reader(read)
                .working_diff_for_control(
                    "branch-a",
                    control,
                    &crate::tracked_state::TrackedStateDiffRequest::default(),
                )
                .await
                .expect("stale epoch must select fallback, not error")
                .is_none(),
            "a stale epoch must not be rebound by a later serial commit"
        );
    }

    #[tokio::test]
    async fn normal_head_projections_preserve_global_fallback_branch_override_and_tombstones() {
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();

        let mut global_override = tracked_global_row("global-override-change");
        global_override.commit_id = Some(commit_id("global-head"));
        let mut global_fallback = tracked_global_row("global-fallback-change");
        global_fallback.entity_pk = EntityPk::single("entity-2");
        global_fallback.commit_id = Some(commit_id("global-head"));
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("global commit read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![global_override, global_fallback],
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs_with(
                        ["global-override-change", "global-fallback-change"],
                        "global-head",
                        "global-head-commit-change",
                        "global-head-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("global tracked commit should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("global tracked commit should persist");

        let mut branch_override = tracked_branch_row("branch-a", "branch-override-change");
        branch_override.commit_id = Some(commit_id("branch-head"));
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("branch commit read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![branch_override],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "branch-a".to_string(),
                    change_refs_with(
                        ["branch-override-change"],
                        "branch-head",
                        "branch-head-commit-change",
                        "branch-head-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("branch tracked commit should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("branch tracked commit should persist");

        let control_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("control read should open");
        let controls = BranchHeadControlContext::new()
            .reader(&control_read)
            .load_many(&[GLOBAL_BRANCH_ID.to_string(), "branch-a".to_string()])
            .await
            .expect("branch controls should load");
        let global_control = controls[0].expect("global control must exist");
        assert_eq!(global_control.head_commit_id, commit_id("global-head"));
        assert_eq!(global_control.working_diff_checkpoint_commit_id, None);
        let branch_control = controls[1].expect("branch control must exist");
        assert_eq!(branch_control.head_commit_id, commit_id("branch-head"));
        assert_eq!(branch_control.working_diff_checkpoint_commit_id, None);

        let counts = Arc::new(TrackedHeadReadCounts::default());
        let scanned = live_state_context()
            .reader(StorageAdapterReadScope::new(CountingTrackedHeadRead {
                inner: memory
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("counted branch scan read should open"),
                counts: Arc::clone(&counts),
            }))
            .scan_rows(&crate::live_state::LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    branch_ids: vec!["branch-a".to_string()],
                    schema_keys: vec!["test_schema".to_string()],
                    untracked: Some(false),
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("branch tracked scan should succeed");
        assert_eq!(
            scanned.len(),
            2,
            "branch scan should retain global fallback"
        );
        let branch_row = scanned
            .iter()
            .find(|row| row.entity_pk == EntityPk::single("entity-1"))
            .expect("branch override should be visible");
        assert_eq!(
            branch_row.change_id,
            Some(change_id("branch-override-change"))
        );
        assert_eq!(branch_row.branch_id.as_ref(), "branch-a");
        assert!(!branch_row.global);
        let fallback_row = scanned
            .iter()
            .find(|row| row.entity_pk == EntityPk::single("entity-2"))
            .expect("global fallback should be visible");
        assert_eq!(
            fallback_row.change_id,
            Some(change_id("global-fallback-change"))
        );
        assert_eq!(fallback_row.branch_id.as_ref(), "branch-a");
        assert!(fallback_row.global);

        let mut branch_tombstone = tracked_branch_row("branch-a", "branch-tombstone-change");
        branch_tombstone.entity_pk = EntityPk::single("entity-2");
        branch_tombstone.snapshot = None;
        branch_tombstone.commit_id = Some(commit_id("branch-tombstone-head"));
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("branch tombstone read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![branch_tombstone],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "branch-a".to_string(),
                    change_refs_with(
                        ["branch-tombstone-change"],
                        "branch-tombstone-head",
                        "branch-tombstone-head-commit-change",
                        "branch-tombstone-head-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("branch tombstone commit should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("branch tombstone commit should persist");

        let scanned = live_state_context()
            .reader(StorageAdapterReadScope::new(CountingTrackedHeadRead {
                inner: memory
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("counted tombstone scan read should open"),
                counts: Arc::clone(&counts),
            }))
            .scan_rows(&crate::live_state::LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    branch_ids: vec!["branch-a".to_string()],
                    schema_keys: vec!["test_schema".to_string()],
                    untracked: Some(false),
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("branch tombstone scan should succeed");
        assert_eq!(
            scanned.len(),
            1,
            "the branch tombstone must hide the global fallback row"
        );
        assert_eq!(scanned[0].entity_pk, EntityPk::single("entity-1"));
        assert_eq!(
            scanned[0].change_id,
            Some(change_id("branch-override-change"))
        );
        assert!(
            counts.branch_control_get_many_calls.load(Ordering::Relaxed) >= 2,
            "each branch scan must batch-load the branch and global controls"
        );
        assert_eq!(
            counts.v10_marker_get_many_calls.load(Ordering::Relaxed),
            0,
            "v11 scans must not read the retired tracked-head marker"
        );
        assert!(
            counts.row_scan_calls.load(Ordering::Relaxed) >= 4,
            "each branch scan must range-scan both current head projections"
        );
        assert_eq!(
            counts.tree_chunk_get_many_calls.load(Ordering::Relaxed),
            0,
            "current global and branch projections must avoid immutable-tree point reads"
        );
        assert_eq!(
            counts.tree_chunk_scan_calls.load(Ordering::Relaxed),
            0,
            "current global and branch projections must avoid immutable-tree scans"
        );
        assert_eq!(
            counts.commit_root_get_many_calls.load(Ordering::Relaxed),
            0,
            "current global and branch projections must not look up root metadata"
        );
        assert_eq!(
            counts.commit_root_scan_calls.load(Ordering::Relaxed),
            0,
            "current global and branch projections must not scan root metadata"
        );
    }

    #[tokio::test]
    async fn stage_changelog_commits_orders_staged_parents_before_children() {
        let storage = StorageAdapter::new(Memory::new());
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
                commit_id: CommitId::for_test_label("child-commit"),
                parent_commit_ids: vec![CommitId::for_test_label("parent-commit")],
                created_at: ts("2026-01-01T00:00:01Z"),
                change_id: ChangeId::for_test_label("child-commit-change"),
                selected_change_refs: Vec::new(),
            },
            FinalizedCommitRow {
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
            &[],
            &BTreeMap::from([
                (CommitId::for_test_label("parent-commit"), vec![0]),
                (CommitId::for_test_label("child-commit"), vec![1]),
            ]),
            &commits,
            false,
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
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let live_state = live_state_context();
        crate::test_support::seed_global_branch_head(storage.clone()).await;
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let state_rows = vec![untracked_global_row("change-untracked")];
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
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

        let loaded = live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&live_state_request())
            .await
            .expect("current row load should succeed")
            .expect("untracked row should be persisted in live state");
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );
        assert_eq!(loaded.change_id, None);

        let mut changelog_reader = ChangelogContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let changes = changelog_reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &[change_id("change-untracked")],
            })
            .await
            .expect("untracked changelog lookup should load");
        assert_eq!(
            changes.entries,
            vec![None],
            "untracked state is history-free and must not enter the changelog"
        );
    }

    #[tokio::test]
    async fn tracked_write_rejects_retention_change_for_existing_untracked_row() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        crate::test_support::seed_global_branch_head(storage.clone()).await;

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![untracked_global_row("change-untracked")],
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
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
        let error = commit_prepared_writes(
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
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect_err("tracked INSERT must not change an existing row's retention");
        assert_eq!(
            error.code,
            LixError::CODE_UNIQUE,
            "tracked and untracked identities must never shadow each other"
        );
    }

    #[tokio::test]
    async fn commit_staged_writes_applies_cross_subsystem_rows_as_one_storage_batch() {
        let counting_storage = CountingStorage::new();
        let write_batches = counting_storage.write_batches();
        let storage = StorageAdapter::new(counting_storage);
        let binary_cas = BinaryCasContext::new();
        let live_state = Arc::new(live_state_context());
        let branch_ctx = BranchContext::new();
        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("setup head read should open");
            let mut setup_row = tracked_global_row("setup-tracked-change");
            setup_row.commit_id = Some(commit_id("setup-commit"));
            let (writes, _) = commit_prepared_writes(
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
                    first_commit_parent_override_by_branch: BTreeMap::new(),
                    checkpoint_publications: Vec::new(),
                    extra_commit_parents_by_branch: BTreeMap::new(),
                    file_data_writes: Vec::new(),
                },
            )
            .await
            .expect("setup head should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("setup head should commit");
        }
        {
            let mut read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("deterministic mode read should open");
            let (writes, _) = commit_prepared_writes(
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
                    first_commit_parent_override_by_branch: BTreeMap::new(),
                    checkpoint_publications: Vec::new(),
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
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            FunctionContext::prepare(&read)
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

        let (writes, _) = commit_prepared_writes(
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
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("cross-subsystem commit should stage and apply");

        assert_eq!(
            write_batches.load(Ordering::SeqCst),
            0,
            "prepared writes should not touch the storage before the write set is committed"
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
            .expect("tracked changelog change should load");
        assert!(matches!(
            changes.entries.as_slice(),
            [Some(tracked), None] if tracked.change_id == change_id("change-tracked")
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

        let untracked = live_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&LiveStateRowRequest {
                schema_key: "test_schema".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single("entity-2"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("untracked row load should succeed")
            .expect("untracked row should persist in live state");
        assert_eq!(
            untracked.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );
        assert_eq!(untracked.change_id, None);

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
        let storage = StorageAdapter::new(Memory::new());
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
        let (writes, _) = commit_prepared_writes(
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
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
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
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_BRANCH_ID.to_string(),
                change_refs(["change-a", "change-b"]),
            )]),
            BTreeMap::new(),
            BTreeMap::new(),
            &BTreeMap::from([(
                GLOBAL_BRANCH_ID.to_string(),
                Some(CommitId::for_test_label("initial-commit")),
            )]),
        )
        .await
        .expect("global commit row should finalize");

        assert_eq!(rows.commit_rows.len(), 1);
        assert_eq!(rows.tracked_roots.len(), 1);
        let row = &rows.commit_rows[0];
        assert_eq!(row.commit_id, commit_id("test-uuid-1"));
        assert_eq!(row.change_id, change_id("test-uuid-2"));
        assert_eq!(row.created_at.to_string(), "2026-01-01T00:00:00.001Z");
        assert_eq!(
            row.parent_commit_ids,
            vec![CommitId::for_test_label("initial-commit")]
        );

        let root = &rows.tracked_roots[0];
        assert_eq!(root.branch_id, GLOBAL_BRANCH_ID);
        assert_eq!(root.commit_id, commit_id("test-uuid-1"));
    }

    #[tokio::test]
    async fn finalize_commit_rows_skips_empty_members() {
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_BRANCH_ID.to_string(),
                StagedCommitChangeRefs::default(),
            )]),
            BTreeMap::new(),
            BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .expect("empty change_refs should be ignored");

        assert!(rows.commit_rows.is_empty());
        assert!(rows.tracked_roots.is_empty());
    }

    #[tokio::test]
    async fn finalize_commit_rows_uses_existing_branch_ref_as_parent() {
        let rows = finalize_commit_rows(
            BTreeMap::from([("branch-a".to_string(), change_refs(["change-a"]))]),
            BTreeMap::new(),
            BTreeMap::new(),
            &BTreeMap::from([(
                "branch-a".to_string(),
                Some(CommitId::for_test_label("previous-commit")),
            )]),
        )
        .await
        .expect("active-branch commit finalization should resolve parent");

        assert_eq!(
            rows.commit_rows[0].parent_commit_ids,
            vec![CommitId::for_test_label("previous-commit")]
        );
        assert_eq!(rows.tracked_roots[0].branch_id, "branch-a");
    }

    #[tokio::test]
    async fn finalize_commit_rows_appends_extra_merge_parent_after_target_head() {
        let rows = finalize_commit_rows(
            BTreeMap::from([("branch-a".to_string(), change_refs(["change-a"]))]),
            BTreeMap::new(),
            BTreeMap::from([(
                "branch-a".to_string(),
                vec![CommitId::for_test_label("source-head")],
            )]),
            &BTreeMap::from([(
                "branch-a".to_string(),
                Some(CommitId::for_test_label("target-head")),
            )]),
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

    #[tokio::test]
    async fn prepared_commit_rejects_missing_non_global_branch_before_materialization() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_ctx = BranchContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = resolve_prepared_commit_parent_heads(
            &branch_ctx,
            &read,
            &PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![tracked_branch_row("missing-branch", "missing-change")],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "missing-branch".to_string(),
                    change_refs(["missing-change"]),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
            true,
        )
        .await
        .expect_err("non-global target must exist when commit materializes");

        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
    }

    #[tokio::test]
    async fn prepared_commit_allows_missing_global_root_before_materialization() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_ctx = BranchContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let heads = resolve_prepared_commit_parent_heads(
            &branch_ctx,
            &read,
            &PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: Vec::new(),
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs(["first-global-change"]),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
            true,
        )
        .await
        .expect("the root global commit may have no parent branch ref");

        assert_eq!(heads.get(GLOBAL_BRANCH_ID), Some(&None));
    }

    fn prepared_direct_branch_ref_update(
        branch_id: &str,
        target_commit_label: &str,
        branch_ref_change_label: &str,
    ) -> PreparedWriteSet {
        PreparedWriteSet {
            insert_identities: BTreeMap::new(),
            state_rows: vec![direct_branch_ref_row(
                branch_id,
                target_commit_label,
                branch_ref_change_label,
            )],
            commit_change_refs_by_branch: BTreeMap::new(),
            first_commit_parent_override_by_branch: BTreeMap::new(),
            checkpoint_publications: Vec::new(),
            extra_commit_parents_by_branch: BTreeMap::new(),
            file_data_writes: Vec::new(),
        }
    }

    async fn seed_empty_commit(storage: &StorageAdapter, label: &str) {
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("empty commit seed read should open");
        let mut writes = StorageWriteSet::new();
        crate::test_support::stage_empty_changelog_commit(&mut read, &mut writes, label, None)
            .await
            .expect("empty commit target should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("empty commit target should persist");
    }

    fn prepared_normal_global_commit(
        row_change_label: &str,
        commit_label: &str,
        commit_change_label: &str,
        branch_ref_change_label: &str,
    ) -> PreparedWriteSet {
        PreparedWriteSet {
            insert_identities: BTreeMap::new(),
            state_rows: vec![tracked_global_row(row_change_label)],
            commit_change_refs_by_branch: BTreeMap::from([(
                GLOBAL_BRANCH_ID.to_string(),
                change_refs_with(
                    [row_change_label],
                    commit_label,
                    commit_change_label,
                    branch_ref_change_label,
                ),
            )]),
            first_commit_parent_override_by_branch: BTreeMap::new(),
            checkpoint_publications: Vec::new(),
            extra_commit_parents_by_branch: BTreeMap::new(),
            file_data_writes: Vec::new(),
        }
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

    fn selected_change_ref(change_id: &str, entity_pk: &str) -> StagedCommitChangeRef {
        StagedCommitChangeRef {
            schema_key: "test_schema".to_string(),
            file_id: None,
            entity_pk: EntityPk::single(entity_pk),
            change_id: self::change_id(change_id),
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
        }
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

    fn direct_branch_ref_row(
        branch_id: &str,
        target_commit_label: &str,
        change_id: &str,
    ) -> PreparedStateRow {
        let mut row = untracked_global_row(change_id);
        row.entity_pk = EntityPk::single(branch_id);
        row.schema_key = BRANCH_REF_SCHEMA_KEY.to_string();
        row.snapshot = Some(
            crate::transaction::types::stage_json_from_value(
                crate::transaction::types::TransactionJson::from_value_for_test(
                    serde_json::json!({
                        "id": branch_id,
                        "commit_id": commit_id(target_commit_label).to_string(),
                    }),
                ),
                "test direct branch-ref snapshot",
            )
            .expect("branch-ref snapshot should stage"),
        );
        row
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

    struct CountingStorage {
        inner: Memory,
        write_batches: Arc<AtomicUsize>,
    }

    impl CountingStorage {
        fn new() -> Self {
            Self {
                inner: Memory::new(),
                write_batches: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn write_batches(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.write_batches)
        }
    }

    impl Storage for CountingStorage {
        type Read<'a>
            = MemoryRead
        where
            Self: 'a;

        type Write<'a>
            = CountingWrite
        where
            Self: 'a;
        async fn begin_read(
            &self,
            opts: StorageReadOptions,
        ) -> Result<Self::Read<'_>, StorageError> {
            self.inner.begin_read(opts).await
        }

        async fn begin_write(
            &self,
            opts: StorageWriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            Ok(CountingWrite {
                inner: self.inner.begin_write(opts).await?,
                write_batches: Arc::clone(&self.write_batches),
            })
        }
    }

    struct CountingWrite {
        inner: MemoryWrite,
        write_batches: Arc<AtomicUsize>,
    }

    impl StorageWrite for CountingWrite {
        async fn put_many(
            &mut self,
            space: SpaceId,
            entries: PutBatch,
        ) -> Result<(), StorageError> {
            self.inner.put_many(space, entries).await
        }

        async fn delete_many(
            &mut self,
            space: SpaceId,
            keys: &[StorageKey],
        ) -> Result<(), StorageError> {
            self.inner.delete_many(space, keys).await
        }

        async fn delete_range(
            &mut self,
            space: SpaceId,
            range: KeyRange,
        ) -> Result<(), StorageError> {
            self.inner.delete_range(space, range).await
        }

        async fn commit(self) -> Result<CommitResult, StorageError> {
            self.write_batches.fetch_add(1, Ordering::SeqCst);
            self.inner.commit().await
        }

        async fn rollback(self) -> Result<(), StorageError> {
            self.inner.rollback().await
        }
    }
}
