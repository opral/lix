#![allow(
    clippy::implicit_clone,
    clippy::unnecessary_mut_passed,
    clippy::unnecessary_wraps
)]

use bytes::Bytes;

use crate::LixError;
use crate::binary_cas::BinaryCasContext;
use crate::branch::{
    BRANCH_REF_SCHEMA_KEY, BranchContext, BranchHeadControl, BranchHeadControlContext,
    BranchHeadControlObservation, BranchRefReader, branch_head_control_precondition,
    stage_branch_head_control, stage_delete_branch_head_control,
};
use crate::changelog::{
    ChangeId, ChangeRecord, ChangelogContext, ChangelogReader, ChangelogWriter, CommitChangeRefSet,
    CommitId, CommitLoadRequest as ChangelogCommitLoadRequest,
    CommitProjection as ChangelogCommitProjection, CommitRecord, TransactionChangeRecordRef,
    TransactionChangelogAppend,
};
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::filesystem::stage_path_index_revision;
use crate::functions::FunctionContext;
use crate::json_store::{JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
use crate::live_state::{
    LiveStateIndexContext, LiveStateIndexDeltaRef, LiveStateIndexFilter, LiveStateIndexRowRequest,
    LiveStateIndexScanRequest, TrackedHeadContext, TrackedHeadDeltaRef, branch_empty_precondition,
    load_local_sidecar_branch_token, local_sidecar_branch_precondition, row_absent_precondition,
    row_raw_token_precondition, stage_local_sidecar_branch_marker,
};
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition, StorageWriteSet};
use crate::tracked_state::{
    TRACKED_STATE_COMMIT_DELTA_SPACE, TrackedStateContext, TrackedStateDeltaRef,
    TrackedStateFilter, TrackedStateKey, TrackedStateKeyRef, TrackedStateReadColumns,
    TrackedStateRootMutationRef, TrackedStateScanRequest, encode_key_ref, stage_commit_delta,
};
use crate::transaction::staging::{
    PreparedInsertIdentity, PreparedStateRowIdentity, PreparedWriteSet,
};
use crate::transaction::types::{PreparedStateRow, StagedCommitChangeRef, StagedCommitChangeRefs};
use std::collections::{BTreeMap, BTreeSet};
use tracing::Instrument as _;

type RowIndex = usize;

/// Commits prepared transaction rows into the unified change ledger and the
/// current live-state indexes.
///
/// Providers decode DataFusion DML into hydrated `PreparedStateRow`s. Every row
/// stages a canonical changelog fact. Tracked rows additionally become commit
/// members and update immutable history roots; untracked rows update only the
/// mutable flat live-state index.
#[cfg(test)]
pub(crate) async fn commit_prepared_writes(
    binary_cas: &BinaryCasContext,
    branch_ctx: &BranchContext,
    live_index: &LiveStateIndexContext,
    runtime_functions: Option<&FunctionContext>,
    read: &mut impl StorageAdapterRead,
    prepared_writes: PreparedWriteSet,
) -> Result<(StorageWriteSet, Vec<StoragePrecondition>), LixError> {
    let commit_parent_heads =
        resolve_prepared_commit_parent_heads(branch_ctx, &*read, &prepared_writes, false).await?;
    commit_prepared_writes_with_parent_heads(
        binary_cas,
        live_index,
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
    live_index: &LiveStateIndexContext,
    runtime_functions: Option<&FunctionContext>,
    commit_parent_heads: &BTreeMap<String, Option<CommitId>>,
    read: &mut impl StorageAdapterRead,
    prepared_writes: PreparedWriteSet,
) -> Result<(StorageWriteSet, Vec<StoragePrecondition>), LixError> {
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
    // V6 removes the automatic flat current row for a normal branch-head
    // advance, but `lix_change` remains an unscoped public ledger. Retain one
    // tiny direct change fact per published control.
    let branch_head_changes = tracked_roots
        .iter()
        .map(branch_ref_change_record)
        .collect::<Result<Vec<_>, _>>()?;
    let has_checkpoint_publication = !prepared_writes.checkpoint_publications.is_empty();
    // v6 publishes automatic tracked heads through one direct control record.
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

    let compactable_change_ids = stage_flat_current_rows(
        live_index,
        read,
        &mut writes,
        &state_rows,
        &engine_rows,
        &insert_identities,
        &mut preconditions,
    )
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.materialization.flat_current_rows"
    ))
    .await?;

    let staged_commits = stage_changelog_commits(
        read,
        &mut writes,
        &state_rows,
        &branch_head_changes,
        &engine_rows,
        &compactable_change_ids,
        &row_index.tracked_row_indices_by_commit,
        &commit_rows,
        has_checkpoint_publication,
    )
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.materialization.changelog"
    ))
    .await?;

    stage_tracked_commit_delta_index(
        &mut writes,
        &state_rows,
        &row_index.tracked_row_indices_by_commit,
        &tracked_roots,
        &staged_commits,
    )?;

    stage_state_json_payloads(&mut json_writer, &mut writes, &state_rows)?;

    let branch_control_observations =
        observe_branch_head_controls(read, &tracked_roots, &state_rows).await?;

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
    let normal_branch_controls = stage_tracked_head(
        read,
        &mut writes,
        &state_rows,
        &row_index.tracked_row_indices_by_commit,
        &tracked_roots,
        &staged_commits,
        &insert_identities,
        &branch_control_observations,
    )
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.materialization.tracked_head"
    ))
    .await?;
    stage_branch_head_control_publications(
        &mut writes,
        &normal_branch_controls,
        &state_rows,
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
    branch_ref_rows: &[EngineCurrentRow],
    compact_change_ids: &[ChangeId],
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    commit_rows: &[FinalizedCommitRow],
    force_root_fence: bool,
) -> Result<BTreeMap<CommitId, StagedChangelogCommit>, LixError> {
    let mut commits = Vec::with_capacity(commit_rows.len());
    let changes = state_rows
        .iter()
        .filter(|row| !(row.untracked && row.snapshot.is_none()))
        .map(transaction_change_record_from_state_row)
        .chain(
            branch_head_changes
                .iter()
                .map(|change| Ok(TransactionChangeRecordRef::from(change))),
        )
        .chain(
            branch_ref_rows
                .iter()
                .filter(|row| row.change.snapshot.is_some())
                .map(|row| Ok(TransactionChangeRecordRef::from(&row.change))),
        )
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
/// advance. The v6 control owns current visibility; this fact keeps the
/// existing `lix_change` contract without reintroducing a flat current row.
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

fn branch_ref_index_request_from_state_row(row: &PreparedStateRow) -> LiveStateIndexRowRequest {
    LiveStateIndexRowRequest {
        branch_id: row.branch_id.clone(),
        schema_key: row.schema_key.clone(),
        entity_pk: row.entity_pk.clone(),
        file_id: row.file_id.clone(),
    }
}

fn branch_ref_index_request_from_engine_row(row: &EngineCurrentRow) -> LiveStateIndexRowRequest {
    LiveStateIndexRowRequest {
        branch_id: row.branch_id.clone(),
        schema_key: row.change.schema_key.clone(),
        entity_pk: row.change.entity_pk.clone(),
        file_id: row.change.file_id.clone(),
    }
}

async fn stage_flat_current_rows(
    current: &LiveStateIndexContext,
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    engine_rows: &[EngineCurrentRow],
    insert_identities: &BTreeMap<PreparedStateRowIdentity, PreparedInsertIdentity>,
    preconditions: &mut Vec<StoragePrecondition>,
) -> Result<Vec<ChangeId>, LixError> {
    let index_reader = current.reader(read);

    // Branch refs are mutable, untracked rows. They intentionally do not
    // rotate the tracked mutation revision, so that revision cannot fence two
    // concurrent ref moves. Guard every directly staged and engine-generated
    // ref identity with the exact persisted flat-row bytes seen by this
    // coherent commit read. The bounded point batch also covers an absent ref
    // with a KeyAbsent precondition, preventing a stale first publication.
    let branch_ref_requests = state_rows
        .iter()
        .filter(|row| row.schema_key == BRANCH_REF_SCHEMA_KEY)
        .map(branch_ref_index_request_from_state_row)
        .chain(
            engine_rows
                .iter()
                .filter(|row| row.change.schema_key == BRANCH_REF_SCHEMA_KEY)
                .map(branch_ref_index_request_from_engine_row),
        )
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let branch_ref_tokens = index_reader
        .load_raw_row_tokens(&branch_ref_requests)
        .await?;
    preconditions.extend(
        branch_ref_requests
            .iter()
            .zip(branch_ref_tokens)
            .map(|(request, token)| row_raw_token_precondition(request, token))
            .collect::<Result<Vec<_>, _>>()?,
    );

    let mut branch_ref_sidecar_tokens = BTreeMap::<String, Option<Bytes>>::new();
    for row in state_rows
        .iter()
        .filter(|row| row.untracked && row.schema_key == BRANCH_REF_SCHEMA_KEY)
    {
        let branch_id = row.entity_pk.as_single_string_owned()?;
        let Some(snapshot) = row.snapshot.as_ref() else {
            let existing_commit_id = load_current_branch_ref_commit_id(read, &branch_id).await?;
            if existing_commit_id.is_some()
                && (has_pending_local_sidecar_rows(state_rows, &branch_id)
                    || guarded_branch_ref_has_local_untracked_rows(
                        &index_reader,
                        read,
                        &branch_id,
                        &mut branch_ref_sidecar_tokens,
                        preconditions,
                    )
                    .await?)
            {
                return Err(branch_ref_with_untracked_rows_error(&branch_id, true));
            }
            continue;
        };
        let Some(commit_id) = snapshot
            .value
            .get("commit_id")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let existing_commit_id = load_current_branch_ref_commit_id(read, &branch_id).await?;
        if existing_commit_id.as_deref() == Some(commit_id) {
            // Updating descriptor fields or assigning the current head again
            // must not disturb branch-local live state.
            continue;
        }
        if existing_commit_id.is_some()
            && (has_pending_local_sidecar_rows(state_rows, &branch_id)
                || guarded_branch_ref_has_local_untracked_rows(
                    &index_reader,
                    read,
                    &branch_id,
                    &mut branch_ref_sidecar_tokens,
                    preconditions,
                )
                .await?)
        {
            return Err(branch_ref_with_untracked_rows_error(&branch_id, false));
        }
        ensure_branch_ref_target_exists(read, commit_id).await?;
    }

    let local_sidecar_branch_ids = state_rows
        .iter()
        .filter(|row| row.untracked && row.branch_id != crate::GLOBAL_BRANCH_ID)
        .map(|row| row.branch_id.as_str())
        .collect::<BTreeSet<_>>();
    for branch_id in &local_sidecar_branch_ids {
        // The token is rotated by every local mutation. A concurrent tracked
        // write compares the exact value it observed, so it retries rather
        // than publishing validation against stale sidecar state.
        stage_local_sidecar_branch_marker(writes, branch_id)?;
    }

    let branch_ids = state_rows
        .iter()
        .map(|row| row.branch_id.as_str())
        .chain(engine_rows.iter().map(|row| row.branch_id.as_str()))
        .collect::<BTreeSet<_>>();
    let mut writer = current.writer(read, writes);
    let mut compactable_change_ids = BTreeSet::new();
    for branch_id in &branch_ids {
        let has_tracked_rows = state_rows
            .iter()
            .any(|row| row.branch_id == *branch_id && !row.untracked);
        let has_tracked_insert = state_rows.iter().any(|row| {
            row.branch_id == *branch_id
                && !row.untracked
                && row.snapshot.is_some()
                && insert_identities
                    .get(&PreparedStateRowIdentity::from(row))
                    .is_some_and(|insert| !insert.untracked())
        });
        let untracked_state_deltas = state_rows
            .iter()
            .filter(|row| row.branch_id == *branch_id && row.untracked)
            .map(current_delta_from_state_row)
            .collect::<Result<Vec<_>, _>>()?;
        let engine_deltas = engine_rows
            .iter()
            .filter(|row| row.branch_id == *branch_id)
            .map(current_delta_from_engine_row)
            .collect::<Vec<_>>();
        let new_deltas = untracked_state_deltas
            .into_iter()
            .chain(engine_deltas)
            .collect::<Vec<_>>();

        if !has_tracked_rows {
            if !new_deltas.is_empty() {
                compactable_change_ids
                    .extend(writer.stage_branch_rows(branch_id, new_deltas).await?);
            }
            continue;
        }

        // Tracked rows are served from the durable tracked-head projection.
        // A branch-local mutable sidecar is explicitly marked when it is
        // first used. Normal tracked commits therefore need one exact-key
        // precondition, rather than opening a flat-index prefix iterator on
        // every write. Global state intentionally keeps the legacy promotion
        // lane: global untracked rows include engine configuration alongside
        // tracked state and are not a branch-local sidecar.
        if *branch_id != crate::GLOBAL_BRANCH_ID {
            let sidecar_token = load_local_sidecar_branch_token(read, branch_id).await?;
            let sidecar_is_absent = sidecar_token.is_none();
            preconditions.push(local_sidecar_branch_precondition(branch_id, sidecar_token)?);
            if sidecar_is_absent {
                if !new_deltas.is_empty() {
                    compactable_change_ids
                        .extend(writer.stage_branch_rows(branch_id, new_deltas).await?);
                }
                continue;
            }
        }

        if !branch_has_local_untracked_rows(&index_reader, branch_id).await? {
            if has_tracked_insert {
                preconditions.push(branch_empty_precondition(branch_id)?);
            }
            if !new_deltas.is_empty() {
                compactable_change_ids
                    .extend(writer.stage_branch_rows(branch_id, new_deltas).await?);
            }
            continue;
        }

        // A populated sidecar is rare. Retain the existing per-row lowering
        // here so tracked writes can atomically reject collisions and clear a
        // sidecar row on the internal durability-promotion path.
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
        let all_deltas = state_deltas
            .into_iter()
            .chain(engine_deltas)
            .collect::<Vec<_>>();
        let absence_guards = state_rows
            .iter()
            .filter(|row| {
                row.branch_id == *branch_id
                    && row.snapshot.is_some()
                    && insert_identities
                        .get(&PreparedStateRowIdentity::from(*row))
                        .is_some_and(|insert| !insert.untracked())
            })
            .map(|row| LiveStateIndexRowRequest {
                branch_id: row.branch_id.clone(),
                schema_key: row.schema_key.clone(),
                entity_pk: row.entity_pk.clone(),
                file_id: row.file_id.clone(),
            })
            .collect::<BTreeSet<_>>();
        preconditions.extend(
            absence_guards
                .iter()
                .map(row_absent_precondition)
                .collect::<Result<Vec<_>, _>>()?,
        );
        compactable_change_ids.extend(
            writer
                .stage_branch_rows_with_absence_guards(branch_id, all_deltas, &absence_guards)
                .await?,
        );
    }
    if compactable_change_ids.is_empty() {
        return Ok(Vec::new());
    }
    let new_ids = state_rows
        .iter()
        .filter_map(|row| row.change_id)
        .chain(engine_rows.iter().map(|row| row.change.change_id))
        .collect::<BTreeSet<_>>();
    compactable_change_ids.retain(|change_id| !new_ids.contains(change_id));
    Ok(compactable_change_ids.into_iter().collect())
}

/// A branch ref targets changelog history, not an implementation detail of
/// its sparse immutable-root checkpoints. Ordinary v4 commits intentionally
/// have no root, so validate the canonical commit record instead.
async fn ensure_branch_ref_target_exists(
    read: &(impl StorageAdapterRead + ?Sized),
    commit_id: &str,
) -> Result<(), LixError> {
    let typed_commit_id = CommitId::parse_lix(commit_id, "branch ref target commit")?;
    let commits = ChangelogContext::new()
        .reader(read)
        .load_commits(ChangelogCommitLoadRequest {
            commit_ids: &[typed_commit_id],
            projection: ChangelogCommitProjection::Record,
        })
        .await?;
    if commits.entries.into_iter().next().flatten().is_some() {
        return Ok(());
    }
    Err(LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("branch ref targets unknown commit '{commit_id}'"),
    ))
}

async fn load_current_branch_ref_commit_id(
    read: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
) -> Result<Option<String>, LixError> {
    Ok(BranchHeadControlContext::new()
        .reader(read)
        .load(branch_id)
        .await?
        .map(|control| control.head_commit_id.to_string()))
}

async fn branch_has_local_untracked_rows<S>(
    reader: &crate::live_state::LiveStateIndexStoreReader<S>,
    branch_id: &str,
) -> Result<bool, LixError>
where
    S: StorageAdapterRead,
{
    Ok(!reader
        .scan_index_rows(&LiveStateIndexScanRequest {
            branch_id: branch_id.to_string(),
            filter: LiveStateIndexFilter::default(),
            projection: Vec::new(),
            limit: Some(1),
        })
        .await?
        .is_empty())
}

/// Branch-ref deletion and repointing inspect the sidecar before its pending
/// rows are staged. Reject a same-transaction collision explicitly rather
/// than allowing the later flat-index write to invalidate that safety check.
fn has_pending_local_sidecar_rows(state_rows: &[PreparedStateRow], branch_id: &str) -> bool {
    branch_id != crate::GLOBAL_BRANCH_ID
        && state_rows
            .iter()
            .any(|row| row.untracked && row.branch_id == branch_id)
}

/// Reads the branch-local sidecar under the same snapshot as a branch-ref
/// safety scan and carries its exact revision into the final write. A ref
/// delete or repoint must retry if a local row appears after that scan.
async fn guarded_branch_ref_has_local_untracked_rows<S, R>(
    reader: &crate::live_state::LiveStateIndexStoreReader<S>,
    read: &R,
    branch_id: &str,
    observed_tokens: &mut BTreeMap<String, Option<Bytes>>,
    preconditions: &mut Vec<StoragePrecondition>,
) -> Result<bool, LixError>
where
    S: StorageAdapterRead,
    R: StorageAdapterRead + ?Sized,
{
    let has_local_rows = branch_has_local_untracked_rows(reader, branch_id).await?;
    if branch_id != crate::GLOBAL_BRANCH_ID && !observed_tokens.contains_key(branch_id) {
        let token = load_local_sidecar_branch_token(read, branch_id).await?;
        preconditions.push(local_sidecar_branch_precondition(branch_id, token.clone())?);
        observed_tokens.insert(branch_id.to_string(), token);
    }
    Ok(has_local_rows)
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

fn current_delta_from_state_row(
    row: &PreparedStateRow,
) -> Result<LiveStateIndexDeltaRef<'_>, LixError> {
    let change_id = row.change_id.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "live-state index row is missing change_id",
        )
    })?;
    Ok(LiveStateIndexDeltaRef {
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

fn current_delta_from_engine_row(row: &EngineCurrentRow) -> LiveStateIndexDeltaRef<'_> {
    LiveStateIndexDeltaRef {
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

fn tracked_head_delta_from_state_row(
    row: &PreparedStateRow,
) -> Result<TrackedHeadDeltaRef<'_>, LixError> {
    let Some(change_id) = row.change_id else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked staged row is missing change_id before tracked-head staging",
        ));
    };
    let Some(commit_id) = row.commit_id else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "tracked staged row is missing commit_id before tracked-head staging",
        ));
    };
    Ok(TrackedHeadDeltaRef {
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        entity_pk: &row.entity_pk,
        change_id,
        commit_id,
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
    let delta_count = tracked_roots
        .iter()
        .map(|root| {
            tracked_row_indices_by_commit
                .get(&root.commit_id)
                .map_or(0, Vec::len)
                + staged_commits
                    .get(&root.commit_id)
                    .map_or(0, |staged| staged.selected_change_refs.len())
        })
        .sum::<usize>();
    writes.reserve_space(TRACKED_STATE_COMMIT_DELTA_SPACE, delta_count, 0);
    for root in tracked_roots {
        for &row_index in tracked_row_indices_by_commit
            .get(&root.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default()
        {
            stage_commit_delta(
                writes,
                tracked_delta_from_state_row(&state_rows[row_index])?,
            )?;
        }
        let staged = staged_commits.get(&root.commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked commit '{}' has no changelog facts for commit-delta staging",
                    root.commit_id
                ),
            )
        })?;
        for change_ref in &staged.selected_change_refs {
            stage_commit_delta(
                writes,
                tracked_delta_from_selected_change_ref(change_ref, root.commit_id)?,
            )?;
        }
    }
    Ok(())
}

/// Stages the generation-keyed head projection only for normal prepared
/// rows. Merge selected references use rootless first-parent replay until
/// their next ordinary child commit bootstraps a generation.
async fn stage_tracked_head(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_roots: &[PendingTrackedRoot],
    staged_commits: &BTreeMap<CommitId, StagedChangelogCommit>,
    insert_identities: &BTreeMap<PreparedStateRowIdentity, PreparedInsertIdentity>,
    observations: &BTreeMap<String, BranchHeadControlObservation>,
) -> Result<BTreeMap<String, BranchHeadControl>, LixError> {
    let tracked_head = TrackedHeadContext::new();
    let mut writer = tracked_head.writer(read, writes);
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
                        "missing v6 branch-control observation for normal publication branch '{}'",
                        root.branch_id
                    ),
                )
            })?
            .control;
        if !staged.selected_change_refs.is_empty() {
            insert_direct_branch_control(
                &mut controls,
                &root.branch_id,
                normal_branch_head_control(root, parent_control, root.commit_id),
            )?;
            continue;
        }
        let state_row_indices = tracked_row_indices_by_commit
            .get(&root.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let deltas = state_row_indices
            .iter()
            .map(|&row_index| tracked_head_delta_from_state_row(&state_rows[row_index]))
            .collect::<Result<Vec<_>, _>>()?;
        let absence_guards = state_row_indices
            .iter()
            .filter_map(|&row_index| {
                let row = &state_rows[row_index];
                tracked_row_requires_absence(row, insert_identities).then(|| TrackedStateKey {
                    schema_key: row.schema_key.clone(),
                    file_id: row.file_id.clone(),
                    entity_pk: row.entity_pk.clone(),
                })
            })
            .collect::<BTreeSet<_>>();

        // The v4 marker is the durable authority for current state. It is
        // intentionally bound only to the first-parent commit: serial normal
        // commits can append deltas without materializing an immutable root.
        let parent_generation = match (root.parent_commit_id, parent_control) {
            (Some(parent_commit_id), Some(control))
                if control.head_commit_id == parent_commit_id =>
            {
                tracked_head
                    .reader(read)
                    .generation_if_control_current(&root.branch_id, control)
                    .await?
            }
            _ => None,
        };
        let parent_is_current = parent_generation.is_some();
        let parent_rows = if parent_is_current || root.parent_commit_id.is_none() {
            None
        } else if let Some(parent_commit_id) = root.parent_commit_id {
            // A parent staged earlier in this same write set is not visible
            // through `read`; publish no partial generation. Once committed,
            // current reads safely take the changelog-replay fallback until a
            // following ordinary child bootstraps a complete v4 head.
            if tracked_roots
                .iter()
                .any(|candidate| candidate.commit_id == parent_commit_id)
            {
                insert_direct_branch_control(
                    &mut controls,
                    &root.branch_id,
                    normal_branch_head_control(root, parent_control, root.commit_id),
                )?;
                continue;
            }
            Some(
                TrackedStateContext::new()
                    .reader(read)
                    .scan_rows_at_commit(
                        &parent_commit_id.to_string(),
                        &TrackedStateScanRequest {
                            filter: TrackedStateFilter {
                                include_tombstones: true,
                                ..TrackedStateFilter::default()
                            },
                            read_columns: TrackedStateReadColumns::default(),
                            limit: None,
                        },
                    )
                    .await?,
            )
        } else {
            None
        };
        let generation = writer
            .stage_commit(
                &root.branch_id,
                parent_generation,
                root.commit_id,
                &deltas,
                &absence_guards,
                parent_rows,
            )
            .await?;
        insert_direct_branch_control(
            &mut controls,
            &root.branch_id,
            normal_branch_head_control(root, parent_control, generation),
        )?;
    }
    Ok(controls)
}

fn normal_branch_head_control(
    root: &PendingTrackedRoot,
    previous: Option<BranchHeadControl>,
    generation: CommitId,
) -> BranchHeadControl {
    BranchHeadControl {
        head_commit_id: root.commit_id,
        generation,
        created_at: previous.map_or(root.ref_updated_at, |control| control.created_at),
        updated_at: root.ref_updated_at,
        ref_change_id: root.ref_change_id,
    }
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

/// Publishes every v6 branch-head control under an exact-byte CAS token.
///
/// Normal tracked commits arrive as `normal_controls`, built from the same
/// parent/generation decision that wrote the v5 group marker. Explicit branch
/// management still enters the prepared-row pipeline for validation and
/// changelog compatibility, but its authoritative moving head is lowered
/// here as well. This deliberately keeps the rare lifecycle lane compatible
/// while removing automatic `lix_branch_ref` materialization from normal
/// CRUD commits.
async fn stage_branch_head_control_publications(
    writes: &mut StorageWriteSet,
    normal_controls: &BTreeMap<String, BranchHeadControl>,
    state_rows: &[PreparedStateRow],
    preconditions: &mut Vec<StoragePrecondition>,
    observations: &BTreeMap<String, BranchHeadControlObservation>,
) -> Result<(), LixError> {
    let explicit_targets = explicit_branch_head_targets(state_rows)?;
    let mut publications = normal_controls
        .iter()
        .map(|(branch_id, control)| (branch_id.clone(), Some(*control)))
        .collect::<BTreeMap<String, Option<BranchHeadControl>>>();

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
                        "missing v6 branch-control observation for explicit publication branch '{branch_id}'"
                    ),
                )
            })?
            .control;
        let desired = target
            .head_commit_id
            .map(|head_commit_id| BranchHeadControl {
                head_commit_id,
                // Repointing to the same head must not invalidate a complete v5
                // serving generation merely because public ref metadata changed.
                generation: existing
                    .filter(|control| control.head_commit_id == head_commit_id)
                    .map_or(head_commit_id, |control| control.generation),
                // The flat v5 branch-ref projection preserved creation time on
                // replacement. The control record owns that same public fact.
                created_at: existing.map_or(target.created_at, |control| control.created_at),
                updated_at: target.updated_at,
                ref_change_id: target.ref_change_id,
            });
        publications.insert(branch_id, desired);
    }

    if publications.is_empty() {
        return Ok(());
    }
    for (branch_id, desired) in &mut publications {
        let observation = observations.get(branch_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "missing v6 branch-control observation for publication branch '{branch_id}'"
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

/// Returns explicit public branch-ref targets. `None` is a deletion; `Some`
/// is a validated commit id. The v6 control record remains the authority, but
/// retaining these rows in the generic lifecycle lowering means its existing
/// sidecar and target-existence checks remain in force.
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
                                "branch ref for branch '{branch_id}' is missing commit_id before v6 publication"
                            ),
                        )
                    })?;
                CommitId::parse_lix(commit_id, "v6 branch-head control target")
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

/// Takes one coherent raw point batch for every control this materialization
/// can publish. The result is threaded through v5 generation selection and
/// final CAS staging, so the control plane adds exactly one control batch to
/// a commit rather than a head lookup plus a second token lookup.
async fn observe_branch_head_controls(
    read: &(impl StorageAdapterRead + ?Sized),
    tracked_roots: &[PendingTrackedRoot],
    state_rows: &[PreparedStateRow],
) -> Result<BTreeMap<String, BranchHeadControlObservation>, LixError> {
    let mut branch_ids = tracked_roots
        .iter()
        .map(|root| root.branch_id.clone())
        .collect::<BTreeSet<_>>();
    for row in state_rows {
        if row.schema_key == BRANCH_REF_SCHEMA_KEY && row.untracked {
            branch_ids.insert(row.entity_pk.as_single_string_owned()?);
        }
    }
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
        let absence_guards = state_row_indices
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
            .collect::<BTreeSet<_>>();
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
/// represented only by the changelog and the v4 durable head projection.
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
/// Moving heads publish through the v6 direct branch-control plane after the
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
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut required_branch_ids = prepared_writes
        .state_rows
        .iter()
        .map(|row| row.branch_id.clone())
        .chain(
            prepared_writes
                .file_data_writes
                .iter()
                .map(|write| write.branch_id.clone()),
        )
        .chain(
            prepared_writes
                .first_commit_parent_override_by_branch
                .keys()
                .cloned(),
        )
        .chain(
            prepared_writes
                .extra_commit_parents_by_branch
                .keys()
                .cloned(),
        )
        .collect::<BTreeSet<_>>();
    required_branch_ids.extend(commit_parent_branch_ids.iter().cloned());

    let branch_ref = branch_ctx.ref_reader(read);
    let mut parent_heads = BTreeMap::new();
    for branch_id in required_branch_ids {
        let head = branch_ref.load_head_commit_id(&branch_id).await?;
        if require_existing_non_global_targets
            && branch_id != crate::GLOBAL_BRANCH_ID
            && head.is_none()
        {
            return Err(LixError::branch_not_found(branch_id, "commit", "target"));
        }
        if commit_parent_branch_ids.contains(&branch_id) {
            parent_heads.insert(branch_id, head);
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
        LiveStateIndexContext, LiveStateIndexRowRequest, LiveStateProjection, LiveStateRowRequest,
    };
    use crate::storage::{
        CommitResult, GetManyResult, GetOptions, Key, KeyRange, PutBatch, ScanChunk, ScanOptions,
        SpaceId, Storage, StorageError, StorageRead, StorageWrite,
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

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            TrackedStateContext::new(),
            LiveStateIndexContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        )
    }

    #[derive(Default)]
    struct TrackedHeadReadCounts {
        marker_get_many_calls: AtomicUsize,
        sidecar_marker_get_many_calls: AtomicUsize,
        row_get_many_calls: AtomicUsize,
        row_scan_calls: AtomicUsize,
        flat_index_scan_calls: AtomicUsize,
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
            space: SpaceId,
            keys: &[Key],
            opts: GetOptions,
        ) -> Result<GetManyResult, StorageError> {
            if space == crate::live_state::TRACKED_HEAD_MARKER_SPACE.id {
                self.counts
                    .marker_get_many_calls
                    .fetch_add(1, Ordering::Relaxed);
            }
            if space == crate::live_state::LIVE_STATE_LOCAL_SIDECAR_BRANCH_SPACE.id {
                self.counts
                    .sidecar_marker_get_many_calls
                    .fetch_add(1, Ordering::Relaxed);
            }
            if space == crate::live_state::TRACKED_HEAD_GROUP_SPACE.id {
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
            self.inner.get_many(space, keys, opts).await
        }

        async fn scan(
            &self,
            space: SpaceId,
            range: KeyRange,
            opts: ScanOptions,
        ) -> Result<ScanChunk, StorageError> {
            if space == crate::live_state::TRACKED_HEAD_GROUP_SPACE.id {
                self.counts.row_scan_calls.fetch_add(1, Ordering::Relaxed);
            }
            if space == crate::live_state::LIVE_STATE_INDEX_ROW_SPACE.id {
                self.counts
                    .flat_index_scan_calls
                    .fetch_add(1, Ordering::Relaxed);
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
            &LiveStateIndexContext::new(),
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
    async fn direct_branch_ref_update_rejects_a_stale_flat_token() {
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
            &LiveStateIndexContext::new(),
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
                    if *space == crate::live_state::LIVE_STATE_INDEX_ROW_SPACE.id
            )
        }));

        let mut winner_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("winner branch-ref read should open");
        let (winner_writes, winner_preconditions) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            &LiveStateIndexContext::new(),
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
        let row = LiveStateIndexContext::new()
            .reader(read)
            .load_index_row(&branch_ref_index_request(branch_id))
            .await
            .expect("winner branch-ref row should load")
            .expect("winner branch-ref row should remain present");
        assert_eq!(
            row.change_id,
            change_id("winner-direct-branch-ref-change"),
            "the stale write must not replace the winner's current ref row"
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
            &LiveStateIndexContext::new(),
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
            &LiveStateIndexContext::new(),
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
            &LiveStateIndexContext::new(),
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
            &LiveStateIndexContext::new(),
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
        let diff = reader
            .diff_commits(
                &commit_id_text("rootless-first-commit"),
                &commit_id_text("rootless-second-commit"),
                &crate::tracked_state::TrackedStateDiffRequest::default(),
            )
            .await
            .expect("rootless commits should diff from replayed state");
        assert!(matches!(
            diff.entries.as_slice(),
            [entry] if entry.kind == crate::tracked_state::TrackedStateDiffKind::Modified
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
            &LiveStateIndexContext::new(),
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
            &LiveStateIndexContext::new(),
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
            &LiveStateIndexContext::new(),
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
            counts.marker_get_many_calls.load(Ordering::Relaxed) > 0,
            "the read must validate the head marker before serving tracked rows"
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
    async fn local_tracked_commit_uses_sidecar_marker_not_flat_index_scan() {
        let memory = Memory::new();
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let counts = Arc::new(TrackedHeadReadCounts::default());
        let mut read = StorageAdapterReadScope::new(CountingTrackedHeadRead {
            inner: memory
                .begin_read(StorageReadOptions::default())
                .await
                .expect("counted commit read should open"),
            counts: Arc::clone(&counts),
        });

        let mut row = tracked_branch_row("branch-a", "local-tracked-change");
        row.commit_id = Some(commit_id("local-tracked-commit"));
        let (_writes, preconditions) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            &LiveStateIndexContext::new(),
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![row],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "branch-a".to_string(),
                    change_refs_with(
                        ["local-tracked-change"],
                        "local-tracked-commit",
                        "local-tracked-commit-change",
                        "local-tracked-branch-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("local tracked commit should stage");

        assert_eq!(
            counts.flat_index_scan_calls.load(Ordering::Relaxed),
            0,
            "normal local tracked staging must not open a flat-sidecar prefix iterator"
        );
        assert_eq!(
            counts.sidecar_marker_get_many_calls.load(Ordering::Relaxed),
            1,
            "normal local tracked staging should probe only the sidecar marker"
        );
        assert!(preconditions.iter().any(|precondition| {
            matches!(
                precondition,
                StoragePrecondition::KeyAbsent { space, .. }
                    if *space == crate::live_state::LIVE_STATE_LOCAL_SIDECAR_BRANCH_SPACE.id
            )
        }));
    }

    #[tokio::test]
    async fn local_sidecar_write_remains_compatible_with_tracked_head_promotion() {
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();

        let mut first_tracked = tracked_branch_row("branch-a", "tracked-first-change");
        first_tracked.commit_id = Some(commit_id("tracked-first-commit"));
        let mut first_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("first tracked read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            &LiveStateIndexContext::new(),
            None,
            &mut first_read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![first_tracked],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "branch-a".to_string(),
                    change_refs_with(
                        ["tracked-first-change"],
                        "tracked-first-commit",
                        "tracked-first-commit-change",
                        "tracked-first-branch-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("first tracked commit should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("first tracked commit should persist");

        let mut sidecar = tracked_branch_row("branch-a", "sidecar-change");
        sidecar.untracked = true;
        sidecar.commit_id = None;
        let mut sidecar_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("sidecar read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            &LiveStateIndexContext::new(),
            None,
            &mut sidecar_read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![sidecar],
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("local sidecar write should stage after a tracked head");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("local sidecar write should persist after a tracked head");
        let sidecar_marker_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("sidecar marker read should open");
        assert!(
            load_local_sidecar_branch_token(&sidecar_marker_read, "branch-a")
                .await
                .expect("sidecar marker should load")
                .is_some(),
            "a local sidecar write must durably mark its branch"
        );

        let counts = Arc::new(TrackedHeadReadCounts::default());
        let mut second_read = StorageAdapterReadScope::new(CountingTrackedHeadRead {
            inner: memory
                .begin_read(StorageReadOptions::default())
                .await
                .expect("second tracked read should open"),
            counts: Arc::clone(&counts),
        });
        let mut second_tracked = tracked_branch_row("branch-a", "tracked-second-change");
        second_tracked.commit_id = Some(commit_id("tracked-second-commit"));
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            &LiveStateIndexContext::new(),
            None,
            &mut second_read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![second_tracked],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "branch-a".to_string(),
                    change_refs_with(
                        ["tracked-second-change"],
                        "tracked-second-commit",
                        "tracked-second-commit-change",
                        "tracked-second-branch-ref-change",
                    ),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect("tracked promotion should stage after a sidecar write");
        assert!(
            counts.flat_index_scan_calls.load(Ordering::Relaxed) > 0,
            "a marked sidecar branch must use the existing promotion path"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("tracked promotion should persist after a sidecar write");

        let visible = live_state_context()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("visible read should open"),
            )
            .load_row(&LiveStateRowRequest {
                schema_key: "test_schema".to_string(),
                branch_id: "branch-a".to_string(),
                entity_pk: EntityPk::single("entity-1"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("tracked promotion should be readable")
            .expect("tracked row should be visible");
        assert!(!visible.untracked);
        assert_eq!(visible.change_id, Some(change_id("tracked-second-change")));
    }

    #[tokio::test]
    async fn branch_ref_delete_rejects_pending_local_sidecar_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        crate::test_support::seed_branch_head(storage.clone(), "branch-a", "branch-head").await;

        let mut branch_ref_delete = untracked_global_row("delete-branch-ref");
        branch_ref_delete.entity_pk = EntityPk::single("branch-a");
        branch_ref_delete.schema_key = BRANCH_REF_SCHEMA_KEY.to_string();
        branch_ref_delete.snapshot = None;

        let mut pending_local_sidecar = tracked_branch_row("branch-a", "pending-sidecar-row");
        pending_local_sidecar.untracked = true;
        pending_local_sidecar.commit_id = None;
        pending_local_sidecar.global = false;

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("branch-ref delete read should open");
        let error = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            &LiveStateIndexContext::new(),
            None,
            &mut read,
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![branch_ref_delete, pending_local_sidecar],
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                file_data_writes: Vec::new(),
            },
        )
        .await
        .expect_err("branch-ref delete must reject a pending local sidecar row");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn serial_local_commit_reads_tracked_head_marker_once() {
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
            &LiveStateIndexContext::new(),
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
            &LiveStateIndexContext::new(),
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
            counts.marker_get_many_calls.load(Ordering::Relaxed),
            1,
            "serial tracked-head staging must reuse the generation it already read"
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
            &LiveStateIndexContext::new(),
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
            &LiveStateIndexContext::new(),
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

        let marker_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("marker read should open");
        let tracked_head = TrackedHeadContext::new();
        assert!(
            tracked_head
                .reader(&marker_read)
                .generation_if_current(GLOBAL_BRANCH_ID, &commit_id_text("global-head"))
                .await
                .expect("global head marker should load")
                .is_some(),
            "the global marker must bind its current branch ref"
        );
        assert!(
            tracked_head
                .reader(&marker_read)
                .generation_if_current("branch-a", &commit_id_text("branch-head"))
                .await
                .expect("branch head marker should load")
                .is_some(),
            "the branch marker must bind its current branch ref"
        );

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
            &LiveStateIndexContext::new(),
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
            counts.marker_get_many_calls.load(Ordering::Relaxed) >= 4,
            "each branch scan must validate both branch and global head markers"
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
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let state_rows = vec![untracked_global_row("change-untracked")];
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            &LiveStateIndexContext::new(),
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

        let loaded = LiveStateIndexContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&live_index_request("entity-1"))
            .await
            .expect("current row load should succeed")
            .expect("untracked row should be persisted in live state");
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );
        assert_eq!(loaded.change_id, change_id("change-untracked"));

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
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let live_state = Arc::new(live_state_context());
        let branch_ctx = BranchContext::new();

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            &LiveStateIndexContext::new(),
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
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            &LiveStateIndexContext::new(),
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
                .await
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
                &LiveStateIndexContext::new(),
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
                &LiveStateIndexContext::new(),
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
            &LiveStateIndexContext::new(),
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

        let untracked = LiveStateIndexContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&live_index_request("entity-2"))
            .await
            .expect("untracked row load should succeed")
            .expect("untracked row should persist in live state");
        assert_eq!(
            untracked.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );

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
            &LiveStateIndexContext::new(),
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

    fn branch_ref_index_request(branch_id: &str) -> LiveStateIndexRowRequest {
        LiveStateIndexRowRequest {
            schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            entity_pk: EntityPk::single(branch_id),
            file_id: None,
        }
    }

    fn live_index_request(entity_pk: &str) -> LiveStateIndexRowRequest {
        LiveStateIndexRowRequest {
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
