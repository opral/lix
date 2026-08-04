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
    ChangelogWriter, CommitId, CommitLoadRequest as ChangelogCommitLoadRequest, CommitRecord,
    TransactionChangeRecordRef, TransactionChangelogAppend,
};
use crate::checkpoint::CHECKPOINT_MARKER_SCHEMA_KEY;
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::filesystem::stage_path_index_revision;
use crate::functions::FunctionContext;
use crate::json_store::{
    JSON_INLINE_MAX_BYTES, JsonRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef,
};
use crate::live_state::{HotTrackedSnapshot, MaterializedLiveStateRow, TrackedHeadContext};
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition, StorageWriteSet};
#[cfg(test)]
use crate::tracked_state::TrackedStateContext;
#[cfg(test)]
use crate::tracked_state::stage_commit_state_manifest;
use crate::tracked_state::{
    CommitDeltaLifecycleSummary, CommitDeltaReplacementGeneration, CommitDeltaReplacementScope,
    CommitStateManifest, CommitStateMutationInventory, TrackedStateCommitDeltaRef,
    TrackedStateDeltaRef, TrackedStateFilter, TrackedStateIndexValueRef, TrackedStateKey,
    TrackedStateKeyRef, TrackedStateReadColumns, TrackedStateScanRequest,
    TrackedStateSingleStringReplacementRef, encode_key_ref, load_change_origin_keys_by_ids,
    stage_addressable_commit_deltas, stage_change_locators,
    stage_ordered_addressable_commit_deltas, stage_ordered_arrow_native_commit_deltas,
};
use crate::transaction::staging::{
    OrderedMutationJournal, PreparedInsertSelection, PreparedWriteSet,
};
#[cfg(test)]
use crate::transaction::types::StagedCommitChangeBatchBuilder;
use crate::transaction::types::{
    PreparedStateBatch, PreparedStateRowRef, StagedCommitChangeBatch, StagedCommitChangeRef,
    StagedCommitChangeRefs,
};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;
use tracing::Instrument as _;

type RowIndex = usize;

#[cfg(test)]
static DIRECT_JOURNAL_REPLACEMENT_PUBLICATIONS: std::sync::LazyLock<
    std::sync::Mutex<BTreeMap<String, usize>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(BTreeMap::new()));

#[cfg(test)]
pub(crate) fn take_direct_journal_replacement_publications(schema_key: &str) -> usize {
    DIRECT_JOURNAL_REPLACEMENT_PUBLICATIONS
        .lock()
        .expect("direct journal publication counters")
        .remove(schema_key)
        .unwrap_or_default()
}

/// Commits prepared transaction rows into tracked history and unified current
/// live state.
///
/// Providers decode DataFusion DML into a hydrated `PreparedStateBatch`. Tracked
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
        None,
        runtime_functions,
        crate::ANONYMOUS_ACCOUNT_ID,
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
    entity_schema_catalog: Option<&crate::catalog::CatalogSnapshot>,
    runtime_functions: Option<&FunctionContext>,
    active_account_id: &str,
    commit_parent_heads: &BTreeMap<String, Option<CommitId>>,
    read: &mut impl StorageAdapterRead,
    prepared_writes: PreparedWriteSet,
) -> Result<(StorageWriteSet, Vec<StoragePrecondition>), LixError> {
    let certified_fresh_plugin_file_id =
        crate::transaction::validation::fresh_plugin_file_import_certificate(&prepared_writes)
            .is_some()
            .then(|| prepared_writes.file_content_writes[0].file_id.clone());
    let mut host_certified_file_schemas =
        BTreeMap::<String, BTreeMap<String, BTreeSet<String>>>::new();
    let mut host_certified_live_increments =
        BTreeMap::<String, BTreeMap<(String, Option<String>), u64>>::new();
    for file in &prepared_writes.file_content_writes {
        for batch in file.certified_entity_batches().iter().filter(|batch| {
            batch.complete_file_state
                && matches!(
                    batch.format,
                    1 | crate::wasm::HOST_CERTIFIED_PACKET_FORMAT
                        | crate::wasm::HOST_CERTIFIED_ZSTD_PACKET_FORMAT
                )
        }) {
            let [schema_key] = batch.schema_keys.as_slice() else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "host-certified packet batch must own exactly one schema",
                ));
            };
            host_certified_file_schemas
                .entry(file.branch_id.clone())
                .or_default()
                .entry(file.file_id.clone())
                .or_default()
                .extend(batch.schema_keys.iter().cloned());
            let increments = host_certified_live_increments
                .entry(file.branch_id.clone())
                .or_default();
            for scope in [
                (schema_key.clone(), None),
                (schema_key.clone(), Some(file.file_id.clone())),
            ] {
                let next = increments
                    .get(&scope)
                    .copied()
                    .unwrap_or_default()
                    .checked_add(batch.row_count)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "host-certified collection live count exceeds u64",
                        )
                    })?;
                increments.insert(scope, next);
            }
        }
    }
    let mut writes = StorageWriteSet::new();
    let mut preconditions = Vec::new();
    for publication in &prepared_writes.checkpoint_publications {
        crate::gc::stage_recovery_ref_rotation(&mut writes, &publication.recovery_ref)?;
        crate::gc::stage_checkpoint_gc_state(&mut writes, &publication.gc_state)?;
    }
    let mut json_writer = JsonStoreContext::new().writer();
    let ordered_replacements = prepared_writes
        .commit_change_refs_by_branch
        .values()
        .filter_map(|refs| {
            refs.ordered_mutation_journal()
                .map(|journal| (refs.commit_id, Arc::clone(journal)))
        })
        .collect::<BTreeMap<_, _>>();

    let filesystem_view_changed = prepared_writes.state_rows.iter().any(|row| {
        matches!(
            row.schema_key.as_str(),
            "lix_file_descriptor"
                | "lix_directory_descriptor"
                | "lix_binary_blob_ref"
                | BRANCH_REF_SCHEMA_KEY
        )
    }) || prepared_writes
        .commit_change_refs_by_branch
        .values()
        .flat_map(StagedCommitChangeRefs::selected_changes)
        .any(|change_ref| {
            matches!(
                change_ref.schema_key(),
                "lix_file_descriptor" | "lix_directory_descriptor" | "lix_binary_blob_ref"
            )
        });
    let mut state_rows = prepared_writes.state_rows;
    // Explicit branch publications are the final commit-planning consumer of
    // decoded JSON. Project them into one typed batch map before dropping the
    // shared parsed column; every later materialization stage consumes this
    // map plus canonical arena slices.
    let explicit_branch_targets = explicit_branch_head_targets(&state_rows)?;
    let deleted_checkpoint_branches = explicit_branch_targets
        .iter()
        .filter_map(|(branch_id, target)| target.head_commit_id.is_none().then_some(branch_id))
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut deleted_checkpoint_files = state_rows
        .iter()
        .filter(|row| row.schema_key == "lix_binary_blob_ref" && row.snapshot.is_none())
        .filter_map(|row| {
            row.file_id
                .map(|file_id| (row.branch_id.to_string(), file_id.to_string()))
        })
        .collect::<BTreeSet<_>>();
    deleted_checkpoint_files.extend(
        prepared_writes
            .file_content_writes
            .iter()
            .filter(|write| write.plugin_checkpoint().is_none())
            .map(|write| (write.branch_id.clone(), write.file_id.clone())),
    );
    for write in prepared_writes
        .file_content_writes
        .iter()
        .filter(|write| write.plugin_checkpoint().is_some())
    {
        deleted_checkpoint_files.remove(&(write.branch_id.clone(), write.file_id.clone()));
    }
    let mut insert_selection = prepared_writes.insert_selection;
    let mut entity_columnar_write_sets = if ordered_replacements.is_empty() {
        prepare_entity_columnar_write_sets(&state_rows, entity_schema_catalog)?
    } else {
        crate::live_state::EntityColumnarWriteSets::new()
    };
    append_sparse_authoritative_entity_columnar_write_sets(
        &mut entity_columnar_write_sets,
        &state_rows,
        entity_schema_catalog,
        true,
    )?;
    append_base_authoritative_arrow_state_write_sets(
        &mut entity_columnar_write_sets,
        &state_rows,
        true,
    )?;
    release_validated_canonical_value_columns(&mut state_rows);
    if !prepared_writes.file_content_writes.is_empty() {
        let mut blob_writer = binary_cas.writer_skipping_existing_chunks(&*read, &mut writes);
        for write in &prepared_writes.file_content_writes {
            if !write.stage_payload_at_commit() {
                debug_assert!(write.auxiliary_payloads().is_empty());
                continue;
            }
            let payload = write
                .inline_payload()
                .expect("only inline file content is staged during commit");
            blob_writer
                .stage_file_payload(
                    payload,
                    write.same_length_blob_splice(),
                    write.edit_blob_splice(),
                )
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.binary_cas_stage_payload"
                ))
                .await?;
            for payload in write.auxiliary_payloads() {
                blob_writer.stage_payload(payload).await?;
            }
        }
        drop(blob_writer);
        for write in &prepared_writes.file_content_writes {
            if let Some(checkpoint) = write.plugin_checkpoint() {
                crate::transaction::plugin_checkpoint::stage_current_plugin_checkpoint(
                    &mut writes,
                    &write.branch_id,
                    &write.file_id,
                    &checkpoint.generation,
                    &checkpoint.semantic_root,
                    write.blob_hash().unwrap_or_else(|| {
                        crate::binary_cas::BlobId::from_content(
                            write
                                .inline_data()
                                .expect("plugin checkpoints require inline file content"),
                        )
                    }),
                    &checkpoint.runtime,
                    &checkpoint.authority,
                )?;
            }
        }
    }
    let deleted_checkpoint_files = deleted_checkpoint_files
        .into_iter()
        .filter(|(branch_id, _)| !deleted_checkpoint_branches.contains(branch_id))
        .collect::<Vec<_>>();
    crate::transaction::plugin_checkpoint::stage_delete_current_plugin_checkpoints(
        &*read,
        &mut writes,
        &deleted_checkpoint_files,
    )
    .await?;
    for branch_id in &deleted_checkpoint_branches {
        crate::transaction::plugin_checkpoint::stage_delete_branch_plugin_checkpoints(
            &*read,
            &mut writes,
            branch_id,
        )
        .await?;
    }
    let finalized = finalize_commit_rows(
        prepared_writes.commit_change_refs_by_branch,
        prepared_writes.first_commit_parent_override_by_branch,
        prepared_writes.extra_commit_parents_by_branch,
        prepared_writes.intermediate_commits,
        commit_parent_heads,
    )
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.materialization.finalize_commit_rows"
    ))
    .await?;
    let commit_rows = finalized.commit_rows;
    let tracked_roots = finalized.tracked_roots;
    let mut certified_packet_root_rows = BTreeMap::<CommitId, Vec<MaterializedLiveStateRow>>::new();
    let mut certified_replacement_markers = BTreeMap::<CommitId, BTreeSet<TrackedStateKey>>::new();
    for file in prepared_writes
        .file_content_writes
        .iter()
        .filter(|file| !file.certified_entity_batches().is_empty())
    {
        let root = tracked_roots
            .iter()
            .find(|root| root.publish_head && root.branch_id == file.branch_id)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "certified entity batch has no matching published commit",
                )
            })?;
        let timestamp = commit_rows
            .iter()
            .find(|commit| commit.commit_id == root.commit_id)
            .map(|commit| commit.created_at)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "certified entity batch commit has no timestamp",
                )
            })?;
        let mut expanded_rows = Vec::new();
        let mut replacement_schemas = BTreeSet::new();
        for batch in file
            .certified_entity_batches()
            .iter()
            .filter(|batch| certified_batch_requires_root_expansion(batch))
        {
            if batch.complete_file_state {
                replacement_schemas.extend(batch.schema_keys.iter().cloned());
            }
            expanded_rows.extend(
                crate::live_state::materialize_certified_root_rows(
                    &file.branch_id,
                    &file.file_id,
                    root.commit_id,
                    timestamp,
                    batch,
                )?
                .into_rows(),
            );
        }
        for schema_key in replacement_schemas {
            let marker = certified_collection_replacement_marker(
                &file.branch_id,
                &file.file_id,
                &schema_key,
                root.commit_id,
                timestamp,
            )?;
            certified_replacement_markers
                .entry(root.commit_id)
                .or_default()
                .insert(TrackedStateKey {
                    schema_key: marker.schema_key.clone(),
                    file_id: marker.file_id.clone(),
                    entity_pk: marker.entity_pk.clone(),
                });
            expanded_rows.push(marker);
        }
        if !expanded_rows.is_empty() {
            certified_packet_root_rows
                .entry(root.commit_id)
                .or_default()
                .append(&mut expanded_rows);
        }
    }
    for (commit_id, rows) in &mut certified_packet_root_rows {
        let ordinary_identities = state_rows
            .iter()
            .filter(|row| row.commit_id == Some(*commit_id))
            .map(|row| {
                (
                    row.schema_key.to_string(),
                    row.file_id.map(ToString::to_string),
                    row.entity_pk.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        rows.retain(|row| {
            !ordinary_identities.contains(&(
                row.schema_key.clone(),
                row.file_id.clone(),
                row.entity_pk.clone(),
            ))
        });
        rows.sort_unstable_by(|left, right| {
            (&left.schema_key, &left.file_id, &left.entity_pk).cmp(&(
                &right.schema_key,
                &right.file_id,
                &right.entity_pk,
            ))
        });
        if rows.windows(2).any(|pair| {
            (&pair[0].schema_key, &pair[0].file_id, &pair[0].entity_pk)
                == (&pair[1].schema_key, &pair[1].file_id, &pair[1].entity_pk)
        }) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified entity batches contain duplicate root identities",
            ));
        }
    }
    let certified_packet_json_refs = certified_root_json_refs(&certified_packet_root_rows);
    let checkpoint_epochs = checkpoint_epoch_bindings(&prepared_writes.checkpoint_publications)?;
    // The current-state protocol removes the automatic mutable branch-ref
    // row for a normal branch-head advance, but `lix_change` remains an
    // unscoped public ledger. Retain one tiny direct change fact per
    // published control.
    let branch_head_changes = tracked_roots
        .iter()
        .filter(|root| root.publish_head)
        .map(|root| branch_ref_change_record(root, active_account_id))
        .collect::<Result<Vec<_>, _>>()?;
    // Every commit publishes an immutable, structurally shared tracked-state
    // root. Historical diff, merge, and point reads can therefore traverse
    // endpoint trees instead of replaying the first-parent changelog.
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
            active_account_id,
        )?);
    }
    retain_untracked_rows_not_superseded_by_engine(
        &mut state_rows,
        &mut insert_selection,
        &engine_rows,
    );
    let row_index = index_prepared_rows(&state_rows)?;

    if state_rows.is_empty()
        && commit_rows.is_empty()
        && engine_rows.is_empty()
        && writes.is_empty()
    {
        return Ok((writes, preconditions));
    }

    let selected_arrow_states = load_selected_arrow_states(read, &commit_rows).await?;
    let replacement_generations = ordered_replacement_generations(&ordered_replacements)?;

    append_ordered_replacement_columnar_write_sets(
        &mut entity_columnar_write_sets,
        &ordered_replacements,
        &replacement_generations,
        entity_schema_catalog,
    )?;

    let mut staged_delta_index = Box::pin(stage_tracked_commit_delta_index(
        read,
        &mut writes,
        &mut state_rows,
        &entity_columnar_write_sets,
        &row_index.tracked_row_indices_by_commit,
        &tracked_roots,
        &commit_rows,
        &selected_arrow_states,
        &certified_packet_root_rows,
        &certified_packet_json_refs,
        &insert_selection,
        &replacement_generations,
        &ordered_replacements,
    ))
    .await?;

    append_sparse_authoritative_entity_columnar_write_sets(
        &mut entity_columnar_write_sets,
        &state_rows,
        entity_schema_catalog,
        false,
    )?;
    append_base_authoritative_arrow_state_write_sets(
        &mut entity_columnar_write_sets,
        &state_rows,
        false,
    )?;
    append_certified_packet_arrow_state_write_sets(
        &mut entity_columnar_write_sets,
        &certified_packet_root_rows,
        &certified_packet_json_refs,
    )?;
    for (&commit_id, state_row_indices) in &row_index.tracked_row_indices_by_commit {
        let Some(inventory) = staged_delta_index.inventories.get_mut(&commit_id) else {
            continue;
        };
        if !inventory.sealed_state_parts.is_empty()
            || inventory.selected_source_commit_id.is_some()
            || usize::try_from(inventory.member_count).ok() != Some(state_row_indices.len())
        {
            continue;
        }
        let Some(descriptors) = stage_certified_native_state_parts(
            &mut writes,
            &state_rows,
            state_row_indices,
            commit_id,
            &entity_columnar_write_sets,
        )?
        else {
            continue;
        };
        let first = state_rows.row(state_row_indices[0]);
        inventory.single_partition = Some(CommitDeltaReplacementScope {
            schema_key: first.schema_key.to_string(),
            file_id: first.file_id.map(ToString::to_string),
        });
        inventory.sealed_state_parts = descriptors;
    }

    let mut external_parent_manifests = BTreeMap::new();
    let replacement_generation_commits = replacement_generations.keys().copied().collect();

    let staged_commits = Box::pin(
        stage_changelog_commits(
            read,
            &mut writes,
            &state_rows,
            &branch_head_changes,
            &engine_rows,
            &[],
            &row_index.tracked_row_indices_by_commit,
            &commit_rows,
            &mut external_parent_manifests,
            active_account_id,
        )
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.materialization.changelog"
        )),
    )
    .await?;

    ensure_explicit_branch_ref_targets_exist(read, &explicit_branch_targets, &staged_commits)
        .await?;

    let selected_change_payloads =
        materialize_selected_arrow_payloads(read, &selected_arrow_states).await?;

    stage_state_json_payloads(
        &mut json_writer,
        &mut writes,
        &state_rows,
        &certified_packet_root_rows,
        &certified_packet_json_refs,
    )?;
    for journal in ordered_replacements.values() {
        json_writer.stage_batch(
            &mut writes,
            JsonWritePlacementRef::OutOfBand,
            journal.iter().filter_map(|row| match row.snapshot_slot() {
                crate::json_store::JsonSlotRef::Ref(json_ref) => Some(
                    NormalizedJsonRef::trusted_prehashed(row.snapshot(), *json_ref),
                ),
                crate::json_store::JsonSlotRef::Inline(_)
                | crate::json_store::JsonSlotRef::None => None,
            }),
        )?;
    }

    let branch_control_observations =
        observe_branch_head_controls(read, &tracked_roots, &state_rows, &engine_rows).await?;

    reject_explicit_branch_ref_lifecycle_with_untracked_rows(
        read,
        &state_rows,
        &engine_rows,
        &explicit_branch_targets,
        &branch_control_observations,
    )
    .await?;

    stage_commit_state_manifests(
        read,
        &mut writes,
        &commit_rows,
        &staged_delta_index.inventories,
        &mut staged_delta_index.planned_members,
        &staged_commits,
        &external_parent_manifests,
        &entity_columnar_write_sets,
    )
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.materialization.current_state_catalog"
    ))
    .await?;
    // Root and untracked-HOT publication have adapter-specific futures. Keep
    // their combined async state out of the parent
    // commit future so an inactive bulk branch cannot inflate every ordinary
    // SlateDB transaction's native stack.
    let mut staged_hot_heads = Box::pin(stage_arrow_root_backed_heads(
        read,
        &mut writes,
        &state_rows,
        &entity_columnar_write_sets,
        &engine_rows,
        &row_index.tracked_row_indices_by_commit,
        &tracked_roots,
        &staged_commits,
        &selected_change_payloads,
        &insert_selection,
        certified_fresh_plugin_file_id.as_deref(),
        &host_certified_file_schemas,
        &host_certified_live_increments,
        &explicit_branch_targets,
        &branch_control_observations,
        &checkpoint_epochs,
        &staged_delta_index.ordered_addressable_commits,
        &replacement_generation_commits,
        &ordered_replacements,
    ))
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.materialization.tracked_head"
    ))
    .await?;
    for file in &prepared_writes.file_content_writes {
        let Some(control) = staged_hot_heads.controls.get_mut(&file.branch_id) else {
            continue;
        };
        control.note_schemas(
            file.certified_entity_batches()
                .iter()
                .flat_map(|batch| batch.schema_keys.iter().map(String::as_str)),
        );
    }
    stage_branch_head_control_publications(
        read,
        &mut writes,
        &staged_hot_heads.controls,
        &state_rows,
        &engine_rows,
        &explicit_branch_targets,
        &insert_selection,
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

fn certified_batch_requires_root_expansion(batch: &crate::wasm::WasmCertifiedEntityBatch) -> bool {
    !matches!(
        batch.format,
        crate::wasm::HOST_CERTIFIED_PACKET_FORMAT | crate::wasm::HOST_CERTIFIED_ZSTD_PACKET_FORMAT
    )
}

fn certified_collection_replacement_marker(
    branch_id: &str,
    file_id: &str,
    schema_key: &str,
    commit_id: CommitId,
    timestamp: LixTimestamp,
) -> Result<MaterializedLiveStateRow, LixError> {
    use crate::collection_generation::{
        COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_key,
    };

    let scope_key = collection_scope_key(CollectionScopeRef {
        schema_key,
        file_id: Some(file_id),
    });
    let snapshot = serde_json::to_string(&serde_json::json!({
        "scope_key": scope_key,
        "schema_key": schema_key,
        "file_id": file_id,
        "live_count": 0,
    }))
    .map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to encode certified collection replacement: {error}"),
        )
    })?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"lix.certified.collection-replacement.v1\0");
    hasher.update(commit_id.as_uuid().as_bytes());
    hasher.update(scope_key.as_bytes());
    let mut change_bytes = [0_u8; 16];
    change_bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ok(MaterializedLiveStateRow {
        entity_pk: EntityPk::single(scope_key),
        schema_key: COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
        file_id: None,
        snapshot_content: Some(snapshot.into()),
        metadata: None,
        deleted: false,
        created_at: timestamp,
        updated_at: timestamp,
        global: false,
        change_id: Some(ChangeId::new(uuid::Uuid::from_bytes(change_bytes))),
        commit_id: Some(commit_id),
        untracked: false,
        branch_id: Arc::from(branch_id),
    })
}

fn retain_untracked_rows_not_superseded_by_engine(
    rows: &mut PreparedStateBatch,
    insert_selection: &mut PreparedInsertSelection,
    engine_rows: &[EngineCurrentRow],
) {
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
    let retained = rows
        .iter()
        .enumerate()
        .filter_map(|(row_index, row)| {
            (!row.untracked
                || !engine_identities.contains(&(
                    row.branch_id.as_str(),
                    row.schema_key.as_str(),
                    row.entity_pk,
                    row.file_id.map(crate::common::SharedStr::as_str),
                )))
            .then_some(row_index)
        })
        .collect::<Vec<_>>();
    if retained.len() != rows.len() {
        rows.select_rows(&retained);
        insert_selection.select_rows(&retained);
    }
}

fn stage_state_json_payloads(
    json_writer: &mut crate::json_store::JsonStoreWriter,
    writes: &mut StorageWriteSet,
    state_rows: &PreparedStateBatch,
    certified_rows_by_commit: &BTreeMap<CommitId, Vec<MaterializedLiveStateRow>>,
    certified_refs_by_commit: &BTreeMap<CommitId, Vec<CertifiedRootJsonRefs>>,
) -> Result<(), LixError> {
    json_writer.stage_batch(
        writes,
        JsonWritePlacementRef::OutOfBand,
        state_rows
            .iter()
            .flat_map(json_payloads_from_state_row)
            .chain(
                certified_rows_by_commit
                    .iter()
                    .flat_map(|(commit_id, rows)| {
                        let refs = &certified_refs_by_commit[commit_id];
                        rows.iter().zip(refs).flat_map(|(row, refs)| {
                            [
                                row.snapshot_content.as_ref().zip(refs.snapshot.as_ref()),
                                row.metadata.as_ref().zip(refs.metadata.as_ref()),
                            ]
                            .into_iter()
                            .flatten()
                            .map(|(json, json_ref)| {
                                NormalizedJsonRef::trusted_prehashed(json.as_str(), *json_ref)
                            })
                        })
                    }),
            ),
    )?;
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct CertifiedRootJsonRefs {
    snapshot: Option<JsonRef>,
    metadata: Option<JsonRef>,
}

fn certified_root_json_refs(
    rows_by_commit: &BTreeMap<CommitId, Vec<MaterializedLiveStateRow>>,
) -> BTreeMap<CommitId, Vec<CertifiedRootJsonRefs>> {
    let mut refs_by_commit = BTreeMap::new();
    for (&commit_id, rows) in rows_by_commit {
        let refs = rows
            .iter()
            .map(|row| CertifiedRootJsonRefs {
                snapshot: row
                    .snapshot_content
                    .as_ref()
                    .filter(|json| json.len() > JSON_INLINE_MAX_BYTES)
                    .map(|json| JsonRef::for_content(json.as_bytes())),
                metadata: row
                    .metadata
                    .as_ref()
                    .filter(|json| json.len() > JSON_INLINE_MAX_BYTES)
                    .map(|json| JsonRef::for_content(json.as_bytes())),
            })
            .collect::<Vec<_>>();
        refs_by_commit.insert(commit_id, refs);
    }
    refs_by_commit
}

fn json_payloads_from_state_row(
    row: PreparedStateRowRef<'_>,
) -> impl Iterator<Item = NormalizedJsonRef<'_>> {
    row.snapshot
        .into_iter()
        .chain(row.metadata)
        .filter(|json| !json.is_inline())
        .map(|json| NormalizedJsonRef::trusted_prehashed(json.normalized(), json.json_ref))
}

struct PreparedRowIndex {
    tracked_row_indices_by_commit: BTreeMap<CommitId, Vec<RowIndex>>,
}

fn index_prepared_rows(rows: &PreparedStateBatch) -> Result<PreparedRowIndex, LixError> {
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
    record: CommitRecord,
    selected_change_batches: Vec<StagedCommitChangeBatch>,
}

struct StagedCommitDeltaIndex {
    ordered_addressable_commits: BTreeSet<CommitId>,
    inventories: BTreeMap<CommitId, CommitStateMutationInventory>,
    planned_members: BTreeMap<CommitId, Vec<crate::tracked_state::CommitDeltaMember>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SelectedChangeKey {
    source_commit_id: CommitId,
    identity: TrackedStateKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SelectedArrowState {
    coordinate: Option<crate::tracked_state::TrackedStateBaseCoordinate>,
    row: Option<crate::tracked_state::HydratedArrowStatePayload>,
    origin_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MaterializedSelectedArrowPayload {
    snapshot_content: Option<crate::common::SharedStr>,
    metadata: Option<crate::common::SharedStr>,
}

fn selected_change_key(change_ref: StagedCommitChangeRef<'_>) -> SelectedChangeKey {
    SelectedChangeKey {
        source_commit_id: change_ref.source_commit_id,
        identity: TrackedStateKey {
            schema_key: change_ref.schema_key().to_owned(),
            file_id: change_ref.file_id().map(str::to_owned),
            entity_pk: change_ref.entity_pk().clone(),
        },
    }
}

fn selected_changes(
    batches: &[StagedCommitChangeBatch],
) -> impl Iterator<Item = StagedCommitChangeRef<'_>> + '_ {
    batches.iter().flat_map(StagedCommitChangeBatch::iter)
}

fn selected_change_count(batches: &[StagedCommitChangeBatch]) -> usize {
    batches.iter().map(StagedCommitChangeBatch::len).sum()
}

fn dense_selected_source_is_exact<'a>(
    source_commit_id: CommitId,
    segment_row_counts: &[u16],
    source_membership_certified: bool,
    selected: impl Iterator<Item = StagedCommitChangeRef<'a>>,
) -> bool {
    if !source_membership_certified {
        return false;
    }
    let expected_count = segment_row_counts
        .iter()
        .map(|&count| usize::from(count))
        .sum::<usize>();
    let mut members = Vec::with_capacity(expected_count);
    for change_ref in selected {
        if change_ref.source_commit_id != source_commit_id {
            return false;
        }
        let Some(locator) = crate::tracked_state::direct_change_locator(change_ref.change_id)
        else {
            return false;
        };
        if locator.commit_id != source_commit_id {
            return false;
        }
        members.push((locator.segment_index, locator.ordinal));
    }
    if members.len() != expected_count {
        return false;
    }
    if !members
        .windows(2)
        .all(|pair| (pair[0].0, pair[0].1) < (pair[1].0, pair[1].1))
    {
        members.sort_unstable_by_key(|member| (member.0, member.1));
    }
    let mut member_index = 0usize;
    for (segment_index, &row_count) in segment_row_counts.iter().enumerate() {
        let Ok(segment_index) = u32::try_from(segment_index) else {
            return false;
        };
        for ordinal in 0..row_count {
            if members
                .get(member_index)
                .is_none_or(|member| (member.0, member.1) != (segment_index, ordinal))
            {
                return false;
            }
            member_index += 1;
        }
    }
    member_index == members.len()
}

async fn stage_changelog_commits(
    read: &mut impl StorageAdapterRead,
    writes: &mut StorageWriteSet,
    state_rows: &PreparedStateBatch,
    branch_head_changes: &[ChangeRecord],
    _branch_ref_rows: &[EngineCurrentRow],
    compact_change_ids: &[ChangeId],
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    commit_rows: &[FinalizedCommitRow],
    external_parent_manifests: &mut BTreeMap<CommitId, CommitStateManifest>,
    active_account_id: &str,
) -> Result<BTreeMap<CommitId, StagedChangelogCommit>, LixError> {
    let mut commits = Vec::with_capacity(commit_rows.len());
    let staged_commit_ids = commit_rows
        .iter()
        .map(|commit| commit.commit_id)
        .collect::<BTreeSet<_>>();
    if staged_commit_ids.len() != commit_rows.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "transaction contains duplicate staged commit ids",
        ));
    }
    let external_parent_ids = commit_rows
        .iter()
        .flat_map(|commit| commit.parent_commit_ids.iter().copied())
        .filter(|parent| !staged_commit_ids.contains(parent))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let external_parent_records = ChangelogContext::new()
        .reader(&mut *read)
        .load_commits(ChangelogCommitLoadRequest {
            commit_ids: &external_parent_ids,
        })
        .await?;
    let mut generations = BTreeMap::new();
    for (commit_id, record) in external_parent_records {
        let record = record.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("commit '{commit_id}' has a missing parent"),
            )
        })?;
        let manifest = crate::tracked_state::load_commit_state_manifest(read, *commit_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("commit '{commit_id}' has no commit-state authority"),
                )
            })?;
        external_parent_manifests.insert(*commit_id, manifest.clone());
        if manifest.generation != record.generation
            || manifest.parent_commit_ids != record.parent_commit_ids
            || manifest.commit_change_id != record.change_id
            || manifest.account_id != record.account_id
            || manifest.created_at != record.created_at
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "commit '{commit_id}' topology projection disagrees with commit-state authority"
                ),
            ));
        }
        generations.insert(*commit_id, manifest.generation);
    }
    let mut staged_parent_count = BTreeMap::<CommitId, usize>::new();
    let mut children = BTreeMap::<CommitId, Vec<CommitId>>::new();
    for commit in commit_rows {
        let mut count = 0;
        for parent in &commit.parent_commit_ids {
            if staged_commit_ids.contains(parent) {
                count += 1;
                children.entry(*parent).or_default().push(commit.commit_id);
            }
        }
        staged_parent_count.insert(commit.commit_id, count);
    }
    let mut ready = staged_parent_count
        .iter()
        .filter_map(|(commit_id, count)| (*count == 0).then_some(*commit_id))
        .collect::<BTreeSet<_>>();
    let commit_rows_by_id = commit_rows
        .iter()
        .map(|commit| (commit.commit_id, commit))
        .collect::<BTreeMap<_, _>>();
    while let Some(commit_id) = ready.pop_first() {
        let commit = commit_rows_by_id[&commit_id];
        let generation = commit
            .parent_commit_ids
            .iter()
            .filter_map(|parent| generations.get(parent).copied())
            .max()
            .map_or(Ok(0), |generation| {
                generation
                    .checked_add(1)
                    .ok_or_else(|| LixError::unknown("commit generation exceeds u64"))
            })?;
        generations.insert(commit_id, generation);
        for child in children.get(&commit_id).into_iter().flatten() {
            let remaining = staged_parent_count
                .get_mut(child)
                .expect("staged child has a parent count");
            *remaining -= 1;
            if *remaining == 0 {
                ready.insert(*child);
            }
        }
    }
    if staged_commit_ids
        .iter()
        .any(|commit_id| !generations.contains_key(commit_id))
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "staged commit graph contains a parent cycle",
        ));
    }
    let changes = state_rows
        .iter()
        // Ordinary untracked members are intentionally current-state only in
        // V16. `lix_branch_ref` is the one control-plane exception: its
        // published control retains a public ref_change_id, so that immutable
        // ledger fact must remain available to `lix_change` and GC even
        // though it is not a commit member.
        .filter(|row| row.untracked && row.schema_key == BRANCH_REF_SCHEMA_KEY)
        .map(|row| transaction_change_record_from_state_row(row, active_account_id))
        .chain(
            branch_head_changes
                .iter()
                .map(|change| Ok(TransactionChangeRecordRef::from(change))),
        )
        // Engine-owned untracked state follows the same current-only rule.
        .collect::<Result<Vec<_>, _>>()?;
    let mut staged = BTreeMap::<CommitId, StagedChangelogCommit>::new();
    for commit_row in commit_rows {
        let generation = generations[&commit_row.commit_id];
        let state_row_indices = tracked_row_indices_by_commit
            .get(&commit_row.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        validate_selected_change_refs(
            commit_row.commit_id,
            state_rows,
            state_row_indices,
            &commit_row.selected_change_batches,
        )?;
        for &row_index in state_row_indices {
            let row = state_rows.row(row_index);
            row.change_id.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked staged row is missing change_id before changelog append",
                )
            })?;
        }
        let record = CommitRecord {
            format_version: 1,
            commit_id: commit_row.commit_id,
            generation,
            parent_commit_ids: commit_row.parent_commit_ids.clone(),
            change_id: commit_row.change_id,
            account_id: active_account_id.to_string(),
            created_at: commit_row.created_at,
        };
        commits.push(record.clone());
        staged.insert(
            commit_row.commit_id,
            StagedChangelogCommit {
                record,
                selected_change_batches: commit_row.selected_change_batches.clone(),
            },
        );
    }

    let append = TransactionChangelogAppend { commits, changes };

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
    state_rows: &PreparedStateBatch,
    state_row_indices: &[RowIndex],
    selected_change_batches: &[StagedCommitChangeBatch],
) -> Result<(), LixError> {
    if selected_change_batches.is_empty() {
        return Ok(());
    }

    let mut identities = BTreeSet::new();
    for &row_index in state_row_indices {
        let row = state_rows.row(row_index);
        row.change_id.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked staged row is missing change_id before changelog append",
            )
        })?;
        if !identities.insert((
            row.schema_key.as_str(),
            row.file_id.map(crate::common::SharedStr::as_str),
            row.entity_pk,
        )) {
            return Err(LixError::unknown(format!(
                "commit '{commit_id}' has duplicate change ref key"
            )));
        }
    }
    for change_ref in selected_changes(selected_change_batches) {
        if !identities.insert((
            change_ref.schema_key(),
            change_ref.file_id(),
            change_ref.entity_pk(),
        )) {
            return Err(LixError::unknown(format!(
                "commit '{commit_id}' has duplicate change ref key"
            )));
        }
    }
    Ok(())
}

fn transaction_change_record_from_state_row<'a>(
    row: PreparedStateRowRef<'a>,
    active_account_id: &'a str,
) -> Result<TransactionChangeRecordRef<'a>, LixError> {
    let Some(change_id) = row.change_id.as_ref() else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "staged row is missing change_id before changelog change construction",
        ));
    };
    Ok(TransactionChangeRecordRef {
        format_version: 2,
        change_id: *change_id,
        account_id: active_account_id,
        entity_pk: row.entity_pk,
        schema_key: row.schema_key,
        file_id: row.file_id.map(crate::common::SharedStr::as_str),
        snapshot: row.snapshot.map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction::types::StageJson::slot_ref,
        ),
        metadata: row.metadata.map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction::types::StageJson::slot_ref,
        ),
        created_at: row.updated_at,
        origin_key: row.origin_key.map(crate::common::SharedStr::as_str),
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
fn branch_ref_change_record(
    root: &PendingTrackedRoot,
    active_account_id: &str,
) -> Result<ChangeRecord, LixError> {
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
    if snapshot.len() > JSON_INLINE_MAX_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!(
                "branch id is too long: its serialized branch ref is {} bytes, but the maximum is {} bytes",
                snapshot.len(),
                JSON_INLINE_MAX_BYTES,
            ),
        ));
    }
    Ok(ChangeRecord {
        format_version: 2,
        change_id: root.ref_change_id,
        account_id: active_account_id.to_string(),
        schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
        entity_pk: EntityPk::uuid_from_canonical(&root.branch_id).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("committed branch ID is not a canonical UUID: {error}"),
            )
        })?,
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
    active_account_id: &str,
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
            account_id: active_account_id.to_string(),
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
    row: PreparedStateRowRef<'_>,
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
    let created_at = row
        .durable_predecessor
        .map(crate::live_state::CertifiedCurrentStatePredecessor::created_at)
        .transpose()?
        .unwrap_or(row.created_at);
    Ok(TrackedStateDeltaRef {
        schema_key: row.schema_key,
        file_id: row.file_id.map(crate::common::SharedStr::as_str),
        entity_pk: row.entity_pk,
        change_id,
        commit_id,
        deleted: row.snapshot.is_none(),
        created_at,
        updated_at: row.updated_at,
    })
}

fn tracked_commit_delta_from_state_row(
    row: PreparedStateRowRef<'_>,
) -> Result<TrackedStateCommitDeltaRef<'_>, LixError> {
    Ok(TrackedStateCommitDeltaRef {
        delta: tracked_delta_from_state_row(row)?,
        snapshot: row.snapshot.map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction::types::StageJson::slot_ref,
        ),
        metadata: row.metadata.map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction::types::StageJson::slot_ref,
        ),
        origin_key: row.origin_key.map(crate::common::SharedStr::as_str),
        base_coordinate: None,
        authored: true,
    })
}

fn tracked_delta_from_certified_root_row(
    row: &MaterializedLiveStateRow,
) -> Result<TrackedStateDeltaRef<'_>, LixError> {
    Ok(TrackedStateDeltaRef {
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        entity_pk: &row.entity_pk,
        change_id: row.change_id.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified root row is missing change_id",
            )
        })?,
        commit_id: row.commit_id.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified root row is missing commit_id",
            )
        })?,
        deleted: row.deleted,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

fn tracked_commit_delta_from_certified_root_row<'a>(
    row: &'a MaterializedLiveStateRow,
    json_refs: &'a CertifiedRootJsonRefs,
) -> Result<TrackedStateCommitDeltaRef<'a>, LixError> {
    Ok(TrackedStateCommitDeltaRef {
        delta: tracked_delta_from_certified_root_row(row)?,
        snapshot: row.snapshot_content.as_ref().map_or(
            crate::json_store::JsonSlotRef::None,
            |snapshot| {
                json_refs.snapshot.as_ref().map_or(
                    crate::json_store::JsonSlotRef::Inline(snapshot.as_str()),
                    crate::json_store::JsonSlotRef::Ref,
                )
            },
        ),
        metadata: row
            .metadata
            .as_ref()
            .map_or(crate::json_store::JsonSlotRef::None, |metadata| {
                json_refs.metadata.as_ref().map_or(
                    crate::json_store::JsonSlotRef::Inline(metadata.as_str()),
                    crate::json_store::JsonSlotRef::Ref,
                )
            }),
        origin_key: None,
        base_coordinate: None,
        authored: true,
    })
}

fn tracked_commit_delta_from_selected_change_ref<'a>(
    change_ref: StagedCommitChangeRef<'a>,
    commit_id: CommitId,
    state: Option<&'a SelectedArrowState>,
) -> Result<TrackedStateCommitDeltaRef<'a>, LixError> {
    if state.and_then(|state| state.row.as_ref()).is_none() && !change_ref.deleted {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "live selected commit delta is missing its Arrow state row",
        ));
    }
    Ok(TrackedStateCommitDeltaRef {
        delta: TrackedStateDeltaRef {
            schema_key: change_ref.schema_key(),
            file_id: change_ref.file_id(),
            entity_pk: change_ref.entity_pk(),
            change_id: change_ref.change_id,
            commit_id,
            deleted: change_ref.deleted,
            created_at: change_ref.created_at,
            updated_at: change_ref.updated_at,
        },
        snapshot: state
            .and_then(|state| state.row.as_ref())
            .map_or(crate::json_store::JsonSlotRef::None, |row| {
                row.snapshot.as_ref_slot()
            }),
        metadata: state
            .and_then(|state| state.row.as_ref())
            .map_or(crate::json_store::JsonSlotRef::None, |row| {
                row.metadata.as_ref_slot()
            }),
        origin_key: state.and_then(|state| state.origin_key.as_deref()),
        base_coordinate: state.and_then(|state| state.coordinate),
        authored: false,
    })
}

fn current_state_delta_from_state_row(
    row: PreparedStateRowRef<'_>,
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
        schema_key: row.schema_key,
        file_id: row.file_id.map(crate::common::SharedStr::as_str),
        entity_pk: row.entity_pk,
        change_id: (!row.untracked).then_some(change_id),
        commit_id,
        untracked: row.untracked,
        deleted: row.snapshot.is_none(),
        created_at: row.created_at,
        updated_at: row.updated_at,
        snapshot: row.snapshot.map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction::types::StageJson::slot_ref,
        ),
        metadata: row.metadata.map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction::types::StageJson::slot_ref,
        ),
        columnar_base_coordinate: None,
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
        columnar_base_coordinate: None,
    }
}

/// Resolves selected merge/checkpoint members through their published Arrow
/// roots. Event storage contributes only compact authored provenance; every
/// snapshot and metadata slot comes from the immutable state leaf.
async fn load_selected_arrow_states(
    read: &(impl StorageAdapterRead + ?Sized),
    commit_rows: &[FinalizedCommitRow],
) -> Result<HashMap<SelectedChangeKey, SelectedArrowState>, LixError> {
    let mut by_source_commit = BTreeMap::<CommitId, Vec<StagedCommitChangeRef<'_>>>::new();
    for change_ref in commit_rows
        .iter()
        .flat_map(|commit| selected_changes(&commit.selected_change_batches))
    {
        by_source_commit
            .entry(change_ref.source_commit_id)
            .or_default()
            .push(change_ref);
    }

    let mut states = HashMap::new();
    for (source_commit_id, change_refs) in by_source_commit {
        let source = crate::tracked_state::load_published_commit_state_manifest(
            read,
            source_commit_id,
        )
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "selected changes reference missing Arrow source commit '{source_commit_id}'"
                ),
            )
        })?;
        let keys = change_refs
            .iter()
            .map(|change_ref| TrackedStateKey {
                schema_key: change_ref.schema_key().to_owned(),
                file_id: change_ref.file_id().map(str::to_owned),
                entity_pk: change_ref.entity_pk().clone(),
            })
            .collect::<Vec<_>>();
        let encoded_keys = keys
            .iter()
            .map(|key| {
                Bytes::from(encode_key_ref(TrackedStateKeyRef {
                    schema_key: &key.schema_key,
                    file_id: key.file_id.as_deref(),
                    entity_pk: &key.entity_pk,
                }))
            })
            .collect::<Vec<_>>();
        let coordinates = crate::tracked_state::load_complete_current_state_coordinates_encoded(
            read,
            &source,
            &encoded_keys,
        )
        .await?;
        let live_coordinates = change_refs
            .iter()
            .zip(&coordinates)
            .filter_map(|(change_ref, coordinate)| (!change_ref.deleted).then_some(*coordinate))
            .map(|coordinate| {
                coordinate.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "selected live change has no Arrow row in its source root",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let live_rows = crate::tracked_state::load_current_state_payloads_at_coordinates(
            read,
            &live_coordinates,
        )
        .await?;
        let mut live_rows = live_rows.into_iter();

        let live_change_ids = change_refs
            .iter()
            .filter_map(|change_ref| (!change_ref.deleted).then_some(change_ref.change_id))
            .collect::<Vec<_>>();
        let origins = load_change_origin_keys_by_ids(read, &live_change_ids).await?;
        let mut origins = origins.into_iter();
        for ((change_ref, coordinate), encoded_key) in
            change_refs.into_iter().zip(coordinates).zip(encoded_keys)
        {
            let row = (!change_ref.deleted)
                .then(|| live_rows.next().expect("one Arrow row per live selection"));
            if let Some(row) = row.as_ref()
                && (row.encoded_key != encoded_key
                    || row.value.change_id != change_ref.change_id
                    || row.value.deleted
                    || row.value.updated_at != change_ref.updated_at)
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "selected change '{}' does not match its Arrow source row",
                        change_ref.change_id
                    ),
                ));
            }
            let origin_key = (!change_ref.deleted)
                .then(|| {
                    origins
                        .next()
                        .expect("one provenance row per live selection")
                })
                .flatten();
            if change_ref.deleted && row.is_some() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "selected tombstone unexpectedly materialized a live Arrow payload",
                ));
            }
            let key = selected_change_key(change_ref);
            let state = SelectedArrowState {
                coordinate,
                row,
                origin_key,
            };
            if let Some(existing) = states.insert(key, state.clone())
                && existing != state
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "selected change '{}' resolves to conflicting source payloads",
                        change_ref.change_id
                    ),
                ));
            }
        }
        debug_assert!(live_rows.next().is_none());
        debug_assert!(origins.next().is_none());
    }
    Ok(states)
}

async fn materialize_selected_arrow_payloads(
    read: &(impl StorageAdapterRead + ?Sized),
    states: &HashMap<SelectedChangeKey, SelectedArrowState>,
) -> Result<HashMap<SelectedChangeKey, MaterializedSelectedArrowPayload>, LixError> {
    let ordered = states
        .iter()
        .filter_map(|(key, state)| state.row.as_ref().map(|row| (key.clone(), row)))
        .collect::<Vec<_>>();
    let mut refs = Vec::new();
    for (_, row) in &ordered {
        for slot in [&row.snapshot, &row.metadata] {
            if let crate::json_store::JsonSlot::Ref(reference) = slot {
                refs.push(*reference);
            }
        }
    }
    let mut values = JsonStoreContext::new()
        .load_bytes_many(
            read,
            crate::json_store::JsonLoadRequestRef {
                refs: &refs,
                scope: crate::json_store::JsonReadScopeRef::OutOfBand,
            },
        )
        .await?
        .into_values()
        .into_iter();
    let mut materialize = |slot: &crate::json_store::JsonSlot| -> Result<_, LixError> {
        match slot {
            crate::json_store::JsonSlot::None => Ok(None),
            crate::json_store::JsonSlot::Inline(json) => {
                Ok(Some(crate::common::SharedStr::from(json.clone())))
            }
            crate::json_store::JsonSlot::Ref(reference) => {
                let bytes = values.next().flatten().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "selected Arrow payload references missing JSON '{}'",
                            reference.to_hex()
                        ),
                    )
                })?;
                let actual = JsonRef::for_content(&bytes);
                if actual != *reference {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "selected Arrow payload JSON failed content-address verification",
                    ));
                }
                Ok(Some(crate::common::SharedStr::from_utf8(bytes).map_err(
                    |error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("selected Arrow payload JSON is not UTF-8: {error}"),
                        )
                    },
                )?))
            }
        }
    };
    let mut output = HashMap::with_capacity(ordered.len());
    for (key, row) in ordered {
        output.insert(
            key,
            MaterializedSelectedArrowPayload {
                snapshot_content: materialize(&row.snapshot)?,
                metadata: materialize(&row.metadata)?,
            },
        );
    }
    debug_assert!(values.next().is_none());
    Ok(output)
}

fn stage_certified_native_state_parts(
    writes: &mut StorageWriteSet,
    state_rows: &PreparedStateBatch,
    state_row_indices: &[RowIndex],
    commit_id: CommitId,
    entity_columnar_write_sets: &crate::live_state::EntityColumnarWriteSets,
) -> Result<Option<Vec<crate::tracked_state::CurrentStatePartDescriptor>>, LixError> {
    if state_row_indices.is_empty() {
        return Ok(None);
    }
    let mut ordered_indices = state_row_indices.to_vec();
    ordered_indices.sort_unstable_by(|&left, &right| {
        let left = state_rows.row(left);
        let right = state_rows.row(right);
        (left.schema_key, left.file_id, left.entity_pk).cmp(&(
            right.schema_key,
            right.file_id,
            right.entity_pk,
        ))
    });
    let first = state_rows.row(ordered_indices[0]);
    if first.file_id.is_some()
        || first.untracked
        || first.snapshot.is_none()
        || ordered_indices.iter().any(|&row_index| {
            let row = state_rows.row(row_index);
            row.commit_id != Some(commit_id)
                || row.schema_key != first.schema_key
                || row.file_id.is_some()
                || row.untracked
                || row.snapshot.is_none()
        })
    {
        return Ok(None);
    }
    let Some(encoded) = entity_columnar_write_sets.get_unfiled(commit_id, first.schema_key) else {
        return Ok(None);
    };
    if encoded.manifest.namespace != crate::sql2::ENTITY_ARROW_STATE_NAMESPACE {
        return Ok(None);
    }
    // The persistent state root owns publication. HOT may reference the same
    // content later, but content-idempotent staging cannot create a second
    // physical owner or conflicting mutation.
    let state_set_id = crate::columnar_row_group::stage_row_group_set(writes, encoded)?;
    let mut refs = ordered_indices
        .iter()
        .flat_map(|&row_index| {
            let row = state_rows.row(row_index);
            [row.snapshot, row.metadata]
        })
        .filter_map(
            |slot| match slot.map(crate::transaction::types::StageJson::slot_ref) {
                Some(crate::json_store::JsonSlotRef::Ref(reference)) => {
                    Some(*reference.as_hash_array())
                }
                Some(
                    crate::json_store::JsonSlotRef::None
                    | crate::json_store::JsonSlotRef::Inline(_),
                )
                | None => None,
            },
        )
        .collect::<Vec<_>>();
    let payload_refs_digest =
        crate::tracked_state::stage_current_state_ref_summary(writes, state_set_id, &mut refs)?;
    let mut offset = 0usize;
    let mut descriptors = Vec::with_capacity(encoded.manifest.groups.len());
    for (group_index, group) in encoded.manifest.groups.iter().enumerate() {
        let row_count = usize::try_from(group.row_count).expect("u32 fits usize");
        let rows = &ordered_indices[offset..offset + row_count];
        let first_key = encode_key_ref(TrackedStateKeyRef {
            schema_key: state_rows.row(rows[0]).schema_key,
            file_id: None,
            entity_pk: state_rows.row(rows[0]).entity_pk,
        });
        let last_key = encode_key_ref(TrackedStateKeyRef {
            schema_key: state_rows
                .row(*rows.last().expect("nonempty group"))
                .schema_key,
            file_id: None,
            entity_pk: state_rows
                .row(*rows.last().expect("nonempty group"))
                .entity_pk,
        });
        descriptors.push(crate::tracked_state::CurrentStatePartDescriptor {
            first_key,
            last_key,
            state_set_id,
            state_group_index: u32::try_from(group_index)
                .map_err(|_| LixError::unknown("native state group index exceeds u32"))?,
            payload_refs_digest,
            row_count: u16::try_from(row_count)
                .map_err(|_| LixError::unknown("native state group row count exceeds u16"))?,
        });
        offset += row_count;
    }
    if offset != ordered_indices.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native Arrow state groups do not cover their certified input",
        ));
    }
    Ok(Some(descriptors))
}

async fn stage_tracked_commit_delta_index(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &mut PreparedStateBatch,
    entity_columnar_write_sets: &crate::live_state::EntityColumnarWriteSets,
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_roots: &[PendingTrackedRoot],
    commit_rows: &[FinalizedCommitRow],
    selected_arrow_states: &HashMap<SelectedChangeKey, SelectedArrowState>,
    certified_packet_root_rows: &BTreeMap<CommitId, Vec<MaterializedLiveStateRow>>,
    certified_packet_json_refs: &BTreeMap<CommitId, Vec<CertifiedRootJsonRefs>>,
    insert_selection: &PreparedInsertSelection,
    replacement_generations: &BTreeMap<CommitId, CommitDeltaReplacementGeneration>,
    ordered_replacements: &BTreeMap<CommitId, Arc<OrderedMutationJournal>>,
) -> Result<StagedCommitDeltaIndex, LixError> {
    let mut ordered_addressable_commits = BTreeSet::new();
    let mut inventories = BTreeMap::new();
    let commit_rows = commit_rows
        .iter()
        .map(|commit| (commit.commit_id, commit))
        .collect::<BTreeMap<_, _>>();
    for root in tracked_roots {
        if let Some(journal) = ordered_replacements.get(&root.commit_id) {
            #[cfg(test)]
            {
                let mut counts = DIRECT_JOURNAL_REPLACEMENT_PUBLICATIONS
                    .lock()
                    .expect("direct journal publication counters");
                let count = counts.entry(journal.schema_key().to_owned()).or_default();
                *count = count.saturating_add(1);
            }
            if !state_rows.is_empty()
                || certified_packet_root_rows
                    .get(&root.commit_id)
                    .is_some_and(|rows| !rows.is_empty())
                || commit_rows
                    .get(&root.commit_id)
                    .is_some_and(|commit| !commit.selected_change_batches.is_empty())
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable replacement journal overlaps another commit payload lane",
                ));
            }
            let generation = replacement_generations
                .get(&root.commit_id)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "immutable replacement journal has no lifecycle generation",
                    )
                })?;
            stage_complete_replacement_arrow_authority(
                writes,
                entity_columnar_write_sets,
                root.commit_id,
                generation,
            )?;
            let created_at = generation.lifecycle_summary.uniform_created_at;
            let columnar_key = (root.commit_id, journal.schema_key().to_owned());
            let stage = crate::tracked_state::stage_ordered_addressable_replacement_parts(
                writes,
                journal.iter().enumerate().map(|(row_index, row)| {
                    Ok(TrackedStateSingleStringReplacementRef {
                        schema_key: journal.schema_key(),
                        file_id: None,
                        entity_pk: row.identity(),
                        commit_id: root.commit_id,
                        created_at,
                        updated_at: journal.timestamp(),
                        snapshot: row.snapshot_slot(),
                        metadata: crate::json_store::JsonSlotRef::None,
                        base_coordinate: entity_columnar_write_sets
                            .replacement_row_location(&columnar_key, row_index),
                    })
                }),
                generation,
            )?;
            inventories.insert(root.commit_id, stage.mutation_inventory().clone());
            ordered_addressable_commits.insert(root.commit_id);
            continue;
        }
        let state_row_indices = tracked_row_indices_by_commit
            .get(&root.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let certified_root_rows = certified_packet_root_rows
            .get(&root.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let certified_root_json_refs = certified_packet_json_refs
            .get(&root.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if certified_root_rows.len() != certified_root_json_refs.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified root JSON placement does not match materialized rows",
            ));
        }
        let staged = commit_rows.get(&root.commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked commit '{}' has no changelog facts for commit-delta staging",
                    root.commit_id
                ),
            )
        })?;
        let can_stream_ordered_addressable = certified_root_rows.is_empty()
            && !state_row_indices.is_empty()
            && staged.selected_change_batches.is_empty()
            && state_row_indices
                .iter()
                .all(|&row_index| state_rows.row(row_index).addressable_change_id);
        if replacement_generations.contains_key(&root.commit_id) && !can_stream_ordered_addressable
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "complete replacement generation cannot use generic commit-delta staging",
            ));
        }
        if can_stream_ordered_addressable {
            let replacement_generation = replacement_generations.get(&root.commit_id);
            let lifecycle_created_at = replacement_generation
                .map(|generation| generation.lifecycle_summary.uniform_created_at);
            let publish_lifecycle_summary = replacement_generation.is_some()
                || state_row_indices.iter().all(|&row_index| {
                    tracked_row_requires_absence(
                        row_index,
                        state_rows.row(row_index),
                        insert_selection,
                    )
                });
            let native_descriptors =
                if replacement_generation.is_none() && publish_lifecycle_summary {
                    stage_certified_native_state_parts(
                        writes,
                        state_rows,
                        state_row_indices,
                        root.commit_id,
                        entity_columnar_write_sets,
                    )?
                } else {
                    None
                };
            let ordered_stage = {
                let _span = tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.materialization.commit_delta.ordered_stream",
                    row_count = state_row_indices.len()
                )
                .entered();
                let state_rows = &*state_rows;
                let make_delta = |row_index| {
                    let row = state_rows.row(row_index);
                    let mut delta = tracked_commit_delta_from_state_row(row)?;
                    if let Some(created_at) = lifecycle_created_at {
                        delta.delta.created_at = created_at;
                    }
                    delta.base_coordinate =
                        entity_columnar_write_sets.state_row_location(row_index);
                    Ok(delta)
                };
                let order_certified = state_rows.certified_tracked_keys_strictly_ordered()
                    && state_row_indices.len() == state_rows.len()
                    && state_row_indices
                        .iter()
                        .enumerate()
                        .all(|(index, &row_index)| index == row_index);
                let replacement_stage = if let Some(generation) = replacement_generation {
                    if !order_certified {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "complete replacement generation is not in canonical identity order",
                        ));
                    }
                    stage_complete_replacement_arrow_authority(
                        writes,
                        entity_columnar_write_sets,
                        root.commit_id,
                        generation,
                    )?;
                    Some(
                        crate::tracked_state::stage_ordered_addressable_replacement_parts(
                            writes,
                            state_row_indices
                                .iter()
                                .map(|&row_index| make_delta(row_index)),
                            generation,
                        )?,
                    )
                } else {
                    None
                };
                match replacement_stage {
                    Some(stage) => Some(stage),
                    None => {
                        let deltas = state_row_indices
                            .iter()
                            .map(|&row_index| make_delta(row_index));
                        if native_descriptors.is_some() {
                            stage_ordered_arrow_native_commit_deltas(
                                writes,
                                deltas,
                                order_certified,
                                publish_lifecycle_summary,
                            )?
                        } else {
                            stage_ordered_addressable_commit_deltas(
                                writes,
                                deltas,
                                order_certified,
                                publish_lifecycle_summary,
                            )?
                        }
                    }
                }
            };
            if let Some(ordered_stage) = ordered_stage {
                let mut inventory = ordered_stage.mutation_inventory().clone();
                if let Some(descriptors) = native_descriptors {
                    let first = state_rows.row(state_row_indices[0]);
                    inventory.single_partition = Some(CommitDeltaReplacementScope {
                        schema_key: first.schema_key.to_string(),
                        file_id: first.file_id.map(ToString::to_string),
                    });
                    inventory.sealed_state_parts = descriptors;
                }
                inventories.insert(root.commit_id, inventory);
                state_rows.set_ordered_addressable_change_ids(state_row_indices, ordered_stage)?;
                ordered_addressable_commits.insert(root.commit_id);
                continue;
            }
        }
        let mut deltas = Vec::with_capacity(
            state_row_indices.len()
                + certified_root_rows.len()
                + selected_change_count(&staged.selected_change_batches),
        );
        let mut addressable = Vec::with_capacity(deltas.capacity());
        let mut selected_members_by_source = BTreeMap::<CommitId, usize>::new();
        for &row_index in state_row_indices {
            let row = state_rows.row(row_index);
            addressable.push(row.addressable_change_id);
            let mut delta = tracked_commit_delta_from_state_row(row)?;
            delta.base_coordinate = entity_columnar_write_sets.state_row_location(row_index);
            deltas.push(delta);
        }
        for (row, json_refs) in certified_root_rows.iter().zip(certified_root_json_refs) {
            let mut delta = tracked_commit_delta_from_certified_root_row(row, json_refs)?;
            let scope = CommitDeltaReplacementScope {
                schema_key: row.schema_key.clone(),
                file_id: row.file_id.clone(),
            };
            let encoded_key = encode_key_ref(TrackedStateKeyRef {
                schema_key: &row.schema_key,
                file_id: row.file_id.as_deref(),
                entity_pk: &row.entity_pk,
            });
            delta.base_coordinate = entity_columnar_write_sets.addressed_row_location(
                root.commit_id,
                &scope,
                &encoded_key,
            );
            if !delta.delta.deleted && delta.base_coordinate.is_none() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "certified live event has no Arrow state coordinate",
                ));
            }
            deltas.push(delta);
            // Certified rows are addressed by commit plus identity. They do
            // not need standalone change locators in the public ledger.
            addressable.push(false);
        }
        for change_ref in selected_changes(&staged.selected_change_batches) {
            *selected_members_by_source
                .entry(change_ref.source_commit_id)
                .or_default() += 1;
            let key = selected_change_key(change_ref);
            let state = selected_arrow_states.get(&key);
            let delta =
                tracked_commit_delta_from_selected_change_ref(change_ref, root.commit_id, state)?;
            deltas.push(delta);
            addressable.push(false);
        }
        if !selected_members_by_source.is_empty() {
            tracing::info!(
                target: "lix_perf",
                commit_id = %root.commit_id,
                selected_members = selected_members_by_source.values().sum::<usize>(),
                selected_source_commits = selected_members_by_source.len(),
                dominant_selected_source_members = selected_members_by_source
                    .values()
                    .copied()
                    .max()
                    .unwrap_or_default(),
                "lix.perf.commit_delta_selected_sources"
            );
        }
        let is_checkpoint_commit = state_row_indices.iter().any(|&row_index| {
            state_rows.row(row_index).schema_key.as_str() == CHECKPOINT_MARKER_SCHEMA_KEY
        });
        let selected_source_alias = if certified_root_rows.is_empty()
            && is_checkpoint_commit
            && selected_members_by_source.len() == 1
        {
            let source_commit_id = *selected_members_by_source
                .first_key_value()
                .expect("one selected source exists")
                .0;
            let source = crate::tracked_state::load_commit_delta_selection_certificate(
                read,
                source_commit_id,
            )
            .await?;
            source
                .is_some_and(|source| {
                    if source.selected_source_commit_id.is_some()
                        || usize::try_from(source.member_count).ok()
                            != Some(selected_change_count(&staged.selected_change_batches))
                    {
                        return false;
                    }
                    if source.direct_segment_row_counts.is_empty() {
                        let selected = selected_changes(&staged.selected_change_batches)
                            .map(|change_ref| {
                                (
                                    encode_key_ref(TrackedStateKeyRef {
                                        schema_key: change_ref.schema_key(),
                                        file_id: change_ref.file_id(),
                                        entity_pk: change_ref.entity_pk(),
                                    }),
                                    (
                                        change_ref.change_id,
                                        change_ref.deleted,
                                        change_ref.created_at,
                                        change_ref.updated_at,
                                    ),
                                )
                            })
                            .collect::<HashMap<_, _>>();
                        return usize::try_from(source.member_count).ok() == Some(selected.len())
                            && source.selection_fingerprint
                                == crate::tracked_state::selected_change_selection_fingerprint(
                                    selected.iter().map(
                                        |(key, (change_id, deleted, created_at, updated_at))| {
                                            (
                                                key.as_slice(),
                                                *change_id,
                                                *deleted,
                                                *created_at,
                                                *updated_at,
                                            )
                                        },
                                    ),
                                );
                    }
                    dense_selected_source_is_exact(
                        source_commit_id,
                        &source.direct_segment_row_counts,
                        staged
                            .selected_change_batches
                            .iter()
                            .all(StagedCommitChangeBatch::source_membership_certified),
                        selected_changes(&staged.selected_change_batches),
                    )
                })
                .then_some(source_commit_id)
        } else {
            None
        };
        let authored_change_ids = state_row_indices
            .iter()
            .filter_map(|&row_index| {
                let row = state_rows.row(row_index);
                (!row.addressable_change_id)
                    .then_some(row.change_id)
                    .flatten()
            })
            .collect::<std::collections::HashSet<_>>();
        let staged = if let Some(source_commit_id) = selected_source_alias {
            deltas.truncate(state_row_indices.len());
            addressable.truncate(state_row_indices.len());
            crate::tracked_state::stage_addressable_commit_deltas_with_selected_source(
                writes,
                &deltas,
                &addressable,
                source_commit_id,
            )?
        } else {
            stage_addressable_commit_deltas(writes, &deltas, &addressable)?
        };
        inventories.insert(root.commit_id, staged.mutation_inventory().clone());
        drop(deltas);
        let assigned_change_ids = staged
            .assigned_change_ids
            .iter()
            .copied()
            .filter(|change_id| *change_id != ChangeId::default())
            .collect::<std::collections::HashSet<_>>();
        for (source_index, &row_index) in state_row_indices.iter().enumerate() {
            if !state_rows.row(row_index).addressable_change_id {
                continue;
            }
            let change_id = staged.assigned_change_ids[source_index];
            if change_id == ChangeId::default() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "addressable tracked row received no commit-delta address",
                ));
            }
            state_rows.set_change_id(row_index, Some(change_id));
        }
        let authored_locators = staged
            .locators
            .into_iter()
            .filter(|locator| {
                authored_change_ids.contains(&locator.change_id)
                    || assigned_change_ids.contains(&locator.change_id)
            })
            .collect::<Vec<_>>();
        stage_change_locators(writes, &authored_locators);
    }
    let mut planned_members = BTreeMap::new();
    for (&commit_id, inventory) in &inventories {
        planned_members.insert(
            commit_id,
            crate::tracked_state::staged_commit_delta_members_for_write(
                read, writes, commit_id, inventory,
            )
            .await?,
        );
    }
    Ok(StagedCommitDeltaIndex {
        ordered_addressable_commits,
        inventories,
        planned_members,
    })
}

fn stage_complete_replacement_arrow_authority(
    writes: &mut StorageWriteSet,
    entity_columnar_write_sets: &crate::live_state::EntityColumnarWriteSets,
    commit_id: CommitId,
    generation: &CommitDeltaReplacementGeneration,
) -> Result<crate::columnar_row_group::ArrowStateSetId, LixError> {
    let encoded = entity_columnar_write_sets
        .get_scope(commit_id, &generation.scope)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "complete replacement omitted its canonical Arrow authority",
            )
        })?;
    crate::columnar_row_group::stage_row_group_set(writes, encoded)
}

struct StagedHotHeads {
    controls: BTreeMap<String, BranchHeadControl>,
}

/// Returns the commit snapshots that must be materialized before publication.
/// Normal serial commits and entity-only selected refs stay on the current generation.
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

/// Publishes committed tracked state exclusively through its immutable Arrow
/// root. Each tracked head advance rotates to a fresh serving generation whose
/// HOT rows contain only history-free workspace state. The root pointer and
/// branch control are the complete tracked serving handoff; no tracked
/// post-image or packed commit payload is copied into mutable HOT storage.
#[allow(clippy::too_many_arguments)]
async fn stage_arrow_root_backed_heads(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &PreparedStateBatch,
    _entity_columnar_write_sets: &crate::live_state::EntityColumnarWriteSets,
    engine_rows: &[EngineCurrentRow],
    _tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_roots: &[PendingTrackedRoot],
    staged_commits: &BTreeMap<CommitId, StagedChangelogCommit>,
    _selected_change_payloads: &HashMap<SelectedChangeKey, MaterializedSelectedArrowPayload>,
    insert_selection: &PreparedInsertSelection,
    _certified_fresh_plugin_file_id: Option<&str>,
    _host_certified_file_schemas: &BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    _host_certified_live_increments: &BTreeMap<String, BTreeMap<(String, Option<String>), u64>>,
    explicit_branch_targets: &BTreeMap<String, ExplicitBranchHeadTarget>,
    observations: &BTreeMap<String, BranchHeadControlObservation>,
    checkpoint_epochs: &BTreeMap<String, CommitId>,
    _ordered_addressable_commits: &BTreeSet<CommitId>,
    _replacement_generation_commits: &BTreeSet<CommitId>,
    _ordered_replacements: &BTreeMap<CommitId, Arc<OrderedMutationJournal>>,
) -> Result<StagedHotHeads, LixError> {
    let tracked_head = TrackedHeadContext::new();
    let mut controls = BTreeMap::new();

    for root in tracked_roots_parent_first(tracked_roots)?
        .into_iter()
        .filter(|root| root.publish_head)
    {
        let staged = staged_commits.get(&root.commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "Arrow root head for commit '{}' has no staged changelog facts",
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
                        "missing branch-control observation for Arrow root publication on '{}'",
                        root.branch_id
                    ),
                )
            })?
            .control;

        reject_tracked_writes_over_untracked_rows(
            read,
            &root.branch_id,
            parent_control,
            &staged.selected_change_batches,
            state_rows,
            engine_rows,
        )
        .await?;

        let untracked_deltas = state_rows
            .iter()
            .filter(|row| {
                row.untracked
                    && row.branch_id.as_str() == root.branch_id
                    && row.schema_key != BRANCH_REF_SCHEMA_KEY
            })
            .map(current_state_delta_from_state_row)
            .chain(
                engine_rows
                    .iter()
                    .filter(|row| row.branch_id == root.branch_id)
                    .map(|row| Ok(current_state_delta_from_engine_row(row))),
            )
            .collect::<Result<Vec<_>, LixError>>()?;
        let untracked_absence_guards = state_rows
            .iter()
            .enumerate()
            .filter(|(row_index, row)| {
                row.untracked
                    && row.branch_id.as_str() == root.branch_id
                    && row.schema_key != BRANCH_REF_SCHEMA_KEY
                    && row.snapshot.is_some()
                    && insert_selection.contains(*row_index)
            })
            .map(|(_, row)| TrackedStateKey {
                schema_key: row.schema_key.to_string(),
                file_id: row.file_id.map(ToString::to_string),
                entity_pk: row.entity_pk.clone(),
            })
            .collect::<BTreeSet<_>>();

        let generation = lifecycle_generation(&root.branch_id, root.commit_id, root.ref_change_id);
        let checkpoint_commit_id = checkpoint_epochs.get(&root.branch_id).copied();
        let (_, untracked_schema_keys) = tracked_head
            .writer(read, writes)
            .stage_complete_current_state(
                &root.branch_id,
                generation,
                HotTrackedSnapshot::default(),
                parent_control.map(|control| control.generation),
                &[],
                &untracked_deltas,
                &untracked_absence_guards,
            )
            .await?;
        tracked_head.writer(read, writes).stage_root_current_base(
            &root.branch_id,
            generation,
            root.commit_id,
        );

        let mut control =
            normal_branch_head_control(root, parent_control, generation, checkpoint_commit_id)?;
        // Root-backed reads answer schema presence from the canonical catalog.
        // A conservative bloom avoids manufacturing a second schema inventory.
        control.schema_presence_bloom = [u64::MAX; 4];
        control.note_schemas(untracked_schema_keys.iter().map(String::as_str));
        insert_direct_branch_control(&mut controls, &root.branch_id, control)?;
    }

    // History-free-only transactions retain their generation and update only
    // HOT workspace rows. Explicit branch moves are handled by the existing
    // root-backed branch publication path below.
    for (branch_id, observation) in observations {
        if controls.contains_key(branch_id) || explicit_branch_targets.contains_key(branch_id) {
            continue;
        }
        let branch_has_untracked_rows =
            state_rows.iter().any(|row| {
                row.untracked
                    && row.branch_id.as_str() == branch_id
                    && row.schema_key != BRANCH_REF_SCHEMA_KEY
            }) || engine_rows.iter().any(|row| row.branch_id == *branch_id);
        if !branch_has_untracked_rows {
            continue;
        }
        let control = observation.control.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("untracked publication requires an existing branch '{branch_id}'"),
            )
        })?;
        let deltas = state_rows
            .iter()
            .filter(|row| {
                row.untracked
                    && row.branch_id.as_str() == branch_id
                    && row.schema_key != BRANCH_REF_SCHEMA_KEY
            })
            .map(current_state_delta_from_state_row)
            .chain(
                engine_rows
                    .iter()
                    .filter(|row| row.branch_id == *branch_id)
                    .map(|row| Ok(current_state_delta_from_engine_row(row))),
            )
            .collect::<Result<Vec<_>, LixError>>()?;
        let absence_guards =
            tracked_head_absence_guards(state_rows, insert_selection, branch_id, None);
        let mut writer = tracked_head.writer(read, writes);
        if absence_guards.is_empty() {
            writer
                .stage_current_state(
                    branch_id,
                    Some(control.generation),
                    control.head_commit_id,
                    &deltas,
                    &BTreeSet::new(),
                    None,
                    None,
                )
                .await?;
        } else {
            writer
                .stage_validated_insert_current_state(
                    branch_id,
                    Some(control.generation),
                    control.head_commit_id,
                    &deltas,
                    &absence_guards,
                    None,
                    None,
                    None,
                )
                .await?;
        }
        let mut next = control.next_current_state_revision()?;
        next.note_schemas(deltas.iter().map(|delta| delta.schema_key));
        insert_direct_branch_control(&mut controls, branch_id, next)?;
    }

    Ok(StagedHotHeads { controls })
}

/// bitmap filters that scope without rebuilding any prepared row identity.
fn tracked_head_absence_guards<'a>(
    state_rows: &'a PreparedStateBatch,
    insert_selection: &PreparedInsertSelection,
    branch_id: &str,
    certified_fresh_plugin_file_id: Option<&str>,
) -> Vec<TrackedStateKeyRef<'a>> {
    if insert_selection.is_empty() {
        return Vec::new();
    }

    let mut guards = state_rows
        .iter()
        .enumerate()
        .filter(|(row_index, row)| {
            insert_selection.contains(*row_index)
                && row.branch_id == branch_id
                && row.schema_key != BRANCH_REF_SCHEMA_KEY
                && row.snapshot.is_some()
                && !certified_fresh_plugin_file_id.is_some_and(|file_id| {
                    row.file_id.map(crate::common::SharedStr::as_str) == Some(file_id)
                })
        })
        .map(|(_, row)| TrackedStateKeyRef {
            schema_key: row.schema_key,
            file_id: row.file_id.map(crate::common::SharedStr::as_str),
            entity_pk: row.entity_pk,
        })
        .collect::<Vec<_>>();
    guards.sort_unstable_by(|left, right| {
        left.schema_key
            .cmp(right.schema_key)
            .then_with(|| left.entity_pk.cmp(right.entity_pk))
            .then_with(|| left.file_id.cmp(&right.file_id))
    });
    guards
}

async fn reject_tracked_writes_over_untracked_rows(
    read: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    control: Option<BranchHeadControl>,
    selected_change_batches: &[StagedCommitChangeBatch],
    state_rows: &PreparedStateBatch,
    engine_rows: &[EngineCurrentRow],
) -> Result<(), LixError> {
    let selected_identities = selected_changes(selected_change_batches)
        .map(|change_ref| TrackedStateKey {
            schema_key: change_ref.schema_key().to_owned(),
            file_id: change_ref.file_id().map(str::to_owned),
            entity_pk: change_ref.entity_pk().clone(),
        })
        .collect::<BTreeSet<_>>();
    let mut tracked_identities = selected_identities.clone();
    tracked_identities.extend(
        state_rows
            .iter()
            .filter(|row| {
                !row.untracked
                    && row.branch_id == branch_id
                    && row.schema_key != BRANCH_REF_SCHEMA_KEY
            })
            .map(|row| TrackedStateKey {
                schema_key: row.schema_key.to_string(),
                file_id: row.file_id.map(ToString::to_string),
                entity_pk: row.entity_pk.clone(),
            }),
    );
    if tracked_identities.is_empty() {
        return Ok(());
    }

    let tracked_keys = tracked_identities.iter().cloned().collect::<Vec<_>>();
    let mut untracked_identities = if let Some(control) = control {
        TrackedHeadContext::new()
            .reader(read)
            .load_projected_live_rows(
                branch_id,
                control,
                &tracked_keys,
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

    let Some(identity) = tracked_identities
        .iter()
        .find(|identity| untracked_identities.contains(*identity))
    else {
        return Ok(());
    };
    if selected_identities.contains(identity) {
        return Err(selected_tracked_ref_untracked_collision_error(
            branch_id, identity,
        ));
    }
    Err(normal_tracked_untracked_collision_error(
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
    state_rows: &PreparedStateBatch,
    engine_rows: &[EngineCurrentRow],
) {
    for row in state_rows.iter().filter(|row| {
        row.untracked && row.branch_id == branch_id && row.schema_key != BRANCH_REF_SCHEMA_KEY
    }) {
        let identity = TrackedStateKey {
            schema_key: row.schema_key.to_string(),
            file_id: row.file_id.map(ToString::to_string),
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

fn normal_tracked_untracked_collision_error(
    branch_id: &str,
    identity: &TrackedStateKey,
) -> LixError {
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "cannot change retention for an existing untracked row on branch '{branch_id}' for schema '{}' entity_pk {:?}",
            identity.schema_key, identity.entity_pk
        ),
    )
    .with_hint("Delete the untracked row before inserting the tracked identity.")
    .with_details(serde_json::json!({
        "kind": "trackedUntrackedIdentityCollision",
        "branchId": branch_id,
        "schemaKey": &identity.schema_key,
        "entityPk": &identity.entity_pk,
        "fileId": &identity.file_id,
    }))
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
        schema_presence_bloom: previous.map_or([0; 4], |control| control.schema_presence_bloom),
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
async fn stage_root_backed_branch_publication(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    head_commit_id: CommitId,
    target: &ExplicitBranchHeadTarget,
    existing: Option<BranchHeadControl>,
    state_rows: &PreparedStateBatch,
    engine_rows: &[EngineCurrentRow],
    insert_selection: &PreparedInsertSelection,
) -> Result<BranchHeadControl, LixError> {
    let generation = lifecycle_generation(branch_id, head_commit_id, target.ref_change_id);
    let current_state_revision = match existing {
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
    let tracked_head = TrackedHeadContext::new();
    tracked_head.writer(read, writes).stage_root_current_base(
        branch_id,
        generation,
        head_commit_id,
    );
    let mut control = BranchHeadControl {
        head_commit_id,
        generation,
        current_state_revision,
        working_diff_checkpoint_commit_id: None,
        created_at: existing.map_or(target.created_at, |control| control.created_at),
        updated_at: target.updated_at,
        ref_change_id: target.ref_change_id,
        // Root reads answer schema presence directly. Keep the bloom
        // conservative until immutable roots carry schema summaries.
        schema_presence_bloom: [u64::MAX; 4],
    };
    let untracked_deltas = state_rows
        .iter()
        .filter(|row| {
            row.untracked
                && row.branch_id.as_str() == branch_id
                && row.schema_key != BRANCH_REF_SCHEMA_KEY
        })
        .map(current_state_delta_from_state_row)
        .chain(
            engine_rows
                .iter()
                .filter(|row| row.branch_id == branch_id)
                .map(|row| Ok(current_state_delta_from_engine_row(row))),
        )
        .collect::<Result<Vec<_>, _>>()?;
    if existing.is_some() || !untracked_deltas.is_empty() {
        let absence_guards = state_rows
            .iter()
            .enumerate()
            .filter(|(row_index, row)| {
                row.untracked
                    && row.branch_id.as_str() == branch_id
                    && row.schema_key != BRANCH_REF_SCHEMA_KEY
                    && row.snapshot.is_some()
                    && insert_selection.contains(*row_index)
            })
            .map(|(_, row)| TrackedStateKey {
                schema_key: row.schema_key.to_string(),
                file_id: row.file_id.map(ToString::to_string),
                entity_pk: row.entity_pk.clone(),
            })
            .collect();
        let (_, schemas) = tracked_head
            .writer(read, writes)
            .stage_complete_current_state(
                branch_id,
                generation,
                HotTrackedSnapshot::default(),
                existing.map(|control| control.generation),
                &[],
                &untracked_deltas,
                &absence_guards,
            )
            .await?;
        control.note_schemas(schemas.iter().map(String::as_str));
    }
    Ok(control)
}

#[allow(clippy::too_many_arguments)]
async fn stage_branch_head_control_publications(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    normal_controls: &BTreeMap<String, BranchHeadControl>,
    state_rows: &PreparedStateBatch,
    engine_rows: &[EngineCurrentRow],
    explicit_branch_targets: &BTreeMap<String, ExplicitBranchHeadTarget>,
    insert_selection: &PreparedInsertSelection,
    checkpoint_publications: &[crate::gc::CheckpointPublication],
    preconditions: &mut Vec<StoragePrecondition>,
    observations: &BTreeMap<String, BranchHeadControlObservation>,
) -> Result<BTreeMap<String, BranchHeadControl>, LixError> {
    let checkpoint_epochs = checkpoint_epoch_bindings(checkpoint_publications)?;
    let mut publications = normal_controls
        .iter()
        .map(|(branch_id, control)| (branch_id.clone(), Some(*control)))
        .collect::<BTreeMap<String, Option<BranchHeadControl>>>();
    for (branch_id, target) in explicit_branch_targets {
        if publications.contains_key(branch_id) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "cannot publish an explicit branch ref and a normal tracked commit for branch '{branch_id}' in one transaction"
                ),
            ));
        }
        let existing = observations
            .get(branch_id)
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
            Some(head_commit_id) => Some(
                Box::pin(stage_root_backed_branch_publication(
                    read,
                    writes,
                    branch_id,
                    head_commit_id,
                    target,
                    existing,
                    state_rows,
                    engine_rows,
                    insert_selection,
                ))
                .await?,
            ),
        };
        publications.insert(branch_id.clone(), desired);
    }

    if publications.is_empty() {
        if !checkpoint_epochs.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "checkpoint epoch publication has no corresponding branch-control publication",
            ));
        }
        return Ok(BTreeMap::new());
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
    Ok(publications
        .into_iter()
        .filter_map(|(branch_id, control)| control.map(|control| (branch_id, control)))
        .collect())
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ExplicitBranchHeadTarget {
    head_commit_id: Option<CommitId>,
    ref_change_id: ChangeId,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
}

fn release_validated_canonical_value_columns(state_rows: &mut PreparedStateBatch) {
    state_rows.release_validated_canonical_value_columns();
}

fn prepare_entity_columnar_write_sets(
    state_rows: &PreparedStateBatch,
    entity_schema_catalog: Option<&crate::catalog::CatalogSnapshot>,
) -> Result<crate::live_state::EntityColumnarWriteSets, LixError> {
    if let Some((commit_id, schema_key, snapshots)) = state_rows.dense_entity_columnar_input() {
        let Some(schema) = entity_schema_catalog.and_then(|catalog| catalog.schema(schema_key))
        else {
            return Ok(crate::live_state::EntityColumnarWriteSets::new());
        };
        let Ok(spec) = crate::sql2::derive_entity_surface_spec_from_schema(schema) else {
            return Ok(crate::live_state::EntityColumnarWriteSets::new());
        };
        let rows =
            state_rows
                .iter()
                .zip(snapshots)
                .enumerate()
                .map(
                    |(row_index, (row, snapshot))| crate::sql2::EntityColumnarRowRef {
                        entity_pk: row.entity_pk,
                        snapshot_bytes: snapshot.normalized().as_bytes(),
                        snapshot_value: Some(snapshot.value()),
                        authority: Some(crate::sql2::EntityColumnarAuthorityRef {
                            value: TrackedStateIndexValueRef {
                                change_id: crate::tracked_state::addressable_change_id(
                                    commit_id,
                                    row_index / 512,
                                    row_index % 512,
                                )
                                .expect("dense certified Arrow state address"),
                                commit_id,
                                deleted: false,
                                created_at: row.created_at,
                                updated_at: row.updated_at,
                            },
                            snapshot: snapshot.slot_ref(),
                            metadata: row.metadata.map_or(
                                crate::json_store::JsonSlotRef::None,
                                crate::transaction::types::StageJson::slot_ref,
                            ),
                        }),
                    },
                );
        let mut encoded =
            crate::live_state::EntityColumnarWriteSets::with_state_row_count(state_rows.len());
        let row_groups =
            crate::sql2::encode_authoritative_registered_entity_row_groups(&spec, rows)?;
        let (row_group_set, input_locations) = row_groups.into_parts();
        let state_set_id = row_group_set.id();
        for (state_row_index, location) in input_locations.into_iter().enumerate() {
            encoded.set_state_row_location(state_row_index, state_set_id, location);
        }
        encoded.insert_unfiled(commit_id, schema_key, row_group_set);
        return Ok(encoded);
    }
    Ok(crate::live_state::EntityColumnarWriteSets::with_state_row_count(state_rows.len()))
}

/// Seals every remaining registered live mutation after commit-delta address
/// assignment, so sparse updates enter the physical layer as typed v2 Arrow
/// rows with their final change identity. These sets are the direct mutation
/// input for the persistent-tree splice; they are not schema-namespaced
/// derived sidecars.
fn append_sparse_authoritative_entity_columnar_write_sets(
    encoded: &mut crate::live_state::EntityColumnarWriteSets,
    state_rows: &PreparedStateBatch,
    entity_schema_catalog: Option<&crate::catalog::CatalogSnapshot>,
    require_preassigned_identity: bool,
) -> Result<(), LixError> {
    let mut indices = BTreeMap::<(CommitId, String), Vec<usize>>::new();
    for (index, row) in state_rows.iter().enumerate() {
        let (Some(commit_id), Some(_snapshot)) = (row.commit_id, row.snapshot) else {
            continue;
        };
        if row.untracked || row.file_id.is_some() {
            continue;
        }
        indices
            .entry((commit_id, row.schema_key.to_string()))
            .or_default()
            .push(index);
    }
    for ((commit_id, schema_key), mut row_indices) in indices {
        if encoded.get_unfiled(commit_id, &schema_key).is_some() {
            continue;
        }
        if require_preassigned_identity
            && row_indices.iter().any(|&index| {
                let row = state_rows.row(index);
                row.change_id.is_none() || row.addressable_change_id
            })
        {
            continue;
        }
        row_indices.sort_unstable_by(|&left, &right| {
            state_rows
                .row(left)
                .entity_pk
                .cmp(state_rows.row(right).entity_pk)
        });
        let Some(schema) = entity_schema_catalog.and_then(|catalog| catalog.schema(&schema_key))
        else {
            continue;
        };
        let Ok(spec) = crate::sql2::derive_entity_surface_spec_from_schema(schema) else {
            continue;
        };
        let rows = row_indices
            .iter()
            .map(|&index| {
                let row = state_rows.row(index);
                let snapshot = row
                    .snapshot
                    .expect("columnar row index retained a snapshot");
                let delta = tracked_delta_from_state_row(row)?;
                Ok(crate::sql2::EntityColumnarRowRef {
                    entity_pk: row.entity_pk,
                    snapshot_bytes: snapshot.normalized().as_bytes(),
                    snapshot_value: None,
                    authority: Some(crate::sql2::EntityColumnarAuthorityRef {
                        value: TrackedStateIndexValueRef {
                            change_id: delta.change_id,
                            commit_id: delta.commit_id,
                            deleted: delta.deleted,
                            created_at: delta.created_at,
                            updated_at: delta.updated_at,
                        },
                        snapshot: snapshot.slot_ref(),
                        metadata: row.metadata.map_or(
                            crate::json_store::JsonSlotRef::None,
                            crate::transaction::types::StageJson::slot_ref,
                        ),
                    }),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let row_groups = crate::sql2::encode_authoritative_registered_entity_row_groups(
            &spec,
            rows.into_iter(),
        )?;
        let (row_group_set, input_locations) = row_groups.into_parts();
        let state_set_id = row_group_set.id();
        for (&state_row_index, location) in row_indices.iter().zip(input_locations) {
            encoded.set_state_row_location(state_row_index, state_set_id, location);
        }
        encoded.insert_unfiled(commit_id, schema_key, row_group_set);
    }
    Ok(())
}

/// Seals every remaining tracked scope into the canonical Arrow authority
/// contract. This is the direct ingress for internal and filed schemas that do
/// not have a registered typed entity projection; it replaces reconstruction
/// from commit-delta payloads during current-state publication.
fn append_base_authoritative_arrow_state_write_sets(
    encoded: &mut crate::live_state::EntityColumnarWriteSets,
    state_rows: &PreparedStateBatch,
    require_preassigned_identity: bool,
) -> Result<(), LixError> {
    let mut scopes = BTreeMap::<(CommitId, CommitDeltaReplacementScope), Vec<usize>>::new();
    for (row_index, row) in state_rows.iter().enumerate() {
        let Some(commit_id) = row.commit_id else {
            continue;
        };
        if row.untracked {
            continue;
        }
        scopes
            .entry((
                commit_id,
                CommitDeltaReplacementScope {
                    schema_key: row.schema_key.to_string(),
                    file_id: row.file_id.map(ToString::to_string),
                },
            ))
            .or_default()
            .push(row_index);
    }
    for ((commit_id, scope), mut row_indices) in scopes {
        if encoded.get_scope(commit_id, &scope).is_some() {
            continue;
        }
        if require_preassigned_identity
            && row_indices.iter().any(|&row_index| {
                let row = state_rows.row(row_index);
                row.change_id.is_none() || row.addressable_change_id
            })
        {
            continue;
        }
        row_indices.sort_unstable_by(|&left, &right| {
            state_rows
                .row(left)
                .entity_pk
                .cmp(state_rows.row(right).entity_pk)
        });
        let owned = row_indices
            .iter()
            .map(|&row_index| {
                let row = state_rows.row(row_index);
                let delta = tracked_delta_from_state_row(row)?;
                Ok((
                    row_index,
                    encode_key_ref(TrackedStateKeyRef {
                        schema_key: row.schema_key,
                        file_id: row.file_id.map(AsRef::as_ref),
                        entity_pk: row.entity_pk,
                    }),
                    TrackedStateIndexValueRef {
                        change_id: delta.change_id,
                        commit_id: delta.commit_id,
                        deleted: delta.deleted,
                        created_at: delta.created_at,
                        updated_at: delta.updated_at,
                    },
                    row.snapshot.map_or(
                        crate::json_store::JsonSlotRef::None,
                        crate::transaction::types::StageJson::slot_ref,
                    ),
                    row.metadata.map_or(
                        crate::json_store::JsonSlotRef::None,
                        crate::transaction::types::StageJson::slot_ref,
                    ),
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let input = owned
            .iter()
            .map(|(_, encoded_key, value, snapshot, metadata)| {
                crate::tracked_state::ArrowStateInputRowRef {
                    encoded_key,
                    value: *value,
                    snapshot: *snapshot,
                    metadata: *metadata,
                }
            })
            .collect::<Vec<_>>();
        let (row_group_set, locations) =
            crate::tracked_state::encode_authoritative_arrow_state_rows(&scope, &input)?;
        let state_set_id = row_group_set.id();
        for ((row_index, ..), location) in owned.iter().zip(locations) {
            encoded.set_state_row_location(*row_index, state_set_id, location);
        }
        encoded.insert_scope(commit_id, scope, row_group_set);
    }
    Ok(())
}

/// Seals host-certified packet post-images into the same canonical Arrow
/// leaves as ordinary mutations. The compact event sidecar may retain authored
/// event identity, but semantic merge and history never need a packet fallback.
fn append_certified_packet_arrow_state_write_sets(
    encoded: &mut crate::live_state::EntityColumnarWriteSets,
    rows_by_commit: &BTreeMap<CommitId, Vec<MaterializedLiveStateRow>>,
    refs_by_commit: &BTreeMap<CommitId, Vec<CertifiedRootJsonRefs>>,
) -> Result<(), LixError> {
    let mut scopes = BTreeMap::<
        (CommitId, CommitDeltaReplacementScope),
        Vec<(&MaterializedLiveStateRow, &CertifiedRootJsonRefs)>,
    >::new();
    for (&commit_id, rows) in rows_by_commit {
        let refs = refs_by_commit.get(&commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified Arrow publication omitted JSON placement",
            )
        })?;
        if rows.len() != refs.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified Arrow publication has misaligned JSON placement",
            ));
        }
        for (row, refs) in rows.iter().zip(refs) {
            scopes
                .entry((
                    commit_id,
                    CommitDeltaReplacementScope {
                        schema_key: row.schema_key.clone(),
                        file_id: row.file_id.clone(),
                    },
                ))
                .or_default()
                .push((row, refs));
        }
    }
    for ((commit_id, scope), mut rows) in scopes {
        if encoded.get_scope(commit_id, &scope).is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "certified Arrow scope '{}:{:?}' overlaps another physical mutation lane",
                    scope.schema_key, scope.file_id
                ),
            ));
        }
        rows.sort_unstable_by(|left, right| left.0.entity_pk.cmp(&right.0.entity_pk));
        let owned = rows
            .iter()
            .map(|(row, refs)| {
                let value = tracked_delta_from_certified_root_row(row)?;
                let snapshot = row.snapshot_content.as_ref().map_or(
                    crate::json_store::JsonSlotRef::None,
                    |snapshot| {
                        refs.snapshot.as_ref().map_or(
                            crate::json_store::JsonSlotRef::Inline(snapshot.as_str()),
                            crate::json_store::JsonSlotRef::Ref,
                        )
                    },
                );
                let metadata = row.metadata.as_ref().map_or(
                    crate::json_store::JsonSlotRef::None,
                    |metadata| {
                        refs.metadata.as_ref().map_or(
                            crate::json_store::JsonSlotRef::Inline(metadata.as_str()),
                            crate::json_store::JsonSlotRef::Ref,
                        )
                    },
                );
                Ok((
                    encode_key_ref(TrackedStateKeyRef {
                        schema_key: &row.schema_key,
                        file_id: row.file_id.as_deref(),
                        entity_pk: &row.entity_pk,
                    }),
                    value,
                    snapshot,
                    metadata,
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let input = owned
            .iter()
            .map(|(encoded_key, value, snapshot, metadata)| {
                crate::tracked_state::ArrowStateInputRowRef {
                    encoded_key,
                    value: TrackedStateIndexValueRef {
                        change_id: value.change_id,
                        commit_id: value.commit_id,
                        deleted: value.deleted,
                        created_at: value.created_at,
                        updated_at: value.updated_at,
                    },
                    snapshot: *snapshot,
                    metadata: *metadata,
                }
            })
            .collect::<Vec<_>>();
        let (row_group_set, locations) =
            crate::tracked_state::encode_authoritative_arrow_state_rows(&scope, &input)?;
        let state_set_id = row_group_set.id();
        for ((encoded_key, ..), location) in owned.iter().zip(locations) {
            encoded.set_addressed_row_location(
                commit_id,
                scope.clone(),
                encoded_key.clone(),
                state_set_id,
                location,
            );
        }
        encoded.insert_scope(commit_id, scope, row_group_set);
    }
    Ok(())
}

fn append_ordered_replacement_columnar_write_sets(
    encoded: &mut crate::live_state::EntityColumnarWriteSets,
    ordered_replacements: &BTreeMap<CommitId, Arc<OrderedMutationJournal>>,
    replacement_generations: &BTreeMap<CommitId, CommitDeltaReplacementGeneration>,
    entity_schema_catalog: Option<&crate::catalog::CatalogSnapshot>,
) -> Result<(), LixError> {
    let _span = tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.materialization.ordered_replacement_arrow"
    )
    .entered();
    for journal in ordered_replacements.values() {
        if encoded
            .get_unfiled(journal.commit_id(), journal.schema_key())
            .is_some()
        {
            continue;
        }
        let Some(schema) =
            entity_schema_catalog.and_then(|catalog| catalog.schema(journal.schema_key()))
        else {
            continue;
        };
        let Ok(spec) = crate::sql2::derive_entity_surface_spec_from_schema(schema) else {
            continue;
        };
        let entity_pks = journal
            .iter()
            .map(|row| EntityPk::single(row.identity().to_owned()))
            .collect::<Vec<_>>();
        let generation = replacement_generations
            .get(&journal.commit_id())
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "ordered Arrow replacement omitted its certified lifecycle",
                )
            })?;
        let created_at = generation.lifecycle_summary.uniform_created_at;
        let rows = journal.iter().zip(entity_pks.iter()).enumerate().map(
            |(row_index, (row, entity_pk))| crate::sql2::EntityColumnarRowRef {
                entity_pk,
                snapshot_bytes: row.snapshot().as_bytes(),
                snapshot_value: None,
                authority: Some(crate::sql2::EntityColumnarAuthorityRef {
                    value: TrackedStateIndexValueRef {
                        change_id: crate::tracked_state::addressable_change_id(
                            journal.commit_id(),
                            row_index / 512,
                            row_index % 512,
                        )
                        .expect("bounded ordered replacement address"),
                        commit_id: journal.commit_id(),
                        deleted: false,
                        created_at,
                        updated_at: journal.timestamp(),
                    },
                    snapshot: row.snapshot_slot(),
                    metadata: crate::json_store::JsonSlotRef::None,
                }),
            },
        );
        let row_groups =
            crate::sql2::encode_authoritative_registered_entity_row_groups(&spec, rows)?;
        let (row_group_set, input_locations) = row_groups.into_parts();
        encoded.insert_replacement(
            (journal.commit_id(), journal.schema_key().to_owned()),
            row_group_set,
            input_locations,
        );
    }
    Ok(())
}

fn ordered_replacement_generations(
    ordered_replacements: &BTreeMap<CommitId, Arc<OrderedMutationJournal>>,
) -> Result<BTreeMap<CommitId, CommitDeltaReplacementGeneration>, LixError> {
    ordered_replacements
        .iter()
        .map(|(&commit_id, journal)| {
            if journal.commit_id() != commit_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "ordered replacement journal is indexed under the wrong commit",
                ));
            }
            let created_at = journal.overlay_uniform_created_at().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "ordered replacement journal omitted its certified lifecycle timestamp",
                )
            })?;
            let scope = CommitDeltaReplacementScope {
                schema_key: journal.schema_key().to_owned(),
                file_id: None,
            };
            let generation = CommitDeltaReplacementGeneration {
                scope: scope.clone(),
                lifecycle_summary: CommitDeltaLifecycleSummary {
                    scope,
                    ordered_identity_digest: journal.replacement_proof().ordered_identity_digest,
                    uniform_created_at: created_at,
                },
            };
            Ok((commit_id, generation))
        })
        .collect()
}

fn explicit_branch_head_targets(
    state_rows: &PreparedStateBatch,
) -> Result<BTreeMap<String, ExplicitBranchHeadTarget>, LixError> {
    let mut targets = BTreeMap::new();
    for row in state_rows {
        if row.schema_key != BRANCH_REF_SCHEMA_KEY || !row.untracked {
            continue;
        }
        let branch_id = row.entity_pk.as_single_string_owned()?;
        let head_commit_id = row
            .snapshot
            .map(|snapshot| {
                let commit_id = snapshot
                    .value()
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
    state_rows: &PreparedStateBatch,
    engine_rows: &[EngineCurrentRow],
    explicit_branch_targets: &BTreeMap<String, ExplicitBranchHeadTarget>,
    observations: &BTreeMap<String, BranchHeadControlObservation>,
) -> Result<(), LixError> {
    let current_state = TrackedHeadContext::new().reader(read);
    for (branch_id, target) in explicit_branch_targets {
        let Some(existing) = observations
            .get(branch_id)
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
                branch_id,
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
            branch_id,
            state_rows,
            engine_rows,
        );
        if !untracked_identities.is_empty() {
            return Err(branch_ref_with_untracked_rows_error(
                branch_id,
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
    explicit_branch_targets: &BTreeMap<String, ExplicitBranchHeadTarget>,
    staged_commits: &BTreeMap<CommitId, StagedChangelogCommit>,
) -> Result<(), LixError> {
    let target_ids = explicit_branch_targets
        .values()
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
        })
        .await?;
    for (commit_id, entry) in commits {
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
    state_rows: &PreparedStateBatch,
    engine_rows: &[EngineCurrentRow],
) -> Result<BTreeMap<String, BranchHeadControlObservation>, LixError> {
    let mut branch_ids = tracked_roots
        .iter()
        .filter(|root| root.publish_head)
        .map(|root| root.branch_id.clone())
        .collect::<BTreeSet<_>>();
    for row in state_rows {
        if row.schema_key == BRANCH_REF_SCHEMA_KEY && row.untracked {
            branch_ids.insert(row.entity_pk.as_single_string_owned()?);
        } else if row.untracked {
            branch_ids.insert(row.branch_id.to_string());
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

fn stage_commit_state_manifests<'a, S>(
    read: &'a S,
    writes: &'a mut StorageWriteSet,
    commit_rows: &'a [FinalizedCommitRow],
    mutation_inventories: &'a BTreeMap<CommitId, CommitStateMutationInventory>,
    planned_members: &'a mut BTreeMap<CommitId, Vec<crate::tracked_state::CommitDeltaMember>>,
    staged_commits: &'a BTreeMap<CommitId, StagedChangelogCommit>,
    external_parent_manifests: &'a BTreeMap<CommitId, CommitStateManifest>,
    arrow_mutations: &'a crate::live_state::EntityColumnarWriteSets,
) -> std::pin::Pin<Box<dyn Future<Output = Result<(), LixError>> + Send + 'a>>
where
    S: StorageAdapterRead + ?Sized + 'a,
{
    Box::pin(async move {
        let mut published_manifests = BTreeMap::new();
        if staged_commits.len() != commit_rows.len()
            || commit_rows
                .iter()
                .any(|commit| !staged_commits.contains_key(&commit.commit_id))
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "commit-state publication is missing a staged topology projection",
            ));
        }
        let mut publication_order = staged_commits.values().collect::<Vec<_>>();
        publication_order
            .sort_unstable_by_key(|staged| (staged.record.generation, staged.record.commit_id));
        for staged in publication_order {
            let record = &staged.record;
            let mut mutations = mutation_inventories
                .get(&record.commit_id)
                .cloned()
                .unwrap_or_default();
            let commit_members = planned_members
                .get(&record.commit_id)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let state_parent_commit_id = record.parent_commit_ids.first().copied();
            let staged_parent =
                state_parent_commit_id.and_then(|parent_id| published_manifests.get(&parent_id));
            let external_parent = if staged_parent.is_none() {
                match state_parent_commit_id {
                    Some(parent_id) => {
                        let published = crate::tracked_state::load_published_commit_state_manifest(
                            read, parent_id,
                        )
                        .await?
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "commit '{}' has no published parent authority",
                                    record.commit_id
                                ),
                            )
                        })?;
                        if external_parent_manifests.get(&parent_id) != Some(&*published) {
                            return Err(LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "published parent authority changed during commit staging",
                            ));
                        }
                        Some(published)
                    }
                    None => None,
                }
            } else {
                None
            };
            let catalog_publication = if let Some(parent) = staged_parent {
                crate::tracked_state::stage_current_state_catalog_from_staged_parent(
                    read,
                    writes,
                    parent,
                    record.commit_id,
                    &mutations,
                    commit_members,
                    Some(arrow_mutations),
                )
                .await?
            } else {
                crate::tracked_state::stage_current_state_catalog_from_published_parent(
                    read,
                    writes,
                    external_parent.as_ref(),
                    record.commit_id,
                    &mutations,
                    commit_members,
                    Some(arrow_mutations),
                )
                .await?
            };
            let current_state_catalog = catalog_publication.root();
            if let Some(commit_members) = planned_members.get_mut(&record.commit_id) {
                for member in commit_members.iter_mut().filter(|member| member.authored) {
                    let encoded_key = encode_key_ref(TrackedStateKeyRef {
                        schema_key: &member.key.schema_key,
                        file_id: member.key.file_id.as_deref(),
                        entity_pk: &member.key.entity_pk,
                    });
                    if let Some(coordinate) = catalog_publication.coordinates().get(&encoded_key) {
                        member.base_coordinate = Some(*coordinate);
                    }
                    if !member.value.deleted && member.base_coordinate.is_none() {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "live authored event has no Arrow state coordinate after publication",
                        ));
                    }
                }
                let local_event_members =
                    mutations.selected_source_commit_id.is_some().then(|| {
                        commit_members
                            .iter()
                            .filter(|member| member.authored)
                            .cloned()
                            .collect::<Vec<_>>()
                    });
                mutations = crate::tracked_state::finalize_commit_delta_event_coordinates(
                    writes,
                    record.commit_id,
                    &mutations,
                    local_event_members.as_deref().unwrap_or(commit_members),
                )?;
            }
            let manifest = CommitStateManifest {
                commit_id: record.commit_id,
                generation: record.generation,
                parent_commit_ids: record.parent_commit_ids.clone(),
                state_parent_commit_id,
                commit_change_id: record.change_id,
                account_id: record.account_id.clone(),
                created_at: record.created_at,
                mutations,
                current_state_catalog,
            };
            let staged_manifest =
                crate::tracked_state::stage_certified_commit_state_manifest_with_handle(
                    writes,
                    &manifest,
                    &catalog_publication,
                )?;
            published_manifests.insert(record.commit_id, staged_manifest);
        }
        Ok(())
    })
}

fn tracked_row_requires_absence(
    row_index: usize,
    row: PreparedStateRowRef<'_>,
    insert_selection: &PreparedInsertSelection,
) -> bool {
    if insert_selection.is_empty() {
        return false;
    }
    row.snapshot.is_some() && !row.untracked && insert_selection.contains(row_index)
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
    selected_change_batches: Vec<StagedCommitChangeBatch>,
}

struct PendingTrackedRoot {
    branch_id: String,
    commit_id: CommitId,
    parent_commit_id: Option<CommitId>,
    /// Metadata for the public synthesized `lix_branch_ref` row.
    ref_change_id: ChangeId,
    ref_updated_at: LixTimestamp,
    publish_head: bool,
}

async fn finalize_commit_rows(
    commit_change_refs_by_branch: BTreeMap<String, StagedCommitChangeRefs>,
    first_commit_parent_override_by_branch: BTreeMap<String, CommitId>,
    extra_commit_parents_by_branch: BTreeMap<String, Vec<CommitId>>,
    intermediate_commits: Vec<crate::transaction::staging::StagedIntermediateCommit>,
    commit_parent_heads: &BTreeMap<String, Option<CommitId>>,
) -> Result<FinalizedCommitRows, LixError> {
    let mut commit_rows = Vec::new();
    let mut tracked_roots = Vec::new();

    for intermediate in intermediate_commits {
        let change_refs = intermediate.change_refs;
        let commit_id = change_refs.commit_id;
        let created_at = change_refs.created_at;
        let commit_change_id = change_refs.commit_change_id;
        let branch_ref_change_id = change_refs.branch_ref_change_id;
        let selected_change_batches = change_refs.into_selected_change_batches();
        commit_rows.push(FinalizedCommitRow {
            commit_id,
            parent_commit_ids: vec![intermediate.parent_commit_id],
            created_at,
            change_id: commit_change_id,
            selected_change_batches,
        });
        tracked_roots.push(PendingTrackedRoot {
            branch_id: intermediate.branch_id,
            commit_id,
            parent_commit_id: Some(intermediate.parent_commit_id),
            ref_change_id: branch_ref_change_id,
            ref_updated_at: created_at,
            publish_head: false,
        });
    }

    for (branch_id, change_refs) in commit_change_refs_by_branch {
        if change_refs.is_empty() && !change_refs.allow_empty {
            continue;
        }

        let commit_id = change_refs.commit_id;
        let commit_change_id = change_refs.commit_change_id;
        let branch_ref_change_id = change_refs.branch_ref_change_id;
        let timestamp = change_refs.created_at;
        let selected_change_batches = change_refs.into_selected_change_batches();
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
            selected_change_batches,
        });
        tracked_roots.push(PendingTrackedRoot {
            branch_id,
            commit_id,
            parent_commit_id,
            ref_change_id: branch_ref_change_id,
            ref_updated_at: timestamp,
            publish_head: true,
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
                .file_content_writes
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
        .chain(
            prepared_writes
                .intermediate_commits
                .iter()
                .map(|commit| commit.branch_id.as_str()),
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
        HOT_ROW_SPACE, LiveStateContext, LiveStateExactBatchRequest, LiveStateExactRowRequest,
        LiveStateProjection, LiveStateRowRequest, ROOT_CURRENT_BASE_SPACE,
    };
    use crate::storage::{
        CommitResult, GetManyResult, KeyRange, PutBatch, ScanChunk, ScanOptions, SpaceId, Storage,
        StorageError, StorageRead, StorageWrite,
    };
    use crate::storage_adapter::{
        Memory, MemoryRead, MemoryWrite, StorageAdapter, StorageAdapterReadScope, StorageKey,
        StorageReadOptions, StorageSpace, StorageWriteOptions,
    };
    use crate::transaction::types::{PreparedRowFacts, TestPreparedStateRow};
    use crate::{GLOBAL_BRANCH_ID, NullableKeyFilter};

    macro_rules! prepared_rows {
        ($($row:expr),* $(,)?) => {
            PreparedStateBatch::from_test_rows(vec![$($row),*])
        };
    }

    fn ts(value: &str) -> LixTimestamp {
        LixTimestamp::expect_parse("timestamp", value)
    }

    #[test]
    fn dense_checkpoint_certificate_requires_provenance_and_exact_coordinates() {
        let source_commit_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_1234_0000_0000,
        ));
        let direct_change_id = |packed: u32| {
            let mut bytes = *source_commit_id.as_uuid().as_bytes();
            bytes[12..].copy_from_slice(&packed.to_be_bytes());
            ChangeId::new(uuid::Uuid::from_bytes(bytes))
        };
        let created_at = ts("2026-01-01T00:00:00Z");
        let identities = ["a", "b"].map(|entity_pk| TrackedStateKey {
            schema_key: "test_schema".to_string(),
            file_id: None,
            entity_pk: EntityPk::single(entity_pk),
        });
        let change_ids = [direct_change_id(1), direct_change_id(2)];
        let row_counts = [2_u16];
        let mut selected = StagedCommitChangeBatchBuilder::with_capacity(2);
        for (identity, change_id) in identities.iter().cloned().zip(change_ids) {
            selected.push(
                crate::tracked_state::TrackedStateDiffIdentity::from_key(identity),
                source_commit_id,
                change_id,
                false,
                created_at,
                created_at,
            );
        }
        let selected = selected.finish_source_certified();
        assert!(dense_selected_source_is_exact(
            source_commit_id,
            &row_counts,
            selected.source_membership_certified(),
            selected.iter(),
        ));

        let mut uncertified = StagedCommitChangeBatchBuilder::with_capacity(2);
        for (identity, change_id) in identities.iter().cloned().zip(change_ids) {
            uncertified.push(
                crate::tracked_state::TrackedStateDiffIdentity::from_key(identity),
                source_commit_id,
                change_id,
                false,
                created_at,
                created_at,
            );
        }
        let uncertified = uncertified.finish();
        assert!(!dense_selected_source_is_exact(
            source_commit_id,
            &row_counts,
            uncertified.source_membership_certified(),
            uncertified.iter(),
        ));

        let mut duplicate_coordinate = StagedCommitChangeBatchBuilder::with_capacity(2);
        for identity in identities {
            duplicate_coordinate.push(
                crate::tracked_state::TrackedStateDiffIdentity::from_key(TrackedStateKey {
                    schema_key: identity.schema_key,
                    file_id: identity.file_id,
                    entity_pk: identity.entity_pk,
                }),
                source_commit_id,
                change_ids[0],
                false,
                created_at,
                created_at,
            );
        }
        let duplicate_coordinate = duplicate_coordinate.finish_source_certified();
        assert!(!dense_selected_source_is_exact(
            source_commit_id,
            &row_counts,
            duplicate_coordinate.source_membership_certified(),
            duplicate_coordinate.iter(),
        ));
    }

    #[test]
    fn host_dense_packets_reuse_ordinary_root_members() {
        let batch = |format| crate::wasm::WasmCertifiedEntityBatch {
            format,
            schema_keys: vec!["test_schema".to_owned()],
            row_count: 1,
            creates: crate::wasm::WasmCreateContext { high: 0, low: 0 },
            create_ranges: Vec::new(),
            complete_file_state: true,
            pages: Vec::new(),
        };
        assert!(!certified_batch_requires_root_expansion(&batch(
            crate::wasm::HOST_CERTIFIED_PACKET_FORMAT
        )));
        assert!(!certified_batch_requires_root_expansion(&batch(
            crate::wasm::HOST_CERTIFIED_ZSTD_PACKET_FORMAT
        )));
        assert!(certified_batch_requires_root_expansion(&batch(1)));
        assert!(certified_batch_requires_root_expansion(&batch(2)));
    }

    const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
    const DETERMINISTIC_SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";
    const TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE_ID: SpaceId = SpaceId(0x0004_002b);
    const TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE: StorageSpace = StorageSpace::mutable(
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE_ID,
        "tracked_state.commit_state_manifest.v1",
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
        arrow_state_get_many_calls: AtomicUsize,
        arrow_state_scan_calls: AtomicUsize,
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
                if space == crate::branch::BRANCH_HEAD_CONTROL_SPACE {
                    self.counts
                        .branch_control_get_many_calls
                        .fetch_add(1, Ordering::Relaxed);
                }
                if space.id == V10_TRACKED_HEAD_MARKER_SPACE_ID {
                    self.counts
                        .v10_marker_get_many_calls
                        .fetch_add(1, Ordering::Relaxed);
                }
                if space == HOT_ROW_SPACE || space == crate::live_state::HOT_FILE_SPACE {
                    self.counts
                        .row_get_many_calls
                        .fetch_add(1, Ordering::Relaxed);
                }
                if matches!(
                    space.id.0,
                    0x0004_0029 | 0x0004_002a | 0x0004_002c | 0x0004_002d | 0x0004_0030
                ) {
                    self.counts
                        .arrow_state_get_many_calls
                        .fetch_add(1, Ordering::Relaxed);
                }
                if space.id == TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE_ID {
                    self.counts
                        .commit_root_get_many_calls
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            self.inner.get_many(requests).await
        }

        async fn scan(
            &self,
            space: StorageSpace,
            range: KeyRange,
            opts: ScanOptions,
        ) -> Result<ScanChunk, StorageError> {
            if space == HOT_ROW_SPACE || space == crate::live_state::HOT_FILE_SPACE {
                self.counts.row_scan_calls.fetch_add(1, Ordering::Relaxed);
            }
            if matches!(
                space.id.0,
                0x0004_0029 | 0x0004_002a | 0x0004_002c | 0x0004_002d | 0x0004_0030
            ) {
                self.counts
                    .arrow_state_scan_calls
                    .fetch_add(1, Ordering::Relaxed);
            }
            if space.id == TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE_ID {
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
            &prepared_rows![row],
            &[0],
            &[selected_change_batch("selected-change", "entity-1")],
        )
        .expect_err("selected ref must not duplicate a normal row identity");
        assert!(error.message.contains("duplicate change ref key"));

        let row = tracked_global_row("normal-change");
        validate_selected_change_refs(
            commit_id("test-uuid-1"),
            &prepared_rows![row],
            &[0],
            &[selected_change_batch("normal-change", "other-entity")],
        )
        .expect("different semantic identities may share one source change id");
    }

    #[tokio::test]
    async fn certified_packet_rows_publish_arrow_state_without_event_payload_duplication() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8000_4321_0000_0000,
        ));
        let timestamp = ts("2026-01-01T00:00:00Z");
        let mut large_snapshot = String::with_capacity(4 * 1024 * 1024 + 2);
        large_snapshot.push('"');
        let mut random = 0x9e37_79b9_u32;
        for _ in 0..(4 * 1024 * 1024) {
            random ^= random << 13;
            random ^= random >> 17;
            random ^= random << 5;
            large_snapshot.push(char::from(b'a' + (random % 26) as u8));
        }
        large_snapshot.push('"');
        let large_snapshot = crate::common::SharedStr::from(large_snapshot);
        let mut state_rows = PreparedStateBatch::new();
        state_rows.push_parts_with_change_addressability(
            SchemaPlanId::for_test(0),
            PreparedRowFacts::default(),
            EntityPk::single("ordinary"),
            "ordinary_schema".into(),
            None,
            Some(
                crate::transaction::types::stage_json_from_value(
                    crate::transaction::types::TransactionJson::from_value_for_test(
                        serde_json::from_str(large_snapshot.as_str())
                            .expect("large ordinary snapshot should parse"),
                    ),
                    "mixed certified ordinary snapshot",
                )
                .expect("ordinary snapshot should stage"),
            ),
            None,
            None,
            None,
            timestamp,
            timestamp,
            true,
            Some(change_id("mixed-ordinary-change")),
            false,
            Some(commit_id),
            false,
            GLOBAL_BRANCH_ID.into(),
        );
        let certified_change_id = change_id("mixed-certified-change");
        let certified_rows = BTreeMap::from([(
            commit_id,
            vec![MaterializedLiveStateRow {
                entity_pk: EntityPk::single("certified"),
                schema_key: "certified_schema".to_owned(),
                file_id: Some("certified.csv".to_owned()),
                snapshot_content: Some(large_snapshot.clone()),
                metadata: None,
                deleted: false,
                created_at: timestamp,
                updated_at: timestamp,
                global: true,
                change_id: Some(certified_change_id),
                commit_id: Some(commit_id),
                untracked: false,
                branch_id: Arc::from(GLOBAL_BRANCH_ID),
            }],
        )]);
        let roots = [PendingTrackedRoot {
            branch_id: GLOBAL_BRANCH_ID.to_owned(),
            commit_id,
            parent_commit_id: None,
            ref_change_id: change_id("mixed-certified-ref"),
            ref_updated_at: timestamp,
            publish_head: true,
        }];
        let commits = [FinalizedCommitRow {
            commit_id,
            parent_commit_ids: Vec::new(),
            created_at: timestamp,
            change_id: change_id("mixed-certified-commit"),
            selected_change_batches: Vec::new(),
        }];
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("mixed certified read should open");
        let mut writes = StorageWriteSet::new();
        let certified_json_refs = certified_root_json_refs(&certified_rows);
        stage_state_json_payloads(
            &mut JsonStoreContext::new().writer(),
            &mut writes,
            &state_rows,
            &certified_rows,
            &certified_json_refs,
        )
        .expect("duplicate large ordinary and certified payload should stage once");
        let large_snapshot_ref = certified_json_refs[&commit_id][0]
            .snapshot
            .expect("large certified snapshot should use a JSON ref");
        let mut entity_columnar_write_sets =
            crate::live_state::EntityColumnarWriteSets::with_state_row_count(state_rows.len());
        append_certified_packet_arrow_state_write_sets(
            &mut entity_columnar_write_sets,
            &certified_rows,
            &certified_json_refs,
        )
        .expect("certified packet should seal into canonical Arrow state");
        append_base_authoritative_arrow_state_write_sets(
            &mut entity_columnar_write_sets,
            &state_rows,
            false,
        )
        .expect("ordinary row should seal into canonical Arrow state");
        let certified_scope = CommitDeltaReplacementScope {
            schema_key: "certified_schema".to_owned(),
            file_id: Some("certified.csv".to_owned()),
        };
        let certified_state = entity_columnar_write_sets
            .get_scope(commit_id, &certified_scope)
            .expect("certified packet should own one Arrow state set");
        assert_eq!(certified_state.manifest.row_count(), 1);
        crate::columnar_row_group::stage_row_group_set(&mut writes, certified_state)
            .expect("certified Arrow state should stage");
        let ordinary_scope = CommitDeltaReplacementScope {
            schema_key: "ordinary_schema".to_owned(),
            file_id: None,
        };
        let ordinary_state = entity_columnar_write_sets
            .get_scope(commit_id, &ordinary_scope)
            .expect("ordinary row should own one Arrow state set");
        crate::columnar_row_group::stage_row_group_set(&mut writes, ordinary_state)
            .expect("ordinary Arrow state should stage");
        let staged_index = stage_tracked_commit_delta_index(
            &read,
            &mut writes,
            &mut state_rows,
            &entity_columnar_write_sets,
            &BTreeMap::from([(commit_id, vec![0])]),
            &roots,
            &commits,
            &HashMap::new(),
            &certified_rows,
            &certified_json_refs,
            &PreparedInsertSelection::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await
        .expect("mixed certified delta should stage");
        let mutations = staged_index
            .inventories
            .get(&commit_id)
            .cloned()
            .expect("mixed certified inventory should stage");
        let current_state_catalog = Box::new(
            crate::tracked_state::empty_current_state_catalog_root(None, commit_id)
                .expect("mixed certified Arrow root should construct"),
        );
        stage_commit_state_manifest(
            &mut writes,
            &CommitStateManifest {
                commit_id,
                generation: 0,
                parent_commit_ids: Vec::new(),
                state_parent_commit_id: None,
                commit_change_id: change_id("mixed-certified-commit"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: timestamp,
                mutations,
                current_state_catalog,
            },
        )
        .expect("mixed certified authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("mixed certified delta should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("mixed certified verification read should open");
        let records = crate::tracked_state::scan_commit_delta_members(&read, commit_id)
            .await
            .expect("mixed certified records should load");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0.schema_key, "certified_schema");
        assert_eq!(
            records[0].1.change_id, certified_change_id,
            "certified packet identity must be present in the commit delta"
        );
        assert_eq!(records[1].0.schema_key, "ordinary_schema");
        let packed_members =
            crate::tracked_state::load_commit_delta_members_with_payloads(&read, commit_id)
                .await
                .expect("mixed certified payloads should load");
        let certified = packed_members
            .iter()
            .find(|member| member.change.change_id == certified_change_id)
            .expect("certified payload should be a commit member");
        assert_eq!(
            certified.change.snapshot,
            crate::json_store::JsonSlot::Ref(large_snapshot_ref)
        );
        assert_eq!(certified.change.metadata, crate::json_store::JsonSlot::None);
        let loaded = JsonStoreContext::new()
            .load_bytes_many(
                &read,
                crate::json_store::JsonLoadRequestRef {
                    refs: &[large_snapshot_ref],
                    scope: crate::json_store::JsonReadScopeRef::OutOfBand,
                },
            )
            .await
            .expect("large certified JSON ref should resolve")
            .into_values();
        assert_eq!(
            loaded[0].as_deref(),
            Some(large_snapshot.as_bytes()),
            "large certified history payload must round-trip exactly"
        );
    }

    #[tokio::test]
    async fn ordinary_unaddressed_tracked_commit_appends_changelog_and_root() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let state_rows = prepared_rows![tracked_global_row("change-1")];
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_selection: PreparedInsertSelection::new(),
                state_rows,
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs(["change-1"]),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
            },
        )
        .await
        .expect("commit should flush staged rows");
        assert!(
            writes.has_mutations_in_space(crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE),
            "an unaddressed tracked commit still requires Arrow-native state leaves"
        );
        assert!(
            writes.has_mutations_in_space(TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE),
            "an unaddressed tracked commit still requires commit-state authority"
        );
        assert!(
            !writes.has_mutations_in_space(HOT_ROW_SPACE),
            "tracked committed rows must not be duplicated into HOT state"
        );
        assert!(
            writes.has_mutations_in_space(ROOT_CURRENT_BASE_SPACE),
            "current serving must publish the authoritative Arrow root"
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
        let commit_ids = [commit_id("test-uuid-1")];
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &commit_ids,
            })
            .await
            .expect("changelog commit should load");
        let Some(record) = commits.into_iter().next().and_then(|(_, value)| value) else {
            panic!("changelog commit should exist");
        };
        assert_eq!(record.change_id, change_id("test-uuid-2"));
        let membership_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("membership read should open");
        let change_ids =
            crate::tracked_state::load_commit_delta_change_ids(&membership_read, record.commit_id)
                .await
                .expect("commit membership should load");
        assert!(
            change_ids.contains(&change_id("change-1")),
            "tracked change should be a packed commit-delta member"
        );
        let packed_members = crate::tracked_state::load_commit_delta_members_with_payloads(
            &membership_read,
            record.commit_id,
        )
        .await
        .expect("packed commit payloads should load");
        let change = packed_members
            .iter()
            .find(|member| member.change.change_id == change_id("change-1"))
            .map(|member| &member.change)
            .expect("tracked change should have an authoritative packed payload");
        assert_eq!(change.schema_key, "test_schema");
        let change_ids = [change_id("change-1"), record.change_id];
        let changes = changelog_reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &change_ids,
            })
            .await
            .expect("changelog change should load");
        assert!(
            changes.iter().all(|(_, value)| value.is_none()),
            "tracked and derived commit changes must not be duplicated in changelog.change"
        );

        let mut tracked_reader = TrackedStateContext::new().reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let commit_id_text = commit_id_text("test-uuid-1");
        let commit_rows = tracked_reader
            .scan_batch_at_commit(
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
            .expect("rooted commit history should scan")
            .into_rows();
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
            .scan_batch(&crate::live_state::LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    schema_keys: vec!["lix_commit".to_string()],
                    branch_ids: vec![GLOBAL_BRANCH_ID.to_string()],
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("derived commit rows should scan")
            .into_rows();
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
        let branch_id = "01960000-0000-7000-8000-000000000001";
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
        let branch_id = "01960000-0000-7000-8000-000000000002";
        crate::test_support::seed_branch_head(storage.clone(), branch_id, "branch-ref-head").await;

        let mut branch_ref_delete = untracked_global_row("delete-branch-ref");
        branch_ref_delete.entity_pk = EntityPk::single(branch_id);
        branch_ref_delete.schema_key = BRANCH_REF_SCHEMA_KEY.into();
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![branch_ref_delete, pending_untracked],
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
        let branch_id = "01960000-0000-7000-8000-000000000003";
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![persisted_untracked],
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
        branch_ref_delete.schema_key = BRANCH_REF_SCHEMA_KEY.into();
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![branch_ref_delete],
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
        cleanup_branch_ref_delete.schema_key = BRANCH_REF_SCHEMA_KEY.into();
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![cleanup_branch_ref_delete, untracked_delete],
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
            insert_selection: PreparedInsertSelection::new(),
            state_rows: prepared_rows![
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
            intermediate_commits: Vec::new(),
            file_content_writes: Vec::new(),
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
    async fn branch_creation_inherits_same_head_schema_presence() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let donor_branch = "01960000-0000-7000-8000-0000000000d1";
        let created_branch = "01960000-0000-7000-8000-0000000000d2";
        let head = "branch-schema-presence-head";
        crate::test_support::seed_branch_head(storage.clone(), donor_branch, head).await;

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("branch creation read should open");
        let (writes, preconditions) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            prepared_direct_branch_ref_update(created_branch, head, "inherited-schema-branch-ref"),
        )
        .await
        .expect("same-head branch creation should stage");
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("same-head branch creation should commit");

        let control = BranchHeadControlContext::new()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("created branch control read should open"),
            )
            .load(created_branch)
            .await
            .expect("created branch control should load")
            .expect("created branch control must exist");
        assert_eq!(
            control.schema_presence_bloom,
            [u64::MAX; 4],
            "a new generation must preserve the donor head's conservative schema visibility"
        );
    }

    #[tokio::test]
    async fn direct_branch_ref_update_rejects_a_stale_control_token() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let branch_id = "01960000-0000-7000-8000-000000000004";
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
                    if *space == crate::branch::BRANCH_HEAD_CONTROL_SPACE
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
                    if *space == crate::branch::BRANCH_HEAD_CONTROL_SPACE
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
    async fn serial_history_publishes_roots_and_diffs() {
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![first],
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
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
            },
        )
        .await
        .expect("first rooted commit should stage");
        assert!(
            writes.has_mutations_in_space(crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE)
                && writes.has_mutations_in_space(TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE),
            "first ordinary commit must publish its immutable root"
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![second],
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
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
            },
        )
        .await
        .expect("second rooted commit should stage");
        assert!(
            writes.has_mutations_in_space(crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE)
                && writes.has_mutations_in_space(TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE),
            "serial ordinary commit must publish its immutable root"
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![third],
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
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
            },
        )
        .await
        .expect("third rooted commit should stage");
        assert!(
            writes.has_mutations_in_space(crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE)
                && writes.has_mutations_in_space(TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE),
            "serial ordinary commit must publish its immutable root"
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![deleted],
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
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
            },
        )
        .await
        .expect("rooted delete commit should stage");
        assert!(
            writes.has_mutations_in_space(crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE)
                && writes.has_mutations_in_space(TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE),
            "serial delete commit must publish its immutable root"
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
            .scan_batch_at_commit(&commit_id_text("rootless-first-commit"), &request)
            .await
            .expect("first rootless commit should replay")
            .into_rows();
        assert!(matches!(
            first_rows.as_slice(),
            [row] if row.change_id == change_id("rootless-first-change")
                && row.snapshot_content.as_deref() == Some("{\"value\":1}")
        ));
        let second_rows = reader
            .scan_batch_at_commit(&commit_id_text("rootless-second-commit"), &request)
            .await
            .expect("second rootless commit should replay")
            .into_rows();
        assert!(matches!(
            second_rows.as_slice(),
            [row] if row.change_id == change_id("rootless-second-change")
                && row.created_at == "2026-01-01T00:00:00.000Z"
                && row.snapshot_content.as_deref() == Some("{\"value\":2}")
        ));
        let third_rows = reader
            .scan_batch_at_commit(&commit_id_text("rootless-third-commit"), &request)
            .await
            .expect("third rootless commit should replay")
            .into_rows();
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
            .scan_batch_at_commit(&commit_id_text("rootless-delete-commit"), &request)
            .await
            .expect("delete rootless commit should replay")
            .into_rows();
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
    async fn selected_reference_commit_publishes_root() {
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![normal],
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
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
            },
        )
        .await
        .expect("normal rooted commit should stage");
        assert!(
            writes.has_mutations_in_space(TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE),
            "ordinary parent must publish a root"
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
        fence_refs.add_selected_change_batch(selected_change_batch_from(
            "fence-normal-change",
            "entity-1",
            "fence-normal-commit",
        ));
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: PreparedStateBatch::new(),
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    fence_refs,
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
            },
        )
        .await
        .expect("selected-reference commit should stage");
        assert!(
            writes.has_mutations_in_space(TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE),
            "selected-reference commits must publish commit-state authority"
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
            .scan_batch_at_commit(
                &commit_id_text("fence-commit"),
                &TrackedStateScanRequest::default(),
            )
            .await
            .expect("rooted selected-reference commit should scan")
            .into_rows();
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![tracked_global_row("tracked-head-change")],
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs(["tracked-head-change"]),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
            },
        )
        .await
        .expect("normal tracked commit should stage");
        assert!(
            writes.has_mutations_in_space(crate::tracked_state::CURRENT_STATE_DATA_PART_REFS_SPACE),
            "an ordinary tracked commit must write Arrow-native state leaves"
        );
        assert!(
            writes.has_mutations_in_space(TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE),
            "an ordinary tracked commit must write commit-state authority"
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
            .scan_batch(&crate::live_state::LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    branch_ids: vec![GLOBAL_BRANCH_ID.to_string()],
                    schema_keys: vec!["test_schema".to_string()],
                    untracked: Some(false),
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("tracked scan should succeed")
            .into_rows();
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
        assert!(
            counts.arrow_state_get_many_calls.load(Ordering::Relaxed)
                + counts.arrow_state_scan_calls.load(Ordering::Relaxed)
                > 0,
            "a current tracked projection must read the authoritative Arrow tree"
        );
        assert!(
            counts.commit_root_get_many_calls.load(Ordering::Relaxed) > 0,
            "a current tracked projection must resolve commit-state authority"
        );
        assert_eq!(
            counts.commit_root_scan_calls.load(Ordering::Relaxed),
            0,
            "a current head projection must not scan commit-state authority"
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
            .load_exact_batch(&LiveStateExactBatchRequest {
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
            .expect("exact tracked batch should succeed")
            .into_rows();
        assert!(matches!(
            exact_rows.as_slice(),
            [Some(row)] if row.change_id == Some(change_id("tracked-head-change"))
        ));
        assert!(
            counts.arrow_state_get_many_calls.load(Ordering::Relaxed)
                + counts.arrow_state_scan_calls.load(Ordering::Relaxed)
                > 0,
            "serving reads must continue through the authoritative Arrow tree"
        );
        assert!(
            counts.commit_root_get_many_calls.load(Ordering::Relaxed) > 0,
            "serving reads must resolve commit-state authority"
        );
        assert_eq!(
            counts.commit_root_scan_calls.load(Ordering::Relaxed),
            0,
            "neither serving read may scan commit-state authority"
        );
    }

    #[tokio::test]
    async fn serial_local_commit_reads_branch_control_for_parent_and_publication() {
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();

        let mut first =
            tracked_branch_row("01920000-0000-7000-8000-0000000000a1", "first-local-change");
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![first],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "01920000-0000-7000-8000-0000000000a1".to_string(),
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
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
        let mut second = tracked_branch_row(
            "01920000-0000-7000-8000-0000000000a1",
            "second-local-change",
        );
        second.commit_id = Some(commit_id("second-local-commit"));
        let (_writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut second_read,
            PreparedWriteSet {
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![second],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "01920000-0000-7000-8000-0000000000a1".to_string(),
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
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![global_override, global_fallback],
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
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
            },
        )
        .await
        .expect("global tracked commit should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("global tracked commit should persist");

        let mut branch_override = tracked_branch_row(
            "01920000-0000-7000-8000-0000000000a1",
            "branch-override-change",
        );
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![branch_override],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "01920000-0000-7000-8000-0000000000a1".to_string(),
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
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
            .load_many(&[
                GLOBAL_BRANCH_ID.to_string(),
                "01920000-0000-7000-8000-0000000000a1".to_string(),
            ])
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
            .scan_batch(&crate::live_state::LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    branch_ids: vec!["01920000-0000-7000-8000-0000000000a1".to_string()],
                    schema_keys: vec!["test_schema".to_string()],
                    untracked: Some(false),
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("branch tracked scan should succeed")
            .into_rows();
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
        assert_eq!(
            branch_row.branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
        assert!(!branch_row.global);
        let fallback_row = scanned
            .iter()
            .find(|row| row.entity_pk == EntityPk::single("entity-2"))
            .expect("global fallback should be visible");
        assert_eq!(
            fallback_row.change_id,
            Some(change_id("global-fallback-change"))
        );
        assert_eq!(
            fallback_row.branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
        assert!(fallback_row.global);

        let mut branch_tombstone = tracked_branch_row(
            "01920000-0000-7000-8000-0000000000a1",
            "branch-tombstone-change",
        );
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![branch_tombstone],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "01920000-0000-7000-8000-0000000000a1".to_string(),
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
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
            .scan_batch(&crate::live_state::LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    branch_ids: vec!["01920000-0000-7000-8000-0000000000a1".to_string()],
                    schema_keys: vec!["test_schema".to_string()],
                    untracked: Some(false),
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("branch tombstone scan should succeed")
            .into_rows();
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
        assert!(
            counts.arrow_state_get_many_calls.load(Ordering::Relaxed)
                + counts.arrow_state_scan_calls.load(Ordering::Relaxed)
                > 0,
            "current global and branch projections must read their authoritative Arrow trees"
        );
        assert!(
            counts.commit_root_get_many_calls.load(Ordering::Relaxed) > 0,
            "current global and branch projections must resolve commit-state authority"
        );
        assert_eq!(
            counts.commit_root_scan_calls.load(Ordering::Relaxed),
            0,
            "current global and branch projections must not scan commit-state authority"
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
                selected_change_batches: Vec::new(),
            },
            FinalizedCommitRow {
                commit_id: CommitId::for_test_label("parent-commit"),
                parent_commit_ids: Vec::new(),
                created_at: ts("2026-01-01T00:00:00Z"),
                change_id: ChangeId::for_test_label("parent-commit-change"),
                selected_change_batches: Vec::new(),
            },
        ];
        let mut external_parent_manifests = BTreeMap::new();
        let staged = stage_changelog_commits(
            &mut read,
            &mut writes,
            &prepared_rows![parent_row, child_row],
            &[],
            &[],
            &[],
            &BTreeMap::from([
                (CommitId::for_test_label("parent-commit"), vec![0]),
                (CommitId::for_test_label("child-commit"), vec![1]),
            ]),
            &commits,
            &mut external_parent_manifests,
            crate::ANONYMOUS_ACCOUNT_ID,
        )
        .await
        .expect("child-before-parent input should still stage parent first");
        let mutation_inventories = commits
            .iter()
            .map(|commit| (commit.commit_id, CommitStateMutationInventory::default()))
            .collect::<BTreeMap<_, _>>();
        let mut planned_members = BTreeMap::new();
        stage_commit_state_manifests(
            &read,
            &mut writes,
            &commits,
            &mutation_inventories,
            &mut planned_members,
            &staged,
            &external_parent_manifests,
            &crate::live_state::EntityColumnarWriteSets::new(),
        )
        .await
        .expect("child-before-parent manifests should publish parent authority first");
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
        let commit_ids = [
            CommitId::for_test_label("parent-commit"),
            CommitId::for_test_label("child-commit"),
        ];
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &commit_ids,
            })
            .await
            .expect("commits should load");
        let commits = commits
            .iter()
            .map(|(_, record)| record.expect("commit"))
            .collect::<Vec<_>>();
        assert_eq!(commits[0].generation, 0);
        assert_eq!(commits[1].generation, 1);
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

        let state_rows = prepared_rows![untracked_global_row("change-untracked")];
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_selection: PreparedInsertSelection::new(),
                state_rows,
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
        let change_ids = [change_id("change-untracked")];
        let changes = changelog_reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &change_ids,
            })
            .await
            .expect("untracked changelog lookup should load");
        assert_eq!(
            changes.iter().all(|(_, value)| value.is_none()),
            true,
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![untracked_global_row("change-untracked")],
                commit_change_refs_by_branch: BTreeMap::new(),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
        let state_rows = prepared_rows![tracked_global_row("change-tracked")];
        let error = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_selection: PreparedInsertSelection::new(),
                state_rows,
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs(["change-tracked"]),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
                    insert_selection: PreparedInsertSelection::new(),
                    state_rows: prepared_rows![setup_row],
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
                    intermediate_commits: Vec::new(),
                    file_content_writes: Vec::new(),
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
                    insert_selection: PreparedInsertSelection::new(),
                    state_rows: prepared_rows![untracked_key_value_row(
                        DETERMINISTIC_MODE_KEY,
                        serde_json::json!({ "enabled": true }),
                        "deterministic-mode-change",
                    )],
                    commit_change_refs_by_branch: BTreeMap::new(),
                    first_commit_parent_override_by_branch: BTreeMap::new(),
                    checkpoint_publications: Vec::new(),
                    extra_commit_parents_by_branch: BTreeMap::new(),
                    intermediate_commits: Vec::new(),
                    file_content_writes: Vec::new(),
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![tracked_row, untracked_row],
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs(["change-tracked"]),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
        let commit_ids = [commit_id("test-uuid-1")];
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &commit_ids,
            })
            .await
            .expect("changelog commit should load");
        let Some(commit) = commits.into_iter().next().and_then(|(_, value)| value) else {
            panic!("changelog commit should exist");
        };
        assert_eq!(commit.change_id, change_id("test-uuid-2"));
        let packed_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("packed payload read should open");
        let packed_members = crate::tracked_state::load_commit_delta_members_with_payloads(
            &packed_read,
            commit.commit_id,
        )
        .await
        .expect("tracked packed change should load");
        assert!(
            packed_members
                .iter()
                .any(|member| member.change.change_id == change_id("change-tracked"))
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
        crate::test_support::seed_branch_head(
            storage.clone(),
            "01920000-0000-7000-8000-0000000000a1",
            "01920000-0000-7000-8000-0000000000a1-before",
        )
        .await;

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let state_rows = prepared_rows![tracked_branch_row(
            "01920000-0000-7000-8000-0000000000a1",
            "change-01920000-0000-7000-8000-0000000000a1",
        )];
        let (writes, _) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut read,
            PreparedWriteSet {
                insert_selection: PreparedInsertSelection::new(),
                state_rows,
                commit_change_refs_by_branch: BTreeMap::from([(
                    "01920000-0000-7000-8000-0000000000a1".to_string(),
                    change_refs(["change-01920000-0000-7000-8000-0000000000a1"]),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
        let commit_ids = [commit_id("test-uuid-1")];
        let commits = changelog_reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &commit_ids,
            })
            .await
            .expect("changelog commit should load");
        let Some(commit) = commits.into_iter().next().and_then(|(_, value)| value) else {
            panic!("changelog commit should exist");
        };
        assert_eq!(commit.change_id, change_id("test-uuid-2"));
        assert_eq!(
            commit.parent_commit_ids,
            vec![CommitId::for_test_label(
                "01920000-0000-7000-8000-0000000000a1-before"
            )]
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
            .load_head_commit_id("01920000-0000-7000-8000-0000000000a1")
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
            Vec::new(),
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
            Vec::new(),
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
            BTreeMap::from([(
                "01920000-0000-7000-8000-0000000000a1".to_string(),
                change_refs(["change-a"]),
            )]),
            BTreeMap::new(),
            BTreeMap::new(),
            Vec::new(),
            &BTreeMap::from([(
                "01920000-0000-7000-8000-0000000000a1".to_string(),
                Some(CommitId::for_test_label("previous-commit")),
            )]),
        )
        .await
        .expect("active-branch commit finalization should resolve parent");

        assert_eq!(
            rows.commit_rows[0].parent_commit_ids,
            vec![CommitId::for_test_label("previous-commit")]
        );
        assert_eq!(
            rows.tracked_roots[0].branch_id,
            "01920000-0000-7000-8000-0000000000a1"
        );
    }

    #[tokio::test]
    async fn finalize_commit_rows_appends_extra_merge_parent_after_target_head() {
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                "01920000-0000-7000-8000-0000000000a1".to_string(),
                change_refs(["change-a"]),
            )]),
            BTreeMap::new(),
            BTreeMap::from([(
                "01920000-0000-7000-8000-0000000000a1".to_string(),
                vec![CommitId::for_test_label("source-head")],
            )]),
            Vec::new(),
            &BTreeMap::from([(
                "01920000-0000-7000-8000-0000000000a1".to_string(),
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![tracked_branch_row("missing-branch", "missing-change")],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "missing-branch".to_string(),
                    change_refs(["missing-change"]),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: PreparedStateBatch::new(),
                commit_change_refs_by_branch: BTreeMap::from([(
                    GLOBAL_BRANCH_ID.to_string(),
                    change_refs(["first-global-change"]),
                )]),
                first_commit_parent_override_by_branch: BTreeMap::new(),
                checkpoint_publications: Vec::new(),
                extra_commit_parents_by_branch: BTreeMap::new(),
                intermediate_commits: Vec::new(),
                file_content_writes: Vec::new(),
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
            insert_selection: PreparedInsertSelection::new(),
            state_rows: prepared_rows![direct_branch_ref_row(
                branch_id,
                target_commit_label,
                branch_ref_change_label,
            )],
            commit_change_refs_by_branch: BTreeMap::new(),
            first_commit_parent_override_by_branch: BTreeMap::new(),
            checkpoint_publications: Vec::new(),
            extra_commit_parents_by_branch: BTreeMap::new(),
            intermediate_commits: Vec::new(),
            file_content_writes: Vec::new(),
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
            insert_selection: PreparedInsertSelection::new(),
            state_rows: prepared_rows![tracked_global_row(row_change_label)],
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
            intermediate_commits: Vec::new(),
            file_content_writes: Vec::new(),
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

    fn selected_change_batch(change_id: &str, entity_pk: &str) -> StagedCommitChangeBatch {
        selected_change_batch_from(change_id, entity_pk, "selected-source")
    }

    fn selected_change_batch_from(
        change_id: &str,
        entity_pk: &str,
        source_commit_id: &str,
    ) -> StagedCommitChangeBatch {
        let identity = crate::tracked_state::TrackedStateDiffIdentity::from_key(TrackedStateKey {
            schema_key: "test_schema".to_string(),
            file_id: None,
            entity_pk: EntityPk::single(entity_pk),
        });
        let mut batch = StagedCommitChangeBatchBuilder::with_capacity(1);
        batch.push(
            identity,
            commit_id(source_commit_id),
            self::change_id(change_id),
            false,
            ts("2026-01-01T00:00:00Z"),
            ts("2026-01-01T00:00:00Z"),
        );
        batch.finish()
    }

    fn tracked_global_row(change_id: &str) -> TestPreparedStateRow {
        tracked_branch_row(GLOBAL_BRANCH_ID, change_id)
    }

    fn tracked_branch_row(branch_id: &str, change_id: &str) -> TestPreparedStateRow {
        TestPreparedStateRow {
            schema_plan_id: SchemaPlanId::for_test(0),
            facts: PreparedRowFacts::default(),
            entity_pk: EntityPk::single("entity-1"),
            schema_key: "test_schema".into(),
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
            branch_id: branch_id.into(),
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

    fn untracked_global_row(change_id: &str) -> TestPreparedStateRow {
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
        TestPreparedStateRow {
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
    ) -> TestPreparedStateRow {
        let mut row = untracked_global_row(change_id);
        row.entity_pk = EntityPk::single(branch_id);
        row.schema_key = BRANCH_REF_SCHEMA_KEY.into();
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
    ) -> TestPreparedStateRow {
        let mut row = untracked_global_row(change_id);
        row.entity_pk = EntityPk::single(key);
        row.schema_key = "lix_key_value".into();
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
            space: StorageSpace,
            entries: PutBatch,
        ) -> Result<(), StorageError> {
            self.inner.put_many(space, entries).await
        }

        async fn delete_many(
            &mut self,
            space: StorageSpace,
            keys: &[StorageKey],
        ) -> Result<(), StorageError> {
            self.inner.delete_many(space, keys).await
        }

        async fn delete_range(
            &mut self,
            space: StorageSpace,
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
