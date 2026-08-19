#![allow(
    clippy::implicit_clone,
    clippy::unnecessary_mut_passed,
    clippy::unnecessary_wraps
)]

use crate::LixError;
use crate::NullableKeyFilter;
use crate::binary_cas::BinaryCasContext;
use crate::branch::{
    BRANCH_REF_SCHEMA_KEY, BranchContext, BranchHeadControl, BranchHeadControlContext,
    BranchHeadControlObservation, BranchRefReader, branch_head_control_precondition,
    stage_branch_head_control, stage_delete_branch_head_control,
};
use crate::changelog::COMMIT_RECORD_FORMAT_VERSION;
use crate::changelog::{
    ChangeId, ChangeRecord, ChangeRecordProjection, ChangeScanRequest, ChangelogContext,
    ChangelogReader, ChangelogWriter, CommitId, CommitLoadRequest as ChangelogCommitLoadRequest,
    CommitRecord, CommitScanRequest, CommitTouchedScopeDigest, TransactionChangeRecordRef,
    TransactionChangelogAppend,
};
use crate::common::LixTimestamp;
use crate::filesystem::stage_path_index_revision;
use crate::functions::FunctionContext;
use crate::hot_state::{
    HotStateContext, HotStateRowRequest, HotTrackedSnapshot, MaterializedHotStateRow,
    TrackedHeadContext, TrackedWorkingDiffEpoch, WorkingDiffIndexCoverage,
    stage_tracked_working_diff_epoch,
};
use crate::json_store::{
    JSON_INLINE_MAX_BYTES, JsonRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef,
};
use crate::row_pk::RowPk;
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition, StorageWriteSet};
#[cfg(test)]
use crate::tracked_state::stage_commit_state_manifest;
use crate::tracked_state::{
    CommitDeltaReplacementGeneration, CommitDeltaReplacementScope, CommitStateManifest,
    CommitStateMutationInventory, CommitStateReplayDebt, MaterializedTrackedStateRow,
    TrackedStateCommitDeltaRef, TrackedStateCommitRoot, TrackedStateContext, TrackedStateDeltaRef,
    TrackedStateFilter, TrackedStateKey, TrackedStateKeyRef, TrackedStateReadColumns,
    TrackedStateRootMutationRef, TrackedStateScanRequest, TrackedStateSingleStringReplacementRef,
    encode_key_ref, load_commit_delta_change_records, load_commit_delta_replay_metadata,
    stage_addressable_commit_deltas, stage_change_locators,
    stage_ordered_addressable_commit_deltas,
};
#[cfg(test)]
use crate::transaction::staged_commit_changes::StagedCommitChangeBatchBuilder;
use crate::transaction::staged_commit_changes::{
    StagedCommitChangeBatch, StagedCommitChangeRef, StagedCommitChangeRefs,
};
use crate::transaction::staging::{
    OrderedMutationJournal, PreparedInsertSelection, PreparedWriteSet,
};
use crate::transaction_types::{PreparedStateBatch, PreparedStateRowRef};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;
use tracing::Instrument as _;

type RowIndex = usize;

// Below this size, per-row HOT writes are cheaper and keep repeated ordinary
// INSERT transactions at one point-addressable current-state lookup. At 1K,
// measured HOT publication still emitted one backend put per row, while the
// immutable packed base removes that amplification without entering the
// single-row path. Keep the crossover at a power-of-two boundary below that
// representative bulk size while leaving ordinary point writes unchanged.
const PACKED_CURRENT_BASE_MIN_ROWS: usize = 512;

// Complete replacements retain an authoritative partition generation.
// Ordinary non-empty ordered commits start bounded rootless intervals
// regardless of workload size; replacement generations reset replay
// accounting because exact misses in their scope are authoritative.
const ROOTLESS_MAX_REPLAY_BYTES: u64 = crate::tracked_state::COMMIT_STATE_MAX_REPLAY_BYTES;

fn compare_certified_predecessors(
    left: &crate::hot_state::CertifiedCurrentStatePredecessorRef<'_>,
    right: &crate::hot_state::CertifiedCurrentStatePredecessorRef<'_>,
) -> std::cmp::Ordering {
    left.schema_key
        .cmp(right.schema_key)
        .then_with(|| left.row_pk.cmp(right.row_pk))
        .then_with(|| left.file_id.cmp(&right.file_id))
}

#[cfg(any(test, feature = "storage-benches"))]
std::thread_local! {
    static ORDERED_PACKED_CURRENT_BASE_PUBLICATIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CERTIFIED_COLUMNAR_CURRENT_BASE_PUBLICATIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static COMPLETE_REPLACEMENT_PACKED_CURRENT_BASE_PUBLICATIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static COMPLETE_REPLACEMENT_PACKED_CURRENT_BASE_RETIREMENTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static ROOTLESS_REPLACEMENT_GENERATION_PUBLICATIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
static DIRECT_JOURNAL_REPLACEMENT_PUBLICATIONS: std::sync::LazyLock<
    std::sync::Mutex<BTreeMap<String, usize>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(BTreeMap::new()));

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn take_ordered_packed_current_base_publications() -> usize {
    ORDERED_PACKED_CURRENT_BASE_PUBLICATIONS.with(|publications| publications.replace(0))
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn take_certified_columnar_current_base_publications() -> usize {
    CERTIFIED_COLUMNAR_CURRENT_BASE_PUBLICATIONS.with(|publications| publications.replace(0))
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn take_complete_replacement_packed_current_base_publications() -> usize {
    COMPLETE_REPLACEMENT_PACKED_CURRENT_BASE_PUBLICATIONS
        .with(|publications| publications.replace(0))
}

#[cfg(test)]
pub(crate) fn take_complete_replacement_packed_current_base_retirements() -> usize {
    COMPLETE_REPLACEMENT_PACKED_CURRENT_BASE_RETIREMENTS.with(|retirements| retirements.replace(0))
}

#[cfg(test)]
pub(crate) fn take_rootless_replacement_generation_publications() -> usize {
    ROOTLESS_REPLACEMENT_GENERATION_PUBLICATIONS.with(|count| count.replace(0))
}

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
    let tracked_state = TrackedStateContext::new();
    let commit_parent_heads =
        resolve_prepared_commit_parent_heads(branch_ctx, &*read, &prepared_writes, false).await?;
    let outcome = commit_prepared_writes_with_parent_heads(
        binary_cas,
        &tracked_state,
        None,
        runtime_functions,
        crate::ANONYMOUS_ACCOUNT_ID,
        &commit_parent_heads,
        read,
        &BTreeMap::new(),
        prepared_writes,
    )
    .await?;
    Ok((outcome.writes, outcome.preconditions))
}

/// A materialized commit: the atomic storage write set, the preconditions it
/// must publish under, and the filesystem rows it staged.
pub(crate) struct MaterializedCommit {
    pub(crate) writes: StorageWriteSet,
    pub(crate) preconditions: Vec<StoragePrecondition>,
    /// Filesystem descriptor and blob-ref rows staged by this commit, carrying
    /// their **final** identities.
    ///
    /// Addressable rows only receive their commit-delta change id during
    /// materialization, so the caller's pre-commit `PreparedWriteSet` cannot
    /// supply a projectable delta: it still holds the provisional change id
    /// that `set_ordered_addressable_change_ids` overwrites. Projecting here is
    /// what lets a file *create* advance the cached path index rather than
    /// invalidate it.
    ///
    /// Empty when the commit changes no filesystem row. Non-empty does not by
    /// itself license a projection — a branch-ref move or a change selected in
    /// from another commit alters the visible filesystem without appearing in
    /// these rows, and the caller must rebuild for those shapes.
    pub(crate) filesystem_delta_rows: Vec<MaterializedHotStateRow>,
}

/// Materializes a prepared commit with branch heads already resolved from the
/// caller's coherent commit snapshot.
pub(crate) async fn commit_prepared_writes_with_parent_heads(
    binary_cas: &BinaryCasContext,
    tracked_state: &TrackedStateContext,
    row_schema_catalog: Option<&crate::catalog::CatalogSnapshot>,
    runtime_functions: Option<&FunctionContext>,
    active_account_id: &str,
    commit_parent_heads: &BTreeMap<String, Option<CommitId>>,
    read: &mut impl StorageAdapterRead,
    branch_checkpoint_bridges: &BTreeMap<String, crate::gc::CheckpointRecoveryRef>,
    prepared_writes: PreparedWriteSet,
) -> Result<MaterializedCommit, LixError> {
    validate_prepared_permission_grant_rows(&prepared_writes)?;
    Box::pin(validate_active_account_and_account_rows(
        read,
        &prepared_writes,
        active_account_id,
    ))
    .await?;
    Box::pin(validate_account_deletions(
        read,
        &prepared_writes,
        active_account_id,
    ))
    .await?;
    let certified_fresh_plugin_file_id =
        crate::transaction::validation::fresh_plugin_file_import_certificate(&prepared_writes)
            .is_some()
            .then(|| prepared_writes.file_content_writes[0].file_id.clone());
    let mut host_certified_file_schemas =
        BTreeMap::<String, BTreeMap<String, BTreeSet<String>>>::new();
    let mut host_certified_live_increments =
        BTreeMap::<String, BTreeMap<(String, Option<String>), u64>>::new();
    for file in &prepared_writes.file_content_writes {
        for batch in file.certified_row_batches().iter().filter(|batch| {
            batch.complete_file_state
                && matches!(
                    batch.format,
                    1 | crate::plugin::runtime::HOST_CERTIFIED_PACKET_FORMAT
                        | crate::plugin::runtime::HOST_CERTIFIED_ZSTD_PACKET_FORMAT
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
    // Which account rows are visible changes when an account row is written
    // and when a branch ref moves (the account rows live in the global
    // branch's tracked state, so a head move can change the view without any
    // account row in this commit). Both cases rotate the token; ordinary CRUD
    // does not, which is the whole point.
    let account_view_changed = prepared_writes.state_rows.iter().any(|row| {
        matches!(
            row.schema_key.as_str(),
            ACCOUNT_SCHEMA_KEY | BRANCH_REF_SCHEMA_KEY
        )
    }) || prepared_writes
        .commit_change_refs_by_branch
        .values()
        .flat_map(StagedCommitChangeRefs::selected_changes)
        .any(|change_ref| change_ref.schema_key() == ACCOUNT_SCHEMA_KEY);
    let mut state_rows = prepared_writes.state_rows;
    #[cfg(feature = "storage-benches")]
    state_rows.record_ownership(crate::storage_bench::CRUD_OWNERSHIP_AUTHORITY);
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
    let mut row_columnar_write_sets =
        prepare_row_columnar_write_sets(&mut state_rows, &insert_selection, row_schema_catalog)?;
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
    let mut certified_packet_root_rows = BTreeMap::<CommitId, Vec<MaterializedHotStateRow>>::new();
    let mut certified_replacement_markers = BTreeMap::<CommitId, BTreeSet<TrackedStateKey>>::new();
    for file in prepared_writes
        .file_content_writes
        .iter()
        .filter(|file| !file.certified_row_batches().is_empty())
    {
        let root = tracked_roots
            .iter()
            .find(|root| root.publish_head && root.branch_id == file.branch_id)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "certified row batch has no matching published commit",
                )
            })?;
        let timestamp = commit_rows
            .iter()
            .find(|commit| commit.commit_id == root.commit_id)
            .map(|commit| commit.created_at)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "certified row batch commit has no timestamp",
                )
            })?;
        let mut expanded_rows = Vec::new();
        let mut replacement_schemas = BTreeSet::new();
        for batch in file
            .certified_row_batches()
            .iter()
            .filter(|batch| certified_batch_requires_root_expansion(batch))
        {
            if batch.complete_file_state {
                replacement_schemas.extend(batch.schema_keys.iter().cloned());
            }
            expanded_rows.extend(
                crate::hot_state::materialize_certified_root_rows(
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
                    row_pk: marker.row_pk.clone(),
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
                    row.row_pk.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        rows.retain(|row| {
            !ordinary_identities.contains(&(
                row.schema_key.clone(),
                row.file_id.clone(),
                row.row_pk.clone(),
            ))
        });
        rows.sort_unstable_by(|left, right| {
            (&left.schema_key, &left.file_id, &left.row_pk).cmp(&(
                &right.schema_key,
                &right.file_id,
                &right.row_pk,
            ))
        });
        if rows.windows(2).any(|pair| {
            (&pair[0].schema_key, &pair[0].file_id, &pair[0].row_pk)
                == (&pair[1].schema_key, &pair[1].file_id, &pair[1].row_pk)
        }) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "certified row batches contain duplicate root identities",
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
        return Ok(MaterializedCommit {
            writes,
            preconditions,
            filesystem_delta_rows: Vec::new(),
        });
    }

    let selected_change_records = load_selected_change_records(read, &commit_rows).await?;
    let mut replacement_generations = certify_complete_replacement_generations(
        read,
        &state_rows,
        &row_index.tracked_row_indices_by_commit,
        &tracked_roots,
    )
    .await?;
    for (commit_id, generation) in
        certify_ordered_journal_replacement_generations(read, &ordered_replacements, &tracked_roots)
            .await?
    {
        if replacement_generations
            .insert(commit_id, generation)
            .is_some()
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "commit has both prepared and immutable replacement authorities",
            ));
        }
    }

    let checkpoint_commit_ids = prepared_writes
        .checkpoint_publications
        .iter()
        .map(|publication| publication.recovery_ref.checkpoint_commit_id)
        .collect::<BTreeSet<_>>();
    let staged_delta_index = Box::pin(stage_tracked_commit_delta_index(
        read,
        &mut writes,
        &mut state_rows,
        &mut row_columnar_write_sets,
        &row_index.tracked_row_indices_by_commit,
        &tracked_roots,
        &commit_rows,
        &selected_change_records,
        &certified_packet_root_rows,
        &certified_packet_json_refs,
        &insert_selection,
        &replacement_generations,
        &ordered_replacements,
        &checkpoint_commit_ids,
    ))
    .await?;

    let mut rootless_ordered_commits = select_new_rootless_ordered_commits(
        &state_rows,
        &row_index.tracked_row_indices_by_commit,
        &tracked_roots,
        &staged_delta_index.ordered_addressable_commits,
        &certified_packet_root_rows,
        &ordered_replacements,
    );
    let replacement_generation_commits = replacement_generations
        .keys()
        .filter(|commit_id| rootless_ordered_commits.contains(commit_id))
        .copied()
        .collect::<BTreeSet<_>>();
    let mut durable_root_rebuild_parents = BTreeSet::new();
    let mut staged_root_rebuild_commits = BTreeSet::new();
    let mut external_parent_manifests = BTreeMap::new();

    let staged_commits = Box::pin(
        stage_changelog_commits(
            read,
            &mut writes,
            &state_rows,
            &branch_head_changes,
            &engine_rows,
            &[],
            &mut rootless_ordered_commits,
            &replacement_generation_commits,
            &mut durable_root_rebuild_parents,
            &mut staged_root_rebuild_commits,
            &row_index.tracked_row_indices_by_commit,
            &commit_rows,
            &certified_packet_root_rows,
            &staged_delta_index.inventories,
            &ordered_replacements,
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
        materialize_selected_change_payloads(read, &selected_change_records).await?;

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

    let staged_snapshot_roots = Box::pin(
        stage_tracked_roots(
            tracked_state,
            read,
            &mut writes,
            &state_rows,
            &row_index.tracked_row_indices_by_commit,
            &tracked_roots,
            &rootless_ordered_commits,
            &durable_root_rebuild_parents,
            &staged_root_rebuild_commits,
            &staged_commits,
            &insert_selection,
            &certified_packet_root_rows,
            &certified_replacement_markers,
        )
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.materialization.tracked_roots"
        )),
    )
    .await?;
    stage_commit_state_manifests(
        read,
        &mut writes,
        &commit_rows,
        &staged_delta_index.inventories,
        &rootless_ordered_commits,
        &staged_commits,
        &staged_snapshot_roots,
        &external_parent_manifests,
    )
    .await?;
    // HOT publication has adapter-specific checkpoint, packed-base, and
    // point-row futures. Keep their combined async state out of the parent
    // commit future so an inactive bulk branch cannot inflate every ordinary
    // SlateDB transaction's native stack.
    let mut staged_hot_heads = Box::pin(stage_tracked_head(
        read,
        &mut writes,
        &state_rows,
        &row_columnar_write_sets,
        &engine_rows,
        &row_index.tracked_row_indices_by_commit,
        &row_index.tracked_delete_indices_by_commit,
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
        &staged_delta_index.inventories,
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
            file.certified_row_batches()
                .iter()
                .flat_map(|batch| batch.schema_keys.iter().map(String::as_str)),
        );
    }
    stage_checkpoint_working_diff_epochs(
        &mut writes,
        &prepared_writes.checkpoint_publications,
        &staged_hot_heads.controls,
    )
    .await?;
    let mut root_backed_branch_publications = BTreeSet::new();
    let published_branch_controls = stage_branch_head_control_publications(
        read,
        &mut writes,
        &staged_hot_heads.controls,
        &state_rows,
        &engine_rows,
        &explicit_branch_targets,
        &insert_selection,
        &prepared_writes.checkpoint_publications,
        branch_checkpoint_bridges,
        &mut preconditions,
        &branch_control_observations,
        &mut root_backed_branch_publications,
    )
    .await?;
    // The binary-CAS publication fence used to ride along with the reachability
    // delta writer. It is an authenticated root-publication fence in its own
    // right, so it stages here even when the transition only revives an
    // existing commit and stages no blob bytes.
    crate::binary_cas::stage_cas_publication_fence(read, &mut writes, &mut preconditions).await?;
    // The out-of-band JSON payload fence, for the same reason and staged
    // unconditionally for the same reason: payload rows are content addressed,
    // so this transition may resolve onto a row it did not write, and a sweep
    // planned before it must not be able to reclaim that row underneath it.
    crate::json_store::stage_json_publication_fence(read, &mut writes, &mut preconditions).await?;
    if !published_branch_controls.contains_key(crate::GLOBAL_BRANCH_ID) {
        let global = branch_control_observations
            .get(crate::GLOBAL_BRANCH_ID)
            .expect("global branch control is always observed");
        preconditions.push(branch_head_control_precondition(
            crate::GLOBAL_BRANCH_ID,
            global.raw_token.clone(),
        )?);
    }
    let commit_created_at = commit_rows
        .iter()
        .map(|commit| (commit.commit_id, commit.created_at))
        .collect::<BTreeMap<_, _>>();
    let certified_files = prepared_writes
        .file_content_writes
        .iter()
        .map(|file| crate::hot_state::CertifiedRowBatchFileRef {
            branch_id: &file.branch_id,
            file_id: &file.file_id,
            batches: file.certified_row_batches(),
        })
        .collect::<Vec<_>>();
    crate::hot_state::stage_certified_row_batches(
        read,
        &mut writes,
        &certified_files,
        &published_branch_controls,
        &branch_control_observations,
        &commit_created_at,
        &root_backed_branch_publications,
    )
    .await?;
    #[cfg(feature = "storage-benches")]
    state_rows.record_ownership(crate::storage_bench::CRUD_OWNERSHIP_ROOT_PUBLICATION);
    // Every staging pass that can rewrite a row's identity has run, so these
    // rows now match what a cold rebuild would read back out of hot state.
    // `stage_tracked_commit_delta_index` above is the one that matters: it
    // replaces the provisional change id every addressable row carried into
    // this function with the row's commit-delta address.
    let filesystem_delta_rows = if filesystem_view_changed {
        state_rows
            .iter()
            .filter(|row| {
                matches!(
                    row.schema_key.as_str(),
                    "lix_file_descriptor" | "lix_directory_descriptor" | "lix_binary_blob_ref"
                )
            })
            .map(MaterializedHotStateRow::from)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    if !staged_hot_heads.deferred_fresh_hot_plans.is_empty() {
        if staged_hot_heads.deferred_fresh_hot_plans.len() != 1 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "one certified fresh import produced multiple deferred hot publications",
            ));
        }
        let state_rows = Arc::new(state_rows);
        let plan = staged_hot_heads
            .deferred_fresh_hot_plans
            .pop()
            .expect("one deferred fresh hot publication was counted");
        writes.stage_deferred_final_put_source(plan.into_source(state_rows))?;
    }
    if filesystem_view_changed {
        stage_path_index_revision(&mut writes);
    }
    if account_view_changed {
        crate::account::stage_account_revision(&mut writes);
    }
    Ok(MaterializedCommit {
        writes,
        preconditions,
        filesystem_delta_rows,
    })
}

fn certified_batch_requires_root_expansion(
    batch: &crate::plugin::runtime::WasmCertifiedRowBatch,
) -> bool {
    !matches!(
        batch.format,
        crate::plugin::runtime::HOST_CERTIFIED_PACKET_FORMAT
            | crate::plugin::runtime::HOST_CERTIFIED_ZSTD_PACKET_FORMAT
    )
}

fn certified_collection_replacement_marker(
    branch_id: &str,
    file_id: &str,
    schema_key: &str,
    commit_id: CommitId,
    timestamp: LixTimestamp,
) -> Result<MaterializedHotStateRow, LixError> {
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
    Ok(MaterializedHotStateRow {
        row_pk: RowPk::single(scope_key),
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
                &row.change.row_pk,
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
                    row.row_pk,
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
    certified_rows_by_commit: &BTreeMap<CommitId, Vec<MaterializedHotStateRow>>,
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
    rows_by_commit: &BTreeMap<CommitId, Vec<MaterializedHotStateRow>>,
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
    tracked_delete_indices_by_commit: BTreeMap<CommitId, Vec<RowIndex>>,
}

fn index_prepared_rows(rows: &PreparedStateBatch) -> Result<PreparedRowIndex, LixError> {
    let mut tracked_row_indices_by_commit = BTreeMap::<CommitId, Vec<RowIndex>>::new();
    let mut tracked_delete_indices_by_commit = BTreeMap::<CommitId, Vec<RowIndex>>::new();

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
        if row.snapshot.is_none() {
            tracked_delete_indices_by_commit
                .entry(*commit_id)
                .or_default()
                .push(row_index);
        }
    }

    Ok(PreparedRowIndex {
        tracked_row_indices_by_commit,
        tracked_delete_indices_by_commit,
    })
}

#[derive(Clone, Debug)]
struct StagedChangelogCommit {
    record: CommitRecord,
    replay_debt: CommitStateReplayDebt,
    change_count: usize,
    selected_change_batches: Vec<StagedCommitChangeBatch>,
}

struct StagedCommitDeltaIndex {
    ordered_addressable_commits: BTreeSet<CommitId>,
    inventories: BTreeMap<CommitId, CommitStateMutationInventory>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SelectedChangeKey {
    source_commit_id: CommitId,
    identity: TrackedStateKey,
}

fn selected_change_key(change_ref: StagedCommitChangeRef<'_>) -> SelectedChangeKey {
    SelectedChangeKey {
        source_commit_id: change_ref.source_commit_id,
        identity: TrackedStateKey {
            schema_key: change_ref.schema_key().to_owned(),
            file_id: change_ref.file_id().map(str::to_owned),
            row_pk: change_ref.row_pk().clone(),
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
    rootless_commit_ids: &mut BTreeSet<CommitId>,
    replacement_generation_commit_ids: &BTreeSet<CommitId>,
    durable_root_rebuild_parents: &mut BTreeSet<CommitId>,
    staged_root_rebuild_commits: &mut BTreeSet<CommitId>,
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    commit_rows: &[FinalizedCommitRow],
    certified_packet_root_rows: &BTreeMap<CommitId, Vec<MaterializedHotStateRow>>,
    mutation_inventories: &BTreeMap<CommitId, CommitStateMutationInventory>,
    ordered_replacements: &BTreeMap<CommitId, Arc<OrderedMutationJournal>>,
    external_parent_manifests: &mut BTreeMap<
        CommitId,
        crate::tracked_state::PublishedCommitStateTopology,
    >,
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
    let mut first_parent_jumps = BTreeMap::new();
    let mut topology_records = BTreeMap::new();
    let mut touched_scope_digests = BTreeMap::<CommitId, CommitTouchedScopeDigest>::new();
    let mut rootless_depths = BTreeMap::new();
    let mut rootless_rows = BTreeMap::new();
    let mut rootless_bytes = BTreeMap::new();
    for (commit_id, record) in external_parent_records {
        let record = record.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("commit '{commit_id}' has a missing parent"),
            )
        })?;
        let published =
            crate::tracked_state::load_published_commit_state_topology(read, *commit_id)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("commit '{commit_id}' has no commit-state authority"),
                    )
                })?;
        // Replay debt is physical layout policy. Rootless commits remain
        // bounded-replay layouts; rooted commits publish their canonical
        // snapshot metadata inside immutable physical authority.
        if published.commit_id() != record.commit_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "commit '{commit_id}' topology projection disagrees with commit-state authority"
                ),
            ));
        }
        generations.insert(*commit_id, record.generation);
        first_parent_jumps.insert(
            *commit_id,
            (
                record.first_parent_jump_commit_id,
                record.first_parent_jump_span,
            ),
        );
        topology_records.insert(*commit_id, record.clone());
        let replay_debt = published.replay_debt();
        rootless_depths.insert(*commit_id, replay_debt.depth);
        rootless_rows.insert(*commit_id, replay_debt.rows);
        rootless_bytes.insert(*commit_id, replay_debt.bytes);
        external_parent_manifests.insert(*commit_id, published);
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
        let parent_record = match commit.parent_commit_ids.as_slice() {
            [parent_commit_id] => Some(
                topology_records
                    .get(parent_commit_id)
                    .cloned()
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("commit '{commit_id}' has missing parent jump metadata"),
                        )
                    })?,
            ),
            _ => None,
        };
        if let Some(parent) = &parent_record
            && !topology_records.contains_key(&parent.first_parent_jump_commit_id)
        {
            let jump_id = parent.first_parent_jump_commit_id;
            let loaded = ChangelogContext::new()
                .reader(&mut *read)
                .load_commits(ChangelogCommitLoadRequest {
                    commit_ids: std::slice::from_ref(&jump_id),
                })
                .await?;
            let jump = loaded
                .into_iter()
                .next()
                .and_then(|(_, record)| record)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("commit '{commit_id}' has a missing jump target '{jump_id}'"),
                    )
                })?;
            topology_records.insert(jump_id, jump);
        }
        let parent_jump_record = parent_record.as_ref().map(|parent| {
            topology_records
                .get(&parent.first_parent_jump_commit_id)
                .expect("loaded parent jump target")
        });
        let first_parent_jump = crate::changelog::next_first_parent_jump(
            commit_id,
            &commit.parent_commit_ids,
            parent_record.as_ref(),
            parent_jump_record,
        )?;
        let selected_as_new_rootless = rootless_commit_ids.contains(&commit_id);
        let commit_delta_rows = tracked_row_indices_by_commit
            .get(&commit_id)
            .map_or(0, Vec::len)
            .checked_add(
                ordered_replacements
                    .get(&commit_id)
                    .map_or(0, |journal| journal.row_count()),
            )
            .and_then(|rows| {
                rows.checked_add(
                    certified_packet_root_rows
                        .get(&commit_id)
                        .map_or(0, Vec::len),
                )
            })
            .and_then(|rows| {
                rows.checked_add(selected_change_count(&commit.selected_change_batches))
            })
            .and_then(|rows| u64::try_from(rows).ok())
            .ok_or_else(|| LixError::unknown("tracked-state rootless row count exceeds u64"))?;
        let commit_row_indices = tracked_row_indices_by_commit
            .get(&commit_id)
            .map(Vec::as_slice);
        let commit_delta_bytes = if let Some(journal) = ordered_replacements.get(&commit_id) {
            journal.replacement_proof().replay_bytes
        } else if let Some(proof) = state_rows
            .complete_collection_replacement_proof()
            .filter(|_| commit_row_indices.is_some_and(|indices| indices.len() == state_rows.len()))
        {
            #[cfg(debug_assertions)]
            debug_assert_eq!(
                proof.replay_bytes,
                replay_bytes_for_rows(state_rows, commit_row_indices)?
            );
            proof.replay_bytes
        } else {
            replay_bytes_for_rows(state_rows, commit_row_indices)?
        };
        let has_unbounded_payload_sources = certified_packet_root_rows
            .get(&commit_id)
            .is_some_and(|rows| !rows.is_empty())
            || selected_change_count(&commit.selected_change_batches) > 0
            || mutation_inventories
                .get(&commit_id)
                .and_then(CommitStateMutationInventory::selected_source_commit_id)
                .is_some();
        let first_parent = commit.parent_commit_ids.first().copied();
        let parent_rootless_depth = first_parent
            .and_then(|parent| rootless_depths.get(&parent).copied())
            .unwrap_or(0);
        let parent_rootless_rows = first_parent
            .and_then(|parent| rootless_rows.get(&parent).copied())
            .unwrap_or(0);
        let parent_rootless_bytes = first_parent
            .and_then(|parent| rootless_bytes.get(&parent).copied())
            .unwrap_or(0);
        let next_rootless_rows = parent_rootless_rows
            .checked_add(commit_delta_rows)
            .ok_or_else(|| LixError::unknown("tracked-state rootless row count exceeds u64"))?;
        let next_rootless_bytes = parent_rootless_bytes
            .checked_add(commit_delta_bytes)
            .ok_or_else(|| LixError::unknown("tracked-state rootless byte count exceeds u64"))?;
        let (rootless_depth, cumulative_rootless_rows, cumulative_rootless_bytes) =
            if selected_as_new_rootless
                && replacement_generation_commit_ids.contains(&commit_id)
                && commit_delta_bytes <= ROOTLESS_MAX_REPLAY_BYTES
                && !has_unbounded_payload_sources
            {
                // A persisted partition replacement is a new immutable base
                // generation. Reads stop at its scope descriptor, so prior
                // rows in that partition do not contribute to replay cost.
                #[cfg(test)]
                ROOTLESS_REPLACEMENT_GENERATION_PUBLICATIONS.with(|count| {
                    count.set(count.get().saturating_add(1));
                });
                (1, commit_delta_rows, commit_delta_bytes)
            } else if parent_rootless_depth > 0 {
                let next_depth = parent_rootless_depth
                    .checked_add(1)
                    .ok_or_else(|| LixError::unknown("tracked-state rootless depth exceeds u16"))?;
                if next_depth <= crate::tracked_state::COMMIT_STATE_MAX_REPLAY_DEPTH
                    && next_rootless_bytes <= crate::tracked_state::COMMIT_STATE_MAX_REPLAY_BYTES
                    && !has_unbounded_payload_sources
                {
                    rootless_commit_ids.insert(commit_id);
                    (next_depth, next_rootless_rows, next_rootless_bytes)
                } else {
                    // Closing an interval wins over selecting another large bulk
                    // commit as a fresh seed. Rebuild any staged prefix directly;
                    // durable ancestors are replayed once before roots are staged.
                    rootless_commit_ids.remove(&commit_id);
                    let mut cursor = first_parent.expect("positive rootless depth has a parent");
                    loop {
                        if let Some(staged_parent) = commit_rows_by_id.get(&cursor) {
                            staged_root_rebuild_commits.insert(cursor);
                            rootless_commit_ids.remove(&cursor);
                            rootless_depths.insert(cursor, 0);
                            rootless_rows.insert(cursor, 0);
                            rootless_bytes.insert(cursor, 0);
                            let Some(parent) = staged_parent.parent_commit_ids.first().copied()
                            else {
                                break;
                            };
                            cursor = parent;
                        } else {
                            if rootless_depths.get(&cursor).copied().unwrap_or(0) > 0 {
                                durable_root_rebuild_parents.insert(cursor);
                            }
                            break;
                        }
                    }
                    (0, 0, 0)
                }
            } else if selected_as_new_rootless
                && commit_delta_bytes <= crate::tracked_state::COMMIT_STATE_MAX_REPLAY_BYTES
                && !has_unbounded_payload_sources
            {
                (1, commit_delta_rows, commit_delta_bytes)
            } else {
                rootless_commit_ids.remove(&commit_id);
                (0, 0, 0)
            };
        rootless_depths.insert(commit_id, rootless_depth);
        rootless_rows.insert(commit_id, cumulative_rootless_rows);
        rootless_bytes.insert(commit_id, cumulative_rootless_bytes);
        generations.insert(commit_id, generation);
        first_parent_jumps.insert(commit_id, first_parent_jump);
        // The membership test history reads is published here, from the same
        // certified inventory the commit already staged. Deriving it costs one
        // walk of the inventory's own bounds; publishing it saves history one
        // replay-state point-read pair per reached commit.
        let touched_scope_digest = match mutation_inventories.get(&commit_id) {
            Some(inventory) => {
                match crate::tracked_state::commit_delta_member_scopes(commit_id, inventory)? {
                    Some(scopes) => CommitTouchedScopeDigest::exact(scopes.iter()),
                    None => CommitTouchedScopeDigest::opaque(),
                }
            }
            // A staged commit with no mutation inventory has no delta members,
            // so nothing a history read asks for can be found in it.
            None => CommitTouchedScopeDigest::exact(std::iter::empty()),
        };
        touched_scope_digests.insert(commit_id, touched_scope_digest.clone());
        topology_records.insert(
            commit_id,
            CommitRecord {
                format_version: COMMIT_RECORD_FORMAT_VERSION,
                commit_id,
                generation,
                parent_commit_ids: commit.parent_commit_ids.clone(),
                first_parent_jump_commit_id: first_parent_jump.0,
                first_parent_jump_span: first_parent_jump.1,
                account_id: active_account_id.to_string(),
                created_at: commit.created_at,
                touched_scope_digest,
            },
        );
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
            format_version: COMMIT_RECORD_FORMAT_VERSION,
            commit_id: commit_row.commit_id,
            generation,
            parent_commit_ids: commit_row.parent_commit_ids.clone(),
            first_parent_jump_commit_id: first_parent_jumps[&commit_row.commit_id].0,
            first_parent_jump_span: first_parent_jumps[&commit_row.commit_id].1,
            account_id: active_account_id.to_string(),
            created_at: commit_row.created_at,
            touched_scope_digest: touched_scope_digests[&commit_row.commit_id].clone(),
        };
        commits.push(record.clone());
        let change_count = state_row_indices.len()
            + ordered_replacements
                .get(&commit_row.commit_id)
                .map_or(0, |journal| journal.row_count())
            + certified_packet_root_rows
                .get(&commit_row.commit_id)
                .map_or(0, Vec::len)
            + selected_change_count(&commit_row.selected_change_batches);
        staged.insert(
            commit_row.commit_id,
            StagedChangelogCommit {
                record,
                replay_debt: CommitStateReplayDebt {
                    depth: rootless_depths[&commit_row.commit_id],
                    rows: rootless_rows[&commit_row.commit_id],
                    bytes: rootless_bytes[&commit_row.commit_id],
                },
                change_count,
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
            row.row_pk,
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
            change_ref.row_pk(),
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
        row_pk: row.row_pk,
        schema_key: row.schema_key,
        file_id: row.file_id.map(crate::common::SharedStr::as_str),
        snapshot: row.snapshot.map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction_types::StageJson::slot_ref,
        ),
        metadata: row.metadata.map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction_types::StageJson::slot_ref,
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
        row_pk: RowPk::uuid_from_canonical(&root.branch_id).map_err(|error| {
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
    let row_pk = RowPk::single(crate::functions::DETERMINISTIC_SEQUENCE_KEY);
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
            row_pk,
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
        .map(crate::hot_state::CertifiedCurrentStatePredecessor::created_at)
        .transpose()?
        .unwrap_or(row.created_at);
    Ok(TrackedStateDeltaRef {
        schema_key: row.schema_key,
        file_id: row.file_id.map(crate::common::SharedStr::as_str),
        row_pk: row.row_pk,
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
            crate::transaction_types::StageJson::slot_ref,
        ),
        metadata: row.metadata.map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction_types::StageJson::slot_ref,
        ),
        origin_key: row.origin_key.map(crate::common::SharedStr::as_str),
        base_coordinate: None,
        authored: true,
    })
}

fn tracked_delta_from_certified_root_row(
    row: &MaterializedHotStateRow,
) -> Result<TrackedStateDeltaRef<'_>, LixError> {
    Ok(TrackedStateDeltaRef {
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        row_pk: &row.row_pk,
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
    row: &'a MaterializedHotStateRow,
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

fn tracked_delta_from_selected_change_ref(
    change_ref: StagedCommitChangeRef<'_>,
    commit_id: CommitId,
) -> Result<TrackedStateDeltaRef<'_>, LixError> {
    Ok(TrackedStateDeltaRef {
        schema_key: change_ref.schema_key(),
        file_id: change_ref.file_id(),
        row_pk: change_ref.row_pk(),
        change_id: change_ref.change_id,
        commit_id,
        deleted: change_ref.deleted,
        created_at: change_ref.created_at,
        updated_at: change_ref.updated_at,
    })
}

fn tracked_commit_delta_from_selected_change_ref<'a>(
    change_ref: StagedCommitChangeRef<'a>,
    commit_id: CommitId,
    record: Option<&'a ChangeRecord>,
) -> Result<TrackedStateCommitDeltaRef<'a>, LixError> {
    if record.is_some_and(|record| record.change_id != change_ref.change_id) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "selected commit delta payload has the wrong change id",
        ));
    }
    if record.is_none() && !change_ref.deleted {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "live selected commit delta is missing its payload",
        ));
    }
    Ok(TrackedStateCommitDeltaRef {
        delta: TrackedStateDeltaRef {
            schema_key: change_ref.schema_key(),
            file_id: change_ref.file_id(),
            row_pk: change_ref.row_pk(),
            change_id: change_ref.change_id,
            commit_id,
            deleted: change_ref.deleted,
            created_at: change_ref.created_at,
            updated_at: change_ref.updated_at,
        },
        snapshot: record.map_or(crate::json_store::JsonSlotRef::None, |record| {
            record.snapshot.as_ref_slot()
        }),
        metadata: record.map_or(crate::json_store::JsonSlotRef::None, |record| {
            record.metadata.as_ref_slot()
        }),
        origin_key: record.and_then(|record| record.origin_key.as_deref()),
        base_coordinate: None,
        authored: false,
    })
}

/// Builds this commit's index entries, plus witnesses for schemas it registers.
///
/// Put-only: rows whose indexed value changed publish a new entry and leave the
/// superseded one behind, and deleted rows publish nothing. Both cases are
/// resolved on read by re-checking candidates against the row, which is what
/// lets this stay one pass over the commit's own rows with no reads.
///
/// The values arrive **pre-extracted** on the batch. Transaction validation
/// already parses every staged snapshot, and `StageJson::value()` panics once
/// that decoded column is released, so decoding again here would be both a
/// second full parse of the commit and a deliberate route around that guard.
/// A batch whose rows never reached extraction carries an empty
/// [`StagedIndexValues`] and therefore publishes nothing at all — no entries
/// and, critically, no witnesses, so those collections keep scanning instead of
/// trusting an incomplete index.
fn hot_index_writes_for_commit(
    state_rows: &PreparedStateBatch,
    branch_id: &str,
    parent_control: Option<&BranchHeadControl>,
) -> (
    Vec<crate::hot_state::HotIndexEntry>,
    BTreeSet<(String, u16)>,
) {
    let staged = state_rows.staged_index_values();
    let mut entries = Vec::new();
    let mut witnesses = staged.registered_collections.clone();
    for row in &staged.rows {
        if row.branch_id.as_str() != branch_id {
            continue;
        }
        // A schema the parent generation proves absent has an empty
        // collection in this branch, so indexing it from this commit forward
        // is complete and the witness is free. The bloom filter has no false
        // negatives, so "absent" here is a proof, not a guess. A collection
        // that predates this plane never gets a witness and keeps scanning.
        let collection_starts_here =
            parent_control.is_none_or(|control| !control.may_have_schema(row.schema_key.as_str()));
        for (ordinal, value) in &row.columns {
            if collection_starts_here {
                witnesses.insert((row.schema_key.as_str().to_owned(), *ordinal));
            }
            let Some(value) = value else {
                continue;
            };
            entries.push(crate::hot_state::HotIndexEntry {
                schema_key: row.schema_key.as_str().to_owned(),
                ordinal: *ordinal,
                value: value.clone(),
                row_pk: row.row_pk.clone(),
            });
        }
    }
    (entries, witnesses)
}

fn current_state_delta_from_state_row(
    row: PreparedStateRowRef<'_>,
) -> Result<crate::hot_state::CurrentStateDeltaRef<'_>, LixError> {
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
    Ok(crate::hot_state::CurrentStateDeltaRef {
        schema_key: row.schema_key,
        file_id: row.file_id.map(crate::common::SharedStr::as_str),
        row_pk: row.row_pk,
        // Untracked rows are identity-bearing but history-free: the minted
        // change_id travels with the row, while commit_id stays absent so the
        // row never joins the commit graph. Changelog exclusion is enforced
        // separately, by the addressable-change filter, not by dropping this.
        change_id: Some(change_id),
        commit_id,
        untracked: row.untracked,
        deleted: row.snapshot.is_none(),
        created_at: row.created_at,
        updated_at: row.updated_at,
        snapshot: row.snapshot.map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction_types::StageJson::slot_ref,
        ),
        metadata: row.metadata.map_or(
            crate::json_store::JsonSlotRef::None,
            crate::transaction_types::StageJson::slot_ref,
        ),
        columnar_base_coordinate: None,
    })
}

fn host_certified_batch_owns_live_row(
    row: PreparedStateRowRef<'_>,
    branch_id: &str,
    certified_commit_id: CommitId,
    host_certified_file_schemas: &BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
) -> bool {
    // A complete certified batch replaces only the incoming live rows. An
    // ownership transition may stage tombstones for the previous plugin under
    // the same file/schema pair; those must remain HOT overlays so the old
    // row identities disappear and collection counts are decremented.
    row.commit_id == Some(certified_commit_id)
        && row.snapshot.is_some()
        && row.file_id.is_some_and(|file_id| {
            host_certified_file_schemas
                .get(branch_id)
                .and_then(|files| files.get(file_id.as_str()))
                .is_some_and(|schemas| schemas.contains(row.schema_key.as_str()))
        })
}

impl crate::hot_state::DeferredFreshHotRows for PreparedStateBatch {
    fn row(&self, index: usize) -> crate::hot_state::DeferredFreshHotRowRef<'_> {
        let row = PreparedStateBatch::row(self, index);
        crate::hot_state::DeferredFreshHotRowRef {
            branch_id: row.branch_id.as_str(),
            delta: crate::hot_state::CurrentStateDeltaRef {
                schema_key: row.schema_key,
                file_id: row.file_id.map(crate::common::SharedStr::as_str),
                row_pk: row.row_pk,
                change_id: row.change_id,
                commit_id: row.commit_id,
                untracked: row.untracked,
                deleted: row.snapshot.is_none(),
                created_at: row.created_at,
                updated_at: row.updated_at,
                snapshot: row.snapshot.map_or(
                    crate::json_store::JsonSlotRef::None,
                    crate::transaction_types::StageJson::slot_ref,
                ),
                metadata: row.metadata.map_or(
                    crate::json_store::JsonSlotRef::None,
                    crate::transaction_types::StageJson::slot_ref,
                ),
                columnar_base_coordinate: None,
            },
        }
    }
}

fn current_state_delta_from_engine_row(
    row: &EngineCurrentRow,
) -> crate::hot_state::CurrentStateDeltaRef<'_> {
    crate::hot_state::CurrentStateDeltaRef {
        schema_key: &row.change.schema_key,
        file_id: row.change.file_id.as_deref(),
        row_pk: &row.change.row_pk,
        // Engine rows arrive with their change record already built, so the id
        // is authoritative here. This lane runs beside — not through — the
        // prepared-row funnel, so the id has to be carried across explicitly.
        change_id: Some(row.change.change_id),
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

/// Stages a compact, identity-addressable change record for every tracked
/// commit. Immutable roots are optional cold accelerators; this index is the
/// authoritative point-read and reconstruction structure for rootless
/// first-parent history.
async fn load_selected_change_records(
    read: &(impl StorageAdapterRead + ?Sized),
    commit_rows: &[FinalizedCommitRow],
) -> Result<HashMap<SelectedChangeKey, ChangeRecord>, LixError> {
    let mut by_source_commit = BTreeMap::<CommitId, Vec<StagedCommitChangeRef<'_>>>::new();
    for change_ref in commit_rows
        .iter()
        .flat_map(|commit| selected_changes(&commit.selected_change_batches))
    {
        // Identity and timestamps fully describe a selected tombstone.
        // Historical rows may be absent or retain metadata, while checkpoint
        // HOT tombstones must carry no payload.
        if change_ref.deleted {
            continue;
        }
        by_source_commit
            .entry(change_ref.source_commit_id)
            .or_default()
            .push(change_ref);
    }

    let mut records = HashMap::new();
    for (source_commit_id, change_refs) in by_source_commit {
        let keys = change_refs
            .iter()
            .map(|change_ref| TrackedStateKey {
                schema_key: change_ref.schema_key().to_owned(),
                file_id: change_ref.file_id().map(str::to_owned),
                row_pk: change_ref.row_pk().clone(),
            })
            .collect::<Vec<_>>();
        let loaded = load_commit_delta_change_records(read, source_commit_id, &keys).await?;
        for (change_ref, record) in change_refs.into_iter().zip(loaded) {
            let Some(record) = record else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "selected change '{}' for ({:?}, {:?}, {:?}) has no authoritative payload at source commit '{}'",
                        change_ref.change_id,
                        change_ref.schema_key(),
                        change_ref.file_id(),
                        change_ref.row_pk(),
                        source_commit_id
                    ),
                ));
            };
            if record.change_id != change_ref.change_id
                || record.schema_key != change_ref.schema_key()
                || record.file_id.as_deref() != change_ref.file_id()
                || record.row_pk != *change_ref.row_pk()
                || record.snapshot.is_none() != change_ref.deleted
                || record.created_at != change_ref.updated_at
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "selected change '{}' does not match its authoritative source payload",
                        change_ref.change_id
                    ),
                ));
            }
            let key = selected_change_key(change_ref);
            if let Some(existing) = records.insert(key, record.clone())
                && existing != record
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
    }
    Ok(records)
}

async fn materialize_selected_change_payloads(
    read: &(impl StorageAdapterRead + ?Sized),
    records: &HashMap<SelectedChangeKey, ChangeRecord>,
) -> Result<HashMap<SelectedChangeKey, crate::changelog::MaterializedChangePayload>, LixError> {
    let ordered = records
        .iter()
        .map(|(key, record)| (key.clone(), record.clone()))
        .collect::<Vec<_>>();
    let payloads = crate::changelog::materialize_known_change_payloads_in_order(
        read,
        ordered.iter().map(|(_, record)| record.clone()),
        ChangeRecordProjection::full(),
    )
    .await?;
    ordered
        .into_iter()
        .zip(payloads)
        .map(|((key, record), (change_id, payload))| {
            if change_id != record.change_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "selected change payload order does not match its source record",
                ));
            }
            Ok((key, payload))
        })
        .collect()
}

async fn stage_tracked_commit_delta_index(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &mut PreparedStateBatch,
    row_columnar_write_sets: &mut crate::hot_state::RowColumnarWriteSets,
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_roots: &[PendingTrackedRoot],
    commit_rows: &[FinalizedCommitRow],
    selected_change_records: &HashMap<SelectedChangeKey, ChangeRecord>,
    certified_packet_root_rows: &BTreeMap<CommitId, Vec<MaterializedHotStateRow>>,
    certified_packet_json_refs: &BTreeMap<CommitId, Vec<CertifiedRootJsonRefs>>,
    insert_selection: &PreparedInsertSelection,
    replacement_generations: &BTreeMap<CommitId, CommitDeltaReplacementGeneration>,
    ordered_replacements: &BTreeMap<CommitId, Arc<OrderedMutationJournal>>,
    checkpoint_commit_ids: &BTreeSet<CommitId>,
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
            let (sealed_rows, parts) = journal.sealed_replacement_prefix();
            let created_at = generation.lifecycle_summary.uniform_created_at;
            let stage = if sealed_rows == journal.row_count() {
                crate::tracked_state::stage_preencoded_ordered_addressable_replacement_parts(
                    writes,
                    root.commit_id,
                    journal.timestamp(),
                    journal.row_count(),
                    parts,
                    generation,
                )?
            } else if sealed_rows > 0 {
                crate::tracked_state::stage_prefixed_ordered_addressable_replacement_parts(
                    writes,
                    root.commit_id,
                    journal.timestamp(),
                    sealed_rows,
                    parts,
                    journal.iter().skip(sealed_rows).map(|row| {
                        Ok(TrackedStateSingleStringReplacementRef {
                            schema_key: journal.schema_key(),
                            file_id: None,
                            row_pk: row.identity(),
                            commit_id: root.commit_id,
                            created_at,
                            updated_at: journal.timestamp(),
                            snapshot: row.snapshot_slot(),
                            metadata: crate::json_store::JsonSlotRef::None,
                        })
                    }),
                    generation,
                )?
            } else {
                crate::tracked_state::stage_ordered_addressable_replacement_parts(
                    writes,
                    journal.iter().map(|row| {
                        Ok(TrackedStateSingleStringReplacementRef {
                            schema_key: journal.schema_key(),
                            file_id: None,
                            row_pk: row.identity(),
                            commit_id: root.commit_id,
                            created_at,
                            updated_at: journal.timestamp(),
                            snapshot: row.snapshot_slot(),
                            metadata: crate::json_store::JsonSlotRef::None,
                        })
                    }),
                    generation,
                )?
            };
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
            let ordered_stage = {
                let _span = tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.materialization.commit_delta.ordered_stream",
                    row_count = state_row_indices.len()
                )
                .entered();
                let state_rows = &*state_rows;
                let order_certified = state_rows.certified_tracked_keys_strictly_ordered()
                    && state_row_indices.len() == state_rows.len()
                    && state_row_indices
                        .iter()
                        .enumerate()
                        .all(|(index, &row_index)| index == row_index);
                if replacement_generation.is_none()
                    && order_certified
                    && let Some(stage) = try_stage_lossless_columnar_mutations(
                        writes,
                        root.commit_id,
                        state_rows,
                        state_row_indices,
                        row_columnar_write_sets,
                    )?
                {
                    Some(stage)
                } else {
                    let make_delta = |row_index| {
                        let row = state_rows.row(row_index);
                        let mut delta = tracked_commit_delta_from_state_row(row)?;
                        if let Some(created_at) = lifecycle_created_at {
                            delta.delta.created_at = created_at;
                        }
                        delta.base_coordinate = row_columnar_write_sets
                            .state_row_location(row_index)
                            .map(
                                |location| crate::tracked_state::TrackedStateBaseCoordinate {
                                    base_commit_id: root.commit_id,
                                    group_index: location.group_index,
                                    row_index: location.row_index,
                                },
                            );
                        Ok(delta)
                    };
                    let replacement_stage = if let Some(generation) = replacement_generation {
                        if !order_certified {
                            return Err(LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "complete replacement generation is not in canonical identity order",
                            ));
                        }
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
                        None => stage_ordered_addressable_commit_deltas(
                            writes,
                            state_row_indices
                                .iter()
                                .map(|&row_index| make_delta(row_index)),
                            order_certified,
                            publish_lifecycle_summary,
                        )?,
                    }
                }
            };
            if let Some(ordered_stage) = ordered_stage {
                inventories.insert(root.commit_id, ordered_stage.mutation_inventory().clone());
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
            delta.base_coordinate =
                row_columnar_write_sets
                    .state_row_location(row_index)
                    .map(
                        |location| crate::tracked_state::TrackedStateBaseCoordinate {
                            base_commit_id: root.commit_id,
                            group_index: location.group_index,
                            row_index: location.row_index,
                        },
                    );
            deltas.push(delta);
        }
        for (row, json_refs) in certified_root_rows.iter().zip(certified_root_json_refs) {
            deltas.push(tracked_commit_delta_from_certified_root_row(
                row, json_refs,
            )?);
            // Certified rows are addressed by commit plus identity. They do
            // not need standalone change locators in the public ledger.
            addressable.push(false);
        }
        for change_ref in selected_changes(&staged.selected_change_batches) {
            *selected_members_by_source
                .entry(change_ref.source_commit_id)
                .or_default() += 1;
            let key = selected_change_key(change_ref);
            let record = selected_change_records.get(&key);
            deltas.push(tracked_commit_delta_from_selected_change_ref(
                change_ref,
                root.commit_id,
                record,
            )?);
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
        let is_checkpoint_commit = checkpoint_commit_ids.contains(&root.commit_id);
        let selected_source_alias = if certified_root_rows.is_empty()
            && !state_row_indices.is_empty()
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
                                        row_pk: change_ref.row_pk(),
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
    Ok(StagedCommitDeltaIndex {
        ordered_addressable_commits,
        inventories,
    })
}

fn try_stage_lossless_columnar_mutations(
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
    state_rows: &PreparedStateBatch,
    state_row_indices: &[RowIndex],
    row_columnar_write_sets: &mut crate::hot_state::RowColumnarWriteSets,
) -> Result<Option<crate::tracked_state::OrderedAddressableCommitDeltaStage>, LixError> {
    if state_row_indices.is_empty()
        || row_columnar_write_sets.dense_state_row_count() != Some(state_row_indices.len())
    {
        return Ok(None);
    }
    let first = state_rows.row(state_row_indices[0]);
    let Some(encoded) = row_columnar_write_sets.get(&(commit_id, first.schema_key.to_string()))
    else {
        return Ok(None);
    };
    if encoded
        .manifest
        .metadata
        .get(crate::sql2::ROW_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY)
        .map(String::as_str)
        != Some("true")
        || encoded.manifest.row_count()
            != u64::try_from(state_row_indices.len()).expect("row count fits u64")
    {
        return Ok(None);
    }
    let mut identity_digest = blake3::Hasher::new();
    for &row_index in state_row_indices {
        let row = state_rows.row(row_index);
        if row.schema_key != first.schema_key
            || row.file_id.is_some()
            || row.snapshot.is_none()
            || row.metadata.is_some()
            || row.created_at != first.created_at
            || row.updated_at != first.updated_at
            || row.origin_key != first.origin_key
            || row.commit_id != Some(commit_id)
            || row.untracked
            || row.global
        {
            return Ok(None);
        }
        let Ok(identity) = row.row_pk.as_single_string() else {
            return Ok(None);
        };
        identity_digest.update(&(identity.len() as u64).to_le_bytes());
        identity_digest.update(identity.as_bytes());
    }
    let last = state_rows.row(*state_row_indices.last().expect("non-empty rows"));
    let mut page_first_keys = Vec::new();
    let mut page_last_keys = Vec::new();
    for page in state_row_indices.chunks(crate::columnar_row_group::ROW_GROUP_PAGE_ROWS) {
        let page_first = state_rows.row(page[0]);
        let page_last = state_rows.row(*page.last().expect("non-empty mutation page"));
        page_first_keys.push(encode_key_ref(TrackedStateKeyRef {
            schema_key: page_first.schema_key.as_str(),
            file_id: None,
            row_pk: page_first.row_pk,
        }));
        page_last_keys.push(encode_key_ref(TrackedStateKeyRef {
            schema_key: page_last.schema_key.as_str(),
            file_id: None,
            row_pk: page_last.row_pk,
        }));
    }
    let parts = crate::tracked_state::ColumnarMutationPartSet {
        owner_commit_id: *commit_id.as_uuid().as_bytes(),
        row_group_set_id: crate::hot_state::row_group_set_id(commit_id, first.schema_key.as_str())
            .as_bytes(),
        manifest_digest: encoded.manifest.content_digest()?,
        schema_key: first.schema_key.to_string(),
        row_count: u32::try_from(state_row_indices.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "columnar mutation row count exceeds u32",
            )
        })?,
        group_row_counts: encoded
            .manifest
            .groups
            .iter()
            .map(|group| group.row_count)
            .collect(),
        first_key: encode_key_ref(TrackedStateKeyRef {
            schema_key: first.schema_key.as_str(),
            file_id: None,
            row_pk: first.row_pk,
        }),
        last_key: encode_key_ref(TrackedStateKeyRef {
            schema_key: last.schema_key.as_str(),
            file_id: None,
            row_pk: last.row_pk,
        }),
        page_first_keys,
        page_last_keys,
        uniform_created_at: first.created_at,
        uniform_updated_at: first.updated_at,
        origin_key: first.origin_key.map(ToString::to_string),
    };
    let encoded = row_columnar_write_sets
        .take(&(commit_id, first.schema_key.to_string()))
        .expect("qualified columnar mutation set remains staged exactly once");
    crate::columnar_row_group::stage_row_group_set(
        writes,
        crate::columnar_row_group::RowGroupSetId::new(parts.row_group_set_id),
        &encoded,
    )?;
    crate::tracked_state::stage_ordered_columnar_mutations(
        commit_id,
        parts,
        *identity_digest.finalize().as_bytes(),
    )
    .map(Some)
}

async fn certify_complete_replacement_generations(
    read: &(impl StorageAdapterRead + ?Sized),
    state_rows: &PreparedStateBatch,
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_roots: &[PendingTrackedRoot],
) -> Result<BTreeMap<CommitId, CommitDeltaReplacementGeneration>, LixError> {
    let mut generations = BTreeMap::new();
    for root in tracked_roots {
        let row_indices = tracked_row_indices_by_commit
            .get(&root.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let Some(scope) =
            certified_complete_replacement_scope(state_rows, row_indices, root.commit_id)
        else {
            continue;
        };
        let Some(replacement_proof) = state_rows.complete_collection_replacement_proof() else {
            continue;
        };
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            Some(replacement_proof.ordered_identity_digest),
            crate::collection_generation::ordered_single_string_identity_digest(
                row_indices
                    .iter()
                    .map(|&row_index| state_rows.row(row_index).row_pk)
            )
        );
        let ordered_identity_digest = replacement_proof.ordered_identity_digest;
        let mut current = root.parent_commit_id;
        let mut seen = BTreeSet::new();
        let mut lifecycle_summary = None;
        let fallback_commit_id = loop {
            let Some(commit_id) = current else {
                break None;
            };
            if !seen.insert(commit_id) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "cannot certify replacement generation '{}': first-parent cycle includes '{commit_id}'",
                        root.commit_id
                    ),
                ));
            }
            let commit_ids = [commit_id];
            let record = ChangelogContext::new()
                .reader(read)
                .load_commits(ChangelogCommitLoadRequest {
                    commit_ids: &commit_ids,
                })
                .await?
                .into_iter()
                .next()
                .and_then(|(_, record)| record)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "replacement generation '{}' has missing parent '{commit_id}'",
                            root.commit_id
                        ),
                    )
                })?;
            let manifest = crate::tracked_state::load_published_commit_state_topology(
                read, commit_id,
            )
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "replacement generation parent '{commit_id}' has no physical authority"
                    ),
                )
            })?;
            if manifest.replay_debt().depth == 0 {
                break Some(commit_id);
            }
            let Some(metadata) = load_commit_delta_replay_metadata(read, commit_id).await? else {
                // Missing replay evidence cannot certify that this interval
                // belongs exclusively to the replaced partition.
                break Some(commit_id);
            };
            if metadata.single_partition.as_ref() != Some(&scope) {
                // Resetting global replay accounting is safe only when the
                // entire skipped interval belongs to the replaced partition.
                break Some(commit_id);
            }
            if let Some(parent_generation) = metadata.replacement_generation {
                if parent_generation.scope != scope {
                    break Some(commit_id);
                }
                if usize::try_from(metadata.member_count).ok() != Some(row_indices.len())
                    || parent_generation.lifecycle_summary.ordered_identity_digest
                        != ordered_identity_digest
                {
                    break Some(commit_id);
                }
                lifecycle_summary = Some(parent_generation.lifecycle_summary);
                break parent_generation.fallback_commit_id;
            }
            let Some(summary) = metadata.lifecycle_summary.as_ref() else {
                // A same-partition sparse commit may have deleted and later
                // reinserted one identity with a new lifecycle. Do not carry
                // an older full-set summary across any commit that does not
                // itself prove the complete ordered identity set.
                break Some(commit_id);
            };
            if usize::try_from(metadata.member_count).ok() != Some(row_indices.len())
                || summary.scope != scope
                || summary.ordered_identity_digest != ordered_identity_digest
            {
                break Some(commit_id);
            }
            lifecycle_summary = Some(summary.clone());
            current = record.parent_commit_ids.first().copied();
        };
        let Some(lifecycle_summary) = lifecycle_summary else {
            continue;
        };
        generations.insert(
            root.commit_id,
            CommitDeltaReplacementGeneration {
                scope,
                fallback_commit_id,
                lifecycle_summary,
            },
        );
    }
    Ok(generations)
}

async fn certify_ordered_journal_replacement_generations(
    read: &(impl StorageAdapterRead + ?Sized),
    journals: &BTreeMap<CommitId, Arc<OrderedMutationJournal>>,
    tracked_roots: &[PendingTrackedRoot],
) -> Result<BTreeMap<CommitId, CommitDeltaReplacementGeneration>, LixError> {
    let mut generations = BTreeMap::new();
    for root in tracked_roots {
        let Some(journal) = journals.get(&root.commit_id) else {
            continue;
        };
        if journal.commit_id() != root.commit_id || journal.row_count() == 0 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable replacement journal owner or cardinality changed",
            ));
        }
        let scope = CommitDeltaReplacementScope {
            schema_key: journal.schema_key().to_owned(),
            file_id: None,
        };
        let proof = journal.replacement_proof();
        let mut current = root.parent_commit_id;
        let mut seen = BTreeSet::new();
        let mut lifecycle_summary = None;
        let fallback_commit_id = loop {
            let Some(commit_id) = current else {
                break None;
            };
            if !seen.insert(commit_id) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "cannot certify immutable replacement '{}': first-parent cycle includes '{commit_id}'",
                        root.commit_id
                    ),
                ));
            }
            let record = ChangelogContext::new()
                .reader(read)
                .load_commits(ChangelogCommitLoadRequest {
                    commit_ids: &[commit_id],
                })
                .await?
                .into_iter()
                .next()
                .and_then(|(_, record)| record)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "immutable replacement '{}' has missing parent '{commit_id}'",
                            root.commit_id
                        ),
                    )
                })?;
            let manifest = crate::tracked_state::load_published_commit_state_topology(
                read, commit_id,
            )
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("immutable replacement parent '{commit_id}' has no physical authority"),
                )
            })?;
            if manifest.replay_debt().depth == 0 {
                break Some(commit_id);
            }
            let Some(metadata) = load_commit_delta_replay_metadata(read, commit_id).await? else {
                break Some(commit_id);
            };
            if metadata.single_partition.as_ref() != Some(&scope) {
                break Some(commit_id);
            }
            if let Some(parent_generation) = metadata.replacement_generation {
                if parent_generation.scope != scope {
                    break Some(commit_id);
                }
                if usize::try_from(metadata.member_count).ok() != Some(journal.row_count())
                    || parent_generation.lifecycle_summary.ordered_identity_digest
                        != proof.ordered_identity_digest
                {
                    break Some(commit_id);
                }
                lifecycle_summary = Some(parent_generation.lifecycle_summary);
                break parent_generation.fallback_commit_id;
            }
            let Some(summary) = metadata.lifecycle_summary.as_ref() else {
                break Some(commit_id);
            };
            if usize::try_from(metadata.member_count).ok() != Some(journal.row_count())
                || summary.scope != scope
                || summary.ordered_identity_digest != proof.ordered_identity_digest
            {
                break Some(commit_id);
            }
            lifecycle_summary = Some(summary.clone());
            current = record.parent_commit_ids.first().copied();
        };
        let Some(lifecycle_summary) = lifecycle_summary else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "immutable replacement journal lacks parent lifecycle authority; hydrate predecessors and lower it before commit",
            ));
        };
        generations.insert(
            root.commit_id,
            CommitDeltaReplacementGeneration {
                scope,
                fallback_commit_id,
                lifecycle_summary,
            },
        );
    }
    Ok(generations)
}

fn certified_complete_replacement_scope(
    state_rows: &PreparedStateBatch,
    row_indices: &[RowIndex],
    commit_id: CommitId,
) -> Option<CommitDeltaReplacementScope> {
    if state_rows.complete_collection_replacement_proof().is_none()
        || row_indices.is_empty()
        || row_indices.len() != state_rows.len()
    {
        return None;
    }
    let first = state_rows.row(row_indices[0]);
    if first.commit_id != Some(commit_id)
        || first.snapshot.is_none()
        || first.untracked
        || first.global
        || row_indices.iter().any(|&row_index| {
            let row = state_rows.row(row_index);
            row.commit_id != Some(commit_id)
                || row.schema_key != first.schema_key
                || row.file_id != first.file_id
                || row.snapshot.is_none()
                || row.untracked
                || row.global
        })
    {
        return None;
    }
    Some(CommitDeltaReplacementScope {
        schema_key: first.schema_key.to_string(),
        file_id: first.file_id.map(ToString::to_string),
    })
}

fn prepared_state_row_replay_bytes(row: PreparedStateRowRef<'_>) -> Result<u64, LixError> {
    let snapshot_bytes = row.snapshot.map_or(0, |json| json.normalized().len());
    let metadata_bytes = row.metadata.map_or(0, |json| json.normalized().len());
    let identity_bytes = row
        .schema_key
        .len()
        .checked_add(row.file_id.map_or(0, |file_id| file_id.as_str().len()))
        .and_then(|bytes| bytes.checked_add(row.row_pk.estimated_heap_bytes()))
        // Fixed typed primary-key components, timestamps, ids, flags, and
        // length prefixes are covered by one conservative per-row envelope.
        .and_then(|bytes| bytes.checked_add(128))
        .ok_or_else(|| LixError::unknown("tracked-state replay row bytes exceed usize"))?;
    identity_bytes
        .checked_add(snapshot_bytes)
        .and_then(|bytes| bytes.checked_add(metadata_bytes))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| LixError::unknown("tracked-state replay row bytes exceed u64"))
}

fn replay_bytes_for_rows(
    state_rows: &PreparedStateBatch,
    row_indices: Option<&[RowIndex]>,
) -> Result<u64, LixError> {
    row_indices
        .into_iter()
        .flatten()
        .try_fold(0_u64, |bytes, &row_index| {
            bytes
                .checked_add(prepared_state_row_replay_bytes(state_rows.row(row_index))?)
                .ok_or_else(|| LixError::unknown("tracked-state rootless byte count exceeds u64"))
        })
}

fn select_new_rootless_ordered_commits(
    state_rows: &PreparedStateBatch,
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_roots: &[PendingTrackedRoot],
    ordered_addressable_commits: &BTreeSet<CommitId>,
    certified_packet_root_rows: &BTreeMap<CommitId, Vec<MaterializedHotStateRow>>,
    ordered_replacements: &BTreeMap<CommitId, Arc<OrderedMutationJournal>>,
) -> BTreeSet<CommitId> {
    let can_start_rootless_interval = tracked_roots.len() == 1;
    let mut rootless = BTreeSet::new();
    for root in tracked_roots {
        if ordered_replacements.contains_key(&root.commit_id) {
            rootless.insert(root.commit_id);
            continue;
        }
        let row_indices = tracked_row_indices_by_commit
            .get(&root.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let starts_ordered_interval = can_start_rootless_interval
            && root.publish_head
            && !row_indices.is_empty()
            && ordered_addressable_commits.contains(&root.commit_id)
            && certified_packet_root_rows
                .get(&root.commit_id)
                .is_none_or(Vec::is_empty)
            && row_indices.len() == state_rows.len();
        if starts_ordered_interval {
            rootless.insert(root.commit_id);
        }
    }
    rootless
}

struct StagedHotHeads {
    controls: BTreeMap<String, BranchHeadControl>,
    deferred_fresh_hot_plans: Vec<crate::hot_state::DeferredFreshHotPlan>,
}

/// Returns the commit snapshots that must be materialized before publication.
/// Normal serial commits and row-only selected refs stay on the
/// O(changed-rows) hot mutation path. A lifecycle discontinuity (checkpoint,
/// staged parent, or branch creation) and selected refs whose filesystem
/// invariants span rows need a complete tracked snapshot so the serving
/// control never points at a partially reconstructed view.
fn lifecycle_snapshot_commit_ids(
    tracked_roots: &[PendingTrackedRoot],
    staged_commits: &BTreeMap<CommitId, StagedChangelogCommit>,
    explicit_branch_targets: &BTreeMap<String, ExplicitBranchHeadTarget>,
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
        let checkpoint_can_reuse_generation = checkpoint_epochs.get(&root.branch_id)
            == Some(&root.commit_id)
            && observations
                .get(&root.branch_id)
                .and_then(|observation| observation.control)
                .is_some();
        if !checkpoint_can_reuse_generation
            && (!parent_is_published
                || selected_refs_require_complete_snapshot(&staged.selected_change_batches))
        {
            required.insert(root.commit_id);
        }
    }
    for (branch_id, target) in explicit_branch_targets {
        if let Some(commit_id) = target.head_commit_id
            && roots_by_id.contains_key(&commit_id)
            && observations
                .get(branch_id)
                .and_then(|observation| observation.control)
                .is_some()
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

fn selected_refs_require_complete_snapshot(
    selected_change_batches: &[StagedCommitChangeBatch],
) -> bool {
    selected_changes(selected_change_batches).any(|change_ref| {
        matches!(
            change_ref.schema_key(),
            FILE_DESCRIPTOR_SCHEMA_KEY | DIRECTORY_DESCRIPTOR_SCHEMA_KEY
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
    state_rows: &PreparedStateBatch,
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_roots: &[PendingTrackedRoot],
    staged_commits: &BTreeMap<CommitId, StagedChangelogCommit>,
    selected_payloads: &HashMap<SelectedChangeKey, crate::changelog::MaterializedChangePayload>,
    insert_selection: &PreparedInsertSelection,
    required: &BTreeSet<CommitId>,
) -> Result<BTreeMap<CommitId, HotTrackedSnapshot>, LixError> {
    if required.is_empty() {
        return Ok(BTreeMap::new());
    }

    let roots_by_id = tracked_roots
        .iter()
        .map(|root| (root.commit_id, root))
        .collect::<BTreeMap<_, _>>();
    let mut prepared_by_identity = HashMap::new();
    for row in state_rows.iter().filter(|row| !row.untracked) {
        let change_id = row.change_id.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked lifecycle snapshot row is missing change_id",
            )
        })?;
        let live = MaterializedHotStateRow::from(row);
        let tracked = MaterializedTrackedStateRow::try_from(&live)?;
        let identity = TrackedStateKey {
            schema_key: tracked.schema_key.clone(),
            file_id: tracked.file_id.clone(),
            row_pk: tracked.row_pk.clone(),
        };
        if prepared_by_identity.insert(identity, tracked).is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked lifecycle snapshot contains duplicate identity for change '{change_id}'"
                ),
            ));
        }
    }
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
                load_persisted_lifecycle_tracked_snapshot(read, &root.branch_id, parent_commit_id)
                    .await?
            }
        };
        let row_indices = tracked_row_indices_by_commit
            .get(&root.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for &row_index in row_indices {
            let row = state_rows.row(row_index);
            let live = MaterializedHotStateRow::from(row);
            let tracked = MaterializedTrackedStateRow::try_from(&live)?;
            apply_lifecycle_tracked_snapshot_row(
                &mut rows,
                tracked,
                tracked_row_requires_absence(row_index, row, insert_selection),
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
        for change_ref in selected_changes(&staged.selected_change_batches) {
            let key = selected_change_key(change_ref);
            let source = prepared_by_identity.get(&key.identity);
            let payload = selected_payloads.get(&key);
            let tracked =
                lifecycle_selected_tracked_row(change_ref, root.commit_id, source, payload)?;
            apply_lifecycle_tracked_snapshot_row(&mut rows, tracked, false)?;
        }
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

const ACCOUNT_SCHEMA_KEY: &str = "lix_account";
const PERMISSION_GRANT_SCHEMA_KEY: &str = "lix_permission_grant";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

async fn load_persisted_lifecycle_tracked_snapshot(
    read: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    commit_id: CommitId,
) -> Result<BTreeMap<TrackedStateKey, MaterializedTrackedStateRow>, LixError> {
    let rows = TrackedStateContext::new()
        .reader(read)
        .scan_batch_at_commit(
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
        .await?
        .into_rows();
    Ok(rows
        .into_iter()
        .filter(|row| {
            branch_id == crate::GLOBAL_BRANCH_ID
                || row.schema_key != crate::checkpoint::CHECKPOINT_SCHEMA_KEY
        })
        .map(|row| {
            let key = TrackedStateKey {
                schema_key: row.schema_key.clone(),
                row_pk: row.row_pk.clone(),
                file_id: row.file_id.clone(),
            };
            (key, row)
        })
        .collect())
}

fn lifecycle_selected_tracked_row(
    change_ref: StagedCommitChangeRef<'_>,
    commit_id: CommitId,
    prepared: Option<&MaterializedTrackedStateRow>,
    payload: Option<&crate::changelog::MaterializedChangePayload>,
) -> Result<MaterializedTrackedStateRow, LixError> {
    let (schema_key, row_pk, file_id, snapshot_content, metadata) = if let Some(row) = prepared {
        (
            row.schema_key.clone(),
            row.row_pk.clone(),
            row.file_id.clone(),
            row.snapshot_content.clone(),
            row.metadata.clone(),
        )
    } else if change_ref.deleted && payload.is_none() {
        (
            change_ref.schema_key().to_owned(),
            change_ref.row_pk().clone(),
            change_ref.file_id().map(str::to_owned),
            None,
            None,
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
            identity.row_pk.clone(),
            identity.file_id.clone(),
            payload.snapshot_content.clone(),
            payload.metadata.clone(),
        )
    };
    if schema_key != change_ref.schema_key()
        || &row_pk != change_ref.row_pk()
        || file_id.as_deref() != change_ref.file_id()
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
        row_pk,
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
    if next.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY && next.snapshot_content.is_none() {
        let file_id = next.row_pk.as_single_string_owned().map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("file descriptor tombstone has invalid identity: {error}"),
            )
        })?;
        let cascade_keys = rows
            .iter()
            .filter(|(key, value)| {
                key.file_id.as_deref() == Some(file_id.as_str()) && !value.deleted
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in cascade_keys {
            let value = rows
                .get_mut(&key)
                .expect("cascade key was selected from this snapshot");
            value.snapshot_content = None;
            value.metadata = None;
            value.deleted = true;
            value.updated_at.clone_from(&next.updated_at);
            value.change_id = next.change_id;
            value.commit_id = next.commit_id;
        }
    }
    let key = TrackedStateKey {
        schema_key: next.schema_key.clone(),
        row_pk: next.row_pk.clone(),
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
    let row_pk = key
        .row_pk
        .as_json_array_text()
        .unwrap_or_else(|_| "<invalid row_pk>".to_string());
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "primary-key constraint violation on schema '{}': INSERT would duplicate row_pk '{row_pk}'",
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
    // Domain-separation tag. These bytes are load-bearing: they feed a
    // blake3 hash whose output is a persisted `CommitId`, so the string is
    // an input to stored data, not a label. It deliberately keeps the old
    // `live_state` spelling and must NOT be renamed with the module.
    hasher.update(b"lix.live_state.lifecycle_generation.v1");
    hasher.update(&(branch_id.len() as u64).to_be_bytes());
    hasher.update(branch_id.as_bytes());
    hasher.update(head_commit_id.as_uuid().as_bytes());
    hasher.update(ref_change_id.as_uuid().as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    // Every commit id must reserve its change address space, including the
    // synthetic ones: the commit's own change id is that address at ordinal
    // zero. `with_change_address_space` folds the low bits back into the
    // random field rather than discarding them, so the hash keeps its
    // distinguishing power.
    CommitId::with_change_address_space(uuid::Uuid::from_bytes(bytes))
}

fn next_current_state_revision(current: u64) -> Result<u64, LixError> {
    current.checked_add(1).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "branch current-state revision overflowed",
        )
    })
}

/// Stages the hot serving plane.  Serial normal commits mutate their current
/// generation; every lifecycle discontinuity publishes a complete fresh
/// generation before its branch control is made visible.
/// Every schema key this transaction publishes on the global branch, or `None`
/// when that set cannot be enumerated completely.
///
/// Serving-view tombstone compaction removes a branch tombstone only once
/// nothing beneath it can resurface, and the global branch sits beneath every
/// other branch. `HotStateStoreReader::has_schema_rows` answers that for what
/// is already durable, but not for what this same transaction is about to
/// publish — and `tracked_roots_parent_first` orders roots by
/// `parent_commit_id` alone, never consulting `branch_id`, so the global root
/// is not guaranteed to be staged before a non-global one and a write-set
/// consultation would be racy. Deriving the set from the transaction's
/// prepared inputs instead makes it order-independent.
///
/// **Polarity.** Every value this returns *restricts* compaction: `None`
/// disables it outright, and each schema key disables it for that schema. An
/// omission here is therefore unsound, not merely pessimistic — anything that
/// can publish a global row has to be enumerated below or force the `None`.
///
/// The sources are exactly those that reach `HotStateWriter` for a root:
/// prepared state rows, engine rows, a root's selected change batches, and the
/// synthesized `lix_branch_ref` row. A global root carrying a lifecycle
/// snapshot is not enumerated at all and forces `None`.
fn global_branch_schema_keys(
    state_rows: &PreparedStateBatch,
    engine_rows: &[EngineCurrentRow],
    tracked_roots: &[PendingTrackedRoot],
    staged_commits: &BTreeMap<CommitId, StagedChangelogCommit>,
    tracked_snapshots: &BTreeMap<CommitId, HotTrackedSnapshot>,
) -> Option<BTreeSet<String>> {
    let global_root_commits = tracked_roots
        .iter()
        .filter(|root| root.branch_id == crate::GLOBAL_BRANCH_ID)
        .map(|root| root.commit_id)
        .collect::<BTreeSet<_>>();
    if global_root_commits
        .iter()
        .any(|commit_id| tracked_snapshots.contains_key(commit_id))
    {
        return None;
    }
    let mut schema_keys = BTreeSet::new();
    if !global_root_commits.is_empty() {
        // Each publishing root synthesizes its own branch-ref row.
        schema_keys.insert(BRANCH_REF_SCHEMA_KEY.to_string());
    }
    for row in state_rows.iter() {
        if row.branch_id.as_str() == crate::GLOBAL_BRANCH_ID {
            schema_keys.insert(row.schema_key.to_string());
        }
    }
    for row in engine_rows {
        if row.branch_id == crate::GLOBAL_BRANCH_ID {
            schema_keys.insert(row.change.schema_key.to_string());
        }
    }
    for commit_id in &global_root_commits {
        let Some(staged) = staged_commits.get(commit_id) else {
            continue;
        };
        for change_ref in selected_changes(&staged.selected_change_batches) {
            schema_keys.insert(change_ref.schema_key().to_string());
        }
    }
    Some(schema_keys)
}

async fn stage_tracked_head(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &PreparedStateBatch,
    row_columnar_write_sets: &crate::hot_state::RowColumnarWriteSets,
    engine_rows: &[EngineCurrentRow],
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_delete_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_roots: &[PendingTrackedRoot],
    staged_commits: &BTreeMap<CommitId, StagedChangelogCommit>,
    selected_change_payloads: &HashMap<
        SelectedChangeKey,
        crate::changelog::MaterializedChangePayload,
    >,
    insert_selection: &PreparedInsertSelection,
    certified_fresh_plugin_file_id: Option<&str>,
    host_certified_file_schemas: &BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    host_certified_live_increments: &BTreeMap<String, BTreeMap<(String, Option<String>), u64>>,
    explicit_branch_targets: &BTreeMap<String, ExplicitBranchHeadTarget>,
    observations: &BTreeMap<String, BranchHeadControlObservation>,
    checkpoint_epochs: &BTreeMap<String, CommitId>,
    mutation_inventories: &BTreeMap<CommitId, CommitStateMutationInventory>,
    ordered_addressable_commits: &BTreeSet<CommitId>,
    replacement_generation_commits: &BTreeSet<CommitId>,
    ordered_replacements: &BTreeMap<CommitId, Arc<OrderedMutationJournal>>,
) -> Result<StagedHotHeads, LixError> {
    let lifecycle_ids = lifecycle_snapshot_commit_ids(
        tracked_roots,
        staged_commits,
        explicit_branch_targets,
        observations,
        checkpoint_epochs,
    )?;
    let mut tracked_snapshots = build_lifecycle_tracked_snapshots(
        read,
        state_rows,
        tracked_row_indices_by_commit,
        tracked_roots,
        staged_commits,
        selected_change_payloads,
        insert_selection,
        &lifecycle_ids,
    )
    .instrument(tracing::debug_span!(
        target: "lix_perf",
        "lix.perf.materialization.tracked_head.lifecycle"
    ))
    .await?;
    let explicit_branches = explicit_branch_targets
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let tracked_head = TrackedHeadContext::new();
    let mut controls = BTreeMap::new();
    let mut deferred_fresh_hot_plans = Vec::new();
    let mut exclusive_certified_columnar_publication = false;
    let transaction_global_schema_keys = global_branch_schema_keys(
        state_rows,
        engine_rows,
        tracked_roots,
        staged_commits,
        &tracked_snapshots,
    );

    for root in tracked_roots_parent_first(tracked_roots)?
        .into_iter()
        .filter(|root| root.publish_head)
    {
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
        let checkpoint_commit_id = checkpoint_epochs.get(&root.branch_id).copied();
        let is_checkpoint_publication = checkpoint_commit_id == Some(root.commit_id);
        let certified_columnar_parts = mutation_inventories
            .get(&root.commit_id)
            .and_then(|inventory| inventory.columnar_parts.as_ref());

        // A checkpoint changes the authenticated epoch/control, not current
        // row values. Take the O(deletes) route before constructing ordinary
        // tracked/untracked delta cohorts or selected-row materializations.
        // The deletion ordinals were built while their typed owners were
        // produced, so this branch never scans non-deleted members.
        if is_checkpoint_publication && !tracked_snapshots.contains_key(&root.commit_id) {
            let parent_generation = parent_control
                .map(|control| control.tracked_generation)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "checkpoint hot publication for '{}' lacks a complete parent generation",
                            root.commit_id
                        ),
                    )
                })?;
            let mut writer = tracked_head.writer(read, writes);
            if let Some(schema_keys) = transaction_global_schema_keys.as_ref() {
                writer = writer.with_transaction_global_schema_keys(schema_keys);
            }
            if writer
                .can_rotate_checkpoint_epoch(&root.branch_id, parent_generation)
                .await?
            {
                let selected_deleted_rows = staged
                    .selected_change_batches
                    .iter()
                    .flat_map(StagedCommitChangeBatch::deleted_iter)
                    .map(|change_ref| {
                        let key = selected_change_key(change_ref);
                        lifecycle_selected_tracked_row(
                            change_ref,
                            root.commit_id,
                            None,
                            selected_change_payloads.get(&key),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let selected_snapshots = selected_deleted_rows
                    .iter()
                    .map(|row| {
                        row.snapshot_content.as_deref().map_or(
                            crate::json_store::JsonSlot::None,
                            crate::json_store::JsonSlot::from_json,
                        )
                    })
                    .collect::<Vec<_>>();
                let selected_metadata = selected_deleted_rows
                    .iter()
                    .map(|row| {
                        row.metadata.as_deref().map_or(
                            crate::json_store::JsonSlot::None,
                            crate::json_store::JsonSlot::from_json,
                        )
                    })
                    .collect::<Vec<_>>();
                let mut deleted_deltas = tracked_delete_indices_by_commit
                    .get(&root.commit_id)
                    .into_iter()
                    .flatten()
                    .map(|&row_index| current_state_delta_from_state_row(state_rows.row(row_index)))
                    .collect::<Result<Vec<_>, _>>()?;
                deleted_deltas.extend(
                    staged
                        .selected_change_batches
                        .iter()
                        .flat_map(StagedCommitChangeBatch::deleted_iter)
                        .zip(selected_deleted_rows.iter())
                        .zip(selected_snapshots.iter().zip(&selected_metadata))
                        .map(|((change_ref, row), (snapshot, metadata))| {
                            crate::hot_state::CurrentStateDeltaRef {
                                schema_key: &row.schema_key,
                                file_id: row.file_id.as_deref(),
                                row_pk: &row.row_pk,
                                change_id: Some(change_ref.change_id),
                                commit_id: Some(root.commit_id),
                                untracked: false,
                                deleted: true,
                                created_at: change_ref.created_at,
                                updated_at: change_ref.updated_at,
                                snapshot: snapshot.as_ref_slot(),
                                metadata: metadata.as_ref_slot(),
                                columnar_base_coordinate: None,
                            }
                        }),
                );
                let mut coverage = WorkingDiffIndexCoverage::default();
                let generation = if deleted_deltas.is_empty() {
                    parent_generation
                } else {
                    writer
                        .stage_checkpoint_current_state(
                            &root.branch_id,
                            parent_generation,
                            root.commit_id,
                            &deleted_deltas,
                            &BTreeSet::new(),
                            checkpoint_commit_id
                                .expect("checkpoint publication has an epoch commit id"),
                            &mut coverage,
                        )
                        .instrument(tracing::debug_span!(
                            target: "lix_perf",
                            "lix.perf.materialization.tracked_head.stage_checkpoint_tombstones"
                        ))
                        .await?
                };
                let control = normal_branch_head_control(
                    root,
                    parent_control,
                    generation,
                    checkpoint_commit_id,
                )?;
                insert_direct_branch_control(&mut controls, &root.branch_id, control)?;
                continue;
            }
        }
        let selected_materialization = if !staged.selected_change_batches.is_empty()
            && !tracked_snapshots.contains_key(&root.commit_id)
        {
            let selected_rows = selected_changes(&staged.selected_change_batches)
                .map(|change_ref| {
                    let key = selected_change_key(change_ref);
                    lifecycle_selected_tracked_row(
                        change_ref,
                        root.commit_id,
                        None,
                        selected_change_payloads.get(&key),
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
        let mut untracked_deltas = if certified_columnar_parts.is_some() {
            Vec::new()
        } else {
            state_rows
                .iter()
                .filter(|row| {
                    row.untracked
                        && row.branch_id.as_str() == root.branch_id
                        && row.schema_key != BRANCH_REF_SCHEMA_KEY
                })
                .map(current_state_delta_from_state_row)
                .collect::<Result<Vec<_>, _>>()?
        };
        untracked_deltas.extend(
            engine_rows
                .iter()
                .filter(|row| row.branch_id == root.branch_id)
                .map(current_state_delta_from_engine_row),
        );
        let can_publish_ordered_packed_current_base = ordered_addressable_commits
            .contains(&root.commit_id)
            && !is_checkpoint_publication
            && state_row_indices.len() >= PACKED_CURRENT_BASE_MIN_ROWS
            && certified_fresh_plugin_file_id.is_none()
            && !host_certified_live_increments.contains_key(&root.branch_id)
            && staged.selected_change_batches.is_empty()
            && selected_materialization.is_none()
            && untracked_deltas.is_empty()
            && engine_rows.is_empty()
            && explicit_branch_targets.is_empty()
            && state_row_indices.len() == state_rows.len()
            && insert_selection.len() == state_rows.len()
            && (certified_columnar_parts.is_some()
                || state_row_indices.iter().all(|&row_index| {
                    let row = state_rows.row(row_index);
                    insert_selection.contains(row_index)
                        && row.branch_id.as_str() == root.branch_id
                        && !row.untracked
                        && row.snapshot.is_some()
                        && row.file_id.is_none()
                        && row.schema_key != BRANCH_REF_SCHEMA_KEY
                        && row.schema_key
                            != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
                        && row.commit_id == Some(root.commit_id)
                        && row
                            .change_id
                            .is_some_and(|change_id| change_id != ChangeId::default())
                }));
        let complete_replacement_schema =
            (state_rows.complete_collection_replacement_proof().is_some()
                && replacement_generation_commits.contains(&root.commit_id)
                && ordered_addressable_commits.contains(&root.commit_id)
                && !is_checkpoint_publication
                && state_row_indices.len() >= PACKED_CURRENT_BASE_MIN_ROWS
                && certified_fresh_plugin_file_id.is_none()
                && !host_certified_live_increments.contains_key(&root.branch_id)
                && staged.selected_change_batches.is_empty()
                && selected_materialization.is_none()
                && untracked_deltas.is_empty()
                && engine_rows.is_empty()
                && explicit_branch_targets.is_empty()
                && state_row_indices.len() == state_rows.len()
                && insert_selection.is_empty())
            .then(|| state_rows.first())
            .flatten()
            .filter(|first| {
                state_row_indices.iter().all(|&row_index| {
                    let row = state_rows.row(row_index);
                    row.schema_key == first.schema_key
                        && row.branch_id.as_str() == root.branch_id
                        && !row.global
                        && !row.untracked
                        && row.snapshot.is_some()
                        && row.metadata.is_none()
                        && row.file_id.is_none()
                        && row.schema_key != BRANCH_REF_SCHEMA_KEY
                        && row.schema_key
                            != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
                        && row.commit_id == Some(root.commit_id)
                        && row
                            .change_id
                            .is_some_and(|change_id| change_id != ChangeId::default())
                })
            })
            .map(|row| row.schema_key.as_str());
        let absence_guards =
            if can_publish_ordered_packed_current_base || complete_replacement_schema.is_some() {
                Vec::new()
            } else {
                let _span = tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.materialization.tracked_head.absence_guards"
                )
                .entered();
                tracked_head_absence_guards(
                    state_rows,
                    insert_selection,
                    &root.branch_id,
                    certified_fresh_plugin_file_id,
                )
            };
        let parent_generation = match (root.parent_commit_id, parent_control) {
            (_, Some(control)) if is_checkpoint_publication => Some(control.tracked_generation),
            (Some(parent_commit_id), Some(control))
                if control.head_commit_id == parent_commit_id =>
            {
                Some(control.tracked_generation)
            }
            _ => None,
        };

        if !staged.selected_change_batches.is_empty() {
            reject_selected_tracked_refs_with_untracked_rows(
                read,
                &root.branch_id,
                parent_control,
                &staged.selected_change_batches,
                state_rows,
                engine_rows,
            )
            .await?;
        }

        if let Some(final_tracked) = tracked_snapshots.get(&root.commit_id).cloned() {
            let generation =
                lifecycle_generation(&root.branch_id, root.commit_id, root.ref_change_id);
            let mut coverage = WorkingDiffIndexCoverage::default();
            let owned_absence_guards = owned_absence_guards(&absence_guards);
            // One generation carries both retentions, so the branch's
            // history-free rows are preserved from — and republished into —
            // that same complete snapshot. There is no second root to advance.
            let (final_tracked, schema_keys) = tracked_head
                .writer(read, writes)
                .stage_complete_current_state_with_working_diff(
                    &root.branch_id,
                    generation,
                    final_tracked,
                    parent_control.map(|control| control.tracked_generation),
                    &[],
                    &untracked_deltas,
                    &owned_absence_guards,
                    checkpoint_commit_id,
                    &mut coverage,
                )
                .await?;
            let mut control =
                normal_branch_head_control(root, parent_control, generation, checkpoint_commit_id)?;
            control.reset_schema_presence();
            control.note_schemas(schema_keys.iter().map(String::as_str));
            tracked_snapshots.insert(root.commit_id, final_tracked);
            insert_direct_branch_control(&mut controls, &root.branch_id, control)?;
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
        if let Some(journal) = ordered_replacements.get(&root.commit_id) {
            if is_checkpoint_publication
                || !staged.selected_change_batches.is_empty()
                || !untracked_deltas.is_empty()
                || !engine_rows.is_empty()
                || !explicit_branch_targets.is_empty()
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "immutable replacement journal entered an incompatible HOT publication lane",
                ));
            }
            #[cfg(any(test, feature = "storage-benches"))]
            COMPLETE_REPLACEMENT_PACKED_CURRENT_BASE_PUBLICATIONS.with(|publications| {
                publications.set(publications.get().saturating_add(1));
            });
            let (generation, _retired_predecessor_bases) = tracked_head
                .writer(read, writes)
                .stage_complete_collection_replacement_current_base(
                    &root.branch_id,
                    parent_generation,
                    root.commit_id,
                    journal.schema_key(),
                    journal.row_count(),
                    row_columnar_write_sets,
                    working_diff_capture_checkpoint_commit_id,
                    &mut coverage,
                )
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.materialization.tracked_head.stage_direct_replacement_parts"
                ))
                .await?;
            #[cfg(test)]
            if _retired_predecessor_bases {
                COMPLETE_REPLACEMENT_PACKED_CURRENT_BASE_RETIREMENTS.with(|retirements| {
                    retirements.set(retirements.get().saturating_add(1));
                });
            }
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
            let mut control = normal_branch_head_control(
                root,
                parent_control,
                generation,
                working_diff_checkpoint_commit_id,
            )?;
            control.note_schemas(std::iter::once(journal.schema_key()));
            insert_direct_branch_control(&mut controls, &root.branch_id, control)?;
            continue;
        }
        if can_publish_ordered_packed_current_base {
            #[cfg(any(test, feature = "storage-benches"))]
            ORDERED_PACKED_CURRENT_BASE_PUBLICATIONS.with(|publications| {
                publications.set(publications.get().saturating_add(1));
            });
            let inventory = mutation_inventories.get(&root.commit_id).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "ordered current-base publication omitted its mutation inventory",
                )
            })?;
            let mut writer = tracked_head.writer(read, writes);
            let generation = if let (Some(parts), Some(lifecycle)) = (
                certified_columnar_parts,
                inventory.lifecycle_summary.as_ref(),
            ) {
                #[cfg(any(test, feature = "storage-benches"))]
                CERTIFIED_COLUMNAR_CURRENT_BASE_PUBLICATIONS.with(|publications| {
                    publications.set(publications.get().saturating_add(1));
                });
                writer
                    .stage_certified_columnar_insert_current_base(
                        &root.branch_id,
                        parent_generation,
                        root.commit_id,
                        parts,
                        lifecycle,
                        working_diff_capture_checkpoint_commit_id,
                        &mut coverage,
                    )
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.materialization.tracked_head.stage_certified_columnar_current_base"
                    ))
                    .await?
            } else {
                writer
                    .stage_ordered_insert_current_base(
                        &root.branch_id,
                        parent_generation,
                        root.commit_id,
                        state_row_indices
                            .iter()
                            .map(|&row_index| state_rows.row(row_index))
                            .map(|row| (row.schema_key.as_str(), row.row_pk)),
                        row_columnar_write_sets,
                        working_diff_capture_checkpoint_commit_id,
                        &mut coverage,
                    )
                    .instrument(tracing::debug_span!(
                        target: "lix_perf",
                        "lix.perf.materialization.tracked_head.stage_ordered_packed_current_base"
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
            let mut control = normal_branch_head_control(
                root,
                parent_control,
                generation,
                working_diff_checkpoint_commit_id,
            )?;
            if let Some(parts) = certified_columnar_parts {
                control.note_schemas(std::iter::once(parts.schema_key.as_str()));
                debug_assert!(
                    state_row_indices.len() == state_rows.len()
                        && insert_selection.len() == state_rows.len()
                        && untracked_deltas.is_empty()
                        && engine_rows.is_empty(),
                    "exclusive columnar publication must cover the whole prepared state batch"
                );
                // The same exclusivity proof makes the later global scan for
                // untracked-only branches redundant.
                exclusive_certified_columnar_publication = true;
            } else {
                control.note_schemas(
                    state_row_indices
                        .iter()
                        .map(|&row_index| state_rows.row(row_index).schema_key.as_str()),
                );
            }
            insert_direct_branch_control(&mut controls, &root.branch_id, control)?;
            continue;
        }
        if let Some(schema_key) = complete_replacement_schema {
            #[cfg(any(test, feature = "storage-benches"))]
            COMPLETE_REPLACEMENT_PACKED_CURRENT_BASE_PUBLICATIONS.with(|publications| {
                publications.set(publications.get().saturating_add(1));
            });
            let (generation, _retired_predecessor_bases) = tracked_head
                .writer(read, writes)
                .stage_complete_collection_replacement_current_base(
                    &root.branch_id,
                    parent_generation,
                    root.commit_id,
                    schema_key,
                    state_row_indices.len(),
                    row_columnar_write_sets,
                    working_diff_capture_checkpoint_commit_id,
                    &mut coverage,
                )
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.materialization.tracked_head.stage_complete_collection_replacement_current_base"
                ))
                .await?;
            #[cfg(test)]
            if _retired_predecessor_bases {
                COMPLETE_REPLACEMENT_PACKED_CURRENT_BASE_RETIREMENTS.with(|retirements| {
                    retirements.set(retirements.get().saturating_add(1));
                });
            }
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
            let mut control = normal_branch_head_control(
                root,
                parent_control,
                generation,
                working_diff_checkpoint_commit_id,
            )?;
            control.note_schemas(std::iter::once(schema_key));
            insert_direct_branch_control(&mut controls, &root.branch_id, control)?;
            continue;
        }
        let can_defer_fresh_hot = certified_fresh_plugin_file_id.is_some()
            && !host_certified_live_increments.contains_key(&root.branch_id)
            && tracked_roots.len() == 1
            && state_row_indices.len() == state_rows.len()
            && staged.selected_change_batches.is_empty()
            && selected_materialization.is_none()
            && untracked_deltas.is_empty()
            && engine_rows.is_empty()
            && explicit_branch_targets.is_empty()
            && checkpoint_epochs.is_empty();
        if can_defer_fresh_hot {
            let certified_file_id = certified_fresh_plugin_file_id
                .expect("deferred fresh hot publication requires its certificate");
            deferred_fresh_hot_plans.push(crate::hot_state::DeferredFreshHotPlan::new(
                &root.branch_id,
                parent_generation,
                state_rows,
                state_row_indices,
                certified_file_id,
                &absence_guards,
                working_diff_capture_checkpoint_commit_id,
                &mut coverage,
            )?);
            if let Some(epoch) = working_diff_epoch {
                let next_epoch = TrackedWorkingDiffEpoch {
                    checkpoint_commit_id: epoch.checkpoint_commit_id,
                    generation: parent_generation,
                    coverage,
                };
                if next_epoch != epoch {
                    stage_tracked_working_diff_epoch(writes, &root.branch_id, next_epoch)?;
                }
            }
            let mut control = normal_branch_head_control(
                root,
                parent_control,
                parent_generation,
                working_diff_checkpoint_commit_id,
            )?;
            control.note_schemas(
                state_row_indices
                    .iter()
                    .map(|&row_index| state_rows.row(row_index).schema_key.as_str()),
            );
            insert_direct_branch_control(&mut controls, &root.branch_id, control)?;
            continue;
        }
        let mut tracked_deltas = {
            let _span = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.materialization.tracked_head.deltas"
            )
            .entered();
            state_row_indices
                .iter()
                .filter(|&&row_index| {
                    let row = state_rows.row(row_index);
                    !row.untracked
                        && !host_certified_batch_owns_live_row(
                            row,
                            &root.branch_id,
                            root.commit_id,
                            host_certified_file_schemas,
                        )
                })
                .map(|&row_index| current_state_delta_from_state_row(state_rows.row(row_index)))
                .collect::<Result<Vec<_>, _>>()?
        };
        if let Some((selected_rows, selected_snapshots, selected_metadata)) =
            &selected_materialization
        {
            tracked_deltas.extend(
                selected_changes(&staged.selected_change_batches)
                    .zip(selected_rows)
                    .zip(selected_snapshots.iter().zip(selected_metadata))
                    .map(|((change_ref, row), (snapshot, metadata))| {
                        crate::hot_state::CurrentStateDeltaRef {
                            schema_key: &row.schema_key,
                            file_id: row.file_id.as_deref(),
                            row_pk: &row.row_pk,
                            change_id: Some(change_ref.change_id),
                            commit_id: Some(root.commit_id),
                            untracked: false,
                            deleted: change_ref.deleted,
                            created_at: change_ref.created_at,
                            updated_at: change_ref.updated_at,
                            snapshot: snapshot.as_ref_slot(),
                            metadata: metadata.as_ref_slot(),
                            columnar_base_coordinate: None,
                        }
                    }),
            );
        }
        let mut durable_predecessors = state_row_indices
            .iter()
            .filter_map(|&row_index| {
                let row = state_rows.row(row_index);
                (!host_certified_batch_owns_live_row(
                    row,
                    &root.branch_id,
                    root.commit_id,
                    host_certified_file_schemas,
                ))
                .then_some(row)
            })
            .filter_map(|row| {
                row.durable_predecessor.map(|value| {
                    crate::hot_state::CertifiedCurrentStatePredecessorRef {
                        schema_key: row.schema_key.as_str(),
                        file_id: row.file_id.map(crate::common::SharedStr::as_str),
                        row_pk: row.row_pk,
                        value,
                    }
                })
            })
            .collect::<Vec<_>>();
        if !durable_predecessors
            .windows(2)
            .all(|pair| compare_certified_predecessors(&pair[0], &pair[1]).is_lt())
        {
            durable_predecessors.sort_unstable_by(compare_certified_predecessors);
        }
        let packed_schema_keys = tracked_deltas
            .iter()
            .map(|delta| delta.schema_key)
            .collect::<BTreeSet<_>>();
        let packed_current_base_candidate = !is_checkpoint_publication
            && certified_fresh_plugin_file_id.is_none()
            && !host_certified_live_increments.contains_key(&root.branch_id)
            && staged.selected_change_batches.is_empty()
            && tracked_deltas.len() >= PACKED_CURRENT_BASE_MIN_ROWS
            && tracked_deltas.iter().all(|delta| {
                !delta.untracked
                    && !delta.deleted
                    && delta.file_id.is_none()
                    && delta.schema_key
                        != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
                    && delta.commit_id == Some(root.commit_id)
                    && delta.change_id.is_some()
            })
            && untracked_deltas
                .iter()
                .all(|delta| !packed_schema_keys.contains(delta.schema_key));
        // Identity sorting is useful only for a route that can actually
        // publish a packed base. Keep ordinary point/small-batch commits on
        // their allocation-free short circuit.
        let packed_guards_match = packed_current_base_candidate
            && !absence_guards.is_empty()
            && packed_current_base_guards_match(&tracked_deltas, &absence_guards);
        // Untracked deltas are ordinary in-place mutations of the same
        // generation, so stage them in the same pass as the tracked deltas:
        // one pass means one coherent view of the incremental collection
        // controls. The packed routes below reject untracked deltas by
        // construction, and they are only candidates when their collection
        // scopes are provably disjoint from the untracked ones, so those
        // routes stage the untracked deltas in a second in-place pass.
        let exact_delete_candidate = !is_checkpoint_publication
            && certified_fresh_plugin_file_id.is_none()
            && !host_certified_live_increments.contains_key(&root.branch_id)
            && staged.selected_change_batches.is_empty()
            && tracked_deltas.len() >= PACKED_CURRENT_BASE_MIN_ROWS
            && untracked_deltas.is_empty()
            && tracked_deltas.iter().all(|delta| {
                !delta.untracked
                    && delta.deleted
                    && delta.file_id.is_none()
                    && delta.schema_key
                        != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
                    && delta.commit_id == Some(root.commit_id)
                    && delta.change_id.is_some()
            });
        let stage_untracked_separately = packed_current_base_candidate || exact_delete_candidate;
        let deltas = if stage_untracked_separately {
            tracked_deltas.clone()
        } else {
            let mut deltas = Vec::with_capacity(tracked_deltas.len() + untracked_deltas.len());
            deltas.extend(tracked_deltas.iter().copied());
            deltas.extend(untracked_deltas.iter().copied());
            deltas
        };
        // Every absence guard above is derived from one of these exact
        // transaction deltas. The fresh-file certificate likewise proves its
        // complete file-scoped namespace absent. The branch-control CAS
        // protects both proofs through publication.
        let has_validated_insert_deltas = staged.selected_change_batches.is_empty()
            && (!absence_guards.is_empty() || certified_fresh_plugin_file_id.is_some());
        let mut writer = tracked_head.writer(read, writes);
        if let Some(schema_keys) = transaction_global_schema_keys.as_ref() {
            writer = writer.with_transaction_global_schema_keys(schema_keys);
        }
        let delete_generation = if exact_delete_candidate {
            writer
                .try_stage_exact_collection_delete_current_base(
                    &root.branch_id,
                    parent_generation,
                    root.parent_commit_id.ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "exact collection deletion lacks parent commit authority",
                        )
                    })?,
                    root.commit_id,
                    &tracked_deltas,
                    working_diff_capture_checkpoint_commit_id,
                )
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.materialization.tracked_head.stage_exact_collection_delete"
                ))
                .await?
        } else {
            None
        };
        let replacement_generation = if delete_generation.is_none()
            && packed_current_base_candidate
            && absence_guards.is_empty()
            && untracked_deltas.is_empty()
        {
            writer
                .try_stage_exact_collection_replacement_current_base(
                    &root.branch_id,
                    parent_generation,
                    root.parent_commit_id.ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "exact collection replacement lacks parent commit authority",
                        )
                    })?,
                    root.commit_id,
                    &tracked_deltas,
                    row_columnar_write_sets,
                    working_diff_capture_checkpoint_commit_id,
                    &mut coverage,
                )
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.materialization.tracked_head.stage_exact_collection_replacement"
                ))
                .await?
        } else {
            None
        };
        let packed_generation = if delete_generation.is_none()
            && replacement_generation.is_none()
            && packed_current_base_candidate
            && (packed_guards_match || absence_guards.is_empty())
        {
            writer
                .try_stage_packed_insert_current_base(
                    &root.branch_id,
                    parent_generation,
                    root.commit_id,
                    &tracked_deltas,
                    &absence_guards,
                    working_diff_capture_checkpoint_commit_id,
                    &mut coverage,
                )
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.materialization.tracked_head.stage_packed_current_base"
                ))
                .await?
        } else {
            None
        };
        let packed_generation = delete_generation
            .or(replacement_generation)
            .or(packed_generation);
        let can_publish_packed_current_base = packed_generation.is_some();
        tracing::debug!(
            target: "lix_perf",
            can_publish_packed_current_base,
            packed_current_base_candidate,
            exact_delete_candidate,
            tracked_delta_count = tracked_deltas.len(),
            packed_min_rows = PACKED_CURRENT_BASE_MIN_ROWS,
            untracked_delta_count = untracked_deltas.len(),
            absence_guard_count = absence_guards.len(),
            packed_guards_match,
            is_checkpoint_publication,
            has_certified_file = certified_fresh_plugin_file_id.is_some(),
            has_certified_counts = host_certified_live_increments.contains_key(&root.branch_id),
            has_selected_batches = !staged.selected_change_batches.is_empty(),
            durable_predecessor_count = durable_predecessors.len(),
            "packed current-base route decision"
        );
        let generation = if is_checkpoint_publication {
            // Reaching this point means an immutable packed base prevented
            // the O(deletes) epoch-rotation route above. Materialize it once;
            // retirement makes later checkpoints eligible for lazy rotation.
            coverage = WorkingDiffIndexCoverage::default();
            writer
                .stage_checkpoint_current_state(
                    &root.branch_id,
                    parent_generation,
                    root.commit_id,
                    &deltas,
                    &owned_absence_guards(&absence_guards),
                    checkpoint_commit_id.expect("checkpoint publication has an epoch commit id"),
                    &mut coverage,
                )
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.materialization.tracked_head.stage_checkpoint"
                ))
                .await?
        } else if let Some(generation) = packed_generation {
            generation
        } else if let Some(certified_live_increments) =
            host_certified_live_increments.get(&root.branch_id)
        {
            let owned_absence_guards = owned_absence_guards(&absence_guards);
            writer
                .stage_current_state_with_certified_counts(
                    &root.branch_id,
                    Some(parent_generation),
                    root.commit_id,
                    &deltas,
                    &owned_absence_guards,
                    working_diff_capture_checkpoint_commit_id,
                    &mut coverage,
                    certified_live_increments,
                )
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.materialization.tracked_head.stage_current_state"
                ))
                .await?
        } else if has_validated_insert_deltas {
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
            let owned_absence_guards = owned_absence_guards(&absence_guards);
            Box::pin(writer.stage_current_state_with_certified_predecessors(
                &root.branch_id,
                Some(parent_generation),
                root.commit_id,
                &deltas,
                &durable_predecessors,
                &owned_absence_guards,
                None,
                None,
                working_diff_capture_checkpoint_commit_id,
                &mut coverage,
            ))
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.materialization.tracked_head.stage_current_state"
            ))
            .await?
        };
        // The index plane is staged from this commit's own rows, so it is
        // correct whichever physical route above published them, and it lands
        // in the same write set as the rows themselves.
        let (index_entries, index_witnesses) =
            hot_index_writes_for_commit(state_rows, &root.branch_id, parent_control.as_ref());
        if !index_entries.is_empty() || !index_witnesses.is_empty() {
            crate::hot_state::stage_hot_index_entries(
                read,
                writes,
                &root.branch_id,
                generation,
                &index_entries,
                &index_witnesses,
            )
            .await?;
        }
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
        let mut control = normal_branch_head_control(
            root,
            parent_control,
            generation,
            working_diff_checkpoint_commit_id,
        )?;
        // The packed routes could not carry the history-free rows. Mutate the
        // same generation they just published, in place.
        // `normal_branch_head_control` already advanced
        // `current_state_revision`, so the control CAS still fences this write.
        if stage_untracked_separately && !untracked_deltas.is_empty() {
            let mut untracked_coverage = WorkingDiffIndexCoverage::default();
            tracked_head
                .writer(read, writes)
                .stage_current_state_with_working_diff(
                    &root.branch_id,
                    Some(generation),
                    generation,
                    &untracked_deltas,
                    &BTreeSet::new(),
                    None,
                    None,
                    None,
                    &mut untracked_coverage,
                )
                .await?;
        }
        control.note_schemas(
            deltas
                .iter()
                .map(|delta| delta.schema_key)
                .chain(untracked_deltas.iter().map(|delta| delta.schema_key)),
        );
        insert_direct_branch_control(&mut controls, &root.branch_id, control)?;
    }

    // An untracked-only transaction touches the same hot rows without
    // creating a commit.  Explicit branch ref publication handles its own
    // branch-local untracked mutations in the fresh generation below.
    let rooted_branches = tracked_roots
        .iter()
        .map(|root| root.branch_id.as_str())
        .collect::<BTreeSet<_>>();
    let current_only_branches = if exclusive_certified_columnar_publication {
        BTreeSet::new()
    } else {
        state_rows
            .iter()
            .filter(|row| row.untracked && row.schema_key != BRANCH_REF_SCHEMA_KEY)
            .map(|row| row.branch_id.as_str())
            .chain(engine_rows.iter().map(|row| row.branch_id.as_str()))
            .filter(|branch_id| {
                !rooted_branches.contains(branch_id) && !explicit_branches.contains(*branch_id)
            })
            .collect::<BTreeSet<_>>()
    };
    for branch_id in current_only_branches {
        let mut control = observations
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
        let absence_guards =
            tracked_head_absence_guards(state_rows, insert_selection, branch_id, None);
        let owned_absence_guards = absence_guards
            .iter()
            .map(|guard| TrackedStateKey {
                schema_key: guard.schema_key.to_owned(),
                row_pk: guard.row_pk.clone(),
                file_id: guard.file_id.map(str::to_owned),
            })
            .collect::<BTreeSet<_>>();
        let next_revision = control
            .current_state_revision
            .checked_add(1)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "branch current-state revision overflowed",
                )
            })?;
        if !deltas.is_empty() {
            let mut untracked_coverage = WorkingDiffIndexCoverage::default();
            tracked_head
                .writer(read, writes)
                .stage_current_state_with_working_diff(
                    branch_id,
                    Some(control.tracked_generation),
                    control.tracked_generation,
                    &deltas,
                    &owned_absence_guards,
                    None,
                    None,
                    None,
                    &mut untracked_coverage,
                )
                .await?;
        }
        control.current_state_revision = next_revision;
        control.note_schemas(deltas.iter().map(|delta| delta.schema_key));
        insert_direct_branch_control(&mut controls, branch_id, control)?;
    }
    Ok(StagedHotHeads {
        controls,
        deferred_fresh_hot_plans,
    })
}

/// Builds the INSERT guards that still need current-state enforcement.
///
/// A certified fresh plugin file was already checked for absence against the
/// coherent transaction snapshot. Its file-scoped rows are also skipped by
/// the validated hot writer under the branch-control CAS. The row-ordinal
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
            row_pk: row.row_pk,
        })
        .collect::<Vec<_>>();
    guards.sort_unstable_by(|left, right| {
        left.schema_key
            .cmp(right.schema_key)
            .then_with(|| left.row_pk.cmp(right.row_pk))
            .then_with(|| left.file_id.cmp(&right.file_id))
    });
    guards
}

fn owned_absence_guards(guards: &[TrackedStateKeyRef<'_>]) -> BTreeSet<TrackedStateKey> {
    guards
        .iter()
        .map(|guard| TrackedStateKey {
            schema_key: guard.schema_key.to_string(),
            file_id: guard.file_id.map(str::to_string),
            row_pk: guard.row_pk.clone(),
        })
        .collect()
}

/// A tracked INSERT must also observe the branch-local untracked generation.
/// The tracked writer intentionally reads only its authenticated tracked root;
/// when the selectors diverge, validate the correlated INSERT identities
/// against the untracked generation before publishing the tracked root.
fn packed_current_base_guards_match(
    deltas: &[crate::hot_state::CurrentStateDeltaRef<'_>],
    guards: &[TrackedStateKeyRef<'_>],
) -> bool {
    if deltas.len() != guards.len() {
        return false;
    }
    let mut delta_identities = deltas
        .iter()
        .map(|delta| (delta.schema_key, delta.row_pk, delta.file_id))
        .collect::<Vec<_>>();
    delta_identities.sort_unstable();
    let mut guard_identities = guards
        .iter()
        .map(|guard| (guard.schema_key, guard.row_pk, guard.file_id))
        .collect::<Vec<_>>();
    guard_identities.sort_unstable();
    delta_identities == guard_identities
}

/// A selected historical change becomes visible through a newly materialized
/// hot generation while that publication retains the branch's untracked
/// members. The two retention modes must never own the same logical identity.
///
/// This is a merge/checkpoint lifecycle fence, not a normal CRUD-path check.
/// Point-loading only the selected identities keeps large untracked repositories
/// out of the publication cost.
async fn reject_selected_tracked_refs_with_untracked_rows(
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
            row_pk: change_ref.row_pk().clone(),
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
                row_pk: row.row_pk,
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
    state_rows: &PreparedStateBatch,
    engine_rows: &[EngineCurrentRow],
) {
    for row in state_rows.iter().filter(|row| {
        row.untracked && row.branch_id == branch_id && row.schema_key != BRANCH_REF_SCHEMA_KEY
    }) {
        let identity = TrackedStateKey {
            schema_key: row.schema_key.to_string(),
            file_id: row.file_id.map(ToString::to_string),
            row_pk: row.row_pk.clone(),
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
            row_pk: row.change.row_pk.clone(),
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
            "cannot publish selected tracked change on branch '{branch_id}': it conflicts with an untracked current row for schema '{}' row_pk {:?}",
            identity.schema_key, identity.row_pk
        ),
    )
    .with_hint("Resolve the tracked and untracked identity conflict before retrying.")
    .with_details(serde_json::json!({
        "kind": "trackedUntrackedIdentityCollision",
        "branchId": branch_id,
        "schemaKey": &identity.schema_key,
        "rowPk": &identity.row_pk,
        "fileId": &identity.file_id,
    }))
}

/// A checkpoint cleans exactly its selected interval changes in the current
/// hot generation. Unchanged rows were already clean, so rotating to an empty
/// sparse dirty-key index starts the next epoch without a full-state rewrite.
async fn stage_checkpoint_working_diff_epochs(
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
        if control.working_diff_checkpoint_commit_id != Some(recovery.checkpoint_commit_id) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "checkpoint '{}' does not match staged working-diff baseline for '{}'",
                    recovery.checkpoint_commit_id, recovery.branch_id
                ),
            ));
        }
        // A partial checkpoint publishes a child head whose unselected
        // changes remain dirty relative to the intermediate checkpoint. The
        // empty HOT epoch is valid only when the checkpoint itself is the
        // published head; otherwise the historical diff path reconstructs
        // the remaining working diff from the two durable roots.
        if control.head_commit_id != recovery.checkpoint_commit_id {
            continue;
        }
        // The previous sparse index becomes unreachable when this marker and
        // branch control commit. Repository GC reclaims stale epoch prefixes;
        // scanning them here would make checkpoint publication O(D).
        stage_tracked_working_diff_epoch(
            writes,
            &recovery.branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: recovery.checkpoint_commit_id,
                generation: control.tracked_generation,
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
        tracked_generation: generation,
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

fn bind_branch_checkpoint_bridge(
    branch_id: &str,
    target: &ExplicitBranchHeadTarget,
    existing: Option<BranchHeadControl>,
    control: &mut BranchHeadControl,
    bridge: &crate::gc::CheckpointRecoveryRef,
) -> Result<TrackedWorkingDiffEpoch, LixError> {
    let Some(target_head) = target.head_commit_id else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("checkpoint ancestry bridge cannot delete branch '{branch_id}'"),
        ));
    };
    if bridge.branch_id != branch_id
        || existing.is_some()
        || bridge.recovered_head_commit_id != target_head
        || control.head_commit_id != target_head
        || bridge.checkpoint_commit_id == target_head
        || !bridge.interval_has_commits
        // New branches provisionally start their private working-diff epoch at
        // the exact root they were created from. A pending authenticated
        // checkpoint bridge may replace only that default cursor; an absent
        // or arbitrary preexisting baseline is a different authority and
        // must fail closed.
        || control.working_diff_checkpoint_commit_id != Some(target_head)
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("branch '{branch_id}' has an invalid checkpoint ancestry bridge"),
        ));
    }
    control.working_diff_checkpoint_commit_id = Some(bridge.checkpoint_commit_id);
    Ok(TrackedWorkingDiffEpoch {
        checkpoint_commit_id: bridge.checkpoint_commit_id,
        generation: control.tracked_generation,
        coverage: WorkingDiffIndexCoverage::default(),
    })
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
    previous_control: Option<BranchHeadControl>,
    stage_initial_working_diff_epoch: bool,
    state_rows: &PreparedStateBatch,
    engine_rows: &[EngineCurrentRow],
    insert_selection: &PreparedInsertSelection,
) -> Result<BranchHeadControl, LixError> {
    let tracked_head = TrackedHeadContext::new();
    // Tracked and untracked rows share one serving generation, so minting a
    // fresh one strands the branch's history-free rows in the old one. A
    // republication that does not move the head therefore keeps its
    // generation; there is nothing new to serve and nothing to copy.
    //
    // A publication that *does* move the head (or deletes the branch) is
    // destructive, and `reject_explicit_branch_ref_lifecycle_with_untracked_rows`
    // already refuses it while branch-local untracked rows exist — so a fresh
    // generation there is only ever reached with no untracked rows to lose.
    let reused_generation = previous_control
        .filter(|previous| previous.head_commit_id == head_commit_id)
        .map(|previous| previous.tracked_generation);
    let generation = match reused_generation {
        Some(generation) => generation,
        None => {
            let generation = lifecycle_generation(branch_id, head_commit_id, target.ref_change_id);
            tracked_head.writer(read, writes).stage_root_current_base(
                branch_id,
                generation,
                head_commit_id,
            );
            generation
        }
    };
    if stage_initial_working_diff_epoch {
        stage_tracked_working_diff_epoch(
            writes,
            branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: head_commit_id,
                generation,
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )?;
    }
    let mut control = BranchHeadControl {
        head_commit_id,
        tracked_generation: generation,
        current_state_revision: match previous_control {
            None => 0,
            // An explicit move of an existing head is a new current-state
            // revision for every reader holding the previous one.
            Some(control) => next_current_state_revision(control.current_state_revision)?,
        },
        // A branch is born at a complete authenticated root. Its private
        // working interval therefore starts at that exact head; no logical
        // checkpoint row or history scan is needed to recover the cursor.
        // Explicit lifecycle moves of an existing branch instead keep that
        // branch's own compaction baseline.
        working_diff_checkpoint_commit_id: match previous_control {
            None => Some(head_commit_id),
            Some(control) => control.working_diff_checkpoint_commit_id,
        },
        created_at: previous_control.map_or(target.created_at, |control| control.created_at),
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
    if !untracked_deltas.is_empty() {
        // A new branch has not consumed its revision yet; an existing branch
        // already advanced one above for this same publication.
        let revision = match previous_control {
            None => next_current_state_revision(control.current_state_revision)?,
            Some(_) => control.current_state_revision,
        };
        let absence_guards = if insert_selection.is_empty() {
            BTreeSet::new()
        } else {
            state_rows
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
                    row_pk: row.row_pk.clone(),
                })
                .collect()
        };
        let mut untracked_coverage = WorkingDiffIndexCoverage::default();
        tracked_head
            .writer(read, writes)
            .stage_current_state_with_working_diff(
                branch_id,
                Some(control.tracked_generation),
                control.tracked_generation,
                &untracked_deltas,
                &absence_guards,
                None,
                None,
                None,
                &mut untracked_coverage,
            )
            .await?;
        control.current_state_revision = revision;
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
    branch_checkpoint_bridges: &BTreeMap<String, crate::gc::CheckpointRecoveryRef>,
    preconditions: &mut Vec<StoragePrecondition>,
    observations: &BTreeMap<String, BranchHeadControlObservation>,
    root_backed_branch_publications: &mut BTreeSet<String>,
) -> Result<BTreeMap<String, BranchHeadControl>, LixError> {
    let checkpoint_epochs = checkpoint_epoch_bindings(checkpoint_publications)?;
    if let Some(branch_id) = branch_checkpoint_bridges
        .keys()
        .find(|branch_id| checkpoint_epochs.contains_key(*branch_id))
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "branch '{branch_id}' cannot publish a checkpoint and an ancestry bridge together"
            ),
        ));
    }
    let mut publications = normal_controls
        .iter()
        .map(|(branch_id, control)| (branch_id.clone(), Some(*control)))
        .collect::<BTreeMap<String, Option<BranchHeadControl>>>();
    let mut consumed_checkpoint_bridges = BTreeSet::new();
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
        let mut desired = match target.head_commit_id {
            None => None,
            Some(head_commit_id) => {
                // Every explicit head move publishes one immutable tracked
                // root reference. The head commit is already authoritative for
                // every tracked identity, so materializing a branch-local copy
                // of the whole working set would only restate it.
                let control = Box::pin(stage_root_backed_branch_publication(
                    read,
                    writes,
                    branch_id,
                    head_commit_id,
                    target,
                    existing,
                    existing.is_none() && !branch_checkpoint_bridges.contains_key(branch_id),
                    state_rows,
                    engine_rows,
                    insert_selection,
                ))
                .await?;
                root_backed_branch_publications.insert(branch_id.clone());
                Some(control)
            }
        };
        if let Some(bridge) = branch_checkpoint_bridges.get(branch_id) {
            let control = desired.as_mut().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "checkpoint ancestry bridge has no branch publication for '{branch_id}'"
                    ),
                )
            })?;
            let epoch =
                bind_branch_checkpoint_bridge(branch_id, target, existing, control, bridge)?;
            stage_tracked_working_diff_epoch(writes, branch_id, epoch)?;
            crate::gc::stage_recovery_ref_rotation(writes, bridge)?;
            consumed_checkpoint_bridges.insert(branch_id.clone());
        }
        publications.insert(branch_id.clone(), desired);
    }
    if consumed_checkpoint_bridges.len() != branch_checkpoint_bridges.len() {
        let branch_id = branch_checkpoint_bridges
            .keys()
            .find(|branch_id| !consumed_checkpoint_bridges.contains(*branch_id))
            .expect("bridge count differs only when one branch was not consumed");
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "checkpoint ancestry bridge has no explicit branch publication for '{branch_id}'"
            ),
        ));
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
        // Everything a superseded control still owned is retired right here,
        // in the same atomic write set that supersedes it. These are all
        // control-scoped facts — the branch's derived serving generations, its
        // superseded standalone branch-ref change, and, on deletion, its
        // plugin-checkpoint prefix — reachable from exactly one place: this
        // control. Once it moves, nothing can ever read them again, so there is
        // nothing to defer and nothing for a publication ledger to remember.
        if let Some(old_control) = observation.control.as_ref() {
            let generation = old_control.tracked_generation;
            if !desired.is_some_and(|new_control| new_control.tracked_generation == generation) {
                crate::hot_state::stage_retire_hot_generation(read, writes, branch_id, generation)
                    .await?;
            }
            if desired
                .is_none_or(|new_control| new_control.ref_change_id != old_control.ref_change_id)
            {
                crate::changelog::stage_delete_standalone_change(
                    read,
                    writes,
                    old_control.ref_change_id,
                )
                .await?;
            }
        }
        match desired {
            Some(control) => stage_branch_head_control(writes, branch_id, *control)?,
            None => {
                stage_delete_branch_head_control(writes, branch_id)?;
                crate::gc::stage_delete_recovery_ref(writes, branch_id)?;
                // A deleted branch must not keep its derived plugin-checkpoint
                // prefix alive; a recreated branch republishes it.
                crate::transaction::stage_delete_branch_plugin_checkpoints(read, writes, branch_id)
                    .await?;
            }
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

fn prepare_row_columnar_write_sets(
    state_rows: &mut PreparedStateBatch,
    insert_selection: &PreparedInsertSelection,
    row_schema_catalog: Option<&crate::catalog::CatalogSnapshot>,
) -> Result<crate::hot_state::RowColumnarWriteSets, LixError> {
    if state_rows.len() < PACKED_CURRENT_BASE_MIN_ROWS {
        return Ok(crate::hot_state::RowColumnarWriteSets::new());
    }
    let publishes_ordered_insert =
        insert_selection.len() == state_rows.len() && insert_selection.covers_all(state_rows.len());
    if !publishes_ordered_insert {
        return Ok(crate::hot_state::RowColumnarWriteSets::new());
    }
    if let Some((commit_id, schema_key, row_groups)) = state_rows.take_dense_row_columnar() {
        let layout_is_current = row_schema_catalog
            .and_then(|catalog| catalog.schema(&schema_key))
            .and_then(|schema| crate::sql2::derive_schema_surface_spec_from_schema(schema).ok())
            .is_some_and(|spec| {
                row_groups.manifest.namespace == schema_key
                    && row_groups
                        .manifest
                        .metadata
                        .get(crate::sql2::ROW_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY)
                        .is_some_and(|fingerprint| {
                            fingerprint == &spec.columnar_layout_fingerprint()
                        })
                    && row_groups.input_locations.len() == state_rows.len()
            });
        if layout_is_current {
            let (row_group_set, input_locations) = row_groups.into_parts();
            let mut encoded = match input_locations {
                crate::sql2::RowGroupLocations::Dense { row_count } => {
                    crate::hot_state::RowColumnarWriteSets::with_dense_state_rows(row_count)
                }
                crate::sql2::RowGroupLocations::Explicit(locations) => {
                    let mut encoded = crate::hot_state::RowColumnarWriteSets::with_state_row_count(
                        state_rows.len(),
                    );
                    for (state_row_index, location) in locations.into_iter().enumerate() {
                        encoded.set_state_row_location(state_row_index, location);
                    }
                    encoded
                }
            };
            encoded.insert((commit_id, schema_key), row_group_set);
            return Ok(encoded);
        }
    }
    if let Some((commit_id, schema_key, snapshots)) = state_rows.dense_row_columnar_input() {
        let Some(schema) = row_schema_catalog.and_then(|catalog| catalog.schema(schema_key)) else {
            return Ok(crate::hot_state::RowColumnarWriteSets::new());
        };
        let Ok(spec) = crate::sql2::derive_schema_surface_spec_from_schema(schema) else {
            return Ok(crate::hot_state::RowColumnarWriteSets::new());
        };
        let rows = state_rows.iter().zip(snapshots).map(|(row, snapshot)| {
            crate::sql2::RowColumnarRowRef {
                row_pk: row.row_pk,
                snapshot_bytes: snapshot.normalized().as_bytes(),
                snapshot_value: snapshot.value(),
            }
        });
        let mut encoded =
            crate::hot_state::RowColumnarWriteSets::with_state_row_count(state_rows.len());
        if let Some(row_groups) = crate::sql2::encode_registered_row_groups(&spec, rows)? {
            let (row_group_set, input_locations) = row_groups.into_parts();
            for (state_row_index, location) in input_locations.iter().enumerate() {
                encoded.set_state_row_location(state_row_index, location);
            }
            encoded.insert((commit_id, schema_key.to_string()), row_group_set);
        }
        return Ok(encoded);
    }
    let mut indices = BTreeMap::<(CommitId, String), Vec<usize>>::new();
    for (index, row) in state_rows.iter().enumerate() {
        if !insert_selection.contains(index) {
            return Ok(crate::hot_state::RowColumnarWriteSets::new());
        }
        let (Some(commit_id), Some(_snapshot)) = (row.commit_id, row.snapshot) else {
            continue;
        };
        if row.untracked || row.global || row.file_id.is_some() {
            continue;
        }
        indices
            .entry((commit_id, row.schema_key.to_string()))
            .or_default()
            .push(index);
    }
    let mut encoded =
        crate::hot_state::RowColumnarWriteSets::with_state_row_count(state_rows.len());
    for ((commit_id, schema_key), row_indices) in indices {
        if row_indices.len() < PACKED_CURRENT_BASE_MIN_ROWS {
            continue;
        }
        let Some(schema) = row_schema_catalog.and_then(|catalog| catalog.schema(&schema_key))
        else {
            continue;
        };
        let Ok(spec) = crate::sql2::derive_schema_surface_spec_from_schema(schema) else {
            continue;
        };
        let rows = row_indices.iter().map(|&index| {
            let row = state_rows.row(index);
            let snapshot = row
                .snapshot
                .expect("columnar row index retained a snapshot");
            crate::sql2::RowColumnarRowRef {
                row_pk: row.row_pk,
                snapshot_bytes: snapshot.normalized().as_bytes(),
                snapshot_value: snapshot.value(),
            }
        });
        if let Some(row_groups) = crate::sql2::encode_registered_row_groups(&spec, rows)? {
            let (row_group_set, input_locations) = row_groups.into_parts();
            for (&state_row_index, location) in row_indices.iter().zip(input_locations.iter()) {
                encoded.set_state_row_location(state_row_index, location);
            }
            encoded.insert((commit_id, schema_key), row_group_set);
        }
    }
    Ok(encoded)
}

fn explicit_branch_head_targets(
    state_rows: &PreparedStateBatch,
) -> Result<BTreeMap<String, ExplicitBranchHeadTarget>, LixError> {
    let mut targets = BTreeMap::new();
    for row in state_rows {
        if row.schema_key != BRANCH_REF_SCHEMA_KEY || !row.untracked {
            continue;
        }
        let branch_id = row.row_pk.as_single_string_owned()?;
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
            .scan_live_batch_for_retention(
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
                Some(true),
            )
            .await?
            .into_rows()
            .into_iter()
            .filter(|row| row.untracked)
            .map(|row| TrackedStateKey {
                schema_key: row.schema_key,
                row_pk: row.row_pk,
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
            branch_ids.insert(row.row_pk.as_single_string_owned()?);
        } else if row.untracked {
            branch_ids.insert(row.branch_id.to_string());
        }
    }
    branch_ids.extend(engine_rows.iter().map(|row| row.branch_id.clone()));
    // Every authored change references a global account. Observing and later
    // fencing this control serializes account deletion/disable with writes on
    // otherwise independent branches.
    branch_ids.insert(crate::GLOBAL_BRANCH_ID.to_string());
    let branch_ids = branch_ids.into_iter().collect::<Vec<_>>();
    let observations = BranchHeadControlContext::new()
        .reader(read)
        .load_observed(&branch_ids)
        .await?;
    Ok(branch_ids.into_iter().zip(observations).collect())
}

async fn stage_tracked_roots(
    tracked_state: &TrackedStateContext,
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state_rows: &PreparedStateBatch,
    tracked_row_indices_by_commit: &BTreeMap<CommitId, Vec<RowIndex>>,
    tracked_roots: &[PendingTrackedRoot],
    rootless_commit_ids: &BTreeSet<CommitId>,
    durable_root_rebuild_parents: &BTreeSet<CommitId>,
    staged_root_rebuild_commits: &BTreeSet<CommitId>,
    staged_commits: &BTreeMap<CommitId, StagedChangelogCommit>,
    insert_selection: &PreparedInsertSelection,
    certified_packet_root_rows: &BTreeMap<CommitId, Vec<MaterializedHotStateRow>>,
    certified_replacement_markers_by_commit: &BTreeMap<CommitId, BTreeSet<TrackedStateKey>>,
) -> Result<BTreeMap<CommitId, TrackedStateCommitRoot>, LixError> {
    let root_fence_ids = tracked_root_fence_ids(tracked_roots);
    if root_fence_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut tracked_writer = tracked_state.writer(read, writes);
    let mut staged_rebuild_plan_ids = BTreeSet::new();
    #[cfg(feature = "storage-benches")]
    let _replay_scope = (!durable_root_rebuild_parents.is_empty())
        .then(crate::storage_bench::RootReplayScope::enter);
    for parent_commit_id in durable_root_rebuild_parents {
        #[cfg(feature = "storage-benches")]
        let plan_load_start = std::time::Instant::now();
        let plans = crate::tracked_state::load_rebuild_plans_to_nearest_available_root(
            read,
            &parent_commit_id.to_string(),
            true,
        )
        .await?;
        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_root_replay_plan_load_nanos(
                plan_load_start.elapsed().as_nanos() as u64,
            );
            crate::storage_bench::record_root_replay_boundary(plans.len());
        }
        let all_new = plans
            .iter()
            .all(|plan| !staged_rebuild_plan_ids.contains(&plan.commit_id));
        #[cfg(feature = "storage-benches")]
        let stage_start = std::time::Instant::now();
        if all_new
            && crate::tracked_state::try_stage_collapsed_rebuild_plans_with_writer(
                &mut tracked_writer,
                &plans,
            )
            .await?
            .is_some()
        {
            #[cfg(feature = "storage-benches")]
            {
                crate::storage_bench::record_root_replay_plan_staged();
                crate::storage_bench::record_root_replay_stage_nanos(
                    stage_start.elapsed().as_nanos() as u64,
                );
            }
            // A collapsed replay stages only its terminal root. Intermediate
            // plan IDs remain unstaged so another rebuild parent sharing this
            // suffix can independently collapse against immutable authority.
            staged_rebuild_plan_ids.insert(plans[0].commit_id);
            continue;
        }
        for plan in plans.iter().rev() {
            if staged_rebuild_plan_ids.insert(plan.commit_id) {
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_root_replay_plan_staged();
                crate::tracked_state::stage_rebuild_plan_with_writer(&mut tracked_writer, plan)
                    .await?;
            }
        }
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_root_replay_stage_nanos(
            stage_start.elapsed().as_nanos() as u64
        );
    }
    let empty_certified_replacement_markers = BTreeSet::new();
    for root in tracked_roots_parent_first(tracked_roots)? {
        if !root_fence_ids.contains(&root.commit_id) {
            continue;
        }
        if rootless_commit_ids.contains(&root.commit_id)
            && !staged_root_rebuild_commits.contains(&root.commit_id)
        {
            continue;
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
        let certified_root_rows = certified_packet_root_rows
            .get(&root.commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let certified_replacement_markers = certified_replacement_markers_by_commit
            .get(&root.commit_id)
            .unwrap_or(&empty_certified_replacement_markers);
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
        // Normal row batches are already in canonical primary-key order.
        // When they cover a substantial fraction of a parent root, stream the
        // parent/changes directly into canonical chunks instead of point
        // reading every key and materializing two more full-workload vectors.
        if certified_root_rows.is_empty()
            && certified_replacement_markers.is_empty()
            && !state_row_indices.is_empty()
            && staged.selected_change_batches.is_empty()
            && tracked_state_rows_are_strictly_sorted(state_rows, state_row_indices)
        {
            let commit_id_text = root.commit_id.to_string();
            let parent_commit_id_text = root.parent_commit_id.map(|id| id.to_string());
            let file_delete_cascades = state_row_indices
                .iter()
                .filter_map(|&row_index| {
                    let row = state_rows.row(row_index);
                    (row.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY && row.snapshot.is_none())
                        .then_some(row)
                })
                .map(|row| {
                    let delta = tracked_delta_from_state_row(row)?;
                    let file_id = row.row_pk.as_single_string_owned().map_err(|error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("file descriptor tombstone has invalid identity: {error}"),
                        )
                    })?;
                    Ok((file_id, delta))
                })
                .collect::<Result<BTreeMap<_, _>, LixError>>()?;
            let first_row = state_rows.row(state_row_indices[0]);
            let first_mutation_key = encode_key_ref(TrackedStateKeyRef {
                schema_key: first_row.schema_key,
                file_id: first_row.file_id.map(crate::common::SharedStr::as_str),
                row_pk: first_row.row_pk,
            });
            if tracked_writer
                .try_stage_bulk_parent_root_from_ordered_mutations(
                    &commit_id_text,
                    parent_commit_id_text.as_deref(),
                    state_row_indices.len(),
                    &first_mutation_key,
                    &file_delete_cascades,
                    OrderedStateRowMutations::new(state_row_indices, state_rows, insert_selection),
                )
                .await?
                .is_some()
            {
                continue;
            }
        }
        let deltas = state_row_indices
            .iter()
            .map(|&row_index| tracked_delta_from_state_row(state_rows.row(row_index)))
            .chain(
                certified_root_rows
                    .iter()
                    .map(tracked_delta_from_certified_root_row),
            )
            .chain(
                selected_changes(&staged.selected_change_batches).map(|change_ref| {
                    tracked_delta_from_selected_change_ref(change_ref, root.commit_id)
                }),
            )
            .collect::<Result<Vec<_>, _>>()?;
        let absence_guards = if insert_selection.is_empty() {
            BTreeSet::new()
        } else {
            state_row_indices
                .iter()
                .filter_map(|&row_index| {
                    let row = state_rows.row(row_index);
                    if row.snapshot.is_none() || row.untracked {
                        return None;
                    }
                    if !insert_selection.contains(row_index) {
                        return None;
                    }
                    Some(TrackedStateKey {
                        schema_key: row.schema_key.to_string(),
                        file_id: row.file_id.map(ToString::to_string),
                        row_pk: row.row_pk.clone(),
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
                certified_replacement_markers,
            )
            .await?;
    }
    Ok(tracked_writer
        .staged_commit_roots()
        .map(|root| (root.commit_id, root.clone()))
        .collect())
}

fn stage_commit_state_manifests<'a, S>(
    read: &'a S,
    writes: &'a mut StorageWriteSet,
    commit_rows: &'a [FinalizedCommitRow],
    mutation_inventories: &'a BTreeMap<CommitId, CommitStateMutationInventory>,
    rootless_commit_ids: &'a BTreeSet<CommitId>,
    staged_commits: &'a BTreeMap<CommitId, StagedChangelogCommit>,
    snapshot_roots: &'a BTreeMap<CommitId, TrackedStateCommitRoot>,
    external_parent_manifests: &'a BTreeMap<
        CommitId,
        crate::tracked_state::PublishedCommitStateTopology,
    >,
) -> std::pin::Pin<Box<dyn Future<Output = Result<(), LixError>> + Send + 'a>>
where
    S: StorageAdapterRead + ?Sized + 'a,
{
    Box::pin(async move {
        let mut published_manifests =
            BTreeMap::<CommitId, crate::tracked_state::StagedCommitStateManifest>::new();
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
            let rootless = rootless_commit_ids.contains(&record.commit_id);
            let snapshot_root = snapshot_roots.get(&record.commit_id).cloned();
            if rootless == snapshot_root.is_some() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "commit '{}' has inconsistent mutation-journal and snapshot-root publication",
                        record.commit_id
                    ),
                ));
            }
            let mut mutations = mutation_inventories
                .get(&record.commit_id)
                .cloned()
                .unwrap_or_default();
            let first_parent =
                (record.parent_commit_ids.len() == 1).then(|| record.parent_commit_ids[0]);
            let staged_parent =
                first_parent.and_then(|parent_id| published_manifests.get(&parent_id));
            let external_parent = if staged_parent.is_none() {
                match first_parent {
                    Some(parent_id) => {
                        Some(external_parent_manifests.get(&parent_id).ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "commit '{}' has no published parent authority",
                                    record.commit_id
                                ),
                            )
                        })?)
                    }
                    None => None,
                }
            } else {
                None
            };
            let catalog_publication = if record.parent_commit_ids.len() <= 1
                && mutations.selected_source_commit_id.is_none()
            {
                Some(if let Some(parent) = staged_parent {
                    crate::tracked_state::stage_current_state_scoped_ranges_from_staged_parent(
                        read,
                        writes,
                        parent,
                        record.commit_id,
                        &record.account_id,
                        &mutations,
                    )
                    .await?
                } else {
                    crate::tracked_state::stage_current_state_scoped_ranges_from_published_topology_parent(
                        read,
                        writes,
                        external_parent,
                        record.commit_id,
                        &record.account_id,
                        &mutations,
                    )
                    .await?
                })
            } else {
                let topology_parents = record
                    .parent_commit_ids
                    .iter()
                    .map(|parent_id| {
                        published_manifests
                            .get(parent_id)
                            .map(crate::tracked_state::CertifiedCommitStateTopologyParent::Staged)
                            .or_else(|| {
                                external_parent_manifests.get(parent_id).map(
                                    crate::tracked_state::CertifiedCommitStateTopologyParent::PublishedTopology,
                                )
                            })
                            .ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    format!(
                                        "commit '{}' has no certified topology parent '{}'",
                                        record.commit_id, parent_id
                                    ),
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let selected_source_id = mutations.selected_source_commit_id();
                let loaded_selected_source = if let Some(source_id) = selected_source_id
                    && !published_manifests.contains_key(&source_id)
                    && !external_parent_manifests.contains_key(&source_id)
                {
                    Some(
                        crate::tracked_state::load_published_commit_state_topology(read, source_id)
                            .await?
                            .ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    format!(
                                        "commit '{}' has no published selected-source authority '{}'",
                                        record.commit_id, source_id
                                    ),
                                )
                            })?,
                    )
                } else {
                    None
                };
                let selected_source = selected_source_id
                    .map(|source_id| {
                        published_manifests
                            .get(&source_id)
                            .map(crate::tracked_state::CertifiedCommitStateTopologyParent::Staged)
                            .or_else(|| {
                                external_parent_manifests.get(&source_id).map(
                                    crate::tracked_state::CertifiedCommitStateTopologyParent::PublishedTopology,
                                )
                            })
                            .or_else(|| {
                                loaded_selected_source.as_ref().map(
                                    crate::tracked_state::CertifiedCommitStateTopologyParent::PublishedTopology,
                                )
                            })
                            .ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    "selected-source authority disappeared during commit staging",
                                )
                            })
                    })
                    .transpose()?;
                Some(
                    crate::tracked_state::stage_current_state_scoped_ranges_from_topology(
                        read,
                        writes,
                        &topology_parents,
                        selected_source,
                        record.commit_id,
                        &record.account_id,
                        &mutations,
                    )
                    .await?,
                )
            };
            let current_state_scoped_ranges = catalog_publication
                .as_ref()
                .and_then(|publication| publication.root());
            let touched_scope_filter = catalog_publication.as_ref().map_or_else(
                crate::tracked_state::incomplete_touched_scope_filter,
                |publication| publication.touched_scope_filter().clone(),
            );
            if mutations.replacement_generation.is_some() {
                mutations.parts.clear();
            }
            let manifest = CommitStateManifest {
                commit_id: record.commit_id,
                change_account_id: record.account_id.clone(),
                replay_debt: if rootless {
                    staged.replay_debt
                } else {
                    CommitStateReplayDebt::default()
                },
                mutations,
                touched_scope_filter,
                current_state_scoped_ranges,
                snapshot_root: snapshot_root.map(Box::new),
            };
            let staged_manifest = if let Some(publication) = catalog_publication.as_ref() {
                crate::tracked_state::stage_certified_commit_state_manifest_with_handle(
                    writes,
                    &manifest,
                    publication,
                )?
            } else {
                crate::tracked_state::stage_commit_state_manifest_with_handle(writes, &manifest)?
            };
            published_manifests.insert(record.commit_id, staged_manifest);
        }
        Ok(())
    })
}

/// Every current-protocol commit is a root fence.
fn tracked_root_fence_ids(tracked_roots: &[PendingTrackedRoot]) -> BTreeSet<CommitId> {
    tracked_roots.iter().map(|root| root.commit_id).collect()
}

fn tracked_state_rows_are_strictly_sorted(
    state_rows: &PreparedStateBatch,
    row_indices: &[RowIndex],
) -> bool {
    row_indices.windows(2).all(|pair| {
        let left = state_rows.row(pair[0]);
        let right = state_rows.row(pair[1]);
        left.schema_key
            .cmp(&right.schema_key)
            .then_with(|| left.file_id.cmp(&right.file_id))
            .then_with(|| left.row_pk.cmp(&right.row_pk))
            .is_lt()
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

struct OrderedStateRowMutations<'a> {
    row_indices: std::slice::Iter<'a, RowIndex>,
    state_rows: &'a PreparedStateBatch,
    insert_selection: &'a PreparedInsertSelection,
}

impl<'a> OrderedStateRowMutations<'a> {
    fn new(
        row_indices: &'a [RowIndex],
        state_rows: &'a PreparedStateBatch,
        insert_selection: &'a PreparedInsertSelection,
    ) -> Self {
        Self {
            row_indices: row_indices.iter(),
            state_rows,
            insert_selection,
        }
    }
}

impl<'a> Iterator for OrderedStateRowMutations<'a> {
    type Item = Result<TrackedStateRootMutationRef<'a>, LixError>;

    fn next(&mut self) -> Option<Self::Item> {
        let row_index = *self.row_indices.next()?;
        let row = self.state_rows.row(row_index);
        Some(
            tracked_delta_from_state_row(row).map(|delta| TrackedStateRootMutationRef {
                delta,
                require_absence: tracked_row_requires_absence(
                    row_index,
                    row,
                    self.insert_selection,
                ),
            }),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.row_indices.size_hint()
    }
}

impl ExactSizeIterator for OrderedStateRowMutations<'_> {}

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
        let branch_ref_change_id = change_refs.branch_ref_change_id;
        let selected_change_batches = change_refs.into_selected_change_batches();
        commit_rows.push(FinalizedCommitRow {
            commit_id,
            parent_commit_ids: vec![intermediate.parent_commit_id],
            created_at,
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

async fn validate_active_account_and_account_rows(
    read: &mut impl StorageAdapterRead,
    prepared_writes: &PreparedWriteSet,
    active_account_id: &str,
) -> Result<(), LixError> {
    let account_pk = RowPk::uuid_from_canonical(active_account_id).map_err(|_| {
        LixError::new(
            "LIX_INVALID_ACCOUNT_ID",
            format!("active account id '{active_account_id}' is not a canonical UUID"),
        )
    })?;
    // Proving the account is active is a hot-state point read plus a JSON
    // parse of its snapshot, and it re-proves the same fact on every commit.
    // The account token identifies the account view this snapshot sees: a
    // commit that could change that view rotates it, so an equal token means
    // an equal view and the proof still holds. A commit that writes account
    // rows itself always re-reads — it is the one changing the answer.
    let account_revision = crate::account::load_account_revision(&*read).await?;
    let writes_account_rows = prepared_writes
        .state_rows
        .iter()
        .any(|row| row.schema_key.as_str() == "lix_account");
    if !writes_account_rows
        && crate::account::account_proven_active(account_revision.as_ref(), active_account_id)
    {
        return validate_prepared_account_rows(prepared_writes);
    }
    let account = HotStateContext::new(
        TrackedStateContext::new(),
        crate::commit_graph::CommitGraphContext::new(),
    )
    .reader(&*read)
    .load_row(&HotStateRowRequest {
        schema_key: "lix_account".to_string(),
        branch_id: crate::GLOBAL_BRANCH_ID.to_string(),
        row_pk: account_pk,
        file_id: NullableKeyFilter::Null,
    })
    .await?;
    if let Some(account) = account {
        let account_snapshot = account.snapshot_content.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("account '{active_account_id}' has no snapshot"),
            )
        })?;
        let account_value: serde_json::Value =
            serde_json::from_str(&account_snapshot).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("account '{active_account_id}' has invalid JSON: {error}"),
                )
            })?;
        if account_value
            .get("status")
            .and_then(serde_json::Value::as_str)
            != Some("active")
        {
            return Err(LixError::new(
                "LIX_ACCOUNT_DISABLED",
                format!("active account '{active_account_id}' is disabled"),
            ));
        }
        crate::account::record_account_proven_active(account_revision.as_ref(), active_account_id);
    } else if crate::init::repository_protocol_status(&*read).await?
        == crate::init::RepositoryProtocolStatus::Current
    {
        return Err(LixError::new(
            "LIX_ACCOUNT_NOT_FOUND",
            format!("active account '{active_account_id}' does not exist"),
        ));
    }

    validate_prepared_account_rows(prepared_writes)
}

/// Shape rules for account rows carried by this commit. These read nothing and
/// run on every commit, cached account proof or not.
fn validate_prepared_account_rows(prepared_writes: &PreparedWriteSet) -> Result<(), LixError> {
    for row in prepared_writes
        .state_rows
        .iter()
        .filter(|row| row.schema_key.as_str() == "lix_account")
    {
        if row.branch_id.as_str() != crate::GLOBAL_BRANCH_ID || !row.global || row.file_id.is_some()
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "lix_account rows must be global",
            ));
        }
        let id = row.row_pk.as_single_string_owned()?;
        let expected_kind = if id == crate::SYSTEM_ACCOUNT_ID {
            Some("system")
        } else if id == crate::ANONYMOUS_ACCOUNT_ID {
            Some("anonymous")
        } else {
            None
        };
        let Some(expected_kind) = expected_kind else {
            continue;
        };
        let Some(snapshot) = row.snapshot else {
            continue;
        };
        let value: serde_json::Value =
            serde_json::from_str(snapshot.normalized()).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("built-in account '{id}' has invalid JSON: {error}"),
                )
            })?;
        if value.get("kind").and_then(serde_json::Value::as_str) != Some(expected_kind)
            || value.get("status").and_then(serde_json::Value::as_str) != Some("active")
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "built-in account '{id}' must retain kind '{expected_kind}' and active status"
                ),
            ));
        }
    }
    Ok(())
}

/// Permission grants are repository-wide facts. They are tracked on the
/// global branch so every working branch observes one authorization boundary
/// and permission edits never become ordinary branch merge inputs.
fn validate_prepared_permission_grant_rows(
    prepared_writes: &PreparedWriteSet,
) -> Result<(), LixError> {
    for row in prepared_writes
        .state_rows
        .iter()
        .filter(|row| row.schema_key.as_str() == PERMISSION_GRANT_SCHEMA_KEY)
    {
        if row.branch_id.as_str() != crate::GLOBAL_BRANCH_ID
            || !row.global
            || row.untracked
            || row.file_id.is_some()
        {
            return Err(invalid_permission_grant(
                "lix_permission_grant rows must be tracked global rows on the global branch",
            ));
        }

        let Some(snapshot) = row.snapshot else {
            continue;
        };
        let value: serde_json::Value =
            serde_json::from_str(snapshot.normalized()).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("lix_permission_grant row has invalid JSON: {error}"),
                )
            })?;
        validate_permission_grant_value(&value)?;
    }
    Ok(())
}

fn validate_permission_grant_value(value: &serde_json::Value) -> Result<(), LixError> {
    let principal_type = required_permission_grant_text(value, "principal_type")?;
    let principal_id = optional_permission_grant_text(value, "principal_id")?;
    match principal_type {
        "anonymous" if principal_id.is_some() => {
            return Err(invalid_permission_grant(
                "anonymous permission grants must not set principal_id",
            ));
        }
        "account" | "group" if principal_id.is_none_or(str::is_empty) => {
            return Err(invalid_permission_grant(format!(
                "{principal_type} permission grants require principal_id"
            )));
        }
        "account" => {
            RowPk::uuid_from_canonical(principal_id.expect("account id checked above")).map_err(
                |_| invalid_permission_grant("account principal_id must be a canonical UUID"),
            )?;
        }
        "group" | "anonymous" => {}
        _ => {
            return Err(invalid_permission_grant(format!(
                "unsupported permission principal_type '{principal_type}'"
            )));
        }
    }

    let access_level = required_permission_grant_text(value, "access_level")?;
    if !matches!(
        access_level,
        "viewer" | "commenter" | "contributor" | "editor" | "manager"
    ) {
        return Err(invalid_permission_grant(format!(
            "unsupported permission access_level '{access_level}'"
        )));
    }

    let resource_type = required_permission_grant_text(value, "resource_type")?;
    let directory_id = optional_permission_grant_text(value, "directory_id")?;
    let file_id = optional_permission_grant_text(value, "file_id")?;
    let schema_key = optional_permission_grant_text(value, "schema_key")?;
    let row_pk = value.get("row_pk").filter(|value| !value.is_null());

    let valid_shape = match resource_type {
        "repository" => {
            directory_id.is_none() && file_id.is_none() && schema_key.is_none() && row_pk.is_none()
        }
        "directory" => {
            directory_id.is_some() && file_id.is_none() && schema_key.is_none() && row_pk.is_none()
        }
        "file" => {
            directory_id.is_none() && file_id.is_some() && schema_key.is_none() && row_pk.is_none()
        }
        "table" => {
            directory_id.is_none()
                && file_id.is_some()
                && schema_key.is_some_and(|key| !key.is_empty())
                && row_pk.is_none()
        }
        "row" => {
            directory_id.is_none()
                && file_id.is_some()
                && schema_key.is_some_and(|key| !key.is_empty())
                && row_pk.is_some_and(|pk| {
                    pk.as_array().is_some_and(|components| !components.is_empty())
                })
        }
        _ => {
            return Err(invalid_permission_grant(format!(
                "unsupported permission resource_type '{resource_type}'"
            )));
        }
    };
    if !valid_shape {
        return Err(invalid_permission_grant(format!(
            "permission resource columns do not match resource_type '{resource_type}'"
        )));
    }
    Ok(())
}

fn required_permission_grant_text<'a>(
    value: &'a serde_json::Value,
    field: &str,
) -> Result<&'a str, LixError> {
    optional_permission_grant_text(value, field)?.ok_or_else(|| {
        invalid_permission_grant(format!("permission grant requires {field}"))
    })
}

fn optional_permission_grant_text<'a>(
    value: &'a serde_json::Value,
    field: &str,
) -> Result<Option<&'a str>, LixError> {
    match value.get(field) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(invalid_permission_grant(format!(
            "permission grant {field} must be text or null"
        ))),
    }
}

fn invalid_permission_grant(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

async fn validate_account_deletions(
    read: &mut impl StorageAdapterRead,
    prepared_writes: &PreparedWriteSet,
    active_account_id: &str,
) -> Result<(), LixError> {
    let deleted_accounts = prepared_writes
        .state_rows
        .iter()
        .filter(|row| row.schema_key.as_str() == "lix_account" && row.snapshot.is_none())
        .map(|row| row.row_pk.as_single_string_owned())
        .collect::<Result<BTreeSet<_>, _>>()?;
    if deleted_accounts.is_empty() {
        return Ok(());
    }
    if deleted_accounts.contains(crate::SYSTEM_ACCOUNT_ID)
        || deleted_accounts.contains(crate::ANONYMOUS_ACCOUNT_ID)
    {
        return Err(LixError::new(
            "LIX_FOREIGN_KEY_VIOLATION",
            "the built-in system and anonymous accounts cannot be deleted",
        ));
    }
    if deleted_accounts.contains(active_account_id) {
        return Err(account_has_changes_error(active_account_id));
    }

    for change in crate::tracked_state::scan_change_records_from_commit_deltas(&*read).await? {
        if deleted_accounts.contains(&change.account_id) {
            return Err(account_has_changes_error(&change.account_id));
        }
    }

    let mut reader = ChangelogContext::new().reader(&*read);
    let mut start_after = None::<String>;
    loop {
        let batch = reader
            .scan_changes(ChangeScanRequest {
                start_after: start_after.as_deref(),
                limit: Some(1_024),
            })
            .await?;
        for change in batch.entries {
            if deleted_accounts.contains(&change.account_id) {
                return Err(account_has_changes_error(&change.account_id));
            }
        }
        let Some(next) = batch.next_start_after else {
            break;
        };
        start_after = Some(next.to_string());
    }
    let mut start_after = None::<String>;
    loop {
        let batch = reader
            .scan_commits(CommitScanRequest {
                start_after: start_after.as_deref(),
                limit: Some(1_024),
            })
            .await?;
        for commit in batch.entries {
            if deleted_accounts.contains(&commit.account_id) {
                return Err(account_has_changes_error(&commit.account_id));
            }
        }
        let Some(next) = batch.next_start_after else {
            break;
        };
        start_after = Some(next.to_string());
    }
    Ok(())
}

fn account_has_changes_error(account_id: &str) -> LixError {
    LixError::new(
        "LIX_FOREIGN_KEY_VIOLATION",
        format!("account '{account_id}' cannot be deleted because changes are attributed to it"),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::branch::BranchContext;
    use crate::catalog::SchemaPlanId;
    use crate::changelog::ChangelogReader;
    use crate::hot_state::{
        HotStateContext, HotStateExactBatchRequest, HotStateExactRowRequest, HotStateProjection,
        HotStateRowRequest, PACKED_CURRENT_BASE_SPACE, ROW_SPACE,
    };
    use crate::storage::{
        BeginScanOptions, CommitResult, GetManyResult, KeyRange, PutBatch, ScanCursor, SpaceId,
        Storage, StorageError, StorageRead, StorageWrite,
    };
    use crate::storage_adapter::{
        Memory, MemoryRead, MemoryWrite, StorageAdapter, StorageAdapterReadScope, StorageKey,
        StorageReadOptions, StorageSpace, StorageWriteOptions,
    };
    use crate::transaction_types::{
        PreparedRowFacts, TestPreparedStateRow, TransactionFileContent,
    };
    use crate::{GLOBAL_BRANCH_ID, NullableKeyFilter};

    macro_rules! prepared_rows {
        ($($row:expr),* $(,)?) => {
            PreparedStateBatch::from_test_rows(vec![$($row),*])
        };
    }

    fn ts(value: &str) -> LixTimestamp {
        LixTimestamp::expect_parse("timestamp", value)
    }

    #[tokio::test]
    async fn branch_creation_owner_publishes_checkpoint_serving_context() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "01960000-0000-7000-8000-0000000000b1";
        let recovered_head = commit_id("branch-bridge-recovered-head");
        let checkpoint = commit_id("branch-bridge-checkpoint");
        let target = ExplicitBranchHeadTarget {
            head_commit_id: Some(recovered_head),
            ref_change_id: change_id("branch-bridge-ref-change"),
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
        };
        let bridge = crate::gc::CheckpointRecoveryRef {
            branch_id: branch_id.to_owned(),
            recovered_head_commit_id: recovered_head,
            checkpoint_commit_id: checkpoint,
            interval_has_commits: true,
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("branch publication read should open");
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        let mut root_backed = BTreeSet::new();
        let controls = stage_branch_head_control_publications(
            &read,
            &mut writes,
            &BTreeMap::new(),
            &PreparedStateBatch::new(),
            &[],
            &BTreeMap::from([(branch_id.to_owned(), target)]),
            &PreparedInsertSelection::new(),
            &[],
            &BTreeMap::from([(branch_id.to_owned(), bridge.clone())]),
            &mut preconditions,
            &BTreeMap::from([(
                branch_id.to_owned(),
                BranchHeadControlObservation {
                    control: None,
                    raw_token: None,
                },
            )]),
            &mut root_backed,
        )
        .await
        .expect("branch owner should stage checkpoint serving context");
        let control = controls
            .get(branch_id)
            .copied()
            .expect("created branch should have a complete control");
        assert_eq!(control.head_commit_id, recovered_head);
        assert_eq!(control.working_diff_checkpoint_commit_id, Some(checkpoint));
        assert!(root_backed.contains(branch_id));
        drop(read);
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("branch serving context should publish atomically");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("branch serving verification read should open");
        let persisted = BranchHeadControlContext::new()
            .reader(&read)
            .load(branch_id)
            .await
            .expect("created branch control should load")
            .expect("created branch control should exist");
        assert_eq!(persisted, control);
        let epoch = TrackedHeadContext::new()
            .reader(&read)
            .working_diff_epoch(branch_id)
            .await
            .expect("branch working-diff epoch should load")
            .expect("branch working-diff epoch should exist");
        assert_eq!(epoch.checkpoint_commit_id, checkpoint);
        assert_eq!(epoch.generation, control.tracked_generation);
        assert_eq!(
            crate::gc::resolve_checkpoint_branch_parent(
                &read,
                branch_id,
                recovered_head,
                control.working_diff_checkpoint_commit_id,
            )
            .await
            .expect("branch serving context should validate"),
            Some(checkpoint)
        );
    }

    #[tokio::test]
    async fn real_checkpoint_without_complete_hot_control_still_fails() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        let checkpoint = commit_id("missing-hot-checkpoint");
        let error = stage_checkpoint_working_diff_epochs(
            &mut writes,
            &[crate::gc::CheckpointPublication {
                recovery_ref: crate::gc::CheckpointRecoveryRef {
                    branch_id: "01960000-0000-7000-8000-0000000000b2".to_owned(),
                    recovered_head_commit_id: commit_id("missing-hot-recovered-head"),
                    checkpoint_commit_id: checkpoint,
                    interval_has_commits: true,
                },
                gc_state: crate::gc::CheckpointGcState::default(),
            }],
            &BTreeMap::new(),
        )
        .await
        .expect_err("real checkpoint publication without complete HOT control must fail");
        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(error.message.contains("has no complete hot control"));
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
        let identities = ["a", "b"].map(|row_pk| TrackedStateKey {
            schema_key: "test_schema".to_string(),
            file_id: None,
            row_pk: RowPk::single(row_pk),
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
                    row_pk: identity.row_pk,
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
    fn host_certified_ownership_change_preserves_old_plugin_tombstone() {
        let mut old_plugin_tombstone = tracked_branch_row("main", "old-plugin-delete");
        old_plugin_tombstone.row_pk = RowPk::single("old-plugin-line");
        old_plugin_tombstone.schema_key = "plugin_line".into();
        old_plugin_tombstone.file_id = Some("file-a".into());
        old_plugin_tombstone.snapshot = None;

        let mut new_plugin_live = tracked_branch_row("main", "new-plugin-create");
        new_plugin_live.row_pk = RowPk::single("new-plugin-line");
        new_plugin_live.schema_key = "plugin_line".into();
        new_plugin_live.file_id = Some("file-a".into());
        let published_commit_id = new_plugin_live
            .commit_id
            .expect("test live row should have a commit");

        let certified = BTreeMap::from([(
            "main".to_string(),
            BTreeMap::from([(
                "file-a".to_string(),
                BTreeSet::from(["plugin_line".to_string()]),
            )]),
        )]);

        assert!(
            !host_certified_batch_owns_live_row(
                old_plugin_tombstone.borrowed(),
                "main",
                published_commit_id,
                &certified,
            ),
            "the previous owner's tombstone must remain in HOT publication",
        );
        assert!(
            current_state_delta_from_state_row(old_plugin_tombstone.borrowed())
                .expect("ownership tombstone should lower")
                .deleted,
            "the retained row must decrement collection counts as a deletion",
        );
        assert!(
            host_certified_batch_owns_live_row(
                new_plugin_live.borrowed(),
                "main",
                published_commit_id,
                &certified,
            ),
            "the certified batch owns the replacement live row",
        );
    }

    #[test]
    fn host_certified_batch_does_not_own_intermediate_commit_rows() {
        let published_commit_id = commit_id("published-certified-batch");
        let mut published = tracked_branch_row("main", "published-live-row");
        published.commit_id = Some(published_commit_id);
        published.schema_key = "plugin_line".into();
        published.file_id = Some("file-a".into());

        let mut intermediate = published.clone();
        intermediate.commit_id = Some(commit_id("intermediate-write"));
        intermediate.change_id = Some(change_id("intermediate-live-row"));

        let certified = BTreeMap::from([(
            "main".to_string(),
            BTreeMap::from([(
                "file-a".to_string(),
                BTreeSet::from(["plugin_line".to_string()]),
            )]),
        )]);

        assert!(host_certified_batch_owns_live_row(
            published.borrowed(),
            "main",
            published_commit_id,
            &certified,
        ));
        assert!(
            !host_certified_batch_owns_live_row(
                intermediate.borrowed(),
                "main",
                published_commit_id,
                &certified,
            ),
            "an intermediate commit has no certified batch under its own commit id",
        );
    }

    #[test]
    fn host_dense_packets_reuse_ordinary_root_members() {
        let batch = |format| crate::plugin::runtime::WasmCertifiedRowBatch {
            format,
            schema_keys: vec!["test_schema".to_owned()],
            row_count: 1,
            creates: crate::plugin::runtime::WasmCreateContext { high: 0, low: 0 },
            create_ranges: Vec::new(),
            complete_file_state: true,
            pages: Vec::new(),
        };
        assert!(!certified_batch_requires_root_expansion(&batch(
            crate::plugin::runtime::HOST_CERTIFIED_PACKET_FORMAT
        )));
        assert!(!certified_batch_requires_root_expansion(&batch(
            crate::plugin::runtime::HOST_CERTIFIED_ZSTD_PACKET_FORMAT
        )));
        assert!(certified_batch_requires_root_expansion(&batch(1)));
        assert!(certified_batch_requires_root_expansion(&batch(2)));
    }

    #[test]
    fn lifecycle_file_delete_cascade_survives_descriptor_recreation() {
        let semantic_key = TrackedStateKey {
            schema_key: "semantic".to_string(),
            row_pk: RowPk::single("line-1"),
            file_id: Some("file-a".to_string()),
        };
        let semantic = MaterializedTrackedStateRow {
            row_pk: semantic_key.row_pk.clone(),
            schema_key: semantic_key.schema_key.clone(),
            file_id: semantic_key.file_id.clone(),
            snapshot_content: Some("{\"value\":1}".into()),
            metadata: Some("{\"source\":\"plugin\"}".into()),
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            change_id: ChangeId::for_test_label("semantic-create"),
            commit_id: CommitId::for_test_label("initial"),
        };
        let mut rows = BTreeMap::from([(semantic_key.clone(), semantic)]);
        let descriptor_delete = MaterializedTrackedStateRow {
            row_pk: RowPk::single("file-a"),
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
            file_id: Some("file-a".to_string()),
            snapshot_content: None,
            metadata: None,
            deleted: true,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-02T00:00:00Z".to_string(),
            change_id: ChangeId::for_test_label("descriptor-delete"),
            commit_id: CommitId::for_test_label("delete"),
        };
        apply_lifecycle_tracked_snapshot_row(&mut rows, descriptor_delete, false)
            .expect("descriptor delete should cascade");

        let mut descriptor_recreate = rows
            .values()
            .find(|row| row.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY)
            .expect("descriptor tombstone should exist")
            .clone();
        descriptor_recreate.snapshot_content = Some("{\"name\":\"a\"}".into());
        descriptor_recreate.deleted = false;
        descriptor_recreate.updated_at = "2026-01-03T00:00:00Z".to_string();
        descriptor_recreate.change_id = ChangeId::for_test_label("descriptor-recreate");
        descriptor_recreate.commit_id = CommitId::for_test_label("recreate");
        apply_lifecycle_tracked_snapshot_row(&mut rows, descriptor_recreate, false)
            .expect("descriptor recreation should apply");

        let semantic = rows
            .get(&semantic_key)
            .expect("semantic cascade tombstone should remain");
        assert!(semantic.deleted);
        assert_eq!(semantic.snapshot_content, None);
        assert_eq!(semantic.metadata, None);
        assert_eq!(
            semantic.change_id,
            ChangeId::for_test_label("descriptor-delete")
        );
    }

    const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
    const DETERMINISTIC_SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";
    // `tracked_state::storage` intentionally keeps this internal; this test
    // observes the durable space rather than reaching through that module.
    const TRACKED_STATE_TREE_CHUNK_SPACE_ID: SpaceId = SpaceId(0x0004_0001);
    const TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE_ID: SpaceId = SpaceId(0x0004_002b);
    const TRACKED_STATE_TREE_CHUNK_SPACE: StorageSpace = StorageSpace::mutable(
        TRACKED_STATE_TREE_CHUNK_SPACE_ID,
        "tracked_state.tree_chunk",
    );
    // Immutable, matching the canonical declaration in `tracked_state`. Only
    // the id is load-bearing here (`has_mutations_in_space` keys on it), but a
    // space id has exactly one value semantics, so restating it wrongly would
    // be the drift `storage_spaces` exists to prevent.
    const TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE: StorageSpace = StorageSpace::immutable(
        TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE_ID,
        "tracked_state.commit_state_manifest.v7",
    );
    // V11 has no tracked-head marker space. Keep the retired v10 ID here only
    // as a negative test sentinel: normal serving and staging must never read
    // it after the branch control became the publication authority.
    const V10_TRACKED_HEAD_MARKER_SPACE_ID: SpaceId = SpaceId(0x0004_0014);

    fn mixed_certified_guard_write_set() -> PreparedWriteSet {
        let branch_id = "01960000-0000-7000-8000-0000000000c1";
        let mut covered_insert = tracked_branch_row(branch_id, "covered-insert");
        covered_insert.schema_key = "covered_schema".into();
        covered_insert.file_id = Some("certified-file".into());
        covered_insert.row_pk = RowPk::single("covered");

        let mut uncovered_file_insert = tracked_branch_row(branch_id, "uncovered-file-insert");
        uncovered_file_insert.schema_key = "uncovered_schema".into();
        uncovered_file_insert.file_id = Some("other-file".into());
        uncovered_file_insert.row_pk = RowPk::single("uncovered-file");

        let mut uncovered_descriptor_insert =
            tracked_branch_row(branch_id, "uncovered-descriptor-insert");
        uncovered_descriptor_insert.schema_key = "lix_file_descriptor".into();
        uncovered_descriptor_insert.file_id = Some("certified-file".into());
        uncovered_descriptor_insert.row_pk = RowPk::single("certified-file");

        let rows = vec![
            covered_insert,
            uncovered_file_insert,
            uncovered_descriptor_insert,
        ];
        let mut writes = PreparedWriteSet {
            insert_selection: PreparedInsertSelection::new(),
            state_rows: PreparedStateBatch::from_test_rows(rows.clone()),
            commit_change_refs_by_branch: BTreeMap::new(),
            first_commit_parent_override_by_branch: BTreeMap::new(),
            checkpoint_publications: Vec::new(),
            extra_commit_parents_by_branch: BTreeMap::new(),
            intermediate_commits: Vec::new(),
            file_content_writes: Vec::new(),
        };
        for row in &rows {
            writes.remember_insert_identity_for_tests(row);
        }
        writes
    }

    #[test]
    fn certified_fresh_file_rows_are_excluded_from_hot_absence_guards() {
        let writes = mixed_certified_guard_write_set();
        let guards = owned_absence_guards(&tracked_head_absence_guards(
            &writes.state_rows,
            &writes.insert_selection,
            "01960000-0000-7000-8000-0000000000c1",
            Some("certified-file"),
        ));

        assert_eq!(
            guards,
            BTreeSet::from([TrackedStateKey {
                schema_key: "uncovered_schema".to_string(),
                file_id: Some("other-file".to_string()),
                row_pk: RowPk::single("uncovered-file"),
            }]),
            "only INSERT identities outside the certified file scope still need row-owned guards"
        );
    }

    #[test]
    fn generic_and_mixed_batches_retain_uncovered_hot_absence_guards() {
        let writes = mixed_certified_guard_write_set();
        let guards = owned_absence_guards(&tracked_head_absence_guards(
            &writes.state_rows,
            &writes.insert_selection,
            "01960000-0000-7000-8000-0000000000c1",
            None,
        ));

        assert_eq!(
            guards.len(),
            3,
            "without a fresh-file certificate every INSERT identity stays guarded"
        );
        assert!(guards.contains(&TrackedStateKey {
            schema_key: "covered_schema".to_string(),
            file_id: Some("certified-file".to_string()),
            row_pk: RowPk::single("covered"),
        }));
        assert!(guards.contains(&TrackedStateKey {
            schema_key: "uncovered_schema".to_string(),
            file_id: Some("other-file".to_string()),
            row_pk: RowPk::single("uncovered-file"),
        }));
        assert!(guards.contains(&TrackedStateKey {
            schema_key: "lix_file_descriptor".to_string(),
            file_id: Some("certified-file".to_string()),
            row_pk: RowPk::single("certified-file"),
        }));
    }

    #[test]
    fn packed_route_rejects_equal_count_guards_for_other_identities() {
        let tracked_pk = RowPk::single("tracked-update");
        let untracked_pk = RowPk::single("untracked-insert");
        let timestamp = ts("2026-01-01T00:00:00Z");
        let delta = crate::hot_state::CurrentStateDeltaRef {
            schema_key: "tracked_schema",
            file_id: None,
            row_pk: &tracked_pk,
            change_id: Some(change_id("tracked-update")),
            commit_id: Some(commit_id("tracked-update")),
            untracked: false,
            deleted: false,
            created_at: timestamp,
            updated_at: timestamp,
            snapshot: crate::json_store::JsonSlotRef::Inline(r#"{"value":1}"#),
            metadata: crate::json_store::JsonSlotRef::None,
            columnar_base_coordinate: None,
        };
        let guard = TrackedStateKeyRef {
            schema_key: "untracked_schema",
            file_id: None,
            row_pk: &untracked_pk,
        };

        assert!(
            !packed_current_base_guards_match(&[delta], &[guard]),
            "equal counts from tracked updates and unrelated untracked inserts must fall back"
        );
    }

    fn hot_state_context() -> HotStateContext {
        HotStateContext::new(
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
                if space == ROW_SPACE || space == crate::hot_state::FILE_SPACE {
                    self.counts
                        .row_get_many_calls
                        .fetch_add(1, Ordering::Relaxed);
                }
                if space.id == TRACKED_STATE_TREE_CHUNK_SPACE_ID {
                    self.counts
                        .tree_chunk_get_many_calls
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

        async fn begin_scan(
            &self,
            space: StorageSpace,
            range: KeyRange,
            opts: BeginScanOptions,
        ) -> Result<ScanCursor<'_>, StorageError> {
            if space == ROW_SPACE || space == crate::hot_state::FILE_SPACE {
                self.counts.row_scan_calls.fetch_add(1, Ordering::Relaxed);
            }
            if space.id == TRACKED_STATE_TREE_CHUNK_SPACE_ID {
                self.counts
                    .tree_chunk_scan_calls
                    .fetch_add(1, Ordering::Relaxed);
            }
            if space.id == TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE_ID {
                self.counts
                    .commit_root_scan_calls
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.inner.begin_scan(space, range, opts).await
        }
    }

    #[test]
    fn selected_change_refs_reject_overlap_with_normal_rows() {
        let row = tracked_global_row("normal-change");
        let error = validate_selected_change_refs(
            commit_id("test-uuid-1"),
            &prepared_rows![row],
            &[0],
            &[selected_change_batch("selected-change", "row-1")],
        )
        .expect_err("selected ref must not duplicate a normal row identity");
        assert!(error.message.contains("duplicate change ref key"));

        let row = tracked_global_row("normal-change");
        validate_selected_change_refs(
            commit_id("test-uuid-1"),
            &prepared_rows![row],
            &[0],
            &[selected_change_batch("normal-change", "other-row")],
        )
        .expect("different semantic identities may share one source change id");
    }

    #[tokio::test]
    async fn ordered_commit_delta_keeps_non_overlapping_certified_packet_rows() {
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
            RowPk::single("ordinary"),
            "ordinary_schema".into(),
            None,
            Some(crate::transaction_types::stage_json_from_value(
                crate::transaction_types::TransactionJson::from_value_for_test(
                    serde_json::from_str(large_snapshot.as_str())
                        .expect("large ordinary snapshot should parse"),
                ),
            )),
            None,
            None,
            None,
            timestamp,
            timestamp,
            true,
            Some(ChangeId::default()),
            true,
            Some(commit_id),
            false,
            GLOBAL_BRANCH_ID.into(),
        );
        let certified_change_id = change_id("mixed-certified-change");
        let certified_rows = BTreeMap::from([(
            commit_id,
            vec![MaterializedHotStateRow {
                row_pk: RowPk::single("certified"),
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
        let mut row_columnar_write_sets = crate::hot_state::RowColumnarWriteSets::new();
        let staged_index = stage_tracked_commit_delta_index(
            &read,
            &mut writes,
            &mut state_rows,
            &mut row_columnar_write_sets,
            &BTreeMap::from([(commit_id, vec![0])]),
            &roots,
            &commits,
            &HashMap::new(),
            &certified_rows,
            &certified_json_refs,
            &PreparedInsertSelection::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeSet::new(),
        )
        .await
        .expect("mixed certified delta should stage");
        stage_commit_state_manifest(
            &mut writes,
            &CommitStateManifest {
                commit_id,
                change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                replay_debt: CommitStateReplayDebt {
                    depth: 1,
                    rows: 2,
                    bytes: 2,
                },
                mutations: staged_index
                    .inventories
                    .get(&commit_id)
                    .cloned()
                    .expect("mixed certified inventory should stage"),
                touched_scope_filter: Default::default(),
                current_state_scoped_ranges: None,
                snapshot_root: None,
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
        let mut json_reader = JsonStoreContext::new().reader(&read);
        let loaded = json_reader
            .load_bytes_many(crate::json_store::JsonLoadRequestRef {
                refs: &[large_snapshot_ref],
                scope: crate::json_store::JsonReadScopeRef::OutOfBand,
            })
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
            writes.has_mutations_in_space(TRACKED_STATE_TREE_CHUNK_SPACE),
            "an unaddressed tracked commit still requires immutable tree chunks"
        );
        assert!(
            writes.has_mutations_in_space(TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE),
            "an unaddressed tracked commit still requires commit-state authority"
        );
        assert!(
            writes.has_mutations_in_space(ROW_SPACE),
            "a small ordinary tracked commit must retain point-addressable HOT state"
        );
        assert!(
            !writes.has_mutations_in_space(PACKED_CURRENT_BASE_SPACE),
            "a small ordinary tracked commit must not accumulate a packed manifest"
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
        assert_eq!(
            record.change_id(),
            commit_id("test-uuid-1").commit_change_id()
        );
        let membership_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("membership read should open");
        let physical_state =
            crate::tracked_state::load_commit_state_manifest(&membership_read, record.commit_id)
                .await
                .expect("physical state should load")
                .expect("physical state should exist");
        assert_eq!(physical_state.replay_debt, CommitStateReplayDebt::default());
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
        let change_ids = [change_id("change-1"), record.change_id()];
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
        let derived_commit_rows = hot_state_context()
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_batch(&crate::hot_state::HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
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
                .any(|row| row.change_id == Some(record.change_id())),
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
        branch_ref_delete.row_pk = RowPk::single(branch_id);
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
        branch_ref_delete.row_pk = RowPk::single(branch_id);
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
        cleanup_branch_ref_delete.row_pk = RowPk::single(branch_id);
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
    async fn rootless_branch_delete_reclaims_its_checkpoint_prefix_and_survives_reopen() {
        let backend = Memory::new();
        crate::engine::Engine::initialize(backend.clone())
            .await
            .expect("rootless checkpoint repository should initialize");
        let storage = StorageAdapter::new(backend.clone());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let branch_id = "01960000-0000-7000-8000-000000000004";
        let file_id = "01960000-0000-7000-8000-000000000005";
        let generation = crate::binary_cas::BlobId::from_content(b"rootless-generation");
        let semantic_root = "01960000-0000-7000-8000-000000000006";
        let blob_hash = crate::binary_cas::BlobId::from_content(b"rootless-file");

        // Model a checkpoint staged before this branch ever acquires a
        // tracked root. The derived row is not itself lifecycle authority.
        let mut checkpoint_writes = storage.new_write_set();
        crate::transaction::plugin_checkpoint::stage_current_plugin_checkpoint(
            &mut checkpoint_writes,
            branch_id,
            file_id,
            &generation.to_hex(),
            semantic_root,
            blob_hash,
            b"runtime",
            b"authority",
        )
        .expect("rootless checkpoint should stage");
        storage
            .commit_write_set(checkpoint_writes, StorageWriteOptions::default())
            .await
            .expect("rootless checkpoint should persist");

        let mut branch_ref_delete = untracked_global_row("delete-rootless-branch-ref");
        branch_ref_delete.row_pk = RowPk::single(branch_id);
        branch_ref_delete.schema_key = BRANCH_REF_SCHEMA_KEY.into();
        branch_ref_delete.snapshot = None;
        let mut delete_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("rootless branch delete read should open");
        let (delete_writes, delete_preconditions) = commit_prepared_writes(
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
        .expect("rootless branch deletion should publish a lifecycle signal");
        drop(delete_read);
        storage
            .commit_write_set(
                delete_writes,
                StorageWriteOptions {
                    preconditions: delete_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("rootless branch deletion should commit");

        let retained_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("retained checkpoint read should open");
        assert!(
            crate::transaction::plugin_checkpoint::load_current_plugin_checkpoint(
                &retained_read,
                branch_id,
                file_id,
                &generation.to_hex(),
                semantic_root,
                blob_hash,
            )
            .await
            .expect("retired rootless checkpoint should load")
            .is_none(),
            "branch deletion reclaims its own checkpoint prefix in the same write set"
        );
        drop(retained_read);

        let gc_read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("rootless GC read should open"),
        );
        let mut gc_writes = storage.new_write_set();
        let mut gc_preconditions = Vec::new();
        crate::gc::stage_repository_gc_with_preconditions(
            gc_read,
            &mut gc_writes,
            &mut gc_preconditions,
        )
        .await
        .expect("authenticated rootless GC should stage");
        storage
            .commit_write_set(
                gc_writes,
                StorageWriteOptions {
                    preconditions: gc_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("authenticated rootless GC should commit");

        let reopened = crate::engine::Engine::new(backend.clone())
            .await
            .expect("repository should reopen after rootless GC");
        let session = reopened
            .open_session()
            .await
            .expect("repository should reopen after rootless GC");
        let main = session
            .execute("SELECT id FROM lix_branch WHERE name = 'main'", &[])
            .await
            .expect("live main branch should survive rootless GC");
        assert_eq!(main.rows().len(), 1);

        let reopened_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("post-reopen checkpoint read should open");
        assert!(
            crate::transaction::plugin_checkpoint::load_current_plugin_checkpoint(
                &reopened_read,
                branch_id,
                file_id,
                &generation.to_hex(),
                semantic_root,
                blob_hash,
            )
            .await
            .expect("post-GC rootless checkpoint lookup should succeed")
            .is_none(),
            "authenticated GC must reclaim the rootless branch checkpoint prefix"
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
                change_refs_with([row_change], target_commit, "same-write-global-ref-change"),
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
    async fn ordinary_deduplicated_publication_first_rejects_stale_cas_gc() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let payload = b"ordinary-deduplicated-publication-race";
        seed_orphan_cas_payload(&storage, payload).await;
        let sweep_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("ordinary stale sweep read should open");
        let mut publication_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("ordinary publication read should open");
        let (sweep, sweep_preconditions) = stage_low_level_cas_sweep(&storage, &sweep_read).await;
        let (publication, publication_preconditions) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut publication_read,
            prepared_normal_file_commit(
                payload,
                "ordinary-publish-row",
                "ordinary-publish-commit",
                "ordinary-publish-branch-change",
            ),
        )
        .await
        .expect("fully deduplicated ordinary file publication should stage");
        drop(sweep_read);
        drop(publication_read);

        storage
            .commit_write_set(
                publication,
                StorageWriteOptions {
                    preconditions: publication_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("ordinary publication should win the CAS epoch");
        let error = storage
            .commit_write_set(
                sweep,
                StorageWriteOptions {
                    preconditions: sweep_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect_err("stale GC must lose after ordinary CAS publication");
        assert!(matches!(
            error,
            crate::storage_adapter::StorageWriteSetError::Storage(
                StorageError::PreconditionFailed(_)
            )
        ));
        assert_cas_payload(&storage, payload, true).await;
    }

    #[tokio::test]
    async fn cas_gc_first_rejects_stale_ordinary_publication_and_retry_restages() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let payload = b"ordinary-deduplicated-publication-race";
        seed_orphan_cas_payload(&storage, payload).await;
        let sweep_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("ordinary winning sweep read should open");
        let mut stale_publication_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("ordinary stale publication read should open");
        let (sweep, sweep_preconditions) = stage_low_level_cas_sweep(&storage, &sweep_read).await;
        let (stale_publication, stale_preconditions) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut stale_publication_read,
            prepared_normal_file_commit(
                payload,
                "ordinary-stale-row",
                "ordinary-stale-commit",
                "ordinary-stale-branch-change",
            ),
        )
        .await
        .expect("stale fully deduplicated ordinary publication should stage");
        drop(sweep_read);
        drop(stale_publication_read);

        storage
            .commit_write_set(
                sweep,
                StorageWriteOptions {
                    preconditions: sweep_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("GC should win the CAS epoch");
        assert_cas_payload(&storage, payload, false).await;
        let error = storage
            .commit_write_set(
                stale_publication,
                StorageWriteOptions {
                    preconditions: stale_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect_err("stale ordinary publication must lose after GC");
        assert!(matches!(
            error,
            crate::storage_adapter::StorageWriteSetError::Storage(
                StorageError::PreconditionFailed(_)
            )
        ));

        let mut retry_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("ordinary publication retry read should open");
        let (retry, retry_preconditions) = commit_prepared_writes(
            &binary_cas,
            &branch_ctx,
            None,
            &mut retry_read,
            prepared_normal_file_commit(
                payload,
                "ordinary-retry-row",
                "ordinary-retry-commit",
                "ordinary-retry-branch-change",
            ),
        )
        .await
        .expect("fresh ordinary publication retry should restage CAS payload");
        drop(retry_read);
        storage
            .commit_write_set(
                retry,
                StorageWriteOptions {
                    preconditions: retry_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("fresh ordinary publication retry should commit");
        assert_cas_payload(&storage, payload, true).await;
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
            writes.has_mutations_in_space(TRACKED_STATE_TREE_CHUNK_SPACE)
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
        second.snapshot = Some(crate::transaction_types::stage_json_from_value(
            crate::transaction_types::TransactionJson::from_value_for_test(
                serde_json::json!({ "value": 2 }),
            ),
        ));
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
            writes.has_mutations_in_space(TRACKED_STATE_TREE_CHUNK_SPACE)
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
        third.snapshot = Some(crate::transaction_types::stage_json_from_value(
            crate::transaction_types::TransactionJson::from_value_for_test(
                serde_json::json!({ "value": 3 }),
            ),
        ));
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
            writes.has_mutations_in_space(TRACKED_STATE_TREE_CHUNK_SPACE)
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
            writes.has_mutations_in_space(TRACKED_STATE_TREE_CHUNK_SPACE)
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

        let mut fence_refs = change_refs_with([], "fence-commit", "fence-branch-ref-change");
        fence_refs.add_selected_change_batch(selected_change_batch_from(
            "fence-normal-change",
            "row-1",
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
            writes.has_mutations_in_space(TRACKED_STATE_TREE_CHUNK_SPACE),
            "an ordinary tracked commit must write immutable tree chunks"
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
        let scanned = hot_state_context()
            .reader(StorageAdapterReadScope::new(CountingTrackedHeadRead {
                inner: memory
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("counted scan read should open"),
                counts: Arc::clone(&counts),
            }))
            .scan_batch(&crate::hot_state::HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
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
            "a current head projection must not first look up commit-state authority"
        );
        assert_eq!(
            counts.commit_root_scan_calls.load(Ordering::Relaxed),
            0,
            "a current head projection must not scan commit-state authority"
        );

        let loaded = hot_state_context()
            .reader(StorageAdapterReadScope::new(CountingTrackedHeadRead {
                inner: memory
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("counted point read should open"),
                counts: Arc::clone(&counts),
            }))
            .load_row(&hot_state_request())
            .await
            .expect("tracked point read should succeed")
            .expect("tracked row should be visible");
        assert_eq!(loaded.change_id, Some(change_id("tracked-head-change")));
        assert!(
            counts.row_get_many_calls.load(Ordering::Relaxed) > 0,
            "the exact tracked lookup must point-read the head projection"
        );

        let exact_rows = hot_state_context()
            .reader(StorageAdapterReadScope::new(CountingTrackedHeadRead {
                inner: memory
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("counted exact batch read should open"),
                counts: Arc::clone(&counts),
            }))
            .load_exact_batch(&HotStateExactBatchRequest {
                rows: vec![HotStateExactRowRequest {
                    schema_key: "test_schema".to_string(),
                    branch_id: GLOBAL_BRANCH_ID.to_string(),
                    row_pk: RowPk::single("row-1"),
                    file_id: None,
                }],
                projection: HotStateProjection::default(),
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
            "neither serving read may look up commit-state authority"
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
            3,
            "serial tracked-head staging reads the local control for parent/publication plus the global account fence"
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

        let mut first =
            tracked_branch_row("01920000-0000-7000-8000-0000000000a1", "epoch-first-change");
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![first],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "01920000-0000-7000-8000-0000000000a1".to_string(),
                    change_refs_with(
                        ["epoch-first-change"],
                        "epoch-first-commit",
                        "epoch-first-branch-ref-change",
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
            "01920000-0000-7000-8000-0000000000a1",
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
        let mut second = tracked_branch_row(
            "01920000-0000-7000-8000-0000000000a1",
            "epoch-second-change",
        );
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
                insert_selection: PreparedInsertSelection::new(),
                state_rows: prepared_rows![second],
                commit_change_refs_by_branch: BTreeMap::from([(
                    "01920000-0000-7000-8000-0000000000a1".to_string(),
                    change_refs_with(
                        ["epoch-second-change"],
                        "epoch-second-commit",
                        "epoch-second-branch-ref-change",
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
            .load("01920000-0000-7000-8000-0000000000a1")
            .await
            .expect("load branch control")
            .expect("branch control must exist");
        assert_eq!(control.head_commit_id, second_commit);
        assert!(
            TrackedHeadContext::new()
                .reader(read)
                .working_diff_for_control(
                    "01920000-0000-7000-8000-0000000000a1",
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
        global_fallback.row_pk = RowPk::single("row-2");
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
        let scanned = hot_state_context()
            .reader(StorageAdapterReadScope::new(CountingTrackedHeadRead {
                inner: memory
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("counted branch scan read should open"),
                counts: Arc::clone(&counts),
            }))
            .scan_batch(&crate::hot_state::HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
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
            .find(|row| row.row_pk == RowPk::single("row-1"))
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
            .find(|row| row.row_pk == RowPk::single("row-2"))
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
        branch_tombstone.row_pk = RowPk::single("row-2");
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

        let scanned = hot_state_context()
            .reader(StorageAdapterReadScope::new(CountingTrackedHeadRead {
                inner: memory
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("counted tombstone scan read should open"),
                counts: Arc::clone(&counts),
            }))
            .scan_batch(&crate::hot_state::HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
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
        assert_eq!(scanned[0].row_pk, RowPk::single("row-1"));
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
            "current global and branch projections must not look up commit-state authority"
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
                selected_change_batches: Vec::new(),
            },
            FinalizedCommitRow {
                commit_id: CommitId::for_test_label("parent-commit"),
                parent_commit_ids: Vec::new(),
                created_at: ts("2026-01-01T00:00:00Z"),
                selected_change_batches: Vec::new(),
            },
        ];
        let mut rootless_commit_ids = BTreeSet::from([CommitId::for_test_label("parent-commit")]);
        let mut durable_root_rebuild_parents = BTreeSet::new();
        let mut staged_root_rebuild_commits = BTreeSet::new();
        let mut external_parent_manifests = BTreeMap::new();
        let staged = stage_changelog_commits(
            &mut read,
            &mut writes,
            &prepared_rows![parent_row, child_row],
            &[],
            &[],
            &[],
            &mut rootless_commit_ids,
            &BTreeSet::new(),
            &mut durable_root_rebuild_parents,
            &mut staged_root_rebuild_commits,
            &BTreeMap::from([
                (CommitId::for_test_label("parent-commit"), vec![0]),
                (CommitId::for_test_label("child-commit"), vec![1]),
            ]),
            &commits,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &mut external_parent_manifests,
            crate::ANONYMOUS_ACCOUNT_ID,
        )
        .await
        .expect("child-before-parent input should still stage parent first");
        let mutation_inventories = commits
            .iter()
            .map(|commit| (commit.commit_id, CommitStateMutationInventory::default()))
            .collect::<BTreeMap<_, _>>();
        stage_commit_state_manifests(
            &read,
            &mut writes,
            &commits,
            &mutation_inventories,
            &rootless_commit_ids,
            &staged,
            &BTreeMap::new(),
            &external_parent_manifests,
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
        let authority_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("physical authority read should open");
        let states =
            crate::tracked_state::load_commit_state_manifests(&authority_read, &commit_ids)
                .await
                .expect("physical states should load")
                .into_iter()
                .map(|state| state.expect("physical state"))
                .collect::<Vec<_>>();
        assert_eq!(
            states
                .iter()
                .map(|state| state.replay_debt.depth)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(states[0].replay_debt.bytes > 0);
        assert!(states[1].replay_debt.bytes > states[0].replay_debt.bytes);
        assert_eq!(
            states
                .iter()
                .map(|state| state.replay_debt.rows)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[tokio::test]
    async fn staged_chain_closes_the_complete_rootless_interval_at_the_depth_fence() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = StorageWriteSet::new();
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let commit_count = usize::from(crate::tracked_state::COMMIT_STATE_MAX_REPLAY_DEPTH) + 1;
        let commit_ids = (0..commit_count)
            .map(|index| CommitId::for_test_label(&format!("staged-fence-commit-{index}")))
            .collect::<Vec<_>>();
        let state_rows = PreparedStateBatch::from_test_rows(
            commit_ids
                .iter()
                .enumerate()
                .map(|(index, commit_id)| {
                    let mut row = tracked_global_row(&format!("staged-fence-change-{index}"));
                    row.commit_id = Some(*commit_id);
                    row
                })
                .collect(),
        );
        let commit_rows = commit_ids
            .iter()
            .enumerate()
            .map(|(index, commit_id)| FinalizedCommitRow {
                commit_id: *commit_id,
                parent_commit_ids: index
                    .checked_sub(1)
                    .map(|parent| vec![commit_ids[parent]])
                    .unwrap_or_default(),
                created_at: ts("2026-01-01T00:00:00Z"),
                selected_change_batches: Vec::new(),
            })
            .collect::<Vec<_>>();
        let row_indices = commit_ids
            .iter()
            .enumerate()
            .map(|(index, commit_id)| (*commit_id, vec![index]))
            .collect::<BTreeMap<_, _>>();
        let mut rootless_commit_ids = BTreeSet::from([commit_ids[0]]);
        let mut durable_root_rebuild_parents = BTreeSet::new();
        let mut staged_root_rebuild_commits = BTreeSet::new();
        let mut external_parent_manifests = BTreeMap::new();

        let staged = stage_changelog_commits(
            &mut read,
            &mut writes,
            &state_rows,
            &[],
            &[],
            &[],
            &mut rootless_commit_ids,
            &BTreeSet::new(),
            &mut durable_root_rebuild_parents,
            &mut staged_root_rebuild_commits,
            &row_indices,
            &commit_rows,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &mut external_parent_manifests,
            crate::ANONYMOUS_ACCOUNT_ID,
        )
        .await
        .expect("depth fence should close a fully staged interval");

        assert!(rootless_commit_ids.is_empty());
        assert!(durable_root_rebuild_parents.is_empty());
        assert_eq!(staged_root_rebuild_commits.len(), commit_count - 1);
        assert!(
            staged
                .values()
                .all(|commit| commit.replay_debt == Default::default())
        );
    }

    #[tokio::test]
    async fn commit_with_only_untracked_writes_does_not_create_lix_commit() {
        let storage = StorageAdapter::new(Memory::new());
        let binary_cas = BinaryCasContext::new();
        let branch_ctx = BranchContext::new();
        let hot_state = hot_state_context();
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

        let loaded = hot_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&hot_state_request())
            .await
            .expect("current row load should succeed")
            .expect("untracked row should be persisted in live state");
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );
        // Identity without history, asserted in one place: the row keeps the
        // exact change id it was staged with, and the changelog lookup for that
        // same id immediately below finds nothing.
        assert_eq!(loaded.change_id, Some(change_id("change-untracked")));

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
        let hot_state = Arc::new(hot_state_context());
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
            FunctionContext::prepare(&read, None)
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
        untracked_row.row_pk = RowPk::single("row-2");

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
        assert_eq!(
            commit.change_id(),
            commit_id("test-uuid-1").commit_change_id()
        );
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

        let untracked = hot_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&HotStateRowRequest {
                schema_key: "test_schema".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                row_pk: RowPk::single("row-2"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("untracked row load should succeed")
            .expect("untracked row should persist in live state");
        assert_eq!(
            untracked.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );
        // The untracked row in this mixed batch keeps its own staged id while
        // the tracked row alongside it takes a commit-delta address.
        assert_eq!(untracked.change_id, Some(change_id("change-untracked")));

        let sequence_row = hot_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&HotStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                row_pk: RowPk::single(DETERMINISTIC_SEQUENCE_KEY),
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
        assert_eq!(
            commit.change_id(),
            commit_id("test-uuid-1").commit_change_id()
        );
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
        TrackedStateContext::new()
            .writer(&read, &mut writes)
            .stage_commit_root(label, None, std::iter::empty())
            .await
            .expect("empty commit target root should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("empty commit target should persist");
    }

    fn prepared_normal_global_commit(
        row_change_label: &str,
        commit_label: &str,
        branch_ref_change_label: &str,
    ) -> PreparedWriteSet {
        PreparedWriteSet {
            insert_selection: PreparedInsertSelection::new(),
            state_rows: prepared_rows![tracked_global_row(row_change_label)],
            commit_change_refs_by_branch: BTreeMap::from([(
                GLOBAL_BRANCH_ID.to_string(),
                change_refs_with([row_change_label], commit_label, branch_ref_change_label),
            )]),
            first_commit_parent_override_by_branch: BTreeMap::new(),
            checkpoint_publications: Vec::new(),
            extra_commit_parents_by_branch: BTreeMap::new(),
            intermediate_commits: Vec::new(),
            file_content_writes: Vec::new(),
        }
    }

    fn prepared_normal_file_commit(
        payload: &[u8],
        row_change_label: &str,
        commit_label: &str,
        branch_ref_change_label: &str,
    ) -> PreparedWriteSet {
        let file_id = "01960000-0000-7000-8000-00000000ca55";
        let blob_id = crate::binary_cas::BlobId::from_content(payload);
        let mut row = tracked_global_row(row_change_label);
        row.row_pk = RowPk::single(file_id);
        row.schema_key = "lix_binary_blob_ref".into();
        row.file_id = Some(file_id.into());
        row.snapshot = Some(crate::transaction_types::stage_json_from_value(
            crate::transaction_types::TransactionJson::from_value_for_test(serde_json::json!({
                "id": file_id,
                "blob_hash": blob_id.to_hex(),
                "size_bytes": payload.len(),
            })),
        ));
        PreparedWriteSet {
            insert_selection: PreparedInsertSelection::new(),
            state_rows: prepared_rows![row],
            commit_change_refs_by_branch: BTreeMap::from([(
                GLOBAL_BRANCH_ID.to_string(),
                change_refs_with([row_change_label], commit_label, branch_ref_change_label),
            )]),
            first_commit_parent_override_by_branch: BTreeMap::new(),
            checkpoint_publications: Vec::new(),
            extra_commit_parents_by_branch: BTreeMap::new(),
            intermediate_commits: Vec::new(),
            file_content_writes: vec![TransactionFileContent::new(
                file_id.into(),
                Some("/ordinary-epoch.bin".into()),
                Some("ordinary-epoch.bin".into()),
                GLOBAL_BRANCH_ID.into(),
                true,
                false,
                payload.to_vec(),
            )],
        }
    }

    async fn seed_orphan_cas_payload(storage: &StorageAdapter, payload: &[u8]) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("orphan CAS seed read should open");
        let mut writes = storage.new_write_set();
        BinaryCasContext::new()
            .writer_skipping_existing_chunks(&read, &mut writes)
            .stage_payload(&crate::binary_cas::BlobPayload::from_bytes(
                payload.to_vec(),
            ))
            .await
            .expect("orphan CAS payload should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("orphan CAS payload should commit");
    }

    async fn stage_low_level_cas_sweep(
        storage: &StorageAdapter,
        read: &impl StorageAdapterRead,
    ) -> (StorageWriteSet, Vec<StoragePrecondition>) {
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        let swept = crate::binary_cas::stage_gc_reclamation(
            read,
            &mut writes,
            &BTreeSet::new(),
            &BTreeMap::new(),
        )
        .await
        .expect("ordinary race CAS sweep should stage");
        assert_eq!(swept.reclaimed_chunk_rows, 1);
        crate::binary_cas::stage_cas_reclamation_fence(read, &mut writes, &mut preconditions)
            .await
            .expect("ordinary race sweep fence should stage");
        (writes, preconditions)
    }

    async fn assert_cas_payload(storage: &StorageAdapter, payload: &[u8], expected: bool) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("cold CAS verification read should open");
        let mut reader = BinaryCasContext::new().reader(read);
        let loaded = reader
            .load_bytes_many(&[crate::binary_cas::BlobId::from_content(payload)])
            .await
            .expect("cold CAS verification should authenticate");
        assert_eq!(loaded.into_vec()[0].as_deref(), expected.then_some(payload));
    }

    fn change_refs<const N: usize>(change_ids: [&str; N]) -> StagedCommitChangeRefs {
        change_refs_with(change_ids, "test-uuid-1", "test-uuid-3")
    }

    fn change_refs_with<const N: usize>(
        change_ids: [&str; N],
        commit_id_label: &str,
        branch_ref_change_id_label: &str,
    ) -> StagedCommitChangeRefs {
        let mut change_refs = StagedCommitChangeRefs::new(
            commit_id(commit_id_label),
            change_id(branch_ref_change_id_label),
            ts("2026-01-01T00:00:00.001Z"),
        );
        for change_id in change_ids {
            change_refs.add_change_id(self::change_id(change_id));
        }
        change_refs
    }

    fn selected_change_batch(change_id: &str, row_pk: &str) -> StagedCommitChangeBatch {
        selected_change_batch_from(change_id, row_pk, "selected-source")
    }

    fn selected_change_batch_from(
        change_id: &str,
        row_pk: &str,
        source_commit_id: &str,
    ) -> StagedCommitChangeBatch {
        let identity = crate::tracked_state::TrackedStateDiffIdentity::from_key(TrackedStateKey {
            schema_key: "test_schema".to_string(),
            file_id: None,
            row_pk: RowPk::single(row_pk),
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
            row_pk: RowPk::single("row-1"),
            schema_key: "test_schema".into(),
            file_id: None,
            snapshot: Some(crate::transaction_types::stage_json_from_value(
                crate::transaction_types::TransactionJson::from_value_for_test(
                    serde_json::json!({ "value": 1 }),
                ),
            )),
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
        row.snapshot = Some(crate::transaction_types::stage_json_from_value(
            crate::transaction_types::TransactionJson::from_value_for_test(
                serde_json::json!({ "value": "untracked" }),
            ),
        ));
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
        row.row_pk = RowPk::single(branch_id);
        row.schema_key = BRANCH_REF_SCHEMA_KEY.into();
        row.snapshot = Some(crate::transaction_types::stage_json_from_value(
            crate::transaction_types::TransactionJson::from_value_for_test(serde_json::json!({
                "id": branch_id,
                "commit_id": commit_id(target_commit_label).to_string(),
            })),
        ));
        row
    }

    fn untracked_key_value_row(
        key: &str,
        value: serde_json::Value,
        change_id: &str,
    ) -> TestPreparedStateRow {
        let mut row = untracked_global_row(change_id);
        row.row_pk = RowPk::single(key);
        row.schema_key = "lix_key_value".into();
        row.snapshot = Some(crate::transaction_types::stage_json_from_value(
            crate::transaction_types::TransactionJson::from_value_for_test(
                serde_json::json!({ "key": key, "value": value }),
            ),
        ));
        row
    }

    fn hot_state_request() -> HotStateRowRequest {
        HotStateRowRequest {
            schema_key: "test_schema".to_string(),
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            row_pk: RowPk::single("row-1"),
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
