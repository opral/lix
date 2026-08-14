//! Transaction-to-ForkTree physical lowering.
//!
//! SQL planning, statement rollback, stale reconciliation, and semantic
//! validation remain owned by `Transaction`. This module begins only after a
//! `PreparedWriteSet` has crossed that boundary. It produces one authenticated
//! `PreparedPublication`; the caller consumes it with `into_storage_plan` and
//! joins the resulting writes and exact preconditions to the existing single
//! backend commit.

use std::collections::{BTreeMap, BTreeSet};

use crate::LixError;
use crate::branch::BranchRefStoreReader;
use crate::changelog::{ChangeRecord, CommitId, CommitRecord};
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::json_store::JsonSlot;
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition, StorageWriteSet};
use crate::transaction::staging::{
    BranchRefPublicationIntent, HistoricalBlobManifestEdges, PreparedWriteSet,
};
use crate::transaction::types::PreparedStateRowRef;

use crate::forktree::{
    BranchSnapshotV1, BranchStateTransition, CanonicalBranchId, ChangeCatalogEntry,
    ChangeCatalogOwner, ChangeId as ForkTreeChangeId, ChangeObjectV1, CheckpointCursorV1,
    CommitCatalogEntry, CommitChangePageV3, CommitId as ForkTreeCommitId, CommitMemberV3,
    CommitObjectV1, HistoricalMemberSelection, ObjectId, OrderedBranchHistoryTransition,
    PreparedPublication, RepositoryRootV1, SelectedHistoricalMember, StateCell, StateKey,
    StateKeyRef, StateMutationAudit, StateSource, StateTreeMutation, StateValue, StateValueRef,
    encode_current_state_packs, encode_state_entity_prefix_bounds, encode_state_key,
    encode_state_value, introduced_checkpoint_marker, load_commit, load_commit_summary,
    open_coherent_view_on_read, select_historical_commit_members, state_points,
};

pub(crate) type RuntimeSequenceCheckpoint = (i64, LixTimestamp, crate::changelog::ChangeId);

/// Complete result of classifying one transaction's currently supported
/// ForkTree publication intent.
///
/// `Noop` is deliberately distinct from an empty `PreparedPublication`: a
/// genuine empty transaction must not rotate the global selector or discard
/// an in-progress GC page. Runtime/idempotency metadata can still be appended
/// by the transaction owner to this empty plan before its sole commit.
pub(crate) enum PreparedForkTreePlan {
    Noop,
    Publication(PreparedPublication),
}

/// Lowers a read execution's deterministic-function checkpoint through the
/// same authenticated ForkTree publication compiler as ordinary transactions.
/// The caller still owns the one backend prepare/commit boundary.
pub(crate) async fn prepare_runtime_sequence_publication<R>(
    active_account_id: &str,
    runtime_checkpoint: RuntimeSequenceCheckpoint,
    read: R,
) -> Result<PreparedForkTreePlan, LixError>
where
    R: StorageAdapterRead + Clone,
{
    prepare_forktree_publication_with_parent_heads(
        active_account_id,
        &BTreeMap::new(),
        Some(runtime_checkpoint),
        read,
        PreparedWriteSet {
            state_rows: crate::transaction::types::PreparedStateBatch::new(),
            insert_selection: crate::transaction::staging::PreparedInsertSelection::new(),
            commit_change_refs_by_branch: BTreeMap::new(),
            first_commit_parent_override_by_branch: BTreeMap::new(),
            checkpoint_publications: Vec::new(),
            extra_commit_parents_by_branch: BTreeMap::new(),
            intermediate_commits: Vec::new(),
            file_content_writes: Vec::new(),
            branch_ref_intents: Vec::new(),
            historical_blob_manifest_edges: BTreeMap::new(),
        },
        None,
    )
    .await
}

impl PreparedForkTreePlan {
    pub(crate) fn into_storage_plan(
        self,
    ) -> Result<(StorageWriteSet, Vec<StoragePrecondition>), LixError> {
        match self {
            Self::Noop => Ok((StorageWriteSet::new(), Vec::new())),
            Self::Publication(publication) => Ok(publication.into_storage_plan()?),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationIntent {
    Noop,
    Ordinary {
        branch_id: CanonicalBranchId,
        semantic_commit: bool,
    },
}

/// Resolves every branch head from the caller's coherent commit snapshot.
///
/// `BranchRefReader` is already a sealed ForkTree selector reader. Keeping
/// this semantic helper here preserves the transaction's pre-validation
/// ordering without restoring branch-control storage.
pub(crate) async fn resolve_prepared_commit_parent_heads(
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
        .chain(
            prepared_writes
                .branch_ref_intents
                .iter()
                .filter(|intent| !intent.create)
                .map(|intent| intent.branch_id.as_str()),
        )
        .collect::<BTreeSet<_>>();
    required_branch_ids.extend(commit_parent_branch_ids.iter().copied());

    let branch_ref = BranchRefStoreReader::new(read);
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
        if commit_parent_branch_ids.contains(branch_id) {
            parent_heads.insert(branch_id.to_string(), head);
        }
    }
    Ok(parent_heads)
}

/// Converts the ordinary prepared transaction cohort into one typed ForkTree
/// publication without performing storage I/O beyond the caller-owned read.
///
/// This is deliberately the first writer-last compiler slice. Cohorts whose
/// typed ForkTree publication owners are converted in later slices reject
/// before producing a storage plan; they never fall back to a deleted layout.
pub(crate) async fn prepare_forktree_publication_with_parent_heads<R>(
    active_account_id: &str,
    commit_parent_heads: &BTreeMap<String, Option<CommitId>>,
    runtime_checkpoint: Option<RuntimeSequenceCheckpoint>,
    read: R,
    mut prepared_writes: PreparedWriteSet,
    pending_publication: Option<PreparedPublication>,
) -> Result<PreparedForkTreePlan, LixError>
where
    R: StorageAdapterRead + Clone,
{
    // Keep certified complete replacements columnar through SQL and
    // transaction staging. The current commit encoder still consumes the
    // neutral prepared-row view; this single terminal lowering avoids the
    // former overlay/index/replay path and is the bridge to direct
    // root-transition page encoding.
    prepared_writes.lower_certified_ordered_mutation_journals()?;
    let intent = classify_publication_intent(&prepared_writes, runtime_checkpoint)?;
    if publication_owner_branch_ids(&prepared_writes, runtime_checkpoint.is_some()).len() > 1 {
        return Box::pin(prepare_batched_forktree_publication(
            active_account_id,
            commit_parent_heads,
            runtime_checkpoint,
            read,
            prepared_writes,
            pending_publication,
        ))
        .await;
    }
    let PublicationIntent::Ordinary {
        branch_id: publication_branch_id,
        semantic_commit,
    } = intent
    else {
        return Ok(match pending_publication {
            Some(publication) => PreparedForkTreePlan::Publication(publication),
            None => PreparedForkTreePlan::Noop,
        });
    };
    let branch_id = uuid::Uuid::from_bytes(*publication_branch_id.as_bytes()).to_string();
    let view = open_coherent_view_on_read(read.clone(), publication_branch_id).await?;
    let mut publication = PreparedPublication::from_branch_view(&view)?;
    if let Some(pending) = pending_publication {
        publication.merge_from(pending)?;
    }
    for owner_branch_id in
        publication_owner_branch_ids(&prepared_writes, runtime_checkpoint.is_some())
    {
        let owner_branch_id = canonical_branch_id(&owner_branch_id)?;
        if owner_branch_id != publication_branch_id {
            let owner_view = open_coherent_view_on_read(read.clone(), owner_branch_id).await?;
            publication.fence_branch_view(&owner_view)?;
        }
    }
    let prepared_blob_manifests =
        prepared_blob_manifest_ids(&mut publication, &view, &prepared_writes).await?;
    let branch_ref_intents = prepared_writes.branch_ref_intents.clone();

    for checkpoint in &prepared_writes.checkpoint_publications {
        crate::gc::stage_checkpoint_publication(&mut publication, &view, checkpoint).await?;
    }

    if !semantic_commit {
        let plan = PreparedForkTreePlan::Publication(publication);
        return append_branch_ref_intents(plan, &view, &branch_ref_intents).await;
    }
    let ordered_history = !prepared_writes.intermediate_commits.is_empty()
        || !prepared_writes
            .first_commit_parent_override_by_branch
            .is_empty()
        || prepared_writes
            .commit_change_refs_by_branch
            .values()
            .any(|refs| refs.has_selected_changes() || refs.tracked_change_count == 0);
    if ordered_history {
        let plan = prepare_ordered_single_branch_history(
            active_account_id,
            commit_parent_heads,
            &view,
            publication,
            prepared_writes,
            prepared_blob_manifests,
        )
        .await;
        return append_branch_ref_intents(plan?, &view, &branch_ref_intents).await;
    }
    let tracked_rows = prepared_writes
        .state_rows
        .iter()
        .filter(|row| !row.untracked)
        .collect::<Vec<_>>();
    debug_assert!(!tracked_rows.is_empty());

    let complete_replacement_bounds = prepared_writes
        .state_rows
        .complete_collection_replacement_proof()
        .map(|proof| {
            let first = tracked_rows
                .first()
                .copied()
                .ok_or_else(|| writer_error("complete replacement contains no rows"))?;
            if proof.replay_bytes == 0
                || proof.ordered_identity_digest
                    != crate::collection_generation::ordered_single_string_identity_digest(
                        tracked_rows.iter().map(|row| row.entity_pk),
                    )
                    .ok_or_else(|| {
                        writer_error("complete replacement has a non-string primary key")
                    })?
                || tracked_rows.iter().any(|row| {
                    row.schema_key != first.schema_key
                        || row.file_id.is_some()
                        || row.branch_id != first.branch_id
                        || row.global
                        || row.untracked
                        || row.snapshot.is_none()
                })
            {
                return Err(writer_error(
                    "complete replacement proof disagrees with its canonical row range",
                ));
            }
            let bounds =
                encode_state_entity_prefix_bounds(first.schema_key.as_str(), &EntityPk::empty());
            Ok((bounds.lower, bounds.upper))
        })
        .transpose()?;

    let change_refs = prepared_writes
        .commit_change_refs_by_branch
        .get(&branch_id)
        .ok_or_else(|| writer_error("tracked rows have no branch commit owner"))?;
    let commit_id = change_refs.commit_id;
    let global = tracked_rows[0].global;
    if tracked_rows
        .iter()
        .any(|row| row.branch_id.as_str() != branch_id || row.global != global)
    {
        return Err(writer_error(
            "one transaction commit mixes global and branch-local state roots",
        ));
    }

    // The catalog identity set is authenticated before any ChangeObject is
    // encoded. Keep this canonical order separate from the caller's member
    // and result-slot order below.
    let mut catalog_change_ids = Vec::with_capacity(tracked_rows.len() + 1);
    for row in &tracked_rows {
        let change_id = row
            .change_id
            .ok_or_else(|| writer_error("tracked row has no change identity"))?;
        catalog_change_ids.push(forktree_change_id(change_id));
    }
    catalog_change_ids.push(forktree_change_id(change_refs.branch_ref_change_id));
    let catalog_order = canonical_change_order(&catalog_change_ids)?;

    let tracked_keys = tracked_rows
        .iter()
        .map(|row| {
            encode_state_key(StateKeyRef {
                schema_key: row.schema_key.as_str(),
                file_id: row.file_id.map(|value| value.as_str()),
                entity_pk: row.entity_pk,
            })
        })
        .collect::<Vec<_>>();
    let previous_rows = if complete_replacement_bounds.is_some() {
        // The complete-set certificate proves that every ordered identity is
        // the current collection.  Reading every predecessor would recreate
        // the O(N log N) point path this root replacement is intended to cut.
        std::iter::repeat_with(|| None)
            .take(tracked_rows.len())
            .collect()
    } else {
        state_points(&view, &tracked_keys, true).await?
    };
    if previous_rows.len() != tracked_rows.len() {
        return Err(writer_error(
            "ForkTree predecessor lookup returned the wrong slot count",
        ));
    }
    let mut members = Vec::with_capacity(tracked_rows.len());
    let mut pending_rows = Vec::with_capacity(tracked_rows.len());
    for ((row, key), previous) in tracked_rows
        .into_iter()
        .zip(tracked_keys)
        .zip(previous_rows)
    {
        let row_commit_id = row
            .commit_id
            .ok_or_else(|| writer_error("tracked row has no commit identity"))?;
        if row_commit_id != commit_id {
            return Err(writer_error(
                "tracked row commit identity differs from its branch commit",
            ));
        }
        let change_id = row
            .change_id
            .ok_or_else(|| writer_error("tracked row has no change identity"))?;
        let blob_manifest_object_ids = blob_manifest_object_ids_for_row(
            row,
            &prepared_blob_manifests,
            &prepared_writes.historical_blob_manifest_edges,
        )?;
        let current_value = StateValue {
            change_id,
            commit_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
            cell: match row.snapshot {
                None => StateCell::Tombstone,
                Some(_) => StateCell::NativeRow(row.native_row.cloned().ok_or_else(|| {
                    writer_error("live prepared row has no authenticated native tuple")
                })?),
            },
            metadata: row
                .metadata
                .map(|value| crate::common::SharedStr::from(value.normalized())),
            origin_key: row.origin_key.map(ToString::to_string),
            blob_manifest_object_ids: blob_manifest_object_ids.clone(),
        };
        let (layout_id, owner_digest, semantic_digest) = match &current_value.cell {
            StateCell::NativeRow(native) => {
                (native.layout_id, native.owner_digest, native.semantic_digest)
            }
            StateCell::Tombstone => ([0; 32], [0; 32], [0; 32]),
            StateCell::Value(_) | StateCell::Null => {
                return Err(writer_error(
                    "tracked history row uses the removed JSON current-state representation",
                ));
            }
        };
        members.push(CommitMemberV3::introduced(
            forktree_change_id(change_id),
            key.clone(),
            layout_id,
            global,
            owner_digest,
            semantic_digest,
            matches!(&current_value.cell, StateCell::Tombstone),
            active_account_id.to_string(),
            row.created_at,
            row.updated_at,
            row.origin_key.map(ToString::to_string),
        ));
        pending_rows.push((row, key, previous, blob_manifest_object_ids, current_value));
    }
    let member_pages = CommitChangePageV3::encode_pages(forktree_commit_id(commit_id), &members)?;
    let current_packs = encode_current_state_packs(
        forktree_commit_id(commit_id),
        global,
        pending_rows
            .iter()
            .zip(&member_pages.member_locations)
            .filter(|((row, ..), _)| !(global && row.snapshot.is_none()))
            .map(|((_, key, _, _, value), location)| (key.clone(), value.clone(), *location))
            .collect(),
    )?;
    let mut state_mutations = Vec::with_capacity(pending_rows.len());
    let mut replacement_entries = complete_replacement_bounds
        .as_ref()
        .map(|_| Vec::with_capacity(pending_rows.len()));
    for ((row, key, previous, blob_manifest_object_ids, _), _) in
        pending_rows.into_iter().zip(&member_pages.member_locations)
    {
        let mutation = if global && row.snapshot.is_none() {
            StateTreeMutation::remove(key)
        } else {
            let location = current_packs
                .locations
                .get(&key)
                .ok_or_else(|| writer_error("current-state pack omitted a published row"))?;
            let encoded = encode_state_value(StateValueRef {
                pack_object_id: location.pack_object_id,
                pack_ordinal: location.pack_ordinal,
            })?;
            let audit = StateMutationAudit {
                commit_id: *commit_id.as_uuid().as_bytes(),
                tombstone: row.snapshot.is_none(),
                blob_manifest_object_ids,
            };
            if let Some(entries) = &mut replacement_entries {
                entries.push((key, encoded, audit));
                continue;
            }
            let exists_at_target_root = previous
                .as_ref()
                .is_some_and(|value| global || value.source == StateSource::Branch);
            if exists_at_target_root {
                StateTreeMutation::update_bound(key, encoded, audit)
            } else {
                StateTreeMutation::insert_bound(key, encoded, audit)
            }
        };
        state_mutations.push(mutation);
        if let Some((lower, upper)) = collection_delete_range(row)? {
            state_mutations.push(StateTreeMutation::remove_range(lower, upper));
        }
    }
    sort_state_mutations(&mut state_mutations)?;

    let state_base = if global {
        view.repository_root().global_state_root
    } else {
        view.branch_snapshot().local_state_root
    };
    let root_transition = complete_replacement_bounds.is_some();
    let mut state_edit = if let (Some((lower, upper)), Some(entries)) =
        (complete_replacement_bounds, replacement_entries)
    {
        view.replace_state_tree_range(state_base, lower, upper, entries)
            .await?
    } else {
        view.edit_state_tree(state_base, state_mutations).await?
    };
    state_edit.stage_objects(current_packs.objects)?;

    let expected_parent = commit_parent_heads
        .get(&branch_id)
        .copied()
        .flatten()
        .ok_or_else(|| writer_error("branch commit has no selected parent"))?;
    let selected_parent = load_commit_summary(&view, forktree_commit_id(expected_parent))
        .await?
        .ok_or_else(|| writer_error("selected branch parent is absent from CommitCatalog"))?;
    let (selected_parent_object_id, _) = selected_parent.encode()?;
    if selected_parent_object_id != view.branch_snapshot().semantic_head_commit_object_id {
        return Err(writer_error(
            "resolved branch parent differs from the authenticated selector head",
        ));
    }
    let mut parent_commit_ids = vec![expected_parent];
    let mut parent_object_ids = vec![selected_parent_object_id];
    let mut generation = selected_parent.generation;
    for parent in prepared_writes
        .extra_commit_parents_by_branch
        .get(&branch_id)
        .into_iter()
        .flatten()
    {
        if parent_commit_ids.contains(parent) {
            continue;
        }
        let parent_object = load_commit(&view, forktree_commit_id(*parent))
            .await?
            .ok_or_else(|| writer_error("extra commit parent is absent from CommitCatalog"))?;
        let (parent_object_id, _) = parent_object.encode()?;
        parent_commit_ids.push(*parent);
        parent_object_ids.push(parent_object_id);
        generation = generation.max(parent_object.generation);
    }
    generation = generation
        .checked_add(1)
        .ok_or_else(|| writer_error("commit generation overflows u64"))?;

    let semantic_change_ids = members
        .iter()
        .map(CommitMemberV3::change_id)
        .collect::<Vec<_>>();
    let current_repository_root = publication.current_repository_root();
    let global_state_root = if global {
        state_edit.root
    } else {
        current_repository_root.global_state_root
    };
    let local_state_root = if global {
        view.branch_snapshot().local_state_root
    } else {
        state_edit.root
    };
    let commit_record = CommitRecord {
        format_version: 2,
        commit_id,
        generation,
        parent_commit_ids,
        change_id: change_refs.commit_change_id,
        account_id: active_account_id.to_string(),
        created_at: change_refs.created_at,
    };
    let checkpoint_cursor = CheckpointCursorV1::after_first_parent(
        selected_parent_object_id,
        &selected_parent,
        publication_branch_id,
        introduced_checkpoint_marker(&members, publication_branch_id)?,
    )?;
    let mut semantic_commit = CommitObjectV1 {
        commit_id: forktree_commit_id(commit_id),
        generation,
        parent_commit_object_ids: parent_object_ids,
        members,
        member_page_object_ids: member_pages.objects.iter().map(|(id, _)| *id).collect(),
        global_state_root,
        local_state_root,
        checkpoint_cursor,
        metadata: crate::changelog::encode_forktree_commit_payload(&commit_record)?,
    };
    let _member_pages = semantic_commit.prepare_member_pages()?;
    let (commit_object_id, _) = semantic_commit.encode()?;

    let ref_payload = crate::changelog::encode_forktree_change_payload(&ChangeRecord {
        format_version: 2,
        change_id: change_refs.branch_ref_change_id,
        account_id: active_account_id.to_string(),
        schema_key: crate::branch::BRANCH_REF_SCHEMA_KEY.to_string(),
        entity_pk: EntityPk::uuid_from_canonical(&branch_id).map_err(|error| {
            writer_error(format!("transaction branch identity is invalid: {error}"))
        })?,
        file_id: None,
        snapshot: JsonSlot::Inline(
            serde_json::json!({
                "branch_id": branch_id,
                "commit_id": commit_id.to_string(),
            })
            .to_string()
            .into(),
        ),
        metadata: JsonSlot::None,
        created_at: change_refs.created_at,
        origin_key: None,
    })?;
    let ref_change = ChangeObjectV1::BranchRef {
        change_id: forktree_change_id(change_refs.branch_ref_change_id),
        updated_at: change_refs.created_at,
        branch_id: publication_branch_id,
        before_semantic_head_commit_object_id: Some(selected_parent_object_id),
        after_semantic_head_commit_object_id: Some(commit_object_id),
        previous_ref_change_object_id: view.branch_snapshot().latest_ref_change_object_id,
        payload: ref_payload,
        json_payload_object_ids: Vec::new(),
    };
    let (ref_object_id, _) = ref_change.encode()?;
    let changes = vec![ref_change];

    let commit_catalog_edit = publication
        .put_commit_catalog_entries(
            &view,
            current_repository_root.commit_catalog_root,
            &[(
                (forktree_commit_id(commit_id)),
                CommitCatalogEntry { commit_object_id },
            )],
        )
        .await?;
    let mut change_entries = Vec::with_capacity(catalog_order.len());
    for index in catalog_order {
        if let Some(change_id) = semantic_change_ids.get(index) {
            change_entries.push((
                *change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id,
                        ordinal: u32::try_from(index)
                            .map_err(|_| writer_error("commit member ordinal exceeds u32"))?,
                    },
                },
            ));
        } else {
            debug_assert_eq!(index, semantic_change_ids.len());
            change_entries.push((
                forktree_change_id(change_refs.branch_ref_change_id),
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::BranchRef {
                        ref_change_object_id: ref_object_id,
                        branch_id: publication_branch_id,
                    },
                },
            ));
        }
    }
    // A complete collection replacement is already authenticated twice: its
    // state leaves point at the exact commit-member pages and the semantic
    // commit authenticates both those pages and the resulting state root.
    // Persisting another ChangeCatalog leaf for every member merely rebuilds
    // a second N-row ownership index during the mutation. Store one packed
    // commit marker plus the independently-addressable branch-ref change;
    // the authenticated member closure remains the canonical root-to-root
    // history owner.
    let persisted_change_entries = if root_transition {
        let mut entries = change_entries
            .iter()
            .filter(|(_, entry)| matches!(entry.owner, ChangeCatalogOwner::BranchRef { .. }))
            .copied()
            .collect::<Vec<_>>();
        entries.push((
            ForkTreeChangeId::from_bytes(*semantic_commit.commit_id.as_bytes()),
            ChangeCatalogEntry {
                owner: ChangeCatalogOwner::PackedCommit {
                    commit_object_id,
                    member_count: u32::try_from(semantic_commit.members.len())
                        .map_err(|_| writer_error("packed commit member count exceeds u32"))?,
                },
            },
        ));
        entries.sort_unstable_by_key(|(id, _)| *id);
        entries
    } else {
        change_entries.clone()
    };
    let mut change_catalog_edit = publication
        .put_change_catalog_entries(
            &view,
            current_repository_root.change_catalog_root,
            &persisted_change_entries,
        )
        .await?;
    if root_transition {
        // Publication validates the exact commit/member ownership map before
        // writing. These ephemeral entries are not a durable second index.
        change_catalog_edit.change_entries.extend(change_entries);
    }
    let repository_root = RepositoryRootV1 {
        global_state_root: if global {
            state_edit.root
        } else {
            current_repository_root.global_state_root
        },
        commit_catalog_root: commit_catalog_edit.root,
        change_catalog_root: change_catalog_edit.root,
    };
    let transition = BranchStateTransition {
        state_edit,
        commit_catalog_edit,
        change_catalog_edit,
        semantic_commit,
        changes,
        branch_snapshot: BranchSnapshotV1 {
            branch_id: publication_branch_id,
            local_state_root,
            semantic_head_commit_object_id: commit_object_id,
            latest_ref_change_object_id: Some(ref_object_id),
            historical_global_state_root: global_state_root,
        },
        repository_root,
    };
    publication
        .publish_state_transition(&view, transition)
        .await?;
    append_branch_ref_intents(
        PreparedForkTreePlan::Publication(publication),
        &view,
        &branch_ref_intents,
    )
    .await
}

async fn prepare_batched_forktree_publication<R>(
    active_account_id: &str,
    commit_parent_heads: &BTreeMap<String, Option<CommitId>>,
    runtime_checkpoint: Option<RuntimeSequenceCheckpoint>,
    read: R,
    prepared_writes: PreparedWriteSet,
    pending_publication: Option<PreparedPublication>,
) -> Result<PreparedForkTreePlan, LixError>
where
    R: StorageAdapterRead + Clone,
{
    let branch_ids = publication_owner_branch_ids(&prepared_writes, runtime_checkpoint.is_some())
        .into_iter()
        .collect::<Vec<_>>();
    let mut branch_ids = branch_ids;
    branch_ids.sort_by(|left, right| {
        (left != crate::GLOBAL_BRANCH_ID)
            .cmp(&(right != crate::GLOBAL_BRANCH_ID))
            .then_with(|| left.cmp(right))
    });
    if branch_ids.len() < 2 {
        return Err(writer_error(
            "batched ForkTree publication was requested without multiple owners",
        ));
    }
    if prepared_writes
        .intermediate_commits
        .iter()
        .any(|commit| !branch_ids.contains(&commit.branch_id))
    {
        return Err(writer_error(
            "batched ForkTree publication contains an unknown intermediate branch",
        ));
    }

    let mut pending_publication = pending_publication;
    for (index, branch_id) in branch_ids.iter().enumerate() {
        let branch_writes = prepared_writes_for_branch(&prepared_writes, branch_id);
        let plan = Box::pin(prepare_forktree_publication_with_parent_heads(
            active_account_id,
            commit_parent_heads,
            (index == 0).then_some(runtime_checkpoint).flatten(),
            read.clone(),
            branch_writes,
            pending_publication,
        ))
        .await?;
        pending_publication = match plan {
            PreparedForkTreePlan::Noop => {
                return Err(writer_error(
                    "batched ForkTree publication contains an owner with no native work",
                ));
            }
            PreparedForkTreePlan::Publication(publication) => Some(publication),
        };
    }
    pending_publication
        .map(PreparedForkTreePlan::Publication)
        .ok_or_else(|| writer_error("batched ForkTree publication produced no plan"))
}

fn prepared_writes_for_branch(prepared: &PreparedWriteSet, branch_id: &str) -> PreparedWriteSet {
    PreparedWriteSet {
        state_rows: prepared.state_rows.for_publication_branch(branch_id),
        insert_selection: prepared.insert_selection.clone(),
        commit_change_refs_by_branch: prepared
            .commit_change_refs_by_branch
            .iter()
            .filter(|(owner, _)| owner.as_str() == branch_id)
            .map(|(owner, refs)| (owner.clone(), refs.clone()))
            .collect(),
        first_commit_parent_override_by_branch: prepared
            .first_commit_parent_override_by_branch
            .iter()
            .filter(|(owner, _)| owner.as_str() == branch_id)
            .map(|(owner, parent)| (owner.clone(), *parent))
            .collect(),
        checkpoint_publications: prepared
            .checkpoint_publications
            .iter()
            .filter(|checkpoint| checkpoint.recovery_ref.branch_id == branch_id)
            .cloned()
            .collect(),
        extra_commit_parents_by_branch: prepared
            .extra_commit_parents_by_branch
            .iter()
            .filter(|(owner, _)| owner.as_str() == branch_id)
            .map(|(owner, parents)| (owner.clone(), parents.clone()))
            .collect(),
        intermediate_commits: prepared
            .intermediate_commits
            .iter()
            .filter(|commit| commit.branch_id == branch_id)
            .cloned()
            .collect(),
        file_content_writes: prepared
            .file_content_writes
            .iter()
            .filter(|write| write.branch_id == branch_id)
            .cloned()
            .collect(),
        branch_ref_intents: prepared
            .branch_ref_intents
            .iter()
            .filter(|intent| intent.branch_id == branch_id)
            .cloned()
            .collect(),
        historical_blob_manifest_edges: prepared
            .historical_blob_manifest_edges
            .iter()
            .filter(|((owner, _), _)| owner == branch_id)
            .map(|(key, edges)| (key.clone(), edges.clone()))
            .collect(),
    }
}

async fn append_branch_ref_intents<R>(
    plan: PreparedForkTreePlan,
    view: &crate::forktree::CoherentView<R>,
    intents: &[BranchRefPublicationIntent],
) -> Result<PreparedForkTreePlan, LixError>
where
    R: StorageAdapterRead + Clone,
{
    let PreparedForkTreePlan::Publication(mut publication) = plan else {
        return Ok(plan);
    };
    let mut branch_intent_ids = BTreeSet::new();
    for intent in intents {
        let branch_id = canonical_branch_id(&intent.branch_id)?;
        if !branch_intent_ids.insert(branch_id) {
            return Err(writer_error(
                "one transaction publishes one branch selector more than once",
            ));
        }
        if intent.create {
            let source_head = intent
                .commit_id
                .ok_or_else(|| writer_error("branch creation has no source commit"))?;
            let source_commit = load_commit_summary(view, forktree_commit_id(source_head))
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_FOREIGN_KEY,
                        format!(
                            "branch ref commit_id '{}' does not reference an existing commit",
                            source_head
                        ),
                    )
                })?;
            publication
                .publish_new_branch_selector(
                    view,
                    branch_id,
                    &source_commit,
                    forktree_change_id(intent.change_id),
                    intent.updated_at,
                )
                .await
                .map_err(LixError::from)?;
        } else {
            let target_view = view.branch_view(branch_id).await?;
            let target_commit = match intent.commit_id {
                Some(commit_id) => Some(
                    // The target branch selector can lag the active branch's
                    // authenticated catalog. Resolve the requested commit
                    // from the operation view, then use the target view only
                    // for selector identity/CAS and branch-local state.
                    load_commit(view, forktree_commit_id(commit_id))
                        .await?
                        .ok_or_else(|| writer_error("branch selector target commit is absent"))?,
                ),
                None => None,
            };
            let requested_object_id = target_commit
                .as_ref()
                .map(|commit| commit.encode().map(|(id, _)| id))
                .transpose()?;
            let moves_head = requested_object_id.is_some_and(|object_id| {
                object_id != target_view.branch_snapshot().semantic_head_commit_object_id
            }) || intent.commit_id.is_none();
            publication
                .publish_branch_selector_intent(
                    &target_view,
                    target_commit,
                    forktree_change_id(intent.change_id),
                    intent.updated_at,
                )
                .await
                .map_err(LixError::from)?;
        }
    }
    Ok(PreparedForkTreePlan::Publication(publication))
}

struct OrderedCommitDraft {
    commit_id: CommitId,
    commit_change_id: crate::changelog::ChangeId,
    branch_ref_change_id: crate::changelog::ChangeId,
    created_at: LixTimestamp,
    parent_commit_ids: Vec<CommitId>,
    selected_change_batches: Vec<crate::transaction::types::StagedCommitChangeBatch>,
    publish_head: bool,
}

struct OrderedCommitContent {
    draft: OrderedCommitDraft,
    mutations: Vec<StateTreeMutation>,
    members: Vec<CommitMemberV3>,
    member_page_object_ids: Vec<ObjectId>,
    current_pack_objects: Vec<(ObjectId, bytes::Bytes)>,
    max_selected_source_generation: Option<u64>,
}

enum PendingStateMutation {
    Remove {
        key: Vec<u8>,
    },
    Put {
        key: Vec<u8>,
        existed: bool,
        tombstone: bool,
        blob_manifest_object_ids: Vec<ObjectId>,
        value: StateValue,
    },
}

async fn prepare_ordered_single_branch_history<R>(
    active_account_id: &str,
    commit_parent_heads: &BTreeMap<String, Option<CommitId>>,
    view: &crate::forktree::CoherentView<R>,
    mut publication: PreparedPublication,
    prepared: PreparedWriteSet,
    prepared_blob_manifests: PreparedBlobManifestMap,
) -> Result<PreparedForkTreePlan, LixError>
where
    R: StorageAdapterRead + Clone,
{
    let branch_id = prepared
        .commit_change_refs_by_branch
        .keys()
        .next()
        .cloned()
        .ok_or_else(|| writer_error("ordered history has no final branch commit owner"))?;
    if prepared.commit_change_refs_by_branch.len() != 1
        || prepared
            .intermediate_commits
            .iter()
            .any(|commit| commit.branch_id != branch_id)
    {
        return Err(writer_error(
            "ordered history must target exactly one branch",
        ));
    }

    let mut drafts = Vec::with_capacity(prepared.intermediate_commits.len() + 1);
    for intermediate in prepared.intermediate_commits.iter().cloned() {
        let refs = intermediate.change_refs;
        drafts.push(OrderedCommitDraft {
            commit_id: refs.commit_id,
            commit_change_id: refs.commit_change_id,
            branch_ref_change_id: refs.branch_ref_change_id,
            created_at: refs.created_at,
            parent_commit_ids: vec![intermediate.parent_commit_id],
            selected_change_batches: refs.into_selected_change_batches(),
            publish_head: false,
        });
    }
    let final_refs = prepared
        .commit_change_refs_by_branch
        .get(&branch_id)
        .cloned()
        .ok_or_else(|| writer_error("ordered history final commit owner is absent"))?;
    let observed_parent = commit_parent_heads
        .get(&branch_id)
        .copied()
        .flatten()
        .ok_or_else(|| writer_error("ordered history branch has no observed parent head"))?;
    let first_parent = prepared
        .first_commit_parent_override_by_branch
        .get(&branch_id)
        .copied()
        .unwrap_or(observed_parent);
    let mut final_parents = vec![first_parent];
    for parent in prepared
        .extra_commit_parents_by_branch
        .get(&branch_id)
        .into_iter()
        .flatten()
    {
        if !final_parents.contains(parent) {
            final_parents.push(*parent);
        }
    }
    drafts.push(OrderedCommitDraft {
        commit_id: final_refs.commit_id,
        commit_change_id: final_refs.commit_change_id,
        branch_ref_change_id: final_refs.branch_ref_change_id,
        created_at: final_refs.created_at,
        parent_commit_ids: final_parents,
        selected_change_batches: final_refs.into_selected_change_batches(),
        publish_head: true,
    });

    let staged_ids = drafts
        .iter()
        .map(|draft| draft.commit_id)
        .collect::<BTreeSet<_>>();
    if staged_ids.len() != drafts.len() {
        return Err(writer_error("ordered history repeats one staged CommitId"));
    }

    // Ordered checkpoint history starts at the authenticated parent of its
    // first draft, not necessarily at the currently selected branch head.
    // Partial checkpoints deliberately publish an intermediate checkpoint
    // from the previous checkpoint and then replay the unselected changes.
    let sequence_parent_id = drafts
        .first()
        .and_then(|draft| draft.parent_commit_ids.first())
        .copied()
        .ok_or_else(|| writer_error("ordered history first draft has no parent"))?;
    let selected_requests = drafts
        .iter()
        .flat_map(|draft| &draft.selected_change_batches)
        .flat_map(crate::transaction::types::StagedCommitChangeBatch::iter)
        .map(|selected| {
            HistoricalMemberSelection::new(
                forktree_commit_id(selected.source_commit_id),
                forktree_change_id(selected.change_id),
                encode_state_key(StateKeyRef {
                    schema_key: selected.schema_key(),
                    file_id: selected.file_id(),
                    entity_pk: selected.entity_pk(),
                }),
            )
        })
        .collect::<Vec<_>>();
    let mut selected_batch = select_historical_commit_members(
        view,
        forktree_commit_id(sequence_parent_id),
        &selected_requests,
    )
    .await?;
    let sequence_parent = selected_batch.sequence_parent().clone();
    let sequence_global_root = sequence_parent.global_state_root;
    let sequence_local_root = sequence_parent.local_state_root;
    let mut selected_members = selected_batch.take_selected().into_iter();

    let mut state_domain = prepared
        .state_rows
        .iter()
        .find(|row| !row.untracked)
        .map(|row| row.global);
    if prepared
        .state_rows
        .iter()
        .filter(|row| !row.untracked)
        .any(|row| row.branch_id.as_str() != branch_id || state_domain != Some(row.global))
    {
        return Err(writer_error(
            "ordered history mixes branch or global state-root domains",
        ));
    }

    let mut touched_presence = BTreeMap::<Vec<u8>, bool>::new();
    // Fresh identities are the only new catalog members in ordered history;
    // selected historical members already own catalog entries. Build the
    // unique authenticated key set before any fresh object is encoded.
    let mut catalog_change_ids = Vec::new();
    for draft in &drafts {
        for row in prepared
            .state_rows
            .iter()
            .filter(|row| !row.untracked && row.commit_id == Some(draft.commit_id))
        {
            let change_id = row
                .change_id
                .ok_or_else(|| writer_error("ordered history row has no ChangeId"))?;
            catalog_change_ids.push(forktree_change_id(change_id));
        }
    }
    catalog_change_ids.push(forktree_change_id(
        drafts
            .last()
            .expect("ordered history is nonempty")
            .branch_ref_change_id,
    ));
    let catalog_order = canonical_change_order(&catalog_change_ids)?;

    let mut contents = Vec::with_capacity(drafts.len());
    for draft in drafts {
        let mut seen_identities = BTreeSet::<Vec<u8>>::new();
        let mut pending_mutations = Vec::new();
        let mut members = Vec::new();
        let mut max_selected_source_generation: Option<u64> = None;
        let fresh_rows = prepared
            .state_rows
            .iter()
            .filter(|row| !row.untracked && row.commit_id == Some(draft.commit_id))
            .collect::<Vec<_>>();
        if fresh_rows.len()
            != if draft.publish_head {
                prepared
                    .commit_change_refs_by_branch
                    .get(&branch_id)
                    .map_or(0, |refs| refs.tracked_change_count)
            } else {
                prepared
                    .intermediate_commits
                    .iter()
                    .find(|commit| commit.change_refs.commit_id == draft.commit_id)
                    .map_or(0, |commit| commit.change_refs.tracked_change_count)
            }
        {
            return Err(writer_error(
                "ordered history fresh row count differs from commit membership",
            ));
        }
        for row in fresh_rows {
            let change_id = row
                .change_id
                .ok_or_else(|| writer_error("ordered history row has no ChangeId"))?;
            let key = encode_state_key(StateKeyRef {
                schema_key: row.schema_key.as_str(),
                file_id: row.file_id.map(|value| value.as_str()),
                entity_pk: row.entity_pk,
            });
            if !seen_identities.insert(key.clone()) {
                return Err(writer_error(
                    "ordered history repeats one logical state identity",
                ));
            }
            let blob_manifest_object_ids = blob_manifest_object_ids_for_row(
                row,
                &prepared_blob_manifests,
                &prepared.historical_blob_manifest_edges,
            )?;
            let deleted = row.snapshot.is_none();
            let (layout_id, owner_digest, semantic_digest) = match (deleted, row.native_row.as_ref()) {
                (false, Some(native)) => {
                    (native.layout_id, native.owner_digest, native.semantic_digest)
                }
                (true, None) => ([0; 32], [0; 32], [0; 32]),
                (false, None) => {
                    return Err(writer_error(
                        "live ordered history row has no authenticated native tuple",
                    ));
                }
                (true, Some(_)) => {
                    return Err(writer_error(
                        "ordered history tombstone unexpectedly carries a native tuple",
                    ));
                }
            };
            members.push(CommitMemberV3::introduced(
                forktree_change_id(change_id),
                key.clone(),
                layout_id,
                row.global,
                owner_digest,
                semantic_digest,
                deleted,
                active_account_id.to_string(),
                row.created_at,
                row.updated_at,
                row.origin_key.map(ToString::to_string),
            ));

            let existed = match touched_presence.get(&key) {
                Some(existed) => *existed,
                None => view
                    .state_point_at_roots(sequence_global_root, sequence_local_root, &key, true)
                    .await?
                    .as_ref()
                    .is_some_and(|(_, source)| row.global || *source == StateSource::Branch),
            };
            let mutation = if row.global && row.snapshot.is_none() {
                touched_presence.insert(key.clone(), false);
                PendingStateMutation::Remove { key }
            } else {
                touched_presence.insert(key.clone(), true);
                PendingStateMutation::Put {
                    key,
                    existed,
                    tombstone: row.snapshot.is_none(),
                    blob_manifest_object_ids: blob_manifest_object_ids.clone(),
                    value: StateValue {
                        change_id,
                        commit_id: draft.commit_id,
                        created_at: row.created_at,
                        updated_at: row.updated_at,
                        cell: match row.snapshot {
                            None => StateCell::Tombstone,
                            Some(_) => StateCell::NativeRow(row.native_row.cloned().ok_or_else(|| {
                                writer_error("live prepared row has no authenticated native tuple")
                            })?),
                        },
                        metadata: row
                            .metadata
                            .map(|value| crate::common::SharedStr::from(value.normalized())),
                        origin_key: row.origin_key.map(ToString::to_string),
                        blob_manifest_object_ids: blob_manifest_object_ids.clone(),
                    },
                }
            };
            pending_mutations.push(mutation);
        }

        for batch in &draft.selected_change_batches {
            for selected in batch.iter() {
                let identity = encode_state_key(StateKeyRef {
                    schema_key: selected.schema_key(),
                    file_id: selected.file_id(),
                    entity_pk: selected.entity_pk(),
                });
                if !seen_identities.insert(identity.clone()) {
                    return Err(writer_error(
                        "ordered history repeats one fresh/selected logical identity",
                    ));
                }
                if staged_ids.contains(&selected.source_commit_id) {
                    return Err(writer_error(
                        "selected history cannot source an uncommitted commit from the same batch",
                    ));
                }
                let SelectedHistoricalMember {
                    member,
                    source_commit,
                    source_state: source_value,
                    source_domain,
                    sequence_state: sequence_value,
                } = selected_members.next().ok_or_else(|| {
                    writer_error("ordered history selected-member batch is incomplete")
                })?;
                max_selected_source_generation = Some(
                    max_selected_source_generation.map_or(source_commit.generation, |generation| {
                        generation.max(source_commit.generation)
                    }),
                );
                let selected_global = source_domain == StateSource::Global;
                let sequence_semantically_absent = sequence_value
                    .as_ref()
                    .is_none_or(|(value, _)| value.cell.deleted());
                let source_membership_exact = member.selected_created_at() == Some(selected.created_at)
                    && source_value.created_at == selected.created_at;
                let canonical_checkpoint_add = !batch.source_membership_certified()
                    && sequence_semantically_absent
                    && !selected.deleted
                    && selected.created_at == source_value.updated_at;
                if state_domain
                    .replace(selected_global)
                    .is_some_and(|domain| domain != selected_global)
                    || source_value.change_id != selected.change_id
                    || source_value.commit_id != selected.source_commit_id
                    || source_value.cell.deleted() != selected.deleted
                    || source_value.updated_at != selected.updated_at
                    || (batch.source_membership_certified() && !source_membership_exact)
                    || (!source_membership_exact && !canonical_checkpoint_add)
                {
                    return Err(writer_error(
                        "selected history source state authority is inconsistent",
                    ));
                }
                let existed = match touched_presence.get(&identity) {
                    Some(existed) => *existed,
                    None => sequence_value.as_ref().is_some_and(|(_, source)| {
                        selected_global || *source == StateSource::Branch
                    }),
                };
                let mutation = if selected_global && selected.deleted {
                    touched_presence.insert(identity.clone(), false);
                    PendingStateMutation::Remove { key: identity }
                } else {
                    touched_presence.insert(identity.clone(), true);
                    PendingStateMutation::Put {
                        key: identity,
                        existed,
                        tombstone: selected.deleted,
                        blob_manifest_object_ids: source_value.blob_manifest_object_ids.clone(),
                        value: StateValue {
                            change_id: selected.change_id,
                            commit_id: draft.commit_id,
                            created_at: selected.created_at,
                            updated_at: selected.updated_at,
                            cell: source_value.cell.clone(),
                            metadata: source_value.metadata.clone(),
                            origin_key: source_value.origin_key.clone(),
                            blob_manifest_object_ids: source_value.blob_manifest_object_ids.clone(),
                        },
                    }
                };
                pending_mutations.push(mutation);
                members.push(
                    member
                        .with_selected_created_at(selected.created_at)
                        .map_err(LixError::from)?,
                );
            }
        }
        let member_pages =
            CommitChangePageV3::encode_pages(forktree_commit_id(draft.commit_id), &members)?;
        if member_pages.member_locations.len() != pending_mutations.len() {
            return Err(writer_error(
                "ordered history state mutations differ from commit membership",
            ));
        }
        let current_packs = encode_current_state_packs(
            forktree_commit_id(draft.commit_id),
            state_domain.unwrap_or(false),
            pending_mutations
                .iter()
                .zip(&member_pages.member_locations)
                .filter_map(|(pending, location)| match pending {
                    PendingStateMutation::Remove { .. } => None,
                    PendingStateMutation::Put { key, value, .. } => {
                        Some((key.clone(), value.clone(), *location))
                    }
                })
                .collect(),
        )?;
        let mutations = pending_mutations
            .into_iter()
            .zip(&member_pages.member_locations)
            .map(|(pending, _history_location)| {
                Ok(match pending {
                    PendingStateMutation::Remove { key } => StateTreeMutation::remove(key),
                    PendingStateMutation::Put {
                        key,
                        existed,
                        tombstone,
                        blob_manifest_object_ids,
                        value: _,
                    } => {
                        let location = current_packs.locations.get(&key).ok_or_else(|| {
                            writer_error("current-state pack omitted an ordered-history row")
                        })?;
                        let encoded = encode_state_value(StateValueRef {
                            pack_object_id: location.pack_object_id,
                            pack_ordinal: location.pack_ordinal,
                        })?;
                        let audit = StateMutationAudit {
                            commit_id: *draft.commit_id.as_uuid().as_bytes(),
                            tombstone,
                            blob_manifest_object_ids,
                        };
                        if existed {
                            StateTreeMutation::update_bound(key, encoded, audit)
                        } else {
                            StateTreeMutation::insert_bound(key, encoded, audit)
                        }
                    }
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        contents.push(OrderedCommitContent {
            draft,
            mutations,
            members,
            member_page_object_ids: member_pages.objects.iter().map(|(id, _)| *id).collect(),
            current_pack_objects: current_packs.objects,
            max_selected_source_generation,
        });
    }
    if selected_members.next().is_some() {
        return Err(writer_error(
            "ordered history selected-member batch has trailing rows",
        ));
    }

    let global = state_domain.unwrap_or(false);
    let state_base = if global {
        sequence_global_root
    } else {
        sequence_local_root
    };
    for content in &mut contents {
        sort_state_mutations(&mut content.mutations)?;
    }
    let mut state_edits = view
        .edit_state_tree_sequence(
            state_base,
            contents
                .iter_mut()
                .map(|content| std::mem::take(&mut content.mutations))
                .collect(),
        )
        .await?;
    for (edit, content) in state_edits.iter_mut().zip(&mut contents) {
        edit.stage_objects(std::mem::take(&mut content.current_pack_objects))?;
    }

    let mut staged_commits = BTreeMap::<CommitId, (ObjectId, CommitObjectV1)>::new();
    let mut semantic_commits = Vec::with_capacity(contents.len());
    let mut commit_entries = Vec::with_capacity(contents.len());
    let mut fresh_owner_rows = Vec::new();
    for (content, state_edit) in contents.iter().zip(&state_edits) {
        let mut generation = None::<u64>;
        let mut parent_object_ids = Vec::with_capacity(content.draft.parent_commit_ids.len());
        let mut first_parent = None::<(ObjectId, CommitObjectV1)>;
        for (parent_index, parent_id) in content.draft.parent_commit_ids.iter().enumerate() {
            let (parent_object_id, parent) =
                if let Some((object_id, commit)) = staged_commits.get(parent_id) {
                    (*object_id, commit.clone())
                } else {
                    let commit = load_commit(view, forktree_commit_id(*parent_id))
                        .await?
                        .ok_or_else(|| writer_error("ordered history parent is absent"))?;
                    let (object_id, _) = commit.encode()?;
                    (object_id, commit)
                };
            if parent_index == 0 {
                first_parent = Some((parent_object_id, parent.clone()));
            }
            parent_object_ids.push(parent_object_id);
            generation =
                Some(generation.map_or(parent.generation, |value| value.max(parent.generation)));
        }
        let generation =
            next_ordered_commit_generation(generation, content.max_selected_source_generation)?;
        let global_state_root = if global {
            state_edit.root
        } else {
            sequence_global_root
        };
        let local_state_root = if global {
            sequence_local_root
        } else {
            state_edit.root
        };
        let record = CommitRecord {
            format_version: 2,
            commit_id: content.draft.commit_id,
            generation,
            parent_commit_ids: content.draft.parent_commit_ids.clone(),
            change_id: content.draft.commit_change_id,
            account_id: active_account_id.to_string(),
            created_at: content.draft.created_at,
        };
        let (first_parent_object_id, first_parent) = first_parent
            .ok_or_else(|| writer_error("ordered history commit has no first parent"))?;
        let checkpoint_cursor = CheckpointCursorV1::after_first_parent(
            first_parent_object_id,
            &first_parent,
            view.branch_id(),
            introduced_checkpoint_marker(&content.members, view.branch_id())?,
        )?;
        let mut commit = CommitObjectV1 {
            commit_id: forktree_commit_id(content.draft.commit_id),
            generation,
            parent_commit_object_ids: parent_object_ids,
            members: content.members.clone(),
            member_page_object_ids: content.member_page_object_ids.clone(),
            global_state_root,
            local_state_root,
            checkpoint_cursor,
            metadata: crate::changelog::encode_forktree_commit_payload(&record)?,
        };
        let _member_pages = commit.prepare_member_pages()?;
        let (commit_object_id, _) = commit.encode()?;
        staged_commits.insert(content.draft.commit_id, (commit_object_id, commit.clone()));
        commit_entries.push((commit.commit_id, CommitCatalogEntry { commit_object_id }));
        for (ordinal, member) in content.members.iter().enumerate() {
            if member.source().is_none() {
                fresh_owner_rows.push((
                    member.change_id(),
                    ChangeCatalogEntry {
                        owner: ChangeCatalogOwner::CommitMember {
                            commit_object_id,
                            ordinal: u32::try_from(ordinal)
                                .map_err(|_| writer_error("ordered member ordinal exceeds u32"))?,
                        },
                    },
                ));
            }
        }
        semantic_commits.push(commit);
    }
    commit_entries.sort_unstable_by_key(|(commit_id, _)| *commit_id);
    let commit_catalog_edit = view
        .put_commit_catalog_entries(view.repository_root().commit_catalog_root, &commit_entries)
        .await?;

    let final_content = contents.last().expect("ordered history is nonempty");
    debug_assert!(final_content.draft.publish_head);
    let final_commit = semantic_commits
        .last()
        .expect("ordered history is nonempty");
    let (final_commit_object_id, _) = final_commit.encode()?;
    let final_global_state_root = final_commit.global_state_root;
    let final_local_state_root = final_commit.local_state_root;
    let ref_payload = crate::changelog::encode_forktree_change_payload(&ChangeRecord {
        format_version: 2,
        change_id: final_content.draft.branch_ref_change_id,
        account_id: active_account_id.to_string(),
        schema_key: crate::branch::BRANCH_REF_SCHEMA_KEY.to_string(),
        entity_pk: EntityPk::uuid_from_canonical(&branch_id).map_err(|error| {
            writer_error(format!("transaction branch identity is invalid: {error}"))
        })?,
        file_id: None,
        snapshot: JsonSlot::Inline(
            serde_json::json!({
                "branch_id": branch_id,
                "commit_id": final_content.draft.commit_id.to_string(),
            })
            .to_string()
            .into(),
        ),
        metadata: JsonSlot::None,
        created_at: final_content.draft.created_at,
        origin_key: None,
    })?;
    let branch_ref_change = ChangeObjectV1::BranchRef {
        change_id: forktree_change_id(final_content.draft.branch_ref_change_id),
        updated_at: final_content.draft.created_at,
        branch_id: view.branch_id(),
        before_semantic_head_commit_object_id: Some(
            view.branch_snapshot().semantic_head_commit_object_id,
        ),
        after_semantic_head_commit_object_id: Some(final_commit_object_id),
        previous_ref_change_object_id: view.branch_snapshot().latest_ref_change_object_id,
        payload: ref_payload,
        json_payload_object_ids: Vec::new(),
    };
    let (ref_object_id, _) = branch_ref_change.encode()?;
    fresh_owner_rows.push((
        branch_ref_change.change_id(),
        ChangeCatalogEntry {
            owner: ChangeCatalogOwner::BranchRef {
                ref_change_object_id: ref_object_id,
                branch_id: view.branch_id(),
            },
        },
    ));
    let fresh_owner_rows = catalog_order
        .into_iter()
        .map(|index| fresh_owner_rows[index])
        .collect::<Vec<_>>();
    let change_catalog_edit = view
        .put_change_catalog_entries(
            view.repository_root().change_catalog_root,
            &fresh_owner_rows,
        )
        .await?;
    let repository_root = RepositoryRootV1 {
        global_state_root: final_global_state_root,
        commit_catalog_root: commit_catalog_edit.root,
        change_catalog_root: change_catalog_edit.root,
    };
    let transition = OrderedBranchHistoryTransition {
        state_edits,
        state_domain_global: state_domain.unwrap_or(false),
        commit_catalog_edit,
        change_catalog_edit,
        semantic_commits,
        fresh_changes: Vec::new(),
        branch_ref_change,
        branch_snapshot: BranchSnapshotV1 {
            branch_id: view.branch_id(),
            local_state_root: final_local_state_root,
            semantic_head_commit_object_id: final_commit_object_id,
            latest_ref_change_object_id: Some(ref_object_id),
            historical_global_state_root: final_global_state_root,
        },
        repository_root,
        selected_history: selected_batch,
    };
    publication
        .publish_ordered_branch_history(view, transition)
        .await?;
    Ok(PreparedForkTreePlan::Publication(publication))
}

fn classify_publication_intent(
    prepared: &PreparedWriteSet,
    runtime_checkpoint: Option<RuntimeSequenceCheckpoint>,
) -> Result<PublicationIntent, LixError> {
    // Commit intent is independent of current-state row count. Inspect every
    // semantic owner before opening a view or constructing a publication, so
    // an unsupported ref/history cohort cannot publish unrelated untracked
    // state or rotate the global epoch while silently dropping its commit.
    for refs in prepared.commit_change_refs_by_branch.values().chain(
        prepared
            .intermediate_commits
            .iter()
            .map(|commit| &commit.change_refs),
    ) {
        if refs.ordered_mutation_journal().is_some() {
            return Err(writer_error(
                "immutable mutation journals require the ForkTree bulk lowering slice",
            ));
        }
    }

    let mut expected_commits = BTreeMap::<CommitId, (&str, usize, bool)>::new();
    for (branch_id, refs) in &prepared.commit_change_refs_by_branch {
        if expected_commits
            .insert(
                refs.commit_id,
                (
                    branch_id.as_str(),
                    refs.tracked_change_count,
                    refs.has_selected_changes(),
                ),
            )
            .is_some()
        {
            return Err(writer_error("semantic commit intent repeats one CommitId"));
        }
    }
    for intermediate in &prepared.intermediate_commits {
        let refs = &intermediate.change_refs;
        if expected_commits
            .insert(
                refs.commit_id,
                (
                    intermediate.branch_id.as_str(),
                    refs.tracked_change_count,
                    refs.has_selected_changes(),
                ),
            )
            .is_some()
        {
            return Err(writer_error("semantic commit intent repeats one CommitId"));
        }
    }
    let mut tracked_rows_by_commit = BTreeMap::<CommitId, usize>::new();
    for row in prepared.state_rows.iter().filter(|row| !row.untracked) {
        let commit_id = row
            .commit_id
            .ok_or_else(|| writer_error("tracked row has no semantic CommitId"))?;
        let Some((branch_id, _, _)) = expected_commits.get(&commit_id) else {
            return Err(writer_error(
                "tracked row is missing its semantic commit owner",
            ));
        };
        if row.branch_id.as_str() != *branch_id {
            return Err(writer_error(
                "tracked row branch differs from its semantic commit owner",
            ));
        }
        *tracked_rows_by_commit.entry(commit_id).or_default() += 1;
    }
    for (commit_id, (_, expected_rows, has_selected)) in &expected_commits {
        let tracked_rows = tracked_rows_by_commit
            .get(commit_id)
            .copied()
            .unwrap_or_default();
        if tracked_rows == 0 {
            if *expected_rows != 0 {
                return Err(writer_error(if *has_selected {
                    "selected historical commit has missing fresh state rows"
                } else {
                    "ref-only commit intent has unexpected tracked state rows"
                }));
            }
            // A zero-row commit is valid when its authenticated members are
            // selected from history, or when it is an empty/ref-only merge.
            // Both are lowered by the ordered ForkTree publisher below; do
            // not reject them before the retained read can authenticate the
            // source members and parent topology.
        }
        if *expected_rows != tracked_rows {
            return Err(writer_error(
                "tracked row count differs from its semantic commit membership",
            ));
        }
    }
    if !prepared.extra_commit_parents_by_branch.is_empty()
        && prepared.commit_change_refs_by_branch.is_empty()
    {
        return Err(writer_error(
            "extra parent intent is missing its semantic commit owner",
        ));
    }

    let has_state_rows = !prepared.state_rows.is_empty();
    let has_branch_ref_intent = !prepared.branch_ref_intents.is_empty();
    let has_commit_intent = !prepared.commit_change_refs_by_branch.is_empty()
        || !prepared.extra_commit_parents_by_branch.is_empty()
        || !prepared.intermediate_commits.is_empty()
        || !prepared.first_commit_parent_override_by_branch.is_empty();
    if !has_state_rows && !has_commit_intent && !has_branch_ref_intent {
        return match runtime_checkpoint {
            None => Ok(PublicationIntent::Noop),
            Some(_) => Ok(PublicationIntent::Ordinary {
                branch_id: canonical_branch_id(crate::GLOBAL_BRANCH_ID)?,
                semantic_commit: false,
            }),
        };
    }

    let branch_id = sole_publication_branch(prepared, runtime_checkpoint.is_some())?;
    Ok(PublicationIntent::Ordinary {
        branch_id: canonical_branch_id(&branch_id)?,
        semantic_commit: has_commit_intent,
    })
}

#[derive(Clone, Copy)]
struct PreparedBlobManifest {
    object_id: ObjectId,
    canonical_blob_id: crate::binary_cas::BlobId,
    logical_bytes: u64,
}

type PreparedBlobManifestMap = BTreeMap<(String, String, bool, bool), PreparedBlobManifest>;

async fn prepared_blob_manifest_ids<R>(
    publication: &mut PreparedPublication,
    view: &crate::forktree::CoherentView<R>,
    prepared: &PreparedWriteSet,
) -> Result<PreparedBlobManifestMap, LixError>
where
    R: StorageAdapterRead + Sync,
{
    let mut manifests = PreparedBlobManifestMap::new();
    for write in &prepared.file_content_writes {
        // Empty content is a deliberate BlobRef tombstone (or an empty
        // insert with no BlobRef row). It has no final manifest owner to
        // validate; non-empty payloads remain required to match their final
        // coalesced authenticated BlobRef below.
        if write.is_empty() {
            continue;
        }
        let manifest = if let Some(receipt) = write.prepared_cas_receipt() {
            ObjectId::from_bytes(receipt.manifest_object_id)
        } else if let Some(payload) = write.inline_payload() {
            if let Some(splice) = write.same_length_blob_splice() {
                if write.untracked {
                    return Err(writer_error(
                        "verified blob splice cannot target an untracked file owner",
                    ));
                }
                let file_id = &write.file_id;
                let state_key = StateKey {
                    schema_key: "lix_binary_blob_ref".to_owned(),
                    file_id: Some(file_id.clone()),
                    entity_pk: EntityPk::uuid_from_canonical(file_id).map_err(|error| {
                        writer_error(format!(
                            "verified blob splice file identity is not a canonical UUID: {error}"
                        ))
                    })?,
                };
                publication
                    .stage_verified_inline_blob_splice(view, &state_key, payload, splice)
                    .await
                    .map_err(LixError::from)?
            } else if let Some(splice) = write.edit_blob_splice() {
                if write.untracked {
                    return Err(writer_error(
                        "verified blob edit cannot target an untracked file owner",
                    ));
                }
                let file_id = &write.file_id;
                let state_key = StateKey {
                    schema_key: "lix_binary_blob_ref".to_owned(),
                    file_id: Some(file_id.clone()),
                    entity_pk: EntityPk::uuid_from_canonical(file_id).map_err(|error| {
                        writer_error(format!(
                            "verified blob edit file identity is not a canonical UUID: {error}"
                        ))
                    })?,
                };
                publication
                    .stage_verified_inline_blob_edit(view, &state_key, payload, splice)
                    .await
                    .map_err(LixError::from)?
            } else if let Some(provenance) = write.splice_provenance() {
                if write.untracked {
                    return Err(writer_error(
                        "verified request blob splice cannot target an untracked file owner",
                    ));
                }
                let file_id = &write.file_id;
                let state_key = StateKey {
                    schema_key: "lix_binary_blob_ref".to_owned(),
                    file_id: Some(file_id.clone()),
                    entity_pk: EntityPk::uuid_from_canonical(file_id).map_err(|error| {
                        writer_error(format!(
                            "verified request blob splice file identity is not a canonical UUID: {error}"
                        ))
                    })?,
                };
                publication
                    .stage_verified_request_blob_splice(view, &state_key, payload, provenance)
                    .await
                    .map_err(LixError::from)?
            } else {
                publication
                    .stage_inline_blob_payload(payload.bytes())
                    .map_err(LixError::from)?
            }
        } else {
            return Err(writer_error(
                "file payload is missing an authenticated ForkTree blob representation",
            ));
        };
        if manifest == ObjectId::ZERO {
            return Err(writer_error(
                "file payload has a zero ForkTree manifest identity",
            ));
        }
        let manifest_value = match publication.staged_blob_manifest(manifest)? {
            Some(value) => value,
            None => {
                let bytes = view.load_object_bytes(manifest).await?;
                crate::forktree::BlobManifestV1::decode(manifest, &bytes)?
            }
        };
        let prepared_manifest = PreparedBlobManifest {
            object_id: manifest,
            canonical_blob_id: manifest_value.canonical_blob_id,
            logical_bytes: manifest_value.logical_bytes,
        };
        let key = (
            write.branch_id.clone(),
            write.file_id.clone(),
            write.global,
            write.untracked,
        );
        // File content writes are ordered within one transaction.  A staged
        // INSERT followed by UPDATE for the same owner intentionally carries
        // two manifests here, while the coalesced state row retains only the
        // final BlobRef.  Keep the last authenticated manifest for that
        // owner; rejecting the earlier superseded payload would make
        // write-your-own-writes fail before publication.
        manifests.insert(key, prepared_manifest);
    }
    validate_final_prepared_blob_manifests(prepared, &manifests)?;
    Ok(manifests)
}

fn validate_final_prepared_blob_manifests(
    prepared: &PreparedWriteSet,
    manifests: &PreparedBlobManifestMap,
) -> Result<(), LixError> {
    let mut final_owners = BTreeMap::new();
    for row in prepared.state_rows.iter() {
        if row.schema_key.as_str() != "lix_binary_blob_ref" {
            continue;
        }
        let file_id = row
            .file_id
            .ok_or_else(|| writer_error("blob-ref state row has no file identity"))?;
        let key = (
            row.branch_id.to_string(),
            file_id.to_string(),
            row.global,
            row.untracked,
        );
        if !manifests.contains_key(&key) {
            continue;
        }
        let snapshot = row.snapshot.ok_or_else(|| {
            writer_error("final blob-ref owner has no authenticated semantic snapshot")
        })?;
        let value: serde_json::Value =
            serde_json::from_str(snapshot.normalized()).map_err(|error| {
                writer_error(format!(
                    "final blob-ref owner snapshot is malformed: {error}"
                ))
            })?;
        let blob_id = value
            .get("blob_hash")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| writer_error("final blob-ref owner has no blob_hash"))
            .and_then(crate::binary_cas::BlobId::from_hex)?;
        let logical_bytes = value
            .get("size_bytes")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| writer_error("final blob-ref owner has no size_bytes"))?;
        if final_owners
            .insert(key.clone(), (blob_id, logical_bytes))
            .is_some()
        {
            return Err(writer_error(
                "prepared blob writes contain duplicate final owner rows",
            ));
        }
    }

    for (key, manifest) in manifests {
        let (blob_id, logical_bytes) = final_owners.get(key).copied().ok_or_else(|| {
            writer_error(
                "ordered file content writes have no matching final coalesced BlobRef owner",
            )
        })?;
        validate_final_manifest_owner(*manifest, blob_id, logical_bytes)?;
    }
    Ok(())
}

fn validate_final_manifest_owner(
    manifest: PreparedBlobManifest,
    final_blob_id: crate::binary_cas::BlobId,
    final_logical_bytes: u64,
) -> Result<(), LixError> {
    if manifest.canonical_blob_id != final_blob_id || manifest.logical_bytes != final_logical_bytes
    {
        return Err(writer_error(
            "final coalesced BlobRef owner does not match its authenticated manifest",
        ));
    }
    Ok(())
}

fn blob_manifest_object_ids_for_row(
    row: PreparedStateRowRef<'_>,
    manifests: &PreparedBlobManifestMap,
    historical_edges: &HistoricalBlobManifestEdges,
) -> Result<Vec<ObjectId>, LixError> {
    if row.schema_key.as_str() != "lix_binary_blob_ref" {
        return Ok(Vec::new());
    }
    if row.snapshot.is_none() {
        return Ok(Vec::new());
    }
    let file_id = row
        .file_id
        .ok_or_else(|| writer_error("blob-ref state row has no file identity"))?;
    let key = (
        row.branch_id.to_string(),
        file_id.to_string(),
        row.global,
        row.untracked,
    );
    if let Some(manifest) = manifests.get(&key).copied() {
        return Ok(vec![manifest.object_id]);
    }
    let state_key = StateKey {
        schema_key: row.schema_key.to_string(),
        file_id: Some(file_id.to_string()),
        entity_pk: row.entity_pk.clone(),
    };
    let owner = if row.global {
        crate::GLOBAL_BRANCH_ID
    } else {
        row.branch_id.as_str()
    };
    let edge = historical_edges
        .get(&(owner.to_owned(), state_key))
        .ok_or_else(|| {
            writer_error("blob-ref state row has no matching prepared ForkTree manifest")
        })?;
    if edge.is_empty() {
        return Err(writer_error(
            "historical blob-ref state row has an empty ForkTree manifest edge",
        ));
    }
    Ok(edge.clone())
}

fn sole_publication_branch(
    prepared: &PreparedWriteSet,
    runtime_checkpoint_present: bool,
) -> Result<String, LixError> {
    let branches = publication_owner_branch_ids(prepared, runtime_checkpoint_present);
    branches
        .iter()
        .find(|branch_id| branch_id.as_str() != crate::GLOBAL_BRANCH_ID)
        .cloned()
        .or_else(|| branches.iter().next().cloned())
        .ok_or_else(|| writer_error("prepared publication has no branch owner"))
}

fn publication_owner_branch_ids(
    prepared: &PreparedWriteSet,
    runtime_checkpoint_present: bool,
) -> BTreeSet<String> {
    let mut branches = prepared
        .state_rows
        .iter()
        .map(|row| {
            if row.untracked && row.global {
                crate::GLOBAL_BRANCH_ID.to_owned()
            } else {
                row.branch_id.to_string()
            }
        })
        .chain(
            prepared
                .file_content_writes
                .iter()
                .map(|write| write.branch_id.clone()),
        )
        .chain(prepared.commit_change_refs_by_branch.keys().cloned())
        .chain(prepared.extra_commit_parents_by_branch.keys().cloned())
        .chain(
            prepared
                .first_commit_parent_override_by_branch
                .keys()
                .cloned(),
        )
        .chain(
            prepared
                .intermediate_commits
                .iter()
                .map(|commit| commit.branch_id.clone()),
        )
        .collect::<BTreeSet<_>>();
    if branches.is_empty() {
        branches.extend(
            prepared
                .branch_ref_intents
                .iter()
                .map(|intent| intent.branch_id.clone()),
        );
    }
    if branches.is_empty() && runtime_checkpoint_present {
        branches.insert(crate::GLOBAL_BRANCH_ID.to_owned());
    }
    branches
}

fn deterministic_sequence_snapshot(highest_seen: i64) -> Result<String, LixError> {
    serde_json::to_string(&serde_json::json!({
        "key": crate::functions::DETERMINISTIC_SEQUENCE_KEY,
        "value": highest_seen,
    }))
    .map_err(|error| {
        writer_error(format!(
            "failed to serialize deterministic sequence checkpoint: {error}"
        ))
    })
}

fn canonical_branch_id(value: &str) -> Result<CanonicalBranchId, LixError> {
    let value = uuid::Uuid::parse_str(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("transaction branch id is not canonical: {error}"),
        )
    })?;
    Ok(CanonicalBranchId::from_bytes(*value.as_bytes()))
}

fn forktree_commit_id(value: CommitId) -> ForkTreeCommitId {
    ForkTreeCommitId::from_bytes(*value.as_uuid().as_bytes())
}

fn forktree_change_id(value: crate::changelog::ChangeId) -> ForkTreeChangeId {
    ForkTreeChangeId::from_bytes(*value.as_uuid().as_bytes())
}

fn writer_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message.into())
}

fn collection_delete_range(
    row: PreparedStateRowRef<'_>,
) -> Result<Option<(Vec<u8>, Option<Vec<u8>>)>, LixError> {
    if row.schema_key.as_str() != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
        || row.global
        || row.untracked
    {
        return Ok(None);
    }
    let Some(snapshot) = row.snapshot else {
        return Ok(None);
    };
    let value: serde_json::Value =
        serde_json::from_str(snapshot.normalized()).map_err(|error| {
            writer_error(format!(
                "collection-generation marker snapshot is malformed: {error}"
            ))
        })?;
    if value.get("live_count").and_then(serde_json::Value::as_u64) != Some(0) {
        return Ok(None);
    }
    let (schema_key, file_id) =
        crate::collection_generation::collection_scope_from_entity_pk(row.entity_pk)?;
    if file_id.is_some() {
        return Ok(None);
    }
    let bounds = encode_state_entity_prefix_bounds(&schema_key, &EntityPk::empty());
    Ok(Some((bounds.lower, bounds.upper)))
}

fn state_mutation_key(mutation: &StateTreeMutation) -> &[u8] {
    match mutation {
        StateTreeMutation::Insert { key, .. }
        | StateTreeMutation::Update { key, .. }
        | StateTreeMutation::Remove { key } => key.as_slice(),
        StateTreeMutation::RemoveRange { lower, .. } => lower.as_slice(),
    }
}

fn sort_state_mutations(mutations: &mut Vec<StateTreeMutation>) -> Result<(), LixError> {
    mutations
        .sort_unstable_by(|left, right| state_mutation_key(left).cmp(state_mutation_key(right)));
    if mutations
        .windows(2)
        .any(|pair| state_mutation_key(&pair[0]) == state_mutation_key(&pair[1]))
    {
        return Err(writer_error(
            "authenticated state mutations contain duplicate encoded keys",
        ));
    }
    Ok(())
}

fn canonical_change_order(ids: &[ForkTreeChangeId]) -> Result<Vec<usize>, LixError> {
    let mut ordered = BTreeMap::new();
    for (index, id) in ids.iter().copied().enumerate() {
        if ordered.insert(id, index).is_some() {
            let duplicate = uuid::Uuid::from_bytes(*id.as_bytes());
            return Err(writer_error(format!(
                "publication contains duplicate semantic ChangeId {duplicate}"
            )));
        }
    }
    Ok(ordered.into_values().collect())
}

fn next_ordered_commit_generation(
    parent_generation: Option<u64>,
    selected_source_generation: Option<u64>,
) -> Result<u64, LixError> {
    parent_generation
        .into_iter()
        .chain(selected_source_generation)
        .max()
        .map_or(Ok(0), |generation| {
            generation
                .checked_add(1)
                .ok_or_else(|| writer_error("ordered history generation overflows u64"))
        })
}

#[cfg(test)]
mod intent_tests {
    use super::*;
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::forktree::StateKey;
    use crate::transaction::staging::{PreparedInsertSelection, PreparedWriteSet};
    use crate::transaction::types::{
        FileContent, PreparedStateBatch, StagedCommitChangeBatchBuilder, StagedCommitChangeRefs,
        TransactionFileContent,
    };

    fn empty_writes() -> PreparedWriteSet {
        PreparedWriteSet {
            state_rows: PreparedStateBatch::new(),
            insert_selection: PreparedInsertSelection::new(),
            commit_change_refs_by_branch: BTreeMap::new(),
            first_commit_parent_override_by_branch: BTreeMap::new(),
            checkpoint_publications: Vec::new(),
            extra_commit_parents_by_branch: BTreeMap::new(),
            intermediate_commits: Vec::new(),
            file_content_writes: Vec::new(),
            branch_ref_intents: Vec::new(),
            historical_blob_manifest_edges: BTreeMap::new(),
        }
    }

    #[test]
    fn genuine_empty_and_unadvanced_runtime_are_noop_intent() {
        assert_eq!(
            classify_publication_intent(&empty_writes(), None).expect("empty intent"),
            PublicationIntent::Noop
        );
    }

    #[test]
    fn advanced_runtime_is_global_publication_intent() {
        let checkpoint = Some((
            7,
            LixTimestamp::from_unix_millis_utc_lossy(7),
            crate::changelog::ChangeId::for_test_label("runtime-7"),
        ));
        assert_eq!(
            classify_publication_intent(&empty_writes(), checkpoint).expect("runtime intent"),
            PublicationIntent::Ordinary {
                branch_id: canonical_branch_id(crate::GLOBAL_BRANCH_ID)
                    .expect("canonical global branch"),
                semantic_commit: false,
            }
        );
        assert_eq!(
            deterministic_sequence_snapshot(7).expect("sequence snapshot"),
            r#"{"key":"lix_deterministic_sequence_number","value":7}"#
        );
    }

    #[test]
    fn inline_file_payload_is_not_rejected_before_forktree_lowering() {
        let mut writes = empty_writes();
        writes.file_content_writes.push(TransactionFileContent::new(
            "inline-file".to_owned(),
            Some("inline.txt".to_owned()),
            Some("inline.txt".to_owned()),
            crate::GLOBAL_BRANCH_ID.to_owned(),
            false,
            false,
            FileContent::inline(b"inline payload".to_vec()),
        ));
        assert_eq!(
            classify_publication_intent(&writes, None).expect("inline payload intent"),
            PublicationIntent::Noop
        );
    }

    #[test]
    fn ordered_insert_then_update_uses_final_manifest_and_rejects_mismatch() {
        let key = (
            crate::GLOBAL_BRANCH_ID.to_owned(),
            "01920000-0000-7000-8000-000000000551".to_owned(),
            false,
            false,
        );
        let first = PreparedBlobManifest {
            object_id: ObjectId::from_bytes([0x11; 32]),
            canonical_blob_id: crate::binary_cas::BlobId::from_bytes([0x21; 32]),
            logical_bytes: 3,
        };
        let final_manifest = PreparedBlobManifest {
            object_id: ObjectId::from_bytes([0x12; 32]),
            canonical_blob_id: crate::binary_cas::BlobId::from_bytes([0x22; 32]),
            logical_bytes: 7,
        };
        let mut manifests = PreparedBlobManifestMap::new();
        manifests.insert(key.clone(), first);
        manifests.insert(key.clone(), final_manifest);
        let selected = manifests.get(&key).copied().expect("final manifest");
        assert_eq!(selected.object_id, final_manifest.object_id);
        validate_final_manifest_owner(
            selected,
            final_manifest.canonical_blob_id,
            final_manifest.logical_bytes,
        )
        .expect("ordered update should match the final coalesced BlobRef");
        assert!(
            validate_final_manifest_owner(selected, first.canonical_blob_id, first.logical_bytes,)
                .is_err()
        );
    }

    #[test]
    fn ref_only_publication_and_unsupported_history_boundaries() {
        let mut ref_only = empty_writes();
        ref_only.commit_change_refs_by_branch.insert(
            crate::GLOBAL_BRANCH_ID.to_string(),
            StagedCommitChangeRefs::default(),
        );
        assert_eq!(
            classify_publication_intent(&ref_only, None).expect("ref-only intent is lowered"),
            PublicationIntent::Ordinary {
                branch_id: canonical_branch_id(crate::GLOBAL_BRANCH_ID)
                    .expect("canonical global branch"),
                semantic_commit: true,
            }
        );

        let mut selected = empty_writes();
        let mut refs = StagedCommitChangeRefs::default();
        let mut batch = StagedCommitChangeBatchBuilder::with_capacity(1);
        batch.push(
            StateKey {
                schema_key: "app.row".to_string(),
                file_id: None,
                entity_pk: EntityPk::single("selected"),
            },
            CommitId::for_test_label("selected-source"),
            crate::changelog::ChangeId::for_test_label("selected-change"),
            false,
            LixTimestamp::from_unix_millis_utc_lossy(1),
            LixTimestamp::from_unix_millis_utc_lossy(2),
        );
        refs.add_selected_change_batch(batch.finish());
        selected
            .commit_change_refs_by_branch
            .insert(crate::GLOBAL_BRANCH_ID.to_string(), refs);
        assert_eq!(
            classify_publication_intent(&selected, None)
                .expect("selected history is lowered by the ordered publisher"),
            PublicationIntent::Ordinary {
                branch_id: canonical_branch_id(crate::GLOBAL_BRANCH_ID)
                    .expect("canonical global branch"),
                semantic_commit: true,
            }
        );

        let mut unsupported = empty_writes();
        unsupported.extra_commit_parents_by_branch.insert(
            crate::GLOBAL_BRANCH_ID.to_string(),
            vec![CommitId::for_test_label("unsupported-parent")],
        );
        let error = classify_publication_intent(&unsupported, None)
            .expect_err("parent-only history must fail before publication");
        assert!(
            error
                .message
                .contains("extra parent intent is missing its semantic commit owner")
        );
    }

    #[test]
    fn ordered_history_generation_advances_past_selected_source() {
        assert_eq!(
            next_ordered_commit_generation(Some(1), Some(2)).expect("generation"),
            3
        );
        assert_eq!(
            next_ordered_commit_generation(Some(2), None).expect("generation"),
            3
        );
        assert!(next_ordered_commit_generation(Some(u64::MAX), None).is_err());
    }

    #[test]
    fn state_mutations_are_sorted_by_encoded_key_before_forktree_edit() {
        let mut mutations = vec![
            StateTreeMutation::remove(b"state/z".to_vec()),
            StateTreeMutation::remove(b"state/a".to_vec()),
        ];
        sort_state_mutations(&mut mutations).expect("out-of-order mutations should be sorted");
        assert_eq!(state_mutation_key(&mutations[0]), b"state/a");
        assert_eq!(state_mutation_key(&mutations[1]), b"state/z");
    }

    #[test]
    fn state_mutations_reject_duplicate_encoded_keys() {
        let mut mutations = vec![
            StateTreeMutation::remove(b"state/a".to_vec()),
            StateTreeMutation::update(b"state/a".to_vec(), b"replacement".to_vec()),
        ];
        let error = sort_state_mutations(&mut mutations)
            .expect_err("duplicate authenticated state keys must fail closed");
        assert!(error.message.contains("duplicate encoded keys"));
    }

    #[test]
    fn ordered_history_mutation_batches_sort_each_tree_edit_input() {
        let mut batches = vec![
            vec![
                StateTreeMutation::remove(b"state/z".to_vec()),
                StateTreeMutation::remove(b"state/a".to_vec()),
            ],
            vec![
                StateTreeMutation::remove(b"state/y".to_vec()),
                StateTreeMutation::remove(b"state/b".to_vec()),
            ],
        ];
        for batch in &mut batches {
            sort_state_mutations(batch).expect("each ordered edit input should be sorted");
        }
        assert_eq!(state_mutation_key(&batches[0][0]), b"state/a");
        assert_eq!(state_mutation_key(&batches[0][1]), b"state/z");
        assert_eq!(state_mutation_key(&batches[1][0]), b"state/b");
        assert_eq!(state_mutation_key(&batches[1][1]), b"state/y");
    }

    #[test]
    fn ordered_history_mutation_batches_reject_duplicate_encoded_keys() {
        let mut batches = vec![vec![
            StateTreeMutation::remove(b"state/a".to_vec()),
            StateTreeMutation::insert(b"state/a".to_vec(), b"replacement".to_vec()),
        ]];
        let error = sort_state_mutations(&mut batches[0])
            .expect_err("ordered edit input duplicates must fail closed");
        assert!(error.message.contains("duplicate encoded keys"));
    }

    #[test]
    fn canonical_catalog_order_preserves_slots_and_rejects_duplicate_ids() {
        let ids = [
            ForkTreeChangeId::from_bytes([3; 16]),
            ForkTreeChangeId::from_bytes([1; 16]),
            ForkTreeChangeId::from_bytes([2; 16]),
        ];
        assert_eq!(
            canonical_change_order(&ids).expect("canonical order"),
            [1, 2, 0]
        );

        let duplicate = [
            ForkTreeChangeId::from_bytes([1; 16]),
            ForkTreeChangeId::from_bytes([1; 16]),
        ];
        let error = canonical_change_order(&duplicate).expect_err("duplicate ids must fail closed");
        assert!(
            error
                .message
                .contains("01010101-0101-0101-0101-010101010101")
        );
    }
}
