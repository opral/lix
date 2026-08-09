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
use crate::branch::{BranchContext, BranchRefReader};
use crate::changelog::{ChangeRecord, CommitId, CommitRecord};
use crate::common::LixTimestamp;
use crate::json_store::JsonSlot;
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition, StorageWriteSet};
use crate::transaction::staging::{BranchRefPublicationIntent, PreparedWriteSet};
use crate::transaction::types::{PreparedStateRowRef, StageJson};

use crate::forktree::{
    BranchSnapshotV1, BranchStateTransition, CanonicalBranchId, ChangeCatalogEntry,
    ChangeCatalogOwner, ChangeId as ForkTreeChangeId, ChangeObjectV1, CommitCatalogEntry,
    CommitId as ForkTreeCommitId, CommitMemberV1, CommitObjectV1, ObjectId,
    OrderedBranchHistoryTransition, PreparedPublication, RepositoryRootV1, StateCellRef,
    StateKeyRef, StateSource, StateTreeMutation, StateValueRef, UntrackedValueRef,
    encode_state_key, encode_state_value, load_commit, load_commit_summary,
    open_coherent_view_on_read, select_historical_commit_member, state_point, state_points,
};

#[cfg(test)]
pub(crate) fn take_ordered_packed_current_base_publications() -> usize {
    0
}

#[cfg(test)]
pub(crate) fn take_certified_columnar_current_base_publications() -> usize {
    0
}

#[cfg(test)]
pub(crate) fn take_complete_replacement_packed_current_base_publications() -> usize {
    0
}

#[cfg(test)]
pub(crate) fn take_complete_replacement_packed_current_base_retirements() -> usize {
    0
}

#[cfg(test)]
pub(crate) fn take_rootless_replacement_generation_publications() -> usize {
    0
}

#[cfg(test)]
pub(crate) fn take_direct_journal_replacement_publications(_schema_key: &str) -> usize {
    0
}

pub(crate) type RuntimeSequenceCheckpoint = (i64, LixTimestamp, crate::changelog::ChangeId);

/// Converts transaction JSON into the representation owned by a ForkTree
/// change. Small values retain their inline semantics; large values are
/// staged as authenticated immutable objects before the change envelope is
/// encoded. A ForkTree change never persists a legacy JsonRef.
fn stage_forktree_json_slot(
    publication: &mut PreparedPublication,
    value: Option<&StageJson>,
) -> Result<JsonSlot, LixError> {
    let Some(value) = value else {
        return Ok(JsonSlot::None);
    };
    if value.is_inline() {
        return Ok(JsonSlot::Inline(value.normalized().into()));
    }
    let object_id = publication
        .stage_json_payload(value.normalized())
        .map_err(LixError::from)?;
    Ok(JsonSlot::ForkTreeObject(*object_id.as_bytes()))
}

fn json_payload_object_ids(
    snapshot: &JsonSlot,
    metadata: &JsonSlot,
) -> Result<Vec<ObjectId>, LixError> {
    let mut ids = Vec::new();
    for slot in [snapshot, metadata] {
        match slot {
            JsonSlot::ForkTreeObject(bytes) => ids.push(ObjectId::from_bytes(*bytes)),
            JsonSlot::Ref(_) => {
                return Err(writer_error(
                    "ForkTree change contains an unlowered JSON side-plane reference",
                ));
            }
            JsonSlot::None | JsonSlot::Inline(_) => {}
        }
    }
    Ok(ids)
}

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
        .chain(
            prepared_writes
                .branch_ref_intents
                .iter()
                .filter(|intent| !intent.create)
                .map(|intent| intent.branch_id.as_str()),
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
    prepared_writes: PreparedWriteSet,
) -> Result<PreparedForkTreePlan, LixError>
where
    R: StorageAdapterRead + Clone,
{
    let intent = classify_publication_intent(&prepared_writes, runtime_checkpoint)?;
    let PublicationIntent::Ordinary {
        branch_id: publication_branch_id,
        semantic_commit,
    } = intent
    else {
        return Ok(PreparedForkTreePlan::Noop);
    };
    let branch_id = sole_publication_branch(&prepared_writes, runtime_checkpoint.is_some())?;
    let view = open_coherent_view_on_read(read, publication_branch_id).await?;
    let mut publication = PreparedPublication::from_branch_view(&view)?;
    let prepared_blob_manifests = prepared_blob_manifest_ids(&mut publication, &prepared_writes)?;
    let branch_ref_intents = prepared_writes.branch_ref_intents.clone();

    for checkpoint in &prepared_writes.checkpoint_publications {
        crate::gc::stage_checkpoint_publication(&mut publication, checkpoint)?;
    }

    let runtime_entity_pk = runtime_checkpoint
        .map(|_| crate::entity_pk::EntityPk::single(crate::functions::DETERMINISTIC_SEQUENCE_KEY));

    for row in prepared_writes
        .state_rows
        .iter()
        .filter(|row| row.untracked)
    {
        if runtime_entity_pk.as_ref().is_some_and(|entity_pk| {
            row.branch_id.as_str() == crate::GLOBAL_BRANCH_ID
                && row.schema_key.as_str() == "lix_key_value"
                && row.file_id.is_none()
                && row.entity_pk == entity_pk
        }) {
            // The engine-owned sequence checkpoint is derived after statement
            // rollback/savepoint handling. It therefore supersedes a staged
            // user row at the same protected identity, matching the previous
            // materializer without creating two values for one untracked key.
            continue;
        }
        let key = StateKeyRef {
            schema_key: row.schema_key.as_str(),
            file_id: row.file_id.map(|value| value.as_str()),
            entity_pk: row.entity_pk,
        };
        let untracked_owner = if row.global {
            canonical_branch_id(crate::GLOBAL_BRANCH_ID)?
        } else {
            publication_branch_id
        };
        if let Some(snapshot) = row.snapshot {
            publication.put_untracked_row(
                untracked_owner,
                key,
                UntrackedValueRef {
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                    cell: StateCellRef::Value(snapshot.normalized()),
                    metadata: row.metadata.map(|value| value.normalized()),
                    origin_key: row.origin_key.map(|value| value.as_str()),
                    blob_manifest_object_ids: &blob_manifest_object_ids_for_row(
                        row,
                        &prepared_blob_manifests,
                    )?,
                },
            )?;
        } else {
            publication.delete_untracked_row(untracked_owner, key)?;
        }
    }

    if let Some((highest_seen, timestamp, _change_id)) = runtime_checkpoint {
        let entity_pk = runtime_entity_pk
            .as_ref()
            .expect("runtime checkpoint necessarily has an entity identity");
        let snapshot = deterministic_sequence_snapshot(highest_seen)?;
        publication.put_untracked_row(
            canonical_branch_id(crate::GLOBAL_BRANCH_ID)?,
            StateKeyRef {
                schema_key: "lix_key_value",
                file_id: None,
                entity_pk,
            },
            UntrackedValueRef {
                created_at: timestamp,
                updated_at: timestamp,
                cell: StateCellRef::Value(&snapshot),
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: &[],
            },
        )?;
        let initialized_entity_pk = crate::entity_pk::EntityPk::single(
            crate::functions::DETERMINISTIC_SEQUENCE_INITIALIZED_KEY,
        );
        let initialized_snapshot = serde_json::to_string(&serde_json::json!({
            "key": crate::functions::DETERMINISTIC_SEQUENCE_INITIALIZED_KEY,
            "value": true,
        }))
        .map_err(|error| {
            writer_error(format!(
                "failed to serialize deterministic sequence initialization marker: {error}"
            ))
        })?;
        publication.put_untracked_row(
            canonical_branch_id(crate::GLOBAL_BRANCH_ID)?,
            StateKeyRef {
                schema_key: "lix_key_value",
                file_id: None,
                entity_pk: &initialized_entity_pk,
            },
            UntrackedValueRef {
                created_at: timestamp,
                updated_at: timestamp,
                cell: StateCellRef::Value(&initialized_snapshot),
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: &[],
            },
        )?;
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
    let previous_rows = state_points(&view, &tracked_keys, true).await?;
    if previous_rows.len() != tracked_rows.len() {
        return Err(writer_error(
            "ForkTree predecessor lookup returned the wrong slot count",
        ));
    }
    let mut changes = Vec::with_capacity(tracked_rows.len().saturating_add(1));
    let mut state_mutations = Vec::with_capacity(tracked_rows.len());
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
        let snapshot = stage_forktree_json_slot(&mut publication, row.snapshot)?;
        let metadata = stage_forktree_json_slot(&mut publication, row.metadata)?;
        let json_payload_object_ids = json_payload_object_ids(&snapshot, &metadata)?;
        let payload = crate::changelog::encode_forktree_change_payload(&ChangeRecord {
            format_version: 2,
            change_id,
            account_id: active_account_id.to_string(),
            schema_key: row.schema_key.to_string(),
            entity_pk: row.entity_pk.clone(),
            file_id: row.file_id.map(ToString::to_string),
            snapshot,
            metadata,
            created_at: row.created_at,
            origin_key: row.origin_key.map(ToString::to_string),
        })?;
        changes.push(ChangeObjectV1::Semantic {
            change_id: forktree_change_id(change_id),
            payload,
            json_payload_object_ids,
        });

        let mutation = if global && row.snapshot.is_none() {
            StateTreeMutation::remove(key)
        } else {
            let cell = match row.snapshot {
                Some(value) => StateCellRef::Value(value.normalized()),
                None => StateCellRef::Tombstone,
            };
            let encoded = encode_state_value(StateValueRef {
                change_id,
                commit_id,
                created_at: row.created_at,
                updated_at: row.updated_at,
                cell,
                metadata: row.metadata.map(|value| value.normalized()),
                origin_key: row.origin_key.map(|value| value.as_str()),
                blob_manifest_object_ids: &blob_manifest_object_ids_for_row(
                    row,
                    &prepared_blob_manifests,
                )?,
            })?;
            let exists_at_target_root = previous
                .as_ref()
                .is_some_and(|value| global || value.source == StateSource::Branch);
            if exists_at_target_root {
                StateTreeMutation::update(key, encoded)
            } else {
                StateTreeMutation::insert(key, encoded)
            }
        };
        state_mutations.push(mutation);
    }
    sort_state_mutations(&mut state_mutations)?;

    let state_base = if global {
        view.repository_root().global_state_root
    } else {
        view.branch_snapshot().local_state_root
    };
    let state_edit = view.edit_state_tree(state_base, state_mutations).await?;

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

    let mut encoded_semantic_changes = Vec::with_capacity(changes.len());
    let mut member_object_ids = Vec::with_capacity(changes.len());
    for change in &changes {
        let (object_id, _) = change.encode()?;
        member_object_ids.push(object_id);
        encoded_semantic_changes.push((change.change_id(), object_id));
    }
    let global_state_root = if global {
        state_edit.root
    } else {
        view.repository_root().global_state_root
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
    let mut semantic_commit = CommitObjectV1 {
        commit_id: forktree_commit_id(commit_id),
        generation,
        parent_commit_object_ids: parent_object_ids,
        members: member_object_ids
            .into_iter()
            .map(CommitMemberV1::introduced)
            .collect(),
        member_page_root: None,
        global_state_root,
        local_state_root,
        metadata: crate::changelog::encode_forktree_commit_payload(&commit_record)?,
    };
    let _member_pages = semantic_commit.prepare_member_pages()?;
    let (commit_object_id, _) = semantic_commit.encode()?;

    let ref_payload = crate::changelog::encode_forktree_change_payload(&ChangeRecord {
        format_version: 2,
        change_id: change_refs.branch_ref_change_id,
        account_id: active_account_id.to_string(),
        schema_key: crate::branch::BRANCH_REF_SCHEMA_KEY.to_string(),
        entity_pk: crate::entity_pk::EntityPk::uuid_from_canonical(&branch_id).map_err(
            |error| writer_error(format!("transaction branch identity is invalid: {error}")),
        )?,
        file_id: None,
        snapshot: JsonSlot::from_json(
            &serde_json::json!({
                "branch_id": branch_id,
                "commit_id": commit_id.to_string(),
            })
            .to_string(),
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
    changes.push(ref_change);

    let commit_catalog_edit = view
        .put_commit_catalog_entries(
            view.repository_root().commit_catalog_root,
            &[(
                (forktree_commit_id(commit_id)),
                CommitCatalogEntry { commit_object_id },
            )],
        )
        .await?;
    let mut change_entries = Vec::with_capacity(catalog_order.len());
    for index in catalog_order {
        if let Some((change_id, change_object_id)) = encoded_semantic_changes.get(index) {
            change_entries.push((
                *change_id,
                ChangeCatalogEntry {
                    change_object_id: *change_object_id,
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id,
                        ordinal: u32::try_from(index)
                            .map_err(|_| writer_error("commit member ordinal exceeds u32"))?,
                    },
                },
            ));
        } else {
            debug_assert_eq!(index, encoded_semantic_changes.len());
            change_entries.push((
                forktree_change_id(change_refs.branch_ref_change_id),
                ChangeCatalogEntry {
                    change_object_id: ref_object_id,
                    owner: ChangeCatalogOwner::BranchRef {
                        ref_change_object_id: ref_object_id,
                        branch_id: publication_branch_id,
                    },
                },
            ));
        }
    }
    let change_catalog_edit = view
        .put_change_catalog_entries(view.repository_root().change_catalog_root, &change_entries)
        .await?;
    let repository_root = RepositoryRootV1 {
        global_state_root,
        commit_catalog_root: commit_catalog_edit.root,
        change_catalog_root: change_catalog_edit.root,
        retention_policy_root: view.repository_root().retention_policy_root,
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
            let source_commit = load_commit(view, forktree_commit_id(source_head))
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
            if moves_head
                && target_view
                    .scan_untracked_overlay_rows()
                    .await?
                    .into_iter()
                    .any(|(owner, _, _)| owner == branch_id)
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "cannot {} branch '{}' while branch-local untracked state exists",
                        if intent.commit_id.is_some() {
                            "repoint"
                        } else {
                            "delete"
                        },
                        intent.branch_id
                    ),
                ));
            }
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
    fresh_changes: Vec<ChangeObjectV1>,
    members: Vec<CommitMemberV1>,
    max_selected_source_generation: Option<u64>,
}

async fn prepare_ordered_single_branch_history<R>(
    active_account_id: &str,
    commit_parent_heads: &BTreeMap<String, Option<CommitId>>,
    view: &crate::forktree::CoherentView<R>,
    mut publication: PreparedPublication,
    prepared: PreparedWriteSet,
    prepared_blob_manifests: BTreeMap<(String, String, bool, bool), ObjectId>,
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
        let mut mutations = Vec::new();
        let mut fresh_changes = Vec::new();
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
            let snapshot = stage_forktree_json_slot(&mut publication, row.snapshot)?;
            let metadata = stage_forktree_json_slot(&mut publication, row.metadata)?;
            let json_payload_object_ids = json_payload_object_ids(&snapshot, &metadata)?;
            let payload = crate::changelog::encode_forktree_change_payload(&ChangeRecord {
                format_version: 2,
                change_id,
                account_id: active_account_id.to_string(),
                schema_key: row.schema_key.to_string(),
                entity_pk: row.entity_pk.clone(),
                file_id: row.file_id.map(ToString::to_string),
                snapshot,
                metadata,
                created_at: row.created_at,
                origin_key: row.origin_key.map(ToString::to_string),
            })?;
            let change = ChangeObjectV1::Semantic {
                change_id: forktree_change_id(change_id),
                payload,
                json_payload_object_ids,
            };
            let (change_object_id, _) = change.encode()?;
            members.push(CommitMemberV1::introduced(change_object_id));
            fresh_changes.push(change);

            let existed = match touched_presence.get(&key) {
                Some(existed) => *existed,
                None => state_point(view, &key, true)
                    .await?
                    .as_ref()
                    .is_some_and(|value| row.global || value.source == StateSource::Branch),
            };
            let mutation = if row.global && row.snapshot.is_none() {
                touched_presence.insert(key.clone(), false);
                StateTreeMutation::remove(key)
            } else {
                let encoded = encode_state_value(StateValueRef {
                    change_id,
                    commit_id: draft.commit_id,
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                    cell: row.snapshot.map_or(StateCellRef::Tombstone, |value| {
                        StateCellRef::Value(value.normalized())
                    }),
                    metadata: row.metadata.map(|value| value.normalized()),
                    origin_key: row.origin_key.map(|value| value.as_str()),
                    blob_manifest_object_ids: &blob_manifest_object_ids_for_row(
                        row,
                        &prepared_blob_manifests,
                    )?,
                })?;
                touched_presence.insert(key.clone(), true);
                if existed {
                    StateTreeMutation::update(key, encoded)
                } else {
                    StateTreeMutation::insert(key, encoded)
                }
            };
            mutations.push(mutation);
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
                let (member, source_commit, source_change) = select_historical_commit_member(
                    view,
                    forktree_commit_id(selected.source_commit_id),
                    forktree_change_id(selected.change_id),
                )
                .await?;
                max_selected_source_generation = Some(
                    max_selected_source_generation.map_or(source_commit.generation, |generation| {
                        generation.max(source_commit.generation)
                    }),
                );
                let ChangeObjectV1::Semantic { payload, .. } = source_change else {
                    return Err(writer_error(
                        "selected history source member has the wrong Change domain",
                    ));
                };
                let record =
                    crate::changelog::decode_forktree_change_payload(&payload, selected.change_id)?;
                if record.schema_key != selected.schema_key()
                    || record.file_id.as_deref() != selected.file_id()
                    || record.entity_pk != *selected.entity_pk()
                    || record.created_at != selected.created_at
                    || record.snapshot.is_none() != selected.deleted
                {
                    return Err(writer_error(
                        "selected history identity or lifecycle differs from its Change payload",
                    ));
                }
                let (source_value, source_domain) = view
                    .state_point_at_roots(
                        source_commit.global_state_root,
                        source_commit.local_state_root,
                        &identity,
                        true,
                    )
                    .await?
                    .ok_or_else(|| writer_error("selected history source state row is absent"))?;
                let selected_global = source_domain == StateSource::Global;
                if state_domain
                    .replace(selected_global)
                    .is_some_and(|domain| domain != selected_global)
                    || source_value.change_id != selected.change_id
                    || source_value.commit_id != selected.source_commit_id
                    || source_value.cell.deleted() != selected.deleted
                    || source_value.created_at != selected.created_at
                    || source_value.updated_at != selected.updated_at
                {
                    return Err(writer_error(
                        "selected history source state authority is inconsistent",
                    ));
                }
                let existed = match touched_presence.get(&identity) {
                    Some(existed) => *existed,
                    None => state_point(view, &identity, true)
                        .await?
                        .as_ref()
                        .is_some_and(|value| {
                            selected_global || value.source == StateSource::Branch
                        }),
                };
                let mutation = if selected_global && selected.deleted {
                    touched_presence.insert(identity.clone(), false);
                    StateTreeMutation::remove(identity)
                } else {
                    let cell = match &source_value.cell {
                        crate::forktree::StateCell::Value(value) => {
                            StateCellRef::Value(value.as_ref())
                        }
                        crate::forktree::StateCell::Null => StateCellRef::Null,
                        crate::forktree::StateCell::Tombstone => StateCellRef::Tombstone,
                    };
                    let encoded = encode_state_value(StateValueRef {
                        change_id: selected.change_id,
                        commit_id: draft.commit_id,
                        created_at: source_value.created_at,
                        updated_at: source_value.updated_at,
                        cell,
                        metadata: source_value.metadata.as_deref(),
                        origin_key: source_value.origin_key.as_deref(),
                        blob_manifest_object_ids: &source_value.blob_manifest_object_ids,
                    })?;
                    touched_presence.insert(identity.clone(), true);
                    if existed {
                        StateTreeMutation::update(identity, encoded)
                    } else {
                        StateTreeMutation::insert(identity, encoded)
                    }
                };
                mutations.push(mutation);
                members.push(member);
            }
        }
        contents.push(OrderedCommitContent {
            draft,
            mutations,
            fresh_changes,
            members,
            max_selected_source_generation,
        });
    }

    let global = state_domain.unwrap_or(false);
    let state_base = if global {
        view.repository_root().global_state_root
    } else {
        view.branch_snapshot().local_state_root
    };
    for content in &mut contents {
        sort_state_mutations(&mut content.mutations)?;
    }
    let state_edits = view
        .edit_state_tree_sequence(
            state_base,
            contents
                .iter_mut()
                .map(|content| std::mem::take(&mut content.mutations))
                .collect(),
        )
        .await?;

    let mut staged_commits = BTreeMap::<CommitId, (ObjectId, CommitObjectV1)>::new();
    let mut semantic_commits = Vec::with_capacity(contents.len());
    let mut commit_entries = Vec::with_capacity(contents.len());
    let mut fresh_changes = Vec::new();
    let mut fresh_owner_rows = Vec::new();
    for (content, state_edit) in contents.iter().zip(&state_edits) {
        let mut generation = None::<u64>;
        let mut parent_object_ids = Vec::with_capacity(content.draft.parent_commit_ids.len());
        for parent_id in &content.draft.parent_commit_ids {
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
            parent_object_ids.push(parent_object_id);
            generation =
                Some(generation.map_or(parent.generation, |value| value.max(parent.generation)));
        }
        let generation =
            next_ordered_commit_generation(generation, content.max_selected_source_generation)?;
        let global_state_root = if global {
            state_edit.root
        } else {
            view.repository_root().global_state_root
        };
        let local_state_root = if global {
            view.branch_snapshot().local_state_root
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
        let mut commit = CommitObjectV1 {
            commit_id: forktree_commit_id(content.draft.commit_id),
            generation,
            parent_commit_object_ids: parent_object_ids,
            members: content.members.clone(),
            member_page_root: None,
            global_state_root,
            local_state_root,
            metadata: crate::changelog::encode_forktree_commit_payload(&record)?,
        };
        let _member_pages = commit.prepare_member_pages()?;
        let (commit_object_id, _) = commit.encode()?;
        staged_commits.insert(content.draft.commit_id, (commit_object_id, commit.clone()));
        commit_entries.push((commit.commit_id, CommitCatalogEntry { commit_object_id }));
        for (ordinal, change) in content.fresh_changes.iter().enumerate() {
            let (change_object_id, _) = change.encode()?;
            fresh_owner_rows.push((
                change.change_id(),
                ChangeCatalogEntry {
                    change_object_id,
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id,
                        ordinal: u32::try_from(ordinal)
                            .map_err(|_| writer_error("ordered member ordinal exceeds u32"))?,
                    },
                },
            ));
            fresh_changes.push(change.clone());
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
        entity_pk: crate::entity_pk::EntityPk::uuid_from_canonical(&branch_id).map_err(
            |error| writer_error(format!("transaction branch identity is invalid: {error}")),
        )?,
        file_id: None,
        snapshot: JsonSlot::from_json(
            &serde_json::json!({
                "branch_id": branch_id,
                "commit_id": final_content.draft.commit_id.to_string(),
            })
            .to_string(),
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
            change_object_id: ref_object_id,
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
        retention_policy_root: view.repository_root().retention_policy_root,
    };
    let transition = OrderedBranchHistoryTransition {
        state_edits,
        state_domain_global: state_domain.unwrap_or(false),
        commit_catalog_edit,
        change_catalog_edit,
        semantic_commits,
        fresh_changes,
        branch_ref_change,
        branch_snapshot: BranchSnapshotV1 {
            branch_id: view.branch_id(),
            local_state_root: final_local_state_root,
            semantic_head_commit_object_id: final_commit_object_id,
            latest_ref_change_object_id: Some(ref_object_id),
            historical_global_state_root: final_global_state_root,
        },
        repository_root,
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

type PreparedBlobManifestMap = BTreeMap<(String, String, bool, bool), ObjectId>;

fn prepared_blob_manifest_ids(
    publication: &mut PreparedPublication,
    prepared: &PreparedWriteSet,
) -> Result<PreparedBlobManifestMap, LixError> {
    let mut manifests = PreparedBlobManifestMap::new();
    for write in &prepared.file_content_writes {
        let manifest = if let Some(receipt) = write.prepared_cas_receipt() {
            ObjectId::from_bytes(receipt.manifest_object_id)
        } else if let Some(payload) = write.inline_payload() {
            publication
                .stage_inline_blob_payload(payload.bytes())
                .map_err(LixError::from)?
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
        let key = (
            write.branch_id.clone(),
            write.file_id.clone(),
            write.global,
            write.untracked,
        );
        if let Some(previous) = manifests.insert(key.clone(), manifest)
            && previous != manifest
        {
            return Err(writer_error(
                "one file scope has conflicting ForkTree manifest identities",
            ));
        }
    }
    Ok(manifests)
}

fn blob_manifest_object_ids_for_row(
    row: PreparedStateRowRef<'_>,
    manifests: &PreparedBlobManifestMap,
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
    let manifest = manifests.get(&key).copied().ok_or_else(|| {
        writer_error("blob-ref state row has no matching prepared ForkTree manifest")
    })?;
    Ok(vec![manifest])
}

fn sole_publication_branch(
    prepared: &PreparedWriteSet,
    runtime_checkpoint_present: bool,
) -> Result<String, LixError> {
    let mut branches = prepared
        .state_rows
        .iter()
        .map(|row| row.branch_id.as_str())
        .chain(
            prepared
                .commit_change_refs_by_branch
                .keys()
                .map(String::as_str),
        )
        .chain(
            prepared
                .extra_commit_parents_by_branch
                .keys()
                .map(String::as_str),
        )
        .chain(
            prepared
                .first_commit_parent_override_by_branch
                .keys()
                .map(String::as_str),
        )
        .chain(
            prepared
                .intermediate_commits
                .iter()
                .map(|commit| commit.branch_id.as_str()),
        )
        .collect::<BTreeSet<_>>();
    if branches.is_empty() {
        branches.extend(
            prepared
                .branch_ref_intents
                .iter()
                .map(|intent| intent.branch_id.as_str()),
        );
    }
    if branches.is_empty() && runtime_checkpoint_present {
        branches.insert(crate::GLOBAL_BRANCH_ID);
    }
    let branch = branches
        .pop_first()
        .ok_or_else(|| writer_error("prepared publication has no branch owner"))?;
    if !branches.is_empty() {
        return Err(writer_error(
            "multi-branch transaction requires the batched ForkTree publication slice",
        ));
    }
    Ok(branch.to_string())
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

fn state_mutation_key(mutation: &StateTreeMutation) -> &[u8] {
    match mutation {
        StateTreeMutation::Insert { key, .. }
        | StateTreeMutation::Update { key, .. }
        | StateTreeMutation::Remove { key } => key.as_slice(),
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
    use crate::tracked_state::{TrackedStateDiffIdentity, TrackedStateKey};
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
            TrackedStateDiffIdentity::from_key(TrackedStateKey {
                schema_key: "app.row".to_string(),
                file_id: None,
                entity_pk: EntityPk::single("selected"),
            }),
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
