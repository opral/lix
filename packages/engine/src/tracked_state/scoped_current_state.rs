//! Manifest-specific authority for the payload-opaque scoped-range tree.
//!
//! The tree authenticates physical scope/range routing. This layer alone binds
//! a tree transition to commit graph ancestry and sealed mutation authority.

use crate::changelog::CommitId;
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};
use crate::tracked_state::current_state_envelope::scoped_range_part_from_current_state_descriptor;
use crate::tracked_state::scoped_range::{
    ScopedRangeCoverageMarker, ScopedRangeRoot, scan_scoped_range_scope,
    stage_replace_scoped_range, stage_scoped_range_tree,
};
use crate::tracked_state::types::{
    CommitDeltaReplacementScope, CommitStateManifest, CommitStateMutationInventory,
    CurrentStatePartDescriptor, CurrentStateScopedRangeRoot,
};
use crate::{LixError, storage_codec};

const TRANSITION_CONTEXT: &str = "lix current-state scoped-range transition v1";

/// Publishes canonical column pages directly when their closed key envelopes
/// are disjoint from every inherited post-image part. Interleaved parent rows
/// require an ordinal-preserving range merge and deliberately fail closed for
/// now; mutation/history authority remains complete either way.
async fn stage_disjoint_columnar_current_state_pages(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    parent_manifest: Option<&CommitStateManifest>,
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<Option<CurrentStateScopedRangeRoot>, LixError> {
    use datafusion::arrow::array::{Array, StringArray};

    let parent =
        parent_manifest.and_then(|manifest| manifest.current_state_scoped_ranges.as_deref());
    let parts = inventory
        .columnar_parts
        .as_ref()
        .ok_or_else(|| scoped_state_error("columnar publication omitted its part authority"))?;
    if parts.owner_commit_id != *commit_id.as_uuid().as_bytes()
        || inventory.single_partition.as_ref()
            != Some(&CommitDeltaReplacementScope {
                schema_key: parts.schema_key.clone(),
                file_id: None,
            })
    {
        return Err(scoped_state_error(
            "columnar publication disagrees with its commit scope",
        ));
    }
    let set_id = crate::columnar_row_group::RowGroupSetId::new(parts.row_group_set_id);
    let manifest = crate::columnar_row_group::load_staged_row_group_manifest(writes, set_id)?
        .ok_or_else(|| scoped_state_error("columnar publication manifest is missing"))?;
    crate::tracked_state::storage::validate_columnar_mutation_manifest(&manifest, parts)?;
    let identity_column = manifest
        .fields
        .len()
        .checked_sub(1)
        .ok_or_else(|| scoped_state_error("columnar publication has no identity column"))?;
    let mut descriptors = Vec::with_capacity(parts.page_first_keys.len());
    let mut fence_index = 0usize;
    let mut global_ordinal = 0u64;
    for (group_index, group) in manifest.groups.iter().enumerate() {
        let page_count =
            (group.row_count as usize).div_ceil(crate::columnar_row_group::ROW_GROUP_PAGE_ROWS);
        for page_index in 0..page_count {
            let batch = crate::columnar_row_group::load_staged_row_group_page(
                writes,
                set_id,
                &manifest,
                group_index,
                page_index,
                &[identity_column],
            )?;
            let identities = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| scoped_state_error("columnar identity page is not UTF-8"))?;
            if identities.is_empty() || identities.null_count() != 0 {
                return Err(scoped_state_error(
                    "columnar identity page is empty or contains nulls",
                ));
            }
            let encoded_keys = (0..identities.len())
                .map(|row_index| {
                    let entity_pk = crate::entity_pk::EntityPk::from_json_array_text(
                        identities.value(row_index),
                    )
                    .map_err(|error| scoped_state_error(error.to_string()))?;
                    Ok(crate::tracked_state::codec::encode_key_ref(
                        crate::tracked_state::types::TrackedStateKeyRef {
                            schema_key: &parts.schema_key,
                            file_id: None,
                            entity_pk: &entity_pk,
                        },
                    ))
                })
                .collect::<Result<Vec<_>, LixError>>()?;
            if encoded_keys.windows(2).any(|pair| pair[0] >= pair[1])
                || parts.page_first_keys.get(fence_index) != encoded_keys.first()
                || parts.page_last_keys.get(fence_index) != encoded_keys.last()
            {
                return Err(scoped_state_error(
                    "columnar identity page disagrees with authenticated key fences",
                ));
            }
            let row_count = u16::try_from(identities.len())
                .map_err(|_| scoped_state_error("columnar page row count exceeds u16"))?;
            descriptors.push(CurrentStatePartDescriptor {
                first_key: encoded_keys.first().expect("non-empty page").clone(),
                last_key: encoded_keys.last().expect("non-empty page").clone(),
                content_digest: parts.manifest_digest,
                payload_refs_digest: [0; 32],
                source_kind: 2,
                source_id: parts.row_group_set_id,
                owner_commit_id: parts.owner_commit_id,
                part_index: u32::try_from(group_index)
                    .map_err(|_| scoped_state_error("columnar group index exceeds u32"))?,
                source_page_index: u16::try_from(page_index)
                    .map_err(|_| scoped_state_error("columnar page index exceeds u16"))?,
                source_row_offset: 0,
                row_count,
                fragmented: false,
                uniform_created_at: parts.uniform_created_at,
                uniform_updated_at: parts.uniform_updated_at,
            });
            global_ordinal = global_ordinal
                .checked_add(identities.len() as u64)
                .ok_or_else(|| scoped_state_error("columnar row count overflows"))?;
            fence_index += 1;
        }
    }
    if fence_index != parts.page_first_keys.len() || global_ordinal != u64::from(parts.row_count) {
        return Err(scoped_state_error(
            "columnar page topology disagrees with mutation authority",
        ));
    }

    let scope = CommitDeltaReplacementScope {
        schema_key: parts.schema_key.clone(),
        file_id: None,
    };
    let prefix = crate::tracked_state::current_state_envelope::current_state_scope_prefix(&scope)?;
    let mut scoped_parts = descriptors
        .iter()
        .map(|descriptor| scoped_range_part_from_current_state_descriptor(&scope, descriptor))
        .collect::<Result<Vec<_>, LixError>>()?;
    let tree = match parent {
        None => {
            if !parent_scope_is_proven_empty(store, parent_manifest, &scope).await? {
                return Ok(None);
            }
            let marker = ScopedRangeCoverageMarker {
                scope: prefix,
                row_count: global_ordinal,
                part_count: u32::try_from(scoped_parts.len())
                    .map_err(|_| scoped_state_error("columnar part count exceeds u32"))?,
            };
            stage_scoped_range_tree(writes, [(marker, scoped_parts)])?
        }
        Some(parent) => {
            let inherited = scan_scoped_range_scope(store, &parent.tree, &prefix).await?;
            let coverage = match inherited.coverage {
                Some(coverage) => coverage,
                None => {
                    if !parent_scope_is_proven_empty(store, parent_manifest, &scope).await? {
                        return Ok(None);
                    }
                    let marker = ScopedRangeCoverageMarker {
                        scope: prefix,
                        row_count: global_ordinal,
                        part_count: u32::try_from(scoped_parts.len())
                            .map_err(|_| scoped_state_error("columnar part count exceeds u32"))?,
                    };
                    let tree = stage_replace_scoped_range(
                        store,
                        writes,
                        &parent.tree,
                        marker,
                        scoped_parts,
                    )
                    .await?
                    .root;
                    return Ok(Some(attest_scoped_range_root(
                        commit_id,
                        Some(parent),
                        inventory,
                        tree,
                    )?));
                }
            };
            scoped_parts.extend(inherited.parts);
            scoped_parts.sort_by(|left, right| left.first_key.cmp(&right.first_key));
            if scoped_parts
                .windows(2)
                .any(|pair| pair[0].last_key >= pair[1].first_key)
            {
                return Ok(None);
            }
            let marker =
                ScopedRangeCoverageMarker {
                    scope: prefix,
                    row_count: coverage
                        .row_count
                        .checked_add(global_ordinal)
                        .ok_or_else(|| scoped_state_error("columnar scope row count overflows"))?,
                    part_count: coverage
                        .part_count
                        .checked_add(u32::try_from(descriptors.len()).map_err(|_| {
                            scoped_state_error("columnar scope part count exceeds u32")
                        })?)
                        .ok_or_else(|| scoped_state_error("columnar scope part count overflows"))?,
                };
            stage_replace_scoped_range(store, writes, &parent.tree, marker, scoped_parts)
                .await?
                .root
        }
    };
    Ok(Some(attest_scoped_range_root(
        commit_id, parent, inventory, tree,
    )?))
}

/// Proves that no commit in the first-parent state lineage could have authored
/// the requested collection. Unknown/cascading scope information fails closed.
async fn parent_scope_is_proven_empty(
    store: &(impl StorageAdapterRead + ?Sized),
    parent: Option<&CommitStateManifest>,
    scope: &CommitDeltaReplacementScope,
) -> Result<bool, LixError> {
    let Some(parent) = parent else {
        return Ok(true);
    };
    let mut next = Some(parent.clone());
    let mut visited = std::collections::BTreeSet::new();
    while let Some(manifest) = next {
        if !visited.insert(manifest.commit_id) {
            return Err(scoped_state_error(
                "parent scope emptiness proof encountered a commit cycle",
            ));
        }
        if manifest.mutations.member_count != 0 {
            if manifest
                .mutations
                .columnar_parts
                .as_ref()
                .is_some_and(|parts| {
                    parts.schema_key == scope.schema_key && scope.file_id.is_none()
                })
            {
                return Ok(false);
            }
            let Some(touched) =
                crate::tracked_state::storage::commit_state_inventory_exact_touched_scopes(
                    manifest.commit_id,
                    &manifest.mutations,
                )?
            else {
                return Ok(false);
            };
            if touched.contains(scope) {
                return Ok(false);
            }
        }
        let Some(parent_id) = manifest.parent_commit_ids.first().copied() else {
            return Ok(true);
        };
        next = crate::tracked_state::storage::load_commit_state_manifest(store, parent_id).await?;
        if next.is_none() {
            return Ok(false);
        }
    }
    Ok(false)
}

/// Opaque proof that the serving root was produced in this write set from the
/// exact graph parent and sealed mutation inventory.
pub(crate) struct CertifiedCurrentStateScopedRangePublication {
    write_set_id: u64,
    parent_commit_id: Option<CommitId>,
    root: Option<CurrentStateScopedRangeRoot>,
}

impl CertifiedCurrentStateScopedRangePublication {
    pub(crate) fn root(&self) -> Option<Box<CurrentStateScopedRangeRoot>> {
        self.root.clone().map(Box::new)
    }

    pub(crate) fn parent_commit_id(&self) -> Option<CommitId> {
        self.parent_commit_id
    }

    pub(crate) fn write_set_id(&self) -> u64 {
        self.write_set_id
    }
}

pub(crate) fn current_state_mutation_authority_digest(
    inventory: &CommitStateMutationInventory,
) -> Result<[u8; 32], LixError> {
    let mut durable = inventory.clone();
    if durable.replacement_generation.is_some() {
        // Complete-replacement bounds are a rebuildable routing projection;
        // immutable part digests and row counts remain in durable authority.
        durable.parts.clear();
    }
    let encoded = storage_codec::encode("current-state mutation authority digest", &durable)?;
    Ok(
        *blake3::Hasher::new_derive_key("lix current-state mutation authority v1")
            .update(&(encoded.len() as u64).to_be_bytes())
            .update(&encoded)
            .finalize()
            .as_bytes(),
    )
}

pub(crate) fn scoped_range_transition_digest(
    commit_id: CommitId,
    parent_root_id: Option<[u8; 32]>,
    inventory: &CommitStateMutationInventory,
    tree: &ScopedRangeRoot,
) -> Result<[u8; 32], LixError> {
    let authority = current_state_mutation_authority_digest(inventory)?;
    let mut digest = blake3::Hasher::new_derive_key(TRANSITION_CONTEXT);
    digest.update(commit_id.as_uuid().as_bytes());
    digest.update(&[u8::from(parent_root_id.is_some())]);
    if let Some(parent_root_id) = parent_root_id {
        digest.update(&parent_root_id);
    }
    digest.update(&authority);
    digest.update(&tree.root_id);
    digest.update(&tree.root_digest);
    digest.update(&tree.marker_count.to_be_bytes());
    digest.update(&tree.part_count.to_be_bytes());
    digest.update(&tree.row_count.to_be_bytes());
    digest.update(&tree.tree_height.to_be_bytes());
    Ok(*digest.finalize().as_bytes())
}

pub(crate) fn attest_scoped_range_root(
    commit_id: CommitId,
    parent: Option<&CurrentStateScopedRangeRoot>,
    inventory: &CommitStateMutationInventory,
    tree: ScopedRangeRoot,
) -> Result<CurrentStateScopedRangeRoot, LixError> {
    let parent_root_id = parent.map(|parent| parent.tree.root_id);
    let transition_digest =
        scoped_range_transition_digest(commit_id, parent_root_id, inventory, &tree)?;
    Ok(CurrentStateScopedRangeRoot {
        tree,
        parent_root_id,
        transition_digest,
    })
}

pub(crate) async fn stage_complete_replacement_scoped_range_root(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    parent: Option<&CurrentStateScopedRangeRoot>,
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<Option<CurrentStateScopedRangeRoot>, LixError> {
    let Some(generation) = inventory.replacement_generation.as_ref() else {
        return Ok(None);
    };
    let authority = inventory.replacement_parts.as_ref().ok_or_else(|| {
        scoped_state_error("replacement generation omitted immutable part authority")
    })?;
    if inventory.parts.is_empty()
        || inventory.parts.len() != inventory.direct_part_row_counts.len()
        || generation.owner_commit_id != *commit_id.as_uuid().as_bytes()
    {
        return Err(scoped_state_error(
            "replacement generation omitted its complete addressable part set",
        ));
    }
    let descriptors = inventory
        .parts
        .iter()
        .zip(&inventory.direct_part_row_counts)
        .enumerate()
        .map(|(part_index, (bounds, &row_count))| {
            let part = bounds.replacement_part.as_ref().ok_or_else(|| {
                scoped_state_error("replacement state set contains a generic mutation part")
            })?;
            Ok(CurrentStatePartDescriptor {
                first_key: bounds.first_key.clone(),
                last_key: bounds.last_key.clone(),
                content_digest: part.content_digest,
                payload_refs_digest: [0; 32],
                source_kind: 0,
                source_id: [0; 16],
                owner_commit_id: part.owner_commit_id,
                part_index: u32::try_from(part_index)
                    .map_err(|_| scoped_state_error("replacement part index overflows"))?,
                source_page_index: 0,
                source_row_offset: 0,
                row_count,
                fragmented: false,
                uniform_created_at: part.uniform_created_at,
                uniform_updated_at: part.uniform_updated_at,
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    if descriptors
        .iter()
        .map(|part| u64::from(part.row_count))
        .sum::<u64>()
        != u64::from(inventory.member_count)
        || crate::tracked_state::current_state_envelope::replacement_directory_digest(&descriptors)?
            != authority.directory_digest
    {
        return Err(scoped_state_error(
            "replacement descriptors disagree with mutation authority",
        ));
    }
    let scope = generation.scope.clone();
    let prefix = crate::tracked_state::current_state_envelope::current_state_scope_prefix(&scope)?;
    let parts = descriptors
        .iter()
        .map(|descriptor| scoped_range_part_from_current_state_descriptor(&scope, descriptor))
        .collect::<Result<Vec<_>, _>>()?;
    let marker = ScopedRangeCoverageMarker {
        scope: prefix,
        row_count: u64::from(inventory.member_count),
        part_count: u32::try_from(parts.len())
            .map_err(|_| scoped_state_error("replacement part count overflows"))?,
    };
    let tree = match parent {
        Some(parent) => {
            stage_replace_scoped_range(store, writes, &parent.tree, marker, parts)
                .await?
                .root
        }
        None => stage_scoped_range_tree(writes, [(marker, parts)])?,
    };
    Ok(Some(attest_scoped_range_root(
        commit_id, parent, inventory, tree,
    )?))
}

/// Applies one commit's certified current-state transition. Broad/unknown
/// mutation scope fails closed by dropping the accelerator; canonical replay
/// remains authoritative. Exact sparse commits path-copy only affected ranges.
pub(crate) async fn stage_current_state_scoped_ranges(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    parent: Option<&CommitStateManifest>,
    commit_id: CommitId,
    account_id: &str,
    inventory: &CommitStateMutationInventory,
) -> Result<CertifiedCurrentStateScopedRangePublication, LixError> {
    let parent_commit_id = parent.map(|parent| parent.commit_id);
    let parent_root = parent.and_then(|parent| parent.current_state_scoped_ranges.as_deref());
    if inventory.columnar_parts.is_some() {
        let root = stage_disjoint_columnar_current_state_pages(
            store, writes, parent, commit_id, inventory,
        )
        .await?;
        return Ok(CertifiedCurrentStateScopedRangePublication {
            write_set_id: writes.identity(),
            parent_commit_id,
            root,
        });
    }
    let touched = crate::tracked_state::storage::commit_state_inventory_exact_touched_scopes(
        commit_id, inventory,
    )?;

    if inventory.replacement_generation.is_some() {
        let root = stage_complete_replacement_scoped_range_root(
            store,
            writes,
            touched.as_ref().and(parent_root),
            commit_id,
            inventory,
        )
        .await?;
        return Ok(CertifiedCurrentStateScopedRangePublication {
            write_set_id: writes.identity(),
            parent_commit_id,
            root,
        });
    }

    let Some(mut touched) = touched else {
        return Ok(CertifiedCurrentStateScopedRangePublication {
            write_set_id: writes.identity(),
            parent_commit_id,
            root: None,
        });
    };
    let Some(parent_root) = parent_root else {
        return Ok(CertifiedCurrentStateScopedRangePublication {
            write_set_id: writes.identity(),
            parent_commit_id,
            root: None,
        });
    };
    touched.sort();
    touched.dedup();
    let staged_segments = crate::tracked_state::storage::staged_commit_delta_segment_bytes(
        writes, commit_id, inventory,
    )?;
    let members = crate::tracked_state::storage::staged_commit_delta_members(
        store,
        commit_id,
        account_id,
        inventory,
        staged_segments,
    )
    .await?;
    let mut members_by_scope = std::collections::BTreeMap::<
        CommitDeltaReplacementScope,
        Vec<crate::tracked_state::storage::CommitDeltaMember>,
    >::new();
    for member in members {
        members_by_scope
            .entry(CommitDeltaReplacementScope {
                schema_key: member.key.schema_key.clone(),
                file_id: member.key.file_id.clone(),
            })
            .or_default()
            .push(member);
    }
    let mut tree = parent_root.tree.clone();
    for scope in touched {
        let transient = CurrentStateScopedRangeRoot {
            tree,
            parent_root_id: parent_root.parent_root_id,
            transition_digest: parent_root.transition_digest,
        };
        let Some(rewritten) =
            crate::tracked_state::storage::stage_sparse_current_state_scoped_range(
                store,
                writes,
                &transient,
                &scope,
                members_by_scope.remove(&scope).unwrap_or_default(),
            )
            .await?
        else {
            return Ok(CertifiedCurrentStateScopedRangePublication {
                write_set_id: writes.identity(),
                parent_commit_id,
                root: None,
            });
        };
        tree = rewritten;
    }
    if !members_by_scope.is_empty() {
        return Err(scoped_state_error(
            "exact touched scopes omitted materialized mutation members",
        ));
    }
    Ok(CertifiedCurrentStateScopedRangePublication {
        write_set_id: writes.identity(),
        parent_commit_id,
        root: Some(attest_scoped_range_root(
            commit_id,
            Some(parent_root),
            inventory,
            tree,
        )?),
    })
}

pub(crate) fn validate_scoped_range_attestation(
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
    root: &CurrentStateScopedRangeRoot,
) -> Result<(), LixError> {
    if root.transition_digest
        != scoped_range_transition_digest(commit_id, root.parent_root_id, inventory, &root.tree)?
    {
        return Err(scoped_state_error(
            "root transition is not bound to commit mutation authority",
        ));
    }
    Ok(())
}

fn scoped_state_error(message: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state current-state scoped range {message}"),
    )
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::json_store::JsonSlotRef;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};
    use crate::tracked_state::codec::encode_key_ref;
    use crate::tracked_state::scoped_range::{
        ScopedRangeCoverageMarker, ScopedRangePart, ScopedRangePartPayload, ScopedRangePrefix,
        plan_scoped_range_part_splice, route_scoped_range_point, scan_scoped_range_interval,
        snapshot_staged_scoped_range_nodes, stage_scoped_range_part_splice,
        stage_scoped_range_tree, validate_scoped_range_tree,
    };
    use crate::tracked_state::storage::{
        CommitDeltaReplacementGeneration, PublishedCommitStateManifest,
        load_complete_current_state_values_from_scoped_root, load_published_commit_state_manifest,
        sparse_current_state_materialization_count_for_test, stage_certified_commit_state_manifest,
        stage_certified_commit_state_manifest_with_handle,
        stage_current_state_scoped_ranges_from_published_parent,
        stage_current_state_scoped_ranges_from_staged_parent,
        stage_ordered_addressable_commit_deltas, stage_ordered_addressable_replacement_parts,
        validate_current_state_scoped_range_parent_manifest,
    };
    use crate::tracked_state::types::{
        CommitDeltaLifecycleSummary, CommitDeltaReplacementScope, CommitStateManifest,
        CommitStateMutationInventory, CommitStateMutationPart, CommitStateReplayDebt,
        TrackedStateCommitDeltaRef, TrackedStateDeltaRef, TrackedStateKeyRef,
        TrackedStateSingleStringReplacementRef,
    };

    use super::{
        attest_scoped_range_root, stage_current_state_scoped_ranges,
        validate_scoped_range_attestation,
    };

    fn scope(schema_key: &str) -> CommitDeltaReplacementScope {
        CommitDeltaReplacementScope {
            schema_key: schema_key.to_owned(),
            file_id: None,
        }
    }

    fn manifest(
        commit_id: CommitId,
        parent_commit_id: Option<CommitId>,
        mutations: CommitStateMutationInventory,
    ) -> CommitStateManifest {
        CommitStateManifest {
            commit_id,
            generation: 0,
            parent_commit_ids: parent_commit_id.into_iter().collect(),
            commit_change_id: ChangeId::for_test_label(&format!("{commit_id}:commit")),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
            replay_debt: CommitStateReplayDebt {
                depth: 4,
                rows: u64::from(mutations.member_count),
                bytes: u64::from(mutations.member_count),
            },
            mutations,
            current_state_scoped_ranges: None,
            snapshot_root: None,
        }
    }

    fn encoded_key(schema_key: &str, entity: &EntityPk) -> Bytes {
        Bytes::from(encode_key_ref(TrackedStateKeyRef {
            schema_key,
            file_id: None,
            entity_pk: entity,
        }))
    }

    async fn publish_replacement_scope(
        storage: &StorageAdapter<Memory>,
        parent: Option<&PublishedCommitStateManifest>,
        commit_id: CommitId,
        schema_key: &str,
    ) -> PublishedCommitStateManifest {
        let created_at = LixTimestamp::from_unix_millis_utc_lossy(10);
        let replacement_scope = scope(schema_key);
        let generation = CommitDeltaReplacementGeneration {
            scope: replacement_scope.clone(),
            fallback_commit_id: None,
            lifecycle_summary: CommitDeltaLifecycleSummary {
                scope: replacement_scope.clone(),
                ordered_identity_digest: [7; 32],
                uniform_created_at: created_at,
            },
        };
        let mut writes = storage.new_write_set();
        let replacement = stage_ordered_addressable_replacement_parts(
            &mut writes,
            ["entity-000", "entity-001"].into_iter().map(|identity| {
                Ok(TrackedStateSingleStringReplacementRef {
                    schema_key,
                    file_id: None,
                    entity_pk: identity,
                    commit_id,
                    created_at,
                    updated_at: created_at,
                    snapshot: JsonSlotRef::Inline("{\"version\":1}"),
                    metadata: JsonSlotRef::None,
                })
            }),
            &generation,
        )
        .expect("replacement parts should stage");
        let mut inventory = replacement.mutation_inventory().clone();
        inventory.replacement_part_digests.clear();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("replacement read should open");
        let publication = stage_current_state_scoped_ranges_from_published_parent(
            &read,
            &mut writes,
            parent,
            commit_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            &inventory,
        )
        .await
        .expect("replacement scope should publish");
        let mut authority = manifest(commit_id, parent.map(|parent| parent.commit_id), inventory);
        authority.current_state_scoped_ranges = publication.root();
        stage_certified_commit_state_manifest(&mut writes, &authority, &publication)
            .expect("replacement authority should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("replacement scope should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("published replacement read should open");
        load_published_commit_state_manifest(&read, commit_id)
            .await
            .expect("replacement authority should load")
            .expect("replacement authority should exist")
    }

    #[tokio::test]
    async fn complete_replacement_and_sparse_insert_delete_publish_one_authoritative_tree() {
        let storage = StorageAdapter::new(Memory::new());
        let parent_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_3000_0000_7000_8000_0000_0000_0000,
        ));
        let child_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_3000_0000_7000_8000_0001_0000_0000,
        ));
        let created_at = LixTimestamp::from_unix_millis_utc_lossy(10);
        let updated_at = LixTimestamp::from_unix_millis_utc_lossy(20);
        let identities = ["entity-000", "entity-001", "entity-002"];
        let replacement_scope = scope("scoped-publication");
        let generation = CommitDeltaReplacementGeneration {
            scope: replacement_scope.clone(),
            fallback_commit_id: None,
            lifecycle_summary: CommitDeltaLifecycleSummary {
                scope: replacement_scope.clone(),
                ordered_identity_digest: [7; 32],
                uniform_created_at: created_at,
            },
        };

        let mut parent_writes = storage.new_write_set();
        let replacement = stage_ordered_addressable_replacement_parts(
            &mut parent_writes,
            identities.iter().map(|identity| {
                Ok(TrackedStateSingleStringReplacementRef {
                    schema_key: "scoped-publication",
                    file_id: None,
                    entity_pk: identity,
                    commit_id: parent_id,
                    created_at,
                    updated_at,
                    snapshot: JsonSlotRef::Inline("{\"version\":1}"),
                    metadata: JsonSlotRef::None,
                })
            }),
            &generation,
        )
        .expect("replacement parts should stage");
        // This fixture exercises the range-addressed replacement handoff used
        // by this cut; the compact digest-only mutation inventory is owned by
        // the concurrent column-page cut and deliberately has no range bounds.
        let mut replacement_inventory = replacement.mutation_inventory().clone();
        replacement_inventory.replacement_part_digests.clear();
        let empty_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("empty read should open");
        let parent_publication = stage_current_state_scoped_ranges(
            &empty_read,
            &mut parent_writes,
            None,
            parent_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            &replacement_inventory,
        )
        .await
        .expect("complete replacement should publish");
        let mut parent_manifest = manifest(parent_id, None, replacement_inventory);
        parent_manifest.current_state_scoped_ranges = parent_publication.root();
        stage_certified_commit_state_manifest(
            &mut parent_writes,
            &parent_manifest,
            &parent_publication,
        )
        .expect("replacement manifest should accept its proof");
        storage
            .commit_write_set(parent_writes, StorageWriteOptions::default())
            .await
            .expect("replacement publication should commit atomically");

        let parent_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("parent read should open");
        let parent = load_published_commit_state_manifest(&parent_read, parent_id)
            .await
            .expect("parent authority should load")
            .expect("parent authority should exist");
        let existing = EntityPk::single("entity-000");
        let absent = EntityPk::single("entity-999");
        let parent_values = load_complete_current_state_values_from_scoped_root(
            &parent_read,
            parent.current_state_scoped_ranges.as_deref().unwrap(),
            &[
                encoded_key("scoped-publication", &existing),
                encoded_key("scoped-publication", &absent),
            ],
        )
        .await
        .expect("replacement points should route")
        .expect("replacement scope should be covered");
        assert!(parent_values[0].is_some());
        assert!(
            parent_values[1].is_none(),
            "covered exact miss is authoritative"
        );

        let deleted = EntityPk::single("entity-001");
        let inserted = EntityPk::single("entity-003");
        let changes = [
            TrackedStateCommitDeltaRef {
                delta: TrackedStateDeltaRef {
                    schema_key: "scoped-publication",
                    file_id: None,
                    entity_pk: &deleted,
                    change_id: ChangeId::for_test_label("scoped-delete"),
                    commit_id: child_id,
                    deleted: true,
                    created_at,
                    updated_at,
                },
                snapshot: JsonSlotRef::None,
                metadata: JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: true,
            },
            TrackedStateCommitDeltaRef {
                delta: TrackedStateDeltaRef {
                    schema_key: "scoped-publication",
                    file_id: None,
                    entity_pk: &inserted,
                    change_id: ChangeId::for_test_label("scoped-insert"),
                    commit_id: child_id,
                    deleted: false,
                    created_at,
                    updated_at,
                },
                snapshot: JsonSlotRef::Inline("{\"version\":2}"),
                metadata: JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: true,
            },
        ];
        let mut child_writes = storage.new_write_set();
        let child_stage = stage_ordered_addressable_commit_deltas(
            &mut child_writes,
            changes.iter().copied().map(Ok),
            true,
            false,
        )
        .expect("sparse mutations should stage")
        .expect("sparse mutations should be addressable");
        let child_publication = stage_current_state_scoped_ranges_from_published_parent(
            &parent_read,
            &mut child_writes,
            Some(&parent),
            child_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            child_stage.mutation_inventory(),
        )
        .await
        .expect("sparse post-image should path-copy");
        let mut child_manifest = manifest(
            child_id,
            Some(parent_id),
            child_stage.mutation_inventory().clone(),
        );
        child_manifest.current_state_scoped_ranges = child_publication.root();
        stage_certified_commit_state_manifest(
            &mut child_writes,
            &child_manifest,
            &child_publication,
        )
        .expect("sparse manifest should accept its proof");
        drop(parent_read);
        storage
            .commit_write_set(child_writes, StorageWriteOptions::default())
            .await
            .expect("sparse publication should commit atomically");

        let child_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("child read should open");
        let child = load_published_commit_state_manifest(&child_read, child_id)
            .await
            .expect("child authority should load")
            .expect("child authority should exist");
        validate_current_state_scoped_range_parent_manifest(&child, Some(&parent))
            .expect("child must bind the exact parent root");
        let values = load_complete_current_state_values_from_scoped_root(
            &child_read,
            child.current_state_scoped_ranges.as_deref().unwrap(),
            &[
                encoded_key("scoped-publication", &existing),
                encoded_key("scoped-publication", &deleted),
                encoded_key("scoped-publication", &inserted),
                encoded_key("scoped-publication", &absent),
            ],
        )
        .await
        .expect("sparse points should route")
        .expect("sparse scope should remain covered");
        assert!(values[0].is_some(), "untouched row must survive");
        assert!(values[1].is_none(), "delete must be authoritative");
        assert_eq!(
            values[2].as_ref().map(|value| value.commit_id),
            Some(child_id)
        );
        assert!(values[3].is_none(), "covered exact miss must not replay");

        let first = encoded_key("scoped-publication", &existing);
        let last = encoded_key("scoped-publication", &inserted);
        let interval = scan_scoped_range_interval(
            &child_read,
            &child.current_state_scoped_ranges.as_deref().unwrap().tree,
            &crate::tracked_state::current_state_envelope::current_state_scope_prefix(
                &replacement_scope,
            )
            .unwrap(),
            &first,
            &last,
        )
        .await
        .expect("sparse post-image parts should scan");
        let descriptors = interval
            .parts
            .iter()
            .map(|part| {
                crate::tracked_state::current_state_envelope::current_state_descriptor_from_scoped_range_part(part)
                    .expect("current-state locator should decode")
            })
            .collect::<Vec<_>>();
        assert_eq!(descriptors.len(), 3);
        assert_eq!(
            descriptors
                .iter()
                .map(|descriptor| (
                    descriptor.source_kind,
                    descriptor.source_row_offset,
                    descriptor.row_count,
                    descriptor.fragmented,
                ))
                .collect::<Vec<_>>(),
            vec![(0, 0, 1, true), (0, 2, 1, true), (1, 0, 1, true)],
            "sparse delete/insert must retain two immutable source slices and write only the insert",
        );
    }

    #[tokio::test]
    async fn one_sparse_commit_rewrites_multiple_covered_scopes_in_one_write_set() {
        let storage = StorageAdapter::new(Memory::new());
        let alpha_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_3100_0000_7000_8000_0001_0000_0000,
        ));
        let alpha = publish_replacement_scope(&storage, None, alpha_id, "alpha").await;
        let beta_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_3100_0000_7000_8000_0002_0000_0000,
        ));
        let beta = publish_replacement_scope(&storage, Some(&alpha), beta_id, "beta").await;
        let child_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_3100_0000_7000_8000_0000_0000_0000,
        ));
        let entity = EntityPk::single("entity-000");
        let created_at = LixTimestamp::from_unix_millis_utc_lossy(10);
        let updated_at = LixTimestamp::from_unix_millis_utc_lossy(20);
        let changes = ["alpha", "beta"].map(|schema_key| TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key,
                file_id: None,
                entity_pk: &entity,
                change_id: ChangeId::for_test_label(&format!("multi-scope-{schema_key}")),
                commit_id: child_id,
                deleted: false,
                created_at,
                updated_at,
            },
            snapshot: JsonSlotRef::Inline("{\"version\":2}"),
            metadata: JsonSlotRef::None,
            origin_key: None,
            base_coordinate: None,
            authored: true,
        });
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("multi-scope read should open");
        let mut writes = storage.new_write_set();
        let staged = stage_ordered_addressable_commit_deltas(
            &mut writes,
            changes.iter().copied().map(Ok),
            true,
            false,
        )
        .expect("multi-scope mutations should stage")
        .expect("multi-scope mutations should be addressable");
        let publication = stage_current_state_scoped_ranges_from_published_parent(
            &read,
            &mut writes,
            Some(&beta),
            child_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            staged.mutation_inventory(),
        )
        .await
        .expect("both scope rewrites should see earlier staged nodes");
        assert_eq!(
            sparse_current_state_materialization_count_for_test(child_id),
            1,
            "one multi-scope inventory must be decoded and materialized exactly once"
        );
        let mut child = manifest(child_id, Some(beta_id), staged.mutation_inventory().clone());
        child.current_state_scoped_ranges = publication.root();
        stage_certified_commit_state_manifest(&mut writes, &child, &publication)
            .expect("multi-scope authority should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("multi-scope commit should publish atomically");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("multi-scope verification read should open");
        let child = load_published_commit_state_manifest(&read, child_id)
            .await
            .expect("multi-scope authority should load")
            .expect("multi-scope authority should exist");
        let values = load_complete_current_state_values_from_scoped_root(
            &read,
            child.current_state_scoped_ranges.as_deref().unwrap(),
            &[encoded_key("alpha", &entity), encoded_key("beta", &entity)],
        )
        .await
        .expect("both rewritten scopes should route")
        .expect("both scopes should stay covered");
        assert!(
            values
                .iter()
                .all(|value| value.as_ref().map(|value| value.commit_id) == Some(child_id))
        );
    }

    #[tokio::test]
    async fn staged_child_rewrites_scoped_root_published_by_parent_in_same_write_set() {
        let storage = StorageAdapter::new(Memory::new());
        let base_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_3200_0000_7000_8000_0002_0000_0000,
        ));
        let base = publish_replacement_scope(&storage, None, base_id, "staged").await;
        let first_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_3200_0000_7000_8000_0000_0000_0000,
        ));
        let second_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_3200_0000_7000_8000_0001_0000_0000,
        ));
        let first_entity = EntityPk::single("entity-000");
        let second_entity = EntityPk::single("entity-001");
        let created_at = LixTimestamp::from_unix_millis_utc_lossy(10);
        let updated_at = LixTimestamp::from_unix_millis_utc_lossy(20);
        let first_change = TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: "staged",
                file_id: None,
                entity_pk: &first_entity,
                change_id: ChangeId::for_test_label("staged-parent-first"),
                commit_id: first_id,
                deleted: false,
                created_at,
                updated_at,
            },
            snapshot: JsonSlotRef::Inline("{\"version\":2}"),
            metadata: JsonSlotRef::None,
            origin_key: None,
            base_coordinate: None,
            authored: true,
        };
        let second_change = TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: "staged",
                file_id: None,
                entity_pk: &second_entity,
                change_id: ChangeId::for_test_label("staged-parent-second"),
                commit_id: second_id,
                deleted: false,
                created_at,
                updated_at,
            },
            snapshot: JsonSlotRef::Inline("{\"version\":3}"),
            metadata: JsonSlotRef::None,
            origin_key: None,
            base_coordinate: None,
            authored: true,
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("staged-parent read should open");
        let mut writes = storage.new_write_set();
        let first_stage = stage_ordered_addressable_commit_deltas(
            &mut writes,
            [Ok(first_change)].into_iter(),
            true,
            false,
        )
        .expect("first child mutation should stage")
        .expect("first child mutation should be addressable");
        let first_publication = stage_current_state_scoped_ranges_from_published_parent(
            &read,
            &mut writes,
            Some(&base),
            first_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            first_stage.mutation_inventory(),
        )
        .await
        .expect("first child root should stage");
        let mut first = manifest(
            first_id,
            Some(base_id),
            first_stage.mutation_inventory().clone(),
        );
        first.current_state_scoped_ranges = first_publication.root();
        let first = stage_certified_commit_state_manifest_with_handle(
            &mut writes,
            &first,
            &first_publication,
        )
        .expect("first child authority should stage");

        let second_stage = stage_ordered_addressable_commit_deltas(
            &mut writes,
            [Ok(second_change)].into_iter(),
            true,
            false,
        )
        .expect("second child mutation should stage")
        .expect("second child mutation should be addressable");
        let second_publication = stage_current_state_scoped_ranges_from_staged_parent(
            &read,
            &mut writes,
            &first,
            second_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            second_stage.mutation_inventory(),
        )
        .await
        .expect("staged child must read its parent's staged scoped nodes");
        let mut second = manifest(
            second_id,
            Some(first_id),
            second_stage.mutation_inventory().clone(),
        );
        second.current_state_scoped_ranges = second_publication.root();
        stage_certified_commit_state_manifest(&mut writes, &second, &second_publication)
            .expect("second child authority should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("both child authorities should publish atomically");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("staged-parent verification read should open");
        let second = load_published_commit_state_manifest(&read, second_id)
            .await
            .expect("second child authority should load")
            .expect("second child authority should exist");
        let values = load_complete_current_state_values_from_scoped_root(
            &read,
            second.current_state_scoped_ranges.as_deref().unwrap(),
            &[
                encoded_key("staged", &first_entity),
                encoded_key("staged", &second_entity),
            ],
        )
        .await
        .expect("second child state should route")
        .expect("staged scope should remain covered");
        assert_eq!(
            values[0].as_ref().map(|value| value.commit_id),
            Some(first_id)
        );
        assert_eq!(
            values[1].as_ref().map(|value| value.commit_id),
            Some(second_id)
        );
    }

    #[tokio::test]
    async fn sparse_splice_reuses_untouched_children() {
        let storage = StorageAdapter::new(Memory::new());
        let prefix = ScopedRangePrefix::try_from_components([b"large-scope".as_slice()]).unwrap();
        let parts = (0..384_u32)
            .map(|index| {
                let key = index.to_be_bytes().to_vec();
                ScopedRangePart {
                    scope: prefix.clone(),
                    first_key: key.clone(),
                    last_key: key,
                    row_count: 1,
                    payload: ScopedRangePartPayload {
                        version: 1,
                        bytes: vec![index as u8],
                    },
                }
            })
            .collect::<Vec<_>>();
        let marker = ScopedRangeCoverageMarker {
            scope: prefix.clone(),
            row_count: parts.len() as u64,
            part_count: parts.len() as u32,
        };
        let mut parent_writes = storage.new_write_set();
        let parent = stage_scoped_range_tree(&mut parent_writes, [(marker.clone(), parts)])
            .expect("parent tree should stage");
        storage
            .commit_write_set(parent_writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let before = validate_scoped_range_tree(&read, &parent).await.unwrap();
        let target = 191_u32.to_be_bytes();
        let mut writes = storage.new_write_set();
        let staged_nodes = snapshot_staged_scoped_range_nodes(&writes).unwrap();
        let plan = plan_scoped_range_part_splice(
            &read,
            writes.identity(),
            staged_nodes,
            &parent,
            &prefix,
            &[Bytes::copy_from_slice(&target)],
        )
        .await
        .expect("bounded splice should plan");
        let replacements = (0..plan.leaf_count())
            .map(|leaf| {
                let mut leaf_parts = plan.leaf_parts(leaf).cloned().collect::<Vec<_>>();
                for part in &mut leaf_parts {
                    if part.first_key == target {
                        part.payload.bytes = b"rewritten".to_vec();
                    }
                }
                leaf_parts
            })
            .collect();
        let rewritten = stage_scoped_range_part_splice(&mut writes, plan, marker, replacements)
            .expect("bounded splice should stage");
        assert!(
            rewritten.stats.reused_children > 0,
            "untouched children must be reused"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let after = validate_scoped_range_tree(&read, &rewritten.root)
            .await
            .unwrap();
        assert!(before.node_ids.intersection(&after.node_ids).count() > 0);
        let route = route_scoped_range_point(&read, &rewritten.root, &prefix, &target)
            .await
            .unwrap();
        assert_eq!(route.covered_part.unwrap().payload.bytes, b"rewritten");
    }

    #[tokio::test]
    async fn publication_proof_rejects_wrong_parent_write_set_and_forged_transition() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = CommitId::for_test_label("scoped-proof");
        let inventory = CommitStateMutationInventory::default();
        let mut writes = storage.new_write_set();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let publication = stage_current_state_scoped_ranges(
            &read,
            &mut writes,
            None,
            commit_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            &inventory,
        )
        .await
        .unwrap();
        let valid = manifest(commit_id, None, inventory.clone());

        let mut foreign_writes = storage.new_write_set();
        let error =
            stage_certified_commit_state_manifest(&mut foreign_writes, &valid, &publication)
                .expect_err("proof must be tied to one write set");
        assert!(error.message.contains("write set"));

        let mut wrong_parent = valid.clone();
        wrong_parent.parent_commit_ids = vec![CommitId::for_test_label("wrong-parent")];
        let error = stage_certified_commit_state_manifest(&mut writes, &wrong_parent, &publication)
            .expect_err("proof must bind the graph parent");
        assert!(error.message.contains("parent"));

        let prefix = ScopedRangePrefix::try_from_components([b"forged".as_slice()]).unwrap();
        let tree = stage_scoped_range_tree(
            &mut writes,
            [(
                ScopedRangeCoverageMarker {
                    scope: prefix,
                    row_count: 0,
                    part_count: 0,
                },
                Vec::new(),
            )],
        )
        .unwrap();
        let mut forged = attest_scoped_range_root(commit_id, None, &inventory, tree).unwrap();
        forged.transition_digest[0] ^= 1;
        assert!(validate_scoped_range_attestation(commit_id, &inventory, &forged).is_err());
    }

    #[tokio::test]
    async fn unknown_and_broad_mutation_scopes_fail_closed() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let commit_id = CommitId::for_test_label("unknown-scope");

        let mut unknown = CommitStateMutationInventory::default();
        unknown.member_count = 1;
        unknown.selected_source_commit_id =
            Some(*CommitId::for_test_label("selected").as_uuid().as_bytes());
        let mut writes = storage.new_write_set();
        let publication = stage_current_state_scoped_ranges(
            &read,
            &mut writes,
            None,
            commit_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            &unknown,
        )
        .await
        .unwrap();
        assert!(publication.root().is_none());

        let left = EntityPk::single("a");
        let right = EntityPk::single("z");
        let mut broad = CommitStateMutationInventory::default();
        broad.member_count = 1;
        broad.parts.push(CommitStateMutationPart {
            first_key: encoded_key("alpha", &left).to_vec(),
            last_key: encoded_key("omega", &right).to_vec(),
            replacement_part: None,
        });
        let mut writes = storage.new_write_set();
        let publication = stage_current_state_scoped_ranges(
            &read,
            &mut writes,
            None,
            commit_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            &broad,
        )
        .await
        .unwrap();
        assert!(
            publication.root().is_none(),
            "cross-scope mutation must not inherit serving authority"
        );
    }
}
