//! Manifest-specific authority for the payload-opaque scoped-range tree.
//!
//! The tree authenticates physical scope/range routing. This layer alone binds
//! a tree transition to commit graph ancestry and certified mutation authority.

use crate::LixError;
use crate::changelog::CommitId;
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};
use crate::tracked_state::current_state_envelope::scoped_range_part_from_current_state_descriptor;
use crate::tracked_state::scoped_range::{
    ScopedRangeCoverageMarker, ScopedRangeRoot, load_scoped_range_coverage,
    load_scoped_range_coverage_with_staged, stage_replace_scoped_range, stage_scoped_range_tree,
};
use crate::tracked_state::types::{
    ColumnarPageSource, CommitDeltaReplacementScope, CommitStateManifest,
    CommitStateMutationInventory, CommitStateTouchedScopeFilter, CurrentStatePartDescriptor,
    CurrentStatePartSource, CurrentStateScopedRangeRoot, ReplacementPartSource,
};

const TRANSITION_CONTEXT: &str = "lix current-state scoped-range transition v2";
const TOUCHED_SCOPE_FILTER_BYTES: usize = 128;
const TOUCHED_SCOPE_FILTER_HASHES: usize = 4;
const TOUCHED_SCOPE_FILTER_CONTEXT: &str = "lix cumulative touched collection scope v1";

#[derive(Clone, Copy)]
pub(super) struct CommitStateTopologyRef<'a> {
    pub(super) commit_id: CommitId,
    pub(super) touched_scope_filter: &'a CommitStateTouchedScopeFilter,
    pub(super) current_state_scoped_ranges: Option<&'a CurrentStateScopedRangeRoot>,
}

impl<'a> From<&'a CommitStateManifest> for CommitStateTopologyRef<'a> {
    fn from(manifest: &'a CommitStateManifest) -> Self {
        Self {
            commit_id: manifest.commit_id,
            touched_scope_filter: &manifest.touched_scope_filter,
            current_state_scoped_ranges: manifest.current_state_scoped_ranges.as_deref(),
        }
    }
}

/// Publishes canonical column pages directly when their closed key envelopes
/// are disjoint from every inherited post-image part. Interleaved parent rows
/// require an ordinal-preserving range merge and deliberately fail closed for
/// now; mutation/history authority remains complete either way.
async fn stage_disjoint_columnar_current_state_pages(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    parent_manifest: Option<CommitStateTopologyRef<'_>>,
    inherited_scope_filter: &CommitStateTouchedScopeFilter,
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<Option<CurrentStateScopedRangeRoot>, LixError> {
    let parent = parent_manifest.and_then(|manifest| manifest.current_state_scoped_ranges);
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
    let mut descriptors =
        Vec::<CurrentStatePartDescriptor>::with_capacity(parts.page_first_keys.len());
    let mut fence_index = 0usize;
    let mut global_ordinal = 0u64;
    for (group_index, group) in manifest.groups.iter().enumerate() {
        let page_count =
            (group.row_count as usize).div_ceil(crate::columnar_row_group::ROW_GROUP_PAGE_ROWS);
        for page_index in 0..page_count {
            let row_count = u16::try_from(
                (group.row_count as usize
                    - page_index * crate::columnar_row_group::ROW_GROUP_PAGE_ROWS)
                    .min(crate::columnar_row_group::ROW_GROUP_PAGE_ROWS),
            )
            .map_err(|_| scoped_state_error("columnar page row count exceeds u16"))?;
            let first_key = parts.page_first_keys.get(fence_index).ok_or_else(|| {
                scoped_state_error("columnar mutation authority omitted a page first-key fence")
            })?;
            let last_key = parts.page_last_keys.get(fence_index).ok_or_else(|| {
                scoped_state_error("columnar mutation authority omitted a page last-key fence")
            })?;
            if first_key > last_key
                || descriptors
                    .last()
                    .is_some_and(|previous| previous.last_key.as_slice() >= first_key.as_slice())
            {
                return Err(scoped_state_error(
                    "columnar mutation authority has unordered page key fences",
                ));
            }
            descriptors.push(CurrentStatePartDescriptor {
                first_key: first_key.clone(),
                last_key: last_key.clone(),
                content_digest: parts.manifest_digest,
                source: CurrentStatePartSource::ColumnarPage(ColumnarPageSource {
                    source_id: parts.row_group_set_id,
                    owner_commit_id: parts.owner_commit_id,
                    part_index: u32::try_from(group_index)
                        .map_err(|_| scoped_state_error("columnar group index exceeds u32"))?,
                    source_page_index: u16::try_from(page_index)
                        .map_err(|_| scoped_state_error("columnar page index exceeds u16"))?,
                    uniform_created_at: parts.uniform_created_at,
                    uniform_updated_at: parts.uniform_updated_at,
                }),
                source_row_offset: 0,
                row_count,
                fragmented: false,
            });
            global_ordinal = global_ordinal
                .checked_add(u64::from(row_count))
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
    let scoped_parts = descriptors
        .iter()
        .map(|descriptor| scoped_range_part_from_current_state_descriptor(&scope, descriptor))
        .collect::<Result<Vec<_>, LixError>>()?;
    let complete_replacement =
        inventory
            .replacement_generation
            .as_ref()
            .is_some_and(|generation| {
                generation.scope == scope && generation.owner_commit_id == parts.owner_commit_id
            });
    if inventory.replacement_generation.is_some() && !complete_replacement {
        return Err(scoped_state_error(
            "columnar replacement generation disagrees with its scope or owner",
        ));
    }
    let tree = if complete_replacement {
        let marker = ScopedRangeCoverageMarker {
            scope: prefix,
            row_count: global_ordinal,
            part_count: u32::try_from(scoped_parts.len())
                .map_err(|_| scoped_state_error("columnar part count exceeds u32"))?,
        };
        match parent {
            Some(parent) => {
                stage_replace_scoped_range(store, writes, &parent.tree, marker, scoped_parts)
                    .await?
                    .root
            }
            None => stage_scoped_range_tree(writes, [(marker, scoped_parts)])?,
        }
    } else {
        // A mutation part set is not a complete post-image by itself. It may
        // seed a scope only when absence is certified; an already covered
        // scope waits for the bounded sparse range-rewrite path.
        if let Some(parent) = parent
            && load_scoped_range_coverage(store, &parent.tree, &prefix)
                .await?
                .is_some()
        {
            return Ok(None);
        }
        if !touched_scope_filter_proves_absent(inherited_scope_filter, &scope)? {
            return Ok(None);
        }
        let marker = ScopedRangeCoverageMarker {
            scope: prefix.clone(),
            row_count: global_ordinal,
            part_count: u32::try_from(scoped_parts.len())
                .map_err(|_| scoped_state_error("columnar part count exceeds u32"))?,
        };
        match parent {
            Some(parent) => {
                stage_replace_scoped_range(store, writes, &parent.tree, marker, scoped_parts)
                    .await?
                    .root
            }
            None => stage_scoped_range_tree(writes, [(marker, scoped_parts)])?,
        }
    };
    Ok(Some(attest_scoped_range_root(
        commit_id,
        parent_manifest
            .zip(parent)
            .map(|(manifest, root)| (manifest.commit_id, root)),
        inventory,
        tree,
    )?))
}

/// Proves one collection has never been authored in the certified linear
/// lineage. Bloom false positives only disable publication; a negative is an
/// exact absence proof because every exactly bounded mutation contributes its
/// scope and unknown/broad mutations make the certificate incomplete.
#[cfg(test)]
fn parent_scope_is_proven_empty(
    parent: Option<&CommitStateManifest>,
    scope: &CommitDeltaReplacementScope,
) -> Result<bool, LixError> {
    let Some(parent) = parent else {
        return Ok(true);
    };
    touched_scope_filter_proves_absent(&parent.touched_scope_filter, scope)
}

fn empty_complete_touched_scope_filter() -> CommitStateTouchedScopeFilter {
    CommitStateTouchedScopeFilter {
        complete: true,
        bits: vec![0; TOUCHED_SCOPE_FILTER_BYTES],
    }
}

pub(crate) fn incomplete_touched_scope_filter() -> CommitStateTouchedScopeFilter {
    CommitStateTouchedScopeFilter::default()
}

fn advance_touched_scope_filter_from_topology(
    parents: &[CommitStateTopologyRef<'_>],
    selected_source: Option<CommitStateTopologyRef<'_>>,
    touched: Option<&[CommitDeltaReplacementScope]>,
) -> Result<CommitStateTouchedScopeFilter, LixError> {
    let filter = if let Some(source) = selected_source {
        validate_touched_scope_filter(source.touched_scope_filter)?;
        if !source.touched_scope_filter.complete {
            return Ok(incomplete_touched_scope_filter());
        }
        source.touched_scope_filter.clone()
    } else {
        for parent in parents {
            validate_touched_scope_filter(parent.touched_scope_filter)?;
            if !parent.touched_scope_filter.complete {
                return Ok(incomplete_touched_scope_filter());
            }
        }
        match parents.split_first() {
            None => empty_complete_touched_scope_filter(),
            Some((first, rest)) => {
                let mut filter = first.touched_scope_filter.clone();
                for parent in rest {
                    for (target, inherited) in filter
                        .bits
                        .iter_mut()
                        .zip(&parent.touched_scope_filter.bits)
                    {
                        *target |= *inherited;
                    }
                }
                filter
            }
        }
    };
    extend_touched_scope_filter(filter, touched)
}

#[cfg(test)]
fn advance_touched_scope_filter(
    parents: &[&CommitStateManifest],
    selected_source: Option<&CommitStateManifest>,
    touched: Option<&[CommitDeltaReplacementScope]>,
) -> Result<CommitStateTouchedScopeFilter, LixError> {
    let parents = parents
        .iter()
        .map(|parent| CommitStateTopologyRef::from(*parent))
        .collect::<Vec<_>>();
    advance_touched_scope_filter_from_topology(
        &parents,
        selected_source.map(CommitStateTopologyRef::from),
        touched,
    )
}

fn extend_touched_scope_filter(
    mut filter: CommitStateTouchedScopeFilter,
    touched: Option<&[CommitDeltaReplacementScope]>,
) -> Result<CommitStateTouchedScopeFilter, LixError> {
    let Some(touched) = touched else {
        return Ok(incomplete_touched_scope_filter());
    };
    if !filter.complete {
        return Ok(filter);
    }
    for scope in touched {
        for bit in touched_scope_filter_bits(scope.schema_key.as_bytes()) {
            filter.bits[bit / 8] |= 1 << (bit % 8);
        }
    }
    Ok(filter)
}

fn touched_scope_filter_proves_absent(
    filter: &CommitStateTouchedScopeFilter,
    scope: &CommitDeltaReplacementScope,
) -> Result<bool, LixError> {
    validate_touched_scope_filter(filter)?;
    if !filter.complete {
        return Ok(false);
    }
    Ok(touched_scope_filter_bits(scope.schema_key.as_bytes())
        .into_iter()
        .any(|bit| filter.bits[bit / 8] & (1 << (bit % 8)) == 0))
}

fn touched_scope_filter_bits(scope: &[u8]) -> [usize; TOUCHED_SCOPE_FILTER_HASHES] {
    let digest = blake3::Hasher::new_derive_key(TOUCHED_SCOPE_FILTER_CONTEXT)
        .update(&(scope.len() as u64).to_be_bytes())
        .update(scope)
        .finalize();
    let bytes = digest.as_bytes();
    std::array::from_fn(|index| {
        let offset = index * 8;
        let hash = u64::from_be_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .expect("BLAKE3 supplies four u64 filter hashes"),
        );
        hash as usize % (TOUCHED_SCOPE_FILTER_BYTES * 8)
    })
}

pub(crate) fn validate_touched_scope_filter(
    filter: &CommitStateTouchedScopeFilter,
) -> Result<(), LixError> {
    if filter.complete {
        if filter.bits.len() != TOUCHED_SCOPE_FILTER_BYTES {
            return Err(scoped_state_error(
                "complete touched-scope filter has the wrong length",
            ));
        }
    } else if !filter.bits.is_empty() {
        return Err(scoped_state_error(
            "incomplete touched-scope filter carries unauthoritative bits",
        ));
    }
    Ok(())
}

/// Opaque proof that the physical serving projection was produced in this
/// write set from the exact graph topology and certified mutation inventory.
pub(crate) struct CertifiedCommitStatePhysicalPublication {
    write_set_id: u64,
    commit_id: CommitId,
    selected_source_commit_id: Option<CommitId>,
    root: Option<CurrentStateScopedRangeRoot>,
    touched_scope_filter: CommitStateTouchedScopeFilter,
}

impl CertifiedCommitStatePhysicalPublication {
    pub(crate) fn root(&self) -> Option<Box<CurrentStateScopedRangeRoot>> {
        self.root.clone().map(Box::new)
    }

    pub(crate) fn selected_source_commit_id(&self) -> Option<CommitId> {
        self.selected_source_commit_id
    }

    pub(crate) fn commit_id(&self) -> CommitId {
        self.commit_id
    }

    pub(crate) fn write_set_id(&self) -> u64 {
        self.write_set_id
    }

    pub(crate) fn touched_scope_filter(&self) -> &CommitStateTouchedScopeFilter {
        &self.touched_scope_filter
    }
}

/// Certifies cumulative schema-family absence authority when a serving range
/// root itself cannot cross a merge or selected-source topology edge. Graph
/// parents are unioned conservatively. A selected source supplies the complete
/// inherited state, so it supersedes graph parents for this proof.
#[cfg(test)]
pub(super) fn certify_topology_touched_scope_filter_from_manifests(
    writes: &StorageWriteSet,
    parents: &[&CommitStateManifest],
    selected_source: Option<&CommitStateManifest>,
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<CertifiedCommitStatePhysicalPublication, LixError> {
    let selected_source_commit_id = inventory.selected_source_commit_id();
    if selected_source_commit_id != selected_source.map(|source| source.commit_id) {
        return Err(scoped_state_error(
            "selected-source manifest disagrees with mutation authority",
        ));
    }
    let empty_base = parents.is_empty() && selected_source.is_none();
    let touched = crate::tracked_state::storage::commit_state_inventory_exact_local_touched_scopes(
        commit_id, inventory, empty_base,
    )?;
    let filter_touched = if touched.is_some() || empty_base {
        touched
    } else {
        crate::tracked_state::storage::commit_state_inventory_exact_local_touched_scopes(
            commit_id, inventory, true,
        )?
    };
    Ok(CertifiedCommitStatePhysicalPublication {
        write_set_id: writes.identity(),
        commit_id,
        selected_source_commit_id,
        root: None,
        touched_scope_filter: advance_touched_scope_filter(
            parents,
            selected_source,
            filter_touched.as_deref(),
        )?,
    })
}

pub(crate) fn current_state_mutation_authority_digest(
    inventory: &CommitStateMutationInventory,
) -> Result<[u8; 32], LixError> {
    // The immutable commit header co-authenticates this transition, the small
    // catalog, and the exact mutation-directory root. Re-encoding every part
    // bound here would construct a second O(parts) digest on the commit path.
    // Bind only logical/topology closure needed by the serving transition;
    // the catalog digest and Merkle root bind exact physical part identity.
    let mut digest = blake3::Hasher::new_derive_key("lix current-state transition authority v2");
    digest.update(&inventory.member_count.to_be_bytes());
    digest.update(&inventory.selection_fingerprint);
    digest.update(&[u8::from(inventory.selected_source_commit_id.is_some())]);
    if let Some(source) = inventory.selected_source_commit_id {
        digest.update(&source);
    }
    digest.update(&(inventory.part_count() as u64).to_be_bytes());
    digest.update(&(inventory.direct_part_row_counts.len() as u64).to_be_bytes());
    digest.update(&(inventory.direct_part_ownership.len() as u64).to_be_bytes());
    for ownership in &inventory.direct_part_ownership {
        digest.update(&(ownership.len() as u64).to_be_bytes());
        digest.update(ownership);
    }
    let generic_part_count = if inventory.replacement_generation.is_some() {
        0
    } else {
        inventory.parts.len()
    };
    digest.update(&(generic_part_count as u64).to_be_bytes());
    digest.update(&(inventory.replacement_part_digests.len() as u64).to_be_bytes());
    digest.update(&[u8::from(!inventory.inline_part.is_empty())]);
    if !inventory.inline_part.is_empty() {
        digest.update(blake3::hash(&inventory.inline_part).as_bytes());
    }
    digest.update(&[u8::from(inventory.single_partition.is_some())]);
    if let Some(scope) = inventory.single_partition.as_ref() {
        digest.update(&(scope.schema_key.len() as u64).to_be_bytes());
        digest.update(scope.schema_key.as_bytes());
        digest.update(&[u8::from(scope.file_id.is_some())]);
        if let Some(file_id) = scope.file_id.as_ref() {
            digest.update(&(file_id.len() as u64).to_be_bytes());
            digest.update(file_id.as_bytes());
        }
    }
    if let Some(generation) = inventory.replacement_generation.as_ref() {
        digest.update(&generation.owner_commit_id);
        digest.update(&generation.integrity_digest);
    }
    if let Some(authority) = inventory.replacement_parts.as_ref() {
        digest.update(&authority.directory_digest);
    }
    if let Some(columnar) = inventory.columnar_parts.as_ref() {
        digest.update(&columnar.owner_commit_id);
        digest.update(&columnar.row_group_set_id);
        digest.update(&columnar.manifest_digest);
        digest.update(&columnar.row_count.to_be_bytes());
    }
    Ok(*digest.finalize().as_bytes())
}

pub(crate) fn scoped_range_transition_digest(
    commit_id: CommitId,
    serving_base_commit_id: Option<CommitId>,
    serving_base_root_id: Option<[u8; 32]>,
    inventory: &CommitStateMutationInventory,
    tree: &ScopedRangeRoot,
) -> Result<[u8; 32], LixError> {
    let authority = current_state_mutation_authority_digest(inventory)?;
    Ok(scoped_range_transition_digest_from_authority(
        commit_id,
        serving_base_commit_id,
        serving_base_root_id,
        authority,
        tree,
    ))
}

pub(crate) fn scoped_range_transition_digest_from_authority(
    commit_id: CommitId,
    serving_base_commit_id: Option<CommitId>,
    serving_base_root_id: Option<[u8; 32]>,
    mutation_authority_digest: [u8; 32],
    tree: &ScopedRangeRoot,
) -> [u8; 32] {
    let mut digest = blake3::Hasher::new_derive_key(TRANSITION_CONTEXT);
    digest.update(commit_id.as_uuid().as_bytes());
    digest.update(&[u8::from(serving_base_commit_id.is_some())]);
    if let Some(serving_base_commit_id) = serving_base_commit_id {
        digest.update(serving_base_commit_id.as_uuid().as_bytes());
    }
    digest.update(&[u8::from(serving_base_root_id.is_some())]);
    if let Some(serving_base_root_id) = serving_base_root_id {
        digest.update(&serving_base_root_id);
    }
    digest.update(&mutation_authority_digest);
    digest.update(&tree.root_id);
    digest.update(&tree.root_digest);
    digest.update(&tree.marker_count.to_be_bytes());
    digest.update(&tree.part_count.to_be_bytes());
    digest.update(&tree.row_count.to_be_bytes());
    digest.update(&tree.tree_height.to_be_bytes());
    *digest.finalize().as_bytes()
}

pub(crate) fn attest_scoped_range_root(
    commit_id: CommitId,
    serving_base: Option<(CommitId, &CurrentStateScopedRangeRoot)>,
    inventory: &CommitStateMutationInventory,
    tree: ScopedRangeRoot,
) -> Result<CurrentStateScopedRangeRoot, LixError> {
    let serving_base_commit_id = serving_base.map(|(commit_id, _)| commit_id);
    let serving_base_root_id = serving_base.map(|(_, root)| root.tree.root_id);
    let transition_digest = scoped_range_transition_digest(
        commit_id,
        serving_base_commit_id,
        serving_base_root_id,
        inventory,
        &tree,
    )?;
    Ok(CurrentStateScopedRangeRoot {
        tree,
        serving_base_commit_id,
        serving_base_root_id,
        transition_digest,
    })
}

pub(crate) async fn stage_complete_replacement_scoped_range_root(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    serving_base_commit_id: Option<CommitId>,
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
                source: CurrentStatePartSource::Replacement(ReplacementPartSource {
                    owner_commit_id: part.owner_commit_id,
                    part_index: u32::try_from(part_index)
                        .map_err(|_| scoped_state_error("replacement part index overflows"))?,
                    uniform_created_at: part.uniform_created_at,
                    uniform_updated_at: part.uniform_updated_at,
                }),
                source_row_offset: 0,
                row_count,
                fragmented: false,
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
        commit_id,
        serving_base_commit_id.zip(parent),
        inventory,
        tree,
    )?))
}

/// Applies one commit's certified current-state transition. Broad/unknown
/// mutation scope fails closed by dropping the accelerator; canonical replay
/// remains authoritative. Exact sparse commits path-copy only affected ranges.
pub(super) async fn stage_current_state_scoped_ranges_from_topology_refs(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    graph_parents: &[CommitStateTopologyRef<'_>],
    selected_source: Option<CommitStateTopologyRef<'_>>,
    serving_base: Option<CommitStateTopologyRef<'_>>,
    commit_id: CommitId,
    account_id: &str,
    inventory: &CommitStateMutationInventory,
) -> Result<CertifiedCommitStatePhysicalPublication, LixError> {
    let selected_source_commit_id = selected_source.map(|source| source.commit_id);
    if selected_source_commit_id != inventory.selected_source_commit_id() {
        return Err(scoped_state_error(
            "selected-source manifest disagrees with mutation authority",
        ));
    }
    let parent_root = serving_base.and_then(|parent| parent.current_state_scoped_ranges);
    let touched = crate::tracked_state::storage::commit_state_inventory_exact_local_touched_scopes(
        commit_id,
        inventory,
        serving_base.is_none(),
    )?;
    // Descriptor cascades make exact per-file serving scopes unknown, but
    // they cannot introduce a schema family that was absent from the parent:
    // every cascaded row had to be authored earlier. Preserve the cumulative
    // negative certificate by adding only the current commit's authored schema
    // families, using empty-base scope extraction to ignore that cascade.
    let filter_touched = if touched.is_some() || serving_base.is_none() {
        touched.clone()
    } else {
        crate::tracked_state::storage::commit_state_inventory_exact_local_touched_scopes(
            commit_id, inventory, true,
        )?
    };
    let inherited_scope_filter =
        advance_touched_scope_filter_from_topology(graph_parents, selected_source, Some(&[]))?;
    if inventory.columnar_parts.is_some() {
        // Bootstrap half of the accelerator: this is the only arm that can
        // mint a scoped root with no parent root in hand.
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_certified_current_state_columnar_root_publication();
        let root = stage_disjoint_columnar_current_state_pages(
            store,
            writes,
            serving_base,
            &inherited_scope_filter,
            commit_id,
            inventory,
        )
        .await?;
        let touched_scope_filter =
            extend_touched_scope_filter(inherited_scope_filter, filter_touched.as_deref())?;
        return Ok(CertifiedCommitStatePhysicalPublication {
            write_set_id: writes.identity(),
            commit_id,
            selected_source_commit_id,
            root,
            touched_scope_filter,
        });
    }
    let touched_scope_filter =
        extend_touched_scope_filter(inherited_scope_filter.clone(), filter_touched.as_deref())?;

    if inventory.replacement_generation.is_some() {
        let root = stage_complete_replacement_scoped_range_root(
            store,
            writes,
            touched
                .as_ref()
                .and(parent_root)
                .and(serving_base.map(|parent| parent.commit_id)),
            touched.as_ref().and(parent_root),
            commit_id,
            inventory,
        )
        .await?;
        return Ok(CertifiedCommitStatePhysicalPublication {
            write_set_id: writes.identity(),
            commit_id,
            selected_source_commit_id,
            root,
            touched_scope_filter,
        });
    }

    let Some(mut touched) = touched else {
        return Ok(CertifiedCommitStatePhysicalPublication {
            write_set_id: writes.identity(),
            commit_id,
            selected_source_commit_id,
            root: None,
            touched_scope_filter,
        });
    };
    let Some(parent_root) = parent_root else {
        return Ok(CertifiedCommitStatePhysicalPublication {
            write_set_id: writes.identity(),
            commit_id,
            selected_source_commit_id,
            root: None,
            touched_scope_filter,
        });
    };
    // Sustain half: the fall-through, not the `else` above. Reaching here means
    // this publication inherited a scoped root from its serving base and will
    // carry it forward rather than dropping the accelerator.
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_certified_current_state_parent_root_hit();
    if touched.is_empty() {
        return Ok(CertifiedCommitStatePhysicalPublication {
            write_set_id: writes.identity(),
            commit_id,
            selected_source_commit_id,
            root: Some(attest_scoped_range_root(
                commit_id,
                serving_base.map(|parent| (parent.commit_id, parent_root)),
                inventory,
                parent_root.tree.clone(),
            )?),
            touched_scope_filter,
        });
    }
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
        let prefix =
            crate::tracked_state::current_state_envelope::current_state_scope_prefix(&scope)?;
        if load_scoped_range_coverage_with_staged(store, writes, &tree, &prefix)
            .await?
            .is_none()
        {
            if !touched_scope_filter_proves_absent(&inherited_scope_filter, &scope)? {
                return Ok(CertifiedCommitStatePhysicalPublication {
                    write_set_id: writes.identity(),
                    commit_id,
                    selected_source_commit_id,
                    root: None,
                    touched_scope_filter,
                });
            }
            tree = stage_replace_scoped_range(
                store,
                writes,
                &tree,
                ScopedRangeCoverageMarker {
                    scope: prefix,
                    row_count: 0,
                    part_count: 0,
                },
                Vec::new(),
            )
            .await?
            .root;
        }
        let transient = CurrentStateScopedRangeRoot {
            tree,
            serving_base_commit_id: parent_root.serving_base_commit_id,
            serving_base_root_id: parent_root.serving_base_root_id,
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
            return Ok(CertifiedCommitStatePhysicalPublication {
                write_set_id: writes.identity(),
                commit_id,
                selected_source_commit_id,
                root: None,
                touched_scope_filter,
            });
        };
        tree = rewritten;
    }
    if !members_by_scope.is_empty() {
        return Err(scoped_state_error(
            "exact touched scopes omitted materialized mutation members",
        ));
    }
    Ok(CertifiedCommitStatePhysicalPublication {
        write_set_id: writes.identity(),
        commit_id,
        selected_source_commit_id,
        root: Some(attest_scoped_range_root(
            commit_id,
            serving_base.map(|parent| (parent.commit_id, parent_root)),
            inventory,
            tree,
        )?),
        touched_scope_filter,
    })
}

#[cfg(test)]
pub(crate) async fn stage_current_state_scoped_ranges(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    graph_parents: &[&CommitStateManifest],
    selected_source: Option<&CommitStateManifest>,
    serving_base: Option<&CommitStateManifest>,
    commit_id: CommitId,
    account_id: &str,
    inventory: &CommitStateMutationInventory,
) -> Result<CertifiedCommitStatePhysicalPublication, LixError> {
    let graph_parents = graph_parents
        .iter()
        .map(|parent| CommitStateTopologyRef::from(*parent))
        .collect::<Vec<_>>();
    stage_current_state_scoped_ranges_from_topology_refs(
        store,
        writes,
        &graph_parents,
        selected_source.map(CommitStateTopologyRef::from),
        serving_base.map(CommitStateTopologyRef::from),
        commit_id,
        account_id,
        inventory,
    )
    .await
}

pub(crate) fn validate_scoped_range_attestation(
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
    root: &CurrentStateScopedRangeRoot,
) -> Result<(), LixError> {
    if root.transition_digest
        != scoped_range_transition_digest(
            commit_id,
            root.serving_base_commit_id,
            root.serving_base_root_id,
            inventory,
            &root.tree,
        )?
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
    use crate::row_pk::RowPk;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};
    use crate::tracked_state::codec::encode_key_ref;
    use crate::tracked_state::scoped_range::{
        ScopedRangeCoverageMarker, ScopedRangePart, ScopedRangePartPayload, ScopedRangePrefix,
        load_scoped_range_coverage_with_staged, plan_scoped_range_part_splice,
        route_scoped_range_point, scan_scoped_range_interval, snapshot_staged_scoped_range_nodes,
        stage_scoped_range_part_splice, stage_scoped_range_tree, validate_scoped_range_tree,
    };
    use crate::tracked_state::storage::{
        CertifiedCommitStateTopologyParent, CommitDeltaReplacementGeneration,
        PublishedCommitStateManifest, load_complete_current_state_values_from_scoped_root,
        load_published_commit_state_manifest, sparse_current_state_materialization_count_for_test,
        stage_certified_commit_state_manifest, stage_certified_commit_state_manifest_with_handle,
        stage_current_state_scoped_ranges_from_published_parent,
        stage_current_state_scoped_ranges_from_staged_parent,
        stage_current_state_scoped_ranges_from_topology, stage_ordered_addressable_commit_deltas,
        stage_ordered_addressable_replacement_parts,
        validate_current_state_scoped_range_serving_base_manifest,
    };
    use crate::tracked_state::types::{
        CommitDeltaLifecycleSummary, CommitDeltaReplacementScope, CommitStateManifest,
        CommitStateMutationInventory, CommitStateMutationPart, CommitStateReplayDebt,
        CommitStateTouchedScopeFilter, CurrentStatePartSource, TrackedStateCommitDeltaRef,
        TrackedStateDeltaRef, TrackedStateKeyRef, TrackedStateSingleStringReplacementRef,
    };

    use super::{
        TOUCHED_SCOPE_FILTER_BYTES, advance_touched_scope_filter, attest_scoped_range_root,
        certify_topology_touched_scope_filter_from_manifests, parent_scope_is_proven_empty,
        stage_current_state_scoped_ranges, touched_scope_filter_proves_absent,
        validate_scoped_range_attestation, validate_touched_scope_filter,
    };

    fn scope(schema_key: &str) -> CommitDeltaReplacementScope {
        CommitDeltaReplacementScope {
            schema_key: schema_key.to_owned(),
            file_id: None,
        }
    }

    fn manifest(
        commit_id: CommitId,
        _parent_commit_id: Option<CommitId>,
        mutations: CommitStateMutationInventory,
    ) -> CommitStateManifest {
        CommitStateManifest {
            commit_id,
            change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            replay_debt: CommitStateReplayDebt {
                depth: 4,
                rows: u64::from(mutations.member_count),
                bytes: u64::from(mutations.member_count),
            },
            mutations,
            touched_scope_filter: Default::default(),
            current_state_scoped_ranges: None,
            snapshot_root: None,
        }
    }

    fn encoded_key(schema_key: &str, row: &RowPk) -> Bytes {
        Bytes::from(encode_key_ref(TrackedStateKeyRef {
            schema_key,
            file_id: None,
            row_pk: row,
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
            ["row-000", "row-001"].into_iter().map(|identity| {
                Ok(TrackedStateSingleStringReplacementRef {
                    schema_key,
                    file_id: None,
                    row_pk: identity,
                    commit_id,
                    created_at,
                    updated_at: created_at,
                    metadata: None,
                    snapshot: b"typed-payload",
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
        authority.touched_scope_filter = publication.touched_scope_filter().clone();
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
        let identities = ["row-000", "row-001", "row-002"];
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
                    row_pk: identity,
                    commit_id: parent_id,
                    created_at,
                    updated_at,
                    metadata: None,
                    snapshot: b"typed-v1",
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
            &[],
            None,
            None,
            parent_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            &replacement_inventory,
        )
        .await
        .expect("complete replacement should publish");
        let mut parent_manifest = manifest(parent_id, None, replacement_inventory);
        parent_manifest.current_state_scoped_ranges = parent_publication.root();
        parent_manifest.touched_scope_filter = parent_publication.touched_scope_filter().clone();
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
        let existing = RowPk::single("row-000");
        let absent = RowPk::single("row-999");
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

        let deleted = RowPk::single("row-001");
        let inserted = RowPk::single("row-003");
        let changes = [
            TrackedStateCommitDeltaRef {
                delta: TrackedStateDeltaRef {
                    schema_key: "scoped-publication",
                    file_id: None,
                    row_pk: &deleted,
                    change_id: ChangeId::for_test_label("scoped-delete"),
                    commit_id: child_id,
                    deleted: true,
                    created_at,
                    updated_at,
                },
                metadata: None,
                snapshot: None,
                origin_key: None,
                base_coordinate: None,
                authored: true,
            },
            TrackedStateCommitDeltaRef {
                delta: TrackedStateDeltaRef {
                    schema_key: "scoped-publication",
                    file_id: None,
                    row_pk: &inserted,
                    change_id: ChangeId::for_test_label("scoped-insert"),
                    commit_id: child_id,
                    deleted: false,
                    created_at,
                    updated_at,
                },
                metadata: None,
                snapshot: Some(b"typed-v2"),
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
        child_manifest.touched_scope_filter = child_publication.touched_scope_filter().clone();
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
        validate_current_state_scoped_range_serving_base_manifest(&child, Some(&parent))
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
                    matches!(descriptor.source, CurrentStatePartSource::Replacement(_)),
                    descriptor.source_row_offset,
                    descriptor.row_count,
                    descriptor.fragmented,
                ))
                .collect::<Vec<_>>(),
            vec![(true, 0, 1, true), (true, 2, 1, true), (false, 0, 1, true)],
            "sparse delete/insert must retain two immutable source slices and write only the insert",
        );
    }

    #[tokio::test]
    async fn sparse_new_scope_requires_cumulative_absence_proof() {
        let storage = StorageAdapter::new(Memory::new());
        let parent_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_3050_0000_7000_8000_0000_0001_0000,
        ));
        let parent = publish_replacement_scope(&storage, None, parent_id, "covered-scope").await;
        let child_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_3050_0000_7000_8000_0000_0002_0000,
        ));
        let row = RowPk::single("row-000");
        let created_at = LixTimestamp::from_unix_millis_utc_lossy(10);
        let updated_at = LixTimestamp::from_unix_millis_utc_lossy(20);
        let change = TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: "certified-new-scope",
                file_id: None,
                row_pk: &row,
                change_id: ChangeId::for_test_label("certified-new-scope-change"),
                commit_id: child_id,
                deleted: false,
                created_at,
                updated_at,
            },
            metadata: None,
            snapshot: Some(b"typed-v1"),
            origin_key: None,
            base_coordinate: None,
            authored: true,
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("new-scope read should open");

        let mut unproven_parent = (*parent).clone();
        unproven_parent.touched_scope_filter = CommitStateTouchedScopeFilter::default();
        let mut fallback_writes = storage.new_write_set();
        let fallback_stage = stage_ordered_addressable_commit_deltas(
            &mut fallback_writes,
            [Ok(change)].into_iter(),
            true,
            false,
        )
        .expect("unproven sparse mutation should stage")
        .expect("unproven sparse mutation should be addressable");
        let fallback = stage_current_state_scoped_ranges(
            &read,
            &mut fallback_writes,
            &[&unproven_parent],
            None,
            Some(&unproven_parent),
            child_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            fallback_stage.mutation_inventory(),
        )
        .await
        .expect("unproven missing scope should fall back without failing the commit");
        assert!(
            fallback.root().is_none(),
            "a missing marker without cumulative absence authority must use canonical replay"
        );

        let mut certified_writes = storage.new_write_set();
        let certified_stage = stage_ordered_addressable_commit_deltas(
            &mut certified_writes,
            [Ok(change)].into_iter(),
            true,
            false,
        )
        .expect("certified sparse mutation should stage")
        .expect("certified sparse mutation should be addressable");
        let certified = stage_current_state_scoped_ranges_from_published_parent(
            &read,
            &mut certified_writes,
            Some(&parent),
            child_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            certified_stage.mutation_inventory(),
        )
        .await
        .expect("certified absent scope should seed an authenticated marker");
        let root = certified
            .root()
            .expect("certified scope should keep serving root");
        let new_scope = crate::tracked_state::current_state_envelope::current_state_scope_prefix(
            &scope("certified-new-scope"),
        )
        .unwrap();
        let marker = load_scoped_range_coverage_with_staged(
            &read,
            &certified_writes,
            &root.tree,
            &new_scope,
        )
        .await
        .expect("new-scope marker should authenticate")
        .expect("new-scope marker should exist");
        assert_eq!((marker.row_count, marker.part_count), (1, 1));
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
        let row = RowPk::single("row-000");
        let created_at = LixTimestamp::from_unix_millis_utc_lossy(10);
        let updated_at = LixTimestamp::from_unix_millis_utc_lossy(20);
        let changes = ["alpha", "beta"].map(|schema_key| TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key,
                file_id: None,
                row_pk: &row,
                change_id: ChangeId::for_test_label(&format!("multi-scope-{schema_key}")),
                commit_id: child_id,
                deleted: false,
                created_at,
                updated_at,
            },
            metadata: None,
            snapshot: Some(b"typed-v2"),
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
        child.touched_scope_filter = publication.touched_scope_filter().clone();
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
            &[encoded_key("alpha", &row), encoded_key("beta", &row)],
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
        let first_row = RowPk::single("row-000");
        let second_row = RowPk::single("row-001");
        let created_at = LixTimestamp::from_unix_millis_utc_lossy(10);
        let updated_at = LixTimestamp::from_unix_millis_utc_lossy(20);
        let first_change = TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: "staged",
                file_id: None,
                row_pk: &first_row,
                change_id: ChangeId::for_test_label("staged-parent-first"),
                commit_id: first_id,
                deleted: false,
                created_at,
                updated_at,
            },
            metadata: None,
            snapshot: Some(b"typed-v2"),
            origin_key: None,
            base_coordinate: None,
            authored: true,
        };
        let second_change = TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: "staged",
                file_id: None,
                row_pk: &second_row,
                change_id: ChangeId::for_test_label("staged-parent-second"),
                commit_id: second_id,
                deleted: false,
                created_at,
                updated_at,
            },
            metadata: None,
            snapshot: Some(b"typed-v3"),
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
        first.touched_scope_filter = first_publication.touched_scope_filter().clone();
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
        second.touched_scope_filter = second_publication.touched_scope_filter().clone();
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
                encoded_key("staged", &first_row),
                encoded_key("staged", &second_row),
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
    async fn publication_proof_rejects_wrong_commit_write_set_and_forged_transition() {
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
            &[],
            None,
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

        let mut wrong_commit = valid.clone();
        wrong_commit.commit_id = CommitId::for_test_label("other-scoped-proof");
        let error = stage_certified_commit_state_manifest(&mut writes, &wrong_commit, &publication)
            .expect_err("proof must be tied to one commit");
        assert!(error.message.contains("identity"));

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
        let selected_id = CommitId::for_test_label("selected");
        unknown.selected_source_commit_id = Some(*selected_id.as_uuid().as_bytes());
        let selected = manifest(selected_id, None, CommitStateMutationInventory::default());
        let mut writes = storage.new_write_set();
        let publication = stage_current_state_scoped_ranges(
            &read,
            &mut writes,
            &[],
            Some(&selected),
            Some(&selected),
            commit_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            &unknown,
        )
        .await
        .unwrap();
        assert!(publication.root().is_none());

        let left = RowPk::single("a");
        let right = RowPk::single("z");
        let mut broad = CommitStateMutationInventory::default();
        broad.member_count = 1;
        broad.parts.push(CommitStateMutationPart {
            first_key: encoded_key("alpha", &left).to_vec(),
            last_key: encoded_key("omega", &right).to_vec(),
            content_digest: [1; 32],
            replacement_part: None,
        });
        let mut writes = storage.new_write_set();
        let publication = stage_current_state_scoped_ranges(
            &read,
            &mut writes,
            &[],
            None,
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

    #[tokio::test]
    async fn empty_scope_proof_rejects_merge_and_selected_source_ancestry() {
        let parent_id = CommitId::for_test_label("empty-proof-parent");
        let requested = scope("not-authored-here");

        let merge = manifest(
            parent_id,
            Some(CommitId::for_test_label("first-parent")),
            CommitStateMutationInventory::default(),
        );
        assert!(!parent_scope_is_proven_empty(Some(&merge), &requested).unwrap());

        let mut selected = manifest(parent_id, None, CommitStateMutationInventory::default());
        selected.mutations.selected_source_commit_id = Some(
            *CommitId::for_test_label("selected-source")
                .as_uuid()
                .as_bytes(),
        );
        assert!(!parent_scope_is_proven_empty(Some(&selected), &requested).unwrap());
    }

    #[test]
    fn cumulative_touched_scope_filter_proves_only_exact_negatives() {
        let authored = scope("authored");
        let child_authored = scope("child-authored");
        let absent = scope("never-authored");
        let root_filter =
            advance_touched_scope_filter(&[], None, Some(&[authored.clone()])).unwrap();
        assert!(root_filter.complete);
        assert!(!touched_scope_filter_proves_absent(&root_filter, &authored).unwrap());
        assert!(touched_scope_filter_proves_absent(&root_filter, &absent).unwrap());

        let mut parent = manifest(
            CommitId::for_test_label("scope-filter-parent"),
            None,
            CommitStateMutationInventory::default(),
        );
        let incomplete_manifest_bytes =
            crate::storage_codec::encode("scope-filter size baseline", &parent)
                .unwrap()
                .len();
        parent.touched_scope_filter = root_filter;
        let complete_manifest_bytes =
            crate::storage_codec::encode("scope-filter size candidate", &parent)
                .unwrap()
                .len();
        assert_eq!(complete_manifest_bytes - incomplete_manifest_bytes, 129);
        let child_filter = advance_touched_scope_filter(
            &[&parent],
            None,
            Some(std::slice::from_ref(&child_authored)),
        )
        .unwrap();
        assert!(!touched_scope_filter_proves_absent(&child_filter, &authored).unwrap());
        assert!(!touched_scope_filter_proves_absent(&child_filter, &child_authored).unwrap());
        assert!(touched_scope_filter_proves_absent(&child_filter, &absent).unwrap());

        let incomplete = advance_touched_scope_filter(&[&parent], None, None).unwrap();
        assert!(!incomplete.complete);
        assert!(incomplete.bits.is_empty());
        assert!(!touched_scope_filter_proves_absent(&incomplete, &absent).unwrap());
    }

    #[test]
    fn cumulative_touched_scope_filter_unions_merges_and_follows_selected_state() {
        let left_scope = scope("left");
        let right_scope = scope("right");
        let local_scope = scope("local");
        let absent = scope("absent");

        let mut left = manifest(
            CommitId::for_test_label("scope-filter-left"),
            None,
            CommitStateMutationInventory::default(),
        );
        left.touched_scope_filter =
            advance_touched_scope_filter(&[], None, Some(std::slice::from_ref(&left_scope)))
                .unwrap();
        let mut right = manifest(
            CommitId::for_test_label("scope-filter-right"),
            None,
            CommitStateMutationInventory::default(),
        );
        right.touched_scope_filter =
            advance_touched_scope_filter(&[], None, Some(std::slice::from_ref(&right_scope)))
                .unwrap();

        let merged = advance_touched_scope_filter(&[&left, &right], None, Some(&[])).unwrap();
        assert!(!touched_scope_filter_proves_absent(&merged, &left_scope).unwrap());
        assert!(!touched_scope_filter_proves_absent(&merged, &right_scope).unwrap());
        assert!(touched_scope_filter_proves_absent(&merged, &absent).unwrap());

        let selected = advance_touched_scope_filter(
            &[&left],
            Some(&right),
            Some(std::slice::from_ref(&local_scope)),
        )
        .unwrap();
        assert!(touched_scope_filter_proves_absent(&selected, &left_scope).unwrap());
        assert!(!touched_scope_filter_proves_absent(&selected, &right_scope).unwrap());
        assert!(!touched_scope_filter_proves_absent(&selected, &local_scope).unwrap());
    }

    #[test]
    fn certified_topology_filter_binds_all_graph_parents() {
        let storage = StorageAdapter::new(Memory::new());
        let mut left = manifest(
            CommitId::for_test_label("certified-filter-left"),
            None,
            CommitStateMutationInventory::default(),
        );
        left.touched_scope_filter = advance_touched_scope_filter(&[], None, Some(&[])).unwrap();
        let mut right = manifest(
            CommitId::for_test_label("certified-filter-right"),
            None,
            CommitStateMutationInventory::default(),
        );
        right.touched_scope_filter = advance_touched_scope_filter(&[], None, Some(&[])).unwrap();
        let commit_id = CommitId::for_test_label("certified-filter-merge");
        let inventory = CommitStateMutationInventory::default();
        let mut writes = storage.new_write_set();
        let publication = certify_topology_touched_scope_filter_from_manifests(
            &writes,
            &[&left, &right],
            None,
            commit_id,
            &inventory,
        )
        .unwrap();
        let mut merged = manifest(commit_id, Some(left.commit_id), inventory);
        merged.touched_scope_filter = publication.touched_scope_filter().clone();
        stage_certified_commit_state_manifest(&mut writes, &merged, &publication).unwrap();
    }

    #[tokio::test]
    async fn topology_publication_binds_merge_and_selected_source_serving_bases() {
        let storage = StorageAdapter::new(Memory::new());
        let left_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_3300_0000_7000_8000_0001_0000_0000,
        ));
        let left = publish_replacement_scope(&storage, None, left_id, "left_schema").await;
        let right_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0199_3300_0000_7000_8000_0002_0000_0000,
        ));
        let right = publish_replacement_scope(&storage, None, right_id, "right_schema").await;
        let left_root = left
            .current_state_scoped_ranges
            .as_deref()
            .expect("left parent has a serving root");
        let right_root = right
            .current_state_scoped_ranges
            .as_deref()
            .expect("right parent has a serving root");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();

        let merge_commit = CommitId::for_test_label("serving-base-merge");
        let merge_inventory = CommitStateMutationInventory::default();
        let mut merge_writes = storage.new_write_set();
        let merge_publication = stage_current_state_scoped_ranges_from_topology(
            &read,
            &mut merge_writes,
            &[
                CertifiedCommitStateTopologyParent::Published(&left),
                CertifiedCommitStateTopologyParent::Published(&right),
            ],
            None,
            merge_commit,
            crate::ANONYMOUS_ACCOUNT_ID,
            &merge_inventory,
        )
        .await
        .unwrap();
        let merge_root = merge_publication.root().expect("merge reuses target root");
        assert_eq!(merge_root.serving_base_commit_id, Some(left.commit_id));
        assert_eq!(
            merge_root.serving_base_root_id,
            Some(left_root.tree.root_id)
        );
        assert_eq!(merge_root.tree, left_root.tree);

        let alias_commit = CommitId::for_test_label("serving-base-selected-source");
        let mut alias_inventory = CommitStateMutationInventory::default();
        alias_inventory.selected_source_commit_id = Some(*right.commit_id.as_uuid().as_bytes());
        let mut alias_writes = storage.new_write_set();
        let alias_publication = stage_current_state_scoped_ranges_from_topology(
            &read,
            &mut alias_writes,
            &[CertifiedCommitStateTopologyParent::Published(&left)],
            Some(CertifiedCommitStateTopologyParent::Published(&right)),
            alias_commit,
            crate::ANONYMOUS_ACCOUNT_ID,
            &alias_inventory,
        )
        .await
        .unwrap();
        let alias_root = alias_publication
            .root()
            .expect("selected source supplies serving root");
        assert_eq!(alias_root.serving_base_commit_id, Some(right.commit_id));
        assert_eq!(
            alias_root.serving_base_root_id,
            Some(right_root.tree.root_id)
        );
        assert_eq!(alias_root.tree, right_root.tree);
    }

    #[test]
    fn touched_scope_filter_rejects_noncanonical_incomplete_or_truncated_state() {
        assert!(
            validate_touched_scope_filter(&CommitStateTouchedScopeFilter {
                complete: false,
                bits: vec![1],
            })
            .is_err()
        );
        assert!(
            validate_touched_scope_filter(&CommitStateTouchedScopeFilter {
                complete: true,
                bits: vec![0; TOUCHED_SCOPE_FILTER_BYTES - 1],
            })
            .is_err()
        );
    }
}
