//! Persistent range directory for immutable committed current-state parts.
//!
//! Mutation inventory remains the historical authority. This directory is a
//! content-addressed serving accelerator over a final, non-overlapping
//! post-image part set and can therefore be rebuilt or discarded.

#![allow(dead_code)]

use std::mem::size_of;

use bytes::Bytes;

use crate::changelog::CommitId;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StorageProjectedValue,
    StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::tracked_state::types::{
    CommitStateManifest, CommitStateMutationInventory, CurrentStateCatalogRoot,
    CurrentStateCoverageAnchor, CurrentStatePartDescriptor, CurrentStatePartDirectoryRoot,
    CurrentStatePartSet,
};
use crate::{LixError, storage_codec};

pub(crate) const CURRENT_STATE_PART_DIRECTORY_SPACE: StorageSpace = StorageSpace::immutable(
    StorageSpaceId(0x0004_002c),
    "tracked_state.current_state_part_directory.v2",
);
pub(crate) const CURRENT_STATE_CATALOG_SPACE: StorageSpace = StorageSpace::immutable(
    StorageSpaceId(0x0004_002d),
    "tracked_state.current_state_catalog.v2",
);

const DIRECTORY_NODE_RAW_MAGIC: &[u8; 6] = b"LXC2DR";
const DIRECTORY_NODE_ZSTD_MAGIC: &[u8; 6] = b"LXC2DZ";
const DIRECTORY_NODE_MAX_DECODED_BYTES: usize = 16 * 1024 * 1024;
const DIRECTORY_FANOUT: usize = 128;
const DIRECTORY_HASH_CONTEXT: &str = "lix current-state part directory node v2";
const CATALOG_NODE_MAGIC: &[u8; 6] = b"LXCSC2";
const CATALOG_HASH_CONTEXT: &str = "lix current-state catalog node v2";
const CURRENT_STATE_LINEAGE_CONTEXT: &str = "lix current-state lineage v2";
const CATALOG_TRANSITION_CONTEXT: &str = "lix current-state catalog transition v1";
const CATALOG_LEAF_MAX_ENTRIES: usize = 128;
const CATALOG_MAX_KEY_BYTES: usize = 64 * 1024;

/// Publishes the serving directory for a certified complete replacement.
/// Ordinary mutation inventories are not state partitions and return `None`.
pub(crate) fn stage_complete_replacement_current_state_part_set(
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<Option<CurrentStatePartSet>, LixError> {
    let Some(initial_generation) = inventory.replacement_generation.as_ref() else {
        return Ok(None);
    };
    let authority = inventory.replacement_parts.as_ref().ok_or_else(|| {
        directory_error("replacement generation omitted its immutable part authority")
    })?;
    let authority_digest = authority.directory_digest;
    if inventory.parts.len() != inventory.direct_part_row_counts.len() || inventory.parts.is_empty()
    {
        return Err(directory_error(
            "replacement generation has no complete directly-addressable part set",
        ));
    }
    let descriptors = inventory
        .parts
        .iter()
        .zip(&inventory.direct_part_row_counts)
        .enumerate()
        .map(|(part_index, (bounds, &row_count))| {
            let part = bounds.replacement_part.as_ref().ok_or_else(|| {
                directory_error("replacement state set contains a generic mutation part")
            })?;
            let descriptor = CurrentStatePartDescriptor {
                first_key: bounds.first_key.clone(),
                last_key: bounds.last_key.clone(),
                content_digest: part.content_digest,
                payload_refs_digest: [0; 32],
                source_kind: 0,
                owner_commit_id: part.owner_commit_id,
                part_index: u32::try_from(part_index)
                    .map_err(|_| directory_error("part index overflows u32"))?,
                source_row_offset: 0,
                row_count,
                uniform_created_at: part.uniform_created_at,
                uniform_updated_at: part.uniform_updated_at,
            };
            Ok(descriptor)
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let directory = stage_current_state_part_directory(writes, &descriptors)?;
    if directory.row_count != u64::from(inventory.member_count)
        || replacement_directory_digest(&descriptors)? != authority_digest
        || initial_generation.owner_commit_id != *commit_id.as_uuid().as_bytes()
    {
        return Err(directory_error(
            "replacement state set disagrees with its commit authority",
        ));
    }
    let generation = initial_generation.clone();
    let state_lineage_digest =
        fresh_current_state_lineage_digest(commit_id, generation.integrity_digest, &directory);
    Ok(Some(CurrentStatePartSet {
        scope: generation.scope.clone(),
        generation_integrity_digest: generation.integrity_digest,
        state_lineage_digest,
        directory,
    }))
}

pub(crate) fn current_state_catalog_transition_digest(
    commit_id: CommitId,
    parent_root_id: Option<[u8; 32]>,
    inventory: &CommitStateMutationInventory,
    root_id: [u8; 32],
    entry_count: u32,
) -> Result<[u8; 32], LixError> {
    let mut durable_inventory = inventory.clone();
    if durable_inventory.replacement_generation.is_some() {
        durable_inventory.parts.clear();
    }
    let inventory_bytes = storage_codec::encode(
        "current-state catalog transition inventory",
        &durable_inventory,
    )?;
    let mut digest = blake3::Hasher::new_derive_key(CATALOG_TRANSITION_CONTEXT);
    digest.update(commit_id.as_uuid().as_bytes());
    match parent_root_id {
        Some(parent_root_id) => {
            digest.update(&[1]);
            digest.update(&parent_root_id);
        }
        None => {
            digest.update(&[0]);
        }
    }
    digest.update(&(inventory_bytes.len() as u64).to_be_bytes());
    digest.update(&inventory_bytes);
    digest.update(&root_id);
    digest.update(&entry_count.to_be_bytes());
    Ok(*digest.finalize().as_bytes())
}

pub(super) fn attest_catalog_root(
    mut root: CurrentStateCatalogRoot,
    parent: Option<&CurrentStateCatalogRoot>,
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<CurrentStateCatalogRoot, LixError> {
    root.parent_root_id = parent.map(|parent| parent.root_id);
    root.transition_digest = current_state_catalog_transition_digest(
        commit_id,
        root.parent_root_id,
        inventory,
        root.root_id,
        root.entry_count,
    )?;
    Ok(root)
}

pub(crate) fn fresh_current_state_lineage_digest(
    commit_id: CommitId,
    generation_integrity_digest: [u8; 32],
    directory: &CurrentStatePartDirectoryRoot,
) -> [u8; 32] {
    *blake3::Hasher::new_derive_key(CURRENT_STATE_LINEAGE_CONTEXT)
        .update(commit_id.as_uuid().as_bytes())
        .update(&generation_integrity_digest)
        .update(&directory.root_id)
        .update(&directory.directory_digest)
        .finalize()
        .as_bytes()
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct CatalogChild {
    #[musli(bytes)]
    route: Vec<u8>,
    node_id: [u8; 32],
    entry_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct CatalogNode {
    depth: u32,
    #[musli(bytes)]
    sample_key: Vec<u8>,
    entries: Vec<CurrentStatePartSet>,
    children: Vec<CatalogChild>,
}

impl CatalogNode {
    fn entry_count(&self) -> Result<u32, LixError> {
        if self.children.is_empty() {
            u32::try_from(self.entries.len())
                .map_err(|_| directory_error("catalog entry count overflows u32"))
        } else {
            self.children.iter().try_fold(0u32, |total, child| {
                total
                    .checked_add(child.entry_count)
                    .ok_or_else(|| directory_error("catalog entry count overflows u32"))
            })
        }
    }
}

/// Opaque proof that the serving catalog was produced by the canonical
/// parent-plus-mutation transition in this module.
pub(crate) struct CertifiedCurrentStateCatalogPublication {
    write_set_id: u64,
    parent_commit_id: Option<CommitId>,
    root: Option<CurrentStateCatalogRoot>,
    anchor: Option<CurrentStateCoverageAnchor>,
}

impl CertifiedCurrentStateCatalogPublication {
    pub(crate) fn parts(
        &self,
    ) -> (
        Option<Box<CurrentStateCatalogRoot>>,
        Option<Box<CurrentStateCoverageAnchor>>,
    ) {
        (
            self.root.clone().map(Box::new),
            self.anchor.clone().map(Box::new),
        )
    }

    pub(crate) fn parent_commit_id(&self) -> Option<CommitId> {
        self.parent_commit_id
    }

    pub(crate) fn write_set_id(&self) -> u64 {
        self.write_set_id
    }
}

/// Path-copies the collection catalog after one sealed commit inventory.
/// Unknown/broad mutation scopes deliberately discard the accelerator; replay
/// remains the semantic fallback and can rebuild the catalog later.
pub(super) async fn stage_current_state_catalog(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    parent: Option<&CommitStateManifest>,
    commit_id: CommitId,
    account_id: &str,
    inventory: &CommitStateMutationInventory,
) -> Result<CertifiedCurrentStateCatalogPublication, LixError> {
    let replacement =
        stage_complete_replacement_current_state_part_set(writes, commit_id, inventory)?;
    let parent_root = parent.and_then(|parent| parent.current_state_catalog.as_deref());
    let parent_commit_id = parent.map(|parent| parent.commit_id);
    let Some(mut touched_scopes) =
        crate::tracked_state::storage::commit_state_inventory_exact_touched_scopes(
            commit_id, inventory,
        )?
    else {
        let anchor = replacement.as_ref().map(coverage_anchor_from_entry);
        let root = replacement
            .map(|entry| stage_catalog_from_entries(writes, vec![entry]))
            .transpose()?;
        let root = root
            .map(|root| attest_catalog_root(root, parent_root, commit_id, inventory))
            .transpose()?;
        return Ok(CertifiedCurrentStateCatalogPublication {
            write_set_id: writes.identity(),
            parent_commit_id,
            root,
            anchor,
        });
    };
    touched_scopes.sort();
    touched_scopes.dedup();

    let mut root = parent_root.cloned();
    let mut staged = std::collections::BTreeMap::<[u8; 32], CatalogNode>::new();
    for scope in touched_scopes {
        if replacement
            .as_ref()
            .is_some_and(|entry| entry.scope == scope)
        {
            continue;
        }
        let rewritten = if let Some(parent_root) = parent_root {
            match load_current_state_catalog_entry_for_write(store, writes, parent_root, &scope)
                .await?
            {
                Some(parent_entry) => {
                    crate::tracked_state::storage::stage_sparse_current_state_part_set(
                        store,
                        writes,
                        &parent_entry,
                        commit_id,
                        account_id,
                        inventory,
                    )
                    .await?
                }
                None => None,
            }
        } else {
            None
        };
        root = update_catalog_entry(store, writes, &mut staged, root.as_ref(), &scope, rewritten)
            .await?;
    }
    let anchor = replacement.as_ref().map(coverage_anchor_from_entry);
    if let Some(entry) = replacement {
        let scope = entry.scope.clone();
        root = update_catalog_entry(
            store,
            writes,
            &mut staged,
            root.as_ref(),
            &scope,
            Some(entry),
        )
        .await?;
    }
    let root = root
        .map(|root| attest_catalog_root(root, parent_root, commit_id, inventory))
        .transpose()?;
    if let Some(root) = root.as_ref() {
        flush_reachable_staged_catalog_nodes(writes, &staged, root.root_id)?;
    }
    Ok(CertifiedCurrentStateCatalogPublication {
        write_set_id: writes.identity(),
        parent_commit_id,
        root,
        anchor,
    })
}

/// Re-derives the complete physical catalog transition from the verified
/// parent root, the sealed inventory, and the fresh replacement anchor. This
/// checks untouched scopes without materializing the parent's full catalog.
pub(crate) async fn validate_current_state_catalog_transition_root(
    store: &(impl StorageAdapterRead + ?Sized),
    state: &CommitStateManifest,
    parent: Option<&CommitStateManifest>,
) -> Result<(), LixError> {
    if state.current_state_catalog.is_none() {
        return Ok(());
    }
    if state.current_state_coverage_anchor.is_none() {
        let mut writes = StorageWriteSet::new();
        let expected = stage_current_state_catalog(
            store,
            &mut writes,
            parent,
            state.commit_id,
            &state.account_id,
            &state.mutations,
        )
        .await?;
        if expected.root.as_ref() != state.current_state_catalog.as_deref() {
            return Err(directory_error(
                "sparse catalog result is not the canonical parent mutation transition",
            ));
        }
        return Ok(());
    }
    let parent = parent.and_then(|parent| parent.current_state_catalog.as_deref());
    let replacement =
        state
            .current_state_coverage_anchor
            .as_ref()
            .map(|anchor| CurrentStatePartSet {
                scope: anchor.scope.clone(),
                generation_integrity_digest: anchor.generation_integrity_digest,
                state_lineage_digest: anchor.state_lineage_digest,
                directory: anchor.directory.clone(),
            });
    let touched = crate::tracked_state::storage::commit_state_inventory_exact_touched_scopes(
        state.commit_id,
        &state.mutations,
    )?;
    let mut writes = StorageWriteSet::new();
    let expected = if let Some(mut touched) = touched {
        touched.sort();
        touched.dedup();
        let mut root = parent.cloned();
        let mut staged = std::collections::BTreeMap::new();
        for scope in touched {
            if replacement
                .as_ref()
                .is_some_and(|entry| entry.scope == scope)
            {
                continue;
            }
            root =
                update_catalog_entry(store, &mut writes, &mut staged, root.as_ref(), &scope, None)
                    .await?;
        }
        if let Some(entry) = replacement {
            let scope = entry.scope.clone();
            root = update_catalog_entry(
                store,
                &mut writes,
                &mut staged,
                root.as_ref(),
                &scope,
                Some(entry),
            )
            .await?;
        }
        root
    } else {
        replacement
            .map(|entry| stage_catalog_from_entries(&mut writes, vec![entry]))
            .transpose()?
    };
    let actual = state.current_state_catalog.as_ref();
    if expected
        .as_ref()
        .map(|root| (root.root_id, root.entry_count))
        != actual.map(|root| (root.root_id, root.entry_count))
    {
        return Err(directory_error(
            "catalog result is not the canonical parent mutation transition",
        ));
    }
    Ok(())
}

pub(super) fn coverage_anchor_from_entry(
    entry: &CurrentStatePartSet,
) -> CurrentStateCoverageAnchor {
    CurrentStateCoverageAnchor {
        scope: entry.scope.clone(),
        generation_integrity_digest: entry.generation_integrity_digest,
        state_lineage_digest: entry.state_lineage_digest,
        directory: entry.directory.clone(),
    }
}

pub(crate) fn stage_fresh_current_state_catalog(
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<CertifiedCurrentStateCatalogPublication, LixError> {
    let Some(entry) =
        stage_complete_replacement_current_state_part_set(writes, commit_id, inventory)?
    else {
        return Ok(CertifiedCurrentStateCatalogPublication {
            write_set_id: writes.identity(),
            parent_commit_id: None,
            root: None,
            anchor: None,
        });
    };
    let anchor = coverage_anchor_from_entry(&entry);
    let root = attest_catalog_root(
        stage_catalog_from_entries(writes, vec![entry])?,
        None,
        commit_id,
        inventory,
    )?;
    Ok(CertifiedCurrentStateCatalogPublication {
        write_set_id: writes.identity(),
        parent_commit_id: None,
        root: Some(root),
        anchor: Some(anchor),
    })
}

pub(super) fn stage_catalog_from_entries(
    writes: &mut StorageWriteSet,
    entries: Vec<CurrentStatePartSet>,
) -> Result<CurrentStateCatalogRoot, LixError> {
    let mut staged = std::collections::BTreeMap::new();
    let child = stage_catalog_subtree(writes, &mut staged, 0, entries)?;
    flush_reachable_staged_catalog_nodes(writes, &staged, child.node_id)?;
    Ok(CurrentStateCatalogRoot {
        root_id: child.node_id,
        entry_count: child.entry_count,
        parent_root_id: None,
        transition_digest: [0; 32],
    })
}

async fn update_catalog_entry(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    staged: &mut std::collections::BTreeMap<[u8; 32], CatalogNode>,
    root: Option<&CurrentStateCatalogRoot>,
    scope: &crate::tracked_state::types::CommitDeltaReplacementScope,
    replacement: Option<CurrentStatePartSet>,
) -> Result<Option<CurrentStateCatalogRoot>, LixError> {
    let route_key = catalog_scope_key(scope)?;
    let Some(root) = root else {
        let Some(entry) = replacement else {
            return Ok(None);
        };
        let child = stage_catalog_subtree(writes, staged, 0, vec![entry])?;
        return Ok(Some(CurrentStateCatalogRoot {
            root_id: child.node_id,
            entry_count: child.entry_count,
            parent_root_id: None,
            transition_digest: [0; 32],
        }));
    };
    let mut path = Vec::<(CatalogNode, usize)>::new();
    let mut node_id = root.root_id;
    let mut node = load_catalog_node_with_staged(
        store,
        staged.get(&node_id).cloned(),
        writes.staged_value(CURRENT_STATE_CATALOG_SPACE, &node_id),
        node_id,
    )
    .await?;
    if node.entry_count()? != root.entry_count || node.depth != 0 {
        return Err(directory_error("catalog root summary mismatch"));
    }
    while !node.children.is_empty() {
        let depth = usize::try_from(node.depth).expect("u32 fits usize");
        let selector = *route_key
            .get(depth)
            .ok_or_else(|| directory_error("catalog exceeds its hash depth"))?;
        match node
            .children
            .binary_search_by_key(&selector, |child| child.route[0])
        {
            Ok(child_index) => {
                let expected = node.children[child_index].clone();
                if route_key.get(depth..depth + expected.route.len())
                    != Some(expected.route.as_slice())
                {
                    let Some(entry) = replacement else {
                        return Ok(Some(root.clone()));
                    };
                    let existing = load_catalog_node_with_staged(
                        store,
                        staged.get(&expected.node_id).cloned(),
                        writes.staged_value(CURRENT_STATE_CATALOG_SPACE, &expected.node_id),
                        expected.node_id,
                    )
                    .await?;
                    validate_catalog_child(&node, &expected, &existing)?;
                    let mismatch = expected
                        .route
                        .iter()
                        .zip(route_key.iter().skip(depth))
                        .position(|(left, right)| left != right)
                        .unwrap_or_else(|| expected.route.len().min(route_key.len() - depth));
                    let divergence = depth + mismatch;
                    if divergence == depth {
                        let child =
                            stage_catalog_subtree(writes, staged, divergence + 1, vec![entry])?;
                        let child = catalog_child_for_parent(staged, depth, child.node_id)?;
                        node.children.insert(child_index, child);
                        node.children.sort_by_key(|child| child.route[0]);
                        let node_id = stage_catalog_node(writes, staged, node)?;
                        return rebuild_catalog_path(store, writes, staged, path, Some(node_id))
                            .await;
                    }
                    let mut new_child =
                        stage_catalog_subtree(writes, staged, divergence + 1, vec![entry])?;
                    new_child = catalog_child_for_parent(staged, divergence, new_child.node_id)?;
                    let old_child = catalog_child_for_parent(staged, divergence, expected.node_id)
                        .or_else(|_| {
                            let route = existing.sample_key[divergence
                                ..usize::try_from(existing.depth).expect("u32 fits usize")]
                                .to_vec();
                            Ok::<CatalogChild, LixError>(CatalogChild {
                                route,
                                node_id: expected.node_id,
                                entry_count: expected.entry_count,
                            })
                        })?;
                    let mut children = vec![old_child, new_child];
                    children.sort_by_key(|child| child.route[0]);
                    let branch = CatalogNode {
                        depth: u32::try_from(divergence).expect("catalog depth fits u32"),
                        sample_key: existing.sample_key.clone(),
                        entries: Vec::new(),
                        children,
                    };
                    let branch_id = stage_catalog_node(writes, staged, branch)?;
                    node.children[child_index] =
                        catalog_child_for_parent(staged, depth, branch_id)?;
                    let node_id = stage_catalog_node(writes, staged, node)?;
                    return rebuild_catalog_path(store, writes, staged, path, Some(node_id)).await;
                }
                path.push((node, child_index));
                node_id = expected.node_id;
                node = load_catalog_node_with_staged(
                    store,
                    staged.get(&node_id).cloned(),
                    writes.staged_value(CURRENT_STATE_CATALOG_SPACE, &node_id),
                    node_id,
                )
                .await?;
                validate_catalog_child(
                    &path.last().expect("catalog path exists").0,
                    &expected,
                    &node,
                )?;
            }
            Err(child_index) => {
                let Some(entry) = replacement else {
                    return Ok(Some(root.clone()));
                };
                let child = stage_catalog_subtree(writes, staged, depth + 1, vec![entry])?;
                let child = catalog_child_for_parent(staged, depth, child.node_id)?;
                node.children.insert(child_index, child);
                node_id = stage_catalog_node(writes, staged, node)?;
                return rebuild_catalog_path(store, writes, staged, path, Some(node_id)).await;
            }
        }
    }

    let entry_index = node
        .entries
        .binary_search_by(|entry| entry.scope.cmp(scope));
    match (entry_index, replacement) {
        (Ok(index), Some(entry)) => node.entries[index] = entry,
        (Err(index), Some(entry)) => node.entries.insert(index, entry),
        (Ok(index), None) => {
            node.entries.remove(index);
        }
        (Err(_), None) => return Ok(Some(root.clone())),
    }
    let child = if node.entries.is_empty() {
        None
    } else {
        Some(stage_catalog_subtree(
            writes,
            staged,
            usize::try_from(node.depth).expect("u32 fits usize"),
            node.entries,
        )?)
    };
    rebuild_catalog_path(
        store,
        writes,
        staged,
        path,
        child.map(|child| child.node_id),
    )
    .await
}

async fn rebuild_catalog_path(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    staged: &mut std::collections::BTreeMap<[u8; 32], CatalogNode>,
    mut path: Vec<(CatalogNode, usize)>,
    mut child_id: Option<[u8; 32]>,
) -> Result<Option<CurrentStateCatalogRoot>, LixError> {
    while let Some((mut parent, child_index)) = path.pop() {
        match child_id {
            Some(node_id) => {
                parent.children[child_index] = catalog_child_for_parent_with_store(
                    store,
                    staged,
                    usize::try_from(parent.depth).expect("u32 fits usize"),
                    node_id,
                )
                .await?;
            }
            None => {
                parent.children.remove(child_index);
            }
        }
        child_id = if parent.children.is_empty() {
            None
        } else if parent.children.len() == 1 && parent.depth != 0 {
            Some(parent.children[0].node_id)
        } else if parent
            .children
            .iter()
            .try_fold(0u32, |total, child| total.checked_add(child.entry_count))
            .is_some_and(|count| count <= CATALOG_LEAF_MAX_ENTRIES as u32)
        {
            let mut entries = Vec::new();
            let mut pending = parent
                .children
                .iter()
                .map(|child| child.node_id)
                .collect::<Vec<_>>();
            while let Some(node_id) = pending.pop() {
                let mut node = load_catalog_node_with_staged(
                    store,
                    staged.get(&node_id).cloned(),
                    writes.staged_value(CURRENT_STATE_CATALOG_SPACE, &node_id),
                    node_id,
                )
                .await?;
                if node.children.is_empty() {
                    entries.append(&mut node.entries);
                } else {
                    pending.extend(node.children.into_iter().map(|child| child.node_id));
                }
            }
            Some(
                stage_catalog_subtree(
                    writes,
                    staged,
                    usize::try_from(parent.depth).expect("catalog depth fits usize"),
                    entries,
                )?
                .node_id,
            )
        } else {
            Some(stage_catalog_node(writes, staged, parent)?)
        };
    }
    let Some(root_id) = child_id else {
        return Ok(None);
    };
    let entry_count = staged
        .get(&root_id)
        .ok_or_else(|| directory_error("staged catalog root is missing"))?
        .entry_count()?;
    Ok(Some(CurrentStateCatalogRoot {
        root_id,
        entry_count,
        parent_root_id: None,
        transition_digest: [0; 32],
    }))
}

fn stage_catalog_subtree(
    writes: &mut StorageWriteSet,
    staged: &mut std::collections::BTreeMap<[u8; 32], CatalogNode>,
    depth: usize,
    mut entries: Vec<CurrentStatePartSet>,
) -> Result<CatalogChild, LixError> {
    entries.sort_by(|left, right| left.scope.cmp(&right.scope));
    if entries
        .windows(2)
        .any(|pair| pair[0].scope == pair[1].scope)
    {
        return Err(directory_error("catalog contains duplicate scopes"));
    }
    if entries.len() <= CATALOG_LEAF_MAX_ENTRIES {
        let sample_key = catalog_scope_key(&entries[0].scope)?;
        let node = CatalogNode {
            depth: u32::try_from(depth).expect("catalog depth fits u32"),
            sample_key,
            entries,
            children: Vec::new(),
        };
        let entry_count = node.entry_count()?;
        let node_id = stage_catalog_node(writes, staged, node)?;
        return Ok(CatalogChild {
            route: Vec::new(),
            node_id,
            entry_count,
        });
    }
    let mut groups = std::collections::BTreeMap::<u8, Vec<CurrentStatePartSet>>::new();
    for entry in entries {
        let route_key = catalog_scope_key(&entry.scope)?;
        let selector = *route_key
            .get(depth)
            .ok_or_else(|| directory_error("duplicate canonical catalog scope key"))?;
        groups.entry(selector).or_default().push(entry);
    }
    if groups.len() == 1 {
        let (selector, entries) = groups
            .into_iter()
            .next()
            .expect("one catalog route group exists");
        let keys = entries
            .iter()
            .map(|entry| catalog_scope_key(&entry.scope))
            .collect::<Result<Vec<_>, _>>()?;
        let shortest = keys.iter().map(Vec::len).min().unwrap_or(depth + 1);
        let mut divergence = depth + 1;
        while divergence < shortest
            && keys
                .iter()
                .all(|key| key[divergence] == keys[0][divergence])
        {
            divergence += 1;
        }
        let child = stage_catalog_subtree(writes, staged, divergence, entries)?;
        let child = catalog_child_for_parent(staged, depth, child.node_id)?;
        debug_assert_eq!(child.route[0], selector);
        if depth == 0 {
            let sample_key = staged
                .get(&child.node_id)
                .expect("staged catalog child exists")
                .sample_key
                .clone();
            let root = CatalogNode {
                depth: 0,
                sample_key,
                entries: Vec::new(),
                children: vec![child],
            };
            let entry_count = root.entry_count()?;
            let node_id = stage_catalog_node(writes, staged, root)?;
            return Ok(CatalogChild {
                route: Vec::new(),
                node_id,
                entry_count,
            });
        }
        return Ok(child);
    }
    let mut children = Vec::with_capacity(groups.len());
    for (selector, entries) in groups {
        let child = stage_catalog_subtree(writes, staged, depth + 1, entries)?;
        let child = catalog_child_for_parent(staged, depth, child.node_id)?;
        debug_assert_eq!(child.route[0], selector);
        children.push(child);
    }
    let sample_key = staged
        .get(&children[0].node_id)
        .expect("staged catalog child exists")
        .sample_key
        .clone();
    let node = CatalogNode {
        depth: u32::try_from(depth).expect("catalog depth fits u32"),
        sample_key,
        entries: Vec::new(),
        children,
    };
    let entry_count = node.entry_count()?;
    let node_id = stage_catalog_node(writes, staged, node)?;
    Ok(CatalogChild {
        route: Vec::new(),
        node_id,
        entry_count,
    })
}

fn stage_catalog_node(
    _writes: &mut StorageWriteSet,
    staged: &mut std::collections::BTreeMap<[u8; 32], CatalogNode>,
    node: CatalogNode,
) -> Result<[u8; 32], LixError> {
    validate_catalog_node(&node)?;
    let payload = storage_codec::encode("current-state catalog node", &node)?;
    if payload.len() > DIRECTORY_NODE_MAX_DECODED_BYTES {
        return Err(directory_error(
            "catalog node exceeds its decoded size bound",
        ));
    }
    let mut encoded = Vec::with_capacity(CATALOG_NODE_MAGIC.len() + payload.len());
    encoded.extend_from_slice(CATALOG_NODE_MAGIC);
    encoded.extend_from_slice(&payload);
    let bytes = Bytes::from(encoded);
    let node_id = catalog_node_digest(&bytes);
    staged.insert(node_id, node);
    Ok(node_id)
}

fn flush_reachable_staged_catalog_nodes(
    writes: &mut StorageWriteSet,
    staged: &std::collections::BTreeMap<[u8; 32], CatalogNode>,
    root_id: [u8; 32],
) -> Result<(), LixError> {
    let mut pending = vec![root_id];
    let mut reachable = std::collections::BTreeSet::new();
    while let Some(node_id) = pending.pop() {
        if !reachable.insert(node_id) {
            continue;
        }
        let Some(node) = staged.get(&node_id) else {
            continue;
        };
        pending.extend(node.children.iter().map(|child| child.node_id));
    }
    for node_id in reachable {
        let Some(node) = staged.get(&node_id) else {
            continue;
        };
        let payload = storage_codec::encode("current-state catalog node", node)?;
        let mut encoded = Vec::with_capacity(CATALOG_NODE_MAGIC.len() + payload.len());
        encoded.extend_from_slice(CATALOG_NODE_MAGIC);
        encoded.extend_from_slice(&payload);
        let bytes = Bytes::from(encoded);
        if catalog_node_digest(&bytes) != node_id {
            return Err(directory_error("staged catalog node digest drifted"));
        }
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_crud_current_state_catalog_bytes(bytes.len());
        writes.put(
            CURRENT_STATE_CATALOG_SPACE,
            StorageKey(Bytes::copy_from_slice(&node_id)),
            StorageValue { bytes },
        );
    }
    Ok(())
}

fn catalog_child_for_parent(
    staged: &std::collections::BTreeMap<[u8; 32], CatalogNode>,
    parent_depth: usize,
    node_id: [u8; 32],
) -> Result<CatalogChild, LixError> {
    let node = staged
        .get(&node_id)
        .ok_or_else(|| directory_error("staged catalog child is missing"))?;
    let child_depth = usize::try_from(node.depth).expect("u32 fits usize");
    if child_depth <= parent_depth || child_depth > node.sample_key.len() {
        return Err(directory_error(
            "catalog child has an invalid compressed route",
        ));
    }
    Ok(CatalogChild {
        route: node.sample_key[parent_depth..child_depth].to_vec(),
        node_id,
        entry_count: node.entry_count()?,
    })
}

async fn catalog_child_for_parent_with_store(
    store: &(impl StorageAdapterRead + ?Sized),
    staged: &std::collections::BTreeMap<[u8; 32], CatalogNode>,
    parent_depth: usize,
    node_id: [u8; 32],
) -> Result<CatalogChild, LixError> {
    if staged.contains_key(&node_id) {
        return catalog_child_for_parent(staged, parent_depth, node_id);
    }
    let node = load_catalog_node(store, node_id).await?;
    let child_depth = usize::try_from(node.depth).expect("u32 fits usize");
    if child_depth <= parent_depth || child_depth > node.sample_key.len() {
        return Err(directory_error(
            "catalog child has an invalid compressed route",
        ));
    }
    Ok(CatalogChild {
        route: node.sample_key[parent_depth..child_depth].to_vec(),
        node_id,
        entry_count: node.entry_count()?,
    })
}

fn validate_catalog_child(
    parent: &CatalogNode,
    expected: &CatalogChild,
    child: &CatalogNode,
) -> Result<(), LixError> {
    let parent_depth = usize::try_from(parent.depth).expect("u32 fits usize");
    let child_depth = usize::try_from(child.depth).expect("u32 fits usize");
    if child.entry_count()? != expected.entry_count
        || child_depth <= parent_depth
        || child_depth > child.sample_key.len()
        || child.sample_key.get(parent_depth..child_depth) != Some(expected.route.as_slice())
    {
        return Err(directory_error("catalog child summary mismatch"));
    }
    Ok(())
}

async fn load_catalog_node_with_staged(
    store: &(impl StorageAdapterRead + ?Sized),
    staged: Option<CatalogNode>,
    staged_bytes: Option<Bytes>,
    node_id: [u8; 32],
) -> Result<CatalogNode, LixError> {
    if let Some(node) = staged {
        return Ok(node);
    }
    if let Some(bytes) = staged_bytes {
        return decode_catalog_node(&bytes, node_id);
    }
    load_catalog_node(store, node_id).await
}

async fn load_catalog_node(
    store: &(impl StorageAdapterRead + ?Sized),
    node_id: [u8; 32],
) -> Result<CatalogNode, LixError> {
    let key = StorageKey(Bytes::copy_from_slice(&node_id));
    let result = PointReadPlan::new(CURRENT_STATE_CATALOG_SPACE, &[key])
        .materialize(store, StorageGetOptions::default())
        .await?;
    let value = result
        .value
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| directory_error("catalog references a missing node"))?;
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(directory_error("catalog node read omitted its value"));
    };
    decode_catalog_node(&bytes, node_id)
}

fn decode_catalog_node(bytes: &[u8], node_id: [u8; 32]) -> Result<CatalogNode, LixError> {
    if catalog_node_digest(bytes) != node_id {
        return Err(directory_error("catalog node content digest mismatch"));
    }
    let payload = bytes
        .strip_prefix(CATALOG_NODE_MAGIC)
        .ok_or_else(|| directory_error("catalog node has an unsupported format"))?;
    let node: CatalogNode = storage_codec::decode("current-state catalog node", payload)?;
    validate_catalog_node(&node)?;
    Ok(node)
}

pub(crate) async fn load_current_state_catalog_entry(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &CurrentStateCatalogRoot,
    scope: &crate::tracked_state::types::CommitDeltaReplacementScope,
) -> Result<Option<CurrentStatePartSet>, LixError> {
    let route_key = catalog_scope_key(scope)?;
    let mut expected_count = root.entry_count;
    let mut minimum_depth = 0u32;
    let mut expected_route: Option<(usize, Vec<u8>)> = None;
    let mut node_id = root.root_id;
    loop {
        let node = load_catalog_node(store, node_id).await?;
        if node.entry_count()? != expected_count
            || node.depth < minimum_depth
            || (minimum_depth == 0 && node.depth != 0)
        {
            return Err(directory_error("catalog node summary mismatch"));
        }
        if let Some((parent_depth, route)) = expected_route.take() {
            let child_depth = usize::try_from(node.depth).expect("u32 fits usize");
            if child_depth > node.sample_key.len()
                || node.sample_key.get(parent_depth..child_depth) != Some(route.as_slice())
            {
                return Err(directory_error("catalog child route mismatch"));
            }
        }
        if node.children.is_empty() {
            return Ok(node
                .entries
                .binary_search_by(|entry| entry.scope.cmp(scope))
                .ok()
                .map(|index| node.entries[index].clone()));
        }
        let depth = usize::try_from(node.depth).expect("u32 fits usize");
        let selector = *route_key
            .get(depth)
            .ok_or_else(|| directory_error("catalog exceeds its hash depth"))?;
        let Ok(index) = node
            .children
            .binary_search_by_key(&selector, |child| child.route[0])
        else {
            return Ok(None);
        };
        let child = &node.children[index];
        if route_key.get(depth..depth + child.route.len()) != Some(child.route.as_slice()) {
            return Ok(None);
        }
        node_id = child.node_id;
        expected_count = child.entry_count;
        minimum_depth = node.depth.saturating_add(1);
        expected_route = Some((depth, child.route.clone()));
    }
}

async fn load_current_state_catalog_entry_for_write(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    root: &CurrentStateCatalogRoot,
    scope: &crate::tracked_state::types::CommitDeltaReplacementScope,
) -> Result<Option<CurrentStatePartSet>, LixError> {
    let route_key = catalog_scope_key(scope)?;
    let mut expected_count = root.entry_count;
    let mut minimum_depth = 0u32;
    let mut expected_route: Option<(usize, Vec<u8>)> = None;
    let mut node_id = root.root_id;
    loop {
        let staged = writes.staged_value(CURRENT_STATE_CATALOG_SPACE, &node_id);
        let node = load_catalog_node_with_staged(store, None, staged, node_id).await?;
        if node.entry_count()? != expected_count
            || node.depth < minimum_depth
            || (minimum_depth == 0 && node.depth != 0)
        {
            return Err(directory_error("catalog node summary mismatch"));
        }
        if let Some((parent_depth, route)) = expected_route.take() {
            let child_depth = usize::try_from(node.depth).expect("u32 fits usize");
            if child_depth > node.sample_key.len()
                || node.sample_key.get(parent_depth..child_depth) != Some(route.as_slice())
            {
                return Err(directory_error("catalog child route mismatch"));
            }
        }
        if node.children.is_empty() {
            return Ok(node
                .entries
                .binary_search_by(|entry| entry.scope.cmp(scope))
                .ok()
                .map(|index| node.entries[index].clone()));
        }
        let depth = usize::try_from(node.depth).expect("u32 fits usize");
        let selector = *route_key
            .get(depth)
            .ok_or_else(|| directory_error("catalog exceeds its hash depth"))?;
        let Ok(index) = node
            .children
            .binary_search_by_key(&selector, |child| child.route[0])
        else {
            return Ok(None);
        };
        let child = &node.children[index];
        if route_key.get(depth..depth + child.route.len()) != Some(child.route.as_slice()) {
            return Ok(None);
        }
        node_id = child.node_id;
        expected_count = child.entry_count;
        minimum_depth = node.depth.saturating_add(1);
        expected_route = Some((depth, child.route.clone()));
    }
}

/// Resolves catalog scopes in caller order while reading each shared trie node
/// at most once. Each frontier is issued as one storage point-read batch.
pub(crate) async fn load_current_state_catalog_entries_for_scopes(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &CurrentStateCatalogRoot,
    scopes: &[crate::tracked_state::types::CommitDeltaReplacementScope],
) -> Result<Vec<Option<CurrentStatePartSet>>, LixError> {
    let mut loaded =
        load_current_state_catalog_entries_for_scope_sets(store, &[(root, scopes)]).await?;
    Ok(loaded.pop().unwrap_or_default())
}

/// Resolves multiple caller-owned scope batches while reading every shared
/// catalog node at most once. Results retain request, scope, and duplicate order.
pub(crate) async fn load_current_state_catalog_entries_for_scope_sets(
    store: &(impl StorageAdapterRead + ?Sized),
    requests: &[(
        &CurrentStateCatalogRoot,
        &[crate::tracked_state::types::CommitDeltaReplacementScope],
    )],
) -> Result<Vec<Vec<Option<CurrentStatePartSet>>>, LixError> {
    #[derive(Clone)]
    struct PendingCatalogNode {
        request_index: usize,
        expected_count: u32,
        minimum_depth: u32,
        expected_route: Option<(usize, Vec<u8>)>,
        scope_indices: Vec<usize>,
    }

    let route_keys = requests
        .iter()
        .map(|(_, scopes)| {
            scopes
                .iter()
                .map(catalog_scope_key)
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut results = requests
        .iter()
        .map(|(_, scopes)| vec![None; scopes.len()])
        .collect::<Vec<_>>();
    let mut frontier = std::collections::BTreeMap::<[u8; 32], Vec<PendingCatalogNode>>::new();
    for (request_index, (root, scopes)) in requests.iter().enumerate() {
        if !scopes.is_empty() {
            frontier
                .entry(root.root_id)
                .or_default()
                .push(PendingCatalogNode {
                    request_index,
                    expected_count: root.entry_count,
                    minimum_depth: 0,
                    expected_route: None,
                    scope_indices: (0..scopes.len()).collect(),
                });
        }
    }
    let mut cache = std::collections::BTreeMap::<[u8; 32], CatalogNode>::new();
    let mut visited = std::collections::BTreeSet::<(usize, [u8; 32])>::new();

    while !frontier.is_empty() {
        let unseen = frontier
            .keys()
            .filter(|node_id| !cache.contains_key(*node_id))
            .copied()
            .collect::<Vec<_>>();
        let storage_keys = unseen
            .iter()
            .map(|node_id| StorageKey(Bytes::copy_from_slice(node_id)))
            .collect::<Vec<_>>();
        if !storage_keys.is_empty() {
            let loaded = PointReadPlan::new(CURRENT_STATE_CATALOG_SPACE, &storage_keys)
                .materialize(store, StorageGetOptions::default())
                .await?;
            for (node_id, value) in unseen.into_iter().zip(loaded.value) {
                let value =
                    value.ok_or_else(|| directory_error("catalog references a missing node"))?;
                let StorageProjectedValue::FullValue(bytes) = value else {
                    return Err(directory_error("catalog node read omitted its value"));
                };
                if catalog_node_digest(&bytes) != node_id {
                    return Err(directory_error("catalog node content digest mismatch"));
                }
                let payload = bytes
                    .strip_prefix(CATALOG_NODE_MAGIC)
                    .ok_or_else(|| directory_error("catalog node has an unsupported format"))?;
                let node: CatalogNode =
                    storage_codec::decode("current-state catalog node", payload)?;
                validate_catalog_node(&node)?;
                cache.insert(node_id, node);
            }
        }
        let mut next = std::collections::BTreeMap::<[u8; 32], Vec<PendingCatalogNode>>::new();

        for (node_id, pending_requests) in frontier {
            let node = cache
                .get(&node_id)
                .ok_or_else(|| directory_error("catalog frontier omitted a node"))?;
            for pending in pending_requests {
                if !visited.insert((pending.request_index, node_id)) {
                    return Err(directory_error(
                        "catalog scoped lookup contains a cycle or duplicate child",
                    ));
                }
                if node.entry_count()? != pending.expected_count
                    || node.depth < pending.minimum_depth
                    || (pending.minimum_depth == 0 && node.depth != 0)
                {
                    return Err(directory_error("catalog node summary mismatch"));
                }
                if let Some((parent_depth, route)) = pending.expected_route {
                    let child_depth = usize::try_from(node.depth).expect("u32 fits usize");
                    if child_depth > node.sample_key.len()
                        || node.sample_key.get(parent_depth..child_depth) != Some(route.as_slice())
                    {
                        return Err(directory_error("catalog child route mismatch"));
                    }
                }

                if node.children.is_empty() {
                    let scopes = requests[pending.request_index].1;
                    for scope_index in pending.scope_indices {
                        results[pending.request_index][scope_index] = node
                            .entries
                            .binary_search_by(|entry| entry.scope.cmp(&scopes[scope_index]))
                            .ok()
                            .map(|index| node.entries[index].clone());
                    }
                    continue;
                }

                let depth = usize::try_from(node.depth).expect("u32 fits usize");
                for scope_index in pending.scope_indices {
                    let route_key = &route_keys[pending.request_index][scope_index];
                    let Some(&selector) = route_key.get(depth) else {
                        // The query cannot lie below this compressed prefix.
                        continue;
                    };
                    let Ok(child_index) = node
                        .children
                        .binary_search_by_key(&selector, |child| child.route[0])
                    else {
                        continue;
                    };
                    let child = &node.children[child_index];
                    if route_key.get(depth..depth + child.route.len())
                        != Some(child.route.as_slice())
                    {
                        continue;
                    }
                    let child_pending = next.entry(child.node_id).or_default();
                    if let Some(existing) = child_pending
                        .iter_mut()
                        .find(|existing| existing.request_index == pending.request_index)
                    {
                        if existing.expected_count != child.entry_count
                            || existing.minimum_depth != node.depth.saturating_add(1)
                            || existing.expected_route.as_ref()
                                != Some(&(depth, child.route.clone()))
                        {
                            return Err(directory_error(
                                "catalog shared child has conflicting summaries",
                            ));
                        }
                        existing.scope_indices.push(scope_index);
                    } else {
                        child_pending.push(PendingCatalogNode {
                            request_index: pending.request_index,
                            expected_count: child.entry_count,
                            minimum_depth: node.depth.saturating_add(1),
                            expected_route: Some((depth, child.route.clone())),
                            scope_indices: vec![scope_index],
                        });
                    }
                }
            }
        }
        frontier = next;
    }

    Ok(results)
}

pub(crate) async fn load_current_state_catalog_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &CurrentStateCatalogRoot,
) -> Result<Vec<CurrentStatePartSet>, LixError> {
    let mut pending = vec![(
        root.root_id,
        root.entry_count,
        0u32,
        None::<(usize, Vec<u8>)>,
    )];
    let mut entries = Vec::with_capacity(root.entry_count as usize);
    while let Some((node_id, expected_count, minimum_depth, expected_route)) = pending.pop() {
        let mut node = load_catalog_node(store, node_id).await?;
        if node.entry_count()? != expected_count
            || node.depth < minimum_depth
            || (minimum_depth == 0 && node.depth != 0)
        {
            return Err(directory_error("catalog node summary mismatch"));
        }
        if let Some((parent_depth, route)) = expected_route {
            let child_depth = usize::try_from(node.depth).expect("u32 fits usize");
            if child_depth > node.sample_key.len()
                || node.sample_key.get(parent_depth..child_depth) != Some(route.as_slice())
            {
                return Err(directory_error("catalog child route mismatch"));
            }
        }
        if node.children.is_empty() {
            entries.append(&mut node.entries);
        } else {
            let child_min_depth = node.depth.saturating_add(1);
            let parent_depth = usize::try_from(node.depth).expect("u32 fits usize");
            pending.extend(node.children.into_iter().rev().map(|child| {
                (
                    child.node_id,
                    child.entry_count,
                    child_min_depth,
                    Some((parent_depth, child.route)),
                )
            }));
        }
    }
    if entries.len() != root.entry_count as usize {
        return Err(directory_error("catalog root entry count mismatch"));
    }
    entries.sort_by(|left, right| left.scope.cmp(&right.scope));
    if entries
        .windows(2)
        .any(|pair| pair[0].scope == pair[1].scope)
    {
        return Err(directory_error("catalog contains duplicate scopes"));
    }
    Ok(entries)
}

pub(crate) async fn load_current_state_catalog_reachability(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &CurrentStateCatalogRoot,
) -> Result<
    (
        std::collections::BTreeSet<[u8; 32]>,
        Vec<CurrentStatePartSet>,
    ),
    LixError,
> {
    let (nodes, mut entries) =
        load_current_state_catalog_reachability_many(store, std::slice::from_ref(root)).await?;
    entries.sort_by(|left, right| left.scope.cmp(&right.scope));
    if entries.len() != root.entry_count as usize
        || entries
            .windows(2)
            .any(|pair| pair[0].scope == pair[1].scope)
    {
        return Err(directory_error(
            "catalog reachability count or scope mismatch",
        ));
    }
    Ok((nodes, entries))
}

pub(crate) async fn load_current_state_catalog_reachability_many(
    store: &(impl StorageAdapterRead + ?Sized),
    roots: &[CurrentStateCatalogRoot],
) -> Result<
    (
        std::collections::BTreeSet<[u8; 32]>,
        Vec<CurrentStatePartSet>,
    ),
    LixError,
> {
    type ExpectedCatalogNode = (u32, u32, Option<(usize, Vec<u8>)>, bool);
    let mut pending = std::collections::BTreeMap::<[u8; 32], Vec<ExpectedCatalogNode>>::new();
    for root in roots {
        pending
            .entry(root.root_id)
            .or_default()
            .push((root.entry_count, 0, None, true));
    }
    let mut node_ids = std::collections::BTreeSet::new();
    let mut node_cache = std::collections::BTreeMap::<[u8; 32], CatalogNode>::new();
    let mut entries = Vec::new();
    while !pending.is_empty() {
        let frontier = std::mem::take(&mut pending);
        let unseen = frontier
            .keys()
            .filter(|node_id| !node_cache.contains_key(*node_id))
            .copied()
            .collect::<Vec<_>>();
        let storage_keys = unseen
            .iter()
            .map(|node_id| StorageKey(Bytes::copy_from_slice(node_id)))
            .collect::<Vec<_>>();
        let loaded = PointReadPlan::new(CURRENT_STATE_CATALOG_SPACE, &storage_keys)
            .materialize(store, StorageGetOptions::default())
            .await?;
        for (node_id, value) in unseen.into_iter().zip(loaded.value) {
            let value =
                value.ok_or_else(|| directory_error("catalog references a missing node"))?;
            let StorageProjectedValue::FullValue(bytes) = value else {
                return Err(directory_error("catalog node read omitted its value"));
            };
            if catalog_node_digest(&bytes) != node_id {
                return Err(directory_error("catalog node content digest mismatch"));
            }
            let payload = bytes
                .strip_prefix(CATALOG_NODE_MAGIC)
                .ok_or_else(|| directory_error("catalog node has an unsupported format"))?;
            let node: CatalogNode = storage_codec::decode("current-state catalog node", payload)?;
            validate_catalog_node(&node)?;
            node_cache.insert(node_id, node);
        }
        for (node_id, expectations) in frontier {
            let already_expanded = !node_ids.insert(node_id);
            let node = node_cache
                .get(&node_id)
                .ok_or_else(|| directory_error("catalog frontier omitted a node"))?;
            for (expected_count, expected_depth, expected_route, is_root) in expectations {
                if node.entry_count()? != expected_count
                    || node.depth < expected_depth
                    || (is_root && node.depth != 0)
                {
                    return Err(directory_error("catalog child summary mismatch"));
                }
                if let Some((parent_depth, route)) = expected_route {
                    let child_depth = usize::try_from(node.depth).expect("u32 fits usize");
                    if child_depth > node.sample_key.len()
                        || node.sample_key.get(parent_depth..child_depth) != Some(route.as_slice())
                    {
                        return Err(directory_error("catalog child route mismatch"));
                    }
                }
            }
            if already_expanded {
                continue;
            }
            if node.children.is_empty() {
                entries.extend(node.entries.iter().cloned());
            } else {
                let child_min_depth = node.depth.saturating_add(1);
                let parent_depth = usize::try_from(node.depth).expect("u32 fits usize");
                for child in &node.children {
                    pending.entry(child.node_id).or_default().push((
                        child.entry_count,
                        child_min_depth,
                        Some((parent_depth, child.route.clone())),
                        false,
                    ));
                }
            }
        }
    }
    Ok((node_ids, entries))
}

pub(crate) async fn load_current_state_part_directory_reachability(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &CurrentStatePartDirectoryRoot,
) -> Result<
    (
        std::collections::BTreeSet<[u8; 32]>,
        Vec<CurrentStatePartDescriptor>,
    ),
    LixError,
> {
    let (nodes, mut descriptor_sets) =
        load_current_state_part_directory_reachability_many(store, std::slice::from_ref(root))
            .await?;
    Ok((nodes, descriptor_sets.pop().unwrap_or_default()))
}

pub(crate) async fn load_current_state_part_directory_reachability_many(
    store: &(impl StorageAdapterRead + ?Sized),
    roots: &[CurrentStatePartDirectoryRoot],
) -> Result<
    (
        std::collections::BTreeSet<[u8; 32]>,
        Vec<Vec<CurrentStatePartDescriptor>>,
    ),
    LixError,
> {
    let mut frontier =
        std::collections::BTreeMap::<[u8; 32], Vec<(usize, Option<DirectoryChild>)>>::new();
    for (root_index, root) in roots.iter().enumerate() {
        if root.part_count == 0 {
            validate_empty_root(root)?;
            continue;
        }
        frontier
            .entry(root.root_id)
            .or_default()
            .push((root_index, None));
    }
    let mut node_ids = std::collections::BTreeSet::new();
    let mut visited = std::collections::BTreeSet::<(usize, [u8; 32])>::new();
    let mut cache = std::collections::BTreeMap::<[u8; 32], DirectoryNode>::new();
    let mut descriptor_sets = roots
        .iter()
        .map(|root| Vec::with_capacity(root.part_count as usize))
        .collect::<Vec<_>>();
    while !frontier.is_empty() {
        let unseen = frontier
            .keys()
            .filter(|node_id| !cache.contains_key(*node_id))
            .copied()
            .collect::<Vec<_>>();
        let loaded = load_directory_nodes(store, &unseen).await?;
        cache.extend(unseen.into_iter().zip(loaded));
        let mut next =
            std::collections::BTreeMap::<[u8; 32], Vec<(usize, Option<DirectoryChild>)>>::new();
        for (node_id, expectations) in frontier {
            node_ids.insert(node_id);
            let node = cache
                .get(&node_id)
                .ok_or_else(|| directory_error("directory frontier omitted a node"))?;
            for (root_index, expected_child) in expectations {
                if !visited.insert((root_index, node_id)) {
                    return Err(directory_error(
                        "part directory contains a cycle or duplicate child",
                    ));
                }
                let root = &roots[root_index];
                if expected_child.is_none() {
                    validate_root_summary(root, node)?;
                }
                if let Some(expected) = expected_child.as_ref() {
                    validate_child_summary(expected, node)?;
                }
                if node.kind == 0 {
                    descriptor_sets[root_index].extend(node.parts.iter().cloned());
                } else {
                    for child in &node.children {
                        next.entry(child.node_id)
                            .or_default()
                            .push((root_index, Some(child.clone())));
                    }
                }
            }
        }
        frontier = next;
    }
    for (root, descriptors) in roots.iter().zip(&mut descriptor_sets) {
        if root.part_count == 0 {
            validate_empty_root(root)?;
            continue;
        }
        // Frontier batching is content-ID ordered, so normalize the
        // caller-visible descriptor sequence before semantic validation.
        descriptors.sort_by(|left, right| left.first_key.cmp(&right.first_key));
        validate_descriptors(descriptors)?;
        if descriptors.len() != root.part_count as usize || !directory_digest_matches(root) {
            return Err(directory_error("part directory reachability mismatch"));
        }
    }
    Ok((node_ids, descriptor_sets))
}

fn validate_catalog_node(node: &CatalogNode) -> Result<(), LixError> {
    let depth = usize::try_from(node.depth).expect("u32 fits usize");
    let empty_directory_digest = empty_current_state_directory_digest();
    if depth > CATALOG_MAX_KEY_BYTES
        || node.sample_key.len() > CATALOG_MAX_KEY_BYTES
        || node.sample_key.len() < depth
        || node.entries.is_empty() == node.children.is_empty()
        || (node.children.len() == 1 && node.depth != 0)
        || (!node.entries.is_empty() && node.entries.len() > CATALOG_LEAF_MAX_ENTRIES)
        || node
            .entries
            .windows(2)
            .any(|pair| pair[0].scope >= pair[1].scope)
        || node.entries.iter().any(|entry| {
            let valid_empty_directory = entry.directory.root_id == [0; 32]
                && entry.directory.directory_digest == empty_directory_digest
                && entry.directory.row_count == 0
                && entry.directory.part_count == 0
                && entry.directory.tree_height == 0;
            let valid_nonempty_directory = entry.directory.root_id != [0; 32]
                && entry.directory.directory_digest != [0; 32]
                && entry.directory.row_count > 0
                && entry.directory.part_count > 0
                && entry.directory.tree_height > 0;
            entry.scope.schema_key.is_empty()
                || entry.generation_integrity_digest == [0; 32]
                || entry.state_lineage_digest == [0; 32]
                || !(valid_empty_directory || valid_nonempty_directory)
        })
        || node
            .children
            .windows(2)
            .any(|pair| pair[0].route.first() >= pair[1].route.first())
        || node.children.iter().any(|child| {
            child.route.is_empty()
                || child.node_id == [0; 32]
                || child.entry_count == 0
                || depth >= CATALOG_MAX_KEY_BYTES
        })
        || node.entries.iter().any(|entry| {
            let Ok(key) = catalog_scope_key(&entry.scope) else {
                return true;
            };
            key.get(..depth) != node.sample_key.get(..depth)
        })
    {
        return Err(directory_error("catalog node is structurally invalid"));
    }
    Ok(())
}

fn catalog_scope_key(
    scope: &crate::tracked_state::types::CommitDeltaReplacementScope,
) -> Result<Vec<u8>, LixError> {
    let file_len = scope.file_id.as_ref().map_or(0, String::len);
    let capacity = 1usize
        .checked_add(size_of::<u32>() * 2)
        .and_then(|value| value.checked_add(file_len))
        .and_then(|value| value.checked_add(scope.schema_key.len()))
        .ok_or_else(|| directory_error("catalog scope key length overflows"))?;
    if capacity > CATALOG_MAX_KEY_BYTES {
        return Err(directory_error("catalog scope key exceeds its bound"));
    }
    let mut encoded = Vec::with_capacity(capacity);
    match scope.file_id.as_deref() {
        Some(file_id) => {
            encoded.push(1);
            encoded.extend_from_slice(
                &u32::try_from(file_id.len())
                    .map_err(|_| directory_error("catalog file id is too long"))?
                    .to_be_bytes(),
            );
            encoded.extend_from_slice(file_id.as_bytes());
        }
        None => {
            encoded.push(0);
            encoded.extend_from_slice(&0u32.to_be_bytes());
        }
    }
    encoded.extend_from_slice(
        &u32::try_from(scope.schema_key.len())
            .map_err(|_| directory_error("catalog schema key is too long"))?
            .to_be_bytes(),
    );
    encoded.extend_from_slice(scope.schema_key.as_bytes());
    Ok(encoded)
}

fn catalog_node_digest(bytes: &[u8]) -> [u8; 32] {
    *blake3::Hasher::new_derive_key(CATALOG_HASH_CONTEXT)
        .update(bytes)
        .finalize()
        .as_bytes()
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct DirectoryChild {
    #[musli(bytes)]
    first_key: Vec<u8>,
    #[musli(bytes)]
    last_key: Vec<u8>,
    node_id: [u8; 32],
    row_count: u64,
    part_count: u32,
    level: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct DirectoryNode {
    kind: u8,
    level: u16,
    parts: Vec<CurrentStatePartDescriptor>,
    children: Vec<DirectoryChild>,
}

impl DirectoryNode {
    fn leaf(parts: Vec<CurrentStatePartDescriptor>) -> Self {
        Self {
            kind: 0,
            level: 0,
            parts,
            children: Vec::new(),
        }
    }

    fn internal(children: Vec<DirectoryChild>) -> Self {
        let level = children
            .first()
            .map(|child| child.level.saturating_add(1))
            .unwrap_or(1);
        Self {
            kind: 1,
            level,
            parts: Vec::new(),
            children,
        }
    }
}

#[derive(Debug, Clone)]
struct StagedNode {
    child: DirectoryChild,
}

fn balanced_directory_chunks<T>(values: &[T]) -> Vec<&[T]> {
    if values.is_empty() {
        return Vec::new();
    }
    let group_count = values.len().div_ceil(DIRECTORY_FANOUT);
    let base = values.len() / group_count;
    let remainder = values.len() % group_count;
    let mut chunks = Vec::with_capacity(group_count);
    let mut start = 0usize;
    for group_index in 0..group_count {
        let length = base + usize::from(group_index < remainder);
        chunks.push(&values[start..start + length]);
        start += length;
    }
    chunks
}

#[derive(Debug, Clone)]
pub(crate) struct CurrentStateDirectorySplicePlan {
    write_set_id: u64,
    root: CurrentStatePartDirectoryRoot,
    leaves: Vec<DirectorySpliceLeaf>,
    internal_by_depth: Vec<std::collections::BTreeMap<[u8; 32], DirectoryNode>>,
}

#[derive(Debug, Clone)]
struct DirectorySpliceLeaf {
    node_id: [u8; 32],
    parts: Vec<CurrentStatePartDescriptor>,
    key_indices: Vec<usize>,
}

impl CurrentStateDirectorySplicePlan {
    pub(crate) fn leaf_count(&self) -> usize {
        self.leaves.len()
    }

    pub(crate) fn leaf_parts(&self, index: usize) -> &[CurrentStatePartDescriptor] {
        &self.leaves[index].parts
    }

    pub(crate) fn leaf_key_indices(&self, index: usize) -> &[usize] {
        &self.leaves[index].key_indices
    }
}

/// Stages one complete ordered post-image part set as a bounded persistent
/// range directory. Equal nodes naturally share storage across generations.
pub(crate) fn stage_current_state_part_directory(
    writes: &mut StorageWriteSet,
    descriptors: &[CurrentStatePartDescriptor],
) -> Result<CurrentStatePartDirectoryRoot, LixError> {
    if descriptors.is_empty() {
        return Ok(CurrentStatePartDirectoryRoot {
            root_id: [0; 32],
            directory_digest: empty_current_state_directory_digest(),
            row_count: 0,
            part_count: 0,
            tree_height: 0,
        });
    }
    validate_descriptors(descriptors)?;
    let mut level = balanced_directory_chunks(descriptors)
        .into_iter()
        .map(|chunk| stage_node(writes, DirectoryNode::leaf(chunk.to_vec())))
        .collect::<Result<Vec<_>, _>>()?;
    let mut tree_height = 1u16;
    while level.len() > 1 {
        level = balanced_directory_chunks(&level)
            .into_iter()
            .map(|chunk| {
                stage_node(
                    writes,
                    DirectoryNode::internal(chunk.iter().map(|node| node.child.clone()).collect()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        tree_height = tree_height
            .checked_add(1)
            .ok_or_else(|| directory_error("tree height overflows"))?;
    }
    let root = level
        .into_iter()
        .next()
        .ok_or_else(|| directory_error("cannot stage an empty part set"))?;
    Ok(CurrentStatePartDirectoryRoot {
        root_id: root.child.node_id,
        directory_digest: current_state_directory_digest(
            root.child.node_id,
            root.child.row_count,
            root.child.part_count,
            tree_height,
        ),
        row_count: root.child.row_count,
        part_count: root.child.part_count,
        tree_height,
    })
}

/// Loads only the immutable search paths and boundary leaves needed to apply
/// an ordered key batch. Keys in gaps are assigned to the preceding leaf (or
/// the first leaf before the directory's minimum), so inserts and exact
/// misses never require flattening the complete descriptor set.
pub(crate) async fn plan_current_state_part_directory_splice(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    root: &CurrentStatePartDirectoryRoot,
    encoded_keys: &[Bytes],
) -> Result<CurrentStateDirectorySplicePlan, LixError> {
    if root.part_count == 0 {
        validate_empty_root(root)?;
        return Ok(CurrentStateDirectorySplicePlan {
            write_set_id: writes.identity(),
            root: root.clone(),
            leaves: Vec::new(),
            internal_by_depth: Vec::new(),
        });
    }
    if encoded_keys.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(directory_error(
            "splice keys are empty, duplicate, or unordered",
        ));
    }
    if encoded_keys.is_empty() {
        return Err(directory_error("splice key batch is empty"));
    }
    let mut cache = std::collections::BTreeMap::<[u8; 32], DirectoryNode>::new();
    let mut internal_by_depth = (0..root.tree_height.saturating_sub(1))
        .map(|_| std::collections::BTreeMap::new())
        .collect::<Vec<_>>();
    let mut leaves = std::collections::BTreeMap::<[u8; 32], DirectorySpliceLeaf>::new();
    for (key_index, encoded_key) in encoded_keys.iter().enumerate() {
        let mut node_id = root.root_id;
        let mut expected_child = None;
        let mut depth = 0usize;
        loop {
            let node = if let Some(node) = cache.get(&node_id) {
                node.clone()
            } else {
                let node = load_node_for_write(store, writes, node_id).await?;
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_crud_current_state_directory_node_loaded();
                cache.insert(node_id, node.clone());
                node
            };
            if depth == 0 {
                validate_root_summary(root, &node)?;
            }
            if let Some(expected) = expected_child.as_ref() {
                validate_child_summary(expected, &node)?;
            }
            match node.kind {
                0 => {
                    #[cfg(feature = "storage-benches")]
                    if !leaves.contains_key(&node_id) {
                        crate::storage_bench::record_crud_current_state_directory_descriptors_visited(
                            node.parts.len(),
                        );
                    }
                    leaves
                        .entry(node_id)
                        .or_insert_with(|| DirectorySpliceLeaf {
                            node_id,
                            parts: node.parts,
                            key_indices: Vec::new(),
                        })
                        .key_indices
                        .push(key_index);
                    break;
                }
                1 => {
                    let upper = node.children.partition_point(|child| {
                        child.first_key.as_slice() <= encoded_key.as_ref()
                    });
                    let child_index = upper.saturating_sub(1);
                    let child = node.children.get(child_index).cloned().ok_or_else(|| {
                        directory_error("splice path reached an empty internal node")
                    })?;
                    internal_by_depth
                        .get_mut(depth)
                        .ok_or_else(|| directory_error("splice path exceeds root height"))?
                        .insert(node_id, node);
                    node_id = child.node_id;
                    expected_child = Some(child);
                    depth += 1;
                }
                _ => unreachable!("validated directory node kind"),
            }
        }
    }
    let leaves = leaves.into_values().collect::<Vec<_>>();
    Ok(CurrentStateDirectorySplicePlan {
        write_set_id: writes.identity(),
        root: root.clone(),
        leaves,
        internal_by_depth,
    })
}

/// Applies leaf replacements to a previously planned immutable search-path
/// frontier. Only changed leaves and their ancestor closure are staged.
pub(crate) async fn stage_current_state_part_directory_splice(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    plan: CurrentStateDirectorySplicePlan,
    replacements: Vec<Vec<CurrentStatePartDescriptor>>,
) -> Result<CurrentStatePartDirectoryRoot, LixError> {
    if plan.write_set_id != writes.identity() {
        return Err(directory_error(
            "splice plan belongs to a different storage write set",
        ));
    }
    if replacements.len() != plan.leaves.len() {
        return Err(directory_error("splice replacement cardinality mismatch"));
    }
    if plan.root.part_count == 0 {
        if replacements.is_empty() {
            return Ok(plan.root);
        }
        return Err(directory_error("empty directory splice cannot name leaves"));
    }
    let mut changes = std::collections::BTreeMap::<[u8; 32], Vec<StagedNode>>::new();
    for (leaf, parts) in plan.leaves.into_iter().zip(replacements) {
        if !parts.is_empty() {
            validate_descriptor_slice(&parts)?;
        }
        let chunks = balanced_directory_chunks(&parts);
        let reuse = (chunks.len() == 1).then_some(leaf.node_id);
        let staged = chunks
            .into_iter()
            .map(|chunk| stage_node_reusing(writes, DirectoryNode::leaf(chunk.to_vec()), reuse))
            .collect::<Result<Vec<_>, _>>()?;
        changes.insert(leaf.node_id, staged);
    }
    for level in plan.internal_by_depth.into_iter().rev() {
        for (node_id, node) in level {
            let mut children = Vec::with_capacity(node.children.len() + DIRECTORY_FANOUT);
            for child in node.children {
                if let Some(replacement) = changes.remove(&child.node_id) {
                    children.extend(replacement.into_iter().map(|node| node.child));
                } else {
                    children.push(child);
                }
            }
            if node_id == plan.root.root_id && children.len() == 1 {
                changes.insert(
                    node_id,
                    vec![StagedNode {
                        child: children.pop().expect("one-child root has one child"),
                    }],
                );
                continue;
            }
            let chunks = balanced_directory_chunks(&children);
            let reuse = (chunks.len() == 1).then_some(node_id);
            let staged = chunks
                .into_iter()
                .map(|chunk| {
                    stage_node_reusing(writes, DirectoryNode::internal(chunk.to_vec()), reuse)
                })
                .collect::<Result<Vec<_>, _>>()?;
            changes.insert(node_id, staged);
        }
    }
    let mut roots = changes.remove(&plan.root.root_id).ok_or_else(|| {
        directory_error("splice plan did not rewrite the root search-path closure")
    })?;
    if !changes.is_empty() {
        return Err(directory_error(
            "splice plan retained disconnected node changes",
        ));
    }
    if roots.is_empty() {
        return Ok(CurrentStatePartDirectoryRoot {
            root_id: [0; 32],
            directory_digest: empty_current_state_directory_digest(),
            row_count: 0,
            part_count: 0,
            tree_height: 0,
        });
    }
    while roots.len() > 1 {
        roots = balanced_directory_chunks(&roots)
            .into_iter()
            .map(|chunk| {
                stage_node(
                    writes,
                    DirectoryNode::internal(chunk.iter().map(|node| node.child.clone()).collect()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
    }
    let mut root = roots.pop().expect("non-empty roots have one final node");
    while root.child.level > 0 {
        let node = load_node_for_write(store, writes, root.child.node_id).await?;
        validate_child_summary(&root.child, &node)?;
        if node.children.len() != 1 {
            break;
        }
        root.child = node
            .children
            .into_iter()
            .next()
            .expect("validated one-child internal node has one child");
    }
    let tree_height = root
        .child
        .level
        .checked_add(1)
        .ok_or_else(|| directory_error("tree height overflows"))?;
    Ok(CurrentStatePartDirectoryRoot {
        root_id: root.child.node_id,
        directory_digest: current_state_directory_digest(
            root.child.node_id,
            root.child.row_count,
            root.child.part_count,
            tree_height,
        ),
        row_count: root.child.row_count,
        part_count: root.child.part_count,
        tree_height,
    })
}

/// Routes one encoded identity to its bounded immutable state part.
pub(crate) async fn route_current_state_part(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &CurrentStatePartDirectoryRoot,
    encoded_key: &[u8],
) -> Result<Option<CurrentStatePartDescriptor>, LixError> {
    if root.part_count == 0 {
        validate_empty_root(root)?;
        return Ok(None);
    }
    let mut node_id = root.root_id;
    let mut expected_child = None;
    loop {
        let node = load_node(store, node_id).await?;
        if node_id == root.root_id {
            validate_root_summary(root, &node)?;
        }
        if let Some(expected) = expected_child.as_ref() {
            validate_child_summary(expected, &node)?;
        }
        match node.kind {
            0 => {
                let parts = node.parts;
                let index = parts.partition_point(|part| part.first_key.as_slice() <= encoded_key);
                let Some(part) = index.checked_sub(1).and_then(|index| parts.get(index)) else {
                    return Ok(None);
                };
                return Ok((encoded_key <= part.last_key.as_slice()).then(|| part.clone()));
            }
            1 => {
                let children = node.children;
                let upper =
                    children.partition_point(|child| child.first_key.as_slice() <= encoded_key);
                let Some(child) = upper.checked_sub(1).and_then(|index| children.get(index)) else {
                    return Ok(None);
                };
                if encoded_key > child.last_key.as_slice() {
                    return Ok(None);
                }
                node_id = child.node_id;
                expected_child = Some(child.clone());
            }
            _ => unreachable!("validated directory node kind"),
        }
    }
}

/// Routes a caller-owned encoded-key batch while reading each shared
/// directory node at most once for that batch.
pub(crate) async fn route_current_state_parts(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &CurrentStatePartDirectoryRoot,
    encoded_keys: &[Bytes],
) -> Result<Vec<Option<CurrentStatePartDescriptor>>, LixError> {
    let mut routed = route_current_state_part_sets(store, &[(root, encoded_keys)]).await?;
    Ok(routed.pop().unwrap_or_default())
}

pub(crate) async fn route_current_state_part_sets(
    store: &(impl StorageAdapterRead + ?Sized),
    requests: &[(&CurrentStatePartDirectoryRoot, &[Bytes])],
) -> Result<Vec<Vec<Option<CurrentStatePartDescriptor>>>, LixError> {
    type PendingDirectoryRoute = (usize, Option<DirectoryChild>, Vec<usize>);
    let mut routes = requests
        .iter()
        .map(|(_, keys)| vec![None; keys.len()])
        .collect::<Vec<_>>();
    let mut frontier = std::collections::BTreeMap::<[u8; 32], Vec<PendingDirectoryRoute>>::new();
    for (request_index, (root, keys)) in requests.iter().enumerate() {
        if root.part_count == 0 {
            validate_empty_root(root)?;
            continue;
        }
        frontier.entry(root.root_id).or_default().push((
            request_index,
            None,
            (0..keys.len()).collect::<Vec<_>>(),
        ));
    }
    let mut cache = std::collections::BTreeMap::<[u8; 32], DirectoryNode>::new();
    let mut visited = std::collections::BTreeSet::<(usize, [u8; 32])>::new();
    while !frontier.is_empty() {
        let unseen = frontier
            .keys()
            .filter(|node_id| !cache.contains_key(*node_id))
            .copied()
            .collect::<Vec<_>>();
        let loaded = load_directory_nodes(store, &unseen).await?;
        cache.extend(unseen.into_iter().zip(loaded));
        let mut next = std::collections::BTreeMap::<[u8; 32], Vec<PendingDirectoryRoute>>::new();
        for (node_id, pending_routes) in frontier {
            let node = cache
                .get(&node_id)
                .ok_or_else(|| directory_error("directory frontier omitted a node"))?;
            for (request_index, expected_child, key_indices) in pending_routes {
                if !visited.insert((request_index, node_id)) {
                    return Err(directory_error(
                        "part directory contains a cycle or duplicate child",
                    ));
                }
                let (root, encoded_keys) = requests[request_index];
                if expected_child.is_none() {
                    validate_root_summary(root, node)?;
                }
                if let Some(expected) = expected_child.as_ref() {
                    validate_child_summary(expected, node)?;
                }
                match node.kind {
                    0 => {
                        for key_index in key_indices {
                            let key = &encoded_keys[key_index];
                            let index = node
                                .parts
                                .partition_point(|part| part.first_key.as_slice() <= key.as_ref());
                            let Some(part) =
                                index.checked_sub(1).and_then(|index| node.parts.get(index))
                            else {
                                continue;
                            };
                            if key.as_ref() <= part.last_key.as_slice() {
                                routes[request_index][key_index] = Some(part.clone());
                            }
                        }
                    }
                    1 => {
                        let mut child_keys = std::collections::BTreeMap::<usize, Vec<usize>>::new();
                        for key_index in key_indices {
                            let key = &encoded_keys[key_index];
                            let upper = node.children.partition_point(|child| {
                                child.first_key.as_slice() <= key.as_ref()
                            });
                            if let Some(child_index) = upper.checked_sub(1)
                                && key.as_ref() <= node.children[child_index].last_key.as_slice()
                            {
                                child_keys.entry(child_index).or_default().push(key_index);
                            }
                        }
                        for (child_index, keys) in child_keys {
                            let child = node.children[child_index].clone();
                            next.entry(child.node_id).or_default().push((
                                request_index,
                                Some(child),
                                keys,
                            ));
                        }
                    }
                    _ => unreachable!("validated directory node kind"),
                }
            }
        }
        frontier = next;
    }
    Ok(routes)
}

/// Loads the complete ordered descriptor set for scans, rebuild, and audit.
pub(crate) async fn load_current_state_part_descriptors(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &CurrentStatePartDirectoryRoot,
) -> Result<Vec<CurrentStatePartDescriptor>, LixError> {
    if root.part_count == 0 {
        validate_empty_root(root)?;
        return Ok(Vec::new());
    }
    let mut pending = vec![(root.root_id, None)];
    let mut descriptors = Vec::with_capacity(root.part_count as usize);
    while let Some((node_id, expected_child)) = pending.pop() {
        let mut node = load_node(store, node_id).await?;
        if node_id == root.root_id {
            validate_root_summary(root, &node)?;
        }
        if let Some(expected) = expected_child.as_ref() {
            validate_child_summary(expected, &node)?;
        }
        match node.kind {
            0 => descriptors.append(&mut node.parts),
            1 => {
                pending.extend(
                    node.children
                        .into_iter()
                        .rev()
                        .map(|child| (child.node_id, Some(child))),
                );
            }
            _ => unreachable!("validated directory node kind"),
        }
    }
    validate_descriptors(&descriptors)?;
    if descriptors.len() != root.part_count as usize
        || descriptors
            .iter()
            .map(|part| u64::from(part.row_count))
            .sum::<u64>()
            != root.row_count
        || !directory_digest_matches(root)
    {
        return Err(directory_error(
            "root summary does not match its descriptor set",
        ));
    }
    Ok(descriptors)
}

/// One ordered key-range whose immutable current-state descriptors differ.
///
/// This is intentionally a physical, source-agnostic result. Consumers decide
/// whether and how to decode the referenced parts; the directory layer only
/// proves which descriptor ranges cannot be skipped by Merkle identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentStatePartDescriptorDiffWindow {
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
    pub(crate) left: Vec<CurrentStatePartDescriptor>,
    pub(crate) right: Vec<CurrentStatePartDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectoryNodeRef {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    node_id: [u8; 32],
    row_count: u64,
    part_count: u32,
    level: u16,
}

impl From<DirectoryChild> for DirectoryNodeRef {
    fn from(child: DirectoryChild) -> Self {
        Self {
            first_key: child.first_key,
            last_key: child.last_key,
            node_id: child.node_id,
            row_count: child.row_count,
            part_count: child.part_count,
            level: child.level,
        }
    }
}

enum DirectoryDiffWork {
    Compare(DirectoryNodeRef, DirectoryNodeRef),
    CollectLeft(DirectoryNodeRef),
    CollectRight(DirectoryNodeRef),
}

/// Compares two persistent part directories without interpreting their parts.
///
/// Equal node IDs prune complete subtrees. Unequal shapes are aligned by their
/// certified key ranges, so leaf splits, contractions, and height changes do
/// not turn into positional comparisons. The returned windows contain only
/// descriptors that are not byte-identical on both sides.
pub(crate) async fn diff_current_state_part_descriptors(
    store: &(impl StorageAdapterRead + ?Sized),
    left_root: &CurrentStatePartDirectoryRoot,
    right_root: &CurrentStatePartDirectoryRoot,
) -> Result<Vec<CurrentStatePartDescriptorDiffWindow>, LixError> {
    let left_empty = left_root.part_count == 0;
    let right_empty = right_root.part_count == 0;
    if left_empty {
        validate_empty_root(left_root)?;
    }
    if right_empty {
        validate_empty_root(right_root)?;
    }
    if left_empty && right_empty {
        return Ok(Vec::new());
    }

    let mut cache = std::collections::BTreeMap::<[u8; 32], DirectoryNode>::new();
    let left = if left_empty {
        None
    } else {
        Some(load_root_ref(store, left_root, &mut cache).await?)
    };
    let right = if right_empty {
        None
    } else {
        Some(load_root_ref(store, right_root, &mut cache).await?)
    };
    let mut work = match (left, right) {
        (Some(left), Some(right)) => vec![DirectoryDiffWork::Compare(left, right)],
        (Some(left), None) => vec![DirectoryDiffWork::CollectLeft(left)],
        (None, Some(right)) => vec![DirectoryDiffWork::CollectRight(right)],
        (None, None) => unreachable!("both empty roots returned above"),
    };
    let mut left_candidates =
        std::collections::BTreeMap::<Vec<u8>, CurrentStatePartDescriptor>::new();
    let mut right_candidates =
        std::collections::BTreeMap::<Vec<u8>, CurrentStatePartDescriptor>::new();

    while let Some(next) = work.pop() {
        match next {
            DirectoryDiffWork::Compare(left, right) => {
                if left.node_id == right.node_id {
                    if left != right {
                        return Err(directory_error(
                            "equal node identity has conflicting range summaries",
                        ));
                    }
                    continue;
                }
                if left.last_key < right.first_key || right.last_key < left.first_key {
                    work.push(DirectoryDiffWork::CollectLeft(left));
                    work.push(DirectoryDiffWork::CollectRight(right));
                    continue;
                }
                let left_node = load_node_ref(store, &left, &mut cache).await?;
                let right_node = load_node_ref(store, &right, &mut cache).await?;
                match (left_node.kind, right_node.kind) {
                    (0, 0) => {
                        insert_descriptor_candidates(&mut left_candidates, left_node.parts)?;
                        insert_descriptor_candidates(&mut right_candidates, right_node.parts)?;
                    }
                    (1, 1) => enqueue_aligned_directory_refs(
                        left_node.children.into_iter().map(Into::into).collect(),
                        right_node.children.into_iter().map(Into::into).collect(),
                        &mut work,
                    ),
                    (1, 0) => enqueue_aligned_directory_refs(
                        left_node.children.into_iter().map(Into::into).collect(),
                        vec![right],
                        &mut work,
                    ),
                    (0, 1) => enqueue_aligned_directory_refs(
                        vec![left],
                        right_node.children.into_iter().map(Into::into).collect(),
                        &mut work,
                    ),
                    _ => unreachable!("validated directory node kind"),
                }
            }
            DirectoryDiffWork::CollectLeft(reference) => {
                collect_descriptor_candidates(
                    store,
                    reference,
                    &mut cache,
                    &mut left_candidates,
                    true,
                    &mut work,
                )
                .await?;
            }
            DirectoryDiffWork::CollectRight(reference) => {
                collect_descriptor_candidates(
                    store,
                    reference,
                    &mut cache,
                    &mut right_candidates,
                    false,
                    &mut work,
                )
                .await?;
            }
        }
    }

    remove_shared_descriptors(&mut left_candidates, &mut right_candidates);
    Ok(descriptor_diff_windows(left_candidates, right_candidates))
}

/// Pure flatten-and-compare oracle for benchmarks and callers that already
/// own complete descriptor slices. Its result is identical to the Merkle
/// walker's result; only descriptor discovery differs.
pub(crate) fn diff_current_state_part_descriptor_slices(
    left: &[CurrentStatePartDescriptor],
    right: &[CurrentStatePartDescriptor],
) -> Result<Vec<CurrentStatePartDescriptorDiffWindow>, LixError> {
    if !left.is_empty() {
        validate_descriptor_slice(left)?;
    }
    if !right.is_empty() {
        validate_descriptor_slice(right)?;
    }
    let mut left_candidates = std::collections::BTreeMap::new();
    let mut right_candidates = std::collections::BTreeMap::new();
    insert_descriptor_candidates(&mut left_candidates, left.to_vec())?;
    insert_descriptor_candidates(&mut right_candidates, right.to_vec())?;
    remove_shared_descriptors(&mut left_candidates, &mut right_candidates);
    Ok(descriptor_diff_windows(left_candidates, right_candidates))
}

async fn load_root_ref(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &CurrentStatePartDirectoryRoot,
    cache: &mut std::collections::BTreeMap<[u8; 32], DirectoryNode>,
) -> Result<DirectoryNodeRef, LixError> {
    let node = load_cached_directory_node(store, root.root_id, cache).await?;
    validate_root_summary(root, &node)?;
    let (first_key, last_key, row_count, part_count) = node_summary(&node)?;
    Ok(DirectoryNodeRef {
        first_key,
        last_key,
        node_id: root.root_id,
        row_count,
        part_count,
        level: node.level,
    })
}

async fn load_node_ref(
    store: &(impl StorageAdapterRead + ?Sized),
    reference: &DirectoryNodeRef,
    cache: &mut std::collections::BTreeMap<[u8; 32], DirectoryNode>,
) -> Result<DirectoryNode, LixError> {
    let node = load_cached_directory_node(store, reference.node_id, cache).await?;
    let (first_key, last_key, row_count, part_count) = node_summary(&node)?;
    if first_key != reference.first_key
        || last_key != reference.last_key
        || row_count != reference.row_count
        || part_count != reference.part_count
        || node.level != reference.level
    {
        return Err(directory_error("node disagrees with its range summary"));
    }
    Ok(node)
}

async fn load_cached_directory_node(
    store: &(impl StorageAdapterRead + ?Sized),
    node_id: [u8; 32],
    cache: &mut std::collections::BTreeMap<[u8; 32], DirectoryNode>,
) -> Result<DirectoryNode, LixError> {
    if let Some(node) = cache.get(&node_id) {
        return Ok(node.clone());
    }
    let node = load_node(store, node_id).await?;
    cache.insert(node_id, node.clone());
    Ok(node)
}

async fn collect_descriptor_candidates(
    store: &(impl StorageAdapterRead + ?Sized),
    reference: DirectoryNodeRef,
    cache: &mut std::collections::BTreeMap<[u8; 32], DirectoryNode>,
    candidates: &mut std::collections::BTreeMap<Vec<u8>, CurrentStatePartDescriptor>,
    left: bool,
    work: &mut Vec<DirectoryDiffWork>,
) -> Result<(), LixError> {
    let node = load_node_ref(store, &reference, cache).await?;
    match node.kind {
        0 => insert_descriptor_candidates(candidates, node.parts),
        1 => {
            for child in node.children.into_iter().rev().map(DirectoryNodeRef::from) {
                work.push(if left {
                    DirectoryDiffWork::CollectLeft(child)
                } else {
                    DirectoryDiffWork::CollectRight(child)
                });
            }
            Ok(())
        }
        _ => unreachable!("validated directory node kind"),
    }
}

fn insert_descriptor_candidates(
    candidates: &mut std::collections::BTreeMap<Vec<u8>, CurrentStatePartDescriptor>,
    descriptors: Vec<CurrentStatePartDescriptor>,
) -> Result<(), LixError> {
    for descriptor in descriptors {
        match candidates.entry(descriptor.first_key.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(descriptor);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &descriptor => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(directory_error(
                    "paired traversal found conflicting descriptors for one first key",
                ));
            }
        }
    }
    Ok(())
}

fn remove_shared_descriptors(
    left: &mut std::collections::BTreeMap<Vec<u8>, CurrentStatePartDescriptor>,
    right: &mut std::collections::BTreeMap<Vec<u8>, CurrentStatePartDescriptor>,
) {
    let shared = left
        .iter()
        .filter_map(|(first_key, left)| {
            right
                .get(first_key)
                .is_some_and(|right| right == left)
                .then(|| first_key.clone())
        })
        .collect::<Vec<_>>();
    for first_key in shared {
        left.remove(&first_key);
        right.remove(&first_key);
    }
}

fn enqueue_aligned_directory_refs(
    left: Vec<DirectoryNodeRef>,
    right: Vec<DirectoryNodeRef>,
    work: &mut Vec<DirectoryDiffWork>,
) {
    let mut pending = Vec::new();
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        let left_ref = &left[left_index];
        let right_ref = &right[right_index];
        if left_ref.last_key < right_ref.first_key {
            pending.push(DirectoryDiffWork::CollectLeft(left_ref.clone()));
            left_index += 1;
            continue;
        }
        if right_ref.last_key < left_ref.first_key {
            pending.push(DirectoryDiffWork::CollectRight(right_ref.clone()));
            right_index += 1;
            continue;
        }
        pending.push(DirectoryDiffWork::Compare(
            left_ref.clone(),
            right_ref.clone(),
        ));
        match left_ref.last_key.cmp(&right_ref.last_key) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Equal => {
                left_index += 1;
                right_index += 1;
            }
            std::cmp::Ordering::Greater => right_index += 1,
        }
    }
    pending.extend(
        left[left_index..]
            .iter()
            .cloned()
            .map(DirectoryDiffWork::CollectLeft),
    );
    pending.extend(
        right[right_index..]
            .iter()
            .cloned()
            .map(DirectoryDiffWork::CollectRight),
    );
    work.extend(pending.into_iter().rev());
}

fn descriptor_diff_windows(
    left: std::collections::BTreeMap<Vec<u8>, CurrentStatePartDescriptor>,
    right: std::collections::BTreeMap<Vec<u8>, CurrentStatePartDescriptor>,
) -> Vec<CurrentStatePartDescriptorDiffWindow> {
    let mut descriptors = left
        .into_values()
        .map(|descriptor| (false, descriptor))
        .chain(right.into_values().map(|descriptor| (true, descriptor)))
        .collect::<Vec<_>>();
    descriptors.sort_by(|(left_side, left), (right_side, right)| {
        left.first_key
            .cmp(&right.first_key)
            .then_with(|| left_side.cmp(right_side))
    });
    let mut windows = Vec::<CurrentStatePartDescriptorDiffWindow>::new();
    for (right_side, descriptor) in descriptors {
        let append = windows
            .last()
            .is_some_and(|window| descriptor.first_key <= window.last_key);
        if !append {
            windows.push(CurrentStatePartDescriptorDiffWindow {
                first_key: descriptor.first_key.clone(),
                last_key: descriptor.last_key.clone(),
                left: Vec::new(),
                right: Vec::new(),
            });
        }
        let window = windows.last_mut().expect("a descriptor created a window");
        if descriptor.last_key > window.last_key {
            window.last_key.clone_from(&descriptor.last_key);
        }
        if right_side {
            window.right.push(descriptor);
        } else {
            window.left.push(descriptor);
        }
    }
    windows
}

fn stage_node(writes: &mut StorageWriteSet, node: DirectoryNode) -> Result<StagedNode, LixError> {
    stage_node_reusing(writes, node, None)
}

fn stage_node_reusing(
    writes: &mut StorageWriteSet,
    node: DirectoryNode,
    reusable_node_id: Option<[u8; 32]>,
) -> Result<StagedNode, LixError> {
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_crud_current_state_directory_node_encoded();
    let (first_key, last_key, row_count, part_count) = node_summary(&node)?;
    let bytes = encode_node(&node)?;
    let node_id = node_digest(&bytes);
    if let Some(staged) = writes.staged_value(CURRENT_STATE_PART_DIRECTORY_SPACE, &node_id) {
        if staged != bytes {
            return Err(directory_error(
                "content-addressed node identity has conflicting staged bytes",
            ));
        }
    } else if reusable_node_id != Some(node_id) {
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_crud_current_state_directory_bytes(bytes.len());
        writes.put(
            CURRENT_STATE_PART_DIRECTORY_SPACE,
            StorageKey(Bytes::copy_from_slice(&node_id)),
            StorageValue {
                bytes: bytes.clone(),
            },
        );
    }
    Ok(StagedNode {
        child: DirectoryChild {
            first_key,
            last_key,
            node_id,
            row_count,
            part_count,
            level: node.level,
        },
    })
}

async fn load_node_for_write(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    node_id: [u8; 32],
) -> Result<DirectoryNode, LixError> {
    if let Some(bytes) = writes.staged_value(CURRENT_STATE_PART_DIRECTORY_SPACE, &node_id) {
        if node_digest(&bytes) != node_id {
            return Err(directory_error("node content digest mismatch"));
        }
        decode_node(&bytes)
    } else {
        load_node(store, node_id).await
    }
}

async fn load_node(
    store: &(impl StorageAdapterRead + ?Sized),
    node_id: [u8; 32],
) -> Result<DirectoryNode, LixError> {
    let key = StorageKey(Bytes::copy_from_slice(&node_id));
    let result = PointReadPlan::new(CURRENT_STATE_PART_DIRECTORY_SPACE, &[key])
        .materialize(store, StorageGetOptions::default())
        .await?;
    let value = result
        .value
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| directory_error("references a missing content-addressed node"))?;
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(directory_error("node read omitted its value"));
    };
    if node_digest(&bytes) != node_id {
        return Err(directory_error("node content digest mismatch"));
    }
    decode_node(&bytes)
}

async fn load_directory_nodes(
    store: &(impl StorageAdapterRead + ?Sized),
    node_ids: &[[u8; 32]],
) -> Result<Vec<DirectoryNode>, LixError> {
    let keys = node_ids
        .iter()
        .map(|node_id| StorageKey(Bytes::copy_from_slice(node_id)))
        .collect::<Vec<_>>();
    let result = PointReadPlan::new(CURRENT_STATE_PART_DIRECTORY_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    node_ids
        .iter()
        .zip(result.value)
        .map(|(node_id, value)| {
            let value = value
                .ok_or_else(|| directory_error("references a missing content-addressed node"))?;
            let StorageProjectedValue::FullValue(bytes) = value else {
                return Err(directory_error("node read omitted its value"));
            };
            if node_digest(&bytes) != *node_id {
                return Err(directory_error("node content digest mismatch"));
            }
            decode_node(&bytes)
        })
        .collect()
}

fn encode_node(node: &DirectoryNode) -> Result<Bytes, LixError> {
    let payload = storage_codec::encode("current-state part directory node", node)?;
    if payload.len() > DIRECTORY_NODE_MAX_DECODED_BYTES {
        return Err(directory_error("node exceeds its decoded size bound"));
    }
    let compressed = crate::compression::compress_zstd_level_1(&payload)
        .map_err(|error| directory_error(format!("compression failed: {error}")))?;
    let use_compressed = compressed.len().saturating_add(size_of::<u32>()) < payload.len();
    let body = if use_compressed {
        compressed.as_slice()
    } else {
        payload.as_slice()
    };
    let mut encoded = Vec::with_capacity(
        DIRECTORY_NODE_RAW_MAGIC.len()
            + if use_compressed { size_of::<u32>() } else { 0 }
            + body.len(),
    );
    if use_compressed {
        encoded.extend_from_slice(DIRECTORY_NODE_ZSTD_MAGIC);
        encoded.extend_from_slice(
            &u32::try_from(payload.len())
                .map_err(|_| directory_error("node exceeds the decoded size bound"))?
                .to_be_bytes(),
        );
    } else {
        encoded.extend_from_slice(DIRECTORY_NODE_RAW_MAGIC);
    }
    encoded.extend_from_slice(body);
    Ok(Bytes::from(encoded))
}

fn decode_node(bytes: &[u8]) -> Result<DirectoryNode, LixError> {
    let payload = if let Some(payload) = bytes.strip_prefix(DIRECTORY_NODE_RAW_MAGIC) {
        if payload.len() > DIRECTORY_NODE_MAX_DECODED_BYTES {
            return Err(directory_error("raw node exceeds its decode bound"));
        }
        payload.to_vec()
    } else if let Some(encoded) = bytes.strip_prefix(DIRECTORY_NODE_ZSTD_MAGIC) {
        let (length, compressed) = encoded
            .split_at_checked(size_of::<u32>())
            .ok_or_else(|| directory_error("compressed node omitted its decoded length"))?;
        let decoded_len = usize::try_from(u32::from_be_bytes(
            length
                .try_into()
                .expect("checked node length is four bytes"),
        ))
        .expect("u32 fits usize");
        if decoded_len > DIRECTORY_NODE_MAX_DECODED_BYTES {
            return Err(directory_error("compressed node exceeds its decode bound"));
        }
        crate::compression::decompress_zstd(compressed, decoded_len)
            .map_err(|error| directory_error(format!("decompression failed: {error}")))?
    } else {
        return Err(directory_error(
            "node has an unsupported format; recreate the serving directory",
        ));
    };
    let node = storage_codec::decode("current-state part directory node", &payload)?;
    validate_node(&node)?;
    Ok(node)
}

fn node_digest(bytes: &[u8]) -> [u8; 32] {
    *blake3::Hasher::new_derive_key(DIRECTORY_HASH_CONTEXT)
        .update(bytes)
        .finalize()
        .as_bytes()
}

fn replacement_directory_digest(
    descriptors: &[CurrentStatePartDescriptor],
) -> Result<[u8; 32], LixError> {
    let mut first_ordinal = 0u32;
    let entries = descriptors
        .iter()
        .map(|descriptor| {
            let entry = crate::tracked_state::replacement_part::ReplacementPartDirectoryEntry::new(
                descriptor.content_digest,
                &descriptor.first_key,
                &descriptor.last_key,
                first_ordinal,
                descriptor.row_count,
            );
            first_ordinal = first_ordinal
                .checked_add(u32::from(descriptor.row_count))
                .expect("validated replacement row count fits u32");
            entry
        })
        .collect();
    crate::tracked_state::replacement_part::ReplacementPartDirectory::try_new(
        entries,
        u32::try_from(
            descriptors
                .iter()
                .map(|descriptor| u64::from(descriptor.row_count))
                .sum::<u64>(),
        )
        .map_err(|_| directory_error("row count overflows u32"))?,
    )?
    .digest()
}

fn current_state_directory_digest(
    root_id: [u8; 32],
    row_count: u64,
    part_count: u32,
    tree_height: u16,
) -> [u8; 32] {
    *blake3::Hasher::new_derive_key("lix current-state merkle directory v2")
        .update(&root_id)
        .update(&row_count.to_be_bytes())
        .update(&part_count.to_be_bytes())
        .update(&tree_height.to_be_bytes())
        .finalize()
        .as_bytes()
}

fn empty_current_state_directory_digest() -> [u8; 32] {
    current_state_directory_digest([0; 32], 0, 0, 0)
}

fn directory_digest_matches(root: &CurrentStatePartDirectoryRoot) -> bool {
    root.directory_digest
        == current_state_directory_digest(
            root.root_id,
            root.row_count,
            root.part_count,
            root.tree_height,
        )
}

fn validate_root_summary(
    root: &CurrentStatePartDirectoryRoot,
    node: &DirectoryNode,
) -> Result<(), LixError> {
    let (_, _, row_count, part_count) = node_summary(node)?;
    if row_count != root.row_count
        || part_count != root.part_count
        || node.level.checked_add(1) != Some(root.tree_height)
        || root.directory_digest
            != current_state_directory_digest(
                root.root_id,
                root.row_count,
                root.part_count,
                root.tree_height,
            )
    {
        return Err(directory_error("root summary disagrees with its node"));
    }
    Ok(())
}

fn validate_empty_root(root: &CurrentStatePartDirectoryRoot) -> Result<(), LixError> {
    if root.root_id != [0; 32]
        || root.directory_digest != empty_current_state_directory_digest()
        || root.row_count != 0
        || root.part_count != 0
        || root.tree_height != 0
    {
        return Err(directory_error("empty root summary is invalid"));
    }
    Ok(())
}

fn validate_child_summary(expected: &DirectoryChild, node: &DirectoryNode) -> Result<(), LixError> {
    let (first_key, last_key, row_count, part_count) = node_summary(node)?;
    if first_key != expected.first_key
        || last_key != expected.last_key
        || row_count != expected.row_count
        || part_count != expected.part_count
        || node.level != expected.level
    {
        return Err(directory_error("child summary disagrees with its node"));
    }
    Ok(())
}

fn node_summary(node: &DirectoryNode) -> Result<(Vec<u8>, Vec<u8>, u64, u32), LixError> {
    validate_node(node)?;
    match node.kind {
        0 => Ok((
            node.parts[0].first_key.clone(),
            node.parts
                .last()
                .expect("validated leaf is non-empty")
                .last_key
                .clone(),
            node.parts
                .iter()
                .map(|part| u64::from(part.row_count))
                .sum(),
            u32::try_from(node.parts.len()).map_err(|_| directory_error("part count overflows"))?,
        )),
        1 => {
            let row_count = node.children.iter().try_fold(0u64, |sum, child| {
                sum.checked_add(child.row_count)
                    .ok_or_else(|| directory_error("directory row count overflows"))
            })?;
            let part_count = node.children.iter().try_fold(0u32, |sum, child| {
                sum.checked_add(child.part_count)
                    .ok_or_else(|| directory_error("directory part count overflows"))
            })?;
            Ok((
                node.children[0].first_key.clone(),
                node.children
                    .last()
                    .expect("validated internal node is non-empty")
                    .last_key
                    .clone(),
                row_count,
                part_count,
            ))
        }
        _ => unreachable!("validated directory node kind"),
    }
}

fn validate_node(node: &DirectoryNode) -> Result<(), LixError> {
    match node.kind {
        0 => {
            if node.level != 0 || !node.children.is_empty() || node.parts.len() > DIRECTORY_FANOUT {
                return Err(directory_error("leaf exceeds bounded fanout"));
            }
            validate_descriptor_slice(&node.parts)
        }
        1 => {
            if !node.parts.is_empty()
                || node.children.is_empty()
                || node.children.len() > DIRECTORY_FANOUT
                || node.children.iter().any(|child| {
                    child.first_key.is_empty()
                        || child.last_key.is_empty()
                        || child.first_key > child.last_key
                        || child.row_count == 0
                        || child.part_count == 0
                        || child.level.checked_add(1) != Some(node.level)
                })
                || node
                    .children
                    .windows(2)
                    .any(|pair| pair[0].last_key >= pair[1].first_key)
            {
                return Err(directory_error(
                    "internal node has invalid or overlapping ranges",
                ));
            }
            Ok(())
        }
        _ => Err(directory_error("node has an unknown kind")),
    }
}

fn validate_descriptors(parts: &[CurrentStatePartDescriptor]) -> Result<(), LixError> {
    validate_descriptor_slice(parts)
}

fn validate_descriptor_slice(parts: &[CurrentStatePartDescriptor]) -> Result<(), LixError> {
    let first = parts.first();
    if first.is_none()
        || parts.iter().any(|part| {
            part.first_key.is_empty()
            || part.last_key.is_empty()
            || part.first_key > part.last_key
            || part.row_count == 0
            || u32::from(part.source_row_offset) + u32::from(part.row_count)
                > crate::tracked_state::current_state_data_part::CURRENT_STATE_DATA_PART_MAX_ROWS
                    as u32
            || part.content_digest == [0; 32]
            || part.source_kind > 1
            || (part.source_kind == 0
                && (part.owner_commit_id == [0; 16] || part.payload_refs_digest != [0; 32]))
            || (part.source_kind == 1
                && (part.owner_commit_id != [0; 16]
                    || part.part_index != 0
                    || part.payload_refs_digest == [0; 32]
                    || part.uniform_created_at.packed() != 0
                    || part.uniform_updated_at.packed() != 0))
        })
        || parts
            .windows(2)
            .any(|pair| pair[0].last_key >= pair[1].first_key)
    {
        return Err(directory_error(
            "descriptor set is empty, unordered, or overlapping",
        ));
    }
    Ok(())
}

fn directory_error(message: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state current-state part directory {message}"),
    )
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::{Arc, Mutex};

    use crate::common::LixTimestamp;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    use super::*;

    struct CountingDirectoryRead<R> {
        inner: R,
        directory_batch_sizes: Arc<Mutex<Vec<usize>>>,
        catalog_batch_sizes: Arc<Mutex<Vec<usize>>>,
    }

    impl<R> StorageAdapterRead for CountingDirectoryRead<R>
    where
        R: StorageAdapterRead,
    {
        fn snapshot_cache_key(&self) -> Option<u128> {
            self.inner.snapshot_cache_key()
        }

        fn get_many(
            &self,
            requests: &[crate::storage::GetManyRequest<'_>],
        ) -> impl Future<
            Output = Result<crate::storage::GetManyResult, crate::storage::StorageError>,
        > + Send {
            for request in requests {
                if request.space == CURRENT_STATE_PART_DIRECTORY_SPACE {
                    self.directory_batch_sizes
                        .lock()
                        .expect("directory batch counts lock")
                        .push(request.keys.len());
                }
                if request.space == CURRENT_STATE_CATALOG_SPACE {
                    self.catalog_batch_sizes
                        .lock()
                        .expect("catalog batch counts lock")
                        .push(request.keys.len());
                }
            }
            self.inner.get_many(requests)
        }

        fn scan(
            &self,
            space: StorageSpace,
            range: crate::storage::KeyRange,
            opts: crate::storage::ScanOptions,
        ) -> impl Future<Output = Result<crate::storage::ScanChunk, crate::storage::StorageError>> + Send
        {
            self.inner.scan(space, range, opts)
        }
    }

    fn descriptors(count: usize) -> Vec<CurrentStatePartDescriptor> {
        let timestamp = LixTimestamp::from_unix_millis_utc_lossy(7);
        (0..count)
            .map(|index| CurrentStatePartDescriptor {
                first_key: format!("key-{index:06}-a").into_bytes(),
                last_key: format!("key-{index:06}-z").into_bytes(),
                content_digest: *blake3::hash(&index.to_be_bytes()).as_bytes(),
                payload_refs_digest: [0; 32],
                source_kind: 0,
                owner_commit_id: [9; 16],
                part_index: u32::try_from(index).expect("fixture index fits u32"),
                source_row_offset: 0,
                row_count: 10,
                uniform_created_at: timestamp,
                uniform_updated_at: timestamp,
            })
            .collect()
    }

    fn catalog_entry(index: usize) -> CurrentStatePartSet {
        CurrentStatePartSet {
            scope: crate::tracked_state::types::CommitDeltaReplacementScope {
                schema_key: format!("schema-{index:05}"),
                file_id: Some(format!("file-{:03}", index % 100)),
            },
            generation_integrity_digest: [2; 32],
            state_lineage_digest: [3; 32],
            directory: CurrentStatePartDirectoryRoot {
                root_id: *blake3::hash(format!("root-{index}").as_bytes()).as_bytes(),
                directory_digest: *blake3::hash(format!("descriptors-{index}").as_bytes())
                    .as_bytes(),
                row_count: 1,
                part_count: 1,
                tree_height: 1,
            },
        }
    }

    #[tokio::test]
    async fn directory_merkle_diff_skips_an_identical_root_after_validation() {
        let adapter = StorageAdapter::new(Memory::new());
        let descriptors = descriptors(DIRECTORY_FANOUT * 2 + 7);
        let mut writes = adapter.new_write_set();
        let root = stage_current_state_part_directory(&mut writes, &descriptors)
            .expect("directory should stage");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("directory should commit");
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let read = CountingDirectoryRead {
            inner: adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("diff read should open"),
            directory_batch_sizes: Arc::clone(&batch_sizes),
            catalog_batch_sizes: Arc::new(Mutex::new(Vec::new())),
        };

        let windows = diff_current_state_part_descriptors(&read, &root, &root)
            .await
            .expect("equal directory should diff");

        assert!(windows.is_empty());
        assert_eq!(
            batch_sizes
                .lock()
                .expect("directory batch counts lock")
                .as_slice(),
            &[1],
            "the root is validated once and every descendant is pruned"
        );
    }

    #[tokio::test]
    async fn directory_merkle_diff_reads_only_the_changed_leaf_paths() {
        let adapter = StorageAdapter::new(Memory::new());
        let descriptors = descriptors(DIRECTORY_FANOUT * 2 + 7);
        let mut writes = adapter.new_write_set();
        let root = stage_current_state_part_directory(&mut writes, &descriptors)
            .expect("directory should stage");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("directory should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("splice read should open");
        let mut splice_writes = adapter.new_write_set();
        let key = Bytes::from_static(b"key-000001-m");
        let plan = plan_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            &root,
            std::slice::from_ref(&key),
        )
        .await
        .expect("one-key splice should plan");
        let mut replacement = plan.leaf_parts(0).to_vec();
        replacement[1].content_digest = [7; 32];
        let rewritten = stage_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            plan,
            vec![replacement],
        )
        .await
        .expect("one-leaf splice should stage");
        drop(read);
        adapter
            .commit_write_set(splice_writes, StorageWriteOptions::default())
            .await
            .expect("splice should commit");

        let mut expected_descriptors = descriptors.clone();
        expected_descriptors[1].content_digest = [7; 32];
        let expected =
            diff_current_state_part_descriptor_slices(&descriptors, &expected_descriptors)
                .expect("flat oracle should compare");
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let read = CountingDirectoryRead {
            inner: adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("diff read should open"),
            directory_batch_sizes: Arc::clone(&batch_sizes),
            catalog_batch_sizes: Arc::new(Mutex::new(Vec::new())),
        };
        let actual = diff_current_state_part_descriptors(&read, &root, &rewritten)
            .await
            .expect("rewritten directory should diff");

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].left, vec![descriptors[1].clone()]);
        assert_eq!(actual[0].right, vec![expected_descriptors[1].clone()]);
        assert_eq!(
            batch_sizes
                .lock()
                .expect("directory batch counts lock")
                .as_slice(),
            &[1, 1, 1, 1],
            "only two roots and the unequal leaf pair are read"
        );
    }

    #[tokio::test]
    async fn directory_merkle_diff_aligns_a_leaf_split_without_positional_pairing() {
        let adapter = StorageAdapter::new(Memory::new());
        let descriptors = descriptors(DIRECTORY_FANOUT * 2);
        let mut writes = adapter.new_write_set();
        let root = stage_current_state_part_directory(&mut writes, &descriptors)
            .expect("directory should stage");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("directory should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("splice read should open");
        let mut splice_writes = adapter.new_write_set();
        let key = Bytes::from_static(b"key-000000-0");
        let plan = plan_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            &root,
            std::slice::from_ref(&key),
        )
        .await
        .expect("boundary insertion should plan");
        let mut inserted = descriptors[0].clone();
        inserted.first_key = b"key-000000-0a".to_vec();
        inserted.last_key = b"key-000000-0z".to_vec();
        inserted.content_digest = [8; 32];
        let mut replacement = Vec::with_capacity(DIRECTORY_FANOUT + 1);
        replacement.push(inserted.clone());
        replacement.extend_from_slice(plan.leaf_parts(0));
        let rewritten = stage_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            plan,
            vec![replacement],
        )
        .await
        .expect("overflowing leaf should split");
        drop(read);
        adapter
            .commit_write_set(splice_writes, StorageWriteOptions::default())
            .await
            .expect("boundary insertion should commit");

        let mut expected_descriptors = Vec::with_capacity(descriptors.len() + 1);
        expected_descriptors.push(inserted.clone());
        expected_descriptors.extend(descriptors.clone());
        let expected =
            diff_current_state_part_descriptor_slices(&descriptors, &expected_descriptors)
                .expect("flat oracle should compare");
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let read = CountingDirectoryRead {
            inner: adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("diff read should open"),
            directory_batch_sizes: Arc::clone(&batch_sizes),
            catalog_batch_sizes: Arc::new(Mutex::new(Vec::new())),
        };
        let actual = diff_current_state_part_descriptors(&read, &root, &rewritten)
            .await
            .expect("split directory should diff");

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 1);
        assert!(actual[0].left.is_empty());
        assert_eq!(actual[0].right, vec![inserted]);
        assert_eq!(
            batch_sizes
                .lock()
                .expect("directory batch counts lock")
                .as_slice(),
            &[1, 1, 1, 1, 1],
            "the unchanged sibling leaf is pruned across unequal shapes"
        );
    }

    #[tokio::test]
    async fn directory_merkle_diff_aligns_a_contracted_root_with_unequal_height() {
        let adapter = StorageAdapter::new(Memory::new());
        let descriptors = descriptors(DIRECTORY_FANOUT * 2);
        let mut writes = adapter.new_write_set();
        let root = stage_current_state_part_directory(&mut writes, &descriptors)
            .expect("directory should stage");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("directory should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("splice read should open");
        let original_root = load_node(&read, root.root_id)
            .await
            .expect("root should load");
        let removed_count = usize::try_from(original_root.children[0].part_count)
            .expect("fixture part count fits usize");
        let mut splice_writes = adapter.new_write_set();
        let key = Bytes::from_static(b"key-000001-m");
        let plan = plan_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            &root,
            std::slice::from_ref(&key),
        )
        .await
        .expect("leaf removal should plan");
        let rewritten = stage_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            plan,
            vec![Vec::new()],
        )
        .await
        .expect("root should contract to its surviving leaf");
        assert_eq!((root.tree_height, rewritten.tree_height), (2, 1));
        drop(read);
        adapter
            .commit_write_set(splice_writes, StorageWriteOptions::default())
            .await
            .expect("contraction should commit");

        let surviving = &descriptors[removed_count..];
        let expected = diff_current_state_part_descriptor_slices(&descriptors, surviving)
            .expect("flat oracle should compare");
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let read = CountingDirectoryRead {
            inner: adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("diff read should open"),
            directory_batch_sizes: Arc::clone(&batch_sizes),
            catalog_batch_sizes: Arc::new(Mutex::new(Vec::new())),
        };
        let actual = diff_current_state_part_descriptors(&read, &root, &rewritten)
            .await
            .expect("unequal-height directories should diff");

        assert_eq!(actual, expected);
        assert_eq!(
            actual.iter().map(|window| window.left.len()).sum::<usize>(),
            removed_count
        );
        assert!(actual.iter().all(|window| window.right.is_empty()));
        assert_eq!(
            batch_sizes
                .lock()
                .expect("directory batch counts lock")
                .as_slice(),
            &[1, 1, 1],
            "the surviving contracted leaf is identified by node ID and not reread"
        );
    }

    #[tokio::test]
    async fn directory_merkle_diff_rejects_a_cross_wired_root_summary() {
        let adapter = StorageAdapter::new(Memory::new());
        let descriptors = descriptors(3);
        let mut writes = adapter.new_write_set();
        let root = stage_current_state_part_directory(&mut writes, &descriptors)
            .expect("directory should stage");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("directory should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("diff read should open");
        let forged = CurrentStatePartDirectoryRoot {
            row_count: root.row_count + 1,
            ..root.clone()
        };

        assert!(
            diff_current_state_part_descriptors(&read, &root, &forged)
                .await
                .is_err(),
            "both endpoint root summaries must be validated before pruning"
        );
    }

    #[tokio::test]
    async fn persistent_catalog_bounds_lookup_and_path_copies_one_leaf() {
        let adapter = StorageAdapter::new(Memory::new());
        let entries = (0..10_000).map(catalog_entry).collect::<Vec<_>>();
        let mut writes = adapter.new_write_set();
        let root =
            stage_catalog_from_entries(&mut writes, entries.clone()).expect("catalog should stage");
        assert_eq!(root.entry_count, 10_000);
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("catalog should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("catalog read should open");
        for index in [0, 31, 32, 4_999, 9_999] {
            assert_eq!(
                load_current_state_catalog_entry(&read, &root, &entries[index].scope)
                    .await
                    .expect("catalog lookup should succeed"),
                Some(entries[index].clone())
            );
        }
        let missing = crate::tracked_state::types::CommitDeltaReplacementScope {
            schema_key: "missing".to_string(),
            file_id: Some("file-001".to_string()),
        };
        assert!(
            load_current_state_catalog_entry(&read, &root, &missing)
                .await
                .expect("catalog miss should succeed")
                .is_none()
        );
        let requested = vec![
            entries[9_999].scope.clone(),
            missing.clone(),
            entries[0].scope.clone(),
            entries[9_999].scope.clone(),
        ];
        assert_eq!(
            load_current_state_catalog_entries_for_scopes(&read, &root, &requested)
                .await
                .expect("batched catalog lookup should succeed"),
            vec![
                Some(entries[9_999].clone()),
                None,
                Some(entries[0].clone()),
                Some(entries[9_999].clone()),
            ],
            "batched lookup must preserve caller order and duplicate scopes"
        );
        let (reachable, loaded) = load_current_state_catalog_reachability(&read, &root)
            .await
            .expect("catalog reachability should validate");
        assert!(
            reachable.len() < 1_000,
            "adaptive buckets must remain shallow; got {} nodes",
            reachable.len()
        );
        assert_eq!(loaded.len(), entries.len());

        let removed = entries[4_999].scope.clone();
        let mut rewrite = adapter.new_write_set();
        let mut staged = std::collections::BTreeMap::new();
        let rewritten = update_catalog_entry(
            &read,
            &mut rewrite,
            &mut staged,
            Some(&root),
            &removed,
            None,
        )
        .await
        .expect("catalog delete should path-copy")
        .expect("catalog should remain non-empty");
        assert_eq!(rewritten.entry_count, root.entry_count - 1);
        assert_ne!(rewritten.root_id, root.root_id);
        assert!(staged.len() < 16, "one update must rewrite a bounded path");
        flush_reachable_staged_catalog_nodes(&mut rewrite, &staged, rewritten.root_id)
            .expect("reachable path copy should flush");
        adapter
            .commit_write_set(rewrite, StorageWriteOptions::default())
            .await
            .expect("catalog rewrite should commit");
        let rewritten_read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("rewritten catalog read should open");
        assert!(
            load_current_state_catalog_entry(&rewritten_read, &rewritten, &removed)
                .await
                .expect("removed scope lookup should succeed")
                .is_none()
        );
        assert_eq!(
            load_current_state_catalog_entry(&rewritten_read, &rewritten, &entries[5_000].scope)
                .await
                .expect("untouched scope lookup should succeed"),
            Some(entries[5_000].clone())
        );
    }

    #[tokio::test]
    async fn catalog_scope_sets_read_shared_root_frontiers_once() {
        let adapter = StorageAdapter::new(Memory::new());
        let entries = (0..10_000).map(catalog_entry).collect::<Vec<_>>();
        let mut writes = adapter.new_write_set();
        let root =
            stage_catalog_from_entries(&mut writes, entries.clone()).expect("catalog should stage");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("catalog should commit");

        let catalog_batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let read = CountingDirectoryRead {
            inner: adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("catalog read should open"),
            directory_batch_sizes: Arc::new(Mutex::new(Vec::new())),
            catalog_batch_sizes: Arc::clone(&catalog_batch_sizes),
        };
        let first = vec![
            entries[9_999].scope.clone(),
            entries[0].scope.clone(),
            entries[9_999].scope.clone(),
        ];
        let second = vec![entries[5_000].scope.clone(), entries[31].scope.clone()];

        let single =
            load_current_state_catalog_entries_for_scope_sets(&read, &[(&root, first.as_slice())])
                .await
                .expect("single catalog request should route");
        let single_batches = std::mem::take(
            &mut *catalog_batch_sizes
                .lock()
                .expect("catalog batch counts lock"),
        );

        let shared = load_current_state_catalog_entries_for_scope_sets(
            &read,
            &[
                (&root, first.as_slice()),
                (&root, second.as_slice()),
                (&root, first.as_slice()),
            ],
        )
        .await
        .expect("shared-root catalog requests should route");
        assert_eq!(shared[0], single[0]);
        assert_eq!(shared[2], single[0]);
        assert_eq!(
            shared[1],
            vec![Some(entries[5_000].clone()), Some(entries[31].clone())]
        );

        let shared_batches = catalog_batch_sizes
            .lock()
            .expect("catalog batch counts lock")
            .clone();
        assert_eq!(shared_batches.len(), single_batches.len());
        assert_eq!(shared_batches[0], 1, "shared root must be read once");
        assert!(shared_batches.iter().any(|&size| size > 1));
        assert!(
            shared_batches.iter().sum::<usize>() < single_batches.iter().sum::<usize>() * 3,
            "three requests must share physical node reads"
        );
    }

    #[tokio::test]
    async fn compressed_catalog_root_handles_short_misses_and_prefix_divergence() {
        let adapter = StorageAdapter::new(Memory::new());
        let mut entries = (0..129).map(catalog_entry).collect::<Vec<_>>();
        for (index, entry) in entries.iter_mut().enumerate() {
            entry.scope.file_id = Some(format!("shared-prefix-{index:04}"));
            entry.scope.schema_key = "shared-schema".to_string();
        }
        let mut writes = adapter.new_write_set();
        let root = stage_catalog_from_entries(&mut writes, entries.clone())
            .expect("compressed catalog should stage");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("compressed catalog should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("compressed catalog read should open");
        let short_scope = crate::tracked_state::types::CommitDeltaReplacementScope {
            schema_key: "x".to_string(),
            file_id: None,
        };
        let mut no_op_writes = adapter.new_write_set();
        let mut no_op_staged = std::collections::BTreeMap::new();
        let unchanged = update_catalog_entry(
            &read,
            &mut no_op_writes,
            &mut no_op_staged,
            Some(&root),
            &short_scope,
            None,
        )
        .await
        .expect("short absent scope must be a no-op")
        .expect("catalog should remain present");
        assert_eq!(unchanged.root_id, root.root_id);
        assert!(no_op_staged.is_empty());

        let mut inserted = catalog_entry(10_000);
        inserted.scope = short_scope.clone();
        let mut insert_writes = adapter.new_write_set();
        let mut insert_staged = std::collections::BTreeMap::new();
        let rewritten = update_catalog_entry(
            &read,
            &mut insert_writes,
            &mut insert_staged,
            Some(&root),
            &short_scope,
            Some(inserted.clone()),
        )
        .await
        .expect("prefix-divergent insertion should stage")
        .expect("catalog should remain present");
        flush_reachable_staged_catalog_nodes(&mut insert_writes, &insert_staged, rewritten.root_id)
            .expect("rewritten catalog should flush");
        drop(read);
        adapter
            .commit_write_set(insert_writes, StorageWriteOptions::default())
            .await
            .expect("prefix-divergent insertion should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("rewritten catalog read should open");
        assert_eq!(
            load_current_state_catalog_entry(&read, &rewritten, &short_scope)
                .await
                .expect("inserted scope should route"),
            Some(inserted)
        );
        assert_eq!(
            load_current_state_catalog_entry(&read, &rewritten, &entries[64].scope)
                .await
                .expect("old compressed scope should still route"),
            Some(entries[64].clone())
        );
    }

    #[tokio::test]
    async fn catalog_delete_collapses_to_an_unstaged_shared_subtree() {
        let adapter = StorageAdapter::new(Memory::new());
        let mut entries = (0..130).map(catalog_entry).collect::<Vec<_>>();
        for (index, entry) in entries.iter_mut().take(129).enumerate() {
            entry.scope.file_id = Some(format!("shared-A-{index:04}"));
            entry.scope.schema_key = "schema".to_string();
        }
        entries[129].scope.file_id = Some("shared-B".to_string());
        entries[129].scope.schema_key = "schema".to_string();
        let removed = entries[129].scope.clone();
        let mut writes = adapter.new_write_set();
        let root = stage_catalog_from_entries(&mut writes, entries.clone())
            .expect("branching catalog should stage");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("branching catalog should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("branching catalog read should open");
        let mut rewrite = adapter.new_write_set();
        let mut staged = std::collections::BTreeMap::new();
        let rewritten = update_catalog_entry(
            &read,
            &mut rewrite,
            &mut staged,
            Some(&root),
            &removed,
            None,
        )
        .await
        .expect("singleton branch deletion should collapse")
        .expect("shared subtree should remain");
        flush_reachable_staged_catalog_nodes(&mut rewrite, &staged, rewritten.root_id)
            .expect("collapsed root should flush");
        drop(read);
        adapter
            .commit_write_set(rewrite, StorageWriteOptions::default())
            .await
            .expect("collapsed catalog should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("collapsed catalog read should open");
        assert!(
            load_current_state_catalog_entry(&read, &rewritten, &removed)
                .await
                .expect("removed branch should miss")
                .is_none()
        );
        assert_eq!(
            load_current_state_catalog_entry(&read, &rewritten, &entries[64].scope)
                .await
                .expect("shared surviving subtree should route"),
            Some(entries[64].clone())
        );
    }

    #[tokio::test]
    async fn persistent_directory_routes_bounded_ranges_and_scans_in_order() {
        let adapter = StorageAdapter::new(Memory::new());
        let descriptors = descriptors(DIRECTORY_FANOUT * 2 + 7);
        let mut writes = adapter.new_write_set();
        let root = stage_current_state_part_directory(&mut writes, &descriptors)
            .expect("directory should stage");
        assert_eq!(root.tree_height, 2);
        assert_eq!(root.part_count as usize, descriptors.len());
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("directory should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        for index in [0, DIRECTORY_FANOUT, descriptors.len() - 1] {
            let key = format!("key-{index:06}-m");
            let routed = route_current_state_part(&read, &root, key.as_bytes())
                .await
                .expect("route should succeed")
                .expect("key should be covered");
            assert_eq!(routed, descriptors[index]);
        }
        assert!(
            route_current_state_part(&read, &root, b"key-000001-zz")
                .await
                .expect("gap route should succeed")
                .is_none()
        );
        assert_eq!(
            load_current_state_part_descriptors(&read, &root)
                .await
                .expect("scan should succeed"),
            descriptors
        );

        let mut foreign_descriptors = descriptors.clone();
        foreign_descriptors[0].content_digest = [7; 32];
        let mut foreign_writes = adapter.new_write_set();
        let foreign_root =
            stage_current_state_part_directory(&mut foreign_writes, &foreign_descriptors)
                .expect("foreign directory should stage");
        drop(read);
        adapter
            .commit_write_set(foreign_writes, StorageWriteOptions::default())
            .await
            .expect("foreign directory should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("cross-wire read should open");
        let forged_root = CurrentStatePartDirectoryRoot {
            root_id: foreign_root.root_id,
            ..root.clone()
        };
        assert!(
            route_current_state_part(&read, &forged_root, b"key-000000-m")
                .await
                .is_err(),
            "the Merkle directory digest must reject a cross-wired foreign root"
        );
    }

    #[tokio::test]
    async fn directory_splice_reads_and_rewrites_only_one_search_path() {
        let adapter = StorageAdapter::new(Memory::new());
        let descriptors = descriptors(DIRECTORY_FANOUT * 2 + 7);
        let mut writes = adapter.new_write_set();
        let root = stage_current_state_part_directory(&mut writes, &descriptors)
            .expect("directory should stage");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("directory should commit");

        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let read = CountingDirectoryRead {
            inner: adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("splice read should open"),
            directory_batch_sizes: Arc::clone(&batch_sizes),
            catalog_batch_sizes: Arc::new(Mutex::new(Vec::new())),
        };
        let key = Bytes::from_static(b"key-000001-m");
        let mut splice_writes = adapter.new_write_set();
        let plan = plan_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            &root,
            std::slice::from_ref(&key),
        )
        .await
        .expect("one-key splice should plan");
        assert_eq!(plan.leaf_count(), 1);
        let first_leaf_len = descriptors.len().div_ceil(3);
        assert_eq!(plan.leaf_parts(0), &descriptors[..first_leaf_len]);
        let mut replacement = plan.leaf_parts(0).to_vec();
        replacement[1].content_digest = [7; 32];
        let mut foreign_writes = adapter.new_write_set();
        assert!(
            stage_current_state_part_directory_splice(
                &read,
                &mut foreign_writes,
                plan.clone(),
                vec![replacement.clone()],
            )
            .await
            .is_err(),
            "a splice plan cannot move staged-only authority into another write set"
        );
        let rewritten = stage_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            plan,
            vec![replacement],
        )
        .await
        .expect("one-leaf splice should stage");
        assert_eq!(
            batch_sizes
                .lock()
                .expect("directory batch counts lock")
                .as_slice(),
            &[1, 1],
            "a two-level directory splice must read only root and one leaf"
        );
        assert_eq!(
            splice_writes.stats().staged_puts,
            2,
            "one update must stage one leaf and its root"
        );
        adapter
            .commit_write_set(splice_writes, StorageWriteOptions::default())
            .await
            .expect("splice should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("rewritten read should open");
        let mut expected = descriptors;
        expected[1].content_digest = [7; 32];
        assert_eq!(
            load_current_state_part_descriptors(&read, &rewritten)
                .await
                .expect("rewritten directory should flatten"),
            expected
        );
    }

    #[tokio::test]
    async fn directory_splice_splits_boundary_leaf_and_preserves_distant_subtrees() {
        let adapter = StorageAdapter::new(Memory::new());
        let descriptors = descriptors(DIRECTORY_FANOUT * 2);
        let mut writes = adapter.new_write_set();
        let root = stage_current_state_part_directory(&mut writes, &descriptors)
            .expect("directory should stage");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("directory should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("splice read should open");
        let original_root = load_node(&read, root.root_id)
            .await
            .expect("root should load");
        let untouched_middle = original_root.children[1].clone();

        let key = Bytes::from_static(b"key-000000-0");
        let mut splice_writes = adapter.new_write_set();
        let plan = plan_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            &root,
            std::slice::from_ref(&key),
        )
        .await
        .expect("boundary insertion should plan");
        let mut inserted = descriptors[0].clone();
        inserted.first_key = b"key-000000-0a".to_vec();
        inserted.last_key = b"key-000000-0z".to_vec();
        inserted.content_digest = [8; 32];
        let mut replacement = Vec::with_capacity(DIRECTORY_FANOUT + 1);
        replacement.push(inserted.clone());
        replacement.extend_from_slice(plan.leaf_parts(0));
        let rewritten = stage_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            plan,
            vec![replacement],
        )
        .await
        .expect("overflowing leaf should split");
        assert_eq!(
            splice_writes.stats().staged_puts,
            3,
            "one overflowing leaf stages two leaves and one root"
        );
        adapter
            .commit_write_set(splice_writes, StorageWriteOptions::default())
            .await
            .expect("boundary insertion should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("rewritten read should open");
        let rewritten_root = load_node(&read, rewritten.root_id)
            .await
            .expect("rewritten root should load");
        assert_eq!(rewritten_root.children[0].part_count, 65);
        assert_eq!(rewritten_root.children[1].part_count, 64);
        assert!(
            rewritten_root
                .children
                .iter()
                .any(|child| child == &untouched_middle),
            "the distant middle subtree must remain byte-identical"
        );
        let mut expected = Vec::with_capacity(descriptors.len() + 1);
        expected.push(inserted);
        expected.extend(descriptors);
        assert_eq!(
            load_current_state_part_descriptors(&read, &rewritten)
                .await
                .expect("split directory should flatten"),
            expected
        );
    }

    #[tokio::test]
    async fn directory_splice_removes_empty_leaf_without_rewriting_siblings() {
        let adapter = StorageAdapter::new(Memory::new());
        let descriptors = descriptors(DIRECTORY_FANOUT * 2);
        let mut writes = adapter.new_write_set();
        let root = stage_current_state_part_directory(&mut writes, &descriptors)
            .expect("directory should stage");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("directory should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("splice read should open");
        let original_root = load_node(&read, root.root_id)
            .await
            .expect("root should load");
        let removed_count = usize::try_from(original_root.children[0].part_count).unwrap();
        let surviving_child = original_root.children[1].clone();
        let key = Bytes::from_static(b"key-000001-m");
        let mut splice_writes = adapter.new_write_set();
        let plan = plan_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            &root,
            std::slice::from_ref(&key),
        )
        .await
        .expect("leaf removal should plan");
        let rewritten = stage_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            plan,
            vec![Vec::new()],
        )
        .await
        .expect("empty leaf should be removed");
        assert_eq!(
            splice_writes.stats().staged_puts,
            0,
            "contracting to an immutable sibling stages no directory node"
        );
        assert_eq!(rewritten.root_id, surviving_child.node_id);
        assert_eq!(rewritten.tree_height, 1);
        adapter
            .commit_write_set(splice_writes, StorageWriteOptions::default())
            .await
            .expect("leaf removal should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("rewritten read should open");
        assert_eq!(
            load_current_state_part_descriptors(&read, &rewritten)
                .await
                .expect("rewritten directory should flatten"),
            descriptors[removed_count..]
        );
    }

    #[tokio::test]
    async fn directory_splice_recursively_contracts_a_height_three_root() {
        let adapter = StorageAdapter::new(Memory::new());
        let descriptors = descriptors(DIRECTORY_FANOUT * DIRECTORY_FANOUT + 1);
        let survivor = descriptors
            .last()
            .expect("descriptors are non-empty")
            .clone();
        let mut writes = adapter.new_write_set();
        let root = stage_current_state_part_directory(&mut writes, &descriptors)
            .expect("height-three directory should stage");
        assert_eq!(root.tree_height, 3);
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("height-three directory should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("splice read should open");
        let deleted_keys = descriptors[..descriptors.len() - 1]
            .iter()
            .map(|descriptor| Bytes::copy_from_slice(&descriptor.first_key))
            .collect::<Vec<_>>();
        let mut splice_writes = adapter.new_write_set();
        let plan = plan_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            &root,
            &deleted_keys,
        )
        .await
        .expect("mass-delete splice should plan");
        let replacements = (0..plan.leaf_count())
            .map(|leaf_index| {
                plan.leaf_parts(leaf_index)
                    .iter()
                    .filter(|part| part.first_key == survivor.first_key)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let rewritten = stage_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            plan,
            replacements,
        )
        .await
        .expect("mass delete should recursively contract");
        assert_eq!(rewritten.tree_height, 1);
        assert_eq!(rewritten.part_count, 1);
        adapter
            .commit_write_set(splice_writes, StorageWriteOptions::default())
            .await
            .expect("contracted directory should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("contracted read should open");
        assert_eq!(
            load_current_state_part_descriptors(&read, &rewritten)
                .await
                .expect("contracted directory should flatten"),
            vec![survivor]
        );
    }

    #[tokio::test]
    async fn directory_splice_coalesces_staged_parent_noop() {
        let adapter = StorageAdapter::new(Memory::new());
        let descriptors = descriptors(DIRECTORY_FANOUT + 1);
        let mut writes = adapter.new_write_set();
        let root = stage_current_state_part_directory(&mut writes, &descriptors)
            .expect("staged parent should build");
        let puts_before = writes.stats().staged_puts;
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("empty backing read should open");
        let key = Bytes::from_static(b"key-000001-m");
        let plan = plan_current_state_part_directory_splice(
            &read,
            &mut writes,
            &root,
            std::slice::from_ref(&key),
        )
        .await
        .expect("staged parent should be readable");
        let unchanged = plan.leaf_parts(0).to_vec();
        let rewritten =
            stage_current_state_part_directory_splice(&read, &mut writes, plan, vec![unchanged])
                .await
                .expect("identical staged nodes should coalesce");
        assert_eq!(rewritten, root);
        assert_eq!(
            writes.stats().staged_puts,
            puts_before,
            "no-op path copying must not stage duplicate immutable puts"
        );
        writes
            .validate()
            .expect("coalesced staged-parent write set must validate");
    }

    #[tokio::test]
    async fn directory_splice_coalesces_two_distant_multilevel_paths() {
        let adapter = StorageAdapter::new(Memory::new());
        let descriptors = descriptors(DIRECTORY_FANOUT * DIRECTORY_FANOUT + 1);
        let mut writes = adapter.new_write_set();
        let root = stage_current_state_part_directory(&mut writes, &descriptors)
            .expect("multilevel directory should stage");
        assert_eq!(root.tree_height, 3);
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("multilevel directory should commit");
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let read = CountingDirectoryRead {
            inner: adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("splice read should open"),
            directory_batch_sizes: Arc::clone(&batch_sizes),
            catalog_batch_sizes: Arc::new(Mutex::new(Vec::new())),
        };
        let keys = [
            Bytes::from_static(b"key-000001-m"),
            Bytes::from(format!("key-{:06}-m", descriptors.len() - 1)),
        ];
        let mut splice_writes = adapter.new_write_set();
        let plan =
            plan_current_state_part_directory_splice(&read, &mut splice_writes, &root, &keys)
                .await
                .expect("distant splice should plan");
        assert_eq!(plan.leaf_count(), 2);
        let mut changed_keys = Vec::new();
        let replacements = (0..plan.leaf_count())
            .map(|leaf_index| {
                let mut parts = plan.leaf_parts(leaf_index).to_vec();
                changed_keys.push(parts[0].first_key.clone());
                parts[0].content_digest = [u8::try_from(10 + leaf_index).unwrap(); 32];
                parts
            })
            .collect::<Vec<_>>();
        let rewritten = stage_current_state_part_directory_splice(
            &read,
            &mut splice_writes,
            plan,
            replacements,
        )
        .await
        .expect("distant paths should splice");
        assert_eq!(
            batch_sizes
                .lock()
                .expect("directory batch counts lock")
                .len(),
            5,
            "two height-three paths share the root and read five nodes"
        );
        assert_eq!(
            splice_writes.stats().staged_puts,
            5,
            "two distant leaves stage their two parents and shared root"
        );
        adapter
            .commit_write_set(splice_writes, StorageWriteOptions::default())
            .await
            .expect("distant splice should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("rewritten read should open");
        let loaded = load_current_state_part_descriptors(&read, &rewritten)
            .await
            .expect("rewritten directory should flatten");
        for key in changed_keys {
            assert_ne!(
                loaded
                    .iter()
                    .find(|part| part.first_key == key)
                    .expect("changed descriptor remains present")
                    .content_digest,
                descriptors
                    .iter()
                    .find(|part| part.first_key == key)
                    .expect("original descriptor exists")
                    .content_digest
            );
        }
    }

    #[test]
    fn directory_nodes_reject_mixed_or_forged_levels() {
        let leaf = DirectoryNode::leaf(descriptors(1));
        let child = StagedNode {
            child: DirectoryChild {
                first_key: b"a".to_vec(),
                last_key: b"z".to_vec(),
                node_id: [1; 32],
                row_count: 1,
                part_count: 1,
                level: 0,
            },
        };
        let mut internal = DirectoryNode::internal(vec![child.child]);
        assert!(validate_node(&leaf).is_ok());
        assert!(validate_node(&internal).is_ok());
        internal.level = 2;
        assert!(validate_node(&internal).is_err());

        let overflowing_rows = DirectoryNode::internal(vec![
            DirectoryChild {
                first_key: b"a".to_vec(),
                last_key: b"b".to_vec(),
                node_id: [2; 32],
                row_count: u64::MAX,
                part_count: 1,
                level: 0,
            },
            DirectoryChild {
                first_key: b"c".to_vec(),
                last_key: b"d".to_vec(),
                node_id: [3; 32],
                row_count: 1,
                part_count: 1,
                level: 0,
            },
        ]);
        assert!(node_summary(&overflowing_rows).is_err());

        let overflowing_parts = DirectoryNode::internal(vec![
            DirectoryChild {
                first_key: b"a".to_vec(),
                last_key: b"b".to_vec(),
                node_id: [4; 32],
                row_count: 1,
                part_count: u32::MAX,
                level: 0,
            },
            DirectoryChild {
                first_key: b"c".to_vec(),
                last_key: b"d".to_vec(),
                node_id: [5; 32],
                row_count: 1,
                part_count: 1,
                level: 0,
            },
        ]);
        assert!(node_summary(&overflowing_parts).is_err());
    }

    #[tokio::test]
    async fn directory_routing_and_reachability_batch_each_tree_frontier() {
        let adapter = StorageAdapter::new(Memory::new());
        let descriptors = descriptors(DIRECTORY_FANOUT * DIRECTORY_FANOUT + 1);
        let mut writes = adapter.new_write_set();
        let root = stage_current_state_part_directory(&mut writes, &descriptors)
            .expect("multi-level directory should stage");
        assert!(root.tree_height >= 3);
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("multi-level directory should commit");

        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let read = CountingDirectoryRead {
            inner: adapter
                .begin_read(StorageReadOptions::default())
                .await
                .expect("directory read should open"),
            directory_batch_sizes: Arc::clone(&batch_sizes),
            catalog_batch_sizes: Arc::new(Mutex::new(Vec::new())),
        };
        let keys = descriptors
            .iter()
            .map(|descriptor| Bytes::copy_from_slice(&descriptor.first_key))
            .collect::<Vec<_>>();
        let routed = route_current_state_parts(&read, &root, &keys)
            .await
            .expect("multi-level batch should route");
        assert_eq!(
            routed,
            descriptors.iter().cloned().map(Some).collect::<Vec<_>>()
        );
        let routing_batches =
            std::mem::take(&mut *batch_sizes.lock().expect("directory batch counts lock"));
        assert_eq!(routing_batches.len(), usize::from(root.tree_height));
        assert!(routing_batches.iter().any(|&size| size > 1));

        let (node_ids, loaded) = load_current_state_part_directory_reachability(&read, &root)
            .await
            .expect("multi-level reachability should load");
        assert!(node_ids.len() > usize::from(root.tree_height));
        assert_eq!(loaded, descriptors);
        let reachability_batches = batch_sizes
            .lock()
            .expect("directory batch counts lock")
            .clone();
        assert_eq!(reachability_batches.len(), usize::from(root.tree_height));
        assert!(reachability_batches.iter().any(|&size| size > 1));

        batch_sizes
            .lock()
            .expect("directory batch counts lock")
            .clear();
        let requests = [(&root, keys.as_slice()), (&root, keys.as_slice())];
        let routed_sets = route_current_state_part_sets(&read, &requests)
            .await
            .expect("shared directory roots should route in one traversal");
        assert_eq!(routed_sets[0], routed_sets[1]);
        assert_eq!(
            batch_sizes
                .lock()
                .expect("directory batch counts lock")
                .len(),
            usize::from(root.tree_height)
        );

        batch_sizes
            .lock()
            .expect("directory batch counts lock")
            .clear();
        let (_, loaded_sets) = load_current_state_part_directory_reachability_many(
            &read,
            &[root.clone(), root.clone()],
        )
        .await
        .expect("shared directory roots should traverse once");
        assert_eq!(loaded_sets, vec![descriptors.clone(), descriptors.clone()]);
        assert_eq!(
            batch_sizes
                .lock()
                .expect("directory batch counts lock")
                .len(),
            usize::from(root.tree_height)
        );
    }

    #[test]
    fn directory_rejects_overlap_and_unknown_sources_but_allows_mixed_owners() {
        let adapter = StorageAdapter::new(Memory::new());
        let mut overlapping = descriptors(2);
        overlapping[1].first_key = overlapping[0].last_key.clone();
        assert!(
            stage_current_state_part_directory(&mut adapter.new_write_set(), &overlapping).is_err()
        );
        let mut owner_drift = descriptors(2);
        owner_drift[1].owner_commit_id = [8; 16];
        assert!(
            stage_current_state_part_directory(&mut adapter.new_write_set(), &owner_drift).is_ok()
        );
        let mut invalid_source = descriptors(2);
        invalid_source[1].source_kind = 2;
        assert!(
            stage_current_state_part_directory(&mut adapter.new_write_set(), &invalid_source)
                .is_err()
        );

        let mut oversized = descriptors(1);
        oversized[0].first_key = vec![b'a'; DIRECTORY_NODE_MAX_DECODED_BYTES / 2 + 1];
        oversized[0].last_key = vec![b'b'; DIRECTORY_NODE_MAX_DECODED_BYTES / 2 + 1];
        assert!(
            stage_current_state_part_directory(&mut adapter.new_write_set(), &oversized).is_err(),
            "staging must not publish a node that its decoder rejects"
        );
    }
}
