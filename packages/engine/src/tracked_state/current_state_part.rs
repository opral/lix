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
use crate::tracked_state::types::{CommitStateMutationInventory, CurrentStatePartSet};
use crate::tracked_state::types::{CurrentStatePartDescriptor, CurrentStatePartDirectoryRoot};
use crate::{LixError, storage_codec};

pub(crate) const CURRENT_STATE_PART_DIRECTORY_SPACE: StorageSpace = StorageSpace::immutable(
    StorageSpaceId(0x0004_002c),
    "tracked_state.current_state_part_directory.v1",
);

const DIRECTORY_NODE_RAW_MAGIC: &[u8; 6] = b"LXCSDR";
const DIRECTORY_NODE_ZSTD_MAGIC: &[u8; 6] = b"LXCSDZ";
const DIRECTORY_NODE_MAX_DECODED_BYTES: usize = 16 * 1024 * 1024;
const DIRECTORY_FANOUT: usize = 128;
const DIRECTORY_HASH_CONTEXT: &str = "lix current-state part directory node v1";

/// Publishes the serving directory for a certified complete replacement.
/// Ordinary mutation inventories are not state partitions and return `None`.
pub(crate) fn stage_complete_replacement_current_state_part_set(
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
    inventory: &CommitStateMutationInventory,
) -> Result<Option<CurrentStatePartSet>, LixError> {
    let Some(generation) = inventory.replacement_generation.as_ref() else {
        return Ok(None);
    };
    let authority = inventory.replacement_parts.as_ref().ok_or_else(|| {
        directory_error("replacement generation omitted its immutable part authority")
    })?;
    if inventory.parts.len() != inventory.direct_part_row_counts.len() || inventory.parts.is_empty()
    {
        return Err(directory_error(
            "replacement generation has no complete directly-addressable part set",
        ));
    }
    let mut first_ordinal = 0u32;
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
                owner_commit_id: part.owner_commit_id,
                part_index: u32::try_from(part_index)
                    .map_err(|_| directory_error("part index overflows u32"))?,
                first_ordinal,
                row_count,
                uniform_created_at: part.uniform_created_at,
                uniform_updated_at: part.uniform_updated_at,
            };
            first_ordinal = first_ordinal
                .checked_add(u32::from(row_count))
                .ok_or_else(|| directory_error("row ordinal overflows u32"))?;
            Ok(descriptor)
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let directory = stage_current_state_part_directory(writes, &descriptors)?;
    if directory.row_count != u64::from(inventory.member_count)
        || directory.descriptor_digest != authority.directory_digest
        || generation.owner_commit_id != *commit_id.as_uuid().as_bytes()
    {
        return Err(directory_error(
            "replacement state set disagrees with its commit authority",
        ));
    }
    Ok(Some(CurrentStatePartSet {
        scope: generation.scope.clone(),
        owner_commit_id: generation.owner_commit_id,
        generation_integrity_digest: generation.integrity_digest,
        mutation_directory_digest: authority.directory_digest,
        directory,
    }))
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
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct DirectoryNode {
    kind: u8,
    /// The semantic replacement-directory digest for the complete part set.
    /// Every node carries it so a valid node from another tree cannot turn a
    /// routing miss into an authoritative absence.
    set_digest: [u8; 32],
    parts: Vec<CurrentStatePartDescriptor>,
    children: Vec<DirectoryChild>,
}

impl DirectoryNode {
    fn leaf(set_digest: [u8; 32], parts: Vec<CurrentStatePartDescriptor>) -> Self {
        Self {
            kind: 0,
            set_digest,
            parts,
            children: Vec::new(),
        }
    }

    fn internal(set_digest: [u8; 32], children: Vec<DirectoryChild>) -> Self {
        Self {
            kind: 1,
            set_digest,
            parts: Vec::new(),
            children,
        }
    }
}

#[derive(Debug, Clone)]
struct StagedNode {
    child: DirectoryChild,
}

/// Stages one complete ordered post-image part set as a bounded persistent
/// range directory. Equal nodes naturally share storage across generations.
pub(crate) fn stage_current_state_part_directory(
    writes: &mut StorageWriteSet,
    descriptors: &[CurrentStatePartDescriptor],
) -> Result<CurrentStatePartDirectoryRoot, LixError> {
    validate_descriptors(descriptors)?;
    // Reuse the historical replacement-directory certificate rather than
    // inventing a second digest vocabulary for the same immutable part set.
    let descriptor_digest = replacement_directory_digest(descriptors)?;
    let mut level = descriptors
        .chunks(DIRECTORY_FANOUT)
        .map(|chunk| {
            stage_node(
                writes,
                DirectoryNode::leaf(descriptor_digest, chunk.to_vec()),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut tree_height = 1u16;
    while level.len() > 1 {
        level = level
            .chunks(DIRECTORY_FANOUT)
            .map(|chunk| {
                stage_node(
                    writes,
                    DirectoryNode::internal(
                        descriptor_digest,
                        chunk.iter().map(|node| node.child.clone()).collect(),
                    ),
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
        descriptor_digest,
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
    let mut node_id = root.root_id;
    loop {
        let node = load_node(store, node_id, root.descriptor_digest).await?;
        if node_id == root.root_id {
            validate_root_summary(root, &node)?;
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
    let mut routes = vec![None; encoded_keys.len()];
    let mut pending = vec![(
        root.root_id,
        (0..encoded_keys.len()).collect::<Vec<usize>>(),
    )];
    while let Some((node_id, key_indices)) = pending.pop() {
        let node = load_node(store, node_id, root.descriptor_digest).await?;
        if node_id == root.root_id {
            validate_root_summary(root, &node)?;
        }
        match node.kind {
            0 => {
                for key_index in key_indices {
                    let key = &encoded_keys[key_index];
                    let index = node
                        .parts
                        .partition_point(|part| part.first_key.as_slice() <= key.as_ref());
                    let Some(part) = index.checked_sub(1).and_then(|index| node.parts.get(index))
                    else {
                        continue;
                    };
                    if key.as_ref() <= part.last_key.as_slice() {
                        routes[key_index] = Some(part.clone());
                    }
                }
            }
            1 => {
                let mut child_keys = std::collections::BTreeMap::<usize, Vec<usize>>::new();
                for key_index in key_indices {
                    let key = &encoded_keys[key_index];
                    let upper = node
                        .children
                        .partition_point(|child| child.first_key.as_slice() <= key.as_ref());
                    if let Some(child_index) = upper.checked_sub(1)
                        && key.as_ref() <= node.children[child_index].last_key.as_slice()
                    {
                        child_keys.entry(child_index).or_default().push(key_index);
                    }
                }
                pending.extend(
                    child_keys
                        .into_iter()
                        .rev()
                        .map(|(child_index, keys)| (node.children[child_index].node_id, keys)),
                );
            }
            _ => unreachable!("validated directory node kind"),
        }
    }
    Ok(routes)
}

/// Loads the complete ordered descriptor set for scans, rebuild, and audit.
pub(crate) async fn load_current_state_part_descriptors(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &CurrentStatePartDirectoryRoot,
) -> Result<Vec<CurrentStatePartDescriptor>, LixError> {
    let mut pending = vec![root.root_id];
    let mut descriptors = Vec::with_capacity(root.part_count as usize);
    while let Some(node_id) = pending.pop() {
        let mut node = load_node(store, node_id, root.descriptor_digest).await?;
        if node_id == root.root_id {
            validate_root_summary(root, &node)?;
        }
        match node.kind {
            0 => descriptors.append(&mut node.parts),
            1 => {
                pending.extend(node.children.into_iter().rev().map(|child| child.node_id));
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
        || replacement_directory_digest(&descriptors)? != root.descriptor_digest
    {
        return Err(directory_error(
            "root summary does not match its descriptor set",
        ));
    }
    Ok(descriptors)
}

/// Deletes a complete replacement directory. First-slice leaf descriptors bind
/// every node to one owner commit, so no node can be shared across owners yet.
/// Sparse structural sharing will replace this with mark/sweep reachability.
pub(crate) async fn stage_delete_current_state_part_directory(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    root: &CurrentStatePartDirectoryRoot,
    expected_owner_commit_id: [u8; 16],
    expected_mutation_directory_digest: [u8; 32],
) -> Result<(), LixError> {
    let descriptors = load_current_state_part_descriptors(store, root).await?;
    if root.descriptor_digest != expected_mutation_directory_digest
        || descriptors
            .iter()
            .any(|descriptor| descriptor.owner_commit_id != expected_owner_commit_id)
    {
        return Err(directory_error(
            "refusing to delete a directory not owned by its dead authority",
        ));
    }
    let mut pending = vec![root.root_id];
    while let Some(node_id) = pending.pop() {
        let node = load_node(store, node_id, root.descriptor_digest).await?;
        if node.kind == 1 {
            pending.extend(node.children.into_iter().map(|child| child.node_id));
        }
        writes.delete(
            CURRENT_STATE_PART_DIRECTORY_SPACE,
            StorageKey(Bytes::copy_from_slice(&node_id)),
        );
    }
    Ok(())
}

fn stage_node(writes: &mut StorageWriteSet, node: DirectoryNode) -> Result<StagedNode, LixError> {
    let (first_key, last_key, row_count, part_count) = node_summary(&node)?;
    let bytes = encode_node(&node)?;
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_crud_current_state_directory_bytes(bytes.len());
    let node_id = node_digest(&bytes);
    writes.put(
        CURRENT_STATE_PART_DIRECTORY_SPACE,
        StorageKey(Bytes::copy_from_slice(&node_id)),
        StorageValue {
            bytes: bytes.clone(),
        },
    );
    Ok(StagedNode {
        child: DirectoryChild {
            first_key,
            last_key,
            node_id,
            row_count,
            part_count,
        },
    })
}

async fn load_node(
    store: &(impl StorageAdapterRead + ?Sized),
    node_id: [u8; 32],
    expected_set_digest: [u8; 32],
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
    let node = decode_node(&bytes)?;
    if node.set_digest != expected_set_digest {
        return Err(directory_error("node belongs to a different part set"));
    }
    Ok(node)
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
    let entries = descriptors
        .iter()
        .map(|descriptor| {
            crate::tracked_state::replacement_part::ReplacementPartDirectoryEntry::new(
                descriptor.content_digest,
                &descriptor.first_key,
                &descriptor.last_key,
                descriptor.first_ordinal,
                descriptor.row_count,
            )
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

fn validate_root_summary(
    root: &CurrentStatePartDirectoryRoot,
    node: &DirectoryNode,
) -> Result<(), LixError> {
    let (_, _, row_count, part_count) = node_summary(node)?;
    if row_count != root.row_count || part_count != root.part_count {
        return Err(directory_error("root summary disagrees with its node"));
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
        1 => Ok((
            node.children[0].first_key.clone(),
            node.children
                .last()
                .expect("validated internal node is non-empty")
                .last_key
                .clone(),
            node.children.iter().map(|child| child.row_count).sum(),
            node.children.iter().map(|child| child.part_count).sum(),
        )),
        _ => unreachable!("validated directory node kind"),
    }
}

fn validate_node(node: &DirectoryNode) -> Result<(), LixError> {
    match node.kind {
        0 => {
            if !node.children.is_empty() || node.parts.len() > DIRECTORY_FANOUT {
                return Err(directory_error("leaf exceeds bounded fanout"));
            }
            validate_descriptor_slice(&node.parts, false)
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
    validate_descriptor_slice(parts, true)
}

fn validate_descriptor_slice(
    parts: &[CurrentStatePartDescriptor],
    require_zero_origin: bool,
) -> Result<(), LixError> {
    let first = parts.first();
    if first.is_none()
        || (require_zero_origin
            && first.is_some_and(|part| part.part_index != 0 || part.first_ordinal != 0))
        || parts.iter().any(|part| {
            part.first_key.is_empty()
                || part.last_key.is_empty()
                || part.first_key > part.last_key
                || part.row_count == 0
                || part.content_digest == [0; 32]
        })
        || parts
            .windows(2)
            .any(|pair| pair[0].last_key >= pair[1].first_key)
        || parts.windows(2).any(|pair| {
            pair[1].part_index != pair[0].part_index + 1
                || pair[1].first_ordinal != pair[0].first_ordinal + u32::from(pair[0].row_count)
                || pair[1].owner_commit_id != pair[0].owner_commit_id
                || pair[1].uniform_created_at != pair[0].uniform_created_at
                || pair[1].uniform_updated_at != pair[0].uniform_updated_at
        })
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
    use crate::common::LixTimestamp;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    use super::*;

    fn descriptors(count: usize) -> Vec<CurrentStatePartDescriptor> {
        let timestamp = LixTimestamp::from_unix_millis_utc_lossy(7);
        (0..count)
            .map(|index| CurrentStatePartDescriptor {
                first_key: format!("key-{index:06}-a").into_bytes(),
                last_key: format!("key-{index:06}-z").into_bytes(),
                content_digest: *blake3::hash(&index.to_be_bytes()).as_bytes(),
                owner_commit_id: [9; 16],
                part_index: u32::try_from(index).expect("fixture index fits u32"),
                first_ordinal: u32::try_from(index * 10).expect("fixture ordinal fits u32"),
                row_count: 10,
                uniform_created_at: timestamp,
                uniform_updated_at: timestamp,
            })
            .collect()
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
            "a valid root from another certified set must not authorize a miss"
        );

        let mut deletes = adapter.new_write_set();
        assert!(
            stage_delete_current_state_part_directory(
                &read,
                &mut deletes,
                &root,
                [8; 16],
                root.descriptor_digest,
            )
            .await
            .is_err(),
            "GC must fail closed when a dead manifest is cross-wired"
        );
        stage_delete_current_state_part_directory(
            &read,
            &mut deletes,
            &root,
            [9; 16],
            root.descriptor_digest,
        )
        .await
        .expect("directory deletion should stage");
        drop(read);
        adapter
            .commit_write_set(deletes, StorageWriteOptions::default())
            .await
            .expect("directory deletion should commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("post-delete read should open");
        assert!(
            route_current_state_part(&read, &root, b"key-000000-m")
                .await
                .is_err()
        );
    }

    #[test]
    fn directory_rejects_overlap_and_owner_drift() {
        let adapter = StorageAdapter::new(Memory::new());
        let mut overlapping = descriptors(2);
        overlapping[1].first_key = overlapping[0].last_key.clone();
        assert!(
            stage_current_state_part_directory(&mut adapter.new_write_set(), &overlapping).is_err()
        );
        let mut owner_drift = descriptors(2);
        owner_drift[1].owner_commit_id = [8; 16];
        assert!(
            stage_current_state_part_directory(&mut adapter.new_write_set(), &owner_drift).is_err()
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
