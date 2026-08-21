//! Frozen protocol-v68 (`LXMD1`) mutation-directory reader.
//!
//! The live directory codec is part of the current repository protocol and is
//! therefore intentionally unsuitable for migration reads.

use bytes::Bytes;

use crate::common::{LixError, LixTimestamp};
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StorageProjectedValue,
};
use crate::storage_codec;
use crate::tracked_state::{
    LAYOUT_BOUNDED_DIRECT, LAYOUT_BOUNDED_INDIRECT, LAYOUT_COMPACT_REPLACEMENT,
    LAYOUT_DIRECT_ROWS_ONLY, MUTATION_DIRECTORY_NODE_SPACE, MutationDirectoryRoot,
};

const NODE_MAGIC: &[u8] = b"LXMD1";
const NODE_HASH_CONTEXT: &str = "lix commit mutation directory node v1";
const ROOT_HASH_CONTEXT: &str = "lix commit mutation directory root v1";
const FANOUT: usize = 128;
const MAX_NODE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(in crate::migration) enum DirectoryEntry {
    Bounded {
        first_key: Vec<u8>,
        last_key: Vec<u8>,
        replacement_part: Option<ReplacementPart>,
        direct_row_count: u16,
    },
    CompactReplacement {
        content_digest: [u8; 32],
        direct_row_count: u16,
    },
    DirectAddress {
        direct_row_count: u16,
    },
}

#[derive(Debug, Clone, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(in crate::migration) struct ReplacementPart {
    pub(in crate::migration) content_digest: [u8; 32],
    pub(in crate::migration) owner_commit_id: [u8; 16],
    pub(in crate::migration) first_address: u32,
    pub(in crate::migration) uniform_created_at: LixTimestamp,
    pub(in crate::migration) uniform_updated_at: LixTimestamp,
}

#[derive(Debug, Clone, musli::Encode, musli::Decode)]
enum StoredEntry {
    Bounded {
        #[musli(bytes)]
        first_key: Vec<u8>,
        #[musli(bytes)]
        last_key: Vec<u8>,
        #[musli(with = storage_codec::option)]
        replacement_part: Option<ReplacementPart>,
        direct_row_count: u16,
    },
    CompactReplacement {
        content_digest: [u8; 32],
        direct_row_count: u16,
    },
    DirectAddress {
        direct_row_count: u16,
    },
}

#[derive(Debug, Clone, musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredChild {
    #[musli(bytes)]
    first_key: Vec<u8>,
    #[musli(bytes)]
    last_key: Vec<u8>,
    node_id: [u8; 32],
    entry_count: u32,
    direct_row_count: u64,
    level: u16,
    layout: u8,
}

#[derive(Debug, Clone, musli::Encode, musli::Decode)]
enum StoredNode {
    Leaf {
        layout: u8,
        entries: Vec<StoredEntry>,
    },
    Internal {
        layout: u8,
        level: u16,
        children: Vec<StoredChild>,
    },
}

#[derive(Clone)]
struct Summary {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    node_id: [u8; 32],
    entry_count: u32,
    direct_row_count: u64,
    level: u16,
    layout: u8,
}

pub(in crate::migration) async fn load_all(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &MutationDirectoryRoot,
) -> Result<Vec<DirectoryEntry>, LixError> {
    validate_root_for_header(root)?;
    let mut frontier = vec![(root.root_id, None::<Summary>)];
    let mut entries = Vec::with_capacity(root.entry_count as usize);
    while !frontier.is_empty() {
        let node_ids = frontier.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let nodes = load_nodes(store, &node_ids).await?;
        let mut next = Vec::new();
        for ((node_id, expected), node) in frontier.into_iter().zip(nodes) {
            let actual = summary(&node, node_id)?;
            if let Some(expected) = expected {
                if !same_summary(&actual, &expected) {
                    return Err(error("child summary mismatch"));
                }
            } else if actual.node_id != root.root_id
                || actual.entry_count != root.entry_count
                || actual.direct_row_count != root.direct_row_count
                || actual.level.checked_add(1) != Some(root.tree_height)
                || actual.layout != root.layout
            {
                return Err(error("root summary mismatch"));
            }
            match node {
                StoredNode::Leaf { entries: leaf, .. } => {
                    entries.extend(leaf.into_iter().map(runtime_entry));
                }
                StoredNode::Internal { children, .. } => {
                    next.extend(children.into_iter().map(|child| {
                        let expected = Summary {
                            first_key: child.first_key,
                            last_key: child.last_key,
                            node_id: child.node_id,
                            entry_count: child.entry_count,
                            direct_row_count: child.direct_row_count,
                            level: child.level,
                            layout: child.layout,
                        };
                        (expected.node_id, Some(expected))
                    }));
                }
            }
        }
        frontier = next;
    }
    if entries.len() != root.entry_count as usize {
        return Err(error("entry count disagrees with root"));
    }
    validate_runtime_entries(root.layout, &entries)?;
    Ok(entries)
}

async fn load_nodes(
    store: &(impl StorageAdapterRead + ?Sized),
    node_ids: &[[u8; 32]],
) -> Result<Vec<StoredNode>, LixError> {
    let keys = node_ids
        .iter()
        .map(|id| StorageKey(Bytes::copy_from_slice(id)))
        .collect::<Vec<_>>();
    let values = PointReadPlan::new(MUTATION_DIRECTORY_NODE_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    node_ids
        .iter()
        .zip(values.value)
        .map(|(expected_id, value)| {
            let Some(StorageProjectedValue::FullValue(bytes)) = value else {
                return Err(error("tree references a missing node"));
            };
            if node_digest(&bytes) != *expected_id {
                return Err(error("node content digest mismatch"));
            }
            decode_node(&bytes)
        })
        .collect()
}

fn decode_node(bytes: &[u8]) -> Result<StoredNode, LixError> {
    let payload = bytes
        .strip_prefix(NODE_MAGIC)
        .ok_or_else(|| error("node has an unsupported format"))?;
    if payload.len() > MAX_NODE_BYTES {
        return Err(error("node exceeds its size bound"));
    }
    let node = storage_codec::decode("v68 commit mutation directory node", payload)?;
    validate_node(&node)?;
    Ok(node)
}

fn validate_node(node: &StoredNode) -> Result<(), LixError> {
    match node {
        StoredNode::Leaf { layout, entries } => {
            validate_layout(*layout)?;
            if entries.is_empty() || entries.len() > FANOUT {
                return Err(error("leaf shape is invalid"));
            }
            validate_stored_entries(*layout, entries)
        }
        StoredNode::Internal {
            layout,
            level,
            children,
        } => {
            validate_layout(*layout)?;
            if *level == 0 || children.is_empty() || children.len() > FANOUT {
                return Err(error("internal node shape is invalid"));
            }
            let child_level = children[0].level;
            if child_level.checked_add(1) != Some(*level)
                || children.iter().any(|child| {
                    child.layout != *layout
                        || child.level != child_level
                        || child.node_id == [0; 32]
                        || child.entry_count == 0
                        || (is_bounded(*layout)
                            && (child.first_key.is_empty() || child.first_key > child.last_key))
                        || (!is_bounded(*layout)
                            && (!child.first_key.is_empty() || !child.last_key.is_empty()))
                })
                || (is_bounded(*layout)
                    && children
                        .windows(2)
                        .any(|pair| pair[0].last_key >= pair[1].first_key))
            {
                return Err(error("internal child summaries are invalid"));
            }
            Ok(())
        }
    }
}

fn summary(node: &StoredNode, node_id: [u8; 32]) -> Result<Summary, LixError> {
    match node {
        StoredNode::Leaf { layout, entries } => Ok(Summary {
            first_key: stored_first(&entries[0]).to_vec(),
            last_key: stored_last(&entries[entries.len() - 1]).to_vec(),
            node_id,
            entry_count: u32::try_from(entries.len())
                .map_err(|_| error("entry count overflows"))?,
            direct_row_count: entries.iter().try_fold(0u64, |sum, entry| {
                sum.checked_add(u64::from(stored_rows(entry)))
                    .ok_or_else(|| error("row count overflows"))
            })?,
            level: 0,
            layout: *layout,
        }),
        StoredNode::Internal {
            layout,
            level,
            children,
        } => {
            let (entry_count, direct_row_count) =
                children
                    .iter()
                    .try_fold((0u32, 0u64), |(entries, rows), child| {
                        Ok::<_, LixError>((
                            entries
                                .checked_add(child.entry_count)
                                .ok_or_else(|| error("entry count overflows"))?,
                            rows.checked_add(child.direct_row_count)
                                .ok_or_else(|| error("row count overflows"))?,
                        ))
                    })?;
            Ok(Summary {
                first_key: children[0].first_key.clone(),
                last_key: children[children.len() - 1].last_key.clone(),
                node_id,
                entry_count,
                direct_row_count,
                level: *level,
                layout: *layout,
            })
        }
    }
}

fn same_summary(left: &Summary, right: &Summary) -> bool {
    left.first_key == right.first_key
        && left.last_key == right.last_key
        && left.node_id == right.node_id
        && left.entry_count == right.entry_count
        && left.direct_row_count == right.direct_row_count
        && left.level == right.level
        && left.layout == right.layout
}

fn runtime_entry(entry: StoredEntry) -> DirectoryEntry {
    match entry {
        StoredEntry::Bounded {
            first_key,
            last_key,
            replacement_part,
            direct_row_count,
        } => DirectoryEntry::Bounded {
            first_key,
            last_key,
            replacement_part,
            direct_row_count,
        },
        StoredEntry::CompactReplacement {
            content_digest,
            direct_row_count,
        } => DirectoryEntry::CompactReplacement {
            content_digest,
            direct_row_count,
        },
        StoredEntry::DirectAddress { direct_row_count } => {
            DirectoryEntry::DirectAddress { direct_row_count }
        }
    }
}

fn validate_stored_entries(layout: u8, entries: &[StoredEntry]) -> Result<(), LixError> {
    validate_layout(layout)?;
    for entry in entries {
        match (layout, entry) {
            (
                LAYOUT_BOUNDED_INDIRECT,
                StoredEntry::Bounded {
                    first_key,
                    last_key,
                    direct_row_count: 0,
                    ..
                },
            ) if !first_key.is_empty() && first_key <= last_key => {}
            (
                LAYOUT_BOUNDED_DIRECT,
                StoredEntry::Bounded {
                    first_key,
                    last_key,
                    direct_row_count,
                    ..
                },
            ) if !first_key.is_empty() && first_key <= last_key && *direct_row_count > 0 => {}
            (
                LAYOUT_COMPACT_REPLACEMENT,
                StoredEntry::CompactReplacement {
                    content_digest,
                    direct_row_count,
                },
            ) if *content_digest != [0; 32] && *direct_row_count > 0 => {}
            (LAYOUT_DIRECT_ROWS_ONLY, StoredEntry::DirectAddress { direct_row_count })
                if *direct_row_count > 0 => {}
            _ => return Err(error("entry disagrees with directory layout")),
        }
    }
    if is_bounded(layout)
        && entries
            .windows(2)
            .any(|pair| stored_last(&pair[0]) >= stored_first(&pair[1]))
    {
        return Err(error("bounded entries overlap or are unordered"));
    }
    Ok(())
}

fn validate_runtime_entries(layout: u8, entries: &[DirectoryEntry]) -> Result<(), LixError> {
    for entry in entries {
        match (layout, entry) {
            (
                LAYOUT_BOUNDED_INDIRECT,
                DirectoryEntry::Bounded {
                    direct_row_count: 0,
                    ..
                },
            ) => {}
            (
                LAYOUT_BOUNDED_DIRECT,
                DirectoryEntry::Bounded {
                    direct_row_count, ..
                },
            ) if *direct_row_count > 0 => {}
            (
                LAYOUT_COMPACT_REPLACEMENT,
                DirectoryEntry::CompactReplacement {
                    content_digest,
                    direct_row_count,
                },
            ) if *content_digest != [0; 32] && *direct_row_count > 0 => {}
            (LAYOUT_DIRECT_ROWS_ONLY, DirectoryEntry::DirectAddress { direct_row_count })
                if *direct_row_count > 0 => {}
            _ => return Err(error("decoded entry disagrees with root layout")),
        }
    }
    Ok(())
}

pub(in crate::migration) fn validate_root_for_header(
    root: &MutationDirectoryRoot,
) -> Result<(), LixError> {
    validate_layout(root.layout)?;
    if root.root_id == [0; 32]
        || root.root_digest == [0; 32]
        || root.entry_count == 0
        || root.tree_height == 0
        || (root.layout == LAYOUT_BOUNDED_INDIRECT && root.direct_row_count != 0)
        || (root.layout != LAYOUT_BOUNDED_INDIRECT && root.direct_row_count == 0)
        || root.root_digest != root_digest(root)
    {
        return Err(error("root is invalid"));
    }
    Ok(())
}

fn validate_layout(layout: u8) -> Result<(), LixError> {
    if matches!(
        layout,
        LAYOUT_BOUNDED_INDIRECT
            | LAYOUT_BOUNDED_DIRECT
            | LAYOUT_COMPACT_REPLACEMENT
            | LAYOUT_DIRECT_ROWS_ONLY
    ) {
        Ok(())
    } else {
        Err(error("layout is unsupported"))
    }
}

fn is_bounded(layout: u8) -> bool {
    matches!(layout, LAYOUT_BOUNDED_INDIRECT | LAYOUT_BOUNDED_DIRECT)
}

fn stored_first(entry: &StoredEntry) -> &[u8] {
    match entry {
        StoredEntry::Bounded { first_key, .. } => first_key,
        _ => &[],
    }
}

fn stored_last(entry: &StoredEntry) -> &[u8] {
    match entry {
        StoredEntry::Bounded { last_key, .. } => last_key,
        _ => &[],
    }
}

fn stored_rows(entry: &StoredEntry) -> u16 {
    match entry {
        StoredEntry::Bounded {
            direct_row_count, ..
        }
        | StoredEntry::CompactReplacement {
            direct_row_count, ..
        }
        | StoredEntry::DirectAddress { direct_row_count } => *direct_row_count,
    }
}

fn node_digest(bytes: &[u8]) -> [u8; 32] {
    let mut digest = blake3::Hasher::new_derive_key(NODE_HASH_CONTEXT);
    digest.update(bytes);
    *digest.finalize().as_bytes()
}

fn root_digest(root: &MutationDirectoryRoot) -> [u8; 32] {
    let mut digest = blake3::Hasher::new_derive_key(ROOT_HASH_CONTEXT);
    digest.update(&root.root_id);
    digest.update(&root.entry_count.to_be_bytes());
    digest.update(&root.direct_row_count.to_be_bytes());
    digest.update(&root.tree_height.to_be_bytes());
    digest.update(&[root.layout]);
    *digest.finalize().as_bytes()
}

fn error(message: impl Into<String>) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("v68 tracked_state mutation directory: {}", message.into()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{
        Memory, StorageAdapter, StorageReadOptions, StorageValue, StorageWriteOptions,
    };

    #[tokio::test]
    async fn reads_frozen_lxmd1_external_leaf() {
        let node = StoredNode::Leaf {
            layout: LAYOUT_BOUNDED_DIRECT,
            entries: vec![StoredEntry::Bounded {
                first_key: vec![1],
                last_key: vec![2],
                replacement_part: None,
                direct_row_count: 1,
            }],
        };
        let payload = storage_codec::encode("frozen LXMD1 fixture", &node).unwrap();
        let mut encoded = NODE_MAGIC.to_vec();
        encoded.extend_from_slice(&payload);
        let node_id = node_digest(&encoded);
        let mut root = MutationDirectoryRoot {
            root_id: node_id,
            root_digest: [0; 32],
            entry_count: 1,
            direct_row_count: 1,
            tree_height: 1,
            layout: LAYOUT_BOUNDED_DIRECT,
        };
        root.root_digest = root_digest(&root);

        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        writes.put(
            MUTATION_DIRECTORY_NODE_SPACE,
            StorageKey(Bytes::copy_from_slice(&node_id)),
            StorageValue {
                bytes: Bytes::from(encoded),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();

        let entries = load_all(&read, &root).await.unwrap();
        assert!(matches!(
            entries.as_slice(),
            [DirectoryEntry::Bounded {
                first_key,
                last_key,
                replacement_part: None,
                direct_row_count: 1,
            }] if first_key == &[1] && last_key == &[2]
        ));
    }
}
