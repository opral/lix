//! Authenticated immutable directory for commit mutation parts.
//!
//! Commit headers authenticate a small catalog, and that catalog authenticates
//! one root from this tree. Part bounds, compact replacement identities, and
//! direct-address row counts live only in leaves. Metadata-only operations can
//! therefore stop at the header while point routing reads one node per level.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StorageProjectedValue,
    StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::{LixError, storage_codec};

use super::types::CommitStateMutationPart;

pub(crate) const MUTATION_DIRECTORY_NODE_SPACE: StorageSpace = StorageSpace::immutable(
    StorageSpaceId(0x0004_002d),
    "tracked_state.commit_mutation_directory_node.v1",
);

const NODE_MAGIC: &[u8] = b"LXMD1";
const NODE_HASH_CONTEXT: &str = "lix commit mutation directory node v1";
const ROOT_HASH_CONTEXT: &str = "lix commit mutation directory root v1";
const FANOUT: usize = 128;
const MAX_NODE_BYTES: usize = 16 * 1024 * 1024;

pub(crate) const LAYOUT_BOUNDED_INDIRECT: u8 = 1;
pub(crate) const LAYOUT_BOUNDED_DIRECT: u8 = 2;
pub(crate) const LAYOUT_COMPACT_REPLACEMENT: u8 = 3;
pub(crate) const LAYOUT_DIRECT_ROWS_ONLY: u8 = 4;

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct MutationDirectoryRoot {
    pub(crate) root_id: [u8; 32],
    pub(crate) root_digest: [u8; 32],
    pub(crate) entry_count: u32,
    pub(crate) direct_row_count: u64,
    pub(crate) tree_height: u16,
    pub(crate) layout: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MutationDirectoryEntry {
    Bounded {
        part: CommitStateMutationPart,
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

impl MutationDirectoryEntry {
    fn direct_row_count(&self) -> u16 {
        match self {
            Self::Bounded {
                direct_row_count, ..
            }
            | Self::CompactReplacement {
                direct_row_count, ..
            }
            | Self::DirectAddress { direct_row_count } => *direct_row_count,
        }
    }

    fn first_key(&self) -> &[u8] {
        match self {
            Self::Bounded { part, .. } => &part.first_key,
            Self::CompactReplacement { .. } => &[],
            Self::DirectAddress { .. } => &[],
        }
    }

    fn last_key(&self) -> &[u8] {
        match self {
            Self::Bounded { part, .. } => &part.last_key,
            Self::CompactReplacement { .. } => &[],
            Self::DirectAddress { .. } => &[],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MutationDirectoryRoute {
    pub(crate) entry_index: u32,
    pub(crate) part: CommitStateMutationPart,
    pub(crate) direct_row_count: u16,
}

#[derive(Debug)]
pub(crate) struct BuiltMutationDirectory {
    pub(crate) root: MutationDirectoryRoot,
    nodes: BTreeMap<[u8; 32], Bytes>,
}

impl BuiltMutationDirectory {
    pub(crate) fn stage(&self, writes: &mut StorageWriteSet) -> Result<(), LixError> {
        for (node_id, bytes) in &self.nodes {
            if let Some(existing) = writes.staged_value(MUTATION_DIRECTORY_NODE_SPACE, node_id) {
                if existing != *bytes {
                    return Err(directory_error("content ID has conflicting staged bytes"));
                }
                continue;
            }
            writes.put(
                MUTATION_DIRECTORY_NODE_SPACE,
                StorageKey(Bytes::copy_from_slice(node_id)),
                StorageValue {
                    bytes: bytes.clone(),
                },
            );
        }
        Ok(())
    }

    pub(crate) fn node_bytes(&self) -> &BTreeMap<[u8; 32], Bytes> {
        &self.nodes
    }
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
enum StoredEntry {
    Bounded {
        #[musli(bytes)]
        first_key: Vec<u8>,
        #[musli(bytes)]
        last_key: Vec<u8>,
        #[musli(with = storage_codec::option)]
        replacement_part: Option<super::types::StoredReplacementPart>,
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

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
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

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeSummary {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    node_id: [u8; 32],
    entry_count: u32,
    direct_row_count: u64,
    level: u16,
    layout: u8,
}

impl From<&NodeSummary> for StoredChild {
    fn from(summary: &NodeSummary) -> Self {
        Self {
            first_key: summary.first_key.clone(),
            last_key: summary.last_key.clone(),
            node_id: summary.node_id,
            entry_count: summary.entry_count,
            direct_row_count: summary.direct_row_count,
            level: summary.level,
            layout: summary.layout,
        }
    }
}

impl From<&StoredChild> for NodeSummary {
    fn from(child: &StoredChild) -> Self {
        Self {
            first_key: child.first_key.clone(),
            last_key: child.last_key.clone(),
            node_id: child.node_id,
            entry_count: child.entry_count,
            direct_row_count: child.direct_row_count,
            level: child.level,
            layout: child.layout,
        }
    }
}

#[cfg(test)]
pub(crate) fn build_mutation_directory(
    layout: u8,
    entries: &[MutationDirectoryEntry],
) -> Result<BuiltMutationDirectory, LixError> {
    validate_entries(layout, entries)?;
    build_stored_mutation_directory(layout, entries.iter().map(stored_entry).collect())
}

pub(crate) fn build_bounded_mutation_directory(
    parts: &[CommitStateMutationPart],
    direct_row_counts: Option<&[u16]>,
) -> Result<BuiltMutationDirectory, LixError> {
    if direct_row_counts.is_some_and(|rows| rows.len() != parts.len()) {
        return Err(directory_error(
            "bounded direct-row counts do not match part count",
        ));
    }
    let layout = if direct_row_counts.is_some() {
        LAYOUT_BOUNDED_DIRECT
    } else {
        LAYOUT_BOUNDED_INDIRECT
    };
    let entries = parts
        .iter()
        .enumerate()
        .map(|(index, part)| StoredEntry::Bounded {
            first_key: part.first_key.clone(),
            last_key: part.last_key.clone(),
            replacement_part: part.replacement_part.clone(),
            direct_row_count: direct_row_counts.map_or(0, |rows| rows[index]),
        })
        .collect();
    build_stored_mutation_directory(layout, entries)
}

pub(crate) fn build_compact_replacement_mutation_directory(
    content_digests: &[[u8; 32]],
    direct_row_counts: &[u16],
) -> Result<BuiltMutationDirectory, LixError> {
    if content_digests.len() != direct_row_counts.len() {
        return Err(directory_error(
            "compact replacement counts do not match digest count",
        ));
    }
    build_stored_mutation_directory(
        LAYOUT_COMPACT_REPLACEMENT,
        content_digests
            .iter()
            .copied()
            .zip(direct_row_counts.iter().copied())
            .map(
                |(content_digest, direct_row_count)| StoredEntry::CompactReplacement {
                    content_digest,
                    direct_row_count,
                },
            )
            .collect(),
    )
}

pub(crate) fn build_direct_rows_mutation_directory(
    direct_row_counts: &[u16],
) -> Result<BuiltMutationDirectory, LixError> {
    build_stored_mutation_directory(
        LAYOUT_DIRECT_ROWS_ONLY,
        direct_row_counts
            .iter()
            .copied()
            .map(|direct_row_count| StoredEntry::DirectAddress { direct_row_count })
            .collect(),
    )
}

fn build_stored_mutation_directory(
    layout: u8,
    entries: Vec<StoredEntry>,
) -> Result<BuiltMutationDirectory, LixError> {
    validate_stored_entries(layout, &entries)?;
    if entries.is_empty() {
        return Err(directory_error("cannot build an empty directory"));
    }
    let mut nodes = BTreeMap::new();
    let mut entries = entries.into_iter();
    let mut level = balanced_chunk_lengths(entries.len())
        .map(|chunk_len| {
            let stored = StoredNode::Leaf {
                layout,
                entries: entries.by_ref().take(chunk_len).collect(),
            };
            stage_encoded_node(&mut nodes, stored)
        })
        .collect::<Result<Vec<_>, _>>()?;
    debug_assert!(entries.next().is_none());
    let mut tree_height = 1u16;
    while level.len() > 1 {
        level = balanced_chunks(&level)
            .into_iter()
            .map(|chunk| {
                let child_level = chunk[0].level;
                let stored = StoredNode::Internal {
                    layout,
                    level: child_level
                        .checked_add(1)
                        .ok_or_else(|| directory_error("tree height overflows"))?,
                    children: chunk.iter().map(StoredChild::from).collect(),
                };
                stage_encoded_node(&mut nodes, stored)
            })
            .collect::<Result<Vec<_>, _>>()?;
        tree_height = tree_height
            .checked_add(1)
            .ok_or_else(|| directory_error("tree height overflows"))?;
    }
    let summary = level.pop().expect("non-empty directory has a root");
    let root = MutationDirectoryRoot {
        root_id: summary.node_id,
        root_digest: root_digest(
            summary.node_id,
            summary.entry_count,
            summary.direct_row_count,
            tree_height,
            layout,
        ),
        entry_count: summary.entry_count,
        direct_row_count: summary.direct_row_count,
        tree_height,
        layout,
    };
    validate_root(&root)?;
    Ok(BuiltMutationDirectory { root, nodes })
}

pub(crate) async fn load_mutation_directory(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &MutationDirectoryRoot,
) -> Result<Vec<MutationDirectoryEntry>, LixError> {
    Ok(load_mutation_directories(store, std::slice::from_ref(root))
        .await?
        .pop()
        .expect("one requested directory returns one result"))
}

/// Loads many authenticated roots level-by-level. Shared nodes and same-level
/// frontiers issue one physical point-read batch instead of one request chain
/// per commit.
pub(crate) async fn load_mutation_directories(
    store: &(impl StorageAdapterRead + ?Sized),
    roots: &[MutationDirectoryRoot],
) -> Result<Vec<Vec<MutationDirectoryEntry>>, LixError> {
    for root in roots {
        validate_root(root)?;
    }
    let mut frontiers = roots
        .iter()
        .map(|root| vec![(root.root_id, None::<NodeSummary>)])
        .collect::<Vec<_>>();
    let mut outputs = roots
        .iter()
        .map(|root| Vec::with_capacity(root.entry_count as usize))
        .collect::<Vec<_>>();
    while frontiers.iter().any(|frontier| !frontier.is_empty()) {
        let node_ids = frontiers
            .iter()
            .flatten()
            .map(|(node_id, _)| *node_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let loaded = node_ids
            .iter()
            .copied()
            .zip(load_nodes(store, &node_ids).await?)
            .collect::<BTreeMap<_, _>>();
        for (root_index, frontier) in frontiers.iter_mut().enumerate() {
            let mut next = Vec::new();
            for (node_id, expected) in std::mem::take(frontier) {
                let (node, summary) = loaded
                    .get(&node_id)
                    .ok_or_else(|| directory_error("directory batch omitted a node"))?;
                match expected {
                    Some(expected) if expected != *summary => {
                        return Err(directory_error("child summary mismatch"));
                    }
                    None if !summary_matches_root(summary, &roots[root_index]) => {
                        return Err(directory_error("root summary mismatch"));
                    }
                    _ => {}
                }
                match node {
                    StoredNode::Leaf { entries, .. } => outputs[root_index].extend(
                        entries
                            .iter()
                            .cloned()
                            .map(runtime_entry)
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                    StoredNode::Internal { children, .. } => {
                        next.extend(children.iter().map(|child| {
                            let summary = NodeSummary::from(child);
                            (child.node_id, Some(summary))
                        }));
                    }
                }
            }
            *frontier = next;
        }
    }
    for (root, entries) in roots.iter().zip(&outputs) {
        validate_entries(root.layout, entries)?;
        if entries.len() != root.entry_count as usize
            || entries
                .iter()
                .map(MutationDirectoryEntry::direct_row_count)
                .map(u64::from)
                .sum::<u64>()
                != root.direct_row_count
        {
            return Err(directory_error("directory closure disagrees with its root"));
        }
    }
    Ok(outputs)
}

/// Routes ordered or unordered point keys through the authenticated bounded
/// directory without decoding unrelated leaves.
pub(crate) async fn route_mutation_directory_points(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &MutationDirectoryRoot,
    keys: &[&[u8]],
) -> Result<Vec<Option<MutationDirectoryRoute>>, LixError> {
    validate_root(root)?;
    if root.layout != LAYOUT_BOUNDED_DIRECT && root.layout != LAYOUT_BOUNDED_INDIRECT {
        return Err(directory_error(
            "compact replacement directory is not key-routable",
        ));
    }
    #[derive(Clone)]
    struct Pending {
        output_index: usize,
        key: Vec<u8>,
        base_index: u32,
        expected: Option<NodeSummary>,
    }
    let mut frontier = BTreeMap::<[u8; 32], Vec<Pending>>::new();
    frontier.insert(
        root.root_id,
        keys.iter()
            .enumerate()
            .map(|(output_index, key)| Pending {
                output_index,
                key: key.to_vec(),
                base_index: 0,
                expected: None,
            })
            .collect(),
    );
    let mut output = vec![None; keys.len()];
    while !frontier.is_empty() {
        let node_ids = frontier.keys().copied().collect::<Vec<_>>();
        let loaded = load_nodes(store, &node_ids).await?;
        let mut next = BTreeMap::<[u8; 32], Vec<Pending>>::new();
        for ((node_id, pending), (node, summary)) in frontier.into_iter().zip(loaded.into_iter()) {
            for request in pending {
                match request.expected.as_ref() {
                    Some(expected) if expected != &summary => {
                        return Err(directory_error("point route child summary mismatch"));
                    }
                    None if !summary_matches_root(&summary, root) => {
                        return Err(directory_error("point route root summary mismatch"));
                    }
                    _ => {}
                }
                match &node {
                    StoredNode::Leaf { entries, .. } => {
                        let index = match entries.binary_search_by(|entry| {
                            stored_entry_first_key(entry).cmp(request.key.as_slice())
                        }) {
                            Ok(index) => index,
                            Err(0) => continue,
                            Err(index) => index - 1,
                        };
                        let entry = runtime_entry(entries[index].clone())?;
                        if request.key.as_slice() <= entry.last_key() {
                            let MutationDirectoryEntry::Bounded {
                                part,
                                direct_row_count,
                            } = entry
                            else {
                                return Err(directory_error("bounded leaf contains compact entry"));
                            };
                            output[request.output_index] = Some(MutationDirectoryRoute {
                                entry_index: request
                                    .base_index
                                    .checked_add(u32::try_from(index).map_err(|_| {
                                        directory_error("leaf entry index overflows")
                                    })?)
                                    .ok_or_else(|| directory_error("entry index overflows"))?,
                                part,
                                direct_row_count,
                            });
                        }
                    }
                    StoredNode::Internal { children, .. } => {
                        let index = match children.binary_search_by(|child| {
                            child.first_key.as_slice().cmp(request.key.as_slice())
                        }) {
                            Ok(index) => index,
                            Err(0) => continue,
                            Err(index) => index - 1,
                        };
                        let child = &children[index];
                        if request.key.as_slice() > child.last_key.as_slice() {
                            continue;
                        }
                        let preceding = children[..index].iter().try_fold(0u32, |sum, child| {
                            sum.checked_add(child.entry_count)
                                .ok_or_else(|| directory_error("entry offset overflows"))
                        })?;
                        next.entry(child.node_id).or_default().push(Pending {
                            base_index: request
                                .base_index
                                .checked_add(preceding)
                                .ok_or_else(|| directory_error("entry offset overflows"))?,
                            expected: Some(NodeSummary::from(child)),
                            ..request
                        });
                    }
                }
            }
            let _ = node_id;
        }
        frontier = next;
    }
    Ok(output)
}

pub(crate) async fn collect_mutation_directory_node_ids(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &MutationDirectoryRoot,
) -> Result<BTreeSet<[u8; 32]>, LixError> {
    validate_root(root)?;
    let mut reachable = BTreeSet::new();
    let mut frontier = vec![(root.root_id, None::<NodeSummary>)];
    while !frontier.is_empty() {
        let node_ids = frontier
            .iter()
            .map(|(node_id, _)| *node_id)
            .collect::<Vec<_>>();
        let loaded = load_nodes(store, &node_ids).await?;
        let mut next = Vec::new();
        for ((node_id, expected), (node, summary)) in frontier.into_iter().zip(loaded) {
            match expected {
                Some(expected) if expected != summary => {
                    return Err(directory_error("reachability child summary mismatch"));
                }
                None if !summary_matches_root(&summary, root) => {
                    return Err(directory_error("reachability root summary mismatch"));
                }
                _ => {}
            }
            if !reachable.insert(node_id) {
                continue;
            }
            if let StoredNode::Internal { children, .. } = node {
                next.extend(children.into_iter().map(|child| {
                    let summary = NodeSummary::from(&child);
                    (child.node_id, Some(summary))
                }));
            }
        }
        frontier = next;
    }
    Ok(reachable)
}

#[cfg(test)]
pub(crate) fn decode_built_mutation_directory(
    built: &BuiltMutationDirectory,
) -> Result<Vec<MutationDirectoryEntry>, LixError> {
    validate_root(&built.root)?;
    let mut frontier = vec![(built.root.root_id, None::<NodeSummary>)];
    let mut entries = Vec::new();
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for (node_id, expected) in frontier {
            let bytes = built
                .nodes
                .get(&node_id)
                .ok_or_else(|| directory_error("built tree omitted a node"))?;
            let node = decode_node(bytes)?;
            let summary = node_summary(&node, node_id)?;
            match expected {
                Some(expected) if expected != summary => {
                    return Err(directory_error("built child summary mismatch"));
                }
                None if !summary_matches_root(&summary, &built.root) => {
                    return Err(directory_error("built root summary mismatch"));
                }
                _ => {}
            }
            match node {
                StoredNode::Leaf { entries: leaf, .. } => entries.extend(
                    leaf.into_iter()
                        .map(runtime_entry)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                StoredNode::Internal { children, .. } => {
                    next.extend(children.into_iter().map(|child| {
                        let summary = NodeSummary::from(&child);
                        (child.node_id, Some(summary))
                    }))
                }
            }
        }
        frontier = next;
    }
    validate_entries(built.root.layout, &entries)?;
    Ok(entries)
}

async fn load_nodes(
    store: &(impl StorageAdapterRead + ?Sized),
    node_ids: &[[u8; 32]],
) -> Result<Vec<(StoredNode, NodeSummary)>, LixError> {
    if node_ids.is_empty() {
        return Ok(Vec::new());
    }
    let keys = node_ids
        .iter()
        .map(|node_id| StorageKey(Bytes::copy_from_slice(node_id)))
        .collect::<Vec<_>>();
    let values = PointReadPlan::new(MUTATION_DIRECTORY_NODE_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    node_ids
        .iter()
        .zip(values)
        .map(|(node_id, value)| {
            let value = value.ok_or_else(|| directory_error("tree references a missing node"))?;
            let StorageProjectedValue::FullValue(bytes) = value else {
                return Err(directory_error("node read omitted its value"));
            };
            if node_digest(&bytes) != *node_id {
                return Err(directory_error("node content digest mismatch"));
            }
            let node = decode_node(&bytes)?;
            let summary = node_summary(&node, *node_id)?;
            Ok((node, summary))
        })
        .collect()
}

fn stage_encoded_node(
    nodes: &mut BTreeMap<[u8; 32], Bytes>,
    node: StoredNode,
) -> Result<NodeSummary, LixError> {
    let bytes = encode_node(&node)?;
    let node_id = node_digest(&bytes);
    let summary = node_summary(&node, node_id)?;
    match nodes.entry(node_id) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(bytes);
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &bytes => {}
        std::collections::btree_map::Entry::Occupied(_) => {
            return Err(directory_error("content ID collision"));
        }
    }
    Ok(summary)
}

fn encode_node(node: &StoredNode) -> Result<Bytes, LixError> {
    validate_node(node)?;
    let payload = storage_codec::encode("commit mutation directory node", node)?;
    if payload.len() > MAX_NODE_BYTES {
        return Err(directory_error("node exceeds its size bound"));
    }
    let mut bytes = Vec::with_capacity(NODE_MAGIC.len() + payload.len());
    bytes.extend_from_slice(NODE_MAGIC);
    bytes.extend_from_slice(&payload);
    Ok(Bytes::from(bytes))
}

fn decode_node(bytes: &[u8]) -> Result<StoredNode, LixError> {
    let Some(payload) = bytes.strip_prefix(NODE_MAGIC) else {
        return Err(directory_error("node has an unsupported format"));
    };
    if payload.len() > MAX_NODE_BYTES {
        return Err(directory_error("node exceeds its size bound"));
    }
    let node = storage_codec::decode("commit mutation directory node", payload)?;
    validate_node(&node)?;
    Ok(node)
}

fn validate_node(node: &StoredNode) -> Result<(), LixError> {
    match node {
        StoredNode::Leaf { layout, entries } => {
            validate_layout(*layout)?;
            if entries.is_empty() || entries.len() > FANOUT {
                return Err(directory_error("leaf shape is invalid"));
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
                return Err(directory_error("internal node shape is invalid"));
            }
            let child_level = children[0].level;
            if child_level.checked_add(1) != Some(*level)
                || children.iter().any(|child| {
                    child.layout != *layout
                        || child.level != child_level
                        || child.node_id == [0; 32]
                        || child.entry_count == 0
                        || (is_bounded(*layout)
                            && (child.first_key.is_empty()
                                || child.first_key.as_slice() > child.last_key.as_slice()))
                        || (!is_bounded(*layout)
                            && (!child.first_key.is_empty() || !child.last_key.is_empty()))
                })
                || (is_bounded(*layout)
                    && children
                        .windows(2)
                        .any(|pair| pair[0].last_key >= pair[1].first_key))
            {
                return Err(directory_error("internal child summaries are invalid"));
            }
            Ok(())
        }
    }
}

fn node_summary(node: &StoredNode, node_id: [u8; 32]) -> Result<NodeSummary, LixError> {
    match node {
        StoredNode::Leaf { layout, entries } => {
            let entry_count = u32::try_from(entries.len())
                .map_err(|_| directory_error("entry count overflows"))?;
            let direct_row_count = entries.iter().try_fold(0u64, |sum, entry| {
                sum.checked_add(u64::from(stored_entry_direct_rows(entry)))
                    .ok_or_else(|| directory_error("row count overflows"))
            })?;
            Ok(NodeSummary {
                first_key: stored_entry_first_key(&entries[0]).to_vec(),
                last_key: stored_entry_last_key(&entries[entries.len() - 1]).to_vec(),
                node_id,
                entry_count,
                direct_row_count,
                level: 0,
                layout: *layout,
            })
        }
        StoredNode::Internal {
            layout,
            level,
            children,
        } => {
            let (entry_count, direct_row_count) =
                children
                    .iter()
                    .try_fold((0u32, 0u64), |(entry_sum, row_sum), child| {
                        Ok::<_, LixError>((
                            entry_sum
                                .checked_add(child.entry_count)
                                .ok_or_else(|| directory_error("entry count overflows"))?,
                            row_sum
                                .checked_add(child.direct_row_count)
                                .ok_or_else(|| directory_error("row count overflows"))?,
                        ))
                    })?;
            Ok(NodeSummary {
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

fn validate_entries(layout: u8, entries: &[MutationDirectoryEntry]) -> Result<(), LixError> {
    validate_layout(layout)?;
    if entries.is_empty() {
        return Ok(());
    }
    for entry in entries {
        match (layout, entry) {
            (
                LAYOUT_BOUNDED_INDIRECT,
                MutationDirectoryEntry::Bounded {
                    part,
                    direct_row_count: 0,
                },
            ) if valid_part(part) => {}
            (
                LAYOUT_BOUNDED_DIRECT,
                MutationDirectoryEntry::Bounded {
                    part,
                    direct_row_count,
                },
            ) if valid_part(part) && *direct_row_count > 0 => {}
            (
                LAYOUT_COMPACT_REPLACEMENT,
                MutationDirectoryEntry::CompactReplacement {
                    content_digest,
                    direct_row_count,
                },
            ) if *content_digest != [0; 32] && *direct_row_count > 0 => {}
            (
                LAYOUT_DIRECT_ROWS_ONLY,
                MutationDirectoryEntry::DirectAddress { direct_row_count },
            ) if *direct_row_count > 0 => {}
            _ => return Err(directory_error("entry disagrees with directory layout")),
        }
    }
    if is_bounded(layout)
        && entries
            .windows(2)
            .any(|pair| pair[0].last_key() >= pair[1].first_key())
    {
        return Err(directory_error("bounded entries overlap or are unordered"));
    }
    Ok(())
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
            _ => return Err(directory_error("entry disagrees with directory layout")),
        }
    }
    if is_bounded(layout)
        && entries
            .windows(2)
            .any(|pair| stored_entry_last_key(&pair[0]) >= stored_entry_first_key(&pair[1]))
    {
        return Err(directory_error("bounded entries overlap or are unordered"));
    }
    Ok(())
}

fn validate_root(root: &MutationDirectoryRoot) -> Result<(), LixError> {
    validate_layout(root.layout)?;
    if root.root_id == [0; 32]
        || root.root_digest == [0; 32]
        || root.entry_count == 0
        || root.tree_height == 0
        || (root.layout == LAYOUT_BOUNDED_INDIRECT && root.direct_row_count != 0)
        || ((root.layout == LAYOUT_BOUNDED_DIRECT
            || root.layout == LAYOUT_COMPACT_REPLACEMENT
            || root.layout == LAYOUT_DIRECT_ROWS_ONLY)
            && root.direct_row_count == 0)
        || root.root_digest
            != root_digest(
                root.root_id,
                root.entry_count,
                root.direct_row_count,
                root.tree_height,
                root.layout,
            )
    {
        return Err(directory_error("root is invalid"));
    }
    Ok(())
}

pub(crate) fn validate_mutation_directory_root(
    root: &MutationDirectoryRoot,
) -> Result<(), LixError> {
    validate_root(root)
}

fn summary_matches_root(summary: &NodeSummary, root: &MutationDirectoryRoot) -> bool {
    summary.node_id == root.root_id
        && summary.entry_count == root.entry_count
        && summary.direct_row_count == root.direct_row_count
        && summary.level.checked_add(1) == Some(root.tree_height)
        && summary.layout == root.layout
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
        Err(directory_error("layout is unsupported"))
    }
}

fn is_bounded(layout: u8) -> bool {
    layout == LAYOUT_BOUNDED_INDIRECT || layout == LAYOUT_BOUNDED_DIRECT
}

fn valid_part(part: &CommitStateMutationPart) -> bool {
    !part.first_key.is_empty() && part.first_key <= part.last_key
}

#[cfg(test)]
fn stored_entry(entry: &MutationDirectoryEntry) -> StoredEntry {
    match entry {
        MutationDirectoryEntry::Bounded {
            part,
            direct_row_count,
        } => StoredEntry::Bounded {
            first_key: part.first_key.clone(),
            last_key: part.last_key.clone(),
            replacement_part: part.replacement_part.clone(),
            direct_row_count: *direct_row_count,
        },
        MutationDirectoryEntry::CompactReplacement {
            content_digest,
            direct_row_count,
        } => StoredEntry::CompactReplacement {
            content_digest: *content_digest,
            direct_row_count: *direct_row_count,
        },
        MutationDirectoryEntry::DirectAddress { direct_row_count } => StoredEntry::DirectAddress {
            direct_row_count: *direct_row_count,
        },
    }
}

fn runtime_entry(entry: StoredEntry) -> Result<MutationDirectoryEntry, LixError> {
    let entry = match entry {
        StoredEntry::Bounded {
            first_key,
            last_key,
            replacement_part,
            direct_row_count,
        } => MutationDirectoryEntry::Bounded {
            part: CommitStateMutationPart {
                first_key,
                last_key,
                replacement_part,
            },
            direct_row_count,
        },
        StoredEntry::CompactReplacement {
            content_digest,
            direct_row_count,
        } => MutationDirectoryEntry::CompactReplacement {
            content_digest,
            direct_row_count,
        },
        StoredEntry::DirectAddress { direct_row_count } => {
            MutationDirectoryEntry::DirectAddress { direct_row_count }
        }
    };
    Ok(entry)
}

fn stored_entry_first_key(entry: &StoredEntry) -> &[u8] {
    match entry {
        StoredEntry::Bounded { first_key, .. } => first_key,
        StoredEntry::CompactReplacement { .. } => &[],
        StoredEntry::DirectAddress { .. } => &[],
    }
}

fn stored_entry_last_key(entry: &StoredEntry) -> &[u8] {
    match entry {
        StoredEntry::Bounded { last_key, .. } => last_key,
        StoredEntry::CompactReplacement { .. } => &[],
        StoredEntry::DirectAddress { .. } => &[],
    }
}

fn stored_entry_direct_rows(entry: &StoredEntry) -> u16 {
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

fn balanced_chunks<T>(values: &[T]) -> Vec<&[T]> {
    debug_assert!(!values.is_empty());
    let chunk_count = values.len().div_ceil(FANOUT);
    let base = values.len() / chunk_count;
    let larger = values.len() % chunk_count;
    let mut chunks = Vec::with_capacity(chunk_count);
    let mut start = 0usize;
    for index in 0..chunk_count {
        let length = base + usize::from(index < larger);
        chunks.push(&values[start..start + length]);
        start += length;
    }
    chunks
}

fn balanced_chunk_lengths(value_count: usize) -> impl Iterator<Item = usize> {
    let chunk_count = value_count.div_ceil(FANOUT);
    let base = value_count / chunk_count;
    let larger = value_count % chunk_count;
    (0..chunk_count).map(move |index| base + usize::from(index < larger))
}

fn node_digest(bytes: &[u8]) -> [u8; 32] {
    let mut digest = blake3::Hasher::new_derive_key(NODE_HASH_CONTEXT);
    digest.update(bytes);
    *digest.finalize().as_bytes()
}

fn root_digest(
    root_id: [u8; 32],
    entry_count: u32,
    direct_row_count: u64,
    tree_height: u16,
    layout: u8,
) -> [u8; 32] {
    let mut digest = blake3::Hasher::new_derive_key(ROOT_HASH_CONTEXT);
    digest.update(&root_id);
    digest.update(&entry_count.to_be_bytes());
    digest.update(&direct_row_count.to_be_bytes());
    digest.update(&tree_height.to_be_bytes());
    digest.update(&[layout]);
    *digest.finalize().as_bytes()
}

fn directory_error(message: impl Into<String>) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state mutation directory: {}", message.into()),
    )
}

#[cfg(test)]
mod tests {
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    use super::*;

    fn bounded_entry(index: u32) -> MutationDirectoryEntry {
        let first_key = index.saturating_mul(10).to_be_bytes().to_vec();
        let last_key = index
            .saturating_mul(10)
            .saturating_add(9)
            .to_be_bytes()
            .to_vec();
        MutationDirectoryEntry::Bounded {
            part: CommitStateMutationPart {
                first_key,
                last_key,
                replacement_part: None,
            },
            direct_row_count: 7,
        }
    }

    #[tokio::test]
    async fn multi_level_directory_round_trips_and_routes_without_flattening() {
        let entries = (0..(FANOUT as u32 * 2 + 17))
            .map(bounded_entry)
            .collect::<Vec<_>>();
        let built = build_mutation_directory(LAYOUT_BOUNDED_DIRECT, &entries).unwrap();
        assert!(built.root.tree_height >= 2);
        assert!(built.node_bytes().len() > 1);

        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        built.stage(&mut writes).unwrap();
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();

        let loaded = load_mutation_directory(&read, &built.root).await.unwrap();
        assert_eq!(loaded, entries);
        let requested = [0u32, FANOUT as u32, FANOUT as u32 * 2 + 16];
        let keys = requested
            .iter()
            .map(|index| index.saturating_mul(10).saturating_add(4).to_be_bytes())
            .collect::<Vec<_>>();
        let key_refs = keys.iter().map(<[u8; 4]>::as_slice).collect::<Vec<_>>();
        let routed = route_mutation_directory_points(&read, &built.root, &key_refs)
            .await
            .unwrap();
        for (&expected_index, route) in requested.iter().zip(routed) {
            let route = route.expect("covered key should route");
            assert_eq!(route.entry_index, expected_index);
            assert_eq!(route.direct_row_count, 7);
            assert_eq!(
                route.part,
                match &entries[expected_index as usize] {
                    MutationDirectoryEntry::Bounded { part, .. } => part.clone(),
                    _ => unreachable!(),
                }
            );
        }
        assert_eq!(
            collect_mutation_directory_node_ids(&read, &built.root)
                .await
                .unwrap()
                .len(),
            built.node_bytes().len()
        );
    }
}
