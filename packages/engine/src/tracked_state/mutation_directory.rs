//! Authenticated immutable directory for commit mutation parts.
//!
//! Commit headers authenticate a small catalog, and that catalog authenticates
//! one root from this tree. Part bounds, compact replacement identities, and
//! direct-address row counts live only in leaves. Metadata-only operations can
//! therefore stop at the header while point routing reads one node per level.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Range;

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

    #[cfg(test)]
    fn first_key(&self) -> &[u8] {
        match self {
            Self::Bounded { part, .. } => &part.first_key,
            Self::CompactReplacement { .. } => &[],
            Self::DirectAddress { .. } => &[],
        }
    }

    #[cfg(test)]
    fn last_key(&self) -> &[u8] {
        match self {
            Self::Bounded { part, .. } => &part.last_key,
            Self::CompactReplacement { .. } => &[],
            Self::DirectAddress { .. } => &[],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MutationDirectoryKeyRange {
    pub(crate) start: Bytes,
    pub(crate) end: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MutationDirectoryDirectCoordinate {
    pub(crate) part_index: u32,
    pub(crate) local_row: u16,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum MutationDirectoryReadSelection<'a> {
    All,
    SortedRanges(&'a [MutationDirectoryKeyRange]),
    SortedUniquePoints(&'a [Bytes]),
    SortedUniqueDirectCoordinates(&'a [MutationDirectoryDirectCoordinate]),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MutationDirectoryPartRun {
    pub(crate) entry_index: u32,
    pub(crate) selector_span: Range<usize>,
    pub(crate) entry: MutationDirectoryEntry,
}

#[derive(Debug)]
pub(crate) struct AuthenticatedMutationPartReadPlan {
    runs: Vec<MutationDirectoryPartRun>,
    #[cfg(test)]
    visited_node_count: usize,
    #[cfg(test)]
    node_summary_owner_count: usize,
    #[cfg(test)]
    node_summary_clone_count: usize,
    #[cfg(test)]
    part_clone_count: usize,
}

impl AuthenticatedMutationPartReadPlan {
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.runs.len()
    }

    pub(crate) fn into_runs(self) -> Vec<MutationDirectoryPartRun> {
        self.runs
    }

    #[cfg(test)]
    pub(crate) fn visited_node_count(&self) -> usize {
        self.visited_node_count
    }

    #[cfg(test)]
    pub(crate) fn node_summary_owner_count(&self) -> usize {
        self.node_summary_owner_count
    }

    #[cfg(test)]
    pub(crate) fn node_summary_clone_count(&self) -> usize {
        self.node_summary_clone_count
    }

    #[cfg(test)]
    pub(crate) fn part_clone_count(&self) -> usize {
        self.part_clone_count
    }
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

#[cfg(test)]
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

impl From<StoredChild> for NodeSummary {
    fn from(child: StoredChild) -> Self {
        Self {
            first_key: child.first_key,
            last_key: child.last_key,
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
    build_stored_mutation_directory(layout, entries.iter().map(stored_entry))
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
    if parts.iter().any(|part| !valid_part(part))
        || parts
            .windows(2)
            .any(|pair| pair[0].last_key >= pair[1].first_key)
        || direct_row_counts.is_some_and(|rows| rows.contains(&0))
    {
        return Err(directory_error("bounded entries overlap or are invalid"));
    }
    let entries = parts
        .iter()
        .enumerate()
        .map(|(index, part)| StoredEntry::Bounded {
            first_key: part.first_key.clone(),
            last_key: part.last_key.clone(),
            replacement_part: part.replacement_part.clone(),
            direct_row_count: direct_row_counts.map_or(0, |rows| rows[index]),
        });
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
    if content_digests.contains(&[0; 32]) || direct_row_counts.contains(&0) {
        return Err(directory_error("compact replacement entry is invalid"));
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
            ),
    )
}

pub(crate) fn build_direct_rows_mutation_directory(
    direct_row_counts: &[u16],
) -> Result<BuiltMutationDirectory, LixError> {
    if direct_row_counts.contains(&0) {
        return Err(directory_error("direct-address entry is invalid"));
    }
    build_stored_mutation_directory(
        LAYOUT_DIRECT_ROWS_ONLY,
        direct_row_counts
            .iter()
            .copied()
            .map(|direct_row_count| StoredEntry::DirectAddress { direct_row_count }),
    )
}

fn build_stored_mutation_directory<I>(
    layout: u8,
    entries: I,
) -> Result<BuiltMutationDirectory, LixError>
where
    I: ExactSizeIterator<Item = StoredEntry>,
{
    validate_layout(layout)?;
    if entries.len() == 0 {
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

/// Loads complete authenticated plans for many roots level-by-level. Shared
/// nodes and same-level frontiers issue one physical point-read batch instead
/// of one request chain per commit. The output remains the same ordered run
/// contract as selective reads; there is no second flat-directory interface.
pub(crate) async fn load_all_mutation_part_read_plans(
    store: &(impl StorageAdapterRead + ?Sized),
    roots: &[MutationDirectoryRoot],
) -> Result<Vec<AuthenticatedMutationPartReadPlan>, LixError> {
    for root in roots {
        validate_root(root)?;
    }
    let mut frontiers = roots
        .iter()
        .map(|root| vec![(root.root_id, 0u32, None::<NodeSummary>)])
        .collect::<Vec<_>>();
    let mut outputs = roots
        .iter()
        .map(|root| Vec::with_capacity(root.entry_count as usize))
        .collect::<Vec<_>>();
    #[cfg(test)]
    let mut visited_node_counts = vec![0usize; roots.len()];
    #[cfg(test)]
    let mut node_summary_owner_counts = vec![0usize; roots.len()];
    #[cfg(test)]
    let mut node_summary_clone_counts = vec![0usize; roots.len()];
    #[cfg(test)]
    let mut part_clone_counts = vec![0usize; roots.len()];
    while frontiers.iter().any(|frontier| !frontier.is_empty()) {
        let mut node_ids = Vec::new();
        let mut use_counts = HashMap::<[u8; 32], usize>::new();
        for (node_id, _, _) in frontiers.iter().flatten() {
            let node_id = *node_id;
            match use_counts.entry(node_id) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(1);
                    node_ids.push(node_id);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    *entry.get_mut() += 1;
                }
            }
        }
        let loaded_nodes = load_nodes(store, &node_ids, true).await?;
        let mut loaded = node_ids
            .into_iter()
            .zip(loaded_nodes)
            .map(|(node_id, node)| {
                let use_count = use_counts[&node_id];
                (node_id, (node, use_count))
            })
            .collect::<HashMap<_, _>>();
        for (root_index, frontier) in frontiers.iter_mut().enumerate() {
            let mut next = Vec::new();
            for (node_id, base_index, expected) in std::mem::take(frontier) {
                let remaining_uses = loaded
                    .get(&node_id)
                    .map(|(_, use_count)| *use_count)
                    .ok_or_else(|| directory_error("directory batch omitted a node"))?;
                let node = if remaining_uses == 1 {
                    loaded
                        .remove(&node_id)
                        .expect("loaded node use was just observed")
                        .0
                } else {
                    let (node, use_count) = loaded
                        .get_mut(&node_id)
                        .expect("loaded node use was just observed");
                    *use_count -= 1;
                    #[cfg(test)]
                    match node {
                        StoredNode::Leaf { entries, .. } => {
                            part_clone_counts[root_index] += entries.len();
                        }
                        StoredNode::Internal { children, .. } => {
                            node_summary_clone_counts[root_index] += children.len();
                        }
                    }
                    node.clone()
                };
                #[cfg(test)]
                {
                    visited_node_counts[root_index] += 1;
                }
                validate_loaded_node(
                    &node,
                    node_id,
                    expected.as_ref(),
                    &roots[root_index],
                    "directory batch",
                )?;
                match node {
                    StoredNode::Leaf { entries, .. } => {
                        for (index, entry) in entries.into_iter().enumerate() {
                            outputs[root_index].push(MutationDirectoryPartRun {
                                entry_index: base_index
                                    .checked_add(u32::try_from(index).map_err(|_| {
                                        directory_error("leaf entry index overflows")
                                    })?)
                                    .ok_or_else(|| directory_error("entry index overflows"))?,
                                selector_span: 0..0,
                                entry: runtime_entry(entry)?,
                            });
                        }
                    }
                    StoredNode::Internal { children, .. } => {
                        let mut preceding = 0u32;
                        for child in children {
                            let child_base = base_index
                                .checked_add(preceding)
                                .ok_or_else(|| directory_error("entry offset overflows"))?;
                            preceding = preceding
                                .checked_add(child.entry_count)
                                .ok_or_else(|| directory_error("entry offset overflows"))?;
                            let node_id = child.node_id;
                            next.push((node_id, child_base, Some(NodeSummary::from(child))));
                            #[cfg(test)]
                            {
                                node_summary_owner_counts[root_index] += 1;
                            }
                        }
                    }
                }
            }
            *frontier = next;
        }
    }
    for (root, runs) in roots.iter().zip(&outputs) {
        if runs.len() != root.entry_count as usize
            || runs
                .iter()
                .map(|run| run.entry.direct_row_count())
                .map(u64::from)
                .sum::<u64>()
                != root.direct_row_count
            || runs
                .iter()
                .enumerate()
                .any(|(index, run)| run.entry_index as usize != index)
        {
            return Err(directory_error("directory closure disagrees with its root"));
        }
    }
    Ok(outputs
        .into_iter()
        .enumerate()
        .map(|(root_index, runs)| {
            #[cfg(not(test))]
            let _ = root_index;
            AuthenticatedMutationPartReadPlan {
                runs,
                #[cfg(test)]
                visited_node_count: visited_node_counts[root_index],
                #[cfg(test)]
                node_summary_owner_count: node_summary_owner_counts[root_index],
                #[cfg(test)]
                node_summary_clone_count: node_summary_clone_counts[root_index],
                #[cfg(test)]
                part_clone_count: part_clone_counts[root_index],
            }
        })
        .collect())
}

/// Selects immutable mutation parts through one authenticated, level-batched
/// directory traversal.
///
/// Points and ranges are caller-canonicalized. The directory rejects any
/// unordered or duplicate point column and any unordered, overlapping, empty,
/// or open-ended non-final range. One run owns one selected part and a compact
/// selector span; keys never own directory bounds.
pub(crate) async fn load_mutation_part_read_plan(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &MutationDirectoryRoot,
    selection: MutationDirectoryReadSelection<'_>,
) -> Result<AuthenticatedMutationPartReadPlan, LixError> {
    validate_root(root)?;
    validate_selection(root, selection)?;
    let selector_count = selection.len();
    if !matches!(selection, MutationDirectoryReadSelection::All) && selector_count == 0 {
        return Ok(AuthenticatedMutationPartReadPlan {
            runs: Vec::new(),
            #[cfg(test)]
            visited_node_count: 0,
            #[cfg(test)]
            node_summary_owner_count: 0,
            #[cfg(test)]
            node_summary_clone_count: 0,
            #[cfg(test)]
            part_clone_count: 0,
        });
    }

    struct PendingNode {
        node_id: [u8; 32],
        base_index: u32,
        selector_span: Range<usize>,
        expected: Option<NodeSummary>,
    }

    let mut frontier = vec![PendingNode {
        node_id: root.root_id,
        base_index: 0,
        selector_span: 0..selector_count,
        expected: None,
    }];
    let mut runs = Vec::new();
    #[cfg(test)]
    let mut visited_node_count = 0usize;
    #[cfg(test)]
    let mut node_summary_owner_count = 0usize;

    while !frontier.is_empty() {
        let node_ids = frontier
            .iter()
            .map(|pending| pending.node_id)
            .collect::<Vec<_>>();
        let loaded = load_nodes(store, &node_ids, is_bounded(root.layout)).await?;
        let mut next = Vec::new();
        for (pending, node) in frontier.into_iter().zip(loaded) {
            #[cfg(test)]
            {
                visited_node_count += 1;
            }
            validate_loaded_node(
                &node,
                pending.node_id,
                pending.expected.as_ref(),
                root,
                "mutation read-plan",
            )?;
            match node {
                StoredNode::Leaf { entries, .. } => {
                    let mut selector_cursor = pending.selector_span.start;
                    for (index, entry) in entries.into_iter().enumerate() {
                        let entry_index = pending
                            .base_index
                            .checked_add(
                                u32::try_from(index)
                                    .map_err(|_| directory_error("leaf entry index overflows"))?,
                            )
                            .ok_or_else(|| directory_error("entry index overflows"))?;
                        let entry_end = entry_index
                            .checked_add(1)
                            .ok_or_else(|| directory_error("entry index overflows"))?;
                        let selector_span = selection_span_for_entry(
                            selection,
                            &mut selector_cursor,
                            pending.selector_span.end,
                            stored_entry_first_key(&entry),
                            stored_entry_last_key(&entry),
                            entry_index,
                            entry_end,
                            Some(stored_entry_direct_rows(&entry)),
                        )?;
                        let Some(selector_span) = selector_span else {
                            continue;
                        };
                        runs.push(MutationDirectoryPartRun {
                            entry_index,
                            selector_span,
                            entry: runtime_entry(entry)?,
                        });
                    }
                }
                StoredNode::Internal { children, .. } => {
                    let mut preceding = 0u32;
                    let mut selector_cursor = pending.selector_span.start;
                    for child in children {
                        let child_base = pending
                            .base_index
                            .checked_add(preceding)
                            .ok_or_else(|| directory_error("entry offset overflows"))?;
                        preceding = preceding
                            .checked_add(child.entry_count)
                            .ok_or_else(|| directory_error("entry offset overflows"))?;
                        let child_end = child_base
                            .checked_add(child.entry_count)
                            .ok_or_else(|| directory_error("entry offset overflows"))?;
                        let selector_span = selection_span_for_entry(
                            selection,
                            &mut selector_cursor,
                            pending.selector_span.end,
                            &child.first_key,
                            &child.last_key,
                            child_base,
                            child_end,
                            None,
                        )?;
                        let Some(selector_span) = selector_span else {
                            continue;
                        };
                        let node_id = child.node_id;
                        next.push(PendingNode {
                            node_id,
                            base_index: child_base,
                            selector_span,
                            expected: Some(NodeSummary::from(child)),
                        });
                        #[cfg(test)]
                        {
                            node_summary_owner_count += 1;
                        }
                    }
                }
            }
        }
        frontier = next;
    }
    if runs.windows(2).any(|pair| {
        pair[0].entry_index >= pair[1].entry_index
            || pair[0].selector_span.start > pair[1].selector_span.start
    }) {
        return Err(directory_error(
            "mutation read-plan output is not canonical",
        ));
    }
    if matches!(
        selection,
        MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(_)
    ) && (runs.first().is_none_or(|run| run.selector_span.start != 0)
        || runs
            .windows(2)
            .any(|pair| pair[0].selector_span.end != pair[1].selector_span.start)
        || runs
            .last()
            .is_none_or(|run| run.selector_span.end != selector_count))
    {
        return Err(directory_error(
            "direct-coordinate read-plan output does not cover every selector",
        ));
    }
    Ok(AuthenticatedMutationPartReadPlan {
        runs,
        #[cfg(test)]
        visited_node_count,
        #[cfg(test)]
        node_summary_owner_count,
        #[cfg(test)]
        node_summary_clone_count: 0,
        #[cfg(test)]
        part_clone_count: 0,
    })
}

impl MutationDirectoryReadSelection<'_> {
    fn len(self) -> usize {
        match self {
            Self::All => 0,
            Self::SortedRanges(ranges) => ranges.len(),
            Self::SortedUniquePoints(points) => points.len(),
            Self::SortedUniqueDirectCoordinates(coordinates) => coordinates.len(),
        }
    }
}

fn validate_selection(
    root: &MutationDirectoryRoot,
    selection: MutationDirectoryReadSelection<'_>,
) -> Result<(), LixError> {
    match selection {
        MutationDirectoryReadSelection::All => Ok(()),
        MutationDirectoryReadSelection::SortedUniquePoints(points) => {
            if !is_bounded(root.layout) {
                return Err(directory_error(
                    "point selection requires a bounded directory",
                ));
            }
            if points.iter().any(Bytes::is_empty)
                || points.windows(2).any(|pair| pair[0] >= pair[1])
            {
                return Err(directory_error(
                    "point selection must be strictly sorted and unique",
                ));
            }
            Ok(())
        }
        MutationDirectoryReadSelection::SortedRanges(ranges) => {
            if !is_bounded(root.layout) {
                return Err(directory_error(
                    "range selection requires a bounded directory",
                ));
            }
            for (index, range) in ranges.iter().enumerate() {
                if range.start.is_empty()
                    || range
                        .end
                        .as_ref()
                        .is_some_and(|end| end.as_ref() <= range.start.as_ref())
                    || (range.end.is_none() && index + 1 != ranges.len())
                {
                    return Err(directory_error(
                        "range selection contains an empty or non-final open range",
                    ));
                }
            }
            if ranges.windows(2).any(|pair| {
                pair[0]
                    .end
                    .as_ref()
                    .is_none_or(|end| end.as_ref() >= pair[1].start.as_ref())
            }) {
                return Err(directory_error(
                    "range selection must be sorted, disjoint, and canonical",
                ));
            }
            Ok(())
        }
        MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(coordinates) => {
            if coordinates.is_empty() {
                return Ok(());
            }
            if root.direct_row_count == 0 {
                return Err(directory_error(
                    "direct-coordinate selection requires direct-row authority",
                ));
            }
            if coordinates.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(directory_error(
                    "direct-coordinate selection must be strictly sorted and unique",
                ));
            }
            if coordinates
                .last()
                .is_some_and(|coordinate| coordinate.part_index >= root.entry_count)
            {
                return Err(directory_error(
                    "direct-coordinate selection exceeds part authority",
                ));
            }
            Ok(())
        }
    }
}

fn selection_span_for_entry(
    selection: MutationDirectoryReadSelection<'_>,
    cursor: &mut usize,
    selector_end: usize,
    first_key: &[u8],
    last_key: &[u8],
    entry_index: u32,
    entry_end: u32,
    direct_row_count: Option<u16>,
) -> Result<Option<Range<usize>>, LixError> {
    match selection {
        MutationDirectoryReadSelection::All => Ok(Some(0..0)),
        MutationDirectoryReadSelection::SortedUniquePoints(points) => {
            while *cursor < selector_end && points[*cursor].as_ref() < first_key {
                *cursor += 1;
            }
            let start = *cursor;
            while *cursor < selector_end && points[*cursor].as_ref() <= last_key {
                *cursor += 1;
            }
            Ok((start < *cursor).then_some(start..*cursor))
        }
        MutationDirectoryReadSelection::SortedRanges(ranges) => {
            while *cursor < selector_end
                && ranges[*cursor]
                    .end
                    .as_ref()
                    .is_some_and(|end| end.as_ref() <= first_key)
            {
                *cursor += 1;
            }
            let start = *cursor;
            let mut end = start;
            while end < selector_end && ranges[end].start.as_ref() <= last_key {
                end += 1;
            }
            Ok((start < end).then_some(start..end))
        }
        MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(coordinates) => {
            while *cursor < selector_end && coordinates[*cursor].part_index < entry_index {
                *cursor += 1;
            }
            let start = *cursor;
            while *cursor < selector_end && coordinates[*cursor].part_index < entry_end {
                *cursor += 1;
            }
            if let Some(direct_row_count) = direct_row_count
                && coordinates[start..*cursor]
                    .iter()
                    .any(|coordinate| coordinate.local_row >= direct_row_count)
            {
                return Err(directory_error(
                    "direct coordinate exceeds authenticated part row count",
                ));
            }
            Ok((start < *cursor).then_some(start..*cursor))
        }
    }
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
        let loaded = load_nodes(store, &node_ids, is_bounded(root.layout)).await?;
        let mut next = Vec::new();
        for ((node_id, expected), node) in frontier.into_iter().zip(loaded) {
            validate_loaded_node(&node, node_id, expected.as_ref(), root, "reachability")?;
            if !reachable.insert(node_id) {
                continue;
            }
            if let StoredNode::Internal { children, .. } = node {
                next.extend(children.into_iter().map(|child| {
                    let node_id = child.node_id;
                    (node_id, Some(NodeSummary::from(child)))
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
    unique: bool,
) -> Result<Vec<StoredNode>, LixError> {
    if node_ids.is_empty() {
        return Ok(Vec::new());
    }
    let keys = node_ids
        .iter()
        .map(|node_id| StorageKey(Bytes::copy_from_slice(node_id)))
        .collect::<Vec<_>>();
    let plan = if unique {
        PointReadPlan::from_unique_keys(MUTATION_DIRECTORY_NODE_SPACE, keys)
    } else {
        PointReadPlan::new(MUTATION_DIRECTORY_NODE_SPACE, &keys)
    };
    let values = plan
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
            decode_node(&bytes)
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

#[derive(Clone, Copy)]
struct NodeSummaryRef<'a> {
    first_key: &'a [u8],
    last_key: &'a [u8],
    node_id: [u8; 32],
    entry_count: u32,
    direct_row_count: u64,
    level: u16,
    layout: u8,
}

fn node_summary_ref(node: &StoredNode, node_id: [u8; 32]) -> Result<NodeSummaryRef<'_>, LixError> {
    match node {
        StoredNode::Leaf { layout, entries } => {
            let entry_count = u32::try_from(entries.len())
                .map_err(|_| directory_error("entry count overflows"))?;
            let direct_row_count = entries.iter().try_fold(0u64, |sum, entry| {
                sum.checked_add(u64::from(stored_entry_direct_rows(entry)))
                    .ok_or_else(|| directory_error("row count overflows"))
            })?;
            Ok(NodeSummaryRef {
                first_key: stored_entry_first_key(&entries[0]),
                last_key: stored_entry_last_key(&entries[entries.len() - 1]),
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
            Ok(NodeSummaryRef {
                first_key: &children[0].first_key,
                last_key: &children[children.len() - 1].last_key,
                node_id,
                entry_count,
                direct_row_count,
                level: *level,
                layout: *layout,
            })
        }
    }
}

fn summary_ref_matches_owned(actual: NodeSummaryRef<'_>, expected: &NodeSummary) -> bool {
    actual.first_key == expected.first_key
        && actual.last_key == expected.last_key
        && actual.node_id == expected.node_id
        && actual.entry_count == expected.entry_count
        && actual.direct_row_count == expected.direct_row_count
        && actual.level == expected.level
        && actual.layout == expected.layout
}

fn summary_ref_matches_root(actual: NodeSummaryRef<'_>, root: &MutationDirectoryRoot) -> bool {
    actual.node_id == root.root_id
        && actual.entry_count == root.entry_count
        && actual.direct_row_count == root.direct_row_count
        && actual.level.checked_add(1) == Some(root.tree_height)
        && actual.layout == root.layout
}

fn validate_loaded_node(
    node: &StoredNode,
    node_id: [u8; 32],
    expected: Option<&NodeSummary>,
    root: &MutationDirectoryRoot,
    operation: &str,
) -> Result<(), LixError> {
    let actual = node_summary_ref(node, node_id)?;
    match expected {
        Some(expected) if !summary_ref_matches_owned(actual, expected) => Err(directory_error(
            format!("{operation} child summary mismatch"),
        )),
        None if !summary_ref_matches_root(actual, root) => Err(directory_error(format!(
            "{operation} root summary mismatch"
        ))),
        _ => Ok(()),
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

#[cfg(test)]
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

#[cfg(test)]
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

    async fn stored_directory(
        built: &BuiltMutationDirectory,
    ) -> (StorageAdapter<Memory>, impl StorageAdapterRead) {
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
        (storage, read)
    }

    #[tokio::test]
    async fn multi_level_directory_emits_distinct_runs_without_per_key_owners() {
        let entries = (0..(FANOUT as u32 * 2 + 17))
            .map(bounded_entry)
            .collect::<Vec<_>>();
        let built = build_mutation_directory(LAYOUT_BOUNDED_DIRECT, &entries).unwrap();
        assert!(built.root.tree_height >= 2);
        assert!(built.node_bytes().len() > 1);
        let (_storage, read) = stored_directory(&built).await;

        let all =
            load_mutation_part_read_plan(&read, &built.root, MutationDirectoryReadSelection::All)
                .await
                .unwrap();
        assert_eq!(all.len(), entries.len());
        assert_eq!(all.visited_node_count(), built.node_bytes().len());
        assert_eq!(all.node_summary_owner_count() + 1, all.visited_node_count());
        assert_eq!(all.node_summary_clone_count(), 0);
        assert_eq!(all.part_clone_count(), 0);
        assert_eq!(
            all.into_runs()
                .into_iter()
                .map(|run| run.entry)
                .collect::<Vec<_>>(),
            entries
        );

        let requested = [0u32, FANOUT as u32, FANOUT as u32 * 2 + 16];
        let points = requested
            .iter()
            .map(|index| {
                Bytes::copy_from_slice(&index.saturating_mul(10).saturating_add(4).to_be_bytes())
            })
            .collect::<Vec<_>>();
        let point_plan = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniquePoints(&points),
        )
        .await
        .unwrap();
        assert_eq!(point_plan.len(), requested.len());
        assert_eq!(
            point_plan.node_summary_owner_count() + 1,
            point_plan.visited_node_count()
        );
        assert!(
            point_plan.visited_node_count()
                <= 1 + requested.len() * built.root.tree_height as usize
        );
        assert_eq!(point_plan.node_summary_clone_count(), 0);
        assert_eq!(point_plan.part_clone_count(), 0);
        for ((&expected_index, expected_selector), run) in
            requested.iter().zip(0usize..).zip(point_plan.into_runs())
        {
            assert_eq!(run.entry_index, expected_index);
            assert_eq!(run.selector_span, expected_selector..expected_selector + 1);
            assert_eq!(run.entry.direct_row_count(), 7);
            assert_eq!(
                run.entry,
                match &entries[expected_index as usize] {
                    entry @ MutationDirectoryEntry::Bounded { .. } => entry.clone(),
                    _ => unreachable!(),
                }
            );
        }

        let clustered_points = [1u32, 4, 8]
            .into_iter()
            .map(|key| Bytes::copy_from_slice(&key.to_be_bytes()))
            .collect::<Vec<_>>();
        let clustered = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniquePoints(&clustered_points),
        )
        .await
        .unwrap()
        .into_runs();
        assert_eq!(clustered.len(), 1, "one touched part must produce one run");
        assert_eq!(clustered[0].entry_index, 0);
        assert_eq!(clustered[0].selector_span, 0..clustered_points.len());

        let coordinates = [
            MutationDirectoryDirectCoordinate {
                part_index: 0,
                local_row: 0,
            },
            MutationDirectoryDirectCoordinate {
                part_index: FANOUT as u32,
                local_row: 3,
            },
            MutationDirectoryDirectCoordinate {
                part_index: u32::try_from(entries.len() - 1).unwrap(),
                local_row: 6,
            },
        ];
        let coordinate_plan = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&coordinates),
        )
        .await
        .unwrap();
        assert_eq!(coordinate_plan.node_summary_clone_count(), 0);
        assert_eq!(coordinate_plan.part_clone_count(), 0);
        assert!(
            coordinate_plan.visited_node_count()
                <= 1 + coordinates.len() * built.root.tree_height as usize
        );
        let coordinate_runs = coordinate_plan.into_runs();
        assert_eq!(coordinate_runs.len(), coordinates.len());
        assert_eq!(coordinate_runs[0].entry_index, 0);
        assert_eq!(coordinate_runs[0].selector_span, 0..1);
        assert_eq!(coordinate_runs[1].entry_index, FANOUT as u32);
        assert_eq!(coordinate_runs[1].selector_span, 1..2);
        assert_eq!(
            coordinate_runs[2].entry_index,
            u32::try_from(entries.len() - 1).unwrap()
        );
        assert_eq!(coordinate_runs[2].selector_span, 2..3);

        let clustered_coordinates = [
            MutationDirectoryDirectCoordinate {
                part_index: 0,
                local_row: 0,
            },
            MutationDirectoryDirectCoordinate {
                part_index: 0,
                local_row: 1,
            },
            MutationDirectoryDirectCoordinate {
                part_index: 0,
                local_row: 6,
            },
        ];
        let clustered_coordinate_runs = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&clustered_coordinates),
        )
        .await
        .unwrap()
        .into_runs();
        assert_eq!(clustered_coordinate_runs.len(), 1);
        assert_eq!(clustered_coordinate_runs[0].entry_index, 0);
        assert_eq!(clustered_coordinate_runs[0].selector_span, 0..3);

        let ranges = points
            .iter()
            .map(|point| {
                let mut end = point.to_vec();
                *end.last_mut().unwrap() += 1;
                MutationDirectoryKeyRange {
                    start: point.clone(),
                    end: Some(Bytes::from(end)),
                }
            })
            .collect::<Vec<_>>();
        let range_runs = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedRanges(&ranges),
        )
        .await
        .unwrap()
        .into_runs();
        assert_eq!(
            range_runs
                .iter()
                .map(|run| (run.entry_index, run.selector_span.clone()))
                .collect::<Vec<_>>(),
            requested
                .iter()
                .copied()
                .zip((0usize..requested.len()).map(|index| index..index + 1))
                .collect::<Vec<_>>()
        );

        let wide_range = [MutationDirectoryKeyRange {
            start: Bytes::copy_from_slice(&4u32.to_be_bytes()),
            end: Some(Bytes::copy_from_slice(&25u32.to_be_bytes())),
        }];
        let wide_runs = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedRanges(&wide_range),
        )
        .await
        .unwrap()
        .into_runs();
        assert_eq!(
            wide_runs
                .iter()
                .map(|run| (run.entry_index, run.selector_span.clone()))
                .collect::<Vec<_>>(),
            vec![(0, 0..1), (1, 0..1), (2, 0..1)]
        );

        let batched =
            load_all_mutation_part_read_plans(&read, &[built.root.clone(), built.root.clone()])
                .await
                .unwrap();
        assert_eq!(batched.len(), 2);
        assert!(batched.iter().all(|plan| plan.len() == entries.len()));
        assert!(batched.iter().all(|plan| {
            plan.node_summary_clone_count() <= plan.node_summary_owner_count()
                && plan.part_clone_count() <= plan.len()
        }));
        assert_eq!(
            collect_mutation_directory_node_ids(&read, &built.root)
                .await
                .unwrap()
                .len(),
            built.node_bytes().len()
        );
    }

    #[tokio::test]
    async fn direct_coordinates_preserve_physical_holes_across_three_tree_levels() {
        let irregular = [2u16, 7, 1]
            .into_iter()
            .map(|direct_row_count| MutationDirectoryEntry::DirectAddress { direct_row_count })
            .collect::<Vec<_>>();
        let built = build_mutation_directory(LAYOUT_DIRECT_ROWS_ONLY, &irregular).unwrap();
        let (_storage, read) = stored_directory(&built).await;
        let coordinates = [
            MutationDirectoryDirectCoordinate {
                part_index: 0,
                local_row: 1,
            },
            MutationDirectoryDirectCoordinate {
                part_index: 1,
                local_row: 6,
            },
            MutationDirectoryDirectCoordinate {
                part_index: 2,
                local_row: 0,
            },
        ];
        let runs = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&coordinates),
        )
        .await
        .unwrap()
        .into_runs();
        assert_eq!(
            runs.iter().map(|run| run.entry_index).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        let error = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&[
                MutationDirectoryDirectCoordinate {
                    part_index: 0,
                    local_row: 2,
                },
            ]),
        )
        .await
        .expect_err("a physical hole must not become a dense ordinal");
        assert!(error.to_string().contains("part row count"));

        let entries = (0..(FANOUT * FANOUT + 1))
            .map(|_| MutationDirectoryEntry::DirectAddress {
                direct_row_count: 1,
            })
            .collect::<Vec<_>>();
        let built = build_mutation_directory(LAYOUT_DIRECT_ROWS_ONLY, &entries).unwrap();
        assert!(built.root.tree_height >= 3);
        let (_storage, read) = stored_directory(&built).await;
        let coordinates = [
            0u32,
            FANOUT as u32 - 1,
            FANOUT as u32,
            u32::try_from(FANOUT * FANOUT - 1).unwrap(),
            u32::try_from(FANOUT * FANOUT).unwrap(),
        ]
        .map(|part_index| MutationDirectoryDirectCoordinate {
            part_index,
            local_row: 0,
        });
        let plan = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&coordinates),
        )
        .await
        .unwrap();
        assert_eq!(plan.len(), coordinates.len());
        assert_eq!(plan.node_summary_clone_count(), 0);
        assert_eq!(plan.part_clone_count(), 0);
        assert!(
            plan.visited_node_count() <= 1 + coordinates.len() * built.root.tree_height as usize
        );
        assert_eq!(
            plan.into_runs()
                .into_iter()
                .map(|run| run.entry_index)
                .collect::<Vec<_>>(),
            coordinates
                .iter()
                .map(|coordinate| coordinate.part_index)
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn read_plan_rejects_noncanonical_selectors_and_handles_empty_inputs() {
        let entries = (0..4).map(bounded_entry).collect::<Vec<_>>();
        let built = build_mutation_directory(LAYOUT_BOUNDED_DIRECT, &entries).unwrap();
        let (_storage, read) = stored_directory(&built).await;
        let point = Bytes::copy_from_slice(&4u32.to_be_bytes());
        let next = Bytes::copy_from_slice(&14u32.to_be_bytes());

        for points in [
            vec![point.clone(), point.clone()],
            vec![next.clone(), point.clone()],
        ] {
            let error = load_mutation_part_read_plan(
                &read,
                &built.root,
                MutationDirectoryReadSelection::SortedUniquePoints(&points),
            )
            .await
            .expect_err("noncanonical points must fail");
            assert!(error.to_string().contains("strictly sorted and unique"));
        }

        let touching = vec![
            MutationDirectoryKeyRange {
                start: point.clone(),
                end: Some(next.clone()),
            },
            MutationDirectoryKeyRange {
                start: next.clone(),
                end: None,
            },
        ];
        let error = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedRanges(&touching),
        )
        .await
        .expect_err("touching ranges must be canonicalized upstream");
        assert!(
            error
                .to_string()
                .contains("sorted, disjoint, and canonical")
        );

        let empty_points = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniquePoints(&[]),
        )
        .await
        .unwrap();
        assert!(empty_points.is_empty());
        assert_eq!(empty_points.visited_node_count(), 0);
        let empty_ranges = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedRanges(&[]),
        )
        .await
        .unwrap();
        assert!(empty_ranges.is_empty());
        assert_eq!(empty_ranges.visited_node_count(), 0);

        for coordinates in [
            vec![
                MutationDirectoryDirectCoordinate {
                    part_index: 0,
                    local_row: 0,
                },
                MutationDirectoryDirectCoordinate {
                    part_index: 0,
                    local_row: 0,
                },
            ],
            vec![
                MutationDirectoryDirectCoordinate {
                    part_index: 1,
                    local_row: 0,
                },
                MutationDirectoryDirectCoordinate {
                    part_index: 0,
                    local_row: 1,
                },
            ],
        ] {
            let error = load_mutation_part_read_plan(
                &read,
                &built.root,
                MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&coordinates),
            )
            .await
            .expect_err("noncanonical direct coordinates must fail");
            assert!(error.to_string().contains("strictly sorted and unique"));
        }
        let error = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&[
                MutationDirectoryDirectCoordinate {
                    part_index: built.root.entry_count,
                    local_row: 0,
                },
            ]),
        )
        .await
        .expect_err("out-of-range part coordinates must fail");
        assert!(error.to_string().contains("exceeds part authority"));
        let error = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&[
                MutationDirectoryDirectCoordinate {
                    part_index: 0,
                    local_row: 7,
                },
            ]),
        )
        .await
        .expect_err("out-of-range local rows must fail");
        assert!(error.to_string().contains("part row count"));
        let empty_coordinates = load_mutation_part_read_plan(
            &read,
            &built.root,
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&[]),
        )
        .await
        .unwrap();
        assert!(empty_coordinates.is_empty());
        assert_eq!(empty_coordinates.visited_node_count(), 0);
    }

    #[tokio::test]
    async fn every_read_plan_mode_fails_closed_on_authenticated_bound_mismatch() {
        let entries = (0..(FANOUT as u32 + 1))
            .map(bounded_entry)
            .collect::<Vec<_>>();
        let built = build_mutation_directory(LAYOUT_BOUNDED_DIRECT, &entries).unwrap();
        let root_bytes = built.node_bytes()[&built.root.root_id].clone();
        let mut root_node = decode_node(&root_bytes).unwrap();
        let StoredNode::Internal { children, .. } = &mut root_node else {
            panic!("fixture must have an internal root");
        };
        *children[0].last_key.last_mut().unwrap() -= 1;
        let tampered_bytes = encode_node(&root_node).unwrap();
        let tampered_id = node_digest(&tampered_bytes);
        let mut tampered_root = built.root.clone();
        tampered_root.root_id = tampered_id;
        tampered_root.root_digest = root_digest(
            tampered_id,
            tampered_root.entry_count,
            tampered_root.direct_row_count,
            tampered_root.tree_height,
            tampered_root.layout,
        );

        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        for (node_id, bytes) in built.node_bytes() {
            writes.put(
                MUTATION_DIRECTORY_NODE_SPACE,
                StorageKey(Bytes::copy_from_slice(node_id)),
                StorageValue {
                    bytes: bytes.clone(),
                },
            );
        }
        writes.put(
            MUTATION_DIRECTORY_NODE_SPACE,
            StorageKey(Bytes::copy_from_slice(&tampered_id)),
            StorageValue {
                bytes: tampered_bytes,
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
        let point = Bytes::copy_from_slice(&4u32.to_be_bytes());
        let ranges = [MutationDirectoryKeyRange {
            start: point.clone(),
            end: Some(Bytes::copy_from_slice(&5u32.to_be_bytes())),
        }];
        let coordinate = [MutationDirectoryDirectCoordinate {
            part_index: 0,
            local_row: 0,
        }];
        for selection in [
            MutationDirectoryReadSelection::All,
            MutationDirectoryReadSelection::SortedRanges(&ranges),
            MutationDirectoryReadSelection::SortedUniquePoints(std::slice::from_ref(&point)),
            MutationDirectoryReadSelection::SortedUniqueDirectCoordinates(&coordinate),
        ] {
            let error = load_mutation_part_read_plan(&read, &tampered_root, selection)
                .await
                .expect_err("every selection must authenticate visited child bounds");
            assert!(error.to_string().contains("child summary mismatch"));
        }

        let mut bad_count = built.root.clone();
        bad_count.entry_count += 1;
        bad_count.root_digest = root_digest(
            bad_count.root_id,
            bad_count.entry_count,
            bad_count.direct_row_count,
            bad_count.tree_height,
            bad_count.layout,
        );
        let error =
            load_mutation_part_read_plan(&read, &bad_count, MutationDirectoryReadSelection::All)
                .await
                .expect_err("root counts must agree with authenticated nodes");
        assert!(error.to_string().contains("root summary mismatch"));
    }

    #[tokio::test]
    async fn read_plan_rejects_missing_nodes_and_content_digest_mismatch() {
        let entries = (0..2).map(bounded_entry).collect::<Vec<_>>();
        let built = build_mutation_directory(LAYOUT_BOUNDED_DIRECT, &entries).unwrap();

        let missing_storage = StorageAdapter::new(Memory::new());
        let missing_read = missing_storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let error = load_mutation_part_read_plan(
            &missing_read,
            &built.root,
            MutationDirectoryReadSelection::All,
        )
        .await
        .expect_err("a missing authenticated node must fail");
        assert!(error.to_string().contains("missing node"));

        let digest_storage = StorageAdapter::new(Memory::new());
        let mut corrupt = built.node_bytes()[&built.root.root_id].to_vec();
        *corrupt.last_mut().unwrap() ^= 1;
        let mut writes = digest_storage.new_write_set();
        writes.put(
            MUTATION_DIRECTORY_NODE_SPACE,
            StorageKey(Bytes::copy_from_slice(&built.root.root_id)),
            StorageValue {
                bytes: Bytes::from(corrupt),
            },
        );
        digest_storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let digest_read = digest_storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let error = load_mutation_part_read_plan(
            &digest_read,
            &built.root,
            MutationDirectoryReadSelection::All,
        )
        .await
        .expect_err("content bytes must match their immutable node id");
        assert!(error.to_string().contains("content digest mismatch"));
    }
}
