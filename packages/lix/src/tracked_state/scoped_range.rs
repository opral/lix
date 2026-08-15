//! Unified immutable range index for scoped current-state parts.
//!
//! A scope marker and all of its ordered parts occupy one key space. This is
//! deliberately independent of mutation-history payloads: part bytes are an
//! opaque, versioned serving contract and the tree only authenticates their
//! scope, bounds, row counts, and content.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::mem::size_of;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;

use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StorageProjectedValue,
    StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet, ValueSemantics,
};
use crate::{LixError, storage_codec};

/// Hard-cut namespace: nodes from the former per-scope directory/catalog
/// formats are neither readable nor hash-compatible with this tree.
pub(crate) const SCOPED_RANGE_NODE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0032),
    "tracked_state.scoped_range.v3",
    ValueSemantics::Immutable,
);

const NODE_RAW_MAGIC: &[u8; 6] = b"LXSR3R";
const NODE_ZSTD_MAGIC: &[u8; 6] = b"LXSR3Z";
const NODE_HASH_CONTEXT: &str = "lix scoped current-state range node v3";
const ROOT_HASH_CONTEXT: &str = "lix scoped current-state range root v3";
const MAX_NODE_DECODED_BYTES: usize = 16 * 1024 * 1024;
const FANOUT: usize = 128;
const MAX_SCOPE_COMPONENT_BYTES: usize = 64 * 1024;
const MAX_SCOPE_PREFIX_BYTES: usize = 256 * 1024;

/// Canonical length-framed tuple used as the leading component of every
/// route. Callers cannot construct non-canonical prefixes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ScopedRangePrefix {
    encoded: Arc<[u8]>,
}

impl ScopedRangePrefix {
    pub(crate) fn try_from_components<'a>(
        components: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<Self, LixError> {
        let components = components.into_iter().collect::<Vec<_>>();
        if components.is_empty() {
            return Err(scoped_range_error("scope has no components"));
        }
        let mut encoded = Vec::new();
        encoded.extend_from_slice(
            &u32::try_from(components.len())
                .map_err(|_| scoped_range_error("scope has too many components"))?
                .to_be_bytes(),
        );
        for component in components {
            if component.len() > MAX_SCOPE_COMPONENT_BYTES {
                return Err(scoped_range_error("scope component exceeds its size bound"));
            }
            encoded.extend_from_slice(
                &u32::try_from(component.len())
                    .expect("bounded scope component fits u32")
                    .to_be_bytes(),
            );
            encoded.extend_from_slice(component);
        }
        if encoded.len() > MAX_SCOPE_PREFIX_BYTES {
            return Err(scoped_range_error("scope prefix exceeds its size bound"));
        }
        Ok(Self {
            encoded: Arc::from(encoded),
        })
    }

    fn validate(&self) -> Result<(), LixError> {
        let mut remaining = self.encoded.as_ref();
        let count = take_u32(&mut remaining, "scope omitted its component count")? as usize;
        if count == 0 {
            return Err(scoped_range_error("scope has no components"));
        }
        for _ in 0..count {
            let length = take_u32(&mut remaining, "scope omitted a component length")? as usize;
            if length > MAX_SCOPE_COMPONENT_BYTES || remaining.len() < length {
                return Err(scoped_range_error(
                    "scope component is truncated or oversized",
                ));
            }
            remaining = &remaining[length..];
        }
        if !remaining.is_empty() || self.encoded.len() > MAX_SCOPE_PREFIX_BYTES {
            return Err(scoped_range_error("scope prefix is not canonical"));
        }
        Ok(())
    }
}

/// Opaque payload owned by the physical part codec. The range tree never
/// interprets `bytes`; the explicit version makes codec replacement a hard
/// protocol boundary rather than a workload flag.
#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct ScopedRangePartPayload {
    pub(crate) version: u16,
    #[musli(bytes)]
    pub(crate) bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopedRangeCoverageMarker {
    pub(crate) scope: ScopedRangePrefix,
    pub(crate) row_count: u64,
    pub(crate) part_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopedRangePart {
    pub(crate) scope: ScopedRangePrefix,
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
    pub(crate) row_count: u64,
    pub(crate) payload: ScopedRangePartPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ScopedRangeEntry {
    Marker(ScopedRangeCoverageMarker),
    Part(ScopedRangePart),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ScopedRangeRoute {
    scope: ScopedRangePrefix,
    kind: u8,
    key: Vec<u8>,
}

impl ScopedRangeEntry {
    fn route(&self) -> ScopedRangeRoute {
        match self {
            Self::Marker(marker) => marker_route(&marker.scope),
            Self::Part(part) => part_route(&part.scope, &part.first_key),
        }
    }

    fn scope(&self) -> &ScopedRangePrefix {
        match self {
            Self::Marker(marker) => &marker.scope,
            Self::Part(part) => &part.scope,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopedRangeChild {
    first: ScopedRangeRoute,
    last: ScopedRangeRoute,
    node_id: [u8; 32],
    marker_count: u32,
    part_count: u32,
    row_count: u64,
    level: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopedRangeNode {
    level: u16,
    entries: Vec<ScopedRangeEntry>,
    children: Vec<ScopedRangeChild>,
}

/// Storage-only leaf representation. One scope prefix is encoded per
/// contiguous run instead of once per marker and part. Runtime entries stay
/// unchanged, so routing and payload ownership remain independent of this
/// physical codec.
#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredScopedRangeLeafRun {
    #[musli(bytes)]
    scope: Vec<u8>,
    #[musli(with = storage_codec::option)]
    marker: Option<StoredScopedRangeMarker>,
    parts: Vec<StoredScopedRangePart>,
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredScopedRangeMarker {
    row_count: u64,
    part_count: u32,
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredScopedRangePart {
    #[musli(bytes)]
    first_key: Vec<u8>,
    #[musli(bytes)]
    last_key: Vec<u8>,
    row_count: u64,
    payload: ScopedRangePartPayload,
}

#[derive(musli::Encode, musli::Decode)]
enum StoredScopedRangeNode {
    Leaf {
        runs: Vec<StoredScopedRangeLeafRun>,
    },
    Internal {
        level: u16,
        children: Vec<StoredScopedRangeChild>,
    },
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredScopedRangeRoute {
    #[musli(bytes)]
    scope: Vec<u8>,
    kind: u8,
    #[musli(bytes)]
    key: Vec<u8>,
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct StoredScopedRangeChild {
    first: StoredScopedRangeRoute,
    last: StoredScopedRangeRoute,
    node_id: [u8; 32],
    marker_count: u32,
    part_count: u32,
    row_count: u64,
    level: u16,
}

/// Encode-only view of the v3 node protocol. Staging borrows immutable route,
/// key, and payload bytes instead of cloning every field into the owned decode
/// representation before Musli immediately copies them into its output.
#[derive(musli::Encode)]
#[musli(packed)]
struct StoredScopedRangeLeafRunRef<'a> {
    #[musli(bytes)]
    scope: &'a [u8],
    #[musli(with = storage_codec::option)]
    marker: Option<StoredScopedRangeMarker>,
    parts: Vec<StoredScopedRangePartRef<'a>>,
}

#[derive(musli::Encode)]
#[musli(packed)]
struct StoredScopedRangePartRef<'a> {
    #[musli(bytes)]
    first_key: &'a [u8],
    #[musli(bytes)]
    last_key: &'a [u8],
    row_count: u64,
    payload: &'a ScopedRangePartPayload,
}

#[derive(musli::Encode)]
enum StoredScopedRangeNodeRef<'a> {
    Leaf {
        runs: Vec<StoredScopedRangeLeafRunRef<'a>>,
    },
    Internal {
        level: u16,
        children: Vec<StoredScopedRangeChildRef<'a>>,
    },
}

#[derive(musli::Encode)]
#[musli(packed)]
struct StoredScopedRangeRouteRef<'a> {
    #[musli(bytes)]
    scope: &'a [u8],
    kind: u8,
    #[musli(bytes)]
    key: &'a [u8],
}

#[derive(musli::Encode)]
#[musli(packed)]
struct StoredScopedRangeChildRef<'a> {
    first: StoredScopedRangeRouteRef<'a>,
    last: StoredScopedRangeRouteRef<'a>,
    node_id: [u8; 32],
    marker_count: u32,
    part_count: u32,
    row_count: u64,
    level: u16,
}

#[derive(Clone)]
struct LoadedScopedRangeNode {
    node: ScopedRangeNode,
    summary: ScopedRangeChild,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct ScopedRangeRoot {
    pub(crate) root_id: [u8; 32],
    pub(crate) root_digest: [u8; 32],
    pub(crate) marker_count: u32,
    pub(crate) part_count: u32,
    pub(crate) row_count: u64,
    pub(crate) tree_height: u16,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopedRangePointRoute {
    pub(crate) coverage: Option<ScopedRangeCoverageMarker>,
    /// Greatest part start not greater than the requested key. It is present
    /// even when the key lies in a gap; `covered_part` performs the bound test.
    pub(crate) predecessor: Option<ScopedRangePart>,
    pub(crate) covered_part: Option<ScopedRangePart>,
}

/// Minimal point-serving result. A same-scope predecessor proves that the
/// explicit coverage marker exists earlier in the authenticated route space;
/// callers that need its totals use `route_scoped_range_points` instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopedRangeCoveredPointRoute {
    pub(crate) scope_covered: bool,
    pub(crate) covered_part: Option<ScopedRangePart>,
}

#[cfg(any(test, feature = "storage-benches"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopedRangeInterval {
    pub(crate) coverage: Option<ScopedRangeCoverageMarker>,
    pub(crate) parts: Vec<ScopedRangePart>,
}

/// A physical equality proof for one completely covered scope. `NotProven`
/// deliberately does not mean that the logical states differ: a missing
/// marker or different immutable partitioning must fall back to semantic diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScopedRangeScopeEqualityProof {
    Equal,
    NotProven,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopedRangeNodeRef {
    first: ScopedRangeRoute,
    last: ScopedRangeRoute,
    node_id: [u8; 32],
    marker_count: u32,
    part_count: u32,
    row_count: u64,
    level: u16,
}

impl From<ScopedRangeChild> for ScopedRangeNodeRef {
    fn from(child: ScopedRangeChild) -> Self {
        Self {
            first: child.first,
            last: child.last,
            node_id: child.node_id,
            marker_count: child.marker_count,
            part_count: child.part_count,
            row_count: child.row_count,
            level: child.level,
        }
    }
}

enum ScopedRangeEqualityWork {
    Compare(ScopedRangeNodeRef, ScopedRangeNodeRef),
    CollectLeft(ScopedRangeNodeRef),
    CollectRight(ScopedRangeNodeRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopedRangeReachability {
    pub(crate) node_ids: BTreeSet<[u8; 32]>,
    pub(crate) markers: Vec<ScopedRangeCoverageMarker>,
    pub(crate) parts: Vec<ScopedRangePart>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ScopedRangeRewriteStats {
    pub(crate) loaded_nodes: u32,
    pub(crate) staged_nodes: u32,
    pub(crate) reused_children: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopedRangeRewrite {
    pub(crate) root: ScopedRangeRoot,
    pub(crate) stats: ScopedRangeRewriteStats,
}

#[derive(Debug, Clone)]
pub(crate) struct ScopedRangeSplicePlan {
    write_set_id: u64,
    root: ScopedRangeRoot,
    scope: ScopedRangePrefix,
    marker: ScopedRangeCoverageMarker,
    leaves: Vec<ScopedRangeSpliceLeaf>,
    nodes: BTreeMap<[u8; 32], ScopedRangeNode>,
}

pub(crate) struct StagedScopedRangeNodes {
    nodes: BTreeMap<[u8; 32], ScopedRangeNode>,
}

pub(crate) fn snapshot_staged_scoped_range_nodes(
    writes: &StorageWriteSet,
) -> Result<StagedScopedRangeNodes, LixError> {
    let mut nodes = BTreeMap::new();
    for (key, bytes) in writes.staged_values_in_space(SCOPED_RANGE_NODE_SPACE) {
        let node_id = <[u8; 32]>::try_from(key.as_ref())
            .map_err(|_| scoped_range_error("staged node key has the wrong length"))?;
        if node_digest(&bytes) != node_id {
            return Err(scoped_range_error("staged node content digest mismatch"));
        }
        nodes.insert(node_id, decode_node(&bytes)?);
    }
    Ok(StagedScopedRangeNodes { nodes })
}

#[derive(Debug, Clone)]
struct ScopedRangeSpliceLeaf {
    node_id: [u8; 32],
    part_range: Range<usize>,
    key_indices: Vec<usize>,
}

impl ScopedRangeSplicePlan {
    pub(crate) fn leaf_count(&self) -> usize {
        self.leaves.len()
    }

    pub(crate) fn leaf_parts(
        &self,
        index: usize,
    ) -> impl ExactSizeIterator<Item = &ScopedRangePart> {
        let leaf = &self.leaves[index];
        self.nodes[&leaf.node_id].entries[leaf.part_range.clone()]
            .iter()
            .map(|entry| match entry {
                ScopedRangeEntry::Part(part) => part,
                ScopedRangeEntry::Marker(_) => {
                    unreachable!("planned part range contains only scoped parts")
                }
            })
    }

    pub(crate) fn leaf_key_indices(&self, index: usize) -> &[usize] {
        &self.leaves[index].key_indices
    }

    pub(crate) fn coverage(&self) -> &ScopedRangeCoverageMarker {
        &self.marker
    }
}

struct StagedNode {
    child: ScopedRangeChild,
}

/// Builds a canonical balanced tree. Every scope must have exactly one marker,
/// followed by a complete ordered non-overlapping part set matching the
/// marker's row/part closure.
pub(crate) fn stage_scoped_range_tree(
    writes: &mut StorageWriteSet,
    scopes: impl IntoIterator<Item = (ScopedRangeCoverageMarker, Vec<ScopedRangePart>)>,
) -> Result<ScopedRangeRoot, LixError> {
    let mut compressor = crate::compression::ZstdLevel1Compressor::new()
        .map_err(|error| scoped_range_error(format!("node compressor setup failed: {error}")))?;
    let mut scopes = scopes.into_iter().collect::<Vec<_>>();
    scopes.sort_by(|left, right| left.0.scope.cmp(&right.0.scope));
    if scopes.is_empty() {
        return Err(scoped_range_error("tree has no coverage markers"));
    }
    if scopes
        .windows(2)
        .any(|pair| pair[0].0.scope == pair[1].0.scope)
    {
        return Err(scoped_range_error(
            "tree contains duplicate coverage markers",
        ));
    }
    let mut entries = Vec::new();
    for (marker, mut parts) in scopes {
        parts.sort_by(|left, right| left.first_key.cmp(&right.first_key));
        entries.push(ScopedRangeEntry::Marker(marker));
        entries.extend(parts.into_iter().map(ScopedRangeEntry::Part));
    }
    validate_entry_closure(&entries)?;

    let mut level = balanced_chunks(&entries)
        .into_iter()
        .map(|chunk| {
            stage_node(
                writes,
                &mut compressor,
                ScopedRangeNode {
                    level: 0,
                    entries: chunk.to_vec(),
                    children: Vec::new(),
                },
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut tree_height = 1u16;
    while level.len() > 1 {
        level = balanced_chunks(&level)
            .into_iter()
            .map(|chunk| {
                stage_node(
                    writes,
                    &mut compressor,
                    ScopedRangeNode {
                        level: chunk[0].child.level + 1,
                        entries: Vec::new(),
                        children: chunk.iter().map(|node| node.child.clone()).collect(),
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        tree_height = tree_height
            .checked_add(1)
            .ok_or_else(|| scoped_range_error("tree height overflows"))?;
    }
    let child = level.pop().expect("non-empty entries stage one root").child;
    Ok(ScopedRangeRoot {
        root_id: child.node_id,
        root_digest: root_digest(
            child.node_id,
            child.marker_count,
            child.part_count,
            child.row_count,
            tree_height,
        ),
        marker_count: child.marker_count,
        part_count: child.part_count,
        row_count: child.row_count,
        tree_height,
    })
}

/// Path-copies one complete scope interval. Only nodes whose certified route
/// ranges contain (or neighbor an absent) scope are read; all other child IDs
/// are retained verbatim. Rebalancing is local and keeps fanout bounded.
pub(crate) async fn stage_replace_scoped_range(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    root: &ScopedRangeRoot,
    marker: ScopedRangeCoverageMarker,
    mut parts: Vec<ScopedRangePart>,
) -> Result<ScopedRangeRewrite, LixError> {
    let mut compressor = crate::compression::ZstdLevel1Compressor::new()
        .map_err(|error| scoped_range_error(format!("node compressor setup failed: {error}")))?;
    validate_root_digest(root)?;
    parts.sort_by(|left, right| left.first_key.cmp(&right.first_key));
    let mut replacement = Vec::with_capacity(parts.len() + 1);
    replacement.push(ScopedRangeEntry::Marker(marker.clone()));
    replacement.extend(parts.into_iter().map(ScopedRangeEntry::Part));
    validate_entry_closure(&replacement)?;

    let mut stats = ScopedRangeRewriteStats::default();
    let mut level = rewrite_scope_node(
        store,
        writes,
        root.root_id,
        &marker.scope,
        Some(replacement),
        &mut stats,
        &mut compressor,
    )
    .await?;
    if level.is_empty() {
        return Err(scoped_range_error(
            "scope replacement produced an empty tree",
        ));
    }
    let mut tree_height = level[0].child.level + 1;
    while level.len() > 1 {
        level = balanced_chunks(&level)
            .into_iter()
            .map(|chunk| {
                let staged = stage_node(
                    writes,
                    &mut compressor,
                    ScopedRangeNode {
                        level: chunk[0].child.level + 1,
                        entries: Vec::new(),
                        children: chunk.iter().map(|node| node.child.clone()).collect(),
                    },
                )?;
                stats.staged_nodes = stats.staged_nodes.saturating_add(1);
                Ok(staged)
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        tree_height += 1;
    }
    let child = level.pop().expect("non-empty rewrite has a root").child;
    let rewritten = ScopedRangeRoot {
        root_id: child.node_id,
        root_digest: root_digest(
            child.node_id,
            child.marker_count,
            child.part_count,
            child.row_count,
            tree_height,
        ),
        marker_count: child.marker_count,
        part_count: child.part_count,
        row_count: child.row_count,
        tree_height,
    };
    Ok(ScopedRangeRewrite {
        root: rewritten,
        stats,
    })
}

/// Loads only the shared search frontier for the scope marker and ordered
/// mutation keys. The returned plan is tied to `writes` and contains every
/// affected leaf plus the internal nodes required for a later read-free
/// path-copy stage.
pub(crate) async fn plan_scoped_range_part_splice(
    store: &(impl StorageAdapterRead + ?Sized),
    write_set_id: u64,
    staged_nodes: StagedScopedRangeNodes,
    root: &ScopedRangeRoot,
    scope: &ScopedRangePrefix,
    ordered_keys: &[Bytes],
) -> Result<ScopedRangeSplicePlan, LixError> {
    validate_root_digest(root)?;
    scope.validate()?;
    if ordered_keys.is_empty()
        || ordered_keys
            .windows(2)
            .any(|pair| pair[0].as_ref() >= pair[1].as_ref())
    {
        return Err(scoped_range_error(
            "splice keys are empty, duplicate, or unordered",
        ));
    }

    #[derive(Clone)]
    struct PendingRoute {
        route: ScopedRangeRoute,
        key_index: Option<usize>,
        expected: Option<ScopedRangeChild>,
    }
    let mut frontier = BTreeMap::<[u8; 32], Vec<PendingRoute>>::new();
    frontier
        .entry(root.root_id)
        .or_default()
        .push(PendingRoute {
            route: marker_route(scope),
            key_index: None,
            expected: None,
        });
    frontier
        .entry(root.root_id)
        .or_default()
        .extend(
            ordered_keys
                .iter()
                .enumerate()
                .map(|(key_index, key)| PendingRoute {
                    route: part_route(scope, key),
                    key_index: Some(key_index),
                    expected: None,
                }),
        );
    let mut nodes = BTreeMap::<[u8; 32], ScopedRangeNode>::new();
    let mut leaves = BTreeMap::<[u8; 32], (Vec<usize>, bool)>::new();
    while !frontier.is_empty() {
        let unseen = frontier
            .keys()
            .filter(|node_id| !nodes.contains_key(*node_id))
            .copied()
            .collect::<Vec<_>>();
        let staged = unseen
            .iter()
            .filter_map(|node_id| {
                staged_nodes
                    .nodes
                    .get(node_id)
                    .cloned()
                    .map(|node| (*node_id, node))
            })
            .collect();
        nodes.extend(
            unseen
                .iter()
                .copied()
                .zip(load_nodes_with_staged(store, &unseen, staged).await?),
        );
        let mut next = BTreeMap::<[u8; 32], Vec<PendingRoute>>::new();
        for (node_id, routes) in frontier {
            let node = nodes
                .get(&node_id)
                .ok_or_else(|| scoped_range_error("splice frontier omitted a node"))?;
            let summary = authenticated_node_summary(node, node_id)?;
            for route in routes {
                if let Some(expected) = route.expected.as_ref() {
                    if *expected != summary {
                        return Err(scoped_range_error("splice child summary mismatch"));
                    }
                } else if summary.marker_count != root.marker_count
                    || summary.part_count != root.part_count
                    || summary.row_count != root.row_count
                    || summary.level.checked_add(1) != Some(root.tree_height)
                {
                    return Err(scoped_range_error("splice root summary mismatch"));
                }
                if node.children.is_empty() {
                    let leaf = leaves.entry(node_id).or_default();
                    match route.key_index {
                        Some(index) => leaf.0.push(index),
                        None => leaf.1 = true,
                    }
                    continue;
                }
                let child_index = match node
                    .children
                    .binary_search_by(|child| child.first.cmp(&route.route))
                {
                    Ok(index) => index,
                    Err(0) => 0,
                    Err(index) => index - 1,
                };
                let child = node.children[child_index].clone();
                next.entry(child.node_id).or_default().push(PendingRoute {
                    expected: Some(child),
                    ..route
                });
            }
        }
        frontier = next;
    }

    let marker_leaf_id = leaves
        .iter()
        .find_map(|(node_id, (_, marker))| marker.then_some(*node_id))
        .ok_or_else(|| scoped_range_error("splice plan omitted the marker leaf"))?;
    let marker_node = nodes
        .get(&marker_leaf_id)
        .ok_or_else(|| scoped_range_error("splice marker leaf is unavailable"))?;
    let marker = marker_node
        .entries
        .iter()
        .find_map(|entry| match entry {
            ScopedRangeEntry::Marker(marker) if marker.scope == *scope => Some(marker.clone()),
            _ => None,
        })
        .ok_or_else(|| scoped_range_error("splice scope has no coverage marker"))?;
    let mut leaves = leaves
        .into_iter()
        .map(|(node_id, (mut key_indices, _contains_marker))| {
            key_indices.sort_unstable();
            key_indices.dedup();
            let entries = &nodes[&node_id].entries;
            let part_start = entries
                .iter()
                .position(
                    |entry| matches!(entry, ScopedRangeEntry::Part(part) if part.scope == *scope),
                )
                .unwrap_or(entries.len());
            let part_end = entries
                .iter()
                .rposition(
                    |entry| matches!(entry, ScopedRangeEntry::Part(part) if part.scope == *scope),
                )
                .map_or(part_start, |index| index + 1);
            if entries[part_start..part_end]
                .iter()
                .any(|entry| !matches!(entry, ScopedRangeEntry::Part(part) if part.scope == *scope))
            {
                return Err(scoped_range_error(
                    "splice scope parts are not contiguous within one leaf",
                ));
            }
            Ok(ScopedRangeSpliceLeaf {
                node_id,
                part_range: part_start..part_end,
                key_indices,
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    leaves.sort_by_key(|leaf| {
        nodes[&leaf.node_id]
            .entries
            .first()
            .expect("validated splice leaf is non-empty")
            .route()
    });
    Ok(ScopedRangeSplicePlan {
        write_set_id,
        root: root.clone(),
        scope: scope.clone(),
        marker,
        leaves,
        nodes,
    })
}

/// Applies replacement part lists for each planned leaf without further
/// reads. Untouched descendants retain their original content IDs. Coverage
/// totals are checked by subtracting the affected old parts and adding the
/// supplied replacements, so closure remains certified without flattening
/// the scope.
pub(crate) fn stage_scoped_range_part_splice(
    writes: &mut StorageWriteSet,
    plan: ScopedRangeSplicePlan,
    marker: ScopedRangeCoverageMarker,
    replacement_parts: Vec<Vec<ScopedRangePart>>,
) -> Result<ScopedRangeRewrite, LixError> {
    let mut compressor = crate::compression::ZstdLevel1Compressor::new()
        .map_err(|error| scoped_range_error(format!("node compressor setup failed: {error}")))?;
    if writes.identity() != plan.write_set_id {
        return Err(scoped_range_error(
            "splice plan belongs to another write set",
        ));
    }
    if marker.scope != plan.scope || replacement_parts.len() != plan.leaves.len() {
        return Err(scoped_range_error(
            "splice marker scope or replacement leaf count is invalid",
        ));
    }
    let (old_parts, old_rows) =
        (0..plan.leaf_count()).fold((0usize, 0u64), |(part_count, row_count), index| {
            let parts = plan.leaf_parts(index);
            (
                part_count + parts.len(),
                row_count + parts.map(|part| part.row_count).sum::<u64>(),
            )
        });
    let new_parts = replacement_parts.iter().map(Vec::len).sum::<usize>();
    let new_rows = replacement_parts
        .iter()
        .flatten()
        .try_fold(0u64, |sum, part| {
            if part.scope != plan.scope {
                return Err(scoped_range_error(
                    "splice replacement part belongs to another scope",
                ));
            }
            validate_part(part)?;
            sum.checked_add(part.row_count)
                .ok_or_else(|| scoped_range_error("splice row count overflows"))
        })?;
    let expected_parts = u64::from(plan.marker.part_count)
        .checked_sub(old_parts as u64)
        .and_then(|count| count.checked_add(new_parts as u64))
        .ok_or_else(|| scoped_range_error("splice part closure overflows"))?;
    let expected_rows = plan
        .marker
        .row_count
        .checked_sub(old_rows)
        .and_then(|count| count.checked_add(new_rows))
        .ok_or_else(|| scoped_range_error("splice row closure overflows"))?;
    if expected_parts != u64::from(marker.part_count) || expected_rows != marker.row_count {
        return Err(scoped_range_error(
            "splice coverage marker does not close its part delta",
        ));
    }

    let mut replacements = BTreeMap::new();
    for (leaf, parts) in plan.leaves.iter().zip(replacement_parts) {
        replacements.insert(leaf.node_id, parts);
    }
    let mut stats = ScopedRangeRewriteStats {
        loaded_nodes: u32::try_from(plan.nodes.len()).unwrap_or(u32::MAX),
        ..ScopedRangeRewriteStats::default()
    };
    let mut level = stage_splice_node(
        writes,
        &plan.nodes,
        plan.root.root_id,
        &plan.scope,
        &marker,
        &mut replacements,
        &mut stats,
        &mut compressor,
    )?;
    if !replacements.is_empty() || level.is_empty() {
        return Err(scoped_range_error(
            "splice plan did not consume every affected leaf",
        ));
    }
    let mut tree_height = level[0].child.level + 1;
    while level.len() > 1 {
        level = balanced_chunks(&level)
            .into_iter()
            .map(|chunk| {
                let staged = stage_node(
                    writes,
                    &mut compressor,
                    ScopedRangeNode {
                        level: chunk[0].child.level + 1,
                        entries: Vec::new(),
                        children: chunk.iter().map(|node| node.child.clone()).collect(),
                    },
                )?;
                stats.staged_nodes = stats.staged_nodes.saturating_add(1);
                Ok(staged)
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        tree_height += 1;
    }
    let root = level.pop().expect("non-empty splice has a root").child;
    Ok(ScopedRangeRewrite {
        root: ScopedRangeRoot {
            root_id: root.node_id,
            root_digest: root_digest(
                root.node_id,
                root.marker_count,
                root.part_count,
                root.row_count,
                tree_height,
            ),
            marker_count: root.marker_count,
            part_count: root.part_count,
            row_count: root.row_count,
            tree_height,
        },
        stats,
    })
}

fn stage_splice_node(
    writes: &mut StorageWriteSet,
    nodes: &BTreeMap<[u8; 32], ScopedRangeNode>,
    node_id: [u8; 32],
    scope: &ScopedRangePrefix,
    marker: &ScopedRangeCoverageMarker,
    replacements: &mut BTreeMap<[u8; 32], Vec<ScopedRangePart>>,
    stats: &mut ScopedRangeRewriteStats,
    compressor: &mut crate::compression::ZstdLevel1Compressor,
) -> Result<Vec<StagedNode>, LixError> {
    let node = nodes
        .get(&node_id)
        .ok_or_else(|| scoped_range_error("splice path omitted a planned node"))?;
    if node.children.is_empty() {
        let Some(parts) = replacements.remove(&node_id) else {
            return Err(scoped_range_error("splice leaf has no replacement list"));
        };
        let contains_marker = node
            .entries
            .iter()
            .any(|entry| matches!(entry, ScopedRangeEntry::Marker(old) if old.scope == *scope));
        let mut entries = node
            .entries
            .iter()
            .filter(|entry| match entry {
                ScopedRangeEntry::Marker(old) => old.scope != *scope,
                ScopedRangeEntry::Part(old) => old.scope != *scope,
            })
            .cloned()
            .collect::<Vec<_>>();
        if contains_marker {
            entries.push(ScopedRangeEntry::Marker(marker.clone()));
        }
        entries.extend(parts.into_iter().map(ScopedRangeEntry::Part));
        entries.sort_by(compare_entry_routes);
        return balanced_chunks(&entries)
            .into_iter()
            .map(|chunk| {
                let staged = stage_node(
                    writes,
                    compressor,
                    ScopedRangeNode {
                        level: 0,
                        entries: chunk.to_vec(),
                        children: Vec::new(),
                    },
                )?;
                stats.staged_nodes = stats.staged_nodes.saturating_add(1);
                Ok(staged)
            })
            .collect();
    }

    let mut children = Vec::new();
    for child in &node.children {
        if nodes.contains_key(&child.node_id) {
            children.extend(
                stage_splice_node(
                    writes,
                    nodes,
                    child.node_id,
                    scope,
                    marker,
                    replacements,
                    stats,
                    compressor,
                )?
                .into_iter()
                .map(|node| node.child),
            );
        } else {
            stats.reused_children = stats.reused_children.saturating_add(1);
            children.push(child.clone());
        }
    }
    children.sort_by(|left, right| left.first.cmp(&right.first));
    balanced_chunks(&children)
        .into_iter()
        .map(|chunk| {
            let staged = stage_node(
                writes,
                compressor,
                ScopedRangeNode {
                    level: chunk[0].level + 1,
                    entries: Vec::new(),
                    children: chunk.to_vec(),
                },
            )?;
            stats.staged_nodes = stats.staged_nodes.saturating_add(1);
            Ok(staged)
        })
        .collect()
}

type RewriteFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<StagedNode>, LixError>> + Send + 'a>>;

fn rewrite_scope_node<'a, R: StorageAdapterRead + ?Sized>(
    store: &'a R,
    writes: &'a mut StorageWriteSet,
    node_id: [u8; 32],
    scope: &'a ScopedRangePrefix,
    replacement: Option<Vec<ScopedRangeEntry>>,
    stats: &'a mut ScopedRangeRewriteStats,
    compressor: &'a mut crate::compression::ZstdLevel1Compressor,
) -> RewriteFuture<'a> {
    Box::pin(async move {
        let staged = staged_node(writes, node_id)?;
        let node = match staged {
            Some(node) => node,
            None => load_node(store, node_id).await?,
        };
        stats.loaded_nodes = stats.loaded_nodes.saturating_add(1);
        if node.children.is_empty() {
            let mut entries = node
                .entries
                .into_iter()
                .filter(|entry| entry.scope() != scope)
                .collect::<Vec<_>>();
            if let Some(replacement) = replacement {
                entries.extend(replacement);
            }
            entries.sort_by(compare_entry_routes);
            return balanced_chunks(&entries)
                .into_iter()
                .map(|chunk| {
                    let staged = stage_node(
                        writes,
                        compressor,
                        ScopedRangeNode {
                            level: 0,
                            entries: chunk.to_vec(),
                            children: Vec::new(),
                        },
                    )?;
                    stats.staged_nodes = stats.staged_nodes.saturating_add(1);
                    Ok(staged)
                })
                .collect();
        }

        let mut selected = node
            .children
            .iter()
            .enumerate()
            .filter_map(|(index, child)| {
                (child.first.scope <= *scope && *scope <= child.last.scope).then_some(index)
            })
            .collect::<Vec<_>>();
        if selected.is_empty() {
            let index = match node
                .children
                .binary_search_by(|child| child.first.scope.cmp(scope))
            {
                Ok(index) => index,
                Err(0) => 0,
                Err(index) => index - 1,
            };
            selected.push(index);
        }
        let selected_set = selected.iter().copied().collect::<BTreeSet<_>>();
        stats.reused_children = stats.reused_children.saturating_add(
            u32::try_from(node.children.len() - selected_set.len()).unwrap_or(u32::MAX),
        );
        let first_selected = selected[0];
        let mut children = Vec::new();
        let mut replacement = replacement;
        for (index, child) in node.children.into_iter().enumerate() {
            if selected_set.contains(&index) {
                children.extend(
                    rewrite_scope_node(
                        store,
                        writes,
                        child.node_id,
                        scope,
                        if index == first_selected {
                            replacement.take()
                        } else {
                            None
                        },
                        stats,
                        compressor,
                    )
                    .await?
                    .into_iter()
                    .map(|node| node.child),
                );
            } else {
                children.push(child);
            }
        }
        children.sort_by(|left, right| left.first.cmp(&right.first));
        balanced_chunks(&children)
            .into_iter()
            .map(|chunk| {
                let staged = stage_node(
                    writes,
                    compressor,
                    ScopedRangeNode {
                        level: chunk[0].level + 1,
                        entries: Vec::new(),
                        children: chunk.to_vec(),
                    },
                )?;
                stats.staged_nodes = stats.staged_nodes.saturating_add(1);
                Ok(staged)
            })
            .collect()
    })
}

/// Resolves both the exact scope marker and the part predecessor for a point.
#[cfg(test)]
pub(crate) async fn route_scoped_range_point(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &ScopedRangeRoot,
    scope: &ScopedRangePrefix,
    key: &[u8],
) -> Result<ScopedRangePointRoute, LixError> {
    route_scoped_range_points(store, root, &[(scope, key)])
        .await?
        .pop()
        .ok_or_else(|| scoped_range_error("single point batch omitted its result"))
}

/// Routes a point batch through one shared frontier per tree level. Marker
/// and predecessor probes, duplicate scopes, and common ancestors share each
/// content-addressed node read.
#[cfg(test)]
pub(crate) async fn route_scoped_range_points(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &ScopedRangeRoot,
    points: &[(&ScopedRangePrefix, &[u8])],
) -> Result<Vec<ScopedRangePointRoute>, LixError> {
    validate_root_digest(root)?;
    #[derive(Clone, Copy)]
    enum ProbeKind {
        Marker,
        Predecessor,
    }
    #[derive(Clone)]
    struct Probe {
        point_index: usize,
        kind: ProbeKind,
        route: ScopedRangeRoute,
        expected: Option<ScopedRangeChild>,
    }

    let mut frontier = BTreeMap::<[u8; 32], Vec<Probe>>::new();
    for (point_index, (scope, key)) in points.iter().enumerate() {
        frontier.entry(root.root_id).or_default().extend([
            Probe {
                point_index,
                kind: ProbeKind::Marker,
                route: marker_route(scope),
                expected: None,
            },
            Probe {
                point_index,
                kind: ProbeKind::Predecessor,
                route: part_route(scope, key),
                expected: None,
            },
        ]);
    }
    let mut cache = BTreeMap::<[u8; 32], LoadedScopedRangeNode>::new();
    let mut marker_results = vec![None; points.len()];
    let mut predecessor_results = vec![None; points.len()];
    while !frontier.is_empty() {
        let unseen = frontier
            .keys()
            .filter(|node_id| !cache.contains_key(*node_id))
            .copied()
            .collect::<Vec<_>>();
        let loaded = load_authenticated_nodes(store, &unseen).await?;
        cache.extend(unseen.into_iter().zip(loaded));
        let mut next = BTreeMap::<[u8; 32], Vec<Probe>>::new();
        for (node_id, probes) in frontier {
            let loaded = cache
                .get(&node_id)
                .ok_or_else(|| scoped_range_error("point frontier omitted a node"))?;
            let node = &loaded.node;
            let summary = &loaded.summary;
            for probe in probes {
                if let Some(expected) = probe.expected.as_ref() {
                    if expected != summary {
                        return Err(scoped_range_error("point child summary mismatch"));
                    }
                } else if summary.marker_count != root.marker_count
                    || summary.part_count != root.part_count
                    || summary.row_count != root.row_count
                    || summary.level.checked_add(1) != Some(root.tree_height)
                {
                    return Err(scoped_range_error("point root summary mismatch"));
                }
                if node.children.is_empty() {
                    let found = match probe.kind {
                        ProbeKind::Marker => node
                            .entries
                            .binary_search_by(|entry| {
                                compare_entry_to_owned_route(entry, &probe.route)
                            })
                            .ok()
                            .map(|index| node.entries[index].clone()),
                        ProbeKind::Predecessor => {
                            let index = match node.entries.binary_search_by(|entry| {
                                compare_entry_to_owned_route(entry, &probe.route)
                            }) {
                                Ok(index) => Some(index),
                                Err(0) => None,
                                Err(index) => Some(index - 1),
                            };
                            index.map(|index| node.entries[index].clone())
                        }
                    };
                    match probe.kind {
                        ProbeKind::Marker => marker_results[probe.point_index] = found,
                        ProbeKind::Predecessor => predecessor_results[probe.point_index] = found,
                    }
                    continue;
                }
                let child_index = match node
                    .children
                    .binary_search_by(|child| child.first.cmp(&probe.route))
                {
                    Ok(index) => index,
                    Err(0) => 0,
                    Err(index) => index - 1,
                };
                let child = node.children[child_index].clone();
                next.entry(child.node_id).or_default().push(Probe {
                    expected: Some(child),
                    ..probe
                });
            }
        }
        frontier = next;
    }
    points
        .iter()
        .enumerate()
        .map(|(index, (scope, key))| {
            let coverage = match marker_results[index].take() {
                Some(ScopedRangeEntry::Marker(marker)) if marker.scope == **scope => Some(marker),
                Some(ScopedRangeEntry::Marker(_)) | None => None,
                Some(ScopedRangeEntry::Part(_)) => {
                    return Err(scoped_range_error("marker route resolved a part"));
                }
            };
            let predecessor = match predecessor_results[index].take() {
                Some(ScopedRangeEntry::Part(part)) if part.scope == **scope => Some(part),
                _ => None,
            };
            let covered_part = predecessor
                .as_ref()
                .filter(|part| {
                    part.first_key.as_slice() <= *key && *key <= part.last_key.as_slice()
                })
                .cloned();
            Ok(ScopedRangePointRoute {
                coverage,
                predecessor,
                covered_part,
            })
        })
        .collect()
}

/// Resolves point-serving routes with one predecessor probe per key. Coverage
/// follows from the canonical entry ordering: a marker starts each scope and
/// the validated tree contains only that scope's parts until the next marker.
pub(crate) async fn route_scoped_range_covered_points(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &ScopedRangeRoot,
    points: &[(&ScopedRangePrefix, &[u8])],
) -> Result<Vec<ScopedRangeCoveredPointRoute>, LixError> {
    validate_root_digest(root)?;
    #[derive(Clone)]
    struct Probe {
        point_index: usize,
        route: ScopedRangeRoute,
        expected: Option<ScopedRangeChild>,
    }

    let mut frontier = BTreeMap::<[u8; 32], Vec<Probe>>::new();
    for (point_index, (scope, key)) in points.iter().enumerate() {
        frontier.entry(root.root_id).or_default().push(Probe {
            point_index,
            route: part_route(scope, key),
            expected: None,
        });
    }
    let mut cache = BTreeMap::<[u8; 32], LoadedScopedRangeNode>::new();
    let mut predecessors = vec![None; points.len()];
    while !frontier.is_empty() {
        let unseen = frontier
            .keys()
            .filter(|node_id| !cache.contains_key(*node_id))
            .copied()
            .collect::<Vec<_>>();
        let loaded = load_authenticated_nodes(store, &unseen).await?;
        cache.extend(unseen.into_iter().zip(loaded));
        let mut next = BTreeMap::<[u8; 32], Vec<Probe>>::new();
        for (node_id, probes) in frontier {
            let loaded = cache
                .get(&node_id)
                .ok_or_else(|| scoped_range_error("point frontier omitted a node"))?;
            let node = &loaded.node;
            let summary = &loaded.summary;
            for probe in probes {
                if let Some(expected) = probe.expected.as_ref() {
                    if expected != summary {
                        return Err(scoped_range_error("point child summary mismatch"));
                    }
                } else if summary.marker_count != root.marker_count
                    || summary.part_count != root.part_count
                    || summary.row_count != root.row_count
                    || summary.level.checked_add(1) != Some(root.tree_height)
                {
                    return Err(scoped_range_error("point root summary mismatch"));
                }
                if node.children.is_empty() {
                    let index = match node
                        .entries
                        .binary_search_by(|entry| compare_entry_to_owned_route(entry, &probe.route))
                    {
                        Ok(index) => Some(index),
                        Err(0) => None,
                        Err(index) => Some(index - 1),
                    };
                    predecessors[probe.point_index] =
                        index.map(|index| node.entries[index].clone());
                    continue;
                }
                let child_index = match node
                    .children
                    .binary_search_by(|child| child.first.cmp(&probe.route))
                {
                    Ok(index) => index,
                    Err(0) => 0,
                    Err(index) => index - 1,
                };
                let child = node.children[child_index].clone();
                next.entry(child.node_id).or_default().push(Probe {
                    expected: Some(child),
                    ..probe
                });
            }
        }
        frontier = next;
    }

    points
        .iter()
        .enumerate()
        .map(|(index, (scope, key))| {
            let predecessor = predecessors[index].take();
            let scope_covered = match predecessor.as_ref() {
                Some(ScopedRangeEntry::Marker(marker)) => marker.scope == **scope,
                Some(ScopedRangeEntry::Part(part)) => part.scope == **scope,
                None => false,
            };
            let covered_part = match predecessor {
                Some(ScopedRangeEntry::Part(part))
                    if part.scope == **scope
                        && part.first_key.as_slice() <= *key
                        && *key <= part.last_key.as_slice() =>
                {
                    Some(part)
                }
                _ => None,
            };
            Ok(ScopedRangeCoveredPointRoute {
                scope_covered,
                covered_part,
            })
        })
        .collect()
}

/// Proves that the immutable part envelopes for one exact covered scope are
/// byte-identical. Equal content IDs prune whole shared subtrees; unequal tree
/// shapes are aligned by authenticated route ranges, including leaf splits and
/// height changes. Any physical difference is deliberately inconclusive.
pub(crate) async fn prove_scoped_range_scope_equal(
    store: &(impl StorageAdapterRead + ?Sized),
    left_root: &ScopedRangeRoot,
    right_root: &ScopedRangeRoot,
    scope: &ScopedRangePrefix,
) -> Result<ScopedRangeScopeEqualityProof, LixError> {
    validate_root_digest(left_root)?;
    validate_root_digest(right_root)?;
    scope.validate()?;

    let left_marker = find_exact(store, left_root, &marker_route(scope)).await?;
    if left_root == right_root {
        return Ok(match left_marker {
            Some(ScopedRangeEntry::Marker(marker)) if marker.scope == *scope => {
                ScopedRangeScopeEqualityProof::Equal
            }
            Some(ScopedRangeEntry::Marker(_)) | None => ScopedRangeScopeEqualityProof::NotProven,
            Some(ScopedRangeEntry::Part(_)) => {
                return Err(scoped_range_error("scope marker route resolved a part"));
            }
        });
    }
    let right_marker = find_exact(store, right_root, &marker_route(scope)).await?;
    match (left_marker, right_marker) {
        (Some(ScopedRangeEntry::Marker(left)), Some(ScopedRangeEntry::Marker(right)))
            if left.scope == *scope && right.scope == *scope && left == right => {}
        (Some(ScopedRangeEntry::Part(_)), _) | (_, Some(ScopedRangeEntry::Part(_))) => {
            return Err(scoped_range_error("scope marker route resolved a part"));
        }
        _ => return Ok(ScopedRangeScopeEqualityProof::NotProven),
    }

    let lower = marker_route(scope);
    let upper = scope_end_route(scope);
    let mut cache = BTreeMap::<[u8; 32], ScopedRangeNode>::new();
    let left = load_scoped_range_root_ref(store, left_root, &mut cache).await?;
    let right = load_scoped_range_root_ref(store, right_root, &mut cache).await?;
    let mut work = vec![ScopedRangeEqualityWork::Compare(left, right)];
    let mut left_candidates = BTreeMap::<Vec<u8>, ScopedRangePart>::new();
    let mut right_candidates = BTreeMap::<Vec<u8>, ScopedRangePart>::new();

    while let Some(next) = work.pop() {
        match next {
            ScopedRangeEqualityWork::Compare(left, right) => {
                if left.node_id == right.node_id {
                    if left != right {
                        return Err(scoped_range_error(
                            "equal node identity has conflicting range summaries",
                        ));
                    }
                    continue;
                }
                if left.last < right.first || right.last < left.first {
                    work.push(ScopedRangeEqualityWork::CollectLeft(left));
                    work.push(ScopedRangeEqualityWork::CollectRight(right));
                    continue;
                }
                let left_node = load_scoped_range_node_ref(store, &left, &mut cache).await?;
                let right_node = load_scoped_range_node_ref(store, &right, &mut cache).await?;
                match (
                    left_node.children.is_empty(),
                    right_node.children.is_empty(),
                ) {
                    (true, true) => {
                        insert_scope_part_candidates(
                            &mut left_candidates,
                            left_node.entries,
                            scope,
                        )?;
                        insert_scope_part_candidates(
                            &mut right_candidates,
                            right_node.entries,
                            scope,
                        )?;
                    }
                    (false, false) => enqueue_aligned_scoped_range_refs(
                        scoped_range_children_in_interval(left_node.children, &lower, &upper),
                        scoped_range_children_in_interval(right_node.children, &lower, &upper),
                        &mut work,
                    ),
                    (false, true) => enqueue_aligned_scoped_range_refs(
                        scoped_range_children_in_interval(left_node.children, &lower, &upper),
                        vec![right],
                        &mut work,
                    ),
                    (true, false) => enqueue_aligned_scoped_range_refs(
                        vec![left],
                        scoped_range_children_in_interval(right_node.children, &lower, &upper),
                        &mut work,
                    ),
                }
            }
            ScopedRangeEqualityWork::CollectLeft(reference) => {
                collect_scope_part_candidates(
                    store,
                    reference,
                    scope,
                    &lower,
                    &upper,
                    &mut cache,
                    &mut left_candidates,
                    true,
                    &mut work,
                )
                .await?;
            }
            ScopedRangeEqualityWork::CollectRight(reference) => {
                collect_scope_part_candidates(
                    store,
                    reference,
                    scope,
                    &lower,
                    &upper,
                    &mut cache,
                    &mut right_candidates,
                    false,
                    &mut work,
                )
                .await?;
            }
        }
    }

    remove_shared_scope_parts(&mut left_candidates, &mut right_candidates);
    Ok(
        if left_candidates.is_empty() && right_candidates.is_empty() {
            ScopedRangeScopeEqualityProof::Equal
        } else {
            ScopedRangeScopeEqualityProof::NotProven
        },
    )
}

/// Returns every part overlapping the closed interval, retaining the explicit
/// scope marker so an empty result is distinguishable from unknown coverage.
#[cfg(test)]
pub(crate) async fn scan_scoped_range_interval(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &ScopedRangeRoot,
    scope: &ScopedRangePrefix,
    first_key: &[u8],
    last_key: &[u8],
) -> Result<ScopedRangeInterval, LixError> {
    if first_key > last_key {
        return Err(scoped_range_error("interval bounds are reversed"));
    }
    let point = route_scoped_range_point(store, root, scope, first_key).await?;
    let mut parts = Vec::new();
    if let Some(part) = point.covered_part {
        parts.push(part);
    }
    let lower = part_route(scope, first_key);
    let upper = part_route(scope, last_key);
    collect_route_interval(store, root, &lower, &upper, &mut parts).await?;
    parts.retain(|part| {
        part.scope == *scope
            && part.first_key.as_slice() <= last_key
            && first_key <= part.last_key.as_slice()
    });
    parts.sort_by(|left, right| left.first_key.cmp(&right.first_key));
    parts.dedup_by(|left, right| left.first_key == right.first_key);
    Ok(ScopedRangeInterval {
        coverage: point.coverage,
        parts,
    })
}

/// Resolves only the authenticated coverage marker for one exact scope.
pub(crate) async fn load_scoped_range_coverage(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &ScopedRangeRoot,
    scope: &ScopedRangePrefix,
) -> Result<Option<ScopedRangeCoverageMarker>, LixError> {
    match find_exact(store, root, &marker_route(scope)).await? {
        Some(ScopedRangeEntry::Marker(marker)) if marker.scope == *scope => Ok(Some(marker)),
        Some(ScopedRangeEntry::Marker(_)) | None => Ok(None),
        Some(ScopedRangeEntry::Part(_)) => {
            Err(scoped_range_error("scope marker route resolved a part"))
        }
    }
}

/// Resolves one exact marker while accepting immutable nodes staged by the
/// same physical publication.
pub(crate) async fn load_scoped_range_coverage_with_staged(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &StorageWriteSet,
    root: &ScopedRangeRoot,
    scope: &ScopedRangePrefix,
) -> Result<Option<ScopedRangeCoverageMarker>, LixError> {
    match find_exact_with_staged(store, writes, root, &marker_route(scope)).await? {
        Some(ScopedRangeEntry::Marker(marker)) if marker.scope == *scope => Ok(Some(marker)),
        Some(ScopedRangeEntry::Marker(_)) | None => Ok(None),
        Some(ScopedRangeEntry::Part(_)) => {
            Err(scoped_range_error("scope marker route resolved a part"))
        }
    }
}

/// Returns every part in one exact scope without inventing a sentinel row
/// key. The synthetic route kind sorts after all part starts in that scope.
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) async fn scan_scoped_range_scope(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &ScopedRangeRoot,
    scope: &ScopedRangePrefix,
) -> Result<ScopedRangeInterval, LixError> {
    let coverage = load_scoped_range_coverage(store, root, scope).await?;
    let Some(coverage) = coverage else {
        return Ok(ScopedRangeInterval {
            coverage: None,
            parts: Vec::new(),
        });
    };
    let lower = marker_route(scope);
    let upper = ScopedRangeRoute {
        scope: scope.clone(),
        kind: 2,
        key: Vec::new(),
    };
    let mut parts = Vec::new();
    collect_route_interval(store, root, &lower, &upper, &mut parts).await?;
    parts.retain(|part| part.scope == *scope);
    parts.sort_by(|left, right| left.first_key.cmp(&right.first_key));
    if parts.len() != coverage.part_count as usize
        || parts.iter().map(|part| part.row_count).sum::<u64>() != coverage.row_count
    {
        return Err(scoped_range_error(
            "scope scan disagrees with coverage marker",
        ));
    }
    Ok(ScopedRangeInterval {
        coverage: Some(coverage),
        parts,
    })
}

/// Loads the complete reachable graph and verifies content IDs, child/root
/// summaries, balance, ordering, and per-scope marker/row/part closure even
/// when a scope spans many leaves.
#[cfg(test)]
pub(crate) async fn validate_scoped_range_tree(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &ScopedRangeRoot,
) -> Result<ScopedRangeReachability, LixError> {
    validate_scoped_range_trees(store, std::slice::from_ref(root)).await
}

/// Authenticates several live roots through one global, batched node frontier.
/// Shared content IDs are loaded and decoded once, while a cache-only walk per
/// root still proves its own ordering, closure, and root summary.
pub(crate) async fn validate_scoped_range_trees(
    store: &(impl StorageAdapterRead + ?Sized),
    roots: &[ScopedRangeRoot],
) -> Result<ScopedRangeReachability, LixError> {
    validate_scoped_range_trees_inner(store, roots, None).await
}

/// Authenticates one tree while accepting immutable nodes staged by the same
/// publication. This is used only by owner-side transitions that must derive
/// exact cascade scopes before the write set is committed.
pub(crate) async fn validate_scoped_range_tree_with_staged(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &StorageWriteSet,
    root: &ScopedRangeRoot,
) -> Result<ScopedRangeReachability, LixError> {
    let staged = snapshot_staged_scoped_range_nodes(writes)?;
    validate_scoped_range_trees_inner(store, std::slice::from_ref(root), Some(&staged.nodes)).await
}

async fn validate_scoped_range_trees_inner(
    store: &(impl StorageAdapterRead + ?Sized),
    roots: &[ScopedRangeRoot],
    staged_nodes: Option<&BTreeMap<[u8; 32], ScopedRangeNode>>,
) -> Result<ScopedRangeReachability, LixError> {
    for root in roots {
        validate_root_digest(root)?;
    }
    let mut frontier = roots
        .iter()
        .map(|root| root.root_id)
        .collect::<BTreeSet<_>>();
    let mut cache = BTreeMap::<[u8; 32], LoadedScopedRangeNode>::new();
    while !frontier.is_empty() {
        let node_ids = frontier.iter().copied().collect::<Vec<_>>();
        frontier.clear();
        let loaded = if let Some(staged_nodes) = staged_nodes {
            let staged = node_ids
                .iter()
                .filter_map(|node_id| {
                    staged_nodes
                        .get(node_id)
                        .cloned()
                        .map(|node| (*node_id, node))
                })
                .collect();
            load_nodes_with_staged(store, &node_ids, staged)
                .await?
                .into_iter()
                .zip(node_ids.iter().copied())
                .map(|(node, node_id)| {
                    let summary = authenticated_node_summary(&node, node_id)?;
                    Ok(LoadedScopedRangeNode { node, summary })
                })
                .collect::<Result<Vec<_>, LixError>>()?
        } else {
            load_authenticated_nodes(store, &node_ids).await?
        };
        for (node_id, loaded) in node_ids.into_iter().zip(loaded) {
            for child in &loaded.node.children {
                if let Some(existing) = cache.get(&child.node_id) {
                    if existing.summary != *child {
                        return Err(scoped_range_error("child summary mismatch"));
                    }
                } else {
                    frontier.insert(child.node_id);
                }
            }
            cache.insert(node_id, loaded);
        }
        frontier.retain(|node_id| !cache.contains_key(node_id));
    }

    let mut reachable_node_ids = BTreeSet::new();
    let mut reachable_markers = Vec::new();
    let mut reachable_parts = Vec::new();
    for (node_id, loaded) in &cache {
        reachable_node_ids.insert(*node_id);
        if loaded.node.children.is_empty() {
            for entry in &loaded.node.entries {
                match entry {
                    ScopedRangeEntry::Marker(marker) => reachable_markers.push(marker.clone()),
                    ScopedRangeEntry::Part(part) => reachable_parts.push(part.clone()),
                }
            }
        }
    }

    for root in roots {
        let root_loaded = cache
            .get(&root.root_id)
            .ok_or_else(|| scoped_range_error("root was omitted from the multi-root cache"))?;
        if root_loaded.summary.marker_count != root.marker_count
            || root_loaded.summary.part_count != root.part_count
            || root_loaded.summary.row_count != root.row_count
            || root_loaded.summary.level.checked_add(1) != Some(root.tree_height)
        {
            return Err(scoped_range_error("root summary mismatch"));
        }
        let mut pending = vec![root.root_id];
        let mut root_nodes = BTreeSet::new();
        let mut entries = Vec::new();
        while let Some(node_id) = pending.pop() {
            if !root_nodes.insert(node_id) {
                return Err(scoped_range_error(
                    "tree contains a cycle or duplicate child",
                ));
            }
            let loaded = cache
                .get(&node_id)
                .ok_or_else(|| scoped_range_error("reachable node was omitted from the cache"))?;
            if loaded.node.children.is_empty() {
                entries.extend(loaded.node.entries.iter().cloned());
            } else {
                for child in loaded.node.children.iter().rev() {
                    let child_loaded = cache.get(&child.node_id).ok_or_else(|| {
                        scoped_range_error("child was omitted from the multi-root cache")
                    })?;
                    if child_loaded.summary != *child {
                        return Err(scoped_range_error("child summary mismatch"));
                    }
                    pending.push(child.node_id);
                }
            }
        }
        entries.sort_by(compare_entry_routes);
        validate_entry_closure(&entries)?;
        let marker_count = entries
            .iter()
            .filter(|entry| matches!(entry, ScopedRangeEntry::Marker(_)))
            .count();
        let parts = entries.iter().filter_map(|entry| match entry {
            ScopedRangeEntry::Marker(_) => None,
            ScopedRangeEntry::Part(part) => Some(part),
        });
        let (part_count, row_count) = parts.fold((0usize, 0u64), |(count, rows), part| {
            (count + 1, rows.saturating_add(part.row_count))
        });
        if marker_count != root.marker_count as usize
            || part_count != root.part_count as usize
            || row_count != root.row_count
        {
            return Err(scoped_range_error(
                "reachable entries disagree with root closure",
            ));
        }
    }
    Ok(ScopedRangeReachability {
        node_ids: reachable_node_ids,
        markers: reachable_markers,
        parts: reachable_parts,
    })
}

fn scope_end_route(scope: &ScopedRangePrefix) -> ScopedRangeRoute {
    ScopedRangeRoute {
        scope: scope.clone(),
        kind: 2,
        key: Vec::new(),
    }
}

async fn load_scoped_range_root_ref(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &ScopedRangeRoot,
    cache: &mut BTreeMap<[u8; 32], ScopedRangeNode>,
) -> Result<ScopedRangeNodeRef, LixError> {
    let node = load_cached_scoped_range_node(store, root.root_id, cache).await?;
    let summary = authenticated_node_summary(&node, root.root_id)?;
    if summary.marker_count != root.marker_count
        || summary.part_count != root.part_count
        || summary.row_count != root.row_count
        || summary.level.checked_add(1) != Some(root.tree_height)
    {
        return Err(scoped_range_error("equality root summary mismatch"));
    }
    Ok(summary.into())
}

async fn load_scoped_range_node_ref(
    store: &(impl StorageAdapterRead + ?Sized),
    reference: &ScopedRangeNodeRef,
    cache: &mut BTreeMap<[u8; 32], ScopedRangeNode>,
) -> Result<ScopedRangeNode, LixError> {
    let node = load_cached_scoped_range_node(store, reference.node_id, cache).await?;
    let summary: ScopedRangeNodeRef = authenticated_node_summary(&node, reference.node_id)?.into();
    if &summary != reference {
        return Err(scoped_range_error(
            "equality node disagrees with its range summary",
        ));
    }
    Ok(node)
}

async fn load_cached_scoped_range_node(
    store: &(impl StorageAdapterRead + ?Sized),
    node_id: [u8; 32],
    cache: &mut BTreeMap<[u8; 32], ScopedRangeNode>,
) -> Result<ScopedRangeNode, LixError> {
    if let Some(node) = cache.get(&node_id) {
        return Ok(node.clone());
    }
    let node = load_node(store, node_id).await?;
    cache.insert(node_id, node.clone());
    Ok(node)
}

fn scoped_range_children_in_interval(
    children: Vec<ScopedRangeChild>,
    lower: &ScopedRangeRoute,
    upper: &ScopedRangeRoute,
) -> Vec<ScopedRangeNodeRef> {
    children
        .into_iter()
        .filter(|child| child.last >= *lower && child.first <= *upper)
        .map(Into::into)
        .collect()
}

async fn collect_scope_part_candidates(
    store: &(impl StorageAdapterRead + ?Sized),
    reference: ScopedRangeNodeRef,
    scope: &ScopedRangePrefix,
    lower: &ScopedRangeRoute,
    upper: &ScopedRangeRoute,
    cache: &mut BTreeMap<[u8; 32], ScopedRangeNode>,
    candidates: &mut BTreeMap<Vec<u8>, ScopedRangePart>,
    left: bool,
    work: &mut Vec<ScopedRangeEqualityWork>,
) -> Result<(), LixError> {
    let node = load_scoped_range_node_ref(store, &reference, cache).await?;
    if node.children.is_empty() {
        insert_scope_part_candidates(candidates, node.entries, scope)
    } else {
        for child in scoped_range_children_in_interval(node.children, lower, upper)
            .into_iter()
            .rev()
        {
            work.push(if left {
                ScopedRangeEqualityWork::CollectLeft(child)
            } else {
                ScopedRangeEqualityWork::CollectRight(child)
            });
        }
        Ok(())
    }
}

fn insert_scope_part_candidates(
    candidates: &mut BTreeMap<Vec<u8>, ScopedRangePart>,
    entries: Vec<ScopedRangeEntry>,
    scope: &ScopedRangePrefix,
) -> Result<(), LixError> {
    for part in entries.into_iter().filter_map(|entry| match entry {
        ScopedRangeEntry::Part(part) if part.scope == *scope => Some(part),
        _ => None,
    }) {
        match candidates.entry(part.first_key.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(part);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &part => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(scoped_range_error(
                    "equality traversal found conflicting parts for one first key",
                ));
            }
        }
    }
    Ok(())
}

fn remove_shared_scope_parts(
    left: &mut BTreeMap<Vec<u8>, ScopedRangePart>,
    right: &mut BTreeMap<Vec<u8>, ScopedRangePart>,
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

fn enqueue_aligned_scoped_range_refs(
    left: Vec<ScopedRangeNodeRef>,
    right: Vec<ScopedRangeNodeRef>,
    work: &mut Vec<ScopedRangeEqualityWork>,
) {
    let mut pending = Vec::new();
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        let left_ref = &left[left_index];
        let right_ref = &right[right_index];
        if left_ref.last < right_ref.first {
            pending.push(ScopedRangeEqualityWork::CollectLeft(left_ref.clone()));
            left_index += 1;
            continue;
        }
        if right_ref.last < left_ref.first {
            pending.push(ScopedRangeEqualityWork::CollectRight(right_ref.clone()));
            right_index += 1;
            continue;
        }
        pending.push(ScopedRangeEqualityWork::Compare(
            left_ref.clone(),
            right_ref.clone(),
        ));
        match left_ref.last.cmp(&right_ref.last) {
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
            .map(ScopedRangeEqualityWork::CollectLeft),
    );
    pending.extend(
        right[right_index..]
            .iter()
            .cloned()
            .map(ScopedRangeEqualityWork::CollectRight),
    );
    work.extend(pending.into_iter().rev());
}

async fn find_exact(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &ScopedRangeRoot,
    route: &ScopedRangeRoute,
) -> Result<Option<ScopedRangeEntry>, LixError> {
    let leaf = load_routed_leaf_with_staged(store, None, root, route).await?;
    Ok(leaf
        .entries
        .binary_search_by_key(route, ScopedRangeEntry::route)
        .ok()
        .map(|index| leaf.entries[index].clone()))
}

async fn find_exact_with_staged(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &StorageWriteSet,
    root: &ScopedRangeRoot,
    route: &ScopedRangeRoute,
) -> Result<Option<ScopedRangeEntry>, LixError> {
    let leaf = load_routed_leaf_with_staged(store, Some(writes), root, route).await?;
    Ok(leaf
        .entries
        .binary_search_by_key(route, ScopedRangeEntry::route)
        .ok()
        .map(|index| leaf.entries[index].clone()))
}

async fn load_routed_leaf_with_staged(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: Option<&StorageWriteSet>,
    root: &ScopedRangeRoot,
    route: &ScopedRangeRoute,
) -> Result<ScopedRangeNode, LixError> {
    let mut node_id = root.root_id;
    let mut expected = None;
    loop {
        let node = match writes
            .map(|writes| staged_node(writes, node_id))
            .transpose()?
        {
            Some(Some(node)) => node,
            Some(None) | None => load_node(store, node_id).await?,
        };
        let summary = authenticated_node_summary(&node, node_id)?;
        if let Some(expected) = expected.take() {
            if summary != expected {
                return Err(scoped_range_error("routed child summary mismatch"));
            }
        } else if summary.marker_count != root.marker_count
            || summary.part_count != root.part_count
            || summary.row_count != root.row_count
            || summary.level.checked_add(1) != Some(root.tree_height)
        {
            return Err(scoped_range_error("routed root summary mismatch"));
        }
        if node.children.is_empty() {
            return Ok(node);
        }
        let index = match node
            .children
            .binary_search_by(|child| child.first.cmp(route))
        {
            Ok(index) => index,
            Err(0) => 0,
            Err(index) => index - 1,
        };
        let child = node.children[index].clone();
        node_id = child.node_id;
        expected = Some(child);
    }
}

#[cfg(any(test, feature = "storage-benches"))]
async fn collect_route_interval(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &ScopedRangeRoot,
    lower: &ScopedRangeRoute,
    upper: &ScopedRangeRoute,
    parts: &mut Vec<ScopedRangePart>,
) -> Result<(), LixError> {
    validate_root_digest(root)?;
    let mut frontier = BTreeMap::from([(root.root_id, None::<ScopedRangeChild>)]);
    while !frontier.is_empty() {
        let node_ids = frontier.keys().copied().collect::<Vec<_>>();
        let loaded = load_authenticated_nodes(store, &node_ids).await?;
        let current = std::mem::take(&mut frontier);
        for ((node_id, expected), loaded) in current.into_iter().zip(loaded) {
            let summary = loaded.summary;
            if let Some(expected) = expected {
                if expected != summary {
                    return Err(scoped_range_error("interval child summary mismatch"));
                }
            } else if summary.marker_count != root.marker_count
                || summary.part_count != root.part_count
                || summary.row_count != root.row_count
                || summary.level.checked_add(1) != Some(root.tree_height)
            {
                return Err(scoped_range_error("interval root summary mismatch"));
            }
            if summary.last < *lower || summary.first > *upper {
                continue;
            }
            let node = loaded.node;
            if node.children.is_empty() {
                parts.extend(node.entries.into_iter().filter_map(|entry| match entry {
                    ScopedRangeEntry::Part(part)
                        if lower <= &part_route(&part.scope, &part.first_key)
                            && &part_route(&part.scope, &part.first_key) <= upper =>
                    {
                        Some(part)
                    }
                    _ => None,
                }));
            } else {
                for child in node
                    .children
                    .into_iter()
                    .filter(|child| child.last >= *lower && child.first <= *upper)
                {
                    match frontier.entry(child.node_id) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(Some(child));
                        }
                        std::collections::btree_map::Entry::Occupied(entry)
                            if entry.get().as_ref() == Some(&child) => {}
                        std::collections::btree_map::Entry::Occupied(_) => {
                            return Err(scoped_range_error(
                                "interval node has conflicting parent summaries",
                            ));
                        }
                    }
                }
            }
            let _ = node_id;
        }
    }
    Ok(())
}

fn stage_node(
    writes: &mut StorageWriteSet,
    compressor: &mut crate::compression::ZstdLevel1Compressor,
    node: ScopedRangeNode,
) -> Result<StagedNode, LixError> {
    let mut child = validate_node_with_summary(&node)?;
    let bytes = encode_node_with_compressor(&node, compressor)?;
    let node_id = node_digest(&bytes);
    child.node_id = node_id;
    if let Some(existing) = writes.staged_value(SCOPED_RANGE_NODE_SPACE, &node_id) {
        if existing != bytes {
            return Err(scoped_range_error(
                "content ID has conflicting staged bytes",
            ));
        }
    } else {
        writes.put(
            SCOPED_RANGE_NODE_SPACE,
            StorageKey(Bytes::copy_from_slice(&node_id)),
            StorageValue { bytes },
        );
    }
    Ok(StagedNode { child })
}

async fn load_node(
    store: &(impl StorageAdapterRead + ?Sized),
    node_id: [u8; 32],
) -> Result<ScopedRangeNode, LixError> {
    let result = PointReadPlan::new(
        SCOPED_RANGE_NODE_SPACE,
        &[StorageKey(Bytes::copy_from_slice(&node_id))],
    )
    .materialize(store, StorageGetOptions::default())
    .await?;
    let value = result
        .value
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| scoped_range_error("tree references a missing node"))?;
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(scoped_range_error("node read omitted its value"));
    };
    if node_digest(&bytes) != node_id {
        return Err(scoped_range_error("node content digest mismatch"));
    }
    decode_node(&bytes)
}

fn staged_node(
    writes: &StorageWriteSet,
    node_id: [u8; 32],
) -> Result<Option<ScopedRangeNode>, LixError> {
    if let Some(bytes) = writes.staged_value(SCOPED_RANGE_NODE_SPACE, &node_id) {
        if node_digest(&bytes) != node_id {
            return Err(scoped_range_error("staged node content digest mismatch"));
        }
        decode_node(&bytes).map(Some)
    } else {
        Ok(None)
    }
}

async fn load_nodes_with_staged(
    store: &(impl StorageAdapterRead + ?Sized),
    node_ids: &[[u8; 32]],
    mut staged: BTreeMap<[u8; 32], ScopedRangeNode>,
) -> Result<Vec<ScopedRangeNode>, LixError> {
    let missing = node_ids
        .iter()
        .copied()
        .filter(|node_id| !staged.contains_key(node_id))
        .collect::<Vec<_>>();
    let mut persisted = missing
        .iter()
        .copied()
        .zip(load_nodes(store, &missing).await?)
        .collect::<BTreeMap<_, _>>();
    node_ids
        .iter()
        .map(|node_id| {
            if let Some(node) = staged.remove(node_id) {
                Ok(node)
            } else {
                persisted
                    .remove(node_id)
                    .ok_or_else(|| scoped_range_error("node batch omitted a persisted node"))
            }
        })
        .collect()
}

async fn load_nodes(
    store: &(impl StorageAdapterRead + ?Sized),
    node_ids: &[[u8; 32]],
) -> Result<Vec<ScopedRangeNode>, LixError> {
    Ok(load_authenticated_nodes(store, node_ids)
        .await?
        .into_iter()
        .map(|loaded| loaded.node)
        .collect())
}

async fn load_authenticated_nodes(
    store: &(impl StorageAdapterRead + ?Sized),
    node_ids: &[[u8; 32]],
) -> Result<Vec<LoadedScopedRangeNode>, LixError> {
    if node_ids.is_empty() {
        return Ok(Vec::new());
    }
    let keys = node_ids
        .iter()
        .map(|node_id| StorageKey(Bytes::copy_from_slice(node_id)))
        .collect::<Vec<_>>();
    let result = PointReadPlan::new(SCOPED_RANGE_NODE_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    node_ids
        .iter()
        .zip(result.value)
        .map(|(node_id, value)| {
            let value =
                value.ok_or_else(|| scoped_range_error("tree references a missing node"))?;
            let StorageProjectedValue::FullValue(bytes) = value else {
                return Err(scoped_range_error("node read omitted its value"));
            };
            if node_digest(&bytes) != *node_id {
                return Err(scoped_range_error("node content digest mismatch"));
            }
            let node = decode_node(&bytes)?;
            let summary = authenticated_node_summary(&node, *node_id)?;
            Ok(LoadedScopedRangeNode { node, summary })
        })
        .collect()
}

fn encode_node_with_compressor(
    node: &ScopedRangeNode,
    compressor: &mut crate::compression::ZstdLevel1Compressor,
) -> Result<Bytes, LixError> {
    let stored = stored_node_ref(node)?;
    let payload = storage_codec::encode("scoped current-state range node", &stored)?;
    if payload.len() > MAX_NODE_DECODED_BYTES {
        return Err(scoped_range_error("node exceeds its decoded size bound"));
    }
    let compressed = compressor
        .compress(&payload)
        .map_err(|error| scoped_range_error(format!("node compression failed: {error}")))?;
    let compressed_len = NODE_ZSTD_MAGIC.len() + size_of::<u32>() + compressed.len();
    let raw_len = NODE_RAW_MAGIC.len() + payload.len();
    let mut bytes = Vec::with_capacity(compressed_len.min(raw_len));
    if compressed_len < raw_len {
        bytes.extend_from_slice(NODE_ZSTD_MAGIC);
        bytes.extend_from_slice(
            &u32::try_from(payload.len())
                .expect("bounded node payload fits u32")
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&compressed);
    } else {
        bytes.extend_from_slice(NODE_RAW_MAGIC);
        bytes.extend_from_slice(&payload);
    }
    Ok(Bytes::from(bytes))
}

fn decode_node(bytes: &[u8]) -> Result<ScopedRangeNode, LixError> {
    let payload = if let Some(payload) = bytes.strip_prefix(NODE_RAW_MAGIC) {
        if payload.len() > MAX_NODE_DECODED_BYTES {
            return Err(scoped_range_error("node exceeds its decoded size bound"));
        }
        payload.to_vec()
    } else if let Some(encoded) = bytes.strip_prefix(NODE_ZSTD_MAGIC) {
        let (raw_len, compressed) = encoded
            .split_at_checked(size_of::<u32>())
            .ok_or_else(|| scoped_range_error("compressed node omitted its decoded length"))?;
        let raw_len = usize::try_from(u32::from_be_bytes(
            raw_len
                .try_into()
                .expect("split compressed node length has four bytes"),
        ))
        .expect("u32 node length fits usize");
        if raw_len > MAX_NODE_DECODED_BYTES {
            return Err(scoped_range_error("node exceeds its decoded size bound"));
        }
        let payload = crate::compression::decompress_zstd(compressed, raw_len)
            .map_err(|error| scoped_range_error(format!("node decompression failed: {error}")))?;
        if payload.len() != raw_len {
            return Err(scoped_range_error(
                "compressed node decoded to the wrong length",
            ));
        }
        payload
    } else {
        return Err(scoped_range_error("node has an unsupported format"));
    };
    if payload.len() > MAX_NODE_DECODED_BYTES {
        return Err(scoped_range_error("node exceeds its decoded size bound"));
    }
    let stored: StoredScopedRangeNode =
        storage_codec::decode("scoped current-state range node", &payload)?;
    let node = runtime_node(stored)?;
    validate_node(&node)?;
    Ok(node)
}

fn stored_node_ref(node: &ScopedRangeNode) -> Result<StoredScopedRangeNodeRef<'_>, LixError> {
    if !node.children.is_empty() {
        return Ok(StoredScopedRangeNodeRef::Internal {
            level: node.level,
            children: node.children.iter().map(stored_child_ref).collect(),
        });
    }
    let mut runs = Vec::<StoredScopedRangeLeafRunRef<'_>>::with_capacity(1);
    for (index, entry) in node.entries.iter().enumerate() {
        let scope = entry.scope();
        if runs
            .last()
            .is_none_or(|run| run.scope != scope.encoded.as_ref())
        {
            let part_capacity = node.entries[index..]
                .iter()
                .take_while(|candidate| candidate.scope() == scope)
                .filter(|candidate| matches!(candidate, ScopedRangeEntry::Part(_)))
                .count();
            runs.push(StoredScopedRangeLeafRunRef {
                scope: scope.encoded.as_ref(),
                marker: None,
                parts: Vec::with_capacity(part_capacity),
            });
        }
        let run = runs.last_mut().expect("leaf entry created its scope run");
        match entry {
            ScopedRangeEntry::Marker(marker) => {
                if run.marker.is_some() || !run.parts.is_empty() {
                    return Err(scoped_range_error(
                        "leaf scope run has a misplaced or duplicate marker",
                    ));
                }
                run.marker = Some(StoredScopedRangeMarker {
                    row_count: marker.row_count,
                    part_count: marker.part_count,
                });
            }
            ScopedRangeEntry::Part(part) => run.parts.push(StoredScopedRangePartRef {
                first_key: &part.first_key,
                last_key: &part.last_key,
                row_count: part.row_count,
                payload: &part.payload,
            }),
        }
    }
    Ok(StoredScopedRangeNodeRef::Leaf { runs })
}

fn runtime_node(stored: StoredScopedRangeNode) -> Result<ScopedRangeNode, LixError> {
    match stored {
        StoredScopedRangeNode::Internal { level, children } => {
            let children = children
                .into_iter()
                .map(runtime_child)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ScopedRangeNode {
                level,
                entries: Vec::new(),
                children,
            })
        }
        StoredScopedRangeNode::Leaf { runs } => {
            if runs.is_empty() {
                return Err(scoped_range_error("leaf omitted its scope runs"));
            }
            let entry_count = runs.iter().try_fold(0usize, |count, run| {
                count
                    .checked_add(usize::from(run.marker.is_some()))
                    .and_then(|count| count.checked_add(run.parts.len()))
                    .ok_or_else(|| scoped_range_error("leaf entry count overflows"))
            })?;
            let mut entries = Vec::with_capacity(entry_count);
            let mut previous_scope: Option<Arc<[u8]>> = None;
            for run in runs {
                let scope = ScopedRangePrefix {
                    encoded: Arc::from(run.scope),
                };
                scope.validate()?;
                if previous_scope
                    .as_deref()
                    .is_some_and(|previous| previous >= scope.encoded.as_ref())
                {
                    return Err(scoped_range_error(
                        "leaf scope runs are duplicate or unordered",
                    ));
                }
                if run.marker.is_none() && run.parts.is_empty() {
                    return Err(scoped_range_error("leaf contains an empty scope run"));
                }
                if let Some(marker) = run.marker {
                    entries.push(ScopedRangeEntry::Marker(ScopedRangeCoverageMarker {
                        scope: scope.clone(),
                        row_count: marker.row_count,
                        part_count: marker.part_count,
                    }));
                }
                entries.extend(run.parts.into_iter().map(|part| {
                    ScopedRangeEntry::Part(ScopedRangePart {
                        scope: scope.clone(),
                        first_key: part.first_key,
                        last_key: part.last_key,
                        row_count: part.row_count,
                        payload: part.payload,
                    })
                }));
                previous_scope = Some(scope.encoded);
            }
            Ok(ScopedRangeNode {
                level: 0,
                entries,
                children: Vec::new(),
            })
        }
    }
}

fn runtime_route(route: StoredScopedRangeRoute) -> Result<ScopedRangeRoute, LixError> {
    let scope = ScopedRangePrefix {
        encoded: Arc::from(route.scope),
    };
    scope.validate()?;
    Ok(ScopedRangeRoute {
        scope,
        kind: route.kind,
        key: route.key,
    })
}

fn stored_route_ref(route: &ScopedRangeRoute) -> StoredScopedRangeRouteRef<'_> {
    StoredScopedRangeRouteRef {
        scope: route.scope.encoded.as_ref(),
        kind: route.kind,
        key: &route.key,
    }
}

fn stored_child_ref(child: &ScopedRangeChild) -> StoredScopedRangeChildRef<'_> {
    StoredScopedRangeChildRef {
        first: stored_route_ref(&child.first),
        last: stored_route_ref(&child.last),
        node_id: child.node_id,
        marker_count: child.marker_count,
        part_count: child.part_count,
        row_count: child.row_count,
        level: child.level,
    }
}

fn runtime_child(child: StoredScopedRangeChild) -> Result<ScopedRangeChild, LixError> {
    Ok(ScopedRangeChild {
        first: runtime_route(child.first)?,
        last: runtime_route(child.last)?,
        node_id: child.node_id,
        marker_count: child.marker_count,
        part_count: child.part_count,
        row_count: child.row_count,
        level: child.level,
    })
}

fn validate_node(node: &ScopedRangeNode) -> Result<(), LixError> {
    let is_leaf = node.children.is_empty();
    if is_leaf == node.entries.is_empty() || node.entries.len().max(node.children.len()) > FANOUT {
        return Err(scoped_range_error("node shape or fanout is invalid"));
    }
    if is_leaf {
        if node.level != 0
            || node
                .entries
                .windows(2)
                .any(|pair| compare_entry_routes(&pair[0], &pair[1]).is_ge())
        {
            return Err(scoped_range_error("leaf level or entry order is invalid"));
        }
        for entry in &node.entries {
            match entry {
                ScopedRangeEntry::Marker(marker) => marker.scope.validate()?,
                ScopedRangeEntry::Part(part) => validate_part(part)?,
            }
        }
    } else {
        let child_level = node.children[0].level;
        if node.level
            != child_level
                .checked_add(1)
                .ok_or_else(|| scoped_range_error("node level overflows"))?
            || node
                .children
                .iter()
                .any(|child| child.level != child_level || child.first > child.last)
            || node
                .children
                .windows(2)
                .any(|pair| pair[0].last >= pair[1].first)
        {
            return Err(scoped_range_error(
                "internal child levels or ranges are invalid",
            ));
        }
    }
    Ok(())
}

/// Staging needs both validation and the authenticated parent summary. Fold
/// counts while validating fields so rewritten nodes are not scanned a third
/// time before publication.
fn validate_node_with_summary(node: &ScopedRangeNode) -> Result<ScopedRangeChild, LixError> {
    let is_leaf = node.children.is_empty();
    if is_leaf == node.entries.is_empty() || node.entries.len().max(node.children.len()) > FANOUT {
        return Err(scoped_range_error("node shape or fanout is invalid"));
    }
    if is_leaf {
        if node.level != 0
            || node
                .entries
                .windows(2)
                .any(|pair| compare_entry_routes(&pair[0], &pair[1]).is_ge())
        {
            return Err(scoped_range_error("leaf level or entry order is invalid"));
        }
        let (marker_count, row_count) = node.entries.iter().try_fold(
            (0u32, 0u64),
            |(marker_count, row_count), entry| -> Result<_, LixError> {
                match entry {
                    ScopedRangeEntry::Marker(marker) => {
                        marker.scope.validate()?;
                        Ok((
                            marker_count
                                .checked_add(1)
                                .ok_or_else(|| scoped_range_error("marker count overflows"))?,
                            row_count,
                        ))
                    }
                    ScopedRangeEntry::Part(part) => {
                        validate_part(part)?;
                        Ok((
                            marker_count,
                            row_count
                                .checked_add(part.row_count)
                                .ok_or_else(|| scoped_range_error("row count overflows"))?,
                        ))
                    }
                }
            },
        )?;
        let part_count = u32::try_from(node.entries.len())
            .map_err(|_| scoped_range_error("part count overflows"))?
            .checked_sub(marker_count)
            .ok_or_else(|| scoped_range_error("marker count exceeds entry count"))?;
        Ok(ScopedRangeChild {
            first: node.entries[0].route(),
            last: node.entries[node.entries.len() - 1].route(),
            node_id: [0; 32],
            marker_count,
            part_count,
            row_count,
            level: 0,
        })
    } else {
        let child_level = node.children[0].level;
        if node.level
            != child_level
                .checked_add(1)
                .ok_or_else(|| scoped_range_error("node level overflows"))?
            || node
                .children
                .windows(2)
                .any(|pair| pair[0].last >= pair[1].first)
        {
            return Err(scoped_range_error(
                "internal child levels or ranges are invalid",
            ));
        }
        let (marker_count, part_count, row_count) = node.children.iter().try_fold(
            (0u32, 0u32, 0u64),
            |(markers, parts, rows), child| -> Result<_, LixError> {
                if child.level != child_level || child.first > child.last {
                    return Err(scoped_range_error(
                        "internal child levels or ranges are invalid",
                    ));
                }
                Ok((
                    markers
                        .checked_add(child.marker_count)
                        .ok_or_else(|| scoped_range_error("marker count overflows"))?,
                    parts
                        .checked_add(child.part_count)
                        .ok_or_else(|| scoped_range_error("part count overflows"))?,
                    rows.checked_add(child.row_count)
                        .ok_or_else(|| scoped_range_error("row count overflows"))?,
                ))
            },
        )?;
        Ok(ScopedRangeChild {
            first: node.children[0].first.clone(),
            last: node.children[node.children.len() - 1].last.clone(),
            node_id: [0; 32],
            marker_count,
            part_count,
            row_count,
            level: node.level,
        })
    }
}

fn validate_part(part: &ScopedRangePart) -> Result<(), LixError> {
    part.scope.validate()?;
    if part.first_key.is_empty()
        || part.first_key > part.last_key
        || part.row_count == 0
        || part.payload.version == 0
    {
        return Err(scoped_range_error(
            "part bounds, count, or payload version are invalid",
        ));
    }
    Ok(())
}

fn validate_entry_closure(entries: &[ScopedRangeEntry]) -> Result<(), LixError> {
    let mut index = 0usize;
    while index < entries.len() {
        let ScopedRangeEntry::Marker(marker) = &entries[index] else {
            return Err(scoped_range_error(
                "scope parts are not preceded by a marker",
            ));
        };
        marker.scope.validate()?;
        index += 1;
        let start = index;
        let mut rows = 0u64;
        let mut previous: Option<&ScopedRangePart> = None;
        while let Some(ScopedRangeEntry::Part(part)) = entries.get(index) {
            if part.scope != marker.scope {
                break;
            }
            validate_part(part)?;
            if previous.is_some_and(|previous| previous.last_key >= part.first_key) {
                return Err(scoped_range_error(
                    "scope parts overlap or repeat a boundary",
                ));
            }
            rows = rows
                .checked_add(part.row_count)
                .ok_or_else(|| scoped_range_error("scope row count overflows"))?;
            previous = Some(part);
            index += 1;
        }
        if index - start != marker.part_count as usize || rows != marker.row_count {
            return Err(scoped_range_error(
                "coverage marker does not close its parts",
            ));
        }
        if let Some(next) = entries.get(index) {
            let ScopedRangeEntry::Marker(next) = next else {
                return Err(scoped_range_error(
                    "part from another scope lacks its marker",
                ));
            };
            if next.scope <= marker.scope {
                return Err(scoped_range_error("coverage markers are unordered"));
            }
        }
    }
    Ok(())
}

/// Loaded nodes have already authenticated `node_id` against their exact
/// stored bytes. Routing therefore derives only the cheap logical summary and
/// binds the known ID instead of serializing and hashing the node again.
fn authenticated_node_summary(
    node: &ScopedRangeNode,
    node_id: [u8; 32],
) -> Result<ScopedRangeChild, LixError> {
    let mut child = node_summary_from_validated(node)?;
    child.node_id = node_id;
    Ok(child)
}

fn node_summary_from_validated(node: &ScopedRangeNode) -> Result<ScopedRangeChild, LixError> {
    if node.children.is_empty() {
        let first = node
            .entries
            .first()
            .expect("validated leaf is non-empty")
            .route();
        let last = node
            .entries
            .last()
            .expect("validated leaf is non-empty")
            .route();
        let (marker_count, row_count) = node.entries.iter().try_fold(
            (0u32, 0u64),
            |(marker_count, row_count), entry| -> Result<(u32, u64), LixError> {
                match entry {
                    ScopedRangeEntry::Marker(_) => Ok((
                        marker_count
                            .checked_add(1)
                            .ok_or_else(|| scoped_range_error("marker count overflows"))?,
                        row_count,
                    )),
                    ScopedRangeEntry::Part(part) => Ok((
                        marker_count,
                        row_count
                            .checked_add(part.row_count)
                            .ok_or_else(|| scoped_range_error("row count overflows"))?,
                    )),
                }
            },
        )?;
        let part_count = u32::try_from(node.entries.len())
            .map_err(|_| scoped_range_error("part count overflows"))?
            - marker_count;
        Ok(ScopedRangeChild {
            first,
            last,
            node_id: [0; 32],
            marker_count,
            part_count,
            row_count,
            level: 0,
        })
    } else {
        let (marker_count, part_count, row_count) = node.children.iter().try_fold(
            (0u32, 0u32, 0u64),
            |(markers, parts, rows), child| -> Result<(u32, u32, u64), LixError> {
                Ok((
                    markers
                        .checked_add(child.marker_count)
                        .ok_or_else(|| scoped_range_error("marker count overflows"))?,
                    parts
                        .checked_add(child.part_count)
                        .ok_or_else(|| scoped_range_error("part count overflows"))?,
                    rows.checked_add(child.row_count)
                        .ok_or_else(|| scoped_range_error("row count overflows"))?,
                ))
            },
        )?;
        Ok(ScopedRangeChild {
            first: node
                .children
                .first()
                .expect("validated internal node is non-empty")
                .first
                .clone(),
            last: node
                .children
                .last()
                .expect("validated internal node is non-empty")
                .last
                .clone(),
            node_id: [0; 32],
            marker_count,
            part_count,
            row_count,
            level: node.level,
        })
    }
}

fn marker_route(scope: &ScopedRangePrefix) -> ScopedRangeRoute {
    ScopedRangeRoute {
        scope: scope.clone(),
        kind: 0,
        key: Vec::new(),
    }
}

fn part_route(scope: &ScopedRangePrefix, key: &[u8]) -> ScopedRangeRoute {
    ScopedRangeRoute {
        scope: scope.clone(),
        kind: 1,
        key: key.to_vec(),
    }
}

fn compare_entry_routes(left: &ScopedRangeEntry, right: &ScopedRangeEntry) -> std::cmp::Ordering {
    compare_entry_to_route(left, right.scope(), entry_kind(right), entry_key(right))
}

fn compare_entry_to_owned_route(
    entry: &ScopedRangeEntry,
    route: &ScopedRangeRoute,
) -> std::cmp::Ordering {
    compare_entry_to_route(entry, &route.scope, route.kind, &route.key)
}

fn compare_entry_to_route(
    entry: &ScopedRangeEntry,
    scope: &ScopedRangePrefix,
    kind: u8,
    key: &[u8],
) -> std::cmp::Ordering {
    entry
        .scope()
        .cmp(scope)
        .then_with(|| entry_kind(entry).cmp(&kind))
        .then_with(|| entry_key(entry).cmp(key))
}

fn entry_kind(entry: &ScopedRangeEntry) -> u8 {
    match entry {
        ScopedRangeEntry::Marker(_) => 0,
        ScopedRangeEntry::Part(_) => 1,
    }
}

fn entry_key(entry: &ScopedRangeEntry) -> &[u8] {
    match entry {
        ScopedRangeEntry::Marker(_) => &[],
        ScopedRangeEntry::Part(part) => &part.first_key,
    }
}

fn balanced_chunks<T>(values: &[T]) -> Vec<&[T]> {
    if values.is_empty() {
        return Vec::new();
    }
    let groups = values.len().div_ceil(FANOUT);
    let base = values.len() / groups;
    let remainder = values.len() % groups;
    let mut start = 0;
    (0..groups)
        .map(|group| {
            let length = base + usize::from(group < remainder);
            let chunk = &values[start..start + length];
            start += length;
            chunk
        })
        .collect()
}

fn node_digest(bytes: &[u8]) -> [u8; 32] {
    *blake3::Hasher::new_derive_key(NODE_HASH_CONTEXT)
        .update(bytes)
        .finalize()
        .as_bytes()
}

fn root_digest(root_id: [u8; 32], markers: u32, parts: u32, rows: u64, height: u16) -> [u8; 32] {
    *blake3::Hasher::new_derive_key(ROOT_HASH_CONTEXT)
        .update(&root_id)
        .update(&markers.to_be_bytes())
        .update(&parts.to_be_bytes())
        .update(&rows.to_be_bytes())
        .update(&height.to_be_bytes())
        .finalize()
        .as_bytes()
}

fn validate_root_digest(root: &ScopedRangeRoot) -> Result<(), LixError> {
    if root.marker_count == 0
        || root.tree_height == 0
        || root.root_digest
            != root_digest(
                root.root_id,
                root.marker_count,
                root.part_count,
                root.row_count,
                root.tree_height,
            )
    {
        return Err(scoped_range_error("root digest or summary is invalid"));
    }
    Ok(())
}

fn take_u32(bytes: &mut &[u8], error: &'static str) -> Result<u32, LixError> {
    let (value, rest) = bytes
        .split_at_checked(4)
        .ok_or_else(|| scoped_range_error(error))?;
    *bytes = rest;
    Ok(u32::from_be_bytes(
        value.try_into().expect("checked four-byte value"),
    ))
}

fn scoped_range_error(message: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state scoped range {message}"),
    )
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::{Arc, Mutex};

    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    use super::*;

    struct CountingScopedRangeRead<R> {
        inner: R,
        batch_sizes: Arc<Mutex<Vec<usize>>>,
    }

    impl<R> StorageAdapterRead for CountingScopedRangeRead<R>
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
                if request.space == SCOPED_RANGE_NODE_SPACE {
                    self.batch_sizes
                        .lock()
                        .expect("scoped-range batch counts lock")
                        .push(request.keys.len());
                }
            }
            self.inner.get_many(requests)
        }

        fn begin_scan(
            &self,
            space: StorageSpace,
            range: crate::storage::KeyRange,
            opts: crate::storage::BeginScanOptions,
        ) -> impl Future<
            Output = Result<crate::storage::ScanCursor<'_>, crate::storage::StorageError>,
        > + Send {
            self.inner.begin_scan(space, range, opts)
        }
    }

    #[test]
    fn borrowed_entry_route_order_matches_canonical_owned_routes() {
        let alpha = scope("alpha");
        let beta = scope("beta");
        let entries = [
            ScopedRangeEntry::Marker(ScopedRangeCoverageMarker {
                scope: alpha.clone(),
                row_count: 0,
                part_count: 0,
            }),
            ScopedRangeEntry::Part(ScopedRangePart {
                scope: alpha.clone(),
                first_key: b"a".to_vec(),
                last_key: b"b".to_vec(),
                row_count: 1,
                payload: ScopedRangePartPayload {
                    version: 1,
                    bytes: Vec::new(),
                },
            }),
            ScopedRangeEntry::Part(ScopedRangePart {
                scope: alpha,
                first_key: b"c".to_vec(),
                last_key: b"d".to_vec(),
                row_count: 1,
                payload: ScopedRangePartPayload {
                    version: 1,
                    bytes: Vec::new(),
                },
            }),
            ScopedRangeEntry::Marker(ScopedRangeCoverageMarker {
                scope: beta,
                row_count: 0,
                part_count: 0,
            }),
        ];
        for left in &entries {
            for right in &entries {
                assert_eq!(
                    compare_entry_routes(left, right),
                    left.route().cmp(&right.route())
                );
                assert_eq!(
                    compare_entry_to_owned_route(left, &right.route()),
                    left.route().cmp(&right.route())
                );
            }
        }
    }

    fn scope(name: &str) -> ScopedRangePrefix {
        ScopedRangePrefix::try_from_components([b"schema".as_slice(), name.as_bytes()]).unwrap()
    }

    fn fixture(
        scope: ScopedRangePrefix,
        count: usize,
    ) -> (ScopedRangeCoverageMarker, Vec<ScopedRangePart>) {
        let parts = (0..count)
            .map(|index| ScopedRangePart {
                scope: scope.clone(),
                first_key: format!("{index:06}-a").into_bytes(),
                last_key: format!("{index:06}-z").into_bytes(),
                row_count: 10,
                payload: ScopedRangePartPayload {
                    version: 1,
                    bytes: index.to_be_bytes().to_vec(),
                },
            })
            .collect::<Vec<_>>();
        (
            ScopedRangeCoverageMarker {
                scope,
                row_count: count as u64 * 10,
                part_count: count as u32,
            },
            parts,
        )
    }

    #[test]
    fn scope_run_codec_borrows_bytes_and_shares_decoded_scope_identity() {
        let (marker, parts) = fixture(scope("run"), FANOUT - 1);
        let mut entries = Vec::with_capacity(FANOUT);
        entries.push(ScopedRangeEntry::Marker(marker));
        entries.extend(parts.into_iter().map(ScopedRangeEntry::Part));
        let node = ScopedRangeNode {
            level: 0,
            entries,
            children: Vec::new(),
        };

        let stored = stored_node_ref(&node).unwrap();
        let StoredScopedRangeNodeRef::Leaf { runs } = &stored else {
            panic!("leaf encoded as an internal node");
        };
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].parts.len(), FANOUT - 1);

        let bytes = storage_codec::encode("scoped current-state range node", &stored).unwrap();
        let owned: StoredScopedRangeNode =
            storage_codec::decode("scoped current-state range node", &bytes).unwrap();
        let decoded = runtime_node(owned).unwrap();
        assert_eq!(decoded, node);
        let ScopedRangeEntry::Marker(marker) = &decoded.entries[0] else {
            panic!("decoded run omitted its marker");
        };
        let ScopedRangeEntry::Part(part) = &decoded.entries[1] else {
            panic!("decoded run omitted its first part");
        };
        assert!(Arc::ptr_eq(&marker.scope.encoded, &part.scope.encoded));
    }

    #[test]
    fn scope_run_codec_rejects_empty_runs() {
        let stored = StoredScopedRangeNode::Leaf {
            runs: vec![StoredScopedRangeLeafRun {
                scope: scope("empty").encoded.to_vec(),
                marker: None,
                parts: Vec::new(),
            }],
        };
        assert!(runtime_node(stored).is_err());
    }

    #[test]
    fn scope_run_codec_rejects_duplicate_in_leaf_runs() {
        let duplicate = scope("duplicate").encoded.to_vec();
        let stored = StoredScopedRangeNode::Leaf {
            runs: vec![
                StoredScopedRangeLeafRun {
                    scope: duplicate.clone(),
                    marker: Some(StoredScopedRangeMarker {
                        row_count: 1,
                        part_count: 1,
                    }),
                    parts: Vec::new(),
                },
                StoredScopedRangeLeafRun {
                    scope: duplicate,
                    marker: None,
                    parts: vec![StoredScopedRangePart {
                        first_key: b"a".to_vec(),
                        last_key: b"z".to_vec(),
                        row_count: 1,
                        payload: ScopedRangePartPayload {
                            version: 1,
                            bytes: Vec::new(),
                        },
                    }],
                },
            ],
        };
        assert!(runtime_node(stored).is_err());
    }

    #[test]
    fn internal_borrowed_codec_matches_owned_protocol() {
        let children = ["alpha", "beta"]
            .into_iter()
            .enumerate()
            .map(|(index, name)| {
                let (marker, parts) = fixture(scope(name), 1);
                let mut entries = vec![ScopedRangeEntry::Marker(marker)];
                entries.extend(parts.into_iter().map(ScopedRangeEntry::Part));
                let mut child = validate_node_with_summary(&ScopedRangeNode {
                    level: 0,
                    entries,
                    children: Vec::new(),
                })
                .unwrap();
                child.node_id = [index as u8 + 1; 32];
                child
            })
            .collect::<Vec<_>>();
        let node = ScopedRangeNode {
            level: 1,
            entries: Vec::new(),
            children,
        };

        let stored = stored_node_ref(&node).unwrap();
        let StoredScopedRangeNodeRef::Internal { children, .. } = &stored else {
            panic!("internal node encoded as a leaf");
        };
        assert_eq!(children.len(), 2);
        let bytes = storage_codec::encode("scoped current-state range node", &stored).unwrap();
        let owned: StoredScopedRangeNode =
            storage_codec::decode("scoped current-state range node", &bytes).unwrap();
        assert_eq!(runtime_node(owned).unwrap(), node);
    }

    #[tokio::test]
    async fn routes_marker_predecessor_and_intervals_across_leaf_splits() {
        let adapter = StorageAdapter::new(Memory::new());
        let target = scope("target");
        let mut writes = adapter.new_write_set();
        let root = stage_scoped_range_tree(
            &mut writes,
            [
                fixture(scope("before"), 2),
                fixture(target.clone(), FANOUT * 2 + 7),
            ],
        )
        .unwrap();
        assert!(root.tree_height > 1);
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();

        let point = route_scoped_range_point(&read, &root, &target, b"000129-m")
            .await
            .unwrap();
        assert_eq!(point.coverage.unwrap().part_count, (FANOUT * 2 + 7) as u32);
        assert_eq!(point.predecessor.as_ref().unwrap().first_key, b"000129-a");
        assert!(point.covered_part.is_some());
        let gap = route_scoped_range_point(&read, &root, &target, b"000129-zz")
            .await
            .unwrap();
        assert!(gap.predecessor.is_some());
        assert!(gap.covered_part.is_none());

        let interval = scan_scoped_range_interval(&read, &root, &target, b"000127-m", b"000130-m")
            .await
            .unwrap();
        assert_eq!(interval.parts.len(), 4);
        let reachable = validate_scoped_range_tree(&read, &root).await.unwrap();
        assert_eq!(reachable.markers.len(), 2);
        assert_eq!(reachable.parts.len(), FANOUT * 2 + 9);
    }

    #[test]
    fn rejects_noncanonical_scope_and_marker_closure_drift() {
        let invalid = ScopedRangePrefix {
            encoded: vec![0, 0, 0, 1, 0, 0, 0, 2, 7].into(),
        };
        assert!(invalid.validate().is_err());
        let adapter = StorageAdapter::new(Memory::new());
        let (mut marker, parts) = fixture(scope("bad"), 2);
        marker.row_count += 1;
        assert!(stage_scoped_range_tree(&mut adapter.new_write_set(), [(marker, parts)]).is_err());
    }

    #[test]
    fn rejects_overlap_and_zero_version_payloads() {
        let adapter = StorageAdapter::new(Memory::new());
        let (marker, mut parts) = fixture(scope("bad"), 2);
        parts[1].first_key = parts[0].last_key.clone();
        assert!(stage_scoped_range_tree(&mut adapter.new_write_set(), [(marker, parts)]).is_err());
        let (marker, mut parts) = fixture(scope("version"), 1);
        parts[0].payload.version = 0;
        assert!(stage_scoped_range_tree(&mut adapter.new_write_set(), [(marker, parts)]).is_err());
    }

    #[tokio::test]
    async fn content_addressing_shares_an_identical_tree() {
        let adapter = StorageAdapter::new(Memory::new());
        let source = fixture(scope("stable"), FANOUT + 1);
        let mut first = adapter.new_write_set();
        let left = stage_scoped_range_tree(&mut first, [source.clone()]).unwrap();
        adapter
            .commit_write_set(first, StorageWriteOptions::default())
            .await
            .unwrap();
        let mut second = adapter.new_write_set();
        let right = stage_scoped_range_tree(&mut second, [source]).unwrap();
        assert_eq!(left, right);
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            validate_scoped_range_tree(&read, &left)
                .await
                .unwrap()
                .node_ids
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn full_scope_replacement_path_copies_and_reuses_untouched_children() {
        let adapter = StorageAdapter::new(Memory::new());
        let scopes = (0..300)
            .map(|index| fixture(scope(&format!("scope-{index:04}")), 2))
            .collect::<Vec<_>>();
        let mut writes = adapter.new_write_set();
        let root = stage_scoped_range_tree(&mut writes, scopes).unwrap();
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let target = scope("scope-0150");
        let mut rewrite_writes = adapter.new_write_set();
        let rewritten = stage_replace_scoped_range(
            &read,
            &mut rewrite_writes,
            &root,
            fixture(target.clone(), 5).0,
            fixture(target.clone(), 5).1,
        )
        .await
        .unwrap();
        assert!(rewritten.stats.loaded_nodes <= u32::from(root.tree_height) + 2);
        assert!(
            rewritten.stats.reused_children > 0,
            "rewrite stats: {:?}",
            rewritten.stats
        );
        adapter
            .commit_write_set(rewrite_writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let rewritten_read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let reachable = validate_scoped_range_tree(&rewritten_read, &rewritten.root)
            .await
            .unwrap();
        let marker = reachable
            .markers
            .iter()
            .find(|marker| marker.scope == target)
            .unwrap();
        assert_eq!((marker.part_count, marker.row_count), (5, 50));
        assert_eq!(reachable.markers.len(), 300);
    }

    #[tokio::test]
    async fn present_empty_scope_is_distinct_from_absent_scope() {
        let adapter = StorageAdapter::new(Memory::new());
        let empty = scope("empty");
        let marker = ScopedRangeCoverageMarker {
            scope: empty.clone(),
            row_count: 0,
            part_count: 0,
        };
        let mut writes = adapter.new_write_set();
        let root = stage_scoped_range_tree(&mut writes, [(marker, Vec::new())]).unwrap();
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let present = route_scoped_range_point(&read, &root, &empty, b"key")
            .await
            .unwrap();
        assert!(present.coverage.is_some());
        assert!(present.predecessor.is_none());
        let absent = route_scoped_range_point(&read, &root, &scope("missing"), b"key")
            .await
            .unwrap();
        assert!(absent.coverage.is_none());
    }

    #[tokio::test]
    async fn scope_equality_prunes_shared_state_and_ignores_other_scopes() {
        let adapter = StorageAdapter::new(Memory::new());
        let target = scope("equality-target");
        let other = scope("equality-other");
        let mut writes = adapter.new_write_set();
        let root = stage_scoped_range_tree(
            &mut writes,
            [
                fixture(target.clone(), FANOUT * 2 + 7),
                fixture(other.clone(), FANOUT + 3),
            ],
        )
        .unwrap();
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let equality_batches = Arc::new(Mutex::new(Vec::new()));
        let read = CountingScopedRangeRead {
            inner: adapter
                .begin_read(StorageReadOptions::default())
                .await
                .unwrap(),
            batch_sizes: Arc::clone(&equality_batches),
        };
        assert_eq!(
            prove_scoped_range_scope_equal(&read, &root, &root, &target)
                .await
                .unwrap(),
            ScopedRangeScopeEqualityProof::Equal
        );
        assert_eq!(
            equality_batches
                .lock()
                .expect("scoped-range equality batch counts lock")
                .len(),
            usize::from(root.tree_height),
            "an identical root must read only the marker path"
        );
        assert_eq!(
            prove_scoped_range_scope_equal(&read, &root, &root, &scope("missing"))
                .await
                .unwrap(),
            ScopedRangeScopeEqualityProof::NotProven
        );

        let mut rewrite_writes = adapter.new_write_set();
        let rewritten = stage_replace_scoped_range(
            &read,
            &mut rewrite_writes,
            &root,
            fixture(other.clone(), 3).0,
            fixture(other, 3).1,
        )
        .await
        .unwrap();
        adapter
            .commit_write_set(rewrite_writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let rewritten_read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            prove_scoped_range_scope_equal(&rewritten_read, &root, &rewritten.root, &target)
                .await
                .unwrap(),
            ScopedRangeScopeEqualityProof::Equal,
            "a different scope must not invalidate the target proof"
        );
    }

    #[tokio::test]
    async fn scope_equality_is_inconclusive_for_sparse_changes_and_unequal_heights() {
        let adapter = StorageAdapter::new(Memory::new());
        let target = scope("equality-change");
        let mut writes = adapter.new_write_set();
        let left =
            stage_scoped_range_tree(&mut writes, [fixture(target.clone(), FANOUT + 1)]).unwrap();
        let mut changed = fixture(target.clone(), FANOUT + 1);
        changed.1[1].payload.bytes.push(99);
        let right = stage_scoped_range_tree(&mut writes, [changed]).unwrap();
        let short =
            stage_scoped_range_tree(&mut writes, [fixture(target.clone(), FANOUT - 1)]).unwrap();
        assert_ne!(left.tree_height, short.tree_height);
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        for candidate in [&right, &short] {
            assert_eq!(
                prove_scoped_range_scope_equal(&read, &left, candidate, &target)
                    .await
                    .unwrap(),
                ScopedRangeScopeEqualityProof::NotProven
            );
        }
    }

    #[tokio::test]
    async fn interval_scan_prunes_children_before_batched_frontier_reads() {
        let adapter = StorageAdapter::new(Memory::new());
        let target = scope("interval-batch");
        let mut writes = adapter.new_write_set();
        let root = stage_scoped_range_tree(
            &mut writes,
            [
                fixture(scope("interval-before"), FANOUT),
                fixture(target.clone(), FANOUT * 2 + 7),
                fixture(scope("interval-after"), FANOUT),
            ],
        )
        .unwrap();
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let read = CountingScopedRangeRead {
            inner: adapter
                .begin_read(StorageReadOptions::default())
                .await
                .unwrap(),
            batch_sizes: Arc::clone(&batch_sizes),
        };
        let mut parts = Vec::new();
        collect_route_interval(
            &read,
            &root,
            &marker_route(&target),
            &scope_end_route(&target),
            &mut parts,
        )
        .await
        .unwrap();
        assert_eq!(parts.len(), FANOUT * 2 + 7);
        let batches = batch_sizes
            .lock()
            .expect("scoped-range batch counts lock")
            .clone();
        assert_eq!(batches.len(), usize::from(root.tree_height));
        assert!(
            batches.iter().skip(1).any(|size| *size > 1),
            "overlapping siblings must share one frontier read: {batches:?}"
        );
    }

    #[tokio::test]
    async fn multi_root_reachability_loads_shared_nodes_once() {
        let adapter = StorageAdapter::new(Memory::new());
        let target = scope("reachability-target");
        let mut writes = adapter.new_write_set();
        let root = stage_scoped_range_tree(
            &mut writes,
            [
                fixture(scope("reachability-before"), FANOUT),
                fixture(target.clone(), FANOUT * 2 + 7),
                fixture(scope("reachability-after"), FANOUT),
            ],
        )
        .unwrap();
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let mut rewrite_writes = adapter.new_write_set();
        let rewritten = stage_replace_scoped_range(
            &read,
            &mut rewrite_writes,
            &root,
            fixture(target.clone(), FANOUT * 2 + 6).0,
            fixture(target, FANOUT * 2 + 6).1,
        )
        .await
        .unwrap();
        adapter
            .commit_write_set(rewrite_writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let read = CountingScopedRangeRead {
            inner: adapter
                .begin_read(StorageReadOptions::default())
                .await
                .unwrap(),
            batch_sizes: Arc::clone(&batch_sizes),
        };
        let reachable = validate_scoped_range_trees(&read, &[root, rewritten.root])
            .await
            .unwrap();
        assert_eq!(
            batch_sizes
                .lock()
                .expect("scoped-range batch counts lock")
                .iter()
                .sum::<usize>(),
            reachable.node_ids.len(),
            "every shared content ID must be read exactly once"
        );
    }

    #[tokio::test]
    async fn one_key_splice_in_large_scope_reads_paths_and_reuses_subtrees() {
        let adapter = StorageAdapter::new(Memory::new());
        let target = scope("large");
        let mut initial_writes = adapter.new_write_set();
        let root = stage_scoped_range_tree(
            &mut initial_writes,
            [fixture(target.clone(), FANOUT * FANOUT + 17)],
        )
        .unwrap();
        adapter
            .commit_write_set(initial_writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let mut splice_writes = adapter.new_write_set();
        let keys = [Bytes::from_static(b"008193-m")];
        let staged_nodes = snapshot_staged_scoped_range_nodes(&splice_writes).unwrap();
        let plan = plan_scoped_range_part_splice(
            &read,
            splice_writes.identity(),
            staged_nodes,
            &root,
            &target,
            &keys,
        )
        .await
        .unwrap();
        assert!(plan.leaf_count() <= 2);
        assert_eq!(
            plan.leaves
                .iter()
                .flat_map(|leaf| &leaf.key_indices)
                .copied()
                .collect::<Vec<_>>(),
            vec![0]
        );
        let marker = plan.coverage().clone();
        let mut replacement_parts = (0..plan.leaf_count())
            .map(|index| plan.leaf_parts(index).cloned().collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let target_leaf = plan
            .leaves
            .iter()
            .position(|leaf| !leaf.key_indices.is_empty())
            .unwrap();
        let changed = replacement_parts[target_leaf]
            .iter_mut()
            .find(|part| {
                part.first_key.as_slice() <= keys[0].as_ref()
                    && keys[0].as_ref() <= part.last_key.as_slice()
            })
            .unwrap();
        changed.payload.bytes.push(99);
        let rewritten =
            stage_scoped_range_part_splice(&mut splice_writes, plan, marker, replacement_parts)
                .unwrap();
        assert!(rewritten.stats.loaded_nodes <= u32::from(root.tree_height) * 2);
        assert!(rewritten.stats.reused_children > 0);
        assert!(rewritten.stats.staged_nodes <= u32::from(root.tree_height) * 3);
        adapter
            .commit_write_set(splice_writes, StorageWriteOptions::default())
            .await
            .unwrap();
        let rewritten_read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let point =
            route_scoped_range_point(&rewritten_read, &rewritten.root, &target, keys[0].as_ref())
                .await
                .unwrap();
        assert_eq!(point.covered_part.unwrap().payload.bytes.last(), Some(&99));
        validate_scoped_range_tree(&rewritten_read, &rewritten.root)
            .await
            .unwrap();
    }
}
