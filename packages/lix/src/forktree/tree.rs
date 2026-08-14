use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;

use crate::storage::StorageError;
use crate::storage::{CoreProjection, GetManyRequest, GetOptions, Key, ProjectedValue};
use crate::storage_adapter::StorageAdapterRead;

use super::codec::{Decoder, Encoder, corruption};
use super::model::{
    BlobChunkV1, BranchSnapshotV1, ChangeCatalogEntry, ChangeCatalogOwner, ChangeId,
    ChangeObjectV1, CommitCatalogEntry, CommitId, CommitObjectV1, UploadPartV1, UploadProgressV1,
    UploadSelectorV1,
};
use super::object::{ObjectDomain, ObjectId, decode_id, decode_object, encode_id, encode_object};

pub(crate) const RECEIPT_TREE_LEAF_ENTRIES: usize = 64;
pub(crate) const RECEIPT_TREE_FANOUT: usize = 32;
const CATALOG_TREE_LEAF_ENTRIES: usize = 64;
const CATALOG_TREE_FANOUT: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum TreeKind {
    CommitCatalog = 1,
    ChangeCatalog = 2,
    Receipt = 3,
    State = 4,
}

impl TreeKind {
    fn decode(value: u8) -> Result<Self, StorageError> {
        match value {
            1 => Ok(Self::CommitCatalog),
            2 => Ok(Self::ChangeCatalog),
            3 => Ok(Self::Receipt),
            4 => Ok(Self::State),
            _ => Err(corruption(format!("unknown ordered-tree kind {value}"))),
        }
    }

    fn limits(self) -> (usize, usize) {
        match self {
            Self::Receipt => (RECEIPT_TREE_LEAF_ENTRIES, RECEIPT_TREE_FANOUT),
            Self::CommitCatalog | Self::ChangeCatalog | Self::State => {
                (CATALOG_TREE_LEAF_ENTRIES, CATALOG_TREE_FANOUT)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ReceiptEntrySummary {
    byte_offset: u64,
    declared_len: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct TreeSummary {
    entry_count: u64,
    logical_bytes: u64,
    contiguous_prefix_bytes: u64,
    first_part: Option<u64>,
    last_part: Option<u64>,
    first_offset: u64,
    last_end: u64,
    fully_contiguous: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LeafEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    receipt: Option<ReceiptEntrySummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NodeRef {
    id: ObjectId,
    max_key: Vec<u8>,
    summary: TreeSummary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum NodeBody {
    Leaf(Vec<LeafEntry>),
    Internal(Vec<NodeRef>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Node {
    kind: TreeKind,
    summary: TreeSummary,
    body: NodeBody,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OrderedTreeRoot {
    pub(crate) object_id: ObjectId,
    pub(crate) entry_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReceiptTreeRoot {
    pub(crate) object_id: ObjectId,
    pub(crate) completed_part_count: u64,
    pub(crate) received_bytes: u64,
    pub(crate) contiguous_prefix_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ImmutableObjectSet {
    objects: BTreeMap<ObjectId, Bytes>,
}

impl ImmutableObjectSet {
    pub(crate) fn get(&self, id: ObjectId) -> Option<&Bytes> {
        self.objects.get(&id)
    }

    pub(crate) fn insert(&mut self, id: ObjectId, bytes: Bytes) -> Result<(), StorageError> {
        match self.objects.get(&id) {
            Some(existing) if existing != &bytes => Err(corruption(format!(
                "object id {id} was assigned two different encodings"
            ))),
            Some(_) => Ok(()),
            None => {
                self.objects.insert(id, bytes);
                Ok(())
            }
        }
    }

    pub(crate) fn extend(&mut self, other: Self) -> Result<(), StorageError> {
        for (id, bytes) in other.objects {
            self.insert(id, bytes)?;
        }
        Ok(())
    }

    pub(crate) fn remove(&mut self, id: ObjectId) {
        self.objects.remove(&id);
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (ObjectId, &Bytes)> {
        self.objects.iter().map(|(id, bytes)| (*id, bytes))
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TreeBuild {
    pub(crate) root: OrderedTreeRoot,
    pub(crate) objects: ImmutableObjectSet,
}

#[derive(Clone, Debug)]
pub(crate) struct ReceiptTreeEdit {
    pub(crate) root: ReceiptTreeRoot,
    pub(crate) objects: ImmutableObjectSet,
    pub(crate) copied_nodes: usize,
    pub(crate) inserted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OrderedTreeMutation {
    Insert { key: Vec<u8>, value: Vec<u8> },
    Update { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl OrderedTreeMutation {
    fn key(&self) -> &[u8] {
        match self {
            Self::Insert { key, .. } | Self::Update { key, .. } | Self::Delete { key } => key,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OrderedTreeEdit {
    pub(crate) root: OrderedTreeRoot,
    pub(crate) objects: ImmutableObjectSet,
    pub(crate) copied_nodes: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct OrderedTreeRangeDelete {
    pub(crate) root: OrderedTreeRoot,
    pub(crate) objects: ImmutableObjectSet,
    pub(crate) copied_nodes: usize,
    pub(crate) deleted_entries: u64,
}

/// Authenticated outgoing object edges from one ordered-tree node.
///
/// Internal nodes contribute child edges. Leaf edges are decoded according to
/// the node's authenticated kind, so reachability never has a second decoder
/// for state, catalog, or receipt values.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct OrderedTreeEdges {
    pub(super) object_ids: Vec<(ObjectId, ObjectDomain)>,
    pub(super) commit_entries: Vec<(CommitId, CommitCatalogEntry)>,
    pub(super) change_entries: Vec<(ChangeId, ChangeCatalogEntry)>,
}

pub(super) fn ordered_tree_edges(
    id: ObjectId,
    bytes: &[u8],
) -> Result<OrderedTreeEdges, StorageError> {
    let node = decode_node(id, bytes)?;
    let mut object_ids = Vec::new();
    let mut commit_entries = Vec::new();
    let mut change_entries = Vec::new();
    match node.body {
        NodeBody::Internal(children) => {
            object_ids.extend(
                children
                    .into_iter()
                    .map(|child| (child.id, ObjectDomain::OrderedTreeNode)),
            );
        }
        NodeBody::Leaf(entries) => {
            for entry in entries {
                match node.kind {
                    TreeKind::State => {
                        let value = super::state::decode_state_value(&entry.value)
                            .map_err(|error| corruption(error.to_string()))?;
                        object_ids.push((value.pack_object_id, ObjectDomain::CurrentStatePackV1));
                    }
                    TreeKind::CommitCatalog => {
                        let key = CommitId::from_bytes(
                            entry
                                .key
                                .as_slice()
                                .try_into()
                                .map_err(|_| corruption("CommitCatalog key is not a UUID"))?,
                        );
                        let value = CommitCatalogEntry::decode(&entry.value)?;
                        object_ids.push((value.commit_object_id, ObjectDomain::CommitV2));
                        commit_entries.push((key, value));
                    }
                    TreeKind::ChangeCatalog => {
                        let key = ChangeId::from_bytes(
                            entry
                                .key
                                .as_slice()
                                .try_into()
                                .map_err(|_| corruption("ChangeCatalog key is not a UUID"))?,
                        );
                        let value = ChangeCatalogEntry::decode(&entry.value)?;
                        match value.owner {
                            ChangeCatalogOwner::CommitMember {
                                commit_object_id, ..
                            } => {
                                object_ids.push((commit_object_id, ObjectDomain::CommitV2));
                            }
                            ChangeCatalogOwner::BranchRef {
                                ref_change_object_id,
                                ..
                            } => {
                                object_ids
                                    .push((ref_change_object_id, ObjectDomain::BranchRefChange));
                            }
                            ChangeCatalogOwner::PackedCommit {
                                commit_object_id, ..
                            } => {
                                object_ids.push((commit_object_id, ObjectDomain::CommitV2));
                            }
                        }
                        change_entries.push((key, value));
                    }
                    TreeKind::Receipt => {
                        let id = ObjectId::from_bytes(
                            entry
                                .value
                                .as_slice()
                                .try_into()
                                .map_err(|_| corruption("tree object edge is not 32 bytes"))?,
                        );
                        object_ids.push((id, ObjectDomain::UploadPart));
                    }
                }
            }
        }
    }
    if object_ids.iter().any(|(id, _)| *id == ObjectId::ZERO) {
        return Err(corruption("ordered-tree node contains a zero object edge"));
    }
    object_ids.sort_unstable_by_key(|(id, domain)| (*id, domain.code()));
    if object_ids
        .windows(2)
        .any(|pair| pair[0].0 == pair[1].0 && pair[0].1 != pair[1].1)
    {
        return Err(corruption(
            "ordered-tree object edge has conflicting authenticated domains",
        ));
    }
    object_ids.dedup();
    Ok(OrderedTreeEdges {
        object_ids,
        commit_entries,
        change_entries,
    })
}

pub(super) fn empty_receipt_tree() -> Result<ReceiptTreeEdit, StorageError> {
    let mut objects = ImmutableObjectSet::default();
    let root = stage_leaf(TreeKind::Receipt, &[], &mut objects)?;
    Ok(ReceiptTreeEdit {
        root: receipt_root(&root),
        objects,
        copied_nodes: 1,
        inserted: true,
    })
}

pub(super) fn build_commit_catalog(
    entries: &[(CommitId, CommitCatalogEntry)],
) -> Result<TreeBuild, StorageError> {
    let mut encoded = Vec::with_capacity(entries.len());
    for (id, value) in entries {
        encoded.push(LeafEntry {
            key: id.as_bytes().to_vec(),
            value: value.encode()?,
            receipt: None,
        });
    }
    build_tree(TreeKind::CommitCatalog, &encoded)
}

pub(super) fn build_change_catalog(
    entries: &[(ChangeId, ChangeCatalogEntry)],
) -> Result<TreeBuild, StorageError> {
    let mut encoded = Vec::with_capacity(entries.len());
    for (id, value) in entries {
        encoded.push(LeafEntry {
            key: id.as_bytes().to_vec(),
            value: value.encode()?,
            receipt: None,
        });
    }
    build_tree(TreeKind::ChangeCatalog, &encoded)
}

pub(super) fn build_state_tree(entries: &[(Vec<u8>, Vec<u8>)]) -> Result<TreeBuild, StorageError> {
    let entries = entries
        .iter()
        .map(|(key, value)| {
            let _ = super::state::decode_state_key(key)
                .map_err(|error| corruption(error.to_string()))?;
            let _ = super::state::decode_state_value(value)
                .map_err(|error| corruption(error.to_string()))?;
            Ok(LeafEntry {
                key: key.clone(),
                value: value.clone(),
                receipt: None,
            })
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    build_tree(TreeKind::State, &entries)
}

/// Applies distinct, sorted mutations by copying only each affected root-to-leaf
/// path. Operation-local intermediate nodes are removed from the returned
/// object set, so only objects reachable from the final root are published.
///
/// Time is `O(U log_F N + copied blocks)` for `U` mutations and memory is
/// `O(U log_F N)` before final reachable-object pruning. The caller may lower
/// mutations in smaller sorted windows when it needs a stricter memory bound.
pub(super) async fn apply_ordered_mutations<R>(
    root: OrderedTreeRoot,
    expected_kind: &'static str,
    mutations: &[OrderedTreeMutation],
    read: &R,
) -> Result<OrderedTreeEdit, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    apply_ordered_mutations_with_policy(root, expected_kind, mutations, read, false).await
}

/// Applies one authenticated batch of ordered mutations. Each affected node
/// is loaded at most once, then all mutations for that leaf are merged before
/// any ancestor is encoded. This keeps the tree as the sole authority while
/// avoiding one authenticated path-copy/object encoding per row.
pub(super) async fn apply_ordered_mutations_idempotent_inserts<R>(
    root: OrderedTreeRoot,
    expected_kind: &'static str,
    mutations: &[OrderedTreeMutation],
    read: &R,
) -> Result<OrderedTreeEdit, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    apply_ordered_mutations_with_policy(root, expected_kind, mutations, read, true).await
}

async fn apply_ordered_mutations_with_policy<R>(
    root: OrderedTreeRoot,
    expected_kind: &'static str,
    mutations: &[OrderedTreeMutation],
    read: &R,
    idempotent_inserts: bool,
) -> Result<OrderedTreeEdit, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if mutations
        .windows(2)
        .any(|pair| pair[0].key() >= pair[1].key())
    {
        return Err(corruption(
            "ordered-tree mutations are not strictly ordered and distinct",
        ));
    }
    let kind = parse_kind(expected_kind)?;
    if mutations.is_empty() {
        return Ok(OrderedTreeEdit {
            root,
            objects: ImmutableObjectSet::default(),
            copied_nodes: 0,
        });
    }
    let loaded = load_affected_nodes(root, kind, mutations, read).await?;
    let mut objects = ImmutableObjectSet::default();
    let mut copied_nodes = 0_usize;
    let rewritten = rewrite_batch_node(
        kind,
        root.object_id,
        None,
        mutations,
        &loaded,
        &mut objects,
        &mut copied_nodes,
        idempotent_inserts,
    )?;
    let mut roots = match rewritten.as_slice() {
        [] => vec![stage_leaf(kind, &[], &mut objects)?],
        _ => rewritten,
    };
    while roots.len() > 1 {
        roots = stage_internal_level(kind, &roots, &mut objects)?;
        copied_nodes = copied_nodes.saturating_add(roots.len());
    }
    let next_root = roots
        .pop()
        .ok_or_else(|| corruption("ordered-tree batch emitted no root"))?;
    let next_root = OrderedTreeRoot {
        object_id: next_root.id,
        entry_count: next_root.summary.entry_count,
    };
    retain_reachable_new_nodes(next_root.object_id, kind, &mut objects)?;
    Ok(OrderedTreeEdit {
        root: next_root,
        objects,
        copied_nodes,
    })
}

/// Deletes one authenticated half-open key range while copying only its two
/// boundary paths. Interior subtrees are removed by their authenticated
/// `entry_count` summaries; their leaves and values are never materialized.
pub(super) async fn delete_ordered_range<R>(
    root: OrderedTreeRoot,
    expected_kind: &'static str,
    lower: &[u8],
    upper: Option<&[u8]>,
    read: &R,
) -> Result<OrderedTreeRangeDelete, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if upper.is_some_and(|upper| lower >= upper) {
        return Err(corruption("ordered-tree delete range is empty or reversed"));
    }
    let kind = parse_kind(expected_kind)?;
    let mut objects = ImmutableObjectSet::default();
    let mut copied_nodes = 0;
    let rewritten = rewrite_range_node(
        kind,
        root.object_id,
        None,
        lower,
        upper,
        read,
        &mut objects,
        &mut copied_nodes,
    )
    .await?;
    let mut roots = match rewritten.refs.as_slice() {
        [] => vec![stage_leaf(kind, &[], &mut objects)?],
        _ => rewritten.refs,
    };
    while roots.len() > 1 {
        roots = stage_internal_level(kind, &roots, &mut objects)?;
        copied_nodes = copied_nodes.saturating_add(roots.len());
    }
    let next = roots
        .pop()
        .ok_or_else(|| corruption("ordered-tree range delete emitted no root"))?;
    let next_root = OrderedTreeRoot {
        object_id: next.id,
        entry_count: next.summary.entry_count,
    };
    if next_root.entry_count
        != root
            .entry_count
            .checked_sub(rewritten.deleted_entries)
            .ok_or_else(|| corruption("ordered-tree range delete count underflow"))?
    {
        return Err(corruption(
            "ordered-tree range delete count differs from authenticated root",
        ));
    }
    retain_reachable_new_nodes(next_root.object_id, kind, &mut objects)?;
    Ok(OrderedTreeRangeDelete {
        root: next_root,
        objects,
        copied_nodes,
        deleted_entries: rewritten.deleted_entries,
    })
}

struct RangeRewrite {
    refs: Vec<NodeRef>,
    deleted_entries: u64,
}

#[expect(clippy::too_many_arguments)]
fn rewrite_range_node<'a, R>(
    kind: TreeKind,
    id: ObjectId,
    expected: Option<NodeRef>,
    lower: &'a [u8],
    upper: Option<&'a [u8]>,
    read: &'a R,
    objects: &'a mut ImmutableObjectSet,
    copied_nodes: &'a mut usize,
) -> Pin<Box<dyn Future<Output = Result<RangeRewrite, StorageError>> + Send + 'a>>
where
    R: StorageAdapterRead + ?Sized + 'a,
{
    Box::pin(async move {
        let node = decode_node(id, &load_object_on_read(read, id).await?)?;
        validate_loaded_node(id, &node, kind, expected.as_ref())?;
        match &node.body {
            NodeBody::Leaf(original) => {
                let mut entries = Vec::with_capacity(original.len());
                let mut deleted_entries = 0_u64;
                for entry in original {
                    let in_range = entry.key.as_slice() >= lower
                        && upper.is_none_or(|upper| entry.key.as_slice() < upper);
                    if in_range {
                        deleted_entries = deleted_entries.saturating_add(1);
                    } else {
                        entries.push(entry.clone());
                    }
                }
                if deleted_entries == 0 {
                    return Ok(RangeRewrite {
                        refs: vec![node_ref(id, &node)],
                        deleted_entries: 0,
                    });
                }
                *copied_nodes = copied_nodes.saturating_add(1);
                Ok(RangeRewrite {
                    refs: if entries.is_empty() {
                        Vec::new()
                    } else {
                        stage_leaf_level(kind, &entries, objects)?
                    },
                    deleted_entries,
                })
            }
            NodeBody::Internal(children) => {
                let mut next = Vec::with_capacity(children.len());
                let mut previous_max: Option<&[u8]> = None;
                let mut deleted_entries = 0_u64;
                let mut changed = false;
                for child in children {
                    let before = child.max_key.as_slice() < lower;
                    let after = upper.is_some_and(|upper| {
                        previous_max.is_some_and(|previous| previous >= upper)
                    });
                    let lower_covers_child =
                        lower.is_empty() || previous_max.is_some_and(|previous| previous >= lower);
                    let upper_covers_child =
                        upper.is_none_or(|upper| child.max_key.as_slice() < upper);
                    if before || after {
                        next.push(child.clone());
                    } else if lower_covers_child && upper_covers_child {
                        deleted_entries = deleted_entries.saturating_add(child.summary.entry_count);
                        changed = true;
                    } else {
                        let rewritten = rewrite_range_node(
                            kind,
                            child.id,
                            Some(child.clone()),
                            lower,
                            upper,
                            read,
                            objects,
                            copied_nodes,
                        )
                        .await?;
                        if rewritten.refs.len() != 1 || rewritten.refs.first() != Some(child) {
                            changed = true;
                        }
                        deleted_entries = deleted_entries.saturating_add(rewritten.deleted_entries);
                        next.extend(rewritten.refs);
                    }
                    previous_max = Some(child.max_key.as_slice());
                }
                if !changed {
                    return Ok(RangeRewrite {
                        refs: vec![node_ref(id, &node)],
                        deleted_entries,
                    });
                }
                *copied_nodes = copied_nodes.saturating_add(1);
                Ok(RangeRewrite {
                    refs: match next.as_slice() {
                        [] => Vec::new(),
                        [only] => vec![only.clone()],
                        _ => stage_internal_level(kind, &next, objects)?,
                    },
                    deleted_entries,
                })
            }
        }
    })
}

async fn load_affected_nodes<R>(
    root: OrderedTreeRoot,
    kind: TreeKind,
    mutations: &[OrderedTreeMutation],
    read: &R,
) -> Result<BTreeMap<ObjectId, Node>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let mut loaded = BTreeMap::<ObjectId, Node>::new();
    let mut pending = vec![(root.object_id, None::<NodeRef>, 0usize, mutations.len())];
    while let Some((id, expected, start, end)) = pending.pop() {
        if let Some(node) = loaded.get(&id) {
            // A content-addressed node may be reached through more than one
            // authenticated edge. Reuse its decoded body only after checking
            // this edge's expected reference as well; the batch loader must
            // never turn an unchecked alias into an authority shortcut.
            validate_loaded_node(id, node, kind, expected.as_ref())?;
            if id == root.object_id && node.summary.entry_count != root.entry_count {
                return Err(corruption(
                    "ordered-tree root count does not match its authenticated node",
                ));
            }
            continue;
        }
        let node = decode_node(id, &load_object_on_read(read, id).await?)?;
        validate_loaded_node(id, &node, kind, expected.as_ref())?;
        if id == root.object_id && node.summary.entry_count != root.entry_count {
            return Err(corruption(
                "ordered-tree root count does not match its authenticated node",
            ));
        }
        if let NodeBody::Internal(children) = &node.body {
            let mut offset = start;
            while offset < end {
                let index = child_index(children, mutations[offset].key());
                let child_start = offset;
                offset += 1;
                while offset < end && child_index(children, mutations[offset].key()) == index {
                    offset += 1;
                }
                let child = children
                    .get(index)
                    .ok_or_else(|| corruption("ordered-tree mutation child index is invalid"))?;
                pending.push((child.id, Some(child.clone()), child_start, offset));
            }
        }
        loaded.insert(id, node);
    }
    Ok(loaded)
}

fn rewrite_batch_node(
    kind: TreeKind,
    id: ObjectId,
    expected: Option<&NodeRef>,
    mutations: &[OrderedTreeMutation],
    loaded: &BTreeMap<ObjectId, Node>,
    objects: &mut ImmutableObjectSet,
    copied_nodes: &mut usize,
    idempotent_inserts: bool,
) -> Result<Vec<NodeRef>, StorageError> {
    let node = loaded
        .get(&id)
        .ok_or_else(|| corruption("ordered-tree batch path references an unloaded node"))?;
    validate_loaded_node(id, node, kind, expected)?;
    match &node.body {
        NodeBody::Leaf(original_entries) => {
            let mut entries = original_entries.clone();
            let mut changed = false;
            for mutation in mutations {
                match entries.binary_search_by(|entry| entry.key.as_slice().cmp(mutation.key())) {
                    Ok(index) => match mutation {
                        OrderedTreeMutation::Insert { value, .. } if idempotent_inserts => {
                            if entries[index].value != *value {
                                return Err(corruption(
                                    "ordered-tree idempotent insert remaps an existing key",
                                ));
                            }
                        }
                        OrderedTreeMutation::Insert { .. } => {
                            return Err(StorageError::WriteConflict);
                        }
                        OrderedTreeMutation::Update { value, .. } => {
                            entries[index].value.clone_from(value);
                            changed = true;
                        }
                        OrderedTreeMutation::Delete { .. } => {
                            entries.remove(index);
                            changed = true;
                        }
                    },
                    Err(index) => match mutation {
                        OrderedTreeMutation::Insert { key, value } => {
                            entries.insert(
                                index,
                                LeafEntry {
                                    key: key.clone(),
                                    value: value.clone(),
                                    receipt: None,
                                },
                            );
                            changed = true;
                        }
                        OrderedTreeMutation::Update { .. } | OrderedTreeMutation::Delete { .. } => {
                            return Err(StorageError::WriteConflict);
                        }
                    },
                }
            }
            if !changed {
                return Ok(vec![node_ref(id, node)]);
            }
            *copied_nodes = copied_nodes.saturating_add(1);
            if entries.is_empty() {
                Ok(Vec::new())
            } else {
                stage_leaf_level(kind, &entries, objects)
            }
        }
        NodeBody::Internal(children) => {
            let mut next = Vec::with_capacity(children.len());
            let mut mutation_offset = 0usize;
            let mut changed = false;
            for (child_idx, child) in children.iter().enumerate() {
                let start = mutation_offset;
                while mutation_offset < mutations.len()
                    && child_index(children, mutations[mutation_offset].key()) == child_idx
                {
                    mutation_offset += 1;
                }
                let child_mutations = &mutations[start..mutation_offset];
                let rewritten = if child_mutations.is_empty() {
                    vec![child.clone()]
                } else {
                    rewrite_batch_node(
                        kind,
                        child.id,
                        Some(child),
                        child_mutations,
                        loaded,
                        objects,
                        copied_nodes,
                        idempotent_inserts,
                    )?
                };
                if rewritten.len() != 1 || rewritten.first() != Some(child) {
                    changed = true;
                }
                next.extend(rewritten);
            }
            if mutation_offset != mutations.len() {
                return Err(corruption(
                    "ordered-tree batch mutations were not routed to a child",
                ));
            }
            if !changed {
                return Ok(vec![node_ref(id, node)]);
            }
            *copied_nodes = copied_nodes.saturating_add(1);
            match next.as_slice() {
                [] => Ok(Vec::new()),
                [only] => Ok(vec![only.clone()]),
                _ => stage_internal_level(kind, &next, objects),
            }
        }
    }
}

pub(super) async fn lookup_on_read<R>(
    root: ObjectId,
    expected_kind: &'static str,
    key: &[u8],
    read: &R,
) -> Result<Option<Vec<u8>>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let kind = parse_kind(expected_kind)?;
    let mut current = root;
    let mut expected: Option<NodeRef> = None;
    loop {
        let node = decode_node(current, &load_object_on_read(read, current).await?)?;
        validate_loaded_node(current, &node, kind, expected.as_ref())?;
        match node.body {
            NodeBody::Leaf(entries) => {
                return Ok(entries
                    .binary_search_by(|entry| entry.key.as_slice().cmp(key))
                    .ok()
                    .map(|index| entries[index].value.clone()));
            }
            NodeBody::Internal(children) => {
                let index = child_index(&children, key);
                expected = Some(children[index].clone());
                current = children[index].id;
            }
        }
    }
}

/// Resolves exact keys by decoding each authenticated index node at most once
/// and batching every tree level through the caller's retained read. The
/// ordered tree remains the sole durable value owner: internal nodes name the
/// canonical leaf ObjectIds and only requested leaves are materialized.
pub(super) async fn lookup_many_on_read<R>(
    root: ObjectId,
    expected_kind: &'static str,
    keys: &[Vec<u8>],
    read: &R,
) -> Result<Vec<Option<Vec<u8>>>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let kind = parse_kind(expected_kind)?;
    let mut output = vec![None; keys.len()];
    let mut frontier = BTreeMap::<ObjectId, (Option<NodeRef>, Vec<usize>)>::new();
    frontier.insert(root, (None, (0..keys.len()).collect()));

    while !frontier.is_empty() {
        let ids = frontier.keys().copied().collect::<Vec<_>>();
        let objects = load_objects_many_on_read(read, &ids).await?;
        let mut next = BTreeMap::<ObjectId, (Option<NodeRef>, Vec<usize>)>::new();
        for (id, (expected, slots)) in frontier {
            let bytes = objects
                .get(&id)
                .ok_or_else(|| corruption(format!("ordered-tree object {id} is absent")))?;
            let node = decode_node(id, bytes)?;
            validate_loaded_node(id, &node, kind, expected.as_ref())?;
            match node.body {
                NodeBody::Leaf(entries) => {
                    for slot in slots {
                        output[slot] = entries
                            .binary_search_by(|entry| entry.key.as_slice().cmp(&keys[slot]))
                            .ok()
                            .map(|index| entries[index].value.clone());
                    }
                }
                NodeBody::Internal(children) => {
                    for slot in slots {
                        let child = children
                            .get(child_index(&children, &keys[slot]))
                            .ok_or_else(|| {
                                corruption("ordered-tree exact lookup child index is invalid")
                            })?
                            .clone();
                        match next.entry(child.id) {
                            std::collections::btree_map::Entry::Vacant(entry) => {
                                entry.insert((Some(child), vec![slot]));
                            }
                            std::collections::btree_map::Entry::Occupied(mut entry) => {
                                if entry.get().0.as_ref() != Some(&child) {
                                    return Err(corruption(
                                        "ordered-tree exact lookup has conflicting child edges",
                                    ));
                                }
                                entry.get_mut().1.push(slot);
                            }
                        }
                    }
                }
            }
        }
        frontier = next;
    }
    Ok(output)
}

pub(super) async fn validate_root_on_read<R>(
    root: ObjectId,
    expected_kind: &'static str,
    read: &R,
) -> Result<OrderedTreeRoot, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let kind = parse_kind(expected_kind)?;
    let node = decode_node(root, &load_object_on_read(read, root).await?)?;
    validate_loaded_node(root, &node, kind, None)?;
    Ok(OrderedTreeRoot {
        object_id: root,
        entry_count: node.summary.entry_count,
    })
}

pub(super) async fn validate_receipt_root_on_read<R>(
    root: ReceiptTreeRoot,
    read: &R,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let node = decode_node(
        root.object_id,
        &load_object_on_read(read, root.object_id).await?,
    )?;
    if node.kind != TreeKind::Receipt || receipt_root(&node_ref(root.object_id, &node)) != root {
        return Err(corruption(
            "receipt root summary does not match its authenticated node",
        ));
    }
    Ok(())
}

pub(super) fn validate_root_bytes(
    root: ObjectId,
    expected_kind: &'static str,
    bytes: &[u8],
) -> Result<OrderedTreeRoot, StorageError> {
    let kind = parse_kind(expected_kind)?;
    let node = decode_node(root, bytes)?;
    validate_loaded_node(root, &node, kind, None)?;
    Ok(OrderedTreeRoot {
        object_id: root,
        entry_count: node.summary.entry_count,
    })
}

/// Returns the next strict raw-key page after `start_after`. The descent seeks
/// directly to the containing leaf and then visits only enough ordered blocks
/// to fill the page, for `O(log_F M + page)` authenticated work.
pub(super) async fn scan_page_on_read<R>(
    root: ObjectId,
    expected_kind: &'static str,
    start_after: Option<&[u8]>,
    page_size: usize,
    read: &R,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if page_size == 0 {
        return Err(corruption("ordered-tree page size must be nonzero"));
    }
    let requested = page_size
        .checked_add(usize::from(start_after.is_some()))
        .ok_or_else(|| corruption("ordered-tree page size overflows usize"))?;
    let mut rows = scan_range_on_read(
        root,
        expected_kind,
        start_after,
        None,
        Some(requested),
        read,
    )
    .await?;
    if let Some(start_after) = start_after {
        rows.retain(|(key, _)| key.as_slice() > start_after);
    }
    rows.truncate(page_size);
    Ok(rows)
}

pub(super) async fn scan_bounded_page_on_read<R>(
    root: ObjectId,
    expected_kind: &'static str,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    start_after: Option<&[u8]>,
    page_size: usize,
    read: &R,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if page_size == 0 {
        return Err(corruption("ordered-tree page size must be nonzero"));
    }
    let effective_lower = match (lower, start_after) {
        (Some(lower), Some(start)) => Some(lower.max(start)),
        (Some(lower), None) => Some(lower),
        (None, Some(start)) => Some(start),
        (None, None) => None,
    };
    let requested = page_size
        .checked_add(usize::from(start_after.is_some()))
        .ok_or_else(|| corruption("ordered-tree page size overflows usize"))?;
    let mut rows = scan_range_on_read(
        root,
        expected_kind,
        effective_lower,
        upper,
        Some(requested),
        read,
    )
    .await?;
    if let Some(start_after) = start_after {
        rows.retain(|(key, _)| key.as_slice() > start_after);
    }
    rows.truncate(page_size);
    Ok(rows)
}

/// Authenticates and returns an ordered half-open range. Work is proportional
/// to visited tree blocks plus returned key/value bytes; unrelated subtrees
/// whose authenticated separator is below the lower bound are skipped.
pub(super) async fn scan_range_on_read<R>(
    root: ObjectId,
    expected_kind: &'static str,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
    limit: Option<usize>,
    read: &R,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if lower.zip(upper).is_some_and(|(lower, upper)| lower > upper) {
        return Err(corruption("ordered-tree range bounds are inverted"));
    }
    let kind = parse_kind(expected_kind)?;
    let mut output = Vec::new();
    let mut frontier = vec![(root, None)];
    while let Some((id, expected)) = frontier.pop() {
        if limit.is_some_and(|limit| output.len() >= limit) {
            break;
        }
        let node = decode_node(id, &load_object_on_read(read, id).await?)?;
        validate_loaded_node(id, &node, kind, expected.as_ref())?;
        match node.body {
            NodeBody::Leaf(entries) => {
                for entry in entries {
                    if lower.is_some_and(|lower| entry.key.as_slice() < lower) {
                        continue;
                    }
                    if upper.is_some_and(|upper| entry.key.as_slice() >= upper) {
                        break;
                    }
                    output.push((entry.key, entry.value));
                    if limit.is_some_and(|limit| output.len() >= limit) {
                        break;
                    }
                }
            }
            NodeBody::Internal(children) => {
                let first = lower.map_or(0, |lower| child_index(&children, lower));
                let last = upper.map_or(children.len() - 1, |upper| child_index(&children, upper));
                for child in children
                    .into_iter()
                    .take(last.saturating_add(1))
                    .skip(first)
                    .rev()
                {
                    frontier.push((child.id, Some(child)));
                }
            }
        }
    }
    if output.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(corruption("ordered-tree range is not globally ordered"));
    }
    Ok(output)
}

/// Authenticates several disjoint half-open ranges in one ordered-tree walk.
///
/// Every internal node shared by two requested ranges is loaded and decoded
/// once. Results remain grouped by request slot and retain intrinsic tree
/// order; no caller-side set, scan, or re-sort is required.
pub(super) async fn scan_ranges_on_read<R>(
    root: ObjectId,
    expected_kind: &'static str,
    ranges: &[(Vec<u8>, Option<Vec<u8>>)],
    read: &R,
) -> Result<Vec<Vec<(Vec<u8>, Vec<u8>)>>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    if ranges.is_empty() {
        return Ok(Vec::new());
    }
    for (index, (lower, upper)) in ranges.iter().enumerate() {
        if upper
            .as_deref()
            .is_some_and(|upper| lower.as_slice() > upper)
        {
            return Err(corruption("ordered-tree range bounds are inverted"));
        }
        if index > 0 {
            let previous_upper = ranges[index - 1]
                .1
                .as_deref()
                .ok_or_else(|| corruption("unbounded ordered-tree range precedes another range"))?;
            if previous_upper > lower.as_slice() {
                return Err(corruption(
                    "ordered-tree batch ranges overlap or are unordered",
                ));
            }
        }
    }

    let kind = parse_kind(expected_kind)?;
    let mut output = vec![Vec::new(); ranges.len()];
    let mut frontier_order = vec![root];
    let mut frontier = BTreeMap::<ObjectId, (Option<NodeRef>, Vec<usize>)>::new();
    frontier.insert(root, (None, (0..ranges.len()).collect()));

    while !frontier_order.is_empty() {
        let objects = load_objects_many_on_read(read, &frontier_order).await?;
        let mut next_order = Vec::new();
        let mut next = BTreeMap::<ObjectId, (Option<NodeRef>, Vec<usize>)>::new();
        for id in frontier_order {
            let (expected, slots) = frontier
                .remove(&id)
                .ok_or_else(|| corruption("ordered-tree range frontier is inconsistent"))?;
            let bytes = objects
                .get(&id)
                .ok_or_else(|| corruption(format!("ordered-tree object {id} is absent")))?;
            let node = decode_node(id, bytes)?;
            validate_loaded_node(id, &node, kind, expected.as_ref())?;
            match node.body {
                NodeBody::Leaf(entries) => {
                    for slot in slots {
                        let (lower, upper) = &ranges[slot];
                        let first = entries.partition_point(|entry| entry.key < *lower);
                        for entry in entries.iter().skip(first) {
                            if upper
                                .as_deref()
                                .is_some_and(|upper| entry.key.as_slice() >= upper)
                            {
                                break;
                            }
                            output[slot].push((entry.key.clone(), entry.value.clone()));
                        }
                    }
                }
                NodeBody::Internal(children) => {
                    for slot in slots {
                        let (lower, upper) = &ranges[slot];
                        let first = child_index(&children, lower);
                        let last = upper
                            .as_deref()
                            .map_or(children.len() - 1, |upper| child_index(&children, upper));
                        for child in children
                            .iter()
                            .take(last.saturating_add(1))
                            .skip(first)
                            .cloned()
                        {
                            match next.entry(child.id) {
                                std::collections::btree_map::Entry::Vacant(entry) => {
                                    next_order.push(child.id);
                                    entry.insert((Some(child), vec![slot]));
                                }
                                std::collections::btree_map::Entry::Occupied(mut entry) => {
                                    if entry.get().0.as_ref() != Some(&child) {
                                        return Err(corruption(
                                            "ordered-tree batch range has conflicting child edges",
                                        ));
                                    }
                                    if entry.get().1.last().copied() != Some(slot) {
                                        entry.get_mut().1.push(slot);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        frontier_order = next_order;
        frontier = next;
    }

    if output.iter().any(|rows| {
        rows.windows(2)
            .any(|pair| pair[0].0.as_slice() >= pair[1].0.as_slice())
    }) {
        return Err(corruption(
            "ordered-tree batch range is not globally ordered",
        ));
    }
    Ok(output)
}

/// Returns the state keys whose authenticated leaf value differs between two
/// ordered-tree roots.
///
/// The comparison is native to the content-addressed state tree. Equal node
/// object IDs skip the complete subtree. Otherwise, internal children are
/// aligned by their authenticated max-key ranges and only overlapping ranges
/// are compared; a range present on one side is compared with an empty side.
/// Leaves use a sorted two-pointer comparison. The returned keys retain the
/// encoded state-tree order, which is the canonical order for this primitive.
///
/// The traversal loads each distinct node at most once per invocation and
/// validates every reused node against each parent edge before it is used.
/// It does not resolve state-page or semantic-change objects: this is a
/// structural state-root diff only.
pub(crate) async fn diff_roots<R>(
    left_root: Option<ObjectId>,
    right_root: Option<ObjectId>,
    read: &R,
) -> Result<Vec<super::state::StateKey>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let mut cache = BTreeMap::<ObjectId, Arc<Node>>::new();
    let mut frontier = vec![DiffTask {
        left: root_diff_side(left_root),
        right: root_diff_side(right_root),
        span: DiffSpan::unbounded(),
    }];
    let mut changed_encoded_keys = Vec::new();

    while let Some(task) = frontier.pop() {
        if same_diff_object(&task.left, &task.right) {
            continue;
        }
        let left = load_diff_side(task.left, read, &mut cache).await?;
        let right = load_diff_side(task.right, read, &mut cache).await?;
        if same_diff_object(&left, &right) {
            continue;
        }

        let mut next = Vec::new();
        match (&left, &right) {
            (DiffSide::Empty, DiffSide::Empty) => {}
            (DiffSide::Loaded { node: left, .. }, DiffSide::Loaded { node: right, .. }) => {
                match (&left.body, &right.body) {
                    (NodeBody::Leaf(left), NodeBody::Leaf(right)) => append_leaf_diffs(
                        Some(left),
                        Some(right),
                        &task.span,
                        &mut changed_encoded_keys,
                    ),
                    (NodeBody::Internal(left), NodeBody::Internal(right)) => {
                        enqueue_internal_pair(left, right, &task.span, &mut next);
                    }
                    (NodeBody::Internal(left), NodeBody::Leaf(right)) => {
                        enqueue_internal_leaf(left, &right, &task.span, false, &mut next);
                    }
                    (NodeBody::Leaf(left), NodeBody::Internal(right)) => {
                        enqueue_internal_leaf(right, &left, &task.span, true, &mut next);
                    }
                }
            }
            (DiffSide::Loaded { node, .. }, DiffSide::Empty) => match &node.body {
                NodeBody::Leaf(entries) => {
                    append_leaf_diffs(Some(entries), None, &task.span, &mut changed_encoded_keys)
                }
                NodeBody::Internal(children) => {
                    enqueue_internal_empty(children, &task.span, false, &mut next)
                }
            },
            (DiffSide::Empty, DiffSide::Loaded { node, .. }) => match &node.body {
                NodeBody::Leaf(entries) => {
                    append_leaf_diffs(None, Some(entries), &task.span, &mut changed_encoded_keys)
                }
                NodeBody::Internal(children) => {
                    enqueue_internal_empty(children, &task.span, true, &mut next)
                }
            },
            (DiffSide::Leaf { entries }, DiffSide::Empty) => {
                append_leaf_diffs(Some(entries), None, &task.span, &mut changed_encoded_keys)
            }
            (DiffSide::Empty, DiffSide::Leaf { entries }) => {
                append_leaf_diffs(None, Some(entries), &task.span, &mut changed_encoded_keys)
            }
            (DiffSide::Leaf { entries: left }, DiffSide::Leaf { entries: right }) => {
                append_leaf_diffs(
                    Some(left),
                    Some(right),
                    &task.span,
                    &mut changed_encoded_keys,
                )
            }
            (DiffSide::Loaded { node, .. }, DiffSide::Leaf { entries }) => match &node.body {
                NodeBody::Leaf(left) => append_leaf_diffs(
                    Some(left),
                    Some(entries),
                    &task.span,
                    &mut changed_encoded_keys,
                ),
                NodeBody::Internal(left) => {
                    enqueue_internal_leaf(left, entries, &task.span, false, &mut next)
                }
            },
            (DiffSide::Leaf { entries }, DiffSide::Loaded { node, .. }) => match &node.body {
                NodeBody::Leaf(right) => append_leaf_diffs(
                    Some(entries),
                    Some(right),
                    &task.span,
                    &mut changed_encoded_keys,
                ),
                NodeBody::Internal(right) => {
                    enqueue_internal_leaf(right, entries, &task.span, true, &mut next)
                }
            },
            _ => unreachable!("diff side resolution leaves no unresolved node refs"),
        }
        frontier.extend(next.into_iter().rev());
    }

    if changed_encoded_keys
        .windows(2)
        .any(|pair| pair[0].as_slice() >= pair[1].as_slice())
    {
        return Err(corruption(
            "state-root diff emitted duplicate or out-of-order keys",
        ));
    }
    changed_encoded_keys
        .into_iter()
        .map(|key| {
            super::state::decode_state_key(&key)
                .map_err(|error| corruption(format!("state-root diff key is invalid: {error}")))
        })
        .collect()
}

#[derive(Clone)]
struct DiffTask {
    left: DiffSide,
    right: DiffSide,
    span: DiffSpan,
}

#[derive(Clone)]
enum DiffSide {
    Empty,
    Leaf {
        entries: Arc<Vec<LeafEntry>>,
    },
    Ref {
        id: ObjectId,
        expected: Option<NodeRef>,
    },
    Loaded {
        id: ObjectId,
        node: Arc<Node>,
    },
}

#[derive(Clone, Default)]
struct DiffSpan {
    lower_exclusive: Option<Vec<u8>>,
    upper_inclusive: Option<Vec<u8>>,
}

impl DiffSpan {
    fn unbounded() -> Self {
        Self::default()
    }

    fn valid(&self) -> bool {
        self.lower_exclusive
            .as_ref()
            .zip(self.upper_inclusive.as_ref())
            .is_none_or(|(lower, upper)| lower < upper)
    }
}

fn root_diff_side(root: Option<ObjectId>) -> DiffSide {
    root.map_or(DiffSide::Empty, |id| DiffSide::Ref { id, expected: None })
}

fn same_diff_object(left: &DiffSide, right: &DiffSide) -> bool {
    match (left, right) {
        (
            DiffSide::Ref {
                id: left,
                expected: left_expected,
            },
            DiffSide::Ref {
                id: right,
                expected: right_expected,
            },
        ) => left == right && left_expected == right_expected,
        (DiffSide::Loaded { id: left, .. }, DiffSide::Loaded { id: right, .. }) => left == right,
        (
            DiffSide::Ref {
                id: left,
                expected: None,
            },
            DiffSide::Loaded { id: right, .. },
        )
        | (
            DiffSide::Loaded { id: left, .. },
            DiffSide::Ref {
                id: right,
                expected: None,
            },
        ) => left == right,
        _ => false,
    }
}

async fn load_diff_side<R>(
    side: DiffSide,
    read: &R,
    cache: &mut BTreeMap<ObjectId, Arc<Node>>,
) -> Result<DiffSide, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let DiffSide::Ref { id, expected } = side else {
        return Ok(side);
    };
    if id == ObjectId::ZERO {
        return Err(corruption("state-root diff references a zero root object"));
    }
    let node = match cache.get(&id) {
        Some(node) => Arc::clone(node),
        None => {
            let node = Arc::new(decode_node(id, &load_object_on_read(read, id).await?)?);
            cache.insert(id, Arc::clone(&node));
            node
        }
    };
    validate_loaded_node(id, &node, TreeKind::State, expected.as_ref())?;
    Ok(DiffSide::Loaded { id, node })
}

fn append_leaf_diffs(
    left: Option<&[LeafEntry]>,
    right: Option<&[LeafEntry]>,
    span: &DiffSpan,
    output: &mut Vec<Vec<u8>>,
) {
    let left = left.unwrap_or_default();
    let right = right.unwrap_or_default();
    let mut left_index = first_leaf_index(left, span.lower_exclusive.as_deref());
    let mut right_index = first_leaf_index(right, span.lower_exclusive.as_deref());
    while left_index < left.len() || right_index < right.len() {
        let left_key = left.get(left_index).map(|entry| entry.key.as_slice());
        let right_key = right.get(right_index).map(|entry| entry.key.as_slice());
        match (left_key, right_key) {
            (Some(left_key), Some(right_key)) if left_key == right_key => {
                if left[left_index].value != right[right_index].value
                    && key_in_diff_span(left_key, span)
                {
                    output.push(left[left_index].key.clone());
                }
                left_index += 1;
                right_index += 1;
            }
            (Some(left_key), Some(right_key)) if left_key < right_key => {
                if key_in_diff_span(left_key, span) {
                    output.push(left[left_index].key.clone());
                }
                left_index += 1;
            }
            (Some(_), Some(right_key)) => {
                if key_in_diff_span(right_key, span) {
                    output.push(right[right_index].key.clone());
                }
                right_index += 1;
            }
            (Some(left_key), None) => {
                if key_in_diff_span(left_key, span) {
                    output.push(left[left_index].key.clone());
                }
                left_index += 1;
            }
            (None, Some(right_key)) => {
                if key_in_diff_span(right_key, span) {
                    output.push(right[right_index].key.clone());
                }
                right_index += 1;
            }
            (None, None) => break,
        }
    }
}

fn first_leaf_index(entries: &[LeafEntry], lower_exclusive: Option<&[u8]>) -> usize {
    lower_exclusive.map_or(0, |lower| {
        entries.partition_point(|entry| entry.key.as_slice() <= lower)
    })
}

fn key_in_diff_span(key: &[u8], span: &DiffSpan) -> bool {
    span.lower_exclusive
        .as_deref()
        .is_none_or(|lower| key > lower)
        && span
            .upper_inclusive
            .as_deref()
            .is_none_or(|upper| key <= upper)
}

fn enqueue_internal_empty(
    children: &[NodeRef],
    span: &DiffSpan,
    empty_on_left: bool,
    output: &mut Vec<DiffTask>,
) {
    for (child_span, child) in internal_segments(children, span) {
        let (left, right) = if empty_on_left {
            (DiffSide::Empty, child)
        } else {
            (child, DiffSide::Empty)
        };
        output.push(DiffTask {
            left,
            right,
            span: child_span,
        });
    }
}

fn enqueue_internal_leaf(
    internal: &[NodeRef],
    leaf: &[LeafEntry],
    span: &DiffSpan,
    leaf_on_left: bool,
    output: &mut Vec<DiffTask>,
) {
    let leaf = Arc::new(leaf.to_vec());
    for (child_span, child) in internal_segments(internal, span) {
        let (left, right) = if leaf_on_left {
            (
                DiffSide::Leaf {
                    entries: Arc::clone(&leaf),
                },
                child,
            )
        } else {
            (
                child,
                DiffSide::Leaf {
                    entries: Arc::clone(&leaf),
                },
            )
        };
        output.push(DiffTask {
            left,
            right,
            span: child_span,
        });
    }
}

fn enqueue_internal_pair(
    left: &[NodeRef],
    right: &[NodeRef],
    span: &DiffSpan,
    output: &mut Vec<DiffTask>,
) {
    let mut left = internal_segments(left, span);
    let mut right = internal_segments(right, span);
    let mut left_index = 0;
    let mut right_index = 0;
    while left_index < left.len() || right_index < right.len() {
        match (left.get(left_index), right.get(right_index)) {
            (Some((left_span, left_side)), Some((right_span, right_side)))
                if span_before_or_at(
                    left_span.upper_inclusive.as_deref(),
                    right_span.lower_exclusive.as_deref(),
                ) =>
            {
                output.push(DiffTask {
                    left: left_side.clone(),
                    right: DiffSide::Empty,
                    span: left_span.clone(),
                });
                left_index += 1;
            }
            (Some((left_span, left_side)), Some((right_span, right_side)))
                if span_before_or_at(
                    right_span.upper_inclusive.as_deref(),
                    left_span.lower_exclusive.as_deref(),
                ) =>
            {
                output.push(DiffTask {
                    left: DiffSide::Empty,
                    right: right_side.clone(),
                    span: right_span.clone(),
                });
                right_index += 1;
            }
            (Some((left_span, left_side)), Some((right_span, right_side))) => {
                let overlap = intersect_diff_spans(left_span, right_span);
                if overlap.valid() {
                    output.push(DiffTask {
                        left: left_side.clone(),
                        right: right_side.clone(),
                        span: overlap,
                    });
                }
                match left_span
                    .upper_inclusive
                    .as_deref()
                    .cmp(&right_span.upper_inclusive.as_deref())
                {
                    std::cmp::Ordering::Less => {
                        left_index += 1;
                        right[right_index].0.lower_exclusive = left_span.upper_inclusive.clone();
                    }
                    std::cmp::Ordering::Greater => {
                        right_index += 1;
                        left[left_index].0.lower_exclusive = right_span.upper_inclusive.clone();
                    }
                    std::cmp::Ordering::Equal => {
                        left_index += 1;
                        right_index += 1;
                    }
                }
            }
            (Some((left_span, left_side)), None) => {
                output.push(DiffTask {
                    left: left_side.clone(),
                    right: DiffSide::Empty,
                    span: left_span.clone(),
                });
                left_index += 1;
            }
            (None, Some((right_span, right_side))) => {
                output.push(DiffTask {
                    left: DiffSide::Empty,
                    right: right_side.clone(),
                    span: right_span.clone(),
                });
                right_index += 1;
            }
            (None, None) => break,
        }
    }
}

fn internal_segments(children: &[NodeRef], parent: &DiffSpan) -> Vec<(DiffSpan, DiffSide)> {
    let mut segments = Vec::with_capacity(children.len());
    let mut previous_max = None;
    for child in children {
        let lower = max_diff_lower(parent.lower_exclusive.as_deref(), previous_max.as_deref());
        let upper = min_diff_upper(
            parent.upper_inclusive.as_deref(),
            Some(child.max_key.as_slice()),
        );
        let child_span = DiffSpan {
            lower_exclusive: lower,
            upper_inclusive: upper,
        };
        if child_span.valid() {
            segments.push((
                child_span,
                DiffSide::Ref {
                    id: child.id,
                    expected: Some(child.clone()),
                },
            ));
        }
        previous_max = Some(child.max_key.clone());
        if parent
            .upper_inclusive
            .as_deref()
            .is_some_and(|upper| child.max_key.as_slice() >= upper)
        {
            break;
        }
    }
    segments
}

fn intersect_diff_spans(left: &DiffSpan, right: &DiffSpan) -> DiffSpan {
    DiffSpan {
        lower_exclusive: max_diff_lower(
            left.lower_exclusive.as_deref(),
            right.lower_exclusive.as_deref(),
        ),
        upper_inclusive: min_diff_upper(
            left.upper_inclusive.as_deref(),
            right.upper_inclusive.as_deref(),
        ),
    }
}

fn max_diff_lower(left: Option<&[u8]>, right: Option<&[u8]>) -> Option<Vec<u8>> {
    match (left, right) {
        (None, None) => None,
        (Some(value), None) | (None, Some(value)) => Some(value.to_vec()),
        (Some(left), Some(right)) => Some(left.max(right).to_vec()),
    }
}

fn min_diff_upper(left: Option<&[u8]>, right: Option<&[u8]>) -> Option<Vec<u8>> {
    match (left, right) {
        (None, None) => None,
        (Some(value), None) | (None, Some(value)) => Some(value.to_vec()),
        (Some(left), Some(right)) => Some(left.min(right).to_vec()),
    }
}

fn span_before_or_at(upper: Option<&[u8]>, lower: Option<&[u8]>) -> bool {
    match (upper, lower) {
        (Some(upper), Some(lower)) => upper <= lower,
        _ => false,
    }
}

pub(super) fn insert_receipt_part(
    root: ReceiptTreeRoot,
    part_object_id: ObjectId,
    part: &UploadPartV1,
    load: impl Fn(ObjectId) -> Result<Bytes, StorageError>,
) -> Result<ReceiptTreeEdit, StorageError> {
    if part_object_id == ObjectId::ZERO {
        return Err(corruption("receipt tree part object id is zero"));
    }
    let key = part.part_number.to_be_bytes().to_vec();
    let entry = LeafEntry {
        key,
        value: part_object_id.as_bytes().to_vec(),
        receipt: Some(ReceiptEntrySummary {
            byte_offset: part.byte_offset,
            declared_len: part.declared_part_len,
        }),
    };
    let mut objects = ImmutableObjectSet::default();
    let mut copied_nodes = 0;
    let root_node = decode_node(root.object_id, &load(root.object_id)?)?;
    if root_node.kind != TreeKind::Receipt
        || receipt_root(&node_ref(root.object_id, &root_node)) != root
    {
        return Err(corruption(
            "receipt edit root summary does not match its authenticated node",
        ));
    }
    let rewritten = rewrite_insert(
        TreeKind::Receipt,
        root.object_id,
        None,
        &entry,
        &load,
        &mut objects,
        &mut copied_nodes,
    )?;
    let inserted = !(rewritten.len() == 1 && rewritten[0].id == root.object_id);
    let next = finish_root(
        TreeKind::Receipt,
        rewritten,
        &load,
        &mut objects,
        &mut copied_nodes,
    )?;
    let next_root = receipt_root(&next);
    if !inserted && next_root != root {
        return Err(corruption(
            "idempotent receipt insertion changed the authenticated root",
        ));
    }
    Ok(ReceiptTreeEdit {
        root: next_root,
        objects,
        copied_nodes,
        inserted,
    })
}

/// Applies one receipt edit using the caller's retained authenticated read.
/// The synchronous tree rewriter needs a lookup closure, so this adapter
/// eagerly loads only the existing receipt-node path set before it performs
/// the path copy. Newly staged nodes are supplied through `overlay` and are
/// never read from a second storage snapshot.
pub(super) async fn insert_receipt_part_on_read<R>(
    root: ReceiptTreeRoot,
    part_object_id: ObjectId,
    part: &UploadPartV1,
    read: &R,
    overlay: &ImmutableObjectSet,
) -> Result<ReceiptTreeEdit, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let mut loaded = BTreeMap::<ObjectId, Bytes>::new();
    let mut pending = vec![root.object_id];
    while let Some(id) = pending.pop() {
        if loaded.contains_key(&id) {
            continue;
        }
        let bytes = match overlay.get(id) {
            Some(bytes) => bytes.clone(),
            None => load_object_on_read(read, id).await?,
        };
        let node = decode_node(id, &bytes)?;
        if node.kind != TreeKind::Receipt {
            return Err(corruption("receipt edit encountered a non-receipt node"));
        }
        if let NodeBody::Internal(children) = &node.body {
            pending.extend(children.iter().map(|child| child.id));
        }
        loaded.insert(id, bytes);
    }
    insert_receipt_part(root, part_object_id, part, |id| {
        loaded
            .get(&id)
            .cloned()
            .ok_or_else(|| corruption("receipt edit path references an unloaded node"))
    })
}

pub(super) fn lookup(
    root: ObjectId,
    expected_kind: &'static str,
    key: &[u8],
    load: impl Fn(ObjectId) -> Result<Bytes, StorageError>,
) -> Result<Option<Vec<u8>>, StorageError> {
    let kind = parse_kind(expected_kind)?;
    let mut current = root;
    let mut expected: Option<NodeRef> = None;
    loop {
        let node = decode_node(current, &load(current)?)?;
        if node.kind != kind {
            return Err(corruption("ordered-tree root has the wrong kind"));
        }
        if expected
            .as_ref()
            .is_some_and(|expected| node_ref(current, &node) != *expected)
        {
            return Err(corruption(
                "ordered-tree point path does not match its authenticated parent reference",
            ));
        }
        match node.body {
            NodeBody::Leaf(entries) => {
                return Ok(entries
                    .binary_search_by(|entry| entry.key.as_slice().cmp(key))
                    .ok()
                    .map(|index| entries[index].value.clone()));
            }
            NodeBody::Internal(children) => {
                let index = children
                    .partition_point(|child| child.max_key.as_slice() < key)
                    .min(children.len().saturating_sub(1));
                expected = Some(children[index].clone());
                current = children[index].id;
            }
        }
    }
}

pub(super) fn scan_all(
    root: ObjectId,
    expected_kind: &'static str,
    load: impl Fn(ObjectId) -> Result<Bytes, StorageError>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
    let kind = parse_kind(expected_kind)?;
    let mut output = Vec::new();
    let mut frontier = vec![(root, None)];
    while let Some((id, expected)) = frontier.pop() {
        let node = decode_node(id, &load(id)?)?;
        if node.kind != kind {
            return Err(corruption("ordered-tree node has the wrong kind"));
        }
        if expected
            .as_ref()
            .is_some_and(|expected| node_ref(id, &node) != *expected)
        {
            return Err(corruption(
                "ordered-tree child body does not match its authenticated parent reference",
            ));
        }
        match node.body {
            NodeBody::Leaf(entries) => {
                output.extend(entries.into_iter().map(|entry| (entry.key, entry.value)))
            }
            NodeBody::Internal(children) => {
                frontier.extend(
                    children
                        .into_iter()
                        .rev()
                        .map(|child| (child.id, Some(child))),
                );
            }
        }
    }
    if output.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(corruption("ordered-tree scan is not globally ordered"));
    }
    Ok(output)
}

pub(super) fn validate_commit_catalog_back_edge(
    key: CommitId,
    entry: CommitCatalogEntry,
    load: impl Fn(ObjectId) -> Result<Bytes, StorageError>,
) -> Result<CommitObjectV1, StorageError> {
    let commit = CommitObjectV1::decode(entry.commit_object_id, &load(entry.commit_object_id)?)?;
    if commit.commit_id != key {
        return Err(corruption(
            "CommitCatalog key does not match embedded CommitId",
        ));
    }
    Ok(commit)
}

pub(super) fn validate_change_catalog_back_edge(
    key: ChangeId,
    entry: ChangeCatalogEntry,
    load: impl Fn(ObjectId) -> Result<Bytes, StorageError>,
) -> Result<ChangeObjectV1, StorageError> {
    let change = match entry.owner {
        ChangeCatalogOwner::CommitMember {
            commit_object_id,
            ordinal,
        } => {
            let commit = CommitObjectV1::decode(commit_object_id, &load(commit_object_id)?)?;
            let members = commit.load_members_with(&load)?;
            let member = members
                .get(ordinal as usize)
                .ok_or_else(|| corruption("ChangeCatalog commit ordinal is out of bounds"))?;
            let super::model::CommitMemberV1::Introduced {
                change_id, payload, ..
            } = member
            else {
                return Err(corruption(
                    "ChangeCatalog canonical owner points to a selected member",
                ));
            };
            ChangeObjectV1::Semantic {
                change_id: *change_id,
                payload: payload.clone(),
                json_payload_object_ids: Vec::new(),
            }
        }
        ChangeCatalogOwner::BranchRef {
            ref_change_object_id,
            branch_id,
        } => {
            let change =
                ChangeObjectV1::decode(ref_change_object_id, &load(ref_change_object_id)?)?;
            let ChangeObjectV1::BranchRef {
                branch_id: object_branch_id,
                before_semantic_head_commit_object_id,
                after_semantic_head_commit_object_id,
                ..
            } = &change
            else {
                return Err(corruption(
                    "branch-ref catalog owner names semantic payload",
                ));
            };
            if branch_id != *object_branch_id {
                return Err(corruption(
                    "ChangeCatalog branch id does not match its RefChange object",
                ));
            }
            if before_semantic_head_commit_object_id.is_none()
                && after_semantic_head_commit_object_id.is_none()
            {
                return Err(corruption("RefChange has no before or after target"));
            }
            change
        }
        ChangeCatalogOwner::PackedCommit { .. } => {
            return Err(corruption(
                "packed commit marker has no standalone Change object",
            ));
        }
    };
    if change.change_id() != key {
        return Err(corruption(
            "ChangeCatalog key does not match embedded ChangeId",
        ));
    }
    Ok(change)
}

pub(super) fn validate_receipt_tree(
    root: ReceiptTreeRoot,
    upload_id: &super::model::CanonicalUploadId,
    load: impl Fn(ObjectId) -> Result<Bytes, StorageError>,
) -> Result<Vec<UploadPartV1>, StorageError> {
    let node = decode_node(root.object_id, &load(root.object_id)?)?;
    if node.kind != TreeKind::Receipt || receipt_root(&node_ref(root.object_id, &node)) != root {
        return Err(corruption(
            "receipt root summary does not match its root node",
        ));
    }
    let entries = scan_all(root.object_id, "receipt", &load)?;
    let mut parts = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        let part_number = u64::from_be_bytes(
            key.as_slice()
                .try_into()
                .map_err(|_| corruption("receipt key is not a part number"))?,
        );
        let part_id = ObjectId::from_bytes(
            value
                .as_slice()
                .try_into()
                .map_err(|_| corruption("receipt value is not an object id"))?,
        );
        let part = UploadPartV1::decode(part_id, &load(part_id)?)?;
        if &part.upload_id != upload_id || part.part_number != part_number {
            return Err(corruption(
                "receipt key/upload binding does not match UploadPart object",
            ));
        }
        let mut digest = blake3::Hasher::new();
        for chunk in &part.ordered_chunks {
            let encoded = load(chunk.chunk_object_id)?;
            let loaded = BlobChunkV1::decode(chunk.chunk_object_id, &encoded)?;
            if loaded.bytes.len() as u64 != chunk.declared_len {
                return Err(corruption(
                    "receipt chunk bytes do not match their declared length",
                ));
            }
            digest.update(&loaded.bytes);
        }
        if digest.finalize().as_bytes() != &part.part_digest {
            return Err(corruption(
                "receipt UploadPart digest does not match authenticated chunks",
            ));
        }
        parts.push(part);
    }
    let recomputed = summary_from_receipt_parts(&parts)?;
    if recomputed.entry_count != root.completed_part_count
        || recomputed.logical_bytes != root.received_bytes
        || recomputed.contiguous_prefix_bytes != root.contiguous_prefix_bytes
    {
        return Err(corruption(
            "receipt root aggregates do not match authenticated UploadPart objects",
        ));
    }
    Ok(parts)
}

pub(super) fn validate_upload_progress_tree(
    progress: &UploadProgressV1,
    load: impl Fn(ObjectId) -> Result<Bytes, StorageError>,
) -> Result<Vec<UploadPartV1>, StorageError> {
    let root = ReceiptTreeRoot {
        object_id: progress.receipt_tree_root,
        completed_part_count: progress.completed_part_count,
        received_bytes: progress.received_bytes,
        contiguous_prefix_bytes: progress.contiguous_prefix_bytes,
    };
    validate_receipt_tree(root, &progress.upload_id, load)
}

pub(super) fn validate_upload_selector_progress(
    selector: &UploadSelectorV1,
    load: impl Fn(ObjectId) -> Result<Bytes, StorageError>,
) -> Result<UploadProgressV1, StorageError> {
    let progress = UploadProgressV1::decode(
        selector.progress_object_id,
        &load(selector.progress_object_id)?,
    )?;
    if selector.upload_id != progress.upload_id
        || selector.binding_digest != progress.binding_digest
    {
        return Err(corruption(
            "upload selector binding does not match its progress object",
        ));
    }
    Ok(progress)
}

pub(super) fn validate_branch_snapshot_ref_edge(
    snapshot: &BranchSnapshotV1,
    load: impl Fn(ObjectId) -> Result<Bytes, StorageError>,
) -> Result<Option<ChangeObjectV1>, StorageError> {
    let Some(ref_change_id) = snapshot.latest_ref_change_object_id else {
        return Err(corruption(
            "branch snapshot has no authenticated latest RefChange edge",
        ));
    };
    let change = ChangeObjectV1::decode(ref_change_id, &load(ref_change_id)?)?;
    match &change {
        ChangeObjectV1::BranchRef {
            branch_id,
            after_semantic_head_commit_object_id,
            ..
        } if *branch_id == snapshot.branch_id
            && *after_semantic_head_commit_object_id
                == Some(snapshot.semantic_head_commit_object_id) =>
        {
            Ok(Some(change))
        }
        ChangeObjectV1::BranchRef { .. } => Err(corruption(
            "branch snapshot latest RefChange does not match branch/head target",
        )),
        ChangeObjectV1::Semantic { .. } => Err(corruption(
            "branch snapshot latest RefChange edge names a semantic Change",
        )),
    }
}

fn build_tree(kind: TreeKind, entries: &[LeafEntry]) -> Result<TreeBuild, StorageError> {
    validate_entries(kind, entries)?;
    let mut objects = ImmutableObjectSet::default();
    let leaf_limit = kind.limits().0;
    let mut level = if entries.is_empty() {
        vec![stage_leaf(kind, &[], &mut objects)?]
    } else {
        balanced_chunk_sizes(entries.len(), leaf_limit)
            .into_iter()
            .scan(0_usize, |offset, size| {
                let start = *offset;
                *offset += size;
                Some((start, size))
            })
            .map(|(start, size)| stage_leaf(kind, &entries[start..start + size], &mut objects))
            .collect::<Result<Vec<_>, _>>()?
    };
    while level.len() > 1 {
        level = stage_internal_level(kind, &level, &mut objects)?;
    }
    let root = level.pop().expect("tree builder always emits a root");
    Ok(TreeBuild {
        root: OrderedTreeRoot {
            object_id: root.id,
            entry_count: root.summary.entry_count,
        },
        objects,
    })
}

fn rewrite_insert(
    kind: TreeKind,
    id: ObjectId,
    expected: Option<&NodeRef>,
    entry: &LeafEntry,
    load: &impl Fn(ObjectId) -> Result<Bytes, StorageError>,
    objects: &mut ImmutableObjectSet,
    copied_nodes: &mut usize,
) -> Result<Vec<NodeRef>, StorageError> {
    let bytes = objects.get(id).cloned().map_or_else(|| load(id), Ok)?;
    let node = decode_node(id, &bytes)?;
    if node.kind != kind {
        return Err(corruption("receipt edit reached a non-receipt node"));
    }
    if expected.is_some_and(|expected| node_ref(id, &node) != *expected) {
        return Err(corruption(
            "receipt edit path does not match its authenticated parent reference",
        ));
    }
    let original_ref = node_ref(id, &node);
    match node.body {
        NodeBody::Leaf(mut entries) => {
            match entries.binary_search_by(|current| current.key.cmp(&entry.key)) {
                Ok(index) if entries[index] == *entry => return Ok(vec![original_ref]),
                Ok(_) => return Err(StorageError::WriteConflict),
                Err(index) => entries.insert(index, entry.clone()),
            }
            *copied_nodes += 1;
            stage_leaf_level(kind, &entries, objects)
        }
        NodeBody::Internal(children) => {
            let child_index = children
                .partition_point(|child| child.max_key < entry.key)
                .min(children.len().saturating_sub(1));
            let rewritten = rewrite_insert(
                kind,
                children[child_index].id,
                Some(&children[child_index]),
                entry,
                load,
                objects,
                copied_nodes,
            )?;
            if rewritten.len() == 1 && rewritten[0] == children[child_index] {
                return Ok(vec![original_ref]);
            }
            let mut next = Vec::with_capacity(children.len() + rewritten.len());
            next.extend_from_slice(&children[..child_index]);
            next.extend(rewritten);
            next.extend_from_slice(&children[child_index + 1..]);
            *copied_nodes += 1;
            stage_internal_level(kind, &next, objects)
        }
    }
}

fn retain_reachable_new_nodes(
    root: ObjectId,
    kind: TreeKind,
    objects: &mut ImmutableObjectSet,
) -> Result<(), StorageError> {
    let mut reachable = BTreeSet::new();
    let mut frontier = vec![root];
    while let Some(id) = frontier.pop() {
        if !reachable.insert(id) {
            continue;
        }
        let Some(bytes) = objects.get(id) else {
            // A durable immutable node cannot reference a not-yet-published
            // child, so an old subtree contains no new objects to retain.
            continue;
        };
        let node = decode_node(id, bytes)?;
        if node.kind != kind {
            return Err(corruption(
                "ordered-tree final root reaches a node of the wrong kind",
            ));
        }
        if let NodeBody::Internal(children) = node.body {
            frontier.extend(children.into_iter().map(|child| child.id));
        }
    }
    objects.objects.retain(|id, _| reachable.contains(id));
    Ok(())
}

fn child_index(children: &[NodeRef], key: &[u8]) -> usize {
    children
        .partition_point(|child| child.max_key.as_slice() < key)
        .min(children.len().saturating_sub(1))
}

fn validate_loaded_node(
    id: ObjectId,
    node: &Node,
    kind: TreeKind,
    expected: Option<&NodeRef>,
) -> Result<(), StorageError> {
    if node.kind != kind {
        return Err(corruption("ordered-tree node has the wrong kind"));
    }
    if expected.is_some_and(|expected| node_ref(id, node) != *expected) {
        return Err(corruption(
            "ordered-tree child body does not match its authenticated parent reference",
        ));
    }
    Ok(())
}

async fn load_object_on_read<R>(read: &R, id: ObjectId) -> Result<Bytes, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let keys = [Key(Bytes::copy_from_slice(id.as_bytes()))];
    let loaded = read
        .get_many(&[GetManyRequest {
            space: super::object::OBJECT_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    match loaded.values.into_iter().next().flatten() {
        Some(ProjectedValue::FullValue(bytes)) => Ok(bytes),
        Some(ProjectedValue::KeyOnly) => Err(corruption(
            "ordered-tree object read returned a key-only projection",
        )),
        None => Err(corruption(format!("ordered-tree object {id} is absent"))),
    }
}

async fn load_objects_many_on_read<R>(
    read: &R,
    ids: &[ObjectId],
) -> Result<BTreeMap<ObjectId, Bytes>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let keys = ids
        .iter()
        .map(|id| Key(Bytes::copy_from_slice(id.as_bytes())))
        .collect::<Vec<_>>();
    let loaded = read
        .get_many(&[GetManyRequest {
            space: super::object::OBJECT_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    if loaded.values.len() != ids.len() {
        return Err(corruption(
            "ordered-tree exact object read returned the wrong slot count",
        ));
    }
    ids.iter()
        .copied()
        .zip(loaded.values)
        .map(|(id, value)| match value {
            Some(ProjectedValue::FullValue(bytes)) => Ok((id, bytes)),
            Some(ProjectedValue::KeyOnly) => Err(corruption(
                "ordered-tree exact object read returned a key-only projection",
            )),
            None => Err(corruption(format!("ordered-tree object {id} is absent"))),
        })
        .collect()
}

fn finish_root(
    kind: TreeKind,
    mut roots: Vec<NodeRef>,
    load: &impl Fn(ObjectId) -> Result<Bytes, StorageError>,
    objects: &mut ImmutableObjectSet,
    copied_nodes: &mut usize,
) -> Result<NodeRef, StorageError> {
    while roots.len() > 1 {
        roots = stage_internal_level(kind, &roots, objects)?;
        *copied_nodes += roots.len();
    }
    let mut root = roots
        .pop()
        .ok_or_else(|| corruption("tree edit emitted no root"))?;
    loop {
        let bytes = objects
            .get(root.id)
            .cloned()
            .map_or_else(|| load(root.id), Ok)?;
        let node = decode_node(root.id, &bytes)?;
        match node.body {
            NodeBody::Internal(children) if children.len() == 1 => {
                root = children[0].clone();
            }
            NodeBody::Leaf(_) | NodeBody::Internal(_) => return Ok(root),
        }
    }
}

fn stage_leaf_level(
    kind: TreeKind,
    entries: &[LeafEntry],
    objects: &mut ImmutableObjectSet,
) -> Result<Vec<NodeRef>, StorageError> {
    if entries.is_empty() {
        return stage_leaf(kind, entries, objects).map(|node| vec![node]);
    }
    balanced_chunk_sizes(entries.len(), kind.limits().0)
        .into_iter()
        .scan(0_usize, |offset, size| {
            let start = *offset;
            *offset += size;
            Some((start, size))
        })
        .map(|(start, size)| stage_leaf(kind, &entries[start..start + size], objects))
        .collect()
}

fn stage_internal_level(
    kind: TreeKind,
    children: &[NodeRef],
    objects: &mut ImmutableObjectSet,
) -> Result<Vec<NodeRef>, StorageError> {
    if children.is_empty() {
        return Err(corruption("internal tree level is empty"));
    }
    balanced_chunk_sizes(children.len(), kind.limits().1)
        .into_iter()
        .scan(0_usize, |offset, size| {
            let start = *offset;
            *offset += size;
            Some((start, size))
        })
        .map(|(start, size)| stage_internal(kind, &children[start..start + size], objects))
        .collect()
}

fn stage_leaf(
    kind: TreeKind,
    entries: &[LeafEntry],
    objects: &mut ImmutableObjectSet,
) -> Result<NodeRef, StorageError> {
    validate_entries(kind, entries)?;
    let summary = summary_from_entries(kind, entries)?;
    let (id, bytes) = encode_node(&Node {
        kind,
        summary,
        body: NodeBody::Leaf(entries.to_vec()),
    })?;
    objects.insert(id, bytes)?;
    Ok(NodeRef {
        id,
        max_key: entries
            .last()
            .map_or_else(Vec::new, |entry| entry.key.clone()),
        summary,
    })
}

fn stage_internal(
    kind: TreeKind,
    children: &[NodeRef],
    objects: &mut ImmutableObjectSet,
) -> Result<NodeRef, StorageError> {
    validate_children(children)?;
    let summary = summary_from_children(kind, children)?;
    let (id, bytes) = encode_node(&Node {
        kind,
        summary,
        body: NodeBody::Internal(children.to_vec()),
    })?;
    objects.insert(id, bytes)?;
    Ok(NodeRef {
        id,
        max_key: children
            .last()
            .expect("validated internal node is nonempty")
            .max_key
            .clone(),
        summary,
    })
}

fn encode_node(node: &Node) -> Result<(ObjectId, Bytes), StorageError> {
    encode_object(ObjectDomain::OrderedTreeNode, |encoder| {
        encoder.u8(node.kind as u8);
        match &node.body {
            NodeBody::Leaf(entries) => {
                encoder.u8(0);
                encode_summary(encoder, node.summary);
                encoder.u32(
                    u32::try_from(entries.len())
                        .map_err(|_| corruption("tree leaf count exceeds u32"))?,
                );
                for entry in entries {
                    encoder.bytes(&entry.key)?;
                    encoder.bytes(&entry.value)?;
                    match entry.receipt {
                        Some(summary) => {
                            encoder.u8(1);
                            encoder.u64(summary.byte_offset);
                            encoder.u64(summary.declared_len);
                        }
                        None => encoder.u8(0),
                    }
                }
            }
            NodeBody::Internal(children) => {
                encoder.u8(1);
                encode_summary(encoder, node.summary);
                encoder.u32(
                    u32::try_from(children.len())
                        .map_err(|_| corruption("tree child count exceeds u32"))?,
                );
                let mut previous_key: &[u8] = &[];
                for child in children {
                    let shared = previous_key
                        .iter()
                        .zip(&child.max_key)
                        .take_while(|(left, right)| left == right)
                        .count();
                    encoder.u32(
                        u32::try_from(shared)
                            .map_err(|_| corruption("tree separator prefix exceeds u32"))?,
                    );
                    encoder.bytes(&child.max_key[shared..])?;
                    encode_id(encoder, child.id);
                    encode_summary(encoder, child.summary);
                    previous_key = &child.max_key;
                }
            }
        }
        Ok(())
    })
}

fn decode_node(id: ObjectId, bytes: &[u8]) -> Result<Node, StorageError> {
    let mut decoder = decode_object(id, ObjectDomain::OrderedTreeNode, bytes)?;
    let kind = TreeKind::decode(decoder.u8()?)?;
    let body_tag = decoder.u8()?;
    let encoded_summary = decode_summary(&mut decoder)?;
    let count = decoder.usize("tree item count")?;
    let body = match body_tag {
        0 => {
            if count > kind.limits().0 || count > decoder.remaining() / 9 {
                return Err(corruption("tree leaf count is invalid"));
            }
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let key = decoder.bytes("tree key")?;
                let value = decoder.bytes("tree value")?;
                let receipt = match decoder.u8()? {
                    0 => None,
                    1 => Some(ReceiptEntrySummary {
                        byte_offset: decoder.u64()?,
                        declared_len: decoder.u64()?,
                    }),
                    tag => {
                        return Err(corruption(format!(
                            "tree receipt-summary tag {tag} is invalid"
                        )));
                    }
                };
                entries.push(LeafEntry {
                    key,
                    value,
                    receipt,
                });
            }
            validate_entries(kind, &entries)?;
            let actual_summary = summary_from_entries(kind, &entries)?;
            if actual_summary != encoded_summary {
                return Err(corruption("tree leaf aggregate is invalid"));
            }
            NodeBody::Leaf(entries)
        }
        1 => {
            if count == 0 || count > kind.limits().1 || count > decoder.remaining() / 85 {
                return Err(corruption("tree internal child count is invalid"));
            }
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                let shared = decoder.usize("tree separator prefix")?;
                let suffix = decoder.bytes("tree separator suffix")?;
                let previous = children
                    .last()
                    .map_or(&[][..], |child: &NodeRef| child.max_key.as_slice());
                if shared > previous.len() {
                    return Err(corruption("tree separator prefix exceeds prior key"));
                }
                let mut max_key = previous[..shared].to_vec();
                max_key.extend_from_slice(&suffix);
                children.push(NodeRef {
                    id: decode_id(&mut decoder)?,
                    max_key,
                    summary: decode_summary(&mut decoder)?,
                });
            }
            validate_children(&children)?;
            let actual_summary = summary_from_children(kind, &children)?;
            if actual_summary != encoded_summary {
                return Err(corruption("tree internal aggregate is invalid"));
            }
            NodeBody::Internal(children)
        }
        tag => return Err(corruption(format!("unknown tree body tag {tag}"))),
    };
    decoder.finish()?;
    Ok(Node {
        kind,
        summary: encoded_summary,
        body,
    })
}

fn encode_summary(encoder: &mut Encoder, summary: TreeSummary) {
    encoder.u64(summary.entry_count);
    encoder.u64(summary.logical_bytes);
    encoder.u64(summary.contiguous_prefix_bytes);
    encode_optional_u64(encoder, summary.first_part);
    encode_optional_u64(encoder, summary.last_part);
    encoder.u64(summary.first_offset);
    encoder.u64(summary.last_end);
    encoder.u8(u8::from(summary.fully_contiguous));
}

fn decode_summary(decoder: &mut Decoder<'_>) -> Result<TreeSummary, StorageError> {
    Ok(TreeSummary {
        entry_count: decoder.u64()?,
        logical_bytes: decoder.u64()?,
        contiguous_prefix_bytes: decoder.u64()?,
        first_part: decode_optional_u64(decoder, "first part")?,
        last_part: decode_optional_u64(decoder, "last part")?,
        first_offset: decoder.u64()?,
        last_end: decoder.u64()?,
        fully_contiguous: match decoder.u8()? {
            0 => false,
            1 => true,
            tag => {
                return Err(corruption(format!(
                    "tree aggregate bool tag {tag} is invalid"
                )));
            }
        },
    })
}

fn encode_optional_u64(encoder: &mut Encoder, value: Option<u64>) {
    match value {
        Some(value) => {
            encoder.u8(1);
            encoder.u64(value);
        }
        None => encoder.u8(0),
    }
}

fn decode_optional_u64(
    decoder: &mut Decoder<'_>,
    label: &str,
) -> Result<Option<u64>, StorageError> {
    match decoder.u8()? {
        0 => Ok(None),
        1 => decoder.u64().map(Some),
        tag => Err(corruption(format!(
            "tree aggregate {label} tag {tag} is invalid"
        ))),
    }
}

fn summary_from_entries(
    kind: TreeKind,
    entries: &[LeafEntry],
) -> Result<TreeSummary, StorageError> {
    if kind != TreeKind::Receipt {
        return Ok(TreeSummary {
            entry_count: entries.len() as u64,
            fully_contiguous: true,
            ..TreeSummary::default()
        });
    }
    let mut parts = Vec::with_capacity(entries.len());
    for entry in entries {
        let part_number = u64::from_be_bytes(
            entry
                .key
                .as_slice()
                .try_into()
                .map_err(|_| corruption("receipt key is not eight bytes"))?,
        );
        let summary = entry
            .receipt
            .ok_or_else(|| corruption("receipt leaf entry has no aggregate"))?;
        parts.push((part_number, summary.byte_offset, summary.declared_len));
    }
    summary_from_part_ranges(&parts)
}

fn summary_from_children(
    kind: TreeKind,
    children: &[NodeRef],
) -> Result<TreeSummary, StorageError> {
    if kind != TreeKind::Receipt {
        return Ok(TreeSummary {
            entry_count: children.iter().try_fold(0_u64, |total, child| {
                total
                    .checked_add(child.summary.entry_count)
                    .ok_or_else(|| corruption("tree entry count overflows u64"))
            })?,
            fully_contiguous: true,
            ..TreeSummary::default()
        });
    }
    combine_receipt_summaries(children.iter().map(|child| child.summary))
}

fn summary_from_receipt_parts(parts: &[UploadPartV1]) -> Result<TreeSummary, StorageError> {
    summary_from_part_ranges(
        &parts
            .iter()
            .map(|part| (part.part_number, part.byte_offset, part.declared_part_len))
            .collect::<Vec<_>>(),
    )
}

fn summary_from_part_ranges(parts: &[(u64, u64, u64)]) -> Result<TreeSummary, StorageError> {
    if parts.is_empty() {
        return Ok(TreeSummary {
            fully_contiguous: true,
            ..TreeSummary::default()
        });
    }
    let mut logical_bytes = 0_u64;
    let mut fully_contiguous = true;
    let mut prefix_end = parts[0].1;
    for (index, &(part_number, offset, length)) in parts.iter().enumerate() {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| corruption("receipt byte range overflows u64"))?;
        logical_bytes = logical_bytes
            .checked_add(length)
            .ok_or_else(|| corruption("receipt byte total overflows u64"))?;
        if index == 0 {
            prefix_end = end;
            continue;
        }
        let (previous_part, previous_offset, previous_length) = parts[index - 1];
        let previous_end = previous_offset
            .checked_add(previous_length)
            .ok_or_else(|| corruption("receipt byte range overflows u64"))?;
        let adjacent = previous_part.checked_add(1) == Some(part_number) && previous_end == offset;
        if fully_contiguous && adjacent {
            prefix_end = end;
        } else {
            fully_contiguous = false;
        }
    }
    let first = parts[0];
    let last = parts[parts.len() - 1];
    let last_end = last
        .1
        .checked_add(last.2)
        .ok_or_else(|| corruption("receipt byte range overflows u64"))?;
    Ok(TreeSummary {
        entry_count: parts.len() as u64,
        logical_bytes,
        contiguous_prefix_bytes: if first.0 == 0 && first.1 == 0 {
            prefix_end
        } else {
            0
        },
        first_part: Some(first.0),
        last_part: Some(last.0),
        first_offset: first.1,
        last_end,
        fully_contiguous,
    })
}

fn combine_receipt_summaries(
    summaries: impl Iterator<Item = TreeSummary>,
) -> Result<TreeSummary, StorageError> {
    let summaries = summaries.collect::<Vec<_>>();
    if summaries.is_empty() {
        return Err(corruption("receipt internal node has no children"));
    }
    let mut entry_count = 0_u64;
    let mut logical_bytes = 0_u64;
    let mut fully_contiguous = true;
    let mut prefix_end = summaries[0].contiguous_prefix_bytes;
    for (index, summary) in summaries.iter().enumerate() {
        entry_count = entry_count
            .checked_add(summary.entry_count)
            .ok_or_else(|| corruption("receipt entry count overflows u64"))?;
        logical_bytes = logical_bytes
            .checked_add(summary.logical_bytes)
            .ok_or_else(|| corruption("receipt byte total overflows u64"))?;
        if index == 0 {
            fully_contiguous = summary.fully_contiguous;
            continue;
        }
        let previous = summaries[index - 1];
        let adjacent = previous.last_part.and_then(|part| part.checked_add(1))
            == summary.first_part
            && previous.last_end == summary.first_offset;
        if fully_contiguous && adjacent && summary.fully_contiguous {
            prefix_end = summary.last_end;
        } else {
            fully_contiguous = false;
        }
    }
    let first = summaries[0];
    let last = summaries[summaries.len() - 1];
    Ok(TreeSummary {
        entry_count,
        logical_bytes,
        contiguous_prefix_bytes: if first.first_part == Some(0) && first.first_offset == 0 {
            prefix_end
        } else {
            0
        },
        first_part: first.first_part,
        last_part: last.last_part,
        first_offset: first.first_offset,
        last_end: last.last_end,
        fully_contiguous,
    })
}

fn validate_entries(kind: TreeKind, entries: &[LeafEntry]) -> Result<(), StorageError> {
    if entries.windows(2).any(|pair| pair[0].key >= pair[1].key) {
        return Err(corruption("tree leaf keys are duplicated or out of order"));
    }
    for entry in entries {
        if entry.key.is_empty() {
            return Err(corruption("tree leaf key is empty"));
        }
        match kind {
            TreeKind::CommitCatalog | TreeKind::ChangeCatalog if entry.key.len() != 16 => {
                return Err(corruption("catalog key is not a raw 16-byte UUID"));
            }
            TreeKind::Receipt if entry.key.len() != 8 || entry.value.len() != 32 => {
                return Err(corruption("receipt entry key/value width is invalid"));
            }
            _ => {}
        }
        if kind == TreeKind::Receipt && entry.receipt.is_none() {
            return Err(corruption("receipt tree entry has no aggregate"));
        }
        if kind != TreeKind::Receipt && entry.receipt.is_some() {
            return Err(corruption("non-receipt tree entry has a receipt aggregate"));
        }
    }
    Ok(())
}

fn validate_children(children: &[NodeRef]) -> Result<(), StorageError> {
    if children.is_empty() {
        return Err(corruption("tree internal node is empty"));
    }
    if children
        .windows(2)
        .any(|pair| pair[0].max_key >= pair[1].max_key)
    {
        return Err(corruption(
            "tree internal child bounds are duplicated or out of order",
        ));
    }
    if children.iter().any(|child| child.id == ObjectId::ZERO) {
        return Err(corruption("tree internal node contains a zero child id"));
    }
    Ok(())
}

fn balanced_chunk_sizes(total: usize, maximum: usize) -> Vec<usize> {
    if total == 0 {
        return Vec::new();
    }
    let chunks = total.div_ceil(maximum);
    let base = total / chunks;
    let remainder = total % chunks;
    (0..chunks)
        .map(|index| base + usize::from(index < remainder))
        .collect()
}

fn node_ref(id: ObjectId, node: &Node) -> NodeRef {
    NodeRef {
        id,
        max_key: match &node.body {
            NodeBody::Leaf(entries) => entries
                .last()
                .map_or_else(Vec::new, |entry| entry.key.clone()),
            NodeBody::Internal(children) => children
                .last()
                .map_or_else(Vec::new, |child| child.max_key.clone()),
        },
        summary: node.summary,
    }
}

fn receipt_root(root: &NodeRef) -> ReceiptTreeRoot {
    ReceiptTreeRoot {
        object_id: root.id,
        completed_part_count: root.summary.entry_count,
        received_bytes: root.summary.logical_bytes,
        contiguous_prefix_bytes: root.summary.contiguous_prefix_bytes,
    }
}

fn parse_kind(value: &str) -> Result<TreeKind, StorageError> {
    match value {
        "commit" => Ok(TreeKind::CommitCatalog),
        "change" => Ok(TreeKind::ChangeCatalog),
        "receipt" => Ok(TreeKind::Receipt),
        "state" => Ok(TreeKind::State),
        _ => Err(corruption(format!("unknown tree lookup kind {value}"))),
    }
}
