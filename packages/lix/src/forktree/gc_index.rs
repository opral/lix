use std::collections::BTreeMap;

use bytes::Bytes;

use crate::storage::StorageError;
use crate::storage_adapter::StorageAdapterRead;

use super::codec::corruption;
use super::model::{
    CanonicalBranchId, GcLiveBranchEntryV1, GcLiveBranchPackV1, GcMarkEntryV2, GcMarkPackV2,
    GcQueueEntryV1, GcQueuePackV1, GcRadixKindV1, GcRadixNodeV1,
};
use super::object::{ObjectDomain, ObjectId, authenticate_object_domain};
use super::view::load_object_bytes;

#[derive(Debug, Default)]
pub(super) struct MaintenanceEdit {
    puts: BTreeMap<ObjectId, Bytes>,
    deletes: BTreeMap<ObjectId, ()>,
}

impl MaintenanceEdit {
    pub(super) fn puts(&self) -> impl Iterator<Item = (ObjectId, &Bytes)> {
        self.puts.iter().map(|(id, bytes)| (*id, bytes))
    }

    pub(super) fn deletes(&self) -> impl Iterator<Item = ObjectId> + '_ {
        self.deletes.keys().copied()
    }

    pub(super) fn stage(&mut self, id: ObjectId, bytes: Bytes) -> Result<(), StorageError> {
        if self.deletes.remove(&id).is_some() {
            return Err(corruption(
                "GC maintenance edit attempted to revive one superseded object",
            ));
        }
        match self.puts.get(&id) {
            Some(existing) if existing != &bytes => Err(corruption(
                "GC maintenance object ID has two canonical encodings",
            )),
            Some(_) => Ok(()),
            None => {
                self.puts.insert(id, bytes);
                Ok(())
            }
        }
    }

    pub(super) fn supersede(&mut self, id: ObjectId, replacement: Option<ObjectId>) {
        if replacement == Some(id) {
            return;
        }
        if self.puts.remove(&id).is_none() {
            self.deletes.insert(id, ());
        }
    }

    async fn load<R>(&self, read: &R, id: ObjectId) -> Result<Bytes, StorageError>
    where
        R: StorageAdapterRead + ?Sized,
    {
        if let Some(bytes) = self.puts.get(&id) {
            Ok(bytes.clone())
        } else {
            load_object_bytes(read, id).await
        }
    }
}

#[derive(Clone, Debug)]
enum IndexRecord {
    Mark(GcMarkEntryV2),
    Queue(GcQueueEntryV1),
    Live(GcLiveBranchEntryV1),
}

impl IndexRecord {
    fn key(&self) -> [u8; 32] {
        match self {
            Self::Mark(entry) => *entry.object_id.as_bytes(),
            Self::Queue(entry) => queue_key(entry.sequence),
            Self::Live(entry) => entry.key_digest,
        }
    }
}

#[derive(Clone, Debug)]
struct RadixPath {
    id: ObjectId,
    node: GcRadixNodeV1,
    child_rank: usize,
}

pub(super) async fn mark_insert<R>(
    read: &R,
    edit: &mut MaintenanceEdit,
    root: Option<ObjectId>,
    cycle_id: [u8; 16],
    entry: GcMarkEntryV2,
) -> Result<(Option<ObjectId>, bool), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    insert_record(
        read,
        edit,
        root,
        cycle_id,
        GcRadixKindV1::Mark,
        IndexRecord::Mark(entry),
    )
    .await
}

pub(super) async fn mark_range_iter<R>(
    read: &R,
    edit: &MaintenanceEdit,
    root: Option<ObjectId>,
    cycle_id: [u8; 16],
    lower: ObjectId,
    upper: ObjectId,
) -> Result<BTreeMap<ObjectId, u16>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let Some(root) = root else {
        return Ok(BTreeMap::new());
    };
    let lower = *lower.as_bytes();
    let upper = *upper.as_bytes();
    let mut output = BTreeMap::new();
    let mut pending = vec![(root, Vec::new())];
    while let Some((id, incoming_prefix)) = pending.pop() {
        let bytes = edit.load(read, id).await?;
        match authenticate_object_domain(id, &bytes)? {
            ObjectDomain::GcRadixNodeV1 => {
                let node = GcRadixNodeV1::decode(id, &bytes)?;
                validate_node(&node, cycle_id, GcRadixKindV1::Mark)?;
                validate_incoming_prefix(&node.consumed_prefix, &incoming_prefix)?;
                let mut child_index = 0;
                for next in 0_u16..=255 {
                    if !bitmap_contains(&node.child_bitmap, next as u8) {
                        continue;
                    }
                    let child = node.child_object_ids[child_index];
                    child_index += 1;
                    let mut prefix = node.consumed_prefix.clone();
                    prefix.push(next as u8);
                    if prefix_intersects(&prefix, &lower, &upper) {
                        pending.push((child, prefix));
                    }
                }
            }
            ObjectDomain::GcMarkPackV2 => {
                let pack = GcMarkPackV2::decode(id, &bytes)?;
                validate_cycle(pack.cycle_id, cycle_id)?;
                validate_incoming_prefix(&pack.consumed_prefix, &incoming_prefix)?;
                for entry in pack.entries {
                    if entry.object_id.as_bytes() >= &lower && entry.object_id.as_bytes() <= &upper
                    {
                        if output
                            .insert(entry.object_id, entry.expected_domain)
                            .is_some()
                        {
                            return Err(corruption("GC mark radix repeats one object ID"));
                        }
                    }
                }
            }
            _ => return Err(corruption("GC mark radix reaches another object domain")),
        }
    }
    Ok(output)
}

pub(super) async fn live_insert<R>(
    read: &R,
    edit: &mut MaintenanceEdit,
    root: Option<ObjectId>,
    cycle_id: [u8; 16],
    entry: GcLiveBranchEntryV1,
) -> Result<(Option<ObjectId>, bool), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    insert_record(
        read,
        edit,
        root,
        cycle_id,
        GcRadixKindV1::LiveBranch,
        IndexRecord::Live(entry),
    )
    .await
}

pub(super) async fn live_contains<R>(
    read: &R,
    edit: &MaintenanceEdit,
    root: Option<ObjectId>,
    cycle_id: [u8; 16],
    key_digest: &[u8; 32],
    branch_id: CanonicalBranchId,
) -> Result<bool, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    match lookup_record(
        read,
        edit,
        root,
        cycle_id,
        GcRadixKindV1::LiveBranch,
        key_digest,
    )
    .await?
    {
        Some(IndexRecord::Live(entry)) if entry.branch_id == branch_id => Ok(true),
        Some(IndexRecord::Live(_)) => Err(corruption(
            "GC live-branch digest collides with another branch identity",
        )),
        Some(_) => Err(corruption(
            "GC live-branch index returned another record kind",
        )),
        None => Ok(false),
    }
}

pub(super) async fn queue_push<R>(
    read: &R,
    edit: &mut MaintenanceEdit,
    root: Option<ObjectId>,
    cycle_id: [u8; 16],
    entry: GcQueueEntryV1,
) -> Result<Option<ObjectId>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let (root, inserted) = insert_record(
        read,
        edit,
        root,
        cycle_id,
        GcRadixKindV1::Queue,
        IndexRecord::Queue(entry),
    )
    .await?;
    if !inserted {
        return Err(corruption("GC queue repeats one sequence"));
    }
    Ok(root)
}

pub(super) async fn queue_pop<R>(
    read: &R,
    edit: &mut MaintenanceEdit,
    root: Option<ObjectId>,
    cycle_id: [u8; 16],
    sequence: u64,
) -> Result<(Option<ObjectId>, GcQueueEntryV1), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let (root, record) = remove_record(
        read,
        edit,
        root,
        cycle_id,
        GcRadixKindV1::Queue,
        &queue_key(sequence),
    )
    .await?;
    match record {
        IndexRecord::Queue(entry) if entry.sequence == sequence => Ok((root, entry)),
        IndexRecord::Queue(_) => Err(corruption("GC queue key/sequence mismatch")),
        _ => Err(corruption("GC queue returned another record kind")),
    }
}

async fn insert_record<R>(
    read: &R,
    edit: &mut MaintenanceEdit,
    root: Option<ObjectId>,
    cycle_id: [u8; 16],
    kind: GcRadixKindV1,
    record: IndexRecord,
) -> Result<(Option<ObjectId>, bool), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let key = record.key();
    let Some(mut current) = root else {
        let id = build_subtree(kind, cycle_id, vec![record], edit)?;
        return Ok((Some(id), true));
    };
    let mut path = Vec::new();
    let mut incoming_prefix = Vec::new();
    loop {
        let bytes = edit.load(read, current).await?;
        match authenticate_object_domain(current, &bytes)? {
            ObjectDomain::GcRadixNodeV1 => {
                let mut node = GcRadixNodeV1::decode(current, &bytes)?;
                validate_node(&node, cycle_id, kind)?;
                validate_incoming_prefix(&node.consumed_prefix, &incoming_prefix)?;
                if !key.starts_with(&node.consumed_prefix) {
                    let new_leaf = build_subtree(kind, cycle_id, vec![record], edit)?;
                    let joined = join_subtrees(
                        kind,
                        cycle_id,
                        &node.consumed_prefix,
                        current,
                        &key,
                        new_leaf,
                        edit,
                    )?;
                    return unwind_insert(path, joined, edit).map(|id| (Some(id), true));
                }
                let next = *key
                    .get(node.consumed_prefix.len())
                    .ok_or_else(|| corruption("GC radix node consumes a complete key"))?;
                let rank = bitmap_rank(&node.child_bitmap, next);
                if !bitmap_contains(&node.child_bitmap, next) {
                    let child = build_subtree(kind, cycle_id, vec![record], edit)?;
                    bitmap_insert(&mut node.child_bitmap, next);
                    node.child_object_ids.insert(rank, child);
                    let (replacement, replacement_bytes) = node.encode()?;
                    edit.stage(replacement, replacement_bytes)?;
                    edit.supersede(current, Some(replacement));
                    return unwind_insert(path, replacement, edit).map(|id| (Some(id), true));
                }
                path.push(RadixPath {
                    id: current,
                    node,
                    child_rank: rank,
                });
                incoming_prefix =
                    node_child_prefix(&path.last().expect("path just pushed").node, next);
                current = path.last().expect("path just pushed").node.child_object_ids[rank];
            }
            domain if domain == leaf_domain(kind) => {
                let mut records = decode_pack(current, &bytes, cycle_id, kind)?;
                validate_record_prefixes(&records, &incoming_prefix)?;
                let position = records.binary_search_by_key(&key, IndexRecord::key);
                let inserted = match position {
                    Ok(index) => {
                        if !same_record(&records[index], &record) {
                            return Err(corruption(
                                "GC index key has conflicting authenticated values",
                            ));
                        }
                        false
                    }
                    Err(index) => {
                        records.insert(index, record);
                        true
                    }
                };
                if !inserted {
                    return Ok((root, false));
                }
                let replacement = build_subtree(kind, cycle_id, records, edit)?;
                edit.supersede(current, Some(replacement));
                return unwind_insert(path, replacement, edit).map(|id| (Some(id), true));
            }
            _ => return Err(corruption("GC radix reaches another object domain")),
        }
    }
}

async fn lookup_record<R>(
    read: &R,
    edit: &MaintenanceEdit,
    root: Option<ObjectId>,
    cycle_id: [u8; 16],
    kind: GcRadixKindV1,
    key: &[u8; 32],
) -> Result<Option<IndexRecord>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let Some(mut current) = root else {
        return Ok(None);
    };
    let mut incoming_prefix = Vec::new();
    loop {
        let bytes = edit.load(read, current).await?;
        match authenticate_object_domain(current, &bytes)? {
            ObjectDomain::GcRadixNodeV1 => {
                let node = GcRadixNodeV1::decode(current, &bytes)?;
                validate_node(&node, cycle_id, kind)?;
                validate_incoming_prefix(&node.consumed_prefix, &incoming_prefix)?;
                if !key.starts_with(&node.consumed_prefix)
                    || node.consumed_prefix.len() == key.len()
                {
                    return Ok(None);
                }
                let next = key[node.consumed_prefix.len()];
                if !bitmap_contains(&node.child_bitmap, next) {
                    return Ok(None);
                }
                incoming_prefix = node_child_prefix(&node, next);
                current = node.child_object_ids[bitmap_rank(&node.child_bitmap, next)];
            }
            domain if domain == leaf_domain(kind) => {
                let records = decode_pack(current, &bytes, cycle_id, kind)?;
                validate_record_prefixes(&records, &incoming_prefix)?;
                return Ok(records
                    .binary_search_by_key(key, IndexRecord::key)
                    .ok()
                    .map(|index| records[index].clone()));
            }
            _ => return Err(corruption("GC radix reaches another object domain")),
        }
    }
}

async fn remove_record<R>(
    read: &R,
    edit: &mut MaintenanceEdit,
    root: Option<ObjectId>,
    cycle_id: [u8; 16],
    kind: GcRadixKindV1,
    key: &[u8; 32],
) -> Result<(Option<ObjectId>, IndexRecord), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let Some(mut current) = root else {
        return Err(corruption("GC index removal has no root"));
    };
    let mut path = Vec::new();
    let mut incoming_prefix = Vec::new();
    loop {
        let bytes = edit.load(read, current).await?;
        match authenticate_object_domain(current, &bytes)? {
            ObjectDomain::GcRadixNodeV1 => {
                let node = GcRadixNodeV1::decode(current, &bytes)?;
                validate_node(&node, cycle_id, kind)?;
                validate_incoming_prefix(&node.consumed_prefix, &incoming_prefix)?;
                if !key.starts_with(&node.consumed_prefix)
                    || node.consumed_prefix.len() == key.len()
                {
                    return Err(corruption("GC index removal key is absent"));
                }
                let next = key[node.consumed_prefix.len()];
                if !bitmap_contains(&node.child_bitmap, next) {
                    return Err(corruption("GC index removal key is absent"));
                }
                let rank = bitmap_rank(&node.child_bitmap, next);
                path.push(RadixPath {
                    id: current,
                    node,
                    child_rank: rank,
                });
                incoming_prefix =
                    node_child_prefix(&path.last().expect("path just pushed").node, next);
                current = path.last().expect("path just pushed").node.child_object_ids[rank];
            }
            domain if domain == leaf_domain(kind) => {
                let mut records = decode_pack(current, &bytes, cycle_id, kind)?;
                validate_record_prefixes(&records, &incoming_prefix)?;
                let index = records
                    .binary_search_by_key(key, IndexRecord::key)
                    .map_err(|_| corruption("GC index removal key is absent"))?;
                let removed = records.remove(index);
                let replacement = if records.is_empty() {
                    None
                } else {
                    Some(build_subtree(kind, cycle_id, records, edit)?)
                };
                edit.supersede(current, replacement);
                let root = unwind_remove(path, replacement, edit)?;
                return Ok((root, removed));
            }
            _ => return Err(corruption("GC radix reaches another object domain")),
        }
    }
}

fn unwind_insert(
    path: Vec<RadixPath>,
    mut replacement: ObjectId,
    edit: &mut MaintenanceEdit,
) -> Result<ObjectId, StorageError> {
    for mut parent in path.into_iter().rev() {
        parent.node.child_object_ids[parent.child_rank] = replacement;
        let (next, bytes) = parent.node.encode()?;
        edit.stage(next, bytes)?;
        edit.supersede(parent.id, Some(next));
        replacement = next;
    }
    Ok(replacement)
}

fn unwind_remove(
    path: Vec<RadixPath>,
    mut replacement: Option<ObjectId>,
    edit: &mut MaintenanceEdit,
) -> Result<Option<ObjectId>, StorageError> {
    for mut parent in path.into_iter().rev() {
        match replacement {
            Some(child) => parent.node.child_object_ids[parent.child_rank] = child,
            None => {
                let next = bitmap_byte_at_rank(&parent.node.child_bitmap, parent.child_rank)
                    .ok_or_else(|| corruption("GC radix child rank is invalid"))?;
                bitmap_remove(&mut parent.node.child_bitmap, next);
                parent.node.child_object_ids.remove(parent.child_rank);
            }
        }
        replacement = match parent.node.child_object_ids.as_slice() {
            [] => None,
            [only] => Some(*only),
            _ => {
                let (id, bytes) = parent.node.encode()?;
                edit.stage(id, bytes)?;
                Some(id)
            }
        };
        edit.supersede(parent.id, replacement);
    }
    Ok(replacement)
}

fn build_subtree(
    kind: GcRadixKindV1,
    cycle_id: [u8; 16],
    records: Vec<IndexRecord>,
    edit: &mut MaintenanceEdit,
) -> Result<ObjectId, StorageError> {
    if records.is_empty() {
        return Err(corruption("GC cannot encode an empty index subtree"));
    }
    if records.len() <= leaf_limit(kind) {
        let (id, bytes) = encode_pack(kind, cycle_id, records)?;
        edit.stage(id, bytes)?;
        return Ok(id);
    }
    let prefix = common_prefix(&records);
    if prefix.len() >= 32 {
        return Err(corruption("GC radix overflow contains duplicate full keys"));
    }
    let mut groups = BTreeMap::<u8, Vec<IndexRecord>>::new();
    for record in records {
        groups
            .entry(record.key()[prefix.len()])
            .or_default()
            .push(record);
    }
    if groups.len() < 2 {
        return Err(corruption(
            "GC radix split did not reduce an overflowing pack",
        ));
    }
    let mut bitmap = [0_u8; 32];
    let mut children = Vec::with_capacity(groups.len());
    for (next, entries) in groups {
        bitmap_insert(&mut bitmap, next);
        children.push(build_subtree(kind, cycle_id, entries, edit)?);
    }
    let node = GcRadixNodeV1 {
        cycle_id,
        kind,
        consumed_prefix: prefix,
        child_bitmap: bitmap,
        child_object_ids: children,
    };
    let (id, bytes) = node.encode()?;
    edit.stage(id, bytes)?;
    Ok(id)
}

fn join_subtrees(
    kind: GcRadixKindV1,
    cycle_id: [u8; 16],
    existing_prefix: &[u8],
    existing: ObjectId,
    new_key: &[u8; 32],
    new_subtree: ObjectId,
    edit: &mut MaintenanceEdit,
) -> Result<ObjectId, StorageError> {
    let common_len = existing_prefix
        .iter()
        .zip(new_key)
        .take_while(|(left, right)| left == right)
        .count();
    let existing_next = *existing_prefix
        .get(common_len)
        .ok_or_else(|| corruption("GC radix divergence has no existing byte"))?;
    let new_next = new_key[common_len];
    if existing_next == new_next {
        return Err(corruption("GC radix divergence bytes are equal"));
    }
    let mut bitmap = [0_u8; 32];
    bitmap_insert(&mut bitmap, existing_next);
    bitmap_insert(&mut bitmap, new_next);
    let child_object_ids = if existing_next < new_next {
        vec![existing, new_subtree]
    } else {
        vec![new_subtree, existing]
    };
    let node = GcRadixNodeV1 {
        cycle_id,
        kind,
        consumed_prefix: existing_prefix[..common_len].to_vec(),
        child_bitmap: bitmap,
        child_object_ids,
    };
    let (id, bytes) = node.encode()?;
    edit.stage(id, bytes)?;
    Ok(id)
}

fn encode_pack(
    kind: GcRadixKindV1,
    cycle_id: [u8; 16],
    records: Vec<IndexRecord>,
) -> Result<(ObjectId, Bytes), StorageError> {
    match kind {
        GcRadixKindV1::Mark => {
            let consumed_prefix = common_prefix(&records);
            let entries = records
                .into_iter()
                .map(|record| match record {
                    IndexRecord::Mark(entry) => Ok(entry),
                    _ => Err(corruption("GC mark pack contains another record kind")),
                })
                .collect::<Result<Vec<_>, _>>()?;
            GcMarkPackV2 {
                cycle_id,
                consumed_prefix,
                entries,
            }
            .encode()
        }
        GcRadixKindV1::Queue => GcQueuePackV1 {
            cycle_id,
            entries: records
                .into_iter()
                .map(|record| match record {
                    IndexRecord::Queue(entry) => Ok(entry),
                    _ => Err(corruption("GC queue pack contains another record kind")),
                })
                .collect::<Result<Vec<_>, _>>()?,
        }
        .encode(),
        GcRadixKindV1::LiveBranch => GcLiveBranchPackV1 {
            cycle_id,
            entries: records
                .into_iter()
                .map(|record| match record {
                    IndexRecord::Live(entry) => Ok(entry),
                    _ => Err(corruption("GC live pack contains another record kind")),
                })
                .collect::<Result<Vec<_>, _>>()?,
        }
        .encode(),
    }
}

fn decode_pack(
    id: ObjectId,
    bytes: &[u8],
    cycle_id: [u8; 16],
    kind: GcRadixKindV1,
) -> Result<Vec<IndexRecord>, StorageError> {
    match kind {
        GcRadixKindV1::Mark => {
            let pack = GcMarkPackV2::decode(id, bytes)?;
            validate_cycle(pack.cycle_id, cycle_id)?;
            Ok(pack.entries.into_iter().map(IndexRecord::Mark).collect())
        }
        GcRadixKindV1::Queue => {
            let pack = GcQueuePackV1::decode(id, bytes)?;
            validate_cycle(pack.cycle_id, cycle_id)?;
            Ok(pack.entries.into_iter().map(IndexRecord::Queue).collect())
        }
        GcRadixKindV1::LiveBranch => {
            let pack = GcLiveBranchPackV1::decode(id, bytes)?;
            validate_cycle(pack.cycle_id, cycle_id)?;
            Ok(pack.entries.into_iter().map(IndexRecord::Live).collect())
        }
    }
}

fn validate_node(
    node: &GcRadixNodeV1,
    cycle_id: [u8; 16],
    kind: GcRadixKindV1,
) -> Result<(), StorageError> {
    validate_cycle(node.cycle_id, cycle_id)?;
    if node.kind != kind || node.consumed_prefix.len() >= 32 {
        return Err(corruption(
            "GC radix kind or branching prefix does not match its owner index",
        ));
    }
    Ok(())
}

fn node_child_prefix(node: &GcRadixNodeV1, next: u8) -> Vec<u8> {
    let mut prefix = node.consumed_prefix.clone();
    prefix.push(next);
    prefix
}

fn validate_incoming_prefix(
    actual_prefix: &[u8],
    incoming_prefix: &[u8],
) -> Result<(), StorageError> {
    if actual_prefix.starts_with(incoming_prefix) {
        Ok(())
    } else {
        Err(corruption(
            "GC radix child is outside its authenticated parent prefix",
        ))
    }
}

fn validate_record_prefixes(
    records: &[IndexRecord],
    incoming_prefix: &[u8],
) -> Result<(), StorageError> {
    if records
        .iter()
        .all(|record| record.key().starts_with(incoming_prefix))
    {
        Ok(())
    } else {
        Err(corruption(
            "GC radix pack contains a key outside its authenticated parent prefix",
        ))
    }
}

fn validate_cycle(actual: [u8; 16], expected: [u8; 16]) -> Result<(), StorageError> {
    if actual != expected {
        Err(corruption("GC maintenance object belongs to another cycle"))
    } else {
        Ok(())
    }
}

fn leaf_domain(kind: GcRadixKindV1) -> ObjectDomain {
    match kind {
        GcRadixKindV1::Mark => ObjectDomain::GcMarkPackV2,
        GcRadixKindV1::Queue => ObjectDomain::GcQueuePackV1,
        GcRadixKindV1::LiveBranch => ObjectDomain::GcLiveBranchPackV1,
    }
}

fn leaf_limit(kind: GcRadixKindV1) -> usize {
    match kind {
        GcRadixKindV1::Mark => GcMarkPackV2::MAX_ENTRIES,
        GcRadixKindV1::Queue => GcQueuePackV1::MAX_ENTRIES,
        GcRadixKindV1::LiveBranch => GcLiveBranchPackV1::MAX_ENTRIES,
    }
}

fn same_record(left: &IndexRecord, right: &IndexRecord) -> bool {
    match (left, right) {
        (IndexRecord::Mark(left), IndexRecord::Mark(right)) => left == right,
        (IndexRecord::Queue(left), IndexRecord::Queue(right)) => left == right,
        (IndexRecord::Live(left), IndexRecord::Live(right)) => left == right,
        _ => false,
    }
}

fn common_prefix(records: &[IndexRecord]) -> Vec<u8> {
    let first = records.first().expect("nonempty record set").key();
    let last = records.last().expect("nonempty record set").key();
    first
        .iter()
        .zip(last.iter())
        .take_while(|(left, right)| left == right)
        .map(|(byte, _)| *byte)
        .collect()
}

fn queue_key(sequence: u64) -> [u8; 32] {
    let mut key = [0_u8; 32];
    key[24..].copy_from_slice(&sequence.to_be_bytes());
    key
}

fn bitmap_contains(bitmap: &[u8; 32], value: u8) -> bool {
    bitmap[usize::from(value) / 8] & (1 << (value % 8)) != 0
}

fn bitmap_rank(bitmap: &[u8; 32], value: u8) -> usize {
    (0..value)
        .filter(|candidate| bitmap_contains(bitmap, *candidate))
        .count()
}

fn bitmap_insert(bitmap: &mut [u8; 32], value: u8) {
    bitmap[usize::from(value) / 8] |= 1 << (value % 8);
}

fn bitmap_remove(bitmap: &mut [u8; 32], value: u8) {
    bitmap[usize::from(value) / 8] &= !(1 << (value % 8));
}

fn bitmap_byte_at_rank(bitmap: &[u8; 32], rank: usize) -> Option<u8> {
    (0_u16..=255)
        .map(|value| value as u8)
        .filter(|value| bitmap_contains(bitmap, *value))
        .nth(rank)
}

fn prefix_intersects(prefix: &[u8], lower: &[u8; 32], upper: &[u8; 32]) -> bool {
    let mut minimum = [0_u8; 32];
    minimum[..prefix.len()].copy_from_slice(prefix);
    let mut maximum = [u8::MAX; 32];
    maximum[..prefix.len()].copy_from_slice(prefix);
    maximum >= *lower && minimum <= *upper
}
