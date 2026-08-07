use std::collections::BTreeMap;
use std::ops::Bound;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use lix::storage::{
    CoreProjection, GetManyRequest, GetOptions, Key, KeyRange, Precondition, ProjectedValue,
    PutBatch, PutEntry, ReadOptions, ScanOptions, SpaceId, Storage, StorageError, StorageRead,
    StorageSpace, StorageWrite, StoredValue, WriteOptions,
};

const OBJECT_MAGIC: &[u8; 4] = b"FKO1";
const REF_MAGIC: &[u8; 4] = b"FKR1";
const LEAF_TAG: u8 = 1;
const INTERNAL_TAG: u8 = 2;
const DELTA_TAG: u8 = 3;
const COMMIT_TAG: u8 = 4;
const VALUE_PACK_TAG: u8 = 5;
const LEAF_ROWS: usize = 8;
const INTERNAL_CHILDREN: usize = 8;

pub const OBJECT_SPACE: StorageSpace =
    StorageSpace::immutable(SpaceId(0x00f0_0001), "forktree_objects");
pub const REF_SPACE: StorageSpace = StorageSpace::mutable(SpaceId(0x00f0_0002), "forktree_refs");

const MAIN_REF_KEY: &[u8] = b"branch/main";
const EPOCH_KEY: &[u8] = b"epoch";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObjectId([u8; 32]);

#[derive(Clone, Debug)]
pub struct Update {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ApplyAccounting {
    pub object_writes: u64,
    pub object_bytes: u64,
    pub node_writes: u64,
    pub node_bytes: u64,
    pub leaf_writes: u64,
    pub leaf_bytes: u64,
    pub internal_writes: u64,
    pub internal_bytes: u64,
    pub reused_objects: u64,
    pub logical_bytes: u64,
}

impl std::ops::AddAssign for ApplyAccounting {
    fn add_assign(&mut self, other: Self) {
        self.object_writes += other.object_writes;
        self.object_bytes += other.object_bytes;
        self.node_writes += other.node_writes;
        self.node_bytes += other.node_bytes;
        self.leaf_writes += other.leaf_writes;
        self.leaf_bytes += other.leaf_bytes;
        self.internal_writes += other.internal_writes;
        self.internal_bytes += other.internal_bytes;
        self.reused_objects += other.reused_objects;
        self.logical_bytes += other.logical_bytes;
    }
}

#[derive(Clone)]
pub struct ForkTree<S> {
    storage: S,
}

#[derive(Clone, Debug)]
struct NodeRef {
    id: ObjectId,
    max_key: Vec<u8>,
}

#[derive(Clone, Debug)]
enum Node {
    Leaf(Vec<LeafEntry>),
    Internal(Vec<NodeRef>),
}

#[derive(Clone, Debug)]
struct LeafEntry {
    key: Vec<u8>,
    value: ValueRef,
}

#[derive(Clone, Copy, Debug)]
struct ValueRef {
    pack: ObjectId,
    index: u32,
}

#[derive(Clone, Debug)]
struct ResolvedUpdate {
    key: Vec<u8>,
    value: ValueRef,
}

#[derive(Clone, Copy)]
struct Commit {
    parent: Option<ObjectId>,
    root: ObjectId,
    delta: ObjectId,
}

struct Head {
    commit: ObjectId,
    epoch: u64,
    raw_ref: Bytes,
    raw_epoch: Bytes,
}

impl<S> ForkTree<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    pub async fn initialize(&self, rows: &[(Vec<u8>, Vec<u8>)]) -> Result<ObjectId, String> {
        validate_sorted_rows(rows)?;
        let mut pending = BTreeMap::new();
        let value_pack = stage_object(
            encode_value_pack(rows.iter().map(|(_, value)| value.as_slice())),
            &mut pending,
        );
        let root = build_tree(rows, value_pack, &mut pending)?;
        let delta = stage_object(encode_initial_delta(root.id, rows.len()), &mut pending);
        let commit = stage_object(
            encode_commit(Commit {
                parent: None,
                root: root.id,
                delta,
            }),
            &mut pending,
        );
        let raw_ref = encode_ref(commit);
        let raw_epoch = encode_epoch(1);
        let options = WriteOptions {
            preconditions: vec![
                Precondition::KeyAbsent {
                    space: REF_SPACE,
                    key: key(MAIN_REF_KEY),
                },
                Precondition::KeyAbsent {
                    space: REF_SPACE,
                    key: key(EPOCH_KEY),
                },
            ],
            batch_capacity_hint_bytes: pending.values().map(Bytes::len).sum::<usize>()
                + raw_ref.len()
                + raw_epoch.len(),
            ..WriteOptions::default()
        };
        let mut write = self
            .storage
            .begin_write(options)
            .await
            .map_err(storage_error)?;
        write
            .put_many(OBJECT_SPACE, object_batch(&pending))
            .await
            .map_err(storage_error)?;
        write
            .put_many(
                REF_SPACE,
                PutBatch {
                    entries: vec![
                        PutEntry {
                            key: key(MAIN_REF_KEY),
                            value: StoredValue { bytes: raw_ref },
                        },
                        PutEntry {
                            key: key(EPOCH_KEY),
                            value: StoredValue { bytes: raw_epoch },
                        },
                    ],
                },
            )
            .await
            .map_err(storage_error)?;
        write.commit().await.map_err(storage_error)?;
        Ok(commit)
    }

    pub async fn apply_sorted_updates(
        &self,
        updates: &[Update],
    ) -> Result<(ObjectId, ApplyAccounting), String> {
        validate_sorted_updates(updates)?;
        let head = self.load_head().await?;
        let commit = self.load_commit(head.commit).await?;
        let mut pending = BTreeMap::new();
        let mut accounting = ApplyAccounting {
            logical_bytes: updates
                .iter()
                .map(|update| update.key.len() as u64 + update.value.len() as u64)
                .sum(),
            ..ApplyAccounting::default()
        };
        let value_pack = stage_object(
            encode_value_pack(updates.iter().map(|update| update.value.as_slice())),
            &mut pending,
        );
        let resolved_updates = updates
            .iter()
            .enumerate()
            .map(|(index, update)| ResolvedUpdate {
                key: update.key.clone(),
                value: ValueRef {
                    pack: value_pack,
                    index: u32::try_from(index).expect("ForkTree value-pack index fits u32"),
                },
            })
            .collect::<Vec<_>>();
        let root = self
            .rewrite_node(
                commit.root,
                &resolved_updates,
                &mut pending,
                &mut accounting,
            )
            .await?;
        let delta = stage_object(
            encode_delta(
                value_pack,
                updates.iter().map(|update| update.key.as_slice()),
            ),
            &mut pending,
        );
        let next_commit = stage_object(
            encode_commit(Commit {
                parent: Some(head.commit),
                root: root.id,
                delta,
            }),
            &mut pending,
        );

        let pending_before_dedup = pending.len();
        self.remove_existing_objects(&mut pending).await?;
        accounting.reused_objects = pending_before_dedup.saturating_sub(pending.len()) as u64;
        for bytes in pending.values() {
            accounting.object_writes += 1;
            accounting.object_bytes += bytes.len() as u64;
            match object_tag(bytes)? {
                LEAF_TAG => {
                    accounting.node_writes += 1;
                    accounting.node_bytes += bytes.len() as u64;
                    accounting.leaf_writes += 1;
                    accounting.leaf_bytes += bytes.len() as u64;
                }
                INTERNAL_TAG => {
                    accounting.node_writes += 1;
                    accounting.node_bytes += bytes.len() as u64;
                    accounting.internal_writes += 1;
                    accounting.internal_bytes += bytes.len() as u64;
                }
                _ => {}
            }
        }

        let next_ref = encode_ref(next_commit);
        let next_epoch = encode_epoch(head.epoch.saturating_add(1));
        let options = WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: REF_SPACE,
                    key: key(MAIN_REF_KEY),
                    expected: head.raw_ref,
                },
                Precondition::KeyValueEquals {
                    space: REF_SPACE,
                    key: key(EPOCH_KEY),
                    expected: head.raw_epoch,
                },
            ],
            batch_capacity_hint_bytes: pending.values().map(Bytes::len).sum::<usize>()
                + next_ref.len()
                + next_epoch.len(),
            ..WriteOptions::default()
        };
        let mut write = self
            .storage
            .begin_write(options)
            .await
            .map_err(storage_error)?;
        if !pending.is_empty() {
            write
                .put_many(OBJECT_SPACE, object_batch(&pending))
                .await
                .map_err(storage_error)?;
        }
        write
            .put_many(
                REF_SPACE,
                PutBatch {
                    entries: vec![
                        PutEntry {
                            key: key(MAIN_REF_KEY),
                            value: StoredValue { bytes: next_ref },
                        },
                        PutEntry {
                            key: key(EPOCH_KEY),
                            value: StoredValue { bytes: next_epoch },
                        },
                    ],
                },
            )
            .await
            .map_err(storage_error)?;
        write.commit().await.map_err(storage_error)?;
        Ok((next_commit, accounting))
    }

    pub async fn read_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let head = self.load_head().await?;
        let commit = self.load_commit(head.commit).await?;
        let mut rows = Vec::new();
        self.collect_rows(commit.root, &mut rows).await?;
        Ok(rows)
    }

    pub async fn object_inventory(&self) -> Result<(u64, u64), String> {
        let read = self
            .storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?;
        let mut resume_after = None;
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        loop {
            let page = read
                .scan(
                    OBJECT_SPACE,
                    KeyRange {
                        lower: Bound::Unbounded,
                        upper: Bound::Unbounded,
                    },
                    ScanOptions {
                        projection: CoreProjection::FullValue,
                        limit_rows: 1_024,
                        resume_after: resume_after.clone(),
                    },
                )
                .await
                .map_err(storage_error)?;
            for entry in &page.entries {
                rows += 1;
                bytes += projected_bytes(&entry.value)?.len() as u64;
            }
            if !page.has_more {
                break;
            }
            resume_after = page.entries.last().map(|entry| entry.key.clone());
        }
        Ok((rows, bytes))
    }

    fn rewrite_node<'a>(
        &'a self,
        id: ObjectId,
        updates: &'a [ResolvedUpdate],
        pending: &'a mut BTreeMap<ObjectId, Bytes>,
        accounting: &'a mut ApplyAccounting,
    ) -> BoxFuture<'a, Result<NodeRef, String>> {
        Box::pin(async move {
            let node = decode_node(&self.load_object(id).await?)?;
            match node {
                Node::Leaf(mut rows) => {
                    for update in updates {
                        let index = rows
                            .binary_search_by(|row| row.key.as_slice().cmp(update.key.as_slice()))
                            .map_err(|_| {
                                format!(
                                    "focused prototype update key is absent: {}",
                                    String::from_utf8_lossy(&update.key)
                                )
                            })?;
                        rows[index].value = update.value;
                    }
                    let bytes = encode_leaf(&rows);
                    let id = stage_object(bytes, pending);
                    Ok(NodeRef {
                        id,
                        max_key: rows.last().expect("leaf nodes are nonempty").key.clone(),
                    })
                }
                Node::Internal(mut children) => {
                    let mut start = 0;
                    for child in &mut children {
                        let length = updates[start..].partition_point(|update| {
                            update.key.as_slice() <= child.max_key.as_slice()
                        });
                        let end = start + length;
                        if end > start {
                            *child = self
                                .rewrite_node(child.id, &updates[start..end], pending, accounting)
                                .await?;
                        }
                        start = end;
                    }
                    if start != updates.len() {
                        return Err("update key sorts beyond tree maximum".to_string());
                    }
                    let bytes = encode_internal(&children);
                    let id = stage_object(bytes, pending);
                    Ok(NodeRef {
                        id,
                        max_key: children
                            .last()
                            .expect("internal nodes are nonempty")
                            .max_key
                            .clone(),
                    })
                }
            }
        })
    }

    fn collect_rows<'a>(
        &'a self,
        id: ObjectId,
        output: &'a mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            match decode_node(&self.load_object(id).await?)? {
                Node::Leaf(rows) => {
                    let ids = rows
                        .iter()
                        .map(|row| row.value.pack)
                        .collect::<std::collections::BTreeSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>();
                    let packs = ids
                        .iter()
                        .copied()
                        .zip(self.load_objects(&ids).await?)
                        .map(|(id, bytes)| decode_value_pack(&bytes).map(|values| (id, values)))
                        .collect::<Result<BTreeMap<_, _>, _>>()?;
                    for row in rows {
                        let values = packs
                            .get(&row.value.pack)
                            .ok_or_else(|| "ForkTree leaf value pack was not loaded".to_string())?;
                        let value = values
                            .get(row.value.index as usize)
                            .ok_or_else(|| {
                                "ForkTree value-pack index is out of bounds".to_string()
                            })?
                            .clone();
                        output.push((row.key, value));
                    }
                }
                Node::Internal(children) => {
                    for child in children {
                        self.collect_rows(child.id, output).await?;
                    }
                }
            }
            Ok(())
        })
    }

    async fn load_head(&self) -> Result<Head, String> {
        let keys = [key(MAIN_REF_KEY), key(EPOCH_KEY)];
        let read = self
            .storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?;
        let result = read
            .get_many(&[GetManyRequest {
                space: REF_SPACE,
                keys: &keys,
                opts: GetOptions::default(),
            }])
            .await
            .map_err(storage_error)?;
        let mut values = result.values.into_iter();
        let raw_ref = projected_bytes(
            &values
                .next()
                .flatten()
                .ok_or_else(|| "missing ForkTree main ref".to_string())?,
        )?
        .clone();
        let raw_epoch = projected_bytes(
            &values
                .next()
                .flatten()
                .ok_or_else(|| "missing ForkTree epoch".to_string())?,
        )?
        .clone();
        Ok(Head {
            commit: decode_ref(&raw_ref)?,
            epoch: decode_epoch(&raw_epoch)?,
            raw_ref,
            raw_epoch,
        })
    }

    async fn load_commit(&self, id: ObjectId) -> Result<Commit, String> {
        decode_commit(&self.load_object(id).await?)
    }

    async fn load_object(&self, id: ObjectId) -> Result<Bytes, String> {
        self.load_objects(&[id])
            .await?
            .pop()
            .ok_or_else(|| "ForkTree object batch unexpectedly empty".to_string())
    }

    async fn load_objects(&self, ids: &[ObjectId]) -> Result<Vec<Bytes>, String> {
        let keys = ids
            .iter()
            .map(|id| Key(Bytes::copy_from_slice(&id.0)))
            .collect::<Vec<_>>();
        let read = self
            .storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?;
        let result = read
            .get_many(&[GetManyRequest {
                space: OBJECT_SPACE,
                keys: &keys,
                opts: GetOptions::default(),
            }])
            .await
            .map_err(storage_error)?;
        result
            .values
            .into_iter()
            .zip(ids)
            .map(|(value, &id)| {
                let value =
                    value.ok_or_else(|| format!("missing ForkTree object {}", hex_id(id)))?;
                let bytes = projected_bytes(&value)?.clone();
                authenticate(id, &bytes)?;
                Ok(bytes)
            })
            .collect()
    }

    async fn remove_existing_objects(
        &self,
        pending: &mut BTreeMap<ObjectId, Bytes>,
    ) -> Result<(), String> {
        if pending.is_empty() {
            return Ok(());
        }
        let ids = pending.keys().copied().collect::<Vec<_>>();
        let keys = ids
            .iter()
            .map(|id| Key(Bytes::copy_from_slice(&id.0)))
            .collect::<Vec<_>>();
        let read = self
            .storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?;
        let result = read
            .get_many(&[GetManyRequest {
                space: OBJECT_SPACE,
                keys: &keys,
                opts: GetOptions::default(),
            }])
            .await
            .map_err(storage_error)?;
        for (id, existing) in ids.into_iter().zip(result.values) {
            let Some(existing) = existing else {
                continue;
            };
            let existing = projected_bytes(&existing)?;
            authenticate(id, existing)?;
            if pending.get(&id).map(Bytes::as_ref) != Some(existing.as_ref()) {
                return Err(format!("authenticated object collision for {}", hex_id(id)));
            }
            pending.remove(&id);
        }
        Ok(())
    }
}

fn build_tree(
    rows: &[(Vec<u8>, Vec<u8>)],
    value_pack: ObjectId,
    pending: &mut BTreeMap<ObjectId, Bytes>,
) -> Result<NodeRef, String> {
    if rows.is_empty() {
        return Err("ForkTree requires at least one row".to_string());
    }
    let leaf_rows = rows
        .iter()
        .enumerate()
        .map(|(index, (key, _))| LeafEntry {
            key: key.clone(),
            value: ValueRef {
                pack: value_pack,
                index: u32::try_from(index).expect("ForkTree initial value-pack index fits u32"),
            },
        })
        .collect::<Vec<_>>();
    let mut level = leaf_rows
        .chunks(LEAF_ROWS)
        .map(|chunk| {
            let id = stage_object(encode_leaf(chunk), pending);
            NodeRef {
                id,
                max_key: chunk.last().expect("leaf chunk is nonempty").key.clone(),
            }
        })
        .collect::<Vec<_>>();
    while level.len() > 1 {
        level = level
            .chunks(INTERNAL_CHILDREN)
            .map(|children| {
                let id = stage_object(encode_internal(children), pending);
                NodeRef {
                    id,
                    max_key: children
                        .last()
                        .expect("internal chunk is nonempty")
                        .max_key
                        .clone(),
                }
            })
            .collect();
    }
    Ok(level.pop().expect("nonempty tree has a root"))
}

fn validate_sorted_rows(rows: &[(Vec<u8>, Vec<u8>)]) -> Result<(), String> {
    if rows.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err("ForkTree bulk input must be strictly key sorted".to_string());
    }
    Ok(())
}

fn validate_sorted_updates(updates: &[Update]) -> Result<(), String> {
    if updates.is_empty() {
        return Err("ForkTree update batch must not be empty".to_string());
    }
    if updates.windows(2).any(|pair| pair[0].key >= pair[1].key) {
        return Err("ForkTree updates must be strictly key sorted".to_string());
    }
    Ok(())
}

fn stage_object(bytes: Bytes, pending: &mut BTreeMap<ObjectId, Bytes>) -> ObjectId {
    let id = ObjectId(*blake3::hash(&bytes).as_bytes());
    match pending.insert(id, bytes.clone()) {
        Some(existing) => assert_eq!(existing, bytes, "BLAKE3 object collision"),
        None => {}
    }
    id
}

fn object_batch(objects: &BTreeMap<ObjectId, Bytes>) -> PutBatch {
    PutBatch {
        entries: objects
            .iter()
            .map(|(id, bytes)| PutEntry {
                key: Key(Bytes::copy_from_slice(&id.0)),
                value: StoredValue {
                    bytes: bytes.clone(),
                },
            })
            .collect(),
    }
}

fn encode_leaf(rows: &[LeafEntry]) -> Bytes {
    let mut body = Vec::new();
    put_u32(&mut body, rows.len());
    for row in rows {
        put_bytes(&mut body, &row.key);
        body.extend_from_slice(&row.value.pack.0);
        body.extend_from_slice(&row.value.index.to_be_bytes());
    }
    let compressed = zstd::bulk::compress(&body, 1).expect("compress canonical ForkTree leaf");
    let mut bytes = object_prefix(LEAF_TAG);
    put_u32(&mut bytes, body.len());
    bytes.extend_from_slice(&compressed);
    Bytes::from(bytes)
}

fn encode_value_pack<'a>(values: impl ExactSizeIterator<Item = &'a [u8]>) -> Bytes {
    let mut body = Vec::new();
    put_u32(&mut body, values.len());
    for value in values {
        put_bytes(&mut body, value);
    }
    let compressed =
        zstd::bulk::compress(&body, 1).expect("compress canonical ForkTree value pack");
    let mut bytes = object_prefix(VALUE_PACK_TAG);
    put_u32(&mut bytes, body.len());
    bytes.extend_from_slice(&compressed);
    Bytes::from(bytes)
}

fn encode_internal(children: &[NodeRef]) -> Bytes {
    let mut bytes = object_prefix(INTERNAL_TAG);
    put_u32(&mut bytes, children.len());
    let mut previous_key: &[u8] = &[];
    for child in children {
        let shared_prefix = previous_key
            .iter()
            .zip(&child.max_key)
            .take_while(|(left, right)| left == right)
            .count();
        put_u32(&mut bytes, shared_prefix);
        put_bytes(&mut bytes, &child.max_key[shared_prefix..]);
        bytes.extend_from_slice(&child.id.0);
        previous_key = &child.max_key;
    }
    Bytes::from(bytes)
}

fn encode_initial_delta(root: ObjectId, rows: usize) -> Bytes {
    let mut bytes = object_prefix(DELTA_TAG);
    bytes.push(0);
    bytes.extend_from_slice(&root.0);
    put_u32(&mut bytes, rows);
    Bytes::from(bytes)
}

fn encode_delta<'a>(value_pack: ObjectId, keys: impl ExactSizeIterator<Item = &'a [u8]>) -> Bytes {
    let mut bytes = object_prefix(DELTA_TAG);
    bytes.push(1);
    bytes.extend_from_slice(&value_pack.0);
    put_u32(&mut bytes, keys.len());
    for key in keys {
        put_bytes(&mut bytes, key);
    }
    Bytes::from(bytes)
}

fn encode_commit(commit: Commit) -> Bytes {
    let mut bytes = object_prefix(COMMIT_TAG);
    put_optional_id(&mut bytes, commit.parent);
    bytes.extend_from_slice(&commit.root.0);
    bytes.extend_from_slice(&commit.delta.0);
    Bytes::from(bytes)
}

fn decode_node(bytes: &[u8]) -> Result<Node, String> {
    let mut decoder = Decoder::new(bytes);
    match decoder.object_tag()? {
        LEAF_TAG => {
            let decoded_length = decoder.u32()?;
            let compressed = decoder.remaining();
            let decoded = zstd::bulk::decompress(compressed, decoded_length)
                .map_err(|error| format!("decompress ForkTree leaf: {error}"))?;
            decoder.finish()?;
            let mut body = Decoder::new(&decoded);
            let count = body.u32()?;
            let mut rows = Vec::with_capacity(count);
            for _ in 0..count {
                rows.push(LeafEntry {
                    key: body.bytes()?,
                    value: ValueRef {
                        pack: body.id()?,
                        index: body.u32_raw()?,
                    },
                });
            }
            body.finish()?;
            if rows.windows(2).any(|pair| pair[0].key >= pair[1].key) {
                return Err("ForkTree leaf keys are not sorted".to_string());
            }
            Ok(Node::Leaf(rows))
        }
        INTERNAL_TAG => {
            let count = decoder.u32()?;
            if count == 0 {
                return Err("empty ForkTree internal node".to_string());
            }
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                let shared_prefix = decoder.u32()?;
                let suffix = decoder.bytes()?;
                let previous_key = children
                    .last()
                    .map_or(&[][..], |child: &NodeRef| child.max_key.as_slice());
                if shared_prefix > previous_key.len() {
                    return Err("ForkTree internal separator prefix is invalid".to_string());
                }
                let mut max_key = previous_key[..shared_prefix].to_vec();
                max_key.extend_from_slice(&suffix);
                children.push(NodeRef {
                    max_key,
                    id: decoder.id()?,
                });
            }
            decoder.finish()?;
            if children
                .windows(2)
                .any(|pair| pair[0].max_key >= pair[1].max_key)
            {
                return Err("ForkTree internal child bounds are not sorted".to_string());
            }
            Ok(Node::Internal(children))
        }
        tag => Err(format!("object tag {tag} is not a tree node")),
    }
}

fn decode_value_pack(bytes: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut decoder = Decoder::new(bytes);
    if decoder.object_tag()? != VALUE_PACK_TAG {
        return Err("ForkTree leaf does not reference a value-pack object".to_string());
    }
    let decoded_length = decoder.u32()?;
    let compressed = decoder.remaining();
    let decoded = zstd::bulk::decompress(compressed, decoded_length)
        .map_err(|error| format!("decompress ForkTree value pack: {error}"))?;
    decoder.finish()?;
    let mut body = Decoder::new(&decoded);
    let count = body.u32()?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(body.bytes()?);
    }
    body.finish()?;
    Ok(values)
}

fn decode_commit(bytes: &[u8]) -> Result<Commit, String> {
    let mut decoder = Decoder::new(bytes);
    if decoder.object_tag()? != COMMIT_TAG {
        return Err("ForkTree head does not name a commit object".to_string());
    }
    let commit = Commit {
        parent: decoder.optional_id()?,
        root: decoder.id()?,
        delta: decoder.id()?,
    };
    decoder.finish()?;
    Ok(commit)
}

fn object_tag(bytes: &[u8]) -> Result<u8, String> {
    Decoder::new(bytes).object_tag()
}

fn object_prefix(tag: u8) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(OBJECT_MAGIC);
    bytes.push(tag);
    bytes
}

fn encode_ref(commit: ObjectId) -> Bytes {
    let mut bytes = Vec::with_capacity(36);
    bytes.extend_from_slice(REF_MAGIC);
    bytes.extend_from_slice(&commit.0);
    Bytes::from(bytes)
}

fn decode_ref(bytes: &[u8]) -> Result<ObjectId, String> {
    if bytes.len() != 36 || &bytes[..4] != REF_MAGIC {
        return Err("invalid ForkTree ref encoding".to_string());
    }
    let mut id = [0; 32];
    id.copy_from_slice(&bytes[4..]);
    Ok(ObjectId(id))
}

fn encode_epoch(epoch: u64) -> Bytes {
    Bytes::copy_from_slice(&epoch.to_be_bytes())
}

fn decode_epoch(bytes: &[u8]) -> Result<u64, String> {
    let encoded: [u8; 8] = bytes
        .try_into()
        .map_err(|_| "invalid ForkTree epoch encoding".to_string())?;
    Ok(u64::from_be_bytes(encoded))
}

fn put_optional_id(bytes: &mut Vec<u8>, id: Option<ObjectId>) {
    match id {
        Some(id) => {
            bytes.push(1);
            bytes.extend_from_slice(&id.0);
        }
        None => bytes.push(0),
    }
}

fn put_u32(bytes: &mut Vec<u8>, value: usize) {
    bytes.extend_from_slice(
        &u32::try_from(value)
            .expect("ForkTree encoded count fits u32")
            .to_be_bytes(),
    );
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) {
    put_u32(output, value.len());
    output.extend_from_slice(value);
}

fn key(value: &[u8]) -> Key {
    Key(Bytes::copy_from_slice(value))
}

fn authenticate(id: ObjectId, bytes: &[u8]) -> Result<(), String> {
    if blake3::hash(bytes).as_bytes() != &id.0 {
        return Err(format!(
            "ForkTree object {} failed authentication",
            hex_id(id)
        ));
    }
    Ok(())
}

fn projected_bytes(value: &ProjectedValue) -> Result<&Bytes, String> {
    match value {
        ProjectedValue::FullValue(bytes) => Ok(bytes),
        ProjectedValue::KeyOnly => {
            Err("ForkTree requested a value but received key-only data".to_string())
        }
    }
}

fn storage_error(error: StorageError) -> String {
    error.to_string()
}

fn hex_id(id: ObjectId) -> String {
    id.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn object_tag(&mut self) -> Result<u8, String> {
        if self.take(4)? != OBJECT_MAGIC {
            return Err("invalid ForkTree object magic".to_string());
        }
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<usize, String> {
        Ok(self.u32_raw()? as usize)
    }

    fn u32_raw(&mut self) -> Result<u32, String> {
        let encoded: [u8; 4] = self
            .take(4)?
            .try_into()
            .expect("decoder returns exact u32 width");
        Ok(u32::from_be_bytes(encoded))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, String> {
        let length = self.u32()?;
        Ok(self.take(length)?.to_vec())
    }

    fn id(&mut self) -> Result<ObjectId, String> {
        let mut id = [0; 32];
        id.copy_from_slice(self.take(32)?);
        Ok(ObjectId(id))
    }

    fn optional_id(&mut self) -> Result<Option<ObjectId>, String> {
        match self.take(1)?[0] {
            0 => Ok(None),
            1 => self.id().map(Some),
            tag => Err(format!("invalid optional object id tag {tag}")),
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| "ForkTree object length overflow".to_string())?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| "truncated ForkTree object".to_string())?;
        self.offset = end;
        Ok(value)
    }

    fn finish(self) -> Result<(), String> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("trailing bytes in ForkTree object".to_string())
        }
    }

    fn remaining(&mut self) -> &'a [u8] {
        let remaining = &self.bytes[self.offset..];
        self.offset = self.bytes.len();
        remaining
    }
}
