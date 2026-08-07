use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;

use bytes::{Bytes, BytesMut};
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
const BLOB_CHUNK_TAG: u8 = 6;
const BLOB_MANIFEST_TAG: u8 = 7;
pub(super) const LEAF_ROWS: usize = 64;
pub(super) const INTERNAL_CHILDREN: usize = 32;
const BLOB_MIN_BYTES: usize = 512 * 1024;
const BLOB_AVG_BYTES: usize = 512 * 1024;
const BLOB_MAX_BYTES: usize = 2 * 1024 * 1024;

pub const OBJECT_SPACE: StorageSpace =
    StorageSpace::immutable(SpaceId(0x00f0_0001), "forktree_objects");
pub const REF_SPACE: StorageSpace = StorageSpace::mutable(SpaceId(0x00f0_0002), "forktree_refs");

const MAIN_REF_KEY: &[u8] = b"branch/main";
const EPOCH_KEY: &[u8] = b"epoch";
const BRANCH_PREFIX: &[u8] = b"branch/";
const CHECKPOINT_PREFIX: &[u8] = b"checkpoint/";
const REDO_PREFIX: &[u8] = b"redo/";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObjectId([u8; 32]);

#[derive(Clone, Debug)]
pub struct Update {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RelationalValue {
    Null,
    Bytes(Vec<u8>),
}

impl RelationalValue {
    fn logical_bytes(&self) -> usize {
        match self {
            Self::Null => 1,
            Self::Bytes(bytes) => 1 + bytes.len(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Mutation {
    Insert {
        key: Vec<u8>,
        value: RelationalValue,
    },
    Update {
        key: Vec<u8>,
        value: RelationalValue,
    },
    Delete {
        key: Vec<u8>,
    },
}

impl Mutation {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Insert { key, .. } | Self::Update { key, .. } | Self::Delete { key } => key,
        }
    }

    fn value(&self) -> Option<&RelationalValue> {
        match self {
            Self::Insert { value, .. } | Self::Update { value, .. } => Some(value),
            Self::Delete { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeConflict {
    pub key: Vec<u8>,
    pub base: Option<RelationalValue>,
    pub target: Option<RelationalValue>,
    pub source: Option<RelationalValue>,
}

#[derive(Clone, Debug)]
pub enum MergeOutcome {
    Merged {
        commit: ObjectId,
        accounting: ApplyAccounting,
    },
    Conflicts(Vec<MergeConflict>),
}

#[derive(Clone, Copy, Debug, Default)]
pub struct BlobAccounting {
    pub chunks: u64,
    pub reused_chunks: u64,
    pub locality_hits: u64,
    pub locality_misses: u64,
    pub object_writes: u64,
    pub object_bytes: u64,
    pub logical_bytes: u64,
    pub chunking_us: u64,
    pub source_read_us: u64,
    pub object_hash_us: u64,
    pub object_encode_us: u64,
    pub dedup_read_us: u64,
    pub emission_us: u64,
    pub publication_us: u64,
    pub emission_batches: u64,
    pub peak_buffer_bytes: u64,
}

/// Canonical byte boundary for the replacement layout. Sources declare their
/// exact logical length and transfer immutable spans without requiring a
/// contiguous payload. Authentication remains owned by ForkTree and is
/// incremental over these spans.
pub trait SegmentedByteSource {
    fn logical_bytes(&self) -> u64;
    fn next_span(&mut self) -> Result<Option<Bytes>, String>;

    /// A streaming producer may reclaim a consumed span's backing allocation.
    /// Stored/read-backed sources can use the default drop behavior.
    fn recycle_span(&mut self, _span: Bytes) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct SegmentedBytes {
    logical_bytes: u64,
    spans: VecDeque<Bytes>,
}

impl SegmentedBytes {
    fn new(logical_bytes: u64, spans: VecDeque<Bytes>) -> Self {
        Self {
            logical_bytes,
            spans,
        }
    }

    pub fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }

    pub fn authenticated_hash(&self) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new();
        for span in &self.spans {
            hasher.update(span);
        }
        hasher.finalize()
    }

    /// Explicit outer-consumer materialization. The object-space read path
    /// itself remains segmented and does not allocate a second full payload.
    pub fn materialize(self) -> Vec<u8> {
        let mut output = Vec::with_capacity(self.logical_bytes as usize);
        for span in self.spans {
            output.extend_from_slice(&span);
        }
        output
    }
}

impl SegmentedByteSource for SegmentedBytes {
    fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }

    fn next_span(&mut self) -> Result<Option<Bytes>, String> {
        Ok(self.spans.pop_front())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct BlobDiff {
    pub before_chunks: u64,
    pub after_chunks: u64,
    pub shared_chunks: u64,
    pub changed_chunks: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ReclaimAccounting {
    pub roots: u64,
    pub reachable_objects: u64,
    pub scanned_objects: u64,
    pub reclaimed_objects: u64,
    pub reclaimed_bytes: u64,
    pub pages: u64,
    pub peak_frontier: u64,
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

#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectLayoutStats {
    pub objects: u64,
    pub object_value_bytes: u64,
    pub object_logical_key_bytes: u64,
    pub object_physical_key_bytes: u64,
    pub object_header_bytes: u64,
    pub reachable_objects: u64,
    pub unreachable_objects: u64,
    pub leaves: u64,
    pub leaf_rows: u64,
    pub leaf_encoded_bytes: u64,
    pub leaf_decoded_bytes: u64,
    pub leaf_key_bytes: u64,
    pub leaf_value_ref_bytes: u64,
    pub internals: u64,
    pub internal_children: u64,
    pub internal_encoded_bytes: u64,
    pub value_packs: u64,
    pub packed_values: u64,
    pub value_pack_encoded_bytes: u64,
    pub value_pack_decoded_bytes: u64,
    pub value_payload_bytes: u64,
    pub deltas: u64,
    pub delta_bytes: u64,
    pub commits: u64,
    pub commit_bytes: u64,
    pub blob_chunks: u64,
    pub blob_chunk_bytes: u64,
    pub blob_manifests: u64,
    pub blob_manifest_bytes: u64,
    pub selectors: u64,
    pub selector_logical_key_bytes: u64,
    pub selector_physical_key_bytes: u64,
    pub selector_value_bytes: u64,
}

/// Read-only authenticated state-shape evidence for the history-independence
/// benchmark. It exposes canonical object identities and tree boundaries but
/// cannot publish, route, or authorize an object.
#[derive(Clone, Debug)]
pub struct StateInspection {
    pub root: ObjectId,
    pub object_bytes: BTreeMap<ObjectId, u64>,
    pub leaf_ranges: Vec<(Vec<u8>, Vec<u8>)>,
    pub internal_boundaries: Vec<Vec<Vec<u8>>>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ValueRef {
    pack: ObjectId,
    index: u32,
}

#[derive(Clone, Debug)]
struct ResolvedMutation {
    key: Vec<u8>,
    operation: ResolvedOperation,
}

#[derive(Clone, Copy, Debug)]
enum ResolvedOperation {
    Insert(ValueRef),
    Update(ValueRef),
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RowChange {
    key: Vec<u8>,
    before: Option<RelationalValue>,
    after: Option<RelationalValue>,
}

#[derive(Clone, Copy)]
struct Commit {
    parents: [Option<ObjectId>; 2],
    root: ObjectId,
    delta: ObjectId,
    blob: Option<ObjectId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BlobChunkRef {
    id: ObjectId,
    bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BlobManifest {
    logical_bytes: u64,
    chunks: Vec<BlobChunkRef>,
}

#[derive(Clone, Copy, Debug, Default)]
struct ChunkEmissionAccounting {
    object_writes: u64,
    object_bytes: u64,
    emission_us: u64,
    emission_batches: u64,
}

struct SegmentedChunk {
    id: ObjectId,
    payload: ChunkPayload,
}

#[derive(Clone)]
struct ChunkPayload {
    spans: Vec<Bytes>,
    bytes: usize,
}

impl ChunkPayload {
    fn into_contiguous(self, crossing_buffer: &mut BytesMut) -> (Bytes, bool) {
        if self.spans.len() == 1 {
            return (
                self.spans.into_iter().next().expect("one chunk span"),
                false,
            );
        }
        let mut encoded = std::mem::take(crossing_buffer);
        encoded.clear();
        encoded.reserve(self.bytes);
        for span in self.spans {
            encoded.extend_from_slice(&span);
        }
        (encoded.freeze(), true)
    }
}

struct SourceSpan {
    bytes: Bytes,
    offset: usize,
}

/// Bounded cursor over one authenticated segmented source. It retains only
/// spans needed by the current chunk/emission batch. Runtime memory is
/// O(source span window + BLOB_MAX_BYTES), independent of logical length.
struct SegmentedCursor<R> {
    source: R,
    spans: VecDeque<SourceSpan>,
    logical_bytes: u64,
    delivered_bytes: u64,
    consumed_bytes: u64,
    available_bytes: usize,
    source_read_us: u64,
}

impl<R> SegmentedCursor<R>
where
    R: SegmentedByteSource,
{
    fn new(source: R) -> Self {
        let logical_bytes = source.logical_bytes();
        Self {
            source,
            spans: VecDeque::new(),
            logical_bytes,
            delivered_bytes: 0,
            consumed_bytes: 0,
            available_bytes: 0,
            source_read_us: 0,
        }
    }

    fn remaining_bytes(&self) -> u64 {
        self.logical_bytes.saturating_sub(self.consumed_bytes)
    }

    fn front_remaining(&self) -> usize {
        self.spans
            .front()
            .map_or(0, |span| span.bytes.len().saturating_sub(span.offset))
    }

    fn front_consumed(&self) -> bool {
        self.spans
            .front()
            .is_some_and(|span| span.offset == span.bytes.len())
    }

    fn ensure_available(&mut self, wanted: usize) -> Result<(), String> {
        let wanted = wanted.min(self.remaining_bytes() as usize);
        while self.available_bytes < wanted && self.delivered_bytes < self.logical_bytes {
            let started = std::time::Instant::now();
            let span = self.source.next_span()?;
            self.source_read_us += started.elapsed().as_micros() as u64;
            let span = span.ok_or_else(|| {
                "ForkTree segmented source ended before its declared length".to_string()
            })?;
            if span.is_empty() {
                return Err("ForkTree segmented source yielded an empty span".to_string());
            }
            let delivered = self
                .delivered_bytes
                .checked_add(span.len() as u64)
                .ok_or_else(|| "ForkTree segmented source length overflow".to_string())?;
            if delivered > self.logical_bytes {
                return Err("ForkTree segmented source exceeded its declared length".to_string());
            }
            self.delivered_bytes = delivered;
            self.available_bytes = self
                .available_bytes
                .checked_add(span.len())
                .ok_or_else(|| "ForkTree segmented source window overflow".to_string())?;
            self.spans.push_back(SourceSpan {
                bytes: span,
                offset: 0,
            });
        }
        if self.available_bytes < wanted {
            return Err("ForkTree segmented source is shorter than declared".to_string());
        }
        Ok(())
    }

    fn gather(&self, bytes: usize) -> Result<ChunkPayload, String> {
        if bytes == 0 || bytes > self.available_bytes {
            return Err("ForkTree segmented source gather is out of bounds".to_string());
        }
        let mut remaining = bytes;
        let mut spans = Vec::new();
        for source in &self.spans {
            if source.offset == source.bytes.len() {
                continue;
            }
            let available = source.bytes.len() - source.offset;
            let take = available.min(remaining);
            spans.push(source.bytes.slice(source.offset..source.offset + take));
            remaining -= take;
            if remaining == 0 {
                break;
            }
        }
        if remaining != 0 {
            return Err("ForkTree segmented source has a discontinuous span window".to_string());
        }
        Ok(ChunkPayload { spans, bytes })
    }

    fn copy_prefix(&self, bytes: usize, output: &mut BytesMut) -> Result<(), String> {
        let payload = self.gather(bytes)?;
        output.clear();
        output.reserve(bytes);
        for span in payload.spans {
            output.extend_from_slice(&span);
        }
        Ok(())
    }

    fn advance(&mut self, bytes: usize) -> Result<(), String> {
        if bytes == 0 || bytes > self.available_bytes {
            return Err("ForkTree segmented source advance is out of bounds".to_string());
        }
        let mut remaining = bytes;
        for span in &mut self.spans {
            let available = span.bytes.len().saturating_sub(span.offset);
            let take = available.min(remaining);
            span.offset += take;
            remaining -= take;
            if remaining == 0 {
                break;
            }
        }
        if remaining != 0 {
            return Err("ForkTree segmented source advance crossed a gap".to_string());
        }
        self.available_bytes -= bytes;
        self.consumed_bytes = self
            .consumed_bytes
            .checked_add(bytes as u64)
            .ok_or_else(|| "ForkTree segmented source consumed length overflow".to_string())?;
        Ok(())
    }

    fn recycle_consumed(&mut self) -> Result<(), String> {
        while self.front_consumed() {
            let span = self.spans.pop_front().expect("consumed source span");
            self.source.recycle_span(span.bytes)?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<u64, String> {
        self.recycle_consumed()?;
        if self.consumed_bytes != self.logical_bytes
            || self.delivered_bytes != self.logical_bytes
            || self.available_bytes != 0
            || !self.spans.is_empty()
        {
            return Err("ForkTree segmented source did not finish exactly".to_string());
        }
        let started = std::time::Instant::now();
        let trailing = self.source.next_span()?;
        self.source_read_us += started.elapsed().as_micros() as u64;
        if trailing.is_some() {
            return Err(
                "ForkTree segmented source has bytes beyond its declared length".to_string(),
            );
        }
        Ok(self.source_read_us)
    }
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
        let root = build_tree(rows, &mut pending)?;
        let delta = stage_object(encode_initial_delta(root.id, rows.len()), &mut pending);
        let commit = stage_object(
            encode_commit(Commit {
                parents: [None, None],
                root: root.id,
                delta,
                blob: None,
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
        self.apply_sorted_updates_on("main", updates).await
    }

    /// Publishes a new branch selector and rotates the one mutable epoch in the
    /// same adapter commit. No immutable object is copied or rewritten.
    pub async fn create_branch(
        &self,
        name: &str,
        from: Option<ObjectId>,
    ) -> Result<ObjectId, String> {
        let source = match from {
            Some(commit) => commit,
            None => self.load_head_at_key(MAIN_REF_KEY).await?.commit,
        };
        self.put_new_selector(selector_key(BRANCH_PREFIX, name), source)
            .await?;
        Ok(source)
    }

    /// Pins an existing commit as a checkpoint using only the ref/epoch plane.
    pub async fn create_checkpoint(&self, name: &str, commit: ObjectId) -> Result<(), String> {
        self.put_new_selector(selector_key(CHECKPOINT_PREFIX, name), commit)
            .await
    }

    pub async fn delete_branch(&self, name: &str) -> Result<(), String> {
        self.delete_selector(selector_key(BRANCH_PREFIX, name))
            .await
    }

    pub async fn delete_checkpoint(&self, name: &str) -> Result<(), String> {
        self.delete_selector(selector_key(CHECKPOINT_PREFIX, name))
            .await
    }

    /// Moves the branch selector to its first parent and retains the old head
    /// in a redo selector. Both selector changes and the epoch rotate atomically.
    pub async fn undo(&self, branch: &str) -> Result<ObjectId, String> {
        let branch_key = selector_key(BRANCH_PREFIX, branch);
        let redo_key = selector_key(REDO_PREFIX, branch);
        let head = self.load_head_at_key(&branch_key).await?;
        let parent = self.load_commit(head.commit).await?.parents[0]
            .ok_or_else(|| "ForkTree root commit cannot be undone".to_string())?;
        let parent_ref = encode_ref(parent);
        let redo_ref = encode_ref(head.commit);
        let next_epoch = encode_epoch(head.epoch.saturating_add(1));
        let mut write = self
            .storage
            .begin_write(WriteOptions {
                preconditions: vec![
                    Precondition::KeyValueEquals {
                        space: REF_SPACE,
                        key: key(&branch_key),
                        expected: head.raw_ref,
                    },
                    Precondition::KeyAbsent {
                        space: REF_SPACE,
                        key: key(&redo_key),
                    },
                    Precondition::KeyValueEquals {
                        space: REF_SPACE,
                        key: key(EPOCH_KEY),
                        expected: head.raw_epoch,
                    },
                ],
                batch_capacity_hint_bytes: parent_ref.len() + redo_ref.len() + next_epoch.len(),
                ..WriteOptions::default()
            })
            .await
            .map_err(storage_error)?;
        write
            .put_many(
                REF_SPACE,
                PutBatch {
                    entries: vec![
                        PutEntry {
                            key: key(&branch_key),
                            value: StoredValue { bytes: parent_ref },
                        },
                        PutEntry {
                            key: key(&redo_key),
                            value: StoredValue { bytes: redo_ref },
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
        Ok(parent)
    }

    pub async fn redo(&self, branch: &str) -> Result<ObjectId, String> {
        let branch_key = selector_key(BRANCH_PREFIX, branch);
        let redo_key = selector_key(REDO_PREFIX, branch);
        let head = self.load_head_at_key(&branch_key).await?;
        let redo_raw = self
            .load_raw_selector(&redo_key)
            .await?
            .ok_or_else(|| "ForkTree branch has no redo selector".to_string())?;
        let redo = decode_ref(&redo_raw)?;
        let next_ref = encode_ref(redo);
        let next_epoch = encode_epoch(head.epoch.saturating_add(1));
        let mut write = self
            .storage
            .begin_write(WriteOptions {
                preconditions: vec![
                    Precondition::KeyValueEquals {
                        space: REF_SPACE,
                        key: key(&branch_key),
                        expected: head.raw_ref,
                    },
                    Precondition::KeyValueEquals {
                        space: REF_SPACE,
                        key: key(&redo_key),
                        expected: redo_raw,
                    },
                    Precondition::KeyValueEquals {
                        space: REF_SPACE,
                        key: key(EPOCH_KEY),
                        expected: head.raw_epoch,
                    },
                ],
                batch_capacity_hint_bytes: next_ref.len() + next_epoch.len(),
                ..WriteOptions::default()
            })
            .await
            .map_err(storage_error)?;
        write
            .put_many(
                REF_SPACE,
                PutBatch {
                    entries: vec![
                        PutEntry {
                            key: key(&branch_key),
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
        write
            .delete_many(REF_SPACE, &[key(&redo_key)])
            .await
            .map_err(storage_error)?;
        write.commit().await.map_err(storage_error)?;
        Ok(redo)
    }

    pub async fn apply_sorted_updates_on(
        &self,
        branch: &str,
        updates: &[Update],
    ) -> Result<(ObjectId, ApplyAccounting), String> {
        let mutations = updates
            .iter()
            .map(|update| Mutation::Update {
                key: update.key.clone(),
                value: RelationalValue::Bytes(update.value.clone()),
            })
            .collect::<Vec<_>>();
        self.apply_sorted_mutations_with_merge_parent(branch, &mutations, None)
            .await
    }

    pub async fn apply_sorted_mutations(
        &self,
        mutations: &[Mutation],
    ) -> Result<(ObjectId, ApplyAccounting), String> {
        self.apply_sorted_mutations_on("main", mutations).await
    }

    pub async fn apply_sorted_mutations_on(
        &self,
        branch: &str,
        mutations: &[Mutation],
    ) -> Result<(ObjectId, ApplyAccounting), String> {
        self.apply_sorted_mutations_with_merge_parent(branch, mutations, None)
            .await
    }

    async fn apply_sorted_mutations_with_merge_parent(
        &self,
        branch: &str,
        mutations: &[Mutation],
        merge_parent: Option<ObjectId>,
    ) -> Result<(ObjectId, ApplyAccounting), String> {
        validate_sorted_mutations(mutations, merge_parent.is_some())?;
        let branch_key = selector_key(BRANCH_PREFIX, branch);
        let head = self.load_head_at_key(&branch_key).await?;
        let commit = self.load_commit(head.commit).await?;
        let mut pending = BTreeMap::new();
        let mut accounting = ApplyAccounting {
            logical_bytes: mutations
                .iter()
                .map(|mutation| {
                    mutation.key().len() as u64
                        + mutation
                            .value()
                            .map_or(0, |value| value.logical_bytes() as u64)
                        + 1
                })
                .sum(),
            ..ApplyAccounting::default()
        };
        let values = mutations
            .iter()
            .filter_map(Mutation::value)
            .collect::<Vec<_>>();
        let value_pack = (!values.is_empty())
            .then(|| stage_object(encode_value_pack(values.iter().copied()), &mut pending));
        let mut value_index = 0_u32;
        let resolved_mutations = mutations
            .iter()
            .map(|mutation| {
                let value_ref = mutation.value().map(|_| {
                    let value = ValueRef {
                        pack: value_pack.expect("nonempty values have a pack"),
                        index: value_index,
                    };
                    value_index = value_index
                        .checked_add(1)
                        .expect("ForkTree value-pack index fits u32");
                    value
                });
                ResolvedMutation {
                    key: mutation.key().to_vec(),
                    operation: match mutation {
                        Mutation::Insert { .. } => {
                            ResolvedOperation::Insert(value_ref.expect("insert value"))
                        }
                        Mutation::Update { .. } => {
                            ResolvedOperation::Update(value_ref.expect("update value"))
                        }
                        Mutation::Delete { .. } => ResolvedOperation::Delete,
                    },
                }
            })
            .collect::<Vec<_>>();
        // One operation-local authenticated working set batches every stored
        // node needed by the changed paths and their bounded rebalance
        // siblings. It is derived from the authoritative root, discarded
        // after publication, and grows only with copied blocks; it is not a
        // durable or serving-side index/cache authority.
        let node_cache = self
            .load_mutation_working_set(commit.root, &resolved_mutations)
            .await?;
        let rewritten =
            if resolved_mutations.is_empty() {
                vec![NodeRef {
                    id: commit.root,
                    max_key: node_max_key(node_cache.get(&commit.root).ok_or_else(|| {
                        "ForkTree root is absent from its working set".to_string()
                    })?),
                }]
            } else {
                self.rewrite_general(commit.root, &resolved_mutations, &node_cache, &mut pending)
                    .await?
            };
        let root = self.finish_root(rewritten, &node_cache, &mut pending)?;
        let delta = stage_object(encode_mutation_delta(value_pack, mutations), &mut pending);
        let next_commit = stage_object(
            encode_commit(Commit {
                parents: [Some(head.commit), merge_parent],
                root: root.id,
                delta,
                blob: commit.blob,
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
                    key: key(&branch_key),
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
                            key: key(&branch_key),
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

    pub async fn branch_head(&self, branch: &str) -> Result<ObjectId, String> {
        self.load_head_at_key(&selector_key(BRANCH_PREFIX, branch))
            .await
            .map(|head| head.commit)
    }

    pub async fn checkpoint_head(&self, name: &str) -> Result<ObjectId, String> {
        self.load_head_at_key(&selector_key(CHECKPOINT_PREFIX, name))
            .await
            .map(|head| head.commit)
    }

    pub async fn read_point(&self, branch: &str, key: &[u8]) -> Result<Vec<u8>, String> {
        let commit = self.load_commit(self.branch_head(branch).await?).await?;
        let value = self.find_value(commit.root, key).await?;
        self.load_value(value).await
    }

    pub async fn read_relational_point(
        &self,
        branch: &str,
        key: &[u8],
    ) -> Result<Option<RelationalValue>, String> {
        let commit = self.load_commit(self.branch_head(branch).await?).await?;
        match self.find_value_optional(commit.root, key).await? {
            Some(value) => self.load_relational_value(value).await.map(Some),
            None => Ok(None),
        }
    }

    pub async fn read_relational_all(
        &self,
        branch: &str,
    ) -> Result<Vec<(Vec<u8>, RelationalValue)>, String> {
        self.read_relational_all_at(self.branch_head(branch).await?)
            .await
    }

    pub async fn read_relational_all_at(
        &self,
        commit: ObjectId,
    ) -> Result<Vec<(Vec<u8>, RelationalValue)>, String> {
        let commit = self.load_commit(commit).await?;
        let mut rows = Vec::new();
        self.collect_relational_rows(commit.root, &mut rows).await?;
        Ok(rows)
    }

    pub async fn read_range(
        &self,
        branch: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        if start > end {
            return Err("ForkTree range start exceeds end".to_string());
        }
        let commit = self.load_commit(self.branch_head(branch).await?).await?;
        let mut entries = Vec::new();
        self.collect_range_entries(commit.root, start, end, &mut entries)
            .await?;
        let mut output = Vec::with_capacity(entries.len());
        for entry in entries {
            output.push((entry.key, self.load_value(entry.value).await?));
        }
        Ok(output)
    }

    pub async fn diff_commits(
        &self,
        before: ObjectId,
        after: ObjectId,
    ) -> Result<Vec<Vec<u8>>, String> {
        Ok(self
            .diff_rows(before, after)
            .await?
            .into_iter()
            .map(|change| change.key)
            .collect())
    }

    pub async fn merge_branches(
        &self,
        target: &str,
        source: &str,
        base: ObjectId,
    ) -> Result<(ObjectId, ApplyAccounting), String> {
        match self.merge_branches_three_way(target, source, base).await? {
            MergeOutcome::Merged { commit, accounting } => Ok((commit, accounting)),
            MergeOutcome::Conflicts(conflicts) => Err(format!(
                "ForkTree three-way merge has {} semantic conflict(s)",
                conflicts.len()
            )),
        }
    }

    pub async fn merge_branches_three_way(
        &self,
        target: &str,
        source: &str,
        base: ObjectId,
    ) -> Result<MergeOutcome, String> {
        let target_head = self.branch_head(target).await?;
        let source_head = self.branch_head(source).await?;
        let source_changes = self
            .diff_rows(base, source_head)
            .await?
            .into_iter()
            .map(|change| (change.key.clone(), change))
            .collect::<BTreeMap<_, _>>();
        let target_changes = self
            .diff_rows(base, target_head)
            .await?
            .into_iter()
            .map(|change| (change.key.clone(), change))
            .collect::<BTreeMap<_, _>>();
        let identities = source_changes
            .keys()
            .chain(target_changes.keys())
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let mut conflicts = Vec::new();
        let mut mutations = Vec::new();
        for key in identities {
            let source = source_changes.get(&key);
            let target = target_changes.get(&key);
            let base_value = source
                .and_then(|change| change.before.clone())
                .or_else(|| target.and_then(|change| change.before.clone()));
            let source_value =
                source.map_or_else(|| base_value.clone(), |change| change.after.clone());
            let target_value =
                target.map_or_else(|| base_value.clone(), |change| change.after.clone());
            if source_value == target_value || source_value == base_value {
                continue;
            }
            if target_value != base_value {
                conflicts.push(MergeConflict {
                    key,
                    base: base_value,
                    target: target_value,
                    source: source_value,
                });
                continue;
            }
            mutations.push(match (target_value, source_value) {
                (None, Some(value)) => Mutation::Insert { key, value },
                (Some(_), Some(value)) => Mutation::Update { key, value },
                (Some(_), None) => Mutation::Delete { key },
                (None, None) => continue,
            });
        }
        if !conflicts.is_empty() {
            return Ok(MergeOutcome::Conflicts(conflicts));
        }
        let (commit, accounting) = self
            .apply_sorted_mutations_with_merge_parent(target, &mutations, Some(source_head))
            .await?;
        Ok(MergeOutcome::Merged { commit, accounting })
    }

    /// Consumes one canonical segmented source through the single CDC profile,
    /// incrementally authenticates borrowed spans, emits immutable chunks in
    /// bounded batches, then atomically publishes only metadata, the branch
    /// selector, and the epoch. FastCDC scans a source span directly; only a
    /// boundary crossing uses a BLOB_MAX_BYTES scratch. A crash before final
    /// publication can leave unreachable immutable objects, never a partial
    /// live blob; reclamation owns those objects later.
    pub async fn ingest_blob<R>(
        &self,
        branch: &str,
        source: R,
    ) -> Result<(ObjectId, BlobAccounting), String>
    where
        R: SegmentedByteSource,
    {
        let branch_key = selector_key(BRANCH_PREFIX, branch);
        let head = self.load_head_at_key(&branch_key).await?;
        let current = self.load_commit(head.commit).await?;
        let previous_manifest = self.load_blob_manifest(current.blob).await?;
        let previous_chunk_positions = previous_manifest
            .chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| (chunk.id, index))
            .collect::<BTreeMap<_, _>>();
        let mut previous_chunk_cursor = 0_usize;
        let mut accounting = BlobAccounting::default();
        let mut chunks = Vec::new();
        let mut unique_chunks = std::collections::BTreeSet::new();
        let mut cursor = SegmentedCursor::new(source);
        let mut boundary_scratch = BytesMut::new();
        let mut crossing_buffer = BytesMut::new();

        while cursor.remaining_bytes() != 0 {
            cursor.ensure_available(1)?;
            if cursor.front_remaining() == 0 {
                return Err("ForkTree segmented source made no progress".to_string());
            }
            let mut segmented_chunks = Vec::new();

            // One emission batch owns chunks beginning in the current source
            // span. It may borrow following spans for a crossing chunk, then
            // releases/recycles every fully consumed source allocation.
            while !cursor.front_consumed() {
                let previous = previous_manifest.chunks.get(previous_chunk_cursor);
                let mut predicted = None;
                if let Some(previous) = previous {
                    let previous_bytes = usize::try_from(previous.bytes)
                        .map_err(|_| "ForkTree prior blob chunk length exceeds usize")?;
                    if previous_bytes as u64 <= cursor.remaining_bytes() {
                        cursor.ensure_available(previous_bytes)?;
                        let payload = cursor.gather(previous_bytes)?;
                        let hash_started = std::time::Instant::now();
                        let id = blob_chunk_id_segments(&payload);
                        accounting.object_hash_us += hash_started.elapsed().as_micros() as u64;
                        predicted = Some((previous_bytes, id, payload));
                        if id == previous.id {
                            accounting.locality_hits += 1;
                        } else {
                            accounting.locality_misses += 1;
                        }
                    }
                }

                let (chunk_bytes, id, known_existing, payload) = match (previous, predicted) {
                    (Some(previous), Some((bytes, id, payload))) if id == previous.id => {
                        previous_chunk_cursor += 1;
                        (bytes, id, true, payload)
                    }
                    (previous, predicted) => {
                        let inspect_bytes = BLOB_MAX_BYTES.min(cursor.remaining_bytes() as usize);
                        cursor.ensure_available(inspect_bytes)?;
                        let chunking_started = std::time::Instant::now();
                        let chunk_bytes = if cursor.front_remaining() >= inspect_bytes {
                            let source = cursor
                                .spans
                                .front()
                                .ok_or_else(|| "ForkTree CDC source span is missing".to_string())?;
                            let (_, bytes) = fastcdc::v2020::cut(
                                &source.bytes[source.offset..source.offset + inspect_bytes],
                                BLOB_MIN_BYTES,
                                BLOB_AVG_BYTES,
                                BLOB_MAX_BYTES,
                                fastcdc::v2020::MASKS[20],
                                fastcdc::v2020::MASKS[18],
                                fastcdc::v2020::MASKS[20] << 1,
                                fastcdc::v2020::MASKS[18] << 1,
                            );
                            bytes
                        } else {
                            cursor.copy_prefix(inspect_bytes, &mut boundary_scratch)?;
                            accounting.peak_buffer_bytes = accounting
                                .peak_buffer_bytes
                                .max(boundary_scratch.len() as u64);
                            let (_, bytes) = fastcdc::v2020::cut(
                                &boundary_scratch,
                                BLOB_MIN_BYTES,
                                BLOB_AVG_BYTES,
                                BLOB_MAX_BYTES,
                                fastcdc::v2020::MASKS[20],
                                fastcdc::v2020::MASKS[18],
                                fastcdc::v2020::MASKS[20] << 1,
                                fastcdc::v2020::MASKS[18] << 1,
                            );
                            bytes
                        };
                        accounting.chunking_us += chunking_started.elapsed().as_micros() as u64;
                        if chunk_bytes == 0 || chunk_bytes > inspect_bytes {
                            return Err("ForkTree CDC produced an invalid chunk size".to_string());
                        }
                        let payload = if let Some((predicted_bytes, _, payload)) = predicted {
                            if predicted_bytes == chunk_bytes {
                                payload
                            } else {
                                cursor.gather(chunk_bytes)?
                            }
                        } else {
                            cursor.gather(chunk_bytes)?
                        };
                        let hash_started = std::time::Instant::now();
                        let id = blob_chunk_id_segments(&payload);
                        accounting.object_hash_us += hash_started.elapsed().as_micros() as u64;
                        let known_existing = if let Some(index) = previous_chunk_positions.get(&id)
                        {
                            previous_chunk_cursor = index.saturating_add(1);
                            true
                        } else if previous.is_some_and(|chunk| chunk.bytes == chunk_bytes as u64) {
                            previous_chunk_cursor = previous_chunk_cursor.saturating_add(1);
                            false
                        } else {
                            false
                        };
                        (chunk_bytes, id, known_existing, payload)
                    }
                };
                chunks.push(BlobChunkRef {
                    id,
                    bytes: chunk_bytes as u64,
                });
                accounting.logical_bytes += chunk_bytes as u64;
                if !unique_chunks.insert(id) || known_existing {
                    accounting.reused_chunks += 1;
                } else {
                    segmented_chunks.push(SegmentedChunk { id, payload });
                }
                cursor.advance(chunk_bytes)?;
            }

            let ids = segmented_chunks
                .iter()
                .map(|chunk| chunk.id)
                .collect::<Vec<_>>();
            let dedup_started = std::time::Instant::now();
            let existing = self.existing_object_ids(&ids).await?;
            accounting.dedup_read_us += dedup_started.elapsed().as_micros() as u64;
            let mut pending_chunks = BTreeMap::new();
            let mut crossing_id = None;
            for chunk in segmented_chunks {
                if existing.contains(&chunk.id) {
                    accounting.reused_chunks += 1;
                    continue;
                }
                let encode_started = std::time::Instant::now();
                let (payload, crossed_source_span) =
                    chunk.payload.into_contiguous(&mut crossing_buffer);
                accounting.object_encode_us += encode_started.elapsed().as_micros() as u64;
                accounting.peak_buffer_bytes = accounting
                    .peak_buffer_bytes
                    .max(payload.len() as u64 * u64::from(crossed_source_span));
                if crossed_source_span {
                    if crossing_id.replace(chunk.id).is_some() {
                        return Err(
                            "ForkTree emission batch has multiple crossing chunks".to_string()
                        );
                    }
                }
                pending_chunks.insert(chunk.id, payload);
            }
            let emitted = self.emit_chunk_batch(&mut pending_chunks).await?;
            accounting.object_writes += emitted.object_writes;
            accounting.object_bytes += emitted.object_bytes;
            accounting.emission_us += emitted.emission_us;
            accounting.emission_batches += emitted.emission_batches;
            if let Some(id) = crossing_id {
                let encoded = pending_chunks.remove(&id).ok_or_else(|| {
                    "ForkTree crossing chunk disappeared after emission".to_string()
                })?;
                crossing_buffer = encoded.try_into_mut().map_err(|_| {
                    "ForkTree storage retained a committed crossing chunk".to_string()
                })?;
            }
            pending_chunks.clear();
            cursor.recycle_consumed()?;
        }
        accounting.source_read_us = cursor.finish()?;

        let manifest = BlobManifest {
            logical_bytes: accounting.logical_bytes,
            chunks,
        };
        let mut pending = BTreeMap::new();
        let manifest_id = if manifest == previous_manifest {
            current
                .blob
                .ok_or_else(|| "ForkTree identical blob has no prior manifest".to_string())?
        } else {
            stage_object(encode_blob_manifest(&manifest), &mut pending)
        };
        let delta = stage_object(encode_blob_delta(current.blob, manifest_id), &mut pending);
        let next_commit = stage_object(
            encode_commit(Commit {
                parents: [Some(head.commit), None],
                root: current.root,
                delta,
                blob: Some(manifest_id),
            }),
            &mut pending,
        );
        self.remove_existing_objects(&mut pending).await?;
        accounting.chunks = manifest.chunks.len() as u64;
        accounting.object_writes += pending.len() as u64;
        accounting.object_bytes += pending
            .values()
            .map(|bytes| bytes.len() as u64)
            .sum::<u64>();

        let next_ref = encode_ref(next_commit);
        let next_epoch = encode_epoch(head.epoch.saturating_add(1));
        let preconditions = vec![
            Precondition::KeyValueEquals {
                space: REF_SPACE,
                key: key(&branch_key),
                expected: head.raw_ref,
            },
            Precondition::KeyValueEquals {
                space: REF_SPACE,
                key: key(EPOCH_KEY),
                expected: head.raw_epoch,
            },
        ];
        let publication_started = std::time::Instant::now();
        let mut write = self
            .storage
            .begin_write(WriteOptions {
                preconditions,
                batch_capacity_hint_bytes: pending.values().map(Bytes::len).sum::<usize>()
                    + next_ref.len()
                    + next_epoch.len(),
                ..WriteOptions::default()
            })
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
                            key: key(&branch_key),
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
        accounting.publication_us = publication_started.elapsed().as_micros() as u64;
        Ok((next_commit, accounting))
    }

    async fn emit_chunk_batch(
        &self,
        pending: &mut BTreeMap<ObjectId, Bytes>,
    ) -> Result<ChunkEmissionAccounting, String> {
        if pending.is_empty() {
            return Ok(ChunkEmissionAccounting::default());
        }
        let object_writes = pending.len() as u64;
        let object_bytes = pending.values().map(|bytes| bytes.len() as u64).sum();
        let mut accounting = ChunkEmissionAccounting {
            object_writes,
            object_bytes,
            ..ChunkEmissionAccounting::default()
        };
        let emission_started = std::time::Instant::now();
        let mut write = self
            .storage
            .begin_write(WriteOptions {
                batch_capacity_hint_bytes: usize::try_from(object_bytes).unwrap_or(usize::MAX),
                ..WriteOptions::default()
            })
            .await
            .map_err(storage_error)?;
        write
            .put_many(OBJECT_SPACE, object_batch(pending))
            .await
            .map_err(storage_error)?;
        write.commit().await.map_err(storage_error)?;
        accounting.emission_us = emission_started.elapsed().as_micros() as u64;
        accounting.emission_batches = 1;
        Ok(accounting)
    }

    pub async fn read_blob(&self, branch: &str) -> Result<SegmentedBytes, String> {
        let commit = self.load_commit(self.branch_head(branch).await?).await?;
        let manifest_id = commit
            .blob
            .ok_or_else(|| "ForkTree branch has no blob root".to_string())?;
        let manifest = decode_blob_manifest(&self.load_object(manifest_id).await?)?;
        self.read_blob_manifest_range(&manifest, 0, manifest.logical_bytes)
            .await
    }

    pub async fn read_blob_range(
        &self,
        branch: &str,
        start: u64,
        end: u64,
    ) -> Result<SegmentedBytes, String> {
        let commit = self.load_commit(self.branch_head(branch).await?).await?;
        let manifest_id = commit
            .blob
            .ok_or_else(|| "ForkTree branch has no blob root".to_string())?;
        let manifest = decode_blob_manifest(&self.load_object(manifest_id).await?)?;
        self.read_blob_manifest_range(&manifest, start, end).await
    }

    pub async fn diff_blob_commits(
        &self,
        before: ObjectId,
        after: ObjectId,
    ) -> Result<BlobDiff, String> {
        let before = self.load_commit(before).await?;
        let after = self.load_commit(after).await?;
        let before = self.load_blob_manifest(before.blob).await?;
        let after = self.load_blob_manifest(after.blob).await?;
        let before_ids = before
            .chunks
            .iter()
            .map(|chunk| chunk.id)
            .collect::<std::collections::BTreeSet<_>>();
        let after_ids = after
            .chunks
            .iter()
            .map(|chunk| chunk.id)
            .collect::<std::collections::BTreeSet<_>>();
        let shared = before_ids.intersection(&after_ids).count() as u64;
        Ok(BlobDiff {
            before_chunks: before.chunks.len() as u64,
            after_chunks: after.chunks.len() as u64,
            shared_chunks: shared,
            changed_chunks: before_ids.symmetric_difference(&after_ids).count() as u64,
        })
    }

    pub async fn merge_blob_branches(
        &self,
        target: &str,
        source: &str,
        base: ObjectId,
    ) -> Result<(ObjectId, BlobAccounting), String> {
        let target_key = selector_key(BRANCH_PREFIX, target);
        let target_head = self.load_head_at_key(&target_key).await?;
        let source_head = self.branch_head(source).await?;
        let base_commit = self.load_commit(base).await?;
        let target_commit = self.load_commit(target_head.commit).await?;
        let source_commit = self.load_commit(source_head).await?;
        if target_commit.blob != base_commit.blob {
            return Err("ForkTree blob merge target changed from its base".to_string());
        }
        let source_blob = source_commit
            .blob
            .ok_or_else(|| "ForkTree blob merge source has no blob".to_string())?;
        let mut pending = BTreeMap::new();
        let delta = stage_object(
            encode_blob_delta(target_commit.blob, source_blob),
            &mut pending,
        );
        let next_commit = stage_object(
            encode_commit(Commit {
                parents: [Some(target_head.commit), Some(source_head)],
                root: target_commit.root,
                delta,
                blob: Some(source_blob),
            }),
            &mut pending,
        );
        self.remove_existing_objects(&mut pending).await?;
        let accounting = BlobAccounting {
            object_writes: pending.len() as u64,
            object_bytes: pending.values().map(|bytes| bytes.len() as u64).sum(),
            ..BlobAccounting::default()
        };
        let next_ref = encode_ref(next_commit);
        let next_epoch = encode_epoch(target_head.epoch.saturating_add(1));
        let mut write = self
            .storage
            .begin_write(WriteOptions {
                preconditions: vec![
                    Precondition::KeyValueEquals {
                        space: REF_SPACE,
                        key: key(&target_key),
                        expected: target_head.raw_ref,
                    },
                    Precondition::KeyValueEquals {
                        space: REF_SPACE,
                        key: key(EPOCH_KEY),
                        expected: target_head.raw_epoch,
                    },
                ],
                batch_capacity_hint_bytes: accounting.object_bytes as usize
                    + next_ref.len()
                    + next_epoch.len(),
                ..WriteOptions::default()
            })
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
                            key: key(&target_key),
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

    /// Marks from every authenticated selector and sweeps the one immutable
    /// object space in bounded pages. Each deleting page rotates the same epoch
    /// used by all publications, so stale publication and stale sweep commits
    /// cannot both succeed.
    pub async fn reclaim_unreachable(&self) -> Result<ReclaimAccounting, String> {
        const PAGE: usize = 512;
        let mut accounting = ReclaimAccounting::default();
        // Fence root discovery before opening its read snapshot. A publication
        // racing anywhere after this load rotates the epoch and invalidates the
        // first deleting page; loading the epoch after discovery could instead
        // pair stale roots with the publisher's new epoch.
        let (mut epoch, mut raw_epoch) = self.load_epoch().await?;
        let mut roots = Vec::new();
        let selector_read = self
            .storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?;
        let mut resume_after = None;
        loop {
            let page = selector_read
                .scan(
                    REF_SPACE,
                    KeyRange {
                        lower: Bound::Unbounded,
                        upper: Bound::Unbounded,
                    },
                    ScanOptions {
                        projection: CoreProjection::FullValue,
                        limit_rows: PAGE,
                        resume_after: resume_after.clone(),
                    },
                )
                .await
                .map_err(storage_error)?;
            for entry in &page.entries {
                if entry.key.0.as_ref() == EPOCH_KEY {
                    continue;
                }
                roots.push(decode_ref(projected_bytes(&entry.value)?)?);
            }
            if !page.has_more {
                break;
            }
            resume_after = page.entries.last().map(|entry| entry.key.clone());
        }
        accounting.roots = roots.len() as u64;

        let mut reachable = std::collections::BTreeSet::new();
        let mut frontier = roots;
        while !frontier.is_empty() {
            accounting.peak_frontier = accounting.peak_frontier.max(frontier.len() as u64);
            let mut ids = Vec::with_capacity(PAGE);
            while ids.len() < PAGE {
                let Some(id) = frontier.pop() else {
                    break;
                };
                if reachable.insert(id) {
                    ids.push(id);
                }
            }
            if ids.is_empty() {
                continue;
            }
            let objects = self.load_objects(&ids).await?;
            for bytes in objects {
                let edges = object_edges(&bytes)?;
                reachable.extend(edges.terminal);
                for edge in edges.traverse {
                    if !reachable.contains(&edge) {
                        frontier.push(edge);
                    }
                }
            }
        }
        accounting.reachable_objects = reachable.len() as u64;

        let object_read = self
            .storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?;
        let mut resume_after = None;
        loop {
            let page = object_read
                .scan(
                    OBJECT_SPACE,
                    KeyRange {
                        lower: Bound::Unbounded,
                        upper: Bound::Unbounded,
                    },
                    ScanOptions {
                        projection: CoreProjection::KeyOnly,
                        limit_rows: PAGE,
                        resume_after: resume_after.clone(),
                    },
                )
                .await
                .map_err(storage_error)?;
            accounting.pages += 1;
            accounting.scanned_objects += page.entries.len() as u64;
            let mut deletes = Vec::new();
            let mut orphan_ids = Vec::new();
            for entry in &page.entries {
                let id = object_id_from_key(&entry.key)?;
                if !reachable.contains(&id) {
                    accounting.reclaimed_objects += 1;
                    deletes.push(entry.key.clone());
                    orphan_ids.push(id);
                }
            }
            if !deletes.is_empty() {
                accounting.reclaimed_bytes += self
                    .load_objects(&orphan_ids)
                    .await?
                    .iter()
                    .map(|bytes| bytes.len() as u64)
                    .sum::<u64>();
                let next_epoch = encode_epoch(epoch.saturating_add(1));
                let mut write = self
                    .storage
                    .begin_write(WriteOptions {
                        preconditions: vec![Precondition::KeyValueEquals {
                            space: REF_SPACE,
                            key: key(EPOCH_KEY),
                            expected: raw_epoch,
                        }],
                        batch_capacity_hint_bytes: next_epoch.len(),
                        ..WriteOptions::default()
                    })
                    .await
                    .map_err(storage_error)?;
                write
                    .delete_many(OBJECT_SPACE, &deletes)
                    .await
                    .map_err(storage_error)?;
                write
                    .put_many(
                        REF_SPACE,
                        PutBatch {
                            entries: vec![PutEntry {
                                key: key(EPOCH_KEY),
                                value: StoredValue {
                                    bytes: next_epoch.clone(),
                                },
                            }],
                        },
                    )
                    .await
                    .map_err(storage_error)?;
                write.commit().await.map_err(storage_error)?;
                epoch = epoch.saturating_add(1);
                raw_epoch = next_epoch;
            }
            if !page.has_more {
                break;
            }
            resume_after = page.entries.last().map(|entry| entry.key.clone());
        }
        Ok(accounting)
    }

    /// Deterministic prototype oracle for both epoch orderings. It stages an
    /// unreachable authenticated path-copy objects as the crash-before-root
    /// case, then
    /// proves publication-first rejects a stale deleting sweep and GC-first
    /// rejects a stale root-only publication. The retry uses the public owner
    /// path after rereading the epoch.
    pub async fn verify_publication_gc_races(&self) -> Result<(), String> {
        let main = self.branch_head("main").await?;

        let publication_first_orphan = self
            .stage_test_path_copy_orphan(b"publication-first")
            .await?;
        let (publication_epoch, publication_raw_epoch) = self.load_epoch().await?;
        self.create_branch("race-publication-first", Some(main))
            .await?;
        if self
            .attempt_test_delete(
                publication_first_orphan,
                publication_epoch,
                publication_raw_epoch,
            )
            .await
            .is_ok()
        {
            return Err("ForkTree stale GC committed after root publication".to_string());
        }
        self.delete_branch("race-publication-first").await?;
        let swept = self.reclaim_unreachable().await?;
        if swept.reclaimed_objects == 0 {
            return Err("ForkTree crash orphan was not reclaimed after retry".to_string());
        }

        self.stage_test_path_copy_orphan(b"gc-first").await?;
        let (gc_epoch, gc_raw_epoch) = self.load_epoch().await?;
        let selector = selector_key(BRANCH_PREFIX, "race-gc-first");
        let raw_ref = encode_ref(main);
        let next_epoch = encode_epoch(gc_epoch.saturating_add(1));
        let swept = self.reclaim_unreachable().await?;
        if swept.reclaimed_objects == 0 {
            return Err("ForkTree GC-first oracle did not rotate the epoch".to_string());
        }
        let stale_publication = async {
            let mut write = self
                .storage
                .begin_write(WriteOptions {
                    preconditions: vec![
                        Precondition::KeyAbsent {
                            space: REF_SPACE,
                            key: key(&selector),
                        },
                        Precondition::KeyValueEquals {
                            space: REF_SPACE,
                            key: key(EPOCH_KEY),
                            expected: gc_raw_epoch,
                        },
                    ],
                    batch_capacity_hint_bytes: raw_ref.len() + next_epoch.len(),
                    ..WriteOptions::default()
                })
                .await
                .map_err(storage_error)?;
            write
                .put_many(
                    REF_SPACE,
                    PutBatch {
                        entries: vec![
                            PutEntry {
                                key: key(&selector),
                                value: StoredValue { bytes: raw_ref },
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
            Ok::<(), String>(())
        }
        .await;
        if stale_publication.is_ok() {
            return Err("ForkTree stale publication committed after GC".to_string());
        }
        self.create_branch("race-gc-first", Some(main)).await?;
        self.delete_branch("race-gc-first").await?;
        Ok(())
    }

    /// Corrupts one live chunk through the raw benchmark adapter and proves
    /// that the next cold-cache owner read authenticates the object key before
    /// returning bytes. This is destructive test injection and must run last.
    pub async fn verify_blob_corruption_fail_closed(&self, branch: &str) -> Result<(), String> {
        let commit = self.load_commit(self.branch_head(branch).await?).await?;
        let manifest = self.load_blob_manifest(commit.blob).await?;
        let chunk = manifest
            .chunks
            .first()
            .ok_or_else(|| "ForkTree corruption oracle needs a blob chunk".to_string())?;
        let mut corrupted = self.load_object(chunk.id).await?.to_vec();
        corrupted[0] ^= 0x80;
        let mut pending = BTreeMap::new();
        pending.insert(chunk.id, Bytes::from(corrupted));
        let overwrite = async {
            let mut write = self
                .storage
                .begin_write(WriteOptions::default())
                .await
                .map_err(storage_error)?;
            write
                .put_many(OBJECT_SPACE, object_batch(&pending))
                .await
                .map_err(storage_error)?;
            write.commit().await.map_err(storage_error)?;
            Ok::<(), String>(())
        }
        .await;
        if overwrite.is_ok() && self.read_blob_range(branch, 0, 1).await.is_ok() {
            return Err("ForkTree returned bytes from a corrupted chunk".to_string());
        }

        // A new forged key bypasses the adapter's duplicate-immutable check,
        // so owner-side domain/hash validation must independently reject it.
        let forged_id = ObjectId([0xa5; 32]);
        let mut forged = BTreeMap::new();
        forged.insert(
            forged_id,
            Bytes::from_static(b"ForkTree forged object bytes"),
        );
        let mut write = self
            .storage
            .begin_write(WriteOptions::default())
            .await
            .map_err(storage_error)?;
        write
            .put_many(OBJECT_SPACE, object_batch(&forged))
            .await
            .map_err(storage_error)?;
        write.commit().await.map_err(storage_error)?;
        if self.load_object(forged_id).await.is_ok() {
            return Err("ForkTree accepted bytes under a forged object identity".to_string());
        }
        Ok(())
    }

    /// Corrupts the live relational root through the raw benchmark adapter and
    /// proves authenticated owner reads reject it. Immutable adapters may
    /// reject the overwrite even earlier; both outcomes are fail-closed.
    pub async fn verify_tree_corruption_fail_closed(&self, branch: &str) -> Result<(), String> {
        let commit = self.load_commit(self.branch_head(branch).await?).await?;
        let mut corrupted = self.load_object(commit.root).await?.to_vec();
        corrupted[0] ^= 0x80;
        let mut pending = BTreeMap::new();
        pending.insert(commit.root, Bytes::from(corrupted));
        let overwrite = async {
            let mut write = self
                .storage
                .begin_write(WriteOptions::default())
                .await
                .map_err(storage_error)?;
            write
                .put_many(OBJECT_SPACE, object_batch(&pending))
                .await
                .map_err(storage_error)?;
            write.commit().await.map_err(storage_error)?;
            Ok::<(), String>(())
        }
        .await;
        if overwrite.is_ok() && self.read_relational_all(branch).await.is_ok() {
            return Err("ForkTree returned rows from a corrupted tree node".to_string());
        }
        Ok(())
    }

    async fn stage_test_path_copy_orphan(&self, label: &[u8]) -> Result<ObjectId, String> {
        let mut pending = BTreeMap::new();
        let value = RelationalValue::Bytes(label.to_vec());
        let pack = stage_object(encode_value_pack([&value].into_iter()), &mut pending);
        let orphan_key = [b"race-path-copy/".as_slice(), label].concat();
        let leaf = stage_object(
            encode_leaf(&[LeafEntry {
                key: orphan_key.clone(),
                value: ValueRef { pack, index: 0 },
            }]),
            &mut pending,
        );
        stage_object(
            encode_mutation_delta(
                Some(pack),
                &[Mutation::Insert {
                    key: orphan_key,
                    value,
                }],
            ),
            &mut pending,
        );
        let mut write = self
            .storage
            .begin_write(WriteOptions {
                batch_capacity_hint_bytes: pending.values().map(Bytes::len).sum(),
                ..WriteOptions::default()
            })
            .await
            .map_err(storage_error)?;
        write
            .put_many(OBJECT_SPACE, object_batch(&pending))
            .await
            .map_err(storage_error)?;
        write.commit().await.map_err(storage_error)?;
        Ok(leaf)
    }

    async fn attempt_test_delete(
        &self,
        id: ObjectId,
        epoch: u64,
        raw_epoch: Bytes,
    ) -> Result<(), String> {
        let next_epoch = encode_epoch(epoch.saturating_add(1));
        let mut write = self
            .storage
            .begin_write(WriteOptions {
                preconditions: vec![Precondition::KeyValueEquals {
                    space: REF_SPACE,
                    key: key(EPOCH_KEY),
                    expected: raw_epoch,
                }],
                batch_capacity_hint_bytes: next_epoch.len(),
                ..WriteOptions::default()
            })
            .await
            .map_err(storage_error)?;
        write
            .delete_many(OBJECT_SPACE, &[key(&id.0)])
            .await
            .map_err(storage_error)?;
        write
            .put_many(
                REF_SPACE,
                PutBatch {
                    entries: vec![PutEntry {
                        key: key(EPOCH_KEY),
                        value: StoredValue { bytes: next_epoch },
                    }],
                },
            )
            .await
            .map_err(storage_error)?;
        write.commit().await.map_err(storage_error)?;
        Ok(())
    }

    /// Publishes an authenticated retention boundary with the same state and
    /// blob roots but no historical parents. Existing checkpoint selectors can
    /// continue to pin the old chain until their independent release.
    pub async fn compact_history(&self, branch: &str) -> Result<ObjectId, String> {
        let branch_key = selector_key(BRANCH_PREFIX, branch);
        let head = self.load_head_at_key(&branch_key).await?;
        let current = self.load_commit(head.commit).await?;
        let mut pending = BTreeMap::new();
        let delta = stage_object(
            encode_retention_delta(current.root, current.blob),
            &mut pending,
        );
        let next_commit = stage_object(
            encode_commit(Commit {
                parents: [None, None],
                root: current.root,
                delta,
                blob: current.blob,
            }),
            &mut pending,
        );
        self.remove_existing_objects(&mut pending).await?;
        let next_ref = encode_ref(next_commit);
        let next_epoch = encode_epoch(head.epoch.saturating_add(1));
        let mut write = self
            .storage
            .begin_write(WriteOptions {
                preconditions: vec![
                    Precondition::KeyValueEquals {
                        space: REF_SPACE,
                        key: key(&branch_key),
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
            })
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
                            key: key(&branch_key),
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
        Ok(next_commit)
    }

    pub async fn read_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let head = self.load_head_at_key(MAIN_REF_KEY).await?;
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

    /// Authenticates one commit and its complete state-tree/value-pack closure.
    /// Commit and semantic-delta objects are intentionally excluded so this
    /// compares physical state packing rather than expected history identity.
    pub async fn inspect_state(&self, commit_id: ObjectId) -> Result<StateInspection, String> {
        let commit = self.load_commit(commit_id).await?;
        let mut object_bytes = BTreeMap::new();
        let mut leaf_ranges = Vec::new();
        let mut internal_boundaries = Vec::new();
        let mut frontier = vec![commit.root];
        let mut visited = std::collections::BTreeSet::new();
        while let Some(id) = frontier.pop() {
            if !visited.insert(id) {
                continue;
            }
            let bytes = self.load_object(id).await?;
            object_bytes.insert(id, bytes.len() as u64);
            match decode_node(&bytes)? {
                Node::Leaf(rows) => {
                    let first = rows
                        .first()
                        .ok_or_else(|| "ForkTree inspection found an empty leaf".to_string())?
                        .key
                        .clone();
                    let last = rows
                        .last()
                        .ok_or_else(|| "ForkTree inspection found an empty leaf".to_string())?
                        .key
                        .clone();
                    leaf_ranges.push((first, last));
                    for pack in rows
                        .iter()
                        .map(|row| row.value.pack)
                        .collect::<std::collections::BTreeSet<_>>()
                    {
                        if visited.insert(pack) {
                            let pack_bytes = self.load_object(pack).await?;
                            decode_value_pack(&pack_bytes)?;
                            object_bytes.insert(pack, pack_bytes.len() as u64);
                        }
                    }
                }
                Node::Internal(children) => {
                    internal_boundaries
                        .push(children.iter().map(|child| child.max_key.clone()).collect());
                    frontier.extend(children.into_iter().rev().map(|child| child.id));
                }
            }
        }
        leaf_ranges.sort();
        internal_boundaries.sort();
        Ok(StateInspection {
            root: commit.root,
            object_bytes,
            leaf_ranges,
            internal_boundaries,
        })
    }

    /// Derives physical-layout accounting from the authenticated object and
    /// selector authorities. The fold retains object identities and the mark
    /// frontier, but never retains object bodies: memory is
    /// `O(unique objects + frontier + page)` and each body is bounded by one
    /// canonical block or blob chunk.
    pub async fn object_layout_stats(&self) -> Result<ObjectLayoutStats, String> {
        const PAGE: usize = 1_024;
        let mut stats = ObjectLayoutStats::default();
        let mut object_ids = std::collections::BTreeSet::new();
        let read = self
            .storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?;
        let mut resume_after = None;
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
                        limit_rows: PAGE,
                        resume_after: resume_after.clone(),
                    },
                )
                .await
                .map_err(storage_error)?;
            for entry in &page.entries {
                let id = object_id_from_key(&entry.key)?;
                let bytes = projected_bytes(&entry.value)?;
                authenticate(id, bytes)?;
                object_ids.insert(id);
                stats.objects += 1;
                stats.object_value_bytes += bytes.len() as u64;
                stats.object_logical_key_bytes += entry.key.0.len() as u64;
                stats.object_physical_key_bytes += 4 + entry.key.0.len() as u64;
                if blob_chunk_id(bytes) == id && blake3::hash(bytes).as_bytes() != &id.0 {
                    stats.blob_chunks += 1;
                    stats.blob_chunk_bytes += bytes.len() as u64;
                    continue;
                }
                stats.object_header_bytes += OBJECT_MAGIC.len() as u64 + 1;
                match object_tag(bytes)? {
                    LEAF_TAG => {
                        let mut decoder = Decoder::new(bytes);
                        let _tag = decoder.object_tag()?;
                        let decoded_bytes = decoder.u32()? as u64;
                        let rows = match decode_node(bytes)? {
                            Node::Leaf(rows) => rows,
                            Node::Internal(_) => unreachable!(),
                        };
                        stats.object_header_bytes += 4;
                        stats.leaves += 1;
                        stats.leaf_rows += rows.len() as u64;
                        stats.leaf_encoded_bytes += bytes.len() as u64;
                        stats.leaf_decoded_bytes += decoded_bytes;
                        stats.leaf_key_bytes +=
                            rows.iter().map(|row| row.key.len() as u64).sum::<u64>();
                        stats.leaf_value_ref_bytes += rows.len() as u64 * (32 + 4);
                    }
                    INTERNAL_TAG => {
                        let children = match decode_node(bytes)? {
                            Node::Internal(children) => children,
                            Node::Leaf(_) => unreachable!(),
                        };
                        stats.internals += 1;
                        stats.internal_children += children.len() as u64;
                        stats.internal_encoded_bytes += bytes.len() as u64;
                    }
                    VALUE_PACK_TAG => {
                        let mut decoder = Decoder::new(bytes);
                        let _tag = decoder.object_tag()?;
                        let decoded_bytes = decoder.u32()? as u64;
                        let values = decode_value_pack(bytes)?;
                        stats.object_header_bytes += 4;
                        stats.value_packs += 1;
                        stats.packed_values += values.len() as u64;
                        stats.value_pack_encoded_bytes += bytes.len() as u64;
                        stats.value_pack_decoded_bytes += decoded_bytes;
                        stats.value_payload_bytes += values
                            .iter()
                            .map(|value| match value {
                                RelationalValue::Null => 0,
                                RelationalValue::Bytes(bytes) => bytes.len() as u64,
                            })
                            .sum::<u64>();
                    }
                    DELTA_TAG => {
                        object_edges(bytes)?;
                        stats.deltas += 1;
                        stats.delta_bytes += bytes.len() as u64;
                    }
                    COMMIT_TAG => {
                        decode_commit(bytes)?;
                        stats.commits += 1;
                        stats.commit_bytes += bytes.len() as u64;
                    }
                    BLOB_MANIFEST_TAG => {
                        decode_blob_manifest(bytes)?;
                        stats.blob_manifests += 1;
                        stats.blob_manifest_bytes += bytes.len() as u64;
                    }
                    tag => return Err(format!("unknown ForkTree layout object tag {tag}")),
                }
            }
            if !page.has_more {
                break;
            }
            resume_after = page.entries.last().map(|entry| entry.key.clone());
        }

        let selector_read = self
            .storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?;
        let mut roots = Vec::new();
        let mut resume_after = None;
        loop {
            let page = selector_read
                .scan(
                    REF_SPACE,
                    KeyRange {
                        lower: Bound::Unbounded,
                        upper: Bound::Unbounded,
                    },
                    ScanOptions {
                        projection: CoreProjection::FullValue,
                        limit_rows: PAGE,
                        resume_after: resume_after.clone(),
                    },
                )
                .await
                .map_err(storage_error)?;
            for entry in &page.entries {
                let value = projected_bytes(&entry.value)?;
                stats.selectors += 1;
                stats.selector_logical_key_bytes += entry.key.0.len() as u64;
                stats.selector_physical_key_bytes += 4 + entry.key.0.len() as u64;
                stats.selector_value_bytes += value.len() as u64;
                if entry.key.0.as_ref() != EPOCH_KEY {
                    roots.push(decode_ref(value)?);
                }
            }
            if !page.has_more {
                break;
            }
            resume_after = page.entries.last().map(|entry| entry.key.clone());
        }

        let mut reachable = std::collections::BTreeSet::new();
        let mut frontier = roots;
        while !frontier.is_empty() {
            let mut ids = Vec::with_capacity(PAGE);
            while ids.len() < PAGE {
                let Some(id) = frontier.pop() else {
                    break;
                };
                if reachable.insert(id) {
                    ids.push(id);
                }
            }
            for bytes in self.load_objects(&ids).await? {
                let edges = object_edges(&bytes)?;
                reachable.extend(edges.terminal);
                frontier.extend(
                    edges
                        .traverse
                        .into_iter()
                        .filter(|edge| !reachable.contains(edge)),
                );
            }
        }
        stats.reachable_objects = reachable.len() as u64;
        stats.unreachable_objects = object_ids.difference(&reachable).count() as u64;
        Ok(stats)
    }

    fn rewrite_general<'a>(
        &'a self,
        id: ObjectId,
        mutations: &'a [ResolvedMutation],
        node_cache: &'a BTreeMap<ObjectId, Node>,
        pending: &'a mut BTreeMap<ObjectId, Bytes>,
    ) -> BoxFuture<'a, Result<Vec<NodeRef>, String>> {
        Box::pin(async move {
            let node = load_pending_or_cached_node(id, node_cache, pending)?;
            match node {
                Node::Leaf(mut rows) => {
                    for mutation in mutations {
                        let position = rows.binary_search_by(|row| {
                            row.key.as_slice().cmp(mutation.key.as_slice())
                        });
                        match (mutation.operation, position) {
                            (ResolvedOperation::Insert(value), Err(index)) => {
                                rows.insert(
                                    index,
                                    LeafEntry {
                                        key: mutation.key.clone(),
                                        value,
                                    },
                                );
                            }
                            (ResolvedOperation::Insert(_), Ok(_)) => {
                                return Err(format!(
                                    "ForkTree insert violates identity uniqueness: {}",
                                    String::from_utf8_lossy(&mutation.key)
                                ));
                            }
                            (ResolvedOperation::Update(value), Ok(index)) => {
                                rows[index].value = value;
                            }
                            (ResolvedOperation::Update(_), Err(_)) => {
                                return Err(format!(
                                    "ForkTree update identity is absent: {}",
                                    String::from_utf8_lossy(&mutation.key)
                                ));
                            }
                            (ResolvedOperation::Delete, Ok(index)) => {
                                rows.remove(index);
                            }
                            (ResolvedOperation::Delete, Err(_)) => {
                                return Err(format!(
                                    "ForkTree delete identity is absent: {}",
                                    String::from_utf8_lossy(&mutation.key)
                                ));
                            }
                        }
                    }
                    Ok(stage_leaf_level(&rows, pending))
                }
                Node::Internal(children) => {
                    let child_count = children.len();
                    let mut rewritten = Vec::new();
                    let mut start = 0;
                    for (index, child) in children.into_iter().enumerate() {
                        let length = if index + 1 == child_count {
                            mutations.len() - start
                        } else {
                            mutations[start..].partition_point(|mutation| {
                                mutation.key.as_slice() <= child.max_key.as_slice()
                            })
                        };
                        let end = start + length;
                        if end > start {
                            rewritten.extend(
                                self.rewrite_general(
                                    child.id,
                                    &mutations[start..end],
                                    node_cache,
                                    pending,
                                )
                                .await?,
                            );
                        } else {
                            rewritten.push(child);
                        }
                        start = end;
                    }
                    if start != mutations.len() {
                        return Err("ForkTree mutation routing lost an identity".to_string());
                    }
                    // Copy only this parent's authenticated references. A
                    // bounded split can add a sibling and an empty child can
                    // disappear; unchanged sibling bodies are not loaded or
                    // rewritten. Nonempty underfull blocks remain valid and
                    // avoid turning ordinary CRUD into fanout-wide repacking.
                    Ok(stage_internal_level(&rewritten, pending))
                }
            }
        })
    }

    fn finish_root(
        &self,
        mut roots: Vec<NodeRef>,
        node_cache: &BTreeMap<ObjectId, Node>,
        pending: &mut BTreeMap<ObjectId, Bytes>,
    ) -> Result<NodeRef, String> {
        if roots.is_empty() {
            let id = stage_object(encode_leaf(&[]), pending);
            return Ok(NodeRef {
                id,
                max_key: Vec::new(),
            });
        }
        while roots.len() > 1 {
            roots = stage_internal_level(&roots, pending);
        }
        let mut root = roots.pop().expect("ForkTree root level is nonempty");
        loop {
            match load_pending_or_cached_node(root.id, node_cache, pending)? {
                Node::Internal(children) if children.len() == 1 => {
                    root = children.into_iter().next().expect("one root child");
                }
                _ => return Ok(root),
            }
        }
    }

    async fn load_mutation_working_set(
        &self,
        root: ObjectId,
        mutations: &[ResolvedMutation],
    ) -> Result<BTreeMap<ObjectId, Node>, String> {
        let mut nodes = BTreeMap::new();
        nodes.insert(root, decode_node(&self.load_object(root).await?)?);
        let mut frontier = vec![(root, 0_usize, mutations.len())];
        while !frontier.is_empty() {
            let mut next = Vec::new();
            let mut needed = std::collections::BTreeSet::new();
            for (id, mutation_start, mutation_end) in frontier {
                let Some(Node::Internal(children)) = nodes.get(&id) else {
                    continue;
                };
                let mutations = &mutations[mutation_start..mutation_end];
                let mut local_start = 0_usize;
                for (index, child) in children.iter().enumerate() {
                    let length = if index + 1 == children.len() {
                        mutations.len() - local_start
                    } else {
                        mutations[local_start..].partition_point(|mutation| {
                            mutation.key.as_slice() <= child.max_key.as_slice()
                        })
                    };
                    let local_end = local_start + length;
                    if local_end > local_start {
                        needed.insert(child.id);
                        next.push((
                            child.id,
                            mutation_start + local_start,
                            mutation_start + local_end,
                        ));
                    }
                    local_start = local_end;
                }
                if local_start != mutations.len() {
                    return Err("ForkTree prefetch routing lost an identity".to_string());
                }
            }
            let missing = needed
                .into_iter()
                .filter(|id| !nodes.contains_key(id))
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                let loaded = self.load_objects(&missing).await?;
                for (id, bytes) in missing.into_iter().zip(loaded) {
                    nodes.insert(id, decode_node(&bytes)?);
                }
            }
            frontier = next;
        }
        Ok(nodes)
    }

    async fn node_max_key(&self, id: ObjectId) -> Result<Vec<u8>, String> {
        Ok(node_max_key(&decode_node(&self.load_object(id).await?)?))
    }

    fn find_value<'a>(
        &'a self,
        id: ObjectId,
        key: &'a [u8],
    ) -> BoxFuture<'a, Result<ValueRef, String>> {
        Box::pin(async move {
            match decode_node(&self.load_object(id).await?)? {
                Node::Leaf(rows) => rows
                    .binary_search_by(|row| row.key.as_slice().cmp(key))
                    .map(|index| rows[index].value)
                    .map_err(|_| "ForkTree point key is absent".to_string()),
                Node::Internal(children) => {
                    let child = children
                        .iter()
                        .find(|child| key <= child.max_key.as_slice())
                        .ok_or_else(|| "ForkTree point key exceeds root maximum".to_string())?;
                    self.find_value(child.id, key).await
                }
            }
        })
    }

    fn find_value_optional<'a>(
        &'a self,
        id: ObjectId,
        key: &'a [u8],
    ) -> BoxFuture<'a, Result<Option<ValueRef>, String>> {
        Box::pin(async move {
            match decode_node(&self.load_object(id).await?)? {
                Node::Leaf(rows) => Ok(rows
                    .binary_search_by(|row| row.key.as_slice().cmp(key))
                    .ok()
                    .map(|index| rows[index].value)),
                Node::Internal(children) => match children
                    .iter()
                    .find(|child| key <= child.max_key.as_slice())
                {
                    Some(child) => self.find_value_optional(child.id, key).await,
                    None => Ok(None),
                },
            }
        })
    }

    fn collect_range_entries<'a>(
        &'a self,
        id: ObjectId,
        start: &'a [u8],
        end: &'a [u8],
        output: &'a mut Vec<LeafEntry>,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            match decode_node(&self.load_object(id).await?)? {
                Node::Leaf(rows) => output.extend(
                    rows.into_iter()
                        .filter(|row| row.key.as_slice() >= start && row.key.as_slice() <= end),
                ),
                Node::Internal(children) => {
                    for child in children {
                        if child.max_key.as_slice() < start {
                            continue;
                        }
                        self.collect_range_entries(child.id, start, end, output)
                            .await?;
                        if end <= child.max_key.as_slice() {
                            break;
                        }
                    }
                }
            }
            Ok(())
        })
    }

    fn collect_relational_rows<'a>(
        &'a self,
        id: ObjectId,
        output: &'a mut Vec<(Vec<u8>, RelationalValue)>,
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
                        let value = packs
                            .get(&row.value.pack)
                            .and_then(|values| values.get(row.value.index as usize))
                            .cloned()
                            .ok_or_else(|| {
                                "ForkTree relational value-pack reference is invalid".to_string()
                            })?;
                        output.push((row.key, value));
                    }
                }
                Node::Internal(children) => {
                    for child in children {
                        self.collect_relational_rows(child.id, output).await?;
                    }
                }
            }
            Ok(())
        })
    }

    async fn diff_rows(&self, before: ObjectId, after: ObjectId) -> Result<Vec<RowChange>, String> {
        if before == after {
            return Ok(Vec::new());
        }
        let before = self.load_commit(before).await?;
        let after = self.load_commit(after).await?;
        let mut output = Vec::new();
        self.diff_forests(
            vec![NodeRef {
                id: before.root,
                max_key: self.node_max_key(before.root).await?,
            }],
            vec![NodeRef {
                id: after.root,
                max_key: self.node_max_key(after.root).await?,
            }],
            &mut output,
        )
        .await?;
        output.sort_by(|left, right| left.key.cmp(&right.key));
        if output.windows(2).any(|pair| pair[0].key >= pair[1].key) {
            return Err("ForkTree diff emitted duplicate identities".to_string());
        }
        Ok(output)
    }

    fn diff_forests<'a>(
        &'a self,
        before: Vec<NodeRef>,
        after: Vec<NodeRef>,
        output: &'a mut Vec<RowChange>,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if before.is_empty() && after.is_empty() {
                return Ok(());
            }
            let after_positions = after
                .iter()
                .enumerate()
                .map(|(index, node)| (node.id, index))
                .collect::<BTreeMap<_, _>>();
            let mut before_start = 0;
            let mut after_start = 0;
            let mut found_common = false;
            for (before_index, node) in before.iter().enumerate() {
                let Some(&after_index) = after_positions.get(&node.id) else {
                    continue;
                };
                if before_index < before_start || after_index < after_start {
                    continue;
                }
                found_common = true;
                self.diff_forests(
                    before[before_start..before_index].to_vec(),
                    after[after_start..after_index].to_vec(),
                    output,
                )
                .await?;
                before_start = before_index + 1;
                after_start = after_index + 1;
            }
            if found_common {
                return self
                    .diff_forests(
                        before[before_start..].to_vec(),
                        after[after_start..].to_vec(),
                        output,
                    )
                    .await;
            }

            let mut before_nodes = Vec::with_capacity(before.len());
            for node in &before {
                before_nodes.push(decode_node(&self.load_object(node.id).await?)?);
            }
            let mut after_nodes = Vec::with_capacity(after.len());
            for node in &after {
                after_nodes.push(decode_node(&self.load_object(node.id).await?)?);
            }
            let before_leaves = before_nodes
                .iter()
                .all(|node| matches!(node, Node::Leaf(_)));
            let after_leaves = after_nodes.iter().all(|node| matches!(node, Node::Leaf(_)));
            if before_leaves && after_leaves {
                let mut before_rows = Vec::new();
                for node in before_nodes {
                    let Node::Leaf(mut rows) = node else {
                        unreachable!();
                    };
                    before_rows.append(&mut rows);
                }
                let mut after_rows = Vec::new();
                for node in after_nodes {
                    let Node::Leaf(mut rows) = node else {
                        unreachable!();
                    };
                    after_rows.append(&mut rows);
                }
                let mut before_index = 0;
                let mut after_index = 0;
                while before_index < before_rows.len() || after_index < after_rows.len() {
                    match (before_rows.get(before_index), after_rows.get(after_index)) {
                        (Some(left), Some(right)) if left.key == right.key => {
                            if left.value != right.value {
                                let before_value = self.load_relational_value(left.value).await?;
                                let after_value = self.load_relational_value(right.value).await?;
                                if before_value != after_value {
                                    output.push(RowChange {
                                        key: left.key.clone(),
                                        before: Some(before_value),
                                        after: Some(after_value),
                                    });
                                }
                            }
                            before_index += 1;
                            after_index += 1;
                        }
                        (Some(left), Some(right)) if left.key < right.key => {
                            output.push(RowChange {
                                key: left.key.clone(),
                                before: Some(self.load_relational_value(left.value).await?),
                                after: None,
                            });
                            before_index += 1;
                        }
                        (Some(_), Some(right)) => {
                            output.push(RowChange {
                                key: right.key.clone(),
                                before: None,
                                after: Some(self.load_relational_value(right.value).await?),
                            });
                            after_index += 1;
                        }
                        (Some(left), None) => {
                            output.push(RowChange {
                                key: left.key.clone(),
                                before: Some(self.load_relational_value(left.value).await?),
                                after: None,
                            });
                            before_index += 1;
                        }
                        (None, Some(right)) => {
                            output.push(RowChange {
                                key: right.key.clone(),
                                before: None,
                                after: Some(self.load_relational_value(right.value).await?),
                            });
                            after_index += 1;
                        }
                        (None, None) => break,
                    }
                }
                return Ok(());
            }

            let before = expand_forest(before, before_nodes);
            let after = expand_forest(after, after_nodes);
            self.diff_forests(before, after, output).await
        })
    }

    async fn load_value(&self, value: ValueRef) -> Result<Vec<u8>, String> {
        match decode_value_pack(&self.load_object(value.pack).await?)?
            .get(value.index as usize)
            .cloned()
            .ok_or_else(|| "ForkTree value-pack index is out of bounds".to_string())?
        {
            RelationalValue::Bytes(bytes) => Ok(bytes),
            RelationalValue::Null => {
                Err("ForkTree byte-only reader encountered a relational NULL".to_string())
            }
        }
    }

    async fn load_relational_value(&self, value: ValueRef) -> Result<RelationalValue, String> {
        decode_value_pack(&self.load_object(value.pack).await?)?
            .get(value.index as usize)
            .cloned()
            .ok_or_else(|| "ForkTree value-pack index is out of bounds".to_string())
    }

    async fn load_blob_manifest(&self, id: Option<ObjectId>) -> Result<BlobManifest, String> {
        match id {
            Some(id) => decode_blob_manifest(&self.load_object(id).await?),
            None => Ok(BlobManifest {
                logical_bytes: 0,
                chunks: Vec::new(),
            }),
        }
    }

    async fn read_blob_manifest_range(
        &self,
        manifest: &BlobManifest,
        start: u64,
        end: u64,
    ) -> Result<SegmentedBytes, String> {
        if start > end || end > manifest.logical_bytes {
            return Err("ForkTree blob range is out of bounds".to_string());
        }
        if start == end {
            return Ok(SegmentedBytes::default());
        }
        let mut offset = 0_u64;
        let mut selected = Vec::new();
        for chunk in &manifest.chunks {
            let chunk_start = offset;
            let chunk_end = offset.saturating_add(chunk.bytes);
            offset = chunk_end;
            if chunk_end <= start || chunk_start >= end {
                continue;
            }
            selected.push((*chunk, chunk_start));
        }
        let ids = selected
            .iter()
            .map(|(chunk, _)| chunk.id)
            .collect::<Vec<_>>();
        let objects = self.load_objects(&ids).await?;
        let mut output = VecDeque::with_capacity(selected.len());
        let mut output_bytes = 0_u64;
        for ((chunk, chunk_start), object) in selected.into_iter().zip(objects) {
            validate_blob_chunk(&object, chunk.bytes)?;
            let local_start = start.saturating_sub(chunk_start) as usize;
            let local_end = end
                .min(chunk_start + chunk.bytes)
                .saturating_sub(chunk_start) as usize;
            output_bytes += (local_end - local_start) as u64;
            output.push_back(object.slice(local_start..local_end));
        }
        if output_bytes != end - start {
            return Err("ForkTree blob manifest has a discontinuous layout".to_string());
        }
        Ok(SegmentedBytes::new(output_bytes, output))
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
                        match value {
                            RelationalValue::Bytes(bytes) => output.push((row.key, bytes)),
                            RelationalValue::Null => {
                                return Err(
                                    "ForkTree byte-only row scan encountered a relational NULL"
                                        .to_string(),
                                );
                            }
                        }
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

    async fn put_new_selector(&self, selector: Vec<u8>, commit: ObjectId) -> Result<(), String> {
        self.load_commit(commit).await?;
        let (epoch, raw_epoch) = self.load_epoch().await?;
        let raw_ref = encode_ref(commit);
        let next_epoch = encode_epoch(epoch.saturating_add(1));
        let mut write = self
            .storage
            .begin_write(WriteOptions {
                preconditions: vec![
                    Precondition::KeyAbsent {
                        space: REF_SPACE,
                        key: key(&selector),
                    },
                    Precondition::KeyValueEquals {
                        space: REF_SPACE,
                        key: key(EPOCH_KEY),
                        expected: raw_epoch,
                    },
                ],
                batch_capacity_hint_bytes: raw_ref.len() + next_epoch.len(),
                ..WriteOptions::default()
            })
            .await
            .map_err(storage_error)?;
        write
            .put_many(
                REF_SPACE,
                PutBatch {
                    entries: vec![
                        PutEntry {
                            key: key(&selector),
                            value: StoredValue { bytes: raw_ref },
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
        Ok(())
    }

    async fn delete_selector(&self, selector: Vec<u8>) -> Result<(), String> {
        let raw_ref = self
            .load_raw_selector(&selector)
            .await?
            .ok_or_else(|| "ForkTree selector does not exist".to_string())?;
        let (epoch, raw_epoch) = self.load_epoch().await?;
        let next_epoch = encode_epoch(epoch.saturating_add(1));
        let mut write = self
            .storage
            .begin_write(WriteOptions {
                preconditions: vec![
                    Precondition::KeyValueEquals {
                        space: REF_SPACE,
                        key: key(&selector),
                        expected: raw_ref,
                    },
                    Precondition::KeyValueEquals {
                        space: REF_SPACE,
                        key: key(EPOCH_KEY),
                        expected: raw_epoch,
                    },
                ],
                batch_capacity_hint_bytes: next_epoch.len(),
                ..WriteOptions::default()
            })
            .await
            .map_err(storage_error)?;
        write
            .delete_many(REF_SPACE, &[key(&selector)])
            .await
            .map_err(storage_error)?;
        write
            .put_many(
                REF_SPACE,
                PutBatch {
                    entries: vec![PutEntry {
                        key: key(EPOCH_KEY),
                        value: StoredValue { bytes: next_epoch },
                    }],
                },
            )
            .await
            .map_err(storage_error)?;
        write.commit().await.map_err(storage_error)?;
        Ok(())
    }

    async fn load_raw_selector(&self, selector: &[u8]) -> Result<Option<Bytes>, String> {
        let selector_key = key(selector);
        let read = self
            .storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?;
        let result = read
            .get_many(&[GetManyRequest {
                space: REF_SPACE,
                keys: std::slice::from_ref(&selector_key),
                opts: GetOptions::default(),
            }])
            .await
            .map_err(storage_error)?;
        result
            .values
            .into_iter()
            .next()
            .flatten()
            .map(|value| projected_bytes(&value).cloned())
            .transpose()
    }

    async fn load_epoch(&self) -> Result<(u64, Bytes), String> {
        let raw = self
            .load_raw_selector(EPOCH_KEY)
            .await?
            .ok_or_else(|| "missing ForkTree epoch".to_string())?;
        Ok((decode_epoch(&raw)?, raw))
    }

    async fn load_head_at_key(&self, selector: &[u8]) -> Result<Head, String> {
        let keys = [key(selector), key(EPOCH_KEY)];
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

    async fn existing_object_ids(
        &self,
        ids: &[ObjectId],
    ) -> Result<std::collections::BTreeSet<ObjectId>, String> {
        if ids.is_empty() {
            return Ok(std::collections::BTreeSet::new());
        }
        let keys = ids.iter().map(|id| key(&id.0)).collect::<Vec<_>>();
        let read = self
            .storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?;
        let result = read
            .get_many(&[GetManyRequest {
                space: OBJECT_SPACE,
                keys: &keys,
                opts: GetOptions {
                    projection: CoreProjection::KeyOnly,
                },
            }])
            .await
            .map_err(storage_error)?;
        let mut existing = std::collections::BTreeSet::new();
        for (&id, value) in ids.iter().zip(result.values) {
            let Some(_value) = value else {
                continue;
            };
            existing.insert(id);
        }
        Ok(existing)
    }
}

fn load_pending_or_cached_node(
    id: ObjectId,
    node_cache: &BTreeMap<ObjectId, Node>,
    pending: &BTreeMap<ObjectId, Bytes>,
) -> Result<Node, String> {
    match pending.get(&id) {
        Some(bytes) => decode_node(bytes),
        None => node_cache.get(&id).cloned().ok_or_else(|| {
            format!(
                "ForkTree node {} is absent from its working set",
                hex_id(id)
            )
        }),
    }
}

fn node_max_key(node: &Node) -> Vec<u8> {
    match node {
        Node::Leaf(rows) => rows.last().map_or_else(Vec::new, |row| row.key.clone()),
        Node::Internal(children) => children
            .last()
            .expect("authenticated internal node is nonempty")
            .max_key
            .clone(),
    }
}

fn build_tree(
    rows: &[(Vec<u8>, Vec<u8>)],
    pending: &mut BTreeMap<ObjectId, Bytes>,
) -> Result<NodeRef, String> {
    if rows.is_empty() {
        return Err("ForkTree requires at least one row".to_string());
    }
    let mut level = rows
        .chunks(LEAF_ROWS)
        .map(|chunk| {
            let values = chunk
                .iter()
                .map(|(_, value)| RelationalValue::Bytes(value.clone()))
                .collect::<Vec<_>>();
            let value_pack = stage_object(encode_value_pack(values.iter()), pending);
            let leaf = chunk
                .iter()
                .enumerate()
                .map(|(index, (key, _))| LeafEntry {
                    key: key.clone(),
                    value: ValueRef {
                        pack: value_pack,
                        index: index as u32,
                    },
                })
                .collect::<Vec<_>>();
            let id = stage_object(encode_leaf(&leaf), pending);
            NodeRef {
                id,
                max_key: chunk.last().expect("leaf chunk is nonempty").0.clone(),
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

fn balanced_chunk_sizes(total: usize, maximum: usize) -> Vec<usize> {
    if total == 0 {
        return Vec::new();
    }
    let groups = total.div_ceil(maximum);
    let base = total / groups;
    let remainder = total % groups;
    (0..groups)
        .map(|index| base + usize::from(index < remainder))
        .collect()
}

fn stage_leaf_level(rows: &[LeafEntry], pending: &mut BTreeMap<ObjectId, Bytes>) -> Vec<NodeRef> {
    let mut offset = 0;
    balanced_chunk_sizes(rows.len(), LEAF_ROWS)
        .into_iter()
        .map(|size| {
            let leaf = &rows[offset..offset + size];
            offset += size;
            NodeRef {
                id: stage_object(encode_leaf(leaf), pending),
                max_key: leaf.last().expect("balanced leaf is nonempty").key.clone(),
            }
        })
        .collect()
}

fn stage_internal_level(
    children: &[NodeRef],
    pending: &mut BTreeMap<ObjectId, Bytes>,
) -> Vec<NodeRef> {
    let mut offset = 0;
    balanced_chunk_sizes(children.len(), INTERNAL_CHILDREN)
        .into_iter()
        .map(|size| {
            let internal = &children[offset..offset + size];
            offset += size;
            NodeRef {
                id: stage_object(encode_internal(internal), pending),
                max_key: internal
                    .last()
                    .expect("balanced internal is nonempty")
                    .max_key
                    .clone(),
            }
        })
        .collect()
}

fn expand_forest(refs: Vec<NodeRef>, nodes: Vec<Node>) -> Vec<NodeRef> {
    refs.into_iter()
        .zip(nodes)
        .flat_map(|(node_ref, node)| match node {
            Node::Leaf(_) => vec![node_ref],
            Node::Internal(children) => children,
        })
        .collect()
}

fn validate_sorted_rows(rows: &[(Vec<u8>, Vec<u8>)]) -> Result<(), String> {
    if rows.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err("ForkTree bulk input must be strictly key sorted".to_string());
    }
    Ok(())
}

fn validate_sorted_mutations(mutations: &[Mutation], allow_empty: bool) -> Result<(), String> {
    if mutations.is_empty() && !allow_empty {
        return Err("ForkTree mutation batch must not be empty".to_string());
    }
    if mutations
        .windows(2)
        .any(|pair| pair[0].key() >= pair[1].key())
    {
        return Err("ForkTree mutations must be strictly identity sorted".to_string());
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

fn encode_value_pack<'a>(values: impl ExactSizeIterator<Item = &'a RelationalValue>) -> Bytes {
    let mut body = Vec::new();
    put_u32(&mut body, values.len());
    for value in values {
        match value {
            RelationalValue::Null => body.push(0),
            RelationalValue::Bytes(bytes) => {
                body.push(1);
                put_bytes(&mut body, bytes);
            }
        }
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

fn encode_mutation_delta(value_pack: Option<ObjectId>, mutations: &[Mutation]) -> Bytes {
    let mut bytes = object_prefix(DELTA_TAG);
    bytes.push(4);
    put_optional_id(&mut bytes, value_pack);
    put_u32(&mut bytes, mutations.len());
    for mutation in mutations {
        bytes.push(match mutation {
            Mutation::Insert { .. } => 0,
            Mutation::Update { .. } => 1,
            Mutation::Delete { .. } => 2,
        });
        put_bytes(&mut bytes, mutation.key());
    }
    Bytes::from(bytes)
}

fn encode_blob_delta(before: Option<ObjectId>, after: ObjectId) -> Bytes {
    let mut bytes = object_prefix(DELTA_TAG);
    bytes.push(2);
    put_optional_id(&mut bytes, before);
    bytes.extend_from_slice(&after.0);
    Bytes::from(bytes)
}

fn encode_retention_delta(root: ObjectId, blob: Option<ObjectId>) -> Bytes {
    let mut bytes = object_prefix(DELTA_TAG);
    bytes.push(3);
    bytes.extend_from_slice(&root.0);
    put_optional_id(&mut bytes, blob);
    Bytes::from(bytes)
}

fn blob_chunk_id(payload: &[u8]) -> ObjectId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(OBJECT_MAGIC);
    hasher.update(&[BLOB_CHUNK_TAG]);
    hasher.update(&(payload.len() as u64).to_be_bytes());
    hasher.update(payload);
    ObjectId(*hasher.finalize().as_bytes())
}

fn blob_chunk_id_segments(payload: &ChunkPayload) -> ObjectId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(OBJECT_MAGIC);
    hasher.update(&[BLOB_CHUNK_TAG]);
    hasher.update(&(payload.bytes as u64).to_be_bytes());
    for span in &payload.spans {
        hasher.update(span);
    }
    ObjectId(*hasher.finalize().as_bytes())
}

fn encode_blob_manifest(manifest: &BlobManifest) -> Bytes {
    let mut bytes = object_prefix(BLOB_MANIFEST_TAG);
    put_u64(&mut bytes, manifest.logical_bytes);
    put_u32(&mut bytes, manifest.chunks.len());
    for chunk in &manifest.chunks {
        bytes.extend_from_slice(&chunk.id.0);
        put_u64(&mut bytes, chunk.bytes);
    }
    Bytes::from(bytes)
}

fn encode_commit(commit: Commit) -> Bytes {
    let mut bytes = object_prefix(COMMIT_TAG);
    put_optional_id(&mut bytes, commit.parents[0]);
    put_optional_id(&mut bytes, commit.parents[1]);
    bytes.extend_from_slice(&commit.root.0);
    bytes.extend_from_slice(&commit.delta.0);
    put_optional_id(&mut bytes, commit.blob);
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

fn decode_value_pack(bytes: &[u8]) -> Result<Vec<RelationalValue>, String> {
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
        values.push(match body.take(1)?[0] {
            0 => RelationalValue::Null,
            1 => RelationalValue::Bytes(body.bytes()?),
            tag => return Err(format!("unknown ForkTree relational value tag {tag}")),
        });
    }
    body.finish()?;
    Ok(values)
}

fn validate_blob_chunk(bytes: &[u8], expected_bytes: u64) -> Result<(), String> {
    if bytes.len() as u64 != expected_bytes {
        return Err("ForkTree blob chunk declared size mismatch".to_string());
    }
    Ok(())
}

fn decode_blob_manifest(bytes: &[u8]) -> Result<BlobManifest, String> {
    let mut decoder = Decoder::new(bytes);
    if decoder.object_tag()? != BLOB_MANIFEST_TAG {
        return Err("ForkTree commit does not reference a blob manifest".to_string());
    }
    let logical_bytes = decoder.u64()?;
    let count = decoder.u32()?;
    let mut chunks = Vec::with_capacity(count);
    for _ in 0..count {
        let id = decoder.id()?;
        let bytes = decoder.u64()?;
        if bytes == 0 {
            return Err("ForkTree blob manifest contains an empty chunk".to_string());
        }
        chunks.push(BlobChunkRef { id, bytes });
    }
    decoder.finish()?;
    if chunks.iter().map(|chunk| chunk.bytes).sum::<u64>() != logical_bytes {
        return Err("ForkTree blob manifest logical size mismatch".to_string());
    }
    Ok(BlobManifest {
        logical_bytes,
        chunks,
    })
}

fn decode_commit(bytes: &[u8]) -> Result<Commit, String> {
    let mut decoder = Decoder::new(bytes);
    if decoder.object_tag()? != COMMIT_TAG {
        return Err("ForkTree head does not name a commit object".to_string());
    }
    let commit = Commit {
        parents: [decoder.optional_id()?, decoder.optional_id()?],
        root: decoder.id()?,
        delta: decoder.id()?,
        blob: decoder.optional_id()?,
    };
    decoder.finish()?;
    Ok(commit)
}

struct ObjectEdges {
    traverse: Vec<ObjectId>,
    terminal: Vec<ObjectId>,
}

impl ObjectEdges {
    fn traverse(ids: impl IntoIterator<Item = ObjectId>) -> Self {
        Self {
            traverse: ids.into_iter().collect(),
            terminal: Vec::new(),
        }
    }

    fn terminal(ids: impl IntoIterator<Item = ObjectId>) -> Self {
        Self {
            traverse: Vec::new(),
            terminal: ids.into_iter().collect(),
        }
    }

    fn empty() -> Self {
        Self {
            traverse: Vec::new(),
            terminal: Vec::new(),
        }
    }
}

fn object_edges(bytes: &[u8]) -> Result<ObjectEdges, String> {
    match object_tag(bytes)? {
        LEAF_TAG => match decode_node(bytes)? {
            Node::Leaf(rows) => Ok(ObjectEdges::terminal(
                rows.into_iter().map(|row| row.value.pack),
            )),
            Node::Internal(_) => unreachable!(),
        },
        INTERNAL_TAG => match decode_node(bytes)? {
            Node::Internal(children) => Ok(ObjectEdges::traverse(
                children.into_iter().map(|child| child.id),
            )),
            Node::Leaf(_) => unreachable!(),
        },
        VALUE_PACK_TAG => {
            decode_value_pack(bytes)?;
            Ok(ObjectEdges::empty())
        }
        DELTA_TAG => {
            let mut decoder = Decoder::new(bytes);
            let _ = decoder.object_tag()?;
            match decoder.take(1)?[0] {
                0 => {
                    let root = decoder.id()?;
                    let _rows = decoder.u32()?;
                    decoder.finish()?;
                    Ok(ObjectEdges::traverse([root]))
                }
                2 => {
                    let before = decoder.optional_id()?;
                    let after = decoder.id()?;
                    decoder.finish()?;
                    Ok(ObjectEdges::traverse(
                        before.into_iter().chain(std::iter::once(after)),
                    ))
                }
                3 => {
                    let root = decoder.id()?;
                    let blob = decoder.optional_id()?;
                    decoder.finish()?;
                    Ok(ObjectEdges::traverse(std::iter::once(root).chain(blob)))
                }
                4 => {
                    let pack = decoder.optional_id()?;
                    let count = decoder.u32()?;
                    for _ in 0..count {
                        match decoder.take(1)?[0] {
                            0..=2 => {}
                            operation => {
                                return Err(format!(
                                    "unknown ForkTree mutation operation {operation}"
                                ));
                            }
                        }
                        let _key = decoder.bytes()?;
                    }
                    decoder.finish()?;
                    Ok(ObjectEdges::terminal(pack))
                }
                mode => Err(format!("unknown ForkTree delta mode {mode}")),
            }
        }
        COMMIT_TAG => {
            let commit = decode_commit(bytes)?;
            Ok(ObjectEdges::traverse(
                commit
                    .parents
                    .into_iter()
                    .flatten()
                    .chain([commit.root, commit.delta])
                    .chain(commit.blob),
            ))
        }
        BLOB_MANIFEST_TAG => Ok(ObjectEdges::terminal(
            decode_blob_manifest(bytes)?
                .chunks
                .into_iter()
                .map(|chunk| chunk.id),
        )),
        tag => Err(format!("unknown ForkTree object tag {tag}")),
    }
}

fn object_id_from_key(key: &Key) -> Result<ObjectId, String> {
    let id: [u8; 32] = key
        .0
        .as_ref()
        .try_into()
        .map_err(|_| "ForkTree object key is not a BLAKE3 digest".to_string())?;
    Ok(ObjectId(id))
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

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) {
    put_u32(output, value.len());
    output.extend_from_slice(value);
}

fn key(value: &[u8]) -> Key {
    Key(Bytes::copy_from_slice(value))
}

fn selector_key(prefix: &[u8], name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + name.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(name.as_bytes());
    key
}

fn authenticate(id: ObjectId, bytes: &[u8]) -> Result<(), String> {
    if blake3::hash(bytes).as_bytes() == &id.0 || blob_chunk_id(bytes) == id {
        return Ok(());
    }
    Err(format!(
        "ForkTree object {} failed authentication",
        hex_id(id)
    ))
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

    fn u64(&mut self) -> Result<u64, String> {
        let encoded: [u8; 8] = self
            .take(8)?
            .try_into()
            .expect("decoder returns exact u64 width");
        Ok(u64::from_be_bytes(encoded))
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
