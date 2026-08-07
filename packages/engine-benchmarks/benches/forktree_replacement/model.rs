use std::collections::BTreeMap;
use std::io::Read;
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
const LEAF_ROWS: usize = 8;
const INTERNAL_CHILDREN: usize = 8;
const BLOB_MIN_BYTES: usize = 512 * 1024;
const BLOB_AVG_BYTES: usize = 512 * 1024;
const BLOB_MAX_BYTES: usize = 2 * 1024 * 1024;
const BLOB_STREAM_WINDOW_BYTES: usize = 8 * 1024 * 1024;

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
struct ResolvedUpdate {
    key: Vec<u8>,
    value: ValueRef,
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

#[derive(Clone, Copy)]
struct WindowChunk {
    id: ObjectId,
    start: usize,
    bytes: usize,
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
        self.apply_sorted_updates_with_merge_parent(branch, updates, None)
            .await
    }

    async fn apply_sorted_updates_with_merge_parent(
        &self,
        branch: &str,
        updates: &[Update],
        merge_parent: Option<ObjectId>,
    ) -> Result<(ObjectId, ApplyAccounting), String> {
        validate_sorted_updates(updates)?;
        let branch_key = selector_key(BRANCH_PREFIX, branch);
        let head = self.load_head_at_key(&branch_key).await?;
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
        if before == after {
            return Ok(Vec::new());
        }
        let before = self.load_commit(before).await?;
        let after = self.load_commit(after).await?;
        let mut changed = Vec::new();
        self.diff_nodes(before.root, after.root, &mut changed)
            .await?;
        Ok(changed)
    }

    pub async fn merge_branches(
        &self,
        target: &str,
        source: &str,
        base: ObjectId,
    ) -> Result<(ObjectId, ApplyAccounting), String> {
        let target_head = self.branch_head(target).await?;
        let source_head = self.branch_head(source).await?;
        let source_changes = self.diff_commits(base, source_head).await?;
        let target_changes = self.diff_commits(base, target_head).await?;
        let target_keys = target_changes
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        if source_changes.iter().any(|key| target_keys.contains(key)) {
            return Err("ForkTree prototype merge has a semantic conflict".to_string());
        }
        let mut updates = Vec::with_capacity(source_changes.len());
        for key in source_changes {
            let source_commit = self.load_commit(source_head).await?;
            let value = self.find_value(source_commit.root, &key).await?;
            updates.push(Update {
                key,
                value: self.load_value(value).await?,
            });
        }
        self.apply_sorted_updates_with_merge_parent(target, &updates, Some(source_head))
            .await
    }

    /// Streams one blob through the single canonical CDC profile, emits
    /// authenticated chunks in bounded immutable batches, then atomically
    /// publishes only metadata, the branch selector, and the epoch. A crash
    /// before publication can leave unreachable immutable objects, but cannot
    /// expose a partial blob; reclamation owns those objects later.
    pub async fn ingest_blob<R>(
        &self,
        branch: &str,
        mut source: R,
    ) -> Result<(ObjectId, BlobAccounting), String>
    where
        R: Read,
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
        let mut buffer = BytesMut::zeroed(BLOB_STREAM_WINDOW_BYTES);
        accounting.peak_buffer_bytes = buffer.len() as u64;
        let mut buffered = 0_usize;
        let mut eof = false;

        while buffered != 0 || !eof {
            let source_started = std::time::Instant::now();
            while buffered < buffer.len() && !eof {
                let read = source
                    .read(&mut buffer[buffered..])
                    .map_err(|error| format!("read ForkTree blob stream: {error}"))?;
                if read == 0 {
                    eof = true;
                } else {
                    buffered += read;
                }
            }
            accounting.source_read_us += source_started.elapsed().as_micros() as u64;
            if buffered == 0 {
                break;
            }

            let mut consumed = 0_usize;
            let mut window_chunks = Vec::new();
            while consumed < buffered {
                let remaining = buffered - consumed;
                if !eof && remaining < BLOB_MAX_BYTES {
                    break;
                }

                // A matching authenticated prior chunk is already a proven
                // canonical boundary: FastCDC is deterministic from the last
                // boundary and the bytes are identical. On a mismatch, run
                // the one canonical chunker and resynchronize by object ID.
                let previous = previous_manifest.chunks.get(previous_chunk_cursor);
                let mut predicted = None;
                if let Some(previous) = previous {
                    let previous_bytes = usize::try_from(previous.bytes)
                        .map_err(|_| "ForkTree prior blob chunk length exceeds usize")?;
                    if previous_bytes <= remaining {
                        let hash_started = std::time::Instant::now();
                        let id = blob_chunk_id(&buffer[consumed..consumed + previous_bytes]);
                        accounting.object_hash_us += hash_started.elapsed().as_micros() as u64;
                        predicted = Some((previous_bytes, id));
                        if id == previous.id {
                            accounting.locality_hits += 1;
                        } else {
                            accounting.locality_misses += 1;
                        }
                    }
                }

                let (chunk_bytes, id, known_existing) = match (previous, predicted) {
                    (Some(previous), Some((bytes, id))) if id == previous.id => {
                        previous_chunk_cursor += 1;
                        (bytes, id, true)
                    }
                    _ => {
                        let chunking_started = std::time::Instant::now();
                        let (_, chunk_bytes) = fastcdc::v2020::cut(
                            &buffer[consumed..buffered],
                            BLOB_MIN_BYTES,
                            BLOB_AVG_BYTES,
                            BLOB_MAX_BYTES,
                            fastcdc::v2020::MASKS[20],
                            fastcdc::v2020::MASKS[18],
                            fastcdc::v2020::MASKS[20] << 1,
                            fastcdc::v2020::MASKS[18] << 1,
                        );
                        accounting.chunking_us += chunking_started.elapsed().as_micros() as u64;
                        if chunk_bytes == 0 || chunk_bytes > remaining {
                            return Err("ForkTree CDC produced an invalid chunk size".to_string());
                        }
                        let id = if let Some((predicted_bytes, id)) = predicted {
                            if predicted_bytes == chunk_bytes {
                                id
                            } else {
                                let hash_started = std::time::Instant::now();
                                let id = blob_chunk_id(&buffer[consumed..consumed + chunk_bytes]);
                                accounting.object_hash_us +=
                                    hash_started.elapsed().as_micros() as u64;
                                id
                            }
                        } else {
                            let hash_started = std::time::Instant::now();
                            let id = blob_chunk_id(&buffer[consumed..consumed + chunk_bytes]);
                            accounting.object_hash_us += hash_started.elapsed().as_micros() as u64;
                            id
                        };
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
                        (chunk_bytes, id, known_existing)
                    }
                };
                if chunk_bytes == 0 || chunk_bytes > remaining {
                    return Err("ForkTree CDC produced an invalid chunk size".to_string());
                }
                chunks.push(BlobChunkRef {
                    id,
                    bytes: chunk_bytes as u64,
                });
                accounting.logical_bytes += chunk_bytes as u64;
                if !unique_chunks.insert(id) || known_existing {
                    accounting.reused_chunks += 1;
                } else {
                    window_chunks.push(WindowChunk {
                        id,
                        start: consumed,
                        bytes: chunk_bytes,
                    });
                }
                consumed += chunk_bytes;
            }
            if consumed == 0 {
                return Err("ForkTree CDC streaming window made no progress".to_string());
            }

            let ids = window_chunks
                .iter()
                .map(|chunk| chunk.id)
                .collect::<Vec<_>>();
            let dedup_started = std::time::Instant::now();
            let existing = self.existing_object_ids(&ids).await?;
            accounting.dedup_read_us += dedup_started.elapsed().as_micros() as u64;
            let window = buffer.freeze();
            let mut pending_chunks = BTreeMap::new();
            for chunk in window_chunks {
                if existing.contains(&chunk.id) {
                    accounting.reused_chunks += 1;
                    continue;
                }
                pending_chunks.insert(
                    chunk.id,
                    window.slice(chunk.start..chunk.start + chunk.bytes),
                );
            }
            accounting.peak_buffer_bytes = accounting.peak_buffer_bytes.max(window.len() as u64);
            let emitted = self.emit_chunk_batch(&mut pending_chunks).await?;
            accounting.object_writes += emitted.object_writes;
            accounting.object_bytes += emitted.object_bytes;
            accounting.emission_us += emitted.emission_us;
            accounting.emission_batches += emitted.emission_batches;

            buffer = window.try_into_mut().map_err(|_| {
                "ForkTree chunk emission retained the completed streaming window".to_string()
            })?;
            buffer.copy_within(consumed..buffered, 0);
            buffered -= consumed;
        }

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
        pending.clear();
        Ok(accounting)
    }

    pub async fn read_blob(&self, branch: &str) -> Result<Vec<u8>, String> {
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
    ) -> Result<Vec<u8>, String> {
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

    fn diff_nodes<'a>(
        &'a self,
        before: ObjectId,
        after: ObjectId,
        output: &'a mut Vec<Vec<u8>>,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if before == after {
                return Ok(());
            }
            let objects = self.load_objects(&[before, after]).await?;
            let before = decode_node(&objects[0])?;
            let after = decode_node(&objects[1])?;
            match (before, after) {
                (Node::Leaf(before), Node::Leaf(after)) => {
                    if before.len() != after.len()
                        || before
                            .iter()
                            .zip(&after)
                            .any(|(left, right)| left.key != right.key)
                    {
                        return Err(
                            "ForkTree focused diff encountered a key-set layout change".to_string()
                        );
                    }
                    output.extend(before.into_iter().zip(after).filter_map(|(left, right)| {
                        (left.value != right.value).then_some(left.key)
                    }));
                }
                (Node::Internal(before), Node::Internal(after)) => {
                    if before.len() != after.len()
                        || before
                            .iter()
                            .zip(&after)
                            .any(|(left, right)| left.max_key != right.max_key)
                    {
                        return Err(
                            "ForkTree focused diff encountered an internal layout change"
                                .to_string(),
                        );
                    }
                    for (left, right) in before.into_iter().zip(after) {
                        if left.id != right.id {
                            self.diff_nodes(left.id, right.id, output).await?;
                        }
                    }
                }
                _ => return Err("ForkTree diff encountered mismatched node kinds".to_string()),
            }
            Ok(())
        })
    }

    async fn load_value(&self, value: ValueRef) -> Result<Vec<u8>, String> {
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
    ) -> Result<Vec<u8>, String> {
        if start > end || end > manifest.logical_bytes {
            return Err("ForkTree blob range is out of bounds".to_string());
        }
        if start == end {
            return Ok(Vec::new());
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
        let mut output = Vec::with_capacity((end - start) as usize);
        for ((chunk, chunk_start), object) in selected.into_iter().zip(objects) {
            let payload = decode_blob_chunk(&object, chunk.bytes)?;
            let local_start = start.saturating_sub(chunk_start) as usize;
            let local_end = end
                .min(chunk_start + chunk.bytes)
                .saturating_sub(chunk_start) as usize;
            output.extend_from_slice(&payload[local_start..local_end]);
        }
        if output.len() as u64 != end - start {
            return Err("ForkTree blob manifest has a discontinuous layout".to_string());
        }
        Ok(output)
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
            let value_pack = stage_object(
                encode_value_pack(chunk.iter().map(|(_, value)| value.as_slice())),
                pending,
            );
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

fn decode_blob_chunk(bytes: &[u8], expected_bytes: u64) -> Result<Vec<u8>, String> {
    if bytes.len() as u64 != expected_bytes {
        return Err("ForkTree blob chunk declared size mismatch".to_string());
    }
    Ok(bytes.to_vec())
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
                1 => {
                    let pack = decoder.id()?;
                    let count = decoder.u32()?;
                    for _ in 0..count {
                        let _ = decoder.bytes()?;
                    }
                    decoder.finish()?;
                    Ok(ObjectEdges::terminal([pack]))
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
