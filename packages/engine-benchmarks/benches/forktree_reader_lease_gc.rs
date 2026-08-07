//! Test-only persisted reader-lease / GC safe-point model for ForkTree.
//! No production owner, adapter, or Stage-2 serving path is wired here.

use std::alloc::GlobalAlloc;
use std::collections::VecDeque;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use lix::storage::{
    CoreProjection, GetManyRequest, GetOptions, Key, KeyRange, Precondition, ProjectedValue,
    PutBatch, PutEntry, ReadOptions, ScanOptions, Storage, StorageError, StorageRead, StorageWrite,
    StoredValue, ValueSemantics, WriteOptions,
};
use lix::storage_bench::synthetic_space_for_bench;
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;

static PROFILE_ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static PROFILE_ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static PROFILE_ALLOCATION_ENABLED: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && PROFILE_ALLOCATION_ENABLED.load(Ordering::Relaxed) {
            PROFILE_ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            PROFILE_ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }

    unsafe fn realloc(
        &self,
        pointer: *mut u8,
        layout: std::alloc::Layout,
        new_size: usize,
    ) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, new_size) };
        if !replacement.is_null()
            && new_size >= layout.size()
            && PROFILE_ALLOCATION_ENABLED.load(Ordering::Relaxed)
        {
            PROFILE_ALLOCATED_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            PROFILE_ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        replacement
    }
}

const META_SPACE: lix::storage::StorageSpace =
    synthetic_space_for_bench(72, ValueSemantics::Mutable);
const OBJECT_SPACE: lix::storage::StorageSpace =
    synthetic_space_for_bench(73, ValueSemantics::Immutable);
const MARK_SPACE: lix::storage::StorageSpace =
    synthetic_space_for_bench(74, ValueSemantics::Mutable);
const GLOBAL_KEY: &[u8] = b"global";
const GC_PROGRESS_KEY: &[u8] = b"gc-progress";
const LEASE_PREFIX: &[u8] = b"lease/";
const ROOT_PREFIX: &[u8] = b"root/";
const MARK_PREFIX: &[u8] = b"mark/";
const PAGE_ROWS: usize = 32;
const GRAPH_DEPTH: usize = 4;
const BENCH_LEASE_SPAN: u64 = 1_000_000;

type Id = [u8; 32];

#[derive(Clone, Debug, Default)]
struct Metrics {
    get_calls: u64,
    get_keys: u64,
    get_bytes: u64,
    scan_calls: u64,
    scan_rows: u64,
    scan_bytes: u64,
    commits: u64,
    puts: u64,
    deletes: u64,
    write_bytes: u64,
    marked: u64,
    swept: u64,
    peak_queue: u64,
}

impl std::ops::AddAssign for Metrics {
    fn add_assign(&mut self, rhs: Self) {
        self.get_calls += rhs.get_calls;
        self.get_keys += rhs.get_keys;
        self.get_bytes += rhs.get_bytes;
        self.scan_calls += rhs.scan_calls;
        self.scan_rows += rhs.scan_rows;
        self.scan_bytes += rhs.scan_bytes;
        self.commits += rhs.commits;
        self.puts += rhs.puts;
        self.deletes += rhs.deletes;
        self.write_bytes += rhs.write_bytes;
        self.marked += rhs.marked;
        self.swept += rhs.swept;
        self.peak_queue = self.peak_queue.max(rhs.peak_queue);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Global {
    epoch: u64,
    gc_watermark: u64,
    next_lease_generation: u64,
}

impl Global {
    fn encode(&self) -> Bytes {
        let mut body = Vec::with_capacity(32);
        body.extend_from_slice(b"GLB1");
        body.extend_from_slice(&self.epoch.to_be_bytes());
        body.extend_from_slice(&self.gc_watermark.to_be_bytes());
        body.extend_from_slice(&self.next_lease_generation.to_be_bytes());
        append_checksum(body)
    }

    fn decode(bytes: &Bytes) -> Result<Self, StorageError> {
        let body = authenticated_body(bytes, b"GLB1", 28)?;
        Ok(Self {
            epoch: read_u64(body, 4)?,
            gc_watermark: read_u64(body, 12)?,
            next_lease_generation: read_u64(body, 20)?,
        })
    }

    fn rotated(&self) -> Self {
        Self {
            epoch: self.epoch.checked_add(1).expect("global epoch overflow"),
            gc_watermark: self.gc_watermark,
            next_lease_generation: self.next_lease_generation,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Lease {
    lease_id: u64,
    generation: u64,
    root: Id,
    view_id: Id,
    valid_through_epoch: u64,
}

impl Lease {
    fn encode(&self) -> Bytes {
        let mut body = Vec::with_capacity(96);
        body.extend_from_slice(b"RLS1");
        body.extend_from_slice(&self.lease_id.to_be_bytes());
        body.extend_from_slice(&self.generation.to_be_bytes());
        body.extend_from_slice(&self.root);
        body.extend_from_slice(&self.view_id);
        body.extend_from_slice(&self.valid_through_epoch.to_be_bytes());
        append_checksum(body)
    }

    fn decode(key: &Key, bytes: &Bytes, global: &Global) -> Result<Self, StorageError> {
        let body = authenticated_body(bytes, b"RLS1", 92)?;
        let lease = Self {
            lease_id: read_u64(body, 4)?,
            generation: read_u64(body, 12)?,
            root: read_id(body, 20)?,
            view_id: read_id(body, 52)?,
            valid_through_epoch: read_u64(body, 84)?,
        };
        if key != &lease_key(lease.lease_id)
            || lease.generation == 0
            || lease.generation >= global.next_lease_generation
            || lease.root == [0; 32]
            || lease.view_id == [0; 32]
        {
            return Err(corruption(
                "reader lease identity/generation is noncanonical",
            ));
        }
        Ok(lease)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GcProgress {
    cycle: u64,
    fenced_global: Bytes,
    minimum_live_generation: u64,
    live_lease_count: u64,
    live_lease_digest: Id,
}

impl GcProgress {
    fn encode(&self) -> Bytes {
        let mut body = Vec::with_capacity(128);
        body.extend_from_slice(b"GCP1");
        body.extend_from_slice(&self.cycle.to_be_bytes());
        body.extend_from_slice(&(self.fenced_global.len() as u32).to_be_bytes());
        body.extend_from_slice(&self.fenced_global);
        body.extend_from_slice(&self.minimum_live_generation.to_be_bytes());
        body.extend_from_slice(&self.live_lease_count.to_be_bytes());
        body.extend_from_slice(&self.live_lease_digest);
        append_checksum(body)
    }

    fn decode(bytes: &Bytes) -> Result<Self, StorageError> {
        if bytes.len() < 4 + 8 + 4 + 8 + 8 + 32 + 32 {
            return Err(corruption("GC progress is truncated"));
        }
        verify_checksum(bytes)?;
        if &bytes[..4] != b"GCP1" {
            return Err(corruption("GC progress magic/version mismatch"));
        }
        let body_len = bytes.len() - 32;
        let global_len = u32::from_be_bytes(
            bytes[12..16]
                .try_into()
                .map_err(|_| corruption("GC global length is malformed"))?,
        ) as usize;
        let global_end = 16usize
            .checked_add(global_len)
            .ok_or_else(|| corruption("GC global length overflow"))?;
        if global_end + 48 != body_len {
            return Err(corruption("GC progress has inconsistent lengths"));
        }
        Ok(Self {
            cycle: read_u64(bytes, 4)?,
            fenced_global: Bytes::copy_from_slice(&bytes[16..global_end]),
            minimum_live_generation: read_u64(bytes, global_end)?,
            live_lease_count: read_u64(bytes, global_end + 8)?,
            live_lease_digest: read_id(bytes, global_end + 16)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Cursor {
    lease_id: u64,
    lease_generation: u64,
    root: Id,
    view_id: Id,
    resume_after: u64,
}

impl Cursor {
    fn encode(&self) -> Bytes {
        let mut body = Vec::with_capacity(96);
        body.extend_from_slice(b"CUR1");
        body.extend_from_slice(&self.lease_id.to_be_bytes());
        body.extend_from_slice(&self.lease_generation.to_be_bytes());
        body.extend_from_slice(&self.root);
        body.extend_from_slice(&self.view_id);
        body.extend_from_slice(&self.resume_after.to_be_bytes());
        append_checksum(body)
    }

    fn decode(bytes: &Bytes) -> Result<Self, StorageError> {
        let body = authenticated_body(bytes, b"CUR1", 92)?;
        Ok(Self {
            lease_id: read_u64(body, 4)?,
            lease_generation: read_u64(body, 12)?,
            root: read_id(body, 20)?,
            view_id: read_id(body, 52)?,
            resume_after: read_u64(body, 84)?,
        })
    }
}

#[derive(Clone, Debug)]
struct Fixture {
    reader_roots: Vec<Id>,
    current_roots: Vec<Id>,
    static_roots: Vec<Id>,
    orphan_roots: Vec<Id>,
}

#[derive(Clone, Debug)]
struct PreparedLeaseMutation {
    raw_global: Bytes,
    global: Global,
    key: Key,
    raw_lease: Option<Bytes>,
    lease: Lease,
}

#[derive(Clone, Debug)]
struct PreparedGc {
    raw_global: Bytes,
    next_global: Global,
    progress: GcProgress,
    roots: Vec<Id>,
    expired_leases: Vec<(Key, Bytes)>,
}

#[derive(Clone, Debug)]
struct ReopenFixture {
    old_cursor: Cursor,
    renewed_cursor: Cursor,
    renewed_lease: Lease,
    old_root: Id,
    current_root: Id,
}

fn corruption(message: impl Into<String>) -> StorageError {
    StorageError::Corruption(message.into())
}

fn append_checksum(mut body: Vec<u8>) -> Bytes {
    let digest = blake3::hash(&body);
    body.extend_from_slice(digest.as_bytes());
    Bytes::from(body)
}

fn verify_checksum(bytes: &Bytes) -> Result<(), StorageError> {
    if bytes.len() < 32 {
        return Err(corruption("authenticated value is truncated"));
    }
    let split = bytes.len() - 32;
    if blake3::hash(&bytes[..split]).as_bytes() != &bytes[split..] {
        return Err(corruption("authenticated value checksum mismatch"));
    }
    Ok(())
}

fn authenticated_body<'a>(
    bytes: &'a Bytes,
    magic: &[u8; 4],
    expected_body_len: usize,
) -> Result<&'a [u8], StorageError> {
    if bytes.len() != expected_body_len + 32 {
        return Err(corruption("authenticated value has invalid length"));
    }
    verify_checksum(bytes)?;
    if &bytes[..4] != magic {
        return Err(corruption("authenticated value magic/version mismatch"));
    }
    Ok(&bytes[..expected_body_len])
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, StorageError> {
    bytes
        .get(offset..offset + 8)
        .ok_or_else(|| corruption("u64 field is truncated"))?
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| corruption("u64 field is malformed"))
}

fn read_id(bytes: &[u8], offset: usize) -> Result<Id, StorageError> {
    bytes
        .get(offset..offset + 32)
        .ok_or_else(|| corruption("object id is truncated"))?
        .try_into()
        .map_err(|_| corruption("object id is malformed"))
}

fn key(bytes: impl Into<Bytes>) -> Key {
    Key(bytes.into())
}

fn prefixed_key(prefix: &[u8], id: u64) -> Key {
    let mut bytes = Vec::with_capacity(prefix.len() + 8);
    bytes.extend_from_slice(prefix);
    bytes.extend_from_slice(&id.to_be_bytes());
    key(Bytes::from(bytes))
}

fn lease_key(id: u64) -> Key {
    prefixed_key(LEASE_PREFIX, id)
}

fn branch_key(id: u64) -> Key {
    prefixed_key(b"root/branch/", id)
}

fn static_root_key(label: u8) -> Key {
    let mut bytes = b"root/static/".to_vec();
    bytes.push(label);
    key(Bytes::from(bytes))
}

fn mark_key(cycle: u64, id: Id) -> Key {
    let mut bytes = Vec::with_capacity(MARK_PREFIX.len() + 8 + 32);
    bytes.extend_from_slice(MARK_PREFIX);
    bytes.extend_from_slice(&cycle.to_be_bytes());
    bytes.extend_from_slice(&id);
    key(Bytes::from(bytes))
}

fn id_key(id: Id) -> Key {
    key(Bytes::copy_from_slice(&id))
}

fn root_value(id: Id) -> Bytes {
    Bytes::copy_from_slice(&id)
}

fn decode_root(bytes: &Bytes) -> Result<Id, StorageError> {
    if bytes.len() != 32 {
        return Err(corruption("root selector has invalid object id length"));
    }
    read_id(bytes, 0)
}

fn object_bytes(child: Option<Id>, seed: u64, depth: usize) -> (Id, Bytes) {
    let mut body = Vec::with_capacity(128);
    body.extend_from_slice(b"OBJ1");
    body.push(u8::from(child.is_some()));
    if let Some(child) = child {
        body.extend_from_slice(&child);
    }
    body.extend_from_slice(&seed.to_be_bytes());
    body.extend_from_slice(&(depth as u64).to_be_bytes());
    body.extend(std::iter::repeat_n((seed as u8) ^ depth as u8, 64));
    let bytes = append_checksum(body);
    let id = *blake3::hash(&bytes).as_bytes();
    (id, bytes)
}

fn decode_object(id: Id, bytes: &Bytes) -> Result<Option<Id>, StorageError> {
    if blake3::hash(bytes).as_bytes() != &id {
        return Err(corruption("object content hash does not match key"));
    }
    verify_checksum(bytes)?;
    if bytes.len() < 4 + 1 + 8 + 8 + 64 + 32 || &bytes[..4] != b"OBJ1" {
        return Err(corruption("object encoding is malformed"));
    }
    match bytes[4] {
        0 => Ok(None),
        1 => Ok(Some(read_id(bytes, 5)?)),
        _ => Err(corruption("object child cardinality is malformed")),
    }
}

fn view_id(branch: u64, root: Id) -> Id {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"reader-view-v1");
    hasher.update(&branch.to_be_bytes());
    hasher.update(&root);
    *hasher.finalize().as_bytes()
}

fn prefix_range(prefix: &[u8]) -> KeyRange {
    let lower = key(Bytes::copy_from_slice(prefix));
    let mut upper = prefix.to_vec();
    for index in (0..upper.len()).rev() {
        if upper[index] != u8::MAX {
            upper[index] += 1;
            upper.truncate(index + 1);
            return KeyRange {
                lower: Bound::Included(lower),
                upper: Bound::Excluded(key(Bytes::from(upper))),
            };
        }
    }
    KeyRange {
        lower: Bound::Included(lower),
        upper: Bound::Unbounded,
    }
}

fn full_value(value: Option<ProjectedValue>, label: &str) -> Result<Option<Bytes>, StorageError> {
    match value {
        None => Ok(None),
        Some(ProjectedValue::FullValue(bytes)) => Ok(Some(bytes)),
        Some(ProjectedValue::KeyOnly) => {
            Err(corruption(format!("{label} returned key-only projection")))
        }
    }
}

async fn get_values<S: Storage>(
    storage: &S,
    requests: &[GetManyRequest<'_>],
    metrics: &mut Metrics,
) -> Result<Vec<Option<Bytes>>, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    get_values_from(&read, requests, metrics).await
}

async fn get_values_from<R: StorageRead>(
    read: &R,
    requests: &[GetManyRequest<'_>],
    metrics: &mut Metrics,
) -> Result<Vec<Option<Bytes>>, StorageError> {
    metrics.get_calls += 1;
    metrics.get_keys += requests
        .iter()
        .map(|request| request.keys.len() as u64)
        .sum::<u64>();
    let result = read.get_many(requests).await?;
    result
        .values
        .into_iter()
        .map(|value| {
            let value = full_value(value, "point read")?;
            metrics.get_bytes += value.as_ref().map_or(0, |bytes| bytes.len() as u64);
            Ok(value)
        })
        .collect()
}

async fn scan_prefix<R: StorageRead>(
    read: &R,
    space: lix::storage::StorageSpace,
    prefix: &[u8],
    metrics: &mut Metrics,
) -> Result<Vec<(Key, Bytes)>, StorageError> {
    let range = prefix_range(prefix);
    let mut rows = Vec::new();
    let mut resume_after = None;
    loop {
        metrics.scan_calls += 1;
        let page = read
            .scan(
                space,
                range.clone(),
                ScanOptions {
                    projection: CoreProjection::FullValue,
                    limit_rows: PAGE_ROWS,
                    resume_after: resume_after.clone(),
                },
            )
            .await?;
        if page.has_more && page.entries.is_empty() {
            return Err(corruption("scan claims more after empty page"));
        }
        let mut previous = resume_after.as_ref();
        for entry in page.entries {
            if previous.is_some_and(|previous| previous >= &entry.key) {
                return Err(corruption("scan page is noncanonical"));
            }
            let value = full_value(Some(entry.value), "scan")?
                .ok_or_else(|| corruption("scan row has no value"))?;
            metrics.scan_rows += 1;
            metrics.scan_bytes += (entry.key.0.len() + value.len()) as u64;
            resume_after = Some(entry.key.clone());
            rows.push((entry.key, value));
            previous = resume_after.as_ref();
        }
        if !page.has_more {
            return Ok(rows);
        }
    }
}

async fn put_batches<S: Storage>(
    storage: &S,
    options: WriteOptions,
    batches: Vec<(lix::storage::StorageSpace, PutBatch)>,
    deletes: Vec<(lix::storage::StorageSpace, Vec<Key>)>,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let mut write = storage.begin_write(options).await?;
    for (space, batch) in batches {
        metrics.puts += batch.entries.len() as u64;
        metrics.write_bytes += batch
            .entries
            .iter()
            .map(|entry| (entry.key.0.len() + entry.value.bytes.len()) as u64)
            .sum::<u64>();
        write.put_many(space, batch).await?;
    }
    for (space, keys) in deletes {
        metrics.deletes += keys.len() as u64;
        metrics.write_bytes += keys.iter().map(|key| key.0.len() as u64).sum::<u64>();
        write.delete_many(space, &keys).await?;
    }
    write.commit().await?;
    metrics.commits += 1;
    Ok(())
}

async fn load_global<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<(Bytes, Global), StorageError> {
    let keys = [key(Bytes::from_static(GLOBAL_KEY))];
    let values = get_values(
        storage,
        &[GetManyRequest {
            space: META_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
        metrics,
    )
    .await?;
    let raw = values[0]
        .clone()
        .ok_or_else(|| corruption("global authority is absent"))?;
    let global = Global::decode(&raw)?;
    Ok((raw, global))
}

async fn seed_global<S: Storage>(storage: &S, metrics: &mut Metrics) -> Result<(), StorageError> {
    let global = Global {
        epoch: 1,
        gc_watermark: 0,
        next_lease_generation: 1,
    }
    .encode();
    put_batches(
        storage,
        WriteOptions {
            preconditions: vec![Precondition::KeyAbsent {
                space: META_SPACE,
                key: key(Bytes::from_static(GLOBAL_KEY)),
            }],
            ..WriteOptions::default()
        },
        vec![(
            META_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: key(Bytes::from_static(GLOBAL_KEY)),
                    value: StoredValue { bytes: global },
                }],
            },
        )],
        Vec::new(),
        metrics,
    )
    .await
}

async fn stage_graph<S: Storage>(
    storage: &S,
    seed: u64,
    metrics: &mut Metrics,
) -> Result<Id, StorageError> {
    let mut child = None;
    let mut entries = Vec::with_capacity(GRAPH_DEPTH);
    for depth in 0..GRAPH_DEPTH {
        let (id, bytes) = object_bytes(child, seed, depth);
        entries.push(PutEntry {
            key: id_key(id),
            value: StoredValue { bytes },
        });
        child = Some(id);
    }
    let root = child.expect("nonempty graph");
    put_batches(
        storage,
        WriteOptions::default(),
        vec![(OBJECT_SPACE, PutBatch { entries })],
        Vec::new(),
        metrics,
    )
    .await?;
    Ok(root)
}

async fn seed_fixture<S: Storage>(
    storage: &S,
    readers: usize,
    metrics: &mut Metrics,
) -> Result<Fixture, StorageError> {
    seed_global(storage, metrics).await?;
    let mut reader_roots = Vec::with_capacity(readers);
    let mut current_roots = Vec::with_capacity(readers);
    let mut branch_entries = Vec::with_capacity(readers);
    for reader in 0..readers {
        let old = stage_graph(storage, 10_000 + reader as u64, metrics).await?;
        let current = stage_graph(storage, 20_000 + reader as u64, metrics).await?;
        reader_roots.push(old);
        current_roots.push(current);
        branch_entries.push(PutEntry {
            key: branch_key(reader as u64),
            value: StoredValue {
                bytes: root_value(old),
            },
        });
    }
    put_batches(
        storage,
        WriteOptions::default(),
        vec![(
            META_SPACE,
            PutBatch {
                entries: branch_entries,
            },
        )],
        Vec::new(),
        metrics,
    )
    .await?;

    // Child branch, checkpoint, history, undo, redo, and open upload roots.
    let mut static_roots = Vec::new();
    let mut static_entries = Vec::new();
    for label in 1..=6u8 {
        let root = stage_graph(storage, 30_000 + u64::from(label), metrics).await?;
        static_roots.push(root);
        static_entries.push(PutEntry {
            key: static_root_key(label),
            value: StoredValue {
                bytes: root_value(root),
            },
        });
    }
    put_batches(
        storage,
        WriteOptions::default(),
        vec![(
            META_SPACE,
            PutBatch {
                entries: static_entries,
            },
        )],
        Vec::new(),
        metrics,
    )
    .await?;

    let mut orphan_roots = Vec::with_capacity(readers.max(1));
    for orphan in 0..readers.max(1) {
        orphan_roots.push(stage_graph(storage, 40_000 + orphan as u64, metrics).await?);
    }
    Ok(Fixture {
        reader_roots,
        current_roots,
        static_roots,
        orphan_roots,
    })
}

async fn prepare_acquire<S: Storage>(
    storage: &S,
    lease_id: u64,
    root: Id,
    span: u64,
    metrics: &mut Metrics,
) -> Result<PreparedLeaseMutation, StorageError> {
    let global_key = key(Bytes::from_static(GLOBAL_KEY));
    let branch = branch_key(lease_id);
    let keys = [global_key, branch];
    let values = get_values(
        storage,
        &[GetManyRequest {
            space: META_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
        metrics,
    )
    .await?;
    let raw_global = values[0]
        .clone()
        .ok_or_else(|| corruption("global is absent during lease acquire"))?;
    let global = Global::decode(&raw_global)?;
    let selected_root = decode_root(
        values[1]
            .as_ref()
            .ok_or_else(|| corruption("reader branch selector is absent"))?,
    )?;
    if selected_root != root {
        return Err(corruption(
            "lease root is not selected by its coherent view",
        ));
    }
    let next_epoch = global.epoch + 1;
    Ok(PreparedLeaseMutation {
        raw_global,
        global: global.clone(),
        key: lease_key(lease_id),
        raw_lease: None,
        lease: Lease {
            lease_id,
            generation: global.next_lease_generation,
            root,
            view_id: view_id(lease_id, root),
            valid_through_epoch: next_epoch.saturating_add(span),
        },
    })
}

async fn prepare_renew<S: Storage>(
    storage: &S,
    lease_id: u64,
    span: u64,
    metrics: &mut Metrics,
) -> Result<PreparedLeaseMutation, StorageError> {
    let global_key = key(Bytes::from_static(GLOBAL_KEY));
    let lease_key = lease_key(lease_id);
    let keys = [global_key, lease_key.clone()];
    let values = get_values(
        storage,
        &[GetManyRequest {
            space: META_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
        metrics,
    )
    .await?;
    let raw_global = values[0]
        .clone()
        .ok_or_else(|| corruption("global is absent during lease renewal"))?;
    let global = Global::decode(&raw_global)?;
    let raw_lease = values[1].clone().ok_or(StorageError::ReadExpired)?;
    let old = Lease::decode(&lease_key, &raw_lease, &global)?;
    if old.valid_through_epoch < global.epoch {
        return Err(StorageError::ReadExpired);
    }
    Ok(PreparedLeaseMutation {
        raw_global,
        global: global.clone(),
        key: lease_key,
        raw_lease: Some(raw_lease),
        lease: Lease {
            generation: global.next_lease_generation,
            valid_through_epoch: global.epoch.saturating_add(1).saturating_add(span),
            ..old
        },
    })
}

async fn commit_lease_mutation<S: Storage>(
    storage: &S,
    prepared: &PreparedLeaseMutation,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let mut next = prepared.global.rotated();
    next.next_lease_generation = prepared.global.next_lease_generation + 1;
    let lease_bytes = prepared.lease.encode();
    let mut preconditions = vec![Precondition::KeyValueEquals {
        space: META_SPACE,
        key: key(Bytes::from_static(GLOBAL_KEY)),
        expected: prepared.raw_global.clone(),
    }];
    preconditions.push(match &prepared.raw_lease {
        Some(raw) => Precondition::KeyValueEquals {
            space: META_SPACE,
            key: prepared.key.clone(),
            expected: raw.clone(),
        },
        None => Precondition::KeyAbsent {
            space: META_SPACE,
            key: prepared.key.clone(),
        },
    });
    put_batches(
        storage,
        WriteOptions {
            preconditions,
            ..WriteOptions::default()
        },
        vec![(
            META_SPACE,
            PutBatch {
                entries: vec![
                    PutEntry {
                        key: key(Bytes::from_static(GLOBAL_KEY)),
                        value: StoredValue {
                            bytes: next.encode(),
                        },
                    },
                    PutEntry {
                        key: prepared.key.clone(),
                        value: StoredValue { bytes: lease_bytes },
                    },
                ],
            },
        )],
        vec![(META_SPACE, vec![key(Bytes::from_static(GC_PROGRESS_KEY))])],
        metrics,
    )
    .await
}

async fn release_lease<S: Storage>(
    storage: &S,
    lease_id: u64,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let (raw_global, global) = load_global(storage, metrics).await?;
    let lease_key = lease_key(lease_id);
    let keys = [lease_key.clone()];
    let values = get_values(
        storage,
        &[GetManyRequest {
            space: META_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
        metrics,
    )
    .await?;
    let raw_lease = values[0].clone().ok_or(StorageError::ReadExpired)?;
    Lease::decode(&lease_key, &raw_lease, &global)?;
    put_batches(
        storage,
        WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: META_SPACE,
                    key: key(Bytes::from_static(GLOBAL_KEY)),
                    expected: raw_global,
                },
                Precondition::KeyValueEquals {
                    space: META_SPACE,
                    key: lease_key.clone(),
                    expected: raw_lease,
                },
            ],
            ..WriteOptions::default()
        },
        vec![(
            META_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: key(Bytes::from_static(GLOBAL_KEY)),
                    value: StoredValue {
                        bytes: global.rotated().encode(),
                    },
                }],
            },
        )],
        vec![
            (META_SPACE, vec![lease_key]),
            (META_SPACE, vec![key(Bytes::from_static(GC_PROGRESS_KEY))]),
        ],
        metrics,
    )
    .await
}

async fn publish_branch<S: Storage>(
    storage: &S,
    branch_id: u64,
    expected_root: Id,
    next_root: Id,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let (raw_global, global) = load_global(storage, metrics).await?;
    let branch_key = branch_key(branch_id);
    put_batches(
        storage,
        WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: META_SPACE,
                    key: key(Bytes::from_static(GLOBAL_KEY)),
                    expected: raw_global,
                },
                Precondition::KeyValueEquals {
                    space: META_SPACE,
                    key: branch_key.clone(),
                    expected: root_value(expected_root),
                },
            ],
            ..WriteOptions::default()
        },
        vec![(
            META_SPACE,
            PutBatch {
                entries: vec![
                    PutEntry {
                        key: key(Bytes::from_static(GLOBAL_KEY)),
                        value: StoredValue {
                            bytes: global.rotated().encode(),
                        },
                    },
                    PutEntry {
                        key: branch_key,
                        value: StoredValue {
                            bytes: root_value(next_root),
                        },
                    },
                ],
            },
        )],
        vec![(META_SPACE, vec![key(Bytes::from_static(GC_PROGRESS_KEY))])],
        metrics,
    )
    .await
}

async fn seed_branch_selector<S: Storage>(
    storage: &S,
    branch_id: u64,
    root: Id,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    put_batches(
        storage,
        WriteOptions {
            preconditions: vec![Precondition::KeyAbsent {
                space: META_SPACE,
                key: branch_key(branch_id),
            }],
            ..WriteOptions::default()
        },
        vec![(
            META_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: branch_key(branch_id),
                    value: StoredValue {
                        bytes: root_value(root),
                    },
                }],
            },
        )],
        Vec::new(),
        metrics,
    )
    .await
}

async fn advance_epoch<S: Storage>(storage: &S, metrics: &mut Metrics) -> Result<(), StorageError> {
    let (raw_global, global) = load_global(storage, metrics).await?;
    put_batches(
        storage,
        WriteOptions {
            preconditions: vec![Precondition::KeyValueEquals {
                space: META_SPACE,
                key: key(Bytes::from_static(GLOBAL_KEY)),
                expected: raw_global,
            }],
            ..WriteOptions::default()
        },
        vec![(
            META_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: key(Bytes::from_static(GLOBAL_KEY)),
                    value: StoredValue {
                        bytes: global.rotated().encode(),
                    },
                }],
            },
        )],
        vec![(META_SPACE, vec![key(Bytes::from_static(GC_PROGRESS_KEY))])],
        metrics,
    )
    .await
}

async fn acquire_all<S: Storage>(
    storage: &S,
    fixture: &Fixture,
    span: u64,
    metrics: &mut Metrics,
) -> Result<Vec<Cursor>, StorageError> {
    let mut cursors = Vec::with_capacity(fixture.reader_roots.len());
    for (reader, root) in fixture.reader_roots.iter().copied().enumerate() {
        let prepared = prepare_acquire(storage, reader as u64, root, span, metrics).await?;
        commit_lease_mutation(storage, &prepared, metrics).await?;
        cursors.push(Cursor {
            lease_id: prepared.lease.lease_id,
            lease_generation: prepared.lease.generation,
            root: prepared.lease.root,
            view_id: prepared.lease.view_id,
            resume_after: 0,
        });
    }
    Ok(cursors)
}

async fn resume_cursor<S: Storage>(
    storage: &S,
    encoded: &Bytes,
    metrics: &mut Metrics,
) -> Result<Cursor, StorageError> {
    let cursor = Cursor::decode(encoded)?;
    let global_key = key(Bytes::from_static(GLOBAL_KEY));
    let lease_key = lease_key(cursor.lease_id);
    let object_key = id_key(cursor.root);
    let read = storage.begin_read(ReadOptions::default()).await?;
    let requests = [
        GetManyRequest {
            space: META_SPACE,
            keys: &[global_key, lease_key.clone()],
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        },
        GetManyRequest {
            space: OBJECT_SPACE,
            keys: &[object_key],
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        },
    ];
    let values = get_values_from(&read, &requests, metrics).await?;
    let raw_global = values[0]
        .as_ref()
        .ok_or_else(|| corruption("cursor global authority is absent"))?;
    let global = Global::decode(raw_global)?;
    let raw_lease = values[1].as_ref().ok_or(StorageError::ReadExpired)?;
    let lease = Lease::decode(&lease_key, raw_lease, &global)?;
    if lease.generation != cursor.lease_generation
        || lease.root != cursor.root
        || lease.view_id != cursor.view_id
        || lease.valid_through_epoch < global.epoch
    {
        return Err(StorageError::ReadExpired);
    }
    let object = values[2]
        .as_ref()
        .ok_or_else(|| corruption("live reader lease points to a missing object"))?;
    decode_object(cursor.root, object)?;
    Ok(Cursor {
        resume_after: cursor.resume_after + 1,
        ..cursor
    })
}

async fn prepare_gc<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<PreparedGc, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    let global_keys = [key(Bytes::from_static(GLOBAL_KEY))];
    let values = get_values_from(
        &read,
        &[GetManyRequest {
            space: META_SPACE,
            keys: &global_keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
        metrics,
    )
    .await?;
    let raw_global = values[0]
        .clone()
        .ok_or_else(|| corruption("GC global authority is absent"))?;
    let global = Global::decode(&raw_global)?;
    let next_global = global.rotated();
    let mut roots = Vec::new();
    for (_, raw) in scan_prefix(&read, META_SPACE, ROOT_PREFIX, metrics).await? {
        roots.push(decode_root(&raw)?);
    }
    let mut live_leases = Vec::new();
    let mut expired_leases = Vec::new();
    for (key, raw) in scan_prefix(&read, META_SPACE, LEASE_PREFIX, metrics).await? {
        let lease = Lease::decode(&key, &raw, &global)?;
        if lease.valid_through_epoch >= next_global.epoch {
            roots.push(lease.root);
            live_leases.push((lease.generation, raw));
        } else {
            expired_leases.push((key, raw));
        }
    }
    live_leases.sort_by_key(|(generation, _)| *generation);
    let minimum_live_generation = live_leases.first().map_or(0, |(generation, _)| *generation);
    let mut digest = blake3::Hasher::new();
    digest.update(b"live-reader-leases-v1");
    for (_, raw) in &live_leases {
        digest.update(&(raw.len() as u64).to_be_bytes());
        digest.update(raw);
    }
    let progress = GcProgress {
        cycle: next_global.epoch,
        fenced_global: next_global.encode(),
        minimum_live_generation,
        live_lease_count: live_leases.len() as u64,
        live_lease_digest: *digest.finalize().as_bytes(),
    };
    Ok(PreparedGc {
        raw_global,
        next_global,
        progress,
        roots,
        expired_leases,
    })
}

async fn start_gc<S: Storage>(
    storage: &S,
    prepared: &PreparedGc,
    metrics: &mut Metrics,
) -> Result<Bytes, StorageError> {
    let raw_progress = prepared.progress.encode();
    let mut preconditions = vec![
        Precondition::KeyValueEquals {
            space: META_SPACE,
            key: key(Bytes::from_static(GLOBAL_KEY)),
            expected: prepared.raw_global.clone(),
        },
        Precondition::KeyAbsent {
            space: META_SPACE,
            key: key(Bytes::from_static(GC_PROGRESS_KEY)),
        },
    ];
    preconditions.extend(prepared.expired_leases.iter().map(|(key, raw)| {
        Precondition::KeyValueEquals {
            space: META_SPACE,
            key: key.clone(),
            expected: raw.clone(),
        }
    }));
    put_batches(
        storage,
        WriteOptions {
            preconditions,
            ..WriteOptions::default()
        },
        vec![(
            META_SPACE,
            PutBatch {
                entries: vec![
                    PutEntry {
                        key: key(Bytes::from_static(GLOBAL_KEY)),
                        value: StoredValue {
                            bytes: prepared.next_global.encode(),
                        },
                    },
                    PutEntry {
                        key: key(Bytes::from_static(GC_PROGRESS_KEY)),
                        value: StoredValue {
                            bytes: raw_progress.clone(),
                        },
                    },
                ],
            },
        )],
        if prepared.expired_leases.is_empty() {
            Vec::new()
        } else {
            vec![(
                META_SPACE,
                prepared
                    .expired_leases
                    .iter()
                    .map(|(key, _)| key.clone())
                    .collect(),
            )]
        },
        metrics,
    )
    .await?;
    Ok(raw_progress)
}

fn gc_preconditions(progress: &GcProgress, raw_progress: &Bytes) -> Vec<Precondition> {
    vec![
        Precondition::KeyValueEquals {
            space: META_SPACE,
            key: key(Bytes::from_static(GLOBAL_KEY)),
            expected: progress.fenced_global.clone(),
        },
        Precondition::KeyValueEquals {
            space: META_SPACE,
            key: key(Bytes::from_static(GC_PROGRESS_KEY)),
            expected: raw_progress.clone(),
        },
    ]
}

async fn run_gc<S: Storage>(
    storage: &S,
    prepared: PreparedGc,
    raw_progress: Bytes,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let progress = GcProgress::decode(&raw_progress)?;
    if progress != prepared.progress || progress.fenced_global != prepared.next_global.encode() {
        return Err(corruption(
            "GC progress does not match its prepared safe point",
        ));
    }
    let mut queue = VecDeque::from(prepared.roots);
    metrics.peak_queue = metrics.peak_queue.max(queue.len() as u64);
    while !queue.is_empty() {
        let mut mark_entries = Vec::new();
        for _ in 0..PAGE_ROWS {
            let Some(id) = queue.pop_front() else {
                break;
            };
            let mark = mark_key(progress.cycle, id);
            let object = id_key(id);
            let requests = [
                GetManyRequest {
                    space: MARK_SPACE,
                    keys: std::slice::from_ref(&mark),
                    opts: GetOptions {
                        projection: CoreProjection::FullValue,
                    },
                },
                GetManyRequest {
                    space: OBJECT_SPACE,
                    keys: &[object],
                    opts: GetOptions {
                        projection: CoreProjection::FullValue,
                    },
                },
            ];
            let values = get_values(storage, &requests, metrics).await?;
            if values[0].is_some() {
                continue;
            }
            let object_bytes = values[1].as_ref().ok_or_else(|| {
                corruption(format!(
                    "GC live root points to missing object {id:02x?} in cycle {}",
                    progress.cycle
                ))
            })?;
            if let Some(child) = decode_object(id, object_bytes)? {
                queue.push_back(child);
            }
            mark_entries.push(PutEntry {
                key: mark,
                value: StoredValue {
                    bytes: Bytes::copy_from_slice(&id),
                },
            });
        }
        metrics.peak_queue = metrics.peak_queue.max(queue.len() as u64);
        if !mark_entries.is_empty() {
            metrics.marked += mark_entries.len() as u64;
            put_batches(
                storage,
                WriteOptions {
                    preconditions: gc_preconditions(&progress, &raw_progress),
                    ..WriteOptions::default()
                },
                vec![(
                    MARK_SPACE,
                    PutBatch {
                        entries: mark_entries,
                    },
                )],
                Vec::new(),
                metrics,
            )
            .await?;
        }
    }

    let read = storage.begin_read(ReadOptions::default()).await?;
    let objects = scan_prefix(&read, OBJECT_SPACE, b"", metrics).await?;
    drop(read);
    for page in objects.chunks(PAGE_ROWS) {
        let mark_keys = page
            .iter()
            .map(|(key, _)| {
                let id = read_id(&key.0, 0).expect("object key id");
                mark_key(progress.cycle, id)
            })
            .collect::<Vec<_>>();
        let values = get_values(
            storage,
            &[GetManyRequest {
                space: MARK_SPACE,
                keys: &mark_keys,
                opts: GetOptions {
                    projection: CoreProjection::FullValue,
                },
            }],
            metrics,
        )
        .await?;
        let deletes = page
            .iter()
            .zip(values)
            .filter_map(|((key, _), mark)| mark.is_none().then_some(key.clone()))
            .collect::<Vec<_>>();
        if !deletes.is_empty() {
            metrics.swept += deletes.len() as u64;
            put_batches(
                storage,
                WriteOptions {
                    preconditions: gc_preconditions(&progress, &raw_progress),
                    ..WriteOptions::default()
                },
                Vec::new(),
                vec![(OBJECT_SPACE, deletes)],
                metrics,
            )
            .await?;
        }
    }

    let read = storage.begin_read(ReadOptions::default()).await?;
    let marks = scan_prefix(&read, MARK_SPACE, MARK_PREFIX, metrics).await?;
    drop(read);
    for page in marks.chunks(PAGE_ROWS) {
        put_batches(
            storage,
            WriteOptions {
                preconditions: gc_preconditions(&progress, &raw_progress),
                ..WriteOptions::default()
            },
            Vec::new(),
            vec![(
                MARK_SPACE,
                page.iter().map(|(key, _)| key.clone()).collect(),
            )],
            metrics,
        )
        .await?;
    }
    let mut completed = prepared.next_global.rotated();
    completed.gc_watermark = progress.cycle;
    put_batches(
        storage,
        WriteOptions {
            preconditions: gc_preconditions(&progress, &raw_progress),
            ..WriteOptions::default()
        },
        vec![(
            META_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: key(Bytes::from_static(GLOBAL_KEY)),
                    value: StoredValue {
                        bytes: completed.encode(),
                    },
                }],
            },
        )],
        vec![(META_SPACE, vec![key(Bytes::from_static(GC_PROGRESS_KEY))])],
        metrics,
    )
    .await
}

async fn collect_gc<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<GcProgress, StorageError> {
    let prepared = prepare_gc(storage, metrics).await?;
    let expected = prepared.progress.clone();
    let raw = start_gc(storage, &prepared, metrics).await?;
    run_gc(storage, prepared, raw, metrics).await?;
    Ok(expected)
}

async fn object_present<S: Storage>(
    storage: &S,
    id: Id,
    metrics: &mut Metrics,
) -> Result<bool, StorageError> {
    let keys = [id_key(id)];
    let values = get_values(
        storage,
        &[GetManyRequest {
            space: OBJECT_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
        metrics,
    )
    .await?;
    Ok(values[0].is_some())
}

async fn lease_present<S: Storage>(
    storage: &S,
    lease_id: u64,
    metrics: &mut Metrics,
) -> Result<Option<Lease>, StorageError> {
    let (_, global) = load_global(storage, metrics).await?;
    let key = lease_key(lease_id);
    let keys = [key.clone()];
    let values = get_values(
        storage,
        &[GetManyRequest {
            space: META_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
        metrics,
    )
    .await?;
    values[0]
        .as_ref()
        .map(|raw| Lease::decode(&key, raw, &global))
        .transpose()
}

async fn run_oracle<S: Storage>(storage: &S) -> Result<(), StorageError> {
    let mut metrics = Metrics::default();
    let fixture = seed_fixture(storage, 2, &mut metrics).await?;
    let prepared_before = prepare_acquire(
        storage,
        0,
        fixture.reader_roots[0],
        BENCH_LEASE_SPAN,
        &mut metrics,
    )
    .await?;
    assert!(lease_present(storage, 0, &mut metrics).await?.is_none());
    commit_lease_mutation(storage, &prepared_before, &mut metrics).await?;
    let cursor = Cursor {
        lease_id: 0,
        lease_generation: prepared_before.lease.generation,
        root: prepared_before.lease.root,
        view_id: prepared_before.lease.view_id,
        resume_after: 0,
    };
    let second = prepare_acquire(
        storage,
        1,
        fixture.reader_roots[1],
        BENCH_LEASE_SPAN,
        &mut metrics,
    )
    .await?;
    commit_lease_mutation(storage, &second, &mut metrics).await?;
    let page_two = resume_cursor(storage, &cursor.encode(), &mut metrics).await?;
    assert_eq!(page_two.resume_after, 1);

    // A process-local coherent read remains pinned while publication and GC
    // run; the persisted lease makes the same root safe across reopen.
    let old_read = storage.begin_read(ReadOptions::default()).await?;
    publish_branch(
        storage,
        0,
        fixture.reader_roots[0],
        fixture.current_roots[0],
        &mut metrics,
    )
    .await?;
    publish_branch(
        storage,
        1,
        fixture.reader_roots[1],
        fixture.current_roots[1],
        &mut metrics,
    )
    .await?;
    let progress = collect_gc(storage, &mut metrics).await?;
    assert_eq!(progress.live_lease_count, 2);
    assert_eq!(progress.minimum_live_generation, cursor.lease_generation);
    let old_keys = [id_key(fixture.reader_roots[0])];
    let old_values = get_values_from(
        &old_read,
        &[GetManyRequest {
            space: OBJECT_SPACE,
            keys: &old_keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
        &mut metrics,
    )
    .await?;
    assert!(old_values[0].is_some());
    drop(old_read);
    assert!(object_present(storage, fixture.reader_roots[0], &mut metrics).await?);

    // Renewal-first invalidates a stale GC start; GC-first invalidates a stale
    // renewal and is itself aborted by the exact retry.
    let stale_gc = prepare_gc(storage, &mut metrics).await?;
    let renewal = prepare_renew(storage, 0, BENCH_LEASE_SPAN, &mut metrics).await?;
    commit_lease_mutation(storage, &renewal, &mut metrics).await?;
    assert!(matches!(
        start_gc(storage, &stale_gc, &mut metrics).await,
        Err(StorageError::PreconditionFailed(_))
    ));

    let gc_first = prepare_gc(storage, &mut metrics).await?;
    let stale_renewal = prepare_renew(storage, 0, BENCH_LEASE_SPAN, &mut metrics).await?;
    let raw_progress = start_gc(storage, &gc_first, &mut metrics).await?;
    assert!(matches!(
        commit_lease_mutation(storage, &stale_renewal, &mut metrics).await,
        Err(StorageError::PreconditionFailed(_))
    ));
    let retry = prepare_renew(storage, 0, BENCH_LEASE_SPAN, &mut metrics).await?;
    commit_lease_mutation(storage, &retry, &mut metrics).await?;
    assert!(
        run_gc(storage, gc_first, raw_progress, &mut metrics)
            .await
            .is_err()
    );

    // Release expires all old cursor generations immediately and does not use
    // wall time. The final reference controls reclamation.
    seed_branch_selector(storage, 99, fixture.reader_roots[0], &mut metrics).await?;
    let shared = prepare_acquire(
        storage,
        99,
        fixture.reader_roots[0],
        BENCH_LEASE_SPAN,
        &mut metrics,
    )
    .await?;
    commit_lease_mutation(storage, &shared, &mut metrics).await?;
    publish_branch(
        storage,
        99,
        fixture.reader_roots[0],
        fixture.current_roots[0],
        &mut metrics,
    )
    .await?;
    release_lease(storage, 0, &mut metrics).await?;
    assert!(matches!(
        resume_cursor(storage, &cursor.encode(), &mut metrics).await,
        Err(StorageError::ReadExpired)
    ));
    collect_gc(storage, &mut metrics).await?;
    assert!(object_present(storage, fixture.reader_roots[0], &mut metrics).await?);
    release_lease(storage, 99, &mut metrics).await?;
    collect_gc(storage, &mut metrics).await?;
    assert!(!object_present(storage, fixture.reader_roots[0], &mut metrics).await?);
    assert!(object_present(storage, fixture.reader_roots[1], &mut metrics).await?);
    release_lease(storage, 1, &mut metrics).await?;
    collect_gc(storage, &mut metrics).await?;
    assert!(!object_present(storage, fixture.reader_roots[1], &mut metrics).await?);

    // Logical-epoch expiry is safe under clock/process failure: the persisted
    // row cannot resume or renew, and the next GC reaps it atomically when
    // establishing its raw-global safe point.
    let expiring = prepare_acquire(storage, 0, fixture.current_roots[0], 1, &mut metrics).await?;
    commit_lease_mutation(storage, &expiring, &mut metrics).await?;
    let expired_cursor = Cursor {
        lease_id: 0,
        lease_generation: expiring.lease.generation,
        root: expiring.lease.root,
        view_id: expiring.lease.view_id,
        resume_after: 0,
    };
    advance_epoch(storage, &mut metrics).await?;
    advance_epoch(storage, &mut metrics).await?;
    assert!(matches!(
        resume_cursor(storage, &expired_cursor.encode(), &mut metrics).await,
        Err(StorageError::ReadExpired)
    ));
    assert!(matches!(
        prepare_renew(storage, 0, 1, &mut metrics).await,
        Err(StorageError::ReadExpired)
    ));
    collect_gc(storage, &mut metrics).await?;
    assert!(lease_present(storage, 0, &mut metrics).await?.is_none());
    assert!(object_present(storage, fixture.current_roots[0], &mut metrics).await?);

    for root in &fixture.static_roots {
        assert!(object_present(storage, *root, &mut metrics).await?);
    }
    for root in &fixture.orphan_roots {
        assert!(!object_present(storage, *root, &mut metrics).await?);
    }
    Ok(())
}

async fn run_corruption_oracle<S: Storage>(storage: &S) -> Result<(), StorageError> {
    let mut metrics = Metrics::default();
    let fixture = seed_fixture(storage, 1, &mut metrics).await?;
    let prepared = prepare_acquire(
        storage,
        0,
        fixture.reader_roots[0],
        BENCH_LEASE_SPAN,
        &mut metrics,
    )
    .await?;
    commit_lease_mutation(storage, &prepared, &mut metrics).await?;
    publish_branch(
        storage,
        0,
        fixture.reader_roots[0],
        fixture.current_roots[0],
        &mut metrics,
    )
    .await?;

    // A malformed persisted lease aborts root collection before any deletion.
    let raw_lease = prepared.lease.encode();
    let mut malformed = raw_lease.to_vec();
    malformed[24] ^= 0x80;
    let mut write = storage.begin_write(WriteOptions::default()).await?;
    write
        .put_many(
            META_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: lease_key(0),
                    value: StoredValue {
                        bytes: Bytes::from(malformed),
                    },
                }],
            },
        )
        .await?;
    write.commit().await?;
    assert!(prepare_gc(storage, &mut metrics).await.is_err());
    assert!(object_present(storage, fixture.reader_roots[0], &mut metrics).await?);

    let mut restore = storage.begin_write(WriteOptions::default()).await?;
    restore
        .put_many(
            META_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: lease_key(0),
                    value: StoredValue { bytes: raw_lease },
                }],
            },
        )
        .await?;
    restore.commit().await?;

    // Corrupting the persisted minimum/digest proof aborts mark/sweep under
    // exact global/progress preconditions.
    let prepared_gc = prepare_gc(storage, &mut metrics).await?;
    let raw_progress = start_gc(storage, &prepared_gc, &mut metrics).await?;
    let mut malformed_progress = raw_progress.to_vec();
    let final_byte = malformed_progress.len() - 1;
    malformed_progress[final_byte] ^= 0x01;
    let mut corrupt = storage.begin_write(WriteOptions::default()).await?;
    corrupt
        .put_many(
            META_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: key(Bytes::from_static(GC_PROGRESS_KEY)),
                    value: StoredValue {
                        bytes: Bytes::from(malformed_progress),
                    },
                }],
            },
        )
        .await?;
    corrupt.commit().await?;
    assert!(
        run_gc(storage, prepared_gc, raw_progress, &mut metrics)
            .await
            .is_err()
    );
    assert!(object_present(storage, fixture.reader_roots[0], &mut metrics).await?);
    Ok(())
}

async fn prepare_reopen_oracle<S: Storage>(storage: &S) -> Result<ReopenFixture, StorageError> {
    let mut metrics = Metrics::default();
    let fixture = seed_fixture(storage, 1, &mut metrics).await?;

    // Crash before publication: prepared bytes are not authority.
    let acquisition = prepare_acquire(
        storage,
        0,
        fixture.reader_roots[0],
        BENCH_LEASE_SPAN,
        &mut metrics,
    )
    .await?;
    assert!(lease_present(storage, 0, &mut metrics).await?.is_none());
    commit_lease_mutation(storage, &acquisition, &mut metrics).await?;
    let old_cursor = Cursor {
        lease_id: 0,
        lease_generation: acquisition.lease.generation,
        root: acquisition.lease.root,
        view_id: acquisition.lease.view_id,
        resume_after: 0,
    };

    // Crash before renewal leaves the exact old generation. Crash after the
    // atomic renewal leaves only the new generation and its cursor binding.
    let renewal = prepare_renew(storage, 0, BENCH_LEASE_SPAN, &mut metrics).await?;
    assert_eq!(
        lease_present(storage, 0, &mut metrics)
            .await?
            .expect("acquired lease"),
        acquisition.lease
    );
    commit_lease_mutation(storage, &renewal, &mut metrics).await?;
    Ok(ReopenFixture {
        old_cursor,
        renewed_cursor: Cursor {
            lease_id: 0,
            lease_generation: renewal.lease.generation,
            root: renewal.lease.root,
            view_id: renewal.lease.view_id,
            resume_after: 0,
        },
        renewed_lease: renewal.lease,
        old_root: fixture.reader_roots[0],
        current_root: fixture.current_roots[0],
    })
}

async fn verify_reopen_oracle<S: Storage>(
    storage: &S,
    fixture: &ReopenFixture,
) -> Result<(), StorageError> {
    let mut metrics = Metrics::default();
    assert_eq!(
        lease_present(storage, 0, &mut metrics)
            .await?
            .expect("renewed lease survives reopen"),
        fixture.renewed_lease
    );
    assert!(matches!(
        resume_cursor(storage, &fixture.old_cursor.encode(), &mut metrics).await,
        Err(StorageError::ReadExpired)
    ));
    resume_cursor(storage, &fixture.renewed_cursor.encode(), &mut metrics).await?;
    publish_branch(
        storage,
        0,
        fixture.old_root,
        fixture.current_root,
        &mut metrics,
    )
    .await?;
    collect_gc(storage, &mut metrics).await?;
    assert!(object_present(storage, fixture.old_root, &mut metrics).await?);
    Ok(())
}

fn begin_profile() {
    PROFILE_ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    PROFILE_ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    PROFILE_ALLOCATION_ENABLED.store(true, Ordering::Relaxed);
}

fn end_profile() -> (u64, u64) {
    PROFILE_ALLOCATION_ENABLED.store(false, Ordering::Relaxed);
    (
        PROFILE_ALLOCATED_BYTES.load(Ordering::Relaxed),
        PROFILE_ALLOCATION_CALLS.load(Ordering::Relaxed),
    )
}

fn process_resident_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").expect("read process statm");
    statm
        .split_whitespace()
        .nth(1)
        .expect("resident pages")
        .parse::<u64>()
        .expect("parse resident pages")
        * 4096
}

fn process_cpu_nanos() -> u64 {
    std::fs::read_dir("/proc/self/task")
        .expect("read process task directory")
        .flatten()
        .map(|entry| entry.path().join("schedstat"))
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .filter_map(|stat| {
            stat.split_whitespace()
                .next()
                .and_then(|value| value.parse::<u64>().ok())
        })
        .sum()
}

fn directory_bytes(path: &Path) -> u64 {
    fn visit(path: &Path) -> u64 {
        let Ok(metadata) = std::fs::symlink_metadata(path) else {
            return 0;
        };
        if metadata.is_file() {
            return metadata.len();
        }
        if !metadata.is_dir() {
            return 0;
        }
        std::fs::read_dir(path).map_or(0, |entries| {
            entries.flatten().map(|entry| visit(&entry.path())).sum()
        })
    }
    visit(path)
}

async fn measure<S: Storage>(
    backend: &str,
    storage: &S,
    path: &Path,
    counters: Option<&SlateDBIoCounters>,
    readers: usize,
) -> Result<(), StorageError> {
    let mut setup = Metrics::default();
    let fixture = seed_fixture(storage, readers, &mut setup).await?;

    let physical_before =
        counters.map_or_else(SlateDBIoSnapshot::default, |value| value.snapshot());
    let disk_before = directory_bytes(path);
    let rss_before = process_resident_bytes();
    let cpu_before = process_cpu_nanos();
    begin_profile();
    let started = Instant::now();
    let mut acquire_metrics = Metrics::default();
    let cursors = acquire_all(storage, &fixture, BENCH_LEASE_SPAN, &mut acquire_metrics).await?;
    let acquire_wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;

    for (reader, (old, current)) in fixture
        .reader_roots
        .iter()
        .zip(&fixture.current_roots)
        .enumerate()
    {
        publish_branch(storage, reader as u64, *old, *current, &mut setup).await?;
    }

    let renew_started = Instant::now();
    let mut renew_metrics = Metrics::default();
    let mut renewed = Vec::with_capacity(readers);
    for reader in 0..readers {
        let prepared =
            prepare_renew(storage, reader as u64, BENCH_LEASE_SPAN, &mut renew_metrics).await?;
        commit_lease_mutation(storage, &prepared, &mut renew_metrics).await?;
        renewed.push(Cursor {
            lease_id: reader as u64,
            lease_generation: prepared.lease.generation,
            root: prepared.lease.root,
            view_id: prepared.lease.view_id,
            resume_after: 0,
        });
    }
    let renew_wall_us = renew_started.elapsed().as_secs_f64() * 1_000_000.0;
    assert!(
        cursors
            .iter()
            .zip(&renewed)
            .all(|(old, new)| old.root == new.root && old.lease_generation != new.lease_generation)
    );

    let gc_started = Instant::now();
    let mut gc_metrics = Metrics::default();
    let progress = collect_gc(storage, &mut gc_metrics).await?;
    let gc_wall_us = gc_started.elapsed().as_secs_f64() * 1_000_000.0;
    assert_eq!(progress.live_lease_count, readers as u64);
    assert!(progress.minimum_live_generation > 0);
    for root in &fixture.reader_roots {
        assert!(object_present(storage, *root, &mut setup).await?);
    }
    for root in &fixture.orphan_roots {
        assert!(!object_present(storage, *root, &mut setup).await?);
    }
    for cursor in renewed {
        resume_cursor(storage, &cursor.encode(), &mut setup).await?;
    }
    let (allocated, allocation_calls) = end_profile();
    let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
    let rss_after = process_resident_bytes();
    let disk_after = directory_bytes(path);
    let physical = counters.map_or_else(SlateDBIoSnapshot::default, |value| {
        value.snapshot().saturating_sub(physical_before)
    });
    let mut total = acquire_metrics.clone();
    total += renew_metrics.clone();
    total += gc_metrics.clone();
    println!(
        "forktree_reader_lease_gc,backend={backend},readers={readers},roots={},reachable={},orphans={},lease_span_epochs={BENCH_LEASE_SPAN},acquire_wall_us={acquire_wall_us:.3},acquire_us_per_reader={:.3},renew_wall_us={renew_wall_us:.3},renew_us_per_reader={:.3},gc_wall_us={gc_wall_us:.3},cpu_us_total={cpu_us:.3},alloc_bytes={allocated},alloc_calls={allocation_calls},rss_before={rss_before},rss_after={rss_after},get_calls={},get_keys={},get_bytes={},scan_calls={},scan_rows={},scan_bytes={},commits={},puts={},deletes={},write_bytes={},marked={},swept={},peak_queue={},disk_before={disk_before},disk_after={disk_after},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},slate_list_operations={},slate_deleted_objects={}",
        readers * 2 + fixture.static_roots.len(),
        (readers * 2 + fixture.static_roots.len()) * GRAPH_DEPTH,
        fixture.orphan_roots.len() * GRAPH_DEPTH,
        acquire_wall_us / readers as f64,
        renew_wall_us / readers as f64,
        total.get_calls,
        total.get_keys,
        total.get_bytes,
        total.scan_calls,
        total.scan_rows,
        total.scan_bytes,
        total.commits,
        total.puts,
        total.deletes,
        total.write_bytes,
        gc_metrics.marked,
        gc_metrics.swept,
        gc_metrics.peak_queue,
        physical.read_objects,
        physical.read_bytes,
        physical.write_objects,
        physical.write_bytes,
        physical.list_operations,
        physical.deleted_objects,
    );
    Ok(())
}

async fn run_rocks(readers: usize) -> Result<(), StorageError> {
    let oracle_dir = tempfile::tempdir().expect("create RocksDB oracle directory");
    let oracle_path = oracle_dir.path().join("oracle");
    let oracle = RocksDB::open(&oracle_path).expect("open RocksDB oracle");
    run_oracle(&oracle).await?;
    oracle.flush()?;
    drop(oracle);
    let reopened = RocksDB::open(&oracle_path).expect("reopen RocksDB oracle");
    let mut verify = Metrics::default();
    for label in 1..=6u8 {
        let keys = [static_root_key(label)];
        assert!(
            get_values(
                &reopened,
                &[GetManyRequest {
                    space: META_SPACE,
                    keys: &keys,
                    opts: GetOptions {
                        projection: CoreProjection::FullValue,
                    },
                }],
                &mut verify,
            )
            .await?[0]
                .is_some()
        );
    }
    drop(reopened);

    let reopen_dir = tempfile::tempdir().expect("create RocksDB lease-reopen directory");
    let reopen_path = reopen_dir.path().join("lease-reopen");
    let before_reopen = RocksDB::open(&reopen_path).expect("open RocksDB lease-reopen oracle");
    let reopen_fixture = prepare_reopen_oracle(&before_reopen).await?;
    before_reopen.flush()?;
    drop(before_reopen);
    let after_reopen = RocksDB::open(&reopen_path).expect("reopen RocksDB lease oracle");
    verify_reopen_oracle(&after_reopen, &reopen_fixture).await?;
    after_reopen.flush()?;

    let corrupt_dir = tempfile::tempdir().expect("create RocksDB corruption directory");
    let corrupt = RocksDB::open(corrupt_dir.path()).expect("open RocksDB corruption oracle");
    run_corruption_oracle(&corrupt).await?;

    let directory = tempfile::tempdir().expect("create RocksDB benchmark directory");
    let path = directory.path().join("bench");
    let storage = RocksDB::open(&path).expect("open RocksDB benchmark");
    measure("rocksdb", &storage, &path, None, readers).await?;
    storage.flush()?;
    println!(
        "forktree_reader_lease_gc_settled,backend=rocksdb,readers={readers},disk_bytes={}",
        directory_bytes(&path)
    );
    Ok(())
}

async fn run_slate(readers: usize) -> Result<(), StorageError> {
    let oracle_dir = tempfile::tempdir().expect("create SlateDB oracle directory");
    let oracle_path = oracle_dir.path().join("oracle");
    let oracle = SlateDB::open(&oracle_path).expect("open SlateDB oracle");
    run_oracle(&oracle).await?;
    oracle.flush().await?;
    drop(oracle);
    let reopened = SlateDB::open(&oracle_path).expect("reopen SlateDB oracle");
    let mut verify = Metrics::default();
    for label in 1..=6u8 {
        let keys = [static_root_key(label)];
        assert!(
            get_values(
                &reopened,
                &[GetManyRequest {
                    space: META_SPACE,
                    keys: &keys,
                    opts: GetOptions {
                        projection: CoreProjection::FullValue,
                    },
                }],
                &mut verify,
            )
            .await?[0]
                .is_some()
        );
    }
    reopened.flush().await?;

    let reopen_dir = tempfile::tempdir().expect("create SlateDB lease-reopen directory");
    let reopen_path = reopen_dir.path().join("lease-reopen");
    let before_reopen = SlateDB::open(&reopen_path).expect("open SlateDB lease-reopen oracle");
    let reopen_fixture = prepare_reopen_oracle(&before_reopen).await?;
    before_reopen.flush().await?;
    drop(before_reopen);
    let after_reopen = SlateDB::open(&reopen_path).expect("reopen SlateDB lease oracle");
    verify_reopen_oracle(&after_reopen, &reopen_fixture).await?;
    after_reopen.flush().await?;

    let corrupt_dir = tempfile::tempdir().expect("create SlateDB corruption directory");
    let corrupt = SlateDB::open(corrupt_dir.path()).expect("open SlateDB corruption oracle");
    run_corruption_oracle(&corrupt).await?;

    let directory = tempfile::tempdir().expect("create SlateDB benchmark directory");
    let path = directory.path().join("bench");
    let counters = SlateDBIoCounters::default();
    let storage =
        SlateDB::open_with_io_counters(&path, counters.clone()).expect("open SlateDB benchmark");
    measure("slatedb", &storage, &path, Some(&counters), readers).await?;
    storage.flush().await?;
    let settled = counters.snapshot();
    println!(
        "forktree_reader_lease_gc_settled,backend=slatedb,readers={readers},disk_bytes={},read_objects={},read_bytes={},write_objects={},write_bytes={}",
        directory_bytes(&path),
        settled.read_objects,
        settled.read_bytes,
        settled.write_objects,
        settled.write_bytes,
    );
    Ok(())
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let backend = args.get(1).map(String::as_str).unwrap_or("rocksdb");
    let readers = args
        .get(2)
        .map_or(1, |value| value.parse::<usize>().expect("reader count"));
    assert!(matches!(readers, 1 | 10 | 100));
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("create reader-lease benchmark runtime")
        .block_on(async {
            let result = match backend {
                "rocksdb" => run_rocks(readers).await,
                "slatedb" => run_slate(readers).await,
                other => panic!("unknown backend '{other}'"),
            };
            result.expect("reader-lease safe-point qualification");
        });
}
