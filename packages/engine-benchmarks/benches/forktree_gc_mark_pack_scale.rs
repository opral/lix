//! Test/model-only bounded-memory mark-pack/sweep scaling oracle.
//! The only durable authorities are semantic roots and one raw GC fence.

use std::alloc::GlobalAlloc;
use std::collections::{BTreeMap, BTreeSet};
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
static ALLOCATED: AtomicU64 = AtomicU64::new(0);
static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static PROFILE: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && PROFILE.load(Ordering::Relaxed) {
            ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
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
        if !replacement.is_null() && new_size >= layout.size() && PROFILE.load(Ordering::Relaxed) {
            ALLOCATED.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        replacement
    }
}

const META: lix::storage::StorageSpace = synthetic_space_for_bench(81, ValueSemantics::Mutable);
const OBJECTS: lix::storage::StorageSpace =
    synthetic_space_for_bench(82, ValueSemantics::Immutable);
const WORK: lix::storage::StorageSpace = synthetic_space_for_bench(83, ValueSemantics::Mutable);
const MARKS: lix::storage::StorageSpace = synthetic_space_for_bench(84, ValueSemantics::Mutable);
const PROGRESS_KEY: &[u8] = b"gc/progress";
const ROOT_PREFIX: &[u8] = b"root/";
const SEED_BATCH: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum Shape {
    OltpRow = 1,
    Version = 2,
    Branch = 3,
    Checkpoint = 4,
    Upload = 5,
    MediaManifest = 6,
    MediaChunk = 7,
}

impl Shape {
    fn for_id(id: u64) -> Self {
        match id % 16 {
            0..=5 => Self::OltpRow,
            6..=9 => Self::Version,
            10 => Self::Branch,
            11 => Self::Checkpoint,
            12 => Self::Upload,
            13 => Self::MediaManifest,
            _ => Self::MediaChunk,
        }
    }

    fn decode(byte: u8) -> Result<Self, StorageError> {
        match byte {
            1 => Ok(Self::OltpRow),
            2 => Ok(Self::Version),
            3 => Ok(Self::Branch),
            4 => Ok(Self::Checkpoint),
            5 => Ok(Self::Upload),
            6 => Ok(Self::MediaManifest),
            7 => Ok(Self::MediaChunk),
            _ => Err(corruption("unknown object shape")),
        }
    }

    fn payload_bytes(self) -> usize {
        match self {
            Self::MediaChunk => 1024,
            Self::MediaManifest => 256,
            _ => 64,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum RootRole {
    Branch = 1,
    Checkpoint = 2,
    Undo = 3,
    Redo = 4,
    Upload = 5,
    Multimedia = 6,
}

impl RootRole {
    fn for_index(index: usize) -> Self {
        match index % 6 {
            0 => Self::Branch,
            1 => Self::Checkpoint,
            2 => Self::Undo,
            3 => Self::Redo,
            4 => Self::Upload,
            _ => Self::Multimedia,
        }
    }

    fn decode(byte: u8) -> Result<Self, StorageError> {
        match byte {
            1 => Ok(Self::Branch),
            2 => Ok(Self::Checkpoint),
            3 => Ok(Self::Undo),
            4 => Ok(Self::Redo),
            5 => Ok(Self::Upload),
            6 => Ok(Self::Multimedia),
            _ => Err(corruption("unknown root role")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum Phase {
    Roots = 1,
    Mark = 2,
    Sweep = 3,
    Cleanup = 4,
}

impl Phase {
    fn decode(byte: u8) -> Result<Self, StorageError> {
        match byte {
            1 => Ok(Self::Roots),
            2 => Ok(Self::Mark),
            3 => Ok(Self::Sweep),
            4 => Ok(Self::Cleanup),
            _ => Err(corruption("unknown GC phase")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Progress {
    cycle: u64,
    phase: Phase,
    root_after: Option<Vec<u8>>,
    sweep_after: Option<u64>,
    total_objects: u64,
    marked: u64,
    swept: u64,
}

impl Progress {
    fn encode(&self) -> Bytes {
        let root = self.root_after.as_deref().unwrap_or_default();
        let mut body = Vec::with_capacity(80 + root.len());
        body.extend_from_slice(b"MPG1");
        body.extend_from_slice(&self.cycle.to_be_bytes());
        body.push(self.phase as u8);
        body.extend_from_slice(&(root.len() as u16).to_be_bytes());
        body.extend_from_slice(root);
        body.push(u8::from(self.sweep_after.is_some()));
        body.extend_from_slice(&self.sweep_after.unwrap_or_default().to_be_bytes());
        body.extend_from_slice(&self.total_objects.to_be_bytes());
        body.extend_from_slice(&self.marked.to_be_bytes());
        body.extend_from_slice(&self.swept.to_be_bytes());
        authenticate(body)
    }

    fn decode(raw: &Bytes) -> Result<Self, StorageError> {
        verify(raw)?;
        if raw.len() < 4 + 8 + 1 + 2 + 1 + 8 * 4 + 32 || &raw[..4] != b"MPG1" {
            return Err(corruption("GC progress is malformed"));
        }
        let root_len = u16::from_be_bytes(
            raw[13..15]
                .try_into()
                .map_err(|_| corruption("root cursor length is malformed"))?,
        ) as usize;
        let cursor_end = 15 + root_len;
        if cursor_end + 1 + 32 + 32 != raw.len() {
            return Err(corruption("GC progress lengths are inconsistent"));
        }
        let has_sweep = raw[cursor_end];
        if has_sweep > 1 {
            return Err(corruption("sweep cursor presence is malformed"));
        }
        Ok(Self {
            cycle: read_u64(raw, 4)?,
            phase: Phase::decode(raw[12])?,
            root_after: (root_len > 0).then(|| raw[15..cursor_end].to_vec()),
            sweep_after: (has_sweep == 1)
                .then(|| read_u64(raw, cursor_end + 1))
                .transpose()?,
            total_objects: read_u64(raw, cursor_end + 9)?,
            marked: read_u64(raw, cursor_end + 17)?,
            swept: read_u64(raw, cursor_end + 25)?,
        })
    }
}

#[derive(Clone, Debug, Default)]
struct Metrics {
    gets: u64,
    get_keys: u64,
    get_bytes: u64,
    scans: u64,
    scan_rows: u64,
    scan_bytes: u64,
    commits: u64,
    puts: u64,
    deletes: u64,
    write_bytes: u64,
    steps: u64,
    peak_object_page: usize,
    peak_root_page: usize,
    peak_pack_count: usize,
    peak_pack_bytes: usize,
}

impl std::ops::AddAssign for Metrics {
    fn add_assign(&mut self, rhs: Self) {
        self.gets += rhs.gets;
        self.get_keys += rhs.get_keys;
        self.get_bytes += rhs.get_bytes;
        self.scans += rhs.scans;
        self.scan_rows += rhs.scan_rows;
        self.scan_bytes += rhs.scan_bytes;
        self.commits += rhs.commits;
        self.puts += rhs.puts;
        self.deletes += rhs.deletes;
        self.write_bytes += rhs.write_bytes;
        self.steps += rhs.steps;
        self.peak_object_page = self.peak_object_page.max(rhs.peak_object_page);
        self.peak_root_page = self.peak_root_page.max(rhs.peak_root_page);
        self.peak_pack_count = self.peak_pack_count.max(rhs.peak_pack_count);
        self.peak_pack_bytes = self.peak_pack_bytes.max(rhs.peak_pack_bytes);
    }
}

fn corruption(message: impl Into<String>) -> StorageError {
    StorageError::Corruption(message.into())
}

fn authenticate(mut body: Vec<u8>) -> Bytes {
    body.extend_from_slice(blake3::hash(&body).as_bytes());
    Bytes::from(body)
}

fn verify(raw: &Bytes) -> Result<(), StorageError> {
    if raw.len() < 32 {
        return Err(corruption("authenticated value is truncated"));
    }
    let split = raw.len() - 32;
    if blake3::hash(&raw[..split]).as_bytes() != &raw[split..] {
        return Err(corruption("authenticated checksum mismatch"));
    }
    Ok(())
}

fn read_u64(raw: &[u8], offset: usize) -> Result<u64, StorageError> {
    raw.get(offset..offset + 8)
        .ok_or_else(|| corruption("u64 is truncated"))?
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| corruption("u64 is malformed"))
}

fn key(raw: impl Into<Bytes>) -> Key {
    Key(raw.into())
}

fn id_key(id: u64) -> Key {
    key(Bytes::copy_from_slice(&id.to_be_bytes()))
}

fn root_key(index: usize, role: RootRole) -> Key {
    let mut raw = ROOT_PREFIX.to_vec();
    raw.push(role as u8);
    raw.extend_from_slice(&(index as u64).to_be_bytes());
    key(Bytes::from(raw))
}

fn mark_key(pack: u64) -> Key {
    id_key(pack)
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

fn object_bytes(id: u64, shape: Shape, child: Option<u64>) -> Bytes {
    let payload_len = shape.payload_bytes();
    let mut body = Vec::with_capacity(32 + payload_len);
    body.extend_from_slice(b"MPO1");
    body.extend_from_slice(&id.to_be_bytes());
    body.push(shape as u8);
    body.push(u8::from(child.is_some()));
    body.extend_from_slice(&child.unwrap_or_default().to_be_bytes());
    body.extend_from_slice(&(payload_len as u32).to_be_bytes());
    body.extend(std::iter::repeat_n((id as u8) ^ shape as u8, payload_len));
    authenticate(body)
}

fn decode_object(id: u64, raw: &Bytes) -> Result<Option<u64>, StorageError> {
    verify(raw)?;
    if raw.len() < 4 + 8 + 1 + 1 + 8 + 4 + 32 || &raw[..4] != b"MPO1" {
        return Err(corruption("object is malformed"));
    }
    if read_u64(raw, 4)? != id {
        return Err(corruption("object key identity mismatch"));
    }
    Shape::decode(raw[12])?;
    if raw[13] > 1 {
        return Err(corruption("object child cardinality is malformed"));
    }
    let payload_len = u32::from_be_bytes(
        raw[22..26]
            .try_into()
            .map_err(|_| corruption("payload length is malformed"))?,
    ) as usize;
    if 26 + payload_len + 32 != raw.len() {
        return Err(corruption("object payload length is inconsistent"));
    }
    Ok((raw[13] == 1).then(|| read_u64(raw, 14)).transpose()?)
}

fn selector_bytes(role: RootRole, root: u64) -> Bytes {
    let mut body = Vec::with_capacity(24);
    body.extend_from_slice(b"MPR1");
    body.push(role as u8);
    body.extend_from_slice(&root.to_be_bytes());
    authenticate(body)
}

fn decode_selector(entry_key: &Key, raw: &Bytes) -> Result<u64, StorageError> {
    verify(raw)?;
    if raw.len() != 4 + 1 + 8 + 32 || &raw[..4] != b"MPR1" {
        return Err(corruption("root selector is malformed"));
    }
    let role = RootRole::decode(raw[4])?;
    if !entry_key.0.starts_with(ROOT_PREFIX)
        || entry_key.0.get(ROOT_PREFIX.len()) != Some(&(role as u8))
    {
        return Err(corruption("root selector key/role mismatch"));
    }
    Ok(read_u64(raw, 5)?)
}

fn work_bytes(id: u64) -> Bytes {
    let mut body = b"MPW1".to_vec();
    body.extend_from_slice(&id.to_be_bytes());
    authenticate(body)
}

fn decode_work(entry_key: &Key, raw: &Bytes) -> Result<u64, StorageError> {
    verify(raw)?;
    if raw.len() != 4 + 8 + 32 || &raw[..4] != b"MPW1" {
        return Err(corruption("work row is malformed"));
    }
    let id = read_u64(raw, 4)?;
    if entry_key != &id_key(id) {
        return Err(corruption("work key identity mismatch"));
    }
    Ok(id)
}

fn empty_pack(pack: u64, pack_bits: usize) -> Bytes {
    encode_pack(pack, &vec![0; pack_bits.div_ceil(8)])
}

fn encode_pack(pack: u64, bits: &[u8]) -> Bytes {
    let mut body = Vec::with_capacity(24 + bits.len());
    body.extend_from_slice(b"MPM1");
    body.extend_from_slice(&pack.to_be_bytes());
    body.extend_from_slice(&(bits.len() as u32).to_be_bytes());
    body.extend_from_slice(bits);
    authenticate(body)
}

fn decode_pack(pack: u64, raw: &Bytes, pack_bits: usize) -> Result<Vec<u8>, StorageError> {
    verify(raw)?;
    if raw.len() < 4 + 8 + 4 + 32 || &raw[..4] != b"MPM1" || read_u64(raw, 4)? != pack {
        return Err(corruption("mark pack is malformed"));
    }
    let len = u32::from_be_bytes(
        raw[12..16]
            .try_into()
            .map_err(|_| corruption("mark pack length is malformed"))?,
    ) as usize;
    if len != pack_bits.div_ceil(8) || 16 + len + 32 != raw.len() {
        return Err(corruption("mark pack size is inconsistent"));
    }
    Ok(raw[16..16 + len].to_vec())
}

fn bit_location(id: u64, pack_bits: usize) -> (u64, usize, u8) {
    let pack = id / pack_bits as u64;
    let within = (id % pack_bits as u64) as usize;
    (pack, within / 8, 1 << (within % 8))
}

fn bit_is_set(bits: &[u8], id: u64, pack_bits: usize) -> bool {
    let (_, byte, mask) = bit_location(id, pack_bits);
    bits[byte] & mask != 0
}

fn set_bit(bits: &mut [u8], id: u64, pack_bits: usize) -> bool {
    let (_, byte, mask) = bit_location(id, pack_bits);
    let was_set = bits[byte] & mask != 0;
    bits[byte] |= mask;
    !was_set
}

fn full_value(value: Option<ProjectedValue>) -> Result<Option<Bytes>, StorageError> {
    match value {
        None => Ok(None),
        Some(ProjectedValue::FullValue(raw)) => Ok(Some(raw)),
        Some(ProjectedValue::KeyOnly) => Err(corruption("unexpected key-only value")),
    }
}

async fn get_from<R: StorageRead>(
    read: &R,
    space: lix::storage::StorageSpace,
    keys: &[Key],
    metrics: &mut Metrics,
) -> Result<Vec<Option<Bytes>>, StorageError> {
    metrics.gets += 1;
    metrics.get_keys += keys.len() as u64;
    let result = read
        .get_many(&[GetManyRequest {
            space,
            keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    result
        .values
        .into_iter()
        .map(|value| {
            let value = full_value(value)?;
            metrics.get_bytes += value.as_ref().map_or(0, |raw| raw.len() as u64);
            Ok(value)
        })
        .collect()
}

async fn get<S: Storage>(
    storage: &S,
    space: lix::storage::StorageSpace,
    keys: &[Key],
    metrics: &mut Metrics,
) -> Result<Vec<Option<Bytes>>, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    get_from(&read, space, keys, metrics).await
}

async fn scan_page<R: StorageRead>(
    read: &R,
    space: lix::storage::StorageSpace,
    range: KeyRange,
    resume_after: Option<Key>,
    limit: usize,
    metrics: &mut Metrics,
) -> Result<(Vec<(Key, Bytes)>, bool), StorageError> {
    metrics.scans += 1;
    let page = read
        .scan(
            space,
            range,
            ScanOptions {
                projection: CoreProjection::FullValue,
                limit_rows: limit,
                resume_after,
            },
        )
        .await?;
    if page.has_more && page.entries.is_empty() {
        return Err(corruption("empty scan page claims continuation"));
    }
    let mut rows = Vec::with_capacity(page.entries.len());
    for entry in page.entries {
        let raw =
            full_value(Some(entry.value))?.ok_or_else(|| corruption("scan value is absent"))?;
        metrics.scan_rows += 1;
        metrics.scan_bytes += (entry.key.0.len() + raw.len()) as u64;
        rows.push((entry.key, raw));
    }
    Ok((rows, page.has_more))
}

async fn write_batches<S: Storage>(
    storage: &S,
    options: WriteOptions,
    puts: Vec<(lix::storage::StorageSpace, PutBatch)>,
    deletes: Vec<(lix::storage::StorageSpace, Vec<Key>)>,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let mut write = storage.begin_write(options).await?;
    for (space, batch) in puts {
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
        metrics.write_bytes += keys.iter().map(|entry| entry.0.len() as u64).sum::<u64>();
        write.delete_many(space, &keys).await?;
    }
    write.commit().await?;
    metrics.commits += 1;
    Ok(())
}

fn root_starts(live: u64) -> Vec<(RootRole, u64)> {
    let count = live.clamp(1, 1_024) as usize;
    (0..count)
        .map(|index| {
            let start = (index as u64 * live) / count as u64;
            (RootRole::for_index(index), start)
        })
        .collect()
}

fn child_for(id: u64, total: u64, live: u64, roots: &[(RootRole, u64)]) -> Option<u64> {
    if id + 1 >= total {
        return None;
    }
    if id < live {
        if id + 1 == live || roots.iter().any(|(_, root)| *root == id + 1) {
            None
        } else {
            Some(id + 1)
        }
    } else {
        Some(id + 1)
    }
}

async fn seed_fixture<S: Storage>(
    storage: &S,
    total: u64,
    live_percent: u64,
    metrics: &mut Metrics,
) -> Result<u64, StorageError> {
    let live = ((total * live_percent) / 100).clamp(1, total);
    let roots = root_starts(live);
    let mut next = 0u64;
    while next < total {
        let end = (next + SEED_BATCH as u64).min(total);
        let entries = (next..end)
            .map(|id| PutEntry {
                key: id_key(id),
                value: StoredValue {
                    bytes: object_bytes(id, Shape::for_id(id), child_for(id, total, live, &roots)),
                },
            })
            .collect();
        write_batches(
            storage,
            WriteOptions::default(),
            vec![(OBJECTS, PutBatch { entries })],
            Vec::new(),
            metrics,
        )
        .await?;
        next = end;
    }
    let selectors = roots
        .iter()
        .enumerate()
        .map(|(index, (role, root))| PutEntry {
            key: root_key(index, *role),
            value: StoredValue {
                bytes: selector_bytes(*role, *root),
            },
        })
        .collect();
    write_batches(
        storage,
        WriteOptions::default(),
        vec![(META, PutBatch { entries: selectors })],
        Vec::new(),
        metrics,
    )
    .await?;
    Ok(live)
}

async fn load_progress<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<Option<(Bytes, Progress)>, StorageError> {
    let values = get(
        storage,
        META,
        &[key(Bytes::from_static(PROGRESS_KEY))],
        metrics,
    )
    .await?;
    values[0]
        .clone()
        .map(|raw| Progress::decode(&raw).map(|progress| (raw, progress)))
        .transpose()
}

async fn start_gc<S: Storage>(
    storage: &S,
    total: u64,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let progress = Progress {
        cycle: 1,
        phase: Phase::Roots,
        root_after: None,
        sweep_after: None,
        total_objects: total,
        marked: 0,
        swept: 0,
    };
    write_batches(
        storage,
        WriteOptions {
            preconditions: vec![Precondition::KeyAbsent {
                space: META,
                key: key(Bytes::from_static(PROGRESS_KEY)),
            }],
            ..WriteOptions::default()
        },
        vec![(
            META,
            PutBatch {
                entries: vec![PutEntry {
                    key: key(Bytes::from_static(PROGRESS_KEY)),
                    value: StoredValue {
                        bytes: progress.encode(),
                    },
                }],
            },
        )],
        Vec::new(),
        metrics,
    )
    .await
}

async fn load_packs<S: Storage>(
    storage: &S,
    pack_ids: &[u64],
    pack_bits: usize,
    metrics: &mut Metrics,
) -> Result<BTreeMap<u64, Vec<u8>>, StorageError> {
    let keys = pack_ids.iter().copied().map(mark_key).collect::<Vec<_>>();
    let values = get(storage, MARKS, &keys, metrics).await?;
    let mut packs = BTreeMap::new();
    for (pack, raw) in pack_ids.iter().copied().zip(values) {
        packs.insert(
            pack,
            match raw {
                Some(raw) => decode_pack(pack, &raw, pack_bits)?,
                None => decode_pack(pack, &empty_pack(pack, pack_bits), pack_bits)?,
            },
        );
    }
    metrics.peak_pack_count = metrics.peak_pack_count.max(packs.len());
    metrics.peak_pack_bytes = metrics
        .peak_pack_bytes
        .max(packs.values().map(Vec::len).sum());
    Ok(packs)
}

struct MarkUpdate {
    pack_entries: Vec<PutEntry>,
    work_entries: Vec<PutEntry>,
    newly_marked: u64,
}

async fn mark_ids<S: Storage>(
    storage: &S,
    ids: impl IntoIterator<Item = u64>,
    pack_bits: usize,
    metrics: &mut Metrics,
) -> Result<MarkUpdate, StorageError> {
    let ids = ids.into_iter().collect::<BTreeSet<_>>();
    let pack_ids = ids
        .iter()
        .map(|id| bit_location(*id, pack_bits).0)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut packs = load_packs(storage, &pack_ids, pack_bits, metrics).await?;
    let mut changed = BTreeSet::new();
    let mut work_entries = Vec::new();
    for id in ids {
        let pack = bit_location(id, pack_bits).0;
        if set_bit(
            packs.get_mut(&pack).expect("loaded mark pack"),
            id,
            pack_bits,
        ) {
            changed.insert(pack);
            work_entries.push(PutEntry {
                key: id_key(id),
                value: StoredValue {
                    bytes: work_bytes(id),
                },
            });
        }
    }
    let pack_entries = changed
        .into_iter()
        .map(|pack| PutEntry {
            key: mark_key(pack),
            value: StoredValue {
                bytes: encode_pack(pack, packs.get(&pack).expect("changed mark pack")),
            },
        })
        .collect();
    Ok(MarkUpdate {
        newly_marked: work_entries.len() as u64,
        pack_entries,
        work_entries,
    })
}

fn progress_precondition(raw: &Bytes) -> Precondition {
    Precondition::KeyValueEquals {
        space: META,
        key: key(Bytes::from_static(PROGRESS_KEY)),
        expected: raw.clone(),
    }
}

async fn commit_step<S: Storage>(
    storage: &S,
    raw_progress: &Bytes,
    progress: Option<&Progress>,
    mut puts: Vec<(lix::storage::StorageSpace, PutBatch)>,
    mut deletes: Vec<(lix::storage::StorageSpace, Vec<Key>)>,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    match progress {
        Some(progress) => puts.push((
            META,
            PutBatch {
                entries: vec![PutEntry {
                    key: key(Bytes::from_static(PROGRESS_KEY)),
                    value: StoredValue {
                        bytes: progress.encode(),
                    },
                }],
            },
        )),
        None => deletes.push((META, vec![key(Bytes::from_static(PROGRESS_KEY))])),
    }
    write_batches(
        storage,
        WriteOptions {
            preconditions: vec![progress_precondition(raw_progress)],
            ..WriteOptions::default()
        },
        puts,
        deletes,
        metrics,
    )
    .await?;
    metrics.steps += 1;
    Ok(())
}

fn push_puts(
    puts: &mut Vec<(lix::storage::StorageSpace, PutBatch)>,
    space: lix::storage::StorageSpace,
    entries: Vec<PutEntry>,
) {
    if !entries.is_empty() {
        puts.push((space, PutBatch { entries }));
    }
}

async fn gc_step<S: Storage>(
    storage: &S,
    page_size: usize,
    pack_bits: usize,
    metrics: &mut Metrics,
) -> Result<bool, StorageError> {
    let Some((raw_progress, mut progress)) = load_progress(storage, metrics).await? else {
        return Ok(true);
    };
    match progress.phase {
        Phase::Roots => {
            let read = storage.begin_read(ReadOptions::default()).await?;
            let resume = progress
                .root_after
                .as_ref()
                .map(|raw| key(Bytes::copy_from_slice(raw)));
            let (rows, has_more) = scan_page(
                &read,
                META,
                prefix_range(ROOT_PREFIX),
                resume,
                page_size,
                metrics,
            )
            .await?;
            drop(read);
            metrics.peak_root_page = metrics.peak_root_page.max(rows.len());
            let roots = rows
                .iter()
                .map(|(entry_key, raw)| decode_selector(entry_key, raw))
                .collect::<Result<Vec<_>, _>>()?;
            let update = mark_ids(storage, roots, pack_bits, metrics).await?;
            progress.marked += update.newly_marked;
            progress.root_after = rows.last().map(|(entry_key, _)| entry_key.0.to_vec());
            if !has_more {
                progress.phase = Phase::Mark;
                progress.root_after = None;
            }
            let mut puts = Vec::new();
            push_puts(&mut puts, MARKS, update.pack_entries);
            push_puts(&mut puts, WORK, update.work_entries);
            commit_step(
                storage,
                &raw_progress,
                Some(&progress),
                puts,
                Vec::new(),
                metrics,
            )
            .await?;
        }
        Phase::Mark => {
            let read = storage.begin_read(ReadOptions::default()).await?;
            let (rows, _) = scan_page(
                &read,
                WORK,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                None,
                page_size,
                metrics,
            )
            .await?;
            metrics.peak_object_page = metrics.peak_object_page.max(rows.len());
            if rows.is_empty() {
                drop(read);
                progress.phase = Phase::Sweep;
                commit_step(
                    storage,
                    &raw_progress,
                    Some(&progress),
                    Vec::new(),
                    Vec::new(),
                    metrics,
                )
                .await?;
            } else {
                let ids = rows
                    .iter()
                    .map(|(entry_key, raw)| decode_work(entry_key, raw))
                    .collect::<Result<Vec<_>, _>>()?;
                let object_keys = ids.iter().copied().map(id_key).collect::<Vec<_>>();
                let objects = get_from(&read, OBJECTS, &object_keys, metrics).await?;
                drop(read);
                let mut children = Vec::new();
                for (id, raw) in ids.iter().copied().zip(objects) {
                    let raw = raw.ok_or_else(|| corruption("marked object is missing"))?;
                    if let Some(child) = decode_object(id, &raw)? {
                        children.push(child);
                    }
                }
                let update = mark_ids(storage, children, pack_bits, metrics).await?;
                progress.marked += update.newly_marked;
                let mut puts = Vec::new();
                push_puts(&mut puts, MARKS, update.pack_entries);
                push_puts(&mut puts, WORK, update.work_entries);
                commit_step(
                    storage,
                    &raw_progress,
                    Some(&progress),
                    puts,
                    vec![(
                        WORK,
                        rows.into_iter().map(|(entry_key, _)| entry_key).collect(),
                    )],
                    metrics,
                )
                .await?;
            }
        }
        Phase::Sweep => {
            let read = storage.begin_read(ReadOptions::default()).await?;
            let resume = progress.sweep_after.map(id_key);
            let (rows, has_more) = scan_page(
                &read,
                OBJECTS,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                resume,
                page_size,
                metrics,
            )
            .await?;
            drop(read);
            metrics.peak_object_page = metrics.peak_object_page.max(rows.len());
            let mut ids = Vec::with_capacity(rows.len());
            for (entry_key, raw) in &rows {
                let id = read_u64(&entry_key.0, 0)?;
                decode_object(id, raw)?;
                ids.push(id);
            }
            let pack_ids = ids
                .iter()
                .map(|id| bit_location(*id, pack_bits).0)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let packs = load_packs(storage, &pack_ids, pack_bits, metrics).await?;
            let deletes = ids
                .iter()
                .zip(&rows)
                .filter_map(|(id, (entry_key, _))| {
                    let pack = bit_location(*id, pack_bits).0;
                    (!bit_is_set(packs.get(&pack).expect("loaded sweep pack"), *id, pack_bits))
                        .then_some(entry_key.clone())
                })
                .collect::<Vec<_>>();
            progress.swept += deletes.len() as u64;
            progress.sweep_after = ids.last().copied();
            if !has_more {
                progress.phase = Phase::Cleanup;
                progress.sweep_after = None;
            }
            let delete_batches = if deletes.is_empty() {
                Vec::new()
            } else {
                vec![(OBJECTS, deletes)]
            };
            commit_step(
                storage,
                &raw_progress,
                Some(&progress),
                Vec::new(),
                delete_batches,
                metrics,
            )
            .await?;
        }
        Phase::Cleanup => {
            let read = storage.begin_read(ReadOptions::default()).await?;
            let (rows, _) = scan_page(
                &read,
                MARKS,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                None,
                page_size,
                metrics,
            )
            .await?;
            drop(read);
            if rows.is_empty() {
                commit_step(
                    storage,
                    &raw_progress,
                    None,
                    Vec::new(),
                    Vec::new(),
                    metrics,
                )
                .await?;
                return Ok(true);
            }
            commit_step(
                storage,
                &raw_progress,
                Some(&progress),
                Vec::new(),
                vec![(
                    MARKS,
                    rows.into_iter().map(|(entry_key, _)| entry_key).collect(),
                )],
                metrics,
            )
            .await?;
        }
    }
    Ok(false)
}

async fn run_to_completion<S: Storage>(
    storage: &S,
    page_size: usize,
    pack_bits: usize,
    metrics: &mut Metrics,
) -> Result<Progress, StorageError> {
    let mut last = None;
    loop {
        if let Some((_, progress)) = load_progress(storage, metrics).await? {
            last = Some(progress);
        }
        if gc_step(storage, page_size, pack_bits, metrics).await? {
            return last.ok_or_else(|| corruption("GC completed without progress"));
        }
    }
}

async fn count_space<S: Storage>(
    storage: &S,
    space: lix::storage::StorageSpace,
    page_size: usize,
    metrics: &mut Metrics,
) -> Result<u64, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    let mut resume = None;
    let mut total = 0u64;
    loop {
        let (rows, has_more) = scan_page(
            &read,
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            resume,
            page_size,
            metrics,
        )
        .await?;
        total += rows.len() as u64;
        resume = rows.last().map(|(entry_key, _)| entry_key.clone());
        if !has_more {
            return Ok(total);
        }
    }
}

async fn delete_meta<S: Storage>(
    storage: &S,
    entry_key: Key,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    write_batches(
        storage,
        WriteOptions::default(),
        Vec::new(),
        vec![(META, vec![entry_key])],
        metrics,
    )
    .await
}

async fn assert_no_reader_residue<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    for forbidden in [b"lease/".as_slice(), b"reader/", b"cursor/"] {
        let (rows, _) = scan_page(&read, META, prefix_range(forbidden), None, 8, metrics).await?;
        assert!(rows.is_empty());
    }
    Ok(())
}

async fn final_reference_oracle<S: Storage>(
    storage: &S,
    page_size: usize,
    pack_bits: usize,
) -> Result<(), StorageError> {
    let mut metrics = Metrics::default();
    write_batches(
        storage,
        WriteOptions::default(),
        vec![
            (
                OBJECTS,
                PutBatch {
                    entries: vec![PutEntry {
                        key: id_key(0),
                        value: StoredValue {
                            bytes: object_bytes(0, Shape::Checkpoint, None),
                        },
                    }],
                },
            ),
            (
                META,
                PutBatch {
                    entries: vec![
                        PutEntry {
                            key: root_key(0, RootRole::Checkpoint),
                            value: StoredValue {
                                bytes: selector_bytes(RootRole::Checkpoint, 0),
                            },
                        },
                        PutEntry {
                            key: root_key(1, RootRole::Undo),
                            value: StoredValue {
                                bytes: selector_bytes(RootRole::Undo, 0),
                            },
                        },
                    ],
                },
            ),
        ],
        Vec::new(),
        &mut metrics,
    )
    .await?;
    start_gc(storage, 1, &mut metrics).await?;
    let progress = run_to_completion(storage, page_size, pack_bits, &mut metrics).await?;
    assert_eq!(progress.marked, 1);
    assert_eq!(
        count_space(storage, OBJECTS, page_size, &mut metrics).await?,
        1
    );
    delete_meta(storage, root_key(0, RootRole::Checkpoint), &mut metrics).await?;
    start_gc(storage, 1, &mut metrics).await?;
    run_to_completion(storage, page_size, pack_bits, &mut metrics).await?;
    assert_eq!(
        count_space(storage, OBJECTS, page_size, &mut metrics).await?,
        1
    );
    delete_meta(storage, root_key(1, RootRole::Undo), &mut metrics).await?;
    start_gc(storage, 1, &mut metrics).await?;
    let progress = run_to_completion(storage, page_size, pack_bits, &mut metrics).await?;
    assert_eq!(progress.swept, 1);
    assert_eq!(
        count_space(storage, OBJECTS, page_size, &mut metrics).await?,
        0
    );
    assert_no_reader_residue(storage, &mut metrics).await
}

async fn corruption_oracle<S: Storage>(
    storage: &S,
    page_size: usize,
    pack_bits: usize,
) -> Result<(), StorageError> {
    let mut metrics = Metrics::default();
    write_batches(
        storage,
        WriteOptions::default(),
        vec![
            (
                OBJECTS,
                PutBatch {
                    entries: vec![
                        PutEntry {
                            key: id_key(0),
                            value: StoredValue {
                                bytes: object_bytes(0, Shape::Branch, Some(1)),
                            },
                        },
                        PutEntry {
                            key: id_key(1),
                            value: StoredValue {
                                bytes: object_bytes(1, Shape::OltpRow, None),
                            },
                        },
                    ],
                },
            ),
            (
                META,
                PutBatch {
                    entries: vec![PutEntry {
                        key: root_key(0, RootRole::Branch),
                        value: StoredValue {
                            bytes: selector_bytes(RootRole::Branch, 0),
                        },
                    }],
                },
            ),
        ],
        Vec::new(),
        &mut metrics,
    )
    .await?;
    start_gc(storage, 2, &mut metrics).await?;
    assert!(!gc_step(storage, page_size, pack_bits, &mut metrics).await?);
    let raw = get(storage, MARKS, &[mark_key(0)], &mut metrics).await?[0]
        .clone()
        .expect("root mark pack");
    let mut corrupt = raw.to_vec();
    *corrupt.last_mut().expect("mark checksum") ^= 1;
    write_batches(
        storage,
        WriteOptions::default(),
        vec![(
            MARKS,
            PutBatch {
                entries: vec![PutEntry {
                    key: mark_key(0),
                    value: StoredValue {
                        bytes: Bytes::from(corrupt),
                    },
                }],
            },
        )],
        Vec::new(),
        &mut metrics,
    )
    .await?;
    let before = count_space(storage, OBJECTS, page_size, &mut metrics).await?;
    assert!(
        gc_step(storage, page_size, pack_bits, &mut metrics)
            .await
            .is_err()
    );
    assert_eq!(
        count_space(storage, OBJECTS, page_size, &mut metrics).await?,
        before
    );
    Ok(())
}

fn begin_profile() {
    ALLOCATED.store(0, Ordering::Relaxed);
    ALLOC_CALLS.store(0, Ordering::Relaxed);
    PROFILE.store(true, Ordering::Relaxed);
}

fn end_profile() -> (u64, u64) {
    PROFILE.store(false, Ordering::Relaxed);
    (
        ALLOCATED.load(Ordering::Relaxed),
        ALLOC_CALLS.load(Ordering::Relaxed),
    )
}

fn process_rss() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .expect("read statm")
        .split_whitespace()
        .nth(1)
        .expect("resident pages")
        .parse::<u64>()
        .expect("parse resident pages")
        * 4096
}

fn process_cpu_nanos() -> u64 {
    std::fs::read_dir("/proc/self/task")
        .expect("read tasks")
        .flatten()
        .filter_map(|entry| std::fs::read_to_string(entry.path().join("schedstat")).ok())
        .filter_map(|raw| raw.split_whitespace().next()?.parse::<u64>().ok())
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

fn process_peak_rss() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .expect("read process status")
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|value| value.split_whitespace().next())
        .expect("VmHWM is present")
        .parse::<u64>()
        .expect("parse VmHWM")
        * 1024
}

#[derive(Debug)]
struct RunResult {
    backend: &'static str,
    objects: u64,
    live: u64,
    page_size: usize,
    pack_bits: usize,
    setup_wall_us: f64,
    gc_wall_us: f64,
    gc_cpu_us: f64,
    gc_alloc_bytes: u64,
    gc_alloc_calls: u64,
    rss_before: u64,
    rss_after: u64,
    peak_rss: u64,
    disk_before: u64,
    disk_after: u64,
    metrics: Metrics,
    physical: SlateDBIoSnapshot,
}

fn verify_result(
    storage_objects: u64,
    work_rows: u64,
    mark_rows: u64,
    progress_present: bool,
    expected_live: u64,
    result: &RunResult,
) {
    assert_eq!(storage_objects, expected_live, "exact live closure");
    assert_eq!(result.metrics.peak_object_page <= result.page_size, true);
    assert_eq!(result.metrics.peak_root_page <= result.page_size, true);
    assert!(result.metrics.peak_pack_count <= result.page_size.max(1));
    let pack_bytes = result.pack_bits.div_ceil(8);
    assert!(result.metrics.peak_pack_bytes <= result.metrics.peak_pack_count * pack_bytes);
    assert_eq!(work_rows, 0, "work frontier cleaned");
    assert_eq!(mark_rows, 0, "mark packs cleaned");
    assert!(!progress_present, "GC progress cleaned");
}

fn print_result(result: &RunResult) {
    println!(
        concat!(
            "RESULT backend={} objects={} live={} orphan={} live_pct={:.3} page={} pack_bits={} ",
            "setup_wall_us={:.0} gc_wall_us={:.0} gc_cpu_us={:.0} alloc_bytes={} alloc_calls={} ",
            "rss_before={} rss_after={} peak_rss={} disk_before={} disk_after={} ",
            "gets={} get_keys={} get_bytes={} scans={} scan_rows={} scan_bytes={} commits={} ",
            "puts={} deletes={} write_bytes={} steps={} peak_object_page={} peak_root_page={} ",
            "peak_pack_count={} peak_pack_bytes={} slate_read_objects={} slate_read_bytes={} ",
            "slate_write_objects={} slate_write_bytes={} slate_list_ops={} slate_listed_objects={} ",
            "slate_deleted_objects={} oracle_final_reference=green oracle_corruption=green ",
            "reader_lease_residue=absent status=GREEN"
        ),
        result.backend,
        result.objects,
        result.live,
        result.objects - result.live,
        result.live as f64 * 100.0 / result.objects as f64,
        result.page_size,
        result.pack_bits,
        result.setup_wall_us,
        result.gc_wall_us,
        result.gc_cpu_us,
        result.gc_alloc_bytes,
        result.gc_alloc_calls,
        result.rss_before,
        result.rss_after,
        result.peak_rss,
        result.disk_before,
        result.disk_after,
        result.metrics.gets,
        result.metrics.get_keys,
        result.metrics.get_bytes,
        result.metrics.scans,
        result.metrics.scan_rows,
        result.metrics.scan_bytes,
        result.metrics.commits,
        result.metrics.puts,
        result.metrics.deletes,
        result.metrics.write_bytes,
        result.metrics.steps,
        result.metrics.peak_object_page,
        result.metrics.peak_root_page,
        result.metrics.peak_pack_count,
        result.metrics.peak_pack_bytes,
        result.physical.read_objects,
        result.physical.read_bytes,
        result.physical.write_objects,
        result.physical.write_bytes,
        result.physical.list_operations,
        result.physical.listed_objects,
        result.physical.deleted_objects,
    );
}

async fn validate_completed<S: Storage>(
    storage: &S,
    expected_live: u64,
    page_size: usize,
    result: &RunResult,
) -> Result<(), StorageError> {
    let mut validation = Metrics::default();
    let objects = count_space(storage, OBJECTS, page_size, &mut validation).await?;
    let work = count_space(storage, WORK, page_size, &mut validation).await?;
    let marks = count_space(storage, MARKS, page_size, &mut validation).await?;
    let progress = load_progress(storage, &mut validation).await?.is_some();
    assert_no_reader_residue(storage, &mut validation).await?;
    verify_result(objects, work, marks, progress, expected_live, result);
    Ok(())
}

async fn run_rocks(
    objects: u64,
    live_percent: u64,
    page_size: usize,
    pack_bits: usize,
) -> Result<RunResult, StorageError> {
    let directory = tempfile::tempdir().expect("RocksDB scaling directory");
    let path = directory.path().join("scale");
    let final_path = directory.path().join("final-reference");
    let corrupt_path = directory.path().join("corruption");
    let setup_started = Instant::now();
    let live = {
        let storage = RocksDB::open(&path)?;
        let mut setup = Metrics::default();
        let live = seed_fixture(&storage, objects, live_percent, &mut setup).await?;
        storage.flush()?;
        live
    };
    let setup_wall_us = setup_started.elapsed().as_secs_f64() * 1_000_000.0;
    let disk_before = directory_bytes(&path);
    let mut metrics = Metrics::default();
    let rss_before = process_rss();
    let cpu_before = process_cpu_nanos();
    begin_profile();
    let gc_started = Instant::now();
    {
        let storage = RocksDB::open(&path)?;
        start_gc(&storage, objects, &mut metrics).await?;
        assert!(!gc_step(&storage, page_size, pack_bits, &mut metrics).await?);
        storage.flush()?;
    }
    let progress = {
        let storage = RocksDB::open(&path)?;
        let progress = run_to_completion(&storage, page_size, pack_bits, &mut metrics).await?;
        storage.flush()?;
        progress
    };
    let gc_wall_us = gc_started.elapsed().as_secs_f64() * 1_000_000.0;
    let gc_cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
    let (gc_alloc_bytes, gc_alloc_calls) = end_profile();
    assert_eq!(progress.marked, live);
    assert_eq!(progress.swept, objects - live);
    let mut result = RunResult {
        backend: "rocksdb",
        objects,
        live,
        page_size,
        pack_bits,
        setup_wall_us,
        gc_wall_us,
        gc_cpu_us,
        gc_alloc_bytes,
        gc_alloc_calls,
        rss_before,
        rss_after: process_rss(),
        peak_rss: process_peak_rss(),
        disk_before,
        disk_after: directory_bytes(&path),
        metrics,
        physical: SlateDBIoSnapshot::default(),
    };
    {
        let storage = RocksDB::open(&path)?;
        validate_completed(&storage, live, page_size, &result).await?;
    }
    {
        let storage = RocksDB::open(&final_path)?;
        final_reference_oracle(&storage, page_size, pack_bits).await?;
        storage.flush()?;
    }
    {
        let storage = RocksDB::open(&corrupt_path)?;
        corruption_oracle(&storage, page_size, pack_bits).await?;
        storage.flush()?;
    }
    result.disk_after = directory_bytes(&path);
    Ok(result)
}

async fn run_slate(
    objects: u64,
    live_percent: u64,
    page_size: usize,
    pack_bits: usize,
) -> Result<RunResult, StorageError> {
    let directory = tempfile::tempdir().expect("SlateDB scaling directory");
    let path = directory.path().join("scale");
    let final_path = directory.path().join("final-reference");
    let corrupt_path = directory.path().join("corruption");
    let counters = SlateDBIoCounters::default();
    let setup_started = Instant::now();
    let live = {
        let storage = SlateDB::open_with_io_counters(&path, counters.clone())?;
        let mut setup = Metrics::default();
        let live = seed_fixture(&storage, objects, live_percent, &mut setup).await?;
        storage.flush().await?;
        live
    };
    let setup_wall_us = setup_started.elapsed().as_secs_f64() * 1_000_000.0;
    let disk_before = directory_bytes(&path);
    let physical_before = counters.snapshot();
    let mut metrics = Metrics::default();
    let rss_before = process_rss();
    let cpu_before = process_cpu_nanos();
    begin_profile();
    let gc_started = Instant::now();
    {
        let storage = SlateDB::open_with_io_counters(&path, counters.clone())?;
        start_gc(&storage, objects, &mut metrics).await?;
        assert!(!gc_step(&storage, page_size, pack_bits, &mut metrics).await?);
        storage.flush().await?;
    }
    let progress = {
        let storage = SlateDB::open_with_io_counters(&path, counters.clone())?;
        let progress = run_to_completion(&storage, page_size, pack_bits, &mut metrics).await?;
        storage.flush().await?;
        progress
    };
    let gc_wall_us = gc_started.elapsed().as_secs_f64() * 1_000_000.0;
    let gc_cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
    let (gc_alloc_bytes, gc_alloc_calls) = end_profile();
    assert_eq!(progress.marked, live);
    assert_eq!(progress.swept, objects - live);
    let physical = counters.snapshot().saturating_sub(physical_before);
    let mut result = RunResult {
        backend: "slatedb",
        objects,
        live,
        page_size,
        pack_bits,
        setup_wall_us,
        gc_wall_us,
        gc_cpu_us,
        gc_alloc_bytes,
        gc_alloc_calls,
        rss_before,
        rss_after: process_rss(),
        peak_rss: process_peak_rss(),
        disk_before,
        disk_after: directory_bytes(&path),
        metrics,
        physical,
    };
    {
        let storage = SlateDB::open_with_io_counters(&path, counters.clone())?;
        validate_completed(&storage, live, page_size, &result).await?;
    }
    {
        let storage = SlateDB::open_with_io_counters(&final_path, counters.clone())?;
        final_reference_oracle(&storage, page_size, pack_bits).await?;
        storage.flush().await?;
    }
    {
        let storage = SlateDB::open_with_io_counters(&corrupt_path, counters.clone())?;
        corruption_oracle(&storage, page_size, pack_bits).await?;
        storage.flush().await?;
    }
    result.disk_after = directory_bytes(&path);
    Ok(result)
}

fn parse_u64(argument: Option<String>, name: &str) -> u64 {
    argument
        .unwrap_or_else(|| panic!("missing {name}"))
        .parse::<u64>()
        .unwrap_or_else(|_| panic!("invalid {name}"))
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let backend = arguments.next().unwrap_or_else(|| "rocksdb".to_owned());
    let objects = parse_u64(arguments.next(), "objects");
    let live_percent = parse_u64(arguments.next(), "live percent");
    let page_size =
        usize::try_from(parse_u64(arguments.next(), "page size")).expect("page size fits usize");
    let pack_bits =
        usize::try_from(parse_u64(arguments.next(), "pack bits")).expect("pack bits fits usize");
    assert!(objects > 0);
    assert!((1..=100).contains(&live_percent));
    assert!(page_size > 0);
    assert!(pack_bits >= 8 && pack_bits.is_multiple_of(8));
    assert!(arguments.next().is_none(), "unexpected argument");
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build GC scaling runtime")
        .block_on(async {
            let result = match backend.as_str() {
                "rocksdb" => run_rocks(objects, live_percent, page_size, pack_bits).await,
                "slatedb" => run_slate(objects, live_percent, page_size, pack_bits).await,
                other => panic!("unknown backend '{other}'"),
            }
            .expect("bounded mark-pack/sweep scaling oracle");
            print_result(&result);
        });
}
