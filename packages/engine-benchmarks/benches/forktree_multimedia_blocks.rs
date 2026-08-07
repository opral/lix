//! Test-only physical-layout model for an authenticated blocked multimedia tree.
//!
//! This deliberately does not call or modify production ForkTree code. It compares
//! the authority shapes and exercises the proposed immutable-object contract on the
//! real RocksDB and SlateDB adapters.

use std::alloc::GlobalAlloc;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use lix::storage::{
    CommitResult, CoreProjection, GetManyRequest, GetManyResult, GetOptions, Key, KeyRange,
    Precondition, ProjectedValue, PutBatch, PutEntry, ReadOptions, ScanChunk, ScanOptions, SpaceId,
    Storage, StorageError, StorageRead, StorageSpace, StorageWrite, StoredValue, WriteOptions,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

const CHUNK_BYTES: usize = 1 << 20;
const FANOUT: usize = 64;
const AUTH_LEAF_BATCH: usize = 8;
const OBJECT_MAGIC: &[u8; 8] = b"LIXMMO\0\x01";
const LEAF_DOMAIN: u8 = 1;
const INTERNAL_DOMAIN: u8 = 2;
const FLAT_DOMAIN: u8 = 3;
const OBJECT_SPACE: StorageSpace =
    StorageSpace::immutable(SpaceId(0x00f1_0001), "bench.media.object.v1");
const SELECTOR_SPACE: StorageSpace =
    StorageSpace::mutable(SpaceId(0x00f1_0002), "bench.media.selector.v1");
const CURRENT_PAYLOAD_SPACE: StorageSpace =
    StorageSpace::immutable(SpaceId(0x00f1_0011), "bench.current.payload");
const CURRENT_PRESENCE_SPACE: StorageSpace =
    StorageSpace::mutable(SpaceId(0x00f1_0012), "bench.current.presence");
const CURRENT_MANIFEST_SPACE: StorageSpace =
    StorageSpace::mutable(SpaceId(0x00f1_0013), "bench.current.manifest");

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

#[derive(Clone, Debug, Default)]
struct IoStats {
    get_calls: u64,
    get_keys: u64,
    get_values: u64,
    get_value_bytes: u64,
    scan_calls: u64,
    scan_entries: u64,
    scan_value_bytes: u64,
    write_batches: u64,
    write_puts: u64,
    write_deletes: u64,
    write_bytes: u64,
}

#[derive(Clone)]
struct CountingStorage<S> {
    inner: S,
    stats: Arc<Mutex<IoStats>>,
}

struct CountingRead<R> {
    inner: R,
    stats: Arc<Mutex<IoStats>>,
}

struct CountingWrite<W> {
    inner: W,
    stats: Arc<Mutex<IoStats>>,
}

impl<S> CountingStorage<S> {
    fn new(inner: S) -> (Self, Arc<Mutex<IoStats>>) {
        let stats = Arc::new(Mutex::new(IoStats::default()));
        (
            Self {
                inner,
                stats: Arc::clone(&stats),
            },
            stats,
        )
    }
}

impl<S: Storage> Storage for CountingStorage<S> {
    type Read<'a>
        = CountingRead<S::Read<'a>>
    where
        Self: 'a;
    type Write<'a>
        = CountingWrite<S::Write<'a>>
    where
        Self: 'a;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        Ok(CountingRead {
            inner: self.inner.begin_read(options).await?,
            stats: Arc::clone(&self.stats),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(CountingWrite {
            inner: self.inner.begin_write(options).await?,
            stats: Arc::clone(&self.stats),
        })
    }
}

impl<R: StorageRead> StorageRead for CountingRead<R> {
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.get_calls += 1;
            stats.get_keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
        }
        let result = self.inner.get_many(requests).await?;
        let mut stats = self.stats.lock().expect("I/O stats mutex");
        for value in result.values.iter().flatten() {
            stats.get_values += 1;
            stats.get_value_bytes += projected_value_len(value) as u64;
        }
        drop(stats);
        Ok(result)
    }

    async fn scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        self.stats.lock().expect("I/O stats mutex").scan_calls += 1;
        let chunk = self.inner.scan(space, range, options).await?;
        let mut stats = self.stats.lock().expect("I/O stats mutex");
        stats.scan_entries += chunk.entries.len() as u64;
        stats.scan_value_bytes += chunk
            .entries
            .iter()
            .map(|entry| projected_value_len(&entry.value) as u64)
            .sum::<u64>();
        drop(stats);
        Ok(chunk)
    }
}

impl<W: StorageWrite> StorageWrite for CountingWrite<W> {
    async fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.write_batches += 1;
            stats.write_puts += entries.entries.len() as u64;
            stats.write_bytes += entries
                .entries
                .iter()
                .map(|entry| (entry.key.0.len() + entry.value.bytes.len()) as u64)
                .sum::<u64>();
        }
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.write_batches += 1;
            stats.write_deletes += keys.len() as u64;
            stats.write_bytes += keys.iter().map(|key| key.0.len() as u64).sum::<u64>();
        }
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ObjectId([u8; 32]);

impl ObjectId {
    fn key(self) -> Key {
        Key(Bytes::copy_from_slice(&self.0))
    }
}

#[derive(Clone, Copy, Debug)]
struct ChildRef {
    id: ObjectId,
    logical_bytes: u64,
}

#[derive(Clone, Debug)]
enum Object {
    Leaf(Bytes),
    Internal {
        level: u8,
        logical_bytes: u64,
        children: Vec<ChildRef>,
    },
    Flat {
        logical_bytes: u64,
        content_digest: [u8; 32],
        children: Vec<ChildRef>,
    },
}

#[derive(Clone, Copy, Debug, Default)]
struct HashWork {
    payload_bytes: u64,
    object_bytes: u64,
    leaf_objects: u64,
    internal_objects: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Layout {
    Current,
    Flat,
    Blocked,
}

impl Layout {
    fn parse(value: &str) -> Self {
        match value {
            "current" => Self::Current,
            "flat" => Self::Flat,
            "blocked" => Self::Blocked,
            other => panic!("unknown layout '{other}'"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Flat => "forktree_flat",
            Self::Blocked => "forktree_blocked",
        }
    }
}

#[derive(Clone, Copy)]
struct Parameters {
    layout: Layout,
    size_mib: usize,
    edit_percent: usize,
    samples: usize,
}

#[derive(Clone)]
struct Fixture {
    base: Arc<Vec<u8>>,
    edit_offset: usize,
    edit: Arc<Vec<u8>>,
    base_root: ObjectId,
    expected_root: ObjectId,
    selector_keys: Vec<Key>,
}

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("create multimedia benchmark runtime")
        .block_on(run());
}

async fn run() {
    let args = std::env::args().collect::<Vec<_>>();
    let mode = args.get(1).map(String::as_str).unwrap_or("perf");
    let backend = args.get(2).map(String::as_str).unwrap_or("rocksdb");
    if mode == "correctness" {
        match backend {
            "rocksdb" => correctness_rocks().await,
            "slatedb" => correctness_slate().await,
            other => panic!("unknown backend '{other}'"),
        }
        return;
    }
    let parameters = Parameters {
        layout: Layout::parse(args.get(3).map(String::as_str).unwrap_or("blocked")),
        size_mib: parse_positive(args.get(4), "size_mib", 64),
        edit_percent: parse_positive(args.get(5), "edit_percent", 1),
        samples: parse_positive(args.get(6), "samples", 3),
    };
    assert!(matches!(parameters.size_mib, 64 | 512));
    assert!(matches!(parameters.edit_percent, 1 | 10));
    match backend {
        "rocksdb" => perf_rocks(parameters).await,
        "slatedb" => perf_slate(parameters).await,
        other => panic!("unknown backend '{other}'"),
    }
}

async fn perf_rocks(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create RocksDB directory");
    let path = directory.path().join("database");
    let database = RocksDB::open(&path).expect("open RocksDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    let fixture = seed_fixture(&storage, parameters).await;
    database.flush().expect("flush seed");
    print_seed(
        "rocksdb",
        parameters,
        &fixture,
        take_stats(&stats),
        directory_bytes(&path),
    );
    measure(
        "rocksdb",
        parameters,
        &storage,
        &fixture,
        &stats,
        &path,
        None,
        || database.flush().expect("flush sample"),
    )
    .await;
    drop(storage);
    drop(database);
    let reopened = RocksDB::open(&path).expect("reopen RocksDB");
    assert_cold_reopen(&reopened, parameters.layout, &fixture).await;
    println!(
        "forktree_media_reopen,backend=rocksdb,layout={},cold_reopen=true,exact_bytes=true",
        parameters.layout.name()
    );
}

async fn perf_slate(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create SlateDB directory");
    let path = directory.path().join("database");
    let counters = SlateDBIoCounters::default();
    let database = SlateDB::open_with_io_counters(&path, counters.clone()).expect("open SlateDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    let fixture = seed_fixture(&storage, parameters).await;
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush seed");
    print_seed(
        "slatedb",
        parameters,
        &fixture,
        take_stats(&stats),
        directory_bytes(&path),
    );
    measure_slate(
        parameters, &storage, &fixture, &stats, &path, &database, &counters,
    )
    .await;
    drop(storage);
    drop(database);
    let reopened = SlateDB::open(&path).expect("reopen SlateDB");
    assert_cold_reopen(&reopened, parameters.layout, &fixture).await;
    println!(
        "forktree_media_reopen,backend=slatedb,layout={},cold_reopen=true,exact_bytes=true",
        parameters.layout.name()
    );
}

async fn measure<S, F>(
    backend: &str,
    parameters: Parameters,
    storage: &S,
    fixture: &Fixture,
    stats: &Arc<Mutex<IoStats>>,
    path: &Path,
    physical_counters: Option<&SlateDBIoCounters>,
    mut flush: F,
) where
    S: Storage,
    F: FnMut(),
{
    for sample in 0..parameters.samples {
        let _ = take_stats(stats);
        let physical_before = physical_counters.map(SlateDBIoCounters::snapshot);
        let disk_before = directory_bytes(path);
        let rss_before = process_resident_bytes();
        let high_water_before = process_high_water_bytes();
        let cpu_before = process_cpu_nanos();
        begin_allocation_profile();
        let started = Instant::now();
        let (root, hash_work) = publish_edit(
            storage,
            parameters.layout,
            &fixture.selector_keys[sample],
            fixture,
        )
        .await;
        let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
        let (allocated_bytes, allocation_calls) = end_allocation_profile();
        let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
        assert_eq!(root, fixture.expected_root);
        let rss_after = process_resident_bytes();
        let high_water_after = process_high_water_bytes();
        let io = take_stats(stats);
        flush();
        let disk_after = directory_bytes(path);
        let physical = physical_before.map_or_else(SlateDBIoSnapshot::default, |before| {
            physical_counters
                .expect("physical counters")
                .snapshot()
                .saturating_sub(before)
        });
        print_sample(
            backend,
            parameters,
            sample + 1,
            wall_us,
            cpu_us,
            allocated_bytes,
            allocation_calls,
            rss_before,
            rss_after,
            high_water_before,
            high_water_after,
            &io,
            physical,
            disk_before,
            disk_after,
            hash_work,
        );
    }
}

async fn measure_slate<S: Storage>(
    parameters: Parameters,
    storage: &S,
    fixture: &Fixture,
    stats: &Arc<Mutex<IoStats>>,
    path: &Path,
    database: &SlateDB,
    counters: &SlateDBIoCounters,
) {
    for sample in 0..parameters.samples {
        let _ = take_stats(stats);
        let physical_before = counters.snapshot();
        let disk_before = directory_bytes(path);
        let rss_before = process_resident_bytes();
        let high_water_before = process_high_water_bytes();
        let cpu_before = process_cpu_nanos();
        begin_allocation_profile();
        let started = Instant::now();
        let (root, hash_work) = publish_edit(
            storage,
            parameters.layout,
            &fixture.selector_keys[sample],
            fixture,
        )
        .await;
        let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
        let (allocated_bytes, allocation_calls) = end_allocation_profile();
        let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
        assert_eq!(root, fixture.expected_root);
        let rss_after = process_resident_bytes();
        let high_water_after = process_high_water_bytes();
        let io = take_stats(stats);
        database
            .flush_memtable_for_diagnostics()
            .await
            .expect("flush sample");
        let physical = counters.snapshot().saturating_sub(physical_before);
        let disk_after = directory_bytes(path);
        print_sample(
            "slatedb",
            parameters,
            sample + 1,
            wall_us,
            cpu_us,
            allocated_bytes,
            allocation_calls,
            rss_before,
            rss_after,
            high_water_before,
            high_water_after,
            &io,
            physical,
            disk_before,
            disk_after,
            hash_work,
        );
    }
}

async fn seed_fixture<S: Storage>(storage: &S, parameters: Parameters) -> Fixture {
    let size = parameters.size_mib << 20;
    let base = Arc::new(deterministic_bytes(size, 7));
    let edit_len = size * parameters.edit_percent / 100;
    let edit_offset = size / 3 + 17;
    let edit_offset = edit_offset.min(size - edit_len);
    let edit = Arc::new(deterministic_bytes(edit_len, 11));
    let selector_keys = (0..parameters.samples)
        .map(|sample| Key(Bytes::from(format!("root-{sample:04}"))))
        .collect::<Vec<_>>();
    let base_root = match parameters.layout {
        Layout::Current => seed_current(storage, &base).await,
        Layout::Flat => seed_flat(storage, &base).await,
        Layout::Blocked => seed_blocked(storage, &base).await,
    };
    seed_selectors(storage, &selector_keys, base_root).await;
    let (expected_root, _) = compute_expected_root(parameters.layout, &base, edit_offset, &edit);
    Fixture {
        base,
        edit_offset,
        edit,
        base_root,
        expected_root,
        selector_keys,
    }
}

async fn seed_current<S: Storage>(storage: &S, bytes: &[u8]) -> ObjectId {
    let refs = chunk_refs(bytes, None, &mut HashWork::default());
    for batch in refs.chunks(32) {
        let entries = batch
            .iter()
            .map(|child| {
                let index = refs
                    .iter()
                    .position(|candidate| candidate.id == child.id)
                    .expect("chunk ref exists");
                let start = index * CHUNK_BYTES;
                let end = (start + child.logical_bytes as usize).min(bytes.len());
                PutEntry {
                    key: child.id.key(),
                    value: StoredValue {
                        bytes: Bytes::copy_from_slice(&bytes[start..end]),
                    },
                }
            })
            .collect::<Vec<_>>();
        put_batch(storage, CURRENT_PAYLOAD_SPACE, entries).await;
    }
    let marker_entries = refs
        .iter()
        .map(|child| PutEntry {
            key: child.id.key(),
            value: StoredValue {
                bytes: Bytes::from_static(b"\x01"),
            },
        })
        .collect();
    put_batch(storage, CURRENT_PRESENCE_SPACE, marker_entries).await;
    let manifest = encode_manifest(bytes.len() as u64, &refs);
    let root = object_id(&manifest);
    put_batch(
        storage,
        CURRENT_MANIFEST_SPACE,
        vec![PutEntry {
            key: root.key(),
            value: StoredValue { bytes: manifest },
        }],
    )
    .await;
    root
}

async fn seed_flat<S: Storage>(storage: &S, bytes: &[u8]) -> ObjectId {
    let mut refs = Vec::new();
    let mut pending = Vec::new();
    for chunk in bytes.chunks(CHUNK_BYTES) {
        let (id, encoded) = encode_object(&Object::Leaf(Bytes::copy_from_slice(chunk)));
        refs.push(ChildRef {
            id,
            logical_bytes: chunk.len() as u64,
        });
        pending.push(PutEntry {
            key: id.key(),
            value: StoredValue { bytes: encoded },
        });
        if pending.len() == 32 {
            put_batch(storage, OBJECT_SPACE, std::mem::take(&mut pending)).await;
        }
    }
    if !pending.is_empty() {
        put_batch(storage, OBJECT_SPACE, pending).await;
    }
    let flat = Object::Flat {
        logical_bytes: bytes.len() as u64,
        content_digest: *blake3::hash(bytes).as_bytes(),
        children: refs,
    };
    let (root, encoded) = encode_object(&flat);
    put_batch(
        storage,
        OBJECT_SPACE,
        vec![PutEntry {
            key: root.key(),
            value: StoredValue { bytes: encoded },
        }],
    )
    .await;
    root
}

async fn seed_blocked<S: Storage>(storage: &S, bytes: &[u8]) -> ObjectId {
    let mut level = Vec::new();
    let mut pending = Vec::new();
    for chunk in bytes.chunks(CHUNK_BYTES) {
        let (id, encoded) = encode_object(&Object::Leaf(Bytes::copy_from_slice(chunk)));
        level.push(ChildRef {
            id,
            logical_bytes: chunk.len() as u64,
        });
        pending.push(PutEntry {
            key: id.key(),
            value: StoredValue { bytes: encoded },
        });
        if pending.len() == 32 {
            put_batch(storage, OBJECT_SPACE, std::mem::take(&mut pending)).await;
        }
    }
    if !pending.is_empty() {
        put_batch(storage, OBJECT_SPACE, pending).await;
    }
    let mut tree_level = 1_u8;
    while level.len() > 1 {
        let mut next = Vec::new();
        let mut entries = Vec::new();
        for children in level.chunks(FANOUT) {
            let total = children.iter().map(|child| child.logical_bytes).sum();
            let node = Object::Internal {
                level: tree_level,
                logical_bytes: total,
                children: children.to_vec(),
            };
            let (id, encoded) = encode_object(&node);
            next.push(ChildRef {
                id,
                logical_bytes: total,
            });
            entries.push(PutEntry {
                key: id.key(),
                value: StoredValue { bytes: encoded },
            });
        }
        put_batch(storage, OBJECT_SPACE, entries).await;
        level = next;
        tree_level += 1;
    }
    level[0].id
}

async fn seed_selectors<S: Storage>(storage: &S, keys: &[Key], root: ObjectId) {
    put_batch(
        storage,
        SELECTOR_SPACE,
        keys.iter()
            .cloned()
            .map(|key| PutEntry {
                key,
                value: StoredValue {
                    bytes: Bytes::copy_from_slice(&root.0),
                },
            })
            .collect(),
    )
    .await;
}

async fn publish_edit<S: Storage>(
    storage: &S,
    layout: Layout,
    selector_key: &Key,
    fixture: &Fixture,
) -> (ObjectId, HashWork) {
    match layout {
        Layout::Current => publish_current(storage, selector_key, fixture).await,
        Layout::Flat => publish_flat(storage, selector_key, fixture).await,
        Layout::Blocked => publish_blocked(storage, selector_key, fixture).await,
    }
}

async fn publish_current<S: Storage>(
    storage: &S,
    selector_key: &Key,
    fixture: &Fixture,
) -> (ObjectId, HashWork) {
    let mut work = HashWork::default();
    let refs = chunk_refs(
        &fixture.base,
        Some((fixture.edit_offset, &fixture.edit)),
        &mut work,
    );
    let manifest = encode_manifest(fixture.base.len() as u64, &refs);
    work.object_bytes += manifest.len() as u64;
    work.internal_objects += 1;
    let root = object_id(&manifest);
    let changed =
        changed_chunk_indices(fixture.base.len(), fixture.edit_offset, fixture.edit.len());
    let mut payloads = Vec::new();
    let mut markers = Vec::new();
    for index in changed {
        let chunk = edited_chunk(fixture, index);
        let child = refs[index];
        payloads.push(PutEntry {
            key: child.id.key(),
            value: StoredValue {
                bytes: Bytes::from(chunk),
            },
        });
        markers.push(PutEntry {
            key: child.id.key(),
            value: StoredValue {
                bytes: Bytes::from_static(b"\x01"),
            },
        });
    }
    let mut write = storage
        .begin_write(cas_options(selector_key, fixture.base_root))
        .await
        .expect("begin current publication");
    write
        .put_many(CURRENT_PAYLOAD_SPACE, PutBatch { entries: payloads })
        .await
        .expect("stage current payloads");
    write
        .put_many(CURRENT_PRESENCE_SPACE, PutBatch { entries: markers })
        .await
        .expect("stage current markers");
    write
        .put_many(
            CURRENT_MANIFEST_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: root.key(),
                    value: StoredValue { bytes: manifest },
                }],
            },
        )
        .await
        .expect("stage current manifest");
    write
        .put_many(
            SELECTOR_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: selector_key.clone(),
                    value: StoredValue {
                        bytes: Bytes::copy_from_slice(&root.0),
                    },
                }],
            },
        )
        .await
        .expect("stage current selector");
    write.commit().await.expect("commit current publication");
    (root, work)
}

async fn publish_flat<S: Storage>(
    storage: &S,
    selector_key: &Key,
    fixture: &Fixture,
) -> (ObjectId, HashWork) {
    let raw = get_one(storage, OBJECT_SPACE, fixture.base_root.key())
        .await
        .expect("flat base manifest exists");
    let base_manifest = decode_object(fixture.base_root, &raw).expect("authenticate flat base");
    let Object::Flat { children, .. } = base_manifest else {
        panic!("flat root is not a flat manifest")
    };
    let mut work = HashWork::default();
    let refs = chunk_refs(
        &fixture.base,
        Some((fixture.edit_offset, &fixture.edit)),
        &mut work,
    );
    assert_eq!(children.len(), refs.len());
    let mut digest = blake3::Hasher::new();
    for (index, original) in fixture.base.chunks(CHUNK_BYTES).enumerate() {
        if ranges_overlap(
            index * CHUNK_BYTES,
            index * CHUNK_BYTES + original.len(),
            fixture.edit_offset,
            fixture.edit_offset + fixture.edit.len(),
        ) {
            let chunk = edited_chunk(fixture, index);
            digest.update(&chunk);
            work.payload_bytes += chunk.len() as u64;
        } else {
            digest.update(original);
            work.payload_bytes += original.len() as u64;
        }
    }
    let flat = Object::Flat {
        logical_bytes: fixture.base.len() as u64,
        content_digest: *digest.finalize().as_bytes(),
        children: refs,
    };
    let (root, encoded) = encode_object_counted(&flat, &mut work);
    let mut entries = vec![PutEntry {
        key: root.key(),
        value: StoredValue { bytes: encoded },
    }];
    for index in changed_chunk_indices(fixture.base.len(), fixture.edit_offset, fixture.edit.len())
    {
        let leaf = Object::Leaf(Bytes::from(edited_chunk(fixture, index)));
        let (id, encoded) = encode_object_counted(&leaf, &mut work);
        assert_eq!(id, flat_child(&flat, index));
        entries.push(PutEntry {
            key: id.key(),
            value: StoredValue { bytes: encoded },
        });
    }
    publish_objects_and_selector(storage, selector_key, fixture.base_root, root, entries).await;
    (root, work)
}

async fn publish_blocked<S: Storage>(
    storage: &S,
    selector_key: &Key,
    fixture: &Fixture,
) -> (ObjectId, HashWork) {
    let changed =
        changed_chunk_indices(fixture.base.len(), fixture.edit_offset, fixture.edit.len());
    let root_raw = get_one(storage, OBJECT_SPACE, fixture.base_root.key())
        .await
        .expect("blocked root exists");
    let root_node = decode_object(fixture.base_root, &root_raw).expect("authenticate blocked root");
    let Object::Internal {
        level: root_level,
        logical_bytes,
        mut children,
    } = root_node
    else {
        panic!("benchmark sizes must have an internal root")
    };
    assert_eq!(logical_bytes, fixture.base.len() as u64);
    let mut work = HashWork::default();
    work.object_bytes += root_raw.len() as u64;
    let mut entries = Vec::new();
    if root_level == 1 {
        for indices in changed.chunks(AUTH_LEAF_BATCH) {
            let leaf_keys = indices
                .iter()
                .map(|index| children[*index].id.key())
                .collect::<Vec<_>>();
            let leaf_values = get_many_values(storage, OBJECT_SPACE, &leaf_keys).await;
            for (index, raw) in indices.iter().copied().zip(leaf_values) {
                let (id, encoded) = path_copy_leaf(children[index], raw, fixture, index, &mut work);
                children[index].id = id;
                entries.push(PutEntry {
                    key: id.key(),
                    value: StoredValue { bytes: encoded },
                });
            }
        }
    } else {
        assert_eq!(root_level, 2);
        let mut groups = BTreeMap::<usize, Vec<usize>>::new();
        for index in changed {
            groups.entry(index / FANOUT).or_default().push(index);
        }
        let groups = groups.into_iter().collect::<Vec<_>>();
        let group_keys = groups
            .iter()
            .map(|(group, _)| children[*group].id.key())
            .collect::<Vec<_>>();
        let group_values = get_many_values(storage, OBJECT_SPACE, &group_keys).await;
        let mut states = Vec::new();
        for ((group, indices), child_raw) in groups.into_iter().zip(group_values) {
            let node = decode_object(children[group].id, &child_raw)
                .expect("authenticate blocked level-one node");
            work.object_bytes += child_raw.len() as u64;
            let Object::Internal {
                level: 1,
                logical_bytes: child_bytes,
                children: leaves,
            } = node
            else {
                panic!("root child is not a level-one node")
            };
            states.push((group, indices, child_bytes, leaves));
        }
        let leaf_requests = states
            .iter()
            .enumerate()
            .flat_map(|(state_index, (_, indices, _, leaves))| {
                indices.iter().map(move |index| {
                    let local = index % FANOUT;
                    (state_index, *index, local, leaves[local])
                })
            })
            .collect::<Vec<_>>();
        for requests in leaf_requests.chunks(AUTH_LEAF_BATCH) {
            let leaf_keys = requests
                .iter()
                .map(|(_, _, _, child)| child.id.key())
                .collect::<Vec<_>>();
            let leaf_values = get_many_values(storage, OBJECT_SPACE, &leaf_keys).await;
            for ((state_index, index, local, child), raw) in
                requests.iter().copied().zip(leaf_values)
            {
                let (id, encoded) = path_copy_leaf(child, raw, fixture, index, &mut work);
                states[state_index].3[local].id = id;
                entries.push(PutEntry {
                    key: id.key(),
                    value: StoredValue { bytes: encoded },
                });
            }
        }
        for (group, _, child_bytes, leaves) in states {
            for (local, leaf) in leaves.iter().enumerate() {
                let index = group * FANOUT + local;
                if index * CHUNK_BYTES < fixture.base.len() {
                    assert_eq!(leaf.logical_bytes, base_chunk(fixture, index).len() as u64);
                }
            }
            let node = Object::Internal {
                level: 1,
                logical_bytes: child_bytes,
                children: leaves,
            };
            let (id, encoded) = encode_object_counted(&node, &mut work);
            children[group].id = id;
            entries.push(PutEntry {
                key: id.key(),
                value: StoredValue { bytes: encoded },
            });
        }
    }
    let root = Object::Internal {
        level: root_level,
        logical_bytes,
        children,
    };
    let (new_root, encoded) = encode_object_counted(&root, &mut work);
    entries.push(PutEntry {
        key: new_root.key(),
        value: StoredValue { bytes: encoded },
    });
    publish_objects_and_selector(storage, selector_key, fixture.base_root, new_root, entries).await;
    (new_root, work)
}

fn path_copy_leaf(
    child: ChildRef,
    raw: Bytes,
    fixture: &Fixture,
    index: usize,
    work: &mut HashWork,
) -> (ObjectId, Bytes) {
    const LEAF_HEADER_BYTES: usize = OBJECT_MAGIC.len() + 1 + 8;
    work.object_bytes += raw.len() as u64;
    assert_eq!(object_id(&raw), child.id, "authenticate changed leaf");
    assert_eq!(&raw[..OBJECT_MAGIC.len()], OBJECT_MAGIC);
    assert_eq!(raw[OBJECT_MAGIC.len()], LEAF_DOMAIN);
    let declared_len = u64::from_be_bytes(
        raw[OBJECT_MAGIC.len() + 1..LEAF_HEADER_BYTES]
            .try_into()
            .expect("leaf declared length width"),
    );
    assert_eq!(declared_len, child.logical_bytes);
    assert_eq!(raw.len(), LEAF_HEADER_BYTES + declared_len as usize);
    assert_eq!(&raw[LEAF_HEADER_BYTES..], base_chunk(fixture, index));

    let mut successor = raw.to_vec();
    let chunk_start = index * CHUNK_BYTES;
    let chunk_end = chunk_start + declared_len as usize;
    let edit_end = fixture.edit_offset + fixture.edit.len();
    let overlap_start = chunk_start.max(fixture.edit_offset);
    let overlap_end = chunk_end.min(edit_end);
    assert!(overlap_start < overlap_end);
    successor[LEAF_HEADER_BYTES + overlap_start - chunk_start
        ..LEAF_HEADER_BYTES + overlap_end - chunk_start]
        .copy_from_slice(
            &fixture.edit[overlap_start - fixture.edit_offset..overlap_end - fixture.edit_offset],
        );
    let id = object_id(&successor);
    work.payload_bytes += declared_len;
    work.object_bytes += successor.len() as u64;
    work.leaf_objects += 1;
    (id, Bytes::from(successor))
}

async fn publish_objects_and_selector<S: Storage>(
    storage: &S,
    selector_key: &Key,
    expected: ObjectId,
    root: ObjectId,
    entries: Vec<PutEntry>,
) {
    let mut write = storage
        .begin_write(cas_options(selector_key, expected))
        .await
        .expect("begin object publication");
    write
        .put_many(OBJECT_SPACE, PutBatch { entries })
        .await
        .expect("stage immutable objects");
    write
        .put_many(
            SELECTOR_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: selector_key.clone(),
                    value: StoredValue {
                        bytes: Bytes::copy_from_slice(&root.0),
                    },
                }],
            },
        )
        .await
        .expect("stage selector");
    write.commit().await.expect("commit atomic publication");
}

fn cas_options(selector_key: &Key, expected: ObjectId) -> WriteOptions {
    WriteOptions {
        preconditions: vec![Precondition::KeyValueEquals {
            space: SELECTOR_SPACE,
            key: selector_key.clone(),
            expected: Bytes::copy_from_slice(&expected.0),
        }],
        ..WriteOptions::default()
    }
}

fn compute_expected_root(
    layout: Layout,
    base: &[u8],
    edit_offset: usize,
    edit: &[u8],
) -> (ObjectId, HashWork) {
    let mut work = HashWork::default();
    let refs = chunk_refs(base, Some((edit_offset, edit)), &mut work);
    match layout {
        Layout::Current => {
            let manifest = encode_manifest(base.len() as u64, &refs);
            (object_id(&manifest), work)
        }
        Layout::Flat => {
            let mut digest = blake3::Hasher::new();
            for (index, original) in base.chunks(CHUNK_BYTES).enumerate() {
                let chunk_start = index * CHUNK_BYTES;
                if ranges_overlap(
                    chunk_start,
                    chunk_start + original.len(),
                    edit_offset,
                    edit_offset + edit.len(),
                ) {
                    digest.update(&apply_edit_to_chunk(original, index, edit_offset, edit));
                } else {
                    digest.update(original);
                }
            }
            let object = Object::Flat {
                logical_bytes: base.len() as u64,
                content_digest: *digest.finalize().as_bytes(),
                children: refs,
            };
            (encode_object(&object).0, work)
        }
        Layout::Blocked => {
            let mut level = refs;
            let mut tree_level = 1;
            while level.len() > 1 {
                level = level
                    .chunks(FANOUT)
                    .map(|children| {
                        let total = children.iter().map(|child| child.logical_bytes).sum();
                        let node = Object::Internal {
                            level: tree_level,
                            logical_bytes: total,
                            children: children.to_vec(),
                        };
                        ChildRef {
                            id: encode_object(&node).0,
                            logical_bytes: total,
                        }
                    })
                    .collect();
                tree_level += 1;
            }
            (level[0].id, work)
        }
    }
}

fn chunk_refs(base: &[u8], edit: Option<(usize, &[u8])>, work: &mut HashWork) -> Vec<ChildRef> {
    base.chunks(CHUNK_BYTES)
        .enumerate()
        .map(|(index, original)| {
            let owned;
            let chunk = if edit.is_some_and(|(offset, bytes)| {
                ranges_overlap(
                    index * CHUNK_BYTES,
                    index * CHUNK_BYTES + original.len(),
                    offset,
                    offset + bytes.len(),
                )
            }) {
                let (offset, bytes) = edit.expect("overlapping edit exists");
                owned = apply_edit_to_chunk(original, index, offset, bytes);
                owned.as_slice()
            } else {
                original
            };
            work.payload_bytes += chunk.len() as u64;
            let id = leaf_id(chunk);
            ChildRef {
                id,
                logical_bytes: chunk.len() as u64,
            }
        })
        .collect()
}

fn changed_chunk_indices(size: usize, edit_offset: usize, edit_len: usize) -> Vec<usize> {
    let first = edit_offset / CHUNK_BYTES;
    let last = (edit_offset + edit_len - 1) / CHUNK_BYTES;
    (first..=last.min((size - 1) / CHUNK_BYTES)).collect()
}

fn base_chunk(fixture: &Fixture, index: usize) -> &[u8] {
    let start = index * CHUNK_BYTES;
    let end = (start + CHUNK_BYTES).min(fixture.base.len());
    &fixture.base[start..end]
}

fn edited_chunk(fixture: &Fixture, index: usize) -> Vec<u8> {
    apply_edit_to_chunk(
        base_chunk(fixture, index),
        index,
        fixture.edit_offset,
        &fixture.edit,
    )
}

fn expected_chunk(fixture: &Fixture, index: usize) -> Vec<u8> {
    let original = base_chunk(fixture, index);
    if ranges_overlap(
        index * CHUNK_BYTES,
        index * CHUNK_BYTES + original.len(),
        fixture.edit_offset,
        fixture.edit_offset + fixture.edit.len(),
    ) {
        edited_chunk(fixture, index)
    } else {
        original.to_vec()
    }
}

fn apply_edit_to_chunk(original: &[u8], index: usize, edit_offset: usize, edit: &[u8]) -> Vec<u8> {
    let chunk_start = index * CHUNK_BYTES;
    let chunk_end = chunk_start + original.len();
    let edit_end = edit_offset + edit.len();
    let overlap_start = chunk_start.max(edit_offset);
    let overlap_end = chunk_end.min(edit_end);
    let mut result = original.to_vec();
    if overlap_start < overlap_end {
        result[overlap_start - chunk_start..overlap_end - chunk_start]
            .copy_from_slice(&edit[overlap_start - edit_offset..overlap_end - edit_offset]);
    }
    result
}

fn ranges_overlap(a_start: usize, a_end: usize, b_start: usize, b_end: usize) -> bool {
    a_start < b_end && b_start < a_end
}

fn encode_object(object: &Object) -> (ObjectId, Bytes) {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(OBJECT_MAGIC);
    match object {
        Object::Leaf(payload) => {
            bytes.push(LEAF_DOMAIN);
            push_u64(&mut bytes, payload.len() as u64);
            bytes.extend_from_slice(payload);
        }
        Object::Internal {
            level,
            logical_bytes,
            children,
        } => {
            bytes.push(INTERNAL_DOMAIN);
            bytes.push(*level);
            push_u64(&mut bytes, *logical_bytes);
            push_u32(&mut bytes, children.len() as u32);
            encode_children(&mut bytes, children);
        }
        Object::Flat {
            logical_bytes,
            content_digest,
            children,
        } => {
            bytes.push(FLAT_DOMAIN);
            push_u64(&mut bytes, *logical_bytes);
            bytes.extend_from_slice(content_digest);
            push_u32(&mut bytes, children.len() as u32);
            encode_children(&mut bytes, children);
        }
    }
    let bytes = Bytes::from(bytes);
    (object_id(&bytes), bytes)
}

fn encode_object_counted(object: &Object, work: &mut HashWork) -> (ObjectId, Bytes) {
    let (id, bytes) = encode_object(object);
    work.object_bytes += bytes.len() as u64;
    match object {
        Object::Leaf(payload) => {
            work.payload_bytes += payload.len() as u64;
            work.leaf_objects += 1;
        }
        Object::Internal { .. } | Object::Flat { .. } => work.internal_objects += 1,
    }
    (id, bytes)
}

fn encode_children(bytes: &mut Vec<u8>, children: &[ChildRef]) {
    for child in children {
        bytes.extend_from_slice(&child.id.0);
        push_u64(bytes, child.logical_bytes);
    }
}

fn decode_object(expected: ObjectId, bytes: &[u8]) -> Result<Object, String> {
    if object_id(bytes) != expected {
        return Err("object bytes do not match the named content ID".into());
    }
    let mut cursor = Cursor::new(bytes);
    if cursor.take(OBJECT_MAGIC.len())? != OBJECT_MAGIC {
        return Err("object magic is malformed".into());
    }
    match cursor.u8()? {
        LEAF_DOMAIN => {
            let len = cursor.u64()? as usize;
            let payload = Bytes::copy_from_slice(cursor.take(len)?);
            cursor.finish()?;
            Ok(Object::Leaf(payload))
        }
        INTERNAL_DOMAIN => {
            let level = cursor.u8()?;
            if level == 0 {
                return Err("internal level must be positive".into());
            }
            let logical_bytes = cursor.u64()?;
            let children = decode_children(&mut cursor)?;
            if children.is_empty() || children.len() > FANOUT {
                return Err("internal fanout is invalid".into());
            }
            if children
                .iter()
                .map(|child| child.logical_bytes)
                .sum::<u64>()
                != logical_bytes
            {
                return Err("internal subtree length is inconsistent".into());
            }
            cursor.finish()?;
            Ok(Object::Internal {
                level,
                logical_bytes,
                children,
            })
        }
        FLAT_DOMAIN => {
            let logical_bytes = cursor.u64()?;
            let content_digest = cursor.array()?;
            let children = decode_children(&mut cursor)?;
            if children
                .iter()
                .map(|child| child.logical_bytes)
                .sum::<u64>()
                != logical_bytes
            {
                return Err("flat manifest length is inconsistent".into());
            }
            cursor.finish()?;
            Ok(Object::Flat {
                logical_bytes,
                content_digest,
                children,
            })
        }
        _ => Err("object domain is unknown".into()),
    }
}

fn decode_children(cursor: &mut Cursor<'_>) -> Result<Vec<ChildRef>, String> {
    let count = cursor.u32()? as usize;
    let mut children = Vec::with_capacity(count);
    for _ in 0..count {
        children.push(ChildRef {
            id: ObjectId(cursor.array()?),
            logical_bytes: cursor.u64()?,
        });
    }
    Ok(children)
}

fn encode_manifest(logical_bytes: u64, children: &[ChildRef]) -> Bytes {
    let mut bytes = Vec::with_capacity(12 + children.len() * 40);
    push_u64(&mut bytes, logical_bytes);
    push_u32(&mut bytes, children.len() as u32);
    encode_children(&mut bytes, children);
    Bytes::from(bytes)
}

fn decode_manifest(bytes: &[u8]) -> Result<(u64, Vec<ChildRef>), String> {
    let mut cursor = Cursor::new(bytes);
    let logical_bytes = cursor.u64()?;
    let children = decode_children(&mut cursor)?;
    if children
        .iter()
        .map(|child| child.logical_bytes)
        .sum::<u64>()
        != logical_bytes
    {
        return Err("manifest length is inconsistent".into());
    }
    cursor.finish()?;
    Ok((logical_bytes, children))
}

fn flat_child(object: &Object, index: usize) -> ObjectId {
    match object {
        Object::Flat { children, .. } => children[index].id,
        _ => panic!("flat child requested from non-flat object"),
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| "object offset overflows".to_string())?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| "object is truncated".to_string())?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], String> {
        self.take(N)?
            .try_into()
            .map_err(|_| "object field width is invalid".to_string())
    }

    fn finish(&self) -> Result<(), String> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("object has trailing bytes".into())
        }
    }
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn object_id(bytes: &[u8]) -> ObjectId {
    ObjectId(*blake3::hash(bytes).as_bytes())
}

fn leaf_id(payload: &[u8]) -> ObjectId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(OBJECT_MAGIC);
    hasher.update(&[LEAF_DOMAIN]);
    hasher.update(&(payload.len() as u64).to_be_bytes());
    hasher.update(payload);
    ObjectId(*hasher.finalize().as_bytes())
}

async fn put_batch<S: Storage>(storage: &S, space: StorageSpace, entries: Vec<PutEntry>) {
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin seed write");
    write
        .put_many(space, PutBatch { entries })
        .await
        .expect("stage seed batch");
    write.commit().await.expect("commit seed batch");
}

async fn get_one<S: Storage>(storage: &S, space: StorageSpace, key: Key) -> Option<Bytes> {
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin point read");
    let result = read
        .get_many(&[GetManyRequest {
            space,
            keys: &[key],
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("point read");
    match result.values.as_slice() {
        [Some(ProjectedValue::FullValue(bytes))] => Some(bytes.clone()),
        [None] => None,
        _ => panic!("point read cardinality/projection mismatch"),
    }
}

async fn get_many_values<S: Storage>(storage: &S, space: StorageSpace, keys: &[Key]) -> Vec<Bytes> {
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin point read");
    let result = read
        .get_many(&[GetManyRequest {
            space,
            keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("point read");
    assert_eq!(result.values.len(), keys.len(), "point read cardinality");
    result
        .values
        .into_iter()
        .map(|value| match value {
            Some(ProjectedValue::FullValue(bytes)) => bytes,
            Some(ProjectedValue::KeyOnly) => panic!("point read projection mismatch"),
            None => panic!("required point value is missing"),
        })
        .collect()
}

async fn assert_cold_reopen<S: Storage>(storage: &S, layout: Layout, fixture: &Fixture) {
    for selector in &fixture.selector_keys {
        let raw = get_one(storage, SELECTOR_SPACE, selector.clone())
            .await
            .expect("selector survives reopen");
        let root = ObjectId(raw.as_ref().try_into().expect("selector ID width"));
        assert_eq!(root, fixture.expected_root);
        let reconstructed = reconstruct(storage, layout, root).await;
        assert_eq!(reconstructed.len(), fixture.base.len());
        for (index, chunk) in reconstructed.chunks(CHUNK_BYTES).enumerate() {
            assert_eq!(chunk, expected_chunk(fixture, index));
        }
    }
}

async fn reconstruct<S: Storage>(storage: &S, layout: Layout, root: ObjectId) -> Vec<u8> {
    match layout {
        Layout::Current => {
            let raw = get_one(storage, CURRENT_MANIFEST_SPACE, root.key())
                .await
                .expect("current manifest exists");
            if object_id(&raw) != root {
                panic!("current manifest failed authentication");
            }
            let (_, refs) = decode_manifest(&raw).expect("decode current manifest");
            let mut result = Vec::new();
            for child in refs {
                let payload = get_one(storage, CURRENT_PAYLOAD_SPACE, child.id.key())
                    .await
                    .expect("current payload exists");
                let leaf_id = leaf_id(&payload);
                assert_eq!(leaf_id, child.id, "current payload failed authentication");
                result.extend_from_slice(&payload);
            }
            result
        }
        Layout::Flat => {
            let raw = get_one(storage, OBJECT_SPACE, root.key())
                .await
                .expect("flat manifest exists");
            let Object::Flat {
                content_digest,
                children,
                ..
            } = decode_object(root, &raw).expect("authenticate flat manifest")
            else {
                panic!("flat root type")
            };
            let mut result = Vec::new();
            for child in children {
                let raw = get_one(storage, OBJECT_SPACE, child.id.key())
                    .await
                    .expect("flat leaf exists");
                let Object::Leaf(payload) =
                    decode_object(child.id, &raw).expect("authenticate flat leaf")
                else {
                    panic!("flat child type")
                };
                assert_eq!(payload.len() as u64, child.logical_bytes);
                result.extend_from_slice(&payload);
            }
            assert_eq!(blake3::hash(&result).as_bytes(), &content_digest);
            result
        }
        Layout::Blocked => reconstruct_blocked(storage, root).await,
    }
}

fn reconstruct_blocked<S: Storage>(
    storage: &S,
    root: ObjectId,
) -> impl Future<Output = Vec<u8>> + '_ {
    async move {
        let mut result = Vec::new();
        let mut pending = vec![root];
        while let Some(id) = pending.pop() {
            let raw = get_one(storage, OBJECT_SPACE, id.key())
                .await
                .expect("blocked object exists");
            match decode_object(id, &raw).expect("authenticate blocked object") {
                Object::Leaf(payload) => result.extend_from_slice(&payload),
                Object::Internal { children, .. } => {
                    pending.extend(children.into_iter().rev().map(|child| child.id));
                }
                Object::Flat { .. } => panic!("flat object in blocked tree"),
            }
        }
        result
    }
}

async fn correctness_rocks() {
    let directory = tempfile::tempdir().expect("create correctness RocksDB directory");
    let path = directory.path().join("database");
    let storage = RocksDB::open(&path).expect("open correctness RocksDB");
    run_correctness(&storage, "rocksdb").await;
    storage.flush().expect("flush correctness RocksDB");
    drop(storage);
    let reopened = RocksDB::open(&path).expect("reopen correctness RocksDB");
    assert_correctness_reopen(&reopened).await;
}

async fn correctness_slate() {
    let directory = tempfile::tempdir().expect("create correctness SlateDB directory");
    let path = directory.path().join("database");
    let storage = SlateDB::open(&path).expect("open correctness SlateDB");
    run_correctness(&storage, "slatedb").await;
    storage
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush correctness SlateDB");
    drop(storage);
    let reopened = SlateDB::open(&path).expect("reopen correctness SlateDB");
    assert_correctness_reopen(&reopened).await;
}

async fn run_correctness<S: Storage>(storage: &S, backend: &str) {
    let parameters = Parameters {
        layout: Layout::Blocked,
        size_mib: 64,
        edit_percent: 1,
        samples: 2,
    };
    let fixture = seed_fixture(storage, parameters).await;
    let (successor, _) = publish_edit(
        storage,
        Layout::Blocked,
        &fixture.selector_keys[0],
        &fixture,
    )
    .await;
    assert_eq!(successor, fixture.expected_root);
    match storage
        .begin_write(cas_options(&fixture.selector_keys[0], fixture.base_root))
        .await
    {
        Err(StorageError::PreconditionFailed(_)) => {}
        Err(error) => panic!("unexpected stale selector begin error: {error:?}"),
        Ok(write) => {
            let raced = write.commit().await;
            assert!(matches!(raced, Err(StorageError::PreconditionFailed(_))));
        }
    }

    let corrupt_payload = Bytes::from_static(b"corrupt immutable leaf");
    let corrupt_id = object_id(&corrupt_payload);
    put_batch(
        storage,
        OBJECT_SPACE,
        vec![PutEntry {
            key: corrupt_id.key(),
            value: StoredValue {
                bytes: Bytes::from_static(b"different bytes"),
            },
        }],
    )
    .await;
    let raw = get_one(storage, OBJECT_SPACE, corrupt_id.key())
        .await
        .expect("corrupt fixture exists");
    assert!(decode_object(corrupt_id, &raw).is_err());

    let base_objects = reachable_objects(storage, fixture.base_root)
        .await
        .expect("base closure authenticates");
    let successor_objects = reachable_objects(storage, successor)
        .await
        .expect("successor closure authenticates");
    let shared = base_objects
        .intersection(&successor_objects)
        .copied()
        .collect::<BTreeSet<_>>();
    assert!(!shared.is_empty());
    let first_sweep = sweep_objects(storage, &[successor]).await;
    assert!(first_sweep > 0);
    for id in &shared {
        assert!(get_one(storage, OBJECT_SPACE, id.key()).await.is_some());
    }
    let second_sweep = sweep_objects(storage, &[]).await;
    assert!(second_sweep >= successor_objects.len());
    for id in successor_objects {
        assert!(get_one(storage, OBJECT_SPACE, id.key()).await.is_none());
    }

    let reopen_key = Key(Bytes::from_static(b"correctness-reopen"));
    let root = seed_blocked(storage, &fixture.base[..4 * CHUNK_BYTES]).await;
    seed_selectors(storage, std::slice::from_ref(&reopen_key), root).await;
    println!(
        "forktree_media_correctness,backend={backend},authenticated_decode=true,corruption_fail_closed=true,selector_cas=true,shared_survives=true,final_reference_reclaims=true"
    );
}

async fn assert_correctness_reopen<S: Storage>(storage: &S) {
    let key = Key(Bytes::from_static(b"correctness-reopen"));
    let raw = get_one(storage, SELECTOR_SPACE, key)
        .await
        .expect("correctness selector survives reopen");
    let root = ObjectId(raw.as_ref().try_into().expect("root width"));
    let bytes = reconstruct_blocked(storage, root).await;
    assert_eq!(bytes.len(), 4 * CHUNK_BYTES);
    println!("forktree_media_correctness_reopen,cold_reopen=true,exact_bytes=true");
}

async fn reachable_objects<S: Storage>(
    storage: &S,
    root: ObjectId,
) -> Result<BTreeSet<ObjectId>, String> {
    let mut marked = BTreeSet::new();
    let mut pending = vec![root];
    while let Some(id) = pending.pop() {
        if !marked.insert(id) {
            continue;
        }
        let raw = get_one(storage, OBJECT_SPACE, id.key())
            .await
            .ok_or_else(|| "reachable object is missing".to_string())?;
        match decode_object(id, &raw)? {
            Object::Leaf(_) => {}
            Object::Internal { children, .. } | Object::Flat { children, .. } => {
                pending.extend(children.into_iter().map(|child| child.id));
            }
        }
    }
    Ok(marked)
}

async fn sweep_objects<S: Storage>(storage: &S, roots: &[ObjectId]) -> usize {
    let mut marked = BTreeSet::new();
    for root in roots {
        marked.extend(
            reachable_objects(storage, *root)
                .await
                .expect("live closure authenticates before sweep"),
        );
    }
    let mut all = Vec::new();
    let mut resume_after = None;
    loop {
        let read = storage
            .begin_read(ReadOptions::default())
            .await
            .expect("begin object scan");
        let chunk = read
            .scan(
                OBJECT_SPACE,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                ScanOptions {
                    projection: CoreProjection::KeyOnly,
                    limit_rows: 1024,
                    resume_after: resume_after.clone(),
                },
            )
            .await
            .expect("scan objects");
        all.extend(chunk.entries.iter().map(|entry| entry.key.clone()));
        if !chunk.has_more {
            break;
        }
        resume_after = chunk.entries.last().map(|entry| entry.key.clone());
    }
    let dead = all
        .into_iter()
        .filter(|key| {
            let Ok(id) = <[u8; 32]>::try_from(key.0.as_ref()) else {
                return true;
            };
            !marked.contains(&ObjectId(id))
        })
        .collect::<Vec<_>>();
    if !dead.is_empty() {
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .expect("begin sweep");
        write
            .delete_many(OBJECT_SPACE, &dead)
            .await
            .expect("stage sweep");
        write.commit().await.expect("commit sweep");
    }
    dead.len()
}

fn deterministic_bytes(size: usize, seed: u64) -> Vec<u8> {
    let mut bytes = vec![0; size];
    for (index, chunk) in bytes.chunks_mut(CHUNK_BYTES).enumerate() {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"lix multimedia blocked tree benchmark bytes v1");
        hasher.update(&seed.to_be_bytes());
        hasher.update(&(index as u64).to_be_bytes());
        hasher.finalize_xof().fill(chunk);
    }
    bytes
}

fn print_seed(
    backend: &str,
    parameters: Parameters,
    fixture: &Fixture,
    io: IoStats,
    disk_bytes: u64,
) {
    println!(
        "forktree_media_seed,backend={backend},layout={},size_mib={},edit_percent={},base_root={},expected_root={},write_batches={},write_puts={},write_bytes={},disk_bytes={disk_bytes}",
        parameters.layout.name(),
        parameters.size_mib,
        parameters.edit_percent,
        hex_id(fixture.base_root),
        hex_id(fixture.expected_root),
        io.write_batches,
        io.write_puts,
        io.write_bytes,
    );
}

#[allow(clippy::too_many_arguments)]
fn print_sample(
    backend: &str,
    parameters: Parameters,
    sample: usize,
    wall_us: f64,
    cpu_us: f64,
    allocated_bytes: u64,
    allocation_calls: u64,
    rss_before: u64,
    rss_after: u64,
    high_water_before: u64,
    high_water_after: u64,
    io: &IoStats,
    physical: SlateDBIoSnapshot,
    disk_before: u64,
    disk_after: u64,
    hash_work: HashWork,
) {
    println!(
        "forktree_media,sample={sample},backend={backend},layout={},size_mib={},edit_percent={},wall_us={wall_us:.3},cpu_us={cpu_us:.3},alloc_bytes={allocated_bytes},alloc_calls={allocation_calls},rss_before_bytes={rss_before},rss_after_bytes={rss_after},rss_hwm_before_bytes={high_water_before},rss_hwm_after_bytes={high_water_after},get_calls={},get_keys={},get_values={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},write_batches={},write_puts={},write_deletes={},write_bytes={},disk_before_bytes={disk_before},disk_after_bytes={disk_after},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},hash_payload_bytes={},hash_object_bytes={},new_leaf_objects={},new_internal_objects={} ",
        parameters.layout.name(),
        parameters.size_mib,
        parameters.edit_percent,
        io.get_calls,
        io.get_keys,
        io.get_values,
        io.get_value_bytes,
        io.scan_calls,
        io.scan_entries,
        io.scan_value_bytes,
        io.write_batches,
        io.write_puts,
        io.write_deletes,
        io.write_bytes,
        physical.read_objects,
        physical.read_bytes,
        physical.write_objects,
        physical.write_bytes,
        hash_work.payload_bytes,
        hash_work.object_bytes,
        hash_work.leaf_objects,
        hash_work.internal_objects,
    );
}

fn projected_value_len(value: &ProjectedValue) -> usize {
    match value {
        ProjectedValue::KeyOnly => 0,
        ProjectedValue::FullValue(value) => value.len(),
    }
}

fn take_stats(stats: &Arc<Mutex<IoStats>>) -> IoStats {
    std::mem::take(&mut *stats.lock().expect("I/O stats mutex"))
}

fn begin_allocation_profile() {
    PROFILE_ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    PROFILE_ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    PROFILE_ALLOCATION_ENABLED.store(true, Ordering::Relaxed);
}

fn end_allocation_profile() -> (u64, u64) {
    PROFILE_ALLOCATION_ENABLED.store(false, Ordering::Relaxed);
    (
        PROFILE_ALLOCATED_BYTES.load(Ordering::Relaxed),
        PROFILE_ALLOCATION_CALLS.load(Ordering::Relaxed),
    )
}

fn process_resident_bytes() -> u64 {
    process_status_kib("VmRSS:")
}

fn process_high_water_bytes() -> u64 {
    process_status_kib("VmHWM:")
}

fn process_status_kib(label: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix(label))
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .map_or(0, |kilobytes| kilobytes.saturating_mul(1024))
}

fn process_cpu_nanos() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `time` is a valid writable timespec and the clock ID has no
    // lifetime or ownership requirements.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut time) };
    assert_eq!(result, 0, "read process CPU clock");
    (time.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(time.tv_nsec as u64)
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries.flatten().fold(0, |total, entry| {
        let path = entry.path();
        let bytes = if path.is_dir() {
            directory_bytes(&path)
        } else {
            entry.metadata().map(|metadata| metadata.len()).unwrap_or(0)
        };
        total.saturating_add(bytes)
    })
}

fn hex_id(id: ObjectId) -> String {
    id.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_positive(value: Option<&String>, name: &str, default: usize) -> usize {
    value.map_or(default, |value| {
        value
            .parse::<usize>()
            .unwrap_or_else(|error| panic!("invalid {name} '{value}': {error}"))
    })
}
