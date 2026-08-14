//! EXP-BEPSILON-11: authenticated root-buffered Bε-tree experiment.
//!
//! Compares identical Schema-v1 typed tuple bytes stored as either:
//! - immutable 64-row slotted snapshot pages with a rewritten page manifest; or
//! - one canonical C2 directory whose authenticated routing root carries a
//!   bounded sorted mutation buffer and flushes key intervals into C2 leaves.
//!
//! The benchmark uses the shipping RocksDB/SlateDB storage traits. There is no
//! SQL or workload-specific production shortcut: both layouts implement the
//! same point, mutation, full readback, diff, history, branch, corruption and
//! cold-reopen operations over content-addressed objects.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::time::Instant;

use bytes::Bytes;
use lix::storage::{
    BeginScanOptions, GetManyRequest, GetOptions, Key, KeyRange, ProjectedValue, PutBatch,
    PutEntry, ReadOptions, SpaceId, Storage, StorageRead, StorageSpace, StorageWrite, StoredValue,
    WriteOptions,
};
use lix_schema::value_layout::{BodyColumn, BodyKind, BodyValue, decode_body, encode_body};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use uuid::Uuid;

const LEDGER: &str = "EXP-BEPSILON-11";
const PAGE_ROWS: usize = 64;
const ROOT_SPACE: StorageSpace =
    StorageSpace::immutable(SpaceId(0x00fe_2001), "exp.delta_page.root");
const PAGE_SPACE: StorageSpace =
    StorageSpace::immutable(SpaceId(0x00fe_2002), "exp.delta_page.page");
const SELECTOR_SPACE: StorageSpace =
    StorageSpace::mutable(SpaceId(0x00fe_2003), "exp.delta_page.selector");

const TUPLE_PLAN: [BodyColumn; 5] = [
    BodyColumn {
        kind: BodyKind::Uuid,
        nullable: false,
    },
    BodyColumn {
        kind: BodyKind::Int8,
        nullable: false,
    },
    BodyColumn {
        kind: BodyKind::Boolean,
        nullable: false,
    },
    BodyColumn {
        kind: BodyKind::Timestamptz,
        nullable: false,
    },
    BodyColumn {
        kind: BodyKind::Text,
        nullable: true,
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Layout {
    Slotted,
    Bepsilon,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Self::Slotted => "slotted",
            Self::Bepsilon => "bepsilon",
        }
    }

    fn tag(self) -> u8 {
        match self {
            Self::Slotted => 1,
            Self::Bepsilon => 2,
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "slotted" => Self::Slotted,
            "bepsilon" => Self::Bepsilon,
            other => panic!("unknown layout {other}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Counters {
    get_calls: u64,
    get_keys: u64,
    get_bytes: u64,
    put_calls: u64,
    puts: u64,
    put_bytes: u64,
    decoded_pages: u64,
    decoded_rows: u64,
    buffer_entries: u64,
    buffer_flushes: u64,
    flushed_pages: u64,
}

impl Counters {
    fn delta(self, before: Self) -> Self {
        Self {
            get_calls: self.get_calls - before.get_calls,
            get_keys: self.get_keys - before.get_keys,
            get_bytes: self.get_bytes - before.get_bytes,
            put_calls: self.put_calls - before.put_calls,
            puts: self.puts - before.puts,
            put_bytes: self.put_bytes - before.put_bytes,
            decoded_pages: self.decoded_pages - before.decoded_pages,
            decoded_rows: self.decoded_rows - before.decoded_rows,
            buffer_entries: self.buffer_entries - before.buffer_entries,
            buffer_flushes: self.buffer_flushes - before.buffer_flushes,
            flushed_pages: self.flushed_pages - before.flushed_pages,
        }
    }

    fn accumulate(&mut self, other: Self) {
        self.get_calls += other.get_calls;
        self.get_keys += other.get_keys;
        self.get_bytes += other.get_bytes;
        self.put_calls += other.put_calls;
        self.puts += other.puts;
        self.put_bytes += other.put_bytes;
        self.decoded_pages += other.decoded_pages;
        self.decoded_rows += other.decoded_rows;
        self.buffer_entries += other.buffer_entries;
        self.buffer_flushes += other.buffer_flushes;
        self.flushed_pages += other.flushed_pages;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ObjectId([u8; 32]);

impl ObjectId {
    fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    fn key(self) -> Key {
        Key(Bytes::copy_from_slice(&self.0))
    }
}

#[derive(Clone, Debug)]
struct Root {
    layout: Layout,
    count: u64,
    /// Sole current-state owner: immutable schema-partitioned pages.
    pages: Vec<ObjectId>,
    /// Authenticated mutation buffer carried by this routing root object.
    buffer: BTreeMap<u64, BufferedMutation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BufferedMutation {
    Value(Vec<u8>),
    Tombstone,
}

#[derive(Clone, Debug)]
struct Config {
    backends: Vec<String>,
    layouts: Vec<Layout>,
    sizes: Vec<usize>,
    histories: Vec<usize>,
    deltas: Vec<DeltaShape>,
    root: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug)]
enum DeltaShape {
    Fixed(usize),
    Percent(u32),
}

impl DeltaShape {
    fn rows(self, n: usize) -> usize {
        match self {
            Self::Fixed(value) => value.min(n),
            Self::Percent(value) => n.saturating_mul(value as usize).div_ceil(100).max(1),
        }
    }

    fn name(self) -> String {
        match self {
            Self::Fixed(value) => value.to_string(),
            Self::Percent(value) => format!("{value}pct"),
        }
    }
}

struct Store<S> {
    inner: S,
    counters: Counters,
}

struct Reader<'a, R> {
    inner: R,
    counters: &'a mut Counters,
}

impl<R: StorageRead> Reader<'_, R> {
    async fn get_many(
        &mut self,
        space: StorageSpace,
        ids: &[ObjectId],
    ) -> Result<Vec<Vec<u8>>, String> {
        self.counters.get_calls += 1;
        self.counters.get_keys += ids.len() as u64;
        let keys = ids.iter().copied().map(ObjectId::key).collect::<Vec<_>>();
        let result = self
            .inner
            .get_many(&[GetManyRequest {
                space,
                keys: &keys,
                opts: GetOptions::default(),
            }])
            .await
            .map_err(|e| e.to_string())?;
        result
            .values
            .into_iter()
            .zip(ids)
            .map(|(value, expected)| {
                let bytes = match value {
                    Some(ProjectedValue::FullValue(bytes)) => bytes.to_vec(),
                    Some(ProjectedValue::KeyOnly) => {
                        return Err("unexpected key-only object".to_owned());
                    }
                    None => return Err("missing authenticated object".to_owned()),
                };
                self.counters.get_bytes += bytes.len() as u64;
                if ObjectId::of(&bytes) != *expected {
                    return Err("content-addressed object authentication failed".to_owned());
                }
                Ok(bytes)
            })
            .collect()
    }

    async fn get_one(&mut self, space: StorageSpace, id: ObjectId) -> Result<Vec<u8>, String> {
        self.get_many(space, &[id])
            .await?
            .pop()
            .ok_or_else(|| "missing result slot".to_owned())
    }
}

impl<S: Storage + Clone> Store<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            counters: Counters::default(),
        }
    }

    async fn reader(&mut self) -> Result<Reader<'_, S::Read<'_>>, String> {
        let read = self
            .inner
            .begin_read(ReadOptions::default())
            .await
            .map_err(|e| e.to_string())?;
        Ok(Reader {
            inner: read,
            counters: &mut self.counters,
        })
    }

    async fn get_many(
        &mut self,
        space: StorageSpace,
        ids: &[ObjectId],
    ) -> Result<Vec<Vec<u8>>, String> {
        self.counters.get_calls += 1;
        self.counters.get_keys += ids.len() as u64;
        let keys = ids.iter().copied().map(ObjectId::key).collect::<Vec<_>>();
        let read = self
            .inner
            .begin_read(ReadOptions::default())
            .await
            .map_err(|e| e.to_string())?;
        let result = read
            .get_many(&[GetManyRequest {
                space,
                keys: &keys,
                opts: GetOptions::default(),
            }])
            .await
            .map_err(|e| e.to_string())?;
        result
            .values
            .into_iter()
            .zip(ids)
            .map(|(value, expected)| {
                let bytes = match value {
                    Some(ProjectedValue::FullValue(bytes)) => bytes.to_vec(),
                    Some(ProjectedValue::KeyOnly) => {
                        return Err("unexpected key-only object".to_owned());
                    }
                    None => return Err("missing authenticated object".to_owned()),
                };
                self.counters.get_bytes += bytes.len() as u64;
                if ObjectId::of(&bytes) != *expected {
                    return Err("content-addressed object authentication failed".to_owned());
                }
                Ok(bytes)
            })
            .collect()
    }

    async fn get_one(&mut self, space: StorageSpace, id: ObjectId) -> Result<Vec<u8>, String> {
        self.get_many(space, &[id])
            .await?
            .pop()
            .ok_or_else(|| "missing result slot".to_owned())
    }

    async fn put_objects(
        &mut self,
        space: StorageSpace,
        objects: &[(ObjectId, Vec<u8>)],
    ) -> Result<(), String> {
        if objects.is_empty() {
            return Ok(());
        }
        for (id, bytes) in objects {
            if ObjectId::of(bytes) != *id {
                return Err("attempted noncanonical object put".to_owned());
            }
        }
        self.counters.put_calls += 1;
        self.counters.puts += objects.len() as u64;
        self.counters.put_bytes += objects
            .iter()
            .map(|(id, bytes)| id.0.len() + bytes.len())
            .sum::<usize>() as u64;
        let mut write = self
            .inner
            .begin_write(WriteOptions::default())
            .await
            .map_err(|e| e.to_string())?;
        write
            .put_many(
                space,
                PutBatch {
                    entries: objects
                        .iter()
                        .map(|(id, bytes)| PutEntry {
                            key: id.key(),
                            value: StoredValue {
                                bytes: Bytes::copy_from_slice(bytes),
                            },
                        })
                        .collect(),
                },
            )
            .await
            .map_err(|e| e.to_string())?;
        write.commit().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn publish_selector(&mut self, branch: u64, root: ObjectId) -> Result<(), String> {
        self.counters.put_calls += 1;
        self.counters.puts += 1;
        self.counters.put_bytes += 8 + 32;
        let mut write = self
            .inner
            .begin_write(WriteOptions::default())
            .await
            .map_err(|e| e.to_string())?;
        write
            .put_many(
                SELECTOR_SPACE,
                PutBatch {
                    entries: vec![PutEntry {
                        key: Key(Bytes::copy_from_slice(&branch.to_be_bytes())),
                        value: StoredValue {
                            bytes: Bytes::copy_from_slice(&root.0),
                        },
                    }],
                },
            )
            .await
            .map_err(|e| e.to_string())?;
        write.commit().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn raw_scan_bytes(&self, space: StorageSpace) -> Result<(u64, u64), String> {
        let read = self
            .inner
            .begin_read(ReadOptions::default())
            .await
            .map_err(|e| e.to_string())?;
        let mut scan = read
            .begin_scan(
                space,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                BeginScanOptions::default(),
            )
            .await
            .map_err(|e| e.to_string())?;
        let mut rows = 0u64;
        let mut bytes = 0u64;
        loop {
            let page = scan.next_page(1024).await.map_err(|e| e.to_string())?;
            let (entries, more) = page.into_parts();
            for entry in entries {
                rows += 1;
                bytes += entry.key.0.len() as u64;
                if let ProjectedValue::FullValue(value) = entry.value {
                    bytes += value.len() as u64;
                }
            }
            if !more {
                break;
            }
        }
        Ok((rows, bytes))
    }
}

#[tokio::main]
async fn main() {
    let config = parse_config();
    buffer_codec_controls();
    println!(
        "{LEDGER},kind=control,control=buffer_codec_corruption,status=pass,cases=ordering+duplicate+outside_fence+truncation+tombstone+null+canonical_order"
    );
    println!(
        "{LEDGER},kind=config,page_rows={PAGE_ROWS},buffer_entries={},buffer_bytes={},pk_kind={},pattern={},sizes={:?},histories={:?}",
        buffer_entry_cap(),
        buffer_byte_cap(),
        pk_kind(),
        mutation_pattern(),
        config.sizes,
        config.histories
    );
    let mut case = 0usize;
    for backend in &config.backends {
        for &n in &config.sizes {
            for &history in &config.histories {
                for &delta in &config.deltas {
                    for &layout in &config.layouts {
                        match backend.as_str() {
                            "rocksdb" => run_rocks(&config, case, n, history, delta, layout).await,
                            "slatedb" => run_slate(&config, case, n, history, delta, layout).await,
                            other => panic!("unknown backend {other}"),
                        }
                    }
                    case += 1;
                }
            }
        }
    }
}

async fn run_rocks(
    config: &Config,
    case: usize,
    n: usize,
    h: usize,
    d: DeltaShape,
    layout: Layout,
) {
    let owned;
    let path = if let Some(root) = &config.root {
        let path = root.join(format!("{case}-rocks-{}", layout.name()));
        std::fs::create_dir_all(&path).unwrap();
        path
    } else {
        owned = tempfile::tempdir().unwrap();
        owned.path().to_owned()
    };
    let db = RocksDB::open(&path).unwrap();
    let reopen = run_case("rocksdb", &path, Store::new(db.clone()), n, h, d, layout).await;
    drop(db);
    let db = RocksDB::open(&path).unwrap();
    verify_reopen("rocksdb", &path, Store::new(db), reopen).await;
}

async fn run_slate(
    config: &Config,
    case: usize,
    n: usize,
    h: usize,
    d: DeltaShape,
    layout: Layout,
) {
    let owned;
    let path = if let Some(root) = &config.root {
        let path = root.join(format!("{case}-slate-{}", layout.name()));
        std::fs::create_dir_all(&path).unwrap();
        path
    } else {
        owned = tempfile::tempdir().unwrap();
        owned.path().to_owned()
    };
    let db = SlateDB::open(&path).unwrap();
    let reopen = run_case("slatedb", &path, Store::new(db.clone()), n, h, d, layout).await;
    db.flush_memtable_for_diagnostics().await.unwrap();
    drop(db);
    let db = SlateDB::open(&path).unwrap();
    verify_reopen("slatedb", &path, Store::new(db), reopen).await;
}

#[derive(Clone)]
struct ReopenState {
    root: ObjectId,
    expected_digest: ObjectId,
}

async fn run_case<S: Storage + Clone>(
    backend: &str,
    path: &Path,
    mut store: Store<S>,
    n: usize,
    h: usize,
    delta: DeltaShape,
    layout: Layout,
) -> ReopenState {
    let mut expected = (0..n as u64)
        .map(|key| (key, tuple(key, 0)))
        .collect::<BTreeMap<_, _>>();
    let mut root = build_initial(&mut store, layout, &expected).await.unwrap();
    store.publish_selector(0, root).await.unwrap();
    let base_root = root;
    let mut history_roots = vec![root];
    let mut history_hot_keys = vec![0u64];
    let mut ever_touched = BTreeSet::new();
    let mut last_touched = 0u64;
    let mut update_wall_us = 0u128;
    let mut update_samples = Vec::with_capacity(h);
    let mut update_counters = Counters::default();
    let d = delta.rows(n);
    for generation in 1..=h as u64 {
        let mutations = mutation_keys(n, d, generation)
            .into_iter()
            .map(|key| {
                ever_touched.insert(key);
                last_touched = key;
                let value = tuple(key, generation);
                expected.insert(key, value.clone());
                (key, value)
            })
            .collect::<BTreeMap<_, _>>();
        let before = store.counters;
        let started = Instant::now();
        root = commit(&mut store, root, &mut expected, &mutations, generation)
            .await
            .unwrap();
        store.publish_selector(0, root).await.unwrap();
        let elapsed = started.elapsed().as_micros();
        update_samples.push(elapsed);
        let operation_counters = store.counters.delta(before);
        update_wall_us += elapsed;
        update_counters.accumulate(operation_counters);
        if generation == 1 || generation == h as u64 {
            emit(
                backend,
                path,
                layout,
                n,
                h,
                d,
                "update",
                elapsed,
                operation_counters,
            );
        }
        history_roots.push(root);
        history_hot_keys.push(last_touched);
    }
    emit(
        backend,
        path,
        layout,
        n,
        h,
        d,
        "update_series",
        update_wall_us,
        update_counters,
    );
    emit_latency_summary(backend, layout, n, h, d, "update", &update_samples);

    let point_key = (n / 2) as u64;
    let before = store.counters;
    let started = Instant::now();
    let value = point(&mut store, root, point_key).await.unwrap();
    assert_eq!(value.as_ref(), expected.get(&point_key));
    emit(
        backend,
        path,
        layout,
        n,
        h,
        d,
        "point",
        started.elapsed().as_micros(),
        store.counters.delta(before),
    );

    let cold_key = (0..n as u64)
        .find(|key| !ever_touched.contains(key))
        .unwrap_or(point_key);
    for (operation, key) in [("point_hot", last_touched), ("point_cold", cold_key)] {
        let before = store.counters;
        let started = Instant::now();
        let value = point(&mut store, root, key).await.unwrap();
        assert_eq!(value.as_ref(), expected.get(&key));
        emit(
            backend,
            path,
            layout,
            n,
            h,
            d,
            operation,
            started.elapsed().as_micros(),
            store.counters.delta(before),
        );
        let mut samples = Vec::with_capacity(20);
        for _ in 0..20 {
            let started = Instant::now();
            let sampled = point(&mut store, root, key).await.unwrap();
            assert_eq!(sampled.as_ref(), expected.get(&key));
            samples.push(started.elapsed().as_micros());
        }
        emit_latency_summary(backend, layout, n, h, d, operation, &samples);
    }

    let missing_key = n as u64 + 1;
    let before = store.counters;
    let started = Instant::now();
    assert!(
        point(&mut store, root, missing_key)
            .await
            .unwrap()
            .is_none()
    );
    emit(
        backend,
        path,
        layout,
        n,
        h,
        d,
        "point_miss",
        started.elapsed().as_micros(),
        store.counters.delta(before),
    );
    let mut miss_samples = Vec::with_capacity(20);
    for _ in 0..20 {
        let started = Instant::now();
        assert!(
            point(&mut store, root, missing_key)
                .await
                .unwrap()
                .is_none()
        );
        miss_samples.push(started.elapsed().as_micros());
    }
    emit_latency_summary(backend, layout, n, h, d, "point_miss", &miss_samples);

    for (operation, keys) in [
        ("point_hot_series", history_hot_keys.clone()),
        ("point_cold_series", vec![cold_key; history_roots.len()]),
    ] {
        let before = store.counters;
        let started = Instant::now();
        for (root_id, key) in history_roots.iter().zip(keys) {
            let value = point(&mut store, *root_id, key).await.unwrap();
            assert!(value.is_some());
        }
        emit(
            backend,
            path,
            layout,
            n,
            h,
            d,
            operation,
            started.elapsed().as_micros(),
            store.counters.delta(before),
        );
    }

    let before = store.counters;
    let started = Instant::now();
    let actual = materialize(&mut store, root).await.unwrap();
    assert_eq!(actual, expected);
    emit(
        backend,
        path,
        layout,
        n,
        h,
        d,
        "readback",
        started.elapsed().as_micros(),
        store.counters.delta(before),
    );

    let range_start = (n / 4) as u64;
    let range_end = (range_start + 100).min(n as u64);
    let before = store.counters;
    let started = Instant::now();
    let ranged = range(&mut store, root, range_start, range_end)
        .await
        .unwrap();
    assert_eq!(
        ranged,
        expected
            .range(range_start..range_end)
            .map(|(key, value)| (*key, value.clone()))
            .collect()
    );
    emit(
        backend,
        path,
        layout,
        n,
        h,
        d,
        "range_100",
        started.elapsed().as_micros(),
        store.counters.delta(before),
    );

    let before = store.counters;
    let started = Instant::now();
    let changed = diff(&mut store, base_root, root).await.unwrap();
    let expected_changed = expected
        .iter()
        .filter(|(key, value)| **value != tuple(**key, 0))
        .count();
    assert_eq!(changed.len(), expected_changed);
    emit(
        backend,
        path,
        layout,
        n,
        h,
        d,
        "diff",
        started.elapsed().as_micros(),
        store.counters.delta(before),
    );

    let before = store.counters;
    let started = Instant::now();
    let mut history_digest = blake3::Hasher::new();
    for value in history_values(&mut store, &history_roots, point_key)
        .await
        .unwrap()
    {
        if let Some(value) = value {
            history_digest.update(&value);
        }
    }
    assert!(
        !history_digest
            .finalize()
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
    );
    emit(
        backend,
        path,
        layout,
        n,
        h,
        d,
        "history",
        started.elapsed().as_micros(),
        store.counters.delta(before),
    );

    let before = store.counters;
    let started = Instant::now();
    store.publish_selector(1, root).await.unwrap();
    let key = (n / 3) as u64;
    let mutation = BTreeMap::from([(key, tuple(key, h as u64 + 1))]);
    let mut branch_expected = expected.clone();
    branch_expected.insert(key, mutation[&key].clone());
    let branch_root = commit(
        &mut store,
        root,
        &mut branch_expected,
        &mutation,
        h as u64 + 1,
    )
    .await
    .unwrap();
    store.publish_selector(1, branch_root).await.unwrap();
    assert_ne!(
        point(&mut store, root, key).await.unwrap(),
        point(&mut store, branch_root, key).await.unwrap()
    );
    emit(
        backend,
        path,
        layout,
        n,
        h,
        d,
        "branch",
        started.elapsed().as_micros(),
        store.counters.delta(before),
    );

    corruption_control(&mut store, root).await.unwrap();
    println!(
        "{LEDGER},kind=control,backend={backend},layout={},n={n},control=authenticated_corruption,status=pass,cases=missing_object+wrong_child+root_substitution",
        layout.name()
    );
    let (root_objects, root_bytes) = store.raw_scan_bytes(ROOT_SPACE).await.unwrap();
    let (page_objects, page_bytes) = store.raw_scan_bytes(PAGE_SPACE).await.unwrap();
    println!(
        "{LEDGER},kind=inventory,backend={backend},layout={},n={n},h={h},d={},root_objects={root_objects},root_bytes={root_bytes},page_objects={page_objects},page_bytes={page_bytes},settled_bytes={}",
        layout.name(),
        delta.name(),
        directory_bytes(path)
    );
    ReopenState {
        root,
        expected_digest: map_digest(&expected),
    }
}

async fn verify_reopen<S: Storage + Clone>(
    backend: &str,
    path: &Path,
    mut store: Store<S>,
    state: ReopenState,
) {
    let before = store.counters;
    let started = Instant::now();
    let actual = materialize(&mut store, state.root).await.unwrap();
    assert_eq!(map_digest(&actual), state.expected_digest);
    emit(
        backend,
        path,
        decode_root(&store.get_one(ROOT_SPACE, state.root).await.unwrap())
            .unwrap()
            .layout,
        actual.len(),
        0,
        0,
        "cold_reopen",
        started.elapsed().as_micros(),
        store.counters.delta(before),
    );
}

fn emit(
    backend: &str,
    path: &Path,
    layout: Layout,
    n: usize,
    h: usize,
    d: usize,
    operation: &str,
    wall_us: u128,
    c: Counters,
) {
    println!(
        "{LEDGER},kind=operation,backend={backend},layout={},n={n},h={h},d={d},operation={operation},wall_us={wall_us},get_calls={},get_keys={},get_bytes={},put_calls={},puts={},put_bytes={},decoded_pages={},decoded_rows={},buffer_entries={},buffer_flushes={},flushed_pages={},settled_bytes={}",
        layout.name(),
        c.get_calls,
        c.get_keys,
        c.get_bytes,
        c.put_calls,
        c.puts,
        c.put_bytes,
        c.decoded_pages,
        c.decoded_rows,
        c.buffer_entries,
        c.buffer_flushes,
        c.flushed_pages,
        directory_bytes(path)
    );
}

fn emit_latency_summary(
    backend: &str,
    layout: Layout,
    n: usize,
    h: usize,
    d: usize,
    operation: &str,
    samples: &[u128],
) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let percentile = |numerator: usize| {
        let index = (sorted.len() * numerator).div_ceil(100).saturating_sub(1);
        sorted[index.min(sorted.len() - 1)]
    };
    println!(
        "{LEDGER},kind=latency_summary,backend={backend},layout={},n={n},h={h},d={d},operation={operation},samples={},p50_us={},p95_us={}",
        layout.name(),
        sorted.len(),
        percentile(50),
        percentile(95)
    );
}

async fn build_initial<S: Storage + Clone>(
    store: &mut Store<S>,
    layout: Layout,
    rows: &BTreeMap<u64, Vec<u8>>,
) -> Result<ObjectId, String> {
    let root = match layout {
        Layout::Slotted => {
            let pages = encode_pages(rows);
            store.put_objects(PAGE_SPACE, &pages).await?;
            Root {
                layout,
                count: rows.len() as u64,
                pages: pages.iter().map(|(id, _)| *id).collect(),
                buffer: BTreeMap::new(),
            }
        }
        Layout::Bepsilon => {
            let pages = encode_pages(rows);
            store.put_objects(PAGE_SPACE, &pages).await?;
            Root {
                layout,
                count: rows.len() as u64,
                pages: pages.iter().map(|(id, _)| *id).collect(),
                buffer: BTreeMap::new(),
            }
        }
    };
    put_root(store, &root).await
}

async fn commit<S: Storage + Clone>(
    store: &mut Store<S>,
    current: ObjectId,
    full: &mut BTreeMap<u64, Vec<u8>>,
    mutations: &BTreeMap<u64, Vec<u8>>,
    _generation: u64,
) -> Result<ObjectId, String> {
    let root = load_root(store, current).await?;
    match root.layout {
        Layout::Slotted => {
            let mut page_ids = root.pages.clone();
            let touched = mutations
                .keys()
                .map(|key| (*key as usize) / PAGE_ROWS)
                .collect::<BTreeSet<_>>();
            let ids = touched
                .iter()
                .filter_map(|index| page_ids.get(*index).copied())
                .collect::<Vec<_>>();
            let encoded = store.get_many(PAGE_SPACE, &ids).await?;
            let mut staged = Vec::new();
            for (index, bytes) in touched.iter().zip(encoded) {
                let mut page = decode_page_counted(&mut store.counters, &bytes)?;
                for (key, value) in mutations
                    .range(page.first_key_value().unwrap().0..=page.last_key_value().unwrap().0)
                {
                    page.insert(*key, value.clone());
                }
                let bytes = encode_page(&page);
                let id = ObjectId::of(&bytes);
                page_ids[*index] = id;
                staged.push((id, bytes));
            }
            store.put_objects(PAGE_SPACE, &staged).await?;
            put_root(
                store,
                &Root {
                    layout: root.layout,
                    count: full.len() as u64,
                    pages: page_ids,
                    buffer: BTreeMap::new(),
                },
            )
            .await
        }
        Layout::Bepsilon => {
            let mut next = root.clone();
            for (key, value) in mutations {
                next.buffer
                    .insert(*key, BufferedMutation::Value(value.clone()));
            }
            store.counters.buffer_entries += next.buffer.len() as u64;
            if should_flush_buffer(&next.buffer) {
                flush_root_buffer(store, &mut next).await?;
            }
            put_root(store, &next).await
        }
    }
}

async fn flush_root_buffer<S: Storage + Clone>(
    store: &mut Store<S>,
    root: &mut Root,
) -> Result<(), String> {
    if root.buffer.is_empty() {
        return Ok(());
    }
    let touched = root
        .buffer
        .keys()
        .map(|key| *key as usize / PAGE_ROWS)
        .collect::<BTreeSet<_>>();
    if touched
        .last()
        .is_some_and(|index| *index >= root.pages.len())
    {
        return Err("buffer flush key lies outside the root child fences".to_owned());
    }
    let page_ids = touched
        .iter()
        .map(|index| root.pages[*index])
        .collect::<Vec<_>>();
    let objects = store.get_many(PAGE_SPACE, &page_ids).await?;
    let mut staged = Vec::new();
    for (page_index, bytes) in touched.into_iter().zip(objects) {
        let mut page = decode_page_counted(&mut store.counters, &bytes)?;
        validate_page_index(&page, page_index)?;
        let start = (page_index * PAGE_ROWS) as u64;
        let end = start + PAGE_ROWS as u64;
        let mut changed = false;
        for (key, mutation) in root.buffer.range(start..end) {
            changed = true;
            match mutation {
                BufferedMutation::Value(value) => {
                    page.insert(*key, value.clone());
                }
                BufferedMutation::Tombstone => {
                    page.remove(key);
                }
            }
        }
        if changed {
            let encoded = encode_page(&page);
            let id = ObjectId::of(&encoded);
            root.pages[page_index] = id;
            staged.push((id, encoded));
        }
    }
    store.put_objects(PAGE_SPACE, &staged).await?;
    root.buffer.clear();
    store.counters.buffer_flushes += 1;
    store.counters.flushed_pages += staged.len() as u64;
    Ok(())
}

async fn point<S: Storage + Clone>(
    store: &mut Store<S>,
    root_id: ObjectId,
    key: u64,
) -> Result<Option<Vec<u8>>, String> {
    let mut read = store.reader().await?;
    point_on_read(&mut read, root_id, key).await
}

async fn point_on_read<R: StorageRead>(
    store: &mut Reader<'_, R>,
    root_id: ObjectId,
    key: u64,
) -> Result<Option<Vec<u8>>, String> {
    let root = load_root_on_read(store, root_id).await?;
    if let Some(mutation) = root.buffer.get(&key) {
        return Ok(match mutation {
            BufferedMutation::Value(value) => Some(value.clone()),
            BufferedMutation::Tombstone => None,
        });
    }
    let page_index = key as usize / PAGE_ROWS;
    let Some(page_id) = root.pages.get(page_index).copied() else {
        return Ok(None);
    };
    let bytes = store.get_one(PAGE_SPACE, page_id).await?;
    let page = decode_page_counted(store.counters, &bytes)?;
    validate_page_index(&page, page_index)?;
    Ok(page.get(&key).cloned())
}

async fn materialize<S: Storage + Clone>(
    store: &mut Store<S>,
    root_id: ObjectId,
) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let mut read = store.reader().await?;
    materialize_on_read(&mut read, root_id).await
}

async fn range<S: Storage + Clone>(
    store: &mut Store<S>,
    root_id: ObjectId,
    start: u64,
    end: u64,
) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let mut read = store.reader().await?;
    let root = load_root_on_read(&mut read, root_id).await?;
    if start >= end {
        return Ok(BTreeMap::new());
    }
    let first = start as usize / PAGE_ROWS;
    let last = (end.saturating_sub(1) as usize / PAGE_ROWS).min(root.pages.len() - 1);
    let objects = read.get_many(PAGE_SPACE, &root.pages[first..=last]).await?;
    let mut rows = BTreeMap::new();
    for (offset, bytes) in objects.iter().enumerate() {
        let page = decode_page_counted(read.counters, bytes)?;
        validate_page_index(&page, first + offset)?;
        rows.extend(page);
    }
    apply_buffer_range(&mut rows, &root.buffer, start, end);
    Ok(rows
        .range(start..end)
        .map(|(k, v)| (*k, v.clone()))
        .collect())
}

async fn materialize_on_read<R: StorageRead>(
    store: &mut Reader<'_, R>,
    root_id: ObjectId,
) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let root = load_root_on_read(store, root_id).await?;
    let mut rows = BTreeMap::new();
    let objects = store.get_many(PAGE_SPACE, &root.pages).await?;
    for (index, bytes) in objects.iter().enumerate() {
        let page = decode_page_counted(store.counters, bytes)?;
        validate_page_index(&page, index)?;
        rows.extend(page);
    }
    apply_buffer_range(&mut rows, &root.buffer, 0, u64::MAX);
    Ok(rows)
}

async fn diff<S: Storage + Clone>(
    store: &mut Store<S>,
    before: ObjectId,
    after: ObjectId,
) -> Result<BTreeSet<u64>, String> {
    let mut read = store.reader().await?;
    diff_on_read(&mut read, before, after).await
}

async fn diff_on_read<R: StorageRead>(
    store: &mut Reader<'_, R>,
    before: ObjectId,
    after: ObjectId,
) -> Result<BTreeSet<u64>, String> {
    if before == after {
        return Ok(BTreeSet::new());
    }
    let before_root = load_root_on_read(store, before).await?;
    let after_root = load_root_on_read(store, after).await?;
    if before_root.layout != after_root.layout {
        return Err("cannot diff roots from different layouts".to_owned());
    }
    match before_root.layout {
        Layout::Slotted => diff_pages(store, &before_root, &after_root).await,
        Layout::Bepsilon => diff_buffered(store, &before_root, &after_root).await,
    }
}

async fn diff_pages<R: StorageRead>(
    store: &mut Reader<'_, R>,
    before: &Root,
    after: &Root,
) -> Result<BTreeSet<u64>, String> {
    if before.pages.len() != after.pages.len() {
        return Err("slotted roots have incompatible page geometry".to_owned());
    }
    let mut changed = BTreeSet::new();
    for (index, (before_id, after_id)) in before.pages.iter().zip(&after.pages).enumerate() {
        if before_id == after_id {
            continue;
        }
        let bytes = store.get_many(PAGE_SPACE, &[*before_id, *after_id]).await?;
        let before_page = decode_visible_page_counted(store.counters, &bytes[0], index)?;
        let after_page = decode_visible_page_counted(store.counters, &bytes[1], index)?;
        changed.extend(
            before_page
                .keys()
                .chain(after_page.keys())
                .copied()
                .filter(|key| before_page.get(key) != after_page.get(key)),
        );
    }
    Ok(changed)
}

async fn diff_buffered<R: StorageRead>(
    store: &mut Reader<'_, R>,
    before: &Root,
    after: &Root,
) -> Result<BTreeSet<u64>, String> {
    if before.pages.len() != after.pages.len() {
        return Err("buffered roots have incompatible child geometry".to_owned());
    }
    let mut page_indices = before
        .pages
        .iter()
        .zip(&after.pages)
        .enumerate()
        .filter_map(|(index, (left, right))| (left != right).then_some(index))
        .collect::<BTreeSet<_>>();
    page_indices.extend(
        before
            .buffer
            .keys()
            .chain(after.buffer.keys())
            .map(|key| *key as usize / PAGE_ROWS),
    );

    let mut object_ids = BTreeSet::new();
    for index in &page_indices {
        let before_id = before
            .pages
            .get(*index)
            .ok_or_else(|| "before buffer key lies outside child geometry".to_owned())?;
        let after_id = after
            .pages
            .get(*index)
            .ok_or_else(|| "after buffer key lies outside child geometry".to_owned())?;
        object_ids.insert(*before_id);
        object_ids.insert(*after_id);
    }
    let object_ids = object_ids.into_iter().collect::<Vec<_>>();
    let objects = store.get_many(PAGE_SPACE, &object_ids).await?;
    let decoded = object_ids
        .into_iter()
        .zip(objects)
        .map(|(id, bytes)| Ok((id, decode_page_counted(store.counters, &bytes)?)))
        .collect::<Result<BTreeMap<_, _>, String>>()?;

    let mut changed = BTreeSet::new();
    for index in page_indices {
        let before_page = decoded.get(&before.pages[index]).unwrap();
        let after_page = decoded.get(&after.pages[index]).unwrap();
        validate_page_index(before_page, index)?;
        validate_page_index(after_page, index)?;
        let start = (index * PAGE_ROWS) as u64;
        let end = start + PAGE_ROWS as u64;
        let candidates = before_page
            .keys()
            .chain(after_page.keys())
            .chain(before.buffer.range(start..end).map(|(key, _)| key))
            .chain(after.buffer.range(start..end).map(|(key, _)| key))
            .copied()
            .collect::<BTreeSet<_>>();
        for key in candidates {
            let before_value = visible_value(before_page, &before.buffer, key);
            let after_value = visible_value(after_page, &after.buffer, key);
            if before_value != after_value {
                changed.insert(key);
            }
        }
    }
    Ok(changed)
}

fn visible_value<'a>(
    page: &'a BTreeMap<u64, Vec<u8>>,
    buffer: &'a BTreeMap<u64, BufferedMutation>,
    key: u64,
) -> Option<&'a Vec<u8>> {
    match buffer.get(&key) {
        Some(BufferedMutation::Value(value)) => Some(value),
        Some(BufferedMutation::Tombstone) => None,
        None => page.get(&key),
    }
}

async fn history_values<S: Storage + Clone>(
    store: &mut Store<S>,
    roots: &[ObjectId],
    key: u64,
) -> Result<Vec<Option<Vec<u8>>>, String> {
    let mut read = store.reader().await?;
    history_values_on_read(&mut read, roots, key).await
}

async fn history_values_on_read<R: StorageRead>(
    store: &mut Reader<'_, R>,
    roots: &[ObjectId],
    key: u64,
) -> Result<Vec<Option<Vec<u8>>>, String> {
    let mut values = Vec::with_capacity(roots.len());
    for root in roots {
        values.push(point_on_read(store, *root, key).await?);
    }
    Ok(values)
}

async fn corruption_control<S: Storage + Clone>(
    store: &mut Store<S>,
    root: ObjectId,
) -> Result<(), String> {
    let root_bytes = store.get_one(ROOT_SPACE, root).await?;
    let decoded = decode_root(&root_bytes)?;
    let wrong = ObjectId([0x55; 32]);
    let mut corrupted = decoded.clone();
    let retained = decoded
        .pages
        .first()
        .copied()
        .ok_or_else(|| "state root has no page".to_owned())?;
    corrupted.pages[0] = wrong;
    let corrupt_bytes = encode_root(&corrupted);
    let corrupt_id = ObjectId::of(&corrupt_bytes);
    store
        .put_objects(ROOT_SPACE, &[(corrupt_id, corrupt_bytes)])
        .await?;
    assert!(materialize(store, corrupt_id).await.is_err());
    assert!(store.get_one(PAGE_SPACE, retained).await.is_ok());
    if decoded.layout == Layout::Bepsilon && decoded.pages.len() >= 2 {
        let mut substituted = decoded.clone();
        substituted.pages[0] = substituted.pages[1];
        let root_bytes = encode_root(&substituted);
        let root_id = ObjectId::of(&root_bytes);
        store
            .put_objects(ROOT_SPACE, &[(root_id, root_bytes)])
            .await?;
        assert!(materialize(store, root_id).await.is_err());
    }
    Ok(())
}

async fn put_root<S: Storage + Clone>(
    store: &mut Store<S>,
    root: &Root,
) -> Result<ObjectId, String> {
    let bytes = encode_root(root);
    let id = ObjectId::of(&bytes);
    store.put_objects(ROOT_SPACE, &[(id, bytes)]).await?;
    Ok(id)
}

async fn load_root<S: Storage + Clone>(store: &mut Store<S>, id: ObjectId) -> Result<Root, String> {
    decode_root(&store.get_one(ROOT_SPACE, id).await?)
}

async fn load_root_on_read<R: StorageRead>(
    store: &mut Reader<'_, R>,
    id: ObjectId,
) -> Result<Root, String> {
    decode_root(&store.get_one(ROOT_SPACE, id).await?)
}

fn encode_pages(rows: &BTreeMap<u64, Vec<u8>>) -> Vec<(ObjectId, Vec<u8>)> {
    rows.iter()
        .collect::<Vec<_>>()
        .chunks(PAGE_ROWS)
        .map(|chunk| {
            let page = chunk
                .iter()
                .map(|(key, value)| (**key, (*value).clone()))
                .collect::<BTreeMap<_, _>>();
            let bytes = encode_page(&page);
            (ObjectId::of(&bytes), bytes)
        })
        .collect()
}

fn encode_page(rows: &BTreeMap<u64, Vec<u8>>) -> Vec<u8> {
    let mut out = b"EDP1".to_vec();
    out.extend_from_slice(&(rows.len() as u32).to_be_bytes());
    for (key, value) in rows {
        let encoded_key = state_key(*key);
        out.extend_from_slice(&(encoded_key.len() as u32).to_be_bytes());
        out.extend_from_slice(&encoded_key);
        out.extend_from_slice(&key.to_be_bytes());
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(value);
    }
    out
}

fn decode_page_counted(
    counters: &mut Counters,
    bytes: &[u8],
) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let page = decode_page(bytes)?;
    counters.decoded_pages += 1;
    counters.decoded_rows += page.len() as u64;
    Ok(page)
}

fn decode_page(bytes: &[u8]) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let mut input = Decoder::new(bytes);
    input.magic(b"EDP1")?;
    let count = input.u32()? as usize;
    let mut rows = BTreeMap::new();
    for _ in 0..count {
        let key_len = input.u32()? as usize;
        let encoded_key = input.bytes(key_len)?.to_vec();
        let key = input.u64()?;
        if encoded_key != state_key(key) {
            return Err("slotted encoded StateKey/ordinal binding mismatch".to_owned());
        }
        let len = input.u32()? as usize;
        let value = input.bytes(len)?.to_vec();
        decode_body(&TUPLE_PLAN, &value).map_err(|e| e.to_string())?;
        if rows.insert(key, value).is_some() {
            return Err("duplicate page key".to_owned());
        }
    }
    input.finish()?;
    Ok(rows)
}

fn encode_root(root: &Root) -> Vec<u8> {
    let mut out = b"BER1".to_vec();
    out.push(root.layout.tag());
    out.extend_from_slice(&root.count.to_be_bytes());
    out.extend_from_slice(&(root.pages.len() as u32).to_be_bytes());
    for page in &root.pages {
        out.extend_from_slice(&page.0);
    }
    out.extend_from_slice(&(root.buffer.len() as u16).to_be_bytes());
    for (key, mutation) in &root.buffer {
        encode_buffered_mutation(&mut out, *key, mutation);
    }
    out
}

fn decode_root(bytes: &[u8]) -> Result<Root, String> {
    let mut input = Decoder::new(bytes);
    input.magic(b"BER1")?;
    let layout = match input.u8()? {
        1 => Layout::Slotted,
        2 => Layout::Bepsilon,
        _ => return Err("unknown root layout".to_owned()),
    };
    let count = input.u64()?;
    let count_pages = input.u32()? as usize;
    let mut pages = Vec::with_capacity(count_pages);
    for _ in 0..count_pages {
        pages.push(ObjectId(input.fixed_32()?));
    }
    if pages.is_empty() && count != 0 {
        return Err("state root is missing its sole page owner".to_owned());
    }
    let buffer_count = u16::from_be_bytes(input.bytes(2)?.try_into().unwrap()) as usize;
    if buffer_count > 512 {
        return Err("root mutation buffer exceeds format bound".to_owned());
    }
    if layout == Layout::Slotted && buffer_count != 0 {
        return Err("slotted root carries a mutation buffer".to_owned());
    }
    let mut buffer = BTreeMap::new();
    let mut previous = None;
    for _ in 0..buffer_count {
        let (key, mutation) = decode_buffered_mutation(&mut input)?;
        if previous.is_some_and(|previous| previous >= key) {
            return Err("duplicate or noncanonical root buffer order".to_owned());
        }
        if key as usize / PAGE_ROWS >= pages.len() {
            return Err("buffer key lies outside the root child fences".to_owned());
        }
        previous = Some(key);
        buffer.insert(key, mutation);
    }
    input.finish()?;
    Ok(Root {
        layout,
        count,
        pages,
        buffer,
    })
}

fn state_key(ordinal: u64) -> Vec<u8> {
    let mut out = b"schema-v1/entity/".to_vec();
    match pk_kind().as_str() {
        "uuid" => {
            out.push(b'u');
            out.extend_from_slice(&Uuid::from_u128(ordinal as u128 + 1).into_bytes());
        }
        "text" => {
            let value = format!("pk-{ordinal:020}");
            out.push(b't');
            out.extend_from_slice(&(value.len() as u32).to_be_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        "composite" => {
            let value = format!("pk-{ordinal:020}");
            out.push(b'c');
            out.extend_from_slice(&ordinal.to_be_bytes());
            out.extend_from_slice(&Uuid::from_u128(ordinal as u128 + 1).into_bytes());
            out.extend_from_slice(&(value.len() as u32).to_be_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        _ => {
            out.push(b'i');
            out.extend_from_slice(&ordinal.to_be_bytes());
        }
    }
    out
}

fn pk_kind() -> String {
    std::env::var("EXP_BEPSILON_PK_KIND").unwrap_or_else(|_| "integer".to_owned())
}

fn buffer_entry_cap() -> usize {
    std::env::var("EXP_BEPSILON_ENTRIES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| matches!(*value, 32 | 128 | 512))
        .unwrap_or(128)
}

fn buffer_byte_cap() -> usize {
    std::env::var("EXP_BEPSILON_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| matches!(*value, 4096 | 16384 | 65536))
        .unwrap_or(16_384)
}

fn encode_buffered_mutation(out: &mut Vec<u8>, key: u64, mutation: &BufferedMutation) {
    let encoded_key = state_key(key);
    out.extend_from_slice(&(encoded_key.len() as u32).to_be_bytes());
    out.extend_from_slice(&encoded_key);
    out.extend_from_slice(&key.to_be_bytes());
    match mutation {
        BufferedMutation::Tombstone => out.push(0),
        BufferedMutation::Value(value) => {
            out.push(1);
            out.extend_from_slice(&(value.len() as u32).to_be_bytes());
            out.extend_from_slice(value);
        }
    }
}

fn decode_buffered_mutation(input: &mut Decoder<'_>) -> Result<(u64, BufferedMutation), String> {
    let key_len = input.u32()? as usize;
    let encoded_key = input.bytes(key_len)?.to_vec();
    let key = input.u64()?;
    if encoded_key != state_key(key) {
        return Err("buffer StateKey/ordinal binding mismatch".to_owned());
    }
    let mutation = match input.u8()? {
        0 => BufferedMutation::Tombstone,
        1 => {
            let value_len = input.u32()? as usize;
            let value = input.bytes(value_len)?.to_vec();
            decode_body(&TUPLE_PLAN, &value).map_err(|error| error.to_string())?;
            BufferedMutation::Value(value)
        }
        _ => return Err("malformed buffered mutation tag".to_owned()),
    };
    Ok((key, mutation))
}

fn buffer_encoded_bytes(buffer: &BTreeMap<u64, BufferedMutation>) -> usize {
    let mut bytes = Vec::new();
    for (key, mutation) in buffer {
        encode_buffered_mutation(&mut bytes, *key, mutation);
    }
    bytes.len()
}

fn should_flush_buffer(buffer: &BTreeMap<u64, BufferedMutation>) -> bool {
    buffer.len() >= buffer_entry_cap() || buffer_encoded_bytes(buffer) >= buffer_byte_cap()
}

fn apply_buffer_range(
    rows: &mut BTreeMap<u64, Vec<u8>>,
    buffer: &BTreeMap<u64, BufferedMutation>,
    start: u64,
    end: u64,
) {
    for (key, mutation) in buffer.range(start..end) {
        match mutation {
            BufferedMutation::Value(value) => {
                rows.insert(*key, value.clone());
            }
            BufferedMutation::Tombstone => {
                rows.remove(key);
            }
        }
    }
}

fn validate_page_index(rows: &BTreeMap<u64, Vec<u8>>, expected: usize) -> Result<(), String> {
    if rows.keys().any(|key| *key as usize / PAGE_ROWS != expected) {
        return Err("state page/root directory position mismatch".to_owned());
    }
    Ok(())
}

fn decode_visible_page_counted(
    counters: &mut Counters,
    bytes: &[u8],
    expected_index: usize,
) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let rows = decode_page_counted(counters, bytes)?;
    validate_page_index(&rows, expected_index)?;
    Ok(rows)
}

fn buffer_codec_controls() {
    let pages = vec![ObjectId([1; 32]), ObjectId([2; 32])];
    let first = Root {
        layout: Layout::Bepsilon,
        count: 128,
        pages: pages.clone(),
        buffer: BTreeMap::from([
            (1, BufferedMutation::Value(tuple(1, 2))),
            (65, BufferedMutation::Tombstone),
        ]),
    };
    let reverse = Root {
        buffer: BTreeMap::from([
            (65, BufferedMutation::Tombstone),
            (1, BufferedMutation::Value(tuple(1, 2))),
        ]),
        ..first.clone()
    };
    let encoded = encode_root(&first);
    assert_eq!(encoded, encode_root(&reverse));
    assert_eq!(decode_root(&encoded).unwrap().buffer, first.buffer);

    let mut malformed = encoded.clone();
    malformed.pop();
    assert!(decode_root(&malformed).is_err());

    let mut outside = first.clone();
    outside
        .buffer
        .insert(128, BufferedMutation::Value(tuple(128, 2)));
    assert!(decode_root(&encode_root(&outside)).is_err());

    // Forge a duplicate/noncanonical buffer entry without BTreeMap sorting.
    let mut duplicate = encode_root(&Root {
        buffer: BTreeMap::new(),
        ..first.clone()
    });
    duplicate.truncate(duplicate.len() - 2);
    duplicate.extend_from_slice(&2u16.to_be_bytes());
    encode_buffered_mutation(&mut duplicate, 1, &BufferedMutation::Tombstone);
    encode_buffered_mutation(&mut duplicate, 1, &BufferedMutation::Value(tuple(1, 3)));
    assert!(decode_root(&duplicate).is_err());

    let mut visible = (0..128).map(|key| (key, tuple(key, 0))).collect();
    apply_buffer_range(&mut visible, &first.buffer, 0, 128);
    assert_eq!(visible.get(&1), Some(&tuple(1, 2)));
    assert!(!visible.contains_key(&65));
}
fn tuple(key: u64, generation: u64) -> Vec<u8> {
    let mut out = Vec::new();
    encode_body(
        &TUPLE_PLAN,
        &[
            BodyValue::Uuid(Uuid::from_u128(key as u128 + 1)),
            BodyValue::Int8(generation as i64),
            BodyValue::Boolean((key + generation).is_multiple_of(2)),
            BodyValue::Timestamptz(1_700_000_000_000_000 + generation as i64),
            if (key + generation).is_multiple_of(11) {
                BodyValue::Null
            } else {
                BodyValue::Text(format!("entity-{key}-generation-{generation}"))
            },
        ],
        &mut out,
    )
    .unwrap();
    out
}

fn mutation_keys(n: usize, d: usize, generation: u64) -> Vec<u64> {
    match mutation_pattern().as_str() {
        "repeated" => vec![(n / 2) as u64],
        "random" => {
            let mut keys = BTreeSet::new();
            let mut state = generation ^ 0x9e37_79b9_7f4a_7c15;
            while keys.len() < d {
                state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
                let mut value = state;
                value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                keys.insert((value ^ (value >> 31)) % n as u64);
            }
            keys.into_iter().collect()
        }
        "prefix" => {
            let start = (generation as usize * d.max(1)) % n;
            (0..d).map(|offset| ((start + offset) % n) as u64).collect()
        }
        _ => {
            let stride = (n / d.max(1)).max(1);
            (0..d)
                .map(|index| ((index * stride + generation as usize) % n) as u64)
                .collect()
        }
    }
}

fn mutation_pattern() -> String {
    std::env::var("EXP_BEPSILON_PATTERN").unwrap_or_else(|_| "uniform".to_owned())
}

fn map_digest(rows: &BTreeMap<u64, Vec<u8>>) -> ObjectId {
    let mut hasher = blake3::Hasher::new();
    for (key, value) in rows {
        hasher.update(&key.to_be_bytes());
        hasher.update(&(value.len() as u64).to_be_bytes());
        hasher.update(value);
    }
    ObjectId(*hasher.finalize().as_bytes())
}

fn parse_config() -> Config {
    let quick = std::env::var("EXP_DELTA_PAGE_QUICK").ok().as_deref() == Some("1");
    Config {
        backends: list_env("EXP_DELTA_PAGE_BACKENDS", "rocksdb,slatedb"),
        layouts: list_env("EXP_DELTA_PAGE_LAYOUTS", "slotted,bepsilon")
            .iter()
            .map(|value| Layout::parse(value))
            .collect(),
        sizes: usize_env(
            "EXP_DELTA_PAGE_SIZES",
            if quick { "1000" } else { "1000,10000,50000" },
        ),
        histories: usize_env(
            "EXP_DELTA_PAGE_HISTORIES",
            if quick { "10" } else { "10,100,1000" },
        ),
        deltas: list_env(
            "EXP_DELTA_PAGE_DELTAS",
            if quick { "1,1pct" } else { "1,10,1pct" },
        )
        .into_iter()
        .map(|value| {
            value.strip_suffix("pct").map_or_else(
                || DeltaShape::Fixed(value.parse().unwrap()),
                |value| DeltaShape::Percent(value.parse().unwrap()),
            )
        })
        .collect(),
        root: std::env::var_os("EXP_DELTA_PAGE_ROOT").map(PathBuf::from),
    }
}

fn list_env(name: &str, default: &str) -> Vec<String> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}
fn usize_env(name: &str, default: &str) -> Vec<usize> {
    list_env(name, default)
        .into_iter()
        .map(|value| value.parse().unwrap())
        .collect()
}
fn directory_bytes(path: &Path) -> u64 {
    fn visit(path: &Path) -> u64 {
        std::fs::read_dir(path).map_or(0, |entries| {
            entries
                .flatten()
                .map(|entry| {
                    let path = entry.path();
                    entry.metadata().map_or(0, |metadata| {
                        if metadata.is_dir() {
                            visit(&path)
                        } else {
                            metadata.len()
                        }
                    })
                })
                .sum()
        })
    }
    visit(path)
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn bytes(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self.offset.checked_add(len).ok_or("decode overflow")?;
        let value = self.bytes.get(self.offset..end).ok_or("truncated object")?;
        self.offset = end;
        Ok(value)
    }
    fn magic(&mut self, magic: &[u8]) -> Result<(), String> {
        if self.bytes(magic.len())? == magic {
            Ok(())
        } else {
            Err("object domain/magic mismatch".to_owned())
        }
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.bytes(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_be_bytes(self.bytes(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_be_bytes(self.bytes(8)?.try_into().unwrap()))
    }
    fn fixed_32(&mut self) -> Result<[u8; 32], String> {
        Ok(self.bytes(32)?.try_into().unwrap())
    }
    fn finish(&self) -> Result<(), String> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("trailing object bytes".to_owned())
        }
    }
}
