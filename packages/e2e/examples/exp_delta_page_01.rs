//! EXP-DELTA-PAGE-01: authenticated versioned-page layout experiment.
//!
//! Compares identical Schema-v1 typed tuple bytes stored as either:
//! - immutable 64-row slotted snapshot pages with a rewritten page manifest; or
//! - immutable schema base pages plus authenticated sparse delta pages, with a
//!   deterministic compaction every `COMPACTION_DEPTH` commits.
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

const LEDGER: &str = "EXP-DELTA-PAGE-01";
const PAGE_ROWS: usize = 64;
const DEFAULT_COMPACTION_PENDING_PERCENT: u64 = 10;
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
    Delta,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Self::Slotted => "slotted",
            Self::Delta => "delta",
        }
    }

    fn tag(self) -> u8 {
        match self {
            Self::Slotted => 1,
            Self::Delta => 2,
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
    bloom_checks: u64,
    bloom_positive: u64,
    bloom_false_positive: u64,
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
            bloom_checks: self.bloom_checks - before.bloom_checks,
            bloom_positive: self.bloom_positive - before.bloom_positive,
            bloom_false_positive: self.bloom_false_positive - before.bloom_false_positive,
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
        self.bloom_checks += other.bloom_checks;
        self.bloom_positive += other.bloom_positive;
        self.bloom_false_positive += other.bloom_false_positive;
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
    generation: u64,
    depth: u32,
    pending_rows: u64,
    parent: Option<ObjectId>,
    /// Immutable schema-partitioned base pages.
    pages: Vec<ObjectId>,
    /// Binary-leveled sparse deltas. Lower levels are newer; each occupied
    /// level represents a deterministic power-of-two commit run.
    levels: Vec<DeltaLevel>,
}

#[derive(Clone, Debug)]
struct DeltaLevel {
    pages: Vec<ObjectId>,
    blooms: Vec<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct Config {
    backends: Vec<String>,
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
    emit_bloom_sweep();
    println!(
        "{LEDGER},kind=config,page_rows={PAGE_ROWS},compaction_pending_percent={},sizes={:?},histories={:?}",
        compaction_pending_percent(),
        config.sizes,
        config.histories
    );
    let mut case = 0usize;
    for backend in &config.backends {
        for &n in &config.sizes {
            for &history in &config.histories {
                for &delta in &config.deltas {
                    for layout in [Layout::Slotted, Layout::Delta] {
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
    }

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
        "{LEDGER},kind=operation,backend={backend},layout={},n={n},h={h},d={d},operation={operation},wall_us={wall_us},get_calls={},get_keys={},get_bytes={},put_calls={},puts={},put_bytes={},decoded_pages={},decoded_rows={},bloom_checks={},bloom_positive={},bloom_false_positive={},settled_bytes={}",
        layout.name(),
        c.get_calls,
        c.get_keys,
        c.get_bytes,
        c.put_calls,
        c.puts,
        c.put_bytes,
        c.decoded_pages,
        c.decoded_rows,
        c.bloom_checks,
        c.bloom_positive,
        c.bloom_false_positive,
        directory_bytes(path)
    );
}

async fn build_initial<S: Storage + Clone>(
    store: &mut Store<S>,
    layout: Layout,
    rows: &BTreeMap<u64, Vec<u8>>,
) -> Result<ObjectId, String> {
    let pages = encode_pages(rows);
    store.put_objects(PAGE_SPACE, &pages).await?;
    let root = Root {
        layout,
        count: rows.len() as u64,
        generation: 0,
        depth: 0,
        pending_rows: 0,
        parent: None,
        pages: pages.iter().map(|(id, _)| *id).collect(),
        levels: Vec::new(),
    };
    put_root(store, &root).await
}

async fn commit<S: Storage + Clone>(
    store: &mut Store<S>,
    current: ObjectId,
    full: &mut BTreeMap<u64, Vec<u8>>,
    mutations: &BTreeMap<u64, Vec<u8>>,
    generation: u64,
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
                    generation,
                    depth: 0,
                    pending_rows: 0,
                    parent: None,
                    pages: page_ids,
                    levels: Vec::new(),
                },
            )
            .await
        }
        Layout::Delta
            if root.pending_rows + (mutations.len() as u64)
                < compaction_row_threshold(full.len()) =>
        {
            let mut levels = root.levels.clone();
            let mut carry = mutations.clone();
            let mut level_index = 0usize;
            loop {
                if levels.len() <= level_index {
                    levels.push(DeltaLevel {
                        pages: Vec::new(),
                        blooms: Vec::new(),
                    });
                }
                if levels[level_index].pages.is_empty() {
                    break;
                }
                let level = &levels[level_index];
                let encoded = store.get_many(PAGE_SPACE, &level.pages).await?;
                let mut older = BTreeMap::new();
                for (index, bytes) in encoded.into_iter().enumerate() {
                    let page = decode_page_counted(&mut store.counters, &bytes)?;
                    validate_level_page_bloom(level, index, &page)?;
                    older.extend(page);
                }
                older.extend(carry);
                carry = older;
                levels[level_index] = DeltaLevel {
                    pages: Vec::new(),
                    blooms: Vec::new(),
                };
                level_index += 1;
            }
            let pages = encode_pages(&carry);
            let blooms = encoded_page_blooms(&pages)?;
            store.put_objects(PAGE_SPACE, &pages).await?;
            levels[level_index] = DeltaLevel {
                pages: pages.iter().map(|(id, _)| *id).collect(),
                blooms,
            };
            put_root(
                store,
                &Root {
                    layout: root.layout,
                    count: full.len() as u64,
                    generation,
                    depth: root.depth + 1,
                    pending_rows: root.pending_rows + mutations.len() as u64,
                    parent: Some(current),
                    pages: root.pages,
                    levels,
                },
            )
            .await
        }
        Layout::Delta => {
            let pages = encode_pages(full);
            store.put_objects(PAGE_SPACE, &pages).await?;
            put_root(
                store,
                &Root {
                    layout: root.layout,
                    count: full.len() as u64,
                    generation,
                    depth: 0,
                    pending_rows: 0,
                    parent: Some(current),
                    pages: pages.iter().map(|(id, _)| *id).collect(),
                    levels: Vec::new(),
                },
            )
            .await
        }
    }
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
    if root.layout == Layout::Delta {
        for level in &root.levels {
            for page_index in delta_page_indexes(level, key, store.counters)? {
                let bytes = store.get_one(PAGE_SPACE, level.pages[page_index]).await?;
                let page = decode_page_counted(store.counters, &bytes)?;
                validate_level_page_bloom(level, page_index, &page)?;
                if let Some(value) = page.get(&key) {
                    return Ok(Some(value.clone()));
                }
                store.counters.bloom_false_positive += 1;
            }
        }
    }
    let page_index = key as usize / PAGE_ROWS;
    let Some(page_id) = root.pages.get(page_index).copied() else {
        return Ok(None);
    };
    let bytes = store.get_one(PAGE_SPACE, page_id).await?;
    let page = decode_page_counted(store.counters, &bytes)?;
    Ok(page.get(&key).cloned())
}

fn delta_page_indexes(
    level: &DeltaLevel,
    key: u64,
    counters: &mut Counters,
) -> Result<Vec<usize>, String> {
    if level.pages.is_empty() {
        return Ok(Vec::new());
    }
    if level.blooms.len() != level.pages.len() {
        return Err("delta root page-bound cardinality mismatch".to_owned());
    }
    let mut indexes = Vec::new();
    for (index, bloom) in level.blooms.iter().enumerate() {
        counters.bloom_checks += 1;
        if bloom_might_contain(bloom, key) {
            counters.bloom_positive += 1;
            indexes.push(index);
        }
    }
    Ok(indexes)
}

async fn materialize<S: Storage + Clone>(
    store: &mut Store<S>,
    root_id: ObjectId,
) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let mut read = store.reader().await?;
    materialize_on_read(&mut read, root_id).await
}

async fn materialize_on_read<R: StorageRead>(
    store: &mut Reader<'_, R>,
    root_id: ObjectId,
) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let root = load_root_on_read(store, root_id).await?;
    let mut rows = BTreeMap::new();
    for bytes in store.get_many(PAGE_SPACE, &root.pages).await? {
        rows.extend(decode_page_counted(store.counters, &bytes)?);
    }
    for level in root.levels.iter().rev() {
        for (index, bytes) in store
            .get_many(PAGE_SPACE, &level.pages)
            .await?
            .into_iter()
            .enumerate()
        {
            let page = decode_page_counted(store.counters, &bytes)?;
            validate_level_page_bloom(level, index, &page)?;
            rows.extend(page);
        }
    }
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
        Layout::Slotted => diff_slotted(store, &before_root, &after_root).await,
        Layout::Delta => diff_delta(store, before, after).await,
    }
}

async fn diff_slotted<R: StorageRead>(
    store: &mut Reader<'_, R>,
    before: &Root,
    after: &Root,
) -> Result<BTreeSet<u64>, String> {
    if before.pages.len() != after.pages.len() {
        return Err("slotted roots have incompatible page geometry".to_owned());
    }
    let mut changed = BTreeSet::new();
    for (before_id, after_id) in before.pages.iter().zip(&after.pages) {
        if before_id == after_id {
            continue;
        }
        let bytes = store.get_many(PAGE_SPACE, &[*before_id, *after_id]).await?;
        let before_page = decode_page_counted(store.counters, &bytes[0])?;
        let after_page = decode_page_counted(store.counters, &bytes[1])?;
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

async fn diff_delta<R: StorageRead>(
    store: &mut Reader<'_, R>,
    before: ObjectId,
    after: ObjectId,
) -> Result<BTreeSet<u64>, String> {
    let before_rows = materialize_on_read(store, before).await?;
    let after_rows = materialize_on_read(store, after).await?;
    Ok(before_rows
        .keys()
        .chain(after_rows.keys())
        .copied()
        .filter(|key| before_rows.get(key) != after_rows.get(key))
        .collect())
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
    let Some(page) = decoded.pages.first().copied() else {
        return Err("root has no page".to_owned());
    };
    let wrong = ObjectId([0x55; 32]);
    let mut corrupted = decoded;
    corrupted.pages[0] = wrong;
    let corrupt_bytes = encode_root(&corrupted);
    let corrupt_id = ObjectId::of(&corrupt_bytes);
    store
        .put_objects(ROOT_SPACE, &[(corrupt_id, corrupt_bytes)])
        .await?;
    assert!(materialize(store, corrupt_id).await.is_err());
    assert!(store.get_one(PAGE_SPACE, page).await.is_ok());
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
        let key = input.u64()?;
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
    let mut out = b"EDR1".to_vec();
    out.push(root.layout.tag());
    out.extend_from_slice(&root.count.to_be_bytes());
    out.extend_from_slice(&root.generation.to_be_bytes());
    out.extend_from_slice(&root.depth.to_be_bytes());
    out.extend_from_slice(&root.pending_rows.to_be_bytes());
    match root.parent {
        Some(parent) => {
            out.push(1);
            out.extend_from_slice(&parent.0);
        }
        None => out.push(0),
    }
    out.extend_from_slice(&(root.pages.len() as u32).to_be_bytes());
    for page in &root.pages {
        out.extend_from_slice(&page.0);
    }
    out.extend_from_slice(&(root.levels.len() as u32).to_be_bytes());
    for level in &root.levels {
        out.extend_from_slice(&(level.pages.len() as u32).to_be_bytes());
        for (page, bloom) in level.pages.iter().zip(&level.blooms) {
            out.extend_from_slice(&page.0);
            out.extend_from_slice(&(bloom.len() as u32).to_be_bytes());
            out.extend_from_slice(bloom);
        }
    }
    out
}

fn decode_root(bytes: &[u8]) -> Result<Root, String> {
    let mut input = Decoder::new(bytes);
    input.magic(b"EDR1")?;
    let layout = match input.u8()? {
        1 => Layout::Slotted,
        2 => Layout::Delta,
        _ => return Err("unknown root layout".to_owned()),
    };
    let count = input.u64()?;
    let generation = input.u64()?;
    let depth = input.u32()?;
    let pending_rows = input.u64()?;
    let parent = match input.u8()? {
        0 => None,
        1 => Some(ObjectId(input.fixed_32()?)),
        _ => return Err("invalid parent tag".to_owned()),
    };
    if layout == Layout::Slotted && (depth != 0 || parent.is_some()) {
        return Err("slotted root has delta ancestry".to_owned());
    }
    if layout == Layout::Delta && generation == 0 && parent.is_some() {
        return Err("initial delta root has parent".to_owned());
    }
    if layout == Layout::Delta && generation > 0 && parent.is_none() {
        return Err("versioned delta root is missing chronology parent".to_owned());
    }
    if depth == 0 && pending_rows != 0 {
        return Err("compacted root has pending delta rows".to_owned());
    }
    if depth > 0 && pending_rows == 0 {
        return Err("delta root has no pending row accounting".to_owned());
    }
    let count_pages = input.u32()? as usize;
    let mut pages = Vec::with_capacity(count_pages);
    for _ in 0..count_pages {
        pages.push(ObjectId(input.fixed_32()?));
    }
    let count_levels = input.u32()? as usize;
    let mut levels = Vec::with_capacity(count_levels);
    for _ in 0..count_levels {
        let page_count = input.u32()? as usize;
        let mut level_pages = Vec::with_capacity(page_count);
        let mut blooms = Vec::with_capacity(page_count);
        for _ in 0..page_count {
            level_pages.push(ObjectId(input.fixed_32()?));
            let bloom_len = input.u32()? as usize;
            if !matches!(bloom_len, 16 | 32 | 64) {
                return Err("noncanonical delta bloom size class".to_owned());
            }
            blooms.push(input.bytes(bloom_len)?.to_vec());
        }
        levels.push(DeltaLevel {
            pages: level_pages,
            blooms,
        });
    }
    if layout == Layout::Slotted && !levels.is_empty() {
        return Err("slotted root has delta levels".to_owned());
    }
    if depth == 0 && levels.iter().any(|level| !level.pages.is_empty()) {
        return Err("compacted delta root has sparse levels".to_owned());
    }
    if depth > 0 && !levels.iter().any(|level| !level.pages.is_empty()) {
        return Err("uncompacted delta root has no sparse levels".to_owned());
    }
    input.finish()?;
    Ok(Root {
        layout,
        count,
        generation,
        depth,
        pending_rows,
        parent,
        pages,
        levels,
    })
}

fn encoded_page_blooms(pages: &[(ObjectId, Vec<u8>)]) -> Result<Vec<Vec<u8>>, String> {
    pages
        .iter()
        .map(|(_, bytes)| Ok(page_bloom(&decode_page(bytes)?)))
        .collect()
}

fn validate_level_page_bloom(
    level: &DeltaLevel,
    index: usize,
    page: &BTreeMap<u64, Vec<u8>>,
) -> Result<(), String> {
    if level.blooms.get(index) != Some(&page_bloom(page)) {
        return Err("authenticated delta page bloom does not match page".to_owned());
    }
    Ok(())
}

fn page_bloom(page: &BTreeMap<u64, Vec<u8>>) -> Vec<u8> {
    let bytes = match page.len() {
        0..=2 => 16,
        3..=16 => 32,
        _ => 64,
    };
    let mut bloom = vec![0u8; bytes];
    for key in page.keys() {
        let hash = blake3::hash(&key.to_be_bytes());
        for pair in hash.as_bytes()[..8].chunks_exact(2) {
            let bit = u16::from_be_bytes([pair[0], pair[1]]) as usize % (bloom.len() * 8);
            bloom[bit / 8] |= 1 << (bit % 8);
        }
    }
    bloom
}

fn bloom_might_contain(bloom: &[u8], key: u64) -> bool {
    let hash = blake3::hash(&key.to_be_bytes());
    hash.as_bytes()[..8].chunks_exact(2).all(|pair| {
        let bit = u16::from_be_bytes([pair[0], pair[1]]) as usize % (bloom.len() * 8);
        bloom[bit / 8] & (1 << (bit % 8)) != 0
    })
}

fn emit_bloom_sweep() {
    for entries in [1usize, 10, 64] {
        let keys = (0..entries)
            .map(|index| index as u64 * 997)
            .collect::<Vec<_>>();
        for bytes in [16usize, 32, 64, 128] {
            let mut bloom = vec![0u8; bytes];
            for key in &keys {
                let hash = blake3::hash(&key.to_be_bytes());
                for pair in hash.as_bytes()[..8].chunks_exact(2) {
                    let bit = u16::from_be_bytes([pair[0], pair[1]]) as usize % (bytes * 8);
                    bloom[bit / 8] |= 1 << (bit % 8);
                }
            }
            let probes = 100_000u64;
            let false_positives = (1_000_000..1_000_000 + probes)
                .filter(|key| {
                    let hash = blake3::hash(&key.to_be_bytes());
                    hash.as_bytes()[..8].chunks_exact(2).all(|pair| {
                        let bit = u16::from_be_bytes([pair[0], pair[1]]) as usize % (bytes * 8);
                        bloom[bit / 8] & (1 << (bit % 8)) != 0
                    })
                })
                .count();
            println!(
                "{LEDGER},kind=bloom_sweep,entries={entries},bloom_bytes={bytes},page_ref_bytes={},probes={probes},false_positives={false_positives},false_positive_ppm={}",
                36 + bytes,
                false_positives as u64 * 1_000_000 / probes
            );
        }
    }
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

fn compaction_pending_percent() -> u64 {
    std::env::var("EXP_DELTA_PAGE_COMPACTION_PERCENT")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value >= 1 && *value <= 100)
        .unwrap_or(DEFAULT_COMPACTION_PENDING_PERCENT)
}

fn compaction_row_threshold(row_count: usize) -> u64 {
    (row_count as u64)
        .saturating_mul(compaction_pending_percent())
        .div_ceil(100)
        .max(1)
}

fn mutation_keys(n: usize, d: usize, generation: u64) -> Vec<u64> {
    let stride = (n / d.max(1)).max(1);
    (0..d)
        .map(|index| ((index * stride + generation as usize) % n) as u64)
        .collect()
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
