//! EXP-BPTREE-07: canonical authenticated high-fanout B+ tree experiment.
//!
//! Compares identical Schema-v1 typed tuple bytes stored as either:
//! - immutable 64-row slotted snapshot pages with a rewritten page manifest; or
//! - a canonical content-addressed B+ tree ordered by the full encoded
//!   StateKey, with prefix-compressed pages and path-copy value updates.
//!
//! The benchmark uses the shipping RocksDB/SlateDB storage traits. There is no
//! SQL or workload-specific production shortcut: both layouts implement the
//! same point, mutation, full readback, diff, history, branch, corruption and
//! cold-reopen operations over content-addressed objects.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::pin::Pin;
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

const LEDGER: &str = "EXP-BPTREE-07";
const PAGE_ROWS: usize = 64;
const BPTREE_MIN_PAGE_BYTES: usize = 4 * 1024;
const BPTREE_MAX_PAGE_BYTES: usize = 32 * 1024;
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
    Bptree,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Self::Slotted => "slotted",
            Self::Bptree => "bptree",
        }
    }

    fn tag(self) -> u8 {
        match self {
            Self::Slotted => 1,
            Self::Bptree => 2,
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
    node_reads: u64,
    nodes_staged: u64,
    hash_pruned: u64,
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
            node_reads: self.node_reads - before.node_reads,
            nodes_staged: self.nodes_staged - before.nodes_staged,
            hash_pruned: self.hash_pruned - before.hash_pruned,
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
        self.node_reads += other.node_reads;
        self.nodes_staged += other.nodes_staged;
        self.hash_pruned += other.hash_pruned;
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
    parent: Option<ObjectId>,
    /// Immutable schema-partitioned base pages.
    pages: Vec<ObjectId>,
    /// Sole current-state owner for the B+ tree layout.
    tree_root: Option<ObjectId>,
}

#[derive(Clone, Debug)]
enum BptreeNode {
    Branch {
        /// Canonical minimum encoded key for each child, in strict order.
        fences: Vec<Vec<u8>>,
        children: Vec<ObjectId>,
    },
    Leaf(Vec<BptreeEntry>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BptreeEntry {
    encoded_key: Vec<u8>,
    ordinal: u64,
    value: Vec<u8>,
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
    bptree_codec_controls();
    println!(
        "{LEDGER},kind=config,page_rows={PAGE_ROWS},bptree_page_bytes={},pk_kind={},pattern={},sizes={:?},histories={:?}",
        bptree_page_bytes(),
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
                    for layout in [Layout::Slotted, Layout::Bptree] {
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
    let range_end = (range_start + 1_000).min(n as u64);
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
        "range_1k",
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
        "{LEDGER},kind=operation,backend={backend},layout={},n={n},h={h},d={d},operation={operation},wall_us={wall_us},get_calls={},get_keys={},get_bytes={},put_calls={},puts={},put_bytes={},decoded_pages={},decoded_rows={},node_reads={},nodes_staged={},hash_pruned={},settled_bytes={}",
        layout.name(),
        c.get_calls,
        c.get_keys,
        c.get_bytes,
        c.put_calls,
        c.puts,
        c.put_bytes,
        c.decoded_pages,
        c.decoded_rows,
        c.node_reads,
        c.nodes_staged,
        c.hash_pruned,
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
                generation: 0,
                parent: None,
                pages: pages.iter().map(|(id, _)| *id).collect(),
                tree_root: None,
            }
        }
        Layout::Bptree => {
            let entries = rows
                .iter()
                .map(|(ordinal, value)| BptreeEntry {
                    encoded_key: state_key(*ordinal),
                    ordinal: *ordinal,
                    value: value.clone(),
                })
                .collect::<Vec<_>>();
            let mut staged = Vec::new();
            let tree_root = build_bptree(&entries, &mut staged)?;
            store.counters.nodes_staged += staged.len() as u64;
            store.put_objects(PAGE_SPACE, &staged).await?;
            Root {
                layout,
                count: rows.len() as u64,
                generation: 0,
                parent: None,
                pages: Vec::new(),
                tree_root: Some(tree_root),
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
                    parent: None,
                    pages: page_ids,
                    tree_root: None,
                },
            )
            .await
        }
        Layout::Bptree => {
            let tree_root = root
                .tree_root
                .ok_or_else(|| "B+ tree root is missing".to_owned())?;
            let entries = mutations
                .iter()
                .map(|(ordinal, value)| BptreeEntry {
                    encoded_key: state_key(*ordinal),
                    ordinal: *ordinal,
                    value: value.clone(),
                })
                .collect::<Vec<_>>();
            let tree_root = bptree_update_many(store, tree_root, entries).await?;
            put_root(
                store,
                &Root {
                    layout: root.layout,
                    count: full.len() as u64,
                    generation,
                    parent: Some(current),
                    pages: Vec::new(),
                    tree_root: Some(tree_root),
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
    if root.layout == Layout::Bptree {
        let encoded_key = state_key(key);
        return bptree_point(
            store,
            root.tree_root
                .ok_or_else(|| "B+ tree root is missing".to_owned())?,
            &encoded_key,
        )
        .await;
    }
    let page_index = key as usize / PAGE_ROWS;
    let Some(page_id) = root.pages.get(page_index).copied() else {
        return Ok(None);
    };
    let bytes = store.get_one(PAGE_SPACE, page_id).await?;
    let page = decode_page_counted(store.counters, &bytes)?;
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
    if root.layout == Layout::Bptree {
        return bptree_range(
            &mut read,
            root.tree_root
                .ok_or_else(|| "B+ tree root is missing".to_owned())?,
            &state_key(start),
            &state_key(end),
        )
        .await;
    }
    let first = start as usize / PAGE_ROWS;
    let last = (end.saturating_sub(1) as usize / PAGE_ROWS).min(root.pages.len() - 1);
    let objects = read.get_many(PAGE_SPACE, &root.pages[first..=last]).await?;
    let mut rows = BTreeMap::new();
    for bytes in &objects {
        rows.extend(decode_page_counted(read.counters, bytes)?);
    }
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
    if root.layout == Layout::Bptree {
        let rows = bptree_enumerate(
            store,
            root.tree_root
                .ok_or_else(|| "B+ tree root is missing".to_owned())?,
        )
        .await?;
        return Ok(rows);
    }
    let mut rows = BTreeMap::new();
    let objects = store.get_many(PAGE_SPACE, &root.pages).await?;
    for bytes in &objects {
        rows.extend(decode_page_counted(store.counters, &bytes)?);
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
        Layout::Bptree => {
            bptree_diff(
                store,
                before_root
                    .tree_root
                    .ok_or_else(|| "B+ tree root is missing".to_owned())?,
                after_root
                    .tree_root
                    .ok_or_else(|| "B+ tree root is missing".to_owned())?,
            )
            .await
        }
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
    let retained = match decoded.layout {
        Layout::Slotted => {
            let page = decoded
                .pages
                .first()
                .copied()
                .ok_or_else(|| "slotted root has no page".to_owned())?;
            corrupted.pages[0] = wrong;
            page
        }
        Layout::Bptree => {
            let node = decoded
                .tree_root
                .ok_or_else(|| "B+ tree root is missing".to_owned())?;
            corrupted.tree_root = Some(wrong);
            node
        }
    };
    let corrupt_bytes = encode_root(&corrupted);
    let corrupt_id = ObjectId::of(&corrupt_bytes);
    store
        .put_objects(ROOT_SPACE, &[(corrupt_id, corrupt_bytes)])
        .await?;
    assert!(materialize(store, corrupt_id).await.is_err());
    assert!(store.get_one(PAGE_SPACE, retained).await.is_ok());
    if decoded.layout == Layout::Bptree {
        let node_bytes = store.get_one(PAGE_SPACE, retained).await?;
        if let BptreeNode::Branch {
            fences,
            mut children,
        } = decode_bptree_node(&node_bytes)?
        {
            if children.len() >= 2 {
                let mut substituted_children = children.clone();
                substituted_children[0] = substituted_children[1];
                let substituted = BptreeNode::Branch {
                    fences: fences.clone(),
                    children: substituted_children,
                };
                let substituted_bytes = encode_bptree_node(&substituted);
                let substituted_id = ObjectId::of(&substituted_bytes);
                store
                    .put_objects(PAGE_SPACE, &[(substituted_id, substituted_bytes)])
                    .await?;
                let mut substituted_root = decoded.clone();
                substituted_root.tree_root = Some(substituted_id);
                let root_bytes = encode_root(&substituted_root);
                let root_id = ObjectId::of(&root_bytes);
                store
                    .put_objects(ROOT_SPACE, &[(root_id, root_bytes)])
                    .await?;
                assert!(materialize(store, root_id).await.is_err());
            }
            children[0] = wrong;
            let malformed_child = BptreeNode::Branch { fences, children };
            let malformed_bytes = encode_bptree_node(&malformed_child);
            let malformed_id = ObjectId::of(&malformed_bytes);
            store
                .put_objects(PAGE_SPACE, &[(malformed_id, malformed_bytes)])
                .await?;
            let mut malformed_root = decoded.clone();
            malformed_root.tree_root = Some(malformed_id);
            let root_bytes = encode_root(&malformed_root);
            let root_id = ObjectId::of(&root_bytes);
            store
                .put_objects(ROOT_SPACE, &[(root_id, root_bytes)])
                .await?;
            assert!(materialize(store, root_id).await.is_err());

            let malformed_fence_bytes = b"BPB1\0\0\0\x01\0\0\0\0".to_vec();
            let malformed_fence_id = ObjectId::of(&malformed_fence_bytes);
            store
                .put_objects(PAGE_SPACE, &[(malformed_fence_id, malformed_fence_bytes)])
                .await?;
            malformed_root.tree_root = Some(malformed_fence_id);
            let root_bytes = encode_root(&malformed_root);
            let root_id = ObjectId::of(&root_bytes);
            store
                .put_objects(ROOT_SPACE, &[(root_id, root_bytes)])
                .await?;
            assert!(materialize(store, root_id).await.is_err());
        }
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
    let mut out = b"EHR1".to_vec();
    out.push(root.layout.tag());
    out.extend_from_slice(&root.count.to_be_bytes());
    out.extend_from_slice(&root.generation.to_be_bytes());
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
    match root.tree_root {
        Some(id) => {
            out.push(1);
            out.extend_from_slice(&id.0);
        }
        None => out.push(0),
    }
    out
}

fn decode_root(bytes: &[u8]) -> Result<Root, String> {
    let mut input = Decoder::new(bytes);
    input.magic(b"EHR1")?;
    let layout = match input.u8()? {
        1 => Layout::Slotted,
        2 => Layout::Bptree,
        _ => return Err("unknown root layout".to_owned()),
    };
    let count = input.u64()?;
    let generation = input.u64()?;
    let parent = match input.u8()? {
        0 => None,
        1 => Some(ObjectId(input.fixed_32()?)),
        _ => return Err("invalid parent tag".to_owned()),
    };
    if layout == Layout::Slotted && parent.is_some() {
        return Err("slotted root has B+ tree ancestry".to_owned());
    }
    if layout == Layout::Bptree && generation == 0 && parent.is_some() {
        return Err("initial B+ tree root has parent".to_owned());
    }
    if layout == Layout::Bptree && generation > 0 && parent.is_none() {
        return Err("versioned B+ tree root is missing chronology parent".to_owned());
    }
    let count_pages = input.u32()? as usize;
    let mut pages = Vec::with_capacity(count_pages);
    for _ in 0..count_pages {
        pages.push(ObjectId(input.fixed_32()?));
    }
    let tree_root = match input.u8()? {
        0 => None,
        1 => Some(ObjectId(input.fixed_32()?)),
        _ => return Err("invalid B+ tree root tag".to_owned()),
    };
    if layout == Layout::Slotted && tree_root.is_some() {
        return Err("slotted root has B+ tree owner".to_owned());
    }
    if layout == Layout::Bptree && (!pages.is_empty() || tree_root.is_none()) {
        return Err("B+ tree root has a second or missing state owner".to_owned());
    }
    input.finish()?;
    Ok(Root {
        layout,
        count,
        generation,
        parent,
        pages,
        tree_root,
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
    std::env::var("EXP_BPTREE_PK_KIND").unwrap_or_else(|_| "integer".to_owned())
}

fn bptree_page_bytes() -> usize {
    std::env::var("EXP_BPTREE_PAGE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (*value).is_power_of_two())
        .filter(|value| (BPTREE_MIN_PAGE_BYTES..=BPTREE_MAX_PAGE_BYTES).contains(value))
        .unwrap_or_else(|| {
            let width = state_key(0).len() + tuple(0, 0).len();
            (width * 32)
                .next_power_of_two()
                .clamp(BPTREE_MIN_PAGE_BYTES, BPTREE_MAX_PAGE_BYTES)
        })
}

fn common_prefix(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}

fn encode_prefixed_keys<'a>(keys: impl Iterator<Item = &'a [u8]>, out: &mut Vec<u8>) {
    let mut previous = Vec::new();
    for key in keys {
        let prefix = common_prefix(&previous, key);
        let suffix = &key[prefix..];
        out.extend_from_slice(&(prefix as u16).to_be_bytes());
        out.extend_from_slice(&(suffix.len() as u16).to_be_bytes());
        out.extend_from_slice(suffix);
        previous.clear();
        previous.extend_from_slice(key);
    }
}

fn decode_prefixed_key(input: &mut Decoder<'_>, previous: &[u8]) -> Result<Vec<u8>, String> {
    let prefix = u16::from_be_bytes(input.bytes(2)?.try_into().unwrap()) as usize;
    let suffix = u16::from_be_bytes(input.bytes(2)?.try_into().unwrap()) as usize;
    if prefix > previous.len() {
        return Err("malformed B+ tree key prefix".to_owned());
    }
    let mut key = previous[..prefix].to_vec();
    key.extend_from_slice(input.bytes(suffix)?);
    Ok(key)
}

fn encode_bptree_node(node: &BptreeNode) -> Vec<u8> {
    match node {
        BptreeNode::Branch { fences, children } => {
            let mut out = b"BPB1".to_vec();
            out.extend_from_slice(&(fences.len() as u32).to_be_bytes());
            encode_prefixed_keys(fences.iter().map(Vec::as_slice), &mut out);
            for child in children {
                out.extend_from_slice(&child.0);
            }
            out
        }
        BptreeNode::Leaf(entries) => {
            let mut out = b"BPL1".to_vec();
            out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
            let mut previous = Vec::new();
            for entry in entries {
                let prefix = common_prefix(&previous, &entry.encoded_key);
                let suffix = &entry.encoded_key[prefix..];
                out.extend_from_slice(&(prefix as u16).to_be_bytes());
                out.extend_from_slice(&(suffix.len() as u16).to_be_bytes());
                out.extend_from_slice(suffix);
                out.extend_from_slice(&entry.ordinal.to_be_bytes());
                out.extend_from_slice(&(entry.value.len() as u32).to_be_bytes());
                out.extend_from_slice(&entry.value);
                previous.clone_from(&entry.encoded_key);
            }
            out
        }
    }
}

fn decode_bptree_node(bytes: &[u8]) -> Result<BptreeNode, String> {
    if bytes.len() > bptree_page_bytes() {
        return Err("B+ tree node exceeds canonical byte target".to_owned());
    }
    if bytes.starts_with(b"BPB1") {
        let mut input = Decoder::new(bytes);
        input.magic(b"BPB1")?;
        let count = input.u32()? as usize;
        if count < 2 {
            return Err("noncanonical unary/empty B+ tree branch".to_owned());
        }
        let mut fences = Vec::with_capacity(count);
        let mut previous = Vec::new();
        for _ in 0..count {
            let key = decode_prefixed_key(&mut input, &previous)?;
            if !previous.is_empty() && previous >= key {
                return Err("duplicate or unordered B+ tree fence".to_owned());
            }
            previous.clone_from(&key);
            fences.push(key);
        }
        let mut children = Vec::with_capacity(count);
        for _ in 0..count {
            children.push(ObjectId(input.fixed_32()?));
        }
        input.finish()?;
        return Ok(BptreeNode::Branch { fences, children });
    }
    let mut input = Decoder::new(bytes);
    input.magic(b"BPL1")?;
    let count = input.u32()? as usize;
    if count == 0 {
        return Err("empty B+ tree leaf".to_owned());
    }
    let mut entries = Vec::with_capacity(count);
    let mut previous = Vec::new();
    for _ in 0..count {
        let encoded_key = decode_prefixed_key(&mut input, &previous)?;
        if !previous.is_empty() && previous >= encoded_key {
            return Err("duplicate or unordered B+ tree leaf key".to_owned());
        }
        let ordinal = input.u64()?;
        let value_len = input.u32()? as usize;
        let value = input.bytes(value_len)?.to_vec();
        if encoded_key != state_key(ordinal) {
            return Err("B+ tree StateKey/ordinal binding mismatch".to_owned());
        }
        decode_body(&TUPLE_PLAN, &value).map_err(|error| error.to_string())?;
        previous.clone_from(&encoded_key);
        entries.push(BptreeEntry {
            encoded_key,
            ordinal,
            value,
        });
    }
    input.finish()?;
    Ok(BptreeNode::Leaf(entries))
}

fn node_min_key(node: &BptreeNode) -> &[u8] {
    match node {
        BptreeNode::Branch { fences, .. } => &fences[0],
        BptreeNode::Leaf(entries) => &entries[0].encoded_key,
    }
}

fn stage_bptree_node(
    node: &BptreeNode,
    staged: &mut Vec<(ObjectId, Vec<u8>)>,
) -> Result<ObjectId, String> {
    let bytes = encode_bptree_node(node);
    if bytes.len() > bptree_page_bytes() {
        return Err("B+ tree node exceeds target during staging".to_owned());
    }
    let id = ObjectId::of(&bytes);
    staged.push((id, bytes));
    Ok(id)
}

fn partition_entries(entries: &[BptreeEntry]) -> Vec<Vec<BptreeEntry>> {
    let mut pages = Vec::new();
    let mut current = Vec::new();
    for entry in entries {
        let mut candidate = current.clone();
        candidate.push(entry.clone());
        if !current.is_empty()
            && encode_bptree_node(&BptreeNode::Leaf(candidate)).len() > bptree_page_bytes()
        {
            pages.push(std::mem::take(&mut current));
        }
        current.push(entry.clone());
    }
    if !current.is_empty() {
        pages.push(current);
    }
    pages
}

fn partition_children(children: &[(Vec<u8>, ObjectId)]) -> Vec<Vec<(Vec<u8>, ObjectId)>> {
    let mut pages = Vec::new();
    let mut current = Vec::new();
    for child in children {
        let mut candidate = current.clone();
        candidate.push(child.clone());
        let node = BptreeNode::Branch {
            fences: candidate.iter().map(|(key, _)| key.clone()).collect(),
            children: candidate.iter().map(|(_, id)| *id).collect(),
        };
        if current.len() >= 2 && encode_bptree_node(&node).len() > bptree_page_bytes() {
            pages.push(std::mem::take(&mut current));
        }
        current.push(child.clone());
    }
    if !current.is_empty() {
        pages.push(current);
    }
    pages
}

fn build_bptree(
    entries: &[BptreeEntry],
    staged: &mut Vec<(ObjectId, Vec<u8>)>,
) -> Result<ObjectId, String> {
    if entries.is_empty() {
        return Err("cannot build empty B+ tree".to_owned());
    }
    let mut sorted = entries.to_vec();
    sorted.sort_by(|left, right| left.encoded_key.cmp(&right.encoded_key));
    if !sorted
        .windows(2)
        .all(|pair| pair[0].encoded_key < pair[1].encoded_key)
    {
        return Err("duplicate B+ tree StateKey".to_owned());
    }
    let mut level = Vec::new();
    for page in partition_entries(&sorted) {
        let node = BptreeNode::Leaf(page);
        let min = node_min_key(&node).to_vec();
        level.push((min, stage_bptree_node(&node, staged)?));
    }
    while level.len() > 1 {
        let groups = partition_children(&level);
        level = groups
            .into_iter()
            .map(|group| {
                if group.len() == 1 {
                    return Ok(group[0].clone());
                }
                let node = BptreeNode::Branch {
                    fences: group.iter().map(|(key, _)| key.clone()).collect(),
                    children: group.iter().map(|(_, id)| *id).collect(),
                };
                let min = node_min_key(&node).to_vec();
                Ok((min, stage_bptree_node(&node, staged)?))
            })
            .collect::<Result<Vec<_>, String>>()?;
    }
    Ok(level[0].1)
}

async fn load_bptree_node<R: StorageRead>(
    store: &mut Reader<'_, R>,
    id: ObjectId,
    expected_min: Option<&[u8]>,
) -> Result<BptreeNode, String> {
    let bytes = store.get_one(PAGE_SPACE, id).await?;
    let node = decode_bptree_node(&bytes)?;
    if expected_min.is_some_and(|expected| expected != node_min_key(&node)) {
        return Err("B+ tree fence/child minimum binding mismatch".to_owned());
    }
    store.counters.node_reads += 1;
    store.counters.decoded_pages += 1;
    if let BptreeNode::Leaf(entries) = &node {
        store.counters.decoded_rows += entries.len() as u64;
    }
    Ok(node)
}

async fn put_bptree_node<S: Storage + Clone>(
    store: &mut Store<S>,
    node: &BptreeNode,
) -> Result<ObjectId, String> {
    let bytes = encode_bptree_node(node);
    if bytes.len() > bptree_page_bytes() {
        return Err("B+ tree path-copy page exceeds target".to_owned());
    }
    let id = ObjectId::of(&bytes);
    store.put_objects(PAGE_SPACE, &[(id, bytes)]).await?;
    store.counters.nodes_staged += 1;
    Ok(id)
}

fn branch_child_index(fences: &[Vec<u8>], key: &[u8]) -> usize {
    fences
        .partition_point(|fence| fence.as_slice() <= key)
        .saturating_sub(1)
}

async fn bptree_update_many<S: Storage + Clone>(
    store: &mut Store<S>,
    node_id: ObjectId,
    entries: Vec<BptreeEntry>,
) -> Result<ObjectId, String> {
    let mut level = bptree_update_nodes(store, node_id, None, entries).await?;
    while level.len() > 1 {
        let groups = partition_children(&level);
        let mut next = Vec::new();
        for group in groups {
            if group.len() == 1 {
                next.push(group[0].clone());
                continue;
            }
            let node = BptreeNode::Branch {
                fences: group.iter().map(|(key, _)| key.clone()).collect(),
                children: group.iter().map(|(_, id)| *id).collect(),
            };
            let min = node_min_key(&node).to_vec();
            next.push((min, put_bptree_node(store, &node).await?));
        }
        level = next;
    }
    Ok(level[0].1)
}

fn bptree_update_nodes<'a, S: Storage + Clone + 'a>(
    store: &'a mut Store<S>,
    node_id: ObjectId,
    expected_min: Option<Vec<u8>>,
    entries: Vec<BptreeEntry>,
) -> Pin<Box<dyn Future<Output = Result<Vec<(Vec<u8>, ObjectId)>, String>> + 'a>> {
    Box::pin(async move {
        if entries.is_empty() {
            return Ok(vec![(
                expected_min.ok_or_else(|| "missing unchanged subtree fence".to_owned())?,
                node_id,
            )]);
        }
        let node = {
            let mut read = store.reader().await?;
            load_bptree_node(&mut read, node_id, expected_min.as_deref()).await?
        };
        match node {
            BptreeNode::Leaf(mut existing) => {
                for entry in entries {
                    let index = existing
                        .binary_search_by(|candidate| candidate.encoded_key.cmp(&entry.encoded_key))
                        .map_err(|_| {
                            "B+ tree update cannot insert through value-only path".to_owned()
                        })?;
                    existing[index] = entry;
                }
                let mut result = Vec::new();
                for page in partition_entries(&existing) {
                    let node = BptreeNode::Leaf(page);
                    let min = node_min_key(&node).to_vec();
                    result.push((min, put_bptree_node(store, &node).await?));
                }
                Ok(result)
            }
            BptreeNode::Branch { fences, children } => {
                let mut grouped = BTreeMap::<usize, Vec<BptreeEntry>>::new();
                for entry in entries {
                    let index = branch_child_index(&fences, &entry.encoded_key);
                    if entry.encoded_key.as_slice() < fences[index].as_slice() {
                        return Err("B+ tree update escaped selected fence".to_owned());
                    }
                    grouped.entry(index).or_default().push(entry);
                }
                let mut replacement = Vec::new();
                for index in 0..children.len() {
                    if let Some(entries) = grouped.remove(&index) {
                        replacement.extend(
                            bptree_update_nodes(
                                store,
                                children[index],
                                Some(fences[index].clone()),
                                entries,
                            )
                            .await?,
                        );
                    } else {
                        replacement.push((fences[index].clone(), children[index]));
                    }
                }
                let mut result = Vec::new();
                for group in partition_children(&replacement) {
                    if group.len() == 1 {
                        result.push(group[0].clone());
                        continue;
                    }
                    let node = BptreeNode::Branch {
                        fences: group.iter().map(|(key, _)| key.clone()).collect(),
                        children: group.iter().map(|(_, id)| *id).collect(),
                    };
                    let min = node_min_key(&node).to_vec();
                    result.push((min, put_bptree_node(store, &node).await?));
                }
                Ok(result)
            }
        }
    })
}

async fn bptree_point<R: StorageRead>(
    store: &mut Reader<'_, R>,
    mut node_id: ObjectId,
    encoded_key: &[u8],
) -> Result<Option<Vec<u8>>, String> {
    let mut expected_min: Option<Vec<u8>> = None;
    loop {
        match load_bptree_node(store, node_id, expected_min.as_deref()).await? {
            BptreeNode::Leaf(entries) => {
                return Ok(entries
                    .binary_search_by(|entry| entry.encoded_key.as_slice().cmp(encoded_key))
                    .ok()
                    .map(|index| entries[index].value.clone()));
            }
            BptreeNode::Branch { fences, children } => {
                let index = branch_child_index(&fences, encoded_key);
                expected_min = Some(fences[index].clone());
                node_id = children[index];
            }
        }
    }
}

fn bptree_collect_range<'a, R: StorageRead + 'a>(
    store: &'a mut Reader<'_, R>,
    node_id: ObjectId,
    expected_min: Option<Vec<u8>>,
    start: &'a [u8],
    end: &'a [u8],
    rows: &'a mut BTreeMap<u64, Vec<u8>>,
) -> Pin<Box<dyn Future<Output = Result<(), String>> + 'a>> {
    Box::pin(async move {
        match load_bptree_node(store, node_id, expected_min.as_deref()).await? {
            BptreeNode::Leaf(entries) => {
                for entry in entries {
                    if entry.encoded_key.as_slice() >= start && entry.encoded_key.as_slice() < end {
                        if rows.insert(entry.ordinal, entry.value).is_some() {
                            return Err("duplicate B+ tree row during range".to_owned());
                        }
                    }
                }
            }
            BptreeNode::Branch { fences, children } => {
                for index in 0..children.len() {
                    let lower = fences[index].as_slice();
                    let upper = fences.get(index + 1).map(Vec::as_slice);
                    if upper.is_some_and(|upper| upper <= start) || lower >= end {
                        continue;
                    }
                    bptree_collect_range(
                        store,
                        children[index],
                        Some(fences[index].clone()),
                        start,
                        end,
                        rows,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    })
}

async fn bptree_range<R: StorageRead>(
    store: &mut Reader<'_, R>,
    node_id: ObjectId,
    start: &[u8],
    end: &[u8],
) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let mut rows = BTreeMap::new();
    bptree_collect_range(store, node_id, None, start, end, &mut rows).await?;
    Ok(rows)
}

async fn bptree_enumerate<R: StorageRead>(
    store: &mut Reader<'_, R>,
    node_id: ObjectId,
) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let mut rows = BTreeMap::new();
    let mut stack = vec![(node_id, None)];
    while let Some((id, expected_min)) = stack.pop() {
        match load_bptree_node(store, id, expected_min.as_deref()).await? {
            BptreeNode::Leaf(entries) => {
                for entry in entries {
                    if rows.insert(entry.ordinal, entry.value).is_some() {
                        return Err("duplicate B+ tree row during enumeration".to_owned());
                    }
                }
            }
            BptreeNode::Branch { fences, children } => {
                stack.extend(
                    children
                        .into_iter()
                        .zip(fences)
                        .rev()
                        .map(|(id, fence)| (id, Some(fence))),
                );
            }
        }
    }
    Ok(rows)
}

async fn bptree_diff<R: StorageRead>(
    store: &mut Reader<'_, R>,
    before: ObjectId,
    after: ObjectId,
) -> Result<BTreeSet<u64>, String> {
    let mut changed = BTreeSet::new();
    let mut stack = vec![(before, after, None::<Vec<u8>>)];
    while let Some((before, after, expected_min)) = stack.pop() {
        if before == after {
            store.counters.hash_pruned += 1;
            continue;
        }
        let before_node = load_bptree_node(store, before, expected_min.as_deref()).await?;
        let after_node = load_bptree_node(store, after, expected_min.as_deref()).await?;
        match (before_node, after_node) {
            (
                BptreeNode::Branch {
                    fences: before_fences,
                    children: before_children,
                },
                BptreeNode::Branch {
                    fences: after_fences,
                    children: after_children,
                },
            ) if before_fences == after_fences => {
                stack.extend(
                    before_children
                        .into_iter()
                        .zip(after_children)
                        .zip(before_fences)
                        .map(|((before, after), fence)| (before, after, Some(fence))),
                );
            }
            (BptreeNode::Leaf(before_entries), BptreeNode::Leaf(after_entries)) => {
                let before_rows = before_entries
                    .into_iter()
                    .map(|entry| (entry.ordinal, entry.value))
                    .collect::<BTreeMap<_, _>>();
                let after_rows = after_entries
                    .into_iter()
                    .map(|entry| (entry.ordinal, entry.value))
                    .collect::<BTreeMap<_, _>>();
                changed.extend(
                    before_rows
                        .keys()
                        .chain(after_rows.keys())
                        .copied()
                        .filter(|key| before_rows.get(key) != after_rows.get(key)),
                );
            }
            _ => {
                // Deterministic split/merge can change local geometry. Only
                // the unequal authenticated subtree is enumerated.
                let before_rows = bptree_enumerate(store, before).await?;
                let after_rows = bptree_enumerate(store, after).await?;
                changed.extend(
                    before_rows
                        .keys()
                        .chain(after_rows.keys())
                        .copied()
                        .filter(|key| before_rows.get(key) != after_rows.get(key)),
                );
            }
        }
    }
    Ok(changed)
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

fn bptree_codec_controls() {
    let entries = (0..1_000u64)
        .map(|ordinal| BptreeEntry {
            encoded_key: state_key(ordinal),
            ordinal,
            value: tuple(ordinal, 0),
        })
        .collect::<Vec<_>>();
    let mut forward = Vec::new();
    let forward_root = build_bptree(&entries, &mut forward).unwrap();
    let mut reversed_entries = entries.clone();
    reversed_entries.reverse();
    let mut reverse = Vec::new();
    let reverse_root = build_bptree(&reversed_entries, &mut reverse).unwrap();
    assert_eq!(forward_root, reverse_root);

    let duplicate_fences = BptreeNode::Branch {
        fences: vec![state_key(0), state_key(0)],
        children: vec![ObjectId([1; 32]), ObjectId([2; 32])],
    };
    assert!(decode_bptree_node(&encode_bptree_node(&duplicate_fences)).is_err());

    let leaf = BptreeNode::Leaf(vec![entries[0].clone()]);
    let mut truncated = encode_bptree_node(&leaf);
    truncated.pop();
    assert!(decode_bptree_node(&truncated).is_err());

    // Canonical deterministic split/merge/delete: rebuilding after deleting
    // an adversarial boundary key is independent of insertion order, and
    // reinsertion restores the original authenticated root.
    let mut deleted = entries.clone();
    let removed = deleted.remove(deleted.len() / 2);
    let mut deleted_forward = Vec::new();
    let deleted_root = build_bptree(&deleted, &mut deleted_forward).unwrap();
    deleted.reverse();
    let mut deleted_reverse = Vec::new();
    assert_eq!(
        deleted_root,
        build_bptree(&deleted, &mut deleted_reverse).unwrap()
    );
    deleted.push(removed);
    let mut restored = Vec::new();
    assert_eq!(forward_root, build_bptree(&deleted, &mut restored).unwrap());
}

fn mutation_pattern() -> String {
    std::env::var("EXP_BPTREE_PATTERN").unwrap_or_else(|_| "uniform".to_owned())
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
