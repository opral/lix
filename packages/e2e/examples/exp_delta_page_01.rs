//! EXP-INLINE-DELTA-09: authenticated inline leaf-delta experiment.
//!
//! Compares identical Schema-v1 typed tuple bytes stored as either:
//! - immutable 64-row slotted snapshot pages with a rewritten page manifest; or
//! - C2 pages with one bounded canonical mutation area encoded inside the same
//!   authenticated leaf object and deterministically compacted into its base.
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

const LEDGER: &str = "EXP-INLINE-DELTA-09";
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
    Inline,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Self::Slotted => "slotted",
            Self::Inline => "inline_delta",
        }
    }

    fn tag(self) -> u8 {
        match self {
            Self::Slotted => 1,
            Self::Inline => 2,
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
    inline_reads: u64,
    inline_entries: u64,
    inline_compactions: u64,
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
            inline_reads: self.inline_reads - before.inline_reads,
            inline_entries: self.inline_entries - before.inline_entries,
            inline_compactions: self.inline_compactions - before.inline_compactions,
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
        self.inline_reads += other.inline_reads;
        self.inline_entries += other.inline_entries;
        self.inline_compactions += other.inline_compactions;
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
    /// Sole current-state owner: immutable schema-partitioned pages.
    pages: Vec<ObjectId>,
}

#[derive(Clone, Debug)]
struct InlinePage {
    base: BTreeMap<u64, Vec<u8>>,
    overlay: BTreeMap<u64, InlineMutation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum InlineMutation {
    Value(Vec<u8>),
    Tombstone,
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
    inline_codec_controls();
    println!(
        "{LEDGER},kind=config,page_rows={PAGE_ROWS},inline_entries={},inline_bytes={},pk_kind={},pattern={},sizes={:?},histories={:?}",
        inline_entry_cap(),
        inline_byte_cap(),
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
                    for layout in [Layout::Slotted, Layout::Inline] {
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
        "{LEDGER},kind=operation,backend={backend},layout={},n={n},h={h},d={d},operation={operation},wall_us={wall_us},get_calls={},get_keys={},get_bytes={},put_calls={},puts={},put_bytes={},decoded_pages={},decoded_rows={},inline_reads={},inline_entries={},inline_compactions={},settled_bytes={}",
        layout.name(),
        c.get_calls,
        c.get_keys,
        c.get_bytes,
        c.put_calls,
        c.puts,
        c.put_bytes,
        c.decoded_pages,
        c.decoded_rows,
        c.inline_reads,
        c.inline_entries,
        c.inline_compactions,
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
            }
        }
        Layout::Inline => {
            let pages = encode_inline_pages(rows);
            store.put_objects(PAGE_SPACE, &pages).await?;
            Root {
                layout,
                count: rows.len() as u64,
                generation: 0,
                parent: None,
                pages: pages.iter().map(|(id, _)| *id).collect(),
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
                },
            )
            .await
        }
        Layout::Inline => {
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
                let mut page = decode_inline_page_counted(&mut store.counters, &bytes)?;
                for (key, value) in mutations {
                    if (*key as usize) / PAGE_ROWS == *index {
                        page.overlay
                            .insert(*key, InlineMutation::Value(value.clone()));
                    }
                }
                store.counters.inline_entries += page.overlay.len() as u64;
                if should_compact_inline(&page) {
                    compact_inline_page(&mut page)?;
                    store.counters.inline_compactions += 1;
                }
                let bytes = encode_inline_page(&page);
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
                    parent: Some(current),
                    pages: page_ids,
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
    let page_index = key as usize / PAGE_ROWS;
    let Some(page_id) = root.pages.get(page_index).copied() else {
        return Ok(None);
    };
    let bytes = store.get_one(PAGE_SPACE, page_id).await?;
    let page = decode_visible_page_counted(store.counters, root.layout, &bytes, page_index)?;
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
        rows.extend(decode_visible_page_counted(
            read.counters,
            root.layout,
            bytes,
            first + offset,
        )?);
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
    let mut rows = BTreeMap::new();
    let objects = store.get_many(PAGE_SPACE, &root.pages).await?;
    for (index, bytes) in objects.iter().enumerate() {
        rows.extend(decode_visible_page_counted(
            store.counters,
            root.layout,
            bytes,
            index,
        )?);
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
        Layout::Slotted | Layout::Inline => diff_pages(store, &before_root, &after_root).await,
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
        let before_page =
            decode_visible_page_counted(store.counters, before.layout, &bytes[0], index)?;
        let after_page =
            decode_visible_page_counted(store.counters, after.layout, &bytes[1], index)?;
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
    if decoded.layout == Layout::Inline && decoded.pages.len() >= 2 {
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
    let mut out = b"EIR1".to_vec();
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
    out
}

fn decode_root(bytes: &[u8]) -> Result<Root, String> {
    let mut input = Decoder::new(bytes);
    input.magic(b"EIR1")?;
    let layout = match input.u8()? {
        1 => Layout::Slotted,
        2 => Layout::Inline,
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
        return Err("slotted root has inline chronology".to_owned());
    }
    if layout == Layout::Inline && generation == 0 && parent.is_some() {
        return Err("initial inline root has parent".to_owned());
    }
    if layout == Layout::Inline && generation > 0 && parent.is_none() {
        return Err("versioned inline root is missing chronology parent".to_owned());
    }
    let count_pages = input.u32()? as usize;
    let mut pages = Vec::with_capacity(count_pages);
    for _ in 0..count_pages {
        pages.push(ObjectId(input.fixed_32()?));
    }
    if pages.is_empty() && count != 0 {
        return Err("state root is missing its sole page owner".to_owned());
    }
    input.finish()?;
    Ok(Root {
        layout,
        count,
        generation,
        parent,
        pages,
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
    std::env::var("EXP_INLINE_PK_KIND").unwrap_or_else(|_| "integer".to_owned())
}

fn inline_entry_cap() -> usize {
    std::env::var("EXP_INLINE_ENTRIES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| matches!(*value, 2 | 4 | 8 | 16))
        .unwrap_or_else(|| {
            let width = state_key(0).len() + tuple(0, 0).len() + 16;
            (256 / width).clamp(2, 16).next_power_of_two()
        })
}

fn inline_byte_cap() -> usize {
    std::env::var("EXP_INLINE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| matches!(*value, 128 | 256 | 512 | 1024 | 2048))
        .unwrap_or_else(|| {
            let width = state_key(0).len() + tuple(0, 0).len() + 16;
            (inline_entry_cap() * width)
                .next_power_of_two()
                .clamp(128, 2048)
        })
}

fn encode_inline_pages(rows: &BTreeMap<u64, Vec<u8>>) -> Vec<(ObjectId, Vec<u8>)> {
    rows.iter()
        .collect::<Vec<_>>()
        .chunks(PAGE_ROWS)
        .map(|chunk| {
            let page = InlinePage {
                base: chunk
                    .iter()
                    .map(|(key, value)| (**key, (*value).clone()))
                    .collect(),
                overlay: BTreeMap::new(),
            };
            let bytes = encode_inline_page(&page);
            (ObjectId::of(&bytes), bytes)
        })
        .collect()
}

fn encode_inline_mutation(out: &mut Vec<u8>, key: u64, mutation: &InlineMutation) {
    let encoded_key = state_key(key);
    out.extend_from_slice(&(encoded_key.len() as u32).to_be_bytes());
    out.extend_from_slice(&encoded_key);
    out.extend_from_slice(&key.to_be_bytes());
    match mutation {
        InlineMutation::Tombstone => out.push(0),
        InlineMutation::Value(value) => {
            out.push(1);
            out.extend_from_slice(&(value.len() as u32).to_be_bytes());
            out.extend_from_slice(value);
        }
    }
}

fn encode_inline_page(page: &InlinePage) -> Vec<u8> {
    let base = encode_page(&page.base);
    let mut out = b"IDP1".to_vec();
    out.extend_from_slice(&(base.len() as u32).to_be_bytes());
    out.extend_from_slice(&base);
    out.extend_from_slice(&(page.overlay.len() as u16).to_be_bytes());
    for (key, mutation) in &page.overlay {
        encode_inline_mutation(&mut out, *key, mutation);
    }
    out
}

fn decode_inline_page(bytes: &[u8]) -> Result<InlinePage, String> {
    let mut input = Decoder::new(bytes);
    input.magic(b"IDP1")?;
    let base_len = input.u32()? as usize;
    let base = decode_page(input.bytes(base_len)?)?;
    if base.is_empty() {
        return Err("inline page has empty base".to_owned());
    }
    let base_page = *base.first_key_value().unwrap().0 as usize / PAGE_ROWS;
    if base
        .keys()
        .any(|key| *key as usize / PAGE_ROWS != base_page)
    {
        return Err("inline page base crosses canonical leaf boundary".to_owned());
    }
    let count = u16::from_be_bytes(input.bytes(2)?.try_into().unwrap()) as usize;
    if count > 16 {
        return Err("inline mutation area exceeds format bound".to_owned());
    }
    let mut overlay = BTreeMap::new();
    let mut previous = None;
    for _ in 0..count {
        let key_len = input.u32()? as usize;
        let encoded_key = input.bytes(key_len)?.to_vec();
        let key = input.u64()?;
        if encoded_key != state_key(key) {
            return Err("inline StateKey/ordinal binding mismatch".to_owned());
        }
        if previous.is_some_and(|previous| previous >= key) {
            return Err("duplicate or noncanonical inline mutation order".to_owned());
        }
        if key as usize / PAGE_ROWS != base_page {
            return Err("inline mutation has wrong base-page binding".to_owned());
        }
        let mutation = match input.u8()? {
            0 => InlineMutation::Tombstone,
            1 => {
                let value_len = input.u32()? as usize;
                let value = input.bytes(value_len)?.to_vec();
                decode_body(&TUPLE_PLAN, &value).map_err(|error| error.to_string())?;
                InlineMutation::Value(value)
            }
            _ => return Err("malformed inline mutation tag".to_owned()),
        };
        previous = Some(key);
        if overlay.insert(key, mutation).is_some() {
            return Err("duplicate inline mutation".to_owned());
        }
    }
    input.finish()?;
    Ok(InlinePage { base, overlay })
}

fn decode_inline_page_counted(counters: &mut Counters, bytes: &[u8]) -> Result<InlinePage, String> {
    let page = decode_inline_page(bytes)?;
    counters.decoded_pages += 1;
    counters.decoded_rows += page.base.len() as u64;
    counters.inline_reads += 1;
    counters.inline_entries += page.overlay.len() as u64;
    Ok(page)
}

fn inline_overlay_bytes(page: &InlinePage) -> usize {
    let mut bytes = Vec::new();
    for (key, mutation) in &page.overlay {
        encode_inline_mutation(&mut bytes, *key, mutation);
    }
    bytes.len()
}

fn should_compact_inline(page: &InlinePage) -> bool {
    page.overlay.len() >= inline_entry_cap() || inline_overlay_bytes(page) >= inline_byte_cap()
}

fn compact_inline_page(page: &mut InlinePage) -> Result<(), String> {
    for (key, mutation) in std::mem::take(&mut page.overlay) {
        match mutation {
            InlineMutation::Value(value) => {
                page.base.insert(key, value);
            }
            InlineMutation::Tombstone => {
                page.base.remove(&key);
            }
        }
    }
    if page.base.is_empty() {
        return Err("inline compaction cannot remove the complete leaf".to_owned());
    }
    Ok(())
}

fn inline_visible(page: InlinePage) -> BTreeMap<u64, Vec<u8>> {
    let mut rows = page.base;
    for (key, mutation) in page.overlay {
        match mutation {
            InlineMutation::Value(value) => {
                rows.insert(key, value);
            }
            InlineMutation::Tombstone => {
                rows.remove(&key);
            }
        }
    }
    rows
}

fn validate_page_index(rows: &BTreeMap<u64, Vec<u8>>, expected: usize) -> Result<(), String> {
    if rows.keys().any(|key| *key as usize / PAGE_ROWS != expected) {
        return Err("state page/root directory position mismatch".to_owned());
    }
    Ok(())
}

fn decode_visible_page_counted(
    counters: &mut Counters,
    layout: Layout,
    bytes: &[u8],
    expected_index: usize,
) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let rows = match layout {
        Layout::Slotted => decode_page_counted(counters, bytes)?,
        Layout::Inline => inline_visible(decode_inline_page_counted(counters, bytes)?),
    };
    validate_page_index(&rows, expected_index)?;
    Ok(rows)
}

fn inline_codec_controls() {
    let base = (0..PAGE_ROWS as u64)
        .map(|key| (key, tuple(key, 0)))
        .collect::<BTreeMap<_, _>>();
    let mut first = InlinePage {
        base: base.clone(),
        overlay: BTreeMap::new(),
    };
    first.overlay.insert(1, InlineMutation::Value(tuple(1, 2)));
    first.overlay.insert(2, InlineMutation::Tombstone);
    let encoded = encode_inline_page(&first);
    let decoded = decode_inline_page(&encoded).unwrap();
    let visible = inline_visible(decoded);
    assert_eq!(visible.get(&1), Some(&tuple(1, 2)));
    assert!(!visible.contains_key(&2));

    let mut reverse = InlinePage {
        base: base.clone(),
        overlay: BTreeMap::new(),
    };
    reverse.overlay.insert(2, InlineMutation::Tombstone);
    reverse
        .overlay
        .insert(1, InlineMutation::Value(tuple(1, 2)));
    assert_eq!(encode_inline_page(&first), encode_inline_page(&reverse));

    let mut compacted_first = first.clone();
    compact_inline_page(&mut compacted_first).unwrap();
    let mut compacted_reverse = reverse;
    compact_inline_page(&mut compacted_reverse).unwrap();
    assert_eq!(
        encode_inline_page(&compacted_first),
        encode_inline_page(&compacted_reverse)
    );

    let mut duplicate = b"IDP1".to_vec();
    let base_bytes = encode_page(&base);
    duplicate.extend_from_slice(&(base_bytes.len() as u32).to_be_bytes());
    duplicate.extend_from_slice(&base_bytes);
    duplicate.extend_from_slice(&2u16.to_be_bytes());
    encode_inline_mutation(&mut duplicate, 1, &InlineMutation::Tombstone);
    encode_inline_mutation(&mut duplicate, 1, &InlineMutation::Value(tuple(1, 1)));
    assert!(decode_inline_page(&duplicate).is_err());

    let wrong_base = InlinePage {
        base,
        overlay: BTreeMap::from([(
            PAGE_ROWS as u64,
            InlineMutation::Value(tuple(PAGE_ROWS as u64, 1)),
        )]),
    };
    assert!(decode_inline_page(&encode_inline_page(&wrong_base)).is_err());

    let mut malformed = encoded.clone();
    malformed.pop();
    assert!(decode_inline_page(&malformed).is_err());

    // Both pre-compaction and compacted bytes are independently complete;
    // publication selects one immutable object, so a crash cannot expose a
    // partially compacted second authority.
    assert!(decode_inline_page(&encode_inline_page(&first)).is_ok());
    assert!(decode_inline_page(&encode_inline_page(&compacted_first)).is_ok());
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
    std::env::var("EXP_INLINE_PATTERN").unwrap_or_else(|_| "uniform".to_owned())
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
