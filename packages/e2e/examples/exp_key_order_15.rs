//! EXP-KEY-ORDER-15: authenticated typed identity key-order experiment.
//!
//! Compares identical Schema-v1 typed tuple bytes stored as either:
//! - production order: schema -> file owner -> encoded typed PK; or
//! - candidate order: schema -> encoded typed PK -> file owner -> scope bits.
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
    PutEntry, ReadOptions, Storage, StorageRead, StorageSpace, StorageWrite, StoredValue,
    ValueSemantics, WriteOptions,
};
use lix::storage_bench::experiment_storage_space;
use lix_schema::value_layout::{BodyColumn, BodyKind, BodyValue, decode_body, encode_body};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use uuid::Uuid;

const LEDGER: &str = "EXP-KEY-ORDER-15";
const PAGE_ROWS: usize = 64;
const FILE_OWNERS: u64 = 4;
const ROOT_SPACE: StorageSpace =
    experiment_storage_space(0x00fe_2001, "exp.key_order.root", ValueSemantics::Immutable);
const PAGE_SPACE: StorageSpace =
    experiment_storage_space(0x00fe_2002, "exp.key_order.page", ValueSemantics::Immutable);
const SELECTOR_SPACE: StorageSpace = experiment_storage_space(
    0x00fe_2003,
    "exp.key_order.selector",
    ValueSemantics::Mutable,
);

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
    OldOrder,
    NewOrder,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OverlayCell {
    Value(Vec<u8>),
    Null,
    Tombstone,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Self::OldOrder => "old_order",
            Self::NewOrder => "new_order",
        }
    }

    fn tag(self) -> u8 {
        match self {
            Self::OldOrder => 1,
            Self::NewOrder => 2,
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "old_order" => Self::OldOrder,
            "new_order" => Self::NewOrder,
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
    key_codec_controls();
    println!(
        "{LEDGER},kind=control,control=key_codec_corruption,status=pass,cases=typed_key+adjacent_prefix+wrong_owner+duplicate+truncation+tombstone+null+canonical_order"
    );
    println!(
        "{LEDGER},kind=config,page_rows={PAGE_ROWS},file_owners={FILE_OWNERS},pk_kind={},pattern={},sizes={:?},histories={:?}",
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
    let exact_pk_start = (point_key / FILE_OWNERS) * FILE_OWNERS;
    let before = store.counters;
    let started = Instant::now();
    let exact_rows = range(
        &mut store,
        root,
        exact_pk_start,
        exact_pk_start + FILE_OWNERS,
    )
    .await
    .unwrap();
    assert_eq!(
        exact_rows,
        expected
            .range(exact_pk_start..exact_pk_start + FILE_OWNERS)
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
        "typed_pk_exact",
        started.elapsed().as_micros(),
        store.counters.delta(before),
    );
    let mut exact_samples = Vec::with_capacity(sample_count());
    for _ in 0..sample_count() {
        let started = Instant::now();
        let sampled = range(
            &mut store,
            root,
            exact_pk_start,
            exact_pk_start + FILE_OWNERS,
        )
        .await
        .unwrap();
        assert_eq!(sampled, exact_rows);
        exact_samples.push(started.elapsed().as_micros());
    }
    emit_latency_summary(backend, layout, n, h, d, "typed_pk_exact", &exact_samples);

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
    let mut readback_samples = Vec::with_capacity(sample_count());
    for _ in 0..sample_count() {
        let started = Instant::now();
        let sampled = materialize(&mut store, root).await.unwrap();
        assert_eq!(sampled, expected);
        readback_samples.push(started.elapsed().as_micros());
    }
    emit_latency_summary(backend, layout, n, h, d, "readback", &readback_samples);

    let range_start = ((n / 4) as u64 / FILE_OWNERS) * FILE_OWNERS;
    let range_end = (range_start + 100 * FILE_OWNERS).min(n as u64);
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
        "typed_pk_range_100",
        started.elapsed().as_micros(),
        store.counters.delta(before),
    );
    let mut range_samples = Vec::with_capacity(sample_count());
    for _ in 0..sample_count() {
        let started = Instant::now();
        let sampled = range(&mut store, root, range_start, range_end)
            .await
            .unwrap();
        assert_eq!(sampled, ranged);
        range_samples.push(started.elapsed().as_micros());
    }
    emit_latency_summary(
        backend,
        layout,
        n,
        h,
        d,
        "typed_pk_range_100",
        &range_samples,
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
    let mut diff_samples = Vec::with_capacity(sample_count());
    for _ in 0..sample_count() {
        let started = Instant::now();
        let sampled = diff(&mut store, base_root, root).await.unwrap();
        assert_eq!(sampled, changed);
        diff_samples.push(started.elapsed().as_micros());
    }
    emit_latency_summary(backend, layout, n, h, d, "diff", &diff_samples);

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
    let encoded_samples = samples
        .iter()
        .map(u128::to_string)
        .collect::<Vec<_>>()
        .join(":");
    let percentile = |numerator: usize| {
        let index = (sorted.len() * numerator).div_ceil(100).saturating_sub(1);
        sorted[index.min(sorted.len() - 1)]
    };
    println!(
        "{LEDGER},kind=latency_summary,backend={backend},layout={},n={n},h={h},d={d},operation={operation},samples={},p50_us={},p95_us={},samples_us={encoded_samples}",
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
    if !rows.len().is_multiple_of(FILE_OWNERS as usize) {
        return Err("row count must be divisible by file-owner count".to_owned());
    };
    let physical = physical_rows(layout, rows);
    let pages = encode_pages(layout, rows.len() as u64, &physical);
    store.put_objects(PAGE_SPACE, &pages).await?;
    let root = Root {
        layout,
        count: rows.len() as u64,
        pages: pages.iter().map(|(id, _)| *id).collect(),
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
    let physical_mutations = mutations
        .iter()
        .map(|(key, value)| {
            (
                logical_to_physical(root.layout, *key, root.count),
                value.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut page_ids = root.pages.clone();
    let touched = physical_mutations
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
        let mut page = decode_page_counted(&mut store.counters, &bytes, root.layout, root.count)?;
        for (key, value) in physical_mutations
            .range(page.first_key_value().unwrap().0..=page.last_key_value().unwrap().0)
        {
            page.insert(*key, value.clone());
        }
        let bytes = encode_page(root.layout, root.count, &page);
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
        },
    )
    .await
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
    if key >= root.count {
        return Ok(None);
    }
    let physical = logical_to_physical(root.layout, key, root.count);
    let page_index = physical as usize / PAGE_ROWS;
    let Some(page_id) = root.pages.get(page_index).copied() else {
        return Ok(None);
    };
    let bytes = store.get_one(PAGE_SPACE, page_id).await?;
    let page = decode_page_counted(store.counters, &bytes, root.layout, root.count)?;
    validate_page_index(&page, page_index)?;
    Ok(page.get(&physical).cloned())
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
    let end = end.min(root.count);
    if start >= end {
        return Ok(BTreeMap::new());
    }
    let physical_keys = (start..end)
        .map(|logical| logical_to_physical(root.layout, logical, root.count))
        .collect::<BTreeSet<_>>();
    let page_indices = physical_keys
        .iter()
        .map(|key| *key as usize / PAGE_ROWS)
        .collect::<BTreeSet<_>>();
    let page_ids = page_indices
        .iter()
        .map(|index| root.pages[*index])
        .collect::<Vec<_>>();
    let objects = read.get_many(PAGE_SPACE, &page_ids).await?;
    let mut rows = BTreeMap::new();
    for (index, bytes) in page_indices.into_iter().zip(objects) {
        let page = decode_page_counted(read.counters, &bytes, root.layout, root.count)?;
        validate_page_index(&page, index)?;
        for (physical, value) in page {
            let logical = physical_to_logical(root.layout, physical, root.count);
            if start <= logical && logical < end {
                rows.insert(logical, value);
            }
        }
    }
    Ok(rows)
}

async fn materialize_on_read<R: StorageRead>(
    store: &mut Reader<'_, R>,
    root_id: ObjectId,
) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let root = load_root_on_read(store, root_id).await?;
    let mut rows = BTreeMap::new();
    let objects = store.get_many(PAGE_SPACE, &root.pages).await?;
    for (index, bytes) in objects.iter().enumerate() {
        let page = decode_page_counted(store.counters, bytes, root.layout, root.count)?;
        validate_page_index(&page, index)?;
        rows.extend(page.into_iter().map(|(physical, value)| {
            (
                physical_to_logical(root.layout, physical, root.count),
                value,
            )
        }));
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
    diff_pages(store, &before_root, &after_root).await
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
        let before_page = decode_visible_page_counted(
            store.counters,
            &bytes[0],
            index,
            before.layout,
            before.count,
        )?;
        let after_page = decode_visible_page_counted(
            store.counters,
            &bytes[1],
            index,
            after.layout,
            after.count,
        )?;
        changed.extend(
            before_page
                .keys()
                .chain(after_page.keys())
                .copied()
                .filter(|key| before_page.get(key) != after_page.get(key))
                .map(|physical| physical_to_logical(before.layout, physical, before.count)),
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
    if decoded.pages.len() >= 2 {
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

fn encode_pages(
    layout: Layout,
    count: u64,
    rows: &BTreeMap<u64, Vec<u8>>,
) -> Vec<(ObjectId, Vec<u8>)> {
    rows.iter()
        .collect::<Vec<_>>()
        .chunks(PAGE_ROWS)
        .map(|chunk| {
            let page = chunk
                .iter()
                .map(|(key, value)| (**key, (*value).clone()))
                .collect::<BTreeMap<_, _>>();
            let bytes = encode_page(layout, count, &page);
            (ObjectId::of(&bytes), bytes)
        })
        .collect()
}

fn encode_page(layout: Layout, count: u64, rows: &BTreeMap<u64, Vec<u8>>) -> Vec<u8> {
    let mut out = b"KOP1".to_vec();
    out.push(layout.tag());
    out.extend_from_slice(&count.to_be_bytes());
    out.extend_from_slice(&(rows.len() as u32).to_be_bytes());
    for (key, value) in rows {
        let encoded_key = state_key(layout, *key, count);
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
    expected_layout: Layout,
    expected_count: u64,
) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let page = decode_page(bytes, expected_layout, expected_count)?;
    counters.decoded_pages += 1;
    counters.decoded_rows += page.len() as u64;
    Ok(page)
}

fn decode_page(
    bytes: &[u8],
    expected_layout: Layout,
    expected_count: u64,
) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let mut input = Decoder::new(bytes);
    input.magic(b"KOP1")?;
    if input.u8()? != expected_layout.tag() {
        return Err("page/root layout binding mismatch".to_owned());
    }
    if input.u64()? != expected_count {
        return Err("page/root identity-count binding mismatch".to_owned());
    }
    let count = input.u32()? as usize;
    let mut rows = BTreeMap::new();
    let mut previous_encoded_key: Option<Vec<u8>> = None;
    for _ in 0..count {
        let key_len = input.u32()? as usize;
        let encoded_key = input.bytes(key_len)?.to_vec();
        if previous_encoded_key
            .as_ref()
            .is_some_and(|previous| previous >= &encoded_key)
        {
            return Err("typed page keys are not strictly ordered".to_owned());
        }
        let key = input.u64()?;
        if encoded_key != state_key(expected_layout, key, expected_count) {
            return Err("typed StateKey/physical-position binding mismatch".to_owned());
        }
        let len = input.u32()? as usize;
        let value = input.bytes(len)?.to_vec();
        decode_body(&TUPLE_PLAN, &value).map_err(|e| e.to_string())?;
        if rows.insert(key, value).is_some() {
            return Err("duplicate page key".to_owned());
        }
        previous_encoded_key = Some(encoded_key);
    }
    input.finish()?;
    Ok(rows)
}

fn encode_root(root: &Root) -> Vec<u8> {
    let mut out = b"KOR1".to_vec();
    out.push(root.layout.tag());
    out.extend_from_slice(&root.count.to_be_bytes());
    out.extend_from_slice(&(root.pages.len() as u32).to_be_bytes());
    for page in &root.pages {
        out.extend_from_slice(&page.0);
    }
    out
}

fn decode_root(bytes: &[u8]) -> Result<Root, String> {
    let mut input = Decoder::new(bytes);
    input.magic(b"KOR1")?;
    let layout = match input.u8()? {
        1 => Layout::OldOrder,
        2 => Layout::NewOrder,
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
    input.finish()?;
    Ok(Root {
        layout,
        count,
        pages,
    })
}

fn physical_rows(layout: Layout, logical_rows: &BTreeMap<u64, Vec<u8>>) -> BTreeMap<u64, Vec<u8>> {
    let count = logical_rows.len() as u64;
    logical_rows
        .iter()
        .map(|(logical, value)| (logical_to_physical(layout, *logical, count), value.clone()))
        .collect()
}

fn logical_to_physical(layout: Layout, logical: u64, count: u64) -> u64 {
    assert!(count.is_multiple_of(FILE_OWNERS));
    let pk = logical / FILE_OWNERS;
    let owner = logical % FILE_OWNERS;
    match layout {
        Layout::OldOrder => owner * (count / FILE_OWNERS) + pk,
        Layout::NewOrder => logical,
    }
}

fn physical_to_logical(layout: Layout, physical: u64, count: u64) -> u64 {
    assert!(count.is_multiple_of(FILE_OWNERS));
    match layout {
        Layout::OldOrder => {
            let pks = count / FILE_OWNERS;
            let owner = physical / pks;
            let pk = physical % pks;
            pk * FILE_OWNERS + owner
        }
        Layout::NewOrder => physical,
    }
}

fn state_key(layout: Layout, physical: u64, count: u64) -> Vec<u8> {
    let logical = physical_to_logical(layout, physical, count);
    let pk = logical / FILE_OWNERS;
    let owner = logical % FILE_OWNERS;
    let mut out = b"KS1".to_vec();
    out.extend_from_slice(&[0x31; 16]); // stable schema identity
    if layout == Layout::OldOrder {
        out.push(b'f');
        out.extend_from_slice(&owner.to_be_bytes());
    }
    encode_typed_pk(pk, &mut out);
    if layout == Layout::NewOrder {
        out.push(b'f');
        out.extend_from_slice(&owner.to_be_bytes());
    }
    out.extend_from_slice(b"s\0tracked\0local");
    out
}

fn encode_typed_pk(pk: u64, out: &mut Vec<u8>) {
    match pk_kind().as_str() {
        "uuid" => {
            out.push(b'u');
            out.extend_from_slice(&Uuid::from_u128(pk as u128 + 1).into_bytes());
        }
        "text" => {
            let value = format!("pk-{pk:020}");
            out.push(b't');
            out.extend_from_slice(&(value.len() as u32).to_be_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        "composite" => {
            let value = format!("pk-{pk:020}");
            out.push(b'c');
            out.extend_from_slice(&pk.to_be_bytes());
            out.extend_from_slice(&Uuid::from_u128(pk as u128 + 1).into_bytes());
            out.extend_from_slice(&(value.len() as u32).to_be_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        _ => {
            out.push(b'i');
            out.extend_from_slice(&pk.to_be_bytes());
        }
    }
}

fn pk_kind() -> String {
    std::env::var("EXP_KEY_ORDER_PK_KIND").unwrap_or_else(|_| "integer".to_owned())
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
    expected_layout: Layout,
    expected_count: u64,
) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let rows = decode_page_counted(counters, bytes, expected_layout, expected_count)?;
    validate_page_index(&rows, expected_index)?;
    Ok(rows)
}

fn key_codec_controls() {
    let count = 1_000;
    for layout in [Layout::OldOrder, Layout::NewOrder] {
        for logical in 0..count {
            let physical = logical_to_physical(layout, logical, count);
            assert_eq!(physical_to_logical(layout, physical, count), logical);
        }
    }
    let pk = 50;
    let new = (0..FILE_OWNERS)
        .map(|owner| logical_to_physical(Layout::NewOrder, pk * FILE_OWNERS + owner, count))
        .collect::<Vec<_>>();
    assert!(new.windows(2).all(|pair| pair[1] == pair[0] + 1));
    let old = (0..FILE_OWNERS)
        .map(|owner| logical_to_physical(Layout::OldOrder, pk * FILE_OWNERS + owner, count))
        .collect::<Vec<_>>();
    assert!(
        old.windows(2)
            .all(|pair| pair[1] - pair[0] == count / FILE_OWNERS)
    );

    let root = Root {
        layout: Layout::NewOrder,
        count,
        pages: vec![ObjectId([1; 32])],
    };
    let encoded = encode_root(&root);
    assert_eq!(encode_root(&decode_root(&encoded).unwrap()), encoded);
    let mut truncated = encoded;
    truncated.pop();
    assert!(decode_root(&truncated).is_err());

    let rows = BTreeMap::from([(0, tuple(0, 0)), (1, tuple(1, 0))]);
    let canonical = encode_page(Layout::NewOrder, count, &rows);
    assert_eq!(
        decode_page(&canonical, Layout::NewOrder, count).unwrap(),
        rows
    );
    assert!(decode_page(&canonical, Layout::OldOrder, count).is_err());
    assert!(decode_page(&canonical, Layout::NewOrder, count + 4).is_err());
    let mut malformed_key = canonical.clone();
    let first_key_start = 4 + 1 + 8 + 4 + 4;
    malformed_key[first_key_start + 3 + 16 + 1] ^= 1;
    assert!(decode_page(&malformed_key, Layout::NewOrder, count).is_err());
    let mut truncated_page = canonical;
    truncated_page.pop();
    assert!(decode_page(&truncated_page, Layout::NewOrder, count).is_err());

    for layout in [Layout::OldOrder, Layout::NewOrder] {
        let left = state_key(layout, logical_to_physical(layout, 4, count), count);
        let adjacent = state_key(layout, logical_to_physical(layout, 40, count), count);
        assert!(!left.starts_with(&adjacent));
        assert!(!adjacent.starts_with(&left));
    }

    let value = OverlayCell::Value(vec![7]);
    assert_eq!(
        resolve_overlay(Some(value.clone()), None),
        Some(value.clone())
    );
    assert_eq!(
        resolve_overlay(Some(OverlayCell::Null), Some(value.clone())),
        Some(OverlayCell::Null)
    );
    assert_eq!(
        resolve_overlay(Some(OverlayCell::Tombstone), Some(value.clone())),
        None
    );
    assert_eq!(resolve_overlay(None, Some(value.clone())), Some(value));
    assert_eq!(
        resolve_tracked_untracked(
            Some(OverlayCell::Value(vec![1])),
            Some(OverlayCell::Value(vec![2]))
        ),
        Some(OverlayCell::Value(vec![2]))
    );
    assert_eq!(
        resolve_tracked_untracked(
            Some(OverlayCell::Value(vec![1])),
            Some(OverlayCell::Tombstone)
        ),
        None
    );
}

fn resolve_overlay(local: Option<OverlayCell>, global: Option<OverlayCell>) -> Option<OverlayCell> {
    match local.or(global) {
        Some(OverlayCell::Tombstone) | None => None,
        visible => visible,
    }
}

fn resolve_tracked_untracked(
    tracked: Option<OverlayCell>,
    untracked: Option<OverlayCell>,
) -> Option<OverlayCell> {
    match untracked.or(tracked) {
        Some(OverlayCell::Tombstone) | None => None,
        visible => visible,
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

fn mutation_keys(n: usize, d: usize, generation: u64) -> Vec<u64> {
    match mutation_pattern().as_str() {
        "repeated" => vec![(n / 2) as u64],
        "random" => {
            let mut keys = BTreeSet::new();
            let mut state = generation
                ^ experiment_seed().wrapping_mul(0xd6e8_feb8_6659_fd93)
                ^ 0x9e37_79b9_7f4a_7c15;
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
    std::env::var("EXP_KEY_ORDER_PATTERN").unwrap_or_else(|_| "uniform".to_owned())
}

fn experiment_seed() -> u64 {
    std::env::var("EXP_KEY_ORDER_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn sample_count() -> usize {
    std::env::var("EXP_KEY_ORDER_SAMPLES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20)
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
        layouts: list_env("EXP_DELTA_PAGE_LAYOUTS", "old_order,new_order")
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
