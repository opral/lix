//! Physical-layout discriminator for schema-owned append-only VCS history.
//!
//! This is deliberately a model, not a production compatibility format. It
//! compares the current commit-first segment/change-locator geometry with one
//! sole-payload-owner geometry: immutable schema chunks selected by an
//! authenticated rolling schema root, commit ordinal references, and one
//! repository head.

use std::ops::Bound;
use std::path::Path;
use std::time::{Duration, Instant};

use blake3::Hasher;
use bytes::Bytes;
use lix::storage::{
    BeginScanOptions, CoreProjection, GetManyRequest, GetOptions, Key, KeyRange, PutBatch,
    PutEntry, ReadOptions, Storage, StorageRead, StorageSpace, StorageWrite, StoredValue,
    WriteOptions,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

const SCHEMAS: usize = 8;
const DESCRIPTOR_BYTES: usize = 96;
const CURRENT_META: StorageSpace =
    StorageSpace::immutable(lix::storage::SpaceId(0x7f20_0001), "model.current.meta");
const CURRENT_SEGMENT: StorageSpace =
    StorageSpace::immutable(lix::storage::SpaceId(0x7f20_0002), "model.current.segment");
const CURRENT_LOCATOR: StorageSpace =
    StorageSpace::mutable(lix::storage::SpaceId(0x7f20_0003), "model.current.locator");
const CURRENT_HEAD: StorageSpace =
    StorageSpace::mutable(lix::storage::SpaceId(0x7f20_0004), "model.current.head");
const CHUNK: StorageSpace =
    StorageSpace::immutable(lix::storage::SpaceId(0x7f20_0011), "model.schema.chunk");
const PACKED_COMMIT: StorageSpace =
    StorageSpace::immutable(lix::storage::SpaceId(0x7f20_0013), "model.schema.commit");
const PACKED_HEAD: StorageSpace =
    StorageSpace::mutable(lix::storage::SpaceId(0x7f20_0014), "model.schema.head");

#[derive(Clone, Copy, Debug)]
enum Layout {
    Current,
    SchemaChunks,
}

#[derive(Default)]
struct Metrics {
    publication: Vec<Duration>,
    logical_bytes: u64,
    puts: u64,
    put_many_calls: u64,
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let histories = args
        .get(1)
        .map(|value| parse_list(value))
        .unwrap_or_else(|| vec![1_000]);
    let widths = args
        .get(2)
        .map(|value| parse_list(value))
        .unwrap_or_else(|| vec![1, 100]);
    let samples = args
        .get(3)
        .and_then(|value| value.parse().ok())
        .unwrap_or(5);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build schema-history model runtime");
    runtime.block_on(async move {
        for history in histories {
            for &width in &widths {
                run_rocks(history, width, samples).await;
                run_slate(history, width, samples).await;
            }
        }
    });
}

async fn run_rocks(history: usize, width: usize, samples: usize) {
    for layout in [Layout::Current, Layout::SchemaChunks] {
        let dir = tempfile::tempdir().expect("create Rocks model directory");
        let path = dir.path().join("db");
        let storage = RocksDB::open(&path).expect("open Rocks model");
        run_case(
            "rocksdb", layout, &storage, &path, history, width, samples, None,
        )
        .await;
        storage.flush().expect("flush Rocks model");
        drop(storage);
        let reopened = RocksDB::open(&path).expect("reopen Rocks model");
        measure_reopen("rocksdb", layout, &reopened, &path, history, width).await;
    }
}

async fn run_slate(history: usize, width: usize, samples: usize) {
    for layout in [Layout::Current, Layout::SchemaChunks] {
        let dir = tempfile::tempdir().expect("create Slate model directory");
        let path = dir.path().join("db");
        let counters = SlateDBIoCounters::default();
        let storage =
            SlateDB::open_with_io_counters(&path, counters.clone()).expect("open Slate model");
        run_case(
            "slatedb",
            layout,
            &storage,
            &path,
            history,
            width,
            samples,
            Some(&counters),
        )
        .await;
        storage.flush().await.expect("flush Slate model");
        drop(storage);
        let reopened = SlateDB::open(&path).expect("reopen Slate model");
        measure_reopen("slatedb", layout, &reopened, &path, history, width).await;
    }
}

async fn run_case<S: Storage>(
    backend: &str,
    layout: Layout,
    storage: &S,
    path: &Path,
    history: usize,
    width: usize,
    samples: usize,
    io_counters: Option<&SlateDBIoCounters>,
) {
    let io_before = io_counters.map(SlateDBIoCounters::snapshot);
    let cpu_before = process_cpu_us();
    let mut metrics = Metrics::default();
    let mut schema_roots = [[0u8; 32]; SCHEMAS];
    let mut ordinals = [0u64; SCHEMAS];
    for commit in 0..history {
        let started = Instant::now();
        match layout {
            Layout::Current => stage_current(storage, commit, width, &mut metrics).await,
            Layout::SchemaChunks => {
                stage_chunks(
                    storage,
                    commit,
                    width,
                    &mut schema_roots,
                    &mut ordinals,
                    &mut metrics,
                )
                .await
            }
        }
        metrics.publication.push(started.elapsed());
    }
    let mut schema_history = Vec::with_capacity(samples);
    let mut key_history = Vec::with_capacity(samples);
    let mut commit_lookup = Vec::with_capacity(samples);
    let mut checksum = 0u64;
    for _ in 0..samples {
        let started = Instant::now();
        checksum ^= load_commit(storage, layout, history / 2).await;
        commit_lookup.push(started.elapsed());
    }
    for _ in 0..samples {
        let started = Instant::now();
        checksum ^= scan_schema(storage, layout, 0).await;
        schema_history.push(started.elapsed());
    }
    for _ in 0..samples {
        let started = Instant::now();
        checksum ^= scan_key_history(storage, layout, 0, 0).await;
        key_history.push(started.elapsed());
    }
    schema_history.sort_unstable();
    key_history.sort_unstable();
    commit_lookup.sort_unstable();
    metrics.publication.sort_unstable();
    let cpu_us = process_cpu_us().saturating_sub(cpu_before);
    let io = io_counters
        .zip(io_before)
        .map_or_else(Default::default, |(counters, before)| {
            counters.snapshot().saturating_sub(before)
        });
    println!(
        "schema_history_chunks,backend={backend},layout={},H={history},D={width},\
         publish_p50_us={},publish_p95_us={},schema_history_p50_us={},schema_history_p95_us={},\
         key_history_p50_us={},key_history_p95_us={},commit_lookup_p50_us={},\
         puts={},put_many_calls={},logical_bytes={},settled_bytes={},cpu_us={},rss_hwm_kib={},\
         backend_read_objects={},backend_read_bytes={},backend_write_objects={},backend_write_bytes={},checksum={checksum}",
        layout_name(layout),
        micros(percentile(&metrics.publication, 50)),
        micros(percentile(&metrics.publication, 95)),
        micros(percentile(&schema_history, 50)),
        micros(percentile(&schema_history, 95)),
        micros(percentile(&key_history, 50)),
        micros(percentile(&key_history, 95)),
        micros(percentile(&commit_lookup, 50)),
        metrics.puts,
        metrics.put_many_calls,
        metrics.logical_bytes,
        directory_bytes(path),
        cpu_us,
        rss_hwm_kib(),
        io.read_objects,
        io.read_bytes,
        io.write_objects,
        io.write_bytes,
    );
}

async fn stage_current<S: Storage>(
    storage: &S,
    commit: usize,
    width: usize,
    metrics: &mut Metrics,
) {
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin current write");
    let descriptors = descriptors(commit, width);
    let mut meta = Vec::with_capacity(64);
    meta.extend_from_slice(b"LXCUR1");
    meta.extend_from_slice(&(width as u64).to_be_bytes());
    meta.extend_from_slice(blake3::hash(&descriptors).as_bytes());
    put(&mut write, CURRENT_META, commit_key(commit), meta, metrics).await;
    for (segment, body) in descriptors.chunks(128 * DESCRIPTOR_BYTES).enumerate() {
        let mut key = commit_key(commit);
        key.extend_from_slice(&(segment as u32).to_be_bytes());
        put(&mut write, CURRENT_SEGMENT, key, body.to_vec(), metrics).await;
    }
    let mut locators = Vec::with_capacity(width);
    for row in 0..width {
        let mut value = Vec::with_capacity(12);
        value.extend_from_slice(&(commit as u64).to_be_bytes());
        value.extend_from_slice(&(row as u32).to_be_bytes());
        locators.push(PutEntry {
            key: Key(change_key(commit, row).into()),
            value: StoredValue {
                bytes: value.into(),
            },
        });
        metrics.puts += 1;
        metrics.logical_bytes += 28;
    }
    write
        .put_many(CURRENT_LOCATOR, PutBatch { entries: locators })
        .await
        .expect("put locators");
    metrics.put_many_calls += 1;
    put(
        &mut write,
        CURRENT_HEAD,
        b"head".to_vec(),
        commit_key(commit),
        metrics,
    )
    .await;
    write.commit().await.expect("commit current publication");
}

async fn stage_chunks<S: Storage>(
    storage: &S,
    commit: usize,
    width: usize,
    schema_roots: &mut [[u8; 32]; SCHEMAS],
    ordinals: &mut [u64; SCHEMAS],
    metrics: &mut Metrics,
) {
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin chunk write");
    let mut commit_body = Vec::with_capacity(64);
    let mut ordinal_directory = Vec::with_capacity(16 + width * 41);
    let mut chunk_entries = Vec::with_capacity(SCHEMAS.min(width) + 1);
    commit_body.extend_from_slice(b"LXSCH1");
    commit_body.extend_from_slice(&(commit as u64).to_be_bytes());
    ordinal_directory.extend_from_slice(b"LXORD1");
    ordinal_directory.extend_from_slice(&(width as u32).to_be_bytes());
    for schema in 0..SCHEMAS.min(width) {
        let rows = (schema..width).step_by(SCHEMAS).collect::<Vec<_>>();
        if rows.is_empty() {
            continue;
        }
        let ordinal = ordinals[schema];
        let mut chunk = Vec::with_capacity(24 + rows.len() * DESCRIPTOR_BYTES);
        chunk.extend_from_slice(b"LXCHK1");
        chunk.push(schema as u8);
        chunk.extend_from_slice(&ordinal.to_be_bytes());
        chunk.extend_from_slice(&(rows.len() as u32).to_be_bytes());
        for row in rows {
            chunk.extend_from_slice(&descriptor(commit, row));
        }
        let digest = *blake3::hash(&chunk).as_bytes();
        let mut chunk_key = vec![schema as u8];
        chunk_key.extend_from_slice(&ordinal.to_be_bytes());
        push_entry(&mut chunk_entries, chunk_key, chunk, metrics);
        schema_roots[schema] = hash_append(schema, ordinal, schema_roots[schema], digest);
        ordinals[schema] += 1;
        ordinal_directory.push(schema as u8);
        ordinal_directory.extend_from_slice(&ordinal.to_be_bytes());
        ordinal_directory.extend_from_slice(&digest);
    }
    let ordinal_directory_id = *blake3::hash(&ordinal_directory).as_bytes();
    let mut ordinal_directory_key = Vec::with_capacity(33);
    ordinal_directory_key.push(0xff);
    ordinal_directory_key.extend_from_slice(&ordinal_directory_id);
    push_entry(
        &mut chunk_entries,
        ordinal_directory_key,
        ordinal_directory,
        metrics,
    );
    let mut root_directory = Vec::with_capacity(48 + SCHEMAS * 41);
    root_directory.extend_from_slice(b"LXRHR1");
    root_directory.extend_from_slice(&ordinal_directory_id);
    for schema in 0..SCHEMAS {
        root_directory.push(schema as u8);
        root_directory.extend_from_slice(&ordinals[schema].to_be_bytes());
        root_directory.extend_from_slice(&schema_roots[schema]);
    }
    let root_directory_id = *blake3::hash(&root_directory).as_bytes();
    let mut root_directory_key = Vec::with_capacity(33);
    root_directory_key.push(0xfe);
    root_directory_key.extend_from_slice(&root_directory_id);
    push_entry(
        &mut chunk_entries,
        root_directory_key,
        root_directory,
        metrics,
    );
    write
        .put_many(
            CHUNK,
            PutBatch {
                entries: chunk_entries,
            },
        )
        .await
        .expect("put schema chunks and authenticated directories");
    metrics.put_many_calls += 1;
    commit_body.extend_from_slice(&root_directory_id);
    let repository_root_id = *blake3::hash(&commit_body).as_bytes();
    put(
        &mut write,
        PACKED_COMMIT,
        commit_key(commit),
        commit_body,
        metrics,
    )
    .await;
    put(
        &mut write,
        PACKED_HEAD,
        b"head".to_vec(),
        repository_root_id.to_vec(),
        metrics,
    )
    .await;
    write
        .commit()
        .await
        .expect("commit schema-chunk publication");
}

fn push_entry(entries: &mut Vec<PutEntry>, key: Vec<u8>, value: Vec<u8>, metrics: &mut Metrics) {
    metrics.puts += 1;
    metrics.logical_bytes += (key.len() + value.len()) as u64;
    entries.push(PutEntry {
        key: Key(Bytes::from(key)),
        value: StoredValue {
            bytes: Bytes::from(value),
        },
    });
}

async fn put<W: StorageWrite>(
    write: &mut W,
    space: StorageSpace,
    key: Vec<u8>,
    value: Vec<u8>,
    metrics: &mut Metrics,
) {
    metrics.puts += 1;
    metrics.put_many_calls += 1;
    metrics.logical_bytes += (key.len() + value.len()) as u64;
    write
        .put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key: Key(Bytes::from(key)),
                    value: StoredValue {
                        bytes: Bytes::from(value),
                    },
                }],
            },
        )
        .await
        .expect("put model object");
}

async fn scan_schema<S: Storage>(storage: &S, layout: Layout, schema: usize) -> u64 {
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin schema scan");
    let (space, range) = match layout {
        Layout::Current => (CURRENT_SEGMENT, KeyRange::unbounded()),
        Layout::SchemaChunks => (CHUNK, prefix_range(&[schema as u8])),
    };
    let mut cursor = read
        .begin_scan(space, range, BeginScanOptions::default())
        .await
        .expect("open schema scan");
    let rows = cursor.collect_all().await.expect("collect schema scan");
    rows.iter()
        .map(|row| match &row.value {
            lix::storage::ProjectedValue::FullValue(value) => value.len() as u64,
            lix::storage::ProjectedValue::KeyOnly => 0,
        })
        .sum()
}

async fn scan_key_history<S: Storage>(
    storage: &S,
    layout: Layout,
    schema: usize,
    key: usize,
) -> u64 {
    let bytes = scan_schema(storage, layout, schema).await;
    bytes ^ key as u64
}

async fn load_commit<S: Storage>(storage: &S, layout: Layout, commit: usize) -> u64 {
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin commit read");
    let keys = [Key(Bytes::from(commit_key(commit)))];
    let space = match layout {
        Layout::Current => CURRENT_META,
        Layout::SchemaChunks => PACKED_COMMIT,
    };
    let result = read
        .get_many(&[GetManyRequest {
            space,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("load commit");
    result.values[0].as_ref().map_or(0, |value| match value {
        lix::storage::ProjectedValue::FullValue(value) => value.len() as u64,
        lix::storage::ProjectedValue::KeyOnly => 0,
    })
}

async fn measure_reopen<S: Storage>(
    backend: &str,
    layout: Layout,
    storage: &S,
    path: &Path,
    history: usize,
    width: usize,
) {
    let started = Instant::now();
    let checksum = scan_schema(storage, layout, 0).await;
    println!(
        "schema_history_reopen,backend={backend},layout={},H={history},D={width},elapsed_us={},settled_bytes={},checksum={checksum}",
        layout_name(layout),
        micros(started.elapsed()),
        directory_bytes(path),
    );
}

fn descriptors(commit: usize, width: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(width * DESCRIPTOR_BYTES);
    for row in 0..width {
        out.extend_from_slice(&descriptor(commit, row));
    }
    out
}

fn descriptor(commit: usize, row: usize) -> [u8; DESCRIPTOR_BYTES] {
    let mut out = [0u8; DESCRIPTOR_BYTES];
    out[0] = (row % SCHEMAS) as u8;
    out[1..9].copy_from_slice(&(commit as u64).to_be_bytes());
    out[9..17].copy_from_slice(&(row as u64).to_be_bytes());
    let hash = blake3::hash(&out[..17]);
    out[17..49].copy_from_slice(hash.as_bytes());
    out[49..81].copy_from_slice(blake3::hash(hash.as_bytes()).as_bytes());
    out
}

fn hash_append(schema: usize, ordinal: u64, previous: [u8; 32], chunk: [u8; 32]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(b"lix.schema-history.append.v1");
    hasher.update(&(schema as u64).to_be_bytes());
    hasher.update(&ordinal.to_be_bytes());
    hasher.update(&previous);
    hasher.update(&chunk);
    *hasher.finalize().as_bytes()
}

fn commit_key(commit: usize) -> Vec<u8> {
    (commit as u64).to_be_bytes().to_vec()
}
fn change_key(commit: usize, row: usize) -> Vec<u8> {
    let mut key = commit_key(commit);
    key.extend_from_slice(&(row as u64).to_be_bytes());
    key
}
fn prefix_range(prefix: &[u8]) -> KeyRange {
    let mut upper = prefix.to_vec();
    let upper = match upper.iter().rposition(|byte| *byte != u8::MAX) {
        Some(index) => {
            upper[index] += 1;
            upper.truncate(index + 1);
            Bound::Excluded(Key(upper.into()))
        }
        None => Bound::Unbounded,
    };
    KeyRange {
        lower: Bound::Included(Key(Bytes::copy_from_slice(prefix))),
        upper,
    }
}
fn parse_list(value: &str) -> Vec<usize> {
    value
        .split(',')
        .map(|part| part.parse().expect("positive integer list"))
        .collect()
}
fn percentile(values: &[Duration], pct: usize) -> Duration {
    values[(values.len() * pct)
        .div_ceil(100)
        .saturating_sub(1)
        .min(values.len() - 1)]
}
fn micros(value: Duration) -> u128 {
    value.as_micros()
}
fn layout_name(layout: Layout) -> &'static str {
    match layout {
        Layout::Current => "commit_interleaved",
        Layout::SchemaChunks => "schema_append_chunks",
    }
}
fn directory_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path).map_or(0, |entries| {
        entries
            .filter_map(Result::ok)
            .map(|entry| {
                entry.metadata().map_or(0, |meta| {
                    if meta.is_dir() {
                        directory_bytes(&entry.path())
                    } else {
                        meta.len()
                    }
                })
            })
            .sum()
    })
}

fn process_cpu_us() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` initializes the supplied rusage on success.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(result, 0, "read process CPU usage");
    // SAFETY: the successful call above initialized `usage`.
    let usage = unsafe { usage.assume_init() };
    timeval_us(usage.ru_utime).saturating_add(timeval_us(usage.ru_stime))
}

fn timeval_us(value: libc::timeval) -> u64 {
    u64::try_from(value.tv_sec)
        .unwrap_or_default()
        .saturating_mul(1_000_000)
        .saturating_add(u64::try_from(value.tv_usec).unwrap_or_default())
}

fn rss_hwm_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmHWM:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
        })
        .unwrap_or_default()
}

trait UnboundedRange {
    fn unbounded() -> Self;
}
impl UnboundedRange for KeyRange {
    fn unbounded() -> Self {
        Self {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        }
    }
}
