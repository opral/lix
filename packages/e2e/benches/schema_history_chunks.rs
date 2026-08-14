//! Physical-layout discriminator for schema-owned append-only VCS history.
//!
//! This is deliberately a model, not a production compatibility format. It
//! compares the current commit-first segment/change-locator geometry with one
//! sole-payload-owner geometry: immutable schema chunks selected by an
//! authenticated rolling schema root, commit ordinal references, and one
//! repository head.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use blake3::Hasher;
use bytes::Bytes;
use lix::storage::{
    BeginScanOptions, CoreProjection, GetManyRequest, GetOptions, Key, KeyRange, ProjectedValue,
    PutBatch, PutEntry, ReadOptions, Storage, StorageRead, StorageSpace, StorageWrite, StoredValue,
    WriteOptions,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

const SCHEMAS: usize = 8;
const DESCRIPTOR_BYTES: usize = 96;
const REPOSITORY_ID: [u8; 16] = *b"lix-model-repo-1";
static ACTIVE_SCHEMAS: OnceLock<usize> = OnceLock::new();
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
    run_corruption_controls();
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

fn active_schemas() -> usize {
    *ACTIVE_SCHEMAS.get_or_init(|| {
        std::env::var("LIX_SCHEMA_HISTORY_SCHEMAS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(SCHEMAS)
            .clamp(1, SCHEMAS)
    })
}

fn run_corruption_controls() {
    let mut chunk = Vec::new();
    chunk.extend_from_slice(b"LXCHK2");
    chunk.extend_from_slice(&REPOSITORY_ID);
    chunk.push(0);
    chunk.extend_from_slice(&0u64.to_be_bytes());
    chunk.extend_from_slice(&1u32.to_be_bytes());
    chunk.extend_from_slice(&[7u8; DESCRIPTOR_BYTES]);
    let parsed = parse_chunk(&chunk).expect("valid corruption-control chunk");
    assert_eq!(parsed.row_count, 1);
    let digest = *blake3::hash(&chunk).as_bytes();

    let mut directory = Vec::new();
    directory.extend_from_slice(b"LXORD3");
    directory.extend_from_slice(&REPOSITORY_ID);
    directory.extend_from_slice(&1u32.to_be_bytes());
    directory.extend_from_slice(&1u16.to_be_bytes());
    directory.push(0);
    directory.extend_from_slice(&0u64.to_be_bytes());
    directory.extend_from_slice(&digest);
    directory.extend_from_slice(&0u16.to_be_bytes());
    directory.extend_from_slice(&0u32.to_be_bytes());
    assert_eq!(
        parse_ordinal_directory(&directory)
            .expect("valid corruption-control directory")
            .len(),
        1
    );

    let mut truncated = directory.clone();
    truncated.pop();
    assert!(parse_ordinal_directory(&truncated).is_err());
    let mut wrong_repository = directory.clone();
    wrong_repository[6] ^= 1;
    assert!(parse_ordinal_directory(&wrong_repository).is_err());
    let mut bad_chunk_index = directory.clone();
    let last_ref = bad_chunk_index.len() - 6;
    bad_chunk_index[last_ref..last_ref + 2].copy_from_slice(&1u16.to_be_bytes());
    assert!(parse_ordinal_directory(&bad_chunk_index).is_err());

    let chunk_entry = directory[28..69].to_vec();
    let row_refs = directory[69..].to_vec();
    let mut duplicate_chunk = directory[..28].to_vec();
    duplicate_chunk[26..28].copy_from_slice(&2u16.to_be_bytes());
    duplicate_chunk.extend_from_slice(&chunk_entry);
    duplicate_chunk.extend_from_slice(&chunk_entry);
    duplicate_chunk.extend_from_slice(&row_refs);
    assert!(parse_ordinal_directory(&duplicate_chunk).is_err());

    let mut duplicate_row = directory.clone();
    duplicate_row[22..26].copy_from_slice(&2u32.to_be_bytes());
    duplicate_row.extend_from_slice(&0u16.to_be_bytes());
    duplicate_row.extend_from_slice(&0u32.to_be_bytes());
    assert!(parse_ordinal_directory(&duplicate_row).is_err());

    let mut substituted = chunk.clone();
    let last = substituted.len() - 1;
    substituted[last] ^= 1;
    assert_ne!(*blake3::hash(&substituted).as_bytes(), digest);
    assert!(parse_chunk(&chunk[..chunk.len() - 1]).is_err());

    let mut commit = Vec::new();
    commit.extend_from_slice(b"LXSCH2");
    commit.extend_from_slice(&REPOSITORY_ID);
    commit.extend_from_slice(&7u64.to_be_bytes());
    commit.extend_from_slice(&[9u8; 32]);
    assert_eq!(parse_commit(&commit, Some(7)).unwrap(), [9u8; 32]);
    assert!(parse_commit(&commit, Some(8)).is_err());
    assert!(parse_commit(&commit[..commit.len() - 1], Some(7)).is_err());

    println!("schema_history_corruption_controls,status=pass");
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
        checksum = checksum
            .wrapping_mul(0x100_0000_01b3)
            .wrapping_add(load_commit(storage, layout, history / 2).await);
        commit_lookup.push(started.elapsed());
    }
    for _ in 0..samples {
        let started = Instant::now();
        checksum = checksum
            .wrapping_mul(0x100_0000_01b3)
            .wrapping_add(scan_schema(storage, layout, 0).await);
        schema_history.push(started.elapsed());
    }
    for _ in 0..samples {
        let started = Instant::now();
        checksum = checksum
            .wrapping_mul(0x100_0000_01b3)
            .wrapping_add(scan_key_history(storage, layout, 0, 0).await);
        key_history.push(started.elapsed());
    }
    schema_history.sort_unstable();
    key_history.sort_unstable();
    commit_lookup.sort_unstable();
    metrics.publication.sort_unstable();
    assert_ne!(checksum, 0, "model checksum must not cancel");
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
    let schema_count = active_schemas();
    let mut directory_chunks = Vec::with_capacity(schema_count.min(width));
    let mut chunk_entries = Vec::with_capacity(schema_count.min(width) + 1);
    commit_body.extend_from_slice(b"LXSCH2");
    commit_body.extend_from_slice(&REPOSITORY_ID);
    commit_body.extend_from_slice(&(commit as u64).to_be_bytes());
    for schema in 0..schema_count.min(width) {
        let rows = (schema..width).step_by(schema_count).collect::<Vec<_>>();
        if rows.is_empty() {
            continue;
        }
        let ordinal = ordinals[schema];
        let mut chunk = Vec::with_capacity(24 + rows.len() * DESCRIPTOR_BYTES);
        chunk.extend_from_slice(b"LXCHK2");
        chunk.extend_from_slice(&REPOSITORY_ID);
        chunk.push(schema as u8);
        chunk.extend_from_slice(&ordinal.to_be_bytes());
        chunk.extend_from_slice(&(rows.len() as u32).to_be_bytes());
        for &row in &rows {
            chunk.extend_from_slice(&descriptor(commit, row));
        }
        let digest = *blake3::hash(&chunk).as_bytes();
        let mut chunk_key = vec![schema as u8];
        chunk_key.extend_from_slice(&ordinal.to_be_bytes());
        push_entry(&mut chunk_entries, chunk_key, chunk, metrics);
        schema_roots[schema] = hash_append(schema, ordinal, schema_roots[schema], digest);
        ordinals[schema] += 1;
        directory_chunks.push((schema, ordinal, digest));
    }
    let mut ordinal_directory = Vec::with_capacity(28 + directory_chunks.len() * 41 + width * 6);
    ordinal_directory.extend_from_slice(b"LXORD3");
    ordinal_directory.extend_from_slice(&REPOSITORY_ID);
    ordinal_directory.extend_from_slice(&(width as u32).to_be_bytes());
    ordinal_directory.extend_from_slice(&(directory_chunks.len() as u16).to_be_bytes());
    for &(schema, ordinal, digest) in &directory_chunks {
        ordinal_directory.push(schema as u8);
        ordinal_directory.extend_from_slice(&ordinal.to_be_bytes());
        ordinal_directory.extend_from_slice(&digest);
    }
    for row in 0..width {
        let chunk_index = row % schema_count;
        let row_index = row / schema_count;
        ordinal_directory.extend_from_slice(&(chunk_index as u16).to_be_bytes());
        ordinal_directory.extend_from_slice(&(row_index as u32).to_be_bytes());
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
    root_directory.extend_from_slice(b"LXRHR2");
    root_directory.extend_from_slice(&REPOSITORY_ID);
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
    let mut selected_head = Vec::with_capacity(62);
    selected_head.extend_from_slice(b"LXHED2");
    selected_head.extend_from_slice(&REPOSITORY_ID);
    selected_head.extend_from_slice(&(commit as u64).to_be_bytes());
    selected_head.extend_from_slice(&repository_root_id);
    put(
        &mut write,
        PACKED_HEAD,
        b"head".to_vec(),
        selected_head,
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

struct SelectedClosure {
    schema_counts: [u64; SCHEMAS],
    schema_roots: [[u8; 32]; SCHEMAS],
}

struct ParsedChunk {
    schema: usize,
    ordinal: u64,
    row_count: usize,
    payload_bytes: usize,
}

async fn load_selected_closure<S: Storage>(storage: &S) -> SelectedClosure {
    let head = get_exact(storage, PACKED_HEAD, b"head").await;
    assert_eq!(head.len(), 62, "selected head length mismatch");
    assert_eq!(&head[..6], b"LXHED2", "selected head domain mismatch");
    assert_eq!(
        &head[6..22],
        &REPOSITORY_ID,
        "selected head repository mismatch"
    );
    let commit = read_u64(&head[22..30]);
    let expected_commit_digest: [u8; 32] = head[30..62].try_into().unwrap();
    let commit_bytes = get_exact(storage, PACKED_COMMIT, &commit_key(commit as usize)).await;
    assert_eq!(
        *blake3::hash(&commit_bytes).as_bytes(),
        expected_commit_digest,
        "selected commit substitution"
    );
    let root_id =
        parse_commit(&commit_bytes, Some(commit as usize)).expect("authenticate selected commit");
    let mut root_key = Vec::with_capacity(33);
    root_key.push(0xfe);
    root_key.extend_from_slice(&root_id);
    let root_bytes = get_exact(storage, CHUNK, &root_key).await;
    assert_eq!(
        *blake3::hash(&root_bytes).as_bytes(),
        root_id,
        "root-directory substitution"
    );
    let (ordinal_directory_id, schema_counts, schema_roots) =
        parse_root_directory(&root_bytes).expect("authenticate root directory");
    let mut ordinal_key = Vec::with_capacity(33);
    ordinal_key.push(0xff);
    ordinal_key.extend_from_slice(&ordinal_directory_id);
    let ordinal_bytes = get_exact(storage, CHUNK, &ordinal_key).await;
    assert_eq!(
        *blake3::hash(&ordinal_bytes).as_bytes(),
        ordinal_directory_id,
        "ordinal-directory substitution"
    );
    let references =
        parse_ordinal_directory(&ordinal_bytes).expect("authenticate ordinal directory");
    authenticate_referenced_chunks(storage, &references).await;
    SelectedClosure {
        schema_counts,
        schema_roots,
    }
}

async fn authenticate_referenced_chunks<S: Storage>(
    storage: &S,
    references: &[(usize, u64, usize, [u8; 32])],
) {
    let mut unique = BTreeMap::<(usize, u64), ([u8; 32], usize)>::new();
    for &(schema, ordinal, row_index, digest) in references {
        let entry = unique.entry((schema, ordinal)).or_insert((digest, 0));
        assert_eq!(
            entry.0, digest,
            "conflicting chunk digest in ordinal directory"
        );
        entry.1 = entry.1.max(row_index + 1);
    }
    let keys = unique
        .keys()
        .map(|&(schema, ordinal)| {
            let mut key = vec![schema as u8];
            key.extend_from_slice(&ordinal.to_be_bytes());
            Key(Bytes::from(key))
        })
        .collect::<Vec<_>>();
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin member-directory validation read");
    let result = read
        .get_many(&[GetManyRequest {
            space: CHUNK,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("load ordinal-directory chunks");
    for (((schema, ordinal), (digest, referenced_rows)), value) in
        unique.into_iter().zip(result.values.into_iter())
    {
        let Some(ProjectedValue::FullValue(value)) = value else {
            panic!("missing ordinal-directory chunk");
        };
        assert_eq!(
            *blake3::hash(&value).as_bytes(),
            digest,
            "chunk substitution"
        );
        let parsed = parse_chunk(&value).expect("authenticate referenced chunk");
        assert_eq!(parsed.schema, schema, "referenced chunk schema mismatch");
        assert_eq!(parsed.ordinal, ordinal, "referenced chunk ordinal mismatch");
        assert_eq!(
            referenced_rows, parsed.row_count,
            "ordinal-directory membership count mismatch"
        );
    }
}

async fn get_exact<S: Storage>(storage: &S, space: StorageSpace, key: &[u8]) -> Bytes {
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin exact model read");
    let keys = [Key(Bytes::copy_from_slice(key))];
    let result = read
        .get_many(&[GetManyRequest {
            space,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("load exact model object");
    let Some(ProjectedValue::FullValue(value)) = result.values[0].clone() else {
        panic!("missing exact model object");
    };
    value
}

fn parse_commit(bytes: &[u8], expected_commit: Option<usize>) -> Result<[u8; 32], &'static str> {
    if bytes.len() != 62 || &bytes[..6] != b"LXSCH2" {
        return Err("commit envelope is malformed");
    }
    if bytes[6..22] != REPOSITORY_ID {
        return Err("commit repository mismatch");
    }
    if expected_commit.is_some_and(|expected| read_u64(&bytes[22..30]) != expected as u64) {
        return Err("commit ordinal mismatch");
    }
    Ok(bytes[30..62].try_into().unwrap())
}

fn parse_root_directory(
    bytes: &[u8],
) -> Result<([u8; 32], [u64; SCHEMAS], [[u8; 32]; SCHEMAS]), &'static str> {
    if bytes.len() != 54 + SCHEMAS * 41 || &bytes[..6] != b"LXRHR2" {
        return Err("root directory is malformed");
    }
    if bytes[6..22] != REPOSITORY_ID {
        return Err("root directory repository mismatch");
    }
    let ordinal_id = bytes[22..54].try_into().unwrap();
    let mut counts = [0u64; SCHEMAS];
    let mut roots = [[0u8; 32]; SCHEMAS];
    for schema in 0..SCHEMAS {
        let offset = 54 + schema * 41;
        if bytes[offset] as usize != schema {
            return Err("root directory schema order mismatch");
        }
        counts[schema] = read_u64(&bytes[offset + 1..offset + 9]);
        roots[schema].copy_from_slice(&bytes[offset + 9..offset + 41]);
    }
    Ok((ordinal_id, counts, roots))
}

fn parse_ordinal_directory(
    bytes: &[u8],
) -> Result<Vec<(usize, u64, usize, [u8; 32])>, &'static str> {
    if bytes.len() < 28 || &bytes[..6] != b"LXORD3" {
        return Err("ordinal directory is malformed");
    }
    if bytes[6..22] != REPOSITORY_ID {
        return Err("ordinal directory repository mismatch");
    }
    let count = read_u32(&bytes[22..26]) as usize;
    let chunk_count = u16::from_be_bytes(bytes[26..28].try_into().unwrap()) as usize;
    let expected_len = 28usize
        .checked_add(chunk_count.checked_mul(41).ok_or("chunk table overflow")?)
        .and_then(|value| value.checked_add(count.checked_mul(6)?))
        .ok_or("ordinal directory length overflow")?;
    if bytes.len() != expected_len || chunk_count == 0 || chunk_count > SCHEMAS {
        return Err("ordinal directory count mismatch");
    }
    let mut chunks = Vec::with_capacity(chunk_count);
    let mut previous_chunk = None;
    for index in 0..chunk_count {
        let offset = 28 + index * 41;
        let schema = bytes[offset] as usize;
        if schema >= SCHEMAS {
            return Err("ordinal directory schema is invalid");
        }
        let ordinal = read_u64(&bytes[offset + 1..offset + 9]);
        let identity = (schema, ordinal);
        if previous_chunk.is_some_and(|value| value >= identity) {
            return Err("ordinal chunk table order or duplicate mismatch");
        }
        previous_chunk = Some(identity);
        chunks.push((
            schema,
            ordinal,
            bytes[offset + 9..offset + 41].try_into().unwrap(),
        ));
    }
    let mut result = Vec::with_capacity(count);
    for index in 0..count {
        let offset = 28 + chunk_count * 41 + index * 6;
        let chunk_index =
            u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap()) as usize;
        let row_index = read_u32(&bytes[offset + 2..offset + 6]) as usize;
        if chunk_index != index % chunk_count || row_index != index / chunk_count {
            return Err("ordinal directory row order or duplicate mismatch");
        }
        let Some(&(schema, ordinal, digest)) = chunks.get(chunk_index) else {
            return Err("ordinal directory chunk index is invalid");
        };
        result.push((schema, ordinal, row_index, digest));
    }
    Ok(result)
}

fn parse_chunk(bytes: &[u8]) -> Result<ParsedChunk, &'static str> {
    if bytes.len() < 35 || &bytes[..6] != b"LXCHK2" {
        return Err("chunk is malformed");
    }
    if bytes[6..22] != REPOSITORY_ID {
        return Err("chunk repository mismatch");
    }
    let schema = bytes[22] as usize;
    if schema >= SCHEMAS {
        return Err("chunk schema is invalid");
    }
    let ordinal = read_u64(&bytes[23..31]);
    let row_count = read_u32(&bytes[31..35]) as usize;
    let payload_bytes = row_count
        .checked_mul(DESCRIPTOR_BYTES)
        .ok_or("chunk length overflow")?;
    if bytes.len() != 35 + payload_bytes {
        return Err("chunk row count mismatch");
    }
    Ok(ParsedChunk {
        schema,
        ordinal,
        row_count,
        payload_bytes,
    })
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().expect("four-byte integer"))
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("eight-byte integer"))
}

async fn scan_schema<S: Storage>(storage: &S, layout: Layout, schema: usize) -> u64 {
    if matches!(layout, Layout::SchemaChunks) {
        return scan_authenticated_schema(storage, schema).await;
    }
    scan_authenticated_current(storage).await
}

async fn scan_authenticated_current<S: Storage>(storage: &S) -> u64 {
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin current authenticated scan");
    let mut cursor = read
        .begin_scan(
            CURRENT_SEGMENT,
            KeyRange::unbounded(),
            BeginScanOptions::default(),
        )
        .await
        .expect("open current authenticated scan");
    let rows = cursor
        .collect_all()
        .await
        .expect("collect current segments");
    let mut commits = BTreeMap::<usize, Vec<(usize, Bytes)>>::new();
    for row in rows {
        assert_eq!(row.key.0.len(), 12, "current segment key is malformed");
        let commit = read_u64(&row.key.0[..8]) as usize;
        let segment = read_u32(&row.key.0[8..12]) as usize;
        let ProjectedValue::FullValue(value) = row.value else {
            panic!("current authenticated scan requires full values");
        };
        commits.entry(commit).or_default().push((segment, value));
    }
    let keys = commits
        .keys()
        .map(|&commit| Key(Bytes::from(commit_key(commit))))
        .collect::<Vec<_>>();
    let result = read
        .get_many(&[GetManyRequest {
            space: CURRENT_META,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .expect("load current commit metadata");
    let mut total = 0u64;
    for ((commit, mut segments), metadata) in commits.into_iter().zip(result.values.into_iter()) {
        let Some(ProjectedValue::FullValue(metadata)) = metadata else {
            panic!("missing current commit metadata");
        };
        assert_eq!(metadata.len(), 46, "current commit metadata is malformed");
        assert_eq!(&metadata[..6], b"LXCUR1", "current commit domain mismatch");
        let width = read_u64(&metadata[6..14]) as usize;
        segments.sort_unstable_by_key(|(segment, _)| *segment);
        let mut descriptors = Vec::with_capacity(width * DESCRIPTOR_BYTES);
        for (expected, (segment, body)) in segments.into_iter().enumerate() {
            assert_eq!(
                segment, expected,
                "current segment order gap at commit {commit}"
            );
            descriptors.extend_from_slice(&body);
        }
        assert_eq!(descriptors.len(), width * DESCRIPTOR_BYTES);
        assert_eq!(
            blake3::hash(&descriptors).as_bytes(),
            &metadata[14..46],
            "current segment substitution"
        );
        total += descriptors.len() as u64;
    }
    total
}

async fn scan_authenticated_schema<S: Storage>(storage: &S, schema: usize) -> u64 {
    let selected = load_selected_closure(storage).await;
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin authenticated schema scan");
    let mut cursor = read
        .begin_scan(
            CHUNK,
            prefix_range(&[schema as u8]),
            BeginScanOptions::default(),
        )
        .await
        .expect("open authenticated schema scan");
    let rows = cursor.collect_all().await.expect("collect schema chunks");
    let mut root = [0u8; 32];
    let mut expected_ordinal = 0u64;
    let mut payload_bytes = 0u64;
    for row in rows {
        let ProjectedValue::FullValue(value) = row.value else {
            panic!("authenticated schema scan requires full values");
        };
        let parsed = parse_chunk(&value).expect("authenticate schema chunk");
        assert_eq!(parsed.schema, schema, "schema chunk owner mismatch");
        assert_eq!(parsed.ordinal, expected_ordinal, "schema chunk ordinal gap");
        let digest = *blake3::hash(&value).as_bytes();
        root = hash_append(schema, parsed.ordinal, root, digest);
        expected_ordinal += 1;
        payload_bytes += parsed.payload_bytes as u64;
    }
    assert_eq!(
        expected_ordinal, selected.schema_counts[schema],
        "schema chunk count mismatch"
    );
    assert_eq!(root, selected.schema_roots[schema], "schema root mismatch");
    payload_bytes
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
        ProjectedValue::FullValue(value) => {
            if matches!(layout, Layout::SchemaChunks) {
                parse_commit(value, Some(commit)).expect("authenticate exact commit metadata");
            }
            value.len() as u64
        }
        ProjectedValue::KeyOnly => 0,
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
    assert_ne!(checksum, 0, "reopen checksum must be nonzero");
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
    out[0] = (row % active_schemas()) as u8;
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
