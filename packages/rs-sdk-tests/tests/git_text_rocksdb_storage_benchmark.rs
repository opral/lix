//! Manual RocksDB accounting for Git-text's derived materialization.
//!
//! The two arms use identical Component-v2 WASM, schema, corpus, and public
//! `lix_file` writes. They differ only in the plugin manifest's durable
//! materialization contract. A clean plugin-install baseline is subtracted
//! from each arm so the archive and schema do not bias the comparison.

use lix_engine::ReadOptions;
use lix_engine::storage_adapter::StorageAdapter;
use lix_engine::storage_bench::layout_accounting;
use lix_rocksdb_storage::RocksDB;
use lix_sdk::{Lix, Storage, Value, open_lix_with_storage};
use std::fs;
use std::io::{Cursor, Write};
use std::path::Path;
use std::time::{Duration, Instant};

const PLUGIN_KEY: &str = "plugin_git_text";
const CORPUS_BYTES: usize = 16 * 1024 * 1024;
const CORPUS_PATH: &str = "/git-text-storage-corpus.txt";

#[derive(Debug, Clone, Copy, Default)]
struct LogicalLayout {
    total_bytes: u64,
    binary_cas_chunk_rows: u64,
    binary_cas_chunk_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct StorageDelta {
    logical: LogicalLayout,
    physical_bytes: u64,
}

#[derive(Debug)]
struct StorageArm {
    materialization: &'static str,
    write_elapsed: Duration,
    delta: StorageDelta,
}

/// Measures the raw RocksDB data-model cost of keeping Git-text source bytes
/// as a CAS blob versus deriving them from its durable line rows.
///
/// Run manually because this intentionally writes two independent 16 MiB
/// stores:
///
/// ```text
/// TMPDIR=/dev/shm CARGO_TARGET_DIR=/dev/shm/lix-derived-target \
/// CARGO_BUILD_JOBS=1 RUST_TEST_THREADS=1 \
/// cargo test -p lix_sdk_tests --test git_text_rocksdb_storage_benchmark \
///   git_text_derived_materialization_rocksdb_storage_benchmark \
///   -- --ignored --exact --nocapture
/// ```
#[tokio::test]
#[ignore = "manual RocksDB derived-vs-blob Git-text storage benchmark"]
async fn git_text_derived_materialization_rocksdb_storage_benchmark() {
    let corpus = deterministic_nul_free_corpus(CORPUS_BYTES);
    assert_eq!(corpus.len(), CORPUS_BYTES);
    assert!(
        !corpus.contains(&0),
        "the corpus must stay Git-text eligible (no NUL bytes)"
    );

    let derived = run_arm("derived", &corpus).await;
    let blob = run_arm("blob", &corpus).await;

    assert_eq!(
        derived.delta.logical.binary_cas_chunk_bytes, 0,
        "derived Git-text must not retain source bytes in binary CAS"
    );
    assert_eq!(
        derived.delta.logical.binary_cas_chunk_rows, 0,
        "derived Git-text must not create binary CAS chunks"
    );
    assert!(
        blob.delta.logical.binary_cas_chunk_bytes >= CORPUS_BYTES as u64,
        "blob control must retain the full incompressible source corpus in binary CAS"
    );
    assert!(
        blob.delta.logical.binary_cas_chunk_rows > 0,
        "blob control must create binary CAS chunks"
    );

    let logical_reduction = reduction(
        blob.delta.logical.total_bytes,
        derived.delta.logical.total_bytes,
    );
    let physical_reduction = reduction(blob.delta.physical_bytes, derived.delta.physical_bytes);
    assert!(
        logical_reduction > 0.10,
        "derived materialization must reduce logical storage by more than 10% \
         (derived={} blob={} reduction={logical_reduction:.2}%)",
        derived.delta.logical.total_bytes,
        blob.delta.logical.total_bytes,
    );
    assert!(
        physical_reduction > 0.10,
        "derived materialization must reduce physical RocksDB storage by more than 10% \
         (derived={} blob={} reduction={physical_reduction:.2}%)",
        derived.delta.physical_bytes,
        blob.delta.physical_bytes,
    );

    for arm in [&derived, &blob] {
        eprintln!(
            "git_text_rocksdb_storage materialization={} corpus_bytes={} write_ms={:.3} \
             logical_delta_bytes={} binary_cas_chunk_rows={} binary_cas_chunk_delta_bytes={} \
             physical_directory_delta_bytes={}",
            arm.materialization,
            corpus.len(),
            arm.write_elapsed.as_secs_f64() * 1_000.0,
            arm.delta.logical.total_bytes,
            arm.delta.logical.binary_cas_chunk_rows,
            arm.delta.logical.binary_cas_chunk_bytes,
            arm.delta.physical_bytes,
        );
    }
    eprintln!(
        "git_text_rocksdb_storage comparison logical_reduction_percent={:.2} \
         physical_reduction_percent={:.2}",
        logical_reduction * 100.0,
        physical_reduction * 100.0,
    );
}

async fn run_arm(materialization: &'static str, corpus: &[u8]) -> StorageArm {
    let root = tempfile::tempdir().expect("create RocksDB benchmark directory");
    let database_path = root.path().join("lix.rocksdb");
    let archive = build_plugin_archive(materialization);

    // Baseline after the exact plugin archive has been installed. This removes
    // all fixed schema/WASM/registry footprint from the reported deltas.
    let storage = RocksDB::open(&database_path).expect("open baseline RocksDB");
    let lix = open_lix_with_storage(storage.clone())
        .await
        .expect("open baseline Lix workspace");
    install_plugin(&lix, &archive).await;
    lix.close().await.expect("close baseline Lix workspace");
    drop(lix);
    let baseline_layout = read_layout(&storage).await;
    storage.flush().expect("flush baseline RocksDB");
    drop(storage);
    let baseline_physical_bytes = directory_bytes(&database_path);

    // Reopen from the clean baseline, use only the public file API, prove the
    // immediate and cold reads are byte-perfect, then cleanly flush and close
    // before physical directory accounting.
    let storage = RocksDB::open(&database_path).expect("reopen data RocksDB");
    let lix = open_lix_with_storage(storage.clone())
        .await
        .expect("open data Lix workspace");
    let write_started = Instant::now();
    write_file(&lix, CORPUS_PATH, corpus).await;
    let write_elapsed = write_started.elapsed();
    assert_eq!(
        read_file(&lix, CORPUS_PATH).await,
        corpus,
        "{materialization} warm public read must preserve the corpus"
    );
    lix.close().await.expect("close written Lix workspace");
    drop(lix);

    let cold_lix = open_lix_with_storage(storage.clone())
        .await
        .expect("reopen cold Lix workspace");
    assert_eq!(
        read_file(&cold_lix, CORPUS_PATH).await,
        corpus,
        "{materialization} cold public read must preserve the corpus"
    );
    cold_lix.close().await.expect("close cold Lix workspace");
    drop(cold_lix);

    let data_layout = read_layout(&storage).await;
    storage.flush().expect("flush data RocksDB");
    drop(storage);
    let data_physical_bytes = directory_bytes(&database_path);

    StorageArm {
        materialization,
        write_elapsed,
        delta: StorageDelta {
            logical: subtract_layout(data_layout, baseline_layout),
            physical_bytes: data_physical_bytes
                .checked_sub(baseline_physical_bytes)
                .expect("data RocksDB directory must not shrink below its baseline"),
        },
    }
}

async fn read_layout(storage: &RocksDB) -> LogicalLayout {
    let storage = StorageAdapter::new(storage.clone());
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("open layout accounting read");
    let rows = layout_accounting(&read).await;
    LogicalLayout {
        total_bytes: rows.iter().map(|row| row.key_bytes + row.value_bytes).sum(),
        binary_cas_chunk_rows: rows
            .iter()
            .find(|row| row.space == "binary_cas.chunk")
            .map_or(0, |row| row.rows),
        binary_cas_chunk_bytes: rows
            .iter()
            .find(|row| row.space == "binary_cas.chunk")
            .map_or(0, |row| row.key_bytes + row.value_bytes),
    }
}

fn subtract_layout(after: LogicalLayout, baseline: LogicalLayout) -> LogicalLayout {
    LogicalLayout {
        total_bytes: after
            .total_bytes
            .checked_sub(baseline.total_bytes)
            .expect("data logical layout must not shrink below its baseline"),
        binary_cas_chunk_rows: after
            .binary_cas_chunk_rows
            .checked_sub(baseline.binary_cas_chunk_rows)
            .expect("data binary-CAS chunk rows must not shrink below baseline"),
        binary_cas_chunk_bytes: after
            .binary_cas_chunk_bytes
            .checked_sub(baseline.binary_cas_chunk_bytes)
            .expect("data binary-CAS chunk bytes must not shrink below baseline"),
    }
}

fn reduction(control: u64, candidate: u64) -> f64 {
    assert!(
        control > candidate,
        "derived arm must use less storage than blob"
    );
    (control - candidate) as f64 / control as f64
}

async fn install_plugin<StorageImpl>(lix: &Lix<StorageImpl>, archive: &[u8])
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    write_file(
        lix,
        &format!("/.lix/plugins/{PLUGIN_KEY}.lixplugin"),
        archive,
    )
    .await;
}

async fn write_file<StorageImpl>(lix: &Lix<StorageImpl>, path: &str, data: &[u8])
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET data = excluded.data",
        &[
            Value::Text(path.to_owned()),
            Value::Blob(data.to_vec().into()),
        ],
    )
    .await
    .expect("write benchmark file");
}

async fn read_file<StorageImpl>(lix: &Lix<StorageImpl>, path: &str) -> Vec<u8>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT data FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .expect("read benchmark file")
    .rows()
    .first()
    .expect("benchmark file should exist")
    .get::<Vec<u8>>("data")
    .expect("benchmark file data should be bytes")
}

fn build_plugin_archive(materialization: &str) -> Vec<u8> {
    assert!(
        matches!(materialization, "derived" | "blob"),
        "benchmark supports only the two materialization contracts"
    );
    let manifest = include_str!("../../../plugins/text/manifest.json").replace(
        "\"materialization\": \"derived\"",
        &format!("\"materialization\": \"{materialization}\""),
    );
    assert!(
        manifest.contains(&format!("\"materialization\": \"{materialization}\"")),
        "benchmark archive must differ only in its materialization contract"
    );
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_GIT_TEXT_plugin_git_text"));
    let wasm = fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built Git text plugin wasm at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        ("manifest.json", manifest.as_bytes()),
        (
            "schema/git_text_line_v2.json",
            include_str!("../../../plugins/text/schema/git_text_line_v2.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer
            .start_file(path, options)
            .expect("archive entry should start");
        writer.write_all(bytes).expect("archive entry should write");
    }
    writer.finish().expect("archive should finish").into_inner()
}

/// XorShift yields a reproducible high-entropy byte stream. Mapping the rare
/// zero byte to `0xff` is the only distortion; it keeps every byte Git text
/// under the 8 KiB NUL heuristic without making the corpus compressible.
fn deterministic_nul_free_corpus(bytes: usize) -> Vec<u8> {
    let mut state = 0x7d21_9ac5_4e03_b6f1_u64;
    (0..bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let byte = (state >> 56) as u8;
            if byte == 0 { 0xff } else { byte }
        })
        .collect()
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    if !metadata.is_dir() {
        return 0;
    }
    fs::read_dir(path)
        .expect("read RocksDB benchmark directory")
        .map(|entry| directory_bytes(&entry.expect("read RocksDB entry").path()))
        .sum()
}
