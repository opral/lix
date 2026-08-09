//! Public adapter smoke/semantic gate for the prepared-CAS streaming owner.
//!
//! This intentionally stays outside production crates. It proves the public
//! receipt path, one visible semantic transaction, deterministic materialized
//! bytes, rollback invisibility, malformed chunk rejection, and cold reopen on
//! all three adapters. Private retained-byte and GC counters remain a separate
//! required owner-side gate; this test records them as unobserved rather than
//! fabricating measurements.

use std::future::Future;
use std::path::PathBuf;

use lix::storage::Storage;
use lix::{Lix, Memory, Value, open_lix};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use lix_storage_rocksdb::RocksDB;
#[cfg(feature = "slatedb")]
use lix_storage_slatedb::SlateDB;

const FILES: usize = 65;
const CHUNK_BYTES: usize = 1024 * 1024;
const MARKER_KEY: &str = "prepared-cas-streaming-adapter-marker";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_cas_streaming_memory() {
    let storage = Memory::new();
    run_case("memory", storage, |storage| async move {
        let snapshot = storage
            .export_snapshot()
            .expect("export Memory cold-reopen snapshot");
        Memory::from_snapshot(&snapshot).expect("reopen Memory snapshot")
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_cas_streaming_rocksdb() {
    let temp = TempDir::new().expect("create RocksDB prepared-CAS directory");
    let path = temp.path().join("rocksdb");
    let storage = RocksDB::open(&path).expect("open RocksDB prepared-CAS storage");
    run_case("rocksdb", storage, move |storage| {
        let path = path.clone();
        async move {
            storage.flush().expect("flush RocksDB prepared-CAS storage");
            RocksDB::open(path).expect("cold reopen RocksDB prepared-CAS storage")
        }
    })
    .await;
}

#[cfg(feature = "slatedb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_cas_streaming_slatedb() {
    let temp = TempDir::new().expect("create SlateDB prepared-CAS directory");
    let path = temp.path().join("slatedb");
    let storage = SlateDB::open(&path).expect("open SlateDB prepared-CAS storage");
    run_case("slatedb", storage, move |storage| {
        let path = path.clone();
        async move {
            storage
                .flush()
                .await
                .expect("flush SlateDB prepared-CAS storage");
            SlateDB::open(path).expect("cold reopen SlateDB prepared-CAS storage")
        }
    })
    .await;
}

async fn run_case<S, R, F>(adapter: &str, storage: S, reopen: R)
where
    S: Storage + Clone + Send + Sync + 'static,
    R: FnOnce(S) -> F,
    F: Future<Output = S>,
{
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize prepared-CAS adapter");
    let expected_digest = expected_digest();
    let mut transaction = lix
        .begin_transaction()
        .await
        .expect("begin one prepared-CAS semantic transaction");

    for index in 0..FILES {
        let id = format!("prepared-cas-{index:03}");
        let path = format!("/prepared-cas/{index:03}.bin");
        let content = chunk_for(index);
        let receipt = transaction
            .stage_prepared_file_content_chunk(
                format!("prepared-cas-upload-{index:03}"),
                CHUNK_BYTES as u64,
                0,
                content,
                true,
            )
            .await
            .expect("stage one authenticated prepared chunk")
            .expect("final chunk returns a receipt");
        transaction
            .upsert_prepared_file_content(
                id,
                path,
                receipt,
                Some(serde_json::json!({"adapter": adapter, "index": index})),
            )
            .await
            .expect("bind prepared receipt to visible file row");
    }
    transaction
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
            &[
                Value::Text(MARKER_KEY.to_owned()),
                Value::Json(serde_json::json!({"files": FILES, "adapter": adapter})),
            ],
        )
        .await
        .expect("stage one semantic marker");
    transaction
        .commit()
        .await
        .expect("commit exactly one prepared-CAS semantic transaction");

    assert_eq!(read_file_count(&lix).await, FILES);
    assert_eq!(read_file_digest(&lix).await, expected_digest);
    assert_eq!(read_marker_count(&lix).await, 1);

    let mut rollback = lix
        .begin_transaction()
        .await
        .expect("begin rollback transaction");
    let rollback_receipt = rollback
        .stage_prepared_file_content_chunk(
            "prepared-cas-rollback",
            CHUNK_BYTES as u64,
            0,
            vec![0x5a; CHUNK_BYTES],
            true,
        )
        .await
        .expect("stage rollback receipt")
        .expect("rollback receipt exists");
    rollback
        .upsert_prepared_file_content(
            "prepared-cas-rollback".to_owned(),
            "/prepared-cas/rollback.bin".to_owned(),
            rollback_receipt,
            None,
        )
        .await
        .expect("stage rollback row");
    drop(rollback);
    assert_eq!(read_path_count(&lix, "/prepared-cas/rollback.bin").await, 0);

    let mut malformed = lix
        .begin_transaction()
        .await
        .expect("begin malformed-receipt transaction");
    let malformed_result = malformed
        .stage_prepared_file_content_chunk(
            "prepared-cas-malformed",
            CHUNK_BYTES as u64,
            1,
            vec![0x11; CHUNK_BYTES],
            true,
        )
        .await;
    assert!(
        malformed_result.is_err(),
        "misaligned chunk must fail closed"
    );
    drop(malformed);

    let reopened_storage = reopen(storage).await;
    let reopened = open_lix()
        .with_storage(reopened_storage)
        .await
        .expect("open prepared-CAS adapter after cold reopen");
    assert_eq!(read_file_count(&reopened).await, FILES);
    assert_eq!(read_file_digest(&reopened).await, expected_digest);
    assert_eq!(read_marker_count(&reopened).await, 1);
    append_observable_result(adapter, &expected_digest);
    reopened
        .close()
        .await
        .expect("close reopened prepared-CAS adapter");
}

fn chunk_for(index: usize) -> Vec<u8> {
    vec![(index % 251) as u8; CHUNK_BYTES]
}

fn expected_digest() -> String {
    let mut digest = Sha256::new();
    for index in 0..FILES {
        digest.update(chunk_for(index));
    }
    hex_digest(&digest.finalize())
}

async fn read_file_count<S: Storage + Clone + Send + Sync + 'static>(lix: &Lix<S>) -> usize {
    lix.execute(
        "SELECT id FROM lix_file WHERE path LIKE '/prepared-cas/%' ORDER BY id",
        &[],
    )
    .await
    .expect("read prepared-CAS file rows")
    .rows()
    .len()
}

async fn read_path_count<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
    path: &str,
) -> usize {
    lix.execute(
        "SELECT id FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .expect("read rollback path")
    .rows()
    .len()
}

async fn read_marker_count<S: Storage + Clone + Send + Sync + 'static>(lix: &Lix<S>) -> usize {
    lix.execute(
        "SELECT key FROM lix_key_value WHERE key = $1",
        &[Value::Text(MARKER_KEY.to_owned())],
    )
    .await
    .expect("read semantic marker")
    .rows()
    .len()
}

async fn read_file_digest<S: Storage + Clone + Send + Sync + 'static>(lix: &Lix<S>) -> String {
    let result = lix
        .execute(
            "SELECT id, content FROM lix_file WHERE path LIKE '/prepared-cas/%' ORDER BY id",
            &[],
        )
        .await
        .expect("read prepared-CAS content rows");
    let mut digest = Sha256::new();
    for row in result.rows() {
        let content = row
            .get::<Vec<u8>>("content")
            .expect("prepared-CAS content is bytes");
        digest.update(content);
    }
    hex_digest(&digest.finalize())
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn append_observable_result(adapter: &str, digest: &str) {
    let dir = std::env::var_os("PREPARED_CAS_RESULT_DIR").map(PathBuf::from);
    let Some(dir) = dir else { return };
    std::fs::create_dir_all(&dir).expect("create prepared-CAS result directory");
    let path = dir.join("public-adapter-results.tsv");
    let new_file = !path.exists();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open prepared-CAS public result file");
    if new_file {
        use std::io::Write as _;
        writeln!(
            file,
            "adapter\tfiles\tpayload_bytes\tsemantic_commits\tmarker_rows\ttree_digest\trollback_visible_rows\tmalformed_chunk_failures\tcold_reopen\tstrict_retained_bytes\torphan_reclamation"
        )
        .expect("write prepared-CAS result header");
    }
    use std::io::Write as _;
    writeln!(
        file,
        "{adapter}\t{FILES}\t{}\t1\t1\t{digest}\t0\t1\ttrue\tUNOBSERVED\tUNOBSERVED",
        FILES * CHUNK_BYTES
    )
    .expect("write prepared-CAS public result row");
}
