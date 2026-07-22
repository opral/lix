use std::env;
use std::future::Future;
use std::process::Command;
use std::sync::mpsc;
use std::time::Duration;

use bytes::Bytes;
use lix_engine::storage::{
    GetOptions, Key, ProjectedValue, PutBatch, PutEntry, ReadOptions, SpaceId, Storage,
    StorageRead, StorageWrite, StoredValue, WriteOptions,
};
use lix_rocksdb_storage::RocksDB;

#[test]
fn same_process_open_reuses_shared_database_handle() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let path = temp_dir.path().join("storage.rocksdb");
    let space = SpaceId(0x0005_0003);
    let storage_a = RocksDB::open(&path).expect("open first storage");
    let storage_b = RocksDB::open(&path).expect("open second storage");

    put_one(
        &storage_a,
        space,
        Key(Bytes::from_static(b"from-a")),
        Bytes::from_static(b"a"),
    );
    assert_eq!(
        read_one(&storage_b, space, Key(Bytes::from_static(b"from-a"))),
        Some(Bytes::from_static(b"a"))
    );

    put_one(
        &storage_b,
        space,
        Key(Bytes::from_static(b"from-b")),
        Bytes::from_static(b"b"),
    );
    assert_eq!(
        read_one(&storage_a, space, Key(Bytes::from_static(b"from-b"))),
        Some(Bytes::from_static(b"b"))
    );
}

#[test]
fn same_process_writes_are_serialized_across_reopened_handles() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let path = temp_dir.path().join("storage.rocksdb");
    let storage_a = RocksDB::open(&path).expect("open first storage");
    let storage_b = RocksDB::open(&path).expect("open second storage");
    let write_a =
        block_on(storage_a.begin_write(WriteOptions::default())).expect("begin first write");

    let (attempt_tx, attempt_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();
    let waiter = std::thread::spawn(move || {
        attempt_tx.send(()).expect("signal write attempt");
        let write_b =
            block_on(storage_b.begin_write(WriteOptions::default())).expect("begin second write");
        acquired_tx.send(()).expect("signal write acquired");
        block_on(write_b.rollback()).expect("rollback second write");
    });

    attempt_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("second write should be attempted");
    assert!(
        acquired_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "second write should wait while the first write is active"
    );

    block_on(write_a.rollback()).expect("rollback first write");
    acquired_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("second write should acquire after first write closes");
    waiter.join().expect("writer thread should finish");
}

#[test]
fn writes_large_values_to_blob_files() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let path = temp_dir.path().join("storage.rocksdb");
    let storage = RocksDB::open(&path).expect("open storage");
    put_one(
        &storage,
        SpaceId(0x0005_0003),
        Key(Bytes::from_static(b"large-value")),
        Bytes::from(vec![7; 128 * 1024]),
    );
    storage.flush().expect("flush storage");
    drop(storage);

    assert!(
        rocksdb_blob_file_count(&path) > 0,
        "large values should be stored in RocksDB blob files"
    );
}

#[test]
fn cross_process_open_reports_locked_database() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let path = temp_dir.path().join("storage.rocksdb");
    let _storage = RocksDB::open(&path).expect("open parent storage");
    let test_binary = env::current_exe().expect("current test binary path should resolve");

    let output = Command::new(test_binary)
        .arg("--exact")
        .arg("cross_process_open_helper")
        .arg("--nocapture")
        .env("LIX_ROCKSDB_LOCK_HELPER_PATH", &path)
        .output()
        .expect("spawn rocksdb lock helper");

    assert!(
        output.status.success(),
        "helper should observe locked RocksDB database\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn cross_process_open_helper() {
    let Some(path) = env::var_os("LIX_ROCKSDB_LOCK_HELPER_PATH") else {
        return;
    };

    let Err(error) = RocksDB::open(path) else {
        panic!("child process should not open RocksDB while parent holds the database lock");
    };

    assert!(
        error
            .to_string()
            .contains("already open by another process"),
        "lock error should be mapped clearly: {error}"
    );
}

fn put_one(storage: &RocksDB, space: SpaceId, key: Key, value: Bytes) {
    let mut write = block_on(storage.begin_write(WriteOptions::default())).expect("begin write");
    block_on(write.put_many(
        space,
        PutBatch {
            entries: vec![PutEntry {
                key,
                value: StoredValue { bytes: value },
            }],
        },
    ))
    .expect("put one row");
    block_on(write.commit()).expect("commit write");
}

fn read_one(storage: &RocksDB, space: SpaceId, key: Key) -> Option<Bytes> {
    let read = block_on(storage.begin_read(ReadOptions::default())).expect("begin read");
    let result =
        block_on(read.get_many(space, &[key], GetOptions::default())).expect("read one row");
    result.values[0].clone().map(|value| match value {
        ProjectedValue::FullValue(bytes) => bytes,
        ProjectedValue::KeyOnly => Bytes::new(),
    })
}

fn rocksdb_blob_file_count(path: &std::path::Path) -> usize {
    std::fs::read_dir(path)
        .expect("read rocksdb directory")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "blob")
        })
        .count()
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build test runtime")
        .block_on(future)
}
