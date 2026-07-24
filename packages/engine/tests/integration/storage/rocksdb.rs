use lix_engine::run_storage_conformance;
use lix_rocksdb_storage::{RocksDB, RocksDBFactory};

#[tokio::test]
async fn rocksdb_passes_storage_conformance() {
    let factory = RocksDBFactory::new();

    run_storage_conformance(&factory).await.assert_no_failures();
}

#[test]
fn rocksdb_exposes_database_path_and_flushes() {
    let temp_dir = tempfile::tempdir().expect("create rocksdb storage temp dir");
    let path = temp_dir.path().join("storage.rocksdb");

    let storage = RocksDB::open(&path).expect("open rocksdb storage");
    storage.flush().expect("flush rocksdb storage");

    assert_eq!(storage.path(), path.as_path());
}
