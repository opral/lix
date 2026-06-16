use lix_backends::{RocksDbBackend, RocksDbBackendFactory};
use lix_engine::run_backend_conformance;

#[test]
fn rocksdb_backend_passes_backend_conformance() {
    let factory = RocksDbBackendFactory::new();

    run_backend_conformance(&factory).assert_no_failures();
}

#[test]
fn rocksdb_backend_exposes_database_path_and_flushes() {
    let temp_dir = tempfile::tempdir().expect("create rocksdb backend temp dir");
    let path = temp_dir.path().join("backend.rocksdb");

    let backend = RocksDbBackend::open(&path).expect("open rocksdb backend");
    backend.flush().expect("flush rocksdb backend");

    assert_eq!(backend.path(), path.as_path());
}
