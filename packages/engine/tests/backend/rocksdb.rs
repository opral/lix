use lix_engine::run_backend_conformance;

use super::support::rocksdb_backend::RocksDbBackendFactory;

#[test]
fn rocksdb_backend_passes_backend_conformance() {
    let factory = RocksDbBackendFactory::new();

    run_backend_conformance(&factory).assert_no_failures();
}
