use lix_engine::run_backend_conformance;

use super::support::redb_backend::RedbBackendFactory;

#[test]
fn redb_backend_passes_backend_v2_conformance() {
    let factory = RedbBackendFactory::new();

    run_backend_conformance(&factory).assert_no_failures();
}
