use lix_engine::run_backend_conformance;

use super::support::sqlite_backend::SqliteBackendFactory;

#[test]
fn sqlite_backend_passes_backend_conformance() {
    let factory = SqliteBackendFactory::new();

    run_backend_conformance(&factory).assert_no_failures();
}
