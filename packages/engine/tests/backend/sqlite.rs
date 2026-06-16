use lix_backends::SqliteBackendFactory;
use lix_engine::run_backend_conformance;

#[test]
fn sqlite_backend_passes_backend_conformance() {
    let factory = SqliteBackendFactory::new();

    run_backend_conformance(&factory).assert_no_failures();
}
