use lix_backends::{RedbBackend, RedbBackendFactory};
use lix_engine::run_backend_conformance;

#[test]
fn redb_backend_passes_backend_conformance() {
    let factory = RedbBackendFactory::new();

    run_backend_conformance(&factory).assert_no_failures();
}

#[test]
fn redb_backend_exposes_database_path() {
    let temp_dir = tempfile::tempdir().expect("create redb backend temp dir");
    let path = temp_dir.path().join("backend.redb");

    let backend = RedbBackend::open(&path).expect("open redb backend");

    assert_eq!(backend.path(), path.as_path());
}
