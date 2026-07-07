use bytes::Bytes;
use lix_backends::{SlateDbBackend, SlateDbBackendFactory};
use lix_engine::backend::{
    Backend, BackendError, BackendWrite, Key, PutBatch, PutEntry, SpaceId, StoredValue,
    WriteOptions,
};
use lix_engine::run_backend_conformance;

#[test]
fn slatedb_backend_passes_backend_conformance() {
    let factory = SlateDbBackendFactory::new();

    run_backend_conformance(&factory).assert_no_failures();
}

#[test]
fn slatedb_backend_exposes_database_path_and_flushes() {
    let temp_dir = tempfile::tempdir().expect("create slatedb backend temp dir");
    let path = temp_dir.path().join("backend.slatedb");

    let backend = SlateDbBackend::open(&path).expect("open slatedb backend");
    backend.flush().expect("flush slatedb backend");

    assert_eq!(backend.path(), path.as_path());
}

#[test]
fn slatedb_backend_rejects_keys_above_physical_limit() {
    let temp_dir = tempfile::tempdir().expect("create slatedb backend temp dir");
    let path = temp_dir.path().join("backend.slatedb");
    let backend = SlateDbBackend::open(path).expect("open slatedb backend");
    let mut write = backend
        .begin_write(WriteOptions::default())
        .expect("begin slatedb write");

    let too_long_logical_key = Key(Bytes::from(vec![0; u16::MAX as usize - 3]));
    let error = write
        .put_many(
            SpaceId(1),
            PutBatch {
                entries: vec![PutEntry {
                    key: too_long_logical_key,
                    value: StoredValue {
                        bytes: Bytes::new(),
                    },
                }],
            },
        )
        .expect_err("oversized physical key should fail");

    assert_eq!(error, BackendError::InvalidKey);
}
