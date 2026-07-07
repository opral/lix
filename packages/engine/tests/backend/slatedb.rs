use bytes::Bytes;
use lix_backends::{SlateDbBackend, SlateDbBackendFactory};
use lix_engine::backend::{
    Backend, BackendError, BackendRead, BackendWrite, CoreProjection, Key, KeyRange, KeyRef,
    ProjectedValueRef, PutBatch, PutEntry, ReadOptions, ScanOptions, SpaceId, StoredValue,
    WriteOptions,
};
use lix_engine::run_backend_conformance;
use std::ops::Bound;

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

#[test]
fn slatedb_backend_streams_unbounded_scan_limits() {
    let temp_dir = tempfile::tempdir().expect("create slatedb backend temp dir");
    let path = temp_dir.path().join("backend.slatedb");
    let backend = SlateDbBackend::open(path).expect("open slatedb backend");
    let mut write = backend
        .begin_write(WriteOptions::default())
        .expect("begin slatedb write");

    write
        .put_many(
            SpaceId(1),
            PutBatch {
                entries: (0..10u8)
                    .map(|index| PutEntry {
                        key: Key(Bytes::from(format!("k{index:04}"))),
                        value: StoredValue {
                            bytes: Bytes::from_static(b"value"),
                        },
                    })
                    .collect(),
            },
        )
        .expect("put slatedb rows");
    write.commit().expect("commit slatedb rows");

    let read = backend
        .begin_read(ReadOptions::default())
        .expect("begin slatedb read");
    let mut rows = 0usize;
    let result = read
        .scan(
            SpaceId(1),
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            ScanOptions {
                projection: CoreProjection::KeyOnly,
                limit_rows: usize::MAX,
                resume_after: None,
            },
            &mut |_key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                assert_eq!(value, ProjectedValueRef::KeyOnly);
                rows += 1;
                Ok(())
            },
        )
        .expect("scan slatedb rows");

    assert_eq!(rows, 10);
    assert_eq!(result.emitted, 10);
    assert!(!result.has_more);
}
