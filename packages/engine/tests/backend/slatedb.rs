use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, BoxStream};
use lix_backends::{
    SlateDbBackend, SlateDbBackendFactory, SlateDbCacheOptions, SlateDbObjectStoreOptions,
};
use lix_engine::backend::{
    Backend, BackendError, BackendRead, BackendWrite, CoreProjection, GetOptions, Key, KeyRange,
    KeyRef, ProjectedValue, ProjectedValueRef, PutBatch, PutEntry, ReadOptions, ScanOptions,
    SpaceId, StoredValue, WriteOptions, get_many,
};
use lix_engine::{BackendFactory, BackendFixture, BackendTestConfig, run_backend_conformance};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions as ObjectStoreGetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult,
};
use std::ops::Bound;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, atomic::AtomicBool};
use tempfile::TempDir;

#[test]
fn slatedb_backend_passes_backend_conformance() {
    let factory = SlateDbBackendFactory::new();

    run_backend_conformance(&factory).assert_no_failures();
}

#[test]
fn cached_slatedb_backend_passes_backend_conformance() {
    let factory = CachedSlateDbBackendFactory::new();

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

#[test]
fn cached_slatedb_backend_rebuilds_after_local_cache_is_deleted() {
    let object_store = Arc::new(InMemory::new());
    let db_path = "cached-slatedb-rebuild";
    let cache_parent = tempfile::tempdir().expect("create SlateDB cache parent");
    let cache_path = cache_parent.path().join("object-cache");
    let space = SpaceId(11);

    {
        let backend = SlateDbBackend::open_object_store(db_path, object_store.clone())
            .expect("open uncached seed backend");
        let mut write = backend
            .begin_write(WriteOptions::default())
            .expect("begin seed write");
        write
            .put_many(
                space,
                PutBatch {
                    entries: [("alpha", "one"), ("beta", "two"), ("gamma", "three")]
                        .into_iter()
                        .map(|(key, value)| PutEntry {
                            key: Key(Bytes::from(key)),
                            value: StoredValue {
                                bytes: Bytes::from(value),
                            },
                        })
                        .collect(),
                },
            )
            .expect("seed cached backend");
        write.commit().expect("commit cached seed data");
        backend.flush().expect("flush cached seed data");
    }

    assert_cached_rows(object_store.clone(), db_path, cache_path.clone(), space);
    std::fs::remove_dir_all(&cache_path).expect("delete ephemeral SlateDB cache");
    assert_cached_rows(object_store, db_path, cache_path, space);
}

#[test]
fn cached_slatedb_backend_does_not_acknowledge_failed_remote_writes() {
    let object_store = Arc::new(InMemory::new());
    let db_path = "cached-slatedb-write-failure";
    let cache_parent = tempfile::tempdir().expect("create SlateDB failure cache parent");
    let cache_path = cache_parent.path().join("object-cache");
    let space = SpaceId(12);
    let durable_key = Key(Bytes::from_static(b"durable"));
    let rejected_key = Key(Bytes::from_static(b"rejected"));

    {
        let backend = SlateDbBackend::open_object_store(db_path, object_store.clone())
            .expect("open failure-test seed backend");
        write_one(&backend, space, durable_key.clone(), b"persisted").expect("persist seed value");
        backend.flush().expect("flush seed value");
    }

    let fault_store = Arc::new(FaultStore::new(object_store.clone()));
    {
        let backend = SlateDbBackend::open_object_store_with_options(
            db_path,
            fault_store.clone(),
            SlateDbObjectStoreOptions {
                cache: Some(cache_options(cache_path.clone())),
            },
        )
        .expect("open cached failure-test backend");
        fault_store.fail_writes.store(true, Ordering::Relaxed);

        let error = write_one(&backend, space, rejected_key.clone(), b"not-persisted")
            .expect_err("remote write failure must fail the backend commit");
        assert!(format!("{error}").contains("not supported"));
    }

    std::fs::remove_dir_all(&cache_path).expect("delete failure-test cache");
    let reopened = SlateDbBackend::open_object_store(db_path, object_store)
        .expect("reopen failure-test backend from durable store");
    let read = reopened
        .begin_read(ReadOptions::default())
        .expect("begin failure-test read");
    let result = get_many(
        &read,
        space,
        &[durable_key, rejected_key],
        GetOptions::default(),
    )
    .expect("read durable values after failed write");

    assert_eq!(
        result.values,
        vec![
            Some(ProjectedValue::FullValue(Bytes::from_static(b"persisted"))),
            None,
        ]
    );
}

fn write_one(
    backend: &SlateDbBackend,
    space: SpaceId,
    key: Key,
    value: &'static [u8],
) -> Result<(), BackendError> {
    let mut write = backend.begin_write(WriteOptions::default())?;
    write.put_many(
        space,
        PutBatch {
            entries: vec![PutEntry {
                key,
                value: StoredValue {
                    bytes: Bytes::from_static(value),
                },
            }],
        },
    )?;
    write.commit().map(|_| ())
}

fn assert_cached_rows(
    object_store: Arc<InMemory>,
    db_path: &str,
    cache_path: PathBuf,
    space: SpaceId,
) {
    let backend = SlateDbBackend::open_object_store_with_options(
        db_path,
        object_store,
        SlateDbObjectStoreOptions {
            cache: Some(cache_options(cache_path)),
        },
    )
    .expect("open cached backend");
    let read = backend
        .begin_read(ReadOptions::default())
        .expect("begin cached read");
    let mut rows = Vec::new();
    let result = read
        .scan(
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            ScanOptions {
                projection: CoreProjection::FullValue,
                limit_rows: usize::MAX,
                resume_after: None,
            },
            &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                let ProjectedValueRef::FullValue(value) = value else {
                    panic!("cached scan returned key-only projection");
                };
                rows.push((key.to_owned_key(), Bytes::copy_from_slice(value)));
                Ok(())
            },
        )
        .expect("scan cached rows");

    assert_eq!(result.emitted, 3);
    assert_eq!(
        rows,
        vec![
            (
                Key(Bytes::from_static(b"alpha")),
                Bytes::from_static(b"one")
            ),
            (Key(Bytes::from_static(b"beta")), Bytes::from_static(b"two")),
            (
                Key(Bytes::from_static(b"gamma")),
                Bytes::from_static(b"three")
            ),
        ]
    );
}

fn cache_options(root_folder: PathBuf) -> SlateDbCacheOptions {
    SlateDbCacheOptions {
        root_folder,
        max_disk_cache_bytes: 16 * 1024 * 1024,
        block_cache_bytes: 4 * 1024 * 1024,
        metadata_cache_bytes: 1024 * 1024,
    }
}

struct CachedSlateDbBackendFactory {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

impl CachedSlateDbBackendFactory {
    fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create cached SlateDB factory directory"),
            next_database_id: AtomicU64::new(0),
        }
    }
}

struct CachedSlateDbBackendFixture {
    object_store: Arc<InMemory>,
    db_path: String,
    cache_path: PathBuf,
}

impl BackendFactory for CachedSlateDbBackendFactory {
    type Backend = SlateDbBackend;
    type Fixture = CachedSlateDbBackendFixture;

    fn create_fixture(&self) -> Self::Fixture {
        let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        CachedSlateDbBackendFixture {
            object_store: Arc::new(InMemory::new()),
            db_path: format!("cached-backend-{database_id}"),
            cache_path: self.temp_dir.path().join(format!("cache-{database_id}")),
        }
    }

    fn config(&self) -> BackendTestConfig {
        BackendTestConfig {
            ephemeral: false,
            supports_concurrent_writers: false,
            ..BackendTestConfig::default()
        }
    }
}

impl BackendFixture for CachedSlateDbBackendFixture {
    type Backend = SlateDbBackend;

    fn open(&self) -> Self::Backend {
        SlateDbBackend::open_object_store_with_options(
            self.db_path.clone(),
            self.object_store.clone(),
            SlateDbObjectStoreOptions {
                cache: Some(cache_options(self.cache_path.clone())),
            },
        )
        .expect("open cached SlateDB fixture")
    }
}

#[derive(Clone, Debug)]
struct FaultStore {
    inner: Arc<InMemory>,
    fail_writes: Arc<AtomicBool>,
}

impl FaultStore {
    fn new(inner: Arc<InMemory>) -> Self {
        Self {
            inner,
            fail_writes: Arc::new(AtomicBool::new(false)),
        }
    }

    fn should_fail_writes(&self) -> bool {
        self.fail_writes.load(Ordering::Relaxed)
    }
}

impl std::fmt::Display for FaultStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FaultStore")
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if self.should_fail_writes() {
            return Err(fault_error());
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        if self.should_fail_writes() {
            return Err(fault_error());
        }
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: ObjectStoreGetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        if self.should_fail_writes() {
            return Box::pin(stream::once(async { Err(fault_error()) }));
        }
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        if self.should_fail_writes() {
            return Err(fault_error());
        }
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        if self.should_fail_writes() {
            return Err(fault_error());
        }
        self.inner.rename_opts(from, to, options).await
    }
}

fn fault_error() -> object_store::Error {
    object_store::Error::NotSupported {
        source: Box::new(std::io::Error::other("injected remote write failure")),
    }
}
