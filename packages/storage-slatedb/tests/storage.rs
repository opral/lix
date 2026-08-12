#![allow(
    clippy::manual_async_fn,
    reason = "test fixtures mirror explicit Send future signatures from StorageFixture"
)]

#[path = "../../lix/tests/adapter_deterministic_sequence_corruption.rs"]
mod deterministic_sequence_corruption;
#[path = "../../lix/tests/adapter_undo_redo_checkpoint.rs"]
mod undo_redo_checkpoint;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, BoxStream};
use lix::integration::Engine;
use lix::storage::conformance::{
    StorageFactory, StorageFixture, StorageTestConfig, run_storage_conformance,
};
use lix::storage::{
    BeginScanOptions, CoreProjection, GetManyRequest, GetOptions, Key, KeyRange, ProjectedValue,
    PutBatch, PutEntry, ReadOptions, SpaceId, Storage, StorageError, StorageRead, StorageSpace,
    StorageWrite, StoredValue, WriteOptions,
};
use lix::{LixError, Value};
use lix_storage_slatedb::{
    SlateDB, SlateDBCacheOptions, SlateDBFactory, SlateDBObjectStoreOptions,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions as ObjectStoreGetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult,
};
use std::future::Future;
use std::ops::Bound;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, atomic::AtomicBool};
use tempfile::TempDir;

#[tokio::test]
async fn slatedb_passes_storage_conformance() {
    let factory = SlateDBFactory::new();

    run_storage_conformance(&factory).await.assert_no_failures();
}

#[tokio::test]
async fn cached_slatedb_passes_storage_conformance() {
    let factory = CachedSlateDBFactory::new();

    run_storage_conformance(&factory).await.assert_no_failures();
}

#[tokio::test]
async fn file_sql_bytea_hard_cut_roundtrips_after_slatedb_reopen() {
    let temp_dir = tempfile::tempdir().expect("create SlateDB temp directory");
    let path = temp_dir.path().join("file-sql.slatedb");
    let storage = SlateDB::open(&path).expect("open SlateDB storage");
    Engine::initialize(storage.clone())
        .await
        .expect("initialize SlateDB storage");
    let engine = Engine::new(storage.clone()).await.expect("open engine");
    let session = engine
        .open_session()
        .await
        .expect("open session");

    session
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ($1, CAST($2 AS BYTEA))",
            &[
                Value::Text("/adapter.bin".to_string()),
                Value::Text("aé—".to_string()),
            ],
        )
        .await
        .expect("insert text through an explicit BYTEA cast");
    let lengths = session
        .execute(
            "SELECT length(content) AS characters, OCTET_LENGTH(content) AS octets \
             FROM lix_file WHERE path = $1",
            &[Value::Text("/adapter.bin".to_string())],
        )
        .await
        .expect("read character and byte lengths");
    assert_eq!(
        lengths.rows()[0]
            .get::<i64>("characters")
            .expect("character length should decode"),
        3
    );
    assert_eq!(
        lengths.rows()[0]
            .get::<i64>("octets")
            .expect("byte length should decode"),
        6
    );

    session
        .execute(
            "UPDATE lix_file SET content = $2 WHERE path = $1",
            &[
                Value::Text("/adapter.bin".to_string()),
                Value::Blob(vec![0xff, 0x00, 0x61].into()),
            ],
        )
        .await
        .expect("update with a direct binary parameter");
    let error = session
        .execute("SELECT X'41'", &[])
        .await
        .expect_err("legacy SQL hex literals should be rejected");
    assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);

    drop(session);
    drop(engine);
    storage.flush().await.expect("flush SlateDB storage");
    drop(storage);

    let reopened = SlateDB::open(&path).expect("reopen SlateDB storage");
    let engine = Engine::new(reopened).await.expect("reopen engine");
    let session = engine
        .open_session()
        .await
        .expect("reopen session");
    let result = session
        .execute(
            "SELECT content, OCTET_LENGTH(content) AS octets FROM lix_file WHERE path = $1",
            &[Value::Text("/adapter.bin".to_string())],
        )
        .await
        .expect("read binary content after reopen");
    assert_eq!(
        result.rows()[0]
            .get::<Vec<u8>>("content")
            .expect("content should decode"),
        vec![0xff, 0x00, 0x61]
    );
    assert_eq!(
        result.rows()[0]
            .get::<i64>("octets")
            .expect("byte length should decode"),
        3
    );
}

#[tokio::test]
async fn slatedb_exposes_database_path_and_flushes() {
    let temp_dir = tempfile::tempdir().expect("create slatedb storage temp dir");
    let path = temp_dir.path().join("storage.slatedb");

    let storage = SlateDB::open(&path).expect("open slatedb storage");
    storage.flush().await.expect("flush slatedb storage");

    assert_eq!(storage.path(), path.as_path());
}

#[tokio::test]
async fn checkpointed_state_survives_undo_redo_and_cold_reopen_on_slatedb() {
    let temp_dir = tempfile::tempdir().expect("create SlateDB temp directory");
    let path = temp_dir.path().join("undo-redo.slatedb");
    let storage = SlateDB::open(&path).expect("open SlateDB storage");
    Engine::initialize(storage.clone())
        .await
        .expect("initialize SlateDB storage");
    let engine = Engine::new(storage.clone()).await.expect("open engine");
    let branch_id = undo_redo_checkpoint::stage_checkpointed_a_and_undo_b(&engine).await;
    drop(engine);
    storage.flush().await.expect("flush undo state");
    drop(storage);

    let storage = SlateDB::open(&path).expect("reopen SlateDB after undo");
    let engine = Engine::new(storage.clone())
        .await
        .expect("reopen engine after undo");
    undo_redo_checkpoint::assert_cold_undo_then_redo(&engine, branch_id.clone()).await;
    drop(engine);
    storage.flush().await.expect("flush redo state");
    drop(storage);

    let storage = SlateDB::open(&path).expect("reopen SlateDB after redo");
    let engine = Engine::new(storage)
        .await
        .expect("reopen engine after redo");
    undo_redo_checkpoint::assert_cold_redo(&engine, branch_id).await;
}

#[tokio::test]
async fn deterministic_sequence_member_corruption_fails_closed_on_slatedb() {
    let temp_dir = tempfile::tempdir().expect("create SlateDB temp directory");

    let initial_path = temp_dir.path().join("sequence-initial.slatedb");
    let storage = SlateDB::open(&initial_path).expect("open initial SlateDB storage");
    deterministic_sequence_corruption::initialize_with_deterministic_mode(storage.clone()).await;
    storage
        .flush()
        .await
        .expect("flush initial deterministic mode");
    drop(storage);
    let storage = SlateDB::open(&initial_path).expect("reopen initial SlateDB storage");
    deterministic_sequence_corruption::assert_next_uuid(storage, "000000000000").await;

    let corrupt_path = temp_dir.path().join("sequence-corrupt.slatedb");
    let storage = SlateDB::open(&corrupt_path).expect("open corruption SlateDB storage");
    deterministic_sequence_corruption::initialize_with_deterministic_mode(storage.clone()).await;
    deterministic_sequence_corruption::assert_next_uuid(storage.clone(), "000000000000").await;
    storage
        .flush()
        .await
        .expect("flush published sequence member");
    drop(storage);

    let storage = SlateDB::open(&corrupt_path).expect("reopen published sequence storage");
    deterministic_sequence_corruption::replace_selected_sequence_member_with_unrelated(&storage)
        .await;
    storage
        .flush()
        .await
        .expect("flush same-count sequence member substitution");
    drop(storage);

    let storage = SlateDB::open(&corrupt_path).expect("reopen corrupt sequence storage");
    deterministic_sequence_corruption::assert_missing_sequence_member_fails_closed(storage).await;
}

#[tokio::test]
async fn slatedb_rejects_keys_above_physical_limit() {
    let temp_dir = tempfile::tempdir().expect("create slatedb storage temp dir");
    let path = temp_dir.path().join("storage.slatedb");
    let storage = SlateDB::open(path).expect("open slatedb storage");
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin slatedb write");

    let too_long_logical_key = Key(Bytes::from(vec![0; u16::MAX as usize - 3]));
    let error = write
        .put_many(
            StorageSpace::mutable(SpaceId(1), "test.mutable"),
            PutBatch {
                entries: vec![PutEntry {
                    key: too_long_logical_key,
                    value: StoredValue {
                        bytes: Bytes::new(),
                    },
                }],
            },
        )
        .await
        .expect_err("oversized physical key should fail");

    assert_eq!(error, StorageError::InvalidKey);
}

#[tokio::test]
async fn slatedb_streams_unbounded_scan_limits() {
    let temp_dir = tempfile::tempdir().expect("create slatedb storage temp dir");
    let path = temp_dir.path().join("storage.slatedb");
    let storage = SlateDB::open(path).expect("open slatedb storage");
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin slatedb write");

    write
        .put_many(
            StorageSpace::mutable(SpaceId(1), "test.mutable"),
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
        .await
        .expect("put slatedb rows");
    write.commit().await.expect("commit slatedb rows");

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin slatedb read");
    let mut cursor = read
        .begin_scan(
            StorageSpace::mutable(SpaceId(1), "test.mutable"),
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions {
                projection: CoreProjection::KeyOnly,
                ..BeginScanOptions::default()
            },
        )
        .await
        .expect("begin scan slatedb rows");
    let (result, result_has_more) = cursor
        .next_page(usize::MAX)
        .await
        .expect("scan slatedb rows").into_parts();

    assert_eq!(result.len(), 10);
    assert!(
        result
            .iter()
            .all(|entry| entry.value == ProjectedValue::KeyOnly)
    );
    assert!(!result_has_more);
}

#[tokio::test]
async fn cached_slatedb_rebuilds_after_local_cache_is_deleted() {
    let object_store = Arc::new(InMemory::new());
    let db_path = "cached-slatedb-rebuild";
    let cache_parent = tempfile::tempdir().expect("create SlateDB cache parent");
    let cache_path = cache_parent.path().join("object-cache");
    let space = StorageSpace::mutable(SpaceId(11), "test.mutable");

    {
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            object_store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open uncached seed storage");
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
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
            .await
            .expect("seed cached storage");
        write.commit().await.expect("commit cached seed data");
        storage.flush().await.expect("flush cached seed data");
    }

    assert_cached_rows(object_store.clone(), db_path, cache_path.clone(), space).await;
    std::fs::remove_dir_all(&cache_path).expect("delete ephemeral SlateDB cache");
    assert_cached_rows(object_store, db_path, cache_path, space).await;
}

#[tokio::test]
async fn cached_slatedb_reports_failed_flush_after_accepting_write() {
    let object_store = Arc::new(InMemory::new());
    let db_path = "cached-slatedb-write-failure";
    let cache_parent = tempfile::tempdir().expect("create SlateDB failure cache parent");
    let cache_path = cache_parent.path().join("object-cache");
    let space = StorageSpace::mutable(SpaceId(12), "test.mutable");
    let durable_key = Key(Bytes::from_static(b"durable"));
    let rejected_key = Key(Bytes::from_static(b"rejected"));

    {
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            object_store.clone(),
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open failure-test seed storage");
        write_one(&storage, space, durable_key.clone(), b"persisted")
            .await
            .expect("persist seed value");
        storage.flush().await.expect("flush seed value");
    }

    let fault_store = Arc::new(FaultStore::new(object_store.clone()));
    {
        let storage = SlateDB::open_object_store_with_options(
            db_path,
            fault_store.clone(),
            SlateDBObjectStoreOptions {
                cache: Some(cache_options(cache_path.clone())),
            },
        )
        .expect("open cached failure-test storage");
        fault_store.fail_writes.store(true, Ordering::Relaxed);

        write_one(&storage, space, rejected_key.clone(), b"not-persisted")
            .await
            .expect("commit should accept the visible write");

        let error = storage
            .flush()
            .await
            .expect_err("remote write failure must fail the explicit flush");
        assert!(format!("{error}").contains("not supported"));
    }

    std::fs::remove_dir_all(&cache_path).expect("delete failure-test cache");
    let reopened = SlateDB::open_object_store_with_options(
        db_path,
        object_store,
        SlateDBObjectStoreOptions::default(),
    )
    .expect("reopen failure-test storage from durable store");
    let read = reopened
        .begin_read(ReadOptions::default())
        .await
        .expect("begin failure-test read");
    let result = read
        .get_many(&[GetManyRequest {
            space,
            keys: &[durable_key, rejected_key],
            opts: GetOptions::default(),
        }])
        .await
        .expect("read durable values after failed write");

    assert_eq!(
        result.values,
        vec![
            Some(ProjectedValue::FullValue(Bytes::from_static(b"persisted"))),
            None,
        ]
    );
}

#[tokio::test]
async fn slatedb_explicit_flush_makes_visible_commit_durable() {
    let object_store = Arc::new(InMemory::new());
    let counting_store = Arc::new(FaultStore::new(object_store));
    let storage = SlateDB::open_object_store_with_options(
        "slatedb-explicit-wal-flush",
        counting_store.clone(),
        SlateDBObjectStoreOptions::default(),
    )
    .expect("open explicit WAL flush storage");
    counting_store.reset_write_count();

    write_one(
        &storage,
        StorageSpace::mutable(SpaceId(13), "test.mutable"),
        Key(Bytes::from_static(b"durable")),
        b"value",
    )
    .await
    .expect("publish visible value");

    storage.flush().await.expect("flush visible value");
    assert_eq!(
        counting_store.write_count(),
        1,
        "the visible commit should require one WAL write"
    );
    storage
        .flush()
        .await
        .expect("flush already durable storage");
    assert_eq!(counting_store.write_count(), 1);
}

async fn write_one(
    storage: &SlateDB,
    space: StorageSpace,
    key: Key,
    value: &'static [u8],
) -> Result<(), StorageError> {
    let mut write = storage.begin_write(WriteOptions::default()).await?;
    write
        .put_many(
            space,
            PutBatch {
                entries: vec![PutEntry {
                    key,
                    value: StoredValue {
                        bytes: Bytes::from_static(value),
                    },
                }],
            },
        )
        .await?;
    write.commit().await.map(|_| ())
}

async fn assert_cached_rows(
    object_store: Arc<InMemory>,
    db_path: &str,
    cache_path: PathBuf,
    space: StorageSpace,
) {
    let storage = SlateDB::open_object_store_with_options(
        db_path,
        object_store,
        SlateDBObjectStoreOptions {
            cache: Some(cache_options(cache_path)),
        },
    )
    .expect("open cached storage");
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin cached read");
    let mut cursor = read
        .begin_scan(
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions {
                projection: CoreProjection::FullValue,
                ..BeginScanOptions::default()
            },
        )
        .await
        .expect("begin cached scan");
    let (result, _result_has_more) = cursor
        .next_page(usize::MAX)
        .await
        .expect("scan cached rows").into_parts();

    assert_eq!(result.len(), 3);
    let rows = result
        .into_iter()
        .map(|entry| {
            let ProjectedValue::FullValue(value) = entry.value else {
                panic!("cached scan returned key-only projection");
            };
            (entry.key, value)
        })
        .collect::<Vec<_>>();
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

fn cache_options(root_folder: PathBuf) -> SlateDBCacheOptions {
    SlateDBCacheOptions {
        root_folder,
        max_disk_cache_bytes: 16 * 1024 * 1024,
        block_cache_bytes: 4 * 1024 * 1024,
        metadata_cache_bytes: 1024 * 1024,
    }
}

struct CachedSlateDBFactory {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

impl CachedSlateDBFactory {
    fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create cached SlateDB factory directory"),
            next_database_id: AtomicU64::new(0),
        }
    }
}

struct CachedSlateDBFixture {
    object_store: Arc<InMemory>,
    db_path: String,
    cache_path: PathBuf,
}

impl StorageFactory for CachedSlateDBFactory {
    type Storage = SlateDB;
    type Fixture = CachedSlateDBFixture;

    fn create_fixture(&self) -> Self::Fixture {
        let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        CachedSlateDBFixture {
            object_store: Arc::new(InMemory::new()),
            db_path: format!("cached-storage-{database_id}"),
            cache_path: self.temp_dir.path().join(format!("cache-{database_id}")),
        }
    }

    fn config(&self) -> StorageTestConfig {
        StorageTestConfig {
            ephemeral: false,
            supports_concurrent_writers: false,
            ..StorageTestConfig::default()
        }
    }
}

impl StorageFixture for CachedSlateDBFixture {
    type Storage = SlateDB;

    fn open(&self) -> impl Future<Output = Self::Storage> + Send {
        async move {
            SlateDB::open_object_store_with_options(
                self.db_path.clone(),
                self.object_store.clone(),
                SlateDBObjectStoreOptions {
                    cache: Some(cache_options(self.cache_path.clone())),
                },
            )
            .expect("open cached SlateDB fixture")
        }
    }
}

#[derive(Clone, Debug)]
struct FaultStore {
    inner: Arc<InMemory>,
    fail_writes: Arc<AtomicBool>,
    write_ops: Arc<AtomicU64>,
}

impl FaultStore {
    fn new(inner: Arc<InMemory>) -> Self {
        Self {
            inner,
            fail_writes: Arc::new(AtomicBool::new(false)),
            write_ops: Arc::new(AtomicU64::new(0)),
        }
    }

    fn should_fail_writes(&self) -> bool {
        self.fail_writes.load(Ordering::Relaxed)
    }

    fn reset_write_count(&self) {
        self.write_ops.store(0, Ordering::Relaxed);
    }

    fn write_count(&self) -> u64 {
        self.write_ops.load(Ordering::Relaxed)
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
        self.write_ops.fetch_add(1, Ordering::Relaxed);
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
        self.write_ops.fetch_add(1, Ordering::Relaxed);
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
        self.write_ops.fetch_add(1, Ordering::Relaxed);
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
        self.write_ops.fetch_add(1, Ordering::Relaxed);
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
        self.write_ops.fetch_add(1, Ordering::Relaxed);
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
