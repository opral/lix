use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use futures_util::StreamExt;
use futures_util::stream::{self, BoxStream};
use lix_backends::{SlateDbBackend, SlateDbCacheOptions, SlateDbObjectStoreOptions};
use lix_engine::{Engine, SessionContext, Value};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult,
};
use serde_json::json;
use tempfile::TempDir;

const DELAYS_MS: &[u64] = &[0, 10, 25, 50];
const SEED_FILE_COUNT: usize = 100;
const FILE_SIZE_BYTES: usize = 4096;
const UPLOAD_BATCH_SIZE: usize = 10;
const BENCH_DISK_CACHE_BYTES: usize = 64 * 1024 * 1024;
const BENCH_BLOCK_CACHE_BYTES: u64 = 16 * 1024 * 1024;
const BENCH_METADATA_CACHE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_UPLOAD_REMOTE_WRITE_OPS: u64 = 8;

static NEXT_DB_ID: AtomicU64 = AtomicU64::new(0);

fn slatedb_file_api_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for slatedb_file_api benchmarks");
    let mut group = c.benchmark_group("slatedb_file_api");
    group.sample_size(10);

    for &delay_ms in DELAYS_MS {
        let delay = Duration::from_millis(delay_ms);
        let delay_label = format!("{delay_ms}ms");

        group.bench_with_input(
            BenchmarkId::new("upload_overwrite_file", &delay_label),
            &delay,
            |b, &delay| {
                b.iter_custom(|iterations| {
                    let fixture = runtime.block_on(UploadBenchFixture::seeded(delay));
                    measure_iterations(iterations, || {
                        fixture.object_store.reset_counts();
                        let result = runtime.block_on(fixture.upload_overwrite_file());
                        let counts = fixture.object_store.counts();
                        assert_eq!(counts.reads, 0, "cached upload issued remote reads");
                        assert!(
                            counts.writes <= MAX_UPLOAD_REMOTE_WRITE_OPS,
                            "cached upload issued {} remote writes",
                            counts.writes
                        );
                        black_box(counts);
                        result
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("list_root_directory", &delay_label),
            &delay,
            |b, &delay| {
                b.iter_custom(|iterations| {
                    let fixture = runtime.block_on(ReadBenchFixture::seeded(delay));
                    measure_iterations(iterations, || {
                        fixture.object_store.reset_counts();
                        let result = runtime.block_on(fixture.list_root_directory());
                        let counts = fixture.object_store.counts();
                        assert_eq!(counts.reads, 0, "cached directory list issued remote reads");
                        assert_eq!(counts.writes, 0, "directory list issued remote writes");
                        black_box(counts);
                        result
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("download_file", delay_label),
            &delay,
            |b, &delay| {
                b.iter_custom(|iterations| {
                    let fixture = runtime.block_on(ReadBenchFixture::seeded(delay));
                    measure_iterations(iterations, || {
                        fixture.object_store.reset_counts();
                        let result = runtime.block_on(fixture.download_file());
                        let counts = fixture.object_store.counts();
                        assert_eq!(counts.reads, 0, "cached download issued remote reads");
                        assert_eq!(counts.writes, 0, "download issued remote writes");
                        black_box(counts);
                        result
                    })
                });
            },
        );
    }

    group.finish();
}

fn measure_iterations<T>(iterations: u64, mut operation: impl FnMut() -> T) -> Duration {
    let mut elapsed = Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        let result = operation();
        elapsed += start.elapsed();
        black_box(result);
    }
    elapsed
}

struct SeededStore {
    object_store: Arc<DelayedObjectStore>,
    db_path: String,
    main_branch_id: String,
    file_id: String,
    upload_path: String,
}

impl SeededStore {
    async fn create(delay: Duration) -> Self {
        let db_id = NEXT_DB_ID.fetch_add(1, Ordering::Relaxed);
        let db_path = format!("slatedb-file-api-bench-{}-{db_id}", std::process::id());
        let seed_object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let backend =
            SlateDbBackend::open_object_store(db_path.clone(), Arc::clone(&seed_object_store))
                .expect("open SlateDB seed backend");
        let init_receipt = Engine::initialize(backend.clone())
            .await
            .expect("initialize SlateDB file benchmark engine");
        let engine = Engine::new(backend.clone())
            .await
            .expect("open SlateDB file benchmark engine");
        let session = engine
            .open_session(init_receipt.main_branch_id.clone())
            .await
            .expect("open SlateDB file benchmark session");

        seed_files(&session).await;
        let upload_path = file_path(0);
        let file_id = lookup_file_id(&session, &upload_path).await;
        backend.flush().expect("flush seeded file benchmark store");
        let object_store = Arc::new(DelayedObjectStore::new(seed_object_store, delay));

        Self {
            object_store,
            db_path,
            main_branch_id: init_receipt.main_branch_id,
            file_id,
            upload_path,
        }
    }

    async fn open_session(&self) -> (SessionContext<SlateDbBackend>, TempDir) {
        let cache_dir = tempfile::tempdir().expect("create SlateDB benchmark cache directory");
        let backend = SlateDbBackend::open_object_store_with_options(
            self.db_path.clone(),
            object_store_handle(&self.object_store),
            cached_object_store_options(&cache_dir),
        )
        .expect("reopen delayed SlateDB backend");
        let engine = Engine::new(backend)
            .await
            .expect("reopen SlateDB file benchmark engine");
        let session = engine
            .open_session(self.main_branch_id.clone())
            .await
            .expect("reopen SlateDB file benchmark session");
        (session, cache_dir)
    }
}

struct UploadBenchFixture {
    backend: SlateDbBackend,
    session: SessionContext<SlateDbBackend>,
    object_store: Arc<DelayedObjectStore>,
    _cache_dir: TempDir,
    next_upload_version: AtomicU64,
    upload_path: String,
}

impl UploadBenchFixture {
    async fn seeded(delay: Duration) -> Self {
        let seeded = SeededStore::create(Duration::ZERO).await;
        let cache_dir = tempfile::tempdir().expect("create SlateDB upload cache directory");
        let backend = SlateDbBackend::open_object_store_with_options(
            seeded.db_path.clone(),
            object_store_handle(&seeded.object_store),
            cached_object_store_options(&cache_dir),
        )
        .expect("reopen delayed SlateDB backend for upload benchmark");
        let engine = Engine::new(backend.clone())
            .await
            .expect("open SlateDB upload benchmark engine");
        let session = engine
            .open_session(seeded.main_branch_id)
            .await
            .expect("open SlateDB upload benchmark session");
        seeded.object_store.set_delay(delay);
        seeded.object_store.reset_counts();

        Self {
            backend,
            session,
            object_store: seeded.object_store,
            _cache_dir: cache_dir,
            next_upload_version: AtomicU64::new(0),
            upload_path: seeded.upload_path,
        }
    }

    async fn upload_overwrite_file(&self) -> u64 {
        let version = self.next_upload_version.fetch_add(1, Ordering::Relaxed);
        let result = self
            .session
            .execute(
                "INSERT INTO lix_file (path, data, lixcol_metadata) VALUES ($1, $2, $3) \
                 ON CONFLICT (path) DO UPDATE SET data = excluded.data, \
                 lixcol_metadata = excluded.lixcol_metadata",
                &[
                    Value::Text(self.upload_path.clone()),
                    Value::Blob(upload_file_bytes(version)),
                    upload_file_metadata(version),
                ],
            )
            .await
            .expect("overwrite benchmark file");
        self.backend
            .flush()
            .expect("flush overwritten benchmark file");
        result.rows_affected()
    }
}

struct ReadBenchFixture {
    session: SessionContext<SlateDbBackend>,
    object_store: Arc<DelayedObjectStore>,
    _cache_dir: TempDir,
    file_id: String,
}

impl ReadBenchFixture {
    async fn seeded(delay: Duration) -> Self {
        let seeded = SeededStore::create(Duration::ZERO).await;
        let (session, cache_dir) = seeded.open_session().await;
        seeded.object_store.set_delay(delay);
        seeded.object_store.reset_counts();

        Self {
            session,
            object_store: seeded.object_store,
            _cache_dir: cache_dir,
            file_id: seeded.file_id,
        }
    }

    async fn list_root_directory(&self) -> usize {
        let dirs = self
            .session
            .execute(
                "SELECT id, path, name, lixcol_updated_at \
                 FROM lix_directory WHERE parent_id IS NULL ORDER BY name",
                &[],
            )
            .await
            .expect("list root directories");
        let files = self
            .session
            .execute(
                "SELECT id, path, name, lixcol_metadata, lixcol_updated_at \
                 FROM lix_file WHERE directory_id IS NULL ORDER BY name",
                &[],
            )
            .await
            .expect("list root files");
        dirs.len() + files.len()
    }

    async fn download_file(&self) -> usize {
        let result = self
            .session
            .execute(
                "SELECT data FROM lix_file WHERE id = $1",
                &[Value::Text(self.file_id.clone())],
            )
            .await
            .expect("download benchmark file");
        let value = result
            .rows()
            .first()
            .and_then(|row| row.values().first())
            .expect("download query should return one data value");
        match value {
            Value::Blob(bytes) => bytes.len(),
            other => panic!("download query returned non-blob value: {other:?}"),
        }
    }
}

async fn seed_files(session: &SessionContext<SlateDbBackend>) {
    for chunk_start in (0..SEED_FILE_COUNT).step_by(UPLOAD_BATCH_SIZE) {
        let chunk_end = (chunk_start + UPLOAD_BATCH_SIZE).min(SEED_FILE_COUNT);
        let mut placeholders = Vec::with_capacity(chunk_end - chunk_start);
        let mut params = Vec::with_capacity((chunk_end - chunk_start) * 3);

        for (row_index, file_index) in (chunk_start..chunk_end).enumerate() {
            placeholders.push(format!(
                "(${}, ${}, ${})",
                row_index * 3 + 1,
                row_index * 3 + 2,
                row_index * 3 + 3
            ));
            params.push(Value::Text(file_path(file_index)));
            params.push(Value::Blob(file_bytes(file_index)));
            params.push(file_metadata());
        }

        let sql = format!(
            "INSERT INTO lix_file (path, data, lixcol_metadata) VALUES {} \
             ON CONFLICT (path) DO UPDATE SET data = excluded.data, \
             lixcol_metadata = excluded.lixcol_metadata",
            placeholders.join(", ")
        );
        session
            .execute(&sql, &params)
            .await
            .expect("seed file benchmark fixture");
    }
}

async fn lookup_file_id(session: &SessionContext<SlateDbBackend>, path: &str) -> String {
    let result = session
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .expect("look up benchmark file id");
    let value = result
        .rows()
        .first()
        .and_then(|row| row.values().first())
        .expect("seeded file should be queryable by path");
    match value {
        Value::Text(id) => id.clone(),
        other => panic!("file id query returned non-text value: {other:?}"),
    }
}

fn file_path(index: usize) -> String {
    format!("/bench-file-{index:04}.bin")
}

fn file_bytes(index: usize) -> Vec<u8> {
    vec![u8::try_from(index % 251).expect("byte pattern fits in u8"); FILE_SIZE_BYTES]
}

fn file_metadata() -> Value {
    Value::Json(json!({ "size": FILE_SIZE_BYTES }))
}

fn upload_file_bytes(version: u64) -> Vec<u8> {
    let seed_file_count = u64::try_from(SEED_FILE_COUNT).expect("seed file count fits in u64");
    let byte = u8::try_from((version % 251 + seed_file_count % 251) % 251)
        .expect("upload byte pattern fits in u8");
    vec![byte; FILE_SIZE_BYTES]
}

fn upload_file_metadata(version: u64) -> Value {
    Value::Json(json!({
        "bench_version": version,
        "size": FILE_SIZE_BYTES,
    }))
}

fn object_store_handle(object_store: &Arc<DelayedObjectStore>) -> Arc<dyn ObjectStore> {
    object_store.clone()
}

fn cached_object_store_options(cache_dir: &TempDir) -> SlateDbObjectStoreOptions {
    SlateDbObjectStoreOptions {
        cache: Some(SlateDbCacheOptions {
            root_folder: cache_dir.path().join("object-cache"),
            max_disk_cache_bytes: BENCH_DISK_CACHE_BYTES,
            block_cache_bytes: BENCH_BLOCK_CACHE_BYTES,
            metadata_cache_bytes: BENCH_METADATA_CACHE_BYTES,
        }),
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ObjectStoreOperationCounts {
    reads: u64,
    writes: u64,
}

#[derive(Clone, Debug)]
struct DelayedObjectStore {
    inner: Arc<dyn ObjectStore>,
    delay_nanos: Arc<AtomicU64>,
    read_ops: Arc<AtomicU64>,
    write_ops: Arc<AtomicU64>,
}

impl DelayedObjectStore {
    fn new(inner: Arc<dyn ObjectStore>, delay: Duration) -> Self {
        Self {
            inner,
            delay_nanos: Arc::new(AtomicU64::new(duration_nanos(delay))),
            read_ops: Arc::new(AtomicU64::new(0)),
            write_ops: Arc::new(AtomicU64::new(0)),
        }
    }

    fn set_delay(&self, delay: Duration) {
        self.delay_nanos
            .store(duration_nanos(delay), Ordering::Relaxed);
    }

    fn delay(&self) -> Duration {
        Duration::from_nanos(self.delay_nanos.load(Ordering::Relaxed))
    }

    fn reset_counts(&self) {
        self.read_ops.store(0, Ordering::Relaxed);
        self.write_ops.store(0, Ordering::Relaxed);
    }

    fn counts(&self) -> ObjectStoreOperationCounts {
        ObjectStoreOperationCounts {
            reads: self.read_ops.load(Ordering::Relaxed),
            writes: self.write_ops.load(Ordering::Relaxed),
        }
    }
}

impl Display for DelayedObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DelayedObjectStore(delay={:?}, inner={})",
            self.delay(),
            self.inner.as_ref()
        )
    }
}

#[async_trait]
impl ObjectStore for DelayedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.write_ops.fetch_add(1, Ordering::Relaxed);
        delay_once(self.delay()).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.write_ops.fetch_add(1, Ordering::Relaxed);
        delay_once(self.delay()).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.read_ops.fetch_add(1, Ordering::Relaxed);
        delay_once(self.delay()).await;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        self.read_ops.fetch_add(1, Ordering::Relaxed);
        delay_once(self.delay()).await;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        let inner = Arc::clone(&self.inner);
        let delay_nanos = Arc::clone(&self.delay_nanos);
        self.write_ops.fetch_add(1, Ordering::Relaxed);
        stream::once(async move {
            delay_once(current_delay(&delay_nanos)).await;
            inner.delete_stream(locations)
        })
        .flatten()
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        let delay_nanos = Arc::clone(&self.delay_nanos);
        self.read_ops.fetch_add(1, Ordering::Relaxed);
        stream::once(async move {
            delay_once(current_delay(&delay_nanos)).await;
            inner.list(prefix.as_ref())
        })
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        let offset = offset.clone();
        let delay_nanos = Arc::clone(&self.delay_nanos);
        self.read_ops.fetch_add(1, Ordering::Relaxed);
        stream::once(async move {
            delay_once(current_delay(&delay_nanos)).await;
            inner.list_with_offset(prefix.as_ref(), &offset)
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.read_ops.fetch_add(1, Ordering::Relaxed);
        delay_once(self.delay()).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.write_ops.fetch_add(1, Ordering::Relaxed);
        delay_once(self.delay()).await;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        self.write_ops.fetch_add(1, Ordering::Relaxed);
        delay_once(self.delay()).await;
        self.inner.rename_opts(from, to, options).await
    }
}

fn duration_nanos(delay: Duration) -> u64 {
    u64::try_from(delay.as_nanos()).expect("benchmark delay fits in u64 nanoseconds")
}

fn current_delay(delay_nanos: &AtomicU64) -> Duration {
    Duration::from_nanos(delay_nanos.load(Ordering::Relaxed))
}

async fn delay_once(delay: Duration) {
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
}

criterion_group!(benches, slatedb_file_api_benches);
criterion_main!(benches);
