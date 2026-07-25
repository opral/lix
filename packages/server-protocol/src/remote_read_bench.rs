//! Release-mode direct SlateDB versus remote protocol read benchmark.
//!
//! The benchmark uses immutable physical clones, equivalent pinned sessions,
//! the same object-store implementation and latency, and independent cache
//! roots. Run the full qualification matrix with:
//!
//! ```text
//! cargo test -p lix_server_protocol --release \
//!   slatedb_direct_versus_remote_reads -- --ignored --nocapture
//! ```

use super::{FILE_FOUND_HEADER, LixProtocolServer, SESSION_ID_HEADER, handler};
use async_trait::async_trait;
use axum::{
    body::{Body, Bytes},
    http::{
        Request, StatusCode,
        header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE},
    },
};
use flate2::read::GzDecoder;
use futures_util::{
    StreamExt,
    stream::{self, BoxStream},
};
use http_body_util::{BodyExt, Full};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use lix_sdk::{Blob, Lix, OpenLixOptions, Value, open_lix};
use lix_slatedb_storage::{SlateDB, SlateDBCacheOptions, SlateDBObjectStoreOptions};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult, memory::InMemory, path::Path,
};
use serde_json::{Value as JsonValue, json};
use std::{
    fmt::{self, Display, Formatter},
    io::Read,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tower::ServiceExt;

const WORKSPACE_PATH: &str = "remote-read-benchmark";
const FILE_COUNT: usize = 1_000;
const LIST_COUNT: usize = 100;
const DEFAULT_SAMPLES: usize = 30;
const DEFAULT_WARM_MEMORY_SAMPLES: usize = 500;
const DISK_CACHE_BYTES: usize = 64 * 1024 * 1024;
const BLOCK_CACHE_BYTES: u64 = 16 * 1024 * 1024;
const METADATA_CACHE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
enum CacheState {
    WarmMemory,
    WarmDisk,
    Cold,
}

impl CacheState {
    const ALL: [Self; 3] = [Self::WarmMemory, Self::WarmDisk, Self::Cold];

    const fn label(self) -> &'static str {
        match self {
            Self::WarmMemory => "warm-memory",
            Self::WarmDisk => "warm-disk",
            Self::Cold => "cold",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    PointPath,
    ListPaths100,
    Download4KiB,
    Download100KiB,
    Download1MiB,
}

impl Operation {
    const ALL: [Self; 5] = [
        Self::PointPath,
        Self::ListPaths100,
        Self::Download4KiB,
        Self::Download100KiB,
        Self::Download1MiB,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::PointPath => "point-path",
            Self::ListPaths100 => "list-paths-100",
            Self::Download4KiB => "download-4kib",
            Self::Download100KiB => "download-100kib",
            Self::Download1MiB => "download-1mib",
        }
    }

    const fn path(self) -> &'static str {
        match self {
            Self::PointPath => "/corpus/file-0000.bin",
            Self::ListPaths100 => "",
            Self::Download4KiB => "/payload-4k.bin",
            Self::Download100KiB => "/payload-100k.bin",
            Self::Download1MiB => "/payload-1m.bin",
        }
    }

    const fn payload_bytes(self) -> Option<usize> {
        match self {
            Self::PointPath | Self::ListPaths100 => None,
            Self::Download4KiB => Some(4 * 1024),
            Self::Download100KiB => Some(100 * 1024),
            Self::Download1MiB => Some(1024 * 1024),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Arm {
    Direct,
    Remote,
}

#[derive(Clone, Copy, Debug, Default)]
struct ReadStats {
    get: u64,
    get_ranges: u64,
    list: u64,
    list_with_delimiter: u64,
}

impl ReadStats {
    fn total(self) -> u64 {
        self.get + self.get_ranges + self.list + self.list_with_delimiter
    }
}

#[derive(Clone, Debug)]
struct AccountedObjectStore {
    inner: Arc<dyn ObjectStore>,
    delay_nanos: Arc<AtomicU64>,
    record_reads: Arc<AtomicBool>,
    read_stats: Arc<Mutex<ReadStats>>,
}

impl AccountedObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            delay_nanos: Arc::new(AtomicU64::new(0)),
            record_reads: Arc::new(AtomicBool::new(false)),
            read_stats: Arc::new(Mutex::new(ReadStats::default())),
        }
    }

    fn set_delay(&self, delay: Duration) {
        self.delay_nanos.store(
            u64::try_from(delay.as_nanos()).expect("benchmark latency fits u64"),
            Ordering::Relaxed,
        );
    }

    fn fork_accounting(&self) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::clone(&self.inner),
            delay_nanos: Arc::clone(&self.delay_nanos),
            record_reads: Arc::new(AtomicBool::new(false)),
            read_stats: Arc::new(Mutex::new(ReadStats::default())),
        })
    }

    fn delay(&self) -> Duration {
        Duration::from_nanos(self.delay_nanos.load(Ordering::Relaxed))
    }

    fn reset_reads(&self) {
        *self.read_stats.lock().expect("read accounting mutex") = ReadStats::default();
        self.record_reads.store(true, Ordering::Relaxed);
    }

    fn stop_reads(&self) -> ReadStats {
        self.record_reads.store(false, Ordering::Relaxed);
        *self.read_stats.lock().expect("read accounting mutex")
    }

    async fn settle_reads(&self) {
        let interval = self
            .delay()
            .checked_mul(4)
            .unwrap_or(Duration::from_secs(1))
            .max(Duration::from_millis(50));
        let mut last_total = self
            .read_stats
            .lock()
            .expect("read accounting mutex")
            .total();
        let mut stable_intervals = 0;
        for _ in 0..50 {
            tokio::time::sleep(interval).await;
            let current_total = self
                .read_stats
                .lock()
                .expect("read accounting mutex")
                .total();
            if current_total == last_total {
                stable_intervals += 1;
                if stable_intervals == 3 {
                    return;
                }
            } else {
                last_total = current_total;
                stable_intervals = 0;
            }
        }
        panic!("SlateDB benchmark request count did not settle");
    }

    async fn wait_for_quiescence(&self) {
        let interval = self
            .delay()
            .checked_mul(4)
            .unwrap_or(Duration::from_secs(1))
            .max(Duration::from_millis(100));
        let mut quiet_intervals = 0;
        for _ in 0..50 {
            self.reset_reads();
            tokio::time::sleep(interval).await;
            if self.stop_reads().total() == 0 {
                quiet_intervals += 1;
                if quiet_intervals == 3 {
                    return;
                }
            } else {
                quiet_intervals = 0;
            }
        }
        panic!("SlateDB benchmark runtime did not become object-store quiet");
    }

    fn account(&self, update: impl FnOnce(&mut ReadStats)) {
        if self.record_reads.load(Ordering::Relaxed) {
            update(&mut self.read_stats.lock().expect("read accounting mutex"));
        }
    }
}

impl Display for AccountedObjectStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "AccountedObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for AccountedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        delay(self.delay()).await;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        delay(self.delay()).await;
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.account(|stats| stats.get += 1);
        delay(self.delay()).await;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        self.account(|stats| stats.get_ranges += 1);
        delay(self.delay()).await;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.account(|stats| stats.list += 1);
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        let delay = self.delay();
        stream::once(async move {
            delay_once(delay).await;
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
        self.account(|stats| stats.list += 1);
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        let offset = offset.clone();
        let delay = self.delay();
        stream::once(async move {
            delay_once(delay).await;
            inner.list_with_offset(prefix.as_ref(), &offset)
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.account(|stats| stats.list_with_delimiter += 1);
        delay(self.delay()).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        delay(self.delay()).await;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        delay(self.delay()).await;
        self.inner.rename_opts(from, to, options).await
    }
}

struct Seed {
    store: Arc<AccountedObjectStore>,
    snapshot: Vec<(String, Bytes)>,
    next_clone: AtomicU64,
}

impl Seed {
    async fn create() -> Self {
        let store = Arc::new(AccountedObjectStore::new(Arc::new(InMemory::new())));
        let storage = open_storage(&store, WORKSPACE_PATH, None);
        let lix = open_lix(OpenLixOptions::new(storage.clone()))
            .await
            .expect("open benchmark seed");
        let mut files = (0..FILE_COUNT)
            .map(|index| {
                (
                    format!("/corpus/file-{index:04}.bin"),
                    Blob::from(vec![u8::try_from(index % 251).expect("byte"); 128]),
                )
            })
            .collect::<Vec<_>>();
        files.extend([
            (
                Operation::Download4KiB.path().to_string(),
                vec![0x41; 4 * 1024].into(),
            ),
            (
                Operation::Download100KiB.path().to_string(),
                vec![0x42; 100 * 1024].into(),
            ),
            (
                Operation::Download1MiB.path().to_string(),
                vec![0x43; 1024 * 1024].into(),
            ),
        ]);
        lix.upsert_file_data_batch(files)
            .await
            .expect("seed benchmark files");
        lix.close().await.expect("close benchmark seed Lix");
        storage.flush().await.expect("flush benchmark seed storage");
        drop(storage);
        let snapshot = snapshot_prefix(&store.inner, WORKSPACE_PATH).await;
        Self {
            store,
            snapshot,
            next_clone: AtomicU64::new(0),
        }
    }

    async fn clone_database(&self) -> String {
        let clone_id = self.next_clone.fetch_add(1, Ordering::Relaxed);
        let path = format!("{WORKSPACE_PATH}-clone-{clone_id:08}");
        for (relative, bytes) in &self.snapshot {
            let location = if relative.is_empty() {
                path.clone()
            } else {
                format!("{path}/{relative}")
            };
            self.store
                .inner
                .put(&Path::from(location), bytes.clone().into())
                .await
                .expect("clone benchmark database object");
        }
        path
    }

    async fn delete_database(&self, database_path: &str) {
        let locations = self
            .store
            .inner
            .list(Some(&Path::from(database_path)))
            .map(|result| result.map(|meta| meta.location))
            .boxed();
        self.store
            .inner
            .delete_stream(locations)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<ObjectStoreResult<Vec<_>>>()
            .expect("delete benchmark database clone");
    }
}

struct OpenFixture {
    root: Arc<Lix<SlateDB>>,
    storage: SlateDB,
    _cache: TempDir,
}

impl OpenFixture {
    async fn open(store: &Arc<AccountedObjectStore>, database_path: &str, cache: TempDir) -> Self {
        let storage = open_storage(store, database_path, Some(&cache));
        let root = Arc::new(
            open_lix(OpenLixOptions::new(storage.clone()))
                .await
                .expect("open benchmark Lix"),
        );
        Self {
            root,
            storage,
            _cache: cache,
        }
    }

    async fn close(self) {
        self.root.close().await.expect("close benchmark Lix");
        self.storage.flush().await.expect("flush benchmark storage");
    }
}

#[derive(Clone, Copy)]
struct Sample {
    elapsed: Duration,
    requests: u64,
    in_process: Option<Duration>,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual release-mode direct-versus-remote SlateDB benchmark"]
async fn slatedb_direct_versus_remote_reads() {
    let samples = std::env::var("LIX_REMOTE_READ_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_SAMPLES)
        .max(3);
    let warm_memory_samples = std::env::var("LIX_REMOTE_READ_WARM_MEMORY_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_WARM_MEMORY_SAMPLES)
        .max(3);
    let delays = std::env::var("LIX_REMOTE_READ_LATENCIES_MS")
        .ok()
        .map_or_else(|| vec![0, 10, 25], |value| parse_delays(&value));
    let seed = Seed::create().await;

    for delay_ms in delays {
        seed.store.set_delay(Duration::from_millis(delay_ms));
        for cache_state in CacheState::ALL {
            if !selected("LIX_REMOTE_READ_CACHE_STATES", cache_state.label()) {
                continue;
            }
            for operation in Operation::ALL {
                if !selected("LIX_REMOTE_READ_OPERATIONS", operation.label()) {
                    continue;
                }
                if matches!(cache_state, CacheState::WarmMemory) {
                    let (direct, remote) =
                        measure_warm_memory_distribution(&seed, operation, warm_memory_samples)
                            .await;
                    report_and_assert(delay_ms, cache_state, operation, &direct, &remote);
                    continue;
                }
                let mut direct = Vec::with_capacity(samples);
                let mut remote = Vec::with_capacity(samples);
                for sample_index in 0..samples {
                    let first = if sample_index % 2 == 0 {
                        Arm::Direct
                    } else {
                        Arm::Remote
                    };
                    for arm in [first, opposite(first)] {
                        let database_path = seed.clone_database().await;
                        let sample = measure_sample(
                            &seed.store,
                            &database_path,
                            cache_state,
                            operation,
                            arm,
                        )
                        .await;
                        seed.delete_database(&database_path).await;
                        match arm {
                            Arm::Direct => direct.push(sample),
                            Arm::Remote => remote.push(sample),
                        }
                    }
                }
                report_and_assert(delay_ms, cache_state, operation, &direct, &remote);
            }
        }
    }
}

async fn measure_warm_memory_distribution(
    seed: &Seed,
    operation: Operation,
    samples: usize,
) -> (Vec<Sample>, Vec<Sample>) {
    let direct_database = seed.clone_database().await;
    let remote_database = seed.clone_database().await;
    let direct_store = seed.store.fork_accounting();
    let remote_store = seed.store.fork_accounting();
    let direct_fixture = OpenFixture::open(
        &direct_store,
        &direct_database,
        tempfile::tempdir().expect("create direct warm-memory cache"),
    )
    .await;
    let remote_fixture = OpenFixture::open(
        &remote_store,
        &remote_database,
        tempfile::tempdir().expect("create remote warm-memory cache"),
    )
    .await;
    let direct_session = open_direct_session(&direct_fixture.root).await;
    let remote_session = RemoteFixture::open(&remote_fixture.root).await;
    for _ in 0..3 {
        run_direct(&direct_session, operation).await;
        remote_session.run(operation).await;
    }
    tokio::join!(
        direct_store.wait_for_quiescence(),
        remote_store.wait_for_quiescence()
    );

    let mut direct = Vec::with_capacity(samples);
    let mut remote = Vec::with_capacity(samples);
    for sample_index in 0..samples {
        let first = if sample_index % 2 == 0 {
            Arm::Direct
        } else {
            Arm::Remote
        };
        for arm in [first, opposite(first)] {
            match arm {
                Arm::Direct => {
                    direct_store.reset_reads();
                    let started = Instant::now();
                    run_direct(&direct_session, operation).await;
                    let elapsed = started.elapsed();
                    let requests = direct_store.stop_reads().total();
                    direct.push(Sample {
                        elapsed,
                        requests,
                        in_process: None,
                    });
                }
                Arm::Remote => {
                    remote_store.reset_reads();
                    let started = Instant::now();
                    remote_session.run(operation).await;
                    let elapsed = started.elapsed();
                    let requests = remote_store.stop_reads().total();
                    remote.push(Sample {
                        elapsed,
                        requests,
                        in_process: None,
                    });
                }
            }
        }
    }
    if operation.payload_bytes().is_some() {
        for sample in &mut remote {
            sample.in_process = remote_session.profile_in_process(operation).await;
        }
    }

    remote_session.close().await;
    direct_session
        .close()
        .await
        .expect("close direct warm-memory session");
    direct_fixture.close().await;
    remote_fixture.close().await;
    seed.delete_database(&direct_database).await;
    seed.delete_database(&remote_database).await;
    (direct, remote)
}

async fn measure_sample(
    store: &Arc<AccountedObjectStore>,
    database_path: &str,
    cache_state: CacheState,
    operation: Operation,
    arm: Arm,
) -> Sample {
    let cache = tempfile::tempdir().expect("create benchmark cache");
    if matches!(cache_state, CacheState::WarmDisk) {
        let fixture = OpenFixture::open(store, database_path, cache).await;
        run_direct(&fixture.root, operation).await;
        let OpenFixture {
            root,
            storage,
            _cache: cache,
        } = fixture;
        root.close().await.expect("close disk prewarm Lix");
        storage.flush().await.expect("flush disk prewarm storage");
        drop(storage);
        return measure_cold_runtime(store, database_path, cache, operation, arm).await;
    }
    match cache_state {
        CacheState::WarmMemory => unreachable!("warm memory uses its live-runtime distribution"),
        CacheState::Cold => measure_cold_runtime(store, database_path, cache, operation, arm).await,
        CacheState::WarmDisk => unreachable!("warm disk returns after preloading"),
    }
}

async fn measure_cold_runtime(
    store: &Arc<AccountedObjectStore>,
    database_path: &str,
    cache: TempDir,
    operation: Operation,
    arm: Arm,
) -> Sample {
    store.reset_reads();
    let started = Instant::now();
    let fixture = OpenFixture::open(store, database_path, cache).await;
    match arm {
        Arm::Direct => {
            let direct = open_direct_session(&fixture.root).await;
            run_direct(&direct, operation).await;
            direct
                .close()
                .await
                .expect("close benchmark direct session");
        }
        Arm::Remote => {
            let remote = RemoteFixture::open(&fixture.root).await;
            remote.run(operation).await;
            remote.close().await;
        }
    }
    let elapsed = started.elapsed();
    store.settle_reads().await;
    let requests = store.stop_reads().total();
    fixture.close().await;
    Sample {
        elapsed,
        requests,
        in_process: None,
    }
}

async fn open_direct_session(root: &Lix<SlateDB>) -> Lix<SlateDB> {
    let branch_id = root
        .active_branch_id()
        .await
        .expect("load benchmark active branch");
    root.open_session(branch_id)
        .await
        .expect("open benchmark direct pinned session")
}

async fn run_direct(lix: &Lix<SlateDB>, operation: Operation) {
    match operation {
        Operation::PointPath => {
            let result = lix
                .execute(
                    "SELECT path FROM lix_file WHERE path = $1",
                    &[Value::Text(operation.path().to_string())],
                )
                .await
                .expect("direct point read");
            assert_eq!(result.rows().len(), 1);
        }
        Operation::ListPaths100 => {
            let result = lix
                .execute("SELECT path FROM lix_file ORDER BY path LIMIT 100", &[])
                .await
                .expect("direct path listing");
            assert_eq!(result.rows().len(), LIST_COUNT);
        }
        _ => {
            let data = lix
                .read_file_data(operation.path())
                .await
                .expect("direct file read")
                .expect("seeded direct file");
            assert_eq!(
                data.len(),
                operation.payload_bytes().expect("payload operation")
            );
        }
    }
}

struct RemoteFixture {
    server: LixProtocolServer<SlateDB>,
    router: axum::Router,
    client: Client<HttpConnector, Full<Bytes>>,
    base_url: String,
    session_id: String,
    shutdown: oneshot::Sender<()>,
    serving: JoinHandle<()>,
}

impl RemoteFixture {
    async fn open(root: &Arc<Lix<SlateDB>>) -> Self {
        let server = LixProtocolServer::new(Arc::clone(root));
        let router = handler(server.clone());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind benchmark protocol server");
        let address = listener
            .local_addr()
            .expect("benchmark protocol server address");
        let (shutdown, shutdown_rx) = oneshot::channel();
        let serving_router = router.clone();
        let serving = tokio::spawn(async move {
            axum::serve(listener, serving_router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve benchmark protocol");
        });
        let connector = HttpConnector::new();
        let client = Client::builder(TokioExecutor::new()).build(connector);
        let base_url = format!("http://{address}");
        let handshake = client
            .request(
                Request::builder()
                    .uri(format!("{base_url}/lix/v1"))
                    .body(Full::new(Bytes::new()))
                    .expect("handshake request"),
            )
            .await
            .expect("handshake response");
        assert_eq!(handshake.status(), StatusCode::OK);
        let handshake_json = response_json(handshake).await;
        let session_id = handshake_json["sessionId"]
            .as_str()
            .expect("handshake session id")
            .to_string();
        Self {
            server,
            router,
            client,
            base_url,
            session_id,
            shutdown,
            serving,
        }
    }

    async fn run(&self, operation: Operation) {
        let response = match operation {
            Operation::PointPath => {
                execute_request(
                    &self.client,
                    &self.base_url,
                    &self.session_id,
                    "SELECT path FROM lix_file WHERE path = $1",
                    json!([{"kind": "text", "value": operation.path()}]),
                )
                .await
            }
            Operation::ListPaths100 => {
                execute_request(
                    &self.client,
                    &self.base_url,
                    &self.session_id,
                    "SELECT path FROM lix_file ORDER BY path LIMIT 100",
                    json!([]),
                )
                .await
            }
            _ => self
                .client
                .request(
                    Request::builder()
                        .uri(format!(
                            "{}/lix/v1/file?path={}",
                            self.base_url,
                            operation.path()
                        ))
                        .header(SESSION_ID_HEADER, &self.session_id)
                        .header(ACCEPT_ENCODING, "zstd")
                        .body(Full::new(Bytes::new()))
                        .expect("file request"),
                )
                .await
                .expect("file response"),
        };
        assert_eq!(response.status(), StatusCode::OK);
        let found = response.headers().get(FILE_FOUND_HEADER).cloned();
        let compressed = response.headers().contains_key(CONTENT_ENCODING);
        let content_length = response
            .headers()
            .get(axum::http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok());
        let body = decoded_response_bytes(response).await;
        match operation.payload_bytes() {
            Some(expected) => {
                if !compressed {
                    assert_eq!(content_length, Some(expected));
                }
                assert_eq!(
                    found.as_ref().and_then(|value| value.to_str().ok()),
                    Some("true")
                );
                assert_eq!(body.len(), expected);
            }
            None => {
                let response: JsonValue =
                    serde_json::from_slice(&body).expect("remote execute response JSON");
                assert_eq!(
                    response["rows"].as_array().expect("remote rows").len(),
                    if matches!(operation, Operation::ListPaths100) {
                        LIST_COUNT
                    } else {
                        1
                    }
                );
            }
        }
    }

    async fn profile_in_process(&self, operation: Operation) -> Option<Duration> {
        let expected = operation.payload_bytes()?;
        let started = Instant::now();
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/lix/v1/file?path={}", operation.path()))
                    .header(SESSION_ID_HEADER, &self.session_id)
                    .body(Body::empty())
                    .expect("profile file request"),
            )
            .await
            .expect("profile file response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("profile response body")
            .to_bytes();
        let elapsed = started.elapsed();
        assert_eq!(body.len(), expected);
        Some(elapsed)
    }

    async fn close(self) {
        let _ = self.shutdown.send(());
        self.serving.await.expect("join benchmark protocol server");
        self.server.close().await.expect("close benchmark server");
    }
}

async fn execute_request(
    client: &Client<HttpConnector, Full<Bytes>>,
    base_url: &str,
    session_id: &str,
    sql: &str,
    params: JsonValue,
) -> hyper::Response<hyper::body::Incoming> {
    client
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("{base_url}/lix/v1/execute"))
                .header(SESSION_ID_HEADER, session_id)
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT_ENCODING, "zstd")
                .body(Full::new(Bytes::from(
                    json!({ "sql": sql, "params": params }).to_string(),
                )))
                .expect("execute request"),
        )
        .await
        .expect("execute response")
}

async fn response_json(response: hyper::Response<hyper::body::Incoming>) -> JsonValue {
    let body = decoded_response_bytes(response).await;
    serde_json::from_slice(&body).expect("response JSON")
}

async fn decoded_response_bytes(response: hyper::Response<hyper::body::Incoming>) -> Bytes {
    let encoding = response
        .headers()
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    match encoding.as_deref() {
        None => body,
        Some("gzip") => {
            let mut decoder = GzDecoder::new(body.as_ref());
            let mut decoded = Vec::new();
            decoder
                .read_to_end(&mut decoded)
                .expect("decode gzip response");
            Bytes::from(decoded)
        }
        Some("zstd") => {
            Bytes::from(zstd::stream::decode_all(body.as_ref()).expect("decode zstd response"))
        }
        Some(encoding) => panic!("unexpected response content encoding {encoding}"),
    }
}

fn report_and_assert(
    delay_ms: u64,
    cache_state: CacheState,
    operation: Operation,
    direct: &[Sample],
    remote: &[Sample],
) {
    let mut direct_elapsed = direct
        .iter()
        .map(|sample| sample.elapsed)
        .collect::<Vec<_>>();
    let mut remote_elapsed = remote
        .iter()
        .map(|sample| sample.elapsed)
        .collect::<Vec<_>>();
    direct_elapsed.sort_unstable();
    remote_elapsed.sort_unstable();
    let direct_p50 = direct_elapsed[direct_elapsed.len() / 2];
    let direct_p95 = direct_elapsed[direct_elapsed.len() * 95 / 100];
    let remote_p50 = remote_elapsed[remote_elapsed.len() / 2];
    let remote_p95 = remote_elapsed[remote_elapsed.len() * 95 / 100];
    let ratio_p50 = remote_p50.as_secs_f64() / direct_p50.as_secs_f64();
    let ratio_p95 = remote_p95.as_secs_f64() / direct_p95.as_secs_f64();
    let direct_max_requests = direct
        .iter()
        .map(|sample| sample.requests)
        .max()
        .unwrap_or(0);
    let remote_max_requests = remote
        .iter()
        .map(|sample| sample.requests)
        .max()
        .unwrap_or(0);
    let remote_max_extra_requests = direct
        .iter()
        .zip(remote.iter())
        .map(|(direct, remote)| remote.requests.saturating_sub(direct.requests))
        .max()
        .unwrap_or(0);
    let direct_requests_total = direct.iter().map(|sample| sample.requests).sum::<u64>();
    let remote_requests_total = remote.iter().map(|sample| sample.requests).sum::<u64>();
    let remote_extra_requests_total = remote_requests_total.saturating_sub(direct_requests_total);
    let mut in_process = remote
        .iter()
        .filter_map(|sample| sample.in_process)
        .collect::<Vec<_>>();
    in_process.sort_unstable();
    let in_process_p50_us = in_process
        .get(in_process.len() / 2)
        .map_or(0, Duration::as_micros);
    eprintln!(
        "slatedb_remote_read delay_ms={delay_ms} cache={} operation={} direct_p50_us={} direct_p95_us={} in_process_p50_us={in_process_p50_us} remote_p50_us={} remote_p95_us={} ratio_p50={ratio_p50:.3} ratio_p95={ratio_p95:.3} direct_requests_total={direct_requests_total} remote_requests_total={remote_requests_total} remote_extra_requests_total={remote_extra_requests_total} direct_requests_max={direct_max_requests} remote_requests_max={remote_max_requests} remote_extra_requests_max={remote_max_extra_requests}",
        cache_state.label(),
        operation.label(),
        direct_p50.as_micros(),
        direct_p95.as_micros(),
        remote_p50.as_micros(),
        remote_p95.as_micros(),
    );
    assert!(
        ratio_p50 <= 2.0,
        "{} {}ms {} p50 ratio {ratio_p50:.3} exceeds 2x",
        cache_state.label(),
        delay_ms,
        operation.label(),
    );
    assert!(
        ratio_p95 <= 2.0,
        "{} {}ms {} p95 ratio {ratio_p95:.3} exceeds 2x",
        cache_state.label(),
        delay_ms,
        operation.label(),
    );
    assert!(
        remote_extra_requests_total <= u64::try_from(remote.len()).expect("sample count fits u64"),
        "{} {}ms {} remote made {remote_extra_requests_total} additional requests across {} samples",
        cache_state.label(),
        delay_ms,
        operation.label(),
        remote.len(),
    );
}

fn open_storage(
    store: &Arc<AccountedObjectStore>,
    database_path: &str,
    cache: Option<&TempDir>,
) -> SlateDB {
    let object_store: Arc<dyn ObjectStore> = store.clone();
    SlateDB::open_object_store_with_options(
        database_path,
        object_store,
        SlateDBObjectStoreOptions {
            cache: cache.map(|cache| SlateDBCacheOptions {
                root_folder: cache.path().join("object-cache"),
                max_disk_cache_bytes: DISK_CACHE_BYTES,
                block_cache_bytes: BLOCK_CACHE_BYTES,
                metadata_cache_bytes: METADATA_CACHE_BYTES,
            }),
        },
    )
    .expect("open benchmark SlateDB")
}

async fn snapshot_prefix(
    object_store: &Arc<dyn ObjectStore>,
    database_path: &str,
) -> Vec<(String, Bytes)> {
    let prefix = Path::from(database_path);
    let mut objects = object_store.list(Some(&prefix));
    let mut snapshot = Vec::new();
    while let Some(meta) = objects.next().await {
        let meta = meta.expect("list benchmark seed object");
        let bytes = object_store
            .get(&meta.location)
            .await
            .expect("get benchmark seed object")
            .bytes()
            .await
            .expect("read benchmark seed object");
        let location = meta.location.to_string();
        let relative = location
            .strip_prefix(database_path)
            .expect("seed object has database prefix")
            .trim_start_matches('/')
            .to_string();
        snapshot.push((relative, bytes));
    }
    assert!(!snapshot.is_empty(), "seed snapshot must contain objects");
    snapshot
}

fn opposite(arm: Arm) -> Arm {
    match arm {
        Arm::Direct => Arm::Remote,
        Arm::Remote => Arm::Direct,
    }
}

fn parse_delays(value: &str) -> Vec<u64> {
    let parsed = value
        .split(',')
        .map(|part| part.trim().parse::<u64>().expect("latency milliseconds"))
        .collect::<Vec<_>>();
    assert!(!parsed.is_empty(), "latency matrix must not be empty");
    parsed
}

fn selected(variable: &str, label: &str) -> bool {
    std::env::var(variable).map_or(true, |value| {
        value
            .split(',')
            .map(str::trim)
            .any(|candidate| candidate == label)
    })
}

async fn delay(duration: Duration) {
    delay_once(duration).await;
}

async fn delay_once(duration: Duration) {
    if !duration.is_zero() {
        tokio::time::sleep(duration).await;
    }
}
