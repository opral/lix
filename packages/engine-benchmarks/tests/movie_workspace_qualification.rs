use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::future::join_all;
use futures_util::stream::{self, BoxStream};
use lix::FILE_UPLOAD_PART_BYTES;
use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::storage_bench::{MediaStructuralAccounting, take_media_structural_accounting};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{
    SlateDB, SlateDBCacheOptions, SlateDBIoCounters, SlateDBIoSnapshot, SlateDBObjectStoreOptions,
};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult,
};

const PROXY_BYTES: usize = 128 * 1024 * 1024;
const INGEST_BYTES: usize = 512 * 1024 * 1024;
const STREAM_READ_BYTES: u64 = 1024 * 1024;
const STREAM_PERIOD: Duration = Duration::from_millis(80);
const PLAYBACK_READ_AHEAD: Duration = Duration::from_secs(1);
const STREAM_SAMPLES: usize = 40;
const SAVE_SAMPLES: usize = 40;
const HISTORY_COMMITS: usize = 5_000;
const TIB: u64 = 1024 * 1024 * 1024 * 1024;
const GIB: u64 = 1024 * 1024 * 1024;
const CORPUS_MODEL: &[(u64, u64)] = &[(20 * GIB, 20), (10 * GIB, 32), (4 * GIB, 64), (GIB, 48)];
static QUALIFICATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn rocksdb_resumes_media_upload_after_engine_restart() {
    let temp = tempfile::tempdir().expect("create RocksDB restart fixture");
    let path = temp.path().join("database");
    {
        let storage = RocksDB::open(&path).expect("open first RocksDB engine");
        stage_restart_part(storage).await;
    }
    let storage = RocksDB::open(&path).expect("reopen RocksDB engine");
    finish_restart_upload(storage).await;
}

#[tokio::test]
async fn slatedb_resumes_media_upload_after_engine_restart() {
    let temp = tempfile::tempdir().expect("create SlateDB restart fixture");
    let path = temp.path().join("database");
    {
        let storage = SlateDB::open(&path).expect("open first SlateDB engine");
        stage_restart_part(storage).await;
    }
    let storage = SlateDB::open(&path).expect("reopen SlateDB engine");
    finish_restart_upload(storage).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "release qualification benchmark"]
async fn rocksdb_movie_workspace_interference() {
    let _qualification_guard = QUALIFICATION_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("create RocksDB movie fixture");
    let storage = RocksDB::open(temp.path().join("database")).expect("open RocksDB fixture");
    qualify("rocksdb", storage).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "release qualification benchmark"]
async fn slatedb_movie_workspace_interference() {
    let _qualification_guard = QUALIFICATION_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("create SlateDB movie fixture");
    let storage = SlateDB::open(temp.path().join("database")).expect("open SlateDB fixture");
    qualify("slatedb", storage).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "release latency qualification benchmark"]
async fn slatedb_movie_workspace_latency_simulation() {
    let _qualification_guard = QUALIFICATION_LOCK.lock().await;
    let mut failures = Vec::new();
    for latency_ms in [10_u64, 30, 50] {
        let mut serial_runs = Vec::new();
        let mut parallel_runs = Vec::new();
        for repetition in 0..4 {
            if repetition % 2 == 0 {
                serial_runs.push(qualify_delayed_slatedb(latency_ms, 1).await);
                parallel_runs.push(qualify_delayed_slatedb(latency_ms, 4).await);
            } else {
                parallel_runs.push(qualify_delayed_slatedb(latency_ms, 4).await);
                serial_runs.push(qualify_delayed_slatedb(latency_ms, 1).await);
            }
        }
        let serial_median = median_ingest_rate(&serial_runs);
        let parallel_median = median_ingest_rate(&parallel_runs);
        let parallel = parallel_runs
            .iter()
            .min_by(|left, right| {
                (left.ingest_mib_s - parallel_median)
                    .abs()
                    .total_cmp(&(right.ingest_mib_s - parallel_median).abs())
            })
            .expect("four-part qualification has repetitions");
        let max_save_p95 = parallel_runs
            .iter()
            .map(|run| run.save_p95)
            .max()
            .expect("four-part qualification has repetitions");
        let late_reads = parallel_runs
            .iter()
            .map(|run| run.late_reads)
            .sum::<usize>();
        let improvement = parallel_median / serial_median - 1.0;
        let gating_profile = latency_ms >= 30;
        println!(
            "movie_workspace_latency,latency_ms={latency_ms},gating={gating_profile},serial_median_mib_s={:.1},four_part_median_mib_s={:.1},improvement_pct={:.1},four_part_save_p95_ms={:.3},four_part_late_reads={},temporary_leaf_rows={},locator_rows={},object_store_logical_calls={}",
            serial_median,
            parallel_median,
            improvement * 100.0,
            max_save_p95.as_secs_f64() * 1000.0,
            late_reads,
            parallel.structural.temporary_manifest_leaf_rows,
            parallel.io.immutable_locator_rows,
            parallel.io.read_objects + parallel.io.write_objects,
        );
        if gating_profile && improvement <= 0.20 {
            failures.push(format!(
                "{latency_ms} ms ingest improvement was only {:.1}%",
                improvement * 100.0
            ));
        }
        if max_save_p95 >= Duration::from_millis(500) {
            failures.push(format!(
                "{latency_ms} ms save p95 was {:.3} ms",
                max_save_p95.as_secs_f64() * 1000.0
            ));
        }
        if late_reads != 0 {
            failures.push(format!(
                "{latency_ms} ms playback had {} late reads",
                late_reads
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("; "));
}

fn median_ingest_rate(runs: &[Qualification]) -> f64 {
    let mut rates = runs.iter().map(|run| run.ingest_mib_s).collect::<Vec<_>>();
    rates.sort_by(f64::total_cmp);
    (rates[rates.len() / 2 - 1] + rates[rates.len() / 2]) / 2.0
}

async fn qualify_delayed_slatedb(latency_ms: u64, upload_concurrency: usize) -> Qualification {
    let temp = tempfile::tempdir().expect("create delayed SlateDB fixture");
    std::fs::create_dir_all(temp.path().join("objects"))
        .expect("create delayed object-store directory");
    let inner: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(temp.path().join("objects"))
            .expect("create local object store"),
    );
    let delayed = LatencyObjectStore::new(inner);
    let counters = SlateDBIoCounters::default();
    let storage = SlateDB::open_object_store_with_options_and_io_counters(
        format!("latency-{latency_ms}-{upload_concurrency}"),
        Arc::new(delayed.clone()),
        SlateDBObjectStoreOptions {
            cache: Some(SlateDBCacheOptions {
                root_folder: temp.path().join("cache"),
                max_disk_cache_bytes: 256 * 1024 * 1024,
                block_cache_bytes: 16 * 1024 * 1024,
                metadata_cache_bytes: 64 * 1024 * 1024,
            }),
        },
        counters.clone(),
    )
    .expect("open delayed SlateDB fixture");
    qualify_case(
        "slatedb-latency",
        storage,
        upload_concurrency,
        Some((delayed, Duration::from_millis(latency_ms))),
        Some(counters),
    )
    .await
}

async fn qualify<S>(backend: &str, storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let part_concurrency = std::env::var("LIX_MOVIE_UPLOAD_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=4).contains(value))
        .unwrap_or(4);
    qualify_case(backend, storage, part_concurrency, None, None).await;
}

#[derive(Clone, Copy, Debug)]
struct Qualification {
    ingest_mib_s: f64,
    save_p95: Duration,
    late_reads: usize,
    structural: MediaStructuralAccounting,
    io: SlateDBIoSnapshot,
}

async fn qualify_case<S>(
    backend: &str,
    storage: S,
    part_concurrency: usize,
    artificial_latency: Option<(LatencyObjectStore, Duration)>,
    io_counters: Option<SlateDBIoCounters>,
) -> Qualification
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let projected_bytes = CORPUS_MODEL
        .iter()
        .map(|(file_bytes, count)| file_bytes * count)
        .sum::<u64>();
    assert_eq!(
        projected_bytes, TIB,
        "corpus projection must be exactly 1 TiB"
    );
    let receipt = Engine::initialize(storage.clone())
        .await
        .expect("initialize movie workspace");
    let engine = Engine::new(storage).await.expect("open movie workspace");
    let seed = engine
        .open_session(receipt.main_branch_id.clone())
        .await
        .expect("open seed session");
    for revision in 0..HISTORY_COMMITS {
        seed.upsert_file_content(
            "/project/edit.json".to_owned(),
            format!("{{\"timelineRevision\":{revision}}}")
                .into_bytes()
                .into(),
        )
        .await
        .expect("seed project history");
    }
    upload(
        &seed,
        "proxy-seed",
        "/media/proxy.mov",
        PROXY_BYTES,
        1,
        part_concurrency,
    )
    .await;
    if let Some((store, latency)) = &artificial_latency {
        store.set_latency(*latency);
    }
    let _ = take_media_structural_accounting();
    let io_before = io_counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);

    let ingest = Arc::new(
        engine
            .open_session(receipt.main_branch_id.clone())
            .await
            .expect("open ingest session"),
    );
    let saver = Arc::new(
        engine
            .open_session(receipt.main_branch_id.clone())
            .await
            .expect("open project-save session"),
    );
    let first_reader = Arc::new(
        engine
            .open_session(receipt.main_branch_id.clone())
            .await
            .expect("open first playback session"),
    );
    let second_reader = Arc::new(
        engine
            .open_session(receipt.main_branch_id)
            .await
            .expect("open second playback session"),
    );

    let started = tokio::time::Instant::now();
    let ingest_started = Instant::now();
    let ingest_future = async {
        upload(
            &ingest,
            "concurrent-ingest",
            "/media/import.mov",
            INGEST_BYTES,
            2,
            part_concurrency,
        )
        .await;
        ingest_started.elapsed()
    };
    let save_future = project_saves(saver, started);
    let first_playback = playback(first_reader, started, 0);
    let second_playback = playback(second_reader, started, PROXY_BYTES as u64 / 2);
    let (ingest_elapsed, mut save_latencies, first_late, second_late) =
        tokio::join!(ingest_future, save_future, first_playback, second_playback);

    save_latencies.sort_unstable();
    let save_p95 = save_latencies[save_latencies.len() * 95 / 100];
    let ingest_mib_s = INGEST_BYTES as f64 / (1024.0 * 1024.0) / ingest_elapsed.as_secs_f64();
    let structural = take_media_structural_accounting();
    let io = io_counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot)
        .saturating_sub(io_before);
    let row_reduction = structural.legacy_equivalent_chunk_rows as f64
        / structural.temporary_manifest_leaf_rows as f64;
    let projected_leaf_rows = TIB / FILE_UPLOAD_PART_BYTES as u64;
    let projected_chunk_rows = TIB / STREAM_READ_BYTES;
    println!(
        "movie_workspace,backend={backend},row_count_projection_tib=1,representative_files=164,file_size_gib=1-20,projected_leaf_rows={projected_leaf_rows},projected_legacy_chunk_rows={projected_chunk_rows},timed_file_mib=512,save_p95_ms={:.3},stream_1_late={},stream_2_late={},ingest_mib_s={ingest_mib_s:.1},temporary_leaf_rows={},legacy_chunk_rows={},row_reduction_x={row_reduction:.1},locator_staging_attempts={},object_store_logical_reads={},object_store_logical_writes={},chunk_hash_bytes_including_retries={},segment_identity_metadata_hash_bytes={},cache_fs_read_attempts={},cache_fs_write_attempts={},cache_fs_remove_attempts={},writer_gate_wait_ms={:.3}",
        save_p95.as_secs_f64() * 1000.0,
        first_late,
        second_late,
        structural.temporary_manifest_leaf_rows,
        structural.legacy_equivalent_chunk_rows,
        io.immutable_locator_rows,
        io.read_objects,
        io.write_objects,
        structural.chunk_payload_hash_bytes,
        structural.segment_identity_hash_bytes,
        io.cache_filesystem_reads,
        io.cache_filesystem_writes,
        io.cache_filesystem_removes,
        io.writer_gate_wait_nanos as f64 / 1_000_000.0,
    );
    assert!(save_p95 < Duration::from_millis(500));
    assert_eq!(first_late, 0, "first 100 Mbit/s stream fell behind");
    assert_eq!(second_late, 0, "second 100 Mbit/s stream fell behind");
    assert_eq!(structural.temporary_manifest_leaf_rows, 32);
    assert_eq!(structural.legacy_equivalent_chunk_rows, 512);
    assert_eq!(row_reduction, 16.0);
    assert_eq!(projected_chunk_rows / projected_leaf_rows, 16);
    assert!(structural.chunk_payload_hash_bytes >= INGEST_BYTES as u64);
    Qualification {
        ingest_mib_s,
        save_p95,
        late_reads: first_late + second_late,
        structural,
        io,
    }
}

async fn stage_restart_part<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let receipt = Engine::initialize(storage.clone())
        .await
        .expect("initialize restart fixture");
    let engine = Engine::new(storage).await.expect("open first engine");
    let session = engine
        .open_session(receipt.main_branch_id)
        .await
        .expect("open first restart session");
    let total = FILE_UPLOAD_PART_BYTES as u64 + 9;
    let progress = session
        .upsert_file_content_part(
            "restart-upload".to_owned(),
            "/media/restart.mov".to_owned(),
            0,
            total,
            vec![0x51; FILE_UPLOAD_PART_BYTES].into(),
        )
        .await
        .expect("stage pre-restart part");
    assert!(!progress.finalized);
}

async fn finish_restart_upload<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let engine = Engine::new(storage).await.expect("reopen engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open resumed session");
    let total = FILE_UPLOAD_PART_BYTES as u64 + 9;
    let progress = session
        .upsert_file_content_part(
            "restart-upload".to_owned(),
            "/media/restart.mov".to_owned(),
            FILE_UPLOAD_PART_BYTES as u64,
            total,
            vec![0x61; 9].into(),
        )
        .await
        .expect("finish post-restart part");
    assert!(progress.finalized);
    let tail = session
        .read_file_content(
            "/media/restart.mov".to_owned(),
            Some(FILE_UPLOAD_PART_BYTES as u64..total),
        )
        .await
        .expect("read restarted upload")
        .expect("restarted file exists");
    assert_eq!(tail.content().as_ref(), &[0x61; 9]);
}

async fn upload<S>(
    session: &SessionContext<S>,
    upload_id: &str,
    path: &str,
    total_bytes: usize,
    seed: u64,
    part_concurrency: usize,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let window_bytes = FILE_UPLOAD_PART_BYTES * part_concurrency;
    let mut window_start = 0usize;
    while window_start < total_bytes {
        let window_end = (window_start + window_bytes).min(total_bytes);
        let parts = (window_start..window_end)
            .step_by(FILE_UPLOAD_PART_BYTES)
            .map(|offset| {
                let len = (total_bytes - offset).min(FILE_UPLOAD_PART_BYTES);
                let data = deterministic_bytes(len, seed ^ offset as u64);
                session.upsert_file_content_part(
                    upload_id.to_owned(),
                    path.to_owned(),
                    offset as u64,
                    total_bytes as u64,
                    data.into(),
                )
            });
        let progresses = join_all(parts).await;
        let acknowledged = progresses
            .into_iter()
            .map(|progress| progress.expect("upload movie part").next_offset)
            .max()
            .expect("upload window contains a part");
        assert_eq!(acknowledged, window_end as u64);
        window_start = window_end;
    }
}

async fn playback<S>(
    session: Arc<SessionContext<S>>,
    schedule_start: tokio::time::Instant,
    initial_offset: u64,
) -> usize
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut late = 0;
    for sample in 0..STREAM_SAMPLES {
        let deadline = schedule_start + PLAYBACK_READ_AHEAD + STREAM_PERIOD * (sample as u32 + 1);
        let offset = (initial_offset + sample as u64 * STREAM_READ_BYTES)
            % (PROXY_BYTES as u64 - STREAM_READ_BYTES);
        let read = session
            .read_file_content(
                "/media/proxy.mov".to_owned(),
                Some(offset..offset + STREAM_READ_BYTES),
            )
            .await
            .expect("read proxy range")
            .expect("proxy exists");
        assert_eq!(read.content().len(), STREAM_READ_BYTES as usize);
        if tokio::time::Instant::now() > deadline {
            late += 1;
        } else {
            tokio::time::sleep_until(deadline).await;
        }
    }
    late
}

async fn project_saves<S>(
    session: Arc<SessionContext<S>>,
    schedule_start: tokio::time::Instant,
) -> Vec<Duration>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut timings = Vec::with_capacity(SAVE_SAMPLES);
    for sample in 0..SAVE_SAMPLES {
        tokio::time::sleep_until(schedule_start + STREAM_PERIOD * sample as u32).await;
        let payload = format!(
            "{{\"timelineRevision\":{sample},\"playhead\":{}}}",
            sample * 24
        );
        let started = Instant::now();
        session
            .upsert_file_content("/project/edit.json".to_owned(), payload.into_bytes().into())
            .await
            .expect("save project file");
        timings.push(started.elapsed());
    }
    timings
}

fn deterministic_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut bytes = vec![0; len];
    let mut state = seed ^ 0xd1b5_4a32_d192_ed03;
    for chunk in bytes.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let generated = state.to_le_bytes();
        chunk.copy_from_slice(&generated[..chunk.len()]);
    }
    bytes
}

#[derive(Clone, Debug)]
struct LatencyObjectStore {
    inner: Arc<dyn ObjectStore>,
    latency_nanos: Arc<AtomicU64>,
}

impl LatencyObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            latency_nanos: Arc::new(AtomicU64::new(0)),
        }
    }

    fn set_latency(&self, latency: Duration) {
        self.latency_nanos.store(
            u64::try_from(latency.as_nanos()).expect("simulation latency fits u64 nanoseconds"),
            Ordering::Relaxed,
        );
    }

    async fn delay(&self) {
        let latency = Duration::from_nanos(self.latency_nanos.load(Ordering::Relaxed));
        if !latency.is_zero() {
            tokio::time::sleep(latency).await;
        }
    }
}

impl Display for LatencyObjectStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "LatencyObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for LatencyObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.delay().await;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.delay().await;
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.delay().await;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        self.delay().await;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
        let store = self.clone();
        stream::once(async move {
            store.delay().await;
            store.inner.delete_stream(locations)
        })
        .flatten()
        .boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        stream::once(async move {
            store.delay().await;
            store.inner.list(prefix.as_ref())
        })
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        let offset = offset.clone();
        stream::once(async move {
            store.delay().await;
            store.inner.list_with_offset(prefix.as_ref(), &offset)
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        self.delay().await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.delay().await;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        self.delay().await;
        self.inner.rename_opts(from, to, options).await
    }
}
