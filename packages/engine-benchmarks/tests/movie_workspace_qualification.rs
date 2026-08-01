use std::sync::Arc;
use std::time::{Duration, Instant};

use lix_engine::{Engine, FILE_UPLOAD_PART_BYTES, SessionContext, Storage};
use lix_rocksdb_storage::RocksDB;
use lix_slatedb_storage::SlateDB;

const PROXY_BYTES: usize = 128 * 1024 * 1024;
const INGEST_BYTES: usize = 512 * 1024 * 1024;
const STREAM_READ_BYTES: u64 = 1024 * 1024;
const STREAM_PERIOD: Duration = Duration::from_millis(80);
const PLAYBACK_READ_AHEAD: Duration = Duration::from_secs(1);
const STREAM_SAMPLES: usize = 40;
const SAVE_SAMPLES: usize = 40;
const HISTORY_COMMITS: usize = 5_000;

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
    let temp = tempfile::tempdir().expect("create RocksDB movie fixture");
    let storage = RocksDB::open(temp.path().join("database")).expect("open RocksDB fixture");
    qualify("rocksdb", storage).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "release qualification benchmark"]
async fn slatedb_movie_workspace_interference() {
    let temp = tempfile::tempdir().expect("create SlateDB movie fixture");
    let storage = SlateDB::open(temp.path().join("database")).expect("open SlateDB fixture");
    qualify("slatedb", storage).await;
}

async fn qualify<S>(backend: &str, storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let receipt = Engine::initialize(storage.clone())
        .await
        .expect("initialize movie workspace");
    let engine = Engine::new(storage).await.expect("open movie workspace");
    let seed = engine
        .open_session(receipt.main_branch_id.clone())
        .await
        .expect("open seed session");
    for revision in 0..HISTORY_COMMITS {
        seed.upsert_file_data(
            "/project/edit.json".to_owned(),
            format!("{{\"timelineRevision\":{revision}}}")
                .into_bytes()
                .into(),
        )
        .await
        .expect("seed project history");
    }
    upload(&seed, "proxy-seed", "/media/proxy.mov", PROXY_BYTES, 1).await;

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
    println!(
        "movie_workspace,backend={backend},save_p95_ms={:.3},stream_1_late={},stream_2_late={},ingest_mib_s={ingest_mib_s:.1}",
        save_p95.as_secs_f64() * 1000.0,
        first_late,
        second_late,
    );
    assert!(save_p95 < Duration::from_millis(500));
    assert_eq!(first_late, 0, "first 100 Mbit/s stream fell behind");
    assert_eq!(second_late, 0, "second 100 Mbit/s stream fell behind");
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
        .upsert_file_data_part(
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
        .upsert_file_data_part(
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
        .read_file_data(
            "/media/restart.mov".to_owned(),
            Some(FILE_UPLOAD_PART_BYTES as u64..total),
        )
        .await
        .expect("read restarted upload")
        .expect("restarted file exists");
    assert_eq!(tail.data().as_ref(), &[0x61; 9]);
}

async fn upload<S>(
    session: &SessionContext<S>,
    upload_id: &str,
    path: &str,
    total_bytes: usize,
    seed: u64,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut offset = 0usize;
    while offset < total_bytes {
        let len = (total_bytes - offset).min(FILE_UPLOAD_PART_BYTES);
        let data = deterministic_bytes(len, seed ^ offset as u64);
        let progress = session
            .upsert_file_data_part(
                upload_id.to_owned(),
                path.to_owned(),
                offset as u64,
                total_bytes as u64,
                data.into(),
            )
            .await
            .expect("upload movie part");
        offset += len;
        assert_eq!(progress.next_offset, offset as u64);
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
            .read_file_data(
                "/media/proxy.mov".to_owned(),
                Some(offset..offset + STREAM_READ_BYTES),
            )
            .await
            .expect("read proxy range")
            .expect("proxy exists");
        assert_eq!(read.data().len(), STREAM_READ_BYTES as usize);
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
            .upsert_file_data("/project/edit.json".to_owned(), payload.into_bytes().into())
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
