//! Measures what one binary-CAS reclamation sweep costs an object store as a
//! function of the number of rows in the chunk plane.
//!
//! The chunk plane is `ValueSemantics::Immutable`, so on SlateDB its values do
//! not live in the LSM at all: the LSM holds a locator and the payload lives in
//! an object-store segment. A reclamation scan that projects `FullValue`
//! therefore pays an object-store round trip per scan page, and the page is
//! hydrated *before* the caller can decide a row is live — so the cost is paid
//! for every chunk in the repository, not only for the reclaimed ones.
//!
//! Setup runs against the unwrapped local object store and the store is closed
//! and reopened before the measured sweep, so an optional latency profile is
//! charged to the sweep only. Request counts are deterministic; wall clock at a
//! given profile is what those counts cost over a link.
//!
//! ```text
//! e3_cas_reclaim_scan <dir> <file_count> <file_kib> <local|regional|wide-area|rtt:mbps> <keep|orphan>
//! ```

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use lix::Value;
use lix::open_lix;
use lix::storage::{
    BeginScanOptions, CoreProjection, KeyRange, MAX_SCAN_PAGE_ROWS, ReadOptions, Storage,
    StorageRead,
};
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::collect_repository_gc_for_bench;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBObjectStoreOptions};

#[path = "support/remote_object_store.rs"]
mod remote_object_store;

use remote_object_store::{RemoteObjectStore, RemoteProfile};

const MANIFEST_SPACE: lix::storage::SpaceId = lix::storage::SpaceId(0x0005_0001);
const MANIFEST_CHUNK_SPACE: lix::storage::SpaceId = lix::storage::SpaceId(0x0005_0002);
const PAYLOAD_SPACE: lix::storage::SpaceId = lix::storage::SpaceId(0x0005_0003);
const PRESENCE_SPACE: lix::storage::SpaceId = lix::storage::SpaceId(0x0005_0004);

fn open(dir: &Path, profile: Option<RemoteProfile>, counters: &SlateDBIoCounters) -> SlateDB {
    std::fs::create_dir_all(dir).expect("create reclaim scan directory");
    let local = object_store::local::LocalFileSystem::new_with_prefix(dir)
        .expect("open reclaim scan local object store");
    let store: Arc<dyn object_store::ObjectStore> = match profile {
        None => Arc::new(local),
        Some(profile) => Arc::new(RemoteObjectStore::new(Arc::new(local), profile)),
    };
    SlateDB::open_object_store_with_options_and_io_counters(
        "db",
        store,
        SlateDBObjectStoreOptions::default(),
        counters.clone(),
    )
    .expect("open reclaim scan SlateDB")
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let dir = args
        .get(1)
        .map(String::as_str)
        .unwrap_or("/tmp/lix-e3-cas-reclaim-scan");
    let file_count = args
        .get(2)
        .map(|value| value.parse::<usize>().expect("file count"))
        .unwrap_or(100);
    let file_kib = args
        .get(3)
        .map(|value| value.parse::<usize>().expect("file KiB"))
        .unwrap_or(16);
    let profile = args
        .get(4)
        .map_or_else(RemoteProfile::from_env, |value| RemoteProfile::parse(value));
    let orphan = args.get(5).map(String::as_str).unwrap_or("orphan") == "orphan";

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("create reclaim scan runtime");

    runtime.block_on(async move {
        let dir = Path::new(dir);
        let _ = std::fs::remove_dir_all(dir);

        // Phase 1: build the fixture over the unwrapped local store so setup
        // never pays the profile under test.
        let setup_counters = SlateDBIoCounters::default();
        let storage = open(dir, None, &setup_counters);
        let session = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("open reclaim scan repository");
        let setup_started = Instant::now();
        for index in 0..file_count {
            session
                .execute(
                    "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
                     ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                    &[
                        Value::Text(format!("/e3/payload-{index:07}.bin")),
                        Value::Blob(distinct_payload(index as u64, file_kib * 1024).into()),
                    ],
                )
                .await
                .expect("write reclaim scan payload");
        }
        if orphan {
            session
                .execute(
                    "DELETE FROM lix_file WHERE path LIKE $1",
                    &[Value::Text("/e3/%".to_owned())],
                )
                .await
                .expect("delete reclaim scan payloads");
            session
                .create_checkpoint()
                .await
                .expect("checkpoint reclaim scan deletion");
        }
        let setup_ms = setup_started.elapsed().as_millis();
        let before = cas_stats(&storage).await;
        drop(session);
        storage.flush().await.expect("flush reclaim scan setup");
        drop(storage);

        // Phase 2: reopen, optionally behind the latency profile, and measure
        // exactly one reclamation sweep.
        let counters = SlateDBIoCounters::default();
        let storage = open(dir, profile, &counters);
        let adapter = StorageAdapter::new(storage.clone());
        let opened = counters.snapshot();
        let started = Instant::now();
        let sweep = collect_repository_gc_for_bench(&adapter)
            .await
            .expect("reclaim scan GC should commit");
        let sweep_ms = started.elapsed().as_millis();
        let io = counters.snapshot().saturating_sub(opened);
        drop(adapter);
        let after = cas_stats(&storage).await;
        storage.flush().await.expect("flush reclaim scan sweep");
        drop(storage);

        println!(
            "e3_cas_reclaim_scan,files={file_count},file_kib={file_kib},orphan={orphan},\
profile={},setup_ms={setup_ms},sweep_ms={sweep_ms},\
chunk_rows_before={},chunk_rows_after={},presence_rows_before={},manifest_rows_before={},\
reclaimed_chunk_rows={},reclaimed_manifest_rows={},staged_deletes={},plan_us={},commit_us={},\
read_objects={},read_bytes={},write_objects={},write_bytes={},list_operations={},\
immutable_locator_rows={}",
            profile.map_or_else(|| "local".to_owned(), |profile| profile.label()),
            before[2].rows,
            after[2].rows,
            before[3].rows,
            before[0].rows,
            sweep.reclaimed_chunk_rows,
            sweep.reclaimed_manifest_rows,
            sweep.staged_deletes,
            sweep.plan_us,
            sweep.commit_us,
            io.read_objects,
            io.read_bytes,
            io.write_objects,
            io.write_bytes,
            io.list_operations,
            io.immutable_locator_rows,
        );
        let _ = std::fs::remove_dir_all(dir);
    });
}

fn distinct_payload(seed: u64, len: usize) -> Vec<u8> {
    let mut output = vec![0_u8; len];
    for (block, bytes) in output.chunks_mut(32).enumerate() {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"lix e3 cas reclaim scan payload v1");
        hasher.update(&seed.to_le_bytes());
        hasher.update(&(block as u64).to_le_bytes());
        let digest = hasher.finalize();
        let count = bytes.len();
        bytes.copy_from_slice(&digest.as_bytes()[..count]);
    }
    output
}

#[derive(Clone, Copy, Debug)]
struct SpaceStats {
    rows: u64,
}

async fn cas_stats<S: Storage>(storage: &S) -> [SpaceStats; 4] {
    [
        space_stats(storage, MANIFEST_SPACE).await,
        space_stats(storage, MANIFEST_CHUNK_SPACE).await,
        space_stats(storage, PAYLOAD_SPACE).await,
        space_stats(storage, PRESENCE_SPACE).await,
    ]
}

async fn space_stats<S: Storage>(storage: &S, space_id: lix::storage::SpaceId) -> SpaceStats {
    let space = lix::storage_bench::storage_space_by_id(space_id.0);
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("open reclaim scan stats read");
    let mut rows = 0;
    let mut cursor = read
        .begin_scan(
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions {
                // Key-only: counting rows must not hydrate immutable segments,
                // or the instrument would cost what it measures.
                projection: CoreProjection::KeyOnly,
                ..BeginScanOptions::default()
            },
        )
        .await
        .expect("begin reclaim scan stats scan");
    loop {
        let (entries, has_more) = cursor
            .next_page(MAX_SCAN_PAGE_ROWS)
            .await
            .expect("scan reclaim scan stats")
            .into_parts();
        rows += entries.len() as u64;
        if !has_more {
            break;
        }
    }
    SpaceStats { rows }
}
