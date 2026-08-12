//! Measures how much a second write of a binary file shares with the first.
//!
//! The binary CAS stores a blob as an ordered list of content-addressed chunks.
//! A second write only pays for the chunks whose hashes are new, so the useful
//! question for media and other binaries is: after editing a file, how many of
//! its bytes still land on chunks that are already stored?
//!
//! The harness answers that with the storage spaces themselves rather than with
//! an in-process model. It writes v1 through the public SQL file surface, reads
//! the binary-CAS payload space, writes v2 through the same surface, reads the
//! payload space again, and reports the difference. `new_payload_bytes` is
//! therefore exactly what v2 cost on disk in chunk content, and
//! `shared_bytes = v2_len - new_payload_bytes` is what it reused.
//!
//! Every case is driven from a real file on disk. Shapes named `supplied/*`
//! come from a second real file produced by a real tool (an ffmpeg re-export, a
//! `git archive` of a later revision). Shapes named `derived/*` apply one
//! byte-level edit -- overwrite, insert, delete, append -- to the real base so
//! the four canonical edit geometries are covered for every case.
//!
//! ```sh
//! cargo run -p lix_e2e --release --features storage-benches,slatedb \
//!   --example cas_sharing -- <corpus-dir>
//! ```
//!
//! The corpus directory holds one subdirectory per case containing `base.bin`
//! and zero or more `v2_<shape>.bin` files.

use std::fmt::{self, Display, Formatter};
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lix::integration::{Engine, SessionContext};
use lix::storage::{
    BeginScanOptions, CoreProjection, KeyRange, MAX_SCAN_PAGE_ROWS, ProjectedValue, ReadOptions,
    SpaceId, Storage, StorageRead,
};
use lix::{CreateBranchOptions, Value};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{
    SlateDB, SlateDBIoCounters, SlateDBIoSnapshot, SlateDBObjectStoreOptions,
};

#[path = "support/remote_object_store.rs"]
mod remote_object_store;

use remote_object_store::{RemoteObjectStore, RemoteProfile};

const MANIFEST_SPACE: SpaceId = SpaceId(0x0005_0001);
const MANIFEST_CHUNK_SPACE: SpaceId = SpaceId(0x0005_0002);
const PAYLOAD_SPACE: SpaceId = SpaceId(0x0005_0003);
const PRESENCE_SPACE: SpaceId = SpaceId(0x0005_0004);
const FILE_PATH: &str = "/case.bin";
const UPSERT_SQL: &str = "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
                          ON CONFLICT (path) DO UPDATE SET content = excluded.content";
const BRANCH_A_ID: &str = "01990000-0000-7000-8000-0000000000a1";
const BRANCH_B_ID: &str = "01990000-0000-7000-8000-0000000000b2";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Backend {
    RocksDB,
    SlateDB,
}

impl Backend {
    fn parse(value: &str) -> Self {
        match value {
            "rocksdb" => Self::RocksDB,
            "slatedb" => Self::SlateDB,
            other => panic!("backend must be rocksdb or slatedb, got '{other}'"),
        }
    }
}

impl Display for Backend {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::RocksDB => formatter.write_str("rocksdb"),
            Self::SlateDB => formatter.write_str("slatedb"),
        }
    }
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let corpus = PathBuf::from(
        arguments
            .next()
            .expect("usage: cas_sharing <corpus-dir> [backends]"),
    );
    let backends = arguments
        .next()
        .unwrap_or_else(|| "slatedb".to_owned())
        .split(',')
        .map(Backend::parse)
        .collect::<Vec<_>>();

    // Absent means the production local open path, exactly as before.
    let profile = RemoteProfile::from_env();
    eprintln!(
        "cas_sharing profile={}",
        profile.map_or_else(|| "local".to_owned(), |profile| profile.label())
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build cas sharing runtime");
    runtime.block_on(run(&corpus, &backends, profile));
}

async fn run(corpus: &Path, backends: &[Backend], profile: Option<RemoteProfile>) {
    let mut cases = fs::read_dir(corpus)
        .expect("read corpus directory")
        .map(|entry| entry.expect("read corpus entry").path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    cases.sort();

    for case in cases {
        let name = case
            .file_name()
            .expect("case directory name")
            .to_string_lossy()
            .into_owned();
        let base = fs::read(case.join("base.bin")).expect("read case base.bin");

        let mut supplied = fs::read_dir(&case)
            .expect("read case directory")
            .map(|entry| entry.expect("read case entry").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|value| value.to_str())
                    .is_some_and(|value| value.starts_with("v2_") && value.ends_with(".bin"))
            })
            .collect::<Vec<_>>();
        supplied.sort();

        let mut shapes = Vec::new();
        for path in &supplied {
            let label = path
                .file_name()
                .and_then(|value| value.to_str())
                .expect("supplied variant name")
                .trim_start_matches("v2_")
                .trim_end_matches(".bin")
                .to_owned();
            shapes.push((
                format!("supplied/{label}"),
                fs::read(path).expect("read supplied variant"),
            ));
        }
        shapes.push(("derived/overwrite_4k_mid".to_owned(), overwrite(&base)));
        shapes.push(("derived/insert_4k_at_1pct".to_owned(), insert(&base)));
        shapes.push(("derived/delete_4k_at_1pct".to_owned(), delete(&base)));
        shapes.push(("derived/append_1m".to_owned(), append(&base)));

        for &backend in backends {
            for (shape, v2) in &shapes {
                let report = measure_pair(backend, &base, v2, profile).await;
                report.print(&name, backend, shape, base.len(), v2.len());
            }
            // Two branches holding different edits of the same base file.
            let branch =
                measure_branches(backend, &base, &insert(&base), &overwrite(&base), profile).await;
            branch.print(&name, backend, base.len());
        }
    }
}

/// One database, three measurement points: empty, after v1, after v2.
struct PairReport {
    after_v1: Layout,
    after_v2: Layout,
    physical_after_v1: u64,
    physical_after_v2: u64,
    v1_elapsed: Duration,
    v2_elapsed: Duration,
    v2_durable: Duration,
    io_after_v1: SlateDBIoSnapshot,
    io_after_v2: SlateDBIoSnapshot,
}

impl PairReport {
    fn print(&self, case: &str, backend: Backend, shape: &str, v1_len: usize, v2_len: usize) {
        let new_payload_bytes = self
            .after_v2
            .payload_value_bytes
            .saturating_sub(self.after_v1.payload_value_bytes);
        let shared_bytes = (v2_len as u64).saturating_sub(new_payload_bytes);
        let sharing_ratio = if v2_len == 0 {
            0.0
        } else {
            shared_bytes as f64 / v2_len as f64
        };
        println!(
            "cas_sharing,case={case},backend={backend},shape={shape},\
             v1_bytes={v1_len},v2_bytes={v2_len},\
             payload_rows_v1={},payload_bytes_v1={},payload_rows_v2={},payload_bytes_v2={},\
             new_payload_rows={},new_payload_bytes={new_payload_bytes},\
             shared_bytes={shared_bytes},sharing_ratio={sharing_ratio:.4},\
             manifest_rows={},manifest_bytes={},manifest_chunk_rows={},manifest_chunk_bytes={},\
             presence_rows={},physical_bytes_v1={},physical_bytes_v2={},\
             physical_delta={},v1_ms={:.3},v2_ms={:.3},v2_durable_ms={:.3},\
             v2_object_write_bytes={},v2_object_write_objects={},\
             v2_object_read_bytes={},v2_object_read_objects={}",
            self.after_v1.payload_rows,
            self.after_v1.payload_value_bytes,
            self.after_v2.payload_rows,
            self.after_v2.payload_value_bytes,
            self.after_v2
                .payload_rows
                .saturating_sub(self.after_v1.payload_rows),
            self.after_v2.manifest_rows,
            self.after_v2.manifest_value_bytes,
            self.after_v2.manifest_chunk_rows,
            self.after_v2.manifest_chunk_value_bytes,
            self.after_v2.presence_rows,
            self.physical_after_v1,
            self.physical_after_v2,
            i128::from(self.physical_after_v2) - i128::from(self.physical_after_v1),
            self.v1_elapsed.as_secs_f64() * 1_000.0,
            self.v2_elapsed.as_secs_f64() * 1_000.0,
            self.v2_durable.as_secs_f64() * 1_000.0,
            self.io_after_v2
                .write_bytes
                .saturating_sub(self.io_after_v1.write_bytes),
            self.io_after_v2
                .write_objects
                .saturating_sub(self.io_after_v1.write_objects),
            self.io_after_v2
                .read_bytes
                .saturating_sub(self.io_after_v1.read_bytes),
            self.io_after_v2
                .read_objects
                .saturating_sub(self.io_after_v1.read_objects),
        );
    }
}

struct BranchReport {
    after_base: Layout,
    after_branches: Layout,
    a_len: usize,
    b_len: usize,
}

impl BranchReport {
    fn print(&self, case: &str, backend: Backend, base_len: usize) {
        let new_payload_bytes = self
            .after_branches
            .payload_value_bytes
            .saturating_sub(self.after_base.payload_value_bytes);
        let logical = (self.a_len + self.b_len) as u64;
        let shared_bytes = logical.saturating_sub(new_payload_bytes);
        let sharing_ratio = shared_bytes as f64 / logical as f64;
        println!(
            "cas_sharing,case={case},backend={backend},shape=branches/two_edits,\
             v1_bytes={base_len},v2_bytes={logical},\
             payload_rows_v1={},payload_bytes_v1={},payload_rows_v2={},payload_bytes_v2={},\
             new_payload_rows={},new_payload_bytes={new_payload_bytes},\
             shared_bytes={shared_bytes},sharing_ratio={sharing_ratio:.4},\
             manifest_rows={},manifest_bytes={},manifest_chunk_rows={},manifest_chunk_bytes={},\
             presence_rows={},physical_bytes_v1=0,physical_bytes_v2=0,physical_delta=0,\
             v1_ms=0.000,v2_ms=0.000,v2_durable_ms=0.000,\
             v2_object_write_bytes=0,v2_object_write_objects=0,\
             v2_object_read_bytes=0,v2_object_read_objects=0",
            self.after_base.payload_rows,
            self.after_base.payload_value_bytes,
            self.after_branches.payload_rows,
            self.after_branches.payload_value_bytes,
            self.after_branches
                .payload_rows
                .saturating_sub(self.after_base.payload_rows),
            self.after_branches.manifest_rows,
            self.after_branches.manifest_value_bytes,
            self.after_branches.manifest_chunk_rows,
            self.after_branches.manifest_chunk_value_bytes,
            self.after_branches.presence_rows,
        );
    }
}

async fn measure_pair(
    backend: Backend,
    v1: &[u8],
    v2: &[u8],
    profile: Option<RemoteProfile>,
) -> PairReport {
    let fixture = Fixture::new(backend, profile).await;
    let v1_elapsed = fixture.write(FILE_PATH, v1).await;
    fixture.flush().await;
    let after_v1 = fixture.layout().await;
    let physical_after_v1 = directory_bytes(fixture.root());
    let io_after_v1 = fixture.io();

    // The durable cost of v2: the SQL write plus the flush that makes it
    // survive. On a remote store most of that cost is in the flush, so timing
    // `execute` alone would price the object traffic at zero.
    let durable_started = Instant::now();
    let v2_elapsed = fixture.write(FILE_PATH, v2).await;
    fixture.flush().await;
    let v2_durable = durable_started.elapsed();
    let after_v2 = fixture.layout().await;
    let physical_after_v2 = directory_bytes(fixture.root());
    let io_after_v2 = fixture.io();

    PairReport {
        after_v1,
        after_v2,
        physical_after_v1,
        physical_after_v2,
        v1_elapsed,
        v2_elapsed,
        v2_durable,
        io_after_v1,
        io_after_v2,
    }
}

async fn measure_branches(
    backend: Backend,
    base: &[u8],
    a: &[u8],
    b: &[u8],
    profile: Option<RemoteProfile>,
) -> BranchReport {
    let fixture = Fixture::new(backend, profile).await;
    fixture.write(FILE_PATH, base).await;
    fixture.flush().await;
    let after_base = fixture.layout().await;

    fixture.write_on_branch(BRANCH_A_ID, "sharing-branch-a", FILE_PATH, a).await;
    fixture.write_on_branch(BRANCH_B_ID, "sharing-branch-b", FILE_PATH, b).await;
    fixture.flush().await;
    let after_branches = fixture.layout().await;

    BranchReport {
        after_base,
        after_branches,
        a_len: a.len(),
        b_len: b.len(),
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Layout {
    manifest_rows: u64,
    manifest_value_bytes: u64,
    manifest_chunk_rows: u64,
    manifest_chunk_value_bytes: u64,
    payload_rows: u64,
    payload_value_bytes: u64,
    presence_rows: u64,
}

struct BackendFixture<S: Storage + 'static> {
    engine: Engine<S>,
    session: SessionContext<S>,
    storage: S,
    _temp_dir: tempfile::TempDir,
    root: PathBuf,
}

impl<S> BackendFixture<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn create(storage: S, temp_dir: tempfile::TempDir) -> Self {
        let root = temp_dir.path().to_owned();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("initialize cas sharing engine");
        let engine = Engine::new(storage.clone())
            .await
            .expect("open cas sharing engine");
        let session = engine
            .open_session_at(receipt.main_branch_id)
            .await
            .expect("open cas sharing session");
        Self {
            engine,
            session,
            storage,
            _temp_dir: temp_dir,
            root,
        }
    }

    async fn write(&self, path: &str, bytes: &[u8]) -> Duration {
        let started = Instant::now();
        let result = self
            .session
            .execute(
                UPSERT_SQL,
                &[
                    Value::Text(path.to_owned()),
                    Value::Blob(bytes.to_vec().into()),
                ],
            )
            .await
            .expect("write cas sharing blob");
        let elapsed = started.elapsed();
        assert_eq!(result.rows_affected(), 1);
        elapsed
    }

    async fn write_on_branch(&self, id: &str, name: &str, path: &str, bytes: &[u8]) {
        let branch = self
            .session
            .create_branch(CreateBranchOptions {
                id: Some(id.to_owned()),
                name: name.to_owned(),
                from_commit_id: None,
            })
            .await
            .expect("create cas sharing branch");
        let session = self
            .engine
            .open_session_at(branch.id)
            .await
            .expect("open cas sharing branch session");
        let result = session
            .execute(
                UPSERT_SQL,
                &[
                    Value::Text(path.to_owned()),
                    Value::Blob(bytes.to_vec().into()),
                ],
            )
            .await
            .expect("write cas sharing branch blob");
        assert_eq!(result.rows_affected(), 1);
    }

    async fn layout(&self) -> Layout {
        let manifest = space_accounting(&self.storage, MANIFEST_SPACE).await;
        let manifest_chunk = space_accounting(&self.storage, MANIFEST_CHUNK_SPACE).await;
        let payload = space_accounting(&self.storage, PAYLOAD_SPACE).await;
        let presence = space_accounting(&self.storage, PRESENCE_SPACE).await;
        Layout {
            manifest_rows: manifest.0,
            manifest_value_bytes: manifest.1,
            manifest_chunk_rows: manifest_chunk.0,
            manifest_chunk_value_bytes: manifest_chunk.1,
            payload_rows: payload.0,
            payload_value_bytes: payload.1,
            presence_rows: presence.0,
        }
    }
}

enum Fixture {
    Rocks(BackendFixture<RocksDB>),
    Slate(BackendFixture<SlateDB>, SlateDBIoCounters),
}

impl Fixture {
    async fn new(backend: Backend, profile: Option<RemoteProfile>) -> Self {
        let temp_dir = tempfile::tempdir().expect("create cas sharing directory");
        let database_path = temp_dir.path().join("database");
        match backend {
            Backend::RocksDB => {
                let storage = RocksDB::open(&database_path).expect("open cas sharing RocksDB");
                Self::Rocks(BackendFixture::create(storage, temp_dir).await)
            }
            Backend::SlateDB => {
                let counters = SlateDBIoCounters::default();
                let storage = match profile {
                    // No profile: the production local open path, unchanged,
                    // so local numbers stay comparable with every prior run.
                    None => SlateDB::open_with_io_counters(&database_path, counters.clone())
                        .expect("open cas sharing SlateDB"),
                    Some(profile) => {
                        fs::create_dir_all(&database_path)
                            .expect("create cas sharing SlateDB directory");
                        let local = object_store::local::LocalFileSystem::new_with_prefix(
                            &database_path,
                        )
                        .expect("open cas sharing local object store");
                        let remote: Arc<dyn object_store::ObjectStore> =
                            Arc::new(RemoteObjectStore::new(Arc::new(local), profile));
                        SlateDB::open_object_store_with_options_and_io_counters(
                            "db",
                            remote,
                            SlateDBObjectStoreOptions::default(),
                            counters.clone(),
                        )
                        .expect("open cas sharing remote SlateDB")
                    }
                };
                Self::Slate(BackendFixture::create(storage, temp_dir).await, counters)
            }
        }
    }

    /// Object-store traffic so far. Zero for RocksDB, which has no object
    /// store: it is a local LSM and its remote profile is its local one.
    fn io(&self) -> SlateDBIoSnapshot {
        match self {
            Self::Rocks(_) => SlateDBIoSnapshot::default(),
            Self::Slate(_, counters) => counters.snapshot(),
        }
    }

    async fn write(&self, path: &str, bytes: &[u8]) -> Duration {
        match self {
            Self::Rocks(fixture) => fixture.write(path, bytes).await,
            Self::Slate(fixture, _) => fixture.write(path, bytes).await,
        }
    }

    async fn write_on_branch(&self, id: &str, name: &str, path: &str, bytes: &[u8]) {
        match self {
            Self::Rocks(fixture) => fixture.write_on_branch(id, name, path, bytes).await,
            Self::Slate(fixture, _) => fixture.write_on_branch(id, name, path, bytes).await,
        }
    }

    async fn layout(&self) -> Layout {
        match self {
            Self::Rocks(fixture) => fixture.layout().await,
            Self::Slate(fixture, _) => fixture.layout().await,
        }
    }

    fn root(&self) -> &Path {
        match self {
            Self::Rocks(fixture) => &fixture.root,
            Self::Slate(fixture, _) => &fixture.root,
        }
    }

    async fn flush(&self) {
        match self {
            Self::Rocks(fixture) => fixture.storage.flush().expect("flush cas sharing RocksDB"),
            Self::Slate(fixture, _) => fixture
                .storage
                .flush()
                .await
                .expect("flush cas sharing SlateDB"),
        }
    }
}

async fn space_accounting<S>(storage: &S, space: SpaceId) -> (u64, u64)
where
    S: Storage,
{
    // A space id has exactly one value semantics and the engine registry is
    // where it is declared; guessing it here scans a different physical
    // location than the engine wrote.
    let space = lix::storage_bench::storage_space_by_id(space.0);
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("open accounting read");
    let mut rows = 0u64;
    let mut value_bytes = 0u64;
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
        .expect("begin accounting scan");
    loop {
        let (page, page_has_more) = cursor
            .next_page(MAX_SCAN_PAGE_ROWS)
            .await
            .expect("scan accounting space").into_parts();
        rows += page.len() as u64;
        value_bytes += page
            .iter()
            .map(|entry| match &entry.value {
                ProjectedValue::KeyOnly => 0,
                ProjectedValue::FullValue(value) => value.len() as u64,
            })
            .sum::<u64>();
        if !page_has_more {
            break;
        }
    }
    (rows, value_bytes)
}

const EDIT_BYTES: usize = 4 << 10;

fn overwrite(base: &[u8]) -> Vec<u8> {
    let mut out = base.to_vec();
    let len = EDIT_BYTES.min(out.len());
    let start = (out.len() - len) / 2;
    fill(&mut out[start..start + len], 0x5a17_5a17_5a17_5a17);
    out
}

fn insert(base: &[u8]) -> Vec<u8> {
    let at = base.len() / 100;
    let mut out = Vec::with_capacity(base.len() + EDIT_BYTES);
    out.extend_from_slice(&base[..at]);
    let mut patch = vec![0u8; EDIT_BYTES];
    fill(&mut patch, 0x1234_5678_9abc_def0);
    out.extend_from_slice(&patch);
    out.extend_from_slice(&base[at..]);
    out
}

fn delete(base: &[u8]) -> Vec<u8> {
    let at = base.len() / 100;
    let end = (at + EDIT_BYTES).min(base.len());
    let mut out = Vec::with_capacity(base.len() - (end - at));
    out.extend_from_slice(&base[..at]);
    out.extend_from_slice(&base[end..]);
    out
}

fn append(base: &[u8]) -> Vec<u8> {
    let mut out = base.to_vec();
    let mut patch = vec![0u8; 1 << 20];
    fill(&mut patch, 0x0fed_cba9_8765_4321);
    out.extend_from_slice(&patch);
    out
}

fn fill(bytes: &mut [u8], seed: u64) {
    let mut state = seed ^ 0xd1b5_4a32_d192_ed03;
    for chunk in bytes.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let generated = state.to_le_bytes();
        chunk.copy_from_slice(&generated[..chunk.len()]);
    }
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    if !metadata.is_dir() {
        return 0;
    }
    fs::read_dir(path)
        .expect("read cas sharing storage directory")
        .map(|entry| directory_bytes(&entry.expect("read cas sharing directory entry").path()))
        .sum()
}
