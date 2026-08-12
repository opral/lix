//! Crash-consistency qualification: kill a real writer process with SIGKILL at
//! every point of the publication window, then reopen the store in a fresh
//! process and prove the invariants still hold.
//!
//! Every other "reopen" test in this repository performs an orderly
//! `flush -> close -> drop -> reopen`. That verifies clean rollback, not crash
//! consistency: it never observes the store in a state a cooperative shutdown
//! cannot produce. This qualification closes that gap for the layout invariant
//! that "state root, commit record, derived-view update and ref move publish in
//! one atomic write set, never in stages another reader can observe half-done".
//!
//! ## How the window is swept
//!
//! [`CrashStorage`] wraps a real backend — [`RocksDB`] or `SlateDB`, selected by
//! the [`CrashBackend`] type parameter — and counts every mutation the engine
//! issues at the storage boundary: `begin_write`, each `put_many`,
//! `delete_many` and `delete_range`, the moment before `commit()` is delegated,
//! and the moment after it returns. A child process is launched once per kill
//! point with `LIX_CRASH_KILL_AT=k`; the wrapper raises `SIGKILL` on itself when
//! the counter reaches `k`, so the process dies *inside* the publication with no
//! unwinding, no destructor, and no flush. The sweep is exhaustive over `k`, and
//! the event index is deterministic for a fixed workload, so a failing point
//! reproduces by rerunning the same `k`.
//!
//! A second phase kills on a seeded wall-clock delay instead. That is the only
//! way to land inside the backend's own write path (RocksDB's WAL append and
//! memtable insert; SlateDB's write pipeline, WAL buffer and object-store
//! upload), which the storage-boundary sweep steps over atomically.
//!
//! ## What SIGKILL can and cannot prove
//!
//! SIGKILL models *process* death: the kernel keeps the page cache, so anything
//! the process had already handed to `write(2)` survives. It therefore proves
//! that the engine does not publish in observable stages. It does **not** model
//! power loss, which additionally requires the backend to have fsynced. See
//! `rocksdb_ignores_await_durable_and_therefore_proves_only_process_crash_consistency`
//! in this file for the separate, statically-decidable half of that question.
//!
//! There is a second, sharper distinction that only shows up once a second
//! backend is swept: the two adapters do not acknowledge the same thing.
//!
//! * RocksDB stages into one `WriteBatch` and applies it inside `db.write()`
//!   before `commit()` returns, so an acknowledged commit is already in the
//!   kernel's page cache. SIGKILL cannot take it back; only power loss can,
//!   because the WAL is never fsynced.
//! * SlateDB's adapter publishes the write set onto an in-process pipeline and
//!   returns (`slatedb.rs:3784-3806`). Unless `WriteOptions::await_durable` is
//!   set, the bytes are still in the dying process's heap when `commit()`
//!   returns `Ok`, and SIGKILL discards them.
//!
//! `WriteOptions::await_durable` is the flag that closes that gap, and **no SQL
//! path sets it**: `stage_atomic_cas_publication`, the only writer that does, is
//! reachable solely from the resumable media-upload session API
//! (`session/media_upload.rs:441`). The sweep therefore measures the flag
//! directly — `LIX_CRASH_AWAIT_DURABLE=1` forces it on at the storage boundary
//! and reruns the identical schedule — and reports consistency violations and
//! lost acknowledgements in separate columns, so a durability contract
//! difference is never collapsed into a corruption-shaped number.
//!
//! ## Cost
//!
//! The default sweep is deliberately small enough for CI. Set
//! `LIX_CRASH_CONSISTENCY_DEEP=1` for a wider workload and a longer timed
//! phase, or set the individual knobs:
//!
//! * `LIX_CRASH_CONSISTENCY_ROWS` — rows rewritten per commit (default 8)
//! * `LIX_CRASH_CONSISTENCY_COMMITS` — commits attempted per trial (default 3)
//! * `LIX_CRASH_CONSISTENCY_TIMED_TRIALS` — seeded wall-clock kills (default 12)
//! * `LIX_CRASH_CONSISTENCY_MAX_POINTS` — cap on swept kill points (default 512)
//! * `LIX_CRASH_CONSISTENCY_SEED` — seed for the wall-clock kill delays
//! * `LIX_CRASH_AWAIT_DURABLE=1` — force `WriteOptions::await_durable` on every
//!   write transaction, in every arm (the durability A/B)
//! * `LIX_CRASH_CONSISTENCY_BLOB_BYTES` — binary file bytes republished in the
//!   same transaction, `0` to publish rows only (default 64 KiB, 128 KiB deep).
//!   A non-zero value routes the publication through the content-addressed blob
//!   path, which widens the swept window. It does **not** set `await_durable`;
//!   measured, not assumed — the sweep prints the durable/total write census.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use lix::storage::{
    CommitResult, Key, KeyRange, PutBatch, ReadOptions, Storage, StorageError, StorageSpace,
    StorageWrite, WriteOptions,
};
use lix::{Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
#[cfg(feature = "slatedb")]
use lix_storage_slatedb::SlateDB;

const SCHEMA_KEY: &str = "crash_consistency_row";
const CHILD_ENV: &str = "LIX_CRASH_CONSISTENCY_CHILD";
const CHILD_TEST: &str = "crash_consistency_child_worker";
const BACKEND_ENV: &str = "LIX_CRASH_BACKEND";

/// Marker added by the post-recovery write so a second reopen can prove the
/// recovered store is stable, not merely readable once.
const RECOVERY_MARK: i64 = 1_000_000;

// ---------------------------------------------------------------------------
// Kill scheduling
// ---------------------------------------------------------------------------

/// Counts storage-boundary events and kills the process at a chosen one.
///
/// Counting is disarmed during setup so that kill point `k` always refers to
/// the same position inside the *measured* publication window regardless of how
/// many writes schema registration and seeding happen to take.
struct Killer {
    counter: AtomicU64,
    armed: AtomicBool,
    kill_at: u64,
    trace: bool,
    /// Write transactions opened inside the measured window, and how many of
    /// them asked for a durable acknowledgement. Recorded rather than assumed:
    /// whether the workload exercises `await_durable` is the thing that
    /// separates the two backends' acknowledgement contracts, so the harness
    /// reports it from the storage boundary instead of inferring it.
    writes: AtomicU64,
    durable_writes: AtomicU64,
}

impl Killer {
    fn new(kill_at: u64, trace: bool) -> Self {
        Self {
            counter: AtomicU64::new(0),
            armed: AtomicBool::new(false),
            kill_at,
            trace,
            writes: AtomicU64::new(0),
            durable_writes: AtomicU64::new(0),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn events(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
    }

    fn record_write(&self, await_durable: bool) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        self.writes.fetch_add(1, Ordering::SeqCst);
        if await_durable {
            self.durable_writes.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn durability_census(&self) -> (u64, u64) {
        (
            self.durable_writes.load(Ordering::SeqCst),
            self.writes.load(Ordering::SeqCst),
        )
    }

    fn tick(&self, label: &str) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        let index = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        if self.trace {
            println!("E20_EVENT {index} {label}");
        }
        if self.kill_at != 0 && index == self.kill_at {
            // Flush whatever the harness itself has buffered; the point is to
            // kill lix mid-publication, not to lose the trial's own bookkeeping.
            let _ = std::io::stdout().flush();
            // SIGKILL cannot be caught, blocked or ignored: no unwinding, no
            // Drop, no backend shutdown hook, no WAL flush, and for SlateDB no
            // chance for the background write pipeline to drain.
            unsafe {
                libc::kill(libc::getpid(), libc::SIGKILL);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Storage wrapper
// ---------------------------------------------------------------------------

/// A shipping storage adapter this qualification can sweep.
///
/// The only thing the harness needs beyond [`Storage`] is "open me at this
/// path", which is deliberately the same call a user makes. Both shipping
/// adapters implement it identically; nothing here is adapter-specific.
trait CrashBackend: Storage + Clone + Send + Sync + Sized + 'static {
    /// Value of [`BACKEND_ENV`] that selects this backend in the child process.
    const NAME: &'static str;

    fn open_at(path: &Path) -> Result<Self, StorageError>;
}

impl CrashBackend for RocksDB {
    const NAME: &'static str = "rocksdb";

    fn open_at(path: &Path) -> Result<Self, StorageError> {
        RocksDB::open(path)
    }
}

#[cfg(feature = "slatedb")]
impl CrashBackend for SlateDB {
    const NAME: &'static str = "slatedb";

    fn open_at(path: &Path) -> Result<Self, StorageError> {
        SlateDB::open(path)
    }
}

#[derive(Clone)]
struct CrashStorage<B> {
    inner: B,
    killer: Arc<Killer>,
    /// Overrides [`WriteOptions::await_durable`] to `true` on every write
    /// transaction the engine opens.
    ///
    /// No SQL path sets the flag on its own — `stage_atomic_cas_publication`,
    /// the only writer that does, is reachable only from the resumable
    /// media-upload session API (`session/media_upload.rs:441`), never from
    /// `INSERT INTO lix_file`. Forcing it at the storage boundary is therefore
    /// the only way to run the same workload, the same kill schedule and the
    /// same invariants with the one bit changed, which is what "does
    /// `await_durable` change the outcome" actually asks.
    force_await_durable: bool,
}

impl<B: CrashBackend> CrashStorage<B> {
    fn open(path: &Path, killer: Arc<Killer>, force_await_durable: bool) -> Self {
        Self {
            inner: B::open_at(path).expect("open crash-consistency backend fixture"),
            killer,
            force_await_durable,
        }
    }
}

impl<B: CrashBackend> Storage for CrashStorage<B> {
    type Read<'a>
        = B::Read<'a>
    where
        Self: 'a;

    type Write<'a>
        = CrashWrite<B::Write<'a>>
    where
        Self: 'a;

    fn begin_read(
        &self,
        opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send {
        self.inner.begin_read(opts)
    }

    fn begin_write(
        &self,
        opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send {
        let killer = self.killer.clone();
        let force_await_durable = self.force_await_durable;
        async move {
            let mut opts = opts;
            opts.await_durable |= force_await_durable;
            killer.record_write(opts.await_durable);
            killer.tick("begin_write");
            let inner = self.inner.begin_write(opts).await?;
            Ok(CrashWrite { inner, killer })
        }
    }
}

struct CrashWrite<W> {
    inner: W,
    killer: Arc<Killer>,
}

impl<W: StorageWrite> StorageWrite for CrashWrite<W> {
    fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.killer.tick("put_many");
        self.inner.put_many(space, entries)
    }

    fn delete_many(
        &mut self,
        space: StorageSpace,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.killer.tick("delete_many");
        self.inner.delete_many(space, keys)
    }

    fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.killer.tick("delete_range");
        self.inner.delete_range(space, range)
    }

    fn commit(self) -> impl Future<Output = Result<CommitResult, StorageError>> + Send {
        let killer = self.killer;
        let inner = self.inner;
        async move {
            killer.tick("pre_commit");
            let result = inner.commit().await;
            killer.tick("post_commit");
            result
        }
    }

    fn rollback(self) -> impl Future<Output = Result<(), StorageError>> + Send {
        let killer = self.killer;
        let inner = self.inner;
        async move {
            killer.tick("rollback");
            inner.rollback().await
        }
    }
}

// The read side is pass-through: the backend's own read handle already
// satisfies `StorageRead`, and a read cannot publish, so reads are not kill
// points.

// ---------------------------------------------------------------------------
// Workload shape
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Workload {
    rows: usize,
    commits: u64,
    /// Bytes of binary file content rewritten in the same transaction.
    ///
    /// A non-zero size routes each publication through the content-addressed
    /// blob path, which stages blob chunks and a publication fence alongside the
    /// row write set — a materially different publication shape from a plain row
    /// commit, and one that widens the swept window.
    ///
    /// Note it does **not** set `await_durable`; measured, not assumed. See
    /// [`Workload::force_await_durable`].
    blob_bytes: usize,
    /// Force `WriteOptions::await_durable` on at the storage boundary.
    ///
    /// The A/B for "what does the durability flag buy" — same workload, same
    /// kill schedule, same invariants, one bit changed.
    force_await_durable: bool,
}

impl Workload {
    fn from_env() -> Self {
        let deep = env_flag("LIX_CRASH_CONSISTENCY_DEEP");
        Self {
            rows: env_usize("LIX_CRASH_CONSISTENCY_ROWS", if deep { 64 } else { 8 }),
            commits: env_u64("LIX_CRASH_CONSISTENCY_COMMITS", if deep { 12 } else { 3 }),
            blob_bytes: env_usize(
                "LIX_CRASH_CONSISTENCY_BLOB_BYTES",
                if deep { 128 * 1024 } else { 64 * 1024 },
            ),
            force_await_durable: env_flag("LIX_CRASH_AWAIT_DURABLE"),
        }
    }
}

const BLOB_PATH: &str = "/crash-consistency.bin";

/// Deterministic content whose every byte depends on the generation, so a blob
/// left over from a different generation cannot masquerade as the current one.
fn blob_for(generation: i64, size: usize) -> Vec<u8> {
    let mut rng = SplitMix64::new(0xb10b_0000_0000_0000 ^ generation as u64);
    (0..size).map(|_| (rng.next() >> 24) as u8).collect()
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).map(|v| v != "0" && !v.is_empty()).unwrap_or(false)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ---------------------------------------------------------------------------
// Child process: the writer that gets killed
// ---------------------------------------------------------------------------

/// Re-entry point for the child processes the sweep spawns.
///
/// A plain test run executes this as a no-op; it only does work when the parent
/// has set [`CHILD_ENV`], which it does when re-invoking this same binary.
#[test]
fn crash_consistency_child_worker() {
    if std::env::var(CHILD_ENV).is_err() {
        return;
    }
    let db = PathBuf::from(std::env::var("LIX_CRASH_DB").expect("child needs LIX_CRASH_DB"));
    let ack = PathBuf::from(std::env::var("LIX_CRASH_ACK").expect("child needs LIX_CRASH_ACK"));
    let kill_at = env_u64("LIX_CRASH_KILL_AT", 0);
    let kill_after_nanos = env_u64("LIX_CRASH_KILL_AFTER_NANOS", 0);
    let trace = env_flag("LIX_CRASH_TRACE");
    let workload = Workload::from_env();
    let backend = std::env::var(BACKEND_ENV).unwrap_or_else(|_| RocksDB::NAME.to_owned());

    let killer = Arc::new(Killer::new(kill_at, trace));

    if kill_after_nanos != 0 {
        // Timed phase: a watchdog thread lands the kill wherever the writer
        // happens to be, including inside RocksDB's own WAL append.
        std::thread::Builder::new()
            .name("crash-consistency-watchdog".to_owned())
            .spawn(move || {
                std::thread::sleep(Duration::from_nanos(kill_after_nanos));
                let _ = std::io::stdout().flush();
                unsafe {
                    libc::kill(libc::getpid(), libc::SIGKILL);
                }
            })
            .expect("spawn crash-consistency watchdog");
    }

    match backend.as_str() {
        name if name == RocksDB::NAME => {
            run_on_large_stack(move || child_main::<RocksDB>(db, ack, killer, workload));
        }
        #[cfg(feature = "slatedb")]
        name if name == SlateDB::NAME => {
            run_on_large_stack(move || child_main::<SlateDB>(db, ack, killer, workload));
        }
        other => panic!("unknown crash-consistency backend {other:?}"),
    }
}

async fn child_main<B: CrashBackend>(
    db: PathBuf,
    ack: PathBuf,
    killer: Arc<Killer>,
    workload: Workload,
) {
    let storage = CrashStorage::<B>::open(&db, killer.clone(), workload.force_await_durable);
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("child opens crash-consistency Lix");

    // --- setup, deliberately not armed -------------------------------------
    let setup_started = Instant::now();
    register_schema(&lix).await;
    write_generation(&lix, workload, 0).await;
    // A fsynced marker so the verifier knows whether a crash landed in setup
    // (where the row invariant does not hold yet) or in the measured window.
    let marker = db.parent().expect("trial directory").join("setup.done");
    fs::write(&marker, b"1").expect("write crash-consistency setup marker");
    fs::File::open(&marker)
        .expect("reopen crash-consistency setup marker")
        .sync_all()
        .expect("fsync crash-consistency setup marker");
    println!("E20_SETUP_NANOS {}", setup_started.elapsed().as_nanos());
    let _ = std::io::stdout().flush();

    // --- measured publication window ---------------------------------------
    killer.arm();
    for generation in 1..=workload.commits {
        write_generation(&lix, workload, generation as i64).await;
        // Acknowledge only after `commit()` returned Ok. Whatever is in this
        // file must be visible after the crash; that is the durability half of
        // the invariant set.
        append_ack(&ack, generation);
    }

    let (durable, total) = killer.durability_census();
    println!("E20_EVENTS {}", killer.events());
    println!("E36_DURABLE_WRITES {durable} {total}");
    let _ = std::io::stdout().flush();
    lix.close().await.expect("child closes crash-consistency Lix");
}

fn append_ack(path: &Path, generation: u64) {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open crash-consistency ack log");
    writeln!(file, "{generation}").expect("append crash-consistency ack");
    file.sync_all().expect("fsync crash-consistency ack log");
}

// ---------------------------------------------------------------------------
// Invariants checked in a fresh process after the kill
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Recovered {
    /// Highest generation visible after the crash, or `None` when the store has
    /// no rows — either because the crash landed before the seed commit
    /// published, or because the backend had not yet persisted it (see
    /// `lost_acknowledgement`).
    generation: Option<i64>,
    /// Set when every state the store presented was internally consistent, but
    /// a commit whose `commit()` had already returned `Ok` is not in it.
    ///
    /// This is deliberately *not* an `Err`. Losing an acknowledged commit is a
    /// durability outcome, not a consistency one — the store is whole, it is
    /// merely older than the writer was told — and the two backends differ
    /// exactly here. Keeping them in separate columns is what lets the sweep
    /// report "0 inconsistencies, N lost acknowledgements" instead of collapsing
    /// a durability contract difference into a corruption-shaped number.
    lost_acknowledgement: Option<String>,
}

/// Everything the brief asks of a store that has just survived a crash.
async fn verify_after_crash<B: CrashBackend>(
    path: PathBuf,
    acked: Option<i64>,
    attempted: i64,
    workload: Workload,
    setup_complete: bool,
) -> Result<Recovered, String> {
    let rows = workload.rows;
    let mut lost_acknowledgement: Option<String> = None;
    // 1. Does the store open at all?
    let storage = B::open_at(&path).map_err(|error| format!("store did not open: {error}"))?;
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .map_err(|error| format!("engine did not open: {error}"))?;

    // 2. Is the branch ref pointing at a commit whose record exists?
    let head = scalar_text(&lix, "SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .map_err(|error| format!("active branch ref unreadable: {error}"))?;
    let head_records = scalar_i64(
        &lix,
        "SELECT COUNT(*) AS n FROM lix_commit WHERE id = $1",
        &[Value::Text(head.clone())],
    )
    .await
    .map_err(|error| format!("commit record lookup failed for head {head}: {error}"))?;
    if head_records != 1 {
        return Err(format!(
            "branch ref points at commit {head}, but lix_commit holds {head_records} record(s)"
        ));
    }
    let branch_id = lix
        .active_branch_id()
        .await
        .map_err(|error| format!("active branch id unreadable: {error}"))?;
    let branch_ref = scalar_text(
        &lix,
        "SELECT commit_id FROM lix_branch WHERE id = $1",
        &[Value::Text(branch_id)],
    )
    .await
    .map_err(|error| format!("branch table unreadable: {error}"))?;
    if branch_ref != head {
        return Err(format!(
            "branch ref disagrees with active head: lix_branch={branch_ref} head={head}"
        ));
    }

    // 3./4. Does a plain SELECT return one whole committed write set?
    //
    // A kill that landed before the seed commit published leaves the table
    // absent or empty. Both are legal *pre-publication* states; what would not
    // be legal is a partially applied seed, so the row count is still pinned to
    // 0 or `rows` and the generations must still be uniform.
    let observed = match read_generations(&lix).await {
        Ok(observed) => observed,
        Err(ReadFailure::TableAbsent) => {
            if setup_complete {
                // Schema registration and the seed commit both returned `Ok`
                // before `setup.done` was fsynced, so the table's absence means
                // acknowledged commits are gone. The store is not damaged — it
                // is empty, which is a legal state a fresh store also has — so
                // this is a durability outcome, not a consistency one.
                lost_acknowledgement = Some(
                    "acknowledged commit lost: schema registration and the seed commit both \
                     returned Ok (setup.done is fsynced) but the table is gone"
                        .to_owned(),
                );
            }
            Vec::new()
        }
        Err(ReadFailure::Other(error)) => {
            return Err(format!("plain SELECT failed after crash: {error}"));
        }
    };
    if !observed.is_empty() && observed.len() != rows {
        return Err(format!(
            "TORN WRITE SET: {} rows visible, expected 0 or {rows} — a publication was observed half-applied",
            observed.len()
        ));
    }
    let generation = if observed.is_empty() {
        if setup_complete && lost_acknowledgement.is_none() {
            // `setup.done` is fsynced only after the seed commit returned `Ok`,
            // so an empty store here means an acknowledged commit is gone. The
            // store is still whole — it is simply older than the writer was
            // told — so this is a durability observation, not a torn write.
            lost_acknowledgement = Some(
                "acknowledged commit lost: the seed commit returned Ok (setup.done is fsynced) \
                 but the store has no rows"
                    .to_owned(),
            );
        }
        None
    } else {
        Some(single_generation(&observed)?)
    };

    // Durability: a commit whose `commit()` returned Ok must still be there.
    if let Some(acked) = acked {
        match generation {
            None => {
                lost_acknowledgement = Some(format!(
                    "acknowledged commit lost: ack log reached generation {acked}, store is empty"
                ));
            }
            Some(visible) if visible < acked => {
                lost_acknowledgement = Some(format!(
                    "acknowledged commit lost: ack log reached generation {acked}, store shows {visible}"
                ));
            }
            Some(_) => {}
        }
    }
    if let Some(visible) = generation
        && visible > attempted
    {
        return Err(format!(
            "store shows generation {visible}, which the writer never attempted (max {attempted})"
        ));
    }

    // Derived views must not be stale-but-authoritative: the history view is
    // rebuilt from canonical records and must agree with the serving read.
    if let Some(visible) = generation
        && visible > 0
    {
        let history_rows = scalar_i64(
            &lix,
            &format!("SELECT COUNT(*) AS n FROM {SCHEMA_KEY}_history() WHERE generation = $1"),
            &[Value::Integer(visible)],
        )
        .await
        .map_err(|error| format!("history view unreadable after crash: {error}"))?;
        if history_rows == 0 {
            return Err(format!(
                "serving read reports generation {visible}, but the canonical history view has no such rows"
            ));
        }
    }

    // The binary file is published through the content-addressed path in the
    // same transaction as the rows. Its bytes must belong to the same
    // generation the rows report, or the write set reached the store in pieces.
    if workload.blob_bytes > 0
        && let Some(visible) = generation
    {
        let content = read_blob(&lix)
            .await
            .map_err(|error| format!("binary file unreadable after crash: {error}"))?;
        match content {
            Some(bytes) if bytes == blob_for(visible, workload.blob_bytes) => {}
            Some(bytes) => {
                let matching = (0..=attempted)
                    .chain(std::iter::once(visible))
                    .find(|candidate| bytes == blob_for(*candidate, workload.blob_bytes));
                return Err(format!(
                    "TORN WRITE SET: rows report generation {visible} but the CAS-published file \
                     holds generation {matching:?} ({} bytes)",
                    bytes.len()
                ));
            }
            None => {
                return Err(format!(
                    "TORN WRITE SET: rows report generation {visible} but the CAS-published file is absent"
                ));
            }
        }
    }

    // 5. Does the next commit after recovery succeed?
    if observed.is_empty() {
        // The crash landed before the schema or the seed published; recovering
        // means the store accepts the setup it never finished.
        if matches!(read_generations(&lix).await, Err(ReadFailure::TableAbsent)) {
            register_schema(&lix).await;
        }
    }
    let recovery_generation = generation.unwrap_or(0) + RECOVERY_MARK;
    upsert_generation(&lix, workload, recovery_generation, observed.is_empty())
        .await
        .map_err(|error| format!("first commit after recovery failed: {error}"))?;
    lix.close()
        .await
        .map_err(|error| format!("close after recovery failed: {error}"))?;
    drop(lix);
    drop(storage);

    // And the recovered store must stay recovered across another clean reopen.
    let storage = B::open_at(&path).map_err(|error| format!("second open failed: {error}"))?;
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .map_err(|error| format!("second engine open failed: {error}"))?;
    let observed = read_generations(&lix)
        .await
        .map_err(|error| format!("SELECT after recovery reopen failed: {error:?}"))?;
    if observed.len() != rows {
        return Err(format!(
            "post-recovery commit left {} rows, expected {rows}",
            observed.len()
        ));
    }
    let confirmed = single_generation(&observed)?;
    if confirmed != recovery_generation {
        return Err(format!(
            "post-recovery commit did not survive reopen: expected {recovery_generation}, saw {confirmed}"
        ));
    }
    lix.close()
        .await
        .map_err(|error| format!("final close failed: {error}"))?;

    Ok(Recovered {
        generation,
        lost_acknowledgement,
    })
}

fn single_generation(rows: &[i64]) -> Result<i64, String> {
    let first = rows[0];
    if rows.iter().any(|value| *value != first) {
        let mut distinct: Vec<i64> = rows.to_vec();
        distinct.sort_unstable();
        distinct.dedup();
        return Err(format!(
            "TORN WRITE SET: rows carry mixed generations {distinct:?} — a publication was observed half-applied"
        ));
    }
    Ok(first)
}

// ---------------------------------------------------------------------------
// SQL helpers
// ---------------------------------------------------------------------------

async fn register_schema<S: Storage + Clone + Send + Sync + 'static>(lix: &Lix<S>) {
    let schema = serde_json::json!({
        "x-lix-key": SCHEMA_KEY,
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "generation", "payload"],
        "properties": {
            "id": { "type": "string" },
            "generation": { "type": "integer" },
            "payload": { "type": "string" }
        },
        "additionalProperties": false
    });
    lix.execute(
        "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
        &[Value::Text(schema.to_string())],
    )
    .await
    .expect("register crash-consistency schema");
}

/// One publication that rewrites *every* row to the same generation.
///
/// This is the oracle: any kill that leaves two rows disagreeing proves the
/// write set reached the store in stages.
async fn write_generation<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
    workload: Workload,
    generation: i64,
) {
    upsert_generation(lix, workload, generation, generation == 0)
        .await
        .expect("crash-consistency generation commit");
}

/// One publication that rewrites every row *and* the binary file to the same
/// generation, so a torn write set is visible either as mixed row generations or
/// as a blob that disagrees with the rows.
async fn upsert_generation<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
    workload: Workload,
    generation: i64,
    insert: bool,
) -> Result<(), lix::LixError> {
    let rows = workload.rows;
    let mut transaction = lix.begin_transaction().await?;
    for index in 0..rows {
        let payload = format!("gen-{generation}-row-{index}");
        if insert {
            transaction
                .execute(
                    &format!(
                        "INSERT INTO {SCHEMA_KEY} (id, generation, payload) VALUES ($1, $2, $3)"
                    ),
                    &[
                        Value::Text(format!("row-{index}")),
                        Value::Integer(generation),
                        Value::Text(payload),
                    ],
                )
                .await?;
        } else {
            transaction
                .execute(
                    &format!(
                        "UPDATE {SCHEMA_KEY} SET generation = $1, payload = $2 WHERE id = $3"
                    ),
                    &[
                        Value::Integer(generation),
                        Value::Text(payload),
                        Value::Text(format!("row-{index}")),
                    ],
                )
                .await?;
        }
    }
    if workload.blob_bytes > 0 {
        let content = Value::Blob(blob_for(generation, workload.blob_bytes).into());
        if insert {
            transaction
                .execute(
                    "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                    &[Value::Text(BLOB_PATH.to_owned()), content],
                )
                .await?;
        } else {
            transaction
                .execute(
                    "UPDATE lix_file SET content = $1 WHERE path = $2",
                    &[content, Value::Text(BLOB_PATH.to_owned())],
                )
                .await?;
        }
    }
    transaction.commit().await?;
    Ok(())
}

/// Why a post-crash read failed. A missing table is a legal pre-publication
/// state; anything else is a defect.
#[derive(Debug)]
enum ReadFailure {
    TableAbsent,
    Other(String),
}

async fn read_generations<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
) -> Result<Vec<i64>, ReadFailure> {
    let result = lix
        .execute(
            &format!("SELECT id, generation FROM {SCHEMA_KEY} ORDER BY id"),
            &[],
        )
        .await
        .map_err(|error| {
            let text = error.to_string();
            if text.contains("LIX_TABLE_NOT_FOUND") {
                ReadFailure::TableAbsent
            } else {
                ReadFailure::Other(text)
            }
        })?;
    Ok(result
        .rows()
        .iter()
        .map(|row| row.get::<i64>("generation").expect("generation is an integer"))
        .collect())
}

async fn read_blob<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
) -> Result<Option<Vec<u8>>, String> {
    let result = lix
        .execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text(BLOB_PATH.to_owned())],
        )
        .await
        .map_err(|error| error.to_string())?;
    let rows = result.rows();
    if rows.is_empty() {
        return Ok(None);
    }
    rows[0]
        .get::<Vec<u8>>("content")
        .map(Some)
        .map_err(|error| format!("content was not a blob: {error}"))
}

async fn scalar_text<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
    sql: &str,
    params: &[Value],
) -> Result<String, String> {
    let result = lix
        .execute(sql, params)
        .await
        .map_err(|error| error.to_string())?;
    let rows = result.rows();
    if rows.is_empty() {
        return Err("query returned no rows".to_owned());
    }
    rows[0]
        .get::<String>(result_column(sql))
        .map_err(|error| format!("column was not text: {error}"))
}

async fn scalar_i64<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
    sql: &str,
    params: &[Value],
) -> Result<i64, String> {
    let result = lix
        .execute(sql, params)
        .await
        .map_err(|error| error.to_string())?;
    let rows = result.rows();
    if rows.is_empty() {
        return Err("query returned no rows".to_owned());
    }
    rows[0]
        .get::<i64>(result_column(sql))
        .map_err(|error| format!("column was not an integer: {error}"))
}

fn result_column(sql: &str) -> &'static str {
    if sql.contains(" AS commit_id") {
        "commit_id"
    } else if sql.contains("SELECT commit_id") {
        "commit_id"
    } else {
        "n"
    }
}

// ---------------------------------------------------------------------------
// Parent process: the sweep
// ---------------------------------------------------------------------------

/// Sweep every kill point in the publication window of one shipping adapter.
///
/// Identical code, identical invariants and an identical null control for every
/// backend: the only thing that varies is which adapter the engine was handed.
/// That is what makes the two arms comparable — a second harness would only
/// prove that two harnesses agree.
fn sweep<B: CrashBackend>(workload: Workload) {
    let root = tempfile::tempdir().expect("create crash-consistency sweep root");
    let mut failures: Vec<String> = Vec::new();
    let mut lost_acks: Vec<String> = Vec::new();
    let mut recovered_generations: Vec<Option<i64>> = Vec::new();
    let mut trials = 0usize;
    let mut killed_trials = 0usize;

    // Calibrate: one unkilled run establishes how many storage events the
    // measured window contains, and how long it takes.
    let started = Instant::now();
    let calibration = run_child::<B>(
        &root.path().join("calibration"),
        workload,
        KillPlan::None,
        true,
    );
    let window_duration = started.elapsed();
    assert!(
        calibration.status_success,
        "calibration child failed: {}",
        calibration.output
    );
    let events = calibration
        .events
        .expect("calibration child must report its storage event count");
    assert!(events > 0, "publication window contained no storage events");
    let labels = calibration.labels.clone();

    // Null control: the same invariant battery against a store that was closed
    // cleanly and never killed. If this does not pass, nothing the sweep reports
    // afterwards is attributable to the crash.
    let control_database = root.path().join("calibration").join("database");
    let control_acked = calibration.acked;
    let control = block_on(move || {
        verify_after_crash::<B>(
            control_database,
            control_acked,
            workload.commits as i64,
            workload,
            true,
        )
    });
    let control = control.unwrap_or_else(|error| {
        panic!(
            "clean-close control failed before any kill was issued — the harness, not the crash, \
             is the problem: {error}"
        )
    });
    assert_eq!(
        control.generation,
        Some(workload.commits as i64),
        "clean-close control did not reach the last attempted generation"
    );
    assert!(
        control.lost_acknowledgement.is_none(),
        "clean-close control lost an acknowledged commit without any kill — the harness, not the \
         crash, is the problem: {:?}",
        control.lost_acknowledgement
    );

    // How many of the window's write transactions actually asked for a durable
    // acknowledgement, read off the storage boundary rather than assumed. This
    // is what decides whether "acknowledged commit lost" below is a contract
    // violation or a documented property of a non-durable write.
    let (durable_writes, total_writes) = calibration.durability.unwrap_or((0, 0));
    let durability_requested = durable_writes > 0;
    let _ = fs::remove_dir_all(root.path().join("calibration"));

    let max_points = env_usize("LIX_CRASH_CONSISTENCY_MAX_POINTS", 512);
    let swept: Vec<u64> = if events as usize <= max_points {
        (1..=events).collect()
    } else {
        // Even coverage of the window when it is larger than the cap.
        (0..max_points)
            .map(|i| 1 + (i as u64 * (events - 1)) / (max_points as u64 - 1))
            .collect()
    };

    for kill_at in &swept {
        let dir = root.path().join(format!("k{kill_at}"));
        let child = run_child::<B>(&dir, workload, KillPlan::AtEvent(*kill_at), false);
        trials += 1;
        if child.killed_by_signal {
            killed_trials += 1;
        }
        let label = labels
            .get((*kill_at as usize).saturating_sub(1))
            .cloned()
            .unwrap_or_else(|| "?".to_owned());
        let database = dir.join("database");
        let acked = child.acked;
        let setup_complete = child.setup_complete;
        let outcome = block_on(move || {
            verify_after_crash::<B>(
                database,
                acked,
                workload.commits as i64,
                workload,
                setup_complete,
            )
        });
        match &outcome {
            Ok(recovered) => {
                recovered_generations.push(recovered.generation);
                if let Some(loss) = &recovered.lost_acknowledgement {
                    lost_acks.push(format!("kill_at={kill_at} ({label}): {loss}"));
                }
            }
            Err(error) => failures.push(format!(
                "kill_at={kill_at} ({label}, killed={}): {error}",
                child.killed_by_signal
            )),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    // Timed phase: land the kill inside the backend's own write path.
    let timed_trials = env_usize(
        "LIX_CRASH_CONSISTENCY_TIMED_TRIALS",
        if env_flag("LIX_CRASH_CONSISTENCY_DEEP") {
            200
        } else {
            12
        },
    );
    let window_nanos = window_duration.as_nanos().max(1) as u64;
    // Setup is not part of the measured window, so aim the timed kills at what
    // follows it. A 10% undershoot deliberately keeps a few kills in late setup:
    // a crash during repository initialization must also leave a usable store.
    let setup_nanos = calibration.setup_nanos.unwrap_or(0);
    let aim_from = setup_nanos - setup_nanos / 10;
    let aim_span = window_nanos.saturating_sub(aim_from).max(1);
    let mut rng = SplitMix64::new(env_u64("LIX_CRASH_CONSISTENCY_SEED", 0x5eed_c0de_1234_5678));
    for trial in 0..timed_trials {
        let delay = aim_from + rng.next() % aim_span;
        let dir = root.path().join(format!("t{trial}"));
        let child = run_child::<B>(&dir, workload, KillPlan::AfterNanos(delay), false);
        trials += 1;
        if child.killed_by_signal {
            killed_trials += 1;
        }
        let database = dir.join("database");
        let acked = child.acked;
        let setup_complete = child.setup_complete;
        let outcome = block_on(move || {
            verify_after_crash::<B>(
                database,
                acked,
                workload.commits as i64,
                workload,
                setup_complete,
            )
        });
        match &outcome {
            Ok(recovered) => {
                recovered_generations.push(recovered.generation);
                if let Some(loss) = &recovered.lost_acknowledgement {
                    lost_acks.push(format!("timed delay={delay}ns: {loss}"));
                }
            }
            Err(error) => failures.push(format!(
                "timed delay={delay}ns (killed={}): {error}",
                child.killed_by_signal
            )),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    let mut distinct: Vec<Option<i64>> = recovered_generations.clone();
    distinct.sort_unstable();
    distinct.dedup();
    println!(
        "crash-consistency sweep [{backend}]: {trials} trials ({} storage-event points over a \
         {events}-event publication window, {timed_trials} seeded wall-clock kills spanning \
         {aim_from}..{}ns), {killed_trials} confirmed SIGKILLed, {} inconsistencies, \
         {} lost acknowledgements ({durable_writes}/{total_writes} window write transactions \
         requested await_durable); recovered generations observed: {distinct:?}",
        swept.len(),
        aim_from + aim_span,
        failures.len(),
        lost_acks.len(),
        backend = B::NAME,
    );

    assert!(
        failures.is_empty(),
        "crash consistency violated on {} in {} of {trials} trials:\n  - {}",
        B::NAME,
        failures.len(),
        failures.join("\n  - ")
    );
    // Losing an acknowledged commit is only a contract violation when the
    // acknowledgement was asked to be durable. `WriteOptions::await_durable` is
    // opt-in and `Storage::begin_write` explicitly documents that "a storage may
    // publish a commit before its background durability boundary", so a
    // non-durable write that vanishes with the process is within contract — it
    // is reported above and analysed in the qualification write-up, not failed.
    if durability_requested {
        assert!(
            lost_acks.is_empty(),
            "{} lost {} acknowledged commits out of {trials} trials even though \
             {durable_writes} of {total_writes} write transactions in the window set \
             await_durable:\n  - {}",
            B::NAME,
            lost_acks.len(),
            lost_acks.join("\n  - ")
        );
    }
    // A sweep in which nothing ever died would be vacuously green.
    assert!(
        killed_trials * 2 >= swept.len(),
        "only {killed_trials} of {trials} trials actually died; the harness is not exercising crashes"
    );
    // Coverage has to be demonstrated, not asserted. A sweep whose kills all
    // landed after the last commit had already published would report exactly
    // one recovered generation and pass every invariant vacuously.
    assert!(
        distinct.len() >= 2,
        "the sweep only ever recovered {distinct:?}; every kill landed in the same state, so the \
         window was not actually swept"
    );
}

#[test]
fn rocksdb_publication_window_survives_sigkill_at_every_storage_event() {
    sweep::<RocksDB>(Workload::from_env());
}

/// The same sweep, the same invariants and the same null control against the
/// other shipping adapter.
///
/// Scope note, stated rather than implied: this runs SlateDB over a
/// **local-filesystem** object store (`SlateDB::open`, which is
/// `LocalFileSystem` under the hood — `slatedb.rs:2366-2402`). For *performance*
/// that configuration is misleading and the round's rules forbid quoting it as
/// "the SlateDB result". For *crash consistency* it is the legitimate
/// configuration and the only one this qualification can express: the kill has
/// to interrupt a real writer process with real bytes crossing a real process
/// boundary, and the object-store simulation used by the benches
/// (`ThrottledStore` over `InMemory`) keeps every byte inside the dying
/// process's heap, so "killed mid-upload" is not even representable there.
#[cfg(feature = "slatedb")]
#[test]
fn slatedb_publication_window_survives_sigkill_at_every_storage_event() {
    sweep::<SlateDB>(Workload::from_env());
}

/// The durable half of the SlateDB arm, and the reason the sweep above reports
/// lost acknowledgements instead of failing on them.
///
/// Identical backend, identical workload, identical kill schedule; the only
/// difference is that every write transaction sets
/// `WriteOptions::await_durable`. The sweep above measures what a SQL commit
/// gets today (acknowledged off an in-process queue, so SIGKILL takes it back);
/// this one measures what the flag buys (acknowledged only after the WAL SST is
/// in the object store, so SIGKILL cannot). Keeping both in the default run
/// means the durable contract is a standing assertion rather than a one-off
/// measurement in a PR body.
#[cfg(feature = "slatedb")]
#[test]
fn slatedb_durable_acknowledgement_survives_sigkill() {
    let mut workload = Workload::from_env();
    workload.force_await_durable = true;
    sweep::<SlateDB>(workload);
}

/// The RocksDB adapter accepts [`WriteOptions::await_durable`] and never acts on
/// it. The option is documented as "do not acknowledge the commit until the
/// backend has crossed its durable persistence boundary", the engine sets it for
/// atomic CAS publications, and the SlateDB adapter honours it.
///
/// This test pins the gap rather than hiding it: it is the reason the sweep
/// above proves process-crash consistency and *not* power-loss durability.
#[test]
fn rocksdb_ignores_await_durable_and_therefore_proves_only_process_crash_consistency() {
    let source = include_str!("../../rocksdb-storage/src/rocksdb.rs");
    assert!(
        !source.contains("await_durable"),
        "the RocksDB adapter now reads await_durable — update this qualification's scope note"
    );
    assert!(
        !source.contains("set_sync(true)") && !source.contains("flush_wal(true)\n"),
        "the RocksDB adapter now syncs on commit — update this qualification's scope note"
    );
}

/// The SlateDB adapter's acknowledgement is the mirror image of RocksDB's, and
/// the sweep above is what turns that from a code reading into a measurement.
///
/// `SlateDBWrite::commit` publishes the write set onto an in-process pipeline
/// and returns; the bytes only reach the object store when the background
/// drainer runs. A commit that did **not** set `await_durable` is therefore
/// acknowledged while its write set is still in the dying process's heap. A
/// commit that **did** set it parks on `completion.wait()`, which resolves only
/// after `drain_write_queue` has run `db.write_with_options` *and* `db.flush()`,
/// i.e. after the WAL SST is in the object store.
///
/// This test pins both halves so the scope note cannot go stale silently.
#[cfg(feature = "slatedb")]
#[test]
fn slatedb_honours_await_durable_and_acknowledges_non_durable_writes_early() {
    let source = include_str!("../../slatedb-storage/src/slatedb.rs");
    assert!(
        source.contains("if await_durable && !apply_backpressure {"),
        "the SlateDB adapter no longer waits for write completion on await_durable — \
         re-measure this qualification's durability arm"
    );
    assert!(
        source.contains("let await_durable = writes.iter().any(|write| write.await_durable);")
            && source.contains("db.flush().await?"),
        "the SlateDB adapter's write drainer no longer flushes the WAL for a durable write — \
         re-measure this qualification's durability arm"
    );
    assert!(
        source.contains("await_durable: false,"),
        "the SlateDB adapter no longer enqueues batches with SlateDB's own await_durable off — \
         re-measure this qualification's durability arm"
    );
}

// ---------------------------------------------------------------------------
// Child process plumbing
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum KillPlan {
    None,
    AtEvent(u64),
    AfterNanos(u64),
}

struct ChildRun {
    status_success: bool,
    killed_by_signal: bool,
    acked: Option<i64>,
    events: Option<u64>,
    setup_nanos: Option<u64>,
    setup_complete: bool,
    labels: Vec<String>,
    /// `(write transactions that set await_durable, write transactions)` in the
    /// measured window. Only a child that ran to completion reports it.
    durability: Option<(u64, u64)>,
    output: String,
}

fn run_child<B: CrashBackend>(
    dir: &Path,
    workload: Workload,
    plan: KillPlan,
    trace: bool,
) -> ChildRun {
    fs::create_dir_all(dir).expect("create crash-consistency trial directory");
    let ack = dir.join("ack.log");
    let exe = std::env::current_exe().expect("locate crash-consistency test binary");

    let mut command = Command::new(exe);
    command
        .args([
            "--exact",
            CHILD_TEST,
            "--nocapture",
            "--test-threads=1",
            "--quiet",
        ])
        .env(CHILD_ENV, "1")
        .env(BACKEND_ENV, B::NAME)
        .env("LIX_CRASH_DB", dir.join("database"))
        .env("LIX_CRASH_ACK", &ack)
        .env(
            "LIX_CRASH_CONSISTENCY_ROWS",
            workload.rows.to_string(),
        )
        .env(
            "LIX_CRASH_CONSISTENCY_COMMITS",
            workload.commits.to_string(),
        )
        .env(
            "LIX_CRASH_CONSISTENCY_BLOB_BYTES",
            workload.blob_bytes.to_string(),
        )
        .env(
            "LIX_CRASH_AWAIT_DURABLE",
            if workload.force_await_durable { "1" } else { "0" },
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if trace {
        command.env("LIX_CRASH_TRACE", "1");
    }
    match plan {
        KillPlan::None => {}
        KillPlan::AtEvent(k) => {
            command.env("LIX_CRASH_KILL_AT", k.to_string());
        }
        KillPlan::AfterNanos(n) => {
            command.env("LIX_CRASH_KILL_AFTER_NANOS", n.to_string());
        }
    }

    let output = command.output().expect("run crash-consistency child");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

    let events = stdout
        .lines()
        .find_map(|line| line.strip_prefix("E20_EVENTS "))
        .and_then(|value| value.trim().parse().ok());
    let labels = stdout
        .lines()
        .filter_map(|line| line.strip_prefix("E20_EVENT "))
        .filter_map(|rest| rest.split_once(' ').map(|(_, label)| label.to_owned()))
        .collect();

    let setup_nanos = stdout
        .lines()
        .find_map(|line| line.strip_prefix("E20_SETUP_NANOS "))
        .and_then(|value| value.trim().parse().ok());

    let durability = stdout
        .lines()
        .find_map(|line| line.strip_prefix("E36_DURABLE_WRITES "))
        .and_then(|rest| rest.trim().split_once(' '))
        .and_then(|(durable, total)| Some((durable.parse().ok()?, total.parse().ok()?)));

    ChildRun {
        status_success: output.status.success(),
        killed_by_signal: signal_of(&output.status) == Some(libc::SIGKILL),
        acked: read_ack(&ack),
        events,
        setup_nanos,
        setup_complete: dir.join("setup.done").exists(),
        labels,
        durability,
        output: format!("stdout:\n{stdout}\nstderr:\n{stderr}"),
    }
}

fn signal_of(status: &std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

/// Highest generation whose `commit()` returned Ok, reading only whole lines —
/// a partially written final line is by definition not acknowledged.
fn read_ack(path: &Path) -> Option<i64> {
    let contents = fs::read_to_string(path).ok()?;
    contents
        .lines()
        .filter(|line| contents.ends_with('\n') || Some(*line) != contents.lines().last())
        .filter_map(|line| line.trim().parse::<i64>().ok())
        .max()
}

// ---------------------------------------------------------------------------
// Runtime plumbing
// ---------------------------------------------------------------------------

fn run_on_large_stack<F, Fut>(make_future: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name("crash-consistency".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build crash-consistency runtime")
                .block_on(make_future());
        })
        .expect("spawn crash-consistency thread")
        .join()
        .expect("crash-consistency thread should not panic");
}

fn block_on<T, F, Fut>(make_future: F) -> T
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T>,
    T: Send + 'static,
{
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("crash-consistency-verify".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            let value = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build crash-consistency verify runtime")
                .block_on(make_future());
            let _ = sender.send(value);
        })
        .expect("spawn crash-consistency verify thread")
        .join()
        .expect("crash-consistency verify thread should not panic");
    receiver.recv().expect("verify thread returned a value")
}

/// Deterministic, seedable, dependency-free.
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}
