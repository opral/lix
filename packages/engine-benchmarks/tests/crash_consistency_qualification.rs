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
//! [`CrashStorage`] wraps [`RocksDB`] and counts every mutation the engine
//! issues at the storage boundary — `begin_write`, each `put_many`,
//! `delete_many` and `delete_range`, the moment before `commit()` is delegated,
//! and the moment after it returns. A child process is launched once per kill
//! point with `LIX_CRASH_KILL_AT=k`; the wrapper raises `SIGKILL` on itself when
//! the counter reaches `k`, so the process dies *inside* the publication with no
//! unwinding, no destructor, and no flush. The sweep is exhaustive over `k`, and
//! the event index is deterministic for a fixed workload, so a failing point
//! reproduces by rerunning the same `k`.
//!
//! A second phase kills on a seeded wall-clock delay instead. That is the only
//! way to land inside RocksDB's own write path (WAL append, memtable insert),
//! which the storage-boundary sweep steps over atomically.
//!
//! ## What SIGKILL can and cannot prove
//!
//! SIGKILL models *process* death: the kernel keeps the page cache, so anything
//! the process had already handed to `write(2)` survives. It therefore proves
//! that the engine does not publish in observable stages. It does **not** model
//! power loss, which additionally requires the backend to have fsynced. See
//! `rocksdb_ignores_await_durable_write_option` in this file for the separate,
//! statically-decidable half of that question.
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
use lix_storage_rocksdb::{RocksDB, RocksDBRead, RocksDBWrite};

const SCHEMA_KEY: &str = "crash_consistency_row";
const CHILD_ENV: &str = "LIX_CRASH_CONSISTENCY_CHILD";
const CHILD_TEST: &str = "crash_consistency_child_worker";

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
}

impl Killer {
    fn new(kill_at: u64, trace: bool) -> Self {
        Self {
            counter: AtomicU64::new(0),
            armed: AtomicBool::new(false),
            kill_at,
            trace,
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn events(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
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
            // Drop, no RocksDB shutdown hook, no WAL flush.
            unsafe {
                libc::kill(libc::getpid(), libc::SIGKILL);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Storage wrapper
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct CrashStorage {
    inner: RocksDB,
    killer: Arc<Killer>,
}

impl CrashStorage {
    fn open(path: &Path, killer: Arc<Killer>) -> Self {
        Self {
            inner: RocksDB::open(path).expect("open crash-consistency RocksDB fixture"),
            killer,
        }
    }
}

impl Storage for CrashStorage {
    type Read<'a>
        = RocksDBRead<'a>
    where
        Self: 'a;

    type Write<'a>
        = CrashWrite
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
        async move {
            killer.tick("begin_write");
            let inner = self.inner.begin_write(opts).await?;
            Ok(CrashWrite { inner, killer })
        }
    }
}

struct CrashWrite {
    inner: RocksDBWrite,
    killer: Arc<Killer>,
}

impl StorageWrite for CrashWrite {
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

// The read side is pass-through: `RocksDBRead` already satisfies `StorageRead`,
// and a read cannot publish, so reads are not kill points.

// ---------------------------------------------------------------------------
// Workload shape
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Workload {
    rows: usize,
    commits: u64,
}

impl Workload {
    fn from_env() -> Self {
        let deep = env_flag("LIX_CRASH_CONSISTENCY_DEEP");
        Self {
            rows: env_usize("LIX_CRASH_CONSISTENCY_ROWS", if deep { 64 } else { 8 }),
            commits: env_u64("LIX_CRASH_CONSISTENCY_COMMITS", if deep { 12 } else { 3 }),
        }
    }
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

    run_on_large_stack(move || child_main(db, ack, killer, workload));
}

async fn child_main(db: PathBuf, ack: PathBuf, killer: Arc<Killer>, workload: Workload) {
    let storage = CrashStorage::open(&db, killer.clone());
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("child opens crash-consistency Lix");

    // --- setup, deliberately not armed -------------------------------------
    register_schema(&lix).await;
    write_generation(&lix, workload.rows, 0).await;

    // --- measured publication window ---------------------------------------
    killer.arm();
    for generation in 1..=workload.commits {
        write_generation(&lix, workload.rows, generation as i64).await;
        // Acknowledge only after `commit()` returned Ok. Whatever is in this
        // file must be visible after the crash; that is the durability half of
        // the invariant set.
        append_ack(&ack, generation);
    }

    println!("E20_EVENTS {}", killer.events());
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
    generation: i64,
}

/// Everything the brief asks of a store that has just survived a crash.
async fn verify_after_crash(
    path: &Path,
    acked: Option<i64>,
    attempted: i64,
    rows: usize,
) -> Result<Recovered, String> {
    // 1. Does the store open at all?
    let storage = RocksDB::open(path).map_err(|error| format!("store did not open: {error}"))?;
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
    let branch_ref = scalar_text(
        &lix,
        "SELECT commit_id FROM lix_branch WHERE id = $1",
        &[Value::Text(lix.active_branch_id().to_string())],
    )
    .await
    .map_err(|error| format!("branch table unreadable: {error}"))?;
    if branch_ref != head {
        return Err(format!(
            "branch ref disagrees with active head: lix_branch={branch_ref} head={head}"
        ));
    }

    // 3./4. Does a plain SELECT return one whole committed write set?
    let observed = read_generations(&lix, rows)
        .await
        .map_err(|error| format!("plain SELECT failed after crash: {error}"))?;
    let generation = single_generation(&observed)?;

    // Durability: a commit whose `commit()` returned Ok must still be there.
    if let Some(acked) = acked
        && generation < acked
    {
        return Err(format!(
            "acknowledged commit lost: ack log reached generation {acked}, store shows {generation}"
        ));
    }
    if generation > attempted {
        return Err(format!(
            "store shows generation {generation}, which the writer never attempted (max {attempted})"
        ));
    }

    // Derived views must not be stale-but-authoritative: the history view is
    // rebuilt from canonical records and must agree with the serving read.
    let history_rows = scalar_i64(
        &lix,
        &format!("SELECT COUNT(*) AS n FROM {SCHEMA_KEY}_history() WHERE generation = $1"),
        &[Value::Integer(generation)],
    )
    .await
    .map_err(|error| format!("history view unreadable after crash: {error}"))?;
    if history_rows == 0 && generation > 0 {
        return Err(format!(
            "serving read reports generation {generation}, but the canonical history view has no such rows"
        ));
    }

    // 5. Does the next commit after recovery succeed?
    let recovery_generation = generation + RECOVERY_MARK;
    write_generation_checked(&lix, rows, recovery_generation)
        .await
        .map_err(|error| format!("first commit after recovery failed: {error}"))?;
    lix.close()
        .await
        .map_err(|error| format!("close after recovery failed: {error}"))?;
    drop(lix);
    drop(storage);

    // And the recovered store must stay recovered across another clean reopen.
    let storage = RocksDB::open(path).map_err(|error| format!("second open failed: {error}"))?;
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .map_err(|error| format!("second engine open failed: {error}"))?;
    let observed = read_generations(&lix, rows)
        .await
        .map_err(|error| format!("SELECT after recovery reopen failed: {error}"))?;
    let confirmed = single_generation(&observed)?;
    if confirmed != recovery_generation {
        return Err(format!(
            "post-recovery commit did not survive reopen: expected {recovery_generation}, saw {confirmed}"
        ));
    }
    lix.close()
        .await
        .map_err(|error| format!("final close failed: {error}"))?;

    Ok(Recovered { generation })
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
    rows: usize,
    generation: i64,
) {
    write_generation_checked(lix, rows, generation)
        .await
        .expect("crash-consistency generation commit");
}

async fn write_generation_checked<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
    rows: usize,
    generation: i64,
) -> Result<(), lix::LixError> {
    let mut transaction = lix.begin_transaction().await?;
    for index in 0..rows {
        let payload = format!("gen-{generation}-row-{index}");
        if generation == 0 {
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
    transaction.commit().await?;
    Ok(())
}

async fn read_generations<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
    rows: usize,
) -> Result<Vec<i64>, String> {
    let result = lix
        .execute(
            &format!("SELECT id, generation FROM {SCHEMA_KEY} ORDER BY id"),
            &[],
        )
        .await
        .map_err(|error| error.to_string())?;
    let observed: Vec<i64> = result
        .rows()
        .iter()
        .map(|row| row.get::<i64>("generation").expect("generation is an integer"))
        .collect();
    if observed.len() != rows {
        return Err(format!(
            "row count is {} after recovery, expected {rows} — a publication was observed half-applied",
            observed.len()
        ));
    }
    Ok(observed)
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
        .ok_or_else(|| "column was not text".to_owned())
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
        .ok_or_else(|| "column was not an integer".to_owned())
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

#[test]
fn rocksdb_publication_window_survives_sigkill_at_every_storage_event() {
    let workload = Workload::from_env();
    let root = tempfile::tempdir().expect("create crash-consistency sweep root");
    let mut failures: Vec<String> = Vec::new();
    let mut trials = 0usize;
    let mut killed_trials = 0usize;

    // Calibrate: one unkilled run establishes how many storage events the
    // measured window contains, and how long it takes.
    let started = Instant::now();
    let calibration = run_child(
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
    let labels = calibration.labels;

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
        let child = run_child(&dir, workload, KillPlan::AtEvent(*kill_at), false);
        trials += 1;
        if child.killed_by_signal {
            killed_trials += 1;
        }
        let label = labels
            .get((*kill_at as usize).saturating_sub(1))
            .cloned()
            .unwrap_or_else(|| "?".to_owned());
        let outcome = block_on(verify_after_crash(
            &dir.join("database"),
            child.acked,
            workload.commits as i64,
            workload.rows,
        ));
        if let Err(error) = &outcome {
            failures.push(format!(
                "kill_at={kill_at} ({label}, killed={}): {error}",
                child.killed_by_signal
            ));
        }
        let _ = fs::remove_dir_all(&dir);
    }

    // Timed phase: land the kill inside RocksDB's own write path.
    let timed_trials = env_usize(
        "LIX_CRASH_CONSISTENCY_TIMED_TRIALS",
        if env_flag("LIX_CRASH_CONSISTENCY_DEEP") {
            200
        } else {
            12
        },
    );
    let window_nanos = window_duration.as_nanos().max(1) as u64;
    let mut rng = SplitMix64::new(env_u64("LIX_CRASH_CONSISTENCY_SEED", 0x5eed_c0de_1234_5678));
    for trial in 0..timed_trials {
        // Sweep the whole observed window, biased to the second half where the
        // measured publications live (setup occupies the first part).
        let delay = window_nanos / 4 + rng.next() % window_nanos;
        let dir = root.path().join(format!("t{trial}"));
        let child = run_child(&dir, workload, KillPlan::AfterNanos(delay), false);
        trials += 1;
        if child.killed_by_signal {
            killed_trials += 1;
        }
        let outcome = block_on(verify_after_crash(
            &dir.join("database"),
            child.acked,
            workload.commits as i64,
            workload.rows,
        ));
        if let Err(error) = &outcome {
            failures.push(format!(
                "timed delay={delay}ns (killed={}): {error}",
                child.killed_by_signal
            ));
        }
        let _ = fs::remove_dir_all(&dir);
    }

    println!(
        "crash-consistency sweep: {trials} trials ({} storage-event points over a {events}-event \
         publication window, {timed_trials} seeded wall-clock kills), {killed_trials} confirmed \
         SIGKILLed, {} inconsistencies",
        swept.len(),
        failures.len()
    );

    assert!(
        failures.is_empty(),
        "crash consistency violated in {} of {trials} trials:\n  - {}",
        failures.len(),
        failures.join("\n  - ")
    );
    // A sweep in which nothing ever died would be vacuously green.
    assert!(
        killed_trials * 2 >= swept.len(),
        "only {killed_trials} of {trials} trials actually died; the harness is not exercising crashes"
    );
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
    labels: Vec<String>,
    output: String,
}

fn run_child(dir: &Path, workload: Workload, plan: KillPlan, trace: bool) -> ChildRun {
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

    ChildRun {
        status_success: output.status.success(),
        killed_by_signal: signal_of(&output.status) == Some(libc::SIGKILL),
        acked: read_ack(&ack),
        events,
        labels,
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

fn block_on<T, Fut>(future: Fut) -> T
where
    Fut: Future<Output = T> + 'static,
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
                .block_on(future);
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
