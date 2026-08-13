//! Tombstone accumulation in the hot row serving view.
//!
//! Not a product module. `ROW_SPACE` is a derived, generation-keyed serving
//! view. A delete publishes a *tombstone* row there rather than removing the
//! key, because the hot row plane is a sparse overlay over packed/root
//! current-state bases and the tombstone is what shadows the base row.
//!
//! The question these probes answer is what happens to those tombstones over a
//! branch's life: how many accumulate per delete, what they cost a collection
//! scan that returns an unchanged answer, and whether anything in the engine
//! (ordinary commits, checkpoints, generation retirement, repository GC) ever
//! removes them.

use serde_json::json;
use std::time::{Duration, Instant};

use crate::engine::Engine;
use crate::session::SessionContext;
use crate::storage::ProjectedValue;
use crate::storage_adapter::{
    Memory, SharedStorageAdapterRead, StorageAdapter, StorageAdapterRead,
    StorageBeginScanOptions, StorageKey, StoragePrefix, StorageReadOptions, StorageSpace,
    StorageWriteOptions,
};

fn sizes_from_env(var: &str, default: &[usize]) -> Vec<usize> {
    match std::env::var(var) {
        Ok(raw) => raw
            .split(',')
            .filter(|part| !part.trim().is_empty())
            .map(|part| part.trim().parse::<usize>().expect("size must parse"))
            .collect(),
        Err(_) => default.to_vec(),
    }
}

fn reps_from_env(default: usize) -> usize {
    std::env::var("LIX_TOMBSTONE_REPS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(default)
}

async fn open_session() -> (Memory, SessionContext<Memory>) {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("engine should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("engine should open");
    let session = engine
        .open_session()
        .await
        .expect("session should open");
    (storage, session)
}

/// Census of the hot row serving view.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RowCensus {
    /// Every key in `ROW_SPACE`, across every branch and generation.
    entries: usize,
    /// Entries whose fixed header carries the deleted flag.
    tombstones: usize,
    /// Records in the packed current-state base plane.
    packed_bases: usize,
    /// Records in the sparse-generation root base plane.
    root_bases: usize,
    /// Records in the sparse per-row working-diff index.
    diff_records: usize,
}

impl RowCensus {
    fn live(self) -> usize {
        self.entries - self.tombstones
    }
}

/// `HEAD_VALUE_DELETED` is bit 0 of the flags byte at offset 1 of the fixed
/// row header (`hot_state/tracked_head.rs`). Reading the byte directly keeps
/// this probe out of the private value codec.
const HEAD_VALUE_DELETED_BIT: u8 = 0b0000_0001;

async fn space_values(
    read: &(impl StorageAdapterRead + ?Sized),
    space: StorageSpace,
) -> Vec<bytes::Bytes> {
    let range = StoragePrefix {
        bytes: bytes::Bytes::new(),
    }
    .to_range()
    .expect("valid empty prefix");
    let mut cursor = read
        .begin_scan(space, range, StorageBeginScanOptions::default())
        .await
        .expect("scan the serving space");
    cursor
        .collect_all()
        .await
        .expect("collect serving entries")
        .into_iter()
        .map(|entry| match entry.value {
            ProjectedValue::FullValue(bytes) => bytes,
            ProjectedValue::KeyOnly => bytes::Bytes::new(),
        })
        .collect()
}

async fn row_census(storage: &Memory) -> RowCensus {
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("read the hot row plane");
    let rows = space_values(&read, crate::hot_state::ROW_SPACE).await;
    let tombstones = rows
        .iter()
        .filter(|value| value.len() > 1 && value[1] & HEAD_VALUE_DELETED_BIT != 0)
        .count();
    let packed_bases = space_values(&read, crate::hot_state::PACKED_CURRENT_BASE_SPACE)
        .await
        .len();
    let root_bases = space_values(&read, crate::hot_state::ROOT_CURRENT_BASE_SPACE)
        .await
        .len();
    let diff_records = space_values(&read, crate::hot_state::DIFF_SPACE).await.len();
    RowCensus {
        entries: rows.len(),
        tombstones,
        packed_bases,
        root_bases,
        diff_records,
    }
}

/// Reopens the engine on the same physical store so every in-process cache is
/// cold. Without this, a probe that mutates storage behind the engine's back
/// measures the cache rather than the store, and a resurrection assertion would
/// pass for the wrong reason.
async fn reopen_session(storage: &Memory) -> SessionContext<Memory> {
    let engine = Engine::new(storage.clone())
        .await
        .expect("engine should reopen");
    engine
        .open_session()
        .await
        .expect("session should reopen")
}

/// Every `(key, value)` in one space.
async fn space_entries(
    read: &(impl StorageAdapterRead + ?Sized),
    space: StorageSpace,
) -> Vec<(StorageKey, bytes::Bytes)> {
    let range = StoragePrefix {
        bytes: bytes::Bytes::new(),
    }
    .to_range()
    .expect("valid empty prefix");
    let mut cursor = read
        .begin_scan(space, range, StorageBeginScanOptions::default())
        .await
        .expect("scan the serving space");
    cursor
        .collect_all()
        .await
        .expect("collect serving entries")
        .into_iter()
        .map(|entry| {
            let value = match entry.value {
                ProjectedValue::FullValue(bytes) => bytes,
                ProjectedValue::KeyOnly => bytes::Bytes::new(),
            };
            (entry.key, value)
        })
        .collect()
}

/// Physically removes every tombstone key in `ROW_SPACE`, simulating exactly
/// what a compaction pass would stage. Returns how many were removed.
///
/// The probe fixture creates no engine-owned tombstones — the `fresh` arm of
/// phase 1 reports 0 tombstones against 42 live engine rows — so every key this
/// removes belongs to the churned probe collection.
async fn drop_all_tombstones(storage: &Memory) -> usize {
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("read the hot row plane");
    let entries = space_entries(&read, crate::hot_state::ROW_SPACE).await;
    drop(read);
    let mut writes = adapter.new_write_set();
    let mut removed = 0_usize;
    for (key, value) in entries {
        if value.len() > 1 && value[1] & HEAD_VALUE_DELETED_BIT != 0 {
            writes.delete(crate::hot_state::ROW_SPACE, key);
            removed += 1;
        }
    }
    adapter
        .commit_write_set(writes, StorageWriteOptions::default())
        .await
        .expect("tombstone removal should commit");
    removed
}

/// Rows the working diff currently reports for one schema.
async fn working_diff_rows(session: &SessionContext<Memory>, schema_key: &str) -> usize {
    session
        .execute(
            "SELECT entity_pk FROM lix_working_diff WHERE schema_key = $1",
            &[crate::Value::Text(schema_key.to_string())],
        )
        .await
        .expect("working diff should read")
        .len()
}

/// Plans and commits a repository GC sweep against the same physical store the
/// session writes through.
async fn run_repository_gc(storage: &Memory) {
    let adapter = StorageAdapter::new(storage.clone());
    let read = SharedStorageAdapterRead::new(
        adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("gc read"),
    );
    let mut gc_writes = adapter.new_write_set();
    crate::gc::stage_repository_gc(read, &mut gc_writes)
        .await
        .expect("repository gc should plan");
    adapter
        .commit_write_set(gc_writes, StorageWriteOptions::default())
        .await
        .expect("gc write set should commit");
}

fn probe_schema(key: &str) -> serde_json::Value {
    json!({
        "x-lix-key": key,
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "locale": { "type": "string" }
        },
        "required": ["id", "locale"],
        "additionalProperties": false
    })
}

async fn register(session: &SessionContext<Memory>, schema: serde_json::Value) {
    session
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
            &[crate::Value::Text(schema.to_string())],
        )
        .await
        .expect("schema should register");
}

const CHUNK: usize = 250;

/// Row 0 carries `locale = 'keep'`; every other row carries `'drop'`.
async fn insert_rows(session: &SessionContext<Memory>, table: &str, count: usize) {
    let mut index = 0;
    while index < count {
        let end = (index + CHUNK).min(count);
        let values = (index..end)
            .map(|i| {
                let locale = if i == 0 { "keep" } else { "drop" };
                format!("('row-{i}', '{locale}')")
            })
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(
                &format!("INSERT INTO {table} (id, locale) VALUES {values}"),
                &[],
            )
            .await
            .expect("rows should insert");
        index = end;
    }
}

/// Deletes `row-1 .. row-{count-1}`, leaving exactly one live row.
async fn delete_all_but_first(session: &SessionContext<Memory>, table: &str, count: usize) {
    let mut index = 1;
    while index < count {
        let end = (index + CHUNK).min(count);
        let ids = (index..end)
            .map(|i| format!("'row-{i}'"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(&format!("DELETE FROM {table} WHERE id IN ({ids})"), &[])
            .await
            .expect("bulk delete should run");
        index = end;
    }
}

async fn timed_scan(
    session: &SessionContext<Memory>,
    sql: &str,
    expect_rows: usize,
    reps: usize,
) -> Duration {
    for _ in 0..2 {
        let rows = session.execute(sql, &[]).await.expect("warmup should run");
        assert_eq!(rows.len(), expect_rows, "warmup returned the wrong count");
    }
    let mut samples = Vec::new();
    for _ in 0..reps {
        let start = Instant::now();
        let rows = session.execute(sql, &[]).await.expect("scan should run");
        let elapsed = start.elapsed();
        assert_eq!(rows.len(), expect_rows, "scan returned the wrong row count");
        samples.push(elapsed);
    }
    samples.sort();
    samples[samples.len() / 2]
}

fn scan_sql(table: &str) -> String {
    // Equality on a column the schema neither keys nor declares, so the engine
    // keeps its ordinary collection scan instead of any point or index route.
    format!("SELECT id FROM {table} WHERE locale = 'keep'")
}

/// PHASE 1 — the rate.
///
/// `churn` inserts N rows and deletes N-1, so the live collection is one row
/// at every size and the answer is one row at every size. `fresh` inserts the
/// single surviving row and nothing else: same live state, same answer, no
/// tombstones. The difference between the two columns is what the tombstones
/// cost, and the census says how many there are per delete.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn hot_row_tombstone_accumulation_and_scan_cost() {
    let sizes = sizes_from_env("LIX_TOMBSTONE_SIZES", &[100, 1_000, 10_000]);
    let reps = reps_from_env(5);
    println!(
        "phase1 | arm,n,deletes,row_entries,tombstones,live_entries,packed_bases,root_bases,answer_rows,scan_us"
    );
    for n in sizes {
        {
            let (storage, session) = open_session().await;
            register(&session, probe_schema("churnrow")).await;
            insert_rows(&session, "churnrow", n).await;
            delete_all_but_first(&session, "churnrow", n).await;
            let census = row_census(&storage).await;
            let scan = timed_scan(&session, &scan_sql("churnrow"), 1, reps).await;
            println!(
                "churn,{n},{},{},{},{},{},{},1,{}",
                n - 1,
                census.entries,
                census.tombstones,
                census.live(),
                census.packed_bases,
                census.root_bases,
                scan.as_micros()
            );
        }
        {
            let (storage, session) = open_session().await;
            register(&session, probe_schema("freshrow")).await;
            insert_rows(&session, "freshrow", 1).await;
            let census = row_census(&storage).await;
            let scan = timed_scan(&session, &scan_sql("freshrow"), 1, reps).await;
            println!(
                "fresh,{n},0,{},{},{},{},{},1,{}",
                census.entries,
                census.tombstones,
                census.live(),
                census.packed_bases,
                census.root_bases,
                scan.as_micros()
            );
        }
    }
}

/// PHASE 2 — what, if anything, reclaims a tombstone.
///
/// One churned collection, then each candidate reclamation event in turn, with
/// a census after each. Every census also re-runs the scan so a reclamation
/// that fires shows up as both a lower count and a cheaper scan.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn hot_row_tombstone_reclamation_events() {
    let n = sizes_from_env("LIX_TOMBSTONE_SIZES", &[1_000])[0];
    let reps = reps_from_env(5);
    let (storage, session) = open_session().await;
    register(&session, probe_schema("evtrow")).await;
    register(&session, probe_schema("otherrow")).await;
    insert_rows(&session, "evtrow", n).await;
    delete_all_but_first(&session, "evtrow", n).await;

    println!("phase2 | event,row_entries,tombstones,live_entries,packed_bases,root_bases,scan_us");
    let report = |label: &str, census: RowCensus, scan: Duration| {
        println!(
            "{label},{},{},{},{},{},{}",
            census.entries,
            census.tombstones,
            census.live(),
            census.packed_bases,
            census.root_bases,
            scan.as_micros()
        );
    };

    let census = row_census(&storage).await;
    let scan = timed_scan(&session, &scan_sql("evtrow"), 1, reps).await;
    report("after_churn", census, scan);

    // (a) ordinary commits inside the same generation, on an unrelated table.
    for index in 0..20 {
        session
            .execute(
                "INSERT INTO otherrow (id, locale) VALUES ($1, 'x')",
                &[crate::Value::Text(format!("o-{index}"))],
            )
            .await
            .expect("unrelated insert should commit");
    }
    let census = row_census(&storage).await;
    let scan = timed_scan(&session, &scan_sql("evtrow"), 1, reps).await;
    report("after_20_commits", census, scan);

    // (b) checkpoint.
    session
        .create_checkpoint()
        .await
        .expect("checkpoint should publish");
    let census = row_census(&storage).await;
    let scan = timed_scan(&session, &scan_sql("evtrow"), 1, reps).await;
    report("after_checkpoint", census, scan);

    // (c) repository GC.
    run_repository_gc(&storage).await;
    let census = row_census(&storage).await;
    let scan = timed_scan(&session, &scan_sql("evtrow"), 1, reps).await;
    report("after_gc", census, scan);

    // (d) a second checkpoint plus GC, in case the first checkpoint's debt had
    //     not yet come due.
    session
        .execute(
            "INSERT INTO otherrow (id, locale) VALUES ('post-ckpt', 'x')",
            &[],
        )
        .await
        .expect("post-checkpoint insert should commit");
    session
        .create_checkpoint()
        .await
        .expect("second checkpoint should publish");
    run_repository_gc(&storage).await;
    let census = row_census(&storage).await;
    let scan = timed_scan(&session, &scan_sql("evtrow"), 1, reps).await;
    report("after_ckpt2_gc", census, scan);
}

/// PHASE 3 — generation rotation.
///
/// The claim under test is that a branch lifecycle publication mints a fresh
/// serving generation and *re-encodes* the retired generation's tombstones into
/// it, so the retirement sweep reclaims nothing on net. The census is taken on
/// the whole `ROW_SPACE`, across every branch and generation, so a carry-forward
/// shows up as a count that does not fall.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn hot_row_tombstone_generation_rotation() {
    let n = sizes_from_env("LIX_TOMBSTONE_SIZES", &[1_000])[0];
    let reps = reps_from_env(5);
    let (storage, session) = open_session().await;
    register(&session, probe_schema("rotrow")).await;
    insert_rows(&session, "rotrow", n).await;
    delete_all_but_first(&session, "rotrow", n).await;

    println!("phase3 | event,row_entries,tombstones,live_entries,packed_bases,root_bases,scan_us");
    let report = |label: &str, census: RowCensus, scan: Duration| {
        println!(
            "{label},{},{},{},{},{},{}",
            census.entries,
            census.tombstones,
            census.live(),
            census.packed_bases,
            census.root_bases,
            scan.as_micros()
        );
    };

    let main_branch_id = session
        .active_branch_id()
        .await
        .expect("active branch should resolve");
    let census = row_census(&storage).await;
    let scan = timed_scan(&session, &scan_sql("rotrow"), 1, reps).await;
    report("after_churn", census, scan);

    let branch = session
        .create_branch(crate::CreateBranchOptions {
            id: None,
            name: "e45-rotation".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("branch should create");
    let census = row_census(&storage).await;
    let scan = timed_scan(&session, &scan_sql("rotrow"), 1, reps).await;
    report("after_create_branch", census, scan);

    session
        .switch_branch(crate::SwitchBranchOptions {
            branch_id: branch.id.clone(),
        })
        .await
        .expect("branch should switch");
    let census = row_census(&storage).await;
    let scan = timed_scan(&session, &scan_sql("rotrow"), 1, reps).await;
    report("after_switch_to_branch", census, scan);

    // A commit on the new branch, then back to main: exercises both branches'
    // serving generations.
    session
        .execute(
            "INSERT INTO rotrow (id, locale) VALUES ('branch-row', 'x')",
            &[],
        )
        .await
        .expect("branch insert should commit");
    session
        .switch_branch(crate::SwitchBranchOptions {
            branch_id: main_branch_id,
        })
        .await
        .expect("branch should switch back");
    let census = row_census(&storage).await;
    let scan = timed_scan(&session, &scan_sql("rotrow"), 1, reps).await;
    report("after_switch_back_to_main", census, scan);

    run_repository_gc(&storage).await;
    let census = row_census(&storage).await;
    let scan = timed_scan(&session, &scan_sql("rotrow"), 1, reps).await;
    report("after_gc", census, scan);
}

/// PHASE 4 — the compaction premise, validated before any engine change.
///
/// The design rests on one rule: a tombstone is load-bearing iff (a) a base
/// visible to its generation still holds its identity, or (b) its working-diff
/// baseline still makes the delete observable. The proposed first landing gates
/// compaction on the generation having **zero** records in both base planes,
/// which makes (a) vacuously false, and runs at the checkpoint, which is where
/// (b) is discharged.
///
/// `safe` is that case: churn, checkpoint, then physically remove every
/// tombstone. The query must still answer identically, the collection must not
/// resurrect, the working diff must not move, and the scan must fall back to
/// the no-tombstone control.
///
/// `near_miss` is the case compaction must refuse: the deletes are *not* yet
/// checkpointed, so their baselines are live and the working diff is reporting
/// them. Removing those same tombstones is observably wrong, and this arm
/// measures the harm rather than asserting it in prose.
///
/// Both arms mutate storage underneath the engine and then **reopen** it, so
/// nothing here can pass because of a warm cache.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn hot_row_tombstone_compaction_premise() {
    let n = sizes_from_env("LIX_TOMBSTONE_SIZES", &[1_000])[0];
    let reps = reps_from_env(5);
    println!("phase4 | arm,event,row_entries,tombstones,packed_bases,root_bases,diff_records,working_diff_rows,answer_rows,collection_rows,scan_us");

    // ---- safe arm: the delete is already checkpointed ----
    {
        let (storage, session) = open_session().await;
        register(&session, probe_schema("safrow")).await;
        insert_rows(&session, "safrow", n).await;
        delete_all_but_first(&session, "safrow", n).await;
        session
            .create_checkpoint()
            .await
            .expect("checkpoint should publish");

        let census = row_census(&storage).await;
        // The precondition the first landing gates on. If this ever fails the
        // gate itself is what changed, not the conclusion.
        assert_eq!(census.packed_bases, 0, "safe arm expects no packed base");
        assert_eq!(census.root_bases, 0, "safe arm expects no root base");
        let diff_before = working_diff_rows(&session, "safrow").await;
        assert_eq!(
            diff_before, 0,
            "a checkpoint must discharge every delete's working-diff obligation"
        );
        let scan_before = timed_scan(&session, &scan_sql("safrow"), 1, reps).await;
        let collection_before = session
            .execute("SELECT id FROM safrow", &[])
            .await
            .expect("collection scan should run")
            .len();
        println!(
            "safe,before_drop,{},{},{},{},{},{},1,{},{}",
            census.entries,
            census.tombstones,
            census.packed_bases,
            census.root_bases,
            census.diff_records,
            diff_before,
            collection_before,
            scan_before.as_micros()
        );

        let removed = drop_all_tombstones(&storage).await;
        assert_eq!(removed, n - 1, "every tombstone should have been removable");
        let session = reopen_session(&storage).await;

        let census = row_census(&storage).await;
        let diff_after = working_diff_rows(&session, "safrow").await;
        let answer = session
            .execute(&scan_sql("safrow"), &[])
            .await
            .expect("query should still run");
        let collection_after = session
            .execute("SELECT id FROM safrow", &[])
            .await
            .expect("collection scan should still run")
            .len();
        let scan_after = timed_scan(&session, &scan_sql("safrow"), 1, reps).await;
        println!(
            "safe,after_drop,{},{},{},{},{},{},{},{},{}",
            census.entries,
            census.tombstones,
            census.packed_bases,
            census.root_bases,
            census.diff_records,
            diff_after,
            answer.len(),
            collection_after,
            scan_after.as_micros()
        );

        // The correctness bar, asserted rather than eyeballed.
        assert_eq!(answer.len(), 1, "the surviving row must still answer");
        assert_eq!(
            collection_after, 1,
            "dropping tombstones must not resurrect a deleted row"
        );
        assert_eq!(
            diff_after, diff_before,
            "dropping a discharged tombstone must not move the working diff"
        );
    }

    // ---- near-miss arm: the delete is NOT yet checkpointed ----
    {
        let (storage, session) = open_session().await;
        register(&session, probe_schema("nmrow")).await;
        insert_rows(&session, "nmrow", n).await;
        // Checkpoint FIRST, so the inserts are the clean baseline and the
        // deletes below are what the working diff is reporting.
        session
            .create_checkpoint()
            .await
            .expect("first checkpoint should publish");
        delete_all_but_first(&session, "nmrow", n).await;

        let census = row_census(&storage).await;
        let diff_before = working_diff_rows(&session, "nmrow").await;
        let scan_before = timed_scan(&session, &scan_sql("nmrow"), 1, reps).await;
        println!(
            "near_miss,before_drop,{},{},{},{},{},{},1,1,{}",
            census.entries,
            census.tombstones,
            census.packed_bases,
            census.root_bases,
            census.diff_records,
            diff_before,
            scan_before.as_micros()
        );
        assert!(
            diff_before > 0,
            "an uncheckpointed delete must be visible in the working diff, \
             otherwise this arm proves nothing"
        );

        let removed = drop_all_tombstones(&storage).await;
        let session = reopen_session(&storage).await;
        let census = row_census(&storage).await;
        let diff_after = working_diff_rows(&session, "nmrow").await;
        let answer = session
            .execute(&scan_sql("nmrow"), &[])
            .await
            .expect("query should still run");
        let collection_after = session
            .execute("SELECT id FROM nmrow", &[])
            .await
            .expect("collection scan should still run")
            .len();
        println!(
            "near_miss,after_drop,{},{},{},{},{},{},{},{},-",
            census.entries,
            census.tombstones,
            census.packed_bases,
            census.root_bases,
            census.diff_records,
            diff_after,
            answer.len(),
            collection_after
        );
        println!(
            "near_miss | removed={removed} working_diff_rows_lost={}",
            diff_before.saturating_sub(diff_after)
        );

        // MEASURED, and it refuted the hypothesis this arm was written to
        // confirm. The prediction was that removing a live-baseline tombstone
        // would lose working-diff deletes. It does not: the diff still reports
        // every delete after all of them are physically gone and the engine has
        // been reopened cold. The ROW_SPACE tombstone is therefore **not** the
        // working diff's authority — `DIFF_SPACE` is, and the census column
        // above is what says so. Condition (b) of the compaction rule does not
        // hold against this plane at all.
        //
        // The assertion is kept, inverted, so that a future change which makes
        // the working diff depend on the tombstone fails here loudly.
        assert_eq!(
            diff_after, diff_before,
            "the working diff must survive tombstone removal; if it stops \
             surviving, ROW_SPACE has become the working diff's authority and \
             the compaction rule needs condition (b) back"
        );
        assert_eq!(
            answer.len(),
            1,
            "the surviving row must still answer before any checkpoint"
        );
        assert_eq!(
            collection_after, 1,
            "removing an uncheckpointed tombstone must still not resurrect a row"
        );
    }
}

/// PHASE 5 — is the rotated-generation read cost proportional to the base, or
/// is it a fixed setup cost?
///
/// Phase 3 measured a 3.9x penalty on a *churned* collection, which confounds
/// two things: the accumulated tombstones and the root-backed serving path. This
/// phase removes the churn entirely — every arm inserts `n` rows and deletes
/// none, so there are zero tombstones — and varies only `n`, holding the answer
/// at exactly one row.
///
/// - `live_main` serves the answer from the branch's own hot generation.
/// - `live_rot` creates a branch over that head and switches to it, so the same
///   answer is served by a sparse generation over a root current base.
///
/// If the penalty is a fixed setup cost it is the same number of microseconds at
/// n = 1 and at n = 10 000. If it is proportional to the base it grows with `n`
/// while the answer stays at one row.
///
/// The point lane (`WHERE id = 'row-0'`, equality on the declared primary key)
/// is measured in the same arms because it is the lane that has an exact
/// root-base route (`load_root_current_base_exact`) available to it; the
/// collection lane does not.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn rotated_generation_read_scaling() {
    let sizes = sizes_from_env("LIX_TOMBSTONE_SIZES", &[1, 100, 1_000, 10_000]);
    let reps = reps_from_env(5);
    println!(
        "phase5 | arm,n,row_entries,tombstones,live_entries,packed_bases,root_bases,scan_us,point_us"
    );
    let point_sql = "SELECT id FROM liverow WHERE id = 'row-0'";
    for n in sizes {
        for rotate in [false, true] {
            let (storage, session) = open_session().await;
            register(&session, probe_schema("liverow")).await;
            insert_rows(&session, "liverow", n).await;
            if rotate {
                let branch = session
                    .create_branch(crate::CreateBranchOptions {
                        id: None,
                        name: "e51-rotation".to_string(),
                        from_commit_id: None,
                    })
                    .await
                    .expect("branch should create");
                session
                    .switch_branch(crate::SwitchBranchOptions {
                        branch_id: branch.id.clone(),
                    })
                    .await
                    .expect("branch should switch");
            }
            let census = row_census(&storage).await;
            let scan = timed_scan(&session, &scan_sql("liverow"), 1, reps).await;
            let point = timed_scan(&session, point_sql, 1, reps).await;
            println!(
                "{},{n},{},{},{},{},{},{},{}",
                if rotate { "live_rot" } else { "live_main" },
                census.entries,
                census.tombstones,
                census.live(),
                census.packed_bases,
                census.root_bases,
                scan.as_micros(),
                point.as_micros()
            );
        }
    }
}

/// Connectivity guard for the root-base serving cache.
///
/// A serving cache that is built but never reached is invisible to a timing
/// sweep: the numbers simply do not move, which reads as "the change did not
/// help" rather than "the change is not wired up". That happened once while
/// building this cache — three plausible reader-construction sites were wired
/// and the lane that actually serves a SQL collection scan
/// (`HotStateContextReader::scan_hot_branch_rows`) was not among them, and the
/// A/B came back flat. This asserts engagement directly instead.
///
/// Note the counters are process-global, so under a parallel test run another
/// test could contribute hits. That can only make this pass spuriously, never
/// fail spuriously; it is a connectivity guard, not an exact accounting test.
#[cfg(feature = "storage-benches")]
#[tokio::test]
async fn rotated_generation_serving_view_is_cached_after_the_first_read() {
    let (_storage, session) = open_session().await;
    register(&session, probe_schema("cachedrow")).await;
    insert_rows(&session, "cachedrow", 50).await;
    let branch = session
        .create_branch(crate::CreateBranchOptions {
            id: None,
            name: "e51-cache-guard".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("branch should create");
    session
        .switch_branch(crate::SwitchBranchOptions {
            branch_id: branch.id.clone(),
        })
        .await
        .expect("branch should switch");

    // Discard whatever branch creation and the switch themselves recorded.
    let _ = crate::storage_bench::take_root_base_batch_cache_accounting();

    for _ in 0..5 {
        let rows = session
            .execute(&scan_sql("cachedrow"), &[])
            .await
            .expect("rotated scan should run");
        assert_eq!(rows.len(), 1, "the rotated generation must serve one row");
    }
    let (hits, misses) = crate::storage_bench::take_root_base_batch_cache_accounting();
    assert!(
        misses > 0,
        "the first rotated read must materialize the serving view (hits={hits} misses={misses})"
    );
    assert!(
        hits > 0,
        "later rotated reads must be served from the materialized view, \
         not re-derived from canonical records (hits={hits} misses={misses})"
    );
}

/// PHASE 6 — write-path question 2: undo/redo must replay from canonical
/// changes, demonstrated rather than argued.
///
/// The code answer is that `session/undo_redo.rs` reads only through
/// `tracked_state_reader()` — `commit_delta_values_for_schemas` and
/// `load_projected_batch_at_commit` — both canonical tracked state at a commit,
/// and never touches `ROW_SPACE`. This is the experiment that shows it.
///
/// Delete a row, physically remove its tombstone, reopen the engine cold, then
/// undo. If undo depended on the `ROW_SPACE` tombstone in any way, the row
/// could not come back.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn undo_restores_a_row_whose_tombstone_was_removed() {
    let (storage, session) = open_session().await;
    register(&session, probe_schema("undorow")).await;
    insert_rows(&session, "undorow", 3).await;
    let before = session
        .execute("SELECT id FROM undorow", &[])
        .await
        .expect("collection reads")
        .len();

    session
        .execute("DELETE FROM undorow WHERE id = 'row-1'", &[])
        .await
        .expect("delete should commit");
    let census_after_delete = row_census(&storage).await;
    let after_delete = session
        .execute("SELECT id FROM undorow", &[])
        .await
        .expect("collection reads")
        .len();

    let removed = drop_all_tombstones(&storage).await;
    let session = reopen_session(&storage).await;
    let census_after_drop = row_census(&storage).await;
    let after_drop = session
        .execute("SELECT id FROM undorow", &[])
        .await
        .expect("collection reads")
        .len();

    session.undo().await.expect("undo should publish");
    let after_undo = session
        .execute("SELECT id FROM undorow", &[])
        .await
        .expect("collection reads")
        .len();
    let restored = session
        .execute("SELECT id, locale FROM undorow WHERE id = 'row-1'", &[])
        .await
        .expect("restored row reads");

    println!(
        "phase6 | before={before} after_delete={after_delete} tombstones_after_delete={} \
         removed={removed} after_drop={after_drop} tombstones_after_drop={} after_undo={after_undo} \
         restored_rows={}",
        census_after_delete.tombstones,
        census_after_drop.tombstones,
        restored.len()
    );

    assert_eq!(before, 3, "fixture should start with three rows");
    assert_eq!(after_delete, 2, "the delete should be visible");
    assert_eq!(removed, 1, "the delete should have left exactly one tombstone");
    assert_eq!(
        after_drop, 2,
        "removing the tombstone must not resurrect the row"
    );
    assert_eq!(
        after_undo, 3,
        "undo must restore the deleted row with its tombstone already gone; \
         if this fails, undo depends on the ROW_SPACE tombstone and the \
         never-write design is unsafe"
    );
    assert_eq!(restored.len(), 1, "the restored row must be readable");
}

/// PHASE 7 — is `reject_retention_change` an invariant, or an artefact of
/// tombstone lifetime?
///
/// The fence refuses an untracked INSERT over a tracked-deleted identity, and
/// it refuses it *because the tombstone is physically present* — `existing` is
/// the predecessor row. `absence_guards` does not cover this case: those are
/// pure "must not be live" assertions, so an absent identity and a tombstone
/// pass identically.
///
/// So if any supported operation on `main` already clears that tombstone, the
/// fence is not durable and the hole is open today — compaction would change
/// *when* it stops fencing, not *whether*. If nothing clears it, the fence is
/// real and compaction would breach it.
///
/// This tries every route to the no-tombstone state and reports which, if any,
/// reaches it. Nothing here compacts anything.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn retention_fence_durability_across_supported_operations() {
    async fn untracked_insert(
        session: &SessionContext<Memory>,
        table: &str,
    ) -> Result<(), String> {
        session
            .execute(
                &format!(
                    "INSERT INTO {table} (id, locale, lixcol_untracked) VALUES ('row-0', 'u', TRUE)"
                ),
                &[],
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    println!("phase6 | route,tombstones,untracked_insert_result");

    // Each route gets a fresh fixture so the routes cannot contaminate one
    // another. `base` is the control: no route applied.
    for route in [
        "base",
        "checkpoint",
        "branch_roundtrip",
        "on_new_branch",
        "tombstone_dropped",
    ] {
        let (storage, session) = open_session().await;
        register(&session, probe_schema("fencerow")).await;
        session
            .execute(
                "INSERT INTO fencerow (id, locale) VALUES ('row-0', 'keep')",
                &[],
            )
            .await
            .expect("tracked insert should commit");
        session
            .execute("DELETE FROM fencerow WHERE id = 'row-0'", &[])
            .await
            .expect("tracked delete should commit");

        let mut session = session;
        match route {
            "base" => {}
            "checkpoint" => {
                session
                    .create_checkpoint()
                    .await
                    .expect("checkpoint should publish");
            }
            "branch_roundtrip" => {
                let main_branch_id = session
                    .active_branch_id()
                    .await
                    .expect("active branch resolves");
                let branch = session
                    .create_branch(crate::CreateBranchOptions {
                        id: None,
                        name: "e45-fence-roundtrip".to_string(),
                        from_commit_id: None,
                    })
                    .await
                    .expect("branch should create");
                session
                    .switch_branch(crate::SwitchBranchOptions {
                        branch_id: branch.id,
                    })
                    .await
                    .expect("switch to branch");
                session
                    .switch_branch(crate::SwitchBranchOptions {
                        branch_id: main_branch_id,
                    })
                    .await
                    .expect("switch back to main");
            }
            "on_new_branch" => {
                // The interesting one: a fresh branch's generation is sparse
                // over a root base and owns no HOT_ROW tombstone of its own.
                let branch = session
                    .create_branch(crate::CreateBranchOptions {
                        id: None,
                        name: "e45-fence-newbranch".to_string(),
                        from_commit_id: None,
                    })
                    .await
                    .expect("branch should create");
                session
                    .switch_branch(crate::SwitchBranchOptions {
                        branch_id: branch.id,
                    })
                    .await
                    .expect("switch to branch");
            }
            // The state compaction would create, reached here by removing the
            // tombstone directly rather than by compacting. No supported
            // operation reaches this state today; compaction would be the
            // first. If the fence stops holding here, that is the hole
            // compaction opens, and it is what PR 1 exists to close.
            "tombstone_dropped" => {
                let removed = drop_all_tombstones(&storage).await;
                assert_eq!(removed, 1, "the tracked delete should leave one tombstone");
                session = reopen_session(&storage).await;
            }
            _ => unreachable!(),
        }

        let census = row_census(&storage).await;
        let result = untracked_insert(&session, "fencerow").await;
        // Every route, including the one that removes the tombstone the fence
        // used to ride on, must refuse. Before the narrowed fence landed,
        // `tombstone_dropped` SUCCEEDED here while the other four refused —
        // that gap is what the fence closes, and this assertion is what proves
        // it rather than restating it.
        assert!(
            result.is_err(),
            "route '{route}' let an untracked row take a tracked-deleted identity; \
             the retention fence does not survive this state"
        );
        let verdict = match &result {
            Ok(()) => "SUCCEEDED".to_string(),
            Err(message) => format!("refused: {}", message.replace(',', ";")),
        };
        println!("{route},{},{verdict}", census.tombstones);

        // If the untracked insert got in, the identity is now untracked while a
        // tracked delete of the same identity sits in canonical history. Undo
        // replays that delete from canonical history (phase 5). If undo brings
        // the tracked row back, the identity is simultaneously tracked and
        // untracked.
        if result.is_ok() {
            let undo = session.undo().await;
            match undo {
                Ok(_) => {
                    let rows = session
                        .execute(
                            "SELECT id, lixcol_untracked FROM fencerow WHERE id = 'row-0'",
                            &[],
                        )
                        .await
                        .expect("identity reads after undo");
                    println!(
                        "{route},POST_UNDO,rows_for_identity={} <- 2 means tracked+untracked \
                         coexist",
                        rows.len()
                    );
                }
                Err(error) => println!(
                    "{route},POST_UNDO,undo_refused: {}",
                    error.to_string().replace(',', ";")
                ),
            }
        }
        drop(session);
    }
}

/// PHASE 8 — the allocation census, and the decision point for whether the
/// per-row identity work on the payload fetch is worth removing.
///
/// The retained profile that motivated this attributed ~6.6% of a rotated
/// collection scan to `load_commit_delta_entry_at_index` and ~6.7% to
/// `codec::decode_key`. That profile was recorded at `49a4bf45a`, which is
/// **before** the root-base serving cache landed. The cache changes the
/// question from "what does one read cost" to "how many reads pay it at all",
/// and no percentage from that profile can answer the second question.
///
/// So this counts, per scan, at the sites themselves:
///
/// - `rows` — `load_commit_delta_entry_at_index` calls, the per-row payload fetch.
/// - `decodes` / `dec_in_b` — `codec::decode_key` calls and the encoded bytes
///   they consumed, all callers.
/// - `own_b` / `esc` — the bytes `into_owned()` copies over what
///   `decode_key_borrowed` already produced, and how many of those strings were
///   escaped and so allocate regardless. `own_b` is the ceiling on what a
///   borrow-based fix at that site can remove; it is not the cost of the decode.
/// - `acct_b` — the per-row `account_id.to_string()`.
/// - `enc` / `enc_b` — per-row `encode_key_ref` reaching the binary search.
/// - `clones` / `clone_b` — the deep key clone `load_commit_delta_change_records`
///   makes to build its request vector.
/// - `matkey` / `matkey_b` — owned `TrackedStateKey` values
///   `materialize_index_payloads` builds from keys it already holds borrowed.
/// - `reverify` — rows reaching the post-fetch identity re-check.
/// - `hit` / `miss` — root-base serving cache, and `dur`/`exa`/`rep` the
///   `scan_batch_at_commit` arm, so a zero census is never ambiguous between
///   "no allocation" and "the lane never ran".
///
/// `cold` is the first read of the rotated generation; `warm` aggregates the
/// next `reps` reads. Per-scan numbers are `warm / reps`.
#[cfg(feature = "storage-benches")]
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn rotated_generation_key_allocation_census() {
    let sizes = sizes_from_env("LIX_TOMBSTONE_SIZES", &[100, 1_000, 10_000]);
    let reps = reps_from_env(5);
    println!(
        "phase8 | arm,n,phase,scans,rows,decodes,dec_in_b,own_b,esc,acct_b,enc,enc_b,\
clones,clone_b,matkey,matkey_b,reverify,hit,miss,dur,exa,rep,us"
    );
    for n in sizes {
        for rotate in [false, true] {
            let (_storage, session) = open_session().await;
            register(&session, probe_schema("censusrow")).await;
            insert_rows(&session, "censusrow", n).await;
            if rotate {
                let branch = session
                    .create_branch(crate::CreateBranchOptions {
                        id: None,
                        name: "e52-census".to_string(),
                        from_commit_id: None,
                    })
                    .await
                    .expect("branch should create");
                session
                    .switch_branch(crate::SwitchBranchOptions {
                        branch_id: branch.id.clone(),
                    })
                    .await
                    .expect("branch should switch");
            }
            let scan = scan_sql("censusrow");
            let arm = if rotate { "rot" } else { "main" };

            // Discard everything setup recorded, so the cold line is the cold
            // read and nothing else.
            let _ = crate::storage_bench::take_tracked_key_allocation_census();
            let _ = crate::storage_bench::take_root_base_batch_cache_accounting();
            let _ = crate::storage_bench::take_tracked_scan_branch_accounting();

            let started = Instant::now();
            let rows = session.execute(&scan, &[]).await.expect("cold scan");
            let cold_us = started.elapsed().as_micros();
            assert_eq!(rows.len(), 1, "the cold scan must answer exactly one row");
            print_census_line(arm, n, "cold", 1, cold_us);

            let started = Instant::now();
            for _ in 0..reps {
                let rows = session.execute(&scan, &[]).await.expect("warm scan");
                assert_eq!(rows.len(), 1, "the warm scan must answer exactly one row");
            }
            let warm_us = started.elapsed().as_micros();
            print_census_line(arm, n, "warm", reps, warm_us);
        }
    }
}

/// Drains every census counter and prints one CSV line. Draining is what makes
/// the next phase's line attributable to that phase alone.
#[cfg(feature = "storage-benches")]
fn print_census_line(arm: &str, n: usize, phase: &str, scans: usize, us: u128) {
    let c = crate::storage_bench::take_tracked_key_allocation_census();
    let (hit, miss) = crate::storage_bench::take_root_base_batch_cache_accounting();
    let (dur, exa, rep) = crate::storage_bench::take_tracked_scan_branch_accounting();
    println!(
        "{arm},{n},{phase},{scans},{},{},{},{},{},{},{},{},{},{},{},{},{},{hit},{miss},{dur},{exa},{rep},{us}",
        c.commit_delta_rows_loaded,
        c.key_decode_calls,
        c.key_decode_input_bytes,
        c.key_decode_owned_string_bytes,
        c.key_decode_escaped_strings,
        c.commit_delta_account_id_bytes,
        c.commit_delta_point_key_encodes,
        c.commit_delta_point_key_encode_bytes,
        c.commit_delta_request_key_clones,
        c.commit_delta_request_key_clone_bytes,
        c.materialize_owned_key_builds,
        c.materialize_owned_key_bytes,
        c.materialize_reverify_rows,
    );
}
