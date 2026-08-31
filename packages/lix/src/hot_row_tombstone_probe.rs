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
    Memory, SharedStorageAdapterRead, StorageAdapter, StorageAdapterRead, StorageBeginScanOptions,
    StorageKey, StoragePrefix, StorageReadOptions, StorageSpace, StorageWriteOptions,
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
    let session = engine.open_session().await.expect("session should open");
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
    let diff_records = space_values(&read, crate::hot_state::DIFF_SPACE)
        .await
        .len();
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
    engine.open_session().await.expect("session should reopen")
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
    let checkpoint = session
        .execute(
            "SELECT commit_id FROM lix_checkpoint ORDER BY lixcol_created_at DESC LIMIT 1",
            &[],
        )
        .await
        .expect("latest checkpoint should read")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("checkpoint ID should decode");
    session
        .execute(
            &format!(
                "SELECT row_ref FROM lix_diff('{schema_key}', $1, lix_active_branch_commit_id())"
            ),
            &[crate::Value::Text(checkpoint)],
        )
        .await
        .expect("working diff should read")
        .len()
}

/// Plans and commits a repository GC sweep against the same physical store the
/// session writes through.
async fn run_repository_gc(storage: &Memory) -> bool {
    let adapter = StorageAdapter::new(storage.clone());
    let read = SharedStorageAdapterRead::new(
        adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("gc read"),
    );
    let mut gc_writes = adapter.new_write_set();
    let plan = crate::gc::stage_repository_gc(read, &mut gc_writes)
        .await
        .expect("repository gc should plan");
    adapter
        .commit_write_set(gc_writes, StorageWriteOptions::default())
        .await
        .expect("gc write set should commit");
    plan.sweep.has_more
}

async fn drain_repository_gc(storage: &Memory) {
    for _ in 0..32 {
        if !run_repository_gc(storage).await {
            return;
        }
    }
    panic!("bounded repository maintenance did not drain within 32 slices");
}

fn probe_schema(key: &str) -> serde_json::Value {
    json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": key,
        "columns": [
            { "name": "id", "type": "text", "nullable": false },
            { "name": "locale", "type": "text", "nullable": false },
        ],
        "primary_key": ["id"],
    })
}

async fn register(session: &SessionContext<Memory>, schema: serde_json::Value) {
    session
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
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
    drain_repository_gc(&storage).await;
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
    println!(
        "phase4 | arm,event,row_entries,tombstones,packed_bases,root_bases,diff_records,working_diff_rows,answer_rows,collection_rows,scan_us"
    );

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
    assert_eq!(
        removed, 1,
        "the delete should have left exactly one tombstone"
    );
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
    async fn untracked_insert(session: &SessionContext<Memory>, table: &str) -> Result<(), String> {
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

/// Reads back the `created_at` the engine currently reports for one identity.
async fn created_at_of(session: &SessionContext<Memory>, table: &str, id: &str) -> String {
    let result = session
        .execute(
            &format!("SELECT lixcol_created_at AS created_at FROM {table} WHERE id = '{id}'"),
            &[],
        )
        .await
        .expect("created_at reads");
    result.rows()[0]
        .get::<String>("created_at")
        .expect("created_at is text")
}

/// PHASE 9 — the `created_at` consequence of compaction, and who rejects it.
///
/// Numbered 9 because #1427 landed its own PHASE 8 (the allocation census)
/// on the integration branch while this work was in flight.
///
/// Phase 4 established that removing an already-checkpointed delete's tombstone
/// is safe for *serving*. This phase asks the separate write-path question the
/// compaction design turns on: once the tombstone is gone, what `created_at`
/// does a later re-insert of that identity receive, and does anything reject
/// the result?
///
/// Today `created_at` reaches a re-inserted row by a two-hop chain — a delete
/// copies the live row's `created_at` into the tombstone, and a re-insert
/// copies the tombstone's forward. Compaction removes the middle hop.
/// Canonical tracked state still retains the original `created_at` for the
/// deleted identity, on a plane compaction never touches, so the value is
/// recoverable. The question is whether the engine recovers it unaided.
///
/// The control arm runs the same sequence with the tombstone left in place, so
/// any difference is attributable to its removal and nothing else.
#[tokio::test]
async fn recreated_identity_created_at_without_compaction() {
    let (_storage, session) = open_session().await;
    register(&session, probe_schema("c8row")).await;

    session
        .execute(
            "INSERT INTO c8row (id, locale) VALUES ('row-0', 'first')",
            &[],
        )
        .await
        .expect("first insert should commit");
    // Mirrors the compacted arm's shape exactly, so the only difference
    // between the two is whether the tombstone survives.
    session
        .create_checkpoint()
        .await
        .expect("baseline checkpoint should publish");
    let first = created_at_of(&session, "c8row", "row-0").await;

    session
        .execute("DELETE FROM c8row WHERE id = 'row-0'", &[])
        .await
        .expect("delete should commit");
    session
        .create_checkpoint()
        .await
        .expect("checkpoint should publish");
    session
        .execute(
            "INSERT INTO c8row (id, locale) VALUES ('row-0', 'second')",
            &[],
        )
        .await
        .expect("re-insert should commit");
    let second = created_at_of(&session, "c8row", "row-0").await;

    println!(
        "phase8 | arm=control first_created_at={first} second_created_at={second} \
         inherited={}",
        first == second
    );

    // This is the behaviour compaction must preserve. It is asserted rather
    // than merely printed because it is the regression guard for the
    // canonical-sourced `created_at` that compaction requires: if that
    // sourcing silently stops firing, this is the assertion that catches it.
    assert_eq!(
        second, first,
        "a re-inserted identity inherits its deleted predecessor's created_at"
    );
}

/// The compaction arm of phase 8. Simulates compaction with the same physical
/// tombstone removal phases 4 and 7 use, then re-inserts and asks the engine
/// what it thinks the identity's `created_at` is — and whether a commit-root
/// rebuild, which is where `validate_diff_row_created_at` runs in production,
/// still accepts the result.
#[tokio::test]
async fn recreated_identity_created_at_after_compaction() {
    let (storage, session) = open_session().await;
    register(&session, probe_schema("c8row")).await;
    let branch_id = session
        .active_branch_id()
        .await
        .expect("active branch id reads");

    session
        .execute(
            "INSERT INTO c8row (id, locale) VALUES ('row-0', 'first')",
            &[],
        )
        .await
        .expect("first insert should commit");
    // The insert has to be checkpointed before the delete, or the two cancel
    // within one checkpoint interval and canonical records the identity as
    // never having existed. In that case a fresh `created_at` on re-insert is
    // the correct answer and there is nothing to recover — measured: the
    // lookup fires with the right key and canonical returns nothing. The
    // interesting case, and the one compaction actually creates, is a delete
    // whose *insert* is already canonical.
    session
        .create_checkpoint()
        .await
        .expect("baseline checkpoint should publish");
    let first = created_at_of(&session, "c8row", "row-0").await;

    session
        .execute("DELETE FROM c8row WHERE id = 'row-0'", &[])
        .await
        .expect("delete should commit");
    // Compaction is a checkpoint-time operation, and the checkpoint is what
    // discharges the delete's working-diff obligation and commits it to
    // canonical state. Removing the tombstone before that point would be
    // testing an operation the design never performs.
    session
        .create_checkpoint()
        .await
        .expect("checkpoint should publish");
    drop(session);

    let before = row_census(&storage).await;
    let removed = drop_all_tombstones(&storage).await;
    let after = row_census(&storage).await;
    println!(
        "phase8 | arm=compacted tombstones_removed={removed} \
         entries {}->{} tombstones {}->{}",
        before.entries, after.entries, before.tombstones, after.tombstones
    );
    // The engine now compacts a discharged tombstone at the checkpoint itself
    // (`hot_compaction_mask`), so by this point there is usually nothing left
    // for the simulation to remove. Either state is a valid entry into the
    // question this arm asks — what a re-insert over a *missing* tombstone
    // gets — and phase 10 pins the engine-driven route with its own test. What
    // must hold here is that no tombstone survives into the re-insert.
    assert_eq!(
        after.tombstones, 0,
        "the identity must carry no tombstone into the re-insert (removed={removed})"
    );

    let session = reopen_session(&storage).await;
    let hits_before = crate::hot_state::BROAD_CANONICAL_CREATED_AT_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    let keys_before = crate::hot_state::BROAD_CANONICAL_CREATED_AT_KEYS
        .load(std::sync::atomic::Ordering::Relaxed);
    let lookups_before = crate::hot_state::BROAD_CANONICAL_CREATED_AT_LOOKUPS
        .load(std::sync::atomic::Ordering::Relaxed);
    session
        .execute(
            "INSERT INTO c8row (id, locale) VALUES ('row-0', 'second')",
            &[],
        )
        .await
        .expect("re-insert should commit");
    let hits = crate::hot_state::BROAD_CANONICAL_CREATED_AT_HITS
        .load(std::sync::atomic::Ordering::Relaxed)
        - hits_before;
    let keys = crate::hot_state::BROAD_CANONICAL_CREATED_AT_KEYS
        .load(std::sync::atomic::Ordering::Relaxed)
        - keys_before;
    let lookups = crate::hot_state::BROAD_CANONICAL_CREATED_AT_LOOKUPS
        .load(std::sync::atomic::Ordering::Relaxed)
        - lookups_before;
    let second = created_at_of(&session, "c8row", "row-0").await;
    println!(
        "phase8 | arm=compacted first_created_at={first} second_created_at={second} \
         inherited={} lookups={lookups} keys={keys} hits={hits}",
        first == second
    );
    drop(session);

    // Engagement, counted inside the route rather than inferred from a timing
    // result. A hit proves the recovery fired; the run below proves it can
    // also miss, so the route is not trivially returning a constant.
    assert!(lookups > 0, "the canonical lookup must have run");
    assert!(
        hits > 0,
        "the re-insert over a compacted identity must have inherited from canonical"
    );
    assert_eq!(
        second, first,
        "canonical must supply the created_at the compacted tombstone used to carry"
    );

    // `validate_diff_row_created_at` derives its expectation from the parent
    // commit's canonical tracked-state index value and never inspects
    // `deleted`, so a tombstoned ancestor still supplies a `created_at` it
    // will insist on. In production that validator runs behind
    // `Engine::rebuild_tracked_state_for_branch`, which means a mismatch is
    // latent: it is written silently and only surfaces when a rebuild or
    // repair is performed.
    let engine = Engine::new(storage.clone())
        .await
        .expect("engine should reopen for rebuild");
    // An instrument that never executes reports exactly what a clean pass
    // reports. Count the validator's entries across the rebuild so "accepted"
    // cannot be confused with "never reached".
    let validations_before = crate::tracked_state::DIFF_ROW_CREATED_AT_VALIDATIONS
        .load(std::sync::atomic::Ordering::Relaxed);
    let rebuild = engine.rebuild_tracked_state_for_branch(&branch_id).await;
    let validations = crate::tracked_state::DIFF_ROW_CREATED_AT_VALIDATIONS
        .load(std::sync::atomic::Ordering::Relaxed)
        - validations_before;
    match &rebuild {
        Ok(()) => println!("phase8 | arm=compacted rebuild=accepted"),
        Err(error) => println!(
            "phase8 | arm=compacted rebuild=REJECTED {}",
            error.to_string().replace('\n', " ")
        ),
    }

    println!(
        "phase8 | verdict inherited={} rebuild_ok={} created_at_validations={validations}",
        first == second,
        rebuild.is_ok()
    );
}

/// The miss side of the same route. A commit that introduces genuinely new
/// identities submits keys to the canonical lookup and must come back empty —
/// a route that only ever hits would be inheriting timestamps for rows that
/// have no canonical ancestor, which is the failure this pairs against.
#[tokio::test]
async fn broad_canonical_created_at_recovery_misses_for_new_identities() {
    let (_storage, session) = open_session().await;
    register(&session, probe_schema("c8new")).await;

    let keys_before = crate::hot_state::BROAD_CANONICAL_CREATED_AT_KEYS
        .load(std::sync::atomic::Ordering::Relaxed);
    let hits_before = crate::hot_state::BROAD_CANONICAL_CREATED_AT_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    session
        .execute(
            "INSERT INTO c8new (id, locale) VALUES ('new-0', 'a'), ('new-1', 'b')",
            &[],
        )
        .await
        .expect("new rows should commit");
    let keys = crate::hot_state::BROAD_CANONICAL_CREATED_AT_KEYS
        .load(std::sync::atomic::Ordering::Relaxed)
        - keys_before;
    let hits = crate::hot_state::BROAD_CANONICAL_CREATED_AT_HITS
        .load(std::sync::atomic::Ordering::Relaxed)
        - hits_before;

    println!("phase8 | arm=new_identities keys={keys} hits={hits}");
    assert!(
        keys >= 2,
        "both new identities must reach the canonical lookup"
    );
    assert_eq!(
        hits, 0,
        "a genuinely new identity has no canonical ancestor to inherit from"
    );
}

/// Compaction engagement, read from inside the route.
///
/// Process-global by construction, so only *deltas* and only *thresholds*
/// scaled to a fixture no concurrent test can reach are meaningful here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct CompactionCounters {
    /// Publications that reached the mask at all.
    routes: u64,
    /// Deltas those publications offered it.
    offered: u64,
    /// Of those, tombstones eligible on shape alone.
    candidates: u64,
    /// Of those, tombstones every gate cleared.
    compacted: u64,
}

fn compaction_counters() -> CompactionCounters {
    let load =
        |counter: &std::sync::atomic::AtomicU64| counter.load(std::sync::atomic::Ordering::Relaxed);
    CompactionCounters {
        routes: load(&crate::hot_state::COMPACTED_TOMBSTONE_ROUTES),
        offered: load(&crate::hot_state::COMPACTED_TOMBSTONE_OFFERED),
        candidates: load(&crate::hot_state::COMPACTED_TOMBSTONE_CANDIDATES),
        compacted: load(&crate::hot_state::COMPACTED_TOMBSTONE_COMPACTED),
    }
}

impl CompactionCounters {
    fn since(self, before: Self) -> Self {
        Self {
            routes: self.routes - before.routes,
            offered: self.offered - before.offered,
            candidates: self.candidates - before.candidates,
            compacted: self.compacted - before.compacted,
        }
    }
}

impl std::fmt::Display for CompactionCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "routes={} offered={} candidates={} compacted={}",
            self.routes, self.offered, self.candidates, self.compacted
        )
    }
}

/// PHASE 10 — checkpoint publication discharges tombstones in O(1), and
/// repository maintenance reclaims them afterward.
///
/// Numbered 10 because phases 1–7 are shared, 8 is #1427's allocation census
/// and 9 is the canonical `created_at` recovery that #1432 landed.
///
/// Phase 4 proved the *premise* by removing tombstones behind the engine's
/// back. This phase asserts that the foreground checkpoint does not enter the
/// scale-dependent route, then that repository GC does. Both halves are
/// explicit because a flat row count cannot distinguish deferred work from an
/// inert compactor.
#[tokio::test]
async fn checkpoint_compacts_discharged_branch_tombstones() {
    const N: usize = 300;
    let (storage, session) = open_session().await;
    register(&session, probe_schema("cp10row")).await;
    insert_rows(&session, "cp10row", N).await;
    // MEASURED, and it is the shape of the whole result: a checkpoint
    // republishes the interval's *dirty set*, and a delete only joins that set
    // while a checkpoint interval is open. Churn before a repository's first
    // checkpoint reaches the compaction route with `offered=2` — the route
    // runs, the deletes are simply not in it. Compaction therefore reclaims
    // deletes made inside an interval, which is the steady state; a
    // never-checkpointed branch's tombstones are a separate, larger problem
    // this change does not claim.
    session
        .create_checkpoint()
        .await
        .expect("baseline checkpoint should publish");
    delete_all_but_first(&session, "cp10row", N).await;

    let before = row_census(&storage).await;
    assert!(
        before.tombstones >= N - 1,
        "the churn must leave one tombstone per delete, saw {}",
        before.tombstones
    );

    let counters_before = compaction_counters();
    session
        .create_checkpoint()
        .await
        .expect("checkpoint should publish");
    let foreground_counters = compaction_counters().since(counters_before);
    let after_checkpoint = row_census(&storage).await;

    assert_eq!(
        foreground_counters,
        CompactionCounters::default(),
        "foreground checkpoint publication must not enter tombstone compaction"
    );
    assert_eq!(
        after_checkpoint.tombstones, before.tombstones,
        "foreground checkpoint publication must leave physical retirement to maintenance"
    );

    let gc_counters_before = compaction_counters();
    drain_repository_gc(&storage).await;
    let counters = compaction_counters().since(gc_counters_before);
    let after = row_census(&storage).await;

    println!(
        "phase10 | background compaction entries {}->{} tombstones {}->{} {counters}",
        before.entries, after.entries, before.tombstones, after.tombstones
    );

    // Engagement first: a flat row count is ambiguous, these counts are not.
    assert!(
        counters.candidates >= (N - 1) as u64,
        "every discharged tombstone must reach the compaction route, saw {}",
        counters.candidates
    );
    assert!(
        counters.compacted >= (N - 1) as u64,
        "every gate must clear for a branch-local schema with no base, saw {}",
        counters.compacted
    );

    // Then the effect the counters predict.
    assert!(
        after.tombstones * 10 < before.tombstones,
        "the checkpoint must reclaim the tombstones it discharged, {} -> {}",
        before.tombstones,
        after.tombstones
    );
    assert!(
        after.entries < before.entries,
        "reclaimed tombstones must leave the serving view smaller"
    );

    // And the answer, cold, so nothing passes off a warm cache.
    let session = reopen_session(&storage).await;
    let survivors = session
        .execute("SELECT id FROM cp10row", &[])
        .await
        .expect("collection scan should run");
    assert_eq!(
        survivors.len(),
        1,
        "compaction must not resurrect a deleted row"
    );
    let answer = session
        .execute(&scan_sql("cp10row"), &[])
        .await
        .expect("scan should run");
    assert_eq!(answer.len(), 1, "the surviving row must still answer");
    assert_eq!(
        working_diff_rows(&session, "cp10row").await,
        0,
        "a checkpoint discharges every delete, compacted or not"
    );
}

/// The refusal is asserted where it can be made deterministic:
/// `hot_state::tracked_head::hot::tests::
/// checkpoint_compaction_keeps_only_globally_shadowed_tombstones` seeds a
/// global-branch row and a branch tombstone in the same schema and checks the
/// tombstone survives the checkpoint, alongside a same-publication control in
/// a schema global has no rows in that is reclaimed.
///
/// It is not asserted through SQL here because there is no reachable SQL path
/// to a global row in a probe-defined schema: a branch-registered schema is
/// not visible to a global write, and a globally registered one is never bound
/// as a table.
/// The `created_at` consequence, pinned against *real* compaction.
///
/// Phase 9 pins the same behaviour against `drop_all_tombstones`, a simulation.
/// This is the version that fails if the engine's own compaction and the
/// canonical `created_at` recovery ever stop lining up — the failure mode with
/// no other guard, because a wrong timestamp is written silently and only
/// surfaces on a later rebuild.
#[tokio::test]
async fn recreated_identity_inherits_created_at_after_engine_compaction() {
    let (storage, session) = open_session().await;
    register(&session, probe_schema("ci10row")).await;
    session
        .execute(
            "INSERT INTO ci10row (id, locale) VALUES ('row-0', 'first')",
            &[],
        )
        .await
        .expect("first insert should commit");
    // The insert must be canonical before the delete, or the pair cancels
    // inside one checkpoint interval and a fresh timestamp is the right answer.
    session
        .create_checkpoint()
        .await
        .expect("baseline checkpoint should publish");
    let first = created_at_of(&session, "ci10row", "row-0").await;

    session
        .execute("DELETE FROM ci10row WHERE id = 'row-0'", &[])
        .await
        .expect("delete should commit");
    let counters_before = compaction_counters();
    session
        .create_checkpoint()
        .await
        .expect("checkpoint should publish");
    let foreground_counters = compaction_counters().since(counters_before);
    assert_eq!(
        foreground_counters,
        CompactionCounters::default(),
        "foreground checkpoint publication must not enter tombstone compaction"
    );

    let gc_counters_before = compaction_counters();
    run_repository_gc(&storage).await;
    let counters = compaction_counters().since(gc_counters_before);
    assert!(
        counters.compacted > 0,
        "this test is about a compacted identity; nothing was compacted"
    );

    let hits_before = crate::hot_state::BROAD_CANONICAL_CREATED_AT_HITS
        .load(std::sync::atomic::Ordering::Relaxed);
    session
        .execute(
            "INSERT INTO ci10row (id, locale) VALUES ('row-0', 'second')",
            &[],
        )
        .await
        .expect("re-insert should commit");
    let hits = crate::hot_state::BROAD_CANONICAL_CREATED_AT_HITS
        .load(std::sync::atomic::Ordering::Relaxed)
        - hits_before;
    let second = created_at_of(&session, "ci10row", "row-0").await;
    println!(
        "phase10 | created_at first={first} second={second} inherited={} \
         canonical_hits={hits} compacted={}",
        first == second,
        counters.compacted
    );

    assert!(
        hits > 0,
        "the re-insert must have sourced its created_at from canonical state"
    );
    assert_eq!(
        second, first,
        "a re-insert over a compacted identity inherits its original created_at"
    );

    drop(session);
    let engine = Engine::new(storage.clone())
        .await
        .expect("engine should reopen for rebuild");
    let branch_id = engine
        .open_session()
        .await
        .expect("session should open")
        .active_branch_id()
        .await
        .expect("active branch id reads");
    engine
        .rebuild_tracked_state_for_branch(&branch_id)
        .await
        .expect("a compacted branch must still rebuild");
}

/// The measurement the change exists for: what a checkpoint now costs a
/// churned collection's scan, at three sizes so the removed term shows as a
/// slope rather than a point.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn checkpoint_compaction_scan_cost() {
    let sizes = sizes_from_env("LIX_TOMBSTONE_SIZES", &[100, 1_000, 10_000]);
    let reps = reps_from_env(5);
    println!("phase10 | n,event,row_entries,tombstones,scan_us,counters");
    for n in sizes {
        let (storage, session) = open_session().await;
        register(&session, probe_schema("m10row")).await;
        insert_rows(&session, "m10row", n).await;
        // The deletes have to fall inside an open checkpoint interval to join
        // the dirty set the next checkpoint republishes. See
        // `checkpoint_compacts_discharged_branch_tombstones`.
        session
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should publish");
        delete_all_but_first(&session, "m10row", n).await;

        let census = row_census(&storage).await;
        let scan = timed_scan(&session, &scan_sql("m10row"), 1, reps).await;
        println!(
            "{n},after_churn,{},{},{},-",
            census.entries,
            census.tombstones,
            scan.as_micros()
        );

        let counters_before = compaction_counters();
        session
            .create_checkpoint()
            .await
            .expect("checkpoint should publish");
        let counters = compaction_counters().since(counters_before);
        let census = row_census(&storage).await;
        let scan = timed_scan(&session, &scan_sql("m10row"), 1, reps).await;
        println!(
            "{n},after_checkpoint,{},{},{},{}",
            census.entries,
            census.tombstones,
            scan.as_micros(),
            counters
        );
    }
}

/// PHASE 7 - how much of a scan's decode work is tombstones, counted at the
/// per-entry decode loop.
///
/// The counters this reads (`HOT_SCAN_DECODED_ENTRIES` and friends) sit inside
/// the per-entry decode loop of both hot scan arms, not at the layer that
/// returns the answer: a post-filter count reads identically under a seek and
/// under a full walk.
///
/// Four arms, all answering exactly one row:
///
/// - `fresh` - one live row, no history. The floor.
/// - `update_churn` - two live rows, one of them updated `n-1` times. Same
///   order of write volume and the same number of generation rotations as
///   `churn`, but zero tombstones. This is the arm that separates "tombstones
///   inflate the scan" from "the plane is rematerialised per commit": if the
///   cost were rematerialisation, this arm would track `churn`.
/// - `churn` - `n` inserts and `n-1` deletes.
/// - `churn_dropped` - the same fixture as `churn` with every tombstone
///   physically removed and the engine reopened cold. Holds write history
///   constant and varies only the tombstones.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn hot_row_tombstone_decode_census() {
    use std::sync::atomic::Ordering;

    fn reset() {
        crate::hot_state::HOT_SCAN_DECODED_ENTRIES.store(0, Ordering::Relaxed);
        crate::hot_state::HOT_SCAN_MATCHED_ENTRIES.store(0, Ordering::Relaxed);
        crate::hot_state::HOT_SCAN_TOMBSTONE_ENTRIES.store(0, Ordering::Relaxed);
    }
    fn read() -> (u64, u64, u64) {
        (
            crate::hot_state::HOT_SCAN_DECODED_ENTRIES.load(Ordering::Relaxed),
            crate::hot_state::HOT_SCAN_MATCHED_ENTRIES.load(Ordering::Relaxed),
            crate::hot_state::HOT_SCAN_TOMBSTONE_ENTRIES.load(Ordering::Relaxed),
        )
    }

    let sizes = sizes_from_env("LIX_TOMBSTONE_SIZES", &[1_000, 10_000]);
    let reps = reps_from_env(5);
    println!(
        "phase7 | arm,n,row_entries,space_tombstones,decoded,matched,decoded_tombstones,answer_rows,scan_us"
    );

    for n in sizes {
        {
            let (storage, session) = open_session().await;
            register(&session, probe_schema("c7fresh")).await;
            insert_rows(&session, "c7fresh", 1).await;
            let census = row_census(&storage).await;
            let _ = timed_scan(&session, &scan_sql("c7fresh"), 1, 1).await;
            reset();
            let rows = session
                .execute(&scan_sql("c7fresh"), &[])
                .await
                .expect("scan");
            let (decoded, matched, tombs) = read();
            let scan = timed_scan(&session, &scan_sql("c7fresh"), 1, reps).await;
            println!(
                "fresh,{n},{},{},{decoded},{matched},{tombs},{},{}",
                census.entries,
                census.tombstones,
                rows.len(),
                scan.as_micros()
            );
        }

        {
            let (storage, session) = open_session().await;
            register(&session, probe_schema("c7upd")).await;
            insert_rows(&session, "c7upd", 2).await;
            for i in 0..(n - 1) {
                session
                    .execute(
                        "UPDATE c7upd SET locale = $1 WHERE id = 'row-1'",
                        &[crate::Value::Text(format!("v{i}"))],
                    )
                    .await
                    .expect("update should commit");
            }
            let census = row_census(&storage).await;
            let _ = timed_scan(&session, &scan_sql("c7upd"), 1, 1).await;
            reset();
            let rows = session
                .execute(&scan_sql("c7upd"), &[])
                .await
                .expect("scan");
            let (decoded, matched, tombs) = read();
            let scan = timed_scan(&session, &scan_sql("c7upd"), 1, reps).await;
            println!(
                "update_churn,{n},{},{},{decoded},{matched},{tombs},{},{}",
                census.entries,
                census.tombstones,
                rows.len(),
                scan.as_micros()
            );
        }

        // churn_clean: a checkpoint between the insert and the delete, so the
        // pre-images are Clean, the elision correctly refuses, and the
        // tombstone counter has something to count. This is the arm that keeps
        // `HOT_SCAN_TOMBSTONE_ENTRIES` honest now that ordinary churn elides.
        {
            let (storage, session) = open_session().await;
            register(&session, probe_schema("c7clean")).await;
            insert_rows(&session, "c7clean", n).await;
            session
                .create_checkpoint()
                .await
                .expect("checkpoint should publish");
            delete_all_but_first(&session, "c7clean", n).await;
            let census = row_census(&storage).await;
            let _ = timed_scan(&session, &scan_sql("c7clean"), 1, 1).await;
            reset();
            let rows = session
                .execute(&scan_sql("c7clean"), &[])
                .await
                .expect("scan");
            let (decoded, matched, tombs) = read();
            let scan = timed_scan(&session, &scan_sql("c7clean"), 1, reps).await;
            println!(
                "churn_clean,{n},{},{},{decoded},{matched},{tombs},{},{}",
                census.entries,
                census.tombstones,
                rows.len(),
                scan.as_micros()
            );
            assert!(
                tombs > 0,
                "a Clean pre-image keeps its tombstones, so the tombstone census must observe them here"
            );
        }

        {
            let (storage, session) = open_session().await;
            register(&session, probe_schema("c7churn")).await;
            insert_rows(&session, "c7churn", n).await;
            delete_all_but_first(&session, "c7churn", n).await;
            let census = row_census(&storage).await;
            let _ = timed_scan(&session, &scan_sql("c7churn"), 1, 1).await;
            reset();
            let rows = session
                .execute(&scan_sql("c7churn"), &[])
                .await
                .expect("scan");
            let (decoded, matched, tombs) = read();
            let scan = timed_scan(&session, &scan_sql("c7churn"), 1, reps).await;
            println!(
                "churn,{n},{},{},{decoded},{matched},{tombs},{},{}",
                census.entries,
                census.tombstones,
                rows.len(),
                scan.as_micros()
            );
            // Engagement, not effect. Before interval-local elision landed
            // this arm carried `n - 1` tombstones and this assertion read
            // `tombs > 0`. The elision drives it to zero, which is the result,
            // so the census's own engagement is proven by the `churn_clean`
            // arm below instead - it keeps a Clean pre-image and therefore
            // keeps its tombstones.
            assert!(
                decoded > 0,
                "the churn arm must decode something, otherwise the census does not observe this site"
            );

            let _ = drop_all_tombstones(&storage).await;
            let session = reopen_session(&storage).await;
            let census = row_census(&storage).await;
            let _ = timed_scan(&session, &scan_sql("c7churn"), 1, 1).await;
            reset();
            let rows = session
                .execute(&scan_sql("c7churn"), &[])
                .await
                .expect("scan");
            let (decoded, matched, tombs) = read();
            let scan = timed_scan(&session, &scan_sql("c7churn"), 1, reps).await;
            println!(
                "churn_dropped,{n},{},{},{decoded},{matched},{tombs},{},{}",
                census.entries,
                census.tombstones,
                rows.len(),
                scan.as_micros()
            );
        }
    }
}

/// PHASE 11 - does a churning workload accumulate tombstones without bound?
///
/// `rounds` repetitions of "create `n` rows, delete all `n`", with the
/// checkpoint cadence as the experimental variable. The live row count returns
/// to its starting value at the end of every round, so anything that grows
/// with `round` is tracking *writes*, not live rows.
///
/// - `no_checkpoint` - never checkpointed. This is the case
///   `checkpoint_compacts_discharged_branch_tombstones` explicitly does not
///   claim.
/// - `checkpoint_each_round` - a checkpoint at the end of every round, which
///   is the interval shape compaction was landed for.
/// - `checkpoint_every_4` - a realistic middling cadence.
///
/// The scan answers exactly one row in every round of every arm, so `scan_us`
/// against `round` is a growth curve at constant answer size.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn hot_row_tombstone_churn_cycles() {
    let n = sizes_from_env("LIX_TOMBSTONE_SIZES", &[500])[0];
    let rounds = sizes_from_env("LIX_TOMBSTONE_ROUNDS", &[8])[0];
    let reps = reps_from_env(5);
    println!(
        "phase11 | cadence,round,row_entries,tombstones,live_entries,packed_bases,root_bases,answer_rows,scan_us,compaction"
    );

    // Cadence 9999 is the marker for the deferred-delete arm below: the rows
    // created in round r are deleted in round r+1, with a checkpoint between,
    // so every delete is against a baseline the interval already discharged.
    for (label, cadence) in [
        ("no_checkpoint", 0_usize),
        ("checkpoint_each_round", 1),
        ("checkpoint_every_4", 4),
        ("deferred_delete_ckpt", 9999),
    ] {
        let (storage, session) = open_session().await;
        let table = match cadence {
            0 => "p11never",
            1 => "p11each",
            9999 => "p11defer",
            _ => "p11four",
        };
        register(&session, probe_schema(table)).await;
        // One permanent survivor so the answer is one row in every round.
        session
            .execute(
                &format!("INSERT INTO {table} (id, locale) VALUES ('row-0', 'keep')"),
                &[],
            )
            .await
            .expect("survivor should insert");

        if cadence == 9999 {
            for round in 1..=rounds {
                let ids = (0..n)
                    .map(|i| format!("('d{round}-{i}', 'drop')"))
                    .collect::<Vec<_>>()
                    .join(",");
                session
                    .execute(
                        &format!("INSERT INTO {table} (id, locale) VALUES {ids}"),
                        &[],
                    )
                    .await
                    .expect("round insert should commit");
                session
                    .create_checkpoint()
                    .await
                    .expect("insert checkpoint should publish");
                let del = (0..n)
                    .map(|i| format!("'d{round}-{i}'"))
                    .collect::<Vec<_>>()
                    .join(",");
                session
                    .execute(&format!("DELETE FROM {table} WHERE id IN ({del})"), &[])
                    .await
                    .expect("round delete should commit");
                let before = compaction_counters();
                session
                    .create_checkpoint()
                    .await
                    .expect("delete checkpoint should publish");
                let counters = compaction_counters().since(before);
                let census = row_census(&storage).await;
                let scan = timed_scan(&session, &scan_sql(table), 1, reps).await;
                let live = session
                    .execute(&format!("SELECT id FROM {table}"), &[])
                    .await
                    .expect("collection scan should run")
                    .len();
                assert_eq!(live, 1, "every round must end with exactly one live row");
                println!(
                    "{label},{round},{},{},{},{},{},1,{},{counters}",
                    census.entries,
                    census.tombstones,
                    census.live(),
                    census.packed_bases,
                    census.root_bases,
                    scan.as_micros()
                );
            }
            continue;
        }

        for round in 1..=rounds {
            let ids = (0..n)
                .map(|i| format!("('c{round}-{i}', 'drop')"))
                .collect::<Vec<_>>()
                .join(",");
            session
                .execute(
                    &format!("INSERT INTO {table} (id, locale) VALUES {ids}"),
                    &[],
                )
                .await
                .expect("round insert should commit");
            let del = (0..n)
                .map(|i| format!("'c{round}-{i}'"))
                .collect::<Vec<_>>()
                .join(",");
            session
                .execute(&format!("DELETE FROM {table} WHERE id IN ({del})"), &[])
                .await
                .expect("round delete should commit");
            let mut counters = CompactionCounters::default();
            if cadence > 0 && round % cadence == 0 {
                let before = compaction_counters();
                session
                    .create_checkpoint()
                    .await
                    .expect("checkpoint should publish");
                counters = compaction_counters().since(before);
            }

            let census = row_census(&storage).await;
            let scan = timed_scan(&session, &scan_sql(table), 1, reps).await;
            let live = session
                .execute(&format!("SELECT id FROM {table}"), &[])
                .await
                .expect("collection scan should run")
                .len();
            assert_eq!(live, 1, "every round must end with exactly one live row");
            println!(
                "{label},{round},{},{},{},{},{},1,{},{counters}",
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

/// Removes only the tombstones of identities created and deleted inside the
/// current checkpoint interval, identified by name prefix rather than by
/// reading the private baseline codec. Returns how many were removed.
async fn drop_tombstones_named(storage: &Memory, needle: &str) -> usize {
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
        let is_tombstone = value.len() > 1 && value[1] & HEAD_VALUE_DELETED_BIT != 0;
        let names = key
            .0
            .windows(needle.len())
            .any(|window| window == needle.as_bytes());
        if is_tombstone && names {
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

/// PHASE 12 - does any reader depend on an interval-local tombstone?
///
/// An *interval-local* identity is one created and deleted inside the same
/// checkpoint interval. Phase 11 showed those are the ones that accumulate
/// forever: the checkpoint reaches the compaction route and is offered
/// nothing, because they are net-absent against the interval baseline.
///
/// The proposed upstream fix is to never publish their tombstone at all. This
/// phase costs that proposal the same way phase 4 costed the sweep - remove
/// exactly those tombstones behind the engine's back, reopen cold, and put
/// every reader the design could plausibly depend on the tombstone in front of
/// the result.
///
/// The readers, one arm each:
///
/// - the collection answer and the point answer, cold;
/// - one-argument `lix_diff`, before and after;
/// - the commit graph, which is what history is derived from;
/// - a branch forked *mid-interval*, while the identity was still alive - the
///   case where the removed key could plausibly have been serving somebody
///   else's read;
/// - a merge of that fork back into the branch whose tombstone was removed;
/// - undo.
///
/// Every arm states what it expects *before* the removal so a reader that is
/// already broken cannot read as a clean pass.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn interval_local_tombstone_has_no_dependent_reader() {
    const N: usize = 40;

    // ---------- arm 1: the local readers ----------
    {
        let (storage, session) = open_session().await;
        register(&session, probe_schema("p12row")).await;
        session
            .execute(
                "INSERT INTO p12row (id, locale) VALUES ('row-0', 'keep')",
                &[],
            )
            .await
            .expect("survivor should insert");
        // Establish the interval baseline, then create and delete inside it.
        session
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should publish");
        for i in 0..N {
            session
                .execute(
                    "INSERT INTO p12row (id, locale) VALUES ($1, 'drop')",
                    &[crate::Value::Text(format!("ephem-{i}"))],
                )
                .await
                .expect("ephemeral insert should commit");
        }
        for i in 0..N {
            session
                .execute(
                    "DELETE FROM p12row WHERE id = $1",
                    &[crate::Value::Text(format!("ephem-{i}"))],
                )
                .await
                .expect("ephemeral delete should commit");
        }

        let census = row_census(&storage).await;
        let diff_before = working_diff_rows(&session, "p12row").await;
        let live_before = session
            .execute("SELECT id FROM p12row", &[])
            .await
            .expect("collection read")
            .len();
        println!(
            "phase12 | arm=local before: entries={} tombstones={} packed_bases={} root_bases={} working_diff_rows={diff_before} live={live_before}",
            census.entries, census.tombstones, census.packed_bases, census.root_bases
        );
        assert_eq!(
            live_before, 1,
            "only the survivor is live before the removal"
        );

        let removed = drop_tombstones_named(&storage, "ephem-").await;
        let session = reopen_session(&storage).await;
        let census = row_census(&storage).await;
        let diff_after = working_diff_rows(&session, "p12row").await;
        let live_after = session
            .execute("SELECT id FROM p12row", &[])
            .await
            .expect("collection read")
            .len();
        let point = session
            .execute(
                "SELECT id FROM p12row WHERE id = $1",
                &[crate::Value::Text("ephem-0".to_string())],
            )
            .await
            .expect("point read")
            .len();
        println!(
            "phase12 | arm=local after: removed={removed} entries={} tombstones={} working_diff_rows={diff_after} live={live_after} point_hits_for_deleted={point}",
            census.entries, census.tombstones
        );

        assert_eq!(
            removed, N,
            "every interval-local tombstone should be removable"
        );
        assert_eq!(live_after, 1, "removal must not resurrect an ephemeral row");
        assert_eq!(point, 0, "a point read must not resurrect an ephemeral row");
        assert_eq!(
            diff_after, diff_before,
            "the working diff must not move when an interval-local tombstone goes"
        );

        // The checkpoint that closes the interval must still succeed and must
        // still report nothing for the vanished identities.
        session
            .create_checkpoint()
            .await
            .expect("closing checkpoint should publish after the removal");
        let live_ckpt = session
            .execute("SELECT id FROM p12row", &[])
            .await
            .expect("collection read")
            .len();
        let census = row_census(&storage).await;
        println!(
            "phase12 | arm=local after_closing_checkpoint: entries={} tombstones={} live={live_ckpt} working_diff_rows={}",
            census.entries,
            census.tombstones,
            working_diff_rows(&session, "p12row").await
        );
        assert_eq!(live_ckpt, 1, "the closing checkpoint must not resurrect");
    }

    // ---------- arm 2: a branch forked mid-interval, then merged back ----------
    // Run twice. `remove=false` is the null control: byte-identical fixture and
    // reader sequence, with the one step that executes the change omitted. A
    // merge verdict is only evidence if the control produces the same one.
    for remove in [false, true] {
        let (storage, session) = open_session().await;
        register(&session, probe_schema("p12fork")).await;
        session
            .execute(
                "INSERT INTO p12fork (id, locale) VALUES ('row-0', 'keep')",
                &[],
            )
            .await
            .expect("survivor should insert");
        session
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should publish");
        let main_branch_id = session
            .active_branch_id()
            .await
            .expect("active branch should resolve");

        for i in 0..N {
            session
                .execute(
                    "INSERT INTO p12fork (id, locale) VALUES ($1, 'drop')",
                    &[crate::Value::Text(format!("ephem-{i}"))],
                )
                .await
                .expect("ephemeral insert should commit");
        }

        // Fork here: on this branch the ephemeral rows are ALIVE.
        let fork = session
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "p12-midinterval".to_string(),
                from_commit_id: None,
            })
            .await
            .expect("branch should create");

        for i in 0..N {
            session
                .execute(
                    "DELETE FROM p12fork WHERE id = $1",
                    &[crate::Value::Text(format!("ephem-{i}"))],
                )
                .await
                .expect("ephemeral delete should commit");
        }

        let commits_before = session
            .execute("SELECT id FROM lix_commit", &[])
            .await
            .expect("commit graph read")
            .len();
        let census = row_census(&storage).await;
        println!(
            "phase12 | arm=fork(remove={remove}) before: entries={} tombstones={} root_bases={} commits={commits_before}",
            census.entries, census.tombstones, census.root_bases
        );

        // Only the ACTIVE branch's tombstones are removed. The fork keeps its
        // own scope, which is the point of the arm.
        let removed = if remove {
            drop_tombstones_named(&storage, "ephem-").await
        } else {
            0
        };
        let session = reopen_session(&storage).await;

        let live_main = session
            .execute("SELECT id FROM p12fork", &[])
            .await
            .expect("collection read")
            .len();
        session
            .switch_branch(crate::SwitchBranchOptions {
                branch_id: fork.id.clone(),
            })
            .await
            .expect("switch to the fork");
        let live_fork = session
            .execute("SELECT id FROM p12fork", &[])
            .await
            .expect("fork collection read")
            .len();
        let commits_after = session
            .execute("SELECT id FROM lix_commit", &[])
            .await
            .expect("commit graph read")
            .len();
        println!(
            "phase12 | arm=fork(remove={remove}) after: removed={removed} live_main={live_main} live_fork={live_fork} commits={commits_after}"
        );
        assert_eq!(
            removed,
            if remove { N } else { 0 },
            "the control must remove nothing and the treatment every tombstone"
        );
        assert_eq!(live_main, 1, "main must still show only the survivor");
        assert_eq!(
            live_fork,
            N + 1,
            "the fork forked while the ephemeral rows were alive and must still see them"
        );
        assert_eq!(
            commits_after, commits_before,
            "removing a serving-view tombstone must not change the commit graph"
        );

        // Merge the fork (rows alive) back into main (rows deleted, tombstones
        // gone). This is the reader with the most to lose.
        session
            .switch_branch(crate::SwitchBranchOptions {
                branch_id: main_branch_id,
            })
            .await
            .expect("switch back to main");
        let merged = session
            .merge_branch(crate::MergeBranchOptions {
                source_branch_id: fork.id.clone(),
            })
            .await;
        let live_merged = session
            .execute("SELECT id FROM p12fork", &[])
            .await
            .expect("post-merge collection read")
            .len();
        println!(
            "phase12 | arm=fork(remove={remove}) merge: ok={} live_after_merge={live_merged}",
            merged.is_ok()
        );
        merged.expect("merge must not error after the tombstones are gone");
    }

    // ---------- arm 3: undo, across the removal ----------
    for remove in [false, true] {
        let (storage, session) = open_session().await;
        register(&session, probe_schema("p12undo")).await;
        session
            .execute(
                "INSERT INTO p12undo (id, locale) VALUES ('row-0', 'keep')",
                &[],
            )
            .await
            .expect("survivor should insert");
        session
            .create_checkpoint()
            .await
            .expect("baseline checkpoint should publish");
        session
            .execute(
                "INSERT INTO p12undo (id, locale) VALUES ('ephem-0', 'drop')",
                &[],
            )
            .await
            .expect("ephemeral insert should commit");
        session
            .execute("DELETE FROM p12undo WHERE id = 'ephem-0'", &[])
            .await
            .expect("ephemeral delete should commit");

        let removed = if remove {
            drop_tombstones_named(&storage, "ephem-").await
        } else {
            0
        };
        let session = reopen_session(&storage).await;
        session.undo().await.expect("undo should run");
        let live = session
            .execute("SELECT id FROM p12undo", &[])
            .await
            .expect("post-undo collection read")
            .len();
        println!("phase12 | arm=undo(remove={remove}): removed={removed} live_after_undo={live}");
        assert_eq!(
            live, 2,
            "undo of the delete must restore the ephemeral row even with its tombstone gone"
        );
    }
}

/// Engagement for the interval-local elision route, read from inside it.
#[derive(Clone, Copy, Debug, Default)]
struct ElisionCounters {
    routes: u64,
    offered: u64,
    candidates: u64,
    elided: u64,
}

fn elision_counters() -> ElisionCounters {
    let load =
        |counter: &std::sync::atomic::AtomicU64| counter.load(std::sync::atomic::Ordering::Relaxed);
    ElisionCounters {
        routes: load(&crate::hot_state::INTERVAL_LOCAL_TOMBSTONE_ROUTES),
        offered: load(&crate::hot_state::INTERVAL_LOCAL_TOMBSTONE_OFFERED),
        candidates: load(&crate::hot_state::INTERVAL_LOCAL_TOMBSTONE_CANDIDATES),
        elided: load(&crate::hot_state::INTERVAL_LOCAL_TOMBSTONE_ELIDED),
    }
}

impl ElisionCounters {
    fn since(self, before: Self) -> Self {
        Self {
            routes: self.routes - before.routes,
            offered: self.offered - before.offered,
            candidates: self.candidates - before.candidates,
            elided: self.elided - before.elided,
        }
    }
}

impl std::fmt::Display for ElisionCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "routes={} offered={} candidates={} elided={}",
            self.routes, self.offered, self.candidates, self.elided
        )
    }
}

/// PHASE 13 — a net-zero identity remains a certified tombstone until the
/// checkpoint retires its dirty-index entry.
///
/// Phase 11 measured why this is needed: with deletes confined to an interval
/// the checkpoint's compaction route runs and is offered nothing
/// (`routes=1 offered=0`), so tombstones accumulate 1:1 with deletes forever.
/// Phase 12 predated the certified HOT working-diff reader. That reader now
/// deliberately depends on a primary row for every dirty-index identity so
/// missing data cannot be mistaken for an intentional net-zero deletion.
///
/// Engagement first, because every gate on this route is conservative and the
/// failure mode is silent inertness.
#[tokio::test]
async fn interval_local_delete_retains_certified_tombstone_until_checkpoint() {
    const N: usize = 200;
    let (storage, session) = open_session().await;
    register(&session, probe_schema("p13row")).await;
    session
        .execute(
            "INSERT INTO p13row (id, locale) VALUES ('row-0', 'keep')",
            &[],
        )
        .await
        .expect("survivor should insert");
    // Open an interval. Everything below is created and deleted inside it.
    session
        .create_checkpoint()
        .await
        .expect("baseline checkpoint should publish");

    let before = elision_counters();
    insert_rows_named(&session, "p13row", "ephem", N).await;
    let after_insert = row_census(&storage).await;
    delete_rows_named(&session, "p13row", "ephem", N).await;
    let counters = elision_counters().since(before);
    let census = row_census(&storage).await;

    println!(
        "phase13 | after_insert entries={} tombstones={} | after_delete entries={} tombstones={} packed={} root={} {counters}",
        after_insert.entries,
        after_insert.tombstones,
        census.entries,
        census.tombstones,
        census.packed_bases,
        census.root_bases
    );

    // Both counter assertions are `>=`, the only bleed-safe direction for a
    // process-global counter. The effect they predict is asserted immediately
    // below from the space footprint, which is local to this fixture.
    assert!(
        counters.candidates >= N as u64,
        "every interval-local delete must reach the elision route, saw {}",
        counters.candidates
    );
    assert!(
        counters.elided >= N as u64,
        "every gate must clear for a branch-local schema with no base, saw {}",
        counters.elided
    );
    assert_eq!(
        census.tombstones, N,
        "every dirty-index identity must retain an authoritative primary tombstone"
    );
    assert_eq!(
        census.entries, after_insert.entries,
        "net-zero churn may not create an unauthenticated hole in the HOT primary plane"
    );

    // The answers, cold, so nothing passes off a warm cache.
    let session = reopen_session(&storage).await;
    let live = session
        .execute("SELECT id FROM p13row", &[])
        .await
        .expect("collection scan should run");
    assert_eq!(live.len(), 1, "eliding must not resurrect a deleted row");
    let point = session
        .execute(
            "SELECT id FROM p13row WHERE id = $1",
            &[crate::Value::Text("ephem-0".to_string())],
        )
        .await
        .expect("point read should run");
    assert_eq!(point.len(), 0, "a point read must not resurrect either");
    assert_eq!(
        working_diff_rows(&session, "p13row").await,
        0,
        "an identity created and deleted in one interval owes the working diff nothing"
    );
    session
        .create_checkpoint()
        .await
        .expect("closing checkpoint should publish");
    let live = session
        .execute("SELECT id FROM p13row", &[])
        .await
        .expect("collection scan should run");
    assert_eq!(live.len(), 1, "the closing checkpoint must not resurrect");
}

/// INVERSION 1 — the rule must refuse when the pre-image predates the interval.
///
/// Same fixture shape, one difference: the rows are checkpointed *before* they
/// are deleted, so their pre-image carries `Clean` rather than `BeforeAbsent`.
/// Their delete is observable against the interval baseline and the tombstone
/// is load-bearing until the next checkpoint discharges it. A rule that
/// admitted this case would drop a row whose before-image is still owed.
#[tokio::test]
async fn clean_pre_image_delete_still_publishes_a_tombstone() {
    const N: usize = 200;
    let (storage, session) = open_session().await;
    register(&session, probe_schema("p13cln")).await;
    insert_rows_named(&session, "p13cln", "keeprow", N).await;
    // This checkpoint is what makes the pre-images `Clean`.
    session
        .create_checkpoint()
        .await
        .expect("baseline checkpoint should publish");

    let before = elision_counters();
    delete_rows_named(&session, "p13cln", "keeprow", N).await;
    let counters = elision_counters().since(before);
    let census = row_census(&storage).await;
    println!(
        "phase13 | inversion=clean_pre_image entries={} tombstones={} working_diff_rows={} {counters}",
        census.entries,
        census.tombstones,
        working_diff_rows(&session, "p13cln").await
    );

    // The refusal is asserted from the SPACE FOOTPRINT, which is local to this
    // fixture, and never from the elision counters, which are process-global:
    // in the parallel suite a concurrent test's publications land in them, and
    // an `assert_eq!(elided, 0)` here read 251 for a fixture that offered 50.
    // Only the `>=` direction of a global counter is safe, because bleed can
    // only inflate it - and `>=` cannot express a refusal. The tombstones
    // surviving in this collection is the refusal.
    assert_eq!(
        census.tombstones, N,
        "the delete of a checkpointed row must still publish its tombstone"
    );
    assert_eq!(
        working_diff_rows(&session, "p13cln").await,
        N,
        "and the working diff must report every one of those deletes"
    );
}

/// INVERSION 2 — a forked local overlay retains the same certified tombstone.
///
/// Composite commits no longer encode global inheritance as a HOT generation
/// base. Rows inserted and deleted entirely after the local checkpoint are
/// They are absent from both the local overlay and pinned global base, but the
/// dirty index still names them until checkpoint. The primary tombstone is the
/// fail-closed proof that this absence is intentional.
#[tokio::test]
async fn interval_local_delete_over_a_base_retains_certified_tombstone() {
    const N: usize = 50;
    let (storage, session) = open_session().await;
    register(&session, probe_schema("p13base")).await;
    insert_rows_named(&session, "p13base", "based", N).await;
    session
        .create_checkpoint()
        .await
        .expect("baseline checkpoint should publish");
    let branch = session
        .create_branch(crate::CreateBranchOptions {
            id: None,
            name: "p13-based".to_string(),
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

    let before = elision_counters();
    insert_rows_named(&session, "p13base", "ephem", N).await;
    delete_rows_named(&session, "p13base", "ephem", N).await;
    let counters = elision_counters().since(before);
    let census = row_census(&storage).await;
    println!(
        "phase13 | inversion=has_base entries={} tombstones={} packed={} root={} {counters}",
        census.entries, census.tombstones, census.packed_bases, census.root_bases
    );

    // Engagement in the `>=` direction only - safe under counter bleed,
    // because a concurrent test can only inflate it. It establishes that this
    // fixture exercised the elision route; the local footprint below proves
    // that a logical commit base is not mistaken for a physical HOT base.
    assert!(
        counters.candidates >= N as u64,
        "the deltas must reach the route, or the refusal below is vacuous, saw {}",
        counters.candidates
    );
    assert_eq!(
        census.tombstones, N,
        "a pinned commit base may not weaken the dirty-index primary-row invariant"
    );

    let session = reopen_session(&storage).await;
    let live = session
        .execute("SELECT id FROM p13base", &[])
        .await
        .expect("collection scan should run");
    assert_eq!(
        live.len(),
        N,
        "the based rows must survive, the ephemerals must not"
    );
}

/// Named-row helpers, so an arm can create and delete a cohort without
/// disturbing the survivors the assertions count.
async fn insert_rows_named(
    session: &SessionContext<Memory>,
    table: &str,
    prefix: &str,
    count: usize,
) {
    let mut index = 0;
    while index < count {
        let end = (index + CHUNK).min(count);
        let values = (index..end)
            .map(|i| format!("('{prefix}-{i}', 'drop')"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(
                &format!("INSERT INTO {table} (id, locale) VALUES {values}"),
                &[],
            )
            .await
            .expect("named rows should insert");
        index = end;
    }
}

async fn delete_rows_named(
    session: &SessionContext<Memory>,
    table: &str,
    prefix: &str,
    count: usize,
) {
    let mut index = 0;
    while index < count {
        let end = (index + CHUNK).min(count);
        let ids = (index..end)
            .map(|i| format!("'{prefix}-{i}'"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(&format!("DELETE FROM {table} WHERE id IN ({ids})"), &[])
            .await
            .expect("named bulk delete should run");
        index = end;
    }
}
