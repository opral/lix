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
        .open_workspace_session()
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
