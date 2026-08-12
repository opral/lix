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
    StorageBeginScanOptions, StoragePrefix, StorageReadOptions, StorageSpace,
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
        .open_workspace_session()
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
    RowCensus {
        entries: rows.len(),
        tombstones,
        packed_bases,
        root_bases,
    }
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
