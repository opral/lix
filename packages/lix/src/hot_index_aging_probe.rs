//! Candidate amplification in the hot declared-column index.
//!
//! Not a product module. It measures how many *candidates* a single-value
//! lookup on an indexed column resolves, versus how many rows actually match,
//! after the collection has churned inside one generation. Entries are
//! put-only and a generation spans a branch's lifetime, so the question is
//! whether the wasted candidate resolutions stay a small constant or grow.

use serde_json::json;
use std::time::{Duration, Instant};

use crate::engine::Engine;
use crate::session::SessionContext;
use crate::storage::ProjectedValue;
use crate::storage_adapter::{
    Memory, StorageAdapter, StorageAdapterRead, StorageBeginScanOptions, StoragePrefix,
    StorageReadOptions,
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
    std::env::var("LIX_INDEX_AGING_REPS")
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

/// `(witnesses, entries)` in the hot index plane.
async fn index_record_counts(storage: &Memory) -> (usize, usize) {
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("read the index plane");
    let range = StoragePrefix {
        bytes: bytes::Bytes::new(),
    }
    .to_range()
    .expect("valid empty prefix");
    let mut cursor = read
        .begin_scan(
            crate::hot_state::INDEX_SPACE,
            range,
            StorageBeginScanOptions::default(),
        )
        .await
        .expect("scan the index plane");
    let entries = cursor.collect_all().await.expect("collect index entries");
    let witnesses = entries
        .iter()
        .filter(|entry| match &entry.value {
            ProjectedValue::FullValue(bytes) => !bytes.starts_with(b"["),
            ProjectedValue::KeyOnly => true,
        })
        .count();
    (witnesses, entries.len() - witnesses)
}

fn probe_schemas(parent: &str, child: &str) -> [serde_json::Value; 2] {
    [
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": parent,
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        }),
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": child,
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "parent_id", "type": "text", "nullable": false },
                { "name": "locale", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
            "foreign_keys": [{
                "columns": ["parent_id"],
                "references": { "schema_key": parent, "columns": ["id"] }
            }],
        }),
    ]
}

async fn register(session: &SessionContext<Memory>, schemas: [serde_json::Value; 2]) {
    for schema in schemas {
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                &[crate::Value::Text(schema.to_string())],
            )
            .await
            .expect("schema should register");
    }
}

const CHUNK: usize = 250;

async fn insert_parents(session: &SessionContext<Memory>, table: &str, count: usize) {
    let mut index = 0;
    while index < count {
        let end = (index + CHUNK).min(count);
        let values = (index..end)
            .map(|i| format!("('parent-{i}')"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(&format!("INSERT INTO {table} (id) VALUES {values}"), &[])
            .await
            .expect("parents should insert");
        index = end;
    }
}

/// Inserts `count` children; child `i` points at `parent_of(i)`.
async fn insert_children(
    session: &SessionContext<Memory>,
    table: &str,
    count: usize,
    parent_of: impl Fn(usize) -> usize,
) {
    let mut index = 0;
    while index < count {
        let end = (index + CHUNK).min(count);
        let values = (index..end)
            .map(|i| {
                let parent = parent_of(i);
                format!("('child-{i}', 'parent-{parent}', 'en')")
            })
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(
                &format!(r#"INSERT INTO {table} (id, "parent_id", locale) VALUES {values}"#),
                &[],
            )
            .await
            .expect("children should insert");
        index = end;
    }
}

/// `id IN (...)` in chunks over `child-1 .. child-{count-1}`.
async fn mutate_children_except_first(
    session: &SessionContext<Memory>,
    statement: impl Fn(&str) -> String,
    count: usize,
) {
    let mut index = 1;
    while index < count {
        let end = (index + CHUNK).min(count);
        let ids = (index..end)
            .map(|i| format!("'child-{i}'"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(&statement(&ids), &[])
            .await
            .expect("bulk mutation should run");
        index = end;
    }
}

async fn timed_lookup(
    session: &SessionContext<Memory>,
    sql: &str,
    expect_rows: usize,
    reps: usize,
) -> Duration {
    let mut samples = Vec::new();
    for _ in 0..reps {
        let start = Instant::now();
        let rows = session.execute(sql, &[]).await.expect("lookup should run");
        let elapsed = start.elapsed();
        assert_eq!(
            rows.len(),
            expect_rows,
            "lookup returned the wrong row count"
        );
        samples.push(elapsed);
    }
    samples.sort();
    samples[samples.len() / 2]
}

/// READ PATH. One live match at `parent-0` in every arm; only the number of
/// stale candidates in that bucket differs.
///
/// * `aged`   — N children created at `parent-0`, then N-1 deleted. Live
///   collection is 1 row. Candidates at `parent-0` are N.
/// * `fresh`  — 1 child created at `parent-0`. Live collection is 1 row,
///   candidates 1. This is the control: identical answer, identical live
///   state, no stale candidates.
/// * `moved`  — N children created at `parent-0`, then N-1 repointed at
///   `parent-1`. Live collection is N rows, candidates at `parent-0` are N.
///   Its `locale` lookup is the unindexed comparator: a full scan of the same
///   N live rows.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn hot_index_read_path_candidate_amplification() {
    let sizes = sizes_from_env("LIX_INDEX_AGING_SIZES", &[100, 1_000, 10_000]);
    let reps = reps_from_env(5);
    println!("read path | arm,n,index_entries,live_rows,answer_rows,median_us,scan_comparator_us");
    for n in sizes {
        // ---- aged arm: N candidates, 1 live row ----
        {
            let (storage, session) = open_session().await;
            register(&session, probe_schemas("agedp", "agedc")).await;
            insert_parents(&session, "agedp", 2).await;
            insert_children(&session, "agedc", n, |_| 0).await;
            mutate_children_except_first(
                &session,
                |ids| format!("DELETE FROM agedc WHERE id IN ({ids})"),
                n,
            )
            .await;
            let (_, entries) = index_record_counts(&storage).await;
            let median = timed_lookup(
                &session,
                r#"SELECT id FROM agedc WHERE "parent_id" = 'parent-0'"#,
                1,
                reps,
            )
            .await;
            // Unindexed comparator on the same churned collection: equality
            // on a column the schema does not declare keeps the ordinary
            // scan, so whatever the 9,999 tombstones cost shows up here and
            // not in the indexed number alone.
            let scan = timed_lookup(
                &session,
                "SELECT id FROM agedc WHERE locale = 'nomatch'",
                0,
                reps,
            )
            .await;
            println!(
                "aged,{n},{entries},1,1,{},{}",
                median.as_micros(),
                scan.as_micros()
            );
        }
        // ---- fresh control: 1 candidate, 1 live row ----
        {
            let (storage, session) = open_session().await;
            register(&session, probe_schemas("freshp", "freshc")).await;
            insert_parents(&session, "freshp", 2).await;
            insert_children(&session, "freshc", 1, |_| 0).await;
            let (_, entries) = index_record_counts(&storage).await;
            let median = timed_lookup(
                &session,
                r#"SELECT id FROM freshc WHERE "parent_id" = 'parent-0'"#,
                1,
                reps,
            )
            .await;
            println!("fresh,{n},{entries},1,1,{},-", median.as_micros());
        }
        // ---- moved arm: N candidates, 1 live match, N live rows ----
        {
            let (storage, session) = open_session().await;
            register(&session, probe_schemas("movedp", "movedc")).await;
            insert_parents(&session, "movedp", 2).await;
            insert_children(&session, "movedc", n, |_| 0).await;
            mutate_children_except_first(
                &session,
                |ids| format!(r#"UPDATE movedc SET "parent_id" = 'parent-1' WHERE id IN ({ids})"#),
                n,
            )
            .await;
            let (_, entries) = index_record_counts(&storage).await;
            let median = timed_lookup(
                &session,
                r#"SELECT id FROM movedc WHERE "parent_id" = 'parent-0'"#,
                1,
                reps,
            )
            .await;
            // Unindexed comparator: same collection, equality on a column the
            // schema does not declare, so the engine keeps its ordinary scan.
            let scan = timed_lookup(
                &session,
                r#"SELECT id FROM movedc WHERE locale = 'nomatch'"#,
                0,
                reps,
            )
            .await;
            println!(
                "moved,{n},{entries},{n},1,{},{}",
                median.as_micros(),
                scan.as_micros()
            );
        }
    }
}

fn unique_probe_schema(key: &str) -> serde_json::Value {
    json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": key,
        "columns": [
            { "name": "id", "type": "text", "nullable": false },
            { "name": "slug", "type": "text", "nullable": false },
            { "name": "tag", "type": "text", "nullable": false },
        ],
        "primary_key": ["id"],
        "unique": [["slug"]],
    })
}

/// A composite unique group: `declared_column_probe` declines composite
/// groups, so this collection keeps the scan route on the write path.
fn composite_unique_schema(key: &str) -> serde_json::Value {
    json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": key,
        "columns": [
            { "name": "id", "type": "text", "nullable": false },
            { "name": "slug", "type": "text", "nullable": false },
            { "name": "tag", "type": "text", "nullable": false },
        ],
        "primary_key": ["id"],
        "unique": [["slug", "tag"]],
    })
}

/// WRITE PATH. `validate_committed_unique_constraints` probes the same plane
/// for single-column unique groups, so every insert re-resolves whatever
/// candidates have accumulated under the value being inserted.
///
/// The loop recreates one logical slot: insert a fresh row holding
/// `slug='dup'`, delete it, repeat. Each iteration leaves one more permanent
/// candidate in the `dup` bucket while the live collection stays at one row.
/// Insert latency is sampled at the requested checkpoints.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn hot_index_write_path_unique_probe_amplification() {
    write_path_churn("uniqprobe", unique_probe_schema("uniqprobe")).await;
}

/// Control for the write-path probe. The unique group is composite, so
/// `declared_column_probe` declines and the same churn runs on the collection
/// scan route. Whatever the accumulated tombstones cost the scan appears here;
/// the difference between the two runs is what the index plane adds.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn hot_index_write_path_composite_unique_control() {
    write_path_churn("compprobe", composite_unique_schema("compprobe")).await;
}

async fn write_path_churn(table: &str, schema: serde_json::Value) {
    let checkpoints = sizes_from_env("LIX_INDEX_AGING_WRITE_CHECKPOINTS", &[10, 100, 500, 1_000]);
    let max = *checkpoints.iter().max().expect("at least one checkpoint");
    let (storage, session) = open_session().await;
    session
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[crate::Value::Text(schema.to_string())],
        )
        .await
        .expect("schema should register");

    println!(
        "write path {table} | accumulated_candidates,index_entries,live_rows,median_insert_us"
    );
    let reps = reps_from_env(5);
    let mut next = 0usize;
    let mut samples: Vec<Duration> = Vec::new();
    for iteration in 0..=max {
        let sampling = next < checkpoints.len() && iteration + reps > checkpoints[next];
        let start = Instant::now();
        session
            .execute(
                &format!("INSERT INTO {table} (id, slug, tag) VALUES ($1, 'dup', 't')"),
                &[crate::Value::Text(format!("row-{iteration}"))],
            )
            .await
            .expect("insert should succeed");
        if sampling {
            samples.push(start.elapsed());
        }
        if next < checkpoints.len() && iteration == checkpoints[next] {
            let (_, entries) = index_record_counts(&storage).await;
            samples.sort();
            let median = samples[samples.len() / 2];
            println!("{iteration},{entries},1,{}", median.as_micros());
            samples.clear();
            next += 1;
        }
        session
            .execute(
                &format!("DELETE FROM {table} WHERE id = $1"),
                &[crate::Value::Text(format!("row-{iteration}"))],
            )
            .await
            .expect("delete should succeed");
    }
}
