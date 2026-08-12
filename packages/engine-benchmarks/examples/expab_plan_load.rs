//! Commit-root rebuild plan-load attribution (experiment AB).
//!
//! Experiment AA measured that 83% of commit-root replay time is spent inside
//! `load_rebuild_plans_to_nearest_available_root`, at ~76 µs per replayed
//! ancestor, and called that "plan loading". This example splits one plan load
//! into its phases and, per phase, separates time spent inside the storage
//! adapter's `get_many` boundary (I/O) from everything else (decode +
//! allocation + setup). It also counts physical read batches and keys per
//! phase, so read amplification is a counter rather than an inference.
//!
//! Phases (see `lix::storage_bench::PlanLoadPhase`):
//!   avail_probe    `load_available_root` — bounded durable-root probe
//!   commit_record  `ChangelogReader::load_commits` for the replayed commit
//!   replay_state   `load_point_replay_commit_state` — header + inventory
//!   delta_segments mutation-directory routing + packed segment reads + decode
//!   collect        owned-key materialization of the decoded batch
//!
//! `delta_segments` nests `replay_state`, so the report prints an exclusive
//! wall column as well as the raw guard wall.
//!
//! The second section answers "is plan loading hot outside replay" by running
//! checkpoint / history / undo / merge / GC against the same repository and
//! reporting the same counters for each.
//!
//! Build with `--features root-replay-trace`; without it the counters are
//! compiled out and the example says so.
//!
//! Usage: `expab_plan_load [commits] [rows_per_commit]`
//! (defaults: 200 commits, 10 rows).

use std::time::Instant;

use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::{
    PLAN_LOAD_PHASE_NAMES, PlanLoadAttribution, collect_repository_gc_for_bench,
    plan_load_trace_enabled, take_plan_load_attribution, take_root_replay_accounting,
};
use lix::{CreateBranchOptions, MergeBranchOptions, Value};
use lix_storage_rocksdb::RocksDB;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let commits = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(200);
    let rows_per_commit = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10);
    assert!(commits >= 2, "need at least two commits to measure replay");

    let directory = tempfile::tempdir().expect("create RocksDB directory");
    let storage = RocksDB::open(directory.path()).expect("open RocksDB");
    Engine::initialize(storage.clone())
        .await
        .expect("initialize repository");
    let engine = Engine::new(storage.clone()).await.expect("open engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open workspace");
    register_schema(&session).await;

    // Discard setup accounting so the reported numbers cover only the measured
    // history.
    let _ = take_root_replay_accounting();
    let _ = take_plan_load_attribution();

    println!(
        "expAB_plan_load commits={commits} rows_per_commit={rows_per_commit} trace={}",
        plan_load_trace_enabled()
    );

    let run_start = Instant::now();
    for index in 0..commits {
        commit_batch(&session, index, rows_per_commit).await;
    }
    let wall = run_start.elapsed();

    let accounting = take_root_replay_accounting();
    let attribution = take_plan_load_attribution();
    println!(
        "wall_ms {:.1} boundaries {} plans_loaded {} plans_staged {} max_plans_in_one_boundary {}",
        wall.as_secs_f64() * 1000.0,
        accounting.boundaries,
        accounting.plans_loaded,
        accounting.plans_staged,
        accounting.max_plans_in_one_boundary
    );
    println!(
        "plan_load_ms {:.1} stage_ms {:.1}",
        accounting.plan_load_nanos as f64 / 1e6,
        accounting.stage_nanos as f64 / 1e6
    );
    report("commit_loop", &attribution);

    // ---- Is plan loading hot outside replay? -------------------------------
    let _ = take_plan_load_attribution();
    let checkpoint_start = Instant::now();
    session
        .create_checkpoint()
        .await
        .expect("checkpoint should publish");
    let checkpoint_wall = checkpoint_start.elapsed();
    let checkpoint = take_plan_load_attribution();
    println!("op checkpoint wall_ms {:.1}", checkpoint_wall.as_secs_f64() * 1000.0);
    report("checkpoint", &checkpoint);

    let history_start = Instant::now();
    let history = session
        .execute(
            "SELECT lixcol_commit_id FROM replay_scope_history LIMIT 100",
            &[],
        )
        .await;
    let history_wall = history_start.elapsed();
    match history {
        Ok(result) => println!(
            "op history wall_ms {:.1} rows {}",
            history_wall.as_secs_f64() * 1000.0,
            result.len()
        ),
        Err(error) => println!("op history unavailable: {error}"),
    }
    report("history", &take_plan_load_attribution());

    let undo_start = Instant::now();
    let undo = session.undo().await;
    let undo_wall = undo_start.elapsed();
    match undo {
        Ok(_) => println!("op undo wall_ms {:.1}", undo_wall.as_secs_f64() * 1000.0),
        Err(error) => println!("op undo unavailable: {error}"),
    }
    report("undo", &take_plan_load_attribution());

    let branch = session
        .create_branch(CreateBranchOptions {
            id: None,
            name: "expab-branch".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("create branch")
        .id;
    let branch_session = engine
        .open_session(branch.clone())
        .await
        .expect("open branch session");
    for index in 0..8 {
        commit_batch(&branch_session, commits + index, rows_per_commit).await;
    }
    let _ = take_plan_load_attribution();
    let merge_start = Instant::now();
    session
        .merge_branch(MergeBranchOptions {
            source_branch_id: branch.clone(),
        })
        .await
        .expect("merge branch");
    let merge_wall = merge_start.elapsed();
    println!("op merge wall_ms {:.1}", merge_wall.as_secs_f64() * 1000.0);
    report("merge", &take_plan_load_attribution());

    let adapter = StorageAdapter::new(storage.clone());
    let gc_start = Instant::now();
    collect_repository_gc_for_bench(&adapter)
        .await
        .expect("collect repository GC");
    let gc_wall = gc_start.elapsed();
    println!("op gc wall_ms {:.1}", gc_wall.as_secs_f64() * 1000.0);
    report("gc", &take_plan_load_attribution());
}

fn report(label: &str, attribution: &PlanLoadAttribution) {
    if !plan_load_trace_enabled() {
        println!("{label}: attribution unavailable, rebuild with --features root-replay-trace");
        return;
    }
    println!(
        "{label}: plans {} members_decoded {} members_kept {} member_key_bytes {}",
        attribution.plans,
        attribution.members_decoded,
        attribution.members_kept,
        attribution.member_payload_bytes
    );
    // `delta_segments` encloses `replay_state`; report the exclusive wall too.
    let replay_state_wall = attribution.phases[3].wall_nanos;
    println!(
        "{label}: {:<15} {:>8} {:>12} {:>12} {:>12} {:>10} {:>8} {:>8} {:>12}",
        "phase", "entries", "wall_ms", "excl_wall_ms", "io_ms", "non_io_ms", "batches", "keys",
        "bytes"
    );
    for (index, name) in PLAN_LOAD_PHASE_NAMES.iter().enumerate() {
        let phase = attribution.phases[index];
        if phase.entries == 0 && phase.read_batches == 0 {
            continue;
        }
        let exclusive = if index == 4 {
            phase.wall_nanos.saturating_sub(replay_state_wall)
        } else {
            phase.wall_nanos
        };
        println!(
            "{label}: {name:<15} {:>8} {:>12.2} {:>12.2} {:>12.2} {:>10.2} {:>8} {:>8} {:>12}",
            phase.entries,
            phase.wall_nanos as f64 / 1e6,
            exclusive as f64 / 1e6,
            phase.io_nanos as f64 / 1e6,
            exclusive.saturating_sub(phase.io_nanos) as f64 / 1e6,
            phase.read_batches,
            phase.read_keys,
            phase.read_bytes
        );
    }
    if attribution.plans > 0 {
        let plans = attribution.plans as f64;
        let plan_phases = [1usize, 2, 3, 4, 5];
        let wall: u64 = plan_phases
            .iter()
            .map(|index| {
                if *index == 4 {
                    attribution.phases[4]
                        .wall_nanos
                        .saturating_sub(replay_state_wall)
                } else {
                    attribution.phases[*index].wall_nanos
                }
            })
            .sum();
        let io: u64 = plan_phases
            .iter()
            .map(|index| attribution.phases[*index].io_nanos)
            .sum();
        let batches: u64 = plan_phases
            .iter()
            .map(|index| attribution.phases[*index].read_batches)
            .sum();
        let keys: u64 = plan_phases
            .iter()
            .map(|index| attribution.phases[*index].read_keys)
            .sum();
        let bytes: u64 = plan_phases
            .iter()
            .map(|index| attribution.phases[*index].read_bytes)
            .sum();
        println!(
            "{label}: per_plan wall_us {:.2} io_us {:.2} non_io_us {:.2} batches {:.2} keys {:.2} bytes {:.1} members {:.2}",
            wall as f64 / plans / 1e3,
            io as f64 / plans / 1e3,
            wall.saturating_sub(io) as f64 / plans / 1e3,
            batches as f64 / plans,
            keys as f64 / plans,
            bytes as f64 / plans,
            attribution.members_decoded as f64 / plans
        );
    }
}

async fn commit_batch<S>(session: &SessionContext<S>, batch: usize, rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = session.begin_transaction().await.expect("begin commit");
    for index in 0..rows {
        transaction
            .execute(
                "INSERT INTO replay_scope (path, value) VALUES ($1, lix_json($2))",
                &[
                    Value::Text(format!("/row/{batch:08}/{index:08}")),
                    Value::Text(format!(r#"{{"batch":{batch},"index":{index}}}"#)),
                ],
            )
            .await
            .expect("insert row");
    }
    transaction.commit().await.expect("commit batch");
}

async fn register_schema<S>(session: &SessionContext<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "x-lix-key": "replay_scope",
        "x-lix-primary-key": ["/path"],
        "type": "object",
        "required": ["path", "value"],
        "properties": {
            "path": { "type": "string" },
            "value": {
                "type": ["object", "array", "string", "number", "integer", "boolean", "null"]
            }
        },
        "additionalProperties": false
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register schema");
}
