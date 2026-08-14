//! Commit-root replay scope and cost attribution (experiment AA).
//!
//! Tracked-state root materialization is bursty: ordinary commits are
//! "rootless" bounded-replay layouts and only every
//! `COMMIT_STATE_MAX_REPLAY_DEPTH`-th commit closes the interval by
//! materializing a durable root. This example drives N ordinary SQL commits
//! through the real `SessionContext` commit path and reports, per boundary,
//! how many ancestor commits that root materialization replayed.
//!
//! If the replay resumed from the previous materialized root the per-boundary
//! replay set is a constant (the interval depth). If it restarts from genesis
//! the replay set grows linearly with commit count and total replay work is
//! quadratic. The printed `plans_per_boundary` row is the distinguishing
//! measurement.
//!
//! Build with `--features root-replay-trace` to additionally attribute the
//! per-replayed-commit cost across storage read / decode / encode / hash.
//!
//! Usage: `expaa_replay_scope [commits] [rows_per_commit]`
//! (defaults: 200 commits, 10 rows).

use std::time::{Duration, Instant};

use lix::Value;
use lix::storage::Storage;
use lix::storage_bench::{
    RootReplayCostBucket, root_replay_trace_enabled, take_root_replay_accounting,
    take_root_replay_cost_attribution,
};
use lix::{Lix, open_lix};
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
    // Checkpoints re-home live rows onto one commit and parent the previous
    // checkpoint directly, so they are a candidate natural resume point. Pass a
    // non-zero interval to test whether they bound the replay set on their own.
    let checkpoint_every = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    assert!(commits >= 2, "need at least two commits to measure growth");

    let directory = tempfile::tempdir().expect("create RocksDB directory");
    let storage = RocksDB::open(directory.path()).expect("open RocksDB");
    open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize repository");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open lix");
    let session = lix.open_another_session().await.expect("open workspace");
    register_schema(&session).await;

    // Discard setup accounting so the reported numbers cover only the measured
    // history.
    let _ = take_root_replay_accounting();
    let _ = take_root_replay_cost_attribution();

    let mut latencies = Vec::with_capacity(commits);
    let mut checkpoints = 0_usize;
    let run_start = Instant::now();
    for index in 0..commits {
        let start = Instant::now();
        commit_batch(&session, index, rows_per_commit).await;
        latencies.push(start.elapsed());
        if checkpoint_every > 0 && (index + 1) % checkpoint_every == 0 {
            session
                .create_checkpoint()
                .await
                .expect("checkpoint should publish");
            checkpoints += 1;
        }
    }
    let wall = run_start.elapsed();

    let accounting = take_root_replay_accounting();
    let attribution = take_root_replay_cost_attribution();

    println!(
        "expAA_replay_scope commits={commits} rows_per_commit={rows_per_commit} checkpoint_every={checkpoint_every} checkpoints={checkpoints}"
    );
    println!("wall_ms {:.1}", wall.as_secs_f64() * 1000.0);
    println!(
        "boundaries {} plans_loaded {} plans_staged {} max_plans_in_one_boundary {}",
        accounting.boundaries,
        accounting.plans_loaded,
        accounting.plans_staged,
        accounting.max_plans_in_one_boundary
    );
    println!(
        "available_root_probes {} available_root_hits {}",
        accounting.available_root_probes, accounting.available_root_hits
    );
    println!(
        "plan_load_ms {:.1} stage_ms {:.1} replay_share_of_wall {:.1}%",
        accounting.plan_load_nanos as f64 / 1e6,
        accounting.stage_nanos as f64 / 1e6,
        (accounting.plan_load_nanos + accounting.stage_nanos) as f64 / wall.as_nanos() as f64
            * 100.0
    );
    println!(
        "plans_per_boundary {}",
        accounting
            .plans_per_boundary
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );

    // Commit latency at the burst boundary versus everywhere else.
    let boundary_indices = top_latency_indices(&latencies, accounting.boundaries as usize);
    println!(
        "slowest_commit_indices {}",
        boundary_indices
            .iter()
            .map(|(index, duration)| format!("{index}:{:.1}ms", duration.as_secs_f64() * 1000.0))
            .collect::<Vec<_>>()
            .join(" ")
    );
    let mut sorted = latencies.clone();
    sorted.sort_unstable();
    println!(
        "commit_latency_ms median {:.2} p95 {:.2} max {:.2}",
        sorted[sorted.len() / 2].as_secs_f64() * 1000.0,
        sorted[sorted.len() * 95 / 100].as_secs_f64() * 1000.0,
        sorted[sorted.len() - 1].as_secs_f64() * 1000.0
    );

    if root_replay_trace_enabled() {
        println!(
            "{:<14} {:>14} {:>14} {:>10} {:>14} {:>14}",
            "phase", "replay_ms", "total_ms", "replay_%", "replay_bytes", "replay_calls"
        );
        for (name, bucket) in [
            ("storage_read", attribution.storage_read),
            ("decode", attribution.decode),
            ("encode", attribution.encode),
            ("hash", attribution.hash),
        ] {
            print_bucket(name, bucket);
        }
    } else {
        println!("cost_attribution unavailable: rebuild with --features root-replay-trace");
    }
}

fn print_bucket(name: &str, bucket: RootReplayCostBucket) {
    let share = if bucket.total_nanos == 0 {
        0.0
    } else {
        bucket.replay_nanos as f64 / bucket.total_nanos as f64 * 100.0
    };
    println!(
        "{name:<14} {:>14.1} {:>14.1} {share:>9.1}% {:>14} {:>14}",
        bucket.replay_nanos as f64 / 1e6,
        bucket.total_nanos as f64 / 1e6,
        bucket.replay_bytes,
        bucket.replay_count
    );
}

fn top_latency_indices(latencies: &[Duration], count: usize) -> Vec<(usize, Duration)> {
    let mut indexed = latencies
        .iter()
        .copied()
        .enumerate()
        .collect::<Vec<(usize, Duration)>>();
    indexed.sort_by_key(|(_, duration)| std::cmp::Reverse(*duration));
    indexed.truncate(count.max(1).min(12));
    indexed.sort_by_key(|(index, _)| *index);
    indexed
}

async fn commit_batch<S>(session: &Lix<S>, batch: usize, rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = session.begin_transaction().await.expect("begin commit");
    for index in 0..rows {
        transaction
            .execute(
                "INSERT INTO replay_scope (path, value) VALUES ($1, CAST($2 AS JSONB))",
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

async fn register_schema<S>(session: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "replay_scope",
        "columns": [
            { "name": "path", "type": "text", "nullable": false },
            { "name": "value", "type": "jsonb", "nullable": false },
        ],
        "primary_key": ["path"],
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (CAST($1 AS JSONB), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register schema");
}
