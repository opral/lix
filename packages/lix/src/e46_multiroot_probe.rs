//! E46 — a commit lane with more than one tracked root.
//!
//! Every lane the round routinely measures commits to exactly one branch, so
//! `tracked_roots.len() == 1` and any cost of the form O(roots × rows) is flat
//! by construction. This probe builds commits that publish `B` roots in one
//! atomic transaction and carry `U` untracked rows, so the slope in both
//! directions is observable.
//!
//! Roots come from `finalize_commit_rows`, which emits one `PendingTrackedRoot`
//! per branch in `commit_change_refs_by_branch`. Writing one tracked row to
//! each of `B` pre-created branches inside one explicit transaction therefore
//! produces exactly `B` roots in one commit.
//!
//! Storage is `Memory`: the claim under test is CPU-side scaling in
//! `stage_tracked_head`, and a backend would add I/O noise without changing the
//! slope. Absolute numbers here are not comparable to the RocksDB lanes.

use crate::common::types::Value;
use crate::engine::Engine;
use crate::session::SessionContext;
use crate::storage_adapter::Memory;

/// One measured configuration.
#[derive(Debug, Clone, Copy)]
pub struct MultiRootPoint {
    pub branches: usize,
    pub untracked_rows: usize,
    pub commits: usize,
    pub nanos_per_commit: u64,
}

fn branch_id(index: usize) -> String {
    // Canonical UUID shape; branch ids are validated as canonical UUIDs.
    format!("01920000-0000-7000-8000-{index:012x}")
}

/// Runs one `(branches, untracked_rows)` configuration and returns its cost per
/// commit. Each commit is one explicit transaction publishing `branches` roots.
pub async fn run_point(
    branches: usize,
    untracked_rows: usize,
    commits: usize,
) -> Result<MultiRootPoint, crate::LixError> {
    let storage = Memory::new();
    let receipt = Engine::initialize(storage.clone()).await?;
    let engine = Engine::new(storage.clone()).await?;
    let session = engine.open_session_at(&receipt.main_branch_id).await?;

    // Branch 0 is the session's own branch; the rest are created up front so
    // the timed loop only pays for the commit, never for branch creation.
    let mut branch_ids = vec![receipt.main_branch_id.clone()];
    for index in 1..branches {
        let id = branch_id(index);
        session
            .execute(
                "INSERT INTO lix_branch (id, name) VALUES ($1, $2)",
                &[Value::Text(id.clone()), Value::Text(format!("e46-{index}"))],
            )
            .await?;
        branch_ids.push(id);
    }

    // Warm the commit path once so the measured loop is not paying first-commit
    // catalog and packed-base costs.
    for round in 0..2 {
        commit_once(&session, &branch_ids, untracked_rows, round).await?;
    }

    let started = std::time::Instant::now();
    for round in 0..commits {
        commit_once(&session, &branch_ids, untracked_rows, round + 2).await?;
    }
    let elapsed = started.elapsed();

    Ok(MultiRootPoint {
        branches,
        untracked_rows,
        commits,
        nanos_per_commit: (elapsed.as_nanos() / commits.max(1) as u128) as u64,
    })
}

/// One transaction: one tracked row per branch (→ one root per branch), plus
/// `untracked_rows` untracked rows spread over the same branches.
async fn commit_once(
    session: &SessionContext<Memory>,
    branch_ids: &[String],
    untracked_rows: usize,
    round: usize,
) -> Result<(), crate::LixError> {
    let mut transaction = session.begin_transaction().await?;
    for (index, id) in branch_ids.iter().enumerate() {
        transaction
            .execute(
                "INSERT INTO lix_key_value_by_branch \
                 (key, value, lixcol_branch_id, lixcol_global) \
                 VALUES ($1, $2, $3, false) \
                 ON CONFLICT (key, lixcol_branch_id) \
                 DO UPDATE SET value = excluded.value",
                &[
                    Value::Text(format!("e46-tracked-{index}")),
                    Value::Text(format!("\"round-{round}\"")),
                    Value::Text(id.clone()),
                ],
            )
            .await?;
    }
    for row in 0..untracked_rows {
        let id = &branch_ids[row % branch_ids.len()];
        transaction
            .execute(
                "INSERT INTO lix_key_value_by_branch \
                 (key, value, lixcol_branch_id, lixcol_global, lixcol_untracked) \
                 VALUES ($1, $2, $3, false, true) \
                 ON CONFLICT (key, lixcol_branch_id) \
                 DO UPDATE SET value = excluded.value",
                &[
                    Value::Text(format!("e46-untracked-{row}")),
                    Value::Text(format!("\"round-{round}\"")),
                    Value::Text(id.clone()),
                ],
            )
            .await?;
    }
    transaction.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Prints the curve. Run with:
    /// `cargo test --release -p lix --features storage-benches e46_multiroot_curve -- --nocapture --ignored`
    #[tokio::test]
    #[ignore = "measurement lane, not a correctness test"]
    async fn e46_multiroot_curve() {
        let commits = std::env::var("E46_COMMITS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(200_usize);
        println!("branches,untracked_rows,commits,ns_per_commit");
        for branches in [1_usize, 2, 4, 8, 16] {
            for untracked_rows in [0_usize, 10, 100] {
                let point = run_point(branches, untracked_rows, commits)
                    .await
                    .expect("multi-root point should run");
                println!(
                    "{},{},{},{}",
                    point.branches, point.untracked_rows, point.commits, point.nanos_per_commit
                );
            }
        }
    }

    /// Establishes what the lane *reaches* before anything is timed on it.
    #[tokio::test]
    #[ignore = "measurement lane, not a correctness test"]
    async fn e46_multiroot_engagement() {
        for branches in [1_usize, 16] {
            run_point(branches, 100, 20)
                .await
                .expect("engagement point should run");
            println!("--- engagement after branches={branches}");
        }
    }
}
