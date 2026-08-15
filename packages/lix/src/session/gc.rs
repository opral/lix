use crate::LixError;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::gc::{
    RepositoryGcPlan, load_checkpoint_gc_state, stage_checkpoint_gc_state,
    stage_repository_gc_with_preconditions,
};
use crate::storage_adapter::{
    SharedStorageAdapterRead, Storage, StorageReadOptions, StorageWriteOptions,
};
use crate::transaction::{begin_commit_boundary, commit_at_boundary};

use super::SessionContext;
use super::checkpoint::checkpoint_gc_due;

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Runs one repository-wide sweep after a checkpoint has committed.
    ///
    /// The checkpoint transaction has already atomically published both the
    /// new branch head and its rotated recovery root. This follow-up pass takes
    /// the same repository write gate as ordinary implicit writes, plans from
    /// one pinned read, and commits the entire sweep as one write set.
    async fn collect_checkpoint_garbage(&self) -> Result<Option<RepositoryGcPlan>, LixError> {
        let write_access = self.begin_session_write_access().await?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let mut gc_state = load_checkpoint_gc_state(&read).await?;
        if !checkpoint_gc_due(gc_state)? {
            return Ok(None);
        }
        let mut writes = self.storage.new_write_set();
        let mut preconditions = Vec::new();
        let plan =
            stage_repository_gc_with_preconditions(read, &mut writes, &mut preconditions).await?;
        gc_state.mark_collected(
            plan.sweep.tracked_commit_roots.len() as u64,
            plan.sweep.live_manifest_count,
        );
        stage_checkpoint_gc_state(&mut writes, &gc_state)?;
        let commit_boundary = self.transaction_commit_boundary();
        let _commit_guard = begin_commit_boundary(Some(&commit_boundary));
        let prepared_commit = self
            .storage
            .prepare_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await?;
        let stats = commit_at_boundary(Some(&commit_boundary), || async move {
            let (_, stats) = prepared_commit.commit().await?;
            Ok(stats)
        })
        .await?;
        drop(write_access);
        self.observe_invalidation.bump_if_storage_changed(&stats);
        Ok(Some(plan))
    }

    /// Persists one failed reclaim attempt so the trigger can damp its retry.
    ///
    /// Deliberately a tiny single-key write rather than part of the sweep's
    /// write set: the sweep's write set is exactly what did not commit.
    async fn record_reclaim_failure(&self) -> Result<(), LixError> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let mut gc_state = load_checkpoint_gc_state(&read).await?;
        drop(read);
        gc_state.note_reclaim_failure();
        let mut writes = self.storage.new_write_set();
        stage_checkpoint_gc_state(&mut writes, &gc_state)?;
        // Through the commit boundary like every other session write, so a
        // concurrent close cannot race the final pre-commit check.
        let commit_boundary = self.transaction_commit_boundary();
        let _commit_guard = begin_commit_boundary(Some(&commit_boundary));
        let prepared_commit = self
            .storage
            .prepare_write_set(writes, StorageWriteOptions::default())
            .await?;
        commit_at_boundary(Some(&commit_boundary), || async move {
            let (_, stats) = prepared_commit.commit().await?;
            Ok(stats)
        })
        .await?;
        Ok(())
    }

    /// Checkpoint creation must not fail merely because opportunistic cleanup
    /// could not complete. Repository-global debt is cleared only in the same
    /// atomic write as a successful sweep, so every later checkpoint retries
    /// while collection remains due.
    pub(super) async fn collect_checkpoint_garbage_best_effort(&self) {
        match self.collect_checkpoint_garbage().await {
            Ok(Some(plan)) => {
                tracing::debug!(
                    swept_commits = plan.changelog.sweep.commits.len(),
                    swept_changes = plan.changelog.sweep.changes.len(),
                    swept_tracked_roots = plan.sweep.tracked_commit_roots.len(),
                    history_manifests_missing = plan.profile.history_manifests_missing,
                    root_discovery_us = plan.profile.root_discovery_us,
                    changelog_us = plan.profile.changelog_us,
                    tracked_root_stage_us = plan.profile.tracked_root_stage_us,
                    gc_total_us = plan.profile.total_us,
                    "completed post-checkpoint garbage collection"
                );
            }
            Ok(None) => {}
            Err(error) => {
                // Persistent failure here used to be undetectable: one
                // `tracing::warn!` and no counter, invisible in production
                // without a subscriber and invisible in tests entirely. Record
                // it so the next occurrence is findable, and damp the retry so
                // a failing sweep cannot re-arm a full repository pass on every
                // checkpoint.
                reclaim_failures_total().fetch_add(1, Ordering::Relaxed);
                if let Err(record_error) = self.record_reclaim_failure().await {
                    tracing::warn!(
                        error = %record_error,
                        "could not record reclaim failure; retry damping is skipped"
                    );
                }
                tracing::warn!(
                    error = %error,
                    "post-checkpoint garbage collection failed; checkpoint remains committed"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::time::{Duration, Instant};

    use super::checkpoint_gc_due;
    use crate::changelog::CommitId;
    use crate::engine::Engine;
    use crate::gc::{load_checkpoint_gc_state, stage_repository_gc_with_preconditions};
    use crate::session::SessionContext;
    use crate::storage::Memory;
    use crate::storage_adapter::{SharedStorageAdapterRead, StorageReadOptions, StorageWriteOptions};
    use crate::{LixError, Value};

    /// Checkpoints a fresh repository must accumulate before the staleness
    /// backstop makes a sweep due. Mirrors `RECLAIM_MAX_STALENESS` in
    /// `session::checkpoint`, which is private to that module.
    ///
    /// The yield ratio can make a sweep due *earlier* than this; the backstop
    /// is the bound that holds when debt is sparse, which is the case these
    /// fixtures build (empty padding checkpoints accrue no debt at all).
    const RECLAIM_MAX_STALENESS: usize = 64;

    /// Checkpointed rounds of writes built before the repository is made
    /// legacy. Each round after the first contributes one interior commit the
    /// sweep is required to reclaim.
    const ROUNDS: usize = 6;
    const WRITES_PER_ROUND: usize = 3;

    async fn open() -> (Engine<Memory>, SessionContext<Memory>) {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("storage initializes");
        let engine = Engine::new(storage).await.expect("engine opens");
        let session = engine.open_session().await.expect("session opens");
        (engine, session)
    }

    async fn head(engine: &Engine<Memory>, branch_id: &str) -> String {
        engine
            .load_branch_head_commit_id(branch_id)
            .await
            .expect("branch head loads")
            .expect("branch head exists")
    }

    /// Which of `commit_ids` the changelog still serves.
    async fn present(session: &SessionContext<Memory>, commit_ids: &[String]) -> Vec<String> {
        let mut present = Vec::new();
        for commit_id in commit_ids {
            let result = session
                .execute(
                    "SELECT id FROM lix_commit WHERE id = $1",
                    &[Value::Text(commit_id.clone())],
                )
                .await
                .expect("commit existence query succeeds");
            if !result.is_empty() {
                present.push(commit_id.clone());
            }
        }
        present
    }


    /// End-to-end engagement for the ratio trigger's two estimates.
    ///
    /// Both are produced by the sweep and written in its write set, so
    /// asserting them off the returned plan is *not* an end-to-end check:
    /// staging, preparing and committing all sit between the plan and
    /// persistence. This reads them back out of committed storage, the same
    /// bar the history-retention fix set for the un-latch.
    #[tokio::test]
    async fn reclaim_trigger_persists_its_estimates_to_committed_storage() {
        let (_engine, session) = open().await;

        for round in 0..ROUNDS {
            for write in 0..WRITES_PER_ROUND {
                session
                    .execute(
                        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) \
                         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                        &[
                            Value::Text(format!("gc-estimate-k{write}")),
                            Value::Jsonb(json!({ "round": round, "write": write }).into()),
                        ],
                    )
                    .await
                    .expect("write commits");
            }
            session
                .create_checkpoint()
                .await
                .expect("round checkpoint succeeds");
        }

        async fn committed_state(
            session: &SessionContext<Memory>,
        ) -> crate::gc::CheckpointGcState {
            let read = SharedStorageAdapterRead::new(
                session
                    .storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("gc state read opens"),
            );
            let state = load_checkpoint_gc_state(&read)
                .await
                .expect("checkpoint gc state loads");
            drop(read);
            state
        }

        // Non-vacuity: both estimates must start unset, or the assertions
        // below could pass against a default-constructed state.
        let before = committed_state(&session).await;
        assert_eq!(
            (before.live_manifest_estimate, before.yield_per_interval_estimate),
            (0, 0),
            "estimates must be unset before any sweep, or this proves nothing"
        );

        // Empty padding accrues no debt, so the staleness backstop is what
        // makes this due -- deliberately the harder path for the estimates.
        for _ in 0..RECLAIM_MAX_STALENESS {
            session
                .create_checkpoint()
                .await
                .expect("padding checkpoint succeeds");
        }

        let plan = session
            .collect_checkpoint_garbage()
            .await
            .expect("the sweep must succeed");

        let after = committed_state(&session).await;
        let observed_live_manifest_count = if let Some(plan) = plan {
            assert!(
                plan.sweep.live_manifest_count > 0,
                "the sweep must have scanned a real inventory to report one"
            );
            plan.sweep.live_manifest_count
        } else {
            // The production checkpoint path schedules this same collection
            // best-effort. An executor-neutral worker may win the race before
            // this explicit collection call; in that case the committed
            // estimate is the observation under test.
            assert!(
                after.live_manifest_estimate > 0,
                "an automatic sweep must persist a real inventory estimate"
            );
            after.live_manifest_estimate
        };
        assert!(
            after.last_gc_sequence > 0,
            "`mark_collected` must have persisted, not merely been staged"
        );
        assert_eq!(
            after.live_manifest_estimate, observed_live_manifest_count,
            "the persisted inventory estimate must be exactly what the sweep observed"
        );
        assert_eq!(
            after.consecutive_reclaim_failures, 0,
            "a successful sweep must clear the failure damping"
        );
        assert!(
            !checkpoint_gc_due(after).expect("due predicate evaluates"),
            "a successful sweep must un-latch the trigger, not re-arm it"
        );
    }

    /// Deletes one commit's physical delta the way the sweep that shipped
    /// before the history-retention fix did, leaving the commit record itself
    /// in place. This is `pub(crate)` on purpose and stays that way: a
    /// publicly reachable way to delete a manifest is a footgun that would
    /// outlive the fixture it was added for, so this test lives in-crate
    /// rather than in the integration suite.
    async fn reclaim_history_delta_like_a_pre_fix_sweep(
        session: &SessionContext<Memory>,
        commit_id: CommitId,
    ) -> Result<(), LixError> {
        let read = session
            .storage
            .begin_read(StorageReadOptions::default())
            .await?;
        let manifest = crate::tracked_state::load_commit_state_manifest(&read, commit_id)
            .await?
            .expect("a commit on the head's first-parent chain still owns its physical delta");
        let mut writes = session.storage.new_write_set();
        crate::tracked_state::stage_delete_commit_state_manifest_for_gc(
            &read,
            &mut writes,
            commit_id,
            &manifest,
        )
        .await?;
        drop(read);
        session
            .storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await?;
        Ok(())
    }

    /// A repository whose row history a pre-fix sweep already took must
    /// still *reclaim*, not merely still *plan*.
    ///
    /// # Why a plan is not enough
    ///
    /// A failed sweep latches. `checkpoint_gc_due` derives its age limit as
    /// `CHECKPOINT_GC_MIN_AGE.max(last_gc_sequence)`, and only a successful
    /// sweep advances `last_gc_sequence` — so once a sweep starts failing, the
    /// limit freezes while `checkpoint_sequence` keeps climbing and the
    /// predicate returns true at every later checkpoint, forever. Each of those
    /// checkpoints then pays for a doomed full-repository sweep, and
    /// [`SessionContext::collect_checkpoint_garbage_best_effort`] swallows the
    /// error, so nothing surfaces. Reaching `mark_collected` is what un-latches
    /// it.
    ///
    /// `mark_collected()` is the statement immediately after the staging `?`,
    /// so a successful *plan* implies the un-latch is staged — but three
    /// fallible steps follow it (`stage_checkpoint_gc_state`,
    /// `prepare_write_set`, and the commit itself), so it does not imply the
    /// un-latch is *persisted*. This test therefore asserts the reclaim and the
    /// advanced sequence out of committed storage, not out of the plan.
    ///
    /// # What it asserts, and what it deliberately does not
    ///
    /// Interior commits — the intra-interval heads a round's checkpoint
    /// supersedes — must be gone. Checkpoint commits must not: they stay on the
    /// head's first-parent chain, and a test asserting they leave would encode
    /// a false invariant and pass for the wrong reason.
    #[tokio::test]
    async fn checkpoint_gc_reclaims_on_a_repository_already_swept_before_the_history_fix() {
        let (engine, session) = open().await;
        let branch_id = session.branch.get().expect("session branch resolves");

        // Interior commits: the head after the first write of every round
        // after the first. The round's checkpoint supersedes each one, it
        // leaves the first-parent chain, and the collector is entitled to it.
        // Round 0's is deliberately not recorded — it is the branch's oldest
        // interval anchor and the collector keeps it, so requiring its removal
        // would fail for a reason unrelated to reclaim.
        let mut interior_commits = Vec::new();
        let mut checkpoints = Vec::new();
        for round in 0..ROUNDS {
            for write in 0..WRITES_PER_ROUND {
                session
                    .execute(
                        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) \
                         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                        &[
                            Value::Text(format!("gc-legacy-k{write}")),
                            Value::Jsonb(json!({ "round": round, "write": write }).into()),
                        ],
                    )
                    .await
                    .expect("write commits");
                if write == 0 && round > 0 {
                    interior_commits.push(head(&engine, &branch_id).await);
                }
            }
            checkpoints.push(
                session
                    .create_checkpoint()
                    .await
                    .expect("round checkpoint succeeds")
                    .commit_id,
            );
        }
        assert_eq!(interior_commits.len(), ROUNDS - 1);

        // Make this a legacy repository: take the physical delta of a commit
        // that is still on the head's first-parent chain, which is exactly
        // what the pre-fix sweep did and what cannot be recomputed.
        let legacy_commit_id = checkpoints[1].clone();
        let legacy = CommitId::parse_lix(&legacy_commit_id, "legacy checkpoint commit id")
            .expect("checkpoint commit id parses");
        reclaim_history_delta_like_a_pre_fix_sweep(&session, legacy)
            .await
            .expect("the pre-fix reclaim stages and commits");

        // Cross the collection interval.
        for _ in 0..RECLAIM_MAX_STALENESS {
            session
                .create_checkpoint()
                .await
                .expect("padding checkpoint succeeds");
        }

        // `create_checkpoint` spawns the sweep, so drive one here rather than
        // racing it; an explicit call is a no-op once the debt is clear, so
        // whichever runs first, the assertions below read the same committed
        // outcome. Without the tolerance this call is what fails, loudly:
        // the sweep hard-errors on the missing manifest.
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            session
                .collect_checkpoint_garbage()
                .await
                .expect("a sweep must not fail on a repository swept before the fix");
            let remaining = present(&session, &interior_commits).await;
            if remaining.is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "checkpoint GC did not reclaim the interior commits {remaining:?}; a repository \
                 whose history a pre-fix sweep already took must still collect"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // The un-latch, read back out of committed storage rather than out of
        // the plan.
        let read = SharedStorageAdapterRead::new(
            session
                .storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("gc state read opens"),
        );
        let state = load_checkpoint_gc_state(&read)
            .await
            .expect("checkpoint gc state loads");
        drop(read);
        assert!(
            state.last_gc_sequence > 0,
            "a sweep that reclaimed must have persisted `mark_collected`; a staged-but-unpersisted \
             un-latch leaves every later checkpoint paying for a doomed sweep"
        );
        assert!(
            !checkpoint_gc_due(state).expect("due predicate evaluates"),
            "collection debt must be cleared, not re-armed at every checkpoint"
        );

        // The tolerance is scoped, and this is the half that says so: the
        // commit whose delta is gone keeps its place on the chain, and every
        // checkpoint commit does too. Only the interior commits left.
        assert_eq!(
            present(&session, &checkpoints).await,
            checkpoints,
            "a checkpoint commit stays on the head's first-parent chain across a sweep"
        );

        // And the sweep that ran was the tolerant one. Planning the same
        // repository again still finds the reclaimed delta and counts it,
        // rather than demanding it.
        let read = SharedStorageAdapterRead::new(
            session
                .storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("tolerance plan read opens"),
        );
        let mut writes = session.storage.new_write_set();
        let mut preconditions = Vec::new();
        let plan = stage_repository_gc_with_preconditions(read, &mut writes, &mut preconditions)
            .await
            .expect("a legacy repository must still plan");
        assert!(
            plan.profile.history_manifests_missing >= 1,
            "the delta this test reclaimed by hand must be counted as tolerated, not swallowed"
        );
    }
}

/// Process-global count of reclaim attempts that failed before reaching
/// `mark_collected`.
///
/// A persistently failing sweep previously produced no observable signal at
/// all, which is why one went unnoticed. This is the cheap half of making the
/// next one findable; the persisted `consecutive_reclaim_failures` is the half
/// that survives a restart.
pub(crate) fn reclaim_failures_total() -> &'static AtomicU64 {
    static RECLAIM_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
    &RECLAIM_FAILURES_TOTAL
}
