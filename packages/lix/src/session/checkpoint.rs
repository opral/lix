use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchReferenceRole};
use crate::checkpoint::{CHECKPOINT_SCHEMA_KEY, checkpoint_stage_row};
use crate::gc::CheckpointGcState;
use crate::storage_adapter::Storage;
use crate::tracked_state::{TrackedStateDiffKind, TrackedStateDiffRequest, TrackedStateDiffRow};
use crate::transaction::StagedCommitChangeBatchBuilder;
use crate::transaction_types::{RawWriteBatch, TransactionWrite, TransactionWriteMode};

use super::context::SessionContext;

/// Receipt returned after compacting the active working interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCheckpointReceipt {
    /// Commit containing the branch state captured by this checkpoint.
    pub commit_id: String,
    /// Logical change that published the repository-global checkpoint entity.
    pub change_id: String,
}

/// Reclaim when the estimated retirable set reaches this fraction of the live
/// inventory the sweep must scan.
const RECLAIM_YIELD_DENOMINATOR: u64 = 4;
/// Inventory floor, so a small repository does not sweep on every checkpoint.
const RECLAIM_MIN_INVENTORY: u64 = 64;
/// Cap on retry damping, so a repository whose sweep recovers is not locked
/// out for an unbounded number of checkpoints.
const RECLAIM_FAILURE_BACKOFF_CAP: u32 = 10;
/// Staleness backstop: collectible debt may not sit uncollected for longer
/// than this many checkpoints, whatever the yield ratio says. Grows with
/// retained history exactly as the previous rule did, which is what keeps
/// full-sweep positions sublinear.
const RECLAIM_MAX_STALENESS: u64 = 64;

struct CreateCheckpointOutcome {
    receipt: CreateCheckpointReceipt,
    gc_due: bool,
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Creates a checkpoint for the active branch.
    ///
    /// The new commit contains the net tracked changes since the previous
    /// checkpoint and parents that checkpoint directly. The old branch head is
    /// retained as a local recovery root for the GC grace window. Publication
    /// returns as soon as that durable root is committed; due garbage
    /// collection runs asynchronously so a history-sized sweep cannot extend
    /// the foreground checkpoint latency.
    pub async fn create_checkpoint(&self) -> Result<CreateCheckpointReceipt, LixError> {
        let outcome = self
            .with_write_transaction_lending(async move |transaction| {
                let branch_id = transaction.active_branch_id().to_string();
                let (previous_recovery, mut gc_state) =
                    transaction.checkpoint_publication_state(&branch_id).await?;
                let head_commit_id = {
                    let reader = transaction.branch_ref_reader().await;
                    BranchLifecycle::new(&reader)
                        .require_existing_commit_id(
                            &branch_id,
                            BranchOperation::CreateCheckpoint,
                            BranchReferenceRole::Target,
                        )
                        .await?
                };
                let direct_working_diff = transaction
                    .working_diff_at_head(
                        &branch_id,
                        head_commit_id,
                        &TrackedStateDiffRequest::default(),
                    )
                    .await?;
                let previous_checkpoint_commit_id = if let Some(direct) = &direct_working_diff {
                    direct.checkpoint_commit_id
                } else {
                    transaction
                        .checkpoint_commit_id_at_head(&branch_id, head_commit_id)
                        .await?
                };
                let interval_has_commits = head_commit_id != previous_checkpoint_commit_id;
                let selected_changes = {
                    let entries = if let Some(direct) = direct_working_diff {
                        direct.diff.entries
                    } else {
                        let mut reader = transaction.tracked_state_reader().await;
                        reader
                            .diff_commits(
                                &previous_checkpoint_commit_id.to_string(),
                                &head_commit_id.to_string(),
                                &TrackedStateDiffRequest::default(),
                            )
                            .await?
                            .entries
                    };
                    let mut selected_changes =
                        StagedCommitChangeBatchBuilder::with_capacity(entries.len());
                    let mut source_membership_exact = true;
                    for entry in entries.into_iter().filter(|entry| {
                        entry.identity.schema_key() != CHECKPOINT_SCHEMA_KEY
                            && entry.identity.schema_key()
                                != crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
                    }) {
                        let row = entry.after.ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "working diff for schema '{}' entity {:?} has no target row",
                                    entry.identity.schema_key(),
                                    entry.identity.entity_pk()
                                ),
                            )
                        })?;
                        source_membership_exact &=
                            push_selected_change(&mut selected_changes, row, entry.kind);
                    }
                    if source_membership_exact {
                        selected_changes.finish_source_certified()
                    } else {
                        selected_changes.finish()
                    }
                };
                gc_state.checkpoint_sequence =
                    gc_state.checkpoint_sequence.checked_add(1).ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "checkpoint sequence overflow",
                        )
                    })?;
                if let Some(previous_recovery) = previous_recovery {
                    gc_state.add_collectible_interval(previous_recovery.interval_has_commits);
                }
                let gc_due = checkpoint_gc_due(gc_state)?;

                let commit_id = transaction.stage_checkpoint_commit(
                    branch_id,
                    previous_checkpoint_commit_id,
                    head_commit_id,
                    interval_has_commits,
                    gc_state,
                    selected_changes,
                )?;
                let checkpoint_commit_id =
                    crate::changelog::CommitId::parse_lix(&commit_id, "checkpoint commit id")?;
                let change_id = transaction.functions().call_uuid_v7().to_string();
                let mut checkpoint_rows = RawWriteBatch::with_capacity(1);
                checkpoint_rows.push(checkpoint_stage_row(
                    &checkpoint_commit_id,
                    change_id.clone(),
                ));
                transaction
                    .stage_write(TransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows: checkpoint_rows,
                    })
                    .await?;
                Ok(CreateCheckpointOutcome {
                    receipt: CreateCheckpointReceipt {
                        commit_id,
                        change_id,
                    },
                    gc_due,
                })
            })
            .await?;
        if outcome.gc_due {
            // GC debt is durable in the checkpoint transaction. The sweep is
            // therefore safely retryable and does not need to delay the user
            // checkpoint. A clone shares the same storage and collaboration
            // write gate; concurrent schedules serialize, and only the first
            // one that still sees debt performs work.
            let gc_session = self.clone();
            tokio::spawn(async move {
                gc_session.collect_checkpoint_garbage_best_effort().await;
            });
        }
        Ok(outcome.receipt)
    }
}

/// Decides whether a repository-wide reclaim is worth running.
///
/// The sweep costs O(live commit manifests) to plan and yields O(retired
/// commits), so amortised cost per reclaimed commit is bounded only when yield
/// is proportional to the inventory the sweep must scan. The previous rule
/// keyed its magnitude to checkpoint *count*, which is blind to how much
/// garbage an interval actually holds: a repository checkpointing once every
/// thousand commits accrued debt a thousand times more slowly than it accrued
/// garbage, while one checkpointing every commit accrued debt far faster.
///
/// Evaluated at checkpoints, and that is correct rather than a compromise.
/// Debt rises only at a checkpoint and the denominator only at ordinary
/// commits, so the ratio can only ever cross *upward* at a checkpoint.
/// Checkpointing is also what makes an interval collectible in the first
/// place, so there is no cadence that reclaims without one.
pub(crate) fn checkpoint_gc_due(state: CheckpointGcState) -> Result<bool, LixError> {
    if !state.has_collectible_debt() {
        return Ok(false);
    }
    let checkpoint_age = state
        .checkpoint_sequence
        .checked_sub(state.last_gc_sequence)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "checkpoint GC sequence is ahead of checkpoint sequence",
            )
        })?;
    // Bootstrap: before any sweep has reported, assume one retired commit per
    // interval, which reduces this to the old interval count.
    let yield_estimate = state.yield_per_interval_estimate.max(1);
    let estimated_reclaimable = state
        .collectible_interval_count
        .saturating_mul(yield_estimate);
    // Retry damping. Without this the predicate latches: a failing sweep never
    // reaches `mark_collected`, so the estimates freeze while debt grows and
    // every later checkpoint re-arms a full repository pass. Measured at
    // 5000 of 5000 checkpoints before this was added.
    let shift = state
        .consecutive_reclaim_failures
        .min(u64::from(RECLAIM_FAILURE_BACKOFF_CAP)) as u32;
    let backoff = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
    let inventory = state
        .live_manifest_estimate
        .max(RECLAIM_MIN_INVENTORY)
        .saturating_mul(backoff);
    // Cost trigger: sweep once the estimated yield pays for the scan. This
    // fires *earlier* than the backstop when an interval holds dense garbage,
    // which is the case the old checkpoint-count rule could not see.
    let yield_due = estimated_reclaimable.saturating_mul(RECLAIM_YIELD_DENOMINATOR) >= inventory;
    // Staleness backstop, retained from the previous rule and load-bearing.
    // The ratio bounds cost but says nothing about *delay*: a repository whose
    // garbage is real but sparse would otherwise wait indefinitely, since a
    // small debt never reaches a fraction of the inventory. Empty padding
    // checkpoints accrue no debt at all, so this is the only thing that
    // collects such a repository.
    let age_limit = RECLAIM_MAX_STALENESS
        .max(state.last_gc_sequence)
        .saturating_mul(backoff);
    let stale_due = checkpoint_age >= age_limit;
    Ok(yield_due || stale_due)
}

fn push_selected_change(
    selected_changes: &mut StagedCommitChangeBatchBuilder,
    row: TrackedStateDiffRow,
    kind: TrackedStateDiffKind,
) -> bool {
    // A changelog change durably stores one timestamp, which rebuild uses for
    // both timestamps when the entity is absent from the parent checkpoint.
    // Canonicalize newly added rows to that representation so checkpoint roots
    // remain content-equivalent after their compacted auto-commits are swept.
    let created_at = match kind {
        TrackedStateDiffKind::Added => row.updated_at,
        TrackedStateDiffKind::Modified | TrackedStateDiffKind::Removed => row.created_at,
    };
    let source_membership_exact = created_at == row.created_at;
    let deleted = row.deleted;
    let source_commit_id = row.commit_id;
    let change_id = row.change_id;
    let updated_at = row.updated_at;
    selected_changes.push(
        row.identity,
        source_commit_id,
        change_id,
        deleted,
        created_at,
        updated_at,
    );
    source_membership_exact
}

#[cfg(test)]
mod tests {
    use super::{
        RECLAIM_MIN_INVENTORY, RECLAIM_YIELD_DENOMINATOR, checkpoint_gc_due,
        push_selected_change,
    };
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::gc::CheckpointGcState;
    use crate::tracked_state::{
        TrackedStateDiffIdentity, TrackedStateDiffKind, TrackedStateDiffRow, TrackedStateKey,
    };
    use crate::transaction::StagedCommitChangeBatchBuilder;

    #[test]
    fn canonicalized_added_timestamp_declines_source_membership_certificate() {
        let created_at = LixTimestamp::expect_parse("created_at", "2026-01-01T00:00:00Z");
        let updated_at = LixTimestamp::expect_parse("updated_at", "2026-01-02T00:00:00Z");
        let mut selected = StagedCommitChangeBatchBuilder::with_capacity(1);
        let source_membership_exact = push_selected_change(
            &mut selected,
            TrackedStateDiffRow {
                identity: TrackedStateDiffIdentity::from_key(TrackedStateKey {
                    schema_key: "test_schema".to_string(),
                    file_id: None,
                    entity_pk: EntityPk::single("entity"),
                }),
                deleted: false,
                created_at,
                updated_at,
                change_id: ChangeId::for_test_label("checkpoint-canonicalized-change"),
                commit_id: CommitId::for_test_label("checkpoint-canonicalized-commit"),
            },
            TrackedStateDiffKind::Added,
        );
        let selected = if source_membership_exact {
            selected.finish_source_certified()
        } else {
            selected.finish()
        };

        assert!(!selected.source_membership_certified());
        assert_eq!(
            selected.iter().next().expect("one selected row").created_at,
            updated_at
        );
    }

    fn state(sequence: u64, last_gc_sequence: u64) -> CheckpointGcState {
        CheckpointGcState {
            checkpoint_sequence: sequence,
            last_gc_sequence,
            collectible_interval_count: 1,
            ..CheckpointGcState::default()
        }
    }

    /// The trigger must key off estimated yield against inventory, not off
    /// how many checkpoints have happened. Verified by inversion: the same
    /// interval count produces opposite verdicts when only the observed yield
    /// differs, and again when only the inventory differs.
    #[test]
    fn reclaim_keys_off_yield_against_inventory_not_checkpoint_count() {
        let intervals = 8;
        let inventory = 1_024;

        // Yield too low for this inventory -> refuses.
        // `last_gc_sequence` is large and the age small, so the staleness
        // backstop cannot fire and the yield ratio is the only thing deciding.
        let lean = CheckpointGcState {
            checkpoint_sequence: 100_000 + intervals,
            last_gc_sequence: 100_000,
            collectible_interval_count: intervals,
            live_manifest_estimate: inventory,
            yield_per_interval_estimate: 4,
            consecutive_reclaim_failures: 0,
        };
        assert!(
            !checkpoint_gc_due(lean).expect("lean state should be valid"),
            "8 intervals x 4 = 32 retirable against 1024 inventory must not sweep"
        );

        // Same interval count, richer intervals -> fires.
        let rich = CheckpointGcState {
            yield_per_interval_estimate: 64,
            ..lean
        };
        assert!(
            checkpoint_gc_due(rich).expect("rich state should be valid"),
            "8 intervals x 64 = 512 retirable against 1024 inventory must sweep"
        );

        // Same yield, smaller inventory -> also fires. The denominator is the
        // thing the old rule could not see.
        let small = CheckpointGcState {
            live_manifest_estimate: 64,
            ..lean
        };
        assert!(
            checkpoint_gc_due(small).expect("small state should be valid"),
            "the same debt against a small inventory must sweep"
        );
    }

    /// The ratio boundary itself, both sides, so a change to the constants
    /// cannot silently move the trigger without failing here.
    #[test]
    fn reclaim_fires_exactly_at_the_ratio_boundary() {
        let inventory = 4_096;
        let at_boundary = inventory / RECLAIM_YIELD_DENOMINATOR;
        // Same isolation as above: staleness must not be able to decide.
        let mut just_under = CheckpointGcState {
            checkpoint_sequence: 10_000_000 + at_boundary,
            last_gc_sequence: 10_000_000,
            collectible_interval_count: at_boundary - 1,
            live_manifest_estimate: inventory,
            yield_per_interval_estimate: 1,
            consecutive_reclaim_failures: 0,
        };
        assert!(!checkpoint_gc_due(just_under).expect("valid"), "just under must refuse");
        just_under.collectible_interval_count = at_boundary;
        assert!(checkpoint_gc_due(just_under).expect("valid"), "at the boundary must fire");
    }

    /// A repository smaller than the inventory floor must not sweep on every
    /// checkpoint just because its inventory is tiny.
    #[test]
    fn small_repositories_are_floored_not_swept_constantly() {
        let tiny = CheckpointGcState {
            checkpoint_sequence: 5,
            last_gc_sequence: 0,
            collectible_interval_count: 1,
            live_manifest_estimate: 4,
            yield_per_interval_estimate: 1,
            consecutive_reclaim_failures: 0,
        };
        assert!(
            !checkpoint_gc_due(tiny).expect("tiny state should be valid"),
            "one interval against the {RECLAIM_MIN_INVENTORY}-manifest floor must not sweep"
        );
    }

    /// The latch. A failing sweep never reaches `mark_collected`, so without
    /// damping the estimates freeze while debt grows and every later
    /// checkpoint re-arms a full repository pass -- measured at 5000 of 5000.
    /// Damping must break that, and must not be permanent.
    #[test]
    fn repeated_reclaim_failures_damp_the_retry() {
        let base = CheckpointGcState {
            checkpoint_sequence: 1_000,
            last_gc_sequence: 0,
            collectible_interval_count: 64,
            live_manifest_estimate: 256,
            yield_per_interval_estimate: 1,
            consecutive_reclaim_failures: 0,
        };
        assert!(
            checkpoint_gc_due(base).expect("valid"),
            "fixture must be due with no failures, or the damping below is vacuous"
        );

        let damped = CheckpointGcState {
            consecutive_reclaim_failures: 4,
            ..base
        };
        assert!(
            !checkpoint_gc_due(damped).expect("valid"),
            "repeated failures must stop re-arming a full sweep every checkpoint"
        );

        // Damping is a delay, not a lockout: enough further debt still fires.
        let recovered = CheckpointGcState {
            collectible_interval_count: 64 * 32,
            ..damped
        };
        assert!(
            checkpoint_gc_due(recovered).expect("valid"),
            "damping must yield once the estimated reclaimable set grows enough"
        );
    }

    /// The property the ratio actually buys is **bounded amortised cost per
    /// reclaimed commit**, not a small sweep count.
    ///
    /// A sweep scans O(live commit manifests) and reclaims some number of
    /// commits; the trigger only fires when the estimated reclaimable set is
    /// at least `1/RECLAIM_YIELD_DENOMINATOR` of that inventory, so total
    /// scanned work stays within a constant factor of total reclaimed work no
    /// matter how the user checkpoints. That is the invariant asserted here.
    ///
    /// Asserting a sweep *count* instead is a proxy and a misleading one: an
    /// earlier version of this test held inventory at the small-repository
    /// floor, where the floor alone set the cadence, and read 625 sweeps while
    /// the cost invariant was never in danger.
    #[test]
    fn reclaim_cost_stays_within_a_constant_factor_of_what_it_reclaims() {
        let mut state = CheckpointGcState::default();
        // Each checkpoint publishes one commit that stays live forever and
        // three that become retirable -- a repository that genuinely grows.
        let mut live = 0u64;
        let mut garbage = 0u64;
        let mut total_scanned = 0u64;
        let mut total_reclaimed = 0u64;
        let mut sweeps = 0u64;
        let mut first_half_scanned = 0u64;
        let mut first_half_reclaimed = 0u64;
        let mut second_half_scanned = 0u64;
        let mut second_half_reclaimed = 0u64;
        for sequence in 1..=10_000 {
            state.checkpoint_sequence = sequence;
            state.add_collectible_interval(true);
            live += 1;
            garbage += 3;
            if checkpoint_gc_due(state).expect("simulated state should be valid") {
                sweeps += 1;
                total_scanned += live + garbage;
                total_reclaimed += garbage;
                if sequence <= 5_000 {
                    first_half_scanned += live + garbage;
                    first_half_reclaimed += garbage;
                } else {
                    second_half_scanned += live + garbage;
                    second_half_reclaimed += garbage;
                }
                let reclaimed = garbage;
                garbage = 0;
                state.mark_collected(reclaimed, live);
            }
        }
        let first_half_ratio_x100 =
            first_half_scanned.saturating_mul(100) / first_half_reclaimed.max(1);
        let second_half_ratio_x100 =
            second_half_scanned.saturating_mul(100) / second_half_reclaimed.max(1);

        assert!(
            sweeps > 0 && total_reclaimed > 0,
            "the simulation must actually sweep and reclaim, or the bounds below are vacuous"
        );

        // Bounded: a sweep scans `live + garbage` while the trigger compares
        // against the live estimate alone, so the achievable constant is
        // `RECLAIM_YIELD_DENOMINATOR + 1` plus one sweep of estimate lag --
        // not the denominator itself. Measured ~5.3x. The assertion is
        // deliberately loose rather than tuned to that number: the claim is
        // that the factor is constant, not that it equals any given value.
        let ratio_x100 = total_scanned.saturating_mul(100) / total_reclaimed;
        assert!(
            ratio_x100 < 800,
            "scanned {total_scanned} manifests to reclaim {total_reclaimed} commits \
             across {sweeps} sweeps ({ratio_x100} per 100), above a constant factor"
        );

        // And constant: the factor must not grow with repository size, which
        // is the whole amortisation claim. A cadence keyed to checkpoint count
        // fails here while passing the bound above.
        assert!(
            second_half_ratio_x100 <= first_half_ratio_x100.saturating_mul(2),
            "cost factor grew from {first_half_ratio_x100} to {second_half_ratio_x100} \
             per 100 as the repository aged; amortised cost is not bounded"
        );

        // Sweeps must stay well short of one per checkpoint.
        assert!(
            sweeps < 10_000 / 4,
            "ratio cadence scheduled {sweeps} sweeps in 10000 checkpoints"
        );
    }

    /// The staleness backstop. The ratio bounds cost but says nothing about
    /// delay: a repository whose garbage is real but sparse never reaches a
    /// fraction of its inventory, and empty padding checkpoints accrue no debt
    /// at all. Without this, such a repository would wait indefinitely --
    /// which is exactly how the history-retention fix's own acceptance test
    /// caught the omission.
    #[test]
    fn sparse_debt_still_collects_via_the_staleness_backstop() {
        let sparse = CheckpointGcState {
            checkpoint_sequence: 70,
            last_gc_sequence: 0,
            collectible_interval_count: 6,
            live_manifest_estimate: 0,
            yield_per_interval_estimate: 0,
            consecutive_reclaim_failures: 0,
        };
        // The ratio alone refuses: 6 intervals x 1 estimated = 6, and
        // 6 * 4 = 24 is below the 64-manifest floor.
        assert!(
            6u64 * RECLAIM_YIELD_DENOMINATOR < RECLAIM_MIN_INVENTORY,
            "fixture must be below the ratio, or this proves nothing about the backstop"
        );
        assert!(
            checkpoint_gc_due(sparse).expect("valid"),
            "sparse but real debt must still collect once it goes stale"
        );

        // And the backstop is not a fixed cadence: it grows with retained
        // history, which is what keeps full-sweep positions sublinear.
        let mature = CheckpointGcState {
            checkpoint_sequence: 8_100,
            last_gc_sequence: 8_000,
            ..sparse
        };
        assert!(
            !checkpoint_gc_due(mature).expect("valid"),
            "a mature repository must not fall back to a fixed 64-checkpoint cadence"
        );
    }

    #[test]
    fn empty_debt_never_schedules_a_sweep() {
        let state = CheckpointGcState {
            checkpoint_sequence: u64::MAX,
            last_gc_sequence: 0,
            ..CheckpointGcState::default()
        };
        assert!(!checkpoint_gc_due(state).expect("empty GC state should be valid"));
    }

    #[test]
    fn invalid_gc_sequence_is_rejected() {
        let state = state(2, 3);
        assert!(checkpoint_gc_due(state).is_err());
    }
}
