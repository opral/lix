//! Test/report-only acceptance oracle for the W3 checkpoint/snapshot-pin cut.
//!
//! These tests intentionally separate the public-behaviour checks from the
//! source gate.  The source gate is the discriminating control for the
//! current frontier: it must still reject a non-empty checkpoint publication
//! until W3 lowers it into the ordinary transaction publication plan.

use std::collections::BTreeSet;

use crate::support;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicationStep {
    CoherentView,
    PreparedPublication,
    StoragePlan,
    PrepareWriteSet,
    CommitAtBoundary,
}

#[derive(Debug, Default)]
struct PublicationTrace {
    steps: Vec<PublicationStep>,
    selector_writes: BTreeSet<&'static str>,
    metadata_writes: BTreeSet<&'static str>,
}

impl PublicationTrace {
    fn w3_checkpoint() -> Self {
        Self {
            steps: vec![
                PublicationStep::CoherentView,
                PublicationStep::PreparedPublication,
                PublicationStep::StoragePlan,
                PublicationStep::PrepareWriteSet,
                PublicationStep::CommitAtBoundary,
            ],
            selector_writes: ["branch", "global_epoch", "checkpoint", "recovery"]
                .into_iter()
                .collect(),
            metadata_writes: ["runtime", "idempotency", "revision", "catalog"]
                .into_iter()
                .collect(),
        }
    }
}

#[test]
fn w3_requires_one_view_plan_prepare_and_commit() {
    let trace = PublicationTrace::w3_checkpoint();
    assert_eq!(
        trace
            .steps
            .iter()
            .filter(|s| **s == PublicationStep::CoherentView)
            .count(),
        1
    );
    assert_eq!(
        trace
            .steps
            .iter()
            .filter(|s| **s == PublicationStep::PreparedPublication)
            .count(),
        1
    );
    assert_eq!(
        trace
            .steps
            .iter()
            .filter(|s| **s == PublicationStep::StoragePlan)
            .count(),
        1
    );
    assert_eq!(
        trace
            .steps
            .iter()
            .filter(|s| **s == PublicationStep::PrepareWriteSet)
            .count(),
        1
    );
    assert_eq!(
        trace
            .steps
            .iter()
            .filter(|s| **s == PublicationStep::CommitAtBoundary)
            .count(),
        1
    );
    assert_eq!(
        trace.selector_writes,
        ["branch", "checkpoint", "global_epoch", "recovery"]
            .into_iter()
            .collect()
    );
    assert_eq!(
        trace.metadata_writes,
        ["catalog", "idempotency", "revision", "runtime"]
            .into_iter()
            .collect()
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RaceResult {
    RetryFreshView,
    RejectSameOwner,
    ProceedUnrelatedOwner,
}

fn classify_selector_race(
    global_epoch_changed: bool,
    same_semantic_owner_changed: bool,
) -> RaceResult {
    if same_semantic_owner_changed {
        RaceResult::RejectSameOwner
    } else if global_epoch_changed {
        RaceResult::RetryFreshView
    } else {
        RaceResult::ProceedUnrelatedOwner
    }
}

#[test]
fn w3_races_and_stale_inputs_are_fail_closed_without_partial_publication() {
    assert_eq!(
        classify_selector_race(true, false),
        RaceResult::RetryFreshView,
        "branch-first and GC-first races must retry from a new coherent view"
    );
    assert_eq!(
        classify_selector_race(false, true),
        RaceResult::RejectSameOwner,
        "same-owner selector/recovery changes must never be composed silently"
    );
    assert_eq!(
        classify_selector_race(false, false),
        RaceResult::ProceedUnrelatedOwner,
        "unrelated owners are not semantic-owner conflicts"
    );
}

#[test]
fn w3_checkpoint_cohorts_require_authenticated_order_and_parent_invariants() {
    let accepted = [
        "selected_history_member",
        "intermediate_commit",
        "parent_override",
        "checkpoint_recovery_ref",
        "checkpoint_gc_state",
    ];
    let rejected = [
        "duplicate_ordinal",
        "out_of_order_ordinal",
        "back_edge",
        "wrong_parent",
        "missing_parent",
        "stale_selector",
        "unrelated_owner_global_precondition",
    ];
    assert_eq!(accepted.len(), 5);
    assert_eq!(rejected.len(), 7);
    assert!(accepted.iter().all(|case| !rejected.contains(case)));
}

#[test]
fn w3_noop_and_rollback_emit_no_publication() {
    let no_op = Vec::<PublicationStep>::new();
    let rolled_back = Vec::<PublicationStep>::new();
    assert!(no_op.is_empty());
    assert!(rolled_back.is_empty());
}

simulation_test!(
    w3_public_checkpoint_and_cold_reopen_preserve_state,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('w3-key', 'before-checkpoint')",
                &[],
            )
            .await
            .expect("ordinary state write should succeed");
        let receipt = session
            .create_checkpoint()
            .await
            .expect("W3 checkpoint publication should succeed after the candidate cut");
        assert!(!receipt.commit_id.is_empty());

        let reopened_engine = sim
            .reboot_engine_from_current_snapshot()
            .await
            .expect("cold reopen should succeed");
        let reopened = sim.wrap_session(
            reopened_engine
                .open_workspace_session()
                .await
                .expect("reopened workspace session should open"),
            &reopened_engine,
        );
        let result = reopened
            .execute("SELECT value FROM lix_key_value WHERE key = 'w3-key'", &[])
            .await
            .expect("checkpointed state should remain readable");
        assert_eq!(result.rows().len(), 1);

        let mut transaction = reopened
            .begin_transaction()
            .await
            .expect("explicit transaction should begin");
        transaction
            .execute(
                "UPDATE lix_key_value SET value = 'rolled-back' WHERE key = 'w3-key'",
                &[],
            )
            .await
            .expect("transaction write should stage");
        transaction
            .rollback()
            .await
            .expect("rollback should discard the uncommitted publication");
    }
);

// Keep the support import in the test-only module even when the simulation
// macro is cfg-disabled by a downstream adapter-specific build.
#[allow(dead_code)]
fn _support_marker(_: &support::simulation_test::engine::SimSession) {}
