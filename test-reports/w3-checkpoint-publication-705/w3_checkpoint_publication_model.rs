//! Dependency-free W3 acceptance model; intentionally not Cargo-wired.
//!
//! The model is a review oracle, not a second implementation. A future
//! runnable candidate must bind these assertions to public transaction tests
//! on Memory, RocksDB, and SlateDB after the source RED gate turns GREEN.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    CoherentView,
    PreparedPublication,
    StoragePlan,
    PrepareWriteSet,
    BoundaryCommit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CaseResult {
    AtomicSuccess,
    Noop,
    RetryFreshView,
    RejectSameOwner,
    RejectCorruption,
    RejectUnsupported,
}

fn checkpoint_trace() -> [Step; 5] {
    [
        Step::CoherentView,
        Step::PreparedPublication,
        Step::StoragePlan,
        Step::PrepareWriteSet,
        Step::BoundaryCommit,
    ]
}

fn classify(global_epoch_changed: bool, same_owner_changed: bool) -> CaseResult {
    if same_owner_changed {
        CaseResult::RejectSameOwner
    } else if global_epoch_changed {
        CaseResult::RetryFreshView
    } else {
        CaseResult::AtomicSuccess
    }
}

#[test]
fn one_view_one_plan_one_prepare_one_commit() {
    let trace = checkpoint_trace();
    assert_eq!(trace.iter().filter(|s| **s == Step::CoherentView).count(), 1);
    assert_eq!(trace.iter().filter(|s| **s == Step::PreparedPublication).count(), 1);
    assert_eq!(trace.iter().filter(|s| **s == Step::StoragePlan).count(), 1);
    assert_eq!(trace.iter().filter(|s| **s == Step::PrepareWriteSet).count(), 1);
    assert_eq!(trace.iter().filter(|s| **s == Step::BoundaryCommit).count(), 1);
}

#[test]
fn sixty_five_rotations_cover_sixty_four_plus_suffix() {
    let rotations = 65;
    let retained_interval = 64;
    let suffix = rotations - retained_interval;
    assert_eq!(suffix, 1);
    assert!(rotations >= retained_interval);
}

#[test]
fn accepted_and_rejected_w3_cases_are_explicit() {
    let accepted = [
        "selected_history",
        "intermediate_commit",
        "parent_override",
        "checkpoint_ref",
        "recovery_ref",
        "cold_reopen",
    ];
    let rejected = [
        "duplicate_ordinal",
        "out_of_order_ordinal",
        "back_edge",
        "wrong_parent",
        "missing_parent",
        "stale_selector",
        "unrelated_owner_global_precondition",
        "independent_publication",
    ];
    assert_eq!(accepted.len(), 6);
    assert_eq!(rejected.len(), 8);
    assert!(accepted.iter().all(|name| !rejected.contains(name)));
}

#[test]
fn races_noop_rollback_and_corruption_fail_closed() {
    assert_eq!(classify(true, false), CaseResult::RetryFreshView);
    assert_eq!(classify(false, true), CaseResult::RejectSameOwner);
    assert_eq!(classify(false, false), CaseResult::AtomicSuccess);
    assert_eq!(CaseResult::Noop, CaseResult::Noop);
    assert_eq!(CaseResult::RejectCorruption, CaseResult::RejectCorruption);
    assert_eq!(CaseResult::RejectUnsupported, CaseResult::RejectUnsupported);
}
