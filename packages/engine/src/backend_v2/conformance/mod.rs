//! Conformance harness for backend_v2 implementations.
//!
//! The harness is colocated with the experimental API for now. Once backend_v2
//! is stable, rs-sdk can re-export this as the public backend author test kit.

mod baseline;
pub mod conformance_backend;
mod factory;
#[cfg(test)]
mod failure_tests;
#[allow(dead_code)]
mod fixtures;
#[allow(dead_code)]
mod model;
mod model_based;
mod projection;
mod pushdown;
mod runner;
mod scan;
mod write;

pub use factory::{BackendFactory, BackendTestConfig};
pub use runner::{
    run_backend_conformance, ConformanceReport, ConformanceResult, ConformanceStatus,
    ConformanceTest,
};

#[cfg(test)]
mod tests {
    use super::{
        conformance_backend::ConformanceBackendFactory, run_backend_conformance, ConformanceStatus,
    };

    #[test]
    fn conformance_backend_passes_baseline_conformance() {
        let report = run_backend_conformance(&ConformanceBackendFactory);

        report.assert_no_failures();

        let passed = report
            .tests
            .iter()
            .filter(|test| matches!(test.status, ConformanceStatus::Passed))
            .map(|test| test.name)
            .collect::<Vec<_>>();
        assert_eq!(
            passed,
            vec![
                "baseline::get_many_preserves_caller_order_duplicates_and_missing",
                "baseline::get_many_empty_key_list",
                "baseline::get_many_missing_only_and_duplicate_missing",
                "baseline::write_reads_its_own_writes",
                "baseline::delete_many_missing_keys_is_idempotent",
                "baseline::put_many_overwrites_existing_value",
                "baseline::scan_range_returns_forward_row_bounded_pages",
                "baseline::scan_range_honors_bound_variants",
                "baseline::scan_range_limit_zero_returns_empty_page",
                "baseline::scan_range_empty_range_returns_empty_page",
                "baseline::scan_prefix_matches_equivalent_range",
                "baseline::scan_prefix_empty_prefix_scans_whole_space",
                "baseline::scan_prefix_ff_prefix_uses_unbounded_upper_range",
                "baseline::commit_is_atomic",
                "baseline::rollback_discards_staged_mutations",
                "baseline::begin_read_pins_coherent_view",
                "baseline::spaces_are_isolated",
                "baseline::full_value_and_key_only_are_core",
                "baseline::read_support_metadata_is_truthful_for_core_reads",
                "baseline::cursor_rejects_changed_range",
                "baseline::cursor_rejects_changed_projection",
                "baseline::cursor_rejects_different_read_transaction",
                "model::deterministic_history_matches_reference_model",
                "scan::reverse_returns_unsupported_when_not_capable",
                "projection::header_returns_unsupported_when_not_capable",
                "projection::refs_returns_unsupported_when_not_capable",
                "projection::header_and_refs_returns_unsupported_when_not_capable",
                "projection::payload_returns_unsupported_when_not_capable",
                "pushdown::predicate_returns_unsupported_when_not_capable",
            ]
        );
    }
}
