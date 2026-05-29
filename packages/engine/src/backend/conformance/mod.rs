//! Conformance harness for backend implementations.
//!
//! The harness is colocated with the experimental API for now. Once backend
//! is stable, lix-sdk can re-export this as the public backend author test kit.

mod baseline;
mod factory;
#[cfg(test)]
mod failure_tests;
#[allow(dead_code)]
mod fixtures;
#[allow(dead_code)]
mod model;
mod model_based;
mod persistence;
mod runner;

pub(crate) use factory::open_backend;
pub use factory::{BackendFactory, BackendFixture, BackendTestConfig};
pub use runner::{
    ConformanceReport, ConformanceResult, ConformanceStatus, ConformanceTest,
    run_backend_conformance,
};

#[cfg(test)]
mod tests {
    use super::{ConformanceStatus, run_backend_conformance};
    use crate::backend::InMemoryBackendFactory;

    #[test]
    fn in_memory_backend_passes_baseline_conformance() {
        let report = run_backend_conformance(&InMemoryBackendFactory);

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
                "baseline::get_many_returns_requested_slots",
                "baseline::get_many_empty_key_list",
                "baseline::delete_many_missing_keys_is_idempotent",
                "baseline::delete_many_removes_existing_keys",
                "baseline::delete_range_removes_exact_range",
                "baseline::delete_range_applies_after_staged_puts",
                "baseline::put_many_applies_after_delete_range",
                "baseline::put_many_overwrites_existing_value",
                "baseline::scan_range_sees_overwritten_existing_value",
                "baseline::scan_range_returns_forward_row_bounded_chunks",
                "baseline::scan_range_honors_bound_variants",
                "baseline::scan_range_resume_before_lower_does_not_widen_range",
                "baseline::scan_range_orders_raw_byte_keys",
                "baseline::scan_range_drains_multi_chunk_limits",
                "baseline::scan_cursor_drains_multi_chunk_limits",
                "baseline::scan_range_empty_range_returns_empty_chunk",
                "baseline::commit_is_atomic",
                "baseline::rollback_discards_staged_mutations",
                "baseline::rollback_discards_overwrite_and_delete",
                "baseline::begin_read_pins_coherent_view",
                "baseline::full_value_and_key_only_are_core",
                "baseline::full_value_preserves_opaque_bytes",
                "model::deterministic_history_matches_reference_model",
            ]
        );
    }
}
