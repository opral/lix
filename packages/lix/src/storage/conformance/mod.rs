//! Conformance harness for storage implementations.

mod baseline;
mod declared;
mod factory;
#[cfg(test)]
mod failure_tests;
mod fixtures;
mod model;
mod model_based;
mod persistence;
mod runner;

pub use declared::*;
pub(crate) use factory::open_storage;
pub use factory::{StorageFactory, StorageFixture, StorageTestConfig};
pub use runner::{
    ConformanceReport, ConformanceResult, ConformanceStatus, ConformanceTest,
    run_storage_conformance,
};

pub(crate) trait SingleSpaceStorageRead {
    async fn get_many_in_space(
        &self,
        space: crate::storage::StorageSpace,
        keys: &[crate::storage::Key],
        opts: crate::storage::GetOptions,
    ) -> Result<crate::storage::GetManyResult, crate::storage::StorageError>;
}

impl<R> SingleSpaceStorageRead for R
where
    R: crate::storage::StorageRead,
{
    async fn get_many_in_space(
        &self,
        space: crate::storage::StorageSpace,
        keys: &[crate::storage::Key],
        opts: crate::storage::GetOptions,
    ) -> Result<crate::storage::GetManyResult, crate::storage::StorageError> {
        crate::storage::StorageRead::get_many(
            self,
            &[crate::storage::GetManyRequest { space, keys, opts }],
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{ConformanceStatus, run_storage_conformance};
    use crate::storage::MemoryFactory;

    #[tokio::test]
    async fn memory_passes_baseline_conformance() {
        let report = run_storage_conformance(&MemoryFactory).await;

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
                "baseline::spaces_do_not_collide",
                "baseline::scan_is_space_scoped",
                "baseline::unbounded_delete_range_truncates_only_target_space",
                "baseline::empty_space_reads_are_empty",
                "baseline::get_many_returns_requested_slots",
                "baseline::get_many_empty_key_list",
                "baseline::content_addressed_space_returns_identical_bytes",
                "baseline::delete_many_missing_keys_is_idempotent",
                "baseline::delete_many_removes_existing_keys",
                "baseline::delete_range_removes_exact_range",
                "baseline::put_many_applies_after_delete_range",
                "baseline::put_many_overwrites_existing_value",
                "baseline::scan_range_sees_overwritten_existing_value",
                "baseline::scan_range_returns_forward_row_bounded_chunks",
                "baseline::scan_range_caps_owned_pages",
                "baseline::scan_range_honors_bound_variants",
                "baseline::scan_range_resume_before_lower_does_not_widen_range",
                "baseline::scan_range_orders_raw_byte_keys",
                "baseline::scan_range_drains_multi_chunk_limits",
                "baseline::scan_cursor_drains_multi_chunk_limits",
                "baseline::scan_range_empty_range_returns_empty_chunk",
                "baseline::commit_is_atomic",
                "baseline::write_precondition_rejects_stale_value",
                "baseline::rollback_discards_staged_mutations",
                "baseline::rollback_discards_overwrite_and_delete",
                "baseline::begin_read_pins_coherent_view",
                "baseline::scan_cursor_survives_concurrent_commit_and_restarts_exclusively",
                "baseline::unpolled_scan_page_cancellation_keeps_cursor_usable",
                "baseline::descending_scan_is_explicitly_unsupported",
                "baseline::invalid_scan_range_fails_closed",
                "baseline::full_value_and_key_only_are_core",
                "baseline::full_value_preserves_opaque_bytes",
                "baseline::immutable_identity_is_idempotent_and_write_once",
                "model::deterministic_history_matches_reference_model",
            ]
        );
    }
}
