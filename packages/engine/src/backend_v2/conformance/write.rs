use crate::backend_v2::conformance::{BackendFactory, ConformanceReport};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
    let capabilities = factory.capabilities();

    if capabilities.write.delete_range {
        report.add("write::delete_range_removes_exact_range");
    }
    if capabilities.write.preconditions {
        report.add("write::preconditions_are_bound_to_commit");
        report.add("write::precondition_failures_identify_items");
    }
    if capabilities.write.idempotent_commit {
        report.add("write::idempotent_commit_retries_without_duplicate_effects");
    }
}
