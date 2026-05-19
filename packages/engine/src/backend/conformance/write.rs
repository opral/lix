use crate::backend::conformance::{BackendFactory, ConformanceReport};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
    let capabilities = factory.capabilities();

    if capabilities.write.preconditions {
        report.add_pending("write::preconditions_are_bound_to_commit");
        report.add_pending("write::precondition_failures_identify_items");
    }
    if capabilities.write.idempotent_commit {
        report.add_pending("write::idempotent_commit_retries_without_duplicate_effects");
    }
}
