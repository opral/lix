use crate::backend_v2::{
    conformance::{BackendFactory, ConformanceReport},
    PredicateSupportLevel,
};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
    let capabilities = factory.capabilities();

    if capabilities.pushdown.key != PredicateSupportLevel::None {
        report.add_pending("pushdown::key_support_metadata_is_truthful");
    }
    if capabilities.pushdown.header != PredicateSupportLevel::None {
        report.add_pending("pushdown::header_support_metadata_is_truthful");
    }
    if capabilities.pushdown.refs != PredicateSupportLevel::None {
        report.add_pending("pushdown::refs_support_metadata_is_truthful");
    }
    if capabilities.pushdown.object_pruning != PredicateSupportLevel::None {
        report.add_pending("pushdown::object_pruning_requires_residual_filtering_when_inexact");
    }
}
