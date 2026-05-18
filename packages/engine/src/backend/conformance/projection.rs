use crate::backend::conformance::{BackendFactory, ConformanceReport};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
    let capabilities = factory.capabilities();

    if capabilities.projection.header {
        report.add_pending("projection::header_returns_header_without_payload");
    }
    if capabilities.projection.refs {
        report.add_pending("projection::refs_returns_refs_without_payload");
    }
    if capabilities.projection.header_and_refs {
        report.add_pending("projection::header_and_refs_returns_both_without_payload");
    }
    if capabilities.projection.payload {
        report.add_pending("projection::payload_returns_payload_only");
    }
}
