use crate::backend_v2::conformance::{BackendFactory, ConformanceReport};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
    let capabilities = factory.capabilities();

    if capabilities.projection.header {
        report.add("projection::header_returns_header_without_payload");
    }
    if capabilities.projection.refs {
        report.add("projection::refs_returns_refs_without_payload");
    }
    if capabilities.projection.header_and_refs {
        report.add("projection::header_and_refs_returns_both_without_payload");
    }
    if capabilities.projection.payload {
        report.add("projection::payload_returns_payload_only");
    }
}
