use crate::backend_v2::conformance::{BackendFactory, ConformanceReport};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
    let capabilities = factory.capabilities();

    report.add("scan::reverse_returns_unsupported_when_not_capable");

    if capabilities.scan.native_prefix_scan {
        report.add("scan::native_prefix_scan_matches_range_lowering");
    }
    if capabilities.scan.reverse {
        report.add("scan::reverse_returns_descending_keys");
    }
    if capabilities.scan.limit_bytes {
        report.add("scan::limit_bytes_bounds_page_size");
    }
    if capabilities.scan.long_lived_cursors {
        report.add("scan::long_lived_cursors_resume_across_read_views");
    }
    if capabilities.scan.parallel_partitions {
        report.add("scan::parallel_partitions_cover_each_key_once");
    }
}
