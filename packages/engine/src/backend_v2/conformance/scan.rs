use crate::backend_v2::conformance::{BackendFactory, ConformanceReport};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
    let capabilities = factory.capabilities();

    if capabilities.scan.native_prefix_scan {
        report.add_pending("scan::native_prefix_scan_matches_range_lowering");
    }
    if capabilities.scan.reverse {
        report.add_pending("scan::reverse_returns_descending_keys");
    }
    if capabilities.scan.limit_bytes {
        report.add_pending("scan::limit_bytes_bounds_chunk_size");
    }
    if capabilities.scan.long_lived_cursors {
        report.add_pending("scan::long_lived_cursors_resume_across_read_views");
    }
    if capabilities.scan.parallel_partitions {
        report.add_pending("scan::parallel_partitions_cover_each_key_once");
    }
}
