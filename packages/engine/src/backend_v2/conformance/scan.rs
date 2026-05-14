use std::ops::Bound;

use crate::backend_v2::conformance::{
    fixtures::{full_put, key, put_batch, space},
    BackendFactory, ConformanceReport, ConformanceResult,
};
use crate::backend_v2::{
    Backend, BackendError, BackendRead, BackendWrite, Capability, KeyRange, ReadOptions,
    ScanDirection, ScanOptions, WriteOptions,
};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
    let capabilities = factory.capabilities();

    if !capabilities.scan.reverse {
        report.run("scan::reverse_returns_unsupported_when_not_capable", || {
            reverse_returns_unsupported_when_not_capable(factory)
        });
    }

    if capabilities.scan.native_prefix_scan {
        report.add_pending("scan::native_prefix_scan_matches_range_lowering");
    }
    if capabilities.scan.reverse {
        report.add_pending("scan::reverse_returns_descending_keys");
    }
    if capabilities.scan.limit_bytes {
        report.add_pending("scan::limit_bytes_bounds_page_size");
    }
    if capabilities.scan.long_lived_cursors {
        report.add_pending("scan::long_lived_cursors_resume_across_read_views");
    }
    if capabilities.scan.parallel_partitions {
        report.add_pending("scan::parallel_partitions_cover_each_key_once");
    }
}

fn reverse_returns_unsupported_when_not_capable<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(test_space, put_batch([full_put(key("a"), "A")]))
        .map_err(|error| format!("put_many failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = read.scan_range(
        test_space,
        KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
        ScanOptions {
            direction: ScanDirection::Reverse,
            ..Default::default()
        },
    );
    let Err(error) = result else {
        return Err(format!(
            "reverse scan without capability returned success: {:?}",
            result.ok()
        ));
    };

    match error {
        BackendError::Unsupported(Capability::ReverseScan) => Ok(()),
        other => Err(format!("expected Unsupported(ReverseScan), got {other:?}")),
    }
}
