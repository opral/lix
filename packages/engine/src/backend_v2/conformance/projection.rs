use crate::backend_v2::conformance::{
    fixtures::{full_put, key, put_batch, space},
    BackendFactory, ConformanceReport, ConformanceResult,
};
use crate::backend_v2::{
    Backend, BackendError, BackendRead, BackendWrite, Capability, GetOptions, ReadOptions,
    ValueProjection, WriteOptions,
};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
    let capabilities = factory.capabilities();

    if !capabilities.projection.header {
        report.run(
            "projection::header_returns_unsupported_when_not_capable",
            || projection_returns_unsupported_when_not_capable(factory, ValueProjection::Header),
        );
    }
    if !capabilities.projection.refs {
        report.run(
            "projection::refs_returns_unsupported_when_not_capable",
            || projection_returns_unsupported_when_not_capable(factory, ValueProjection::Refs),
        );
    }
    if !capabilities.projection.header_and_refs {
        report.run(
            "projection::header_and_refs_returns_unsupported_when_not_capable",
            || {
                projection_returns_unsupported_when_not_capable(
                    factory,
                    ValueProjection::HeaderAndRefs,
                )
            },
        );
    }
    if !capabilities.projection.payload {
        report.run(
            "projection::payload_returns_unsupported_when_not_capable",
            || projection_returns_unsupported_when_not_capable(factory, ValueProjection::Payload),
        );
    }

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

fn projection_returns_unsupported_when_not_capable<F>(
    factory: &F,
    projection: ValueProjection,
) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    let test_key = key("a");

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(test_space, put_batch([full_put(test_key.clone(), "A")]))
        .map_err(|error| format!("put_many failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = read.get_many(
        test_space,
        &[test_key],
        GetOptions {
            projection,
            ..Default::default()
        },
    );
    let Err(error) = result else {
        return Err(format!(
            "unsupported projection returned success: {:?}",
            result.ok()
        ));
    };

    match error {
        BackendError::Unsupported(Capability::Projection(actual)) if actual == projection => Ok(()),
        other => Err(format!(
            "expected Unsupported(Projection({projection:?})), got {other:?}"
        )),
    }
}
