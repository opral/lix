use crate::backend_v2::{
    conformance::{
        fixtures::{full_put, key, put_batch, space},
        BackendFactory, ConformanceReport, ConformanceResult,
    },
    Backend, BackendError, BackendPredicate, BackendRead, BackendWrite, Capability, GetOptions,
    KeyPredicate, PredicateExpr, PredicateId, PredicateSupportLevel, ReadOptions, WriteOptions,
};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
    let capabilities = factory.capabilities();

    if capabilities.pushdown.key == PredicateSupportLevel::None
        && capabilities.pushdown.header == PredicateSupportLevel::None
        && capabilities.pushdown.refs == PredicateSupportLevel::None
    {
        report.run(
            "pushdown::predicate_returns_unsupported_when_not_capable",
            || predicate_returns_unsupported_when_not_capable(factory),
        );
    }

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

fn predicate_returns_unsupported_when_not_capable<F>(factory: &F) -> ConformanceResult
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

    let predicate = BackendPredicate {
        id: PredicateId(1),
        expr: PredicateExpr::Key(KeyPredicate::Eq(test_key.clone())),
    };
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = read.get_many(
        test_space,
        &[test_key],
        GetOptions {
            predicates: &[predicate],
            ..Default::default()
        },
    );
    let Err(error) = result else {
        return Err(format!(
            "unsupported predicate pushdown returned success: {:?}",
            result.ok()
        ));
    };

    match error {
        BackendError::Unsupported(Capability::PredicatePushdown) => Ok(()),
        other => Err(format!(
            "expected Unsupported(PredicatePushdown), got {other:?}"
        )),
    }
}
