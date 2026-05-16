use crate::backend_v2::conformance::{
    fixtures::{full_put, key, put_batch, space},
    BackendFactory, BackendFixture, ConformanceReport, ConformanceResult,
};
use crate::backend_v2::{
    get_many as backend_get_many, Backend, BackendWrite, GetOptions, ProjectedValue, ReadOptions,
    WriteOptions,
};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
    report.run("persistence::committed_data_survives_reopen", || {
        committed_data_survives_reopen(factory)
    });
    report.run(
        "persistence::rolled_back_data_does_not_survive_reopen",
        || rolled_back_data_does_not_survive_reopen(factory),
    );
    report.run(
        "persistence::overwrite_and_delete_final_state_survives_reopen",
        || overwrite_and_delete_final_state_survives_reopen(factory),
    );
}

fn committed_data_survives_reopen<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let fixture = factory.create_fixture();
    let test_space = space(71);
    let alpha = key("alpha");
    let beta = key("beta");

    {
        let backend = fixture.open();
        let mut write = backend
            .begin_write(WriteOptions::default())
            .map_err(|error| format!("begin_write failed: {error}"))?;
        write
            .put_many(put_batch([
                full_put(alpha.clone(), "persisted-alpha"),
                full_put(beta.clone(), "persisted-beta"),
            ]))
            .map_err(|error| format!("put_many failed: {error}"))?;
        write
            .commit()
            .map_err(|error| format!("commit failed: {error}"))?;
    }

    let reopened = fixture.open();
    assert_full_values(
        &reopened,
        test_space,
        &[
            (alpha, Some("persisted-alpha")),
            (beta, Some("persisted-beta")),
        ],
    )
}

fn rolled_back_data_does_not_survive_reopen<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let fixture = factory.create_fixture();
    let test_space = space(72);
    let rolled_back = key("rolled-back");

    {
        let backend = fixture.open();
        let mut write = backend
            .begin_write(WriteOptions::default())
            .map_err(|error| format!("begin_write failed: {error}"))?;
        write
            .put_many(put_batch([full_put(
                rolled_back.clone(),
                "should-not-persist",
            )]))
            .map_err(|error| format!("put_many failed: {error}"))?;
        write
            .rollback()
            .map_err(|error| format!("rollback failed: {error}"))?;
    }

    let reopened = fixture.open();
    assert_full_values(&reopened, test_space, &[(rolled_back, None)])
}

fn overwrite_and_delete_final_state_survives_reopen<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let fixture = factory.create_fixture();
    let test_space = space(73);
    let overwritten = key("overwritten");
    let deleted = key("deleted");

    {
        let backend = fixture.open();
        let mut write = backend
            .begin_write(WriteOptions::default())
            .map_err(|error| format!("begin_write failed: {error}"))?;
        write
            .put_many(put_batch([
                full_put(overwritten.clone(), "old"),
                full_put(deleted.clone(), "delete-me"),
            ]))
            .map_err(|error| format!("initial put_many failed: {error}"))?;
        write
            .commit()
            .map_err(|error| format!("initial commit failed: {error}"))?;
    }

    {
        let backend = fixture.open();
        let mut write = backend
            .begin_write(WriteOptions::default())
            .map_err(|error| format!("begin_write failed: {error}"))?;
        write
            .put_many(put_batch([full_put(overwritten.clone(), "new")]))
            .map_err(|error| format!("overwrite put_many failed: {error}"))?;
        write
            .delete_many(&[deleted.clone()])
            .map_err(|error| format!("delete_many failed: {error}"))?;
        write
            .commit()
            .map_err(|error| format!("final commit failed: {error}"))?;
    }

    let reopened = fixture.open();
    assert_full_values(
        &reopened,
        test_space,
        &[(overwritten, Some("new")), (deleted, None)],
    )
}

fn assert_full_values<B>(
    backend: &B,
    _test_space: crate::backend_v2::SpaceId,
    expected: &[(crate::backend_v2::Key, Option<&str>)],
) -> ConformanceResult
where
    B: Backend,
{
    let keys = expected
        .iter()
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = backend_get_many(&read, &keys, GetOptions::default())
        .map_err(|error| format!("get_many failed: {error}"))?;

    for (index, (key, expected_value)) in expected.iter().enumerate() {
        let actual = match result.values.get(index).and_then(|value| value.as_ref()) {
            Some(ProjectedValue::FullValue(bytes)) => Some(
                std::str::from_utf8(bytes.as_ref())
                    .map_err(|error| format!("slot {index} contained non-utf8 bytes: {error}"))?,
            ),
            Some(other) => {
                return Err(format!("slot {index} returned unexpected value: {other:?}"));
            }
            None => None,
        };
        if actual != *expected_value {
            return Err(format!(
                "key {:?} value mismatch: expected {:?}, got {:?}",
                key, expected_value, actual
            ));
        }
    }

    Ok(())
}
