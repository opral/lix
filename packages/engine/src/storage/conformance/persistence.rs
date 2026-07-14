use crate::storage::conformance::{
    ConformanceReport, ConformanceResult, StorageFactory, StorageFixture,
    fixtures::{full_put, key, put_batch, space},
};
use crate::storage::{
    GetOptions, ProjectedValue, ReadOptions, SpaceId, Storage, StorageRead, StorageWrite,
    WriteOptions,
};

/// Single space used by these fixtures; cross-space isolation is pinned
/// by the baseline cross-space tests.
const TEST_SPACE: SpaceId = SpaceId(7);

pub(crate) async fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: StorageFactory,
{
    report
        .run(
            "persistence::committed_data_survives_reopen",
            committed_data_survives_reopen(factory),
        )
        .await;
    report
        .run(
            "persistence::rolled_back_data_does_not_survive_reopen",
            rolled_back_data_does_not_survive_reopen(factory),
        )
        .await;
    report
        .run(
            "persistence::overwrite_and_delete_final_state_survives_reopen",
            overwrite_and_delete_final_state_survives_reopen(factory),
        )
        .await;
}

async fn committed_data_survives_reopen<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let fixture = factory.create_fixture();
    let test_space = space(71);
    let alpha = key("alpha");
    let beta = key("beta");

    {
        let storage = fixture.open().await;
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .map_err(|error| format!("begin_write failed: {error}"))?;
        write
            .put_many(
                TEST_SPACE,
                put_batch([
                    full_put(alpha.clone(), "persisted-alpha"),
                    full_put(beta.clone(), "persisted-beta"),
                ]),
            )
            .await
            .map_err(|error| format!("put_many failed: {error}"))?;
        write
            .commit()
            .await
            .map_err(|error| format!("commit failed: {error}"))?;
    }

    let reopened = fixture.open().await;
    assert_full_values(
        &reopened,
        test_space,
        &[
            (alpha, Some("persisted-alpha")),
            (beta, Some("persisted-beta")),
        ],
    )
    .await
}

async fn rolled_back_data_does_not_survive_reopen<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let fixture = factory.create_fixture();
    let test_space = space(72);
    let rolled_back = key("rolled-back");

    {
        let storage = fixture.open().await;
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .map_err(|error| format!("begin_write failed: {error}"))?;
        write
            .put_many(
                TEST_SPACE,
                put_batch([full_put(rolled_back.clone(), "should-not-persist")]),
            )
            .await
            .map_err(|error| format!("put_many failed: {error}"))?;
        write
            .rollback()
            .await
            .map_err(|error| format!("rollback failed: {error}"))?;
    }

    let reopened = fixture.open().await;
    assert_full_values(&reopened, test_space, &[(rolled_back, None)]).await
}

async fn overwrite_and_delete_final_state_survives_reopen<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let fixture = factory.create_fixture();
    let test_space = space(73);
    let overwritten = key("overwritten");
    let deleted = key("deleted");

    {
        let storage = fixture.open().await;
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .map_err(|error| format!("begin_write failed: {error}"))?;
        write
            .put_many(
                TEST_SPACE,
                put_batch([
                    full_put(overwritten.clone(), "old"),
                    full_put(deleted.clone(), "delete-me"),
                ]),
            )
            .await
            .map_err(|error| format!("initial put_many failed: {error}"))?;
        write
            .commit()
            .await
            .map_err(|error| format!("initial commit failed: {error}"))?;
    }

    {
        let storage = fixture.open().await;
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .map_err(|error| format!("begin_write failed: {error}"))?;
        write
            .put_many(
                TEST_SPACE,
                put_batch([full_put(overwritten.clone(), "new")]),
            )
            .await
            .map_err(|error| format!("overwrite put_many failed: {error}"))?;
        write
            .delete_many(TEST_SPACE, std::slice::from_ref(&deleted))
            .await
            .map_err(|error| format!("delete_many failed: {error}"))?;
        write
            .commit()
            .await
            .map_err(|error| format!("final commit failed: {error}"))?;
    }

    let reopened = fixture.open().await;
    assert_full_values(
        &reopened,
        test_space,
        &[(overwritten, Some("new")), (deleted, None)],
    )
    .await
}

async fn assert_full_values<StorageImpl>(
    storage: &StorageImpl,
    _test_space: SpaceId,
    expected: &[(crate::storage::Key, Option<&str>)],
) -> ConformanceResult
where
    StorageImpl: Storage,
{
    let keys = expected
        .iter()
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = read
        .get_many(TEST_SPACE, &keys, GetOptions::default())
        .await
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
                "key {key:?} value mismatch: expected {expected_value:?}, got {actual:?}"
            ));
        }
    }

    Ok(())
}
