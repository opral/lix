//! Explicit authority capability upgrade. This is not a normal-open fallback:
//! older authorities reject the new marker and already-admitted older writers
//! fail the exact marker precondition on every ordinary storage commit.
use crate::{
    LixError,
    storage_adapter::{
        PointReadPlan, PutBatch, PutEntry, Storage, StoragePrecondition, StorageProjectedValue,
        StorageValue, StorageWrite, StorageWriteOptions,
    },
};
use bytes::Bytes;
const PRE_LEASE_AUTHORITY_MARKER: &[u8] = b"certified-authority-v4";

/// Explicitly upgrades an existing authority to support partial-replica baseline leases.
///
/// Close all handles before passing storage here. After checking authority
/// eligibility, this explicitly runs registered full-format migrations (v72 onward)
/// and atomically fences authorities predating leases. It preserves repository rows;
/// it is never performed implicitly by partial-replica opening.
pub async fn upgrade_authority_for_partial_sync<S>(storage: S) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let session = crate::storage_adapter::StorageSession::acquire(storage).await?;
    upgrade_authority_native_baseline_fence(&session).await
}

pub(crate) async fn upgrade_authority_native_baseline_fence<S>(storage: &S) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    // Admission can migrate an old epoch. Validate the source first so this
    // explicit authority operation never upgrades a replica or ordinary store.
    let source = super::epoch::inspect_existing_epoch_adapter(storage).await?;
    let source_read = source.begin_read(Default::default()).await?;
    let original_marker = supported_authority_marker(&source_read).await?;
    if crate::sync::has_any_sync_replica_state(&source_read).await? {
        return Err(LixError::new(
            "LIX_AUTHORITY_UPGRADE_REQUIRED",
            "replica storage cannot be upgraded as an authority",
        ));
    }
    drop(source_read);
    drop(source);
    // Reuse the ordinary registered, resumable epoch migration rather than
    // rewriting the old source in place or constructing an engine.
    let adapter = super::admit_existing_repository(storage).await?;
    let read = adapter.begin_read(Default::default()).await?;
    let marker = supported_authority_marker(&read).await?;
    if marker != original_marker || crate::sync::has_any_sync_replica_state(&read).await? {
        return Err(LixError::new(
            "LIX_AUTHORITY_UPGRADE_REQUIRED",
            "authority identity changed during format migration",
        ));
    }
    if marker.as_ref() == crate::sync::AUTHORITY_STATE_VALUE {
        return Ok(());
    }
    let revision = crate::storage_adapter::load_repository_mutation_revision(&read).await?;
    drop(read);
    let mut write = adapter
        .begin_migration_write(StorageWriteOptions {
            await_durable: true,
            preconditions: vec![
                StoragePrecondition::KeyValueEquals {
                    space: crate::sync::SYNC_AUTHORITY_STATE_SPACE,
                    key: crate::sync::authority_state_key(),
                    expected: marker,
                },
                StoragePrecondition::KeyAbsent {
                    space: crate::sync::SYNC_REPLICA_STATE_SPACE,
                    key: crate::sync::replica_state_key(),
                },
                StoragePrecondition::KeyAbsent {
                    space: crate::sync::PARTIAL_REPLICA_STATE_SPACE,
                    key: crate::sync::partial_replica_state_key(),
                },
                crate::storage_adapter::repository_mutation_revision_precondition(revision),
            ],
            ..Default::default()
        })
        .await?;
    write
        .put_many(
            crate::sync::SYNC_AUTHORITY_STATE_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: crate::sync::authority_state_key(),
                    value: StorageValue {
                        bytes: Bytes::from_static(crate::sync::AUTHORITY_STATE_VALUE),
                    },
                }],
            },
        )
        .await?;
    crate::storage_adapter::stage_mutation_revision(&mut write).await?;
    write.commit().await?;
    Ok(())
}

async fn supported_authority_marker(
    read: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
) -> Result<Bytes, LixError> {
    let values = PointReadPlan::new(
        crate::sync::SYNC_AUTHORITY_STATE_SPACE,
        &[crate::sync::authority_state_key()],
    )
    .materialize(read, Default::default())
    .await?
    .value;
    let Some(StorageProjectedValue::FullValue(marker)) = values.into_iter().next().flatten() else {
        return Err(LixError::new(
            "LIX_AUTHORITY_UPGRADE_REQUIRED",
            "existing authority marker is missing",
        ));
    };
    if marker.as_ref() != PRE_LEASE_AUTHORITY_MARKER
        && marker.as_ref() != crate::sync::AUTHORITY_STATE_VALUE
    {
        return Err(LixError::new(
            "LIX_AUTHORITY_UPGRADE_REQUIRED",
            "authority marker is not a supported upgrade source",
        ));
    }
    Ok(marker)
}

#[cfg(all(test, feature = "server-protocol"))]
mod tests {
    use super::*;
    #[tokio::test]
    async fn released_v75_authority_upgrades_preserving_rows_and_blob() {
        let storage = crate::Memory::new();
        let session = crate::storage_adapter::StorageSession::acquire(storage.clone())
            .await
            .unwrap();
        let session = crate::snapshot::restore_snapshot(
            session,
            futures_lite::io::Cursor::new(
                include_bytes!("../../tests/fixtures/v75_released_repository.lixsnap").as_slice(),
            ),
        )
        .await
        .unwrap();
        let source = super::super::epoch::inspect_existing_epoch_adapter(&session)
            .await
            .unwrap();
        assert!(matches!(
            super::super::inspect_lix_with_adapter(&source)
                .await
                .unwrap(),
            super::super::MigrationStatus::Required {
                from_version: 75,
                ..
            }
        ));
        let mut write = source
            .begin_migration_write(StorageWriteOptions {
                await_durable: true,
                ..Default::default()
            })
            .await
            .unwrap();
        write
            .put_many(
                crate::sync::SYNC_AUTHORITY_STATE_SPACE,
                PutBatch {
                    entries: vec![PutEntry {
                        key: crate::sync::authority_state_key(),
                        value: StorageValue {
                            bytes: Bytes::from_static(PRE_LEASE_AUTHORITY_MARKER),
                        },
                    }],
                },
            )
            .await
            .unwrap();
        write.commit().await.unwrap();
        drop(source);
        drop(session);
        upgrade_authority_for_partial_sync(storage.clone())
            .await
            .unwrap();
        upgrade_authority_for_partial_sync(storage.clone())
            .await
            .unwrap();
        let session = crate::storage_adapter::StorageSession::acquire(storage)
            .await
            .unwrap();
        let adapter = super::super::admit_existing_repository(&session)
            .await
            .unwrap();
        crate::sync::admit_sync_authority_storage(&adapter, None)
            .await
            .unwrap();
        let engine =
            crate::engine::Engine::new_with_adapter(adapter, crate::engine::EngineOptions::new())
                .await
                .unwrap();
        let reader = engine.open_session().await.unwrap();
        let value = reader
            .execute(
                "SELECT value FROM lix_key_value WHERE key='fixture-shared'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            value.rows()[0].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!({ "generation": 75, "lane": "main" })
        );
        let blob = reader
            .execute(
                "SELECT content FROM lix_file WHERE path='/docs/released-v75.bin'",
                &[],
            )
            .await
            .unwrap();
        let expected: Vec<u8> = (0..65_537)
            .map(|index| ((index * 31 + index / 251) % 256) as u8)
            .collect();
        assert_eq!(
            blob.rows()[0].values(),
            &[crate::Value::Blob(expected.into())]
        );
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn non_authority_v75_is_rejected_before_format_migration() {
        let storage = crate::Memory::new();
        let session = crate::storage_adapter::StorageSession::acquire(storage.clone())
            .await
            .unwrap();
        let session = crate::snapshot::restore_snapshot(
            session,
            futures_lite::io::Cursor::new(
                include_bytes!("../../tests/fixtures/v75_released_repository.lixsnap").as_slice(),
            ),
        )
        .await
        .unwrap();
        drop(session);
        assert!(
            upgrade_authority_for_partial_sync(storage.clone())
                .await
                .is_err()
        );
        let session = crate::storage_adapter::StorageSession::acquire(storage)
            .await
            .unwrap();
        let source = super::super::epoch::inspect_existing_epoch_adapter(&session)
            .await
            .unwrap();
        assert!(matches!(
            super::super::inspect_lix_with_adapter(&source)
                .await
                .unwrap(),
            super::super::MigrationStatus::Required {
                from_version: 75,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn explicit_authority_upgrade_fences_older_writer_and_preserves_rows() {
        let storage = crate::sync::durable_memory_for_test(crate::Memory::new());
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('authority-upgrade','kept')",
            &[],
        )
        .await
        .unwrap();
        let adapter = lix.storage_adapter();
        let mut seed = adapter.new_write_set();
        seed.put(
            crate::sync::SYNC_AUTHORITY_STATE_SPACE,
            crate::sync::authority_state_key(),
            PRE_LEASE_AUTHORITY_MARKER,
        );
        adapter
            .commit_write_set(seed, Default::default())
            .await
            .unwrap();
        let prior_guard = StoragePrecondition::KeyValueEquals {
            space: crate::sync::SYNC_AUTHORITY_STATE_SPACE,
            key: crate::sync::authority_state_key(),
            expected: Bytes::from_static(PRE_LEASE_AUTHORITY_MARKER),
        };
        lix.close().await.unwrap();
        drop(lix);
        {
            let inspection = crate::storage_adapter::StorageSession::acquire(storage.clone())
                .await
                .unwrap();
            let adapter = super::super::admit_existing_repository(&inspection)
                .await
                .unwrap();
            assert!(
                crate::sync::admit_sync_authority_storage(&adapter, None)
                    .await
                    .is_err(),
                "normal admission must not silently upgrade an old authority"
            );
        }
        upgrade_authority_for_partial_sync(storage.clone())
            .await
            .unwrap();
        upgrade_authority_for_partial_sync(storage.clone())
            .await
            .unwrap();
        let storage = crate::storage_adapter::StorageSession::acquire(storage)
            .await
            .unwrap();
        let adapter = super::super::admit_existing_repository(&storage)
            .await
            .unwrap();
        // This is exactly the durable predicate compiled into the previous
        // admitted authority writer, including its GC and binary CAS commits.
        let old_write = adapter
            .begin_migration_write(StorageWriteOptions {
                preconditions: vec![prior_guard],
                ..Default::default()
            })
            .await;
        match old_write {
            Err(_) => {}
            Ok(write) => assert!(write.commit().await.is_err()),
        };
        crate::sync::admit_sync_authority_storage(&adapter, None)
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let values = PointReadPlan::new(
            crate::sync::SYNC_AUTHORITY_STATE_SPACE,
            &[crate::sync::authority_state_key()],
        )
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value;
        assert!(
            matches!(&values[0],Some(StorageProjectedValue::FullValue(bytes)) if bytes.as_ref()==crate::sync::AUTHORITY_STATE_VALUE)
        );
        drop(read);
        let engine = crate::engine::Engine::new_with_adapter(
            adapter.clone(),
            crate::engine::EngineOptions::new(),
        )
        .await
        .unwrap();
        let session = engine.open_session().await.unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT value FROM lix_key_value WHERE key='authority-upgrade'",
                    &[]
                )
                .await
                .unwrap()
                .rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!("kept")
        );
        session.close().await.unwrap();
    }
}
