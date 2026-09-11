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
/// First complete the ordinary repository-format migration using the existing
/// opening workflow. Close all handles before passing storage here. This operation
/// preserves repository rows and atomically fences authorities predating leases;
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
    let adapter = super::admit_existing_repository(storage).await?;
    let read = adapter.begin_read(Default::default()).await?;
    if !matches!(
        super::inspect_lix_with_adapter(&adapter).await?,
        super::MigrationStatus::Current { .. }
    ) {
        return Err(LixError::new(
            "LIX_AUTHORITY_UPGRADE_REQUIRED",
            "authority fence upgrade requires current native full format",
        ));
    }
    let values = PointReadPlan::new(
        crate::sync::SYNC_AUTHORITY_STATE_SPACE,
        &[crate::sync::authority_state_key()],
    )
    .materialize(&read, Default::default())
    .await?
    .value;
    let Some(StorageProjectedValue::FullValue(marker)) = values.into_iter().next().flatten() else {
        return Err(LixError::new(
            "LIX_AUTHORITY_UPGRADE_REQUIRED",
            "existing authority marker is missing",
        ));
    };
    if marker.as_ref() == crate::sync::AUTHORITY_STATE_VALUE {
        return Ok(());
    }
    if marker.as_ref() != PRE_LEASE_AUTHORITY_MARKER {
        return Err(LixError::new(
            "LIX_AUTHORITY_UPGRADE_REQUIRED",
            "authority marker is not a supported upgrade source",
        ));
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

#[cfg(all(test, feature = "server-protocol"))]
mod tests {
    use super::*;
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
