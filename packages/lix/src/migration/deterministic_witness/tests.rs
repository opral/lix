use super::*;
use crate::storage_adapter::{
    Memory, StorageAdapterRead, StoragePrefix, StorageProjectedValue, StorageReadOptions,
    StorageSpace,
};

async fn snapshot<S: StorageAdapterRead>(read: &S, space: StorageSpace) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut cursor = read
        .begin_scan(
            space,
            StoragePrefix {
                bytes: bytes::Bytes::new(),
            }
            .to_range()
            .unwrap(),
            Default::default(),
        )
        .await
        .unwrap();
    cursor
        .collect_all()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| {
            let StorageProjectedValue::FullValue(value) = entry.value else {
                panic!("native full value");
            };
            (entry.key.0.to_vec(), value.to_vec())
        })
        .collect()
}

async fn fixture() -> (Memory, StorageAdapter<Memory>) {
    let memory = Memory::new();
    let adapter = StorageAdapter::new(memory.clone());
    crate::engine::Engine::initialize_with_adapter(adapter.clone(), None)
        .await
        .unwrap();
    let engine = crate::engine::Engine::new_with_adapter(
        adapter.clone(),
        crate::engine::EngineOptions::new(),
    )
    .await
    .unwrap();
    let session = engine.open_session().await.unwrap();
    session.execute("INSERT INTO lix_key_value (key,value,lixcol_global) VALUES ('witness-global-a','one',true),('witness-global-b','two',true)", &[]).await.unwrap();
    session
        .execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('witness-local','preserved')",
            &[],
        )
        .await
        .unwrap();
    drop(session);
    drop(engine);
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .unwrap();
    let witnesses = snapshot(
        &read,
        crate::hot_state::DETERMINISTIC_IDENTITY_WITNESS_SPACE,
    )
    .await;
    assert!(!witnesses.is_empty());
    drop(read);
    let mut writes = adapter.new_write_set();
    for (key, _) in witnesses {
        writes.delete(
            crate::hot_state::DETERMINISTIC_IDENTITY_WITNESS_SPACE,
            StorageKey(key.into()),
        );
    }
    writes.put(
        crate::init::REPOSITORY_PROTOCOL_SPACE,
        crate::init::REPOSITORY_PROTOCOL_KEY,
        crate::init::REPOSITORY_PROTOCOL_V78,
    );
    adapter
        .commit_write_set(writes, Default::default())
        .await
        .unwrap();
    (memory, adapter)
}

#[tokio::test]
async fn v78_backfill_preserves_native_rows_history_and_controls() {
    let (_, adapter) = fixture().await;
    let spaces = [
        crate::hot_state::ROW_SPACE,
        crate::changelog::COMMIT_SPACE,
        crate::branch::BRANCH_HEAD_CONTROL_SPACE,
        crate::hot_state::COLLECTION_CONTROL_SPACE,
    ];
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let mut before = Vec::new();
    for space in spaces {
        before.push(snapshot(&read, space).await);
    }
    drop(read);
    backfill(&adapter, MigrationOptions::default(), true)
        .await
        .unwrap();
    assert_eq!(
        super::super::api::load_repository_protocol_marker(&adapter)
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        crate::init::REPOSITORY_PROTOCOL_VALUE
    );
    let read = adapter.begin_read(Default::default()).await.unwrap();
    for (space, expected) in spaces.into_iter().zip(before) {
        assert_eq!(snapshot(&read, space).await, expected);
    }
    assert!(
        !snapshot(
            &read,
            crate::hot_state::DETERMINISTIC_IDENTITY_WITNESS_SPACE
        )
        .await
        .is_empty()
    );
    drop(read);
    let engine =
        crate::engine::Engine::new_with_adapter(adapter, crate::engine::EngineOptions::new())
            .await
            .unwrap();
    engine
        .open_session()
        .await
        .unwrap()
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'witness-local'",
            &[],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn v78_backfill_budget_failure_keeps_old_marker_and_native_source() {
    let (_, adapter) = fixture().await;
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let before = snapshot(&read, crate::hot_state::ROW_SPACE).await;
    drop(read);
    let error = backfill(
        &adapter,
        MigrationOptions {
            max_changes: 0,
            max_preflight_bytes: 0,
        },
        true,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED");
    assert_eq!(
        super::super::api::load_repository_protocol_marker(&adapter)
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        crate::init::REPOSITORY_PROTOCOL_V78
    );
    let read = adapter.begin_read(Default::default()).await.unwrap();
    assert_eq!(snapshot(&read, crate::hot_state::ROW_SPACE).await, before);
    assert!(
        snapshot(
            &read,
            crate::hot_state::DETERMINISTIC_IDENTITY_WITNESS_SPACE
        )
        .await
        .is_empty()
    );
}

#[tokio::test]
async fn additive_backfill_keeps_legacy_marker_before_logical_migrations() {
    for marker in [
        b"tracked-default-branch.v72".as_slice(),
        b"tracked-default-branch.v73".as_slice(),
        b"tracked-default-branch.v74".as_slice(),
        crate::init::REPOSITORY_PROTOCOL_V75,
        crate::init::REPOSITORY_PROTOCOL_V76,
        crate::init::REPOSITORY_PROTOCOL_V77,
    ] {
        let (_, adapter) = fixture().await;
        let mut writes = adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            marker,
        );
        adapter
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        backfill(&adapter, MigrationOptions::default(), false)
            .await
            .unwrap();
        assert_eq!(
            super::super::api::load_repository_protocol_marker(&adapter)
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            marker
        );
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert!(
            !snapshot(
                &read,
                crate::hot_state::DETERMINISTIC_IDENTITY_WITNESS_SPACE
            )
            .await
            .is_empty()
        );
    }
}

#[tokio::test]
async fn v78_public_migration_failure_can_resume_without_losing_source() {
    let (memory, adapter) = fixture().await;
    let error = super::super::api::migrate_lix_with_adapter(
        memory.clone(),
        adapter.clone(),
        MigrationOptions {
            max_changes: 0,
            max_preflight_bytes: 0,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED");
    assert_eq!(
        super::super::api::load_repository_protocol_marker(&adapter)
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        crate::init::REPOSITORY_PROTOCOL_V78
    );
    let report = super::super::api::migrate_lix_with_adapter(
        memory,
        adapter.clone(),
        MigrationOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(report.from_version, 78);
    assert_eq!(report.to_version, crate::init::CURRENT_FORMAT_VERSION);
    let engine =
        crate::engine::Engine::new_with_adapter(adapter, crate::engine::EngineOptions::new())
            .await
            .unwrap();
    engine
        .open_session()
        .await
        .unwrap()
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'witness-local'",
            &[],
        )
        .await
        .unwrap();
}
