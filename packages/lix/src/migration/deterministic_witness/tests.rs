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

#[tokio::test]
async fn v78_existing_incomplete_merge_catalog_is_rebuilt_without_changing_pending_history() {
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
    session
        .execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('base','B'), ('deleted-before-migration','gone')",
            &[],
        )
        .await
        .unwrap();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let historical_head = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load(&session.active_branch_id().await.unwrap())
        .await
        .unwrap()
        .unwrap()
        .head_commit_id;
    drop(read);
    session
        .execute(
            "DELETE FROM lix_key_value WHERE key='deleted-before-migration'",
            &[],
        )
        .await
        .unwrap();
    let checkpoint = session.create_checkpoint().await.unwrap();
    let branch = session
        .create_branch(crate::CreateBranchOptions {
            id: None,
            name: "migration-source".into(),
            from_commit_id: Some(checkpoint.commit_id),
        })
        .await
        .unwrap();
    let source = engine.open_session_at(branch.id.clone()).await.unwrap();
    source
        .execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('local-only','L')",
            &[],
        )
        .await
        .unwrap();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let source_head = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load(&branch.id)
        .await
        .unwrap()
        .unwrap()
        .head_commit_id;
    let wrong_root = crate::tracked_state::load_commit_state_manifest(&read, source_head)
        .await
        .unwrap()
        .unwrap()
        .row_pk_index_root_id
        .unwrap();
    drop(read);
    session
        .execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('remote-only','R')",
            &[],
        )
        .await
        .unwrap();
    session
        .merge_branch(crate::MergeBranchOptions {
            source_branch_id: branch.id,
        })
        .await
        .unwrap();
    let selected = session.active_branch_id().await.unwrap();
    drop(source);
    drop(session);
    drop(engine);
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let control = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load(&selected)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        Some(control.head_commit_id),
        control.working_diff_checkpoint_commit_id,
        "native pending edits must be preserved"
    );
    let mut manifest =
        crate::tracked_state::load_commit_state_manifest(&read, control.head_commit_id)
            .await
            .unwrap()
            .unwrap();
    manifest.row_pk_index_root_id = Some(wrong_root.clone());
    let expected_revision = crate::storage_adapter::load_repository_mutation_revision(&read)
        .await
        .unwrap();
    let spaces = [
        crate::hot_state::ROW_SPACE,
        crate::changelog::COMMIT_SPACE,
        crate::branch::BRANCH_HEAD_CONTROL_SPACE,
    ];
    let mut before = Vec::new();
    for space in spaces {
        before.push(snapshot(&read, space).await);
    }
    drop(read);
    // Reproduce the released writer's valid but incomplete selected-parent
    // catalog. All canonical native state/history remains intact.
    let mut plan = crate::migration::publish::PublicationPlan::bounded(8, 1024 * 1024);
    for (space, key, value) in
        crate::tracked_state::encode_commit_state_manifest_replacement_for_migration(&manifest)
            .unwrap()
    {
        plan.replace_immutable(space, vec![(key, value)]).unwrap();
    }
    crate::migration::publish::publish(
        &adapter,
        expected_revision,
        crate::init::REPOSITORY_PROTOCOL_VALUE,
        crate::init::REPOSITORY_PROTOCOL_V78,
        plan,
    )
    .await
    .unwrap();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let keys = crate::tracked_state::TrackedStateContext::new()
        .reader(&read)
        .enumerate_schema_row_pk_keys_at_commit(
            control.head_commit_id,
            "lix_key_value",
            &[crate::row_pk::RowPk::single("remote-only")],
        )
        .await
        .unwrap();
    assert!(
        keys.is_empty(),
        "fixture must have an existing but incomplete catalog"
    );
    drop(read);
    let error = super::super::api::migrate_lix_with_adapter(
        memory.clone(),
        adapter.clone(),
        MigrationOptions {
            max_changes: 1,
            ..MigrationOptions::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED");
    let read = adapter.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        crate::tracked_state::load_commit_state_manifest(&read, control.head_commit_id)
            .await
            .unwrap()
            .unwrap()
            .row_pk_index_root_id,
        Some(wrong_root)
    );
    drop(read);
    super::super::api::migrate_lix_with_adapter(
        memory,
        adapter.clone(),
        MigrationOptions::default(),
    )
    .await
    .unwrap();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    for (space, expected) in spaces.into_iter().zip(before) {
        assert_eq!(snapshot(&read, space).await, expected);
    }
    let keys = crate::tracked_state::TrackedStateContext::new()
        .reader(&read)
        .enumerate_schema_row_pk_keys_at_commit(
            control.head_commit_id,
            "lix_key_value",
            &[crate::row_pk::RowPk::single("remote-only")],
        )
        .await
        .unwrap();
    assert_eq!(keys.len(), 1);
    for commit_id in [historical_head, control.head_commit_id] {
        let keys = crate::tracked_state::TrackedStateContext::new()
            .reader(&read)
            .enumerate_schema_row_pk_keys_at_commit(
                commit_id,
                "lix_key_value",
                &[crate::row_pk::RowPk::single("deleted-before-migration")],
            )
            .await
            .unwrap();
        assert_eq!(
            keys.len(),
            1,
            "retain the original identity and its explicit tombstone"
        );
    }
    drop(read);
    let engine =
        crate::engine::Engine::new_with_adapter(adapter, crate::engine::EngineOptions::new())
            .await
            .unwrap();
    let session = engine.open_session().await.unwrap();
    assert!(
        session
            .execute(
                "SELECT value FROM lix_key_value WHERE key='deleted-before-migration'",
                &[]
            )
            .await
            .unwrap()
            .rows()
            .is_empty()
    );
    for key in ["local-only", "remote-only"] {
        assert_eq!(
            session
                .execute(
                    "SELECT value FROM lix_key_value WHERE key=$1",
                    &[crate::Value::Text(key.into())]
                )
                .await
                .unwrap()
                .rows()
                .len(),
            1
        );
    }
}
