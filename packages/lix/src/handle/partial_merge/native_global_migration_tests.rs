use super::*;
#[tokio::test]
async fn global_native_migration_retains_bodies_and_replays_exact_lost_outcome() {
    run_global_migration(false).await;
}
#[tokio::test]
async fn global_migration_restarts_after_authority_moves_with_original_bodies_retained_locally() {
    run_global_migration(true).await;
}
async fn run_global_migration(restart: bool) {
    let memory = crate::Memory::new();
    let authority = crate::open_lix()
        .with_storage(memory.clone())
        .await
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_key_value(key,value) VALUES('base','initial')",
            &[],
        )
        .await
        .unwrap();
    let base = authority.partial_replica_descriptor(None).await.unwrap();
    let local = crate::open_lix()
        .with_storage(memory.fork().unwrap())
        .await
        .unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    let created = local
        .create_branch(crate::CreateBranchOptions {
            id: None,
            name: "migrated-local".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let child = local
        .open_another_session()
        .with_branch(created.id.clone())
        .await
        .unwrap();
    child
        .execute(
            "UPDATE lix_key_value SET value='local-pending' WHERE key='base'",
            &[],
        )
        .await
        .unwrap();
    let source = child
        .partial_replica_descriptor(Some(&created.id))
        .await
        .unwrap();
    let remote_branch = authority
        .create_branch(crate::CreateBranchOptions {
            id: None,
            name: "remote-added".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let remote = authority.partial_replica_descriptor(None).await.unwrap();
    let mut request = crate::sync::NativeGlobalMigrationRequest {
        attempt_id: uuid::Uuid::now_v7().to_string(),
        base_commit_id: base.global_branch.head.commit_id,
        expected_authority_head_commit_id: remote.global_branch.head.commit_id,
        captured_local_head_commit_id: source.global_branch.head.commit_id.clone(),
        checkpoint_commit_id: remote.global_branch.checkpoint.commit_id,
        new_branches: vec![crate::sync::NativeNewBranchCoordinate {
            branch_id: created.id.clone(),
            head_commit_id: source.selected_branch.head.commit_id.clone(),
            checkpoint_commit_id: source.selected_branch.checkpoint.commit_id.clone(),
        }],
    };
    let global = crate::sync::export_sync_commit(&local, &request.captured_local_head_commit_id)
        .await
        .unwrap()
        .unwrap();
    let mut global_wave = crate::sync::NativeGlobalBodyWaveRequest {
        request: request.clone(),
        branch_id: crate::GLOBAL_BRANCH_ID.into(),
        previous_commit_id: request.base_commit_id.clone(),
        bodies: crate::sync::SyncPushRequest {
            commits: vec![global],
            ref_updates: vec![],
            inline_blobs: vec![],
        },
    };
    authority
        .push_global_migration_body_wave_for_account(&global_wave, authority.active_account_id())
        .await
        .unwrap();
    let selected = crate::sync::export_sync_commit(&child, &source.selected_branch.head.commit_id)
        .await
        .unwrap()
        .unwrap();
    let mut selected_wave = crate::sync::NativeGlobalBodyWaveRequest {
        request: request.clone(),
        branch_id: created.id.clone(),
        previous_commit_id: source.selected_branch.checkpoint.commit_id.clone(),
        bodies: crate::sync::SyncPushRequest {
            commits: vec![selected],
            ref_updates: vec![],
            inline_blobs: vec![],
        },
    };
    authority
        .push_global_migration_body_wave_for_account(&selected_wave, authority.active_account_id())
        .await
        .unwrap();
    // Drop both acknowledgments and retry exact bodies before publication.
    authority
        .push_global_migration_body_wave_for_account(&global_wave, authority.active_account_id())
        .await
        .unwrap();
    authority
        .push_global_migration_body_wave_for_account(&selected_wave, authority.active_account_id())
        .await
        .unwrap();
    let adapter = authority.storage_adapter();
    let read = crate::storage_adapter::SharedStorageAdapterRead::new(
        adapter.begin_read(Default::default()).await.unwrap(),
    );
    let mut writes = adapter.new_write_set();
    let mut preconditions = vec![];
    crate::gc::stage_repository_gc_with_preconditions(read, &mut writes, &mut preconditions)
        .await
        .unwrap();
    adapter
        .commit_write_set(
            writes,
            crate::storage_adapter::StorageWriteOptions {
                preconditions,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    if restart {
        let held = adapter.begin_read(Default::default()).await.unwrap();
        let late_guard = crate::sync::require_unaborted_global_migration(
            &held,
            authority.lix_id(),
            authority.active_account_id(),
            &request,
        )
        .await
        .unwrap();
        drop(held);
        authority
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "racing-remote".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let intent = crate::sync::NativeGlobalRestartRequest {
            request: request.clone(),
            next_attempt_id: uuid::Uuid::now_v7().to_string(),
        };
        let outcome = authority
            .restart_native_global_migration_for_account(&intent, authority.active_account_id())
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            crate::sync::NativeGlobalRestartReceipt::Restarted { .. }
        ));
        assert_eq!(
            authority
                .restart_native_global_migration_for_account(&intent, authority.active_account_id())
                .await
                .unwrap(),
            outcome
        );
        assert!(
            adapter
                .commit_write_set(
                    adapter.new_write_set(),
                    crate::storage_adapter::StorageWriteOptions {
                        preconditions: vec![late_guard],
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(
            authority
                .merge_native_global_migration_for_account(&request, authority.active_account_id())
                .await
                .unwrap_err()
                .code,
            "LIX_MIGRATION_GLOBAL_ATTEMPT_RESTARTED"
        );
        request.attempt_id = intent.next_attempt_id;
        request.expected_authority_head_commit_id = authority
            .partial_replica_descriptor(None)
            .await
            .unwrap()
            .global_branch
            .head
            .commit_id;
        global_wave.request = request.clone();
        selected_wave.request = request.clone();
        authority
            .push_global_migration_body_wave_for_account(
                &global_wave,
                authority.active_account_id(),
            )
            .await
            .unwrap();
        authority
            .push_global_migration_body_wave_for_account(
                &selected_wave,
                authority.active_account_id(),
            )
            .await
            .unwrap();
    }
    let receipt = authority
        .merge_native_global_migration_for_account(&request, authority.active_account_id())
        .await
        .unwrap();
    assert_eq!(
        authority
            .merge_native_global_migration_for_account(&request, authority.active_account_id())
            .await
            .unwrap(),
        receipt
    );
    let published = authority
        .partial_replica_descriptor(Some(&created.id))
        .await
        .unwrap();
    assert_eq!(
        published.selected_branch.head.commit_id,
        source.selected_branch.head.commit_id
    );
    assert_eq!(
        published.selected_branch.checkpoint.commit_id,
        source.selected_branch.checkpoint.commit_id
    );
    let view = authority
        .open_another_session()
        .with_branch(created.id.clone())
        .await
        .unwrap();
    let rows = view
        .execute("SELECT value FROM lix_key_value WHERE key='base'", &[])
        .await
        .unwrap();
    let crate::Value::Jsonb(value) = rows.rows()[0].get::<crate::Value>("value").unwrap() else {
        panic!("expected JSONB value")
    };
    assert_eq!(value.as_json_string().as_deref(), Some("local-pending"));
    assert_eq!(
        view.execute(
            "SELECT diff_type FROM lix_diff('lix_key_value') WHERE key='base'",
            &[]
        )
        .await
        .unwrap()
        .rows()
        .len(),
        1,
        "migration must preserve the original dirty checkpoint interval"
    );
    assert!(
        authority
            .partial_replica_descriptor(Some(&remote_branch.id))
            .await
            .is_ok()
    );
    let row = crate::sync::export_sync_commit(&authority, &receipt.merge_commit_id)
        .await
        .unwrap()
        .unwrap();
    assert!(row.global_scope);
    assert!(row.base_commit_id.is_none());
    assert_eq!(
        row.parent_commit_ids,
        vec![
            request.expected_authority_head_commit_id.clone(),
            request.captured_local_head_commit_id.clone()
        ]
    );
    let restart_after_commit = crate::sync::NativeGlobalRestartRequest {
        request: request.clone(),
        next_attempt_id: uuid::Uuid::now_v7().to_string(),
    };
    assert_eq!(
        authority
            .restart_native_global_migration_for_account(
                &restart_after_commit,
                authority.active_account_id()
            )
            .await
            .unwrap(),
        crate::sync::NativeGlobalRestartReceipt::Committed {
            receipt: receipt.clone()
        }
    );
    assert!(
        authority
            .cleanup_native_global_migration_for_account(&request, authority.active_account_id())
            .await
            .unwrap()
    );
    assert!(
        !authority
            .cleanup_native_global_migration_for_account(&request, authority.active_account_id())
            .await
            .unwrap()
    );
}
#[tokio::test]
async fn global_migration_rejects_existing_target_even_when_its_head_matches() {
    let memory = crate::Memory::new();
    let authority = crate::open_lix()
        .with_storage(memory.clone())
        .await
        .unwrap();
    let base = authority.partial_replica_descriptor(None).await.unwrap();
    let local = crate::open_lix()
        .with_storage(memory.fork().unwrap())
        .await
        .unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    let branch = local
        .create_branch(crate::CreateBranchOptions {
            id: None,
            name: "local-name".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    authority
        .create_branch(crate::CreateBranchOptions {
            id: Some(branch.id.clone()),
            name: "concurrent-name".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let source = local
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap();
    let remote = authority
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap();
    assert_eq!(
        source.selected_branch.head.commit_id,
        remote.selected_branch.head.commit_id
    );
    let request = crate::sync::NativeGlobalMigrationRequest {
        attempt_id: uuid::Uuid::now_v7().to_string(),
        base_commit_id: base.global_branch.head.commit_id,
        expected_authority_head_commit_id: remote.global_branch.head.commit_id.clone(),
        captured_local_head_commit_id: source.global_branch.head.commit_id.clone(),
        checkpoint_commit_id: remote.global_branch.checkpoint.commit_id.clone(),
        new_branches: vec![crate::sync::NativeNewBranchCoordinate {
            branch_id: branch.id.clone(),
            head_commit_id: source.selected_branch.head.commit_id.clone(),
            checkpoint_commit_id: source.selected_branch.checkpoint.commit_id.clone(),
        }],
    };
    let error = authority
        .merge_native_global_migration_for_account(&request, authority.active_account_id())
        .await
        .unwrap_err();
    assert_eq!(error.code, "LIX_MIGRATION_GLOBAL_SCOPE_UNSUPPORTED");
    assert_eq!(
        authority
            .partial_replica_descriptor(Some(&branch.id))
            .await
            .unwrap(),
        remote
    );
    assert_eq!(
        local
            .partial_replica_descriptor(Some(&branch.id))
            .await
            .unwrap(),
        source
    );
}
