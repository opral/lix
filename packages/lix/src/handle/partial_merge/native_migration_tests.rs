use super::*;
// Integration regression to register with typed migration validation admission.
// Both branches are valid individually; selected native refs alone would miss
// this cross-row UNIQUE collision. Failed merge must preserve both histories.
#[tokio::test]
async fn migration_native_merge_validates_disjoint_custom_unique_rows() {
    let authority = open_lix().await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    let schema = serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"migration_unique_probe","columns":[{"name":"id","type":"text","nullable":false},{"name":"value","type":"text","nullable":false}],"primary_key":["id"],"unique":[["value"]]});
    authority
        .execute(
            "INSERT INTO lix_registered_schema(value) VALUES(CAST($1 AS JSONB))",
            &[Value::Text(schema.to_string())],
        )
        .await
        .unwrap();
    let branch = authority
        .create_branch(CreateBranchOptions {
            id: None,
            name: "migration-source".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let source = authority
        .open_another_session()
        .with_branch(branch.id.clone())
        .await
        .unwrap();
    let base = source
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap()
        .selected_branch
        .head
        .commit_id;
    authority
        .execute(
            "INSERT INTO migration_unique_probe(id,value) VALUES('remote','same')",
            &[],
        )
        .await
        .unwrap();
    source
        .execute(
            "INSERT INTO migration_unique_probe(id,value) VALUES('local','same')",
            &[],
        )
        .await
        .unwrap();
    let remote = authority.partial_replica_descriptor(None).await.unwrap();
    let target = remote.selected_branch.head.commit_id.clone();
    let local = source
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap()
        .selected_branch
        .head
        .commit_id;
    let request = crate::sync::PartialMergeRequest {
        attempt_id: uuid::Uuid::now_v7().to_string(),
        branch_id: remote.selected_branch.branch_id.clone(),
        base_commit_id: base,
        expected_authority_head_commit_id: target.clone(),
        captured_local_head_commit_id: local.clone(),
        checkpoint_commit_id: remote.selected_branch.checkpoint.commit_id.clone(),
        global_head_commit_id: remote.global_branch.head.commit_id.clone(),
        global_checkpoint_commit_id: remote.global_branch.checkpoint.commit_id.clone(),
    };
    let error = authority
        .merge_native_migration_for_account(&request, authority.active_account_id(), &branch.id)
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_UNIQUE);
    assert_eq!(
        authority
            .partial_replica_descriptor(None)
            .await
            .unwrap()
            .selected_branch
            .head
            .commit_id,
        target
    );
    assert_eq!(
        source
            .partial_replica_descriptor(Some(&branch.id))
            .await
            .unwrap()
            .selected_branch
            .head
            .commit_id,
        local
    );
}

#[tokio::test]
async fn migration_native_merge_preserves_custom_indexes_and_exact_outcome() {
    let authority = open_lix().await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    let schema = serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"migration_unique_probe","columns":[{"name":"id","type":"text","nullable":false},{"name":"value","type":"text","nullable":false}],"primary_key":["id"],"unique":[["value"]]});
    authority
        .execute(
            "INSERT INTO lix_registered_schema(value) VALUES(CAST($1 AS JSONB))",
            &[Value::Text(schema.to_string())],
        )
        .await
        .unwrap();
    let branch = authority
        .create_branch(CreateBranchOptions {
            id: None,
            name: "migration-source".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let source = authority
        .open_another_session()
        .with_branch(branch.id.clone())
        .await
        .unwrap();
    let base = source
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap()
        .selected_branch
        .head
        .commit_id;
    authority
        .execute(
            "INSERT INTO migration_unique_probe(id,value) VALUES('remote','same')",
            &[],
        )
        .await
        .unwrap();
    source
        .execute(
            "INSERT INTO migration_unique_probe(id,value) VALUES('local','different')",
            &[],
        )
        .await
        .unwrap();
    let remote = authority.partial_replica_descriptor(None).await.unwrap();
    let target = remote.selected_branch.head.commit_id.clone();
    let local = source
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap()
        .selected_branch
        .head
        .commit_id;
    let request = crate::sync::PartialMergeRequest {
        attempt_id: uuid::Uuid::now_v7().to_string(),
        branch_id: remote.selected_branch.branch_id.clone(),
        base_commit_id: base,
        expected_authority_head_commit_id: target.clone(),
        captured_local_head_commit_id: local.clone(),
        checkpoint_commit_id: remote.selected_branch.checkpoint.commit_id.clone(),
        global_head_commit_id: remote.global_branch.head.commit_id.clone(),
        global_checkpoint_commit_id: remote.global_branch.checkpoint.commit_id.clone(),
    };
    let receipt = authority
        .merge_native_migration_for_account(&request, authority.active_account_id(), &branch.id)
        .await
        .unwrap();
    let rows = authority
        .execute(
            "SELECT id FROM migration_unique_probe WHERE value='different'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.rows().len(), 1);
    assert_eq!(rows.rows()[0].get::<String>("id").unwrap(), "local");
    let remote_rows = authority
        .execute(
            "SELECT id FROM migration_unique_probe WHERE value='same'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(remote_rows.rows().len(), 1);
    assert_eq!(remote_rows.rows()[0].get::<String>("id").unwrap(), "remote");
    // Recovering the same exact request must return the first native M even
    // though the target head no longer equals captured R.
    let recovered = authority
        .merge_native_migration_for_account(&request, authority.active_account_id(), &branch.id)
        .await
        .unwrap();
    assert_eq!(recovered, receipt);
    assert_eq!(
        source
            .partial_replica_descriptor(Some(&branch.id))
            .await
            .unwrap()
            .selected_branch
            .head
            .commit_id,
        local
    );
}

#[tokio::test]
async fn migration_native_merge_preserves_ordinary_file_content() {
    let authority = open_lix().await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    authority
        .upsert_file_content("/local.bin", vec![1, 2])
        .await
        .unwrap();
    authority
        .upsert_file_content("/remote.bin", vec![3, 4])
        .await
        .unwrap();
    let branch = authority
        .create_branch(CreateBranchOptions {
            id: None,
            name: "migration-source".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let source = authority
        .open_another_session()
        .with_branch(branch.id.clone())
        .await
        .unwrap();
    let base = source
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap()
        .selected_branch
        .head
        .commit_id;
    authority
        .upsert_file_content("/remote.bin", vec![5, 6, 7])
        .await
        .unwrap();
    source
        .upsert_file_content("/local.bin", vec![8, 9, 10])
        .await
        .unwrap();
    let remote = authority.partial_replica_descriptor(None).await.unwrap();
    let target = remote.selected_branch.head.commit_id.clone();
    let local = source
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap()
        .selected_branch
        .head
        .commit_id;
    let request = crate::sync::PartialMergeRequest {
        attempt_id: uuid::Uuid::now_v7().to_string(),
        branch_id: remote.selected_branch.branch_id.clone(),
        base_commit_id: base,
        expected_authority_head_commit_id: target.clone(),
        captured_local_head_commit_id: local.clone(),
        checkpoint_commit_id: remote.selected_branch.checkpoint.commit_id.clone(),
        global_head_commit_id: remote.global_branch.head.commit_id.clone(),
        global_checkpoint_commit_id: remote.global_branch.checkpoint.commit_id.clone(),
    };
    let receipt = authority
        .merge_native_migration_for_account(&request, authority.active_account_id(), &branch.id)
        .await
        .unwrap();
    assert_eq!(
        authority
            .read_file_content("/local.bin", None)
            .await
            .unwrap()
            .unwrap()
            .content()
            .as_bytes()
            .as_ref(),
        &[8, 9, 10]
    );
    assert_eq!(
        authority
            .read_file_content("/remote.bin", None)
            .await
            .unwrap()
            .unwrap()
            .content()
            .as_bytes()
            .as_ref(),
        &[5, 6, 7]
    );
    // Recovering the same exact request must return the first native M even
    // though the target head no longer equals captured R.
    let recovered = authority
        .merge_native_migration_for_account(&request, authority.active_account_id(), &branch.id)
        .await
        .unwrap();
    assert_eq!(recovered, receipt);
    assert_eq!(
        source
            .partial_replica_descriptor(Some(&branch.id))
            .await
            .unwrap()
            .selected_branch
            .head
            .commit_id,
        local
    );
}

#[tokio::test]
async fn migration_native_merge_validates_reverse_foreign_key_deletion() {
    let authority = open_lix().await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    let parent = serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"migration_fk_parent","columns":[{"name":"id","type":"text","nullable":false}],"primary_key":["id"]});
    let child = serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"migration_fk_child","columns":[{"name":"id","type":"text","nullable":false},{"name":"parent_id","type":"text","nullable":false}],"primary_key":["id"],"foreign_keys":[{"columns":["parent_id"],"references":{"schema_key":"migration_fk_parent","columns":["id"]}}]});
    for schema in [parent, child] {
        authority
            .execute(
                "INSERT INTO lix_registered_schema(value) VALUES(CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
    }
    authority
        .execute("INSERT INTO migration_fk_parent(id) VALUES('parent')", &[])
        .await
        .unwrap();
    let branch = authority
        .create_branch(CreateBranchOptions {
            id: None,
            name: "migration-source".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let source = authority
        .open_another_session()
        .with_branch(branch.id.clone())
        .await
        .unwrap();
    let base = source
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap()
        .selected_branch
        .head
        .commit_id;
    authority
        .execute(
            "INSERT INTO migration_fk_child(id,parent_id) VALUES('child','parent')",
            &[],
        )
        .await
        .unwrap();
    source
        .execute("DELETE FROM migration_fk_parent WHERE id='parent'", &[])
        .await
        .unwrap();
    let remote = authority.partial_replica_descriptor(None).await.unwrap();
    let target = remote.selected_branch.head.commit_id.clone();
    let local = source
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap()
        .selected_branch
        .head
        .commit_id;
    let request = crate::sync::PartialMergeRequest {
        attempt_id: uuid::Uuid::now_v7().to_string(),
        branch_id: remote.selected_branch.branch_id.clone(),
        base_commit_id: base,
        expected_authority_head_commit_id: target.clone(),
        captured_local_head_commit_id: local.clone(),
        checkpoint_commit_id: remote.selected_branch.checkpoint.commit_id.clone(),
        global_head_commit_id: remote.global_branch.head.commit_id.clone(),
        global_checkpoint_commit_id: remote.global_branch.checkpoint.commit_id.clone(),
    };
    let error = authority
        .merge_native_migration_for_account(&request, authority.active_account_id(), &branch.id)
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
    assert_eq!(
        authority
            .partial_replica_descriptor(None)
            .await
            .unwrap()
            .selected_branch
            .head
            .commit_id,
        target
    );
    assert_eq!(
        source
            .partial_replica_descriptor(Some(&branch.id))
            .await
            .unwrap()
            .selected_branch
            .head
            .commit_id,
        local
    );
}

#[tokio::test]
async fn migration_native_merge_validates_file_changes_without_remote_edits() {
    let authority = open_lix().await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    authority
        .upsert_file_content("/local.bin", vec![1, 2])
        .await
        .unwrap();
    authority
        .upsert_file_content("/remote.bin", vec![3, 4])
        .await
        .unwrap();
    let branch = authority
        .create_branch(CreateBranchOptions {
            id: None,
            name: "migration-source".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let source = authority
        .open_another_session()
        .with_branch(branch.id.clone())
        .await
        .unwrap();
    let base = source
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap()
        .selected_branch
        .head
        .commit_id;
    source
        .upsert_file_content("/local.bin", vec![8, 9, 10])
        .await
        .unwrap();
    let remote = authority.partial_replica_descriptor(None).await.unwrap();
    let target = remote.selected_branch.head.commit_id.clone();
    let local = source
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap()
        .selected_branch
        .head
        .commit_id;
    let request = crate::sync::PartialMergeRequest {
        attempt_id: uuid::Uuid::now_v7().to_string(),
        branch_id: remote.selected_branch.branch_id.clone(),
        base_commit_id: base,
        expected_authority_head_commit_id: target.clone(),
        captured_local_head_commit_id: local.clone(),
        checkpoint_commit_id: remote.selected_branch.checkpoint.commit_id.clone(),
        global_head_commit_id: remote.global_branch.head.commit_id.clone(),
        global_checkpoint_commit_id: remote.global_branch.checkpoint.commit_id.clone(),
    };
    assert_eq!(
        request.base_commit_id,
        request.expected_authority_head_commit_id
    );
    let receipt = authority
        .merge_native_migration_for_account(&request, authority.active_account_id(), &branch.id)
        .await
        .unwrap();
    assert_eq!(
        authority
            .read_file_content("/local.bin", None)
            .await
            .unwrap()
            .unwrap()
            .content()
            .as_bytes()
            .as_ref(),
        &[8, 9, 10]
    );
    assert_eq!(
        authority
            .read_file_content("/remote.bin", None)
            .await
            .unwrap()
            .unwrap()
            .content()
            .as_bytes()
            .as_ref(),
        &[3, 4]
    );
    // Recovering the same exact request must return the first native M even
    // though the target head no longer equals captured R.
    let recovered = authority
        .merge_native_migration_for_account(&request, authority.active_account_id(), &branch.id)
        .await
        .unwrap();
    assert_eq!(recovered, receipt);
    assert_eq!(
        source
            .partial_replica_descriptor(Some(&branch.id))
            .await
            .unwrap()
            .selected_branch
            .head
            .commit_id,
        local
    );
}

#[tokio::test]
async fn migration_cleanup_fences_changed_surviving_head_and_retries_exactly() {
    let authority = open_lix().await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    let schema = serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"migration_unique_probe","columns":[{"name":"id","type":"text","nullable":false},{"name":"value","type":"text","nullable":false}],"primary_key":["id"],"unique":[["value"]]});
    authority
        .execute(
            "INSERT INTO lix_registered_schema(value) VALUES(CAST($1 AS JSONB))",
            &[Value::Text(schema.to_string())],
        )
        .await
        .unwrap();
    let branch = authority
        .create_branch(CreateBranchOptions {
            id: None,
            name: "migration-source".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let source = authority
        .open_another_session()
        .with_branch(branch.id.clone())
        .await
        .unwrap();
    let base = source
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap()
        .selected_branch
        .head
        .commit_id;
    authority
        .execute(
            "INSERT INTO migration_unique_probe(id,value) VALUES('remote','same')",
            &[],
        )
        .await
        .unwrap();
    source
        .execute(
            "INSERT INTO migration_unique_probe(id,value) VALUES('local','different')",
            &[],
        )
        .await
        .unwrap();
    let remote = authority.partial_replica_descriptor(None).await.unwrap();
    let target = remote.selected_branch.head.commit_id.clone();
    let local = source
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap()
        .selected_branch
        .head
        .commit_id;
    let request = crate::sync::PartialMergeRequest {
        attempt_id: uuid::Uuid::now_v7().to_string(),
        branch_id: remote.selected_branch.branch_id.clone(),
        base_commit_id: base,
        expected_authority_head_commit_id: target.clone(),
        captured_local_head_commit_id: local.clone(),
        checkpoint_commit_id: remote.selected_branch.checkpoint.commit_id.clone(),
        global_head_commit_id: remote.global_branch.head.commit_id.clone(),
        global_checkpoint_commit_id: remote.global_branch.checkpoint.commit_id.clone(),
    };
    let receipt = authority
        .merge_native_migration_for_account(&request, authority.active_account_id(), &branch.id)
        .await
        .unwrap();
    let rows = authority
        .execute(
            "SELECT id FROM migration_unique_probe WHERE value='different'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.rows().len(), 1);
    assert_eq!(rows.rows()[0].get::<String>("id").unwrap(), "local");
    let remote_rows = authority
        .execute(
            "SELECT id FROM migration_unique_probe WHERE value='same'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(remote_rows.rows().len(), 1);
    assert_eq!(remote_rows.rows()[0].get::<String>("id").unwrap(), "remote");
    // Recovering the same exact request must return the first native M even
    // though the target head no longer equals captured R.
    let recovered = authority
        .merge_native_migration_for_account(&request, authority.active_account_id(), &branch.id)
        .await
        .unwrap();
    assert_eq!(recovered, receipt);
    assert_eq!(
        source
            .partial_replica_descriptor(Some(&branch.id))
            .await
            .unwrap()
            .selected_branch
            .head
            .commit_id,
        local
    );

    // Native push installs the same checkpoint coordinate used by a real
    // migration pin; create_branch's default checkpoint is its fork head.
    let source_before = source
        .partial_replica_descriptor(Some(&branch.id))
        .await
        .unwrap();
    authority
        .push_sync_repository_for_account(
            &crate::sync::SyncPushRequest {
                commits: vec![],
                inline_blobs: vec![],
                ref_updates: vec![crate::sync::SyncRefUpdate {
                    branch_id: branch.id.clone(),
                    expected_head_commit_id: Some(local.clone()),
                    expected_checkpoint_commit_id: Some(
                        source_before.selected_branch.checkpoint.commit_id,
                    ),
                    head_commit_id: Some(local.clone()),
                    checkpoint_commit_id: Some(request.checkpoint_commit_id.clone()),
                }],
            },
            authority.active_account_id(),
        )
        .await
        .unwrap();
    let cleanup = crate::sync::NativeMigrationCleanupRequest {
        migration: crate::sync::NativeMigrationMergeRequest {
            request: request.clone(),
            source_branch_id: branch.id.clone(),
        },
    };
    let adapter = authority.storage_adapter();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let guards = crate::sync::native_migration_cleanup_guards(
        &read,
        authority.lix_id(),
        authority.active_account_id(),
        &cleanup,
        &cleanup.deletion().unwrap(),
    )
    .await
    .unwrap();
    drop(read);
    authority
        .execute(
            "UPDATE migration_unique_probe SET value='new-remote' WHERE id='remote'",
            &[],
        )
        .await
        .unwrap();
    let mut writes = adapter.new_write_set();
    crate::branch::stage_delete_branch_head_control(&mut writes, &branch.id).unwrap();
    assert!(
        adapter
            .commit_write_set(
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    preconditions: guards,
                    await_durable: true,
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    let read = adapter.begin_read(Default::default()).await.unwrap();
    assert!(
        crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(&branch.id)
            .await
            .unwrap()
            .is_some()
    );
    drop(read);
    authority
        .cleanup_native_migration_for_account(&cleanup, authority.active_account_id())
        .await
        .unwrap();
    authority
        .cleanup_native_migration_for_account(&cleanup, authority.active_account_id())
        .await
        .unwrap();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    assert!(
        crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(&branch.id)
            .await
            .unwrap()
            .is_none()
    );
}
