// Included in repository::tests to exercise actual certified snapshot admission.

async fn omission_space_rows(
    read: &impl StorageAdapterRead,
    space: StorageSpace,
) -> Vec<crate::storage_adapter::StorageReadEntry> {
    let mut cursor = read
        .begin_scan(
            space,
            StoragePrefix {
                bytes: Bytes::new(),
            }
            .to_range()
            .unwrap(),
            Default::default(),
        )
        .await
        .unwrap();
    cursor.collect_all().await.unwrap()
}

#[tokio::test]
async fn v79_snapshot_omission_migration_restores_only_certified_owners_atomically() {
    let authority = open_lix().await.unwrap();
    write_key_value(&authority, "omitted-owner", "retained").await;
    for index in 0..8 {
        write_key_value(&authority, "later-owner", &index.to_string()).await;
    }
    let snapshot = authority.pull_sync_repository(None, 1).await.unwrap();
    let replica = replica_from_snapshot(&authority, &snapshot).await;
    let adapter = replica.storage_adapter();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let space = crate::tracked_state::TRACKED_STATE_COMMIT_HISTORY_DEFERRED_SPACE;
    let mut omitted = Vec::new();
    for entry in omission_space_rows(&read, space).await {
        let id = CommitId::new(uuid::Uuid::from_slice(&entry.key.0).unwrap());
        if crate::tracked_state::commit_history_is_omitted(&read, id)
            .await
            .unwrap()
        {
            omitted.push(id);
        }
    }
    assert!(
        !omitted.is_empty(),
        "snapshot must omit at least one authenticated semantic owner"
    );
    let mut writes = adapter.new_write_set();
    for id in &omitted {
        stage_commit_history_available(&mut writes, *id);
    }
    drop(read);
    adapter
        .commit_certified_replica_write_set(
            crate::sync::certified_replica_write_capability(),
            writes,
            Default::default(),
        )
        .await
        .unwrap();
    crate::migration::downgrade_headers_for_test(&adapter, false).await;
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let header_count = omission_space_rows(
        &read,
        crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
    )
    .await
    .len();
    let locator_count = omission_space_rows(
        &read,
        crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
    )
    .await
    .len();
    drop(read);
    let error = crate::migration::migrate_lix_with_adapter(
        adapter.storage().clone(),
        adapter.clone(),
        crate::migration::MigrationOptions {
            max_changes: header_count + locator_count + 1,
            max_preflight_bytes: usize::MAX,
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED");
    let read = adapter.begin_read(Default::default()).await.unwrap();
    for id in &omitted {
        assert!(
            !crate::tracked_state::commit_history_is_omitted(&read, *id)
                .await
                .unwrap()
        );
    }
    assert!(matches!(
        crate::init::repository_protocol_status(&read)
            .await
            .unwrap(),
        crate::init::RepositoryProtocolStatus::MigrationRequired { found_version: 79 }
    ));
    drop(read);
    crate::migration::migrate_headers_for_test(&adapter, false).await;
    let read = adapter.begin_read(Default::default()).await.unwrap();
    for id in omitted {
        assert!(
            crate::tracked_state::commit_history_is_omitted(&read, id)
                .await
                .unwrap()
        );
        assert_eq!(
            deferred_commit_global_scope(&read, id).await.unwrap(),
            None,
            "containing snapshot lane cannot determine omitted author scope"
        );
    }
    drop(read);
    assert_eq!(read_key_value(&replica, "omitted-owner").await, "retained");
}

#[tokio::test]
async fn v79_standalone_missing_owner_is_not_granted_snapshot_omission() {
    let authority = open_lix().await.unwrap();
    write_key_value(&authority, "standalone-owner", "retained").await;
    let source =
        CommitId::parse_lix(&current_branch_head(&authority).await, "test source").unwrap();
    write_key_value(&authority, "later-owner", "later").await;
    let adapter = authority.storage_adapter();
    let mut writes = adapter.new_write_set();
    let key = StorageKey(Bytes::copy_from_slice(source.as_uuid().as_bytes()));
    writes.delete(COMMIT_SPACE, key.clone());
    writes.delete(
        crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        key,
    );
    adapter
        .commit_write_set(writes, Default::default())
        .await
        .unwrap();
    crate::migration::downgrade_headers_for_test(&adapter, false).await;
    crate::migration::migrate_headers_for_test(&adapter, false).await;
    let read = adapter.begin_read(Default::default()).await.unwrap();
    assert!(!commit_history_is_deferred(&read, source).await.unwrap());
}
