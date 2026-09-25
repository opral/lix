use super::*;
use crate::storage_adapter::{Memory, StorageSpace};

async fn snapshot(read: &impl StorageAdapterRead, space: StorageSpace) -> Vec<(Vec<u8>, Vec<u8>)> {
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
                panic!("full value")
            };
            (entry.key.0.to_vec(), value.to_vec())
        })
        .collect()
}

async fn fixture() -> StorageAdapter<Memory> {
    let adapter = StorageAdapter::new(Memory::new());
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
            "INSERT INTO lix_key_value (key,value) VALUES ('migration-pending','preserved')",
            &[],
        )
        .await
        .unwrap();
    drop(session);
    drop(engine);
    downgrade(&adapter).await;
    adapter
}

async fn downgrade(adapter: &StorageAdapter<Memory>) {
    downgrade_headers_for_test(adapter, false).await;
}

#[tokio::test]
async fn full_and_sparse_migrations_produce_identical_headers_and_accept_native_hydration() {
    use crate::sync::native_metadata::{
        NativeMetadata, NativeMetadataRequest, NativeMetadataResponse, stage_native_metadata,
    };
    let authority = crate::open_lix().await.unwrap();
    authority
        .execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('selected','one'),('other','two')",
            &[],
        )
        .await
        .unwrap();
    let source = authority
        .execute("SELECT lix_active_branch_commit_id() AS id", &[])
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();
    let checkpoint = authority.execute("SELECT commit_id FROM lix_create_checkpoint('Checkpoint', '{\"_type\":\"zettel_doc\",\"blocks\":[]}'::JSONB, ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))", &[]).await.unwrap().rows()[0].get::<String>("commit_id").unwrap();
    authority
        .execute(
            "UPDATE lix_key_value SET value='ordinary' WHERE key='other'",
            &[],
        )
        .await
        .unwrap();
    let state = crate::sync::PartialReplicaState::new(
        format!("https://example.test/lix/{}", authority.lix_id()),
        authority.active_account_id().into(),
        uuid::Uuid::now_v7().to_string(),
        authority.partial_replica_descriptor(None).await.unwrap(),
    )
    .unwrap();
    let full = authority.storage_adapter();
    let mut writes = full.new_write_set();
    crate::sync::stage_sync_checkpoint_source(
        &mut writes,
        &state.descriptor().selected_branch.branch_id,
        CommitId::new(uuid::Uuid::parse_str(&checkpoint).unwrap()),
        CommitId::new(uuid::Uuid::parse_str(&source).unwrap()),
    )
    .unwrap();
    full.commit_write_set(writes, Default::default())
        .await
        .unwrap();
    downgrade_headers_for_test(&full, false).await;
    let read = full.begin_read(Default::default()).await.unwrap();
    let legacy = snapshot(
        &read,
        crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
    )
    .await;
    assert!(
        !snapshot(&read, crate::sync::SYNC_CHECKPOINT_SOURCE_SPACE)
            .await
            .is_empty()
    );
    drop(read);

    let sparse = StorageAdapter::new(Memory::new());
    let read = sparse.begin_read(Default::default()).await.unwrap();
    let mut writes = sparse.new_write_set();
    let guards = crate::sync::stage_partial_bootstrap(&read, &mut writes, &state).unwrap();
    writes.put(
        crate::init::REPOSITORY_PROTOCOL_SPACE,
        crate::init::REPOSITORY_PROTOCOL_KEY,
        crate::init::PARTIAL_REPOSITORY_PROTOCOL_V79,
    );
    for (key, bytes) in legacy {
        writes.put(
            crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            key,
            bytes,
        );
    }
    drop(read);
    sparse
        .commit_write_set(
            writes,
            crate::storage_adapter::StorageWriteOptions {
                preconditions: guards,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    migrate(&full, MigrationOptions::default(), false)
        .await
        .unwrap();
    migrate(&sparse, MigrationOptions::default(), true)
        .await
        .unwrap();
    let full_read = full.begin_read(Default::default()).await.unwrap();
    let read = sparse.begin_read(Default::default()).await.unwrap();
    let canonical = snapshot(
        &full_read,
        crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
    )
    .await;
    assert_eq!(
        snapshot(
            &read,
            crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE
        )
        .await,
        canonical
    );
    for space in [
        crate::changelog::COMMIT_SPACE,
        crate::sync::SYNC_CHECKPOINT_SOURCE_SPACE,
        crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
    ] {
        assert!(
            snapshot(&read, space).await.is_empty(),
            "sparse migration must not require missing graph/source/state inputs"
        );
    }
    drop(read);
    drop(full_read);
    for batch in canonical.chunks(32) {
        let objects = batch
            .iter()
            .map(|(key, bytes)| NativeMetadata {
                address: crate::tracked_state::NativeMetadataRef::CommitStateHeader(
                    uuid::Uuid::from_slice(key).unwrap().to_string(),
                ),
                bytes: bytes.clone(),
            })
            .collect::<Vec<_>>();
        let request = NativeMetadataRequest {
            epoch_id: state.epoch_id().into(),
            objects: objects
                .iter()
                .map(|object| object.address.clone())
                .collect(),
        };
        let response = NativeMetadataResponse {
            dependencies: Default::default(),
            lix_id: state.repository_id().into(),
            epoch_id: state.epoch_id().into(),
            objects,
        };
        let read = sparse.begin_read(Default::default()).await.unwrap();
        let mut writes = sparse.new_write_set();
        let guards = stage_native_metadata(&read, &mut writes, &state, &request, &response)
            .await
            .unwrap();
        assert!(
            guards.iter().all(|guard| matches!(
                guard,
                crate::storage_adapter::StoragePrecondition::KeyValueEquals { .. }
            )),
            "already resident canonical metadata must take the exact equality path"
        );
        for object in response.objects {
            let crate::tracked_state::NativeMetadataRef::CommitStateHeader(id) = object.address
            else {
                unreachable!()
            };
            assert_eq!(
                writes
                    .staged_value(
                        crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
                        uuid::Uuid::parse_str(&id).unwrap().as_bytes()
                    )
                    .unwrap()
                    .as_ref(),
                object.bytes
            );
        }
    }
}

#[tokio::test]
async fn legacy_sql_checkpoints_preserve_members_without_inferred_provenance() {
    for full in [false, true] {
        let adapter = fixture().await;
        migrate(&adapter, MigrationOptions::default(), false)
            .await
            .unwrap();
        super::super::runtime_epoch::migrate(&adapter, false)
            .await
            .unwrap();
    super::super::hot_indexes::migrate(&adapter, MigrationOptions::default(), false).await.unwrap();
        let engine = crate::engine::Engine::new_with_adapter(
            adapter.clone(),
            crate::engine::EngineOptions::new(),
        )
        .await
        .unwrap();
        let session = engine.open_session().await.unwrap();
        session
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('unselected','retained')",
                &[],
            )
            .await
            .unwrap();
        let _source = session
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        session.execute("INSERT INTO lix_key_value (key,value,lixcol_global) VALUES ('migration-global','advanced',true)", &[]).await.unwrap();
        let sql = if full {
            "SELECT commit_id FROM lix_create_checkpoint('Checkpoint', '{\"_type\":\"zettel_doc\",\"blocks\":[]}'::JSONB, ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))"
        } else {
            "SELECT commit_id FROM lix_create_checkpoint('Checkpoint', '{\"_type\":\"zettel_doc\",\"blocks\":[]}'::JSONB, ARRAY(SELECT row_ref FROM lix_diff('lix_key_value') WHERE key='migration-pending'))"
        };
        let checkpoint = session.execute(sql, &[]).await.unwrap().rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        let working = session
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        drop(session);
        drop(engine);
        downgrade(&adapter).await;
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let members = snapshot(
            &read,
            crate::tracked_state::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
        )
        .await;
        drop(read);
        migrate(&adapter, MigrationOptions::default(), false)
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert_eq!(
            snapshot(
                &read,
                crate::tracked_state::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE
            )
            .await,
            members
        );
        let parse = |id: &str| CommitId::new(uuid::Uuid::parse_str(id).unwrap());
        let target = if full { checkpoint } else { working };
        assert_eq!(
            load_published_commit_state_topology(&read, parse(&target))
                .await
                .unwrap()
                .unwrap()
                .incorporation(),
            CommitStateIncorporation::LegacyUnknown
        );
    }
}

#[tokio::test]
async fn legacy_headers_upgrade_without_rewriting_rows_history_or_membership() {
    let adapter = fixture().await;
    let spaces = [
        crate::hot_state::ROW_SPACE,
        crate::changelog::COMMIT_SPACE,
        crate::branch::BRANCH_HEAD_CONTROL_SPACE,
        crate::tracked_state::TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
    ];
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let mut before = Vec::new();
    for space in spaces {
        before.push(snapshot(&read, space).await);
    }
    drop(read);
    migrate(&adapter, MigrationOptions::default(), false)
        .await
        .unwrap();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        marker(&read).await.unwrap().as_ref(),
        crate::init::REPOSITORY_PROTOCOL_V80
    );
    for (space, expected) in spaces.into_iter().zip(before) {
        assert_eq!(snapshot(&read, space).await, expected);
    }
    drop(read);
    migrate(&adapter, MigrationOptions::default(), false)
        .await
        .unwrap();
    super::super::runtime_epoch::migrate(&adapter, false)
        .await
        .unwrap();
    super::super::hot_indexes::migrate(&adapter, MigrationOptions::default(), false).await.unwrap();
    let engine =
        crate::engine::Engine::new_with_adapter(adapter, crate::engine::EngineOptions::new())
            .await
            .unwrap();
    engine
        .open_session()
        .await
        .unwrap()
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'migration-pending'",
            &[],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn bounded_failure_preserves_legacy_headers_and_marker_for_retry() {
    let adapter = fixture().await;
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let before = snapshot(
        &read,
        crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
    )
    .await;
    drop(read);
    let result = migrate(
        &adapter,
        MigrationOptions {
            max_changes: 0,
            ..Default::default()
        },
        false,
    )
    .await;
    assert!(result.is_err());
    let read = adapter.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        marker(&read).await.unwrap().as_ref(),
        crate::init::REPOSITORY_PROTOCOL_V79
    );
    assert_eq!(
        snapshot(
            &read,
            crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE
        )
        .await,
        before
    );
    drop(read);
    migrate(&adapter, MigrationOptions::default(), false)
        .await
        .unwrap();
}

#[tokio::test]
async fn sparse_missing_graph_preserves_explicit_unknown_headers() {
    let adapter = fixture().await;
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let graphs = snapshot(&read, crate::changelog::COMMIT_SPACE).await;
    drop(read);
    let mut writes = adapter.new_write_set();
    for (key, _) in graphs {
        writes.delete(crate::changelog::COMMIT_SPACE, StorageKey(key.into()));
    }
    writes.put(
        crate::init::REPOSITORY_PROTOCOL_SPACE,
        crate::init::REPOSITORY_PROTOCOL_KEY,
        crate::init::PARTIAL_REPOSITORY_PROTOCOL_V79,
    );
    adapter
        .commit_write_set(writes, Default::default())
        .await
        .unwrap();
    migrate(&adapter, MigrationOptions::default(), true)
        .await
        .unwrap();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let headers = snapshot(
        &read,
        crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
    )
    .await;
    let mut unknown = 0;
    for (key, _) in headers {
        let id = CommitId::new(uuid::Uuid::from_slice(&key).unwrap());
        let topology = load_published_commit_state_topology(&read, id)
            .await
            .unwrap()
            .unwrap();
        if topology.incorporation() == CommitStateIncorporation::LegacyUnknown {
            unknown += 1;
        }
    }
    assert!(
        unknown > 0,
        "missing graph evidence cannot become a negative proof"
    );
    assert_eq!(
        marker(&read).await.unwrap().as_ref(),
        crate::init::PARTIAL_REPOSITORY_PROTOCOL_V80
    );
}
