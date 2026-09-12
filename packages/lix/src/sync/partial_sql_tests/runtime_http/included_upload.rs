//! Lost ordinary ACK is settled as inclusion, never replayed as a later write.
use super::*;

#[tokio::test]
async fn included_frozen_upload_preserves_later_authority_winner_and_new_local_suffix() {
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_key_value(key,value) VALUES('same','B'),('newer','B')",
            &[],
        )
        .await
        .unwrap();
    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let transport = HttpSyncTransport::connect_with(
        Client {
            server,
            lose_body: Arc::new(AtomicBool::new(false)),
        },
        &format!("https://example.test/lix/{}", authority.lix_id()),
    )
    .await
    .unwrap();
    let wrapper = transport.partial_replica_descriptor(None).await.unwrap();
    let state = PartialReplicaState::from_leased(
        transport.protocol_url().into(),
        authority.active_account_id().into(),
        uuid::Uuid::now_v7().to_string(),
        wrapper.wire,
    )
    .unwrap();
    let storage = StorageAdapter::new(Memory::new());
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let preconditions = stage_partial_bootstrap(&read, &mut writes, &state).unwrap();
    crate::init::stage_partial_repository_protocol(&mut writes);
    drop(read);
    storage
        .commit_write_set(
            writes,
            StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (engine, session) =
        Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &state)
            .await
            .unwrap();
    engine.sync_mode().admit_partial_replica(
        Arc::new(state.clone()),
        crate::sync::partial_replica_write_capability(),
    );
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    execute_hydrating(
        &session,
        &storage,
        &state,
        &authority,
        "UPDATE lix_key_value SET value='L1' WHERE key='same'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    let branch = &state.descriptor().selected_branch.branch_id;
    let lost = crate::sync::partial_upload_cycle::upload_partial_once(
        &storage,
        &state,
        branch,
        uuid::Uuid::now_v7().to_string(),
        32,
        64 * 1024 * 1024,
        |request| {
            let transport = &transport;
            async move {
                crate::sync::SyncTransport::push(transport, &request).await?;
                Err(LixError::new(
                    "TEST_LOST_ORDINARY_ACK",
                    "authority accepted before response was lost",
                ))
            }
        },
    )
    .await
    .unwrap_err();
    assert_eq!(lost.code, "TEST_LOST_ORDINARY_ACK");
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (pending, _, _) =
        crate::sync::partial_push_state::load_partial_push_state(&read, &state, branch)
            .await
            .unwrap();
    let frozen = pending.prepared.clone().unwrap();
    drop(read);
    crate::sync::admit_sync_authority_storage(&authority.storage_adapter(), None)
        .await
        .unwrap();
    authority
        .execute(
            "UPDATE lix_key_value SET value='R-later' WHERE key='same'",
            &[],
        )
        .await
        .unwrap();
    execute_hydrating(
        &session,
        &storage,
        &state,
        &authority,
        "UPDATE lix_key_value SET value='L2' WHERE key='newer'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    let controls_before = admitted_controls(&storage, &state).await.unwrap();
    let remote = transport.partial_replica_descriptor(None).await.unwrap();
    assert_ne!(
        remote.wire.descriptor.selected_branch.head.commit_id,
        frozen.target.head
    );
    let leased = transport
        .fork_native_baseline_lease(&remote.wire.lease)
        .unwrap();
    for attempt in 0..64 {
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let result = async {
            // Head inclusion is insufficient: this distinct native checkpoint
            // does not contain the frozen selected checkpoint. The owner must
            // leave the exact tuple pending and stage no bookkeeping writes.
            let mut unrelated = remote.wire.descriptor.clone();
            unrelated.selected_branch.checkpoint = unrelated.global_branch.checkpoint.clone();
            assert_ne!(
                unrelated.selected_branch.checkpoint.commit_id,
                frozen.target.checkpoint
            );
            let mut rejected = storage.new_write_set();
            let rejected_ack =
                crate::sync::partial_push_state::stage_acknowledge_included_partial_upload(
                    &read,
                    &mut rejected,
                    &state,
                    &unrelated,
                )
                .await?;
            assert!(rejected_ack.is_none());
            assert!(rejected.is_empty());
            let (still_pending, _, _) =
                crate::sync::partial_push_state::load_partial_push_state(&read, &state, branch)
                    .await?;
            assert_eq!(still_pending.prepared.as_ref(), Some(&frozen));
            assert_eq!(still_pending.confirmed, pending.confirmed);
            crate::sync::partial_push_state::stage_acknowledge_included_partial_upload(
                &read,
                &mut writes,
                &state,
                &remote.wire.descriptor,
            )
            .await
        }
        .await;
        drop(read);
        match result {
            Ok(Some(preconditions)) => {
                storage
                    .commit_partial_replica_write_set(
                        crate::sync::partial_replica_write_capability(),
                        writes,
                        StorageWriteOptions {
                            preconditions,
                            await_durable: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                break;
            }
            Ok(None) => panic!("exact frozen upload was not recognized as included"),
            Err(error) => {
                assert!(attempt < 63, "{error}");
                let demand = crate::sync::runtime::native_sync_demand_request_for_error(&error)
                    .unwrap()
                    .unwrap_or_else(|| panic!("{error}"));
                crate::sync::partial_runtime::hydrate_demand(&storage, &state, &leased, demand)
                    .await
                    .unwrap();
            }
        }
    }
    assert_eq!(
        admitted_controls(&storage, &state).await.unwrap(),
        controls_before
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (settled, _, _) =
        crate::sync::partial_push_state::load_partial_push_state(&read, &state, branch)
            .await
            .unwrap();
    assert_eq!(settled.confirmed, frozen.target);
    assert!(settled.prepared.is_none());
    assert_eq!(settled.confirmed.checkpoint, frozen.target.checkpoint);
    assert_ne!(
        controls_before[0].head_commit_id.to_string(),
        settled.confirmed.head
    );
    drop(read);
    assert_eq!(
        value(
            authority
                .execute("SELECT value FROM lix_key_value WHERE key='same'", &[])
                .await
                .unwrap()
        ),
        "R-later"
    );
    assert_eq!(
        value(
            session
                .execute("SELECT value FROM lix_key_value WHERE key='newer'", &[])
                .await
                .unwrap()
        ),
        "L2"
    );
}
