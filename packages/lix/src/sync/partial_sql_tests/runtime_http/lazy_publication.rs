//! Live observation hydrates its own new basis after coordinate publication.
use super::*;

#[tokio::test]
async fn observer_hydrates_after_lazy_baseline_adoption() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let backing = Memory::new();
        let authority = open_lix().with_storage(backing.clone()).await.unwrap();
        authority.set_sync_role(crate::sync::SyncRole::Authority).unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value(key,value) VALUES('resident','before')",
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
        let client = Client {
            server,
            lose_body: Arc::new(AtomicBool::new(false)),
        };
        let transport = HttpSyncTransport::connect_with(
            client.clone(),
            &format!("https://example.test/lix/{}", authority.lix_id()),
        )
        .await
        .unwrap();
        let wrapper = transport.partial_replica_descriptor(None).await.unwrap();
        let old = Arc::new(
            PartialReplicaState::from_leased(
                transport.protocol_url().into(),
                authority.active_account_id().into(),
                uuid::Uuid::now_v7().to_string(),
                wrapper.wire,
            )
            .unwrap(),
        );
        transport
            .bind_native_baseline_lease(old.baseline_lease())
            .unwrap();
        let storage = StorageAdapter::new(Memory::new());
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let preconditions = stage_partial_bootstrap(&read, &mut writes, &old).unwrap();
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
            Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &old)
                .await
                .unwrap();
        let engine = Arc::new(engine);
        engine
            .sync_mode()
            .admit_partial_replica(old.clone(), crate::sync::partial_replica_write_capability());
        storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());

        let (shutdown, shutdown_rx) =
            tokio::sync::watch::channel(crate::sync::runtime::SyncShutdown::Running);
        let (sender, demand_rx) = tokio::sync::mpsc::channel(4);
        // Prime only the original basis before the server worker starts. The
        // authority and HTTP server use distinct engines over the same storage;
        // updating before long-poll avoids relying on cross-engine notification.
        execute_hydrating(
            &session,
            &storage,
            &old,
            &authority,
            "SELECT value FROM lix_key_value WHERE key='resident'",
            &[],
            &mut Fetches::default(),
        )
        .await
        .unwrap();
        let sql = "SELECT value FROM lix_key_value WHERE key='resident'";
        let mut events = session
            .observe(sql, &[])
            .unwrap()
            .with_sync_demand_sender(Some(sender));
        let initial = events.next().await.unwrap().unwrap();
        assert_eq!(
            initial.rows.rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            "before"
        );
        crate::sync::admit_sync_authority_storage(&authority.storage_adapter(), None)
            .await
            .unwrap();
        authority
            .execute(
                "UPDATE lix_key_value SET value='after' WHERE key='resident'",
                &[],
            )
            .await
            .unwrap();
        let wrapper = transport.partial_replica_descriptor(None).await.unwrap();
        assert!(wrapper.wire.descriptor.cursor > old.descriptor().cursor,
            "authority fixture must publish its mutation to the sync cursor");
        let prepared = prepare_descriptor_with_merge(
            engine.clone(), old.clone(), &transport, wrapper,
            crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
        ).await.unwrap();
        let crate::sync::partial_reconcile::PreparedDescriptor::Ready(prepared) = prepared else {
            panic!("updated authority must prepare a new basis");
        };
        crate::sync::partial_publication::publish_prepared_partial(engine.clone(), prepared).await.unwrap();
        let current = engine.sync_mode().partial_admission().unwrap();
        let transport = transport.fork_native_baseline_lease(current.baseline_lease()).unwrap();
        let worker = crate::sync::partial_runtime::run_partial_worker_with_engine(
            storage.clone(),
            current,
            Some(transport),
            || Box::pin(async { Err(LixError::unknown("unexpected reconnect")) }),
            shutdown_rx,
            demand_rx,
            None,
            Some(engine.clone()),
        );
        let observe = async {
            // Publication must not have warmed the previously observed query.
            let missing = session.execute(sql, &[]).await.unwrap_err();
            assert!(
                NativeObjectRef::from_missing_error(&missing)
                    .unwrap()
                    .is_some()
                    || NativeMetadataRef::from_missing_error(&missing)
                        .unwrap()
                        .is_some(),
                "{missing:?}"
            );
            // The real observer demand sender and runtime HTTP dispatcher now
            // hydrate and retry; no manual execute_hydrating helper is involved.
            let changed = events.next().await.unwrap().unwrap();
            assert!(changed.sequence > initial.sequence);
            assert_eq!(
                changed.rows.rows()[0]
                    .get::<serde_json::Value>("value")
                    .unwrap(),
                "after"
            );
            assert_eq!(
                session.execute(sql, &[]).await.unwrap().rows(),
                changed.rows.rows()
            );
            drop(events);
            shutdown
                .send(crate::sync::runtime::SyncShutdown::Stop)
                .unwrap();
        };
        tokio::pin!(worker);
        tokio::select! {
            result = &mut worker => panic!("worker stopped before observation completed: {result:?}"),
            () = observe => {}
        }
        worker.await.unwrap();
    })
    .await
    .expect("observer must hydrate and deliver the newly admitted basis");
}
