use super::*;

#[tokio::test]
async fn background_reconciliation_refreshes_warm_file_listing_after_conflicting_edits() {
    tokio::time::timeout(std::time::Duration::from_secs(30), Box::pin(async {
        let backing = Memory::new();
        let authority = open_lix().with_storage(backing.clone()).await.unwrap();
        authority.set_sync_role(crate::sync::SyncRole::Authority).unwrap();
        for path in ["/gtm/edit.txt", "/gtm/delete.txt", "/elsewhere/keep.txt"] {
            authority.upsert_file_content(path, b"before".to_vec()).await.unwrap();
        }
        let server = open_lix().with_storage(backing).serve().with_embedded_lix_id().await.unwrap();
        let enabled = Arc::new(AtomicBool::new(false));
        let client = ExpiringClient {
            server, lease: Arc::default(), expire: Arc::default(), lose_session: Arc::default(), fetches: Arc::default(),
            live_updates: Some(enabled.clone()),
        };
        let transport = HttpSyncTransport::connect_with(client.clone(),
            &format!("https://example.test/lix/{}", authority.lix_id())).await.unwrap();
        let wrapper = transport.partial_replica_descriptor(None).await.unwrap();
        let old = Arc::new(PartialReplicaState::from_leased(
            transport.protocol_url().into(), authority.active_account_id().into(),
            uuid::Uuid::now_v7().to_string(), wrapper.wire,
        ).unwrap());
        let storage = StorageAdapter::new(Memory::new()).with_session().await.unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let preconditions = crate::sync::partial_bootstrap::stage_partial_bootstrap(&read, &mut writes, &old).unwrap();
        crate::init::stage_partial_repository_protocol(&mut writes);
        drop(read);
        storage.commit_write_set(writes, StorageWriteOptions { preconditions, await_durable:true, ..Default::default() }).await.unwrap();
        let (engine, session) = Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &old).await.unwrap();
        let engine = Arc::new(engine);
        engine.sync_mode().admit_partial_replica(old.clone(), crate::sync::partial_replica_write_capability());
        storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
        transport.bind_native_baseline_lease(old.baseline_lease()).unwrap();
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(crate::sync::runtime::SyncShutdown::Running);
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let lix = Lix::from_partial_engine_for_test(engine.clone(), session, sender);
        let worker = crate::sync::partial_runtime::run_partial_worker_with_engine(
            storage, old.clone(), Some(transport),
            || Box::pin(async {Err(LixError::unknown("unexpected reconnect"))}),
            shutdown_rx, receiver, None, Some(engine.clone()),
        );
        let caller = async {
            let sql = "SELECT path,name FROM lix_file WHERE path LIKE '/gtm/%' ORDER BY path LIMIT 100";
            let initial = lix.execute(sql, &[]).await.unwrap();
            assert_eq!(initial.rows().len(), 2);
            let initial_fetches = client.fetches.load(Ordering::SeqCst);
            assert_eq!(format!("{:?}", lix.execute(sql, &[]).await.unwrap().rows()), format!("{:?}", initial.rows()));
            assert_eq!(client.fetches.load(Ordering::SeqCst), initial_fetches);
            lix.execute("UPDATE lix_file SET path='/gtm/local.txt' WHERE path='/gtm/edit.txt'", &[]).await.unwrap();
            authority_sql(&client.server, authority.lix_id(), None,
                "UPDATE lix_file SET path='/gtm/remote.txt' WHERE path='/gtm/edit.txt'").await;
            authority_sql(&client.server, authority.lix_id(), None,
                "DELETE FROM lix_file WHERE path='/gtm/delete.txt'").await;
            authority_sql(&client.server, authority.lix_id(), None,
                "INSERT INTO lix_file(path,content) VALUES('/gtm/added.txt',CAST('new' AS BYTEA))").await;
            enabled.store(true, Ordering::SeqCst);
            // Let the real watch/update lane observe and settle the remote
            // cursor. No manual descriptor publication or foreground recovery.
            loop {
                let admitted = engine.sync_mode().partial_admission().unwrap();
                if admitted.descriptor().cursor > old.descriptor().cursor
                    && crate::sync::partial_publication::require_clean_switch_source(&engine, &admitted).await.is_ok()
                {
                    break;
                }
                crate::sync::platform::sleep(std::time::Duration::from_millis(2)).await;
            }
            let expected = authority_sql(&client.server, authority.lix_id(), None, sql).await;
            let actual = lix.execute(sql, &[]).await.unwrap();
            assert_eq!(actual.rows().len(), 2, "remote add/delete changes must invalidate the warm listing");
            assert_eq!(format!("{:?}", actual.rows()), format!("{:?}", expected.rows()),
                "settled local and authoritative ordered file cells must agree");
            let fetched = client.fetches.load(Ordering::SeqCst);
            for _ in 0..3 {
                assert_eq!(format!("{:?}", lix.execute(sql, &[]).await.unwrap().rows()), format!("{:?}", expected.rows()));
            }
            assert_eq!(client.fetches.load(Ordering::SeqCst), fetched,
                "valid warmed file listing must perform no native fetches");
            shutdown.send_replace(crate::sync::runtime::SyncShutdown::Stop);
        };
        let (result, ()) = futures_util::join!(Box::pin(worker), Box::pin(caller));
        result.unwrap();
    })).await.expect("live file listing must converge after background sync");
}
