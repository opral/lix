use super::*;

#[derive(Clone)]
struct FailingDescriptorClient {
    inner: ExpiringClient,
    enabled: Arc<AtomicBool>,
    unavailable: Arc<AtomicBool>,
}
impl RawHttpClient for FailingDescriptorClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            if request.url.contains("/sync/descriptor?") && request.url.contains("after=") {
                while !self.enabled.load(Ordering::SeqCst) {
                    crate::sync::platform::sleep(std::time::Duration::from_millis(1)).await;
                }
                if self.unavailable.load(Ordering::SeqCst) {
                    return Ok(RawHttpResponse {
                        status: 503,
                        status_text: "Service Unavailable".into(),
                        body: br#"{"code":"TEST_AUTHORITY_UNAVAILABLE","message":"injected descriptor outage"}"#.to_vec(),
                    });
                }
            }
            self.inner.send(request).await
        })
    }
}

#[tokio::test]
async fn unavailable_retained_history_does_not_block_background_file_updates() {
    tokio::time::timeout(std::time::Duration::from_secs(30), Box::pin(async {
        let backing = Memory::new();
        let authority = open_lix().with_storage(backing.clone()).await.unwrap();
        authority.set_sync_role(crate::sync::SyncRole::Authority).unwrap();
        for path in ["/gtm/edit.txt", "/gtm/delete.txt", "/elsewhere/keep.txt"] {
            authority.upsert_file_content(path, b"before".to_vec()).await.unwrap();
        }
        let before = authority.execute("SELECT commit_id FROM lix_create_checkpoint()", &[]).await.unwrap().rows()[0].get::<String>("commit_id").unwrap();
        authority.upsert_file_content("/gtm/edit.txt", b"historical change".to_vec()).await.unwrap();
        let after = authority.execute("SELECT commit_id FROM lix_create_checkpoint()", &[]).await.unwrap().rows()[0].get::<String>("commit_id").unwrap();
        let historical_sql = format!("SELECT diff_type, from_path, to_path FROM lix_diff('lix_file', '{before}', '{after}') ORDER BY to_path");
        let server = open_lix().with_storage(backing).serve().with_embedded_lix_id().await.unwrap();
        let enabled = Arc::new(AtomicBool::new(false));
        let client = ExpiringClient {
            server, lease: Arc::default(), expire: Arc::default(), lose_session: Arc::default(), fetches: Arc::default(),
            live_updates: Some(enabled.clone()),
        };
        let unavailable = Arc::new(AtomicBool::new(true));
        let transport = HttpSyncTransport::connect_with(FailingDescriptorClient {
            inner: client.clone(), enabled: enabled.clone(), unavailable: unavailable.clone(),
        },
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
            storage.clone(), old.clone(), Some(transport),
            || Box::pin(async {Err(LixError::unknown("unexpected reconnect"))}),
            shutdown_rx, receiver, None, Some(engine.clone()),
        );
        let caller = async {
            let sql = "SELECT path,name FROM lix_file ORDER BY path";
            let initial = lix.execute(sql, &[]).await.unwrap();
            let initial_authority = authority_sql(&client.server, authority.lix_id(), None, sql).await;
            assert_eq!(initial.rows(), initial_authority.rows());
            let initial_fetches = client.fetches.load(Ordering::SeqCst);
            assert_eq!(lix.execute(sql, &[]).await.unwrap().rows(), initial.rows());
            assert_eq!(client.fetches.load(Ordering::SeqCst), initial_fetches);
            let historical = lix.execute(&historical_sql, &[]).await.unwrap();
            assert_eq!(historical.rows().len(), 1);
            // Model a restored journal entry whose immutable commit has since
            // been pruned remotely. It must remain retained, never subscribed.
            let registry = engine.sync_mode().read_interests().unwrap();
            registry.register(crate::hot_state::LogicalReadInterest::Diff {
                branch_id: None,
                relation: "lix_file".into(),
                from: crate::hot_state::DiffInterestEndpoint::Fixed(uuid::Uuid::now_v7().to_string()),
                to: crate::hot_state::DiffInterestEndpoint::Fixed(uuid::Uuid::now_v7().to_string()),
                filter: Default::default(),
                retain_payloads: false,
                projected_columns: vec!["diff_type".into()],
                limit: None,
            }).unwrap();
            crate::sync::partial_interest_journal::flush_partial_read_interests(&storage, &old, &registry).await.unwrap();
            authority_sql(&client.server, authority.lix_id(), None,
                "INSERT INTO lix_file(path,content) VALUES('/gtm/added.txt',CAST('new' AS BYTEA))").await;
            enabled.store(true, Ordering::SeqCst);
            while lix.sync_health().state != crate::sync::SyncHealthState::Stalled {
                crate::sync::platform::sleep(std::time::Duration::from_millis(2)).await;
            }
            let stalled = lix.sync_health();
            assert_eq!(stalled.applied_cursor, Some(old.descriptor().cursor));
            assert!(stalled.failures.contains_key(&crate::sync::SyncPhase::Descriptor));
            let during_outage = client.fetches.load(Ordering::SeqCst);
            for _ in 0..3 {
                assert_eq!(lix.execute(sql, &[]).await.unwrap().rows(), initial.rows());
                assert_eq!(lix.execute(&historical_sql, &[]).await.unwrap().rows(), historical.rows());
                assert_eq!(lix.sync_health(), stalled, "successful local SQL must not hide the HTTP outage");
            }
            assert_eq!(client.fetches.load(Ordering::SeqCst), during_outage);
            unavailable.store(false, Ordering::SeqCst);
            // Let the real descriptor wait lane observe and settle the remote
            // cursor. No manual descriptor publication or foreground recovery.
            loop {
                let admitted = engine.sync_mode().partial_admission().unwrap();
                if admitted.descriptor().cursor > old.descriptor().cursor
                    && lix.sync_health().applied_cursor == Some(admitted.descriptor().cursor)
                    && crate::sync::partial_publication::require_clean_switch_source(&engine, &admitted).await.is_ok()
                {
                    break;
                }
                crate::sync::platform::sleep(std::time::Duration::from_millis(2)).await;
            }
            assert_eq!(lix.sync_health().state, crate::sync::SyncHealthState::Running);
            assert!(lix.sync_health().failures.is_empty(), "successful descriptor recovery must clear the outage");
            let expected = authority_sql(&client.server, authority.lix_id(), None, sql).await;
            let actual = lix.execute(sql, &[]).await.unwrap();
            assert_eq!(actual.rows().len(), initial.rows().len() + 1, "remote insertion must invalidate the warm listing");
            assert_eq!(actual.rows(), expected.rows(),
                "settled local and authoritative ordered file cells must agree");
            let history_fetches = client.fetches.load(Ordering::SeqCst);
            assert_eq!(lix.execute(&historical_sql, &[]).await.unwrap().rows(), historical.rows(),
                "cached immutable history must survive unrelated descriptor advancement");
            assert_eq!(client.fetches.load(Ordering::SeqCst), history_fetches,
                "already retained immutable inputs must remain locally readable");
            let fetched = client.fetches.load(Ordering::SeqCst);
            for _ in 0..3 {
                assert_eq!(lix.execute(&historical_sql, &[]).await.unwrap().rows(), historical.rows());
                assert_eq!(lix.execute(sql, &[]).await.unwrap().rows(), expected.rows());
            }
            assert_eq!(client.fetches.load(Ordering::SeqCst), fetched,
                "valid warmed file listing must perform no native fetches");
            shutdown.send_replace(crate::sync::runtime::SyncShutdown::Stop);
        };
        let (result, ()) = futures_util::join!(Box::pin(worker), Box::pin(caller));
        result.unwrap();
    })).await.expect("live file listing must converge after background sync");
}
