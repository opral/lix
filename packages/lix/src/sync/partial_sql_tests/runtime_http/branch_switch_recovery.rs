use super::*;
use std::sync::atomic::AtomicUsize;

#[derive(Clone)]
struct ExpiringBranchClient {
    inner: Client,
    descriptors: Arc<AtomicUsize>,
    expire_next: Arc<AtomicBool>,
    target: String,
}
impl RawHttpClient for ExpiringBranchClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            if request.url.contains("/sync/descriptor") {
                self.descriptors.fetch_add(1, Ordering::SeqCst);
                if url::Url::parse(&request.url)
                    .unwrap()
                    .query_pairs()
                    .any(|(key, value)| key == "branchId" && value == self.target)
                    && self.expire_next.swap(false, Ordering::SeqCst)
                {
                    return Err(LixError::new(
                        "LIX_PARTIAL_BASELINE_EXPIRED",
                        "expired during branch preparation",
                    ));
                }
            }
            self.inner.send(request).await
        })
    }
}

#[tokio::test]
async fn branch_switch_awaits_pending_source_and_restarts_expired_candidate() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let backing = Memory::new();
        let authority = open_lix().with_storage(backing.clone()).await.unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value(key,value) VALUES('local','before')",
                &[],
            )
            .await
            .unwrap();
        let target = authority
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "switch-recovery-target".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let server = open_lix()
            .with_storage(backing)
            .serve()
            .with_embedded_lix_id()
            .await
            .unwrap();
        let client = ExpiringBranchClient {
            inner: Client {
                server,
                lose_body: Arc::new(AtomicBool::new(false)),
            },
            descriptors: Arc::default(),
            expire_next: Arc::default(),
            target: target.id.clone(),
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
        execute_hydrating(
            &session,
            &storage,
            &old,
            &authority,
            "UPDATE lix_key_value SET value='pending' WHERE key='local'",
            &[],
            &mut Fetches::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            crate::sync::partial_publication::require_clean_switch_source(&engine, &old)
                .await
                .unwrap_err()
                .code,
            "LIX_PARTIAL_BRANCH_SWITCH_PENDING"
        );
        let (demand_tx, demand_rx) =
            tokio::sync::mpsc::channel::<crate::sync::runtime::SyncDemand>(1);
        let (shutdown, shutdown_rx) =
            tokio::sync::watch::channel(crate::sync::runtime::SyncShutdown::Running);
        let worker = crate::sync::partial_runtime::run_partial_worker_with_engine(
            storage.clone(),
            old.clone(),
            Some(transport.clone()),
            || Box::pin(async { Err(LixError::unknown("unexpected reconnect")) }),
            shutdown_rx,
            demand_rx,
            None,
            Some(engine.clone()),
        );
        let caller = async {
            client.expire_next.store(true, Ordering::SeqCst);
            let prepared = crate::sync::partial_branch_switch::prepare_existing_branch_with_retry(
                engine.clone(),
                &transport,
                &target.id,
                Some(&demand_tx),
            )
            .await
            .expect("one awaited switch recovers pending edits and descriptor expiry");
            let primary_gate = Arc::new(tokio::sync::Mutex::new(()));
            let completion = Arc::new(
                session
                    .partial_switch_completion(
                        target.id.clone(),
                        Some(primary_gate.clone().lock_owned().await),
                    )
                    .await
                    .unwrap(),
            );
            let source = engine.sync_mode().partial_admission().unwrap();
            execute_hydrating(
                &session,
                &storage,
                &source,
                &authority,
                "SELECT value FROM lix_key_value WHERE key='new-scope-before-publication'",
                &[],
                &mut Fetches::default(),
            )
            .await
            .unwrap();
            let error = crate::sync::partial_publication::publish_prepared_partial(
                engine.clone(),
                prepared.with_branch_switch_completion(completion.clone()),
            )
            .await
            .unwrap_err();
            assert_eq!(
                error.code, "LIX_PARTIAL_READ_INTEREST_CHANGED",
                "new read interest invalidates candidate publication"
            );
            assert!(
                primary_gate.try_lock().is_err(),
                "failed publication retains the switch gate for retry"
            );
            assert_ne!(session.active_branch_id().await.unwrap(), target.id);
            let prepared = crate::sync::partial_branch_switch::prepare_existing_branch_with_retry(
                engine.clone(),
                &transport,
                &target.id,
                Some(&demand_tx),
            )
            .await
            .unwrap();
            crate::sync::partial_publication::publish_prepared_partial(
                engine.clone(),
                prepared.with_branch_switch_completion(completion.clone()),
            )
            .await
            .unwrap();
            drop(completion);
            assert!(
                primary_gate.try_lock().is_ok(),
                "completed switch releases its gates"
            );
            assert_eq!(session.active_branch_id().await.unwrap(), target.id);
            assert_eq!(
                engine
                    .sync_mode()
                    .partial_admission()
                    .unwrap()
                    .descriptor()
                    .selected_branch
                    .branch_id,
                target.id
            );
            assert!(
                client.descriptors.load(Ordering::SeqCst) >= 4,
                "expired descriptor is replaced within the original switch"
            );
            let confirmed = authority
                .execute("SELECT value FROM lix_key_value WHERE key='local'", &[])
                .await
                .unwrap();
            assert_eq!(
                confirmed.rows()[0]
                    .get::<serde_json::Value>("value")
                    .unwrap(),
                "pending"
            );
            shutdown.send_replace(crate::sync::runtime::SyncShutdown::Stop);
        };
        let (result, ()) = futures_util::join!(worker, caller);
        result.unwrap();
    })
    .await
    .expect("switch recovery must not deadlock");
}
