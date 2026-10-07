//! A rejected upload must not impose its retry deadline on network recovery.
use super::*;
use std::sync::atomic::AtomicUsize;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

#[derive(Clone)]
struct SlowRecoveryClient {
    inner: Client,
    path: &'static str,
    conflicts: Arc<AtomicUsize>,
    started: Arc<AtomicUsize>,
    finished: Arc<AtomicBool>,
    release_descriptor: Option<Arc<Notify>>,
    pushes: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    requests: Arc<std::sync::Mutex<Vec<String>>>,
    injected_conflicts_remaining: Arc<AtomicUsize>,
    push_times: Arc<std::sync::Mutex<Vec<Instant>>>,
}
impl RawHttpClient for SlowRecoveryClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            let push = request.url.ends_with("/sync/push");
            self.requests.lock().unwrap().push(request.url.clone());
            if push {
                self.push_times.lock().unwrap().push(Instant::now());
                self.pushes
                    .lock()
                    .unwrap()
                    .push(request.body.clone().unwrap_or_default());
                if self
                    .injected_conflicts_remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
                {
                    self.conflicts.fetch_add(1, Ordering::SeqCst);
                    return Ok(RawHttpResponse {
                        status: 409,
                        status_text: "Conflict".into(),
                        body: serde_json::to_vec(&serde_json::json!({
                            "error": {
                                "code": LixError::CODE_TRANSACTION_CONFLICT,
                                "message": "injected unchanged-basis CAS conflict"
                            }
                        }))
                        .unwrap(),
                    });
                }
            }
            if self.conflicts.load(Ordering::SeqCst) > 0
                && request.url.contains(self.path)
                && !self.finished.load(Ordering::SeqCst)
            {
                self.started.fetch_add(1, Ordering::SeqCst);
                if let Some(release) = &self.release_descriptor {
                    release.notified().await;
                } else {
                    // Exceed even the maximum upload backoff. Cancellation must
                    // not make a subsequent attempt faster, or the regression
                    // is hidden.
                    tokio::time::sleep(Duration::from_secs(6)).await;
                }
                self.finished.store(true, Ordering::SeqCst);
            }
            let response = self.inner.send(request).await?;
            if push && response.status == 409 {
                self.conflicts.fetch_add(1, Ordering::SeqCst);
            }
            Ok(response)
        })
    }
}

#[derive(Clone)]
struct ExpiredPendingWaveClient {
    inner: Client,
    successful_pushes: Arc<AtomicUsize>,
    push_ack: Arc<Notify>,
    expiration_sent: Arc<AtomicBool>,
    recovery_descriptor_started: Arc<Notify>,
    recovery_descriptor_released: Arc<AtomicBool>,
    recovery_descriptor_release: Arc<Notify>,
    final_descriptor_started: Arc<Notify>,
    final_descriptor_released: Arc<AtomicBool>,
    final_descriptor_release: Arc<Notify>,
    push_leases: Arc<std::sync::Mutex<Vec<String>>>,
}

impl RawHttpClient for ExpiredPendingWaveClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            let path = url::Url::parse(&request.url)
                .map_err(|error| LixError::unknown(error.to_string()))?
                .path()
                .to_owned();
            let is_push = path.ends_with("/sync/push");
            let is_descriptor = path.ends_with("/sync/descriptor");
            if is_push {
                let lease = request
                    .headers
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case("lix-native-baseline-lease"))
                    .map(|(_, value)| value.clone())
                    .unwrap_or_default();
                self.push_leases.lock().unwrap().push(lease);
            }
            if is_descriptor {
                let pushes = self.successful_pushes.load(Ordering::SeqCst);
                if pushes >= 2 && !self.final_descriptor_released.load(Ordering::SeqCst) {
                    self.final_descriptor_started.notify_one();
                    loop {
                        if self.final_descriptor_released.load(Ordering::SeqCst) {
                            break;
                        }
                        self.final_descriptor_release.notified().await;
                    }
                } else if pushes >= 1 && !self.recovery_descriptor_released.load(Ordering::SeqCst) {
                    if self.expiration_sent.load(Ordering::SeqCst) {
                        self.recovery_descriptor_started.notify_one();
                    }
                    loop {
                        if self.recovery_descriptor_released.load(Ordering::SeqCst) {
                            break;
                        }
                        self.recovery_descriptor_release.notified().await;
                    }
                }
            }
            if path.ends_with("/sync/baseline-lease/renew")
                && self.successful_pushes.load(Ordering::SeqCst) > 0
                && !self.expiration_sent.swap(true, Ordering::SeqCst)
            {
                return Err(LixError::new(
                    "LIX_PARTIAL_BASELINE_EXPIRED",
                    "injected expiry after the first acknowledged upload",
                ));
            }
            let response = self.inner.send(request).await?;
            if is_push && (200..300).contains(&response.status) {
                self.successful_pushes.fetch_add(1, Ordering::SeqCst);
                self.push_ack.notify_one();
            }
            Ok(response)
        })
    }
}

#[tokio::test]
async fn stale_upload_retry_does_not_cancel_slow_fresh_descriptor() {
    slow_recovery("/sync/descriptor").await;
}

#[tokio::test]
async fn stale_upload_retry_does_not_cancel_slow_reconciliation_input() {
    slow_recovery("/sync/native-metadata").await;
}

#[tokio::test]
async fn local_notifications_do_not_preempt_stale_upload_recovery() {
    slow_recovery_inner("/sync/descriptor", true, false, false, 0).await;
}

#[tokio::test]
async fn global_conflict_is_not_hidden_by_selected_file_progress() {
    slow_recovery_inner("/sync/descriptor", true, true, false, 0).await;
}

#[tokio::test]
async fn unchanged_authority_conflict_retries_the_same_frozen_upload() {
    slow_recovery_inner("/never-delay", false, false, false, 1).await;
}

#[tokio::test]
async fn unchanged_authority_conflict_retries_frozen_global_creation() {
    slow_recovery_inner("/never-delay", false, false, true, 1).await;
}

#[tokio::test]
async fn repeated_unchanged_authority_conflicts_keep_exponential_backoff() {
    slow_recovery_inner("/never-delay", false, false, false, 2).await;
}

#[tokio::test]
async fn expired_serving_basis_retries_confirmed_wave_before_adoption() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let backing = Memory::new();
        let authority = open_lix().with_storage(backing.clone()).await.unwrap();
        authority
            .execute(
                "INSERT INTO lix_file(path,content) VALUES ('/lease-wave.txt',CAST('B' AS BYTEA))",
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
        let inner_client = Client {
            server,
            lose_body: Arc::new(AtomicBool::new(false)),
        };
        let initial_transport = HttpSyncTransport::connect_with(
            inner_client.clone(),
            &format!("https://example.test/lix/{}", authority.lix_id()),
        )
        .await
        .unwrap();
        let wrapper = initial_transport
            .partial_replica_descriptor(None)
            .await
            .unwrap();
        let initial = Arc::new(
            PartialReplicaState::from_leased(
                initial_transport.protocol_url().into(),
                authority.active_account_id().into(),
                uuid::Uuid::now_v7().to_string(),
                wrapper.wire,
            )
            .unwrap(),
        );
        let storage = StorageAdapter::new(Memory::new());
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let preconditions = stage_partial_bootstrap(&read, &mut writes, &initial).unwrap();
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
            Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &initial)
                .await
                .unwrap();
        let engine = Arc::new(engine);
        engine
            .sync_mode()
            .admit_partial_replica(initial.clone(), crate::sync::partial_replica_write_capability());
        storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
        let mut fetches = Fetches::default();
        for sql in [
            "SELECT path,content FROM lix_file WHERE path='/lease-wave.txt'",
            "UPDATE lix_file SET content=CAST('L1' AS BYTEA) WHERE path='/lease-wave.txt'",
        ] {
            execute_hydrating(&session, &storage, &initial, &authority, sql, &[], &mut fetches)
                .await
                .unwrap();
        }

        // Expire the serving lease shortly after the worker starts, while
        // leaving the authority's actual lease row available for normal reads.
        let mut short_lease = initial.baseline_lease().clone();
        short_lease.expires_at_ms = crate::telemetry::unix_time_ms() + 6_000;
        let old = Arc::new(initial.with_reacquired_baseline_lease(short_lease).unwrap());
        let read = storage.begin_read(Default::default()).await.unwrap();
        let (_, receipt) = crate::sync::partial_state::load_partial_replica_state(&read)
            .await
            .unwrap()
            .unwrap();
        let mut writes = storage.new_write_set();
        let guard = crate::sync::partial_state::stage_partial_replica_state(
            &mut writes,
            &old,
            Some(receipt),
        )
        .unwrap();
        drop(read);
        storage
            .commit_partial_replica_write_set(
                crate::sync::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: vec![guard],
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        engine
            .sync_mode()
            .admit_partial_replica(old.clone(), crate::sync::partial_replica_write_capability());

        let client = ExpiredPendingWaveClient {
            inner: inner_client,
            successful_pushes: Arc::default(),
            push_ack: Arc::new(Notify::new()),
            expiration_sent: Arc::new(AtomicBool::new(false)),
            recovery_descriptor_started: Arc::new(Notify::new()),
            recovery_descriptor_released: Arc::new(AtomicBool::new(false)),
            recovery_descriptor_release: Arc::new(Notify::new()),
            final_descriptor_started: Arc::new(Notify::new()),
            final_descriptor_released: Arc::new(AtomicBool::new(false)),
            final_descriptor_release: Arc::new(Notify::new()),
            push_leases: Arc::new(std::sync::Mutex::new(Vec::new())),
        };
        let authority_url = format!("https://example.test/lix/{}", authority.lix_id());
        let transport = HttpSyncTransport::connect_with(client.clone(), &authority_url)
            .await
            .unwrap();
        transport
            .bind_native_baseline_lease(old.baseline_lease())
            .unwrap();
        let (shutdown, shutdown_rx) =
            tokio::sync::watch::channel(crate::sync::runtime::SyncShutdown::Running);
        let (demand_tx, demand_rx) = tokio::sync::mpsc::channel(1);
        let (changes, changes_rx) = tokio::sync::watch::channel(0);
        let worker = crate::sync::partial_runtime::run_partial_worker_with_engine(
            storage.clone(),
            old.clone(),
            Some(transport),
            || Box::pin(async { Err(LixError::unknown("unexpected reconnect")) }),
            shutdown_rx,
            demand_rx,
            Some(changes_rx),
            Some(engine.clone()),
        );

        let observer = async {
            tokio::time::timeout(Duration::from_secs(5), client.push_ack.notified())
                .await
                .expect("the first wave should be acknowledged");
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let read = storage.begin_read(Default::default()).await.unwrap();
                    let (push, _, _) = crate::sync::partial_push_state::load_partial_push_state(
                        &read,
                        &old,
                        &old.descriptor().selected_branch.branch_id,
                    )
                    .await
                    .unwrap();
                    let control = crate::branch::BranchHeadControlContext::new()
                        .reader(&read)
                        .load(&old.descriptor().selected_branch.branch_id)
                        .await
                        .unwrap()
                        .unwrap();
                    if push.prepared.is_none()
                        && push.confirmed.head == control.head_commit_id
                        && push.confirmed.checkpoint
                            == control
                                .working_diff_checkpoint_commit_id
                                .map(|id| id.to_string())
                                .unwrap()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("the first wave receipt should settle before the next local write");
            let authority_l1 = authority
                .execute(
                    "SELECT CAST(content AS TEXT) AS value FROM lix_file WHERE path='/lease-wave.txt'",
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(
                authority_l1.rows()[0].get::<String>("value").unwrap(),
                "L1"
            );

            session
                .execute(
                    "UPDATE lix_file SET content=CAST('L2' AS BYTEA) WHERE path='/lease-wave.txt'",
                    &[],
                )
                .await
                .unwrap();

            // The worker has cancelled its pre-expiry long poll and is now
            // holding a fresh authenticated descriptor after renewal reported
            // expiry. Local notification must not preempt this dependency.
            tokio::time::timeout(
                Duration::from_secs(8),
                client.recovery_descriptor_started.notified(),
            )
            .await
            .expect("expiry should lead to a fresh descriptor");
            assert!(client.expiration_sent.load(Ordering::SeqCst));
            assert_eq!(
                engine.sync_mode().partial_admission().unwrap().as_ref(),
                old.as_ref(),
                "the expired serving basis remains fenced until durable publication"
            );
            changes.send_replace(1);
            client
                .recovery_descriptor_released
                .store(true, Ordering::SeqCst);
            client.recovery_descriptor_release.notify_waiters();

            tokio::time::timeout(Duration::from_secs(5), client.push_ack.notified())
                .await
                .expect("the pending second wave should use the fresh descriptor lease");
            let authority_l2 = authority
                .execute(
                    "SELECT CAST(content AS TEXT) AS value FROM lix_file WHERE path='/lease-wave.txt'",
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(
                authority_l2.rows()[0].get::<String>("value").unwrap(),
                "L2"
            );
            tokio::time::timeout(Duration::from_secs(3), client.final_descriptor_started.notified())
                .await
                .expect("the final descriptor should wait before admission publication");
            assert_eq!(
                engine.sync_mode().partial_admission().unwrap().as_ref(),
                old.as_ref(),
                "successful upload alone must not un-fence the expired serving roots"
            );
            let push_leases = client.push_leases.lock().unwrap().clone();
            assert!(push_leases.len() >= 2);
            assert_eq!(push_leases[0], old.baseline_lease().lease_id);
            assert_ne!(push_leases[1], old.baseline_lease().lease_id);

            client
                .final_descriptor_released
                .store(true, Ordering::SeqCst);
            client.final_descriptor_release.notify_waiters();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let current = engine.sync_mode().partial_admission().unwrap();
                    if current.baseline_lease().lease_id != old.baseline_lease().lease_id
                        && current.descriptor().selected_branch.head.commit_id
                            == authority
                                .partial_replica_descriptor(None)
                                .await
                                .unwrap()
                                .selected_branch
                                .head
                                .commit_id
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("durable publication should adopt the new serving lease");
            let local_l2 = session
                .execute(
                    "SELECT CAST(content AS TEXT) AS value FROM lix_file WHERE path='/lease-wave.txt'",
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(local_l2.rows()[0].get::<String>("value").unwrap(), "L2");
            shutdown
                .send(crate::sync::runtime::SyncShutdown::Stop)
                .unwrap();
        };
        let (result, ()) = tokio::join!(worker, observer);
        result.unwrap();
        authority.close().await.unwrap();
        session.close().await.unwrap();
        let _ = demand_tx;
    })
    .await
    .expect("expired pending-wave recovery timed out");
}

async fn slow_recovery(path: &'static str) {
    slow_recovery_inner(path, false, false, false, 0).await;
}

async fn slow_recovery_inner(
    path: &'static str,
    local_notifications: bool,
    global_conflict: bool,
    local_branch_creation: bool,
    injected_conflicts: usize,
) {
    let started = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicBool::new(false));
    let release_descriptor = local_notifications.then(|| Arc::new(Notify::new()));
    let pushes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let push_times = Arc::new(std::sync::Mutex::new(Vec::new()));
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let inject_conflict_case = injected_conflicts > 0;
    let injected_conflicts_remaining = Arc::new(AtomicUsize::new(injected_conflicts));
    tokio::time::timeout(Duration::from_secs(20), async {
        let backing = Memory::new();
        let authority = open_lix().with_storage(backing.clone()).await.unwrap();
        authority
            .execute(
                "INSERT INTO lix_file(path,content) VALUES \
                    ('/recovery-local.txt',CAST('B' AS BYTEA)), \
                    ('/recovery-remote.txt',CAST('B' AS BYTEA))",
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
        let client = SlowRecoveryClient {
            inner: Client {
                server,
                lose_body: Arc::new(AtomicBool::new(false)),
            },
            path,
            conflicts: Arc::default(),
            started: started.clone(),
            finished: finished.clone(),
            release_descriptor: release_descriptor.clone(),
            pushes: pushes.clone(),
            push_times: push_times.clone(),
            requests: requests.clone(),
            injected_conflicts_remaining: injected_conflicts_remaining.clone(),
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
        let mut fetches = Fetches::default();
        for sql in [
            "SELECT path,content FROM lix_file WHERE path IN ('/recovery-local.txt','/recovery-remote.txt')",
            "UPDATE lix_file SET content=CAST('L' AS BYTEA) WHERE path='/recovery-local.txt'",
        ] {
            execute_hydrating(&session, &storage, &old, &authority, sql, &[], &mut fetches)
                .await
                .unwrap();
        }
        let local_branch_id = if global_conflict || local_branch_creation {
            Some(
                create_local_branch(
                    &session,
                    &engine,
                    &old,
                    &transport,
                    "client-global-branch",
                )
                .await,
            )
        } else {
            None
        };
        prepare_baseline_jump_spines(&storage, &old, &authority, &mut fetches)
            .await
            .unwrap();
        crate::sync::admit_sync_authority_storage(&authority.storage_adapter(), None)
            .await
            .unwrap();
        if global_conflict {
            authority
                .create_branch(crate::CreateBranchOptions {
                    id: None,
                    name: "authority-global-advance".into(),
                    from_commit_id: None,
                })
                .await
                .unwrap();
        }
        if !global_conflict && !inject_conflict_case {
            authority
                .execute(
                    "UPDATE lix_file SET content=CAST('R' AS BYTEA) WHERE path='/recovery-remote.txt'",
                    &[],
                )
                .await
                .unwrap();
        }
        let (shutdown, shutdown_rx) =
            tokio::sync::watch::channel(crate::sync::runtime::SyncShutdown::Running);
        let (demand_tx, demand_rx) = tokio::sync::mpsc::channel(1);
        let (changes, changes_rx) = tokio::sync::watch::channel(0);
        let worker = crate::sync::partial_runtime::run_partial_worker_with_engine(
            storage.clone(),
            old.clone(),
            Some(transport),
            || Box::pin(async { Err(LixError::unknown("unexpected reconnect")) }),
            shutdown_rx,
            demand_rx,
            Some(changes_rx),
            Some(engine.clone()),
        );
        let observer = async {
            if local_notifications {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while started.load(Ordering::SeqCst) == 0 {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .expect("stale push should start authoritative descriptor recovery");

                // Simulate sustained local edit notifications while the fresh
                // descriptor is deliberately held in flight. They must remain
                // queued until the conflict's recovery dependency completes.
                for generation in 1..=20 {
                    changes.send_replace(generation);
                    tokio::task::yield_now().await;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert_eq!(started.load(Ordering::SeqCst), 1);
                let captured_count = pushes.lock().unwrap().len();
                assert_eq!(
                    captured_count,
                    if global_conflict { 2 } else { 1 },
                    "do not resend a frozen stale body before recovery"
                );
                if global_conflict {
                    let uploaded_selected_file = authority
                        .execute(
                            "SELECT CAST(content AS TEXT) AS value FROM lix_file WHERE path='/recovery-local.txt'",
                            &[],
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        uploaded_selected_file.rows()[0]
                            .get::<String>("value")
                            .unwrap(),
                        "L",
                        "selected-file progress must remain durable while GLOBAL recovery waits"
                    );
                }
                release_descriptor
                    .as_ref()
                    .expect("test descriptor gate")
                    .notify_one();
            }

            loop {
                let result = authority
                    .execute(
                        "SELECT CAST(content AS TEXT) AS value FROM lix_file WHERE path='/recovery-local.txt'",
                        &[],
                    )
                    .await
                    .unwrap();
                let current = engine.sync_mode().partial_admission().unwrap();
                let file_is_authoritative = result.rows()[0]
                    .get::<String>("value")
                    .unwrap()
                    == "L";
                let cursor_advanced = current.descriptor().cursor > old.descriptor().cursor;
                let global_branch_is_authoritative = if let Some(branch_id) = &local_branch_id {
                    authority
                        .execute(
                            "SELECT id FROM lix_branch WHERE id=$1",
                            &[Value::Text(branch_id.clone())],
                        )
                        .await
                        .unwrap()
                        .rows()
                        .len()
                        == 1
                } else {
                    true
                };
                if file_is_authoritative
                    && global_branch_is_authoritative
                    && (inject_conflict_case || cursor_advanced)
                {
                    // The newly published basis is intentionally cold. Exercise
                    // its SQL demand through the same worker under test.
                    let mut retry = crate::sync::SyncDemandRetry::default();
                    let result = loop {
                        match session
                            .execute(
                                "SELECT CAST(content AS TEXT) AS value FROM lix_file WHERE path='/recovery-remote.txt'",
                                &[],
                            )
                            .await
                        {
                            Ok(result) => break result,
                            Err(error) => retry
                                .hydrate_for_retry(Some(&demand_tx), error)
                                .await
                                .unwrap(),
                        }
                    };
                    assert_eq!(
                        result.rows()[0].get::<String>("value").unwrap(),
                        if global_conflict || inject_conflict_case { "B" } else { "R" }
                    );
                    let mut retry = crate::sync::SyncDemandRetry::default();
                    let local = loop {
                        match session
                            .execute(
                                "SELECT CAST(content AS TEXT) AS value FROM lix_file WHERE path='/recovery-local.txt'",
                                &[],
                            )
                            .await
                        {
                            Ok(result) => break result,
                            Err(error) => retry
                                .hydrate_for_retry(Some(&demand_tx), error)
                                .await
                                .unwrap(),
                        }
                    };
                    assert_eq!(
                        local.rows()[0].get::<String>("value").unwrap(),
                        "L"
                    );
                    let remote = authority
                        .execute(
                            "SELECT CAST(content AS TEXT) AS value FROM lix_file WHERE path='/recovery-remote.txt'",
                            &[],
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        remote.rows()[0].get::<String>("value").unwrap(),
                        if global_conflict || inject_conflict_case { "B" } else { "R" }
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(
                client.conflicts.load(Ordering::SeqCst) > 0,
                "exercise a real stale upload"
            );
            assert!(
                client.finished.load(Ordering::SeqCst)
                    || (inject_conflict_case
                        && injected_conflicts_remaining.load(Ordering::SeqCst) == 0),
                "complete the slow recovery input or consume the injected conflict"
            );
            if let Some(branch_id) = local_branch_id {
                let branch = authority
                    .execute(
                        "SELECT id FROM lix_branch WHERE id=$1",
                        &[Value::Text(branch_id)],
                    )
                    .await
                    .unwrap();
                assert_eq!(branch.rows().len(), 1, "complete GLOBAL recovery");
            }
            assert_eq!(
                client.started.load(Ordering::SeqCst),
                if inject_conflict_case { 0 } else { 1 },
                "upload retries and local notifications must not cancel recovery"
            );
            if inject_conflict_case {
                let bodies = pushes.lock().unwrap();
                assert!(bodies.len() >= injected_conflicts + 1, "retry the frozen attempt");
                for pair in bodies.windows(2).take(injected_conflicts) {
                    assert_eq!(pair[0], pair[1], "do not rebuild the immutable request");
                }
                drop(bodies);
                let times = push_times.lock().unwrap();
                for pair in times.windows(2).take(injected_conflicts) {
                    assert!(
                        pair[1].duration_since(pair[0]) >= Duration::from_millis(150),
                        "retry was not delayed by the first exponential backoff"
                    );
                }
                if injected_conflicts >= 2 {
                    assert!(
                        times[2].duration_since(times[1]) >= Duration::from_millis(300),
                        "repeated conflict reset backoff instead of increasing it"
                    );
                }
                drop(times);
                let requests = requests.lock().unwrap();
                let push_indexes = requests
                    .iter()
                    .enumerate()
                    .filter_map(|(index, url)| url.ends_with("/sync/push").then_some(index))
                    .collect::<Vec<_>>();
                assert!(push_indexes.len() >= 2);
                let first_push = push_indexes[0];
                let second_push = push_indexes[1];
                assert!(
                    requests[first_push + 1..second_push]
                        .iter()
                        .any(|url| url.contains("/sync/descriptor")),
                    "a fresh descriptor must precede the exact retry"
                );
            }
            shutdown
                .send(crate::sync::runtime::SyncShutdown::Stop)
                .unwrap();
        };
        let (result, ()) = tokio::join!(worker, observer);
        result.unwrap();
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "stale upload did not converge: slow recovery started {} times, completed {}, injected conflict pending {}, push count {}, requests {:?}",
            started.load(Ordering::SeqCst),
            finished.load(Ordering::SeqCst),
            injected_conflicts_remaining.load(Ordering::SeqCst),
            pushes.lock().unwrap().len(),
            requests.lock().unwrap()
        )
    });
}

async fn create_local_branch(
    session: &SessionContext<Memory>,
    engine: &Engine<Memory>,
    state: &PartialReplicaState,
    transport: &HttpSyncTransport<SlowRecoveryClient>,
    name: &str,
) -> String {
    let lease = transport
        .fork_native_baseline_lease(state.baseline_lease())
        .unwrap();
    let branch_id = uuid::Uuid::now_v7().to_string();
    let storage = engine.storage();
    for _ in 0..512 {
        match session
            .create_branch(crate::CreateBranchOptions {
                id: Some(branch_id.clone()),
                name: name.into(),
                from_commit_id: None,
            })
            .await
        {
            Ok(branch) => return branch.id,
            Err(error) => {
                let demand = crate::sync::runtime::native_sync_demand_request_for_error(&error)
                    .unwrap()
                    .unwrap_or_else(|| panic!("unexpected local branch dependency: {error:?}"));
                crate::sync::partial_runtime::hydrate_demand(&storage, state, &lease, demand)
                    .await
                    .unwrap();
            }
        }
    }
    panic!("local branch creation exceeded bounded native hydration")
}
