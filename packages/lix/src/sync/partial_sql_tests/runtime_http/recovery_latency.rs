//! A rejected upload must not impose its retry deadline on network recovery.
use super::*;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

#[derive(Clone)]
struct SlowRecoveryClient {
    inner: Client,
    path: &'static str,
    conflicts: Arc<AtomicUsize>,
    started: Arc<AtomicUsize>,
    finished: Arc<AtomicBool>,
}
impl RawHttpClient for SlowRecoveryClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            if self.conflicts.load(Ordering::SeqCst) > 0
                && request.url.contains(self.path)
                && !self.finished.load(Ordering::SeqCst)
            {
                self.started.fetch_add(1, Ordering::SeqCst);
                // Exceed even the maximum upload backoff. Cancellation must not
                // make a subsequent attempt faster, or the regression is hidden.
                tokio::time::sleep(Duration::from_secs(6)).await;
                self.finished.store(true, Ordering::SeqCst);
            }
            let push = request.url.ends_with("/sync/push");
            let response = self.inner.send(request).await?;
            if push && response.status == 409 {
                self.conflicts.fetch_add(1, Ordering::SeqCst);
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

async fn slow_recovery(path: &'static str) {
    let started = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicBool::new(false));
    tokio::time::timeout(Duration::from_secs(20), async {
        let backing = Memory::new();
        let authority = open_lix().with_storage(backing.clone()).await.unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value(key,value) VALUES('local','B'),('remote','B')",
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
            "SELECT key,value FROM lix_key_value WHERE key IN ('local','remote')",
            "UPDATE lix_key_value SET value='L' WHERE key='local'",
        ] {
            execute_hydrating(&session, &storage, &old, &authority, sql, &[], &mut fetches)
                .await
                .unwrap();
        }
        prepare_baseline_jump_spines(&storage, &old, &authority, &mut fetches)
            .await
            .unwrap();
        crate::sync::admit_sync_authority_storage(&authority.storage_adapter(), None)
            .await
            .unwrap();
        authority
            .execute("UPDATE lix_key_value SET value='R' WHERE key='remote'", &[])
            .await
            .unwrap();
        let (shutdown, shutdown_rx) =
            tokio::sync::watch::channel(crate::sync::runtime::SyncShutdown::Running);
        let (_demand_tx, demand_rx) = tokio::sync::mpsc::channel(1);
        let (_changes, changes_rx) = tokio::sync::watch::channel(0);
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
            loop {
                let result = authority
                    .execute("SELECT value FROM lix_key_value WHERE key='local'", &[])
                    .await
                    .unwrap();
                let current = engine.sync_mode().partial_admission().unwrap();
                if result.rows()[0].get::<serde_json::Value>("value").unwrap() == "L"
                    && current.descriptor().cursor > old.descriptor().cursor
                {
                    let result = session
                        .execute("SELECT value FROM lix_key_value WHERE key='remote'", &[])
                        .await
                        .unwrap();
                    assert_eq!(
                        result.rows()[0].get::<serde_json::Value>("value").unwrap(),
                        "R"
                    );
                    let local = session
                        .execute("SELECT value FROM lix_key_value WHERE key='local'", &[])
                        .await
                        .unwrap();
                    assert_eq!(
                        local.rows()[0].get::<serde_json::Value>("value").unwrap(),
                        "L"
                    );
                    let remote = authority
                        .execute("SELECT value FROM lix_key_value WHERE key='remote'", &[])
                        .await
                        .unwrap();
                    assert_eq!(
                        remote.rows()[0].get::<serde_json::Value>("value").unwrap(),
                        "R"
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
                client.finished.load(Ordering::SeqCst),
                "complete the slow recovery input"
            );
            assert_eq!(
                client.started.load(Ordering::SeqCst),
                1,
                "upload retries must not cancel recovery"
            );
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
            "stale upload did not converge: slow recovery started {} times, completed {}",
            started.load(Ordering::SeqCst),
            finished.load(Ordering::SeqCst)
        )
    });
}
