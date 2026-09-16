//! Public awaited SQL across lease expiry, using the canonical authority handler.
mod live_file_listing;
mod retained_history;
mod supplied_snapshot;
use crate::engine::{Engine, EngineOptions};
use crate::server_protocol::{LixServerProtocol, ServerProtocolBody, ServerProtocolContext};
use crate::storage_adapter::{StorageAdapter, StorageWriteOptions};
use crate::sync::SyncTransportFuture;
use crate::sync::http::{HttpSyncTransport, RawHttpClient, RawHttpRequest, RawHttpResponse};
use crate::sync::partial_state::PartialReplicaState;
use crate::{Lix, LixError, Memory, open_lix};
use http_body_util::BodyExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Clone)]
struct ExpiringClient {
    server: LixServerProtocol<Memory>,
    lease: Arc<std::sync::Mutex<String>>,
    expire: Arc<AtomicBool>,
    fetches: Arc<AtomicUsize>,
    live_updates: Option<Arc<AtomicBool>>,
}
impl RawHttpClient for ExpiringClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            if request.url.contains("/sync/descriptor?") && request.url.contains("after=") {
                match &self.live_updates {
                    Some(enabled) => while !enabled.load(Ordering::SeqCst) {
                        crate::sync::platform::sleep(std::time::Duration::from_millis(1)).await;
                    },
                    None => futures_util::future::pending::<()>().await,
                }
            }
            if request.url.contains("/sync/native-") {
                self.fetches.fetch_add(1, Ordering::SeqCst);
                if self.expire.load(Ordering::SeqCst)
                    && request.headers.iter().any(|(k, v)| {
                        k == "lix-native-baseline-lease" && v == &*self.lease.lock().unwrap()
                    })
                {
                    return Err(LixError::new(
                        "LIX_PARTIAL_BASELINE_EXPIRED",
                        "test expired baseline",
                    ));
                }
            }
            let mut builder = http::Request::builder()
                .method(request.method)
                .uri(request.url);
            for (key, value) in request.headers {
                builder = builder.header(key, value);
            }
            let response = self
                .server
                .handle(
                    builder
                        .body(ServerProtocolBody::from(request.body.unwrap_or_default()))
                        .unwrap(),
                    ServerProtocolContext::anonymous(),
                )
                .await;
            let status = response.status();
            Ok(RawHttpResponse {
                status: status.as_u16(),
                status_text: status.to_string(),
                body: response
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes()
                    .to_vec(),
            })
        })
    }
}

// Mutations use the server's canonical engine. Opening a second standalone
// engine on its backing Memory would retain stale full-repository revisions.
async fn authority_sql(
    server: &LixServerProtocol<Memory>,
    lix_id: &str,
    branch: Option<&str>,
    sql: &str,
) -> crate::ExecuteResult {
    let base = format!("https://example.test/lix/v1/{lix_id}");
    let uri = branch.map_or_else(
        || base.clone(),
        |branch| format!("{base}?activeBranchId={branch}"),
    );
    let response = server
        .handle(
            http::Request::builder()
                .method("GET")
                .uri(uri)
                .header(
                    "lix-server-protocol-version",
                    crate::SERVER_PROTOCOL_VERSION.to_string(),
                )
                .body(ServerProtocolBody::from(Vec::new()))
                .unwrap(),
            ServerProtocolContext::anonymous(),
        )
        .await;
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert!(status.is_success(), "{}", String::from_utf8_lossy(&bytes));
    let handshake: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let session = handshake["sessionId"].as_str().unwrap();
    let response = server
        .handle(
            http::Request::builder()
                .method("POST")
                .uri(format!("{base}/execute"))
                .header(
                    "lix-server-protocol-version",
                    crate::SERVER_PROTOCOL_VERSION.to_string(),
                )
                .header("lix-session-id", session)
                .header("idempotency-key", uuid::Uuid::now_v7().to_string())
                .header("content-type", "application/json")
                .body(ServerProtocolBody::from(
                    serde_json::to_vec(&serde_json::json!({"sql":sql,"params":[]})).unwrap(),
                ))
                .unwrap(),
            ServerProtocolContext::anonymous(),
        )
        .await;
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert!(status.is_success(), "{}", String::from_utf8_lossy(&bytes));
    serde_json::from_slice::<crate::authority_client::wire::ExecuteResponseBody>(&bytes)
        .unwrap()
        .into_execute_result()
        .unwrap()
}

async fn recovery(dirty: bool, advanced: bool, transaction: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(30), Box::pin(async {
        let backing = Memory::new();
        let authority = open_lix().with_storage(backing.clone()).await.unwrap();
        authority.set_sync_role(crate::sync::SyncRole::Authority).unwrap();
        authority.execute("INSERT INTO lix_key_value(key,value) VALUES('edit','before')", &[]).await.unwrap();
        authority.upsert_file_content("/cold.bin", if transaction == 2 { vec![42u8; 5 * 1024 * 1024] } else { b"cold contents".to_vec() }).await.unwrap();
        let server = open_lix().with_storage(backing).serve().with_embedded_lix_id().await.unwrap();
        let client = ExpiringClient { server, lease: Arc::default(), expire: Arc::default(), fetches: Arc::default(), live_updates: None };
        let transport = HttpSyncTransport::connect_with(client.clone(), &format!("https://example.test/lix/{}", authority.lix_id())).await.unwrap();
        let wrapper = transport.partial_replica_descriptor(None).await.unwrap();
        let old = Arc::new(PartialReplicaState::from_leased(
            transport.protocol_url().into(), authority.active_account_id().into(), uuid::Uuid::now_v7().to_string(), wrapper.wire,
        ).unwrap());
        *client.lease.lock().unwrap() = old.baseline_lease().lease_id.clone();
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
        let lix = Lix::from_partial_engine_for_test(engine.clone(), session, sender.clone());
        let worker = crate::sync::partial_runtime::run_partial_worker_with_engine(storage.clone(), old.clone(), Some(transport),
            || Box::pin(async {Err(LixError::unknown("unexpected reconnect"))}), shutdown_rx, receiver, None, Some(engine.clone()));
        let caller = async {
            if dirty { lix.execute("UPDATE lix_key_value SET value='local' WHERE key='edit'", &[]).await.unwrap(); }
            if advanced { authority_sql(&client.server, authority.lix_id(), None, "UPDATE lix_key_value SET value='remote' WHERE key='edit'").await; }
            client.expire.store(transaction == 0, Ordering::SeqCst);
            if transaction == 6 {
                let mut retry = crate::sync::SyncDemandRetry::default();
                loop {
                    let state = engine.sync_mode().partial_admission().unwrap();
                    let result = crate::sync::partial_upload_cycle::upload_partial_once(
                        &storage, &state, &state.descriptor().selected_branch.branch_id,
                        uuid::Uuid::now_v7().to_string(), 32, 1024 * 1024,
                        |_| async { Err(LixError::new("TEST_FROZEN_SELECTED_UPLOAD", "capture the request without sending it")) },
                    ).await;
                    match result {
                        Err(error) if error.code == "TEST_FROZEN_SELECTED_UPLOAD" => break,
                        Err(error) => retry.hydrate_for_retry(Some(&sender), error).await.unwrap(),
                        Ok(_) => panic!("selected local edit must be captured before authority acceptance"),
                    }
                }
                let state = engine.sync_mode().partial_admission().unwrap();
                let read = storage.begin_read(Default::default()).await.unwrap();
                let (push, _, _) = crate::sync::partial_push_state::load_partial_push_state(
                    &read, &state, &state.descriptor().selected_branch.branch_id,
                ).await.unwrap();
                assert!(push.prepared.is_some(), "selected ordinary attempt is durably frozen");
                drop(read);
                let server_value = authority.execute("SELECT value FROM lix_key_value WHERE key='edit'", &[]).await.unwrap();
                assert_eq!(server_value.rows()[0].get::<serde_json::Value>("value").unwrap(), "before", "the frozen request has not been sent");
            }
            if transaction == 3 || transaction == 6 {
                let local_global = lix.open_another_session().with_branch(crate::GLOBAL_BRANCH_ID).await.unwrap();
                local_global.execute("INSERT INTO lix_key_value(key,value) VALUES('global-race','local')", &[]).await.unwrap();
                authority_sql(&client.server, authority.lix_id(), Some(crate::GLOBAL_BRANCH_ID), "INSERT INTO lix_key_value(key,value) VALUES('global-race','server')").await;
                client.expire.store(true, Ordering::SeqCst);
            }
            let fetches_before = client.fetches.load(Ordering::SeqCst);
            let started = std::time::Instant::now();
            let sql = "SELECT content FROM lix_file WHERE path='/cold.bin'";
            let result = if transaction == 1 || transaction == 2 || transaction == 5 {
                let mut tx = lix.begin_transaction().await.expect("open cold partial transaction");
                let result = tx.execute(sql, &[]).await.expect("cold file SQL hydrates inside the pinned transaction");
                let before_inner_warm = client.fetches.load(Ordering::SeqCst);
                let warm = tx.execute(sql, &[]).await.unwrap();
                assert_eq!(result, warm);
                assert_eq!(before_inner_warm, client.fetches.load(Ordering::SeqCst), "same transaction warm SQL needs no network");
                if transaction == 5 {
                    tx.execute("UPDATE lix_file SET content=$1 WHERE path='/cold.bin'", &[crate::Value::Blob(b"committed transaction contents".to_vec().into())]).await.expect("cold file mutation hydrates before commit");
                    let staged = tx.execute(sql, &[]).await.unwrap();
                    tx.commit().await.expect("explicit file mutation commits once");
                    staged
                } else {
                    tx.rollback().await.unwrap();
                    result
                }
            } else { lix.execute(sql, &[]).await.expect("normal awaited SQL recovers without application retries") };
            let elapsed_us = started.elapsed().as_micros();
            assert_eq!(result.rows().len(), 1, "cold SQL returns the requested file");
            assert_eq!(result.rows()[0].get::<Vec<u8>>("content").unwrap(),
                if transaction == 2 { vec![42u8; 5 * 1024 * 1024] } else if transaction == 5 { b"committed transaction contents".to_vec() } else { b"cold contents".to_vec() });
            let before = client.fetches.load(Ordering::SeqCst);
            let warm = lix.execute(sql, &[]).await.unwrap();
            assert_eq!(result, warm);
            if transaction == 1 || transaction == 2 || transaction == 5 {
                // The outer session has a different read/auto-commit boundary;
                // warm it once, then require its own repeated query to be local.
                let before_outer_warm = client.fetches.load(Ordering::SeqCst);
                let warm_again = lix.execute(sql, &[]).await.unwrap();
                assert_eq!(warm, warm_again);
                assert_eq!(before_outer_warm, client.fetches.load(Ordering::SeqCst), "same outer session warm SQL needs no network");
            } else {
                assert_eq!(before, client.fetches.load(Ordering::SeqCst), "warm SQL performs no foreground fetch");
            }
            let value = lix.execute("SELECT value FROM lix_key_value WHERE key='edit'", &[]).await.unwrap();
            assert!(format!("{value:?}").contains(if dirty {"local"} else if advanced {"remote"} else {"before"}));
            eprintln!("{}", serde_json::json!({"profile":"public_recovery", "dirty":dirty,"advanced":advanced,"elapsed_us":elapsed_us,"native_requests":before - fetches_before,"transaction":transaction}));
            if transaction == 3 || transaction == 6 {
                let global = lix.open_another_session().with_branch(crate::GLOBAL_BRANCH_ID).await.unwrap();
                let value = global.execute("SELECT value FROM lix_key_value WHERE key='global-race'", &[]).await.unwrap();
                assert_eq!(value.rows()[0].get::<serde_json::Value>("value").unwrap(), "server", "unmergeable GLOBAL state follows the authority inside awaited SQL");
            }
            if transaction == 6 {
                let accepted = authority.execute("SELECT value FROM lix_key_value WHERE key='edit'", &[]).await.unwrap();
                assert_eq!(accepted.rows()[0].get::<serde_json::Value>("value").unwrap(), "local", "GLOBAL divergence must not starve the frozen selected upload");
            }
            shutdown.send_replace(crate::sync::runtime::SyncShutdown::Stop);
        };
        let (result, ()) = futures_util::join!(Box::pin(worker), Box::pin(caller));
        result.unwrap();
    })).await.expect("transparent recovery completes");
}

#[tokio::test]
async fn public_sql_recovers_expired_clean_baseline() {
    Box::pin(recovery(false, false, 0)).await;
}
#[tokio::test]
async fn public_sql_recovers_expired_dirty_unchanged_baseline() {
    Box::pin(recovery(true, false, 0)).await;
}
#[tokio::test]
async fn public_sql_recovers_expired_dirty_advanced_baseline() {
    Box::pin(recovery(true, true, 0)).await;
}

#[tokio::test]
async fn public_transaction_hydrates_cold_file() {
    Box::pin(recovery(false, false, 1)).await;
}
#[tokio::test]
async fn public_transaction_hydrates_cold_chunked_file() {
    Box::pin(recovery(false, false, 2)).await;
}

#[tokio::test]
async fn public_sql_recovers_global_overlap_with_authority_winning() {
    Box::pin(recovery(false, false, 3)).await;
}

#[tokio::test]
async fn public_transaction_hydrates_cold_file_write_and_commits() {
    Box::pin(recovery(false, false, 5)).await;
}

#[tokio::test]
async fn public_sql_settles_frozen_selected_upload_despite_global_divergence() {
    Box::pin(recovery(true, false, 6)).await;
}
